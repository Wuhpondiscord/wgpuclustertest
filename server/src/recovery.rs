//! Retry-safe task ownership primitives for Mesh's future job scheduler.
//!
//! This module deliberately keeps the coordinator authoritative. Worker events and
//! artifact hashes can be layered on later, but only the current lease generation
//! can commit a result.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct TaskLease {
    pub task_id: String,
    pub worker_id: String,
    pub coordinator_epoch: u64,
    pub generation: u64,
    pub attempt_id: u64,
    pub expires_at_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
struct TaskRecord {
    generation: u64,
    #[serde(skip)]
    lease: Option<TaskLease>,
    result_digest: Option<String>,
    #[serde(skip)]
    observation: Option<AttemptObservation>,
    #[serde(skip)]
    unavailable_at_ms: Option<u64>,
    #[serde(skip)]
    unavailable_reason: Option<AttemptEndReason>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AttemptEndReason {
    Disconnect,
    Expiry,
    Release,
    Revoked,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct AttemptObservation {
    pub worker_id: String,
    pub previous_worker_id: Option<String>,
    pub generation: u64,
    pub attempt_id: u64,
    pub coordinator_epoch: u64,
    pub claimed_at_ms: u64,
    pub failure_detected_at_ms: Option<u64>,
    pub reassigned_at_ms: Option<u64>,
    #[serde(default)]
    pub previous_attempt_reason: Option<AttemptEndReason>,
    #[serde(default)]
    pub previous_attempt_ended_at_ms: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct RecoveryLedger {
    coordinator_epoch: u64,
    next_attempt_id: u64,
    tasks: BTreeMap<String, TaskRecord>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LeaseError {
    DuplicateTask,
    UnknownTask,
    AlreadyComplete,
    LeaseBusy,
    InvalidDuration,
    LeaseExpired,
    StaleLease,
    ResultConflict,
    CounterExhausted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResultCommit {
    Accepted,
    Duplicate,
}

impl Default for RecoveryLedger {
    fn default() -> Self {
        Self {
            coordinator_epoch: 1,
            next_attempt_id: 0,
            tasks: BTreeMap::new(),
        }
    }
}

impl RecoveryLedger {
    /// Epochs are opaque fencing tokens. A fresh process supplies a fresh token
    /// because runtime claims are deliberately absent from durable snapshots.
    pub fn start_session(&mut self, epoch: u64) {
        self.coordinator_epoch = epoch;
        self.next_attempt_id = 0;
        for record in self.tasks.values_mut() {
            record.lease = None;
            record.unavailable_at_ms = None;
            record.unavailable_reason = None;
            record.observation = None;
        }
    }

    /// Keep renewals and disconnect deadlines that arrived while a durable
    /// snapshot was being written. Newly committed tasks keep their result.
    pub fn preserve_runtime_from(&mut self, current: &Self) {
        self.coordinator_epoch = current.coordinator_epoch;
        self.next_attempt_id = current.next_attempt_id;
        for (id, record) in &mut self.tasks {
            if record.result_digest.is_none() {
                if let Some(runtime) = current.tasks.get(id) {
                    record.generation = runtime.generation;
                    record.lease = runtime.lease.clone();
                    record.observation = runtime.observation.clone();
                    record.unavailable_at_ms = runtime.unavailable_at_ms;
                    record.unavailable_reason = runtime.unavailable_reason;
                }
            }
        }
    }

    pub fn shorten_worker_leases(&mut self, worker_id: &str, now_ms: u64, grace_ms: u64) {
        self.end_worker_attempts(worker_id, now_ms, grace_ms, AttemptEndReason::Disconnect);
    }

    pub fn revoke_worker_leases(&mut self, worker_id: &str, now_ms: u64) {
        self.end_worker_attempts(worker_id, now_ms, 0, AttemptEndReason::Revoked);
    }

    fn end_worker_attempts(
        &mut self,
        worker_id: &str,
        now_ms: u64,
        grace_ms: u64,
        reason: AttemptEndReason,
    ) {
        let deadline = now_ms.saturating_add(grace_ms);
        for record in self.tasks.values_mut() {
            if let Some(lease) = record
                .lease
                .as_mut()
                .filter(|lease| lease.worker_id == worker_id)
            {
                if record.unavailable_at_ms.is_none() {
                    record.unavailable_at_ms = Some(now_ms);
                    record.unavailable_reason = Some(reason);
                }
                lease.expires_at_ms = lease.expires_at_ms.min(deadline);
            }
        }
    }

    pub fn resume_worker_leases(&mut self, worker_id: &str, now_ms: u64, duration_ms: u64) {
        for record in self.tasks.values_mut() {
            if let Some(lease) = record
                .lease
                .as_mut()
                .filter(|lease| lease.worker_id == worker_id && lease.expires_at_ms > now_ms)
            {
                lease.expires_at_ms = now_ms.saturating_add(duration_ms);
                record.unavailable_at_ms = None;
                record.unavailable_reason = None;
            }
        }
    }

    pub fn add_task(&mut self, task_id: impl Into<String>) -> Result<(), LeaseError> {
        let task_id = task_id.into();
        if self.tasks.contains_key(&task_id) {
            return Err(LeaseError::DuplicateTask);
        }
        self.tasks.insert(
            task_id,
            TaskRecord {
                generation: 0,
                lease: None,
                result_digest: None,
                observation: None,
                unavailable_at_ms: None,
                unavailable_reason: None,
            },
        );
        Ok(())
    }

    pub fn remove_task(&mut self, task_id: &str) -> bool {
        self.tasks.remove(task_id).is_some()
    }

    /// Claim an unassigned or expired task. A new generation fences every prior
    /// worker attempt; unrelated task records are unaffected.
    pub fn claim(
        &mut self,
        task_id: &str,
        worker_id: impl Into<String>,
        now_ms: u64,
        lease_duration_ms: u64,
    ) -> Result<TaskLease, LeaseError> {
        if lease_duration_ms == 0 {
            return Err(LeaseError::InvalidDuration);
        }
        let record = self.tasks.get_mut(task_id).ok_or(LeaseError::UnknownTask)?;
        if record.result_digest.is_some() {
            return Err(LeaseError::AlreadyComplete);
        }
        if record
            .lease
            .as_ref()
            .is_some_and(|lease| lease.expires_at_ms > now_ms)
        {
            return Err(LeaseError::LeaseBusy);
        }
        let generation = record
            .generation
            .checked_add(1)
            .ok_or(LeaseError::CounterExhausted)?;
        let attempt_id = self
            .next_attempt_id
            .checked_add(1)
            .ok_or(LeaseError::CounterExhausted)?;
        let expires_at_ms = now_ms
            .checked_add(lease_duration_ms)
            .ok_or(LeaseError::CounterExhausted)?;
        record.generation = generation;
        self.next_attempt_id = attempt_id;
        let worker_id = worker_id.into();
        let previous_worker_id = record
            .observation
            .as_ref()
            .map(|observation| observation.worker_id.clone());
        let expired = record.lease.is_some(); // A live unexpired lease was rejected above.
        let previous_attempt_reason = record
            .unavailable_reason
            .take()
            .or(expired.then_some(AttemptEndReason::Expiry));
        let previous_attempt_ended_at_ms = record
            .unavailable_at_ms
            .take()
            .or(expired.then_some(now_ms));
        let failure_detected_at_ms = match previous_attempt_reason {
            Some(AttemptEndReason::Disconnect | AttemptEndReason::Expiry) => {
                previous_attempt_ended_at_ms
            }
            _ => None,
        };
        record.observation = Some(AttemptObservation {
            worker_id: worker_id.clone(),
            previous_worker_id,
            generation,
            attempt_id,
            coordinator_epoch: self.coordinator_epoch,
            claimed_at_ms: now_ms,
            failure_detected_at_ms,
            reassigned_at_ms: if generation > 1 { Some(now_ms) } else { None },
            previous_attempt_reason,
            previous_attempt_ended_at_ms,
        });
        let lease = TaskLease {
            task_id: task_id.to_owned(),
            worker_id,
            coordinator_epoch: self.coordinator_epoch,
            generation,
            attempt_id,
            expires_at_ms,
        };
        record.lease = Some(lease.clone());
        Ok(lease)
    }

    /// Renew only the currently active, unexpired attempt. The returned lease
    /// carries the new deadline and should replace the caller's previous copy.
    pub fn renew(
        &mut self,
        lease: &TaskLease,
        now_ms: u64,
        lease_duration_ms: u64,
    ) -> Result<TaskLease, LeaseError> {
        if lease_duration_ms == 0 {
            return Err(LeaseError::InvalidDuration);
        }
        let record = self
            .tasks
            .get_mut(&lease.task_id)
            .ok_or(LeaseError::UnknownTask)?;
        let current = record.lease.as_ref().ok_or(LeaseError::StaleLease)?;
        if !same_attempt(current, lease) {
            return Err(LeaseError::StaleLease);
        }
        if now_ms >= current.expires_at_ms {
            return Err(LeaseError::LeaseExpired);
        }
        let mut renewed = current.clone();
        renewed.expires_at_ms = now_ms
            .checked_add(lease_duration_ms)
            .ok_or(LeaseError::CounterExhausted)?;
        record.lease = Some(renewed.clone());
        Ok(renewed)
    }

    /// Commit a result at most once. Replaying the same digest is idempotent;
    /// a different result for an already-completed task is rejected.
    pub fn commit_result(
        &mut self,
        lease: &TaskLease,
        now_ms: u64,
        result_digest: impl Into<String>,
    ) -> Result<ResultCommit, LeaseError> {
        let digest = result_digest.into();
        let record = self
            .tasks
            .get_mut(&lease.task_id)
            .ok_or(LeaseError::UnknownTask)?;
        if let Some(accepted) = record.result_digest.as_ref() {
            return if accepted == &digest {
                Ok(ResultCommit::Duplicate)
            } else {
                Err(LeaseError::ResultConflict)
            };
        }
        let current = record.lease.as_ref().ok_or(LeaseError::StaleLease)?;
        if !same_attempt(current, lease) {
            return Err(LeaseError::StaleLease);
        }
        if now_ms >= current.expires_at_ms {
            return Err(LeaseError::LeaseExpired);
        }
        record.result_digest = Some(digest);
        record.lease = None;
        Ok(ResultCommit::Accepted)
    }

    /// A coordinator restart invalidates all in-flight worker leases. Completed
    /// results remain durable; unfinished tasks can be claimed again.
    pub fn restart_coordinator(&mut self) -> Result<u64, LeaseError> {
        self.coordinator_epoch = self
            .coordinator_epoch
            .checked_add(1)
            .ok_or(LeaseError::CounterExhausted)?;
        for record in self.tasks.values_mut() {
            if record.result_digest.is_none() {
                record.lease = None;
                record.observation = None;
                record.unavailable_at_ms = None;
                record.unavailable_reason = None;
            }
        }
        Ok(self.coordinator_epoch)
    }

    pub fn is_complete(&self, task_id: &str) -> bool {
        self.tasks
            .get(task_id)
            .is_some_and(|record| record.result_digest.is_some())
    }

    pub fn accepted_digest(&self, task_id: &str) -> Option<&str> {
        self.tasks
            .get(task_id)
            .and_then(|record| record.result_digest.as_deref())
    }

    pub fn observation(&self, task_id: &str) -> Option<AttemptObservation> {
        self.tasks
            .get(task_id)
            .and_then(|record| record.observation.clone())
    }

    /// Voluntary release only affects the exact current attempt. It does not
    /// advance the generation; the next claim performs the fencing transition.
    pub fn release(&mut self, lease: &TaskLease, now_ms: u64) -> Result<(), LeaseError> {
        let record = self
            .tasks
            .get_mut(&lease.task_id)
            .ok_or(LeaseError::UnknownTask)?;
        if record.result_digest.is_some() {
            return Err(LeaseError::AlreadyComplete);
        }
        let current = record.lease.as_mut().ok_or(LeaseError::StaleLease)?;
        if !same_attempt(current, lease) {
            return Err(LeaseError::StaleLease);
        }
        if record.unavailable_at_ms.is_none() {
            record.unavailable_at_ms = Some(now_ms);
            record.unavailable_reason = Some(if now_ms >= current.expires_at_ms {
                AttemptEndReason::Expiry
            } else {
                AttemptEndReason::Release
            });
        }
        current.expires_at_ms = current.expires_at_ms.min(now_ms);
        Ok(())
    }
}

fn same_attempt(current: &TaskLease, submitted: &TaskLease) -> bool {
    current.task_id == submitted.task_id
        && current.worker_id == submitted.worker_id
        && current.coordinator_epoch == submitted.coordinator_epoch
        && current.generation == submitted.generation
        && current.attempt_id == submitted.attempt_id
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn release_is_fenced_and_observation_distinguishes_retry_from_takeover() {
        let mut ledger = RecoveryLedger::default();
        ledger.add_task("batch").unwrap();
        let first = ledger.claim("batch", "a", 0, 100).unwrap();
        ledger.release(&first, 10).unwrap();
        let retry = ledger.claim("batch", "a", 11, 100).unwrap();
        let released = ledger.observation("batch").unwrap();
        assert_eq!(
            released.previous_attempt_reason,
            Some(AttemptEndReason::Release)
        );
        assert_eq!(released.previous_attempt_ended_at_ms, Some(10));
        assert_eq!(released.failure_detected_at_ms, None);
        assert_eq!(ledger.release(&first, 12), Err(LeaseError::StaleLease));
        assert_eq!(
            ledger
                .observation("batch")
                .unwrap()
                .previous_worker_id
                .as_deref(),
            Some("a")
        );
        ledger.shorten_worker_leases("a", 20, 10);
        let takeover = ledger.claim("batch", "b", 30, 100).unwrap();
        let observed = ledger.observation("batch").unwrap();
        assert_eq!(observed.worker_id, "b");
        assert_eq!(observed.previous_worker_id.as_deref(), Some("a"));
        assert_eq!(observed.claimed_at_ms, 30);
        assert_eq!(observed.reassigned_at_ms, Some(30));
        assert_eq!(observed.failure_detected_at_ms, Some(20));
        assert_eq!(
            observed.previous_attempt_reason,
            Some(AttemptEndReason::Disconnect)
        );
        ledger.commit_result(&takeover, 40, "hash").unwrap();
        assert_eq!(ledger.release(&retry, 41), Err(LeaseError::AlreadyComplete));
        assert_eq!(ledger.observation("batch").unwrap(), observed);
    }

    #[test]
    fn expiry_and_revocation_have_separate_causes_and_old_observations_load() {
        let mut ledger = RecoveryLedger::default();
        ledger.add_task("batch").unwrap();
        ledger.claim("batch", "a", 0, 10).unwrap();
        ledger.claim("batch", "b", 20, 100).unwrap();
        let expired = ledger.observation("batch").unwrap();
        assert_eq!(
            expired.previous_attempt_reason,
            Some(AttemptEndReason::Expiry)
        );
        assert_eq!(expired.failure_detected_at_ms, Some(20));
        ledger.revoke_worker_leases("b", 30);
        ledger.claim("batch", "c", 40, 100).unwrap();
        let revoked = ledger.observation("batch").unwrap();
        assert_eq!(
            revoked.previous_attempt_reason,
            Some(AttemptEndReason::Revoked)
        );
        assert_eq!(revoked.failure_detected_at_ms, None);
        let mut old = serde_json::to_value(&expired).unwrap();
        old.as_object_mut()
            .unwrap()
            .remove("previous_attempt_reason");
        old.as_object_mut()
            .unwrap()
            .remove("previous_attempt_ended_at_ms");
        let old: AttemptObservation = serde_json::from_value(old).unwrap();
        assert_eq!(old.previous_attempt_reason, None);
    }

    #[test]
    fn renewal_survives_the_original_deadline_and_a_durable_snapshot_swap() {
        let mut current = RecoveryLedger::default();
        current.add_task("batch").unwrap();
        let lease = current.claim("batch", "worker", 0, 30).unwrap();
        let mut snapshot = current.clone();
        current.renew(&lease, 20, 100).unwrap();
        snapshot.preserve_runtime_from(&current);
        assert_eq!(
            snapshot.commit_result(&lease, 80, "result"),
            Ok(ResultCommit::Accepted)
        );
    }

    #[test]
    fn disconnect_grace_allows_reconnect_then_fences_reassigned_work() {
        let mut ledger = RecoveryLedger::default();
        ledger.add_task("a").unwrap();
        ledger.add_task("b").unwrap();
        let old = ledger.claim("a", "worker-a", 0, 100).unwrap();
        let other = ledger.claim("b", "worker-b", 0, 100).unwrap();
        ledger.shorten_worker_leases("worker-a", 10, 10);
        assert_eq!(
            ledger.claim("a", "worker-c", 19, 100),
            Err(LeaseError::LeaseBusy)
        );
        ledger.resume_worker_leases("worker-a", 15, 100);
        assert_eq!(
            ledger.claim("a", "worker-c", 25, 100),
            Err(LeaseError::LeaseBusy)
        );
        ledger.shorten_worker_leases("worker-a", 30, 10);
        let reassigned = ledger.claim("a", "worker-c", 40, 100).unwrap();
        assert_eq!(ledger.renew(&old, 41, 100), Err(LeaseError::StaleLease));
        assert_eq!(
            ledger.commit_result(&old, 41, "stale"),
            Err(LeaseError::StaleLease)
        );
        assert_eq!(
            ledger.commit_result(&reassigned, 41, "fresh"),
            Ok(ResultCommit::Accepted)
        );
        assert_eq!(
            ledger.commit_result(&other, 41, "independent"),
            Ok(ResultCommit::Accepted)
        );
    }

    #[test]
    fn snapshots_exclude_leases_and_fresh_sessions_fence_old_attempts() {
        let mut ledger = RecoveryLedger::default();
        ledger.add_task("batch").unwrap();
        ledger.start_session(101);
        let old = ledger.claim("batch", "worker", 0, 100).unwrap();
        let json = serde_json::to_string(&ledger).unwrap();
        assert!(!json.contains("expires_at_ms"));
        let mut restored: RecoveryLedger = serde_json::from_str(&json).unwrap();
        restored.start_session(102);
        let fresh = restored.claim("batch", "worker", 1, 100).unwrap();
        assert_eq!(
            restored.commit_result(&old, 2, "stale"),
            Err(LeaseError::StaleLease)
        );
        assert_eq!(
            restored.commit_result(&fresh, 2, "fresh"),
            Ok(ResultCommit::Accepted)
        );
    }

    #[test]
    fn expired_attempt_is_fenced_after_reassignment() {
        let mut ledger = RecoveryLedger::default();
        ledger.add_task("tile-7").unwrap();
        ledger.add_task("tile-8").unwrap();
        let old = ledger.claim("tile-7", "worker-a", 100, 50).unwrap();
        let independent = ledger.claim("tile-8", "worker-c", 100, 50).unwrap();
        assert_eq!(
            ledger.commit_result(&independent, 120, "result-8"),
            Ok(ResultCommit::Accepted)
        );

        let current = ledger.claim("tile-7", "worker-b", 150, 50).unwrap();
        assert!(current.generation > old.generation);
        assert_eq!(
            ledger.commit_result(&old, 151, "stale-result"),
            Err(LeaseError::StaleLease)
        );
        assert_eq!(
            ledger.commit_result(&current, 160, "result-7"),
            Ok(ResultCommit::Accepted)
        );
        assert_eq!(ledger.accepted_digest("tile-7"), Some("result-7"));
        assert!(ledger.is_complete("tile-8"));
    }

    #[test]
    fn expired_lease_is_rejected_even_before_another_worker_claims_it() {
        let mut ledger = RecoveryLedger::default();
        ledger.add_task("task").unwrap();
        let lease = ledger.claim("task", "worker-a", 10, 20).unwrap();
        assert_eq!(
            ledger.commit_result(&lease, 30, "late"),
            Err(LeaseError::LeaseExpired)
        );
    }

    #[test]
    fn duplicate_result_commit_is_idempotent_but_conflicting_result_is_not() {
        let mut ledger = RecoveryLedger::default();
        ledger.add_task("task").unwrap();
        let lease = ledger.claim("task", "worker-a", 0, 100).unwrap();
        assert_eq!(
            ledger.commit_result(&lease, 1, "digest-a"),
            Ok(ResultCommit::Accepted)
        );
        assert_eq!(
            ledger.commit_result(&lease, 2, "digest-a"),
            Ok(ResultCommit::Duplicate)
        );
        assert_eq!(
            ledger.commit_result(&lease, 2, "digest-b"),
            Err(LeaseError::ResultConflict)
        );
    }

    #[test]
    fn coordinator_restart_invalidates_in_flight_leases_but_keeps_completed_work() {
        let mut ledger = RecoveryLedger::default();
        ledger.add_task("running").unwrap();
        ledger.add_task("done").unwrap();
        let in_flight = ledger.claim("running", "worker-a", 0, 100).unwrap();
        let completed = ledger.claim("done", "worker-b", 0, 100).unwrap();
        ledger.commit_result(&completed, 1, "durable").unwrap();

        let new_epoch = ledger.restart_coordinator().unwrap();
        assert_eq!(
            ledger.commit_result(&in_flight, 2, "old-epoch"),
            Err(LeaseError::StaleLease)
        );
        assert_eq!(ledger.accepted_digest("done"), Some("durable"));
        let resumed = ledger.claim("running", "worker-c", 2, 100).unwrap();
        assert_eq!(resumed.coordinator_epoch, new_epoch);
        assert_eq!(
            ledger.commit_result(&resumed, 3, "new-epoch"),
            Ok(ResultCommit::Accepted)
        );
    }

    #[test]
    fn task_idempotency_and_expiry_are_explicit() {
        let mut ledger = RecoveryLedger::default();
        assert_eq!(ledger.add_task("task"), Ok(()));
        assert_eq!(ledger.add_task("task"), Err(LeaseError::DuplicateTask));
        assert_eq!(
            ledger.claim("task", "worker", 0, 0),
            Err(LeaseError::InvalidDuration)
        );
        assert_eq!(
            ledger.claim("missing", "worker", 0, 10),
            Err(LeaseError::UnknownTask)
        );
    }
}
