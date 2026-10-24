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
    lease: Option<TaskLease>,
    result_digest: Option<String>,
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
        Self { coordinator_epoch: 1, next_attempt_id: 0, tasks: BTreeMap::new() }
    }
}

impl RecoveryLedger {
    pub fn add_task(&mut self, task_id: impl Into<String>) -> Result<(), LeaseError> {
        let task_id = task_id.into();
        if self.tasks.contains_key(&task_id) { return Err(LeaseError::DuplicateTask); }
        self.tasks.insert(task_id, TaskRecord { generation: 0, lease: None, result_digest: None });
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
        if lease_duration_ms == 0 { return Err(LeaseError::InvalidDuration); }
        let record = self.tasks.get_mut(task_id).ok_or(LeaseError::UnknownTask)?;
        if record.result_digest.is_some() { return Err(LeaseError::AlreadyComplete); }
        if record.lease.as_ref().is_some_and(|lease| lease.expires_at_ms > now_ms) {
            return Err(LeaseError::LeaseBusy);
        }
        let generation = record.generation.checked_add(1).ok_or(LeaseError::CounterExhausted)?;
        let attempt_id = self.next_attempt_id.checked_add(1).ok_or(LeaseError::CounterExhausted)?;
        let expires_at_ms = now_ms.checked_add(lease_duration_ms).ok_or(LeaseError::CounterExhausted)?;
        record.generation = generation;
        self.next_attempt_id = attempt_id;
        let lease = TaskLease {
            task_id: task_id.to_owned(),
            worker_id: worker_id.into(),
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
        if lease_duration_ms == 0 { return Err(LeaseError::InvalidDuration); }
        let record = self.tasks.get_mut(&lease.task_id).ok_or(LeaseError::UnknownTask)?;
        let current = record.lease.as_ref().ok_or(LeaseError::StaleLease)?;
        if !same_attempt(current, lease) { return Err(LeaseError::StaleLease); }
        if now_ms >= current.expires_at_ms { return Err(LeaseError::LeaseExpired); }
        let mut renewed = current.clone();
        renewed.expires_at_ms = now_ms.checked_add(lease_duration_ms).ok_or(LeaseError::CounterExhausted)?;
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
        let record = self.tasks.get_mut(&lease.task_id).ok_or(LeaseError::UnknownTask)?;
        if let Some(accepted) = record.result_digest.as_ref() {
            return if accepted == &digest { Ok(ResultCommit::Duplicate) } else { Err(LeaseError::ResultConflict) };
        }
        let current = record.lease.as_ref().ok_or(LeaseError::StaleLease)?;
        if !same_attempt(current, lease) { return Err(LeaseError::StaleLease); }
        if now_ms >= current.expires_at_ms { return Err(LeaseError::LeaseExpired); }
        record.result_digest = Some(digest);
        record.lease = None;
        Ok(ResultCommit::Accepted)
    }

    /// A coordinator restart invalidates all in-flight worker leases. Completed
    /// results remain durable; unfinished tasks can be claimed again.
    pub fn restart_coordinator(&mut self) -> Result<u64, LeaseError> {
        self.coordinator_epoch = self.coordinator_epoch.checked_add(1).ok_or(LeaseError::CounterExhausted)?;
        for record in self.tasks.values_mut() {
            if record.result_digest.is_none() { record.lease = None; }
        }
        Ok(self.coordinator_epoch)
    }

    pub fn is_complete(&self, task_id: &str) -> bool {
        self.tasks.get(task_id).is_some_and(|record| record.result_digest.is_some())
    }

    pub fn accepted_digest(&self, task_id: &str) -> Option<&str> {
        self.tasks.get(task_id).and_then(|record| record.result_digest.as_deref())
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
    fn expired_attempt_is_fenced_after_reassignment() {
        let mut ledger = RecoveryLedger::default();
        ledger.add_task("tile-7").unwrap();
        ledger.add_task("tile-8").unwrap();
        let old = ledger.claim("tile-7", "worker-a", 100, 50).unwrap();
        let independent = ledger.claim("tile-8", "worker-c", 100, 50).unwrap();
        assert_eq!(ledger.commit_result(&independent, 120, "result-8"), Ok(ResultCommit::Accepted));

        let current = ledger.claim("tile-7", "worker-b", 150, 50).unwrap();
        assert!(current.generation > old.generation);
        assert_eq!(ledger.commit_result(&old, 151, "stale-result"), Err(LeaseError::StaleLease));
        assert_eq!(ledger.commit_result(&current, 160, "result-7"), Ok(ResultCommit::Accepted));
        assert_eq!(ledger.accepted_digest("tile-7"), Some("result-7"));
        assert!(ledger.is_complete("tile-8"));
    }

    #[test]
    fn expired_lease_is_rejected_even_before_another_worker_claims_it() {
        let mut ledger = RecoveryLedger::default();
        ledger.add_task("task").unwrap();
        let lease = ledger.claim("task", "worker-a", 10, 20).unwrap();
        assert_eq!(ledger.commit_result(&lease, 30, "late"), Err(LeaseError::LeaseExpired));
    }

    #[test]
    fn duplicate_result_commit_is_idempotent_but_conflicting_result_is_not() {
        let mut ledger = RecoveryLedger::default();
        ledger.add_task("task").unwrap();
        let lease = ledger.claim("task", "worker-a", 0, 100).unwrap();
        assert_eq!(ledger.commit_result(&lease, 1, "digest-a"), Ok(ResultCommit::Accepted));
        assert_eq!(ledger.commit_result(&lease, 2, "digest-a"), Ok(ResultCommit::Duplicate));
        assert_eq!(ledger.commit_result(&lease, 2, "digest-b"), Err(LeaseError::ResultConflict));
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
        assert_eq!(ledger.commit_result(&in_flight, 2, "old-epoch"), Err(LeaseError::StaleLease));
        assert_eq!(ledger.accepted_digest("done"), Some("durable"));
        let resumed = ledger.claim("running", "worker-c", 2, 100).unwrap();
        assert_eq!(resumed.coordinator_epoch, new_epoch);
        assert_eq!(ledger.commit_result(&resumed, 3, "new-epoch"), Ok(ResultCommit::Accepted));
    }

    #[test]
    fn task_idempotency_and_expiry_are_explicit() {
        let mut ledger = RecoveryLedger::default();
        assert_eq!(ledger.add_task("task"), Ok(()));
        assert_eq!(ledger.add_task("task"), Err(LeaseError::DuplicateTask));
        assert_eq!(ledger.claim("task", "worker", 0, 0), Err(LeaseError::InvalidDuration));
        assert_eq!(ledger.claim("missing", "worker", 0, 10), Err(LeaseError::UnknownTask));
    }
}

