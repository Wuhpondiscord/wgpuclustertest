//! Repair scheduling and manifest swaps; reconstruction stays on browser peers.
use crate::{
    is_current_node_connection, mutate_store, now, ApiError, ApiResult, AppState, ShardReceipt,
};
use axum::{extract::ws::Message, http::StatusCode};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

const REPAIR_TTL_SECONDS: u64 = 180;
const RETRY_BACKOFF_SECONDS: u64 = 10;

#[derive(Clone, Debug, Serialize)]
pub struct RepairTask {
    pub id: Uuid,
    pub job_id: Uuid,
    pub batch_index: u64,
    pub manifest_revision: u64,
    pub sha256: String,
    pub bytes: u64,
    pub shards: Vec<ShardReceipt>,
    pub target: ShardReceipt,
    pub worker_node_id: Uuid,
    pub expires_at: u64,
    #[serde(skip)]
    worker_session: Uuid,
}

pub struct RepairQueue {
    tasks: HashMap<Uuid, RepairTask>,
    retry_after: HashMap<(Uuid, u64), u64>,
    unavailable_since: HashMap<Uuid, u64>,
    grace_seconds: u64,
}

impl Default for RepairQueue {
    fn default() -> Self {
        let grace_seconds = std::env::var("MESH_REPAIR_GRACE_SECS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(20)
            .clamp(1, 300);
        Self {
            tasks: HashMap::new(),
            retry_after: HashMap::new(),
            unavailable_since: HashMap::new(),
            grace_seconds,
        }
    }
}

impl RepairQueue {
    pub fn fail(&mut self, id: Uuid, worker: Uuid, session: Uuid, at: u64) {
        if self
            .tasks
            .get(&id)
            .is_some_and(|task| task.worker_node_id == worker && task.worker_session == session)
        {
            if let Some(task) = self.tasks.remove(&id) {
                self.retry_after
                    .insert((task.job_id, task.batch_index), at + RETRY_BACKOFF_SECONDS);
            }
        }
    }
}

pub fn dispatch(state: &AppState) {
    let (jobs, nodes) = match state.store.lock() {
        Ok(store) => (
            store.jobs.values().cloned().collect::<Vec<_>>(),
            store.nodes.clone(),
        ),
        Err(_) => return,
    };
    let connections = match state.node_connections.lock() {
        Ok(connections) => connections.clone(),
        Err(_) => return,
    };
    let active: HashSet<_> = nodes
        .values()
        .filter(|node| node.online && !node.paused && connections.contains_key(&node.id))
        .map(|node| node.id)
        .collect();
    let ready: HashSet<_> = connections
        .iter()
        .filter(|(id, connection)| active.contains(*id) && connection.receipt_key.is_some())
        .map(|(id, _)| *id)
        .collect();
    let at = now();
    let Ok(mut runtime) = state.durability.lock() else {
        return;
    };
    let queue = &mut runtime.repairs;
    for node in nodes.values() {
        if active.contains(&node.id) {
            queue.unavailable_since.remove(&node.id);
        } else {
            queue.unavailable_since.entry(node.id).or_insert(at);
        }
    }
    let job_ids: HashSet<_> = jobs.iter().map(|job| job.id).collect();
    queue.retry_after.retain(|(id, _), _| job_ids.contains(id));
    let task_nodes: Vec<_> = queue
        .tasks
        .values()
        .flat_map(|task| [task.worker_node_id, task.target.node_id])
        .collect();
    for id in task_nodes {
        if !active.contains(&id) {
            queue.unavailable_since.entry(id).or_insert(at);
        }
    }
    let stale: Vec<_> = queue
        .tasks
        .values()
        .filter(|task| {
            task.expires_at <= at
                || !job_ids.contains(&task.job_id)
                || [task.worker_node_id, task.target.node_id].iter().any(|id| {
                    !active.contains(id)
                        && queue
                            .unavailable_since
                            .get(id)
                            .is_some_and(|since| at.saturating_sub(*since) >= queue.grace_seconds)
                })
        })
        .map(|task| (task.id, task.job_id, task.batch_index))
        .collect();
    for (id, job, batch) in stale {
        queue.tasks.remove(&id);
        queue
            .retry_after
            .insert((job, batch), at + RETRY_BACKOFF_SECONDS);
    }

    let mut dispatched = 0;
    for job in jobs {
        for (batch_index, output) in job.outputs {
            if !matches!(output.shards.len(), 2 | 3)
                || queue
                    .tasks
                    .values()
                    .any(|task| task.job_id == job.id && task.batch_index == batch_index)
                || queue
                    .retry_after
                    .get(&(job.id, batch_index))
                    .is_some_and(|deadline| *deadline > at)
            {
                continue;
            }
            let available: Vec<_> = output
                .shards
                .iter()
                .filter(|shard| active.contains(&shard.node_id))
                .collect();
            let threshold = if output.shards.len() == 3 { 2 } else { 1 };
            if available.len() < threshold || available.len() == output.shards.len() {
                continue;
            }
            let missing = output
                .shards
                .iter()
                .find(|shard| !active.contains(&shard.node_id))
                .expect("degraded manifest");
            let since = *queue.unavailable_since.entry(missing.node_id).or_insert(at);
            if at.saturating_sub(since) < queue.grace_seconds {
                continue;
            }
            let worker = available.iter().find(|shard| {
                ready.contains(&shard.node_id)
                    && !queue
                        .tasks
                        .values()
                        .any(|task| task.worker_node_id == shard.node_id)
            });
            let Some(worker) = worker else {
                continue;
            };
            let mut targets: Vec<_> = nodes
                .values()
                .filter(|node| {
                    node.cluster_id == job.cluster_id
                        && ready.contains(&node.id)
                        && !output.shards.iter().any(|shard| shard.node_id == node.id)
                        && !queue
                            .tasks
                            .values()
                            .any(|task| task.target.node_id == node.id)
                })
                .map(|node| node.id)
                .collect();
            targets.sort();
            let Some(target_id) = targets.first() else {
                continue;
            };
            let Some(connection) = connections.get(&worker.node_id) else {
                continue;
            };
            let id = Uuid::new_v4();
            let mut target = missing.clone();
            target.node_id = *target_id;
            target.artifact_name = format!("repair-{id}-s{}", target.shard_index);
            let task = RepairTask {
                id,
                job_id: job.id,
                batch_index,
                manifest_revision: output.manifest_revision,
                sha256: output.sha256.clone(),
                bytes: output.bytes,
                shards: output.shards.clone(),
                target,
                worker_node_id: worker.node_id,
                worker_session: connection.session_id,
                expires_at: at + REPAIR_TTL_SECONDS,
            };
            if connection
                .sender
                .try_send(Message::Text(
                    serde_json::json!({"type":"repair_checkpoint","task":task}).to_string(),
                ))
                .is_ok()
            {
                queue.tasks.insert(id, task);
                dispatched += 1;
                if dispatched >= 8 {
                    return;
                }
            }
        }
    }
}

pub async fn complete(state: &AppState, id: Uuid, worker: Uuid, session: Uuid) -> ApiResult<()> {
    if !is_current_node_connection(state, worker, session) {
        return Err(ApiError(
            StatusCode::CONFLICT,
            "The repair worker session changed".into(),
        ));
    }
    let task = state
        .durability
        .lock()
        .map_err(|_| {
            ApiError(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Repair queue unavailable".into(),
            )
        })?
        .repairs
        .tasks
        .get(&id)
        .filter(|task| {
            task.worker_node_id == worker
                && task.worker_session == session
                && task.expires_at > now()
        })
        .cloned()
        .ok_or(ApiError(
            StatusCode::CONFLICT,
            "The repair attempt expired or was reassigned".into(),
        ))?;
    let durability = state.durability.clone();
    let expected = task.clone();
    let cluster_id = mutate_store(state, move |store| {
        let job = store
            .jobs
            .get_mut(&expected.job_id)
            .ok_or(ApiError(StatusCode::NOT_FOUND, "Job was removed".into()))?;
        let output = job.outputs.get_mut(&expected.batch_index).ok_or(ApiError(
            StatusCode::NOT_FOUND,
            "Checkpoint was removed".into(),
        ))?;
        if output.manifest_revision != expected.manifest_revision
            || output.sha256 != expected.sha256
            || output.shards != expected.shards
        {
            return Err(ApiError(
                StatusCode::CONFLICT,
                "Checkpoint changed while repair was running".into(),
            ));
        }
        if !store
            .nodes
            .get(&expected.target.node_id)
            .is_some_and(|node| node.cluster_id == job.cluster_id && node.online && !node.paused)
        {
            return Err(ApiError(
                StatusCode::CONFLICT,
                "The replacement holder is no longer active".into(),
            ));
        }
        let runtime = durability.lock().map_err(|_| {
            ApiError(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Holder registry unavailable".into(),
            )
        })?;
        if !runtime
            .repairs
            .tasks
            .get(&id)
            .is_some_and(|current| current.worker_session == session && current.expires_at > now())
        {
            return Err(ApiError(
                StatusCode::CONFLICT,
                "Repair ownership was lost before manifest commit".into(),
            ));
        }
        let proof = runtime
            .custody
            .proof(&expected.target, now())
            .map_err(|message| ApiError(StatusCode::CONFLICT, message))?;
        let index = expected.target.shard_index as usize;
        let old = output
            .shards
            .get(index)
            .ok_or(ApiError(
                StatusCode::CONFLICT,
                "Unknown repair shard".into(),
            ))?
            .clone();
        output.shards[index] = expected.target.clone();
        output.holder_proofs.retain(|proof| {
            proof.node_id != old.node_id || proof.artifact_name != old.artifact_name
        });
        output.holder_proofs.push(proof);
        output.replica_node_id = output.shards[1].node_id;
        output.manifest_revision = output.manifest_revision.checked_add(1).ok_or(ApiError(
            StatusCode::CONFLICT,
            "Manifest revision exhausted".into(),
        ))?;
        Ok(job.cluster_id)
    })
    .await?;
    if let Ok(mut runtime) = state.durability.lock() {
        runtime.repairs.tasks.remove(&id);
        runtime
            .repairs
            .retry_after
            .remove(&(task.job_id, task.batch_index));
    }
    let event = serde_json::json!({"type":"checkpoint_repaired","job_id":task.job_id,"batch_index":task.batch_index}).to_string();
    if let Ok(connections) = state.node_connections.lock() {
        for connection in connections
            .values()
            .filter(|connection| connection.cluster_id == cluster_id)
        {
            let _ = connection.sender.try_send(Message::Text(event.clone()));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        BatchOutput, ComputeJob, ComputeWorkload, DurabilityRuntime, Node, NodeConnection, Store,
    };
    use std::sync::{Arc, Mutex};

    fn fixture() -> (
        AppState,
        Uuid,
        Vec<ShardReceipt>,
        Vec<tokio::sync::mpsc::Receiver<Message>>,
    ) {
        let cluster = Uuid::new_v4();
        let job_id = Uuid::new_v4();
        let shards: Vec<_> = (0..3)
            .map(|index| ShardReceipt {
                shard_index: index,
                node_id: Uuid::new_v4(),
                artifact_name: format!("original-s{index}"),
                sha256: format!("{index}").repeat(64),
                bytes: 32_768,
            })
            .collect();
        let mut runtime = DurabilityRuntime::default();
        runtime.repairs.grace_seconds = 0;
        for shard in &shards {
            crate::custody::tests::confirm(&mut runtime.custody, shard, now());
        }
        let proofs = shards
            .iter()
            .map(|shard| runtime.custody.proof(shard, now()).unwrap())
            .collect();
        let mut store = Store::default();
        let mut connections = HashMap::new();
        let mut receivers = Vec::new();
        for (index, id) in shards
            .iter()
            .map(|shard| shard.node_id)
            .chain([Uuid::new_v4()])
            .enumerate()
        {
            store.nodes.insert(
                id,
                Node {
                    id,
                    cluster_id: cluster,
                    name: format!("node-{index}"),
                    owner_id: "owner".into(),
                    token_hash: "token-hash".into(),
                    paused: index == 2,
                    online: true,
                    last_seen: Some(now()),
                },
            );
            let (sender, receiver) = tokio::sync::mpsc::channel(16);
            receivers.push(receiver);
            connections.insert(
                id,
                NodeConnection {
                    session_id: Uuid::new_v4(),
                    cluster_id: cluster,
                    sender,
                    receipt_nonce: "session".into(),
                    receipt_key: Some("ready".into()),
                },
            );
        }
        store.jobs.insert(
            job_id,
            ComputeJob {
                id: job_id,
                cluster_id: cluster,
                created_at: now(),
                workload: ComputeWorkload::VectorAdd,
                total_units: 1_000,
                batch_units: 1_000,
                outputs: HashMap::from([(
                    0,
                    BatchOutput {
                        sha256: "a".repeat(64),
                        bytes: 65_536,
                        worker_node_id: shards[0].node_id,
                        replica_node_id: shards[1].node_id,
                        committed_at: now(),
                        shards: shards.clone(),
                        holder_proofs: proofs,
                        manifest_revision: 0,
                    },
                )]),
            },
        );
        let state = AppState {
            store: Arc::new(Mutex::new(store)),
            mutation_lock: Arc::new(tokio::sync::Mutex::new(())),
            data_file: Arc::new(std::env::temp_dir().join(format!("mesh-repair-{job_id}.json"))),
            github: None,
            github_file_sha: Arc::new(Mutex::new(None)),
            node_connections: Arc::new(Mutex::new(connections)),
            durability: Arc::new(Mutex::new(runtime)),
        };
        (state, job_id, shards, receivers)
    }

    #[tokio::test]
    async fn repair_keeps_old_manifest_until_a_replacement_holder_confirms() {
        let (state, job_id, old, _receivers) = fixture();
        dispatch(&state);
        let task = state
            .durability
            .lock()
            .unwrap()
            .repairs
            .tasks
            .values()
            .next()
            .unwrap()
            .clone();
        assert!(
            complete(&state, task.id, task.worker_node_id, task.worker_session)
                .await
                .is_err()
        );
        assert_eq!(
            state.store.lock().unwrap().jobs[&job_id].outputs[&0].shards,
            old
        );
        crate::custody::tests::confirm(
            &mut state.durability.lock().unwrap().custody,
            &task.target,
            now(),
        );
        assert!(
            complete(&state, task.id, Uuid::new_v4(), task.worker_session)
                .await
                .is_err()
        );
        complete(&state, task.id, task.worker_node_id, task.worker_session)
            .await
            .unwrap();
        {
            let store = state.store.lock().unwrap();
            let output = &store.jobs[&job_id].outputs[&0];
            assert_eq!(output.manifest_revision, 1);
            assert_eq!(output.shards[2], task.target);
            assert_eq!(output.holder_proofs.len(), 3);
            assert_eq!(output.sha256, "a".repeat(64));
        }
        assert!(
            complete(&state, task.id, task.worker_node_id, task.worker_session)
                .await
                .is_err()
        );
        let _ = std::fs::remove_file(state.data_file.as_path());
    }

    #[tokio::test]
    async fn failed_persistence_and_expired_repairs_cannot_advance_the_manifest() {
        let (mut state, job_id, old, _receivers) = fixture();
        let blocker = std::env::temp_dir().join(format!("mesh-repair-blocker-{}", Uuid::new_v4()));
        std::fs::write(&blocker, b"not a directory").unwrap();
        state.data_file = Arc::new(blocker.join("state.json"));
        dispatch(&state);
        let task = state
            .durability
            .lock()
            .unwrap()
            .repairs
            .tasks
            .values()
            .next()
            .unwrap()
            .clone();
        crate::custody::tests::confirm(
            &mut state.durability.lock().unwrap().custody,
            &task.target,
            now(),
        );
        assert!(
            complete(&state, task.id, task.worker_node_id, task.worker_session)
                .await
                .is_err()
        );
        assert_eq!(
            state.store.lock().unwrap().jobs[&job_id].outputs[&0].shards,
            old
        );
        state
            .durability
            .lock()
            .unwrap()
            .repairs
            .tasks
            .get_mut(&task.id)
            .unwrap()
            .expires_at = now() - 1;
        assert!(
            complete(&state, task.id, task.worker_node_id, task.worker_session)
                .await
                .is_err()
        );
        assert_eq!(
            state.store.lock().unwrap().jobs[&job_id].outputs[&0].manifest_revision,
            0
        );
        let _ = std::fs::remove_file(blocker);
    }

    #[test]
    fn repair_waits_for_enough_sources_and_a_distinct_spare_holder() {
        let (state, _, shards, _receivers) = fixture();
        state
            .store
            .lock()
            .unwrap()
            .nodes
            .get_mut(&shards[1].node_id)
            .unwrap()
            .paused = true;
        dispatch(&state);
        assert!(state.durability.lock().unwrap().repairs.tasks.is_empty());
        state
            .store
            .lock()
            .unwrap()
            .nodes
            .get_mut(&shards[1].node_id)
            .unwrap()
            .paused = false;
        let spare = *state
            .store
            .lock()
            .unwrap()
            .nodes
            .keys()
            .find(|id| !shards.iter().any(|shard| shard.node_id == **id))
            .unwrap();
        state
            .store
            .lock()
            .unwrap()
            .nodes
            .get_mut(&spare)
            .unwrap()
            .paused = true;
        dispatch(&state);
        assert!(state.durability.lock().unwrap().repairs.tasks.is_empty());
    }
}
