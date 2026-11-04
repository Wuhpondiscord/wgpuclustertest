use axum::{
    extract::DefaultBodyLimit,
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, Query, State,
    },
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    fs::OpenOptions,
    io::{Read, Write},
    net::SocketAddr,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Instant, SystemTime, UNIX_EPOCH},
};
use tokio::time::{interval, Duration};
use tower_http::services::ServeDir;
use uuid::Uuid;

use mesh_coordinator::recovery;
mod custody;
mod delivery;
mod repair;
mod security;
mod snapshot;

const IDENTITY_HEADER: &str = "x-mesh-identity";
const INVITE_TTL_SECS: u64 = 7 * 24 * 60 * 60;
const NODE_PING_INTERVAL_SECS: u64 = 15;
const NODE_HEARTBEAT_TIMEOUT_SECS: u64 = 45;
const MAX_STORAGE_CONFLICT_RETRIES: usize = 3;
const JOB_LEASE_DURATION_MS: u64 = 45_000;
const WORKER_RECONNECT_GRACE_MS: u64 = 10_000;
const MAX_JOB_UNITS: u64 = 10_000_000;
const MAX_JOB_BATCHES: u64 = 256;
const MAX_CLUSTER_JOBS: usize = 32;
const MAX_CLUSTER_BATCHES: u64 = 2048;
const MAX_STATE_BYTES: usize = 16 * 1024 * 1024;
const MAX_PEER_ARTIFACT_BYTES: u64 = 8 * 1024 * 1024;
const SHARDING_THRESHOLD_BYTES: u64 = 64 * 1024;

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
enum Role {
    Owner,
    Member,
}

#[derive(Clone, Deserialize, Serialize)]
struct Cluster {
    id: Uuid,
    name: String,
    owner_id: String,
    members: HashMap<String, Role>,
}

#[derive(Clone, Deserialize, Serialize)]
struct Invite {
    cluster_id: Uuid,
    expires_at: u64,
}

#[derive(Clone, Deserialize, Serialize)]
struct Node {
    id: Uuid,
    cluster_id: Uuid,
    name: String,
    owner_id: String,
    token_hash: String,
    paused: bool,
    online: bool,
    last_seen: Option<u64>,
}

#[derive(Clone, Default, Deserialize, Serialize)]
struct Store {
    #[serde(default)]
    accounts: HashMap<String, security::Account>,
    #[serde(default)]
    applications: HashMap<Uuid, security::Application>,
    #[serde(default)]
    revoked_credentials: std::collections::HashSet<String>,
    clusters: HashMap<Uuid, Cluster>,
    invites: HashMap<String, Invite>,
    nodes: HashMap<Uuid, Node>,
    #[serde(default)]
    jobs: HashMap<Uuid, ComputeJob>,
    #[serde(default)]
    recovery: recovery::RecoveryLedger,
}

impl Store {
    fn merge_runtime(&mut self, current: &Self) {
        self.recovery.preserve_runtime_from(&current.recovery);
        for (id, node) in &mut self.nodes {
            if let Some(runtime) = current.nodes.get(id) {
                node.online = runtime.online;
                node.last_seen = runtime.last_seen;
            }
        }
        // Durable removals and pauses must win over runtime lease preservation.
        for id in current.nodes.keys() {
            if !self.nodes.get(id).is_some_and(|node| !node.paused) {
                self.recovery
                    .revoke_worker_leases(&id.to_string(), now_ms());
            }
        }
    }
}

fn valid_artifact_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

#[derive(Clone, Deserialize, Serialize)]
struct ComputeJob {
    id: Uuid,
    cluster_id: Uuid,
    created_at: u64,
    #[serde(default)]
    workload: ComputeWorkload,
    total_units: u64,
    batch_units: u64,
    outputs: HashMap<u64, BatchOutput>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
enum ComputeWorkload {
    #[default]
    CpuHash,
    VectorAdd,
}

#[derive(Clone, Deserialize, Serialize)]
struct BatchOutput {
    sha256: String,
    bytes: u64,
    worker_node_id: Uuid,
    replica_node_id: Uuid,
    committed_at: u64,
    #[serde(default)]
    shards: Vec<ShardReceipt>,
    #[serde(default)]
    holder_proofs: Vec<custody::HolderProof>,
    #[serde(default)]
    manifest_revision: u64,
    #[serde(default)]
    execution: Option<ExecutionReport>,
    #[serde(default)]
    attempt: Option<recovery::AttemptObservation>,
    #[serde(default)]
    committed_at_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ExecutionReport {
    execution_mode: String,
    fallback_reason: Option<String>,
    compute_ms: f64,
    validation_ms: f64,
    encoding_ms: f64,
    transfer_ms: f64,
}
impl ExecutionReport {
    fn valid(&self) -> bool {
        matches!(
            self.execution_mode.as_str(),
            "webgpu" | "cpu-fallback" | "cpu"
        ) && self
            .fallback_reason
            .as_ref()
            .is_none_or(|reason| reason.len() <= 512 && !reason.chars().any(char::is_control))
            && [
                self.compute_ms,
                self.validation_ms,
                self.encoding_ms,
                self.transfer_ms,
            ]
            .iter()
            .all(|ms| ms.is_finite() && *ms >= 0.0 && *ms <= 86_400_000.0)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
struct ShardReceipt {
    shard_index: u8,
    node_id: Uuid,
    artifact_name: String,
    sha256: String,
    bytes: u64,
}

#[derive(Clone)]
struct GitHubStorage {
    token: String,
    repo: String,
    branch: String,
    path: String,
}
#[derive(Clone)]
struct AppState {
    store: Arc<Mutex<Store>>,
    // Serializes durable state changes while GitHub/file persistence runs off the async runtime.
    mutation_lock: Arc<tokio::sync::Mutex<()>>,
    data_file: Arc<PathBuf>,
    snapshot: Arc<snapshot::Codec>,
    github: Option<Arc<GitHubStorage>>,
    github_file_sha: Arc<Mutex<Option<String>>>,
    node_connections: Arc<Mutex<HashMap<Uuid, NodeConnection>>>,
    durability: Arc<Mutex<DurabilityRuntime>>,
}

#[derive(Default)]
struct DurabilityRuntime {
    accounts: security::Runtime,
    custody: custody::Registry,
    repairs: repair::RepairQueue,
    invite_refresh_after: Option<Instant>,
}

#[derive(Clone)]
struct NodeConnection {
    session_id: Uuid,
    cluster_id: Uuid,
    sender: delivery::Outbound,
    receipt_nonce: String,
    receipt_key: Option<String>,
    signal_budget: delivery::SignalBudget,
}

#[derive(Deserialize)]
struct CreateCluster {
    name: String,
}
#[derive(Deserialize)]
struct RenameCluster {
    name: String,
}
#[derive(Deserialize)]
struct CreateNode {
    name: String,
}
#[derive(Deserialize)]
struct CreateJob {
    #[serde(default)]
    workload: ComputeWorkload,
    total_units: u64,
    batch_units: u64,
}
#[derive(Deserialize)]
struct ClaimJob {
    node_id: Uuid,
}
#[derive(Deserialize)]
struct RenewJob {
    lease: recovery::TaskLease,
}
#[derive(Deserialize)]
struct CommitBatch {
    lease: recovery::TaskLease,
    #[serde(default)]
    replica_node_id: Option<Uuid>,
    #[serde(default)]
    shards: Vec<ShardReceipt>,
    sha256: String,
    bytes: u64,
    #[serde(default)]
    execution: Option<ExecutionReport>,
}
#[derive(Deserialize)]
struct JoinInvite {
    token: String,
}
#[derive(Deserialize)]
struct NodeSocketQuery {
    token: Option<String>,
}
#[derive(Serialize)]
struct ClusterSummary {
    id: Uuid,
    name: String,
    role: Role,
    members: usize,
    nodes: usize,
    online_nodes: usize,
}
#[derive(Serialize)]
struct NodeSummary {
    id: Uuid,
    cluster_id: Uuid,
    name: String,
    paused: bool,
    online: bool,
    last_seen: Option<u64>,
    holder_ready: bool,
}
#[derive(Serialize)]
struct NodeCreated {
    node: NodeSummary,
    token: String,
}
#[derive(Serialize)]
struct CheckpointSummary {
    batch_index: u64,
    sha256: String,
    bytes: u64,
    shards: Vec<ShardReceipt>,
    available_shards: usize,
    verified_holders: usize,
    durability: String,
    manifest_revision: u64,
    worker_node_id: Uuid,
    committed_at_ms: u64,
    execution: Option<ExecutionReport>,
    attempt: Option<recovery::AttemptObservation>,
}
#[derive(Serialize)]
struct JobSummary {
    id: Uuid,
    cluster_id: Uuid,
    created_at: u64,
    workload: ComputeWorkload,
    total_units: u64,
    batch_units: u64,
    total_batches: u64,
    completed_batches: usize,
    checkpoints: Vec<CheckpointSummary>,
}
#[derive(Serialize)]
struct JobLease {
    job_id: Uuid,
    batch_index: u64,
    start: u64,
    end: u64,
    lease: recovery::TaskLease,
}
#[derive(Serialize)]
struct InvitePreview {
    cluster_id: Uuid,
    cluster_name: String,
    expires_at: u64,
}
#[derive(Serialize)]
struct MemberSummary {
    id: String,
    role: Role,
}
#[derive(Serialize)]
struct InviteSummary {
    token: String,
    expires_at: u64,
}
#[derive(Serialize)]
struct InviteCreated {
    token: String,
    expires_at: u64,
}

#[derive(Debug)]
struct ApiError(StatusCode, String);
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.0,
            Json(serde_json::json!({"source":"mesh-coordinator","error":self.1})),
        )
            .into_response()
    }
}
type ApiResult<T> = Result<T, ApiError>;

fn should_retry_storage_conflict(
    error: &ApiError,
    attempt: usize,
    github_configured: bool,
) -> bool {
    github_configured && error.0 == StatusCode::CONFLICT && attempt < MAX_STORAGE_CONFLICT_RETRIES
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
fn identity(headers: &HeaderMap) -> ApiResult<String> {
    // The router strips this header from incoming traffic and resolves it itself.
    if let Some(user) = headers
        .get(security::USER_HEADER)
        .and_then(|v| v.to_str().ok())
    {
        return Ok(user.to_owned());
    }
    let id = headers
        .get(IDENTITY_HEADER)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .trim();
    if id.len() < 32 || id.len() > 128 {
        return Err(ApiError(
            StatusCode::UNAUTHORIZED,
            "A valid Mesh identity is required".into(),
        ));
    }
    Ok(secret_hash(id))
}
fn secret_hash(value: &str) -> String {
    format!("{:x}", Sha256::digest(value.as_bytes()))
}
fn coordinator_epoch(session: Uuid) -> u64 {
    // Leases cross the JSON/JavaScript boundary. Keep the opaque random token
    // exactly representable by Number so browsers return the same fencing token.
    (session.as_u64_pair().0 & ((1_u64 << 53) - 1)).max(1)
}
fn cluster_name(name: &str) -> Option<String> {
    let name = name.trim();
    (!name.is_empty() && name.chars().count() <= 80).then(|| name.to_owned())
}
fn summary(cluster: &Cluster, store: &Store, user: &str) -> ClusterSummary {
    let cluster_nodes: Vec<&Node> = store
        .nodes
        .values()
        .filter(|node| node.cluster_id == cluster.id)
        .collect();
    ClusterSummary {
        id: cluster.id,
        name: cluster.name.clone(),
        role: *cluster.members.get(user).unwrap_or(&Role::Member),
        members: cluster.members.len(),
        nodes: cluster_nodes.len(),
        online_nodes: cluster_nodes
            .iter()
            .filter(|node| node.online && !node.paused)
            .count(),
    }
}
fn require_member<'a>(store: &'a Store, cluster_id: Uuid, user: &str) -> ApiResult<&'a Cluster> {
    store
        .clusters
        .get(&cluster_id)
        .filter(|cluster| cluster.members.contains_key(user))
        .ok_or(ApiError(StatusCode::NOT_FOUND, "Cluster not found".into()))
}
fn require_owner<'a>(store: &'a Store, cluster_id: Uuid, user: &str) -> ApiResult<&'a Cluster> {
    let cluster = require_member(store, cluster_id, user)?;
    if cluster.owner_id != user {
        return Err(ApiError(
            StatusCode::FORBIDDEN,
            "Only the cluster owner can do that".into(),
        ));
    }
    Ok(cluster)
}
fn node_summary(node: &Node) -> NodeSummary {
    NodeSummary {
        id: node.id,
        cluster_id: node.cluster_id,
        name: node.name.clone(),
        paused: node.paused,
        online: node.online,
        last_seen: node.last_seen,
        holder_ready: false,
    }
}
fn job_summary(job: &ComputeJob, store: &Store) -> JobSummary {
    let mut checkpoints = job
        .outputs
        .iter()
        .map(|(batch_index, output)| {
            let available_shards = output
                .shards
                .iter()
                .filter(|shard| {
                    store
                        .nodes
                        .get(&shard.node_id)
                        .is_some_and(|node| node.online && !node.paused)
                })
                .count();
            let threshold = if output.shards.len() == 3 { 2 } else { 1 };
            CheckpointSummary {
                batch_index: *batch_index,
                worker_node_id: output.worker_node_id,
                committed_at_ms: if output.committed_at_ms == 0 {
                    output.committed_at.saturating_mul(1000)
                } else {
                    output.committed_at_ms
                },
                execution: output.execution.clone(),
                attempt: output.attempt.clone(),
                sha256: output.sha256.clone(),
                bytes: output.bytes,
                shards: output.shards.clone(),
                available_shards,
                verified_holders: output.holder_proofs.len(),
                manifest_revision: output.manifest_revision,
                durability: if available_shards == output.shards.len() {
                    "healthy"
                } else if available_shards >= threshold {
                    "degraded"
                } else {
                    "unavailable"
                }
                .into(),
            }
        })
        .collect::<Vec<_>>();
    checkpoints.sort_by_key(|checkpoint| checkpoint.batch_index);
    JobSummary {
        id: job.id,
        cluster_id: job.cluster_id,
        created_at: job.created_at,
        workload: job.workload,
        total_units: job.total_units,
        batch_units: job.batch_units,
        total_batches: job.total_units.div_ceil(job.batch_units),
        completed_batches: job.outputs.len(),
        checkpoints,
    }
}
fn persist(state: &AppState, store: &Store) -> ApiResult<()> {
    let serialized = state.snapshot.encode(store).map_err(|message| {
        let status = if message.contains("16 MiB") {
            StatusCode::INSUFFICIENT_STORAGE
        } else {
            StatusCode::INTERNAL_SERVER_ERROR
        };
        ApiError(status, message)
    })?;
    if let Some(github) = state.github.as_deref() {
        let agent = ureq::AgentBuilder::new()
            .timeout(Duration::from_secs(20))
            .build();
        let mut file_sha = state.github_file_sha.lock().map_err(|_| {
            ApiError(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Storage unavailable".into(),
            )
        })?;
        let url = format!(
            "https://api.github.com/repos/{}/contents/{}",
            github.repo, github.path
        );
        let mut body = serde_json::json!({
            "message": "Update Mesh cluster state",
            "content": BASE64.encode(&serialized),
            "branch": github.branch,
        });
        if let Some(sha) = file_sha.as_ref() {
            body["sha"] = serde_json::Value::String(sha.clone());
        }
        let response = agent.put(&url)
            .set("Authorization", &format!("Bearer {}", github.token))
            .set("Accept", "application/vnd.github+json")
            .set("X-GitHub-Api-Version", "2022-11-28")
            .set("User-Agent", "mesh-cluster-coordinator")
            .send_json(&body)
            .map_err(|error| {
                let message = match error {
                    ureq::Error::Status(409, _) | ureq::Error::Status(422, _) => "Cluster state revision conflict",
                    ureq::Error::Status(401, _) | ureq::Error::Status(403, _) => "GitHub rejected the storage credentials or repository permissions.",
                    _ => "Could not save cluster state to the private GitHub storage repository. Check connectivity and retry.",
                };
                let status = if message == "Cluster state revision conflict" { StatusCode::CONFLICT } else { StatusCode::SERVICE_UNAVAILABLE };
                ApiError(status, message.into())
            })?;
        let result: serde_json::Value = response.into_json().map_err(|_| {
            ApiError(
                StatusCode::BAD_GATEWAY,
                "GitHub returned an invalid storage response".into(),
            )
        })?;
        *file_sha = result
            .pointer("/content/sha")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned);
        if file_sha.is_none() {
            return Err(ApiError(
                StatusCode::BAD_GATEWAY,
                "GitHub did not return the saved state file revision".into(),
            ));
        }
        return Ok(());
    }
    if let Some(parent) = state
        .data_file
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent).map_err(|_| {
            ApiError(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Could not create data directory".into(),
            )
        })?;
    }
    let temp_path = state
        .data_file
        .with_extension(format!("{}.tmp", Uuid::new_v4()));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut temp = options.open(&temp_path).map_err(|_| {
        ApiError(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Could not open temporary state file".into(),
        )
    })?;
    let written = temp
        .write_all(&serialized)
        .and_then(|_| temp.sync_all())
        .map_err(|_| {
            ApiError(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Could not write temporary state file".into(),
            )
        });
    drop(temp);
    if let Err(error) = written {
        let _ = std::fs::remove_file(&temp_path);
        return Err(error);
    }
    let replaced = std::fs::rename(&temp_path, state.data_file.as_path()).map_err(|_| {
        ApiError(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Could not atomically replace persisted state".into(),
        )
    });
    if replaced.is_err() {
        let _ = std::fs::remove_file(&temp_path);
    }
    replaced
}

fn finish_storage_startup(state: &AppState, needs_migration: bool) -> ApiResult<()> {
    if needs_migration {
        let store = state.store.lock().map_err(|_| {
            ApiError(
                StatusCode::SERVICE_UNAVAILABLE,
                "Startup store unavailable".into(),
            )
        })?;
        persist(state, &store)?;
    }
    state.snapshot.finish_migration();
    Ok(())
}

async fn mutate_store<R, F>(state: &AppState, change: F) -> ApiResult<R>
where
    R: Send,
    F: FnMut(&mut Store) -> ApiResult<R>,
{
    let _mutation = state.mutation_lock.lock().await;
    let mut next = state
        .store
        .lock()
        .map_err(|_| {
            ApiError(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Store unavailable".into(),
            )
        })?
        .clone();
    let mut change = change;
    for attempt in 0..=MAX_STORAGE_CONFLICT_RETRIES {
        let result = change(&mut next)?;
        next.invites.retain(|_, invite| invite.expires_at > now());
        let persist_state = state.clone();
        let persistence_snapshot = next.clone();
        let persisted =
            tokio::task::spawn_blocking(move || persist(&persist_state, &persistence_snapshot))
                .await
                .map_err(|_| {
                    ApiError(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "Storage worker stopped unexpectedly".into(),
                    )
                })?;
        if let Err(error) = persisted {
            if !should_retry_storage_conflict(&error, attempt, state.github.is_some()) {
                return Err(error);
            }
            let config = state.github.clone().expect("GitHub storage is configured");
            let snapshot = state.snapshot.clone();
            let (mut latest, latest_sha, _) =
                tokio::task::spawn_blocking(move || load_github_store(&config, &snapshot))
                    .await
                    .map_err(|_| {
                        ApiError(
                            StatusCode::SERVICE_UNAVAILABLE,
                            "Storage worker stopped unexpectedly".into(),
                        )
                    })?
                    .map_err(|_| {
                        ApiError(
                            StatusCode::BAD_GATEWAY,
                            "Could not reload the latest GitHub state after a write conflict"
                                .into(),
                        )
                    })?;
            {
                let current = state.store.lock().map_err(|_| {
                    ApiError(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "Store unavailable".into(),
                    )
                })?;
                latest.merge_runtime(&current);
            }
            *state.github_file_sha.lock().map_err(|_| {
                ApiError(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Storage unavailable".into(),
                )
            })? = latest_sha;
            next = latest;
            continue;
        }

        let mut current = state.store.lock().map_err(|_| {
            ApiError(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Store unavailable".into(),
            )
        })?;
        // Heartbeats are runtime-only state. Preserve them when committing a durable snapshot.
        next.merge_runtime(&current);
        *current = next;
        return Ok(result);
    }
    Err(ApiError(
        StatusCode::CONFLICT,
        "Cluster state kept changing. Please retry.".into(),
    ))
}

async fn refresh_invite_from_github(state: &AppState, token: &str) -> ApiResult<bool> {
    let Some(config) = state.github.clone() else {
        return Ok(false);
    };
    if token.len() != 64 || !token.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Ok(false);
    }
    {
        let mut runtime = state.durability.lock().map_err(|_| {
            ApiError(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Invite refresh unavailable".into(),
            )
        })?;
        if !reserve_invite_refresh(&mut runtime.invite_refresh_after, Instant::now()) {
            return Err(ApiError(
                StatusCode::TOO_MANY_REQUESTS,
                "Invite refresh is cooling down; retry in one minute".into(),
            ));
        }
    }
    // Never hold the durable write lock during an unauthenticated network read.
    // Compare revisions before swapping so this fetch cannot overwrite a newer write.
    let previous_sha = state
        .github_file_sha
        .lock()
        .map_err(|_| {
            ApiError(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Storage unavailable".into(),
            )
        })?
        .clone();
    let key = secret_hash(token);
    if state
        .store
        .lock()
        .map_err(|_| {
            ApiError(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Store unavailable".into(),
            )
        })?
        .invites
        .get(&key)
        .is_some_and(|invite| invite.expires_at > now())
    {
        return Ok(true);
    }

    let snapshot = state.snapshot.clone();
    let (fresh_store, fresh_sha, _) =
        tokio::task::spawn_blocking(move || load_github_store(&config, &snapshot))
            .await
            .map_err(|_| {
                ApiError(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "Storage worker stopped unexpectedly".into(),
                )
            })?
            .map_err(|_| {
                ApiError(
                    StatusCode::BAD_GATEWAY,
                    "Could not refresh invite state from GitHub storage".into(),
                )
            })?;
    let found = fresh_store
        .invites
        .get(&key)
        .is_some_and(|invite| invite.expires_at > now());
    if found {
        let _mutation = state.mutation_lock.try_lock().map_err(|_| {
            ApiError(
                StatusCode::SERVICE_UNAVAILABLE,
                "Cluster state is being updated; retry the invite shortly".into(),
            )
        })?;
        if *state.github_file_sha.lock().map_err(|_| {
            ApiError(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Storage unavailable".into(),
            )
        })? != previous_sha
        {
            return has_active_invite(state, token);
        }
        let mut store = state.store.lock().map_err(|_| {
            ApiError(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Store unavailable".into(),
            )
        })?;
        let mut fresh_store = fresh_store;
        fresh_store.merge_runtime(&store);
        *store = fresh_store;
        let mut file_sha = state.github_file_sha.lock().map_err(|_| {
            ApiError(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Storage unavailable".into(),
            )
        })?;
        *file_sha = fresh_sha;
    }
    Ok(found)
}

fn reserve_invite_refresh(next: &mut Option<Instant>, at: Instant) -> bool {
    if next.is_some_and(|deadline| at < deadline) {
        return false;
    }
    *next = Some(at + Duration::from_secs(60));
    true
}

fn has_active_invite(state: &AppState, token: &str) -> ApiResult<bool> {
    let store = state.store.lock().map_err(|_| {
        ApiError(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Store unavailable".into(),
        )
    })?;
    Ok(store
        .invites
        .get(&secret_hash(token))
        .is_some_and(|invite| invite.expires_at > now()))
}

fn load_github_store(
    config: &GitHubStorage,
    codec: &snapshot::Codec,
) -> Result<(Store, Option<String>, bool), String> {
    let repo_url = format!("https://api.github.com/repos/{}", config.repo);
    let agent = ureq::AgentBuilder::new()
        .timeout(Duration::from_secs(20))
        .build();
    let repo_response = agent.get(&repo_url)
        .set("Authorization", &format!("Bearer {}", config.token))
        .set("Accept", "application/vnd.github+json")
        .set("X-GitHub-Api-Version", "2022-11-28")
        .set("User-Agent", "mesh-cluster-coordinator")
        .call()
        .map_err(|_| "Could not access the configured GitHub storage repository. Check GH_TOKEN and MESH_STORAGE_REPO.".to_string())?;
    let repo_info: serde_json::Value = repo_response
        .into_json()
        .map_err(|_| "GitHub returned invalid repository metadata".to_string())?;
    if repo_info
        .get("private")
        .and_then(serde_json::Value::as_bool)
        != Some(true)
    {
        return Err("MESH_STORAGE_REPO must point to a private GitHub repository".into());
    }
    let url = format!(
        "https://api.github.com/repos/{}/contents/{}?ref={}",
        config.repo, config.path, config.branch
    );
    let response = agent
        .get(&url)
        .set("Authorization", &format!("Bearer {}", config.token))
        .set("Accept", "application/vnd.github.object+json")
        .set("X-GitHub-Api-Version", "2022-11-28")
        .set("User-Agent", "mesh-cluster-coordinator")
        .call();
    let response = match response {
        Ok(response) => response,
        Err(ureq::Error::Status(404, _)) => return Ok((Store::default(), None, false)),
        Err(_) => return Err("Could not load state from the private GitHub storage repository. Check GH_TOKEN permissions, repo name, and branch.".into()),
    };
    let result: serde_json::Value = response
        .into_json()
        .map_err(|_| "GitHub returned an invalid storage response".to_string())?;
    let sha = result
        .get("sha")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| "GitHub state file has no revision".to_string())?;
    let bytes = decode_github_snapshot(&result, || {
        // Fetch by immutable blob SHA, not by a branch that could change between requests.
        let blob_url = format!(
            "https://api.github.com/repos/{}/git/blobs/{sha}",
            config.repo
        );
        let response = agent
            .get(&blob_url)
            .set("Authorization", &format!("Bearer {}", config.token))
            .set("Accept", "application/vnd.github.raw+json")
            .set("X-GitHub-Api-Version", "2022-11-28")
            .set("User-Agent", "mesh-cluster-coordinator")
            .call()
            .map_err(|_| "Could not load the GitHub state blob".to_string())?;
        let mut bytes = Vec::new();
        response
            .into_reader()
            .take((snapshot::MAX_FILE_BYTES + 1) as u64)
            .read_to_end(&mut bytes)
            .map_err(|_| "Could not read the GitHub state blob".to_string())?;
        Ok(bytes)
    })?;
    let (store, migration) = codec.decode(&bytes)?;
    Ok((store, Some(sha), migration))
}

fn decode_github_snapshot(
    metadata: &serde_json::Value,
    blob: impl FnOnce() -> Result<Vec<u8>, String>,
) -> Result<Vec<u8>, String> {
    if metadata
        .get("size")
        .and_then(serde_json::Value::as_u64)
        .is_some_and(|size| size > snapshot::MAX_FILE_BYTES as u64)
    {
        return Err("GitHub state exceeds the 16 MiB metadata limit".into());
    }
    let bytes = match metadata.get("encoding").and_then(serde_json::Value::as_str) {
        Some("base64") => {
            let content = metadata
                .get("content")
                .and_then(serde_json::Value::as_str)
                .ok_or("GitHub state file has no content")?;
            if content.len() > (snapshot::MAX_FILE_BYTES * 2) {
                return Err("GitHub state content is too large".into());
            }
            BASE64
                .decode(content.replace('\n', ""))
                .map_err(|_| "GitHub state file is not valid base64")?
        }
        Some("none") => blob()?,
        _ => return Err("Unsupported GitHub state encoding".into()),
    };
    if bytes.len() > snapshot::MAX_FILE_BYTES {
        return Err("GitHub state exceeds the encrypted metadata limit".into());
    }
    Ok(bytes)
}

async fn health() -> impl IntoResponse {
    (
        [("x-mesh-state-format", snapshot::FORMAT)],
        "mesh coordinator ready",
    )
}
async fn list_clusters(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> ApiResult<Json<Vec<ClusterSummary>>> {
    let user = identity(&headers)?;
    let store = state.store.lock().map_err(|_| {
        ApiError(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Store unavailable".into(),
        )
    })?;
    Ok(Json(
        store
            .clusters
            .values()
            .filter(|c| c.members.contains_key(&user))
            .map(|c| summary(c, &store, &user))
            .collect(),
    ))
}
async fn create_cluster(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(input): Json<CreateCluster>,
) -> ApiResult<(StatusCode, Json<ClusterSummary>)> {
    let user = identity(&headers)?;
    let name = cluster_name(&input.name).ok_or(ApiError(
        StatusCode::BAD_REQUEST,
        "Name must be 1–80 characters".into(),
    ))?;
    let result = mutate_store(&state, move |store| {
        let mut members = HashMap::new();
        members.insert(user.clone(), Role::Owner);
        let cluster = Cluster {
            id: Uuid::new_v4(),
            name: name.clone(),
            owner_id: user.clone(),
            members,
        };
        store.clusters.insert(cluster.id, cluster.clone());
        Ok(summary(&cluster, store, &user))
    })
    .await?;
    Ok((StatusCode::CREATED, Json(result)))
}
async fn rename_cluster(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
    Json(input): Json<RenameCluster>,
) -> ApiResult<Json<ClusterSummary>> {
    let user = identity(&headers)?;
    let name = cluster_name(&input.name).ok_or(ApiError(
        StatusCode::BAD_REQUEST,
        "Name must be 1–80 characters".into(),
    ))?;
    let result = mutate_store(&state, move |store| {
        require_owner(store, id, &user)?;
        let cluster = store.clusters.get_mut(&id).expect("checked above");
        cluster.name = name.clone();
        let cluster = cluster.clone();
        Ok(summary(&cluster, store, &user))
    })
    .await?;
    Ok(Json(result))
}
async fn list_jobs(
    State(state): State<AppState>,
    Path(cluster_id): Path<Uuid>,
    headers: HeaderMap,
) -> ApiResult<Json<Vec<JobSummary>>> {
    let user = identity(&headers)?;
    let store = state.store.lock().map_err(|_| {
        ApiError(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Store unavailable".into(),
        )
    })?;
    require_member(&store, cluster_id, &user)?;
    Ok(Json(
        store
            .jobs
            .values()
            .filter(|job| job.cluster_id == cluster_id)
            .map(|job| job_summary(job, &store))
            .collect(),
    ))
}
async fn create_job(
    State(state): State<AppState>,
    Path(cluster_id): Path<Uuid>,
    headers: HeaderMap,
    Json(input): Json<CreateJob>,
) -> ApiResult<(StatusCode, Json<JobSummary>)> {
    let user = identity(&headers)?;
    if input.total_units == 0
        || input.total_units > MAX_JOB_UNITS
        || input.batch_units < 1_000
        || input.batch_units > 500_000
        || input.total_units.div_ceil(input.batch_units) > MAX_JOB_BATCHES
    {
        return Err(ApiError(StatusCode::BAD_REQUEST, "Job must contain 1–10,000,000 units, batches of 1,000–500,000 units, and at most 256 batches".into()));
    }
    let job_id = Uuid::new_v4();
    let workload = input.workload;
    let total_units = input.total_units;
    let batch_units = input.batch_units;
    let result = mutate_store(&state, move |store| {
        require_member(store, cluster_id, &user)?;
        let jobs: Vec<_> = store
            .jobs
            .values()
            .filter(|job| job.cluster_id == cluster_id)
            .collect();
        if jobs.len() >= MAX_CLUSTER_JOBS
            || jobs
                .iter()
                .map(|job| job.total_units.div_ceil(job.batch_units))
                .sum::<u64>()
                + total_units.div_ceil(batch_units)
                > MAX_CLUSTER_BATCHES
        {
            return Err(ApiError(
                StatusCode::CONFLICT,
                "This cluster has reached its retained job limit; ask the owner to delete old jobs"
                    .into(),
            ));
        }
        let total_batches = total_units.div_ceil(batch_units);
        for index in 0..total_batches {
            store
                .recovery
                .add_task(format!("{job_id}:{index}"))
                .map_err(|_| {
                    ApiError(
                        StatusCode::CONFLICT,
                        "Could not register a job batch".into(),
                    )
                })?;
        }
        let job = ComputeJob {
            id: job_id,
            cluster_id,
            created_at: now(),
            workload,
            total_units,
            batch_units,
            outputs: HashMap::new(),
        };
        let summary = job_summary(&job, store);
        store.jobs.insert(job_id, job);
        Ok(summary)
    })
    .await?;
    notify_work_available(&state, cluster_id);
    Ok((StatusCode::CREATED, Json(result)))
}

async fn delete_job(
    State(state): State<AppState>,
    Path(job_id): Path<Uuid>,
    headers: HeaderMap,
) -> ApiResult<StatusCode> {
    let user = identity(&headers)?;
    mutate_store(&state, move |store| {
        let job = store
            .jobs
            .get(&job_id)
            .ok_or(ApiError(StatusCode::NOT_FOUND, "Job not found".into()))?;
        require_owner(store, job.cluster_id, &user)?;
        let batches = job.total_units.div_ceil(job.batch_units);
        for index in 0..batches {
            store.recovery.remove_task(&format!("{job_id}:{index}"));
        }
        store.jobs.remove(&job_id);
        Ok(())
    })
    .await?;
    Ok(StatusCode::NO_CONTENT)
}
async fn claim_job_batch(
    State(state): State<AppState>,
    Path(job_id): Path<Uuid>,
    headers: HeaderMap,
    Json(input): Json<ClaimJob>,
) -> ApiResult<Json<Option<JobLease>>> {
    let user = identity(&headers)?;
    let node_id = input.node_id;
    // Claims serialize with durable changes, but never write storage themselves.
    let _mutation = state.mutation_lock.lock().await;
    let mut current = state.store.lock().map_err(|_| {
        ApiError(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Store unavailable".into(),
        )
    })?;
    let result = (|store: &mut Store| {
        let job = store
            .jobs
            .get(&job_id)
            .cloned()
            .ok_or(ApiError(StatusCode::NOT_FOUND, "Job not found".into()))?;
        require_member(store, job.cluster_id, &user)?;
        let node = store
            .nodes
            .get(&node_id)
            .filter(|node| node.cluster_id == job.cluster_id && node.owner_id == user)
            .ok_or(ApiError(
                StatusCode::FORBIDDEN,
                "This node is not owned by your Mesh identity in this cluster".into(),
            ))?;
        if !node.online || node.paused {
            return Err(ApiError(
                StatusCode::CONFLICT,
                "The worker node must be online and active".into(),
            ));
        }
        let now_ms = now_ms();
        let total_batches = job.total_units.div_ceil(job.batch_units);
        for batch_index in 0..total_batches {
            if job.outputs.contains_key(&batch_index) {
                continue;
            }
            let task_id = format!("{job_id}:{batch_index}");
            match store
                .recovery
                .claim(&task_id, node_id.to_string(), now_ms, JOB_LEASE_DURATION_MS)
            {
                Ok(lease) => {
                    let start = batch_index.saturating_mul(job.batch_units);
                    let end = start.saturating_add(job.batch_units).min(job.total_units);
                    return Ok(Some(JobLease {
                        job_id,
                        batch_index,
                        start,
                        end,
                        lease,
                    }));
                }
                Err(recovery::LeaseError::LeaseBusy | recovery::LeaseError::AlreadyComplete) => {
                    continue
                }
                Err(_) => {
                    return Err(ApiError(
                        StatusCode::CONFLICT,
                        "Could not lease this job batch".into(),
                    ))
                }
            }
        }
        Ok(None)
    })(&mut current)?;
    if let Some(claim) = result.as_ref() {
        coordinator_event(
            "batch_claimed",
            serde_json::json!({"job_id":job_id,"worker_node_id":claim.lease.worker_id,"generation":claim.lease.generation,"attempt_id":claim.lease.attempt_id}),
        );
    }
    Ok(Json(result))
}
async fn release_job_batch(
    State(state): State<AppState>,
    Path(job_id): Path<Uuid>,
    headers: HeaderMap,
    Json(input): Json<RenewJob>,
) -> ApiResult<StatusCode> {
    let user = identity(&headers)?;
    let cluster_id = {
        let mut store = state.store.lock().map_err(|_| {
            ApiError(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Store unavailable".into(),
            )
        })?;
        let job = store
            .jobs
            .get(&job_id)
            .ok_or(ApiError(StatusCode::NOT_FOUND, "Job not found".into()))?;
        require_member(&store, job.cluster_id, &user)?;
        let cluster_id = job.cluster_id;
        let worker_id = Uuid::parse_str(&input.lease.worker_id)
            .map_err(|_| ApiError(StatusCode::FORBIDDEN, "Invalid worker".into()))?;
        if !input.lease.task_id.starts_with(&format!("{job_id}:"))
            || !store
                .nodes
                .get(&worker_id)
                .is_some_and(|node| node.cluster_id == cluster_id && node.owner_id == user)
        {
            return Err(ApiError(
                StatusCode::FORBIDDEN,
                "The lease does not belong to this worker and job".into(),
            ));
        }
        store
            .recovery
            .release(&input.lease, now_ms())
            .map_err(|_| {
                ApiError(
                    StatusCode::CONFLICT,
                    "The lease is complete or no longer current".into(),
                )
            })?;
        cluster_id
    };
    notify_work_available(&state, cluster_id);
    Ok(StatusCode::NO_CONTENT)
}

fn notify_work_available(state: &AppState, cluster_id: Uuid) {
    if let Ok(connections) = state.node_connections.lock() {
        for connection in connections
            .values()
            .filter(|connection| connection.cluster_id == cluster_id)
        {
            connection
                .sender
                .hint(Message::Text("{\"type\":\"work_available\"}".into()));
        }
    }
}

fn coordinator_event(event: &str, details: serde_json::Value) {
    eprintln!(
        "{}",
        serde_json::json!({"event":event,"server_at_ms":now_ms(),"details":details})
    );
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}

async fn renew_job_batch(
    State(state): State<AppState>,
    Path(job_id): Path<Uuid>,
    headers: HeaderMap,
    Json(input): Json<RenewJob>,
) -> ApiResult<Json<recovery::TaskLease>> {
    let user = identity(&headers)?;
    // Renewal must remain available while a slow durable write is in progress.
    let mut store = state.store.lock().map_err(|_| {
        ApiError(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Store unavailable".into(),
        )
    })?;
    let job = store
        .jobs
        .get(&job_id)
        .ok_or(ApiError(StatusCode::NOT_FOUND, "Job not found".into()))?;
    require_member(&store, job.cluster_id, &user)?;
    let prefix = format!("{job_id}:");
    let batch = input
        .lease
        .task_id
        .strip_prefix(&prefix)
        .and_then(|value| value.parse::<u64>().ok());
    if !batch.is_some_and(|index| index < job.total_units.div_ceil(job.batch_units)) {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "Lease does not belong to this job".into(),
        ));
    }
    let worker_id = Uuid::parse_str(&input.lease.worker_id)
        .map_err(|_| ApiError(StatusCode::FORBIDDEN, "Invalid lease worker".into()))?;
    let node = store
        .nodes
        .get(&worker_id)
        .filter(|node| node.cluster_id == job.cluster_id && node.owner_id == user)
        .ok_or(ApiError(
            StatusCode::FORBIDDEN,
            "The lease worker is not owned by your Mesh identity".into(),
        ))?;
    if node.paused {
        return Err(ApiError(
            StatusCode::CONFLICT,
            "The worker node is paused".into(),
        ));
    }
    if !node.online {
        return Err(ApiError(
            StatusCode::SERVICE_UNAVAILABLE,
            "The worker is reconnecting; retry renewal within its grace period".into(),
        ));
    }
    let renewed = store
        .recovery
        .renew(&input.lease, now_ms(), JOB_LEASE_DURATION_MS)
        .map_err(|_| {
            ApiError(
                StatusCode::CONFLICT,
                "The worker lease expired or was reassigned; claim the batch again".into(),
            )
        })?;
    Ok(Json(renewed))
}
async fn commit_job_batch(
    State(state): State<AppState>,
    Path(job_id): Path<Uuid>,
    headers: HeaderMap,
    Json(input): Json<CommitBatch>,
) -> ApiResult<StatusCode> {
    let user = identity(&headers)?;
    if input
        .execution
        .as_ref()
        .is_some_and(|report| !report.valid())
    {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "Invalid self-reported execution metrics".into(),
        ));
    }
    if input.bytes == 0
        || input.bytes > MAX_PEER_ARTIFACT_BYTES
        || input.sha256.len() != 64
        || !input.sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "Checkpoint artifact metadata is invalid".into(),
        ));
    }
    let durability = state.durability.clone();
    mutate_store(&state, move |store| {
        let job = store
            .jobs
            .get(&job_id)
            .cloned()
            .ok_or(ApiError(StatusCode::NOT_FOUND, "Job not found".into()))?;
        require_member(store, job.cluster_id, &user)?;
        let worker_id = Uuid::parse_str(&input.lease.worker_id)
            .map_err(|_| ApiError(StatusCode::FORBIDDEN, "Invalid lease worker".into()))?;
        let _worker = store
            .nodes
            .get(&worker_id)
            .filter(|node| node.cluster_id == job.cluster_id && node.owner_id == user)
            .ok_or(ApiError(
                StatusCode::FORBIDDEN,
                "The lease worker is not owned by your Mesh identity".into(),
            ))?;
        let prefix = format!("{job_id}:");
        let batch_index = input
            .lease
            .task_id
            .strip_prefix(&prefix)
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or(ApiError(
                StatusCode::BAD_REQUEST,
                "Lease does not belong to this job".into(),
            ))?;
        if batch_index >= job.total_units.div_ceil(job.batch_units) {
            return Err(ApiError(
                StatusCode::BAD_REQUEST,
                "Lease batch is out of range".into(),
            ));
        }
        if let Some(existing) = job.outputs.get(&batch_index) {
            return if existing.sha256 == input.sha256 {
                Ok(())
            } else {
                Err(ApiError(
                    StatusCode::CONFLICT,
                    "This batch already committed a different artifact".into(),
                ))
            };
        }
        // Legacy browser clients may still submit a two-copy receipt. New clients
        // submit two copies for small artifacts and three XOR-coded shards above
        // the threshold.
        let receipts = if input.shards.is_empty() {
            let replica_id = input.replica_node_id.ok_or(ApiError(
                StatusCode::BAD_REQUEST,
                "Checkpoint shard receipts are required".into(),
            ))?;
            vec![
                ShardReceipt {
                    shard_index: 0,
                    node_id: worker_id,
                    artifact_name: format!("legacy-{job_id}-{batch_index}-0"),
                    sha256: input.sha256.clone(),
                    bytes: input.bytes,
                },
                ShardReceipt {
                    shard_index: 1,
                    node_id: replica_id,
                    artifact_name: format!("legacy-{job_id}-{batch_index}-1"),
                    sha256: input.sha256.clone(),
                    bytes: input.bytes,
                },
            ]
        } else {
            input.shards.clone()
        };
        // A large artifact can degrade to two complete copies if a shard peer
        // drops during transfer. Treat only a complete three-receipt manifest
        // as 2+1 coded; two receipts are the replicated recovery policy at any size.
        let erasure_coded = input.shards.len() == 3 && input.bytes >= SHARDING_THRESHOLD_BYTES;
        let expected_shards = if erasure_coded { 3 } else { 2 };
        if receipts.len() != expected_shards
            || receipts.iter().enumerate().any(|(index, shard)| {
                shard.shard_index as usize != index
                    || !valid_artifact_name(&shard.artifact_name)
                    || shard.bytes == 0
                    || shard.bytes > MAX_PEER_ARTIFACT_BYTES
                    || shard.sha256.len() != 64
                    || !shard.sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
            })
            || receipts
                .iter()
                .map(|shard| shard.node_id)
                .collect::<std::collections::HashSet<_>>()
                .len()
                != expected_shards
            || receipts.first().map(|shard| shard.node_id) != Some(worker_id)
        {
            return Err(ApiError(
                StatusCode::BAD_REQUEST,
                "Shard manifest is incomplete or invalid".into(),
            ));
        }
        let expected_shard_bytes = if erasure_coded {
            input.bytes.div_ceil(2)
        } else {
            input.bytes
        };
        if receipts.iter().any(|shard| {
            shard.bytes != expected_shard_bytes
                || (!erasure_coded && shard.sha256.to_lowercase() != input.sha256.to_lowercase())
        }) {
            return Err(ApiError(
                StatusCode::BAD_REQUEST,
                "Shard sizes or replicated digests do not match the checkpoint".into(),
            ));
        }
        if receipts.iter().any(|shard| {
            !store.nodes.get(&shard.node_id).is_some_and(|node| {
                node.cluster_id == job.cluster_id && !node.paused && node.online
            })
        }) {
            return Err(ApiError(
                StatusCode::CONFLICT,
                if erasure_coded {
                    "2+1 sharding requires three distinct active cluster nodes".into()
                } else {
                    "Two active cluster nodes must acknowledge the replicated checkpoint".into()
                },
            ));
        }
        let proofs = durability
            .lock()
            .map_err(|_| {
                ApiError(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Holder registry unavailable".into(),
                )
            })?
            .custody
            .verify_manifest(&input.lease.task_id, input.bytes, &receipts, now())
            .map_err(|message| ApiError(StatusCode::CONFLICT, message))?;
        store
            .recovery
            .commit_result(&input.lease, now_ms(), input.sha256.clone())
            .map_err(|error| match error {
                recovery::LeaseError::LeaseExpired => ApiError(
                    StatusCode::CONFLICT,
                    "The worker lease expired; claim the batch again".into(),
                ),
                recovery::LeaseError::StaleLease => {
                    ApiError(StatusCode::CONFLICT, "This worker lease is stale".into())
                }
                _ => ApiError(
                    StatusCode::CONFLICT,
                    "The result could not be committed".into(),
                ),
            })?;
        let observation = store.recovery.observation(&input.lease.task_id);
        store
            .jobs
            .get_mut(&job_id)
            .expect("job was checked")
            .outputs
            .insert(
                batch_index,
                BatchOutput {
                    execution: input.execution.clone(),
                    attempt: observation,
                    committed_at_ms: now_ms(),
                    sha256: input.sha256.clone(),
                    bytes: input.bytes,
                    worker_node_id: worker_id,
                    replica_node_id: receipts[1].node_id,
                    committed_at: now(),
                    shards: receipts,
                    holder_proofs: proofs,
                    manifest_revision: 0,
                },
            );
        Ok(())
    })
    .await?;
    let cluster_id = state
        .store
        .lock()
        .ok()
        .and_then(|store| store.jobs.get(&job_id).map(|job| job.cluster_id));
    if let Some(cluster_id) = cluster_id {
        coordinator_event(
            "checkpoint_published",
            serde_json::json!({"job_id":job_id,"cluster_id":cluster_id}),
        );
        notify_work_available(&state, cluster_id);
    }
    Ok(StatusCode::NO_CONTENT)
}
async fn delete_cluster(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
) -> ApiResult<StatusCode> {
    let user = identity(&headers)?;
    let removed_nodes = mutate_store(&state, move |store| {
        require_owner(store, id, &user)?;
        let removed_nodes: Vec<Uuid> = store
            .nodes
            .values()
            .filter(|node| node.cluster_id == id)
            .map(|node| node.id)
            .collect();
        let removed_jobs = store
            .jobs
            .values()
            .filter(|job| job.cluster_id == id)
            .cloned()
            .collect::<Vec<_>>();
        for job in removed_jobs {
            for index in 0..job.total_units.div_ceil(job.batch_units) {
                store.recovery.remove_task(&format!("{}:{index}", job.id));
            }
        }
        store.jobs.retain(|_, job| job.cluster_id != id);
        store
            .applications
            .retain(|_, application| application.cluster_id != id);
        store.clusters.remove(&id);
        store.nodes.retain(|_, node| node.cluster_id != id);
        store.invites.retain(|_, inv| inv.cluster_id != id);
        Ok(removed_nodes)
    })
    .await?;
    end_removed_node_sessions(&state, &removed_nodes);
    Ok(StatusCode::NO_CONTENT)
}
async fn create_invite(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
) -> ApiResult<(StatusCode, Json<InviteCreated>)> {
    let user = identity(&headers)?;
    let result = mutate_store(&state, move |store| {
        require_owner(store, id, &user)?;
        store.invites.retain(|_, invite| invite.expires_at > now());
        let token = security::new_key()?;
        let expires_at = now() + INVITE_TTL_SECS;
        store.invites.insert(
            secret_hash(&token),
            Invite {
                cluster_id: id,
                expires_at,
            },
        );
        Ok(InviteCreated { token, expires_at })
    })
    .await?;
    Ok((StatusCode::CREATED, Json(result)))
}
async fn list_invites(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
) -> ApiResult<Json<Vec<InviteSummary>>> {
    let user = identity(&headers)?;
    let store = state.store.lock().map_err(|_| {
        ApiError(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Store unavailable".into(),
        )
    })?;
    require_owner(&store, id, &user)?;
    let invites = store
        .invites
        .iter()
        .filter(|(_, invite)| invite.cluster_id == id && invite.expires_at > now())
        .map(|(token, invite)| InviteSummary {
            token: token.clone(),
            expires_at: invite.expires_at,
        })
        .collect();
    Ok(Json(invites))
}
async fn revoke_invite(
    State(state): State<AppState>,
    Path((id, token)): Path<(Uuid, String)>,
    headers: HeaderMap,
) -> ApiResult<StatusCode> {
    let user = identity(&headers)?;
    mutate_store(&state, move |store| {
        require_owner(store, id, &user)?;
        if !store
            .invites
            .get(&token)
            .is_some_and(|invite| invite.cluster_id == id)
        {
            return Err(ApiError(StatusCode::NOT_FOUND, "Invite not found".into()));
        }
        store.invites.remove(&token);
        Ok(())
    })
    .await?;
    Ok(StatusCode::NO_CONTENT)
}
async fn preview_invite(
    State(state): State<AppState>,
    Path(token): Path<String>,
) -> ApiResult<Json<InvitePreview>> {
    if !has_active_invite(&state, &token)? && !refresh_invite_from_github(&state, &token).await? {
        return Err(ApiError(
            StatusCode::NOT_FOUND,
            "Invite is invalid or expired".into(),
        ));
    }
    let store = state.store.lock().map_err(|_| {
        ApiError(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Store unavailable".into(),
        )
    })?;
    let invite = store
        .invites
        .get(&secret_hash(&token))
        .filter(|i| i.expires_at > now())
        .ok_or(ApiError(
            StatusCode::NOT_FOUND,
            "Invite is invalid or expired".into(),
        ))?;
    let cluster = store
        .clusters
        .get(&invite.cluster_id)
        .ok_or(ApiError(StatusCode::NOT_FOUND, "Cluster not found".into()))?;
    Ok(Json(InvitePreview {
        cluster_id: cluster.id,
        cluster_name: cluster.name.clone(),
        expires_at: invite.expires_at,
    }))
}
async fn join_invite(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(input): Json<JoinInvite>,
) -> ApiResult<Json<ClusterSummary>> {
    let user = identity(&headers)?;
    if !has_active_invite(&state, &input.token)?
        && !refresh_invite_from_github(&state, &input.token).await?
    {
        return Err(ApiError(
            StatusCode::NOT_FOUND,
            "Invite is invalid or expired".into(),
        ));
    }
    let result = mutate_store(&state, move |store| {
        let invite = store
            .invites
            .get(&secret_hash(&input.token))
            .filter(|i| i.expires_at > now())
            .cloned()
            .ok_or(ApiError(
                StatusCode::NOT_FOUND,
                "Invite is invalid or expired".into(),
            ))?;
        let cluster = store
            .clusters
            .get_mut(&invite.cluster_id)
            .ok_or(ApiError(StatusCode::NOT_FOUND, "Cluster not found".into()))?;
        cluster.members.entry(user.clone()).or_insert(Role::Member);
        let cluster = cluster.clone();
        Ok(summary(&cluster, store, &user))
    })
    .await?;
    Ok(Json(result))
}
async fn leave_cluster(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
) -> ApiResult<StatusCode> {
    let user = identity(&headers)?;
    let removed = mutate_store(&state, move |store| {
        let cluster = require_member(store, id, &user)?;
        if cluster.owner_id == user {
            return Err(ApiError(
                StatusCode::BAD_REQUEST,
                "Transfer ownership or delete this cluster before leaving".into(),
            ));
        }
        store
            .clusters
            .get_mut(&id)
            .expect("checked above")
            .members
            .remove(&user);
        for application in store
            .applications
            .values_mut()
            .filter(|a| a.cluster_id == id && a.owner_id != user)
        {
            application.members.remove(&user);
        }
        let removed: Vec<_> = store
            .nodes
            .values()
            .filter(|node| node.cluster_id == id && node.owner_id == user)
            .map(|node| node.id)
            .collect();
        store
            .nodes
            .retain(|_, node| node.cluster_id != id || node.owner_id != user);
        Ok(removed)
    })
    .await?;
    end_removed_node_sessions(&state, &removed);
    Ok(StatusCode::NO_CONTENT)
}

async fn list_members(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
) -> ApiResult<Json<Vec<MemberSummary>>> {
    let user = identity(&headers)?;
    let store = state.store.lock().map_err(|_| {
        ApiError(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Store unavailable".into(),
        )
    })?;
    let cluster = require_member(&store, id, &user)?;
    Ok(Json(
        cluster
            .members
            .iter()
            .map(|(id, role)| MemberSummary {
                id: format!("member-{}", id.get(..8).unwrap_or(id)),
                role: *role,
            })
            .collect(),
    ))
}
async fn remove_member(
    State(state): State<AppState>,
    Path((id, member_id)): Path<(Uuid, String)>,
    headers: HeaderMap,
) -> ApiResult<StatusCode> {
    let user = identity(&headers)?;
    let removed_nodes = mutate_store(&state, move |store| {
        require_owner(store, id, &user)?;
        let cluster = store.clusters.get_mut(&id).expect("checked above");
        let target = cluster
            .members
            .keys()
            .find(|key| {
                key.get(..8)
                    .is_some_and(|prefix| format!("member-{prefix}") == member_id)
            })
            .cloned()
            .ok_or(ApiError(StatusCode::NOT_FOUND, "Member not found".into()))?;
        if target == cluster.owner_id {
            return Err(ApiError(
                StatusCode::BAD_REQUEST,
                "The owner cannot be removed".into(),
            ));
        }
        cluster.members.remove(&target);
        for application in store
            .applications
            .values_mut()
            .filter(|a| a.cluster_id == id && a.owner_id != target)
        {
            application.members.remove(&target);
        }
        let removed_nodes: Vec<Uuid> = store
            .nodes
            .values()
            .filter(|node| node.cluster_id == id && node.owner_id == target)
            .map(|node| node.id)
            .collect();
        store
            .nodes
            .retain(|_, node| node.cluster_id != id || node.owner_id != target);
        Ok(removed_nodes)
    })
    .await?;
    end_removed_node_sessions(&state, &removed_nodes);
    Ok(StatusCode::NO_CONTENT)
}
async fn list_nodes(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
) -> ApiResult<Json<Vec<NodeSummary>>> {
    let user = identity(&headers)?;
    let ready: std::collections::HashSet<_> = state
        .node_connections
        .lock()
        .map_err(|_| {
            ApiError(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Connection registry unavailable".into(),
            )
        })?
        .iter()
        .filter(|(_, connection)| connection.receipt_key.is_some())
        .map(|(id, _)| *id)
        .collect();
    let store = state.store.lock().map_err(|_| {
        ApiError(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Store unavailable".into(),
        )
    })?;
    require_member(&store, id, &user)?;
    Ok(Json(
        store
            .nodes
            .values()
            .filter(|n| n.cluster_id == id)
            .map(|node| {
                let mut summary = node_summary(node);
                summary.holder_ready = ready.contains(&node.id);
                summary
            })
            .collect(),
    ))
}
async fn create_node(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
    Json(input): Json<CreateNode>,
) -> ApiResult<(StatusCode, Json<NodeCreated>)> {
    let user = identity(&headers)?;
    let name = cluster_name(&input.name).ok_or(ApiError(
        StatusCode::BAD_REQUEST,
        "Node name must be 1–80 characters".into(),
    ))?;
    let token = security::new_key()?;
    let node_id = Uuid::new_v4();
    let token_hash = secret_hash(&token);
    let token_for_response = token.clone();
    let result = mutate_store(&state, move |store| {
        require_member(store, id, &user)?;
        let node = Node {
            id: node_id,
            cluster_id: id,
            name: name.clone(),
            owner_id: user.clone(),
            token_hash: token_hash.clone(),
            paused: false,
            online: false,
            last_seen: None,
        };
        let node_summary = node_summary(&node);
        store.nodes.insert(node.id, node);
        Ok(NodeCreated {
            node: node_summary,
            token: token_for_response.clone(),
        })
    })
    .await?;
    Ok((StatusCode::CREATED, Json(result)))
}
async fn set_node_paused(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
    paused: bool,
) -> ApiResult<Json<NodeSummary>> {
    let user = identity(&headers)?;
    let result = mutate_store(&state, move |store| {
        let (cluster_id, node_owner) = store
            .nodes
            .get(&id)
            .map(|node| (node.cluster_id, node.owner_id.clone()))
            .ok_or(ApiError(StatusCode::NOT_FOUND, "Node not found".into()))?;
        let cluster_owner = store
            .clusters
            .get(&cluster_id)
            .ok_or(ApiError(StatusCode::NOT_FOUND, "Cluster not found".into()))?
            .owner_id
            .clone();
        if cluster_owner != user && node_owner != user {
            return Err(ApiError(
                StatusCode::FORBIDDEN,
                "You cannot manage this node".into(),
            ));
        }
        let node = store.nodes.get_mut(&id).expect("checked above");
        node.paused = paused;
        let summary = node_summary(node);
        if paused {
            store
                .recovery
                .revoke_worker_leases(&id.to_string(), now_ms());
        }
        Ok(summary)
    })
    .await?;
    let (node_session, peers) = if let Ok(connections) = state.node_connections.lock() {
        let node_session = connections.get(&id).map(|connection| {
            let control = serde_json::json!({"type":"control","paused":paused}).to_string();
            let _ = connection.sender.try_send(Message::Text(control));
            connection.session_id
        });
        let peers = connections
            .values()
            .filter(|connection| connection.cluster_id == result.cluster_id)
            .filter(|connection| Some(connection.session_id) != node_session)
            .map(|connection| connection.sender.clone())
            .collect::<Vec<_>>();
        (node_session, peers)
    } else {
        (None, Vec::new())
    };
    if paused {
        let event = serde_json::json!({"type":"peer_left","peer_node_id":id}).to_string();
        for peer in peers {
            let _ = peer.try_send(Message::Text(event.clone()));
        }
    } else {
        let _ = node_session;
        announce_peer_presence(&state, id, true);
    }
    Ok(Json(result))
}
async fn pause_node(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
) -> ApiResult<Json<NodeSummary>> {
    set_node_paused(State(state), Path(id), headers, true).await
}
async fn resume_node(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
) -> ApiResult<Json<NodeSummary>> {
    set_node_paused(State(state), Path(id), headers, false).await
}
async fn delete_node(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
) -> ApiResult<StatusCode> {
    let user = identity(&headers)?;
    mutate_store(&state, move |store| {
        let node = store
            .nodes
            .get(&id)
            .ok_or(ApiError(StatusCode::NOT_FOUND, "Node not found".into()))?;
        let cluster = store
            .clusters
            .get(&node.cluster_id)
            .ok_or(ApiError(StatusCode::NOT_FOUND, "Cluster not found".into()))?;
        if cluster.owner_id != user && node.owner_id != user {
            return Err(ApiError(
                StatusCode::FORBIDDEN,
                "You cannot remove this node".into(),
            ));
        }
        store.nodes.remove(&id);
        store
            .recovery
            .revoke_worker_leases(&id.to_string(), now_ms());
        Ok(())
    })
    .await?;
    end_removed_node_sessions(&state, &[id]);
    Ok(StatusCode::NO_CONTENT)
}

fn end_removed_node_sessions(state: &AppState, ids: &[Uuid]) {
    // Acquire connection, durability and store locks in that order when nested.
    // Here each registry is released before entering the next one.
    let removed: Vec<_> = state
        .node_connections
        .lock()
        .map(|mut connections| {
            ids.iter()
                .filter_map(|id| {
                    connections.remove(id).map(|connection| {
                        connection.sender.invalidate();
                        (*id, connection.receipt_nonce)
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    if let Ok(mut runtime) = state.durability.lock() {
        for (id, nonce) in removed {
            runtime.custody.end_session(id, &nonce);
        }
    }
    if let Ok(mut store) = state.store.lock() {
        for id in ids {
            store
                .recovery
                .revoke_worker_leases(&id.to_string(), now_ms());
        }
    }
}
async fn node_socket(
    State(state): State<AppState>,
    Query(query): Query<NodeSocketQuery>,
    ws: WebSocketUpgrade,
) -> ApiResult<Response> {
    let node_id = query
        .token
        .as_deref()
        .map(|token| node_for_token(&state, token))
        .transpose()?;
    Ok(ws
        .max_message_size(64 * 1024)
        .max_frame_size(16 * 1024)
        .on_upgrade(move |mut socket| async move {
            let id = match node_id {
                Some(id) => id, // Compatibility for older clients; new clients authenticate in a frame.
                None => {
                    let token =
                        match tokio::time::timeout(Duration::from_secs(5), socket.recv()).await {
                            Ok(Some(Ok(Message::Text(text)))) => {
                                serde_json::from_str::<serde_json::Value>(&text)
                                    .ok()
                                    .filter(|v| v["type"] == "authenticate")
                                    .and_then(|v| v["token"].as_str().map(str::to_owned))
                            }
                            _ => None,
                        };
                    let Some(id) = token.and_then(|token| node_for_token(&state, &token).ok())
                    else {
                        let _ = socket.close().await;
                        return;
                    };
                    id
                }
            };
            run_node_socket(socket, state, id, Uuid::new_v4()).await;
        }))
}
fn node_for_token(state: &AppState, token: &str) -> ApiResult<Uuid> {
    if token.len() != 64 || !token.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(ApiError(
            StatusCode::UNAUTHORIZED,
            "Invalid node token".into(),
        ));
    }
    state
        .store
        .lock()
        .map_err(|_| ApiError(StatusCode::SERVICE_UNAVAILABLE, "Store unavailable".into()))?
        .nodes
        .values()
        .find(|node| node.token_hash == secret_hash(token))
        .map(|node| node.id)
        .ok_or(ApiError(
            StatusCode::UNAUTHORIZED,
            "Invalid node token".into(),
        ))
}

fn register_node_connection(
    state: &AppState,
    id: Uuid,
    session_id: Uuid,
    sender: impl Into<delivery::Outbound>,
) -> bool {
    let sender = sender.into();
    let cluster_id = if let Ok(store) = state.store.lock() {
        if let Some(node) = store.nodes.get(&id) {
            node.cluster_id
        } else {
            return false;
        }
    } else {
        return false;
    };
    let receipt_nonce = Uuid::new_v4().to_string();
    let inserted = if let Ok(mut connections) = state.node_connections.lock() {
        if let Ok(mut runtime) = state.durability.lock() {
            runtime.custody.begin_session(id, receipt_nonce.clone());
        } else {
            return false;
        }
        let signal_budget = connections
            .get(&id)
            .map(|connection| connection.signal_budget.clone())
            .unwrap_or_default();
        let previous = connections.insert(
            id,
            NodeConnection {
                session_id,
                cluster_id,
                sender,
                receipt_nonce,
                receipt_key: None,
                signal_budget,
            },
        );
        if let Some(previous) = previous {
            previous.sender.invalidate();
        }
        true
    } else {
        false
    };
    if !inserted {
        return false;
    }
    let registered = if let Ok(mut store) = state.store.lock() {
        if let Some(node) = store.nodes.get_mut(&id) {
            node.online = true;
            node.last_seen = Some(now());
            store
                .recovery
                .resume_worker_leases(&id.to_string(), now_ms(), JOB_LEASE_DURATION_MS);
            true
        } else {
            false
        }
    } else {
        false
    };
    // Release the store guard before acquiring connection/durability registries.
    if !registered {
        disconnect_node(state, id, session_id);
    }
    registered
}

fn is_current_node_connection(state: &AppState, id: Uuid, session_id: Uuid) -> bool {
    state
        .node_connections
        .lock()
        .map(|connections| {
            connections
                .get(&id)
                .is_some_and(|connection| connection.session_id == session_id)
        })
        .unwrap_or(false)
}

fn update_node_heartbeat(state: &AppState, id: Uuid, session_id: Uuid) -> bool {
    if !is_current_node_connection(state, id, session_id) {
        return false;
    }
    if let Ok(mut store) = state.store.lock() {
        if let Some(node) = store.nodes.get_mut(&id) {
            node.online = true;
            node.last_seen = Some(now());
            return true;
        }
    }
    false
}

fn announce_peer_presence(state: &AppState, id: Uuid, joined: bool) {
    let active_ids = state
        .store
        .lock()
        .ok()
        .map(|store| {
            store
                .nodes
                .values()
                .filter(|node| !node.paused)
                .map(|node| node.id)
                .collect::<std::collections::HashSet<_>>()
        })
        .unwrap_or_default();
    let Ok(connections) = state.node_connections.lock() else {
        return;
    };
    let Some(current) = connections.get(&id) else {
        return;
    };
    let cluster_id = current.cluster_id;
    let peers = connections
        .iter()
        .filter(|(peer_id, connection)| {
            **peer_id != id && connection.cluster_id == cluster_id && active_ids.contains(peer_id)
        })
        .map(|(peer_id, connection)| (*peer_id, connection.sender.clone()))
        .collect::<Vec<_>>();
    let peer_ids = peers
        .iter()
        .map(|(peer_id, _)| *peer_id)
        .collect::<Vec<_>>();
    if let Ok(payload) =
        serde_json::to_string(&serde_json::json!({"type":"peer_list","peer_node_ids":peer_ids}))
    {
        let _ = current.sender.try_send(Message::Text(payload));
    }
    let event_type = if joined { "peer_joined" } else { "peer_left" };
    let event = serde_json::json!({"type":event_type,"peer_node_id":id}).to_string();
    for (_, sender) in peers {
        let _ = sender.try_send(Message::Text(event.clone()));
    }
}

fn disconnect_node(state: &AppState, id: Uuid, session_id: Uuid) {
    let removed_cluster = if let Ok(mut connections) = state.node_connections.lock() {
        if connections
            .get(&id)
            .is_some_and(|connection| connection.session_id == session_id)
        {
            connections
                .remove(&id)
                .map(|connection| (connection.cluster_id, connection.receipt_nonce))
        } else {
            None
        }
    } else {
        None
    };
    if let Some((cluster_id, receipt_nonce)) = removed_cluster {
        if let Ok(mut runtime) = state.durability.lock() {
            runtime.custody.end_session(id, &receipt_nonce);
        }
        if let Ok(mut store) = state.store.lock() {
            if let Some(node) = store.nodes.get_mut(&id) {
                node.online = false;
                node.last_seen = Some(now());
            }
            store.recovery.shorten_worker_leases(
                &id.to_string(),
                now_ms(),
                WORKER_RECONNECT_GRACE_MS,
            );
            coordinator_event(
                "node_disconnected",
                serde_json::json!({"node_id":id,"grace_ms":WORKER_RECONNECT_GRACE_MS}),
            );
        }
        // Tell remaining same-cluster workers to close stale peer channels.
        let event = serde_json::json!({"type":"peer_left","peer_node_id":id}).to_string();
        if let Ok(connections) = state.node_connections.lock() {
            for connection in connections
                .values()
                .filter(|connection| connection.cluster_id == cluster_id)
            {
                let _ = connection.sender.try_send(Message::Text(event.clone()));
            }
        }
    }
}

async fn route_peer_signal(state: &AppState, from_id: Uuid, session_id: Uuid, text: &str) {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(text) else {
        return;
    };
    if value.get("type").and_then(serde_json::Value::as_str) != Some("signal") {
        return;
    }
    let Some(peer_id) = value
        .get("peer_node_id")
        .and_then(serde_json::Value::as_str)
        .and_then(|id| Uuid::parse_str(id).ok())
    else {
        return;
    };
    let Some(payload) = value.get("payload") else {
        return;
    };
    let Ok(payload_bytes) = serde_json::to_vec(payload) else {
        return;
    };
    if payload_bytes.len() > 32 * 1024 || peer_id == from_id {
        return;
    }
    let same_active_cluster = state.store.lock().ok().is_some_and(|store| {
        store.nodes.get(&from_id).is_some_and(|from| {
            !from.paused
                && store
                    .nodes
                    .get(&peer_id)
                    .is_some_and(|to| !to.paused && to.cluster_id == from.cluster_id)
        })
    });
    if !same_active_cluster {
        return;
    }
    let Ok(mut connections) = state.node_connections.lock() else {
        return;
    };
    let Some(from) = connections
        .get(&from_id)
        .filter(|connection| connection.session_id == session_id)
    else {
        return;
    };
    let Some(to) = connections
        .get(&peer_id)
        .filter(|connection| connection.cluster_id == from.cluster_id)
    else {
        return;
    };
    let sender = to.sender.clone();
    if !connections.get_mut(&from_id).is_some_and(|from| {
        from.signal_budget
            .allow(payload_bytes.len(), Instant::now())
    }) {
        return;
    }
    drop(connections);
    let forwarded =
        serde_json::json!({"type":"signal","peer_node_id":from_id,"payload":payload}).to_string();
    // A peer controls this traffic. Congestion must not invalidate its target's
    // session or consume the capacity reserved for authoritative messages.
    sender.optional(Message::Text(forwarded));
}

fn node_reply(
    state: &AppState,
    node: Uuid,
    session: Uuid,
    request_id: &str,
    result: Result<(), String>,
) {
    if let Ok(connections) = state.node_connections.lock() {
        if let Some(connection) = connections
            .get(&node)
            .filter(|connection| connection.session_id == session)
        {
            let message = match result {
                Ok(()) => {
                    serde_json::json!({"type":"node_reply","request_id":request_id,"ok":true})
                }
                Err(error) => {
                    serde_json::json!({"type":"node_reply","request_id":request_id,"ok":false,"error":error})
                }
            };
            let _ = connection
                .sender
                .try_send(Message::Text(message.to_string()));
        }
    }
}

fn route_durability_message(state: &AppState, node: Uuid, session: Uuid, text: &str) -> bool {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(text) else {
        return false;
    };
    let kind = value
        .get("type")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("");
    if !matches!(
        kind,
        "custody_key" | "custody_receipt" | "repair_complete" | "repair_failed"
    ) {
        return false;
    }
    if !is_current_node_connection(state, node, session) {
        return true;
    }
    let request_id = value
        .get("request_id")
        .and_then(serde_json::Value::as_str)
        .filter(|id| id.len() <= 64)
        .unwrap_or("");
    let active = state.store.lock().ok().is_some_and(|store| {
        store
            .nodes
            .get(&node)
            .is_some_and(|node| node.online && (kind == "custody_key" || !node.paused))
    });
    if !active {
        node_reply(
            state,
            node,
            session,
            request_id,
            Err("The holder node must be active".into()),
        );
        return true;
    }
    match kind {
        "custody_key" => {
            let key = value
                .get("public_key")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("");
            let nonce = value
                .get("nonce")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("");
            let result = (|| {
                custody::public_key(key)?;
                let mut connections = state
                    .node_connections
                    .lock()
                    .map_err(|_| "Connection registry unavailable")?;
                let connection = connections
                    .get_mut(&node)
                    .filter(|connection| {
                        connection.session_id == session && connection.receipt_nonce == nonce
                    })
                    .ok_or("Holder session changed")?;
                if connection
                    .receipt_key
                    .as_ref()
                    .is_some_and(|existing| existing != key)
                {
                    return Err("A signing key is already bound to this session".into());
                }
                state
                    .durability
                    .lock()
                    .map_err(|_| "Holder registry unavailable")?
                    .custody
                    .register_key(node, nonce, key)?;
                connection.receipt_key = Some(key.to_owned());
                Ok(())
            })();
            let registered = result.is_ok();
            node_reply(state, node, session, request_id, result);
            if registered {
                if let Ok(connections) = state.node_connections.lock() {
                    if let Some(holder) = connections.get(&node) {
                        let message =
                            serde_json::json!({"type":"holder_ready","node_id":node}).to_string();
                        for connection in connections
                            .values()
                            .filter(|connection| connection.cluster_id == holder.cluster_id)
                        {
                            let _ = connection.sender.try_send(Message::Text(message.clone()));
                        }
                    }
                }
            }
        }
        "custody_receipt" => {
            let result = (|| {
                let proof: custody::HolderProof = serde_json::from_value(
                    value
                        .get("receipt")
                        .cloned()
                        .ok_or("Holder receipt is missing")?,
                )
                .map_err(|_| "Invalid holder receipt")?;
                let connection = state
                    .node_connections
                    .lock()
                    .map_err(|_| "Connection registry unavailable")?
                    .get(&node)
                    .filter(|connection| connection.session_id == session)
                    .cloned()
                    .ok_or("Holder session changed")?;
                let key = connection
                    .receipt_key
                    .as_deref()
                    .ok_or("Register a holder signing key first")?;
                state
                    .durability
                    .lock()
                    .map_err(|_| "Holder registry unavailable")?
                    .custody
                    .accept(proof, node, &connection.receipt_nonce, key, now())
            })();
            node_reply(state, node, session, request_id, result);
        }
        "repair_complete" => {
            if let Some(id) = value
                .get("repair_id")
                .and_then(serde_json::Value::as_str)
                .and_then(|id| Uuid::parse_str(id).ok())
            {
                let state = state.clone();
                let request_id = request_id.to_owned();
                // Durable manifest writes must not stall the node's signaling,
                // receipt acknowledgements, and heartbeat receive loop.
                tokio::spawn(async move {
                    let result = repair::complete(&state, id, node, session)
                        .await
                        .map_err(|error| error.1);
                    if result.is_err() {
                        if let Ok(mut runtime) = state.durability.lock() {
                            runtime.repairs.fail(id, node, session, now());
                        }
                    }
                    node_reply(&state, node, session, &request_id, result);
                });
            } else {
                node_reply(
                    state,
                    node,
                    session,
                    request_id,
                    Err("Invalid repair ID".into()),
                );
            }
        }
        "repair_failed" => {
            if let Some(id) = value
                .get("repair_id")
                .and_then(serde_json::Value::as_str)
                .and_then(|id| Uuid::parse_str(id).ok())
            {
                if let Ok(mut runtime) = state.durability.lock() {
                    runtime.repairs.fail(id, node, session, now());
                }
            }
        }
        _ => {}
    }
    true
}

async fn run_node_socket(socket: WebSocket, state: AppState, id: Uuid, session_id: Uuid) {
    let (sender, mut queued) = tokio::sync::mpsc::channel::<Message>(128);
    let outbound = delivery::Outbound::from(sender);
    if !register_node_connection(&state, id, session_id, outbound.clone()) {
        return;
    }
    if let Ok(connections) = state.node_connections.lock() {
        if let Some(connection) = connections.get(&id) {
            let _ = outbound.try_send(Message::Text(
                serde_json::json!({"type":"custody_welcome","nonce":connection.receipt_nonce})
                    .to_string(),
            ));
        }
    }
    announce_peer_presence(&state, id, true);
    let (mut sink, mut stream) = socket.split();
    let mut tick = interval(Duration::from_secs(NODE_PING_INTERVAL_SECS));
    tick.tick().await;
    let mut last_paused = None;
    loop {
        tokio::select! {
            _ = outbound.failed() => {
                eprintln!("{{\"event\":\"node_delivery_resync\",\"node_id\":\"{id}\"}}");
                let _ = tokio::time::timeout(Duration::from_secs(2), sink.send(Message::Close(None))).await;
                break;
            },
            message = stream.next() => match message {
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                Some(Ok(Message::Ping(bytes))) => {
                    if !update_node_heartbeat(&state, id, session_id) { break; }
                    if !matches!(tokio::time::timeout(Duration::from_secs(5), sink.send(Message::Pong(bytes))).await, Ok(Ok(()))) { break; }
                }
                Some(Ok(Message::Text(text))) => {
                    if !update_node_heartbeat(&state, id, session_id) { break; }
                    if !route_durability_message(&state, id, session_id, &text) { route_peer_signal(&state, id, session_id, &text).await; }
                }
                Some(Ok(Message::Binary(_))) | Some(Ok(Message::Pong(_))) => {
                    if !update_node_heartbeat(&state, id, session_id) { break; }
                }
            },
            outgoing = queued.recv() => match outgoing {
                Some(message) => if !matches!(tokio::time::timeout(Duration::from_secs(5), sink.send(message)).await, Ok(Ok(()))) { break; },
                None => break,
            },
            _ = tick.tick() => {
                if !is_current_node_connection(&state, id, session_id) { break; }
                let status = state.store.lock().ok().and_then(|store| store.nodes.get(&id).map(|node| (node.paused, node.last_seen)));
                match status {
                    Some((paused, last_seen)) => {
                        if now().saturating_sub(last_seen.unwrap_or_default()) > NODE_HEARTBEAT_TIMEOUT_SECS { break; }
                        if !matches!(tokio::time::timeout(Duration::from_secs(5), sink.send(Message::Ping(Vec::new()))).await, Ok(Ok(()))) { break; }
                        if last_paused != Some(paused) {
                            let message = serde_json::json!({"type": "control", "paused": paused}).to_string();
                            if !matches!(tokio::time::timeout(Duration::from_secs(5), sink.send(Message::Text(message))).await, Ok(Ok(()))) { break; }
                            last_paused = Some(paused);
                        }
                    }
                    None => break,
                }
            }
        }
    }
    disconnect_node(&state, id, session_id);
}

fn app(state: AppState) -> Router {
    Router::new()
        .route("/api/identities", post(security::bootstrap))
        .route("/api/accounts", post(security::signup))
        .route("/api/accounts/login", post(security::login))
        .route("/api/account", get(security::me))
        .route("/api/account/logout", post(security::logout))
        .route("/api/account/key", post(security::rotate_key))
        .route("/api/account/key/confirm", post(security::confirm_rotation))
        .route("/api/account/activate", post(security::activate))
        .route("/api/account/totp/setup", post(security::begin_totp))
        .route("/api/account/totp/enable", post(security::enable_totp))
        .route("/api/account/totp/disable", post(security::disable_totp))
        .route(
            "/api/clusters/:id/applications",
            get(security::list_apps).post(security::create_app),
        )
        .route(
            "/api/applications/by-name/:username/:slug",
            get(security::by_name),
        )
        .route(
            "/api/applications/:id",
            get(security::get_app).delete(security::delete_app),
        )
        .route("/api/applications/:id/members", post(security::grant))
        .route(
            "/api/applications/:id/members/:username",
            axum::routing::delete(security::revoke),
        )
        .route("/health", get(health))
        .route("/api/clusters", get(list_clusters).post(create_cluster))
        .route(
            "/api/clusters/:id",
            axum::routing::patch(rename_cluster).delete(delete_cluster),
        )
        .route("/api/clusters/:id/jobs", get(list_jobs).post(create_job))
        .route("/api/jobs/:id/claim", post(claim_job_batch))
        .route("/api/jobs/:id/renew", post(renew_job_batch))
        .route("/api/jobs/:id/release", post(release_job_batch))
        .route("/api/jobs/:id/commit", post(commit_job_batch))
        .route("/api/jobs/:id", axum::routing::delete(delete_job))
        .route(
            "/api/clusters/:id/invites",
            get(list_invites).post(create_invite),
        )
        .route(
            "/api/clusters/:id/invites/:token",
            axum::routing::delete(revoke_invite),
        )
        .route("/api/clusters/:id/leave", post(leave_cluster))
        .route("/api/clusters/:id/members", get(list_members))
        .route(
            "/api/clusters/:id/members/:member_id",
            axum::routing::delete(remove_member),
        )
        .route("/api/clusters/:id/nodes", get(list_nodes).post(create_node))
        .route("/api/invites/:token", get(preview_invite))
        .route("/api/invites/join", post(join_invite))
        .route("/api/nodes/:id/pause", post(pause_node))
        .route("/api/nodes/:id/resume", post(resume_node))
        .route("/api/nodes/:id", axum::routing::delete(delete_node))
        .route("/api/nodes/connect", get(node_socket))
        .layer(DefaultBodyLimit::max(64 * 1024))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            security::authenticate,
        ))
        .with_state(state.clone())
        .nest_service("/assets", ServeDir::new("dist/assets"))
        .route("/:username/:application", get(security::application_page))
        .fallback_service(ServeDir::new("dist").append_index_html_on_directories(true))
}

#[tokio::main]
async fn main() {
    let data_file = PathBuf::from(
        std::env::var("MESH_DATA_FILE").unwrap_or_else(|_| "/tmp/mesh-state.json".into()),
    );
    let github = std::env::var("GH_TOKEN")
        .ok()
        .filter(|token| !token.trim().is_empty())
        .map(|token| {
            Arc::new(GitHubStorage {
                token,
                repo: std::env::var("MESH_STORAGE_REPO")
                    .unwrap_or_else(|_| "Wuhpondiscord/gpuclustertest-state".into()),
                branch: std::env::var("MESH_STORAGE_BRANCH").unwrap_or_else(|_| "main".into()),
                path: std::env::var("MESH_STORAGE_PATH")
                    .unwrap_or_else(|_| "mesh-state.json".into()),
            })
        });
    if std::env::var("MESH_REQUIRE_GITHUB_STORAGE").as_deref() == Ok("1") && github.is_none() {
        panic!("Set the GH_TOKEN secret on the Hugging Face Space to enable persistent cluster storage");
    }
    let context = if let Some(config) = github.as_deref() {
        serde_json::to_string(&(&config.repo, &config.branch, &config.path))
            .expect("storage context")
    } else {
        "local".to_owned()
    };
    let snapshot = Arc::new(
        snapshot::Codec::from_env(&context).expect("State encryption configuration invalid"),
    );
    let (mut store, github_sha, needs_migration) = if let Some(config) = github.as_deref() {
        load_github_store(config, &snapshot)
            .expect("GitHub cluster storage is configured but could not be loaded")
    } else {
        match std::fs::File::open(&data_file) {
            Ok(file) => {
                let mut bytes = Vec::new();
                file.take((snapshot::MAX_FILE_BYTES + 1) as u64)
                    .read_to_end(&mut bytes)
                    .expect("Could not read local state");
                let (store, migration) = snapshot
                    .decode(&bytes)
                    .expect("Could not authenticate local state");
                (store, None, migration)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                (Store::default(), None, false)
            }
            Err(_) => {
                panic!("Could not open local state; refusing to initialize an empty database")
            }
        }
    };
    store
        .recovery
        .start_session(coordinator_epoch(Uuid::new_v4()));
    for node in store.nodes.values_mut() {
        node.online = false;
    }
    let state = AppState {
        store: Arc::new(Mutex::new(store)),
        mutation_lock: Arc::new(tokio::sync::Mutex::new(())),
        data_file: Arc::new(data_file),
        snapshot,
        github,
        github_file_sha: Arc::new(Mutex::new(github_sha)),
        node_connections: Arc::new(Mutex::new(HashMap::new())),
        durability: Arc::new(Mutex::new(DurabilityRuntime::default())),
    };
    finish_storage_startup(&state, needs_migration)
        .expect("Could not encrypt legacy state; refusing to serve it");
    if needs_migration {
        println!("Legacy state migrated to authenticated encrypted storage");
    }
    let app = app(state.clone());
    let port = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(7860);
    let repair_state = state.clone();
    tokio::spawn(async move {
        let mut ticks = interval(Duration::from_secs(5));
        loop {
            ticks.tick().await;
            repair::dispatch(&repair_state);
        }
    });
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("bind coordinator");
    println!("Mesh coordinator listening on http://{addr}");
    axum::serve(listener, app).await.expect("serve coordinator");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn startup_migration_preserves_data_and_failure_prevents_completion() {
        let mut state = AppState::for_test();
        let id = Uuid::new_v4();
        state.store.lock().unwrap().clusters.insert(
            id,
            Cluster {
                id,
                name: "migration-original-name".into(),
                owner_id: "original-owner".into(),
                members: HashMap::new(),
            },
        );
        let legacy = serde_json::to_vec(&*state.store.lock().unwrap()).unwrap();
        std::fs::write(state.data_file.as_path(), &legacy).unwrap();
        finish_storage_startup(&state, true).unwrap();
        let disk = std::fs::read(state.data_file.as_path()).unwrap();
        assert!(!String::from_utf8_lossy(&disk).contains("migration-original-name"));
        let (restored, migration) = state.snapshot.decode(&disk).unwrap();
        assert!(!migration);
        assert_eq!(restored.clusters[&id].owner_id, "original-owner");
        assert!(state.snapshot.decode(&legacy).is_err());
        let _ = std::fs::remove_file(state.data_file.as_path());
        // A file in place of the parent directory forces a real persistence failure.
        let blocker = std::env::temp_dir().join(format!("mesh-migration-{}", Uuid::new_v4()));
        std::fs::write(&blocker, b"preserve-me").unwrap();
        state.data_file = Arc::new(blocker.join("state.json"));
        assert!(finish_storage_startup(&state, true).is_err());
        assert_eq!(std::fs::read(&blocker).unwrap(), b"preserve-me");
        assert_eq!(
            state.store.lock().unwrap().clusters[&id].owner_id,
            "original-owner"
        );
        let _ = std::fs::remove_file(blocker);
    }

    #[test]
    fn execution_reports_are_bounded_and_modes_are_explicit() {
        let mut report = ExecutionReport {
            execution_mode: "webgpu".into(),
            fallback_reason: None,
            compute_ms: 1.0,
            validation_ms: 2.0,
            encoding_ms: 3.0,
            transfer_ms: 4.0,
        };
        assert!(report.valid());
        report.compute_ms = f64::NAN;
        assert!(!report.valid());
        report.compute_ms = -1.0;
        assert!(!report.valid());
        report.compute_ms = 1.0;
        report.execution_mode = "trusted-gpu".into();
        assert!(!report.valid());
        report.execution_mode = "cpu-fallback".into();
        report.fallback_reason = Some("x".repeat(513));
        assert!(!report.valid());
    }

    #[tokio::test]
    async fn release_authorizes_the_exact_job_and_does_not_persist_runtime_leases() {
        let state = AppState::for_test();
        let owner = test_headers(&"a".repeat(64));
        let (_, Json(cluster)) = create_cluster(
            State(state.clone()),
            owner.clone(),
            Json(CreateCluster {
                name: "release".into(),
            }),
        )
        .await
        .unwrap();
        let (_, Json(worker)) = create_node(
            State(state.clone()),
            Path(cluster.id),
            owner.clone(),
            Json(CreateNode {
                name: "worker".into(),
            }),
        )
        .await
        .unwrap();
        state
            .store
            .lock()
            .unwrap()
            .nodes
            .get_mut(&worker.node.id)
            .unwrap()
            .online = true;
        let (_, Json(job)) = create_job(
            State(state.clone()),
            Path(cluster.id),
            owner.clone(),
            Json(CreateJob {
                workload: ComputeWorkload::CpuHash,
                total_units: 1000,
                batch_units: 1000,
            }),
        )
        .await
        .unwrap();
        let disk = std::fs::read(state.data_file.as_path()).unwrap();
        let Json(Some(claim)) = claim_job_batch(
            State(state.clone()),
            Path(job.id),
            owner.clone(),
            Json(ClaimJob {
                node_id: worker.node.id,
            }),
        )
        .await
        .unwrap() else {
            panic!("missing claim")
        };
        assert!(release_job_batch(
            State(state.clone()),
            Path(job.id),
            test_headers(&"b".repeat(64)),
            Json(RenewJob {
                lease: claim.lease.clone()
            })
        )
        .await
        .is_err());
        release_job_batch(
            State(state.clone()),
            Path(job.id),
            owner.clone(),
            Json(RenewJob {
                lease: claim.lease.clone(),
            }),
        )
        .await
        .unwrap();
        let Json(Some(next)) = claim_job_batch(
            State(state.clone()),
            Path(job.id),
            owner.clone(),
            Json(ClaimJob {
                node_id: worker.node.id,
            }),
        )
        .await
        .unwrap() else {
            panic!("released batch was not reclaimable")
        };
        assert!(next.lease.generation > claim.lease.generation);
        assert!(release_job_batch(
            State(state.clone()),
            Path(job.id),
            owner,
            Json(RenewJob { lease: claim.lease })
        )
        .await
        .is_err());
        assert_eq!(std::fs::read(state.data_file.as_path()).unwrap(), disk);
        let _ = std::fs::remove_file(state.data_file.as_path());
    }

    impl AppState {
        fn for_test() -> Self {
            Self {
                store: Arc::new(Mutex::new(Store::default())),
                mutation_lock: Arc::new(tokio::sync::Mutex::new(())),
                data_file: Arc::new(
                    std::env::temp_dir().join(format!("mesh-unit-{}.json", Uuid::new_v4())),
                ),
                snapshot: Arc::new(snapshot::Codec::for_test()),
                github: None,
                github_file_sha: Arc::new(Mutex::new(None)),
                node_connections: Arc::new(Mutex::new(HashMap::new())),
                durability: Arc::new(Mutex::new(DurabilityRuntime::default())),
            }
        }
    }

    #[test]
    fn invite_refresh_is_globally_bounded_even_for_different_unknown_tokens() {
        let mut next = None;
        let at = Instant::now();
        assert!(reserve_invite_refresh(&mut next, at));
        for second in 0..60 {
            assert!(!reserve_invite_refresh(
                &mut next,
                at + Duration::from_secs(second)
            ));
        }
        assert!(reserve_invite_refresh(
            &mut next,
            at + Duration::from_secs(60)
        ));
    }

    #[test]
    fn github_snapshot_loads_large_blob_and_inline_content_and_rejects_oversize() {
        let mut snapshot = Store::default();
        let id = Uuid::new_v4();
        snapshot.clusters.insert(
            id,
            Cluster {
                id,
                name: "x".repeat(1_100_000),
                owner_id: "owner".into(),
                members: HashMap::new(),
            },
        );
        let codec = snapshot::Codec::for_test();
        let bytes = codec.encode(&snapshot).unwrap();
        let metadata = serde_json::json!({"encoding":"none", "content":"", "size":bytes.len()});
        let (loaded, migrated) = codec
            .decode(&decode_github_snapshot(&metadata, || Ok(bytes)).unwrap())
            .unwrap();
        assert!(!migrated);
        assert_eq!(loaded.clusters[&id].name.len(), 1_100_000);
        let inline = serde_json::json!({"encoding":"base64", "content":BASE64.encode(codec.encode(&Store::default()).unwrap())});
        assert!(
            decode_github_snapshot(&inline, || panic!("inline content must not fetch a blob"))
                .is_ok()
        );
        assert!(decode_github_snapshot(
            &serde_json::json!({"size":snapshot::MAX_FILE_BYTES+1}),
            || panic!("oversize metadata must not fetch a blob")
        )
        .is_err());
        assert!(
            decode_github_snapshot(&serde_json::json!({"encoding":"none"}), || Ok(vec![
                0;
                snapshot::MAX_FILE_BYTES
                    + 1
            ]))
            .is_err()
        );
    }

    #[tokio::test]
    async fn router_enforces_auth_path_and_body_limits() {
        use axum::{body::Body, http::Request};
        use tower::ServiceExt;
        let state = AppState::for_test();
        let router = app(state);
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/clusters")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(response.headers()["content-type"], "application/json");
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let error: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(error["source"], "mesh-coordinator");
        assert!(error["error"].is_string());
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/jobs/not-a-uuid/claim")
                    .method("POST")
                    .header(IDENTITY_HEADER, "a".repeat(64))
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let response = router
            .oneshot(
                Request::builder()
                    .uri("/api/clusters")
                    .method("POST")
                    .header(IDENTITY_HEADER, "a".repeat(64))
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({"name":"x".repeat(65536)}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test]
    async fn job_caps_can_be_reclaimed_by_authorized_deletion() {
        let state = AppState::for_test();
        let owner = test_headers(&"a".repeat(64));
        let (_, Json(cluster)) = create_cluster(
            State(state.clone()),
            owner.clone(),
            Json(CreateCluster {
                name: "bounded".into(),
            }),
        )
        .await
        .unwrap();
        let first = Uuid::new_v4();
        {
            let mut store = state.store.lock().unwrap();
            for index in 0..MAX_CLUSTER_JOBS {
                let id = if index == 0 { first } else { Uuid::new_v4() };
                store.recovery.add_task(format!("{id}:0")).unwrap();
                store.jobs.insert(
                    id,
                    ComputeJob {
                        id,
                        cluster_id: cluster.id,
                        created_at: 0,
                        workload: ComputeWorkload::CpuHash,
                        total_units: 1000,
                        batch_units: 1000,
                        outputs: HashMap::new(),
                    },
                );
            }
        }
        assert!(matches!(
            create_job(
                State(state.clone()),
                Path(cluster.id),
                owner.clone(),
                Json(CreateJob {
                    workload: ComputeWorkload::CpuHash,
                    total_units: 1000,
                    batch_units: 1000
                })
            )
            .await,
            Err(ApiError(StatusCode::CONFLICT, _))
        ));
        assert!(delete_job(
            State(state.clone()),
            Path(first),
            test_headers(&"b".repeat(64))
        )
        .await
        .is_err());
        delete_job(State(state.clone()), Path(first), owner.clone())
            .await
            .unwrap();
        assert!(!state
            .store
            .lock()
            .unwrap()
            .recovery
            .is_complete(&format!("{first}:0")));
        assert!(create_job(
            State(state.clone()),
            Path(cluster.id),
            owner,
            Json(CreateJob {
                workload: ComputeWorkload::CpuHash,
                total_units: 1000,
                batch_units: 1000
            })
        )
        .await
        .is_ok());
        let _ = std::fs::remove_file(state.data_file.as_path());
    }

    fn test_headers(token: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(IDENTITY_HEADER, token.parse().unwrap());
        headers
    }

    #[test]
    fn old_job_snapshots_and_create_requests_default_to_cpu_hash() {
        let old_request: CreateJob = serde_json::from_value(serde_json::json!({
            "total_units": 1000,
            "batch_units": 1000
        }))
        .unwrap();
        assert_eq!(old_request.workload, ComputeWorkload::CpuHash);

        let old_snapshot: ComputeJob = serde_json::from_value(serde_json::json!({
            "id": Uuid::nil(),
            "cluster_id": Uuid::nil(),
            "created_at": 1,
            "total_units": 1000,
            "batch_units": 1000,
            "outputs": {"0": {
                "sha256": "a".repeat(64), "bytes": 128,
                "worker_node_id": Uuid::nil(), "replica_node_id": Uuid::nil(), "committed_at": 1
            }}
        }))
        .unwrap();
        assert_eq!(old_snapshot.workload, ComputeWorkload::CpuHash);
        assert!(old_snapshot.outputs[&0].shards.is_empty());

        let vector_add: CreateJob = serde_json::from_value(serde_json::json!({
            "workload": "vector-add",
            "total_units": 1000,
            "batch_units": 1000
        }))
        .unwrap();
        assert_eq!(vector_add.workload, ComputeWorkload::VectorAdd);
    }

    #[test]
    fn cluster_names_are_trimmed_and_bounded() {
        assert_eq!(
            cluster_name("  Research pool  ").as_deref(),
            Some("Research pool")
        );
        assert_eq!(cluster_name(" "), None);
        assert_eq!(cluster_name(&"x".repeat(81)), None);
    }

    #[test]
    fn github_revision_conflicts_retry_only_with_bounded_attempts() {
        let conflict = ApiError(StatusCode::CONFLICT, "revision conflict".into());
        let unavailable = ApiError(StatusCode::SERVICE_UNAVAILABLE, "offline".into());
        assert!(should_retry_storage_conflict(&conflict, 0, true));
        assert!(should_retry_storage_conflict(
            &conflict,
            MAX_STORAGE_CONFLICT_RETRIES - 1,
            true
        ));
        assert!(!should_retry_storage_conflict(
            &conflict,
            MAX_STORAGE_CONFLICT_RETRIES,
            true
        ));
        assert!(!should_retry_storage_conflict(&conflict, 0, false));
        assert!(!should_retry_storage_conflict(&unavailable, 0, true));
    }

    #[test]
    fn identities_need_unpredictable_length_tokens() {
        assert!(identity(&HeaderMap::new()).is_err());
        let mut headers = HeaderMap::new();
        headers.insert(
            IDENTITY_HEADER,
            "0123456789abcdef0123456789abcdef".parse().unwrap(),
        );
        let hashed_identity = identity(&headers).unwrap();
        assert_eq!(hashed_identity.len(), 64);
        assert_ne!(hashed_identity, "0123456789abcdef0123456789abcdef");
    }

    #[test]
    fn members_can_belong_to_multiple_clusters() {
        let mut members = HashMap::new();
        members.insert("user-a".to_owned(), Role::Owner);
        let first = Cluster {
            id: Uuid::new_v4(),
            name: "one".into(),
            owner_id: "user-a".into(),
            members: members.clone(),
        };
        let second = Cluster {
            id: Uuid::new_v4(),
            name: "two".into(),
            owner_id: "user-b".into(),
            members,
        };
        assert!(first.members.contains_key("user-a") && second.members.contains_key("user-a"));
    }

    #[test]
    fn github_file_revision_uses_contents_response_sha() {
        let response = serde_json::json!({ "content": { "sha": "expected-blob-sha" } });
        assert_eq!(
            response
                .pointer("/content/sha")
                .and_then(serde_json::Value::as_str),
            Some("expected-blob-sha")
        );
    }

    #[tokio::test]
    async fn owner_invites_member_and_can_revoke_invite() {
        let file = std::env::temp_dir().join(format!("mesh-test-{}.json", Uuid::new_v4()));
        let state = AppState {
            store: Arc::new(Mutex::new(Store::default())),
            mutation_lock: Arc::new(tokio::sync::Mutex::new(())),
            data_file: Arc::new(file.clone()),
            snapshot: Arc::new(snapshot::Codec::for_test()),
            github: None,
            github_file_sha: Arc::new(Mutex::new(None)),
            node_connections: Arc::new(Mutex::new(HashMap::new())),
            durability: Arc::new(Mutex::new(DurabilityRuntime::default())),
        };
        let owner = test_headers("owner-identity-token-01234567890123456789");
        let guest = test_headers("guest-identity-token-01234567890123456789");

        let (_, Json(cluster)) = create_cluster(
            State(state.clone()),
            owner.clone(),
            Json(CreateCluster {
                name: "  Lab  ".into(),
            }),
        )
        .await
        .unwrap();
        assert_eq!(cluster.name, "Lab");
        let (_, Json(invite)) =
            create_invite(State(state.clone()), Path(cluster.id), owner.clone())
                .await
                .unwrap();
        assert!(state
            .store
            .lock()
            .unwrap()
            .invites
            .contains_key(&secret_hash(&invite.token)));
        assert!(!state
            .store
            .lock()
            .unwrap()
            .invites
            .contains_key(&invite.token));
        let Json(preview) = preview_invite(State(state.clone()), Path(invite.token.clone()))
            .await
            .unwrap();
        assert_eq!(preview.cluster_id, cluster.id);
        let Json(joined) = join_invite(
            State(state.clone()),
            guest.clone(),
            Json(JoinInvite {
                token: invite.token.clone(),
            }),
        )
        .await
        .unwrap();
        assert_eq!(joined.members, 2);
        let Json(guest_clusters) = list_clusters(State(state.clone()), guest).await.unwrap();
        assert_eq!(guest_clusters.len(), 1);
        assert!(delete_cluster(
            State(state.clone()),
            Path(cluster.id),
            test_headers("stranger-identity-token-01234567890123456789")
        )
        .await
        .is_err());
        assert_eq!(
            revoke_invite(
                State(state.clone()),
                Path((cluster.id, secret_hash(&invite.token))),
                owner
            )
            .await
            .unwrap(),
            StatusCode::NO_CONTENT
        );
        assert!(preview_invite(State(state), Path(invite.token))
            .await
            .is_err());
        let _ = std::fs::remove_file(file);
    }

    #[tokio::test]
    async fn deleting_cluster_removes_its_nodes_and_invites() {
        let file = std::env::temp_dir().join(format!("mesh-delete-test-{}.json", Uuid::new_v4()));
        let state = AppState {
            store: Arc::new(Mutex::new(Store::default())),
            mutation_lock: Arc::new(tokio::sync::Mutex::new(())),
            data_file: Arc::new(file.clone()),
            snapshot: Arc::new(snapshot::Codec::for_test()),
            github: None,
            github_file_sha: Arc::new(Mutex::new(None)),
            node_connections: Arc::new(Mutex::new(HashMap::new())),
            durability: Arc::new(Mutex::new(DurabilityRuntime::default())),
        };
        let owner = test_headers("delete-owner-identity-token-012345678901234567");
        let (_, Json(cluster)) = create_cluster(
            State(state.clone()),
            owner.clone(),
            Json(CreateCluster {
                name: "Removable".into(),
            }),
        )
        .await
        .unwrap();
        let (_, Json(invite)) =
            create_invite(State(state.clone()), Path(cluster.id), owner.clone())
                .await
                .unwrap();
        let (_, Json(node)) = create_node(
            State(state.clone()),
            Path(cluster.id),
            owner.clone(),
            Json(CreateNode {
                name: "Worker".into(),
            }),
        )
        .await
        .unwrap();

        assert_eq!(
            delete_cluster(State(state.clone()), Path(cluster.id), owner)
                .await
                .unwrap(),
            StatusCode::NO_CONTENT
        );
        let store = state.store.lock().unwrap();
        assert!(!store.clusters.contains_key(&cluster.id));
        assert!(!store.nodes.contains_key(&node.node.id));
        assert!(!store.invites.contains_key(&secret_hash(&invite.token)));
        drop(store);
        let _ = std::fs::remove_file(file);
    }

    #[tokio::test]
    async fn failed_cluster_delete_keeps_cluster_in_memory() {
        let blocker = std::env::temp_dir().join(format!("mesh-delete-blocker-{}", Uuid::new_v4()));
        std::fs::write(&blocker, b"not a directory").unwrap();
        let owner = "delete-owner-identity-token-012345678901234567";
        let cluster_id = Uuid::new_v4();
        let mut members = HashMap::new();
        members.insert(secret_hash(owner), Role::Owner);
        let mut store = Store::default();
        store.clusters.insert(
            cluster_id,
            Cluster {
                id: cluster_id,
                name: "Keep me".into(),
                owner_id: secret_hash(owner),
                members,
            },
        );
        let state = AppState {
            store: Arc::new(Mutex::new(store)),
            mutation_lock: Arc::new(tokio::sync::Mutex::new(())),
            data_file: Arc::new(blocker.join("state.json")),
            snapshot: Arc::new(snapshot::Codec::for_test()),
            github: None,
            github_file_sha: Arc::new(Mutex::new(None)),
            node_connections: Arc::new(Mutex::new(HashMap::new())),
            durability: Arc::new(Mutex::new(DurabilityRuntime::default())),
        };

        assert!(
            delete_cluster(State(state.clone()), Path(cluster_id), test_headers(owner))
                .await
                .is_err()
        );
        assert!(state
            .store
            .lock()
            .unwrap()
            .clusters
            .contains_key(&cluster_id));
        let _ = std::fs::remove_file(blocker);
    }

    #[tokio::test]
    async fn failed_persistence_never_publishes_a_mutation() {
        let blocker =
            std::env::temp_dir().join(format!("mesh-transaction-blocker-{}", Uuid::new_v4()));
        std::fs::write(&blocker, b"not a directory").unwrap();
        let state = AppState {
            store: Arc::new(Mutex::new(Store::default())),
            mutation_lock: Arc::new(tokio::sync::Mutex::new(())),
            data_file: Arc::new(blocker.join("state.json")),
            snapshot: Arc::new(snapshot::Codec::for_test()),
            github: None,
            github_file_sha: Arc::new(Mutex::new(None)),
            node_connections: Arc::new(Mutex::new(HashMap::new())),
            durability: Arc::new(Mutex::new(DurabilityRuntime::default())),
        };

        let result = mutate_store(&state, |store| {
            let id = Uuid::new_v4();
            store.clusters.insert(
                id,
                Cluster {
                    id,
                    name: "Should not commit".into(),
                    owner_id: "owner".into(),
                    members: HashMap::new(),
                },
            );
            Ok(())
        })
        .await;

        assert!(result.is_err());
        assert!(state.store.lock().unwrap().clusters.is_empty());
        let _ = std::fs::remove_file(blocker);
    }

    #[tokio::test]
    async fn concurrent_mutations_are_serialized_without_lost_updates() {
        let file =
            std::env::temp_dir().join(format!("mesh-concurrent-test-{}.json", Uuid::new_v4()));
        let state = AppState {
            store: Arc::new(Mutex::new(Store::default())),
            mutation_lock: Arc::new(tokio::sync::Mutex::new(())),
            data_file: Arc::new(file.clone()),
            snapshot: Arc::new(snapshot::Codec::for_test()),
            github: None,
            github_file_sha: Arc::new(Mutex::new(None)),
            node_connections: Arc::new(Mutex::new(HashMap::new())),
            durability: Arc::new(Mutex::new(DurabilityRuntime::default())),
        };
        let first_id = Uuid::new_v4();
        let second_id = Uuid::new_v4();
        let (first, second) = tokio::join!(
            mutate_store(&state, move |store| {
                store.clusters.insert(
                    first_id,
                    Cluster {
                        id: first_id,
                        name: "first".into(),
                        owner_id: "owner".into(),
                        members: HashMap::new(),
                    },
                );
                Ok(())
            }),
            mutate_store(&state, move |store| {
                store.clusters.insert(
                    second_id,
                    Cluster {
                        id: second_id,
                        name: "second".into(),
                        owner_id: "owner".into(),
                        members: HashMap::new(),
                    },
                );
                Ok(())
            }),
        );

        assert!(first.is_ok() && second.is_ok());
        let (disk, _) = state
            .snapshot
            .decode(&std::fs::read(&file).unwrap())
            .unwrap();
        assert_eq!(disk.clusters.len(), 2);
        assert_eq!(state.store.lock().unwrap().clusters.len(), 2);
        let _ = std::fs::remove_file(file);
    }

    #[test]
    fn replaced_node_connection_cannot_mark_new_connection_offline() {
        let node_id = Uuid::new_v4();
        let cluster_id = Uuid::new_v4();
        let mut members = HashMap::new();
        members.insert("owner".to_owned(), Role::Owner);
        let mut store = Store::default();
        store.clusters.insert(
            cluster_id,
            Cluster {
                id: cluster_id,
                name: "test".into(),
                owner_id: "owner".into(),
                members,
            },
        );
        store.nodes.insert(
            node_id,
            Node {
                id: node_id,
                cluster_id,
                name: "node".into(),
                owner_id: "owner".into(),
                token_hash: "hash".into(),
                paused: false,
                online: false,
                last_seen: None,
            },
        );
        let state = AppState {
            store: Arc::new(Mutex::new(store)),
            mutation_lock: Arc::new(tokio::sync::Mutex::new(())),
            data_file: Arc::new(PathBuf::from("unused")),
            snapshot: Arc::new(snapshot::Codec::for_test()),
            github: None,
            github_file_sha: Arc::new(Mutex::new(None)),
            node_connections: Arc::new(Mutex::new(HashMap::new())),
            durability: Arc::new(Mutex::new(DurabilityRuntime::default())),
        };
        let old_session = Uuid::new_v4();
        let new_session = Uuid::new_v4();

        let (old_tx, _old_rx) = tokio::sync::mpsc::channel(8);
        let (new_tx, _new_rx) = tokio::sync::mpsc::channel(8);
        assert!(register_node_connection(
            &state,
            node_id,
            old_session,
            old_tx
        ));
        assert!(register_node_connection(
            &state,
            node_id,
            new_session,
            new_tx
        ));
        let now_ms = crate::now_ms();
        let lease = {
            let mut store = state.store.lock().unwrap();
            store.recovery.add_task("connection-test").unwrap();
            store
                .recovery
                .claim(
                    "connection-test",
                    node_id.to_string(),
                    now_ms,
                    JOB_LEASE_DURATION_MS,
                )
                .unwrap()
        };
        disconnect_node(&state, node_id, old_session);
        assert!(is_current_node_connection(&state, node_id, new_session));
        assert!(
            state
                .store
                .lock()
                .unwrap()
                .nodes
                .get(&node_id)
                .unwrap()
                .online
        );
        let mut still_active = state.store.lock().unwrap().recovery.clone();
        assert_eq!(
            still_active.claim(
                "connection-test",
                "other-worker",
                now_ms + WORKER_RECONNECT_GRACE_MS + 1,
                JOB_LEASE_DURATION_MS
            ),
            Err(recovery::LeaseError::LeaseBusy)
        );
        disconnect_node(&state, node_id, new_session);
        assert!(
            !state
                .store
                .lock()
                .unwrap()
                .nodes
                .get(&node_id)
                .unwrap()
                .online
        );
        let mut disconnected = state.store.lock().unwrap().recovery.clone();
        let replacement = disconnected
            .claim(
                "connection-test",
                "other-worker",
                crate::now_ms() + WORKER_RECONNECT_GRACE_MS,
                JOB_LEASE_DURATION_MS,
            )
            .unwrap();
        assert!(replacement.generation > lease.generation);
    }

    #[tokio::test]
    async fn peer_signals_are_forwarded_only_inside_the_same_cluster() {
        let cluster_a = Uuid::new_v4();
        let cluster_b = Uuid::new_v4();
        let sender_id = Uuid::new_v4();
        let same_cluster_id = Uuid::new_v4();
        let other_cluster_id = Uuid::new_v4();
        let mut store = Store::default();
        for (id, cluster_id) in [
            (sender_id, cluster_a),
            (same_cluster_id, cluster_a),
            (other_cluster_id, cluster_b),
        ] {
            store.nodes.insert(
                id,
                Node {
                    id,
                    cluster_id,
                    name: "node".into(),
                    owner_id: "owner".into(),
                    token_hash: "hash".into(),
                    paused: false,
                    online: false,
                    last_seen: None,
                },
            );
        }
        let state = AppState {
            store: Arc::new(Mutex::new(store)),
            mutation_lock: Arc::new(tokio::sync::Mutex::new(())),
            data_file: Arc::new(PathBuf::from("unused")),
            snapshot: Arc::new(snapshot::Codec::for_test()),
            github: None,
            github_file_sha: Arc::new(Mutex::new(None)),
            node_connections: Arc::new(Mutex::new(HashMap::new())),
            durability: Arc::new(Mutex::new(DurabilityRuntime::default())),
        };
        let sender_session = Uuid::new_v4();
        let same_session = Uuid::new_v4();
        let other_session = Uuid::new_v4();
        let (sender_tx, _sender_rx) = tokio::sync::mpsc::channel(8);
        let (same_tx, mut same_rx) = tokio::sync::mpsc::channel(8);
        let (other_tx, mut other_rx) = tokio::sync::mpsc::channel(8);
        assert!(register_node_connection(
            &state,
            sender_id,
            sender_session,
            sender_tx
        ));
        assert!(register_node_connection(
            &state,
            same_cluster_id,
            same_session,
            same_tx
        ));
        assert!(register_node_connection(
            &state,
            other_cluster_id,
            other_session,
            other_tx
        ));

        let signal = serde_json::json!({"type":"signal","peer_node_id":same_cluster_id,"payload":{"description":{"type":"offer","sdp":"test"}}}).to_string();
        route_peer_signal(&state, sender_id, sender_session, &signal).await;
        let Message::Text(forwarded) = same_rx.try_recv().unwrap() else {
            panic!("expected forwarded peer signal");
        };
        let forwarded: serde_json::Value = serde_json::from_str(&forwarded).unwrap();
        assert_eq!(forwarded["peer_node_id"], sender_id.to_string());
        assert!(other_rx.try_recv().is_err());

        // A malicious source must not evict a slow destination's session or
        // consume its critical reserve, even if it reconnects over an active socket.
        for _ in 0..1000 {
            route_peer_signal(&state, sender_id, sender_session, &signal).await;
        }
        let target = state
            .node_connections
            .lock()
            .unwrap()
            .get(&same_cluster_id)
            .unwrap()
            .sender
            .clone();
        target
            .try_send(Message::Text("critical-receipt".into()))
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(10), target.failed())
                .await
                .is_err()
        );
        let mut received = Vec::new();
        while let Ok(message) = same_rx.try_recv() {
            received.push(message);
        }
        assert!(received.contains(&Message::Text("critical-receipt".into())));
        let replacement = Uuid::new_v4();
        let (tx, _rx) = tokio::sync::mpsc::channel(8);
        assert!(register_node_connection(&state, sender_id, replacement, tx));
        route_peer_signal(&state, sender_id, sender_session, &signal).await;
        assert!(same_rx.try_recv().is_err());

        let cross_cluster = serde_json::json!({"type":"signal","peer_node_id":other_cluster_id,"payload":{"description":{"type":"offer","sdp":"test"}}}).to_string();
        route_peer_signal(&state, sender_id, sender_session, &cross_cluster).await;
        assert!(other_rx.try_recv().is_err());

        state
            .store
            .lock()
            .unwrap()
            .nodes
            .get_mut(&same_cluster_id)
            .unwrap()
            .paused = true;
        route_peer_signal(&state, sender_id, sender_session, &signal).await;
        assert!(same_rx.try_recv().is_err());
    }

    #[test]
    fn peer_discovery_excludes_paused_nodes() {
        let cluster_id = Uuid::new_v4();
        let current_id = Uuid::new_v4();
        let active_id = Uuid::new_v4();
        let paused_id = Uuid::new_v4();
        let mut store = Store::default();
        for (id, paused) in [(current_id, false), (active_id, false), (paused_id, true)] {
            store.nodes.insert(
                id,
                Node {
                    id,
                    cluster_id,
                    name: "node".into(),
                    owner_id: "owner".into(),
                    token_hash: "hash".into(),
                    paused,
                    online: false,
                    last_seen: None,
                },
            );
        }
        let state = AppState {
            store: Arc::new(Mutex::new(store)),
            mutation_lock: Arc::new(tokio::sync::Mutex::new(())),
            data_file: Arc::new(PathBuf::from("unused")),
            snapshot: Arc::new(snapshot::Codec::for_test()),
            github: None,
            github_file_sha: Arc::new(Mutex::new(None)),
            node_connections: Arc::new(Mutex::new(HashMap::new())),
            durability: Arc::new(Mutex::new(DurabilityRuntime::default())),
        };
        let (current_tx, mut current_rx) = tokio::sync::mpsc::channel(8);
        let (active_tx, mut active_rx) = tokio::sync::mpsc::channel(8);
        let (paused_tx, mut paused_rx) = tokio::sync::mpsc::channel(8);
        assert!(register_node_connection(
            &state,
            current_id,
            Uuid::new_v4(),
            current_tx
        ));
        assert!(register_node_connection(
            &state,
            active_id,
            Uuid::new_v4(),
            active_tx
        ));
        assert!(register_node_connection(
            &state,
            paused_id,
            Uuid::new_v4(),
            paused_tx
        ));

        announce_peer_presence(&state, current_id, true);
        let Message::Text(peer_list) = current_rx.try_recv().unwrap() else {
            panic!("expected peer list");
        };
        let peer_list: serde_json::Value = serde_json::from_str(&peer_list).unwrap();
        assert_eq!(peer_list["peer_node_ids"], serde_json::json!([active_id]));
        let Message::Text(joined) = active_rx.try_recv().unwrap() else {
            panic!("expected peer joined event");
        };
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&joined).unwrap()["peer_node_id"],
            current_id.to_string()
        );
        assert!(paused_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn job_batches_require_a_live_owned_worker_and_replicated_checkpoint_before_commit() {
        let file = std::env::temp_dir().join(format!("mesh-job-test-{}.json", Uuid::new_v4()));
        let state = AppState {
            store: Arc::new(Mutex::new(Store::default())),
            mutation_lock: Arc::new(tokio::sync::Mutex::new(())),
            data_file: Arc::new(file.clone()),
            snapshot: Arc::new(snapshot::Codec::for_test()),
            github: None,
            github_file_sha: Arc::new(Mutex::new(None)),
            node_connections: Arc::new(Mutex::new(HashMap::new())),
            durability: Arc::new(Mutex::new(DurabilityRuntime::default())),
        };
        let owner_token = "job-owner-identity-token-012345678901234567890";
        let owner_headers = test_headers(owner_token);
        let stranger_headers = test_headers("job-stranger-identity-token-01234567890123456789");
        let (_, Json(cluster)) = create_cluster(
            State(state.clone()),
            owner_headers.clone(),
            Json(CreateCluster {
                name: "Compute".into(),
            }),
        )
        .await
        .unwrap();
        let (_, Json(worker)) = create_node(
            State(state.clone()),
            Path(cluster.id),
            owner_headers.clone(),
            Json(CreateNode {
                name: "Worker".into(),
            }),
        )
        .await
        .unwrap();
        let (_, Json(replica)) = create_node(
            State(state.clone()),
            Path(cluster.id),
            owner_headers.clone(),
            Json(CreateNode {
                name: "Replica".into(),
            }),
        )
        .await
        .unwrap();
        for node_id in [worker.node.id, replica.node.id] {
            state
                .store
                .lock()
                .unwrap()
                .nodes
                .get_mut(&node_id)
                .unwrap()
                .online = true;
        }
        let (status, Json(job)) = create_job(
            State(state.clone()),
            Path(cluster.id),
            owner_headers.clone(),
            Json(CreateJob {
                workload: ComputeWorkload::CpuHash,
                total_units: 2_500,
                batch_units: 1_000,
            }),
        )
        .await
        .unwrap();
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(job.total_batches, 3);
        assert!(claim_job_batch(
            State(state.clone()),
            Path(job.id),
            stranger_headers,
            Json(ClaimJob {
                node_id: worker.node.id
            })
        )
        .await
        .is_err());

        let Json(Some(claim)) = claim_job_batch(
            State(state.clone()),
            Path(job.id),
            owner_headers.clone(),
            Json(ClaimJob {
                node_id: worker.node.id,
            }),
        )
        .await
        .unwrap() else {
            panic!("expected an available batch");
        };
        assert_eq!((claim.start, claim.end), (0, 1_000));
        let digest = "a".repeat(64);
        state
            .store
            .lock()
            .unwrap()
            .nodes
            .get_mut(&replica.node.id)
            .unwrap()
            .online = false;
        assert!(commit_job_batch(
            State(state.clone()),
            Path(job.id),
            owner_headers.clone(),
            Json(CommitBatch {
                execution: None,
                lease: claim.lease.clone(),
                replica_node_id: Some(replica.node.id),
                shards: Vec::new(),
                sha256: digest.clone(),
                bytes: 128
            })
        )
        .await
        .is_err());
        state
            .store
            .lock()
            .unwrap()
            .nodes
            .get_mut(&replica.node.id)
            .unwrap()
            .online = true;
        // A worker cannot fabricate custody by naming an online peer.
        assert!(commit_job_batch(
            State(state.clone()),
            Path(job.id),
            owner_headers.clone(),
            Json(CommitBatch {
                execution: None,
                lease: claim.lease.clone(),
                replica_node_id: Some(replica.node.id),
                shards: Vec::new(),
                sha256: digest.clone(),
                bytes: 128
            })
        )
        .await
        .is_err());
        for (index, node_id) in [worker.node.id, replica.node.id].into_iter().enumerate() {
            let shard = ShardReceipt {
                shard_index: index as u8,
                node_id,
                artifact_name: format!("legacy-{}-0-{index}", job.id),
                sha256: digest.clone(),
                bytes: 128,
            };
            custody::tests::confirm(&mut state.durability.lock().unwrap().custody, &shard, now());
        }
        let commit = CommitBatch {
            execution: None,
            lease: claim.lease.clone(),
            replica_node_id: Some(replica.node.id),
            shards: Vec::new(),
            sha256: digest.clone(),
            bytes: 128,
        };
        assert_eq!(
            commit_job_batch(
                State(state.clone()),
                Path(job.id),
                owner_headers.clone(),
                Json(commit)
            )
            .await
            .unwrap(),
            StatusCode::NO_CONTENT
        );
        assert_eq!(
            commit_job_batch(
                State(state.clone()),
                Path(job.id),
                owner_headers.clone(),
                Json(CommitBatch {
                    execution: None,
                    lease: claim.lease,
                    replica_node_id: Some(replica.node.id),
                    shards: Vec::new(),
                    sha256: digest,
                    bytes: 128
                })
            )
            .await
            .unwrap(),
            StatusCode::NO_CONTENT
        );

        let Json(jobs) = list_jobs(State(state.clone()), Path(cluster.id), owner_headers)
            .await
            .unwrap();
        assert_eq!(jobs[0].completed_batches, 1);
        let Json(Some(next)) = claim_job_batch(
            State(state.clone()),
            Path(job.id),
            test_headers(owner_token),
            Json(ClaimJob {
                node_id: worker.node.id,
            }),
        )
        .await
        .unwrap() else {
            panic!("expected the next independent batch");
        };
        assert_eq!((next.start, next.end), (1_000, 2_000));
        let _ = std::fs::remove_file(file);
    }

    #[tokio::test]
    async fn large_checkpoints_require_three_nodes_for_shards_but_can_degrade_to_two_full_replicas()
    {
        let file = std::env::temp_dir().join(format!("mesh-shard-test-{}.json", Uuid::new_v4()));
        let state = AppState {
            store: Arc::new(Mutex::new(Store::default())),
            mutation_lock: Arc::new(tokio::sync::Mutex::new(())),
            data_file: Arc::new(file.clone()),
            snapshot: Arc::new(snapshot::Codec::for_test()),
            github: None,
            github_file_sha: Arc::new(Mutex::new(None)),
            node_connections: Arc::new(Mutex::new(HashMap::new())),
            durability: Arc::new(Mutex::new(DurabilityRuntime::default())),
        };
        let owner = test_headers("shard-owner-identity-token-01234567890123456789");
        let (_, Json(cluster)) = create_cluster(
            State(state.clone()),
            owner.clone(),
            Json(CreateCluster {
                name: "Shards".into(),
            }),
        )
        .await
        .unwrap();
        let (_, Json(worker)) = create_node(
            State(state.clone()),
            Path(cluster.id),
            owner.clone(),
            Json(CreateNode {
                name: "Worker".into(),
            }),
        )
        .await
        .unwrap();
        let (_, Json(peer_a)) = create_node(
            State(state.clone()),
            Path(cluster.id),
            owner.clone(),
            Json(CreateNode {
                name: "Peer A".into(),
            }),
        )
        .await
        .unwrap();
        let (_, Json(peer_b)) = create_node(
            State(state.clone()),
            Path(cluster.id),
            owner.clone(),
            Json(CreateNode {
                name: "Peer B".into(),
            }),
        )
        .await
        .unwrap();
        for node_id in [worker.node.id, peer_a.node.id, peer_b.node.id] {
            state
                .store
                .lock()
                .unwrap()
                .nodes
                .get_mut(&node_id)
                .unwrap()
                .online = true;
        }
        let (_, Json(job)) = create_job(
            State(state.clone()),
            Path(cluster.id),
            owner.clone(),
            Json(CreateJob {
                workload: ComputeWorkload::VectorAdd,
                total_units: 1_000,
                batch_units: 1_000,
            }),
        )
        .await
        .unwrap();
        let Json(Some(claim)) = claim_job_batch(
            State(state.clone()),
            Path(job.id),
            owner.clone(),
            Json(ClaimJob {
                node_id: worker.node.id,
            }),
        )
        .await
        .unwrap() else {
            panic!("expected a batch lease");
        };
        let digest = "a".repeat(64);
        let shards = vec![
            ShardReceipt {
                shard_index: 0,
                node_id: worker.node.id,
                artifact_name: "cp-1-shard-0".into(),
                sha256: "1".repeat(64),
                bytes: 32_768,
            },
            ShardReceipt {
                shard_index: 1,
                node_id: peer_a.node.id,
                artifact_name: "cp-1-shard-1".into(),
                sha256: "2".repeat(64),
                bytes: 32_768,
            },
            ShardReceipt {
                shard_index: 2,
                node_id: peer_b.node.id,
                artifact_name: "cp-1-shard-2".into(),
                sha256: "3".repeat(64),
                bytes: 32_768,
            },
        ];
        state
            .store
            .lock()
            .unwrap()
            .nodes
            .get_mut(&peer_b.node.id)
            .unwrap()
            .online = false;
        let request = || CommitBatch {
            execution: None,
            lease: claim.lease.clone(),
            replica_node_id: None,
            shards: shards.clone(),
            sha256: digest.clone(),
            bytes: SHARDING_THRESHOLD_BYTES,
        };
        assert!(commit_job_batch(
            State(state.clone()),
            Path(job.id),
            owner.clone(),
            Json(request())
        )
        .await
        .is_err());
        let replicas = vec![
            ShardReceipt {
                shard_index: 0,
                node_id: worker.node.id,
                artifact_name: "cp-1-full-0".into(),
                sha256: digest.clone(),
                bytes: SHARDING_THRESHOLD_BYTES,
            },
            ShardReceipt {
                shard_index: 1,
                node_id: peer_a.node.id,
                artifact_name: "cp-1-full-1".into(),
                sha256: digest.clone(),
                bytes: SHARDING_THRESHOLD_BYTES,
            },
        ];
        for shard in &replicas {
            custody::tests::confirm(&mut state.durability.lock().unwrap().custody, shard, now());
        }
        let fallback = CommitBatch {
            execution: None,
            lease: claim.lease.clone(),
            replica_node_id: None,
            shards: replicas,
            sha256: digest.clone(),
            bytes: SHARDING_THRESHOLD_BYTES,
        };
        assert_eq!(
            commit_job_batch(
                State(state.clone()),
                Path(job.id),
                owner.clone(),
                Json(fallback)
            )
            .await
            .unwrap(),
            StatusCode::NO_CONTENT
        );
        let Json(jobs) = list_jobs(State(state), Path(cluster.id), owner)
            .await
            .unwrap();
        assert_eq!(jobs[0].checkpoints[0].shards.len(), 2);
        assert!(jobs[0].checkpoints[0]
            .shards
            .iter()
            .all(|shard| shard.sha256 == digest));
        let _ = std::fs::remove_file(file);
    }

    #[test]
    fn older_cluster_snapshots_load_with_empty_job_state() {
        let store: Store =
            serde_json::from_str(r#"{"clusters":{},"invites":{},"nodes":{}}"#).unwrap();
        assert!(store.jobs.is_empty());
        assert!(!store.recovery.is_complete("missing"));
    }

    #[test]
    fn process_epochs_round_trip_through_javascript_numbers() {
        let epoch = coordinator_epoch(Uuid::from_u128(u128::MAX));
        assert!(epoch > 0 && epoch < (1_u64 << 53));
        assert_eq!((epoch as f64) as u64, epoch);
        assert_ne!(
            coordinator_epoch(Uuid::from_u128(1_u128 << 64)),
            coordinator_epoch(Uuid::from_u128(2_u128 << 64))
        );
    }

    #[tokio::test]
    async fn claims_and_renewals_do_not_write_storage_and_renewal_bypasses_durable_lock() {
        let file = std::env::temp_dir().join(format!("mesh-runtime-lease-{}.json", Uuid::new_v4()));
        let state = AppState {
            store: Arc::new(Mutex::new(Store::default())),
            mutation_lock: Arc::new(tokio::sync::Mutex::new(())),
            data_file: Arc::new(file.clone()),
            snapshot: Arc::new(snapshot::Codec::for_test()),
            github: None,
            github_file_sha: Arc::new(Mutex::new(None)),
            node_connections: Arc::new(Mutex::new(HashMap::new())),
            durability: Arc::new(Mutex::new(DurabilityRuntime::default())),
        };
        let owner = test_headers("runtime-owner-identity-01234567890123456789012345");
        let (_, Json(cluster)) = create_cluster(
            State(state.clone()),
            owner.clone(),
            Json(CreateCluster {
                name: "Runtime".into(),
            }),
        )
        .await
        .unwrap();
        let (_, Json(worker)) = create_node(
            State(state.clone()),
            Path(cluster.id),
            owner.clone(),
            Json(CreateNode {
                name: "Worker".into(),
            }),
        )
        .await
        .unwrap();
        state
            .store
            .lock()
            .unwrap()
            .nodes
            .get_mut(&worker.node.id)
            .unwrap()
            .online = true;
        let (_, Json(job)) = create_job(
            State(state.clone()),
            Path(cluster.id),
            owner.clone(),
            Json(CreateJob {
                workload: ComputeWorkload::CpuHash,
                total_units: 1_000,
                batch_units: 1_000,
            }),
        )
        .await
        .unwrap();
        let before = std::fs::read(&file).unwrap();
        let Json(Some(claim)) = claim_job_batch(
            State(state.clone()),
            Path(job.id),
            owner.clone(),
            Json(ClaimJob {
                node_id: worker.node.id,
            }),
        )
        .await
        .unwrap() else {
            panic!("expected lease");
        };
        assert_eq!(std::fs::read(&file).unwrap(), before);
        let durable_lock = state.mutation_lock.lock().await;
        let Json(renewed) = tokio::time::timeout(
            Duration::from_millis(100),
            renew_job_batch(
                State(state.clone()),
                Path(job.id),
                owner.clone(),
                Json(RenewJob {
                    lease: claim.lease.clone(),
                }),
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(renewed.generation, claim.lease.generation);
        assert!(renewed.expires_at_ms >= claim.lease.expires_at_ms);
        drop(durable_lock);
        assert_eq!(std::fs::read(&file).unwrap(), before);
        assert!(renew_job_batch(
            State(state.clone()),
            Path(Uuid::new_v4()),
            owner.clone(),
            Json(RenewJob {
                lease: renewed.clone()
            })
        )
        .await
        .is_err());
        assert!(renew_job_batch(
            State(state.clone()),
            Path(job.id),
            test_headers("stranger-identity-012345678901234567890123456789"),
            Json(RenewJob {
                lease: renewed.clone()
            })
        )
        .await
        .is_err());
        let mut stale = renewed.clone();
        stale.generation += 1;
        assert!(renew_job_batch(
            State(state.clone()),
            Path(job.id),
            owner.clone(),
            Json(RenewJob { lease: stale })
        )
        .await
        .is_err());
        state
            .store
            .lock()
            .unwrap()
            .nodes
            .get_mut(&worker.node.id)
            .unwrap()
            .paused = true;
        assert!(renew_job_batch(
            State(state.clone()),
            Path(job.id),
            owner,
            Json(RenewJob { lease: renewed })
        )
        .await
        .is_err());
        let _ = std::fs::remove_file(file);
    }
}
