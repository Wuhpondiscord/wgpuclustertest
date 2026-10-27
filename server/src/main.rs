use axum::{
    extract::{ws::{Message, WebSocket, WebSocketUpgrade}, Path, Query, State},
    http::{HeaderMap, StatusCode},
    extract::DefaultBodyLimit,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::HashMap, fs::OpenOptions, io::Write, net::SocketAddr, path::PathBuf, sync::{Arc, Mutex}, time::{SystemTime, UNIX_EPOCH}};
use tokio::time::{interval, Duration};
use tower_http::services::ServeDir;
use uuid::Uuid;

#[allow(dead_code)]
mod checkpoint;
mod erasure;
mod recovery;

const IDENTITY_HEADER: &str = "x-mesh-identity";
const INVITE_TTL_SECS: u64 = 7 * 24 * 60 * 60;
const NODE_PING_INTERVAL_SECS: u64 = 15;
const NODE_HEARTBEAT_TIMEOUT_SECS: u64 = 45;
const MAX_STORAGE_CONFLICT_RETRIES: usize = 3;
const JOB_LEASE_DURATION_MS: u64 = 45_000;
const WORKER_RECONNECT_GRACE_MS: u64 = 10_000;
const MAX_JOB_UNITS: u64 = 10_000_000;
const MAX_JOB_BATCHES: u64 = 256;
const MAX_PEER_ARTIFACT_BYTES: u64 = 8 * 1024 * 1024;
const SHARDING_THRESHOLD_BYTES: u64 = 64 * 1024;

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
enum Role { Owner, Member }

#[derive(Clone, Deserialize, Serialize)]
struct Cluster {
    id: Uuid,
    name: String,
    owner_id: String,
    members: HashMap<String, Role>,
}

#[derive(Clone, Deserialize, Serialize)]
struct Invite { cluster_id: Uuid, expires_at: u64 }

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
    clusters: HashMap<Uuid, Cluster>,
    invites: HashMap<String, Invite>,
    nodes: HashMap<Uuid, Node>,
    #[serde(default)]
    jobs: HashMap<Uuid, ComputeJob>,
    #[serde(default)]
    recovery: recovery::RecoveryLedger,
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
}

#[derive(Clone, Deserialize, Serialize)]
struct ShardReceipt {
    shard_index: u8,
    node_id: Uuid,
    artifact_name: String,
    sha256: String,
    bytes: u64,
}

#[derive(Clone)]
struct GitHubStorage { token: String, repo: String, branch: String, path: String }
#[derive(Clone)]
struct AppState {
    store: Arc<Mutex<Store>>,
    // Serializes durable state changes while GitHub/file persistence runs off the async runtime.
    mutation_lock: Arc<tokio::sync::Mutex<()>>,
    data_file: Arc<PathBuf>,
    github: Option<Arc<GitHubStorage>>,
    github_file_sha: Arc<Mutex<Option<String>>>,
    node_connections: Arc<Mutex<HashMap<Uuid, NodeConnection>>>,
}

#[derive(Clone)]
struct NodeConnection {
    session_id: Uuid,
    cluster_id: Uuid,
    sender: tokio::sync::mpsc::Sender<Message>,
}

#[derive(Deserialize)] struct CreateCluster { name: String }
#[derive(Deserialize)] struct RenameCluster { name: String }
#[derive(Deserialize)] struct CreateNode { name: String }
#[derive(Deserialize)] struct CreateJob { #[serde(default)] workload: ComputeWorkload, total_units: u64, batch_units: u64 }
#[derive(Deserialize)] struct ClaimJob { node_id: Uuid }
#[derive(Deserialize)] struct RenewJob { lease: recovery::TaskLease }
#[derive(Deserialize)] struct CommitBatch {
    lease: recovery::TaskLease,
    #[serde(default)] replica_node_id: Option<Uuid>,
    #[serde(default)] shards: Vec<ShardReceipt>,
    sha256: String,
    bytes: u64,
}
#[derive(Deserialize)] struct JoinInvite { token: String }
#[derive(Deserialize)] struct NodeSocketQuery { token: String }
#[derive(Serialize)]
struct ClusterSummary { id: Uuid, name: String, role: Role, members: usize, nodes: usize, online_nodes: usize }
#[derive(Serialize)]
struct NodeSummary { id: Uuid, cluster_id: Uuid, name: String, paused: bool, online: bool, last_seen: Option<u64> }
#[derive(Serialize)]
struct NodeCreated { node: NodeSummary, token: String }
#[derive(Serialize)]
struct CheckpointSummary { batch_index: u64, sha256: String, bytes: u64, shards: Vec<ShardReceipt> }
#[derive(Serialize)]
struct JobSummary { id: Uuid, cluster_id: Uuid, created_at: u64, workload: ComputeWorkload, total_units: u64, batch_units: u64, total_batches: u64, completed_batches: usize, checkpoints: Vec<CheckpointSummary> }
#[derive(Serialize)]
struct JobLease { job_id: Uuid, batch_index: u64, start: u64, end: u64, lease: recovery::TaskLease }
#[derive(Serialize)]
struct InvitePreview { cluster_id: Uuid, cluster_name: String, expires_at: u64 }
#[derive(Serialize)]
struct MemberSummary { id: String, role: Role }
#[derive(Serialize)]
struct InviteSummary { token: String, expires_at: u64 }
#[derive(Serialize)]
struct InviteCreated { token: String, expires_at: u64 }

#[derive(Debug)]
struct ApiError(StatusCode, String);
impl IntoResponse for ApiError {
    fn into_response(self) -> Response { (self.0, self.1).into_response() }
}
type ApiResult<T> = Result<T, ApiError>;

fn should_retry_storage_conflict(error: &ApiError, attempt: usize, github_configured: bool) -> bool {
    github_configured && error.0 == StatusCode::CONFLICT && attempt < MAX_STORAGE_CONFLICT_RETRIES
}

fn now() -> u64 { SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() }
fn identity(headers: &HeaderMap) -> ApiResult<String> {
    let id = headers.get(IDENTITY_HEADER).and_then(|v| v.to_str().ok()).unwrap_or("").trim();
    if id.len() < 32 || id.len() > 128 { return Err(ApiError(StatusCode::UNAUTHORIZED, "A valid Mesh identity is required".into())); }
    Ok(secret_hash(id))
}
fn secret_hash(value: &str) -> String { format!("{:x}", Sha256::digest(value.as_bytes())) }
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
    let cluster_nodes: Vec<&Node> = store.nodes.values().filter(|node| node.cluster_id == cluster.id).collect();
    ClusterSummary {
        id: cluster.id,
        name: cluster.name.clone(),
        role: *cluster.members.get(user).unwrap_or(&Role::Member),
        members: cluster.members.len(),
        nodes: cluster_nodes.len(),
        online_nodes: cluster_nodes.iter().filter(|node| node.online && !node.paused).count(),
    }
}
fn require_member<'a>(store: &'a Store, cluster_id: Uuid, user: &str) -> ApiResult<&'a Cluster> {
    store.clusters.get(&cluster_id).filter(|cluster| cluster.members.contains_key(user))
        .ok_or(ApiError(StatusCode::NOT_FOUND, "Cluster not found".into()))
}
fn require_owner<'a>(store: &'a Store, cluster_id: Uuid, user: &str) -> ApiResult<&'a Cluster> {
    let cluster = require_member(store, cluster_id, user)?;
    if cluster.owner_id != user { return Err(ApiError(StatusCode::FORBIDDEN, "Only the cluster owner can do that".into())); }
    Ok(cluster)
}
fn node_summary(node: &Node) -> NodeSummary {
    NodeSummary { id: node.id, cluster_id: node.cluster_id, name: node.name.clone(), paused: node.paused, online: node.online, last_seen: node.last_seen }
}
fn job_summary(job: &ComputeJob) -> JobSummary {
    let mut checkpoints = job.outputs.iter().map(|(batch_index, output)| CheckpointSummary {
        batch_index: *batch_index, sha256: output.sha256.clone(), bytes: output.bytes, shards: output.shards.clone(),
    }).collect::<Vec<_>>();
    checkpoints.sort_by_key(|checkpoint| checkpoint.batch_index);
    JobSummary { id: job.id, cluster_id: job.cluster_id, created_at: job.created_at, workload: job.workload, total_units: job.total_units,
        batch_units: job.batch_units, total_batches: job.total_units.div_ceil(job.batch_units), completed_batches: job.outputs.len(), checkpoints }
}
fn persist(state: &AppState, store: &Store) -> ApiResult<()> {
    let serialized = serde_json::to_vec(store).map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Could not serialize state".into()))?;
    if let Some(github) = state.github.as_deref() {
        let agent = ureq::AgentBuilder::new().timeout(Duration::from_secs(20)).build();
        let mut file_sha = state.github_file_sha.lock().map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Storage unavailable".into()))?;
        let url = format!("https://api.github.com/repos/{}/contents/{}", github.repo, github.path);
        let mut body = serde_json::json!({
            "message": "Update Mesh cluster state",
            "content": BASE64.encode(&serialized),
            "branch": github.branch,
        });
        if let Some(sha) = file_sha.as_ref() { body["sha"] = serde_json::Value::String(sha.clone()); }
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
        let result: serde_json::Value = response.into_json().map_err(|_| ApiError(StatusCode::BAD_GATEWAY, "GitHub returned an invalid storage response".into()))?;
        *file_sha = result.pointer("/content/sha").and_then(serde_json::Value::as_str).map(str::to_owned);
        if file_sha.is_none() { return Err(ApiError(StatusCode::BAD_GATEWAY, "GitHub did not return the saved state file revision".into())); }
        return Ok(());
    }
    if let Some(parent) = state.data_file.parent().filter(|path| !path.as_os_str().is_empty()) { std::fs::create_dir_all(parent).map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Could not create data directory".into()))?; }
    let temp_path = state.data_file.with_extension("json.tmp");
    let mut temp = OpenOptions::new().write(true).create(true).truncate(true).open(&temp_path)
        .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Could not open temporary state file".into()))?;
    temp.write_all(&serialized).and_then(|_| temp.sync_all())
        .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Could not write temporary state file".into()))?;
    std::fs::rename(&temp_path, state.data_file.as_path())
        .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Could not atomically replace persisted state".into()))
}

async fn mutate_store<R, F>(state: &AppState, change: F) -> ApiResult<R>
where
    R: Send,
    F: FnMut(&mut Store) -> ApiResult<R>,
{
    let _mutation = state.mutation_lock.lock().await;
    let mut next = state.store.lock()
        .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Store unavailable".into()))?
        .clone();
    let mut change = change;
    for attempt in 0..=MAX_STORAGE_CONFLICT_RETRIES {
        let result = change(&mut next)?;
        let persist_state = state.clone();
        let persistence_snapshot = next.clone();
        let persisted = tokio::task::spawn_blocking(move || persist(&persist_state, &persistence_snapshot))
            .await
            .map_err(|_| ApiError(StatusCode::SERVICE_UNAVAILABLE, "Storage worker stopped unexpectedly".into()))?;
        if let Err(error) = persisted {
            if !should_retry_storage_conflict(&error, attempt, state.github.is_some()) {
                return Err(error);
            }
            let config = state.github.clone().expect("GitHub storage is configured");
            let (mut latest, latest_sha) = tokio::task::spawn_blocking(move || load_github_store(&config))
                .await
                .map_err(|_| ApiError(StatusCode::SERVICE_UNAVAILABLE, "Storage worker stopped unexpectedly".into()))?
                .map_err(|_| ApiError(StatusCode::BAD_GATEWAY, "Could not reload the latest GitHub state after a write conflict".into()))?;
            {
                let current = state.store.lock()
                    .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Store unavailable".into()))?;
                for (id, node) in latest.nodes.iter_mut() {
                    if let Some(runtime_node) = current.nodes.get(id) {
                        node.online = runtime_node.online;
                        node.last_seen = runtime_node.last_seen;
                    }
                }
                latest.recovery.preserve_runtime_from(&current.recovery);
            }
            *state.github_file_sha.lock()
                .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Storage unavailable".into()))? = latest_sha;
            next = latest;
            continue;
        }

        let mut current = state.store.lock()
            .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Store unavailable".into()))?;
        // Heartbeats are runtime-only state. Preserve them when committing a durable snapshot.
        next.recovery.preserve_runtime_from(&current.recovery);
        for (id, node) in next.nodes.iter_mut() {
            if let Some(runtime_node) = current.nodes.get(id) {
                node.online = runtime_node.online;
                node.last_seen = runtime_node.last_seen;
            }
        }
        *current = next;
        return Ok(result);
    }
    Err(ApiError(StatusCode::CONFLICT, "Cluster state kept changing. Please retry.".into()))
}

async fn refresh_invite_from_github(state: &AppState, token: &str) -> ApiResult<bool> {
    let Some(config) = state.github.clone() else { return Ok(false); };
    let _mutation = state.mutation_lock.lock().await;
    let key = secret_hash(token);
    if state.store.lock().map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Store unavailable".into()))?
        .invites.get(&key).is_some_and(|invite| invite.expires_at > now()) { return Ok(true); }

    let (fresh_store, fresh_sha) = tokio::task::spawn_blocking(move || load_github_store(&config))
        .await
        .map_err(|_| ApiError(StatusCode::SERVICE_UNAVAILABLE, "Storage worker stopped unexpectedly".into()))?
        .map_err(|_| ApiError(StatusCode::BAD_GATEWAY, "Could not refresh invite state from GitHub storage".into()))?;
    let found = fresh_store.invites.get(&key).is_some_and(|invite| invite.expires_at > now());
    if found {
        let mut store = state.store.lock().map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Store unavailable".into()))?;
        let mut fresh_store = fresh_store;
        fresh_store.recovery.preserve_runtime_from(&store.recovery);
        for (id, node) in fresh_store.nodes.iter_mut() {
            if let Some(runtime_node) = store.nodes.get(id) { node.online = runtime_node.online; node.last_seen = runtime_node.last_seen; }
        }
        *store = fresh_store;
        let mut file_sha = state.github_file_sha.lock().map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Storage unavailable".into()))?;
        *file_sha = fresh_sha;
    }
    Ok(found)
}

fn has_active_invite(state: &AppState, token: &str) -> ApiResult<bool> {
    let store = state.store.lock().map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Store unavailable".into()))?;
    Ok(store.invites.get(&secret_hash(token)).is_some_and(|invite| invite.expires_at > now()))
}

fn load_github_store(config: &GitHubStorage) -> Result<(Store, Option<String>), String> {
    let repo_url = format!("https://api.github.com/repos/{}", config.repo);
    let agent = ureq::AgentBuilder::new().timeout(Duration::from_secs(20)).build();
    let repo_response = agent.get(&repo_url)
        .set("Authorization", &format!("Bearer {}", config.token))
        .set("Accept", "application/vnd.github+json")
        .set("X-GitHub-Api-Version", "2022-11-28")
        .set("User-Agent", "mesh-cluster-coordinator")
        .call()
        .map_err(|_| "Could not access the configured GitHub storage repository. Check GH_TOKEN and MESH_STORAGE_REPO.".to_string())?;
    let repo_info: serde_json::Value = repo_response.into_json().map_err(|_| "GitHub returned invalid repository metadata".to_string())?;
    if repo_info.get("private").and_then(serde_json::Value::as_bool) != Some(true) {
        return Err("MESH_STORAGE_REPO must point to a private GitHub repository".into());
    }
    let url = format!("https://api.github.com/repos/{}/contents/{}?ref={}", config.repo, config.path, config.branch);
    let response = agent.get(&url)
        .set("Authorization", &format!("Bearer {}", config.token))
        .set("Accept", "application/vnd.github+json")
        .set("X-GitHub-Api-Version", "2022-11-28")
        .set("User-Agent", "mesh-cluster-coordinator")
        .call();
    let response = match response {
        Ok(response) => response,
        Err(ureq::Error::Status(404, _)) => return Ok((Store::default(), None)),
        Err(_) => return Err("Could not load state from the private GitHub storage repository. Check GH_TOKEN permissions, repo name, and branch.".into()),
    };
    let result: serde_json::Value = response.into_json().map_err(|_| "GitHub returned an invalid storage response".to_string())?;
    let encoded = result.get("content").and_then(serde_json::Value::as_str).ok_or_else(|| "GitHub state file has no content".to_string())?;
    let bytes = BASE64.decode(encoded.replace('\n', "")).map_err(|_| "GitHub state file is not valid base64".to_string())?;
    let store = serde_json::from_slice(&bytes).map_err(|_| "GitHub state file is not valid Mesh data".to_string())?;
    let sha = result.get("sha").and_then(serde_json::Value::as_str).map(str::to_owned).ok_or_else(|| "GitHub state file has no revision".to_string())?;
    Ok((store, Some(sha)))
}

async fn health() -> &'static str { "mesh coordinator ready" }
async fn list_clusters(State(state): State<AppState>, headers: HeaderMap) -> ApiResult<Json<Vec<ClusterSummary>>> {
    let user = identity(&headers)?;
    let store = state.store.lock().map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Store unavailable".into()))?;
    Ok(Json(store.clusters.values().filter(|c| c.members.contains_key(&user)).map(|c| summary(c, &store, &user)).collect()))
}
async fn create_cluster(State(state): State<AppState>, headers: HeaderMap, Json(input): Json<CreateCluster>) -> ApiResult<(StatusCode, Json<ClusterSummary>)> {
    let user = identity(&headers)?;
    let name = cluster_name(&input.name).ok_or(ApiError(StatusCode::BAD_REQUEST, "Name must be 1–80 characters".into()))?;
    let result = mutate_store(&state, move |store| {
        let mut members = HashMap::new(); members.insert(user.clone(), Role::Owner);
        let cluster = Cluster { id: Uuid::new_v4(), name: name.clone(), owner_id: user.clone(), members };
        store.clusters.insert(cluster.id, cluster.clone());
        Ok(summary(&cluster, store, &user))
    }).await?;
    Ok((StatusCode::CREATED, Json(result)))
}
async fn rename_cluster(State(state): State<AppState>, Path(id): Path<Uuid>, headers: HeaderMap, Json(input): Json<RenameCluster>) -> ApiResult<Json<ClusterSummary>> {
    let user = identity(&headers)?;
    let name = cluster_name(&input.name).ok_or(ApiError(StatusCode::BAD_REQUEST, "Name must be 1–80 characters".into()))?;
    let result = mutate_store(&state, move |store| {
        require_owner(store, id, &user)?;
        let cluster = store.clusters.get_mut(&id).expect("checked above");
        cluster.name = name.clone();
        let cluster = cluster.clone();
        Ok(summary(&cluster, store, &user))
    }).await?;
    Ok(Json(result))
}
async fn list_jobs(State(state): State<AppState>, Path(cluster_id): Path<Uuid>, headers: HeaderMap) -> ApiResult<Json<Vec<JobSummary>>> {
    let user = identity(&headers)?;
    let store = state.store.lock().map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Store unavailable".into()))?;
    require_member(&store, cluster_id, &user)?;
    Ok(Json(store.jobs.values().filter(|job| job.cluster_id == cluster_id).map(job_summary).collect()))
}
async fn create_job(State(state): State<AppState>, Path(cluster_id): Path<Uuid>, headers: HeaderMap, Json(input): Json<CreateJob>) -> ApiResult<(StatusCode, Json<JobSummary>)> {
    let user = identity(&headers)?;
    if input.total_units == 0 || input.total_units > MAX_JOB_UNITS || input.batch_units < 1_000 || input.batch_units > 500_000
        || input.total_units.div_ceil(input.batch_units) > MAX_JOB_BATCHES {
        return Err(ApiError(StatusCode::BAD_REQUEST, "Job must contain 1–10,000,000 units, batches of 1,000–500,000 units, and at most 256 batches".into()));
    }
    let job_id = Uuid::new_v4();
    let workload = input.workload;
    let total_units = input.total_units;
    let batch_units = input.batch_units;
    let result = mutate_store(&state, move |store| {
        require_member(store, cluster_id, &user)?;
        let total_batches = total_units.div_ceil(batch_units);
        for index in 0..total_batches {
            store.recovery.add_task(format!("{job_id}:{index}"))
                .map_err(|_| ApiError(StatusCode::CONFLICT, "Could not register a job batch".into()))?;
        }
        let job = ComputeJob { id: job_id, cluster_id, created_at: now(), workload, total_units, batch_units, outputs: HashMap::new() };
        let summary = job_summary(&job);
        store.jobs.insert(job_id, job);
        Ok(summary)
    }).await?;
    Ok((StatusCode::CREATED, Json(result)))
}
async fn claim_job_batch(State(state): State<AppState>, Path(job_id): Path<Uuid>, headers: HeaderMap, Json(input): Json<ClaimJob>) -> ApiResult<Json<Option<JobLease>>> {
    let user = identity(&headers)?;
    let node_id = input.node_id;
    // Claims serialize with durable changes, but never write storage themselves.
    let _mutation = state.mutation_lock.lock().await;
    let mut current = state.store.lock().map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Store unavailable".into()))?;
    let result = (|store: &mut Store| {
        let job = store.jobs.get(&job_id).cloned().ok_or(ApiError(StatusCode::NOT_FOUND, "Job not found".into()))?;
        require_member(store, job.cluster_id, &user)?;
        let node = store.nodes.get(&node_id).filter(|node| node.cluster_id == job.cluster_id && node.owner_id == user)
            .ok_or(ApiError(StatusCode::FORBIDDEN, "This node is not owned by your Mesh identity in this cluster".into()))?;
        if !node.online || node.paused { return Err(ApiError(StatusCode::CONFLICT, "The worker node must be online and active".into())); }
        let now_ms = now().saturating_mul(1_000);
        let total_batches = job.total_units.div_ceil(job.batch_units);
        for batch_index in 0..total_batches {
            if job.outputs.contains_key(&batch_index) { continue; }
            let task_id = format!("{job_id}:{batch_index}");
            match store.recovery.claim(&task_id, node_id.to_string(), now_ms, JOB_LEASE_DURATION_MS) {
                Ok(lease) => {
                    let start = batch_index.saturating_mul(job.batch_units);
                    let end = start.saturating_add(job.batch_units).min(job.total_units);
                    return Ok(Some(JobLease { job_id, batch_index, start, end, lease }));
                }
                Err(recovery::LeaseError::LeaseBusy | recovery::LeaseError::AlreadyComplete) => continue,
                Err(_) => return Err(ApiError(StatusCode::CONFLICT, "Could not lease this job batch".into())),
            }
        }
        Ok(None)
    })(&mut current)?;
    Ok(Json(result))
}
async fn renew_job_batch(State(state): State<AppState>, Path(job_id): Path<Uuid>, headers: HeaderMap, Json(input): Json<RenewJob>) -> ApiResult<Json<recovery::TaskLease>> {
    let user = identity(&headers)?;
    // Renewal must remain available while a slow durable write is in progress.
    let mut store = state.store.lock().map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Store unavailable".into()))?;
    let job = store.jobs.get(&job_id).ok_or(ApiError(StatusCode::NOT_FOUND, "Job not found".into()))?;
    require_member(&store, job.cluster_id, &user)?;
    let prefix = format!("{job_id}:");
    let batch = input.lease.task_id.strip_prefix(&prefix).and_then(|value| value.parse::<u64>().ok());
    if !batch.is_some_and(|index| index < job.total_units.div_ceil(job.batch_units)) {
        return Err(ApiError(StatusCode::BAD_REQUEST, "Lease does not belong to this job".into()));
    }
    let worker_id = Uuid::parse_str(&input.lease.worker_id).map_err(|_| ApiError(StatusCode::FORBIDDEN, "Invalid lease worker".into()))?;
    let node = store.nodes.get(&worker_id).filter(|node| node.cluster_id == job.cluster_id && node.owner_id == user)
        .ok_or(ApiError(StatusCode::FORBIDDEN, "The lease worker is not owned by your Mesh identity".into()))?;
    if node.paused { return Err(ApiError(StatusCode::CONFLICT, "The worker node is paused".into())); }
    if !node.online { return Err(ApiError(StatusCode::SERVICE_UNAVAILABLE, "The worker is reconnecting; retry renewal within its grace period".into())); }
    let renewed = store.recovery.renew(&input.lease, now().saturating_mul(1_000), JOB_LEASE_DURATION_MS)
        .map_err(|_| ApiError(StatusCode::CONFLICT, "The worker lease expired or was reassigned; claim the batch again".into()))?;
    Ok(Json(renewed))
}
async fn commit_job_batch(State(state): State<AppState>, Path(job_id): Path<Uuid>, headers: HeaderMap, Json(input): Json<CommitBatch>) -> ApiResult<StatusCode> {
    let user = identity(&headers)?;
    if input.bytes == 0 || input.bytes > MAX_PEER_ARTIFACT_BYTES || input.sha256.len() != 64 || !input.sha256.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(ApiError(StatusCode::BAD_REQUEST, "Checkpoint artifact metadata is invalid".into()));
    }
    let result = mutate_store(&state, move |store| {
        let job = store.jobs.get(&job_id).cloned().ok_or(ApiError(StatusCode::NOT_FOUND, "Job not found".into()))?;
        require_member(store, job.cluster_id, &user)?;
        let worker_id = Uuid::parse_str(&input.lease.worker_id).map_err(|_| ApiError(StatusCode::FORBIDDEN, "Invalid lease worker".into()))?;
        let worker = store.nodes.get(&worker_id).filter(|node| node.cluster_id == job.cluster_id && node.owner_id == user)
            .ok_or(ApiError(StatusCode::FORBIDDEN, "The lease worker is not owned by your Mesh identity".into()))?;
        let prefix = format!("{job_id}:");
        let batch_index = input.lease.task_id.strip_prefix(&prefix).and_then(|value| value.parse::<u64>().ok())
            .ok_or(ApiError(StatusCode::BAD_REQUEST, "Lease does not belong to this job".into()))?;
        if batch_index >= job.total_units.div_ceil(job.batch_units) { return Err(ApiError(StatusCode::BAD_REQUEST, "Lease batch is out of range".into())); }
        if let Some(existing) = job.outputs.get(&batch_index) {
            return if existing.sha256 == input.sha256 { Ok(()) } else { Err(ApiError(StatusCode::CONFLICT, "This batch already committed a different artifact".into())) };
        }
        // Legacy browser clients may still submit a two-copy receipt. New clients
        // submit two copies for small artifacts and three XOR-coded shards above
        // the threshold.
        let receipts = if input.shards.is_empty() {
            let replica_id = input.replica_node_id.ok_or(ApiError(StatusCode::BAD_REQUEST, "Checkpoint shard receipts are required".into()))?;
            vec![
                ShardReceipt { shard_index: 0, node_id: worker_id, artifact_name: format!("legacy-{job_id}-{batch_index}-0"), sha256: input.sha256.clone(), bytes: input.bytes },
                ShardReceipt { shard_index: 1, node_id: replica_id, artifact_name: format!("legacy-{job_id}-{batch_index}-1"), sha256: input.sha256.clone(), bytes: input.bytes },
            ]
        } else { input.shards.clone() };
        // A large artifact can degrade to two complete copies if a shard peer
        // drops during transfer. Treat only a complete three-receipt manifest
        // as 2+1 coded; two receipts are the replicated recovery policy at any size.
        let erasure_coded = input.shards.len() == 3 && input.bytes >= SHARDING_THRESHOLD_BYTES;
        let expected_shards = if erasure_coded { 3 } else { 2 };
        if receipts.len() != expected_shards
            || receipts.iter().enumerate().any(|(index, shard)| shard.shard_index as usize != index
                || shard.artifact_name.is_empty() || shard.artifact_name.len() > 128
                || shard.bytes == 0 || shard.bytes > MAX_PEER_ARTIFACT_BYTES
                || shard.sha256.len() != 64 || !shard.sha256.bytes().all(|byte| byte.is_ascii_hexdigit()))
            || receipts.iter().map(|shard| shard.node_id).collect::<std::collections::HashSet<_>>().len() != expected_shards
            || receipts.first().map(|shard| shard.node_id) != Some(worker_id)
        {
            return Err(ApiError(StatusCode::BAD_REQUEST, "Shard manifest is incomplete or invalid".into()));
        }
        let expected_shard_bytes = if erasure_coded { input.bytes.div_ceil(2) } else { input.bytes };
        if receipts.iter().any(|shard| shard.bytes != expected_shard_bytes
            || (!erasure_coded && shard.sha256.to_lowercase() != input.sha256.to_lowercase()))
        {
            return Err(ApiError(StatusCode::BAD_REQUEST, "Shard sizes or replicated digests do not match the checkpoint".into()));
        }
        if receipts.iter().any(|shard| !store.nodes.get(&shard.node_id).is_some_and(|node|
            node.cluster_id == job.cluster_id && !node.paused && node.online))
        {
            return Err(ApiError(StatusCode::CONFLICT, if erasure_coded {
                "2+1 sharding requires three distinct active cluster nodes".into()
            } else { "Two active cluster nodes must acknowledge the replicated checkpoint".into() }));
        }
        store.recovery.commit_result(&input.lease, now().saturating_mul(1_000), input.sha256.clone())
            .map_err(|error| match error {
                recovery::LeaseError::LeaseExpired => ApiError(StatusCode::CONFLICT, "The worker lease expired; claim the batch again".into()),
                recovery::LeaseError::StaleLease => ApiError(StatusCode::CONFLICT, "This worker lease is stale".into()),
                _ => ApiError(StatusCode::CONFLICT, "The result could not be committed".into()),
            })?;
        store.jobs.get_mut(&job_id).expect("job was checked").outputs.insert(batch_index, BatchOutput {
            sha256: input.sha256.clone(), bytes: input.bytes, worker_node_id: worker_id,
            replica_node_id: receipts[1].node_id, committed_at: now(), shards: receipts,
        });
        Ok(())
    }).await?;
    let _ = result;
    Ok(StatusCode::NO_CONTENT)
}
async fn delete_cluster(State(state): State<AppState>, Path(id): Path<Uuid>, headers: HeaderMap) -> ApiResult<StatusCode> {
    let user = identity(&headers)?;
    let removed_nodes = mutate_store(&state, move |store| {
        require_owner(store, id, &user)?;
        let removed_nodes: Vec<Uuid> = store.nodes.values().filter(|node| node.cluster_id == id).map(|node| node.id).collect();
        let removed_jobs = store.jobs.values().filter(|job| job.cluster_id == id).cloned().collect::<Vec<_>>();
        for job in removed_jobs {
            for index in 0..job.total_units.div_ceil(job.batch_units) { store.recovery.remove_task(&format!("{}:{index}", job.id)); }
        }
        store.jobs.retain(|_, job| job.cluster_id != id);
        store.clusters.remove(&id); store.nodes.retain(|_, node| node.cluster_id != id); store.invites.retain(|_, inv| inv.cluster_id != id);
        Ok(removed_nodes)
    }).await?;
    if let Ok(mut connections) = state.node_connections.lock() { for node_id in removed_nodes { connections.remove(&node_id); } }
    Ok(StatusCode::NO_CONTENT)
}
async fn create_invite(State(state): State<AppState>, Path(id): Path<Uuid>, headers: HeaderMap) -> ApiResult<(StatusCode, Json<InviteCreated>)> {
    let user = identity(&headers)?;
    let result = mutate_store(&state, move |store| {
        require_owner(store, id, &user)?;
        store.invites.retain(|_, invite| invite.expires_at > now());
        let token = Uuid::new_v4().simple().to_string() + &Uuid::new_v4().simple().to_string();
        let expires_at = now() + INVITE_TTL_SECS;
        store.invites.insert(secret_hash(&token), Invite { cluster_id: id, expires_at });
        Ok(InviteCreated { token, expires_at })
    }).await?;
    Ok((StatusCode::CREATED, Json(result)))
}
async fn list_invites(State(state): State<AppState>, Path(id): Path<Uuid>, headers: HeaderMap) -> ApiResult<Json<Vec<InviteSummary>>> {
    let user = identity(&headers)?;
    let mut store = state.store.lock().map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Store unavailable".into()))?;
    require_owner(&store, id, &user)?;
    let invites = store.invites.iter().filter(|(_, invite)| invite.cluster_id == id && invite.expires_at > now()).map(|(token, invite)| InviteSummary { token: token.clone(), expires_at: invite.expires_at }).collect();
    Ok(Json(invites))
}
async fn revoke_invite(State(state): State<AppState>, Path((id, token)): Path<(Uuid, String)>, headers: HeaderMap) -> ApiResult<StatusCode> {
    let user = identity(&headers)?;
    mutate_store(&state, move |store| {
        require_owner(store, id, &user)?;
        if !store.invites.get(&token).is_some_and(|invite| invite.cluster_id == id) { return Err(ApiError(StatusCode::NOT_FOUND, "Invite not found".into())); }
        store.invites.remove(&token);
        Ok(())
    }).await?;
    Ok(StatusCode::NO_CONTENT)
}
async fn preview_invite(State(state): State<AppState>, Path(token): Path<String>) -> ApiResult<Json<InvitePreview>> {
    if !has_active_invite(&state, &token)? && !refresh_invite_from_github(&state, &token).await? {
        return Err(ApiError(StatusCode::NOT_FOUND, "Invite is invalid or expired".into()));
    }
    let store = state.store.lock().map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Store unavailable".into()))?;
    let invite = store.invites.get(&secret_hash(&token)).filter(|i| i.expires_at > now()).ok_or(ApiError(StatusCode::NOT_FOUND, "Invite is invalid or expired".into()))?;
    let cluster = store.clusters.get(&invite.cluster_id).ok_or(ApiError(StatusCode::NOT_FOUND, "Cluster not found".into()))?;
    Ok(Json(InvitePreview { cluster_id: cluster.id, cluster_name: cluster.name.clone(), expires_at: invite.expires_at }))
}
async fn join_invite(State(state): State<AppState>, headers: HeaderMap, Json(input): Json<JoinInvite>) -> ApiResult<Json<ClusterSummary>> {
    let user = identity(&headers)?;
    if !has_active_invite(&state, &input.token)? && !refresh_invite_from_github(&state, &input.token).await? {
        return Err(ApiError(StatusCode::NOT_FOUND, "Invite is invalid or expired".into()));
    }
    let result = mutate_store(&state, move |store| {
        let invite = store.invites.get(&secret_hash(&input.token)).filter(|i| i.expires_at > now()).cloned().ok_or(ApiError(StatusCode::NOT_FOUND, "Invite is invalid or expired".into()))?;
        let cluster = store.clusters.get_mut(&invite.cluster_id).ok_or(ApiError(StatusCode::NOT_FOUND, "Cluster not found".into()))?;
        cluster.members.entry(user.clone()).or_insert(Role::Member);
        let cluster = cluster.clone();
        Ok(summary(&cluster, store, &user))
    }).await?;
    Ok(Json(result))
}
async fn leave_cluster(State(state): State<AppState>, Path(id): Path<Uuid>, headers: HeaderMap) -> ApiResult<StatusCode> {
    let user = identity(&headers)?;
    mutate_store(&state, move |store| {
        let cluster = require_member(store, id, &user)?;
        if cluster.owner_id == user { return Err(ApiError(StatusCode::BAD_REQUEST, "Transfer ownership or delete this cluster before leaving".into())); }
        store.clusters.get_mut(&id).expect("checked above").members.remove(&user);
        Ok(())
    }).await?;
    Ok(StatusCode::NO_CONTENT)
}
async fn list_members(State(state): State<AppState>, Path(id): Path<Uuid>, headers: HeaderMap) -> ApiResult<Json<Vec<MemberSummary>>> {
    let user = identity(&headers)?;
    let store = state.store.lock().map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Store unavailable".into()))?;
    let cluster = require_member(&store, id, &user)?;
    Ok(Json(cluster.members.iter().map(|(id, role)| MemberSummary { id: format!("member-{}", id.get(..8).unwrap_or(id)), role: *role }).collect()))
}
async fn remove_member(State(state): State<AppState>, Path((id, member_id)): Path<(Uuid, String)>, headers: HeaderMap) -> ApiResult<StatusCode> {
    let user = identity(&headers)?;
    let removed_nodes = mutate_store(&state, move |store| {
        require_owner(store, id, &user)?;
        let cluster = store.clusters.get_mut(&id).expect("checked above");
        let target = cluster.members.keys().find(|key| key.get(..8).is_some_and(|prefix| format!("member-{prefix}") == member_id)).cloned().ok_or(ApiError(StatusCode::NOT_FOUND, "Member not found".into()))?;
        if target == cluster.owner_id { return Err(ApiError(StatusCode::BAD_REQUEST, "The owner cannot be removed".into())); }
        cluster.members.remove(&target);
        let removed_nodes: Vec<Uuid> = store.nodes.values().filter(|node| node.cluster_id == id && node.owner_id == target).map(|node| node.id).collect();
        store.nodes.retain(|_, node| node.cluster_id != id || node.owner_id != target);
        Ok(removed_nodes)
    }).await?;
    if let Ok(mut connections) = state.node_connections.lock() { for node_id in removed_nodes { connections.remove(&node_id); } }
    Ok(StatusCode::NO_CONTENT)
}
async fn list_nodes(State(state): State<AppState>, Path(id): Path<Uuid>, headers: HeaderMap) -> ApiResult<Json<Vec<NodeSummary>>> {
    let user = identity(&headers)?;
    let store = state.store.lock().map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Store unavailable".into()))?;
    require_member(&store, id, &user)?;
    Ok(Json(store.nodes.values().filter(|n| n.cluster_id == id).map(node_summary).collect()))
}
async fn create_node(State(state): State<AppState>, Path(id): Path<Uuid>, headers: HeaderMap, Json(input): Json<CreateNode>) -> ApiResult<(StatusCode, Json<NodeCreated>)> {
    let user = identity(&headers)?;
    let name = cluster_name(&input.name).ok_or(ApiError(StatusCode::BAD_REQUEST, "Node name must be 1–80 characters".into()))?;
    let token = Uuid::new_v4().simple().to_string() + &Uuid::new_v4().simple().to_string();
    let node_id = Uuid::new_v4();
    let token_hash = secret_hash(&token);
    let token_for_response = token.clone();
    let result = mutate_store(&state, move |store| {
        require_member(store, id, &user)?;
        let node = Node { id: node_id, cluster_id: id, name: name.clone(), owner_id: user.clone(), token_hash: token_hash.clone(), paused: false, online: false, last_seen: None };
        let node_summary = node_summary(&node);
        store.nodes.insert(node.id, node);
        Ok(NodeCreated { node: node_summary, token: token_for_response.clone() })
    }).await?;
    Ok((StatusCode::CREATED, Json(result)))
}
async fn set_node_paused(State(state): State<AppState>, Path(id): Path<Uuid>, headers: HeaderMap, paused: bool) -> ApiResult<Json<NodeSummary>> {
    let user = identity(&headers)?;
    let result = mutate_store(&state, move |store| {
        let (cluster_id, node_owner) = store.nodes.get(&id).map(|node| (node.cluster_id, node.owner_id.clone())).ok_or(ApiError(StatusCode::NOT_FOUND, "Node not found".into()))?;
        let cluster_owner = store.clusters.get(&cluster_id).ok_or(ApiError(StatusCode::NOT_FOUND, "Cluster not found".into()))?.owner_id.clone();
        if cluster_owner != user && node_owner != user { return Err(ApiError(StatusCode::FORBIDDEN, "You cannot manage this node".into())); }
        let node = store.nodes.get_mut(&id).expect("checked above");
        node.paused = paused;
        let summary = node_summary(node);
        if paused { store.recovery.shorten_worker_leases(&id.to_string(), now().saturating_mul(1_000), 0); }
        Ok(summary)
    }).await?;
    let (node_session, peers) = if let Ok(connections) = state.node_connections.lock() {
        let node_session = connections.get(&id).map(|connection| {
            let control = serde_json::json!({"type":"control","paused":paused}).to_string();
            let _ = connection.sender.try_send(Message::Text(control));
            connection.session_id
        });
        let peers = connections.values().filter(|connection| connection.cluster_id == result.cluster_id)
            .filter(|connection| Some(connection.session_id) != node_session)
            .map(|connection| connection.sender.clone()).collect::<Vec<_>>();
        (node_session, peers)
    } else { (None, Vec::new()) };
    if paused {
        let event = serde_json::json!({"type":"peer_left","peer_node_id":id}).to_string();
        for peer in peers { let _ = peer.try_send(Message::Text(event.clone())); }
    } else {
        let _ = node_session;
        announce_peer_presence(&state, id, true);
    }
    Ok(Json(result))
}
async fn pause_node(State(state): State<AppState>, Path(id): Path<Uuid>, headers: HeaderMap) -> ApiResult<Json<NodeSummary>> { set_node_paused(State(state), Path(id), headers, true).await }
async fn resume_node(State(state): State<AppState>, Path(id): Path<Uuid>, headers: HeaderMap) -> ApiResult<Json<NodeSummary>> { set_node_paused(State(state), Path(id), headers, false).await }
async fn delete_node(State(state): State<AppState>, Path(id): Path<Uuid>, headers: HeaderMap) -> ApiResult<StatusCode> {
    let user = identity(&headers)?;
    mutate_store(&state, move |store| {
        let node = store.nodes.get(&id).ok_or(ApiError(StatusCode::NOT_FOUND, "Node not found".into()))?;
        let cluster = store.clusters.get(&node.cluster_id).ok_or(ApiError(StatusCode::NOT_FOUND, "Cluster not found".into()))?;
        if cluster.owner_id != user && node.owner_id != user { return Err(ApiError(StatusCode::FORBIDDEN, "You cannot remove this node".into())); }
        store.nodes.remove(&id);
        store.recovery.shorten_worker_leases(&id.to_string(), now().saturating_mul(1_000), 0);
        Ok(())
    }).await?;
    if let Ok(mut connections) = state.node_connections.lock() { connections.remove(&id); }
    Ok(StatusCode::NO_CONTENT)
}
async fn node_socket(State(state): State<AppState>, Query(query): Query<NodeSocketQuery>, ws: WebSocketUpgrade) -> ApiResult<Response> {
    if query.token.len() != 64 || !query.token.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(ApiError(StatusCode::UNAUTHORIZED, "Invalid node token".into()));
    }
    let node_id = {
        let store = state.store.lock().map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Store unavailable".into()))?;
        store.nodes.values().find(|n| n.token_hash == secret_hash(&query.token)).map(|n| n.id).ok_or(ApiError(StatusCode::UNAUTHORIZED, "Invalid node token".into()))?
    };
    let session_id = Uuid::new_v4();
    Ok(ws.max_message_size(64 * 1024).max_frame_size(16 * 1024)
        .on_upgrade(move |socket| run_node_socket(socket, state, node_id, session_id)))
}
fn register_node_connection(state: &AppState, id: Uuid, session_id: Uuid, sender: tokio::sync::mpsc::Sender<Message>) -> bool {
    let cluster_id = if let Ok(store) = state.store.lock() {
        if let Some(node) = store.nodes.get(&id) { node.cluster_id } else { return false; }
    } else { return false; };
    let inserted = if let Ok(mut connections) = state.node_connections.lock() {
        connections.insert(id, NodeConnection { session_id, cluster_id, sender });
        true
    } else { false };
    if !inserted { return false; }
    if let Ok(mut store) = state.store.lock() {
        if let Some(node) = store.nodes.get_mut(&id) {
            node.online = true; node.last_seen = Some(now());
            store.recovery.resume_worker_leases(&id.to_string(), now().saturating_mul(1_000), JOB_LEASE_DURATION_MS);
            true
        }
        else {
            if let Ok(mut connections) = state.node_connections.lock() {
                if connections.get(&id).is_some_and(|connection| connection.session_id == session_id) { connections.remove(&id); }
            }
            false
        }
    } else { false }
}

fn is_current_node_connection(state: &AppState, id: Uuid, session_id: Uuid) -> bool {
    state.node_connections.lock().map(|connections| connections.get(&id).is_some_and(|connection| connection.session_id == session_id)).unwrap_or(false)
}

fn update_node_heartbeat(state: &AppState, id: Uuid, session_id: Uuid) -> bool {
    if !is_current_node_connection(state, id, session_id) { return false; }
    if let Ok(mut store) = state.store.lock() {
        if let Some(node) = store.nodes.get_mut(&id) { node.online = true; node.last_seen = Some(now()); return true; }
    }
    false
}

fn announce_peer_presence(state: &AppState, id: Uuid, joined: bool) {
    let active_ids = state.store.lock().ok().map(|store| store.nodes.values().filter(|node| !node.paused).map(|node| node.id).collect::<std::collections::HashSet<_>>()).unwrap_or_default();
    let Ok(connections) = state.node_connections.lock() else { return; };
    let Some(current) = connections.get(&id) else { return; };
    let cluster_id = current.cluster_id;
    let peers = connections.iter().filter(|(peer_id, connection)| **peer_id != id && connection.cluster_id == cluster_id && active_ids.contains(peer_id))
        .map(|(peer_id, connection)| (*peer_id, connection.sender.clone())).collect::<Vec<_>>();
    let peer_ids = peers.iter().map(|(peer_id, _)| *peer_id).collect::<Vec<_>>();
    if let Ok(payload) = serde_json::to_string(&serde_json::json!({"type":"peer_list","peer_node_ids":peer_ids})) {
        let _ = current.sender.try_send(Message::Text(payload));
    }
    let event_type = if joined { "peer_joined" } else { "peer_left" };
    let event = serde_json::json!({"type":event_type,"peer_node_id":id}).to_string();
    for (_, sender) in peers { let _ = sender.try_send(Message::Text(event.clone())); }
}

fn disconnect_node(state: &AppState, id: Uuid, session_id: Uuid) {
    let removed_cluster = if let Ok(mut connections) = state.node_connections.lock() {
        if connections.get(&id).is_some_and(|connection| connection.session_id == session_id) { connections.remove(&id).map(|connection| connection.cluster_id) } else { None }
    } else { None };
    if let Some(cluster_id) = removed_cluster {
        if let Ok(mut store) = state.store.lock() {
            if let Some(node) = store.nodes.get_mut(&id) { node.online = false; node.last_seen = Some(now()); }
            store.recovery.shorten_worker_leases(&id.to_string(), now().saturating_mul(1_000), WORKER_RECONNECT_GRACE_MS);
        }
        // Tell remaining same-cluster workers to close stale peer channels.
        let event = serde_json::json!({"type":"peer_left","peer_node_id":id}).to_string();
        if let Ok(connections) = state.node_connections.lock() {
            for connection in connections.values().filter(|connection| connection.cluster_id == cluster_id) {
                let _ = connection.sender.try_send(Message::Text(event.clone()));
            }
        }
    }
}

async fn route_peer_signal(state: &AppState, from_id: Uuid, session_id: Uuid, text: &str) {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(text) else { return; };
    if value.get("type").and_then(serde_json::Value::as_str) != Some("signal") { return; }
    let Some(peer_id) = value.get("peer_node_id").and_then(serde_json::Value::as_str).and_then(|id| Uuid::parse_str(id).ok()) else { return; };
    let Some(payload) = value.get("payload") else { return; };
    if serde_json::to_vec(payload).map_or(true, |bytes| bytes.len() > 32 * 1024) { return; }
    let same_active_cluster = state.store.lock().ok().is_some_and(|store| {
        store.nodes.get(&from_id).is_some_and(|from| {
            !from.paused && store.nodes.get(&peer_id).is_some_and(|to| !to.paused && to.cluster_id == from.cluster_id)
        })
    });
    if !same_active_cluster { return; }
    let Ok(connections) = state.node_connections.lock() else { return; };
    let Some(from) = connections.get(&from_id).filter(|connection| connection.session_id == session_id) else { return; };
    let Some(to) = connections.get(&peer_id).filter(|connection| connection.cluster_id == from.cluster_id) else { return; };
    let sender = to.sender.clone();
    drop(connections);
    let forwarded = serde_json::json!({"type":"signal","peer_node_id":from_id,"payload":payload}).to_string();
    let _ = sender.try_send(Message::Text(forwarded));
}

async fn run_node_socket(socket: WebSocket, state: AppState, id: Uuid, session_id: Uuid) {
    let (outbound, mut queued) = tokio::sync::mpsc::channel::<Message>(128);
    if !register_node_connection(&state, id, session_id, outbound.clone()) { return; }
    announce_peer_presence(&state, id, true);
    let (mut sink, mut stream) = socket.split();
    let mut tick = interval(Duration::from_secs(NODE_PING_INTERVAL_SECS));
    tick.tick().await;
    let mut last_paused = None;
    loop {
        tokio::select! {
            message = stream.next() => match message {
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                Some(Ok(Message::Ping(bytes))) => {
                    if !update_node_heartbeat(&state, id, session_id) { break; }
                    if sink.send(Message::Pong(bytes)).await.is_err() { break; }
                }
                Some(Ok(Message::Text(text))) => {
                    if !update_node_heartbeat(&state, id, session_id) { break; }
                    route_peer_signal(&state, id, session_id, &text).await;
                }
                Some(Ok(Message::Binary(_))) | Some(Ok(Message::Pong(_))) => {
                    if !update_node_heartbeat(&state, id, session_id) { break; }
                }
            },
            outgoing = queued.recv() => match outgoing {
                Some(message) => if sink.send(message).await.is_err() { break; },
                None => break,
            },
            _ = tick.tick() => {
                if !is_current_node_connection(&state, id, session_id) { break; }
                let status = state.store.lock().ok().and_then(|store| store.nodes.get(&id).map(|node| (node.paused, node.last_seen)));
                match status {
                    Some((paused, last_seen)) => {
                        if now().saturating_sub(last_seen.unwrap_or_default()) > NODE_HEARTBEAT_TIMEOUT_SECS { break; }
                        if sink.send(Message::Ping(Vec::new())).await.is_err() { break; }
                        if last_paused != Some(paused) {
                            let message = serde_json::json!({"type": "control", "paused": paused}).to_string();
                            if sink.send(Message::Text(message)).await.is_err() { break; }
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

#[tokio::main]
async fn main() {
    let data_file = PathBuf::from(std::env::var("MESH_DATA_FILE").unwrap_or_else(|_| "/tmp/mesh-state.json".into()));
    let github = std::env::var("GH_TOKEN").ok().filter(|token| !token.trim().is_empty()).map(|token| Arc::new(GitHubStorage {
        token,
        repo: std::env::var("MESH_STORAGE_REPO").unwrap_or_else(|_| "Wuhpondiscord/gpuclustertest-state".into()),
        branch: std::env::var("MESH_STORAGE_BRANCH").unwrap_or_else(|_| "main".into()),
        path: std::env::var("MESH_STORAGE_PATH").unwrap_or_else(|_| "mesh-state.json".into()),
    }));
    if std::env::var("MESH_REQUIRE_GITHUB_STORAGE").as_deref() == Ok("1") && github.is_none() {
        panic!("Set the GH_TOKEN secret on the Hugging Face Space to enable persistent cluster storage");
    }
    let (mut store, github_sha) = if let Some(config) = github.as_deref() {
        load_github_store(config).expect("GitHub cluster storage is configured but could not be loaded")
    } else {
        let store = std::fs::read(&data_file).ok().and_then(|bytes| serde_json::from_slice(&bytes).ok()).unwrap_or_default();
        (store, None)
    };
    store.recovery.start_session(coordinator_epoch(Uuid::new_v4()));
    for node in store.nodes.values_mut() { node.online = false; }
    let state = AppState { store: Arc::new(Mutex::new(store)), mutation_lock: Arc::new(tokio::sync::Mutex::new(())), data_file: Arc::new(data_file), github, github_file_sha: Arc::new(Mutex::new(github_sha)), node_connections: Arc::new(Mutex::new(HashMap::new())) };
    let app = Router::new()
        .route("/health", get(health))
        .route("/api/clusters", get(list_clusters).post(create_cluster))
        .route("/api/clusters/:id", axum::routing::patch(rename_cluster).delete(delete_cluster))
        .route("/api/clusters/:id/jobs", get(list_jobs).post(create_job))
        .route("/api/jobs/:id/claim", post(claim_job_batch))
        .route("/api/jobs/:id/renew", post(renew_job_batch))
        .route("/api/jobs/:id/commit", post(commit_job_batch))
        .route("/api/clusters/:id/invites", get(list_invites).post(create_invite))
        .route("/api/clusters/:id/invites/:token", axum::routing::delete(revoke_invite))
        .route("/api/clusters/:id/leave", post(leave_cluster))
        .route("/api/clusters/:id/members", get(list_members))
        .route("/api/clusters/:id/members/:member_id", axum::routing::delete(remove_member))
        .route("/api/clusters/:id/nodes", get(list_nodes).post(create_node))
        .route("/api/invites/:token", get(preview_invite))
        .route("/api/invites/join", post(join_invite))
        .route("/api/nodes/:id/pause", post(pause_node))
        .route("/api/nodes/:id/resume", post(resume_node))
        .route("/api/nodes/:id", axum::routing::delete(delete_node))
        .route("/api/nodes/connect", get(node_socket))
        .layer(DefaultBodyLimit::max(64 * 1024))
        .with_state(state.clone())
        .fallback_service(ServeDir::new("dist").append_index_html_on_directories(true));
    let port = std::env::var("PORT").ok().and_then(|p| p.parse::<u16>().ok()).unwrap_or(7860);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr).await.expect("bind coordinator");
    println!("Mesh coordinator listening on http://{addr}");
    axum::serve(listener, app).await.expect("serve coordinator");
}

#[cfg(test)]
mod tests {
    use super::*;

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
        })).unwrap();
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
        })).unwrap();
        assert_eq!(old_snapshot.workload, ComputeWorkload::CpuHash);
        assert!(old_snapshot.outputs[&0].shards.is_empty());

        let vector_add: CreateJob = serde_json::from_value(serde_json::json!({
            "workload": "vector-add",
            "total_units": 1000,
            "batch_units": 1000
        })).unwrap();
        assert_eq!(vector_add.workload, ComputeWorkload::VectorAdd);
    }

    #[test]
    fn cluster_names_are_trimmed_and_bounded() {
        assert_eq!(cluster_name("  Research pool  ").as_deref(), Some("Research pool"));
        assert_eq!(cluster_name(" "), None);
        assert_eq!(cluster_name(&"x".repeat(81)), None);
    }

    #[test]
    fn github_revision_conflicts_retry_only_with_bounded_attempts() {
        let conflict = ApiError(StatusCode::CONFLICT, "revision conflict".into());
        let unavailable = ApiError(StatusCode::SERVICE_UNAVAILABLE, "offline".into());
        assert!(should_retry_storage_conflict(&conflict, 0, true));
        assert!(should_retry_storage_conflict(&conflict, MAX_STORAGE_CONFLICT_RETRIES - 1, true));
        assert!(!should_retry_storage_conflict(&conflict, MAX_STORAGE_CONFLICT_RETRIES, true));
        assert!(!should_retry_storage_conflict(&conflict, 0, false));
        assert!(!should_retry_storage_conflict(&unavailable, 0, true));
    }

    #[test]
    fn identities_need_unpredictable_length_tokens() {
        assert!(identity(&HeaderMap::new()).is_err());
        let mut headers = HeaderMap::new();
        headers.insert(IDENTITY_HEADER, "0123456789abcdef0123456789abcdef".parse().unwrap());
        let hashed_identity = identity(&headers).unwrap();
        assert_eq!(hashed_identity.len(), 64);
        assert_ne!(hashed_identity, "0123456789abcdef0123456789abcdef");
    }

    #[test]
    fn members_can_belong_to_multiple_clusters() {
        let mut members = HashMap::new(); members.insert("user-a".to_owned(), Role::Owner);
        let first = Cluster { id: Uuid::new_v4(), name: "one".into(), owner_id: "user-a".into(), members: members.clone() };
        let second = Cluster { id: Uuid::new_v4(), name: "two".into(), owner_id: "user-b".into(), members };
        assert!(first.members.contains_key("user-a") && second.members.contains_key("user-a"));
    }

    #[test]
    fn github_file_revision_uses_contents_response_sha() {
        let response = serde_json::json!({ "content": { "sha": "expected-blob-sha" } });
        assert_eq!(response.pointer("/content/sha").and_then(serde_json::Value::as_str), Some("expected-blob-sha"));
    }

    #[tokio::test]
    async fn owner_invites_member_and_can_revoke_invite() {
        let file = std::env::temp_dir().join(format!("mesh-test-{}.json", Uuid::new_v4()));
        let state = AppState { store: Arc::new(Mutex::new(Store::default())), mutation_lock: Arc::new(tokio::sync::Mutex::new(())), data_file: Arc::new(file.clone()), github: None, github_file_sha: Arc::new(Mutex::new(None)), node_connections: Arc::new(Mutex::new(HashMap::new())) };
        let owner = test_headers("owner-identity-token-01234567890123456789");
        let guest = test_headers("guest-identity-token-01234567890123456789");

        let (_, Json(cluster)) = create_cluster(State(state.clone()), owner.clone(), Json(CreateCluster { name: "  Lab  ".into() })).await.unwrap();
        assert_eq!(cluster.name, "Lab");
        let (_, Json(invite)) = create_invite(State(state.clone()), Path(cluster.id), owner.clone()).await.unwrap();
        assert!(state.store.lock().unwrap().invites.contains_key(&secret_hash(&invite.token)));
        assert!(!state.store.lock().unwrap().invites.contains_key(&invite.token));
        let Json(preview) = preview_invite(State(state.clone()), Path(invite.token.clone())).await.unwrap();
        assert_eq!(preview.cluster_id, cluster.id);
        let Json(joined) = join_invite(State(state.clone()), guest.clone(), Json(JoinInvite { token: invite.token.clone() })).await.unwrap();
        assert_eq!(joined.members, 2);
        let Json(guest_clusters) = list_clusters(State(state.clone()), guest).await.unwrap();
        assert_eq!(guest_clusters.len(), 1);
        assert!(delete_cluster(State(state.clone()), Path(cluster.id), test_headers("stranger-identity-token-01234567890123456789")).await.is_err());
        assert_eq!(revoke_invite(State(state.clone()), Path((cluster.id, secret_hash(&invite.token))), owner).await.unwrap(), StatusCode::NO_CONTENT);
        assert!(preview_invite(State(state), Path(invite.token)).await.is_err());
        let _ = std::fs::remove_file(file);
    }

    #[tokio::test]
    async fn deleting_cluster_removes_its_nodes_and_invites() {
        let file = std::env::temp_dir().join(format!("mesh-delete-test-{}.json", Uuid::new_v4()));
        let state = AppState { store: Arc::new(Mutex::new(Store::default())), mutation_lock: Arc::new(tokio::sync::Mutex::new(())), data_file: Arc::new(file.clone()), github: None, github_file_sha: Arc::new(Mutex::new(None)), node_connections: Arc::new(Mutex::new(HashMap::new())) };
        let owner = test_headers("delete-owner-identity-token-012345678901234567");
        let (_, Json(cluster)) = create_cluster(State(state.clone()), owner.clone(), Json(CreateCluster { name: "Removable".into() })).await.unwrap();
        let (_, Json(invite)) = create_invite(State(state.clone()), Path(cluster.id), owner.clone()).await.unwrap();
        let (_, Json(node)) = create_node(State(state.clone()), Path(cluster.id), owner.clone(), Json(CreateNode { name: "Worker".into() })).await.unwrap();

        assert_eq!(delete_cluster(State(state.clone()), Path(cluster.id), owner).await.unwrap(), StatusCode::NO_CONTENT);
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
        store.clusters.insert(cluster_id, Cluster { id: cluster_id, name: "Keep me".into(), owner_id: secret_hash(owner), members });
        let state = AppState { store: Arc::new(Mutex::new(store)), mutation_lock: Arc::new(tokio::sync::Mutex::new(())), data_file: Arc::new(blocker.join("state.json")), github: None, github_file_sha: Arc::new(Mutex::new(None)), node_connections: Arc::new(Mutex::new(HashMap::new())) };

        assert!(delete_cluster(State(state.clone()), Path(cluster_id), test_headers(owner)).await.is_err());
        assert!(state.store.lock().unwrap().clusters.contains_key(&cluster_id));
        let _ = std::fs::remove_file(blocker);
    }

    #[tokio::test]
    async fn failed_persistence_never_publishes_a_mutation() {
        let blocker = std::env::temp_dir().join(format!("mesh-transaction-blocker-{}", Uuid::new_v4()));
        std::fs::write(&blocker, b"not a directory").unwrap();
        let state = AppState {
            store: Arc::new(Mutex::new(Store::default())),
            mutation_lock: Arc::new(tokio::sync::Mutex::new(())),
            data_file: Arc::new(blocker.join("state.json")),
            github: None,
            github_file_sha: Arc::new(Mutex::new(None)),
            node_connections: Arc::new(Mutex::new(HashMap::new())),
        };

        let result = mutate_store(&state, |store| {
            let id = Uuid::new_v4();
            store.clusters.insert(id, Cluster {
                id,
                name: "Should not commit".into(),
                owner_id: "owner".into(),
                members: HashMap::new(),
            });
            Ok(())
        }).await;

        assert!(result.is_err());
        assert!(state.store.lock().unwrap().clusters.is_empty());
        let _ = std::fs::remove_file(blocker);
    }

    #[tokio::test]
    async fn concurrent_mutations_are_serialized_without_lost_updates() {
        let file = std::env::temp_dir().join(format!("mesh-concurrent-test-{}.json", Uuid::new_v4()));
        let state = AppState {
            store: Arc::new(Mutex::new(Store::default())),
            mutation_lock: Arc::new(tokio::sync::Mutex::new(())),
            data_file: Arc::new(file.clone()),
            github: None,
            github_file_sha: Arc::new(Mutex::new(None)),
            node_connections: Arc::new(Mutex::new(HashMap::new())),
        };
        let first_id = Uuid::new_v4();
        let second_id = Uuid::new_v4();
        let (first, second) = tokio::join!(
            mutate_store(&state, move |store| {
                store.clusters.insert(first_id, Cluster { id: first_id, name: "first".into(), owner_id: "owner".into(), members: HashMap::new() });
                Ok(())
            }),
            mutate_store(&state, move |store| {
                store.clusters.insert(second_id, Cluster { id: second_id, name: "second".into(), owner_id: "owner".into(), members: HashMap::new() });
                Ok(())
            }),
        );

        assert!(first.is_ok() && second.is_ok());
        let disk: Store = serde_json::from_slice(&std::fs::read(&file).unwrap()).unwrap();
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
        store.clusters.insert(cluster_id, Cluster { id: cluster_id, name: "test".into(), owner_id: "owner".into(), members });
        store.nodes.insert(node_id, Node { id: node_id, cluster_id, name: "node".into(), owner_id: "owner".into(), token_hash: "hash".into(), paused: false, online: false, last_seen: None });
        let state = AppState { store: Arc::new(Mutex::new(store)), mutation_lock: Arc::new(tokio::sync::Mutex::new(())), data_file: Arc::new(PathBuf::from("unused")), github: None, github_file_sha: Arc::new(Mutex::new(None)), node_connections: Arc::new(Mutex::new(HashMap::new())) };
        let old_session = Uuid::new_v4();
        let new_session = Uuid::new_v4();

        let (old_tx, _old_rx) = tokio::sync::mpsc::channel(8);
        let (new_tx, _new_rx) = tokio::sync::mpsc::channel(8);
        assert!(register_node_connection(&state, node_id, old_session, old_tx));
        assert!(register_node_connection(&state, node_id, new_session, new_tx));
        let now_ms = now().saturating_mul(1_000);
        let lease = {
            let mut store = state.store.lock().unwrap();
            store.recovery.add_task("connection-test").unwrap();
            store.recovery.claim("connection-test", node_id.to_string(), now_ms, JOB_LEASE_DURATION_MS).unwrap()
        };
        disconnect_node(&state, node_id, old_session);
        assert!(is_current_node_connection(&state, node_id, new_session));
        assert!(state.store.lock().unwrap().nodes.get(&node_id).unwrap().online);
        let mut still_active = state.store.lock().unwrap().recovery.clone();
        assert_eq!(still_active.claim("connection-test", "other-worker", now_ms + WORKER_RECONNECT_GRACE_MS + 1, JOB_LEASE_DURATION_MS), Err(recovery::LeaseError::LeaseBusy));
        disconnect_node(&state, node_id, new_session);
        assert!(!state.store.lock().unwrap().nodes.get(&node_id).unwrap().online);
        let mut disconnected = state.store.lock().unwrap().recovery.clone();
        let replacement = disconnected.claim("connection-test", "other-worker", now().saturating_mul(1_000) + WORKER_RECONNECT_GRACE_MS, JOB_LEASE_DURATION_MS).unwrap();
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
        for (id, cluster_id) in [(sender_id, cluster_a), (same_cluster_id, cluster_a), (other_cluster_id, cluster_b)] {
            store.nodes.insert(id, Node { id, cluster_id, name: "node".into(), owner_id: "owner".into(), token_hash: "hash".into(), paused: false, online: false, last_seen: None });
        }
        let state = AppState { store: Arc::new(Mutex::new(store)), mutation_lock: Arc::new(tokio::sync::Mutex::new(())), data_file: Arc::new(PathBuf::from("unused")), github: None, github_file_sha: Arc::new(Mutex::new(None)), node_connections: Arc::new(Mutex::new(HashMap::new())) };
        let sender_session = Uuid::new_v4();
        let same_session = Uuid::new_v4();
        let other_session = Uuid::new_v4();
        let (sender_tx, _sender_rx) = tokio::sync::mpsc::channel(8);
        let (same_tx, mut same_rx) = tokio::sync::mpsc::channel(8);
        let (other_tx, mut other_rx) = tokio::sync::mpsc::channel(8);
        assert!(register_node_connection(&state, sender_id, sender_session, sender_tx));
        assert!(register_node_connection(&state, same_cluster_id, same_session, same_tx));
        assert!(register_node_connection(&state, other_cluster_id, other_session, other_tx));

        let signal = serde_json::json!({"type":"signal","peer_node_id":same_cluster_id,"payload":{"description":{"type":"offer","sdp":"test"}}}).to_string();
        route_peer_signal(&state, sender_id, sender_session, &signal).await;
        let Message::Text(forwarded) = same_rx.try_recv().unwrap() else { panic!("expected forwarded peer signal"); };
        let forwarded: serde_json::Value = serde_json::from_str(&forwarded).unwrap();
        assert_eq!(forwarded["peer_node_id"], sender_id.to_string());
        assert!(other_rx.try_recv().is_err());

        let cross_cluster = serde_json::json!({"type":"signal","peer_node_id":other_cluster_id,"payload":{"description":{"type":"offer","sdp":"test"}}}).to_string();
        route_peer_signal(&state, sender_id, sender_session, &cross_cluster).await;
        assert!(other_rx.try_recv().is_err());

        state.store.lock().unwrap().nodes.get_mut(&same_cluster_id).unwrap().paused = true;
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
            store.nodes.insert(id, Node { id, cluster_id, name: "node".into(), owner_id: "owner".into(), token_hash: "hash".into(), paused, online: false, last_seen: None });
        }
        let state = AppState { store: Arc::new(Mutex::new(store)), mutation_lock: Arc::new(tokio::sync::Mutex::new(())), data_file: Arc::new(PathBuf::from("unused")), github: None, github_file_sha: Arc::new(Mutex::new(None)), node_connections: Arc::new(Mutex::new(HashMap::new())) };
        let (current_tx, mut current_rx) = tokio::sync::mpsc::channel(8);
        let (active_tx, mut active_rx) = tokio::sync::mpsc::channel(8);
        let (paused_tx, mut paused_rx) = tokio::sync::mpsc::channel(8);
        assert!(register_node_connection(&state, current_id, Uuid::new_v4(), current_tx));
        assert!(register_node_connection(&state, active_id, Uuid::new_v4(), active_tx));
        assert!(register_node_connection(&state, paused_id, Uuid::new_v4(), paused_tx));

        announce_peer_presence(&state, current_id, true);
        let Message::Text(peer_list) = current_rx.try_recv().unwrap() else { panic!("expected peer list"); };
        let peer_list: serde_json::Value = serde_json::from_str(&peer_list).unwrap();
        assert_eq!(peer_list["peer_node_ids"], serde_json::json!([active_id]));
        let Message::Text(joined) = active_rx.try_recv().unwrap() else { panic!("expected peer joined event"); };
        assert_eq!(serde_json::from_str::<serde_json::Value>(&joined).unwrap()["peer_node_id"], current_id.to_string());
        assert!(paused_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn job_batches_require_a_live_owned_worker_and_replicated_checkpoint_before_commit() {
        let file = std::env::temp_dir().join(format!("mesh-job-test-{}.json", Uuid::new_v4()));
        let state = AppState { store: Arc::new(Mutex::new(Store::default())), mutation_lock: Arc::new(tokio::sync::Mutex::new(())), data_file: Arc::new(file.clone()), github: None, github_file_sha: Arc::new(Mutex::new(None)), node_connections: Arc::new(Mutex::new(HashMap::new())) };
        let owner_token = "job-owner-identity-token-012345678901234567890";
        let owner_headers = test_headers(owner_token);
        let stranger_headers = test_headers("job-stranger-identity-token-01234567890123456789");
        let (_, Json(cluster)) = create_cluster(State(state.clone()), owner_headers.clone(), Json(CreateCluster { name: "Compute".into() })).await.unwrap();
        let (_, Json(worker)) = create_node(State(state.clone()), Path(cluster.id), owner_headers.clone(), Json(CreateNode { name: "Worker".into() })).await.unwrap();
        let (_, Json(replica)) = create_node(State(state.clone()), Path(cluster.id), owner_headers.clone(), Json(CreateNode { name: "Replica".into() })).await.unwrap();
        for node_id in [worker.node.id, replica.node.id] { state.store.lock().unwrap().nodes.get_mut(&node_id).unwrap().online = true; }
        let (status, Json(job)) = create_job(State(state.clone()), Path(cluster.id), owner_headers.clone(), Json(CreateJob { workload: ComputeWorkload::CpuHash, total_units: 2_500, batch_units: 1_000 })).await.unwrap();
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(job.total_batches, 3);
        assert!(claim_job_batch(State(state.clone()), Path(job.id), stranger_headers, Json(ClaimJob { node_id: worker.node.id })).await.is_err());

        let Json(Some(claim)) = claim_job_batch(State(state.clone()), Path(job.id), owner_headers.clone(), Json(ClaimJob { node_id: worker.node.id })).await.unwrap() else { panic!("expected an available batch"); };
        assert_eq!((claim.start, claim.end), (0, 1_000));
        let digest = "a".repeat(64);
        state.store.lock().unwrap().nodes.get_mut(&replica.node.id).unwrap().online = false;
        assert!(commit_job_batch(State(state.clone()), Path(job.id), owner_headers.clone(), Json(CommitBatch { lease: claim.lease.clone(), replica_node_id: Some(replica.node.id), shards: Vec::new(), sha256: digest.clone(), bytes: 128 })).await.is_err());
        state.store.lock().unwrap().nodes.get_mut(&replica.node.id).unwrap().online = true;
        let commit = CommitBatch { lease: claim.lease.clone(), replica_node_id: Some(replica.node.id), shards: Vec::new(), sha256: digest.clone(), bytes: 128 };
        assert_eq!(commit_job_batch(State(state.clone()), Path(job.id), owner_headers.clone(), Json(commit)).await.unwrap(), StatusCode::NO_CONTENT);
        assert_eq!(commit_job_batch(State(state.clone()), Path(job.id), owner_headers.clone(), Json(CommitBatch { lease: claim.lease, replica_node_id: Some(replica.node.id), shards: Vec::new(), sha256: digest, bytes: 128 })).await.unwrap(), StatusCode::NO_CONTENT);

        let Json(jobs) = list_jobs(State(state.clone()), Path(cluster.id), owner_headers).await.unwrap();
        assert_eq!(jobs[0].completed_batches, 1);
        let Json(Some(next)) = claim_job_batch(State(state.clone()), Path(job.id), test_headers(owner_token), Json(ClaimJob { node_id: worker.node.id })).await.unwrap() else { panic!("expected the next independent batch"); };
        assert_eq!((next.start, next.end), (1_000, 2_000));
        let _ = std::fs::remove_file(file);
    }

    #[tokio::test]
    async fn large_checkpoints_require_three_nodes_for_shards_but_can_degrade_to_two_full_replicas() {
        let file = std::env::temp_dir().join(format!("mesh-shard-test-{}.json", Uuid::new_v4()));
        let state = AppState { store: Arc::new(Mutex::new(Store::default())), mutation_lock: Arc::new(tokio::sync::Mutex::new(())), data_file: Arc::new(file.clone()), github: None, github_file_sha: Arc::new(Mutex::new(None)), node_connections: Arc::new(Mutex::new(HashMap::new())) };
        let owner = test_headers("shard-owner-identity-token-01234567890123456789");
        let (_, Json(cluster)) = create_cluster(State(state.clone()), owner.clone(), Json(CreateCluster { name: "Shards".into() })).await.unwrap();
        let (_, Json(worker)) = create_node(State(state.clone()), Path(cluster.id), owner.clone(), Json(CreateNode { name: "Worker".into() })).await.unwrap();
        let (_, Json(peer_a)) = create_node(State(state.clone()), Path(cluster.id), owner.clone(), Json(CreateNode { name: "Peer A".into() })).await.unwrap();
        let (_, Json(peer_b)) = create_node(State(state.clone()), Path(cluster.id), owner.clone(), Json(CreateNode { name: "Peer B".into() })).await.unwrap();
        for node_id in [worker.node.id, peer_a.node.id, peer_b.node.id] { state.store.lock().unwrap().nodes.get_mut(&node_id).unwrap().online = true; }
        let (_, Json(job)) = create_job(State(state.clone()), Path(cluster.id), owner.clone(), Json(CreateJob { workload: ComputeWorkload::VectorAdd, total_units: 1_000, batch_units: 1_000 })).await.unwrap();
        let Json(Some(claim)) = claim_job_batch(State(state.clone()), Path(job.id), owner.clone(), Json(ClaimJob { node_id: worker.node.id })).await.unwrap() else { panic!("expected a batch lease"); };
        let digest = "a".repeat(64);
        let shards = vec![
            ShardReceipt { shard_index: 0, node_id: worker.node.id, artifact_name: "cp-1-shard-0".into(), sha256: "1".repeat(64), bytes: 32_768 },
            ShardReceipt { shard_index: 1, node_id: peer_a.node.id, artifact_name: "cp-1-shard-1".into(), sha256: "2".repeat(64), bytes: 32_768 },
            ShardReceipt { shard_index: 2, node_id: peer_b.node.id, artifact_name: "cp-1-shard-2".into(), sha256: "3".repeat(64), bytes: 32_768 },
        ];
        state.store.lock().unwrap().nodes.get_mut(&peer_b.node.id).unwrap().online = false;
        let request = || CommitBatch { lease: claim.lease.clone(), replica_node_id: None, shards: shards.clone(), sha256: digest.clone(), bytes: SHARDING_THRESHOLD_BYTES };
        assert!(commit_job_batch(State(state.clone()), Path(job.id), owner.clone(), Json(request())).await.is_err());
        let replicas = vec![
            ShardReceipt { shard_index: 0, node_id: worker.node.id, artifact_name: "cp-1-full-0".into(), sha256: digest.clone(), bytes: SHARDING_THRESHOLD_BYTES },
            ShardReceipt { shard_index: 1, node_id: peer_a.node.id, artifact_name: "cp-1-full-1".into(), sha256: digest.clone(), bytes: SHARDING_THRESHOLD_BYTES },
        ];
        let fallback = CommitBatch { lease: claim.lease.clone(), replica_node_id: None, shards: replicas, sha256: digest.clone(), bytes: SHARDING_THRESHOLD_BYTES };
        assert_eq!(commit_job_batch(State(state.clone()), Path(job.id), owner.clone(), Json(fallback)).await.unwrap(), StatusCode::NO_CONTENT);
        let Json(jobs) = list_jobs(State(state), Path(cluster.id), owner).await.unwrap();
        assert_eq!(jobs[0].checkpoints[0].shards.len(), 2);
        assert!(jobs[0].checkpoints[0].shards.iter().all(|shard| shard.sha256 == digest));
        let _ = std::fs::remove_file(file);
    }

    #[test]
    fn older_cluster_snapshots_load_with_empty_job_state() {
        let store: Store = serde_json::from_str(r#"{"clusters":{},"invites":{},"nodes":{}}"#).unwrap();
        assert!(store.jobs.is_empty());
        assert!(!store.recovery.is_complete("missing"));
    }

    #[test]
    fn process_epochs_round_trip_through_javascript_numbers() {
        let epoch = coordinator_epoch(Uuid::from_u128(u128::MAX));
        assert!(epoch > 0 && epoch <= (1_u64 << 53) - 1);
        assert_eq!((epoch as f64) as u64, epoch);
        assert_ne!(coordinator_epoch(Uuid::from_u128(1_u128 << 64)), coordinator_epoch(Uuid::from_u128(2_u128 << 64)));
    }

    #[tokio::test]
    async fn claims_and_renewals_do_not_write_storage_and_renewal_bypasses_durable_lock() {
        let file = std::env::temp_dir().join(format!("mesh-runtime-lease-{}.json", Uuid::new_v4()));
        let state = AppState { store: Arc::new(Mutex::new(Store::default())), mutation_lock: Arc::new(tokio::sync::Mutex::new(())), data_file: Arc::new(file.clone()), github: None, github_file_sha: Arc::new(Mutex::new(None)), node_connections: Arc::new(Mutex::new(HashMap::new())) };
        let owner = test_headers("runtime-owner-identity-01234567890123456789012345");
        let (_, Json(cluster)) = create_cluster(State(state.clone()), owner.clone(), Json(CreateCluster { name: "Runtime".into() })).await.unwrap();
        let (_, Json(worker)) = create_node(State(state.clone()), Path(cluster.id), owner.clone(), Json(CreateNode { name: "Worker".into() })).await.unwrap();
        state.store.lock().unwrap().nodes.get_mut(&worker.node.id).unwrap().online = true;
        let (_, Json(job)) = create_job(State(state.clone()), Path(cluster.id), owner.clone(), Json(CreateJob { workload: ComputeWorkload::CpuHash, total_units: 1_000, batch_units: 1_000 })).await.unwrap();
        let before = std::fs::read(&file).unwrap();
        let Json(Some(claim)) = claim_job_batch(State(state.clone()), Path(job.id), owner.clone(), Json(ClaimJob { node_id: worker.node.id })).await.unwrap() else { panic!("expected lease"); };
        assert_eq!(std::fs::read(&file).unwrap(), before);
        let durable_lock = state.mutation_lock.lock().await;
        let Json(renewed) = tokio::time::timeout(Duration::from_millis(100), renew_job_batch(State(state.clone()), Path(job.id), owner.clone(), Json(RenewJob { lease: claim.lease.clone() }))).await.unwrap().unwrap();
        assert_eq!(renewed.generation, claim.lease.generation);
        assert!(renewed.expires_at_ms >= claim.lease.expires_at_ms);
        drop(durable_lock);
        assert_eq!(std::fs::read(&file).unwrap(), before);
        assert!(renew_job_batch(State(state.clone()), Path(Uuid::new_v4()), owner.clone(), Json(RenewJob { lease: renewed.clone() })).await.is_err());
        assert!(renew_job_batch(State(state.clone()), Path(job.id), test_headers("stranger-identity-012345678901234567890123456789"), Json(RenewJob { lease: renewed.clone() })).await.is_err());
        let mut stale = renewed.clone();
        stale.generation += 1;
        assert!(renew_job_batch(State(state.clone()), Path(job.id), owner.clone(), Json(RenewJob { lease: stale })).await.is_err());
        state.store.lock().unwrap().nodes.get_mut(&worker.node.id).unwrap().paused = true;
        assert!(renew_job_batch(State(state.clone()), Path(job.id), owner, Json(RenewJob { lease: renewed })).await.is_err());
        let _ = std::fs::remove_file(file);
    }
}

