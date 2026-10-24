use axum::{
    extract::{ws::{Message, WebSocket, WebSocketUpgrade}, Path, Query, State},
    http::{HeaderMap, StatusCode},
    extract::DefaultBodyLimit,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::HashMap, fs::OpenOptions, io::Write, net::SocketAddr, path::PathBuf, sync::{Arc, Mutex}, time::{SystemTime, UNIX_EPOCH}};
use tokio::time::{interval, Duration};
use tower_http::services::ServeDir;
use uuid::Uuid;

#[allow(dead_code)]
mod checkpoint;
mod recovery;

const IDENTITY_HEADER: &str = "x-mesh-identity";
const INVITE_TTL_SECS: u64 = 7 * 24 * 60 * 60;
const NODE_PING_INTERVAL_SECS: u64 = 15;
const NODE_HEARTBEAT_TIMEOUT_SECS: u64 = 45;
const MAX_STORAGE_CONFLICT_RETRIES: usize = 3;

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
    node_connections: Arc<Mutex<HashMap<Uuid, Uuid>>>,
}

#[derive(Deserialize)] struct CreateCluster { name: String }
#[derive(Deserialize)] struct RenameCluster { name: String }
#[derive(Deserialize)] struct CreateNode { name: String }
#[derive(Deserialize)] struct JoinInvite { token: String }
#[derive(Deserialize)] struct NodeSocketQuery { token: String }

#[derive(Serialize)]
struct ClusterSummary { id: Uuid, name: String, role: Role, members: usize, nodes: usize, online_nodes: usize }
#[derive(Serialize)]
struct NodeSummary { id: Uuid, cluster_id: Uuid, name: String, paused: bool, online: bool, last_seen: Option<u64> }
#[derive(Serialize)]
struct NodeCreated { node: NodeSummary, token: String }
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
            }
            *state.github_file_sha.lock()
                .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Storage unavailable".into()))? = latest_sha;
            next = latest;
            continue;
        }

        let mut current = state.store.lock()
            .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "Store unavailable".into()))?;
        // Heartbeats are runtime-only state. Preserve them when committing a durable snapshot.
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
async fn delete_cluster(State(state): State<AppState>, Path(id): Path<Uuid>, headers: HeaderMap) -> ApiResult<StatusCode> {
    let user = identity(&headers)?;
    let removed_nodes = mutate_store(&state, move |store| {
        require_owner(store, id, &user)?;
        let removed_nodes: Vec<Uuid> = store.nodes.values().filter(|node| node.cluster_id == id).map(|node| node.id).collect();
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
        Ok(node_summary(node))
    }).await?;
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
fn register_node_connection(state: &AppState, id: Uuid, session_id: Uuid) -> bool {
    if let Ok(mut connections) = state.node_connections.lock() { connections.insert(id, session_id); } else { return false; }
    let registered = if let Ok(mut store) = state.store.lock() {
        if let Some(node) = store.nodes.get_mut(&id) { node.online = true; node.last_seen = Some(now()); true } else { false }
    } else { false };
    if !registered { if let Ok(mut connections) = state.node_connections.lock() { if connections.get(&id) == Some(&session_id) { connections.remove(&id); } } }
    registered
}
fn is_current_node_connection(state: &AppState, id: Uuid, session_id: Uuid) -> bool {
    state.node_connections.lock().map(|connections| connections.get(&id) == Some(&session_id)).unwrap_or(false)
}
fn update_node_heartbeat(state: &AppState, id: Uuid, session_id: Uuid) -> bool {
    if !is_current_node_connection(state, id, session_id) { return false; }
    if let Ok(mut store) = state.store.lock() {
        if let Some(node) = store.nodes.get_mut(&id) { node.online = true; node.last_seen = Some(now()); return true; }
    }
    false
}
fn disconnect_node(state: &AppState, id: Uuid, session_id: Uuid) {
    let removed = if let Ok(mut connections) = state.node_connections.lock() {
        if connections.get(&id) == Some(&session_id) { connections.remove(&id); true } else { false }
    } else { false };
    if removed { if let Ok(mut store) = state.store.lock() { if let Some(node) = store.nodes.get_mut(&id) { node.online = false; node.last_seen = Some(now()); } } }
}
async fn run_node_socket(mut socket: WebSocket, state: AppState, id: Uuid, session_id: Uuid) {
    if !register_node_connection(&state, id, session_id) { return; }
    let mut tick = interval(Duration::from_secs(NODE_PING_INTERVAL_SECS));
    tick.tick().await;
    let mut last_paused = None;
    loop {
        tokio::select! {
            message = socket.next() => match message {
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                Some(Ok(Message::Ping(bytes))) => {
                    if !update_node_heartbeat(&state, id, session_id) { break; }
                    if socket.send(Message::Pong(bytes)).await.is_err() { break; }
                }
                Some(Ok(Message::Text(_))) | Some(Ok(Message::Binary(_))) | Some(Ok(Message::Pong(_))) => {
                    if !update_node_heartbeat(&state, id, session_id) { break; }
                }
            },
            _ = tick.tick() => {
                if !is_current_node_connection(&state, id, session_id) { break; }
                let status = state.store.lock().ok().and_then(|store| store.nodes.get(&id).map(|node| (node.paused, node.last_seen)));
                match status {
                    Some((paused, last_seen)) => {
                        if now().saturating_sub(last_seen.unwrap_or_default()) > NODE_HEARTBEAT_TIMEOUT_SECS { break; }
                        if socket.send(Message::Ping(Vec::new())).await.is_err() { break; }
                        if last_paused != Some(paused) {
                            let message = serde_json::json!({"type": "control", "paused": paused}).to_string();
                            if socket.send(Message::Text(message)).await.is_err() { break; }
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
    for node in store.nodes.values_mut() { node.online = false; }
    let state = AppState { store: Arc::new(Mutex::new(store)), mutation_lock: Arc::new(tokio::sync::Mutex::new(())), data_file: Arc::new(data_file), github, github_file_sha: Arc::new(Mutex::new(github_sha)), node_connections: Arc::new(Mutex::new(HashMap::new())) };
    let app = Router::new()
        .route("/health", get(health))
        .route("/api/clusters", get(list_clusters).post(create_cluster))
        .route("/api/clusters/:id", axum::routing::patch(rename_cluster).delete(delete_cluster))
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

        assert!(register_node_connection(&state, node_id, old_session));
        assert!(register_node_connection(&state, node_id, new_session));
        disconnect_node(&state, node_id, old_session);
        assert!(is_current_node_connection(&state, node_id, new_session));
        assert!(state.store.lock().unwrap().nodes.get(&node_id).unwrap().online);
        disconnect_node(&state, node_id, new_session);
        assert!(!state.store.lock().unwrap().nodes.get(&node_id).unwrap().online);
    }
}

