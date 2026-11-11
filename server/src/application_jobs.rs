//! Private, bounded browser jobs. Inference APIs and SDK requests share this queue.
use crate::{
    identity, mutate_store, now, now_ms, recovery, secret_hash, security, ApiError, ApiResult,
    AppState, Store, JOB_LEASE_DURATION_MS,
};
use axum::{
    body::to_bytes,
    extract::{Path, Request, State},
    http::{header, HeaderMap, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use mesh_coordinator::browser_jobs::{
    self as protocol, InferenceVerification, Input, Output, RuntimeReport,
};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use subtle::ConstantTimeEq;
use tower_http::cors::{Any, CorsLayer};
use uuid::Uuid;

#[path = "application_deployments.rs"]
mod deployments;
pub use deployments::Deployment;
use deployments::{DeploymentTask, RunDeployment};

const MAX_JOBS: usize = 16;
const MAX_GLOBAL_JOBS: usize = 128;
const JOB_TTL_MS: u64 = 30 * 60 * 1000;
const MAX_COMMIT_BYTES: usize = 128 * 1024;
const KEY_TTL: u64 = 30 * 24 * 60 * 60;

#[derive(Clone, Deserialize, Serialize)]
pub struct AppKey {
    id: Uuid,
    app_id: Uuid,
    issuer: String,
    label: String,
    hash: String,
    created_at: u64,
    expires_at: u64,
    read_only: bool,
}
#[derive(Clone, Deserialize, Serialize)]
pub struct Job {
    pub id: Uuid,
    pub app_id: Uuid,
    input: Input,
    created_at_ms: u64,
    actor: String,
    idempotency: Option<String>,
    request_digest: String,
    receipts: Vec<Receipt>,
    cancelled: bool,
    failed: Option<String>,
    failures: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    deployment: Option<DeploymentTask>,
}
#[derive(Clone, Deserialize, Serialize)]
struct Receipt {
    slot: usize,
    worker: Uuid,
    owner: String,
    epoch: u64,
    generation: u64,
    attempt_id: u64,
    digest: String,
    claimed_at_ms: u64,
    received_at_ms: u64,
    output: Output,
    runtime: RuntimeReport,
}
#[derive(Serialize)]
pub struct Summary {
    id: Uuid,
    application_id: Uuid,
    status: &'static str,
    input: Input,
    verification: &'static str,
    created_at_ms: u64,
    expires_at_ms: u64,
    replicas_required: usize,
    replicas_received: usize,
    output: Option<Output>,
    error: Option<String>,
    status_url: String,
    workers: Vec<ReceiptSummary>,
    queue_wait_ms: Option<u64>,
    result_received_elapsed_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    deployment: Option<DeploymentTask>,
}
#[derive(Serialize)]
struct ReceiptSummary {
    node_id: Uuid,
    attempt_id: u64,
    coordinator_epoch: u64,
    generation: u64,
    backend: String,
    compute_ms: f64,
    load_ms: f64,
    fallback_reason: Option<String>,
    received_at_ms: u64,
}
impl From<&Receipt> for ReceiptSummary {
    fn from(receipt: &Receipt) -> Self {
        Self {
            node_id: receipt.worker,
            attempt_id: receipt.attempt_id,
            coordinator_epoch: receipt.epoch,
            generation: receipt.generation,
            backend: receipt.runtime.backend.clone(),
            compute_ms: receipt.runtime.compute_ms,
            load_ms: receipt.runtime.load_ms,
            fallback_reason: receipt.runtime.fallback_reason.clone(),
            received_at_ms: receipt.received_at_ms,
        }
    }
}
fn hidden() -> ApiError {
    ApiError(
        StatusCode::NOT_FOUND,
        "Application or job not found or access denied".into(),
    )
}
fn unavailable() -> ApiError {
    ApiError(
        StatusCode::SERVICE_UNAVAILABLE,
        "Application job service unavailable".into(),
    )
}
fn invalid(message: impl Into<String>) -> ApiError {
    ApiError(StatusCode::UNPROCESSABLE_ENTITY, message.into())
}
fn fenced() -> ApiError {
    ApiError(
        StatusCode::CONFLICT,
        "Application worker lease is no longer current".into(),
    )
}
fn task(job: Uuid, slot: usize) -> String {
    format!("application:{job}:{slot}")
}
fn digest<T: Serialize>(value: &T) -> ApiResult<String> {
    Ok(secret_hash(
        &serde_json::to_string(value).map_err(|_| invalid("Invalid job payload"))?,
    ))
}

#[derive(Clone)]
enum Actor {
    Account(String),
    Key(Uuid, String),
}
#[derive(Clone, Copy)]
enum Permission {
    Read,
    Submit,
    Manage,
}
impl Actor {
    fn name(&self) -> String {
        match self {
            Self::Account(user) => format!("account:{user}"),
            Self::Key(id, _) => format!("key:{id}"),
        }
    }
    fn authorize(&self, store: &Store, app: Uuid, permission: Permission) -> ApiResult<()> {
        match self {
            Self::Account(user) => security::application_access(
                store,
                app,
                user,
                !matches!(permission, Permission::Read),
            ),
            Self::Key(id, hash) => {
                let key = store
                    .application_keys
                    .get(id)
                    .filter(|key| {
                        key.app_id == app
                            && key.expires_at > now()
                            && bool::from(key.hash.as_bytes().ct_eq(hash.as_bytes()))
                    })
                    .ok_or(ApiError(
                        StatusCode::UNAUTHORIZED,
                        "Invalid or expired application API key".into(),
                    ))?;
                security::application_access(store, app, &key.issuer, true)?;
                if matches!(permission, Permission::Manage)
                    || key.read_only && matches!(permission, Permission::Submit)
                {
                    return Err(ApiError(
                        StatusCode::FORBIDDEN,
                        "Application API key does not grant this operation".into(),
                    ));
                }
                Ok(())
            }
        }
    }
}
fn actor(
    state: &AppState,
    headers: &HeaderMap,
    app: Uuid,
    permission: Permission,
) -> ApiResult<Actor> {
    let actor = if let Some(value) = headers.get(header::AUTHORIZATION) {
        let token = value
            .to_str()
            .ok()
            .and_then(|value| value.strip_prefix("Bearer "))
            .filter(|value| value.starts_with("mak_") && value.len() == 68)
            .ok_or(ApiError(
                StatusCode::UNAUTHORIZED,
                "Use an application-scoped Bearer API key".into(),
            ))?;
        let hash = secret_hash(token);
        let store = state.store.lock().map_err(|_| unavailable())?;
        let key = store
            .application_keys
            .values()
            .find(|key| bool::from(key.hash.as_bytes().ct_eq(hash.as_bytes())))
            .ok_or(ApiError(
                StatusCode::UNAUTHORIZED,
                "Invalid or expired application API key".into(),
            ))?;
        Actor::Key(key.id, hash)
    } else {
        Actor::Account(identity(headers)?)
    };
    actor.authorize(
        &*state.store.lock().map_err(|_| unavailable())?,
        app,
        permission,
    )?;
    Ok(actor)
}
fn status(job: &Job, store: &Store) -> &'static str {
    if job.cancelled {
        "cancelled"
    } else if job.failed.is_some() {
        "failed"
    } else if job.receipts.len() == job.input.replicas() {
        "completed"
    } else if now_ms() >= job.created_at_ms.saturating_add(JOB_TTL_MS) {
        "expired"
    } else if (0..job.input.replicas()).any(|slot| {
        store
            .recovery
            .active_lease(&task(job.id, slot), now_ms())
            .is_some()
    }) {
        "running"
    } else if !job.receipts.is_empty() {
        "awaiting_verification"
    } else {
        "queued"
    }
}
fn summary(job: &Job, store: &Store) -> Summary {
    let status = status(job, store);
    Summary {
        id: job.id,
        application_id: job.app_id,
        status,
        input: job.input.clone(),
        verification: job.input.verification(),
        created_at_ms: job.created_at_ms,
        expires_at_ms: job.created_at_ms.saturating_add(JOB_TTL_MS),
        replicas_required: job.input.replicas(),
        replicas_received: job.receipts.len(),
        output: if status == "completed" {
            job.receipts.first().map(|receipt| receipt.output.clone())
        } else {
            None
        },
        error: job.failed.clone(),
        status_url: format!("/api/applications/{}/jobs/{}", job.app_id, job.id),
        workers: job.receipts.iter().map(ReceiptSummary::from).collect(),
        queue_wait_ms: job
            .receipts
            .iter()
            .map(|receipt| receipt.claimed_at_ms.saturating_sub(job.created_at_ms))
            .min(),
        result_received_elapsed_ms: if status == "completed" {
            job.receipts
                .iter()
                .map(|receipt| receipt.received_at_ms.saturating_sub(job.created_at_ms))
                .max()
        } else {
            None
        },
        deployment: job.deployment.clone(),
    }
}
pub fn prune(store: &mut Store) {
    store
        .vision_models
        .retain(|app, _| store.applications.contains_key(app));
    store.invites.retain(|_, invite| {
        invite
            .application_id
            .is_none_or(|id| store.applications.contains_key(&id))
    });
    store
        .application_deployments
        .retain(|app, _| store.applications.contains_key(app));
    let removed: Vec<_> = store
        .application_jobs
        .values()
        .filter(|job| !store.applications.contains_key(&job.app_id))
        .map(|job| job.id)
        .collect();
    for id in removed {
        if let Some(job) = store.application_jobs.remove(&id) {
            for slot in 0..job.input.replicas() {
                store.recovery.remove_task(&task(id, slot));
            }
        }
    }
    store
        .application_keys
        .retain(|_, key| store.applications.contains_key(&key.app_id) && key.expires_at > now());
}
/// Run after runtime merge so durable grant changes cannot retain stale leases.
pub fn revoke_ineligible_leases(store: &mut Store) {
    let at = now_ms();
    let leases: Vec<_> = store
        .application_jobs
        .values()
        .flat_map(|job| {
            (0..job.input.replicas())
                .filter_map(|slot| {
                    let lease = store.recovery.active_lease(&task(job.id, slot), at)?;
                    let permitted = Uuid::parse_str(&lease.worker_id)
                        .ok()
                        .and_then(|id| store.nodes.get(&id))
                        .is_some_and(|node| {
                            !node.paused
                                && application_worker_access(store, job.app_id, &node.owner_id)
                                    .is_ok()
                        });
                    (!permitted).then_some(lease)
                })
                .collect::<Vec<_>>()
        })
        .collect();
    for lease in leases {
        let _ = store.recovery.release(&lease, at);
    }
}
pub fn routes() -> Router<AppState> {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
        .allow_headers([
            header::AUTHORIZATION,
            header::CONTENT_TYPE,
            header::HeaderName::from_static("idempotency-key"),
        ]);
    Router::new()
        .route("/api/applications/:id/jobs", get(list).post(submit))
        .route(
            "/api/applications/:id/jobs/:job",
            get(get_job).delete(delete_job),
        )
        .route("/api/applications/:id/inference", post(inference))
        .route("/api/applications/:id/runtime", get(runtime_info))
        .route(
            "/api/applications/:id/deployment",
            get(deployments::get).post(deployments::deploy),
        )
        .route(
            "/api/applications/:id/deployment/run",
            post(deployments::run),
        )
        .layer(cors)
        .route(
            "/api/applications/:id/deployment/:revision",
            axum::routing::delete(deployments::remove),
        )
        .route(
            "/api/applications/:id/keys",
            get(list_keys).post(create_key),
        )
        .route(
            "/api/applications/:id/keys/:key",
            axum::routing::delete(revoke_key),
        )
        .route("/api/clusters/:id/application-work/claim", post(claim))
        .route("/api/application-work/:job/renew", post(renew))
        .route("/api/application-work/:job/release", post(release))
        .route("/api/application-work/:job/commit", post(commit))
        .route("/api/application-work/:job/receipt", post(receipt))
}
pub async fn list(
    State(state): State<AppState>,
    Path(app): Path<Uuid>,
    headers: HeaderMap,
) -> ApiResult<Json<Vec<Summary>>> {
    let who = actor(&state, &headers, app, Permission::Read)?;
    let store = state.store.lock().map_err(|_| unavailable())?;
    who.authorize(&store, app, Permission::Read)?;
    let mut jobs: Vec<_> = store
        .application_jobs
        .values()
        .filter(|job| job.app_id == app)
        .map(|job| summary(job, &store))
        .collect();
    jobs.sort_by_key(|job| std::cmp::Reverse(job.created_at_ms));
    Ok(Json(jobs))
}
pub async fn get_job(
    State(state): State<AppState>,
    Path((app, id)): Path<(Uuid, Uuid)>,
    headers: HeaderMap,
) -> ApiResult<Json<Summary>> {
    let who = actor(&state, &headers, app, Permission::Read)?;
    let store = state.store.lock().map_err(|_| unavailable())?;
    who.authorize(&store, app, Permission::Read)?;
    let job = store
        .application_jobs
        .get(&id)
        .filter(|job| job.app_id == app)
        .ok_or_else(hidden)?;
    Ok(Json(summary(job, &store)))
}
async fn enqueue(
    state: &AppState,
    app: Uuid,
    headers: &HeaderMap,
    input: Input,
) -> ApiResult<(StatusCode, Json<Summary>)> {
    let mut jobs = enqueue_jobs(state, app, headers, Submission::Direct(input)).await?;
    Ok((StatusCode::ACCEPTED, Json(jobs.remove(0))))
}

enum Submission {
    Direct(Input),
    Deployment(RunDeployment),
}

async fn enqueue_jobs(
    state: &AppState,
    app: Uuid,
    headers: &HeaderMap,
    submission: Submission,
) -> ApiResult<Vec<Summary>> {
    let who = actor(state, headers, app, Permission::Submit)?;
    let request_digest = match &submission {
        Submission::Direct(input) => {
            input.validate().map_err(invalid)?;
            digest(input)?
        }
        Submission::Deployment(run) => digest(&("deployment", run))?,
    };
    let idempotency = headers
        .get("idempotency-key")
        .map(|value| {
            value
                .to_str()
                .map(str::to_owned)
                .map_err(|_| invalid("Invalid idempotency key"))
        })
        .transpose()?;
    if idempotency.as_ref().is_some_and(|key| {
        key.is_empty()
            || key.len() > 64
            || !key
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
    }) {
        return Err(invalid(
            "Idempotency key must contain at most 64 letters, digits, hyphens or underscores",
        ));
    }
    let at = now_ms();
    let actor_name = who.name();
    let job_ids = mutate_store(state, |store| {
        who.authorize(store, app, Permission::Submit)?;
        if let Some(key) = idempotency.as_ref() {
            let existing: Vec<_> = store
                .application_jobs
                .values()
                .filter(|job| {
                    job.app_id == app
                        && job.actor == actor_name
                        && job.idempotency.as_ref() == Some(key)
                })
                .collect();
            if !existing.is_empty() {
                if existing
                    .iter()
                    .any(|job| job.request_digest != request_digest)
                {
                    return Err(ApiError(
                        StatusCode::CONFLICT,
                        "Idempotency key already belongs to a different request".into(),
                    ));
                }
                return Ok(existing.into_iter().map(|job| job.id).collect());
            }
        }
        let planned = match &submission {
            Submission::Direct(input) => vec![(input.clone(), None)],
            Submission::Deployment(run) => run.plan(store, app)?,
        };
        if store.application_jobs.len() + planned.len() > MAX_GLOBAL_JOBS
            || store
                .application_jobs
                .values()
                .filter(|job| job.app_id == app)
                .count()
                + planned.len()
                > MAX_JOBS
        {
            return Err(ApiError(
                StatusCode::CONFLICT,
                "Retained application job limit reached; an owner or operator must delete old jobs"
                    .into(),
            ));
        }
        let mut ids = Vec::with_capacity(planned.len());
        for (input, deployment) in planned {
            let id = Uuid::new_v4();
            for slot in 0..input.replicas() {
                store
                    .recovery
                    .add_task(task(id, slot))
                    .map_err(|_| fenced())?;
            }
            store.application_jobs.insert(
                id,
                Job {
                    id,
                    app_id: app,
                    input,
                    created_at_ms: at,
                    actor: actor_name.clone(),
                    idempotency: idempotency.clone(),
                    request_digest: request_digest.clone(),
                    receipts: Vec::new(),
                    cancelled: false,
                    failed: None,
                    failures: 0,
                    deployment,
                },
            );
            ids.push(id);
        }
        Ok(ids)
    })
    .await?;
    let store = state.store.lock().map_err(|_| unavailable())?;
    let mut result = job_ids
        .iter()
        .map(|id| {
            store
                .application_jobs
                .get(id)
                .map(|job| summary(job, &store))
                .ok_or_else(hidden)
        })
        .collect::<ApiResult<Vec<_>>>()?;
    result.sort_by(|a, b| {
        a.deployment
            .as_ref()
            .map(|job| &job.name)
            .cmp(&b.deployment.as_ref().map(|job| &job.name))
    });
    let cluster = store.applications.get(&app).ok_or_else(hidden)?.cluster_id;
    drop(store);
    crate::notify_work_available(state, cluster);
    Ok(result)
}
pub async fn submit(
    State(state): State<AppState>,
    Path(app): Path<Uuid>,
    headers: HeaderMap,
    Json(input): Json<Input>,
) -> ApiResult<(StatusCode, Json<Summary>)> {
    enqueue(&state, app, &headers, input).await
}
#[derive(Deserialize)]
pub struct InferenceInput {
    model: String,
    input: Vec<String>,
    #[serde(default)]
    verification: InferenceVerification,
}
pub async fn inference(
    State(state): State<AppState>,
    Path(app): Path<Uuid>,
    headers: HeaderMap,
    Json(input): Json<InferenceInput>,
) -> ApiResult<(StatusCode, Json<Summary>)> {
    enqueue(
        &state,
        app,
        &headers,
        Input::Embedding {
            model: input.model,
            texts: input.input,
            verification: input.verification,
        },
    )
    .await
}
pub async fn delete_job(
    State(state): State<AppState>,
    Path((app, id)): Path<(Uuid, Uuid)>,
    headers: HeaderMap,
) -> ApiResult<StatusCode> {
    let who = actor(&state, &headers, app, Permission::Manage)?;
    mutate_store(&state, |store| {
        who.authorize(store, app, Permission::Manage)?;
        let job = store
            .application_jobs
            .get(&id)
            .filter(|job| job.app_id == app)
            .ok_or_else(hidden)?;
        for slot in 0..job.input.replicas() {
            store.recovery.remove_task(&task(id, slot));
        }
        store.application_jobs.remove(&id);
        Ok(())
    })
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

fn require_application_owner(store: &Store, app: Uuid, user: &str) -> ApiResult<()> {
    security::application_access(store, app, user, true)?;
    if store
        .applications
        .get(&app)
        .is_none_or(|app| app.owner_id != user)
    {
        return Err(ApiError(
            StatusCode::FORBIDDEN,
            "Only the application owner can manage this resource".into(),
        ));
    }
    Ok(())
}
#[derive(Serialize)]
pub struct KeySummary {
    id: Uuid,
    label: String,
    created_at: u64,
    expires_at: u64,
    read_only: bool,
}
fn key_summary(key: &AppKey) -> KeySummary {
    KeySummary {
        id: key.id,
        label: key.label.clone(),
        created_at: key.created_at,
        expires_at: key.expires_at,
        read_only: key.read_only,
    }
}
#[derive(Deserialize)]
pub struct CreateKey {
    label: String,
    #[serde(default)]
    read_only: bool,
}
pub async fn create_key(
    State(state): State<AppState>,
    Path(app): Path<Uuid>,
    headers: HeaderMap,
    Json(input): Json<CreateKey>,
) -> ApiResult<Json<serde_json::Value>> {
    let user = identity(&headers)?;
    if input.label.trim().is_empty()
        || input.label.len() > 80
        || input.label.chars().any(char::is_control)
    {
        return Err(invalid("API key label must contain 1–80 characters"));
    }
    let raw = format!("mak_{}", security::new_key()?);
    let hash = secret_hash(&raw);
    let id = Uuid::new_v4();
    let key = mutate_store(&state, |store| {
        require_application_owner(store, app, &user)?;
        if store
            .application_keys
            .values()
            .filter(|key| key.app_id == app && key.expires_at > now())
            .count()
            >= 16
        {
            return Err(ApiError(
                StatusCode::CONFLICT,
                "Application API key limit reached; revoke an old key".into(),
            ));
        }
        let key = AppKey {
            id,
            app_id: app,
            issuer: user.clone(),
            label: input.label.trim().into(),
            hash: hash.clone(),
            created_at: now(),
            expires_at: now() + KEY_TTL,
            read_only: input.read_only,
        };
        let summary = key_summary(&key);
        store.application_keys.insert(id, key);
        Ok(summary)
    })
    .await?;
    Ok(Json(serde_json::json!({"key":raw,"metadata":key})))
}
pub async fn list_keys(
    State(state): State<AppState>,
    Path(app): Path<Uuid>,
    headers: HeaderMap,
) -> ApiResult<Json<Vec<KeySummary>>> {
    let user = identity(&headers)?;
    let store = state.store.lock().map_err(|_| unavailable())?;
    require_application_owner(&store, app, &user)?;
    Ok(Json(
        store
            .application_keys
            .values()
            .filter(|key| key.app_id == app && key.expires_at > now())
            .map(key_summary)
            .collect(),
    ))
}
pub async fn revoke_key(
    State(state): State<AppState>,
    Path((app, id)): Path<(Uuid, Uuid)>,
    headers: HeaderMap,
) -> ApiResult<StatusCode> {
    let user = identity(&headers)?;
    mutate_store(&state, |store| {
        require_application_owner(store, app, &user)?;
        if store
            .application_keys
            .get(&id)
            .is_none_or(|key| key.app_id != app)
        {
            return Err(hidden());
        }
        store.application_keys.remove(&id);
        Ok(())
    })
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
pub struct ClaimInput {
    node_id: Uuid,
    #[serde(default)]
    inference: bool,
}
#[derive(Serialize)]
pub struct Work {
    job_id: Uuid,
    application_id: Uuid,
    slot: usize,
    input: Input,
    lease: recovery::TaskLease,
    timeout_ms: u64,
}
fn owned_node<'a>(
    store: &'a Store,
    node: Uuid,
    user: &str,
    active: bool,
) -> ApiResult<&'a crate::Node> {
    let node = store
        .nodes
        .get(&node)
        .filter(|node| node.owner_id == user)
        .ok_or(ApiError(
            StatusCode::FORBIDDEN,
            "The worker node is not owned by this account".into(),
        ))?;
    if active && (!node.online || node.paused) {
        return Err(ApiError(
            StatusCode::CONFLICT,
            "The worker node must be connected and active".into(),
        ));
    }
    Ok(node)
}
pub async fn claim(
    State(state): State<AppState>,
    Path(cluster): Path<Uuid>,
    headers: HeaderMap,
    Json(input): Json<ClaimInput>,
) -> ApiResult<Json<Option<Work>>> {
    let user = identity(&headers)?;
    let _mutation = state.mutation_lock.lock().await;
    let mut store = state.store.lock().map_err(|_| unavailable())?;
    crate::require_member(&store, cluster, &user)?;
    if owned_node(&store, input.node_id, &user, true)?.cluster_id != cluster {
        return Err(hidden());
    }
    let mut jobs: Vec<_> = store
        .application_jobs
        .values()
        .filter(|job| {
            store
                .applications
                .get(&job.app_id)
                .is_some_and(|app| app.cluster_id == cluster)
                && application_worker_access(&store, job.app_id, &user).is_ok()
                && matches!(
                    status(job, &store),
                    "queued" | "running" | "awaiting_verification"
                )
                && (input.inference || !matches!(job.input, Input::Embedding { .. }))
        })
        .cloned()
        .collect();
    jobs.sort_by_key(|job| job.created_at_ms);
    let at = now_ms();
    for job in &jobs {
        for slot in 0..job.input.replicas() {
            if let Some(lease) = store
                .recovery
                .active_lease(&task(job.id, slot), at)
                .filter(|lease| lease.worker_id == input.node_id.to_string())
            {
                return Ok(Json(Some(Work {
                    job_id: job.id,
                    application_id: job.app_id,
                    slot,
                    input: job.input.clone(),
                    lease,
                    timeout_ms: protocol::TASK_TIMEOUT_MS,
                })));
            }
        }
    }
    for job in jobs {
        let occupied: HashSet<_> = job
            .receipts
            .iter()
            .map(|receipt| receipt.worker.to_string())
            .chain((0..job.input.replicas()).filter_map(|slot| {
                store
                    .recovery
                    .active_lease(&task(job.id, slot), at)
                    .map(|lease| lease.worker_id)
            }))
            .collect();
        if occupied.contains(&input.node_id.to_string()) {
            continue;
        }
        for slot in 0..job.input.replicas() {
            if job.receipts.iter().any(|receipt| receipt.slot == slot) {
                continue;
            }
            if let Ok(lease) = store.recovery.claim(
                &task(job.id, slot),
                input.node_id.to_string(),
                at,
                JOB_LEASE_DURATION_MS,
            ) {
                return Ok(Json(Some(Work {
                    job_id: job.id,
                    application_id: job.app_id,
                    slot,
                    input: job.input.clone(),
                    lease,
                    timeout_ms: protocol::TASK_TIMEOUT_MS,
                })));
            }
        }
    }
    Ok(Json(None))
}
#[derive(Deserialize)]
pub struct LeaseInput {
    lease: recovery::TaskLease,
}
pub(crate) fn application_worker_access(store: &Store, app: Uuid, user: &str) -> ApiResult<()> {
    if security::application_access(store, app, user, true).is_ok() {
        return Ok(());
    }
    let application = store.applications.get(&app).ok_or_else(hidden)?;
    crate::require_member(store, application.cluster_id, user).map_err(|_| hidden())?;
    if store.invites.values().any(|invite| {
        invite.application_id == Some(app)
            && invite.expires_at > now()
            && invite.contributors.contains(user)
    }) {
        return Ok(());
    }
    Err(hidden())
}
fn worker_access(
    store: &Store,
    job_id: Uuid,
    user: &str,
    lease: &recovery::TaskLease,
    active: bool,
) -> ApiResult<(Uuid, usize)> {
    let job = store.application_jobs.get(&job_id).ok_or_else(hidden)?;
    application_worker_access(store, job.app_id, user)?;
    let worker = Uuid::parse_str(&lease.worker_id).map_err(|_| fenced())?;
    let node = owned_node(store, worker, user, active)?;
    if node.cluster_id
        != store
            .applications
            .get(&job.app_id)
            .ok_or_else(hidden)?
            .cluster_id
    {
        return Err(hidden());
    }
    let slot = (0..job.input.replicas())
        .find(|slot| task(job_id, *slot) == lease.task_id)
        .ok_or_else(fenced)?;
    Ok((worker, slot))
}
#[derive(Serialize)]
pub struct WorkerAcknowledgement {
    status: &'static str,
    workers: [ReceiptSummary; 1],
}

fn receipt_for_lease<'a>(
    job: &'a Job,
    worker: Uuid,
    slot: usize,
    lease: &recovery::TaskLease,
) -> Option<&'a Receipt> {
    job.receipts.iter().find(|receipt| {
        receipt.slot == slot
            && receipt.worker == worker
            && receipt.epoch == lease.coordinator_epoch
            && receipt.generation == lease.generation
            && receipt.attempt_id == lease.attempt_id
    })
}

pub async fn receipt(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
    Json(input): Json<LeaseInput>,
) -> ApiResult<Json<WorkerAcknowledgement>> {
    let user = identity(&headers)?;
    let store = state.store.lock().map_err(|_| unavailable())?;
    let (worker, slot) = worker_access(&store, id, &user, &input.lease, false)?;
    let job = &store.application_jobs[&id];
    let accepted = receipt_for_lease(job, worker, slot, &input.lease).ok_or_else(hidden)?;
    Ok(Json(WorkerAcknowledgement {
        status: status(job, &store),
        workers: [ReceiptSummary::from(accepted)],
    }))
}
pub async fn renew(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
    Json(input): Json<LeaseInput>,
) -> ApiResult<Json<recovery::TaskLease>> {
    let user = identity(&headers)?;
    let mut store = state.store.lock().map_err(|_| unavailable())?;
    worker_access(&store, id, &user, &input.lease, true)?;
    if !matches!(
        status(&store.application_jobs[&id], &store),
        "queued" | "running" | "awaiting_verification"
    ) {
        return Err(fenced());
    }
    Ok(Json(
        store
            .recovery
            .renew(&input.lease, now_ms(), JOB_LEASE_DURATION_MS)
            .map_err(|_| fenced())?,
    ))
}
#[derive(Deserialize)]
pub struct ReleaseInput {
    lease: recovery::TaskLease,
    error: Option<String>,
}
fn note_failure(store: &mut Store, id: Uuid, error: &str) -> ApiResult<()> {
    let job = store.application_jobs.get(&id).ok_or_else(hidden)?;
    // A takeover may finish between the runtime release and this durable write.
    // A late failure must never override an accepted or otherwise terminal job.
    if !matches!(
        status(job, store),
        "queued" | "running" | "awaiting_verification"
    ) {
        return Ok(());
    }
    let job = store.application_jobs.get_mut(&id).ok_or_else(hidden)?;
    job.failures += 1;
    if job.failures >= 3 {
        job.failed = Some(format!("Three worker attempts failed: {error}"));
    }
    Ok(())
}
pub async fn release(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
    Json(input): Json<ReleaseInput>,
) -> ApiResult<StatusCode> {
    let user = identity(&headers)?;
    // Release runtime state before persisting the failure counter. merge_runtime
    // preserves current leases and must not resurrect a release made only in a clone.
    {
        let _mutation = state.mutation_lock.lock().await;
        let mut store = state.store.lock().map_err(|_| unavailable())?;
        worker_access(&store, id, &user, &input.lease, false)?;
        store
            .recovery
            .release(&input.lease, now_ms())
            .map_err(|_| fenced())?;
    }
    if let Some(error) = input.error {
        let error = error
            .chars()
            .filter(|c| !c.is_control())
            .take(256)
            .collect::<String>();
        mutate_store(&state, |store| {
            worker_access(store, id, &user, &input.lease, false)?;
            note_failure(store, id, &error)
        })
        .await?;
    }
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
#[path = "application_jobs_tests.rs"]
mod tests;
#[derive(Deserialize)]
struct CommitInput {
    lease: recovery::TaskLease,
    output: Output,
    runtime: RuntimeReport,
}
pub async fn commit(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
    request: Request,
) -> ApiResult<Response> {
    let user = identity(&headers)?;
    {
        let store = state.store.lock().map_err(|_| unavailable())?;
        let job = store.application_jobs.get(&id).ok_or_else(hidden)?;
        application_worker_access(&store, job.app_id, &user)?;
    }
    let bytes = to_bytes(request.into_body(), MAX_COMMIT_BYTES)
        .await
        .map_err(|_| {
            ApiError(
                StatusCode::PAYLOAD_TOO_LARGE,
                "Application result exceeds the 128 KiB limit".into(),
            )
        })?;
    let input: CommitInput = serde_json::from_slice(&bytes)
        .map_err(|_| invalid("Invalid application result payload"))?;
    input.runtime.validate().map_err(invalid)?;
    let result_digest = digest(&input.output)?;
    mutate_store(&state, |store| {
        let (worker, slot) = worker_access(store, id, &user, &input.lease, false)?;
        let job = store.application_jobs.get(&id).ok_or_else(hidden)?;
        if let Some(accepted) = job.receipts.iter().find(|receipt| receipt.slot == slot) {
            return if accepted.worker == worker
                && accepted.digest == result_digest
                && accepted.epoch == input.lease.coordinator_epoch
                && accepted.generation == input.lease.generation
                && accepted.attempt_id == input.lease.attempt_id
            {
                Ok(())
            } else {
                Err(fenced())
            };
        }
        worker_access(store, id, &user, &input.lease, true)?;
        if !matches!(
            status(job, store),
            "queued" | "running" | "awaiting_verification"
        ) || job.receipts.iter().any(|receipt| receipt.worker == worker)
        {
            return Err(fenced());
        }
        let at = now_ms();
        let claimed_at = store
            .recovery
            .observation(&input.lease.task_id)
            .map(|o| o.claimed_at_ms)
            .unwrap_or(at);
        store
            .recovery
            .commit_result(&input.lease, at, &result_digest)
            .map_err(|_| fenced())?;
        let job = store.application_jobs.get_mut(&id).ok_or_else(hidden)?;
        job.input.validate_output(&input.output).map_err(invalid)?;
        if let Some(previous) = job.receipts.first() {
            if !job.input.agrees(&previous.output, &input.output) {
                job.failed = Some(
                    "Replicated worker results disagree; no verified output was published".into(),
                );
            }
        }
        job.receipts.push(Receipt {
            slot,
            worker,
            owner: user.clone(),
            epoch: input.lease.coordinator_epoch,
            generation: input.lease.generation,
            attempt_id: input.lease.attempt_id,
            digest: result_digest.clone(),
            claimed_at_ms: claimed_at,
            received_at_ms: at,
            output: input.output.clone(),
            runtime: input.runtime.clone(),
        });
        Ok(())
    })
    .await?;
    let store = state.store.lock().map_err(|_| unavailable())?;
    let job = store.application_jobs.get(&id).ok_or_else(hidden)?;
    let (worker, slot) = worker_access(&store, id, &user, &input.lease, false)?;
    let result = if security::application_access(&store, job.app_id, &user, true).is_ok() {
        Json(summary(job, &store)).into_response()
    } else {
        let accepted = receipt_for_lease(job, worker, slot, &input.lease).ok_or_else(fenced)?;
        Json(WorkerAcknowledgement {
            status: status(job, &store),
            workers: [ReceiptSummary::from(accepted)],
        })
        .into_response()
    };
    let cluster = store.applications[&job.app_id].cluster_id;
    drop(store);
    crate::notify_work_available(&state, cluster);
    Ok(result)
}
pub async fn runtime_info(
    State(state): State<AppState>,
    Path(app): Path<Uuid>,
    headers: HeaderMap,
) -> ApiResult<Json<serde_json::Value>> {
    let who = actor(&state, &headers, app, Permission::Read)?;
    let store = state.store.lock().map_err(|_| unavailable())?;
    who.authorize(&store, app, Permission::Read)?;
    let jobs: Vec<_> = store
        .application_jobs
        .values()
        .filter(|job| job.app_id == app)
        .collect();
    let mut elapsed: Vec<_> = jobs
        .iter()
        .filter(|job| status(job, &store) == "completed")
        .filter_map(|job| {
            job.receipts
                .iter()
                .map(|r| r.received_at_ms.saturating_sub(job.created_at_ms))
                .max()
        })
        .collect();
    elapsed.sort_unstable();
    let percentile = |fraction: f64| {
        elapsed
            .get(((elapsed.len().saturating_sub(1)) as f64 * fraction).ceil() as usize)
            .copied()
    };
    Ok(Json(
        serde_json::json!({"runtime":"browser-jobs-v1","operations":["vector_add","matmul","embedding"],"asynchronous":true,"task_timeout_ms":protocol::TASK_TIMEOUT_MS,
        "models":[{"id":protocol::MODEL,"repository":protocol::MODEL_REPOSITORY,"revision":protocol::MODEL_REVISION,"dimensions":protocol::EMBEDDING_DIMENSIONS,"dtype":"fp32","max_texts":4,"max_tokens":128}],
        "verification":{"numeric":"full coordinator comparison","inference_default":"two distinct node IDs agree; not a proof of honest computation","single":"shape checks only; unverified"},
        "retained_jobs":jobs.len(),"metrics":{"completed_samples":elapsed.len(),"result_received_p50_ms":percentile(0.5),"result_received_p95_ms":percentile(0.95),"note":"Coordinator queue-to-result-receipt timing; worker compute/load times are self-reported. Client round-trip includes persistence and polling."}}),
    ))
}
