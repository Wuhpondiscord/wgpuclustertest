//! Bounded in-memory batch data plane. GitHub persists app grants and links only.
use crate::{
    application_jobs, identity, now, secret_hash, security, ApiError, ApiResult, AppState, Store,
};
use axum::{
    body::to_bytes,
    extract::{Path, Query, Request, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{Arc, Mutex, MutexGuard, Weak},
    time::{Duration, Instant},
};
use tokio::sync::{Notify, Semaphore};
use uuid::Uuid;

const IMAGE_B64_LEN: usize = 96 * 96 * 4; // RGB8, exactly three bytes per pixel.
const MAX_ITEMS: usize = 1024;
const MAX_RUNS: usize = 12;
const MAX_APP_RUNS: usize = 4;
const MAX_BUFFERED_BYTES: usize = 72 * 1024 * 1024;
const MAX_BODY: usize = (IMAGE_B64_LEN + 3) * MAX_ITEMS + 128;
const LEASE: Duration = Duration::from_secs(45);
const RETENTION: Duration = Duration::from_secs(30 * 60);
const LONG_POLL: Duration = Duration::from_secs(12);
const MAX_ATTEMPTS: u32 = 5;
const INVITE_HEADER: &str = "x-mesh-ml-invite";

pub struct Runtime {
    queue: Mutex<Queue>,
    uploads: Semaphore,
    waiters: Semaphore,
}
impl Default for Runtime {
    fn default() -> Self {
        Self {
            queue: Mutex::new(Queue::default()),
            uploads: Semaphore::new(1),
            waiters: Semaphore::new(4096),
        }
    }
}
impl Runtime {
    fn queue(&self) -> ApiResult<MutexGuard<'_, Queue>> {
        self.queue.lock().map_err(|_| unavailable())
    }
    pub fn sweep(&self, store: &Store) {
        if let Ok(mut queue) = self.queue() {
            let deleted: Vec<_> = queue
                .runs
                .values()
                .filter(|run| !store.applications.contains_key(&run.app))
                .map(|run| run.id)
                .collect();
            for id in deleted {
                queue.remove(id);
            }
            queue.sweep(Instant::now());
        }
    }
}

#[derive(Default)]
struct Queue {
    runs: HashMap<Uuid, Run>,
    apps: HashMap<Uuid, VecDeque<Uuid>>,
    wait: HashMap<(Uuid, bool), Weak<Notify>>,
    buffered: usize,
}
struct Run {
    id: Uuid,
    app: Uuid,
    expires: Instant,
    items: Vec<Item>,
    ready: VecDeque<usize>,
    active: HashMap<Worker, usize>,
    completed: usize,
    failed: usize,
    bytes: usize,
    features: bool,
}
#[derive(Clone, PartialEq, Eq, Hash)]
struct Worker {
    id: Uuid,
    principal: String,
}
struct Item {
    image: Option<String>,
    attempts: u32,
    lease: Option<Lease>,
    result: Option<ResultRow>,
    failed: bool,
}
struct Lease {
    token: Uuid,
    worker: Worker,
    expires: Instant,
}
#[derive(Clone, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct Prediction {
    class_id: u16,
    score: f32,
}
struct ResultRow {
    token: Uuid,
    worker: Worker,
    predictions: Vec<Prediction>,
    features: Option<Vec<f32>>,
    backend: Backend,
    compute_ms: f64,
    load_ms: f64,
}
#[derive(Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum Backend {
    Webgpu,
    Wasm,
}

impl Run {
    fn new(id: Uuid, app: Uuid, images: Vec<String>, at: Instant) -> Self {
        let ready = (0..images.len()).collect();
        let bytes = images.iter().map(String::len).sum();
        Self {
            id,
            app,
            expires: at + RETENTION,
            ready,
            bytes,
            features: false,
            items: images
                .into_iter()
                .map(|image| Item {
                    image: Some(image),
                    attempts: 0,
                    lease: None,
                    result: None,
                    failed: false,
                })
                .collect(),
            active: HashMap::new(),
            completed: 0,
            failed: 0,
        }
    }
    fn claim(&mut self, worker: &Worker, at: Instant) -> Option<Work> {
        let index = if let Some(index) = self.active.get(worker).copied() {
            if self.items[index].lease.as_ref()?.expires <= at {
                return None;
            }
            index
        } else {
            let index = self.ready.pop_front()?;
            let item = &mut self.items[index];
            item.attempts += 1;
            item.lease = Some(Lease {
                token: Uuid::new_v4(),
                worker: worker.clone(),
                expires: at + LEASE,
            });
            self.active.insert(worker.clone(), index);
            index
        };
        let item = &self.items[index];
        Some(Work {
            run_id: self.id,
            index,
            lease: item.lease.as_ref()?.token,
            image_rgb96: item.image.clone()?,
            features: self.features,
        })
    }
    fn checked_lease(
        &mut self,
        input: &Attempt,
        worker: &Worker,
        at: Instant,
    ) -> ApiResult<&mut Lease> {
        self.items
            .get_mut(input.index)
            .and_then(|item| item.lease.as_mut())
            .filter(|lease| {
                lease.token == input.lease && &lease.worker == worker && lease.expires > at
            })
            .ok_or_else(fenced)
    }
    fn requeue(&mut self, index: usize) -> usize {
        let item = &mut self.items[index];
        if let Some(lease) = item.lease.take() {
            self.active.remove(&lease.worker);
            if item.attempts < MAX_ATTEMPTS {
                self.ready.push_back(index);
            } else {
                item.failed = true;
                let released = item.image.take().map_or(0, |image| image.len());
                self.failed += 1;
                self.bytes -= released;
                return released;
            }
        }
        0
    }
}
impl Queue {
    fn notifier(&mut self, app: Uuid, warm: bool) -> Arc<Notify> {
        if let Some(current) = self.wait.get(&(app, warm)).and_then(Weak::upgrade) {
            return current;
        }
        let created = Arc::new(Notify::new());
        self.wait.insert((app, warm), Arc::downgrade(&created));
        created
    }
    fn wake(&self, app: Uuid, mut ready: usize) {
        // Each publication supplies a wake budget. Warm waiters get first pick
        // without waking more browsers than the number of newly ready images.
        for warm in [true, false] {
            if let Some(waiters) = self.wait.get(&(app, warm)).and_then(Weak::upgrade) {
                let count = ready.min(Arc::strong_count(&waiters) - 1);
                for _ in 0..count {
                    waiters.notify_one();
                }
                ready -= count;
            }
        }
    }
    fn remove(&mut self, id: Uuid) {
        if let Some(run) = self.runs.remove(&id) {
            self.buffered -= run.bytes;
            self.apps
                .entry(run.app)
                .and_modify(|ids| ids.retain(|id2| id2 != &id));
        }
    }
    fn sweep(&mut self, at: Instant) {
        let expired: Vec<_> = self
            .runs
            .values()
            .filter(|run| run.expires <= at)
            .map(|run| run.id)
            .collect();
        for id in expired {
            self.remove(id);
        }
        self.apps.retain(|_, ids| !ids.is_empty());
        self.wait.retain(|_, waiter| waiter.strong_count() != 0);
        let mut woken = HashMap::<Uuid, usize>::new();
        for run in self.runs.values_mut() {
            for index in 0..run.items.len() {
                if run.items[index]
                    .lease
                    .as_ref()
                    .is_some_and(|lease| lease.expires <= at)
                {
                    self.buffered -= run.requeue(index);
                    if !run.items[index].failed {
                        *woken.entry(run.app).or_default() += 1;
                    }
                }
            }
        }
        for (app, count) in woken {
            self.wake(app, count);
        }
    }
    fn insert(&mut self, app: Uuid, images: Vec<String>, at: Instant) -> ApiResult<Uuid> {
        self.sweep(at);
        let id = Uuid::new_v4();
        let run = Run::new(id, app, images, at);
        if self.runs.len() >= MAX_RUNS
            || self.buffered + run.bytes > MAX_BUFFERED_BYTES
            || self
                .apps
                .get(&app)
                .is_some_and(|ids| ids.len() >= MAX_APP_RUNS)
        {
            return Err(ApiError(
                StatusCode::CONFLICT,
                "Image batch storage is full. Delete a run or wait for it to expire.".into(),
            ));
        }
        self.buffered += run.bytes;
        let count = run.ready.len();
        self.runs.insert(id, run);
        self.apps.entry(app).or_default().push_back(id);
        self.wake(app, count);
        Ok(id)
    }
    #[cfg(test)]
    fn claim(&mut self, app: Uuid, worker: &Worker, at: Instant) -> Option<Work> {
        self.claim_capable(app, worker, at, false)
    }
    fn claim_capable(
        &mut self,
        app: Uuid,
        worker: &Worker,
        at: Instant,
        features: bool,
    ) -> Option<Work> {
        let ids = self.apps.get(&app)?;
        let runs = &self.runs;
        let live = |id: &&Uuid| runs[*id].expires > at && (!runs[*id].features || features);
        let resume = ids
            .iter()
            .filter(live)
            .find(|id| runs[*id].active.contains_key(worker));
        let id = resume.or_else(|| {
            ids.iter()
                .filter(live)
                .find(|id| !runs[*id].ready.is_empty())
        })?;
        self.runs.get_mut(id)?.claim(worker, at)
    }
    fn run(&self, app: Uuid, id: Uuid, at: Instant) -> ApiResult<&Run> {
        self.runs
            .get(&id)
            .filter(|run| run.app == app && run.expires > at)
            .ok_or_else(hidden)
    }
    fn run_mut(&mut self, app: Uuid, id: Uuid, at: Instant) -> ApiResult<&mut Run> {
        self.runs
            .get_mut(&id)
            .filter(|run| run.app == app && run.expires > at)
            .ok_or_else(hidden)
    }
}

fn hidden() -> ApiError {
    ApiError(
        StatusCode::NOT_FOUND,
        "Image batch or contributor link unavailable".into(),
    )
}
fn fenced() -> ApiError {
    ApiError(
        StatusCode::CONFLICT,
        "Image task lease is no longer current".into(),
    )
}
fn unavailable() -> ApiError {
    ApiError(
        StatusCode::SERVICE_UNAVAILABLE,
        "Image runtime is temporarily busy".into(),
    )
}
fn invalid() -> ApiError {
    ApiError(
        StatusCode::UNPROCESSABLE_ENTITY,
        "Invalid image batch or prediction".into(),
    )
}

#[derive(Deserialize)]
pub struct InviteOptions {
    #[serde(default)]
    pub ml_only: bool,
}
fn invite<'a>(store: &'a Store, headers: &HeaderMap) -> ApiResult<(&'a crate::Invite, String)> {
    let token = headers
        .get(INVITE_HEADER)
        .and_then(|value| value.to_str().ok())
        .filter(|value| value.len() == 64 && value.bytes().all(|c| c.is_ascii_hexdigit()))
        .ok_or_else(hidden)?;
    let hash = secret_hash(token);
    let invite = store
        .invites
        .get(&hash)
        .filter(|invite| invite.ml_only && invite.expires_at > now())
        .ok_or_else(hidden)?;
    Ok((invite, hash))
}
fn worker(store: &Store, headers: &HeaderMap, app: Uuid, id: Uuid) -> ApiResult<Worker> {
    let principal = if headers.contains_key(INVITE_HEADER) {
        let (grant, hash) = invite(store, headers)?;
        if grant.application_id != Some(app) || !store.applications.contains_key(&app) {
            return Err(hidden());
        }
        let browser_key = headers
            .get("x-mesh-ml-browser")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| Uuid::parse_str(value).ok())
            .ok_or_else(hidden)?;
        format!("{hash}:{}", secret_hash(&browser_key.to_string()))
    } else {
        let user = identity(headers)?;
        application_jobs::application_worker_access(store, app, &user)?;
        user
    };
    Ok(Worker { id, principal })
}
async fn preview(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> ApiResult<Json<serde_json::Value>> {
    let store = state.store.lock().map_err(|_| unavailable())?;
    let (invite, _) = invite(&store, &headers)?;
    let app = invite
        .application_id
        .and_then(|id| store.applications.get(&id))
        .ok_or_else(hidden)?;
    let cluster = store.clusters.get(&invite.cluster_id).ok_or_else(hidden)?;
    Ok(Json(
        serde_json::json!({"application_id":app.id,"application_name":app.name,"cluster_name":cluster.name}),
    ))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Images {
    images: Vec<String>,
}
fn parse_images(bytes: &[u8]) -> ApiResult<Vec<String>> {
    let Images { images } = serde_json::from_slice(bytes).map_err(|_| invalid())?;
    if images.is_empty()
        || images.len() > MAX_ITEMS
        || images.iter().any(|image| {
            image.len() != IMAGE_B64_LEN
                || !image
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || c == b'+' || c == b'/')
        })
    {
        return Err(invalid());
    }
    Ok(images)
}
#[derive(Default, Deserialize)]
struct FeatureOptions {
    #[serde(default)]
    features: bool,
}
async fn submit(
    State(state): State<AppState>,
    Path(app): Path<Uuid>,
    Query(options): Query<FeatureOptions>,
    headers: HeaderMap,
    request: Request,
) -> ApiResult<(StatusCode, Json<serde_json::Value>)> {
    let user = identity(&headers)?;
    {
        let store = state.store.lock().map_err(|_| unavailable())?;
        security::application_access(&store, app, &user, true)?;
    }
    let _upload = state
        .image_batches
        .uploads
        .try_acquire()
        .map_err(|_| unavailable())?;
    let bytes = tokio::time::timeout(
        Duration::from_secs(60),
        to_bytes(request.into_body(), MAX_BODY),
    )
    .await
    .map_err(|_| ApiError(StatusCode::REQUEST_TIMEOUT, "Image upload timed out".into()))?
    .map_err(|_| {
        ApiError(
            StatusCode::PAYLOAD_TOO_LARGE,
            "Image batch upload is too large".into(),
        )
    })?;
    let images = tokio::task::spawn_blocking(move || parse_images(&bytes))
        .await
        .map_err(|_| unavailable())??;
    let store = state.store.lock().map_err(|_| unavailable())?;
    security::application_access(&store, app, &user, true)?;
    let mut queue = state.image_batches.queue()?;
    let reserved = if options.features {
        images.len() * 1000 * 4
    } else {
        0
    };
    if queue.buffered + images.iter().map(String::len).sum::<usize>() + reserved
        > MAX_BUFFERED_BYTES
    {
        return Err(unavailable());
    }
    let id = queue.insert(app, images, Instant::now())?;
    let run = queue.runs.get_mut(&id).ok_or_else(unavailable)?;
    run.features = options.features;
    run.bytes += reserved;
    queue.buffered += reserved;
    Ok((StatusCode::ACCEPTED, Json(serde_json::json!({"id":id}))))
}
#[derive(Serialize)]
struct Row {
    index: usize,
    worker_id: Uuid,
    attempts: u32,
    backend: Backend,
    compute_ms: f64,
    load_ms: f64,
    predictions: Vec<Prediction>,
    features: Option<Vec<f32>>,
}
#[derive(Serialize)]
struct Progress {
    id: Uuid,
    total: usize,
    completed: usize,
    failed: usize,
    active: usize,
    contributors: usize,
}
impl From<&Run> for Progress {
    fn from(run: &Run) -> Self {
        Self {
            id: run.id,
            total: run.items.len(),
            completed: run.completed,
            failed: run.failed,
            active: run.active.len(),
            contributors: run
                .items
                .iter()
                .filter_map(|item| item.result.as_ref())
                .map(|result| result.worker.id)
                .collect::<HashSet<_>>()
                .len(),
        }
    }
}
async fn list(
    State(state): State<AppState>,
    Path(app): Path<Uuid>,
    headers: HeaderMap,
) -> ApiResult<Json<Vec<Progress>>> {
    let user = identity(&headers)?;
    let store = state.store.lock().map_err(|_| unavailable())?;
    security::application_access(&store, app, &user, false)?;
    drop(store);
    let queue = state.image_batches.queue()?;
    let at = Instant::now();
    let runs = queue
        .apps
        .get(&app)
        .into_iter()
        .flatten()
        .rev()
        .filter_map(|id| queue.run(app, *id, at).ok())
        .map(Progress::from)
        .collect();
    Ok(Json(runs))
}
#[derive(Deserialize)]
struct DetailOptions {
    #[serde(default)]
    results: bool,
}
async fn details(
    State(state): State<AppState>,
    Path((app, run)): Path<(Uuid, Uuid)>,
    Query(options): Query<DetailOptions>,
    headers: HeaderMap,
) -> ApiResult<Json<serde_json::Value>> {
    let user = identity(&headers)?;
    let store = state.store.lock().map_err(|_| unavailable())?;
    security::application_access(&store, app, &user, false)?;
    drop(store);
    let queue = state.image_batches.queue()?;
    let run = queue.run(app, run, Instant::now())?;
    let progress = Progress::from(run);
    let results = options.results.then(|| {
        run.items
            .iter()
            .enumerate()
            .filter_map(|(index, item)| {
                let result = item.result.as_ref()?;
                Some(Row {
                    index,
                    worker_id: result.worker.id,
                    attempts: item.attempts,
                    backend: result.backend,
                    compute_ms: result.compute_ms,
                    load_ms: result.load_ms,
                    predictions: result.predictions.clone(),
                    features: result.features.clone(),
                })
            })
            .collect::<Vec<_>>()
    });
    drop(queue);
    #[derive(Serialize)]
    struct Details {
        #[serde(flatten)]
        progress: Progress,
        results: Option<Vec<Row>>,
    }
    Ok(Json(
        serde_json::to_value(Details { progress, results }).map_err(|_| unavailable())?,
    ))
}
async fn delete(
    State(state): State<AppState>,
    Path((app, run)): Path<(Uuid, Uuid)>,
    headers: HeaderMap,
) -> ApiResult<StatusCode> {
    let user = identity(&headers)?;
    let store = state.store.lock().map_err(|_| unavailable())?;
    security::application_access(&store, app, &user, true)?;
    let mut queue = state.image_batches.queue()?;
    queue.run(app, run, Instant::now())?;
    queue.remove(run);
    Ok(StatusCode::NO_CONTENT)
}
#[derive(Deserialize)]
struct Claim {
    worker_id: Uuid,
    #[serde(default)]
    warm: bool,
    #[serde(default)]
    features: bool,
}
#[derive(Serialize)]
struct Work {
    run_id: Uuid,
    index: usize,
    lease: Uuid,
    image_rgb96: String,
    features: bool,
}
#[derive(Serialize)]
#[serde(untagged)]
enum ClaimReply {
    Work(Work),
    Ready { ready: bool },
}
async fn claim(
    State(state): State<AppState>,
    Path(app): Path<Uuid>,
    headers: HeaderMap,
    Json(input): Json<Claim>,
) -> ApiResult<Json<Option<ClaimReply>>> {
    let (_permit, waiting) = {
        let store = state.store.lock().map_err(|_| unavailable())?;
        let worker = worker(&store, &headers, app, input.worker_id)?;
        let mut queue = state.image_batches.queue()?;
        if let Some(work) = queue.claim_capable(app, &worker, Instant::now(), input.features) {
            return Ok(Json(Some(ClaimReply::Work(work))));
        }
        let permit = state
            .image_batches
            .waiters
            .try_acquire()
            .map_err(|_| unavailable())?;
        let mut waiting = Box::pin(queue.notifier(app, input.warm).notified_owned());
        // Register before releasing the queue lock so publication can't miss us.
        waiting.as_mut().enable();
        (permit, waiting)
    };
    if tokio::time::timeout(LONG_POLL, waiting).await.is_err() {
        return Ok(Json(None));
    }
    let store = state.store.lock().map_err(|_| unavailable())?;
    if let Err(error) = worker(&store, &headers, app, input.worker_id) {
        state.image_batches.queue()?.wake(app, 1);
        return Err(error);
    }
    // A proxy can keep an idle request alive after its tab closes. Only a new
    // request can take work after this wake, so an abandoned poll takes no lease.
    Ok(Json(Some(ClaimReply::Ready { ready: true })))
}
#[derive(Deserialize)]
struct Attempt {
    worker_id: Uuid,
    index: usize,
    lease: Uuid,
}
#[derive(Deserialize)]
struct Commit {
    #[serde(flatten)]
    attempt: Attempt,
    predictions: Vec<Prediction>,
    features: Option<Vec<f32>>,
    backend: Backend,
    compute_ms: f64,
    load_ms: f64,
}
async fn renew(
    State(state): State<AppState>,
    Path((app, id)): Path<(Uuid, Uuid)>,
    headers: HeaderMap,
    Json(input): Json<Attempt>,
) -> ApiResult<StatusCode> {
    let store = state.store.lock().map_err(|_| unavailable())?;
    let worker = worker(&store, &headers, app, input.worker_id)?;
    let mut queue = state.image_batches.queue()?;
    let at = Instant::now();
    queue
        .run_mut(app, id, at)?
        .checked_lease(&input, &worker, at)?
        .expires = at + LEASE;
    Ok(StatusCode::NO_CONTENT)
}
async fn release(
    State(state): State<AppState>,
    Path((app, id)): Path<(Uuid, Uuid)>,
    headers: HeaderMap,
    Json(input): Json<Attempt>,
) -> ApiResult<StatusCode> {
    let store = state.store.lock().map_err(|_| unavailable())?;
    let worker = worker(&store, &headers, app, input.worker_id)?;
    let mut queue = state.image_batches.queue()?;
    let at = Instant::now();
    let run = queue.run_mut(app, id, at)?;
    run.checked_lease(&input, &worker, at)?;
    let released = run.requeue(input.index);
    queue.buffered -= released;
    if released == 0 {
        queue.wake(app, 1);
    }
    Ok(StatusCode::NO_CONTENT)
}
async fn commit(
    State(state): State<AppState>,
    Path((app, id)): Path<(Uuid, Uuid)>,
    headers: HeaderMap,
    Json(input): Json<Commit>,
) -> ApiResult<StatusCode> {
    if input.features.as_ref().is_some_and(|values| {
        values.len() != 1000
            || values
                .iter()
                .any(|value| !value.is_finite() || value.abs() > 10000.0)
    }) || input.predictions.len() != 3
        || input
            .predictions
            .iter()
            .any(|p| p.class_id >= 1000 || !(0.0..=1.0).contains(&p.score))
        || input.predictions.iter().enumerate().any(|(i, p)| {
            input.predictions[..i]
                .iter()
                .any(|previous| previous.class_id == p.class_id || previous.score < p.score)
        })
        || input.predictions.iter().map(|p| p.score).sum::<f32>() > 1.00001
        || [input.compute_ms, input.load_ms]
            .iter()
            .any(|ms| !(0.0..=300_000.0).contains(ms))
    {
        return Err(invalid());
    }
    let store = state.store.lock().map_err(|_| unavailable())?;
    let worker = worker(&store, &headers, app, input.attempt.worker_id)?;
    let mut queue = state.image_batches.queue()?;
    let at = Instant::now();
    let run = queue.run_mut(app, id, at)?;
    if run.features != input.features.is_some() {
        return Err(invalid());
    }
    if let Some(result) = run
        .items
        .get(input.attempt.index)
        .and_then(|item| item.result.as_ref())
    {
        return if result.worker == worker
            && result.token == input.attempt.lease
            && result.predictions == input.predictions
            && result.features == input.features
        {
            Ok(StatusCode::NO_CONTENT)
        } else {
            Err(fenced())
        };
    }
    run.checked_lease(&input.attempt, &worker, at)?;
    let item = &mut run.items[input.attempt.index];
    item.lease = None;
    let released = item.image.take().map_or(0, |image| image.len());
    run.active.remove(&worker);
    item.result = Some(ResultRow {
        token: input.attempt.lease,
        worker,
        predictions: input.predictions,
        features: input.features,
        backend: input.backend,
        compute_ms: input.compute_ms,
        load_ms: input.load_ms,
    });
    run.completed += 1;
    run.bytes -= released;
    queue.buffered -= released;
    Ok(StatusCode::NO_CONTENT)
}
pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/api/ml-invite", get(preview))
        .route(
            "/api/applications/:app/image-batches",
            get(list).post(submit),
        )
        .route(
            "/api/applications/:app/image-batches/:run",
            get(details).delete(delete),
        )
        .route("/api/applications/:app/image-work/claim", post(claim))
        .route("/api/applications/:app/image-work/:run/renew", post(renew))
        .route(
            "/api/applications/:app/image-work/:run/release",
            post(release),
        )
        .route(
            "/api/applications/:app/image-work/:run/commit",
            post(commit),
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn old_workers_skip_feature_tasks_and_memory_is_reclaimed() {
        let mut queue = Queue::default();
        let app = Uuid::new_v4();
        let at = Instant::now();
        let id = queue.insert(app, vec!["input".into()], at).unwrap();
        queue.runs.get_mut(&id).unwrap().features = true;
        let worker = Worker {
            id: Uuid::new_v4(),
            principal: "test".into(),
        };
        assert!(queue.claim(app, &worker, at).is_none());
        assert!(
            queue
                .claim_capable(app, &worker, at, true)
                .unwrap()
                .features
        );
        queue.remove(id);
        assert_eq!(queue.buffered, 0);
    }

    #[test]
    fn wakes_a_warm_waiter_first_when_only_one_image_is_ready() {
        use futures_util::FutureExt;
        let mut queue = Queue::default();
        let app = Uuid::new_v4();
        let at = Instant::now();
        let mut warm_waiter = Box::pin(queue.notifier(app, true).notified_owned());
        let mut cold_waiter = Box::pin(queue.notifier(app, false).notified_owned());
        warm_waiter.as_mut().enable();
        cold_waiter.as_mut().enable();
        queue.insert(app, vec!["input".into()], at).unwrap();
        assert!(warm_waiter.now_or_never().is_some());
        assert!(cold_waiter.now_or_never().is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn dispatches_a_thousand_waiting_workers_then_reassigns_expired_tasks() {
        const WORKERS: usize = 1000;
        let queue = Arc::new(Mutex::new(Queue::default()));
        let app = Uuid::new_v4();
        let notify = queue.lock().unwrap().notifier(app, false);
        let (ready, mut waiting) = tokio::sync::mpsc::unbounded_channel();
        let mut workers = tokio::task::JoinSet::new();
        let at = Instant::now();
        for _ in 0..WORKERS {
            let (queue, notify, ready) = (queue.clone(), notify.clone(), ready.clone());
            workers.spawn(async move {
                let next = notify.notified();
                tokio::pin!(next);
                next.as_mut().enable();
                ready.send(()).unwrap();
                next.await;
                let worker = Worker {
                    id: Uuid::new_v4(),
                    principal: "test".into(),
                };
                let work = queue.lock().unwrap().claim(app, &worker, at).unwrap();
                (worker, work)
            });
        }
        for _ in 0..WORKERS {
            waiting.recv().await.unwrap();
        }
        let run_id = queue
            .lock()
            .unwrap()
            .insert(app, vec!["simulated input".into(); WORKERS], at)
            .unwrap();
        let assignments = tokio::time::timeout(Duration::from_secs(5), async {
            let mut unique = HashSet::new();
            let mut first = None;
            while let Some(result) = workers.join_next().await {
                let (worker, work) = result.unwrap();
                assert_eq!(work.run_id, run_id);
                assert!(unique.insert(work.index));
                if work.index == 0 {
                    first = Some((worker, work));
                }
            }
            assert_eq!(unique.len(), WORKERS);
            first.unwrap()
        })
        .await
        .expect("every waiting worker receives a targeted wake");
        let mut queue = queue.lock().unwrap();
        let (worker, first) = assignments;
        let (other_app, wrong_id) = (Uuid::new_v4(), Uuid::new_v4());
        assert!(queue.claim(other_app, &worker, at).is_none());
        assert_eq!(queue.claim(app, &worker, at).unwrap().lease, first.lease);
        let resumed = at + LEASE;
        queue.sweep(resumed);
        let takeover = Worker {
            id: wrong_id,
            principal: "test".into(),
        };
        let second = queue.claim(app, &takeover, resumed).unwrap();
        let run = queue.run_mut(app, run_id, resumed).unwrap();
        assert_eq!(second.index, first.index);
        assert_ne!(first.lease, second.lease);
        let stale = Attempt {
            worker_id: worker.id,
            index: first.index,
            lease: first.lease,
        };
        assert!(run.checked_lease(&stale, &worker, resumed).is_err());
        let valid = Attempt {
            worker_id: takeover.id,
            index: second.index,
            lease: second.lease,
        };
        assert!(run.checked_lease(&valid, &takeover, resumed).is_ok());
        assert_eq!(run.items[second.index].attempts, 2);
        queue.sweep(at + RETENTION);
        assert!(queue.runs.is_empty());
        assert_eq!(queue.buffered, 0);
    }
}
