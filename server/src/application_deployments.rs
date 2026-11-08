//! Versioned, declarative application deployments. Workers receive ordinary leased jobs.
use super::{
    actor, enqueue_jobs, hidden, invalid, require_application_owner, unavailable, ApiError,
    ApiResult, AppState, HeaderMap, Input, Json, Path, Permission, State, StatusCode, Store,
    Submission, Summary, Uuid,
};
use crate::{identity, mutate_store, now_ms};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

const MAX_MANIFEST_BYTES: usize = 32 * 1024;
const MAX_DEPLOYMENT_JOBS: usize = 8;
const MAX_ACTIVE_DEPLOYMENTS: usize = 64;

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    version: u8,
    name: String,
    jobs: Vec<ManifestJob>,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ManifestJob {
    name: String,
    input: Input,
}

impl Manifest {
    fn validate(&self) -> ApiResult<()> {
        if self.version != 1 {
            return Err(invalid("Use mesh.json version 1"));
        }
        if self.name.trim().is_empty()
            || self.name.len() > 80
            || self.name.chars().any(char::is_control)
            || !(1..=MAX_DEPLOYMENT_JOBS).contains(&self.jobs.len())
        {
            return Err(invalid(
                "Use a workflow name of up to 80 bytes and 1 to 8 jobs",
            ));
        }
        let mut names = HashSet::new();
        for job in &self.jobs {
            if job.name.is_empty()
                || job.name.len() > 40
                || !job
                    .name
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
                || !names.insert(&job.name)
            {
                return Err(invalid("Job names must be unique and use 1 to 40 letters, digits, hyphens or underscores"));
            }
            job.input
                .validate()
                .map_err(|error| invalid(format!("Job {}: {error}", job.name)))?;
        }
        let bytes = serde_json::to_vec(self).map_err(|_| invalid("Invalid workflow manifest"))?;
        if bytes.len() > MAX_MANIFEST_BYTES {
            return Err(invalid("Workflow manifest exceeds 32 KiB"));
        }
        Ok(())
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Source {
    Upload,
    Github { repository: String, commit: String },
}

impl Source {
    fn validate(&self) -> ApiResult<()> {
        if let Self::Github { repository, commit } = self {
            let valid_repository = repository.split_once('/').is_some_and(|(owner, repo)| {
                !owner.is_empty()
                    && owner.len() <= 39
                    && owner
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'-')
                    && !matches!(repo, "" | "." | "..")
                    && repo.len() <= 100
                    && repo
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
            });
            if !valid_repository
                || commit.len() != 40
                || !commit.bytes().all(|b| b.is_ascii_hexdigit())
            {
                return Err(invalid(
                    "Use a GitHub owner/repository and a full commit SHA",
                ));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Deserialize, Serialize)]
pub struct Deployment {
    id: Uuid,
    deployed_at_ms: u64,
    manifest: Manifest,
    source: Source,
}

#[derive(Clone, Deserialize, Serialize)]
pub(super) struct DeploymentTask {
    pub(super) name: String,
    revision_id: Uuid,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DeployRequest {
    expected_revision_id: Option<Uuid>,
    manifest: Manifest,
    source: Source,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RunDeployment {
    revision_id: Uuid,
}

fn revision_changed() -> ApiError {
    ApiError(
        StatusCode::CONFLICT,
        "The active deployment changed. Refresh the application and try again".into(),
    )
}

impl RunDeployment {
    pub(super) fn plan(
        &self,
        store: &Store,
        app: Uuid,
    ) -> ApiResult<Vec<(Input, Option<DeploymentTask>)>> {
        let deployment = store
            .application_deployments
            .get(&app)
            .ok_or_else(revision_changed)?;
        if deployment.id != self.revision_id {
            return Err(revision_changed());
        }
        Ok(deployment
            .manifest
            .jobs
            .iter()
            .map(|job| {
                (
                    job.input.clone(),
                    Some(DeploymentTask {
                        name: job.name.clone(),
                        revision_id: deployment.id,
                    }),
                )
            })
            .collect())
    }
}

pub(super) async fn get(
    State(state): State<AppState>,
    Path(app): Path<Uuid>,
    headers: HeaderMap,
) -> ApiResult<Json<Option<Deployment>>> {
    let who = actor(&state, &headers, app, Permission::Read)?;
    let store = state.store.lock().map_err(|_| unavailable())?;
    who.authorize(&store, app, Permission::Read)?;
    Ok(Json(store.application_deployments.get(&app).cloned()))
}

pub(super) async fn deploy(
    State(state): State<AppState>,
    Path(app): Path<Uuid>,
    headers: HeaderMap,
    Json(request): Json<DeployRequest>,
) -> ApiResult<(StatusCode, Json<Deployment>)> {
    let user = identity(&headers)?;
    request.manifest.validate()?;
    request.source.validate()?;
    let deployment = Deployment {
        id: Uuid::new_v4(),
        deployed_at_ms: now_ms(),
        manifest: request.manifest,
        source: request.source,
    };
    mutate_store(&state, |store| {
        require_application_owner(store, app, &user)?;
        let current = store.application_deployments.get(&app).map(|item| item.id);
        if current != request.expected_revision_id {
            return Err(revision_changed());
        }
        if current.is_none() && store.application_deployments.len() >= MAX_ACTIVE_DEPLOYMENTS {
            return Err(ApiError(
                StatusCode::CONFLICT,
                "Coordinator deployment limit reached".into(),
            ));
        }
        store
            .application_deployments
            .insert(app, deployment.clone());
        Ok(())
    })
    .await?;
    Ok((StatusCode::CREATED, Json(deployment)))
}

pub(super) async fn remove(
    State(state): State<AppState>,
    Path((app, revision)): Path<(Uuid, Uuid)>,
    headers: HeaderMap,
) -> ApiResult<StatusCode> {
    let user = identity(&headers)?;
    mutate_store(&state, |store| {
        require_application_owner(store, app, &user)?;
        let deployment = store.application_deployments.get(&app).ok_or_else(hidden)?;
        if deployment.id != revision {
            return Err(revision_changed());
        }
        store.application_deployments.remove(&app);
        Ok(())
    })
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

pub(super) async fn run(
    State(state): State<AppState>,
    Path(app): Path<Uuid>,
    headers: HeaderMap,
    Json(run): Json<RunDeployment>,
) -> ApiResult<(StatusCode, Json<Vec<Summary>>)> {
    let jobs = enqueue_jobs(&state, app, &headers, Submission::Deployment(run)).await?;
    Ok((StatusCode::ACCEPTED, Json(jobs)))
}
