//! Small, versioned detector heads. Image datasets and feature batches are never persisted here.
use crate::{identity, mutate_store, now_ms, security, ApiError, ApiResult, AppState};
use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    routing::get,
    Json, Router,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

const BACKBONE: &str = "3ba07f12712fa58fd6b3d661f9909c9e332c5005";
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Detector {
    format: String,
    backbone_revision: String,
    labels: Vec<String>,
    prototypes: Vec<String>,
    counts: Vec<u32>,
    threshold: f32,
    margin: f32,
}
fn invalid() -> ApiError {
    ApiError(StatusCode::UNPROCESSABLE_ENTITY, "Invalid detector. Use 1–4 classes, background, and normalized 1000-value prototypes from the pinned backbone".into())
}
impl Detector {
    fn validate(&self) -> ApiResult<()> {
        if self.format != "mesh-region-prototypes-v1"
            || self.backbone_revision != BACKBONE
            || !(2..=5).contains(&self.labels.len())
            || self.prototypes.len() != self.labels.len()
            || self.counts.len() != self.labels.len()
            || self.counts.iter().any(|n| !(2..=1024).contains(n))
            || !self.labels.iter().any(|label| label == "__background__")
            || self.labels.iter().enumerate().any(|(i, label)| {
                label.trim().is_empty()
                    || label.len() > 40
                    || !label
                        .bytes()
                        .all(|c| c.is_ascii_alphanumeric() || b" _-".contains(&c))
                    || self.labels[..i].contains(label)
            })
            || !(0.0..=1.0).contains(&self.threshold)
            || !(0.0..=1.0).contains(&self.margin)
        {
            return Err(invalid());
        }
        for prototype in &self.prototypes {
            if prototype.len() != 5336 {
                return Err(invalid());
            }
            let bytes = STANDARD.decode(prototype).map_err(|_| invalid())?;
            if bytes.len() != 4000 {
                return Err(invalid());
            }
            let values: Vec<_> = bytes
                .as_chunks::<4>()
                .0
                .iter()
                .map(|chunk| f32::from_le_bytes(*chunk))
                .collect();
            if values.iter().any(|value| !value.is_finite())
                || (values.iter().map(|value| value * value).sum::<f32>() - 1.0).abs() > 0.002
            {
                return Err(invalid());
            }
        }
        Ok(())
    }
}
#[derive(Clone, Deserialize, Serialize)]
pub struct Deployment {
    id: Uuid,
    saved_at_ms: u64,
    name: String,
    model: Detector,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Save {
    expected_revision_id: Option<Uuid>,
    name: String,
    model: Detector,
}
fn unavailable() -> ApiError {
    ApiError(
        StatusCode::SERVICE_UNAVAILABLE,
        "Model storage unavailable".into(),
    )
}
fn conflict() -> ApiError {
    ApiError(
        StatusCode::CONFLICT,
        "The deployed detector changed. Reload before replacing it".into(),
    )
}
fn owner(store: &crate::Store, app: Uuid, user: &str) -> ApiResult<()> {
    security::application_access(store, app, user, true)?;
    if store
        .applications
        .get(&app)
        .is_none_or(|app| app.owner_id != user)
    {
        return Err(ApiError(
            StatusCode::FORBIDDEN,
            "Only the application owner can deploy or remove models".into(),
        ));
    }
    Ok(())
}
async fn read(
    State(state): State<AppState>,
    Path(app): Path<Uuid>,
    headers: HeaderMap,
) -> ApiResult<Json<Option<Deployment>>> {
    let user = identity(&headers)?;
    let store = state.store.lock().map_err(|_| unavailable())?;
    security::application_access(&store, app, &user, false)?;
    Ok(Json(store.vision_models.get(&app).cloned()))
}
async fn save(
    State(state): State<AppState>,
    Path(app): Path<Uuid>,
    headers: HeaderMap,
    Json(input): Json<Save>,
) -> ApiResult<Json<Deployment>> {
    let user = identity(&headers)?;
    input.model.validate()?;
    if input.name.trim().is_empty()
        || input.name.len() > 80
        || input.name.chars().any(char::is_control)
    {
        return Err(invalid());
    }
    let deployment = Deployment {
        id: Uuid::new_v4(),
        saved_at_ms: now_ms(),
        name: input.name,
        model: input.model,
    };
    mutate_store(&state, |store| {
        owner(store, app, &user)?;
        if store.vision_models.get(&app).map(|value| value.id) != input.expected_revision_id {
            return Err(conflict());
        }
        if !store.vision_models.contains_key(&app) && store.vision_models.len() >= 64 {
            return Err(ApiError(
                StatusCode::CONFLICT,
                "Coordinator model capacity reached".into(),
            ));
        }
        store.vision_models.insert(app, deployment.clone());
        Ok(())
    })
    .await?;
    Ok(Json(deployment))
}
async fn remove(
    State(state): State<AppState>,
    Path((app, revision)): Path<(Uuid, Uuid)>,
    headers: HeaderMap,
) -> ApiResult<StatusCode> {
    let user = identity(&headers)?;
    mutate_store(&state, |store| {
        owner(store, app, &user)?;
        if store.vision_models.get(&app).map(|value| value.id) != Some(revision) {
            return Err(conflict());
        }
        store.vision_models.remove(&app);
        Ok(())
    })
    .await?;
    Ok(StatusCode::NO_CONTENT)
}
pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/api/applications/:app/detector", get(read).post(save))
        .route(
            "/api/applications/:app/detector/:revision",
            axum::routing::delete(remove),
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn validates_bounded_portable_detector_heads() {
        let mut bytes = vec![0u8; 4000];
        bytes[..4].copy_from_slice(&1f32.to_le_bytes());
        let mut model = Detector {
            format: "mesh-region-prototypes-v1".into(),
            backbone_revision: BACKBONE.into(),
            labels: vec!["__background__".into(), "cat".into()],
            prototypes: vec![STANDARD.encode(&bytes); 2],
            counts: vec![2, 3],
            threshold: 0.65,
            margin: 0.05,
        };
        model.validate().unwrap();
        model.labels[1] = "__background__".into();
        assert!(model.validate().is_err());
        model.labels[1] = "cat".into();
        bytes[..4].copy_from_slice(&f32::NAN.to_le_bytes());
        model.prototypes[0] = STANDARD.encode(&bytes);
        assert!(model.validate().is_err());
        bytes[..4].copy_from_slice(&0f32.to_le_bytes());
        model.prototypes[0] = STANDARD.encode(&bytes);
        assert!(model.validate().is_err());
    }
}
