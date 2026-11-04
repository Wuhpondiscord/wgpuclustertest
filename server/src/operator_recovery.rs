//! The operator pins the public recipient through deployment/Space configuration.
//! HTTP callers can only download already-encrypted bytes, never select a key.
use crate::{ApiError, ApiResult, AppState};
use axum::{
    extract::State,
    http::{HeaderMap, HeaderValue, StatusCode},
    response::IntoResponse,
};
use mesh_coordinator::key_recovery::{self, KeyBackup, StorageLocation};

pub struct Export {
    recipient: String,
    ciphertext: Vec<u8>,
}

pub fn prepare(state: &AppState, recipient: Option<&str>) -> Result<Option<Export>, &'static str> {
    let Some(recipient) = recipient.map(str::trim).filter(|s| !s.is_empty()) else {
        return Ok(None);
    };
    let auth_key = state
        .durability
        .lock()
        .map_err(|_| "Recovery configuration unavailable")?
        .accounts
        .key_for_operator_backup();
    let storage = match state.github.as_deref() {
        Some(config) => StorageLocation {
            repository: Some(config.repo.clone()),
            branch: Some(config.branch.clone()),
            path: config.path.clone(),
        },
        None => StorageLocation {
            repository: None,
            branch: None,
            path: "local".into(),
        },
    };
    let backup = KeyBackup::new(
        &state.snapshot.key_for_operator_backup(),
        auth_key.as_ref(),
        storage,
        crate::now(),
    );
    Ok(Some(Export {
        recipient: recipient.to_owned(),
        ciphertext: key_recovery::encrypt(recipient, &backup)?,
    }))
}

pub async fn download(State(state): State<AppState>) -> ApiResult<impl IntoResponse> {
    let export = state.key_backup.as_ref().ok_or(ApiError(
        StatusCode::NOT_FOUND,
        "Operator key recovery is not configured".into(),
    ))?;
    let mut headers = HeaderMap::new();
    headers.insert(
        "content-type",
        HeaderValue::from_static("application/octet-stream"),
    );
    headers.insert(
        "content-disposition",
        HeaderValue::from_static("attachment; filename=mesh-operator-keys.age"),
    );
    headers.insert("cache-control", HeaderValue::from_static("no-store"));
    headers.insert(
        "x-content-type-options",
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        "x-mesh-recovery-recipient",
        HeaderValue::from_str(&export.recipient).map_err(|_| {
            ApiError(
                StatusCode::SERVICE_UNAVAILABLE,
                "Recovery configuration unavailable".into(),
            )
        })?,
    );
    Ok((headers, export.ciphertext.clone()))
}
