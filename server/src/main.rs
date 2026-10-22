use axum::{extract::State, http::StatusCode, routing::{get, post}, Json, Router};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, net::SocketAddr, sync::{Arc, Mutex}};
use tower_http::services::ServeDir;
use uuid::Uuid;

#[derive(Clone, Serialize)]
struct Cluster { id: Uuid, name: String, invite_code: String, members: u32 }
#[derive(Clone, Serialize)]
struct Contribution { user_id: String, cluster_id: Uuid, validated_work_units: f64, credit_minutes: f64 }
#[derive(Default)]
struct Store { clusters: HashMap<Uuid, Cluster>, contributions: Vec<Contribution> }
type AppState = Arc<Mutex<Store>>;
#[derive(Deserialize)]
struct CreateCluster { name: String }
#[derive(Deserialize)]
struct JoinCluster { invite_code: String }
#[derive(Deserialize)]
struct RecordWork { user_id: String, cluster_id: Uuid, validated_work_units: f64 }

fn valid_cluster_name(name: &str) -> Option<String> {
    let name = name.trim();
    (!name.is_empty() && name.len() <= 80).then(|| name.to_owned())
}

fn credits_for_work(units: f64) -> Option<f64> {
    (units.is_finite() && units > 0.0).then_some(units)
}

async fn health() -> &'static str { "mesh coordinator ready" }
async fn list_clusters(State(state): State<AppState>) -> Json<Vec<Cluster>> {
    Json(state.lock().expect("store mutex poisoned").clusters.values().cloned().collect())
}
async fn create_cluster(State(state): State<AppState>, Json(input): Json<CreateCluster>) -> Result<(StatusCode, Json<Cluster>), (StatusCode, String)> {
    let name = valid_cluster_name(&input.name).ok_or((StatusCode::BAD_REQUEST, "name must be 1–80 characters".into()))?;
    let cluster = Cluster { id: Uuid::new_v4(), name, invite_code: Uuid::new_v4().simple().to_string()[..10].into(), members: 1 };
    state.lock().map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "store unavailable".into()))?.clusters.insert(cluster.id, cluster.clone());
    Ok((StatusCode::CREATED, Json(cluster)))
}
async fn join_cluster(State(state): State<AppState>, Json(input): Json<JoinCluster>) -> Result<Json<Cluster>, (StatusCode, String)> {
    let mut store = state.lock().map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "store unavailable".into()))?;
    let cluster = store.clusters.values_mut().find(|c| c.invite_code == input.invite_code).ok_or((StatusCode::NOT_FOUND, "invite not found".into()))?;
    cluster.members += 1;
    Ok(Json(cluster.clone()))
}
async fn record_work(State(state): State<AppState>, Json(input): Json<RecordWork>) -> Result<(StatusCode, Json<Contribution>), (StatusCode, String)> {
    let credit_minutes = credits_for_work(input.validated_work_units).ok_or((StatusCode::BAD_REQUEST, "validated work must be a positive finite number".into()))?;
    let mut store = state.lock().map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "store unavailable".into()))?;
    if !store.clusters.contains_key(&input.cluster_id) { return Err((StatusCode::NOT_FOUND, "cluster not found".into())); }
    // Placeholder conversion: production should distribute a pool's available service credits
    // in proportion to independently verified work, with anti-cheat and replay protection.
    let contribution = Contribution { user_id: input.user_id, cluster_id: input.cluster_id, validated_work_units: input.validated_work_units, credit_minutes };
    store.contributions.push(contribution.clone());
    Ok((StatusCode::CREATED, Json(contribution)))
}

#[tokio::main]
async fn main() {
    let state = Arc::new(Mutex::new(Store::default()));
    let app = Router::new().route("/health", get(health)).route("/api/clusters", get(list_clusters).post(create_cluster)).route("/api/clusters/join", post(join_cluster)).route("/api/contributions", post(record_work)).with_state(state).fallback_service(ServeDir::new("dist").append_index_html_on_directories(true));
    let port = std::env::var("PORT").ok().and_then(|p| p.parse::<u16>().ok()).unwrap_or(7860);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr).await.expect("bind coordinator");
    println!("Mesh coordinator listening on http://{addr}");
    axum::serve(listener, app).await.expect("serve coordinator");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cluster_name_is_trimmed_and_bounded() {
        assert_eq!(valid_cluster_name("  Lab  ").as_deref(), Some("Lab"));
        assert_eq!(valid_cluster_name("  "), None);
        assert_eq!(valid_cluster_name(&"x".repeat(81)), None);
    }

    #[test]
    fn work_credits_only_accept_positive_finite_values() {
        for invalid in [0.0, -1.0, f64::NAN, f64::INFINITY] { assert_eq!(credits_for_work(invalid), None); }
        assert_eq!(credits_for_work(3.5), Some(3.5));
    }
}
