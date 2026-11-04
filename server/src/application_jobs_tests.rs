use super::*;
use axum::{body::Body, http::Request};
use serde_json::{json, Value};
use tower::ServiceExt;

async fn call(
    router: &Router,
    method: &str,
    path: &str,
    credential: &str,
    data: Value,
    idem: Option<&str>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder()
        .method(method)
        .uri(path)
        .header("content-type", "application/json");
    if credential.starts_with("mak_") {
        builder = builder.header("authorization", format!("Bearer {credential}"));
    } else if !credential.is_empty() {
        builder = builder.header("x-mesh-identity", credential);
    }
    if let Some(key) = idem {
        builder = builder.header("idempotency-key", key);
    }
    let response = router
        .clone()
        .oneshot(
            builder
                .body(if data.is_null() {
                    Body::empty()
                } else {
                    Body::from(data.to_string())
                })
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 512 * 1024).await.unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}
struct Fixture {
    state: AppState,
    router: Router,
    session: String,
    cluster: String,
    app: String,
    nodes: Vec<String>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(self.state.data_file.as_path());
    }
}
impl Fixture {
    async fn new() -> Self {
        let state = AppState::for_test();
        let router = crate::app(state.clone());
        let (status, account) = call(
            &router,
            "POST",
            "/api/accounts",
            &"a".repeat(64),
            json!({"username":"runtime-owner"}),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{account}");
        let session = account["session"].as_str().unwrap().to_owned();
        assert_eq!(
            call(
                &router,
                "POST",
                "/api/account/activate",
                &session,
                Value::Null,
                None
            )
            .await
            .0,
            StatusCode::NO_CONTENT
        );
        let (_, cluster) = call(
            &router,
            "POST",
            "/api/clusters",
            &session,
            json!({"name":"runtime pool"}),
            None,
        )
        .await;
        let cluster = cluster["id"].as_str().unwrap().to_owned();
        let (_, app) = call(
            &router,
            "POST",
            &format!("/api/clusters/{cluster}/applications"),
            &session,
            json!({"name":"runtime","slug":"runtime"}),
            None,
        )
        .await;
        let app = app["id"].as_str().unwrap().to_owned();
        let mut nodes = Vec::new();
        for _ in 0..3 {
            let (_, node) = call(
                &router,
                "POST",
                &format!("/api/clusters/{cluster}/nodes"),
                &session,
                json!({"name":"worker"}),
                None,
            )
            .await;
            let node = node["node"]["id"].as_str().unwrap().to_owned();
            state
                .store
                .lock()
                .unwrap()
                .nodes
                .get_mut(&Uuid::parse_str(&node).unwrap())
                .unwrap()
                .online = true;
            nodes.push(node);
        }
        Self {
            state,
            router,
            session,
            cluster,
            app,
            nodes,
        }
    }
    async fn submit(&self, input: Value) -> Value {
        let (status, job) = call(
            &self.router,
            "POST",
            &format!("/api/applications/{}/jobs", self.app),
            &self.session,
            input,
            None,
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{job}");
        job
    }
    async fn claim(&self, node: usize, inference: bool) -> Value {
        let (status, work) = call(
            &self.router,
            "POST",
            &format!("/api/clusters/{}/application-work/claim", self.cluster),
            &self.session,
            json!({"node_id":self.nodes[node],"inference":inference}),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{work}");
        work
    }
    async fn commit(&self, work: &Value, values: Value, shape: Value) -> (StatusCode, Value) {
        call(&self.router,"POST",&format!("/api/application-work/{}/commit",work["job_id"].as_str().unwrap()),&self.session,json!({"lease":work["lease"],"output":{"values":values,"shape":shape},"runtime":{"backend":"cpu","compute_ms":1.,"load_ms":0.,"fallback_reason":null}}),None).await
    }
}
fn numeric() -> Value {
    json!({"operation":"vector_add","a":[1.,2.,3.],"b":[4.,5.,6.]})
}
fn embedding() -> Value {
    json!({"operation":"embedding","model":"minilm-l6-v2","texts":["hello"]})
}
fn unit(index: usize) -> Value {
    let mut values = vec![0.; 384];
    values[index] = 1.;
    json!(values)
}

#[tokio::test]
async fn cluster_members_and_viewers_cannot_work_and_revoked_operators_are_fenced() {
    let f = Fixture::new().await;
    let raw = "b".repeat(64);
    let (_, invite) = call(
        &f.router,
        "POST",
        &format!("/api/clusters/{}/invites", f.cluster),
        &f.session,
        Value::Null,
        None,
    )
    .await;
    call(
        &f.router,
        "POST",
        "/api/invites/join",
        &raw,
        json!({"token":invite["token"]}),
        None,
    )
    .await;
    let (_, account) = call(
        &f.router,
        "POST",
        "/api/accounts",
        &raw,
        json!({"username":"runtime-member"}),
        None,
    )
    .await;
    let session = account["session"].as_str().unwrap();
    call(
        &f.router,
        "POST",
        "/api/account/activate",
        session,
        Value::Null,
        None,
    )
    .await;
    let (_, node) = call(
        &f.router,
        "POST",
        &format!("/api/clusters/{}/nodes", f.cluster),
        session,
        json!({"name":"member node"}),
        None,
    )
    .await;
    let node = node["node"]["id"].as_str().unwrap();
    f.state
        .store
        .lock()
        .unwrap()
        .nodes
        .get_mut(&Uuid::parse_str(node).unwrap())
        .unwrap()
        .online = true;
    let jobs = format!("/api/applications/{}/jobs", f.app);
    let claim_path = format!("/api/clusters/{}/application-work/claim", f.cluster);
    let claim_body = json!({"node_id":node,"inference":true});
    assert_eq!(
        call(&f.router, "GET", &jobs, session, Value::Null, None)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    f.submit(numeric()).await;
    assert!(call(
        &f.router,
        "POST",
        &claim_path,
        session,
        claim_body.clone(),
        None
    )
    .await
    .1
    .is_null());
    let grants = format!("/api/applications/{}/members", f.app);
    call(
        &f.router,
        "POST",
        &grants,
        &f.session,
        json!({"username":"runtime-member","role":"viewer"}),
        None,
    )
    .await;
    assert_eq!(
        call(&f.router, "GET", &jobs, session, Value::Null, None)
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(
        call(&f.router, "POST", &jobs, session, numeric(), None)
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert!(call(
        &f.router,
        "POST",
        &claim_path,
        session,
        claim_body.clone(),
        None
    )
    .await
    .1
    .is_null());
    call(
        &f.router,
        "POST",
        &grants,
        &f.session,
        json!({"username":"runtime-member","role":"operator"}),
        None,
    )
    .await;
    let (_, work) = call(&f.router, "POST", &claim_path, session, claim_body, None).await;
    assert!(work["lease"].is_object());
    assert_eq!(
        call(
            &f.router,
            "DELETE",
            &format!("{grants}/runtime-member"),
            &f.session,
            Value::Null,
            None
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    let next = f.claim(0, false).await;
    assert_eq!(next["job_id"], work["job_id"]);
    assert_ne!(next["lease"]["attempt_id"], work["lease"]["attempt_id"]);
    assert_eq!(
        call(
            &f.router,
            "POST",
            &format!(
                "/api/application-work/{}/renew",
                work["job_id"].as_str().unwrap()
            ),
            session,
            json!({"lease":work["lease"]}),
            None
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn scoped_keys_are_hashed_isolated_read_only_and_revocable() {
    let f = Fixture::new().await;
    let path = format!("/api/applications/{}/keys", f.app);
    let (_, issued) = call(
        &f.router,
        "POST",
        &path,
        &f.session,
        json!({"label":"sdk","read_only":true}),
        None,
    )
    .await;
    let key = issued["key"].as_str().unwrap();
    assert!(key.starts_with("mak_"));
    assert_eq!(key.len(), 68);
    let serialized = serde_json::to_string(&*f.state.store.lock().unwrap()).unwrap();
    assert!(!serialized.contains(key));
    let jobs = format!("/api/applications/{}/jobs", f.app);
    assert_eq!(
        call(&f.router, "GET", &jobs, key, Value::Null, None)
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(
        call(&f.router, "POST", &jobs, key, numeric(), None).await.0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            &f.router,
            "GET",
            &format!("/api/applications/{}/jobs", Uuid::new_v4()),
            key,
            Value::Null,
            None
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(&f.router, "GET", &path, key, Value::Null, None)
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    let (_, list) = call(&f.router, "GET", &path, &f.session, Value::Null, None).await;
    assert!(!list.to_string().contains("hash"));
    assert!(!list.to_string().contains(key));
    assert_eq!(
        call(
            &f.router,
            "DELETE",
            &format!("{path}/{}", issued["metadata"]["id"].as_str().unwrap()),
            &f.session,
            Value::Null,
            None
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        call(&f.router, "GET", &jobs, key, Value::Null, None)
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
}
#[tokio::test]
async fn idempotency_caps_body_limits_and_private_routes() {
    let f = Fixture::new().await;
    let path = format!("/api/applications/{}/jobs", f.app);
    let (_, first) = call(
        &f.router,
        "POST",
        &path,
        &f.session,
        numeric(),
        Some("unique-request"),
    )
    .await;
    let (_, second) = call(
        &f.router,
        "POST",
        &path,
        &f.session,
        numeric(),
        Some("unique-request"),
    )
    .await;
    assert_eq!(first["id"], second["id"]);
    assert_eq!(
        call(
            &f.router,
            "POST",
            &path,
            &f.session,
            json!({"operation":"vector_add","a":[1],"b":[2]}),
            Some("unique-request")
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        call(&f.router, "GET", &path, &"b".repeat(64), Value::Null, None)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(
            &f.router,
            "POST",
            &path,
            &f.session,
            json!({"operation":"vector_add","a":vec![1.;10000],"b":vec![1.;10000]}),
            None
        )
        .await
        .0,
        StatusCode::PAYLOAD_TOO_LARGE
    );
    for _ in 1..MAX_JOBS {
        f.submit(numeric()).await;
    }
    assert_eq!(
        call(&f.router, "POST", &path, &f.session, numeric(), None)
            .await
            .0,
        StatusCode::CONFLICT
    );
    // Cluster-wide demo APIs never include app inputs or outputs.
    let (_, demos) = call(
        &f.router,
        "GET",
        &format!("/api/clusters/{}/jobs", f.cluster),
        &f.session,
        Value::Null,
        None,
    )
    .await;
    assert_eq!(demos, json!([]));
}
#[tokio::test]
async fn numerical_commit_is_fenced_validated_and_idempotent_without_claim_writes() {
    let f = Fixture::new().await;
    f.submit(numeric()).await;
    let before = std::fs::read(f.state.data_file.as_path()).unwrap();
    let old = f.claim(0, false).await;
    assert_eq!(f.claim(0, false).await["lease"], old["lease"]);
    let renew = format!(
        "/api/application-work/{}/renew",
        old["job_id"].as_str().unwrap()
    );
    assert_eq!(
        call(
            &f.router,
            "POST",
            &renew,
            &f.session,
            json!({"lease":old["lease"]}),
            None
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(std::fs::read(f.state.data_file.as_path()).unwrap(), before);
    assert_eq!(
        f.commit(&old, json!([5., 7., 99.]), json!([3])).await.0,
        StatusCode::UNPROCESSABLE_ENTITY
    );
    f.state
        .store
        .lock()
        .unwrap()
        .recovery
        .revoke_worker_leases(&f.nodes[0], now_ms());
    let new = f.claim(1, false).await;
    assert_eq!(
        f.commit(&old, json!([5., 7., 9.]), json!([3])).await.0,
        StatusCode::CONFLICT
    );
    let (status, accepted) = f.commit(&new, json!([5., 7., 9.]), json!([3])).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(accepted["status"], "completed");
    assert_eq!(
        f.commit(&new, json!([5., 7., 9.]), json!([3])).await.0,
        StatusCode::OK
    );
    assert_eq!(
        f.commit(&old, json!([5., 7., 9.]), json!([3])).await.0,
        StatusCode::CONFLICT
    );
}
#[tokio::test]
async fn inference_requires_opt_in_distinct_nodes_and_agreement() {
    let f = Fixture::new().await;
    f.submit(embedding()).await;
    assert!(f.claim(0, false).await.is_null());
    let a = f.claim(0, true).await;
    let (_, accepted) = f.commit(&a, unit(0), json!([1, 384])).await;
    assert_eq!(accepted["status"], "awaiting_verification");
    assert!(accepted["output"].is_null());
    assert!(f.claim(0, true).await.is_null());
    let b = f.claim(1, true).await;
    let (_, accepted) = f.commit(&b, unit(0), json!([1, 384])).await;
    assert_eq!(accepted["status"], "completed");
    assert_eq!(accepted["verification"], "two_node_agreement");
    f.submit(embedding()).await;
    let a = f.claim(0, true).await;
    f.commit(&a, unit(0), json!([1, 384])).await;
    let b = f.claim(1, true).await;
    let (_, rejected) = f.commit(&b, unit(1), json!([1, 384])).await;
    assert_eq!(rejected["status"], "failed");
    assert!(rejected["output"].is_null());
}
#[tokio::test]
async fn releases_deletion_revocation_and_failed_persistence_do_not_publish_results() {
    let mut f = Fixture::new().await;
    let job = f.submit(numeric()).await;
    let first = f.claim(0, false).await;
    let path = format!(
        "/api/application-work/{}/release",
        job["id"].as_str().unwrap()
    );
    assert_eq!(
        call(
            &f.router,
            "POST",
            &path,
            &f.session,
            json!({"lease":first["lease"],"error":"device failed"}),
            None
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    let second = f.claim(1, false).await;
    assert_ne!(first["lease"]["attempt_id"], second["lease"]["attempt_id"]);
    let real_path = f.state.data_file.clone();
    let blocker = std::env::temp_dir().join(format!("mesh-job-blocker-{}", Uuid::new_v4()));
    std::fs::write(&blocker, b"file blocks directory creation").unwrap();
    f.state.data_file = std::sync::Arc::new(blocker.join("state.json"));
    f.router = crate::app(f.state.clone());
    assert!(!f
        .commit(&second, json!([5., 7., 9.]), json!([3]))
        .await
        .0
        .is_success());
    assert!(f
        .state
        .store
        .lock()
        .unwrap()
        .application_jobs
        .values()
        .next()
        .unwrap()
        .receipts
        .is_empty());
    let _ = std::fs::remove_file(&blocker);
    f.state.data_file = real_path;
    f.router = crate::app(f.state.clone());
    assert_eq!(
        call(
            &f.router,
            "DELETE",
            &format!(
                "/api/applications/{}/jobs/{}",
                f.app,
                job["id"].as_str().unwrap()
            ),
            &f.session,
            Value::Null,
            None
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        f.commit(&second, json!([5., 7., 9.]), json!([3])).await.0,
        StatusCode::NOT_FOUND
    );
    f.submit(numeric()).await;
    call(
        &f.router,
        "POST",
        &format!("/api/applications/{}/keys", f.app),
        &f.session,
        json!({"label":"sdk"}),
        None,
    )
    .await;
    call(
        &f.router,
        "DELETE",
        &format!("/api/applications/{}", f.app),
        &f.session,
        Value::Null,
        None,
    )
    .await;
    let store = f.state.store.lock().unwrap();
    assert!(store.application_jobs.is_empty());
    assert!(store.application_keys.is_empty());
}
#[tokio::test]
async fn browser_cors_and_result_body_limit_are_enforced_at_router() {
    let f = Fixture::new().await;
    let response = f
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("OPTIONS")
                .uri(format!("/api/applications/{}/inference", f.app))
                .header("origin", "https://developer.example")
                .header("access-control-request-method", "POST")
                .header(
                    "access-control-request-headers",
                    "authorization,content-type,idempotency-key",
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.headers()["access-control-allow-origin"], "*");
    assert!(!response
        .headers()
        .contains_key("access-control-allow-credentials"));
    assert!(!response.headers()["access-control-allow-headers"]
        .to_str()
        .unwrap()
        .contains("x-mesh-identity"));
    let job = f.submit(numeric()).await;
    let response = f
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!(
                    "/api/application-work/{}/commit",
                    job["id"].as_str().unwrap()
                ))
                .header("x-mesh-identity", &f.session)
                .body(Body::from(vec![b' '; MAX_COMMIT_BYTES + 1]))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
}
