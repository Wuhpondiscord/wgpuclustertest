use super::*;
use axum::{body::Body, http::Request};
use serde_json::{json, Value};
use tower::ServiceExt;

fn workflow(expected: Value) -> Value {
    json!({
        "expected_revision_id": expected,
        "source": {"kind":"upload"},
        "manifest": {
            "version":1,
            "name":"Smoke workflow",
            "jobs":[{"name":"first","input":numeric()},{"name":"second","input":numeric()}]
        }
    })
}

#[tokio::test]
async fn workflow_runs_atomically_and_replays_the_pinned_revision() {
    let f = Fixture::new().await;
    let base = format!("/api/applications/{}", f.app);
    let path = format!("{base}/deployment");
    let run = format!("{path}/run");
    let (status, first) = call(
        &f.router,
        "POST",
        &path,
        &f.session,
        workflow(Value::Null),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{first}");
    let (status, _) = call(
        &f.router,
        "POST",
        &path,
        &f.session,
        workflow(Value::Null),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    let bad = json!({"manifest":{"version":1,"name":"Invalid","jobs":[{"name":"script","input":{"operation":"javascript","script":"anything"}}]},"source":{"kind":"upload"},"expected_revision_id":first["id"]});
    assert_eq!(
        call(&f.router, "POST", &path, &f.session, bad, None)
            .await
            .0,
        StatusCode::UNPROCESSABLE_ENTITY
    );
    assert_eq!(
        call(&f.router, "GET", &path, &f.session, Value::Null, None)
            .await
            .1["id"],
        first["id"]
    );

    for _ in 0..(MAX_JOBS - 1) {
        f.submit(numeric()).await;
    }
    let input = json!({"revision_id": first["id"]});
    let persisted_before = std::fs::read(&*f.state.data_file).unwrap();
    assert_eq!(
        call(
            &f.router,
            "POST",
            &run,
            &f.session,
            input.clone(),
            Some("run-a")
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        std::fs::read(&*f.state.data_file).unwrap(),
        persisted_before
    );
    let jobs = f.state.store.lock().unwrap().application_jobs.len();
    assert_eq!(jobs, MAX_JOBS - 1);
    let id = f
        .state
        .store
        .lock()
        .unwrap()
        .application_jobs
        .keys()
        .next()
        .copied()
        .unwrap();
    assert_eq!(
        call(
            &f.router,
            "DELETE",
            &format!("{base}/jobs/{id}"),
            &f.session,
            Value::Null,
            None
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );

    let (status, accepted) = call(
        &f.router,
        "POST",
        &run,
        &f.session,
        input.clone(),
        Some("run-a"),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{accepted}");
    assert_eq!(accepted.as_array().unwrap().len(), 2);
    assert_eq!(accepted[0]["deployment"]["name"], "first");
    assert_eq!(accepted[1]["deployment"]["revision_id"], first["id"]);
    assert_eq!(
        f.state.store.lock().unwrap().application_jobs.len(),
        MAX_JOBS
    );
    let (status, replacement) = call(
        &f.router,
        "POST",
        &path,
        &f.session,
        workflow(first["id"].clone()),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{replacement}");
    assert_ne!(replacement["id"], first["id"]);
    let (status, repeated) = call(
        &f.router,
        "POST",
        &run,
        &f.session,
        input.clone(),
        Some("run-a"),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(repeated[0]["id"], accepted[0]["id"]);
    assert_eq!(repeated[1]["id"], accepted[1]["id"]);
    assert_eq!(
        call(&f.router, "POST", &run, &f.session, input, Some("run-b"))
            .await
            .0,
        StatusCode::CONFLICT
    );
    let removed = format!("{path}/{}", first["id"].as_str().unwrap());
    assert_eq!(
        call(&f.router, "DELETE", &removed, &f.session, Value::Null, None)
            .await
            .0,
        StatusCode::CONFLICT
    );
}

#[tokio::test]
async fn deployments_use_app_permissions_and_are_removed_with_the_app() {
    let f = Fixture::new().await;
    let base = format!("/api/applications/{}", f.app);
    let path = format!("{base}/deployment");
    let run = format!("{path}/run");
    let (_, release) = call(
        &f.router,
        "POST",
        &path,
        &f.session,
        workflow(Value::Null),
        None,
    )
    .await;
    assert_eq!(
        call(&f.router, "GET", &path, &"b".repeat(64), Value::Null, None)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    for read_only in [true, false] {
        let (_, issued) = call(
            &f.router,
            "POST",
            &format!("{base}/keys"),
            &f.session,
            json!({"label":"workflow","read_only":read_only}),
            None,
        )
        .await;
        let key = issued["key"].as_str().unwrap();
        assert_eq!(
            call(&f.router, "GET", &path, key, Value::Null, None)
                .await
                .1["id"],
            release["id"]
        );
        assert_eq!(
            call(
                &f.router,
                "POST",
                &path,
                key,
                workflow(release["id"].clone()),
                None
            )
            .await
            .0,
            StatusCode::UNAUTHORIZED
        );
        let (status, _) = call(
            &f.router,
            "POST",
            &run,
            key,
            json!({"revision_id":release["id"]}),
            Some("key-run"),
        )
        .await;
        assert_eq!(
            status,
            if read_only {
                StatusCode::FORBIDDEN
            } else {
                StatusCode::ACCEPTED
            }
        );
    }
    assert_eq!(
        call(&f.router, "DELETE", &base, &f.session, Value::Null, None)
            .await
            .0,
        StatusCode::NO_CONTENT
    );
    let store = f.state.store.lock().unwrap();
    assert!(store.application_deployments.is_empty());
    assert!(store.application_jobs.is_empty());
}

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

#[tokio::test]
async fn ml_link_is_work_only_and_image_results_do_not_write_the_state_file() {
    let f = Fixture::new().await;
    let app = format!("/api/applications/{}", f.app);
    let (status, link) = call(
        &f.router,
        "POST",
        &format!("{app}/contributor-invites?ml_only=true"),
        &f.session,
        Value::Null,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let token = link["token"].as_str().unwrap();
    let guest_id = Uuid::new_v4();
    let browser_key = Uuid::new_v4();
    let guest_call = |method: &'static str, path: String, data: Value, browser: Uuid| {
        let router = f.router.clone();
        async move {
            let response = router
                .oneshot(
                    Request::builder()
                        .method(method)
                        .uri(path)
                        .header("content-type", "application/json")
                        .header("x-mesh-ml-invite", token)
                        .header("x-mesh-ml-browser", browser.to_string())
                        .body(Body::from(data.to_string()))
                        .unwrap(),
                )
                .await
                .unwrap();
            let status = response.status();
            let bytes = to_bytes(response.into_body(), 128 * 1024).await.unwrap();
            (
                status,
                serde_json::from_slice::<Value>(&bytes).unwrap_or(Value::Null),
            )
        }
    };
    assert_eq!(
        guest_call("GET", "/api/ml-invite".into(), Value::Null, browser_key)
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(
        guest_call(
            "GET",
            format!("{app}/image-batches"),
            Value::Null,
            browser_key
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(
            &f.router,
            "POST",
            "/api/invites/join",
            &"b".repeat(64),
            json!({"token":token}),
            None
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    let snapshot = std::fs::read(f.state.data_file.as_ref()).unwrap();
    let work = format!("{app}/image-work");
    let idle = guest_call(
        "POST",
        format!("{work}/claim"),
        json!({"worker_id":Uuid::new_v4()}),
        browser_key,
    );
    tokio::pin!(idle);
    assert!(futures_util::poll!(&mut idle).is_pending());
    let image = "A".repeat(96 * 96 * 4);
    let (status, run) = call(
        &f.router,
        "POST",
        &format!("{app}/image-batches"),
        &f.session,
        json!({"images":[image, image]}),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{run}");
    assert_eq!(idle.await, (StatusCode::OK, json!({"ready":true})));
    let claim = || {
        guest_call(
            "POST",
            format!("{work}/claim"),
            json!({"worker_id":guest_id}),
            browser_key,
        )
    };
    let (status, first) = claim().await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first["run_id"], run["id"]);
    assert_eq!(first["index"], 0);
    let id = run["id"].as_str().unwrap();
    let commit_path = format!("{work}/{id}/commit");
    let result = json!({"worker_id":guest_id,"lease":first["lease"],"index":first["index"],
        "predictions":[{"class_id":292,"score":0.6},{"class_id":282,"score":0.2},{"class_id":1,"score":0.1}],
        "backend":"wasm","load_ms":0,"compute_ms":1});
    assert_eq!(
        guest_call("POST", commit_path.clone(), result.clone(), Uuid::new_v4())
            .await
            .0,
        StatusCode::CONFLICT
    );
    for _ in 0..2 {
        assert_eq!(
            guest_call("POST", commit_path.clone(), result.clone(), browser_key)
                .await
                .0,
            StatusCode::NO_CONTENT
        );
    }
    let (status, output) = call(
        &f.router,
        "GET",
        &format!("{app}/image-batches/{id}?results=true"),
        &f.session,
        Value::Null,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(output["completed"], 1);
    assert_eq!(output["results"][0]["worker_id"], guest_id.to_string());
    assert_eq!(snapshot, std::fs::read(f.state.data_file.as_ref()).unwrap());
    let (status, second) = claim().await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(second["index"], 1);
    let revoke = format!(
        "/api/clusters/{}/invites/{}",
        f.cluster,
        crate::secret_hash(token)
    );
    assert_eq!(
        call(&f.router, "DELETE", &revoke, &f.session, Value::Null, None)
            .await
            .0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        guest_call(
            "POST",
            format!("{work}/{id}/renew"),
            json!({"worker_id":guest_id,"index":1,"lease":second["lease"]}),
            browser_key
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
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
async fn project_link_grants_assigned_work_only_and_revocation_fences_the_worker() {
    let f = Fixture::new().await;
    let guest = "b".repeat(64);
    let app_path = format!("/api/applications/{}", f.app);
    let cluster_path = format!("/api/clusters/{}", f.cluster);
    let (status, invite) = call(
        &f.router,
        "POST",
        &format!("{app_path}/contributor-invites"),
        &f.session,
        Value::Null,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{invite}");
    let token = invite["token"].as_str().unwrap();
    let (status, preview) = call(
        &f.router,
        "GET",
        &format!("/api/invites/{token}"),
        "",
        Value::Null,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(preview["project_name"], "runtime");
    assert_eq!(
        call(
            &f.router,
            "POST",
            "/api/invites/join",
            &guest,
            json!({"token":token}),
            None
        )
        .await
        .0,
        StatusCode::OK
    );
    let (status, node) = call(
        &f.router,
        "POST",
        &format!("{cluster_path}/nodes"),
        &guest,
        json!({"name":"guest browser"}),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let node = node["node"]["id"].as_str().unwrap();
    f.state
        .store
        .lock()
        .unwrap()
        .nodes
        .get_mut(&Uuid::parse_str(node).unwrap())
        .unwrap()
        .online = true;
    for path in [
        app_path.clone(),
        format!("{app_path}/jobs"),
        format!("{app_path}/deployment"),
    ] {
        assert_eq!(
            call(&f.router, "GET", &path, &guest, Value::Null, None)
                .await
                .0,
            StatusCode::NOT_FOUND
        );
    }
    assert_eq!(
        call(
            &f.router,
            "POST",
            &format!("{app_path}/jobs"),
            &guest,
            numeric(),
            None
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    let (_, other) = call(
        &f.router,
        "POST",
        &format!("{cluster_path}/applications"),
        &f.session,
        json!({"name":"other","slug":"other"}),
        None,
    )
    .await;
    let other_jobs = format!("/api/applications/{}/jobs", other["id"].as_str().unwrap());
    assert_eq!(
        call(&f.router, "POST", &other_jobs, &f.session, numeric(), None)
            .await
            .0,
        StatusCode::ACCEPTED
    );
    let claim_path = format!("{cluster_path}/application-work/claim");
    let claim = || {
        call(
            &f.router,
            "POST",
            &claim_path,
            &guest,
            json!({"node_id":node,"inference":false}),
            None,
        )
    };
    assert!(claim().await.1.is_null());
    let job = f.submit(numeric()).await;
    let (status, work) = claim().await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(work["job_id"], job["id"]);
    assert_eq!(work["input"]["a"], json!([1., 2., 3.]));
    let work_path = format!("/api/application-work/{}", job["id"].as_str().unwrap());
    let (status, committed) = call(&f.router, "POST", &format!("{work_path}/commit"), &guest, json!({"lease":work["lease"],"output":{"values":[5.,7.,9.],"shape":[3]},"runtime":{"backend":"cpu","compute_ms":1.,"load_ms":0.}}), None).await;
    assert_eq!(status, StatusCode::OK, "{committed}");
    assert_eq!(committed["status"], "completed");
    assert!(committed["output"].is_null());
    assert!(committed["input"].is_null());
    assert!(committed["deployment"].is_null());
    let receipt_path = format!("{work_path}/receipt");
    let (status, receipt) = call(
        &f.router,
        "POST",
        &receipt_path,
        &guest,
        json!({"lease":work["lease"]}),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(receipt["workers"].as_array().unwrap().len(), 1);
    assert_eq!(receipt["workers"][0]["node_id"], node);
    let mut wrong_lease = work["lease"].clone();
    wrong_lease["attempt_id"] = json!(0);
    assert_eq!(
        call(
            &f.router,
            "POST",
            &receipt_path,
            &guest,
            json!({"lease":wrong_lease}),
            None
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    let next = f.submit(numeric()).await;
    let (_, work) = claim().await;
    assert_eq!(work["job_id"], next["id"]);
    let revoke_path = format!("{cluster_path}/invites/{}", crate::secret_hash(token));
    assert_eq!(
        call(
            &f.router,
            "DELETE",
            &revoke_path,
            &f.session,
            Value::Null,
            None
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    let renew = format!(
        "/api/application-work/{}/renew",
        next["id"].as_str().unwrap()
    );
    assert_eq!(
        call(
            &f.router,
            "POST",
            &renew,
            &guest,
            json!({"lease":work["lease"]}),
            None
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert!(claim().await.1.is_null());
    let task = work["lease"]["task_id"].as_str().unwrap();
    assert!(f
        .state
        .store
        .lock()
        .unwrap()
        .recovery
        .active_lease(task, crate::now_ms())
        .is_none());
    assert_eq!(
        call(
            &f.router,
            "GET",
            &format!("{cluster_path}/nodes"),
            &guest,
            Value::Null,
            None
        )
        .await
        .0,
        StatusCode::OK
    );
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

#[tokio::test]
async fn a_delayed_failure_counter_cannot_override_a_completed_takeover() {
    let f = Fixture::new().await;
    let job = f.submit(numeric()).await;
    let id = Uuid::parse_str(job["id"].as_str().unwrap()).unwrap();
    for node in 0..2 {
        let attempt = f.claim(node, false).await;
        let response = call(
            &f.router,
            "POST",
            &format!("/api/application-work/{id}/release"),
            &f.session,
            json!({"lease":attempt["lease"],"error":"device failure"}),
            None,
        )
        .await;
        assert_eq!(response.0, StatusCode::NO_CONTENT);
    }
    // Pause the third failure between its runtime release and durable counter.
    let failed = f.claim(2, false).await;
    call(
        &f.router,
        "POST",
        &format!("/api/application-work/{id}/release"),
        &f.session,
        json!({"lease":failed["lease"]}),
        None,
    )
    .await;
    let takeover = f.claim(0, false).await;
    assert_eq!(
        f.commit(&takeover, json!([5., 7., 9.]), json!([3])).await.0,
        StatusCode::OK
    );
    let mut store = f.state.store.lock().unwrap();
    note_failure(&mut store, id, "late third failure").unwrap();
    let result = summary(&store.application_jobs[&id], &store);
    assert_eq!(result.status, "completed");
    assert_eq!(result.output.unwrap().values, vec![5., 7., 9.]);
    assert!(result.error.is_none());
}
