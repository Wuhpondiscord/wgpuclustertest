use super::*;
use axum::{
    body::{to_bytes, Body},
    http::Request,
    Router,
};
use std::sync::{Arc, Mutex};
use tower::ServiceExt;

fn test_state() -> AppState {
    let mut runtime = crate::DurabilityRuntime::default();
    runtime.accounts.encryption_key = Some([42; 32]);
    AppState {
        store: Arc::new(Mutex::new(Store::default())),
        mutation_lock: Arc::new(tokio::sync::Mutex::new(())),
        data_file: Arc::new(
            std::env::temp_dir().join(format!("mesh-accounts-{}.json", Uuid::new_v4())),
        ),
        snapshot: Arc::new(crate::snapshot::Codec::for_test()),
        key_backup: None,
        github: None,
        github_file_sha: Arc::new(Mutex::new(None)),
        node_connections: Arc::new(Mutex::new(HashMap::new())),
        durability: Arc::new(Mutex::new(runtime)),
        image_batches: Arc::new(crate::image_batches::Runtime::default()),
    }
}
async fn call(
    router: &Router,
    method: &str,
    path: &str,
    key: &str,
    body: serde_json::Value,
) -> (StatusCode, serde_json::Value) {
    let mut request = Request::builder()
        .method(method)
        .uri(path)
        .header("content-type", "application/json");
    if !key.is_empty() {
        request = request.header(IDENTITY_HEADER, key);
    }
    let response = router
        .clone()
        .oneshot(
            request
                .body(if body.is_null() {
                    Body::empty()
                } else {
                    Body::from(body.to_string())
                })
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.headers()["cache-control"], "no-store");
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    let data = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| serde_json::Value::String(String::from_utf8_lossy(&bytes).into()));
    (status, data)
}
async fn register(router: &Router, raw: &str, name: &str) -> serde_json::Value {
    let (status, data) = call(
        router,
        "POST",
        "/api/accounts",
        raw,
        serde_json::json!({"username":name}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{data}");
    assert!(key_format(data["key"].as_str().unwrap()));
    assert_eq!(
        call(
            router,
            "POST",
            "/api/account/activate",
            data["session"].as_str().unwrap(),
            serde_json::Value::Null
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    data
}
#[test]
fn random_keys_rfc_totp_and_authenticated_secret_storage() {
    let mut keys = HashSet::new();
    for _ in 0..256 {
        let key = new_key().unwrap();
        assert!(key_format(&key));
        assert!(keys.insert(key));
    }
    // RFC 6238 SHA1 vectors, truncated to the interoperable six-digit format.
    for (time, expected) in [
        (59, "287082"),
        (1_111_111_109, "081804"),
        (1_111_111_111, "050471"),
        (1_234_567_890, "005924"),
        (2_000_000_000, "279037"),
    ] {
        assert_eq!(totp(b"12345678901234567890", time / 30), expected);
    }
    let encrypted = seal(&[42; 32], "owner", b"authenticator secret").unwrap();
    assert_eq!(
        unseal(&[42; 32], "owner", &encrypted).unwrap(),
        b"authenticator secret"
    );
    assert!(unseal(&[43; 32], "owner", &encrypted).is_err());
    assert!(unseal(&[42; 32], "other", &encrypted).is_err());
    let mut tampered = encrypted.clone();
    tampered.ciphertext = BASE64.encode([0; 32]);
    assert!(unseal(&[42; 32], "owner", &tampered).is_err());
}
#[tokio::test]
async fn account_activation_is_recoverable_and_preserves_cluster_ownership() {
    let state = test_state();
    let router = crate::app(state.clone());
    let raw = "a".repeat(64);
    let (_, cluster) = call(
        &router,
        "POST",
        "/api/clusters",
        &raw,
        serde_json::json!({"name":"existing"}),
    )
    .await;
    let (_, lost) = call(
        &router,
        "POST",
        "/api/accounts",
        &raw,
        serde_json::json!({"username":"alice"}),
    )
    .await;
    // Lost registration response must leave the old anonymous credential usable.
    assert_eq!(
        call(
            &router,
            "GET",
            "/api/account",
            &raw,
            serde_json::Value::Null
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    let alice = register(&router, &raw, "alice").await;
    assert_ne!(lost["key"], alice["key"]);
    let session = alice["session"].as_str().unwrap();
    let (_, clusters) = call(
        &router,
        "GET",
        "/api/clusters",
        session,
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(clusters[0]["id"], cluster["id"]);
    assert_eq!(clusters[0]["role"], "owner");
    assert_eq!(
        call(
            &router,
            "GET",
            "/api/clusters",
            &raw,
            serde_json::Value::Null
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    // A saved root key is not an API session and cannot bypass future 2FA.
    assert_eq!(
        call(
            &router,
            "GET",
            "/api/account",
            alice["key"].as_str().unwrap(),
            serde_json::Value::Null
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    let request = Request::builder()
        .uri("/api/clusters")
        .header(IDENTITY_HEADER, "z".repeat(64))
        .header(USER_HEADER, secret_hash(&raw));
    let response = router
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let body = to_bytes(response.into_body(), 4096).await.unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
        serde_json::json!([])
    );
    state
        .durability
        .lock()
        .unwrap()
        .accounts
        .sessions
        .get_mut(&secret_hash(session))
        .unwrap()
        .expires = 0;
    let (status, expired) = call(
        &crate::app(state.clone()),
        "GET",
        "/api/clusters",
        session,
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(expired["code"], "account_session_required");
    state.durability.lock().unwrap().accounts.sessions.clear();
    assert_eq!(
        call(
            &crate::app(state.clone()),
            "GET",
            "/api/clusters",
            session,
            serde_json::Value::Null
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    let persisted = std::fs::read_to_string(state.data_file.as_path()).unwrap();
    assert!(!persisted.contains(alice["key"].as_str().unwrap()));
    assert!(!persisted.contains(session));
}
#[tokio::test]
async fn authenticator_replay_recovery_rotation_and_restart_are_enforced() {
    let state = test_state();
    let router = crate::app(state.clone());
    let alice = register(&router, &"a".repeat(64), "alice").await;
    let root = alice["key"].as_str().unwrap();
    let initial = alice["session"].as_str().unwrap();
    let (status, setup) = call(
        &router,
        "POST",
        "/api/account/totp/setup",
        initial,
        serde_json::json!({"key":root}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let secret = BASE32_NOPAD
        .decode(setup["secret"].as_str().unwrap().as_bytes())
        .unwrap();
    let code = totp(&secret, now() / 30);
    let (status, enabled) = call(
        &router,
        "POST",
        "/api/account/totp/enable",
        initial,
        serde_json::json!({"challenge":setup["challenge"],"code":code}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{enabled}");
    let credentials =
        |key: &str, code: &str| serde_json::json!({"username":"alice","key":key,"code":code});
    assert_eq!(
        call(
            &router,
            "POST",
            "/api/accounts/login",
            "",
            credentials(root, "")
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(
            &router,
            "POST",
            "/api/accounts/login",
            "",
            credentials(root, &code)
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    let recovery = enabled["recovery_codes"].as_array().unwrap();
    assert_eq!(recovery.len(), 8);
    let (status, logged_in) = call(
        &router,
        "POST",
        "/api/accounts/login",
        "",
        credentials(root, recovery[0].as_str().unwrap()),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        call(
            &router,
            "POST",
            "/api/accounts/login",
            "",
            credentials(root, recovery[0].as_str().unwrap())
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    let session = logged_in["session"].as_str().unwrap();
    let (status, rotation) = call(
        &router,
        "POST",
        "/api/account/key",
        session,
        serde_json::json!({"key":root,"code":recovery[1]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let replacement = rotation["key"].as_str().unwrap();
    assert_ne!(replacement, root);
    assert_eq!(
        call(
            &router,
            "POST",
            "/api/accounts/login",
            "",
            credentials(replacement, recovery[2].as_str().unwrap())
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    let (status, new_session) = call(
        &router,
        "POST",
        "/api/account/key/confirm",
        session,
        serde_json::json!({"key":replacement}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        call(
            &router,
            "POST",
            "/api/accounts/login",
            "",
            credentials(root, recovery[2].as_str().unwrap())
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(
            &router,
            "POST",
            "/api/accounts/login",
            "",
            credentials(replacement, recovery[2].as_str().unwrap())
        )
        .await
        .0,
        StatusCode::OK
    );
    let persisted = std::fs::read_to_string(state.data_file.as_path()).unwrap();
    for sensitive in [
        root,
        replacement,
        setup["secret"].as_str().unwrap(),
        recovery[3].as_str().unwrap(),
        new_session["session"].as_str().unwrap(),
    ] {
        assert!(!persisted.contains(sensitive));
    }
    // Restore persisted state and a fresh runtime: encrypted seed and used-code state survive.
    *state.store.lock().unwrap() = state.snapshot.decode(persisted.as_bytes()).unwrap().0;
    let runtime = Runtime {
        encryption_key: Some([42; 32]),
        ..Runtime::default()
    };
    state.durability.lock().unwrap().accounts = runtime;
    assert_eq!(
        call(
            &router,
            "POST",
            "/api/accounts/login",
            "",
            credentials(replacement, recovery[0].as_str().unwrap())
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(
            &router,
            "POST",
            "/api/accounts/login",
            "",
            credentials(replacement, recovery[3].as_str().unwrap())
        )
        .await
        .0,
        StatusCode::OK
    );
}
#[tokio::test]
async fn private_apps_require_explicit_grants_even_for_the_cluster_owner() {
    let state = test_state();
    let router = crate::app(state.clone());
    let raw_a = "a".repeat(64);
    let raw_b = "b".repeat(64);
    let (_, cluster) = call(
        &router,
        "POST",
        "/api/clusters",
        &raw_a,
        serde_json::json!({"name":"pool"}),
    )
    .await;
    let cluster = cluster["id"].as_str().unwrap();
    let (_, invite) = call(
        &router,
        "POST",
        &format!("/api/clusters/{cluster}/invites"),
        &raw_a,
        serde_json::Value::Null,
    )
    .await;
    call(
        &router,
        "POST",
        "/api/invites/join",
        &raw_b,
        serde_json::json!({"token":invite["token"]}),
    )
    .await;
    let a = register(&router, &raw_a, "alice").await;
    let b = register(&router, &raw_b, "bob").await;
    let a = a["session"].as_str().unwrap();
    let b = b["session"].as_str().unwrap();
    // Cluster member Bob creates an app; cluster owner Alice has no implicit access.
    let (status, app) = call(
        &router,
        "POST",
        &format!("/api/clusters/{cluster}/applications"),
        b,
        serde_json::json!({"slug":"private-app","name":"Secret project"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{app}");
    let id = app["id"].as_str().unwrap();
    for path in [
        format!("/api/applications/{id}"),
        "/api/applications/by-name/bob/private-app".into(),
    ] {
        assert_eq!(
            call(&router, "GET", &path, a, serde_json::Value::Null)
                .await
                .0,
            StatusCode::NOT_FOUND
        );
    }
    assert_eq!(
        call(
            &router,
            "GET",
            &format!("/api/clusters/{cluster}/applications"),
            a,
            serde_json::Value::Null
        )
        .await
        .1,
        serde_json::json!([])
    );
    assert_eq!(
        call(
            &router,
            "POST",
            &format!("/api/applications/{id}/members"),
            b,
            serde_json::json!({"username":"alice","role":"viewer"})
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        call(
            &router,
            "GET",
            &format!("/api/applications/{id}"),
            a,
            serde_json::Value::Null
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        call(
            &router,
            "POST",
            &format!("/api/applications/{id}/members"),
            a,
            serde_json::json!({"username":"alice","role":"owner"})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            &router,
            "DELETE",
            &format!("/api/applications/{id}"),
            a,
            serde_json::Value::Null
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    call(
        &router,
        "DELETE",
        &format!("/api/applications/{id}/members/alice"),
        b,
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(
        call(
            &router,
            "GET",
            &format!("/api/applications/{id}"),
            a,
            serde_json::Value::Null
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    call(
        &router,
        "DELETE",
        &format!("/api/clusters/{cluster}"),
        a,
        serde_json::Value::Null,
    )
    .await;
    assert!(state.store.lock().unwrap().applications.is_empty());
}
#[tokio::test]
async fn authentication_limits_replay_and_failed_persistence_fail_closed() {
    let state = test_state();
    let router = crate::app(state.clone());
    let alice = register(&router, &"a".repeat(64), "alice").await;
    let session = alice["session"].as_str().unwrap();
    let root = alice["key"].as_str().unwrap();
    let (_, setup) = call(
        &router,
        "POST",
        "/api/account/totp/setup",
        session,
        serde_json::json!({"key":root}),
    )
    .await;
    let code = totp(
        &BASE32_NOPAD
            .decode(setup["secret"].as_str().unwrap().as_bytes())
            .unwrap(),
        now() / 30,
    );
    call(
        &router,
        "POST",
        "/api/account/totp/enable",
        session,
        serde_json::json!({"challenge":setup["challenge"],"code":code}),
    )
    .await;
    for _ in 0..6 {
        assert_eq!(
            call(
                &router,
                "POST",
                "/api/accounts/login",
                "",
                serde_json::json!({"username":"alice","key":root,"code":"invalid"})
            )
            .await
            .0,
            StatusCode::UNAUTHORIZED
        );
    }
    assert_eq!(
        call(
            &router,
            "POST",
            "/api/accounts/login",
            "",
            serde_json::json!({"username":"alice","key":root,"code":"invalid"})
        )
        .await
        .0,
        StatusCode::TOO_MANY_REQUESTS
    );
    let blocked = test_state();
    let blocker =
        std::env::temp_dir().join(format!("mesh-account-write-blocker-{}", Uuid::new_v4()));
    std::fs::write(&blocker, b"block parent directory creation").unwrap();
    let blocked = AppState {
        data_file: Arc::new(blocker.join("state.json")),
        ..blocked
    };
    let router = crate::app(blocked.clone());
    assert!(call(
        &router,
        "POST",
        "/api/accounts",
        &"c".repeat(64),
        serde_json::json!({"username":"carol"})
    )
    .await
    .0
    .is_server_error());
    assert!(blocked.store.lock().unwrap().accounts.is_empty());
    let _ = std::fs::remove_file(blocker);
}

#[tokio::test]
async fn last_recovery_code_can_reset_the_lost_authenticator_but_normal_sessions_cannot() {
    let state = test_state();
    let router = crate::app(state.clone());
    let alice = register(&router, &"a".repeat(64), "alice").await;
    let key = alice["key"].as_str().unwrap();
    let session = alice["session"].as_str().unwrap();
    let (_, setup) = call(
        &router,
        "POST",
        "/api/account/totp/setup",
        session,
        serde_json::json!({"key":key}),
    )
    .await;
    let code = totp(
        &BASE32_NOPAD
            .decode(setup["secret"].as_str().unwrap().as_bytes())
            .unwrap(),
        now() / 30,
    );
    let (_, enabled) = call(
        &router,
        "POST",
        "/api/account/totp/enable",
        session,
        serde_json::json!({"challenge":setup["challenge"],"code":code}),
    )
    .await;
    let normal = enabled["session"].as_str().unwrap();
    assert_eq!(
        call(
            &router,
            "POST",
            "/api/account/totp/disable",
            normal,
            serde_json::json!({"key":key})
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    let last = enabled["recovery_codes"][0].as_str().unwrap();
    state
        .store
        .lock()
        .unwrap()
        .accounts
        .get_mut("alice")
        .unwrap()
        .recovery = HashSet::from([secret_hash(last)]);
    let (status, login) = call(
        &router,
        "POST",
        "/api/accounts/login",
        "",
        serde_json::json!({"username":"alice","key":key,"code":last}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(login["account"]["recovery_codes_remaining"], 0);
    let recovery = login["session"].as_str().unwrap();
    assert_eq!(
        call(
            &router,
            "GET",
            "/api/account",
            recovery,
            serde_json::Value::Null
        )
        .await
        .1["recovery_session"],
        true
    );
    assert_eq!(
        call(
            &router,
            "POST",
            "/api/account/totp/disable",
            recovery,
            serde_json::json!({"key":"wrong"})
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(
            &router,
            "POST",
            "/api/account/totp/disable",
            recovery,
            serde_json::json!({"key":key})
        )
        .await
        .0,
        StatusCode::OK
    );
    let (status, login) = call(
        &router,
        "POST",
        "/api/accounts/login",
        "",
        serde_json::json!({"username":"alice","key":key}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(login["account"]["two_factor_enabled"], false);
}

#[tokio::test]
async fn profile_pictures_are_private_bounded_and_scoped_to_the_session_owner() {
    use axum::http::Method;
    use image::{codecs::jpeg::JpegEncoder, ExtendedColorType};

    let state = test_state();
    let router = crate::app(state.clone());
    let profile = "/api/account/avatar";
    let alice = register(&router, &"a".repeat(64), "alice").await;
    let bob = register(&router, &"b".repeat(64), "bob").await;
    let alice_session = alice["session"].as_str().unwrap();
    let bob_session = bob["session"].as_str().unwrap();
    let anonymous = "c".repeat(64);
    for token in ["", anonymous.as_str(), alice["key"].as_str().unwrap()] {
        for (method, path) in [
            ("GET", "/api/account"),
            ("POST", "/api/account/logout"),
            ("POST", "/api/account/activate"),
            ("POST", "/api/account/key"),
            ("POST", "/api/account/key/confirm"),
            ("POST", "/api/account/totp/setup"),
            ("POST", "/api/account/totp/enable"),
            ("POST", "/api/account/totp/disable"),
            ("PUT", profile),
            ("DELETE", profile),
        ] {
            assert_eq!(
                call(&router, method, path, token, serde_json::json!({}))
                    .await
                    .0,
                StatusCode::UNAUTHORIZED,
                "{method} {path}"
            );
        }
    }
    let forged = Request::builder()
        .method(Method::DELETE)
        .uri(profile)
        .header(USER_HEADER, secret_hash(&"a".repeat(64)))
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        router.clone().oneshot(forged).await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
    let old_account = serde_json::to_value(&state.store.lock().unwrap().accounts["alice"]).unwrap();
    assert!(old_account.get("avatar").is_none());
    assert!(serde_json::from_value::<Account>(old_account)
        .unwrap()
        .avatar
        .is_none());

    let mut pixels = Vec::new();
    JpegEncoder::new(&mut pixels)
        .encode(&[85; 96 * 96 * 3], 96, 96, ExtendedColorType::Rgb8)
        .unwrap();
    let jpeg = BASE64.encode(&pixels);
    for (body, expected) in [
        (
            serde_json::json!({"jpeg":"PHN2Zy8+"}),
            StatusCode::BAD_REQUEST,
        ),
        (
            serde_json::json!({"jpeg":"A".repeat(MAX_AVATAR_BASE64 + 1)}),
            StatusCode::PAYLOAD_TOO_LARGE,
        ),
        (
            serde_json::json!({"jpeg":jpeg,"username":"bob"}),
            StatusCode::UNPROCESSABLE_ENTITY,
        ),
    ] {
        assert_eq!(
            call(&router, "PUT", profile, alice_session, body).await.0,
            expected
        );
    }
    let (status, saved) = call(
        &router,
        "PUT",
        profile,
        alice_session,
        serde_json::json!({"jpeg":jpeg}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{saved}");
    assert!(saved["avatar"].as_str().is_some());
    assert_eq!(
        call(
            &router,
            "GET",
            "/api/account",
            alice_session,
            serde_json::Value::Null
        )
        .await
        .1["avatar"],
        saved["avatar"]
    );
    assert!(call(
        &router,
        "GET",
        "/api/account",
        bob_session,
        serde_json::Value::Null
    )
    .await
    .1["avatar"]
        .is_null());
    assert_eq!(
        call(
            &router,
            "DELETE",
            profile,
            alice_session,
            serde_json::Value::Null
        )
        .await
        .0,
        StatusCode::OK
    );
    assert!(call(
        &router,
        "GET",
        "/api/account",
        alice_session,
        serde_json::Value::Null
    )
    .await
    .1["avatar"]
        .is_null());
    assert_eq!(
        call(
            &router,
            "POST",
            "/api/account/logout",
            alice_session,
            serde_json::Value::Null
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        call(
            &router,
            "PUT",
            profile,
            alice_session,
            serde_json::json!({"jpeg":jpeg})
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    let _ = std::fs::remove_file(state.data_file.as_path());
}
