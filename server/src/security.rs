//! Account credentials, TOTP and private application metadata. No user app HTML
//! executes at the dashboard origin. Runtime execution is a separate subsystem.
use crate::{
    identity, mutate_store, now, require_member, secret_hash, ApiError, ApiResult, AppState, Store,
    IDENTITY_HEADER,
};
use aes_gcm::{
    aead::{Aead, KeyInit, Payload},
    Aes256Gcm, Nonce,
};
use axum::{
    extract::{Path, Request, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use data_encoding::BASE32_NOPAD;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha1::Sha1;
use std::{
    collections::{HashMap, HashSet},
    time::Instant,
};
use subtle::ConstantTimeEq;
use uuid::Uuid;

pub const USER_HEADER: &str = "x-mesh-resolved-principal";
const SESSION_SECONDS: u64 = 12 * 60 * 60;

#[derive(Clone, Deserialize, Serialize)]
pub struct Account {
    pub id: String,
    pub username: String,
    key_hash: String,
    #[serde(default)]
    activated: bool,
    totp: Option<Sealed>,
    last_step: Option<u64>,
    recovery: HashSet<String>,
}
#[derive(Clone, Deserialize, Serialize)]
struct Sealed {
    nonce: String,
    ciphertext: String,
}
#[derive(Clone, Deserialize, Serialize)]
pub struct Application {
    pub id: Uuid,
    pub cluster_id: Uuid,
    pub owner_id: String,
    pub slug: String,
    pub name: String,
    pub members: HashMap<String, AppRole>,
}
#[derive(Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AppRole {
    Owner,
    Operator,
    Viewer,
}
struct Session {
    user: String,
    credential_hash: String,
    recovery_authenticated: bool,
    expires: u64,
}
struct Pending {
    user: String,
    secret: Vec<u8>,
    expires: u64,
}
struct PendingRotation {
    user: String,
    old_hash: String,
    expires: u64,
}
struct Bucket {
    tokens: f64,
    at: Instant,
}
impl Bucket {
    fn take(&mut self, burst: f64, rate: f64) -> bool {
        let at = Instant::now();
        self.tokens = (self.tokens + at.duration_since(self.at).as_secs_f64() * rate).min(burst);
        self.at = at;
        if self.tokens < 1.0 {
            false
        } else {
            self.tokens -= 1.0;
            true
        }
    }
}
pub struct Runtime {
    sessions: HashMap<String, Session>,
    pending: HashMap<String, Pending>,
    rotations: HashMap<String, PendingRotation>,
    login_limits: HashMap<String, (u32, u64)>,
    bootstrap: Bucket,
    signup: Bucket,
    encryption_key: Option<[u8; 32]>,
}
impl Default for Runtime {
    fn default() -> Self {
        let encryption_key = std::env::var("MESH_AUTH_ENCRYPTION_KEY")
            .ok()
            .and_then(|key| data_encoding::HEXLOWER.decode(key.as_bytes()).ok())
            .and_then(|key| key.try_into().ok());
        Self {
            sessions: HashMap::new(),
            pending: HashMap::new(),
            rotations: HashMap::new(),
            login_limits: HashMap::new(),
            bootstrap: Bucket {
                tokens: 64.0,
                at: Instant::now(),
            },
            signup: Bucket {
                tokens: 4.0,
                at: Instant::now(),
            },
            encryption_key,
        }
    }
}
fn unavailable() -> ApiError {
    ApiError(
        StatusCode::SERVICE_UNAVAILABLE,
        "Account service unavailable".into(),
    )
}
fn denied() -> ApiError {
    ApiError(
        StatusCode::UNAUTHORIZED,
        "Invalid credentials or authentication code".into(),
    )
}
fn hidden() -> ApiError {
    ApiError(
        StatusCode::NOT_FOUND,
        "Application not found or access denied".into(),
    )
}
fn random<const N: usize>() -> ApiResult<[u8; N]> {
    let mut bytes = [0; N];
    getrandom::getrandom(&mut bytes).map_err(|_| unavailable())?;
    Ok(bytes)
}
pub fn new_key() -> ApiResult<String> {
    Ok(data_encoding::HEXLOWER.encode(&random::<32>()?))
}
fn matches(hash: &str, raw: &str) -> bool {
    bool::from(hash.as_bytes().ct_eq(secret_hash(raw).as_bytes()))
}
fn handle(raw: &str) -> Option<String> {
    let name = raw.trim().to_ascii_lowercase();
    if !(3..=32).contains(&name.len())
        || !name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        || name.starts_with('-')
        || name.ends_with('-')
        || [
            "api", "assets", "health", "account", "login", "admin", "docs",
        ]
        .contains(&name.as_str())
    {
        None
    } else {
        Some(name)
    }
}
fn key_format(key: &str) -> bool {
    key.len() == 64
        && key
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}
fn encryption_key(state: &AppState) -> ApiResult<[u8; 32]> {
    state
        .durability
        .lock()
        .map_err(|_| unavailable())?
        .accounts
        .encryption_key
        .ok_or(ApiError(
            StatusCode::SERVICE_UNAVAILABLE,
            "Authenticator setup is unavailable until the server encryption secret is configured"
                .into(),
        ))
}
fn seal(key: &[u8; 32], user: &str, secret: &[u8]) -> ApiResult<Sealed> {
    let nonce = random::<12>()?;
    let cipher = Aes256Gcm::new_from_slice(key).map_err(|_| unavailable())?;
    let ciphertext = cipher
        .encrypt(
            &Nonce::from(nonce),
            Payload {
                msg: secret,
                aad: user.as_bytes(),
            },
        )
        .map_err(|_| unavailable())?;
    Ok(Sealed {
        nonce: BASE64.encode(nonce),
        ciphertext: BASE64.encode(ciphertext),
    })
}
fn unseal(key: &[u8; 32], user: &str, value: &Sealed) -> ApiResult<Vec<u8>> {
    let nonce: [u8; 12] = BASE64
        .decode(&value.nonce)
        .map_err(|_| unavailable())?
        .try_into()
        .map_err(|_| unavailable())?;
    Aes256Gcm::new_from_slice(key)
        .map_err(|_| unavailable())?
        .decrypt(
            &Nonce::from(nonce),
            Payload {
                msg: &BASE64
                    .decode(&value.ciphertext)
                    .map_err(|_| unavailable())?,
                aad: user.as_bytes(),
            },
        )
        .map_err(|_| unavailable())
}
fn totp(secret: &[u8], step: u64) -> String {
    let mut mac = <Hmac<Sha1> as Mac>::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(&step.to_be_bytes());
    let hash = mac.finalize().into_bytes();
    let offset = (hash[19] & 15) as usize;
    let value = u32::from_be_bytes(
        hash[offset..offset + 4]
            .try_into()
            .expect("bounded HMAC offset"),
    ) & 0x7fff_ffff;
    format!("{:06}", value % 1_000_000)
}
fn consume_factor(
    account: &mut Account,
    input: &str,
    key: Option<&[u8; 32]>,
    at: u64,
) -> ApiResult<()> {
    let Some(sealed) = &account.totp else {
        return Ok(());
    };
    if account.recovery.remove(&secret_hash(input)) {
        return Ok(());
    }
    let secret = unseal(key.ok_or_else(unavailable)?, &account.id, sealed)?;
    if input.len() != 6 || !input.bytes().all(|b| b.is_ascii_digit()) {
        return Err(denied());
    }
    let current = at / 30;
    // Never accept a time step twice, including parallel logins.
    for step in current.saturating_sub(1)..=current + 1 {
        if account.last_step.is_none_or(|last| step > last)
            && bool::from(totp(&secret, step).as_bytes().ct_eq(input.as_bytes()))
        {
            account.last_step = Some(step);
            return Ok(());
        }
    }
    Err(denied())
}
fn account_for<'a>(store: &'a Store, user: &str) -> ApiResult<&'a Account> {
    store
        .accounts
        .values()
        .find(|a| a.id == user)
        .ok_or_else(denied)
}
#[derive(Serialize)]
pub struct AccountSummary {
    pub username: String,
    pub activated: bool,
    pub recovery_session: bool,
    pub two_factor_enabled: bool,
    pub recovery_codes_remaining: usize,
}
fn account_summary(a: &Account) -> AccountSummary {
    AccountSummary {
        username: a.username.clone(),
        activated: a.activated,
        recovery_session: false,
        two_factor_enabled: a.totp.is_some(),
        recovery_codes_remaining: a.recovery.len(),
    }
}
fn issue_session(state: &AppState, user: String) -> ApiResult<String> {
    issue_session_kind(state, user, false)
}
fn issue_session_kind(
    state: &AppState,
    user: String,
    recovery_authenticated: bool,
) -> ApiResult<String> {
    let token = format!("ms_{}", new_key()?);
    let credential_hash = {
        let store = state.store.lock().map_err(|_| unavailable())?;
        account_for(&store, &user)?.key_hash.clone()
    };
    let mut runtime = state.durability.lock().map_err(|_| unavailable())?;
    runtime.accounts.sessions.retain(|_, s| s.expires > now());
    if runtime.accounts.sessions.len() >= 4096 {
        return Err(unavailable());
    }
    runtime.accounts.sessions.insert(
        secret_hash(&token),
        Session {
            user,
            credential_hash,
            recovery_authenticated,
            expires: now() + SESSION_SECONDS,
        },
    );
    Ok(token)
}
fn end_sessions(state: &AppState, user: &str) {
    if let Ok(mut rt) = state.durability.lock() {
        rt.accounts.sessions.retain(|_, s| s.user != user);
        rt.accounts.pending.retain(|_, p| p.user != user);
        rt.accounts.rotations.retain(|_, p| p.user != user);
    }
}

pub async fn authenticate(
    State(state): State<AppState>,
    mut request: Request,
    next: Next,
) -> Response {
    request.headers_mut().remove(USER_HEADER); // Never trust a client-supplied resolved principal.
    let raw = request
        .headers()
        .get(IDENTITY_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    if let Some(raw) = raw {
        let resolved = (|| -> ApiResult<String> {
            if !(32..=128).contains(&raw.len()) {
                return Err(denied());
            }
            let hash = secret_hash(&raw);
            // Lock order: runtime is released before reading persistent state.
            let session = state
                .durability
                .lock()
                .map_err(|_| unavailable())?
                .accounts
                .sessions
                .get(&hash)
                .filter(|s| s.expires > now())
                .map(|s| (s.user.clone(), s.credential_hash.clone()));
            let store = state.store.lock().map_err(|_| unavailable())?;
            if let Some((user, credential_hash)) = session {
                if account_for(&store, &user)?.key_hash != credential_hash {
                    return Err(denied());
                }
                return Ok(user);
            }
            if raw.starts_with("ms_") {
                return Err(denied());
            }
            if store.revoked_credentials.contains(&hash)
                || store.accounts.values().any(|a| a.key_hash == hash)
            {
                return Err(denied());
            }
            Ok(hash) // Compatibility for existing anonymous cluster identities.
        })();
        match resolved {
            Ok(user) => {
                request.headers_mut().insert(
                    USER_HEADER,
                    HeaderValue::from_str(&user).expect("hex principal"),
                );
            }
            Err(error) if raw.starts_with("ms_") && error.0 == StatusCode::UNAUTHORIZED => return private((StatusCode::UNAUTHORIZED, Json(serde_json::json!({"source":"mesh-coordinator","code":"account_session_required","error":"Sign in again to resume this account session"}))).into_response()),
            Err(error) => return private(error.into_response()),
        }
    }
    private(next.run(request).await)
}
fn private(mut response: Response) -> Response {
    response
        .headers_mut()
        .insert("cache-control", HeaderValue::from_static("no-store"));
    response
        .headers_mut()
        .insert("referrer-policy", HeaderValue::from_static("no-referrer"));
    response
}
pub async fn bootstrap(State(state): State<AppState>) -> ApiResult<Json<serde_json::Value>> {
    if !state
        .durability
        .lock()
        .map_err(|_| unavailable())?
        .accounts
        .bootstrap
        .take(64.0, 4.0)
    {
        return Err(ApiError(
            StatusCode::TOO_MANY_REQUESTS,
            "Please retry shortly".into(),
        ));
    }
    Ok(Json(serde_json::json!({"key":new_key()?})))
}
#[derive(Deserialize)]
pub struct Signup {
    username: String,
}
pub async fn signup(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(input): Json<Signup>,
) -> ApiResult<Json<serde_json::Value>> {
    let user = identity(&headers)?;
    let username = handle(&input.username).ok_or(ApiError(
        StatusCode::BAD_REQUEST,
        "Use 3–32 lowercase letters, digits or internal hyphens for a username".into(),
    ))?;
    if !state
        .durability
        .lock()
        .map_err(|_| unavailable())?
        .accounts
        .signup
        .take(4.0, 1.0 / 60.0)
    {
        return Err(ApiError(
            StatusCode::TOO_MANY_REQUESTS,
            "Account creation is temporarily rate limited".into(),
        ));
    }
    let key = new_key()?;
    let key_hash = secret_hash(&key);
    let result = mutate_store(&state, |store| {
        if store.accounts.len() >= 4096
            || store
                .accounts
                .get(&username)
                .is_some_and(|a| a.id != user || a.activated)
            || store
                .accounts
                .values()
                .any(|a| a.id == user && (a.username != username || a.activated))
        {
            return Err(ApiError(
                StatusCode::CONFLICT,
                "Username unavailable or this identity already has an account".into(),
            ));
        }
        let account = Account {
            id: user.clone(),
            username: username.clone(),
            key_hash: key_hash.clone(),
            activated: false,
            totp: None,
            last_step: None,
            recovery: HashSet::new(),
        };
        let summary = account_summary(&account);
        store.accounts.insert(username.clone(), account);
        Ok(summary)
    })
    .await?;
    end_sessions(&state, &user);
    Ok(Json(
        serde_json::json!({"account":result,"key":key,"session":issue_session(&state,user)?}),
    ))
}
#[derive(Deserialize)]
pub struct Credentials {
    username: String,
    key: String,
    #[serde(default)]
    code: String,
}
fn login_attempt(state: &AppState, username: &str) -> ApiResult<()> {
    let mut runtime = state.durability.lock().map_err(|_| unavailable())?;
    let at = now();
    runtime
        .accounts
        .login_limits
        .retain(|_, (_, until)| *until > at);
    if runtime.accounts.login_limits.len() >= 4096
        && !runtime.accounts.login_limits.contains_key(username)
    {
        return Err(ApiError(
            StatusCode::TOO_MANY_REQUESTS,
            "Sign-in temporarily rate limited".into(),
        ));
    }
    let entry = runtime
        .accounts
        .login_limits
        .entry(username.into())
        .or_insert((0, at + 60));
    if entry.0 >= 6 {
        return Err(ApiError(
            StatusCode::TOO_MANY_REQUESTS,
            "Too many authentication attempts; wait one minute".into(),
        ));
    }
    entry.0 += 1;
    Ok(())
}
pub async fn login(
    State(state): State<AppState>,
    Json(input): Json<Credentials>,
) -> ApiResult<Json<serde_json::Value>> {
    let username = handle(&input.username).ok_or_else(denied)?;
    if !key_format(&input.key) || input.code.len() > 64 {
        return Err(denied());
    }
    {
        let store = state.store.lock().map_err(|_| unavailable())?;
        if !store
            .accounts
            .get(&username)
            .is_some_and(|a| matches(&a.key_hash, &input.key))
        {
            return Err(denied());
        }
    }
    login_attempt(&state, &username)?;
    let key = state
        .durability
        .lock()
        .map_err(|_| unavailable())?
        .accounts
        .encryption_key;
    let (user, mut summary, recovery_login) = mutate_store(&state, |store| {
        let account = store
            .accounts
            .get_mut(&username)
            .filter(|a| matches(&a.key_hash, &input.key))
            .ok_or_else(denied)?;
        let recovery_login =
            account.totp.is_some() && account.recovery.contains(&secret_hash(&input.code));
        consume_factor(account, &input.code, key.as_ref(), now())?;
        Ok((account.id.clone(), account_summary(account), recovery_login))
    })
    .await?;
    summary.recovery_session = recovery_login;
    Ok(Json(
        serde_json::json!({"account":summary,"session":issue_session_kind(&state,user,recovery_login)?}),
    ))
}
pub async fn me(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> ApiResult<Json<Option<AccountSummary>>> {
    let user = identity(&headers)?;
    let recovery_session = is_recovery_session(&state, &headers, &user);
    let store = state.store.lock().map_err(|_| unavailable())?;
    Ok(Json(
        store
            .accounts
            .values()
            .find(|a| {
                a.id == user
                    && (a.activated
                        || secret_hash(
                            headers
                                .get(IDENTITY_HEADER)
                                .and_then(|v| v.to_str().ok())
                                .unwrap_or(""),
                        ) != a.id)
            })
            .map(|a| {
                let mut summary = account_summary(a);
                summary.recovery_session = recovery_session;
                summary
            }),
    ))
}
pub async fn logout(State(state): State<AppState>, headers: HeaderMap) -> StatusCode {
    if let Some(raw) = headers.get(IDENTITY_HEADER).and_then(|v| v.to_str().ok()) {
        if let Ok(mut rt) = state.durability.lock() {
            rt.accounts.sessions.remove(&secret_hash(raw));
        }
    }
    StatusCode::NO_CONTENT
}
#[derive(Deserialize)]
pub struct FactorProof {
    key: String,
    #[serde(default)]
    code: String,
}
pub async fn begin_totp(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(proof): Json<FactorProof>,
) -> ApiResult<Json<serde_json::Value>> {
    let user = identity(&headers)?;
    let _ = encryption_key(&state)?;
    let username = {
        let store = state.store.lock().map_err(|_| unavailable())?;
        let a = account_for(&store, &user)?;
        if !a.activated || !matches(&a.key_hash, &proof.key) || a.totp.is_some() {
            return Err(denied());
        }
        a.username.clone()
    };
    let secret = random::<32>()?.to_vec();
    let encoded = BASE32_NOPAD.encode(&secret);
    let challenge = new_key()?;
    let mut rt = state.durability.lock().map_err(|_| unavailable())?;
    rt.accounts
        .pending
        .retain(|_, p| p.expires > now() && p.user != user);
    if rt.accounts.pending.len() >= 4096 {
        return Err(unavailable());
    }
    rt.accounts.pending.insert(
        secret_hash(&challenge),
        Pending {
            user,
            secret,
            expires: now() + 600,
        },
    );
    Ok(Json(
        serde_json::json!({"secret":encoded,"challenge":challenge,"uri":format!("otpauth://totp/Mesh:{username}?secret={encoded}&issuer=Mesh&algorithm=SHA1&digits=6&period=30")}),
    ))
}
#[derive(Deserialize)]
pub struct ConfirmFactor {
    challenge: String,
    code: String,
}
pub async fn enable_totp(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(proof): Json<ConfirmFactor>,
) -> ApiResult<Json<serde_json::Value>> {
    let user = identity(&headers)?;
    login_attempt(&state, &format!("factor:{user}"))?;
    let secret = {
        let rt = state.durability.lock().map_err(|_| unavailable())?;
        rt.accounts
            .pending
            .get(&secret_hash(&proof.challenge))
            .filter(|p| p.user == user && p.expires > now())
            .map(|p| p.secret.clone())
            .ok_or_else(denied)?
    };
    let key = encryption_key(&state)?;
    let sealed = seal(&key, &user, &secret)?;
    let codes = (0..8)
        .map(|_| random::<16>().map(|value| data_encoding::HEXLOWER.encode(&value)))
        .collect::<ApiResult<Vec<_>>>()?;
    mutate_store(&state, |store| {
        let a = store
            .accounts
            .values_mut()
            .find(|a| a.id == user && a.totp.is_none())
            .ok_or_else(denied)?;
        a.totp = Some(sealed.clone());
        consume_factor(a, &proof.code, Some(&key), now())?;
        a.recovery = codes.iter().map(|code| secret_hash(code)).collect();
        Ok(())
    })
    .await?;
    end_sessions(&state, &user);
    Ok(Json(
        serde_json::json!({"recovery_codes":codes,"session":issue_session(&state,user)?}),
    ))
}
pub async fn rotate_key(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(proof): Json<FactorProof>,
) -> ApiResult<Json<serde_json::Value>> {
    let user = identity(&headers)?;
    login_attempt(&state, &format!("factor:{user}"))?;
    let encryption = state
        .durability
        .lock()
        .map_err(|_| unavailable())?
        .accounts
        .encryption_key;
    let key = new_key()?;
    let old_hash = mutate_store(&state, |store| {
        let a = store
            .accounts
            .values_mut()
            .find(|a| a.id == user && a.activated && matches(&a.key_hash, &proof.key))
            .ok_or_else(denied)?;
        consume_factor(a, &proof.code, encryption.as_ref(), now())?;
        Ok(a.key_hash.clone())
    })
    .await?;
    let mut rt = state.durability.lock().map_err(|_| unavailable())?;
    rt.accounts
        .rotations
        .retain(|_, p| p.expires > now() && p.user != user);
    if rt.accounts.rotations.len() >= 4096 {
        return Err(unavailable());
    }
    rt.accounts.rotations.insert(
        secret_hash(&key),
        PendingRotation {
            user,
            old_hash,
            expires: now() + 600,
        },
    );
    Ok(Json(serde_json::json!({"key":key})))
}
#[derive(Deserialize)]
pub struct RotationConfirmation {
    key: String,
}
pub async fn confirm_rotation(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(input): Json<RotationConfirmation>,
) -> ApiResult<Json<serde_json::Value>> {
    let user = identity(&headers)?;
    let hash = secret_hash(&input.key);
    let old_hash = {
        let rt = state.durability.lock().map_err(|_| unavailable())?;
        rt.accounts
            .rotations
            .get(&hash)
            .filter(|p| p.user == user && p.expires > now())
            .map(|p| p.old_hash.clone())
            .ok_or_else(denied)?
    };
    mutate_store(&state, |store| {
        let a = store
            .accounts
            .values_mut()
            .find(|a| a.id == user && a.key_hash == old_hash)
            .ok_or_else(denied)?;
        store.revoked_credentials.insert(a.key_hash.clone());
        a.key_hash = hash.clone();
        Ok(())
    })
    .await?;
    end_sessions(&state, &user);
    Ok(Json(
        serde_json::json!({"session":issue_session(&state,user)?}),
    ))
}
pub async fn activate(State(state): State<AppState>, headers: HeaderMap) -> ApiResult<StatusCode> {
    let user = identity(&headers)?;
    // Only a newly issued account session can retire the old anonymous credential.
    let raw = headers
        .get(IDENTITY_HEADER)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(denied)?;
    let version = state
        .durability
        .lock()
        .map_err(|_| unavailable())?
        .accounts
        .sessions
        .get(&secret_hash(raw))
        .filter(|s| s.user == user && s.expires > now())
        .map(|s| s.credential_hash.clone())
        .ok_or_else(denied)?;
    mutate_store(&state, |store| {
        let a = store
            .accounts
            .values_mut()
            .find(|a| a.id == user && a.key_hash == version)
            .ok_or_else(denied)?;
        a.activated = true;
        store.revoked_credentials.insert(user.clone());
        Ok(())
    })
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn disable_totp(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(proof): Json<FactorProof>,
) -> ApiResult<Json<serde_json::Value>> {
    let user = identity(&headers)?;
    login_attempt(&state, &format!("factor:{user}"))?;
    let encryption = state
        .durability
        .lock()
        .map_err(|_| unavailable())?
        .accounts
        .encryption_key;
    let recovery_session = is_recovery_session(&state, &headers, &user);
    mutate_store(&state, |store| {
        let a = store
            .accounts
            .values_mut()
            .find(|a| a.id == user && a.totp.is_some() && matches(&a.key_hash, &proof.key))
            .ok_or_else(denied)?;
        if !recovery_session || !proof.code.is_empty() {
            consume_factor(a, &proof.code, encryption.as_ref(), now())?;
        }
        a.totp = None;
        a.last_step = None;
        a.recovery.clear();
        Ok(())
    })
    .await?;
    end_sessions(&state, &user);
    Ok(Json(
        serde_json::json!({"session":issue_session(&state,user)?}),
    ))
}

#[derive(Serialize)]
pub struct AppSummary {
    id: Uuid,
    cluster_id: Uuid,
    name: String,
    slug: String,
    owner: String,
    url: String,
    role: AppRole,
    members: Vec<AppMember>,
    runtime_status: &'static str,
}
#[derive(Serialize)]
struct AppMember {
    username: String,
    role: AppRole,
}
fn app_summary(app: &Application, store: &Store, user: &str) -> ApiResult<AppSummary> {
    require_member(store, app.cluster_id, user).map_err(|_| hidden())?;
    let role = *app.members.get(user).ok_or_else(hidden)?;
    let username = |id: &str| {
        store
            .accounts
            .values()
            .find(|a| a.id == id)
            .map(|a| a.username.clone())
    };
    let owner = username(&app.owner_id).ok_or_else(hidden)?;
    Ok(AppSummary {
        id: app.id,
        cluster_id: app.cluster_id,
        name: app.name.clone(),
        slug: app.slug.clone(),
        url: format!("/{owner}/{}", app.slug),
        owner,
        role,
        members: app
            .members
            .iter()
            .filter_map(|(id, role)| {
                username(id).map(|username| AppMember {
                    username,
                    role: *role,
                })
            })
            .collect(),
        runtime_status: "not_deployed",
    })
}
#[derive(Deserialize)]
pub struct CreateApplication {
    slug: String,
    name: String,
}
pub async fn list_apps(
    State(state): State<AppState>,
    Path(cluster): Path<Uuid>,
    headers: HeaderMap,
) -> ApiResult<Json<Vec<AppSummary>>> {
    let user = identity(&headers)?;
    let store = state.store.lock().map_err(|_| unavailable())?;
    require_member(&store, cluster, &user)?;
    Ok(Json(
        store
            .applications
            .values()
            .filter(|a| a.cluster_id == cluster && a.members.contains_key(&user))
            .filter_map(|a| app_summary(a, &store, &user).ok())
            .collect(),
    ))
}
pub async fn create_app(
    State(state): State<AppState>,
    Path(cluster): Path<Uuid>,
    headers: HeaderMap,
    Json(input): Json<CreateApplication>,
) -> ApiResult<Json<AppSummary>> {
    let user = identity(&headers)?;
    let slug = handle(&input.slug).ok_or(ApiError(
        StatusCode::BAD_REQUEST,
        "Use a 3–32 character application slug".into(),
    ))?;
    let name = crate::cluster_name(&input.name).ok_or(ApiError(
        StatusCode::BAD_REQUEST,
        "Application name is required".into(),
    ))?;
    let id = Uuid::new_v4();
    let result = mutate_store(&state, |store| {
        require_member(store, cluster, &user)?;
        if !account_for(store, &user)?.activated {
            return Err(ApiError(
                StatusCode::FORBIDDEN,
                "Save and activate your account key first".into(),
            ));
        }
        if store
            .applications
            .values()
            .filter(|a| a.cluster_id == cluster)
            .count()
            >= 32
            || store
                .applications
                .values()
                .any(|a| a.owner_id == user && a.slug == slug)
        {
            return Err(ApiError(
                StatusCode::CONFLICT,
                "Application name unavailable or cluster application limit reached".into(),
            ));
        }
        let app = Application {
            id,
            cluster_id: cluster,
            owner_id: user.clone(),
            slug: slug.clone(),
            name: name.clone(),
            members: HashMap::from([(user.clone(), AppRole::Owner)]),
        };
        let summary = app_summary(&app, store, &user)?;
        store.applications.insert(id, app);
        Ok(summary)
    })
    .await?;
    Ok(Json(result))
}
pub async fn get_app(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
) -> ApiResult<Json<AppSummary>> {
    let user = identity(&headers)?;
    let store = state.store.lock().map_err(|_| unavailable())?;
    Ok(Json(app_summary(
        store.applications.get(&id).ok_or_else(hidden)?,
        &store,
        &user,
    )?))
}
pub async fn by_name(
    State(state): State<AppState>,
    Path((owner, slug)): Path<(String, String)>,
    headers: HeaderMap,
) -> ApiResult<Json<AppSummary>> {
    let user = identity(&headers)?;
    let store = state.store.lock().map_err(|_| unavailable())?;
    let owner = store.accounts.get(&owner).ok_or_else(hidden)?;
    let app = store
        .applications
        .values()
        .find(|a| a.owner_id == owner.id && a.slug == slug)
        .ok_or_else(hidden)?;
    Ok(Json(app_summary(app, &store, &user)?))
}
#[derive(Deserialize)]
pub struct Grant {
    username: String,
    role: AppRole,
}
pub async fn grant(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
    Json(input): Json<Grant>,
) -> ApiResult<Json<AppSummary>> {
    let user = identity(&headers)?;
    let result = mutate_store(&state, |store| {
        let app = store.applications.get(&id).ok_or_else(hidden)?;
        app_summary(app, store, &user)?;
        if app.owner_id != user || input.role == AppRole::Owner {
            return Err(ApiError(
                StatusCode::FORBIDDEN,
                "Only the application owner can grant viewer or operator access".into(),
            ));
        }
        let target = store
            .accounts
            .get(&input.username)
            .ok_or_else(hidden)?
            .id
            .clone();
        require_member(store, app.cluster_id, &target).map_err(|_| hidden())?;
        if target == user || app.members.len() >= 64 {
            return Err(ApiError(
                StatusCode::CONFLICT,
                "Cannot change owner access or exceed the application member limit".into(),
            ));
        }
        let app = store.applications.get_mut(&id).ok_or_else(hidden)?;
        app.members.insert(target, input.role);
        let application = app.clone();
        app_summary(&application, store, &user)
    })
    .await?;
    Ok(Json(result))
}
pub async fn revoke(
    State(state): State<AppState>,
    Path((id, username)): Path<(Uuid, String)>,
    headers: HeaderMap,
) -> ApiResult<StatusCode> {
    let user = identity(&headers)?;
    mutate_store(&state, |store| {
        let app = store.applications.get(&id).ok_or_else(hidden)?;
        app_summary(app, store, &user)?;
        if app.owner_id != user {
            return Err(ApiError(
                StatusCode::FORBIDDEN,
                "Only the application owner can revoke access".into(),
            ));
        }
        let target = store.accounts.get(&username).ok_or_else(hidden)?.id.clone();
        if target == user {
            return Err(ApiError(
                StatusCode::CONFLICT,
                "Cannot revoke the owner".into(),
            ));
        }
        store
            .applications
            .get_mut(&id)
            .ok_or_else(hidden)?
            .members
            .remove(&target);
        Ok(())
    })
    .await?;
    Ok(StatusCode::NO_CONTENT)
}
pub async fn delete_app(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
) -> ApiResult<StatusCode> {
    let user = identity(&headers)?;
    mutate_store(&state, |store| {
        let app = store.applications.get(&id).ok_or_else(hidden)?;
        app_summary(app, store, &user)?;
        if app.owner_id != user {
            return Err(ApiError(
                StatusCode::FORBIDDEN,
                "Only the application owner can delete it".into(),
            ));
        }
        store.applications.remove(&id);
        Ok(())
    })
    .await?;
    Ok(StatusCode::NO_CONTENT)
}
pub async fn application_page() -> ApiResult<axum::response::Html<String>> {
    Ok(axum::response::Html(
        std::fs::read_to_string("dist/index.html").map_err(|_| unavailable())?,
    ))
}

#[cfg(test)]
#[path = "security_tests.rs"]
mod tests;

fn is_recovery_session(state: &AppState, headers: &HeaderMap, user: &str) -> bool {
    let Some(raw) = headers.get(IDENTITY_HEADER).and_then(|v| v.to_str().ok()) else {
        return false;
    };
    state.durability.lock().ok().is_some_and(|runtime| {
        runtime
            .accounts
            .sessions
            .get(&secret_hash(raw))
            .is_some_and(|session| {
                session.user == user && session.expires > now() && session.recovery_authenticated
            })
    })
}
