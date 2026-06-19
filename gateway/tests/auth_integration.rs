//! Integration tests for gateway RBAC enforcement.
//!
//! These tests verify that the gateway correctly gates account-data methods
//! based on JWT role and wallet ownership.
//!
//! Strategy:
//! - Spin up a real Postgres container (testcontainers).
//! - Bootstrap the schema using `private_channel_auth::db::init_schema` — the same
//!   function the auth service uses, so tests stay in sync with schema changes.
//! - Insert test users and wallets directly via sqlx (no auth HTTP service needed).
//! - Mint JWTs directly with `jsonwebtoken` using a known test secret.
//! - Start the gateway with auth enforcement enabled, pointing at a mock backend.
//!
//! Run with:
//!   cargo test --test auth_integration -p private-channel-gateway -- --test-threads=1

use std::net::SocketAddr;
use std::sync::Arc;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use chrono::Utc;
use jsonwebtoken::{encode, EncodingKey, Header};
use private_channel_auth::db;
use reqwest::Client;
use serde_json::json;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use uuid::Uuid;

use private_channel_gateway::{serve, Gateway};

// ── Constants ──────────────────────────────────────────────────────────────────

/// Shared JWT secret used by both the token minter and the gateway under test.
const TEST_JWT_SECRET: &str = "test-gateway-secret";

// ── Infrastructure helpers ─────────────────────────────────────────────────────

/// Spin up a Postgres container and return a live connection pool and its URL.
/// The container is returned to keep it alive for the duration of the test.
async fn start_postgres() -> (PgPool, String, testcontainers::ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_db_name("gateway_auth_test")
        .with_user("postgres")
        .with_password("password")
        .start()
        .await
        .expect("failed to start postgres container");

    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!(
        "postgres://postgres:password@{}:{}/gateway_auth_test",
        host, port
    );

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&url)
        .await
        .expect("failed to connect to test postgres");

    (pool, url, container)
}

/// Insert a bare-minimum user row with the given role and return its UUID.
/// We only need the user to exist so verified_wallets can reference it —
/// the gateway reads the role from the JWT, not from this row. But keeping
/// the DB role consistent with the token makes tests easier to reason about.
async fn insert_user(pool: &PgPool, role: &str) -> Uuid {
    let id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO private_channel_auth.users (id, username, password_hash, role)
         VALUES ($1, $2, 'irrelevant_hash', $3::private_channel_auth.user_role)",
    )
    .bind(id)
    .bind(format!("testuser_{}", id))
    .bind(role)
    .execute(pool)
    .await
    .unwrap();
    id
}

/// Register a pubkey as a verified wallet for the given user.
async fn insert_wallet(pool: &PgPool, user_id: Uuid, pubkey: &str) {
    sqlx::query(
        "INSERT INTO private_channel_auth.verified_wallets (id, user_id, pubkey)
         VALUES ($1, $2, $3)",
    )
    .bind(Uuid::new_v4())
    .bind(user_id)
    .bind(pubkey)
    .execute(pool)
    .await
    .unwrap();
}

/// Generate a signed JWT for the given user and role using TEST_JWT_SECRET.
/// Expires in 1 hour — sufficient for any test run.
fn generate_token(user_id: Uuid, role: &str) -> String {
    let claims = json!({
        "sub": user_id.to_string(),
        "role": role,
        "exp": (Utc::now().timestamp() + 3600) as usize,
        "iss": "private-channel-auth",
        "aud": "private-channel-gateway",
    });
    encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(TEST_JWT_SECRET.as_bytes()),
    )
    .unwrap()
}

/// Generate an already-expired JWT (expired 1 hour ago, well past the 60-second
/// clock-skew leeway in jsonwebtoken's default Validation).
fn generate_expired_token(user_id: Uuid, role: &str) -> String {
    let claims = json!({
        "sub": user_id.to_string(),
        "role": role,
        "exp": (Utc::now().timestamp() - 3600) as usize,
        "iss": "private-channel-auth",
        "aud": "private-channel-gateway",
    });
    encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(TEST_JWT_SECRET.as_bytes()),
    )
    .unwrap()
}

/// Spawn a mock HTTP backend that always returns the given body as a 200 response.
/// Handles multiple sequential connections so the gateway can call it for both
/// the auth account-fetch and the actual proxied request.
async fn start_mock_backend_with_body(body: impl Into<String> + Send + 'static) -> SocketAddr {
    let body = body.into();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let mut buf = vec![0u8; 4096];
            let _ = stream.read(&mut buf).await;
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = stream.write_all(resp.as_bytes()).await;
        }
    });
    addr
}

/// Start a gateway with auth enforcement enabled.
///
/// `write_url` defaults to an unreachable port when not needed — most tests
/// only exercise read methods. Pass an actual URL when testing write routing.
async fn start_gateway(auth_db: PgPool, write_url: String, read_url: String) -> SocketAddr {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .ok();

    let gateway = Arc::new(Gateway::new(
        write_url,
        read_url,
        "*".to_string(),
        Some(TEST_JWT_SECRET.to_string()),
        Some(auth_db),
    ));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        let _ = serve(listener, gateway).await;
    });

    addr
}

// ── Mock account data builders ─────────────────────────────────────────────────

/// Build a getAccountInfo JSON-RPC response for a System Program account.
/// The data field is empty — System Program accounts carry no inner data.
/// Used to simulate a user querying their own SOL wallet pubkey directly.
fn system_account_response() -> String {
    json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": {
            "value": {
                "lamports": 1_000_000,
                "owner": "11111111111111111111111111111111",
                "data": ["", "base64"],
                "executable": false,
                "rentEpoch": 0
            }
        }
    })
    .to_string()
}

/// Build a getAccountInfo JSON-RPC response for a null account (does not exist).
fn null_account_response() -> String {
    json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": { "value": null }
    })
    .to_string()
}

/// Build a getAccountInfo JSON-RPC response for an SPL token account.
///
/// `owner_bytes` is placed at bytes 32-63 of the account data (the token
/// account owner field). If `delegate_bytes` is `Some`, bytes 72-75 are set
/// to `[1,0,0,0]` (Some discriminant) and bytes 76-107 hold the delegate.
fn token_account_response(owner_bytes: &[u8; 32], delegate_bytes: Option<&[u8; 32]>) -> String {
    let mut data = vec![0u8; 165];
    data[32..64].copy_from_slice(owner_bytes);
    if let Some(d) = delegate_bytes {
        data[72..76].copy_from_slice(&[1, 0, 0, 0]);
        data[76..108].copy_from_slice(d);
    }
    let encoded = BASE64.encode(&data);
    json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": {
            "value": {
                "lamports": 2_039_280,
                "owner": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
                "data": [encoded, "base64"],
                "executable": false,
                "rentEpoch": 0
            }
        }
    })
    .to_string()
}

/// Build a getAccountInfo JSON-RPC response for a mint account (82 bytes, SPL
/// Token program). Used to verify the gateway rejects mint queries for users.
fn mint_account_response() -> String {
    let encoded = BASE64.encode(vec![0u8; 82]);
    json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": {
            "value": {
                "lamports": 1_461_600,
                "owner": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
                "data": [encoded, "base64"],
                "executable": false,
                "rentEpoch": 0
            }
        }
    })
    .to_string()
}

// ── Tests ──────────────────────────────────────────────────────────────────────

/// `getAccountInfo` with no Authorization header must be rejected with 401.
///
/// This is the most basic gate check: any attempt to read account data without
/// identifying yourself must be turned away at the gateway.
#[tokio::test(flavor = "multi_thread")]
async fn test_get_account_info_no_token_returns_401() {
    let (pool, _url, _container) = start_postgres().await;
    db::init_schema(&pool).await.unwrap();
    let backend = start_mock_backend_with_body(system_account_response()).await;
    let addr = start_gateway(
        pool,
        "http://127.0.0.1:1".to_string(),
        format!("http://{}", backend),
    )
    .await;

    let res = Client::new()
        .post(format!("http://{}", addr))
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getAccountInfo",
            "params": ["So11111111111111111111111111111111111111112"]
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(res.status(), 401);
}

/// `getAccountInfo` with an expired JWT must return 401.
#[tokio::test(flavor = "multi_thread")]
async fn test_get_account_info_expired_token_returns_401() {
    let (pool, _url, _container) = start_postgres().await;
    db::init_schema(&pool).await.unwrap();
    let user_id = insert_user(&pool, "user").await;
    let token = generate_expired_token(user_id, "user");
    let backend = start_mock_backend_with_body(system_account_response()).await;
    let addr = start_gateway(
        pool,
        "http://127.0.0.1:1".to_string(),
        format!("http://{}", backend),
    )
    .await;

    let res = Client::new()
        .post(format!("http://{}", addr))
        .bearer_auth(token)
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getAccountInfo",
            "params": ["So11111111111111111111111111111111111111112"]
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(res.status(), 401);
}

/// `getAccountInfo` with a JWT signed by a different secret must return 401.
///
/// Ensures the gateway rejects tokens it did not issue (wrong secret = tampered
/// or from a different environment).
#[tokio::test(flavor = "multi_thread")]
async fn test_get_account_info_invalid_token_returns_401() {
    let (pool, _url, _container) = start_postgres().await;
    db::init_schema(&pool).await.unwrap();
    let backend = start_mock_backend_with_body(system_account_response()).await;
    let addr = start_gateway(
        pool,
        "http://127.0.0.1:1".to_string(),
        format!("http://{}", backend),
    )
    .await;

    let bad_token = encode(
        &Header::default(),
        &json!({
            "sub": Uuid::new_v4().to_string(),
            "role": "user",
            "exp": (Utc::now().timestamp() + 3600) as usize,
        }),
        &EncodingKey::from_secret(b"wrong-secret"),
    )
    .unwrap();

    let res = Client::new()
        .post(format!("http://{}", addr))
        .bearer_auth(bad_token)
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getAccountInfo",
            "params": ["So11111111111111111111111111111111111111112"]
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(res.status(), 401);
}

/// A user querying their own wallet pubkey directly (System Program account)
/// must be allowed through — the fallback pubkey check in Phase 2 handles this.
#[tokio::test(flavor = "multi_thread")]
async fn test_get_account_info_owned_wallet_pubkey_is_proxied() {
    let (pool, _url, _container) = start_postgres().await;
    db::init_schema(&pool).await.unwrap();

    let pubkey = "So11111111111111111111111111111111111111112";
    let user_id = insert_user(&pool, "user").await;
    insert_wallet(&pool, user_id, pubkey).await;
    let token = generate_token(user_id, "user");

    // System Program account: non-token program triggers the pubkey fallback check.
    let backend = start_mock_backend_with_body(system_account_response()).await;
    let addr = start_gateway(
        pool,
        "http://127.0.0.1:1".to_string(),
        format!("http://{}", backend),
    )
    .await;

    let res = Client::new()
        .post(format!("http://{}", addr))
        .bearer_auth(token)
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getAccountInfo",
            "params": [pubkey]
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(res.status(), 200);
}

/// A user querying a System Program account whose pubkey is NOT in their
/// verified wallets must be denied with 403.
#[tokio::test(flavor = "multi_thread")]
async fn test_get_account_info_unowned_system_account_returns_403() {
    let (pool, _url, _container) = start_postgres().await;
    db::init_schema(&pool).await.unwrap();

    // User with no registered wallets.
    let user_id = insert_user(&pool, "user").await;
    let token = generate_token(user_id, "user");

    let backend = start_mock_backend_with_body(system_account_response()).await;
    let addr = start_gateway(
        pool,
        "http://127.0.0.1:1".to_string(),
        format!("http://{}", backend),
    )
    .await;

    let res = Client::new()
        .post(format!("http://{}", addr))
        .bearer_auth(token)
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getAccountInfo",
            "params": ["So11111111111111111111111111111111111111112"]
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(res.status(), 403);
}

/// A user querying an ATA whose owner field matches one of their verified
/// wallets must be allowed through.
#[tokio::test(flavor = "multi_thread")]
async fn test_get_account_info_ata_owned_by_user_wallet_is_proxied() {
    let (pool, _url, _container) = start_postgres().await;
    db::init_schema(&pool).await.unwrap();

    // Fixed bytes for the wallet that owns the ATA.
    let wallet_bytes = [7u8; 32];
    let wallet_pubkey = bs58::encode(wallet_bytes).into_string();

    let user_id = insert_user(&pool, "user").await;
    insert_wallet(&pool, user_id, &wallet_pubkey).await;
    let token = generate_token(user_id, "user");

    // ATA account: owner field == wallet_bytes.
    let ata_pubkey = bs58::encode([8u8; 32]).into_string();
    let backend = start_mock_backend_with_body(token_account_response(&wallet_bytes, None)).await;
    let addr = start_gateway(
        pool,
        "http://127.0.0.1:1".to_string(),
        format!("http://{}", backend),
    )
    .await;

    let res = Client::new()
        .post(format!("http://{}", addr))
        .bearer_auth(token)
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getAccountInfo",
            "params": [ata_pubkey]
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(res.status(), 200);
}

/// A user querying an ATA whose delegate field matches one of their verified
/// wallets must be allowed through (even if the owner does not match).
#[tokio::test(flavor = "multi_thread")]
async fn test_get_account_info_ata_delegated_to_user_wallet_is_proxied() {
    let (pool, _url, _container) = start_postgres().await;
    db::init_schema(&pool).await.unwrap();

    let delegate_bytes = [9u8; 32];
    let delegate_pubkey = bs58::encode(delegate_bytes).into_string();
    let unrelated_owner = [10u8; 32]; // owner is someone else

    let user_id = insert_user(&pool, "user").await;
    insert_wallet(&pool, user_id, &delegate_pubkey).await;
    let token = generate_token(user_id, "user");

    let ata_pubkey = bs58::encode([11u8; 32]).into_string();
    let backend = start_mock_backend_with_body(token_account_response(
        &unrelated_owner,
        Some(&delegate_bytes),
    ))
    .await;
    let addr = start_gateway(
        pool,
        "http://127.0.0.1:1".to_string(),
        format!("http://{}", backend),
    )
    .await;

    let res = Client::new()
        .post(format!("http://{}", addr))
        .bearer_auth(token)
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getAccountInfo",
            "params": [ata_pubkey]
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(res.status(), 200);
}

/// A user querying an ATA whose owner and delegate do not match any of their
/// verified wallets must be denied with 403.
#[tokio::test(flavor = "multi_thread")]
async fn test_get_account_info_ata_unowned_returns_403() {
    let (pool, _url, _container) = start_postgres().await;
    db::init_schema(&pool).await.unwrap();

    let user_wallet_bytes = [12u8; 32];
    let user_wallet_pubkey = bs58::encode(user_wallet_bytes).into_string();
    let unrelated_owner = [13u8; 32]; // ATA is owned by someone else, no delegate

    let user_id = insert_user(&pool, "user").await;
    insert_wallet(&pool, user_id, &user_wallet_pubkey).await;
    let token = generate_token(user_id, "user");

    let ata_pubkey = bs58::encode([14u8; 32]).into_string();
    let backend =
        start_mock_backend_with_body(token_account_response(&unrelated_owner, None)).await;
    let addr = start_gateway(
        pool,
        "http://127.0.0.1:1".to_string(),
        format!("http://{}", backend),
    )
    .await;

    let res = Client::new()
        .post(format!("http://{}", addr))
        .bearer_auth(token)
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getAccountInfo",
            "params": [ata_pubkey]
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(res.status(), 403);
}

/// A user querying a mint account (SPL Token program, 82 bytes) must be denied
/// with 403 — mints are not user accounts and should not be readable this way.
#[tokio::test(flavor = "multi_thread")]
async fn test_get_account_info_mint_returns_403() {
    let (pool, _url, _container) = start_postgres().await;
    db::init_schema(&pool).await.unwrap();

    let user_id = insert_user(&pool, "user").await;
    let token = generate_token(user_id, "user");

    let backend = start_mock_backend_with_body(mint_account_response()).await;
    let addr = start_gateway(
        pool,
        "http://127.0.0.1:1".to_string(),
        format!("http://{}", backend),
    )
    .await;

    let res = Client::new()
        .post(format!("http://{}", addr))
        .bearer_auth(token)
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getAccountInfo",
            "params": [bs58::encode([15u8; 32]).into_string()]
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(res.status(), 403);
}

/// A user querying an account that does not exist on-chain must be denied with
/// 403 — we cannot verify ownership of a non-existent account.
#[tokio::test(flavor = "multi_thread")]
async fn test_get_account_info_nonexistent_account_returns_403() {
    let (pool, _url, _container) = start_postgres().await;
    db::init_schema(&pool).await.unwrap();

    let user_id = insert_user(&pool, "user").await;
    let token = generate_token(user_id, "user");

    let backend = start_mock_backend_with_body(null_account_response()).await;
    let addr = start_gateway(
        pool,
        "http://127.0.0.1:1".to_string(),
        format!("http://{}", backend),
    )
    .await;

    let res = Client::new()
        .post(format!("http://{}", addr))
        .bearer_auth(token)
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getAccountInfo",
            "params": [bs58::encode([16u8; 32]).into_string()]
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(res.status(), 403);
}

/// An Operator JWT can call `getAccountInfo` for any pubkey, regardless of
/// whether it is in their verified wallets.
///
/// Operators have unrestricted read access — they are the admins of the system.
#[tokio::test(flavor = "multi_thread")]
async fn test_get_account_info_operator_bypasses_ownership_check() {
    let (pool, _url, _container) = start_postgres().await;
    db::init_schema(&pool).await.unwrap();

    // Operator user with NO registered wallets — ownership check must be skipped.
    let operator_id = insert_user(&pool, "operator").await;
    let token = generate_token(operator_id, "operator");

    let backend = start_mock_backend_with_body(system_account_response()).await;
    let addr = start_gateway(
        pool,
        "http://127.0.0.1:1".to_string(),
        format!("http://{}", backend),
    )
    .await;

    let res = Client::new()
        .post(format!("http://{}", addr))
        .bearer_auth(token)
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getAccountInfo",
            "params": ["So11111111111111111111111111111111111111112"]
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(res.status(), 200);
}

/// `getTokenAccountBalance` is gated by the same ownership rules as
/// `getAccountInfo`. A user may only call it for an ATA they own.
#[tokio::test(flavor = "multi_thread")]
async fn test_get_token_account_balance_gated_by_ownership() {
    let (pool, _url, _container) = start_postgres().await;
    db::init_schema(&pool).await.unwrap();

    let wallet_bytes = [17u8; 32];
    let wallet_pubkey = bs58::encode(wallet_bytes).into_string();
    let user_id = insert_user(&pool, "user").await;
    insert_wallet(&pool, user_id, &wallet_pubkey).await;
    let token = generate_token(user_id, "user");

    let ata_pubkey = bs58::encode([18u8; 32]).into_string();

    // Owned ATA → 200.
    let backend = start_mock_backend_with_body(token_account_response(&wallet_bytes, None)).await;
    let addr = start_gateway(
        pool.clone(),
        "http://127.0.0.1:1".to_string(),
        format!("http://{}", backend),
    )
    .await;

    let res = Client::new()
        .post(format!("http://{}", addr))
        .bearer_auth(&token)
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTokenAccountBalance",
            "params": [ata_pubkey]
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(res.status(), 200);

    // Unowned ATA → 403.
    let unrelated_owner = [19u8; 32];
    let backend2 =
        start_mock_backend_with_body(token_account_response(&unrelated_owner, None)).await;
    let addr2 = start_gateway(
        pool,
        "http://127.0.0.1:1".to_string(),
        format!("http://{}", backend2),
    )
    .await;

    let res2 = Client::new()
        .post(format!("http://{}", addr2))
        .bearer_auth(&token)
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTokenAccountBalance",
            "params": [bs58::encode([20u8; 32]).into_string()]
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(res2.status(), 403);
}

/// An expired operator JWT must be rejected with 401 on operator-only methods.
/// Verifies that token expiry is validated before the role check.
#[tokio::test(flavor = "multi_thread")]
async fn test_operator_only_method_expired_token_returns_401() {
    let (pool, _url, _container) = start_postgres().await;
    db::init_schema(&pool).await.unwrap();

    let operator_id = insert_user(&pool, "operator").await;
    let expired_token = generate_expired_token(operator_id, "operator");

    let generic_response = json!({"jsonrpc":"2.0","id":1,"result":null}).to_string();
    let backend = start_mock_backend_with_body(generic_response).await;
    let addr = start_gateway(
        pool,
        "http://127.0.0.1:1".to_string(),
        format!("http://{}", backend),
    )
    .await;

    let res = Client::new()
        .post(format!("http://{}", addr))
        .bearer_auth(expired_token)
        .json(&json!({"jsonrpc":"2.0","id":1,"method":"getBlock","params":[]}))
        .send()
        .await
        .unwrap();

    assert_eq!(res.status(), 401);
}

/// A Token-2022 account with extensions (data > 165 bytes) must be treated the
/// same as a standard SPL token account — owner/delegate check at fixed offsets.
#[tokio::test(flavor = "multi_thread")]
async fn test_get_account_info_token2022_extended_account_is_proxied() {
    let (pool, _url, _container) = start_postgres().await;
    db::init_schema(&pool).await.unwrap();

    let wallet_bytes = [21u8; 32];
    let wallet_pubkey = bs58::encode(wallet_bytes).into_string();
    let user_id = insert_user(&pool, "user").await;
    insert_wallet(&pool, user_id, &wallet_pubkey).await;
    let token = generate_token(user_id, "user");

    // Build a Token-2022 account: standard 165-byte layout plus 50 bytes of
    // extension data. Owner field at bytes 32-63 matches the user's wallet.
    let mut data = vec![0u8; 215];
    data[32..64].copy_from_slice(&wallet_bytes);
    let encoded = BASE64.encode(&data);
    let mock_body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": {
            "value": {
                "lamports": 2_039_280,
                "owner": "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",
                "data": [encoded, "base64"],
                "executable": false,
                "rentEpoch": 0
            }
        }
    })
    .to_string();

    let ata_pubkey = bs58::encode([22u8; 32]).into_string();
    let backend = start_mock_backend_with_body(mock_body).await;
    let addr = start_gateway(
        pool,
        "http://127.0.0.1:1".to_string(),
        format!("http://{}", backend),
    )
    .await;

    let res = Client::new()
        .post(format!("http://{}", addr))
        .bearer_auth(token)
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getAccountInfo",
            "params": [ata_pubkey]
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(res.status(), 200);
}

/// `getBlock`, `getTransaction`, and `simulateTransaction` are operator-only.
///
/// - No token → 401
/// - Valid User JWT → 403
/// - Valid Operator JWT → 200 (proxied)
#[tokio::test(flavor = "multi_thread")]
async fn test_operator_only_methods_enforce_role() {
    let (pool, _url, _container) = start_postgres().await;
    db::init_schema(&pool).await.unwrap();

    let user_id = insert_user(&pool, "user").await;
    let operator_id = insert_user(&pool, "operator").await;
    let user_token = generate_token(user_id, "user");
    let operator_token = generate_token(operator_id, "operator");

    let generic_response = json!({"jsonrpc":"2.0","id":1,"result":null}).to_string();
    let backend = start_mock_backend_with_body(generic_response).await;
    let addr = start_gateway(
        pool,
        "http://127.0.0.1:1".to_string(),
        format!("http://{}", backend),
    )
    .await;

    let client = Client::new();

    for method in &["getBlock", "getTransaction", "simulateTransaction"] {
        let payload = json!({ "jsonrpc": "2.0", "id": 1, "method": method, "params": [] });

        // No token → 401.
        let res = client
            .post(format!("http://{}", addr))
            .json(&payload)
            .send()
            .await
            .unwrap();
        assert_eq!(
            res.status(),
            401,
            "{method}: expected 401 for missing token"
        );

        // User role → 403.
        let res = client
            .post(format!("http://{}", addr))
            .bearer_auth(&user_token)
            .json(&payload)
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 403, "{method}: expected 403 for user role");

        // Operator role → 200.
        let res = client
            .post(format!("http://{}", addr))
            .bearer_auth(&operator_token)
            .json(&payload)
            .send()
            .await
            .unwrap();
        assert_eq!(
            res.status(),
            200,
            "{method}: expected 200 for operator role"
        );
    }
}

/// Public methods like `sendTransaction` and `getSlot` must pass through
/// without any JWT, since they don't expose participant data.
///
/// This ensures auth enforcement is additive — it does not accidentally break
/// unauthenticated access to methods that were always public.
///
/// Both a read and a write mock backend are started because `sendTransaction`
/// routes to the write node while the others route to the read node.
#[tokio::test(flavor = "multi_thread")]
async fn test_public_methods_pass_through_without_token() {
    let (pool, _url, _container) = start_postgres().await;
    db::init_schema(&pool).await.unwrap();

    let generic_response = json!({"jsonrpc":"2.0","id":1,"result":"ok"}).to_string();
    let read_backend = start_mock_backend_with_body(generic_response.clone()).await;
    let write_backend = start_mock_backend_with_body(generic_response).await;

    let addr = start_gateway(
        pool,
        format!("http://{}", write_backend),
        format!("http://{}", read_backend),
    )
    .await;

    let client = Client::new();

    for method in &["sendTransaction", "getSlot", "getLatestBlockhash"] {
        let res = client
            .post(format!("http://{}", addr))
            .json(&json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": method,
            }))
            .send()
            .await
            .unwrap();

        assert_eq!(
            res.status(),
            200,
            "expected public method '{}' to be proxied without a token",
            method
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_empty_jwt_secret_disables_auth() {
    // A gateway configured with JWT_SECRET="" and a real auth DB should still disable
    // auth enforcement — the empty string must be treated as "not set".
    // Passing a real DB ensures the test isn't passing just because auth_db is None.
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .ok();

    let (auth_db, _, _container) = start_postgres().await;
    db::init_schema(&auth_db)
        .await
        .expect("failed to init schema");

    // Spin up a minimal mock backend that always returns 200.
    let read_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let read_backend = read_listener.local_addr().unwrap();
    tokio::spawn(async move {
        if let Ok((mut stream, _)) = read_listener.accept().await {
            let mut buf = vec![0u8; 4096];
            let _ = stream.read(&mut buf).await;
            let body = r#"{"jsonrpc":"2.0","id":1,"result":{"value":null}}"#;
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = stream.write_all(resp.as_bytes()).await;
        }
    });

    let gateway = Arc::new(Gateway::new(
        "http://127.0.0.1:1".to_string(),
        format!("http://{}", read_backend),
        "*".to_string(),
        Some("".to_string()), // empty string — must be treated as "not set"
        Some(auth_db),
    ));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = serve(listener, gateway).await;
    });

    // A gated method with no token should get 200 (auth disabled, proxied to backend).
    let res = Client::new()
        .post(format!("http://{}", addr))
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getAccountInfo",
            "params": ["SomePubkey"]
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(
        res.status(),
        200,
        "empty JWT_SECRET should disable auth enforcement"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_whitespace_jwt_secret_disables_auth() {
    // A whitespace-only JWT_SECRET must be treated as "not set", disabling enforcement
    // rather than enabling RBAC with a key the auth service would reject at startup.
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .ok();

    let (auth_db, _, _container) = start_postgres().await;
    db::init_schema(&auth_db)
        .await
        .expect("failed to init schema");

    // Spin up a minimal mock backend that always returns 200.
    let read_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let read_backend = read_listener.local_addr().unwrap();
    tokio::spawn(async move {
        if let Ok((mut stream, _)) = read_listener.accept().await {
            let mut buf = vec![0u8; 4096];
            let _ = stream.read(&mut buf).await;
            let body = r#"{"jsonrpc":"2.0","id":1,"result":{"value":null}}"#;
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = stream.write_all(resp.as_bytes()).await;
        }
    });

    let gateway = Arc::new(Gateway::new(
        "http://127.0.0.1:1".to_string(),
        format!("http://{}", read_backend),
        "*".to_string(),
        Some("   ".to_string()), // whitespace-only — must be treated as "not set"
        Some(auth_db),
    ));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = serve(listener, gateway).await;
    });

    // A gated method with no token should get 200 (auth disabled, proxied to backend).
    let res = Client::new()
        .post(format!("http://{}", addr))
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getAccountInfo",
            "params": ["SomePubkey"]
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(
        res.status(),
        200,
        "whitespace-only JWT_SECRET should disable auth enforcement"
    );
}

/// `getSignaturesForAddress` with no token must return 401.
#[tokio::test(flavor = "multi_thread")]
async fn test_get_signatures_for_address_no_token_returns_401() {
    let (pool, _url, _container) = start_postgres().await;
    db::init_schema(&pool).await.unwrap();

    let generic_response = json!({"jsonrpc":"2.0","id":1,"result":[]}).to_string();
    let backend = start_mock_backend_with_body(generic_response).await;
    let addr = start_gateway(
        pool,
        "http://127.0.0.1:1".to_string(),
        format!("http://{}", backend),
    )
    .await;

    let res = Client::new()
        .post(format!("http://{}", addr))
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSignaturesForAddress",
            "params": ["So11111111111111111111111111111111111111112"]
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(res.status(), 401);
}

/// `getSignaturesForAddress` with a User JWT for an address they own must be proxied.
#[tokio::test(flavor = "multi_thread")]
async fn test_get_signatures_for_address_owned_wallet_is_proxied() {
    let (pool, _url, _container) = start_postgres().await;
    db::init_schema(&pool).await.unwrap();

    let pubkey = "So11111111111111111111111111111111111111112";
    let user_id = insert_user(&pool, "user").await;
    insert_wallet(&pool, user_id, pubkey).await;
    let token = generate_token(user_id, "user");

    // The backend is called twice: first to fetch account data for ownership
    // check (Phase 2), then to proxy the actual getSignaturesForAddress request.
    let backend = start_mock_backend_with_body(system_account_response()).await;
    let addr = start_gateway(
        pool,
        "http://127.0.0.1:1".to_string(),
        format!("http://{}", backend),
    )
    .await;

    let res = Client::new()
        .post(format!("http://{}", addr))
        .bearer_auth(token)
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSignaturesForAddress",
            "params": [pubkey]
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(res.status(), 200);
}

/// `getSignaturesForAddress` with a User JWT for an address they do not own must return 403.
#[tokio::test(flavor = "multi_thread")]
async fn test_get_signatures_for_address_unowned_address_returns_403() {
    let (pool, _url, _container) = start_postgres().await;
    db::init_schema(&pool).await.unwrap();

    let user_id = insert_user(&pool, "user").await;
    let token = generate_token(user_id, "user");

    // User has no registered wallets — ownership check will fail.
    let backend = start_mock_backend_with_body(system_account_response()).await;
    let addr = start_gateway(
        pool,
        "http://127.0.0.1:1".to_string(),
        format!("http://{}", backend),
    )
    .await;

    let res = Client::new()
        .post(format!("http://{}", addr))
        .bearer_auth(token)
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSignaturesForAddress",
            "params": ["So11111111111111111111111111111111111111112"]
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(res.status(), 403);
}

/// `getSignaturesForAddress` with an Operator JWT must be proxied for any address.
#[tokio::test(flavor = "multi_thread")]
async fn test_get_signatures_for_address_operator_is_proxied() {
    let (pool, _url, _container) = start_postgres().await;
    db::init_schema(&pool).await.unwrap();

    let operator_id = insert_user(&pool, "operator").await;
    let token = generate_token(operator_id, "operator");

    let generic_response = json!({"jsonrpc":"2.0","id":1,"result":[]}).to_string();
    let backend = start_mock_backend_with_body(generic_response).await;
    let addr = start_gateway(
        pool,
        "http://127.0.0.1:1".to_string(),
        format!("http://{}", backend),
    )
    .await;

    let res = Client::new()
        .post(format!("http://{}", addr))
        .bearer_auth(token)
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSignaturesForAddress",
            "params": ["So11111111111111111111111111111111111111112"]
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(res.status(), 200);
}
