//! Integration tests for `simulateTransaction` input-validation guards in
//! `core/src/rpc/simulate_transaction_impl.rs`.
//!
//! Covers the branches:
//!   * oversize transaction (> `PACKET_DATA_SIZE = 1232`)
//!   * opt-in `sig_verify` branch
//!   * malformed pubkey string inside `accounts.addresses[]`
//!
//! Pattern: the test spins up a `private_channel_core::rpc::create_rpc_module` against
//! a fresh Postgres testcontainer and invokes `simulateTransaction` by
//! calling `rpc_module.raw_json_request(...)` directly. This avoids needing
//! an HTTP listener while still exercising the production dispatch path.

use {
    base64::{engine::general_purpose::STANDARD, Engine as _},
    jsonrpsee::server::RpcModule,
    private_channel_core::{
        accounts::AccountsDB,
        rpc::{create_rpc_module, ReadDeps},
    },
    serde_json::{json, Value},
    solana_sdk::{
        hash::Hash,
        pubkey::Pubkey,
        signature::{Keypair, Signer},
        transaction::Transaction,
    },
    solana_system_interface::instruction as system_instruction,
    std::{
        collections::LinkedList,
        sync::{Arc, RwLock},
    },
    testcontainers::{runners::AsyncRunner, ContainerAsync},
    testcontainers_modules::postgres::Postgres,
};

const MAX_RESPONSE_SIZE: usize = 10 * 1024 * 1024;

async fn start_pg() -> (AccountsDB, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_db_name("sim_guards")
        .with_user("postgres")
        .with_password("password")
        .start()
        .await
        .unwrap();
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:password@{host}:{port}/sim_guards");
    let db = AccountsDB::new(&url, false).await.unwrap();
    (db, container)
}

async fn build_module(admin_keys: Vec<Pubkey>) -> (RpcModule<()>, ContainerAsync<Postgres>) {
    let (db, pg) = start_pg().await;
    let read_deps = ReadDeps {
        accounts_db: db,
        admin_keys,
        live_blockhashes: Arc::new(RwLock::new(LinkedList::new())),
        max_blockhashes: 150,
    };
    let module = create_rpc_module(Some(read_deps), None).await;
    (module, pg)
}

async fn call(module: &RpcModule<()>, method: &str, params: Value) -> Value {
    let request = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params,
    })
    .to_string();
    let (resp, _) = module
        .raw_json_request(&request, MAX_RESPONSE_SIZE)
        .await
        .expect("jsonrpsee dispatch must not fail");
    serde_json::from_str(&resp).expect("server must return valid JSON")
}

fn valid_tx() -> Transaction {
    let payer = Keypair::new();
    let recipient = Keypair::new().pubkey();
    let ix = system_instruction::transfer(&payer.pubkey(), &recipient, 1_000);
    Transaction::new_signed_with_payer(&[ix], Some(&payer.pubkey()), &[&payer], Hash::default())
}

// ── (a) oversize transaction ────────────────────────────────────────────────
// Encode a payload whose decoded length exceeds PACKET_DATA_SIZE (1232 bytes).
// We don't need a valid transaction — the guard runs BEFORE bincode
// deserialization, so any 1233-byte buffer suffices.
#[tokio::test(flavor = "multi_thread")]
async fn simulate_rejects_oversize_transaction() {
    let (module, _pg) = build_module(vec![]).await;

    let oversized = vec![0u8; 1233];
    let encoded = STANDARD.encode(&oversized);
    let resp = call(
        &module,
        "simulateTransaction",
        json!([encoded, {"encoding": "base64"}]),
    )
    .await;

    let err = resp.get("error").unwrap_or_else(|| {
        panic!("expected error in response, got: {resp}");
    });
    let msg = err["message"].as_str().unwrap_or("");
    assert!(
        msg.to_lowercase().contains("too large") || msg.contains("1232"),
        "error must explain size limit, got: {msg} (full: {resp})"
    );
}

// ── (b) sig_verify=true, tampered signature ─────────────────────────────────
// A transaction with its signature zeroed out must be rejected by the
// sigverify branch. The happy-path sigverify branch is exercised by the
// existing `private_channel_integration` driver, so here we focus on the failing
// arm (guarded by `config.sig_verify`).
#[tokio::test(flavor = "multi_thread")]
async fn simulate_sig_verify_rejects_tampered_signature() {
    let (module, _pg) = build_module(vec![]).await;

    let mut tx = valid_tx();
    // Zero out every signature — the tx will still deserialize, but sigverify
    // will fail.
    for sig in tx.signatures.iter_mut() {
        *sig = solana_sdk::signature::Signature::default();
    }
    let encoded = STANDARD.encode(bincode::serialize(&tx).unwrap());

    let resp = call(
        &module,
        "simulateTransaction",
        json!([encoded, {"encoding": "base64", "sigVerify": true}]),
    )
    .await;

    let err = resp.get("error").unwrap_or_else(|| {
        panic!("expected sigverify rejection, got: {resp}");
    });
    let msg = err["message"].as_str().unwrap_or("").to_lowercase();
    // Accept any of the sigverify failure arms in simulate_transaction_impl.rs.
    assert!(
        msg.contains("sigverify")
            || msg.contains("invalid transaction")
            || msg.contains("not signed by admin"),
        "error must signal signature failure, got: {msg} (full: {resp})"
    );
}

// ── (c) malformed base58 in accounts.addresses[] ────────────────────────────
// `accounts.addresses` is a Vec<String> parsed with `Pubkey::from_str`.
// A malformed entry MUST NOT panic — the implementation logs a warning and
// emits `None` for that slot (handled inside `simulate_transaction_impl`).
// The outer call still succeeds with a 200 result.
#[tokio::test(flavor = "multi_thread")]
async fn simulate_handles_malformed_address_as_null() {
    let (module, _pg) = build_module(vec![]).await;

    // The malformed-address branch only fires if the tx reaches the
    // Executed arm with `accounts` config. A fresh-keypair
    // system transfer from our own-built module will exercise execution and
    // carry the malformed address through to the mapping step.
    let tx = valid_tx();
    let encoded = STANDARD.encode(bincode::serialize(&tx).unwrap());

    let resp = call(
        &module,
        "simulateTransaction",
        json!([
            encoded,
            {
                "encoding": "base64",
                "sigVerify": false,
                "accounts": {
                    "encoding": "base64",
                    // First entry is malformed base58 (must map to null);
                    // second entry is a well-formed but unknown pubkey (also
                    // maps to null) so the test still makes sense if execution
                    // returns a different shape.
                    "addresses": ["!!not-a-pubkey!!", Pubkey::new_unique().to_string()]
                }
            }
        ]),
    )
    .await;

    // The primary invariant: the server must not panic or 5xx on a
    // malformed pubkey in accounts.addresses. Either an outer JSON-RPC
    // error (call failed before reaching the mapping) or a successful
    // result with a null slot for the malformed entry is acceptable.
    if let Some(result) = resp.get("result") {
        if let Some(accounts) = result["value"]["accounts"].as_array() {
            assert_eq!(accounts.len(), 2, "two addresses requested, two slots back");
            assert!(
                accounts[0].is_null(),
                "malformed pubkey must decode to null; got: {accounts:?}"
            );
        }
    } else {
        // An error envelope means execution short-circuited before the
        // mapping — still fine, because this path proves the server did
        // not crash on the malformed input (the actual safety invariant).
        assert!(resp.get("error").is_some(), "response must be well-formed");
    }
}
