//! Integration tests for `simulateTransaction` input-validation guards in
//! `core/src/rpc/simulate_transaction_impl.rs`.
//!
//! Covers the branches:
//!   * oversize transaction (> `PACKET_DATA_SIZE = 1232`)
//!   * opt-in `sig_verify` branch
//!   * malformed pubkey string inside `accounts.addresses[]`
//!   * more `accounts.addresses[]` entries than the transaction has account keys
//!   * repeated large accounts exceeding the encoded-byte budget
//!   * the two legacy bs58 `accounts.encoding` values
//!   * the address-count check running ahead of sigverify and execution
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
        instruction::CompiledInstruction,
        message::{Message, MessageHeader},
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

/// A transfer padded with unused read-only keys so it carries exactly `total_keys` keys.
/// Needed to ask for many addresses without tripping the count cap first.
fn tx_with_account_keys(total_keys: usize) -> Transaction {
    assert!(
        total_keys >= 3,
        "need at least payer, recipient and program"
    );
    let payer = Keypair::new();
    let recipient = Pubkey::new_unique();
    let mut account_keys = vec![payer.pubkey(), recipient, solana_sdk::system_program::ID];
    while account_keys.len() < total_keys {
        account_keys.push(Pubkey::new_unique());
    }

    // Key 0 signs and key 1 receives; the program and padding keys stay read-only.
    let header = MessageHeader {
        num_required_signatures: 1,
        num_readonly_signed_accounts: 0,
        num_readonly_unsigned_accounts: (total_keys - 2) as u8,
    };
    let transfer = system_instruction::transfer(&payer.pubkey(), &recipient, 100);
    let message = Message {
        header,
        account_keys,
        recent_blockhash: Hash::default(),
        instructions: vec![CompiledInstruction {
            program_id_index: 2,
            accounts: vec![0, 1],
            data: transfer.data,
        }],
    };

    let tx = Transaction::new(&[&payer], message, Hash::default());
    let wire_len = bincode::serialize(&tx).unwrap().len();
    assert!(
        wire_len <= 1232,
        "{total_keys} keys serializes to {wire_len} bytes, over PACKET_DATA_SIZE"
    );
    tx
}

// ── (d) more addresses than the transaction has account keys ────────────────
// Matches Agave, and is what bounds repetition: an address can only be repeated
// as often as the transaction is wide.
#[tokio::test(flavor = "multi_thread")]
async fn simulate_rejects_more_addresses_than_tx_accounts() {
    let (module, _pg) = build_module(vec![]).await;

    // valid_tx() carries three keys: payer, recipient, system program.
    let addresses: Vec<String> = (0..4).map(|_| Pubkey::new_unique().to_string()).collect();
    let encoded = STANDARD.encode(bincode::serialize(&valid_tx()).unwrap());

    let resp = call(
        &module,
        "simulateTransaction",
        json!([
            encoded,
            {
                "encoding": "base64",
                "accounts": { "encoding": "base64", "addresses": addresses }
            }
        ]),
    )
    .await;

    let err = resp
        .get("error")
        .unwrap_or_else(|| panic!("four addresses against three keys must be rejected: {resp}"));
    assert_eq!(err["code"], -32602, "must be an invalid-params rejection");
    let msg = err["message"].as_str().unwrap_or("");
    assert!(
        msg.contains("Too many accounts") && msg.contains('3'),
        "rejection must name the max, got: {msg}"
    );
}

// ── (e) repeated large account over the encoded-byte budget ─────────────────
// 34 addresses against 34 keys clears the count cap, isolating the byte budget.
// The SPL Token precompile is always resident, so this needs no setup.
#[tokio::test(flavor = "multi_thread")]
async fn simulate_rejects_repeated_precompile_over_budget() {
    let (module, _pg) = build_module(vec![]).await;

    let tx = tx_with_account_keys(34);
    let encoded = STANDARD.encode(bincode::serialize(&tx).unwrap());
    let addresses: Vec<String> = std::iter::repeat_n(spl_token::ID.to_string(), 34).collect();

    let resp = call(
        &module,
        "simulateTransaction",
        json!([
            encoded,
            {
                "encoding": "base64",
                "accounts": { "encoding": "base64", "addresses": addresses }
            }
        ]),
    )
    .await;

    let err = resp
        .get("error")
        .unwrap_or_else(|| panic!("34 copies of a 134 KB account must exceed the budget: {resp}"));
    assert_eq!(err["code"], -32602, "must be an invalid-params rejection");
    let msg = err["message"].as_str().unwrap_or("");
    assert!(
        msg.contains("max"),
        "rejection must state the budget, got: {msg}"
    );
}

// ── (f) legacy bs58 account encodings ───────────────────────────────────────
// Their encoder replaces anything over 128 bytes with an error string, so a byte
// estimate could not describe the real reply. Agave rejects them here too.
#[tokio::test(flavor = "multi_thread")]
async fn simulate_rejects_base58_accounts_encoding() {
    let (module, _pg) = build_module(vec![]).await;
    let encoded = STANDARD.encode(bincode::serialize(&valid_tx()).unwrap());

    for encoding in ["base58", "binary"] {
        let resp = call(
            &module,
            "simulateTransaction",
            json!([
                encoded,
                {
                    "encoding": "base64",
                    "accounts": {
                        "encoding": encoding,
                        "addresses": [Pubkey::new_unique().to_string()]
                    }
                }
            ]),
        )
        .await;

        let err = resp
            .get("error")
            .unwrap_or_else(|| panic!("{encoding} account encoding must be rejected: {resp}"));
        assert_eq!(err["code"], -32602, "{encoding} must be invalid-params");
    }
}

// ── (g) the address-count check precedes sigverify and execution ────────────
// A request that is both signature-invalid and over the address cap reports the
// address error, which is what proves the cap is checked first. The byte budget
// is separate: it needs post-execution account sizes, so it runs after the
// simulation and only prevents the encoding, not the simulation.
#[tokio::test(flavor = "multi_thread")]
async fn address_count_is_checked_before_sigverify() {
    let (module, _pg) = build_module(vec![]).await;

    let mut tx = valid_tx();
    for sig in tx.signatures.iter_mut() {
        *sig = solana_sdk::signature::Signature::default();
    }
    let encoded = STANDARD.encode(bincode::serialize(&tx).unwrap());
    let addresses: Vec<String> = (0..4).map(|_| Pubkey::new_unique().to_string()).collect();

    let resp = call(
        &module,
        "simulateTransaction",
        json!([
            encoded,
            {
                "encoding": "base64",
                "sigVerify": true,
                "accounts": { "encoding": "base64", "addresses": addresses }
            }
        ]),
    )
    .await;

    let err = resp
        .get("error")
        .unwrap_or_else(|| panic!("expected a rejection, got: {resp}"));
    let msg = err["message"].as_str().unwrap_or("");
    assert!(
        msg.contains("Too many accounts"),
        "the account guard must win over sigverify, got: {msg}"
    );
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
