//! One place to build a `SenderState` for tests.
//!
//! Building it in one place keeps a new field from meaning a dozen edited call
//! sites.

use super::types::{InFlightQueue, SenderState, MAX_IN_FLIGHT};
use crate::config::ProgramType;
use crate::operator::utils::account_util::bitmap_account_bytes;
use crate::operator::utils::rpc_util::{RetryConfig, RpcClientWithRetry};
use crate::operator::MintCache;
use crate::storage::common::amount::TokenAmount;
use crate::storage::common::models::{DbTransaction, TransactionStatus, TransactionType};
use crate::storage::common::storage::mock::MockStorage;
use crate::storage::Storage;
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use solana_commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use spl_token::solana_program::program_pack::Pack;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::Once;
use std::time::Duration;
use tokio::sync::Semaphore;

/// Install an in-memory admin signer once per test process.
///
/// Anything that builds a real instruction reaches `SignerUtil`, which panics
/// when no signer is configured. The key itself is never checked, so a fresh
/// throwaway keypair is enough to let those paths run.
pub(super) fn ensure_test_signer() {
    static INIT_TEST_SIGNER: Once = Once::new();
    INIT_TEST_SIGNER.call_once(|| {
        let keypair = solana_sdk::signer::keypair::Keypair::new();
        std::env::set_var("ADMIN_SIGNER", "memory");
        std::env::set_var(
            "ADMIN_PRIVATE_KEY",
            bs58::encode(keypair.to_bytes()).into_string(),
        );
    });
}

/// A `SenderState` pointed at `rpc_url`, with mock storage and no instance.
pub(super) fn sender_state(rpc_url: &str) -> SenderState {
    sender_state_with_storage(rpc_url, MockStorage::new())
}

/// Same, but with a caller-prepared `MockStorage` so a test can seed rows or
/// arm a simulated failure before the state is built.
pub(super) fn sender_state_with_storage(rpc_url: &str, mock: MockStorage) -> SenderState {
    sender_state_with_storage_and_role(rpc_url, mock, ProgramType::Escrow)
}

/// Same, with the operator role chosen by the caller. Role-gated paths need a
/// state on both sides of the gate.
pub(super) fn sender_state_with_storage_and_role(
    rpc_url: &str,
    mock: MockStorage,
    program_type: ProgramType,
) -> SenderState {
    let storage = Arc::new(Storage::Mock(mock));
    // One attempt with negligible backoff: tests that expect an RPC failure
    // should not pay the production retry schedule for it.
    let rpc_client = Arc::new(RpcClientWithRetry::with_retry_config(
        rpc_url.to_string(),
        RetryConfig {
            max_attempts: 1,
            base_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(1),
        },
        CommitmentConfig::confirmed(),
    ));

    SenderState {
        rpc_client: rpc_client.clone(),
        source_rpc_client: rpc_client,
        fallback_rpc_client: None,
        storage: storage.clone(),
        instance_pda: None,
        in_flight_withdrawals: HashSet::new(),
        cached_generation: None,
        retry_counts: HashMap::new(),
        rotation_retry_attempts: 0,
        rotation_in_flight: None,
        rotation_bound_generation: None,
        rotation_rearm_attempts: 0,
        rotation_blocked_passes: 0,
        mint_builders: HashMap::new(),
        mint_cache: MintCache::new(storage),
        retry_max_attempts: 3,
        confirmation_poll_interval_ms: 1,
        rotation_retry_queue: Vec::new(),
        pending_rotation: None,
        program_type,
        remint_cache: HashMap::new(),
        pending_signatures: HashMap::new(),
        release_leases: HashMap::new(),
        pending_remints: Vec::new(),
        in_flight: InFlightQueue::new(),
        semaphore: Arc::new(Semaphore::new(MAX_IN_FLIGHT)),
    }
}

/// A mock holding one `Processing` withdrawal row, which is the state every
/// release the sender may park or unpark starts from.
pub(super) fn mock_with_processing_row(transaction_id: i64) -> MockStorage {
    let mock = MockStorage::new();
    push_processing_row(&mock, transaction_id);
    mock
}

/// Add another `Processing` withdrawal row to an existing mock.
pub(super) fn push_processing_row(mock: &MockStorage, transaction_id: i64) {
    mock.pending_transactions
        .lock()
        .unwrap()
        .push(withdrawal_row(
            transaction_id,
            TransactionStatus::Processing,
        ));
}

/// Add a `Processing` deposit row, the state a mint the sender owns starts from.
pub(super) fn push_processing_deposit_row(mock: &MockStorage, transaction_id: i64) {
    let mut row = withdrawal_row(transaction_id, TransactionStatus::Processing);
    row.transaction_type = TransactionType::Deposit;
    row.withdrawal_nonce = None;
    mock.pending_transactions.lock().unwrap().push(row);
}

/// A mock holding one already-`Parked` withdrawal row, the state the drain
/// expects to find when it comes back for a queued release.
pub(super) fn mock_with_parked_row(transaction_id: i64) -> MockStorage {
    let mock = MockStorage::new();
    mock.pending_transactions
        .lock()
        .unwrap()
        .push(withdrawal_row(transaction_id, TransactionStatus::Parked));
    mock
}

/// Seed a withdrawal that carries `nonce`, which is what the rotation gate reads.
pub(super) fn push_withdrawal_with_nonce(
    mock: &MockStorage,
    transaction_id: i64,
    nonce: i64,
    status: TransactionStatus,
) {
    let mut row = withdrawal_row(transaction_id, status);
    row.withdrawal_nonce = Some(nonce);
    mock.pending_transactions.lock().unwrap().push(row);
}

/// The status of `transaction_id` in `mock`, or `None` if it holds no such row.
pub(super) fn row_status(mock: &MockStorage, transaction_id: i64) -> Option<TransactionStatus> {
    mock.pending_transactions
        .lock()
        .unwrap()
        .iter()
        .find(|txn| txn.id == transaction_id)
        .map(|txn| txn.status)
}

/// When `transaction_id` was last written, which is what the park heartbeat
/// refreshes and the recovery sweep ages out.
pub(super) fn row_updated_at(
    mock: &MockStorage,
    transaction_id: i64,
) -> Option<chrono::DateTime<chrono::Utc>> {
    mock.pending_transactions
        .lock()
        .unwrap()
        .iter()
        .find(|txn| txn.id == transaction_id)
        .map(|txn| txn.updated_at)
}

fn withdrawal_row(id: i64, status: TransactionStatus) -> DbTransaction {
    let now = chrono::Utc::now();
    DbTransaction {
        id,
        signature: format!("sig-{id}"),
        instruction_index: 0,
        trace_id: format!("trace-{id}"),
        slot: 100,
        initiator: Pubkey::new_unique().to_string(),
        recipient: Pubkey::new_unique().to_string(),
        mint: Pubkey::new_unique().to_string(),
        amount: TokenAmount(1_000),
        memo: None,
        transaction_type: TransactionType::Withdrawal,
        withdrawal_nonce: None,
        status,
        created_at: now,
        updated_at: now,
        processed_at: None,
        counterpart_signature: None,
        remint_signatures: None,
        remint_last_valid_block_heights: None,
        pending_remint_deadline_at: None,
        finality_check_attempts: 0,
        recovery_requeue_attempts: 0,
        inner_index: None,
        landed_remint_signature: None,
        release_refused_on_chain: false,
    }
}

/// A `getAccountInfo` JSON-RPC response carrying a withdrawal bitmap account.
fn bitmap_account_response(generation: u64, consumed: &[u64]) -> String {
    let bytes = bitmap_account_bytes(generation, consumed, 255);
    serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": {
            "context": {"slot": 1},
            "value": {
                "owner": Pubkey::new_unique().to_string(),
                "lamports": 1_000_000u64,
                "data": [STANDARD.encode(&bytes), "base64"],
                "executable": false,
                "rentEpoch": 0
            }
        }
    })
    .to_string()
}

/// Mount a withdrawal bitmap account as the server's `getAccountInfo` reply.
pub(super) fn mock_bitmap_account(
    server: &mut mockito::ServerGuard,
    generation: u64,
    consumed: &[u64],
) -> mockito::Mock {
    server
        .mock("POST", "/")
        .match_body(mockito::Matcher::Regex(
            r#""method"\s*:\s*"getAccountInfo""#.into(),
        ))
        .with_status(200)
        .with_body(bitmap_account_response(generation, consumed))
        .create()
}

/// A finalized `getLatestBlockhash` whose context slot is the anchor an
/// anchored bitmap read has to carry. Successive calls walk `slots`, so a test
/// can prove a re-read takes a fresh anchor instead of reusing the first.
pub(super) fn mock_finalized_anchor(
    server: &mut mockito::ServerGuard,
    slots: Vec<u64>,
) -> mockito::Mock {
    let calls = Arc::new(AtomicUsize::new(0));
    server
        .mock("POST", "/")
        .match_body(mockito::Matcher::Regex(
            r#""method"\s*:\s*"getLatestBlockhash""#.into(),
        ))
        .with_status(200)
        .with_body_from_request(move |_| {
            let i = calls.fetch_add(1, Ordering::SeqCst).min(slots.len() - 1);
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": {
                    "context": {"slot": slots[i]},
                    "value": {
                        "blockhash": "11111111111111111111111111111111",
                        "lastValidBlockHeight": 1_000u64
                    }
                }
            })
            .to_string()
            .into_bytes()
        })
        .expect_at_least(1)
        .create()
}

/// A bitmap that answers only a read bound to `slot`. An unanchored read finds
/// no matching mock and errors, which is what a lagging backend would do.
pub(super) fn mock_bitmap_at_slot(
    server: &mut mockito::ServerGuard,
    slot: u64,
    generation: u64,
    consumed: &[u64],
) -> mockito::Mock {
    server
        .mock("POST", "/")
        .match_body(mockito::Matcher::AllOf(vec![
            mockito::Matcher::Regex(r#""method"\s*:\s*"getAccountInfo""#.into()),
            mockito::Matcher::Regex(format!(r#""minContextSlot"\s*:\s*{slot}\b"#)),
        ]))
        .with_status(200)
        .with_body(bitmap_account_response(generation, consumed))
        .expect(1)
        .create()
}

/// Same as `mock_bitmap_account`, but counts how many reads the sender actually
/// made, so a test can prove a read was skipped rather than merely unused.
pub(super) fn mock_bitmap_account_counted(
    server: &mut mockito::ServerGuard,
    generation: u64,
    reads: Arc<AtomicUsize>,
) -> mockito::Mock {
    let body = bitmap_account_response(generation, &[]);
    server
        .mock("POST", "/")
        .match_body(mockito::Matcher::Regex(
            r#""method"\s*:\s*"getAccountInfo""#.into(),
        ))
        .with_status(200)
        .with_body_from_request(move |_| {
            reads.fetch_add(1, Ordering::SeqCst);
            body.clone().into_bytes()
        })
        .expect_at_least(0)
        .create()
}

/// Answer `getAccountInfo` with an initialized SPL mint owned by `authority`,
/// which is the verdict that sends the JIT path down its retry arm.
pub(super) fn mock_initialized_mint(
    server: &mut mockito::ServerGuard,
    authority: Pubkey,
) -> mockito::Mock {
    let mint = spl_token::state::Mint {
        mint_authority: solana_sdk::program_option::COption::Some(authority),
        supply: 0,
        decimals: 6,
        is_initialized: true,
        freeze_authority: solana_sdk::program_option::COption::None,
    };
    let mut data = vec![0u8; spl_token::state::Mint::LEN];
    spl_token::state::Mint::pack(mint, &mut data).expect("pack mint");
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": {
            "context": {"slot": 1},
            "value": {
                "owner": spl_token::id().to_string(),
                "lamports": 1_000_000u64,
                "data": [STANDARD.encode(&data), "base64"],
                "executable": false,
                "rentEpoch": 0
            }
        }
    })
    .to_string();
    server
        .mock("POST", "/")
        .match_body(mockito::Matcher::Regex(
            r#""method"\s*:\s*"getAccountInfo""#.into(),
        ))
        .with_status(200)
        .with_body(body)
        .create()
}

/// Fail every `getAccountInfo`, so a bitmap read errors instead of answering.
pub(super) fn mock_bitmap_read_failure(server: &mut mockito::ServerGuard) -> mockito::Mock {
    server
        .mock("POST", "/")
        .match_body(mockito::Matcher::Regex(
            r#""method"\s*:\s*"getAccountInfo""#.into(),
        ))
        .with_status(500)
        .with_body("bitmap read unavailable")
        .create()
}

/// Answer the first `getAccountInfo` with a bitmap and fail every read after it.
pub(super) fn mock_bitmap_then_read_failure(
    server: &mut mockito::ServerGuard,
    generation: u64,
    consumed: &[u64],
) -> (mockito::Mock, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    let consumed = consumed.to_vec();

    let mock = server
        .mock("POST", "/")
        .match_body(mockito::Matcher::Regex(
            r#""method"\s*:\s*"getAccountInfo""#.into(),
        ))
        .with_status(200)
        .with_body_from_request(move |_| {
            if counter.fetch_add(1, Ordering::SeqCst) == 0 {
                return bitmap_account_response(generation, &consumed).into_bytes();
            }
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "error": {"code": -32003, "message": "node is behind"}
            })
            .to_string()
            .into_bytes()
        })
        .expect_at_least(1)
        .create();

    (mock, calls)
}

/// Serve a different bitmap on each successive `getAccountInfo`, so a test can
/// stage a chain that moves between two reads. The counter is returned so the
/// test can assert how many reads actually happened.
pub(super) fn mock_bitmap_sequence(
    server: &mut mockito::ServerGuard,
    reads: Vec<(u64, Vec<u64>)>,
) -> (mockito::Mock, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();

    let mock = server
        .mock("POST", "/")
        .match_body(mockito::Matcher::Regex(
            r#""method"\s*:\s*"getAccountInfo""#.into(),
        ))
        .with_status(200)
        .with_body_from_request(move |_| {
            let index = counter.fetch_add(1, Ordering::SeqCst);
            let (generation, consumed) = reads
                .get(index)
                .or_else(|| reads.last())
                .expect("mock_bitmap_sequence needs at least one read");
            bitmap_account_response(*generation, consumed).into_bytes()
        })
        .expect_at_least(1)
        .create();

    (mock, calls)
}
