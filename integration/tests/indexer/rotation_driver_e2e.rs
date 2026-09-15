//! E2E tests for the rotation driver, over a real Postgres and a mock RPC.
//!
//! The driver answers one question on a timer: is the bitmap's generation behind
//! the withdrawals waiting on it? Everything here is about that answer being
//! reached from durable state alone, so it still holds when the row that used to
//! trigger a rotation is quarantined, or was never written at all.
//!
//! Uses testcontainers for isolated Postgres instances.

use solana_commitment_config::CommitmentLevel;
use {
    base64::{engine::general_purpose::STANDARD, Engine as _},
    private_channel_indexer::operator::utils::instruction_util::TransactionBuilder,
    private_channel_indexer::{
        config::{PostgresConfig, PrivateChannelIndexerConfig, ProgramType, StorageType},
        operator::{
            bitmap_constants::NONCES_PER_GENERATION,
            sender::{test_hooks, types::SenderState},
            utils::account_util::bitmap_account_bytes,
        },
        storage::{
            common::models::{DbTransactionBuilder, TransactionType},
            PostgresDb, Storage,
        },
    },
    serde_json::json,
    solana_sdk::pubkey::Pubkey,
    std::sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, Once,
    },
    test_utils::mock_rpc::{MockRpcServer, Reply},
};

// ── fixture helpers ─────────────────────────────────────────────────────────

/// Install an in-memory admin signer once per test process, which every path
/// that builds a real instruction needs.
fn ensure_admin_signer_env() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let keypair = solana_sdk::signature::Keypair::new();
        let key = bs58::encode(keypair.to_bytes()).into_string();
        std::env::set_var("ADMIN_SIGNER", "memory");
        std::env::set_var("ADMIN_PRIVATE_KEY", &key);
        std::env::set_var("OPERATOR_SIGNER", "memory");
        std::env::set_var("OPERATOR_PRIVATE_KEY", &key);
    });
}

async fn start_pg(
    db_name: &str,
) -> (
    Arc<Storage>,
    sqlx::PgPool,
    testcontainers::ContainerAsync<testcontainers_modules::postgres::Postgres>,
) {
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::postgres::Postgres;

    let container = Postgres::default()
        .with_db_name(db_name)
        .with_user("postgres")
        .with_password("password")
        .start()
        .await
        .expect("postgres container");
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:password@{}:{}/{}", host, port, db_name);
    let db = PostgresDb::new(&PostgresConfig {
        database_url: url.clone(),
        max_connections: 10,
    })
    .await
    .unwrap();
    let storage = Storage::Postgres(db);
    storage.init_schema().await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    (Arc::new(storage), pool, container)
}

/// A `getAccountInfo` reply carrying a withdrawal bitmap on `generation`.
fn bitmap_reply(generation: u64) -> Reply {
    let data = bitmap_account_bytes(generation, &[], 255);
    Reply::result(json!({
        "context": { "slot": 100 },
        "value": {
            "data": [STANDARD.encode(&data), "base64"],
            "executable": false,
            "lamports": 1_461_600u64,
            "owner": Pubkey::new_unique().to_string(),
            "rentEpoch": 0u64,
            "space": data.len(),
        }
    }))
}

/// A withdraw-role sender over the given storage, pointed at `rpc_url`.
fn build_sender(storage: Arc<Storage>, rpc_url: String) -> SenderState {
    ensure_admin_signer_env();
    test_hooks::new_sender_state(
        &PrivateChannelIndexerConfig {
            program_type: ProgramType::Withdraw,
            storage_type: StorageType::Postgres,
            rpc_url,
            fallback_rpc_url: None,
            source_rpc_url: None,
            postgres: PostgresConfig {
                database_url: "postgres://placeholder/none".to_string(),
                max_connections: 1,
            },
            escrow_instance_id: None,
        },
        CommitmentLevel::Confirmed,
        Some(Pubkey::new_unique()),
        storage,
        1,
        1,
        None,
    )
    .expect("SenderState construction must succeed")
}

/// Insert a withdrawal and force it to `nonce` and `status`. Both are set after
/// the fact because the insert trigger picks the nonce and every row lands
/// `pending`.
async fn seed_withdrawal(
    storage: &Storage,
    pool: &sqlx::PgPool,
    tag: &str,
    nonce: i64,
    status: &str,
) -> i64 {
    let mint = Pubkey::new_unique().to_string();
    let user = Pubkey::new_unique().to_string();
    let row = DbTransactionBuilder::new(tag.to_string(), 1, mint, 10_000u64)
        .initiator(user.clone())
        .recipient(user)
        .transaction_type(TransactionType::Withdrawal)
        .build();
    let id = storage.insert_db_transaction(&row).await.unwrap();
    sqlx::query(
        "UPDATE transactions SET status = $2::transaction_status, withdrawal_nonce = $3 WHERE id = $1",
    )
    .bind(id)
    .bind(status)
    .bind(nonce)
    .execute(pool)
    .await
    .unwrap();
    id
}

/// The first nonce of the generation after the one the chain is on.
fn next_generation_nonce(offset: i64) -> i64 {
    NONCES_PER_GENERATION as i64 + offset
}

// ── tests ───────────────────────────────────────────────────────────────────

/// The case no reordering inside the processor could ever fix. Nonces come from
/// a sequence that a losing insert can consume without leaving a row, so the
/// boundary nonce may simply not exist. The driver reads the generation of the
/// work that is actually waiting, so it never needed that row.
#[tokio::test(flavor = "multi_thread")]
async fn rotation_happens_with_no_boundary_row_present() {
    let (storage, pool, _pg) = start_pg("rotation_no_boundary_row").await;
    let rpc = MockRpcServer::start().await;
    rpc.enqueue_sequence("getAccountInfo", vec![bitmap_reply(0), bitmap_reply(0)]);

    // Both rows sit in the next generation, and the boundary nonce itself is
    // absent, exactly as a burned sequence value would leave it.
    seed_withdrawal(&storage, &pool, "w1", next_generation_nonce(1), "pending").await;
    seed_withdrawal(&storage, &pool, "w2", next_generation_nonce(2), "parked").await;

    let mut state = build_sender(storage, rpc.url());
    test_hooks::originate_rotation_if_needed(&mut state).await;

    assert!(
        test_hooks::take_pending_rotation_if_ready(&mut state)
            .await
            .is_some(),
        "a rotation must be armed and ready even with no boundary row"
    );
}

/// Withholding the rotation is the safe answer while a lower nonce can still be
/// resolved into a release, and resolving that row is what lets it through.
#[tokio::test(flavor = "multi_thread")]
async fn a_manual_review_row_blocks_rotation_until_resolved() {
    let (storage, pool, _pg) = start_pg("rotation_blocked_by_manual_review").await;
    let rpc = MockRpcServer::start().await;
    rpc.enqueue_sequence("getAccountInfo", vec![bitmap_reply(0), bitmap_reply(0)]);

    let blocking = seed_withdrawal(&storage, &pool, "stuck", 2, "manual_review").await;
    seed_withdrawal(
        &storage,
        &pool,
        "waiting",
        next_generation_nonce(1),
        "pending",
    )
    .await;

    let mut state = build_sender(storage.clone(), rpc.url());

    // Only the withholding is asserted here. Reporting waits for the block to
    // persist, which is a pass count the unit tests drive far more cheaply than
    // a real database can.
    test_hooks::originate_rotation_if_needed(&mut state).await;
    assert!(
        test_hooks::take_pending_rotation_if_ready(&mut state)
            .await
            .is_none(),
        "an unresolved lower nonce must hold the rotation back"
    );

    // A human resolves the row, which is what releases the block.
    sqlx::query("UPDATE transactions SET status = 'completed' WHERE id = $1")
        .bind(blocking)
        .execute(&pool)
        .await
        .unwrap();

    test_hooks::originate_rotation_if_needed(&mut state).await;
    assert!(
        test_hooks::take_pending_rotation_if_ready(&mut state)
            .await
            .is_some(),
        "resolving the blocking nonce must let the rotation through"
    );
}

/// The arm lives only in memory, so a restart drops it. Nothing persists it on
/// purpose: the same two authorities that produced it, the database and the
/// chain, still say a rotation is owed, so the next pass rebuilds it.
#[tokio::test(flavor = "multi_thread")]
async fn rotation_is_re_derived_after_a_restart_that_dropped_the_arm() {
    let (storage, pool, _pg) = start_pg("rotation_survives_restart").await;
    let rpc = MockRpcServer::start().await;
    rpc.enqueue_sequence("getAccountInfo", vec![bitmap_reply(0), bitmap_reply(0)]);

    seed_withdrawal(&storage, &pool, "w1", next_generation_nonce(0), "pending").await;

    let mut state = build_sender(storage.clone(), rpc.url());
    test_hooks::originate_rotation_if_needed(&mut state).await;
    assert!(
        state.pending_rotation.is_some(),
        "the first pass must arm a rotation"
    );

    // Stand in for a crash between arming and sending: the whole sender is gone.
    drop(state);

    let mut restarted = build_sender(storage, rpc.url());
    test_hooks::originate_rotation_if_needed(&mut restarted).await;
    assert!(
        restarted.pending_rotation.is_some(),
        "a restart must rebuild the rotation from the database and the chain"
    );
}

/// The chain, as far as the sender can tell: a generation, and a record of the
/// generation every rotation was signed against.
#[derive(Clone, Default)]
struct FakeChain {
    generation: Arc<AtomicU64>,
    signed: Arc<Mutex<Vec<u64>>>,
    last_refused: Arc<AtomicBool>,
}

/// The one argument of the `RotateBitmap` instruction in a signed transaction,
/// little-endian after the one-byte discriminator. Compute-budget instructions
/// share that shape, so the program id is what picks the rotation out.
fn signed_generation(encoded_tx: &str) -> u64 {
    let bytes = STANDARD
        .decode(encoded_tx)
        .expect("valid base64 transaction");
    let tx: solana_sdk::transaction::VersionedTransaction =
        bincode::deserialize(&bytes).expect("valid wire transaction");
    let keys = tx.message.static_account_keys();
    let data = tx
        .message
        .instructions()
        .iter()
        .find(|ix| keys[ix.program_id_index as usize] != solana_compute_budget_interface::id())
        .map(|ix| ix.data.clone())
        .expect("the transaction must carry a rotation instruction");
    u64::from_le_bytes(data[1..9].try_into().unwrap())
}

/// Script the whole send path against `chain`, enough times for two dispatches
/// and their confirmation polls.
fn script_chain(rpc: &MockRpcServer, chain: &FakeChain) {
    const ROUNDS: usize = 64;

    let gen_for_account = chain.generation.clone();
    rpc.enqueue_sequence(
        "getAccountInfo",
        std::iter::repeat_with(|| {
            let generation = gen_for_account.clone();
            Reply::dynamic(move |_| {
                let data = bitmap_account_bytes(generation.load(Ordering::SeqCst), &[], 255);
                json!({
                    "context": { "slot": 100 },
                    "value": {
                        "data": [STANDARD.encode(&data), "base64"],
                        "executable": false,
                        "lamports": 1_461_600u64,
                        "owner": Pubkey::new_unique().to_string(),
                        "rentEpoch": 0u64,
                        "space": data.len(),
                    }
                })
            })
        })
        .take(ROUNDS),
    );

    rpc.enqueue_sequence(
        "getLatestBlockhash",
        std::iter::repeat_with(|| {
            Reply::result(json!({
                "context": { "slot": 100 },
                "value": {
                    "blockhash": "GHtXQBsoZHjzkAm2Sdm6FTyFHBCqBnLanJJhZFCFJXoe",
                    "lastValidBlockHeight": 1_000u64
                }
            }))
        })
        .take(ROUNDS),
    );

    // Stands in for the program's generation check: a rotation bound to the
    // generation the chain is on lands and advances it, anything else is a
    // duplicate the program refuses.
    let send_chain = chain.clone();
    rpc.enqueue_sequence(
        "sendTransaction",
        std::iter::repeat_with(move || {
            let chain = send_chain.clone();
            Reply::dynamic(move |req| {
                let encoded = req["params"][0].as_str().expect("encoded transaction");
                let signed = signed_generation(encoded);
                chain.signed.lock().unwrap().push(signed);
                let accepted = chain
                    .generation
                    .compare_exchange(signed, signed + 1, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok();
                chain.last_refused.store(!accepted, Ordering::SeqCst);
                let bytes = STANDARD.decode(encoded).unwrap();
                json!(bs58::encode(&bytes[1..65]).into_string())
            })
        })
        .take(ROUNDS),
    );

    // A refused rotation comes back as UnexpectedGeneration (custom code 14).
    // An accepted one is never observed at all, which is the case that drives
    // the sender into its re-arm while the rotation has in fact landed.
    let status_chain = chain.clone();
    rpc.enqueue_sequence(
        "getSignatureStatuses",
        std::iter::repeat_with(move || {
            let refused = status_chain.last_refused.clone();
            Reply::dynamic(move |_| {
                let value = if refused.load(Ordering::SeqCst) {
                    json!([{
                        "slot": 100,
                        "confirmations": null,
                        "confirmationStatus": "finalized",
                        "err": { "InstructionError": [0, { "Custom": 14 }] },
                        "status": { "Err": { "InstructionError": [0, { "Custom": 14 }] } }
                    }])
                } else {
                    json!([null])
                };
                json!({ "context": { "slot": 100 }, "value": value })
            })
        })
        .take(ROUNDS),
    );
}

/// A rotation can land and still be reported failed, because the operator only
/// ever sees the confirmation. Re-binding the re-armed rotation to the chain's
/// new generation would make that replay look legitimate and close a second
/// window nobody opened, stranding every nonce numbered into it.
#[tokio::test(flavor = "multi_thread")]
async fn a_rotation_reported_failed_after_it_landed_is_refused_not_rebound() {
    let (storage, pool, _pg) = start_pg("rotation_landed_but_reported_failed").await;
    let rpc = MockRpcServer::start().await;
    let chain = FakeChain::default();
    script_chain(&rpc, &chain);

    seed_withdrawal(&storage, &pool, "w1", next_generation_nonce(1), "pending").await;

    let mut state = build_sender(storage, rpc.url());
    let (storage_tx, _storage_rx) = tokio::sync::mpsc::channel(16);

    test_hooks::originate_rotation_if_needed(&mut state).await;
    let first = test_hooks::take_pending_rotation_if_ready(&mut state)
        .await
        .expect("a rotation must be armed");
    test_hooks::submit_transaction(
        &mut state,
        TransactionBuilder::RotateBitmap(first),
        &storage_tx,
    )
    .await;

    assert_eq!(
        chain.generation.load(Ordering::SeqCst),
        1,
        "the first rotation must land on chain"
    );
    let rearmed = state
        .pending_rotation
        .take()
        .expect("an unconfirmed rotation must be re-armed");

    test_hooks::submit_transaction(
        &mut state,
        TransactionBuilder::RotateBitmap(rearmed),
        &storage_tx,
    )
    .await;

    assert_eq!(
        *chain.signed.lock().unwrap(),
        vec![0, 0],
        "the re-armed rotation must be signed against its original generation"
    );
    assert_eq!(
        chain.generation.load(Ordering::SeqCst),
        1,
        "the replay must be refused, leaving the chain one generation ahead, not two"
    );
    assert!(
        state.pending_rotation.is_none() && state.rotation_in_flight.is_none(),
        "a rotation refused as a duplicate is settled, not owed"
    );
}
