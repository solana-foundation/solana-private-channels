//! End-to-end multi-instruction pipeline integrity (SOLA2-13)
//!
//! Target files:
//!   - `indexer/src/indexer/transaction_processor.rs` (buffering + convert)
//!   - `indexer/src/storage/postgres/db.rs` (composite-key batch insert)
//! Binary: `reconciliation_integration` (attached via `#[path]` mod from
//! `tests/indexer/reconciliation.rs`).
//!
//! The decoder-level tests (`decoder.rs`, `yellowstone/source.rs`) prove the
//! datasources stamp an absolute `instruction_index` per instruction, and the
//! storage-level test (`db_migration_race.rs`) proves the batch insert keys on
//! `(signature, instruction_index)`. Neither drives the *processor* against a
//! real database. These tests close that gap: they push two escrow `Deposit`
//! instructions that share one transaction signature through the real
//! `TransactionProcessor` channel API into Postgres and assert both economic
//! events survive as distinct rows — the exact scenario SOLA2-13 reported as
//! silently collapsing to one row.

use {
    private_channel_indexer::{
        config::ProgramType,
        indexer::{
            checkpoint::CheckpointUpdate,
            datasource::common::{
                parser::{DepositAccounts, DepositData, DepositEvent, EscrowInstruction},
                types::{InstructionWithMetadata, ProcessorMessage, ProgramInstruction},
            },
            transaction_processor::TransactionProcessor,
        },
        storage::{PostgresDb, Storage},
        PostgresConfig,
    },
    solana_sdk::{pubkey::Pubkey, signature::Signature},
    std::sync::Arc,
    testcontainers::runners::AsyncRunner,
    testcontainers_modules::postgres::Postgres,
    tokio::sync::mpsc,
};

async fn start_postgres(
    db_name: &str,
) -> (PostgresDb, String, testcontainers::ContainerAsync<Postgres>) {
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
    (db, url, container)
}

/// Build a `Deposit` instruction scoped to `instance` so it survives the
/// processor's per-instruction instance filter. `event_amount` is what lands in
/// the `amount` column (the event-reported received amount), distinct from
/// `data.amount`, so the two rows can be told apart.
fn deposit_meta(
    slot: u64,
    signature: &str,
    instance: Pubkey,
    instruction_index: u32,
    recipient: Pubkey,
    event_amount: u64,
) -> InstructionWithMetadata {
    let p = |i: u8| {
        let mut b = [0u8; 32];
        b[0] = i;
        Pubkey::new_from_array(b)
    };
    InstructionWithMetadata {
        instruction: ProgramInstruction::Escrow(Box::new(EscrowInstruction::Deposit {
            accounts: DepositAccounts {
                payer: p(10),
                user: p(1),
                instance,
                mint: p(2),
                allowed_mint: p(12),
                user_ata: p(13),
                instance_ata: p(14),
                system_program: p(15),
                token_program: p(16),
                associated_token_program: p(17),
                event_authority: p(18),
                private_channel_escrow_program: p(19),
            },
            data: DepositData {
                amount: 1000,
                recipient: Some(recipient),
            },
            event: DepositEvent {
                amount: event_amount,
            },
        })),
        slot,
        program_type: ProgramType::Escrow,
        signature: Some(signature.to_string()),
        instruction_index,
    }
}

/// Drive the processor over one slot's worth of messages, then close the input
/// so `start()` returns. Returns once the processor task has fully drained.
async fn run_slot(storage: Arc<Storage>, instance: Pubkey, messages: Vec<ProcessorMessage>) {
    let (checkpoint_tx, _checkpoint_rx) = mpsc::channel::<CheckpointUpdate>(16);
    let (instr_tx, instr_rx) = mpsc::channel::<ProcessorMessage>(16);

    let processor =
        TransactionProcessor::new(storage, checkpoint_tx).with_escrow_instance_id(instance);
    let handle = tokio::spawn(processor.start(instr_rx));

    for msg in messages {
        instr_tx.send(msg).await.expect("send to processor");
    }
    drop(instr_tx); // end the recv loop
    handle
        .await
        .expect("processor task join")
        .expect("processor ok");
}

/// Two `Deposit` instructions sharing one signature must both persist as
/// distinct rows with their absolute instruction indices — not collapse to one.
#[tokio::test(flavor = "multi_thread")]
async fn two_same_signature_deposits_persist_as_distinct_rows() {
    let (db, url, _pg) = start_postgres("c1_multi_ix_pipeline").await;
    db.init_schema().await.unwrap();
    let storage = Arc::new(Storage::Postgres(db));

    let instance = Pubkey::new_unique();
    let signature = Signature::new_unique().to_string();
    let recipient_a = Pubkey::new_unique();
    let recipient_b = Pubkey::new_unique();

    run_slot(
        storage,
        instance,
        vec![
            ProcessorMessage::Instruction(deposit_meta(
                7,
                &signature,
                instance,
                0,
                recipient_a,
                990,
            )),
            ProcessorMessage::Instruction(deposit_meta(
                7,
                &signature,
                instance,
                1,
                recipient_b,
                480,
            )),
            ProcessorMessage::SlotComplete {
                slot: 7,
                program_type: ProgramType::Escrow,
            },
        ],
    )
    .await;

    let pool = sqlx::PgPool::connect(&url).await.expect("sqlx connect");

    // Both economic events survive.
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM transactions WHERE signature = $1")
        .bind(&signature)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        count, 2,
        "both same-signature deposits must persist; got {count} (SOLA2-13 collapse)"
    );

    // Distinct absolute indices, ordered.
    let rows: Vec<(i32, i64, String)> = sqlx::query_as(
        "SELECT instruction_index, amount, recipient FROM transactions \
         WHERE signature = $1 ORDER BY instruction_index",
    )
    .bind(&signature)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].0, 0, "first deposit keeps instruction_index 0");
    assert_eq!(rows[1].0, 1, "second deposit keeps instruction_index 1");
    // The second (would-be dropped) instruction is the one that must survive
    // intact: its amount and recipient, not instruction 0's, define its row.
    assert_eq!(rows[0].1, 990);
    assert_eq!(
        rows[1].1, 480,
        "instruction 1's economic value is not overwritten by instruction 0"
    );
    assert_eq!(rows[0].2, recipient_a.to_string());
    assert_eq!(rows[1].2, recipient_b.to_string());
}

/// Reprocessing the same slot (the indexer replays a slot whose checkpoint did
/// not commit) must stay idempotent on the composite key: still exactly two
/// rows, no duplicates and no collapse.
#[tokio::test(flavor = "multi_thread")]
async fn replayed_slot_is_idempotent_on_composite_key() {
    let (db, url, _pg) = start_postgres("c1_multi_ix_replay").await;
    db.init_schema().await.unwrap();
    let storage = Arc::new(Storage::Postgres(db));

    let instance = Pubkey::new_unique();
    let signature = Signature::new_unique().to_string();
    let recipient_a = Pubkey::new_unique();
    let recipient_b = Pubkey::new_unique();

    let messages = || {
        vec![
            ProcessorMessage::Instruction(deposit_meta(
                9,
                &signature,
                instance,
                0,
                recipient_a,
                990,
            )),
            ProcessorMessage::Instruction(deposit_meta(
                9,
                &signature,
                instance,
                1,
                recipient_b,
                480,
            )),
            ProcessorMessage::SlotComplete {
                slot: 9,
                program_type: ProgramType::Escrow,
            },
        ]
    };

    // Process the slot, then process the identical slot again.
    run_slot(storage.clone(), instance, messages()).await;
    run_slot(storage, instance, messages()).await;

    let pool = sqlx::PgPool::connect(&url).await.expect("sqlx connect");
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM transactions WHERE signature = $1")
        .bind(&signature)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        count, 2,
        "slot replay must not duplicate rows on the (signature, instruction_index) key; got {count}"
    );
}
