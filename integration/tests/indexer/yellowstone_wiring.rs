//! End-to-end wiring test for `MockYellowstoneServer` + `YellowstoneSource`.
//!
//! Analogue of `mock_rpc_retry` for the Yellowstone gRPC datasource.
//! Validates the full pipe from a scripted gRPC stream → `YellowstoneSource`
//! → `ProcessorMessage` channel. Confirms the mock is a drop-in substitute
//! for a real Yellowstone node and that the production source decodes a
//! scripted `SubscribeUpdate` stream end-to-end.
//!
//! What's exercised here:
//!   - `MockYellowstoneServer::start` + `enqueue(Update::ok(...))`
//!   - `YellowstoneSource::start` → connects over plain HTTP to the mock
//!   - `BlockMeta` path: surfaces as `ProcessorMessage::SlotComplete`
//!   - `Transaction` path: decodes an escrow `Deposit` and surfaces as
//!     `ProcessorMessage::Instruction(ProgramInstruction::Escrow(Deposit))`
//!   - `call_count("subscribe") == 1` and `remaining_scripted == 0`

use private_channel_indexer::config::ProgramType;
use private_channel_indexer::indexer::datasource::common::datasource::DataSource;
use private_channel_indexer::indexer::datasource::common::parser::escrow::PRIVATE_CHANNEL_ESCROW_PROGRAM_ID;
use private_channel_indexer::indexer::datasource::common::types::{
    ProcessorMessage, ProgramInstruction,
};
use private_channel_indexer::indexer::datasource::yellowstone::YellowstoneSource;
use std::str::FromStr;
use std::time::Duration;
use test_utils::mock_yellowstone::{MockYellowstoneServer, Update, UpdateMatcher};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use yellowstone_grpc_proto::geyser::{
    subscribe_update::UpdateOneof, SubscribeUpdate, SubscribeUpdateBlockMeta,
    SubscribeUpdateTransaction, SubscribeUpdateTransactionInfo,
};
use yellowstone_grpc_proto::solana::storage::confirmed_block::{
    CompiledInstruction as ProtoCompiledInstruction, InnerInstruction as ProtoInnerInstruction,
    InnerInstructions as ProtoInnerInstructions, Message as ProtoMessage, MessageHeader,
    Transaction as ProtoTransaction, TransactionStatusMeta,
};

/// Build a well-formed `SubscribeUpdate` carrying a single escrow Deposit
/// instruction (discriminator 6 + 8-byte amount + 1-byte `Option::None`)
/// plus a `meta.inner_instructions` entry carrying the matching DepositEvent
/// CPI — required by the parser, which reads the authoritative received
/// amount from the event rather than the instruction args.
///
/// The account list is padded with deterministic junk pubkeys so parsing the
/// Deposit accounts (12 required) succeeds.
fn deposit_tx_update(slot: u64) -> SubscribeUpdate {
    let program_id =
        solana_sdk::pubkey::Pubkey::from_str(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID).unwrap();

    // 12 account keys + program id at index 12 (the instruction references
    // indices 0..12 and program_id_index = 12).
    let mut account_keys: Vec<Vec<u8>> = (0..12)
        .map(|i| {
            let mut bytes = [0u8; 32];
            bytes[0] = (i + 1) as u8;
            bytes.to_vec()
        })
        .collect();
    account_keys.push(program_id.to_bytes().to_vec());

    // Deposit discriminator = 6, data layout: amount (u64 LE) + Option<Pubkey> (1 byte tag).
    let mut ix_data = vec![6u8];
    ix_data.extend_from_slice(&1_000u64.to_le_bytes());
    ix_data.push(0u8); // None

    let instruction = ProtoCompiledInstruction {
        program_id_index: 12,
        accounts: (0u8..12).collect(),
        data: ix_data,
    };

    let message = ProtoMessage {
        header: Some(MessageHeader {
            num_required_signatures: 1,
            num_readonly_signed_accounts: 0,
            num_readonly_unsigned_accounts: 1,
        }),
        account_keys,
        recent_blockhash: vec![0u8; 32],
        instructions: vec![instruction],
        versioned: false,
        address_table_lookups: vec![],
    };

    let transaction = ProtoTransaction {
        signatures: vec![vec![7u8; 64]],
        message: Some(message),
    };

    // DepositEvent payload: EVENT_IX_TAG(8) + disc=6 + instance_seed(32)
    // + user(32) + amount=1000 LE(8) + recipient(32) + mint(32) = 145 bytes.
    let mut event_data = vec![];
    event_data.extend_from_slice(&0x1d9acb512ea545e4u64.to_le_bytes());
    event_data.push(6);
    event_data.extend_from_slice(&[0u8; 32]);
    event_data.extend_from_slice(&[0u8; 32]);
    event_data.extend_from_slice(&1_000u64.to_le_bytes());
    event_data.extend_from_slice(&[0u8; 32]);
    event_data.extend_from_slice(&[0u8; 32]);

    let meta = TransactionStatusMeta {
        inner_instructions: vec![ProtoInnerInstructions {
            index: 0,
            instructions: vec![ProtoInnerInstruction {
                program_id_index: 12,
                accounts: vec![],
                data: event_data,
                stack_height: Some(2),
            }],
        }],
        ..Default::default()
    };

    let tx_info = SubscribeUpdateTransactionInfo {
        signature: vec![7u8; 64],
        is_vote: false,
        transaction: Some(transaction),
        meta: Some(meta),
        index: 0,
    };

    SubscribeUpdate {
        filters: vec!["private_channel_program".to_string()],
        update_oneof: Some(UpdateOneof::Transaction(SubscribeUpdateTransaction {
            transaction: Some(tx_info),
            slot,
        })),
        created_at: None,
    }
}

fn block_meta(slot: u64) -> SubscribeUpdate {
    SubscribeUpdate {
        filters: vec!["all_blocks_meta".to_string()],
        update_oneof: Some(UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
            slot,
            blockhash: format!("hash-{slot}"),
            ..Default::default()
        })),
        created_at: None,
    }
}

/// End-to-end wiring: scripted BlockMeta + escrow Deposit land in the
/// processor channel via YellowstoneSource, in order, with exactly one
/// subscribe handshake.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn yellowstone_source_consumes_scripted_stream() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,private_channel_indexer=debug")
        .with_test_writer()
        .try_init();

    let server = MockYellowstoneServer::start().await;

    // Script a BlockMeta → Deposit → BlockMeta sequence.
    server.enqueue(UpdateMatcher, Update::ok(block_meta(100)));
    server.enqueue(UpdateMatcher, Update::ok(deposit_tx_update(101)));
    server.enqueue(UpdateMatcher, Update::ok(block_meta(101)));

    let (tx, mut rx) = mpsc::channel::<ProcessorMessage>(64);
    let cancel = CancellationToken::new();

    let mut source = YellowstoneSource::new(
        server.url(),
        None,
        "confirmed".to_string(),
        ProgramType::Escrow,
        None,
    );

    let handle = source
        .start(tx, cancel.clone())
        .await
        .expect("yellowstone source start");

    // Collect up to 3 messages within a generous deadline.
    let mut slot_completes: Vec<u64> = vec![];
    let mut deposits_seen = 0usize;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);

    while slot_completes.len() < 2 || deposits_seen < 1 {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        let msg = match tokio::time::timeout(remaining, rx.recv()).await {
            Ok(Some(m)) => m,
            Ok(None) => break,
            Err(_) => break,
        };
        match msg {
            ProcessorMessage::SlotComplete { slot, .. } => slot_completes.push(slot),
            ProcessorMessage::Instruction(meta) => {
                if matches!(meta.instruction, ProgramInstruction::Escrow(ref b) if matches!(
                    **b,
                    private_channel_indexer::indexer::datasource::common::parser::EscrowInstruction::Deposit { .. }
                )) {
                    deposits_seen += 1;
                    assert_eq!(meta.slot, 101);
                }
            }
        }
    }

    assert_eq!(
        slot_completes,
        vec![100, 101],
        "BlockMeta updates should land as SlotComplete in FIFO order"
    );
    assert_eq!(
        deposits_seen, 1,
        "the scripted Deposit instruction should be parsed and forwarded"
    );
    assert_eq!(
        server.remaining_scripted(),
        0,
        "all scripted updates should have been consumed"
    );
    assert_eq!(
        server.call_count("subscribe"),
        1,
        "exactly one subscribe handshake expected on a clean stream"
    );

    // Shut down — cancel token tells the source to close its stream; the
    // server shutdown drains the gRPC endpoint.
    cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(3), handle).await;
    server.shutdown().await;
}
