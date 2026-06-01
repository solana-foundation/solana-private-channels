//! Yellowstone transaction-parse defensive arms.
//!
//! Covers two annotated regions in
//! `indexer/src/indexer/datasource/yellowstone/source.rs`:
//!
//!   * `:649-668` inner_instructions branch — exercised by feeding a
//!     transaction whose `meta.inner_instructions` carries one nested
//!     CPI instruction. The branch walks the outer vec and inner vec
//!     and builds the typed `InnerInstructions` accumulator.
//!
//!   * `:736-773` unsupported/invalid escrow & withdraw instruction
//!     arms — fed by a transaction whose top-level instruction data
//!     starts with a discriminator the parser does not recognise
//!     (`Ok(None)` branch). The indexer should filter the frame
//!     silently rather than error the stream.
//!
//! This test uses `MockYellowstoneServer` + `YellowstoneSource` — same
//! wiring as `yellowstone_wiring.rs`. It does not assert on output
//! beyond "the stream stays healthy and non-parseable frames are
//! dropped" because the public channel surface only forwards known
//! instructions; the `Ok(None)` arm is defined as "silently filtered".

use {
    private_channel_indexer::{
        config::ProgramType,
        indexer::datasource::{
            common::{
                datasource::DataSource,
                parser::escrow::PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
                types::{ProcessorMessage, ProgramInstruction},
            },
            yellowstone::YellowstoneSource,
        },
    },
    std::{str::FromStr, time::Duration},
    test_utils::mock_yellowstone::{MockYellowstoneServer, Update, UpdateMatcher},
    tokio::sync::mpsc,
    tokio_util::sync::CancellationToken,
    yellowstone_grpc_proto::{
        geyser::{
            subscribe_update::UpdateOneof, SubscribeUpdate, SubscribeUpdateBlockMeta,
            SubscribeUpdateTransaction, SubscribeUpdateTransactionInfo,
        },
        solana::storage::confirmed_block::{
            CompiledInstruction as ProtoCompiledInstruction,
            InnerInstruction as ProtoInnerInstruction, InnerInstructions as ProtoInnerInstructions,
            Message as ProtoMessage, MessageHeader, Transaction as ProtoTransaction,
            TransactionStatusMeta,
        },
    },
};

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

/// Build an escrow Deposit transaction carrying one `meta.inner_instructions`
/// entry. The inner frame is a valid DepositEvent CPI — required by the
/// parser, which reads the authoritative received amount from the event.
fn deposit_with_inner_instructions(slot: u64) -> SubscribeUpdate {
    let program_id =
        solana_sdk::pubkey::Pubkey::from_str(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID).unwrap();
    let mut account_keys: Vec<Vec<u8>> = (0..12)
        .map(|i| {
            let mut bytes = [0u8; 32];
            bytes[0] = (i + 1) as u8;
            bytes.to_vec()
        })
        .collect();
    account_keys.push(program_id.to_bytes().to_vec());

    // Deposit discriminator = 6, amount u64 LE, then Option::None (1 byte).
    let mut ix_data = vec![6u8];
    ix_data.extend_from_slice(&1_000u64.to_le_bytes());
    ix_data.push(0u8);

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

    // Attach a `meta` with one inner-instruction set carrying a valid
    // DepositEvent CPI: EVENT_IX_TAG(8) + disc=6 + instance_seed(32)
    // + user(32) + amount=1000 LE(8) + recipient(32) + mint(32) = 145 bytes.
    let mut event_data = vec![];
    event_data.extend_from_slice(&0x1d9acb512ea545e4u64.to_le_bytes());
    event_data.push(6);
    event_data.extend_from_slice(&[0u8; 32]);
    event_data.extend_from_slice(&[0u8; 32]);
    event_data.extend_from_slice(&1_000u64.to_le_bytes());
    event_data.extend_from_slice(&[0u8; 32]);
    event_data.extend_from_slice(&[0u8; 32]);
    let inner_ix = ProtoInnerInstruction {
        program_id_index: 12,
        accounts: vec![0u8, 1, 2],
        data: event_data,
        stack_height: Some(2),
    };
    let inner_set = ProtoInnerInstructions {
        index: 0,
        instructions: vec![inner_ix],
    };
    let meta = TransactionStatusMeta {
        inner_instructions: vec![inner_set],
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

/// Build an escrow transaction whose top-level instruction data begins with
/// an unrecognised discriminator (0xFE). The escrow parser returns
/// `Ok(None)` for unknown discriminators, firing the `:736-773` branch.
fn unknown_discriminator_tx(slot: u64) -> SubscribeUpdate {
    let program_id =
        solana_sdk::pubkey::Pubkey::from_str(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID).unwrap();
    let mut account_keys: Vec<Vec<u8>> = (0..12)
        .map(|i| {
            let mut bytes = [0u8; 32];
            bytes[0] = (i + 100) as u8;
            bytes.to_vec()
        })
        .collect();
    account_keys.push(program_id.to_bytes().to_vec());

    // Unknown discriminator — the parser's match on the first byte has no
    // arm for 0xFE, so it returns Ok(None).
    let ix_data = vec![0xFEu8];
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
        signatures: vec![vec![0x42u8; 64]],
        message: Some(message),
    };

    let tx_info = SubscribeUpdateTransactionInfo {
        signature: vec![0x42u8; 64],
        is_vote: false,
        transaction: Some(transaction),
        meta: None,
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

/// Feeds:
///   1. BlockMeta slot 200
///   2. Deposit tx with meta.inner_instructions (covers :649-668)
///   3. Unknown-discriminator escrow tx (covers :736-773)
///   4. BlockMeta slot 201
///
/// Asserts:
///   - The deposit instruction still surfaces on the processor channel
///     (i.e. inner_instructions parsing succeeds without breaking the
///     outer frame).
///   - The unknown-discriminator tx is silently dropped — no
///     `ProgramInstruction` message, no error on the channel.
///   - Both BlockMetas surface as `SlotComplete`, proving the stream
///     stayed healthy across both defensive arms.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn yellowstone_handles_inner_instructions_and_unknown_discriminator() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,private_channel_indexer=debug")
        .with_test_writer()
        .try_init();

    let server = MockYellowstoneServer::start().await;
    server.enqueue(UpdateMatcher, Update::ok(block_meta(200)));
    server.enqueue(
        UpdateMatcher,
        Update::ok(deposit_with_inner_instructions(201)),
    );
    server.enqueue(UpdateMatcher, Update::ok(unknown_discriminator_tx(202)));
    server.enqueue(UpdateMatcher, Update::ok(block_meta(203)));

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

    let mut slot_completes: Vec<u64> = vec![];
    let mut deposits = 0usize;
    let mut other_instructions = 0usize;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while slot_completes.len() < 2 || deposits < 1 {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, rx.recv()).await {
            Ok(Some(ProcessorMessage::SlotComplete { slot, .. })) => slot_completes.push(slot),
            Ok(Some(ProcessorMessage::Instruction(meta))) => match meta.instruction {
                ProgramInstruction::Escrow(ref b)
                    if matches!(
                        **b,
                        private_channel_indexer::indexer::datasource::common::parser::EscrowInstruction::Deposit { .. }
                    ) =>
                {
                    deposits += 1;
                }
                _ => {
                    other_instructions += 1;
                }
            },
            Ok(None) | Err(_) => break,
        }
    }

    assert_eq!(
        slot_completes,
        vec![200, 203],
        "BlockMeta frames must bracket both defensive-arm transactions"
    );
    assert_eq!(
        deposits, 1,
        "the deposit-with-inner-instructions tx must surface (inner_instructions parsing succeeded)"
    );
    assert_eq!(
        other_instructions, 0,
        "unknown-discriminator tx must be silently filtered, not forwarded"
    );
    assert_eq!(
        server.remaining_scripted(),
        0,
        "every scripted update must be consumed"
    );

    cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(3), handle).await;
    server.shutdown().await;
}
