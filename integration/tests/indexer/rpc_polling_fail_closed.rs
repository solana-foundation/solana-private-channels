//! Fail-closed decoding for `RpcPollingSource`. An instruction the indexer supports but
//! cannot decode leaves the slot's contents unknown, so the slot must not complete; a
//! configured fallback RPC gets one chance to serve a copy that decodes, and is judged on
//! decoding rather than on its meta merely being present.
//!
//! Mock-only: mockito stands in for both RPC endpoints, so no validator or database.

use mockito::Server;
use private_channel_indexer::config::ProgramType;
use private_channel_indexer::indexer::datasource::common::datasource::DataSource;
use private_channel_indexer::indexer::datasource::common::parser::withdraw::PRIVATE_CHANNEL_WITHDRAW_PROGRAM_ID;
use private_channel_indexer::indexer::datasource::common::types::ProcessorMessage;
use private_channel_indexer::indexer::datasource::rpc_polling::RpcPollingSource;
use serde_json::json;
use solana_sdk::commitment_config::CommitmentLevel;
use solana_sdk::pubkey::Pubkey;
use solana_transaction_status::UiTransactionEncoding;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

const BLOCKHASH: &str = "TestBlockHash11111111111111111111111111111";

/// Deterministic account key from a seed.
fn test_pubkey(seed: u8) -> Pubkey {
    let mut bytes = [0u8; 32];
    bytes[0] = seed;
    Pubkey::new_from_array(bytes)
}

/// Mock `getSlot` replying with the chain tip.
fn mock_get_slot(server: &mut Server, slot: u64) -> mockito::Mock {
    server
        .mock("POST", "/")
        .match_body(mockito::Matcher::PartialJson(
            json!({ "method": "getSlot" }),
        ))
        .with_status(200)
        .with_body(json!({ "jsonrpc": "2.0", "result": slot, "id": 1 }).to_string())
        .expect_at_least(1)
        .create()
}

/// Mock `getBlocks(start, end)` replying with the slots that produced a block.
/// Body-matched on method and range so it coexists with the getBlock mocks.
fn mock_get_blocks(server: &mut Server, start: u64, end: u64, produced: &[u64]) -> mockito::Mock {
    server
        .mock("POST", "/")
        .match_body(mockito::Matcher::PartialJson(json!({
            "method": "getBlocks",
            "params": [start, end]
        })))
        .with_status(200)
        .with_body(json!({ "jsonrpc": "2.0", "result": produced, "id": 1 }).to_string())
        .create()
}

/// getBlock returns an empty but well-formed block, so the slot completes and polling
/// advances past it.
fn mock_get_block_success(server: &mut Server, slot: u64, expect_at_least: usize) -> mockito::Mock {
    server
        .mock("POST", "/")
        .match_body(mockito::Matcher::PartialJson(json!({
            "method": "getBlock",
            "params": [slot]
        })))
        .with_status(200)
        .with_body(
            json!({
                "jsonrpc": "2.0",
                "result": {
                    "blockhash": BLOCKHASH,
                    "parentSlot": slot - 1,
                    "transactions": []
                },
                "id": 1
            })
            .to_string(),
        )
        .expect_at_least(expect_at_least)
        .create()
}

/// A well-formed WithdrawFunds payload: discriminator 0, borsh amount (u64 LE), then
/// a None destination.
fn withdraw_ix_data() -> Vec<u8> {
    let mut data = vec![0u8];
    data.extend_from_slice(&1000u64.to_le_bytes());
    data.push(0);
    data
}

/// Program id and account keys for a top-level WithdrawFunds transaction, so a block built
/// from these yields exactly one indexed instruction when it decodes. `data` is the raw
/// instruction payload, so a caller can supply a truncated one the parser recognizes but
/// cannot decode.
fn withdraw_block_transaction(meta: serde_json::Value, data: Vec<u8>) -> serde_json::Value {
    let ix_data = bs58::encode(data).into_string();
    let mut account_keys = vec![PRIVATE_CHANNEL_WITHDRAW_PROGRAM_ID.to_string()];
    for seed in 1u8..=5 {
        account_keys.push(test_pubkey(seed).to_string());
    }
    json!({
        "transaction": {
            "signatures": ["sig_undecodable"],
            "message": {
                "accountKeys": account_keys,
                "instructions": [{
                    "programIdIndex": 0,
                    "accounts": [1, 2, 3, 4, 5],
                    "data": ix_data
                }]
            }
        },
        "meta": meta
    })
}

/// Successful transaction meta, with nothing the parser needs beyond the instruction.
fn successful_meta() -> serde_json::Value {
    json!({
        "err": null,
        "logMessages": null,
        "innerInstructions": null,
        "loadedAddresses": null
    })
}

/// getBlock returns a complete block whose in-scope WithdrawFunds carries only its
/// discriminator: an instruction the indexer supports, with a borsh body that cannot be
/// decoded. The block itself is well-formed, so only the parser can catch this.
fn mock_get_block_undecodable_withdraw(
    server: &mut Server,
    slot: u64,
    expect_at_least: usize,
) -> mockito::Mock {
    mock_get_block_withdraw(server, slot, BLOCKHASH, vec![0u8], expect_at_least)
}

/// getBlock returns the same in-scope transaction with a payload that decodes, so the
/// WithdrawFunds is indexed.
fn mock_get_block_complete_withdraw(
    server: &mut Server,
    slot: u64,
    expect_at_least: usize,
) -> mockito::Mock {
    mock_get_block_withdraw(server, slot, BLOCKHASH, withdraw_ix_data(), expect_at_least)
}

/// getBlock returns one successful WithdrawFunds carrying `data`, under a caller-chosen
/// blockhash so a test can exercise the primary/fallback cross-check.
fn mock_get_block_withdraw(
    server: &mut Server,
    slot: u64,
    blockhash: &str,
    data: Vec<u8>,
    expect_at_least: usize,
) -> mockito::Mock {
    server
        .mock("POST", "/")
        .match_body(mockito::Matcher::PartialJson(json!({
            "method": "getBlock",
            "params": [slot]
        })))
        .with_status(200)
        .with_body(
            json!({
                "jsonrpc": "2.0",
                "result": {
                    "blockhash": blockhash,
                    "parentSlot": slot - 1,
                    "transactions": [withdraw_block_transaction(successful_meta(), data)]
                },
                "id": 1
            })
            .to_string(),
        )
        .expect_at_least(expect_at_least)
        .create()
}

/// A withdraw-indexing source polling `primary`, optionally failing over to `fallback`.
fn withdraw_source(
    primary_url: String,
    fallback_url: Option<String>,
    batch_size: usize,
) -> RpcPollingSource {
    RpcPollingSource::new(
        primary_url,
        Some(100),
        10,
        10,
        batch_size,
        UiTransactionEncoding::Json,
        CommitmentLevel::Finalized,
        ProgramType::Withdraw,
        None,
        fallback_url,
    )
}

/// Drain whatever the source emitted before cancellation.
fn drain(rx: &mut mpsc::Receiver<ProcessorMessage>) -> Vec<ProcessorMessage> {
    let mut messages = vec![];
    while let Ok(message) = rx.try_recv() {
        messages.push(message);
    }
    messages
}

fn completed_slot_100(messages: &[ProcessorMessage]) -> bool {
    messages
        .iter()
        .any(|message| matches!(message, ProcessorMessage::SlotComplete { slot, .. } if *slot == 100))
}

fn emitted_instruction(messages: &[ProcessorMessage]) -> bool {
    messages
        .iter()
        .any(|message| matches!(message, ProcessorMessage::Instruction(_)))
}

/// An instruction the indexer claims to support but cannot decode leaves the slot's
/// contents unknown, so the slot must not complete: completing it would checkpoint past a
/// real on-chain action that was never indexed, and normal polling would never come back
/// for it. Before the fail-closed arm the slot advanced and the instruction was lost for
/// good, so both assertions are falsifiable.
#[tokio::test]
async fn undecodable_instruction_does_not_emit_slot_complete_or_instruction() {
    let mut server = Server::new_async().await;

    // Chain tip ahead and batch size 1 so each poll asks for exactly [100].
    let _slot_mock = mock_get_slot(&mut server, 105);
    let _blocks_mock = mock_get_blocks(&mut server, 100, 100, &[100]);
    // Slot 100 always returns the undecodable payload; expect >=2 fetches proving no advance.
    let block_mock = mock_get_block_undecodable_withdraw(&mut server, 100, 2);

    let mut source = withdraw_source(server.url(), None, 1);

    let (tx, mut rx) = mpsc::channel(64);
    let cancel = CancellationToken::new();
    let handle = source.start(tx, cancel.clone()).await.unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;
    cancel.cancel();
    let _ = handle.await;

    // Slot 100 re-requested at least twice: polling did not advance past it.
    block_mock.assert();

    let messages = drain(&mut rx);
    assert!(
        !completed_slot_100(&messages),
        "SlotComplete{{slot:100}} must not be emitted for a slot holding an undecodable instruction"
    );
    assert!(
        !emitted_instruction(&messages),
        "no instruction may be emitted from a slot that failed to decode"
    );
}

/// The primary serves an instruction it cannot decode, but the fallback serves the same
/// block with a payload that does decode (the case where the primary strips metadata).
/// The slot is then indexed, completed, and polling advances.
#[tokio::test]
async fn undecodable_instruction_recovers_from_fallback() {
    let mut primary = Server::new_async().await;
    let mut fallback = Server::new_async().await;

    let _slot_mock = mock_get_slot(&mut primary, 103);
    let _blocks_mock = mock_get_blocks(&mut primary, 100, 102, &[100, 101, 102]);
    let _primary_block = mock_get_block_undecodable_withdraw(&mut primary, 100, 1);
    // Later slots resolve cleanly so the loop can advance past 100.
    let _primary_block_101 = mock_get_block_success(&mut primary, 101, 1);
    let _primary_block_102 = mock_get_block_success(&mut primary, 102, 1);
    let fallback_block = mock_get_block_complete_withdraw(&mut fallback, 100, 1);

    let mut source = withdraw_source(primary.url(), Some(fallback.url()), 10);

    let (tx, mut rx) = mpsc::channel(64);
    let cancel = CancellationToken::new();
    let handle = source.start(tx, cancel.clone()).await.unwrap();

    let mut saw_slot_100 = false;
    let mut saw_instruction = false;
    let deadline = tokio::time::Instant::now() + Duration::from_millis(600);
    while tokio::time::Instant::now() < deadline && !(saw_slot_100 && saw_instruction) {
        match tokio::time::timeout(Duration::from_millis(50), rx.recv()).await {
            Ok(Some(ProcessorMessage::SlotComplete { slot: 100, .. })) => saw_slot_100 = true,
            Ok(Some(ProcessorMessage::Instruction(_))) => saw_instruction = true,
            Ok(Some(_)) => {}
            _ => {}
        }
    }
    cancel.cancel();
    let _ = handle.await;

    fallback_block.assert();
    assert!(
        saw_slot_100,
        "SlotComplete{{slot:100}} must be emitted after fallback recovery"
    );
    assert!(
        saw_instruction,
        "the fallback block's WithdrawFunds must be indexed"
    );
}

/// Both endpoints serve the same undecodable instruction. The fallback is consulted and
/// rejected on decoding, not merely on its meta being present, so the slot fails closed.
#[tokio::test]
async fn undecodable_instruction_fallback_also_undecodable_fails_closed() {
    let mut primary = Server::new_async().await;
    let mut fallback = Server::new_async().await;

    let _slot_mock = mock_get_slot(&mut primary, 105);
    let _blocks_mock = mock_get_blocks(&mut primary, 100, 100, &[100]);
    let primary_block = mock_get_block_undecodable_withdraw(&mut primary, 100, 2);
    let fallback_block = mock_get_block_undecodable_withdraw(&mut fallback, 100, 1);

    let mut source = withdraw_source(primary.url(), Some(fallback.url()), 1);

    let (tx, mut rx) = mpsc::channel(64);
    let cancel = CancellationToken::new();
    let handle = source.start(tx, cancel.clone()).await.unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;
    cancel.cancel();
    let _ = handle.await;

    // Primary re-requested (no advance) and the fallback was consulted.
    primary_block.assert();
    fallback_block.assert();

    let messages = drain(&mut rx);
    assert!(
        !completed_slot_100(&messages),
        "SlotComplete{{slot:100}} must not be emitted when both endpoints serve an undecodable instruction"
    );
}

/// The fallback serves a decodable block, but for a different blockhash: a divergent fork
/// or wrong cluster. It must be rejected rather than indexed, so the slot fails closed.
#[tokio::test]
async fn undecodable_instruction_fallback_wrong_blockhash_fails_closed() {
    let mut primary = Server::new_async().await;
    let mut fallback = Server::new_async().await;

    let _slot_mock = mock_get_slot(&mut primary, 105);
    let _blocks_mock = mock_get_blocks(&mut primary, 100, 100, &[100]);
    let primary_block = mock_get_block_undecodable_withdraw(&mut primary, 100, 2);
    let fallback_block = mock_get_block_withdraw(
        &mut fallback,
        100,
        "DifferentBlockHash2222222222222222222222222",
        withdraw_ix_data(),
        1,
    );

    let mut source = withdraw_source(primary.url(), Some(fallback.url()), 1);

    let (tx, mut rx) = mpsc::channel(64);
    let cancel = CancellationToken::new();
    let handle = source.start(tx, cancel.clone()).await.unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;
    cancel.cancel();
    let _ = handle.await;

    primary_block.assert();
    fallback_block.assert();

    let messages = drain(&mut rx);
    assert!(
        !completed_slot_100(&messages),
        "SlotComplete{{slot:100}} must not be emitted when the fallback blockhash differs"
    );
    assert!(
        !emitted_instruction(&messages),
        "a fallback block on a different fork must not be indexed"
    );
}
