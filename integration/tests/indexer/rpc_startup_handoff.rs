//! RPC polling backfill->live handoff.
//!
//! `rpc_live_from_slot` seeds the live poller at `T0 + 1` (one past the
//! inclusive backfill target) so the poller covers the seam slots instead of
//! jumping to an independently-sampled chain tip. This drives a real
//! `RpcPollingSource` seeded at `T0 + 1` against a mockito RPC backend and
//! asserts the seam slots (and an instruction landing inside the seam) reach
//! the processor channel. Reuses the mockito getSlot/getBlock pattern from the
//! `rpc_polling/source.rs` unit tests.

use mockito::{Matcher, Server as MockitoServer};
use private_channel_indexer::config::ProgramType;
use private_channel_indexer::indexer::datasource::common::datasource::DataSource;
use private_channel_indexer::indexer::datasource::common::types::ProcessorMessage;
use private_channel_indexer::indexer::datasource::rpc_polling::RpcPollingSource;
use serde_json::json;
use solana_sdk::commitment_config::CommitmentLevel;
use solana_transaction_status::UiTransactionEncoding;
use std::collections::HashSet;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

// Inclusive backfill target; the live poller is seeded at SEED = T0 + 1.
const T0: u64 = 110;
const SEED: u64 = T0 + 1;
// Chain tip (exclusive upper bound of get_slots_to_process), so the seam the
// poller must cover before reaching the tip is SEED..=TIP-1 (111..=114).
const TIP: u64 = T0 + 5;
// A withdraw instruction lands inside the seam, proving real data is captured.
const INSTR_SLOT: u64 = T0 + 3;

const WITHDRAW_PROGRAM_ID: &str = "J231K9UEpS4y4KAPwGc4gsMNCjKFRMYcQBcjVW7vBhVi";

fn mock_get_slot(server: &mut MockitoServer, slot: u64) -> mockito::Mock {
    server
        .mock("POST", "/")
        .match_body(Matcher::PartialJson(json!({"method": "getSlot"})))
        .with_status(200)
        .with_body(json!({"jsonrpc": "2.0", "result": slot, "id": 1}).to_string())
        .expect_at_least(1)
        .create()
}

fn mock_empty_block(server: &mut MockitoServer, slot: u64) -> mockito::Mock {
    server
        .mock("POST", "/")
        .match_body(Matcher::PartialJson(
            json!({"method": "getBlock", "params": [slot]}),
        ))
        .with_status(200)
        .with_body(
            json!({
                "jsonrpc": "2.0",
                "result": {
                    "blockhash": "TestBlockHash11111111111111111111111111111",
                    "parentSlot": slot - 1,
                    "transactions": []
                },
                "id": 1
            })
            .to_string(),
        )
        .expect_at_least(1)
        .create()
}

/// base58-encoded WithdrawFunds payload: discriminator 0, amount, None destination.
fn withdraw_funds_data() -> String {
    let mut data = vec![0u8];
    data.extend_from_slice(&1000u64.to_le_bytes());
    data.push(0);
    bs58::encode(data).into_string()
}

fn mock_withdraw_block(server: &mut MockitoServer, slot: u64) -> mockito::Mock {
    // accountKeys 0..4 are the WithdrawFunds accounts; index 5 is the program.
    let account_keys = json!([
        "11111111111111111111111111111111",
        "11111111111111111111111111111111",
        "11111111111111111111111111111111",
        "11111111111111111111111111111111",
        "11111111111111111111111111111111",
        WITHDRAW_PROGRAM_ID
    ]);
    server
        .mock("POST", "/")
        .match_body(Matcher::PartialJson(
            json!({"method": "getBlock", "params": [slot]}),
        ))
        .with_status(200)
        .with_body(
            json!({
                "jsonrpc": "2.0",
                "result": {
                    "blockhash": "TestBlockHash11111111111111111111111111111",
                    "parentSlot": slot - 1,
                    "transactions": [{
                        "transaction": {
                            "signatures": ["5".repeat(64)],
                            "message": {
                                "accountKeys": account_keys,
                                "instructions": [{
                                    "programIdIndex": 5,
                                    "accounts": [0, 1, 2, 3, 4],
                                    "data": withdraw_funds_data()
                                }]
                            }
                        },
                        "meta": null
                    }]
                },
                "id": 1
            })
            .to_string(),
        )
        .expect_at_least(1)
        .create()
}

/// Seeded at T0+1, the poller emits SlotComplete for the seam slots
/// (T0+1..=TIP-1) and the withdraw instruction in the seam block - it covers
/// the handoff window instead of jumping to the chain tip.
#[tokio::test]
async fn live_poller_seeded_at_t0_plus_1_emits_seam_slots() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,private_channel_indexer=debug")
        .with_test_writer()
        .try_init();

    let mut server = MockitoServer::new_async().await;
    let _slot = mock_get_slot(&mut server, TIP);
    let _b111 = mock_empty_block(&mut server, SEED);
    let _b112 = mock_empty_block(&mut server, T0 + 2);
    let _b113 = mock_withdraw_block(&mut server, INSTR_SLOT);
    let _b114 = mock_empty_block(&mut server, T0 + 4);

    let mut source = RpcPollingSource::new(
        server.url(),
        Some(SEED),
        10,
        10,
        16,
        UiTransactionEncoding::Json,
        CommitmentLevel::Finalized,
        ProgramType::Withdraw,
        None,
    );

    let (tx, mut rx) = mpsc::channel::<ProcessorMessage>(64);
    let cancel = CancellationToken::new();
    let handle = source.start(tx, cancel.clone()).await.unwrap();

    let wanted: HashSet<u64> = (SEED..TIP).collect(); // 111..=114, exclusive of tip
    let mut seen_slots: HashSet<u64> = HashSet::new();
    let mut saw_instruction = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !(wanted.is_subset(&seen_slots) && saw_instruction) {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            panic!(
                "timed out; slots seen: {:?}, instruction: {}",
                seen_slots, saw_instruction
            );
        }
        match tokio::time::timeout(remaining, rx.recv()).await {
            Ok(Some(ProcessorMessage::SlotComplete { slot, .. })) => {
                seen_slots.insert(slot);
            }
            Ok(Some(ProcessorMessage::Instruction(meta))) if meta.slot == INSTR_SLOT => {
                saw_instruction = true;
            }
            _ => {}
        }
    }

    cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(3), handle).await;

    assert!(
        wanted.is_subset(&seen_slots),
        "poller must cover the seam {:?}; saw {:?}",
        wanted,
        seen_slots
    );
    assert!(
        saw_instruction,
        "the withdraw instruction in the seam block must be emitted"
    );
    assert!(
        !seen_slots.contains(&TIP),
        "the tip slot is above the seam and must not be fetched in this window"
    );
}
