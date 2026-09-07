//! Shared builders for the Yellowstone `blocks` integration tests.
//!
//! Each yellowstone `[[test]]` binary includes this file via
//! `#[path = "yellowstone_helpers.rs"] mod yellowstone_helpers;`, so a given
//! binary only exercises a subset of the builders below.
#![allow(dead_code)]

use std::str::FromStr;

use private_channel_indexer::indexer::datasource::common::parser::escrow::PRIVATE_CHANNEL_ESCROW_PROGRAM_ID;
use yellowstone_grpc_proto::geyser::{
    subscribe_update::UpdateOneof, SubscribeUpdate, SubscribeUpdateBlock, SubscribeUpdateSlot,
    SubscribeUpdateTransactionInfo,
};
use yellowstone_grpc_proto::solana::storage::confirmed_block::{
    CompiledInstruction as ProtoCompiledInstruction, InnerInstruction as ProtoInnerInstruction,
    InnerInstructions as ProtoInnerInstructions, Message as ProtoMessage, MessageHeader,
    Transaction as ProtoTransaction, TransactionStatusMeta,
};

/// Wrap tx infos in one atomic block update for `slot`. Each block delivers the
/// slot's boundary and all of its transactions in a single message.
pub fn block(slot: u64, txs: Vec<SubscribeUpdateTransactionInfo>) -> SubscribeUpdate {
    SubscribeUpdate {
        filters: vec!["private_channel_blocks".to_string()],
        update_oneof: Some(UpdateOneof::Block(SubscribeUpdateBlock {
            slot,
            transactions: txs,
            ..Default::default()
        })),
        created_at: None,
    }
}

/// A produced block with no program transactions still completes its slot.
pub fn empty_block(slot: u64) -> SubscribeUpdate {
    block(slot, vec![])
}

/// The stream's own resume point. Carries no transactions, so it only arms the gap gate.
pub fn slot_update(slot: u64) -> SubscribeUpdate {
    SubscribeUpdate {
        filters: vec!["private_channel_slots".to_string()],
        update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
            slot,
            ..Default::default()
        })),
        created_at: None,
    }
}

/// The DepositEvent CPI payload the escrow parser reads the authoritative
/// amount from: EVENT_IX_TAG(8) + disc=6 + instance_seed(32) + user(32)
/// + amount LE(8) + recipient(32) + mint(32).
fn deposit_event_data(amount: u64) -> Vec<u8> {
    let mut event_data = vec![];
    event_data.extend_from_slice(&0x1d9acb512ea545e4u64.to_le_bytes());
    event_data.push(6);
    event_data.extend_from_slice(&[0u8; 32]);
    event_data.extend_from_slice(&[0u8; 32]);
    event_data.extend_from_slice(&amount.to_le_bytes());
    event_data.extend_from_slice(&[0u8; 32]);
    event_data.extend_from_slice(&[0u8; 32]);
    event_data
}

/// An escrow Deposit tx (discriminator 6 + amount + `Option::None`) carrying the
/// matching DepositEvent CPI in `meta.inner_instructions`. Padded account list
/// so the 12 required Deposit accounts resolve.
pub fn escrow_deposit_tx_info() -> SubscribeUpdateTransactionInfo {
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

    let meta = TransactionStatusMeta {
        inner_instructions: vec![ProtoInnerInstructions {
            index: 0,
            instructions: vec![ProtoInnerInstruction {
                program_id_index: 12,
                accounts: vec![0u8, 1, 2],
                data: deposit_event_data(1_000),
                stack_height: Some(2),
            }],
        }],
        ..Default::default()
    };

    SubscribeUpdateTransactionInfo {
        signature: vec![7u8; 64],
        is_vote: false,
        transaction: Some(ProtoTransaction {
            signatures: vec![vec![7u8; 64]],
            message: Some(message),
        }),
        meta: Some(meta),
        index: 0,
    }
}

/// An escrow tx whose top-level discriminator (0xFE) has no parser arm, so it
/// yields `Ok(None)` and is silently filtered.
pub fn unknown_discriminator_tx_info() -> SubscribeUpdateTransactionInfo {
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

    let instruction = ProtoCompiledInstruction {
        program_id_index: 12,
        accounts: (0u8..12).collect(),
        data: vec![0xFEu8],
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

    SubscribeUpdateTransactionInfo {
        signature: vec![0x42u8; 64],
        is_vote: false,
        transaction: Some(ProtoTransaction {
            signatures: vec![vec![0x42u8; 64]],
            message: Some(message),
        }),
        meta: Some(TransactionStatusMeta::default()),
        index: 0,
    }
}

/// A tx whose `program_id_index` (99) exceeds the 3-key account list; the
/// source's bounds check skips it without forwarding an instruction.
pub fn bad_program_index_tx_info() -> SubscribeUpdateTransactionInfo {
    let account_keys: Vec<Vec<u8>> = (0..3)
        .map(|i| {
            let mut bytes = [0u8; 32];
            bytes[0] = (i + 1) as u8;
            bytes.to_vec()
        })
        .collect();

    let instruction = ProtoCompiledInstruction {
        program_id_index: 99,
        accounts: vec![0u8, 1, 2],
        data: vec![0x00],
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

    SubscribeUpdateTransactionInfo {
        signature: vec![0x77u8; 64],
        is_vote: false,
        transaction: Some(ProtoTransaction {
            signatures: vec![vec![0x77u8; 64]],
            message: Some(message),
        }),
        meta: Some(TransactionStatusMeta::default()),
        index: 0,
    }
}

/// A tx missing its inner `transaction.message`, tripping the "Missing message"
/// Err path in the block handler (fail-closed).
pub fn missing_message_tx_info() -> SubscribeUpdateTransactionInfo {
    SubscribeUpdateTransactionInfo {
        signature: vec![0x55; 64],
        is_vote: false,
        transaction: Some(ProtoTransaction {
            signatures: vec![vec![0x55; 64]],
            message: None,
        }),
        meta: Some(TransactionStatusMeta::default()),
        index: 0,
    }
}

/// A Deposit-shaped tx whose instruction targets `program_id` instead of the
/// escrow program, so the client-side program filter drops it.
pub fn wrong_program_tx_info(
    program_id: solana_sdk::pubkey::Pubkey,
) -> SubscribeUpdateTransactionInfo {
    let mut account_keys: Vec<Vec<u8>> = (0..12)
        .map(|i| {
            let mut bytes = [0u8; 32];
            bytes[0] = (i + 1) as u8;
            bytes.to_vec()
        })
        .collect();
    account_keys.push(program_id.to_bytes().to_vec());

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

    SubscribeUpdateTransactionInfo {
        signature: vec![7u8; 64],
        is_vote: false,
        transaction: Some(ProtoTransaction {
            signatures: vec![vec![7u8; 64]],
            message: Some(message),
        }),
        meta: Some(TransactionStatusMeta::default()),
        index: 0,
    }
}
