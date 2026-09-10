//! Proto → VersionedMessage conversion helper vendored from upstream.
//!
//! In yellowstone-grpc v9 this lived at `yellowstone_grpc_proto::convert_from`
//! behind the `convert` feature. v12 removed the feature and relocated the
//! helper into `yellowstone-grpc-geyser` (a plugin binary crate), so external
//! consumers can no longer import it. The implementation is short and stable;
//! we vendor just the subset the indexer needs (the `create_message` call
//! chain) rather than pulling the full plugin crate.
//!
//! Upstream source:
//!   https://github.com/rpcpool/yellowstone-grpc/blob/
//!   v12.3.0+solana.3.1.13/yellowstone-grpc-geyser/src/plugin/convert_from.rs
//! License: Apache-2.0 (upstream). PrivateChannel repo is MIT; Apache-2.0 is compatible.

use {
    solana_sdk::{
        hash::{Hash, HASH_BYTES},
        message::{
            compiled_instruction::CompiledInstruction,
            v0::{Message as MessageV0, MessageAddressTableLookup},
            Message, MessageHeader, VersionedMessage,
        },
        pubkey::Pubkey,
    },
    yellowstone_grpc_proto::prelude as proto,
};

pub(super) type CreateResult<T> = Result<T, &'static str>;

pub(super) fn create_message(message: proto::Message) -> CreateResult<VersionedMessage> {
    let header = message.header.ok_or("failed to get MessageHeader")?;
    let header = MessageHeader {
        num_required_signatures: header
            .num_required_signatures
            .try_into()
            .map_err(|_| "failed to parse num_required_signatures")?,
        num_readonly_signed_accounts: header
            .num_readonly_signed_accounts
            .try_into()
            .map_err(|_| "failed to parse num_readonly_signed_accounts")?,
        num_readonly_unsigned_accounts: header
            .num_readonly_unsigned_accounts
            .try_into()
            .map_err(|_| "failed to parse num_readonly_unsigned_accounts")?,
    };

    if message.recent_blockhash.len() != HASH_BYTES {
        return Err("failed to parse hash");
    }

    // `versioned` is true for v0 and v1 alike, so v1 lands in the V0 arm. That
    // only loses `Message.config`, which we never read. v1 has no lookups, so the
    // vec stays empty. Upstream fixed the same thing in geyser 15.1.1:
    // https://github.com/rpcpool/yellowstone-grpc/blob/master/CHANGELOG.md
    Ok(if message.versioned {
        let mut address_table_lookups = Vec::with_capacity(message.address_table_lookups.len());
        for table in message.address_table_lookups {
            address_table_lookups.push(MessageAddressTableLookup {
                account_key: Pubkey::try_from(table.account_key.as_slice())
                    .map_err(|_| "failed to parse Pubkey")?,
                writable_indexes: table.writable_indexes,
                readonly_indexes: table.readonly_indexes,
            });
        }

        VersionedMessage::V0(MessageV0 {
            header,
            account_keys: create_pubkey_vec(message.account_keys)?,
            recent_blockhash: Hash::new_from_array(
                <[u8; HASH_BYTES]>::try_from(message.recent_blockhash.as_slice()).unwrap(),
            ),
            instructions: create_message_instructions(message.instructions)?,
            address_table_lookups,
        })
    } else {
        VersionedMessage::Legacy(Message {
            header,
            account_keys: create_pubkey_vec(message.account_keys)?,
            recent_blockhash: Hash::new_from_array(
                <[u8; HASH_BYTES]>::try_from(message.recent_blockhash.as_slice()).unwrap(),
            ),
            instructions: create_message_instructions(message.instructions)?,
        })
    })
}

fn create_message_instructions(
    ixs: Vec<proto::CompiledInstruction>,
) -> CreateResult<Vec<CompiledInstruction>> {
    ixs.into_iter().map(create_message_instruction).collect()
}

fn create_message_instruction(ix: proto::CompiledInstruction) -> CreateResult<CompiledInstruction> {
    Ok(CompiledInstruction {
        program_id_index: ix
            .program_id_index
            .try_into()
            .map_err(|_| "failed to decode CompiledInstruction.program_id_index)")?,
        accounts: ix.accounts,
        data: ix.data,
    })
}

fn create_pubkey_vec(pubkeys: Vec<Vec<u8>>) -> CreateResult<Vec<Pubkey>> {
    pubkeys
        .iter()
        .map(|pubkey| Pubkey::try_from(pubkey.as_slice()).map_err(|_| "failed to parse Pubkey"))
        .collect()
}
