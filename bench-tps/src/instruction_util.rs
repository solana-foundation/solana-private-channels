//! Convert codama v3 `Instruction` → `solana_sdk::instruction::Instruction` (v2.x).
//!
//! Duplicated in `indexer/src/operator/utils/instruction_util.rs` and
//! `scripts/devnet/src/lib.rs`. The three consumers share no lightweight
//! common dep, and a new crate for an 8-line helper isn't worth it.
//!
//! Safe to duplicate: both types are 32-byte newtypes, so the only correct
//! body is "copy the bytes and rebuild the struct" — no behavior to drift on.
//! All three copies go away when the workspace migrates off `solana-sdk 2.x`.

use solana_sdk::{
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
};

/// Convert a codama/`solana_instruction` v3 `Instruction` into the
/// `solana_sdk::instruction::Instruction` consumed by `RpcClient` and
/// `Transaction::new_signed_with_payer`.
pub fn ix_v3_to_sdk(ix: solana_instruction::Instruction) -> Instruction {
    Instruction {
        program_id: Pubkey::new_from_array(ix.program_id.to_bytes()),
        accounts: ix
            .accounts
            .into_iter()
            .map(|m| AccountMeta {
                pubkey: Pubkey::new_from_array(m.pubkey.to_bytes()),
                is_signer: m.is_signer,
                is_writable: m.is_writable,
            })
            .collect(),
        data: ix.data,
    }
}
