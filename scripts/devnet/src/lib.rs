//! Helpers shared by the devnet bin scripts. Convert codama v3
//! `Address`/`Instruction` → `solana_sdk` 2.x `Pubkey`/`Instruction` at the
//! boundary so the bin code can keep using `Pubkey`.
//!
//! The instruction conversion is duplicated in
//! `indexer/src/operator/utils/instruction_util.rs` and
//! `bench-tps/src/instruction_util.rs`. The three consumers share no
//! lightweight common dep, and a new crate for an 8-line helper isn't
//! worth it.
//!
//! Safe to duplicate: both types are 32-byte newtypes, so the only correct
//! body is "copy the bytes and rebuild the struct" — no behavior to drift on.
//! All three copies go away when the workspace migrates off `solana-sdk 2.x`.

use solana_sdk::{
    instruction::{AccountMeta as SdkAccountMeta, Instruction as SdkInstruction},
    pubkey::Pubkey,
};

/// Convert a solana-sdk 2.x `Pubkey` into a solana-address 2.x `Address`.
pub fn to_addr<P: PubkeyLike>(p: P) -> solana_address::Address {
    solana_address::Address::new_from_array(p.pubkey_bytes())
}

pub trait PubkeyLike {
    fn pubkey_bytes(&self) -> [u8; 32];
}

impl PubkeyLike for Pubkey {
    fn pubkey_bytes(&self) -> [u8; 32] {
        self.to_bytes()
    }
}

impl PubkeyLike for &Pubkey {
    fn pubkey_bytes(&self) -> [u8; 32] {
        (**self).to_bytes()
    }
}

/// Convert the v3 `solana_instruction::Instruction` emitted by codama-generated
/// builders into the solana-sdk 2.x `Instruction` accepted by `solana-client`
/// 2.x's `send_and_confirm_transaction`.
pub fn ix_v3_to_sdk(ix: solana_instruction::Instruction) -> SdkInstruction {
    SdkInstruction {
        program_id: Pubkey::new_from_array(ix.program_id.to_bytes()),
        accounts: ix
            .accounts
            .into_iter()
            .map(|m| SdkAccountMeta {
                pubkey: Pubkey::new_from_array(m.pubkey.to_bytes()),
                is_signer: m.is_signer,
                is_writable: m.is_writable,
            })
            .collect(),
        data: ix.data,
    }
}
