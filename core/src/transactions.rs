use solana_sdk::message::VersionedMessage;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::transaction::SanitizedTransaction;
use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;

const SPL_INITIALIZE_MINT: u8 = 0;
const SPL_INITIALIZE_MINT2: u8 = 20;

/// A lazy-initialized static mapping from program_id (Pubkey) to a HashSet of admin instruction types (u8)
pub static ADMIN_INSTRUCTIONS_MAP: LazyLock<HashMap<Pubkey, HashSet<u8>>> = LazyLock::new(|| {
    HashMap::from([(
        spl_token::id(),
        HashSet::from([SPL_INITIALIZE_MINT, SPL_INITIALIZE_MINT2]),
    )])
});

/// Checks if an instruction is an admin-only instruction
pub fn is_admin_instruction(program_id: &Pubkey, instruction_type: u8) -> bool {
    ADMIN_INSTRUCTIONS_MAP
        .get(program_id)
        .is_some_and(|set| set.contains(&instruction_type))
}

// TODO: Make this configurable at startup
/// Checks if an instruction is allowed. Currently, only SPL instructions are
/// allowed
pub fn is_allowed_instruction(program_id: &Pubkey, _instruction_type: u8) -> bool {
    program_id == &spl_token::id()
}

/// Admission policy for a single top-level instruction. System is refused: SOL
/// has no use here, and a Transfer to a new address creates a permanent account
/// the gasless model never charges for. System still runs as a CPI target.
pub fn is_allowed_program_instruction(program_id: &Pubkey, _data: &[u8]) -> bool {
    *program_id == spl_token::id()
        || *program_id == spl_associated_token_account::id()
        || *program_id == spl_memo::id()
        || *program_id
            == private_channel_withdraw_program_client::PRIVATE_CHANNEL_WITHDRAW_PROGRAM_ID
        || *program_id == dvp_swap_program_client::DVP_SWAP_PROGRAM_ID
}

/// Rejection reason for a transaction with a top-level instruction outside the allowlist.
pub const PROGRAM_NOT_ALLOWED: &str =
    "Only SPL token, ATA, Memo, Withdraw, and Swap program transactions are accepted";

/// Whether every top-level instruction of `tx` passes the admission policy.
pub fn all_instructions_allowed(tx: &SanitizedTransaction) -> bool {
    tx.message()
        .program_instructions_iter()
        .all(|(program_id, ix)| is_allowed_program_instruction(program_id, &ix.data))
}

/// Rejection reason for a transaction that lists the spl-token native mint.
pub const NATIVE_MINT_UNSUPPORTED: &str = "The native mint (wSOL) is not supported on this channel";

/// spl-token builds a native (wSOL) account without loading the mint, so refusing
/// any tx that lists it keeps fabricated gasless lamports from becoming wSOL. A CPI
/// can only reach listed keys, and lookup tables are refused before this runs.
pub fn lists_native_mint(tx: &SanitizedTransaction) -> bool {
    tx.message()
        .account_keys()
        .iter()
        .any(|key| *key == spl_token::native_mint::id())
}

/// Rejection reason shared by every admission path, so clients see one wording.
pub const ADDRESS_LOOKUP_UNSUPPORTED: &str =
    "Address lookup tables are not supported; submit a transaction with no address table lookups";

/// True when a v0 message declares address table lookups.
///
/// No lookup table program is admitted here, so a declared lookup names a table
/// that cannot exist. Admitting one unresolved would leave the transaction's
/// account keys missing every address the lookup was supposed to supply.
pub fn has_address_table_lookups(message: &VersionedMessage) -> bool {
    // Exhaustive on purpose: a future message version must be classified here
    // rather than falling through to a silent false.
    match message {
        VersionedMessage::Legacy(_) => false,
        VersionedMessage::V0(m) => !m.address_table_lookups.is_empty(),
        // The v1 format has no lookup tables at all.
        VersionedMessage::V1(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::hash::Hash;
    use solana_sdk::message::{v0, v0::MessageAddressTableLookup, Message, MessageHeader};
    use solana_system_interface::instruction::SystemInstruction;

    fn v0_message(num_lookups: usize) -> VersionedMessage {
        let address_table_lookups = (0..num_lookups)
            .map(|_| MessageAddressTableLookup {
                account_key: Pubkey::new_unique(),
                writable_indexes: vec![0],
                readonly_indexes: vec![],
            })
            .collect();
        VersionedMessage::V0(v0::Message {
            header: MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            },
            account_keys: vec![Pubkey::new_unique(), spl_token::id()],
            recent_blockhash: Hash::default(),
            instructions: vec![],
            address_table_lookups,
        })
    }

    // The predicate keys on declaration, not on whether an instruction uses the
    // table, and legacy messages have no lookups to declare.
    #[test]
    fn address_table_lookup_detection() {
        let cases = [
            (
                "legacy",
                VersionedMessage::Legacy(Message::default()),
                false,
            ),
            ("v0 with no lookups", v0_message(0), false),
            ("v0 with one lookup", v0_message(1), true),
            ("v0 with two lookups", v0_message(2), true),
        ];
        for (label, message, expected) in cases {
            assert_eq!(
                has_address_table_lookups(&message),
                expected,
                "{label} must report {expected}"
            );
        }
    }

    #[test]
    fn spl_initialize_mint_is_admin() {
        assert!(is_admin_instruction(&spl_token::id(), 0));
    }

    #[test]
    fn spl_initialize_mint2_is_admin() {
        assert!(is_admin_instruction(&spl_token::id(), 20));
    }

    #[test]
    fn spl_transfer_is_not_admin() {
        // SPL token transfer = instruction type 3
        assert!(!is_admin_instruction(&spl_token::id(), 3));
    }

    #[test]
    fn unknown_program_is_not_admin() {
        let random = Pubkey::new_unique();
        assert!(!is_admin_instruction(&random, 0));
    }

    #[test]
    fn test_is_allowed_instruction_spl_token() {
        // SPL token transfer (type 3) should be allowed
        assert!(is_allowed_instruction(&spl_token::id(), 3));
        // SPL token initialize mint (type 0) should also be allowed
        assert!(is_allowed_instruction(&spl_token::id(), 0));
    }

    #[test]
    fn test_is_allowed_instruction_unknown() {
        let random = Pubkey::new_unique();
        assert!(!is_allowed_instruction(&random, 0));
        assert!(!is_allowed_instruction(&random, 3));
    }

    // ── Admission policy (`is_allowed_program_instruction`) ──────────────────

    fn system_id() -> Pubkey {
        solana_sdk_ids::system_program::ID
    }

    fn encode(ix: &SystemInstruction) -> Vec<u8> {
        bincode::serialize(ix).unwrap()
    }

    /// A1: System is refused, Transfer included, whatever the data.
    #[test]
    fn system_transfer_is_rejected() {
        let data = encode(&SystemInstruction::Transfer { lamports: 1 });
        for data in [&data[..], &[][..], &[2][..], &[2, 0, 0][..]] {
            assert!(
                !is_allowed_program_instruction(&system_id(), data),
                "System data {data:?} must be rejected"
            );
        }
    }

    // A2: serialize the real enum so every variant, present and future, is
    // pinned as refused.
    #[test]
    fn every_system_variant_is_rejected() {
        let owner = Pubkey::new_unique();
        let base = Pubkey::new_unique();
        let cases: [(&str, SystemInstruction); 13] = [
            ("Transfer", SystemInstruction::Transfer { lamports: 1 }),
            (
                "CreateAccount",
                SystemInstruction::CreateAccount {
                    lamports: 0,
                    space: 10 * 1024 * 1024,
                    owner,
                },
            ),
            ("Assign", SystemInstruction::Assign { owner }),
            (
                "CreateAccountWithSeed",
                SystemInstruction::CreateAccountWithSeed {
                    base,
                    seed: "seed".to_string(),
                    lamports: 0,
                    space: 10 * 1024 * 1024,
                    owner,
                },
            ),
            (
                "AdvanceNonceAccount",
                SystemInstruction::AdvanceNonceAccount,
            ),
            (
                "WithdrawNonceAccount",
                SystemInstruction::WithdrawNonceAccount(1),
            ),
            (
                "InitializeNonceAccount",
                SystemInstruction::InitializeNonceAccount(owner),
            ),
            (
                "AuthorizeNonceAccount",
                SystemInstruction::AuthorizeNonceAccount(owner),
            ),
            (
                "Allocate",
                SystemInstruction::Allocate {
                    space: 10 * 1024 * 1024,
                },
            ),
            (
                "AllocateWithSeed",
                SystemInstruction::AllocateWithSeed {
                    base,
                    seed: "seed".to_string(),
                    space: 10 * 1024 * 1024,
                    owner,
                },
            ),
            (
                "AssignWithSeed",
                SystemInstruction::AssignWithSeed {
                    base,
                    seed: "seed".to_string(),
                    owner,
                },
            ),
            (
                "TransferWithSeed",
                SystemInstruction::TransferWithSeed {
                    lamports: 1,
                    from_seed: "seed".to_string(),
                    from_owner: owner,
                },
            ),
            (
                "UpgradeNonceAccount",
                SystemInstruction::UpgradeNonceAccount,
            ),
        ];

        for (name, ix) in cases {
            let data = encode(&ix);
            assert!(
                !is_allowed_program_instruction(&system_id(), &data),
                "System::{name} must be rejected at ingress"
            );
        }
    }

    /// A6: non-System allowlisted programs are admitted for any instruction data.
    #[test]
    fn allowlisted_programs_admit_any_data() {
        let programs = [
            spl_token::id(),
            spl_associated_token_account::id(),
            spl_memo::id(),
            private_channel_withdraw_program_client::PRIVATE_CHANNEL_WITHDRAW_PROGRAM_ID,
            dvp_swap_program_client::DVP_SWAP_PROGRAM_ID,
        ];
        for program_id in programs {
            for data in [&[][..], &[0xff; 8][..]] {
                assert!(
                    is_allowed_program_instruction(&program_id, data),
                    "allowlisted program {program_id} must stay admitted"
                );
            }
        }
    }

    /// A7: an unknown program is rejected whatever the data.
    #[test]
    fn unknown_program_is_rejected() {
        let random = Pubkey::new_unique();
        for data in [&[][..], &[2, 0, 0, 0][..], &[0xff; 8][..]] {
            assert!(!is_allowed_program_instruction(&random, data));
        }
    }
}
