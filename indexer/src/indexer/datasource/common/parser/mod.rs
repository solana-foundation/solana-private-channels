pub mod escrow;
pub mod pubkey;
pub mod withdraw;

pub use escrow::*;
pub use pubkey::*;
pub use withdraw::*;

use crate::error::{account::AccountError, ParserError};
use crate::indexer::datasource::common::types::CompiledInstruction;
use solana_sdk::pubkey::Pubkey;

/// Look up the pubkey of the instruction's `pos`-th account. Accounts are stored
/// as positions into `account_keys`, so this bounds-checks both `pos` and that
/// position and returns a `ParserError` (never panics) if either is out of range.
pub(crate) fn resolve_account(
    instruction: &CompiledInstruction,
    account_keys: &[Pubkey],
    pos: usize,
) -> Result<Pubkey, ParserError> {
    let key_index = *instruction
        .accounts
        .get(pos)
        .ok_or(AccountError::AccountIndexOutOfBounds { index: pos })? as usize;
    account_keys
        .get(key_index)
        .copied()
        .ok_or_else(|| AccountError::AccountIndexOutOfBounds { index: key_index }.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::pubkey::test_pubkey;

    fn ix(accounts: Vec<u8>) -> CompiledInstruction {
        CompiledInstruction {
            program_id_index: 0,
            accounts,
            data: String::new(),
        }
    }

    #[test]
    fn resolve_account_returns_keyed_pubkey() {
        let keys = [test_pubkey(10), test_pubkey(11), test_pubkey(12)];
        // accounts[1] -> account_keys[2]
        let result = resolve_account(&ix(vec![0, 2, 1]), &keys, 1);
        assert_eq!(result.unwrap(), test_pubkey(12));
    }

    #[test]
    fn resolve_account_pos_past_instruction_accounts_errs() {
        let keys = [test_pubkey(10)];
        let result = resolve_account(&ix(vec![0]), &keys, 5);
        assert!(result.unwrap_err().to_string().contains("out of bounds"));
    }

    #[test]
    fn resolve_account_index_past_key_list_errs() {
        let keys = [test_pubkey(10)];
        // accounts[0] points at key index 7, but only one key exists.
        let result = resolve_account(&ix(vec![7]), &keys, 0);
        assert!(result.unwrap_err().to_string().contains("out of bounds"));
    }
}
