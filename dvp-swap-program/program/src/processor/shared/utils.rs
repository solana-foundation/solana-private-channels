use pinocchio::{account::AccountView, error::ProgramError};

use crate::processor::shared::token_utils::MAX_HOOK_REMAINING_ACCOUNTS;

#[macro_export]
macro_rules! require_len {
    ($data:expr, $len:expr) => {
        if $data.len() < $len {
            return Err(ProgramError::InvalidInstructionData);
        }
    };
}

#[macro_export]
macro_rules! require {
    ($condition:expr, $error:expr) => {
        if !$condition {
            return Err($error.into());
        }
    };
}

/// Splits an instruction's account slice into a fixed prefix and two
/// trailing transfer-hook extras slices, one per leg.
///
/// Settle, Cancel and Reject all take a fixed account prefix followed
/// by a variable number of transfer-hook accounts; the split point
/// between leg A's and leg B's extras is the first byte of the
/// instruction data (`leg_a_extras_count: u8`). This helper centralises
/// the parse + bounds-check + split so each processor only writes the
/// fixed-prefix destructuring.
#[inline(always)]
#[allow(clippy::type_complexity)]
pub fn split_leg_remaining_accounts<'a>(
    accounts: &'a [AccountView],
    instruction_data: &[u8],
    fixed_len: usize,
) -> Result<(&'a [AccountView], &'a [AccountView], &'a [AccountView]), ProgramError> {
    if instruction_data.is_empty() {
        return Err(ProgramError::InvalidInstructionData);
    }
    let leg_a_extras_count = instruction_data[0] as usize;

    require!(
        accounts.len() >= fixed_len,
        ProgramError::NotEnoughAccountKeys
    );
    let (fixed, remaining) = accounts.split_at(fixed_len);

    require!(
        leg_a_extras_count <= remaining.len(),
        ProgramError::InvalidInstructionData
    );
    let (leg_a_extras, leg_b_extras) = remaining.split_at(leg_a_extras_count);
    // Both legs must fit the per-CPI cap that `transfer_checked_cpi`
    // enforces; checking here surfaces an obvious instruction-data
    // error rather than the late `InvalidArgument` from inside the CPI.
    require!(
        leg_a_extras.len() <= MAX_HOOK_REMAINING_ACCOUNTS
            && leg_b_extras.len() <= MAX_HOOK_REMAINING_ACCOUNTS,
        ProgramError::InvalidInstructionData
    );
    Ok((fixed, leg_a_extras, leg_b_extras))
}
