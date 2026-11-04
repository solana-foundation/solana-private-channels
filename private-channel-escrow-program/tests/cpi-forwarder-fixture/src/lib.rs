//! Test-only program that forwards its instruction to another program by CPI, so the
//! indexer sees escrow instructions as inner instructions as a wrapping program sends them.
//!
//! Account 0 is the target program; the rest go to it unchanged, with the same data.
//! Deployed at FZvL33G3brVb4Pv5wmqzQitbCzbjrxsYASHXsge3v2nk by the integration validator.
#![no_std]

use pinocchio::{
    account::AccountView,
    cpi::invoke_with_bounds,
    default_allocator,
    error::ProgramError,
    instruction::{InstructionAccount, InstructionView},
    nostd_panic_handler, program_entrypoint, Address, ProgramResult,
};

program_entrypoint!(process_instruction);
default_allocator!();
nostd_panic_handler!();

/// Enough for every escrow instruction plus hook extras.
const MAX_ACCOUNTS: usize = 32;

pub fn process_instruction(
    _program_id: &Address,
    accounts: &[AccountView],
    instruction_data: &[u8],
) -> ProgramResult {
    let (target, rest) = accounts
        .split_first()
        .ok_or(ProgramError::NotEnoughAccountKeys)?;
    if rest.len() > MAX_ACCOUNTS {
        return Err(ProgramError::InvalidArgument);
    }
    let metas: [InstructionAccount; MAX_ACCOUNTS] = core::array::from_fn(|i| {
        rest.get(i).map_or(
            InstructionAccount::readonly(target.address()),
            InstructionAccount::from,
        )
    });
    let views: [&AccountView; MAX_ACCOUNTS] =
        core::array::from_fn(|i| rest.get(i).unwrap_or(target));
    let instruction = InstructionView {
        program_id: target.address(),
        data: instruction_data,
        accounts: &metas[..rest.len()],
    };
    invoke_with_bounds::<MAX_ACCOUNTS>(&instruction, &views[..rest.len()])
}
