//! Minimal Token-2022 transfer-hook program used only by the swap
//! program's integration tests. Logs the account count it received and
//! returns `Ok`; tests use the log line to assert that
//! `transfer_checked_cpi` forwarded every account declared in the
//! mint's `ExtraAccountMetaList` PDA.
#![no_std]

use pinocchio::{
    account::AccountView, address::Address, default_allocator, nostd_panic_handler,
    program_entrypoint, ProgramResult,
};
use pinocchio_log::log;

solana_address::declare_id!("HookqJupt6Khm8s8jB3p93NkhPoiAg2M7vkEhkS15CtC");

program_entrypoint!(process_instruction);
default_allocator!();
nostd_panic_handler!();

pub fn process_instruction(
    _program_id: &Address,
    accounts: &[AccountView],
    _instruction_data: &[u8],
) -> ProgramResult {
    log!("hook accounts: {}", accounts.len());
    Ok(())
}
