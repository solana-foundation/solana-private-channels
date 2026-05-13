use pinocchio::{account::AccountView, entrypoint, error::ProgramError, Address, ProgramResult};

use crate::{
    discriminator::DvpSwapInstructionDiscriminators,
    processor::{
        process_cancel_dvp, process_create_dvp, process_reclaim_dvp, process_reject_dvp,
        process_settle_dvp,
    },
};

entrypoint!(process_instruction);

pub fn process_instruction(
    program_id: &Address,
    accounts: &[AccountView],
    instruction_data: &[u8],
) -> ProgramResult {
    let (discriminator, instruction_data) = instruction_data
        .split_first()
        .ok_or(ProgramError::InvalidInstructionData)?;

    let discriminator = DvpSwapInstructionDiscriminators::try_from(*discriminator)
        .map_err(|_| ProgramError::InvalidInstructionData)?;

    match discriminator {
        DvpSwapInstructionDiscriminators::CreateDvp => {
            process_create_dvp(program_id, accounts, instruction_data)
        }
        DvpSwapInstructionDiscriminators::ReclaimDvp => {
            process_reclaim_dvp(program_id, accounts, instruction_data)
        }
        DvpSwapInstructionDiscriminators::SettleDvp => {
            process_settle_dvp(program_id, accounts, instruction_data)
        }
        DvpSwapInstructionDiscriminators::CancelDvp => {
            process_cancel_dvp(program_id, accounts, instruction_data)
        }
        DvpSwapInstructionDiscriminators::RejectDvp => {
            process_reject_dvp(program_id, accounts, instruction_data)
        }
    }
}
