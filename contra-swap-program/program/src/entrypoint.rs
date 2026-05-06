use pinocchio::{account::AccountView, entrypoint, error::ProgramError, Address, ProgramResult};

use crate::{
    discriminator::ContraSwapInstructionDiscriminators,
    processor::{
        process_cancel_dvp, process_create_dvp, process_fund_dvp, process_reclaim_dvp,
        process_reject_dvp, process_settle_dvp,
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

    let discriminator = ContraSwapInstructionDiscriminators::try_from(*discriminator)
        .map_err(|_| ProgramError::InvalidInstructionData)?;

    match discriminator {
        ContraSwapInstructionDiscriminators::CreateDvp => {
            process_create_dvp(program_id, accounts, instruction_data)
        }
        ContraSwapInstructionDiscriminators::FundDvp => {
            process_fund_dvp(program_id, accounts, instruction_data)
        }
        ContraSwapInstructionDiscriminators::ReclaimDvp => {
            process_reclaim_dvp(program_id, accounts, instruction_data)
        }
        ContraSwapInstructionDiscriminators::SettleDvp => {
            process_settle_dvp(program_id, accounts, instruction_data)
        }
        ContraSwapInstructionDiscriminators::CancelDvp => {
            process_cancel_dvp(program_id, accounts, instruction_data)
        }
        ContraSwapInstructionDiscriminators::RejectDvp => {
            process_reject_dvp(program_id, accounts, instruction_data)
        }
    }
}
