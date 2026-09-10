use pinocchio::{account::AccountView, entrypoint, error::ProgramError, Address, ProgramResult};

use crate::{
    processor::{
        process_add_operator, process_allow_mint, process_block_mint, process_create_instance,
        process_deposit, process_emit_event::process_emit_event, process_release_funds,
        process_remove_operator, process_rotate_bitmap, process_set_new_admin,
    },
    state::discriminator::PrivateChannelEscrowInstructionDiscriminators,
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

    let discriminator = PrivateChannelEscrowInstructionDiscriminators::try_from(*discriminator)
        .map_err(|_| ProgramError::InvalidInstructionData)?;

    match discriminator {
        PrivateChannelEscrowInstructionDiscriminators::CreateInstance => {
            process_create_instance(program_id, accounts, instruction_data)
        }
        PrivateChannelEscrowInstructionDiscriminators::AllowMint => {
            process_allow_mint(program_id, accounts, instruction_data)
        }
        PrivateChannelEscrowInstructionDiscriminators::BlockMint => {
            process_block_mint(program_id, accounts, instruction_data)
        }
        PrivateChannelEscrowInstructionDiscriminators::AddOperator => {
            process_add_operator(program_id, accounts, instruction_data)
        }
        PrivateChannelEscrowInstructionDiscriminators::RemoveOperator => {
            process_remove_operator(program_id, accounts, instruction_data)
        }
        PrivateChannelEscrowInstructionDiscriminators::SetNewAdmin => {
            process_set_new_admin(program_id, accounts, instruction_data)
        }
        PrivateChannelEscrowInstructionDiscriminators::Deposit => {
            process_deposit(program_id, accounts, instruction_data)
        }
        PrivateChannelEscrowInstructionDiscriminators::ReleaseFunds => {
            process_release_funds(program_id, accounts, instruction_data)
        }
        PrivateChannelEscrowInstructionDiscriminators::RotateBitmap => {
            process_rotate_bitmap(program_id, accounts, instruction_data)
        }
        PrivateChannelEscrowInstructionDiscriminators::EmitEvent => {
            process_emit_event(program_id, accounts)
        }
    }
}
