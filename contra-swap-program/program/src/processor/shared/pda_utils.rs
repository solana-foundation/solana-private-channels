use pinocchio::{
    account::AccountView,
    address::Address,
    cpi::{Seed, Signer},
    sysvars::rent::Rent,
    ProgramResult,
};
use pinocchio_system::instructions::{Allocate, Assign, CreateAccount, Transfer};

pub fn create_pda_account<const N: usize>(
    payer: &AccountView,
    rent: &Rent,
    space: usize,
    owner: &Address,
    new_pda_account: &AccountView,
    new_pda_signer_seeds: [Seed; N],
    min_rent_space: Option<usize>,
) -> ProgramResult {
    let signers = [Signer::from(&new_pda_signer_seeds)];
    let rent_space = min_rent_space.map_or(space, |min| min.max(space));
    let required_lamports = rent.try_minimum_balance(rent_space)?.max(1);

    if new_pda_account.lamports() > 0 {
        let required_lamports = required_lamports.saturating_sub(new_pda_account.lamports());
        if required_lamports > 0 {
            Transfer {
                from: payer,
                to: new_pda_account,
                lamports: required_lamports,
            }
            .invoke()?;
        }
        Allocate {
            account: new_pda_account,
            space: space as u64,
        }
        .invoke_signed(&signers)?;
        Assign {
            account: new_pda_account,
            owner,
        }
        .invoke_signed(&signers)
    } else {
        CreateAccount {
            from: payer,
            to: new_pda_account,
            lamports: required_lamports,
            space: space as u64,
            owner,
        }
        .invoke_signed(&signers)
    }
}
