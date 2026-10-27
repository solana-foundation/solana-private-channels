//! Seize: the permanent delegate moves a channel balance into the compliance ATA.
//!
//! The node admits a delegate move only as a TransferChecked into
//! ATA(compliance owner, mint, Token-2022), and Token-2022 refuses to move a frozen
//! balance, so a frozen source is thawed first and frozen again in the same tx.

use solana_sdk::{instruction::Instruction, pubkey::Pubkey};
use spl_associated_token_account::{
    get_associated_token_address_with_program_id,
    instruction::create_associated_token_account_idempotent,
};

/// What the seize tx needs to know about the source account and its mint.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SeizePlan {
    pub mint: Pubkey,
    pub decimals: u8,
    pub source: Pubkey,
    pub source_frozen: bool,
    pub amount: u64,
    pub delegate: Pubkey,
    pub freeze_authority: Pubkey,
    pub compliance_owner: Pubkey,
}

/// The compliance ATA for `mint`, the only destination the node admits.
pub fn compliance_ata(compliance_owner: &Pubkey, mint: &Pubkey) -> Pubkey {
    get_associated_token_address_with_program_id(compliance_owner, mint, &spl_token_2022::ID)
}

/// `[CreateIdempotent(compliance), Thaw?, TransferChecked, Freeze?]`, paid by the delegate.
pub fn seize_instructions(plan: &SeizePlan) -> Vec<Instruction> {
    let program = spl_token_2022::ID;
    let destination = compliance_ata(&plan.compliance_owner, &plan.mint);
    let mut instructions = vec![create_associated_token_account_idempotent(
        &plan.delegate,
        &plan.compliance_owner,
        &plan.mint,
        &program,
    )];
    if plan.source_frozen {
        instructions.push(
            spl_token_2022::instruction::thaw_account(
                &program,
                &plan.source,
                &plan.mint,
                &plan.freeze_authority,
                &[],
            )
            .expect("thaw builds"),
        );
    }
    instructions.push(
        spl_token_2022::instruction::transfer_checked(
            &program,
            &plan.source,
            &plan.mint,
            &destination,
            &plan.delegate,
            &[],
            plan.amount,
            plan.decimals,
        )
        .expect("transfer_checked builds"),
    );
    if plan.source_frozen {
        instructions.push(
            spl_token_2022::instruction::freeze_account(
                &program,
                &plan.source,
                &plan.mint,
                &plan.freeze_authority,
                &[],
            )
            .expect("freeze builds"),
        );
    }
    instructions
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan(frozen: bool) -> SeizePlan {
        SeizePlan {
            mint: Pubkey::new_unique(),
            decimals: 6,
            source: Pubkey::new_unique(),
            source_frozen: frozen,
            amount: 1_000,
            delegate: Pubkey::new_unique(),
            freeze_authority: Pubkey::new_unique(),
            compliance_owner: Pubkey::new_unique(),
        }
    }

    fn first_bytes(ixs: &[Instruction]) -> Vec<Option<u8>> {
        ixs.iter().map(|ix| ix.data.first().copied()).collect()
    }

    #[test]
    fn unfrozen_source_is_create_then_transfer_checked() {
        let p = plan(false);
        let ixs = seize_instructions(&p);
        // ATA CreateIdempotent is data [1]; TransferChecked is 12.
        assert_eq!(first_bytes(&ixs), vec![Some(1), Some(12)]);
        let transfer = &ixs[1];
        assert_eq!(transfer.accounts[0].pubkey, p.source);
        assert_eq!(
            transfer.accounts[2].pubkey,
            compliance_ata(&p.compliance_owner, &p.mint)
        );
        assert_eq!(transfer.accounts[3].pubkey, p.delegate);
    }

    #[test]
    fn frozen_source_is_thawed_and_frozen_again_in_the_same_tx() {
        let ixs = seize_instructions(&plan(true));
        // Thaw 11, TransferChecked 12, Freeze 10.
        assert_eq!(
            first_bytes(&ixs),
            vec![Some(1), Some(11), Some(12), Some(10)]
        );
    }
}
