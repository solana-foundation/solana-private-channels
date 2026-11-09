extern crate alloc;

use alloc::{vec, vec::Vec};
use core::mem::{offset_of, size_of};
use pinocchio::{
    account::AccountView,
    cpi::{invoke_signed_with_bounds, Signer},
    error::ProgramError,
    instruction::{InstructionAccount, InstructionView},
    sysvars::instructions::Instructions,
    Address, ProgramResult,
};
use pinocchio_token_2022::ID as TOKEN_2022_PROGRAM_ID;
use spl_token_2022::{
    extension::{
        confidential_transfer::{
            instruction::ConfidentialTransferInstruction, ConfidentialTransferAccount,
            ConfidentialTransferMint, DecryptableBalance, EncryptedBalance,
            DEFAULT_MAXIMUM_PENDING_BALANCE_CREDIT_COUNTER,
        },
        pausable::PausableConfig,
        BaseStateWithExtensions, ExtensionType, StateWithExtensions,
    },
    solana_zk_sdk::zk_elgamal_proof_program::{
        instruction::ProofInstruction,
        proof_data::{BatchedGroupedCiphertext3HandlesValidityProofContext, ProofType},
        state::ProofContextStateMeta,
    },
    state::{Account, Mint},
};

use crate::{
    error::PrivateChannelEscrowProgramError,
    processor::shared::{
        account_check::verify_account_owner,
        token_utils::{invoke_memo, requires_memo, MAX_HOOK_REMAINING_ACCOUNTS},
    },
    require_len,
};

/// `TokenInstruction::ConfidentialTransferExtension`, the first byte of every
/// confidential transfer instruction.
const CONFIDENTIAL_TRANSFER_EXTENSION: u8 = 27;

/// `TokenInstruction::Reallocate`.
const REALLOCATE: u8 = 29;

/// ZK ElGamal proof program, which verifies the transfer proofs.
const ZK_ELGAMAL_PROOF_PROGRAM_ID: Address =
    Address::from_str_const("ZkE1Gama1Proof11111111111111111111111111111");

const CIPHERTEXT_LEN: usize = size_of::<EncryptedBalance>();
const DECRYPTABLE_BALANCE_LEN: usize = size_of::<DecryptableBalance>();

/// Source, mint, destination, instructions sysvar, five proof contexts and the
/// authority, then the hook extras.
const MAX_CPI_ACCOUNTS: usize = 10 + MAX_HOOK_REMAINING_ACCOUNTS;

/// An amount split into its low 16 and high 32 bits, each ElGamal-encrypted.
/// Token-2022 keeps pending balances and transfer amounts this way.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SplitCiphertext {
    pub lo: [u8; CIPHERTEXT_LEN],
    pub hi: [u8; CIPHERTEXT_LEN],
}

/// Where each transfer proof is: a proof instruction at that position relative to
/// this instruction, or a context account when 0. The fee offsets are read only
/// for mints with a transfer fee.
#[derive(Debug)]
pub struct TransferProofOffsets {
    pub equality: i8,
    pub ciphertext_validity: i8,
    pub range: i8,
    pub fee_sigma: i8,
    pub fee_ciphertext_validity: i8,
}

impl TransferProofOffsets {
    /// The offsets Token-2022 reads, in the order of its instruction data and of
    /// the context accounts.
    pub fn token_2022_order(&self, with_fee: bool) -> Vec<i8> {
        if with_fee {
            vec![
                self.equality,
                self.ciphertext_validity,
                self.fee_sigma,
                self.fee_ciphertext_validity,
                self.range,
            ]
        } else {
            vec![self.equality, self.ciphertext_validity, self.range]
        }
    }
}

/// The sender's half of a Token-2022 confidential transfer, passed through as is.
#[derive(Debug)]
pub struct ConfidentialTransferArgs {
    pub new_source_decryptable_available_balance: [u8; DECRYPTABLE_BALANCE_LEN],
    pub auditor_ciphertext: SplitCiphertext,
    pub proof_offsets: TransferProofOffsets,
}

impl ConfidentialTransferArgs {
    pub const LEN: usize = DECRYPTABLE_BALANCE_LEN + 2 * CIPHERTEXT_LEN + 5;

    pub fn try_from_bytes(data: &[u8]) -> Result<Self, ProgramError> {
        require_len!(data, Self::LEN);

        let mut offset = 0;

        let mut new_source_decryptable_available_balance = [0u8; DECRYPTABLE_BALANCE_LEN];
        new_source_decryptable_available_balance
            .copy_from_slice(&data[offset..offset + DECRYPTABLE_BALANCE_LEN]);
        offset += DECRYPTABLE_BALANCE_LEN;

        let mut auditor_ciphertext = SplitCiphertext {
            lo: [0u8; CIPHERTEXT_LEN],
            hi: [0u8; CIPHERTEXT_LEN],
        };
        auditor_ciphertext
            .lo
            .copy_from_slice(&data[offset..offset + CIPHERTEXT_LEN]);
        offset += CIPHERTEXT_LEN;
        auditor_ciphertext
            .hi
            .copy_from_slice(&data[offset..offset + CIPHERTEXT_LEN]);
        offset += CIPHERTEXT_LEN;

        let proof_offsets = TransferProofOffsets {
            equality: data[offset] as i8,
            ciphertext_validity: data[offset + 1] as i8,
            range: data[offset + 2] as i8,
            fee_sigma: data[offset + 3] as i8,
            fee_ciphertext_validity: data[offset + 4] as i8,
        };

        Ok(Self {
            new_source_decryptable_available_balance,
            auditor_ciphertext,
            proof_offsets,
        })
    }
}

/// Token-2022 reads one context account per zero offset.
pub fn context_account_count(offsets: &[i8]) -> usize {
    offsets.iter().filter(|offset| **offset == 0).count()
}

/// Splits the accounts after an instruction's fixed ones into the proof context
/// accounts and the hook extras behind them.
pub fn split_proof_contexts<'a>(
    accounts: &'a [AccountView],
    offsets: &[i8],
) -> Result<(&'a [AccountView], &'a [AccountView]), ProgramError> {
    let count = context_account_count(offsets);
    if accounts.len() < count {
        return Err(ProgramError::NotEnoughAccountKeys);
    }
    Ok(accounts.split_at(count))
}

/// A copy of the account's `ConfidentialTransferAccount` bytes, so no borrow
/// outlives the read. Fails if the account was never configured.
fn read_confidential_extension(
    token_account: &AccountView,
) -> Result<[u8; size_of::<ConfidentialTransferAccount>()], ProgramError> {
    let data = token_account.try_borrow()?;
    let state = StateWithExtensions::<Account>::unpack(&data)
        .map_err(|_| PrivateChannelEscrowProgramError::InvalidTokenAccount)?;
    let extension = state
        .get_extension_bytes::<ConfidentialTransferAccount>()
        .map_err(|_| PrivateChannelEscrowProgramError::ConfidentialAccountNotConfigured)?;
    extension
        .try_into()
        .map_err(|_| PrivateChannelEscrowProgramError::InvalidTokenAccount.into())
}

/// The ElGamal key the escrow account was configured with.
pub fn read_elgamal_pubkey(token_account: &AccountView) -> Result<[u8; 32], ProgramError> {
    let extension = read_confidential_extension(token_account)?;
    let start = offset_of!(ConfidentialTransferAccount, elgamal_pubkey);
    let mut elgamal_pubkey = [0u8; 32];
    elgamal_pubkey.copy_from_slice(&extension[start..start + 32]);
    Ok(elgamal_pubkey)
}

/// The escrow account's pending balance. Fails if the account was never configured.
pub fn read_pending_balance(token_account: &AccountView) -> Result<SplitCiphertext, ProgramError> {
    let extension = read_confidential_extension(token_account)?;
    let lo = offset_of!(ConfidentialTransferAccount, pending_balance_lo);
    let hi = offset_of!(ConfidentialTransferAccount, pending_balance_hi);
    let mut pending = SplitCiphertext {
        lo: [0u8; CIPHERTEXT_LEN],
        hi: [0u8; CIPHERTEXT_LEN],
    };
    pending
        .lo
        .copy_from_slice(&extension[lo..lo + CIPHERTEXT_LEN]);
    pending
        .hi
        .copy_from_slice(&extension[hi..hi + CIPHERTEXT_LEN]);
    Ok(pending)
}

/// The escrow account's available balance ciphertext. Fails if the account was
/// never configured.
pub fn read_available_balance(
    token_account: &AccountView,
) -> Result<[u8; CIPHERTEXT_LEN], ProgramError> {
    let extension = read_confidential_extension(token_account)?;
    let start = offset_of!(ConfidentialTransferAccount, available_balance);
    let mut available = [0u8; CIPHERTEXT_LEN];
    available.copy_from_slice(&extension[start..start + CIPHERTEXT_LEN]);
    Ok(available)
}

/// Credits in the escrow account's pending balance since the last apply.
pub fn read_pending_credit_counter(token_account: &AccountView) -> Result<u64, ProgramError> {
    let extension = read_confidential_extension(token_account)?;
    let start = offset_of!(ConfidentialTransferAccount, pending_balance_credit_counter);
    let mut counter = [0u8; 8];
    counter.copy_from_slice(&extension[start..start + 8]);
    Ok(u64::from_le_bytes(counter))
}

/// What a transfer's ciphertext validity proof names: the amount under the
/// source key, and the destination and auditor keys it was built for.
pub struct ValidityContext {
    pub source_amount: SplitCiphertext,
    pub destination_pubkey: [u8; 32],
    pub auditor_pubkey: [u8; 32],
}

/// The ciphertext validity proof's context. A proof instruction's data is a
/// one-byte tag and then the proof context; a context account's data is a header
/// and then the context. Anything but a ZK ElGamal validity proof is refused, as
/// Token-2022 refuses it, so a misplaced proof is never read as this one.
pub fn read_validity_context(
    offsets: &TransferProofOffsets,
    instructions_sysvar: &AccountView,
    proof_contexts: &[AccountView],
) -> Result<ValidityContext, ProgramError> {
    if offsets.ciphertext_validity != 0 {
        let instructions = Instructions::try_from(instructions_sysvar)?;
        let instruction =
            instructions.get_instruction_relative(offsets.ciphertext_validity as i64)?;
        if instruction.get_program_id() != &ZK_ELGAMAL_PROOF_PROGRAM_ID {
            return Err(ProgramError::InvalidInstructionData);
        }
        let (tag, context) = instruction
            .get_instruction_data()
            .split_first()
            .ok_or(ProgramError::InvalidInstructionData)?;
        if *tag != ProofInstruction::VerifyBatchedGroupedCiphertext3HandlesValidity as u8 {
            return Err(ProgramError::InvalidInstructionData);
        }
        return validity_context_from(context);
    }

    // Equality comes first in Token-2022's order, so its context, if any, is ahead.
    let validity_context = &proof_contexts[(offsets.equality == 0) as usize];
    verify_account_owner(validity_context, &ZK_ELGAMAL_PROOF_PROGRAM_ID)?;
    let data = validity_context.try_borrow()?;
    let proof_type = offset_of!(ProofContextStateMeta, proof_type);
    if data.get(proof_type) != Some(&(ProofType::BatchedGroupedCiphertext3HandlesValidity as u8)) {
        return Err(ProgramError::InvalidInstructionData);
    }
    // The proof type is the header's last byte, so the context starts in bounds.
    validity_context_from(&data[size_of::<ProofContextStateMeta>()..])
}

/// The keys come as source, destination, auditor. A grouped ciphertext opens with
/// the commitment and the source's handle, which together are the ciphertext
/// under the source key.
fn validity_context_from(context: &[u8]) -> Result<ValidityContext, ProgramError> {
    let destination = offset_of!(
        BatchedGroupedCiphertext3HandlesValidityProofContext,
        second_pubkey
    );
    let auditor = offset_of!(
        BatchedGroupedCiphertext3HandlesValidityProofContext,
        third_pubkey
    );
    let lo = offset_of!(
        BatchedGroupedCiphertext3HandlesValidityProofContext,
        grouped_ciphertext_lo
    );
    let hi = offset_of!(
        BatchedGroupedCiphertext3HandlesValidityProofContext,
        grouped_ciphertext_hi
    );
    require_len!(context, hi + CIPHERTEXT_LEN);

    let mut validity = ValidityContext {
        source_amount: SplitCiphertext {
            lo: [0u8; CIPHERTEXT_LEN],
            hi: [0u8; CIPHERTEXT_LEN],
        },
        destination_pubkey: [0u8; 32],
        auditor_pubkey: [0u8; 32],
    };
    validity
        .source_amount
        .lo
        .copy_from_slice(&context[lo..lo + CIPHERTEXT_LEN]);
    validity
        .source_amount
        .hi
        .copy_from_slice(&context[hi..hi + CIPHERTEXT_LEN]);
    validity
        .destination_pubkey
        .copy_from_slice(&context[destination..destination + 32]);
    validity
        .auditor_pubkey
        .copy_from_slice(&context[auditor..auditor + 32]);
    Ok(validity)
}

/// Refuses a release destination Token-2022 would refuse, each state with its own
/// error.
pub fn check_recipient_ready(
    token_account: &AccountView,
    destination_pubkey: &[u8; 32],
) -> ProgramResult {
    if token_account.is_data_empty() {
        return Err(PrivateChannelEscrowProgramError::RecipientNotConfidential.into());
    }

    let data = token_account.try_borrow()?;
    let state = StateWithExtensions::<Account>::unpack(&data)
        .map_err(|_| PrivateChannelEscrowProgramError::InvalidTokenAccount)?;
    if state.base.is_frozen() {
        return Err(PrivateChannelEscrowProgramError::RecipientFrozen.into());
    }

    let extension = state
        .get_extension_bytes::<ConfidentialTransferAccount>()
        .map_err(|_| PrivateChannelEscrowProgramError::RecipientNotConfidential)?;
    // Flags are one byte each, zero when false.
    if extension[offset_of!(ConfidentialTransferAccount, approved)] == 0 {
        return Err(PrivateChannelEscrowProgramError::RecipientNotApproved.into());
    }
    if extension[offset_of!(ConfidentialTransferAccount, allow_confidential_credits)] == 0 {
        return Err(PrivateChannelEscrowProgramError::RecipientCreditsDisabled.into());
    }

    let counter = offset_of!(ConfidentialTransferAccount, pending_balance_credit_counter);
    let mut pending_credits = [0u8; 8];
    pending_credits.copy_from_slice(&extension[counter..counter + 8]);
    let maximum = offset_of!(
        ConfidentialTransferAccount,
        maximum_pending_balance_credit_counter
    );
    let mut maximum_credits = [0u8; 8];
    maximum_credits.copy_from_slice(&extension[maximum..maximum + 8]);
    if u64::from_le_bytes(pending_credits) >= u64::from_le_bytes(maximum_credits) {
        return Err(PrivateChannelEscrowProgramError::RecipientPendingFull.into());
    }

    let key = offset_of!(ConfidentialTransferAccount, elgamal_pubkey);
    if extension[key..key + 32] != destination_pubkey[..] {
        return Err(PrivateChannelEscrowProgramError::RecipientKeyChanged.into());
    }
    Ok(())
}

/// Refuses a paused mint, which Token-2022 would refuse to transfer.
pub fn check_mint_not_paused(mint: &AccountView) -> ProgramResult {
    let data = mint.try_borrow()?;
    let state = StateWithExtensions::<Mint>::unpack(&data)
        .map_err(|_| PrivateChannelEscrowProgramError::InvalidMint)?;
    if state
        .get_extension::<PausableConfig>()
        .is_ok_and(|pausable| bool::from(pausable.paused))
    {
        return Err(PrivateChannelEscrowProgramError::MintPaused.into());
    }
    Ok(())
}

/// Refuses proofs built for another auditor than the mint's. A mint without one
/// stores zeros, which Token-2022 then expects the proofs to name.
pub fn check_auditor_key(mint: &AccountView, auditor_pubkey: &[u8; 32]) -> ProgramResult {
    let data = mint.try_borrow()?;
    let state = StateWithExtensions::<Mint>::unpack(&data)
        .map_err(|_| PrivateChannelEscrowProgramError::InvalidMint)?;
    let extension = state
        .get_extension_bytes::<ConfidentialTransferMint>()
        .map_err(|_| PrivateChannelEscrowProgramError::UnsupportedConfidentialMint)?;
    let key = offset_of!(ConfidentialTransferMint, auditor_elgamal_pubkey);
    if extension[key..key + 32] != auditor_pubkey[..] {
        return Err(PrivateChannelEscrowProgramError::StaleAuditorKey.into());
    }
    Ok(())
}

/// The accounts of a Token-2022 CPI, kept in step with their metas.
#[derive(Default)]
struct CpiAccounts<'a> {
    infos: Vec<&'a AccountView>,
    metas: Vec<InstructionAccount<'a>>,
}

impl<'a> CpiAccounts<'a> {
    fn push(&mut self, info: &'a AccountView, writable: bool, signer: bool) {
        self.infos.push(info);
        self.metas
            .push(InstructionAccount::new(info.address(), writable, signer));
    }

    /// The instructions sysvar if any proof is an instruction, then one context
    /// account per proof that is not.
    fn push_proofs(
        &mut self,
        offsets: &[i8],
        instructions_sysvar: &'a AccountView,
        proof_contexts: &'a [AccountView],
    ) {
        if offsets.iter().any(|offset| *offset != 0) {
            self.push(instructions_sysvar, false, false);
        }
        for context in proof_contexts {
            self.push(context, false, false);
        }
    }

    fn invoke(&self, data: &[u8], signers: &[Signer]) -> ProgramResult {
        invoke_signed_with_bounds::<MAX_CPI_ACCOUNTS>(
            &InstructionView {
                program_id: &TOKEN_2022_PROGRAM_ID,
                accounts: &self.metas,
                data,
            },
            &self.infos,
            signers,
        )
    }
}

/// Grows the escrow account for `ConfidentialTransferAccount`, plus
/// `ConfidentialTransferFeeAmount` on a fee mint, which `ConfigureAccount` adds too.
pub fn reallocate_cpi(
    token_account: &AccountView,
    payer: &AccountView,
    system_program: &AccountView,
    owner: &AccountView,
    with_fee: bool,
    signers: &[Signer],
) -> ProgramResult {
    let mut data = vec![REALLOCATE];
    data.extend_from_slice(&(ExtensionType::ConfidentialTransferAccount as u16).to_le_bytes());
    if with_fee {
        data.extend_from_slice(
            &(ExtensionType::ConfidentialTransferFeeAmount as u16).to_le_bytes(),
        );
    }

    let mut accounts = CpiAccounts::default();
    accounts.push(token_account, true, false);
    accounts.push(payer, true, true);
    accounts.push(system_program, false, false);
    accounts.push(owner, false, true);
    accounts.invoke(&data, signers)
}

/// `ConfigureAccount` with the ElGamal key proven by the `VerifyPubkeyValidity`
/// instruction at `proof_instruction_offset`, and Token-2022's default limit of
/// pending credits.
pub fn configure_account_cpi(
    token_account: &AccountView,
    mint: &AccountView,
    instructions_sysvar: &AccountView,
    owner: &AccountView,
    decryptable_zero_balance: &[u8; DECRYPTABLE_BALANCE_LEN],
    proof_instruction_offset: i8,
    signers: &[Signer],
) -> ProgramResult {
    let mut data = vec![
        CONFIDENTIAL_TRANSFER_EXTENSION,
        ConfidentialTransferInstruction::ConfigureAccount as u8,
    ];
    data.extend_from_slice(decryptable_zero_balance);
    data.extend_from_slice(&DEFAULT_MAXIMUM_PENDING_BALANCE_CREDIT_COUNTER.to_le_bytes());
    data.push(proof_instruction_offset as u8);

    let mut accounts = CpiAccounts::default();
    accounts.push(token_account, true, false);
    accounts.push(mint, false, false);
    accounts.push(instructions_sysvar, false, false);
    accounts.push(owner, false, true);
    accounts.invoke(&data, signers)
}

pub fn set_confidential_credits_cpi(
    token_account: &AccountView,
    owner: &AccountView,
    allow: bool,
    signers: &[Signer],
) -> ProgramResult {
    let instruction = if allow {
        ConfidentialTransferInstruction::EnableConfidentialCredits
    } else {
        ConfidentialTransferInstruction::DisableConfidentialCredits
    };
    let data = [CONFIDENTIAL_TRANSFER_EXTENSION, instruction as u8];

    let mut accounts = CpiAccounts::default();
    accounts.push(token_account, true, false);
    accounts.push(owner, false, true);
    accounts.invoke(&data, signers)
}

pub fn apply_pending_balance_cpi(
    token_account: &AccountView,
    owner: &AccountView,
    expected_credit_counter: u64,
    new_decryptable_available_balance: &[u8; DECRYPTABLE_BALANCE_LEN],
    signers: &[Signer],
) -> ProgramResult {
    let mut data = vec![
        CONFIDENTIAL_TRANSFER_EXTENSION,
        ConfidentialTransferInstruction::ApplyPendingBalance as u8,
    ];
    data.extend_from_slice(&expected_credit_counter.to_le_bytes());
    data.extend_from_slice(new_decryptable_available_balance);

    let mut accounts = CpiAccounts::default();
    accounts.push(token_account, true, false);
    accounts.push(owner, false, true);
    accounts.invoke(&data, signers)
}

/// Token-2022's confidential `Deposit`: public balance to pending balance.
pub fn public_to_pending_cpi(
    token_account: &AccountView,
    mint: &AccountView,
    owner: &AccountView,
    amount: u64,
    decimals: u8,
    signers: &[Signer],
) -> ProgramResult {
    let mut data = vec![
        CONFIDENTIAL_TRANSFER_EXTENSION,
        ConfidentialTransferInstruction::Deposit as u8,
    ];
    data.extend_from_slice(&amount.to_le_bytes());
    data.push(decimals);

    let mut accounts = CpiAccounts::default();
    accounts.push(token_account, true, false);
    accounts.push(mint, false, false);
    accounts.push(owner, false, true);
    accounts.invoke(&data, signers)
}

/// Token-2022's confidential `Withdraw`: available balance to public balance.
#[allow(clippy::too_many_arguments)]
pub fn available_to_public_cpi(
    token_account: &AccountView,
    mint: &AccountView,
    instructions_sysvar: &AccountView,
    proof_contexts: &[AccountView],
    owner: &AccountView,
    amount: u64,
    decimals: u8,
    new_decryptable_available_balance: &[u8; DECRYPTABLE_BALANCE_LEN],
    proof_offsets: [i8; 2],
    signers: &[Signer],
) -> ProgramResult {
    let mut data = vec![
        CONFIDENTIAL_TRANSFER_EXTENSION,
        ConfidentialTransferInstruction::Withdraw as u8,
    ];
    data.extend_from_slice(&amount.to_le_bytes());
    data.push(decimals);
    data.extend_from_slice(new_decryptable_available_balance);
    data.extend(proof_offsets.iter().map(|offset| *offset as u8));

    let mut accounts = CpiAccounts::default();
    accounts.push(token_account, true, false);
    accounts.push(mint, false, false);
    accounts.push_proofs(&proof_offsets, instructions_sysvar, proof_contexts);
    accounts.push(owner, false, true);
    accounts.invoke(&data, signers)
}

/// Token-2022's confidential `Transfer`, or `TransferWithFee` on a fee mint. Hook
/// extras follow the authority without their signer bit, as in
/// `transfer_checked_cpi`.
#[allow(clippy::too_many_arguments)]
pub fn confidential_transfer_cpi(
    from: &AccountView,
    mint: &AccountView,
    to: &AccountView,
    instructions_sysvar: &AccountView,
    proof_contexts: &[AccountView],
    authority: &AccountView,
    args: &ConfidentialTransferArgs,
    with_fee: bool,
    memo_program: Option<&AccountView>,
    hook_extras: &[AccountView],
    signers: &[Signer],
) -> ProgramResult {
    // The memo has to be the transfer's preceding sibling, as in transfer_checked_cpi.
    if requires_memo(to)? {
        invoke_memo(memo_program.ok_or(ProgramError::NotEnoughAccountKeys)?)?;
    }

    if hook_extras.len() > MAX_HOOK_REMAINING_ACCOUNTS {
        return Err(ProgramError::InvalidArgument);
    }

    let instruction = if with_fee {
        ConfidentialTransferInstruction::TransferWithFee
    } else {
        ConfidentialTransferInstruction::Transfer
    };
    let proof_offsets = args.proof_offsets.token_2022_order(with_fee);
    let mut data = vec![CONFIDENTIAL_TRANSFER_EXTENSION, instruction as u8];
    data.extend_from_slice(&args.new_source_decryptable_available_balance);
    data.extend_from_slice(&args.auditor_ciphertext.lo);
    data.extend_from_slice(&args.auditor_ciphertext.hi);
    data.extend(proof_offsets.iter().map(|offset| *offset as u8));

    let mut accounts = CpiAccounts::default();
    accounts.push(from, true, false);
    accounts.push(mint, false, false);
    accounts.push(to, true, false);
    accounts.push_proofs(&proof_offsets, instructions_sysvar, proof_contexts);
    accounts.push(authority, false, true);
    for extra in hook_extras {
        accounts.push(extra, extra.is_writable(), false);
    }
    accounts.invoke(&data, signers)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec::Vec;

    fn transfer_args_bytes(offsets: [i8; 5]) -> Vec<u8> {
        let mut data = Vec::new();
        data.extend_from_slice(&[1u8; 36]);
        data.extend_from_slice(&[2u8; 64]);
        data.extend_from_slice(&[3u8; 64]);
        data.extend(offsets.iter().map(|offset| *offset as u8));
        data
    }

    #[test]
    fn test_parse_transfer_args() {
        let data = transfer_args_bytes([1, 2, 3, 4, 5]);

        let args = ConfidentialTransferArgs::try_from_bytes(&data).unwrap();

        assert_eq!(args.new_source_decryptable_available_balance, [1u8; 36]);
        assert_eq!(args.auditor_ciphertext.lo, [2u8; 64]);
        assert_eq!(args.auditor_ciphertext.hi, [3u8; 64]);
        assert_eq!(args.proof_offsets.equality, 1);
        assert_eq!(args.proof_offsets.ciphertext_validity, 2);
        assert_eq!(args.proof_offsets.range, 3);
        assert_eq!(args.proof_offsets.fee_sigma, 4);
        assert_eq!(args.proof_offsets.fee_ciphertext_validity, 5);
    }

    #[test]
    fn test_parse_transfer_args_too_short() {
        let data = transfer_args_bytes([1, 2, 3, 4, 5]);

        let result = ConfidentialTransferArgs::try_from_bytes(&data[..data.len() - 1]);

        assert_eq!(result.err(), Some(ProgramError::InvalidInstructionData));
    }

    // Token-2022 orders a fee transfer's proofs equality, validity, fee sigma,
    // fee validity, range. A plain transfer reads only equality, validity, range.
    #[test]
    fn test_token_2022_order() {
        let offsets = TransferProofOffsets {
            equality: 1,
            ciphertext_validity: 2,
            range: 3,
            fee_sigma: 4,
            fee_ciphertext_validity: 5,
        };

        assert_eq!(offsets.token_2022_order(false), [1, 2, 3]);
        assert_eq!(offsets.token_2022_order(true), [1, 2, 4, 5, 3]);
    }

    #[test]
    fn test_context_account_count() {
        assert_eq!(context_account_count(&[1, 2, 3]), 0);
        assert_eq!(context_account_count(&[0, 0, 0]), 3);
        assert_eq!(context_account_count(&[0, -1, 0, 2, 0]), 3);
    }
}
