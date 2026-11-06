//! Confidential deposit into the escrow (`ConfidentialDeposit`). Configures the user's
//! Token-2022 account for confidential transfers if needed and tops its available balance up
//! from pending, then from public, before proving the transfer to the escrow's key.
use crate::confidential::{
    confidential_account, decrypt_available, decrypt_pending, pda, plan_top_up, send_v1,
    ConfidentialKeyPair, MAX_CONFIDENTIAL_AMOUNT,
};
use private_channel_escrow_program_client::{
    instructions::ConfidentialDepositBuilder, PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
};
use solana_client::rpc_client::RpcClient;
use solana_sdk::{
    instruction::AccountMeta,
    instruction::Instruction,
    pubkey::Pubkey,
    signature::{Keypair, Signature, Signer},
};
use solana_zk_elgamal_proof_interface::instruction::ProofInstruction;
use solana_zk_sdk::{
    encryption::{
        auth_encryption::AeCiphertext,
        elgamal::{ElGamalCiphertext, ElGamalPubkey},
    },
    zk_elgamal_proof_program::build_pubkey_validity_proof_data,
};
use spl_associated_token_account::{
    get_associated_token_address_with_program_id,
    instruction::create_associated_token_account_idempotent,
};
use spl_token_2022_interface::{
    extension::{
        confidential_transfer::{
            instruction as confidential_instruction, ConfidentialTransferMint,
            DEFAULT_MAXIMUM_PENDING_BALANCE_CREDIT_COUNTER,
        },
        confidential_transfer_fee::ConfidentialTransferFeeConfig,
        transfer_fee::TransferFeeConfig,
        transfer_hook::TransferHook,
        BaseStateWithExtensions, ExtensionType, StateWithExtensions,
    },
    state::{Account, Mint},
};
use spl_token_confidential_transfer_proof_extraction::instruction::ProofLocation;
use spl_token_confidential_transfer_proof_generation::{
    transfer::transfer_split_proof_data, transfer_with_fee::transfer_with_fee_split_proof_data,
};
use std::error::Error;

type Result<T> = std::result::Result<T, Box<dyn Error>>;

const TOKEN_2022: Pubkey = spl_token_2022_interface::ID;

struct MintInfo {
    hook: bool,
    decimals: u8,
    auditor: Option<ElGamalPubkey>,
    /// Basis points, maximum fee and the withheld authority's key, on fee mints.
    fee: Option<(u16, u64, ElGamalPubkey)>,
}

/// Deposit `amount` confidentially into the escrow of `instance`. `hook_extras` are the
/// resolved transfer hook accounts of a hook mint. With `forwarder`, the escrow instruction
/// is sent through that program by CPI instead of top level.
#[allow(clippy::too_many_arguments)]
pub fn deposit(
    client: &RpcClient,
    user: &Keypair,
    keys: &ConfidentialKeyPair,
    instance: Pubkey,
    mint: Pubkey,
    amount: u64,
    recipient: Option<Pubkey>,
    hook_extras: &[AccountMeta],
    forwarder: Option<Pubkey>,
) -> Result<Signature> {
    if amount == 0 || amount > MAX_CONFIDENTIAL_AMOUNT {
        return Err(format!("amount must be 1..={MAX_CONFIDENTIAL_AMOUNT}").into());
    }

    let info = read_mint(client, &mint)?;
    if info.hook && hook_extras.is_empty() {
        return Err("transfer hook mints need their resolved extra accounts".into());
    }
    let escrow_ata = get_associated_token_address_with_program_id(&instance, &mint, &TOKEN_2022);
    let escrow_data = client
        .get_account_data(&escrow_ata)
        .map_err(|e| format!("escrow account {escrow_ata} not found: {e}"))?;
    let (_, escrow) = confidential_account(&escrow_data)?
        .ok_or("the escrow account is not configured for confidential transfers")?;
    let escrow_key = ElGamalPubkey::try_from(escrow.elgamal_pubkey)
        .map_err(|_| "escrow ElGamal key is invalid")?;

    let user_ata = get_associated_token_address_with_program_id(&user.pubkey(), &mint, &TOKEN_2022);
    ensure_configured(client, user, keys, &mint, &user_ata, info.fee.is_some())?;
    top_up(client, user, keys, &mint, &user_ata, info.decimals, amount)?;
    let data = client.get_account_data(&user_ata)?;
    let (_, state) = confidential_account(&data)?.ok_or("user account lost its configuration")?;
    let available_ct = ElGamalCiphertext::try_from(state.available_balance)
        .map_err(|_| "available balance ciphertext is malformed")?;
    let balance = decrypt_available(keys, &state)?;
    // The proof builders read the balance from an AES ciphertext.
    let decryptable = keys.aes.encrypt(balance);
    let new_balance = keys.aes.encrypt(balance - amount).to_bytes();

    let user_allowed = pda::allowed_wallet(&instance, &user.pubkey());
    let mut builder = ConfidentialDepositBuilder::new();
    builder
        .payer(user.pubkey())
        .user(user.pubkey())
        .instance(instance)
        .mint(mint)
        .allowed_mint(pda::allowed_mint(&instance, &mint))
        .user_ata(user_ata)
        .instance_ata(escrow_ata)
        .event_authority(pda::event_authority())
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .user_allowed_wallet_pda(user_allowed)
        .recipient_allowed_wallet_pda(
            recipient.map_or(user_allowed, |r| pda::allowed_wallet(&instance, &r)),
        )
        .new_source_decryptable_available_balance(new_balance);
    if let Some(recipient) = recipient {
        builder.recipient(recipient);
    }

    // Proofs go right after the escrow instruction, in Token-2022's order.
    let mut proofs: Vec<Instruction> = Vec::new();
    let mut place = |instruction: Instruction| -> i8 {
        proofs.push(instruction);
        proofs.len() as i8
    };
    match info.fee {
        None => {
            let data = transfer_split_proof_data(
                &available_ct,
                &decryptable,
                amount,
                &keys.elgamal,
                &keys.aes,
                &escrow_key,
                info.auditor.as_ref(),
            )
            .map_err(|e| format!("proof generation failed: {e}"))?;
            let validity = &data.ciphertext_validity_proof_data_with_ciphertext;
            builder
                .auditor_ciphertext_lo(validity.ciphertext_lo.0)
                .auditor_ciphertext_hi(validity.ciphertext_hi.0)
                .equality_proof_instruction_offset(place(
                    ProofInstruction::VerifyCiphertextCommitmentEquality
                        .encode_verify_proof(None, &data.equality_proof_data),
                ))
                .ciphertext_validity_proof_instruction_offset(place(
                    ProofInstruction::VerifyBatchedGroupedCiphertext3HandlesValidity
                        .encode_verify_proof(None, &validity.proof_data),
                ))
                .range_proof_instruction_offset(place(
                    ProofInstruction::VerifyBatchedRangeProofU128
                        .encode_verify_proof(None, &data.range_proof_data),
                ))
                .fee_sigma_proof_instruction_offset(0)
                .fee_ciphertext_validity_proof_instruction_offset(0);
        }
        Some((basis_points, maximum_fee, withheld_key)) => {
            let data = transfer_with_fee_split_proof_data(
                &available_ct,
                &decryptable,
                amount,
                &keys.elgamal,
                &keys.aes,
                &escrow_key,
                info.auditor.as_ref(),
                &withheld_key,
                basis_points,
                maximum_fee,
            )
            .map_err(|e| format!("proof generation failed: {e}"))?;
            let validity = &data.transfer_amount_ciphertext_validity_proof_data_with_ciphertext;
            builder
                .auditor_ciphertext_lo(validity.ciphertext_lo.0)
                .auditor_ciphertext_hi(validity.ciphertext_hi.0)
                .equality_proof_instruction_offset(place(
                    ProofInstruction::VerifyCiphertextCommitmentEquality
                        .encode_verify_proof(None, &data.equality_proof_data),
                ))
                .ciphertext_validity_proof_instruction_offset(place(
                    ProofInstruction::VerifyBatchedGroupedCiphertext3HandlesValidity
                        .encode_verify_proof(None, &validity.proof_data),
                ))
                .fee_sigma_proof_instruction_offset(place(
                    ProofInstruction::VerifyPercentageWithCap
                        .encode_verify_proof(None, &data.percentage_with_cap_proof_data),
                ))
                .fee_ciphertext_validity_proof_instruction_offset(place(
                    ProofInstruction::VerifyBatchedGroupedCiphertext2HandlesValidity
                        .encode_verify_proof(None, &data.fee_ciphertext_validity_proof_data),
                ))
                .range_proof_instruction_offset(place(
                    ProofInstruction::VerifyBatchedRangeProofU256
                        .encode_verify_proof(None, &data.range_proof_data),
                ));
        }
    }

    builder.add_remaining_accounts(hook_extras);
    let mut instructions = vec![wrap(builder.instruction(), forwarder)];
    instructions.extend(proofs);
    send_v1(client, user, &instructions, &[])
}

/// The same instruction sent through `forwarder`, which passes it on by CPI.
fn wrap(instruction: Instruction, forwarder: Option<Pubkey>) -> Instruction {
    let Some(forwarder) = forwarder else {
        return instruction;
    };
    let mut accounts = vec![AccountMeta::new_readonly(instruction.program_id, false)];
    accounts.extend(instruction.accounts);
    Instruction {
        program_id: forwarder,
        accounts,
        data: instruction.data,
    }
}

fn read_mint(client: &RpcClient, mint: &Pubkey) -> Result<MintInfo> {
    let account = client.get_account(mint)?;
    if account.owner != TOKEN_2022 {
        return Err(format!("mint {mint} is not a Token-2022 mint").into());
    }
    let state = StateWithExtensions::<Mint>::unpack(&account.data)?;
    let confidential = state
        .get_extension::<ConfidentialTransferMint>()
        .map_err(|_| "mint has no confidential transfer extension")?;
    let hook = state
        .get_extension::<TransferHook>()
        .is_ok_and(|hook| Option::<Pubkey>::from(hook.program_id).is_some());
    let auditor = confidential
        .auditor_elgamal_pubkey
        .get()
        .map(ElGamalPubkey::try_from)
        .transpose()
        .map_err(|_| "auditor key is invalid")?;
    let fee = match state.get_extension::<TransferFeeConfig>() {
        Ok(config) => {
            let epoch = client.get_epoch_info()?.epoch;
            let fee = config.get_epoch_fee(epoch);
            let withheld = state
                .get_extension::<ConfidentialTransferFeeConfig>()
                .map_err(|_| "fee mint has no confidential transfer fee config")?;
            let withheld_key =
                ElGamalPubkey::try_from(withheld.withdraw_withheld_authority_elgamal_pubkey)
                    .map_err(|_| "withheld authority key is invalid")?;
            Some((
                u16::from(fee.transfer_fee_basis_points),
                u64::from(fee.maximum_fee),
                withheld_key,
            ))
        }
        Err(_) => None,
    };
    Ok(MintInfo {
        hook,
        decimals: state.base.decimals,
        auditor,
        fee,
    })
}

/// Create and configure the user's account for confidential transfers when it is not yet.
pub fn ensure_configured(
    client: &RpcClient,
    user: &Keypair,
    keys: &ConfidentialKeyPair,
    mint: &Pubkey,
    user_ata: &Pubkey,
    fee_mint: bool,
) -> Result<()> {
    if let Ok(data) = client.get_account_data(user_ata) {
        if confidential_account(&data)?.is_some() {
            return Ok(());
        }
    }
    let mut extensions = vec![ExtensionType::ConfidentialTransferAccount];
    if fee_mint {
        extensions.push(ExtensionType::ConfidentialTransferFeeAmount);
    }
    let proof = build_pubkey_validity_proof_data(&keys.elgamal)?;
    let mut instructions = vec![
        create_associated_token_account_idempotent(
            &user.pubkey(),
            &user.pubkey(),
            mint,
            &TOKEN_2022,
        ),
        spl_token_2022_interface::instruction::reallocate(
            &TOKEN_2022,
            user_ata,
            &user.pubkey(),
            &user.pubkey(),
            &[],
            &extensions,
        )?,
    ];
    instructions.extend(confidential_instruction::configure_account(
        &TOKEN_2022,
        user_ata,
        mint,
        &keys.aes.encrypt(0).into(),
        DEFAULT_MAXIMUM_PENDING_BALANCE_CREDIT_COUNTER,
        &user.pubkey(),
        &[],
        ProofLocation::InstructionOffset(1.try_into().unwrap(), &proof),
    )?);
    send_v1(client, user, &instructions, &[])?;
    Ok(())
}

// Make at least `amount` available: apply pending, moving public tokens in first if short.
fn top_up(
    client: &RpcClient,
    user: &Keypair,
    keys: &ConfidentialKeyPair,
    mint: &Pubkey,
    user_ata: &Pubkey,
    decimals: u8,
    amount: u64,
) -> Result<()> {
    let data = client.get_account_data(user_ata)?;
    let public = StateWithExtensions::<Account>::unpack(&data)?.base.amount;
    let (_, state) = confidential_account(&data)?.ok_or("user account is not configured")?;
    let available = decrypt_available(keys, &state)?;
    let pending = decrypt_pending(keys, &state)?;
    let plan = plan_top_up(available, pending, public, amount)?;
    if !plan.apply {
        return Ok(());
    }
    let mut counter = u64::from(state.pending_balance_credit_counter);
    let mut instructions = Vec::new();
    if plan.from_public > 0 {
        instructions.push(confidential_instruction::deposit(
            &TOKEN_2022,
            user_ata,
            mint,
            plan.from_public,
            decimals,
            &user.pubkey(),
            &[],
        )?);
        counter += 1;
    }
    let new_available = available + pending + plan.from_public;
    let copy: AeCiphertext = keys.aes.encrypt(new_available);
    instructions.push(confidential_instruction::apply_pending_balance(
        &TOKEN_2022,
        user_ata,
        counter,
        &copy.into(),
        &user.pubkey(),
        &[],
    )?);
    send_v1(client, user, &instructions, &[])?;
    Ok(())
}
