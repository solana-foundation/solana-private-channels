//! Confidential-transfer helpers: keys, mints, accounts, proofs and decryption.
//!
//! Trident runs agave 2.3, whose ZK ElGamal proof program is a builtin, so proofs
//! come from the zk-sdk 2.3 stack. Token-2022 is mainnet's v11.1.0 (Trident.toml)
//! and reads only the proof contexts, whose layout has not changed.

use std::mem::size_of;

use bytemuck::Pod;
// solana-sdk 2.3 points at solana-system-interface, which is not a dependency here.
#[allow(deprecated)]
use solana_sdk::system_instruction;
use solana_sdk::{compute_budget::ComputeBudgetInstruction, rent::Rent};
use solana_zk_sdk::{
    encryption::{
        auth_encryption::{AeCiphertext, AeKey},
        elgamal::{ElGamalCiphertext, ElGamalKeypair, ElGamalPubkey},
        pod::elgamal::PodElGamalCiphertext,
    },
    zk_elgamal_proof_program::{
        instruction::{ContextStateInfo, ProofInstruction},
        proof_data::{PubkeyValidityProofData, ZkProofData},
        state::ProofContextState,
    },
};
use spl_associated_token_account::{
    get_associated_token_address_with_program_id,
    instruction::create_associated_token_account_idempotent,
};
use spl_token_2022_interface::{
    extension::{
        confidential_mint_burn::instruction::initialize_mint as initialize_mint_burn,
        confidential_transfer::{
            instruction as confidential_instruction, ConfidentialTransferAccount,
            DecryptableBalance,
        },
        confidential_transfer_fee::{
            instruction::initialize_confidential_transfer_fee_config, ConfidentialTransferFeeAmount,
        },
        transfer_fee::{instruction::initialize_transfer_fee_config, TransferFeeAmount},
        BaseStateWithExtensions, ExtensionType, StateWithExtensions,
    },
    instruction::{initialize_mint2, mint_to, reallocate},
    state::{Account as TokenAccount, Mint},
};
use spl_token_confidential_transfer_proof_extraction::instruction::ProofLocation;
use spl_token_confidential_transfer_proof_generation::{
    transfer::transfer_split_proof_data, transfer_with_fee::transfer_with_fee_split_proof_data,
    withdraw::withdraw_proof_data,
};
use trident_fuzz::fuzzing::*;

pub const TOKEN_2022_ID: Pubkey = pubkey!("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb");
pub const CONFIDENTIAL_MINT_DECIMALS: u8 = 6;
/// Pending credits Token-2022 accepts before an apply, as the escrow configures it.
pub const MAXIMUM_PENDING_CREDITS: u64 = 65_536;

// ── Transactions ──────────────────────────────────────────────────────────────

/// What a transaction did. Trident does not export its own result type.
pub struct Outcome {
    pub success: bool,
    pub logs: String,
}

impl Outcome {
    /// Whether `program` failed first, with custom error `code`. Escrow and
    /// Token-2022 error numbers overlap, and a failed CPI fails its caller with
    /// the same error, so only the first failure in the logs names the program
    /// that refused.
    pub fn refused_by(&self, program: &Pubkey, code: u32) -> bool {
        let Some((failed_program, reason)) = self
            .logs
            .split("Program ")
            .skip(1)
            .find_map(|log| log.split_once(" failed: "))
        else {
            return false;
        };
        let expected = format!("custom program error: {code:#x}");
        // The log line ends at its closing quote.
        failed_program == program.to_string() && reason.split('"').next() == Some(expected.as_str())
    }
}

/// Sends `instructions` behind the maximum compute budget: proof verification and
/// Token-2022's confidential paths need far more than the default.
pub fn send(trident: &mut Trident, instructions: &[Instruction], label: &str) -> Outcome {
    let mut all = vec![ComputeBudgetInstruction::set_compute_unit_limit(1_400_000)];
    all.extend_from_slice(instructions);
    let result = trident.process_transaction(&all, Some(label));
    Outcome {
        success: result.is_success(),
        logs: result.logs(),
    }
}

/// Like [`send`], for setup steps that must land.
pub fn send_ok(trident: &mut Trident, instructions: &[Instruction], label: &str) {
    let outcome = send(trident, instructions, label);
    assert!(outcome.success, "{label} failed: {}", outcome.logs);
}

// ── Keys ──────────────────────────────────────────────────────────────────────

/// The ElGamal and AES keys behind one confidential account.
pub struct ConfidentialKeys {
    pub elgamal: ElGamalKeypair,
    pub aes: AeKey,
}

impl ConfidentialKeys {
    /// Derived from a keypair the Trident RNG made, so a replayed seed gets the
    /// same keys. Proof randomness still comes from the OS, which changes proof
    /// bytes but never an outcome.
    pub fn from_signer(signer: &Keypair) -> Self {
        Self {
            elgamal: ElGamalKeypair::new_from_signer(signer, b"elgamal").unwrap(),
            aes: AeKey::new_from_signer(signer, b"aes").unwrap(),
        }
    }

    pub fn decryptable(&self, amount: u64) -> DecryptableBalance {
        self.aes.encrypt(amount).into()
    }

    /// zk-sdk 2.3 keeps pod bytes private; the escrow builders take bytes.
    pub fn decryptable_bytes(&self, amount: u64) -> [u8; 36] {
        bytemuck::bytes_of(&self.decryptable(amount))
            .try_into()
            .unwrap()
    }
}

pub fn elgamal_bytes(ciphertext: PodElGamalCiphertext) -> [u8; 64] {
    bytemuck::cast(ciphertext)
}

// ── Mints and accounts ────────────────────────────────────────────────────────

#[derive(Clone, Copy)]
pub struct TransferFee {
    pub basis_points: u16,
    pub maximum_fee: u64,
}

impl TransferFee {
    /// Token-2022's fee: the rate rounded up, capped at the maximum.
    pub fn of(&self, amount: u64) -> u64 {
        (amount * self.basis_points as u64)
            .div_ceil(10_000)
            .min(self.maximum_fee)
    }
}

pub enum MintKind {
    Plain,
    Fee(TransferFee),
    /// Confidential supply: Token-2022 refuses public mint and burn, so the
    /// escrow refuses to configure it.
    MintBurn,
}

pub struct ConfidentialMint {
    pub address: Pubkey,
    pub auditor: ElGamalKeypair,
    pub withdraw_withheld_authority: ElGamalKeypair,
    pub fee: Option<TransferFee>,
}

impl ConfidentialMint {
    pub fn fee(&self, amount: u64) -> u64 {
        self.fee.map_or(0, |fee| fee.of(amount))
    }
}

/// A confidential Token-2022 mint with an auditor and the payer as authority.
/// Trident's mint helper has the confidential extensions commented out.
pub fn create_confidential_mint(trident: &mut Trident, kind: MintKind) -> ConfidentialMint {
    let payer = trident.payer().pubkey();
    let address = trident.random_pubkey();
    let auditor = ConfidentialKeys::from_signer(&trident.random_keypair()).elgamal;
    let withdraw_withheld_authority =
        ConfidentialKeys::from_signer(&trident.random_keypair()).elgamal;

    let mut extensions = vec![ExtensionType::ConfidentialTransferMint];
    match kind {
        MintKind::Plain => {}
        MintKind::Fee(_) => {
            extensions.push(ExtensionType::TransferFeeConfig);
            extensions.push(ExtensionType::ConfidentialTransferFeeConfig);
        }
        MintKind::MintBurn => extensions.push(ExtensionType::ConfidentialMintBurn),
    }
    let space = ExtensionType::try_calculate_account_len::<Mint>(&extensions).unwrap();
    let rent = trident.get_sysvar::<Rent>();

    let mut instructions = vec![
        system_instruction::create_account(
            &payer,
            &address,
            rent.minimum_balance(space),
            space as u64,
            &TOKEN_2022_ID,
        ),
        confidential_instruction::initialize_mint(
            &TOKEN_2022_ID,
            &address,
            Some(payer),
            true,
            Some((*auditor.pubkey()).into()),
        )
        .unwrap(),
    ];
    let fee = match kind {
        MintKind::Plain => None,
        MintKind::Fee(fee) => {
            instructions.push(
                initialize_transfer_fee_config(
                    &TOKEN_2022_ID,
                    &address,
                    Some(&payer),
                    Some(&payer),
                    fee.basis_points,
                    fee.maximum_fee,
                )
                .unwrap(),
            );
            instructions.push(
                initialize_confidential_transfer_fee_config(
                    &TOKEN_2022_ID,
                    &address,
                    Some(payer),
                    &(*withdraw_withheld_authority.pubkey()).into(),
                )
                .unwrap(),
            );
            Some(fee)
        }
        MintKind::MintBurn => {
            let supply = ConfidentialKeys::from_signer(&trident.random_keypair());
            instructions.push(
                initialize_mint_burn(
                    &TOKEN_2022_ID,
                    &address,
                    &(*supply.elgamal.pubkey()).into(),
                    &supply.decryptable(0),
                )
                .unwrap(),
            );
            None
        }
    };
    instructions.push(
        initialize_mint2(
            &TOKEN_2022_ID,
            &address,
            &payer,
            None,
            CONFIDENTIAL_MINT_DECIMALS,
        )
        .unwrap(),
    );
    send_ok(trident, &instructions, "create_confidential_mint");

    ConfidentialMint {
        address,
        auditor,
        withdraw_withheld_authority,
        fee,
    }
}

/// Creates `owner`'s ATA for `mint` and configures it for confidential transfers.
pub fn create_confidential_account(
    trident: &mut Trident,
    owner: &Pubkey,
    mint: &ConfidentialMint,
    keys: &ConfidentialKeys,
) -> Pubkey {
    let payer = trident.payer().pubkey();
    let account =
        get_associated_token_address_with_program_id(owner, &mint.address, &TOKEN_2022_ID);

    let mut extensions = vec![ExtensionType::ConfidentialTransferAccount];
    if mint.fee.is_some() {
        extensions.push(ExtensionType::ConfidentialTransferFeeAmount);
    }
    let proof = PubkeyValidityProofData::new(&keys.elgamal).unwrap();
    let mut instructions = vec![
        create_associated_token_account_idempotent(&payer, owner, &mint.address, &TOKEN_2022_ID),
        reallocate(&TOKEN_2022_ID, &account, &payer, owner, &[], &extensions).unwrap(),
    ];
    instructions.extend(
        confidential_instruction::configure_account(
            &TOKEN_2022_ID,
            &account,
            &mint.address,
            &keys.decryptable(0),
            MAXIMUM_PENDING_CREDITS,
            owner,
            &[],
            ProofLocation::InstructionOffset(1.try_into().unwrap(), &proof),
        )
        .unwrap(),
    );
    send_ok(trident, &instructions, "create_confidential_account");
    account
}

/// Mints `amount` to `account`'s public balance.
pub fn mint_public(trident: &mut Trident, mint: &Pubkey, account: &Pubkey, amount: u64) {
    let payer = trident.payer().pubkey();
    send_ok(
        trident,
        &[mint_to(&TOKEN_2022_ID, mint, account, &payer, &[], amount).unwrap()],
        "mint_public",
    );
}

/// Mints `amount` to `account` and moves it into its available confidential
/// balance, which must be empty: Token-2022's deposit and apply.
pub fn fund_confidential(
    trident: &mut Trident,
    owner: &Pubkey,
    account: &Pubkey,
    mint: &Pubkey,
    keys: &ConfidentialKeys,
    amount: u64,
) {
    let payer = trident.payer().pubkey();
    send_ok(
        trident,
        &[
            mint_to(&TOKEN_2022_ID, mint, account, &payer, &[], amount).unwrap(),
            confidential_instruction::deposit(
                &TOKEN_2022_ID,
                account,
                mint,
                amount,
                CONFIDENTIAL_MINT_DECIMALS,
                owner,
                &[],
            )
            .unwrap(),
            confidential_instruction::apply_pending_balance(
                &TOKEN_2022_ID,
                account,
                1,
                &keys.decryptable(amount),
                owner,
                &[],
            )
            .unwrap(),
        ],
        "fund_confidential",
    );
}

// ── Proofs ────────────────────────────────────────────────────────────────────

/// Where a flow puts its proofs.
#[derive(Clone, Copy)]
pub enum ProofPlacement {
    /// Verify instructions right after the escrow instruction, in one transaction.
    Inline,
    /// Context accounts, verified in earlier transactions.
    ContextAccounts,
    /// Each proof one way or the other, at random.
    Mixed,
}

/// The proofs of one instruction, in Token-2022's proof order.
pub struct PlacedProofs {
    placement: ProofPlacement,
    instructions: Vec<Instruction>,
    pub context_accounts: Vec<AccountMeta>,
}

impl PlacedProofs {
    pub fn new(placement: ProofPlacement) -> Self {
        Self {
            placement,
            instructions: Vec::new(),
            context_accounts: Vec::new(),
        }
    }

    /// Places one proof and returns the offset the instruction passes for it.
    /// A context account is verified here, so only honest proofs go there.
    pub fn place<T: Pod + ZkProofData<U>, U: Pod>(
        &mut self,
        trident: &mut Trident,
        instruction: ProofInstruction,
        proof: &T,
    ) -> i8 {
        let inline = match self.placement {
            ProofPlacement::Inline => true,
            ProofPlacement::ContextAccounts => false,
            ProofPlacement::Mixed => trident.random_bool(),
        };
        if inline {
            self.instructions
                .push(instruction.encode_verify_proof(None, proof));
            // Counts only the inline proofs, so it stays right when they are mixed.
            return self.instructions.len() as i8;
        }

        let payer = trident.payer().pubkey();
        let account = trident.random_pubkey();
        let space = size_of::<ProofContextState<U>>();
        let rent = trident.get_sysvar::<Rent>();
        send_ok(
            trident,
            &[
                system_instruction::create_account(
                    &payer,
                    &account,
                    rent.minimum_balance(space),
                    space as u64,
                    &solana_zk_sdk::zk_elgamal_proof_program::id(),
                ),
                instruction.encode_verify_proof(
                    Some(ContextStateInfo {
                        context_state_account: &account,
                        context_state_authority: &payer,
                    }),
                    proof,
                ),
            ],
            "verify_into_context_account",
        );
        self.context_accounts
            .push(AccountMeta::new_readonly(account, false));
        0
    }

    /// `instruction` with the context accounts appended, followed by the inline
    /// proofs.
    pub fn with(&self, mut instruction: Instruction) -> Vec<Instruction> {
        instruction
            .accounts
            .extend(self.context_accounts.iter().cloned());
        let mut instructions = vec![instruction];
        instructions.extend(self.instructions.iter().cloned());
        instructions
    }
}

/// A confidential transfer's proofs and the sender fields the escrow forwards.
pub struct PreparedTransfer {
    pub proofs: PlacedProofs,
    pub new_source_decryptable_available_balance: [u8; 36],
    pub auditor_ciphertext_lo: [u8; 64],
    pub auditor_ciphertext_hi: [u8; 64],
    pub equality_proof_instruction_offset: i8,
    pub ciphertext_validity_proof_instruction_offset: i8,
    pub range_proof_instruction_offset: i8,
    pub fee_sigma_proof_instruction_offset: i8,
    pub fee_ciphertext_validity_proof_instruction_offset: i8,
}

/// Proves a transfer of `amount` out of `source`, whose available balance is
/// claimed to be `balance`. A wrong claim yields proofs that fail verification,
/// so keep those inline. `None` when `amount` exceeds the claim.
#[allow(clippy::too_many_arguments)]
pub fn prepare_transfer(
    trident: &mut Trident,
    source: &Pubkey,
    source_keys: &ConfidentialKeys,
    balance: u64,
    destination: &ElGamalPubkey,
    mint: &ConfidentialMint,
    amount: u64,
    placement: ProofPlacement,
) -> Option<PreparedTransfer> {
    let new_balance = balance.checked_sub(amount)?;
    let available = available_ciphertext(trident, source);
    // The generators read the balance from an AES ciphertext.
    let decryptable = source_keys.aes.encrypt(balance);
    let mut proofs = PlacedProofs::new(placement);

    // Placed in Token-2022's order, so the context accounts line up with it.
    let Some(fee) = mint.fee else {
        let data = transfer_split_proof_data(
            &available,
            &decryptable,
            amount,
            &source_keys.elgamal,
            &source_keys.aes,
            destination,
            Some(mint.auditor.pubkey()),
        )
        .ok()?;
        let validity = &data.ciphertext_validity_proof_data_with_ciphertext;
        let equality_proof_instruction_offset = proofs.place(
            trident,
            ProofInstruction::VerifyCiphertextCommitmentEquality,
            &data.equality_proof_data,
        );
        let ciphertext_validity_proof_instruction_offset = proofs.place(
            trident,
            ProofInstruction::VerifyBatchedGroupedCiphertext3HandlesValidity,
            &validity.proof_data,
        );
        let range_proof_instruction_offset = proofs.place(
            trident,
            ProofInstruction::VerifyBatchedRangeProofU128,
            &data.range_proof_data,
        );
        return Some(PreparedTransfer {
            new_source_decryptable_available_balance: source_keys.decryptable_bytes(new_balance),
            auditor_ciphertext_lo: elgamal_bytes(validity.ciphertext_lo),
            auditor_ciphertext_hi: elgamal_bytes(validity.ciphertext_hi),
            equality_proof_instruction_offset,
            ciphertext_validity_proof_instruction_offset,
            range_proof_instruction_offset,
            fee_sigma_proof_instruction_offset: 0,
            fee_ciphertext_validity_proof_instruction_offset: 0,
            proofs,
        });
    };

    let data = transfer_with_fee_split_proof_data(
        &available,
        &decryptable,
        amount,
        &source_keys.elgamal,
        &source_keys.aes,
        destination,
        Some(mint.auditor.pubkey()),
        mint.withdraw_withheld_authority.pubkey(),
        fee.basis_points,
        fee.maximum_fee,
    )
    .ok()?;
    let validity = &data.transfer_amount_ciphertext_validity_proof_data_with_ciphertext;
    let equality_proof_instruction_offset = proofs.place(
        trident,
        ProofInstruction::VerifyCiphertextCommitmentEquality,
        &data.equality_proof_data,
    );
    let ciphertext_validity_proof_instruction_offset = proofs.place(
        trident,
        ProofInstruction::VerifyBatchedGroupedCiphertext3HandlesValidity,
        &validity.proof_data,
    );
    let fee_sigma_proof_instruction_offset = proofs.place(
        trident,
        ProofInstruction::VerifyPercentageWithCap,
        &data.percentage_with_cap_proof_data,
    );
    let fee_ciphertext_validity_proof_instruction_offset = proofs.place(
        trident,
        ProofInstruction::VerifyBatchedGroupedCiphertext2HandlesValidity,
        &data.fee_ciphertext_validity_proof_data,
    );
    let range_proof_instruction_offset = proofs.place(
        trident,
        ProofInstruction::VerifyBatchedRangeProofU256,
        &data.range_proof_data,
    );
    Some(PreparedTransfer {
        new_source_decryptable_available_balance: source_keys.decryptable_bytes(new_balance),
        auditor_ciphertext_lo: elgamal_bytes(validity.ciphertext_lo),
        auditor_ciphertext_hi: elgamal_bytes(validity.ciphertext_hi),
        equality_proof_instruction_offset,
        ciphertext_validity_proof_instruction_offset,
        range_proof_instruction_offset,
        fee_sigma_proof_instruction_offset,
        fee_ciphertext_validity_proof_instruction_offset,
        proofs,
    })
}

/// The proofs for moving `amount` from `account`'s available balance, claimed to
/// be `balance`, to its public one. `None` when `amount` exceeds the claim.
pub struct PreparedWithdraw {
    pub proofs: PlacedProofs,
    pub new_decryptable_available_balance: [u8; 36],
    pub equality_proof_instruction_offset: i8,
    pub range_proof_instruction_offset: i8,
}

pub fn prepare_withdraw(
    trident: &mut Trident,
    account: &Pubkey,
    keys: &ConfidentialKeys,
    balance: u64,
    amount: u64,
    placement: ProofPlacement,
) -> Option<PreparedWithdraw> {
    let new_balance = balance.checked_sub(amount)?;
    let available = available_ciphertext(trident, account);
    let data = withdraw_proof_data(&available, balance, amount, &keys.elgamal).ok()?;

    let mut proofs = PlacedProofs::new(placement);
    let equality_proof_instruction_offset = proofs.place(
        trident,
        ProofInstruction::VerifyCiphertextCommitmentEquality,
        &data.equality_proof_data,
    );
    let range_proof_instruction_offset = proofs.place(
        trident,
        ProofInstruction::VerifyBatchedRangeProofU64,
        &data.range_proof_data,
    );
    Some(PreparedWithdraw {
        proofs,
        new_decryptable_available_balance: keys.decryptable_bytes(new_balance),
        equality_proof_instruction_offset,
        range_proof_instruction_offset,
    })
}

// ── State ─────────────────────────────────────────────────────────────────────

/// The account's available balance ciphertext. An escrow account not yet
/// configured has none, so proofs start from zero and the transaction still
/// reaches the escrow, which must refuse it.
fn available_ciphertext(trident: &mut Trident, account: &Pubkey) -> ElGamalCiphertext {
    match confidential_state(trident, account) {
        Some(state) => state.available_balance.try_into().unwrap(),
        None => ElGamalCiphertext::from_bytes(&[0; 64]).unwrap(),
    }
}

/// The account's confidential extension, `None` before it is configured.
pub fn confidential_state(
    trident: &mut Trident,
    account: &Pubkey,
) -> Option<ConfidentialTransferAccount> {
    let account = trident.get_account(account);
    let state = StateWithExtensions::<TokenAccount>::unpack(account.data()).ok()?;
    state
        .get_extension::<ConfidentialTransferAccount>()
        .ok()
        .copied()
}

pub fn public_balance(trident: &mut Trident, account: &Pubkey) -> u64 {
    let account = trident.get_account(account);
    StateWithExtensions::<TokenAccount>::unpack(account.data())
        .unwrap()
        .base
        .amount
}

/// Public transfer fees withheld in the account.
pub fn public_withheld(trident: &mut Trident, account: &Pubkey) -> u64 {
    let account = trident.get_account(account);
    StateWithExtensions::<TokenAccount>::unpack(account.data())
        .unwrap()
        .get_extension::<TransferFeeAmount>()
        .map_or(0, |fee| fee.withheld_amount.into())
}

/// Confidential transfer fees withheld in the account, under the mint's
/// withdraw-withheld authority key.
pub fn confidential_withheld(
    trident: &mut Trident,
    account: &Pubkey,
    mint: &ConfidentialMint,
) -> u64 {
    let account = trident.get_account(account);
    StateWithExtensions::<TokenAccount>::unpack(account.data())
        .unwrap()
        .get_extension::<ConfidentialTransferFeeAmount>()
        .map_or(0, |fee| {
            decrypt(&mint.withdraw_withheld_authority, fee.withheld_amount)
        })
}

pub fn mint_supply(trident: &mut Trident, mint: &Pubkey) -> u64 {
    let account = trident.get_account(mint);
    StateWithExtensions::<Mint>::unpack(account.data())
        .unwrap()
        .base
        .supply
}

/// Every balance in this harness stays below 2^32 base units, so a u32 discrete
/// log decrypts it.
pub fn decrypt(keypair: &ElGamalKeypair, ciphertext: PodElGamalCiphertext) -> u64 {
    let ciphertext: ElGamalCiphertext = ciphertext.try_into().unwrap();
    ciphertext.decrypt_u32(keypair.secret()).unwrap()
}

/// A part of a pending balance, which can be negative: Token-2022 takes a fee off
/// the low and high parts separately, so a fee whose low part is larger than the
/// amount's leaves the low part below zero.
fn decrypt_part(keypair: &ElGamalKeypair, ciphertext: PodElGamalCiphertext) -> i64 {
    let ciphertext: ElGamalCiphertext = ciphertext.try_into().unwrap();
    if let Some(value) = ciphertext.decrypt_u32(keypair.secret()) {
        return value as i64;
    }
    let zero = ElGamalCiphertext::from_bytes(&[0; 64]).unwrap();
    -((zero - ciphertext).decrypt_u32(keypair.secret()).unwrap() as i64)
}

/// Pending low part plus the high part shifted past the low 16 bits.
pub fn decrypt_pending(state: &ConfidentialTransferAccount, keys: &ConfidentialKeys) -> u64 {
    (decrypt_part(&keys.elgamal, state.pending_balance_lo)
        + (decrypt_part(&keys.elgamal, state.pending_balance_hi) << 16)) as u64
}

/// The available balance as Token-2022 holds it, not the AES copy.
pub fn decrypt_available(state: &ConfidentialTransferAccount, keys: &ConfidentialKeys) -> u64 {
    decrypt(&keys.elgamal, state.available_balance)
}

/// The AES copy of the available balance, as last written.
pub fn decrypt_aes_copy(state: &ConfidentialTransferAccount, keys: &ConfidentialKeys) -> u64 {
    let ciphertext: AeCiphertext = state.decryptable_available_balance.try_into().unwrap();
    keys.aes.decrypt(&ciphertext).unwrap()
}
