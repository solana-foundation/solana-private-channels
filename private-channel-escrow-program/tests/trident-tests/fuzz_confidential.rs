//! # Fuzz harness: confidential transfers through the escrow
//!
//! One escrow per iteration on a confidential Token-2022 mint: plain, with a
//! transfer fee, or now and then a ConfidentialMintBurn mint, which the escrow
//! refuses to configure. The escrow starts unconfigured, as every deployed one is,
//! and configure is a flow, so it can land after public deposits and fees, and
//! every confidential flow also runs before it. Flows mix confidential deposits and
//! releases, applies from fresh or stale operator reads, moves between the two
//! balances, public deposits and releases on the same account, re-allowing the
//! mint, the gates and the whitelist. Every flow predicts its outcome from the
//! model, and a refused one must leave the accounts byte for byte as they were.
//!
//! Escrow events are not checked: Trident does not expose inner instructions. The
//! integration tests check each event; here the same facts are read from the
//! accounts.
//!
//! Invariants checked at the end of each iteration:
//! - The escrow's public, pending and available balances, its credit count and its
//!   withheld fees match the model, and so do the user's.
//! - The mint supply equals every balance plus every withheld fee.
//! - Confidential credits stay off outside the escrow's own instructions.
//! - The AES copy holds the value last written: possibly stale after an apply,
//!   exact after a release or a move to public.
//! - The escrow's ElGamal key never changes.
//! - The bitmap holds exactly the nonces the model spent, which both kinds of
//!   release share.

mod confidential;
#[allow(dead_code)]
mod shared;

use std::collections::HashSet;

use private_channel_escrow_program_client::{
    instructions::{
        AddOperatorBuilder, AddOrUpdateAllowedWalletBuilder, AllowMintBuilder,
        ApplyPendingBalanceBuilder, BlockMintBuilder, ConfidentialDepositBuilder,
        ConfidentialReleaseFundsBuilder, ConfigureConfidentialAccountBuilder,
        CreateInstanceBuilder, DepositBuilder, MoveToConfidentialBuilder, MoveToPublicBuilder,
        ReleaseFundsBuilder, RemoveAllowedWalletBuilder, SetWhitelistEnabledBuilder,
    },
    PrivateChannelEscrowProgramError,
};
use solana_zk_sdk::{
    encryption::pod::elgamal::PodElGamalPubkey,
    zk_elgamal_proof_program::{
        instruction::ProofInstruction, proof_data::PubkeyValidityProofData,
    },
};
use spl_associated_token_account::get_associated_token_address_with_program_id;
use spl_token_2022_interface::extension::confidential_transfer::instruction as confidential_instruction;
use spl_token_confidential_transfer_proof_extraction::instruction::ProofLocation;
use trident_fuzz::fuzzing::*;

use confidential::{
    confidential_state, confidential_withheld, create_confidential_account,
    create_confidential_mint, decrypt_aes_copy, decrypt_available, decrypt_pending,
    fund_confidential, mint_public, mint_supply, prepare_transfer, prepare_withdraw,
    public_balance, public_withheld, send, send_ok, ConfidentialKeys, ConfidentialMint, MintKind,
    ProofPlacement, TransferFee, TOKEN_2022_ID,
};
use shared::{
    ToClient, ToTrident, FUZZ_MIN_DEPOSIT_AMOUNT, FUZZ_MIN_WITHDRAW_AMOUNT, FUZZ_WITHDRAW_FEE,
    PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
};

const USER_PUBLIC_BALANCE: u64 = 50_000_000;
const USER_CONFIDENTIAL_BALANCE: u64 = 50_000_000;
/// Largest random amount. Edge amounts come on top, and nothing an iteration moves
/// gets near the 2^32 base units a u32 discrete log decrypts.
const MAX_AMOUNT: u64 = 2_000_000;
/// Releases draw nonces from this range, so they collide, across both kinds.
const NONCE_RANGE: u64 = 64;
/// Discriminator, bump and generation come before the bitmap's bits.
const BITMAP_BITS_OFFSET: usize = 10;

// ── Escrow and model ──────────────────────────────────────────────────────────

/// Everything one iteration created.
struct Escrow {
    admin: Pubkey,
    instance: Pubkey,
    withdrawal_bitmap: Pubkey,
    operator: Pubkey,
    operator_pda: Pubkey,
    allowed_mint: Pubkey,
    escrow_ata: Pubkey,
    escrow_keys: ConfidentialKeys,
    mint: ConfidentialMint,
    /// The depositor, and the destination of every release.
    user: Pubkey,
    user_ata: Pubkey,
    user_keys: ConfidentialKeys,
    /// A second wallet, as deposit recipient or release withdrawer, so the
    /// whitelist checks two different wallets.
    other: Pubkey,
    /// False for a ConfidentialMintBurn mint, which configure must refuse.
    configurable: bool,
}

impl Escrow {
    fn wallet(&self, wallet: Wallet) -> Pubkey {
        match wallet {
            Wallet::User => self.user,
            Wallet::Other => self.other,
        }
    }

    fn allowed_wallet_pda(&self, wallet: &Pubkey) -> Pubkey {
        Pubkey::find_program_address(
            &[b"allowed_wallet", self.instance.as_ref(), wallet.as_ref()],
            &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
        )
        .0
    }
}

#[derive(Clone, Copy)]
enum Wallet {
    User,
    Other,
}

/// A whitelist entry as the model sees it.
#[derive(Default, Clone, Copy)]
struct Entry {
    listed: bool,
    deposits_blocked: bool,
    withdrawals_blocked: bool,
}

impl Entry {
    fn allows_deposit(&self) -> bool {
        self.listed && !self.deposits_blocked
    }

    fn allows_withdrawal(&self) -> bool {
        self.listed && !self.withdrawals_blocked
    }
}

/// What every account should hold.
#[derive(Default)]
struct Model {
    /// ConfigureConfidentialAccount landed.
    configured: bool,
    escrow_public: u64,
    escrow_pending: u64,
    /// The operator's own count of the available balance.
    escrow_available: u64,
    escrow_credits: u64,
    /// The available balance the AES copy was last written with.
    escrow_aes_copy: u64,
    escrow_public_withheld: u64,
    escrow_confidential_withheld: u64,
    user_public: u64,
    user_pending: u64,
    user_available: u64,
    user_public_withheld: u64,
    user_confidential_withheld: u64,
    /// The pending balance the operator last read. An apply built from it misses
    /// later credits, so the AES copy it writes lags.
    operator_read_pending: u64,
    nonces: HashSet<u64>,
    deposits_blocked: bool,
    withdrawals_blocked: bool,
    deposits_whitelisted: bool,
    withdrawals_whitelisted: bool,
    user_entry: Entry,
    other_entry: Entry,
}

impl Model {
    fn entry(&self, wallet: Wallet) -> Entry {
        match wallet {
            Wallet::User => self.user_entry,
            Wallet::Other => self.other_entry,
        }
    }

    fn entry_mut(&mut self, wallet: Wallet) -> &mut Entry {
        match wallet {
            Wallet::User => &mut self.user_entry,
            Wallet::Other => &mut self.other_entry,
        }
    }

    /// The user deposits for `recipient`: both must be allowed to deposit.
    fn deposit_allowed(&self, recipient: Wallet) -> bool {
        !self.deposits_blocked
            && (!self.deposits_whitelisted
                || (self.user_entry.allows_deposit() && self.entry(recipient).allows_deposit()))
    }

    /// `withdrawer` withdraws to the user: both must be allowed to withdraw.
    fn withdrawal_allowed(&self, withdrawer: Wallet) -> bool {
        !self.withdrawals_blocked
            && (!self.withdrawals_whitelisted
                || (self.entry(withdrawer).allows_withdrawal()
                    && self.user_entry.allows_withdrawal()))
    }
}

/// What a release or move to public proves the escrow's available balance from.
#[derive(Clone, Copy)]
enum ProofSource {
    /// The operator's count: always right.
    Count,
    /// The AES copy, which lags after an apply from a stale read.
    AesCopy,
    /// The count, but an apply lands between proving and sending.
    Stale,
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn random_wallet(trident: &mut Trident) -> Wallet {
    if trident.random_bool() {
        Wallet::User
    } else {
        Wallet::Other
    }
}

/// All inline, all in context accounts, or mixed, which puts a context account
/// for one proof after an inline one and shifts the escrow's pick among them.
fn random_placement(trident: &mut Trident) -> ProofPlacement {
    match trident.random_from_range(0..3u8) {
        0 => ProofPlacement::Inline,
        1 => ProofPlacement::ContextAccounts,
        _ => ProofPlacement::Mixed,
    }
}

/// An amount up to `limit`, half the time from the edges: zero, around the
/// 16-bit split of a transfer amount, or exactly `limit`.
fn pick_amount(trident: &mut Trident, limit: u64) -> u64 {
    let edges = [0, 1, 65_535, 65_536, 65_537, limit];
    let amount = if trident.random_bool() {
        edges[trident.random_from_range(0..edges.len())]
    } else {
        trident.random_from_range(0..=MAX_AMOUNT)
    };
    amount.min(limit)
}

/// Like [`pick_amount`], but now and then one past `limit`, for the flows that
/// need no proof of the amount.
fn pick_amount_or_over(trident: &mut Trident, limit: u64) -> u64 {
    if trident.random_from_range(0..4u8) == 0 {
        limit + 1
    } else {
        pick_amount(trident, limit)
    }
}

fn account_bytes(trident: &mut Trident, accounts: &[Pubkey]) -> Vec<Vec<u8>> {
    accounts
        .iter()
        .map(|account| trident.get_account(account).data().to_vec())
        .collect()
}

/// ApplyPendingBalance, with the AES copy built from a fresh read of the pending
/// balance or from the operator's last one.
fn apply_pending(trident: &mut Trident, escrow: &Escrow, model: &mut Model, stale: bool) {
    let read_pending = if stale {
        model.operator_read_pending
    } else {
        model.escrow_pending
    };
    let aes_copy = model.escrow_available + read_pending;
    let instruction = ApplyPendingBalanceBuilder::new()
        .payer(trident.payer().pubkey().client())
        .operator(escrow.operator.client())
        .instance(escrow.instance.client())
        .operator_pda(escrow.operator_pda.client())
        .mint(escrow.mint.address.client())
        .instance_ata(escrow.escrow_ata.client())
        .expected_credit_counter(model.escrow_credits)
        .new_decryptable_available_balance(escrow.escrow_keys.decryptable_bytes(aes_copy))
        .instruction()
        .to_trident();

    let before = account_bytes(trident, &[escrow.escrow_ata]);
    let outcome = send(trident, &[instruction], "apply_pending_balance");
    if !model.configured {
        assert!(
            outcome.refused_by(
                &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
                PrivateChannelEscrowProgramError::ConfidentialAccountNotConfigured as u32,
            ),
            "apply on an unconfigured escrow: {}",
            outcome.logs
        );
        assert_eq!(account_bytes(trident, &[escrow.escrow_ata]), before);
        return;
    }
    // No counter check, so a credit can never starve it.
    assert!(outcome.success, "apply failed: {}", outcome.logs);

    model.escrow_available += model.escrow_pending;
    model.escrow_pending = 0;
    model.escrow_credits = 0;
    model.escrow_aes_copy = aes_copy;
    model.operator_read_pending = 0;
}

// ── Fuzz test ─────────────────────────────────────────────────────────────────

#[derive(Default, FuzzTestMethods)]
pub struct FuzzTest {
    pub trident: Trident,
    /// Trident's flow executor resets this between iterations. The addresses live
    /// in `escrow` instead.
    pub fuzz_accounts: (),
    escrow: Option<Escrow>,
    model: Model,
}

#[flow_executor]
impl FuzzTest {
    fn new() -> Self {
        Self::default()
    }

    #[init]
    fn start(&mut self) {
        let trident = &mut self.trident;
        let payer = trident.payer().pubkey();

        let kind = match trident.random_from_range(0..10u8) {
            0 => MintKind::MintBurn,
            1..=4 => MintKind::Fee(TransferFee {
                basis_points: trident.random_from_range(1..=1_000u16),
                maximum_fee: trident.random_from_range(1..=50_000u64),
            }),
            _ => MintKind::Plain,
        };
        let mint_burn = matches!(kind, MintKind::MintBurn);
        let mint = create_confidential_mint(trident, kind);

        // Instance, allowed mint and operator.
        let admin = trident.random_pubkey();
        let instance_seed = trident.random_pubkey();
        let (instance, instance_bump) = Pubkey::find_program_address(
            &[b"instance", instance_seed.as_ref()],
            &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
        );
        let (withdrawal_bitmap, bitmap_bump) = Pubkey::find_program_address(
            &[b"withdrawal_bitmap", instance.as_ref()],
            &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
        );
        send_ok(
            trident,
            &[CreateInstanceBuilder::new()
                .payer(payer.client())
                .admin(admin.client())
                .instance_seed(instance_seed.client())
                .instance(instance.client())
                .withdrawal_bitmap(withdrawal_bitmap.client())
                .bump(instance_bump)
                .bitmap_bump(bitmap_bump)
                .instruction()
                .to_trident()],
            "create_instance",
        );

        let (allowed_mint, allowed_mint_bump) = Pubkey::find_program_address(
            &[b"allowed_mint", instance.as_ref(), mint.address.as_ref()],
            &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
        );
        let escrow_ata =
            get_associated_token_address_with_program_id(&instance, &mint.address, &TOKEN_2022_ID);
        send_ok(
            trident,
            &[AllowMintBuilder::new()
                .payer(payer.client())
                .admin(admin.client())
                .instance(instance.client())
                .mint(mint.address.client())
                .allowed_mint(allowed_mint.client())
                .instance_ata(escrow_ata.client())
                .token_program(TOKEN_2022_ID.client())
                .bump(allowed_mint_bump)
                .withdraw_fee(FUZZ_WITHDRAW_FEE)
                .min_withdraw_amount(FUZZ_MIN_WITHDRAW_AMOUNT)
                .min_deposit_amount(FUZZ_MIN_DEPOSIT_AMOUNT)
                .instruction()
                .to_trident()],
            "allow_mint",
        );

        let operator = trident.random_pubkey();
        let (operator_pda, operator_bump) = Pubkey::find_program_address(
            &[b"operator", instance.as_ref(), operator.as_ref()],
            &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
        );
        send_ok(
            trident,
            &[AddOperatorBuilder::new()
                .payer(payer.client())
                .admin(admin.client())
                .instance(instance.client())
                .operator(operator.client())
                .operator_pda(operator_pda.client())
                .bump(operator_bump)
                .instruction()
                .to_trident()],
            "add_operator",
        );

        // The user, funded on both sides unless public mint is refused.
        let user = trident.random_pubkey();
        let user_keys = ConfidentialKeys::from_signer(&trident.random_keypair());
        let user_ata = create_confidential_account(trident, &user, &mint, &user_keys);
        let mut model = Model::default();
        if !mint_burn {
            fund_confidential(
                trident,
                &user,
                &user_ata,
                &mint.address,
                &user_keys,
                USER_CONFIDENTIAL_BALANCE,
            );
            mint_public(trident, &mint.address, &user_ata, USER_PUBLIC_BALANCE);
            model.user_available = USER_CONFIDENTIAL_BALANCE;
            model.user_public = USER_PUBLIC_BALANCE;
        }

        // The keys the escrow is configured with, once fuzz_configure lands.
        let escrow_keys = ConfidentialKeys::from_signer(&trident.random_keypair());

        self.escrow = Some(Escrow {
            admin,
            instance,
            withdrawal_bitmap,
            operator,
            operator_pda,
            allowed_mint,
            escrow_ata,
            escrow_keys,
            mint,
            user,
            user_ata,
            user_keys,
            other: trident.random_pubkey(),
            configurable: !mint_burn,
        });
        self.model = model;

        // Start from a random whitelist, so the flows meet every combination of
        // listed and unlisted wallets, not only the ones a few flows can build.
        self.fuzz_set_whitelist();
        self.fuzz_add_or_update_allowed_wallet();
        self.fuzz_add_or_update_allowed_wallet();

        // Half the iterations start configured, so long confidential sequences get
        // every step. The other half configure later, over an account that may
        // already hold public deposits and fees.
        if self.trident.random_bool() {
            self.fuzz_configure();
        }
    }

    // ── Confidential flows ────────────────────────────────────────────────────

    /// The user deposits a hidden amount for themselves or the other wallet.
    #[flow]
    fn fuzz_confidential_deposit(&mut self) {
        let escrow = self.escrow.as_ref().unwrap();
        let trident = &mut self.trident;
        let model = &mut self.model;

        let recipient = random_wallet(trident);
        let amount = pick_amount(trident, model.user_available);
        let placement = random_placement(trident);
        let Some(transfer) = prepare_transfer(
            trident,
            &escrow.user_ata,
            &escrow.user_keys,
            model.user_available,
            escrow.escrow_keys.elgamal.pubkey(),
            &escrow.mint,
            amount,
            placement,
        ) else {
            return;
        };
        let recipient_wallet = escrow.wallet(recipient);
        let instruction = ConfidentialDepositBuilder::new()
            .payer(trident.payer().pubkey().client())
            .user(escrow.user.client())
            .instance(escrow.instance.client())
            .mint(escrow.mint.address.client())
            .allowed_mint(escrow.allowed_mint.client())
            .user_ata(escrow.user_ata.client())
            .instance_ata(escrow.escrow_ata.client())
            .user_allowed_wallet_pda(escrow.allowed_wallet_pda(&escrow.user).client())
            .recipient_allowed_wallet_pda(escrow.allowed_wallet_pda(&recipient_wallet).client())
            .new_source_decryptable_available_balance(
                transfer.new_source_decryptable_available_balance,
            )
            .auditor_ciphertext_lo(transfer.auditor_ciphertext_lo)
            .auditor_ciphertext_hi(transfer.auditor_ciphertext_hi)
            .equality_proof_instruction_offset(transfer.equality_proof_instruction_offset)
            .ciphertext_validity_proof_instruction_offset(
                transfer.ciphertext_validity_proof_instruction_offset,
            )
            .range_proof_instruction_offset(transfer.range_proof_instruction_offset)
            .fee_sigma_proof_instruction_offset(transfer.fee_sigma_proof_instruction_offset)
            .fee_ciphertext_validity_proof_instruction_offset(
                transfer.fee_ciphertext_validity_proof_instruction_offset,
            )
            .recipient(recipient_wallet.client())
            .instruction()
            .to_trident();

        let watched = [escrow.escrow_ata, escrow.user_ata];
        let before = account_bytes(trident, &watched);
        let outcome = send(
            trident,
            &transfer.proofs.with(instruction),
            "confidential_deposit",
        );
        if !(model.configured && model.deposit_allowed(recipient)) {
            assert!(!outcome.success, "confidential deposit of {amount} landed");
            // The gates and the whitelist come first, then the pending balance read.
            if model.deposit_allowed(recipient) {
                assert!(
                    outcome.refused_by(
                        &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
                        PrivateChannelEscrowProgramError::ConfidentialAccountNotConfigured as u32,
                    ),
                    "confidential deposit of {amount} before configure: {}",
                    outcome.logs
                );
            }
            assert_eq!(
                account_bytes(trident, &watched),
                before,
                "a refused confidential deposit changed an account"
            );
            return;
        }
        assert!(
            outcome.success,
            "confidential deposit of {amount} failed: {}",
            outcome.logs
        );

        let fee = escrow.mint.fee(amount);
        model.user_available -= amount;
        model.escrow_pending += amount - fee;
        model.escrow_confidential_withheld += fee;
        model.escrow_credits += 1;
        // The escrow turns credits on for its own transfer only.
        let state = confidential_state(trident, &escrow.escrow_ata).unwrap();
        assert!(
            !bool::from(state.allow_confidential_credits),
            "ConfidentialDeposit left confidential credits on"
        );
    }

    /// The operator reads the pending balance, for a later stale apply.
    #[flow]
    fn fuzz_operator_read(&mut self) {
        self.model.operator_read_pending = self.model.escrow_pending;
    }

    #[flow]
    fn fuzz_apply(&mut self) {
        let stale = self.trident.random_bool();
        apply_pending(
            &mut self.trident,
            self.escrow.as_ref().unwrap(),
            &mut self.model,
            stale,
        );
    }

    /// A hidden release to the user, for the user or the other wallet's burn.
    /// Before configure, Token-2022 refuses the transfer.
    #[flow]
    fn fuzz_confidential_release(&mut self) {
        let escrow = self.escrow.as_ref().unwrap();
        let trident = &mut self.trident;
        let model = &mut self.model;

        let withdrawer = random_wallet(trident);
        let nonce = trident.random_from_range(0..NONCE_RANGE);
        let source = match trident.random_from_range(0..3u8) {
            0 => ProofSource::Count,
            1 => ProofSource::AesCopy,
            _ => ProofSource::Stale,
        };
        // A wrong claim fails verification, which only an inline proof can.
        let (claimed, placement) = match source {
            ProofSource::AesCopy => (model.escrow_aes_copy, ProofPlacement::Inline),
            _ => (model.escrow_available, random_placement(trident)),
        };
        let amount = pick_amount(trident, claimed);
        let Some(transfer) = prepare_transfer(
            trident,
            &escrow.escrow_ata,
            &escrow.escrow_keys,
            claimed,
            escrow.user_keys.elgamal.pubkey(),
            &escrow.mint,
            amount,
            placement,
        ) else {
            return;
        };
        let proofs_valid = match source {
            ProofSource::Count => true,
            ProofSource::AesCopy => model.escrow_aes_copy == model.escrow_available,
            // Only an apply that moves a credit changes the available ciphertext.
            ProofSource::Stale if model.escrow_credits > 0 => {
                apply_pending(trident, escrow, model, false);
                false
            }
            ProofSource::Stale => true,
        };
        let withdrawer_wallet = escrow.wallet(withdrawer);
        let instruction = ConfidentialReleaseFundsBuilder::new()
            .payer(trident.payer().pubkey().client())
            .operator(escrow.operator.client())
            .instance(escrow.instance.client())
            .withdrawal_bitmap(escrow.withdrawal_bitmap.client())
            .operator_pda(escrow.operator_pda.client())
            .mint(escrow.mint.address.client())
            .allowed_mint(escrow.allowed_mint.client())
            .user_ata(escrow.user_ata.client())
            .instance_ata(escrow.escrow_ata.client())
            .user_allowed_wallet_pda(escrow.allowed_wallet_pda(&escrow.user).client())
            .withdrawer_allowed_wallet_pda(escrow.allowed_wallet_pda(&withdrawer_wallet).client())
            .user(escrow.user.client())
            .transaction_nonce(nonce)
            .withdrawer(withdrawer_wallet.client())
            .expected_available_balance(transfer.expected_available_balance)
            .new_source_decryptable_available_balance(
                transfer.new_source_decryptable_available_balance,
            )
            .auditor_ciphertext_lo(transfer.auditor_ciphertext_lo)
            .auditor_ciphertext_hi(transfer.auditor_ciphertext_hi)
            .equality_proof_instruction_offset(transfer.equality_proof_instruction_offset)
            .ciphertext_validity_proof_instruction_offset(
                transfer.ciphertext_validity_proof_instruction_offset,
            )
            .range_proof_instruction_offset(transfer.range_proof_instruction_offset)
            .fee_sigma_proof_instruction_offset(transfer.fee_sigma_proof_instruction_offset)
            .fee_ciphertext_validity_proof_instruction_offset(
                transfer.fee_ciphertext_validity_proof_instruction_offset,
            )
            .instruction()
            .to_trident();

        let watched = [escrow.escrow_ata, escrow.user_ata, escrow.withdrawal_bitmap];
        let before = account_bytes(trident, &watched);
        let outcome = send(
            trident,
            &transfer.proofs.with(instruction),
            "confidential_release",
        );
        if !(model.configured
            && proofs_valid
            && model.withdrawal_allowed(withdrawer)
            && !model.nonces.contains(&nonce))
        {
            assert!(
                !outcome.success,
                "confidential release of {amount} with nonce {nonce} landed"
            );
            assert_eq!(
                account_bytes(trident, &watched),
                before,
                "a refused confidential release changed an account"
            );
            return;
        }
        assert!(
            outcome.success,
            "confidential release of {amount} with nonce {nonce} failed: {}",
            outcome.logs
        );

        let fee = escrow.mint.fee(amount);
        model.escrow_available -= amount;
        model.escrow_aes_copy = model.escrow_available;
        model.user_pending += amount - fee;
        model.user_confidential_withheld += fee;
        model.nonces.insert(nonce);
    }

    #[flow]
    fn fuzz_move_to_confidential(&mut self) {
        let escrow = self.escrow.as_ref().unwrap();
        let trident = &mut self.trident;
        let model = &mut self.model;

        let amount = pick_amount_or_over(trident, model.escrow_public);
        let instruction = MoveToConfidentialBuilder::new()
            .payer(trident.payer().pubkey().client())
            .operator(escrow.operator.client())
            .instance(escrow.instance.client())
            .operator_pda(escrow.operator_pda.client())
            .mint(escrow.mint.address.client())
            .instance_ata(escrow.escrow_ata.client())
            .amount(amount)
            .instruction()
            .to_trident();

        let before = account_bytes(trident, &[escrow.escrow_ata]);
        let outcome = send(trident, &[instruction], "move_to_confidential");
        if !(model.configured && amount > 0 && amount <= model.escrow_public) {
            assert!(!outcome.success, "move to confidential of {amount} landed");
            if amount == 0 {
                assert!(outcome.refused_by(
                    &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
                    PrivateChannelEscrowProgramError::ZeroAmount as u32,
                ));
            }
            assert_eq!(account_bytes(trident, &[escrow.escrow_ata]), before);
            return;
        }
        assert!(
            outcome.success,
            "move to confidential of {amount} failed: {}",
            outcome.logs
        );

        model.escrow_public -= amount;
        model.escrow_pending += amount;
        model.escrow_credits += 1;
        let state = confidential_state(trident, &escrow.escrow_ata).unwrap();
        assert!(
            !bool::from(state.allow_confidential_credits),
            "MoveToConfidential left confidential credits on"
        );
    }

    /// Before configure, Token-2022 refuses the withdraw.
    #[flow]
    fn fuzz_move_to_public(&mut self) {
        let escrow = self.escrow.as_ref().unwrap();
        let trident = &mut self.trident;
        let model = &mut self.model;

        let stale = trident.random_bool();
        let placement = random_placement(trident);
        let amount = pick_amount(trident, model.escrow_available);
        let Some(withdraw) = prepare_withdraw(
            trident,
            &escrow.escrow_ata,
            &escrow.escrow_keys,
            model.escrow_available,
            amount,
            placement,
        ) else {
            return;
        };
        let proofs_valid = if stale && model.escrow_credits > 0 {
            apply_pending(trident, escrow, model, false);
            false
        } else {
            true
        };
        let instruction = MoveToPublicBuilder::new()
            .payer(trident.payer().pubkey().client())
            .operator(escrow.operator.client())
            .instance(escrow.instance.client())
            .operator_pda(escrow.operator_pda.client())
            .mint(escrow.mint.address.client())
            .instance_ata(escrow.escrow_ata.client())
            .amount(amount)
            .new_decryptable_available_balance(withdraw.new_decryptable_available_balance)
            .equality_proof_instruction_offset(withdraw.equality_proof_instruction_offset)
            .range_proof_instruction_offset(withdraw.range_proof_instruction_offset)
            .instruction()
            .to_trident();

        let before = account_bytes(trident, &[escrow.escrow_ata]);
        let outcome = send(
            trident,
            &withdraw.proofs.with(instruction),
            "move_to_public",
        );
        if !(model.configured && proofs_valid && amount > 0) {
            assert!(!outcome.success, "move to public of {amount} landed");
            if amount == 0 {
                assert!(outcome.refused_by(
                    &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
                    PrivateChannelEscrowProgramError::ZeroAmount as u32,
                ));
            }
            assert_eq!(account_bytes(trident, &[escrow.escrow_ata]), before);
            return;
        }
        assert!(
            outcome.success,
            "move to public of {amount} failed: {}",
            outcome.logs
        );

        model.escrow_available -= amount;
        model.escrow_aes_copy = model.escrow_available;
        model.escrow_public += amount;
    }

    /// A Token-2022 confidential transfer straight into the escrow account, skipping
    /// the escrow, its whitelist and its event. Credits are off, so it never lands.
    #[flow]
    fn fuzz_direct_transfer_to_escrow(&mut self) {
        let escrow = self.escrow.as_ref().unwrap();
        let trident = &mut self.trident;
        let model = &mut self.model;
        // A fee mint takes TransferWithFee, which needs nothing new to be refused.
        if escrow.mint.fee.is_some() {
            return;
        }

        let amount = pick_amount(trident, model.user_available);
        let Some(transfer) = prepare_transfer(
            trident,
            &escrow.user_ata,
            &escrow.user_keys,
            model.user_available,
            escrow.escrow_keys.elgamal.pubkey(),
            &escrow.mint,
            amount,
            ProofPlacement::ContextAccounts,
        ) else {
            return;
        };
        let [equality, validity, range] =
            [0, 1, 2].map(|index| transfer.proofs.context_accounts[index].pubkey);
        let instructions = confidential_instruction::transfer(
            &TOKEN_2022_ID,
            &escrow.user_ata,
            &escrow.mint.address,
            &escrow.escrow_ata,
            &bytemuck::pod_read_unaligned(&transfer.new_source_decryptable_available_balance),
            &bytemuck::cast(transfer.auditor_ciphertext_lo),
            &bytemuck::cast(transfer.auditor_ciphertext_hi),
            &escrow.user,
            &[],
            ProofLocation::ContextStateAccount(&equality),
            ProofLocation::ContextStateAccount(&validity),
            ProofLocation::ContextStateAccount(&range),
        )
        .unwrap();

        let watched = [escrow.escrow_ata, escrow.user_ata];
        let before = account_bytes(trident, &watched);
        let outcome = send(trident, &instructions, "direct_transfer_to_escrow");
        assert!(
            !outcome.success,
            "a confidential transfer reached the escrow without the escrow"
        );
        assert_eq!(account_bytes(trident, &watched), before);
    }

    /// Either kind of release naming the instance itself as the user.
    #[flow]
    fn fuzz_release_to_instance(&mut self) {
        let escrow = self.escrow.as_ref().unwrap();
        let trident = &mut self.trident;
        let model = &mut self.model;

        let nonce = trident.random_from_range(0..NONCE_RANGE);
        let payer = trident.payer().pubkey();
        let instance_allowed_wallet_pda = escrow.allowed_wallet_pda(&escrow.instance);
        let user_allowed_wallet_pda = escrow.allowed_wallet_pda(&escrow.user);
        let instructions = if trident.random_bool() {
            let amount = pick_amount(trident, model.escrow_public);
            vec![ReleaseFundsBuilder::new()
                .payer(payer.client())
                .operator(escrow.operator.client())
                .instance(escrow.instance.client())
                .withdrawal_bitmap(escrow.withdrawal_bitmap.client())
                .operator_pda(escrow.operator_pda.client())
                .mint(escrow.mint.address.client())
                .allowed_mint(escrow.allowed_mint.client())
                .user_ata(escrow.escrow_ata.client())
                .instance_ata(escrow.escrow_ata.client())
                .token_program(TOKEN_2022_ID.client())
                .user_allowed_wallet_pda(instance_allowed_wallet_pda.client())
                .withdrawer_allowed_wallet_pda(user_allowed_wallet_pda.client())
                .amount(amount)
                .user(escrow.instance.client())
                .transaction_nonce(nonce)
                .withdrawer(escrow.user.client())
                .instruction()
                .to_trident()]
        } else {
            let amount = pick_amount(trident, model.escrow_available);
            let Some(transfer) = prepare_transfer(
                trident,
                &escrow.escrow_ata,
                &escrow.escrow_keys,
                model.escrow_available,
                escrow.escrow_keys.elgamal.pubkey(),
                &escrow.mint,
                amount,
                ProofPlacement::Inline,
            ) else {
                return;
            };
            transfer.proofs.with(
                ConfidentialReleaseFundsBuilder::new()
                    .payer(payer.client())
                    .operator(escrow.operator.client())
                    .instance(escrow.instance.client())
                    .withdrawal_bitmap(escrow.withdrawal_bitmap.client())
                    .operator_pda(escrow.operator_pda.client())
                    .mint(escrow.mint.address.client())
                    .allowed_mint(escrow.allowed_mint.client())
                    .user_ata(escrow.escrow_ata.client())
                    .instance_ata(escrow.escrow_ata.client())
                    .user_allowed_wallet_pda(instance_allowed_wallet_pda.client())
                    .withdrawer_allowed_wallet_pda(user_allowed_wallet_pda.client())
                    .user(escrow.instance.client())
                    .transaction_nonce(nonce)
                    .withdrawer(escrow.user.client())
                    .expected_available_balance(transfer.expected_available_balance)
                    .new_source_decryptable_available_balance(
                        transfer.new_source_decryptable_available_balance,
                    )
                    .auditor_ciphertext_lo(transfer.auditor_ciphertext_lo)
                    .auditor_ciphertext_hi(transfer.auditor_ciphertext_hi)
                    .equality_proof_instruction_offset(transfer.equality_proof_instruction_offset)
                    .ciphertext_validity_proof_instruction_offset(
                        transfer.ciphertext_validity_proof_instruction_offset,
                    )
                    .range_proof_instruction_offset(transfer.range_proof_instruction_offset)
                    .fee_sigma_proof_instruction_offset(transfer.fee_sigma_proof_instruction_offset)
                    .fee_ciphertext_validity_proof_instruction_offset(
                        transfer.fee_ciphertext_validity_proof_instruction_offset,
                    )
                    .instruction()
                    .to_trident(),
            )
        };

        let watched = [escrow.escrow_ata, escrow.withdrawal_bitmap];
        let before = account_bytes(trident, &watched);
        let outcome = send(trident, &instructions, "release_to_instance");
        assert!(!outcome.success, "a release to the instance landed");
        // The gates and the whitelist are checked first; the instance is never listed.
        if !model.withdrawals_blocked && !model.withdrawals_whitelisted {
            assert!(
                outcome.refused_by(
                    &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
                    PrivateChannelEscrowProgramError::InvalidRecipient as u32,
                ),
                "a release to the instance: {}",
                outcome.logs
            );
        }
        assert_eq!(account_bytes(trident, &watched), before);
    }

    /// ConfigureConfidentialAccount at any point, as on a deployed escrow that
    /// already holds public deposits and withheld fees. It lands once, on a
    /// supported mint, and leaves the public side alone. A repeat brings new keys,
    /// which must not replace the configured ones.
    #[flow]
    fn fuzz_configure(&mut self) {
        let escrow = self.escrow.as_ref().unwrap();
        let trident = &mut self.trident;
        let model = &mut self.model;

        let repeat_keys = model
            .configured
            .then(|| ConfidentialKeys::from_signer(&trident.random_keypair()));
        let keys = repeat_keys.as_ref().unwrap_or(&escrow.escrow_keys);
        let configure = ConfigureConfidentialAccountBuilder::new()
            .payer(trident.payer().pubkey().client())
            .admin(escrow.admin.client())
            .instance(escrow.instance.client())
            .mint(escrow.mint.address.client())
            .allowed_mint(escrow.allowed_mint.client())
            .instance_ata(escrow.escrow_ata.client())
            .decryptable_zero_balance(keys.decryptable_bytes(0))
            .proof_instruction_offset(1)
            .instruction()
            .to_trident();
        let proof = PubkeyValidityProofData::new(&keys.elgamal).unwrap();

        let before = account_bytes(trident, &[escrow.escrow_ata]);
        let outcome = send(
            trident,
            &[
                configure,
                ProofInstruction::VerifyPubkeyValidity.encode_verify_proof(None, &proof),
            ],
            "configure_confidential_account",
        );
        if model.configured || !escrow.configurable {
            assert!(
                !outcome.success,
                "configure landed twice or on an unsupported mint"
            );
            if !escrow.configurable {
                assert!(
                    outcome.refused_by(
                        &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
                        PrivateChannelEscrowProgramError::UnsupportedConfidentialMint as u32,
                    ),
                    "configure on a ConfidentialMintBurn mint: {}",
                    outcome.logs
                );
            }
            assert_eq!(account_bytes(trident, &[escrow.escrow_ata]), before);
            return;
        }
        assert!(outcome.success, "configure failed: {}", outcome.logs);

        model.configured = true;
        assert_eq!(
            public_balance(trident, &escrow.escrow_ata),
            model.escrow_public,
            "configure changed the public balance"
        );
        assert_eq!(
            public_withheld(trident, &escrow.escrow_ata),
            model.escrow_public_withheld,
            "configure changed the withheld fees"
        );
    }

    /// AllowMint again on the allowed, possibly configured, mint. It rewrites the
    /// AllowedMint and reopens both gates, and must leave the escrow account,
    /// confidential extensions included, as it was.
    #[flow]
    fn fuzz_re_allow(&mut self) {
        let escrow = self.escrow.as_ref().unwrap();
        let trident = &mut self.trident;
        let model = &mut self.model;

        let (_, allowed_mint_bump) = Pubkey::find_program_address(
            &[
                b"allowed_mint",
                escrow.instance.as_ref(),
                escrow.mint.address.as_ref(),
            ],
            &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
        );
        let instruction = AllowMintBuilder::new()
            .payer(trident.payer().pubkey().client())
            .admin(escrow.admin.client())
            .instance(escrow.instance.client())
            .mint(escrow.mint.address.client())
            .allowed_mint(escrow.allowed_mint.client())
            .instance_ata(escrow.escrow_ata.client())
            .token_program(TOKEN_2022_ID.client())
            .bump(allowed_mint_bump)
            .withdraw_fee(FUZZ_WITHDRAW_FEE)
            .min_withdraw_amount(FUZZ_MIN_WITHDRAW_AMOUNT)
            .min_deposit_amount(FUZZ_MIN_DEPOSIT_AMOUNT)
            .instruction()
            .to_trident();

        let before = account_bytes(trident, &[escrow.escrow_ata]);
        send_ok(trident, &[instruction], "re_allow");
        assert_eq!(
            account_bytes(trident, &[escrow.escrow_ata]),
            before,
            "re-allowing the mint changed the escrow account"
        );
        model.deposits_blocked = false;
        model.withdrawals_blocked = false;
    }

    // ── Public flows on the same account ──────────────────────────────────────

    #[flow]
    fn fuzz_public_deposit(&mut self) {
        let escrow = self.escrow.as_ref().unwrap();
        let trident = &mut self.trident;
        let model = &mut self.model;

        let recipient = random_wallet(trident);
        let amount = pick_amount_or_over(trident, model.user_public);
        let recipient_wallet = escrow.wallet(recipient);
        let instruction = DepositBuilder::new()
            .payer(trident.payer().pubkey().client())
            .user(escrow.user.client())
            .instance(escrow.instance.client())
            .mint(escrow.mint.address.client())
            .allowed_mint(escrow.allowed_mint.client())
            .user_ata(escrow.user_ata.client())
            .instance_ata(escrow.escrow_ata.client())
            .token_program(TOKEN_2022_ID.client())
            .user_allowed_wallet_pda(escrow.allowed_wallet_pda(&escrow.user).client())
            .recipient_allowed_wallet_pda(escrow.allowed_wallet_pda(&recipient_wallet).client())
            .amount(amount)
            .recipient(recipient_wallet.client())
            .instruction()
            .to_trident();

        let fee = escrow.mint.fee(amount);
        let watched = [escrow.escrow_ata, escrow.user_ata];
        let before = account_bytes(trident, &watched);
        let outcome = send(trident, &[instruction], "public_deposit");
        // A deposit whose whole amount goes to the fee is refused like a zero one.
        if !(model.deposit_allowed(recipient) && amount <= model.user_public && amount > fee) {
            assert!(!outcome.success, "public deposit of {amount} landed");
            assert_eq!(account_bytes(trident, &watched), before);
            return;
        }
        assert!(
            outcome.success,
            "public deposit of {amount} failed: {}",
            outcome.logs
        );

        model.user_public -= amount;
        model.escrow_public += amount - fee;
        model.escrow_public_withheld += fee;
    }

    #[flow]
    fn fuzz_public_release(&mut self) {
        let escrow = self.escrow.as_ref().unwrap();
        let trident = &mut self.trident;
        let model = &mut self.model;

        let withdrawer = random_wallet(trident);
        let nonce = trident.random_from_range(0..NONCE_RANGE);
        let amount = pick_amount_or_over(trident, model.escrow_public);
        let withdrawer_wallet = escrow.wallet(withdrawer);
        let instruction = ReleaseFundsBuilder::new()
            .payer(trident.payer().pubkey().client())
            .operator(escrow.operator.client())
            .instance(escrow.instance.client())
            .withdrawal_bitmap(escrow.withdrawal_bitmap.client())
            .operator_pda(escrow.operator_pda.client())
            .mint(escrow.mint.address.client())
            .allowed_mint(escrow.allowed_mint.client())
            .user_ata(escrow.user_ata.client())
            .instance_ata(escrow.escrow_ata.client())
            .token_program(TOKEN_2022_ID.client())
            .user_allowed_wallet_pda(escrow.allowed_wallet_pda(&escrow.user).client())
            .withdrawer_allowed_wallet_pda(escrow.allowed_wallet_pda(&withdrawer_wallet).client())
            .amount(amount)
            .user(escrow.user.client())
            .transaction_nonce(nonce)
            .withdrawer(withdrawer_wallet.client())
            .instruction()
            .to_trident();

        let watched = [escrow.escrow_ata, escrow.user_ata, escrow.withdrawal_bitmap];
        let before = account_bytes(trident, &watched);
        let outcome = send(trident, &[instruction], "public_release");
        if !(model.withdrawal_allowed(withdrawer)
            && !model.nonces.contains(&nonce)
            && amount <= model.escrow_public)
        {
            assert!(
                !outcome.success,
                "public release of {amount} with nonce {nonce} landed"
            );
            assert_eq!(account_bytes(trident, &watched), before);
            return;
        }
        assert!(
            outcome.success,
            "public release of {amount} with nonce {nonce} failed: {}",
            outcome.logs
        );

        let fee = escrow.mint.fee(amount);
        model.escrow_public -= amount;
        model.user_public += amount - fee;
        model.user_public_withheld += fee;
        model.nonces.insert(nonce);
    }

    /// The user applies their own pending balance, so releases keep flowing back
    /// into what they can deposit.
    #[flow]
    fn fuzz_user_apply(&mut self) {
        let escrow = self.escrow.as_ref().unwrap();
        let trident = &mut self.trident;
        let model = &mut self.model;

        let credits: u64 = confidential_state(trident, &escrow.user_ata)
            .unwrap()
            .pending_balance_credit_counter
            .into();
        let available = model.user_available + model.user_pending;
        send_ok(
            trident,
            &[confidential_instruction::apply_pending_balance(
                &TOKEN_2022_ID,
                &escrow.user_ata,
                credits,
                &escrow.user_keys.decryptable(available),
                &escrow.user,
                &[],
            )
            .unwrap()],
            "user_apply",
        );
        model.user_available = available;
        model.user_pending = 0;
    }

    // ── Gates and whitelist ───────────────────────────────────────────────────

    #[flow]
    fn fuzz_set_gates(&mut self) {
        let escrow = self.escrow.as_ref().unwrap();
        let trident = &mut self.trident;
        let model = &mut self.model;

        let block_deposits = trident.random_bool();
        let block_withdrawals = trident.random_bool();
        let instruction = BlockMintBuilder::new()
            .payer(trident.payer().pubkey().client())
            .admin(escrow.admin.client())
            .instance(escrow.instance.client())
            .mint(escrow.mint.address.client())
            .allowed_mint(escrow.allowed_mint.client())
            .block_deposits(block_deposits)
            .block_withdrawals(block_withdrawals)
            .instruction()
            .to_trident();

        let before = account_bytes(trident, &[escrow.escrow_ata]);
        send_ok(trident, &[instruction], "set_gates");
        assert_eq!(
            account_bytes(trident, &[escrow.escrow_ata]),
            before,
            "setting a gate changed the escrow account"
        );
        model.deposits_blocked = block_deposits;
        model.withdrawals_blocked = block_withdrawals;
    }

    #[flow]
    fn fuzz_set_whitelist(&mut self) {
        let escrow = self.escrow.as_ref().unwrap();
        let trident = &mut self.trident;
        let model = &mut self.model;

        let whitelist_deposits = trident.random_bool();
        let whitelist_withdrawals = trident.random_bool();
        let instruction = SetWhitelistEnabledBuilder::new()
            .payer(trident.payer().pubkey().client())
            .admin(escrow.admin.client())
            .instance(escrow.instance.client())
            .whitelist_deposits(whitelist_deposits)
            .whitelist_withdrawals(whitelist_withdrawals)
            .instruction()
            .to_trident();

        send_ok(trident, &[instruction], "set_whitelist");
        model.deposits_whitelisted = whitelist_deposits;
        model.withdrawals_whitelisted = whitelist_withdrawals;
    }

    /// Creates or rewrites the entry of the user or the other wallet. Blocking both
    /// ways is no entry, so it is refused.
    #[flow]
    fn fuzz_add_or_update_allowed_wallet(&mut self) {
        let escrow = self.escrow.as_ref().unwrap();
        let trident = &mut self.trident;
        let model = &mut self.model;

        let wallet = random_wallet(trident);
        let block_deposits = trident.random_bool();
        let block_withdrawals = trident.random_bool();
        let address = escrow.wallet(wallet);
        let (allowed_wallet_pda, bump) = Pubkey::find_program_address(
            &[
                b"allowed_wallet",
                escrow.instance.as_ref(),
                address.as_ref(),
            ],
            &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
        );
        let instruction = AddOrUpdateAllowedWalletBuilder::new()
            .payer(trident.payer().pubkey().client())
            .admin(escrow.admin.client())
            .instance(escrow.instance.client())
            .wallet(address.client())
            .allowed_wallet_pda(allowed_wallet_pda.client())
            .bump(bump)
            .block_deposits(block_deposits)
            .block_withdrawals(block_withdrawals)
            .instruction()
            .to_trident();

        let outcome = send(trident, &[instruction], "add_or_update_allowed_wallet");
        if block_deposits && block_withdrawals {
            assert!(!outcome.success, "an entry blocked both ways was accepted");
            return;
        }
        assert!(outcome.success, "add or update failed: {}", outcome.logs);
        *model.entry_mut(wallet) = Entry {
            listed: true,
            deposits_blocked: block_deposits,
            withdrawals_blocked: block_withdrawals,
        };
    }

    #[flow]
    fn fuzz_remove_allowed_wallet(&mut self) {
        let escrow = self.escrow.as_ref().unwrap();
        let trident = &mut self.trident;
        let model = &mut self.model;

        let wallet = random_wallet(trident);
        let address = escrow.wallet(wallet);
        let instruction = RemoveAllowedWalletBuilder::new()
            .payer(trident.payer().pubkey().client())
            .admin(escrow.admin.client())
            .instance(escrow.instance.client())
            .wallet(address.client())
            .allowed_wallet_pda(escrow.allowed_wallet_pda(&address).client())
            .instruction()
            .to_trident();

        let outcome = send(trident, &[instruction], "remove_allowed_wallet");
        assert_eq!(
            outcome.success,
            model.entry(wallet).listed,
            "removing an entry must succeed exactly when it exists"
        );
        *model.entry_mut(wallet) = Entry::default();
    }

    // ── Invariants ────────────────────────────────────────────────────────────

    #[end]
    fn end(&mut self) {
        let escrow = self.escrow.as_ref().unwrap();
        let trident = &mut self.trident;
        let model = &self.model;

        assert_eq!(
            public_balance(trident, &escrow.escrow_ata),
            model.escrow_public,
            "escrow public balance"
        );
        assert_eq!(
            public_withheld(trident, &escrow.escrow_ata),
            model.escrow_public_withheld,
            "escrow public withheld fees"
        );
        assert_eq!(
            public_balance(trident, &escrow.user_ata),
            model.user_public,
            "user public balance"
        );
        assert_eq!(
            public_withheld(trident, &escrow.user_ata),
            model.user_public_withheld,
            "user public withheld fees"
        );

        let user_state = confidential_state(trident, &escrow.user_ata).unwrap();
        assert_eq!(
            decrypt_pending(&user_state, &escrow.user_keys),
            model.user_pending,
            "user pending balance"
        );
        assert_eq!(
            decrypt_available(&user_state, &escrow.user_keys),
            model.user_available,
            "user available balance"
        );
        assert_eq!(
            confidential_withheld(trident, &escrow.user_ata, &escrow.mint),
            model.user_confidential_withheld,
            "user confidential withheld fees"
        );

        if model.configured {
            let state = confidential_state(trident, &escrow.escrow_ata).unwrap();
            assert_eq!(
                decrypt_pending(&state, &escrow.escrow_keys),
                model.escrow_pending,
                "escrow pending balance"
            );
            assert_eq!(
                decrypt_available(&state, &escrow.escrow_keys),
                model.escrow_available,
                "escrow available balance"
            );
            assert_eq!(
                u64::from(state.pending_balance_credit_counter),
                model.escrow_credits,
                "escrow pending credits"
            );
            assert_eq!(
                decrypt_aes_copy(&state, &escrow.escrow_keys),
                model.escrow_aes_copy,
                "escrow AES copy"
            );
            assert!(
                !bool::from(state.allow_confidential_credits),
                "confidential credits left on"
            );
            assert!(
                bool::from(state.allow_non_confidential_credits),
                "public credits turned off"
            );
            assert_eq!(
                bytemuck::bytes_of(&state.elgamal_pubkey),
                bytemuck::bytes_of(&PodElGamalPubkey::from(
                    *escrow.escrow_keys.elgamal.pubkey()
                )),
                "the escrow's ElGamal key changed"
            );
            assert_eq!(
                confidential_withheld(trident, &escrow.escrow_ata, &escrow.mint),
                model.escrow_confidential_withheld,
                "escrow confidential withheld fees"
            );
        } else {
            assert!(
                confidential_state(trident, &escrow.escrow_ata).is_none(),
                "the escrow account was configured without configure landing"
            );
        }

        let supply = mint_supply(trident, &escrow.mint.address);
        let held = model.escrow_public
            + model.escrow_public_withheld
            + model.escrow_pending
            + model.escrow_available
            + model.escrow_confidential_withheld
            + model.user_public
            + model.user_public_withheld
            + model.user_pending
            + model.user_available
            + model.user_confidential_withheld;
        assert_eq!(
            supply, held,
            "mint supply differs from every balance and fee"
        );

        let bitmap = trident
            .get_account(&escrow.withdrawal_bitmap)
            .data()
            .to_vec();
        for nonce in 0..NONCE_RANGE {
            let bit = nonce as usize;
            let spent = bitmap[BITMAP_BITS_OFFSET + bit / 8] & (1u8 << (bit % 8)) != 0;
            assert_eq!(
                spent,
                model.nonces.contains(&nonce),
                "bitmap bit of nonce {nonce}"
            );
        }
    }
}

fn main() {
    FuzzTest::fuzz(200, 24);
}
