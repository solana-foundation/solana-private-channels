//! # Fuzz harness — bitmap rotation lifecycle
//!
//! Invariants tested:
//! - **Stale nonce rejection**: after a rotation, nonces from the previous generation must fail.
//! - **Replay rejection**: a nonce already consumed in this generation must fail.
//! - **Fresh nonce acceptance**: unconsumed nonces in the current generation must succeed.
//! - **Balance conservation**: rotations must never move tokens.
//!
//! Nonces are generation-aware: `nonce = current_generation * NONCES_PER_GENERATION + offset`.

mod shared;

use private_channel_escrow_program_client::instructions::{
    DepositBuilder, ReleaseFundsBuilder, RotateBitmapBuilder,
};
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use std::collections::HashSet;
use trident_fuzz::fuzzing::*;

use shared::{
    clamp_amount, setup_escrow, token_amount, AccountAddresses, PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
};

/// Nonces covered by one bitmap generation. Must match the on-chain constant.
const NONCES_PER_GENERATION: u64 = 65_536;

/// Clamp to a nonce offset within the generation, spanning the whole bitmap so
/// the byte index arithmetic is exercised beyond the first few bytes.
/// Absolute nonce = `current_generation * NONCES_PER_GENERATION + offset`.
///
/// Reuse does not rely on random collisions: `fuzz_replay_nonce` picks a nonce
/// known to be consumed, and `fuzz_stale_nonce` builds one deliberately.
fn clamp_nonce_offset(raw: u64) -> u64 {
    raw % NONCES_PER_GENERATION
}

// ── Fuzz test ─────────────────────────────────────────────────────────────────

#[derive(Default, FuzzTestMethods)]
pub struct FuzzTest {
    pub trident: Trident,
    pub fuzz_accounts: AccountAddresses,
    /// Nonces consumed in the current generation. Cleared on rotation, mirroring
    /// the on-chain bits.
    consumed_nonces: HashSet<u64>,
    /// Mirrors the on-chain bitmap generation.
    current_generation: u64,
    /// User's token balance at the start of the iteration (after minting).
    initial_user_balance: u64,
    total_deposited: u64,
    total_released: u64,
}

#[flow_executor]
impl FuzzTest {
    fn new() -> Self {
        Self::default()
    }

    #[init]
    fn start(&mut self) {
        self.initial_user_balance = setup_escrow(&mut self.trident, &mut self.fuzz_accounts);
        self.consumed_nonces = HashSet::new();
        self.current_generation = 0;
        self.total_deposited = 0;
        self.total_released = 0;
    }

    // ── Flows ─────────────────────────────────────────────────────────────────

    /// Deposit a random amount. Accumulates `total_deposited` on success.
    #[flow]
    fn fuzz_deposit(&mut self) {
        let amount = clamp_amount(self.trident.random_from_range(1..u64::MAX));

        let user = self.fuzz_accounts.user.get(&mut self.trident).unwrap();
        let instance = self.fuzz_accounts.instance.get(&mut self.trident).unwrap();
        let mint = self.fuzz_accounts.mint.get(&mut self.trident).unwrap();
        let allowed_mint = self
            .fuzz_accounts
            .allowed_mint
            .get(&mut self.trident)
            .unwrap();
        let user_ata = self.fuzz_accounts.user_ata.get(&mut self.trident).unwrap();
        let instance_ata = self
            .fuzz_accounts
            .instance_ata
            .get(&mut self.trident)
            .unwrap();

        let instance_bal_before = token_amount(&mut self.trident, &instance_ata);
        let user_bal_before = token_amount(&mut self.trident, &user_ata);

        let ix = DepositBuilder::new()
            .payer(self.trident.payer().pubkey())
            .user(user)
            .instance(instance)
            .mint(mint)
            .allowed_mint(allowed_mint)
            .user_ata(user_ata)
            .instance_ata(instance_ata)
            .amount(amount)
            .instruction();

        let res = self.trident.process_transaction(&[ix], Some("deposit"));
        if res.is_success() {
            assert_eq!(
                token_amount(&mut self.trident, &instance_ata),
                instance_bal_before + amount
            );
            assert_eq!(
                token_amount(&mut self.trident, &user_ata),
                user_bal_before - amount
            );
            self.total_deposited = self.total_deposited.checked_add(amount).unwrap();
        }
    }

    /// Valid release within the current generation. Skipped silently if
    /// preconditions aren't met (nonce already used, or insufficient balance).
    #[flow]
    fn fuzz_release(&mut self) {
        let amount = clamp_amount(self.trident.random_from_range(1..u64::MAX));
        let nonce = self.current_generation * NONCES_PER_GENERATION
            + clamp_nonce_offset(self.trident.random_from_range(0..u64::MAX));

        let operator = self.fuzz_accounts.operator.get(&mut self.trident).unwrap();
        let instance = self.fuzz_accounts.instance.get(&mut self.trident).unwrap();
        let withdrawal_bitmap = self
            .fuzz_accounts
            .withdrawal_bitmap
            .get(&mut self.trident)
            .unwrap();
        let operator_pda = self
            .fuzz_accounts
            .operator_pda
            .get(&mut self.trident)
            .unwrap();
        let mint = self.fuzz_accounts.mint.get(&mut self.trident).unwrap();
        let allowed_mint = self
            .fuzz_accounts
            .allowed_mint
            .get(&mut self.trident)
            .unwrap();
        let user = self.fuzz_accounts.user.get(&mut self.trident).unwrap();
        let user_ata = self.fuzz_accounts.user_ata.get(&mut self.trident).unwrap();
        let instance_ata = self
            .fuzz_accounts
            .instance_ata
            .get(&mut self.trident)
            .unwrap();

        let instance_bal_before = token_amount(&mut self.trident, &instance_ata);
        let user_bal_before = token_amount(&mut self.trident, &user_ata);

        if self.consumed_nonces.contains(&nonce) || amount > instance_bal_before {
            return;
        }

        let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(1_200_000);
        let ix = ReleaseFundsBuilder::new()
            .payer(self.trident.payer().pubkey())
            .operator(operator)
            .instance(instance)
            .withdrawal_bitmap(withdrawal_bitmap)
            .operator_pda(operator_pda)
            .mint(mint)
            .allowed_mint(allowed_mint)
            .user_ata(user_ata)
            .instance_ata(instance_ata)
            .amount(amount)
            .user(user)
            .transaction_nonce(nonce)
            .instruction();

        let res = self
            .trident
            .process_transaction(&[cu_ix, ix], Some("release"));
        assert!(
            res.is_success(),
            "valid release failed generation={} nonce={} amount={}: {}",
            self.current_generation,
            nonce,
            amount,
            res.logs()
        );

        self.consumed_nonces.insert(nonce);
        assert_eq!(
            token_amount(&mut self.trident, &instance_ata),
            instance_bal_before - amount
        );
        assert_eq!(
            token_amount(&mut self.trident, &user_ata),
            user_bal_before + amount
        );
        self.total_released = self.total_released.checked_add(amount).unwrap();
    }

    /// Replay a nonce already consumed in this generation — must be rejected.
    /// This is the property the bitmap exists to enforce.
    #[flow]
    fn fuzz_replay_nonce(&mut self) {
        let Some(&nonce) = self.consumed_nonces.iter().next() else {
            return;
        };

        let operator = self.fuzz_accounts.operator.get(&mut self.trident).unwrap();
        let instance = self.fuzz_accounts.instance.get(&mut self.trident).unwrap();
        let withdrawal_bitmap = self
            .fuzz_accounts
            .withdrawal_bitmap
            .get(&mut self.trident)
            .unwrap();
        let operator_pda = self
            .fuzz_accounts
            .operator_pda
            .get(&mut self.trident)
            .unwrap();
        let mint = self.fuzz_accounts.mint.get(&mut self.trident).unwrap();
        let allowed_mint = self
            .fuzz_accounts
            .allowed_mint
            .get(&mut self.trident)
            .unwrap();
        let user = self.fuzz_accounts.user.get(&mut self.trident).unwrap();
        let user_ata = self.fuzz_accounts.user_ata.get(&mut self.trident).unwrap();
        let instance_ata = self
            .fuzz_accounts
            .instance_ata
            .get(&mut self.trident)
            .unwrap();

        let instance_bal_before = token_amount(&mut self.trident, &instance_ata);
        let user_bal_before = token_amount(&mut self.trident, &user_ata);

        let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(1_200_000);
        let ix = ReleaseFundsBuilder::new()
            .payer(self.trident.payer().pubkey())
            .operator(operator)
            .instance(instance)
            .withdrawal_bitmap(withdrawal_bitmap)
            .operator_pda(operator_pda)
            .mint(mint)
            .allowed_mint(allowed_mint)
            .user_ata(user_ata)
            .instance_ata(instance_ata)
            .amount(1)
            .user(user)
            .transaction_nonce(nonce)
            .instruction();

        let res = self
            .trident
            .process_transaction(&[cu_ix, ix], Some("replay_nonce"));
        assert!(
            !res.is_success(),
            "consumed nonce must be rejected: generation={} nonce={}",
            self.current_generation,
            nonce,
        );
        assert_eq!(
            token_amount(&mut self.trident, &instance_ata),
            instance_bal_before,
            "instance balance changed on replay rejection",
        );
        assert_eq!(
            token_amount(&mut self.trident, &user_ata),
            user_bal_before,
            "user balance changed on replay rejection",
        );
    }

    /// Rotate the on-chain bitmap and advance the generation.
    /// Balances must not be affected.
    #[flow]
    fn fuzz_rotate_bitmap(&mut self) {
        let operator = self.fuzz_accounts.operator.get(&mut self.trident).unwrap();
        let instance = self.fuzz_accounts.instance.get(&mut self.trident).unwrap();
        let withdrawal_bitmap = self
            .fuzz_accounts
            .withdrawal_bitmap
            .get(&mut self.trident)
            .unwrap();
        let operator_pda = self
            .fuzz_accounts
            .operator_pda
            .get(&mut self.trident)
            .unwrap();
        let event_authority = self
            .fuzz_accounts
            .event_authority
            .get(&mut self.trident)
            .unwrap();
        let instance_ata = self
            .fuzz_accounts
            .instance_ata
            .get(&mut self.trident)
            .unwrap();
        let user_ata = self.fuzz_accounts.user_ata.get(&mut self.trident).unwrap();
        let instance_bal_before = token_amount(&mut self.trident, &instance_ata);
        let user_bal_before = token_amount(&mut self.trident, &user_ata);

        let ix = RotateBitmapBuilder::new()
            .payer(self.trident.payer().pubkey())
            .operator(operator)
            .instance(instance)
            .withdrawal_bitmap(withdrawal_bitmap)
            .operator_pda(operator_pda)
            .event_authority(event_authority)
            .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
            .expected_generation(self.current_generation)
            .instruction();

        let res = self.trident.process_transaction(&[ix], Some("rotate"));
        assert!(res.is_success(), "RotateBitmap failed: {}", res.logs());

        self.current_generation += 1;
        self.consumed_nonces.clear();

        assert_eq!(
            token_amount(&mut self.trident, &instance_ata),
            instance_bal_before,
            "instance balance changed on rotation"
        );
        assert_eq!(
            token_amount(&mut self.trident, &user_ata),
            user_bal_before,
            "user balance changed on rotation"
        );
    }

    /// Attempt a release with a nonce from the previous generation — must be rejected.
    /// Rotation clears the bits, so only the generation check keeps that range closed.
    /// Skipped if no rotation has occurred yet.
    #[flow]
    fn fuzz_stale_nonce(&mut self) {
        if self.current_generation == 0 {
            return;
        }

        let stale_nonce = (self.current_generation - 1) * NONCES_PER_GENERATION
            + clamp_nonce_offset(self.trident.random_from_range(0..u64::MAX));

        let operator = self.fuzz_accounts.operator.get(&mut self.trident).unwrap();
        let instance = self.fuzz_accounts.instance.get(&mut self.trident).unwrap();
        let withdrawal_bitmap = self
            .fuzz_accounts
            .withdrawal_bitmap
            .get(&mut self.trident)
            .unwrap();
        let operator_pda = self
            .fuzz_accounts
            .operator_pda
            .get(&mut self.trident)
            .unwrap();
        let mint = self.fuzz_accounts.mint.get(&mut self.trident).unwrap();
        let allowed_mint = self
            .fuzz_accounts
            .allowed_mint
            .get(&mut self.trident)
            .unwrap();
        let user = self.fuzz_accounts.user.get(&mut self.trident).unwrap();
        let user_ata = self.fuzz_accounts.user_ata.get(&mut self.trident).unwrap();
        let instance_ata = self
            .fuzz_accounts
            .instance_ata
            .get(&mut self.trident)
            .unwrap();
        let instance_bal_before = token_amount(&mut self.trident, &instance_ata);
        let user_bal_before = token_amount(&mut self.trident, &user_ata);

        let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(1_200_000);
        let ix = ReleaseFundsBuilder::new()
            .payer(self.trident.payer().pubkey())
            .operator(operator)
            .instance(instance)
            .withdrawal_bitmap(withdrawal_bitmap)
            .operator_pda(operator_pda)
            .mint(mint)
            .allowed_mint(allowed_mint)
            .user_ata(user_ata)
            .instance_ata(instance_ata)
            .amount(1)
            .user(user)
            .transaction_nonce(stale_nonce)
            .instruction();

        let res = self
            .trident
            .process_transaction(&[cu_ix, ix], Some("stale_nonce"));
        assert!(
            !res.is_success(),
            "stale nonce must be rejected: prev_generation={} nonce={} current_generation={}",
            self.current_generation - 1,
            stale_nonce,
            self.current_generation,
        );
        assert_eq!(
            token_amount(&mut self.trident, &instance_ata),
            instance_bal_before,
            "instance balance changed on stale nonce rejection",
        );
        assert_eq!(
            token_amount(&mut self.trident, &user_ata),
            user_bal_before,
            "user balance changed on stale nonce rejection",
        );
    }

    // ── Invariant ─────────────────────────────────────────────────────────────

    /// `escrow_balance == total_deposited - total_released`
    /// `user_balance == initial_user_balance - total_deposited + total_released`
    #[end]
    fn end(&mut self) {
        let instance_ata = self
            .fuzz_accounts
            .instance_ata
            .get(&mut self.trident)
            .unwrap();
        let user_ata = self.fuzz_accounts.user_ata.get(&mut self.trident).unwrap();

        let expected_instance = self
            .total_deposited
            .checked_sub(self.total_released)
            .expect("released more than deposited");
        assert_eq!(
            token_amount(&mut self.trident, &instance_ata),
            expected_instance,
            "final escrow balance mismatch: deposited={} released={} rotations={}",
            self.total_deposited,
            self.total_released,
            self.current_generation,
        );

        let expected_user = self
            .initial_user_balance
            .checked_sub(self.total_deposited)
            .and_then(|x| x.checked_add(self.total_released))
            .expect("user balance model overflow");
        assert_eq!(
            token_amount(&mut self.trident, &user_ata),
            expected_user,
            "final user balance mismatch: initial={} deposited={} released={} rotations={}",
            self.initial_user_balance,
            self.total_deposited,
            self.total_released,
            self.current_generation,
        );
    }
}

fn main() {
    FuzzTest::fuzz(1000, 32);
}
