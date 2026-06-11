//! # Fuzz harness — SMT reset lifecycle
//!
//! Invariants tested:
//! - **Stale nonce rejection**: after a reset, nonces from the previous tree generation must fail.
//! - **Fresh nonce acceptance**: nonces in the new generation with valid proofs must succeed.
//! - **Balance conservation**: resets must never move tokens.
//!
//! Nonces are generation-aware: `nonce = current_tree_index * MAX_TREE_LEAVES + offset`.
//! Invalid-proof rejection is covered by `fuzz_escrow` — this harness focuses on the reset lifecycle.

mod shared;

use private_channel_escrow_program_client::instructions::{
    DepositBuilder, ReleaseFundsBuilder, ResetSmtRootBuilder,
};
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use tests_private_channel_escrow_program::smt_utils::{ProcessorSMT, MAX_TREE_LEAVES};
use trident_fuzz::fuzzing::*;

use shared::{
    clamp_amount, setup_escrow, token_amount, AccountAddresses, PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
};

/// Clamp to a nonce offset within [0, 99].
/// Absolute nonce = `current_tree_index * MAX_TREE_LEAVES + offset`.
fn clamp_nonce_offset(raw: u64) -> u64 {
    raw % 100
}

// ── Fuzz test ─────────────────────────────────────────────────────────────────

#[derive(Default, FuzzTestMethods)]
pub struct FuzzTest {
    pub trident: Trident,
    pub fuzz_accounts: AccountAddresses,
    /// Local mirror of the on-chain SMT. Reset alongside the on-chain root.
    smt: ProcessorSMT,
    /// Mirrors the on-chain tree generation index.
    current_tree_index: u64,
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
        self.smt = ProcessorSMT::new();
        self.current_tree_index = 0;
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

    /// Valid release within the current tree generation. Skipped silently if
    /// preconditions aren't met (nonce already used, or insufficient balance).
    #[flow]
    fn fuzz_release(&mut self) {
        let amount = clamp_amount(self.trident.random_from_range(1..u64::MAX));
        let nonce = self.current_tree_index * MAX_TREE_LEAVES as u64
            + clamp_nonce_offset(self.trident.random_from_range(0..u64::MAX));

        let operator = self.fuzz_accounts.operator.get(&mut self.trident).unwrap();
        let instance = self.fuzz_accounts.instance.get(&mut self.trident).unwrap();
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

        if self.smt.contains(nonce) || amount > instance_bal_before {
            return;
        }

        let (_, proofs) = self.smt.generate_exclusion_proof(nonce);
        let mut next = self.smt.clone();
        next.insert(nonce);
        let new_root = next.current_root();

        let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(1_200_000);
        let ix = ReleaseFundsBuilder::new()
            .payer(self.trident.payer().pubkey())
            .operator(operator)
            .instance(instance)
            .operator_pda(operator_pda)
            .mint(mint)
            .allowed_mint(allowed_mint)
            .user_ata(user_ata)
            .instance_ata(instance_ata)
            .amount(amount)
            .user(user)
            .new_withdrawal_root(new_root)
            .transaction_nonce(nonce)
            .sibling_proofs(proofs)
            .instruction();

        let res = self
            .trident
            .process_transaction(&[cu_ix, ix], Some("release"));
        assert!(
            res.is_success(),
            "valid release failed tree={} nonce={} amount={}: {}",
            self.current_tree_index,
            nonce,
            amount,
            res.logs()
        );

        self.smt.insert(nonce);
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

    /// Reset the on-chain SMT root and advance the tree generation index.
    /// Balances must not be affected.
    #[flow]
    fn fuzz_reset_smt_root(&mut self) {
        let operator = self.fuzz_accounts.operator.get(&mut self.trident).unwrap();
        let instance = self.fuzz_accounts.instance.get(&mut self.trident).unwrap();
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

        let ix = ResetSmtRootBuilder::new()
            .payer(self.trident.payer().pubkey())
            .operator(operator)
            .instance(instance)
            .operator_pda(operator_pda)
            .event_authority(event_authority)
            .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
            .expected_current_tree_index(self.current_tree_index)
            .instruction();

        let res = self
            .trident
            .process_transaction(&[ix], Some("reset_smt_root"));
        assert!(res.is_success(), "ResetSmtRoot failed: {}", res.logs());

        self.current_tree_index += 1;
        self.smt = ProcessorSMT::new();

        assert_eq!(
            token_amount(&mut self.trident, &instance_ata),
            instance_bal_before,
            "instance balance changed on reset"
        );
        assert_eq!(
            token_amount(&mut self.trident, &user_ata),
            user_bal_before,
            "user balance changed on reset"
        );
    }

    /// Attempt a release with a nonce from the previous tree generation — must be rejected.
    /// Skipped if no reset has occurred yet.
    #[flow]
    fn fuzz_stale_nonce(&mut self) {
        if self.current_tree_index == 0 {
            return;
        }

        let stale_nonce = (self.current_tree_index - 1) * MAX_TREE_LEAVES as u64
            + clamp_nonce_offset(self.trident.random_from_range(0..u64::MAX));

        let operator = self.fuzz_accounts.operator.get(&mut self.trident).unwrap();
        let instance = self.fuzz_accounts.instance.get(&mut self.trident).unwrap();
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

        // Use a valid exclusion proof against the current (freshly reset) SMT so
        // the transaction is rejected specifically by the stale-nonce/generation
        // check, not by proof verification.
        let (_, proofs) = self.smt.generate_exclusion_proof(stale_nonce);
        let mut next = self.smt.clone();
        next.insert(stale_nonce);
        let new_root = next.current_root();

        let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(1_200_000);
        let ix = ReleaseFundsBuilder::new()
            .payer(self.trident.payer().pubkey())
            .operator(operator)
            .instance(instance)
            .operator_pda(operator_pda)
            .mint(mint)
            .allowed_mint(allowed_mint)
            .user_ata(user_ata)
            .instance_ata(instance_ata)
            .amount(1)
            .user(user)
            .new_withdrawal_root(new_root)
            .transaction_nonce(stale_nonce)
            .sibling_proofs(proofs)
            .instruction();

        let res = self
            .trident
            .process_transaction(&[cu_ix, ix], Some("stale_nonce"));
        assert!(
            !res.is_success(),
            "stale nonce must be rejected: prev_tree={} nonce={} current_tree={}",
            self.current_tree_index - 1,
            stale_nonce,
            self.current_tree_index,
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
            "final escrow balance mismatch: deposited={} released={} resets={}",
            self.total_deposited,
            self.total_released,
            self.current_tree_index,
        );

        let expected_user = self
            .initial_user_balance
            .checked_sub(self.total_deposited)
            .and_then(|x| x.checked_add(self.total_released))
            .expect("user balance model overflow");
        assert_eq!(
            token_amount(&mut self.trident, &user_ata),
            expected_user,
            "final user balance mismatch: initial={} deposited={} released={} resets={}",
            self.initial_user_balance,
            self.total_deposited,
            self.total_released,
            self.current_tree_index,
        );
    }
}

fn main() {
    FuzzTest::fuzz(1000, 32);
}
