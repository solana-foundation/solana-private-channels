//! # Fuzz harness — escrow core lifecycle
//!
//! Invariants tested:
//! - **Balance conservation**: `escrow_balance == total_deposited - total_released`
//! - **Foreign-bitmap rejection**: a bitmap that is not this instance's PDA must
//!   fail without touching balances.
//! - **Double-spend prevention**: replaying a successful release must be rejected.
//! - **Gate independence**: the deposit and withdrawal gates are set as absolute
//!   values in any combination, and each governs only its own instruction. A mint
//!   with deposits blocked must still release, which is the property that keeps
//!   blocking a mint from stranding the balances already in it.
//! - **Gates never move tokens**: neither setting a gate nor re-allowing a mint
//!   may change an escrow balance.

mod shared;

use std::collections::HashMap;

use private_channel_escrow_program_client::instructions::{
    AllowMintBuilder, BlockMintBuilder, DepositBuilder, ReleaseFundsBuilder,
};
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::pubkey::Pubkey;
use trident_fuzz::fuzzing::*;

use shared::{
    clamp_amount, setup_escrow, token_amount, AccountAddresses,
    PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
};

/// Nonces covered by one bitmap generation. Must match the on-chain constant.
const NONCES_PER_GENERATION: u64 = 65_536;

/// Clamp nonces to generation 0, spanning the whole bitmap so the byte index
/// arithmetic is exercised beyond the first few bytes.
fn clamp_nonce(raw: u64) -> u64 {
    raw % NONCES_PER_GENERATION
}

// ── State ─────────────────────────────────────────────────────────────────────

/// Everything needed to replay a previously successful release.
#[derive(Clone)]
struct SuccessfulRelease {
    amount: u64,
}

// ── Fuzz test ─────────────────────────────────────────────────────────────────

#[derive(Default, FuzzTestMethods)]
pub struct FuzzTest {
    pub trident: Trident,
    pub fuzz_accounts: AccountAddresses,
    /// Successful releases keyed by nonce. Doubles as the mirror of the
    /// on-chain bitmap bits: a key here means that nonce is consumed.
    successful_releases: HashMap<u64, SuccessfulRelease>,
    /// User's token balance at the start of the iteration (after minting).
    initial_user_balance: u64,
    total_deposited: u64,
    total_released: u64,
    /// Mirror of the mint's two on-chain gates. The gates are independent, so the
    /// model tracks them separately and every flow predicts its outcome from the
    /// one that governs it.
    deposits_blocked: bool,
    withdrawals_blocked: bool,
}

#[flow_executor]
impl FuzzTest {
    fn new() -> Self {
        Self::default()
    }

    #[init]
    fn start(&mut self) {
        self.initial_user_balance = setup_escrow(&mut self.trident, &mut self.fuzz_accounts);
        self.successful_releases.clear();
        self.total_deposited = 0;
        self.total_released = 0;
        // AllowMint leaves both gates open.
        self.deposits_blocked = false;
        self.withdrawals_blocked = false;
    }

    // ── Flows ─────────────────────────────────────────────────────────────────

    /// Deposit a random amount. Asserts exact ATA balance movement on success.
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

        if self.deposits_blocked {
            assert!(
                !res.is_success(),
                "deposit landed while the mint's deposit gate was closed"
            );
            assert_eq!(
                token_amount(&mut self.trident, &instance_ata),
                instance_bal_before,
                "instance balance changed on a blocked deposit"
            );
            assert_eq!(
                token_amount(&mut self.trident, &user_ata),
                user_bal_before,
                "user balance changed on a blocked deposit"
            );
            return;
        }

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

    /// 50% valid release / 50% release against a foreign bitmap.
    ///
    /// Valid path: this instance's bitmap — must succeed, balances must shift.
    /// Invalid path: a random address in the bitmap slot — must fail, balances
    /// must be unchanged and the nonce must stay unconsumed.
    #[flow]
    fn fuzz_release(&mut self) {
        let amount = clamp_amount(self.trident.random_from_range(1..u64::MAX));
        let nonce = clamp_nonce(self.trident.random_from_range(0..u64::MAX));
        let use_valid = self.trident.random_from_range(0..=1u8) == 0;

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

        // Which bitmap is the fuzzed dimension; the expected outcome is derived
        // separately. Tying the two together would send every release the gate
        // must refuse against a foreign bitmap, so it would fail for that reason
        // instead and the gate would never be exercised.
        let bitmap_account = if use_valid {
            withdrawal_bitmap
        } else {
            Pubkey::new_unique()
        };
        let should_succeed = use_valid
            && !self.withdrawals_blocked
            && !self.successful_releases.contains_key(&nonce)
            && amount <= instance_bal_before;

        let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(1_200_000);
        let ix = ReleaseFundsBuilder::new()
            .payer(self.trident.payer().pubkey())
            .operator(operator)
            .instance(instance)
            .withdrawal_bitmap(bitmap_account)
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

        if should_succeed {
            assert!(
                res.is_success(),
                "valid release failed nonce={nonce} amount={amount}: {}",
                res.logs()
            );
            self.successful_releases
                .insert(nonce, SuccessfulRelease { amount });
            assert_eq!(
                token_amount(&mut self.trident, &instance_ata),
                instance_bal_before - amount
            );
            assert_eq!(
                token_amount(&mut self.trident, &user_ata),
                user_bal_before + amount
            );
            self.total_released = self.total_released.checked_add(amount).unwrap();
        } else {
            assert!(
                !res.is_success(),
                "invalid release should fail nonce={nonce}"
            );
            assert_eq!(
                token_amount(&mut self.trident, &instance_ata),
                instance_bal_before,
                "instance balance changed on failed release"
            );
            assert_eq!(
                token_amount(&mut self.trident, &user_ata),
                user_bal_before,
                "user balance changed on failed release"
            );
        }
    }

    /// Set both gates to a random combination. Both flags are absolute, so this
    /// covers closing either, closing both, and re-opening either.
    ///
    /// The admin can always set the gates, whatever they already are, so this must
    /// succeed every time. The balances are untouched: setting a gate never moves
    /// tokens, which is what makes blocking deposits safe for existing balances.
    #[flow]
    fn fuzz_set_gates(&mut self) {
        let block_deposits = self.trident.random_from_range(0..=1u8) == 0;
        let block_withdrawals = self.trident.random_from_range(0..=1u8) == 0;

        let admin = self.fuzz_accounts.admin.get(&mut self.trident).unwrap();
        let instance = self.fuzz_accounts.instance.get(&mut self.trident).unwrap();
        let mint = self.fuzz_accounts.mint.get(&mut self.trident).unwrap();
        let allowed_mint = self
            .fuzz_accounts
            .allowed_mint
            .get(&mut self.trident)
            .unwrap();
        let instance_ata = self
            .fuzz_accounts
            .instance_ata
            .get(&mut self.trident)
            .unwrap();

        let instance_bal_before = token_amount(&mut self.trident, &instance_ata);

        let ix = BlockMintBuilder::new()
            .payer(self.trident.payer().pubkey())
            .admin(admin)
            .instance(instance)
            .mint(mint)
            .allowed_mint(allowed_mint)
            .block_deposits(block_deposits)
            .block_withdrawals(block_withdrawals)
            .instruction();

        let res = self
            .trident
            .process_transaction(&[ix], Some("set_gates"));
        assert!(
            res.is_success(),
            "admin must always be able to set the gates: {}",
            res.logs()
        );
        assert_eq!(
            token_amount(&mut self.trident, &instance_ata),
            instance_bal_before,
            "setting a gate moved escrowed tokens"
        );

        self.deposits_blocked = block_deposits;
        self.withdrawals_blocked = block_withdrawals;
    }

    /// Re-allow the mint. `AllowMint` doubles as the re-allow path: the PDA
    /// already exists, so it is rewritten rather than created, and both gates
    /// re-open. Escrowed balances must survive it untouched.
    #[flow]
    fn fuzz_re_allow(&mut self) {
        let admin = self.fuzz_accounts.admin.get(&mut self.trident).unwrap();
        let instance = self.fuzz_accounts.instance.get(&mut self.trident).unwrap();
        let mint = self.fuzz_accounts.mint.get(&mut self.trident).unwrap();
        let allowed_mint = self
            .fuzz_accounts
            .allowed_mint
            .get(&mut self.trident)
            .unwrap();
        let instance_ata = self
            .fuzz_accounts
            .instance_ata
            .get(&mut self.trident)
            .unwrap();

        let (_, allowed_mint_bump) = Pubkey::find_program_address(
            &[b"allowed_mint", instance.as_ref(), mint.as_ref()],
            &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
        );

        let instance_bal_before = token_amount(&mut self.trident, &instance_ata);

        let ix = AllowMintBuilder::new()
            .payer(self.trident.payer().pubkey())
            .admin(admin)
            .instance(instance)
            .mint(mint)
            .allowed_mint(allowed_mint)
            .instance_ata(instance_ata)
            .bump(allowed_mint_bump)
            .instruction();

        let res = self.trident.process_transaction(&[ix], Some("re_allow"));
        assert!(
            res.is_success(),
            "re-allowing an existing mint must succeed: {}",
            res.logs()
        );
        assert_eq!(
            token_amount(&mut self.trident, &instance_ata),
            instance_bal_before,
            "re-allow moved escrowed tokens"
        );

        self.deposits_blocked = false;
        self.withdrawals_blocked = false;
    }

    /// Replay an already-processed release verbatim — must be rejected.
    ///
    /// Picks a nonce known to be consumed rather than guessing one: a random
    /// nonce almost never lands on the handful actually released, which would
    /// make this flow a no-op.
    #[flow]
    fn fuzz_double_spend(&mut self) {
        let Some((&nonce, prev)) = self.successful_releases.iter().next() else {
            return;
        };
        let prev = prev.clone();

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
            .amount(prev.amount)
            .user(user)
            .transaction_nonce(nonce)
            .instruction();

        let res = self
            .trident
            .process_transaction(&[cu_ix, ix], Some("double_spend"));
        assert!(
            !res.is_success(),
            "double-spend must be rejected: nonce={nonce}"
        );
        assert_eq!(
            token_amount(&mut self.trident, &instance_ata),
            instance_bal_before,
            "instance balance changed on double-spend"
        );
        assert_eq!(
            token_amount(&mut self.trident, &user_ata),
            user_bal_before,
            "user balance changed on double-spend"
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
            "final escrow balance mismatch: deposited={} released={}",
            self.total_deposited,
            self.total_released,
        );

        let expected_user = self
            .initial_user_balance
            .checked_sub(self.total_deposited)
            .and_then(|x| x.checked_add(self.total_released))
            .expect("user balance model overflow");
        assert_eq!(
            token_amount(&mut self.trident, &user_ata),
            expected_user,
            "final user balance mismatch: initial={} deposited={} released={}",
            self.initial_user_balance,
            self.total_deposited,
            self.total_released,
        );
    }
}

fn main() {
    FuzzTest::fuzz(1000, 32);
}
