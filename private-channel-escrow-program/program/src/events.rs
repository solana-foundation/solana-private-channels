extern crate alloc;

use alloc::vec::Vec;
use codama::CodamaType;
use pinocchio::Address as Pubkey;

use crate::constants::EVENT_IX_TAG_LE;
use crate::processor::shared::confidential_utils::SplitCiphertext;

#[repr(u8)]
pub enum EventDiscriminators {
    CreateInstance = 0,
    AllowMint = 1,
    BlockMint = 2,
    AddOperator = 3,
    RemoveOperator = 4,
    SetNewAdmin = 5,
    Deposit = 6,
    ReleaseFunds = 7,
    RotateBitmap = 8,
    AddOrUpdateAllowedWallet = 9,
    RemoveAllowedWallet = 10,
    SetWhitelistEnabled = 11,
    ConfigureConfidentialAccount = 12,
    ConfidentialDeposit = 13,
    ConfidentialReleaseFunds = 14,
    ApplyPendingBalance = 15,
    MoveToConfidential = 16,
    MoveToPublic = 17,
}

#[derive(CodamaType)]
pub struct CreateInstanceEvent {
    /// Unique u8 byte for event type.
    pub event_discriminator: u8,
    /// Instance seed pubkey
    pub instance_seed: Pubkey,
    /// Admin pubkey
    pub admin: Pubkey,
}

impl CreateInstanceEvent {
    pub fn new(instance_seed: Pubkey, admin: Pubkey) -> Self {
        Self {
            event_discriminator: EventDiscriminators::CreateInstance as u8,
            instance_seed,
            admin,
        }
    }

    // 8 (tag) + 1 (discriminator) + 32 (instance_seed) + 32 (admin)
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut data = Vec::with_capacity(73);
        data.extend_from_slice(EVENT_IX_TAG_LE);
        data.push(self.event_discriminator);
        data.extend_from_slice(self.instance_seed.as_ref());
        data.extend_from_slice(self.admin.as_ref());
        data
    }
}

#[derive(CodamaType)]
pub struct AllowMintEvent {
    /// Unique u8 byte for event type.
    pub event_discriminator: u8,
    /// Instance seed pubkey
    pub instance_seed: Pubkey,
    /// Mint pubkey that was allowed
    pub mint: Pubkey,
    /// Decimals of the mint (required to parse data on PrivateChannel)
    pub decimals: u8,
    /// Per-withdrawal fee this AllowMint set on the channel, 0 allowed
    pub withdraw_fee: u64,
    /// Smallest channel withdrawal amount this AllowMint set, 0 for none
    pub min_withdraw_amount: u64,
    /// Smallest amount a deposit must land in the escrow, 0 for none
    pub min_deposit_amount: u64,
}

impl AllowMintEvent {
    pub fn new(
        instance_seed: Pubkey,
        mint: Pubkey,
        decimals: u8,
        withdraw_fee: u64,
        min_withdraw_amount: u64,
        min_deposit_amount: u64,
    ) -> Self {
        Self {
            event_discriminator: EventDiscriminators::AllowMint as u8,
            instance_seed,
            mint,
            decimals,
            withdraw_fee,
            min_withdraw_amount,
            min_deposit_amount,
        }
    }

    // 8 (tag) + 1 (discriminator) + 32 (instance_seed) + 32 (mint) + 1 (decimals)
    // + 8 (withdraw_fee) + 8 (min_withdraw_amount) + 8 (min_deposit_amount)
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut data = Vec::with_capacity(98);
        data.extend_from_slice(EVENT_IX_TAG_LE);
        data.push(self.event_discriminator);
        data.extend_from_slice(self.instance_seed.as_ref());
        data.extend_from_slice(self.mint.as_ref());
        data.push(self.decimals);
        data.extend_from_slice(&self.withdraw_fee.to_le_bytes());
        data.extend_from_slice(&self.min_withdraw_amount.to_le_bytes());
        data.extend_from_slice(&self.min_deposit_amount.to_le_bytes());
        data
    }
}

#[derive(CodamaType)]
pub struct BlockMintEvent {
    /// Unique u8 byte for event type.
    pub event_discriminator: u8,
    /// Instance seed pubkey
    pub instance_seed: Pubkey,
    /// Mint pubkey whose gates were set
    pub mint: Pubkey,
    /// Deposit gate after this instruction
    pub deposits_blocked: bool,
    /// Withdrawal gate after this instruction
    pub withdrawals_blocked: bool,
}

impl BlockMintEvent {
    pub fn new(
        instance_seed: Pubkey,
        mint: Pubkey,
        deposits_blocked: bool,
        withdrawals_blocked: bool,
    ) -> Self {
        Self {
            event_discriminator: EventDiscriminators::BlockMint as u8,
            instance_seed,
            mint,
            deposits_blocked,
            withdrawals_blocked,
        }
    }

    // 8 (tag) + 1 (discriminator) + 32 (instance_seed) + 32 (mint) + 1 + 1 (gates)
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut data = Vec::with_capacity(75);
        data.extend_from_slice(EVENT_IX_TAG_LE);
        data.push(self.event_discriminator);
        data.extend_from_slice(self.instance_seed.as_ref());
        data.extend_from_slice(self.mint.as_ref());
        data.push(self.deposits_blocked as u8);
        data.push(self.withdrawals_blocked as u8);
        data
    }
}

#[derive(CodamaType)]
pub struct AddOperatorEvent {
    /// Unique u8 byte for event type.
    pub event_discriminator: u8,
    /// Instance seed pubkey
    pub instance_seed: Pubkey,
    /// Operator pubkey that was granted operator access
    pub operator: Pubkey,
}

impl AddOperatorEvent {
    pub fn new(instance_seed: Pubkey, operator: Pubkey) -> Self {
        Self {
            event_discriminator: EventDiscriminators::AddOperator as u8,
            instance_seed,
            operator,
        }
    }

    // 8 (tag) + 1 (discriminator) + 32 (instance_seed) + 32 (operator)
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut data = Vec::with_capacity(73);
        data.extend_from_slice(EVENT_IX_TAG_LE);
        data.push(self.event_discriminator);
        data.extend_from_slice(self.instance_seed.as_ref());
        data.extend_from_slice(self.operator.as_ref());
        data
    }
}

#[derive(CodamaType)]
pub struct RemoveOperatorEvent {
    /// Unique u8 byte for event type.
    pub event_discriminator: u8,
    /// Instance seed pubkey
    pub instance_seed: Pubkey,
    /// Operator pubkey that had operator access removed
    pub operator: Pubkey,
}

impl RemoveOperatorEvent {
    pub fn new(instance_seed: Pubkey, operator: Pubkey) -> Self {
        Self {
            event_discriminator: EventDiscriminators::RemoveOperator as u8,
            instance_seed,
            operator,
        }
    }

    // 8 (tag) + 1 (discriminator) + 32 (instance_seed) + 32 (operator)
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut data = Vec::with_capacity(73);
        data.extend_from_slice(EVENT_IX_TAG_LE);
        data.push(self.event_discriminator);
        data.extend_from_slice(self.instance_seed.as_ref());
        data.extend_from_slice(self.operator.as_ref());
        data
    }
}

#[derive(CodamaType)]
pub struct SetNewAdminEvent {
    /// Unique u8 byte for event type.
    pub event_discriminator: u8,
    /// Instance seed pubkey
    pub instance_seed: Pubkey,
    /// Previous admin pubkey
    pub old_admin: Pubkey,
    /// New admin pubkey
    pub new_admin: Pubkey,
}

impl SetNewAdminEvent {
    pub fn new(instance_seed: Pubkey, old_admin: Pubkey, new_admin: Pubkey) -> Self {
        Self {
            event_discriminator: EventDiscriminators::SetNewAdmin as u8,
            instance_seed,
            old_admin,
            new_admin,
        }
    }

    // 8 (tag) + 1 (discriminator) + 32 (instance_seed) + 32 (old_admin) + 32 (new_admin)
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut data = Vec::with_capacity(105);
        data.extend_from_slice(EVENT_IX_TAG_LE);
        data.push(self.event_discriminator);
        data.extend_from_slice(self.instance_seed.as_ref());
        data.extend_from_slice(self.old_admin.as_ref());
        data.extend_from_slice(self.new_admin.as_ref());
        data
    }
}

#[derive(CodamaType)]
pub struct DepositEvent {
    /// Unique u8 byte for event type.
    pub event_discriminator: u8,
    /// Instance seed pubkey
    pub instance_seed: Pubkey,
    /// User who made the deposit
    pub user: Pubkey,
    /// Amount of tokens deposited
    pub amount: u64,
    /// Recipient (for PrivateChannel tracking)
    pub recipient: Pubkey,
    /// Mint of the deposited tokens
    pub mint: Pubkey,
}

impl DepositEvent {
    pub fn new(
        instance_seed: Pubkey,
        user: Pubkey,
        amount: u64,
        recipient: Pubkey,
        mint: Pubkey,
    ) -> Self {
        Self {
            event_discriminator: EventDiscriminators::Deposit as u8,
            instance_seed,
            user,
            amount,
            recipient,
            mint,
        }
    }

    // 8 (tag) + 1 (discriminator) + 32 (instance_seed) + 32 (user) + 8 (amount) + 32 (recipient) + 32 (mint)
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut data = Vec::with_capacity(145);
        data.extend_from_slice(EVENT_IX_TAG_LE);
        data.push(self.event_discriminator);
        data.extend_from_slice(self.instance_seed.as_ref());
        data.extend_from_slice(self.user.as_ref());
        data.extend_from_slice(&self.amount.to_le_bytes());
        data.extend_from_slice(self.recipient.as_ref());
        data.extend_from_slice(self.mint.as_ref());
        data
    }
}

#[derive(CodamaType)]
pub struct ReleaseFundsEvent {
    /// Unique u8 byte for event type.
    pub event_discriminator: u8,
    /// Instance seed pubkey
    pub instance_seed: Pubkey,
    /// Operator who released the funds
    pub operator: Pubkey,
    /// Amount of tokens released
    pub amount: u64,
    /// User receiving the funds
    pub user: Pubkey,
    /// Mint of the released tokens
    pub mint: Pubkey,
}

impl ReleaseFundsEvent {
    pub fn new(
        instance_seed: Pubkey,
        operator: Pubkey,
        amount: u64,
        user: Pubkey,
        mint: Pubkey,
    ) -> Self {
        Self {
            event_discriminator: EventDiscriminators::ReleaseFunds as u8,
            instance_seed,
            operator,
            amount,
            user,
            mint,
        }
    }

    // 8 (tag) + 1 (discriminator) + 32 (instance_seed) + 32 (operator) + 8 (amount) + 32 (user) + 32 (mint)
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut data = Vec::with_capacity(145);
        data.extend_from_slice(EVENT_IX_TAG_LE);
        data.push(self.event_discriminator);
        data.extend_from_slice(self.instance_seed.as_ref());
        data.extend_from_slice(self.operator.as_ref());
        data.extend_from_slice(&self.amount.to_le_bytes());
        data.extend_from_slice(self.user.as_ref());
        data.extend_from_slice(self.mint.as_ref());
        data
    }
}

#[derive(CodamaType)]
pub struct RotateBitmapEvent {
    /// Unique u8 byte for event type.
    pub event_discriminator: u8,
    /// Instance seed pubkey
    pub instance_seed: Pubkey,
    /// Operator who rotated the bitmap
    pub operator: Pubkey,
    /// Generation the bitmap now covers
    pub new_generation: u64,
}

impl RotateBitmapEvent {
    pub fn new(instance_seed: Pubkey, operator: Pubkey, new_generation: u64) -> Self {
        Self {
            event_discriminator: EventDiscriminators::RotateBitmap as u8,
            instance_seed,
            operator,
            new_generation,
        }
    }

    // 8 (tag) + 1 (discriminator) + 32 (instance_seed) + 32 (operator) + 8 (new_generation)
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut data = Vec::with_capacity(81);
        data.extend_from_slice(EVENT_IX_TAG_LE);
        data.push(self.event_discriminator);
        data.extend_from_slice(self.instance_seed.as_ref());
        data.extend_from_slice(self.operator.as_ref());
        data.extend_from_slice(&self.new_generation.to_le_bytes());
        data
    }
}

#[derive(CodamaType)]
pub struct AddOrUpdateAllowedWalletEvent {
    /// Unique u8 byte for event type.
    pub event_discriminator: u8,
    /// Instance seed pubkey
    pub instance_seed: Pubkey,
    /// Wallet whose entry was written
    pub wallet: Pubkey,
    /// Deposit flag after this instruction
    pub deposits_blocked: bool,
    /// Withdrawal flag after this instruction
    pub withdrawals_blocked: bool,
}

impl AddOrUpdateAllowedWalletEvent {
    pub fn new(
        instance_seed: Pubkey,
        wallet: Pubkey,
        deposits_blocked: bool,
        withdrawals_blocked: bool,
    ) -> Self {
        Self {
            event_discriminator: EventDiscriminators::AddOrUpdateAllowedWallet as u8,
            instance_seed,
            wallet,
            deposits_blocked,
            withdrawals_blocked,
        }
    }

    // 8 (tag) + 1 (discriminator) + 32 (instance_seed) + 32 (wallet) + 1 + 1 (blocked flags)
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut data = Vec::with_capacity(75);
        data.extend_from_slice(EVENT_IX_TAG_LE);
        data.push(self.event_discriminator);
        data.extend_from_slice(self.instance_seed.as_ref());
        data.extend_from_slice(self.wallet.as_ref());
        data.push(self.deposits_blocked as u8);
        data.push(self.withdrawals_blocked as u8);
        data
    }
}

#[derive(CodamaType)]
pub struct RemoveAllowedWalletEvent {
    /// Unique u8 byte for event type.
    pub event_discriminator: u8,
    /// Instance seed pubkey
    pub instance_seed: Pubkey,
    /// Wallet removed from the whitelist
    pub wallet: Pubkey,
}

impl RemoveAllowedWalletEvent {
    pub fn new(instance_seed: Pubkey, wallet: Pubkey) -> Self {
        Self {
            event_discriminator: EventDiscriminators::RemoveAllowedWallet as u8,
            instance_seed,
            wallet,
        }
    }

    // 8 (tag) + 1 (discriminator) + 32 (instance_seed) + 32 (wallet)
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut data = Vec::with_capacity(73);
        data.extend_from_slice(EVENT_IX_TAG_LE);
        data.push(self.event_discriminator);
        data.extend_from_slice(self.instance_seed.as_ref());
        data.extend_from_slice(self.wallet.as_ref());
        data
    }
}

#[derive(CodamaType)]
pub struct SetWhitelistEnabledEvent {
    /// Unique u8 byte for event type.
    pub event_discriminator: u8,
    /// Instance seed pubkey
    pub instance_seed: Pubkey,
    /// Whether Deposit now requires allowed wallets
    pub deposits_whitelisted: bool,
    /// Whether ReleaseFunds now requires allowed wallets
    pub withdrawals_whitelisted: bool,
}

impl SetWhitelistEnabledEvent {
    pub fn new(
        instance_seed: Pubkey,
        deposits_whitelisted: bool,
        withdrawals_whitelisted: bool,
    ) -> Self {
        Self {
            event_discriminator: EventDiscriminators::SetWhitelistEnabled as u8,
            instance_seed,
            deposits_whitelisted,
            withdrawals_whitelisted,
        }
    }

    // 8 (tag) + 1 (discriminator) + 32 (instance_seed) + 1 + 1 (whitelist flags)
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut data = Vec::with_capacity(43);
        data.extend_from_slice(EVENT_IX_TAG_LE);
        data.push(self.event_discriminator);
        data.extend_from_slice(self.instance_seed.as_ref());
        data.push(self.deposits_whitelisted as u8);
        data.push(self.withdrawals_whitelisted as u8);
        data
    }
}

#[derive(CodamaType)]
pub struct ConfigureConfidentialAccountEvent {
    /// Unique u8 byte for event type.
    pub event_discriminator: u8,
    /// Instance seed pubkey
    pub instance_seed: Pubkey,
    /// Mint whose escrow account was configured
    pub mint: Pubkey,
}

impl ConfigureConfidentialAccountEvent {
    pub fn new(instance_seed: Pubkey, mint: Pubkey) -> Self {
        Self {
            event_discriminator: EventDiscriminators::ConfigureConfidentialAccount as u8,
            instance_seed,
            mint,
        }
    }

    // 8 (tag) + 1 (discriminator) + 32 (instance_seed) + 32 (mint)
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut data = Vec::with_capacity(73);
        data.extend_from_slice(EVENT_IX_TAG_LE);
        data.push(self.event_discriminator);
        data.extend_from_slice(self.instance_seed.as_ref());
        data.extend_from_slice(self.mint.as_ref());
        data
    }
}

/// The deposit amount is the difference between the pending balances, decrypted
/// with the instance key. Nothing else can credit the escrow account in between.
#[derive(CodamaType)]
pub struct ConfidentialDepositEvent {
    /// Unique u8 byte for event type.
    pub event_discriminator: u8,
    /// Instance seed pubkey
    pub instance_seed: Pubkey,
    /// User who made the deposit
    pub user: Pubkey,
    /// Recipient (for PrivateChannel tracking)
    pub recipient: Pubkey,
    /// Mint of the deposited tokens
    pub mint: Pubkey,
    /// Escrow pending balance, low bits, before the transfer
    pub pending_balance_lo_before: [u8; 64],
    /// Escrow pending balance, high bits, before the transfer
    pub pending_balance_hi_before: [u8; 64],
    /// Escrow pending balance, low bits, after the transfer
    pub pending_balance_lo_after: [u8; 64],
    /// Escrow pending balance, high bits, after the transfer
    pub pending_balance_hi_after: [u8; 64],
}

impl ConfidentialDepositEvent {
    pub fn new(
        instance_seed: Pubkey,
        user: Pubkey,
        recipient: Pubkey,
        mint: Pubkey,
        pending_before: SplitCiphertext,
        pending_after: SplitCiphertext,
    ) -> Self {
        Self {
            event_discriminator: EventDiscriminators::ConfidentialDeposit as u8,
            instance_seed,
            user,
            recipient,
            mint,
            pending_balance_lo_before: pending_before.lo,
            pending_balance_hi_before: pending_before.hi,
            pending_balance_lo_after: pending_after.lo,
            pending_balance_hi_after: pending_after.hi,
        }
    }

    // 8 (tag) + 1 (discriminator) + 32 (instance_seed) + 32 (user) + 32 (recipient)
    // + 32 (mint) + 4 * 64 (pending lo and hi, before and after)
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut data = Vec::with_capacity(393);
        data.extend_from_slice(EVENT_IX_TAG_LE);
        data.push(self.event_discriminator);
        data.extend_from_slice(self.instance_seed.as_ref());
        data.extend_from_slice(self.user.as_ref());
        data.extend_from_slice(self.recipient.as_ref());
        data.extend_from_slice(self.mint.as_ref());
        data.extend_from_slice(&self.pending_balance_lo_before);
        data.extend_from_slice(&self.pending_balance_hi_before);
        data.extend_from_slice(&self.pending_balance_lo_after);
        data.extend_from_slice(&self.pending_balance_hi_after);
        data
    }
}

#[derive(CodamaType)]
pub struct ConfidentialReleaseFundsEvent {
    /// Unique u8 byte for event type.
    pub event_discriminator: u8,
    /// Instance seed pubkey
    pub instance_seed: Pubkey,
    /// Operator who released the funds
    pub operator: Pubkey,
    /// User receiving the funds
    pub user: Pubkey,
    /// Mint of the released tokens
    pub mint: Pubkey,
    /// Amount that left the escrow, low bits, encrypted under the instance key
    pub amount_lo: [u8; 64],
    /// Amount that left the escrow, high bits, encrypted under the instance key
    pub amount_hi: [u8; 64],
}

impl ConfidentialReleaseFundsEvent {
    pub fn new(
        instance_seed: Pubkey,
        operator: Pubkey,
        user: Pubkey,
        mint: Pubkey,
        amount: SplitCiphertext,
    ) -> Self {
        Self {
            event_discriminator: EventDiscriminators::ConfidentialReleaseFunds as u8,
            instance_seed,
            operator,
            user,
            mint,
            amount_lo: amount.lo,
            amount_hi: amount.hi,
        }
    }

    // 8 (tag) + 1 (discriminator) + 32 (instance_seed) + 32 (operator) + 32 (user)
    // + 32 (mint) + 64 (amount lo) + 64 (amount hi)
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut data = Vec::with_capacity(265);
        data.extend_from_slice(EVENT_IX_TAG_LE);
        data.push(self.event_discriminator);
        data.extend_from_slice(self.instance_seed.as_ref());
        data.extend_from_slice(self.operator.as_ref());
        data.extend_from_slice(self.user.as_ref());
        data.extend_from_slice(self.mint.as_ref());
        data.extend_from_slice(&self.amount_lo);
        data.extend_from_slice(&self.amount_hi);
        data
    }
}

#[derive(CodamaType)]
pub struct ApplyPendingBalanceEvent {
    /// Unique u8 byte for event type.
    pub event_discriminator: u8,
    /// Instance seed pubkey
    pub instance_seed: Pubkey,
    /// Operator who applied the pending balance
    pub operator: Pubkey,
    /// Mint of the escrow account
    pub mint: Pubkey,
}

impl ApplyPendingBalanceEvent {
    pub fn new(instance_seed: Pubkey, operator: Pubkey, mint: Pubkey) -> Self {
        Self {
            event_discriminator: EventDiscriminators::ApplyPendingBalance as u8,
            instance_seed,
            operator,
            mint,
        }
    }

    // 8 (tag) + 1 (discriminator) + 32 (instance_seed) + 32 (operator) + 32 (mint)
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut data = Vec::with_capacity(105);
        data.extend_from_slice(EVENT_IX_TAG_LE);
        data.push(self.event_discriminator);
        data.extend_from_slice(self.instance_seed.as_ref());
        data.extend_from_slice(self.operator.as_ref());
        data.extend_from_slice(self.mint.as_ref());
        data
    }
}

#[derive(CodamaType)]
pub struct MoveToConfidentialEvent {
    /// Unique u8 byte for event type.
    pub event_discriminator: u8,
    /// Instance seed pubkey
    pub instance_seed: Pubkey,
    /// Operator who moved the funds
    pub operator: Pubkey,
    /// Mint of the escrow account
    pub mint: Pubkey,
    /// Amount moved from the public to the pending balance
    pub amount: u64,
}

impl MoveToConfidentialEvent {
    pub fn new(instance_seed: Pubkey, operator: Pubkey, mint: Pubkey, amount: u64) -> Self {
        Self {
            event_discriminator: EventDiscriminators::MoveToConfidential as u8,
            instance_seed,
            operator,
            mint,
            amount,
        }
    }

    // 8 (tag) + 1 (discriminator) + 32 (instance_seed) + 32 (operator) + 32 (mint) + 8 (amount)
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut data = Vec::with_capacity(113);
        data.extend_from_slice(EVENT_IX_TAG_LE);
        data.push(self.event_discriminator);
        data.extend_from_slice(self.instance_seed.as_ref());
        data.extend_from_slice(self.operator.as_ref());
        data.extend_from_slice(self.mint.as_ref());
        data.extend_from_slice(&self.amount.to_le_bytes());
        data
    }
}

#[derive(CodamaType)]
pub struct MoveToPublicEvent {
    /// Unique u8 byte for event type.
    pub event_discriminator: u8,
    /// Instance seed pubkey
    pub instance_seed: Pubkey,
    /// Operator who moved the funds
    pub operator: Pubkey,
    /// Mint of the escrow account
    pub mint: Pubkey,
    /// Amount moved from the available to the public balance
    pub amount: u64,
}

impl MoveToPublicEvent {
    pub fn new(instance_seed: Pubkey, operator: Pubkey, mint: Pubkey, amount: u64) -> Self {
        Self {
            event_discriminator: EventDiscriminators::MoveToPublic as u8,
            instance_seed,
            operator,
            mint,
            amount,
        }
    }

    // 8 (tag) + 1 (discriminator) + 32 (instance_seed) + 32 (operator) + 32 (mint) + 8 (amount)
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut data = Vec::with_capacity(113);
        data.extend_from_slice(EVENT_IX_TAG_LE);
        data.push(self.event_discriminator);
        data.extend_from_slice(self.instance_seed.as_ref());
        data.extend_from_slice(self.operator.as_ref());
        data.extend_from_slice(self.mint.as_ref());
        data.extend_from_slice(&self.amount.to_le_bytes());
        data
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_instance_event() {
        let instance_seed = Pubkey::new_from_array([1u8; 32]);
        let admin = Pubkey::new_from_array([2u8; 32]);
        let event = CreateInstanceEvent::new(instance_seed, admin);

        assert_eq!(
            event.event_discriminator,
            EventDiscriminators::CreateInstance as u8
        );
        assert_eq!(event.instance_seed, instance_seed);
        assert_eq!(event.admin, admin);

        // 8 (tag) + 1 (disc) + 32 (instance_seed) + 32 (admin)
        let bytes = event.to_bytes();
        assert_eq!(bytes.len(), 73);
        assert_eq!(&bytes[..8], EVENT_IX_TAG_LE);
        assert_eq!(bytes[8], EventDiscriminators::CreateInstance as u8);
    }

    #[test]
    fn test_allow_mint_event() {
        let instance_seed = Pubkey::new_from_array([1u8; 32]);
        let mint = Pubkey::new_from_array([2u8; 32]);
        let withdraw_fee = 1_234_567u64;
        let min_withdraw_amount = 7_654_321u64;
        let min_deposit_amount = 2_345_678u64;
        let event = AllowMintEvent::new(
            instance_seed,
            mint,
            6,
            withdraw_fee,
            min_withdraw_amount,
            min_deposit_amount,
        );

        assert_eq!(
            event.event_discriminator,
            EventDiscriminators::AllowMint as u8
        );
        assert_eq!(event.instance_seed, instance_seed);
        assert_eq!(event.mint, mint);
        assert_eq!(event.decimals, 6);
        assert_eq!(event.withdraw_fee, withdraw_fee);
        assert_eq!(event.min_withdraw_amount, min_withdraw_amount);
        assert_eq!(event.min_deposit_amount, min_deposit_amount);

        // 8 (tag) + 1 (disc) + 32 (instance_seed) + 32 (mint) + 1 (decimals)
        // + 8 (withdraw_fee) + 8 (min_withdraw_amount) + 8 (min_deposit_amount)
        let bytes = event.to_bytes();
        assert_eq!(bytes.len(), 98);
        assert_eq!(&bytes[..8], EVENT_IX_TAG_LE);
        assert_eq!(bytes[8], EventDiscriminators::AllowMint as u8);
        assert_eq!(bytes[73], 6);
        assert_eq!(&bytes[74..82], &withdraw_fee.to_le_bytes());
        assert_eq!(&bytes[82..90], &min_withdraw_amount.to_le_bytes());
        assert_eq!(&bytes[90..98], &min_deposit_amount.to_le_bytes());
    }

    #[test]
    fn test_block_mint_event() {
        let instance_seed = Pubkey::new_from_array([1u8; 32]);
        let mint = Pubkey::new_from_array([2u8; 32]);
        let event = BlockMintEvent::new(instance_seed, mint, true, false);

        assert_eq!(
            event.event_discriminator,
            EventDiscriminators::BlockMint as u8
        );
        assert_eq!(event.instance_seed, instance_seed);
        assert_eq!(event.mint, mint);

        // 8 (tag) + 1 (disc) + 32 (instance_seed) + 32 (mint) + 1 + 1 (gates)
        let bytes = event.to_bytes();
        assert_eq!(bytes.len(), 75);
        assert_eq!(&bytes[..8], EVENT_IX_TAG_LE);
        assert_eq!(bytes[8], EventDiscriminators::BlockMint as u8);
        // Opposite values pin the gate byte order the indexer reads.
        assert_eq!(bytes[73], 1);
        assert_eq!(bytes[74], 0);
    }

    #[test]
    fn test_add_operator_event() {
        let instance_seed = Pubkey::new_from_array([1u8; 32]);
        let operator = Pubkey::new_from_array([2u8; 32]);
        let event = AddOperatorEvent::new(instance_seed, operator);

        assert_eq!(
            event.event_discriminator,
            EventDiscriminators::AddOperator as u8
        );
        assert_eq!(event.instance_seed, instance_seed);
        assert_eq!(event.operator, operator);

        // 8 (tag) + 1 (disc) + 32 (instance_seed) + 32 (operator)
        let bytes = event.to_bytes();
        assert_eq!(bytes.len(), 73);
        assert_eq!(&bytes[..8], EVENT_IX_TAG_LE);
        assert_eq!(bytes[8], EventDiscriminators::AddOperator as u8);
    }

    #[test]
    fn test_remove_operator_event() {
        let instance_seed = Pubkey::new_from_array([1u8; 32]);
        let operator = Pubkey::new_from_array([2u8; 32]);
        let event = RemoveOperatorEvent::new(instance_seed, operator);

        assert_eq!(
            event.event_discriminator,
            EventDiscriminators::RemoveOperator as u8
        );
        assert_eq!(event.instance_seed, instance_seed);
        assert_eq!(event.operator, operator);

        // 8 (tag) + 1 (disc) + 32 (instance_seed) + 32 (operator)
        let bytes = event.to_bytes();
        assert_eq!(bytes.len(), 73);
        assert_eq!(&bytes[..8], EVENT_IX_TAG_LE);
        assert_eq!(bytes[8], EventDiscriminators::RemoveOperator as u8);
    }

    #[test]
    fn test_set_new_admin_event() {
        let instance_seed = Pubkey::new_from_array([1u8; 32]);
        let old_admin = Pubkey::new_from_array([2u8; 32]);
        let new_admin = Pubkey::new_from_array([3u8; 32]);
        let event = SetNewAdminEvent::new(instance_seed, old_admin, new_admin);

        assert_eq!(
            event.event_discriminator,
            EventDiscriminators::SetNewAdmin as u8
        );
        assert_eq!(event.instance_seed, instance_seed);
        assert_eq!(event.old_admin, old_admin);
        assert_eq!(event.new_admin, new_admin);

        // 8 (tag) + 1 (disc) + 32 (instance_seed) + 32 (old_admin) + 32 (new_admin)
        let bytes = event.to_bytes();
        assert_eq!(bytes.len(), 105);
        assert_eq!(&bytes[..8], EVENT_IX_TAG_LE);
        assert_eq!(bytes[8], EventDiscriminators::SetNewAdmin as u8);
    }

    #[test]
    fn test_deposit_event() {
        let instance_seed = Pubkey::new_from_array([1u8; 32]);
        let user = Pubkey::new_from_array([2u8; 32]);
        let recipient = Pubkey::new_from_array([3u8; 32]);
        let mint = Pubkey::new_from_array([4u8; 32]);
        let amount = 1_000_000u64;
        let event = DepositEvent::new(instance_seed, user, amount, recipient, mint);

        assert_eq!(
            event.event_discriminator,
            EventDiscriminators::Deposit as u8
        );
        assert_eq!(event.instance_seed, instance_seed);
        assert_eq!(event.user, user);
        assert_eq!(event.amount, amount);
        assert_eq!(event.recipient, recipient);
        assert_eq!(event.mint, mint);

        // 8 (tag) + 1 (disc) + 32 (instance_seed) + 32 (user) + 8 (amount) + 32 (recipient) + 32 (mint)
        let bytes = event.to_bytes();
        assert_eq!(bytes.len(), 145);
        assert_eq!(&bytes[..8], EVENT_IX_TAG_LE);
        assert_eq!(bytes[8], EventDiscriminators::Deposit as u8);
    }

    #[test]
    fn test_release_funds_event() {
        let instance_seed = Pubkey::new_from_array([1u8; 32]);
        let operator = Pubkey::new_from_array([2u8; 32]);
        let user = Pubkey::new_from_array([3u8; 32]);
        let mint = Pubkey::new_from_array([4u8; 32]);
        let amount = 500_000u64;
        let event = ReleaseFundsEvent::new(instance_seed, operator, amount, user, mint);

        assert_eq!(
            event.event_discriminator,
            EventDiscriminators::ReleaseFunds as u8
        );
        assert_eq!(event.instance_seed, instance_seed);
        assert_eq!(event.operator, operator);
        assert_eq!(event.amount, amount);
        assert_eq!(event.user, user);
        assert_eq!(event.mint, mint);

        // 8 (tag) + 1 (disc) + 32 (instance_seed) + 32 (operator) + 8 (amount) + 32 (user) + 32 (mint)
        let bytes = event.to_bytes();
        assert_eq!(bytes.len(), 145);
        assert_eq!(&bytes[..8], EVENT_IX_TAG_LE);
        assert_eq!(bytes[8], EventDiscriminators::ReleaseFunds as u8);
    }

    #[test]
    fn test_rotate_bitmap_event() {
        let instance_seed = Pubkey::new_from_array([1u8; 32]);
        let operator = Pubkey::new_from_array([2u8; 32]);
        let new_generation = 3u64;
        let event = RotateBitmapEvent::new(instance_seed, operator, new_generation);

        assert_eq!(
            event.event_discriminator,
            EventDiscriminators::RotateBitmap as u8
        );
        assert_eq!(event.instance_seed, instance_seed);
        assert_eq!(event.operator, operator);
        assert_eq!(event.new_generation, new_generation);

        // 8 (tag) + 1 (disc) + 32 (instance_seed) + 32 (operator) + 8 (new_generation)
        let bytes = event.to_bytes();
        assert_eq!(bytes.len(), 81);
        assert_eq!(&bytes[..8], EVENT_IX_TAG_LE);
        assert_eq!(bytes[8], EventDiscriminators::RotateBitmap as u8);
    }

    #[test]
    fn test_add_or_update_allowed_wallet_event() {
        let instance_seed = Pubkey::new_from_array([1u8; 32]);
        let wallet = Pubkey::new_from_array([2u8; 32]);
        let event = AddOrUpdateAllowedWalletEvent::new(instance_seed, wallet, true, false);

        // 8 (tag) + 1 (disc) + 32 (instance_seed) + 32 (wallet) + 1 + 1 (blocked flags)
        let bytes = event.to_bytes();
        assert_eq!(bytes.len(), 75);
        assert_eq!(&bytes[..8], EVENT_IX_TAG_LE);
        assert_eq!(
            bytes[8],
            EventDiscriminators::AddOrUpdateAllowedWallet as u8
        );
        assert_eq!(&bytes[41..73], wallet.as_ref());
        // Opposite values pin the flag byte order.
        assert_eq!(bytes[73], 1);
        assert_eq!(bytes[74], 0);
    }

    #[test]
    fn test_set_whitelist_enabled_event() {
        let instance_seed = Pubkey::new_from_array([1u8; 32]);
        let event = SetWhitelistEnabledEvent::new(instance_seed, true, false);

        // 8 (tag) + 1 (disc) + 32 (instance_seed) + 1 + 1 (whitelist flags)
        let bytes = event.to_bytes();
        assert_eq!(bytes.len(), 43);
        assert_eq!(&bytes[..8], EVENT_IX_TAG_LE);
        assert_eq!(bytes[8], EventDiscriminators::SetWhitelistEnabled as u8);
        assert_eq!(&bytes[9..41], instance_seed.as_ref());
        // Opposite values pin the flag byte order.
        assert_eq!(bytes[41], 1);
        assert_eq!(bytes[42], 0);
    }

    #[test]
    fn test_configure_confidential_account_event() {
        let instance_seed = Pubkey::new_from_array([1u8; 32]);
        let mint = Pubkey::new_from_array([2u8; 32]);
        let event = ConfigureConfidentialAccountEvent::new(instance_seed, mint);

        // 8 (tag) + 1 (disc) + 32 (instance_seed) + 32 (mint)
        let bytes = event.to_bytes();
        assert_eq!(bytes.len(), 73);
        assert_eq!(&bytes[..8], EVENT_IX_TAG_LE);
        assert_eq!(
            bytes[8],
            EventDiscriminators::ConfigureConfidentialAccount as u8
        );
        assert_eq!(&bytes[41..73], mint.as_ref());
    }

    #[test]
    fn test_confidential_deposit_event() {
        let instance_seed = Pubkey::new_from_array([1u8; 32]);
        let user = Pubkey::new_from_array([2u8; 32]);
        let recipient = Pubkey::new_from_array([3u8; 32]);
        let mint = Pubkey::new_from_array([4u8; 32]);
        let pending_before = SplitCiphertext {
            lo: [5u8; 64],
            hi: [6u8; 64],
        };
        let pending_after = SplitCiphertext {
            lo: [7u8; 64],
            hi: [8u8; 64],
        };
        let event = ConfidentialDepositEvent::new(
            instance_seed,
            user,
            recipient,
            mint,
            pending_before,
            pending_after,
        );

        // 8 (tag) + 1 (disc) + 32 (instance_seed) + 32 (user) + 32 (recipient) + 32 (mint)
        // + 4 * 64 (pending lo and hi, before and after)
        let bytes = event.to_bytes();
        assert_eq!(bytes.len(), 393);
        assert_eq!(&bytes[..8], EVENT_IX_TAG_LE);
        assert_eq!(bytes[8], EventDiscriminators::ConfidentialDeposit as u8);
        assert_eq!(&bytes[41..73], user.as_ref());
        assert_eq!(&bytes[73..105], recipient.as_ref());
        assert_eq!(&bytes[105..137], mint.as_ref());
        assert_eq!(&bytes[137..201], &[5u8; 64]);
        assert_eq!(&bytes[201..265], &[6u8; 64]);
        assert_eq!(&bytes[265..329], &[7u8; 64]);
        assert_eq!(&bytes[329..393], &[8u8; 64]);
    }

    #[test]
    fn test_confidential_release_funds_event() {
        let instance_seed = Pubkey::new_from_array([1u8; 32]);
        let operator = Pubkey::new_from_array([2u8; 32]);
        let user = Pubkey::new_from_array([3u8; 32]);
        let mint = Pubkey::new_from_array([4u8; 32]);
        let amount = SplitCiphertext {
            lo: [5u8; 64],
            hi: [6u8; 64],
        };
        let event = ConfidentialReleaseFundsEvent::new(instance_seed, operator, user, mint, amount);

        // 8 (tag) + 1 (disc) + 32 (instance_seed) + 32 (operator) + 32 (user) + 32 (mint)
        // + 64 (amount lo) + 64 (amount hi)
        let bytes = event.to_bytes();
        assert_eq!(bytes.len(), 265);
        assert_eq!(&bytes[..8], EVENT_IX_TAG_LE);
        assert_eq!(
            bytes[8],
            EventDiscriminators::ConfidentialReleaseFunds as u8
        );
        assert_eq!(&bytes[73..105], user.as_ref());
        assert_eq!(&bytes[105..137], mint.as_ref());
        assert_eq!(&bytes[137..201], &[5u8; 64]);
        assert_eq!(&bytes[201..265], &[6u8; 64]);
    }

    #[test]
    fn test_apply_pending_balance_event() {
        let instance_seed = Pubkey::new_from_array([1u8; 32]);
        let operator = Pubkey::new_from_array([2u8; 32]);
        let mint = Pubkey::new_from_array([3u8; 32]);
        let event = ApplyPendingBalanceEvent::new(instance_seed, operator, mint);

        // 8 (tag) + 1 (disc) + 32 (instance_seed) + 32 (operator) + 32 (mint)
        let bytes = event.to_bytes();
        assert_eq!(bytes.len(), 105);
        assert_eq!(&bytes[..8], EVENT_IX_TAG_LE);
        assert_eq!(bytes[8], EventDiscriminators::ApplyPendingBalance as u8);
        assert_eq!(&bytes[73..105], mint.as_ref());
    }

    #[test]
    fn test_move_to_confidential_event() {
        let instance_seed = Pubkey::new_from_array([1u8; 32]);
        let operator = Pubkey::new_from_array([2u8; 32]);
        let mint = Pubkey::new_from_array([3u8; 32]);
        let amount = 1_234_567u64;
        let event = MoveToConfidentialEvent::new(instance_seed, operator, mint, amount);

        // 8 (tag) + 1 (disc) + 32 (instance_seed) + 32 (operator) + 32 (mint) + 8 (amount)
        let bytes = event.to_bytes();
        assert_eq!(bytes.len(), 113);
        assert_eq!(&bytes[..8], EVENT_IX_TAG_LE);
        assert_eq!(bytes[8], EventDiscriminators::MoveToConfidential as u8);
        assert_eq!(&bytes[105..113], &amount.to_le_bytes());
    }

    #[test]
    fn test_move_to_public_event() {
        let instance_seed = Pubkey::new_from_array([1u8; 32]);
        let operator = Pubkey::new_from_array([2u8; 32]);
        let mint = Pubkey::new_from_array([3u8; 32]);
        let amount = 1_234_567u64;
        let event = MoveToPublicEvent::new(instance_seed, operator, mint, amount);

        // 8 (tag) + 1 (disc) + 32 (instance_seed) + 32 (operator) + 32 (mint) + 8 (amount)
        let bytes = event.to_bytes();
        assert_eq!(bytes.len(), 113);
        assert_eq!(&bytes[..8], EVENT_IX_TAG_LE);
        assert_eq!(bytes[8], EventDiscriminators::MoveToPublic as u8);
        assert_eq!(&bytes[105..113], &amount.to_le_bytes());
    }
}
