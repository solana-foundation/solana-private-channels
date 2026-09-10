extern crate alloc;

use alloc::vec::Vec;
use codama::CodamaType;
use pinocchio::Address as Pubkey;

use crate::constants::EVENT_IX_TAG_LE;

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
}

impl AllowMintEvent {
    pub fn new(instance_seed: Pubkey, mint: Pubkey, decimals: u8) -> Self {
        Self {
            event_discriminator: EventDiscriminators::AllowMint as u8,
            instance_seed,
            mint,
            decimals,
        }
    }

    // 8 (tag) + 1 (discriminator) + 32 (instance_seed) + 32 (mint) + 1 (decimals)
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut data = Vec::with_capacity(74);
        data.extend_from_slice(EVENT_IX_TAG_LE);
        data.push(self.event_discriminator);
        data.extend_from_slice(self.instance_seed.as_ref());
        data.extend_from_slice(self.mint.as_ref());
        data.push(self.decimals);
        data
    }
}

#[derive(CodamaType)]
pub struct BlockMintEvent {
    /// Unique u8 byte for event type.
    pub event_discriminator: u8,
    /// Instance seed pubkey
    pub instance_seed: Pubkey,
    /// Mint pubkey that was blocked
    pub mint: Pubkey,
}

impl BlockMintEvent {
    pub fn new(instance_seed: Pubkey, mint: Pubkey) -> Self {
        Self {
            event_discriminator: EventDiscriminators::BlockMint as u8,
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
        let event = AllowMintEvent::new(instance_seed, mint, 6);

        assert_eq!(
            event.event_discriminator,
            EventDiscriminators::AllowMint as u8
        );
        assert_eq!(event.instance_seed, instance_seed);
        assert_eq!(event.mint, mint);
        assert_eq!(event.decimals, 6);

        // 8 (tag) + 1 (disc) + 32 (instance_seed) + 32 (mint) + 1 (decimals)
        let bytes = event.to_bytes();
        assert_eq!(bytes.len(), 74);
        assert_eq!(&bytes[..8], EVENT_IX_TAG_LE);
        assert_eq!(bytes[8], EventDiscriminators::AllowMint as u8);
    }

    #[test]
    fn test_block_mint_event() {
        let instance_seed = Pubkey::new_from_array([1u8; 32]);
        let mint = Pubkey::new_from_array([2u8; 32]);
        let event = BlockMintEvent::new(instance_seed, mint);

        assert_eq!(
            event.event_discriminator,
            EventDiscriminators::BlockMint as u8
        );
        assert_eq!(event.instance_seed, instance_seed);
        assert_eq!(event.mint, mint);

        // 8 (tag) + 1 (disc) + 32 (instance_seed) + 32 (mint)
        let bytes = event.to_bytes();
        assert_eq!(bytes.len(), 73);
        assert_eq!(&bytes[..8], EVENT_IX_TAG_LE);
        assert_eq!(bytes[8], EventDiscriminators::BlockMint as u8);
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
}
