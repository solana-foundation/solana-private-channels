//! Token-2022 channel account fixtures shared by core, indexer and integration tests.
//!
//! The golden byte arrays were captured from the real Token-2022 program on a test
//! validator for the fixed keys below; the builders must reproduce them exactly.

use solana_sdk::{
    pubkey::Pubkey,
    signature::{Keypair, Signer},
};
use spl_token_2022::{
    extension::{
        immutable_owner::ImmutableOwner, permanent_delegate::PermanentDelegate,
        BaseStateWithExtensionsMut, ExtensionType, StateWithExtensionsMut,
    },
    state::{Account, AccountState, Mint},
};

/// Size of a channel mint: base mint, account type byte and the PermanentDelegate extension.
pub const T22_MINT_LEN: usize = 202;
/// Size of a channel token account: base account, account type byte and ImmutableOwner.
pub const T22_ACCOUNT_LEN: usize = 170;

pub const GOLDEN_DECIMALS: u8 = 6;
pub const GOLDEN_ATA_AMOUNT: u64 = 1_000;
/// Permanent delegate written into the golden mint.
pub const GOLDEN_DELEGATE: Pubkey = Pubkey::new_from_array([0xD0; 32]);
/// Owner of the golden token account.
pub const GOLDEN_OWNER: Pubkey = Pubkey::new_from_array([0x0B; 32]);

/// Mint and freeze authority of the golden mint; a keypair so the validator test can mint.
pub fn golden_authority() -> Keypair {
    Keypair::new_from_array([0x0A; 32])
}

/// Address of the golden mint; a keypair so the validator test can allocate it.
pub fn golden_mint() -> Keypair {
    Keypair::new_from_array([0x0C; 32])
}

/// `[InitializePermanentDelegate(GOLDEN_DELEGATE), InitializeMint2(6, authority, Some(authority))]`.
pub const GOLDEN_T22_MINT: [u8; T22_MINT_LEN] = [
    1, 0, 0, 0, 67, 167, 46, 113, 68, 1, 118, 45, 246, 107, 104, 194, 109, 251, 223, 38, 130, 170,
    236, 159, 36, 116, 236, 164, 97, 62, 66, 74, 15, 186, 253, 60, 0, 0, 0, 0, 0, 0, 0, 0, 6, 1, 1,
    0, 0, 0, 67, 167, 46, 113, 68, 1, 118, 45, 246, 107, 104, 194, 109, 251, 223, 38, 130, 170,
    236, 159, 36, 116, 236, 164, 97, 62, 66, 74, 15, 186, 253, 60, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 1, 12, 0, 32, 0, 208, 208, 208, 208, 208, 208, 208, 208, 208, 208, 208,
    208, 208, 208, 208, 208, 208, 208, 208, 208, 208, 208, 208, 208, 208, 208, 208, 208, 208, 208,
    208, 208,
];

/// The golden mint's ATA for `GOLDEN_OWNER` after `MintTo(GOLDEN_ATA_AMOUNT)`.
pub const GOLDEN_T22_ATA: [u8; T22_ACCOUNT_LEN] = [
    11, 81, 58, 217, 180, 146, 64, 21, 202, 9, 2, 237, 7, 144, 68, 211, 172, 93, 190, 194, 48, 111,
    6, 148, 140, 16, 218, 142, 182, 227, 159, 45, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11,
    11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 232, 3, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 2, 7, 0, 0, 0,
];

/// A Token-2022 mint, with the PermanentDelegate extension when `delegate` is set.
pub fn t22_mint(
    decimals: u8,
    authority: &Pubkey,
    freeze: Option<&Pubkey>,
    delegate: Option<&Pubkey>,
    supply: u64,
) -> Vec<u8> {
    let extensions: &[ExtensionType] = if delegate.is_some() {
        &[ExtensionType::PermanentDelegate]
    } else {
        &[]
    };
    let len = ExtensionType::try_calculate_account_len::<Mint>(extensions).unwrap();
    let mut data = vec![0u8; len];
    let mut state = StateWithExtensionsMut::<Mint>::unpack_uninitialized(&mut data).unwrap();
    if let Some(delegate) = delegate {
        state
            .init_extension::<PermanentDelegate>(true)
            .unwrap()
            .delegate = Some(*delegate).try_into().unwrap();
    }
    state.base = Mint {
        mint_authority: Some(*authority).into(),
        supply,
        decimals,
        is_initialized: true,
        freeze_authority: freeze.copied().into(),
    };
    state.pack_base();
    state.init_account_type().unwrap();
    data
}

/// A Token-2022 token account with ImmutableOwner, as the ATA program creates it.
pub fn t22_ata(mint: &Pubkey, owner: &Pubkey, amount: u64, frozen: bool) -> Vec<u8> {
    let extensions = [ExtensionType::ImmutableOwner];
    let len = ExtensionType::try_calculate_account_len::<Account>(&extensions).unwrap();
    let mut data = vec![0u8; len];
    let mut state = StateWithExtensionsMut::<Account>::unpack_uninitialized(&mut data).unwrap();
    state.init_extension::<ImmutableOwner>(true).unwrap();
    state.base = Account {
        mint: *mint,
        owner: *owner,
        amount,
        state: if frozen {
            AccountState::Frozen
        } else {
            AccountState::Initialized
        },
        ..Account::default()
    };
    state.pack_base();
    state.init_account_type().unwrap();
    data
}

/// The golden mint built with `t22_mint`.
pub fn golden_t22_mint() -> Vec<u8> {
    let authority = golden_authority().pubkey();
    t22_mint(
        GOLDEN_DECIMALS,
        &authority,
        Some(&authority),
        Some(&GOLDEN_DELEGATE),
        0,
    )
}

/// The golden token account built with `t22_ata`.
pub fn golden_t22_ata() -> Vec<u8> {
    let mint = golden_mint().pubkey();
    t22_ata(&mint, &GOLDEN_OWNER, GOLDEN_ATA_AMOUNT, false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builders_reproduce_the_golden_mint() {
        assert_eq!(golden_t22_mint(), GOLDEN_T22_MINT.to_vec());
    }

    #[test]
    fn builders_reproduce_the_golden_token_account() {
        assert_eq!(golden_t22_ata(), GOLDEN_T22_ATA.to_vec());
    }

    #[test]
    fn layouts_have_the_channel_sizes() {
        assert_eq!(golden_t22_mint().len(), T22_MINT_LEN);
        assert_eq!(golden_t22_ata().len(), T22_ACCOUNT_LEN);
        // Without the delegate the mint is a bare base mint.
        let bare = t22_mint(6, &Pubkey::new_unique(), None, None, 0);
        assert_eq!(bare.len(), 82);
    }
}
