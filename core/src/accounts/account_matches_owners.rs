use {
    super::{
        get_account_shared_data::get_account_shared_data, get_accounts::AccountLoadError,
        traits::AccountsDB,
    },
    solana_sdk::{account::ReadableAccount, pubkey::Pubkey},
};

/// `Ok(None)` means no owner matched or the account is absent. A store that
/// could not answer is an `Err`, never a silent non-match.
pub async fn account_matches_owners(
    db: &AccountsDB,
    account: &Pubkey,
    owners: &[Pubkey],
) -> Result<Option<usize>, AccountLoadError> {
    let account_data = get_account_shared_data(db, account).await?;
    Ok(account_data.and_then(|account| owners.iter().position(|key| account.owner().eq(key))))
}
