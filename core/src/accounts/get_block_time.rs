use {super::traits::AccountsDB, anyhow::Result};

/// Reads the same row as `get_block`, so it inherits the same split: `Ok(None)`
/// is a pruned or timeless block, and a read failure is an `Err`.
pub async fn get_block_time(db: &AccountsDB, slot: u64) -> Result<Option<i64>> {
    Ok(super::get_block::get_block(db, slot)
        .await?
        .and_then(|b| b.block_time))
}
