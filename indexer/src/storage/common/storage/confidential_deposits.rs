use crate::error::StorageError;
use crate::storage::common::amount::TokenAmount;
use crate::storage::common::storage::{live_lock::LiveLockGuard, Storage};
use crate::storage::postgres::db::PostgresDb;

pub async fn get_confidential_poisoned_mints(
    storage: &Storage,
) -> Result<Vec<String>, StorageError> {
    match storage {
        Storage::Postgres(db) => Ok(db.get_confidential_poisoned_mints_internal().await?),
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock) => mock.get_confidential_poisoned_mints().await,
    }
}

pub async fn get_confidential_undecrypted(
    storage: &Storage,
    mint: &str,
) -> Result<Vec<(i64, Vec<u8>)>, StorageError> {
    match storage {
        Storage::Postgres(db) => Ok(db.get_confidential_undecrypted_internal(mint).await?),
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock) => mock.get_confidential_undecrypted(mint).await,
    }
}

/// Write recovered amounts in one transaction on the session holding the exclusive live
/// lock, so recovery cannot race live indexing.
pub async fn recover_confidential_fenced(
    storage: &Storage,
    lock: &LiveLockGuard,
    mint: &str,
    recovered: Vec<(i64, TokenAmount)>,
) -> Result<(), StorageError> {
    let mint = mint.to_string();
    match storage {
        Storage::Postgres(_) => {
            lock.run_fenced(move |conn| {
                Box::pin(async move {
                    PostgresDb::recover_confidential_on(conn, &mint, &recovered).await
                })
            })
            .await
        }
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock) => {
            mock.confidential_sender
                .lock()
                .unwrap()
                .mint_states
                .remove(&mint);
            mock.recover_confidential(&recovered).await
        }
    }
}
