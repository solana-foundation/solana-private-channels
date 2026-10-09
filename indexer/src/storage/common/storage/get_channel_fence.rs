use crate::{
    error::StorageError,
    storage::common::{models::ChannelFence, storage::Storage},
};

pub async fn get_channel_fence(storage: &Storage) -> Result<Option<ChannelFence>, StorageError> {
    match storage {
        Storage::Postgres(db) => Ok(db.get_channel_fence_internal().await?),
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock_db) => mock_db.get_channel_fence().await,
    }
}
