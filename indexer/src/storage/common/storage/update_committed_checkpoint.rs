use crate::{
    error::StorageError,
    storage::common::{models::ChannelFence, storage::Storage},
};

pub async fn update_committed_checkpoint(
    storage: &Storage,
    program_type: &str,
    slot: u64,
    fence: Option<&ChannelFence>,
) -> Result<(), StorageError> {
    match storage {
        Storage::Postgres(db) => Ok(db
            .update_committed_checkpoint_internal(program_type, slot, fence)
            .await?),
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock_db) => {
            mock_db
                .update_committed_checkpoint_with_fence(program_type, slot, fence)
                .await
        }
    }
}
