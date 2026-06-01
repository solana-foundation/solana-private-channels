use crate::{
    config::{BackfillConfig, ProgramType},
    error::{DataSourceError, IndexerError},
    indexer::{
        backfill::BackfillService, checkpoint::CheckpointWriter,
        datasource::rpc_polling::rpc::RpcPoller, transaction_processor::TransactionProcessor,
    },
    storage::Storage,
};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{error, info};

/// Resync service for rebuilding indexer database from chain history
pub struct ResyncService {
    storage: Arc<Storage>,
    rpc_poller: Arc<RpcPoller>,
    program_type: ProgramType,
    backfill_config_base: BackfillConfig,
    escrow_instance_id: Option<solana_sdk::pubkey::Pubkey>,
}

impl ResyncService {
    pub fn new(
        storage: Arc<Storage>,
        rpc_poller: Arc<RpcPoller>,
        program_type: ProgramType,
        backfill_config_base: BackfillConfig,
        escrow_instance_id: Option<solana_sdk::pubkey::Pubkey>,
    ) -> Self {
        Self {
            storage,
            rpc_poller,
            program_type,
            backfill_config_base,
            escrow_instance_id,
        }
    }

    /// Run the resync process
    /// Returns Ok(()) if resync successful, Err otherwise
    pub async fn run(&self, genesis_slot: u64) -> Result<(), IndexerError> {
        info!(
            "Starting database resync for {:?} from slot {}...",
            self.program_type, genesis_slot
        );

        // Step 1: Drop existing tables
        info!("Dropping existing database tables...");
        self.storage.drop_tables().await.map_err(|e| {
            error!("Failed to drop database tables during resync: {}", e);
            e
        })?;
        info!("Database tables dropped successfully");

        // Step 2: Recreate schema
        info!("Recreating database schema...");
        self.storage.init_schema().await.map_err(|e| {
            error!("Failed to recreate database schema during resync: {}", e);
            e
        })?;
        info!("Database schema recreated successfully");

        // Step 3: Create BackfillService with genesis_slot configuration
        let backfill_config = BackfillConfig {
            enabled: true,
            exit_after_backfill: false,
            rpc_url: self.backfill_config_base.rpc_url.clone(),
            batch_size: self.backfill_config_base.batch_size,
            max_gap_slots: u64::MAX, // No limit for full resync
            start_slot: Some(genesis_slot),
        };

        let backfill_service = BackfillService::new(
            self.storage.clone(),
            self.rpc_poller.clone(),
            self.program_type,
            backfill_config,
            self.escrow_instance_id,
        );

        // Step 4: Setup processing pipeline
        // Create channels for instruction flow and checkpoint updates
        let (instruction_tx, instruction_rx) = mpsc::channel(1000);
        let (checkpoint_tx, checkpoint_rx) = mpsc::channel(1000);

        // Start checkpoint writer service
        let checkpoint_writer = CheckpointWriter::new(self.storage.clone());
        let checkpoint_handle = checkpoint_writer.start(checkpoint_rx);
        info!("CheckpointWriter service started");

        // Start transaction processor as separate tokio task
        let mut transaction_processor =
            TransactionProcessor::new(self.storage.clone(), checkpoint_tx.clone());
        // Wire the escrow instance scope. Config validation guarantees Some for the
        // Escrow program; None here means the Withdraw program, where no instance
        // scoping applies.
        if let Some(instance_id) = self.escrow_instance_id {
            transaction_processor = transaction_processor.with_escrow_instance_id(instance_id);
        }
        let processor_handle =
            tokio::spawn(async move { transaction_processor.start(instruction_rx).await });
        info!("TransactionProcessor task spawned");

        // Run backfill service (this will process all transactions from genesis_slot to current)
        let current_slot = self.rpc_poller.get_latest_slot().await.map_err(|e| {
            error!("Failed to fetch current slot before resync backfill: {}", e);
            IndexerError::DataSource(e.into())
        })?;

        // Validate genesis_slot is not in the future
        if genesis_slot > current_slot {
            error!(
                "Invalid genesis_slot {}: cannot be ahead of current_slot {}",
                genesis_slot, current_slot
            );
            return Err(IndexerError::from(DataSourceError::InvalidConfig {
                reason: format!(
                    "genesis_slot {} is ahead of current_slot {}",
                    genesis_slot, current_slot
                ),
            }));
        }

        let total_slots = current_slot.saturating_sub(genesis_slot);

        info!(
            "Starting backfill from slot {} to slot {} ({} slots to process)...",
            genesis_slot, current_slot, total_slots
        );

        backfill_service
            .run(instruction_tx.clone())
            .await
            .map_err(|e| {
                error!(
                    "Backfill service failed during resync from slot {} to {}: {}",
                    genesis_slot, current_slot, e
                );
                e
            })?;
        info!("Backfill service completed");

        // Drop instruction_tx to signal no more instructions coming
        drop(instruction_tx);

        // Wait for processor to finish processing all instructions
        match processor_handle.await {
            Ok(Ok(())) => info!("Transaction processor completed successfully"),
            Ok(Err(e)) => {
                error!("Transaction processor failed during resync: {}", e);
                return Err(e);
            }
            Err(e) => {
                error!("Transaction processor task panicked during resync: {:?}", e);
                return Err(IndexerError::ShutdownChannelSend);
            }
        }

        // Perform cleanup after backfill
        if let Err(e) = crate::shutdown_utils::cleanup_after_backfill(
            checkpoint_handle,
            checkpoint_tx,
            self.storage.clone(),
        )
        .await
        {
            error!("Cleanup after resync backfill failed: {}", e);
            return Err(IndexerError::ShutdownChannelSend);
        }

        info!(
            "Resync complete for {:?}. Processed {} slots (from {} to {})",
            self.program_type, total_slots, genesis_slot, current_slot
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{BackfillConfig, ProgramType};
    use crate::indexer::datasource::rpc_polling::rpc::RpcPoller;
    use crate::storage::common::storage::mock::MockStorage;
    use crate::storage::Storage;
    use solana_sdk::commitment_config::CommitmentLevel;
    use solana_transaction_status::UiTransactionEncoding;
    use std::sync::Arc;

    #[test]
    fn resync_service_new_with_escrow_instance_id() {
        use solana_sdk::pubkey::Pubkey;
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let rpc_poller = Arc::new(RpcPoller::new(
            "http://localhost:8899".to_string(),
            UiTransactionEncoding::Json,
            CommitmentLevel::Finalized,
        ));
        let backfill_config = BackfillConfig {
            enabled: false,
            exit_after_backfill: false,
            rpc_url: "http://localhost:8899".to_string(),
            batch_size: 50,
            max_gap_slots: 500,
            start_slot: Some(1000),
        };
        let instance_id = Pubkey::new_unique();

        let service = ResyncService::new(
            storage,
            rpc_poller,
            ProgramType::Withdraw,
            backfill_config,
            Some(instance_id),
        );

        assert_eq!(service.program_type, ProgramType::Withdraw);
        assert_eq!(service.escrow_instance_id, Some(instance_id));
        assert_eq!(service.backfill_config_base.start_slot, Some(1000));
    }
}
