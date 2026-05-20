use super::convert::create_message;
use crate::metrics;
use async_trait::async_trait;
use futures::stream::StreamExt;
use futures::SinkExt;
use private_channel_metrics::MetricLabel;
use solana_sdk::message::VersionedMessage;
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
#[cfg(feature = "datasource-rpc")]
use tracing::warn;
use tracing::{debug, error, info};
use yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcClient};
use yellowstone_grpc_proto::geyser::{
    subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
    SubscribeRequestFilterBlocksMeta, SubscribeRequestFilterTransactions, SubscribeRequestPing,
};

use crate::channel_utils::send_guaranteed;
use crate::config::ProgramType;
use crate::error::{DataSourceError, DataSourceRpcError};
use crate::indexer::datasource::common::parser::escrow::parse_escrow_instruction;
use crate::indexer::datasource::common::parser::withdraw::parse_withdraw_instruction;
use crate::indexer::datasource::common::{datasource::DataSource, types::*};
use crate::indexer::datasource::rpc_polling::types::InnerInstructions;
use crate::storage::Storage;

#[cfg(feature = "datasource-rpc")]
use crate::indexer::{
    backfill::{fill_slot_range, validate_gap},
    checkpoint::get_last_checkpoint,
    datasource::rpc_polling::rpc::RpcPoller,
};

/// Yellowstone gRPC datasource - directly subscribes to transactions + blocks_meta
pub struct YellowstoneSource {
    endpoint: String,
    x_token: Option<String>,
    commitment: String,
    program_type: ProgramType,
    escrow_instance_id: Option<Pubkey>,
    #[cfg(feature = "datasource-rpc")]
    rpc_poller: Option<Arc<RpcPoller>>,
    #[cfg(feature = "datasource-rpc")]
    max_gap_slots: u64,
    #[cfg(feature = "datasource-rpc")]
    batch_size: usize,
    #[cfg(feature = "datasource-rpc")]
    storage: Option<Arc<Storage>>,
    health: Option<Arc<private_channel_metrics::HealthState>>,
}

impl YellowstoneSource {
    pub fn new(
        endpoint: String,
        x_token: Option<String>,
        commitment: String,
        program_type: ProgramType,
        escrow_instance_id: Option<Pubkey>,
    ) -> Self {
        Self {
            endpoint,
            x_token,
            commitment,
            program_type,
            escrow_instance_id,
            #[cfg(feature = "datasource-rpc")]
            rpc_poller: None,
            #[cfg(feature = "datasource-rpc")]
            max_gap_slots: 0,
            #[cfg(feature = "datasource-rpc")]
            batch_size: 0,
            #[cfg(feature = "datasource-rpc")]
            storage: None,
            health: None,
        }
    }

    pub fn with_health(mut self, health: Arc<private_channel_metrics::HealthState>) -> Self {
        self.health = Some(health);
        self
    }

    #[cfg(feature = "datasource-rpc")]
    pub fn with_gap_detection(
        mut self,
        rpc_poller: Arc<RpcPoller>,
        max_gap_slots: u64,
        batch_size: usize,
    ) -> Self {
        self.rpc_poller = Some(rpc_poller);
        self.max_gap_slots = max_gap_slots;
        self.batch_size = batch_size;
        self
    }

    /// Storage holds the durable checkpoint that anchors reconnect backfill.
    /// Without it, reconnect gap-fill is a no-op.
    #[cfg(feature = "datasource-rpc")]
    pub fn with_storage(mut self, storage: Arc<Storage>) -> Self {
        self.storage = Some(storage);
        self
    }
}

#[cfg(feature = "datasource-rpc")]
async fn try_fill_reconnect_gap(
    checkpoint: u64,
    rpc_poller: &RpcPoller,
    max_gap_slots: u64,
    batch_size: usize,
    program_type: ProgramType,
    escrow_instance_id: Option<Pubkey>,
    instruction_tx: &InstructionSender,
) -> Result<u64, DataSourceError> {
    let current_slot =
        rpc_poller
            .get_latest_slot()
            .await
            .map_err(|e| DataSourceError::GapFillFailed {
                reason: format!("Failed to get latest slot: {}", e),
            })?;

    // Validate against the real checkpoint distance; the boundary slot is
    // included only when handing off to fill_slot_range below.
    match validate_gap(current_slot, checkpoint, max_gap_slots) {
        Ok(None) => {
            info!(
                "No gap detected on reconnect. Current slot: {}, checkpoint: {}",
                current_slot, checkpoint
            );
            Ok(0)
        }
        Ok(Some(gap)) => {
            let replay_anchor = checkpoint.saturating_sub(1);
            info!(
                "Gap detected on reconnect: {} slots (replaying from {} to {}). Backfilling...",
                gap, replay_anchor, current_slot
            );
            fill_slot_range(
                rpc_poller,
                replay_anchor,
                current_slot,
                batch_size,
                program_type,
                escrow_instance_id,
                instruction_tx,
            )
            .await
            .map_err(|e| DataSourceError::GapFillFailed {
                reason: e.to_string(),
            })
        }
        Err(e) => Err(DataSourceError::GapFillFailed {
            reason: e.to_string(),
        }),
    }
}

#[async_trait]
impl DataSource for YellowstoneSource {
    async fn start(
        &mut self,
        tx: InstructionSender,
        cancellation_token: CancellationToken,
    ) -> Result<tokio::task::JoinHandle<()>, DataSourceError> {
        let _ = rustls::crypto::ring::default_provider().install_default();

        let program_id = self.program_type.to_pubkey();
        let commitment_level = CommitmentLevel::from_str_name(&self.commitment.to_uppercase())
            .ok_or_else(|| DataSourceError::InvalidCommitment {
                value: self.commitment.clone(),
            })?;

        info!(
            "Starting Yellowstone datasource for program {:?} (ID: {}) at {} (commitment: {:?})",
            self.program_type, program_id, self.endpoint, commitment_level
        );

        let endpoint = self.endpoint.clone();
        let x_token = self.x_token.clone();
        let program_type = self.program_type;
        let escrow_instance_id = self.escrow_instance_id;
        let health = self.health.clone();

        #[cfg(feature = "datasource-rpc")]
        let rpc_poller = self.rpc_poller.clone();
        #[cfg(feature = "datasource-rpc")]
        let max_gap_slots = self.max_gap_slots;
        #[cfg(feature = "datasource-rpc")]
        let batch_size = self.batch_size;
        #[cfg(feature = "datasource-rpc")]
        let storage = self.storage.clone();

        let handle = tokio::spawn(async move {
            loop {
                if cancellation_token.is_cancelled() {
                    info!("Yellowstone source received cancellation signal, stopping...");
                    break;
                }

                match connect_and_stream(
                    &endpoint,
                    x_token.clone(),
                    commitment_level,
                    program_type,
                    escrow_instance_id,
                    tx.clone(),
                    cancellation_token.clone(),
                    health.as_ref(),
                )
                .await
                {
                    Ok(_) => {
                        info!("Yellowstone gRPC stream ended, reconnecting...");
                        metrics::INDEXER_DATASOURCE_RECONNECTS
                            .with_label_values(&[program_type.as_label()])
                            .inc();
                    }
                    Err(e) => {
                        let error_msg = format!("{}", e);
                        error!(
                            "Yellowstone gRPC error: {}, reconnecting in 5s...",
                            error_msg
                        );
                        metrics::INDEXER_RPC_ERRORS
                            .with_label_values(&[program_type.as_label(), "stream"])
                            .inc();
                        metrics::INDEXER_DATASOURCE_RECONNECTS
                            .with_label_values(&[program_type.as_label()])
                            .inc();
                        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                    }
                }

                #[cfg(feature = "datasource-rpc")]
                {
                    // Anchor on the durable checkpoint, not an in-memory watermark.
                    // BlockMeta(S) can race partial tx delivery, so replay must include S itself.
                    // Tx/mint inserts are idempotent, so replaying the boundary slot is safe.
                    if let (Some(ref poller), Some(ref storage)) = (&rpc_poller, &storage) {
                        let checkpoint = match get_last_checkpoint(storage, program_type).await {
                            Ok(slot) => slot,
                            Err(e) => {
                                warn!(
                                    "Reconnect gap-fill skipped: failed to read checkpoint: {}",
                                    e
                                );
                                // Backoff so a persistent storage outage paired with a
                                // fast-failing Yellowstone endpoint can't spin the loop.
                                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                                continue;
                            }
                        };

                        if checkpoint == 0 {
                            // Fresh system, startup backfill handles initial catch-up.
                            continue;
                        }

                        match try_fill_reconnect_gap(
                            checkpoint,
                            poller,
                            max_gap_slots,
                            batch_size,
                            program_type,
                            escrow_instance_id,
                            &tx,
                        )
                        .await
                        {
                            Ok(filled) => {
                                if filled > 0 {
                                    info!(
                                        "Reconnect gap-fill complete: {} slots backfilled \
                                         (from checkpoint {})",
                                        filled, checkpoint
                                    );
                                }
                            }
                            Err(DataSourceError::GapFillFailed { ref reason })
                                if reason.contains("Gap too large") =>
                            {
                                error!(
                                    "Reconnect gap too large (checkpoint: {}): {}. \
                                     Operator should investigate; next startup backfill will catch it.",
                                    checkpoint, reason
                                );
                            }
                            Err(e) => {
                                warn!(
                                    "Reconnect gap-fill failed (checkpoint: {}): {}. Continuing reconnect.",
                                    checkpoint, e
                                );
                            }
                        }
                    }
                }
            }

            info!("Yellowstone source stopped gracefully");
        });

        Ok(handle)
    }

    async fn shutdown(&mut self) -> Result<(), DataSourceError> {
        info!("Yellowstone source shutdown requested (gRPC connection will be closed by cancellation)");
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
async fn connect_and_stream(
    endpoint: &str,
    x_token: Option<String>,
    commitment: CommitmentLevel,
    program_type: ProgramType,
    escrow_instance_id: Option<Pubkey>,
    tx: InstructionSender,
    cancellation_token: CancellationToken,
    health: Option<&Arc<private_channel_metrics::HealthState>>,
) -> Result<(), DataSourceError> {
    let mut client = GeyserGrpcClient::build_from_shared(endpoint.to_string())
        .map_err(|e| DataSourceRpcError::Protocol {
            reason: e.to_string(),
        })?
        .x_token(x_token)
        .map_err(|e| DataSourceRpcError::Protocol {
            reason: e.to_string(),
        })?
        .tls_config(ClientTlsConfig::new().with_native_roots())
        .map_err(|e| DataSourceRpcError::Protocol {
            reason: e.to_string(),
        })?
        .connect()
        .await
        .map_err(|e| DataSourceRpcError::Protocol {
            reason: e.to_string(),
        })?;

    let program_id = program_type.to_pubkey();

    info!("Connected to Yellowstone gRPC at {}", endpoint);

    // Subscribe to transactions for our program
    // Always put program_id in account_required
    // If escrow_instance_id is provided, also add it to account_required
    let mut account_required = vec![program_id.to_string()];
    if let Some(instance_id) = escrow_instance_id {
        account_required.push(instance_id.to_string());
    }

    let mut transaction_filters = HashMap::new();
    transaction_filters.insert(
        "private_channel_program".to_string(),
        SubscribeRequestFilterTransactions {
            vote: Some(false),
            failed: Some(false),
            signature: None,
            account_include: vec![],
            account_exclude: vec![],
            account_required,
        },
    );

    // Subscribe to ALL block metadata for slot completion
    let mut blocks_meta = HashMap::new();
    blocks_meta.insert(
        "all_blocks_meta".to_string(),
        SubscribeRequestFilterBlocksMeta {},
    );

    let subscribe_request = SubscribeRequest {
        slots: HashMap::new(),
        accounts: HashMap::new(),
        transactions: transaction_filters,
        transactions_status: HashMap::new(),
        entry: HashMap::new(),
        blocks: HashMap::new(),
        blocks_meta,
        commitment: Some(commitment as i32),
        accounts_data_slice: vec![],
        ping: None,
        from_slot: None,
    };

    info!(
        "Subscribing to Yellowstone gRPC with transactions (program: {}) + blocks_meta (all slots)",
        program_id.to_string()
    );

    let (mut subscribe_tx, mut stream) = client
        .subscribe_with_request(Some(subscribe_request))
        .await
        .map_err(|e| DataSourceRpcError::Protocol {
            reason: e.to_string(),
        })?;

    loop {
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                info!("Yellowstone stream cancelled, closing connection...");
                drop(stream);
                drop(subscribe_tx);
                info!("Yellowstone gRPC connection closed");
                break;
            }
            message = stream.next() => {
                match message {
                    None => break,
                    Some(message) => match message {
            Ok(msg) => match msg.update_oneof {
                Some(UpdateOneof::Transaction(tx_update)) => {
                    if let Err(e) =
                        handle_transaction(tx_update, &program_id, program_type, &tx).await
                    {
                        error!("Error handling transaction: {}", e);
                        // Convert RpcError to DataSourceError for consistency
                        return Err(DataSourceError::Rpc(e));
                    }
                }
                Some(UpdateOneof::BlockMeta(block_meta)) => {
                    metrics::INDEXER_CHAIN_TIP_SLOT
                        .with_label_values(&[program_type.as_label()])
                        .set(block_meta.slot as f64);
                    if let Some(h) = health {
                        // Yellowstone is push-based — a BlockMeta per slot means
                        // we're caught up; pending stays 0. The continuous_progress
                        // flag in HealthConfig::indexer() makes the staleness check
                        // fire even at pending=0, so a dead stream is detected.
                        h.set_pending(0);
                    }
                    debug!("Yellowstone BlockMeta for slot {}", block_meta.slot);

                    let res = send_guaranteed(
                        &tx,
                        ProcessorMessage::SlotComplete {
                            slot: block_meta.slot,
                            program_type,
                        },
                        "SlotComplete (yellowstone)",
                    )
                    .await;
                    if let Err(e) = res {
                        error!(
                            "SlotComplete send failed, stopping Yellowstone gracefully: {}",
                            e
                        );
                        break;
                    }
                }
                Some(UpdateOneof::Ping(_)) => {
                    subscribe_tx
                        .send(SubscribeRequest {
                            ping: Some(SubscribeRequestPing { id: 1 }),
                            ..Default::default()
                        })
                        .await
                        .map_err(|e| DataSourceRpcError::Protocol {
                            reason: e.to_string(),
                        })?;
                }
                _ => {}
            },
            Err(error) => {
                error!("Geyser stream error: {error:?}");
                metrics::INDEXER_RPC_ERRORS
                    .with_label_values(&[program_type.as_label(), "stream"])
                    .inc();
                return Err(DataSourceRpcError::Protocol {
                    reason: format!("Stream error: {:?}", error),
                }.into());
            }
                    }
                }
            }
        }
    }

    Ok(())
}

#[cfg(all(test, feature = "datasource-rpc"))]
mod tests {
    use super::*;
    use crate::indexer::datasource::rpc_polling::rpc::RpcPoller;
    use mockito::Server;
    use serde_json::json;
    use solana_sdk::commitment_config::CommitmentLevel;
    use solana_transaction_status::UiTransactionEncoding;
    use tokio::sync::mpsc;

    fn empty_block_json() -> serde_json::Value {
        json!({
            "blockhash": "TestBlockHash11111111111111111111111111111",
            "parentSlot": 0,
            "transactions": []
        })
    }

    fn mock_get_slot(server: &mut Server, slot: u64) -> mockito::Mock {
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(json!({
                "method": "getSlot"
            })))
            .with_status(200)
            .with_body(
                json!({
                    "jsonrpc": "2.0",
                    "result": slot,
                    "id": 1
                })
                .to_string(),
            )
            .create()
    }

    fn mock_get_slot_error(server: &mut Server) -> mockito::Mock {
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(json!({
                "method": "getSlot"
            })))
            .with_status(200)
            .with_body(
                json!({
                    "jsonrpc": "2.0",
                    "error": { "code": -32600, "message": "Invalid request" },
                    "id": 1
                })
                .to_string(),
            )
            .create()
    }

    fn mock_get_block_success(server: &mut Server, slot: u64) -> mockito::Mock {
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(json!({
                "method": "getBlock",
                "params": [slot]
            })))
            .with_status(200)
            .with_body(
                json!({
                    "jsonrpc": "2.0",
                    "result": empty_block_json(),
                    "id": 1
                })
                .to_string(),
            )
            .create()
    }

    #[tokio::test]
    async fn try_fill_reconnect_gap_no_gap() {
        let mut server = Server::new_async().await;

        let _m = mock_get_slot(&mut server, 100);

        let poller = RpcPoller::new(
            server.url(),
            UiTransactionEncoding::Json,
            CommitmentLevel::Finalized,
        );

        let (tx, _rx) = mpsc::channel(64);
        let result =
            try_fill_reconnect_gap(100, &poller, 1000, 10, ProgramType::Escrow, None, &tx).await;

        assert_eq!(result.unwrap(), 0);
    }

    #[tokio::test]
    async fn try_fill_reconnect_gap_fills_gap() {
        let mut server = Server::new_async().await;

        // checkpoint = 100, current_slot = 103 → replay anchor = 99,
        // fill_slot_range emits slots 100..=103 (boundary slot included).
        let _m_slot = mock_get_slot(&mut server, 103);
        let _m0 = mock_get_block_success(&mut server, 100);
        let _m1 = mock_get_block_success(&mut server, 101);
        let _m2 = mock_get_block_success(&mut server, 102);
        let _m3 = mock_get_block_success(&mut server, 103);

        let poller = RpcPoller::new(
            server.url(),
            UiTransactionEncoding::Json,
            CommitmentLevel::Finalized,
        );

        let (tx, mut rx) = mpsc::channel(64);
        let result =
            try_fill_reconnect_gap(100, &poller, 1000, 10, ProgramType::Escrow, None, &tx).await;

        assert_eq!(result.unwrap(), 4);
        drop(tx);

        let mut slots = vec![];
        while let Some(msg) = rx.recv().await {
            if let ProcessorMessage::SlotComplete { slot, .. } = msg {
                slots.push(slot);
            }
        }

        assert_eq!(slots, vec![100, 101, 102, 103]);
    }

    #[tokio::test]
    async fn try_fill_reconnect_gap_too_large() {
        let mut server = Server::new_async().await;

        let _m = mock_get_slot(&mut server, 200);

        let poller = RpcPoller::new(
            server.url(),
            UiTransactionEncoding::Json,
            CommitmentLevel::Finalized,
        );

        let (tx, _rx) = mpsc::channel(64);
        let result =
            try_fill_reconnect_gap(100, &poller, 10, 10, ProgramType::Escrow, None, &tx).await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        let err_str = err.to_string();
        assert!(
            err_str.contains("Gap too large"),
            "Expected 'Gap too large' in error: {}",
            err_str
        );
    }

    #[tokio::test]
    async fn try_fill_reconnect_gap_rpc_failure() {
        let mut server = Server::new_async().await;

        let _m = mock_get_slot_error(&mut server);

        let poller = RpcPoller::new(
            server.url(),
            UiTransactionEncoding::Json,
            CommitmentLevel::Finalized,
        );

        let (tx, _rx) = mpsc::channel(64);
        let result =
            try_fill_reconnect_gap(100, &poller, 1000, 10, ProgramType::Escrow, None, &tx).await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        let err_str = err.to_string();
        assert!(
            err_str.contains("Failed to get latest slot"),
            "Expected 'Failed to get latest slot' in error: {}",
            err_str
        );
    }
}

async fn handle_transaction(
    tx_update: yellowstone_grpc_proto::geyser::SubscribeUpdateTransaction,
    program_id: &Pubkey,
    program_type: ProgramType,
    channel: &InstructionSender,
) -> Result<(), DataSourceRpcError> {
    let slot = tx_update.slot;

    let tx_info = tx_update
        .transaction
        .ok_or_else(|| DataSourceRpcError::Protocol {
            reason: "Missing transaction info".to_string(),
        })?;

    let mut inner_instructions_vec: Vec<InnerInstructions> = vec![];

    if let Some(meta) = &tx_info.meta {
        inner_instructions_vec = meta
            .inner_instructions
            .iter()
            .map(|ix_set| InnerInstructions {
                index: ix_set.index as u8,
                instructions: ix_set
                    .instructions
                    .iter()
                    .map(|ix| CompiledInstruction {
                        program_id_index: ix.program_id_index as u8,
                        accounts: ix.accounts.clone(),
                        data: bs58::encode(&ix.data).into_string(),
                    })
                    .collect(),
            })
            .collect();
    }

    // Extract signature
    let signature = bs58::encode(&tx_info.signature).into_string();

    // Convert protobuf transaction to Solana types
    let proto_tx = tx_info
        .transaction
        .ok_or_else(|| DataSourceRpcError::Protocol {
            reason: "Missing transaction".to_string(),
        })?;
    let proto_message = proto_tx
        .message
        .ok_or_else(|| DataSourceRpcError::Protocol {
            reason: "Missing message".to_string(),
        })?;
    let versioned_message =
        create_message(proto_message).map_err(|e| DataSourceRpcError::Protocol {
            reason: format!("Failed to create message: {}", e),
        })?;

    // Get account keys and instructions
    let (account_keys, instructions): (
        Vec<Pubkey>,
        Vec<solana_sdk::message::compiled_instruction::CompiledInstruction>,
    ) = match &versioned_message {
        VersionedMessage::Legacy(msg) => (msg.account_keys.clone(), msg.instructions.clone()),
        VersionedMessage::V0(msg) => (msg.account_keys.clone(), msg.instructions.clone()),
    };

    info!(
        "Yellowstone received transaction at slot {}, signature: {}, {} instructions",
        slot,
        signature,
        instructions.len()
    );

    // Parse each instruction that belongs to our program
    for instruction in instructions {
        let program_id_index = instruction.program_id_index as usize;
        if program_id_index >= account_keys.len() {
            error!(
                "Invalid program_id_index {} for transaction {}",
                program_id_index, signature
            );
            continue;
        }

        let ix_program_id = account_keys[program_id_index];
        if ix_program_id != *program_id {
            continue; // Not our program
        }

        // Convert to our CompiledInstruction type (from types.rs)
        let compiled_ix = CompiledInstruction {
            program_id_index: instruction.program_id_index,
            accounts: instruction.accounts.clone(),
            data: bs58::encode(&instruction.data).into_string(),
        };

        // Parse instruction based on program type and handle immediately to avoid Send issues
        let instruction_data = match program_type {
            ProgramType::Escrow => {
                match parse_escrow_instruction(&compiled_ix, &account_keys, &inner_instructions_vec)
                {
                    Ok(Some(inst)) => Some(ProgramInstruction::Escrow(Box::new(inst))),
                    Ok(None) => {
                        debug!(
                            "Yellowstone: Unsupported escrow instruction at slot {}",
                            slot
                        );
                        None
                    }
                    Err(e) => {
                        error!("Failed to parse escrow instruction at slot {}: {}", slot, e);
                        None
                    }
                }
            }
            ProgramType::Withdraw => {
                match parse_withdraw_instruction(
                    &compiled_ix,
                    &account_keys,
                    &inner_instructions_vec,
                ) {
                    Ok(Some(inst)) => Some(ProgramInstruction::Withdraw(Box::new(inst))),
                    Ok(None) => {
                        debug!(
                            "Yellowstone: Unsupported withdraw instruction at slot {}",
                            slot
                        );
                        None
                    }
                    Err(e) => {
                        error!(
                            "Failed to parse withdraw instruction at slot {}: {}",
                            slot, e
                        );
                        None
                    }
                }
            }
        };

        if let Some(instruction_data) = instruction_data {
            let instruction_meta = InstructionWithMetadata {
                instruction: instruction_data,
                slot,
                program_type,
                signature: Some(signature.clone()),
            };

            let res = send_guaranteed(
                channel,
                ProcessorMessage::Instruction(instruction_meta),
                "instruction (yellowstone)",
            )
            .await;
            if let Err(e) = res {
                return Err(DataSourceRpcError::Protocol {
                    reason: format!("Instruction send failed: {}", e),
                });
            }
        }
    }

    Ok(())
}
