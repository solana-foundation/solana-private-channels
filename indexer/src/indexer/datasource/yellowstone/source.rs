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
use tracing::{debug, error, info, warn};
use yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcClient};
use yellowstone_grpc_proto::geyser::{
    subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest, SubscribeRequestFilterBlocks,
    SubscribeRequestFilterSlots, SubscribeRequestPing, SubscribeUpdateTransactionInfo,
};

use crate::channel_utils::send_guaranteed;
use crate::config::ProgramType;
use crate::error::{DataSourceError, DataSourceRpcError};
use crate::indexer::datasource::common::parser::escrow::parse_escrow_instruction;
use crate::indexer::datasource::common::parser::withdraw::parse_withdraw_instruction;
use crate::indexer::datasource::common::{datasource::DataSource, types::*};
use crate::indexer::datasource::rpc_polling::types::{InnerInstruction, InnerInstructions};
use crate::storage::Storage;

#[cfg(feature = "datasource-rpc")]
use crate::error::{BackfillError, CheckpointError};
#[cfg(feature = "datasource-rpc")]
use crate::indexer::{
    backfill::{fill_slot_range, validate_gap},
    checkpoint::get_last_checkpoint,
    datasource::rpc_polling::rpc::RpcPoller,
};
#[cfg(feature = "datasource-rpc")]
use std::future::Future;
#[cfg(feature = "datasource-rpc")]
use std::time::Duration;

/// Yellowstone gRPC datasource - subscribes to atomic per-slot blocks
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
    #[cfg(feature = "datasource-rpc")]
    startup_floor: Option<u64>,
    health: Option<Arc<private_channel_metrics::HealthState>>,
    /// Silent-stream watchdog window. Defaults to STREAM_STALL_TIMEOUT; overridable
    /// (mainly so tests can drive the reconnect path without a 120s wait).
    stall_timeout: std::time::Duration,
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
            #[cfg(feature = "datasource-rpc")]
            startup_floor: None,
            health: None,
            stall_timeout: STREAM_STALL_TIMEOUT,
        }
    }

    pub fn with_health(mut self, health: Arc<private_channel_metrics::HealthState>) -> Self {
        self.health = Some(health);
        self
    }

    /// Override the silent-stream watchdog window (mainly for tests).
    pub fn with_stall_timeout(mut self, stall_timeout: std::time::Duration) -> Self {
        self.stall_timeout = stall_timeout;
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

    /// Holds the durable checkpoint that anchors gap repair; without it the gate never arms.
    #[cfg(feature = "datasource-rpc")]
    pub fn with_storage(mut self, storage: Arc<Storage>) -> Self {
        self.storage = Some(storage);
        self
    }

    /// Highest slot startup already owns, taken once by whichever connection arms the gate first.
    /// It raises that gate's target so a cold start cannot narrow a startup gate, and bounds the fill.
    #[cfg(feature = "datasource-rpc")]
    pub fn with_startup_floor(mut self, slot: u64) -> Self {
        self.startup_floor = Some(slot);
        self
    }
}

#[cfg(feature = "datasource-rpc")]
const RECONNECT_GAP_RETRY_BACKOFF: Duration = Duration::from_secs(5);

// Stall recovery: a half-open stream (peer stops sending, no FIN/error) hangs
// stream.next() forever. Two independent backstops force a reconnect.

/// HTTP/2 keepalive interval: send a PING to the peer this often so a dead transport
/// surfaces as a stream error instead of hanging. Paired with keep_alive_while_idle so
/// pings fire even when no blocks stream (escrow blocks are sparse on quiet clusters).
const GRPC_KEEPALIVE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(15);

/// Tear the connection down if a keepalive PING goes unanswered this long.
const GRPC_KEEPALIVE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Application-level watchdog: force a reconnect if no message of ANY kind (a block or
/// a server ping) arrives within this window. Backstops the h2 keepalive for a server
/// that keeps the socket up but wedges mid-stream. When escrow is idle, server pings are
/// the only regular signal, so this sits well above any reasonable ping cadence to avoid
/// tearing down a healthy but quiet stream. If idle false-reconnects ever show up in the
/// reconnect metric, subscribe to slots for a per-slot heartbeat instead of widening this.
const STREAM_STALL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);

/// Terminal states of the reconnect gap repair; there is no error variant because
/// failures retry inside the loop instead of falling through to a resubscribe.
#[cfg(feature = "datasource-rpc")]
#[derive(Debug, PartialEq, Eq)]
enum GapFillOutcome {
    Resolved,
    Cancelled,
}

/// Repairs the reconnect gap (checkpoint, target] up to the fixed observed resume slot,
/// returning Resolved only once every slot is indexed or proven empty; failures retry in
/// place with backoff. The gate keeps the checkpoint frozen until this completes (an
/// un-fillable slot fails closed). Cancellation wins every wait.
#[cfg(feature = "datasource-rpc")]
#[allow(clippy::too_many_arguments)]
async fn fill_reconnect_gap_to<F, Fut>(
    target: u64,
    floor: Option<u64>,
    get_checkpoint: F,
    rpc_poller: &RpcPoller,
    max_gap_slots: u64,
    batch_size: usize,
    program_type: ProgramType,
    escrow_instance_id: Option<Pubkey>,
    instruction_tx: &InstructionSender,
    cancel: &CancellationToken,
    backoff: Duration,
) -> GapFillOutcome
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<Option<u64>, CheckpointError>>,
{
    loop {
        if cancel.is_cancelled() {
            return GapFillOutcome::Cancelled;
        }

        // Re-read every cycle so an operator resync moves the anchor without a restart. A cold
        // start keeps its floor as the lower bound, so a stale checkpoint cannot drag it down.
        let checkpoint = match get_checkpoint().await {
            Ok(Some(slot)) => floor.map_or(slot, |f| f.max(slot)),
            // With no anchor there is no lower bound to replay from, so resolving here
            // would forward live slots over an interval nobody read. Retry instead.
            Ok(None) => match floor {
                Some(slot) => slot,
                None => {
                    error!(
                        "Reconnect gap-fill: no durable checkpoint anchor for {:?}; holding the \
                         checkpoint rather than skipping the gap",
                        program_type
                    );
                    record_missing_anchor(program_type);
                    if backoff_cancelled(cancel, backoff).await {
                        return GapFillOutcome::Cancelled;
                    }
                    continue;
                }
            },
            // The floor is a durable bound of its own, so a failed read need not stall a cold start.
            Err(e) => match floor {
                Some(slot) => slot,
                None => {
                    warn!(
                        "Reconnect gap-fill: checkpoint read failed, retrying: {}",
                        e
                    );
                    record_gap_fill_error(program_type);
                    if backoff_cancelled(cancel, backoff).await {
                        return GapFillOutcome::Cancelled;
                    }
                    continue;
                }
            },
        };

        let gap = match validate_gap(target, checkpoint, max_gap_slots) {
            Ok(None) => {
                info!(
                    "No reconnect gap to fill. Target: {}, checkpoint: {}",
                    target, checkpoint
                );
                return GapFillOutcome::Resolved;
            }
            Ok(Some(gap)) => gap,
            // Retrying cannot shrink an oversized gap (validate_gap is endpoint-independent),
            // so hold the checkpoint frozen and stay loud until an operator intervenes.
            Err(BackfillError::GapTooLarge { gap, max_gap }) => {
                error!(
                    "Reconnect gap too large: {} slots (max {}). Checkpoint frozen at {}; \
                     resync to advance the checkpoint (the loop recovers without a restart \
                     once the gap is within bounds), or raise backfill.max_gap_slots and restart.",
                    gap, max_gap, checkpoint
                );
                record_gap_fill_error(program_type);
                if backoff_cancelled(cancel, backoff).await {
                    return GapFillOutcome::Cancelled;
                }
                continue;
            }
            Err(e) => {
                warn!("Reconnect gap validation failed, retrying: {}", e);
                record_gap_fill_error(program_type);
                if backoff_cancelled(cancel, backoff).await {
                    return GapFillOutcome::Cancelled;
                }
                continue;
            }
        };

        // The boundary slot may be half-forwarded when the stream died, so replay
        // from checkpoint-1 to include it; inserts are idempotent.
        let replay_anchor = checkpoint.saturating_sub(1);
        info!(
            "Reconnect gap: {} slots (replaying from {} to {}). Backfilling...",
            gap, replay_anchor, target
        );

        match fill_slot_range(
            rpc_poller,
            replay_anchor,
            target,
            batch_size,
            program_type,
            escrow_instance_id,
            instruction_tx,
        )
        .await
        {
            Ok(filled) => {
                info!(
                    "Reconnect gap-fill complete: {} slots backfilled (from checkpoint {})",
                    filled, checkpoint
                );
                return GapFillOutcome::Resolved;
            }
            Err(e) => {
                warn!("Reconnect gap-fill failed, retrying: {}", e);
                record_gap_fill_error(program_type);
            }
        }

        if backoff_cancelled(cancel, backoff).await {
            return GapFillOutcome::Cancelled;
        }
    }
}

/// What `connect_and_stream` needs to arm the gate and backfill on a connection's first block.
/// Built once per source task; `None` when storage or the poller is missing, disabling repair.
#[cfg(feature = "datasource-rpc")]
struct ReconnectGapCtx {
    poller: Arc<RpcPoller>,
    storage: Arc<Storage>,
    max_gap_slots: u64,
    batch_size: usize,
    program_type: ProgramType,
    escrow_instance_id: Option<Pubkey>,
}

/// Gate the checkpoint up to the slot this stream opened at, or to `floor` when that is higher,
/// then spawn a backfill for the window. Sent first so the gate beats that block's SlotComplete.
#[cfg(feature = "datasource-rpc")]
/// Returns whether it is safe to forward the block that triggered this arm. `false`
/// means cancellation ended the wait before the gate was armed, so the caller must NOT
/// forward the block: its SlotComplete would advance the checkpoint over the still
/// unfilled gap, and every slot underneath it would stop being reachable. `true` means
/// the gate is armed and the block is safe to hand on.
async fn arm_reconnect_gap(
    t_sub: u64,
    floor: &mut Option<u64>,
    ctx: &ReconnectGapCtx,
    tx: &InstructionSender,
    cancel: &CancellationToken,
    backoff: Duration,
    prev: &mut Option<tokio::task::JoinHandle<()>>,
) -> Result<bool, DataSourceRpcError> {
    // Held rather than taken: every path that leaves without a gate must return it unspent.
    let floor_slot = *floor;
    // Never below what startup still owes, so its gate is widened rather than narrowed.
    let target = floor_slot.map_or(t_sub, |slot| t_sub.max(slot));

    // Resolve the anchor before arming, because the gate seeds its frontier from it.
    let checkpoint = loop {
        match get_last_checkpoint(&ctx.storage, ctx.program_type).await {
            Ok(Some(slot)) => break slot,
            // Slot zero is a real anchor and repairs like any other, but absence has no lower
            // bound, so forwarding here would carry the checkpoint over unread slots. Hold.
            Ok(None) => {
                error!(
                    "Reconnect gap: no durable checkpoint anchor for {:?}; withholding live \
                     slots until one exists",
                    ctx.program_type
                );
                record_missing_anchor(ctx.program_type);
                if backoff_cancelled(cancel, backoff).await {
                    return Ok(false);
                }
            }
            Err(e) => {
                warn!("Reconnect gap: checkpoint read failed, retrying: {}", e);
                record_gap_fill_error(ctx.program_type);
                if backoff_cancelled(cancel, backoff).await {
                    return Ok(false);
                }
            }
        }
    };

    send_guaranteed(
        tx,
        ProcessorMessage::Regate {
            program_type: ctx.program_type,
            from: checkpoint,
            target,
        },
        "Regate (yellowstone)",
    )
    .await
    .map_err(|e| DataSourceRpcError::Protocol {
        reason: format!("Regate send failed: {e}"),
    })?;

    // Supersede any prior in-flight backfill; the new one covers the superset range.
    if let Some(handle) = prev.take() {
        handle.abort();
    }

    let poller = ctx.poller.clone();
    let storage = ctx.storage.clone();
    let program_type = ctx.program_type;
    let escrow_instance_id = ctx.escrow_instance_id;
    let max_gap_slots = ctx.max_gap_slots;
    let batch_size = ctx.batch_size;
    let tx = tx.clone();
    let cancel = cancel.clone();

    let handle = tokio::spawn(async move {
        fill_reconnect_gap_to(
            target,
            floor_slot,
            || get_last_checkpoint(&storage, program_type),
            &poller,
            max_gap_slots,
            batch_size,
            program_type,
            escrow_instance_id,
            &tx,
            &cancel,
            backoff,
        )
        .await;
    });
    *prev = Some(handle);
    // The gate is set and the fill owns the range, so no later connection may claim it again.
    *floor = None;
    Ok(true)
}

/// Waits one backoff period; true means cancellation fired first.
#[cfg(feature = "datasource-rpc")]
async fn backoff_cancelled(cancel: &CancellationToken, backoff: Duration) -> bool {
    tokio::select! {
        _ = cancel.cancelled() => true,
        _ = tokio::time::sleep(backoff) => false,
    }
}

#[cfg(feature = "datasource-rpc")]
fn record_gap_fill_error(program_type: ProgramType) {
    metrics::INDEXER_RPC_ERRORS
        .with_label_values(&[program_type.as_label(), "gap_fill"])
        .inc();
}

/// Counted apart from a gap-fill error because the cause and the fix differ: this one means
/// the recovery anchor is missing and live slots are being held back, which needs the anchor
/// restored, while a gap-fill error means the RPC endpoint is misbehaving and usually clears
/// on its own. Sharing one label would page the wrong person.
#[cfg(feature = "datasource-rpc")]
fn record_missing_anchor(program_type: ProgramType) {
    metrics::INDEXER_RPC_ERRORS
        .with_label_values(&[program_type.as_label(), "missing_anchor"])
        .inc();
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
        // Only the reconnect gap-fill (RPC path) still needs the instance id.
        #[cfg(feature = "datasource-rpc")]
        let escrow_instance_id = self.escrow_instance_id;
        let health = self.health.clone();
        let stall_timeout = self.stall_timeout;

        #[cfg(feature = "datasource-rpc")]
        let rpc_poller = self.rpc_poller.clone();
        #[cfg(feature = "datasource-rpc")]
        let max_gap_slots = self.max_gap_slots;
        #[cfg(feature = "datasource-rpc")]
        let batch_size = self.batch_size;
        #[cfg(feature = "datasource-rpc")]
        let storage = self.storage.clone();
        #[cfg(feature = "datasource-rpc")]
        let startup_floor = self.startup_floor;

        let handle = tokio::spawn(async move {
            // Gap gate context, built once; None disables gap gating for this source.
            #[cfg(feature = "datasource-rpc")]
            let gap_ctx_opt = match (rpc_poller, storage) {
                (Some(poller), Some(storage)) => Some(ReconnectGapCtx {
                    poller,
                    storage,
                    max_gap_slots,
                    batch_size,
                    program_type,
                    escrow_instance_id,
                }),
                _ => None,
            };
            // One in-flight gap backfill, owned here and superseded on each connection.
            #[cfg(feature = "datasource-rpc")]
            let mut backfill_handle: Option<tokio::task::JoinHandle<()>> = None;
            // Taken by whichever connection arms first; every later one anchors on the checkpoint.
            #[cfg(feature = "datasource-rpc")]
            let mut startup_floor = startup_floor;

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
                    tx.clone(),
                    cancellation_token.clone(),
                    health.as_ref(),
                    stall_timeout,
                    #[cfg(feature = "datasource-rpc")]
                    gap_ctx_opt.as_ref(),
                    #[cfg(feature = "datasource-rpc")]
                    &mut backfill_handle,
                    #[cfg(feature = "datasource-rpc")]
                    &mut startup_floor,
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
                        // Cancellation-aware backoff so shutdown does not wait out the 5s.
                        tokio::select! {
                            _ = cancellation_token.cancelled() => {}
                            _ = tokio::time::sleep(tokio::time::Duration::from_secs(5)) => {}
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
    tx: InstructionSender,
    cancellation_token: CancellationToken,
    health: Option<&Arc<private_channel_metrics::HealthState>>,
    stall_timeout: std::time::Duration,
    // Set on every connection, the first one included, so that connection's first live block
    // gates the window below it. A process start and a stream resume are the same event here.
    #[cfg(feature = "datasource-rpc")] gap_ctx: Option<&ReconnectGapCtx>,
    #[cfg(feature = "datasource-rpc")] backfill_handle: &mut Option<tokio::task::JoinHandle<()>>,
    // The slot startup already owns, consumed by whichever connection arms the gate first.
    #[cfg(feature = "datasource-rpc")] startup_floor: &mut Option<u64>,
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
        .http2_keep_alive_interval(GRPC_KEEPALIVE_INTERVAL)
        .keep_alive_timeout(GRPC_KEEPALIVE_TIMEOUT)
        .keep_alive_while_idle(true)
        .connect()
        .await
        .map_err(|e| DataSourceRpcError::Protocol {
            reason: e.to_string(),
        })?;

    let program_id = program_type.to_pubkey();

    info!("Connected to Yellowstone gRPC at {}", endpoint);

    // One blocks message carries a slot plus all its transactions, so a completion never races a tx.
    // program-id is the only filter blocks allows; instance scoping happens later in the parser.
    let mut blocks = HashMap::new();
    blocks.insert(
        "private_channel_blocks".to_string(),
        SubscribeRequestFilterBlocks {
            account_include: vec![program_id.to_string()],
            include_transactions: Some(true),
            include_accounts: Some(false),
            include_entries: Some(false),
        },
    );

    // Slots carry the point the stream resumed at, which is what the gap gate arms on. Blocks
    // are program-filtered, so waiting for one would measure idle time instead of the outage.
    let mut slots = HashMap::new();
    slots.insert(
        "private_channel_slots".to_string(),
        SubscribeRequestFilterSlots {
            // Same commitment as the blocks, so the armed target is a slot they can reach.
            filter_by_commitment: Some(true),
            interslot_updates: Some(false),
        },
    );

    let subscribe_request = SubscribeRequest {
        slots,
        accounts: HashMap::new(),
        transactions: HashMap::new(),
        transactions_status: HashMap::new(),
        entry: HashMap::new(),
        blocks,
        blocks_meta: HashMap::new(),
        commitment: Some(commitment as i32),
        accounts_data_slice: vec![],
        ping: None,
        from_slot: None,
    };

    info!(
        "Subscribing to Yellowstone gRPC blocks (program: {})",
        program_id.to_string()
    );

    let (mut subscribe_tx, mut stream) = client
        .subscribe_with_request(Some(subscribe_request))
        .await
        .map_err(|e| DataSourceRpcError::Protocol {
            reason: e.to_string(),
        })?;

    // Arm the gap gate on the first live block of this connection only.
    #[cfg(feature = "datasource-rpc")]
    let mut armed = false;

    loop {
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                info!("Yellowstone stream cancelled, closing connection...");
                drop(stream);
                drop(subscribe_tx);
                info!("Yellowstone gRPC connection closed");
                break;
            }
            // Fresh timer each iteration, so any inbound message resets it. Fires only
            // when the stream goes fully silent; returns Err to take the reconnect +
            // gap-fill path (which replays whatever slots were missed while wedged).
            _ = tokio::time::sleep(stall_timeout) => {
                warn!(
                    "Yellowstone stream stalled: no message in {:?}, forcing reconnect",
                    stall_timeout
                );
                metrics::INDEXER_RPC_ERRORS
                    .with_label_values(&[program_type.as_label(), "stall"])
                    .inc();
                return Err(DataSourceRpcError::Protocol {
                    reason: format!("stream stalled: no message in {stall_timeout:?}"),
                }
                .into());
            }
            message = stream.next() => {
                match message {
                    None => break,
                    Some(message) => match message {
            Ok(msg) => match msg.update_oneof {
                // Where this stream actually resumed, so the gap is the outage and nothing
                // else. Slots carry no transactions, so none is forwarded from here.
                Some(UpdateOneof::Slot(slot_update)) => {
                    #[cfg(feature = "datasource-rpc")]
                    if let Some(ctx) = gap_ctx {
                        if !armed {
                            armed = true;
                            let safe_to_forward = arm_reconnect_gap(
                                slot_update.slot,
                                startup_floor,
                                ctx,
                                &tx,
                                &cancellation_token,
                                RECONNECT_GAP_RETRY_BACKOFF,
                                backfill_handle,
                            )
                            .await
                            .map_err(DataSourceError::Rpc)?;
                            // Cancelled mid-arm, so stop rather than stream on ungated.
                            if !safe_to_forward {
                                break;
                            }
                        }
                    }
                    #[cfg(not(feature = "datasource-rpc"))]
                    let _ = slot_update;
                }
                Some(UpdateOneof::Block(block)) => {
                    metrics::INDEXER_CHAIN_TIP_SLOT
                        .with_label_values(&[program_type.as_label()])
                        .set(block.slot as f64);
                    if let Some(h) = health {
                        // Yellowstone is push-based - a block per produced slot means
                        // we're caught up; pending stays 0. The continuous_progress
                        // flag in HealthConfig::indexer() makes the staleness check
                        // fire even at pending=0, so a dead stream is detected.
                        h.set_pending(0);
                    }
                    debug!(
                        "Yellowstone Block for slot {} with {} txs",
                        block.slot,
                        block.transactions.len()
                    );

                    // Arm the gate and backfill BEFORE forwarding this block, so its
                    // SlotComplete cannot leapfrog the window still unfilled underneath it.
                    #[cfg(feature = "datasource-rpc")]
                    if let Some(ctx) = gap_ctx {
                        if !armed {
                            armed = true;
                            let safe_to_forward = arm_reconnect_gap(
                                block.slot,
                                startup_floor,
                                ctx,
                                &tx,
                                &cancellation_token,
                                RECONNECT_GAP_RETRY_BACKOFF,
                                backfill_handle,
                            )
                            .await
                            .map_err(DataSourceError::Rpc)?;
                            // Cancelled mid-arm before the gate was set: stop instead of
                            // forwarding this block, whose SlotComplete would leapfrog the
                            // unfilled gap. Shutdown proceeds; a restart replays the slot.
                            if !safe_to_forward {
                                break;
                            }
                        }
                    }

                    // Fail-closed: any parse/send failure returns Err (reconnect + gap-fill
                    // replays the slot) and no SlotComplete is emitted for it.
                    if let Err(e) = handle_block(block, &program_id, program_type, &tx).await {
                        error!("Error handling block: {}", e);
                        return Err(DataSourceError::Rpc(e));
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
    use crate::test_utils::rpc_mocks::mock_get_blocks;
    use mockito::Server;
    use serde_json::json;
    use solana_sdk::commitment_config::CommitmentLevel;
    use solana_transaction_status::UiTransactionEncoding;
    use tokio::sync::mpsc;

    /// An empty block whose parent link names the previous slot, so a run of these
    /// forms the unbroken chain the classifier proves absence against.
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
                    "result": {
                        "blockhash": "TestBlockHash11111111111111111111111111111",
                        "parentSlot": slot.saturating_sub(1),
                        "transactions": []
                    },
                    "id": 1
                })
                .to_string(),
            )
            .create()
    }

    /// Every slot in `[start, end]` produced a block, the dense case.
    fn mock_dense_range(server: &mut Server, start: u64, end: u64) -> mockito::Mock {
        let produced: Vec<u64> = (start..=end).collect();
        mock_get_blocks(server, start, end, &produced)
    }

    /// A short backoff keeps the retry-loop tests fast; prod uses RECONNECT_GAP_RETRY_BACKOFF.
    const TEST_BACKOFF: Duration = Duration::from_millis(10);

    fn test_poller(server: &Server) -> RpcPoller {
        RpcPoller::new(
            server.url(),
            UiTransactionEncoding::Json,
            CommitmentLevel::Finalized,
        )
    }

    /// An anchor an operator can move mid-run, so the retry loop can be seen re-reading it.
    fn checkpoint_shared(
        slot: Arc<std::sync::atomic::AtomicU64>,
    ) -> impl Fn() -> std::future::Ready<Result<Option<u64>, CheckpointError>> {
        move || std::future::ready(Ok(Some(slot.load(std::sync::atomic::Ordering::SeqCst))))
    }

    /// A durable anchor at `slot`.
    fn checkpoint_ok(
        slot: u64,
    ) -> impl Fn() -> std::future::Ready<Result<Option<u64>, CheckpointError>> {
        move || std::future::ready(Ok(Some(slot)))
    }

    /// A store that has never recorded an anchor, which must not be read as slot zero.
    fn checkpoint_absent() -> impl Fn() -> std::future::Ready<Result<Option<u64>, CheckpointError>>
    {
        || std::future::ready(Ok(None))
    }

    /// Mock storage seeded with an escrow checkpoint, backing arm_reconnect_gap's
    /// checkpoint read (get_last_checkpoint on program "escrow").
    fn storage_with_checkpoint(slot: u64) -> Arc<Storage> {
        use crate::storage::common::storage::mock::MockStorage;
        let mock = MockStorage::new();
        mock.set_checkpoint("escrow", slot);
        Arc::new(Storage::Mock(mock))
    }

    /// Escrow reconnect-gap context over the mock RPC server and storage.
    fn test_gap_ctx(server: &Server, storage: Arc<Storage>, max_gap_slots: u64) -> ReconnectGapCtx {
        ReconnectGapCtx {
            poller: Arc::new(test_poller(server)),
            storage,
            max_gap_slots,
            batch_size: 10,
            program_type: ProgramType::Escrow,
            escrow_instance_id: None,
        }
    }

    /// No gap up to the target resolves immediately without fetching any block.
    #[tokio::test]
    async fn gap_fill_no_gap_resolves_without_fetching_blocks() {
        let mut server = Server::new_async().await;
        let no_blocks = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(json!({"method": "getBlock"})))
            .expect(0)
            .create_async()
            .await;

        let poller = test_poller(&server);
        let (tx, mut rx) = mpsc::channel(64);
        let cancel = CancellationToken::new();

        let outcome = fill_reconnect_gap_to(
            100,
            None,
            checkpoint_ok(100),
            &poller,
            1000,
            10,
            ProgramType::Escrow,
            None,
            &tx,
            &cancel,
            TEST_BACKOFF,
        )
        .await;

        assert_eq!(outcome, GapFillOutcome::Resolved);
        no_blocks.assert_async().await;
        drop(tx);
        assert!(
            rx.recv().await.is_none(),
            "no messages may be emitted when there is no gap"
        );
    }

    /// Checkpoint 100, target 103; the fill replays the boundary slot (anchor 99)
    /// and emits SlotComplete for exactly 100..=103.
    #[tokio::test]
    async fn gap_fill_fills_gap_and_replays_boundary_slot() {
        let mut server = Server::new_async().await;
        let _enum = mock_dense_range(&mut server, 100, 103);
        let _blocks: Vec<_> = (100..=103)
            .map(|s| mock_get_block_success(&mut server, s))
            .collect();

        let poller = test_poller(&server);
        let (tx, mut rx) = mpsc::channel(64);
        let cancel = CancellationToken::new();

        let outcome = fill_reconnect_gap_to(
            103,
            None,
            checkpoint_ok(100),
            &poller,
            1000,
            10,
            ProgramType::Escrow,
            None,
            &tx,
            &cancel,
            TEST_BACKOFF,
        )
        .await;

        assert_eq!(outcome, GapFillOutcome::Resolved);
        drop(tx);

        let mut slots = vec![];
        while let Some(msg) = rx.recv().await {
            if let ProcessorMessage::SlotComplete { slot, .. } = msg {
                slots.push(slot);
            }
        }
        assert_eq!(slots, vec![100, 101, 102, 103]);
    }

    /// A store with no anchor must stall, never resolve over a range nothing has read.
    #[tokio::test]
    async fn gap_fill_missing_anchor_retries_instead_of_resolving() {
        let mut server = Server::new_async().await;
        let untouched = server.mock("POST", "/").expect(0).create_async().await;

        let poller = test_poller(&server);
        let (tx, mut rx) = mpsc::channel(64);
        let cancel = CancellationToken::new();
        let cancel_task = cancel.clone();

        let mut handle = tokio::spawn(async move {
            fill_reconnect_gap_to(
                100,
                None,
                checkpoint_absent(),
                &poller,
                1000,
                10,
                ProgramType::Escrow,
                None,
                &tx,
                &cancel_task,
                TEST_BACKOFF,
            )
            .await
        });

        // Several retry cycles must pass without the loop resolving or touching the RPC.
        tokio::time::sleep(TEST_BACKOFF * 5).await;
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), &mut handle)
                .await
                .is_err(),
            "a missing anchor must stall, not resolve"
        );
        untouched.assert_async().await;
        assert!(
            rx.try_recv().is_err(),
            "no message may be emitted while the anchor is missing"
        );

        cancel.cancel();
        let outcome = tokio::time::timeout(std::time::Duration::from_secs(2), handle)
            .await
            .expect("cancellation must end the stall")
            .unwrap();
        assert_eq!(outcome, GapFillOutcome::Cancelled);
    }

    /// A durable slot 0 is a real anchor; the batch starts one past it, so 0 is not replayed.
    #[tokio::test]
    async fn gap_fill_genuine_zero_anchor_fills_gap() {
        let mut server = Server::new_async().await;
        let _enum = mock_dense_range(&mut server, 1, 3);
        let _blocks: Vec<_> = (1..=3)
            .map(|s| mock_get_block_success(&mut server, s))
            .collect();

        let poller = test_poller(&server);
        let (tx, mut rx) = mpsc::channel(64);
        let cancel = CancellationToken::new();

        let outcome = fill_reconnect_gap_to(
            3,
            None,
            checkpoint_ok(0),
            &poller,
            1000,
            10,
            ProgramType::Escrow,
            None,
            &tx,
            &cancel,
            TEST_BACKOFF,
        )
        .await;

        assert_eq!(outcome, GapFillOutcome::Resolved);
        drop(tx);

        let mut slots = vec![];
        while let Some(msg) = rx.recv().await {
            if let ProcessorMessage::SlotComplete { slot, .. } = msg {
                slots.push(slot);
            }
        }
        assert_eq!(slots, vec![1, 2, 3]);
    }

    /// A failing checkpoint read retries instead of skipping the gap check.
    #[tokio::test]
    async fn gap_fill_retries_checkpoint_read_failures() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let server = Server::new_async().await;

        let calls = Arc::new(AtomicUsize::new(0));
        let calls_in = calls.clone();
        let get_checkpoint = move || {
            let n = calls_in.fetch_add(1, Ordering::SeqCst);
            async move {
                if n < 2 {
                    Err(CheckpointError::InvalidCheckpoint { slot: 0, last: 0 })
                } else {
                    Ok(Some(100))
                }
            }
        };

        let poller = test_poller(&server);
        let (tx, _rx) = mpsc::channel(64);
        let cancel = CancellationToken::new();

        // target == checkpoint => no gap once the read heals.
        let outcome = fill_reconnect_gap_to(
            100,
            None,
            get_checkpoint,
            &poller,
            1000,
            10,
            ProgramType::Escrow,
            None,
            &tx,
            &cancel,
            TEST_BACKOFF,
        )
        .await;

        assert_eq!(outcome, GapFillOutcome::Resolved);
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    /// An unavailable (pruned) slot stalls the loop with repeated attempts;
    /// no SlotComplete ever leaks and cancellation ends the stall.
    #[tokio::test]
    async fn gap_fill_unavailable_slot_stalls_without_slot_complete() {
        let mut server = Server::new_async().await;
        // Slot 100 is listed as a producer but cannot be served => Unavailable.
        let _enum = mock_dense_range(&mut server, 100, 103);
        let absent = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(json!({
                "method": "getBlock",
                "params": [100]
            })))
            .with_status(200)
            .with_body(
                json!({
                    "jsonrpc": "2.0",
                    "error": { "code": -32009, "message": "Slot was skipped" },
                    "id": 1
                })
                .to_string(),
            )
            .expect_at_least(2)
            .create_async()
            .await;
        let _rest: Vec<_> = (101..=103)
            .map(|s| mock_get_block_success(&mut server, s))
            .collect();

        let poller = test_poller(&server);
        let (tx, mut rx) = mpsc::channel(64);
        let cancel = CancellationToken::new();
        let cancel_task = cancel.clone();

        let mut handle = tokio::spawn(async move {
            fill_reconnect_gap_to(
                103,
                None,
                checkpoint_ok(100),
                &poller,
                1000,
                10,
                ProgramType::Escrow,
                None,
                &tx,
                &cancel_task,
                TEST_BACKOFF,
            )
            .await
        });

        // Stalls across at least two fill attempts instead of returning an error.
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !absent.matched_async().await {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("expected at least two attempts on the pruned slot");

        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), &mut handle)
                .await
                .is_err(),
            "must not return while the slot is unavailable"
        );
        while let Ok(msg) = rx.try_recv() {
            assert!(
                !matches!(msg, ProcessorMessage::SlotComplete { .. }),
                "no SlotComplete may leak past an unavailable slot"
            );
        }

        cancel.cancel();
        let outcome = tokio::time::timeout(std::time::Duration::from_secs(2), handle)
            .await
            .expect("cancellation must end the stall")
            .unwrap();
        assert_eq!(outcome, GapFillOutcome::Cancelled);
    }

    /// An oversized gap to the target stalls loudly with no fill attempt at all,
    /// because validate_gap rejects it before any block fetch.
    #[tokio::test]
    async fn gap_fill_too_large_stalls_without_fill() {
        let mut server = Server::new_async().await;
        let no_blocks = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(json!({"method": "getBlock"})))
            .expect(0)
            .create_async()
            .await;

        let poller = test_poller(&server);
        let (tx, _rx) = mpsc::channel(64);
        let cancel = CancellationToken::new();
        let cancel_task = cancel.clone();

        let mut handle = tokio::spawn(async move {
            fill_reconnect_gap_to(
                200,
                None,
                checkpoint_ok(100),
                &poller,
                10,
                10,
                ProgramType::Escrow,
                None,
                &tx,
                &cancel_task,
                TEST_BACKOFF,
            )
            .await
        });

        // Let several retry cycles elapse; the loop must neither resolve nor fill.
        tokio::time::sleep(TEST_BACKOFF * 5).await;
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), &mut handle)
                .await
                .is_err(),
            "an oversized gap must stall, not resolve"
        );
        no_blocks.assert_async().await;

        cancel.cancel();
        let outcome = tokio::time::timeout(std::time::Duration::from_secs(2), handle)
            .await
            .expect("cancellation must end the stall")
            .unwrap();
        assert_eq!(outcome, GapFillOutcome::Cancelled);
    }

    /// Cancellation during the backoff returns promptly instead of sleeping
    /// out the full production backoff.
    #[tokio::test]
    async fn gap_fill_cancellation_interrupts_backoff_promptly() {
        let server = Server::new_async().await;

        let poller = test_poller(&server);
        let (tx, _rx) = mpsc::channel(64);
        let cancel = CancellationToken::new();
        let cancel_task = cancel.clone();

        // A checkpoint read that always errors forces the loop into its backoff.
        let get_checkpoint =
            || std::future::ready(Err(CheckpointError::InvalidCheckpoint { slot: 0, last: 0 }));

        let handle = tokio::spawn(async move {
            fill_reconnect_gap_to(
                103,
                None,
                get_checkpoint,
                &poller,
                1000,
                10,
                ProgramType::Escrow,
                None,
                &tx,
                &cancel_task,
                RECONNECT_GAP_RETRY_BACKOFF,
            )
            .await
        });

        // Land inside the first (5s) backoff, then cancel.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        cancel.cancel();

        let outcome = tokio::time::timeout(std::time::Duration::from_millis(500), handle)
            .await
            .expect("cancellation must interrupt the backoff, not wait it out")
            .unwrap();
        assert_eq!(outcome, GapFillOutcome::Cancelled);
    }

    /// arm_reconnect_gap emits the gate re-arm first, then backfills up to the
    /// observed resume slot (boundary replay from checkpoint-1).
    #[tokio::test]
    async fn arm_reconnect_gap_emits_regate_then_fills_to_target() {
        let mut server = Server::new_async().await;
        let _enum = mock_dense_range(&mut server, 100, 103);
        let _blocks: Vec<_> = (100..=103)
            .map(|s| mock_get_block_success(&mut server, s))
            .collect();

        let ctx = test_gap_ctx(&server, storage_with_checkpoint(100), 1000);
        let (tx, mut rx) = mpsc::channel(64);
        let cancel = CancellationToken::new();
        let mut prev: Option<tokio::task::JoinHandle<()>> = None;

        arm_reconnect_gap(103, &mut None, &ctx, &tx, &cancel, TEST_BACKOFF, &mut prev)
            .await
            .unwrap();
        drop(tx);

        let mut msgs = vec![];
        while let Some(m) = rx.recv().await {
            msgs.push(m);
        }
        prev.unwrap().await.unwrap();

        assert!(
            matches!(msgs[0], ProcessorMessage::Regate { target: 103, .. }),
            "the gate re-arm must be emitted first"
        );
        let slots: Vec<u64> = msgs
            .iter()
            .filter_map(|m| match m {
                ProcessorMessage::SlotComplete { slot, .. } => Some(*slot),
                _ => None,
            })
            .collect();
        assert_eq!(slots, vec![100, 101, 102, 103]);
    }

    /// A lagging provider can open the stream below the slot startup backfill is still filling.
    /// Gating there would free the checkpoint over that range, so the floor has to win.
    #[tokio::test]
    async fn arm_cold_start_raises_target_to_the_startup_floor() {
        let mut server = Server::new_async().await;
        let no_blocks = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(json!({"method": "getBlock"})))
            .expect(0)
            .create_async()
            .await;

        let ctx = test_gap_ctx(&server, storage_with_checkpoint(100), 1000);
        let (tx, mut rx) = mpsc::channel(64);
        let cancel = CancellationToken::new();
        let mut prev: Option<tokio::task::JoinHandle<()>> = None;

        // Startup owns up to 110; the stream opened at 103, seven slots behind it.
        arm_reconnect_gap(
            103,
            &mut Some(110),
            &ctx,
            &tx,
            &cancel,
            TEST_BACKOFF,
            &mut prev,
        )
        .await
        .unwrap();
        drop(tx);

        let mut msgs = vec![];
        while let Some(m) = rx.recv().await {
            msgs.push(m);
        }
        prev.unwrap().await.unwrap();

        assert!(
            matches!(
                msgs[0],
                ProcessorMessage::Regate {
                    from: 100,
                    target: 110,
                    ..
                }
            ),
            "the gate must widen to the floor, not narrow to the resume slot: {:?}",
            msgs[0]
        );
        // Startup backfill covers everything up to the floor, so this fill has nothing to do.
        assert_eq!(msgs.len(), 1, "no slot may be refetched below the floor");
        no_blocks.assert_async().await;
    }

    /// Only one connection may spend the floor, so a completed arm has to clear it.
    #[tokio::test]
    async fn arm_clears_the_startup_floor_once_the_gate_is_set() {
        let mut server = Server::new_async().await;
        let _no_blocks = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(json!({"method": "getBlock"})))
            .expect(0)
            .create_async()
            .await;

        let ctx = test_gap_ctx(&server, storage_with_checkpoint(100), 1000);
        let (tx, mut rx) = mpsc::channel(64);
        let cancel = CancellationToken::new();
        let mut prev: Option<tokio::task::JoinHandle<()>> = None;
        let mut floor = Some(110);

        let armed = arm_reconnect_gap(103, &mut floor, &ctx, &tx, &cancel, TEST_BACKOFF, &mut prev)
            .await
            .unwrap();
        drop(tx);
        while rx.recv().await.is_some() {}
        prev.unwrap().await.unwrap();

        assert!(armed);
        assert_eq!(floor, None, "a spent floor must not be reused");
    }

    /// An oversized gap clears when an operator resyncs the checkpoint above the floor. Pinning
    /// the anchor to the floor would make every retry read the same slot and never recover.
    #[tokio::test]
    async fn cold_start_fill_recovers_when_the_checkpoint_is_resynced() {
        use std::sync::atomic::{AtomicU64, Ordering};

        let mut server = Server::new_async().await;
        let _enum = mock_dense_range(&mut server, 995, 1000);
        let _blocks: Vec<_> = (995..=1000)
            .map(|s| mock_get_block_success(&mut server, s))
            .collect();

        let poller = test_poller(&server);
        let (tx, _rx) = mpsc::channel(256);
        let cancel = CancellationToken::new();
        // Gap 1000 - 100 is far over the bound, so the fill can only spin here.
        let checkpoint = Arc::new(AtomicU64::new(100));

        let fill = tokio::spawn({
            let checkpoint = checkpoint.clone();
            let cancel = cancel.clone();
            let url = server.url();
            async move {
                let poller =
                    RpcPoller::new(url, UiTransactionEncoding::Json, CommitmentLevel::Finalized);
                fill_reconnect_gap_to(
                    1000,
                    Some(100),
                    checkpoint_shared(checkpoint),
                    &poller,
                    10,
                    10,
                    ProgramType::Escrow,
                    None,
                    &tx,
                    &cancel,
                    TEST_BACKOFF,
                )
                .await
            }
        });

        // Several retries at this backoff, so the first verdict is provably not the last.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!fill.is_finished(), "an oversized gap must not resolve");

        checkpoint.store(995, Ordering::SeqCst);

        let outcome = tokio::time::timeout(Duration::from_secs(10), fill)
            .await
            .expect("the resynced checkpoint must let the fill proceed")
            .unwrap();
        assert!(matches!(outcome, GapFillOutcome::Resolved));
        drop(poller);
    }

    /// An arm that gives up leaves no gate behind, so the next connection still needs the
    /// floor. Dropping it there would let that connection gate below what startup owes.
    #[tokio::test]
    async fn arm_keeps_the_startup_floor_when_it_cannot_arm() {
        use crate::storage::common::storage::mock::MockStorage;

        let server = Server::new_async().await;
        // No checkpoint to anchor on, so the arm parks until cancellation ends it.
        let ctx = test_gap_ctx(&server, Arc::new(Storage::Mock(MockStorage::new())), 1000);
        let (tx, _rx) = mpsc::channel(64);
        let cancel = CancellationToken::new();
        cancel.cancel();
        let mut prev: Option<tokio::task::JoinHandle<()>> = None;
        let mut floor = Some(110);

        let armed = arm_reconnect_gap(103, &mut floor, &ctx, &tx, &cancel, TEST_BACKOFF, &mut prev)
            .await
            .unwrap();

        assert!(!armed, "no gate was set");
        assert_eq!(floor, Some(110), "the floor must survive for the next arm");
    }

    /// The checkpoint can sit far below the floor, so replaying from it would refetch slots
    /// startup owns or that start_slot meant to skip. The fill anchors on the floor instead.
    #[tokio::test]
    async fn cold_start_fill_starts_at_the_floor_not_the_checkpoint() {
        let mut server = Server::new_async().await;
        let _enum = mock_dense_range(&mut server, 100, 103);
        let _blocks: Vec<_> = (100..=103)
            .map(|s| mock_get_block_success(&mut server, s))
            .collect();

        let poller = test_poller(&server);
        let (tx, mut rx) = mpsc::channel(256);
        let cancel = CancellationToken::new();

        let outcome = fill_reconnect_gap_to(
            103,
            Some(100),
            checkpoint_ok(10),
            &poller,
            1000,
            10,
            ProgramType::Escrow,
            None,
            &tx,
            &cancel,
            TEST_BACKOFF,
        )
        .await;
        drop(tx);

        assert_eq!(outcome, GapFillOutcome::Resolved);
        let mut slots = vec![];
        while let Some(ProcessorMessage::SlotComplete { slot, .. }) = rx.recv().await {
            slots.push(slot);
        }
        assert_eq!(
            slots,
            vec![100, 101, 102, 103],
            "the fill must start at the floor, not walk up from checkpoint 10"
        );
    }

    /// No anchor means the block is unsafe to forward; letting it through was the bug.
    #[tokio::test]
    async fn arm_reconnect_gap_missing_anchor_withholds_block() {
        use crate::storage::common::storage::mock::MockStorage;
        let mut server = Server::new_async().await;
        let no_blocks = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(json!({"method": "getBlock"})))
            .expect(0)
            .create_async()
            .await;

        // No checkpoint seeded; the pre-cancelled token ends the retry loop at once.
        let ctx = test_gap_ctx(&server, Arc::new(Storage::Mock(MockStorage::new())), 1000);
        let (tx, mut rx) = mpsc::channel(64);
        let cancel = CancellationToken::new();
        cancel.cancel();
        let mut prev: Option<tokio::task::JoinHandle<()>> = None;

        let safe = arm_reconnect_gap(103, &mut None, &ctx, &tx, &cancel, TEST_BACKOFF, &mut prev)
            .await
            .unwrap();
        drop(tx);

        assert!(
            !safe,
            "a missing anchor must report the block unsafe to forward"
        );
        assert!(
            prev.is_none(),
            "no backfill is spawned without a durable anchor"
        );
        assert!(
            rx.recv().await.is_none(),
            "no Regate and no SlotComplete may be emitted without an anchor"
        );
        no_blocks.assert_async().await;
    }

    /// Cancellation while the checkpoint read is failing reports the block unsafe to
    /// forward, so its SlotComplete cannot leapfrog the still-unfilled gap on shutdown.
    #[tokio::test]
    async fn arm_reconnect_gap_cancelled_during_read_withholds_block() {
        use crate::storage::common::storage::mock::MockStorage;
        let mut server = Server::new_async().await;
        let no_blocks = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(json!({"method": "getBlock"})))
            .expect(0)
            .create_async()
            .await;

        // The checkpoint read fails, so arm loops; a pre-cancelled token ends it at once.
        let mock = MockStorage::new();
        mock.set_checkpoint("escrow", 100);
        mock.set_should_fail("get_committed_checkpoint", true);
        let ctx = test_gap_ctx(&server, Arc::new(Storage::Mock(mock)), 1000);

        let (tx, mut rx) = mpsc::channel(64);
        let cancel = CancellationToken::new();
        cancel.cancel();
        let mut prev: Option<tokio::task::JoinHandle<()>> = None;

        let safe = arm_reconnect_gap(103, &mut None, &ctx, &tx, &cancel, TEST_BACKOFF, &mut prev)
            .await
            .unwrap();
        drop(tx);

        assert!(
            !safe,
            "cancellation before arming must report the block unsafe to forward"
        );
        assert!(prev.is_none(), "no backfill is spawned on a cancelled arm");
        assert!(
            rx.recv().await.is_none(),
            "no Regate is emitted on a cancelled arm"
        );
        no_blocks.assert_async().await;
    }

    /// A second arm aborts the prior backfill and leaves exactly one in flight,
    /// bounding concurrent reconnect backfills to one.
    #[tokio::test]
    async fn arm_reconnect_gap_aborts_prior_backfill() {
        let mut server = Server::new_async().await;
        let _ = mock_get_block_success(&mut server, 100);

        // max_gap_slots = 10 with target 1000 => GapTooLarge => the fill loops
        // forever, keeping the first backfill in flight so it can be aborted.
        let ctx = test_gap_ctx(&server, storage_with_checkpoint(100), 10);
        let (tx, _rx) = mpsc::channel(64);
        let cancel = CancellationToken::new();
        let mut prev: Option<tokio::task::JoinHandle<()>> = None;

        arm_reconnect_gap(1000, &mut None, &ctx, &tx, &cancel, TEST_BACKOFF, &mut prev)
            .await
            .unwrap();
        let first = prev.as_ref().unwrap().abort_handle();
        assert!(!first.is_finished(), "first backfill is still in flight");

        arm_reconnect_gap(1000, &mut None, &ctx, &tx, &cancel, TEST_BACKOFF, &mut prev)
            .await
            .unwrap();

        // The prior backfill was aborted; only the replacement remains in flight.
        for _ in 0..100 {
            if first.is_finished() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert!(first.is_finished(), "the prior backfill must be aborted");
        let second = prev.as_ref().unwrap().abort_handle();
        assert!(
            !second.is_finished(),
            "the replacement backfill stays in flight"
        );

        cancel.cancel();
        if let Some(h) = prev.take() {
            let _ = h.await;
        }
    }

    /// Borsh-encoded WithdrawFunds payload (discriminator 0, amount, None destination)
    /// that `parse_withdraw_instruction` accepts.
    fn withdraw_funds_proto_data() -> Vec<u8> {
        let mut data = vec![0u8]; // WITHDRAW_FUNDS discriminator
        data.extend_from_slice(&1000u64.to_le_bytes());
        data.push(0); // None destination
        data
    }

    /// Account-key slot holding the watched program; any other program_id_index
    /// points at a foreign program that the handler must filter out.
    const WITHDRAW_PROGRAM_KEY_INDEX: u32 = 5;

    /// One transaction whose instructions target the given `program_indices`
    /// (`WITHDRAW_PROGRAM_KEY_INDEX` is the watched program, anything else is a
    /// foreign program that gets filtered out). Used to assert absolute
    /// per-instruction indexing, including across filtered-out instructions.
    fn withdraw_tx_update_with_program_indices(
        signature: Vec<u8>,
        program_id: &Pubkey,
        program_indices: &[u32],
    ) -> yellowstone_grpc_proto::geyser::SubscribeUpdateTransaction {
        use yellowstone_grpc_proto::prelude as proto;

        let mut account_keys: Vec<Vec<u8>> = (0..WITHDRAW_PROGRAM_KEY_INDEX)
            .map(|i| {
                let mut b = [0u8; 32];
                b[0] = i as u8 + 1;
                b.to_vec()
            })
            .collect();
        account_keys.push(program_id.to_bytes().to_vec());

        let instructions = program_indices
            .iter()
            .map(|&pidx| proto::CompiledInstruction {
                program_id_index: pidx,
                accounts: vec![0, 1, 2, 3, 4],
                data: withdraw_funds_proto_data(),
            })
            .collect();

        let message = proto::Message {
            header: Some(proto::MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            }),
            account_keys,
            recent_blockhash: vec![0u8; 32],
            instructions,
            versioned: false,
            address_table_lookups: vec![],
        };

        yellowstone_grpc_proto::geyser::SubscribeUpdateTransaction {
            slot: 42,
            transaction: Some(proto::SubscribeUpdateTransactionInfo {
                signature,
                is_vote: false,
                transaction: Some(proto::Transaction {
                    signatures: vec![signature_placeholder()],
                    message: Some(message),
                }),
                // Present-but-empty meta: a real tx always carries meta, and the handler
                // now requires it; tests that add inner instructions override this.
                meta: Some(proto::TransactionStatusMeta::default()),
                index: 0,
            }),
        }
    }

    /// One transaction carrying `count` WithdrawFunds instructions, all targeting
    /// the withdraw program.
    fn withdraw_tx_update(
        signature: Vec<u8>,
        program_id: &Pubkey,
        count: usize,
    ) -> yellowstone_grpc_proto::geyser::SubscribeUpdateTransaction {
        let program_indices = vec![WITHDRAW_PROGRAM_KEY_INDEX; count];
        withdraw_tx_update_with_program_indices(signature, program_id, &program_indices)
    }

    fn signature_placeholder() -> Vec<u8> {
        vec![7u8; 64]
    }

    #[tokio::test]
    async fn handle_transaction_emits_absolute_instruction_index_per_instruction() {
        use std::str::FromStr;
        let program_id = Pubkey::from_str("J231K9UEpS4y4KAPwGc4gsMNCjKFRMYcQBcjVW7vBhVi").unwrap();
        let signature = vec![3u8; 64];

        let tx_update = withdraw_tx_update(signature.clone(), &program_id, 2);

        let (tx, mut rx) = mpsc::channel(8);
        handle_transaction(tx_update, &program_id, ProgramType::Withdraw, &tx)
            .await
            .unwrap();
        drop(tx);

        let mut metas = vec![];
        while let Some(ProcessorMessage::Instruction(meta)) = rx.recv().await {
            metas.push(meta);
        }

        assert_eq!(metas.len(), 2);
        let expected_sig = bs58::encode(&signature).into_string();
        assert_eq!(metas[0].signature.as_deref(), Some(expected_sig.as_str()));
        assert_eq!(metas[1].signature.as_deref(), Some(expected_sig.as_str()));
        assert_eq!(metas[0].instruction_index, 0);
        assert_eq!(metas[1].instruction_index, 1);
    }

    #[tokio::test]
    async fn handle_transaction_keeps_absolute_index_across_filtered_instruction() {
        use std::str::FromStr;
        let program_id = Pubkey::from_str("J231K9UEpS4y4KAPwGc4gsMNCjKFRMYcQBcjVW7vBhVi").unwrap();
        let signature = vec![4u8; 64];

        // Position 1 targets a foreign program (account index 0) and is filtered out;
        // the surviving withdraw instructions must keep absolute positions 0 and 2,
        // not the relative 0 and 1.
        let tx_update = withdraw_tx_update_with_program_indices(
            signature.clone(),
            &program_id,
            &[WITHDRAW_PROGRAM_KEY_INDEX, 0, WITHDRAW_PROGRAM_KEY_INDEX],
        );

        let (tx, mut rx) = mpsc::channel(8);
        handle_transaction(tx_update, &program_id, ProgramType::Withdraw, &tx)
            .await
            .unwrap();
        drop(tx);

        let mut metas = vec![];
        while let Some(ProcessorMessage::Instruction(meta)) = rx.recv().await {
            metas.push(meta);
        }

        assert_eq!(metas.len(), 2);
        assert_eq!(metas[0].instruction_index, 0);
        assert_eq!(metas[1].instruction_index, 2);
    }

    /// A CPI-only withdraw surfaces as a row carrying the parent's top-level index and the inner position.
    #[tokio::test]
    async fn handle_transaction_emits_inner_cpi_withdraw() {
        use std::str::FromStr;
        use yellowstone_grpc_proto::prelude as proto;

        let program_id = Pubkey::from_str("J231K9UEpS4y4KAPwGc4gsMNCjKFRMYcQBcjVW7vBhVi").unwrap();
        let signature = vec![9u8; 64];

        // Top-level targets a foreign program (account index 0); the withdraw program at WITHDRAW_PROGRAM_KEY_INDEX is CPI-only.
        let mut tx_update =
            withdraw_tx_update_with_program_indices(signature.clone(), &program_id, &[0]);

        let inner = proto::InnerInstruction {
            program_id_index: WITHDRAW_PROGRAM_KEY_INDEX,
            accounts: vec![0, 1, 2, 3, 4],
            data: withdraw_funds_proto_data(),
            stack_height: Some(2),
        };
        let inner_set = proto::InnerInstructions {
            index: 0,
            instructions: vec![inner],
        };
        tx_update.transaction.as_mut().unwrap().meta = Some(proto::TransactionStatusMeta {
            inner_instructions: vec![inner_set],
            ..Default::default()
        });

        let (tx, mut rx) = mpsc::channel(8);
        handle_transaction(tx_update, &program_id, ProgramType::Withdraw, &tx)
            .await
            .unwrap();
        drop(tx);

        let mut metas = vec![];
        while let Some(ProcessorMessage::Instruction(meta)) = rx.recv().await {
            metas.push(meta);
        }

        assert_eq!(metas.len(), 1, "the inner CPI withdraw must surface");
        assert_eq!(metas[0].instruction_index, 0, "parent top-level index");
        assert_eq!(metas[0].inner_index, Some(0), "inner position");
    }

    // ============================================================================
    // Escrow CPI deposit parsing (real parser, loaded-address resolution)
    // ============================================================================

    /// Build an escrow `SubscribeUpdateTransaction`. `account_keys` are static
    /// message keys (raw 32-byte); instruction `data` is raw (the handler base58-
    /// encodes); `loaded_writable`/`loaded_readonly` become the meta's ALT keys.
    fn escrow_tx_update(
        signature: Vec<u8>,
        account_keys: Vec<Vec<u8>>,
        top_level: Vec<yellowstone_grpc_proto::prelude::CompiledInstruction>,
        inner_instructions: Vec<yellowstone_grpc_proto::prelude::InnerInstructions>,
        loaded_writable: Vec<Vec<u8>>,
        loaded_readonly: Vec<Vec<u8>>,
    ) -> yellowstone_grpc_proto::geyser::SubscribeUpdateTransaction {
        use yellowstone_grpc_proto::prelude as proto;
        let message = proto::Message {
            header: Some(proto::MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            }),
            account_keys,
            recent_blockhash: vec![0u8; 32],
            instructions: top_level,
            versioned: true,
            address_table_lookups: vec![],
        };
        yellowstone_grpc_proto::geyser::SubscribeUpdateTransaction {
            slot: 7,
            transaction: Some(proto::SubscribeUpdateTransactionInfo {
                signature,
                is_vote: false,
                transaction: Some(proto::Transaction {
                    signatures: vec![signature_placeholder()],
                    message: Some(message),
                }),
                meta: Some(proto::TransactionStatusMeta {
                    inner_instructions,
                    loaded_writable_addresses: loaded_writable,
                    loaded_readonly_addresses: loaded_readonly,
                    ..Default::default()
                }),
                index: 0,
            }),
        }
    }

    fn escrow_pubkey() -> Pubkey {
        use std::str::FromStr;
        Pubkey::from_str(
            crate::indexer::datasource::common::parser::escrow::PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
        )
        .unwrap()
    }

    /// DepositEvent amount carried by a parsed escrow Deposit row.
    fn escrow_deposit_amount(meta: &InstructionWithMetadata) -> u64 {
        use crate::indexer::datasource::common::parser::EscrowInstruction;
        match &meta.instruction {
            ProgramInstruction::Escrow(ix) => match ix.as_ref() {
                EscrowInstruction::Deposit { event, .. } => event.amount,
                _ => panic!("expected a Deposit instruction"),
            },
            _ => panic!("expected an Escrow instruction"),
        }
    }

    /// Two CPI escrow deposits sharing one transaction each resolve their own
    /// DepositEvent amount by stack height (proto `stack_height` is threaded
    /// through), landing as two rows with distinct inner indices. The borsh
    /// amount stays at the default so the asserted amounts can only come from the
    /// scoped event.
    #[tokio::test]
    async fn handle_transaction_scopes_two_cpi_escrow_deposits_by_stack_height() {
        use crate::test_utils::escrow_fixtures::{deposit_event_bytes, deposit_ix_bytes};
        use yellowstone_grpc_proto::prelude as proto;

        let escrow = escrow_pubkey();
        // Escrow at static key index 0; indices 1..12 fill the deposits' accounts.
        let mut account_keys: Vec<Vec<u8>> = (0u8..12)
            .map(|i| {
                let mut b = [0u8; 32];
                b[0] = i + 1;
                b.to_vec()
            })
            .collect();
        account_keys[0] = escrow.to_bytes().to_vec();

        // One foreign top-level instruction (index 1) that CPIs the two deposits.
        let top = vec![proto::CompiledInstruction {
            program_id_index: 1,
            accounts: vec![],
            data: vec![],
        }];

        let deposit = || proto::InnerInstruction {
            program_id_index: 0,
            accounts: (0u8..12).collect(),
            data: deposit_ix_bytes(1000, None),
            stack_height: Some(2),
        };
        let event = |amount: u64| proto::InnerInstruction {
            program_id_index: 0,
            accounts: vec![],
            data: deposit_event_bytes(amount),
            stack_height: Some(3),
        };
        // Pre-order walk: [0] deposit A h2, [1] event 300 h3, [2] deposit B h2, [3] event 480 h3.
        let inner_set = vec![proto::InnerInstructions {
            index: 0,
            instructions: vec![deposit(), event(300), deposit(), event(480)],
        }];

        let tx_update =
            escrow_tx_update(vec![9u8; 64], account_keys, top, inner_set, vec![], vec![]);

        let (tx, mut rx) = mpsc::channel(8);
        handle_transaction(tx_update, &escrow, ProgramType::Escrow, &tx)
            .await
            .unwrap();
        drop(tx);

        let mut metas = vec![];
        while let Some(ProcessorMessage::Instruction(m)) = rx.recv().await {
            metas.push(m);
        }

        assert_eq!(
            metas.len(),
            2,
            "two CPI deposits; the event self-CPIs are not counted as rows"
        );
        assert_eq!(metas[0].inner_index, Some(0));
        assert_eq!(
            escrow_deposit_amount(&metas[0]),
            300,
            "deposit A reads its own event"
        );
        assert_eq!(metas[1].inner_index, Some(2));
        assert_eq!(
            escrow_deposit_amount(&metas[1]),
            480,
            "deposit B reads its own event, not A's"
        );
    }

    /// A top-level escrow deposit whose program id is ALT-loaded is still parsed.
    /// Escrow is the readonly loaded key behind one writable loaded key, so it
    /// only resolves if loaded keys are appended writable-then-readonly after the
    /// static keys (its full-list index is 13, not 12).
    #[tokio::test]
    async fn handle_transaction_resolves_alt_loaded_escrow_program() {
        use crate::test_utils::escrow_fixtures::{deposit_event_bytes, deposit_ix_bytes};
        use yellowstone_grpc_proto::prelude as proto;

        let escrow = escrow_pubkey();
        // 12 static keys (0..11), none is escrow.
        let account_keys: Vec<Vec<u8>> = (0u8..12)
            .map(|i| {
                let mut b = [0u8; 32];
                b[0] = i + 1;
                b.to_vec()
            })
            .collect();
        let dummy_writable = {
            let mut b = [0u8; 32];
            b[0] = 200;
            b.to_vec()
        };

        // Top-level deposit + its event target index 13: static 0..11, writable 12, readonly 13 (escrow).
        let top = vec![proto::CompiledInstruction {
            program_id_index: 13,
            accounts: (0u8..12).collect(),
            data: deposit_ix_bytes(1000, None),
        }];
        let inner_set = vec![proto::InnerInstructions {
            index: 0,
            instructions: vec![proto::InnerInstruction {
                program_id_index: 13,
                accounts: vec![],
                data: deposit_event_bytes(555),
                stack_height: Some(2),
            }],
        }];

        let tx_update = escrow_tx_update(
            vec![10u8; 64],
            account_keys,
            top,
            inner_set,
            vec![dummy_writable],
            vec![escrow.to_bytes().to_vec()],
        );

        let (tx, mut rx) = mpsc::channel(8);
        handle_transaction(tx_update, &escrow, ProgramType::Escrow, &tx)
            .await
            .unwrap();
        drop(tx);

        let mut metas = vec![];
        while let Some(ProcessorMessage::Instruction(m)) = rx.recv().await {
            metas.push(m);
        }

        assert_eq!(
            metas.len(),
            1,
            "top-level ALT-loaded escrow deposit must be indexed"
        );
        assert_eq!(metas[0].instruction_index, 0);
        assert!(
            metas[0].inner_index.is_none(),
            "a top-level deposit has a NULL inner_index"
        );
        assert_eq!(escrow_deposit_amount(&metas[0]), 555);
    }

    // ============================================================================
    // Block handler: parse-all-then-complete, fail-closed on a malformed tx.
    // ============================================================================

    /// Wrap tx infos in a block for the given slot.
    fn block_update(
        slot: u64,
        txs: Vec<yellowstone_grpc_proto::geyser::SubscribeUpdateTransactionInfo>,
    ) -> yellowstone_grpc_proto::geyser::SubscribeUpdateBlock {
        yellowstone_grpc_proto::geyser::SubscribeUpdateBlock {
            slot,
            transactions: txs,
            ..Default::default()
        }
    }

    /// Every tx is forwarded, then SlotComplete lands strictly last, so a
    /// completion can never precede one of its slot's transactions.
    #[tokio::test]
    async fn block_forwards_all_txs_then_completes() {
        use std::str::FromStr;
        let program_id = Pubkey::from_str("J231K9UEpS4y4KAPwGc4gsMNCjKFRMYcQBcjVW7vBhVi").unwrap();
        let tx_a = withdraw_tx_update(vec![1u8; 64], &program_id, 1)
            .transaction
            .unwrap();
        let tx_b = withdraw_tx_update(vec![2u8; 64], &program_id, 1)
            .transaction
            .unwrap();
        let block = block_update(500, vec![tx_a, tx_b]);

        let (tx, mut rx) = mpsc::channel(8);
        handle_block(block, &program_id, ProgramType::Withdraw, &tx)
            .await
            .unwrap();
        drop(tx);

        let mut msgs = vec![];
        while let Some(m) = rx.recv().await {
            msgs.push(m);
        }

        assert_eq!(msgs.len(), 3);
        let sig_a = bs58::encode([1u8; 64]).into_string();
        let sig_b = bs58::encode([2u8; 64]).into_string();
        match &msgs[0] {
            ProcessorMessage::Instruction(m) => {
                assert_eq!(m.signature.as_deref(), Some(sig_a.as_str()))
            }
            _ => panic!("expected instruction A first"),
        }
        match &msgs[1] {
            ProcessorMessage::Instruction(m) => {
                assert_eq!(m.signature.as_deref(), Some(sig_b.as_str()))
            }
            _ => panic!("expected instruction B second"),
        }
        match &msgs[2] {
            ProcessorMessage::SlotComplete { slot, .. } => assert_eq!(*slot, 500),
            _ => panic!("expected SlotComplete last"),
        }
    }

    /// A block with no program txs still checkpoints its slot.
    #[tokio::test]
    async fn empty_block_completes() {
        use std::str::FromStr;
        let program_id = Pubkey::from_str("J231K9UEpS4y4KAPwGc4gsMNCjKFRMYcQBcjVW7vBhVi").unwrap();
        let block = block_update(700, vec![]);

        let (tx, mut rx) = mpsc::channel(8);
        handle_block(block, &program_id, ProgramType::Withdraw, &tx)
            .await
            .unwrap();
        drop(tx);

        let mut msgs = vec![];
        while let Some(m) = rx.recv().await {
            msgs.push(m);
        }
        assert_eq!(msgs.len(), 1);
        match &msgs[0] {
            ProcessorMessage::SlotComplete { slot, .. } => assert_eq!(*slot, 700),
            _ => panic!("expected a lone SlotComplete"),
        }
    }

    /// A tx targeting a foreign program is soft-skipped, yet the slot still completes.
    #[tokio::test]
    async fn block_with_foreign_tx_skips_but_completes() {
        use std::str::FromStr;
        let program_id = Pubkey::from_str("J231K9UEpS4y4KAPwGc4gsMNCjKFRMYcQBcjVW7vBhVi").unwrap();
        // program_id_index 0 points at a non-program account, so it is filtered out.
        let foreign = withdraw_tx_update_with_program_indices(vec![3u8; 64], &program_id, &[0])
            .transaction
            .unwrap();
        let block = block_update(800, vec![foreign]);

        let (tx, mut rx) = mpsc::channel(8);
        handle_block(block, &program_id, ProgramType::Withdraw, &tx)
            .await
            .unwrap();
        drop(tx);

        let mut msgs = vec![];
        while let Some(m) = rx.recv().await {
            msgs.push(m);
        }
        assert_eq!(msgs.len(), 1, "foreign tx yields no Instruction");
        match &msgs[0] {
            ProcessorMessage::SlotComplete { slot, .. } => assert_eq!(*slot, 800),
            _ => panic!("slot still completes after a soft-skipped tx"),
        }
    }

    /// A malformed protobuf tx fails the whole block and no SlotComplete is sent,
    /// so an incompletely parsed slot is never checkpointed.
    #[tokio::test]
    async fn block_malformed_tx_errs_without_completing() {
        use std::str::FromStr;
        use yellowstone_grpc_proto::prelude as proto;
        let program_id = Pubkey::from_str("J231K9UEpS4y4KAPwGc4gsMNCjKFRMYcQBcjVW7vBhVi").unwrap();

        // A tx whose inner message is absent triggers the "Missing message" Err path
        // (meta is present so the missing-meta guard is not what fires here).
        let bad = proto::SubscribeUpdateTransactionInfo {
            signature: vec![5u8; 64],
            is_vote: false,
            transaction: Some(proto::Transaction {
                signatures: vec![signature_placeholder()],
                message: None,
            }),
            meta: Some(proto::TransactionStatusMeta::default()),
            index: 0,
        };
        let block = block_update(900, vec![bad]);

        let (tx, mut rx) = mpsc::channel(8);
        let res = handle_block(block, &program_id, ProgramType::Withdraw, &tx).await;
        assert!(res.is_err(), "a malformed tx must fail the block");
        drop(tx);

        let mut saw_complete = false;
        while let Some(m) = rx.recv().await {
            if matches!(m, ProcessorMessage::SlotComplete { .. }) {
                saw_complete = true;
            }
        }
        assert!(
            !saw_complete,
            "slot must not complete when a tx fails to parse"
        );
    }

    /// Absolute per-instruction indices from the block path match the per-tx path.
    #[tokio::test]
    async fn block_absolute_instruction_index_preserved() {
        use std::str::FromStr;
        let program_id = Pubkey::from_str("J231K9UEpS4y4KAPwGc4gsMNCjKFRMYcQBcjVW7vBhVi").unwrap();
        let tx = withdraw_tx_update(vec![6u8; 64], &program_id, 2)
            .transaction
            .unwrap();
        let block = block_update(1000, vec![tx]);

        let (chan, mut rx) = mpsc::channel(8);
        handle_block(block, &program_id, ProgramType::Withdraw, &chan)
            .await
            .unwrap();
        drop(chan);

        let mut metas = vec![];
        while let Some(m) = rx.recv().await {
            if let ProcessorMessage::Instruction(meta) = m {
                metas.push(meta);
            }
        }
        assert_eq!(metas.len(), 2);
        assert_eq!(metas[0].instruction_index, 0);
        assert_eq!(metas[1].instruction_index, 1);
    }

    /// A transaction that reverted on-chain is skipped, never indexed, yet the
    /// block still completes, so a failed deposit/withdrawal is not credited.
    #[tokio::test]
    async fn block_skips_failed_tx_but_completes() {
        use std::str::FromStr;
        use yellowstone_grpc_proto::prelude as proto;
        let program_id = Pubkey::from_str("J231K9UEpS4y4KAPwGc4gsMNCjKFRMYcQBcjVW7vBhVi").unwrap();
        // A well-formed withdraw that would otherwise parse to one instruction,
        // marked as failed on-chain via meta.err.
        let mut failed = withdraw_tx_update(vec![8u8; 64], &program_id, 1)
            .transaction
            .unwrap();
        failed.meta = Some(proto::TransactionStatusMeta {
            err: Some(proto::TransactionError { err: vec![1] }),
            ..Default::default()
        });
        let block = block_update(1100, vec![failed]);

        let (tx, mut rx) = mpsc::channel(8);
        handle_block(block, &program_id, ProgramType::Withdraw, &tx)
            .await
            .unwrap();
        drop(tx);

        let mut instructions = 0;
        let mut completed = false;
        while let Some(m) = rx.recv().await {
            match m {
                ProcessorMessage::Instruction(_) => instructions += 1,
                ProcessorMessage::SlotComplete { slot, .. } => {
                    assert_eq!(slot, 1100);
                    completed = true;
                }
                ProcessorMessage::Regate { .. } => panic!("no Regate expected here"),
            }
        }
        assert_eq!(instructions, 0, "a failed tx must not be indexed");
        assert!(completed, "the slot still completes");
    }

    /// A transaction without meta cannot be proven successful or in scope, so the
    /// whole block fails closed and the slot is not completed (gap-fill replays it).
    #[tokio::test]
    async fn block_missing_meta_tx_errs_without_completing() {
        use std::str::FromStr;
        let program_id = Pubkey::from_str("J231K9UEpS4y4KAPwGc4gsMNCjKFRMYcQBcjVW7vBhVi").unwrap();
        // A well-formed withdraw that would otherwise parse, but carrying no meta.
        let mut no_meta = withdraw_tx_update(vec![9u8; 64], &program_id, 1)
            .transaction
            .unwrap();
        no_meta.meta = None;
        let block = block_update(1200, vec![no_meta]);

        let (tx, mut rx) = mpsc::channel(8);
        let res = handle_block(block, &program_id, ProgramType::Withdraw, &tx).await;
        assert!(res.is_err(), "a tx without meta must fail the block");
        drop(tx);

        let mut saw_complete = false;
        while let Some(m) = rx.recv().await {
            if matches!(m, ProcessorMessage::SlotComplete { .. }) {
                saw_complete = true;
            }
        }
        assert!(!saw_complete, "slot must not complete when a tx lacks meta");
    }
}

/// Parse all of the block's transactions, then send SlotComplete last so a completion never
/// precedes a tx. A malformed tx returns Err and the slot is not checkpointed, so gap-fill replays it.
async fn handle_block(
    block: yellowstone_grpc_proto::geyser::SubscribeUpdateBlock,
    program_id: &Pubkey,
    program_type: ProgramType,
    channel: &InstructionSender,
) -> Result<(), DataSourceRpcError> {
    let slot = block.slot;

    for tx_info in block.transactions {
        handle_transaction_info(tx_info, slot, program_id, program_type, channel).await?;
    }

    send_guaranteed(
        channel,
        ProcessorMessage::SlotComplete { slot, program_type },
        "SlotComplete (yellowstone)",
    )
    .await
    .map_err(|e| DataSourceRpcError::Protocol {
        reason: format!("SlotComplete send failed: {e}"),
    })?;

    Ok(())
}

/// Thin wrapper preserving the per-transaction entry point for the unit tests;
/// the live path now flows through `handle_block` instead.
#[cfg(all(test, feature = "datasource-rpc"))]
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

    handle_transaction_info(tx_info, slot, program_id, program_type, channel).await
}

async fn handle_transaction_info(
    tx_info: SubscribeUpdateTransactionInfo,
    slot: u64,
    program_id: &Pubkey,
    program_type: ProgramType,
    channel: &InstructionSender,
) -> Result<(), DataSourceRpcError> {
    // A tx without meta cannot be proven successful or in scope (it loses its revert status,
    // CPI events, and v0 ALT keys), so fail closed rather than index it: the slot is not
    // checkpointed and gap-fill replays it, the same guard the getBlock backfill applies.
    let Some(meta) = &tx_info.meta else {
        return Err(DataSourceRpcError::Protocol {
            reason: format!("transaction at slot {slot} missing meta"),
        });
    };

    // A failed on-chain tx is still in the block; skip it so a reverted deposit/withdrawal
    // is never indexed as real (the RPC decoder skips the same way; blocks cannot filter it).
    if meta.err.is_some() {
        return Ok(());
    }

    // ALT-resolved keys live on meta, not the message; capture them so inner and v0
    // top-level account indices resolve (no RPC).
    let inner_instructions_vec: Vec<InnerInstructions> = meta
        .inner_instructions
        .iter()
        .map(|ix_set| InnerInstructions {
            index: ix_set.index as u8,
            instructions: ix_set
                .instructions
                .iter()
                .map(|ix| InnerInstruction {
                    instruction: CompiledInstruction {
                        program_id_index: ix.program_id_index as u8,
                        accounts: ix.accounts.clone(),
                        data: bs58::encode(&ix.data).into_string(),
                    },
                    stack_height: ix.stack_height,
                })
                .collect(),
        })
        .collect();

    // Order matters: writable then readonly, matching execution order.
    let loaded_pubkeys: Vec<Pubkey> = match meta
        .loaded_writable_addresses
        .iter()
        .chain(meta.loaded_readonly_addresses.iter())
        .map(|bytes| Pubkey::try_from(bytes.as_slice()))
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(keys) => keys,
        Err(e) => {
            warn!("Skipping transaction at slot {slot}: invalid loaded address: {e}");
            return Ok(());
        }
    };

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
    let (static_keys, instructions): (
        Vec<Pubkey>,
        Vec<solana_sdk::message::compiled_instruction::CompiledInstruction>,
    ) = match &versioned_message {
        VersionedMessage::Legacy(msg) => (msg.account_keys.clone(), msg.instructions.clone()),
        VersionedMessage::V0(msg) => (msg.account_keys.clone(), msg.instructions.clone()),
    };

    // Full account list (static message keys, then loaded writable, then readonly) that inner and v0 top-level account indices reference.
    let mut account_keys = static_keys;
    account_keys.extend(loaded_pubkeys);

    info!(
        "Yellowstone received transaction at slot {}, signature: {}, {} instructions",
        slot,
        signature,
        instructions.len()
    );

    // Parse each top-level instruction that belongs to our program.
    for (ix_index, instruction) in instructions.into_iter().enumerate() {
        let program_id_index = instruction.program_id_index as usize;
        if program_id_index >= account_keys.len() {
            error!(
                "Invalid program_id_index {} for transaction {}",
                program_id_index, signature
            );
            continue;
        }

        if account_keys[program_id_index] != *program_id {
            continue; // Not our program
        }

        let compiled_ix = CompiledInstruction {
            program_id_index: instruction.program_id_index,
            accounts: instruction.accounts.clone(),
            data: bs58::encode(&instruction.data).into_string(),
        };
        let location = InstructionLocation::top_level(ix_index as u32);

        parse_and_send(
            &compiled_ix,
            &account_keys,
            &inner_instructions_vec,
            location,
            program_type,
            slot,
            &signature,
            channel,
        )
        .await?;
    }

    // Parse our program's inner (CPI) instructions, skipping the excluded operator/admin escrow discriminators.
    for inner_set in &inner_instructions_vec {
        for (inner_ix_index, inner) in inner_set.instructions.iter().enumerate() {
            let program_id_index = inner.instruction.program_id_index as usize;
            if account_keys.get(program_id_index) != Some(program_id) {
                continue; // Not our program
            }
            if inner_discriminator_excluded(program_type, &inner.instruction) {
                continue;
            }

            let location = InstructionLocation {
                top_level_index: inner_set.index as u32,
                inner: Some(InnerLocation {
                    inner_index: inner_ix_index as u32,
                    stack_height: inner.stack_height,
                }),
            };

            parse_and_send(
                &inner.instruction,
                &account_keys,
                &inner_instructions_vec,
                location,
                program_type,
                slot,
                &signature,
                channel,
            )
            .await?;
        }
    }

    Ok(())
}

/// Whether an inner (CPI) instruction's discriminator is one the indexer skips,
/// via the per-program predicate. Decodes the leading byte; an empty/undecodable
/// payload is never excluded (it parses to `Ok(None)` downstream).
fn inner_discriminator_excluded(
    program_type: ProgramType,
    instruction: &CompiledInstruction,
) -> bool {
    let Some(discriminator) = bs58::decode(&instruction.data)
        .into_vec()
        .ok()
        .and_then(|d| d.first().copied())
    else {
        return false;
    };
    match program_type {
        ProgramType::Escrow => {
            crate::indexer::datasource::common::parser::escrow::escrow_inner_discriminator_excluded(
                discriminator,
            )
        }
        ProgramType::Withdraw => {
            crate::indexer::datasource::common::parser::withdraw::withdraw_inner_discriminator_excluded(
                discriminator,
            )
        }
    }
}

/// Parse one compiled instruction and forward it on the processor channel; shared by the top-level and inner (CPI) paths so both produce identical rows.
#[allow(clippy::too_many_arguments)]
async fn parse_and_send(
    compiled_ix: &CompiledInstruction,
    account_keys: &[Pubkey],
    inner_instructions: &[InnerInstructions],
    location: InstructionLocation,
    program_type: ProgramType,
    slot: u64,
    signature: &str,
    channel: &InstructionSender,
) -> Result<(), DataSourceRpcError> {
    let instruction_data = match program_type {
        ProgramType::Escrow => {
            match parse_escrow_instruction(compiled_ix, account_keys, inner_instructions, location)
            {
                Ok(Some(inst)) => Some(ProgramInstruction::Escrow(Box::new(inst))),
                Ok(None) => {
                    debug!("Yellowstone: Unsupported escrow instruction at slot {slot}");
                    None
                }
                Err(e) => {
                    error!("Failed to parse escrow instruction at slot {slot}: {e}");
                    None
                }
            }
        }
        ProgramType::Withdraw => {
            match parse_withdraw_instruction(
                compiled_ix,
                account_keys,
                inner_instructions,
                location,
            ) {
                Ok(Some(inst)) => Some(ProgramInstruction::Withdraw(Box::new(inst))),
                Ok(None) => {
                    debug!("Yellowstone: Unsupported withdraw instruction at slot {slot}");
                    None
                }
                Err(e) => {
                    error!("Failed to parse withdraw instruction at slot {slot}: {e}");
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
            signature: Some(signature.to_string()),
            // A Solana tx holds at most a few hundred instructions, far below u32/i32 max, so this cast cannot wrap.
            instruction_index: location.top_level_index,
            inner_index: location.inner.map(|i| i.inner_index),
        };

        send_guaranteed(
            channel,
            ProcessorMessage::Instruction(instruction_meta),
            "instruction (yellowstone)",
        )
        .await
        .map_err(|e| DataSourceRpcError::Protocol {
            reason: format!("Instruction send failed: {e}"),
        })?;
    }

    Ok(())
}
