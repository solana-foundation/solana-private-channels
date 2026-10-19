use crate::channel_utils::send_guaranteed;
use crate::error::account::AccountError;
use crate::error::{OperatorError, StorageError};
use crate::operator::bitmap_constants::NONCES_PER_GENERATION;
use crate::operator::recovery::{load_pending_sigs, JournalError};
use crate::operator::sender::types::{PendingRemint, TransactionContext};
use crate::operator::solvency_fence::SolvencyFence;
use crate::operator::{
    fetch_bitmap_generation, fetch_bitmap_if_present, fetch_consumed_nonces,
    fetch_consumed_nonces_at, find_withdrawal_bitmap_pda, BitmapState, RetryConfig,
    RpcClientWithRetry,
};
use crate::operator::{
    MintCache, SourceEventId, TransactionKind, TransactionStatusUpdate, WithdrawalRemintInfo,
};
use crate::storage::common::storage::Storage;
use crate::storage::TransactionStatus;
use crate::{PrivateChannelIndexerConfig, ProgramType};
use chrono::Utc;
use private_channel_metrics::MetricLabel;
use solana_commitment_config::{CommitmentConfig, CommitmentLevel};
use solana_sdk::clock::MAX_PROCESSING_AGE;
use solana_sdk::pubkey::Pubkey;
use spl_associated_token_account::get_associated_token_address_with_program_id;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::{mpsc, Semaphore};
use tracing::{error, info, warn};

use super::remint::{FinalityRpc, REMINT_HYDRATE_PAGE};
use super::types::{InFlightQueue, SenderState, MAX_IN_FLIGHT};
use super::{classify_signatures, SigFinality};

impl SenderState {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        config: &PrivateChannelIndexerConfig,
        operator_commitment: CommitmentLevel,
        instance_pda: Option<Pubkey>,
        storage: Arc<Storage>,
        retry_max_attempts: u32,
        confirmation_poll_interval_ms: u64,
        source_rpc_client: Option<Arc<RpcClientWithRetry>>,
        solvency_fence: SolvencyFence,
    ) -> Result<Self, OperatorError> {
        // Initialize global RPC client with retry
        let rpc_client = Arc::new(RpcClientWithRetry::with_retry_config(
            config.rpc_url.clone(),
            RetryConfig::default(),
            CommitmentConfig {
                commitment: operator_commitment,
            },
        ));

        let mint_rpc_client = source_rpc_client.unwrap_or_else(|| rpc_client.clone());
        let mint_cache = MintCache::with_rpc(storage.clone(), mint_rpc_client.clone());

        // Optional destination fallback, same retry/commitment as its primary.
        // Empty means unset (env renders unconfigured as ""), so it maps to None.
        let fallback_rpc_client = config
            .fallback_rpc_url
            .as_deref()
            .filter(|s| !s.is_empty())
            .map(|url| {
                Arc::new(RpcClientWithRetry::with_retry_config(
                    url.to_string(),
                    RetryConfig::default(),
                    CommitmentConfig {
                        commitment: operator_commitment,
                    },
                ))
            });

        Ok(Self {
            rpc_client,
            // Source chain client (also used by MintCache). Remints broadcast here.
            source_rpc_client: mint_rpc_client,
            fallback_rpc_client,
            storage,
            instance_pda,
            in_flight_withdrawals: HashSet::new(),
            cached_generation: None,
            anchor_high_water: 0,
            refusal_floor: 0,
            retry_counts: HashMap::new(),
            rotation_retry_attempts: 0,
            rotation_in_flight: None,
            rotation_bound_generation: None,
            rotation_rearm_attempts: 0,
            rotation_blocked_passes: 0,
            mint_cache,
            mint_builders: HashMap::new(),
            retry_max_attempts,
            confirmation_poll_interval_ms,
            rotation_retry_queue: Vec::new(),
            pending_rotation: None,
            program_type: config.program_type,
            remint_cache: HashMap::new(),
            pending_signatures: HashMap::new(),
            release_leases: HashMap::new(),
            pending_remints: Vec::new(),
            remint_backlog_cursor: None,
            remint_runtime_ids: HashSet::new(),
            remint_backlog_unloaded: 0,
            in_flight: InFlightQueue::new(),
            semaphore: Arc::new(Semaphore::new(MAX_IN_FLIGHT)),
            solvency_fence,
            send_tasks: tokio::task::JoinSet::new(),
            consumed: None,
        })
    }
}

/// Three-way outcome of asking the chain whether a nonce's release landed.
///
/// `Uncertain` fails closed: every read, freshness and coverage ambiguity maps
/// here and never to `NotLanded`, because only `NotLanded` authorises paying the
/// user again.
pub(crate) enum ReleaseVerdict {
    /// The bit is set: the release landed, in the generation carried here.
    Landed { generation: u64 },
    /// The bit is clear inside the window the bitmap currently covers.
    NotLanded,
    /// The chain could not answer, which is not the same as answering "no".
    Uncertain(String),
    /// Finality has not yet passed the attempt's validity window, so the bits cannot answer
    /// yet. Unlike `Uncertain` this is expected and clears as the chain moves.
    Pending(String),
}

/// Prove on-chain whether withdrawal `nonce` was released, before anything acts
/// on a verdict that would pay the user a second time.
///
/// The hard part is proving the snapshot is fresh behind a load-balanced RPC
/// where two calls can hit different backends. The finalized blockhash and its
/// response context slot come from one call, so the slot and the tip height it
/// implies agree; the tip must be strictly past every attempt's last valid block
/// height, since at that height a release can still land. The bitmap is then read
/// bound to that slot, so a lagging backend errors instead of serving an older
/// snapshot whose clear bit would read as proof of non-release.
pub(crate) async fn verify_release_landed(
    rpc: &RpcClientWithRetry,
    instance_pda: Option<Pubkey>,
    nonce: u64,
    max_lvbh: u64,
) -> ReleaseVerdict {
    verify_release_landed_at(rpc, instance_pda, nonce, max_lvbh)
        .await
        .0
}

/// `verify_release_landed`, plus the bitmap's context slot when the read shows the release
/// window closed: `NotLanded` (tip past every lvbh) or the nonce's generation rotated away.
/// Every release of the nonce happened at or below that slot, so it bounds the window.
pub(crate) async fn verify_release_landed_at(
    rpc: &RpcClientWithRetry,
    instance_pda: Option<Pubkey>,
    nonce: u64,
    max_lvbh: u64,
) -> (ReleaseVerdict, Option<u64>) {
    let Some(instance_pda) = instance_pda else {
        return (
            ReleaseVerdict::Uncertain("no instance pda configured".to_string()),
            None,
        );
    };

    let (ref_slot, lvbh) = match rpc
        .get_latest_blockhash_with_context(CommitmentConfig::finalized())
        .await
    {
        Ok(v) => v,
        Err(e) => {
            return (
                ReleaseVerdict::Uncertain(format!("finalized blockhash read failed: {e}")),
                None,
            )
        }
    };
    // A blockhash stays valid for MAX_PROCESSING_AGE blocks past the tip it was
    // taken at, so its lvbh minus that window is the tip height at `ref_slot`.
    let Some(tip_height) = lvbh.checked_sub(MAX_PROCESSING_AGE as u64) else {
        return (
            ReleaseVerdict::Uncertain(format!(
                "finalized last valid block height {lvbh} below MAX_PROCESSING_AGE; \
                 cannot derive a tip height"
            )),
            None,
        );
    };
    let pending = (tip_height <= max_lvbh).then(|| {
        ReleaseVerdict::Pending(format!(
            "finalized tip height {tip_height} is not past the attempt's last valid block \
             height {max_lvbh}, so the bits are too stale to prove non-release"
        ))
    });

    // Read even while pending: a rotation is final whatever the lvbh, and its slot bounds the window.
    let (bitmap, slot) = match fetch_consumed_nonces_at(
        rpc,
        &find_withdrawal_bitmap_pda(&instance_pda),
        Some(ref_slot),
        CommitmentConfig::finalized(),
    )
    .await
    {
        Ok(read) => read,
        Err(e) => {
            let verdict = pending
                .unwrap_or_else(|| ReleaseVerdict::Uncertain(format!("bitmap read failed: {e}")));
            return (verdict, None);
        }
    };

    // Rotation clears every bit, so outside the current window a clear bit is
    // indistinguishable from a release that happened and was then wiped.
    if nonce / NONCES_PER_GENERATION < bitmap.generation {
        return (
            ReleaseVerdict::Uncertain(format!(
                "the bitmap rotated to generation {} and its bits say nothing about nonce {nonce}",
                bitmap.generation
            )),
            Some(slot),
        );
    }
    if let Some(pending) = pending {
        return (pending, None);
    }
    if !bitmap.covers(nonce) {
        return (
            ReleaseVerdict::Uncertain(format!(
                "the bitmap is on generation {} and its bits say nothing about nonce {nonce}",
                bitmap.generation
            )),
            None,
        );
    }

    if bitmap.is_consumed(nonce) {
        (
            ReleaseVerdict::Landed {
                generation: bitmap.generation,
            },
            None,
        )
    } else {
        (ReleaseVerdict::NotLanded, Some(slot))
    }
}

/// Whether `nonce`'s bit is already set at confirmed. Anchored at the finalized tip, so a
/// backend behind it errors instead of answering with bits older than that proof.
pub(crate) async fn release_seen_at_confirmed(
    rpc: &RpcClientWithRetry,
    instance_pda: Pubkey,
    nonce: u64,
) -> Result<bool, String> {
    let anchor = finalized_anchor(rpc).await.map_err(|e| e.to_string())?;
    let bitmap = fetch_consumed_nonces(
        rpc,
        &find_withdrawal_bitmap_pda(&instance_pda),
        Some(anchor),
        CommitmentConfig::confirmed(),
    )
    .await
    .map_err(|e| format!("confirmed bitmap read failed: {e}"))?;
    if !bitmap.covers(nonce) {
        return Err(format!(
            "the confirmed bitmap is on generation {} and its bits say nothing about nonce {nonce}",
            bitmap.generation
        ));
    }
    Ok(bitmap.is_consumed(nonce))
}

/// Boot check of released nonces, from the current generation up, against Completed rows.
/// A set bit with no Completed row boots only when a landed journaled release on that
/// row explains it. A Completed row with a clear bit claims a release the chain never
/// made, and a DB numbering behind the chain's generation was restored; both refuse.
pub(crate) async fn validate_bitmap_consistency(
    storage: &Storage,
    rpc_client: &RpcClientWithRetry,
    fallback_rpc_client: Option<&RpcClientWithRetry>,
    instance_pda: Option<Pubkey>,
) -> Result<(), OperatorError> {
    let instance_pda = instance_pda.ok_or_else(|| AccountError::InstanceNotFound {
        instance: Pubkey::default(),
    })?;
    let bitmap_pda = find_withdrawal_bitmap_pda(&instance_pda);

    info!(
        instance = %instance_pda,
        bitmap = %bitmap_pda,
        "Validating withdrawal bitmap against completed withdrawals"
    );

    // Finalized, not the client's commitment: releases stay processing until final, and a
    // fork-only bit on such a row would otherwise send it to manual review at boot.
    let bitmap = fetch_bitmap_if_present(
        rpc_client,
        &bitmap_pda,
        Some(finalized_anchor(rpc_client).await?),
        CommitmentConfig::finalized(),
    )
    .await?;
    // The program creates the bitmap with the instance and never closes it, so absent
    // at a finalized slot means nothing was ever released. That is an empty generation 0.
    let bitmap = bitmap.unwrap_or_else(|| {
        warn!(
            bitmap = %bitmap_pda,
            "Withdrawal bitmap does not exist; diffing against an empty generation 0"
        );
        BitmapState {
            generation: 0,
            consumed: Vec::new(),
        }
    });
    refuse_if_numbering_behind(storage, bitmap.generation).await?;
    let (mut db_only, mut chain_only) = diff_bitmap(storage, &bitmap).await?;

    // The bitmap and the database are read at different instants, so a release
    // that lands between them looks exactly like the database running ahead.
    // A second read after the fact tells the two apart: a real divergence
    // survives it, a race does not.
    if !db_only.is_empty() {
        warn!(
            nonces = ?db_only,
            "Completed withdrawals appear unconsumed on-chain; re-reading the bitmap before halting"
        );
        // A re-read that cannot be taken clears nothing, so the first verdict stands
        // and the divergence is reported, not a vaguer read error.
        match confirm_divergence(storage, rpc_client, &bitmap_pda).await {
            Ok((rediffed_db_only, rediffed_chain_only)) => {
                db_only = rediffed_db_only;
                chain_only = rediffed_chain_only;
            }
            Err(e) => warn!(
                "Could not re-read the bitmap to confirm the divergence, keeping the first verdict: {e}"
            ),
        }
    }

    if !db_only.is_empty() {
        crate::metrics::OPERATOR_TRANSACTION_ERRORS
            .with_label_values(&[ProgramType::Withdraw.as_label(), "bitmap_divergence"])
            .inc();
        error!(
            instance = %instance_pda,
            generation = bitmap.generation,
            db_only = ?db_only,
            chain_only = ?chain_only,
            "Withdrawal bitmap divergence: the database claims releases the chain never made. \
             Refusing to start; reconcile these nonces before restarting."
        );
        return Err(crate::error::ProgramError::BitmapDivergence {
            db_only,
            chain_only,
        }
        .into());
    }

    if chain_only.is_empty() {
        info!(
            generation = bitmap.generation,
            consumed = bitmap.consumed.len(),
            "Withdrawal bitmap verification passed"
        );
        return Ok(());
    }

    warn!(
        generation = bitmap.generation,
        nonces = ?chain_only,
        "Releases landed on-chain without a Completed row; resolving from broadcast signatures"
    );
    // The bitmap is the withdraw role's, so its releases were broadcast to Solana.
    let finality = FinalityRpc::solana(rpc_client, fallback_rpc_client);
    let mut paid_twice = Vec::new();
    let mut unexplained = Vec::new();
    for nonce in &chain_only {
        match resolve_chain_ahead_nonce(storage, &finality, *nonce).await {
            ChainAheadOutcome::Repaired | ChainAheadOutcome::Explained => {}
            ChainAheadOutcome::Unexplained => unexplained.push(*nonce),
            ChainAheadOutcome::DoublePayout => paid_twice.push(*nonce),
        }
    }

    // A nonce that was refunded and also released is money out of the instance
    // twice over. Unlike an ordinary chain-ahead gap there is nothing to repair
    // and no version of the history in which the operator was right, so it stops
    // here rather than continuing to send withdrawals from a short balance.
    if !paid_twice.is_empty() {
        error!(
            instance = %instance_pda,
            generation = bitmap.generation,
            nonces = ?paid_twice,
            "Withdrawal bitmap divergence: these nonces were reminted to the user and released \
             on-chain. Refusing to start; reconcile the double payouts before restarting."
        );
        return Err(crate::error::ProgramError::BitmapDivergence {
            db_only: Vec::new(),
            chain_only: paid_twice,
        }
        .into());
    }

    // Amount or recipient cannot tell two burns apart and a release names no burn, so
    // a set bit no landed journaled signature explains might pay any pending row twice.
    if !unexplained.is_empty() {
        error!(
            instance = %instance_pda,
            generation = bitmap.generation,
            nonces = ?unexplained,
            "Consumed withdrawal nonces have no row that proves the release; the indexer \
             database was likely restored past them. Refusing to start."
        );
        return Err(crate::error::ProgramError::UnexplainedConsumedNonces {
            nonces: unexplained,
        }
        .into());
    }

    Ok(())
}

/// Refuse when the DB's numbering sits in a generation the chain rotated past. Rotation
/// is only armed by a row already numbered in the next generation, so this never fires
/// in normal operation, and a DB numbering ahead of the chain is the usual wait.
async fn refuse_if_numbering_behind(
    storage: &Storage,
    chain_generation: u64,
) -> Result<(), OperatorError> {
    let db_generation = storage
        .get_max_withdrawal_nonce()
        .await?
        .map(|nonce| nonce / NONCES_PER_GENERATION);
    if db_generation.unwrap_or(0) < chain_generation {
        crate::metrics::OPERATOR_TRANSACTION_ERRORS
            .with_label_values(&[ProgramType::Withdraw.as_label(), "bitmap_divergence"])
            .inc();
        return Err(crate::error::ProgramError::NonceGenerationBehindChain {
            db_generation,
            chain_generation,
        }
        .into());
    }
    Ok(())
}

/// The finalized context slot to bind a bitmap read to. Behind a load balancer
/// two calls can land on different backends, and a lagging one shows clear bits
/// for nonces that were released; bound to this slot it errors instead.
async fn finalized_anchor(rpc_client: &RpcClientWithRetry) -> Result<u64, OperatorError> {
    rpc_client
        .get_latest_blockhash_with_context(CommitmentConfig::finalized())
        .await
        .map(|(slot, _)| slot)
        .map_err(|e| {
            crate::error::ProgramError::BitmapUnavailable {
                reason: format!("finalized anchor read failed: {e}"),
            }
            .into()
        })
}

/// Take the confirmatory second read and re-diff it against the database.
///
/// The anchor is taken fresh here on purpose. The re-read exists to give a
/// release racing the first read a chance to land, and reusing the first slot
/// would return the identical snapshot.
async fn confirm_divergence(
    storage: &Storage,
    rpc_client: &RpcClientWithRetry,
    bitmap_pda: &Pubkey,
) -> Result<(Vec<u64>, Vec<u64>), OperatorError> {
    let bitmap = fetch_consumed_nonces(
        rpc_client,
        bitmap_pda,
        Some(finalized_anchor(rpc_client).await?),
        CommitmentConfig::finalized(),
    )
    .await?;
    diff_bitmap(storage, &bitmap).await
}

/// Split the nonces from the current generation up into "database only" and "chain only".
/// Older generations are skipped because a rotation cleared their bits. Later ones are
/// kept because the chain cannot release a nonce before rotating into its generation.
async fn diff_bitmap(
    storage: &Storage,
    bitmap: &BitmapState,
) -> Result<(Vec<u64>, Vec<u64>), OperatorError> {
    // The nonce column is signed, so both bounds stop at i64::MAX. A corrupt generation gives an empty window.
    let max_nonce = i64::MAX as u64;
    let min_nonce = bitmap
        .generation
        .saturating_mul(NONCES_PER_GENERATION)
        .min(max_nonce);

    let completed: HashSet<u64> = storage
        .get_completed_withdrawal_nonces(min_nonce, max_nonce)
        .await?
        .into_iter()
        .collect();
    let consumed: HashSet<u64> = bitmap.consumed.iter().copied().collect();

    let mut db_only: Vec<u64> = completed.difference(&consumed).copied().collect();
    let mut chain_only: Vec<u64> = consumed.difference(&completed).copied().collect();
    db_only.sort_unstable();
    chain_only.sort_unstable();

    Ok((db_only, chain_only))
}

/// What a chain-ahead nonce turned out to be, which decides whether startup
/// can continue past it.
///
/// The three cases differ in what is still true afterwards: one closes the gap,
/// one leaves a real payout unattributed for a human, and one says the instance
/// paid out twice.
enum ChainAheadOutcome {
    /// The row was still open, so the repair write lands and closes the gap.
    Repaired,
    /// A landed journaled release on a row the repair cannot write; nothing is written.
    Explained,
    /// No landed journaled signature ties the payout to a row, so boot refuses.
    Unexplained,
    /// The user was refunded and the chain released the nonce as well.
    DoublePayout,
}

/// Count and log a chain-ahead nonce the boot repair could not explain.
fn report_unexplained(nonce: u64) {
    crate::metrics::OPERATOR_TRANSACTION_ERRORS
        .with_label_values(&[ProgramType::Withdraw.as_label(), "bitmap_divergence"])
        .inc();
    crate::metrics::BITMAP_UNEXPLAINED_NONCE
        .with_label_values(&[ProgramType::Withdraw.as_label()])
        .inc();
    error!(nonce, "Consumed nonce could not be explained at boot");
}

/// Repair a single nonce the chain consumed but the database never recorded.
///
/// A landed signature proves which broadcast paid out, so the row becomes
/// Completed against it. Anything else leaves the payout real but unattributed,
/// which a human has to close out.
///
/// The completion is a CAS on the row as read before the RPC call, so a row that
/// moved in the meantime is left alone. Those cases are reported rather than
/// repaired, because claiming a repair that the database dropped is how a real
/// divergence disappears into an info log.
async fn resolve_chain_ahead_nonce(
    storage: &Storage,
    finality: &FinalityRpc<'_>,
    nonce: u64,
) -> ChainAheadOutcome {
    let row = match storage.get_withdrawal_by_nonce(nonce).await {
        Ok(Some(row)) => row,
        Ok(None) => {
            error!(
                nonce,
                "Nonce consumed on-chain has no withdrawal row; funds moved with no record"
            );
            report_unexplained(nonce);
            return ChainAheadOutcome::Unexplained;
        }
        Err(e) => {
            error!(
                nonce,
                "Could not load withdrawal row for consumed nonce: {e}"
            );
            report_unexplained(nonce);
            return ChainAheadOutcome::Unexplained;
        }
    };

    // The burn was already refunded, so a consumed bit means the release paid
    // the user as well. No status write can undo either half of that, and the
    // instance is short by the amount, so this is reported rather than repaired
    // and the caller stops the operator on it.
    if row.status == TransactionStatus::FailedReminted || row.landed_remint_signature.is_some() {
        error!(
            nonce,
            transaction_id = row.id,
            "Nonce was reminted to the user and also released on-chain; the funds moved twice"
        );
        crate::metrics::OPERATOR_TRANSACTION_ERRORS
            .with_label_values(&[ProgramType::Withdraw.as_label(), "bitmap_divergence"])
            .inc();
        return ChainAheadOutcome::DoublePayout;
    }

    // Only these two are still writable; anywhere else the update is discarded.
    let repairable = matches!(
        row.status,
        TransactionStatus::Processing | TransactionStatus::PendingRemint
    );

    // Like an unread row, an unread journal proves nothing, so nothing is written and boot refuses.
    let signatures = match load_pending_sigs(storage, row.id).await {
        Ok(signatures) => signatures,
        Err(e) => {
            error!(
                nonce,
                transaction_id = row.id,
                "Release signature journal unusable, leaving the row as it is: {e}"
            );
            report_unexplained(nonce);
            return ChainAheadOutcome::Unexplained;
        }
    };

    let verdict = if signatures.is_empty() {
        None
    } else {
        match classify_signatures(finality, &signatures).await {
            SigFinality::Landed(sig) => Some(sig),
            SigFinality::Live(reason) | SigFinality::Uncertain(reason) => {
                warn!(nonce, transaction_id = row.id, "Unresolved: {reason}");
                None
            }
            SigFinality::Dead => None,
        }
    };

    // Only a payout attributed to one of our sends is closed here; the rest are left for a human.
    //
    // Both CASes pin the `updated_at` read before the RPC call, so a row a replacement
    // operator moved meanwhile is not completed from under it. The generic status
    // writer checks neither version nor lock and must not carry this write.
    if let (Some(sig), true) = (verdict, repairable) {
        let completed = match row.status {
            // Fenced on the sender lock: the sender may be reminting this row.
            TransactionStatus::PendingRemint => {
                storage
                    .try_complete_stalled_withdrawal(
                        row.id,
                        row.updated_at,
                        TransactionStatus::PendingRemint,
                        Some(sig.to_string()),
                    )
                    .await
            }
            _ => {
                storage
                    .try_complete_processing(row.id, row.updated_at, Some(sig.to_string()), None)
                    .await
            }
        };
        return match completed {
            Ok(true) => {
                info!(
                    nonce,
                    transaction_id = row.id,
                    status = ?row.status,
                    "Consumed nonce matched a landed signature; marked the row Completed"
                );
                ChainAheadOutcome::Repaired
            }
            outcome => {
                error!(
                    nonce,
                    transaction_id = row.id,
                    status = ?row.status,
                    ?outcome,
                    "Consumed nonce matched a landed signature but the repair did not apply"
                );
                report_unexplained(nonce);
                ChainAheadOutcome::Unexplained
            }
        };
    }

    // The row's own landed release pays this nonce, so a re-indexed burn cannot reuse it.
    // The ManualReview sweep or a human closes the row.
    if let Some(sig) = verdict {
        warn!(
            nonce,
            transaction_id = row.id,
            status = ?row.status,
            %sig,
            "Consumed nonce is explained by this row's landed release; the row is left as it is"
        );
        return ChainAheadOutcome::Explained;
    }

    // No row is written: boot refuses, and a status write here would turn an RPC blip
    // into a permanent manual review. The runbook completes rows from observed releases.
    error!(
        nonce,
        transaction_id = row.id,
        status = ?row.status,
        repairable,
        "Consumed nonce cannot be tied to this row's landed release"
    );
    report_unexplained(nonce);
    ChainAheadOutcome::Unexplained
}

impl SenderState {
    /// Read the generation the withdrawal bitmap is currently on.
    ///
    /// The chain is the authority; this answer is never inferred from local
    /// state. Callers that want it remembered go through `refresh_generation`
    /// instead, which is the only way a generation reaches the cache and is
    /// what keeps the cache from ever holding a number nobody read.
    pub(super) async fn fetch_current_generation(&mut self) -> Result<u64, OperatorError> {
        let instance_pda = self.instance_pda.ok_or(AccountError::InstanceNotFound {
            instance: Pubkey::default(),
        })?;
        let read = match self.generation_floor().await {
            Ok(floor) => {
                fetch_bitmap_generation(
                    &self.rpc_client,
                    &find_withdrawal_bitmap_pda(&instance_pda),
                    floor,
                )
                .await
            }
            Err(e) => Err(e),
        };
        match read {
            Ok((generation, slot)) => {
                self.anchor_high_water = self.anchor_high_water.max(slot);
                Ok(generation)
            }
            Err(e) => {
                crate::metrics::OPERATOR_TRANSACTION_ERRORS
                    .with_label_values(&[self.program_type.as_label(), "generation_read_failed"])
                    .inc();
                Err(e)
            }
        }
    }

    /// The slot a generation read must answer at or past: the endpoint's finalized tip, never
    /// below an earlier anchor, answer or refusal this sender saw. A node lagging evenly can still
    /// answer at its own old tip; that costs at most one refused send, never a wrong payout.
    pub(super) async fn generation_floor(&mut self) -> Result<u64, OperatorError> {
        let anchor = finalized_anchor(&self.rpc_client).await?;
        self.anchor_high_water = self.anchor_high_water.max(anchor);
        Ok(self.anchor_high_water.max(self.refusal_floor))
    }

    /// Read the current generation and remember it.
    ///
    /// Every cache write funnels through here or through a confirmed rotation,
    /// so the cached value is always something the chain reported rather than
    /// something the operator guessed. A failed read leaves the previous value
    /// alone: it was already true at some point, which is more than a guess.
    pub(super) async fn refresh_generation(&mut self) -> Result<u64, OperatorError> {
        let generation = self.fetch_current_generation().await?;
        self.cached_generation = Some(generation);
        Ok(generation)
    }

    /// Sends a ManualReview status update during startup recovery when a stored
    /// transaction cannot be reconstructed (e.g. unparseable pubkey or signature).         
    /// Using send_guaranteed so the alert is never silently dropped.                       
    async fn send_recovery_manual_review(
        storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
        transaction_id: i64,
        trace_id: &str,
        reason: &str,
    ) {
        send_guaranteed(
            storage_tx,
            TransactionStatusUpdate {
                transaction_id,
                trace_id: Some(trace_id.to_string()),
                status: TransactionStatus::ManualReview,
                counterpart_signature: None,
                processed_at: Some(Utc::now()),
                error_message: Some(format!("recovery failed: {}", reason)),
                remint_signature: None,
                remint_attempted: false,
                alert_only: false,
            },
            "transaction status update",
        )
        .await
        .ok();
    }

    /// On an error, logs it and sends a ManualReview update. Returns `None` on error.
    async fn or_manual_review<T>(
        result: Result<T, String>,
        storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
        tx_id: i64,
        trace_id: &str,
    ) -> Option<T> {
        match result {
            Ok(value) => Some(value),
            Err(msg) => {
                error!(transaction_id = tx_id, "Recovery: {}", msg);
                Self::send_recovery_manual_review(storage_tx, tx_id, trace_id, &msg).await;

                None
            }
        }
    }

    pub(super) async fn recover_pending_remints(
        &mut self,
        storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
    ) -> Result<(), OperatorError> {
        // Deferred remints are Withdraw-only; another role sharing the database
        // must never claim a row it would classify against the wrong chain.
        if self.program_type != ProgramType::Withdraw {
            return Ok(());
        }
        // Armed only at boot, so a sender that never recovered never pages rows behind its queue.
        // Ids are BIGSERIAL, so 0 is below every row.
        self.remint_backlog_cursor = Some(0);
        self.hydrate_pending_remints(storage_tx).await
    }

    /// Load the next page of durable pending remints; a no-op once the backlog is drained.
    pub(super) async fn hydrate_pending_remints(
        &mut self,
        storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
    ) -> Result<(), OperatorError> {
        let Some(after_id) = self.remint_backlog_cursor else {
            return Ok(());
        };
        // Counted first so the remint gauge can still show the whole backlog, not just this page.
        let waiting = self
            .storage
            .count_pending_remint_transactions(after_id)
            .await?;
        // Paged by id, not deadline: ids never change, so every row is read at most once.
        let transactions = self
            .storage
            .get_pending_remint_transactions(after_id, REMINT_HYDRATE_PAGE as i64)
            .await?;

        // A short page means no unread row is left; ids are ascending, so the last is the max.
        self.remint_backlog_cursor = match transactions.last() {
            Some(last) if transactions.len() == REMINT_HYDRATE_PAGE => Some(last.id),
            _ => None,
        };
        self.remint_backlog_unloaded = match self.remint_backlog_cursor {
            Some(_) => (waiting.max(0) as u64).saturating_sub(transactions.len() as u64),
            None => 0,
        };
        // A runtime handoff may still have its terminal write queued, e.g. a ManualReview.
        // Reloading it could remint a row that then lands in manual review and is refunded again.
        let transactions: Vec<_> = transactions
            .into_iter()
            .filter(|tx| !self.remint_runtime_ids.contains(&tx.id))
            .collect();

        if transactions.is_empty() {
            self.forget_runtime_ids_once_drained();
            return Ok(());
        }

        info!(
            "Recovering {} pending remint(s) from database",
            transactions.len()
        );

        // PrivateChannel only supports SPL Token for now.
        let private_channel_token_program = self.mint_cache.get_private_channel_token_program();

        // Id of the last row fully handled, where a failed journal read rewinds the cursor.
        let mut done_through = after_id;
        for tx in transactions {
            let resume_after = std::mem::replace(&mut done_through, tx.id);
            // Parse pubkeys stored as strings. On any failure we cannot remint safely,
            // and silently skipping would leave the row stuck in PendingRemint on every
            // restart — so we escalate to ManualReview.
            let Some(mint) = Self::or_manual_review(
                Pubkey::from_str(&tx.mint).map_err(|e| format!("invalid mint pubkey: {e}")),
                storage_tx,
                tx.id,
                &tx.trace_id,
            )
            .await
            else {
                continue;
            };

            // The burn debited the initiator, so the remint credits them, not
            // `recipient` (the Solana destination of the failed release).
            let Some(initiator) = Self::or_manual_review(
                Pubkey::from_str(&tx.initiator)
                    .map_err(|e| format!("invalid initiator pubkey: {e}")),
                storage_tx,
                tx.id,
                &tx.trace_id,
            )
            .await
            else {
                continue;
            };

            let initiator_ata = get_associated_token_address_with_program_id(
                &initiator,
                &mint,
                &private_channel_token_program,
            );

            let amount = tx.amount.value();

            // The journal is the authority, as on the live handoff: only it carries
            // each attempt's blockhash slot, which the coverage proof needs. An empty
            // or corrupt journal cannot prove the release dead, so it escalates.
            let parsed = match load_pending_sigs(&self.storage, tx.id).await {
                Ok(sigs) if sigs.is_empty() => {
                    Err("no withdrawal signatures stored; cannot verify finality".to_string())
                }
                Ok(sigs) => Ok(sigs),
                Err(JournalError::Corrupt(reason)) => Err(reason),
                // A failed read says nothing about the row, so fail like the row query
                // above instead of sending it to ManualReview. The rewind makes the next
                // page read retry this row and every one after it.
                Err(JournalError::Unavailable(reason)) => {
                    self.remint_backlog_cursor = Some(resume_after);
                    return Err(StorageError::DatabaseError {
                        message: format!(
                            "pending remint recovery of transaction {}: {reason}",
                            tx.id
                        ),
                    }
                    .into());
                }
            };

            let Some(signatures) =
                Self::or_manual_review(parsed, storage_tx, tx.id, &tx.trace_id).await
            else {
                continue;
            };

            // Restore the original deadline. Fall back to now() if missing (shouldn't
            // happen) so the entry fires on the next tick instead of waiting 32s more.
            let deadline = tx.pending_remint_deadline_at.unwrap_or_else(Utc::now);

            let ctx = TransactionContext {
                kind: TransactionKind::ReleaseFunds,
                transaction_id: Some(tx.id),
                // Carried so logs and the bitmap gate can name this withdrawal.
                withdrawal_nonce: tx.withdrawal_nonce.map(|n| n as u64),
                trace_id: Some(tx.trace_id.clone()),
                deposit_claim_lease: None,
            };

            let remint_info = WithdrawalRemintInfo {
                transaction_id: tx.id,
                source_event_id: SourceEventId::new(
                    &tx.signature,
                    tx.instruction_index,
                    tx.inner_index,
                ),
                trace_id: tx.trace_id.clone(),
                mint,
                user: initiator,
                user_ata: initiator_ata,
                token_program: private_channel_token_program,
                amount,
            };

            info!(
                transaction_id = tx.id,
                nonce = ctx.withdrawal_nonce.map(|n| n as i64),
                sigs = signatures.len(),
                "Recovered PendingRemint, deadline={}",
                deadline,
            );

            // A corrupt negative value would wrap to a huge u32 and skip the
            // attempt cap, defeating the whole point of persisting it.
            let Some(finality_check_attempts) = Self::or_manual_review(
                u32::try_from(tx.finality_check_attempts).map_err(|_| {
                    format!(
                        "negative finality_check_attempts: {}",
                        tx.finality_check_attempts
                    )
                }),
                storage_tx,
                tx.id,
                &tx.trace_id,
            )
            .await
            else {
                continue;
            };

            self.pending_remints.push(PendingRemint {
                ctx,
                remint_info,
                signatures,
                // The original error string is not stored in DB. Only surfaced in
                // combined error messages if the remint itself also fails.
                original_error: "recovered from persistent storage".to_string(),
                deadline,
                finality_check_attempts,
                // Only a refusal the program itself made frees the refund from
                // the bitmap gate, and it is durable precisely so a restart
                // inside the finality window cannot turn an automatic refund
                // into a manual one. Anything else is held to the ordinary gate.
                release_refused_on_chain: tx.release_refused_on_chain,
                coverage_slot: None,
                coverage_checkpoint: None,
                free_waits: 0,
            });
        }

        self.forget_runtime_ids_once_drained();
        Ok(())
    }

    /// Runtime ids only guard against reloading while the backlog drains, so drop them once
    /// it has. Done after a page loads, since a rewind may page past them again.
    fn forget_runtime_ids_once_drained(&mut self) {
        if self.remint_backlog_cursor.is_none() {
            self.remint_runtime_ids.clear();
        }
    }

    /// Queue a remint handed off at runtime; while the backlog drains, paging must never reload it.
    pub(super) fn queue_runtime_remint(&mut self, entry: PendingRemint) {
        if let (Some(_), Some(id)) = (self.remint_backlog_cursor, entry.ctx.transaction_id) {
            self.remint_runtime_ids.insert(id);
        }
        self.pending_remints.push(entry);
    }

    /// Queued remints plus backlog rows not loaded yet, the value the remint gauge reports.
    /// While the backlog drains, a runtime row past the cursor can be counted twice.
    pub(super) fn remint_backlog_size(&self) -> usize {
        self.pending_remints.len() + self.remint_backlog_unloaded as usize
    }

    /// Backlog entries in the queue, the count the refill threshold uses.
    pub(super) fn queued_backlog_remints(&self) -> usize {
        self.pending_remints
            .iter()
            .filter(|e| {
                e.ctx
                    .transaction_id
                    .is_none_or(|id| !self.remint_runtime_ids.contains(&id))
            })
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::sender::test_support::{
        mock_bitmap_account, mock_bitmap_at_commitment, mock_bitmap_at_slot, mock_bitmap_sequence,
        mock_bitmap_then_read_failure, mock_finalized_anchor, sender_state_with_storage,
        sender_state_with_storage_and_role,
    };
    use crate::storage::common::amount::TokenAmount;
    use crate::storage::common::models::{
        DbTransaction, StoredSig, TransactionStatus, TransactionType,
    };
    use crate::storage::common::storage::mock::MockStorage;
    use crate::storage::Storage;
    use solana_sdk::pubkey::Pubkey;
    use solana_sdk::signature::Signature;
    use tokio::sync::mpsc;

    fn make_sender_state(mock: MockStorage) -> SenderState {
        make_sender_state_with_role(mock, ProgramType::Withdraw)
    }

    fn make_sender_state_with_role(mock: MockStorage, role: ProgramType) -> SenderState {
        sender_state_with_storage_and_role("http://localhost:8899", mock, role)
    }

    /// Build a minimal DbTransaction representing a PendingRemint row.
    /// All string fields use real base58-encoded pubkeys and signatures so
    /// `recover_pending_remints` can parse them without error.
    fn make_pending_remint_row(
        id: i64,
        mint: &Pubkey,
        initiator: &Pubkey,
        recipient: &Pubkey,
        sig: &Signature,
        deadline: chrono::DateTime<Utc>,
    ) -> DbTransaction {
        let now = Utc::now();
        DbTransaction {
            id,
            signature: Signature::new_unique().to_string(),
            trace_id: format!("trace-{id}"),
            slot: 100,
            initiator: initiator.to_string(),
            recipient: recipient.to_string(),
            mint: mint.to_string(),
            amount: TokenAmount(5_000),
            memo: None,
            transaction_type: TransactionType::Withdrawal,
            withdrawal_nonce: Some(id),
            status: TransactionStatus::PendingRemint,
            created_at: now,
            updated_at: now,
            processed_at: None,
            counterpart_signature: None,
            remint_signatures: Some(vec![sig.to_string()]),
            remint_last_valid_block_heights: Some(vec![12_345]),
            pending_remint_deadline_at: Some(deadline),
            finality_check_attempts: 0,
            recovery_requeue_attempts: 0,
            instruction_index: 0,
            inner_index: None,
            landed_remint_signature: None,
            release_refused_on_chain: false,
        }
    }

    /// Journal the release signatures the row mirrors, as the handoff writes both
    /// from one journal read.
    fn journal_mirrored_releases(
        mock: &MockStorage,
        row: &DbTransaction,
        blockhash_slot: Option<i64>,
    ) {
        let signatures = row.remint_signatures.clone().unwrap_or_default();
        let heights = row
            .remint_last_valid_block_heights
            .clone()
            .unwrap_or_default();
        let journal = signatures
            .into_iter()
            .zip(heights)
            .map(|(signature, last_valid_block_height)| StoredSig {
                signature,
                last_valid_block_height,
                blockhash_slot,
            })
            .collect();
        mock.release_signatures
            .lock()
            .unwrap()
            .insert(row.id, journal);
    }

    // ── recover_pending_remints: happy path ──────────────────────────

    /// On startup, the first page of PendingRemint rows must be fully
    /// reconstructed into the in-memory `pending_remints` queue so the
    /// operator can continue where it left off before the crash.
    ///
    /// This test verifies that every field is correctly restored:
    /// - transaction_id, trace_id, amount, mint, initiator
    /// - withdrawal signatures (needed for the finality check)
    /// - the original deadline (not a fresh 32s window — the clock keeps
    ///   ticking across restarts)
    /// - finality_check_attempts round-trips from the DB so the
    ///   MAX_FINALITY_CHECK_ATTEMPTS budget survives restarts
    ///
    /// No channel messages should be sent — there is nothing wrong with
    /// these rows, they just need to be re-queued.
    #[tokio::test]
    async fn recover_pending_remints_rehydrates_queue() {
        let mock = MockStorage::new();
        let mint = Pubkey::new_unique();
        // Distinct from `recipient`: the burn debited the initiator, so the
        // remint must target them, not the withdrawal's Solana destination.
        let initiator = Pubkey::new_unique();
        let recipient = Pubkey::new_unique();
        let sig = Signature::new_unique();
        let deadline = Utc::now() + chrono::Duration::seconds(20);

        // Mid-budget value so the round-trip assertion is meaningful: a reset
        // to 0 on recovery would re-arm the cap and let an ambiguous row
        // outlive the intended ManualReview escalation.
        let mut row = make_pending_remint_row(42, &mint, &initiator, &recipient, &sig, deadline);
        row.finality_check_attempts = 2;
        let blockhash_slot = 12_500;
        journal_mirrored_releases(&mock, &row, Some(blockhash_slot));
        mock.pending_remint_transactions.lock().unwrap().push(row);

        let mut state = make_sender_state(mock);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        state.recover_pending_remints(&storage_tx).await.unwrap();

        // Exactly one entry should be re-queued.
        assert_eq!(state.pending_remints.len(), 1);
        let entry = &state.pending_remints[0];

        // Identity fields.
        assert_eq!(entry.ctx.transaction_id, Some(42));
        assert_eq!(entry.ctx.trace_id.as_deref(), Some("trace-42"));

        // Amount must be correctly cast from i64 → u64.
        assert_eq!(entry.remint_info.amount, 5_000u64);

        // Pubkeys must be correctly parsed from their string representation.
        assert_eq!(entry.remint_info.mint, mint);

        // The remint reverses a burn, so it must credit the account that was
        // debited (initiator), not the withdrawal's Solana destination.
        assert_eq!(entry.remint_info.user, initiator);
        assert_ne!(entry.remint_info.user, recipient);
        assert_eq!(
            entry.remint_info.user_ata,
            get_associated_token_address_with_program_id(&initiator, &mint, &spl_token::id()),
            "remint must mint into the initiator's private channel ATA"
        );

        // Signatures must be parsed back — they drive the finality check.
        // lvbh must round-trip too: the gate needs it to prove a broadcast
        // can no longer land.
        assert_eq!(entry.signatures.len(), 1);
        assert_eq!(entry.signatures[0].signature, sig);
        assert_eq!(entry.signatures[0].last_valid_block_height, 12_345);
        // A pruned endpoint can prove the attempt's window only from its journaled slot.
        assert_eq!(
            entry.signatures[0].blockhash_slot,
            Some(blockhash_slot as u64),
            "a restart must keep the journaled blockhash slot"
        );

        // Deadline must be the stored one, not a fresh window.
        // Allows up to 1s of clock skew between DB write and assertion.
        assert!(
            (entry.deadline - deadline).num_milliseconds().abs() < 1_000,
            "deadline should be restored from DB, got {:?}",
            entry.deadline
        );

        // The counter must survive the round-trip. A reset would re-arm the
        // attempt cap on every restart.
        assert_eq!(entry.finality_check_attempts, 2);

        // Standard recovery marker so combined error messages are meaningful.
        assert_eq!(entry.original_error, "recovered from persistent storage");

        // No status update sent — valid rows are silently re-queued.
        assert!(
            storage_rx.try_recv().is_err(),
            "no channel message expected for a valid recovery row"
        );
    }

    /// A release the program refused is direct proof no payout happened, and it
    /// is the only such proof that outlives a rotation. Losing it to a restart
    /// turns a refund the operator could have made on its own into a manual one,
    /// so it has to come back off the row exactly as it was written.
    #[tokio::test]
    async fn recover_pending_remints_restores_the_on_chain_refusal() {
        let mock = MockStorage::new();
        let mint = Pubkey::new_unique();
        let initiator = Pubkey::new_unique();
        let recipient = Pubkey::new_unique();
        let deadline = Utc::now() + chrono::Duration::seconds(20);

        let mut refused = make_pending_remint_row(
            1,
            &mint,
            &initiator,
            &recipient,
            &Signature::new_unique(),
            deadline,
        );
        refused.release_refused_on_chain = true;
        let ordinary = make_pending_remint_row(
            2,
            &mint,
            &initiator,
            &recipient,
            &Signature::new_unique(),
            deadline,
        );
        journal_mirrored_releases(&mock, &refused, None);
        journal_mirrored_releases(&mock, &ordinary, None);
        mock.pending_remint_transactions
            .lock()
            .unwrap()
            .extend([refused, ordinary]);

        let mut state = make_sender_state(mock);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        state.recover_pending_remints(&storage_tx).await.unwrap();

        assert_eq!(state.pending_remints.len(), 2);
        assert!(
            state.pending_remints[0].release_refused_on_chain,
            "a refused release must come back refused"
        );
        assert!(
            !state.pending_remints[1].release_refused_on_chain,
            "an ordinary failure must stay held to the bitmap gate"
        );
        assert!(storage_rx.try_recv().is_err(), "both rows are valid");
    }

    /// A negative `finality_check_attempts` should never appear (the column is
    /// `INTEGER NOT NULL DEFAULT 0`, only ever written to non-negative values),
    /// but a corrupt row must escalate rather than wrap silently into a huge
    /// `u32` that bypasses the attempt cap.
    #[tokio::test]
    async fn recover_pending_remints_escalates_negative_attempt_counter() {
        let mock = MockStorage::new();
        let mint = Pubkey::new_unique();
        let recipient = Pubkey::new_unique();
        let sig = Signature::new_unique();
        let deadline = Utc::now() + chrono::Duration::seconds(20);

        let mut row =
            make_pending_remint_row(7, &mint, &Pubkey::new_unique(), &recipient, &sig, deadline);
        row.finality_check_attempts = -1;
        journal_mirrored_releases(&mock, &row, None);
        mock.pending_remint_transactions.lock().unwrap().push(row);

        let mut state = make_sender_state(mock);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        state.recover_pending_remints(&storage_tx).await.unwrap();

        assert!(state.pending_remints.is_empty());
        let update = storage_rx
            .try_recv()
            .expect("corrupt row must produce a ManualReview update");
        assert_eq!(update.transaction_id, 7);
        assert_eq!(update.status, TransactionStatus::ManualReview);
        let err = update.error_message.as_deref().unwrap_or("");
        assert!(
            err.contains("negative finality_check_attempts"),
            "must escalate on the counter, not on an earlier check: {err}"
        );
    }

    // ── recover_pending_remints: parse error escalations ─────────────

    /// A corrupted mint pubkey in a PendingRemint row cannot be parsed back
    /// into a `Pubkey`, so the remint cannot be safely executed.
    ///
    /// The operator must escalate to ManualReview immediately rather than
    /// silently skipping — skipping would leave the row stuck in PendingRemint
    /// and re-surface the same corrupt row on every subsequent restart.
    ///
    /// Critically, the bad row must not block recovery of other valid rows:
    /// if there are two rows and one is corrupt, the valid one must still
    /// be queued.
    #[tokio::test]
    async fn recover_pending_remints_escalates_invalid_mint_to_manual_review() {
        let mock = MockStorage::new();
        let recipient = Pubkey::new_unique();
        let sig = Signature::new_unique();
        let deadline = Utc::now() + chrono::Duration::seconds(20);

        // Row 1: invalid mint — should escalate to ManualReview and be skipped.
        let mut bad_row = make_pending_remint_row(
            10,
            &Pubkey::new_unique(),
            &Pubkey::new_unique(),
            &recipient,
            &sig,
            deadline,
        );
        bad_row.mint = "not-a-valid-pubkey".to_string();

        // Row 2: valid — must still be recovered despite the bad row above.
        let good_mint = Pubkey::new_unique();
        let good_row = make_pending_remint_row(
            11,
            &good_mint,
            &Pubkey::new_unique(),
            &recipient,
            &sig,
            deadline,
        );
        journal_mirrored_releases(&mock, &good_row, None);

        mock.pending_remint_transactions
            .lock()
            .unwrap()
            .extend([bad_row, good_row]);

        let mut state = make_sender_state(mock);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        state.recover_pending_remints(&storage_tx).await.unwrap();

        // The bad row must produce exactly one ManualReview update.
        let update = storage_rx
            .try_recv()
            .expect("should receive ManualReview for bad row");
        assert_eq!(update.transaction_id, 10);
        assert_eq!(update.status, TransactionStatus::ManualReview);
        let err = update.error_message.as_deref().unwrap_or("");
        assert!(
            err.contains("invalid mint pubkey"),
            "error message should describe the parse failure: {err}"
        );

        // The valid row must still be queued — bad rows don't abort recovery.
        assert_eq!(state.pending_remints.len(), 1);
        assert_eq!(state.pending_remints[0].ctx.transaction_id, Some(11));

        // No further channel messages.
        assert!(storage_rx.try_recv().is_err());
    }

    /// A corrupted initiator pubkey cannot be parsed into a `Pubkey`, so the
    /// operator cannot compute the burner's ATA and has no valid destination
    /// for the remint.
    ///
    /// Same escalation rule as invalid mint: ManualReview immediately, do not
    /// skip silently, do not block other rows.
    #[tokio::test]
    async fn recover_pending_remints_escalates_invalid_initiator_to_manual_review() {
        let mock = MockStorage::new();
        let mint = Pubkey::new_unique();
        let sig = Signature::new_unique();
        let deadline = Utc::now() + chrono::Duration::seconds(20);

        let mut bad_row = make_pending_remint_row(
            20,
            &mint,
            &Pubkey::new_unique(),
            &Pubkey::new_unique(),
            &sig,
            deadline,
        );
        bad_row.initiator = "not-a-valid-pubkey".to_string();

        mock.pending_remint_transactions
            .lock()
            .unwrap()
            .push(bad_row);

        let mut state = make_sender_state(mock);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        state.recover_pending_remints(&storage_tx).await.unwrap();

        let update = storage_rx
            .try_recv()
            .expect("should receive ManualReview for bad initiator");
        assert_eq!(update.transaction_id, 20);
        assert_eq!(update.status, TransactionStatus::ManualReview);
        let err = update.error_message.as_deref().unwrap_or("");
        assert!(
            err.contains("invalid initiator pubkey"),
            "error message should describe the parse failure: {err}"
        );

        assert!(
            state.pending_remints.is_empty(),
            "bad row must not be queued"
        );
        assert!(storage_rx.try_recv().is_err());
    }

    // No negative-amount test here anymore: `TokenAmount(u64)` makes a negative
    // amount unconstructable; the rejection now lives in TokenAmount's decode tests.

    /// An unparseable signature in a PendingRemint row's release journal breaks
    /// the finality check: the operator cannot call `get_signature_statuses` with
    /// an invalid signature, so it cannot determine whether the original
    /// withdrawal landed on-chain.
    ///
    /// Reminting without that check risks a double-credit — the operator must
    /// escalate to ManualReview instead of queuing the entry.
    #[tokio::test]
    async fn recover_pending_remints_escalates_invalid_signature_to_manual_review() {
        let mock = MockStorage::new();
        let mint = Pubkey::new_unique();
        let recipient = Pubkey::new_unique();
        let deadline = Utc::now() + chrono::Duration::seconds(20);

        let bad_row = make_pending_remint_row(
            40,
            &mint,
            &Pubkey::new_unique(),
            &recipient,
            &Signature::new_unique(),
            deadline,
        );
        // Only the journal is corrupt: boot must read it, not the row's mirror.
        mock.release_signatures.lock().unwrap().insert(
            bad_row.id,
            vec![StoredSig {
                signature: "not-a-valid-signature".to_string(),
                last_valid_block_height: 12_345,
                blockhash_slot: None,
            }],
        );

        mock.pending_remint_transactions
            .lock()
            .unwrap()
            .push(bad_row);

        let mut state = make_sender_state(mock);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        state.recover_pending_remints(&storage_tx).await.unwrap();

        let update = storage_rx
            .try_recv()
            .expect("should receive ManualReview for invalid signature");
        assert_eq!(update.transaction_id, 40);
        assert_eq!(update.status, TransactionStatus::ManualReview);
        let err = update.error_message.as_deref().unwrap_or("");
        assert!(
            err.contains("malformed stored release signature"),
            "error message should describe the signature parse failure: {err}"
        );

        assert!(
            state.pending_remints.is_empty(),
            "row with invalid signature must not be queued"
        );
        assert!(storage_rx.try_recv().is_err());
    }

    /// Boot rebuilds from the journal, so a row with nothing journaled has no
    /// signature to prove the release dead against, whatever its mirror holds.
    #[tokio::test]
    async fn recover_pending_remints_escalates_an_empty_journal_to_manual_review() {
        let mock = MockStorage::new();
        let deadline = Utc::now() + chrono::Duration::seconds(20);
        let transaction_id = 45;

        // The mirror carries a signature, but nothing is journaled.
        let row = make_pending_remint_row(
            transaction_id,
            &Pubkey::new_unique(),
            &Pubkey::new_unique(),
            &Pubkey::new_unique(),
            &Signature::new_unique(),
            deadline,
        );
        mock.pending_remint_transactions.lock().unwrap().push(row);

        let mut state = make_sender_state(mock);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        state.recover_pending_remints(&storage_tx).await.unwrap();

        let update = storage_rx
            .try_recv()
            .expect("an empty journal must escalate");
        assert_eq!(update.transaction_id, transaction_id);
        assert_eq!(update.status, TransactionStatus::ManualReview);
        let err = update.error_message.as_deref().unwrap_or("");
        assert!(
            err.contains("no withdrawal signatures stored"),
            "error message should say nothing is journaled: {err}"
        );
        assert!(
            state.pending_remints.is_empty(),
            "a row with an empty journal must not be queued"
        );
        assert!(storage_rx.try_recv().is_err());
    }

    /// No writer stores a negative height, so one is corruption. Wrapped to a huge
    /// u64 it would read as Live on every tick and reach ManualReview only after
    /// the waits run out, under a reason that points the on-call the wrong way.
    #[tokio::test]
    async fn recover_pending_remints_escalates_a_negative_journaled_height_to_manual_review() {
        let mock = MockStorage::new();
        let deadline = Utc::now() + chrono::Duration::seconds(20);
        let transaction_id = 47;
        let sig = Signature::new_unique();

        let row = make_pending_remint_row(
            transaction_id,
            &Pubkey::new_unique(),
            &Pubkey::new_unique(),
            &Pubkey::new_unique(),
            &sig,
            deadline,
        );
        mock.release_signatures.lock().unwrap().insert(
            transaction_id,
            vec![StoredSig {
                signature: sig.to_string(),
                last_valid_block_height: -1,
                blockhash_slot: None,
            }],
        );
        mock.pending_remint_transactions.lock().unwrap().push(row);

        let mut state = make_sender_state(mock);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        state.recover_pending_remints(&storage_tx).await.unwrap();

        let update = storage_rx
            .try_recv()
            .expect("a negative journaled height must escalate at once");
        assert_eq!(update.transaction_id, transaction_id);
        assert_eq!(update.status, TransactionStatus::ManualReview);
        let err = update.error_message.as_deref().unwrap_or("");
        // The prefix is what the runbook routes on.
        assert!(
            err.contains("malformed stored release signature")
                && err.contains("negative last_valid_block_height"),
            "error message should name the corrupt height: {err}"
        );
        assert!(
            state.pending_remints.is_empty(),
            "a row with a corrupt journal must not be queued"
        );
        assert!(storage_rx.try_recv().is_err());
    }

    /// A failed journal read says nothing about the row, so boot fails as it does
    /// when the row query fails, rather than sending the row to ManualReview.
    #[tokio::test]
    async fn recover_pending_remints_fails_boot_when_the_journal_is_unreadable() {
        let mock = MockStorage::new();
        let deadline = Utc::now() + chrono::Duration::seconds(20);
        let transaction_id = 46;

        let row = make_pending_remint_row(
            transaction_id,
            &Pubkey::new_unique(),
            &Pubkey::new_unique(),
            &Pubkey::new_unique(),
            &Signature::new_unique(),
            deadline,
        );
        journal_mirrored_releases(&mock, &row, None);
        mock.pending_remint_transactions.lock().unwrap().push(row);
        mock.set_should_fail("get_release_signatures", true);

        let mut state = make_sender_state(mock);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        let err = state
            .recover_pending_remints(&storage_tx)
            .await
            .expect_err("an unreadable journal must fail the boot");
        assert!(
            err.to_string()
                .contains(&format!("transaction {transaction_id}")),
            "the boot error must name the row: {err}"
        );
        assert!(
            state.pending_remints.is_empty(),
            "a row whose journal could not be read must not be queued"
        );
        assert!(
            storage_rx.try_recv().is_err(),
            "a read failure must not send the row to ManualReview"
        );
    }

    /// The deferred remint queue belongs to the Withdraw role. An Escrow sender
    /// sharing the transactions database must not hydrate a PendingRemint row:
    /// it would later classify the release signature against the wrong chain.
    #[tokio::test]
    async fn recover_pending_remints_noop_for_escrow_role() {
        let mock = MockStorage::new();
        let sig = Signature::new_unique();
        let deadline = Utc::now() - chrono::Duration::seconds(10);

        mock.pending_remint_transactions
            .lock()
            .unwrap()
            .push(make_pending_remint_row(
                70,
                &Pubkey::new_unique(),
                &Pubkey::new_unique(),
                &Pubkey::new_unique(),
                &sig,
                deadline,
            ));

        let mut state = make_sender_state_with_role(mock, ProgramType::Escrow);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        state.recover_pending_remints(&storage_tx).await.unwrap();

        assert!(
            state.pending_remints.is_empty(),
            "Escrow must not claim a Withdraw remint row"
        );
        assert!(
            storage_rx.try_recv().is_err(),
            "the row must be left untouched for the Withdraw sender"
        );
    }

    /// On a clean startup with no PendingRemint rows in the database,
    /// `recover_pending_remints` must be a complete no-op: no entries queued,
    /// no channel messages sent, no errors returned.
    #[tokio::test]
    async fn recover_pending_remints_empty_db_is_noop() {
        let mock = MockStorage::new();
        // pending_remint_transactions is empty by default.
        let mut state = make_sender_state(mock);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        let result = state.recover_pending_remints(&storage_tx).await;

        assert!(result.is_ok(), "should not error on empty DB");
        assert!(
            state.pending_remints.is_empty(),
            "queue should remain empty"
        );
        assert!(
            storage_rx.try_recv().is_err(),
            "no channel messages expected"
        );
    }

    /// A PendingRemint row whose deadline has already passed (e.g. the operator
    /// was down for longer than the finality window) must still be queued on
    /// recovery. The deadline is preserved as-is so that `process_pending_remints`
    /// sees it as already matured and processes it on the very next tick.
    #[tokio::test]
    async fn recover_pending_remints_past_deadline_queued_with_past_deadline() {
        let mock = MockStorage::new();
        let mint = Pubkey::new_unique();
        let recipient = Pubkey::new_unique();
        let sig = Signature::new_unique();
        // Deadline already in the past — crash happened mid-finality window.
        let past_deadline = Utc::now() - chrono::Duration::seconds(10);

        let row = make_pending_remint_row(
            50,
            &mint,
            &Pubkey::new_unique(),
            &recipient,
            &sig,
            past_deadline,
        );
        journal_mirrored_releases(&mock, &row, None);
        mock.pending_remint_transactions.lock().unwrap().push(row);

        let mut state = make_sender_state(mock);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        state.recover_pending_remints(&storage_tx).await.unwrap();

        // Entry must be queued — recovery re-queues, does not process.
        assert_eq!(state.pending_remints.len(), 1);
        let entry = &state.pending_remints[0];
        assert_eq!(entry.ctx.transaction_id, Some(50));

        // Past deadline preserved — process_pending_remints will fire it immediately.
        assert!(
            entry.deadline <= Utc::now(),
            "past deadline should be restored so entry matures on next tick: {:?}",
            entry.deadline
        );

        // No ManualReview
        assert!(storage_rx.try_recv().is_err());
    }

    /// When `pending_remint_deadline_at` is NULL in the database (corrupt row or
    /// schema inconsistency), recovery falls back to `Utc::now()`. This means the
    /// entry is treated as immediately matured — `process_pending_remints` will
    /// pick it up on the next tick instead of waiting a full 32s window.
    #[tokio::test]
    async fn recover_pending_remints_missing_deadline_defaults_to_now() {
        let mock = MockStorage::new();
        let mint = Pubkey::new_unique();
        let recipient = Pubkey::new_unique();
        let sig = Signature::new_unique();

        let mut row = make_pending_remint_row(
            60,
            &mint,
            &Pubkey::new_unique(),
            &recipient,
            &sig,
            Utc::now() + chrono::Duration::seconds(30),
        );
        row.pending_remint_deadline_at = None; // simulate missing deadline
        journal_mirrored_releases(&mock, &row, None);

        mock.pending_remint_transactions.lock().unwrap().push(row);

        let mut state = make_sender_state(mock);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        let before = Utc::now();
        state.recover_pending_remints(&storage_tx).await.unwrap();
        let after = Utc::now();

        // Entry must still be queued (not skipped).
        assert_eq!(state.pending_remints.len(), 1);
        let entry = &state.pending_remints[0];
        assert_eq!(entry.ctx.transaction_id, Some(60));

        // Deadline must be ~Utc::now() at the time of recovery — entry fires on next tick.
        assert!(
            entry.deadline >= before - chrono::Duration::milliseconds(100)
                && entry.deadline <= after + chrono::Duration::milliseconds(100),
            "missing deadline should default to ~now, got {:?}",
            entry.deadline
        );

        // No ManualReview sent — missing deadline is handled gracefully.
        assert!(storage_rx.try_recv().is_err());
    }

    // ── pending remint backlog paging ────────────────────────────────

    use crate::operator::sender::remint::{process_pending_remints, REMINT_HYDRATE_PAGE as PAGE};

    const PAGE_READ: &str = "get_pending_remint_transactions";

    /// Seed parseable, journaled PendingRemint rows with the given ids, due an hour from now.
    fn seed_backlog(mock: &MockStorage, ids: impl IntoIterator<Item = i64>) {
        let deadline = Utc::now() + chrono::Duration::hours(1);
        for id in ids {
            let row = make_pending_remint_row(
                id,
                &Pubkey::new_unique(),
                &Pubkey::new_unique(),
                &Pubkey::new_unique(),
                &Signature::new_unique(),
                deadline,
            );
            journal_mirrored_releases(mock, &row, None);
            mock.pending_remint_transactions.lock().unwrap().push(row);
        }
    }

    /// A remint handed off at runtime, due an hour from now so no tick dials RPC.
    fn runtime_entry(id: i64) -> PendingRemint {
        PendingRemint {
            ctx: TransactionContext {
                kind: TransactionKind::ReleaseFunds,
                transaction_id: Some(id),
                withdrawal_nonce: Some(id as u64),
                trace_id: Some(format!("trace-{id}")),
                deposit_claim_lease: None,
            },
            remint_info: WithdrawalRemintInfo {
                transaction_id: id,
                source_event_id: SourceEventId::new(&format!("sig-{id}"), 0, None),
                trace_id: format!("trace-{id}"),
                mint: Pubkey::new_unique(),
                user: Pubkey::new_unique(),
                user_ata: Pubkey::new_unique(),
                token_program: spl_token::id(),
                amount: 1000,
            },
            signatures: Vec::new(),
            original_error: "release_funds failed".to_string(),
            deadline: Utc::now() + chrono::Duration::hours(1),
            finality_check_attempts: 0,
            release_refused_on_chain: false,
            coverage_slot: None,
            coverage_checkpoint: None,
            free_waits: 0,
        }
    }

    fn queued_ids(state: &SenderState) -> Vec<i64> {
        state
            .pending_remints
            .iter()
            .map(|e| e.ctx.transaction_id.unwrap())
            .collect()
    }

    fn page_reads(mock: &MockStorage) -> usize {
        mock.call_counts
            .lock()
            .unwrap()
            .get(PAGE_READ)
            .copied()
            .unwrap_or(0)
    }

    /// A journal read that fails while a page loads must not skip the rows after it: the
    /// cursor rewinds to the last row handled, so the next read loads them.
    #[tokio::test]
    async fn a_failed_journal_read_rewinds_the_backlog_cursor() {
        let mock = MockStorage::new();
        seed_backlog(&mock, 1..=3);
        let mut state = make_sender_state(mock.clone());
        let (storage_tx, _rx) = mpsc::channel(10);
        state.remint_backlog_cursor = Some(0);

        mock.set_should_fail("get_release_signatures", true);
        assert!(state.hydrate_pending_remints(&storage_tx).await.is_err());
        assert!(queued_ids(&state).is_empty());
        assert_eq!(
            state.remint_backlog_cursor,
            Some(0),
            "a short page that failed must be read again"
        );

        mock.set_should_fail("get_release_signatures", false);
        state.hydrate_pending_remints(&storage_tx).await.unwrap();
        assert_eq!(queued_ids(&state), vec![1, 2, 3]);
        assert_eq!(state.remint_backlog_cursor, None);
    }

    /// Boot must load at most one page, however large the durable backlog, and
    /// arm the cursor only when a full page says more rows may follow.
    #[tokio::test]
    async fn recover_loads_one_page_and_arms_the_cursor() {
        for n in [0, 3, PAGE, PAGE + 1] {
            let mock = MockStorage::new();
            seed_backlog(&mock, 1..=n as i64);
            let mut state = make_sender_state(mock.clone());
            let (storage_tx, mut storage_rx) = mpsc::channel(10);

            state.recover_pending_remints(&storage_tx).await.unwrap();

            let expected: Vec<i64> = (1..=n.min(PAGE) as i64).collect();
            assert_eq!(queued_ids(&state), expected, "n = {n}");
            let cursor = (n >= PAGE).then_some(PAGE as i64);
            assert_eq!(state.remint_backlog_cursor, cursor, "n = {n}");
            assert!(storage_rx.try_recv().is_err(), "n = {n}");
            assert_eq!(page_reads(&mock), 1, "n = {n}");
        }

        let mock = MockStorage::new();
        seed_backlog(&mock, 1..=PAGE as i64 + 1);
        let mut state = make_sender_state_with_role(mock.clone(), ProgramType::Escrow);
        let (storage_tx, _rx) = mpsc::channel(10);
        state.recover_pending_remints(&storage_tx).await.unwrap();
        assert_eq!(page_reads(&mock), 0, "Escrow must never read the backlog");
        assert_eq!(state.remint_backlog_cursor, None);
    }

    /// A runtime handoff whose terminal write may still be queued must never be
    /// reloaded by a page, and runtime entries must not hold back the refill.
    #[tokio::test]
    async fn runtime_handoffs_are_never_paged_and_do_not_hold_the_refill() {
        let page = PAGE as i64;
        let mock = MockStorage::new();
        seed_backlog(&mock, 1..=page + 5);
        let mut state = make_sender_state(mock.clone());
        let (storage_tx, _rx) = mpsc::channel(10);
        state.recover_pending_remints(&storage_tx).await.unwrap();

        // Two ids a page would see, plus memory-only fillers so the next page stays short.
        state.queue_runtime_remint(runtime_entry(page + 2));
        state.queue_runtime_remint(runtime_entry(page + 4));
        for id in 10_000..10_000 + page - 2 {
            state.queue_runtime_remint(runtime_entry(id));
        }
        // Resolved with its ManualReview still queued: gone from memory,
        // still pending_remint in the DB.
        state
            .pending_remints
            .retain(|e| e.ctx.transaction_id != Some(page + 2));
        state
            .pending_remints
            .retain(|e| e.ctx.transaction_id != Some(1));
        assert!(state.pending_remints.len() > PAGE);

        let before = queued_ids(&state);
        process_pending_remints(&mut state, &storage_tx).await;

        let added: Vec<i64> = queued_ids(&state)
            .into_iter()
            .filter(|id| !before.contains(id))
            .collect();
        assert_eq!(added, vec![page + 1, page + 3, page + 5]);
        let copies = |id| queued_ids(&state).iter().filter(|&&q| q == id).count();
        assert_eq!(
            copies(page + 2),
            0,
            "a resolved runtime row must not come back"
        );
        assert_eq!(
            copies(page + 4),
            1,
            "a queued runtime row must not be loaded twice"
        );
        assert_eq!(state.remint_backlog_cursor, None);
        assert!(state.remint_runtime_ids.is_empty());
    }

    /// Ticks load the rest of the backlog one full page at a time, only once
    /// fewer than a page is queued, each row once, and stop reading when drained.
    #[tokio::test]
    async fn ticks_drain_the_backlog_page_by_page() {
        let page = PAGE as i64;
        let mock = MockStorage::new();
        seed_backlog(&mock, 1..=2 * page + 10);
        mock.pending_remint_transactions.lock().unwrap()[PAGE].mint = "bad-mint".to_string();
        let mut state = make_sender_state(mock.clone());
        let (storage_tx, mut storage_rx) = mpsc::channel(1024);

        state.recover_pending_remints(&storage_tx).await.unwrap();
        assert_eq!(state.remint_backlog_unloaded, page as u64 + 10);
        assert_eq!(
            state.remint_backlog_size(),
            2 * PAGE + 10,
            "the gauge sees the whole backlog"
        );
        process_pending_remints(&mut state, &storage_tx).await;
        assert_eq!(page_reads(&mock), 1, "a full queue must not read");

        state.pending_remints.pop();
        let before = queued_ids(&state);
        process_pending_remints(&mut state, &storage_tx).await;
        let added: Vec<i64> = queued_ids(&state)
            .into_iter()
            .filter(|id| !before.contains(id))
            .collect();
        assert_eq!(added, (page + 2..=2 * page).collect::<Vec<_>>());
        assert_eq!(state.remint_backlog_unloaded, 10);

        state.pending_remints.truncate(PAGE - 1);
        let before = queued_ids(&state);
        process_pending_remints(&mut state, &storage_tx).await;
        let added: Vec<i64> = queued_ids(&state)
            .into_iter()
            .filter(|id| !before.contains(id))
            .collect();
        assert_eq!(added, (2 * page + 1..=2 * page + 10).collect::<Vec<_>>());
        assert_eq!(state.remint_backlog_cursor, None);
        assert_eq!(state.remint_backlog_unloaded, 0);
        assert_eq!(page_reads(&mock), 3);
        let mut ids = queued_ids(&state);
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(
            ids.len(),
            state.pending_remints.len(),
            "no row is queued twice"
        );

        state.pending_remints.clear();
        process_pending_remints(&mut state, &storage_tx).await;
        assert_eq!(
            page_reads(&mock),
            3,
            "a drained backlog must not be read again"
        );

        let update = storage_rx.try_recv().expect("the bad row escalates");
        assert_eq!(update.transaction_id, page + 1);
        assert_eq!(update.status, TransactionStatus::ManualReview);
        assert!(storage_rx.try_recv().is_err(), "it escalates exactly once");
    }

    /// A page read error at boot fails boot; on a tick it only delays the page.
    #[tokio::test]
    async fn page_read_failure_fails_boot_but_only_delays_a_tick() {
        let mock = MockStorage::new();
        seed_backlog(&mock, 1..=3);
        mock.set_fail_times(PAGE_READ, 1);
        let mut state = make_sender_state(mock.clone());
        let (storage_tx, _rx) = mpsc::channel(10);
        let err = state
            .recover_pending_remints(&storage_tx)
            .await
            .unwrap_err();
        assert!(matches!(err, OperatorError::Storage(_)), "{err:?}");

        let page = PAGE as i64;
        let mock = MockStorage::new();
        seed_backlog(&mock, 1..=page + 5);
        let mut state = make_sender_state(mock.clone());
        state.recover_pending_remints(&storage_tx).await.unwrap();
        state.pending_remints.pop();

        mock.set_fail_times(PAGE_READ, 1);
        let failures = || {
            crate::metrics::OPERATOR_TRANSACTION_ERRORS
                .with_label_values(&["withdraw", "pending_remint_page_read_failed"])
                .get()
        };
        let failed_before = failures();
        let before = queued_ids(&state);
        process_pending_remints(&mut state, &storage_tx).await;
        assert_eq!(state.remint_backlog_cursor, Some(page));
        assert_eq!(queued_ids(&state), before);
        assert_eq!(
            failures(),
            failed_before + 1.0,
            "a failed page read must be counted"
        );

        process_pending_remints(&mut state, &storage_tx).await;
        let added: Vec<i64> = queued_ids(&state)
            .into_iter()
            .filter(|id| !before.contains(id))
            .collect();
        assert_eq!(added, (page + 1..=page + 5).collect::<Vec<_>>());
    }

    // ── SenderState construction tests ───────────────────────────────

    use crate::config::{PostgresConfig, ProgramType, StorageType};

    fn make_config() -> PrivateChannelIndexerConfig {
        PrivateChannelIndexerConfig {
            program_type: ProgramType::Escrow,
            storage_type: StorageType::Postgres,
            rpc_url: "http://localhost:8899".to_string(),
            fallback_rpc_url: None,
            source_rpc_url: None,
            postgres: PostgresConfig {
                database_url: "postgresql://localhost/test".to_string(),
                max_connections: 5,
            },
            escrow_instance_id: None,
        }
    }

    /// `SenderState::new` with no instance PDA and Escrow program type must succeed and
    /// hold no withdrawal replay state of its own.
    #[test]
    fn sender_state_new_constructs_successfully() {
        let mock = MockStorage::new();
        let storage = Arc::new(Storage::Mock(mock));
        let config = make_config();

        let result = SenderState::new(
            &config,
            CommitmentLevel::Confirmed,
            None,
            storage,
            3,
            400,
            None,
            SolvencyFence::open_from_start(),
        );

        assert!(result.is_ok());
        let state = result.unwrap();
        assert!(state.instance_pda.is_none());
        assert!(state.in_flight_withdrawals.is_empty());
        assert_eq!(state.retry_max_attempts, 3);
        assert_eq!(state.program_type, ProgramType::Escrow);
    }

    /// Providing an instance PDA and a higher retry limit must be reflected in the
    /// constructed state; the PDA is stored as-is and later derives the bitmap.
    #[test]
    fn sender_state_new_with_instance_pda() {
        let mock = MockStorage::new();
        let storage = Arc::new(Storage::Mock(mock));
        let instance_pda = Pubkey::new_unique();
        let config = make_config();

        let result = SenderState::new(
            &config,
            CommitmentLevel::Finalized,
            Some(instance_pda),
            storage,
            5,
            400,
            None,
            SolvencyFence::open_from_start(),
        );

        assert!(result.is_ok());
        let state = result.unwrap();
        assert_eq!(state.instance_pda, Some(instance_pda));
        assert_eq!(state.retry_max_attempts, 5);
    }

    // ── validate_bitmap_consistency ──────────────────────────────────

    /// Seed a Completed withdrawal row carrying `nonce`, with one broadcast
    /// signature already persisted so the chain-ahead path has something to
    /// classify.
    fn seed_withdrawal(
        mock: &MockStorage,
        id: i64,
        nonce: u64,
        status: TransactionStatus,
        signature: Option<Signature>,
    ) {
        let now = Utc::now();
        mock.pending_transactions
            .lock()
            .unwrap()
            .push(DbTransaction {
                id,
                signature: Signature::new_unique().to_string(),
                trace_id: format!("trace-{id}"),
                slot: 1,
                initiator: Pubkey::new_unique().to_string(),
                recipient: Pubkey::new_unique().to_string(),
                mint: Pubkey::new_unique().to_string(),
                amount: TokenAmount(1_000),
                memo: None,
                transaction_type: TransactionType::Withdrawal,
                withdrawal_nonce: Some(nonce as i64),
                status,
                created_at: now,
                updated_at: now,
                processed_at: None,
                counterpart_signature: None,
                remint_signatures: None,
                remint_last_valid_block_heights: None,
                pending_remint_deadline_at: None,
                finality_check_attempts: 0,
                recovery_requeue_attempts: 0,
                instruction_index: 0,
                inner_index: None,
                landed_remint_signature: None,
                release_refused_on_chain: false,
            });

        if let Some(sig) = signature {
            mock.release_signatures.lock().unwrap().insert(
                id,
                vec![crate::storage::common::models::StoredSig {
                    signature: sig.to_string(),
                    last_valid_block_height: 1,
                    blockhash_slot: None,
                }],
            );
        }
    }

    /// The pre-flight cannot diff anything without an instance to derive the
    /// bitmap from, and must say so rather than silently pass.
    #[tokio::test]
    async fn validate_bitmap_consistency_fails_without_instance_pda() {
        let state = make_sender_state(MockStorage::new());

        let err = super::validate_bitmap_consistency(&state.storage, &state.rpc_client, None, None)
            .await
            .unwrap_err();

        assert!(
            matches!(
                err,
                OperatorError::Account(crate::error::AccountError::InstanceNotFound { .. })
            ),
            "expected InstanceNotFound, got: {err}"
        );
    }

    /// Chain and database agree: no halt, no repair, no status writes.
    #[tokio::test]
    async fn validate_bitmap_consistency_identical_sets_pass() {
        let mut server = mockito::Server::new_async().await;
        let _anchor = mock_finalized_anchor(&mut server, vec![1]);
        let _bitmap = mock_bitmap_account(&mut server, 0, &[1, 3]);

        let mock = MockStorage::new();
        seed_withdrawal(&mock, 1, 1, TransactionStatus::Completed, None);
        seed_withdrawal(&mock, 3, 3, TransactionStatus::Completed, None);

        let state = sender_state_with_storage(&server.url(), mock);

        let result = super::validate_bitmap_consistency(
            &state.storage,
            &state.rpc_client,
            None,
            Some(Pubkey::new_unique()),
        )
        .await;

        assert!(result.is_ok(), "agreeing sets must pass: {result:?}");
    }

    /// Chain ahead: the release landed and a stored signature proves which one,
    /// so the row is completed and startup continues. Halting here would stop
    /// the pipeline over a bookkeeping gap where the money already moved.
    #[tokio::test]
    async fn validate_bitmap_consistency_chain_ahead_completes_and_starts() {
        let mut server = mockito::Server::new_async().await;
        let _anchor = mock_finalized_anchor(&mut server, vec![1]);
        let landed = Signature::new_unique();
        let _bitmap = mock_bitmap_account(&mut server, 0, &[2]);
        let _statuses = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "method": "getSignatureStatuses"
            })))
            .with_status(200)
            .with_body(
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {
                        "context": {"slot": 100},
                        "value": [{
                            "slot": 10,
                            "confirmations": null,
                            "err": null,
                            "status": {"Ok": null},
                            "confirmationStatus": "finalized"
                        }]
                    }
                })
                .to_string(),
            )
            .create();

        let transaction_id = 7;
        let mock = MockStorage::new();
        seed_withdrawal(
            &mock,
            transaction_id,
            2,
            TransactionStatus::Processing,
            Some(landed),
        );

        let state = sender_state_with_storage(&server.url(), mock.clone());

        let result = super::validate_bitmap_consistency(
            &state.storage,
            &state.rpc_client,
            None,
            Some(Pubkey::new_unique()),
        )
        .await;

        assert!(result.is_ok(), "chain-ahead must not halt: {result:?}");
        let row = mock
            .pending_transactions
            .lock()
            .unwrap()
            .iter()
            .find(|txn| txn.id == transaction_id)
            .cloned()
            .expect("seeded row");
        assert_eq!(row.status, TransactionStatus::Completed);
        assert_eq!(row.counterpart_signature, Some(landed.to_string()));
    }

    /// Chain ahead with nothing to attribute the payout to: boot refuses, and the row is
    /// left as it was so a later boot can still explain it.
    #[tokio::test]
    async fn validate_bitmap_consistency_chain_ahead_without_signatures_refuses() {
        let mut server = mockito::Server::new_async().await;
        let _anchor = mock_finalized_anchor(&mut server, vec![1]);
        let _bitmap = mock_bitmap_account(&mut server, 0, &[2]);

        let mock = MockStorage::new();
        seed_withdrawal(&mock, 7, 2, TransactionStatus::Processing, None);

        let state = sender_state_with_storage(&server.url(), mock);

        let result = super::validate_bitmap_consistency(
            &state.storage,
            &state.rpc_client,
            None,
            Some(Pubkey::new_unique()),
        )
        .await;

        assert!(
            matches!(
                result,
                Err(OperatorError::Program(
                    crate::error::ProgramError::UnexplainedConsumedNonces { .. }
                ))
            ),
            "an unexplained consumed nonce must refuse boot: {result:?}"
        );
        assert_eq!(
            state
                .storage
                .get_withdrawal_by_nonce(2)
                .await
                .unwrap()
                .unwrap()
                .status,
            TransactionStatus::Processing
        );
    }

    /// Serve one finalized-success status for every `getSignatureStatuses` call.
    fn mock_landed_status(server: &mut mockito::ServerGuard) -> mockito::Mock {
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "method": "getSignatureStatuses"
            })))
            .with_body(
                serde_json::json!({ "jsonrpc": "2.0", "id": 1, "result": {
                    "context": {"slot": 100},
                    "value": [{ "slot": 10, "confirmations": null, "err": null,
                        "status": {"Ok": null}, "confirmationStatus": "finalized" }]
                }})
                .to_string(),
            )
            .create()
    }

    /// Run the boot gate against a chain whose generation-`generation` bitmap has
    /// `consumed` set, over the rows `seed` puts in the DB.
    async fn nonce_gate(
        generation: u64,
        consumed: &[u64],
        statuses: Option<&str>,
        seed: impl FnOnce(&MockStorage),
    ) -> (Result<(), OperatorError>, MockStorage) {
        let mut server = mockito::Server::new_async().await;
        let _anchor = mock_finalized_anchor(&mut server, vec![1]);
        let _bitmap = mock_bitmap_account(&mut server, generation, consumed);
        let _statuses = match statuses {
            Some("landed") => Some(mock_landed_status(&mut server)),
            _ => None,
        };
        let mock = MockStorage::new();
        seed(&mock);
        let state = sender_state_with_storage(&server.url(), mock.clone());
        let result = super::validate_bitmap_consistency(
            &state.storage,
            &state.rpc_client,
            None,
            Some(Pubkey::new_unique()),
        )
        .await;
        (result, mock)
    }

    fn refused_on(result: &Result<(), OperatorError>) -> Option<Vec<u64>> {
        match result {
            Err(OperatorError::Program(
                crate::error::ProgramError::UnexplainedConsumedNonces { nonces },
            )) => Some(nonces.clone()),
            _ => None,
        }
    }

    /// After an indexer restore past a release, re-indexed burns are renumbered and a
    /// set bit can no longer be tied to the burn it paid. Only a landed journaled
    /// signature on the row explains a bit; anything else refuses boot.
    #[tokio::test]
    async fn nonce_gate_matrix() {
        // (case, row status, stored signature, status answer, boots)
        type Case<'a> = (
            &'a str,
            Option<TransactionStatus>,
            Option<Signature>,
            Option<&'a str>,
            bool,
        );
        let landed = Some(Signature::new_unique());
        let cases: Vec<Case> = vec![
            ("no row", None, None, None, false),
            (
                "pending row",
                Some(TransactionStatus::Pending),
                None,
                None,
                false,
            ),
            (
                "processing, no signature",
                Some(TransactionStatus::Processing),
                None,
                None,
                false,
            ),
            (
                "processing, landed",
                Some(TransactionStatus::Processing),
                landed,
                Some("landed"),
                true,
            ),
            (
                "processing, status unreadable",
                Some(TransactionStatus::Processing),
                landed,
                None,
                false,
            ),
            (
                "failed, landed",
                Some(TransactionStatus::Failed),
                landed,
                Some("landed"),
                true,
            ),
            (
                "manual review, landed",
                Some(TransactionStatus::ManualReview),
                landed,
                Some("landed"),
                true,
            ),
        ];
        for (case, status, signature, statuses, ok) in cases {
            let (result, mock) = nonce_gate(0, &[2], statuses, |mock| {
                if let Some(status) = status {
                    seed_withdrawal(mock, 7, 2, status, signature);
                }
            })
            .await;
            if let (Some(status), false) = (status, ok) {
                let row = mock.pending_transactions.lock().unwrap()[0].clone();
                assert_eq!(
                    row.status, status,
                    "{case}: a refusal leaves the row as it was"
                );
            }
            if ok {
                assert!(result.is_ok(), "{case}: {result:?}");
            } else {
                assert_eq!(refused_on(&result), Some(vec![2]), "{case}: {result:?}");
            }
        }

        // A reminted row whose nonce was also released stays a double payout.
        let (result, _) = nonce_gate(0, &[2], None, |mock| {
            seed_withdrawal(mock, 7, 2, TransactionStatus::FailedReminted, None)
        })
        .await;
        assert!(matches!(
            result,
            Err(OperatorError::Program(
                crate::error::ProgramError::BitmapDivergence { .. }
            ))
        ));

        // The shifted-gap case: burns B3 and B4 were released as 4 and 5, the DB was
        // restored and re-indexed them as 3 and 4. Neither bit is explained.
        let (result, _) = nonce_gate(0, &[4, 5], None, |mock| {
            seed_withdrawal(mock, 3, 3, TransactionStatus::Pending, None);
            seed_withdrawal(mock, 4, 4, TransactionStatus::Pending, None);
        })
        .await;
        assert_eq!(refused_on(&result), Some(vec![4, 5]));
    }

    /// Bits of a rotated-away generation are gone, so a DB restored to before the rotation
    /// cannot be diffed against them. Its numbering still sits in the old generation,
    /// which rotation never allows in normal operation, so boot refuses.
    #[tokio::test]
    async fn nonce_gate_generation_behind_chain_refuses() {
        let per = crate::operator::bitmap_constants::NONCES_PER_GENERATION;
        let (behind, _) = nonce_gate(1, &[], None, |mock| {
            seed_withdrawal(mock, 1, per - 1, TransactionStatus::Completed, None)
        })
        .await;
        assert!(
            matches!(
                behind,
                Err(OperatorError::Program(
                    crate::error::ProgramError::NonceGenerationBehindChain { .. }
                ))
            ),
            "{behind:?}"
        );

        // An empty DB on a rotated chain is behind it too.
        let (empty, _) = nonce_gate(1, &[], None, |_| {}).await;
        assert!(empty.is_err(), "{empty:?}");

        // The DB numbering ahead of the chain is the normal wait for a rotation.
        let (ahead, _) = nonce_gate(0, &[], None, |mock| {
            seed_withdrawal(mock, 1, per + 3, TransactionStatus::Pending, None)
        })
        .await;
        assert!(ahead.is_ok(), "{ahead:?}");
    }

    /// A journal that could not be read is not an empty journal: the row may be paid, so it
    /// must stay Processing and the nonce stays unexplained.
    #[tokio::test]
    async fn chain_ahead_nonce_with_unreadable_journal_leaves_the_row_untouched() {
        let mock = MockStorage::new();
        seed_withdrawal(
            &mock,
            7,
            2,
            TransactionStatus::Processing,
            Some(Signature::new_unique()),
        );
        mock.set_should_fail("get_release_signatures", true);

        let state = make_sender_state(mock.clone());

        let outcome = super::resolve_chain_ahead_nonce(
            &state.storage,
            &FinalityRpc::solana(&state.rpc_client, None),
            2,
        )
        .await;

        assert!(matches!(outcome, super::ChainAheadOutcome::Unexplained));
        let row = mock
            .pending_transactions
            .lock()
            .unwrap()
            .iter()
            .find(|txn| txn.id == 7)
            .cloned()
            .expect("seeded row");
        assert_eq!(row.status, TransactionStatus::Processing);
    }

    /// At the boot gate the same read failure refuses to start and writes nothing, like an
    /// unread row.
    #[tokio::test]
    async fn validate_bitmap_consistency_refuses_without_a_write_when_the_journal_cannot_be_read() {
        let mut server = mockito::Server::new_async().await;
        let _anchor = mock_finalized_anchor(&mut server, vec![1]);
        let _bitmap = mock_bitmap_account(&mut server, 0, &[2]);

        let mock = MockStorage::new();
        seed_withdrawal(
            &mock,
            7,
            2,
            TransactionStatus::Processing,
            Some(Signature::new_unique()),
        );
        mock.set_should_fail("get_release_signatures", true);

        let state = sender_state_with_storage(&server.url(), mock.clone());

        let err = super::validate_bitmap_consistency(
            &state.storage,
            &state.rpc_client,
            None,
            Some(Pubkey::new_unique()),
        )
        .await
        .unwrap_err();

        match err {
            OperatorError::Program(crate::error::ProgramError::UnexplainedConsumedNonces {
                nonces,
            }) => assert_eq!(nonces, vec![2]),
            other => panic!("expected UnexplainedConsumedNonces, got: {other}"),
        }
        let row = mock
            .pending_transactions
            .lock()
            .unwrap()
            .iter()
            .find(|txn| txn.id == 7)
            .cloned()
            .expect("seeded row");
        assert_eq!(row.status, TransactionStatus::Processing);
    }

    /// A journal read error on an earlier nonce must not stop a later double payout being found.
    #[tokio::test]
    async fn validate_bitmap_consistency_double_payout_halts_despite_an_earlier_journal_error() {
        let mut server = mockito::Server::new_async().await;
        let _anchor = mock_finalized_anchor(&mut server, vec![1]);
        let _bitmap = mock_bitmap_account(&mut server, 0, &[2, 3]);

        let mock = MockStorage::new();
        seed_withdrawal(
            &mock,
            7,
            2,
            TransactionStatus::Processing,
            Some(Signature::new_unique()),
        );
        seed_withdrawal(&mock, 8, 3, TransactionStatus::FailedReminted, None);
        mock.set_should_fail("get_release_signatures", true);

        let state = sender_state_with_storage(&server.url(), mock);

        let err = super::validate_bitmap_consistency(
            &state.storage,
            &state.rpc_client,
            None,
            Some(Pubkey::new_unique()),
        )
        .await
        .unwrap_err();

        match err {
            OperatorError::Program(crate::error::ProgramError::BitmapDivergence {
                chain_only,
                ..
            }) => assert_eq!(chain_only, vec![3]),
            other => panic!("expected BitmapDivergence, got: {other}"),
        }
    }

    /// A journal entry that does not parse is corrupt, so the row is left as it is rather
    /// than repaired from the entries that do parse, and the nonce stays unexplained.
    #[tokio::test]
    async fn chain_ahead_nonce_with_a_corrupt_journal_leaves_the_row_untouched() {
        let transaction_id = 7;
        let mock = MockStorage::new();
        seed_withdrawal(
            &mock,
            transaction_id,
            2,
            TransactionStatus::Processing,
            None,
        );
        mock.release_signatures.lock().unwrap().insert(
            transaction_id,
            vec![
                crate::storage::common::models::StoredSig {
                    signature: "not-a-signature".to_string(),
                    last_valid_block_height: 1,
                    blockhash_slot: None,
                },
                crate::storage::common::models::StoredSig {
                    signature: Signature::new_unique().to_string(),
                    last_valid_block_height: 1,
                    blockhash_slot: None,
                },
            ],
        );

        let state = make_sender_state(mock.clone());

        let outcome = super::resolve_chain_ahead_nonce(
            &state.storage,
            &FinalityRpc::solana(&state.rpc_client, None),
            2,
        )
        .await;

        assert!(matches!(outcome, super::ChainAheadOutcome::Unexplained));
        let row = mock
            .pending_transactions
            .lock()
            .unwrap()
            .iter()
            .find(|txn| txn.id == transaction_id)
            .cloned()
            .expect("seeded row");
        assert_eq!(row.status, TransactionStatus::Processing);
    }

    /// A payout no signature accounts for is unexplained money leaving the instance, so it must not report as repaired.
    #[tokio::test]
    async fn chain_ahead_nonce_with_no_attributable_signature_reports_unexplained() {
        let mock = MockStorage::new();
        seed_withdrawal(&mock, 7, 2, TransactionStatus::Processing, None);

        let state = make_sender_state(mock);

        let outcome = super::resolve_chain_ahead_nonce(
            &state.storage,
            &FinalityRpc::solana(&state.rpc_client, None),
            2,
        )
        .await;

        assert!(
            matches!(outcome, super::ChainAheadOutcome::Unexplained),
            "an unattributed payout is not a repair"
        );
        assert_eq!(
            state
                .storage
                .get_withdrawal_by_nonce(2)
                .await
                .unwrap()
                .unwrap()
                .status,
            TransactionStatus::Processing,
            "the gate refuses boot instead of writing the row"
        );
    }

    /// The other half of that rule: an attributed payout is genuinely closed and must raise no alert.
    #[tokio::test]
    async fn chain_ahead_nonce_matched_to_a_landed_signature_reports_repaired() {
        let mut server = mockito::Server::new_async().await;
        let landed = Signature::new_unique();
        let _statuses = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "method": "getSignatureStatuses"
            })))
            .with_status(200)
            .with_body(
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {
                        "context": {"slot": 100},
                        "value": [{
                            "slot": 10,
                            "confirmations": null,
                            "err": null,
                            "status": {"Ok": null},
                            "confirmationStatus": "finalized"
                        }]
                    }
                })
                .to_string(),
            )
            .create();

        let transaction_id = 7;
        let nonce = 2;
        let mock = MockStorage::new();
        seed_withdrawal(
            &mock,
            transaction_id,
            nonce,
            TransactionStatus::Processing,
            Some(landed),
        );

        let state = sender_state_with_storage(&server.url(), mock.clone());

        let outcome = super::resolve_chain_ahead_nonce(
            &state.storage,
            &FinalityRpc::solana(&state.rpc_client, None),
            nonce,
        )
        .await;

        assert!(
            matches!(outcome, super::ChainAheadOutcome::Repaired),
            "an attributed payout closes the gap"
        );
        let row = mock
            .pending_transactions
            .lock()
            .unwrap()
            .iter()
            .find(|txn| txn.id == transaction_id)
            .cloned()
            .expect("seeded row");
        assert_eq!(row.status, TransactionStatus::Completed);
    }

    /// A pending_remint row belongs to the sender, which may be reminting it, so the
    /// repair must go through the CAS that is fenced on the sender lock rather than the
    /// unfenced status writer.
    #[tokio::test]
    async fn chain_ahead_pending_remint_row_completes_through_the_stalled_cas() {
        let mut server = mockito::Server::new_async().await;
        let landed = Signature::new_unique();
        let _statuses = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "method": "getSignatureStatuses"
            })))
            .with_status(200)
            .with_body(
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {
                        "context": {"slot": 100},
                        "value": [{
                            "slot": 10,
                            "confirmations": null,
                            "err": null,
                            "status": {"Ok": null},
                            "confirmationStatus": "finalized"
                        }]
                    }
                })
                .to_string(),
            )
            .create();

        let transaction_id = 7;
        let nonce = 2;
        let mock = MockStorage::new();
        seed_withdrawal(
            &mock,
            transaction_id,
            nonce,
            TransactionStatus::PendingRemint,
            Some(landed),
        );

        let state = sender_state_with_storage(&server.url(), mock.clone());

        let outcome = super::resolve_chain_ahead_nonce(
            &state.storage,
            &FinalityRpc::solana(&state.rpc_client, None),
            nonce,
        )
        .await;

        assert!(matches!(outcome, super::ChainAheadOutcome::Repaired));
        let row = mock
            .pending_transactions
            .lock()
            .unwrap()
            .iter()
            .find(|txn| txn.id == transaction_id)
            .cloned()
            .expect("seeded row");
        assert_eq!(row.status, TransactionStatus::Completed);
        assert_eq!(row.counterpart_signature, Some(landed.to_string()));
    }

    /// A repair the database refused is still an unattributed payout, so it must not report as repaired.
    #[tokio::test]
    async fn chain_ahead_pending_remint_row_whose_cas_fails_reports_unrepaired() {
        let mut server = mockito::Server::new_async().await;
        let landed = Signature::new_unique();
        let _statuses = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "method": "getSignatureStatuses"
            })))
            .with_status(200)
            .with_body(
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {
                        "context": {"slot": 100},
                        "value": [{
                            "slot": 10,
                            "confirmations": null,
                            "err": null,
                            "status": {"Ok": null},
                            "confirmationStatus": "finalized"
                        }]
                    }
                })
                .to_string(),
            )
            .create();

        let nonce = 2;
        let mock = MockStorage::new();
        seed_withdrawal(
            &mock,
            7,
            nonce,
            TransactionStatus::PendingRemint,
            Some(landed),
        );
        mock.set_should_fail("try_complete_stalled_withdrawal", true);

        let state = sender_state_with_storage(&server.url(), mock);

        let outcome = super::resolve_chain_ahead_nonce(
            &state.storage,
            &FinalityRpc::solana(&state.rpc_client, None),
            nonce,
        )
        .await;

        assert!(matches!(outcome, super::ChainAheadOutcome::Unexplained));
    }

    /// The repair decides on a status read before its RPC call. If this operator's lock
    /// dies during that call, a replacement can move the row to pending_remint and start
    /// reminting it, so the stale completion must not land on the row it now sees.
    #[tokio::test]
    async fn chain_ahead_processing_row_moved_to_pending_remint_mid_repair_is_not_completed() {
        let transaction_id = 7;
        let nonce = 2;
        let mock = MockStorage::new();
        seed_withdrawal(
            &mock,
            transaction_id,
            nonce,
            TransactionStatus::Processing,
            Some(Signature::new_unique()),
        );

        // The replacement's transition lands while our status read is in flight.
        let replacement = mock.clone();
        let mut server = mockito::Server::new_async().await;
        let _statuses = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "method": "getSignatureStatuses"
            })))
            .with_status(200)
            .with_body_from_request(move |_| {
                for txn in replacement.pending_transactions.lock().unwrap().iter_mut() {
                    if txn.id == transaction_id {
                        txn.status = TransactionStatus::PendingRemint;
                        txn.updated_at += chrono::Duration::seconds(1);
                    }
                }
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {
                        "context": {"slot": 100},
                        "value": [{
                            "slot": 10,
                            "confirmations": null,
                            "err": null,
                            "status": {"Ok": null},
                            "confirmationStatus": "finalized"
                        }]
                    }
                })
                .to_string()
                .into_bytes()
            })
            .create();

        let state = sender_state_with_storage(&server.url(), mock.clone());

        let outcome = super::resolve_chain_ahead_nonce(
            &state.storage,
            &FinalityRpc::solana(&state.rpc_client, None),
            nonce,
        )
        .await;

        assert!(matches!(outcome, super::ChainAheadOutcome::Unexplained));
        let row = mock
            .pending_transactions
            .lock()
            .unwrap()
            .iter()
            .find(|txn| txn.id == transaction_id)
            .cloned()
            .expect("seeded row");
        assert_eq!(row.status, TransactionStatus::PendingRemint);
    }

    /// A row that already refunded the burn, whose nonce the chain also records
    /// as released, means both sides paid. That is a settled double payout, not
    /// a bookkeeping gap, and the operator must not keep sending withdrawals on
    /// top of a balance it can no longer account for.
    #[tokio::test]
    async fn validate_bitmap_consistency_reminted_row_with_consumed_nonce_halts() {
        let mut server = mockito::Server::new_async().await;
        let _anchor = mock_finalized_anchor(&mut server, vec![1]);
        let _bitmap = mock_bitmap_account(&mut server, 0, &[2]);

        let mock = MockStorage::new();
        seed_withdrawal(&mock, 7, 2, TransactionStatus::FailedReminted, None);

        let state = sender_state_with_storage(&server.url(), mock);

        let err = super::validate_bitmap_consistency(
            &state.storage,
            &state.rpc_client,
            None,
            Some(Pubkey::new_unique()),
        )
        .await
        .unwrap_err();

        match err {
            OperatorError::Program(crate::error::ProgramError::BitmapDivergence {
                chain_only,
                ..
            }) => assert_eq!(
                chain_only,
                vec![2],
                "the halt must name the paid-twice nonce"
            ),
            other => panic!("expected BitmapDivergence, got: {other}"),
        }
    }

    /// A row the repair cannot write is still explained by its own landed release, so
    /// boot goes on and the row is left as it is for the sweep or a human.
    #[tokio::test]
    async fn validate_bitmap_consistency_chain_ahead_on_unwritable_row_is_explained() {
        for status in [TransactionStatus::Failed, TransactionStatus::ManualReview] {
            let mut server = mockito::Server::new_async().await;
            let _anchor = mock_finalized_anchor(&mut server, vec![1]);
            let landed = Signature::new_unique();
            let _bitmap = mock_bitmap_account(&mut server, 0, &[2]);
            let _statuses = server
                .mock("POST", "/")
                .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                    "method": "getSignatureStatuses"
                })))
                .with_status(200)
                .with_body(
                    serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": 1,
                        "result": {
                            "context": {"slot": 100},
                            "value": [{
                                "slot": 10,
                                "confirmations": null,
                                "err": null,
                                "status": {"Ok": null},
                                "confirmationStatus": "finalized"
                            }]
                        }
                    })
                    .to_string(),
                )
                .create();

            let mock = MockStorage::new();
            seed_withdrawal(&mock, 7, 2, status, Some(landed));

            let state = sender_state_with_storage(&server.url(), mock);

            let result = super::validate_bitmap_consistency(
                &state.storage,
                &state.rpc_client,
                None,
                Some(Pubkey::new_unique()),
            )
            .await;

            assert!(
                result.is_ok(),
                "a landed release on the row explains the nonce: {result:?}"
            );
            assert_eq!(
                state
                    .storage
                    .get_withdrawal_by_nonce(2)
                    .await
                    .unwrap()
                    .unwrap()
                    .status,
                status,
                "the row is not written"
            );
        }
    }

    /// The re-read exists to fail closed on the one divergence that matters, so
    /// a re-read that cannot be taken must keep the first verdict. Letting the
    /// read error escape instead downgrades the refusal to start into a warning,
    /// because only the divergence itself stops the boot.
    #[tokio::test]
    async fn validate_bitmap_consistency_db_ahead_halts_when_the_reread_fails() {
        let mut server = mockito::Server::new_async().await;
        let _anchor = mock_finalized_anchor(&mut server, vec![1]);
        let (_bitmap, reads) = mock_bitmap_then_read_failure(&mut server, 0, &[]);

        let mock = MockStorage::new();
        seed_withdrawal(&mock, 5, 4, TransactionStatus::Completed, None);
        let state = sender_state_with_storage(&server.url(), mock);

        let err = super::validate_bitmap_consistency(
            &state.storage,
            &state.rpc_client,
            None,
            Some(Pubkey::new_unique()),
        )
        .await
        .unwrap_err();

        assert!(
            matches!(
                err,
                OperatorError::Program(crate::error::ProgramError::BitmapDivergence { .. })
            ),
            "an unconfirmable divergence must still refuse to start: {err:?}"
        );
        assert_eq!(reads.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    /// Database ahead: the operator believes in a release the chain denies, so
    /// every later decision it makes would rest on a false history. Refuse.
    #[tokio::test]
    async fn validate_bitmap_consistency_db_ahead_halts() {
        let mut server = mockito::Server::new_async().await;
        let _anchor = mock_finalized_anchor(&mut server, vec![1]);
        let _bitmap = mock_bitmap_account(&mut server, 0, &[]);

        let mock = MockStorage::new();
        seed_withdrawal(&mock, 5, 4, TransactionStatus::Completed, None);

        let state = sender_state_with_storage(&server.url(), mock);

        let err = super::validate_bitmap_consistency(
            &state.storage,
            &state.rpc_client,
            None,
            Some(Pubkey::new_unique()),
        )
        .await
        .unwrap_err();

        match err {
            OperatorError::Program(crate::error::ProgramError::BitmapDivergence {
                db_only,
                ..
            }) => assert_eq!(db_only, vec![4], "the halt must name the offending nonce"),
            other => panic!("expected BitmapDivergence, got: {other}"),
        }
    }

    /// Divergence in both directions still halts: the database-ahead side is the
    /// one that decides, and the chain-ahead nonces ride along in the report.
    #[tokio::test]
    async fn validate_bitmap_consistency_both_directions_halts() {
        let mut server = mockito::Server::new_async().await;
        let _anchor = mock_finalized_anchor(&mut server, vec![1]);
        let _bitmap = mock_bitmap_account(&mut server, 0, &[6]);

        let mock = MockStorage::new();
        seed_withdrawal(&mock, 5, 4, TransactionStatus::Completed, None);

        let state = sender_state_with_storage(&server.url(), mock);

        let err = super::validate_bitmap_consistency(
            &state.storage,
            &state.rpc_client,
            None,
            Some(Pubkey::new_unique()),
        )
        .await
        .unwrap_err();

        match err {
            OperatorError::Program(crate::error::ProgramError::BitmapDivergence {
                db_only,
                chain_only,
            }) => {
                assert_eq!(db_only, vec![4]);
                assert_eq!(chain_only, vec![6]);
            }
            other => panic!("expected BitmapDivergence, got: {other}"),
        }
    }

    /// A release landing between the bitmap read and the database read looks
    /// exactly like database-ahead. The second read must be taken and believed,
    /// or every such race becomes a spurious refusal to start.
    #[tokio::test]
    async fn validate_bitmap_consistency_db_ahead_rereads_before_halting() {
        let mut server = mockito::Server::new_async().await;
        let _anchor = mock_finalized_anchor(&mut server, vec![1]);
        // First read predates the release, second sees it. That is the race.
        let (_bitmap, reads) = mock_bitmap_sequence(&mut server, vec![(0, vec![]), (0, vec![4])]);

        let mock = MockStorage::new();
        seed_withdrawal(&mock, 5, 4, TransactionStatus::Completed, None);
        let state = sender_state_with_storage(&server.url(), mock);

        let result = super::validate_bitmap_consistency(
            &state.storage,
            &state.rpc_client,
            None,
            Some(Pubkey::new_unique()),
        )
        .await;

        assert!(
            result.is_ok(),
            "a clean re-read must clear the divergence: {result:?}"
        );
        assert_eq!(
            reads.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "the halt direction must consult the bitmap a second time"
        );
    }

    /// A divergence that survives the re-read is real and must still halt, or
    /// the race handling would swallow every genuine one.
    #[tokio::test]
    async fn validate_bitmap_consistency_db_ahead_halts_after_reread_confirms() {
        let mut server = mockito::Server::new_async().await;
        let _anchor = mock_finalized_anchor(&mut server, vec![1]);
        let (_bitmap, reads) = mock_bitmap_sequence(&mut server, vec![(0, vec![])]);

        let mock = MockStorage::new();
        seed_withdrawal(&mock, 5, 4, TransactionStatus::Completed, None);
        let state = sender_state_with_storage(&server.url(), mock);

        let err = super::validate_bitmap_consistency(
            &state.storage,
            &state.rpc_client,
            None,
            Some(Pubkey::new_unique()),
        )
        .await
        .unwrap_err();

        assert!(matches!(
            err,
            OperatorError::Program(crate::error::ProgramError::BitmapDivergence { .. })
        ));
        assert_eq!(reads.load(std::sync::atomic::Ordering::SeqCst), 2);
    }
    /// Behind a load balancer the bitmap can come from a backend that has not
    /// caught up, and its clear bits read as a database claiming releases the
    /// chain never made. Binding both reads to a proven slot makes a lagging
    /// backend error instead. The confirmation takes its own anchor, because
    /// re-reading at the first slot would return the identical snapshot.
    #[tokio::test]
    async fn validate_bitmap_consistency_binds_both_reads_to_a_proven_slot() {
        let mut server = mockito::Server::new_async().await;
        let _anchor = mock_finalized_anchor(&mut server, vec![500, 900]);
        let boot = mock_bitmap_at_slot(&mut server, 500, 0, &[]);
        let reread = mock_bitmap_at_slot(&mut server, 900, 0, &[4]);

        let mock = MockStorage::new();
        seed_withdrawal(&mock, 5, 4, TransactionStatus::Completed, None);
        let state = sender_state_with_storage(&server.url(), mock);

        let result = super::validate_bitmap_consistency(
            &state.storage,
            &state.rpc_client,
            None,
            Some(Pubkey::new_unique()),
        )
        .await;

        assert!(
            result.is_ok(),
            "a re-read at a fresh anchor must clear the divergence: {result:?}"
        );
        boot.assert();
        reread.assert();
    }

    /// A bit set only at confirmed can still be rolled back by a fork, so it must not
    /// count as proof that a release landed. The client itself runs at confirmed.
    #[tokio::test]
    async fn verify_release_landed_reads_bitmap_at_finalized() {
        let mut server = mockito::Server::new_async().await;
        let _anchor = mock_finalized_anchor(&mut server, vec![500]);
        let _finalized = mock_bitmap_at_slot(&mut server, 500, 0, &[]);
        let _fork_only = mock_bitmap_at_commitment(&mut server, "confirmed", 0, &[4]);
        let state = sender_state_with_storage(&server.url(), MockStorage::new());

        let verdict =
            super::verify_release_landed(&state.rpc_client, Some(Pubkey::new_unique()), 4, 10)
                .await;

        assert!(
            matches!(verdict, ReleaseVerdict::NotLanded),
            "a fork-only bit must not read as a landed release"
        );
    }

    fn generation_reader(url: &str) -> SenderState {
        let mut state = sender_state_with_storage(url, MockStorage::new());
        state.instance_pda = Some(Pubkey::new_unique());
        state
    }

    /// The floor is the latest of the finalized anchor, any earlier anchor and any refusal.
    #[tokio::test]
    async fn generation_floor_is_max_of_anchor_high_water_and_refusal_floor() {
        for (refusal_floor, expected) in [(0u64, 300u64), (500, 500)] {
            let mut server = mockito::Server::new_async().await;
            let _anchor = mock_finalized_anchor(&mut server, vec![300]);
            let mut state = generation_reader(&server.url());
            state.refusal_floor = refusal_floor;

            assert_eq!(state.generation_floor().await.unwrap(), expected);
        }
    }

    /// A later anchor from a lagging backend cannot pull the floor back.
    #[tokio::test]
    async fn anchor_high_water_only_rises() {
        let mut server = mockito::Server::new_async().await;
        let _anchor = mock_finalized_anchor(&mut server, vec![500, 400]);
        let mut state = generation_reader(&server.url());

        assert_eq!(state.generation_floor().await.unwrap(), 500);
        assert_eq!(state.generation_floor().await.unwrap(), 500);
        assert_eq!(state.anchor_high_water, 500);
    }

    /// A read answered past the anchor raises the floor to its slot, so a later read on an
    /// older backend cannot roll the cached generation back.
    #[tokio::test]
    async fn generation_read_raises_the_floor_to_its_answer_slot() {
        let mut server = mockito::Server::new_async().await;
        let _anchor = mock_finalized_anchor(&mut server, vec![100, 110]);
        let _bitmap = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getAccountInfo""#.into(),
            ))
            .with_status(200)
            .with_body(
                crate::operator::sender::test_support::bitmap_account_response_at(1, &[], 200),
            )
            .create();
        let mut state = generation_reader(&server.url());

        assert_eq!(state.refresh_generation().await.unwrap(), 1);
        assert_eq!(state.generation_floor().await.unwrap(), 200);
    }

    /// An answer behind the floor is a bitmap failure, and what the cache knew stays.
    #[tokio::test]
    async fn fetch_current_generation_leaves_the_cache_when_behind_the_floor() {
        let mut server = mockito::Server::new_async().await;
        let _anchor = mock_finalized_anchor(&mut server, vec![500]);
        let _behind = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getAccountInfo""#.into(),
            ))
            .with_status(200)
            .with_body(
                crate::operator::sender::test_support::bitmap_account_response_at(2, &[], 400),
            )
            .create();
        let mut state = generation_reader(&server.url());
        state.cached_generation = Some(3);

        let err = state.refresh_generation().await.unwrap_err();

        assert!(
            matches!(
                err,
                OperatorError::Program(crate::error::ProgramError::BitmapUnavailable { .. })
            ),
            "{err:?}"
        );
        assert_eq!(state.cached_generation, Some(3));
    }

    /// A clear bit read at the anchored finalized slot reports that slot, the bound of
    /// every slot a release could sit in.
    #[tokio::test]
    async fn verify_release_landed_at_carries_the_bitmap_slot() {
        let mut server = mockito::Server::new_async().await;
        let _anchor = mock_finalized_anchor(&mut server, vec![500]);
        let _finalized = mock_bitmap_at_slot(&mut server, 500, 0, &[]);
        let state = generation_reader(&server.url());

        let (verdict, slot) =
            super::verify_release_landed_at(&state.rpc_client, state.instance_pda, 4, 10).await;

        assert!(matches!(verdict, ReleaseVerdict::NotLanded));
        assert_eq!(slot, Some(500));
    }

    /// A rotation is final whatever the attempt's lvbh, so a rotated-past nonce is
    /// reported, with its slot, even while the tip has not passed the lvbh.
    #[tokio::test]
    async fn rotated_past_is_reported_even_before_the_tip_passes_lvbh() {
        // The anchor's tip height is 850, below the attempt's lvbh of 900.
        for (generation, rotated) in [(1u64, true), (0, false)] {
            let mut server = mockito::Server::new_async().await;
            let _anchor = mock_finalized_anchor(&mut server, vec![500]);
            let _finalized = mock_bitmap_at_slot(&mut server, 500, generation, &[]);
            let state = generation_reader(&server.url());

            let (verdict, slot) =
                super::verify_release_landed_at(&state.rpc_client, state.instance_pda, 4, 900)
                    .await;

            if rotated {
                assert!(matches!(verdict, ReleaseVerdict::Uncertain(_)));
                assert_eq!(slot, Some(500));
            } else {
                assert!(matches!(verdict, ReleaseVerdict::Pending(_)));
                assert_eq!(slot, None);
            }
        }
    }

    /// Releases now stay processing until finality, so a restart in that window sees a bit
    /// at confirmed whose signature is not final yet. The boot read must ignore it rather
    /// than send the row to manual review.
    #[tokio::test]
    async fn validate_bitmap_consistency_ignores_a_confirmed_only_bit() {
        let mut server = mockito::Server::new_async().await;
        let _anchor = mock_finalized_anchor(&mut server, vec![500]);
        let _finalized = mock_bitmap_at_slot(&mut server, 500, 0, &[]);
        let _fork_only = mock_bitmap_at_commitment(&mut server, "confirmed", 0, &[4]);
        let _statuses = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getSignatureStatuses""#.into(),
            ))
            .with_status(200)
            .with_body(
                r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[{
                "slot":100,"confirmations":5,"err":null,"status":{"Ok":null},
                "confirmationStatus":"confirmed"}]},"id":0}"#,
            )
            .create();

        let mock = MockStorage::new();
        seed_withdrawal(&mock, 5, 4, TransactionStatus::Processing, None);
        let state = sender_state_with_storage(&server.url(), mock);
        state
            .storage
            .insert_release_signature(5, Signature::new_unique().to_string(), 1, None)
            .await
            .unwrap();

        let result = super::validate_bitmap_consistency(
            &state.storage,
            &state.rpc_client,
            None,
            Some(Pubkey::new_unique()),
        )
        .await;

        assert!(result.is_ok(), "boot must pass: {result:?}");
        assert_eq!(
            state
                .storage
                .get_withdrawal_by_nonce(4)
                .await
                .unwrap()
                .unwrap()
                .status,
            TransactionStatus::Processing,
            "an in-flight release must not be touched at boot"
        );
    }

    /// The anchor is what makes the bitmap trustworthy, so without one there is
    /// nothing to diff against. The check reports that it could not run, the
    /// same way an unreadable bitmap does, rather than clearing a boot on bits
    /// no one proved were current.
    #[tokio::test]
    async fn validate_bitmap_consistency_gives_no_verdict_when_the_anchor_read_fails() {
        let mut server = mockito::Server::new_async().await;
        let _down = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getLatestBlockhash""#.into(),
            ))
            .with_status(500)
            .with_body("boom")
            .create();
        // Agrees with the database, so an unanchored read would start cleanly.
        let _bitmap = mock_bitmap_account(&mut server, 0, &[]);

        let state = sender_state_with_storage(&server.url(), MockStorage::new());

        let err = super::validate_bitmap_consistency(
            &state.storage,
            &state.rpc_client,
            None,
            Some(Pubkey::new_unique()),
        )
        .await
        .unwrap_err();

        assert!(
            matches!(
                err,
                OperatorError::Program(crate::error::ProgramError::BitmapUnavailable { .. })
            ),
            "an unanchored bitmap must yield no verdict: {err:?}"
        );
    }
}
