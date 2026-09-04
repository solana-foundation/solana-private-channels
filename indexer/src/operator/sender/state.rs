use crate::channel_utils::send_guaranteed;
use crate::config::ProgramType;
use crate::error::account::AccountError;
use crate::error::OperatorError;
use crate::operator::sender::types::{PendingRemint, PendingSig, TransactionContext};
use crate::operator::tree_constants::MAX_TREE_LEAVES;
use crate::operator::utils::instruction_util::ResetSmtRootBuilderWithTarget;
use crate::operator::utils::smt_util::SmtState;
use crate::operator::{
    find_event_authority_pda, find_operator_pda, parse_instance, RetryConfig, RpcClientWithRetry,
    SignerUtil,
};
use crate::operator::{MintCache, TransactionStatusUpdate, WithdrawalRemintInfo};
use crate::storage::common::storage::Storage;
use crate::storage::TransactionStatus;
use crate::PrivateChannelIndexerConfig;
use chrono::Utc;
use private_channel_metrics::MetricLabel;
use solana_sdk::clock::MAX_PROCESSING_AGE;
use solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use spl_associated_token_account::get_associated_token_address_with_program_id;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::{mpsc, Semaphore};
use tracing::{error, info, warn};

use super::types::{InFlightQueue, SenderSMTState, SenderState, MAX_IN_FLIGHT};

impl SenderState {
    /// `channel_blockhash_window` is seeded off the channel endpoint at operator
    /// startup, never configured, and the retention proof re-reads it per verdict.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        config: &PrivateChannelIndexerConfig,
        operator_commitment: CommitmentLevel,
        instance_pda: Option<Pubkey>,
        storage: Arc<Storage>,
        retry_max_attempts: u32,
        confirmation_poll_interval_ms: u64,
        source_rpc_client: Option<Arc<RpcClientWithRetry>>,
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
            smt_state: None,
            retry_counts: HashMap::new(),
            mint_cache,
            mint_builders: HashMap::new(),
            retry_max_attempts,
            confirmation_poll_interval_ms,
            rotation_retry_queue: Vec::new(),
            ambiguous_retry_queue: Vec::new(),
            pending_rotation: None,
            program_type: config.program_type,
            remint_cache: HashMap::new(),
            pending_signatures: HashMap::new(),
            release_leases: HashMap::new(),
            pending_remints: Vec::new(),
            in_flight: InFlightQueue::new(),
            semaphore: Arc::new(Semaphore::new(MAX_IN_FLIGHT)),
        })
    }

    /// Initialize SMT state lazily on first use
    /// Fetches tree_index from chain and populates SMT with completed withdrawals from DB
    pub(super) async fn initialize_smt_state(&mut self) -> Result<(), OperatorError> {
        let smt_state =
            validate_smt_root(&self.storage, &self.rpc_client, self.instance_pda).await?;

        self.smt_state = Some(SenderSMTState {
            smt_state,
            nonce_to_builder: HashMap::new(),
        });

        Ok(())
    }
}

/// Build the local SMT for the current tree window from DB-completed nonces and
/// assert it matches the on-chain root, returning the built tree on agreement.
///
/// Shared by the sender's lazy `initialize_smt_state` (which needs the tree for
/// proofs) and the boot pre-flight (which uses it purely as a consistency gate).
pub(crate) async fn validate_smt_root(
    storage: &Storage,
    rpc_client: &RpcClientWithRetry,
    instance_pda: Option<Pubkey>,
) -> Result<SmtState, OperatorError> {
    let instance_pda = instance_pda.ok_or_else(|| AccountError::InstanceNotFound {
        instance: Pubkey::default(),
    })?;

    info!("Validating SMT root for instance {}", instance_pda);

    let instance_data = rpc_client
        .get_account_data(&instance_pda)
        .await
        .map_err(|_| AccountError::AccountNotFound {
            pubkey: instance_pda,
        })?;

    let instance =
        parse_instance(&instance_data).map_err(|e| AccountError::AccountDeserializationFailed {
            pubkey: instance_pda,
            reason: e.to_string(),
        })?;

    let tree_index = instance.current_tree_index;
    let smt_state = rebuild_completed_tree(storage, tree_index).await?;

    let computed_root = smt_state.current_root();
    let onchain_root = instance.withdrawal_transactions_root;

    if computed_root != onchain_root {
        error!(
            instance = %instance_pda,
            tree_index,
            local_root = ?computed_root,
            onchain_root = ?onchain_root,
            nonces = ?smt_state.get_nonces(),
            "SMT root mismatch: database out of sync with on-chain state. \
             A release likely landed on-chain but its Completed write was lost; \
             resync the database from on-chain events to reconcile."
        );

        return Err(crate::error::ProgramError::SmtRootMismatch {
            local_root: computed_root,
            onchain_root,
        }
        .into());
    }

    info!(
        tree_index,
        nonces = smt_state.nonce_count(),
        "SMT root verification passed"
    );

    Ok(smt_state)
}

/// Rebuild the local SMT for `tree_index` from the DB-completed withdrawal
/// nonces in that tree's window. Shared by `validate_smt_root` and the release
/// verifier so both reason from the identical window math and insert path.
async fn rebuild_completed_tree(
    storage: &Storage,
    tree_index: u64,
) -> Result<SmtState, OperatorError> {
    let leaves = MAX_TREE_LEAVES as u64;
    // Window is [tree_index*leaves, (tree_index+1)*leaves). Checked math so a
    // corrupt tree_index cannot wrap into the wrong window.
    let min_nonce =
        tree_index
            .checked_mul(leaves)
            .ok_or(AccountError::AccountIndexOutOfBounds {
                index: tree_index as usize,
            })?;
    let max_nonce = tree_index
        .checked_add(1)
        .and_then(|t| t.checked_mul(leaves))
        .ok_or(AccountError::AccountIndexOutOfBounds {
            index: tree_index as usize,
        })?;

    let nonces = storage
        .get_completed_withdrawal_nonces(min_nonce, max_nonce)
        .await?;

    let mut smt_state = SmtState::new(tree_index);
    for nonce in &nonces {
        smt_state.insert_nonce(*nonce);
    }
    Ok(smt_state)
}

/// Three-way outcome of confirming a release from the on-chain SMT root.
/// `Uncertain` fails closed: every read/parse/window/staleness ambiguity maps
/// here, never to `NotLanded`, so a demote or remint never fires on doubt.
pub(crate) enum ReleaseVerdict {
    /// The on-chain root matches the completed set WITH the candidate nonce.
    Landed,
    /// The on-chain root matches the completed set WITHOUT the candidate nonce.
    NotLanded,
    /// Could not prove either way; treat as still-pending.
    Uncertain(String),
}

/// Check whether the withdrawal `nonce` actually released on-chain, so recovery
/// never re-pays a release that a pruned or lagging RPC endpoint is hiding.
///
/// The escrow instance holds a Merkle root over every released nonce. We read it
/// at finalized commitment, rebuild the same tree from our own DB, and compare.
/// Any doubt returns `Uncertain`, never a false `NotLanded`.
///
/// The hard part is proving the snapshot is fresh even behind a load-balanced RPC
/// where two calls can hit different backends. We first read the finalized latest
/// blockhash together with its response context slot in one call; the tip block
/// height at that slot is the blockhash's last_valid_block_height minus
/// MAX_PROCESSING_AGE. If that tip height is past every attempt's lvbh we then read
/// the instance account requiring the node to answer at a context slot at least the
/// blockhash's slot. That binds the account snapshot to a finalized height we have
/// already proven fresh, so a lagging backend that still excludes a released nonce
/// cannot pass as authoritative. The nonce must also belong to the tree the instance
/// currently holds, and the rebuilt root must equal the on-chain root either with
/// the nonce (`Landed`) or without it (`NotLanded`).
pub(crate) async fn verify_release_landed(
    rpc: &RpcClientWithRetry,
    storage: &Storage,
    instance_pda: Option<Pubkey>,
    nonce: u64,
    max_lvbh: u64,
) -> ReleaseVerdict {
    let Some(instance_pda) = instance_pda else {
        return ReleaseVerdict::Uncertain("no instance pda configured".to_string());
    };

    // Freshness: read the finalized latest blockhash and its context slot in one
    // call so the slot and its height agree. The tip height at that slot is the
    // blockhash's last_valid_block_height minus MAX_PROCESSING_AGE (a blockhash
    // stays valid for that many blocks past the tip it was taken at).
    let (ref_slot, lvbh0) = match rpc
        .get_latest_blockhash_with_context(CommitmentConfig::finalized())
        .await
    {
        Ok(v) => v,
        Err(e) => {
            return ReleaseVerdict::Uncertain(format!("finalized blockhash read failed: {e}"))
        }
    };
    let Some(tip_height) = lvbh0.checked_sub(MAX_PROCESSING_AGE as u64) else {
        return ReleaseVerdict::Uncertain(format!(
            "finalized last_valid_block_height {lvbh0} below MAX_PROCESSING_AGE; cannot derive tip height"
        ));
    };

    // The tip must be strictly past every attempt's lvbh. At height == lvbh a
    // release can still land, so a snapshot only at that height could miss an
    // edge-of-window release and wrongly report NotLanded, risking a double-pay.
    if tip_height <= max_lvbh {
        return ReleaseVerdict::Uncertain(format!(
            "finalized tip height {tip_height} not past max lvbh {max_lvbh}; too stale to prove non-inclusion"
        ));
    }

    // Bind the account snapshot to that proven-fresh point: require the node to
    // answer at a context slot at least ref_slot, so its height is at least
    // tip_height and therefore past max_lvbh. A lagging backend that cannot serve
    // there errors instead of returning an older root, and we fail closed.
    let response = match rpc
        .get_account_with_context_min_slot(
            &instance_pda,
            CommitmentConfig::finalized(),
            Some(ref_slot),
        )
        .await
    {
        Ok(r) => r,
        Err(e) => return ReleaseVerdict::Uncertain(format!("instance read failed: {e}")),
    };
    let Some(account) = response.value else {
        return ReleaseVerdict::Uncertain(format!(
            "instance {instance_pda} absent at finalized commitment"
        ));
    };
    let instance = match parse_instance(&account.data) {
        Ok(i) => i,
        Err(e) => return ReleaseVerdict::Uncertain(format!("instance parse failed: {e}")),
    };

    // Tree-window check: membership can only be proven against the tree currently
    // on-chain; a rotated-away nonce needs a historical root we do not fetch.
    let expected_tree = nonce / MAX_TREE_LEAVES as u64;
    if expected_tree != instance.current_tree_index {
        return ReleaseVerdict::Uncertain(format!(
            "nonce {nonce} maps to tree {expected_tree}, on-chain tree is {}",
            instance.current_tree_index
        ));
    }

    // Root-membership check: root equality is proof because the SMT is
    // order-independent and collision-correct. Compare against the completed set
    // without, then with, the candidate nonce.
    let mut tree = match rebuild_completed_tree(storage, instance.current_tree_index).await {
        Ok(t) => t,
        Err(e) => return ReleaseVerdict::Uncertain(format!("completed-tree rebuild failed: {e}")),
    };
    let onchain_root = instance.withdrawal_transactions_root;

    // Drop the candidate so `tree` is the completed set WITHOUT it.
    tree.remove_nonce(nonce);
    if tree.current_root() == onchain_root {
        return ReleaseVerdict::NotLanded;
    }
    tree.insert_nonce(nonce);
    if tree.current_root() == onchain_root {
        return ReleaseVerdict::Landed;
    }
    ReleaseVerdict::Uncertain(format!(
        "on-chain root matches neither completed set (with nor without nonce {nonce})"
    ))
}

impl SenderState {
    /// Read the authoritative current_tree_index from the on-chain instance.
    pub(super) async fn fetch_onchain_tree_index(&self) -> Result<u64, OperatorError> {
        let instance_pda = self.instance_pda.ok_or(AccountError::InstanceNotFound {
            instance: Pubkey::default(),
        })?;
        let data = self
            .rpc_client
            .get_account_data(&instance_pda)
            .await
            .map_err(|_| AccountError::AccountNotFound {
                pubkey: instance_pda,
            })?;
        let instance =
            parse_instance(&data).map_err(|e| AccountError::AccountDeserializationFailed {
                pubkey: instance_pda,
                reason: e.to_string(),
            })?;
        Ok(instance.current_tree_index)
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
                release_signatures: None,
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
        // Deferred remints are Withdraw-only; other roles must never claim a shared PendingRemint row.
        if self.program_type != ProgramType::Withdraw {
            return Ok(());
        }

        let transactions = self.storage.get_pending_remint_transactions().await?;

        if transactions.is_empty() {
            return Ok(());
        }

        info!(
            "Recovering {} pending remint(s) from database",
            transactions.len()
        );

        // PrivateChannel only supports SPL Token for now.
        let private_channel_token_program = self.mint_cache.get_private_channel_token_program();

        for tx in transactions {
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

            let Some(user) = Self::or_manual_review(
                Pubkey::from_str(&tx.recipient).map_err(|e| format!("invalid user pubkey: {e}")),
                storage_tx,
                tx.id,
                &tx.trace_id,
            )
            .await
            else {
                continue;
            };

            let user_ata = get_associated_token_address_with_program_id(
                &user,
                &mint,
                &private_channel_token_program,
            );

            let amount = tx.amount.value();

            // Pair each stored signature with its last_valid_block_height. The
            // remint gate needs both to verify the withdrawal cannot still land.
            // An empty array, a bad signature, or an array-length mismatch means
            // we cannot safely run that check, so we escalate to ManualReview.
            let sig_strings = tx.remint_signatures.unwrap_or_default();
            let lvbhs = tx.remint_last_valid_block_heights.unwrap_or_default();

            let parsed: Result<Vec<PendingSig>, String> = if sig_strings.is_empty() {
                Err("no withdrawal signatures stored; cannot verify finality".to_string())
            } else if sig_strings.len() != lvbhs.len() {
                Err(format!(
                    "lvbh length {} != signatures length {}",
                    lvbhs.len(),
                    sig_strings.len()
                ))
            } else {
                sig_strings
                    .iter()
                    .zip(&lvbhs)
                    .map(|(sig_string, &lvbh)| {
                        let signature = Signature::from_str(sig_string)
                            .map_err(|e| format!("invalid withdrawal signature: {e}"))?;
                        let last_valid_block_height = u64::try_from(lvbh)
                            .map_err(|_| format!("negative last_valid_block_height: {lvbh}"))?;
                        Ok(PendingSig {
                            signature,
                            last_valid_block_height,
                            // The transactions-row mirror never carried a slot;
                            // the journal table is the authority for one.
                            blockhash_slot: None,
                        })
                    })
                    .collect()
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
                transaction_id: Some(tx.id),
                // Nonce is not needed for the remint — SMT cleanup already ran in
                // handle_permanent_failure before the row was written as PendingRemint.
                withdrawal_nonce: tx.withdrawal_nonce.map(|n| n as u64),
                trace_id: Some(tx.trace_id.clone()),
                deposit_claim_lease: None,
            };

            let remint_info = WithdrawalRemintInfo {
                transaction_id: tx.id,
                // Build from individual fields: `tx.remint_signatures` was moved out above,
                // so the whole-row borrow `from_row` would take is no longer available.
                source_event_id: crate::operator::instruction_util::SourceEventId::new(
                    &tx.signature,
                    tx.instruction_index,
                    tx.inner_index,
                ),
                trace_id: tx.trace_id.clone(),
                mint,
                user,
                user_ata,
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
            });
        }

        Ok(())
    }

    /// Re-arm the rotation this operator still owes the chain, read from the durable
    /// target the processor wrote before dispatching it. A reset carries no DB row and
    /// no nonce, so without this a crash between arming and confirmation would drop the
    /// only automatic rotation for that boundary.
    ///
    /// Arming is all this does: the rotation tick reads the chain before every attempt,
    /// so a target the chain already reached is disarmed there without sending.
    pub(super) async fn rearm_owed_rotation(&mut self) -> Result<(), OperatorError> {
        // Only the withdraw role can owe a rotation: it is the only one with an escrow
        // instance to reset, so instance_pda is None for every other role.
        let Some(instance_pda) = self.instance_pda else {
            return Ok(());
        };

        let Some(target_tree_index) = self
            .storage
            .get_owed_rotation_target(self.program_type.as_label())
            .await?
        else {
            return Ok(());
        };

        let operator_pubkey = SignerUtil::get_operator_pubkey();
        self.pending_rotation = Some(Box::new(ResetSmtRootBuilderWithTarget::new(
            SignerUtil::get_admin_pubkey(),
            operator_pubkey,
            instance_pda,
            find_operator_pda(&instance_pda, &operator_pubkey),
            find_event_authority_pda(),
            target_tree_index,
        )));

        info!(
            target_tree_index,
            "Re-armed owed tree rotation from persistent storage"
        );

        Ok(())
    }

    /// Retire the rotation now that a chain read proved `target_tree_index` landed.
    /// Clears the durable target first, then the in-memory arm.
    ///
    /// A failed clear is not fatal: the next boot re-arms the target, and the submit
    /// gate's chain read disarms it again without sending anything.
    pub(super) async fn disarm_rotation(&mut self, target_tree_index: u64) {
        if let Err(e) = self
            .storage
            .clear_owed_rotation_target(self.program_type.as_label(), target_tree_index)
            .await
        {
            warn!(target_tree_index, "Owed rotation clear failed: {e}");
        }
        self.pending_rotation = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::MintCache;
    use crate::storage::common::amount::TokenAmount;
    use crate::storage::common::models::{DbTransaction, TransactionStatus, TransactionType};
    use crate::storage::common::storage::mock::MockStorage;
    use crate::storage::Storage;
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine;
    use borsh::BorshSerialize;
    use private_channel_escrow_program_client::Instance;
    use solana_client::rpc_request::RpcRequest;
    use solana_sdk::pubkey::Pubkey;
    use solana_sdk::signature::Signature;
    use std::collections::HashMap;
    use tokio::sync::mpsc;

    fn make_sender_state(mock: MockStorage) -> SenderState {
        make_sender_state_with_role(mock, crate::config::ProgramType::Withdraw)
    }

    fn make_sender_state_with_role(
        mock: MockStorage,
        role: crate::config::ProgramType,
    ) -> SenderState {
        let storage = Arc::new(Storage::Mock(mock));
        let rpc = Arc::new(RpcClientWithRetry::with_retry_config(
            "http://localhost:8899".to_string(),
            RetryConfig::default(),
            solana_sdk::commitment_config::CommitmentConfig::confirmed(),
        ));
        SenderState {
            rpc_client: rpc.clone(),
            source_rpc_client: rpc,
            fallback_rpc_client: None,
            storage: storage.clone(),
            instance_pda: None,
            smt_state: None,
            retry_counts: HashMap::new(),
            mint_builders: HashMap::new(),
            mint_cache: MintCache::new(storage),
            retry_max_attempts: 3,
            confirmation_poll_interval_ms: 400,
            rotation_retry_queue: Vec::new(),
            ambiguous_retry_queue: Vec::new(),
            pending_rotation: None,
            program_type: role,
            remint_cache: HashMap::new(),
            pending_signatures: HashMap::new(),
            release_leases: HashMap::new(),
            pending_remints: Vec::new(),
            in_flight: InFlightQueue::new(),
            semaphore: Arc::new(Semaphore::new(MAX_IN_FLIGHT)),
        }
    }

    /// Build a minimal DbTransaction representing a PendingRemint row.
    /// All string fields use real base58-encoded pubkeys and signatures so
    /// `recover_pending_remints` can parse them without error.
    fn make_pending_remint_row(
        id: i64,
        mint: &Pubkey,
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
            initiator: Pubkey::new_unique().to_string(),
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
        }
    }

    // ── recover_pending_remints: happy path ──────────────────────────

    /// On startup, all PendingRemint rows from the database must be fully
    /// reconstructed into the in-memory `pending_remints` queue so the
    /// operator can continue where it left off before the crash.
    ///
    /// This test verifies that every field is correctly restored:
    /// - transaction_id, trace_id, amount, mint, recipient
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
        let recipient = Pubkey::new_unique();
        let sig = Signature::new_unique();
        let deadline = Utc::now() + chrono::Duration::seconds(20);

        // Mid-budget value so the round-trip assertion is meaningful: a reset
        // to 0 on recovery would re-arm the cap and let an ambiguous row
        // outlive the intended ManualReview escalation.
        let mut row = make_pending_remint_row(42, &mint, &recipient, &sig, deadline);
        row.finality_check_attempts = 2;
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
        assert_eq!(entry.remint_info.user, recipient);

        // Signatures must be parsed back — they drive the finality check.
        // lvbh must round-trip too: the gate needs it to prove a broadcast
        // can no longer land.
        assert_eq!(entry.signatures.len(), 1);
        assert_eq!(entry.signatures[0].signature, sig);
        assert_eq!(entry.signatures[0].last_valid_block_height, 12_345);

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

        let mut row = make_pending_remint_row(7, &mint, &recipient, &sig, deadline);
        row.finality_check_attempts = -1;
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
        let mut bad_row =
            make_pending_remint_row(10, &Pubkey::new_unique(), &recipient, &sig, deadline);
        bad_row.mint = "not-a-valid-pubkey".to_string();

        // Row 2: valid — must still be recovered despite the bad row above.
        let good_mint = Pubkey::new_unique();
        let good_row = make_pending_remint_row(11, &good_mint, &recipient, &sig, deadline);

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

    /// A corrupted recipient pubkey cannot be parsed into a `Pubkey`, so the
    /// operator cannot compute the user's ATA and has no valid destination
    /// for the remint.
    ///
    /// Same escalation rule as invalid mint: ManualReview immediately, do not
    /// skip silently, do not block other rows.
    #[tokio::test]
    async fn recover_pending_remints_escalates_invalid_recipient_to_manual_review() {
        let mock = MockStorage::new();
        let mint = Pubkey::new_unique();
        let sig = Signature::new_unique();
        let deadline = Utc::now() + chrono::Duration::seconds(20);

        let mut bad_row = make_pending_remint_row(20, &mint, &Pubkey::new_unique(), &sig, deadline);
        bad_row.recipient = "not-a-valid-pubkey".to_string();

        mock.pending_remint_transactions
            .lock()
            .unwrap()
            .push(bad_row);

        let mut state = make_sender_state(mock);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        state.recover_pending_remints(&storage_tx).await.unwrap();

        let update = storage_rx
            .try_recv()
            .expect("should receive ManualReview for bad recipient");
        assert_eq!(update.transaction_id, 20);
        assert_eq!(update.status, TransactionStatus::ManualReview);
        let err = update.error_message.as_deref().unwrap_or("");
        assert!(
            err.contains("invalid user pubkey"),
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

    /// An unparseable withdrawal signature in a PendingRemint row breaks the
    /// finality check: the operator cannot call `get_signature_statuses` with
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

        let mut bad_row =
            make_pending_remint_row(40, &mint, &recipient, &Signature::new_unique(), deadline);
        // Replace the valid signature with garbage.
        bad_row.remint_signatures = Some(vec!["not-a-valid-signature".to_string()]);

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
            err.contains("invalid withdrawal signature"),
            "error message should describe the signature parse failure: {err}"
        );

        assert!(
            state.pending_remints.is_empty(),
            "row with invalid signature must not be queued"
        );
        assert!(storage_rx.try_recv().is_err());
    }

    /// A PendingRemint row whose `remint_signatures` and
    /// `remint_last_valid_block_heights` arrays have different lengths cannot
    /// be turned into a coherent `Vec<PendingSig>`. Index-pairing would be
    /// undefined, so the remint gate cannot reliably check liveness.
    ///
    /// Escalate to ManualReview rather than guessing which sig got which lvbh.
    #[tokio::test]
    async fn recover_pending_remints_escalates_lvbh_length_mismatch_to_manual_review() {
        let mock = MockStorage::new();
        let mint = Pubkey::new_unique();
        let recipient = Pubkey::new_unique();
        let deadline = Utc::now() + chrono::Duration::seconds(20);

        let mut bad_row =
            make_pending_remint_row(50, &mint, &recipient, &Signature::new_unique(), deadline);
        bad_row.remint_signatures = Some(vec![
            Signature::new_unique().to_string(),
            Signature::new_unique().to_string(),
        ]);
        bad_row.remint_last_valid_block_heights = Some(vec![100]);

        mock.pending_remint_transactions
            .lock()
            .unwrap()
            .push(bad_row);

        let mut state = make_sender_state(mock);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        state.recover_pending_remints(&storage_tx).await.unwrap();

        let update = storage_rx
            .try_recv()
            .expect("should receive ManualReview for length mismatch");
        assert_eq!(update.transaction_id, 50);
        assert_eq!(update.status, TransactionStatus::ManualReview);
        let err = update.error_message.as_deref().unwrap_or("");
        assert!(
            err.contains("lvbh length"),
            "error message should describe the length mismatch: {err}"
        );

        assert!(
            state.pending_remints.is_empty(),
            "row with mismatched array lengths must not be queued"
        );
        assert!(storage_rx.try_recv().is_err());
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

        mock.pending_remint_transactions
            .lock()
            .unwrap()
            .push(make_pending_remint_row(
                50,
                &mint,
                &recipient,
                &sig,
                past_deadline,
            ));

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
            &recipient,
            &sig,
            Utc::now() + chrono::Duration::seconds(30),
        );
        row.pending_remint_deadline_at = None; // simulate missing deadline

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

    /// The deferred remint queue is a Withdraw-only responsibility. An Escrow
    /// sender sharing the transactions DB must never claim a PendingRemint row:
    /// it would classify the release signature on the wrong chain and could
    /// flip the row to ManualReview, stranding it from the real Withdraw sender.
    #[tokio::test]
    async fn recover_pending_remints_noop_for_escrow_role() {
        let mock = MockStorage::new();
        let mint = Pubkey::new_unique();
        let recipient = Pubkey::new_unique();
        let sig = Signature::new_unique();
        let deadline = Utc::now() - chrono::Duration::seconds(10);

        // A matured PendingRemint withdrawal row is present in the shared DB.
        mock.pending_remint_transactions
            .lock()
            .unwrap()
            .push(make_pending_remint_row(
                70, &mint, &recipient, &sig, deadline,
            ));

        let mut state = make_sender_state_with_role(mock, crate::config::ProgramType::Escrow);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        state.recover_pending_remints(&storage_tx).await.unwrap();

        // The Escrow sender must not hydrate the row into its queue.
        assert!(
            state.pending_remints.is_empty(),
            "Escrow must not claim Withdraw remint rows"
        );

        // No status update emitted, especially no ManualReview: the row is left
        // untouched for the real Withdraw sender.
        assert!(
            storage_rx.try_recv().is_err(),
            "Escrow must not emit any status update for a remint row"
        );
    }

    // ── rearm_owed_rotation ──────────────────────────────────────────

    static INIT_TEST_SIGNER: std::sync::Once = std::sync::Once::new();

    /// Configure an in-memory admin signer so the rotation builder's account wiring can
    /// resolve the process-global signers. Must run before their first access.
    fn ensure_test_signer() {
        INIT_TEST_SIGNER.call_once(|| {
            let keypair = solana_sdk::signer::keypair::Keypair::new();
            let encoded = bs58::encode(keypair.to_bytes()).into_string();
            std::env::set_var("ADMIN_SIGNER", "memory");
            std::env::set_var("ADMIN_PRIVATE_KEY", &encoded);
        });
    }

    /// The finding's restart hole: a reset carries no DB row and no nonce, so a crash
    /// between arming and confirmation dropped the only automatic rotation for the
    /// boundary. The stored target is what puts it back.
    #[tokio::test]
    async fn rearm_owed_rotation_arms_from_stored_target() {
        ensure_test_signer();
        let target_tree_index = 3u64;

        let mock = MockStorage::new();
        let mut state = make_sender_state(mock);
        state.instance_pda = Some(Pubkey::new_unique());
        state
            .storage
            .set_owed_rotation_target(state.program_type.as_label(), target_tree_index)
            .await
            .unwrap();

        state.rearm_owed_rotation().await.unwrap();

        let rotation = state
            .pending_rotation
            .as_ref()
            .expect("a stored target must re-arm the rotation");
        assert_eq!(rotation.target_tree_index, target_tree_index);

        // The re-armed builder must be a complete reset, not just a carrier for the
        // target: the sender wires these accounts itself, from globals and the instance,
        // so a wrong or missing one would only surface on-chain. Bind the generation the
        // way the submit path does, which is the only field left unset here.
        let operator_pubkey = SignerUtil::get_operator_pubkey();
        let mut builder = rotation.builder.clone();
        builder.expected_current_tree_index(target_tree_index - 1);
        let accounts: Vec<Pubkey> = builder
            .instruction()
            .accounts
            .iter()
            .map(|account| account.pubkey)
            .collect();
        let instance_pda = state.instance_pda.unwrap();
        for expected in [
            SignerUtil::get_admin_pubkey(),
            operator_pubkey,
            instance_pda,
            find_operator_pda(&instance_pda, &operator_pubkey),
            find_event_authority_pda(),
        ] {
            assert!(
                accounts.contains(&expected),
                "re-armed reset is missing account {expected}"
            );
        }
    }

    #[tokio::test]
    async fn rearm_owed_rotation_noop_without_stored_target() {
        ensure_test_signer();
        let mock = MockStorage::new();
        let mut state = make_sender_state(mock);
        state.instance_pda = Some(Pubkey::new_unique());

        state.rearm_owed_rotation().await.unwrap();

        assert!(
            state.pending_rotation.is_none(),
            "nothing owed means nothing armed"
        );
    }

    /// Both roles can share a database, so the escrow sender must not pick up the
    /// withdraw operator's owed rotation. It has no instance to reset, so it must not
    /// even read the target.
    #[tokio::test]
    async fn rearm_owed_rotation_noop_for_escrow_role() {
        let mock = MockStorage::new();
        let mut state = make_sender_state_with_role(mock, crate::config::ProgramType::Escrow);
        state
            .storage
            .set_owed_rotation_target("withdraw", 3)
            .await
            .unwrap();

        state.rearm_owed_rotation().await.unwrap();

        assert!(
            state.pending_rotation.is_none(),
            "Escrow must not claim the withdraw operator's rotation"
        );
        let Storage::Mock(mock) = state.storage.as_ref() else {
            unreachable!("mock storage")
        };
        assert_eq!(
            mock.calls("get_owed_rotation_target"),
            0,
            "Escrow must not even read the owed target"
        );
    }

    // ── SenderState construction tests ───────────────────────────────

    use crate::config::{PostgresConfig, ProgramType, StorageType};
    use crate::operator::utils::rpc_util::{RetryConfig, RpcClientWithRetry};
    use std::sync::Arc;

    fn make_sender_state_with_pda(pda: Option<Pubkey>) -> SenderState {
        let mock = MockStorage::new();
        let storage = Arc::new(Storage::Mock(mock));
        let rpc_client = Arc::new(RpcClientWithRetry::with_retry_config(
            "http://localhost:8899".to_string(),
            RetryConfig::default(),
            CommitmentConfig {
                commitment: CommitmentLevel::Confirmed,
            },
        ));
        SenderState {
            rpc_client: rpc_client.clone(),
            source_rpc_client: rpc_client.clone(),
            fallback_rpc_client: None,
            storage: storage.clone(),
            instance_pda: pda,
            smt_state: None,
            retry_counts: HashMap::new(),
            mint_builders: HashMap::new(),
            mint_cache: MintCache::new(storage),
            retry_max_attempts: 3,
            confirmation_poll_interval_ms: 400,
            rotation_retry_queue: Vec::new(),
            ambiguous_retry_queue: Vec::new(),
            pending_rotation: None,
            program_type: ProgramType::Escrow,
            remint_cache: HashMap::new(),
            pending_signatures: HashMap::new(),
            release_leases: HashMap::new(),
            pending_remints: Vec::new(),
            in_flight: InFlightQueue::new(),
            semaphore: Arc::new(Semaphore::new(MAX_IN_FLIGHT)),
        }
    }

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

    /// `validate_smt_root` without a PDA must return `AccountError::InstanceNotFound` (as `OperatorError::Account`).
    #[tokio::test]
    async fn validate_smt_root_fails_without_instance_pda() {
        let state = make_sender_state_with_pda(None);

        let result = super::validate_smt_root(&state.storage, &state.rpc_client, None).await;
        let err = result.unwrap_err();
        assert!(
            matches!(
                err,
                OperatorError::Account(crate::error::AccountError::InstanceNotFound { .. })
            ),
            "expected OperatorError::Account(InstanceNotFound), got: {err}"
        );
    }

    /// `SenderState::new` with no instance PDA and Escrow program type must succeed and leave
    /// SMT state uninitialised (it is lazily loaded on first use).
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
        );

        assert!(result.is_ok());
        let state = result.unwrap();
        assert!(state.instance_pda.is_none());
        assert!(state.smt_state.is_none());
        assert_eq!(state.retry_max_attempts, 3);
        assert_eq!(state.program_type, ProgramType::Escrow);
    }

    /// Providing an instance PDA and a higher retry limit must be reflected in the constructed
    /// state; the PDA is stored as-is for later SMT initialisation.
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
        );

        assert!(result.is_ok());
        let state = result.unwrap();
        assert_eq!(state.instance_pda, Some(instance_pda));
        assert_eq!(state.retry_max_attempts, 5);
    }

    /// An empty fallback URL (how env renders an unconfigured value) must
    /// build no client, so the destination oracle stays single-endpoint.
    #[test]
    fn empty_fallback_url_builds_no_client() {
        let mock = MockStorage::new();
        let storage = Arc::new(Storage::Mock(mock));
        let mut config = make_config();
        config.fallback_rpc_url = Some(String::new());

        let state = SenderState::new(
            &config,
            CommitmentLevel::Confirmed,
            None,
            storage,
            3,
            400,
            None,
        )
        .expect("construction must succeed with an empty fallback URL");

        assert!(
            state.fallback_rpc_client.is_none(),
            "empty fallback URL must not build a client"
        );
        assert!(
            state.dest_finality().fallback.is_none(),
            "empty fallback must leave the destination single-endpoint"
        );
    }

    /// A non-empty fallback URL builds a client, so the destination oracle
    /// carries a corroborating endpoint.
    #[test]
    fn set_fallback_url_builds_client() {
        let mock = MockStorage::new();
        let storage = Arc::new(Storage::Mock(mock));
        let mut config = make_config();
        config.fallback_rpc_url = Some("http://localhost:9999".to_string());

        let state = SenderState::new(
            &config,
            CommitmentLevel::Confirmed,
            None,
            storage,
            3,
            400,
            None,
        )
        .expect("construction must succeed with a set fallback URL");

        assert!(state.fallback_rpc_client.is_some());
        assert!(state.dest_finality().fallback.is_some());
        // Source stays single-endpoint regardless of the fallback.
        assert!(state.source_finality().fallback.is_none());
    }

    /// The chain tag drives the retention window and the metric label, so it must
    /// follow the role and not the field name: the two roles use `rpc_client` and
    /// `source_rpc_client` for mirror-image chains.
    #[test]
    fn finality_chain_tags_follow_the_operator_role() {
        use crate::operator::sender::remint::Chain;

        let withdraw = make_sender_state_with_role(MockStorage::new(), ProgramType::Withdraw);
        assert_eq!(withdraw.dest_finality().chain, Chain::Solana);
        assert_eq!(withdraw.source_finality().chain, Chain::Channel);

        let escrow = make_sender_state_with_role(MockStorage::new(), ProgramType::Escrow);
        assert_eq!(escrow.dest_finality().chain, Chain::Channel);
        assert_eq!(escrow.source_finality().chain, Chain::Solana);
    }

    /// Pins the SmtRootMismatch wedge: a landed release whose nonce never reaches
    /// `Completed` leaves the DB one nonce behind the chain, so `validate_smt_root`
    /// MUST diverge and return `Err(SmtRootMismatch)`. A change that silently
    /// absorbs it breaks here.
    #[tokio::test]
    async fn validate_smt_root_halts_on_consumed_but_unrecorded_nonce() {
        let landed_nonce: u64 = 1;
        let tree_index: u64 = 0;

        // On-chain root = root of an SMT that DOES include the landed nonce.
        let mut onchain_tree = SmtState::new(tree_index);
        onchain_tree.insert_nonce(landed_nonce);
        let onchain_root = onchain_tree.current_root();

        // Craft the Instance account the operator will fetch on boot, carrying
        // the advanced on-chain root.
        let instance = Instance {
            discriminator: 0,
            bump: 0,
            version: 0,
            instance_seed: Pubkey::new_unique(),
            admin: Pubkey::new_unique(),
            withdrawal_transactions_root: onchain_root,
            current_tree_index: tree_index,
        };
        let mut instance_bytes = Vec::new();
        instance.serialize(&mut instance_bytes).unwrap();

        // Mock getAccountInfo to return that crafted Instance account.
        let account_response = serde_json::json!({
            "context": {"slot": 1},
            "value": {
                "owner": Pubkey::new_unique().to_string(),
                "lamports": 1_000_000u64,
                "data": [STANDARD.encode(&instance_bytes), "base64"],
                "executable": false,
                "rentEpoch": 0
            }
        });
        let mut mocks = std::collections::HashMap::new();
        mocks.insert(RpcRequest::GetAccountInfo, account_response);
        let mock_rpc = RpcClientWithRetry {
            rpc_client: Arc::new(
                solana_client::nonblocking::rpc_client::RpcClient::new_mock_with_mocks(
                    "http://127.0.0.1:8899".to_string(),
                    mocks,
                ),
            ),
            retry_config: RetryConfig::default(),
        };

        // DB returns NO completed nonces — the landed nonce was never recorded.
        // This is the divergence: chain has the nonce, DB does not.
        let mut state = make_sender_state_with_pda(Some(Pubkey::new_unique()));
        state.rpc_client = Arc::new(mock_rpc);

        let err = super::validate_smt_root(&state.storage, &state.rpc_client, state.instance_pda)
            .await
            .unwrap_err();

        match err {
            OperatorError::Program(crate::error::ProgramError::SmtRootMismatch {
                local_root,
                onchain_root: reported_onchain,
            }) => {
                // The local (DB-derived, empty) root must differ from the
                // advanced on-chain root, and the reported on-chain root must
                // be the one carrying the consumed nonce.
                assert_ne!(
                    local_root, reported_onchain,
                    "mismatch must show diverging roots"
                );
                assert_eq!(
                    reported_onchain, onchain_root,
                    "on-chain root must be the one that included the landed nonce"
                );
                assert_eq!(
                    local_root,
                    SmtState::new(tree_index).current_root(),
                    "local root must be the empty-tree root (nonce never recorded)"
                );
            }
            other => panic!("expected SmtRootMismatch, got: {other}"),
        }
    }

    // verify_release_landed (SMT confirmation gate)
    //
    // These run under the `test-tree` feature so MAX_TREE_LEAVES = 8 keeps the
    // tree windows small: tree 0 covers nonces [0,8), tree 1 covers [8,16).
    use super::{verify_release_landed, ReleaseVerdict};
    use solana_client::nonblocking::rpc_client::RpcClient;

    /// Borsh-serialize an Instance carrying `root` and `tree_index`.
    fn instance_bytes(root: [u8; 32], tree_index: u64) -> Vec<u8> {
        let instance = Instance {
            discriminator: 0,
            bump: 0,
            version: 0,
            instance_seed: Pubkey::new_unique(),
            admin: Pubkey::new_unique(),
            withdrawal_transactions_root: root,
            current_tree_index: tree_index,
        };
        let mut bytes = Vec::new();
        instance.serialize(&mut bytes).unwrap();
        bytes
    }

    /// A finalized getLatestBlockhash Response whose derived tip height equals
    /// `tip_height`. The verifier computes tip = last_valid_block_height -
    /// MAX_PROCESSING_AGE, so we set the height to tip + MAX_PROCESSING_AGE. The
    /// context slot (used as the account read's min_context_slot) is set to the
    /// tip height too; the mock does not enforce it, so any value serves.
    fn blockhash_context_response(tip_height: u64) -> serde_json::Value {
        serde_json::json!({
            "context": {"slot": tip_height},
            "value": {
                "blockhash": "11111111111111111111111111111111",
                "lastValidBlockHeight": tip_height + MAX_PROCESSING_AGE as u64,
            }
        })
    }

    /// Mock RPC whose finalized getLatestBlockhash yields a tip height of
    /// `tip_height` and whose getAccountInfo returns an Instance account with the
    /// given root and tree_index. The verifier reads the blockhash first for
    /// freshness, then binds the account read to it, so both mocks are required.
    fn mock_instance_rpc(root: [u8; 32], tree_index: u64, tip_height: u64) -> RpcClientWithRetry {
        let bytes = instance_bytes(root, tree_index);
        let account_response = serde_json::json!({
            "context": {"slot": tip_height},
            "value": {
                "owner": Pubkey::new_unique().to_string(),
                "lamports": 1_000_000u64,
                "data": [STANDARD.encode(&bytes), "base64"],
                "executable": false,
                "rentEpoch": 0
            }
        });
        let mut mocks = HashMap::new();
        mocks.insert(RpcRequest::GetAccountInfo, account_response);
        mocks.insert(
            RpcRequest::GetLatestBlockhash,
            blockhash_context_response(tip_height),
        );
        RpcClientWithRetry {
            rpc_client: Arc::new(RpcClient::new_mock_with_mocks(
                "http://127.0.0.1:8899".to_string(),
                mocks,
            )),
            retry_config: RetryConfig::default(),
        }
    }

    /// Mock RPC whose getAccountInfo returns a null value (account absent), with a
    /// fresh finalized getLatestBlockhash so the gate reaches the absent-account check.
    fn mock_absent_instance_rpc(tip_height: u64) -> RpcClientWithRetry {
        let account_response = serde_json::json!({"context": {"slot": tip_height}, "value": null});
        let mut mocks = HashMap::new();
        mocks.insert(RpcRequest::GetAccountInfo, account_response);
        mocks.insert(
            RpcRequest::GetLatestBlockhash,
            blockhash_context_response(tip_height),
        );
        RpcClientWithRetry {
            rpc_client: Arc::new(RpcClient::new_mock_with_mocks(
                "http://127.0.0.1:8899".to_string(),
                mocks,
            )),
            retry_config: RetryConfig::default(),
        }
    }

    /// A completed withdrawal row so `get_completed_withdrawal_nonces` sees `nonce`.
    fn completed_withdrawal_row(id: i64, nonce: u64) -> DbTransaction {
        let now = Utc::now();
        DbTransaction {
            id,
            signature: Signature::new_unique().to_string(),
            trace_id: format!("trace-{id}"),
            slot: 100,
            initiator: Pubkey::new_unique().to_string(),
            recipient: Pubkey::new_unique().to_string(),
            mint: Pubkey::new_unique().to_string(),
            amount: TokenAmount(5_000),
            memo: None,
            transaction_type: TransactionType::Withdrawal,
            withdrawal_nonce: Some(nonce as i64),
            status: TransactionStatus::Completed,
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
        }
    }

    /// Build a mock storage seeded with the given completed nonces.
    fn storage_with_completed(nonces: &[u64]) -> Arc<Storage> {
        let mock = MockStorage::new();
        for (i, n) in nonces.iter().enumerate() {
            mock.pending_transactions
                .lock()
                .unwrap()
                .push(completed_withdrawal_row(i as i64 + 1, *n));
        }
        Arc::new(Storage::Mock(mock))
    }

    /// Root of a fresh tree_index-0 tree containing `nonces`.
    fn tree_root(tree_index: u64, nonces: &[u64]) -> [u8; 32] {
        let mut smt = SmtState::new(tree_index);
        for n in nonces {
            smt.insert_nonce(*n);
        }
        smt.current_root()
    }

    /// V1: on-chain root includes N which the DB has not recorded, fresh height, yields Landed.
    #[tokio::test]
    async fn verify_release_landed_v1_with_candidate_match() {
        let storage = storage_with_completed(&[1]);
        let onchain = tree_root(0, &[1, 3]);
        // Finalized height 100 > max_lvbh 50, so the freshness check passes.
        let rpc = mock_instance_rpc(onchain, 0, 100);
        let verdict =
            verify_release_landed(&rpc, &storage, Some(Pubkey::new_unique()), 3, 50).await;
        assert!(matches!(verdict, ReleaseVerdict::Landed), "expected Landed");
    }

    /// V2: on-chain root equals the completed set without N, fresh height, yields NotLanded.
    #[tokio::test]
    async fn verify_release_landed_v2_without_candidate_match() {
        let storage = storage_with_completed(&[1]);
        let onchain = tree_root(0, &[1]);
        let rpc = mock_instance_rpc(onchain, 0, 100);
        let verdict =
            verify_release_landed(&rpc, &storage, Some(Pubkey::new_unique()), 3, 50).await;
        assert!(
            matches!(verdict, ReleaseVerdict::NotLanded),
            "expected NotLanded"
        );
    }

    /// V3: nonce belongs to a different tree window than on-chain, yields Uncertain
    /// (tree-window check).
    #[tokio::test]
    async fn verify_release_landed_v3_wrong_tree_window() {
        let storage = storage_with_completed(&[]);
        // nonce 3 maps to tree 0, but the on-chain instance is on tree 1.
        let onchain = tree_root(1, &[]);
        let rpc = mock_instance_rpc(onchain, 1, 100);
        let verdict =
            verify_release_landed(&rpc, &storage, Some(Pubkey::new_unique()), 3, 50).await;
        assert!(
            matches!(verdict, ReleaseVerdict::Uncertain(_)),
            "tree-window check must fail closed"
        );
    }

    /// V4: root would say NotLanded but the finalized height is not past max_lvbh,
    /// yields Uncertain (freshness check).
    #[tokio::test]
    async fn verify_release_landed_v4_stale_snapshot() {
        let storage = storage_with_completed(&[1]);
        let onchain = tree_root(0, &[1]);
        // Finalized height 50 == max_lvbh 50, so height <= max_lvbh fails the gate.
        let rpc = mock_instance_rpc(onchain, 0, 50);
        let verdict =
            verify_release_landed(&rpc, &storage, Some(Pubkey::new_unique()), 3, 50).await;
        assert!(
            matches!(verdict, ReleaseVerdict::Uncertain(_)),
            "freshness check must fail closed on a stale snapshot"
        );
    }

    /// V4b regression: a large account context slot that WOULD have passed the old
    /// slot-based check is still Uncertain when the finalized tip height is not past
    /// max_lvbh, proving the old slot-vs-height confusion is closed. Freshness now
    /// comes from the blockhash tip height, so the account slot cannot paper over it.
    #[tokio::test]
    async fn verify_release_landed_v4b_large_slot_stale_height_uncertain() {
        let storage = storage_with_completed(&[1]);
        let onchain = tree_root(0, &[1]);
        // Account context slot is huge (would pass a slot > lvbh test), but the
        // finalized tip height 100 == max_lvbh 100 is not strictly past it.
        let bytes = instance_bytes(onchain, 0);
        let account_response = serde_json::json!({
            "context": {"slot": 20_000_000u64},
            "value": {
                "owner": Pubkey::new_unique().to_string(),
                "lamports": 1_000_000u64,
                "data": [STANDARD.encode(&bytes), "base64"],
                "executable": false,
                "rentEpoch": 0
            }
        });
        let mut mocks = HashMap::new();
        mocks.insert(RpcRequest::GetAccountInfo, account_response);
        mocks.insert(
            RpcRequest::GetLatestBlockhash,
            blockhash_context_response(100),
        );
        let rpc = RpcClientWithRetry {
            rpc_client: Arc::new(RpcClient::new_mock_with_mocks(
                "http://127.0.0.1:8899".to_string(),
                mocks,
            )),
            retry_config: RetryConfig::default(),
        };
        let verdict =
            verify_release_landed(&rpc, &storage, Some(Pubkey::new_unique()), 3, 100).await;
        assert!(
            matches!(verdict, ReleaseVerdict::Uncertain(_)),
            "a large slot must not paper over a stale finalized tip height"
        );
    }

    /// V5: on-chain root reflects a nonce beyond completed set plus or minus N,
    /// yields Uncertain (matches-neither).
    #[tokio::test]
    async fn verify_release_landed_v5_matches_neither() {
        let storage = storage_with_completed(&[1]);
        let onchain = tree_root(0, &[1, 3, 5]);
        let rpc = mock_instance_rpc(onchain, 0, 100);
        let verdict =
            verify_release_landed(&rpc, &storage, Some(Pubkey::new_unique()), 3, 50).await;
        assert!(
            matches!(verdict, ReleaseVerdict::Uncertain(_)),
            "matches-neither must be Uncertain"
        );
    }

    /// V6: getAccountInfo value null yields Uncertain (absent is not NotLanded).
    #[tokio::test]
    async fn verify_release_landed_v6_absent_instance() {
        let storage = storage_with_completed(&[1]);
        let rpc = mock_absent_instance_rpc(100);
        let verdict =
            verify_release_landed(&rpc, &storage, Some(Pubkey::new_unique()), 3, 50).await;
        assert!(
            matches!(verdict, ReleaseVerdict::Uncertain(_)),
            "absent instance must be Uncertain"
        );
    }

    /// V7: getAccountInfo RPC error yields Uncertain (read error is not NotLanded).
    #[tokio::test]
    async fn verify_release_landed_v7_rpc_error() {
        let storage = storage_with_completed(&[1]);
        // Unreachable endpoint, single attempt, so the read fails fast.
        let rpc = RpcClientWithRetry::with_retry_config(
            "http://127.0.0.1:1".to_string(),
            RetryConfig {
                max_attempts: 1,
                base_delay: std::time::Duration::from_millis(1),
                max_delay: std::time::Duration::from_millis(1),
            },
            CommitmentConfig::confirmed(),
        );
        let verdict =
            verify_release_landed(&rpc, &storage, Some(Pubkey::new_unique()), 3, 50).await;
        assert!(
            matches!(verdict, ReleaseVerdict::Uncertain(_)),
            "RPC error must be Uncertain"
        );
    }

    /// V8: DB already records N as Completed and on-chain includes it, yields Landed
    /// via the remove_nonce base path.
    #[tokio::test]
    async fn verify_release_landed_v8_db_has_candidate() {
        let storage = storage_with_completed(&[1, 3]);
        let onchain = tree_root(0, &[1, 3]);
        let rpc = mock_instance_rpc(onchain, 0, 100);
        let verdict =
            verify_release_landed(&rpc, &storage, Some(Pubkey::new_unique()), 3, 50).await;
        assert!(
            matches!(verdict, ReleaseVerdict::Landed),
            "remove_nonce base path must still resolve Landed"
        );
    }

    /// Bind regression (the double-pay vector). The finalized tip height is fresh,
    /// but the account backend is behind and cannot answer at the bound context
    /// slot, so the min_context_slot read errors. The verifier must return Uncertain,
    /// never a false NotLanded that would demote/remint a release that may have
    /// landed on a lagging, load-balanced RPC endpoint.
    #[tokio::test]
    async fn verify_release_landed_bind_account_behind_is_uncertain() {
        // The completed set without nonce 3 would look NotLanded IF the account
        // were served; the bind is what forces Uncertain instead.
        let storage = storage_with_completed(&[1]);

        let mut server = mockito::Server::new_async().await;
        // Fresh finalized blockhash: tip = 100150 - 150 = 100000, past max_lvbh 50.
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getLatestBlockhash""#.into(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{"jsonrpc":"2.0","result":{"context":{"slot":100000},"value":{"blockhash":"11111111111111111111111111111111","lastValidBlockHeight":100150}},"id":0}"#,
            )
            .create_async()
            .await;
        // The account backend is behind: it rejects the min_context_slot bind with
        // an RPC error rather than returning an older snapshot.
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getAccountInfo""#.into(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{"jsonrpc":"2.0","error":{"code":-32016,"message":"Minimum context slot has not been reached"},"id":0}"#,
            )
            .create_async()
            .await;

        let rpc = RpcClientWithRetry::with_retry_config(
            server.url(),
            RetryConfig {
                max_attempts: 1,
                base_delay: std::time::Duration::from_millis(1),
                max_delay: std::time::Duration::from_millis(1),
            },
            CommitmentConfig::confirmed(),
        );
        let verdict =
            verify_release_landed(&rpc, &storage, Some(Pubkey::new_unique()), 3, 50).await;
        assert!(
            matches!(verdict, ReleaseVerdict::Uncertain(_)),
            "an account backend behind the freshness point must be Uncertain, never NotLanded"
        );
    }
}
