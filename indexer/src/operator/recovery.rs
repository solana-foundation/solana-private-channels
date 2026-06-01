//! Recovers rows stuck in `Processing` after an operator crash.

use crate::channel_utils::send_guaranteed;
use crate::config::ProgramType;
use crate::error::OperatorError;
use crate::metrics::OPERATOR_STALE_PROCESSING_RECOVERED;
use crate::operator::sender::find_existing_mint_signature;
use crate::operator::utils::instruction_util::{MintToBuilder, MintToBuilderWithTxnId};
use crate::operator::utils::mint_idempotency_memo;
use crate::operator::utils::rpc_util::RpcClientWithRetry;
use crate::operator::TransactionStatusUpdate;
use crate::storage::common::models::{DbTransaction, TransactionStatus, TransactionType};
use crate::storage::common::storage::Storage;
use chrono::{DateTime, Utc};
use solana_sdk::pubkey::Pubkey;
use spl_associated_token_account::get_associated_token_address_with_program_id;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

/// How often the recovery loop runs.
pub(crate) const RECOVERY_INTERVAL: Duration = Duration::from_secs(60);

/// Age cutoff for "stuck"; must exceed the sender's 30s drain + retries.
pub(crate) const STALE_THRESHOLD: Duration = Duration::from_secs(5 * 60);

/// Per-tick batch cap; leftovers are picked up next tick.
pub(crate) const RECOVERY_BATCH_LIMIT: i64 = 100;

/// Three-way outcome: uncertainty must NOT demote (double-mint risk).
enum DepositOutcome {
    Landed { signature: String },
    NotLanded,
    Ambiguous { reason: String },
}

/// Two-way outcome: on-chain SMT prevents duplicate release anyway.
enum WithdrawalAction {
    Demote,
    Quarantine { reason: String },
}

/// Unified action for the storage router.
enum RecoveryAction {
    Complete { signature: String },
    Demote,
    Quarantine { reason: String },
}

/// Recovery loop. First tick runs on boot (the prime crash-recovery moment).
pub async fn run_recovery_worker(
    storage: Arc<Storage>,
    rpc_client: Arc<RpcClientWithRetry>,
    admin_pubkey: Pubkey,
    program_type: ProgramType,
    storage_tx: mpsc::Sender<TransactionStatusUpdate>,
    cancellation_token: CancellationToken,
) -> Result<(), OperatorError> {
    info!("Starting recovery worker");
    let mut interval = tokio::time::interval(RECOVERY_INTERVAL);
    loop {
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                info!("Recovery worker received cancellation, exiting");
                break;
            }
            _ = interval.tick() => {
                if let Err(e) = recover_once(
                    &storage,
                    &rpc_client,
                    admin_pubkey,
                    program_type,
                    &storage_tx,
                    &cancellation_token,
                )
                .await
                {
                    // Per-row writes are independent; retry next tick.
                    warn!("Recovery tick failed: {}", e);
                }
            }
        }
    }
    Ok(())
}

async fn recover_once(
    storage: &Storage,
    rpc_client: &RpcClientWithRetry,
    admin_pubkey: Pubkey,
    program_type: ProgramType,
    storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
    cancellation_token: &CancellationToken,
) -> Result<(), OperatorError> {
    let stale = storage
        .get_stale_processing_transactions(STALE_THRESHOLD, RECOVERY_BATCH_LIMIT)
        .await?;

    if stale.is_empty() {
        return Ok(());
    }

    debug!(
        count = stale.len(),
        "Recovery sweep found stale Processing rows"
    );

    for row in stale {
        // Cooperate with shutdown between rows so long batches exit cleanly.
        if cancellation_token.is_cancelled() {
            info!("Recovery sweep cancelled; remaining rows deferred");
            return Ok(());
        }
        // Capture `updated_at` before the RPC so the write below CAS-checks it.
        let captured = row.updated_at;
        let action = decide_action(&row, rpc_client, admin_pubkey).await;
        route_outcome(storage, &row, captured, action, program_type, storage_tx).await;
    }
    Ok(())
}

async fn decide_action(
    row: &DbTransaction,
    rpc_client: &RpcClientWithRetry,
    admin_pubkey: Pubkey,
) -> RecoveryAction {
    match row.transaction_type {
        TransactionType::Deposit => {
            match check_deposit_idempotency(row, rpc_client, admin_pubkey).await {
                DepositOutcome::Landed { signature } => RecoveryAction::Complete { signature },
                DepositOutcome::NotLanded => RecoveryAction::Demote,
                DepositOutcome::Ambiguous { reason } => RecoveryAction::Quarantine { reason },
            }
        }
        TransactionType::Withdrawal => match check_withdrawal(row) {
            WithdrawalAction::Demote => RecoveryAction::Demote,
            WithdrawalAction::Quarantine { reason } => RecoveryAction::Quarantine { reason },
        },
    }
}

async fn check_deposit_idempotency(
    row: &DbTransaction,
    rpc_client: &RpcClientWithRetry,
    admin_pubkey: Pubkey,
) -> DepositOutcome {
    let builder = match reconstruct_mint_builder_for_lookup(row, admin_pubkey) {
        Ok(b) => b,
        Err(e) => {
            return DepositOutcome::Ambiguous {
                reason: format!("deposit idempotency: rebuild: {e}"),
            }
        }
    };
    match find_existing_mint_signature(rpc_client, &builder).await {
        Ok(Some(sig)) => DepositOutcome::Landed {
            signature: sig.to_string(),
        },
        Ok(None) => DepositOutcome::NotLanded,
        // Never demote on uncertainty — risks a double-mint on re-pickup.
        Err(e) => DepositOutcome::Ambiguous {
            reason: format!("deposit idempotency: {e}"),
        },
    }
}

fn check_withdrawal(row: &DbTransaction) -> WithdrawalAction {
    match row.withdrawal_nonce {
        Some(_) => WithdrawalAction::Demote,
        None => WithdrawalAction::Quarantine {
            reason: "withdrawal row missing nonce".to_string(),
        },
    }
}

/// Rebuild the processor's mint builder so idempotency lookup keys match.
fn reconstruct_mint_builder_for_lookup(
    row: &DbTransaction,
    admin_pubkey: Pubkey,
) -> Result<MintToBuilderWithTxnId, String> {
    let mint = Pubkey::from_str(&row.mint).map_err(|e| format!("invalid mint pubkey: {e}"))?;
    let recipient =
        Pubkey::from_str(&row.recipient).map_err(|e| format!("invalid recipient pubkey: {e}"))?;
    // PrivateChannel is SPL-Token-only today (mirror `mint_util.rs`).
    let token_program = spl_token::id();
    let recipient_ata =
        get_associated_token_address_with_program_id(&recipient, &mint, &token_program);
    let amount =
        u64::try_from(row.amount).map_err(|_| format!("negative amount: {}", row.amount))?;

    let mut builder = MintToBuilder::new();
    builder
        .mint(mint)
        .recipient(recipient)
        .recipient_ata(recipient_ata)
        .payer(admin_pubkey)
        .mint_authority(admin_pubkey)
        .token_program(token_program)
        .amount(amount)
        .idempotency_memo(mint_idempotency_memo(row.id));

    Ok(MintToBuilderWithTxnId {
        builder,
        txn_id: row.id,
        trace_id: row.trace_id.clone(),
    })
}

async fn route_outcome(
    storage: &Storage,
    row: &DbTransaction,
    captured_updated_at: DateTime<Utc>,
    action: RecoveryAction,
    program_type: ProgramType,
    storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
) {
    let pt_label = match program_type {
        ProgramType::Escrow => "escrow",
        ProgramType::Withdraw => "withdraw",
    };
    let type_label = match row.transaction_type {
        TransactionType::Deposit => "deposit",
        TransactionType::Withdrawal => "withdrawal",
    };

    match action {
        RecoveryAction::Complete { signature } => {
            match storage
                .try_complete_processing(row.id, captured_updated_at, Some(signature.clone()))
                .await
            {
                Ok(true) => {
                    info!(
                        transaction_id = row.id,
                        signature, "Recovery promoted stale Processing → Completed"
                    );
                    OPERATOR_STALE_PROCESSING_RECOVERED
                        .with_label_values(&[pt_label, "completed", type_label])
                        .inc();
                }
                Ok(false) => {
                    debug!(
                        id = row.id,
                        "recovery skipped — another writer touched the row first"
                    );
                }
                Err(e) => warn!(id = row.id, "recovery write error: {}", e),
            }
        }
        RecoveryAction::Demote => {
            // Trigger bumps `updated_at`; the next sweep skips it.
            match storage
                .try_requeue_processing(row.id, captured_updated_at)
                .await
            {
                Ok(true) => {
                    info!(
                        transaction_id = row.id,
                        "Recovery demoted stale Processing → Pending"
                    );
                    OPERATOR_STALE_PROCESSING_RECOVERED
                        .with_label_values(&[pt_label, "requeued", type_label])
                        .inc();
                }
                Ok(false) => {
                    debug!(
                        id = row.id,
                        "recovery skipped — another writer touched the row first"
                    );
                }
                Err(e) => warn!(id = row.id, "recovery write error: {}", e),
            }
        }
        RecoveryAction::Quarantine { reason } => {
            // Noisy by design — page on uncertainty, never silently demote.
            match storage
                .try_quarantine_processing(row.id, captured_updated_at)
                .await
            {
                Ok(true) => {
                    warn!(
                        transaction_id = row.id,
                        reason = %reason,
                        "Recovery quarantined stale Processing → ManualReview"
                    );
                    OPERATOR_STALE_PROCESSING_RECOVERED
                        .with_label_values(&[pt_label, "quarantined", type_label])
                        .inc();
                    // Fire the existing webhook + alert log (see sender/state.rs).
                    let update = TransactionStatusUpdate {
                        transaction_id: row.id,
                        trace_id: Some(row.trace_id.clone()),
                        status: TransactionStatus::ManualReview,
                        counterpart_signature: None,
                        processed_at: Some(Utc::now()),
                        error_message: Some(reason),
                        remint_signature: None,
                        remint_attempted: false,
                    };
                    // Closed channel = on-call alert lost; surface it loudly.
                    if let Err(e) =
                        send_guaranteed(storage_tx, update, "recovery manual review").await
                    {
                        warn!(
                            transaction_id = row.id,
                            "Recovery quarantined row but failed to deliver alert webhook: {}", e
                        );
                    }
                }
                Ok(false) => {
                    debug!(
                        id = row.id,
                        "recovery skipped — another writer touched the row first"
                    );
                }
                Err(e) => warn!(id = row.id, "recovery write error: {}", e),
            }
        }
    }
}

#[cfg(any(test, feature = "test-mock-storage"))]
pub mod test_hooks {
    //! Test-only entry to drive a single recovery tick deterministically.
    use super::*;

    pub async fn run_recovery_once(
        storage: &Storage,
        rpc_client: &RpcClientWithRetry,
        admin_pubkey: Pubkey,
        program_type: ProgramType,
        storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
    ) -> Result<(), OperatorError> {
        // Fresh, never-cancelled token; tests run to completion.
        let token = CancellationToken::new();
        recover_once(
            storage,
            rpc_client,
            admin_pubkey,
            program_type,
            storage_tx,
            &token,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::utils::rpc_util::RetryConfig;
    use crate::storage::common::storage::mock::MockStorage;
    use solana_sdk::commitment_config::CommitmentConfig;

    fn make_deposit_row(id: i64) -> DbTransaction {
        let now = Utc::now();
        DbTransaction {
            id,
            signature: format!("sig-{id}"),
            trace_id: format!("trace-{id}"),
            slot: 100,
            initiator: Pubkey::new_unique().to_string(),
            recipient: Pubkey::new_unique().to_string(),
            mint: Pubkey::new_unique().to_string(),
            amount: 1_000,
            memo: None,
            transaction_type: TransactionType::Deposit,
            withdrawal_nonce: None,
            status: TransactionStatus::Processing,
            created_at: now,
            updated_at: now,
            processed_at: None,
            counterpart_signature: None,
            remint_signatures: None,
            remint_last_valid_block_heights: None,
            pending_remint_deadline_at: None,
            finality_check_attempts: 0,
        }
    }

    fn make_withdrawal_row(id: i64, nonce: Option<i64>) -> DbTransaction {
        let mut row = make_deposit_row(id);
        row.transaction_type = TransactionType::Withdrawal;
        row.withdrawal_nonce = nonce;
        row
    }

    fn make_rpc_client(url: &str) -> RpcClientWithRetry {
        RpcClientWithRetry::with_retry_config(
            url.to_string(),
            RetryConfig {
                max_attempts: 1,
                base_delay: Duration::from_millis(1),
                max_delay: Duration::from_millis(1),
            },
            CommitmentConfig::confirmed(),
        )
    }

    // ── check_deposit_idempotency outcome matrix ─────────────────────

    #[tokio::test]
    async fn check_deposit_idempotency_landed_when_rpc_returns_match() {
        // Scripted mock with memo-matching mint; unit equivalent of the IT fixture.
        let mut server = mockito::Server::new_async().await;

        let row = make_deposit_row(7_777);
        let mint = Pubkey::from_str(&row.mint).unwrap();
        let recipient = Pubkey::from_str(&row.recipient).unwrap();
        let admin_pubkey = Pubkey::new_unique();
        let token_program = spl_token::id();
        let recipient_ata =
            get_associated_token_address_with_program_id(&recipient, &mint, &token_program);
        let memo = mint_idempotency_memo(row.id);
        let signature = solana_sdk::signature::Signature::new_unique();
        let memo_program_id = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr";

        let _sigs_mock = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "method": "getSignaturesForAddress"
            })))
            .with_status(200)
            .with_body(
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": [
                        {
                            "signature": signature.to_string(),
                            "slot": 100u64,
                            "err": serde_json::Value::Null,
                            "memo": format!("[{}] {}", memo.len(), memo),
                            "blockTime": 1_700_000_000i64,
                            "confirmationStatus": "finalized",
                        }
                    ]
                })
                .to_string(),
            )
            .create();

        let _tx_mock = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "method": "getTransaction"
            })))
            .with_status(200)
            .with_body(
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {
                        "slot": 100,
                        "blockTime": 1_700_000_000i64,
                        "meta": {
                            "err": null,
                            "status": { "Ok": null },
                            "fee": 5000u64,
                            "innerInstructions": [],
                            "preBalances": [1_000_000u64],
                            "postBalances": [999_995u64],
                            "logMessages": [],
                            "preTokenBalances": [],
                            "postTokenBalances": [],
                            "rewards": [],
                            "computeUnitsConsumed": 0u64,
                        },
                        "transaction": {
                            "signatures": [signature.to_string()],
                            "message": {
                                "accountKeys": [
                                    {"pubkey": admin_pubkey.to_string(), "signer": true, "writable": true, "source": "transaction"},
                                    {"pubkey": recipient_ata.to_string(), "signer": false, "writable": true, "source": "transaction"},
                                    {"pubkey": mint.to_string(), "signer": false, "writable": true, "source": "transaction"},
                                    {"pubkey": token_program.to_string(), "signer": false, "writable": false, "source": "transaction"},
                                    {"pubkey": memo_program_id, "signer": false, "writable": false, "source": "transaction"},
                                ],
                                "recentBlockhash": "GHtXQBsoZHjzkAm2Sdm6FTyFHBCqBnLanJJhZFCFJXoe",
                                "instructions": [
                                    {"program": "spl-memo", "programId": memo_program_id, "parsed": memo},
                                    {
                                        "program": "spl-token",
                                        "programId": token_program.to_string(),
                                        "parsed": {
                                            "type": "mintTo",
                                            "info": {
                                                "mint": mint.to_string(),
                                                "account": recipient_ata.to_string(),
                                                "mintAuthority": admin_pubkey.to_string(),
                                                "amount": (row.amount as u64).to_string(),
                                            },
                                        },
                                    },
                                ],
                            },
                        },
                    }
                })
                .to_string(),
            )
            .create();

        let client = make_rpc_client(&server.url());
        let outcome = check_deposit_idempotency(&row, &client, admin_pubkey).await;
        match outcome {
            DepositOutcome::Landed { signature: s } => {
                assert_eq!(s, signature.to_string());
            }
            _ => panic!("expected Landed"),
        }
    }

    #[tokio::test]
    async fn check_deposit_idempotency_not_landed_on_empty_signatures() {
        let mut server = mockito::Server::new_async().await;
        let _m = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "method": "getSignaturesForAddress"
            })))
            .with_status(200)
            .with_body(serde_json::json!({"jsonrpc":"2.0","id":1,"result":[]}).to_string())
            .create();
        let client = make_rpc_client(&server.url());
        let row = make_deposit_row(1);
        let admin_pubkey = Pubkey::new_unique();
        let outcome = check_deposit_idempotency(&row, &client, admin_pubkey).await;
        assert!(matches!(outcome, DepositOutcome::NotLanded));
    }

    #[tokio::test]
    async fn check_deposit_idempotency_ambiguous_on_transport_error() {
        let mut server = mockito::Server::new_async().await;
        // HTTP 500 → transport error → Err path → Ambiguous.
        let _m = server
            .mock("POST", "/")
            .with_status(500)
            .with_body("internal server error")
            .create();
        let client = make_rpc_client(&server.url());
        let row = make_deposit_row(1);
        let admin_pubkey = Pubkey::new_unique();
        let outcome = check_deposit_idempotency(&row, &client, admin_pubkey).await;
        match outcome {
            DepositOutcome::Ambiguous { reason } => {
                assert!(
                    reason.starts_with("deposit idempotency:"),
                    "reason: {reason}"
                );
            }
            _ => panic!("expected Ambiguous"),
        }
    }

    #[tokio::test]
    async fn check_deposit_idempotency_ambiguous_on_method_not_found() {
        let mut server = mockito::Server::new_async().await;
        // -32601 must NOT collapse to NotLanded → would demote + double-mint.
        let _m = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "method": "getSignaturesForAddress"
            })))
            .with_status(200)
            .with_body(
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "error": { "code": -32601, "message": "Method not found" }
                })
                .to_string(),
            )
            .create();
        let client = make_rpc_client(&server.url());
        let row = make_deposit_row(1);
        let admin_pubkey = Pubkey::new_unique();
        let outcome = check_deposit_idempotency(&row, &client, admin_pubkey).await;
        assert!(
            matches!(outcome, DepositOutcome::Ambiguous { .. }),
            "method-not-found must be Ambiguous, never NotLanded"
        );
    }

    // ── check_withdrawal outcome matrix ───────────────────────────────

    #[test]
    fn check_withdrawal_demotes_when_nonce_present() {
        let row = make_withdrawal_row(1, Some(42));
        let action = check_withdrawal(&row);
        assert!(matches!(action, WithdrawalAction::Demote));
    }

    #[test]
    fn check_withdrawal_quarantines_when_nonce_missing() {
        let row = make_withdrawal_row(1, None);
        let action = check_withdrawal(&row);
        match action {
            WithdrawalAction::Quarantine { reason } => {
                assert!(reason.contains("withdrawal row missing nonce"));
            }
            _ => panic!("expected Quarantine"),
        }
    }

    // ── route_outcome calls the right storage helper per variant ─────

    async fn seed_processing_row(mock: &MockStorage, row: DbTransaction) -> DateTime<Utc> {
        let captured = row.updated_at;
        mock.pending_transactions.lock().unwrap().push(row);
        captured
    }

    #[tokio::test]
    async fn route_outcome_complete_writes_completed() {
        let mock = MockStorage::new();
        let mut row = make_deposit_row(1);
        row.status = TransactionStatus::Processing;
        let captured = seed_processing_row(&mock, row.clone()).await;
        let storage = Storage::Mock(mock.clone());
        let (storage_tx, _rx) = mpsc::channel(8);

        route_outcome(
            &storage,
            &row,
            captured,
            RecoveryAction::Complete {
                signature: "sig-abc".to_string(),
            },
            ProgramType::Escrow,
            &storage_tx,
        )
        .await;

        let after = mock.pending_transactions.lock().unwrap();
        assert_eq!(after[0].status, TransactionStatus::Completed);
        assert_eq!(after[0].counterpart_signature.as_deref(), Some("sig-abc"));
    }

    #[tokio::test]
    async fn route_outcome_demote_writes_pending() {
        let mock = MockStorage::new();
        let mut row = make_deposit_row(2);
        row.status = TransactionStatus::Processing;
        let captured = seed_processing_row(&mock, row.clone()).await;
        let storage = Storage::Mock(mock.clone());
        let (storage_tx, _rx) = mpsc::channel(8);

        route_outcome(
            &storage,
            &row,
            captured,
            RecoveryAction::Demote,
            ProgramType::Escrow,
            &storage_tx,
        )
        .await;

        let after = mock.pending_transactions.lock().unwrap();
        assert_eq!(after[0].status, TransactionStatus::Pending);
    }

    #[tokio::test]
    async fn route_outcome_quarantine_writes_manual_review_and_sends_alert() {
        let mock = MockStorage::new();
        let mut row = make_withdrawal_row(3, None);
        row.status = TransactionStatus::Processing;
        let captured = seed_processing_row(&mock, row.clone()).await;
        let storage = Storage::Mock(mock.clone());
        let (storage_tx, mut storage_rx) = mpsc::channel(8);

        route_outcome(
            &storage,
            &row,
            captured,
            RecoveryAction::Quarantine {
                reason: "withdrawal row missing nonce".to_string(),
            },
            ProgramType::Withdraw,
            &storage_tx,
        )
        .await;

        let after = mock.pending_transactions.lock().unwrap();
        assert_eq!(after[0].status, TransactionStatus::ManualReview);
        drop(after);

        let update = storage_rx
            .try_recv()
            .expect("expected manual review update");
        assert_eq!(update.transaction_id, row.id);
        assert_eq!(update.status, TransactionStatus::ManualReview);
        assert_eq!(
            update.error_message.as_deref(),
            Some("withdrawal row missing nonce")
        );
    }

    #[tokio::test]
    async fn route_outcome_demote_noops_when_captured_updated_at_stale() {
        // The `updated_at` check fails → no metric increment, row unchanged.
        let mock = MockStorage::new();
        let mut row = make_deposit_row(4);
        row.status = TransactionStatus::Processing;
        mock.pending_transactions.lock().unwrap().push(row.clone());
        let storage = Storage::Mock(mock.clone());
        let (storage_tx, _rx) = mpsc::channel(8);

        // Captured timestamp that does NOT match the seeded row's updated_at.
        let stale = row.updated_at - chrono::Duration::seconds(60);
        route_outcome(
            &storage,
            &row,
            stale,
            RecoveryAction::Demote,
            ProgramType::Escrow,
            &storage_tx,
        )
        .await;

        let after = mock.pending_transactions.lock().unwrap();
        assert_eq!(after[0].status, TransactionStatus::Processing);
    }
}
