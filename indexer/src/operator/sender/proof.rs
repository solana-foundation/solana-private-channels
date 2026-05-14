use crate::error::{OperatorError, ProgramError};
use crate::operator::sender::mint;
use crate::operator::tree_constants::MAX_TREE_LEAVES;
use crate::operator::{ReleaseFundsBuilderWithNonce, SIBLING_PROOF_SIZE};
use private_channel_escrow_program_client::instructions::ResetSmtRootBuilder;
use solana_keychain::Signer;
use solana_sdk::pubkey::Pubkey;
use tracing::{error, info, warn};

#[cfg(test)]
use super::types::InFlightQueue;
use super::types::{InstructionWithSigners, SenderSMTState, SenderState, TransactionContext};

impl SenderSMTState {
    pub(super) fn handle_release_funds_transaction(
        &mut self,
        builder_with_nonce: Box<ReleaseFundsBuilderWithNonce>,
        fee_payer: Pubkey,
        signers: Vec<&'static Signer>,
        compute_unit_price: Option<u64>,
        compute_budget: Option<u32>,
    ) -> Result<InstructionWithSigners, OperatorError> {
        let nonce = builder_with_nonce.nonce;
        let transaction_id = builder_with_nonce.transaction_id;
        let trace_id = builder_with_nonce.trace_id;
        let mut builder = builder_with_nonce.builder;

        // Check if this nonce expects a different tree than current local tree
        let expected_tree_index = nonce / MAX_TREE_LEAVES as u64;
        let current_tree_index = self.smt_state.tree_index();

        if expected_tree_index != current_tree_index {
            info!(
                "Nonce {} expects tree_index {} but current is {} - will retry after rotation",
                nonce, expected_tree_index, current_tree_index
            );

            return Err(ProgramError::TreeIndexMismatch {
                nonce,
                expected_tree_index,
                current_tree_index,
            }
            .into());
        }

        // Store incomplete builder for potential retry
        let ctx = TransactionContext {
            transaction_id: Some(transaction_id),
            withdrawal_nonce: Some(nonce),
            trace_id: Some(trace_id),
        };
        self.nonce_to_builder.insert(nonce, (ctx, builder.clone()));

        // Check if nonce already exists
        if self.smt_state.contains_nonce(nonce) {
            return Err(ProgramError::InvalidProof {
                reason: format!("Nonce {} already exists in SMT", nonce),
            }
            .into());
        }

        // Generate exclusion proof BEFORE inserting nonce
        let exclusion_proof = self.smt_state.generate_exclusion_proof(nonce);

        // Insert nonce into SMT (updates tree state)
        if !self.smt_state.insert_nonce(nonce) {
            return Err(ProgramError::SmtProofFailed {
                reason: format!("Failed to insert nonce {} (already exists)", nonce),
            }
            .into());
        }

        // This will be used for inclusion proof
        let new_root = self.smt_state.current_root();

        let mut sibling_proofs_flat = [0u8; SIBLING_PROOF_SIZE];
        for (i, sibling) in exclusion_proof.iter().enumerate() {
            sibling_proofs_flat[i * 32..(i + 1) * 32].copy_from_slice(sibling);
        }

        builder
            .sibling_proofs(sibling_proofs_flat)
            .new_withdrawal_root(new_root);

        Ok(InstructionWithSigners {
            instructions: vec![builder.instruction()],
            fee_payer,
            signers,
            compute_budget,
            compute_unit_price,
        })
    }
}

/// Check if pending rotation can now be processed
/// Returns the ResetSmtRoot builder if ready to execute
pub fn take_pending_rotation_if_ready(state: &mut SenderState) -> Option<Box<ResetSmtRootBuilder>> {
    state.pending_rotation.as_ref()?;

    // Check if all in-flight transactions are settled
    let has_in_flight = if let Some(ref smt_state) = state.smt_state {
        !smt_state.nonce_to_builder.is_empty()
    } else {
        false
    };

    if !has_in_flight {
        info!("All in-flight transactions settled, rotation ready to execute");
        state.pending_rotation.take()
    } else {
        None
    }
}

/// Rebuild transaction with regenerated SMT proof and retry
pub(super) async fn rebuild_with_regenerated_proof(
    state: &mut SenderState,
    nonce: Option<u64>,
    instruction: InstructionWithSigners,
) -> Option<InstructionWithSigners> {
    error!("InvalidSmtProof detected - rebuilding with new proof");

    let Some(nonce) = nonce else {
        error!("InvalidSmtProof error but not a ReleaseFunds transaction");
        return None;
    };

    let Some(ref mut smt_state) = state.smt_state else {
        error!("No SMT state available");
        return None;
    };

    let Some((ctx, builder)) = smt_state.nonce_to_builder.get(&nonce).cloned() else {
        error!("No cached builder found for nonce {}", nonce);
        return None;
    };

    info!(
        "Rebuilding transaction with regenerated proof for nonce {}",
        nonce
    );

    let transaction_id = ctx
        .transaction_id
        .expect("rebuild must have transaction_id");
    let trace_id = ctx.trace_id.expect("rebuild must have trace_id");

    let remint_info = state.remint_cache.get(&nonce).cloned();
    if remint_info.is_none() {
        error!(
            "Missing remint_info for rebuild nonce {} - remint will not be possible on failure",
            nonce
        );
    }

    let builder_with_nonce = Box::new(ReleaseFundsBuilderWithNonce {
        builder,
        nonce,
        transaction_id,
        trace_id,
        remint_info,
    });

    match smt_state.handle_release_funds_transaction(
        builder_with_nonce,
        instruction.fee_payer,
        instruction.signers.clone(),
        instruction.compute_unit_price,
        instruction.compute_budget,
    ) {
        Ok(new_instruction) => {
            info!("Successfully rebuilt transaction with new proof");
            Some(new_instruction)
        }
        Err(e) => {
            error!("Failed to rebuild transaction: {}", e);
            None
        }
    }
}

/// Cleanup SMT state and caches when transaction fails
///
/// Removes the nonce from local SMT to keep it in sync with on-chain state.
/// Also clears builder cache, retry counts, and remint cache.
pub(super) fn cleanup_failed_transaction(state: &mut SenderState, nonce: Option<u64>) {
    if let (Some(nonce), Some(ref mut smt_state)) = (nonce, state.smt_state.as_mut()) {
        if smt_state.smt_state.remove_nonce(nonce) {
            warn!("Rolled back SMT state for failed nonce {}", nonce);
        }
        smt_state.nonce_to_builder.remove(&nonce);
        state.retry_counts.remove(&nonce);
    }
    if let Some(nonce) = nonce {
        // Note: when called from handle_permanent_failure, remint_cache is
        // already drained. This removal is defensive for any other call site.
        state.remint_cache.remove(&nonce);
    }

    mint::cleanup_mint_builder(state, nonce.map(|n| n as i64));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::utils::smt_util::SmtState;
    use crate::operator::MintCache;
    use crate::storage::common::storage::mock::MockStorage;
    use crate::storage::Storage;
    use private_channel_escrow_program_client::instructions::ReleaseFundsBuilder;
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::operator::sender::types::MAX_IN_FLIGHT;
    use crate::operator::utils::instruction_util::WithdrawalRemintInfo;
    use tokio::sync::Semaphore;

    /// Build a minimal SenderState for testing (no RPC needed)
    fn make_sender_state() -> SenderState {
        let mock = MockStorage::new();
        let storage = Arc::new(Storage::Mock(mock));
        let rpc = Arc::new(crate::operator::RpcClientWithRetry::with_retry_config(
            "http://localhost:8899".to_string(),
            crate::operator::RetryConfig::default(),
            solana_sdk::commitment_config::CommitmentConfig::confirmed(),
        ));
        SenderState {
            rpc_client: rpc.clone(),
            storage: storage.clone(),
            instance_pda: None,
            smt_state: None,
            retry_counts: HashMap::new(),
            mint_builders: HashMap::new(),
            mint_cache: MintCache::new(storage),
            retry_max_attempts: 3,
            confirmation_poll_interval_ms: 400,
            rotation_retry_queue: Vec::new(),
            pending_rotation: None,
            program_type: crate::config::ProgramType::Escrow,
            remint_cache: HashMap::new(),
            pending_signatures: HashMap::new(),
            pending_remints: Vec::new(),
            in_flight: InFlightQueue::new(),
            semaphore: Arc::new(Semaphore::new(MAX_IN_FLIGHT)),
        }
    }

    fn make_test_remint_info(transaction_id: i64, trace_id: &str) -> WithdrawalRemintInfo {
        WithdrawalRemintInfo {
            transaction_id,
            trace_id: trace_id.to_string(),
            mint: Pubkey::new_unique(),
            user: Pubkey::new_unique(),
            user_ata: Pubkey::new_unique(),
            token_program: spl_token::id(),
            amount: 1000,
        }
    }

    fn make_smt_state(tree_index: u64) -> SenderSMTState {
        SenderSMTState {
            smt_state: SmtState::new(tree_index),
            nonce_to_builder: HashMap::new(),
        }
    }

    fn make_release_funds_builder() -> ReleaseFundsBuilder {
        let mut b = ReleaseFundsBuilder::new();
        let pk = Pubkey::new_unique();
        b.payer(pk)
            .operator(pk)
            .instance(pk)
            .operator_pda(pk)
            .mint(pk)
            .allowed_mint(pk)
            .user_ata(pk)
            .instance_ata(pk)
            .token_program(spl_token::id())
            .user(pk)
            .amount(1000)
            .transaction_nonce(0);
        b
    }

    // ── take_pending_rotation_if_ready ────────────────────────────────

    #[test]
    fn rotation_returns_none_when_no_pending() {
        let mut state = make_sender_state();
        assert!(take_pending_rotation_if_ready(&mut state).is_none());
    }

    #[test]
    fn rotation_returns_builder_when_no_inflight() {
        let mut state = make_sender_state();
        state.pending_rotation = Some(Box::new(ResetSmtRootBuilder::new()));
        // No smt_state means no in-flight
        let result = take_pending_rotation_if_ready(&mut state);
        assert!(result.is_some());
        assert!(state.pending_rotation.is_none(), "should be taken");
    }

    #[test]
    fn rotation_blocked_by_inflight_transactions() {
        let mut state = make_sender_state();
        state.pending_rotation = Some(Box::new(ResetSmtRootBuilder::new()));

        // Add smt_state with an in-flight nonce
        let mut smt = make_smt_state(0);
        let ctx = TransactionContext {
            transaction_id: Some(1),
            withdrawal_nonce: Some(0),
            trace_id: Some("t".to_string()),
        };
        smt.nonce_to_builder
            .insert(0, (ctx, ReleaseFundsBuilder::new()));
        state.smt_state = Some(smt);

        assert!(take_pending_rotation_if_ready(&mut state).is_none());
        assert!(state.pending_rotation.is_some(), "should NOT be taken yet");
    }

    #[test]
    fn rotation_ready_after_inflight_cleared() {
        let mut state = make_sender_state();
        state.pending_rotation = Some(Box::new(ResetSmtRootBuilder::new()));
        state.smt_state = Some(make_smt_state(0)); // empty nonce_to_builder

        assert!(take_pending_rotation_if_ready(&mut state).is_some());
    }

    // ── cleanup_failed_transaction ───────────────────────────────────

    #[test]
    fn cleanup_removes_nonce_from_smt_and_caches() {
        let mut state = make_sender_state();
        let mut smt = make_smt_state(0);
        smt.smt_state.insert_nonce(5);
        let ctx = TransactionContext {
            transaction_id: Some(1),
            withdrawal_nonce: Some(5),
            trace_id: Some("t".to_string()),
        };
        smt.nonce_to_builder
            .insert(5, (ctx, ReleaseFundsBuilder::new()));
        state.smt_state = Some(smt);
        state.retry_counts.insert(5, 2);
        state.remint_cache.insert(5, make_test_remint_info(1, "t"));

        cleanup_failed_transaction(&mut state, Some(5));

        let smt = state.smt_state.as_ref().unwrap();
        assert!(!smt.smt_state.contains_nonce(5));
        assert!(!smt.nonce_to_builder.contains_key(&5));
        assert!(!state.retry_counts.contains_key(&5));
        assert!(!state.remint_cache.contains_key(&5));
    }

    #[test]
    fn cleanup_with_none_nonce_is_noop() {
        let mut state = make_sender_state();
        state.smt_state = Some(make_smt_state(0));
        cleanup_failed_transaction(&mut state, None);
        // Just verify it doesn't panic
    }

    #[test]
    fn cleanup_without_smt_state_is_noop() {
        let mut state = make_sender_state();
        cleanup_failed_transaction(&mut state, Some(5));
    }

    // ── handle_release_funds_transaction ──────────────────────────────

    #[test]
    fn handle_release_funds_tree_index_mismatch() {
        let mut smt = make_smt_state(0); // tree_index 0

        let builder = make_release_funds_builder();
        // Nonce in tree_index 1 range (>= MAX_TREE_LEAVES)
        let nonce = MAX_TREE_LEAVES as u64;
        let bwn = Box::new(ReleaseFundsBuilderWithNonce {
            builder,
            nonce,
            transaction_id: 1,
            trace_id: "t".to_string(),
            remint_info: Some(make_test_remint_info(1, "t")),
        });

        let result =
            smt.handle_release_funds_transaction(bwn, Pubkey::new_unique(), vec![], None, None);
        assert!(result.is_err());
        assert!(matches!(
            result,
            Err(OperatorError::Program(
                ProgramError::TreeIndexMismatch { .. }
            ))
        ));
    }

    #[test]
    fn handle_release_funds_duplicate_nonce_errors() {
        let mut smt = make_smt_state(0);
        smt.smt_state.insert_nonce(3); // pre-insert

        let builder = make_release_funds_builder();
        let bwn = Box::new(ReleaseFundsBuilderWithNonce {
            builder,
            nonce: 3,
            transaction_id: 1,
            trace_id: "t".to_string(),
            remint_info: Some(make_test_remint_info(1, "t")),
        });

        let result =
            smt.handle_release_funds_transaction(bwn, Pubkey::new_unique(), vec![], None, None);
        assert!(result.is_err());
    }

    #[test]
    fn handle_release_funds_success_inserts_nonce_and_caches_builder() {
        let mut smt = make_smt_state(0);
        let builder = make_release_funds_builder();
        let bwn = Box::new(ReleaseFundsBuilderWithNonce {
            builder,
            nonce: 0,
            transaction_id: 42,
            trace_id: "trace-42".to_string(),
            remint_info: Some(make_test_remint_info(42, "trace-42")),
        });

        let result = smt.handle_release_funds_transaction(
            bwn,
            Pubkey::new_unique(),
            vec![],
            Some(5000),
            Some(200_000),
        );
        assert!(result.is_ok());

        // Nonce inserted into SMT
        assert!(smt.smt_state.contains_nonce(0));
        // Builder cached for potential retry
        assert!(smt.nonce_to_builder.contains_key(&0));

        let ix = result.unwrap();
        assert_eq!(ix.instructions.len(), 1);
        assert_eq!(ix.compute_unit_price, Some(5000));
        assert_eq!(ix.compute_budget, Some(200_000));
    }

    #[test]
    fn handle_release_funds_multiple_nonces_produces_different_roots() {
        let mut smt = make_smt_state(0);

        let build_and_insert = |smt: &mut SenderSMTState, nonce: u64| -> [u8; 32] {
            let mut builder = make_release_funds_builder();
            builder.transaction_nonce(nonce);
            let bwn = Box::new(ReleaseFundsBuilderWithNonce {
                builder,
                nonce,
                transaction_id: nonce as i64,
                trace_id: format!("t-{nonce}"),
                remint_info: Some(make_test_remint_info(nonce as i64, &format!("t-{nonce}"))),
            });
            smt.handle_release_funds_transaction(bwn, Pubkey::new_unique(), vec![], None, None)
                .unwrap();
            smt.smt_state.current_root()
        };

        let root_after_0 = build_and_insert(&mut smt, 0);
        let root_after_1 = build_and_insert(&mut smt, 1);

        assert_ne!(root_after_0, root_after_1);
    }

    // ── rebuild_with_regenerated_proof ────────────────────────────────

    #[tokio::test]
    async fn rebuild_returns_none_when_nonce_is_none() {
        let mut state = make_sender_state();
        let ix = InstructionWithSigners {
            instructions: vec![],
            fee_payer: Pubkey::new_unique(),
            signers: vec![],
            compute_unit_price: None,
            compute_budget: None,
        };
        assert!(rebuild_with_regenerated_proof(&mut state, None, ix)
            .await
            .is_none());
    }

    #[tokio::test]
    async fn rebuild_returns_none_without_smt_state() {
        let mut state = make_sender_state();
        let ix = InstructionWithSigners {
            instructions: vec![],
            fee_payer: Pubkey::new_unique(),
            signers: vec![],
            compute_unit_price: None,
            compute_budget: None,
        };
        assert!(rebuild_with_regenerated_proof(&mut state, Some(0), ix)
            .await
            .is_none());
    }

    #[tokio::test]
    async fn rebuild_returns_none_when_no_cached_builder() {
        let mut state = make_sender_state();
        state.smt_state = Some(make_smt_state(0));
        let ix = InstructionWithSigners {
            instructions: vec![],
            fee_payer: Pubkey::new_unique(),
            signers: vec![],
            compute_unit_price: None,
            compute_budget: None,
        };
        assert!(rebuild_with_regenerated_proof(&mut state, Some(99), ix)
            .await
            .is_none());
    }

    #[tokio::test]
    async fn rebuild_success_with_cached_builder() {
        let mut state = make_sender_state();
        let mut smt = make_smt_state(0);

        // Cache a builder at nonce 0
        let builder = make_release_funds_builder();
        let ctx = TransactionContext {
            transaction_id: Some(1),
            withdrawal_nonce: Some(0),
            trace_id: Some("t".to_string()),
        };
        smt.nonce_to_builder.insert(0, (ctx, builder));
        state.smt_state = Some(smt);

        let fee_payer = Pubkey::new_unique();
        let ix = InstructionWithSigners {
            instructions: vec![],
            fee_payer,
            signers: vec![],
            compute_unit_price: Some(1000),
            compute_budget: Some(300_000),
        };

        let result = rebuild_with_regenerated_proof(&mut state, Some(0), ix).await;
        assert!(result.is_some());
        let rebuilt = result.unwrap();
        assert_eq!(rebuilt.fee_payer, fee_payer);
        assert_eq!(rebuilt.compute_unit_price, Some(1000));
    }

    #[tokio::test]
    async fn rebuild_propagates_cached_remint_info() {
        let mut state = make_sender_state();
        let mut smt = make_smt_state(0);

        let builder = make_release_funds_builder();
        let ctx = TransactionContext {
            transaction_id: Some(1),
            withdrawal_nonce: Some(0),
            trace_id: Some("t".to_string()),
        };
        smt.nonce_to_builder.insert(0, (ctx, builder));
        state.smt_state = Some(smt);

        // Seed remint_cache so rebuild can propagate it
        let remint = make_test_remint_info(1, "t");
        let expected_mint = remint.mint;
        state.remint_cache.insert(0, remint);

        let ix = InstructionWithSigners {
            instructions: vec![],
            fee_payer: Pubkey::new_unique(),
            signers: vec![],
            compute_unit_price: None,
            compute_budget: None,
        };

        let result = rebuild_with_regenerated_proof(&mut state, Some(0), ix).await;
        assert!(result.is_some());

        // Verify remint_cache is still present (rebuild reads but doesn't drain)
        let cached = state.remint_cache.get(&0);
        assert!(cached.is_some(), "remint_cache should survive rebuild");
        assert_eq!(cached.unwrap().mint, expected_mint);
    }
}
