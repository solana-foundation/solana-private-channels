use crate::error::OperatorError;
use crate::operator::bitmap_constants::NONCES_PER_GENERATION;
use crate::operator::sender::mint;
use crate::operator::{
    find_event_authority_pda, find_operator_pda, find_withdrawal_bitmap_pda,
    ReleaseFundsBuilderWithNonce, SignerUtil, TransactionKind,
};
use private_channel_escrow_program_client::instructions::RotateBitmapBuilder;
use private_channel_metrics::MetricLabel;
use solana_keychain::Signer;
use solana_sdk::pubkey::Pubkey;
use std::time::Duration;
use tracing::{error, info, warn};

use super::transaction::{classify_generation, park_release_for_rotation, GenerationWindow};
use super::types::{InstructionWithSigners, SenderState, TransactionContext};

impl SenderState {
    /// Build a signable release, unless the bitmap's window says it is doomed.
    ///
    /// The program refuses a nonce from another generation, so a release built
    /// here can be worth nothing before it is ever signed. This check is only an
    /// optimisation in front of that refusal, which stays the authority and
    /// still handles anything this check waves through.
    pub(super) async fn handle_release_funds_transaction(
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
        let builder = builder_with_nonce.builder;

        let (window, chain_generation) = self.release_window(nonce).await;
        let instruction = InstructionWithSigners {
            instructions: vec![builder.instruction()],
            fee_payer,
            signers,
            compute_budget,
            compute_unit_price,
        };

        let nonce_generation = nonce / NONCES_PER_GENERATION;
        match window {
            GenerationWindow::Open => {
                self.in_flight_withdrawals.insert(nonce);
                Ok(instruction)
            }
            // Nothing in a release depends on the generation, so the instruction
            // built here is exactly the one the rotation makes valid. The nonce
            // stays out of the in-flight set on purpose: that set is what the
            // rotation waits on, and waiting on this nonce would hold back the
            // very rotation that unblocks it.
            GenerationWindow::NotYetOpen => {
                // The wait has to be durable before it is queued: an unparked
                // entry would leave the in-memory queue as the only record of a
                // release that was never broadcast.
                if !park_release_for_rotation(&self.storage, transaction_id, nonce).await {
                    return Err(generation_mismatch(
                        nonce,
                        nonce_generation,
                        chain_generation,
                    ));
                }
                info!(
                    nonce,
                    nonce_generation,
                    chain_generation,
                    "Holding the release until its window opens"
                );
                self.rotation_retry_queue.push((
                    TransactionContext {
                        kind: TransactionKind::ReleaseFunds,
                        transaction_id: Some(transaction_id),
                        withdrawal_nonce: Some(nonce),
                        trace_id: Some(trace_id),
                    },
                    instruction,
                ));
                Err(generation_mismatch(
                    nonce,
                    nonce_generation,
                    chain_generation,
                ))
            }
            GenerationWindow::Closed => {
                error!(
                    nonce,
                    nonce_generation,
                    chain_generation,
                    "Not sending a release whose generation the bitmap has rotated past"
                );
                Err(generation_mismatch(
                    nonce,
                    nonce_generation,
                    chain_generation,
                ))
            }
        }
    }

    /// Where a nonce sits relative to the bitmap's current generation.
    ///
    ///   cache holds this generation  -> Open                    no RPC
    ///   anything else                -> read the chain          Open | NotYetOpen | Closed
    ///   the read failed              -> Open                    let the program judge
    ///
    /// Only a fresh read is allowed to say no. The cache can lag the chain, and a
    /// lagging cache that refused a withdrawal would strand it forever: unsent
    /// means unrejected, and that rejection is what would have corrected the cache.
    pub(super) async fn release_window(&mut self, nonce: u64) -> (GenerationWindow, u64) {
        let nonce_generation = nonce / NONCES_PER_GENERATION;

        if self.cached_generation == Some(nonce_generation) {
            return (GenerationWindow::Open, nonce_generation);
        }

        match self.refresh_generation().await {
            Ok(chain_generation) => (
                classify_generation(nonce, chain_generation),
                chain_generation,
            ),
            // An unread bitmap refuses nothing. Without a fresh answer the only
            // choice that cannot strand a releasable withdrawal is to broadcast
            // it and let the program judge.
            Err(e) => {
                warn!(nonce, "Sending without a generation check: {e}");
                (GenerationWindow::Open, nonce_generation)
            }
        }
    }
}

fn generation_mismatch(nonce: u64, nonce_generation: u64, chain_generation: u64) -> OperatorError {
    crate::error::ProgramError::GenerationMismatch {
        nonce,
        nonce_generation,
        chain_generation,
    }
    .into()
}

/// How long a withheld rotation has to persist before it is reported. A boundary
/// crossing looks blocked while the closing generation's last row settles, and
/// that clears itself, so the report waits it out.
const ROTATION_BLOCKED_ALERT_AFTER: Duration = Duration::from_secs(300);

/// The same threshold counted in passes, since that is what the driver has. Derived
/// so that retuning the interval moves the passes with it instead of quietly
/// retiming an alert the runbooks describe in minutes.
const ROTATION_BLOCKED_ALERT_PASSES: u32 = (ROTATION_BLOCKED_ALERT_AFTER.as_millis()
    / super::ROTATION_ORIGINATION_INTERVAL.as_millis())
    as u32;

/// Arm a rotation when the chain's generation is behind the work waiting on it.
///
/// The only thing that starts a rotation. Driven by state, not by a particular
/// row reaching the processor, so it still fires when that row was quarantined,
/// swept aside, or never existed. Arming is all it does: the in-flight barrier
/// decides when the rotation is sent, and binds the generation then.
pub(super) async fn originate_rotation_if_needed(state: &mut SenderState) {
    // Only the withdraw role has an escrow instance whose bitmap can rotate.
    let Some(instance_pda) = state.instance_pda else {
        return;
    };

    // One already armed or sent owns this window; a second would pay twice and
    // race the re-arm path holding the first. A rotation on its way is also the
    // end of any block, since the block is precisely the absence of one.
    if state.pending_rotation.is_some() || state.rotation_in_flight.is_some() {
        state.rotation_blocked_passes = 0;
        return;
    }

    // Start the search at the cached generation so the usual pass, which finds
    // nothing to do, skips the older nonces instead of scanning all of them. The
    // cache sits at or behind the chain, and a refused release corrects it if not.
    let cached_floor = state
        .cached_generation
        .map(|cached| cached.saturating_mul(NONCES_PER_GENERATION))
        .unwrap_or(0);
    let highest = match read_unreleased_bounds(state, cached_floor).await {
        Ok(Some((_, highest))) => highest,
        Ok(None) => {
            state.rotation_blocked_passes = 0;
            return;
        }
        Err(BoundsUnavailable) => return,
    };

    // Everything waiting is still inside that window, so nothing needs a
    // rotation and the chain need not be read to say so.
    if state
        .cached_generation
        .is_some_and(|cached| highest / NONCES_PER_GENERATION <= cached)
    {
        state.rotation_blocked_passes = 0;
        return;
    }

    let chain_generation = match state.refresh_generation().await {
        Ok(generation) => generation,
        Err(e) => {
            report_read_failure(state);
            warn!("Not arming a rotation: could not read the current generation: {e}");
            return;
        }
    };

    // Measured from the window the chain is on, since that is the only one a
    // rotation closes. A nonce below it is already past saving, and waiting on
    // one would stall every withdrawal behind it forever.
    let window_floor = chain_generation.saturating_mul(NONCES_PER_GENERATION);
    let (lowest, highest) = match read_unreleased_bounds(state, window_floor).await {
        Ok(Some(bounds)) => bounds,
        Ok(None) => {
            state.rotation_blocked_passes = 0;
            return;
        }
        Err(BoundsUnavailable) => return,
    };

    let lowest_generation = lowest / NONCES_PER_GENERATION;
    let highest_generation = highest / NONCES_PER_GENERATION;

    if lowest_generation > chain_generation {
        info!(
            chain_generation,
            lowest_generation, "Chain is behind the waiting withdrawals, arming a rotation"
        );
        state.pending_rotation = Some(build_rotation(instance_pda));
        state.rotation_blocked_passes = 0;
        return;
    }

    // Nothing is waiting past the window the chain is on, so nothing is blocked.
    if highest_generation <= chain_generation {
        state.rotation_blocked_passes = 0;
        return;
    }

    // Withholding is correct here, but only a human clears it, so report it.
    // Once per threshold, not once per block: a counter that stops ticking lets
    // the alert resolve while the stall is still running. Every pass that
    // establishes no block clears the streak, so this counts consecutive ones.
    state.rotation_blocked_passes = state.rotation_blocked_passes.saturating_add(1);
    if !state
        .rotation_blocked_passes
        .is_multiple_of(ROTATION_BLOCKED_ALERT_PASSES)
    {
        return;
    }

    crate::metrics::OPERATOR_TRANSACTION_ERRORS
        .with_label_values(&[
            state.program_type.as_label(),
            "rotation_blocked_by_lower_nonce",
        ])
        .inc();
    warn!(
        chain_generation,
        blocking_nonce = lowest,
        highest_waiting_nonce = highest,
        "Rotation withheld: a lower nonce still owes a release on the current generation"
    );
}

/// The gate could not be read. Distinct from reading it and finding nothing,
/// because an unread gate is only ever evidence of an outage: it may not arm a
/// rotation, and it may not be taken for proof that a block has cleared.
struct BoundsUnavailable;

/// Bounds of the unreleased nonces at or above `min_nonce`, `None` when there are
/// none, and an error when the answer is unknown. Arming needs the bounds, so
/// both of the other two withhold; only `None` proves nothing is owed.
async fn read_unreleased_bounds(
    state: &SenderState,
    min_nonce: u64,
) -> Result<Option<(u64, u64)>, BoundsUnavailable> {
    let Ok(floor) = i64::try_from(min_nonce) else {
        report_read_failure(state);
        error!(
            min_nonce,
            "Not arming a rotation: the nonce floor does not fit"
        );
        return Err(BoundsUnavailable);
    };

    let bounds = match state
        .storage
        .unreleased_withdrawal_nonce_bounds(floor)
        .await
    {
        Ok(Some(bounds)) => bounds,
        Ok(None) => return Ok(None),
        Err(e) => {
            report_read_failure(state);
            warn!("Not arming a rotation: could not read the unreleased nonces: {e}");
            return Err(BoundsUnavailable);
        }
    };

    match (u64::try_from(bounds.0), u64::try_from(bounds.1)) {
        (Ok(lowest), Ok(highest)) => Ok(Some((lowest, highest))),
        _ => {
            report_read_failure(state);
            error!(
                lowest = bounds.0,
                highest = bounds.1,
                "Not arming a rotation: an unreleased nonce is negative"
            );
            Err(BoundsUnavailable)
        }
    }
}

/// A read the driver needed and did not get. Every one of these withholds a
/// rotation, so an outage long enough to matter wedges the next generation; the
/// counter is what separates that from the far more common idle pass.
fn report_read_failure(state: &SenderState) {
    crate::metrics::OPERATOR_TRANSACTION_ERRORS
        .with_label_values(&[
            state.program_type.as_label(),
            "rotation_origination_read_failed",
        ])
        .inc();
}

/// A rotation for `instance_pda`, with every account derived rather than carried
/// over from whatever built it. `expected_generation` is deliberately left unset:
/// the submit path binds it from a fresh read, which is the only moment the value
/// is still current.
fn build_rotation(instance_pda: Pubkey) -> Box<RotateBitmapBuilder> {
    let operator_pubkey = SignerUtil::get_operator_pubkey();
    let mut builder = RotateBitmapBuilder::new();
    builder
        .payer(SignerUtil::get_admin_pubkey())
        .operator(operator_pubkey)
        .instance(instance_pda)
        .withdrawal_bitmap(find_withdrawal_bitmap_pda(&instance_pda))
        .operator_pda(find_operator_pda(&instance_pda, &operator_pubkey))
        .event_authority(find_event_authority_pda());
    Box::new(builder)
}

/// Check if pending rotation can now be processed.
/// Returns the RotateBitmap builder if ready to execute.
///
/// Two things hold a rotation back, and both are about the bits it would erase.
/// An in-flight release still has a nonce the chain has not recorded yet, and a
/// deferred remint has a nonce whose bit is the only proof of whether the user
/// was already paid. Wiping either turns a decision the chain can answer into
/// one it cannot.
pub async fn take_pending_rotation_if_ready(
    state: &mut SenderState,
) -> Option<Box<RotateBitmapBuilder>> {
    state.pending_rotation.as_ref()?;

    if !state.in_flight_withdrawals.is_empty() {
        return None;
    }

    if !pending_remints_have_settled(state).await {
        return None;
    }

    info!("All in-flight transactions settled, rotation ready to execute");
    state.pending_rotation.take()
}

/// True when no deferred remint still depends on a bit this rotation would clear.
///
/// The generation is read rather than remembered, and only when a remint is
/// actually outstanding, so the ordinary rotation pays for no extra round trip.
/// A failed read leaves us unable to say which nonces the rotation would erase,
/// so it holds the rotation: the next tick tries again, whereas a bit cleared
/// out from under a maturing remint never comes back.
async fn pending_remints_have_settled(state: &SenderState) -> bool {
    let pending_nonces: Vec<u64> = state
        .pending_remints
        .iter()
        .filter_map(|entry| entry.ctx.withdrawal_nonce)
        .collect();

    if pending_nonces.is_empty() {
        return true;
    }

    let generation = match state.fetch_current_generation().await {
        Ok(generation) => generation,
        Err(e) => {
            warn!("Holding the rotation: could not read the current generation: {e}");
            return false;
        }
    };

    let blocking = pending_nonces
        .iter()
        .find(|nonce| **nonce / NONCES_PER_GENERATION == generation);

    if let Some(nonce) = blocking {
        info!(
            generation,
            "Holding the rotation: nonce {nonce} still has a remint waiting on its bit"
        );
        return false;
    }

    true
}

/// Drop the caches a failed withdrawal owns.
///
/// The nonce leaves the in-flight set so a queued rotation is no longer blocked
/// by it. Nothing else needs undoing: the chain records consumption, so there is
/// no local replay state that could drift from it.
pub(super) fn cleanup_failed_transaction(state: &mut SenderState, nonce: Option<u64>) {
    if let Some(nonce) = nonce {
        state.in_flight_withdrawals.remove(&nonce);
        state.retry_counts.remove(&nonce);
        // Note: when called from handle_permanent_failure, remint_cache is
        // already drained. This removal is defensive for any other call site.
        state.remint_cache.remove(&nonce);
    }

    mint::cleanup_mint_builder(state, nonce.map(|n| n as i64));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ProgramError;
    use crate::operator::sender::test_support::{
        ensure_test_signer, mock_bitmap_account, mock_bitmap_account_counted,
        mock_bitmap_read_failure, mock_bitmap_sequence, mock_with_processing_row,
        push_processing_row, push_withdrawal_with_nonce, row_status, sender_state,
        sender_state_with_storage,
    };
    use crate::operator::sender::transaction::handle_nonce_outside_generation;
    use crate::operator::sender::types::{PendingRemint, PendingSig, TransactionContext};
    use crate::operator::utils::instruction_util::WithdrawalRemintInfo;
    use crate::storage::common::models::TransactionStatus;
    use crate::storage::common::storage::mock::MockStorage;
    use private_channel_escrow_program_client::instructions::ReleaseFundsBuilder;
    use solana_sdk::signature::Signature;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::sync::mpsc;

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

    fn make_release_funds_builder() -> ReleaseFundsBuilder {
        let mut b = ReleaseFundsBuilder::new();
        let pk = Pubkey::new_unique();
        b.payer(pk)
            .operator(pk)
            .instance(pk)
            .withdrawal_bitmap(pk)
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

    fn rotation_builder() -> Box<RotateBitmapBuilder> {
        let mut b = RotateBitmapBuilder::new();
        let pk = Pubkey::new_unique();
        b.payer(pk)
            .operator(pk)
            .instance(pk)
            .withdrawal_bitmap(pk)
            .operator_pda(pk)
            .expected_generation(0);
        Box::new(b)
    }

    /// Queue a matured deferred remint for `nonce`.
    fn queue_pending_remint(state: &mut SenderState, nonce: u64) {
        state.pending_remints.push(PendingRemint {
            ctx: TransactionContext {
                kind: TransactionKind::ReleaseFunds,
                transaction_id: Some(1),
                withdrawal_nonce: Some(nonce),
                trace_id: Some("t".to_string()),
            },
            remint_info: make_test_remint_info(1, "t"),
            signatures: Vec::new(),
            original_error: "release_funds failed".to_string(),
            deadline: chrono::Utc::now(),
            finality_check_attempts: 0,
            release_refused_on_chain: false,
            coverage_slot: None,
        });
    }

    // ── take_pending_rotation_if_ready ───────────────────────────────

    #[tokio::test]
    async fn rotation_returns_none_when_no_pending() {
        let mut state = sender_state("http://localhost:8899");
        assert!(take_pending_rotation_if_ready(&mut state).await.is_none());
    }

    #[tokio::test]
    async fn rotation_returns_builder_when_no_inflight() {
        let mut state = sender_state("http://localhost:8899");
        state.pending_rotation = Some(rotation_builder());

        assert!(take_pending_rotation_if_ready(&mut state).await.is_some());
        assert!(state.pending_rotation.is_none(), "should be taken");
    }

    /// Rotation clears every bit, so it must not overtake a release whose nonce
    /// is still in flight; that nonce would become replayable.
    #[tokio::test]
    async fn rotation_blocked_by_inflight_withdrawal() {
        let mut state = sender_state("http://localhost:8899");
        state.pending_rotation = Some(rotation_builder());
        state.in_flight_withdrawals.insert(0);

        assert!(take_pending_rotation_if_ready(&mut state).await.is_none());
        assert!(state.pending_rotation.is_some(), "should NOT be taken yet");
    }

    #[tokio::test]
    async fn rotation_ready_after_inflight_cleared() {
        let mut state = sender_state("http://localhost:8899");
        state.pending_rotation = Some(rotation_builder());
        state.in_flight_withdrawals.insert(0);
        state.in_flight_withdrawals.remove(&0);

        assert!(take_pending_rotation_if_ready(&mut state).await.is_some());
    }

    /// A deferred remint has left the in-flight set but its outcome still turns
    /// on the bit for its nonce. Rotating now wipes that bit, so the gate that
    /// decides whether to credit the user would be reading a blank window and
    /// would have to give up on the only answer it trusts.
    #[tokio::test]
    async fn rotation_blocked_by_pending_remint_in_the_current_generation() {
        let mut server = mockito::Server::new_async().await;
        let _bitmap = mock_bitmap_account(&mut server, 0, &[]);

        let mut state = sender_state(&server.url());
        state.instance_pda = Some(Pubkey::new_unique());
        state.pending_rotation = Some(rotation_builder());
        queue_pending_remint(&mut state, 3);

        assert!(take_pending_rotation_if_ready(&mut state).await.is_none());
        assert!(state.pending_rotation.is_some(), "should NOT be taken yet");
    }

    /// Only the generation about to be wiped matters. An older nonce's bit is
    /// already gone, so holding the rotation for it would close the next
    /// generation to every withdrawal in it and repair nothing at all for the
    /// nonce it was held on behalf of.
    #[tokio::test]
    async fn rotation_ready_when_pending_remint_belongs_to_an_older_generation() {
        use crate::operator::bitmap_constants::NONCES_PER_GENERATION;

        let mut server = mockito::Server::new_async().await;
        let _bitmap = mock_bitmap_account(&mut server, 1, &[]);

        let mut state = sender_state(&server.url());
        state.instance_pda = Some(Pubkey::new_unique());
        state.pending_rotation = Some(rotation_builder());
        queue_pending_remint(&mut state, NONCES_PER_GENERATION - 1);

        assert!(take_pending_rotation_if_ready(&mut state).await.is_some());
    }

    /// Not knowing which generation the chain is on means not knowing whether
    /// the rotation would erase evidence a pending remint still needs, and a
    /// cleared bit never comes back, so the rotation waits for a reading.
    #[tokio::test]
    async fn rotation_blocked_when_the_generation_cannot_be_read() {
        let mut server = mockito::Server::new_async().await;
        let _bitmap = mock_bitmap_read_failure(&mut server);

        let mut state = sender_state(&server.url());
        state.instance_pda = Some(Pubkey::new_unique());
        state.pending_rotation = Some(rotation_builder());
        queue_pending_remint(&mut state, 3);

        assert!(take_pending_rotation_if_ready(&mut state).await.is_none());
        assert!(state.pending_rotation.is_some(), "should NOT be taken yet");
    }

    // ── originate_rotation_if_needed ─────────────────────────────────

    /// A withdraw-role state whose storage holds exactly the seeded rows.
    fn originator_state(url: &str, rows: &[(i64, i64, TransactionStatus)]) -> SenderState {
        ensure_test_signer();
        let mock = MockStorage::new();
        for (id, nonce, status) in rows {
            push_withdrawal_with_nonce(&mock, *id, *nonce, *status);
        }
        let mut state = sender_state_with_storage(url, mock);
        state.instance_pda = Some(Pubkey::new_unique());
        state
    }

    fn blocked_count() -> f64 {
        crate::metrics::OPERATOR_TRANSACTION_ERRORS
            .with_label_values(&["escrow", "rotation_blocked_by_lower_nonce"])
            .get()
    }

    fn read_failure_count() -> f64 {
        crate::metrics::OPERATOR_TRANSACTION_ERRORS
            .with_label_values(&["escrow", "rotation_origination_read_failed"])
            .get()
    }

    /// Two ways the documented threshold can drift from the one that ships, kept
    /// apart so a failure says which. The runbooks name five minutes, and the
    /// passes are that threshold divided by an interval from another file, a
    /// division that only lands on it exactly if the interval divides it.
    #[test]
    fn the_blocked_alert_reports_after_five_minutes() {
        assert_eq!(
            ROTATION_BLOCKED_ALERT_AFTER,
            Duration::from_secs(300),
            "the runbooks document five minutes; change them with this"
        );
        assert_eq!(
            super::super::ROTATION_ORIGINATION_INTERVAL * ROTATION_BLOCKED_ALERT_PASSES,
            ROTATION_BLOCKED_ALERT_AFTER,
            "the interval must divide the threshold exactly"
        );
    }

    /// The whole point of the driver: work waiting in the next generation with
    /// nothing left owing in this one is what a rotation is for, and no
    /// particular row has to survive for the driver to see it.
    // Forces the admin-signer statics, and `signer_util`'s tests clear that
    // env while they run, so this has to be ordered against them.
    #[tokio::test]
    #[serial_test::serial]
    async fn originates_when_lowest_unreleased_is_in_a_later_generation() {
        let mut server = mockito::Server::new_async().await;
        let _bitmap = mock_bitmap_account(&mut server, 0, &[]);

        let mut state = originator_state(
            &server.url(),
            &[(1, NONCES_PER_GENERATION as i64, TransactionStatus::Pending)],
        );

        originate_rotation_if_needed(&mut state).await;

        assert!(
            state.pending_rotation.is_some(),
            "a generation the chain has not opened yet must arm a rotation"
        );
    }

    /// The current window still owes a release, so rotating would close the only
    /// window that release can land in.
    #[tokio::test]
    async fn does_not_originate_when_lowest_unreleased_is_in_the_current_generation() {
        let mut server = mockito::Server::new_async().await;
        let _bitmap = mock_bitmap_account(&mut server, 0, &[]);

        let mut state = originator_state(&server.url(), &[(1, 1, TransactionStatus::Pending)]);

        originate_rotation_if_needed(&mut state).await;

        assert!(state.pending_rotation.is_none());
    }

    #[tokio::test]
    async fn does_not_originate_when_nothing_is_unreleased() {
        let mut server = mockito::Server::new_async().await;
        let _bitmap = mock_bitmap_account(&mut server, 0, &[]);

        let mut state = originator_state(&server.url(), &[]);

        originate_rotation_if_needed(&mut state).await;

        assert!(
            state.pending_rotation.is_none(),
            "a rotation nothing is waiting on buys nothing"
        );
    }

    /// The streak has to count consecutive blocked passes, not blocked passes
    /// ever seen. A block that clears and a later unrelated one are two
    /// incidents, and carrying the first count into the second would report the
    /// second within one pass of starting rather than after the settling window.
    #[tokio::test]
    async fn a_pass_that_sees_no_block_clears_the_streak() {
        // Every way the driver can finish a pass having established that nothing
        // is being withheld, short of the two that already clear it.
        for case in [
            "nothing live at all",
            "a rotation already armed",
            "everything waiting inside the cached window",
        ] {
            let mut server = mockito::Server::new_async().await;
            let _bitmap = mock_bitmap_account(&mut server, 0, &[]);

            let rows: &[(i64, i64, TransactionStatus)] = match case {
                "nothing live at all" => &[],
                _ => &[(1, 1, TransactionStatus::Pending)],
            };
            let mut state = originator_state(&server.url(), rows);
            match case {
                "a rotation already armed" => state.pending_rotation = Some(rotation_builder()),
                "everything waiting inside the cached window" => state.cached_generation = Some(0),
                _ => {}
            }
            state.rotation_blocked_passes = ROTATION_BLOCKED_ALERT_PASSES - 1;

            originate_rotation_if_needed(&mut state).await;

            assert_eq!(
                state.rotation_blocked_passes, 0,
                "{case}: a pass that saw no block must not leave the next one primed to report"
            );
        }
    }

    /// A read that failed establishes nothing, so it must not be mistaken for a
    /// pass that saw the block clear. Clearing the streak on an outage would
    /// restart the settling window every time the database blinked, and a stall
    /// spanning those blinks would never reach the threshold to be reported.
    // Reads a process-wide counter, so it has to be ordered against the other
    // tests that assert on it.
    #[serial_test::serial]
    #[tokio::test]
    async fn a_pass_that_could_not_read_leaves_the_streak_alone() {
        let mut server = mockito::Server::new_async().await;
        let _bitmap = mock_bitmap_account(&mut server, 0, &[]);

        let mut state = originator_state(
            &server.url(),
            &[(1, NONCES_PER_GENERATION as i64, TransactionStatus::Pending)],
        );
        match state.storage.as_ref() {
            crate::storage::Storage::Mock(mock) => {
                mock.set_should_fail("unreleased_withdrawal_nonce_bounds", true)
            }
            _ => unreachable!("the originator harness is built on the mock"),
        }
        state.rotation_blocked_passes = ROTATION_BLOCKED_ALERT_PASSES - 1;

        originate_rotation_if_needed(&mut state).await;

        assert_eq!(
            state.rotation_blocked_passes,
            ROTATION_BLOCKED_ALERT_PASSES - 1,
            "an unread pass must neither count as a block nor clear one"
        );
    }

    /// One rotation advances one generation, so a gap wider than that has to be
    /// closed by the driver firing again rather than by a single larger step.
    // Forces the admin-signer statics, and `signer_util`'s tests clear that
    // env while they run, so this has to be ordered against them.
    #[tokio::test]
    #[serial_test::serial]
    async fn originates_again_after_a_rotation_when_the_gap_spans_generations() {
        let mut server = mockito::Server::new_async().await;
        let _bitmap = mock_bitmap_account(&mut server, 0, &[]);

        let mut state = originator_state(
            &server.url(),
            &[(
                1,
                2 * NONCES_PER_GENERATION as i64,
                TransactionStatus::Pending,
            )],
        );

        originate_rotation_if_needed(&mut state).await;
        assert!(state.pending_rotation.is_some(), "first rotation arms");

        // Stand in for that rotation landing: the arm clears and the chain moves on.
        state.pending_rotation = None;
        let mut server = mockito::Server::new_async().await;
        let _bitmap = mock_bitmap_account(&mut server, 1, &[]);
        state.rpc_client = std::sync::Arc::new(
            crate::operator::utils::rpc_util::RpcClientWithRetry::with_retry_config(
                server.url(),
                crate::operator::utils::rpc_util::RetryConfig {
                    max_attempts: 1,
                    base_delay: std::time::Duration::from_millis(1),
                    max_delay: std::time::Duration::from_millis(1),
                },
                solana_sdk::commitment_config::CommitmentConfig::confirmed(),
            ),
        );
        state.cached_generation = Some(1);

        originate_rotation_if_needed(&mut state).await;
        assert!(
            state.pending_rotation.is_some(),
            "a two-generation gap needs a second rotation"
        );
    }

    /// Crossing a boundary always looks blocked for a moment: the first row of
    /// the next generation arrives while the last of the current one is still
    /// settling, and that resolves itself. Reporting it would page an operator
    /// for ordinary traffic, so the signal has to wait for the block to persist.
    // Reads a process-wide counter, so it has to be ordered against the other
    // tests that assert on it.
    #[serial_test::serial]
    #[tokio::test]
    async fn an_ordinary_boundary_crossing_is_not_reported_as_blocked() {
        let mut server = mockito::Server::new_async().await;
        let _bitmap = mock_bitmap_account(&mut server, 0, &[]);

        let mut state = originator_state(
            &server.url(),
            &[
                (1, 2, TransactionStatus::Processing),
                (2, NONCES_PER_GENERATION as i64, TransactionStatus::Pending),
            ],
        );

        let before = blocked_count();
        originate_rotation_if_needed(&mut state).await;

        assert!(state.pending_rotation.is_none(), "the gate must withhold");
        assert_eq!(
            blocked_count(),
            before,
            "a block that has just started is ordinary traffic, not an incident"
        );
    }

    /// The stall this driver must not hide: later work is queued up but a lower
    /// nonce still owes a release, so the rotation is correctly withheld and the
    /// operator has to be told rather than left reading a quiet log. Only once
    /// the block outlasts the settling window, since nothing but a human
    /// resolving that row clears it from there.
    // Reads a process-wide counter, so it has to be ordered against the other
    // tests that assert on it.
    #[serial_test::serial]
    #[tokio::test]
    async fn a_block_that_persists_is_reported() {
        let mut server = mockito::Server::new_async().await;
        let _bitmap = mock_bitmap_account(&mut server, 0, &[]);

        let mut state = originator_state(
            &server.url(),
            &[
                (1, 2, TransactionStatus::ManualReview),
                (2, NONCES_PER_GENERATION as i64, TransactionStatus::Pending),
            ],
        );

        let before = blocked_count();
        for _ in 0..ROTATION_BLOCKED_ALERT_PASSES {
            originate_rotation_if_needed(&mut state).await;
        }

        assert!(state.pending_rotation.is_none(), "the gate must withhold");
        assert_eq!(
            blocked_count(),
            before + 1.0,
            "the pass that crosses the threshold must report exactly once"
        );
    }

    /// A counter that ticks once and then goes quiet lets an alert resolve while
    /// the stall it was raised for is still running. The block is only cleared by
    /// a human resolving the nonce that holds it, which can take far longer than
    /// any evaluation window, so the report has to keep repeating for as long as
    /// the rotation is still being withheld.
    // Reads a process-wide counter, so it has to be ordered against the other
    // tests that assert on it.
    #[serial_test::serial]
    #[tokio::test]
    async fn a_block_that_outlasts_the_first_report_keeps_reporting() {
        let mut server = mockito::Server::new_async().await;
        let _bitmap = mock_bitmap_account(&mut server, 0, &[]);

        let mut state = originator_state(
            &server.url(),
            &[
                (1, 2, TransactionStatus::ManualReview),
                (2, NONCES_PER_GENERATION as i64, TransactionStatus::Pending),
            ],
        );

        let before = blocked_count();
        for _ in 0..(ROTATION_BLOCKED_ALERT_PASSES * 3) {
            originate_rotation_if_needed(&mut state).await;
        }

        assert!(state.pending_rotation.is_none(), "the gate must withhold");
        assert_eq!(
            blocked_count(),
            before + 3.0,
            "a block three thresholds long must report once per threshold"
        );
    }

    /// The gate runs on a timer for the life of the process, so the pass that
    /// finds nothing to do must not aggregate the whole nonce history. The cache
    /// lags the chain but never leads it, so its own window is a floor the query
    /// can be bounded by without hiding anything the gate would have acted on.
    #[tokio::test]
    async fn the_quiet_pass_reads_only_from_the_cached_window() {
        let mut server = mockito::Server::new_async().await;
        let reads = Arc::new(AtomicUsize::new(0));
        let _bitmap = mock_bitmap_account_counted(&mut server, 4, reads.clone());

        let mut state = originator_state(
            &server.url(),
            &[(
                1,
                4 * NONCES_PER_GENERATION as i64 + 1,
                TransactionStatus::Pending,
            )],
        );
        state.cached_generation = Some(4);
        let floors = match state.storage.as_ref() {
            crate::storage::Storage::Mock(mock) => mock.unreleased_bounds_floors.clone(),
            _ => unreachable!("the originator harness is built on the mock"),
        };

        originate_rotation_if_needed(&mut state).await;

        assert_eq!(
            *floors.lock().unwrap(),
            vec![4 * NONCES_PER_GENERATION as i64],
            "the quiet pass must ask only about the cached window, and only once"
        );
        assert_eq!(
            reads.load(Ordering::SeqCst),
            0,
            "a cache that already covers the waiting work must cost no RPC"
        );
        assert!(state.pending_rotation.is_none());
    }

    /// Nothing is known at boot, so the cache cannot be trusted to skip and the
    /// chain has to be read before the gate can answer either way.
    // Forces the admin-signer statics, and `signer_util`'s tests clear that
    // env while they run, so this has to be ordered against them.
    #[tokio::test]
    #[serial_test::serial]
    async fn reads_fresh_when_the_cached_generation_is_none() {
        let mut server = mockito::Server::new_async().await;
        let reads = Arc::new(AtomicUsize::new(0));
        let _bitmap = mock_bitmap_account_counted(&mut server, 0, reads.clone());

        let mut state = originator_state(
            &server.url(),
            &[(1, NONCES_PER_GENERATION as i64, TransactionStatus::Pending)],
        );
        state.cached_generation = None;

        originate_rotation_if_needed(&mut state).await;

        assert_eq!(
            reads.load(Ordering::SeqCst),
            1,
            "an unknown cache must be settled against the chain"
        );
        assert!(
            state.pending_rotation.is_some(),
            "the fresh read shows the chain behind the waiting work"
        );
    }

    /// A rotation already armed or already sent owns the window it moves. A
    /// second would pay for the same rotation twice and fight the re-arm path
    /// that is holding the first.
    #[tokio::test]
    async fn does_not_originate_while_a_rotation_is_armed_or_in_flight() {
        for already_in_flight in [false, true] {
            let mut server = mockito::Server::new_async().await;
            let reads = Arc::new(AtomicUsize::new(0));
            let _bitmap = mock_bitmap_account_counted(&mut server, 0, reads.clone());

            let mut state = originator_state(
                &server.url(),
                &[(1, NONCES_PER_GENERATION as i64, TransactionStatus::Pending)],
            );
            if already_in_flight {
                state.rotation_in_flight = Some(rotation_builder());
            } else {
                state.pending_rotation = Some(rotation_builder());
            }

            originate_rotation_if_needed(&mut state).await;

            assert_eq!(
                reads.load(Ordering::SeqCst),
                0,
                "a rotation already under way must end the pass before any read"
            );
        }
    }

    /// An unread gate is not a passed gate: arming without knowing what is still
    /// owed could rotate past a nonce whose release can then never land. A
    /// database outage therefore wedges every later generation, so the withhold
    /// has to move a counter rather than pass as an ordinary idle tick.
    // Reads a process-wide counter, so it has to be ordered against the other
    // tests that assert on it.
    #[serial_test::serial]
    #[tokio::test]
    async fn does_not_originate_when_the_storage_read_fails() {
        let mut server = mockito::Server::new_async().await;
        let _bitmap = mock_bitmap_account(&mut server, 0, &[]);

        let mut state = originator_state(
            &server.url(),
            &[(1, NONCES_PER_GENERATION as i64, TransactionStatus::Pending)],
        );
        match state.storage.as_ref() {
            crate::storage::Storage::Mock(mock) => {
                mock.set_should_fail("unreleased_withdrawal_nonce_bounds", true)
            }
            _ => unreachable!("the originator harness is built on the mock"),
        }

        let before = read_failure_count();
        originate_rotation_if_needed(&mut state).await;

        assert!(
            state.pending_rotation.is_none(),
            "a gate that could not be read must arm nothing"
        );
        assert_eq!(
            read_failure_count(),
            before + 1.0,
            "a stall this driver cannot see past must be visible on the dashboards"
        );
    }

    /// The same stall from the other read: the chain's generation is what the
    /// bounds are measured against, so an RPC outage withholds every rotation
    /// just as a database one does and has to be just as visible.
    // Reads a process-wide counter, so it has to be ordered against the other
    // tests that assert on it.
    #[serial_test::serial]
    #[tokio::test]
    async fn does_not_originate_when_the_generation_read_fails() {
        let mut server = mockito::Server::new_async().await;
        let _bitmap = mock_bitmap_read_failure(&mut server);

        let mut state = originator_state(
            &server.url(),
            &[(1, NONCES_PER_GENERATION as i64, TransactionStatus::Pending)],
        );

        let before = read_failure_count();
        originate_rotation_if_needed(&mut state).await;

        assert!(
            state.pending_rotation.is_none(),
            "an unreadable chain generation must arm nothing"
        );
        assert_eq!(
            read_failure_count(),
            before + 1.0,
            "a stall this driver cannot see past must be visible on the dashboards"
        );
    }

    /// Only the withdraw role has an escrow instance whose bitmap can rotate, so
    /// the escrow sender must not read, arm, or report anything.
    #[tokio::test]
    async fn does_not_originate_for_the_escrow_role() {
        let mut server = mockito::Server::new_async().await;
        let reads = Arc::new(AtomicUsize::new(0));
        let _bitmap = mock_bitmap_account_counted(&mut server, 0, reads.clone());

        let mut state = originator_state(
            &server.url(),
            &[(1, NONCES_PER_GENERATION as i64, TransactionStatus::Pending)],
        );
        state.instance_pda = None;

        originate_rotation_if_needed(&mut state).await;

        assert!(state.pending_rotation.is_none(), "no bitmap, no rotation");
        assert_eq!(
            reads.load(Ordering::SeqCst),
            0,
            "the escrow role must not read a bitmap it does not have"
        );
    }

    /// A live nonce below the chain's generation is already past saving: its
    /// window shut and no rotation reopens it. Holding the rotation for it would
    /// buy that row nothing and would stall every withdrawal behind it forever,
    /// so the gate has to look at the current window rather than at the lowest
    /// live nonce anywhere. Reachable on the first deploy, because the rotation
    /// this replaces fired regardless of what was unresolved below it.
    // Forces the admin-signer statics, and `signer_util`'s tests clear that
    // env while they run, so this has to be ordered against them.
    #[tokio::test]
    #[serial_test::serial]
    async fn originates_despite_a_nonce_stranded_below_the_current_generation() {
        let mut server = mockito::Server::new_async().await;
        let _bitmap = mock_bitmap_account(&mut server, 1, &[]);

        let mut state = originator_state(
            &server.url(),
            &[
                (1, 1, TransactionStatus::ManualReview),
                (
                    2,
                    2 * NONCES_PER_GENERATION as i64,
                    TransactionStatus::Pending,
                ),
            ],
        );

        originate_rotation_if_needed(&mut state).await;

        assert!(
            state.pending_rotation.is_some(),
            "a nonce whose window already closed must not hold the rotation back"
        );
    }

    /// Arming is all the driver does. The barrier that protects bits from being
    /// erased under an in-flight release still decides when it is sent.
    #[tokio::test]
    async fn arming_does_not_submit_while_the_barrier_is_closed() {
        let mut server = mockito::Server::new_async().await;
        let _bitmap = mock_bitmap_account(&mut server, 0, &[]);

        let mut state = originator_state(
            &server.url(),
            &[(1, NONCES_PER_GENERATION as i64, TransactionStatus::Pending)],
        );
        state.in_flight_withdrawals.insert(3);

        originate_rotation_if_needed(&mut state).await;
        assert!(state.pending_rotation.is_some(), "the driver still arms");

        assert!(
            take_pending_rotation_if_ready(&mut state).await.is_none(),
            "the in-flight barrier still holds the rotation back"
        );
        assert!(
            state.pending_rotation.is_some(),
            "the arm survives for a later tick"
        );
    }

    // ── cleanup_failed_transaction ───────────────────────────────────

    /// A failed withdrawal must release the rotation barrier it was holding,
    /// otherwise a queued rotation never fires.
    #[test]
    fn cleanup_clears_inflight_and_caches() {
        let mut state = sender_state("http://localhost:8899");
        state.in_flight_withdrawals.insert(5);
        state.retry_counts.insert(5, 2);
        state.remint_cache.insert(5, make_test_remint_info(1, "t"));

        cleanup_failed_transaction(&mut state, Some(5));

        assert!(!state.in_flight_withdrawals.contains(&5));
        assert!(!state.retry_counts.contains_key(&5));
        assert!(!state.remint_cache.contains_key(&5));
    }

    #[test]
    fn cleanup_with_none_nonce_is_noop() {
        let mut state = sender_state("http://localhost:8899");
        state.in_flight_withdrawals.insert(5);

        cleanup_failed_transaction(&mut state, None);

        assert!(state.in_flight_withdrawals.contains(&5));
    }

    // ── handle_release_funds_transaction ─────────────────────────────

    /// A release for `nonce`, carrying the remint info every withdrawal has.
    /// The row id every built release in these tests belongs to.
    const RELEASE_TXID: i64 = 1;

    fn release_with_nonce(nonce: u64) -> Box<ReleaseFundsBuilderWithNonce> {
        Box::new(ReleaseFundsBuilderWithNonce {
            builder: make_release_funds_builder(),
            nonce,
            transaction_id: RELEASE_TXID,
            trace_id: "t".to_string(),
            remint_info: Some(make_test_remint_info(1, "t")),
        })
    }

    /// Run the release build path for `nonce`.
    async fn build_release(
        state: &mut SenderState,
        nonce: u64,
    ) -> Result<InstructionWithSigners, OperatorError> {
        state
            .handle_release_funds_transaction(
                release_with_nonce(nonce),
                Pubkey::new_unique(),
                vec![],
                None,
                None,
            )
            .await
    }

    /// The deadlock guard, and the reason the cache is trusted asymmetrically.
    ///
    /// A cache that has not seen the rotation yet says this nonce is out of the
    /// window. Refusing on that answer alone would never send the withdrawal,
    /// so it would never be rejected, so nothing would ever correct the cache
    /// that refused it: the withdrawal is stuck for good.
    #[tokio::test]
    async fn stale_cache_does_not_withhold_a_nonce_the_chain_has_rotated_into() {
        let mut server = mockito::Server::new_async().await;
        let (_bitmap, reads) = mock_bitmap_sequence(&mut server, vec![(1, Vec::new())]);

        let mut state = sender_state(&server.url());
        state.instance_pda = Some(Pubkey::new_unique());
        state.cached_generation = Some(0);
        let nonce = NONCES_PER_GENERATION;

        let result = build_release(&mut state, nonce).await;

        assert!(
            result.is_ok(),
            "the chain has rotated into this nonce's window, so it must be sent"
        );
        assert!(state.in_flight_withdrawals.contains(&nonce));
        assert!(state.rotation_retry_queue.is_empty());
        assert_eq!(reads.load(Ordering::SeqCst), 1, "one confirming read");
        assert_eq!(state.cached_generation, Some(1), "the read is remembered");
    }

    /// A cache that wrongly permits a send is no worse than having no cache at
    /// all. The release is broadcast, the program refuses it, and the rejection
    /// path routes the withdrawal to its compensating remint and corrects the
    /// cache on the way through.
    #[tokio::test]
    async fn stale_cache_that_permits_a_send_degrades_to_the_rejection_path() {
        let mut server = mockito::Server::new_async().await;
        let (_bitmap, reads) = mock_bitmap_sequence(&mut server, vec![(1, Vec::new())]);

        // The deferral is a compare-and-set against a Processing row, so the row
        // has to exist for the rejection path to persist anything.
        let mock = mock_with_processing_row(1);
        // The compensating remint classifies the journaled attempt, so the row
        // needs the write-ahead record an earlier broadcast left behind.
        let broadcast = Signature::new_unique();
        mock.insert_release_signature(1, broadcast.to_string(), 1, None)
            .await
            .unwrap();
        let mut state = sender_state_with_storage(&server.url(), mock);
        state.instance_pda = Some(Pubkey::new_unique());
        // The chain rotated to generation 1 without the cache hearing about it.
        state.cached_generation = Some(0);
        state.remint_cache.insert(0, make_test_remint_info(1, "t"));
        state.pending_signatures.insert(
            0,
            vec![PendingSig {
                signature: broadcast,
                last_valid_block_height: 1,
            }],
        );

        let instruction = build_release(&mut state, 0)
            .await
            .expect("a matching cache permits the send");
        assert_eq!(reads.load(Ordering::SeqCst), 0, "the cache answered");

        let (tx, mut rx) = mpsc::channel(10);
        let ctx = TransactionContext {
            kind: TransactionKind::ReleaseFunds,
            transaction_id: Some(1),
            withdrawal_nonce: Some(0),
            trace_id: Some("t".to_string()),
        };
        handle_nonce_outside_generation(
            &mut state,
            &ctx,
            Signature::new_unique(),
            instruction,
            &tx,
        )
        .await;

        assert_eq!(
            state.pending_remints.len(),
            1,
            "the rejection path still compensates the withdrawal"
        );
        assert!(rx.try_recv().is_err(), "no terminal status was written");
        assert_eq!(
            state.cached_generation,
            Some(1),
            "the rejection path refreshed the stale cache"
        );
        assert!(!state.in_flight_withdrawals.contains(&0));
    }

    /// An unknown cache is not a refusal. It has to be resolved against the
    /// chain first, or the first withdrawal after a restart would be written off
    /// for no reason other than the operator having just started and not yet
    /// having looked.
    #[tokio::test]
    async fn unknown_cache_verifies_against_the_chain_before_refusing() {
        let mut server = mockito::Server::new_async().await;
        let (_bitmap, reads) = mock_bitmap_sequence(&mut server, vec![(2, Vec::new())]);

        let mut state = sender_state(&server.url());
        state.instance_pda = Some(Pubkey::new_unique());
        state.cached_generation = None;
        let nonce = 2 * NONCES_PER_GENERATION;

        let result = build_release(&mut state, nonce).await;

        assert!(result.is_ok(), "the nonce is inside the current window");
        assert!(state.in_flight_withdrawals.contains(&nonce));
        assert_eq!(reads.load(Ordering::SeqCst), 1);
        assert_eq!(state.cached_generation, Some(2));
    }

    /// The steady state: an agreeing cache costs no round trip.
    #[tokio::test]
    async fn warm_matching_cache_sends_without_reading_the_bitmap() {
        let mut server = mockito::Server::new_async().await;
        let (_bitmap, reads) = mock_bitmap_sequence(&mut server, vec![(0, Vec::new())]);

        let mut state = sender_state(&server.url());
        state.instance_pda = Some(Pubkey::new_unique());
        state.cached_generation = Some(0);

        let result = build_release(&mut state, 7).await;

        assert!(result.is_ok());
        assert!(state.in_flight_withdrawals.contains(&7));
        assert_eq!(
            reads.load(Ordering::SeqCst),
            0,
            "a warm matching cache must cost no RPC"
        );
    }

    /// A nonce whose window has not opened is held for the rotation that opens
    /// it, and the row records that wait: the queue dies with the process, so
    /// the database has to be the copy a restart can still find.
    #[tokio::test]
    async fn nonce_ahead_of_the_window_is_queued_for_the_rotation() {
        let mut server = mockito::Server::new_async().await;
        let _bitmap = mock_bitmap_account(&mut server, 0, &[]);

        let mock = mock_with_processing_row(RELEASE_TXID);
        let mut state = sender_state_with_storage(&server.url(), mock.clone());
        state.instance_pda = Some(Pubkey::new_unique());
        let nonce = NONCES_PER_GENERATION;

        let result = build_release(&mut state, nonce).await;

        assert!(matches!(
            result,
            Err(OperatorError::Program(ProgramError::GenerationMismatch {
                nonce_generation: 1,
                chain_generation: 0,
                ..
            }))
        ));
        assert_eq!(state.rotation_retry_queue.len(), 1);
        assert_eq!(
            state.rotation_retry_queue[0].0.withdrawal_nonce,
            Some(nonce)
        );
        assert_eq!(
            row_status(&mock, RELEASE_TXID),
            Some(TransactionStatus::Parked),
            "a queued release must be durable, not only in memory"
        );
        assert!(
            !state.in_flight_withdrawals.contains(&nonce),
            "a queued withdrawal must not hold the rotation barrier"
        );
    }

    /// The queue is only safe because a parked row backs every entry. A park the
    /// database refused leaves nothing behind it, so queueing anyway would
    /// recreate the in-memory-only state while looking like it was fixed.
    #[tokio::test]
    async fn a_release_whose_park_was_refused_is_not_queued() {
        let mut server = mockito::Server::new_async().await;
        let _bitmap = mock_bitmap_account(&mut server, 0, &[]);

        // No row at all, so the park CAS finds nothing to move.
        let mut state = sender_state_with_storage(&server.url(), MockStorage::new());
        state.instance_pda = Some(Pubkey::new_unique());

        let _ = build_release(&mut state, NONCES_PER_GENERATION).await;

        assert!(
            state.rotation_retry_queue.is_empty(),
            "an unparked release must be left to recovery, not held in memory"
        );
    }

    /// An unreadable park is not a park. Absence of an error says nothing about
    /// whether the row moved, so only a positive answer may queue.
    #[tokio::test]
    async fn a_release_whose_park_errored_is_not_queued() {
        let mut server = mockito::Server::new_async().await;
        let _bitmap = mock_bitmap_account(&mut server, 0, &[]);

        let mock = mock_with_processing_row(RELEASE_TXID);
        mock.set_should_fail("try_park_processing", true);
        let mut state = sender_state_with_storage(&server.url(), mock);
        state.instance_pda = Some(Pubkey::new_unique());

        let _ = build_release(&mut state, NONCES_PER_GENERATION).await;

        assert!(
            state.rotation_retry_queue.is_empty(),
            "an unconfirmed park must not queue financial work"
        );
    }

    /// The queue and the parked rows are one fact recorded twice, so the
    /// invariant has to hold across every producer rather than per call site.
    /// Both feed the same queue, and a rotation that never lands leaves whatever
    /// they queued sitting there until a restart is the only thing left to find
    /// it.
    #[tokio::test]
    async fn every_queued_release_has_a_parked_row_behind_it() {
        let mut server = mockito::Server::new_async().await;
        let _bitmap = mock_bitmap_account(&mut server, 0, &[]);

        // One row for the pre-send hold, one for the on-chain refusal, and one
        // whose park is refused because it is not there at all.
        const REFUSED_ROW: i64 = 2;
        let mock = mock_with_processing_row(RELEASE_TXID);
        push_processing_row(&mock, REFUSED_ROW);
        let mut state = sender_state_with_storage(&server.url(), mock.clone());
        state.instance_pda = Some(Pubkey::new_unique());

        let ahead = NONCES_PER_GENERATION;
        let _ = build_release(&mut state, ahead).await;

        let instruction = InstructionWithSigners {
            instructions: vec![],
            fee_payer: Pubkey::new_unique(),
            signers: vec![],
            compute_budget: None,
            compute_unit_price: None,
        };
        let (tx, _rx) = mpsc::channel(10);
        for (transaction_id, nonce) in [(REFUSED_ROW, ahead + 1), (404, ahead + 2)] {
            state.cached_generation = None;
            let ctx = TransactionContext {
                kind: TransactionKind::ReleaseFunds,
                transaction_id: Some(transaction_id),
                withdrawal_nonce: Some(nonce),
                trace_id: Some("t".to_string()),
            };
            handle_nonce_outside_generation(
                &mut state,
                &ctx,
                Signature::new_unique(),
                instruction.clone(),
                &tx,
            )
            .await;
        }

        assert_eq!(
            state.rotation_retry_queue.len(),
            2,
            "the row that could not be parked must not be queued"
        );
        for (ctx, _) in &state.rotation_retry_queue {
            let transaction_id = ctx.transaction_id.expect("a queued release names its row");
            assert_eq!(
                row_status(&mock, transaction_id),
                Some(TransactionStatus::Parked),
                "queued release {transaction_id} has no parked row behind it"
            );
        }
    }

    /// A nonce the chain rotated past is not queued: no rotation brings it back.
    #[tokio::test]
    async fn nonce_behind_the_window_is_refused_and_not_queued() {
        let mut server = mockito::Server::new_async().await;
        let _bitmap = mock_bitmap_account(&mut server, 1, &[]);

        let mut state = sender_state(&server.url());
        state.instance_pda = Some(Pubkey::new_unique());

        let result = build_release(&mut state, 0).await;

        assert!(matches!(
            result,
            Err(OperatorError::Program(ProgramError::GenerationMismatch {
                nonce_generation: 0,
                chain_generation: 1,
                ..
            }))
        ));
        assert!(state.rotation_retry_queue.is_empty());
        assert!(!state.in_flight_withdrawals.contains(&0));
    }

    /// The cache tracks the chain, never the traffic it happens to be handed.
    #[tokio::test]
    async fn cache_never_advances_past_what_the_chain_reported() {
        let mut server = mockito::Server::new_async().await;
        let _bitmap = mock_bitmap_account(&mut server, 0, &[]);

        let mut state = sender_state(&server.url());
        state.instance_pda = Some(Pubkey::new_unique());
        state.cached_generation = Some(0);

        let result = build_release(&mut state, 5 * NONCES_PER_GENERATION).await;

        assert!(result.is_err());
        assert_eq!(
            state.cached_generation,
            Some(0),
            "only the chain moves the cache"
        );
    }

    /// An unreadable bitmap cannot refuse anything; the program still judges.
    #[tokio::test]
    async fn unreadable_bitmap_sends_rather_than_withholds() {
        let mut server = mockito::Server::new_async().await;
        let _down = mock_bitmap_read_failure(&mut server);

        let mut state = sender_state(&server.url());
        state.instance_pda = Some(Pubkey::new_unique());
        let nonce = NONCES_PER_GENERATION;

        let result = build_release(&mut state, nonce).await;

        assert!(result.is_ok(), "a failed read must not withhold a release");
        assert!(state.in_flight_withdrawals.contains(&nonce));
        assert_eq!(state.cached_generation, None, "nothing was learned");
    }

    #[tokio::test]
    async fn handle_release_funds_marks_nonce_in_flight() {
        let mut state = sender_state("http://localhost:8899");
        state.cached_generation = Some(0);
        let bwn = Box::new(ReleaseFundsBuilderWithNonce {
            builder: make_release_funds_builder(),
            nonce: 0,
            transaction_id: 42,
            trace_id: "trace-42".to_string(),
            remint_info: Some(make_test_remint_info(42, "trace-42")),
        });

        let ix = state
            .handle_release_funds_transaction(
                bwn,
                Pubkey::new_unique(),
                vec![],
                Some(5000),
                Some(200_000),
            )
            .await
            .expect("builder is complete");

        assert!(state.in_flight_withdrawals.contains(&0));
        assert_eq!(ix.instructions.len(), 1);
        assert_eq!(ix.compute_unit_price, Some(5000));
        assert_eq!(ix.compute_budget, Some(200_000));
    }
}
