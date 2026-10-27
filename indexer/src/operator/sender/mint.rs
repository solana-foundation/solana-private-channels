use crate::error::ReconciliationError;
use crate::operator::utils::instruction_util::{
    mint_idempotency_memo, remint_idempotency_memo, InitializeMintBuilder, TransactionBuilder,
    TransactionKind,
};
use crate::operator::utils::transaction_util::{check_transaction_status, ConfirmationResult};
use crate::operator::{
    sign_and_send_transaction, RpcClientWithRetry, SignerUtil, SourceEventId,
    MINT_IDEMPOTENCY_MEMO_PREFIX, REMINT_IDEMPOTENCY_MEMO_PREFIX,
};
use crate::storage::common::amount::TokenAmount;
use crate::storage::common::models::DbConsumedMint;
use crate::storage::{DbTransaction, Storage, TransactionType};
use serde_json::Value;
use solana_commitment_config::CommitmentConfig;
use solana_keychain::SolanaSigner;
use solana_sdk::program_option::COption;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_transaction_status::parse_instruction::ParsedInstruction;
use solana_transaction_status::{
    EncodedTransaction, UiCompiledInstruction, UiInstruction, UiMessage, UiParsedInstruction,
    UiParsedMessage, UiPartiallyDecodedInstruction, UiRawMessage,
};
use spl_associated_token_account::get_associated_token_address_with_program_id;
use spl_token_2022::extension::{permanent_delegate::get_permanent_delegate, StateWithExtensions};
use spl_token_2022::state::Mint;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use tracing::{error, info, warn};

use super::types::{InstructionWithSigners, SenderState};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct MintToFields {
    mint: Pubkey,
    recipient_ata: Pubkey,
    mint_authority: Pubkey,
    token_program: Pubkey,
    amount: u64,
}

/// Verdict from `try_jit_mint_initialization`. The caller in
/// `transaction.rs` matches on this to decide whether to re-issue the mint,
/// quarantine the deposit to ManualReview, or re-arm it for another attempt.
/// The ManualReview payload is an operator-visible `error_message` and is
/// constructed in full here (with the literal `"Mint instruction failed after
/// JIT: "` prefix) so `drill_1` can grep the runbook-dispatch substrings in a
/// single source file. The Transient payload only ever reaches the logs.
pub enum JitOutcome {
    /// Mint is correctly initialized with the operator's admin as
    /// `mint_authority`. Caller should retry the supplied instruction.
    Retry(InstructionWithSigners),

    /// Mint exists on-chain but in a state the operator cannot fix by
    /// re-issuing `mint_to` (wrong authority, corrupt data, or post-init
    /// inconsistency). Caller routes to `ManualReview`.
    ManualReview(String),

    /// No site behind this proves the mint unusable, so the caller re-arms
    /// the deposit under a cap; the re-mint is gated on its stored signature.
    Transient(String),
}

/// Outcome of decoding raw mint account bytes and comparing the embedded
/// `mint_authority` and permanent delegate to the operator's. Drives the
/// `JitOutcome` branching in `try_jit_mint_initialization`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum AuthorityCheck {
    /// Mint decoded; `is_initialized = true`; `mint_authority` equals the
    /// supplied expected authority. Retry is safe.
    Match,
    /// Mint decoded; `is_initialized = true`; `mint_authority` does NOT
    /// equal the expected authority. Holds the actual authority for log /
    /// error context. Quarantine to ManualReview.
    Mismatch(Pubkey),
    /// Authority matches but the mint's permanent delegate is missing, revoked
    /// or another key, so the operator would mint without the expected seize
    /// authority. Holds the actual delegate. Quarantine to ManualReview.
    DelegateMismatch(Option<Pubkey>),
    /// Mint decoded but `is_initialized = false`. The account is allocated
    /// and Token-2022-owned but not yet a mint. JIT proceeds to
    /// InitializeMint.
    Uninitialized,
    /// Decode failed: data length wrong, COption discriminant invalid, or
    /// other corruption. Quarantine to ManualReview — the operator cannot
    /// recover this without engineering intervention.
    CorruptData,
}

/// Decode `data` as a Token-2022 `Mint` and classify it relative to the
/// supplied expected authority and permanent delegate.
///
/// `unpack` refuses an uninitialized base with `UninitializedAccount`, which
/// is mapped to `Uninitialized` so the legitimate "mint allocated but not yet
/// initialized" state is not conflated with corruption.
///
/// Returns:
/// - `Match` if decoded, initialized, `mint_authority` equals
///   `expected_authority` and the permanent delegate equals `expected_delegate`.
/// - `Mismatch(actual)` if the authority differs. A mint with no authority
///   is `Mismatch(Pubkey::default())`: the operator cannot `mint_to` it either way.
/// - `DelegateMismatch(actual)` if the authority matches but the delegate does
///   not (missing extension, revoked, or another key).
/// - `Uninitialized` if the account is allocated but not yet a mint.
/// - `CorruptData` for any other decode failure.
pub(super) fn decode_and_check_authority(
    data: &[u8],
    expected_authority: &Pubkey,
    expected_delegate: &Pubkey,
) -> AuthorityCheck {
    let mint = match StateWithExtensions::<Mint>::unpack(data) {
        Ok(m) => m,
        Err(solana_sdk::program_error::ProgramError::UninitializedAccount) => {
            return AuthorityCheck::Uninitialized
        }
        Err(_) => return AuthorityCheck::CorruptData,
    };
    match mint.base.mint_authority {
        COption::Some(actual) if actual == *expected_authority => {}
        COption::Some(actual) => return AuthorityCheck::Mismatch(actual),
        COption::None => return AuthorityCheck::Mismatch(Pubkey::default()),
    }
    match get_permanent_delegate(&mint) {
        Some(actual) if actual == *expected_delegate => AuthorityCheck::Match,
        actual => AuthorityCheck::DelegateMismatch(actual),
    }
}

// Operator-visible error_message literals constructed in this file.
// Pinned by `drill_1_error_message_contracts_present_in_source` and the
// runbook dispatch tables in `docs/runbooks/deposit_manual_review.md`.
const MR_AUTHORITY_MISMATCH_PRECHECK: &str =
    "Mint instruction failed after JIT: mint_authority mismatch — admin key rotated or mint owned by another authority";
const MR_AUTHORITY_MISMATCH_POSTINIT: &str =
    "Mint instruction failed after JIT: mint_authority mismatch — race with concurrent admin rotation during InitializeMint";
const MR_CORRUPT_MINT_STATE: &str =
    "Mint instruction failed after JIT: corrupt mint state on-chain — decode failed";
const MR_PERMANENT_DELEGATE_MISMATCH: &str =
    "Mint instruction failed after JIT: permanent delegate mismatch, the mint lacks the configured seize authority";

/// Attempt JIT mint initialization. Returns a `JitOutcome` verdict for the
/// caller to dispatch (Retry / ManualReview / Transient).
pub(super) async fn try_jit_mint_initialization(
    state: &mut SenderState,
    transaction_id: i64,
    instruction: InstructionWithSigners,
) -> JitOutcome {
    // 1. Get cached builder + extract mint.
    let Some(builder) = state.mint_builders.get(&transaction_id).cloned() else {
        return JitOutcome::Transient(format!(
            "no cached MintToBuilder for transaction_id {}",
            transaction_id
        ));
    };
    let Some(mint) = builder.get_mint() else {
        return JitOutcome::Transient(format!(
            "MintToBuilder for transaction_id {} is missing mint pubkey",
            transaction_id
        ));
    };

    let admin_pubkey = SignerUtil::admin_signer().pubkey();
    let delegate = SignerUtil::permanent_delegate();

    // 2. Pre-check on-chain mint state.
    match state.rpc_client.get_account_data(&mint).await {
        Ok(data) => match decode_and_check_authority(&data, &admin_pubkey, &delegate) {
            AuthorityCheck::Match => return JitOutcome::Retry(instruction),
            AuthorityCheck::Mismatch(actual) => {
                warn!(
                    "JIT pre-check: mint {} initialized with authority {} (expected admin {})",
                    mint, actual, admin_pubkey
                );
                return JitOutcome::ManualReview(MR_AUTHORITY_MISMATCH_PRECHECK.to_string());
            }
            AuthorityCheck::DelegateMismatch(actual) => {
                warn!(
                    "JIT pre-check: mint {} has permanent delegate {:?} (expected {})",
                    mint, actual, delegate
                );
                return JitOutcome::ManualReview(MR_PERMANENT_DELEGATE_MISMATCH.to_string());
            }
            AuthorityCheck::CorruptData => {
                warn!(
                    "JIT pre-check: mint {} bytes do not decode as a Token-2022 mint",
                    mint
                );
                return JitOutcome::ManualReview(MR_CORRUPT_MINT_STATE.to_string());
            }
            AuthorityCheck::Uninitialized => {
                info!(
                    "Mint {} not initialized on PrivateChannel - attempting JIT initialization",
                    mint
                );
                // fall through to init path
            }
        },
        Err(e) => {
            warn!(
                "RPC error checking mint {} - assuming it doesn't exist: {}",
                mint, e
            );
            // Proceed with JIT as fail-safe
        }
    }

    // Look up the mint's decimals.
    //
    // `get_mint_decimals` reads the DB and falls back to a source-chain
    // RPC fetch if it has no row. That fallback would be dangerous on
    // its own: it would let any source-chain mint be resolved
    // here, initialized on the private channel, and minted into a
    // user's account.
    //
    // It's safe in this position because the deposit processor
    // already refuses to forward a deposit whose mint was not in
    // allowed status at the deposit's slot (see
    // `assert_mint_allowed_at_slot` in `process_deposit_funds`). By the
    // time execution reaches this point, the mint is known-allowed.
    let Ok(decimals) = state.mint_cache.get_mint_decimals(&mint).await else {
        error!("Mint {} decimals unavailable", mint);
        return JitOutcome::Transient(format!("mint decimals unavailable: {}", mint));
    };

    info!("Found mint decimals: {} for {}", decimals, mint);

    // 4. Build InitializeMint transaction.
    let init_mint_builder = InitializeMintBuilder::new(
        mint,
        decimals,
        admin_pubkey,
        state.mint_cache.get_private_channel_token_program(),
        admin_pubkey,
        delegate,
    );

    let init_tx_builder = TransactionBuilder::InitializeMint(Box::new(init_mint_builder));

    // 5. Convert to instruction.
    let init_instruction = match state
        .handle_transaction_builder(init_tx_builder.clone())
        .await
    {
        Ok(ix) => ix,
        Err(e) => {
            error!("Failed to build InitializeMint instruction: {}", e);
            return JitOutcome::Transient(format!(
                "Failed to build InitializeMint instruction: {}",
                e
            ));
        }
    };

    // 6. Send transaction.
    info!("Sending InitializeMint transaction for mint {}", mint);
    let sig = match sign_and_send_transaction(
        state.rpc_client.clone(),
        init_instruction,
        init_tx_builder.retry_policy(),
    )
    .await
    {
        Ok((s, _, _)) => s,
        Err(e) => {
            error!("Failed to send InitializeMint transaction: {}", e);
            return JitOutcome::Transient(format!(
                "Failed to send InitializeMint transaction: {}",
                e
            ));
        }
    };

    // 7. Wait for confirmation.
    let result = match check_transaction_status(
        state.rpc_client.clone(),
        &sig,
        TransactionKind::InitializeMint,
        CommitmentConfig::confirmed(),
        &init_tx_builder.extra_error_checks_policy(),
        state.confirmation_poll_interval_ms,
    )
    .await
    {
        Ok(r) => r,
        Err(e) => {
            error!("Failed to check InitializeMint status: {}", e);
            return JitOutcome::Transient(format!("Failed to check InitializeMint status: {}", e));
        }
    };

    // 8. Branch on confirmation result.
    match result {
        ConfirmationResult::Confirmed => {
            // `Confirmed` covers both a successful InitializeMint and the
            // `AccountAlreadyInitialized` race, which the extra error check
            // on this builder remaps to `Confirmed` without an RPC re-check.
            info!("InitializeMint transaction confirmed: {}", sig);

            // Re-fetch and check authority — catches the race where another
            // party initialized the same mint with a different authority
            // during our send window.
            let check = match state.rpc_client.get_account_data(&mint).await {
                Ok(data) => decode_and_check_authority(&data, &admin_pubkey, &delegate),
                Err(_) => {
                    mint_authority_check_with_backoff(
                        &state.rpc_client,
                        &mint,
                        &admin_pubkey,
                        &delegate,
                    )
                    .await
                }
            };
            jit_verdict(check, instruction, &mint, None)
        }
        _ => {
            // Fallback for unknown failures (network blips, timeouts): re-read
            // the mint on-chain with backoff in case it was initialized out-of-band.
            let check = mint_authority_check_with_backoff(
                &state.rpc_client,
                &mint,
                &admin_pubkey,
                &delegate,
            )
            .await;
            jit_verdict(check, instruction, &mint, Some(&result))
        }
    }
}

/// Map an `AuthorityCheck` to a `JitOutcome`.
///
/// `fallback_result`:
/// - `None` → post-`Confirmed` context (InitializeMint succeeded; we're
///   re-checking the on-chain state to catch the post-init authority race).
/// - `Some(result)` → fallback context (the InitializeMint poll did NOT
///   return `Confirmed` — timeout / RPC error — and we're re-reading the
///   mint with backoff in case it landed out-of-band).
///
/// The two contexts only diverge in two arms:
/// - `Match`: the fallback path logs an info "treating-as-success" message
///   so operators can see the race-recovery happen; post-init is silent.
/// - `Uninitialized`: post-init treats this as an RPC inconsistency
///   (InitializeMint said Confirmed but the mint isn't there); fallback
///   treats it as the canonical "InitializeMint could not be confirmed".
///   Both are Transient: neither proves the mint is unusable, so the
///   deposit is re-armed rather than ended.
fn jit_verdict(
    check: AuthorityCheck,
    instruction: InstructionWithSigners,
    mint: &Pubkey,
    fallback_result: Option<&ConfirmationResult>,
) -> JitOutcome {
    match check {
        AuthorityCheck::Match => {
            if let Some(result) = fallback_result {
                info!(
                    "InitializeMint not confirmed cleanly (result={:?}), but mint {} reads as \
                     initialized with admin authority — treating JIT as success",
                    result, mint
                );
            }
            JitOutcome::Retry(instruction)
        }
        AuthorityCheck::Mismatch(actual) => {
            warn!(
                "JIT: mint {} initialized with authority {} (expected admin)",
                mint, actual
            );
            JitOutcome::ManualReview(MR_AUTHORITY_MISMATCH_POSTINIT.to_string())
        }
        AuthorityCheck::DelegateMismatch(actual) => {
            warn!(
                "JIT: mint {} has permanent delegate {:?} (expected the configured one)",
                mint, actual
            );
            JitOutcome::ManualReview(MR_PERMANENT_DELEGATE_MISMATCH.to_string())
        }
        AuthorityCheck::CorruptData => {
            warn!(
                "JIT: mint {} bytes do not decode as a Token-2022 mint",
                mint
            );
            JitOutcome::ManualReview(MR_CORRUPT_MINT_STATE.to_string())
        }
        AuthorityCheck::Uninitialized => match fallback_result {
            Some(result) => {
                error!(
                    "InitializeMint transaction could not be confirmed: {:?}",
                    result
                );
                JitOutcome::Transient(
                    "InitializeMint transaction could not be confirmed".to_string(),
                )
            }
            None => {
                error!(
                    "JIT post-init: InitializeMint confirmed but mint {} reads as uninitialized",
                    mint
                );
                JitOutcome::Transient(format!(
                    "InitializeMint confirmed but mint {} reads as uninitialized — RPC inconsistency",
                    mint
                ))
            }
        },
    }
}

/// Read the mint on-chain with backoff, returning the `AuthorityCheck`
/// from the first successful decode. Absorbs read-RPC lag after a racing
/// InitializeMint. On exhausted attempts with no successful decode,
/// returns `Uninitialized` (the most conservative "I couldn't confirm
/// it's there" reading — caller maps this to Transient on the
/// fallback path).
async fn mint_authority_check_with_backoff(
    rpc_client: &RpcClientWithRetry,
    mint: &Pubkey,
    expected_authority: &Pubkey,
    expected_delegate: &Pubkey,
) -> AuthorityCheck {
    const ATTEMPTS: u32 = 4;
    const BACKOFF_MS: u64 = 250;

    let mut last_check = AuthorityCheck::Uninitialized;
    for attempt in 0..ATTEMPTS {
        match rpc_client.get_account_data(mint).await {
            Ok(data) => {
                let check =
                    decode_and_check_authority(&data, expected_authority, expected_delegate);
                if !matches!(check, AuthorityCheck::Uninitialized) {
                    return check;
                }
                last_check = check;
            }
            Err(e) => {
                if attempt + 1 == ATTEMPTS {
                    warn!(
                        "RPC error re-checking mint {} after failed JIT init: {}",
                        mint, e
                    );
                }
            }
        }
        if attempt + 1 < ATTEMPTS {
            tokio::time::sleep(tokio::time::Duration::from_millis(BACKOFF_MS)).await;
        }
    }
    last_check
}

/// Which serviced operation a consumed channel mint corresponds to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConsumedMintKind {
    /// A deposit `mint_to`
    Deposit,
    /// A withdrawal remint `mint_to`
    Remint,
}

/// A channel mint the authority signed, with the `MintTo` fields it executed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConsumedMint {
    pub signature: Signature,
    pub kind: ConsumedMintKind,
    pub mint: Pubkey,
    pub recipient_ata: Pubkey,
    pub token_program: Pubkey,
    pub amount: u64,
}

/// Set of source events the PrivateChannel has already serviced, keyed by their durable
/// source-event-id. Built once, before a resync wipe, so the rebuild can reconcile each
/// row to its terminal state.
pub type ConsumedSet = HashMap<SourceEventId, ConsumedMint>;

/// Whose channel history a consumed-set scan lists, and which of its transactions count.
#[derive(Clone, Copy, Debug)]
enum Scope {
    /// Mints the authority signed.
    Signer(Pubkey),
    /// Mints on this receipt mint, by whichever key held its mint authority at the time.
    Mint(Pubkey),
}

impl Scope {
    fn address(&self) -> &Pubkey {
        match self {
            Scope::Signer(address) | Scope::Mint(address) => address,
        }
    }
}

/// Enumerate every idempotency-memo'd mint the `authority` has confirmed on the channel
/// into a `ConsumedSet`. Transactions the authority did not sign are skipped.
///
/// Fails closed on any RPC/pagination error, and on an authority-signed tx that cannot be
/// authenticated: more than one marker, a legacy source-event-id, no exact Memo
/// instruction, or not exactly one `MintTo` by the authority. Also fails on two distinct
/// successful mints for one source event.
pub async fn enumerate_consumed_mints(
    rpc: &RpcClientWithRetry,
    authority: &Pubkey,
    page_limit: usize,
) -> Result<ConsumedSet, String> {
    let mut set = ConsumedSet::new();
    collect(
        rpc,
        Scope::Signer(*authority),
        page_limit,
        &HashSet::new(),
        None,
        &mut set,
    )
    .await?;
    Ok(set)
}

/// Add every memo'd `MintTo` on `mint` to `set` whoever signed it, so keys rotated out of the
/// mint authority count too, and return how many were added. Fails closed like
/// `enumerate_consumed_mints`; on `Err` the set may be partly extended and must be discarded.
pub async fn extend_with_mint_history(
    rpc: &RpcClientWithRetry,
    mint: &Pubkey,
    page_limit: usize,
    set: &mut ConsumedSet,
) -> Result<usize, String> {
    collect(
        rpc,
        Scope::Mint(*mint),
        page_limit,
        &HashSet::new(),
        None,
        set,
    )
    .await
    .map(|(added, _)| added)
}

/// Wait for the channel address index unless the node indexes addresses with the block.
pub(crate) async fn ensure_address_index(channel_rpc: &RpcClientWithRetry) -> Result<(), String> {
    match channel_rpc.get_address_index_slot().await {
        // Only core indexes addresses after the block lands; a node without the
        // method writes its index with the block, so its history is complete.
        Err(e) if crate::operator::utils::rpc_util::is_method_not_found(&e) => {
            warn!("Channel RPC has no getAddressIndexSlot; its address index is synchronous");
            Ok(())
        }
        _ => {
            wait_for_address_index(
                channel_rpc,
                INDEX_CATCH_UP_BUDGET,
                std::time::Duration::from_millis(500),
            )
            .await
        }
    }
}

/// How long boot waits for the channel address index to reach the newest block.
pub(crate) const INDEX_CATCH_UP_BUDGET: std::time::Duration = std::time::Duration::from_secs(30);

/// Wait until the channel address index covers the block that was newest at the first
/// read, so its history holds every mint landed before that point. A pinned target, not
/// `getSlot` or a moving tip, so idle ticks and live traffic cannot outrun it.
pub(crate) async fn wait_for_address_index(
    channel_rpc: &RpcClientWithRetry,
    budget: std::time::Duration,
    poll: std::time::Duration,
) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + budget;
    let mut target = None;
    loop {
        // One poll interval at least, so a zero budget still gets a real read.
        let remaining = deadline
            .saturating_duration_since(tokio::time::Instant::now())
            .max(poll);
        let reply =
            match tokio::time::timeout(remaining, channel_rpc.get_address_index_slot()).await {
                Ok(reply) => reply.map_err(|e| e.to_string()),
                Err(_) => Err(format!("no reply within {remaining:?}")),
            };
        let (watermark, latest_block) = reply.map_err(|e| {
            format!(
                "channel address index progress unreadable: {e}. The channel RPC must be \
                 the operators' read node running a core that serves getAddressIndexSlot; \
                 see docs/runbooks/resync_consumed_mint_mismatch.md"
            )
        })?;
        let target = *target.get_or_insert(latest_block);
        if watermark >= target {
            info!(watermark, target, "Channel address index is caught up");
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "channel address index is at slot {watermark}, behind block {target}, \
                 the newest when the wait started, so its history may miss serviced mints. \
                 The write node must be running and the channel RPC must be the operators' \
                 read node; retry once it catches up, see \
                 docs/runbooks/resync_consumed_mint_mismatch.md"
            ));
        }
        tokio::time::sleep(poll).await;
    }
}

/// What a claimed row's source event already did on the channel.
#[derive(Debug, Clone)]
pub(crate) enum ConsumedLookup {
    /// No channel mint for this event, so it may be served.
    Miss,
    /// A channel mint paid exactly this row.
    Hit(ConsumedMint),
    /// A channel mint carries this event's marker but pays something else.
    Mismatch(String),
}

/// Channel mints the indexer DB may have forgotten, consulted before every mint, remint and
/// release. After an indexer restore the DB re-creates rows the channel already paid, and
/// only the channel's own mint history (which no DB restore rewinds) can tell.
pub(crate) struct ConsumedSetCache {
    set: ConsumedSet,
    walked: HashSet<Pubkey>,
    page_limit: usize,
    /// The rows this operator settles. Its walk stops only at signatures it wrote itself:
    /// the other operator may run first after a restore and land newer mints of its own.
    role: TransactionType,
}

impl ConsumedSetCache {
    /// Walk every mint the DB knows. Refusing here keeps the operator from serving rows the
    /// channel may already have paid.
    pub(crate) async fn build_at_boot(
        rpc: &RpcClientWithRetry,
        storage: &Storage,
        page_limit: usize,
        role: TransactionType,
    ) -> Result<Self, String> {
        let mut cache = Self {
            set: ConsumedSet::new(),
            walked: HashSet::new(),
            page_limit,
            role,
        };
        let recorded = storage
            .get_consumed_mints()
            .await
            .map_err(|e| format!("recorded channel mints unreadable: {e}"))?;
        for row in &recorded {
            let (event, consumed) = consumed_from_db(row)?;
            cache.set.insert(event, consumed);
        }
        let mints = storage
            .get_mint_addresses()
            .await
            .map_err(|e| format!("mint list unreadable: {e}"))?;
        for (mint, _) in mints {
            let mint = Pubkey::from_str(&mint)
                .map_err(|e| format!("mints table holds a bad address {mint}: {e}"))?;
            cache.walk_mint(rpc, storage, &mint).await?;
        }
        info!(
            mints = cache.walked.len(),
            entries = cache.set.len(),
            "Consumed set built from channel mint history"
        );
        Ok(cache)
    }

    /// The channel's record of `row`'s source event. A mint first named after boot (its
    /// `AllowMint` was indexed after the restore target) is walked here, once.
    pub(crate) async fn lookup(
        &mut self,
        rpc: &RpcClientWithRetry,
        storage: &Storage,
        row: &DbTransaction,
    ) -> Result<ConsumedLookup, String> {
        // A bad mint is refused by the build step that follows, with its own error.
        let Ok(mint) = Pubkey::from_str(&row.mint) else {
            return Ok(ConsumedLookup::Miss);
        };
        self.walk_mint(rpc, storage, &mint).await?;
        let Some(consumed) = self.set.get(&SourceEventId::from_row(row)) else {
            return Ok(ConsumedLookup::Miss);
        };
        Ok(match check_consumed_mint(consumed, row) {
            Ok(()) => ConsumedLookup::Hit(*consumed),
            Err(mismatch) => ConsumedLookup::Mismatch(mismatch.to_string()),
        })
    }

    /// Walk `mint`'s history newest first, down to the newest mint the DB already knows.
    /// A restore to time T leaves every mint before T known to the DB, so that range is
    /// exactly what the DB can have forgotten, and it is empty in normal operation.
    /// What the walk finds is recorded: closing a forgotten mint moves that bound above it.
    async fn walk_mint(
        &mut self,
        rpc: &RpcClientWithRetry,
        storage: &Storage,
        mint: &Pubkey,
    ) -> Result<(), String> {
        if self.walked.contains(mint) {
            return Ok(());
        }
        // A mint landed before the DB's snapshot whose row is not yet completed is processing
        // with a journal, which the reopened-row gate checks, so only newer ones matter here.
        let stop_at = storage
            .get_newest_known_mint_signatures(&mint.to_string(), self.role)
            .await
            .map_err(|e| format!("known mint signatures for {mint} unreadable: {e}"))?
            .iter()
            .map(|sig| {
                Signature::from_str(sig)
                    .map_err(|e| format!("DB holds a bad mint signature {sig} for {mint}: {e}"))
            })
            .collect::<Result<HashSet<_>, _>>()?;

        // Walked into a fresh set so a failed walk leaves nothing half-added.
        let mut found = ConsumedSet::new();
        let (_, met) = collect(
            rpc,
            Scope::Mint(*mint),
            self.page_limit,
            &stop_at,
            Some(match self.role {
                TransactionType::Deposit => ConsumedMintKind::Deposit,
                TransactionType::Withdrawal => ConsumedMintKind::Remint,
            }),
            &mut found,
        )
        .await?;
        if !stop_at.is_empty() && !met {
            self.accept_pruned_walk(rpc, mint, &stop_at).await?;
        }
        if !found.is_empty() {
            let rows: Vec<_> = found
                .iter()
                .map(|(event, consumed)| consumed_to_db(event, consumed))
                .collect();
            storage.insert_consumed_mints(&rows).await.map_err(|e| {
                format!("channel mints walked on {mint} could not be recorded: {e}")
            })?;
        }
        for (event, consumed) in found {
            match self.set.entry(event) {
                Entry::Vacant(vacant) => {
                    vacant.insert(consumed);
                }
                Entry::Occupied(occupied) if occupied.get().signature == consumed.signature => {}
                Entry::Occupied(occupied) => {
                    return Err(format!(
                        "source event {} has two successful channel mints, {} and {}",
                        occupied.key(),
                        occupied.get().signature,
                        consumed.signature
                    ));
                }
            }
        }
        self.walked.insert(*mint);
        Ok(())
    }

    /// The channel mint for event `id` on `mint`, walking `mint` first if needed.
    pub(crate) async fn lookup_event(
        &mut self,
        rpc: &RpcClientWithRetry,
        storage: &Storage,
        mint: &Pubkey,
        id: &SourceEventId,
    ) -> Result<Option<ConsumedMint>, String> {
        self.walk_mint(rpc, storage, mint).await?;
        Ok(self.set.get(id).copied())
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.set.len()
    }

    /// Missing the newest known mint proves a rewind only on a channel holding its whole
    /// history. After any `truncate` the miss looks like pruning and is accepted, so only the
    /// channel fence catches a rewind then, and only one behind the fence slot.
    async fn accept_pruned_walk(
        &self,
        rpc: &RpcClientWithRetry,
        mint: &Pubkey,
        stop_at: &HashSet<Signature>,
    ) -> Result<(), String> {
        let floor = rpc.get_first_available_block().await.map_err(|e| {
            format!(
                "the channel history of {mint} never reached the newest mint the DB knows \
                 ({stop_at:?}) and its first available block is unreadable: {e}"
            )
        })?;
        if floor == 0 {
            return Err(format!(
                "the channel history of {mint} never reached the newest mint the DB knows \
                 ({stop_at:?}): the channel was rewound"
            ));
        }
        warn!(
            %mint, floor,
            "Newest known mint is below the channel's pruning floor; the walk covers the retained history"
        );
        Ok(())
    }

    /// A cache that already walked `mints` and holds `entries`, for claim-path tests.
    #[cfg(test)]
    pub(crate) fn with_entries(
        mints: &[Pubkey],
        entries: Vec<(SourceEventId, ConsumedMint)>,
    ) -> Self {
        Self {
            set: entries.into_iter().collect(),
            walked: mints.iter().copied().collect(),
            page_limit: 1,
            role: TransactionType::Deposit,
        }
    }
}

fn consumed_to_db(event: &SourceEventId, consumed: &ConsumedMint) -> DbConsumedMint {
    DbConsumedMint {
        source_event_id: event.to_string(),
        signature: consumed.signature.to_string(),
        kind: match consumed.kind {
            ConsumedMintKind::Deposit => "deposit",
            ConsumedMintKind::Remint => "remint",
        }
        .to_string(),
        mint: consumed.mint.to_string(),
        recipient_ata: consumed.recipient_ata.to_string(),
        token_program: consumed.token_program.to_string(),
        amount: TokenAmount(consumed.amount),
    }
}

fn consumed_from_db(row: &DbConsumedMint) -> Result<(SourceEventId, ConsumedMint), String> {
    let bad = |field: &str| format!("recorded channel mint {} has a bad {field}", row.signature);
    let event = SourceEventId::from_encoded(&row.source_event_id).ok_or_else(|| bad("event"))?;
    let key = |value: &str, field: &str| Pubkey::from_str(value).map_err(|_| bad(field));
    let consumed = ConsumedMint {
        signature: Signature::from_str(&row.signature).map_err(|_| bad("signature"))?,
        kind: match row.kind.as_str() {
            "deposit" => ConsumedMintKind::Deposit,
            "remint" => ConsumedMintKind::Remint,
            _ => return Err(bad("kind")),
        },
        mint: key(&row.mint, "mint")?,
        recipient_ata: key(&row.recipient_ata, "recipient")?,
        token_program: key(&row.token_program, "token program")?,
        amount: row.amount.0,
    };
    Ok((event, consumed))
}

/// The boot-built consumed set shared by the processor and the remint worker, with the
/// channel endpoint a lazy walk reads from.
#[derive(Clone)]
pub struct ConsumedClaims {
    cache: std::sync::Arc<tokio::sync::Mutex<ConsumedSetCache>>,
    rpc: std::sync::Arc<RpcClientWithRetry>,
    /// Set once the address index has caught up. Boot with no mints skips the wait, so the
    /// first lazy walk does it and sees every mint landed before it.
    index_ready: std::sync::Arc<tokio::sync::OnceCell<()>>,
}

impl ConsumedClaims {
    pub(crate) fn new(
        cache: ConsumedSetCache,
        rpc: std::sync::Arc<RpcClientWithRetry>,
        index_ready: bool,
    ) -> Self {
        let ready = if index_ready {
            tokio::sync::OnceCell::new_with(Some(()))
        } else {
            tokio::sync::OnceCell::new()
        };
        Self {
            cache: std::sync::Arc::new(tokio::sync::Mutex::new(cache)),
            rpc,
            index_ready: std::sync::Arc::new(ready),
        }
    }

    async fn ensure_index_ready(&self) -> Result<(), String> {
        self.index_ready
            .get_or_try_init(|| ensure_address_index(&self.rpc))
            .await
            .map(|_| ())
    }

    pub(crate) async fn lookup(
        &self,
        storage: &Storage,
        row: &DbTransaction,
    ) -> Result<ConsumedLookup, String> {
        self.ensure_index_ready().await?;
        self.cache
            .lock()
            .await
            .lookup(&self.rpc, storage, row)
            .await
    }

    pub(crate) async fn lookup_event(
        &self,
        storage: &Storage,
        mint: &Pubkey,
        id: &SourceEventId,
    ) -> Result<Option<ConsumedMint>, String> {
        self.ensure_index_ready().await?;
        self.cache
            .lock()
            .await
            .lookup_event(&self.rpc, storage, mint, id)
            .await
    }
}

/// Walk the history of `scope`'s address and add each authenticated channel mint to `set`.
/// Stops at the first page holding a `stop_at` signature and reports whether it met one.
/// With `only`, entries carrying no marker of that kind are skipped before any fetch.
async fn collect(
    rpc: &RpcClientWithRetry,
    scope: Scope,
    page_limit: usize,
    stop_at: &HashSet<Signature>,
    only: Option<ConsumedMintKind>,
    set: &mut ConsumedSet,
) -> Result<(usize, bool), String> {
    let address = scope.address();
    let (signatures, met) = rpc
        .get_signatures_for_address_paginated(address, page_limit, stop_at, |status| {
            status.err.is_none()
                && status.memo.as_deref().is_some_and(|m| {
                    memo_markers(m)
                        .iter()
                        .any(|(kind, _)| only.is_none_or(|only| *kind == only))
                })
        })
        .await
        .map_err(|e| {
            format!("consumed-set enumeration failed listing signatures for {address}: {e}")
        })?;

    // An accepted signature would yield the same entry again, so it is not fetched twice.
    let mut accepted: HashSet<Signature> = set.values().map(|mint| mint.signature).collect();
    let (candidates, mut fetched, mut added) = (signatures.len(), 0usize, 0usize);
    for status in signatures {
        // The listing already dropped failed and marker-less entries.
        let markers = memo_markers(status.memo.as_deref().unwrap_or_default());

        let signature = Signature::from_str(&status.signature)
            .map_err(|e| format!("invalid signature {} from RPC: {e}", status.signature))?;
        if accepted.contains(&signature) {
            continue;
        }
        let transaction = rpc
            .get_transaction(&signature)
            .await
            .map_err(|e| format!("consumed-set enumeration failed fetching {signature}: {e}"))?;
        fetched += 1;
        let targets = mint_to_targets(&transaction);
        // History lists every tx that mentions the address, not only its mints.
        let counts = transaction_succeeded(&transaction)
            && match scope {
                Scope::Signer(authority) => transaction_signed_by(&transaction, &authority),
                // Reads the raw MintTo target, never the decoded authority, so a MintTo the
                // strict decode below cannot read aborts instead of being skipped.
                Scope::Mint(mint) => targets.contains(&mint),
            };
        if !counts {
            continue;
        }

        let &[(kind, encoded)] = markers.as_slice() else {
            return Err(format!(
                "channel mint {signature} passed the mint gate but carries {} idempotency \
                 markers; one mint cannot service them all",
                markers.len()
            ));
        };

        let Some(source_event_id) = SourceEventId::from_encoded(encoded) else {
            return Err(format!(
                "channel mint {} carries an idempotency memo that does not parse to a \
                 current-scheme source-event-id (legacy memo scheme); resync cannot reconcile \
                 across the memo cutover - see docs/runbooks/mint_memo_cutover.md",
                status.signature
            ));
        };

        let expected_memo = match kind {
            ConsumedMintKind::Deposit => mint_idempotency_memo(&source_event_id),
            ConsumedMintKind::Remint => remint_idempotency_memo(&source_event_id),
        };
        if !transaction_has_memo(&transaction, &expected_memo) {
            return Err(format!(
                "channel mint {signature} passed the mint gate but has no Memo \
                 instruction equal to {expected_memo}; cannot authenticate it"
            ));
        }

        let mint_tos = transaction_mint_tos(&transaction);
        let [mint_to] = mint_tos[..] else {
            return Err(format!(
                "channel mint {signature} passed the mint gate but has {} MintTo \
                 instructions, expected one",
                mint_tos.len()
            ));
        };
        match scope {
            Scope::Signer(authority) if mint_to.mint_authority != authority => {
                return Err(format!(
                    "channel mint {signature} mints under {} instead of the authority {authority}",
                    mint_to.mint_authority
                ));
            }
            Scope::Mint(mint) if targets.len() != 1 || mint_to.mint != mint => {
                return Err(format!(
                    "channel mint {signature} passed the mint gate on {mint} but has {} \
                     MintTo instructions and decodes one on {}; cannot authenticate it",
                    targets.len(),
                    mint_to.mint
                ));
            }
            _ => {}
        }

        // The operator re-signs only after proving the earlier attempt dead, so two
        // successful mints for one event are a double issuance, never a harmless resend.
        match set.entry(source_event_id) {
            Entry::Vacant(vacant) => {
                vacant.insert(ConsumedMint {
                    signature,
                    kind,
                    mint: mint_to.mint,
                    recipient_ata: mint_to.recipient_ata,
                    token_program: mint_to.token_program,
                    amount: mint_to.amount,
                });
                accepted.insert(signature);
                added += 1;
            }
            Entry::Occupied(occupied) if occupied.get().signature == signature => {}
            Entry::Occupied(occupied) => {
                return Err(format!(
                    "source event {} has two successful channel mints, {} and {signature}; \
                     one event may be minted once, see \
                     docs/runbooks/resync_consumed_mint_mismatch.md",
                    occupied.key(),
                    occupied.get().signature
                ));
            }
        }
    }
    info!(
        %address,
        candidates,
        fetched,
        added,
        "Scanned channel history for the consumed-set"
    );
    Ok((added, met))
}

/// Check that `consumed_mint` paid this row: a marker of the row's kind, the row's mint and
/// amount, into the ATA of the deposit recipient or, for a remint, the withdrawal
/// initiator. Unparseable row keys never match. Shared by the resync rebuild
/// and the operator's claim-time consumed-set check.
pub(crate) fn check_consumed_mint(
    consumed_mint: &ConsumedMint,
    transaction: &DbTransaction,
) -> Result<(), ReconciliationError> {
    let reason = match (consumed_mint.kind, transaction.transaction_type) {
        (ConsumedMintKind::Deposit, TransactionType::Deposit) => {
            field_differences(consumed_mint, transaction, &transaction.recipient)
        }
        (ConsumedMintKind::Remint, TransactionType::Withdrawal) => {
            field_differences(consumed_mint, transaction, &transaction.initiator)
        }
        (kind, transaction_type) => Some(format!(
            "kind: channel {kind:?} marker, row {transaction_type:?}"
        )),
    };

    match reason {
        None => Ok(()),
        Some(reason) => Err(ReconciliationError::ConsumedMintMismatch {
            source_event_id: SourceEventId::from_row(transaction).to_string(),
            source_signature: transaction.signature.clone(),
            channel_signature: consumed_mint.signature.to_string(),
            reason,
        }),
    }
}

/// Each field where `consumed_mint` differs from the row paid into `owner`'s ATA, as
/// `field: channel X, row Y`, or `None` when they all agree.
pub(crate) fn field_differences(
    consumed_mint: &ConsumedMint,
    transaction: &DbTransaction,
    owner: &str,
) -> Option<String> {
    let (Ok(owner), Ok(mint)) = (Pubkey::from_str(owner), Pubkey::from_str(&transaction.mint))
    else {
        return Some(format!(
            "row owner {owner} or mint {} is not a pubkey",
            transaction.mint
        ));
    };
    let expected_ata =
        get_associated_token_address_with_program_id(&owner, &mint, &consumed_mint.token_program);

    let mut differences = Vec::new();
    if consumed_mint.mint != mint {
        differences.push(format!("mint: channel {}, row {mint}", consumed_mint.mint));
    }
    if consumed_mint.recipient_ata != expected_ata {
        differences.push(format!(
            "recipient ATA: channel {}, row {expected_ata}",
            consumed_mint.recipient_ata
        ));
    }
    if consumed_mint.amount != transaction.amount.value() {
        differences.push(format!(
            "amount: channel {}, row {}",
            consumed_mint.amount,
            transaction.amount.value()
        ));
    }
    (!differences.is_empty()).then(|| differences.join("; "))
}

/// Idempotency markers in a memo field, which can carry several "; "-joined entries, each
/// possibly length-prefixed.
fn memo_markers(memo: &str) -> Vec<(ConsumedMintKind, &str)> {
    let mut markers = Vec::new();
    for piece in memo.split("; ") {
        let value = strip_memo_length_prefix(piece);
        if let Some(rest) = value.strip_prefix(MINT_IDEMPOTENCY_MEMO_PREFIX) {
            markers.push((ConsumedMintKind::Deposit, rest));
        } else if let Some(rest) = value.strip_prefix(REMINT_IDEMPOTENCY_MEMO_PREFIX) {
            markers.push((ConsumedMintKind::Remint, rest));
        }
    }
    markers
}

fn strip_memo_length_prefix(memo: &str) -> &str {
    let Some(stripped) = memo.strip_prefix('[') else {
        return memo;
    };

    let Some((length, value)) = stripped.split_once("] ") else {
        return memo;
    };

    if length.chars().all(|c| c.is_ascii_digit()) {
        value
    } else {
        memo
    }
}

fn transaction_succeeded(
    transaction: &solana_transaction_status::EncodedConfirmedTransactionWithStatusMeta,
) -> bool {
    transaction
        .transaction
        .meta
        .as_ref()
        .is_some_and(|meta| meta.err.is_none())
}

fn transaction_signed_by(
    transaction: &solana_transaction_status::EncodedConfirmedTransactionWithStatusMeta,
    signer: &Pubkey,
) -> bool {
    let EncodedTransaction::Json(ui_transaction) = &transaction.transaction.transaction else {
        return false;
    };

    match &ui_transaction.message {
        UiMessage::Parsed(parsed_message) => parsed_message_has_signer(parsed_message, signer),
        UiMessage::Raw(raw_message) => raw_message_has_signer(raw_message, signer),
    }
}

fn transaction_has_memo(
    transaction: &solana_transaction_status::EncodedConfirmedTransactionWithStatusMeta,
    expected_memo: &str,
) -> bool {
    let EncodedTransaction::Json(ui_transaction) = &transaction.transaction.transaction else {
        return false;
    };

    match &ui_transaction.message {
        UiMessage::Parsed(parsed_message) => parsed_message
            .instructions
            .iter()
            .any(|instruction| instruction_has_memo(instruction, expected_memo)),
        UiMessage::Raw(raw_message) => raw_message
            .instructions
            .iter()
            .any(|instruction| raw_instruction_has_memo(raw_message, instruction, expected_memo)),
    }
}

fn transaction_mint_tos(
    transaction: &solana_transaction_status::EncodedConfirmedTransactionWithStatusMeta,
) -> Vec<MintToFields> {
    let EncodedTransaction::Json(ui_transaction) = &transaction.transaction.transaction else {
        return Vec::new();
    };

    match &ui_transaction.message {
        UiMessage::Parsed(parsed_message) => parsed_message
            .instructions
            .iter()
            .filter_map(decode_mint_to)
            .collect(),
        UiMessage::Raw(raw_message) => raw_message
            .instructions
            .iter()
            .filter_map(|instruction| decode_raw_mint_to(raw_message, instruction))
            .collect(),
    }
}

/// The mint (account 0) of each top-level `mintTo`/`mintToChecked`, read without the
/// authority fields, so it also covers the multisig shape the strict decode rejects.
fn mint_to_targets(
    transaction: &solana_transaction_status::EncodedConfirmedTransactionWithStatusMeta,
) -> Vec<Pubkey> {
    let EncodedTransaction::Json(ui_transaction) = &transaction.transaction.transaction else {
        return Vec::new();
    };

    match &ui_transaction.message {
        UiMessage::Parsed(parsed_message) => parsed_message
            .instructions
            .iter()
            .filter_map(mint_to_target)
            .collect(),
        UiMessage::Raw(raw_message) => raw_message
            .instructions
            .iter()
            .filter_map(|instruction| {
                let keys = &raw_message.account_keys;
                let program_id = parse_pubkey(keys.get(instruction.program_id_index as usize)?)?;
                let data = bs58::decode(&instruction.data).into_vec().ok()?;
                parse_token_instruction_mint_amount(&program_id, &data)?;
                parse_pubkey(keys.get(*instruction.accounts.first()? as usize)?)
            })
            .collect(),
    }
}

/// Parsed-message counterpart of `mint_to_targets` for one instruction.
fn mint_to_target(instruction: &UiInstruction) -> Option<Pubkey> {
    match instruction {
        UiInstruction::Compiled(_) => None,
        UiInstruction::Parsed(UiParsedInstruction::Parsed(parsed_instruction)) => {
            let program_id = parse_pubkey(&parsed_instruction.program_id)?;
            if program_id != spl_token::id() && program_id != spl_token_2022::id() {
                return None;
            }
            match parsed_instruction
                .parsed
                .get("type")
                .and_then(Value::as_str)?
            {
                "mintTo" | "mintToChecked" => {
                    parse_pubkey_field(parsed_instruction.parsed.get("info")?, "mint")
                }
                _ => None,
            }
        }
        UiInstruction::Parsed(UiParsedInstruction::PartiallyDecoded(partially_decoded)) => {
            let program_id = parse_pubkey(&partially_decoded.program_id)?;
            let data = bs58::decode(&partially_decoded.data).into_vec().ok()?;
            parse_token_instruction_mint_amount(&program_id, &data)?;
            parse_pubkey(partially_decoded.accounts.first()?)
        }
    }
}

fn parsed_message_has_signer(parsed_message: &UiParsedMessage, signer: &Pubkey) -> bool {
    parsed_message
        .account_keys
        .iter()
        .any(|account| account.signer && parse_pubkey(&account.pubkey) == Some(*signer))
}

fn raw_message_has_signer(raw_message: &UiRawMessage, signer: &Pubkey) -> bool {
    raw_message
        .account_keys
        .iter()
        .position(|account| parse_pubkey(account) == Some(*signer))
        .is_some_and(|index| index < raw_message.header.num_required_signatures as usize)
}

fn raw_instruction_has_memo(
    raw_message: &UiRawMessage,
    instruction: &UiCompiledInstruction,
    expected_memo: &str,
) -> bool {
    let Some(program_id) = raw_message
        .account_keys
        .get(instruction.program_id_index as usize)
    else {
        return false;
    };

    is_memo_program_id(program_id)
        && bs58::decode(&instruction.data)
            .into_vec()
            .map(|memo_data| memo_data == expected_memo.as_bytes())
            .unwrap_or(false)
}

fn instruction_has_memo(instruction: &UiInstruction, expected_memo: &str) -> bool {
    match instruction {
        UiInstruction::Compiled(_) => false,
        UiInstruction::Parsed(UiParsedInstruction::Parsed(parsed_instruction)) => {
            is_memo_program_id(&parsed_instruction.program_id)
                && parsed_instruction.parsed.as_str() == Some(expected_memo)
        }
        UiInstruction::Parsed(UiParsedInstruction::PartiallyDecoded(partially_decoded)) => {
            is_memo_program_id(&partially_decoded.program_id)
                && bs58::decode(&partially_decoded.data)
                    .into_vec()
                    .map(|memo_data| memo_data == expected_memo.as_bytes())
                    .unwrap_or(false)
        }
    }
}

/// Fields of a `mintTo` or `mintToChecked` from spl-token or token-2022, or `None`.
fn decode_mint_to(instruction: &UiInstruction) -> Option<MintToFields> {
    match instruction {
        UiInstruction::Compiled(_) => None,
        UiInstruction::Parsed(UiParsedInstruction::Parsed(parsed_instruction)) => {
            decode_parsed_mint_to(parsed_instruction)
        }
        UiInstruction::Parsed(UiParsedInstruction::PartiallyDecoded(partially_decoded)) => {
            decode_partially_decoded_mint_to(partially_decoded)
        }
    }
}

fn decode_parsed_mint_to(parsed_instruction: &ParsedInstruction) -> Option<MintToFields> {
    let token_program = parse_pubkey(&parsed_instruction.program_id)?;
    if token_program != spl_token::id() && token_program != spl_token_2022::id() {
        return None;
    }

    let instruction_type = parsed_instruction
        .parsed
        .get("type")
        .and_then(Value::as_str)?;
    let info = parsed_instruction.parsed.get("info")?;
    let amount = match instruction_type {
        "mintTo" => parse_u64_field(info, "amount")?,
        "mintToChecked" => parse_u64_field(info.get("tokenAmount")?, "amount")?,
        _ => return None,
    };

    Some(MintToFields {
        mint: parse_pubkey_field(info, "mint")?,
        recipient_ata: parse_pubkey_field(info, "account")?,
        mint_authority: parse_pubkey_field(info, "mintAuthority")?,
        token_program,
        amount,
    })
}

fn decode_partially_decoded_mint_to(
    partially_decoded: &UiPartiallyDecodedInstruction,
) -> Option<MintToFields> {
    let token_program = parse_pubkey(&partially_decoded.program_id)?;
    let data = bs58::decode(&partially_decoded.data).into_vec().ok()?;

    Some(MintToFields {
        mint: parse_pubkey(partially_decoded.accounts.first()?)?,
        recipient_ata: parse_pubkey(partially_decoded.accounts.get(1)?)?,
        mint_authority: parse_pubkey(partially_decoded.accounts.get(2)?)?,
        amount: parse_token_instruction_mint_amount(&token_program, &data)?,
        token_program,
    })
}

/// Raw-message counterpart of `decode_mint_to`.
fn decode_raw_mint_to(
    raw_message: &UiRawMessage,
    instruction: &UiCompiledInstruction,
) -> Option<MintToFields> {
    let keys = &raw_message.account_keys;
    let token_program = parse_pubkey(keys.get(instruction.program_id_index as usize)?)?;
    let mint = parse_pubkey(keys.get(*instruction.accounts.first()? as usize)?)?;
    let recipient_ata = parse_pubkey(keys.get(*instruction.accounts.get(1)? as usize)?)?;
    let mint_authority = parse_pubkey(keys.get(*instruction.accounts.get(2)? as usize)?)?;
    let data = bs58::decode(&instruction.data).into_vec().ok()?;

    Some(MintToFields {
        mint,
        recipient_ata,
        mint_authority,
        amount: parse_token_instruction_mint_amount(&token_program, &data)?,
        token_program,
    })
}

fn parse_pubkey(value: &str) -> Option<Pubkey> {
    Pubkey::from_str(value).ok()
}

fn parse_pubkey_field(value: &Value, field: &str) -> Option<Pubkey> {
    value
        .get(field)
        .and_then(Value::as_str)
        .and_then(parse_pubkey)
}

fn parse_u64_field(value: &Value, field: &str) -> Option<u64> {
    value
        .get(field)
        .and_then(Value::as_str)
        .and_then(|amount| amount.parse::<u64>().ok())
}

fn parse_token_instruction_mint_amount(program_id: &Pubkey, data: &[u8]) -> Option<u64> {
    if *program_id == spl_token::id() {
        return match spl_token::instruction::TokenInstruction::unpack(data).ok()? {
            spl_token::instruction::TokenInstruction::MintTo { amount }
            | spl_token::instruction::TokenInstruction::MintToChecked { amount, .. } => {
                Some(amount)
            }
            _ => None,
        };
    }

    if *program_id == spl_token_2022::id() {
        return match spl_token_2022::instruction::TokenInstruction::unpack(data).ok()? {
            spl_token_2022::instruction::TokenInstruction::MintTo { amount }
            | spl_token_2022::instruction::TokenInstruction::MintToChecked { amount, .. } => {
                Some(amount)
            }
            _ => None,
        };
    }

    None
}

fn is_memo_program_id(program_id: &str) -> bool {
    Pubkey::from_str(program_id)
        .map(|pubkey| pubkey == spl_memo::id())
        .unwrap_or(false)
}

/// Cleanup mint builder cache when transaction completes or fails
pub(super) fn cleanup_mint_builder(state: &mut SenderState, transaction_id: Option<i64>) {
    if let Some(txn_id) = transaction_id {
        state.mint_builders.remove(&txn_id);
    }
}

#[cfg(test)]
mod tests {
    use super::{decode_and_check_authority, AuthorityCheck};
    use private_channel_core::t22_fixtures::{t22_mint, T22_MINT_LEN};
    use solana_sdk::pubkey::Pubkey;
    use spl_token_2022::extension::{
        permanent_delegate::PermanentDelegate, BaseStateWithExtensionsMut, StateWithExtensionsMut,
    };
    use spl_token_2022::state::Mint;

    // Tests for `decode_and_check_authority`, the pure helper that drives
    // the JIT pre-check, post-confirm re-check, and fallback backoff. Each
    // variant must be reachable: Match / Mismatch / DelegateMismatch /
    // Uninitialized / CorruptData.

    /// A channel mint with `authority` and the permanent delegate `delegate`.
    fn channel_mint(authority: Option<Pubkey>, delegate: Option<Pubkey>) -> Vec<u8> {
        let mut data = t22_mint(6, &Pubkey::new_unique(), None, delegate.as_ref(), 0);
        let mut state = StateWithExtensionsMut::<Mint>::unpack(&mut data).unwrap();
        state.base.mint_authority = authority.into();
        state.pack_base();
        data
    }

    /// Initialized mint with `mint_authority` and delegate matching the operator's.
    #[test]
    fn decode_and_check_authority_match_returns_match() {
        let (admin, delegate) = (Pubkey::new_unique(), Pubkey::new_unique());
        let data = channel_mint(Some(admin), Some(delegate));
        assert_eq!(
            decode_and_check_authority(&data, &admin, &delegate),
            AuthorityCheck::Match
        );
    }

    /// Initialized mint with `mint_authority` set to a different pubkey
    /// (the rotated-admin / different-operator scenario). Helper must
    /// surface the actual authority for log/error context.
    #[test]
    fn decode_and_check_authority_mismatch_returns_mismatch() {
        let (admin, delegate, other) = (
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
        );
        let data = channel_mint(Some(other), Some(delegate));
        match decode_and_check_authority(&data, &admin, &delegate) {
            AuthorityCheck::Mismatch(actual) => assert_eq!(actual, other),
            other => panic!("expected Mismatch, got {:?}", other),
        }
    }

    /// Initialized mint whose authority has been cleared (`COption::None`)
    /// is treated as a mismatch with `Pubkey::default()`: the operator
    /// cannot mint without an authority either way.
    #[test]
    fn decode_and_check_authority_no_authority_returns_mismatch_default() {
        let (admin, delegate) = (Pubkey::new_unique(), Pubkey::new_unique());
        let data = channel_mint(None, Some(delegate));
        match decode_and_check_authority(&data, &admin, &delegate) {
            AuthorityCheck::Mismatch(actual) => assert_eq!(actual, Pubkey::default()),
            other => panic!("expected Mismatch, got {:?}", other),
        }
    }

    /// The operator never mints into a mint that lacks the seize authority it expects.
    #[test]
    fn decode_and_check_authority_wrong_or_missing_delegate_returns_delegate_mismatch() {
        let (admin, delegate, other) = (
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
        );
        let mut revoked = channel_mint(Some(admin), Some(delegate));
        StateWithExtensionsMut::<Mint>::unpack(&mut revoked)
            .unwrap()
            .get_extension_mut::<PermanentDelegate>()
            .unwrap()
            .delegate = None.try_into().unwrap();
        // A legacy 82-byte mint decodes as a Token-2022 mint with no extension.
        let cases = [
            (
                "other delegate",
                channel_mint(Some(admin), Some(other)),
                Some(other),
            ),
            ("no extension", channel_mint(Some(admin), None), None),
            ("revoked delegate", revoked, None),
        ];
        for (label, data, actual) in cases {
            assert_eq!(
                decode_and_check_authority(&data, &admin, &delegate),
                AuthorityCheck::DelegateMismatch(actual),
                "{label}"
            );
        }
    }

    /// Mint allocated and Token-2022-owned but not yet initialized (the
    /// legitimate JIT case where InitializeMint should run).
    #[test]
    fn decode_and_check_authority_uninitialized_returns_uninitialized() {
        let admin = Pubkey::new_unique();
        assert_eq!(
            decode_and_check_authority(&[0u8; T22_MINT_LEN], &admin, &admin),
            AuthorityCheck::Uninitialized
        );
    }

    /// Empty account data: there is no reliable way to tell "account doesn't
    /// exist" from corrupt data via the bytes alone, so it lands in `CorruptData`;
    /// the caller distinguishes by also checking `Err` from the RPC.
    #[test]
    fn decode_and_check_authority_empty_returns_corrupt() {
        let admin = Pubkey::new_unique();
        assert_eq!(
            decode_and_check_authority(&[], &admin, &admin),
            AuthorityCheck::CorruptData
        );
    }

    /// Data of the wrong length cannot decode as a mint.
    #[test]
    fn decode_and_check_authority_wrong_length_returns_corrupt() {
        let admin = Pubkey::new_unique();
        for len in [10, 100] {
            assert_eq!(
                decode_and_check_authority(&vec![0xFFu8; len], &admin, &admin),
                AuthorityCheck::CorruptData,
                "{len} bytes"
            );
        }
    }

    /// Random bytes of the base length usually contain an invalid `COption`
    /// discriminant byte, which the unpack rejects.
    #[test]
    fn decode_and_check_authority_random_bytes_returns_corrupt() {
        let admin = Pubkey::new_unique();
        let data: Vec<u8> = (0u8..82).collect();
        assert_eq!(
            decode_and_check_authority(&data, &admin, &admin),
            AuthorityCheck::CorruptData
        );
    }
}

#[cfg(test)]
mod mint_to_decode_tests {
    use super::{decode_mint_to, decode_raw_mint_to, MintToFields};
    use serde_json::json;
    use solana_sdk::pubkey::Pubkey;
    use solana_transaction_status::{UiCompiledInstruction, UiInstruction, UiRawMessage};

    const AMOUNT: u64 = 1_000;
    const DECIMALS: u8 = 6;

    fn expected_fields(token_program: Pubkey) -> MintToFields {
        MintToFields {
            token_program,
            mint: Pubkey::new_unique(),
            recipient_ata: Pubkey::new_unique(),
            mint_authority: Pubkey::new_unique(),
            amount: AMOUNT,
        }
    }

    #[test]
    fn decodes_parsed_mint_to() {
        let expected = expected_fields(spl_token::id());
        let instruction: UiInstruction = serde_json::from_value(json!({
            "program": "spl-token",
            "programId": expected.token_program.to_string(),
            "parsed": {
                "type": "mintTo",
                "info": {
                    "mint": expected.mint.to_string(),
                    "account": expected.recipient_ata.to_string(),
                    "mintAuthority": expected.mint_authority.to_string(),
                    "amount": AMOUNT.to_string(),
                },
            },
        }))
        .unwrap();

        assert_eq!(decode_mint_to(&instruction), Some(expected));
    }

    #[test]
    fn decodes_parsed_mint_to_checked() {
        let expected = expected_fields(spl_token_2022::id());
        let instruction: UiInstruction = serde_json::from_value(json!({
            "program": "spl-token-2022",
            "programId": expected.token_program.to_string(),
            "parsed": {
                "type": "mintToChecked",
                "info": {
                    "mint": expected.mint.to_string(),
                    "account": expected.recipient_ata.to_string(),
                    "mintAuthority": expected.mint_authority.to_string(),
                    "tokenAmount": {
                        "amount": AMOUNT.to_string(),
                        "decimals": DECIMALS,
                        "uiAmountString": "0.001",
                    },
                },
            },
        }))
        .unwrap();

        assert_eq!(decode_mint_to(&instruction), Some(expected));
    }

    #[test]
    fn decodes_partially_decoded_mint_to() {
        let expected = expected_fields(spl_token::id());
        let data = spl_token::instruction::mint_to(
            &expected.token_program,
            &expected.mint,
            &expected.recipient_ata,
            &expected.mint_authority,
            &[],
            AMOUNT,
        )
        .unwrap()
        .data;
        let instruction: UiInstruction = serde_json::from_value(json!({
            "programId": expected.token_program.to_string(),
            "accounts": [
                expected.mint.to_string(),
                expected.recipient_ata.to_string(),
                expected.mint_authority.to_string(),
            ],
            "data": bs58::encode(data).into_string(),
        }))
        .unwrap();

        assert_eq!(decode_mint_to(&instruction), Some(expected));
    }

    #[test]
    fn decodes_raw_mint_to_checked() {
        let expected = expected_fields(spl_token_2022::id());
        let data = spl_token_2022::instruction::mint_to_checked(
            &expected.token_program,
            &expected.mint,
            &expected.recipient_ata,
            &expected.mint_authority,
            &[],
            AMOUNT,
            DECIMALS,
        )
        .unwrap()
        .data;
        let raw_message: UiRawMessage = serde_json::from_value(json!({
            "header": {
                "numRequiredSignatures": 1,
                "numReadonlySignedAccounts": 0,
                "numReadonlyUnsignedAccounts": 2,
            },
            "accountKeys": [
                expected.mint_authority.to_string(),
                expected.recipient_ata.to_string(),
                expected.mint.to_string(),
                expected.token_program.to_string(),
            ],
            "recentBlockhash": "11111111111111111111111111111111",
            "instructions": [{
                "programIdIndex": 3,
                "accounts": [2, 1, 0],
                "data": bs58::encode(data).into_string(),
            }],
        }))
        .unwrap();
        let instruction: &UiCompiledInstruction = &raw_message.instructions[0];

        assert_eq!(
            decode_raw_mint_to(&raw_message, instruction),
            Some(expected)
        );
    }

    #[test]
    fn rejects_non_mint_token_instruction() {
        let expected = expected_fields(spl_token::id());
        let data = spl_token::instruction::transfer(
            &expected.token_program,
            &expected.mint,
            &expected.recipient_ata,
            &expected.mint_authority,
            &[],
            AMOUNT,
        )
        .unwrap()
        .data;
        let instruction: UiInstruction = serde_json::from_value(json!({
            "programId": expected.token_program.to_string(),
            "accounts": [
                expected.mint.to_string(),
                expected.recipient_ata.to_string(),
                expected.mint_authority.to_string(),
            ],
            "data": bs58::encode(data).into_string(),
        }))
        .unwrap();

        assert_eq!(decode_mint_to(&instruction), None);
    }
}

#[cfg(test)]
mod consumed_set_tests {
    use super::{
        enumerate_consumed_mints, extend_with_mint_history, ConsumedMint, ConsumedMintKind,
        ConsumedSet, MintToFields,
    };
    use crate::operator::instruction_util::{
        mint_idempotency_memo, remint_idempotency_memo, SourceEventId,
    };
    use crate::operator::{RetryConfig, RpcClientWithRetry};
    use serde_json::{json, Value};
    use solana_commitment_config::CommitmentConfig;
    use solana_sdk::pubkey::Pubkey;
    use solana_sdk::signature::Signature;

    const PAGE_LIMIT: usize = 2;
    const AMOUNT: u64 = 1_000;

    fn fast_rpc(url: &str) -> RpcClientWithRetry {
        RpcClientWithRetry::with_retry_config(
            url.to_string(),
            RetryConfig {
                max_attempts: 1,
                base_delay: std::time::Duration::from_millis(1),
                max_delay: std::time::Duration::from_millis(1),
            },
            CommitmentConfig::confirmed(),
        )
    }

    fn sig_entry(signature: &str, memo: &str) -> String {
        format!(
            r#"{{"signature":"{signature}","slot":1,"err":null,"memo":"{memo}","blockTime":null,"confirmationStatus":"confirmed"}}"#
        )
    }

    /// A `MintTo` of `AMOUNT` signed by `mint_authority` into fresh mint and ATA keys.
    fn mint_to_by(mint_authority: &Pubkey) -> MintToFields {
        mint_to_on(&Pubkey::new_unique(), mint_authority)
    }

    /// A `MintTo` of `AMOUNT` on `mint` signed by `mint_authority` into a fresh ATA.
    fn mint_to_on(mint: &Pubkey, mint_authority: &Pubkey) -> MintToFields {
        MintToFields {
            mint: *mint,
            recipient_ata: Pubkey::new_unique(),
            mint_authority: *mint_authority,
            token_program: spl_token::id(),
            amount: AMOUNT,
        }
    }

    /// Serves `entries` as the one-page history of `address`, and nothing for other addresses.
    async fn history_for(
        server: &mut mockito::ServerGuard,
        address: &Pubkey,
        entries: &[String],
    ) -> mockito::Mock {
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::AllOf(vec![
                mockito::Matcher::Regex(r#""method"\s*:\s*"getSignaturesForAddress""#.into()),
                mockito::Matcher::Regex(address.to_string()),
            ]))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(format!(
                r#"{{"jsonrpc":"2.0","result":[{}],"id":0}}"#,
                entries.join(",")
            ))
            .create_async()
            .await
    }

    /// Rewrites each parsed `mintTo` on `mint` in `reply` the way `jsonParsed` renders one with
    /// more than three accounts: `multisigMintAuthority` plus `signers`, no `mintAuthority`.
    fn with_multisig_authority(reply: String, mint: &Pubkey) -> String {
        let mut reply: Value = serde_json::from_str(&reply).unwrap();
        let instructions = reply["result"]["transaction"]["message"]["instructions"]
            .as_array_mut()
            .unwrap();
        for instruction in instructions {
            if instruction["parsed"]["type"] == "mintTo"
                && instruction["parsed"]["info"]["mint"] == mint.to_string()
            {
                let info = instruction["parsed"]["info"].as_object_mut().unwrap();
                let authority = info.remove("mintAuthority").unwrap();
                info.insert("multisigMintAuthority".to_string(), authority.clone());
                info.insert("signers".to_string(), json!([authority]));
            }
        }
        reply.to_string()
    }

    /// Raw-message `getTransaction` reply for a tx signed only by `signer`, with `mentioned`
    /// as a non-signing account key, one Memo instruction carrying `memo` and one `MintTo`
    /// per `mint_tos` entry. `meta_err` is the JSON error, or `Value::Null` for success.
    fn transaction_reply(
        signer: &Pubkey,
        mentioned: &Pubkey,
        memo: &str,
        mint_tos: &[MintToFields],
        meta_err: Value,
    ) -> String {
        let mut account_keys = vec![
            signer.to_string(),
            mentioned.to_string(),
            spl_memo::id().to_string(),
        ];
        let mut instructions = vec![json!({
            "programIdIndex": 2,
            "accounts": [],
            "data": bs58::encode(memo).into_string(),
        })];
        for mint_to in mint_tos {
            let base = account_keys.len() as u8;
            account_keys.extend([
                mint_to.mint.to_string(),
                mint_to.recipient_ata.to_string(),
                mint_to.mint_authority.to_string(),
                mint_to.token_program.to_string(),
            ]);
            let data = spl_token::instruction::mint_to(
                &mint_to.token_program,
                &mint_to.mint,
                &mint_to.recipient_ata,
                &mint_to.mint_authority,
                &[],
                mint_to.amount,
            )
            .unwrap()
            .data;
            instructions.push(json!({
                "programIdIndex": base + 3,
                "accounts": [base, base + 1, base + 2],
                "data": bs58::encode(data).into_string(),
            }));
        }
        let balances = vec![0u64; account_keys.len()];
        let status = if meta_err.is_null() {
            json!({"Ok": null})
        } else {
            json!({"Err": meta_err})
        };

        json!({
            "jsonrpc": "2.0",
            "id": 0,
            "result": {
                "slot": 1,
                "blockTime": null,
                "transaction": {
                    "signatures": [Signature::new_unique().to_string()],
                    "message": {
                        "header": {
                            "numRequiredSignatures": 1,
                            "numReadonlySignedAccounts": 0,
                            "numReadonlyUnsignedAccounts": account_keys.len() - 1,
                        },
                        "accountKeys": account_keys,
                        "recentBlockhash": "11111111111111111111111111111111",
                        "instructions": instructions,
                    },
                },
                "meta": {
                    "err": meta_err,
                    "status": status,
                    "fee": 5000,
                    "preBalances": balances,
                    "postBalances": balances,
                },
            },
        })
        .to_string()
    }

    /// Parsed-message (`jsonParsed`, as production fetches) `getTransaction` reply for a
    /// successful tx signed only by `signer`, with `mentioned` as a non-signing account key,
    /// one Memo instruction carrying `memo` and one `mintTo` per `mint_tos` entry.
    fn parsed_transaction_reply(
        signer: &Pubkey,
        mentioned: &Pubkey,
        memo: &str,
        mint_tos: &[MintToFields],
    ) -> String {
        let mut instructions = vec![json!({
            "program": "spl-memo",
            "programId": spl_memo::id().to_string(),
            "parsed": memo,
        })];
        for mint_to in mint_tos {
            instructions.push(json!({
                "program": "spl-token",
                "programId": mint_to.token_program.to_string(),
                "parsed": {
                    "type": "mintTo",
                    "info": {
                        "mint": mint_to.mint.to_string(),
                        "account": mint_to.recipient_ata.to_string(),
                        "mintAuthority": mint_to.mint_authority.to_string(),
                        "amount": mint_to.amount.to_string(),
                    },
                },
            }));
        }

        json!({
            "jsonrpc": "2.0",
            "id": 0,
            "result": {
                "slot": 1,
                "blockTime": null,
                "transaction": {
                    "signatures": [Signature::new_unique().to_string()],
                    "message": {
                        "accountKeys": [
                            {"pubkey": signer.to_string(), "writable": true, "signer": true, "source": "transaction"},
                            {"pubkey": mentioned.to_string(), "writable": true, "signer": false, "source": "transaction"},
                        ],
                        "recentBlockhash": "11111111111111111111111111111111",
                        "instructions": instructions,
                    },
                },
                "meta": {
                    "err": null,
                    "status": {"Ok": null},
                    "fee": 5000,
                    "preBalances": [0, 0],
                    "postBalances": [0, 0],
                },
            },
        })
        .to_string()
    }

    /// A serviced mint that sits on the second page (reached via the `before` cursor)
    /// must still be collected, guarding the bounded-lookback blind spot.
    #[tokio::test]
    async fn enumerate_pages_to_completion() {
        let mut server = mockito::Server::new_async().await;
        let authority = Pubkey::new_unique();

        let page1_a = Signature::new_unique().to_string();
        let page1_b = Signature::new_unique().to_string();
        let page2_a = Signature::new_unique().to_string();

        let id1 = SourceEventId::new("evt-page1", 0, None);
        let id2 = SourceEventId::new("evt-page2", 0, None);
        let memo1 = mint_idempotency_memo(&id1);
        let memo2 = mint_idempotency_memo(&id2);

        // Page 1: full page (== PAGE_LIMIT) so the cursor advances to page1_b.
        let _p1 = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::AllOf(vec![
                mockito::Matcher::Regex(r#""method"\s*:\s*"getSignaturesForAddress""#.into()),
                mockito::Matcher::Regex(r#""before"\s*:\s*null"#.into()),
            ]))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(format!(
                r#"{{"jsonrpc":"2.0","result":[{},{}],"id":0}}"#,
                sig_entry(&page1_a, &memo1),
                sig_entry(&page1_b, "unrelated-memo"),
            ))
            .create_async()
            .await;

        // Page 2: short page (< PAGE_LIMIT) terminates pagination.
        let _p2 = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::AllOf(vec![
                mockito::Matcher::Regex(r#""method"\s*:\s*"getSignaturesForAddress""#.into()),
                mockito::Matcher::Regex(format!(r#""before"\s*:\s*"{}""#, page1_b)),
            ]))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(format!(
                r#"{{"jsonrpc":"2.0","result":[{}],"id":0}}"#,
                sig_entry(&page2_a, &memo2),
            ))
            .create_async()
            .await;

        let _tx1 = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::AllOf(vec![
                mockito::Matcher::Regex(r#""method"\s*:\s*"getTransaction""#.into()),
                mockito::Matcher::Regex(page1_a.clone()),
            ]))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(transaction_reply(
                &authority,
                &Pubkey::new_unique(),
                &memo1,
                &[mint_to_by(&authority)],
                Value::Null,
            ))
            .create_async()
            .await;
        let _tx2 = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::AllOf(vec![
                mockito::Matcher::Regex(r#""method"\s*:\s*"getTransaction""#.into()),
                mockito::Matcher::Regex(page2_a.clone()),
            ]))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(transaction_reply(
                &authority,
                &Pubkey::new_unique(),
                &memo2,
                &[mint_to_by(&authority)],
                Value::Null,
            ))
            .create_async()
            .await;

        let rpc = fast_rpc(&server.url());
        let set = enumerate_consumed_mints(&rpc, &authority, PAGE_LIMIT)
            .await
            .expect("enumeration should succeed across pages");

        assert_eq!(set.len(), 2, "both pages' deposit mints must be collected");
        assert_eq!(
            set.get(&id1).map(|consumed| consumed.kind),
            Some(ConsumedMintKind::Deposit)
        );
        assert_eq!(
            set.get(&id2).map(|consumed| consumed.kind),
            Some(ConsumedMintKind::Deposit)
        );
    }

    /// An RPC error during enumeration returns Err, never an empty set (fail closed).
    #[tokio::test]
    async fn enumerate_rpc_error_is_err_not_empty() {
        let mut server = mockito::Server::new_async().await;
        let _m = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getSignaturesForAddress""#.into(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{"jsonrpc":"2.0","error":{"code":-32601,"message":"Method not found"},"id":0}"#,
            )
            .create_async()
            .await;

        let rpc = fast_rpc(&server.url());
        let result = enumerate_consumed_mints(&rpc, &Pubkey::new_unique(), PAGE_LIMIT).await;
        assert!(
            result.is_err(),
            "RPC failure must surface as Err, not an empty set"
        );
    }

    /// A legacy serial-id idempotency memo (unparseable under the current scheme) aborts
    /// enumeration so resync fails closed instead of re-minting that serviced deposit.
    #[tokio::test]
    async fn enumerate_legacy_scheme_memo_is_err() {
        let mut server = mockito::Server::new_async().await;
        let authority = Pubkey::new_unique();
        // Legacy serial-id memo: prefix present, value is a bare number.
        let legacy_memo = "private_channel:mint-idempotency:42";
        let _m = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getSignaturesForAddress""#.into(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(format!(
                r#"{{"jsonrpc":"2.0","result":[{}],"id":0}}"#,
                sig_entry(&Signature::new_unique().to_string(), legacy_memo),
            ))
            .create_async()
            .await;
        let _tx = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getTransaction""#.into(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(transaction_reply(
                &authority,
                &Pubkey::new_unique(),
                legacy_memo,
                &[mint_to_by(&authority)],
                Value::Null,
            ))
            .create_async()
            .await;

        let rpc = fast_rpc(&server.url());
        let result = enumerate_consumed_mints(&rpc, &authority, PAGE_LIMIT).await;
        let err = result.expect_err("legacy-scheme memo must abort enumeration");
        assert!(
            err.contains("cutover"),
            "error should name the memo cutover: {err}"
        );
    }

    /// A user tx that only names the authority as an account (e.g. a transfer recipient)
    /// must not count as a mint, nor abort enumeration with a legacy-looking memo. Its
    /// two memo pieces must cost one fetch, not one each.
    #[tokio::test]
    async fn enumerate_skips_memo_the_authority_did_not_sign() {
        let mut server = mockito::Server::new_async().await;
        let authority = Pubkey::new_unique();
        let attacker = Pubkey::new_unique();
        let victim_id = SourceEventId::new("evt-victim", 0, None);
        let forged_memo = format!(
            "{}; private_channel:mint-idempotency:42",
            mint_idempotency_memo(&victim_id)
        );

        let _m = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getSignaturesForAddress""#.into(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(format!(
                r#"{{"jsonrpc":"2.0","result":[{}],"id":0}}"#,
                // One short page, so pagination stops after it.
                sig_entry(&Signature::new_unique().to_string(), &forged_memo),
            ))
            .create_async()
            .await;
        let tx = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getTransaction""#.into(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(transaction_reply(
                &attacker,
                &authority,
                &forged_memo,
                &[],
                Value::Null,
            ))
            .expect(1)
            .create_async()
            .await;

        let rpc = fast_rpc(&server.url());
        let set = enumerate_consumed_mints(&rpc, &authority, PAGE_LIMIT)
            .await
            .expect("unsigned memos must be skipped, not abort enumeration");
        assert!(
            set.is_empty(),
            "an unsigned memo must not mark a deposit minted"
        );
        tx.assert_async().await;
    }

    /// Production fetches `jsonParsed`, so the parsed signer check must accept a mint the
    /// authority signed.
    #[tokio::test]
    async fn enumerate_collects_parsed_memo_the_authority_signed() {
        let mut server = mockito::Server::new_async().await;
        let authority = Pubkey::new_unique();
        let landed = Signature::new_unique();
        let source_event_id = SourceEventId::new("evt-parsed", 0, None);
        let memo = mint_idempotency_memo(&source_event_id);
        let mint_to = mint_to_by(&authority);

        let _m = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getSignaturesForAddress""#.into(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(format!(
                r#"{{"jsonrpc":"2.0","result":[{}],"id":0}}"#,
                sig_entry(&landed.to_string(), &memo),
            ))
            .create_async()
            .await;
        let _tx = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getTransaction""#.into(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(parsed_transaction_reply(
                &authority,
                &Pubkey::new_unique(),
                &memo,
                &[mint_to],
            ))
            .create_async()
            .await;

        let rpc = fast_rpc(&server.url());
        let set = enumerate_consumed_mints(&rpc, &authority, PAGE_LIMIT)
            .await
            .expect("enumeration should succeed");
        assert_eq!(
            set.get(&source_event_id),
            Some(&ConsumedMint {
                signature: landed,
                kind: ConsumedMintKind::Deposit,
                mint: mint_to.mint,
                recipient_ata: mint_to.recipient_ata,
                token_program: mint_to.token_program,
                amount: mint_to.amount,
            })
        );
    }

    /// Serves one history page holding `landed` with `history_memo`, answers its fetch
    /// with `reply`, and enumerates the authority's history.
    async fn enumerate_one(
        authority: &Pubkey,
        landed: &Signature,
        history_memo: &str,
        reply: String,
    ) -> Result<ConsumedSet, String> {
        let mut server = mockito::Server::new_async().await;
        let _m = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getSignaturesForAddress""#.into(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(format!(
                r#"{{"jsonrpc":"2.0","result":[{}],"id":0}}"#,
                sig_entry(&landed.to_string(), history_memo),
            ))
            .create_async()
            .await;
        let _tx = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getTransaction""#.into(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(reply)
            .create_async()
            .await;

        enumerate_consumed_mints(&fast_rpc(&server.url()), authority, PAGE_LIMIT).await
    }

    /// Serves one full history page holding `newer` then `older`, both carrying `memo`,
    /// answers each fetch with its own reply, then an empty page, and enumerates.
    async fn enumerate_two(
        authority: &Pubkey,
        memo: &str,
        (newer, newer_reply): (&Signature, String),
        (older, older_reply): (&Signature, String),
    ) -> Result<ConsumedSet, String> {
        let mut server = mockito::Server::new_async().await;
        let _page1 = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::AllOf(vec![
                mockito::Matcher::Regex(r#""method"\s*:\s*"getSignaturesForAddress""#.into()),
                mockito::Matcher::Regex(r#""before"\s*:\s*null"#.into()),
            ]))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(format!(
                r#"{{"jsonrpc":"2.0","result":[{},{}],"id":0}}"#,
                sig_entry(&newer.to_string(), memo),
                sig_entry(&older.to_string(), memo),
            ))
            .create_async()
            .await;
        let _page2 = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::AllOf(vec![
                mockito::Matcher::Regex(r#""method"\s*:\s*"getSignaturesForAddress""#.into()),
                mockito::Matcher::Regex(format!(r#""before"\s*:\s*"{older}""#)),
            ]))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"jsonrpc":"2.0","result":[],"id":0}"#)
            .create_async()
            .await;
        let _newer_tx = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::AllOf(vec![
                mockito::Matcher::Regex(r#""method"\s*:\s*"getTransaction""#.into()),
                mockito::Matcher::Regex(newer.to_string()),
            ]))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(newer_reply)
            .create_async()
            .await;
        let _older_tx = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::AllOf(vec![
                mockito::Matcher::Regex(r#""method"\s*:\s*"getTransaction""#.into()),
                mockito::Matcher::Regex(older.to_string()),
            ]))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(older_reply)
            .create_async()
            .await;

        enumerate_consumed_mints(&fast_rpc(&server.url()), authority, PAGE_LIMIT).await
    }

    /// Two signed mints for one event that disagree: keeping only the newest would hide
    /// the older one from validation, so enumeration aborts.
    #[tokio::test]
    async fn enumerate_aborts_on_conflicting_mints_for_one_event() {
        let authority = Pubkey::new_unique();
        let newer = Signature::new_unique();
        let older = Signature::new_unique();
        let memo = mint_idempotency_memo(&SourceEventId::new("evt-conflict", 0, None));
        let paying = mint_to_by(&authority);
        let underpaying = MintToFields {
            amount: AMOUNT / 2,
            ..paying
        };

        let err = enumerate_two(
            &authority,
            &memo,
            (
                &newer,
                transaction_reply(
                    &authority,
                    &Pubkey::new_unique(),
                    &memo,
                    &[paying],
                    Value::Null,
                ),
            ),
            (
                &older,
                transaction_reply(
                    &authority,
                    &Pubkey::new_unique(),
                    &memo,
                    &[underpaying],
                    Value::Null,
                ),
            ),
        )
        .await
        .expect_err("two signed mints for one event must abort enumeration");
        for signature in [newer, older] {
            assert!(
                err.contains(&signature.to_string()),
                "error should name {signature}: {err}"
            );
        }
    }

    /// Two successful signed mints for one event are a double issuance even when both
    /// pay the right amount.
    #[tokio::test]
    async fn enumerate_aborts_on_double_mint_of_one_event() {
        let authority = Pubkey::new_unique();
        let newer = Signature::new_unique();
        let older = Signature::new_unique();
        let memo = mint_idempotency_memo(&SourceEventId::new("evt-double", 0, None));
        let paying = mint_to_by(&authority);

        let result = enumerate_two(
            &authority,
            &memo,
            (
                &newer,
                transaction_reply(
                    &authority,
                    &Pubkey::new_unique(),
                    &memo,
                    &[paying],
                    Value::Null,
                ),
            ),
            (
                &older,
                transaction_reply(
                    &authority,
                    &Pubkey::new_unique(),
                    &memo,
                    &[paying],
                    Value::Null,
                ),
            ),
        )
        .await;

        assert!(
            result.is_err(),
            "a double mint of one event must abort enumeration: {result:?}"
        );
    }

    /// A signed tx carrying the marker but no `MintTo` proves nothing was minted.
    #[tokio::test]
    async fn enumerate_aborts_when_signed_tx_has_no_mint_to() {
        let authority = Pubkey::new_unique();
        let landed = Signature::new_unique();
        let memo = mint_idempotency_memo(&SourceEventId::new("evt-no-mint", 0, None));
        let reply = transaction_reply(&authority, &Pubkey::new_unique(), &memo, &[], Value::Null);

        let err = enumerate_one(&authority, &landed, &memo, reply)
            .await
            .expect_err("a signed tx without a MintTo must abort enumeration");
        assert!(
            err.contains(&landed.to_string()),
            "error should name the tx: {err}"
        );
    }

    /// Two `MintTo`s leave the marker bound to neither, so the tx cannot be trusted.
    #[tokio::test]
    async fn enumerate_aborts_when_signed_tx_has_two_mint_tos() {
        let authority = Pubkey::new_unique();
        let landed = Signature::new_unique();
        let memo = mint_idempotency_memo(&SourceEventId::new("evt-two-mints", 0, None));
        let reply = transaction_reply(
            &authority,
            &Pubkey::new_unique(),
            &memo,
            &[mint_to_by(&authority), mint_to_by(&authority)],
            Value::Null,
        );

        let err = enumerate_one(&authority, &landed, &memo, reply)
            .await
            .expect_err("a signed tx with two MintTos must abort enumeration");
        assert!(
            err.contains(&landed.to_string()),
            "error should name the tx: {err}"
        );
    }

    /// A `MintTo` under another mint authority is not the operator's mint.
    #[tokio::test]
    async fn enumerate_aborts_when_mint_to_is_not_by_the_authority() {
        let authority = Pubkey::new_unique();
        let landed = Signature::new_unique();
        let memo = mint_idempotency_memo(&SourceEventId::new("evt-foreign-mint", 0, None));
        let reply = transaction_reply(
            &authority,
            &Pubkey::new_unique(),
            &memo,
            &[mint_to_by(&Pubkey::new_unique())],
            Value::Null,
        );

        let err = enumerate_one(&authority, &landed, &memo, reply)
            .await
            .expect_err("a MintTo by another authority must abort enumeration");
        assert!(
            err.contains(&landed.to_string()),
            "error should name the tx: {err}"
        );
    }

    /// One `MintTo` cannot service two source events, so two markers in one tx abort.
    #[tokio::test]
    async fn enumerate_aborts_when_signed_tx_carries_two_markers() {
        let authority = Pubkey::new_unique();
        let landed = Signature::new_unique();
        let first_memo = mint_idempotency_memo(&SourceEventId::new("evt-first", 0, None));
        let second_memo = mint_idempotency_memo(&SourceEventId::new("evt-second", 0, None));
        let history_memo = format!("{first_memo}; {second_memo}");
        let reply = transaction_reply(
            &authority,
            &Pubkey::new_unique(),
            &first_memo,
            &[mint_to_by(&authority)],
            Value::Null,
        );

        let err = enumerate_one(&authority, &landed, &history_memo, reply)
            .await
            .expect_err("a signed tx with two markers must abort enumeration");
        assert!(
            err.contains(&landed.to_string()),
            "error should name the tx: {err}"
        );
    }

    /// A signed tx whose Memo instructions lack the exact marker the history reports
    /// cannot be authenticated, so enumeration aborts.
    #[tokio::test]
    async fn enumerate_aborts_when_signed_tx_lacks_exact_memo() {
        let mut server = mockito::Server::new_async().await;
        let authority = Pubkey::new_unique();
        let landed = Signature::new_unique();
        let source_event_id = SourceEventId::new("evt-memo-mismatch", 0, None);

        let _m = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getSignaturesForAddress""#.into(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(format!(
                r#"{{"jsonrpc":"2.0","result":[{}],"id":0}}"#,
                sig_entry(
                    &landed.to_string(),
                    &mint_idempotency_memo(&source_event_id)
                ),
            ))
            .create_async()
            .await;
        let _tx = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getTransaction""#.into(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(transaction_reply(
                &authority,
                &Pubkey::new_unique(),
                "unrelated-memo",
                &[mint_to_by(&authority)],
                Value::Null,
            ))
            .create_async()
            .await;

        let rpc = fast_rpc(&server.url());
        let err = enumerate_consumed_mints(&rpc, &authority, PAGE_LIMIT)
            .await
            .expect_err("a signed tx without the exact memo must abort enumeration");
        assert!(
            err.contains(&landed.to_string()),
            "error should name the tx: {err}"
        );
    }

    /// A forged remint memo on a user tx that only names the authority must not count.
    #[tokio::test]
    async fn enumerate_skips_remint_memo_the_authority_did_not_sign() {
        let mut server = mockito::Server::new_async().await;
        let authority = Pubkey::new_unique();
        let attacker = Pubkey::new_unique();
        let victim_id = SourceEventId::new("evt-victim-remint", 0, None);
        let forged_memo = remint_idempotency_memo(&victim_id);

        let _m = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getSignaturesForAddress""#.into(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(format!(
                r#"{{"jsonrpc":"2.0","result":[{}],"id":0}}"#,
                sig_entry(&Signature::new_unique().to_string(), &forged_memo),
            ))
            .create_async()
            .await;
        let _tx = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getTransaction""#.into(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(parsed_transaction_reply(
                &attacker,
                &authority,
                &forged_memo,
                &[],
            ))
            .create_async()
            .await;

        let rpc = fast_rpc(&server.url());
        let set = enumerate_consumed_mints(&rpc, &authority, PAGE_LIMIT)
            .await
            .expect("unsigned memos must be skipped, not abort enumeration");
        assert!(
            set.is_empty(),
            "an unsigned remint memo must not mark a withdrawal reminted"
        );
    }

    /// A tx whose fetched meta failed must not count, even if the authority signed it and
    /// the history entry reported no error.
    #[tokio::test]
    async fn enumerate_skips_failed_transaction() {
        let mut server = mockito::Server::new_async().await;
        let authority = Pubkey::new_unique();
        let source_event_id = SourceEventId::new("evt-failed", 0, None);
        let memo = mint_idempotency_memo(&source_event_id);

        let _m = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getSignaturesForAddress""#.into(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(format!(
                r#"{{"jsonrpc":"2.0","result":[{}],"id":0}}"#,
                sig_entry(&Signature::new_unique().to_string(), &memo),
            ))
            .create_async()
            .await;
        let _tx = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getTransaction""#.into(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(transaction_reply(
                &authority,
                &Pubkey::new_unique(),
                &memo,
                &[mint_to_by(&authority)],
                json!({"InstructionError": [0, {"Custom": 1}]}),
            ))
            .create_async()
            .await;

        let rpc = fast_rpc(&server.url());
        let set = enumerate_consumed_mints(&rpc, &authority, PAGE_LIMIT)
            .await
            .expect("a failed tx must be skipped, not abort enumeration");
        assert!(set.is_empty(), "a failed tx must not mark a deposit minted");
    }

    /// What a mint-history scan made of one listed transaction.
    #[derive(Debug, PartialEq)]
    enum Verdict {
        Accepted(ConsumedMintKind),
        Skipped,
        Refused,
    }

    /// Lists one `history_memo` entry (`history_err` as its status error) on `mint`, answers
    /// its fetch with `reply`, and scans the mint's history into an empty set.
    async fn scan_one(
        mint: &Pubkey,
        landed: &Signature,
        history_memo: &str,
        history_err: bool,
        reply: String,
    ) -> Result<ConsumedSet, String> {
        let mut server = mockito::Server::new_async().await;
        let mut entry = sig_entry(&landed.to_string(), history_memo);
        if history_err {
            entry = entry.replace(
                r#""err":null"#,
                r#""err":{"InstructionError":[0,{"Custom":1}]}"#,
            );
        }
        let _history = history_for(&mut server, mint, &[entry]).await;
        let _tx = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getTransaction""#.into(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(reply)
            .create_async()
            .await;

        let mut set = ConsumedSet::new();
        let added =
            extend_with_mint_history(&fast_rpc(&server.url()), mint, PAGE_LIMIT, &mut set).await?;
        assert_eq!(added, set.len(), "the count must be what the scan added");
        Ok(set)
    }

    /// A mint's history counts any key that minted on it, since a successful `MintTo` proves
    /// that key held the authority then. Everything that did not mint on the mint is skipped,
    /// and a tx that did but cannot be authenticated aborts, including a decode gap.
    #[tokio::test]
    async fn mint_history_gate_matrix() {
        use ConsumedMintKind::{Deposit, Remint};
        use Verdict::{Accepted, Refused, Skipped};
        let mint = Pubkey::new_unique();
        let old_key = Pubkey::new_unique();
        let id = SourceEventId::new("evt-rotated", 0, None);
        let deposit = mint_idempotency_memo(&id);
        let remint = remint_idempotency_memo(&id);
        let other = mint_idempotency_memo(&SourceEventId::new("evt-other", 0, None));
        let legacy = "private_channel:mint-idempotency:42".to_string();
        let on_mint = mint_to_on(&mint, &old_key);
        let elsewhere = mint_to_by(&old_key);
        let raw = |memo: &str, mint_tos: &[MintToFields], meta_err: Value| {
            transaction_reply(&old_key, &Pubkey::new_unique(), memo, mint_tos, meta_err)
        };
        let parsed = |memo: &str, mint_tos: &[MintToFields]| {
            parsed_transaction_reply(&old_key, &Pubkey::new_unique(), memo, mint_tos)
        };
        let failed = json!({"InstructionError": [0, {"Custom": 1}]});

        let cases: Vec<(&str, String, bool, String, Verdict)> = vec![
            (
                "a: deposit by an old key",
                deposit.clone(),
                false,
                parsed(&deposit, &[on_mint]),
                Accepted(Deposit),
            ),
            (
                "a: raw deposit by an old key",
                deposit.clone(),
                false,
                raw(&deposit, &[on_mint], Value::Null),
                Accepted(Deposit),
            ),
            (
                "b: remint by an old key",
                remint.clone(),
                false,
                parsed(&remint, &[on_mint]),
                Accepted(Remint),
            ),
            (
                "c: marker without a MintTo",
                deposit.clone(),
                false,
                parsed(&deposit, &[]),
                Skipped,
            ),
            (
                "d: MintTo on another mint",
                deposit.clone(),
                false,
                parsed(&deposit, &[mint_to_by(&old_key)]),
                Skipped,
            ),
            (
                "e: failed meta",
                deposit.clone(),
                false,
                raw(&deposit, &[on_mint], failed.clone()),
                Skipped,
            ),
            (
                "f: failed history status",
                deposit.clone(),
                true,
                parsed(&deposit, &[on_mint]),
                Skipped,
            ),
            (
                "g: two markers",
                format!("{deposit}; {other}"),
                false,
                parsed(&deposit, &[on_mint]),
                Refused,
            ),
            (
                "h: legacy id",
                legacy.clone(),
                false,
                parsed(&legacy, &[on_mint]),
                Refused,
            ),
            (
                "i: no exact Memo instruction",
                deposit.clone(),
                false,
                parsed("unrelated-memo", &[on_mint]),
                Refused,
            ),
            (
                "j: two MintTos",
                deposit.clone(),
                false,
                parsed(&deposit, &[on_mint, mint_to_on(&mint, &old_key)]),
                Refused,
            ),
            (
                "k: multisig or extra accounts",
                deposit.clone(),
                false,
                with_multisig_authority(parsed(&deposit, &[on_mint]), &mint),
                Refused,
            ),
            (
                "k: second MintTo only the raw gate sees",
                deposit.clone(),
                false,
                with_multisig_authority(parsed(&deposit, &[on_mint, elsewhere]), &elsewhere.mint),
                Refused,
            ),
        ];
        for (case, history_memo, history_err, reply, expected) in cases {
            let landed = Signature::new_unique();
            let verdict = match scan_one(&mint, &landed, &history_memo, history_err, reply).await {
                Err(e) => {
                    assert!(
                        e.contains(&landed.to_string()) || e.contains("cutover"),
                        "{case}: {e}"
                    );
                    Refused
                }
                Ok(set) if set.is_empty() => Skipped,
                Ok(set) => {
                    let consumed = set.get(&id).unwrap_or_else(|| panic!("{case}: {set:?}"));
                    assert_eq!(consumed.signature, landed, "{case}");
                    assert_eq!(consumed.mint, mint, "{case}");
                    assert_eq!(consumed.recipient_ata, on_mint.recipient_ata, "{case}");
                    assert_eq!(consumed.amount, AMOUNT, "{case}");
                    Accepted(consumed.kind)
                }
            };
            assert_eq!(verdict, expected, "{case}");
        }
    }

    /// A signature already in the set is the same tx, so the mint scan must not fetch it again.
    #[tokio::test]
    async fn mint_history_skips_accepted_signatures() {
        let mut server = mockito::Server::new_async().await;
        let mint = Pubkey::new_unique();
        let landed = Signature::new_unique();
        let id = SourceEventId::new("evt-accepted", 0, None);
        let memo = mint_idempotency_memo(&id);
        let mint_to = mint_to_on(&mint, &Pubkey::new_unique());
        let accepted = ConsumedMint {
            signature: landed,
            kind: ConsumedMintKind::Deposit,
            mint,
            recipient_ata: mint_to.recipient_ata,
            token_program: mint_to.token_program,
            amount: AMOUNT,
        };
        let _history =
            history_for(&mut server, &mint, &[sig_entry(&landed.to_string(), &memo)]).await;
        let fetch = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getTransaction""#.into(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(parsed_transaction_reply(
                &mint_to.mint_authority,
                &Pubkey::new_unique(),
                &memo,
                &[mint_to],
            ))
            .expect(0)
            .create_async()
            .await;

        let mut set = ConsumedSet::from([(id.clone(), accepted)]);
        let added = extend_with_mint_history(&fast_rpc(&server.url()), &mint, PAGE_LIMIT, &mut set)
            .await
            .expect("an accepted signature is not an error");
        assert_eq!(added, 0);
        assert_eq!(set, ConsumedSet::from([(id, accepted)]));
        fetch.assert_async().await;
    }

    /// A second key's mint of an event the set already holds is a double issuance across a
    /// rotation, while the same signature again is the same mint.
    #[tokio::test]
    async fn mint_history_duplicate_event_matrix() {
        let mint = Pubkey::new_unique();
        let id = SourceEventId::new("evt-dup", 0, None);
        let memo = mint_idempotency_memo(&id);
        let first = Signature::new_unique();
        let mint_to = mint_to_on(&mint, &Pubkey::new_unique());
        let accepted = ConsumedMint {
            signature: first,
            kind: ConsumedMintKind::Deposit,
            mint,
            recipient_ata: mint_to.recipient_ata,
            token_program: mint_to.token_program,
            amount: AMOUNT,
        };

        for (case, listed, expect_err) in [
            ("same signature", first, false),
            ("other signature", Signature::new_unique(), true),
        ] {
            let mut server = mockito::Server::new_async().await;
            let _history =
                history_for(&mut server, &mint, &[sig_entry(&listed.to_string(), &memo)]).await;
            let _tx = server
                .mock("POST", "/")
                .match_body(mockito::Matcher::Regex(
                    r#""method"\s*:\s*"getTransaction""#.into(),
                ))
                .with_status(200)
                .with_header("content-type", "application/json")
                .with_body(parsed_transaction_reply(
                    &Pubkey::new_unique(),
                    &Pubkey::new_unique(),
                    &memo,
                    &[mint_to_on(&mint, &Pubkey::new_unique())],
                ))
                .create_async()
                .await;

            let mut set = ConsumedSet::from([(id.clone(), accepted)]);
            let result =
                extend_with_mint_history(&fast_rpc(&server.url()), &mint, PAGE_LIMIT, &mut set)
                    .await;
            match (result, expect_err) {
                (Ok(added), false) => assert_eq!(added, 0, "{case}"),
                (Err(e), true) => {
                    for signature in [first, listed] {
                        assert!(e.contains(&signature.to_string()), "{case}: {e}");
                    }
                }
                (other, _) => panic!("{case}: unexpected {other:?}"),
            }
            assert_eq!(
                set.get(&id),
                Some(&accepted),
                "{case}: the entry must not change"
            );
        }
    }

    // ── Claim-time consumed set ─────────────────────────────────────────────

    use super::{ConsumedLookup, ConsumedSetCache};
    use crate::storage::common::amount::TokenAmount;
    use crate::storage::common::storage::mock::MockStorage;
    use crate::storage::{DbTransaction, Storage, TransactionStatus, TransactionType};
    use spl_associated_token_account::get_associated_token_address_with_program_id;

    const WALK_PAGE: usize = 10;

    /// A row of `kind` on `mint` paid to `owner`, whose source event is `tag`.
    fn row(tag: &str, mint: &Pubkey, owner: &Pubkey, kind: TransactionType) -> DbTransaction {
        let now = chrono::Utc::now();
        DbTransaction {
            id: 1,
            signature: tag.to_string(),
            trace_id: format!("trace-{tag}"),
            slot: 100,
            initiator: owner.to_string(),
            recipient: owner.to_string(),
            mint: mint.to_string(),
            amount: TokenAmount(AMOUNT),
            memo: None,
            transaction_type: kind,
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
            recovery_requeue_attempts: 0,
            instruction_index: 0,
            inner_index: None,
            landed_remint_signature: None,
            release_refused_on_chain: false,
        }
    }

    /// The `MintTo` that pays `owner` on `mint`, signed by `authority`.
    fn paying(mint: &Pubkey, owner: &Pubkey, authority: &Pubkey) -> MintToFields {
        MintToFields {
            mint: *mint,
            recipient_ata: get_associated_token_address_with_program_id(
                owner,
                mint,
                &spl_token::id(),
            ),
            mint_authority: *authority,
            token_program: spl_token::id(),
            amount: AMOUNT,
        }
    }

    /// Serve `signature`'s transaction: a deposit mint for event `tag` paying `owner`.
    async fn deposit_tx(
        server: &mut mockito::ServerGuard,
        signature: &str,
        memo: &str,
        mint: &Pubkey,
        owner: &Pubkey,
    ) -> mockito::Mock {
        let authority = Pubkey::new_unique();
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::AllOf(vec![
                mockito::Matcher::Regex(r#""method"\s*:\s*"getTransaction""#.into()),
                mockito::Matcher::Regex(signature.to_string()),
            ]))
            .with_body(transaction_reply(
                &authority,
                &Pubkey::new_unique(),
                memo,
                &[paying(mint, owner, &authority)],
                Value::Null,
            ))
            .create_async()
            .await
    }

    /// A completed deposit the DB already knows, minted by `signature`.
    fn known_deposit(mock: &MockStorage, mint: &Pubkey, signature: &str) {
        let mut known = row(
            "known",
            mint,
            &Pubkey::new_unique(),
            TransactionType::Deposit,
        );
        known.status = TransactionStatus::Completed;
        known.counterpart_signature = Some(signature.to_string());
        mock.pending_transactions.lock().unwrap().push(known);
    }

    fn allow(mock: &MockStorage, mint: &Pubkey) {
        mock.mints.lock().unwrap().insert(
            mint.to_string(),
            crate::storage::common::models::DbMint::new(
                mint.to_string(),
                6,
                spl_token::id().to_string(),
                crate::storage::common::amount::TokenAmount(0),
            ),
        );
    }

    /// After a restore the two operators boot at different times, so the withdraw operator
    /// may already have landed a newer remint on the mint. The escrow walk must still reach
    /// its own newest known deposit mint, or a forgotten deposit mint between them is missed.
    #[tokio::test]
    async fn boot_consumed_set_stops_only_at_its_own_role() {
        let owner = Pubkey::new_unique();
        let mut server = mockito::Server::new_async().await;
        let mint = Pubkey::new_unique();
        let deposit_memo = |tag: &str| mint_idempotency_memo(&SourceEventId::new(tag, 0, None));
        let remint_memo = remint_idempotency_memo(&SourceEventId::new("burn", 0, None));
        let (remint, forgotten, known) = (
            Signature::new_unique().to_string(),
            Signature::new_unique().to_string(),
            Signature::new_unique().to_string(),
        );
        let _h = history_for(
            &mut server,
            &mint,
            &[
                sig_entry(&remint, &remint_memo),
                sig_entry(&forgotten, &deposit_memo("lost")),
                sig_entry(&known, &deposit_memo("kept")),
            ],
        )
        .await;
        let _r = deposit_tx(&mut server, &remint, &remint_memo, &mint, &owner).await;
        let _f = deposit_tx(
            &mut server,
            &forgotten,
            &deposit_memo("lost"),
            &mint,
            &owner,
        )
        .await;
        let mock = MockStorage::new();
        allow(&mock, &mint);
        known_deposit(&mock, &mint, &known);
        let mut reminted = row("burn", &mint, &owner, TransactionType::Withdrawal);
        reminted.status = TransactionStatus::FailedReminted;
        reminted.landed_remint_signature = Some(remint.clone());
        mock.pending_transactions.lock().unwrap().push(reminted);
        let storage = Storage::Mock(mock);
        let rpc = fast_rpc(&server.url());
        let mut cache =
            ConsumedSetCache::build_at_boot(&rpc, &storage, WALK_PAGE, TransactionType::Deposit)
                .await
                .expect("boot");
        let lost = row("lost", &mint, &owner, TransactionType::Deposit);
        assert!(
            matches!(
                cache.lookup(&rpc, &storage, &lost).await,
                Ok(ConsumedLookup::Hit(_))
            ),
            "the forgotten deposit mint below the newer remint must be found"
        );
    }

    /// Closing a forgotten deposit after a restore makes it the newest known mint. A second
    /// boot then stops there, so an older forgotten mint is only known from the first walk.
    #[tokio::test]
    async fn consumed_set_survives_a_restart_mid_recovery() {
        let owner = Pubkey::new_unique();
        let mut server = mockito::Server::new_async().await;
        let mint = Pubkey::new_unique();
        let memo = |tag: &str| mint_idempotency_memo(&SourceEventId::new(tag, 0, None));
        let (a, b, known) = (
            Signature::new_unique().to_string(),
            Signature::new_unique().to_string(),
            Signature::new_unique().to_string(),
        );
        let _h = history_for(
            &mut server,
            &mint,
            &[
                sig_entry(&a, &memo("a")),
                sig_entry(&b, &memo("b")),
                sig_entry(&known, &memo("kept")),
            ],
        )
        .await;
        let _a = deposit_tx(&mut server, &a, &memo("a"), &mint, &owner).await;
        let _b = deposit_tx(&mut server, &b, &memo("b"), &mint, &owner).await;
        let mock = MockStorage::new();
        allow(&mock, &mint);
        known_deposit(&mock, &mint, &known);
        let storage = Storage::Mock(mock.clone());
        let rpc = fast_rpc(&server.url());
        ConsumedSetCache::build_at_boot(&rpc, &storage, WALK_PAGE, TransactionType::Deposit)
            .await
            .expect("first boot");

        // The operator closes A from the set, then restarts before B is claimed.
        known_deposit(&mock, &mint, &a);
        let mut cache =
            ConsumedSetCache::build_at_boot(&rpc, &storage, WALK_PAGE, TransactionType::Deposit)
                .await
                .expect("second boot");
        let forgotten = row("b", &mint, &owner, TransactionType::Deposit);
        assert!(
            matches!(
                cache.lookup(&rpc, &storage, &forgotten).await,
                Ok(ConsumedLookup::Hit(_))
            ),
            "the forgotten mint found by the first walk must still be known"
        );
    }

    /// The withdraw operator settles remints only, so its walk must not fetch the deposit
    /// mints that make up nearly all of a mint's history. None is served here.
    #[tokio::test]
    async fn withdraw_walk_fetches_remints_only() {
        let owner = Pubkey::new_unique();
        let mut server = mockito::Server::new_async().await;
        let mint = Pubkey::new_unique();
        let deposit_memo = mint_idempotency_memo(&SourceEventId::new("dep", 0, None));
        let remint_memo = remint_idempotency_memo(&SourceEventId::new("burn", 0, None));
        let (remint, deposit) = (
            Signature::new_unique().to_string(),
            Signature::new_unique().to_string(),
        );
        let _h = history_for(
            &mut server,
            &mint,
            &[
                sig_entry(&remint, &remint_memo),
                sig_entry(&deposit, &deposit_memo),
            ],
        )
        .await;
        let _r = deposit_tx(&mut server, &remint, &remint_memo, &mint, &owner).await;
        let mock = MockStorage::new();
        allow(&mock, &mint);
        let storage = Storage::Mock(mock);
        let rpc = fast_rpc(&server.url());
        let mut cache =
            ConsumedSetCache::build_at_boot(&rpc, &storage, WALK_PAGE, TransactionType::Withdrawal)
                .await
                .expect("boot must not fetch the deposit mint");
        let burn = row("burn", &mint, &owner, TransactionType::Withdrawal);
        assert!(matches!(
            cache.lookup(&rpc, &storage, &burn).await,
            Ok(ConsumedLookup::Hit(_))
        ));
    }

    /// `admin truncate` prunes old channel history, so an idle mint's newest known mint can
    /// fall below the floor. That walk is accepted; a full history that lacks it is a rewind.
    #[tokio::test]
    async fn boot_consumed_set_pruned_history() {
        let owner = Pubkey::new_unique();
        let memo = mint_idempotency_memo(&SourceEventId::new("recent", 0, None));
        // (first available block, boots)
        for (floor, boots) in [(500, true), (0, false)] {
            let mut server = mockito::Server::new_async().await;
            let mint = Pubkey::new_unique();
            let recent = Signature::new_unique().to_string();
            let _h = history_for(&mut server, &mint, &[sig_entry(&recent, &memo)]).await;
            let _t = deposit_tx(&mut server, &recent, &memo, &mint, &owner).await;
            let _f = server
                .mock("POST", "/")
                .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                    "method": "getFirstAvailableBlock"
                })))
                .with_body(
                    serde_json::json!({"jsonrpc": "2.0", "id": 1, "result": floor}).to_string(),
                )
                .create_async()
                .await;
            let mock = MockStorage::new();
            allow(&mock, &mint);
            // The newest known mint was pruned away with the rest of the old history.
            known_deposit(&mock, &mint, &Signature::new_unique().to_string());
            let result = ConsumedSetCache::build_at_boot(
                &fast_rpc(&server.url()),
                &Storage::Mock(mock),
                WALK_PAGE,
                TransactionType::Deposit,
            )
            .await;
            assert_eq!(result.is_ok(), boots, "floor {floor}: {:?}", result.err());
            if let Ok(cache) = result {
                assert_eq!(cache.len(), 1, "the mint above the floor is still read");
            }
        }
    }

    #[tokio::test]
    async fn boot_consumed_set_matrix() {
        let owner = Pubkey::new_unique();
        let memo_for = |tag: &str| mint_idempotency_memo(&SourceEventId::new(tag, 0, None));

        // Stop met: only mints newer than the newest known one are read.
        {
            let mut server = mockito::Server::new_async().await;
            let mint = Pubkey::new_unique();
            let (newer, known) = (
                Signature::new_unique().to_string(),
                Signature::new_unique().to_string(),
            );
            let _h = history_for(
                &mut server,
                &mint,
                &[
                    sig_entry(&newer, &memo_for("lost")),
                    sig_entry(&known, &memo_for("kept")),
                ],
            )
            .await;
            let _t = deposit_tx(&mut server, &newer, &memo_for("lost"), &mint, &owner).await;
            let mock = MockStorage::new();
            allow(&mock, &mint);
            known_deposit(&mock, &mint, &known);
            let storage = Storage::Mock(mock);
            let mut cache = ConsumedSetCache::build_at_boot(
                &fast_rpc(&server.url()),
                &storage,
                WALK_PAGE,
                TransactionType::Deposit,
            )
            .await
            .expect("stop met");
            let lost = row("lost", &mint, &owner, TransactionType::Deposit);
            assert!(matches!(
                cache
                    .lookup(&fast_rpc(&server.url()), &storage, &lost)
                    .await,
                Ok(ConsumedLookup::Hit(_))
            ));
            assert_eq!(cache.len(), 1, "the known mint itself is never read");
        }

        // No known mint: the whole history is walked.
        {
            let mut server = mockito::Server::new_async().await;
            let mint = Pubkey::new_unique();
            let (a, b) = (
                Signature::new_unique().to_string(),
                Signature::new_unique().to_string(),
            );
            let _h = history_for(
                &mut server,
                &mint,
                &[sig_entry(&a, &memo_for("a")), sig_entry(&b, &memo_for("b"))],
            )
            .await;
            let _ta = deposit_tx(&mut server, &a, &memo_for("a"), &mint, &owner).await;
            let _tb = deposit_tx(&mut server, &b, &memo_for("b"), &mint, &owner).await;
            let mock = MockStorage::new();
            allow(&mock, &mint);
            let cache = ConsumedSetCache::build_at_boot(
                &fast_rpc(&server.url()),
                &Storage::Mock(mock),
                WALK_PAGE,
                TransactionType::Deposit,
            )
            .await
            .expect("full walk");
            assert_eq!(cache.len(), 2);
        }

        // A known mint the history never reaches (rewound or pruned below it) refuses.
        {
            let mut server = mockito::Server::new_async().await;
            let mint = Pubkey::new_unique();
            let a = Signature::new_unique().to_string();
            let _h = history_for(&mut server, &mint, &[sig_entry(&a, &memo_for("a"))]).await;
            let _ta = deposit_tx(&mut server, &a, &memo_for("a"), &mint, &owner).await;
            let mock = MockStorage::new();
            allow(&mock, &mint);
            known_deposit(&mock, &mint, &Signature::new_unique().to_string());
            let err = ConsumedSetCache::build_at_boot(
                &fast_rpc(&server.url()),
                &Storage::Mock(mock),
                WALK_PAGE,
                TransactionType::Deposit,
            )
            .await
            .err()
            .expect("an unmet stop must refuse");
            assert!(err.contains("never reached"), "{err}");
        }

        // A legacy memo above the stop fails closed; one below it is never read.
        for above in [true, false] {
            let mut server = mockito::Server::new_async().await;
            let mint = Pubkey::new_unique();
            let (legacy, known) = (
                Signature::new_unique().to_string(),
                Signature::new_unique().to_string(),
            );
            let legacy_memo = format!("{}legacy-id", crate::operator::MINT_IDEMPOTENCY_MEMO_PREFIX);
            let entries = if above {
                vec![
                    sig_entry(&legacy, &legacy_memo),
                    sig_entry(&known, &memo_for("k")),
                ]
            } else {
                vec![
                    sig_entry(&known, &memo_for("k")),
                    sig_entry(&legacy, &legacy_memo),
                ]
            };
            let _h = history_for(&mut server, &mint, &entries).await;
            let legacy_tx = deposit_tx(&mut server, &legacy, &legacy_memo, &mint, &owner).await;
            let mock = MockStorage::new();
            allow(&mock, &mint);
            known_deposit(&mock, &mint, &known);
            let result = ConsumedSetCache::build_at_boot(
                &fast_rpc(&server.url()),
                &Storage::Mock(mock),
                WALK_PAGE,
                TransactionType::Deposit,
            )
            .await;
            assert_eq!(
                result.is_err(),
                above,
                "legacy memo above the stop: {above}"
            );
            assert_eq!(legacy_tx.matched_async().await, above);
        }

        // Two mints are walked against their own stops.
        {
            let mut server = mockito::Server::new_async().await;
            let (m1, m2) = (Pubkey::new_unique(), Pubkey::new_unique());
            let (a1, k1, a2) = (
                Signature::new_unique().to_string(),
                Signature::new_unique().to_string(),
                Signature::new_unique().to_string(),
            );
            let _h1 = history_for(
                &mut server,
                &m1,
                &[
                    sig_entry(&a1, &memo_for("a1")),
                    sig_entry(&k1, &memo_for("k1")),
                ],
            )
            .await;
            let _h2 = history_for(&mut server, &m2, &[sig_entry(&a2, &memo_for("a2"))]).await;
            let _t1 = deposit_tx(&mut server, &a1, &memo_for("a1"), &m1, &owner).await;
            let _t2 = deposit_tx(&mut server, &a2, &memo_for("a2"), &m2, &owner).await;
            let mock = MockStorage::new();
            allow(&mock, &m1);
            allow(&mock, &m2);
            known_deposit(&mock, &m1, &k1);
            let cache = ConsumedSetCache::build_at_boot(
                &fast_rpc(&server.url()),
                &Storage::Mock(mock),
                WALK_PAGE,
                TransactionType::Deposit,
            )
            .await
            .expect("both mints");
            assert_eq!(cache.len(), 2);
        }

        // A mint allowed after the restore target is walked on its first claim, once.
        {
            let mut server = mockito::Server::new_async().await;
            let late = Pubkey::new_unique();
            let a = Signature::new_unique().to_string();
            let history = history_for(&mut server, &late, &[sig_entry(&a, &memo_for("late"))])
                .await
                .expect(1);
            let _t = deposit_tx(&mut server, &a, &memo_for("late"), &late, &owner).await;
            let storage = Storage::Mock(MockStorage::new());
            let rpc = fast_rpc(&server.url());
            let mut cache = ConsumedSetCache::build_at_boot(
                &rpc,
                &storage,
                WALK_PAGE,
                TransactionType::Deposit,
            )
            .await
            .unwrap();
            assert_eq!(cache.len(), 0);
            let claimed = row("late", &late, &owner, TransactionType::Deposit);
            assert!(matches!(
                cache.lookup(&rpc, &storage, &claimed).await,
                Ok(ConsumedLookup::Hit(_))
            ));
            let other = row("other", &late, &owner, TransactionType::Deposit);
            assert!(matches!(
                cache.lookup(&rpc, &storage, &other).await,
                Ok(ConsumedLookup::Miss)
            ));
            history.assert_async().await;
        }
    }

    /// A hit that pays someone else is not proof this row was paid.
    #[tokio::test]
    async fn consumed_lookup_reports_a_payload_mismatch() {
        let mut server = mockito::Server::new_async().await;
        let mint = Pubkey::new_unique();
        let memo = mint_idempotency_memo(&SourceEventId::new("evt", 0, None));
        let a = Signature::new_unique().to_string();
        let _h = history_for(&mut server, &mint, &[sig_entry(&a, &memo)]).await;
        let _t = deposit_tx(&mut server, &a, &memo, &mint, &Pubkey::new_unique()).await;
        let mock = MockStorage::new();
        allow(&mock, &mint);
        let storage = Storage::Mock(mock);
        let rpc = fast_rpc(&server.url());
        let mut cache =
            ConsumedSetCache::build_at_boot(&rpc, &storage, WALK_PAGE, TransactionType::Deposit)
                .await
                .unwrap();
        let claimed = row(
            "evt",
            &mint,
            &Pubkey::new_unique(),
            TransactionType::Deposit,
        );
        assert!(matches!(
            cache.lookup(&rpc, &storage, &claimed).await,
            Ok(ConsumedLookup::Mismatch(_))
        ));
    }
}
