use crate::operator::utils::instruction_util::{
    InitializeMintBuilder, MintToBuilderWithTxnId, TransactionBuilder,
};
use crate::operator::utils::transaction_util::{check_transaction_status, ConfirmationResult};
use crate::operator::{
    sign_and_send_transaction, RpcClientWithRetry, SignerUtil, SourceEventId,
    MINT_IDEMPOTENCY_MEMO_PREFIX, MINT_IDEMPOTENCY_SIGNATURE_LOOKBACK_LIMIT,
    REMINT_IDEMPOTENCY_MEMO_PREFIX,
};
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
use spl_token::solana_program::program_pack::Pack;
use spl_token::state::Mint;
use std::collections::HashMap;
use std::str::FromStr;
use tracing::{error, info, warn};

use super::types::{InstructionWithSigners, SenderState};

#[derive(Clone, Copy, Debug)]
struct ExpectedMintInstruction {
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
/// `mint_authority` to the operator's admin pubkey. Drives the
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
    /// Mint decoded but `is_initialized = false`. The account is allocated
    /// and SPL-Token-owned but not yet a mint. JIT proceeds to
    /// InitializeMint.
    Uninitialized,
    /// Decode failed: data length wrong, COption discriminant invalid, or
    /// other corruption. Quarantine to ManualReview — the operator cannot
    /// recover this without engineering intervention.
    CorruptData,
}

/// Decode `data` as an SPL `Mint` and classify it relative to the supplied
/// expected authority.
///
/// SPL's `Mint::unpack` rejects uninitialized data (returns `Err`), which
/// would conflate the legitimate "mint allocated but `is_initialized =
/// false`" state with genuine corruption. To distinguish, this helper uses
/// `Mint::unpack_unchecked` and inspects `is_initialized` itself.
///
/// Returns:
/// - `Match` if decoded, initialized, and `mint_authority` equals the
///   supplied `expected_authority`.
/// - `Mismatch(actual)` if decoded and initialized but the authority
///   differs. A mint with `mint_authority = COption::None` is treated as
///   `Mismatch(Pubkey::default())` — the operator cannot `mint_to` a
///   no-authority mint either way.
/// - `Uninitialized` if the bytes decode but `is_initialized = false`
///   (allocated, SPL-Token-owned, but not yet a mint).
/// - `CorruptData` for any decode failure (wrong length, invalid `COption`
///   discriminant, etc.).
pub(super) fn decode_and_check_authority(
    data: &[u8],
    expected_authority: &Pubkey,
) -> AuthorityCheck {
    // `unpack_unchecked` differs from `unpack` only in skipping the
    // is_initialized check, so wrong-length / invalid-discriminant inputs
    // still surface as `Err` here.
    let mint = match Mint::unpack_unchecked(data) {
        Ok(m) => m,
        Err(_) => return AuthorityCheck::CorruptData,
    };
    if !mint.is_initialized {
        return AuthorityCheck::Uninitialized;
    }
    match mint.mint_authority {
        COption::Some(actual) if actual == *expected_authority => AuthorityCheck::Match,
        COption::Some(actual) => AuthorityCheck::Mismatch(actual),
        COption::None => AuthorityCheck::Mismatch(Pubkey::default()),
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

    // 2. Pre-check on-chain mint state.
    match state.rpc_client.get_account_data(&mint).await {
        Ok(data) => match decode_and_check_authority(&data, &admin_pubkey) {
            AuthorityCheck::Match => return JitOutcome::Retry(instruction),
            AuthorityCheck::Mismatch(actual) => {
                warn!(
                    "JIT pre-check: mint {} initialized with authority {} (expected admin {})",
                    mint, actual, admin_pubkey
                );
                return JitOutcome::ManualReview(MR_AUTHORITY_MISMATCH_PRECHECK.to_string());
            }
            AuthorityCheck::CorruptData => {
                warn!(
                    "JIT pre-check: mint {} bytes do not decode as SPL Mint",
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

    // Look up the mint's decimals. This is a pure metadata read,
    // only `decimals` is used.
    //
    // `get_mint_metadata` checks the in-memory cache first, then the
    // DB, and finally falls back to a source-chain RPC fetch if
    // neither has the mint. That last fallback would be dangerous on
    // its own: it would let any source-chain mint be resolved
    // here, initialized on the private channel, and minted into a
    // user's account.
    //
    // It's safe in this position because the deposit processor
    // already refuses to forward a deposit whose mint was not in
    // allowed status at the deposit's slot (see
    // `assert_mint_allowed_at_slot` in `process_deposit_funds`). By the
    // time execution reaches this point, the mint is known-allowed.
    let Ok(mint_metadata) = state.mint_cache.get_mint_metadata(&mint).await else {
        error!("Mint {} not found in mint cache", mint);
        return JitOutcome::Transient(format!("mint not in mint cache: {}", mint));
    };

    info!(
        "Found mint metadata: {} decimals for {}",
        mint_metadata.decimals, mint
    );

    // 4. Build InitializeMint transaction.
    let init_mint_builder = InitializeMintBuilder::new(
        mint,
        mint_metadata.decimals,
        admin_pubkey,
        state.mint_cache.get_private_channel_token_program(),
        admin_pubkey,
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
                Ok(data) => decode_and_check_authority(&data, &admin_pubkey),
                Err(_) => {
                    mint_authority_check_with_backoff(&state.rpc_client, &mint, &admin_pubkey).await
                }
            };
            jit_verdict(check, instruction, &mint, None)
        }
        _ => {
            // Fallback for unknown failures (network blips, timeouts): re-read
            // the mint on-chain with backoff in case it was initialized out-of-band.
            let check =
                mint_authority_check_with_backoff(&state.rpc_client, &mint, &admin_pubkey).await;
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
        AuthorityCheck::CorruptData => {
            warn!("JIT: mint {} bytes do not decode as SPL Mint", mint);
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
) -> AuthorityCheck {
    const ATTEMPTS: u32 = 4;
    const BACKOFF_MS: u64 = 250;

    let mut last_check = AuthorityCheck::Uninitialized;
    for attempt in 0..ATTEMPTS {
        match rpc_client.get_account_data(mint).await {
            Ok(data) => {
                let check = decode_and_check_authority(&data, expected_authority);
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

/// Set of source events the PrivateChannel has already serviced, keyed by their durable
/// source-event-id. Built once, before a resync wipe, so the rebuild can reconcile each
/// row to its terminal state.
pub type ConsumedSet = HashMap<SourceEventId, (Signature, ConsumedMintKind)>;

/// Enumerate every idempotency-memo'd mint the `authority` has confirmed on the channel
/// into a `ConsumedSet`.
///
/// Fails closed on any RPC/pagination error, and on an idempotency-prefixed memo that
/// doesn't parse to a current source-event-id (a serviced mint resync can't reconcile —
/// proceeding would re-mint it).
pub async fn enumerate_consumed_mints(
    rpc: &RpcClientWithRetry,
    authority: &Pubkey,
    page_limit: usize,
) -> Result<ConsumedSet, String> {
    let signatures = rpc
        .get_signatures_for_address_paginated(authority, page_limit)
        .await
        .map_err(|e| {
            format!("consumed-set enumeration failed listing signatures for {authority}: {e}")
        })?;

    let mut set = ConsumedSet::new();
    for status in signatures {
        if status.err.is_some() {
            continue;
        }
        let Some(memo) = status.memo.as_deref() else {
            continue;
        };

        // A memo field can carry several "; "-joined entries, each possibly length-prefixed.
        for piece in memo.split("; ") {
            let value = strip_memo_length_prefix(piece);
            let (kind, encoded) =
                if let Some(rest) = value.strip_prefix(MINT_IDEMPOTENCY_MEMO_PREFIX) {
                    (ConsumedMintKind::Deposit, rest)
                } else if let Some(rest) = value.strip_prefix(REMINT_IDEMPOTENCY_MEMO_PREFIX) {
                    (ConsumedMintKind::Remint, rest)
                } else {
                    continue;
                };

            let Some(source_event_id) = SourceEventId::from_encoded(encoded) else {
                return Err(format!(
                    "channel mint {} carries an idempotency memo that does not parse to a \
                     current-scheme source-event-id (legacy memo scheme); resync cannot reconcile \
                     across the memo cutover - see docs/runbooks/mint_memo_cutover.md",
                    status.signature
                ));
            };

            let signature = Signature::from_str(&status.signature)
                .map_err(|e| format!("invalid signature {} from RPC: {e}", status.signature))?;
            // First (newest) confirmed mint for an id wins; a re-send would tie anyway.
            set.entry(source_event_id).or_insert((signature, kind));
        }
    }
    Ok(set)
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

/// Cleanup mint builder cache when transaction completes or fails
/// Check recent ATA signatures for an already-confirmed mint carrying the given memo.
/// Any RPC failure (including `-32601`) is returned as `Err` — callers decide.
pub async fn find_existing_mint_signature_with_memo(
    rpc_client: &RpcClientWithRetry,
    builder_with_txn_id: &MintToBuilderWithTxnId,
    expected_memo: &str,
) -> Result<Option<Signature>, String> {
    let transaction_id = builder_with_txn_id.txn_id;
    let Some(expected_mint) = expected_mint_instruction(transaction_id, builder_with_txn_id) else {
        return Ok(None);
    };

    let signatures = match rpc_client
        .get_signatures_for_address(
            &expected_mint.recipient_ata,
            MINT_IDEMPOTENCY_SIGNATURE_LOOKBACK_LIMIT,
        )
        .await
    {
        Ok(signatures) => signatures,
        Err(e) => {
            return Err(format!(
                "Failed idempotency lookup for transaction_id {} on {}: {}",
                transaction_id, expected_mint.recipient_ata, e
            ));
        }
    };

    for signature_status in signatures {
        if signature_status.err.is_some() {
            continue;
        }

        let memo = match signature_status.memo.as_deref() {
            Some(memo) if memo_matches(memo, expected_memo) => memo,
            _ => continue,
        };

        let signature = match Signature::from_str(&signature_status.signature) {
            Ok(signature) => signature,
            Err(e) => {
                warn!(
                    "Skipping invalid signature returned by RPC during idempotency check: {} ({})",
                    signature_status.signature, e
                );
                continue;
            }
        };

        let transaction = match rpc_client.get_transaction(&signature).await {
            Ok(transaction) => transaction,
            Err(e) => {
                return Err(format!(
                    "Failed to fetch transaction {} for idempotency confirmation: {}",
                    signature, e
                ));
            }
        };

        if transaction_matches_expected_mint(&transaction, expected_memo, &expected_mint) {
            info!(
                "Skipping resend for transaction_id {}: found existing confirmed mint {} with memo {}",
                transaction_id, signature, memo
            );
            return Ok(Some(signature));
        }
    }

    Ok(None)
}

fn expected_mint_instruction(
    transaction_id: i64,
    builder_with_txn_id: &MintToBuilderWithTxnId,
) -> Option<ExpectedMintInstruction> {
    let (mint, recipient_ata, mint_authority, token_program, amount) =
        builder_with_txn_id.builder.try_as_expected_mint().or_else(|| {
            warn!(
                "Cannot run mint idempotency check for transaction_id {}: builder fields incomplete",
                transaction_id
            );
            None
        })?;
    Some(ExpectedMintInstruction {
        mint,
        recipient_ata,
        mint_authority,
        token_program,
        amount,
    })
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

fn transaction_matches_expected_mint(
    transaction: &solana_transaction_status::EncodedConfirmedTransactionWithStatusMeta,
    expected_memo: &str,
    expected_mint: &ExpectedMintInstruction,
) -> bool {
    if !transaction_succeeded(transaction) {
        return false;
    }

    let EncodedTransaction::Json(ui_transaction) = &transaction.transaction.transaction else {
        return false;
    };

    match &ui_transaction.message {
        UiMessage::Parsed(parsed_message) => {
            parsed_message_has_signer(parsed_message, &expected_mint.mint_authority)
                && parsed_message
                    .instructions
                    .iter()
                    .any(|instruction| instruction_has_memo(instruction, expected_memo))
                && parsed_message
                    .instructions
                    .iter()
                    .any(|instruction| instruction_has_expected_mint(instruction, expected_mint))
        }
        UiMessage::Raw(raw_message) => {
            raw_message_has_signer(raw_message, &expected_mint.mint_authority)
                && raw_message.instructions.iter().any(|instruction| {
                    raw_instruction_has_memo(raw_message, instruction, expected_memo)
                })
                && raw_message.instructions.iter().any(|instruction| {
                    raw_instruction_has_expected_mint(raw_message, instruction, expected_mint)
                })
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

fn instruction_has_expected_mint(
    instruction: &UiInstruction,
    expected_mint: &ExpectedMintInstruction,
) -> bool {
    match instruction {
        UiInstruction::Compiled(_) => false,
        UiInstruction::Parsed(UiParsedInstruction::Parsed(parsed_instruction)) => {
            parsed_instruction_has_expected_mint(parsed_instruction, expected_mint)
        }
        UiInstruction::Parsed(UiParsedInstruction::PartiallyDecoded(partially_decoded)) => {
            partially_decoded_instruction_has_expected_mint(partially_decoded, expected_mint)
        }
    }
}

fn parsed_instruction_has_expected_mint(
    parsed_instruction: &ParsedInstruction,
    expected_mint: &ExpectedMintInstruction,
) -> bool {
    if parse_pubkey(&parsed_instruction.program_id) != Some(expected_mint.token_program) {
        return false;
    }

    let Some(instruction_type) = parsed_instruction
        .parsed
        .get("type")
        .and_then(Value::as_str)
    else {
        return false;
    };

    if instruction_type != "mintTo" && instruction_type != "mintToChecked" {
        return false;
    }

    let Some(info) = parsed_instruction.parsed.get("info") else {
        return false;
    };

    if parse_pubkey_field(info, "mint") != Some(expected_mint.mint)
        || parse_pubkey_field(info, "account") != Some(expected_mint.recipient_ata)
        || parse_pubkey_field(info, "mintAuthority") != Some(expected_mint.mint_authority)
    {
        return false;
    }

    let amount = match instruction_type {
        "mintTo" => parse_u64_field(info, "amount"),
        "mintToChecked" => info
            .get("tokenAmount")
            .and_then(|token_amount| parse_u64_field(token_amount, "amount")),
        _ => None,
    };

    amount == Some(expected_mint.amount)
}

fn accounts_and_amount_match(
    program_id: &Pubkey,
    mint: &Pubkey,
    recipient_ata: &Pubkey,
    mint_authority: &Pubkey,
    instruction_data: &[u8],
    expected: &ExpectedMintInstruction,
) -> bool {
    *program_id == expected.token_program
        && *mint == expected.mint
        && *recipient_ata == expected.recipient_ata
        && *mint_authority == expected.mint_authority
        && parse_token_instruction_mint_amount(program_id, instruction_data)
            == Some(expected.amount)
}

fn partially_decoded_instruction_has_expected_mint(
    partially_decoded: &UiPartiallyDecodedInstruction,
    expected_mint: &ExpectedMintInstruction,
) -> bool {
    let Some(program_id) = parse_pubkey(&partially_decoded.program_id) else {
        return false;
    };
    let Some(mint) = partially_decoded
        .accounts
        .first()
        .and_then(|a| parse_pubkey(a))
    else {
        return false;
    };
    let Some(recipient_ata) = partially_decoded
        .accounts
        .get(1)
        .and_then(|a| parse_pubkey(a))
    else {
        return false;
    };
    let Some(mint_authority) = partially_decoded
        .accounts
        .get(2)
        .and_then(|a| parse_pubkey(a))
    else {
        return false;
    };
    let Ok(data) = bs58::decode(&partially_decoded.data).into_vec() else {
        return false;
    };
    accounts_and_amount_match(
        &program_id,
        &mint,
        &recipient_ata,
        &mint_authority,
        &data,
        expected_mint,
    )
}

fn raw_instruction_has_expected_mint(
    raw_message: &UiRawMessage,
    instruction: &UiCompiledInstruction,
    expected_mint: &ExpectedMintInstruction,
) -> bool {
    let Some(program_id) = raw_message
        .account_keys
        .get(instruction.program_id_index as usize)
        .and_then(|a| parse_pubkey(a))
    else {
        return false;
    };
    let Some(mint) = instruction
        .accounts
        .first()
        .and_then(|i| raw_message.account_keys.get(*i as usize))
        .and_then(|a| parse_pubkey(a))
    else {
        return false;
    };
    let Some(recipient_ata) = instruction
        .accounts
        .get(1)
        .and_then(|i| raw_message.account_keys.get(*i as usize))
        .and_then(|a| parse_pubkey(a))
    else {
        return false;
    };
    let Some(mint_authority) = instruction
        .accounts
        .get(2)
        .and_then(|i| raw_message.account_keys.get(*i as usize))
        .and_then(|a| parse_pubkey(a))
    else {
        return false;
    };
    let Ok(data) = bs58::decode(&instruction.data).into_vec() else {
        return false;
    };
    accounts_and_amount_match(
        &program_id,
        &mint,
        &recipient_ata,
        &mint_authority,
        &data,
        expected_mint,
    )
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

fn memo_matches(returned_memo: &str, expected_memo: &str) -> bool {
    returned_memo
        .split("; ")
        .any(|memo| strip_memo_length_prefix(memo) == expected_memo)
}

pub(super) fn cleanup_mint_builder(state: &mut SenderState, transaction_id: Option<i64>) {
    if let Some(txn_id) = transaction_id {
        state.mint_builders.remove(&txn_id);
    }
}

#[cfg(test)]
mod tests {
    use super::{decode_and_check_authority, AuthorityCheck};
    use solana_sdk::pubkey::Pubkey;
    use spl_token::solana_program::program_option::COption;
    use spl_token::solana_program::program_pack::Pack;
    use spl_token::state::Mint;

    // Tests for `decode_and_check_authority`, the pure helper that drives
    // the JIT pre-check, post-confirm re-check, and fallback backoff. Four
    // variants must each be reachable: Match / Mismatch / Uninitialized /
    // CorruptData.

    fn pack_mint(is_initialized: bool, authority: COption<Pubkey>) -> Vec<u8> {
        let mint = Mint {
            mint_authority: authority,
            supply: 0,
            decimals: 6,
            is_initialized,
            freeze_authority: COption::None,
        };
        let mut data = vec![0u8; Mint::LEN];
        Mint::pack(mint, &mut data).expect("pack mint");
        data
    }

    /// Initialized mint with `mint_authority` matching the supplied admin.
    #[test]
    fn decode_and_check_authority_match_returns_match() {
        let admin = Pubkey::new_unique();
        let data = pack_mint(true, COption::Some(admin));
        assert_eq!(
            decode_and_check_authority(&data, &admin),
            AuthorityCheck::Match
        );
    }

    /// Initialized mint with `mint_authority` set to a different pubkey
    /// (the rotated-admin / different-operator scenario). Helper must
    /// surface the actual authority for log/error context.
    #[test]
    fn decode_and_check_authority_mismatch_returns_mismatch() {
        let admin = Pubkey::new_unique();
        let other = Pubkey::new_unique();
        let data = pack_mint(true, COption::Some(other));
        match decode_and_check_authority(&data, &admin) {
            AuthorityCheck::Mismatch(actual) => assert_eq!(actual, other),
            other => panic!("expected Mismatch, got {:?}", other),
        }
    }

    /// Initialized mint whose authority has been cleared (`COption::None`)
    /// is treated as a mismatch with `Pubkey::default()` — the operator
    /// cannot mint without an authority either way.
    #[test]
    fn decode_and_check_authority_no_authority_returns_mismatch_default() {
        let admin = Pubkey::new_unique();
        let data = pack_mint(true, COption::None);
        match decode_and_check_authority(&data, &admin) {
            AuthorityCheck::Mismatch(actual) => assert_eq!(actual, Pubkey::default()),
            other => panic!("expected Mismatch, got {:?}", other),
        }
    }

    /// Mint allocated and SPL-Token-owned but with `is_initialized = false`
    /// (the legitimate JIT case where InitializeMint should run).
    #[test]
    fn decode_and_check_authority_uninitialized_returns_uninitialized() {
        let admin = Pubkey::new_unique();
        let data = pack_mint(false, COption::Some(admin));
        assert_eq!(
            decode_and_check_authority(&data, &admin),
            AuthorityCheck::Uninitialized
        );
    }

    /// Empty account data — the mint account was never created. Pre-fix
    /// this returned `false` from the old bool helper; the new helper
    /// reports `CorruptData` (decode-fail) rather than `Uninitialized`,
    /// because there is no reliable way to distinguish "account doesn't
    /// exist" from "account has wrong-length corrupt data" via account
    /// data alone. Both cases need JIT to attempt InitializeMint, but the
    /// caller distinguishes by also checking `Err` from the RPC; this test
    /// pins that empty/short data lands in `CorruptData`.
    #[test]
    fn decode_and_check_authority_empty_returns_corrupt() {
        let admin = Pubkey::new_unique();
        assert_eq!(
            decode_and_check_authority(&[], &admin),
            AuthorityCheck::CorruptData
        );
    }

    /// Data of the wrong length cannot decode as a mint.
    #[test]
    fn decode_and_check_authority_wrong_length_returns_corrupt() {
        let admin = Pubkey::new_unique();
        assert_eq!(
            decode_and_check_authority(&[0u8; 10], &admin),
            AuthorityCheck::CorruptData
        );
        assert_eq!(
            decode_and_check_authority(&[0xFFu8; Mint::LEN + 1], &admin),
            AuthorityCheck::CorruptData
        );
    }

    /// Random bytes of the correct length usually contain an invalid
    /// `COption` discriminant byte — `Mint::unpack` rejects them.
    #[test]
    fn decode_and_check_authority_random_bytes_returns_corrupt() {
        let admin = Pubkey::new_unique();
        let data: Vec<u8> = (0u8..Mint::LEN as u8).collect();
        assert_eq!(
            decode_and_check_authority(&data, &admin),
            AuthorityCheck::CorruptData
        );
    }
}

#[cfg(test)]
mod consumed_set_tests {
    use super::{enumerate_consumed_mints, ConsumedMintKind};
    use crate::operator::instruction_util::{mint_idempotency_memo, SourceEventId};
    use crate::operator::{RetryConfig, RpcClientWithRetry};
    use solana_commitment_config::CommitmentConfig;
    use solana_sdk::pubkey::Pubkey;
    use solana_sdk::signature::Signature;

    const PAGE_LIMIT: usize = 2;

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
                sig_entry(&page1_a, &mint_idempotency_memo(&id1)),
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
                sig_entry(&page2_a, &mint_idempotency_memo(&id2)),
            ))
            .create_async()
            .await;

        let rpc = fast_rpc(&server.url());
        let set = enumerate_consumed_mints(&rpc, &authority, PAGE_LIMIT)
            .await
            .expect("enumeration should succeed across pages");

        assert_eq!(set.len(), 2, "both pages' deposit mints must be collected");
        assert_eq!(
            set.get(&id1).map(|(_, k)| *k),
            Some(ConsumedMintKind::Deposit)
        );
        assert_eq!(
            set.get(&id2).map(|(_, k)| *k),
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
        let _m = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getSignaturesForAddress""#.into(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(format!(
                r#"{{"jsonrpc":"2.0","result":[{}],"id":0}}"#,
                // Legacy serial-id memo: prefix present, value is a bare number.
                sig_entry(
                    &Signature::new_unique().to_string(),
                    "private_channel:mint-idempotency:42"
                ),
            ))
            .create_async()
            .await;

        let rpc = fast_rpc(&server.url());
        let result = enumerate_consumed_mints(&rpc, &Pubkey::new_unique(), PAGE_LIMIT).await;
        let err = result.expect_err("legacy-scheme memo must abort enumeration");
        assert!(
            err.contains("cutover"),
            "error should name the memo cutover: {err}"
        );
    }
}
