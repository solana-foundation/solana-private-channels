use crate::operator::utils::instruction_util::{
    mint_idempotency_memo, remint_idempotency_memo, InitializeMintBuilder, TransactionBuilder,
};
use crate::operator::utils::transaction_util::{check_transaction_status, ConfirmationResult};
use crate::operator::{
    sign_and_send_transaction, RpcClientWithRetry, SignerUtil, SourceEventId,
    MINT_IDEMPOTENCY_MEMO_PREFIX, REMINT_IDEMPOTENCY_MEMO_PREFIX,
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
use std::collections::hash_map::Entry;
use std::collections::HashMap;
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
        let mut markers = Vec::new();
        for piece in memo.split("; ") {
            let value = strip_memo_length_prefix(piece);
            if let Some(rest) = value.strip_prefix(MINT_IDEMPOTENCY_MEMO_PREFIX) {
                markers.push((ConsumedMintKind::Deposit, rest));
            } else if let Some(rest) = value.strip_prefix(REMINT_IDEMPOTENCY_MEMO_PREFIX) {
                markers.push((ConsumedMintKind::Remint, rest));
            }
        }
        if markers.is_empty() {
            continue;
        }

        let signature = Signature::from_str(&status.signature)
            .map_err(|e| format!("invalid signature {} from RPC: {e}", status.signature))?;
        let transaction = rpc
            .get_transaction(&signature)
            .await
            .map_err(|e| format!("consumed-set enumeration failed fetching {signature}: {e}"))?;
        // History lists every tx that mentions the authority, not only ones it signed.
        if !transaction_succeeded(&transaction) || !transaction_signed_by(&transaction, authority) {
            continue;
        }

        let &[(kind, encoded)] = markers.as_slice() else {
            return Err(format!(
                "channel mint {signature} is signed by the authority but carries {} idempotency \
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
                "channel mint {signature} is signed by the authority but has no Memo \
                 instruction equal to {expected_memo}; cannot authenticate it"
            ));
        }

        let mint_tos = transaction_mint_tos(&transaction);
        let [mint_to] = mint_tos[..] else {
            return Err(format!(
                "channel mint {signature} is signed by the authority but has {} MintTo \
                 instructions, expected one",
                mint_tos.len()
            ));
        };
        if mint_to.mint_authority != *authority {
            return Err(format!(
                "channel mint {signature} mints under {} instead of the authority {authority}",
                mint_to.mint_authority
            ));
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
            }
            Entry::Occupied(occupied) if occupied.get().signature == signature => {}
            Entry::Occupied(occupied) => {
                return Err(format!(
                    "source event {} has two successful channel mints signed by the \
                     authority, {} and {signature}; one event may be minted once, see \
                     docs/runbooks/resync_consumed_mint_mismatch.md",
                    occupied.key(),
                    occupied.get().signature
                ));
            }
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
        enumerate_consumed_mints, ConsumedMint, ConsumedMintKind, ConsumedSet, MintToFields,
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
        MintToFields {
            mint: Pubkey::new_unique(),
            recipient_ata: Pubkey::new_unique(),
            mint_authority: *mint_authority,
            token_program: spl_token::id(),
            amount: AMOUNT,
        }
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
}
