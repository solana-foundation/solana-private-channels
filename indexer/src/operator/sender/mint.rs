use crate::operator::utils::instruction_util::{
    mint_idempotency_memo, InitializeMintBuilder, MintToBuilderWithTxnId, TransactionBuilder,
};
use crate::operator::utils::transaction_util::{check_transaction_status, ConfirmationResult};
use crate::operator::{
    sign_and_send_transaction, RpcClientWithRetry, SignerUtil,
    MINT_IDEMPOTENCY_SIGNATURE_LOOKBACK_LIMIT,
};
use serde_json::Value;
use solana_keychain::SolanaSigner;
use solana_rpc_client_api::client_error::ErrorKind;
use solana_rpc_client_api::request::RpcError;
use solana_sdk::commitment_config::CommitmentConfig;
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
/// `transaction.rs` matches on this to decide whether to recursively retry,
/// quarantine the deposit to ManualReview, or route to the existing
/// PermanentFailure path. The `String` payloads are operator-visible
/// `error_message`s; ManualReview reasons are constructed in full here
/// (with the literal `"Mint instruction failed after JIT: "` prefix) so
/// `drill_1` can grep the runbook-dispatch substrings in a single source
/// file.
pub enum JitOutcome {
    /// Mint is correctly initialized with the operator's admin as
    /// `mint_authority`. Caller should retry the supplied instruction.
    Retry(InstructionWithSigners),

    /// Mint exists on-chain but in a state the operator cannot fix by
    /// re-issuing `mint_to` (wrong authority, corrupt data, or post-init
    /// inconsistency). Caller routes to `ManualReview`.
    ManualReview(String),

    /// Transient or builder failure (RPC, mint cache miss, build error).
    /// Caller routes to the existing permanent-failure path (`Failed`
    /// status).
    PermanentFailure(String),
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
/// caller to dispatch (Retry / ManualReview / PermanentFailure).
pub(super) async fn try_jit_mint_initialization(
    state: &mut SenderState,
    transaction_id: i64,
    instruction: InstructionWithSigners,
) -> JitOutcome {
    // 1. Get cached builder + extract mint.
    let Some(builder) = state.mint_builders.get(&transaction_id).cloned() else {
        return JitOutcome::PermanentFailure(format!(
            "no cached MintToBuilder for transaction_id {}",
            transaction_id
        ));
    };
    let Some(mint) = builder.get_mint() else {
        return JitOutcome::PermanentFailure(format!(
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

    // 3. Look up mint decimals from mint cache.
    let Ok(mint_metadata) = state.mint_cache.get_mint_metadata(&mint).await else {
        error!("Mint {} not found in mint cache", mint);
        return JitOutcome::PermanentFailure(format!("mint not in mint cache: {}", mint));
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
            return JitOutcome::PermanentFailure(format!(
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
        Ok((s, _)) => s,
        Err(e) => {
            error!("Failed to send InitializeMint transaction: {}", e);
            return JitOutcome::PermanentFailure(format!(
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
            return JitOutcome::PermanentFailure(format!(
                "Failed to check InitializeMint status: {}",
                e
            ));
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
///   treats it as the canonical "InitializeMint could not be confirmed"
///   permanent failure that drives the existing Failed-runbook dispatch.
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
                JitOutcome::PermanentFailure(
                    "InitializeMint transaction could not be confirmed".to_string(),
                )
            }
            None => {
                error!(
                    "JIT post-init: InitializeMint confirmed but mint {} reads as uninitialized",
                    mint
                );
                JitOutcome::PermanentFailure(format!(
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
/// it's there" reading — caller maps this to PermanentFailure on the   
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

/// Check recent ATA signatures for an already-confirmed mint carrying this transaction's
/// deterministic idempotency memo.
pub async fn find_existing_mint_signature(
    rpc_client: &RpcClientWithRetry,
    builder_with_txn_id: &MintToBuilderWithTxnId,
) -> Result<Option<Signature>, String> {
    let expected_memo = mint_idempotency_memo(builder_with_txn_id.txn_id);
    find_existing_mint_signature_with_memo(rpc_client, builder_with_txn_id, &expected_memo).await
}

/// Check recent ATA signatures for an already-confirmed mint carrying the given memo.
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
            if is_method_not_found_error(e.as_ref()) {
                warn!(
                    "Skipping mint idempotency lookup for transaction_id {}: \
                     RPC endpoint does not support getSignaturesForAddress",
                    transaction_id
                );
                return Ok(None);
            }
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

fn is_method_not_found_error(error: &solana_rpc_client_api::client_error::Error) -> bool {
    matches!(
        error.kind(),
        ErrorKind::RpcError(RpcError::RpcResponseError { code: -32601, .. })
    )
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
pub(super) fn cleanup_mint_builder(state: &mut SenderState, transaction_id: Option<i64>) {
    if let Some(txn_id) = transaction_id {
        state.mint_builders.remove(&txn_id);
    }
}

#[cfg(test)]
mod tests {
    use super::{
        accounts_and_amount_match, decode_and_check_authority, expected_mint_instruction,
        instruction_has_expected_mint, instruction_has_memo, is_method_not_found_error,
        memo_matches, parse_token_instruction_mint_amount,
        partially_decoded_instruction_has_expected_mint, raw_instruction_has_expected_mint,
        strip_memo_length_prefix, transaction_matches_expected_mint, AuthorityCheck,
        ExpectedMintInstruction,
    };
    use crate::operator::utils::instruction_util::{MintToBuilder, MintToBuilderWithTxnId};
    use solana_rpc_client_api::{
        client_error::{self, ErrorKind},
        request::{RpcError, RpcResponseErrorData},
    };
    use solana_sdk::pubkey::Pubkey;
    use solana_transaction_status::parse_instruction::ParsedInstruction;
    use solana_transaction_status::{
        option_serializer::OptionSerializer, parse_accounts::ParsedAccount,
        EncodedConfirmedTransactionWithStatusMeta, EncodedTransaction,
        EncodedTransactionWithStatusMeta, UiCompiledInstruction, UiInstruction, UiMessage,
        UiParsedInstruction, UiParsedMessage, UiPartiallyDecodedInstruction, UiRawMessage,
        UiTransaction, UiTransactionStatusMeta,
    };
    use spl_token::solana_program::program_option::COption;
    use spl_token::solana_program::program_pack::Pack;
    use spl_token::state::Mint;

    fn make_expected() -> (Pubkey, Pubkey, Pubkey, ExpectedMintInstruction) {
        let mint = Pubkey::new_unique();
        let recipient_ata = Pubkey::new_unique();
        let mint_authority = Pubkey::new_unique();
        let expected = ExpectedMintInstruction {
            mint,
            recipient_ata,
            mint_authority,
            token_program: spl_token::id(),
            amount: 1000,
        };
        (mint, recipient_ata, mint_authority, expected)
    }

    fn build_test_transaction_parsed(
        signers: &[Pubkey],
        instructions: Vec<UiInstruction>,
        meta_err: Option<solana_sdk::transaction::TransactionError>,
    ) -> EncodedConfirmedTransactionWithStatusMeta {
        let account_keys: Vec<ParsedAccount> = signers
            .iter()
            .map(|pk| ParsedAccount {
                pubkey: pk.to_string(),
                writable: true,
                signer: true,
                source: None,
            })
            .collect();

        EncodedConfirmedTransactionWithStatusMeta {
            slot: 0,
            transaction: EncodedTransactionWithStatusMeta {
                transaction: EncodedTransaction::Json(UiTransaction {
                    signatures: vec!["sig".to_string()],
                    message: UiMessage::Parsed(UiParsedMessage {
                        account_keys,
                        recent_blockhash: "11111111111111111111111111111111".to_string(),
                        instructions,
                        address_table_lookups: None,
                    }),
                }),
                meta: Some(UiTransactionStatusMeta {
                    err: meta_err,
                    status: Ok(()),
                    fee: 5000,
                    pre_balances: vec![],
                    post_balances: vec![],
                    inner_instructions: OptionSerializer::None,
                    log_messages: OptionSerializer::None,
                    pre_token_balances: OptionSerializer::None,
                    post_token_balances: OptionSerializer::None,
                    rewards: OptionSerializer::None,
                    loaded_addresses: OptionSerializer::Skip,
                    return_data: OptionSerializer::Skip,
                    compute_units_consumed: OptionSerializer::Skip,
                    cost_units: OptionSerializer::Skip,
                }),
                version: None,
            },
            block_time: None,
        }
    }

    #[test]
    fn strip_memo_length_prefix_handles_formatted_values() {
        assert_eq!(
            strip_memo_length_prefix("[12] private_channel:mint-idempotency:42"),
            "private_channel:mint-idempotency:42"
        );
        assert_eq!(
            strip_memo_length_prefix("private_channel:mint-idempotency:42"),
            "private_channel:mint-idempotency:42"
        );
    }

    #[test]
    fn memo_matches_handles_plain_and_formatted_values() {
        let expected = "private_channel:mint-idempotency:99";

        assert!(memo_matches(expected, expected));
        assert!(memo_matches(
            "[27] private_channel:mint-idempotency:99",
            expected
        ));
        assert!(memo_matches(
            "[5] hello; [27] private_channel:mint-idempotency:99",
            expected
        ));
        assert!(!memo_matches("[5] hello", expected));
    }

    #[test]
    fn instruction_has_expected_mint_matches_mint_to_instruction() {
        let mint = Pubkey::new_unique();
        let recipient_ata = Pubkey::new_unique();
        let mint_authority = Pubkey::new_unique();
        let amount = 123_u64;
        let expected = ExpectedMintInstruction {
            mint,
            recipient_ata,
            mint_authority,
            token_program: spl_token::id(),
            amount,
        };
        let instruction = UiInstruction::Parsed(UiParsedInstruction::Parsed(ParsedInstruction {
            program: "spl-token".to_string(),
            program_id: spl_token::id().to_string(),
            parsed: serde_json::json!({
                "type": "mintTo",
                "info": {
                    "mint": mint.to_string(),
                    "account": recipient_ata.to_string(),
                    "mintAuthority": mint_authority.to_string(),
                    "amount": amount.to_string(),
                }
            }),
            stack_height: None,
        }));

        assert!(instruction_has_expected_mint(&instruction, &expected));
    }

    #[test]
    fn instruction_has_expected_mint_rejects_amount_mismatch() {
        let mint = Pubkey::new_unique();
        let recipient_ata = Pubkey::new_unique();
        let mint_authority = Pubkey::new_unique();
        let expected = ExpectedMintInstruction {
            mint,
            recipient_ata,
            mint_authority,
            token_program: spl_token::id(),
            amount: 500_u64,
        };
        let instruction = UiInstruction::Parsed(UiParsedInstruction::Parsed(ParsedInstruction {
            program: "spl-token".to_string(),
            program_id: spl_token::id().to_string(),
            parsed: serde_json::json!({
                "type": "mintTo",
                "info": {
                    "mint": mint.to_string(),
                    "account": recipient_ata.to_string(),
                    "mintAuthority": mint_authority.to_string(),
                    "amount": "123",
                }
            }),
            stack_height: None,
        }));

        assert!(!instruction_has_expected_mint(&instruction, &expected));
    }

    #[test]
    fn instruction_has_expected_mint_matches_mint_to_checked_instruction() {
        let mint = Pubkey::new_unique();
        let recipient_ata = Pubkey::new_unique();
        let mint_authority = Pubkey::new_unique();
        let amount = 888_u64;
        let expected = ExpectedMintInstruction {
            mint,
            recipient_ata,
            mint_authority,
            token_program: spl_token::id(),
            amount,
        };
        let instruction = UiInstruction::Parsed(UiParsedInstruction::Parsed(ParsedInstruction {
            program: "spl-token".to_string(),
            program_id: spl_token::id().to_string(),
            parsed: serde_json::json!({
                "type": "mintToChecked",
                "info": {
                    "mint": mint.to_string(),
                    "account": recipient_ata.to_string(),
                    "mintAuthority": mint_authority.to_string(),
                    "tokenAmount": {
                        "amount": amount.to_string(),
                    }
                }
            }),
            stack_height: None,
        }));

        assert!(instruction_has_expected_mint(&instruction, &expected));
    }

    #[test]
    fn expected_mint_instruction_complete_builder() {
        let mint = Pubkey::new_unique();
        let recipient_ata = Pubkey::new_unique();
        let mint_authority = Pubkey::new_unique();
        let mut builder = MintToBuilder::new();
        builder
            .mint(mint)
            .recipient_ata(recipient_ata)
            .mint_authority(mint_authority)
            .token_program(spl_token::id())
            .amount(500);

        let builder_with_id = MintToBuilderWithTxnId {
            builder,
            txn_id: 7,
            trace_id: "test".to_string(),
        };
        let result = expected_mint_instruction(7, &builder_with_id).unwrap();
        assert_eq!(result.mint, mint);
        assert_eq!(result.recipient_ata, recipient_ata);
        assert_eq!(result.mint_authority, mint_authority);
        assert_eq!(result.token_program, spl_token::id());
        assert_eq!(result.amount, 500);
    }

    #[test]
    fn expected_mint_instruction_incomplete_builder() {
        let mut builder = MintToBuilder::new();
        builder.mint(Pubkey::new_unique());
        // missing recipient_ata, mint_authority, token_program, amount

        let builder_with_id = MintToBuilderWithTxnId {
            builder,
            txn_id: 1,
            trace_id: "test".to_string(),
        };
        assert!(expected_mint_instruction(1, &builder_with_id).is_none());
    }

    #[test]
    fn accounts_and_amount_match_all_fields() {
        let (mint, recipient_ata, mint_authority, expected) = make_expected();
        let data = spl_token::instruction::TokenInstruction::MintTo { amount: 1000 }.pack();
        assert!(accounts_and_amount_match(
            &spl_token::id(),
            &mint,
            &recipient_ata,
            &mint_authority,
            &data,
            &expected,
        ));
    }

    #[test]
    fn accounts_and_amount_match_rejects_each_field() {
        let (mint, recipient_ata, mint_authority, expected) = make_expected();
        let data = spl_token::instruction::TokenInstruction::MintTo { amount: 1000 }.pack();

        // wrong program
        assert!(!accounts_and_amount_match(
            &Pubkey::new_unique(),
            &mint,
            &recipient_ata,
            &mint_authority,
            &data,
            &expected,
        ));

        // wrong mint
        assert!(!accounts_and_amount_match(
            &spl_token::id(),
            &Pubkey::new_unique(),
            &recipient_ata,
            &mint_authority,
            &data,
            &expected,
        ));

        // wrong recipient_ata
        assert!(!accounts_and_amount_match(
            &spl_token::id(),
            &mint,
            &Pubkey::new_unique(),
            &mint_authority,
            &data,
            &expected,
        ));

        // wrong mint_authority
        assert!(!accounts_and_amount_match(
            &spl_token::id(),
            &mint,
            &recipient_ata,
            &Pubkey::new_unique(),
            &data,
            &expected,
        ));

        // wrong amount
        let wrong_data = spl_token::instruction::TokenInstruction::MintTo { amount: 9999 }.pack();
        assert!(!accounts_and_amount_match(
            &spl_token::id(),
            &mint,
            &recipient_ata,
            &mint_authority,
            &wrong_data,
            &expected,
        ));
    }

    #[test]
    fn parse_token_instruction_mint_amount_spl_token() {
        let data = spl_token::instruction::TokenInstruction::MintTo { amount: 42 }.pack();
        assert_eq!(
            parse_token_instruction_mint_amount(&spl_token::id(), &data),
            Some(42)
        );

        let data_checked = spl_token::instruction::TokenInstruction::MintToChecked {
            amount: 77,
            decimals: 6,
        }
        .pack();
        assert_eq!(
            parse_token_instruction_mint_amount(&spl_token::id(), &data_checked),
            Some(77)
        );
    }

    #[test]
    fn parse_token_instruction_mint_amount_spl_token_2022() {
        let data = spl_token_2022::instruction::TokenInstruction::MintTo { amount: 100 }.pack();
        assert_eq!(
            parse_token_instruction_mint_amount(&spl_token_2022::id(), &data),
            Some(100)
        );

        let data_checked = spl_token_2022::instruction::TokenInstruction::MintToChecked {
            amount: 200,
            decimals: 9,
        }
        .pack();
        assert_eq!(
            parse_token_instruction_mint_amount(&spl_token_2022::id(), &data_checked),
            Some(200)
        );
    }

    #[test]
    fn parse_token_instruction_mint_amount_rejects_transfer() {
        let data = spl_token::instruction::TokenInstruction::Transfer { amount: 50 }.pack();
        assert_eq!(
            parse_token_instruction_mint_amount(&spl_token::id(), &data),
            None
        );
    }

    #[test]
    fn partially_decoded_mint_happy_path() {
        let (mint, recipient_ata, mint_authority, expected) = make_expected();
        let data = spl_token::instruction::TokenInstruction::MintTo { amount: 1000 }.pack();
        let partially_decoded = UiPartiallyDecodedInstruction {
            program_id: spl_token::id().to_string(),
            accounts: vec![
                mint.to_string(),
                recipient_ata.to_string(),
                mint_authority.to_string(),
            ],
            data: bs58::encode(&data).into_string(),
            stack_height: None,
        };
        assert!(partially_decoded_instruction_has_expected_mint(
            &partially_decoded,
            &expected,
        ));
    }

    #[test]
    fn partially_decoded_mint_wrong_amount() {
        let (mint, recipient_ata, mint_authority, expected) = make_expected();
        let data = spl_token::instruction::TokenInstruction::MintTo { amount: 9999 }.pack();
        let partially_decoded = UiPartiallyDecodedInstruction {
            program_id: spl_token::id().to_string(),
            accounts: vec![
                mint.to_string(),
                recipient_ata.to_string(),
                mint_authority.to_string(),
            ],
            data: bs58::encode(&data).into_string(),
            stack_height: None,
        };
        assert!(!partially_decoded_instruction_has_expected_mint(
            &partially_decoded,
            &expected,
        ));
    }

    #[test]
    fn raw_instruction_mint_happy_path() {
        let (mint, recipient_ata, mint_authority, expected) = make_expected();
        let data = spl_token::instruction::TokenInstruction::MintTo { amount: 1000 }.pack();
        let raw_message = UiRawMessage {
            header: solana_sdk::message::MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            account_keys: vec![
                mint_authority.to_string(),
                spl_token::id().to_string(),
                mint.to_string(),
                recipient_ata.to_string(),
            ],
            recent_blockhash: "11111111111111111111111111111111".to_string(),
            instructions: vec![],
            address_table_lookups: None,
        };
        let compiled = UiCompiledInstruction {
            program_id_index: 1,
            accounts: vec![2, 3, 0],
            data: bs58::encode(&data).into_string(),
            stack_height: None,
        };
        assert!(raw_instruction_has_expected_mint(
            &raw_message,
            &compiled,
            &expected,
        ));
    }

    #[test]
    fn raw_instruction_mint_wrong_program() {
        let (mint, recipient_ata, mint_authority, expected) = make_expected();
        let data = spl_token::instruction::TokenInstruction::MintTo { amount: 1000 }.pack();
        let wrong_program = Pubkey::new_unique();
        let raw_message = UiRawMessage {
            header: solana_sdk::message::MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            account_keys: vec![
                mint_authority.to_string(),
                wrong_program.to_string(),
                mint.to_string(),
                recipient_ata.to_string(),
            ],
            recent_blockhash: "11111111111111111111111111111111".to_string(),
            instructions: vec![],
            address_table_lookups: None,
        };
        let compiled = UiCompiledInstruction {
            program_id_index: 1,
            accounts: vec![2, 3, 0],
            data: bs58::encode(&data).into_string(),
            stack_height: None,
        };
        assert!(!raw_instruction_has_expected_mint(
            &raw_message,
            &compiled,
            &expected,
        ));
    }

    #[test]
    fn transaction_matches_expected_mint_parsed_happy_path() {
        let (mint, recipient_ata, mint_authority, expected) = make_expected();
        let memo_text = "private_channel:mint-idempotency:42";

        let memo_ix = UiInstruction::Parsed(UiParsedInstruction::Parsed(ParsedInstruction {
            program: "spl-memo".to_string(),
            program_id: spl_memo::id().to_string(),
            parsed: serde_json::Value::String(memo_text.to_string()),
            stack_height: None,
        }));
        let mint_ix = UiInstruction::Parsed(UiParsedInstruction::Parsed(ParsedInstruction {
            program: "spl-token".to_string(),
            program_id: spl_token::id().to_string(),
            parsed: serde_json::json!({
                "type": "mintTo",
                "info": {
                    "mint": mint.to_string(),
                    "account": recipient_ata.to_string(),
                    "mintAuthority": mint_authority.to_string(),
                    "amount": "1000",
                }
            }),
            stack_height: None,
        }));

        let tx = build_test_transaction_parsed(&[mint_authority], vec![memo_ix, mint_ix], None);

        assert!(transaction_matches_expected_mint(&tx, memo_text, &expected));
    }

    #[test]
    fn transaction_matches_expected_mint_rejects_failed_tx() {
        let (mint, recipient_ata, mint_authority, expected) = make_expected();
        let memo_text = "private_channel:mint-idempotency:42";

        let memo_ix = UiInstruction::Parsed(UiParsedInstruction::Parsed(ParsedInstruction {
            program: "spl-memo".to_string(),
            program_id: spl_memo::id().to_string(),
            parsed: serde_json::Value::String(memo_text.to_string()),
            stack_height: None,
        }));
        let mint_ix = UiInstruction::Parsed(UiParsedInstruction::Parsed(ParsedInstruction {
            program: "spl-token".to_string(),
            program_id: spl_token::id().to_string(),
            parsed: serde_json::json!({
                "type": "mintTo",
                "info": {
                    "mint": mint.to_string(),
                    "account": recipient_ata.to_string(),
                    "mintAuthority": mint_authority.to_string(),
                    "amount": "1000",
                }
            }),
            stack_height: None,
        }));

        let tx = build_test_transaction_parsed(
            &[mint_authority],
            vec![memo_ix, mint_ix],
            Some(solana_sdk::transaction::TransactionError::AccountNotFound),
        );

        assert!(!transaction_matches_expected_mint(
            &tx, memo_text, &expected
        ));
    }

    #[test]
    fn transaction_matches_expected_mint_rejects_wrong_memo() {
        let (mint, recipient_ata, mint_authority, expected) = make_expected();
        let expected_memo = "private_channel:mint-idempotency:42";

        let wrong_memo_ix = UiInstruction::Parsed(UiParsedInstruction::Parsed(ParsedInstruction {
            program: "spl-memo".to_string(),
            program_id: spl_memo::id().to_string(),
            parsed: serde_json::Value::String("private_channel:mint-idempotency:999".to_string()),
            stack_height: None,
        }));
        let mint_ix = UiInstruction::Parsed(UiParsedInstruction::Parsed(ParsedInstruction {
            program: "spl-token".to_string(),
            program_id: spl_token::id().to_string(),
            parsed: serde_json::json!({
                "type": "mintTo",
                "info": {
                    "mint": mint.to_string(),
                    "account": recipient_ata.to_string(),
                    "mintAuthority": mint_authority.to_string(),
                    "amount": "1000",
                }
            }),
            stack_height: None,
        }));

        let tx =
            build_test_transaction_parsed(&[mint_authority], vec![wrong_memo_ix, mint_ix], None);

        assert!(!transaction_matches_expected_mint(
            &tx,
            expected_memo,
            &expected,
        ));
    }

    // ====================================================================
    // instruction_has_memo tests
    // ====================================================================

    /// Compiled instructions carry no program-id string, so the memo check must
    /// return false regardless of the memo argument.
    #[test]
    fn instruction_has_memo_compiled_returns_false() {
        let ix = UiInstruction::Compiled(UiCompiledInstruction {
            program_id_index: 0,
            accounts: vec![],
            data: "".to_string(),
            stack_height: None,
        });
        assert!(!instruction_has_memo(&ix, "any-memo"));
    }

    /// A fully-parsed spl-memo instruction with the canonical program id and
    /// matching memo text must be recognized as containing the expected memo.
    #[test]
    fn instruction_has_memo_parsed_correct_memo() {
        let memo_text = "private_channel:mint-idempotency:7";
        let ix = UiInstruction::Parsed(UiParsedInstruction::Parsed(ParsedInstruction {
            program: "spl-memo".to_string(),
            program_id: spl_memo::id().to_string(),
            parsed: serde_json::Value::String(memo_text.to_string()),
            stack_height: None,
        }));
        assert!(instruction_has_memo(&ix, memo_text));
    }

    /// Matching memo text is not enough; the program_id must also equal spl_memo::id(),
    /// so an instruction from a different program is rejected.
    #[test]
    fn instruction_has_memo_parsed_wrong_program() {
        let memo_text = "private_channel:mint-idempotency:7";
        let wrong_program = Pubkey::new_unique();
        let ix = UiInstruction::Parsed(UiParsedInstruction::Parsed(ParsedInstruction {
            program: "not-memo".to_string(),
            program_id: wrong_program.to_string(),
            parsed: serde_json::Value::String(memo_text.to_string()),
            stack_height: None,
        }));
        assert!(!instruction_has_memo(&ix, memo_text));
    }

    /// Only `serde_json::Value::String` is accepted as the parsed field; a JSON object
    /// (even from the correct program) must cause the check to return false.
    #[test]
    fn instruction_has_memo_parsed_non_string_parsed_value() {
        let ix = UiInstruction::Parsed(UiParsedInstruction::Parsed(ParsedInstruction {
            program: "spl-memo".to_string(),
            program_id: spl_memo::id().to_string(),
            parsed: serde_json::json!({ "not": "a string" }),
            stack_height: None,
        }));
        assert!(!instruction_has_memo(&ix, "any-memo"));
    }

    /// PartiallyDecoded instructions store memo bytes as bs58; verify the decode-and-compare
    /// path correctly recognises the expected memo text.
    #[test]
    fn instruction_has_memo_partially_decoded_correct_memo() {
        let memo_text = "private_channel:mint-idempotency:99";
        let encoded_memo = bs58::encode(memo_text.as_bytes()).into_string();
        let ix = UiInstruction::Parsed(UiParsedInstruction::PartiallyDecoded(
            UiPartiallyDecodedInstruction {
                program_id: spl_memo::id().to_string(),
                accounts: vec![],
                data: encoded_memo,
                stack_height: None,
            },
        ));
        assert!(instruction_has_memo(&ix, memo_text));
    }

    /// A correct memo payload attached to a non-memo program id must be rejected
    /// even in the PartiallyDecoded encoding.
    #[test]
    fn instruction_has_memo_partially_decoded_wrong_program() {
        let memo_text = "private_channel:mint-idempotency:99";
        let encoded_memo = bs58::encode(memo_text.as_bytes()).into_string();
        let wrong_program = Pubkey::new_unique();
        let ix = UiInstruction::Parsed(UiParsedInstruction::PartiallyDecoded(
            UiPartiallyDecodedInstruction {
                program_id: wrong_program.to_string(),
                accounts: vec![],
                data: encoded_memo,
                stack_height: None,
            },
        ));
        assert!(!instruction_has_memo(&ix, memo_text));
    }

    // ====================================================================
    // is_method_not_found_error tests
    // ====================================================================

    /// JSON-RPC error code -32601 is the standard "method not found" code; the helper
    /// must return true exactly for this value.
    #[test]
    fn is_method_not_found_error_returns_true_for_32601() {
        let error = client_error::Error::new_with_request(
            ErrorKind::RpcError(RpcError::RpcResponseError {
                code: -32601,
                message: "Method not found".to_string(),
                data: RpcResponseErrorData::Empty,
            }),
            solana_rpc_client_api::request::RpcRequest::GetBalance,
        );
        assert!(is_method_not_found_error(&error));
    }

    /// Any other RPC response error code (e.g. -32600 "invalid request") must not be
    /// confused with method-not-found.
    #[test]
    fn is_method_not_found_error_returns_false_for_other_rpc_code() {
        let error = client_error::Error::new_with_request(
            ErrorKind::RpcError(RpcError::RpcResponseError {
                code: -32600,
                message: "Invalid request".to_string(),
                data: RpcResponseErrorData::Empty,
            }),
            solana_rpc_client_api::request::RpcRequest::GetBalance,
        );
        assert!(!is_method_not_found_error(&error));
    }

    // ====================================================================
    // transaction_matches_expected_mint with Raw message
    // ====================================================================

    fn build_test_transaction_raw(
        account_keys: Vec<String>,
        num_required_signatures: u8,
        instructions: Vec<UiCompiledInstruction>,
        meta_err: Option<solana_sdk::transaction::TransactionError>,
    ) -> EncodedConfirmedTransactionWithStatusMeta {
        EncodedConfirmedTransactionWithStatusMeta {
            slot: 0,
            transaction: EncodedTransactionWithStatusMeta {
                transaction: EncodedTransaction::Json(UiTransaction {
                    signatures: vec!["sig".to_string()],
                    message: UiMessage::Raw(UiRawMessage {
                        header: solana_sdk::message::MessageHeader {
                            num_required_signatures,
                            num_readonly_signed_accounts: 0,
                            num_readonly_unsigned_accounts: 0,
                        },
                        account_keys,
                        recent_blockhash: "11111111111111111111111111111111".to_string(),
                        instructions,
                        address_table_lookups: None,
                    }),
                }),
                meta: Some(UiTransactionStatusMeta {
                    err: meta_err,
                    status: Ok(()),
                    fee: 5000,
                    pre_balances: vec![],
                    post_balances: vec![],
                    inner_instructions: OptionSerializer::None,
                    log_messages: OptionSerializer::None,
                    pre_token_balances: OptionSerializer::None,
                    post_token_balances: OptionSerializer::None,
                    rewards: OptionSerializer::None,
                    loaded_addresses: OptionSerializer::Skip,
                    return_data: OptionSerializer::Skip,
                    compute_units_consumed: OptionSerializer::Skip,
                    cost_units: OptionSerializer::Skip,
                }),
                version: None,
            },
            block_time: None,
        }
    }

    /// End-to-end check: a UiRawMessage transaction with the correct memo, spl-token MintTo
    /// instruction, and matching signers/accounts must pass the full validation.
    #[test]
    fn transaction_matches_expected_mint_raw_message_happy_path() {
        let (mint, recipient_ata, mint_authority, expected) = make_expected();
        let memo_text = "private_channel:mint-idempotency:42";

        let mint_data = spl_token::instruction::TokenInstruction::MintTo { amount: 1000 }.pack();

        // account_keys layout:
        // 0 = mint_authority (signer)
        // 1 = spl_memo program
        // 2 = spl_token program
        // 3 = mint
        // 4 = recipient_ata
        let account_keys = vec![
            mint_authority.to_string(),
            spl_memo::id().to_string(),
            spl_token::id().to_string(),
            mint.to_string(),
            recipient_ata.to_string(),
        ];

        let memo_ix = UiCompiledInstruction {
            program_id_index: 1, // spl_memo
            accounts: vec![],
            data: bs58::encode(memo_text.as_bytes()).into_string(),
            stack_height: None,
        };
        let mint_ix = UiCompiledInstruction {
            program_id_index: 2,     // spl_token
            accounts: vec![3, 4, 0], // mint, recipient_ata, mint_authority
            data: bs58::encode(&mint_data).into_string(),
            stack_height: None,
        };

        let tx = build_test_transaction_raw(account_keys, 1, vec![memo_ix, mint_ix], None);
        assert!(transaction_matches_expected_mint(&tx, memo_text, &expected));
    }

    /// If the real mint_authority is not in a signing position (index ≥ num_required_signatures),
    /// the transaction must be rejected even when all other fields match.
    #[test]
    fn transaction_matches_expected_mint_raw_message_rejects_wrong_signer() {
        let (mint, recipient_ata, mint_authority, expected) = make_expected();
        let memo_text = "private_channel:mint-idempotency:42";

        let mint_data = spl_token::instruction::TokenInstruction::MintTo { amount: 1000 }.pack();
        let wrong_authority = Pubkey::new_unique();

        // mint_authority is not in signed position (not index < num_required_signatures)
        let account_keys = vec![
            wrong_authority.to_string(), // index 0 is the signer, but it's a different key
            mint_authority.to_string(),  // index 1 is the real authority, but not a signer
            spl_memo::id().to_string(),
            spl_token::id().to_string(),
            mint.to_string(),
            recipient_ata.to_string(),
        ];

        let memo_ix = UiCompiledInstruction {
            program_id_index: 2,
            accounts: vec![],
            data: bs58::encode(memo_text.as_bytes()).into_string(),
            stack_height: None,
        };
        let mint_ix = UiCompiledInstruction {
            program_id_index: 3,
            accounts: vec![4, 5, 1], // uses index 1 (mint_authority) as signer account
            data: bs58::encode(&mint_data).into_string(),
            stack_height: None,
        };

        // num_required_signatures = 1, so only index 0 is a signer
        // mint_authority is at index 1, which is NOT a signer
        let tx = build_test_transaction_raw(account_keys, 1, vec![memo_ix, mint_ix], None);
        assert!(!transaction_matches_expected_mint(
            &tx, memo_text, &expected
        ));
    }

    // ====================================================================
    // strip_memo_length_prefix edge cases
    // ====================================================================

    /// Strings with no opening bracket have no length prefix to strip; the original
    /// value must be returned unchanged.
    #[test]
    fn strip_memo_length_prefix_no_bracket() {
        assert_eq!(strip_memo_length_prefix("plain memo"), "plain memo");
    }

    /// A bracket prefix like `[abc]` whose content is not all digits is not a valid
    /// length prefix, so the original string must be returned unchanged.
    #[test]
    fn strip_memo_length_prefix_non_digit_length() {
        assert_eq!(
            strip_memo_length_prefix("[abc] some memo"),
            "[abc] some memo"
        );
    }

    /// `split_once("] ")` requires a space after the closing bracket; without it the
    /// prefix is not stripped and the original string is returned unchanged.
    #[test]
    fn strip_memo_length_prefix_no_space_after_bracket() {
        assert_eq!(strip_memo_length_prefix("[123]no-space"), "[123]no-space");
    }

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
