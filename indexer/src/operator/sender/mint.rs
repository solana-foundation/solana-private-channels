use crate::operator::utils::instruction_util::{InitializeMintBuilder, TransactionBuilder};
use crate::operator::utils::transaction_util::{check_transaction_status, ConfirmationResult};
use crate::operator::{sign_and_send_transaction, RpcClientWithRetry, SignerUtil};
use solana_keychain::SolanaSigner;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::program_option::COption;
use solana_sdk::pubkey::Pubkey;
use spl_token::solana_program::program_pack::Pack;
use spl_token::state::Mint;
use tracing::{error, info, warn};

use super::types::{InstructionWithSigners, SenderState};

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
