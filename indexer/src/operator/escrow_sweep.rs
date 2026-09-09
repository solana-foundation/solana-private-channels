//! Shared on-chain escrow balance sweep.
//!
//! Both the operator's continuous reconciliation and the indexer's startup
//! reconciliation need the authoritative custody view: the token balance the
//! escrow instance actually holds, summed per mint across every token account
//! it owns. Deriving the set of mints from this sweep (rather than from the DB
//! `mints` table) is what closes the startup blind spot where a fresh or
//! partially restored DB with real escrow balances would otherwise pass the
//! check without ever looking on-chain.

use crate::operator::utils::instruction_util::RetryPolicy;
use crate::operator::utils::rpc_util::RpcClientWithRetry;
use solana_account_decoder_client_types::UiAccountData;
use solana_client::rpc_request::TokenAccountsFilter;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use spl_token::solana_program::program_pack::Pack;
use spl_token::state::Account as TokenAccount;
use spl_token::state::Mint;
use std::collections::HashMap;
use std::str::FromStr;
use tracing::warn;

/// Failure to read the escrow's on-chain token holdings. Carries a human reason
/// so each caller can wrap it in its own error type without losing context.
#[derive(Debug, Clone)]
pub struct EscrowSweepError {
    pub reason: String,
}

impl std::fmt::Display for EscrowSweepError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.reason)
    }
}

impl std::error::Error for EscrowSweepError {}

/// Why a custody sweep could not produce a snapshot.
///
/// Kept apart from a plain read failure because the two deserve different answers: a slot
/// the calls never settled on is a property of that attempt, not of the chain, so sweeping
/// again can fix it, while a failing read usually cannot.
#[derive(Debug, Clone)]
pub enum SweepFailure {
    /// An RPC call or an account decode failed.
    Read(EscrowSweepError),
    /// The two token-program calls never answered at the same slot, so no single slot
    /// describes the merged balances.
    SlotUnsettled { attempts: u32, low: u64, high: u64 },
}

impl std::fmt::Display for SweepFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SweepFailure::Read(e) => write!(f, "{e}"),
            SweepFailure::SlotUnsettled {
                attempts,
                low,
                high,
            } => write!(
                f,
                "token program sweeps never settled on one slot after {attempts} attempts (last spread {low}..{high})"
            ),
        }
    }
}

/// On-chain custody together with the slot the reading reflects.
///
/// The slot is what lets a caller compare this against a ledger bounded at the same
/// point instead of against one that has drifted, so the two sides describe one instant.
#[derive(Debug, Clone)]
pub struct CustodySnapshot {
    /// Per-mint custody; a mint absent from the map holds zero on-chain.
    pub balances: HashMap<Pubkey, u64>,
    /// Slot the whole snapshot is valid as of.
    pub slot: u64,
}

/// Attempts allowed to get both token-program sweeps to answer at the same slot. Set
/// generously because running out is fatal, not a fallback: back-to-back finalized reads
/// agree almost every time, so needing five means something is genuinely wrong.
const SWEEP_SLOT_AGREEMENT_ATTEMPTS: u32 = 5;

/// Sum every token account owned by the escrow instance, grouped by mint, across
/// the SPL Token and Token-2022 programs.
///
/// The two programs need one call each, so their readings can land on different slots and
/// the merged balances would then hold activity the lower slot never saw. No single slot
/// describes such a snapshot honestly, and labelling it with the lower one understates the
/// custody it carries, so the sweep is taken again until both calls answer at the same
/// slot. Back-to-back finalized reads normally agree on the first try.
///
/// If they never agree, the sweep fails rather than hand back a slot the balances do not
/// match: a caller that bounds its ledger read by that slot would compare two different
/// moments and call an ordinary channel broken. The failure is reported as its own kind so
/// a caller can sweep again, since the skew belongs to the attempt rather than the chain.
pub async fn fetch_escrow_balances_by_mint(
    rpc_client: &RpcClientWithRetry,
    escrow_instance_id: Pubkey,
) -> Result<CustodySnapshot, SweepFailure> {
    let mut attempt = 1;
    loop {
        let (balances, low, high) = sweep_once(rpc_client, escrow_instance_id)
            .await
            .map_err(SweepFailure::Read)?;
        if low != high && attempt < SWEEP_SLOT_AGREEMENT_ATTEMPTS {
            attempt += 1;
            continue;
        }
        if low != high {
            warn!(
                low_slot = low,
                high_slot = high,
                attempts = SWEEP_SLOT_AGREEMENT_ATTEMPTS,
                "Escrow sweep: token programs kept answering at different slots"
            );
            return Err(SweepFailure::SlotUnsettled {
                attempts: SWEEP_SLOT_AGREEMENT_ATTEMPTS,
                low,
                high,
            });
        }
        return Ok(CustodySnapshot {
            balances,
            slot: low,
        });
    }
}

/// One pass over both token programs. Returns the merged balances plus the lowest and
/// highest slot the two responses reported, which agree when the pass saw one instant.
async fn sweep_once(
    rpc_client: &RpcClientWithRetry,
    escrow_instance_id: Pubkey,
) -> Result<(HashMap<Pubkey, u64>, u64, u64), EscrowSweepError> {
    let mut balances = HashMap::new();
    let token_programs = [spl_token::id(), spl_token_2022::id()];
    let mut lowest_slot = u64::MAX;
    let mut highest_slot = 0u64;

    for token_program_id in token_programs {
        let response = rpc_client
            .with_retry(
                "get_token_accounts_by_owner",
                RetryPolicy::Idempotent,
                || async {
                    rpc_client
                        .rpc_client
                        .get_token_accounts_by_owner_with_commitment(
                            &escrow_instance_id,
                            TokenAccountsFilter::ProgramId(token_program_id),
                            CommitmentConfig::finalized(),
                        )
                        .await
                },
            )
            .await
            .map_err(|e| EscrowSweepError {
                reason: format!(
                    "Failed to fetch token accounts for program {token_program_id}: {e}"
                ),
            })?;

        lowest_slot = lowest_slot.min(response.context.slot);
        highest_slot = highest_slot.max(response.context.slot);
        let accounts = response.value;

        // The RPC may return accounts in binary (base64) or JSON-parsed form
        // depending on the requested encoding; handle both.
        for keyed_account in accounts {
            let (mint, amount) = if let Some(decoded) = keyed_account.account.data.decode() {
                let token_account =
                    TokenAccount::unpack(&decoded).map_err(|e| EscrowSweepError {
                        reason: format!(
                            "Failed to parse token account for program {token_program_id}: {e}"
                        ),
                    })?;
                (token_account.mint, token_account.amount)
            } else if let UiAccountData::Json(parsed) = &keyed_account.account.data {
                let info = parsed.parsed.get("info").ok_or_else(|| EscrowSweepError {
                    reason: "Missing 'info' in parsed token account".to_string(),
                })?;
                let mint_str =
                    info.get("mint")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| EscrowSweepError {
                            reason: "Missing 'mint' in parsed token account info".to_string(),
                        })?;
                let amount_str = info
                    .get("tokenAmount")
                    .and_then(|v| v.get("amount"))
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| EscrowSweepError {
                        reason: "Missing 'tokenAmount.amount' in parsed token account".to_string(),
                    })?;
                let mint = Pubkey::from_str(mint_str).map_err(|e| EscrowSweepError {
                    reason: format!("Invalid mint pubkey '{mint_str}': {e}"),
                })?;
                let amount = amount_str.parse::<u64>().map_err(|e| EscrowSweepError {
                    reason: format!("Invalid token amount '{amount_str}': {e}"),
                })?;
                (mint, amount)
            } else {
                warn!(
                    token_program = %token_program_id,
                    "Skipping escrow token account with unrecognised data encoding"
                );
                continue;
            };

            // One mint can span several token accounts; sum them. Saturating so a corrupt
            // over-u64 sum reports u64::MAX (and trips the mismatch) instead of wrapping.
            let acc = balances.entry(mint).or_insert(0u64);
            *acc = acc.saturating_add(amount);
        }
    }

    Ok((balances, lowest_slot, highest_slot))
}

/// Read the channel-token supply for `mint` on the PrivateChannel chain. An
/// absent mint account (nothing minted yet) reads as supply 0; any other RPC or
/// decode failure is surfaced so a bad read never silently looks like 0 supply.
pub async fn fetch_channel_supply(
    channel_rpc: &RpcClientWithRetry,
    mint: &Pubkey,
) -> Result<u64, EscrowSweepError> {
    // get_account_with_commitment cleanly separates a truly-absent account
    // (Ok(value = None)) from a transport/node error (Err). The plain get_account
    // convenience formats BOTH as an "AccountNotFound" error, which would let an
    // RPC outage masquerade as zero supply and blind the supply invariant.
    let commitment = CommitmentConfig::finalized();
    let response = channel_rpc
        .with_retry(
            "get_channel_mint_account",
            RetryPolicy::Idempotent,
            || async {
                channel_rpc
                    .rpc_client
                    .get_account_with_commitment(mint, commitment)
                    .await
            },
        )
        .await
        .map_err(|e| EscrowSweepError {
            reason: format!("Failed to fetch channel mint account {mint}: {e}"),
        })?;

    // Absent account = nothing minted yet.
    let account = match response.value {
        Some(account) => account,
        None => return Ok(0),
    };

    // The channel program mints classic SPL tokens (not Token-2022).
    let mint_state = Mint::unpack(&account.data).map_err(|e| EscrowSweepError {
        reason: format!("Failed to parse channel mint account {mint}: {e}"),
    })?;
    Ok(mint_state.supply)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::RetryConfig;
    use base64::Engine as _;
    use solana_sdk::commitment_config::CommitmentConfig;
    use spl_token::solana_program::program_option::COption;
    use spl_token::state::AccountState;

    fn client(url: &str) -> RpcClientWithRetry {
        RpcClientWithRetry::with_retry_config(
            url.to_string(),
            RetryConfig::default(),
            CommitmentConfig::finalized(),
        )
    }

    /// One `RpcKeyedAccount` whose `data` is the SPL Token-2022/Token binary layout,
    /// base64-encoded, exercising the `data.decode()` + `TokenAccount::unpack` path.
    fn base64_account(mint: Pubkey, amount: u64) -> String {
        let account = TokenAccount {
            mint,
            owner: Pubkey::new_unique(),
            amount,
            delegate: COption::None,
            state: AccountState::Initialized,
            is_native: COption::None,
            delegated_amount: 0,
            close_authority: COption::None,
        };
        let mut buf = vec![0u8; TokenAccount::LEN];
        account.pack_into_slice(&mut buf);
        let b64 = base64::engine::general_purpose::STANDARD.encode(&buf);
        format!(
            r#"{{"pubkey":"{ata}","account":{{"lamports":2039280,"owner":"{prog}","executable":false,"rentEpoch":0,"space":165,"data":["{b64}","base64"]}}}}"#,
            ata = Pubkey::new_unique(),
            prog = spl_token::id(),
        )
    }

    /// One `RpcKeyedAccount` whose `data` is jsonParsed, exercising the JSON path.
    fn json_parsed_account(mint: Pubkey, amount: u64) -> String {
        format!(
            r#"{{"pubkey":"{ata}","account":{{"lamports":2039280,"owner":"{prog}","executable":false,"rentEpoch":0,"space":165,"data":{{"program":"spl-token","space":165,"parsed":{{"type":"account","info":{{"mint":"{mint}","owner":"{owner}","tokenAmount":{{"amount":"{amount}","decimals":6,"uiAmount":null,"uiAmountString":"{amount}"}}}}}}}}}}}}"#,
            ata = Pubkey::new_unique(),
            prog = spl_token::id(),
            owner = Pubkey::new_unique(),
        )
    }

    /// One `RpcKeyedAccount` whose `data` carries the legacy `binary` encoding tag,
    /// which `decode()` cannot handle and which is not jsonParsed: the unrecognised
    /// branch the sweep skips with a warning instead of erroring.
    fn unrecognised_encoding_account() -> String {
        format!(
            r#"{{"pubkey":"{ata}","account":{{"lamports":1,"owner":"{prog}","executable":false,"rentEpoch":0,"space":4,"data":["AAAA","binary"]}}}}"#,
            ata = Pubkey::new_unique(),
            prog = spl_token::id(),
        )
    }

    fn result_body(values: &[String], slot: u64) -> String {
        format!(
            r#"{{"jsonrpc":"2.0","result":{{"context":{{"slot":{slot}}},"value":[{}]}},"id":1}}"#,
            values.join(",")
        )
    }

    fn empty_body(slot: u64) -> String {
        result_body(&[], slot)
    }

    /// The sweep calls `get_token_accounts_by_owner` once per token program. Route the
    /// SPL Token call (matched by its program id in the request body) to `spl_accounts`
    /// and the Token-2022 call to an empty list so the two are not double-counted.
    async fn mock_sweep(server: &mut mockito::Server, spl_accounts: &[String]) {
        mock_sweep_at_slots(server, spl_accounts, 1, 1).await;
    }

    /// Same routing, with the context slot of each call pinned separately.
    async fn mock_sweep_at_slots(
        server: &mut mockito::Server,
        spl_accounts: &[String],
        spl_slot: u64,
        token_2022_slot: u64,
    ) {
        mock_sweep_at_slots_times(server, spl_accounts, spl_slot, token_2022_slot, None).await;
    }

    /// `times` caps how many sweeps this pair is served for, so a test can script one pass
    /// followed by a different one.
    async fn mock_sweep_at_slots_times(
        server: &mut mockito::Server,
        spl_accounts: &[String],
        spl_slot: u64,
        token_2022_slot: u64,
        times: Option<usize>,
    ) {
        let spl = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(spl_token::id().to_string()))
            .with_status(200)
            .with_body(result_body(spl_accounts, spl_slot));
        let token_2022 = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(spl_token_2022::id().to_string()))
            .with_status(200)
            .with_body(empty_body(token_2022_slot));
        match times {
            Some(n) => {
                spl.expect(n).create_async().await;
                token_2022.expect(n).create_async().await;
            }
            None => {
                spl.create_async().await;
                token_2022.create_async().await;
            }
        };
    }

    #[tokio::test]
    async fn json_parsed_sums_multiple_accounts_per_mint() {
        let mut server = mockito::Server::new_async().await;
        let mint1 = Pubkey::new_unique();
        let mint2 = Pubkey::new_unique();
        // mint1 split across two accounts (100 + 200), mint2 in one (500).
        mock_sweep(
            &mut server,
            &[
                json_parsed_account(mint1, 100),
                json_parsed_account(mint1, 200),
                json_parsed_account(mint2, 500),
            ],
        )
        .await;

        let balances = fetch_escrow_balances_by_mint(&client(&server.url()), Pubkey::new_unique())
            .await
            .unwrap()
            .balances;

        assert_eq!(balances.len(), 2);
        assert_eq!(balances[&mint1], 300, "same mint across accounts must sum");
        assert_eq!(balances[&mint2], 500);
    }

    #[tokio::test]
    async fn decodes_base64_binary_accounts() {
        let mut server = mockito::Server::new_async().await;
        let mint = Pubkey::new_unique();
        mock_sweep(&mut server, &[base64_account(mint, 1_234)]).await;

        let balances = fetch_escrow_balances_by_mint(&client(&server.url()), Pubkey::new_unique())
            .await
            .unwrap()
            .balances;

        assert_eq!(balances[&mint], 1_234, "base64 layout must unpack and sum");
    }

    #[tokio::test]
    async fn skips_unrecognised_encoding_without_erroring() {
        let mut server = mockito::Server::new_async().await;
        let mint = Pubkey::new_unique();
        // A valid account plus one with an unrecognised encoding: the latter is skipped,
        // not fatal, so the valid balance still lands.
        mock_sweep(
            &mut server,
            &[
                json_parsed_account(mint, 50),
                unrecognised_encoding_account(),
            ],
        )
        .await;

        let balances = fetch_escrow_balances_by_mint(&client(&server.url()), Pubkey::new_unique())
            .await
            .unwrap()
            .balances;

        assert_eq!(balances.len(), 1);
        assert_eq!(balances[&mint], 50);
    }

    /// When the two calls never settle on one slot, no slot describes the merged balances.
    /// Handing back the lower one would let a caller bound its ledger at a point the
    /// balances overshoot, so the sweep fails instead of guessing.
    #[tokio::test]
    async fn sweep_fails_when_the_two_programs_never_agree_on_a_slot() {
        let mut server = mockito::Server::new_async().await;
        let mint = Pubkey::new_unique();
        mock_sweep_at_slots(&mut server, &[json_parsed_account(mint, 42)], 900, 880).await;

        let result =
            fetch_escrow_balances_by_mint(&client(&server.url()), Pubkey::new_unique()).await;

        let err = result.expect_err("a snapshot with no coherent slot must not be returned");
        assert!(
            matches!(err, SweepFailure::SlotUnsettled { .. }),
            "the caller has to be able to tell this apart from a read failure: {err:?}"
        );
    }

    /// The failure has to be its own kind, not a generic read error: startup retries a
    /// sweep that would not settle, because the node was moving under it, and gives up on
    /// one that could not be read at all.
    #[tokio::test]
    async fn a_slot_that_never_settles_is_reported_apart_from_a_read_failure() {
        let mut server = mockito::Server::new_async().await;
        let mint = Pubkey::new_unique();
        mock_sweep_at_slots(&mut server, &[json_parsed_account(mint, 42)], 900, 880).await;

        let err = fetch_escrow_balances_by_mint(&client(&server.url()), Pubkey::new_unique())
            .await
            .expect_err("a snapshot with no coherent slot must not be returned");

        match err {
            SweepFailure::SlotUnsettled { low, high, .. } => {
                assert_eq!(
                    (low, high),
                    (880, 900),
                    "the spread is reported as measured"
                );
            }
            other => panic!("a slot disagreement must not look like a read failure: {other:?}"),
        }
    }

    /// A deposit finalizing between the two calls leaves the merged balances holding
    /// activity the lower slot never saw, so labelling them with it understates custody
    /// and reconciliation reads the difference as a mismatch. Taking the sweep again is
    /// enough, because the skew lasts only as long as the gap between the two calls.
    #[tokio::test]
    async fn sweep_is_retaken_until_both_token_programs_answer_at_one_slot() {
        let mut server = mockito::Server::new_async().await;
        let mint = Pubkey::new_unique();
        // First pass straddles a slot boundary; the next one lands inside a single slot.
        mock_sweep_at_slots_times(
            &mut server,
            &[json_parsed_account(mint, 42)],
            900,
            880,
            Some(1),
        )
        .await;
        mock_sweep_at_slots(&mut server, &[json_parsed_account(mint, 42)], 905, 905).await;

        let snapshot = fetch_escrow_balances_by_mint(&client(&server.url()), Pubkey::new_unique())
            .await
            .unwrap();

        assert_eq!(
            snapshot.slot, 905,
            "a re-read that agrees must replace the straddled pass"
        );
        assert_eq!(snapshot.balances[&mint], 42);
    }

    #[tokio::test]
    async fn empty_owner_returns_empty_map() {
        let mut server = mockito::Server::new_async().await;
        mock_sweep(&mut server, &[]).await;

        let balances = fetch_escrow_balances_by_mint(&client(&server.url()), Pubkey::new_unique())
            .await
            .unwrap()
            .balances;

        assert!(balances.is_empty());
    }

    /// getAccountInfo response wrapping an 82-byte SPL Mint blob with `supply`.
    fn mint_account_body(supply: u64) -> String {
        let mint = Mint {
            mint_authority: COption::Some(Pubkey::new_unique()),
            supply,
            decimals: 6,
            is_initialized: true,
            freeze_authority: COption::None,
        };
        let mut buf = vec![0u8; Mint::LEN];
        mint.pack_into_slice(&mut buf);
        let b64 = base64::engine::general_purpose::STANDARD.encode(&buf);
        format!(
            r#"{{"jsonrpc":"2.0","id":1,"result":{{"context":{{"slot":1}},"value":{{"owner":"{prog}","lamports":1000000,"data":["{b64}","base64"],"executable":false,"rentEpoch":0}}}}}}"#,
            prog = spl_token::id(),
        )
    }

    #[tokio::test]
    async fn fetch_channel_supply_decodes_supply() {
        let mut server = mockito::Server::new_async().await;
        server
            .mock("POST", "/")
            .with_status(200)
            .with_body(mint_account_body(1_234_567))
            .create_async()
            .await;

        let supply = fetch_channel_supply(&client(&server.url()), &Pubkey::new_unique())
            .await
            .unwrap();
        assert_eq!(supply, 1_234_567);
    }

    #[tokio::test]
    async fn fetch_channel_supply_absent_mint_is_zero() {
        let mut server = mockito::Server::new_async().await;
        // A null account value: the channel mint does not exist yet -> 0 supply.
        server
            .mock("POST", "/")
            .with_status(200)
            .with_body(r#"{"jsonrpc":"2.0","id":1,"result":{"context":{"slot":1},"value":null}}"#)
            .create_async()
            .await;

        let supply = fetch_channel_supply(&client(&server.url()), &Pubkey::new_unique())
            .await
            .unwrap();
        assert_eq!(supply, 0, "absent mint account must read as zero supply");
    }

    #[tokio::test]
    async fn fetch_channel_supply_rpc_error_is_err() {
        // A transport/node error must surface as Err, never Ok(0): the plain
        // get_account convenience formats a 503 with the same AccountNotFound
        // prefix as a genuinely-absent mint, which would let an RPC outage
        // masquerade as zero supply and blind the invariant.
        let mut server = mockito::Server::new_async().await;
        server
            .mock("POST", "/")
            .with_status(503)
            .create_async()
            .await;

        let fast = RpcClientWithRetry::with_retry_config(
            server.url(),
            RetryConfig {
                max_attempts: 1,
                base_delay: std::time::Duration::from_millis(1),
                max_delay: std::time::Duration::from_millis(1),
            },
            CommitmentConfig::finalized(),
        );
        let result = fetch_channel_supply(&fast, &Pubkey::new_unique()).await;
        assert!(result.is_err(), "an RPC outage must be Err, not Ok(0)");
    }

    #[tokio::test]
    async fn errors_on_malformed_json_account() {
        let mut server = mockito::Server::new_async().await;
        // jsonParsed account missing the `tokenAmount` field: a corrupt response must
        // surface as an error, never a silently dropped balance.
        let malformed = format!(
            r#"{{"pubkey":"{ata}","account":{{"lamports":1,"owner":"{prog}","executable":false,"rentEpoch":0,"space":165,"data":{{"program":"spl-token","space":165,"parsed":{{"type":"account","info":{{"mint":"{mint}","owner":"{prog}"}}}}}}}}}}"#,
            ata = Pubkey::new_unique(),
            prog = spl_token::id(),
            mint = Pubkey::new_unique(),
        );
        mock_sweep(&mut server, &[malformed]).await;

        let result =
            fetch_escrow_balances_by_mint(&client(&server.url()), Pubkey::new_unique()).await;

        let err = result.expect_err("malformed account must error");
        let SweepFailure::Read(read) = &err else {
            panic!("a decode failure is a read failure, not a slot disagreement: {err:?}");
        };
        assert!(
            read.reason.contains("tokenAmount"),
            "unexpected error: {}",
            read.reason
        );
    }
}
