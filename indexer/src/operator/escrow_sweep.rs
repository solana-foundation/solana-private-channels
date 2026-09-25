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
use solana_account_decoder_client_types::{UiAccount, UiAccountData};
use solana_client::rpc_request::{RpcRequest, TokenAccountsFilter};
use solana_client::rpc_response::Response;
use solana_commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use spl_associated_token_account::get_associated_token_address_with_program_id;
use spl_token::solana_program::program_pack::Pack;
use spl_token::state::Account as TokenAccount;
use spl_token::state::Mint;
use spl_token_2022::extension::StateWithExtensions;
use spl_token_2022::state::Account as Token2022Account;
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

/// Escrow custody read from the instance ATAs, with the slot each mint's reading reflects.
#[derive(Debug, Clone)]
pub struct EscrowCustody {
    /// Per-mint custody; a mint absent from the map holds zero on-chain.
    pub balances: HashMap<Pubkey, u64>,
    /// Slot each requested mint's balance was read at.
    pub slots: HashMap<Pubkey, u64>,
    /// Highest slot read, or the finalized slot when there are no mints.
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

/// Most keys one `getMultipleAccounts` call accepts.
const MAX_ACCOUNTS_PER_CALL: usize = 100;

/// Escrow custody read from the instance ATA of each `(mint, token_program)`. The program only
/// moves funds through these ATAs, and reading them by address, unlike listing every account the
/// escrow owns, is a fixed request nobody can inflate by creating accounts for the escrow.
pub async fn fetch_escrow_custody(
    rpc_client: &RpcClientWithRetry,
    escrow_instance_id: Pubkey,
    mints: &[(Pubkey, Pubkey)],
) -> Result<EscrowCustody, SweepFailure> {
    if mints.is_empty() {
        let slot = rpc_client
            .with_retry("get_slot", RetryPolicy::Idempotent, || async {
                rpc_client
                    .rpc_client
                    .get_slot_with_commitment(CommitmentConfig::finalized())
                    .await
            })
            .await
            .map_err(|e| read_failure(format!("Failed to read the finalized slot: {e}")))?;
        return Ok(EscrowCustody {
            balances: HashMap::new(),
            slots: HashMap::new(),
            slot,
        });
    }

    // Each mint is compared with the ledger at its own slot, so chunks need not agree on one,
    // but each must be at or past a recent block so a lagging backend cannot hide a drain.
    let anchor = newest_block_anchor(rpc_client, "Solana")
        .await
        .map_err(SweepFailure::Read)?;
    let (balances, slots) = read_custody_once(rpc_client, escrow_instance_id, mints, anchor)
        .await
        .map_err(SweepFailure::Read)?;
    let slot = slots.values().copied().max().unwrap_or_default();
    Ok(EscrowCustody {
        balances,
        slots,
        slot,
    })
}

fn read_failure(reason: String) -> SweepFailure {
    SweepFailure::Read(EscrowSweepError { reason })
}

/// One pass over every ATA. Returns the balances plus the slot each mint was read at, refusing
/// any chunk answered below `anchor`.
async fn read_custody_once(
    rpc_client: &RpcClientWithRetry,
    escrow_instance_id: Pubkey,
    mints: &[(Pubkey, Pubkey)],
    anchor: u64,
) -> Result<(HashMap<Pubkey, u64>, HashMap<Pubkey, u64>), EscrowSweepError> {
    let mut balances = HashMap::new();
    let mut slots = HashMap::new();

    for chunk in mints.chunks(MAX_ACCOUNTS_PER_CALL) {
        let keys: Vec<String> = chunk
            .iter()
            .map(|(mint, token_program)| {
                get_associated_token_address_with_program_id(
                    &escrow_instance_id,
                    mint,
                    token_program,
                )
                .to_string()
            })
            .collect();
        // Raw request so an account that will not decode is an error, never "absent". A backend
        // below `anchor` refuses with -32016, which is retried rather than read as stale.
        let response = rpc_client
            .with_retry("get_multiple_accounts", RetryPolicy::Idempotent, || async {
                rpc_client
                    .rpc_client
                    .send::<Response<Vec<Option<UiAccount>>>>(
                        RpcRequest::GetMultipleAccounts,
                        serde_json::json!([
                            keys,
                            {"encoding": "base64", "commitment": "finalized", "minContextSlot": anchor}
                        ]),
                    )
                    .await
            })
            .await
            .map_err(|e| EscrowSweepError {
                reason: format!("Failed to read escrow ATAs: {e}"),
            })?;
        if response.value.len() != chunk.len() {
            return Err(EscrowSweepError {
                reason: format!(
                    "Escrow ATA read returned {} accounts for {} keys",
                    response.value.len(),
                    chunk.len()
                ),
            });
        }
        if response.context.slot < anchor {
            return Err(EscrowSweepError {
                reason: format!(
                    "Escrow ATA read answered at slot {}, behind the Solana block {anchor}",
                    response.context.slot
                ),
            });
        }
        for (mint, _) in chunk {
            slots.insert(*mint, response.context.slot);
        }
        for ((mint, token_program), account) in chunk.iter().zip(response.value) {
            // No account at the ATA means the escrow holds none of this mint.
            let Some(account) = account else { continue };
            let amount = decode_ata_amount(&account, mint, token_program)?;
            balances.insert(*mint, amount);
        }
    }

    Ok((balances, slots))
}

/// The balance of one escrow ATA, refusing anything that is not `mint`'s token account.
fn decode_ata_amount(
    account: &UiAccount,
    mint: &Pubkey,
    token_program: &Pubkey,
) -> Result<u64, EscrowSweepError> {
    let fail = |why: &str| EscrowSweepError {
        reason: format!("Escrow ATA for mint {mint} {why}"),
    };
    if account.owner != token_program.to_string() {
        return Err(fail(&format!(
            "is owned by {} instead of {token_program}",
            account.owner
        )));
    }
    let data = account
        .data
        .decode()
        .ok_or_else(|| fail("did not decode"))?;
    let (account_mint, amount) = if *token_program == spl_token_2022::id() {
        let state = StateWithExtensions::<Token2022Account>::unpack(&data)
            .map_err(|e| fail(&format!("is not a token account: {e}")))?;
        (state.base.mint, state.base.amount)
    } else {
        let state = TokenAccount::unpack(&data)
            .map_err(|e| fail(&format!("is not a token account: {e}")))?;
        (state.mint, state.amount)
    };
    if account_mint != *mint {
        return Err(fail(&format!("holds mint {account_mint}")));
    }
    Ok(amount)
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
                if token_program_id == spl_token_2022::id() {
                    let token_account = StateWithExtensions::<Token2022Account>::unpack(&decoded)
                        .map_err(|e| EscrowSweepError {
                        reason: format!("Failed to parse token-2022 account: {e}"),
                    })?;
                    (token_account.base.mint, token_account.base.amount)
                } else {
                    let token_account =
                        TokenAccount::unpack(&decoded).map_err(|e| EscrowSweepError {
                            reason: format!(
                                "Failed to parse token account for program {token_program_id}: {e}"
                            ),
                        })?;
                    (token_account.mint, token_account.amount)
                }
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
    fetch_channel_supply_at(channel_rpc, mint)
        .await
        .map(|(supply, _)| supply)
}

/// Channel supply for `mint` and the context slot the node answered at. The node reads
/// its slot before the account, so the supply is at least as new as that slot.
pub async fn fetch_channel_supply_at(
    channel_rpc: &RpcClientWithRetry,
    mint: &Pubkey,
) -> Result<(u64, u64), EscrowSweepError> {
    // Only a truly absent account is Ok(None); a node error or data that will not
    // decode is Err, so neither can masquerade as zero supply.
    let response = channel_rpc
        .get_account_with_context(mint, CommitmentConfig::finalized())
        .await
        .map_err(|e| EscrowSweepError {
            reason: format!("Failed to fetch channel mint account {mint}: {e}"),
        })?;

    let slot = response.context.slot;

    // Absent account = nothing minted yet.
    let account = match response.value {
        Some(account) => account,
        None => return Ok((0, slot)),
    };

    // The channel program mints classic SPL tokens (not Token-2022).
    let mint_state = Mint::unpack(&account.data).map_err(|e| EscrowSweepError {
        reason: format!("Failed to parse channel mint account {mint}: {e}"),
    })?;
    Ok((mint_state.supply, slot))
}

/// Extra reads a channel supply answer behind the anchor gets before it counts as stale.
pub const SUPPLY_REREADS: u32 = 3;

/// Channel supply for `mint`, re-read with the client's backoff while it answers behind
/// `anchor`, since the node ignores `minContextSlot`. The last answer is returned either way.
pub async fn fetch_fresh_channel_supply(
    channel_rpc: &RpcClientWithRetry,
    mint: &Pubkey,
    anchor: u64,
) -> Result<(u64, u64), EscrowSweepError> {
    let retry = &channel_rpc.retry_config;
    let mut read = fetch_channel_supply_at(channel_rpc, mint).await?;
    for attempt in 0..SUPPLY_REREADS {
        if read.1 >= anchor {
            break;
        }
        tokio::time::sleep((retry.base_delay * 2_u32.pow(attempt)).min(retry.max_delay)).await;
        read = fetch_channel_supply_at(channel_rpc, mint).await?;
    }
    Ok(read)
}

/// Oldest a chain's newest block may be, and how far its time may run ahead of ours, before
/// its reads count as unknown. Far above heartbeat, replica lag and clock skew, far below the halt window.
pub const CHANNEL_MAX_AGE_SECS: i64 = 120;

/// Slot windows searched below the tip for the newest block, smallest first.
const ANCHOR_WINDOWS: [u64; 3] = [64, 4_096, 262_144];

/// The channel's newest block slot, proven younger than `CHANNEL_MAX_AGE_SECS`. The node ignores
/// `minContextSlot`, so a supply read answered at or after this slot is what proves freshness;
/// a frozen node, a lagging replica or an older load-balanced backend all fail it.
pub async fn channel_anchor(channel_rpc: &RpcClientWithRetry) -> Result<u64, EscrowSweepError> {
    newest_block_anchor(channel_rpc, "channel").await
}

/// `chain`'s newest finalized block slot, proven younger than `CHANNEL_MAX_AGE_SECS`.
async fn newest_block_anchor(
    rpc: &RpcClientWithRetry,
    chain: &str,
) -> Result<u64, EscrowSweepError> {
    let fail = |reason: String| EscrowSweepError { reason };
    // Finalized like the reads it anchors, so a node that honors commitment compares like with like.
    let finalized = CommitmentConfig::finalized();
    let tip = rpc
        .with_retry("get_slot", RetryPolicy::Idempotent, || async {
            rpc.rpc_client.get_slot_with_commitment(finalized).await
        })
        .await
        .map_err(|e| fail(format!("Failed to read the {chain} slot: {e}")))?;

    // Idle slots carry no block, so look back for the newest one that does.
    let mut block = None;
    for window in ANCHOR_WINDOWS {
        let start = tip.saturating_sub(window);
        let blocks = rpc
            .with_retry("get_blocks", RetryPolicy::Idempotent, || async {
                rpc.rpc_client
                    .get_blocks_with_commitment(start, Some(tip), finalized)
                    .await
            })
            .await
            .map_err(|e| fail(format!("Failed to list {chain} blocks: {e}")))?;
        if let Some(newest) = blocks.into_iter().max() {
            block = Some(newest);
            break;
        }
    }
    let block =
        block.ok_or_else(|| fail(format!("no {chain} block found at or below slot {tip}")))?;

    let block_time = rpc
        .get_block_time(block)
        .await
        .map_err(|e| fail(format!("{chain} block {block} has no time: {e}")))?;
    let age = chrono::Utc::now().timestamp().saturating_sub(block_time);
    if age > CHANNEL_MAX_AGE_SECS {
        return Err(fail(format!(
            "{chain}'s newest block {block} is {age}s old, past the {CHANNEL_MAX_AGE_SECS}s limit"
        )));
    }
    // A clock far ahead of ours would hide a frozen node for as long as it is ahead.
    if age < -CHANNEL_MAX_AGE_SECS {
        return Err(fail(format!(
            "{chain}'s newest block {block} is stamped {}s ahead of our clock, past the {CHANNEL_MAX_AGE_SECS}s limit",
            -age
        )));
    }
    Ok(block)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::operator::RetryConfig;
    use base64::Engine as _;
    use solana_commitment_config::CommitmentConfig;
    use spl_token::solana_program::program_option::COption;
    use spl_token::state::AccountState;
    use spl_token_2022::extension::transfer_hook::TransferHookAccount;
    use spl_token_2022::extension::{
        BaseStateWithExtensionsMut, ExtensionType, StateWithExtensionsMut,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

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
    async fn fetch_channel_supply_undecodable_mint_is_err() {
        // Data the operator cannot decode must never read as an absent mint, which is 0 supply.
        let mut server = mockito::Server::new_async().await;
        server
            .mock("POST", "/")
            .with_status(200)
            .with_body(r#"{"jsonrpc":"2.0","id":1,"result":{"context":{"slot":1},"value":{"owner":"11111111111111111111111111111111","lamports":1,"data":["not base64!","base64"],"executable":false,"rentEpoch":0}}}"#)
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
        assert!(
            result.is_err(),
            "undecodable mint must be Err, got {result:?}"
        );
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

    fn token2022_base(mint: Pubkey, amount: u64) -> Token2022Account {
        Token2022Account {
            mint,
            owner: Pubkey::new_unique(),
            amount,
            delegate: COption::None,
            state: spl_token_2022::state::AccountState::Initialized,
            is_native: COption::None,
            delegated_amount: 0,
            close_authority: COption::None,
        }
    }

    /// Wrap raw Token-2022 account data as one base64 `RpcKeyedAccount`, reporting the
    /// real length as `space` so the fixture stays self-consistent.
    fn keyed_token2022_account(data: &[u8]) -> String {
        let b64 = base64::engine::general_purpose::STANDARD.encode(data);
        format!(
            r#"{{"pubkey":"{ata}","account":{{"lamports":2039280,"owner":"{prog}","executable":false,"rentEpoch":0,"space":{space},"data":["{b64}","base64"]}}}}"#,
            ata = Pubkey::new_unique(),
            prog = spl_token_2022::id(),
            space = data.len(),
        )
    }

    /// An extended Token-2022 account, sized by the library so the fixture cannot drift
    /// from the on-chain layout: base state, account-type discriminator, then a TLV entry
    /// costing four bytes of header before its value.
    fn base64_token2022_account(mint: Pubkey, amount: u64) -> String {
        keyed_token2022_account(&token2022_extended_bytes(mint, amount))
    }

    /// Raw bytes of an extended Token-2022 account.
    fn token2022_extended_bytes(mint: Pubkey, amount: u64) -> Vec<u8> {
        let len = ExtensionType::try_calculate_account_len::<Token2022Account>(&[
            ExtensionType::TransferHookAccount,
        ])
        .expect("a fixed-length extension has a calculable account length");
        let mut buf = vec![0u8; len];
        let mut state = StateWithExtensionsMut::<Token2022Account>::unpack_uninitialized(&mut buf)
            .expect("a zeroed buffer holds an uninitialized account");
        state
            .init_extension::<TransferHookAccount>(true)
            .expect("the extension fits the calculated length");
        state.base = token2022_base(mint, amount);
        state.pack_base();
        state
            .init_account_type()
            .expect("the account type matches the extension");
        buf
    }

    /// The other layout Token-2022 produces: no extensions, so the account stays at the
    /// bare 165-byte base with no discriminator and no TLV data.
    fn base64_token2022_account_without_extensions(mint: Pubkey, amount: u64) -> String {
        let mut buf = vec![0u8; Token2022Account::LEN];
        token2022_base(mint, amount).pack_into_slice(&mut buf);
        keyed_token2022_account(&buf)
    }

    /// Mirror of `mock_sweep` with the accounts on the Token-2022 side instead, so the
    /// extension layout actually reaches the unpacker. Both calls answer at the same
    /// slot to satisfy the sweep's slot-agreement check.
    async fn mock_sweep_token_2022(server: &mut mockito::Server, token_2022_accounts: &[String]) {
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(spl_token::id().to_string()))
            .with_status(200)
            .with_body(empty_body(1))
            .create_async()
            .await;
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(spl_token_2022::id().to_string()))
            .with_status(200)
            .with_body(result_body(token_2022_accounts, 1))
            .create_async()
            .await;
    }

    #[tokio::test]
    async fn decodes_token2022_account_with_extensions() {
        let mut server = mockito::Server::new_async().await;
        let mint = Pubkey::new_unique();
        mock_sweep_token_2022(&mut server, &[base64_token2022_account(mint, 4_242)]).await;

        let balances = fetch_escrow_balances_by_mint(&client(&server.url()), Pubkey::new_unique())
            .await
            .unwrap()
            .balances;

        assert_eq!(
            balances[&mint], 4_242,
            "token-2022 extension layout must unpack"
        );
    }

    /// The common case: a Token-2022 account carrying no extensions is the bare base
    /// layout, and dispatching it through `StateWithExtensions` must still read it.
    #[tokio::test]
    async fn decodes_token2022_account_without_extensions() {
        let mut server = mockito::Server::new_async().await;
        let mint = Pubkey::new_unique();
        mock_sweep_token_2022(
            &mut server,
            &[base64_token2022_account_without_extensions(mint, 7_000)],
        )
        .await;

        let balances = fetch_escrow_balances_by_mint(&client(&server.url()), Pubkey::new_unique())
            .await
            .unwrap()
            .balances;

        assert_eq!(
            balances[&mint], 7_000,
            "an unextended token-2022 account must unpack"
        );
    }

    /// Guards the fixtures themselves: Token-2022 allocates either the bare base or the
    /// base plus a discriminator plus at least one four-byte TLV header, never the
    /// base-plus-discriminator layout in between.
    #[test]
    fn token2022_fixtures_use_lengths_the_program_allocates() {
        let bare = ExtensionType::try_calculate_account_len::<Token2022Account>(&[]).unwrap();
        let extended = ExtensionType::try_calculate_account_len::<Token2022Account>(&[
            ExtensionType::TransferHookAccount,
        ])
        .unwrap();

        assert_eq!(bare, Token2022Account::LEN, "no extensions means no suffix");
        assert!(
            extended >= Token2022Account::LEN + 5,
            "an extended account carries a discriminator and a TLV header, got {extended}"
        );
    }

    // ── fetch_escrow_custody ──────────────────────────────────────────

    /// Raw bytes of an SPL token account for `mint`.
    fn spl_account_bytes(mint: Pubkey, amount: u64) -> Vec<u8> {
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
        buf
    }

    /// One `getMultipleAccounts` entry holding `data` under the program `owner`.
    fn ui_account(owner: Pubkey, data: &[u8]) -> serde_json::Value {
        serde_json::json!({
            "lamports": 2_039_280u64,
            "owner": owner.to_string(),
            "executable": false,
            "rentEpoch": 0,
            "space": data.len(),
            "data": [base64::engine::general_purpose::STANDARD.encode(data), "base64"],
        })
    }

    /// Answer `getMultipleAccounts` per requested key from `accounts` (absent keys are null).
    /// Call `n` answers at `slots[n]`, repeating the last slot; requested keys are recorded.
    async fn mock_multiple_accounts(
        server: &mut mockito::Server,
        accounts: HashMap<Pubkey, serde_json::Value>,
        slots: Vec<u64>,
        requested: Arc<Mutex<Vec<Vec<String>>>>,
    ) {
        let calls = Arc::new(AtomicUsize::new(0));
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(
                serde_json::json!({"method": "getMultipleAccounts"}),
            ))
            .with_status(200)
            .with_body_from_request(move |req| {
                let body: serde_json::Value = serde_json::from_slice(req.body().unwrap()).unwrap();
                let keys: Vec<String> = body["params"][0]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|k| k.as_str().unwrap().to_string())
                    .collect();
                let value: Vec<serde_json::Value> = keys
                    .iter()
                    .map(|k| {
                        accounts
                            .get(&Pubkey::from_str(k).unwrap())
                            .cloned()
                            .unwrap_or(serde_json::Value::Null)
                    })
                    .collect();
                requested.lock().unwrap().push(keys);
                let n = calls.fetch_add(1, Ordering::SeqCst);
                let slot = slots[n.min(slots.len() - 1)];
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {"context": {"slot": slot}, "value": value}
                })
                .to_string()
                .into_bytes()
            })
            .create_async()
            .await;
    }

    fn ata(instance: &Pubkey, mint: &Pubkey, program: &Pubkey) -> Pubkey {
        spl_associated_token_account::get_associated_token_address_with_program_id(
            instance, mint, program,
        )
    }

    /// Custody is exactly the instance ATAs: an absent ATA holds zero, both token programs
    /// decode, and nothing else the escrow owns is ever listed, so it cannot be inflated.
    #[tokio::test]
    async fn custody_reads_only_the_instance_atas() {
        let mut server = mockito::Server::new_async().await;
        let owner_sweep = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(
                serde_json::json!({"method": "getTokenAccountsByOwner"}),
            ))
            .expect(0)
            .create_async()
            .await;
        let instance = Pubkey::new_unique();
        let (absent, spl, t22) = (
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
        );
        let accounts = HashMap::from([
            (
                ata(&instance, &spl, &spl_token::id()),
                ui_account(spl_token::id(), &spl_account_bytes(spl, 500)),
            ),
            (
                ata(&instance, &t22, &spl_token_2022::id()),
                ui_account(spl_token_2022::id(), &token2022_extended_bytes(t22, 700)),
            ),
        ]);
        let requested = Arc::new(Mutex::new(Vec::new()));
        mock_channel_clock(&mut server, 42, vec![42], Some(1)).await;
        mock_multiple_accounts(&mut server, accounts, vec![42], requested.clone()).await;
        let mints = [
            (absent, spl_token::id()),
            (spl, spl_token::id()),
            (t22, spl_token_2022::id()),
        ];

        let snapshot = fetch_escrow_custody(&client(&server.url()), instance, &mints)
            .await
            .unwrap();

        assert_eq!(snapshot.slot, 42);
        assert_eq!(snapshot.balances.get(&absent).copied().unwrap_or(0), 0);
        assert_eq!(snapshot.balances[&spl], 500);
        assert_eq!(snapshot.balances[&t22], 700);
        let expected: Vec<String> = mints
            .iter()
            .map(|(m, p)| ata(&instance, m, p).to_string())
            .collect();
        assert_eq!(*requested.lock().unwrap(), vec![expected]);
        owner_sweep.assert_async().await;
    }

    /// An account that does not look like this mint's token account is an error, never zero.
    #[tokio::test]
    async fn custody_rejects_an_account_that_is_not_the_mints_ata() {
        let instance = Pubkey::new_unique();
        let mint = Pubkey::new_unique();
        let key = ata(&instance, &mint, &spl_token::id());
        let cases = [
            (
                "other mint",
                ui_account(spl_token::id(), &spl_account_bytes(Pubkey::new_unique(), 5)),
            ),
            (
                "other program",
                ui_account(Pubkey::new_unique(), &spl_account_bytes(mint, 5)),
            ),
            ("undecodable", ui_account(spl_token::id(), &[1, 2, 3])),
        ];
        for (label, account) in cases {
            let mut server = mockito::Server::new_async().await;
            mock_channel_clock(&mut server, 1, vec![1], Some(1)).await;
            mock_multiple_accounts(
                &mut server,
                HashMap::from([(key, account)]),
                vec![1],
                Arc::new(Mutex::new(Vec::new())),
            )
            .await;

            let result =
                fetch_escrow_custody(&client(&server.url()), instance, &[(mint, spl_token::id())])
                    .await;

            assert!(
                matches!(result, Err(SweepFailure::Read(_))),
                "{label}: {result:?}"
            );
        }
    }

    /// More mints than one call takes are read in chunks, and each mint keeps its chunk's slot.
    #[tokio::test]
    async fn custody_chunks_keep_each_mints_slot() {
        let instance = Pubkey::new_unique();
        let mints: Vec<(Pubkey, Pubkey)> = (0..101)
            .map(|_| (Pubkey::new_unique(), spl_token::id()))
            .collect();
        let held = mints[100].0;
        let accounts = HashMap::from([(
            ata(&instance, &held, &spl_token::id()),
            ui_account(spl_token::id(), &spl_account_bytes(held, 9)),
        )]);

        let mut agreeing = mockito::Server::new_async().await;
        let requested = Arc::new(Mutex::new(Vec::new()));
        mock_channel_clock(&mut agreeing, 7, vec![7], Some(1)).await;
        mock_multiple_accounts(&mut agreeing, accounts.clone(), vec![7], requested.clone()).await;
        let snapshot = fetch_escrow_custody(&client(&agreeing.url()), instance, &mints)
            .await
            .unwrap();
        assert_eq!(snapshot.slot, 7);
        assert_eq!(snapshot.balances[&held], 9);
        let sizes: Vec<usize> = requested.lock().unwrap().iter().map(Vec::len).collect();
        assert_eq!(sizes, vec![100, 1], "one call per 100 keys");

        // Chunks answering at different slots are kept, each mint tagged with its own slot.
        let mut split = mockito::Server::new_async().await;
        let alternating = (0..20).map(|n| 10 + n % 2).collect();
        mock_channel_clock(&mut split, 10, vec![10], Some(1)).await;
        mock_multiple_accounts(
            &mut split,
            accounts,
            alternating,
            Arc::new(Mutex::new(Vec::new())),
        )
        .await;
        let custody = fetch_escrow_custody(&client(&split.url()), instance, &mints)
            .await
            .unwrap();
        assert_eq!(custody.slots[&mints[0].0], 10);
        assert_eq!(custody.slots[&held], 11);
        assert_eq!(custody.slot, 11, "the highest slot read");
        assert_eq!(custody.balances[&held], 9);
    }

    /// A chunk answered below the chain's newest recent block is stale and fails the read,
    /// whether it is the only chunk or one of several.
    #[tokio::test]
    async fn custody_rejects_a_chunk_behind_the_chain_anchor() {
        let instance = Pubkey::new_unique();
        let many: Vec<(Pubkey, Pubkey)> = (0..101)
            .map(|_| (Pubkey::new_unique(), spl_token::id()))
            .collect();
        let one = vec![many[0]];
        for (label, mints, slots) in [
            ("one lagging chunk of two", &many, vec![50, 40]),
            ("a single lagging chunk", &one, vec![40]),
        ] {
            let mut server = mockito::Server::new_async().await;
            mock_channel_clock(&mut server, 50, vec![50], Some(1)).await;
            mock_multiple_accounts(
                &mut server,
                HashMap::new(),
                slots,
                Arc::new(Mutex::new(Vec::new())),
            )
            .await;

            let result = fetch_escrow_custody(&fast_client(&server.url()), instance, mints).await;

            assert!(
                matches!(result, Err(SweepFailure::Read(_))),
                "{label}: {result:?}"
            );
        }
    }

    /// Answer `getMultipleAccounts` with -32016 for the first `lagging` calls, then with no
    /// accounts at `slot`, recording each call's config.
    async fn mock_multiple_accounts_min_slot(
        server: &mut mockito::Server,
        lagging: usize,
        slot: u64,
        configs: Arc<Mutex<Vec<serde_json::Value>>>,
    ) {
        let calls = Arc::new(AtomicUsize::new(0));
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(
                serde_json::json!({"method": "getMultipleAccounts"}),
            ))
            .with_status(200)
            .with_body_from_request(move |req| {
                let body: serde_json::Value = serde_json::from_slice(req.body().unwrap()).unwrap();
                configs.lock().unwrap().push(body["params"][1].clone());
                let n = body["params"][0].as_array().unwrap().len();
                let reply = if calls.fetch_add(1, Ordering::SeqCst) < lagging {
                    serde_json::json!({"jsonrpc": "2.0", "id": 1, "error": {
                        "code": -32016, "message": "Minimum context slot has not been reached"}})
                } else {
                    serde_json::json!({"jsonrpc": "2.0", "id": 1, "result": {
                        "context": {"slot": slot}, "value": vec![serde_json::Value::Null; n]}})
                };
                reply.to_string().into_bytes()
            })
            .create_async()
            .await;
    }

    /// A retrying client with no real backoff, so lag cases run fast.
    fn retrying_client(url: &str, max_attempts: u32) -> RpcClientWithRetry {
        RpcClientWithRetry::with_retry_config(
            url.to_string(),
            RetryConfig {
                max_attempts,
                base_delay: std::time::Duration::from_millis(1),
                max_delay: std::time::Duration::from_millis(1),
            },
            CommitmentConfig::finalized(),
        )
    }

    /// Custody asks the node for the anchor as its minimum slot, so a lagging backend refuses
    /// instead of answering old, and a retry lands once it or another backend catches up.
    #[tokio::test]
    async fn custody_retries_a_backend_below_the_min_context_slot() {
        let mut server = mockito::Server::new_async().await;
        mock_channel_clock(&mut server, 50, vec![50], Some(1)).await;
        let configs = Arc::new(Mutex::new(Vec::new()));
        mock_multiple_accounts_min_slot(&mut server, 2, 50, configs.clone()).await;

        let custody = fetch_escrow_custody(
            &retrying_client(&server.url(), 5),
            Pubkey::new_unique(),
            &[(Pubkey::new_unique(), spl_token::id())],
        )
        .await
        .unwrap();

        assert_eq!(custody.slot, 50);
        let configs = configs.lock().unwrap();
        assert_eq!(configs.len(), 3, "two refusals then an answer");
        assert!(
            configs.iter().all(|c| c["minContextSlot"] == 50),
            "{configs:?}"
        );
    }

    /// A backend that never reaches the anchor still fails the read once retries run out.
    #[tokio::test]
    async fn custody_fails_when_the_backend_stays_below_the_min_context_slot() {
        let mut server = mockito::Server::new_async().await;
        mock_channel_clock(&mut server, 50, vec![50], Some(1)).await;
        mock_multiple_accounts_min_slot(&mut server, usize::MAX, 50, Arc::default()).await;

        let result = fetch_escrow_custody(
            &retrying_client(&server.url(), 3),
            Pubkey::new_unique(),
            &[(Pubkey::new_unique(), spl_token::id())],
        )
        .await;

        assert!(matches!(result, Err(SweepFailure::Read(_))), "{result:?}");
    }

    /// Custody is only as fresh as the chain it is read from: an old newest block fails the read.
    #[tokio::test]
    async fn custody_requires_a_recent_chain_block() {
        let mut server = mockito::Server::new_async().await;
        mock_channel_clock(&mut server, 50, vec![50], Some(CHANNEL_MAX_AGE_SECS + 1)).await;
        mock_multiple_accounts(
            &mut server,
            HashMap::new(),
            vec![50],
            Arc::new(Mutex::new(Vec::new())),
        )
        .await;

        let result = fetch_escrow_custody(
            &fast_client(&server.url()),
            Pubkey::new_unique(),
            &[(Pubkey::new_unique(), spl_token::id())],
        )
        .await;

        assert!(matches!(result, Err(SweepFailure::Read(_))), "{result:?}");
    }

    /// With no mints there is nothing to read, but the snapshot still needs a slot to pin the ledger.
    #[tokio::test]
    async fn custody_with_no_mints_takes_the_finalized_slot() {
        let mut server = mockito::Server::new_async().await;
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(
                serde_json::json!({"method": "getSlot"}),
            ))
            .with_status(200)
            .with_body(r#"{"jsonrpc":"2.0","id":1,"result":77}"#)
            .create_async()
            .await;

        let snapshot = fetch_escrow_custody(&client(&server.url()), Pubkey::new_unique(), &[])
            .await
            .unwrap();

        assert_eq!(snapshot.slot, 77);
        assert!(snapshot.balances.is_empty());
    }

    // ── channel freshness ─────────────────────────────────────────────

    /// Mock the channel clock: `getSlot` answers `tip`, `getBlocks` answers the slots of
    /// `blocks` inside the requested range, and `getBlockTime` answers `age_secs` before
    /// the moment of the request (null when `age_secs` is None).
    pub(crate) async fn mock_channel_clock(
        server: &mut mockito::Server,
        tip: u64,
        blocks: Vec<u64>,
        age_secs: Option<i64>,
    ) {
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(
                serde_json::json!({"method": "getSlot"}),
            ))
            .with_status(200)
            .with_body(format!(r#"{{"jsonrpc":"2.0","id":1,"result":{tip}}}"#))
            .create_async()
            .await;
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(
                serde_json::json!({"method": "getBlocks"}),
            ))
            .with_status(200)
            .with_body_from_request(move |req| {
                let body: serde_json::Value = serde_json::from_slice(req.body().unwrap()).unwrap();
                let start = body["params"][0].as_u64().unwrap();
                let end = body["params"][1].as_u64().unwrap();
                let hits: Vec<u64> = blocks
                    .iter()
                    .copied()
                    .filter(|b| (start..=end).contains(b))
                    .collect();
                serde_json::json!({"jsonrpc": "2.0", "id": 1, "result": hits})
                    .to_string()
                    .into_bytes()
            })
            .create_async()
            .await;
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(
                serde_json::json!({"method": "getBlockTime"}),
            ))
            .with_status(200)
            .with_body_from_request(move |_| {
                let time = age_secs.map(|age| chrono::Utc::now().timestamp() - age);
                serde_json::json!({"jsonrpc": "2.0", "id": 1, "result": time})
                    .to_string()
                    .into_bytes()
            })
            .create_async()
            .await;
    }

    /// A single-attempt client so an error case fails fast.
    fn fast_client(url: &str) -> RpcClientWithRetry {
        RpcClientWithRetry::with_retry_config(
            url.to_string(),
            RetryConfig {
                max_attempts: 1,
                base_delay: std::time::Duration::from_millis(1),
                max_delay: std::time::Duration::from_millis(1),
            },
            CommitmentConfig::finalized(),
        )
    }

    /// The anchor is the newest block at or below the tip, and only if it is recent.
    #[tokio::test]
    async fn channel_anchor_requires_a_recent_block() {
        // (label, tip, blocks, block age, expected anchor)
        type Case = (&'static str, u64, Vec<u64>, Option<i64>, Option<u64>);
        let cases: [Case; 7] = [
            (
                "fresh block in the first window",
                1_000,
                vec![990, 995],
                Some(1),
                Some(995),
            ),
            (
                "block only in a wider window",
                10_000,
                vec![8_000],
                Some(1),
                Some(8_000),
            ),
            ("no block at all", 10_000, vec![], Some(1), None),
            ("block with no time", 1_000, vec![995], None, None),
            (
                "stale block",
                1_000,
                vec![995],
                Some(CHANNEL_MAX_AGE_SECS + 1),
                None,
            ),
            (
                "clock ahead of ours",
                1_000,
                vec![995],
                Some(-30),
                Some(995),
            ),
            (
                "clock far ahead of ours",
                1_000,
                vec![995],
                Some(-(CHANNEL_MAX_AGE_SECS + 1)),
                None,
            ),
        ];
        for (label, tip, blocks, age, expected) in cases {
            let mut server = mockito::Server::new_async().await;
            mock_channel_clock(&mut server, tip, blocks, age).await;

            let anchor = channel_anchor(&fast_client(&server.url())).await;

            assert_eq!(anchor.ok(), expected, "{label}");
        }
    }

    /// The supply read reports the slot it was answered at, even for a mint not created yet.
    #[tokio::test]
    async fn channel_supply_reports_its_context_slot() {
        let mut present = mockito::Server::new_async().await;
        present
            .mock("POST", "/")
            .with_status(200)
            .with_body(mint_account_body(1_234).replace(r#""slot":1"#, r#""slot":4242"#))
            .create_async()
            .await;
        let got = fetch_channel_supply_at(&client(&present.url()), &Pubkey::new_unique())
            .await
            .unwrap();
        assert_eq!(got, (1_234, 4242));

        let mut absent = mockito::Server::new_async().await;
        absent
            .mock("POST", "/")
            .with_status(200)
            .with_body(r#"{"jsonrpc":"2.0","id":1,"result":{"context":{"slot":77},"value":null}}"#)
            .create_async()
            .await;
        let got = fetch_channel_supply_at(&client(&absent.url()), &Pubkey::new_unique())
            .await
            .unwrap();
        assert_eq!(got, (0, 77));
    }

    /// Answer every `getAccountInfo` with a mint of supply 7 at the next slot in `slots`,
    /// repeating the last one, and count the calls.
    async fn mock_supply_slots(server: &mut mockito::Server, slots: Vec<u64>) -> Arc<AtomicUsize> {
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = calls.clone();
        server
            .mock("POST", "/")
            .with_status(200)
            .with_body_from_request(move |_| {
                let n = counter.fetch_add(1, Ordering::SeqCst);
                let slot = slots[n.min(slots.len() - 1)];
                mint_account_body(7)
                    .replace(r#""slot":1"#, &format!(r#""slot":{slot}"#))
                    .into_bytes()
            })
            .create_async()
            .await;
        calls
    }

    /// The channel ignores `minContextSlot`, so a read behind the anchor is re-read until it
    /// catches up.
    #[tokio::test]
    async fn fresh_supply_rereads_until_the_anchor() {
        let mut server = mockito::Server::new_async().await;
        let calls = mock_supply_slots(&mut server, vec![8, 9, 10]).await;

        let got = fetch_fresh_channel_supply(
            &retrying_client(&server.url(), 1),
            &Pubkey::new_unique(),
            10,
        )
        .await
        .unwrap();

        assert_eq!(got, (7, 10));
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    /// Re-reads are bounded: a read that stays behind comes back behind, for the caller to refuse.
    #[tokio::test]
    async fn fresh_supply_gives_up_behind_the_anchor() {
        let mut server = mockito::Server::new_async().await;
        let calls = mock_supply_slots(&mut server, vec![8]).await;

        let got = fetch_fresh_channel_supply(
            &retrying_client(&server.url(), 1),
            &Pubkey::new_unique(),
            10,
        )
        .await
        .unwrap();

        assert_eq!(got, (7, 8));
        assert_eq!(calls.load(Ordering::SeqCst), 1 + SUPPLY_REREADS as usize);
    }
}
