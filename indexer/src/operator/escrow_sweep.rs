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
use solana_sdk::pubkey::Pubkey;
use spl_token::solana_program::program_pack::Pack;
use spl_token::state::Account as TokenAccount;
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

/// Sum every token account owned by the escrow instance, grouped by mint, across
/// the SPL Token and Token-2022 programs. The returned map is the on-chain
/// custody snapshot; a mint absent from the map holds zero on-chain.
pub async fn fetch_escrow_balances_by_mint(
    rpc_client: &RpcClientWithRetry,
    escrow_instance_id: Pubkey,
) -> Result<HashMap<Pubkey, u64>, EscrowSweepError> {
    let mut balances = HashMap::new();
    let token_programs = [spl_token::id(), spl_token_2022::id()];

    for token_program_id in token_programs {
        let accounts = rpc_client
            .with_retry(
                "get_token_accounts_by_owner",
                RetryPolicy::Idempotent,
                || async {
                    rpc_client
                        .rpc_client
                        .get_token_accounts_by_owner(
                            &escrow_instance_id,
                            TokenAccountsFilter::ProgramId(token_program_id),
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

    Ok(balances)
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

    fn result_body(values: &[String]) -> String {
        format!(
            r#"{{"jsonrpc":"2.0","result":{{"context":{{"slot":1}},"value":[{}]}},"id":1}}"#,
            values.join(",")
        )
    }

    const EMPTY_BODY: &str =
        r#"{"jsonrpc":"2.0","result":{"context":{"slot":1},"value":[]},"id":1}"#;

    /// The sweep calls `get_token_accounts_by_owner` once per token program. Route the
    /// SPL Token call (matched by its program id in the request body) to `spl_accounts`
    /// and the Token-2022 call to an empty list so the two are not double-counted.
    async fn mock_sweep(server: &mut mockito::Server, spl_accounts: &[String]) {
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(spl_token::id().to_string()))
            .with_status(200)
            .with_body(result_body(spl_accounts))
            .create_async()
            .await;
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(spl_token_2022::id().to_string()))
            .with_status(200)
            .with_body(EMPTY_BODY)
            .create_async()
            .await;
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
            .unwrap();

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
            .unwrap();

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
            .unwrap();

        assert_eq!(balances.len(), 1);
        assert_eq!(balances[&mint], 50);
    }

    #[tokio::test]
    async fn empty_owner_returns_empty_map() {
        let mut server = mockito::Server::new_async().await;
        mock_sweep(&mut server, &[]).await;

        let balances = fetch_escrow_balances_by_mint(&client(&server.url()), Pubkey::new_unique())
            .await
            .unwrap();

        assert!(balances.is_empty());
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

        let err = result.expect_err("malformed account must error").reason;
        assert!(err.contains("tokenAmount"), "unexpected error: {err}");
    }
}
