use solana_account_decoder_client_types::UiAccountEncoding;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_client::GetConfirmedSignaturesForAddress2Config;
use solana_client::rpc_response::{Response, RpcBlockhash};
use solana_rpc_client_api::client_error;
use solana_rpc_client_api::client_error::ErrorKind;
use solana_rpc_client_api::config::{RpcAccountInfoConfig, RpcTransactionConfig};
use solana_rpc_client_api::request::{RpcError, RpcRequest};
use solana_sdk::account::Account;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::hash::Hash;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::warn;

use crate::operator::utils::instruction_util::RetryPolicy;

const DEFAULT_MAX_ATTEMPTS: u32 = 5;
const DEFAULT_BASE_DELAY: Duration = Duration::from_millis(100);
const DEFAULT_MAX_DELAY: Duration = Duration::from_secs(10);

/// Configuration for RPC retry behavior
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retry attempts
    pub max_attempts: u32,
    /// Base delay between retries (exponential backoff applied)
    pub base_delay: Duration,
    /// Maximum delay between retries
    pub max_delay: Duration,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            base_delay: DEFAULT_BASE_DELAY,
            max_delay: DEFAULT_MAX_DELAY,
        }
    }
}

/// Returns `true` for errors that will never succeed on retry.
// TODO: remove -32601 check once the RPC endpoint implements all required methods.
fn is_permanent_rpc_error(e: &client_error::Error) -> bool {
    let ErrorKind::RpcError(rpc_err) = e.kind() else {
        return false;
    };
    match rpc_err {
        // Method not supported by this RPC endpoint — protocol-level rejection.
        RpcError::RpcResponseError { code: -32601, .. } => true,
        // "AccountNotFound" is a definitive answer, not a transient failure.
        RpcError::ForUser(msg) => msg.contains("AccountNotFound"),
        _ => false,
    }
}

pub struct RpcClientWithRetry {
    pub rpc_client: Arc<RpcClient>,
    pub retry_config: RetryConfig,
}

impl RpcClientWithRetry {
    /// Create a new RPC client with custom retry config
    pub fn with_retry_config(
        url: String,
        retry_config: RetryConfig,
        commitment: CommitmentConfig,
    ) -> Self {
        Self {
            rpc_client: Arc::new(RpcClient::new_with_commitment(url, commitment)),
            retry_config,
        }
    }

    /// Execute an RPC operation with configurable retry behavior
    ///
    /// # Arguments
    /// * `operation_name` - Name for logging/debugging
    /// * `retry_policy` - Controls retry behavior (None or Idempotent)
    /// * `f` - Async operation to execute/retry
    ///
    /// # Returns
    /// Result from the operation or MaxRetriesExceeded error
    pub async fn with_retry<F, Fut, T, E>(
        &self,
        operation_name: &str,
        retry_policy: RetryPolicy,
        f: F,
    ) -> Result<T, Box<client_error::Error>>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
        E: std::fmt::Display + Into<Box<client_error::Error>>,
    {
        match retry_policy {
            RetryPolicy::None => {
                // Single attempt - no retry
                f().await.map_err(|e| e.into())
            }
            RetryPolicy::Idempotent => {
                let mut attempts = 0;

                loop {
                    attempts += 1;

                    match f().await {
                        Ok(result) => return Ok(result),
                        Err(e) => {
                            let err: Box<client_error::Error> = e.into();
                            if attempts >= self.retry_config.max_attempts
                                || is_permanent_rpc_error(&err)
                            {
                                warn!(
                                    "{} failed after {} attempts: {}",
                                    operation_name, attempts, err
                                );
                                return Err(err);
                            }

                            let delay = self.retry_config.base_delay * 2_u32.pow(attempts - 1);
                            sleep(delay.min(self.retry_config.max_delay)).await;
                        }
                    }
                }
            }
        }
    }

    /// Get recent blockhash with retry
    pub async fn get_latest_blockhash(&self) -> Result<Hash, Box<client_error::Error>> {
        self.with_retry("get_latest_blockhash", RetryPolicy::Idempotent, || async {
            self.rpc_client.get_latest_blockhash().await
        })
        .await
    }

    /// Get recent blockhash + last_valid_block_height with retry.
    pub async fn get_latest_blockhash_with_commitment(
        &self,
    ) -> Result<(Hash, u64), Box<client_error::Error>> {
        self.with_retry(
            "get_latest_blockhash_with_commitment",
            RetryPolicy::Idempotent,
            || async {
                self.rpc_client
                    .get_latest_blockhash_with_commitment(self.rpc_client.commitment())
                    .await
            },
        )
        .await
    }

    /// The same blockhash read as `get_latest_blockhash_with_commitment`, at the
    /// same commitment, but also returning the response's context slot as
    /// `(blockhash, context_slot, last_valid_block_height)`.
    ///
    /// A transaction cannot land in a block older than the blockhash it was signed
    /// with, so recording that slot alongside the broadcast gives the finality
    /// classifier an exact lower bound on where the signature could be, one that
    /// does not depend on what the node's blockhash window happens to be later.
    pub async fn get_latest_blockhash_with_commitment_and_context(
        &self,
    ) -> Result<(Hash, u64, u64), Box<client_error::Error>> {
        let commitment = self.rpc_client.commitment();
        self.with_retry(
            "get_latest_blockhash_with_commitment_and_context",
            RetryPolicy::Idempotent,
            || async {
                let response = self
                    .rpc_client
                    .send::<Response<RpcBlockhash>>(
                        RpcRequest::GetLatestBlockhash,
                        serde_json::json!([commitment]),
                    )
                    .await?;
                let blockhash = Hash::from_str(&response.value.blockhash).map_err(|e| {
                    Box::new(client_error::Error::from(ErrorKind::Custom(format!(
                        "unparseable blockhash {}: {e}",
                        response.value.blockhash
                    ))))
                })?;
                Ok::<_, Box<client_error::Error>>((
                    blockhash,
                    response.context.slot,
                    response.value.last_valid_block_height,
                ))
            },
        )
        .await
    }

    /// Get the current block height with retry, to compare against each stored
    /// signature's `last_valid_block_height` and decide whether a broadcast can
    /// still land. Both chains need it: a response context slot is a slot on
    /// either, and slots outrun heights, so judging an lvbh against one would
    /// abandon broadcasts that are still live.
    pub async fn get_block_height(&self) -> Result<u64, Box<client_error::Error>> {
        self.with_retry("get_block_height", RetryPolicy::Idempotent, || async {
            self.rpc_client.get_block_height().await
        })
        .await
    }

    /// Read the latest blockhash at `commitment` together with the response
    /// context slot, returning `(context_slot, last_valid_block_height)`. One RPC
    /// call so the slot and height come from the same backend and are mutually
    /// consistent. The release verifier reads this at finalized to anchor a
    /// freshness point it can then bind an account snapshot to via min_context_slot.
    pub async fn get_latest_blockhash_with_context(
        &self,
        commitment: CommitmentConfig,
    ) -> Result<(u64, u64), Box<client_error::Error>> {
        self.with_retry(
            "get_latest_blockhash_with_context",
            RetryPolicy::Idempotent,
            || async {
                self.rpc_client
                    .send::<Response<RpcBlockhash>>(
                        RpcRequest::GetLatestBlockhash,
                        serde_json::json!([commitment]),
                    )
                    .await
                    .map(|resp| (resp.context.slot, resp.value.last_valid_block_height))
            },
        )
        .await
    }

    /// Get the node's lowest retained slot with retry. An absence-based `Dead`
    /// finality verdict consults this to prove the endpoint still retains the
    /// attempt's slot range.
    pub async fn get_first_available_block(&self) -> Result<u64, Box<client_error::Error>> {
        self.with_retry(
            "get_first_available_block",
            RetryPolicy::Idempotent,
            || async { self.rpc_client.get_first_available_block().await },
        )
        .await
    }

    /// Get the cluster genesis hash with retry. Used once at withdraw startup to
    /// prove the fallback endpoint is on the same cluster as the primary.
    pub async fn get_genesis_hash(&self) -> Result<Hash, Box<client_error::Error>> {
        self.with_retry("get_genesis_hash", RetryPolicy::Idempotent, || async {
            self.rpc_client.get_genesis_hash().await
        })
        .await
    }

    /// Send transaction with configurable retry policy
    ///
    /// # Arguments
    /// * `transaction` - The transaction to send
    /// * `retry_policy` - Controls retry behavior:
    ///   - `RetryPolicy::None`: Single attempt, no retry (for non-idempotent operations)
    ///   - `RetryPolicy::Idempotent`: Retry with exponential backoff (for idempotent operations)
    ///
    /// # Safety
    /// For operations that can duplicate side effects (for example mint sends), use
    /// `RetryPolicy::None` at send time and add an external idempotency check before resubmission.
    /// Only use retry for operations that are safe to execute multiple times.
    pub async fn send_transaction(
        &self,
        transaction: &solana_sdk::transaction::Transaction,
        retry_policy: RetryPolicy,
    ) -> Result<solana_sdk::signature::Signature, Box<client_error::Error>> {
        self.with_retry("send_transaction", retry_policy, || async {
            self.rpc_client.send_transaction(transaction).await
        })
        .await
    }

    /// Get account with retry
    pub async fn get_account_data(
        &self,
        pubkey: &Pubkey,
    ) -> Result<Vec<u8>, Box<client_error::Error>> {
        self.with_retry("get_account_info", RetryPolicy::Idempotent, || async {
            self.rpc_client.get_account_data(pubkey).await
        })
        .await
    }

    /// Get account with retry
    pub async fn get_account(&self, pubkey: &Pubkey) -> Result<Account, Box<client_error::Error>> {
        self.with_retry("get_account", RetryPolicy::Idempotent, || async {
            self.rpc_client.get_account(pubkey).await
        })
        .await
    }

    /// Read an account at the given commitment, returning the response context
    /// (its `slot`) with the account. An absent account is `Ok(None)`, kept
    /// distinct from a read error (`Err`).
    pub async fn get_account_with_context(
        &self,
        pubkey: &Pubkey,
        commitment: CommitmentConfig,
    ) -> Result<Response<Option<Account>>, Box<client_error::Error>> {
        self.with_retry(
            "get_account_with_context",
            RetryPolicy::Idempotent,
            || async {
                self.rpc_client
                    .get_account_with_commitment(pubkey, commitment)
                    .await
            },
        )
        .await
    }

    /// Like `get_account_with_context`, but requires the node to answer from a
    /// snapshot whose context slot is at least `min_context_slot`. If the node
    /// cannot serve at that slot (a lagging or load-balanced backend) it returns
    /// an RPC error rather than an older snapshot, letting the caller fail closed.
    /// This binds the returned account to a slot the caller has already proven fresh.
    pub async fn get_account_with_context_min_slot(
        &self,
        pubkey: &Pubkey,
        commitment: CommitmentConfig,
        min_context_slot: Option<u64>,
    ) -> Result<Response<Option<Account>>, Box<client_error::Error>> {
        self.with_retry(
            "get_account_with_context_min_slot",
            RetryPolicy::Idempotent,
            || async {
                let config = RpcAccountInfoConfig {
                    encoding: Some(UiAccountEncoding::Base64),
                    data_slice: None,
                    commitment: Some(commitment),
                    min_context_slot,
                };
                self.rpc_client
                    .get_account_with_config(pubkey, config)
                    .await
            },
        )
        .await
    }

    /// Get token account balance with retry (read-only, safe to retry)
    pub async fn get_token_account_balance(
        &self,
        pubkey: &Pubkey,
    ) -> Result<solana_account_decoder_client_types::token::UiTokenAmount, Box<client_error::Error>>
    {
        self.with_retry(
            "get_token_account_balance",
            RetryPolicy::Idempotent,
            || async { self.rpc_client.get_token_account_balance(pubkey).await },
        )
        .await
    }

    /// Get signature statuses with retry (read-only, always safe to retry)
    pub async fn get_signature_statuses(
        &self,
        signatures: &[Signature],
    ) -> Result<
        Response<Vec<Option<solana_transaction_status::TransactionStatus>>>,
        Box<client_error::Error>,
    > {
        self.with_retry(
            "get_signature_statuses",
            RetryPolicy::Idempotent,
            || async { self.rpc_client.get_signature_statuses(signatures).await },
        )
        .await
    }

    /// Like `get_signature_statuses`, but sets `searchTransactionHistory: true` so the
    /// validator consults long-term ledger storage when the recent status cache misses.
    /// Use for authoritative finality checks where the signature may have aged out of
    /// cache (e.g. recovery after operator downtime); an `Ok` response with all `None`
    /// means the signature is genuinely not on-chain.
    pub async fn get_signature_statuses_with_history(
        &self,
        signatures: &[Signature],
    ) -> Result<
        Response<Vec<Option<solana_transaction_status::TransactionStatus>>>,
        Box<client_error::Error>,
    > {
        self.with_retry(
            "get_signature_statuses_with_history",
            RetryPolicy::Idempotent,
            || async {
                self.rpc_client
                    .get_signature_statuses_with_history(signatures)
                    .await
            },
        )
        .await
    }

    /// Get recent signatures that touched an address (read-only, safe to retry)
    pub async fn get_signatures_for_address(
        &self,
        address: &Pubkey,
        limit: usize,
    ) -> Result<
        Vec<solana_rpc_client_api::response::RpcConfirmedTransactionStatusWithSignature>,
        Box<client_error::Error>,
    > {
        self.with_retry(
            "get_signatures_for_address",
            RetryPolicy::Idempotent,
            || async {
                let config = GetConfirmedSignaturesForAddress2Config {
                    before: None,
                    until: None,
                    limit: Some(limit),
                    commitment: Some(CommitmentConfig::confirmed()),
                };

                self.rpc_client
                    .get_signatures_for_address_with_config(address, config)
                    .await
            },
        )
        .await
    }

    /// Page `getSignaturesForAddress` to completeness via the `before` cursor.
    ///
    /// The bounded `get_signatures_for_address` is a fixed-window lookback; this walks
    /// the entire signature history for an address, oldest page last, so a consumed-set
    /// enumeration cannot miss a mint that sits beyond the first window. Any page error
    /// propagates as `Err` so the caller can fail closed rather than treat a partial
    /// history as complete.
    pub async fn get_signatures_for_address_paginated(
        &self,
        address: &Pubkey,
        page_limit: usize,
    ) -> Result<
        Vec<solana_rpc_client_api::response::RpcConfirmedTransactionStatusWithSignature>,
        Box<client_error::Error>,
    > {
        let mut all = Vec::new();
        let mut before: Option<Signature> = None;
        loop {
            let page = self
                .with_retry(
                    "get_signatures_for_address_paginated",
                    RetryPolicy::Idempotent,
                    || async {
                        let config = GetConfirmedSignaturesForAddress2Config {
                            before,
                            until: None,
                            limit: Some(page_limit),
                            commitment: Some(CommitmentConfig::confirmed()),
                        };
                        self.rpc_client
                            .get_signatures_for_address_with_config(address, config)
                            .await
                    },
                )
                .await?;

            let page_len = page.len();
            // Capture the oldest signature on this page; it becomes the next page's cursor.
            let last_signature = page.last().map(|s| s.signature.clone());
            all.extend(page);

            // A short page is the only legitimate end of history.
            if page_len < page_limit {
                break;
            }

            // Full page: we must advance the cursor to keep walking. If the last signature
            // does not parse we cannot guarantee completeness, so fail closed instead of
            // silently returning a truncated history.
            match last_signature
                .as_deref()
                .and_then(|s| Signature::from_str(s).ok())
            {
                Some(sig) => before = Some(sig),
                None => {
                    return Err(Box::new(client_error::Error::from(ErrorKind::Custom(
                        format!(
                            "get_signatures_for_address_paginated: full page for {address} but \
                             last cursor signature failed to parse; cannot guarantee completeness"
                        ),
                    ))));
                }
            }
        }
        Ok(all)
    }

    /// Get a confirmed transaction in JSON-parsed encoding (read-only, safe to retry)
    pub async fn get_transaction(
        &self,
        signature: &Signature,
    ) -> Result<
        solana_transaction_status::EncodedConfirmedTransactionWithStatusMeta,
        Box<client_error::Error>,
    > {
        let config = RpcTransactionConfig {
            encoding: Some(solana_transaction_status::UiTransactionEncoding::JsonParsed),
            commitment: Some(CommitmentConfig::confirmed()),
            max_supported_transaction_version: Some(0),
        };

        self.with_retry("get_transaction", RetryPolicy::Idempotent, || async {
            self.rpc_client
                .get_transaction_with_config(signature, config)
                .await
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[tokio::test]
    async fn with_retry_none_policy_single_attempt() {
        let client = RpcClientWithRetry::with_retry_config(
            "http://localhost:8899".to_string(),
            RetryConfig::default(),
            CommitmentConfig::confirmed(),
        );
        let call_count = Arc::new(AtomicU32::new(0));
        let cc = call_count.clone();

        let result: Result<u32, Box<client_error::Error>> = client
            .with_retry("test_op", RetryPolicy::None, || {
                let cc = cc.clone();
                async move {
                    cc.fetch_add(1, Ordering::SeqCst);
                    Ok::<u32, client_error::Error>(42)
                }
            })
            .await;

        assert_eq!(result.unwrap(), 42);
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn with_retry_none_policy_propagates_error() {
        let client = RpcClientWithRetry::with_retry_config(
            "http://localhost:8899".to_string(),
            RetryConfig::default(),
            CommitmentConfig::confirmed(),
        );

        let result: Result<u32, Box<client_error::Error>> = client
            .with_retry("test_op", RetryPolicy::None, || async {
                Err::<u32, Box<client_error::Error>>(Box::new(
                    client_error::Error::new_with_request(
                        client_error::ErrorKind::Custom("test error".to_string()),
                        solana_rpc_client_api::request::RpcRequest::GetBalance,
                    ),
                ))
            })
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn with_retry_idempotent_succeeds_on_second_try() {
        let client = RpcClientWithRetry::with_retry_config(
            "http://localhost:8899".to_string(),
            RetryConfig {
                max_attempts: 3,
                base_delay: Duration::from_millis(1), // fast for tests
                max_delay: Duration::from_millis(10),
            },
            CommitmentConfig::confirmed(),
        );
        let call_count = Arc::new(AtomicU32::new(0));
        let cc = call_count.clone();

        let result: Result<u32, Box<client_error::Error>> = client
            .with_retry("test_op", RetryPolicy::Idempotent, || {
                let cc = cc.clone();
                async move {
                    let count = cc.fetch_add(1, Ordering::SeqCst);
                    if count == 0 {
                        Err::<u32, Box<client_error::Error>>(Box::new(
                            client_error::Error::new_with_request(
                                client_error::ErrorKind::Custom("transient".to_string()),
                                solana_rpc_client_api::request::RpcRequest::GetBalance,
                            ),
                        ))
                    } else {
                        Ok(99)
                    }
                }
            })
            .await;

        assert_eq!(result.unwrap(), 99);
        assert_eq!(call_count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn with_retry_idempotent_exhausts_attempts() {
        let client = RpcClientWithRetry::with_retry_config(
            "http://localhost:8899".to_string(),
            RetryConfig {
                max_attempts: 2,
                base_delay: Duration::from_millis(1),
                max_delay: Duration::from_millis(10),
            },
            CommitmentConfig::confirmed(),
        );
        let call_count = Arc::new(AtomicU32::new(0));
        let cc = call_count.clone();

        let result: Result<u32, Box<client_error::Error>> = client
            .with_retry("test_op", RetryPolicy::Idempotent, || {
                let cc = cc.clone();
                async move {
                    cc.fetch_add(1, Ordering::SeqCst);
                    Err::<u32, Box<client_error::Error>>(Box::new(
                        client_error::Error::new_with_request(
                            client_error::ErrorKind::Custom("always fail".to_string()),
                            solana_rpc_client_api::request::RpcRequest::GetBalance,
                        ),
                    ))
                }
            })
            .await;

        assert!(result.is_err());
        assert_eq!(call_count.load(Ordering::SeqCst), 2);
    }

    /// Even when base_delay is huge (100s), max_delay (1ms) acts as a hard ceiling so each
    /// inter-attempt pause is clamped to 1ms, keeping wall-clock time well under 1 second.
    #[tokio::test]
    async fn with_retry_backoff_clamped_to_max_delay() {
        let client = RpcClientWithRetry::with_retry_config(
            "http://localhost:8899".to_string(),
            RetryConfig {
                max_attempts: 10,
                base_delay: Duration::from_secs(100), // very large base delay
                max_delay: Duration::from_millis(1),  // tiny max delay
            },
            CommitmentConfig::confirmed(),
        );
        let call_count = Arc::new(AtomicU32::new(0));
        let cc = call_count.clone();

        let start = std::time::Instant::now();
        let _: Result<u32, Box<client_error::Error>> = client
            .with_retry("test_op", RetryPolicy::Idempotent, || {
                let cc = cc.clone();
                async move {
                    let count = cc.fetch_add(1, Ordering::SeqCst);
                    if count < 2 {
                        Err::<u32, Box<client_error::Error>>(Box::new(
                            client_error::Error::new_with_request(
                                client_error::ErrorKind::Custom("fail".to_string()),
                                solana_rpc_client_api::request::RpcRequest::GetBalance,
                            ),
                        ))
                    } else {
                        Ok(1)
                    }
                }
            })
            .await;

        // Should complete quickly because max_delay clamps the large base_delay
        assert!(start.elapsed() < Duration::from_secs(1));
        assert_eq!(call_count.load(Ordering::SeqCst), 3);
    }

    fn make_client_fast() -> RpcClientWithRetry {
        RpcClientWithRetry::with_retry_config(
            "http://localhost:8899".to_string(),
            RetryConfig {
                max_attempts: 5,
                base_delay: Duration::from_millis(1),
                max_delay: Duration::from_millis(1),
            },
            CommitmentConfig::confirmed(),
        )
    }

    fn rpc_method_not_found() -> client_error::Error {
        client_error::Error::new_with_request(
            client_error::ErrorKind::RpcError(RpcError::RpcResponseError {
                code: -32601,
                message: "Method not found".to_string(),
                data: solana_rpc_client_api::request::RpcResponseErrorData::Empty,
            }),
            solana_rpc_client_api::request::RpcRequest::GetBalance,
        )
    }

    fn rpc_account_not_found() -> client_error::Error {
        client_error::Error::new_with_request(
            client_error::ErrorKind::RpcError(RpcError::ForUser(
                "AccountNotFound: pubkey=So11111111111111111111111111111111111111112".to_string(),
            )),
            solana_rpc_client_api::request::RpcRequest::GetAccountInfo,
        )
    }

    fn rpc_transient() -> client_error::Error {
        client_error::Error::new_with_request(
            client_error::ErrorKind::RpcError(RpcError::ForUser("NodeUnhealthy".to_string())),
            solana_rpc_client_api::request::RpcRequest::GetBalance,
        )
    }

    /// -32601 (Method not found) must abort on the first attempt — no retries.
    #[tokio::test]
    async fn permanent_error_method_not_found_stops_immediately() {
        let client = make_client_fast();
        let call_count = Arc::new(AtomicU32::new(0));
        let cc = call_count.clone();

        let result: Result<u32, Box<client_error::Error>> = client
            .with_retry("op", RetryPolicy::Idempotent, || {
                let cc = cc.clone();
                async move {
                    cc.fetch_add(1, Ordering::SeqCst);
                    Err::<u32, client_error::Error>(rpc_method_not_found())
                }
            })
            .await;

        assert!(result.is_err());
        assert_eq!(
            call_count.load(Ordering::SeqCst),
            1,
            "-32601 must not be retried"
        );
    }

    /// AccountNotFound is a definitive answer — must abort on the first attempt.
    #[tokio::test]
    async fn permanent_error_account_not_found_stops_immediately() {
        let client = make_client_fast();
        let call_count = Arc::new(AtomicU32::new(0));
        let cc = call_count.clone();

        let result: Result<u32, Box<client_error::Error>> = client
            .with_retry("op", RetryPolicy::Idempotent, || {
                let cc = cc.clone();
                async move {
                    cc.fetch_add(1, Ordering::SeqCst);
                    Err::<u32, client_error::Error>(rpc_account_not_found())
                }
            })
            .await;

        assert!(result.is_err());
        assert_eq!(
            call_count.load(Ordering::SeqCst),
            1,
            "AccountNotFound must not be retried"
        );
    }

    /// Transient RPC errors (e.g. NodeUnhealthy) must be retried up to max_attempts.
    #[tokio::test]
    async fn transient_rpc_error_is_retried() {
        let client = make_client_fast();
        let call_count = Arc::new(AtomicU32::new(0));
        let cc = call_count.clone();

        let result: Result<u32, Box<client_error::Error>> = client
            .with_retry("op", RetryPolicy::Idempotent, || {
                let cc = cc.clone();
                async move {
                    cc.fetch_add(1, Ordering::SeqCst);
                    Err::<u32, client_error::Error>(rpc_transient())
                }
            })
            .await;

        assert!(result.is_err());
        assert_eq!(
            call_count.load(Ordering::SeqCst),
            5,
            "transient error must be retried to max_attempts"
        );
    }

    /// ForUser message that mentions AccountNotFound only as a substring of a larger word
    /// must NOT be treated as permanent — only exact "AccountNotFound" prefix matches.
    #[tokio::test]
    async fn for_user_error_unrelated_message_is_retried() {
        let client = make_client_fast();
        let call_count = Arc::new(AtomicU32::new(0));
        let cc = call_count.clone();

        // Message does not contain "AccountNotFound"
        let result: Result<u32, Box<client_error::Error>> = client
            .with_retry("op", RetryPolicy::Idempotent, || {
                let cc = cc.clone();
                async move {
                    cc.fetch_add(1, Ordering::SeqCst);
                    Err::<u32, client_error::Error>(client_error::Error::new_with_request(
                        client_error::ErrorKind::RpcError(RpcError::ForUser(
                            "BlockNotFound".to_string(),
                        )),
                        solana_rpc_client_api::request::RpcRequest::GetBalance,
                    ))
                }
            })
            .await;

        assert!(result.is_err());
        assert_eq!(
            call_count.load(Ordering::SeqCst),
            5,
            "unrelated ForUser error must be retried"
        );
    }

    fn make_client_at(url: &str) -> RpcClientWithRetry {
        RpcClientWithRetry::with_retry_config(
            url.to_string(),
            RetryConfig {
                max_attempts: 1,
                base_delay: Duration::from_millis(1),
                max_delay: Duration::from_millis(1),
            },
            CommitmentConfig::confirmed(),
        )
    }

    #[tokio::test]
    async fn get_first_available_block_success() {
        let mut server = mockito::Server::new_async().await;
        let _m = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getFirstAvailableBlock""#.into(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"jsonrpc":"2.0","result":12345,"id":0}"#)
            .create_async()
            .await;
        let client = make_client_at(&server.url());
        assert_eq!(client.get_first_available_block().await.unwrap(), 12345);
    }

    #[tokio::test]
    async fn get_first_available_block_rpc_error() {
        let mut server = mockito::Server::new_async().await;
        let _m = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getFirstAvailableBlock""#.into(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{"jsonrpc":"2.0","error":{"code":-32000,"message":"node unavailable"},"id":0}"#,
            )
            .create_async()
            .await;
        let client = make_client_at(&server.url());
        assert!(client.get_first_available_block().await.is_err());
    }

    /// R1: a present account is returned as `Some`, and the response context
    /// exposes the finalized snapshot slot.
    #[tokio::test]
    async fn get_account_with_context_some_exposes_slot() {
        let mut server = mockito::Server::new_async().await;
        let _m = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getAccountInfo""#.into(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{"jsonrpc":"2.0","result":{"context":{"slot":4242},"value":{"owner":"11111111111111111111111111111111","lamports":1000000,"data":["","base64"],"executable":false,"rentEpoch":0}},"id":0}"#,
            )
            .create_async()
            .await;
        let client = make_client_at(&server.url());
        let resp = client
            .get_account_with_context(&Pubkey::default(), CommitmentConfig::finalized())
            .await
            .unwrap();
        assert_eq!(resp.context.slot, 4242);
        assert!(resp.value.is_some());
    }

    /// R2: an absent account (`value: null`) is `Ok(None)`, kept distinct from a
    /// read error so the caller reads absence as uncertainty, never non-inclusion.
    #[tokio::test]
    async fn get_account_with_context_null_is_none() {
        let mut server = mockito::Server::new_async().await;
        let _m = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getAccountInfo""#.into(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"jsonrpc":"2.0","result":{"context":{"slot":7},"value":null},"id":0}"#)
            .create_async()
            .await;
        let client = make_client_at(&server.url());
        let resp = client
            .get_account_with_context(&Pubkey::default(), CommitmentConfig::finalized())
            .await
            .unwrap();
        assert!(resp.value.is_none());
    }

    #[tokio::test]
    async fn get_genesis_hash_success() {
        let mut server = mockito::Server::new_async().await;
        let _m = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getGenesisHash""#.into(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"jsonrpc":"2.0","result":"11111111111111111111111111111111","id":0}"#)
            .create_async()
            .await;
        let client = make_client_at(&server.url());
        let hash = client.get_genesis_hash().await.unwrap();
        assert_eq!(hash, Hash::default());
    }

    #[tokio::test]
    async fn get_genesis_hash_rpc_error() {
        let mut server = mockito::Server::new_async().await;
        let _m = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getGenesisHash""#.into(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{"jsonrpc":"2.0","error":{"code":-32000,"message":"node unavailable"},"id":0}"#,
            )
            .create_async()
            .await;
        let client = make_client_at(&server.url());
        assert!(client.get_genesis_hash().await.is_err());
    }
}
