use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_client::GetConfirmedSignaturesForAddress2Config;
use solana_rpc_client_api::client_error;
use solana_rpc_client_api::client_error::ErrorKind;
use solana_rpc_client_api::config::RpcTransactionConfig;
use solana_rpc_client_api::request::RpcError;
use solana_sdk::account::Account;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::hash::Hash;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
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
        solana_client::rpc_response::Response<
            Vec<Option<solana_transaction_status::TransactionStatus>>,
        >,
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
        solana_client::rpc_response::Response<
            Vec<Option<solana_transaction_status::TransactionStatus>>,
        >,
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
}
