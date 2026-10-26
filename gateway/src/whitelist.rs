//! Opt-in check that refuses a public `sendTransaction` whose `WithdrawFunds` the
//! escrow would refuse on Mainnet, before the channel burns anything. ReleaseFunds
//! stays the guarantee, so anything this cannot read is forwarded.

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use governor::{DefaultDirectRateLimiter, Quota, RateLimiter};
use http_body_util::{BodyExt, Full, Limited};
use hyper::body::Bytes;
use hyper::{Request, Uri};
use hyper_util::client::legacy::{connect::HttpConnector, Client};
use private_channel_escrow_program_client::{
    accounts::AllowedWallet, decode_instance, PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
};
use private_channel_withdraw_program_client::{
    instructions::WITHDRAW_FUNDS_DISCRIMINATOR, PRIVATE_CHANNEL_WITHDRAW_PROGRAM_ID,
};
use serde_json::Value;
use solana_pubkey::Pubkey;
use solana_sdk::message::v1::MAX_TRANSACTION_SIZE;
use solana_sdk::transaction::VersionedTransaction;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Mutex;
use std::time::Duration;
use tokio::time::Instant;

/// Longest a public withdrawal waits on Mainnet before it is forwarded unchecked.
const CHECK_TIMEOUT: Duration = Duration::from_secs(2);

/// How long a Mainnet read answers later checks, so an admin change reaches the check
/// up to this late. The escrow still checks every release.
const CACHE_TTL: Duration = Duration::from_secs(5);

/// Most accounts cached. Expired reads are dropped on each insert; past the cap, a read
/// is used but not cached.
const MAX_CACHED_ACCOUNTS: usize = 10_000;

/// Seeds of an escrow AllowedWallet PDA: `[ALLOWED_WALLET_SEED, instance, wallet]`.
const ALLOWED_WALLET_SEED: &[u8] = b"allowed_wallet";

/// Largest `getMultipleAccounts` answer read. A handful of small accounts fits many times.
const MAX_RESPONSE_BYTES: usize = 64 * 1024;

/// `getMultipleAccounts` takes at most 100 keys: the instance plus 99 entries.
const MAX_WALLETS: usize = 99;

/// The bound the write node's decoder sets (`core/src/rpc/decode.rs`): wincode reserves
/// from a declared count, so a few bytes could otherwise reserve its 4 MiB default.
const PREALLOCATION_LIMIT: usize = MAX_TRANSACTION_SIZE * 64;

type DecodeConfig = wincode::config::Configuration<true, PREALLOCATION_LIMIT>;

/// An account as `getMultipleAccounts` returned it.
#[derive(Clone, Debug)]
struct FetchedAccount {
    owner: Pubkey,
    data: Vec<u8>,
}

/// Mainnet reads by address, each with when it was read. None is an account that
/// does not exist.
type AccountCache = HashMap<Pubkey, (Instant, Option<FetchedAccount>)>;

#[derive(Debug, PartialEq)]
pub enum Verdict {
    Allowed,
    Refused(Pubkey),
    /// The check could not run, so the transaction is forwarded.
    Unchecked(String),
}

pub struct WithdrawWhitelist {
    rpc_uri: Uri,
    instance: Pubkey,
    cache: Mutex<AccountCache>,
    /// Mainnet calls across all clients, so fresh wallets from many addresses cannot
    /// multiply them.
    reads: DefaultDirectRateLimiter,
}

impl WithdrawWhitelist {
    /// Both settings set turns the check on, and both unset or blank leaves it off.
    /// Only one set is an error, so a half-configured gateway never runs without it.
    pub fn from_config(
        rpc_url: Option<&str>,
        instance: Option<&str>,
        reads: Quota,
    ) -> Result<Option<Self>, String> {
        let rpc_url = rpc_url.map(str::trim).filter(|value| !value.is_empty());
        let instance = instance.map(str::trim).filter(|value| !value.is_empty());
        match (rpc_url, instance) {
            (None, None) => Ok(None),
            (Some(rpc_url), Some(instance)) => Ok(Some(Self {
                rpc_uri: rpc_url
                    .parse()
                    .map_err(|e| format!("GATEWAY_WHITELIST_RPC_URL is not a valid URL: {e}"))?,
                instance: Pubkey::from_str(instance).map_err(|e| {
                    format!("GATEWAY_WHITELIST_INSTANCE is not a valid pubkey: {e}")
                })?,
                cache: Mutex::new(HashMap::new()),
                reads: RateLimiter::direct(reads),
            })),
            _ => Err(
                "GATEWAY_WHITELIST_RPC_URL and GATEWAY_WHITELIST_INSTANCE must be set together"
                    .to_string(),
            ),
        }
    }

    /// Whether the escrow would release the withdrawals in a `sendTransaction`'s params.
    /// A send with no withdrawal costs no RPC call.
    pub async fn check(
        &self,
        client: &Client<hyper_rustls::HttpsConnector<HttpConnector>, Full<Bytes>>,
        params: &Value,
        deadline: Instant,
    ) -> Verdict {
        let wallets = withdraw_wallets(params);
        if wallets.is_empty() {
            return Verdict::Allowed;
        }
        if wallets.len() > MAX_WALLETS {
            return Verdict::Unchecked("too many withdraw wallets to check".to_string());
        }

        // While the cached instance does not whitelist withdrawals, no entry is read.
        if let Some(instance) = self.cached(&self.instance) {
            if whitelists_withdrawals(&instance) != Some(true) {
                return decide(&wallets, &[instance]);
            }
        }

        let mut keys = vec![self.instance];
        for wallet in &wallets {
            let (entry, _) = Pubkey::find_program_address(
                &[ALLOWED_WALLET_SEED, self.instance.as_ref(), wallet.as_ref()],
                &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
            );
            keys.push(entry);
        }

        let check_deadline = deadline.min(Instant::now() + CHECK_TIMEOUT);
        match tokio::time::timeout_at(check_deadline, self.fetch_cached(client, &keys)).await {
            Ok(Ok(accounts)) => decide(&wallets, &accounts),
            Ok(Err(reason)) => Verdict::Unchecked(reason),
            Err(_) => Verdict::Unchecked("Mainnet RPC did not answer in time".to_string()),
        }
    }

    /// `key`'s account while its cached read is younger than CACHE_TTL.
    fn cached(&self, key: &Pubkey) -> Option<Option<FetchedAccount>> {
        let cache = self.cache.lock().unwrap();
        let (read_at, account) = cache.get(key)?;
        (read_at.elapsed() < CACHE_TTL).then(|| account.clone())
    }

    /// Caches `accounts`, read at `read_at` from `keys`.
    fn remember(&self, read_at: Instant, keys: &[Pubkey], accounts: &[Option<FetchedAccount>]) {
        let mut cache = self.cache.lock().unwrap();
        // Drop expired reads, so the map holds only the last CACHE_TTL of them.
        cache.retain(|_, (cached_at, _)| cached_at.elapsed() < CACHE_TTL);
        // Every entry left is fresh: rather than drop them, this read goes uncached.
        if cache.len() + keys.len() > MAX_CACHED_ACCOUNTS {
            return;
        }
        for (key, account) in keys.iter().zip(accounts) {
            cache.insert(*key, (read_at, account.clone()));
        }
    }

    /// The accounts at `keys`, from the cache where fresh and the rest in one Mainnet
    /// call within the read budget. A failed or refused read caches nothing.
    async fn fetch_cached(
        &self,
        client: &Client<hyper_rustls::HttpsConnector<HttpConnector>, Full<Bytes>>,
        keys: &[Pubkey],
    ) -> Result<Vec<Option<FetchedAccount>>, String> {
        let read_at = Instant::now();
        let cached: Vec<Option<Option<FetchedAccount>>> =
            keys.iter().map(|key| self.cached(key)).collect();
        let missing: Vec<Pubkey> = keys
            .iter()
            .zip(&cached)
            .filter(|(_, account)| account.is_none())
            .map(|(key, _)| *key)
            .collect();
        let fetched = if missing.is_empty() {
            Vec::new()
        } else {
            self.reads
                .check()
                .map_err(|_| "Mainnet reads over GATEWAY_WHITELIST_READS_PER_SECOND".to_string())?;
            let fetched = self.fetch(client, &missing).await?;
            self.remember(read_at, &missing, &fetched);
            fetched
        };

        let mut fetched = fetched.into_iter();
        Ok(cached
            .into_iter()
            .map(|account| account.unwrap_or_else(|| fetched.next().flatten()))
            .collect())
    }

    /// The accounts at `keys`, in one `getMultipleAccounts` at confirmed.
    async fn fetch(
        &self,
        client: &Client<hyper_rustls::HttpsConnector<HttpConnector>, Full<Bytes>>,
        keys: &[Pubkey],
    ) -> Result<Vec<Option<FetchedAccount>>, String> {
        let keys: Vec<String> = keys.iter().map(Pubkey::to_string).collect();

        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getMultipleAccounts",
            "params": [keys, { "encoding": "base64", "commitment": "confirmed" }],
        });
        let request = Request::post(self.rpc_uri.clone())
            .header("Content-Type", "application/json")
            .body(Full::new(Bytes::from(body.to_string())))
            .map_err(|e| format!("could not build the Mainnet RPC request: {e}"))?;
        let response = client
            .request(request)
            .await
            .map_err(|e| format!("Mainnet RPC request failed: {e}"))?;
        if !response.status().is_success() {
            return Err(format!("Mainnet RPC answered {}", response.status()));
        }
        let bytes = Limited::new(response.into_body(), MAX_RESPONSE_BYTES)
            .collect()
            .await
            .map_err(|e| format!("could not read the Mainnet RPC answer: {e}"))?
            .to_bytes();

        let answer: Value = serde_json::from_slice(&bytes)
            .map_err(|e| format!("Mainnet RPC answer is not JSON: {e}"))?;
        let Some(values) = answer.pointer("/result/value").and_then(Value::as_array) else {
            return Err(format!(
                "Mainnet RPC returned no accounts: {}",
                answer.get("error").unwrap_or(&Value::Null)
            ));
        };
        if values.len() != keys.len() {
            return Err(format!(
                "Mainnet RPC returned {} accounts for {} keys",
                values.len(),
                keys.len()
            ));
        }

        let mut accounts = Vec::with_capacity(values.len());
        for value in values {
            if value.is_null() {
                accounts.push(None);
                continue;
            }
            let owner = value
                .get("owner")
                .and_then(Value::as_str)
                .and_then(|owner| Pubkey::from_str(owner).ok());
            let data = value
                .pointer("/data/0")
                .and_then(Value::as_str)
                .and_then(|data| BASE64.decode(data).ok());
            let (Some(owner), Some(data)) = (owner, data) else {
                return Err(format!(
                    "Mainnet RPC returned an unreadable account: {value}"
                ));
            };
            accounts.push(Some(FetchedAccount { owner, data }));
        }
        Ok(accounts)
    }
}

/// 403 body for a withdrawal the escrow would refuse.
pub fn wallet_not_allowed_body(wallet: &Pubkey) -> Bytes {
    Bytes::from(
        serde_json::json!({
            "error": {
                "code": -32006,
                "message": format!("Forbidden: {wallet} is not allowed to withdraw from this channel"),
            }
        })
        .to_string(),
    )
}

/// The withdrawer, then the destination, of every top-level WithdrawFunds, without
/// repeats: the order ReleaseFunds checks them in. Empty for anything the write node
/// refuses on its own: larger than a v1 transaction, or not decodable. No program the
/// channel admits calls WithdrawFunds by CPI; one that did would skip this check.
fn withdraw_wallets(params: &Value) -> Vec<Pubkey> {
    let Some(bytes) = params
        .get(0)
        .and_then(Value::as_str)
        .and_then(|encoded| BASE64.decode(encoded).ok())
    else {
        return Vec::new();
    };
    if bytes.len() > MAX_TRANSACTION_SIZE {
        return Vec::new();
    }
    let Ok(transaction) =
        wincode::config::deserialize::<VersionedTransaction, _>(&bytes, DecodeConfig::new())
    else {
        return Vec::new();
    };

    let keys = transaction.message.static_account_keys();
    let mut wallets = Vec::new();
    for instruction in transaction.message.instructions() {
        let program = keys.get(usize::from(instruction.program_id_index));
        if program != Some(&PRIVATE_CHANNEL_WITHDRAW_PROGRAM_ID)
            || instruction.data.first() != Some(&WITHDRAW_FUNDS_DISCRIMINATOR)
        {
            continue;
        }
        let Some(withdrawer) = instruction
            .accounts
            .first()
            .and_then(|index| keys.get(usize::from(*index)))
        else {
            continue;
        };

        // Discriminator (1) and amount (8), then an optional destination: tag 1 and 32 bytes.
        let destination = match instruction.data.get(9) {
            Some(1) => instruction
                .data
                .get(10..42)
                .and_then(|bytes| Pubkey::try_from(bytes).ok()),
            _ => None,
        };
        for wallet in [*withdrawer, destination.unwrap_or(*withdrawer)] {
            if !wallets.contains(&wallet) {
                wallets.push(wallet);
            }
        }
    }
    wallets
}

/// Whether `instance` whitelists withdrawals, or None when it is not an escrow instance.
fn whitelists_withdrawals(instance: &Option<FetchedAccount>) -> Option<bool> {
    instance
        .as_ref()
        .filter(|account| account.owner == PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .and_then(|account| decode_instance(&account.data).ok())
        .map(|instance| instance.withdrawals_whitelisted)
}

/// ReleaseFunds' rule while withdrawals are whitelisted: every wallet needs an entry
/// that does not block withdrawals. `accounts` is the instance, then one entry per
/// wallet. Anything the escrow could read differently is left to it.
fn decide(wallets: &[Pubkey], accounts: &[Option<FetchedAccount>]) -> Verdict {
    let Some((instance_account, entries)) = accounts.split_first() else {
        return Verdict::Unchecked("no instance account returned".to_string());
    };
    let Some(whitelisted) = whitelists_withdrawals(instance_account) else {
        return Verdict::Unchecked("the configured instance is not an escrow instance".to_string());
    };
    if !whitelisted {
        return Verdict::Allowed;
    }

    for (wallet, entry) in wallets.iter().zip(entries) {
        // The escrow reads an empty account at the entry address as no entry.
        let Some(entry) = entry.as_ref().filter(|entry| !entry.data.is_empty()) else {
            return Verdict::Refused(*wallet);
        };
        if entry.owner != PRIVATE_CHANNEL_ESCROW_PROGRAM_ID {
            return Verdict::Unchecked(format!(
                "the whitelist entry of {wallet} is not owned by the escrow"
            ));
        }
        match AllowedWallet::from_bytes(&entry.data) {
            Ok(allowed_wallet) if allowed_wallet.withdrawals_blocked => {
                return Verdict::Refused(*wallet)
            }
            Ok(_) => {}
            Err(_) => {
                return Verdict::Unchecked(format!(
                    "the whitelist entry of {wallet} does not decode"
                ))
            }
        }
    }
    Verdict::Allowed
}

#[cfg(test)]
mod tests {
    use super::*;
    use private_channel_escrow_program_client::{
        INSTANCE_V1_LEN, PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
    };
    use private_channel_withdraw_program_client::{
        instructions::{
            WithdrawFundsBuilder, SET_WITHDRAW_CONFIG_DISCRIMINATOR, WITHDRAW_FUNDS_DISCRIMINATOR,
        },
        PRIVATE_CHANNEL_WITHDRAW_PROGRAM_ID,
    };
    use serde_json::json;
    use solana_sdk::{
        hash::Hash,
        instruction::{AccountMeta, Instruction},
        signature::{Keypair, Signer},
        transaction::{Transaction, VersionedTransaction},
    };
    use std::num::NonZeroU32;

    /// A WithdrawFunds by `withdrawer`. Only the withdrawer and destination are read here.
    fn withdraw_funds(withdrawer: &Pubkey, destination: Option<Pubkey>) -> Instruction {
        let mut builder = WithdrawFundsBuilder::new();
        builder
            .user(*withdrawer)
            .mint(Pubkey::new_unique())
            .token_account(Pubkey::new_unique())
            .token_program(Pubkey::new_unique())
            .associated_token_program(Pubkey::new_unique())
            .withdraw_config(Pubkey::new_unique())
            .treasury_token_account(Pubkey::new_unique())
            .amount(1_000);
        if let Some(destination) = destination {
            builder.destination(destination);
        }
        builder.instruction()
    }

    /// `sendTransaction` params as a wallet sends them: the signed transaction in base64.
    fn send_params(instructions: &[Instruction], signers: &[&Keypair]) -> Value {
        let transaction = Transaction::new_signed_with_payer(
            instructions,
            Some(&signers[0].pubkey()),
            signers,
            Hash::default(),
        );
        let bytes = wincode::serialize(&VersionedTransaction::from(transaction))
            .expect("the transaction serializes");
        json!([BASE64.encode(bytes), { "encoding": "base64" }])
    }

    fn escrow_account(data: Vec<u8>) -> Option<FetchedAccount> {
        Some(FetchedAccount {
            owner: PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
            data,
        })
    }

    /// Discriminator, bump, version, seed, admin, the deposit and withdrawal flags, then
    /// the reserved bytes. The deposit flag takes the opposite value, so reading the wrong
    /// byte fails.
    fn instance_data(withdrawals_whitelisted: bool) -> Vec<u8> {
        let mut data = vec![0, 255, 2];
        data.extend_from_slice(&[7; 32]);
        data.extend_from_slice(&[42; 32]);
        data.push(u8::from(!withdrawals_whitelisted));
        data.push(u8::from(withdrawals_whitelisted));
        data.extend_from_slice(&[0; 64]);
        data
    }

    /// Discriminator, bump, the deposit and withdrawal blocks, then the reserved bytes.
    fn allowed_wallet_data(deposits_blocked: bool, withdrawals_blocked: bool) -> Vec<u8> {
        let mut data = vec![
            4,
            255,
            u8::from(deposits_blocked),
            u8::from(withdrawals_blocked),
        ];
        data.extend_from_slice(&[0; 32]);
        data
    }

    #[test]
    fn a_withdrawal_without_destination_checks_only_the_withdrawer() {
        let withdrawer = Keypair::new();
        let params = send_params(
            &[withdraw_funds(&withdrawer.pubkey(), None)],
            &[&withdrawer],
        );

        assert_eq!(withdraw_wallets(&params), vec![withdrawer.pubkey()]);
    }

    #[test]
    fn a_withdrawal_checks_the_withdrawer_then_the_destination() {
        let withdrawer = Keypair::new();
        let destination = Pubkey::new_unique();
        let params = send_params(
            &[withdraw_funds(&withdrawer.pubkey(), Some(destination))],
            &[&withdrawer],
        );

        assert_eq!(
            withdraw_wallets(&params),
            vec![withdrawer.pubkey(), destination]
        );
    }

    // A first withdrawal by a listed wallet must not cover a second one.
    #[test]
    fn every_withdrawal_in_a_transaction_is_checked() {
        let first_withdrawer = Keypair::new();
        let second_withdrawer = Keypair::new();
        let params = send_params(
            &[
                withdraw_funds(&first_withdrawer.pubkey(), None),
                withdraw_funds(&second_withdrawer.pubkey(), None),
            ],
            &[&first_withdrawer, &second_withdrawer],
        );

        assert_eq!(
            withdraw_wallets(&params),
            vec![first_withdrawer.pubkey(), second_withdrawer.pubkey()]
        );
    }

    // The write node refuses a send this large, so none of it is read.
    #[test]
    fn a_send_over_the_size_limit_is_not_read() {
        let withdrawer = Keypair::new();
        let withdrawals: Vec<Instruction> = (0..20)
            .map(|_| withdraw_funds(&withdrawer.pubkey(), Some(Pubkey::new_unique())))
            .collect();
        let params = send_params(&withdrawals, &[&withdrawer]);

        assert!(withdraw_wallets(&params).is_empty());
    }

    #[test]
    fn other_instructions_need_no_check() {
        let payer = Keypair::new();
        let signer = AccountMeta::new(payer.pubkey(), true);
        let other_program = Instruction::new_with_bytes(
            Pubkey::new_unique(),
            &[WITHDRAW_FUNDS_DISCRIMINATOR],
            vec![signer.clone()],
        );
        let set_withdraw_config = Instruction::new_with_bytes(
            PRIVATE_CHANNEL_WITHDRAW_PROGRAM_ID,
            &[SET_WITHDRAW_CONFIG_DISCRIMINATOR],
            vec![signer],
        );
        let params = send_params(&[other_program, set_withdraw_config], &[&payer]);

        assert!(withdraw_wallets(&params).is_empty());
    }

    // Anyone can send this shape, so reading it must not index past its accounts.
    #[test]
    fn a_withdrawal_with_no_accounts_is_skipped() {
        let payer = Keypair::new();
        let no_accounts = Instruction::new_with_bytes(
            PRIVATE_CHANNEL_WITHDRAW_PROGRAM_ID,
            &[WITHDRAW_FUNDS_DISCRIMINATOR],
            vec![],
        );
        let params = send_params(&[no_accounts], &[&payer]);

        assert!(withdraw_wallets(&params).is_empty());
    }

    #[test]
    fn unlisted_wallets_pass_while_withdrawals_are_not_whitelisted() {
        let withdrawer = Pubkey::new_unique();
        let accounts = [escrow_account(instance_data(false)), None];

        assert_eq!(decide(&[withdrawer], &accounts), Verdict::Allowed);
    }

    // A 0.1.0 instance ends at the admin. It has no whitelist, so it passes rather than
    // going unchecked.
    #[test]
    fn unlisted_wallets_pass_a_0_1_0_instance() {
        let withdrawer = Pubkey::new_unique();
        let instance_0_1_0 = instance_data(true)[..INSTANCE_V1_LEN].to_vec();
        let accounts = [escrow_account(instance_0_1_0), None];

        assert_eq!(decide(&[withdrawer], &accounts), Verdict::Allowed);
    }

    #[test]
    fn an_unlisted_withdrawer_is_refused() {
        let withdrawer = Pubkey::new_unique();
        let destination = Pubkey::new_unique();
        let accounts = [
            escrow_account(instance_data(true)),
            None,
            escrow_account(allowed_wallet_data(false, false)),
        ];

        assert_eq!(
            decide(&[withdrawer, destination], &accounts),
            Verdict::Refused(withdrawer)
        );
    }

    #[test]
    fn an_unlisted_destination_is_refused() {
        let withdrawer = Pubkey::new_unique();
        let destination = Pubkey::new_unique();
        let accounts = [
            escrow_account(instance_data(true)),
            escrow_account(allowed_wallet_data(false, false)),
            None,
        ];

        assert_eq!(
            decide(&[withdrawer, destination], &accounts),
            Verdict::Refused(destination)
        );
    }

    #[test]
    fn an_entry_blocking_withdrawals_is_refused() {
        let withdrawer = Pubkey::new_unique();
        let accounts = [
            escrow_account(instance_data(true)),
            escrow_account(allowed_wallet_data(false, true)),
        ];

        assert_eq!(
            decide(&[withdrawer], &accounts),
            Verdict::Refused(withdrawer)
        );
    }

    #[test]
    fn an_entry_blocking_only_deposits_can_withdraw() {
        let withdrawer = Pubkey::new_unique();
        let accounts = [
            escrow_account(instance_data(true)),
            escrow_account(allowed_wallet_data(true, false)),
        ];

        assert_eq!(decide(&[withdrawer], &accounts), Verdict::Allowed);
    }

    // Lamports sent to the entry address leave a system-owned account with no data,
    // which the escrow reads as no entry.
    #[test]
    fn an_empty_account_at_the_entry_address_is_refused() {
        let withdrawer = Pubkey::new_unique();
        let accounts = [
            escrow_account(instance_data(true)),
            // The system program's id is the all-zero key.
            Some(FetchedAccount {
                owner: Pubkey::default(),
                data: Vec::new(),
            }),
        ];

        assert_eq!(
            decide(&[withdrawer], &accounts),
            Verdict::Refused(withdrawer)
        );
    }

    #[test]
    fn a_missing_instance_is_left_to_the_escrow() {
        let withdrawer = Pubkey::new_unique();
        let accounts = [None, None];

        assert!(matches!(
            decide(&[withdrawer], &accounts),
            Verdict::Unchecked(_)
        ));
    }

    #[test]
    fn the_whitelist_settings_go_together() {
        let mainnet_rpc = Some("https://api.mainnet-beta.solana.com");
        let instance_pda = Pubkey::new_unique().to_string();
        let instance_pda = Some(instance_pda.as_str());
        let blank = Some("  ");
        let reads = Quota::per_second(NonZeroU32::new(20).unwrap());
        // The last column is whether the check is on, or None when the gateway refuses to start.
        let cases = [
            (None, None, Some(false)),
            (blank, blank, Some(false)),
            (mainnet_rpc, instance_pda, Some(true)),
            (mainnet_rpc, None, None),
            (mainnet_rpc, blank, None),
            (None, instance_pda, None),
        ];

        for (rpc_url_setting, instance_setting, expected) in cases {
            let enabled = WithdrawWhitelist::from_config(rpc_url_setting, instance_setting, reads)
                .map(|whitelist| whitelist.is_some())
                .ok();
            assert_eq!(
                enabled, expected,
                "rpc_url={rpc_url_setting:?} instance={instance_setting:?}"
            );
        }
    }

    /// A whitelist with the check on and an empty cache.
    fn enabled_whitelist() -> WithdrawWhitelist {
        let instance = Pubkey::new_unique().to_string();
        let reads = Quota::per_second(NonZeroU32::new(20).unwrap());
        WithdrawWhitelist::from_config(Some("http://127.0.0.1:1"), Some(instance.as_str()), reads)
            .expect("the whitelist settings are valid")
            .expect("both settings are set")
    }

    #[test]
    fn a_full_cache_drops_only_expired_reads() {
        let whitelist = enabled_whitelist();
        let fresh = Pubkey::new_unique();
        {
            let mut cache = whitelist.cache.lock().unwrap();
            for _ in 1..MAX_CACHED_ACCOUNTS {
                cache.insert(Pubkey::new_unique(), (Instant::now() - CACHE_TTL, None));
            }
            cache.insert(fresh, (Instant::now(), None));
        }
        let read = Pubkey::new_unique();

        whitelist.remember(Instant::now(), &[read], &[None]);

        let cache = whitelist.cache.lock().unwrap();
        assert_eq!(cache.len(), 2);
        assert!(cache.contains_key(&fresh));
        assert!(cache.contains_key(&read));
    }

    #[test]
    fn a_cache_full_of_fresh_reads_keeps_them() {
        let whitelist = enabled_whitelist();
        {
            let mut cache = whitelist.cache.lock().unwrap();
            for _ in 0..MAX_CACHED_ACCOUNTS {
                cache.insert(Pubkey::new_unique(), (Instant::now(), None));
            }
        }
        let read = Pubkey::new_unique();

        whitelist.remember(Instant::now(), &[read], &[None]);

        let cache = whitelist.cache.lock().unwrap();
        assert_eq!(cache.len(), MAX_CACHED_ACCOUNTS);
        assert!(!cache.contains_key(&read));
    }
}
