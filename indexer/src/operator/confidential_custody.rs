//! Confidential custody of a configured escrow ATA, read from the account alone.
//!
//! Custody is public + pending + x, where pending is decrypted from its lo and hi ciphertexts and
//! x is the AES copy of available, confirmed by an opening check on the ElGamal ciphertext.

use crate::error::StorageError;
use crate::metrics::{
    RECONCILIATION_CONFIDENTIAL_HOLD_AGE, RECONCILIATION_CONFIDENTIAL_LAGGING,
    RECONCILIATION_CONFIDENTIAL_MISMATCH,
};
use crate::operator::escrow_sweep::ConfidentialBalances;
use crate::storage::Storage;
use private_channel_confidential::{Ciphertext, Keys, PendingError};
use solana_sdk::pubkey::Pubkey;
use std::collections::{HashMap, HashSet};

/// Why a configured mint is not compared this time. Alert-only: none of these count toward
/// the input-dark halt, since each is either lag or a failure contained to the mint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfidentialHold {
    /// The AES copy does not open and the counters say a deposit raced the last apply.
    StaleAes,
    /// Pending hi is beyond the bounded search until the sender applies it.
    PendingTooLarge,
    /// A confidential deposit could not be decrypted.
    Poisoned,
    /// The account is configured with a key other than ours.
    KeyMismatch,
}

impl ConfidentialHold {
    pub const LABELS: [&'static str; 4] =
        ["stale_aes", "pending_too_large", "poisoned", "key_mismatch"];

    pub fn label(&self) -> &'static str {
        match self {
            Self::StaleAes => "stale_aes",
            Self::PendingTooLarge => "pending_too_large",
            Self::Poisoned => "poisoned",
            Self::KeyMismatch => "key_mismatch",
        }
    }

    /// Lag the sender clears on its own, as opposed to a hold that needs an operator.
    pub fn is_transient(&self) -> bool {
        matches!(self, Self::StaleAes | Self::PendingTooLarge)
    }
}

/// One configured mint's custody verdict.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfidentialCustody {
    /// Public plus pending plus the verified available balance.
    Counted(u64),
    /// Not compared; alert only.
    Held(ConfidentialHold),
    /// Custody could not be proven on a healthy mint. Counted as an unreadable input, so a
    /// persistent mismatch still halts.
    Mismatch(String),
}

/// Judge one configured mint from its account read.
pub async fn resolve_confidential_custody(
    storage: &Storage,
    keys: Option<&Keys>,
    mint: &Pubkey,
    public: u64,
    balances: &ConfidentialBalances,
) -> Result<ConfidentialCustody, StorageError> {
    let poisoned = poisoned_mints(storage).await?.contains(&mint.to_string());
    let verdict = judge_off_executor(poisoned, keys, public, balances).await;
    publish(mint, &verdict);
    Ok(verdict)
}

async fn poisoned_mints(storage: &Storage) -> Result<HashSet<String>, StorageError> {
    Ok(storage
        .get_confidential_poisoned_mints()
        .await?
        .into_iter()
        .collect())
}

/// The verdict for one configured account.
fn judge(
    poisoned: bool,
    keys: Option<&Keys>,
    public: u64,
    balances: &ConfidentialBalances,
) -> ConfidentialCustody {
    use ConfidentialCustody::{Counted, Held, Mismatch};
    if poisoned {
        return Held(ConfidentialHold::Poisoned);
    }
    let Some(keys) = keys else {
        return Mismatch("no confidential key loaded for a configured escrow account".into());
    };
    if keys.pubkey() != balances.elgamal_pubkey {
        return Held(ConfidentialHold::KeyMismatch);
    }
    // Checked before the pending search so a current copy costs no discrete log.
    let copy = keys.decrypt_aes(&balances.decryptable_available);
    let available = match copy.filter(|x| keys.opens_to(&balances.available, *x)) {
        Some(available) => available,
        // A deposit raced the last apply: available is the copy plus the signed gap to it.
        None if balances.expected_credit_counter != balances.actual_credit_counter => {
            match copy.and_then(|x| available_from_gap(keys, &balances.available, x)) {
                Some(available) => available,
                None => return Held(ConfidentialHold::StaleAes),
            }
        }
        None => {
            return Mismatch(
                "AES copy does not open available and no apply race is recorded".into(),
            )
        }
    };
    let pending = match keys.decrypt_pending(&balances.pending_lo, &balances.pending_hi) {
        Ok(pending) => pending,
        Err(PendingError::HiTooLarge) => return Held(ConfidentialHold::PendingTooLarge),
        Err(e) => return Mismatch(format!("pending balance does not decrypt: {e}")),
    };
    match public
        .checked_add(pending)
        .and_then(|sum| sum.checked_add(available))
    {
        Some(custody) => Counted(custody),
        None => Mismatch("custody overflow: public + pending + available exceeds u64".into()),
    }
}

/// `judge` on a blocking thread: the pending and gap searches are discrete logs that must not
/// stall the async workers.
async fn judge_off_executor(
    poisoned: bool,
    keys: Option<&Keys>,
    public: u64,
    balances: &ConfidentialBalances,
) -> ConfidentialCustody {
    let keys = keys.cloned();
    let balances = *balances;
    tokio::task::spawn_blocking(move || judge(poisoned, keys.as_ref(), public, &balances))
        .await
        .unwrap_or_else(|e| ConfidentialCustody::Mismatch(format!("custody judgement failed: {e}")))
}

/// The value `available` encrypts, found from the copy `x` plus their decrypted signed gap.
fn available_from_gap(keys: &Keys, available: &Ciphertext, x: u64) -> Option<u64> {
    let gap = keys.decrypt_signed_gap(available, x)?;
    let value = u64::try_from(i128::from(x) + i128::from(gap)).ok()?;
    keys.opens_to(available, value).then_some(value)
}

fn publish(mint: &Pubkey, verdict: &ConfidentialCustody) {
    let mint = mint.to_string();
    let held = match verdict {
        ConfidentialCustody::Held(hold) => Some(hold.label()),
        _ => None,
    };
    for label in ConfidentialHold::LABELS {
        RECONCILIATION_CONFIDENTIAL_LAGGING
            .with_label_values(&[&mint, label])
            .set(if held == Some(label) { 1.0 } else { 0.0 });
    }
    let mismatch = matches!(verdict, ConfidentialCustody::Mismatch(_));
    RECONCILIATION_CONFIDENTIAL_MISMATCH
        .with_label_values(&[&mint])
        .set(if mismatch { 1.0 } else { 0.0 });
}

/// Custody with every configured mint judged: counted mints carry their full custody, the
/// rest are listed apart and must be left out of every comparison.
#[derive(Debug, Default)]
pub struct ResolvedCustody {
    pub balances: HashMap<Pubkey, u64>,
    pub held: Vec<(Pubkey, ConfidentialHold)>,
    pub mismatched: Vec<(Pubkey, String)>,
}

/// Judge every configured mint in one custody read.
pub async fn resolve_custody(
    storage: &Storage,
    keys: Option<&Keys>,
    balances: &HashMap<Pubkey, u64>,
    confidential: &HashMap<Pubkey, ConfidentialBalances>,
) -> Result<ResolvedCustody, StorageError> {
    let mut resolved = ResolvedCustody {
        balances: balances.clone(),
        ..Default::default()
    };
    if confidential.is_empty() {
        return Ok(resolved);
    }
    let poisoned = poisoned_mints(storage).await?;
    for (mint, part) in confidential {
        let public = balances.get(mint).copied().unwrap_or(0);
        let verdict =
            judge_off_executor(poisoned.contains(&mint.to_string()), keys, public, part).await;
        publish(mint, &verdict);
        match verdict {
            ConfidentialCustody::Counted(custody) => {
                resolved.balances.insert(*mint, custody);
            }
            ConfidentialCustody::Held(hold) => {
                resolved.balances.remove(mint);
                resolved.held.push((*mint, hold));
            }
            ConfidentialCustody::Mismatch(reason) => {
                resolved.balances.remove(mint);
                resolved.mismatched.push((*mint, reason));
            }
        }
    }
    Ok(resolved)
}

/// Export how long `mint` has been held for `reason` (0 for every other reason).
pub fn publish_hold_age(mint: &Pubkey, reason: Option<&str>, seconds: f64) {
    let mint = mint.to_string();
    for label in ConfidentialHold::LABELS {
        let age = if reason == Some(label) { seconds } else { 0.0 };
        RECONCILIATION_CONFIDENTIAL_HOLD_AGE
            .with_label_values(&[&mint, label])
            .set(age);
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::operator::escrow_sweep::tests::confidential_balances;
    use crate::storage::common::models::{DbTransactionBuilder, TransactionType};
    use crate::storage::common::storage::mock::MockStorage;
    use private_channel_confidential::test_support::{keys, other_keys};
    use std::sync::Arc;
    use ConfidentialCustody::{Counted, Held, Mismatch};

    /// A healthy account: 70,000 pending in two credits, 5,000 applied and its AES copy current.
    pub(crate) fn healthy(keys: &Keys) -> ConfidentialBalances {
        confidential_balances(keys, &[(60_000, 0), (10_000, 0)], 5_000, 5_000, (4, 4))
    }

    /// Custody of `healthy` on top of `public`.
    pub(crate) const HEALTHY_CONFIDENTIAL: u64 = 75_000;

    /// The same account after a deposit raced its last apply: the AES copy misses it.
    pub(crate) fn stale_aes(keys: &Keys) -> ConfidentialBalances {
        confidential_balances(keys, &[], 5_000, 4_000, (4, 5))
    }

    /// A raced copy whose gap to the ciphertext cannot be decrypted: lagging until repaired.
    pub(crate) fn stale_aes_unreadable_gap(keys: &Keys) -> ConfidentialBalances {
        let mut b = stale_aes(keys);
        b.available = [0xff; 64];
        b
    }

    /// The AES copy disagrees with available while the counters match: a real mismatch.
    pub(crate) fn mismatched(keys: &Keys) -> ConfidentialBalances {
        confidential_balances(keys, &[], 5_000, 4_000, (5, 5))
    }

    fn storage(poisoned: &[Pubkey]) -> Storage {
        let mock = MockStorage::new();
        *mock.pending_transactions.lock().unwrap() = poisoned
            .iter()
            .map(|mint| {
                DbTransactionBuilder::new("s".into(), 1, mint.to_string(), 0)
                    .initiator("user".into())
                    .recipient("user".into())
                    .transaction_type(TransactionType::Deposit)
                    .confidential(true)
                    .undecrypted(vec![1])
                    .build()
            })
            .collect();
        Storage::Mock(mock)
    }

    async fn resolve(
        keys: Option<&Keys>,
        public: u64,
        b: &ConfidentialBalances,
    ) -> ConfidentialCustody {
        let mint = Pubkey::new_unique();
        resolve_confidential_custody(&storage(&[]), keys, &mint, public, b)
            .await
            .unwrap()
    }

    /// Custody is public + pending + the verified AES copy.
    #[tokio::test]
    async fn custody_is_public_plus_pending_plus_verified_available() {
        let keys = keys();
        assert_eq!(
            resolve(Some(&keys), 900, &healthy(&keys)).await,
            Counted(900 + HEALTHY_CONFIDENTIAL)
        );
    }

    /// On a fee mint pending lo can be negative; the total still counts exactly.
    #[tokio::test]
    async fn custody_counts_a_fee_mint_with_negative_pending_lo() {
        let keys = keys();
        // 70,000 minus a 5,000 fee: lo is 4,464 - 5,000 < 0.
        let b = confidential_balances(&keys, &[(70_000, 5_000)], 7, 7, (0, 0));
        assert_eq!(resolve(Some(&keys), 1, &b).await, Counted(1 + 65_000 + 7));
    }

    /// A raced copy is counted exactly from its gap to the ciphertext; with matching counters a
    /// copy that does not open is a mismatch, as is one that does not decrypt under our key.
    #[tokio::test]
    async fn a_stale_aes_copy_counts_its_gap_only_when_the_counters_differ() {
        let keys = keys();
        assert_eq!(
            resolve(Some(&keys), 7, &stale_aes(&keys)).await,
            Counted(7 + 5_000)
        );
        assert!(matches!(
            resolve(Some(&keys), 0, &mismatched(&keys)).await,
            Mismatch(_)
        ));
        let mut garbage = healthy(&keys);
        garbage.decryptable_available = [0xAB; 36];
        assert!(matches!(
            resolve(Some(&keys), 0, &garbage).await,
            Mismatch(_)
        ));
    }

    /// A copy above the ciphertext's value (negative gap) still counts the ciphertext's value,
    /// with pending on top.
    #[tokio::test]
    async fn a_negative_gap_counts_the_ciphertext_value() {
        let keys = keys();
        let b = confidential_balances(&keys, &[(300, 0)], 5_000, 6_500, (4, 5));
        assert_eq!(resolve(Some(&keys), 1, &b).await, Counted(1 + 300 + 5_000));
    }

    /// A gap past the bounded search, or a copy that does not decrypt, is lagging: alert only.
    #[tokio::test]
    async fn an_undecryptable_gap_is_lagging_not_dark() {
        let keys = keys();
        let far = confidential_balances(&keys, &[], (1 << 41) + 3, 0, (4, 5));
        assert_eq!(
            resolve(Some(&keys), 0, &far).await,
            Held(ConfidentialHold::StaleAes)
        );
        assert_eq!(
            resolve(Some(&keys), 0, &stale_aes_unreadable_gap(&keys)).await,
            Held(ConfidentialHold::StaleAes)
        );
        let mut no_copy = stale_aes(&keys);
        no_copy.decryptable_available = [0xAB; 36];
        assert_eq!(
            resolve(Some(&keys), 0, &no_copy).await,
            Held(ConfidentialHold::StaleAes)
        );
    }

    /// Pending hi past the bounded search is lagging, not a mismatch.
    #[tokio::test]
    async fn pending_hi_too_large_is_lagging() {
        let keys = keys();
        let b = confidential_balances(&keys, &[(1 << 57, 0)], 0, 0, (0, 0));
        assert_eq!(
            resolve(Some(&keys), 0, &b).await,
            Held(ConfidentialHold::PendingTooLarge)
        );
    }

    #[tokio::test]
    async fn a_poisoned_mint_holds() {
        let mint = Pubkey::new_unique();
        let keys = keys();
        assert_eq!(
            resolve_confidential_custody(&storage(&[mint]), Some(&keys), &mint, 0, &healthy(&keys))
                .await
                .unwrap(),
            Held(ConfidentialHold::Poisoned)
        );
    }

    #[tokio::test]
    async fn a_foreign_account_key_holds_and_a_missing_key_is_a_mismatch() {
        let keys = keys();
        let mut b = healthy(&keys);
        assert!(matches!(resolve(None, 0, &b).await, Mismatch(_)));
        b.elgamal_pubkey = other_keys().pubkey();
        assert_eq!(
            resolve(Some(&keys), 0, &b).await,
            Held(ConfidentialHold::KeyMismatch)
        );
    }

    /// public + pending + x past u64 is an error, never a wrapped total.
    #[tokio::test]
    async fn custody_overflow_is_a_mismatch() {
        let keys = keys();
        assert!(matches!(
            resolve(Some(&keys), u64::MAX - 1, &healthy(&keys)).await,
            Mismatch(reason) if reason.contains("overflow")
        ));
    }

    /// Held and mismatched mints leave the custody map, so no caller can read them as 0.
    #[tokio::test]
    async fn resolve_custody_removes_every_uncounted_mint() {
        let keys = keys();
        let (held, bad, counted, public_only) = (
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
        );
        let balances_map = HashMap::from([(held, 2), (bad, 3), (counted, 4), (public_only, 5)]);
        let confidential = HashMap::from([
            (held, healthy(&keys)),
            (bad, mismatched(&keys)),
            (counted, healthy(&keys)),
        ]);
        let resolved =
            resolve_custody(&storage(&[held]), Some(&keys), &balances_map, &confidential)
                .await
                .unwrap();
        assert_eq!(
            resolved.balances,
            HashMap::from([(counted, 4 + HEALTHY_CONFIDENTIAL), (public_only, 5)])
        );
        assert_eq!(resolved.held, vec![(held, ConfidentialHold::Poisoned)]);
        assert_eq!(resolved.mismatched.len(), 1);
        assert_eq!(resolved.mismatched[0].0, bad);
    }

    /// Judging on a blocking thread gives exactly the inline verdicts, and the executor keeps
    /// running while a long search is under way.
    #[tokio::test(flavor = "current_thread")]
    async fn judging_off_the_executor_gives_the_inline_verdicts() {
        let keys = keys();
        let mut foreign = healthy(&keys);
        foreign.elgamal_pubkey = other_keys().pubkey();
        let negative = confidential_balances(&keys, &[(300, 0)], 5_000, 6_500, (4, 5));
        let cases = [
            (false, healthy(&keys)),
            (false, stale_aes(&keys)),
            (false, negative),
            (false, stale_aes_unreadable_gap(&keys)),
            (false, mismatched(&keys)),
            (false, foreign),
            (true, healthy(&keys)),
        ];
        for (poisoned, b) in &cases {
            assert_eq!(
                judge_off_executor(*poisoned, Some(&keys), 9, b).await,
                judge(*poisoned, Some(&keys), 9, b)
            );
        }
        assert_eq!(
            judge_off_executor(false, None, 9, &cases[0].1).await,
            judge(false, None, 9, &cases[0].1)
        );

        let ticks = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = ticks.clone();
        let ticker = tokio::spawn(async move {
            loop {
                counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        });
        let far = confidential_balances(&keys, &[], (1 << 41) + 3, 0, (4, 5));
        assert_eq!(
            judge_off_executor(false, Some(&keys), 0, &far).await,
            Held(ConfidentialHold::StaleAes)
        );
        ticker.abort();
        assert!(ticks.load(std::sync::atomic::Ordering::SeqCst) > 10);
    }

    /// The lagging gauge carries the hold reason; the mismatch gauge marks the dark case.
    #[tokio::test]
    async fn verdicts_publish_their_gauges() {
        let keys = keys();
        let mint = Pubkey::new_unique();
        let storage = storage(&[]);
        let label = mint.to_string();
        let lagging = |reason: &str| {
            RECONCILIATION_CONFIDENTIAL_LAGGING
                .with_label_values(&[&label, reason])
                .get()
        };
        resolve_confidential_custody(
            &storage,
            Some(&keys),
            &mint,
            0,
            &stale_aes_unreadable_gap(&keys),
        )
        .await
        .unwrap();
        assert_eq!(lagging("stale_aes"), 1.0);
        resolve_confidential_custody(&storage, Some(&keys), &mint, 0, &mismatched(&keys))
            .await
            .unwrap();
        assert_eq!(lagging("stale_aes"), 0.0);
        assert_eq!(
            RECONCILIATION_CONFIDENTIAL_MISMATCH
                .with_label_values(&[&label])
                .get(),
            1.0
        );
    }
}
