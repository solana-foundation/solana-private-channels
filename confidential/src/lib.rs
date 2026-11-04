//! The escrow's confidential-transfer crypto, shared by the indexer, the operator and their tests.
//!
//! Per-event amounts and pending balances are decrypted by discrete log; the available balance is
//! read from its AES copy and confirmed against the ElGamal ciphertext with [`Keys::opens_to`].

use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use curve25519_dalek::{constants::RISTRETTO_BASEPOINT_POINT, scalar::Scalar};
use serde::Deserialize;
use solana_zk_sdk::encryption::{
    auth_encryption::{AeCiphertext, AeKey},
    discrete_log::DiscreteLog,
    elgamal::{ElGamalCiphertext, ElGamalKeypair, ElGamalSecretKey},
};
use std::num::NonZeroUsize;
use zeroize::{Zeroize, Zeroizing};

mod proofs;
#[cfg(any(test, feature = "test-support"))]
pub mod test_support;

pub use proofs::{MoveToPublicProofs, ProofError, ReleaseFee, ReleaseProofs};

/// Environment variable holding the instance keys as base64 JSON.
pub const KEYS_ENV: &str = "ESCROW_CONFIDENTIAL_KEYS";

/// An ElGamal ciphertext as Token-2022 and the escrow events store it.
pub type Ciphertext = [u8; 64];

/// Token-2022's AES copy of the available balance (`decryptable_available_balance`).
pub type AeBytes = [u8; 36];

/// Largest amount a confidential transfer can carry: 16 low bits plus 32 high bits.
pub const MAX_CONFIDENTIAL_AMOUNT: u64 = (1 << 48) - 1;

const LO_BITS: u32 = 16;
const LO_MAX: u64 = (1 << LO_BITS) - 1;
const ELGAMAL_SECRET_LEN: usize = 32;
const AES_KEY_LEN: usize = 16;
const WIDE_BITS: u32 = 40;
const MAX_SEARCH_THREADS: usize = 16;

/// Key loading errors. Each names the variable only, never its contents.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum KeysError {
    #[error("{KEYS_ENV} is not valid UTF-8")]
    NotUnicode,
    #[error("{KEYS_ENV} is not JSON with base64 elgamal_secret and aes_key fields")]
    Malformed,
    #[error("{KEYS_ENV} elgamal_secret must be {ELGAMAL_SECRET_LEN} bytes")]
    ElGamalLength,
    #[error("{KEYS_ENV} elgamal_secret is not a valid secret key")]
    ElGamalInvalid,
    #[error("{KEYS_ENV} aes_key must be {AES_KEY_LEN} bytes")]
    AesLength,
}

/// Why a pending balance could not be decrypted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PendingError {
    #[error("pending balance ciphertext is not valid")]
    Invalid,
    #[error("pending lo does not decrypt in the 32-bit range")]
    LoUndecryptable,
    #[error("pending hi is beyond the bounded search")]
    HiTooLarge,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct KeysJson {
    elgamal_secret: String,
    aes_key: String,
}

impl Drop for KeysJson {
    fn drop(&mut self) {
        self.elgamal_secret.zeroize();
        self.aes_key.zeroize();
    }
}

/// The escrow instance's ElGamal and AES keys. Both parts zeroize themselves on drop, clones
/// included, so a copy handed to a blocking task leaves nothing behind.
#[derive(Clone)]
pub struct Keys {
    elgamal: ElGamalKeypair,
    aes: AeKey,
}

impl std::fmt::Debug for Keys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Keys")
            .field("pubkey", &BASE64.encode(self.pubkey()))
            .finish_non_exhaustive()
    }
}

impl Keys {
    /// Load the keys from [`KEYS_ENV`]. Unset or blank is `None`: keys are optional until
    /// an escrow account is configured.
    pub fn from_env() -> Result<Option<Self>, KeysError> {
        match std::env::var(KEYS_ENV) {
            Ok(value) => {
                let value = Zeroizing::new(value);
                if value.trim().is_empty() {
                    Ok(None)
                } else {
                    Self::from_json(&value).map(Some)
                }
            }
            Err(std::env::VarError::NotPresent) => Ok(None),
            Err(std::env::VarError::NotUnicode(_)) => Err(KeysError::NotUnicode),
        }
    }

    /// Parse `{"elgamal_secret": "<base64>", "aes_key": "<base64>"}`.
    pub fn from_json(json: &str) -> Result<Self, KeysError> {
        let parsed: KeysJson = serde_json::from_str(json).map_err(|_| KeysError::Malformed)?;
        let secret = Zeroizing::new(
            BASE64
                .decode(parsed.elgamal_secret.as_bytes())
                .map_err(|_| KeysError::Malformed)?,
        );
        let aes = Zeroizing::new(
            BASE64
                .decode(parsed.aes_key.as_bytes())
                .map_err(|_| KeysError::Malformed)?,
        );
        Self::from_bytes(&secret, &aes)
    }

    /// Build from the raw 32-byte ElGamal secret and 16-byte AES key.
    pub fn from_bytes(elgamal_secret: &[u8], aes_key: &[u8]) -> Result<Self, KeysError> {
        if elgamal_secret.len() != ELGAMAL_SECRET_LEN {
            return Err(KeysError::ElGamalLength);
        }
        if aes_key.len() != AES_KEY_LEN {
            return Err(KeysError::AesLength);
        }
        let secret =
            ElGamalSecretKey::try_from(elgamal_secret).map_err(|_| KeysError::ElGamalInvalid)?;
        let aes = AeKey::try_from(aes_key).map_err(|_| KeysError::AesLength)?;
        Ok(Self {
            elgamal: ElGamalKeypair::new(secret),
            aes,
        })
    }

    /// The keys as the JSON [`Keys::from_json`] reads.
    pub fn to_json(&self) -> Zeroizing<String> {
        let secret: Zeroizing<[u8; ELGAMAL_SECRET_LEN]> =
            Zeroizing::new(self.elgamal.secret().into());
        let aes: Zeroizing<[u8; AES_KEY_LEN]> = Zeroizing::new((&self.aes).into());
        Zeroizing::new(format!(
            r#"{{"elgamal_secret":"{}","aes_key":"{}"}}"#,
            BASE64.encode(*secret),
            BASE64.encode(*aes)
        ))
    }

    /// The ElGamal public key, as stored in the escrow account and the configure event.
    pub fn pubkey(&self) -> [u8; 32] {
        self.elgamal.pubkey().to_bytes()
    }

    /// The AES key, for the operator's decryptable balances.
    pub fn aes(&self) -> &AeKey {
        &self.aes
    }

    /// One deposit's net amount from the pending balance before and after it.
    /// The low part can be negative on fee mints; the high part never is.
    pub fn decrypt_deposit(
        &self,
        lo_before: &Ciphertext,
        hi_before: &Ciphertext,
        lo_after: &Ciphertext,
        hi_after: &Ciphertext,
    ) -> Option<u64> {
        let lo = self.decrypt_signed_change(lo_before, lo_after)?;
        if lo.unsigned_abs() > LO_MAX {
            return None;
        }
        let hi = self.decrypt_u32(&sub(hi_after, hi_before)?)?;
        let amount = i128::from(lo) + (i128::from(hi) << LO_BITS);
        u64::try_from(amount)
            .ok()
            .filter(|amount| *amount <= MAX_CONFIDENTIAL_AMOUNT)
    }

    /// A release's gross amount from its low and high ciphertexts, both non-negative.
    pub fn decrypt_release(&self, lo: &Ciphertext, hi: &Ciphertext) -> Option<u64> {
        let lo = self.decrypt_u32(&ElGamalCiphertext::from_bytes(lo)?)?;
        if lo > LO_MAX {
            return None;
        }
        let hi = self.decrypt_u32(&ElGamalCiphertext::from_bytes(hi)?)?;
        Some(lo + (hi << LO_BITS))
    }

    /// The pending balance `lo + 2^16 * hi`. lo may be negative on fee mints; hi uses the
    /// 32-bit search, then a bounded 40-bit one.
    pub fn decrypt_pending(&self, lo: &Ciphertext, hi: &Ciphertext) -> Result<u64, PendingError> {
        let lo_ct = ElGamalCiphertext::from_bytes(lo).ok_or(PendingError::Invalid)?;
        let hi_ct = ElGamalCiphertext::from_bytes(hi).ok_or(PendingError::Invalid)?;
        // A negative total only arises on fee mints; on others the flip simply fails too.
        let lo = match self.decrypt_u32(&lo_ct) {
            Some(lo) => i128::from(lo),
            None => -i128::from(
                self.decrypt_u32(&(ElGamalCiphertext::default() - lo_ct))
                    .ok_or(PendingError::LoUndecryptable)?,
            ),
        };
        let hi = self
            .decrypt_u32(&hi_ct)
            .or_else(|| self.decrypt_wide(&hi_ct));
        let hi = hi.ok_or(PendingError::HiTooLarge)?;
        u64::try_from(lo + (i128::from(hi) << LO_BITS)).map_err(|_| PendingError::Invalid)
    }

    /// The available balance from its AES copy. Confirm it with [`Keys::opens_to`] before use.
    pub fn decrypt_aes(&self, aes: &AeBytes) -> Option<u64> {
        self.aes.decrypt(&AeCiphertext::from_bytes(aes)?)
    }

    /// True when `ciphertext` encrypts exactly `value` under these keys.
    /// One opening check, no discrete log: `C - s*D == value*G`.
    pub fn opens_to(&self, ciphertext: &Ciphertext, value: u64) -> bool {
        let Some(ciphertext) = ElGamalCiphertext::from_bytes(ciphertext) else {
            return false;
        };
        let rest = ciphertext.subtract_amount(Scalar::from(value));
        let secret = self.elgamal.secret().get_scalar();
        rest.commitment.get_point() == &(secret * rest.handle.get_point())
    }

    /// What `ciphertext` encrypts minus `known`, when the difference is non-negative and
    /// inside the bounded search. Recovers the deposits a stale AES copy missed.
    pub fn decrypt_gap(&self, ciphertext: &Ciphertext, known: u64) -> Option<u64> {
        let rest = ElGamalCiphertext::from_bytes(ciphertext)?.subtract_amount(Scalar::from(known));
        self.decrypt_u32(&rest).or_else(|| self.decrypt_wide(&rest))
    }

    /// What `ciphertext` encrypts minus `known`, of either sign, inside the bounded search.
    pub fn decrypt_signed_gap(&self, ciphertext: &Ciphertext, known: u64) -> Option<i64> {
        let rest = ElGamalCiphertext::from_bytes(ciphertext)?.subtract_amount(Scalar::from(known));
        let search =
            |ct: &ElGamalCiphertext| self.decrypt_u32(ct).or_else(|| self.decrypt_wide(ct));
        if let Some(gain) = search(&rest) {
            return i64::try_from(gain).ok();
        }
        let loss = search(&(ElGamalCiphertext::default() - rest))?;
        i64::try_from(loss).ok().map(|loss| -loss)
    }

    // The zk-sdk search split over threads: each walks its own residue class of the same
    // 2^16 steps, so the answer is the one the single-threaded search gives.
    fn decrypt_u32(&self, ciphertext: &ElGamalCiphertext) -> Option<u64> {
        let mut search = ciphertext.decrypt(self.elgamal.secret());
        // Refused only for a non power of two, which leaves the search single-threaded.
        let _ = search.num_threads(search_threads());
        search.decode_u32()
    }

    // Values in [2^32, 2^40): one 32-bit search per top byte, so the cost is bounded.
    fn decrypt_wide(&self, ciphertext: &ElGamalCiphertext) -> Option<u64> {
        let secret = self.elgamal.secret().get_scalar();
        let mut target = ciphertext.commitment.get_point() - secret * ciphertext.handle.get_point();
        let step = Scalar::from(1u64 << 32) * RISTRETTO_BASEPOINT_POINT;
        for top in 1..(1u64 << (WIDE_BITS - 32)) {
            target -= step;
            let mut search = DiscreteLog::new_for_g(target);
            search.num_threads(search_threads()).ok()?;
            if let Some(low) = search.decode_u32() {
                return Some((top << 32) + low);
            }
        }
        None
    }

    fn decrypt_signed_change(&self, before: &Ciphertext, after: &Ciphertext) -> Option<i64> {
        if let Some(gain) = self.decrypt_u32(&sub(after, before)?) {
            return i64::try_from(gain).ok();
        }
        let loss = self.decrypt_u32(&sub(before, after)?)?;
        i64::try_from(loss).ok().map(|loss| -loss)
    }
}

/// Threads per 32-bit search: the largest power of two the host's cores allow, up to 16.
fn search_threads() -> NonZeroUsize {
    static THREADS: std::sync::OnceLock<NonZeroUsize> = std::sync::OnceLock::new();
    *THREADS.get_or_init(|| {
        let cores = std::thread::available_parallelism().map_or(1, NonZeroUsize::get);
        let threads = 1usize << cores.min(MAX_SEARCH_THREADS).ilog2();
        NonZeroUsize::new(threads).unwrap_or(NonZeroUsize::MIN)
    })
}

fn sub(left: &Ciphertext, right: &Ciphertext) -> Option<ElGamalCiphertext> {
    let left = ElGamalCiphertext::from_bytes(left)?;
    let right = ElGamalCiphertext::from_bytes(right)?;
    Some(left - right)
}

/// Split an amount into Token-2022's 16-bit low and 32-bit high parts.
pub fn split(amount: u64) -> (u64, u64) {
    (amount & LO_MAX, amount >> LO_BITS)
}

#[cfg(test)]
mod tests {
    use super::test_support::*;
    use super::*;

    const EDGES: [u64; 5] = [0, 1, LO_MAX, 1 << 16, MAX_CONFIDENTIAL_AMOUNT];

    fn zero() -> Ciphertext {
        [0u8; 64]
    }

    #[test]
    fn decrypt_deposit_no_fee_edges() {
        let keys = keys();
        for amount in EDGES {
            let (lo, hi) = split(amount);
            let (lo_after, hi_after) = credit(&keys, &zero(), &zero(), lo as i64, hi);
            assert_eq!(
                keys.decrypt_deposit(&zero(), &zero(), &lo_after, &hi_after),
                Some(amount),
                "amount {amount}"
            );
        }
    }

    #[test]
    fn decrypt_deposit_on_nonzero_pending() {
        let keys = keys();
        let (lo0, hi0) = credit(&keys, &zero(), &zero(), 1234, 5);
        let (lo1, hi1) = credit(&keys, &lo0, &hi0, 40_000, 2);
        assert_eq!(
            keys.decrypt_deposit(&lo0, &hi0, &lo1, &hi1),
            Some(40_000 + (2 << 16))
        );
    }

    #[test]
    fn decrypt_deposit_fee_zero_matches_no_fee() {
        let keys = keys();
        let (lo, hi) = net_of_fee(70_000, 0);
        let (lo_after, hi_after) = credit(&keys, &zero(), &zero(), lo, hi);
        assert_eq!(
            keys.decrypt_deposit(&zero(), &zero(), &lo_after, &hi_after),
            Some(70_000)
        );
    }

    #[test]
    fn decrypt_deposit_capped_fee_with_negative_lo() {
        let keys = keys();
        // 70,000 is lo 4,464 hi 1; a 5,000 fee leaves lo -536 hi 1.
        let (lo, hi) = net_of_fee(70_000, 5_000);
        assert_eq!((lo, hi), (-536, 1));
        let (lo_after, hi_after) = credit(&keys, &zero(), &zero(), lo, hi);
        assert_eq!(
            keys.decrypt_deposit(&zero(), &zero(), &lo_after, &hi_after),
            Some(65_000)
        );
    }

    #[test]
    fn decrypt_deposit_wrong_key_fails() {
        let keys = keys();
        let (lo_after, hi_after) = credit(&keys, &zero(), &zero(), 100, 1);
        assert_eq!(
            other_keys().decrypt_deposit(&zero(), &zero(), &lo_after, &hi_after),
            None
        );
    }

    #[test]
    fn decrypt_deposit_garbage_bytes_fail() {
        let keys = keys();
        assert_eq!(
            keys.decrypt_deposit(&[0xff; 64], &zero(), &zero(), &zero()),
            None
        );
    }

    #[test]
    fn decrypt_release_edges() {
        let keys = keys();
        for amount in EDGES {
            let (lo, hi) = split(amount);
            assert_eq!(
                keys.decrypt_release(&encrypt(&keys, lo), &encrypt(&keys, hi)),
                Some(amount),
                "amount {amount}"
            );
        }
    }

    #[test]
    fn decrypt_release_wrong_key_fails() {
        let keys = keys();
        let (lo, hi) = split(70_000);
        assert_eq!(
            other_keys().decrypt_release(&encrypt(&keys, lo), &encrypt(&keys, hi)),
            None
        );
    }

    #[test]
    fn decrypt_release_rejects_lo_above_16_bits() {
        let keys = keys();
        assert_eq!(
            keys.decrypt_release(&encrypt(&keys, 1 << 16), &encrypt(&keys, 0)),
            None
        );
    }

    #[test]
    fn decrypt_pending_lo_positive_and_negative() {
        let keys = keys();
        let mut account = SimAccount::new(&keys);
        account.credit(&keys, 1_234, 0);
        account.credit(&keys, 70_000, 0);
        let pending = keys.decrypt_pending(&account.pending_lo, &account.pending_hi);
        assert_eq!(pending, Ok(71_234));
        // Fee mint: lo -536 hi 1 leaves lo negative in total.
        let mut fee = SimAccount::new(&keys);
        fee.credit(&keys, 70_000, 5_000);
        assert_eq!(
            keys.decrypt_pending(&fee.pending_lo, &fee.pending_hi),
            Ok(65_000)
        );
    }

    #[test]
    fn decrypt_pending_hi_inside_and_beyond_32_bits() {
        let keys = keys();
        let pending = |lo: i64, hi: u64| credit(&keys, &zero(), &zero(), lo, hi);
        let (lo, hi) = pending(7, u32::MAX as u64);
        assert_eq!(
            keys.decrypt_pending(&lo, &hi),
            Ok(7 + ((u32::MAX as u64) << 16))
        );
        // Needs the wider search: hi just past 32 bits.
        let wide = (1u64 << 32) + 5;
        let (lo, hi) = pending(9, wide);
        assert_eq!(keys.decrypt_pending(&lo, &hi), Ok(9 + (wide << 16)));
        let (lo, hi) = pending(0, 1 << 40);
        assert_eq!(
            keys.decrypt_pending(&lo, &hi),
            Err(PendingError::HiTooLarge)
        );
    }

    #[test]
    fn decrypt_pending_wrong_key_and_garbage_fail() {
        let keys = keys();
        let (lo, hi) = credit(&keys, &zero(), &zero(), 100, 0);
        assert_eq!(
            other_keys().decrypt_pending(&lo, &hi),
            Err(PendingError::LoUndecryptable)
        );
        assert_eq!(
            keys.decrypt_pending(&[0xff; 64], &hi),
            Err(PendingError::Invalid)
        );
    }

    #[test]
    fn decrypt_aes_confirmed_by_opening_check() {
        let keys = keys();
        let mut account = SimAccount::new(&keys);
        account.credit(&keys, 70_000, 5_000);
        account.credit(&keys, 3, 0);
        account.apply();
        account.debit(&keys, 1_000);
        let x = 65_000 + 3 - 1_000;
        let aes = encrypt_aes(&keys, x);
        assert_eq!(keys.decrypt_aes(&aes), Some(x));
        assert!(keys.opens_to(&account.available, x));
        assert!(!keys.opens_to(&account.available, x + 1));
        assert!(!keys.opens_to(&account.available, x - 1));
        // A stale copy decrypts, but to a value the ciphertext does not open to.
        let stale = keys.decrypt_aes(&encrypt_aes(&keys, x - 3)).unwrap();
        assert!(!keys.opens_to(&account.available, stale));
        assert_eq!(other_keys().decrypt_aes(&aes), None);
        assert!(!other_keys().opens_to(&account.available, x));
        assert_eq!(keys.decrypt_aes(&[0u8; 36]), None);
    }

    #[test]
    fn signed_gap_reads_both_signs_and_the_wide_range() {
        let keys = keys();
        let mut account = SimAccount::new(&keys);
        account.credit(&keys, 70_000, 0);
        account.apply();
        // Copy below, at and above the ciphertext's value.
        assert_eq!(
            keys.decrypt_signed_gap(&account.available, 69_000),
            Some(1_000)
        );
        assert_eq!(keys.decrypt_signed_gap(&account.available, 70_000), Some(0));
        assert_eq!(
            keys.decrypt_signed_gap(&account.available, 71_234),
            Some(-1_234)
        );
        // Past 32 bits needs the wide search, either sign.
        let wide = (1u64 << 32) + 5;
        let big = encrypt(&keys, wide + 7);
        assert_eq!(keys.decrypt_signed_gap(&big, 7), Some(wide as i64));
        assert_eq!(
            keys.decrypt_signed_gap(&encrypt(&keys, 7), wide + 7),
            Some(-(wide as i64))
        );
        assert_eq!(keys.decrypt_signed_gap(&[0xff; 64], 0), None);
    }

    #[test]
    fn opens_to_rejects_garbage() {
        assert!(!keys().opens_to(&[0xff; 64], 0));
    }

    #[test]
    fn keys_from_json_round_trips() {
        let keys = keys();
        let parsed = Keys::from_json(&keys.to_json()).unwrap();
        assert_eq!(parsed.pubkey(), keys.pubkey());
    }

    #[test]
    fn keys_from_json_rejects_malformed_and_wrong_lengths() {
        let b64 = |n: usize| BASE64.encode(vec![1u8; n]);
        let cases = [
            ("not json".to_string(), KeysError::Malformed),
            (
                r#"{"elgamal_secret":"x"}"#.to_string(),
                KeysError::Malformed,
            ),
            (
                format!(r#"{{"elgamal_secret":"!!","aes_key":"{}"}}"#, b64(16)),
                KeysError::Malformed,
            ),
            (
                format!(
                    r#"{{"elgamal_secret":"{}","aes_key":"{}"}}"#,
                    b64(31),
                    b64(16)
                ),
                KeysError::ElGamalLength,
            ),
            (
                format!(
                    r#"{{"elgamal_secret":"{}","aes_key":"{}"}}"#,
                    b64(32),
                    b64(15)
                ),
                KeysError::AesLength,
            ),
            (
                format!(
                    r#"{{"elgamal_secret":"{}","aes_key":"{}"}}"#,
                    BASE64.encode([0xffu8; 32]),
                    b64(16)
                ),
                KeysError::ElGamalInvalid,
            ),
        ];
        for (json, want) in cases {
            assert_eq!(Keys::from_json(&json).unwrap_err(), want, "{json}");
        }
    }

    #[test]
    fn key_errors_name_the_variable_only() {
        let secret = BASE64.encode([0xffu8; 32]);
        let json = format!(r#"{{"elgamal_secret":"{secret}","aes_key":"AAAA"}}"#);
        let message = Keys::from_json(&json).unwrap_err().to_string();
        assert!(message.contains(KEYS_ENV));
        assert!(!message.contains(&secret));
        assert!(!format!("{:?}", keys()).contains(&keys_json()[20..40]));
    }

    #[test]
    fn keys_from_env_valid_missing_blank_and_malformed() {
        // One test owns the variable so parallel tests cannot race on it.
        std::env::remove_var(KEYS_ENV);
        assert!(Keys::from_env().unwrap().is_none());
        std::env::set_var(KEYS_ENV, "  ");
        assert!(Keys::from_env().unwrap().is_none());
        std::env::set_var(KEYS_ENV, keys_json());
        assert_eq!(Keys::from_env().unwrap().unwrap().pubkey(), keys().pubkey());
        std::env::set_var(KEYS_ENV, "{");
        assert_eq!(Keys::from_env().unwrap_err(), KeysError::Malformed);
        std::env::remove_var(KEYS_ENV);
    }

    #[test]
    fn key_parts_zeroize_on_drop() {
        // Both key types derive Zeroize with a drop hook in zk-sdk.
        fn zeroizes<T: Zeroize>() -> bool {
            std::mem::needs_drop::<T>()
        }
        assert!(zeroizes::<ElGamalSecretKey>());
        assert!(zeroizes::<AeKey>());
        assert!(std::mem::needs_drop::<Keys>());
    }

    #[test]
    fn split_is_16_and_32_bits() {
        assert_eq!(split(70_000), (4_464, 1));
        assert_eq!(split(MAX_CONFIDENTIAL_AMOUNT), (LO_MAX, u32::MAX as u64));
    }

    /// The zk-sdk's own single-threaded search, the path every amount used before.
    fn single_threaded(keys: &Keys, ciphertext: &Ciphertext) -> Option<u64> {
        ElGamalCiphertext::from_bytes(ciphertext)?.decrypt_u32(keys.elgamal.secret())
    }

    #[test]
    fn threaded_search_returns_exactly_what_the_single_threaded_one_does() {
        let keys = keys();
        let mut values = vec![0, 1, LO_MAX, 1 << 16, (1 << 32) - 1, 4_464, 65_000];
        values.extend((0..8).map(|i| 0x9E37_79B9u64.wrapping_mul(i + 1) & 0xFFFF_FFFF));
        for value in values {
            let ciphertext = encrypt(&keys, value);
            let new = keys.decrypt_u32(&ElGamalCiphertext::from_bytes(&ciphertext).unwrap());
            assert_eq!(new, single_threaded(&keys, &ciphertext), "value {value}");
            assert_eq!(new, Some(value));
        }
        // Outside the range, and a negative lo (fee mints): both searches find nothing.
        let (negative_lo, _) = credit(&keys, &zero(), &zero(), -536, 1);
        for ciphertext in [encrypt(&keys, 1 << 32), negative_lo] {
            let new = keys.decrypt_u32(&ElGamalCiphertext::from_bytes(&ciphertext).unwrap());
            assert_eq!(new, None);
            assert_eq!(single_threaded(&keys, &ciphertext), None);
        }
        // The sign flip of that negative lo reads the same either way.
        let flipped =
            ElGamalCiphertext::default() - ElGamalCiphertext::from_bytes(&negative_lo).unwrap();
        assert_eq!(keys.decrypt_u32(&flipped), Some(536));
        assert_eq!(flipped.decrypt_u32(keys.elgamal.secret()), Some(536));
        // And a whole fee-mint deposit with a negative lo still reads its net amount.
        let (lo, hi) = net_of_fee(70_000, 5_000);
        let (lo_after, hi_after) = credit(&keys, &zero(), &zero(), lo, hi);
        assert_eq!(
            keys.decrypt_deposit(&zero(), &zero(), &lo_after, &hi_after),
            Some(65_000)
        );
    }

    /// Run with `--ignored --nocapture`: timing depends on the host.
    #[test]
    #[ignore]
    fn deposits_decrypt_at_least_20_per_second() {
        let keys = keys();
        let deposits: Vec<_> = (0..40u64)
            .map(|i| {
                let (lo, hi) = split(1 + i * 7_919);
                credit(&keys, &zero(), &zero(), lo as i64, hi)
            })
            .collect();
        let start = std::time::Instant::now();
        for (i, (lo, hi)) in deposits.iter().enumerate() {
            assert_eq!(
                keys.decrypt_deposit(&zero(), &zero(), lo, hi),
                Some(1 + i as u64 * 7_919)
            );
        }
        let rate = deposits.len() as f64 / start.elapsed().as_secs_f64();
        println!("decrypt_deposit: {rate:.1} deposits/s");
        assert!(rate >= 20.0, "{rate:.1} deposits/s");
    }
}
