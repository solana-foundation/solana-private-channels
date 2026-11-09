//! Fixed keys, ciphertext helpers and escrow event byte builders for tests.

use crate::{split, AeBytes, Ciphertext, Keys};
use solana_zk_sdk::encryption::elgamal::{ElGamalCiphertext, ElGamalPubkey};

/// The escrow's self-CPI event tag.
pub const EVENT_IX_TAG_LE: [u8; 8] = [0xe4, 0x45, 0xa5, 0x2e, 0x51, 0xcb, 0x9a, 0x1d];

pub const CONFIGURE_EVENT: u8 = 12;
pub const DEPOSIT_EVENT: u8 = 13;
pub const RELEASE_EVENT: u8 = 14;
pub const APPLY_EVENT: u8 = 15;
pub const MOVE_TO_CONFIDENTIAL_EVENT: u8 = 16;
pub const MOVE_TO_PUBLIC_EVENT: u8 = 17;

/// The instance keys every test uses.
pub fn keys() -> Keys {
    Keys::from_bytes(&scalar_bytes(1), &[2u8; 16]).expect("fixed keys")
}

/// A second key pair, for wrong-key cases.
pub fn other_keys() -> Keys {
    Keys::from_bytes(&scalar_bytes(3), &[4u8; 16]).expect("fixed keys")
}

/// [`keys`] as the JSON `ESCROW_CONFIDENTIAL_KEYS` holds.
pub fn keys_json() -> String {
    keys().to_json().to_string()
}

// A canonical scalar: small little-endian value, top bytes zero.
fn scalar_bytes(seed: u8) -> [u8; 32] {
    let mut bytes = [0u8; 32];
    bytes[0] = seed;
    bytes[1] = 0x5a;
    bytes
}

fn pubkey(keys: &Keys) -> ElGamalPubkey {
    ElGamalPubkey::try_from(keys.pubkey().as_slice()).expect("valid pubkey")
}

fn ct(bytes: &Ciphertext) -> ElGamalCiphertext {
    ElGamalCiphertext::from_bytes(bytes).expect("valid ciphertext")
}

/// `amount` encrypted under `keys` with fresh randomness.
pub fn encrypt(keys: &Keys, amount: u64) -> Ciphertext {
    pubkey(keys).encrypt(amount).to_bytes()
}

/// `amount` as Token-2022's AES copy of the available balance.
pub fn encrypt_aes(keys: &Keys, amount: u64) -> AeBytes {
    keys.aes().encrypt(amount).to_bytes()
}

/// Pending lo and hi after a credit of `lo_delta + 2^16 * hi_delta`, as Token-2022 adds it.
pub fn credit(
    keys: &Keys,
    lo_before: &Ciphertext,
    hi_before: &Ciphertext,
    lo_delta: i64,
    hi_delta: u64,
) -> (Ciphertext, Ciphertext) {
    let lo_change = ct(&encrypt(keys, lo_delta.unsigned_abs()));
    let lo_after = if lo_delta >= 0 {
        ct(lo_before) + lo_change
    } else {
        ct(lo_before) - lo_change
    };
    let hi_after = ct(hi_before) + ct(&encrypt(keys, hi_delta));
    (lo_after.to_bytes(), hi_after.to_bytes())
}

/// The lo and hi parts a fee mint credits: each part of the amount minus that part of the fee.
pub fn net_of_fee(amount: u64, fee: u64) -> (i64, u64) {
    let (amount_lo, amount_hi) = split(amount);
    let (fee_lo, fee_hi) = split(fee);
    (amount_lo as i64 - fee_lo as i64, amount_hi - fee_hi)
}

/// The three confidential balances of one Token-2022 account.
pub struct SimAccount {
    pub available: Ciphertext,
    pub pending_lo: Ciphertext,
    pub pending_hi: Ciphertext,
}

impl SimAccount {
    pub fn new(keys: &Keys) -> Self {
        Self {
            available: encrypt(keys, 0),
            pending_lo: [0u8; 64],
            pending_hi: [0u8; 64],
        }
    }

    /// A credit to pending, net of `fee`. Returns the pending lo and hi before it.
    pub fn credit(&mut self, keys: &Keys, amount: u64, fee: u64) -> (Ciphertext, Ciphertext) {
        let before = (self.pending_lo, self.pending_hi);
        let (lo, hi) = net_of_fee(amount, fee);
        (self.pending_lo, self.pending_hi) =
            credit(keys, &self.pending_lo, &self.pending_hi, lo, hi);
        before
    }

    /// A release or MoveToPublic: `amount` leaves available.
    pub fn debit(&mut self, keys: &Keys, amount: u64) {
        self.available = (ct(&self.available) - ct(&encrypt(keys, amount))).to_bytes();
    }

    /// ApplyPendingBalance.
    pub fn apply(&mut self) {
        let shifted_hi = (1u64 << 16) * ct(&self.pending_hi);
        let applied = ct(&self.pending_lo) + shifted_hi;
        self.available = (ct(&self.available) + applied).to_bytes();
        self.pending_lo = [0u8; 64];
        self.pending_hi = [0u8; 64];
    }
}

fn event(discriminator: u8, capacity: usize) -> Vec<u8> {
    let mut data = Vec::with_capacity(capacity);
    data.extend_from_slice(&EVENT_IX_TAG_LE);
    data.push(discriminator);
    data
}

/// ConfigureConfidentialAccountEvent, 105 bytes.
pub fn configure_event(
    instance_seed: [u8; 32],
    mint: [u8; 32],
    elgamal_pubkey: [u8; 32],
) -> Vec<u8> {
    let mut data = event(CONFIGURE_EVENT, 105);
    data.extend_from_slice(&instance_seed);
    data.extend_from_slice(&mint);
    data.extend_from_slice(&elgamal_pubkey);
    data
}

/// ConfidentialDepositEvent, 401 bytes; the trailing credit counter is left at zero.
#[allow(clippy::too_many_arguments)]
pub fn deposit_event(
    instance_seed: [u8; 32],
    user: [u8; 32],
    recipient: [u8; 32],
    mint: [u8; 32],
    lo_before: &Ciphertext,
    hi_before: &Ciphertext,
    lo_after: &Ciphertext,
    hi_after: &Ciphertext,
) -> Vec<u8> {
    let mut data = event(DEPOSIT_EVENT, 401);
    for part in [instance_seed, user, recipient, mint] {
        data.extend_from_slice(&part);
    }
    for part in [lo_before, hi_before, lo_after, hi_after] {
        data.extend_from_slice(part);
    }
    data.extend_from_slice(&0u64.to_le_bytes());
    data
}

/// ConfidentialReleaseFundsEvent, 265 bytes.
pub fn release_event(
    instance_seed: [u8; 32],
    operator: [u8; 32],
    user: [u8; 32],
    mint: [u8; 32],
    amount_lo: &Ciphertext,
    amount_hi: &Ciphertext,
) -> Vec<u8> {
    let mut data = event(RELEASE_EVENT, 265);
    for part in [instance_seed, operator, user, mint] {
        data.extend_from_slice(&part);
    }
    data.extend_from_slice(amount_lo);
    data.extend_from_slice(amount_hi);
    data
}

/// ApplyPendingBalanceEvent, 241 bytes.
pub fn apply_event(
    instance_seed: [u8; 32],
    operator: [u8; 32],
    mint: [u8; 32],
    applied_lo: &Ciphertext,
    applied_hi: &Ciphertext,
    applied_credit_counter: u64,
) -> Vec<u8> {
    let mut data = event(APPLY_EVENT, 241);
    for part in [instance_seed, operator, mint] {
        data.extend_from_slice(&part);
    }
    data.extend_from_slice(applied_lo);
    data.extend_from_slice(applied_hi);
    data.extend_from_slice(&applied_credit_counter.to_le_bytes());
    data
}

fn move_event(
    discriminator: u8,
    instance_seed: [u8; 32],
    operator: [u8; 32],
    mint: [u8; 32],
    amount: u64,
) -> Vec<u8> {
    let mut data = event(discriminator, 113);
    for part in [instance_seed, operator, mint] {
        data.extend_from_slice(&part);
    }
    data.extend_from_slice(&amount.to_le_bytes());
    data
}

/// MoveToConfidentialEvent, 113 bytes.
pub fn move_to_confidential_event(
    instance_seed: [u8; 32],
    operator: [u8; 32],
    mint: [u8; 32],
    amount: u64,
) -> Vec<u8> {
    move_event(
        MOVE_TO_CONFIDENTIAL_EVENT,
        instance_seed,
        operator,
        mint,
        amount,
    )
}

/// MoveToPublicEvent, 113 bytes.
pub fn move_to_public_event(
    instance_seed: [u8; 32],
    operator: [u8; 32],
    mint: [u8; 32],
    amount: u64,
) -> Vec<u8> {
    move_event(MOVE_TO_PUBLIC_EVENT, instance_seed, operator, mint, amount)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_builders_match_the_program_lengths() {
        let z = [0u8; 32];
        let c = [0u8; 64];
        assert_eq!(configure_event(z, z, z).len(), 105);
        assert_eq!(deposit_event(z, z, z, z, &c, &c, &c, &c).len(), 401);
        assert_eq!(release_event(z, z, z, z, &c, &c).len(), 265);
        assert_eq!(apply_event(z, z, z, &c, &c, 0).len(), 241);
        assert_eq!(move_to_confidential_event(z, z, z, 0).len(), 113);
        assert_eq!(move_to_public_event(z, z, z, 0).len(), 113);
    }
}
