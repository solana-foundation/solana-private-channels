/// Encoding for the u64 counters kept in the `metadata` table, matching the
/// little-endian form `transaction_count` already uses.
pub fn encode(value: u64) -> [u8; 8] {
    value.to_le_bytes()
}

/// Decode a counter, returning `None` for anything that is not exactly 8 bytes.
pub fn decode(bytes: &[u8]) -> Option<u64> {
    let array: [u8; 8] = bytes.try_into().ok()?;
    Some(u64::from_le_bytes(array))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counter_round_trips() {
        for value in [0u64, 1, 150, u64::MAX] {
            assert_eq!(decode(&encode(value)), Some(value));
        }
    }

    #[test]
    fn wrong_length_decodes_to_none() {
        assert!(decode(&[]).is_none());
        assert!(decode(&[1, 2, 3]).is_none());
        assert!(decode(&[0u8; 9]).is_none());
    }
}
