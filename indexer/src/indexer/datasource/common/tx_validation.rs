//! Structural checks shared by the Yellowstone and RPC decoders. The runtime enforces every
//! rule here, so a successful transaction that breaks one was corrupted by the provider.

/// Length of an ed25519 transaction signature.
pub const SIGNATURE_LEN: usize = 64;

/// A transaction id must be a full signature, since it keys the row and its source event id.
pub fn check_signature(bytes: &[u8]) -> Result<(), String> {
    if bytes.len() != SIGNATURE_LEN {
        return Err(format!(
            "signature is {} bytes, expected {SIGNATURE_LEN}",
            bytes.len()
        ));
    }
    Ok(())
}

/// The loaded address lists must be exactly as long as the message's lookups ask for.
pub fn check_loaded_counts(
    expected: (usize, usize),
    loaded_writable: usize,
    loaded_readonly: usize,
) -> Result<(), String> {
    if expected != (loaded_writable, loaded_readonly) {
        return Err(format!(
            "lookups load {expected:?} (writable, readonly) addresses but meta carries ({loaded_writable}, {loaded_readonly})"
        ));
    }
    Ok(())
}

/// An instruction's program and accounts must resolve against the full key list.
pub fn check_instruction(
    num_keys: usize,
    program_id_index: u32,
    accounts: &[u8],
) -> Result<(), String> {
    check_key_index("program", program_id_index as usize, num_keys)?;
    for &index in accounts {
        check_key_index("account", index as usize, num_keys)?;
    }
    Ok(())
}

/// One index into the key list. A u32 index past u8 is named apart, since narrowing it would wrap.
fn check_key_index(what: &str, index: usize, num_keys: usize) -> Result<(), String> {
    if index > u8::MAX as usize {
        return Err(format!("{what} index {index} does not fit in a u8"));
    }
    if index >= num_keys {
        return Err(format!(
            "{what} index {index} is outside {num_keys} account keys"
        ));
    }
    Ok(())
}

/// An inner instruction set must name an existing top-level instruction.
pub fn check_inner_set_index(index: u32, top_level_count: usize) -> Result<(), String> {
    if index > u8::MAX as u32 {
        return Err(format!(
            "inner instruction set {index} does not fit in a u8"
        ));
    }
    if index as usize >= top_level_count {
        return Err(format!(
            "inner instruction set {index} names no top-level instruction (the transaction has {top_level_count})"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signature_must_be_exactly_64_bytes() {
        assert!(check_signature(&[1u8; SIGNATURE_LEN]).is_ok());
        for len in [0, 63, 65] {
            assert!(check_signature(&vec![1u8; len]).is_err(), "length {len}");
        }
    }

    #[test]
    fn loaded_counts_must_match_the_lookups() {
        assert!(check_loaded_counts((2, 1), 2, 1).is_ok());
        assert!(check_loaded_counts((0, 0), 0, 0).is_ok());
        assert!(check_loaded_counts((2, 1), 3, 1).is_err());
        assert!(check_loaded_counts((2, 1), 2, 0).is_err());
        assert!(check_loaded_counts((0, 0), 1, 0).is_err());
    }

    #[test]
    fn instruction_indices_must_resolve() {
        assert!(check_instruction(4, 3, &[0, 1, 2, 3]).is_ok());
        assert!(
            check_instruction(4, 4, &[0]).is_err(),
            "program index = key count"
        );
        assert!(
            check_instruction(4, 0, &[4]).is_err(),
            "account index = key count"
        );
        // A u32 index past u8 would silently truncate onto a real key, even inside the key list.
        let past_u8 = check_instruction(300, 256, &[]).unwrap_err();
        assert!(past_u8.contains("does not fit in a u8"), "{past_u8}");
        let past_keys = check_instruction(4, 4, &[]).unwrap_err();
        assert!(
            past_keys.contains("is outside 4 account keys"),
            "{past_keys}"
        );
    }

    #[test]
    fn inner_set_must_name_a_top_level_instruction() {
        assert!(check_inner_set_index(1, 2).is_ok());
        assert!(check_inner_set_index(2, 2).is_err());
        let past_u8 = check_inner_set_index(256, 300).unwrap_err();
        assert!(past_u8.contains("does not fit in a u8"), "{past_u8}");
    }
}
