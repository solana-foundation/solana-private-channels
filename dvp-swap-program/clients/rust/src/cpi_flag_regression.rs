//! Regression guard for a @codama/renderers-rust bug (present through v3.1.0)
//! that serialized CPI remaining-account flags in the wrong order. The
//! generated invoke helper must map tuple `.1` to is_writable and `.2` to
//! is_signer, matching the `add_remaining_account(account, is_writable,
//! is_signer)` builder. The fix is applied post-generation by
//! scripts/lib/patch-rust-cpi-account-flags.ts; this test fails if that patch
//! is dropped or stops matching.

const RECLAIM: &str = include_str!("generated/instructions/reclaim_dvp.rs");
const SETTLE: &str = include_str!("generated/instructions/settle_dvp.rs");
const CANCEL: &str = include_str!("generated/instructions/cancel_dvp.rs");
const REJECT: &str = include_str!("generated/instructions/reject_dvp.rs");
const CREATE: &str = include_str!("generated/instructions/create_dvp.rs");

fn assert_correct_flag_order(name: &str, src: &str) {
    // Security invariant: the buggy mapping (`.1` read as is_signer) must
    // never appear, in any instruction.
    assert!(
        !src.contains("is_signer: remaining_account.1,"),
        "{name}: buggy CPI remaining-account flag mapping (`.1` read as is_signer)"
    );
    // For instructions that actually emit remaining-account CPI scaffolding,
    // confirm the corrected order is present. Skipped otherwise so the test
    // does not couple to codama emitting that scaffolding for every
    // instruction.
    if src.contains("remaining_account.") {
        assert!(
            src.contains("is_writable: remaining_account.1,")
                && src.contains("is_signer: remaining_account.2,"),
            "{name}: expected `.1` -> is_writable and `.2` -> is_signer"
        );
    }
}

#[test]
fn cpi_remaining_account_flags_match_builder_tuple_order() {
    for (name, src) in [
        ("reclaim_dvp", RECLAIM),
        ("settle_dvp", SETTLE),
        ("cancel_dvp", CANCEL),
        ("reject_dvp", REJECT),
        ("create_dvp", CREATE),
    ] {
        assert_correct_flag_order(name, src);
    }
}
