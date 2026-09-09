use crate::{
    state_utils::{
        assert_get_or_add_operator, assert_get_or_allow_mint, assert_get_or_create_instance,
        assert_get_or_release_funds, assert_get_or_rotate_bitmap,
    },
    utils::{
        assert_program_error, set_mint, setup_test_balances, TestContext, NONCE_ALREADY_USED_ERROR,
        NONCE_OUTSIDE_CURRENT_GENERATION_ERROR,
    },
};

use solana_sdk::signature::{Keypair, Signer};
use spl_token::ID as TOKEN_PROGRAM_ID;

const LARGE_DEPOSIT: u64 = 10_000_000;
const RELEASE_AMOUNT: u64 = 100_000;

#[test]
fn test_double_spend_same_nonce_after_bitmap_rotation() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let operator = Keypair::new();
    let user = Keypair::new();
    let mint = Keypair::new();
    let instance_seed = Keypair::new();

    set_mint(&mut context, &mint.pubkey());

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");

    assert_get_or_allow_mint(
        &mut context,
        &admin,
        &instance_pda,
        &mint.pubkey(),
        false,
        false,
    )
    .expect("AllowMint should succeed");

    let (operator_pda, _) = assert_get_or_add_operator(
        &mut context,
        &admin,
        &instance_pda,
        &operator.pubkey(),
        false,
        false,
    )
    .expect("AddOperator should succeed");

    setup_test_balances(
        &mut context,
        &user,
        &instance_pda,
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
        0,
        LARGE_DEPOSIT,
    );

    let nonce: u64 = 42;

    assert_get_or_release_funds(
        &mut context,
        &operator,
        &instance_pda,
        &operator_pda,
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
        RELEASE_AMOUNT,
        &user.pubkey(),
        nonce,
        false,
    )
    .expect("First release with nonce 42 should succeed");

    assert_get_or_rotate_bitmap(&mut context, &operator, &instance_pda, &operator_pda, false)
        .expect("RotateBitmap should succeed");

    // After rotation, generation 1 expects nonces 65536..131071.
    // Nonce 42 belongs to generation 0 and should be rejected even though
    // the rotation cleared its bit.
    context.warp_to_slot(2);

    let result = assert_get_or_release_funds(
        &mut context,
        &operator,
        &instance_pda,
        &operator_pda,
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
        RELEASE_AMOUNT,
        &user.pubkey(),
        nonce,
        false,
    );

    assert_program_error(result, NONCE_OUTSIDE_CURRENT_GENERATION_ERROR);
}

#[test]
fn test_double_spend_bitmap_rejects_used_nonce() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let operator = Keypair::new();
    let user = Keypair::new();
    let mint = Keypair::new();
    let instance_seed = Keypair::new();

    set_mint(&mut context, &mint.pubkey());

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");

    assert_get_or_allow_mint(
        &mut context,
        &admin,
        &instance_pda,
        &mint.pubkey(),
        false,
        false,
    )
    .expect("AllowMint should succeed");

    let (operator_pda, _) = assert_get_or_add_operator(
        &mut context,
        &admin,
        &instance_pda,
        &operator.pubkey(),
        false,
        false,
    )
    .expect("AddOperator should succeed");

    setup_test_balances(
        &mut context,
        &user,
        &instance_pda,
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
        0,
        LARGE_DEPOSIT,
    );

    let nonce: u64 = 42;

    assert_get_or_release_funds(
        &mut context,
        &operator,
        &instance_pda,
        &operator_pda,
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
        RELEASE_AMOUNT,
        &user.pubkey(),
        nonce,
        false,
    )
    .expect("First release with nonce 42 should succeed");

    // Replay the same nonce without rotating. Its bit is set, so the bitmap
    // itself prevents the double-spend, independent of generation validation.
    context.warp_to_slot(2);

    let result = assert_get_or_release_funds(
        &mut context,
        &operator,
        &instance_pda,
        &operator_pda,
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
        RELEASE_AMOUNT,
        &user.pubkey(),
        nonce,
        false,
    );

    assert_program_error(result, NONCE_ALREADY_USED_ERROR);
}

#[test]
fn test_double_spend_sequential_releases_then_replay() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let operator = Keypair::new();
    let user = Keypair::new();
    let mint = Keypair::new();
    let instance_seed = Keypair::new();

    set_mint(&mut context, &mint.pubkey());

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");

    assert_get_or_allow_mint(
        &mut context,
        &admin,
        &instance_pda,
        &mint.pubkey(),
        false,
        false,
    )
    .expect("AllowMint should succeed");

    let (operator_pda, _) = assert_get_or_add_operator(
        &mut context,
        &admin,
        &instance_pda,
        &operator.pubkey(),
        false,
        false,
    )
    .expect("AddOperator should succeed");

    setup_test_balances(
        &mut context,
        &user,
        &instance_pda,
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
        0,
        LARGE_DEPOSIT,
    );

    for nonce in [42u64, 43, 44] {
        assert_get_or_release_funds(
            &mut context,
            &operator,
            &instance_pda,
            &operator_pda,
            &mint.pubkey(),
            &TOKEN_PROGRAM_ID,
            RELEASE_AMOUNT,
            &user.pubkey(),
            nonce,
            false,
        )
        .unwrap_or_else(|_| panic!("Release with nonce {} should succeed", nonce));
    }

    // Replay nonce 42 after three sequential releases. Nonces 42, 43 and 44 all
    // live in the same bitmap byte, so this also pins that setting a neighbour
    // does not clear an earlier bit.
    context.warp_to_slot(2);

    let replay_nonce: u64 = 42;

    let result = assert_get_or_release_funds(
        &mut context,
        &operator,
        &instance_pda,
        &operator_pda,
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
        RELEASE_AMOUNT,
        &user.pubkey(),
        replay_nonce,
        false,
    );

    assert_program_error(result, NONCE_ALREADY_USED_ERROR);
}
