use crate::{
    state_utils::{
        assert_create_dvp, assert_fund_a, assert_fund_b, assert_settle_dvp, setup_dvp, AMOUNT_A,
        AMOUNT_B, INITIAL_BALANCE,
    },
    utils::{get_token_balance, TestContext},
};

#[test]
fn test_settle_dvp_success() {
    let mut context = TestContext::new();
    let fixture = setup_dvp(&mut context, 0);
    assert_create_dvp(&mut context, &fixture);
    assert_fund_a(&mut context, &fixture);
    assert_fund_b(&mut context, &fixture);

    assert_settle_dvp(&mut context, &fixture);

    // user_a paid asset, received cash; user_b paid cash, received asset.
    assert_eq!(
        get_token_balance(&context, &fixture.user_a_ata_a),
        INITIAL_BALANCE - AMOUNT_A
    );
    assert_eq!(get_token_balance(&context, &fixture.user_a_ata_b), AMOUNT_B);
    assert_eq!(get_token_balance(&context, &fixture.user_b_ata_a), AMOUNT_A);
    assert_eq!(
        get_token_balance(&context, &fixture.user_b_ata_b),
        INITIAL_BALANCE - AMOUNT_B
    );

    // SwapDvp + both escrow ATAs are closed.
    assert!(
        context.get_account(&fixture.swap_dvp).is_none(),
        "SwapDvp must be closed"
    );
    assert!(
        context.get_account(&fixture.dvp_ata_a).is_none(),
        "dvp_ata_a must be closed"
    );
    assert!(
        context.get_account(&fixture.dvp_ata_b).is_none(),
        "dvp_ata_b must be closed"
    );
}
