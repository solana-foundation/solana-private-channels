use crate::{
    state_utils::{
        assert_create_dvp, assert_fund_a, assert_fund_b, setup_dvp, AMOUNT_A, AMOUNT_B,
        INITIAL_BALANCE,
    },
    utils::{get_token_balance, TestContext},
};

#[test]
fn test_fund_dvp_success() {
    let mut context = TestContext::new();
    let fixture = setup_dvp(&mut context, 0);
    assert_create_dvp(&mut context, &fixture);

    assert_fund_a(&mut context, &fixture);
    assert_eq!(get_token_balance(&context, &fixture.dvp_ata_a), AMOUNT_A);
    assert_eq!(
        get_token_balance(&context, &fixture.user_a_ata_a),
        INITIAL_BALANCE - AMOUNT_A
    );

    assert_fund_b(&mut context, &fixture);
    assert_eq!(get_token_balance(&context, &fixture.dvp_ata_b), AMOUNT_B);
    assert_eq!(
        get_token_balance(&context, &fixture.user_b_ata_b),
        INITIAL_BALANCE - AMOUNT_B
    );
}
