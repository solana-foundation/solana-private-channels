use crate::{
    state_utils::{
        assert_create_dvp, assert_fund_a, assert_reject_dvp, setup_dvp, INITIAL_BALANCE,
    },
    utils::{get_token_balance, TestContext},
};

#[test]
fn test_reject_dvp_success() {
    let mut context = TestContext::new();
    let fixture = setup_dvp(&mut context, 0);
    assert_create_dvp(&mut context, &fixture);
    assert_fund_a(&mut context, &fixture);

    assert_reject_dvp(&mut context, &fixture, &fixture.user_b);

    assert_eq!(
        get_token_balance(&context, &fixture.user_a_ata_a),
        INITIAL_BALANCE
    );
    assert_eq!(
        get_token_balance(&context, &fixture.user_b_ata_b),
        INITIAL_BALANCE
    );
    assert!(context.get_account(&fixture.swap_dvp).is_none());
    assert!(context.get_account(&fixture.dvp_ata_a).is_none());
    assert!(context.get_account(&fixture.dvp_ata_b).is_none());
}
