use crate::{
    state_utils::{
        assert_create_dvp, assert_fund_a, assert_reclaim_a, setup_dvp, AMOUNT_A, INITIAL_BALANCE,
    },
    utils::{get_token_balance, TestContext},
};

#[test]
fn test_reclaim_dvp_success() {
    let mut context = TestContext::new();
    let fixture = setup_dvp(&mut context, 0);
    assert_create_dvp(&mut context, &fixture);
    assert_fund_a(&mut context, &fixture);
    assert_eq!(get_token_balance(&context, &fixture.dvp_ata_a), AMOUNT_A);

    assert_reclaim_a(&mut context, &fixture);

    // Funds restored to user_a; DvP itself stays open.
    assert_eq!(get_token_balance(&context, &fixture.dvp_ata_a), 0);
    assert_eq!(
        get_token_balance(&context, &fixture.user_a_ata_a),
        INITIAL_BALANCE
    );
    assert!(
        context.get_account(&fixture.swap_dvp).is_some(),
        "SwapDvp stays open after reclaim"
    );
    assert!(
        context.get_account(&fixture.dvp_ata_a).is_some(),
        "escrow stays open after reclaim"
    );
}
