use crate::{
    state_utils::{assert_create_dvp, setup_dvp},
    utils::{get_token_balance, TestContext},
};

#[test]
fn test_create_dvp_success() {
    let mut context = TestContext::new();
    let fixture = setup_dvp(&mut context, 0);

    assert_create_dvp(&mut context, &fixture);

    assert!(
        context.get_account(&fixture.swap_dvp).is_some(),
        "SwapDvp PDA must exist"
    );
    assert!(
        context.get_account(&fixture.dvp_ata_a).is_some(),
        "dvp_ata_a must exist"
    );
    assert!(
        context.get_account(&fixture.dvp_ata_b).is_some(),
        "dvp_ata_b must exist"
    );
    assert_eq!(get_token_balance(&context, &fixture.dvp_ata_a), 0);
    assert_eq!(get_token_balance(&context, &fixture.dvp_ata_b), 0);
}
