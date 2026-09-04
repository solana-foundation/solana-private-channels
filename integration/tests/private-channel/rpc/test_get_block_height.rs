use super::test_context::PrivateChannelContext;

pub async fn run_get_block_height_test(ctx: &PrivateChannelContext) {
    println!("\n=== Block Height Test ===");

    let height_before = ctx.get_block_height().await.unwrap();
    let slot = ctx.get_slot().await.unwrap();
    let height_after = ctx.get_block_height().await.unwrap();
    println!(
        "Height before: {}, slot: {}, height after: {}",
        height_before, slot, height_after
    );

    // Height counts blocks and the slot counts ticks, so the height trails the
    // slot and never overtakes it.
    assert!(
        height_after <= slot,
        "Block height {} must not exceed the slot {} read alongside it",
        height_after,
        slot
    );
    assert!(
        height_after >= height_before,
        "Block height must never go backwards: {} then {}",
        height_before,
        height_after
    );

    println!("✓ Block height test passed!");
}
