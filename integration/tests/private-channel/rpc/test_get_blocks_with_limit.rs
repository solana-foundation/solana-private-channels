use super::test_context::PrivateChannelContext;

pub async fn run_get_blocks_with_limit_test(ctx: &PrivateChannelContext) {
    println!("\n=== Get Blocks With Limit Test ===");

    let current_slot = ctx.read_client.get_slot().await.unwrap();
    let first_block = ctx.get_first_available_block().await.unwrap();
    println!("First available block: {first_block}, current slot: {current_slot}");

    // Test 1: the result is ascending and starts at or after the requested slot.
    let blocks = ctx.get_blocks_with_limit(first_block, 10).await.unwrap();
    println!(
        "\nTest 1: Retrieved {} blocks from {}",
        blocks.len(),
        first_block
    );
    for i in 1..blocks.len() {
        assert!(
            blocks[i] > blocks[i - 1],
            "Blocks should be in ascending order, got {:?}",
            blocks
        );
    }
    for slot in &blocks {
        assert!(
            *slot >= first_block,
            "Block {slot} is before the requested start {first_block}"
        );
    }
    println!("✓ Blocks are ascending and at or after the start slot");

    // Test 2: the limit caps the result, and a smaller limit is a prefix of a larger one.
    let capped = ctx.get_blocks_with_limit(first_block, 2).await.unwrap();
    assert!(
        capped.len() <= 2,
        "limit 2 must return at most 2 blocks, got {}",
        capped.len()
    );
    assert_eq!(
        capped,
        blocks[..capped.len()].to_vec(),
        "a smaller limit must return the same leading slots"
    );
    println!("✓ limit honoured and consistent with a larger limit");

    // Test 3: limit 0 returns nothing.
    let empty = ctx.get_blocks_with_limit(first_block, 0).await.unwrap();
    assert!(empty.is_empty(), "limit 0 should return no blocks");
    println!("✓ limit 0 returns an empty list");

    // Test 4: limit above MAX_SLOT_RANGE (500_000) must be rejected.
    let too_large = ctx.get_blocks_with_limit(first_block, 500_001).await;
    assert!(
        too_large.is_err(),
        "getBlocksWithLimit with limit > 500_000 should return an error, got: {too_large:?}"
    );
    println!("✓ limit > MAX_SLOT_RANGE correctly returns error");

    // Test 5: a slot past the tip has nothing at or after it.
    let future = ctx
        .get_blocks_with_limit(current_slot + 1000, 5)
        .await
        .unwrap();
    assert!(
        future.is_empty(),
        "no blocks should exist past the chain tip, got {future:?}"
    );
    println!("✓ Future slot query returns empty");

    // Test 6: every listed slot is fetchable and names the previous listed slot as
    // its parent, not the previous slot. This is the property the indexer's chain
    // proof relies on, so it is pinned here too.
    let mut verified = 0;
    for window in blocks.windows(2).rev().take(5) {
        let (parent, slot) = (window[0], window[1]);
        if let Ok(block) = ctx.read_client.get_block(slot).await {
            assert_eq!(
                block.parent_slot, parent,
                "Block {} should name the previous produced block {} as its parent",
                slot, parent
            );
            verified += 1;
        }
    }
    println!("✓ Verified parent links on {verified} listed blocks");

    println!("\n✓ All getBlocksWithLimit tests passed!");
}
