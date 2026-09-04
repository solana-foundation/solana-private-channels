use super::test_context::PrivateChannelContext;

pub async fn run_get_blocks_test(ctx: &PrivateChannelContext) {
    println!("\n=== Get Blocks Test ===");

    // Get the current slot
    let current_slot = ctx.read_client.get_slot().await.unwrap();
    println!("Current slot: {}", current_slot);

    // Get the first available block
    let first_block = ctx.get_first_available_block().await.unwrap();
    println!("First available block: {}", first_block);

    // Test 1: Get blocks from first to current slot
    println!(
        "\nTest 1: Get blocks from {} to {}",
        first_block, current_slot
    );
    let blocks = ctx
        .get_blocks(first_block, Some(current_slot))
        .await
        .unwrap();
    println!("Retrieved {} blocks", blocks.len());

    // Verify the blocks are in ascending order
    for i in 1..blocks.len() {
        assert!(
            blocks[i] > blocks[i - 1],
            "Blocks should be in ascending order"
        );
    }

    // Verify all blocks are within the requested range
    for slot in &blocks {
        assert!(
            *slot >= first_block && *slot <= current_slot,
            "Block {} is outside the requested range [{}, {}]",
            slot,
            first_block,
            current_slot
        );
    }
    println!("✓ Blocks are in correct order and within range");

    // Test 2: Get blocks without specifying end_slot
    println!("\nTest 2: Get blocks from {} without end_slot", first_block);
    let blocks_no_end = ctx.get_blocks(first_block, None).await.unwrap();
    println!("Retrieved {} blocks", blocks_no_end.len());

    // Should get similar or more blocks (if new blocks were produced)
    assert!(
        blocks_no_end.len() >= blocks.len(),
        "Should get at least as many blocks when not specifying end_slot"
    );
    println!(
        "✓ Retrieved {} blocks without end_slot",
        blocks_no_end.len()
    );

    // Test 3: Verify getBlock and getBlocks consistency
    println!("\nTest 3: Verify getBlock and getBlocks consistency");
    if !blocks.is_empty() {
        // Try to verify blocks with actual data
        // Note: Not all slots returned by getBlocks may have full block data stored
        let mut successful_verifications = 0;
        let target_verifications = 3.min(blocks.len());

        // Start from the end since more recent blocks are more likely to have data.
        // Walk consecutive pairs: blocks are sparse relative to slots, so a block
        // names the previous listed producer, not the previous slot.
        for window in blocks.windows(2).rev().take(10) {
            let (parent, slot) = (window[0], window[1]);
            match ctx.read_client.get_block(slot).await {
                Ok(block) => {
                    println!(
                        "✓ Block {} exists and contains {} transactions",
                        slot,
                        block.transactions.len()
                    );

                    assert_eq!(
                        block.parent_slot, parent,
                        "Block {} should name the previous produced block {} as its parent",
                        slot, parent
                    );

                    successful_verifications += 1;
                    if successful_verifications >= target_verifications {
                        break;
                    }
                }
                Err(_) => {
                    // Some slots don't have full block data, which is acceptable
                    continue;
                }
            }
        }

        if successful_verifications > 0 {
            println!(
                "✓ Verified {} blocks from getBlocks can be retrieved with getBlock",
                successful_verifications
            );
        } else {
            println!(
                "Note: No blocks with full data found, but getBlocks returned slots successfully"
            );
        }
    }

    // Test 4: Get blocks in a small range
    if blocks.len() > 5 {
        let start = blocks[0];
        let end = blocks[4];
        println!("\nTest 4: Get blocks in range [{}, {}]", start, end);
        let range_blocks = ctx.get_blocks(start, Some(end)).await.unwrap();
        println!("Retrieved {} blocks", range_blocks.len());

        assert!(range_blocks.len() <= 5, "Should not get more than 5 blocks");

        // Verify all returned blocks are in the range
        for slot in &range_blocks {
            assert!(
                *slot >= start && *slot <= end,
                "Block {} is outside range [{}, {}]",
                slot,
                start,
                end
            );
        }
        println!("✓ Range query returned correct blocks");
    }

    // Test 5: Query from a future slot (should return empty or error gracefully)
    let future_slot = current_slot + 1000;
    println!("\nTest 5: Query from future slot {}", future_slot);
    match ctx.get_blocks(future_slot, Some(future_slot + 10)).await {
        Ok(future_blocks) => {
            println!("Retrieved {} blocks (expected empty)", future_blocks.len());
            assert_eq!(
                future_blocks.len(),
                0,
                "Should return empty array for future slots"
            );
        }
        Err(e) => {
            println!("Query returned error (acceptable): {}", e);
        }
    }
    println!("✓ Future slot query handled correctly");

    // Test 6: end_slot < start_slot must return an error
    println!("\nTest 6: Query with end_slot < start_slot (expect error)");
    let inv_result = ctx.get_blocks(current_slot + 1, Some(current_slot)).await;
    assert!(
        inv_result.is_err(),
        "getBlocks with end_slot < start_slot should return an error, got: {:?}",
        inv_result
    );
    println!("✓ end_slot < start_slot correctly returns error");

    // Test 7: range > MAX_SLOT_RANGE (500_000) must return an error
    println!("\nTest 7: Query with range > MAX_SLOT_RANGE (expect error)");
    let large_range_result = ctx.get_blocks(0, Some(500_001)).await;
    assert!(
        large_range_result.is_err(),
        "getBlocks with range > 500_000 should return an error"
    );
    println!("✓ Range > MAX_SLOT_RANGE correctly returns error");

    println!("\n✓ All getBlocks tests passed!");
}
