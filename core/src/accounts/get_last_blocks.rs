use {
    super::{
        postgres::PostgresAccountsDB,
        traits::{AccountsDB, BlockInfo},
    },
    anyhow::{bail, ensure, Context, Result},
    sqlx::Row,
    tracing::warn,
};

/// The newest `limit` blocks, oldest first. A count of blocks, not a span of
/// slots: the blockhash window is `max_blockhashes` blocks, and a slot range that
/// wide holds far fewer of them once idle ticks stop producing one each.
///
/// The dedup rebuild reads this, so what comes back is always a contiguous run
/// ending at the chain tip: blocks at or below a gap are dropped with a warning,
/// and a window short of the durable tip fails instead.
pub async fn get_last_blocks(db: &AccountsDB, limit: usize) -> Result<Vec<BlockInfo>> {
    match db {
        AccountsDB::Postgres(postgres_db) => get_last_blocks_postgres(postgres_db, limit).await,
        // Served from the source of truth: the cache cannot express which blocks
        // it is missing, and this path feeds the dedup rebuild, where a dropped
        // block means a replay slips through.
        AccountsDB::Redis(redis_db) => get_last_blocks_postgres(&redis_db.fallback, limit).await,
    }
}

async fn get_last_blocks_postgres(db: &PostgresAccountsDB, limit: usize) -> Result<Vec<BlockInfo>> {
    let pool = db.pool.clone();

    // The rows and the counter are compared below, so they are read in one
    // snapshot. Read committed takes a fresh snapshot per statement, which would
    // straddle a concurrent block and report a healthy ledger as truncated.
    let mut tx = pool
        .begin()
        .await
        .context("Failed to open the recent-blocks read transaction")?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *tx)
        .await
        .context("Failed to pin the recent-blocks read to one snapshot")?;

    let rows = sqlx::query("SELECT data FROM blocks ORDER BY slot DESC LIMIT $1")
        .bind(limit as i64)
        .fetch_all(&mut *tx)
        .await
        .context("Failed to query the most recent blocks")?;

    // The raw counter, not the height reader: that one falls back to the highest
    // slot when the key is gone, which would compare a slot against a height.
    let durable_height = super::get_block_height::read_block_height_counter(&mut *tx).await?;

    tx.commit()
        .await
        .context("Failed to close the recent-blocks read transaction")?;

    let mut blocks = Vec::with_capacity(rows.len());
    for row in rows.into_iter().rev() {
        let data: Vec<u8> = row.get("data");
        // This path feeds the dedup rebuild, so a decode failure fails closed
        // rather than silently seeding a short cache.
        let block = bincode::deserialize::<BlockInfo>(&data)
            .context("Failed to deserialize a recent block (likely pre-upgrade block data; wipe the DB or add a migration shim)")?;
        blocks.push(block);
    }

    verified_dedup_tail(blocks, durable_height)
}

/// The contiguous tail of the chain, which is the part of the restored window
/// that can be trusted. Heights are checked rather than slots, because the writer
/// bumps the height once per stored block while idle ticks advance the slot.
fn verified_dedup_tail(
    mut blocks: Vec<BlockInfo>,
    durable_height: Option<u64>,
) -> Result<Vec<BlockInfo>> {
    if blocks.is_empty() {
        // An empty table is a fresh ledger only when no counter claims otherwise.
        if let Some(durable) = durable_height {
            bail!(
                "the ledger has no block rows but the durable block height counter is {durable}, \
                 so every block in the dedup window is missing; restore from backup"
            );
        }
        return Ok(blocks);
    }

    let mut heights = Vec::with_capacity(blocks.len());
    for block in &blocks {
        // Production writes a height with every block, so an absent one is a
        // corrupt row rather than a gap, and nothing about it can be proven.
        let height = block.block_height.ok_or_else(|| {
            anyhow::anyhow!(
                "the block at slot {} has no block height, so the dedup window cannot be proven \
                 complete",
                block.slot
            )
        })?;
        heights.push(height);
    }

    let mut last_gap: Option<usize> = None;
    for index in 1..heights.len() {
        if Some(heights[index]) != heights[index - 1].checked_add(1) {
            last_gap = Some(index);
        }
    }

    let newest_height = *heights.last().expect("the slice is non-empty");
    let newest_slot = blocks.last().expect("the slice is non-empty").slot;
    match durable_height {
        // A gap at the top is the replay hole: those blocks' transactions named
        // hashes that survive, so the hashes stay live with no record of their use.
        Some(durable) => ensure!(
            newest_height == durable,
            "the newest restored block at slot {newest_slot} has height {newest_height} but the \
             durable block height counter is {durable}, so the dedup window is not the chain tip; \
             restore from backup",
        ),
        // Deliberately no fallback to the highest slot the way the block height
        // reader has: a substitute here would wave a lost counter through.
        None => bail!(
            "the durable block height counter is missing while the blocks table still holds rows, \
             so the dedup window cannot be proven complete; a backup of this ledger is missing it \
             too, so put the block_height metadata counter back from the newest stored block's \
             height once that block is confirmed to be the chain tip"
        ),
    }

    // Everything at or below a gap is dropped, which takes those blockhashes out
    // of the live set, so the transactions that named them are refused as unknown
    // rather than replayed.
    if let Some(gap) = last_gap {
        warn!(
            "Dedup: the restore window has a gap between height {} and height {}, so the {gap} \
             blocks at or below it are dropped and transactions carrying their blockhashes will be \
             rejected as unknown inside their published lastValidBlockHeight",
            heights[gap - 1],
            heights[gap],
        );
        return Ok(blocks.split_off(gap));
    }

    Ok(blocks)
}

#[cfg(test)]
mod tests {
    use {super::*, crate::test_helpers::create_test_block_info, solana_sdk::hash::Hash};

    fn block(slot: u64, height: Option<u64>) -> BlockInfo {
        let mut block = create_test_block_info(slot, Hash::new_unique());
        block.block_height = height;
        block
    }

    /// What a window shape must produce: either the heights that survive, or the
    /// fragments the fatal error must name.
    enum Expected {
        Kept(&'static [u64]),
        Fatal(&'static [&'static str]),
    }

    /// A case name, the (slot, height) rows to check, the durable counter, and
    /// what the restore must make of them.
    type WindowCase = (
        &'static str,
        &'static [(u64, Option<u64>)],
        Option<u64>,
        Expected,
    );

    /// One table over every window shape the restore can see. A gap below the tip
    /// keeps only the contiguous suffix; a gap at the tip and a lost counter are
    /// still fatal, because those blocks' hashes are still live.
    #[test]
    fn dedup_tail_keeps_the_contiguous_suffix_and_refuses_a_broken_tip() {
        use Expected::{Fatal, Kept};

        let cases: &[WindowCase] = &[
            ("fresh ledger", &[], None, Kept(&[])),
            (
                "rows gone under a live tip",
                &[],
                Some(0),
                Fatal(&["no block rows"]),
            ),
            ("genesis only", &[(0, Some(0))], Some(0), Kept(&[0])),
            (
                "sparse slots, contiguous heights",
                &[(0, Some(0)), (10, Some(1)), (20, Some(2)), (25, Some(3))],
                Some(3),
                Kept(&[0, 1, 2, 3]),
            ),
            (
                "truncated prefix",
                &[(50, Some(5)), (60, Some(6)), (70, Some(7))],
                Some(7),
                Kept(&[5, 6, 7]),
            ),
            (
                "interior gap",
                &[(0, Some(0)), (10, Some(1)), (30, Some(3))],
                Some(3),
                Kept(&[3]),
            ),
            (
                "gap at the very start",
                &[(0, Some(0)), (30, Some(3)), (40, Some(4)), (50, Some(5))],
                Some(5),
                Kept(&[3, 4, 5]),
            ),
            (
                "two gaps",
                &[(0, Some(0)), (20, Some(2)), (30, Some(3)), (60, Some(6))],
                Some(6),
                Kept(&[6]),
            ),
            (
                "gap immediately below the tip",
                &[(0, Some(0)), (10, Some(1)), (20, Some(2)), (60, Some(6))],
                Some(6),
                Kept(&[6]),
            ),
            (
                "newest row missing",
                &[(0, Some(0)), (10, Some(1)), (20, Some(2))],
                Some(3),
                Fatal(&["counter is 3"]),
            ),
            (
                "gap below a missing tip",
                &[(0, Some(0)), (30, Some(3))],
                Some(4),
                Fatal(&["counter is 4"]),
            ),
            (
                "counter behind the tip",
                &[(0, Some(0)), (10, Some(1)), (20, Some(2))],
                Some(1),
                Fatal(&["counter is 1"]),
            ),
            (
                "counter missing with rows",
                &[(0, Some(0))],
                None,
                Fatal(&["counter is missing"]),
            ),
            (
                "height absent",
                &[(0, Some(0)), (10, None)],
                Some(1),
                Fatal(&["no block height"]),
            ),
        ];

        for (name, rows, durable, expected) in cases {
            let blocks: Vec<BlockInfo> = rows.iter().map(|(s, h)| block(*s, *h)).collect();
            let result = verified_dedup_tail(blocks, *durable);

            match (result, expected) {
                (Ok(kept), Kept(heights)) => {
                    let kept: Vec<Option<u64>> = kept.iter().map(|b| b.block_height).collect();
                    let want: Vec<Option<u64>> = heights.iter().map(|h| Some(*h)).collect();
                    assert_eq!(kept, want, "{name}: the wrong blocks survived");
                }
                (Ok(_), Fatal(_)) => panic!("{name}: the window must be rejected, but it passed"),
                (Err(e), Kept(_)) => {
                    panic!("{name}: the window must restore, but it was rejected with {e}")
                }
                (Err(e), Fatal(fragments)) => {
                    let message = e.to_string();
                    for fragment in *fragments {
                        assert!(
                            message.contains(fragment),
                            "{name}: the error must name {fragment:?}, got {message:?}"
                        );
                    }
                }
            }
        }
    }

    /// A lost metadata counter is not recoverable from a backup of the same
    /// ledger, so the error must not send the operator to one.
    #[test]
    fn a_lost_counter_is_not_a_restore_from_backup() {
        let error = verified_dedup_tail(vec![block(0, Some(0))], None)
            .expect_err("a window with no durable counter must not restore")
            .to_string();

        assert!(
            !error.contains("restore from backup"),
            "a backup carries the same missing counter, got {error:?}"
        );
        assert!(
            error.contains("block_height"),
            "the error must name the counter to put back, got {error:?}"
        );
    }
}
