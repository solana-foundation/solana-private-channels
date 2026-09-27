use {
    super::{
        current_slot::CURRENT_SLOT_KEY, get_block_height::BLOCK_HEIGHT_KEY,
        get_latest_slot::LATEST_SLOT_KEY, postgres::PostgresAccountsDB, redis::RedisAccountsDB,
        traits::AccountsDB,
    },
    anyhow::{anyhow, Context, Result},
    solana_sdk::hash::Hash,
    sqlx::Row,
    std::str::FromStr,
    tracing::warn,
};

/// Metadata key holding the tip blockhash.
pub const LATEST_BLOCKHASH_KEY: &str = "latest_blockhash";

pub async fn get_latest_blockhash(db: &AccountsDB) -> Result<Hash> {
    match db {
        AccountsDB::Postgres(postgres_db) => get_latest_blockhash_postgres(postgres_db).await,
        AccountsDB::Redis(redis_db) => get_latest_blockhash_redis(redis_db).await,
    }
}

/// The tip slot, block height and blockhash, all read from one state of the ledger.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockhashSnapshot {
    pub slot: Option<u64>,
    pub block_height: Option<u64>,
    pub blockhash: Hash,
}

pub async fn get_blockhash_snapshot(db: &AccountsDB) -> Result<BlockhashSnapshot> {
    match db {
        AccountsDB::Postgres(postgres_db) => get_blockhash_snapshot_postgres(postgres_db).await,
        AccountsDB::Redis(redis_db) => get_blockhash_snapshot_redis(redis_db).await,
    }
}

/// One statement, so one snapshot, with the same fallbacks as the separate getters.
async fn get_blockhash_snapshot_postgres(db: &PostgresAccountsDB) -> Result<BlockhashSnapshot> {
    let row = sqlx::query(
        "SELECT (SELECT value FROM metadata WHERE key = $1),
                (SELECT value FROM metadata WHERE key = $2),
                (SELECT value FROM metadata WHERE key = $3),
                (SELECT value FROM metadata WHERE key = $4),
                (SELECT MAX(slot) FROM blocks)",
    )
    .bind(CURRENT_SLOT_KEY)
    .bind(LATEST_SLOT_KEY)
    .bind(BLOCK_HEIGHT_KEY)
    .bind(LATEST_BLOCKHASH_KEY)
    .fetch_one(db.pool.as_ref())
    .await
    .context("Failed to query the blockhash snapshot")?;

    let decode = |i: usize| {
        row.get::<Option<Vec<u8>>, _>(i)
            .as_deref()
            .and_then(super::counter::decode)
    };
    let max_slot = row.get::<Option<i64>, _>(4).map(|slot| slot as u64);
    let bytes = row
        .get::<Option<Vec<u8>>, _>(3)
        .ok_or_else(|| anyhow!("No blockhash found in metadata table"))?;
    let hash_array: [u8; 32] = bytes
        .as_slice()
        .try_into()
        .map_err(|_| anyhow!("Invalid blockhash bytes length: {}", bytes.len()))?;
    Ok(BlockhashSnapshot {
        slot: decode(0).or(decode(1)).or(max_slot),
        block_height: decode(2).or(max_slot),
        blockhash: Hash::new_from_array(hash_array),
    })
}

/// The stamp, current slot, block height and blockhash, as one `MGET` returns them.
type CachedTip = (Option<Vec<u8>>, Option<u64>, Option<u64>, Option<String>);

/// One `MGET`, which sees one mirrored block. Anything short of a full answer reads
/// all three from Postgres, so a cached hash is never paired with a Postgres height.
async fn get_blockhash_snapshot_redis(db: &RedisAccountsDB) -> Result<BlockhashSnapshot> {
    let mut conn = db.connection.clone();
    let cached: redis::RedisResult<CachedTip> = redis::cmd("MGET")
        .arg(super::redis_coherence::DEPLOYMENT_ID_KEY)
        .arg(CURRENT_SLOT_KEY)
        .arg(BLOCK_HEIGHT_KEY)
        .arg(LATEST_BLOCKHASH_KEY)
        .query_async(&mut conn)
        .await;

    match cached {
        Ok((stamp, Some(slot), Some(block_height), Some(hash_str)))
            if db.stamp_is_current(stamp.as_ref()) =>
        {
            let blockhash = Hash::from_str(&hash_str)
                .map_err(|e| anyhow!("Invalid blockhash format: {}", e))?;
            Ok(BlockhashSnapshot {
                slot: Some(slot),
                block_height: Some(block_height),
                blockhash,
            })
        }
        Ok(_) => get_blockhash_snapshot_postgres(&db.fallback).await,
        Err(e) => {
            warn!("Failed to get the blockhash snapshot from Redis: {}", e);
            get_blockhash_snapshot_postgres(&db.fallback).await
        }
    }
}

async fn get_latest_blockhash_postgres(db: &PostgresAccountsDB) -> Result<Hash> {
    let pool = db.pool.clone();
    // Get the latest blockhash from metadata table
    let blockhash_bytes: Option<Vec<u8>> =
        sqlx::query_scalar("SELECT value FROM metadata WHERE key = 'latest_blockhash'")
            .fetch_optional(pool.as_ref())
            .await
            .context("Failed to query latest blockhash")?;

    if let Some(bytes) = blockhash_bytes {
        // The blockhash is stored as raw bytes (32 bytes)
        let hash_array: [u8; 32] = bytes
            .as_slice()
            .try_into()
            .map_err(|_| anyhow!("Invalid blockhash bytes length: {}", bytes.len()))?;
        Ok(Hash::new_from_array(hash_array))
    } else {
        Err(anyhow!("No blockhash found in metadata table"))
    }
}

async fn get_latest_blockhash_redis(db: &RedisAccountsDB) -> Result<Hash> {
    let cached = match db.get_trusted::<String>(LATEST_BLOCKHASH_KEY).await {
        Ok(hash_str) => hash_str,
        Err(e) => {
            warn!("Failed to get latest blockhash from Redis: {}", e);
            None
        }
    };

    if let Some(hash_str) = cached {
        return Hash::from_str(&hash_str).map_err(|e| anyhow!("Invalid blockhash format: {}", e));
    }

    // No cached blockhash is a miss, not a ledger without a tip.
    get_latest_blockhash_postgres(&db.fallback).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::{create_test_block_info, start_test_postgres};

    /// The snapshot keeps each old getter's fallback, and an absent hash stays an error.
    #[tokio::test(flavor = "multi_thread")]
    async fn blockhash_snapshot_fallbacks() {
        let (mut db, _pg) = start_test_postgres().await;
        let AccountsDB::Postgres(ref pg) = db else {
            unreachable!()
        };
        let pool = pg.pool.clone();
        let blockhash = Hash::new_unique();
        db.write_batch(
            &[],
            vec![],
            Some(crate::accounts::traits::BlockInfo {
                block_height: Some(3),
                ..create_test_block_info(7, blockhash)
            }),
        )
        .await
        .unwrap();
        let set = |key: &'static str, value: u64| {
            let pool = pool.clone();
            async move {
                sqlx::query("UPDATE metadata SET value = $2 WHERE key = $1")
                    .bind(key)
                    .bind(&super::super::counter::encode(value)[..])
                    .execute(pool.as_ref())
                    .await
                    .unwrap();
            }
        };
        let delete = |key: &'static str| {
            let pool = pool.clone();
            async move {
                sqlx::query("DELETE FROM metadata WHERE key = $1")
                    .bind(key)
                    .execute(pool.as_ref())
                    .await
                    .unwrap();
            }
        };
        // Distinct values so each fallback is visible: idle ticks moved the slot to 9.
        set("current_slot", 9).await;
        set("latest_slot", 8).await;

        // (label, keys deleted first, expected slot, expected height)
        let cases: [(&str, &[&'static str], u64, u64); 4] = [
            ("all keys", &[], 9, 3),
            ("no height counter", &["block_height"], 9, 7),
            ("no current slot", &["current_slot"], 8, 7),
            ("neither slot key", &["latest_slot"], 7, 7),
        ];
        for (label, deleted, slot, height) in cases {
            for key in deleted {
                delete(key).await;
            }
            let snapshot = db.get_blockhash_snapshot().await.unwrap();
            assert_eq!(
                (snapshot.slot, snapshot.block_height, snapshot.blockhash),
                (Some(slot), Some(height), blockhash),
                "{label}"
            );
            // Same answer the separate getters give.
            assert_eq!(
                (snapshot.slot, snapshot.block_height),
                (
                    db.get_current_slot().await.unwrap(),
                    db.get_block_height().await.unwrap()
                ),
                "{label}"
            );
        }

        delete(LATEST_BLOCKHASH_KEY).await;
        assert!(db.get_latest_blockhash().await.is_err());
        assert!(
            db.get_blockhash_snapshot().await.is_err(),
            "no hash is an error"
        );
    }
}
