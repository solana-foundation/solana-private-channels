use futures::future::BoxFuture;
use sqlx::{postgres::PgPoolOptions, Acquire, PgConnection, PgPool};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::{info, warn};

use crate::{
    error::StorageError,
    storage::common::models::{
        DbMint, DbMintStatus, DbObservedRelease, DbTransaction, HaltInfo, MintDbBalance,
        MintInFlightAmount, MintStatusAtSlot, StoredSig, TransactionStatus, TransactionType,
    },
    storage::common::storage::live_lock::{LiveLockMode, LIVE_STATE_LOCK_KEY},
    storage::common::storage::RequeueOutcome,
    storage::postgres::lock_connection::LockConnection,
    PostgresConfig,
};

mod transaction_cols {
    pub const ID: &str = "id";
    pub const SIGNATURE: &str = "signature";
    pub const SLOT: &str = "slot";
    pub const INITIATOR: &str = "initiator";
    pub const RECIPIENT: &str = "recipient";
    pub const MINT: &str = "mint";
    pub const AMOUNT: &str = "amount";
    pub const MEMO: &str = "memo";
    pub const STATUS: &str = "status";
    pub const TRANSACTION_TYPE: &str = "transaction_type";
    pub const WITHDRAWAL_NONCE: &str = "withdrawal_nonce";
    pub const CREATED_AT: &str = "created_at";
    pub const UPDATED_AT: &str = "updated_at";
    pub const PROCESSED_AT: &str = "processed_at";
    pub const COUNTERPART_SIGNATURE: &str = "counterpart_signature";
    pub const TRACE_ID: &str = "trace_id";
    pub const REMINT_SIGNATURES: &str = "remint_signatures";
    pub const REMINT_LAST_VALID_BLOCK_HEIGHTS: &str = "remint_last_valid_block_heights";
    pub const PENDING_REMINT_DEADLINE_AT: &str = "pending_remint_deadline_at";
    pub const FINALITY_CHECK_ATTEMPTS: &str = "finality_check_attempts";
    pub const RECOVERY_REQUEUE_ATTEMPTS: &str = "recovery_requeue_attempts";
    pub const INSTRUCTION_INDEX: &str = "instruction_index";
    pub const INNER_INDEX: &str = "inner_index";
    pub const LANDED_REMINT_SIGNATURE: &str = "landed_remint_signature";
    pub const RELEASE_REFUSED_ON_CHAIN: &str = "release_refused_on_chain";
}

/// ON CONFLICT target for the transactions composite uniqueness. inner_index is
/// NULL for top-level rows and a unique index treats NULLs as distinct, so it is
/// coalesced to -1 (an impossible position) to keep the triple `(signature,
/// instruction_index, inner_index)` collision-detecting for them too. Inner rows
/// carry a real >= 0 inner_index.
const TX_CONFLICT_TARGET: &str = "(signature, instruction_index, COALESCE(inner_index, -1))";

#[derive(Clone)]
pub struct PostgresDb {
    pool: PgPool,
    /// Installed once a sender wins the advisory lock. Shared across clones so
    /// every sender-owned write in the process routes to the one session that
    /// proves ownership. `None` in every other process and in tests that never
    /// take the lock, where those writes use the pool exactly as before.
    sender_fence: Arc<Mutex<Option<Arc<LockConnection>>>>,
}

/// Idle seconds before Postgres starts probing a lock session's socket, then the
/// probe spacing and how many may go unanswered. Together they reap a vanished
/// holder in under two minutes instead of the OS default of roughly two hours.
const LOCK_KEEPALIVE_IDLE_SECS: u32 = 60;
const LOCK_KEEPALIVE_INTERVAL_SECS: u32 = 15;
const LOCK_KEEPALIVE_COUNT: u32 = 3;

/// How long a statement on a lock session may wait for a lock another session holds.
/// A resync runs with the workers scaled to zero, so nothing should be queueing it; this
/// is long enough to outlast a passing query and short enough to fail fast otherwise.
const LOCK_WAIT_TIMEOUT_MS: &str = "10000";

/// Ask Postgres to reap this session quickly if the holder's host disappears.
///
/// A vanished host sends no FIN, so the backend sits in `recv()` holding the
/// advisory lock for hours while nothing is running. Every worker holds the
/// live-state key, so one such host would refuse every resync for that long.
/// Best effort: a unix socket or an unsupported platform ignores these.
pub async fn apply_lock_session_keepalives(conn: &mut PgConnection) {
    // `SET` takes no bind parameters, which would force the values into the statement text.
    let applied = sqlx::query(
        "SELECT set_config('tcp_keepalives_idle', $1, false),
                set_config('tcp_keepalives_interval', $2, false),
                set_config('tcp_keepalives_count', $3, false)",
    )
    .bind(LOCK_KEEPALIVE_IDLE_SECS.to_string())
    .bind(LOCK_KEEPALIVE_INTERVAL_SECS.to_string())
    .bind(LOCK_KEEPALIVE_COUNT.to_string())
    .execute(conn)
    .await;

    if let Err(e) = applied {
        warn!("Could not set TCP keepalives on the lock session: {e}");
    }
}

/// Bound how long fenced work waits for somebody else's table lock.
///
/// The drop needs ACCESS EXCLUSIVE on every table, so one unexpected session holding a
/// read is enough to queue it. Without this that wait is unbounded, and the heartbeat
/// reads the busy connection as alive, so a queued drop wedges the resync while it holds
/// the exclusive lock and every worker stays refused. Best effort; the client-side cap
/// still applies.
pub async fn apply_lock_session_lock_timeout(conn: &mut PgConnection) {
    // `SET` takes no bind parameters, which would force the value into the statement text.
    if let Err(e) = sqlx::query("SELECT set_config('lock_timeout', $1, false)")
        .bind(LOCK_WAIT_TIMEOUT_MS)
        .execute(conn)
        .await
    {
        warn!("Could not set lock_timeout on the lock session: {e}");
    }
}

/// Does *this* session still hold the advisory lock for `key`?
///
/// Not `pg_try_advisory_lock`: it returns true once we have lost the lock too,
/// silently re-taking it and hiding the gap we are looking for.
///
/// A bigint key lives in three columns, `classid` (high 32 bits), `objid` (low
/// 32) and `objsubid` (always 1). Match all three, since both sender roles share
/// a `classid`, and `pid` too, so the answer is "we hold it", not "someone does".
pub async fn probe_advisory_lock_held(
    conn: &mut PgConnection,
    key: i64,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(
        r#"
        SELECT EXISTS (
          SELECT 1 FROM pg_locks
          WHERE locktype = 'advisory'
            AND pid = pg_backend_pid()
            AND objsubid = 1
            AND granted
            AND ((classid::bigint << 32) | objid::bigint) = $1
        )
        "#,
    )
    .bind(key)
    .fetch_one(conn)
    .await
}

/// Release the session advisory lock for `key`. Returning to the pool does not do this.
///
/// sqlx runs at most a ping when a connection goes back and never `DISCARD ALL`,
/// so without an explicit unlock the lock rides an idle pooled connection and
/// locks out every future sender until the pool happens to recycle it.
pub async fn release_advisory_lock(conn: &mut PgConnection, key: i64) -> Result<(), sqlx::Error> {
    sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(key)
        .execute(conn)
        .await?;
    Ok(())
}

// Returns true when the URL parses and its password is absent or empty (a blanked secret).
// Kept in sync with the identical guard in core's accounts/postgres.rs.
fn database_url_password_is_blank(database_url: &str) -> bool {
    match url::Url::parse(database_url) {
        // None (no password) and Some("") (blanked secret) are both missing credentials.
        Ok(parsed) => parsed.password().unwrap_or("").is_empty(),
        // Leave unparseable URLs for sqlx to reject with the real connection error.
        Err(_) => false,
    }
}

/// Everything a rebuild removes, in dependency order. One list so the pooled and the
/// fenced path cannot drift apart. `observed_releases` goes with the rest because the
/// nonce sequence does: a resync reassigns nonces from zero, so a surviving row would
/// name a different withdrawal than the one it was written for.
const DROP_STATEMENTS: [&str; 10] = [
    "DROP TABLE IF EXISTS pending_release_signatures CASCADE",
    "DROP TABLE IF EXISTS observed_releases CASCADE",
    "DROP TABLE IF EXISTS pending_remint_signatures CASCADE",
    "DROP TABLE IF EXISTS reconciliation_halt CASCADE",
    "DROP TABLE IF EXISTS transactions CASCADE",
    "DROP TABLE IF EXISTS indexer_state CASCADE",
    "DROP TABLE IF EXISTS mints CASCADE",
    "DROP SEQUENCE IF EXISTS withdrawal_nonce_seq CASCADE",
    "DROP TYPE IF EXISTS transaction_status CASCADE",
    "DROP TYPE IF EXISTS transaction_type CASCADE",
];

impl PostgresDb {
    pub async fn new(config: &PostgresConfig) -> Result<Self, sqlx::Error> {
        // Fail closed: reject a blank password before connecting (blanked env templates interpolate an empty ${POSTGRES_PASSWORD} into a passwordless URL).
        if database_url_password_is_blank(&config.database_url) {
            return Err(sqlx::Error::Configuration(
                "database_url password component is empty; set a non-empty POSTGRES_PASSWORD"
                    .into(),
            ));
        }

        let pool = PgPoolOptions::new()
            .max_connections(config.max_connections)
            .connect(&config.database_url)
            .await?;

        Ok(Self {
            pool,
            sender_fence: Arc::new(Mutex::new(None)),
        })
    }

    fn sender_fence(&self) -> Option<Arc<LockConnection>> {
        self.sender_fence
            .lock()
            .expect("sender fence mutex poisoned")
            .clone()
    }

    /// Run one sender-owned write. With a sender lock held it executes on the
    /// lock's own session, which is what makes the write unforgeable proof of
    /// ownership; without one it behaves exactly as an unfenced pool write.
    ///
    /// Only ops whose sole production caller is the sender may use this. Routing
    /// recovery, the processor or the boot pre-flight through here would make a
    /// dead sender's rows uncleanable, which is the opposite of the intent.
    async fn run_sender_owned<T, F>(&self, f: F) -> Result<T, sqlx::Error>
    where
        F: for<'c> FnOnce(&'c mut PgConnection) -> BoxFuture<'c, Result<T, sqlx::Error>>,
    {
        match self.sender_fence() {
            Some(fence) => fence.run(f).await,
            None => {
                let mut conn = self.pool.acquire().await?;
                f(&mut conn).await
            }
        }
    }

    pub async fn init_schema(&self) -> Result<(), sqlx::Error> {
        // Ensure pgcrypto is available for gen_random_uuid()
        sqlx::query(r#"CREATE EXTENSION IF NOT EXISTS "pgcrypto""#)
            .execute(&self.pool)
            .await?;

        // Create enum type for transaction status
        sqlx::query(
            r#"
            DO $$ BEGIN
                CREATE TYPE transaction_status AS ENUM ('pending', 'processing', 'completed', 'failed');
            EXCEPTION
                WHEN duplicate_object THEN null;
            END $$;
            "#,
        )
        .execute(&self.pool)

        .await?;

        // Create enum type for transaction type
        sqlx::query(
            r#"
            DO $$ BEGIN
                CREATE TYPE transaction_type AS ENUM ('deposit', 'withdrawal');
            EXCEPTION
                WHEN duplicate_object THEN null;
            END $$;
            "#,
        )
        .execute(&self.pool)
        .await?;

        // Create transactions table
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS transactions (
                id BIGSERIAL PRIMARY KEY,
                signature TEXT NOT NULL,
                instruction_index INTEGER NOT NULL DEFAULT 0,
                slot BIGINT NOT NULL,
                initiator TEXT NOT NULL,
                recipient TEXT NOT NULL,
                mint TEXT NOT NULL,
                amount NUMERIC(20,0) NOT NULL,
                memo TEXT,
                status transaction_status NOT NULL DEFAULT 'pending',
                transaction_type transaction_type NOT NULL,
                withdrawal_nonce BIGINT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                processed_at TIMESTAMPTZ,
                counterpart_signature TEXT,
                trace_id TEXT NOT NULL DEFAULT gen_random_uuid()::text
            );
            "#,
        )
        .execute(&self.pool)
        .await?;

        // Durable identity is the triple (signature, instruction_index, inner_index):
        // a transaction can carry several instructions and each can emit more via CPI.
        // Add the columns, then build the triple index directly (skipping the obsolete
        // two-part index older schemas used) while any old single-signature uniqueness
        // is still in force, so signature is never unprotected; backfilled rows stay
        // unique so the build is clean. signature leads the index, so existing
        // WHERE signature = $1 lookups remain index-served.
        info!("Running transaction identity migration if needed...");
        sqlx::query(
            r#"
            DO $$ BEGIN
                ALTER TABLE transactions
                ADD COLUMN IF NOT EXISTS instruction_index INTEGER NOT NULL DEFAULT 0;
                ALTER TABLE transactions
                ADD COLUMN IF NOT EXISTS inner_index INTEGER;
            END $$;
            "#,
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_transactions_signature_ix_inner \
             ON transactions (signature, instruction_index, COALESCE(inner_index, -1))",
        )
        .execute(&self.pool)
        .await?;
        // Drop the old single-signature and two-part uniqueness now that the triple
        // index is in force. The two-part index is cleaned up for older databases that
        // carry it but is never rebuilt: valid CPI rows sharing (signature,
        // instruction_index) would make a rebuild collide and abort startup.
        sqlx::query(
            r#"
            DO $$ BEGIN
                ALTER TABLE transactions DROP CONSTRAINT IF EXISTS transactions_signature_key;
                DROP INDEX IF EXISTS idx_transactions_signature;
                DROP INDEX IF EXISTS idx_transactions_signature_ix;
            END $$;
            "#,
        )
        .execute(&self.pool)
        .await?;
        info!("transaction identity migration complete");

        // Create indexes for transactions
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_transactions_status ON transactions (status)")
            .execute(&self.pool)
            .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_transactions_type ON transactions (transaction_type)",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_transactions_slot ON transactions (slot)")
            .execute(&self.pool)
            .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_transactions_initiator ON transactions (initiator)",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_transactions_recipient ON transactions (recipient)",
        )
        .execute(&self.pool)
        .await?;

        // Idempotent migration: add trace_id to existing databases
        info!("Running trace_id migration if needed...");
        sqlx::query(
            r#"
            DO $$ BEGIN
                ALTER TABLE transactions ADD COLUMN IF NOT EXISTS trace_id TEXT;
                UPDATE transactions SET trace_id = gen_random_uuid()::text WHERE trace_id IS NULL;
                IF EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'transactions' AND column_name = 'trace_id' AND is_nullable = 'YES'
                ) THEN
                    ALTER TABLE transactions ALTER COLUMN trace_id SET NOT NULL;
                END IF;
            END $$;
            "#,
        )
        .execute(&self.pool)
        .await?;
        info!("trace_id migration complete");

        // Idempotent migration: add remint_signatures to existing databases
        info!("Running remint_signatures migration if needed...");
        sqlx::query(
            r#"
            DO $$ BEGIN                                                                         
                ALTER TABLE transactions ADD COLUMN IF NOT EXISTS remint_signatures TEXT[];     
            END $$;
            "#,
        )
        .execute(&self.pool)
        .await?;
        info!("remint_signatures migration complete");

        // Idempotent migration: durable full release-attempt list recorded on an
        // SMT-confirmed completion. Mirrors the remint_signatures column shape.
        info!("Running release_signatures migration if needed...");
        sqlx::query(
            r#"
            DO $$ BEGIN
                ALTER TABLE transactions ADD COLUMN IF NOT EXISTS release_signatures TEXT[];
            END $$;
            "#,
        )
        .execute(&self.pool)
        .await?;
        info!("release_signatures migration complete");

        // Idempotent migration: add pending_remint_deadline_at to existing databases
        info!("Running pending_remint_deadline_at migration if needed...");
        sqlx::query(
            r#"
            DO $$ BEGIN
                ALTER TABLE transactions ADD COLUMN IF NOT EXISTS pending_remint_deadline_at
        TIMESTAMPTZ;
            END $$;
            "#,
        )
        .execute(&self.pool)
        .await?;
        info!("pending_remint_deadline_at migration complete");

        // Parallel array to remint_signatures: last_valid_block_height per stored
        // signature so the remint gate can prove a broadcast can no longer land.
        info!("Running remint_last_valid_block_heights migration if needed...");
        sqlx::query(
            r#"
            DO $$ BEGIN
                ALTER TABLE transactions
                ADD COLUMN IF NOT EXISTS remint_last_valid_block_heights BIGINT[];
            END $$;
            "#,
        )
        .execute(&self.pool)
        .await?;
        info!("remint_last_valid_block_heights migration complete");

        // Persisted defer-counter for pending remints so the
        // MAX_FINALITY_CHECK_ATTEMPTS budget survives operator restarts.
        info!("Running finality_check_attempts migration if needed...");
        sqlx::query(
            r#"
            DO $$ BEGIN
                ALTER TABLE transactions
                ADD COLUMN IF NOT EXISTS finality_check_attempts INTEGER NOT NULL DEFAULT 0;
            END $$;
            "#,
        )
        .execute(&self.pool)
        .await?;
        info!("finality_check_attempts migration complete");

        // Durable recovery requeue counter so the MAX_RECOVERY_REQUEUE_ATTEMPTS
        // cap survives operator restarts.
        info!("Running recovery_requeue_attempts migration if needed...");
        sqlx::query(
            r#"
            DO $$ BEGIN
                ALTER TABLE transactions
                ADD COLUMN IF NOT EXISTS recovery_requeue_attempts INTEGER NOT NULL DEFAULT 0;
            END $$;
            "#,
        )
        .execute(&self.pool)
        .await?;
        info!("recovery_requeue_attempts migration complete");

        // Confirmed remint signature, recorded synchronously after the remint
        // confirms so a crash before the async writer cannot leave the row at
        // pending_remint with a landed remint (which restart recovery replays).
        info!("Running landed_remint_signature migration if needed...");
        sqlx::query(
            r#"
            DO $$ BEGIN
                ALTER TABLE transactions
                ADD COLUMN IF NOT EXISTS landed_remint_signature TEXT;
            END $$;
            "#,
        )
        .execute(&self.pool)
        .await?;
        info!("landed_remint_signature migration complete");

        // A release the program refused is proof no payout occurred and the only
        // such proof that outlives a bitmap rotation, so it is persisted with the
        // pending remint. NOT NULL DEFAULT FALSE: every row written before this
        // column existed was queued without a refusal, so false is its true value.
        info!("Running release_refused_on_chain migration if needed...");
        sqlx::query(
            r#"
            DO $$ BEGIN
                ALTER TABLE transactions
                ADD COLUMN IF NOT EXISTS release_refused_on_chain BOOLEAN NOT NULL DEFAULT FALSE;
            END $$;
            "#,
        )
        .execute(&self.pool)
        .await?;
        info!("release_refused_on_chain migration complete");

        // Widen a legacy BIGINT amount column to NUMERIC(20,0). BIGINT wraps amounts
        // above i64::MAX negative; the cast is lossless and the guard makes it a no-op
        // once already NUMERIC. Required because the BigDecimal decoder rejects BIGINT.
        info!("Running amount NUMERIC widening migration if needed...");
        sqlx::query(
            r#"
            DO $$ BEGIN
                IF EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'transactions'
                      AND column_name = 'amount'
                      AND data_type = 'bigint'
                ) THEN
                    ALTER TABLE transactions ALTER COLUMN amount TYPE NUMERIC(20,0);
                END IF;
            END $$;
            "#,
        )
        .execute(&self.pool)
        .await?;
        info!("amount NUMERIC widening migration complete");

        sqlx::query(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_transactions_trace_id ON transactions (trace_id)",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_transactions_counterpart_signature ON transactions (counterpart_signature) WHERE counterpart_signature IS NOT NULL",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_transactions_withdrawal_nonce_unique ON transactions (withdrawal_nonce) WHERE withdrawal_nonce IS NOT NULL AND transaction_type = 'withdrawal'",
        )
        .execute(&self.pool)
        .await?;

        // Create withdrawal nonce sequence
        sqlx::query(
            r#"
            CREATE SEQUENCE IF NOT EXISTS withdrawal_nonce_seq START 0 MINVALUE 0;
            "#,
        )
        .execute(&self.pool)
        .await?;

        // Create trigger to auto-assign withdrawal_nonce for withdrawal transactions
        sqlx::query(
            r#"
            CREATE OR REPLACE FUNCTION assign_withdrawal_nonce()
            RETURNS TRIGGER AS $$
            BEGIN
                IF NEW.transaction_type = 'withdrawal' AND NEW.withdrawal_nonce IS NULL THEN
                    NEW.withdrawal_nonce := NEXTVAL('withdrawal_nonce_seq');
                END IF;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            DROP TRIGGER IF EXISTS trigger_assign_withdrawal_nonce ON transactions;
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TRIGGER trigger_assign_withdrawal_nonce
            BEFORE INSERT ON transactions
            FOR EACH ROW
            EXECUTE FUNCTION assign_withdrawal_nonce();
            "#,
        )
        .execute(&self.pool)
        .await?;

        // Create indexer_state table for checkpoint tracking
        sqlx::query(
            r#"
            -- last_committed_slot stays nullable so only the checkpoint writer can claim
            -- a slot. A default would let a row created for a rotation target read back
            -- as "indexed through genesis" on a ledger nothing has ever indexed.
            CREATE TABLE IF NOT EXISTS indexer_state (
                program_type TEXT PRIMARY KEY,
                last_committed_slot BIGINT,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_indexer_state_program ON indexer_state (program_type)",
        )
        .execute(&self.pool)
        .await?;

        // Widen an already-created table. Rows written before this still hold their
        // defaulted zero, which no migration can tell apart from a real genesis
        // checkpoint, so this only stops new phantom rows being made.
        sqlx::query(
            "ALTER TABLE indexer_state
                ALTER COLUMN last_committed_slot DROP DEFAULT,
                ALTER COLUMN last_committed_slot DROP NOT NULL",
        )
        .execute(&self.pool)
        .await?;

        // Create updated_at trigger function
        sqlx::query(
            r#"
            CREATE OR REPLACE FUNCTION update_updated_at_column()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = NOW();
                RETURN NEW;
            END;
            $$ language 'plpgsql';
            "#,
        )
        .execute(&self.pool)
        .await?;

        // Add triggers for updated_at
        sqlx::query(
            r#"
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'update_transactions_updated_at') THEN
                    CREATE TRIGGER update_transactions_updated_at BEFORE UPDATE ON transactions
                    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
                END IF;

            END $$;
            "#,
        )
        .execute(&self.pool)
        .await?;

        // Add trigger for indexer_state updated_at
        sqlx::query(
            r#"
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'update_indexer_state_updated_at') THEN
                    CREATE TRIGGER update_indexer_state_updated_at BEFORE UPDATE ON indexer_state
                    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
                END IF;
            END $$;
            "#,
        )
        .execute(&self.pool)
        .await?;

        // Create mints table for simple lookup
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS mints (
                mint_address TEXT PRIMARY KEY,
                decimals SMALLINT NOT NULL,
                token_program TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            "#,
        )
        .execute(&self.pool)
        .await?;

        // Idempotent migration: add is_pausable to existing databases.
        // Nullable = "unknown"; populated lazily by the operator after an RPC
        // check against the on-chain mint's Token-2022 PausableConfig extension.
        sqlx::query("ALTER TABLE mints ADD COLUMN IF NOT EXISTS is_pausable BOOLEAN")
            .execute(&self.pool)
            .await?;

        // Same pattern for the PermanentDelegate extension — resolved lazily
        // the first time the operator touches the mint. Gate for the balance
        // pre-flight that guards against permanent-delegate drains.
        sqlx::query("ALTER TABLE mints ADD COLUMN IF NOT EXISTS has_permanent_delegate BOOLEAN")
            .execute(&self.pool)
            .await?;

        // Current allow/block state. Existing rows backfill to 'allowed' via the
        // default; the point-in-time history lives in mint_status_history.
        sqlx::query(
            "ALTER TABLE mints ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'allowed'",
        )
        .execute(&self.pool)
        .await?;

        // Add failed_reminted status for withdrawal remint recovery
        sqlx::query(
            r#"
            ALTER TYPE transaction_status ADD VALUE IF NOT EXISTS 'failed_reminted';
            "#,
        )
        .execute(&self.pool)
        .await?;

        // Add manual_review status for unconfirmed remints requiring investigation
        sqlx::query(
            r#"
            ALTER TYPE transaction_status ADD VALUE IF NOT EXISTS 'manual_review';
            "#,
        )
        .execute(&self.pool)
        .await?;

        // Add pending_remint status for withdrawals that failed and have to be processed for remint
        sqlx::query(
            r#"
            ALTER TYPE transaction_status ADD VALUE IF NOT EXISTS 'pending_remint';
            "#,
        )
        .execute(&self.pool)
        .await?;

        // Add parked status for withdrawals blocked by an unresolved ambiguous nonce
        sqlx::query(
            r#"
            ALTER TYPE transaction_status ADD VALUE IF NOT EXISTS 'parked';
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS mint_status_history (
                mint_address    TEXT       NOT NULL,
                status          TEXT       NOT NULL CHECK (status IN ('allowed','blocked')),
                effective_slot  BIGINT     NOT NULL,
                signature       TEXT       NOT NULL,
                created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (mint_address, effective_slot)
            );
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_mint_status_history_lookup
             ON mint_status_history (mint_address, effective_slot DESC)",
        )
        .execute(&self.pool)
        .await?;

        // Broadcast release signatures written at send time; recovery reads
        // them to verify a release landed before demoting (avoids double-payout).
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS pending_release_signatures (
                id BIGSERIAL PRIMARY KEY,
                transaction_id BIGINT NOT NULL REFERENCES transactions(id) ON DELETE CASCADE,
                signature TEXT NOT NULL,
                last_valid_block_height BIGINT NOT NULL,
                blockhash_slot BIGINT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_prs_transaction_id ON pending_release_signatures(transaction_id)",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_prs_signature ON pending_release_signatures(signature)",
        )
        .execute(&self.pool)
        .await?;

        // Every ReleaseFunds the indexer saw succeed on chain, keyed by the nonce
        // it consumed. A refund reads this to tell a nonce that was never paid out
        // from one that was, which nothing else can answer once the nonce's
        // generation has rotated. The nonce is the primary key because the program
        // lets exactly one release consume it.
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS observed_releases (
                withdrawal_nonce BIGINT PRIMARY KEY,
                signature TEXT NOT NULL,
                slot BIGINT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            "#,
        )
        .execute(&self.pool)
        .await?;

        // Write-ahead log for compensating remint MintTo signatures. Separate
        // from pending_release_signatures because these land on the source
        // (PrivateChannel) chain and are classified against source_rpc_client,
        // never the destination chain the release signatures belong to.
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS pending_remint_signatures (
                id BIGSERIAL PRIMARY KEY,
                transaction_id BIGINT NOT NULL REFERENCES transactions(id) ON DELETE CASCADE,
                signature TEXT NOT NULL,
                last_valid_block_height BIGINT NOT NULL,
                blockhash_slot BIGINT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            "#,
        )
        .execute(&self.pool)
        .await?;

        // Slot the broadcast blockhash was read at. A transaction cannot land in a
        // block older than its blockhash, so this is an exact lower bound on where
        // the signature could be, independent of the node's blockhash window at
        // verdict time. NULL on rows written before this column existed; those fall
        // back to deriving the bound from the window.
        info!("Running blockhash_slot migration if needed...");
        sqlx::query(
            r#"
            DO $$ BEGIN
                ALTER TABLE pending_release_signatures
                ADD COLUMN IF NOT EXISTS blockhash_slot BIGINT;
                ALTER TABLE pending_remint_signatures
                ADD COLUMN IF NOT EXISTS blockhash_slot BIGINT;
            END $$;
            "#,
        )
        .execute(&self.pool)
        .await?;
        info!("blockhash_slot migration complete");

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_prms_transaction_id ON pending_remint_signatures(transaction_id)",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_prms_signature ON pending_remint_signatures(signature)",
        )
        .execute(&self.pool)
        .await?;

        // Two senders sign the same remint against different blockhashes, so they
        // produce different signatures and a signature-keyed insert accepts both.
        // Retiring a beaten attempt instead of deleting it keeps it classifiable,
        // which matters because a superseded attempt can still land late.
        info!("Running pending_remint_signatures superseded migration if needed...");
        sqlx::query(
            r#"
            DO $$ BEGIN
                ALTER TABLE pending_remint_signatures
                ADD COLUMN IF NOT EXISTS superseded BOOLEAN NOT NULL DEFAULT FALSE;
            END $$;
            "#,
        )
        .execute(&self.pool)
        .await?;

        // Rows written before the column existed all default to live, so retire
        // every attempt but the newest per transaction first. Building the partial
        // unique index against those duplicates would fail and brick startup.
        sqlx::query(
            r#"
            UPDATE pending_remint_signatures p
            SET superseded = TRUE
            WHERE NOT p.superseded
              AND p.id < (
                  SELECT MAX(q.id) FROM pending_remint_signatures q
                  WHERE q.transaction_id = p.transaction_id
              )
            "#,
        )
        .execute(&self.pool)
        .await?;

        // The arbiter: at most one live attempt per transaction. Only a unique
        // index actually serializes two senders; under READ COMMITTED a
        // check-then-insert lets both pass because neither sees the other's
        // uncommitted row.
        sqlx::query(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_prms_one_live
             ON pending_remint_signatures(transaction_id) WHERE NOT superseded",
        )
        .execute(&self.pool)
        .await?;
        info!("pending_remint_signatures superseded migration complete");

        // Durable single-row reconciliation halt flag. The CHECK(id) plus the
        // fixed TRUE default pins the table to at most one row, so both operators'
        // fetchers read the same flag. Absent row (fresh deploy) means not halted.
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS reconciliation_halt (
                id          BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (id),
                halted      BOOLEAN NOT NULL,
                reason      TEXT NOT NULL,
                halted_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            "#,
        )
        .execute(&self.pool)
        .await?;

        info!("Database schema initialized");
        Ok(())
    }

    pub async fn drop_tables(&self) -> Result<(), sqlx::Error> {
        info!("Dropping database tables...");
        for statement in DROP_STATEMENTS {
            sqlx::query(statement).execute(&self.pool).await?;
        }
        info!("Database tables dropped successfully");
        Ok(())
    }

    /// Drop everything on `conn` rather than through the pool.
    ///
    /// Called with the session that holds the live-state lock, which is what keeps the
    /// drop and the lock inseparable: the lock dies exactly when this session does, so a
    /// statement issued here cannot outlive it. One transaction, so a session lost part
    /// way rolls back rather than leaving a half-dropped schema behind.
    pub async fn drop_tables_on(conn: &mut PgConnection) -> Result<(), sqlx::Error> {
        info!("Dropping database tables...");
        let mut tx = conn.begin().await?;
        for statement in DROP_STATEMENTS {
            sqlx::query(statement).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        info!("Database tables dropped successfully");
        Ok(())
    }

    pub async fn insert_transaction_internal(
        &self,
        transaction: &DbTransaction,
    ) -> Result<i64, sqlx::Error> {
        let existing: Option<(i64,)> = sqlx::query_as(&format!(
            "SELECT {} FROM transactions WHERE {} = $1 AND {} = $2 \
             AND COALESCE({}, -1) = COALESCE($3, -1)",
            transaction_cols::ID,
            transaction_cols::SIGNATURE,
            transaction_cols::INSTRUCTION_INDEX,
            transaction_cols::INNER_INDEX,
        ))
        .bind(&transaction.signature)
        .bind(transaction.instruction_index)
        .bind(transaction.inner_index)
        .fetch_optional(&self.pool)
        .await?;

        if let Some((id,)) = existing {
            return Ok(id);
        }

        let result: Option<(i64,)> = sqlx::query_as(&format!(
            r#"
            INSERT INTO transactions (
                {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT {} DO NOTHING
            RETURNING {}
            "#,
            transaction_cols::SIGNATURE,
            transaction_cols::INSTRUCTION_INDEX,
            transaction_cols::INNER_INDEX,
            transaction_cols::SLOT,
            transaction_cols::INITIATOR,
            transaction_cols::RECIPIENT,
            transaction_cols::MINT,
            transaction_cols::AMOUNT,
            transaction_cols::MEMO,
            transaction_cols::TRANSACTION_TYPE,
            transaction_cols::STATUS,
            transaction_cols::TRACE_ID,
            TX_CONFLICT_TARGET,
            transaction_cols::ID,
        ))
        .bind(&transaction.signature)
        .bind(transaction.instruction_index)
        .bind(transaction.inner_index)
        .bind(transaction.slot)
        .bind(&transaction.initiator)
        .bind(&transaction.recipient)
        .bind(&transaction.mint)
        .bind(transaction.amount)
        .bind(&transaction.memo)
        .bind(transaction.transaction_type)
        .bind(transaction.status)
        .bind(&transaction.trace_id)
        .fetch_optional(&self.pool)
        .await?;

        if let Some((id,)) = result {
            return Ok(id);
        }

        // Conflict occurred, fetch existing ID
        let (id,): (i64,) = sqlx::query_as(&format!(
            "SELECT {} FROM transactions WHERE {} = $1 AND {} = $2 \
             AND COALESCE({}, -1) = COALESCE($3, -1)",
            transaction_cols::ID,
            transaction_cols::SIGNATURE,
            transaction_cols::INSTRUCTION_INDEX,
            transaction_cols::INNER_INDEX,
        ))
        .bind(&transaction.signature)
        .bind(transaction.instruction_index)
        .bind(transaction.inner_index)
        .fetch_one(&self.pool)
        .await?;

        Ok(id)
    }

    pub async fn insert_transactions_batch_internal(
        &self,
        transactions: &[DbTransaction],
    ) -> Result<Vec<i64>, sqlx::Error> {
        if transactions.is_empty() {
            return Ok(Vec::new());
        }

        let mut ids = Vec::with_capacity(transactions.len());

        // Use a transaction for batch insert
        let mut tx = self.pool.begin().await?;

        for transaction in transactions {
            // Check if already exists
            let existing: Option<(i64,)> = sqlx::query_as(&format!(
                "SELECT {} FROM transactions WHERE {} = $1 AND {} = $2 \
                 AND COALESCE({}, -1) = COALESCE($3, -1)",
                transaction_cols::ID,
                transaction_cols::SIGNATURE,
                transaction_cols::INSTRUCTION_INDEX,
                transaction_cols::INNER_INDEX,
            ))
            .bind(&transaction.signature)
            .bind(transaction.instruction_index)
            .bind(transaction.inner_index)
            .fetch_optional(&mut *tx)
            .await?;

            if let Some((id,)) = existing {
                ids.push(id);
                continue;
            }

            // Insert new transaction. counterpart_signature / landed_remint_signature are
            // bound too: on a normal indexing path both are NULL (identical to the column
            // defaults), while a resync reconcile-in-place carries the serviced row's
            // terminal signature so the rebuilt row records it in the same insert.
            let result: Option<(i64,)> = sqlx::query_as(&format!(
                r#"
                INSERT INTO transactions (
                    {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                ON CONFLICT {} DO NOTHING
                RETURNING {}
                "#,
                transaction_cols::SIGNATURE,
                transaction_cols::INSTRUCTION_INDEX,
                transaction_cols::INNER_INDEX,
                transaction_cols::SLOT,
                transaction_cols::INITIATOR,
                transaction_cols::RECIPIENT,
                transaction_cols::MINT,
                transaction_cols::AMOUNT,
                transaction_cols::MEMO,
                transaction_cols::TRANSACTION_TYPE,
                transaction_cols::STATUS,
                transaction_cols::TRACE_ID,
                transaction_cols::COUNTERPART_SIGNATURE,
                transaction_cols::LANDED_REMINT_SIGNATURE,
                TX_CONFLICT_TARGET,
                transaction_cols::ID,
            ))
            .bind(&transaction.signature)
            .bind(transaction.instruction_index)
            .bind(transaction.inner_index)
            .bind(transaction.slot)
            .bind(&transaction.initiator)
            .bind(&transaction.recipient)
            .bind(&transaction.mint)
            .bind(transaction.amount)
            .bind(&transaction.memo)
            .bind(transaction.transaction_type)
            .bind(transaction.status)
            .bind(&transaction.trace_id)
            .bind(&transaction.counterpart_signature)
            .bind(&transaction.landed_remint_signature)
            .fetch_optional(&mut *tx)
            .await?;

            if let Some((id,)) = result {
                ids.push(id);
            } else {
                // Conflict occurred, fetch existing ID
                let (id,): (i64,) = sqlx::query_as(&format!(
                    "SELECT {} FROM transactions WHERE {} = $1 AND {} = $2 \
                     AND COALESCE({}, -1) = COALESCE($3, -1)",
                    transaction_cols::ID,
                    transaction_cols::SIGNATURE,
                    transaction_cols::INSTRUCTION_INDEX,
                    transaction_cols::INNER_INDEX,
                ))
                .bind(&transaction.signature)
                .bind(transaction.instruction_index)
                .bind(transaction.inner_index)
                .fetch_one(&mut *tx)
                .await?;
                ids.push(id);
            }
        }

        tx.commit().await?;
        Ok(ids)
    }

    pub async fn get_pending_withdrawals_internal(
        &self,
        transaction_type: TransactionType,
        limit: i64,
    ) -> Result<Vec<DbTransaction>, sqlx::Error> {
        sqlx::query_as::<_, DbTransaction>(&format!(
            r#"
            SELECT
                {}, {}, {}, {}, {}, {}, {}, {}, {}, {},
                {}, {}, {}, {}, {}, {}, {}, {}, {}, {},
                {}, {}, {}, {}, {}
            FROM transactions
            WHERE {} = $1 AND {} = $2
            ORDER BY {} ASC
            LIMIT $3
            "#,
            transaction_cols::ID,
            transaction_cols::SIGNATURE,
            transaction_cols::TRACE_ID,
            transaction_cols::SLOT,
            transaction_cols::INITIATOR,
            transaction_cols::RECIPIENT,
            transaction_cols::MINT,
            transaction_cols::AMOUNT,
            transaction_cols::MEMO,
            transaction_cols::TRANSACTION_TYPE,
            transaction_cols::WITHDRAWAL_NONCE,
            transaction_cols::STATUS,
            transaction_cols::CREATED_AT,
            transaction_cols::UPDATED_AT,
            transaction_cols::PROCESSED_AT,
            transaction_cols::COUNTERPART_SIGNATURE,
            transaction_cols::REMINT_SIGNATURES,
            transaction_cols::REMINT_LAST_VALID_BLOCK_HEIGHTS,
            transaction_cols::PENDING_REMINT_DEADLINE_AT,
            transaction_cols::FINALITY_CHECK_ATTEMPTS,
            transaction_cols::RECOVERY_REQUEUE_ATTEMPTS,
            transaction_cols::INSTRUCTION_INDEX,
            transaction_cols::INNER_INDEX,
            transaction_cols::LANDED_REMINT_SIGNATURE,
            transaction_cols::RELEASE_REFUSED_ON_CHAIN,
            // Filters
            transaction_cols::STATUS,
            transaction_cols::TRANSACTION_TYPE,
            // Ordering
            transaction_cols::ID,
        ))
        .bind(TransactionStatus::Pending)
        .bind(transaction_type)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
    }

    /// Returns all withdrawal transactions currently in PendingRemint status.
    /// Called on startup to re-hydrate the in-memory remint queue after a crash.
    pub async fn get_pending_remint_transactions_internal(
        &self,
    ) -> Result<Vec<DbTransaction>, sqlx::Error> {
        sqlx::query_as::<_, DbTransaction>(&format!(
            r#"
            SELECT
                {}, {}, {}, {}, {}, {}, {}, {}, {}, {},
                {}, {}, {}, {}, {}, {}, {}, {}, {}, {},
                {}, {}, {}, {}, {}
            FROM transactions
            WHERE {} = $1 AND {} = $2
            ORDER BY {} ASC
            "#,
            transaction_cols::ID,
            transaction_cols::SIGNATURE,
            transaction_cols::TRACE_ID,
            transaction_cols::SLOT,
            transaction_cols::INITIATOR,
            transaction_cols::RECIPIENT,
            transaction_cols::MINT,
            transaction_cols::AMOUNT,
            transaction_cols::MEMO,
            transaction_cols::TRANSACTION_TYPE,
            transaction_cols::WITHDRAWAL_NONCE,
            transaction_cols::STATUS,
            transaction_cols::CREATED_AT,
            transaction_cols::UPDATED_AT,
            transaction_cols::PROCESSED_AT,
            transaction_cols::COUNTERPART_SIGNATURE,
            transaction_cols::REMINT_SIGNATURES,
            transaction_cols::REMINT_LAST_VALID_BLOCK_HEIGHTS,
            transaction_cols::PENDING_REMINT_DEADLINE_AT,
            transaction_cols::FINALITY_CHECK_ATTEMPTS,
            transaction_cols::RECOVERY_REQUEUE_ATTEMPTS,
            transaction_cols::INSTRUCTION_INDEX,
            transaction_cols::INNER_INDEX,
            transaction_cols::LANDED_REMINT_SIGNATURE,
            transaction_cols::RELEASE_REFUSED_ON_CHAIN,
            // Filters
            transaction_cols::STATUS,
            transaction_cols::TRANSACTION_TYPE,
            // Ordering (FIFO)
            transaction_cols::ID,
        ))
        .bind(TransactionStatus::PendingRemint)
        .bind(TransactionType::Withdrawal)
        .fetch_all(&self.pool)
        .await
    }

    /// Fetch the withdrawal row owning `nonce`, whatever its status.
    pub async fn get_withdrawal_by_nonce_internal(
        &self,
        nonce: i64,
    ) -> Result<Option<DbTransaction>, sqlx::Error> {
        sqlx::query_as::<_, DbTransaction>(&format!(
            r#"
            SELECT
                {}, {}, {}, {}, {}, {}, {}, {}, {}, {},
                {}, {}, {}, {}, {}, {}, {}, {}, {}, {},
                {}, {}, {}, {}, {}
            FROM transactions
            WHERE {} = $1 AND {} = $2
            ORDER BY {} DESC
            LIMIT 1
            "#,
            transaction_cols::ID,
            transaction_cols::SIGNATURE,
            transaction_cols::TRACE_ID,
            transaction_cols::SLOT,
            transaction_cols::INITIATOR,
            transaction_cols::RECIPIENT,
            transaction_cols::MINT,
            transaction_cols::AMOUNT,
            transaction_cols::MEMO,
            transaction_cols::TRANSACTION_TYPE,
            transaction_cols::WITHDRAWAL_NONCE,
            transaction_cols::STATUS,
            transaction_cols::CREATED_AT,
            transaction_cols::UPDATED_AT,
            transaction_cols::PROCESSED_AT,
            transaction_cols::COUNTERPART_SIGNATURE,
            transaction_cols::REMINT_SIGNATURES,
            transaction_cols::REMINT_LAST_VALID_BLOCK_HEIGHTS,
            transaction_cols::PENDING_REMINT_DEADLINE_AT,
            transaction_cols::FINALITY_CHECK_ATTEMPTS,
            transaction_cols::RECOVERY_REQUEUE_ATTEMPTS,
            transaction_cols::INSTRUCTION_INDEX,
            transaction_cols::INNER_INDEX,
            transaction_cols::LANDED_REMINT_SIGNATURE,
            transaction_cols::RELEASE_REFUSED_ON_CHAIN,
            // Filters
            transaction_cols::WITHDRAWAL_NONCE,
            transaction_cols::TRANSACTION_TYPE,
            // Newest first, so a re-armed row wins over an abandoned predecessor.
            transaction_cols::ID,
        ))
        .bind(nonce)
        .bind(TransactionType::Withdrawal)
        .fetch_optional(&self.pool)
        .await
    }

    /// Try to acquire the advisory lock for `key`. The lock lives on the pinned
    /// connection; holding it keeps the connection out of the pool so Postgres
    /// keeps the lock held. Returns `None` if another holder exists.
    ///
    /// On success the handle is also installed as this process's sender fence,
    /// so every sender-owned write from here on executes in the locked session.
    pub(crate) async fn try_acquire_sender_lock(
        &self,
        key: i64,
        program_type: &'static str,
        operator_token: tokio_util::sync::CancellationToken,
    ) -> Result<Option<Arc<LockConnection>>, sqlx::Error> {
        let mut conn = self.pool.acquire().await?;
        let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
            .bind(key)
            .fetch_one(&mut *conn)
            .await?;
        if !acquired {
            return Ok(None);
        }

        let lock = Arc::new(LockConnection::new(conn, key, program_type, operator_token));
        lock.apply_lock_timeout().await;
        *self
            .sender_fence
            .lock()
            .expect("sender fence mutex poisoned") = Some(lock.clone());
        Ok(Some(lock))
    }

    /// Try to take the live-state lock in `mode`. `None` means a conflicting
    /// holder exists, and the connection goes straight back to the pool.
    ///
    /// The session is detached from the pool on success. It has to outlive every
    /// database write the caller makes, and shutdown closes the pool while waiting
    /// for checked-out connections, so a pooled one would stall every shutdown
    /// until that wait timed out.
    ///
    /// Detaching frees the pool's permit rather than shrinking it, so the pool can
    /// still open `max_connections`. That means a worker holds one server-side
    /// session more than its pool size, which the server's own `max_connections`
    /// has to have room for.
    pub(crate) async fn try_acquire_live_lock(
        &self,
        mode: LiveLockMode,
    ) -> Result<Option<PgConnection>, sqlx::Error> {
        let mut conn = self.pool.acquire().await?;
        // Before the lock, so a session that takes it is already reapable. Doing it
        // after would leave a window where a dying process holds the lock for hours,
        // which is the failure these settings exist to prevent. A refused acquire
        // returns the connection to the pool still carrying them, which is harmless.
        apply_lock_session_keepalives(&mut conn).await;
        apply_lock_session_lock_timeout(&mut conn).await;
        let acquired: bool = sqlx::query_scalar(mode.acquire_sql())
            .bind(LIVE_STATE_LOCK_KEY)
            .fetch_one(&mut *conn)
            .await?;
        if !acquired {
            return Ok(None);
        }
        Ok(Some(conn.detach()))
    }

    /// Get all transactions of a given type regardless of status
    pub async fn get_all_transactions_internal(
        &self,
        transaction_type: TransactionType,
        limit: i64,
    ) -> Result<Vec<DbTransaction>, sqlx::Error> {
        sqlx::query_as::<_, DbTransaction>(&format!(
            r#"
            SELECT
                {}, {}, {}, {}, {}, {}, {}, {}, {}, {},
                {}, {}, {}, {}, {}, {}, {}, {}, {}, {},
                {}, {}, {}, {}, {}
            FROM transactions
            WHERE {} = $1
            ORDER BY {} DESC
            LIMIT $2
            "#,
            transaction_cols::ID,
            transaction_cols::SIGNATURE,
            transaction_cols::TRACE_ID,
            transaction_cols::SLOT,
            transaction_cols::INITIATOR,
            transaction_cols::RECIPIENT,
            transaction_cols::MINT,
            transaction_cols::AMOUNT,
            transaction_cols::MEMO,
            transaction_cols::TRANSACTION_TYPE,
            transaction_cols::STATUS,
            transaction_cols::WITHDRAWAL_NONCE,
            transaction_cols::CREATED_AT,
            transaction_cols::UPDATED_AT,
            transaction_cols::PROCESSED_AT,
            transaction_cols::COUNTERPART_SIGNATURE,
            transaction_cols::REMINT_SIGNATURES,
            transaction_cols::REMINT_LAST_VALID_BLOCK_HEIGHTS,
            transaction_cols::PENDING_REMINT_DEADLINE_AT,
            transaction_cols::FINALITY_CHECK_ATTEMPTS,
            transaction_cols::RECOVERY_REQUEUE_ATTEMPTS,
            transaction_cols::INSTRUCTION_INDEX,
            transaction_cols::INNER_INDEX,
            transaction_cols::LANDED_REMINT_SIGNATURE,
            transaction_cols::RELEASE_REFUSED_ON_CHAIN,
            // Filter
            transaction_cols::TRANSACTION_TYPE,
            // Ordering
            transaction_cols::CREATED_AT,
        ))
        .bind(transaction_type)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn get_committed_checkpoint_internal(
        &self,
        program_type: &str,
    ) -> Result<Option<u64>, sqlx::Error> {
        // A row can exist with no slot yet, so an unset column reads as absence too.
        let result: Option<(Option<i64>,)> =
            sqlx::query_as("SELECT last_committed_slot FROM indexer_state WHERE program_type = $1")
                .bind(program_type)
                .fetch_optional(&self.pool)
                .await?;

        Ok(result.and_then(|(slot,)| slot).map(|slot| slot as u64))
    }

    pub async fn update_committed_checkpoint_internal(
        &self,
        program_type: &str,
        slot: u64,
    ) -> Result<(), sqlx::Error> {
        // Monotonic guard: GREATEST() prevents a lower slot (e.g. backfill
        // replay after a flushed Yellowstone update) from regressing the cursor.
        sqlx::query(
            r#"
            INSERT INTO indexer_state (program_type, last_committed_slot, updated_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (program_type)
            DO UPDATE SET
                last_committed_slot = GREATEST(indexer_state.last_committed_slot, EXCLUDED.last_committed_slot),
                updated_at = NOW()
            "#,
        )
        .bind(program_type)
        .bind(slot as i64)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn get_and_lock_pending_transactions_internal(
        &self,
        transaction_type: TransactionType,
        limit: i64,
    ) -> Result<Vec<DbTransaction>, sqlx::Error> {
        // Use a transaction to ensure atomicity
        let mut tx = self.pool.begin().await?;

        // Lock rows with FOR UPDATE SKIP LOCKED
        let mut transactions = sqlx::query_as::<_, DbTransaction>(&format!(
            r#"
            SELECT
                {}, {}, {}, {}, {}, {}, {}, {}, {}, {},
                {}, {}, {}, {}, {}, {}, {}, {}, {}, {},
                {}, {}, {}, {}, {}
            FROM transactions
            WHERE {} = $1 AND {} = $2
            ORDER BY {} ASC
            LIMIT $3
            FOR UPDATE SKIP LOCKED
            "#,
            transaction_cols::ID,
            transaction_cols::SIGNATURE,
            transaction_cols::TRACE_ID,
            transaction_cols::SLOT,
            transaction_cols::INITIATOR,
            transaction_cols::RECIPIENT,
            transaction_cols::MINT,
            transaction_cols::AMOUNT,
            transaction_cols::MEMO,
            transaction_cols::TRANSACTION_TYPE,
            transaction_cols::WITHDRAWAL_NONCE,
            transaction_cols::STATUS,
            transaction_cols::CREATED_AT,
            transaction_cols::UPDATED_AT,
            transaction_cols::PROCESSED_AT,
            transaction_cols::COUNTERPART_SIGNATURE,
            transaction_cols::REMINT_SIGNATURES,
            transaction_cols::REMINT_LAST_VALID_BLOCK_HEIGHTS,
            transaction_cols::PENDING_REMINT_DEADLINE_AT,
            transaction_cols::FINALITY_CHECK_ATTEMPTS,
            transaction_cols::RECOVERY_REQUEUE_ATTEMPTS,
            transaction_cols::INSTRUCTION_INDEX,
            transaction_cols::INNER_INDEX,
            transaction_cols::LANDED_REMINT_SIGNATURE,
            transaction_cols::RELEASE_REFUSED_ON_CHAIN,
            // Filters
            transaction_cols::STATUS,
            transaction_cols::TRANSACTION_TYPE,
            // Ordering (FIFO)
            transaction_cols::CREATED_AT,
        ))
        .bind(TransactionStatus::Pending)
        .bind(transaction_type)
        .bind(limit)
        .fetch_all(&mut *tx)
        .await?;

        // Update status to Processing, returning the trigger-bumped `updated_at`
        // so the fetched row carries its true post-lock token; the sender CASes
        // on that at broadcast, not on the stale Pending value.
        if !transactions.is_empty() {
            let ids: Vec<i64> = transactions.iter().map(|txn| txn.id).collect();
            let bumped: Vec<chrono::DateTime<chrono::Utc>> = sqlx::query_scalar(&format!(
                "UPDATE transactions SET {} = $1 WHERE {} = ANY($2) RETURNING {}",
                transaction_cols::STATUS,
                transaction_cols::ID,
                transaction_cols::UPDATED_AT,
            ))
            .bind(TransactionStatus::Processing)
            .bind(&ids)
            .fetch_all(&mut *tx)
            .await?;

            // NOW() is constant across this transaction, so every locked row got
            // the same post-lock timestamp; apply that one value to all of them.
            if let Some(&post_lock_updated_at) = bumped.first() {
                for txn in transactions.iter_mut() {
                    txn.status = TransactionStatus::Processing;
                    txn.updated_at = post_lock_updated_at;
                }
            }
        }

        // Commit to release locks with Processing status
        tx.commit().await?;

        Ok(transactions)
    }

    /// Returns true if the row was updated; false if already terminal.
    pub async fn update_transaction_status_internal(
        &self,
        transaction_id: i64,
        status: TransactionStatus,
        counterpart_signature: Option<String>,
        processed_at: chrono::DateTime<chrono::Utc>,
        release_signatures: Option<Vec<String>>,
    ) -> Result<bool, sqlx::Error> {
        // Only write non-terminal source states — blocks late writes after recovery.
        // release_signatures is COALESCE-guarded so a None never wipes provenance.
        let result = sqlx::query(
            r#"
            UPDATE transactions
            SET
                status = $2,
                counterpart_signature = $3,
                processed_at = $4,
                release_signatures = COALESCE($5, release_signatures)
            WHERE id = $1
              AND status IN ('processing', 'pending_remint')
            "#,
        )
        .bind(transaction_id)
        .bind(status)
        .bind(counterpart_signature)
        .bind(processed_at)
        .bind(release_signatures)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() == 1)
    }

    /// Stale `Processing` rows of one type older than the threshold, oldest-first.
    pub async fn get_stale_processing_transactions_internal(
        &self,
        threshold: Duration,
        limit: i64,
        transaction_type: TransactionType,
    ) -> Result<Vec<DbTransaction>, sqlx::Error> {
        let threshold_secs = threshold.as_secs() as f64;
        sqlx::query_as::<_, DbTransaction>(&format!(
            r#"
            SELECT
                {}, {}, {}, {}, {}, {}, {}, {}, {}, {},
                {}, {}, {}, {}, {}, {}, {}, {}, {}, {},
                {}, {}, {}, {}, {}
            FROM transactions
            WHERE {} = 'processing'
              AND {} < NOW() - make_interval(secs => $1)
              AND {} = $3
            ORDER BY {} ASC
            LIMIT $2
            "#,
            transaction_cols::ID,
            transaction_cols::SIGNATURE,
            transaction_cols::TRACE_ID,
            transaction_cols::SLOT,
            transaction_cols::INITIATOR,
            transaction_cols::RECIPIENT,
            transaction_cols::MINT,
            transaction_cols::AMOUNT,
            transaction_cols::MEMO,
            transaction_cols::TRANSACTION_TYPE,
            transaction_cols::WITHDRAWAL_NONCE,
            transaction_cols::STATUS,
            transaction_cols::CREATED_AT,
            transaction_cols::UPDATED_AT,
            transaction_cols::PROCESSED_AT,
            transaction_cols::COUNTERPART_SIGNATURE,
            transaction_cols::REMINT_SIGNATURES,
            transaction_cols::REMINT_LAST_VALID_BLOCK_HEIGHTS,
            transaction_cols::PENDING_REMINT_DEADLINE_AT,
            transaction_cols::FINALITY_CHECK_ATTEMPTS,
            transaction_cols::RECOVERY_REQUEUE_ATTEMPTS,
            transaction_cols::INSTRUCTION_INDEX,
            transaction_cols::INNER_INDEX,
            transaction_cols::LANDED_REMINT_SIGNATURE,
            transaction_cols::RELEASE_REFUSED_ON_CHAIN,
            // Filters
            transaction_cols::STATUS,
            transaction_cols::UPDATED_AT,
            transaction_cols::TRANSACTION_TYPE,
            // Ordering (FIFO over stale)
            transaction_cols::UPDATED_AT,
        ))
        .bind(threshold_secs)
        .bind(limit)
        .bind(transaction_type)
        .fetch_all(&self.pool)
        .await
    }

    /// CAS `Processing` → `Pending` keyed on `updated_at`; no-op if stale.
    pub async fn try_requeue_processing_internal(
        &self,
        transaction_id: i64,
        expected_updated_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<bool, sqlx::Error> {
        let result = sqlx::query(
            r#"
            UPDATE transactions
            SET status = 'pending',
                recovery_requeue_attempts = recovery_requeue_attempts + 1
            WHERE id = $1
              AND status = 'processing'
              AND updated_at = $2
            "#,
        )
        .bind(transaction_id)
        .bind(expected_updated_at)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() == 1)
    }

    /// Status-only CAS `Processing` to `Pending` for pre-broadcast build/sign
    /// failures. Bumps `recovery_requeue_attempts` so the recovery quarantine cap
    /// survives restarts.
    ///
    /// Deliberately ungated on `updated_at`: it only ever re-arms a row that is
    /// already going back in the queue, so the worst a stale caller can do is
    /// requeue an incarnation someone else owns and spend one of its capped
    /// attempts. It can never authorize a broadcast; that decision is gated by
    /// `claim_and_persist_signature`, which does present the generational token.
    pub async fn try_requeue_prebroadcast_internal(
        &self,
        transaction_id: i64,
        max_attempts: i32,
    ) -> Result<RequeueOutcome, sqlx::Error> {
        // One atomic write enforces the cap: the CASE requeues (and increments) only
        // while under max_attempts, otherwise leaves the row Processing. RETURNING the
        // post-update count plus whether the row is now Pending distinguishes the
        // three outcomes without a separate counter read that could fail.
        let row: Option<(i32, bool)> = sqlx::query_as(
            r#"
            UPDATE transactions
            SET status = CASE WHEN recovery_requeue_attempts < $2
                              THEN 'pending'::transaction_status ELSE status END,
                recovery_requeue_attempts = CASE WHEN recovery_requeue_attempts < $2
                              THEN recovery_requeue_attempts + 1
                              ELSE recovery_requeue_attempts END
            WHERE id = $1
              AND status = 'processing'
            RETURNING recovery_requeue_attempts, (status = 'pending') AS requeued
            "#,
        )
        .bind(transaction_id)
        .bind(max_attempts)
        .fetch_optional(&self.pool)
        .await?;

        Ok(match row {
            None => RequeueOutcome::NotProcessing,
            Some((attempts, true)) => RequeueOutcome::Requeued { attempts },
            Some((_, false)) => RequeueOutcome::AtCap,
        })
    }

    /// CAS `Processing`/`Parked` → `Parked`. Accepts an already-parked row so the
    /// drain's per-tick re-park bumps `updated_at` (the heartbeat).
    pub async fn try_park_processing_internal(
        &self,
        transaction_id: i64,
    ) -> Result<bool, sqlx::Error> {
        let result = self
            .run_sender_owned(|conn| {
                Box::pin(async move {
                    sqlx::query(
                        r#"
                        UPDATE transactions
                        SET status = 'parked'
                        WHERE id = $1
                          AND status IN ('processing', 'parked')
                        "#,
                    )
                    .bind(transaction_id)
                    .execute(conn)
                    .await
                })
            })
            .await?;

        Ok(result.rows_affected() == 1)
    }

    /// CAS `Parked` to `Processing`. Strict on purpose: if recovery requeued the
    /// row and a new processor already took it back to `processing`, this returns
    /// `Ok(None)` so the drain drops its stale builder instead of double-sending.
    ///
    /// The winner gets the post-update `updated_at` back. Park and unpark each bump
    /// the row, so the token the parked builder arrived with is already dead; this
    /// is the incarnation the sender's release claim must present.
    pub async fn try_unpark_to_processing_internal(
        &self,
        transaction_id: i64,
    ) -> Result<Option<chrono::DateTime<chrono::Utc>>, sqlx::Error> {
        self.run_sender_owned(|conn| {
            Box::pin(async move {
                sqlx::query_scalar::<_, chrono::DateTime<chrono::Utc>>(
                    r#"
                        UPDATE transactions
                        SET status = 'processing'
                        WHERE id = $1
                          AND status = 'parked'
                        RETURNING updated_at
                        "#,
                )
                .bind(transaction_id)
                .fetch_optional(conn)
                .await
            })
        })
        .await
    }

    /// Stale `Parked` rows of one type older than the threshold, oldest-first.
    pub async fn get_stale_parked_transactions_internal(
        &self,
        threshold: Duration,
        limit: i64,
        transaction_type: TransactionType,
    ) -> Result<Vec<DbTransaction>, sqlx::Error> {
        let threshold_secs = threshold.as_secs() as f64;
        sqlx::query_as::<_, DbTransaction>(&format!(
            r#"
            SELECT
                {}, {}, {}, {}, {}, {}, {}, {}, {}, {},
                {}, {}, {}, {}, {}, {}, {}, {}, {}, {},
                {}, {}, {}, {}, {}
            FROM transactions
            WHERE {} = 'parked'
              AND {} < NOW() - make_interval(secs => $1)
              AND {} = $3
            ORDER BY {} ASC
            LIMIT $2
            "#,
            transaction_cols::ID,
            transaction_cols::SIGNATURE,
            transaction_cols::TRACE_ID,
            transaction_cols::SLOT,
            transaction_cols::INITIATOR,
            transaction_cols::RECIPIENT,
            transaction_cols::MINT,
            transaction_cols::AMOUNT,
            transaction_cols::MEMO,
            transaction_cols::TRANSACTION_TYPE,
            transaction_cols::WITHDRAWAL_NONCE,
            transaction_cols::STATUS,
            transaction_cols::CREATED_AT,
            transaction_cols::UPDATED_AT,
            transaction_cols::PROCESSED_AT,
            transaction_cols::COUNTERPART_SIGNATURE,
            transaction_cols::REMINT_SIGNATURES,
            transaction_cols::REMINT_LAST_VALID_BLOCK_HEIGHTS,
            transaction_cols::PENDING_REMINT_DEADLINE_AT,
            transaction_cols::FINALITY_CHECK_ATTEMPTS,
            transaction_cols::RECOVERY_REQUEUE_ATTEMPTS,
            transaction_cols::INSTRUCTION_INDEX,
            transaction_cols::INNER_INDEX,
            transaction_cols::LANDED_REMINT_SIGNATURE,
            transaction_cols::RELEASE_REFUSED_ON_CHAIN,
            // Filters
            transaction_cols::STATUS,
            transaction_cols::UPDATED_AT,
            transaction_cols::TRANSACTION_TYPE,
            // Ordering (FIFO over stale)
            transaction_cols::UPDATED_AT,
        ))
        .bind(threshold_secs)
        .bind(limit)
        .bind(transaction_type)
        .fetch_all(&self.pool)
        .await
    }

    /// CAS `Parked` → `Pending` keyed on `updated_at`; no-op if stale.
    pub async fn try_requeue_parked_internal(
        &self,
        transaction_id: i64,
        expected_updated_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<bool, sqlx::Error> {
        let result = sqlx::query(
            r#"
            UPDATE transactions
            SET status = 'pending'
            WHERE id = $1
              AND status = 'parked'
              AND updated_at = $2
            "#,
        )
        .bind(transaction_id)
        .bind(expected_updated_at)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() == 1)
    }

    /// CAS `Processing` → `Completed` keyed on `updated_at`; sig may be `None`.
    /// `release_signatures` is COALESCE-guarded so `None` never clobbers a value.
    pub async fn try_complete_processing_internal(
        &self,
        transaction_id: i64,
        expected_updated_at: chrono::DateTime<chrono::Utc>,
        counterpart_signature: Option<String>,
        release_signatures: Option<Vec<String>>,
    ) -> Result<bool, sqlx::Error> {
        let result = sqlx::query(
            r#"
            UPDATE transactions
            SET status = 'completed',
                counterpart_signature = COALESCE($3, counterpart_signature),
                release_signatures = COALESCE($4, release_signatures),
                processed_at = NOW()
            WHERE id = $1
              AND status = 'processing'
              AND updated_at = $2
            "#,
        )
        .bind(transaction_id)
        .bind(expected_updated_at)
        .bind(counterpart_signature)
        .bind(release_signatures)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() == 1)
    }

    /// CAS `Processing` → `ManualReview`; reason rides on the webhook, not DB.
    ///
    /// The optional signature arrays are persisted by the same statement rather
    /// than a preceding one. Splitting them is not merely slower, it cannot
    /// work: the `updated_at` trigger fires on the first write, after which this
    /// statement's `updated_at` compare can never match and the row would be
    /// stranded in `processing` forever. `COALESCE` keeps a `None` call inert,
    /// so callers with no evidence to record leave both columns as they were.
    pub async fn try_quarantine_processing_internal(
        &self,
        transaction_id: i64,
        expected_updated_at: chrono::DateTime<chrono::Utc>,
        remint_signatures: Option<Vec<String>>,
        remint_last_valid_block_heights: Option<Vec<i64>>,
    ) -> Result<bool, sqlx::Error> {
        let result = sqlx::query(
            r#"
            UPDATE transactions
            SET status = 'manual_review',
                remint_signatures = COALESCE($3, remint_signatures),
                remint_last_valid_block_heights = COALESCE($4, remint_last_valid_block_heights),
                processed_at = NOW()
            WHERE id = $1
              AND status = 'processing'
              AND updated_at = $2
            "#,
        )
        .bind(transaction_id)
        .bind(expected_updated_at)
        .bind(remint_signatures)
        .bind(remint_last_valid_block_heights)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() == 1)
    }

    /// Withdrawals stalled in `status` that still carry stored release
    /// signatures, keyed forward from `after_id`.
    ///
    /// The evidence predicates are in SQL so a row that can never be classified
    /// (structurally corrupt, or quarantined before anything was broadcast) is
    /// never fetched and never costs an RPC round trip. A NULL nonce is excluded
    /// for the same reason: no nonce means no release was ever built, so the row
    /// cannot account for a bit the on-chain bitmap is holding. The two arrays
    /// are index-paired, and they arrived in separate migrations, so a row can
    /// legitimately carry signatures with no heights; that is unclassifiable too.
    ///
    /// A row whose refund was already claimed or landed is excluded as well:
    /// completing it on release evidence alone would pay the nonce twice and
    /// then let the GC drop the claim. The bitmap check adjudicates those.
    ///
    /// Paging is keyed on `id` rather than offset by `updated_at`. A row that
    /// does not classify is left untouched by design, so its `updated_at` never
    /// moves; ordering on it would return the same blocked rows on every sweep
    /// and starve everything behind them. `id` gives the caller a cursor that
    /// always advances.
    pub async fn get_stalled_withdrawals_with_signatures_internal(
        &self,
        status: TransactionStatus,
        after_id: i64,
        limit: i64,
    ) -> Result<Vec<DbTransaction>, sqlx::Error> {
        sqlx::query_as::<_, DbTransaction>(&format!(
            r#"
            SELECT
                {}, {}, {}, {}, {}, {}, {}, {}, {}, {},
                {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}
            FROM transactions
            WHERE {} = 'withdrawal'
              AND {} = $1
              AND {} IS NOT NULL
              AND {} IS NOT NULL
              AND array_length({}, 1) > 0
              AND {} IS NOT NULL
              AND {} IS NULL
              AND NOT EXISTS (
                  SELECT 1 FROM pending_remint_signatures p
                  WHERE p.transaction_id = transactions.{}
              )
              AND {} > $2
            ORDER BY {} ASC
            LIMIT $3
            "#,
            transaction_cols::ID,
            transaction_cols::SIGNATURE,
            transaction_cols::TRACE_ID,
            transaction_cols::SLOT,
            transaction_cols::INITIATOR,
            transaction_cols::RECIPIENT,
            transaction_cols::MINT,
            transaction_cols::AMOUNT,
            transaction_cols::MEMO,
            transaction_cols::TRANSACTION_TYPE,
            transaction_cols::WITHDRAWAL_NONCE,
            transaction_cols::STATUS,
            transaction_cols::CREATED_AT,
            transaction_cols::UPDATED_AT,
            transaction_cols::PROCESSED_AT,
            transaction_cols::COUNTERPART_SIGNATURE,
            transaction_cols::REMINT_SIGNATURES,
            transaction_cols::REMINT_LAST_VALID_BLOCK_HEIGHTS,
            transaction_cols::PENDING_REMINT_DEADLINE_AT,
            transaction_cols::FINALITY_CHECK_ATTEMPTS,
            transaction_cols::RECOVERY_REQUEUE_ATTEMPTS,
            transaction_cols::INSTRUCTION_INDEX,
            transaction_cols::INNER_INDEX,
            transaction_cols::LANDED_REMINT_SIGNATURE,
            transaction_cols::RELEASE_REFUSED_ON_CHAIN,
            // Filters
            transaction_cols::TRANSACTION_TYPE,
            transaction_cols::STATUS,
            transaction_cols::WITHDRAWAL_NONCE,
            transaction_cols::REMINT_SIGNATURES,
            transaction_cols::REMINT_SIGNATURES,
            transaction_cols::REMINT_LAST_VALID_BLOCK_HEIGHTS,
            // Refund interlock
            transaction_cols::LANDED_REMINT_SIGNATURE,
            transaction_cols::ID,
            // Keyset cursor
            transaction_cols::ID,
            // Ordering must match the cursor so paging cannot repeat or skip
            transaction_cols::ID,
        ))
        .bind(status)
        .bind(after_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
    }

    /// CAS a stalled withdrawal to `Completed` once its release is proven landed.
    ///
    /// `from_status` is bound by the caller, but the statement additionally pins
    /// the allowed source statuses inline. That redundancy is deliberate: the
    /// guard is what stops this from resurrecting a terminal row or stealing a
    /// `processing` row from a live sender, and it belongs in the SQL rather
    /// than resting on every present and future caller binding the right value.
    pub async fn try_complete_stalled_withdrawal_internal(
        &self,
        transaction_id: i64,
        expected_updated_at: chrono::DateTime<chrono::Utc>,
        from_status: TransactionStatus,
        counterpart_signature: Option<String>,
    ) -> Result<bool, sqlx::Error> {
        let result = sqlx::query(
            r#"
            UPDATE transactions
            SET status = 'completed',
                counterpart_signature = COALESCE($4, counterpart_signature),
                processed_at = NOW()
            WHERE id = $1
              AND updated_at = $2
              AND status = $3
              AND status IN ('manual_review', 'pending_remint')
              AND transaction_type = 'withdrawal'
            "#,
        )
        .bind(transaction_id)
        .bind(expected_updated_at)
        .bind(from_status)
        .bind(counterpart_signature)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() == 1)
    }

    /// Transitions a withdrawal to PendingRemint status, storing the withdrawal
    /// signatures needed for the finality check on restart and whether the
    /// program itself refused the release. The refusal rides in this same UPDATE
    /// so a crash can never leave a queued refund without the proof that lets it
    /// be paid out once the bitmap has rotated past the nonce.
    ///
    /// Idempotent for an identical payload: a row already PendingRemint with the
    /// same signatures is re-written, so a retry after a lost acknowledgement
    /// succeeds. A different payload still fails, and no other status matches.
    pub async fn set_pending_remint_internal(
        &self,
        transaction_id: i64,
        remint_signatures: Vec<String>,
        remint_last_valid_block_heights: Vec<i64>,
        deadline_at: chrono::DateTime<chrono::Utc>,
        release_refused_on_chain: bool,
    ) -> Result<(), sqlx::Error> {
        let result = self
            .run_sender_owned(move |conn| {
                Box::pin(async move {
                    sqlx::query(
                        r#"
                        UPDATE transactions
                        SET
                            status = $2,
                            remint_signatures = $3,
                            remint_last_valid_block_heights = $4,
                            pending_remint_deadline_at = $5,
                            release_refused_on_chain = $6,
                            updated_at = NOW()
                        WHERE id = $1
                            AND (status = 'processing'
                                 OR (status = 'pending_remint' AND remint_signatures = $3))
                        "#,
                    )
                    .bind(transaction_id)
                    .bind(TransactionStatus::PendingRemint)
                    .bind(remint_signatures)
                    .bind(remint_last_valid_block_heights)
                    .bind(deadline_at)
                    .bind(release_refused_on_chain)
                    .execute(conn)
                    .await
                })
            })
            .await?;

        if result.rows_affected() == 0 {
            return Err(sqlx::Error::RowNotFound);
        }

        Ok(())
    }

    /// Persists an incremented defer counter and the extended deadline for an
    /// already-PendingRemint row. The status guard prevents resurrecting a
    /// terminal row (Completed / FailedReminted / ManualReview).
    pub async fn bump_pending_remint_finality_attempt_internal(
        &self,
        transaction_id: i64,
        attempts: i32,
        new_deadline: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), sqlx::Error> {
        let result = self
            .run_sender_owned(|conn| {
                Box::pin(async move {
                    sqlx::query(
                        r#"
                        UPDATE transactions
                        SET
                            finality_check_attempts = $2,
                            pending_remint_deadline_at = $3,
                            updated_at = NOW()
                        WHERE id = $1
                            AND status = 'pending_remint'
                        "#,
                    )
                    .bind(transaction_id)
                    .bind(attempts)
                    .bind(new_deadline)
                    .execute(conn)
                    .await
                })
            })
            .await?;

        if result.rows_affected() == 0 {
            return Err(sqlx::Error::RowNotFound);
        }

        Ok(())
    }

    /// Current status of one row, or `None` if it does not exist.
    pub async fn get_transaction_status_internal(
        &self,
        transaction_id: i64,
    ) -> Result<Option<TransactionStatus>, sqlx::Error> {
        sqlx::query_scalar::<_, TransactionStatus>("SELECT status FROM transactions WHERE id = $1")
            .bind(transaction_id)
            .fetch_optional(&self.pool)
            .await
    }

    /// Durably record a confirmed remint: flip status to FailedReminted and
    /// store the signature in one UPDATE, before the async writer runs. The
    /// `pending_remint` guard makes it a no-op on an already-terminal row, so
    /// a replayed call can never resurrect or double-record.
    pub async fn record_remint_result_internal(
        &self,
        transaction_id: i64,
        remint_signature: String,
    ) -> Result<(), sqlx::Error> {
        // Shares the closure so the read runs on the same session as the UPDATE it explains.
        let (applied, current) = self
            .run_sender_owned(move |conn| {
                Box::pin(async move {
                    let result = sqlx::query(
                        r#"
                        UPDATE transactions
                        SET
                            status = $2,
                            landed_remint_signature = $3,
                            processed_at = NOW(),
                            updated_at = NOW()
                        WHERE id = $1
                            AND status = 'pending_remint'
                        "#,
                    )
                    .bind(transaction_id)
                    .bind(TransactionStatus::FailedReminted)
                    .bind(remint_signature)
                    .execute(&mut *conn)
                    .await?;

                    if result.rows_affected() != 0 {
                        return Ok((true, None));
                    }
                    let current: Option<String> =
                        sqlx::query_scalar("SELECT status::text FROM transactions WHERE id = $1")
                            .bind(transaction_id)
                            .fetch_optional(conn)
                            .await?;
                    Ok((false, current))
                })
            })
            .await?;

        if !applied {
            // The guarded UPDATE matched nothing. Distinguish the two cases for
            // on-call: a missing row is a bug (the id came from a live
            // PendingRemint row), a non-pending_remint status is expected on an
            // idempotent replay. Both still signal RowNotFound so the caller
            // falls back to the async writer.
            match current.as_deref() {
                None => warn!("record_remint_result: transaction {transaction_id} not found"),
                Some(status) => info!(
                    "record_remint_result: transaction {transaction_id} not pending_remint \
                     (status {status:?}); skipping"
                ),
            }
            return Err(sqlx::Error::RowNotFound);
        }

        Ok(())
    }

    /// Persist a broadcast release signature so recovery can verify finality
    /// before demoting. Idempotent on `signature`.
    pub async fn insert_release_signature_internal(
        &self,
        transaction_id: i64,
        signature: String,
        last_valid_block_height: i64,
        blockhash_slot: Option<i64>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
            INSERT INTO pending_release_signatures
                (transaction_id, signature, last_valid_block_height, blockhash_slot)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (signature) DO NOTHING
            "#,
        )
        .bind(transaction_id)
        .bind(signature)
        .bind(last_valid_block_height)
        .bind(blockhash_slot)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Atomically claim a `Processing` row and persist its broadcast signature in
    /// one transaction. The CAS on `updated_at` bumps the row so a racing recovery
    /// demote (also a CAS on that column) loses; sharing one transaction leaves no
    /// bumped-but-unsigned window. `Ok(Some(lease))` returns the committed
    /// post-claim `updated_at`, valid as the next CAS token; `Ok(None)` means the
    /// row was demoted or re-locked, so the caller must not broadcast.
    ///
    /// Nothing here is type-specific: the deposit mint and the withdrawal release
    /// both need exactly this ownership proof before they move funds.
    pub async fn claim_and_persist_signature_internal(
        &self,
        transaction_id: i64,
        expected_updated_at: chrono::DateTime<chrono::Utc>,
        signature: String,
        last_valid_block_height: i64,
        blockhash_slot: Option<i64>,
    ) -> Result<Option<chrono::DateTime<chrono::Utc>>, sqlx::Error> {
        let mut tx = self.pool.begin().await?;

        // RETURNING yields the post-trigger committed value, so the lease handed
        // back is exactly the token a subsequent CAS must present.
        let claimed = sqlx::query_scalar::<_, chrono::DateTime<chrono::Utc>>(
            r#"
            UPDATE transactions
            SET updated_at = NOW()
            WHERE id = $1
              AND status = 'processing'
              AND updated_at = $2
            RETURNING updated_at
            "#,
        )
        .bind(transaction_id)
        .bind(expected_updated_at)
        .fetch_optional(&mut *tx)
        .await?;

        let Some(lease) = claimed else {
            tx.rollback().await?;
            return Ok(None);
        };

        sqlx::query(
            r#"
            INSERT INTO pending_release_signatures
                (transaction_id, signature, last_valid_block_height, blockhash_slot)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (signature) DO NOTHING
            "#,
        )
        .bind(transaction_id)
        .bind(signature)
        .bind(last_valid_block_height)
        .bind(blockhash_slot)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(Some(lease))
    }

    /// Return a transaction's release signatures as (signature, lvbh).
    pub async fn get_release_signatures_internal(
        &self,
        transaction_id: i64,
    ) -> Result<Vec<StoredSig>, sqlx::Error> {
        sqlx::query_as::<_, StoredSig>(
            r#"
            SELECT signature, last_valid_block_height, blockhash_slot
            FROM pending_release_signatures
            WHERE transaction_id = $1
            ORDER BY id ASC
            "#,
        )
        .bind(transaction_id)
        .fetch_all(&self.pool)
        .await
    }

    /// Delete all stored release signatures for a transaction.
    pub async fn delete_release_signatures_internal(
        &self,
        transaction_id: i64,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("DELETE FROM pending_release_signatures WHERE transaction_id = $1")
            .bind(transaction_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Delete one stored release signature, keeping the transaction's others.
    pub async fn delete_release_signature_internal(
        &self,
        transaction_id: i64,
        signature: &str,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "DELETE FROM pending_release_signatures WHERE transaction_id = $1 AND signature = $2",
        )
        .bind(transaction_id)
        .bind(signature)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Record the releases seen in one slot; idempotent on the nonce.
    pub async fn insert_observed_releases_batch_internal(
        &self,
        releases: &[DbObservedRelease],
    ) -> Result<(), sqlx::Error> {
        for release in releases {
            sqlx::query(
                r#"
                INSERT INTO observed_releases (withdrawal_nonce, signature, slot)
                VALUES ($1, $2, $3)
                ON CONFLICT (withdrawal_nonce) DO NOTHING
                "#,
            )
            .bind(release.withdrawal_nonce)
            .bind(&release.signature)
            .bind(release.slot)
            .execute(&self.pool)
            .await?;
        }
        Ok(())
    }

    /// The release recorded for `nonce`, if the indexer has seen one.
    pub async fn get_observed_release_internal(
        &self,
        nonce: i64,
    ) -> Result<Option<DbObservedRelease>, sqlx::Error> {
        sqlx::query_as::<_, DbObservedRelease>(
            r#"
            SELECT withdrawal_nonce, signature, slot
            FROM observed_releases
            WHERE withdrawal_nonce = $1
            "#,
        )
        .bind(nonce)
        .fetch_optional(&self.pool)
        .await
    }

    /// Drop release signatures whose parent transaction is no longer
    /// `processing`. Returns the number of rows removed.
    /// Only genuinely terminal rows are reclaimed; every non-terminal
    /// status keeps its write-ahead journal. A demoted, quarantined, parked, or
    /// pending-remint row can still be picked up or reminted, and the pre-mint
    /// gate re-verifies those signatures before it would broadcast again, so
    /// deleting them early would let a landed mint be re-issued.
    pub async fn gc_stale_release_signatures_internal(&self) -> Result<u64, sqlx::Error> {
        let result = sqlx::query(
            r#"
            DELETE FROM pending_release_signatures
            WHERE transaction_id IN (SELECT id FROM transactions
                                     WHERE status IN ('completed', 'failed', 'failed_reminted'))
            "#,
        )
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    /// Write-ahead record of a remint MintTo signature, persisted before the
    /// broadcast so restart recovery can classify it instead of reminting blind,
    /// and in the same step the exclusive claim on that broadcast.
    ///
    /// The claim is keyed on the transaction, not the signature: two senders sign
    /// against different blockhashes, so a signature-keyed insert accepts both and
    /// both mint. `superseded_signatures` must contain only attempts the caller has
    /// already proven dead on-chain, and retiring them is scoped to exactly that
    /// observed set, so a claim another sender took in the meantime is never
    /// cleared. `ON CONFLICT DO NOTHING` does not abort the surrounding
    /// transaction, so a lost claim still retires the attempts it proved dead.
    ///
    /// Returns true when the caller owns the attempt and may broadcast.
    pub async fn claim_remint_attempt_internal(
        &self,
        transaction_id: i64,
        signature: String,
        last_valid_block_height: i64,
        blockhash_slot: Option<i64>,
        superseded_signatures: &[String],
    ) -> Result<bool, sqlx::Error> {
        // Both statements are sender-owned, so the transaction moves whole and never mixes fenced work.
        let superseded_signatures = superseded_signatures.to_vec();
        self.run_sender_owned(move |conn| {
            Box::pin(async move {
                let mut tx = conn.begin().await?;

                sqlx::query(
                    r#"
                    UPDATE pending_remint_signatures
                    SET superseded = TRUE
                    WHERE transaction_id = $1
                      AND signature = ANY($2)
                      AND NOT superseded
                    "#,
                )
                .bind(transaction_id)
                .bind(&superseded_signatures)
                .execute(&mut *tx)
                .await?;

                let claimed = sqlx::query(
                    r#"
                    INSERT INTO pending_remint_signatures
                        (transaction_id, signature, last_valid_block_height, blockhash_slot)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (transaction_id) WHERE NOT superseded DO NOTHING
                    "#,
                )
                .bind(transaction_id)
                .bind(signature)
                .bind(last_valid_block_height)
                .bind(blockhash_slot)
                .execute(&mut *tx)
                .await?;

                tx.commit().await?;
                Ok(claimed.rows_affected() == 1)
            })
        })
        .await
    }

    /// Return a transaction's remint signatures as (signature, lvbh).
    pub async fn get_remint_signatures_internal(
        &self,
        transaction_id: i64,
    ) -> Result<Vec<StoredSig>, sqlx::Error> {
        sqlx::query_as::<_, StoredSig>(
            r#"
            SELECT signature, last_valid_block_height, blockhash_slot
            FROM pending_remint_signatures
            WHERE transaction_id = $1
            ORDER BY id ASC
            "#,
        )
        .bind(transaction_id)
        .fetch_all(&self.pool)
        .await
    }

    /// Delete all stored remint signatures for a transaction.
    pub async fn delete_remint_signatures_internal(
        &self,
        transaction_id: i64,
    ) -> Result<(), sqlx::Error> {
        self.run_sender_owned(|conn| {
            Box::pin(async move {
                sqlx::query("DELETE FROM pending_remint_signatures WHERE transaction_id = $1")
                    .bind(transaction_id)
                    .execute(conn)
                    .await
            })
        })
        .await?;
        Ok(())
    }

    /// Drop remint signatures whose parent transaction is no longer
    /// `pending_remint`. Returns the number of rows removed.
    pub async fn gc_stale_remint_signatures_internal(&self) -> Result<u64, sqlx::Error> {
        let result = sqlx::query(
            r#"
            DELETE FROM pending_remint_signatures
            WHERE transaction_id IN (
                SELECT id FROM transactions WHERE status <> 'pending_remint'
            )
            "#,
        )
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    /// Flip active withdrawals at or above `min_nonce` to `ManualReview`.
    ///
    /// `exclude_id` is the poison row that the caller has already quarantined
    /// via the async `storage_tx` writer. That update may not have hit the DB
    /// yet when this sweep runs, so the row's status is still active here;
    /// excluding it prevents a second `ManualReview` webhook for the same
    /// transaction.
    ///
    /// `min_nonce` is the poison row's own nonce. Bounding the sweep keeps
    /// lower-nonce rows out of it: such a row may already be signed or
    /// broadcast by the sender, and terminalizing one drops its later
    /// `Completed` write. A NULL `withdrawal_nonce` never satisfies the
    /// comparison, so such a row is left alone. `None` sweeps everything.
    ///
    /// Terminal rows are left untouched so the webhook does not re-alert on
    /// already-handled transactions. Returns the number of rows affected.
    ///
    /// Journalled release signatures are copied onto the row in the same
    /// UPDATE. The journal is GC'd once the row leaves `Processing`, and the
    /// reconcile sweep only fetches rows carrying those columns.
    ///
    /// Scope is intentionally DB-wide over `transaction_type = 'withdrawal'`
    /// to match the fetcher's own scope. The data model assumes a single
    /// withdrawal operator per database; multi-instance isolation would
    /// require an `instance_pda` column on `transactions` that does not exist
    /// today.
    // Coverage-ignore rationale (category b, defensive recovery):
    //   `quarantine_active_withdrawals_internal` is only invoked by
    //   the poison-pill pipeline in `operator/processor.rs`
    //   (`halt_withdrawal_pipeline`), which is itself LCOV-excluded.
    //   Integration tests do not produce malformed rows that would trip
    //   it. The SQL itself is trivial; the behavior is covered via the
    //   `Storage::Mock` variant in in-crate tests and by the runbook drills.
    pub async fn quarantine_active_withdrawals_internal(
        &self,
        exclude_id: Option<i64>,
        min_nonce: Option<i64>,
    ) -> Result<u64, sqlx::Error> {
        let result = sqlx::query(
            r#"
            UPDATE transactions
            SET status = 'manual_review',
                updated_at = NOW(),
                remint_signatures = COALESCE(
                    (SELECT array_agg(p.signature ORDER BY p.id)
                     FROM pending_release_signatures p
                     WHERE p.transaction_id = transactions.id),
                    remint_signatures
                ),
                remint_last_valid_block_heights = COALESCE(
                    (SELECT array_agg(p.last_valid_block_height ORDER BY p.id)
                     FROM pending_release_signatures p
                     WHERE p.transaction_id = transactions.id),
                    remint_last_valid_block_heights
                )
            WHERE transaction_type = 'withdrawal'
              AND status IN ('pending', 'processing', 'parked')
              AND ($1::BIGINT IS NULL OR id <> $1)
              AND ($2::BIGINT IS NULL OR withdrawal_nonce >= $2)
            "#,
        )
        .bind(exclude_id)
        .bind(min_nonce)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected())
    }

    pub async fn upsert_mints_batch_internal(&self, mints: &[DbMint]) -> Result<(), StorageError> {
        if mints.is_empty() {
            return Ok(());
        }

        // Use a transaction for batch upsert
        let mut tx = self.pool.begin().await?;

        for mint in mints {
            sqlx::query(
                r#"
                INSERT INTO mints (mint_address, decimals, token_program, status)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (mint_address) DO UPDATE
                SET decimals = EXCLUDED.decimals,
                    token_program = EXCLUDED.token_program
                "#,
            )
            .bind(&mint.mint_address)
            .bind(mint.decimals)
            .bind(&mint.token_program)
            .bind(&mint.status)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    /// Set each mint's `status` mirror to its latest `mint_status_history`
    /// transition (highest `effective_slot`); a mint with no row is untouched.
    pub async fn sync_mint_status_internal(
        &self,
        mint_addresses: &[String],
    ) -> Result<(), StorageError> {
        if mint_addresses.is_empty() {
            return Ok(());
        }

        sqlx::query(
            r#"
            UPDATE mints m
            SET status = h.status
            FROM (
                SELECT DISTINCT ON (mint_address) mint_address, status
                FROM mint_status_history
                WHERE mint_address = ANY($1)
                ORDER BY mint_address, effective_slot DESC
            ) h
            WHERE m.mint_address = h.mint_address
            "#,
        )
        .bind(mint_addresses)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn insert_mint_statuses_batch_internal(
        &self,
        statuses: &[DbMintStatus],
    ) -> Result<(), StorageError> {
        if statuses.is_empty() {
            return Ok(());
        }

        let mut tx = self.pool.begin().await?;

        for status in statuses {
            sqlx::query(
                r#"
                INSERT INTO mint_status_history
                    (mint_address, status, effective_slot, signature)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (mint_address, effective_slot) DO NOTHING
                "#,
            )
            .bind(&status.mint_address)
            .bind(&status.status)
            .bind(status.effective_slot)
            .bind(&status.signature)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn get_mint_status_at_slot_internal(
        &self,
        mint_address: &str,
        slot: i64,
    ) -> Result<MintStatusAtSlot, StorageError> {
        let row: Option<(String,)> = sqlx::query_as(
            r#"
            SELECT status FROM mint_status_history
            WHERE mint_address = $1 AND effective_slot <= $2
            ORDER BY effective_slot DESC
            LIMIT 1
            "#,
        )
        .bind(mint_address)
        .bind(slot)
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some((s,)) if s == "allowed" => Ok(MintStatusAtSlot::Allowed),
            Some((s,)) if s == "blocked" => Ok(MintStatusAtSlot::Blocked),
            // Unrecognized status is data corruption; fail closed to `Blocked` and log loudly.
            Some((other,)) => {
                warn!(
                    mint_address,
                    slot,
                    status = %other,
                    "Unrecognized mint status in mint_status_history; treating as Blocked"
                );
                Ok(MintStatusAtSlot::Blocked)
            }
            None => Ok(MintStatusAtSlot::NeverAllowed),
        }
    }

    /// Write-back from the operator's MintCache after it resolves whether
    /// the on-chain mint carries the Token-2022 PausableConfig and
    /// PermanentDelegate extensions. Both flags are always resolved in the
    /// same RPC fetch, so they're persisted together in a single update.
    /// Errors if the row doesn't exist — the indexer always lands the
    /// `mints` row before any withdrawal for that mint can reach the
    /// operator, so a missing row indicates an ordering bug.
    pub async fn set_mint_extension_flags_internal(
        &self,
        mint_address: &str,
        is_pausable: bool,
        has_permanent_delegate: bool,
    ) -> Result<(), StorageError> {
        let result = sqlx::query(
            "UPDATE mints SET is_pausable = $2, has_permanent_delegate = $3 WHERE mint_address = $1",
        )
        .bind(mint_address)
        .bind(is_pausable)
        .bind(has_permanent_delegate)
        .execute(&self.pool)
        .await?;

        if result.rows_affected() == 0 {
            return Err(StorageError::DatabaseError {
                message: format!("set_mint_extension_flags: no mints row for {mint_address}"),
            });
        }

        Ok(())
    }

    pub async fn get_mint_internal(
        &self,
        mint_address: &str,
    ) -> Result<Option<DbMint>, StorageError> {
        Ok(
            sqlx::query_as::<_, DbMint>("SELECT * FROM mints WHERE mint_address = $1")
                .bind(mint_address)
                .fetch_optional(&self.pool)
                .await?,
        )
    }

    /// Return per-mint aggregate balances for startup reconciliation.
    ///
    /// For each mint known to the DB, sums:
    /// - `total_deposits`  : ALL indexed deposits (any status), because a deposit increases
    ///   the escrow ATA balance on-chain the moment it is observed — the operator's private_channel minting
    ///   status (`pending`/`processing`/`completed`/`failed`) does not change what is on-chain.
    /// - `total_withdrawals`: only `completed` withdrawals, because only a completed
    ///   `release_funds` call actually moves tokens out of the ATA.
    ///
    /// Mints with no transactions still appear (with totals = 0) because of the LEFT JOIN.
    ///
    /// `as_of_slot` bounds the totals to what was indexed at or below that slot, so the
    /// answer describes the ledger at one point rather than at whatever moment the query
    /// happened to run. The bound sits in the JOIN, not a WHERE clause, because moving it
    /// to WHERE would discard the NULL rows the LEFT JOIN produces for a mint with no
    /// qualifying transactions and silently drop that mint from the comparison.
    pub async fn get_mint_balances_for_reconciliation_internal(
        &self,
        as_of_slot: i64,
    ) -> Result<Vec<MintDbBalance>, sqlx::Error> {
        sqlx::query_as::<_, MintDbBalance>(
            r#"
            SELECT
                m.mint_address,
                m.token_program,
                COALESCE(
                    SUM(CASE WHEN t.transaction_type = 'deposit' THEN t.amount ELSE 0 END),
                    0
                )::NUMERIC AS total_deposits,
                COALESCE(
                    SUM(CASE WHEN t.transaction_type = 'withdrawal' AND t.status = 'completed' THEN t.amount ELSE 0 END),
                    0
                )::NUMERIC AS total_withdrawals
            FROM mints m
            LEFT JOIN transactions t ON t.mint = m.mint_address AND t.slot <= $1
            GROUP BY m.mint_address, m.token_program
            "#,
        )
        .bind(as_of_slot)
        .fetch_all(&self.pool)
        .await
    }

    /// Query escrow balances by mint for continuous reconciliation checks.
    /// Only counts **completed** transactions for both deposits and withdrawals.
    /// This provides a conservative view based on finalized database state,
    /// suitable for comparing against on-chain escrow ATA balances.
    ///
    /// Returns per-mint aggregate balances where:
    /// - `total_deposits`: sum of completed deposit amounts
    /// - `total_withdrawals`: sum of completed withdrawal amounts
    ///
    /// Expected net on-chain balance = total_deposits - total_withdrawals
    pub async fn get_escrow_balances_by_mint_internal(
        &self,
    ) -> Result<Vec<MintDbBalance>, sqlx::Error> {
        sqlx::query_as::<_, MintDbBalance>(
            r#"
            SELECT
                m.mint_address,
                m.token_program,
                COALESCE(
                    SUM(CASE WHEN t.transaction_type = 'deposit' AND t.status = 'completed' THEN t.amount ELSE 0 END),
                    0
                )::NUMERIC AS total_deposits,
                COALESCE(
                    SUM(CASE WHEN t.transaction_type = 'withdrawal' AND t.status = 'completed' THEN t.amount ELSE 0 END),
                    0
                )::NUMERIC AS total_withdrawals
            FROM mints m
            LEFT JOIN transactions t ON t.mint = m.mint_address
            GROUP BY m.mint_address, m.token_program
            "#,
        )
        .fetch_all(&self.pool)
        .await
    }

    /// Per-mint sum of every unsettled transaction amount (the in-flight
    /// envelope). Both types are summed as a deliberate over-approximation: a
    /// larger envelope only ever delays detection of a real insolvency, never
    /// fabricates a false halt.
    pub async fn get_in_flight_amounts_by_mint_internal(
        &self,
    ) -> Result<Vec<MintInFlightAmount>, sqlx::Error> {
        // Sum of every in-flight row per mint: the supply-vs-custody transient bound.
        // Deposits and pending_remint raise supply; burn-side withdrawals over-count but only widen it, never false-halt.
        sqlx::query_as::<_, MintInFlightAmount>(
            r#"
            SELECT mint AS mint_address,
                   COALESCE(SUM(amount), 0)::NUMERIC AS in_flight_amount
            FROM transactions
            WHERE status IN ('pending', 'processing', 'parked', 'pending_remint')
            GROUP BY mint
            "#,
        )
        .fetch_all(&self.pool)
        .await
    }

    /// Set (or refresh) the durable reconciliation halt flag. Idempotent on the
    /// single row so repeated trips do not error or duplicate.
    pub async fn set_reconciliation_halt_internal(&self, reason: &str) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
            INSERT INTO reconciliation_halt (id, halted, reason, halted_at)
            VALUES (TRUE, TRUE, $1, NOW())
            ON CONFLICT (id) DO UPDATE
            SET halted = TRUE, reason = EXCLUDED.reason, halted_at = NOW()
            "#,
        )
        .bind(reason)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Return the halt reason/timestamp when the flag is set, else `None`.
    /// A row with `halted = FALSE` (cleared) also reads as not halted.
    pub async fn is_reconciliation_halted_internal(&self) -> Result<Option<HaltInfo>, sqlx::Error> {
        sqlx::query_as::<_, HaltInfo>(
            r#"
            SELECT reason, halted_at
            FROM reconciliation_halt
            WHERE id = TRUE AND halted = TRUE
            "#,
        )
        .fetch_optional(&self.pool)
        .await
    }

    /// Clear the halt so the pipelines can resume. Manual/runbook use only.
    pub async fn clear_reconciliation_halt_internal(&self) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
            UPDATE reconciliation_halt
            SET halted = FALSE, halted_at = NOW()
            WHERE id = TRUE
            "#,
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// `transactions.id` for every `deposit` row whose mint was not in
    /// `allowed` status at the deposit's slot, per `mint_status_history`.
    pub async fn get_orphan_deposit_ids_internal(&self) -> Result<Vec<i64>, sqlx::Error> {
        let rows: Vec<(i64,)> = sqlx::query_as(
            r#"
            SELECT t.id
            FROM transactions t
            LEFT JOIN LATERAL (
                SELECT status
                FROM mint_status_history h
                WHERE h.mint_address = t.mint
                  AND h.effective_slot <= t.slot
                ORDER BY h.effective_slot DESC
                LIMIT 1
            ) latest ON true
            WHERE t.transaction_type = 'deposit'
              AND (latest.status IS NULL OR latest.status = 'blocked')
            ORDER BY t.id ASC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|(id,)| id).collect())
    }

    pub async fn close(&self) -> Result<(), sqlx::Error> {
        info!("Closing database connection pool...");
        self.pool.close().await;
        info!("Database connection pool closed");
        Ok(())
    }

    pub async fn count_pending_transactions_internal(
        &self,
        transaction_type: TransactionType,
    ) -> Result<i64, sqlx::Error> {
        let (count,): (i64,) = sqlx::query_as(&format!(
            "SELECT COUNT(*) FROM transactions WHERE {} = $1 AND {} = $2",
            transaction_cols::STATUS,
            transaction_cols::TRANSACTION_TYPE,
        ))
        .bind(TransactionStatus::Pending)
        .bind(transaction_type)
        .fetch_one(&self.pool)
        .await?;

        Ok(count)
    }

    /// Lowest and highest withdrawal nonce at or above `min_nonce` that still
    /// owes a release.
    ///
    /// The status set is the complement of the terminal ones. `completed` was
    /// released, `failed` and `failed_reminted` were written off or refunded, and
    /// none of the three can ever need their generation's window again, so the
    /// rotation is free to move past them. Everything else may still have to land
    /// on the bitmap as it stands today, `manual_review` included, because a human
    /// can still resolve one of those rows into a release.
    ///
    /// Both aggregates are NULL together when nothing matches, and MIN/MAX skip
    /// NULL nonces on their own, so a row without one contributes no bound.
    pub async fn unreleased_withdrawal_nonce_bounds_internal(
        &self,
        min_nonce: i64,
    ) -> Result<Option<(i64, i64)>, sqlx::Error> {
        let bounds: (Option<i64>, Option<i64>) = sqlx::query_as(&format!(
            r#"
            SELECT MIN({nonce}), MAX({nonce}) FROM transactions
            WHERE {ttype} = $1
              AND {nonce} >= $2
              AND {status} IN ('pending', 'processing', 'parked', 'pending_remint', 'manual_review')
            "#,
            nonce = transaction_cols::WITHDRAWAL_NONCE,
            ttype = transaction_cols::TRANSACTION_TYPE,
            status = transaction_cols::STATUS,
        ))
        .bind(TransactionType::Withdrawal)
        .bind(min_nonce)
        .fetch_one(&self.pool)
        .await?;

        Ok(match bounds {
            (Some(lowest), Some(highest)) => Some((lowest, highest)),
            _ => None,
        })
    }

    pub async fn get_completed_withdrawal_nonces_internal(
        &self,
        min_nonce: i64,
        max_nonce: i64,
    ) -> Result<Vec<i64>, sqlx::Error> {
        let nonces: Vec<(i64,)> = sqlx::query_as(
            r#"
            SELECT withdrawal_nonce FROM transactions
            WHERE transaction_type = 'withdrawal'
              AND status = 'completed'
              AND withdrawal_nonce >= $1
              AND withdrawal_nonce < $2
            ORDER BY withdrawal_nonce ASC
            "#,
        )
        .bind(min_nonce)
        .bind(max_nonce)
        .fetch_all(&self.pool)
        .await?;

        Ok(nonces.into_iter().map(|(n,)| n).collect())
    }
}

#[cfg(test)]
mod password_guard_tests {
    use super::database_url_password_is_blank;

    #[test]
    fn flags_blank_and_missing_password() {
        // Set-but-empty password (blanked template) is flagged.
        assert!(database_url_password_is_blank(
            "postgres://user:@host:5434/indexer"
        ));
        // No password at all is flagged.
        assert!(database_url_password_is_blank(
            "postgres://user@host:5434/indexer"
        ));
        // No userinfo at all is flagged.
        assert!(database_url_password_is_blank(
            "postgres://host:5434/indexer"
        ));
        // A real password is not blank.
        assert!(!database_url_password_is_blank(
            "postgres://user:secret@host:5434/indexer"
        ));
        // A percent-encoded password is a real, non-empty credential.
        assert!(!database_url_password_is_blank(
            "postgres://user:p%40ss@host:5434/indexer"
        ));
        // Unparseable URLs are not flagged; sqlx surfaces the real connect error.
        assert!(!database_url_password_is_blank("not-a-valid-url"));
    }
}
