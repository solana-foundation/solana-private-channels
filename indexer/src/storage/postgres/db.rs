use futures::future::BoxFuture;
use sqlx::{postgres::PgPoolOptions, Acquire, PgConnection, PgPool};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::{info, warn};

use crate::{
    error::StorageError,
    storage::common::models::{
        DbMint, DbMintStatus, DbTransaction, HaltInfo, MintDbBalance, MintInFlightAmount,
        MintStatusAtSlot, StoredSig, TransactionStatus, TransactionType,
    },
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

        // Tree generation the sender still owes the chain. NULL means none owed.
        // Written before the rotation is dispatched, cleared only once a chain read
        // shows the tree reached it, so a crash leaves the rotation re-armable.
        sqlx::query(
            "ALTER TABLE indexer_state ADD COLUMN IF NOT EXISTS owed_rotation_target BIGINT",
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

        // Drop tables with CASCADE to handle dependencies
        sqlx::query("DROP TABLE IF EXISTS pending_release_signatures CASCADE")
            .execute(&self.pool)
            .await?;

        sqlx::query("DROP TABLE IF EXISTS pending_remint_signatures CASCADE")
            .execute(&self.pool)
            .await?;

        sqlx::query("DROP TABLE IF EXISTS reconciliation_halt CASCADE")
            .execute(&self.pool)
            .await?;

        sqlx::query("DROP TABLE IF EXISTS transactions CASCADE")
            .execute(&self.pool)
            .await?;

        sqlx::query("DROP TABLE IF EXISTS indexer_state CASCADE")
            .execute(&self.pool)
            .await?;

        sqlx::query("DROP TABLE IF EXISTS mints CASCADE")
            .execute(&self.pool)
            .await?;

        // Drop sequences
        sqlx::query("DROP SEQUENCE IF EXISTS withdrawal_nonce_seq CASCADE")
            .execute(&self.pool)
            .await?;

        // Drop enum types
        sqlx::query("DROP TYPE IF EXISTS transaction_status CASCADE")
            .execute(&self.pool)
            .await?;

        sqlx::query("DROP TYPE IF EXISTS transaction_type CASCADE")
            .execute(&self.pool)
            .await?;

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
                {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}
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
                {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}
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
                {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}
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

    pub async fn get_owed_rotation_target_internal(
        &self,
        program_type: &str,
    ) -> Result<Option<u64>, sqlx::Error> {
        let result: Option<(Option<i64>,)> = sqlx::query_as(
            "SELECT owed_rotation_target FROM indexer_state WHERE program_type = $1",
        )
        .bind(program_type)
        .fetch_optional(&self.pool)
        .await?;

        Ok(result
            .and_then(|(target,)| target)
            .map(|target| target as u64))
    }

    pub async fn set_owed_rotation_target_internal(
        &self,
        program_type: &str,
        target_tree_index: u64,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
            INSERT INTO indexer_state (program_type, owed_rotation_target, updated_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (program_type)
            DO UPDATE SET
                owed_rotation_target = EXCLUDED.owed_rotation_target,
                updated_at = NOW()
            "#,
        )
        .bind(program_type)
        .bind(target_tree_index as i64)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Clear only if the stored target is still the one the caller proved landed, so a
    /// clear can never retire a rotation that is still owed.
    pub async fn clear_owed_rotation_target_internal(
        &self,
        program_type: &str,
        target_tree_index: u64,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
            UPDATE indexer_state
            SET owed_rotation_target = NULL
            WHERE program_type = $1
              AND owed_rotation_target = $2
            "#,
        )
        .bind(program_type)
        .bind(target_tree_index as i64)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn get_and_lock_pending_transactions_internal(
        &self,
        transaction_type: TransactionType,
        limit: i64,
    ) -> Result<Vec<DbTransaction>, sqlx::Error> {
        // Deposits dequeue FIFO by created_at. Withdrawals dequeue by nonce and
        // enforce a frontier: never hand out a nonce while a lower one is still
        // active, or the lower nonce gets stranded on a closed SMT tree after a
        // boundary rotation. The `< MIN(lower active nonce)` filter is the
        // frontier; dropping SKIP LOCKED stops a second worker from skipping a
        // locked lower nonce and leapfrogging to a higher one.
        let is_withdrawal = matches!(transaction_type, TransactionType::Withdrawal);
        let (order_col, frontier_filter, lock_clause) = if is_withdrawal {
            (
                transaction_cols::WITHDRAWAL_NONCE,
                // NULL-nonce rows are poison (e.g. a corrupt withdrawal); they have
                // no tree, so the frontier doesn't apply - still dequeue them so the
                // processor can quarantine them. ORDER BY ... ASC sorts them last.
                format!(
                    " AND ({nonce} IS NULL OR {nonce} < COALESCE((SELECT MIN({nonce}) \
                     FROM transactions WHERE {ttype} = $2 AND {status} IN \
                     ('processing', 'parked', 'pending_remint', 'manual_review')), {max}))",
                    nonce = transaction_cols::WITHDRAWAL_NONCE,
                    ttype = transaction_cols::TRANSACTION_TYPE,
                    status = transaction_cols::STATUS,
                    max = i64::MAX,
                ),
                "FOR UPDATE",
            )
        } else {
            (
                transaction_cols::CREATED_AT,
                String::new(),
                "FOR UPDATE SKIP LOCKED",
            )
        };

        // Use a transaction to ensure atomicity
        let mut tx = self.pool.begin().await?;

        let mut transactions = sqlx::query_as::<_, DbTransaction>(&format!(
            r#"
            SELECT
                {}, {}, {}, {}, {}, {}, {}, {}, {}, {},
                {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}
            FROM transactions
            WHERE {} = $1 AND {} = $2{frontier}
            ORDER BY {} ASC
            LIMIT $3
            {lock}
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
            // Filters
            transaction_cols::STATUS,
            transaction_cols::TRANSACTION_TYPE,
            // Ordering: nonce for withdrawals, created_at for deposits
            order_col,
            frontier = frontier_filter,
            lock = lock_clause,
        ))
        .bind(TransactionStatus::Pending)
        .bind(transaction_type)
        .bind(limit)
        .fetch_all(&mut *tx)
        .await?;

        // Update status to Processing and RETURNING the trigger-bumped
        // `updated_at`, so the fetched row carries its true post-lock token (the
        // deposit sender CASes on it at broadcast, not the stale Pending value).
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

    /// True if any withdrawal with a lower nonce is unresolved and not yet handed
    /// to the sender. Gates the boundary rotation: rotating past such a nonce
    /// would strand it on the closed tree. `Processing` rows are excluded on
    /// purpose - they are already dispatched ahead of the rotation, so the
    /// sender's in-flight guard holds the rotation until they settle.
    pub async fn has_active_withdrawal_below_internal(
        &self,
        nonce: i64,
    ) -> Result<bool, sqlx::Error> {
        let exists: bool = sqlx::query_scalar(&format!(
            r#"
            SELECT EXISTS (
                SELECT 1 FROM transactions
                WHERE {ttype} = $2
                  AND {nonce} < $1
                  AND {status} IN ('pending', 'parked', 'pending_remint', 'manual_review')
            )
            "#,
            ttype = transaction_cols::TRANSACTION_TYPE,
            nonce = transaction_cols::WITHDRAWAL_NONCE,
            status = transaction_cols::STATUS,
        ))
        .bind(nonce)
        .bind(TransactionType::Withdrawal)
        .fetch_one(&self.pool)
        .await?;
        Ok(exists)
    }

    /// Lowest withdrawal nonce below `nonce` that still owes a release, or `None` if every
    /// lower nonce is terminal. Gates the sender's rotation submit: rotating past a nonce
    /// that still owes a release closes the only tree that release can ever land on.
    ///
    /// Evaluated fresh on every attempt, so it covers rows that entered a live status
    /// after the rotation was dispatched (a recovery demote to `pending`, a park, a queued
    /// remint, a quarantine). `Processing` is included where
    /// `has_active_withdrawal_below_internal` omits it: that query runs in the processor,
    /// where a `Processing` row is one the sender holds in memory, and this one must hold
    /// after a restart dropped that memory. Terminal means `completed` (released) or
    /// `failed`/`failed_reminted` (written off or reminted), which are safe to rotate past.
    pub async fn lowest_unreleased_withdrawal_below_internal(
        &self,
        nonce: i64,
    ) -> Result<Option<i64>, sqlx::Error> {
        let lowest: Option<i64> = sqlx::query_scalar(&format!(
            r#"
            SELECT MIN({nonce_col}) FROM transactions
            WHERE {ttype} = $2
              AND {nonce_col} < $1
              AND {status} IN ('pending', 'processing', 'parked', 'pending_remint', 'manual_review')
            "#,
            ttype = transaction_cols::TRANSACTION_TYPE,
            nonce_col = transaction_cols::WITHDRAWAL_NONCE,
            status = transaction_cols::STATUS,
        ))
        .bind(nonce)
        .bind(TransactionType::Withdrawal)
        .fetch_one(&self.pool)
        .await?;
        Ok(lowest)
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
        transaction_type: TransactionType,
        threshold: Duration,
        limit: i64,
    ) -> Result<Vec<DbTransaction>, sqlx::Error> {
        let threshold_secs = threshold.as_secs() as f64;
        sqlx::query_as::<_, DbTransaction>(&format!(
            r#"
            SELECT
                {}, {}, {}, {}, {}, {}, {}, {}, {}, {},
                {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}
            FROM transactions
            WHERE {} = 'processing'
              AND {} < NOW() - make_interval(secs => $1)
              AND {} = $2
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
            // Filters
            transaction_cols::STATUS,
            transaction_cols::UPDATED_AT,
            transaction_cols::TRANSACTION_TYPE,
            // Ordering (FIFO over stale)
            transaction_cols::UPDATED_AT,
        ))
        .bind(threshold_secs)
        .bind(transaction_type)
        .bind(limit)
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
        transaction_type: TransactionType,
        threshold: Duration,
        limit: i64,
    ) -> Result<Vec<DbTransaction>, sqlx::Error> {
        let threshold_secs = threshold.as_secs() as f64;
        sqlx::query_as::<_, DbTransaction>(&format!(
            r#"
            SELECT
                {}, {}, {}, {}, {}, {}, {}, {}, {}, {},
                {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}
            FROM transactions
            WHERE {} = 'parked'
              AND {} < NOW() - make_interval(secs => $1)
              AND {} = $2
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
            // Filters
            transaction_cols::STATUS,
            transaction_cols::UPDATED_AT,
            transaction_cols::TRANSACTION_TYPE,
            // Ordering (FIFO over stale)
            transaction_cols::UPDATED_AT,
        ))
        .bind(threshold_secs)
        .bind(transaction_type)
        .bind(limit)
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
    pub async fn try_quarantine_processing_internal(
        &self,
        transaction_id: i64,
        expected_updated_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<bool, sqlx::Error> {
        let result = sqlx::query(
            r#"
            UPDATE transactions
            SET status = 'manual_review',
                processed_at = NOW()
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

    /// Transitions a withdrawal to PendingRemint status, storing the
    /// withdrawal signatures needed for the finality check on restart.
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

    /// Flip every `Pending`/`Processing` withdrawal to `ManualReview`.
    ///
    /// `exclude_id` is the poison row that the caller has already quarantined
    /// via the async `storage_tx` writer. That update may not have hit the DB
    /// yet when this sweep runs, so the row's status is still
    /// `Pending`/`Processing` here; excluding it prevents a second
    /// `ManualReview` webhook for the same transaction.
    ///
    /// Terminal rows are left untouched so the webhook does not re-alert on
    /// already-handled transactions. Returns the number of rows affected.
    ///
    /// Scope is intentionally DB-wide over `transaction_type = 'withdrawal'`
    /// to match the fetcher's own scope. The data model assumes a single
    /// withdrawal operator per database; multi-instance isolation would
    /// require an `instance_pda` column on `transactions` that does not exist
    /// today.
    // Coverage-ignore rationale (category b, defensive recovery):
    //   `quarantine_all_active_withdrawals_internal` is only invoked by
    //   the poison-pill pipeline in `operator/processor.rs`
    //   (`halt_withdrawal_pipeline`), which is itself LCOV-excluded —
    //   integration tests do not produce malformed rows that would trip
    //   it. The SQL itself is trivial; the behavior is covered via the
    //   `Storage::Mock` variant in in-crate tests.
    pub async fn quarantine_all_active_withdrawals_internal(
        &self,
        exclude_id: Option<i64>,
    ) -> Result<u64, sqlx::Error> {
        let result = sqlx::query(
            r#"
            UPDATE transactions
            SET status = 'manual_review', updated_at = NOW()
            WHERE transaction_type = 'withdrawal'
              AND status IN ('pending', 'processing', 'parked')
              AND ($1::BIGINT IS NULL OR id <> $1)
            "#,
        )
        .bind(exclude_id)
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

    /// Every mint address the DB knows: the mint universe that runtime reconciliation checks.
    ///
    /// Addresses only: runtime compares on-chain custody against on-chain channel
    /// supply, so no ledger amount is read here and none is aggregated.
    pub async fn get_mint_addresses_internal(&self) -> Result<Vec<String>, sqlx::Error> {
        sqlx::query_scalar("SELECT mint_address FROM mints")
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
