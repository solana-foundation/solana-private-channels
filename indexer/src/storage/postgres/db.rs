use sqlx::{postgres::PgPoolOptions, PgPool};
use tracing::{info, warn};

use crate::{
    error::StorageError,
    storage::common::models::{
        DbMint, DbMintStatus, DbTransaction, MintDbBalance, MintStatusAtSlot, TransactionStatus,
        TransactionType,
    },
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
}

#[derive(Clone)]
pub struct PostgresDb {
    pool: PgPool,
}

impl PostgresDb {
    pub async fn new(config: &PostgresConfig) -> Result<Self, sqlx::Error> {
        let pool = PgPoolOptions::new()
            .max_connections(config.max_connections)
            .connect(&config.database_url)
            .await?;

        Ok(Self { pool })
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
                signature TEXT NOT NULL UNIQUE,
                slot BIGINT NOT NULL,
                initiator TEXT NOT NULL,
                recipient TEXT NOT NULL,
                mint TEXT NOT NULL,
                amount BIGINT NOT NULL,
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

        // Add unique index for signatures and counterpart_signature
        sqlx::query(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_transactions_signature ON transactions (signature)",
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
            CREATE TABLE IF NOT EXISTS indexer_state (
                program_type TEXT PRIMARY KEY,
                last_committed_slot BIGINT NOT NULL DEFAULT 0,
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

        info!("Database schema initialized");
        Ok(())
    }

    pub async fn drop_tables(&self) -> Result<(), sqlx::Error> {
        info!("Dropping database tables...");

        // Drop tables with CASCADE to handle dependencies
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
            "SELECT {} FROM transactions WHERE {} = $1",
            transaction_cols::ID,
            transaction_cols::SIGNATURE
        ))
        .bind(&transaction.signature)
        .fetch_optional(&self.pool)
        .await?;

        if let Some((id,)) = existing {
            return Ok(id);
        }

        let result: Option<(i64,)> = sqlx::query_as(&format!(
            r#"
            INSERT INTO transactions (
                {}, {}, {}, {}, {}, {}, {}, {}, {}, {}
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT ({}) DO NOTHING
            RETURNING {}
            "#,
            transaction_cols::SIGNATURE,
            transaction_cols::SLOT,
            transaction_cols::INITIATOR,
            transaction_cols::RECIPIENT,
            transaction_cols::MINT,
            transaction_cols::AMOUNT,
            transaction_cols::MEMO,
            transaction_cols::TRANSACTION_TYPE,
            transaction_cols::STATUS,
            transaction_cols::TRACE_ID,
            transaction_cols::SIGNATURE,
            transaction_cols::ID,
        ))
        .bind(&transaction.signature)
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
            "SELECT {} FROM transactions WHERE {} = $1",
            transaction_cols::ID,
            transaction_cols::SIGNATURE
        ))
        .bind(&transaction.signature)
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
                "SELECT {} FROM transactions WHERE {} = $1",
                transaction_cols::ID,
                transaction_cols::SIGNATURE
            ))
            .bind(&transaction.signature)
            .fetch_optional(&mut *tx)
            .await?;

            if let Some((id,)) = existing {
                ids.push(id);
                continue;
            }

            // Insert new transaction
            let result: Option<(i64,)> = sqlx::query_as(&format!(
                r#"
                INSERT INTO transactions (
                    {}, {}, {}, {}, {}, {}, {}, {}, {}, {}
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                ON CONFLICT ({}) DO NOTHING
                RETURNING {}
                "#,
                transaction_cols::SIGNATURE,
                transaction_cols::SLOT,
                transaction_cols::INITIATOR,
                transaction_cols::RECIPIENT,
                transaction_cols::MINT,
                transaction_cols::AMOUNT,
                transaction_cols::MEMO,
                transaction_cols::TRANSACTION_TYPE,
                transaction_cols::STATUS,
                transaction_cols::TRACE_ID,
                transaction_cols::SIGNATURE,
                transaction_cols::ID,
            ))
            .bind(&transaction.signature)
            .bind(transaction.slot)
            .bind(&transaction.initiator)
            .bind(&transaction.recipient)
            .bind(&transaction.mint)
            .bind(transaction.amount)
            .bind(&transaction.memo)
            .bind(transaction.transaction_type)
            .bind(transaction.status)
            .bind(&transaction.trace_id)
            .fetch_optional(&mut *tx)
            .await?;

            if let Some((id,)) = result {
                ids.push(id);
            } else {
                // Conflict occurred, fetch existing ID
                let (id,): (i64,) = sqlx::query_as(&format!(
                    "SELECT {} FROM transactions WHERE {} = $1",
                    transaction_cols::ID,
                    transaction_cols::SIGNATURE
                ))
                .bind(&transaction.signature)
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
                {}, {}, {}, {}, {}, {}, {}, {}, {}, {}
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
                {}, {}, {}, {}, {}, {}, {}, {}, {}, {}
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
                {}, {}, {}, {}, {}, {}, {}, {}, {}, {}
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
        let result: Option<(i64,)> =
            sqlx::query_as("SELECT last_committed_slot FROM indexer_state WHERE program_type = $1")
                .bind(program_type)
                .fetch_optional(&self.pool)
                .await?;

        Ok(result.map(|(slot,)| slot as u64))
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
        let transactions = sqlx::query_as::<_, DbTransaction>(&format!(
            r#"
            SELECT
                {}, {}, {}, {}, {}, {}, {}, {}, {}, {},
                {}, {}, {}, {}, {}, {}, {}, {}, {}, {}
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

        // Update status to Processing in a single query
        if !transactions.is_empty() {
            let ids: Vec<i64> = transactions.iter().map(|txn| txn.id).collect();
            sqlx::query(&format!(
                "UPDATE transactions SET {} = $1 WHERE {} = ANY($2)",
                transaction_cols::STATUS,
                transaction_cols::ID
            ))
            .bind(TransactionStatus::Processing)
            .bind(&ids)
            .execute(&mut *tx)
            .await?;
        }

        // Commit to release locks with Processing status
        tx.commit().await?;

        Ok(transactions)
    }

    pub async fn update_transaction_status_internal(
        &self,
        transaction_id: i64,
        status: TransactionStatus,
        counterpart_signature: Option<String>,
        processed_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
            UPDATE transactions
            SET
                status = $2,
                counterpart_signature = $3,
                processed_at = $4,
                updated_at = NOW()
            WHERE id = $1
            "#,
        )
        .bind(transaction_id)
        .bind(status)
        .bind(counterpart_signature)
        .bind(processed_at)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Transitions a withdrawal to PendingRemint status, storing the
    /// withdrawal signatures needed for the finality check on restart.
    pub async fn set_pending_remint_internal(
        &self,
        transaction_id: i64,
        remint_signatures: Vec<String>,
        remint_last_valid_block_heights: Vec<i64>,
        deadline_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), sqlx::Error> {
        let result = sqlx::query(
            r#"
            UPDATE transactions
            SET
                status = $2,
                remint_signatures = $3,
                remint_last_valid_block_heights = $4,
                pending_remint_deadline_at = $5,
                updated_at = NOW()
            WHERE id = $1
                AND status = 'processing'
            "#,
        )
        .bind(transaction_id)
        .bind(TransactionStatus::PendingRemint)
        .bind(remint_signatures)
        .bind(remint_last_valid_block_heights)
        .bind(deadline_at)
        .execute(&self.pool)
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
        let result = sqlx::query(
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
        .execute(&self.pool)
        .await?;

        if result.rows_affected() == 0 {
            return Err(sqlx::Error::RowNotFound);
        }

        Ok(())
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
              AND status IN ('pending', 'processing')
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
                INSERT INTO mints (mint_address, decimals, token_program)
                VALUES ($1, $2, $3)
                ON CONFLICT (mint_address) DO UPDATE
                SET decimals = EXCLUDED.decimals,
                    token_program = EXCLUDED.token_program
                "#,
            )
            .bind(&mint.mint_address)
            .bind(mint.decimals)
            .bind(&mint.token_program)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
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
    pub async fn get_mint_balances_for_reconciliation_internal(
        &self,
    ) -> Result<Vec<MintDbBalance>, sqlx::Error> {
        sqlx::query_as::<_, MintDbBalance>(
            r#"
            SELECT
                m.mint_address,
                m.token_program,
                COALESCE(
                    SUM(CASE WHEN t.transaction_type = 'deposit' THEN t.amount ELSE 0 END),
                    0
                )::BIGINT AS total_deposits,
                COALESCE(
                    SUM(CASE WHEN t.transaction_type = 'withdrawal' AND t.status = 'completed' THEN t.amount ELSE 0 END),
                    0
                )::BIGINT AS total_withdrawals
            FROM mints m
            LEFT JOIN transactions t ON t.mint = m.mint_address
            GROUP BY m.mint_address, m.token_program
            "#,
        )
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
                )::BIGINT AS total_deposits,
                COALESCE(
                    SUM(CASE WHEN t.transaction_type = 'withdrawal' AND t.status = 'completed' THEN t.amount ELSE 0 END),
                    0
                )::BIGINT AS total_withdrawals
            FROM mints m
            LEFT JOIN transactions t ON t.mint = m.mint_address
            GROUP BY m.mint_address, m.token_program
            "#,
        )
        .fetch_all(&self.pool)
        .await
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
