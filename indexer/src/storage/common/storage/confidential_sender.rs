//! Storage the withdraw sender needs for confidential releases: balance waits, the
//! read-freshness seed, and rebalance requests.

use crate::error::StorageError;
use crate::storage::common::storage::Storage;
use chrono::{DateTime, Utc};

/// Which way a rebalance moves the escrow's balance.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RebalanceDirection {
    ToPublic,
    ToConfidential,
}

impl RebalanceDirection {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ToPublic => "to_public",
            Self::ToConfidential => "to_confidential",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "to_public" => Some(Self::ToPublic),
            "to_confidential" => Some(Self::ToConfidential),
            _ => None,
        }
    }
}

/// Request lifecycle. `sent` is written with the signature before broadcast.
pub mod rebalance_status {
    pub const PENDING: &str = "pending";
    pub const SENT: &str = "sent";
    pub const COMPLETED: &str = "completed";
    pub const REFUSED: &str = "refused";
    pub const FAILED: &str = "failed";
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RebalanceRequest {
    pub id: i64,
    pub mint: String,
    pub direction: RebalanceDirection,
    pub amount: u64,
    pub expected_available: u64,
    pub status: String,
    pub signature: Option<String>,
    pub error: Option<String>,
    /// The sent attempt's blockhash validity, so it is judged dead only once it expired.
    pub last_valid_block_height: Option<u64>,
    pub blockhash_slot: Option<u64>,
}

/// In-memory mirror of the tables these calls touch, for `MockStorage`.
#[cfg(any(test, feature = "test-mock-storage"))]
#[derive(Clone, Debug, Default)]
pub struct ConfidentialSenderMock {
    pub rebalance_requests: Vec<RebalanceRequest>,
    pub park_labels: std::collections::HashMap<i64, String>,
    pub landing_slot: Option<u64>,
}

impl Storage {
    /// Send a confidential release back to `Pending` until `not_before`, keeping the first
    /// `wait_started_at`. Only an unjournaled `Processing` row owned under `lease` moves.
    pub async fn requeue_confidential_wait(
        &self,
        transaction_id: i64,
        lease: DateTime<Utc>,
        not_before: DateTime<Utc>,
        wait_started_at: DateTime<Utc>,
    ) -> Result<bool, StorageError> {
        match self {
            Storage::Postgres(db) => Ok(db
                .requeue_confidential_wait_internal(
                    transaction_id,
                    lease,
                    not_before,
                    wait_started_at,
                )
                .await?),
            #[cfg(any(test, feature = "test-mock-storage"))]
            Storage::Mock(mock) => {
                mock.requeue_confidential_wait(transaction_id, lease, not_before, wait_started_at)
                    .await
            }
        }
    }

    /// Highest blockhash slot journaled for any release: a floor for the sender's reads
    /// at boot, so none answers from before its own last transaction.
    pub async fn max_release_journal_slot(&self) -> Result<Option<u64>, StorageError> {
        match self {
            Storage::Postgres(db) => Ok(db.max_release_journal_slot_internal().await?),
            #[cfg(any(test, feature = "test-mock-storage"))]
            Storage::Mock(mock) => mock.max_release_journal_slot().await,
        }
    }

    /// Record the label a journaled release parks with once its gate proves nothing landed.
    pub async fn set_confidential_park_label(
        &self,
        transaction_id: i64,
        label: &str,
    ) -> Result<(), StorageError> {
        match self {
            Storage::Postgres(db) => Ok(db
                .set_confidential_park_label_internal(transaction_id, label)
                .await?),
            #[cfg(any(test, feature = "test-mock-storage"))]
            Storage::Mock(mock) => {
                mock.set_confidential_park_label(transaction_id, label)
                    .await
            }
        }
    }

    /// Park labels of rows still waiting on their journal gate, for the sender at boot.
    pub async fn get_confidential_park_labels(&self) -> Result<Vec<(i64, String)>, StorageError> {
        match self {
            Storage::Postgres(db) => Ok(db.get_confidential_park_labels_internal().await?),
            #[cfg(any(test, feature = "test-mock-storage"))]
            Storage::Mock(mock) => mock.get_confidential_park_labels().await,
        }
    }

    /// Raise the stored landing slot of the sender's last own confirmed transaction.
    pub async fn record_confidential_landing_slot(&self, slot: u64) -> Result<(), StorageError> {
        match self {
            Storage::Postgres(db) => Ok(db.record_confidential_landing_slot_internal(slot).await?),
            #[cfg(any(test, feature = "test-mock-storage"))]
            Storage::Mock(mock) => mock.record_confidential_landing_slot(slot).await,
        }
    }

    pub async fn confidential_landing_slot(&self) -> Result<Option<u64>, StorageError> {
        match self {
            Storage::Postgres(db) => Ok(db.confidential_landing_slot_internal().await?),
            #[cfg(any(test, feature = "test-mock-storage"))]
            Storage::Mock(mock) => mock.confidential_landing_slot().await,
        }
    }

    /// When the oldest parked confidential withdrawal was parked.
    pub async fn oldest_confidential_park(&self) -> Result<Option<DateTime<Utc>>, StorageError> {
        match self {
            Storage::Postgres(db) => Ok(db.oldest_confidential_park_internal().await?),
            #[cfg(any(test, feature = "test-mock-storage"))]
            Storage::Mock(mock) => mock.oldest_confidential_park().await,
        }
    }

    pub async fn insert_rebalance_request(
        &self,
        mint: &str,
        direction: RebalanceDirection,
        amount: u64,
        expected_available: u64,
    ) -> Result<i64, StorageError> {
        match self {
            Storage::Postgres(db) => Ok(db
                .insert_rebalance_request_internal(mint, direction, amount, expected_available)
                .await?),
            #[cfg(any(test, feature = "test-mock-storage"))]
            Storage::Mock(mock) => {
                mock.insert_rebalance_request(mint, direction, amount, expected_available)
                    .await
            }
        }
    }

    pub async fn get_rebalance_requests(
        &self,
        status: &str,
    ) -> Result<Vec<RebalanceRequest>, StorageError> {
        match self {
            Storage::Postgres(db) => Ok(db.get_rebalance_requests_internal(status).await?),
            #[cfg(any(test, feature = "test-mock-storage"))]
            Storage::Mock(mock) => mock.get_rebalance_requests(status).await,
        }
    }

    /// CAS a request from `pending` to `sent`, journaling the attempt before it is broadcast.
    pub async fn claim_rebalance_request(
        &self,
        id: i64,
        signature: &str,
        last_valid_block_height: u64,
        blockhash_slot: u64,
    ) -> Result<bool, StorageError> {
        match self {
            Storage::Postgres(db) => Ok(db
                .claim_rebalance_request_internal(
                    id,
                    signature,
                    last_valid_block_height,
                    blockhash_slot,
                )
                .await?),
            #[cfg(any(test, feature = "test-mock-storage"))]
            Storage::Mock(mock) => {
                mock.claim_rebalance_request(id, signature, last_valid_block_height, blockhash_slot)
                    .await
            }
        }
    }

    /// CAS a request from `from` to `to`; false when another writer moved it first.
    pub async fn transition_rebalance_request(
        &self,
        id: i64,
        from: &str,
        to: &str,
        signature: Option<String>,
        error: Option<String>,
    ) -> Result<bool, StorageError> {
        match self {
            Storage::Postgres(db) => Ok(db
                .transition_rebalance_request_internal(id, from, to, signature, error)
                .await?),
            #[cfg(any(test, feature = "test-mock-storage"))]
            Storage::Mock(mock) => {
                mock.transition_rebalance_request(id, from, to, signature, error)
                    .await
            }
        }
    }
}
