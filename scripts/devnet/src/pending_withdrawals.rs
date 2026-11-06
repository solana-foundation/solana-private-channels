//! The withdraw operator's releases in flight, read from the indexer DB, so an operator
//! is not removed while a release it may have signed is still unconfirmed.

/// The only status with a release transaction in flight. `pending` rows are not sent yet
/// (the new operator sends them) and `manual_review` rows are parked, so neither blocks.
pub const IN_FLIGHT_STATUS: &str = "processing";

/// One row: the in-flight count and the oldest in-flight withdrawal.
pub fn pending_query() -> String {
    format!(
        "SELECT count(*) OVER (), id, status::text, withdrawal_nonce, signature, created_at::text \
         FROM transactions WHERE transaction_type = 'withdrawal' \
         AND status::text = '{IN_FLIGHT_STATUS}' ORDER BY created_at, id LIMIT 1"
    )
}

/// The oldest unsettled withdrawal and how many there are.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pending {
    pub count: i64,
    pub id: i64,
    pub status: String,
    pub nonce: Option<i64>,
    pub signature: String,
    pub created_at: String,
}

impl Pending {
    fn describe(&self) -> String {
        let nonce = self.nonce.map_or("none".to_string(), |n| n.to_string());
        format!(
            "{} withdrawal(s) in flight; oldest: id {} status {} nonce {} created {} burn {}",
            self.count, self.id, self.status, nonce, self.created_at, self.signature
        )
    }
}

/// Refuses while any release is in flight; `force` turns that into a warning.
pub fn gate(pending: Option<&Pending>, force: bool) -> Result<Option<String>, String> {
    match pending {
        None => Ok(None),
        Some(p) if force => Ok(Some(format!(
            "WARNING: --force: removing the operator with {}",
            p.describe()
        ))),
        Some(p) => Err(format!(
            "{}. Wait for them to confirm and rerun, or pass --force",
            p.describe()
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(count: i64) -> Pending {
        Pending {
            count,
            id: 7,
            status: "processing".into(),
            nonce: Some(3),
            signature: "Burn1".into(),
            created_at: "2026-11-06 10:00:00+00".into(),
        }
    }

    #[test]
    fn refuses_with_the_count_and_the_oldest_row() {
        let err = gate(Some(&row(2)), false).unwrap_err();
        for part in [
            "2 withdrawal(s) in flight",
            "id 7",
            "processing",
            "nonce 3",
            "Burn1",
        ] {
            assert!(err.contains(part), "{err}");
        }
    }

    #[test]
    fn force_warns_and_passes() {
        let warning = gate(Some(&row(1)), true).unwrap().unwrap();
        assert!(warning.starts_with("WARNING: --force"), "{warning}");
        assert_eq!(gate(None, false).unwrap(), None);
    }

    #[test]
    fn the_query_counts_only_withdrawals_in_flight() {
        let q = pending_query();
        assert!(q.contains("status::text = 'processing'"), "{q}");
        assert!(q.contains("transaction_type = 'withdrawal'"), "{q}");
        assert!(!q.contains("NOT IN"), "{q}");
    }
}
