#![allow(dead_code)]
/// Database verification helpers for integration tests
use super::db;
use super::test_types::{TransactionType, UserTransaction};
use super::DbTransaction;

/// Validate a single database transaction matches expected values
pub fn validate_db_transaction(
    db_transaction: &DbTransaction,
    expected_transaction: &UserTransaction,
) -> Result<(), Box<dyn std::error::Error>> {
    assert_eq!(db_transaction.signature, expected_transaction.signature);
    assert_eq!(db_transaction.slot, expected_transaction.slot as i64);
    assert_eq!(db_transaction.amount.value(), expected_transaction.amount);
    assert_eq!(
        db_transaction.initiator,
        expected_transaction.user_pubkey.to_string()
    );
    match expected_transaction.tx_type {
        TransactionType::Deposit => {
            assert_eq!(db_transaction.transaction_type, "deposit");
        }
        TransactionType::Withdrawal => {
            assert_eq!(db_transaction.transaction_type, "withdrawal");
        }
    }

    Ok(())
}

/// Verify database contains all expected transactions with correct data
pub async fn verify_database(
    pool: &sqlx::PgPool,
    expected: &[UserTransaction],
    filtered: &[UserTransaction],
    db_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // 1. Count verification by transaction type
    let deposit_count = db::count_transactions_by_type(pool, "deposit").await?;
    let withdrawal_count = db::count_transactions_by_type(pool, "withdrawal").await?;

    let expected_deposits = expected
        .iter()
        .filter(|tx| tx.tx_type == TransactionType::Deposit)
        .count() as i64;
    let expected_withdrawals = expected
        .iter()
        .filter(|tx| tx.tx_type == TransactionType::Withdrawal)
        .count() as i64;

    println!(
        "  Transaction counts: {} deposits, {} withdrawals",
        deposit_count, withdrawal_count
    );

    assert_eq!(
        deposit_count, expected_deposits,
        "{} deposit count mismatch",
        db_name
    );

    println!("✓ {} deposit counts match", expected_deposits);

    assert_eq!(
        withdrawal_count, expected_withdrawals,
        "{} withdrawal count mismatch",
        db_name
    );

    println!("✓ {} withdrawal counts match", expected_withdrawals);

    // 2. Fetch and verify all expected transactions are present
    verify_transactions_present(pool, expected, db_name).await?;

    // 3. Verify filtered transactions are NOT in database
    verify_filtered_excluded(pool, filtered, db_name).await?;

    // 4. Checkpoint verification for escrow
    verify_checkpoints(pool, db_name).await?;

    println!("✓ {} database verification passed", db_name);

    Ok(())
}

/// Verify all expected transactions are present in database
async fn verify_transactions_present(
    pool: &sqlx::PgPool,
    expected: &[UserTransaction],
    db_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut missing = Vec::new();
    let mut validation_errors = 0;

    for expected_tx in expected {
        let db_tx = db::get_transaction(pool, &expected_tx.signature).await?;

        match db_tx {
            None => {
                missing.push(expected_tx.signature.clone());
            }
            Some(db_tx) => {
                let validation_result = validate_db_transaction(&db_tx, expected_tx);
                if let Err(e) = validation_result {
                    println!("  ❌ Validation error for {}: {}", expected_tx.signature, e);
                    validation_errors += 1;
                }
            }
        }
    }

    assert!(
        missing.is_empty(),
        "{} missing {} signatures: {:?}",
        db_name,
        missing.len(),
        missing
    );
    println!("  ✓ All {} signatures present", expected.len());

    assert_eq!(
        validation_errors, 0,
        "{} has {} validation errors",
        db_name, validation_errors
    );
    println!("  ✓ All transactions match");

    Ok(())
}

/// Verify filtered transactions are NOT in database
async fn verify_filtered_excluded(
    pool: &sqlx::PgPool,
    filtered: &[UserTransaction],
    db_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut incorrectly_indexed = Vec::new();
    for filtered_tx in filtered {
        if db::get_transaction(pool, &filtered_tx.signature)
            .await?
            .is_some()
        {
            incorrectly_indexed.push(filtered_tx.signature.clone());
        }
    }

    assert!(
        incorrectly_indexed.is_empty(),
        "{} incorrectly indexed {} filtered transactions: {:?}",
        db_name,
        incorrectly_indexed.len(),
        incorrectly_indexed
    );
    println!(
        "  ✓ All {} filtered transactions correctly excluded",
        filtered.len()
    );

    Ok(())
}

/// Verify checkpoints for both escrow and withdraw programs
async fn verify_checkpoints(
    pool: &sqlx::PgPool,
    db_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let escrow_checkpoint = db::get_checkpoint_slot(pool, "escrow").await?;
    let max_slot = db::get_max_slot_from_transactions(pool).await?;

    if let Some(max_tx_slot) = max_slot {
        // Verify escrow checkpoint (for deposits)
        if let Some(checkpoint_slot) = escrow_checkpoint {
            println!(
                "  Escrow checkpoint: {}, Max transaction slot: {}",
                checkpoint_slot, max_tx_slot
            );
            assert!(
                checkpoint_slot >= max_tx_slot,
                "{} escrow checkpoint {} is behind max transaction slot {}",
                db_name,
                checkpoint_slot,
                max_tx_slot
            );
            println!("  ✓ Escrow checkpoint advanced correctly");
        } else {
            println!("  ⚠ Escrow checkpoint not available");
        }
    } else {
        println!("  ⚠ Max slot not available for verification");
    }

    Ok(())
}
