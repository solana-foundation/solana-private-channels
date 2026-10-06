use {
    super::test_context::PrivateChannelContext,
    solana_sdk::{signature::Keypair, signer::Signer},
    solana_transaction_status::UiTransactionEncoding,
};

pub async fn run_get_transaction_test(ctx: &PrivateChannelContext) {
    println!("\n=== Get Transaction Test ===");

    // First, send a transaction so we have something to query
    let from_keypair = Keypair::new();

    let blockhash = ctx.get_blockhash().await.unwrap();
    let memo_ix = solana_sdk::instruction::Instruction {
        program_id: spl_memo::id(),
        accounts: vec![],
        data: b"get-transaction".to_vec(),
    };

    let transaction = solana_sdk::transaction::Transaction::new_signed_with_payer(
        &[memo_ix],
        Some(&from_keypair.pubkey()),
        &[&from_keypair],
        blockhash,
    );

    let sig = ctx.send_transaction(&transaction).await.unwrap();
    println!("Sent test transaction: {}", sig);

    // Wait for confirmation
    ctx.check_transaction_exists(sig).await;
    println!("Transaction confirmed: {}", sig);

    // Test 1: Get transaction with default encoding (Json)
    println!("\n  Test 1: getTransaction with Json encoding");
    test_get_transaction_json(ctx, &sig).await;

    // Test 2: Get transaction with base64 encoding
    println!("\n  Test 2: getTransaction with base64 encoding");
    test_get_transaction_base64(ctx, &sig).await;

    // Test 3: Get transaction with base58 encoding
    println!("\n  Test 3: getTransaction with base58 encoding");
    test_get_transaction_base58(ctx, &sig).await;

    // Test 4: Get transaction with jsonParsed encoding
    println!("\n  Test 4: getTransaction with jsonParsed encoding");
    test_get_transaction_json_parsed(ctx, &sig).await;

    // Test 5: Verify get_transaction returns same data as get_block
    println!("\n  Test 5: Verify get_transaction matches get_block");
    test_get_transaction_matches_get_block(ctx, &sig).await;

    println!("\n✓ Get transaction test passed!");
}

async fn test_get_transaction_json(
    ctx: &PrivateChannelContext,
    signature: &solana_sdk::signature::Signature,
) {
    let result = ctx
        .get_transaction_with_encoding(signature, UiTransactionEncoding::Json)
        .await
        .expect("Failed to get transaction")
        .expect("Transaction should exist");

    // Verify the transaction response has the expected fields
    assert!(result.get("slot").is_some(), "Missing slot field");
    assert!(
        result.get("transaction").is_some(),
        "Missing transaction field"
    );

    // For JSON encoding, transaction should be an object with message and signatures
    let transaction = result
        .get("transaction")
        .expect("Missing transaction field");
    assert!(
        transaction.is_object(),
        "Transaction should be an object for JSON encoding"
    );
    assert!(
        transaction.get("message").is_some(),
        "Transaction should have message field"
    );
    assert!(
        transaction.get("signatures").is_some(),
        "Transaction should have signatures field"
    );

    println!("    ✓ Json encoding works correctly");
}

async fn test_get_transaction_base64(
    ctx: &PrivateChannelContext,
    signature: &solana_sdk::signature::Signature,
) {
    let result = ctx
        .get_transaction_with_encoding(signature, UiTransactionEncoding::Base64)
        .await
        .expect("Failed to get transaction")
        .expect("Transaction should exist");

    assert!(result.get("slot").is_some(), "Missing slot field");

    // For base64 encoding, transaction should be an array [<base64-string>, "base64"]
    let transaction = result
        .get("transaction")
        .expect("Missing transaction field");
    assert!(
        transaction.is_array(),
        "Transaction should be an array for base64 encoding"
    );

    let tx_array = transaction.as_array().unwrap();
    assert_eq!(
        tx_array.len(),
        2,
        "Transaction array should have 2 elements"
    );
    assert!(
        tx_array[0].is_string(),
        "First element should be base64 string"
    );
    assert_eq!(
        tx_array[1].as_str().unwrap(),
        "base64",
        "Second element should be 'base64'"
    );

    println!("    ✓ base64 encoding works correctly");
}

async fn test_get_transaction_base58(
    ctx: &PrivateChannelContext,
    signature: &solana_sdk::signature::Signature,
) {
    let result = ctx
        .get_transaction_with_encoding(signature, UiTransactionEncoding::Base58)
        .await
        .expect("Failed to get transaction")
        .expect("Transaction should exist");

    assert!(result.get("slot").is_some(), "Missing slot field");

    // For base58 encoding, transaction should be an array [<base58-string>, "base58"]
    let transaction = result
        .get("transaction")
        .expect("Missing transaction field");
    assert!(
        transaction.is_array(),
        "Transaction should be an array for base58 encoding"
    );

    let tx_array = transaction.as_array().unwrap();
    assert_eq!(
        tx_array.len(),
        2,
        "Transaction array should have 2 elements"
    );
    assert!(
        tx_array[0].is_string(),
        "First element should be base58 string"
    );
    assert_eq!(
        tx_array[1].as_str().unwrap(),
        "base58",
        "Second element should be 'base58'"
    );

    println!("    ✓ base58 encoding works correctly");
}

async fn test_get_transaction_json_parsed(
    ctx: &PrivateChannelContext,
    signature: &solana_sdk::signature::Signature,
) {
    let result = ctx
        .get_transaction_with_encoding(signature, UiTransactionEncoding::JsonParsed)
        .await
        .expect("Failed to get transaction")
        .expect("Transaction should exist");

    assert!(result.get("slot").is_some(), "Missing slot field");

    // For jsonParsed encoding, transaction should be an object with message containing accountKeys with metadata
    let transaction = result
        .get("transaction")
        .expect("Missing transaction field");
    assert!(
        transaction.is_object(),
        "Transaction should be an object for jsonParsed encoding"
    );

    let message = transaction.get("message").expect("Missing message field");
    let account_keys = message
        .get("accountKeys")
        .expect("Missing accountKeys field");
    assert!(account_keys.is_array(), "accountKeys should be an array");

    // Check that account keys have the parsed format with pubkey, signer, source, writable
    if let Some(first_key) = account_keys.as_array().and_then(|arr| arr.first()) {
        assert!(
            first_key.get("pubkey").is_some(),
            "accountKey should have pubkey field"
        );
        assert!(
            first_key.get("signer").is_some(),
            "accountKey should have signer field"
        );
        assert!(
            first_key.get("source").is_some(),
            "accountKey should have source field"
        );
        assert!(
            first_key.get("writable").is_some(),
            "accountKey should have writable field"
        );
    }

    println!("    ✓ jsonParsed encoding works correctly");
}

async fn test_get_transaction_matches_get_block(
    ctx: &PrivateChannelContext,
    signature: &solana_sdk::signature::Signature,
) {
    // Get the transaction using getTransaction
    let tx_response = ctx
        .get_transaction(signature)
        .await
        .expect("Failed to get transaction")
        .expect("Transaction should exist");

    // Get the slot from the transaction
    let slot = tx_response
        .get("slot")
        .and_then(|s| s.as_u64())
        .expect("Transaction should have slot field");

    // Get the block containing this transaction
    let block = ctx
        .read_client
        .get_block(slot)
        .await
        .expect("Failed to get block");

    println!("    Block has {} transactions", block.transactions.len());

    // Our getBlock returns raw JSON, so let's compare JSON directly
    let block_json = serde_json::to_value(&block).expect("Failed to serialize block");
    let block_txs = block_json
        .get("transactions")
        .and_then(|t| t.as_array())
        .expect("Block should have transactions array");

    // Find our transaction by comparing the signature in the JSON
    let block_tx_json = block_txs
        .iter()
        .find(|tx| {
            let tx_sigs = tx
                .get("transaction")
                .and_then(|t| t.get("signatures"))
                .and_then(|s| s.as_array());
            if let Some(sigs) = tx_sigs {
                if let Some(first_sig) = sigs.first().and_then(|s| s.as_str()) {
                    return first_sig == signature.to_string();
                }
            }
            false
        })
        .expect("Transaction should be in block");

    // Compare the transaction data
    let tx_from_get_transaction = tx_response
        .get("transaction")
        .expect("getTransaction response should have transaction field");

    let tx_from_get_block = block_tx_json
        .get("transaction")
        .expect("getBlock response should have transaction field");

    println!("    Comparing transaction data...");

    // Compare the entire transaction structure
    assert_eq!(
        tx_from_get_transaction, tx_from_get_block,
        "Transaction data should match between getTransaction and getBlock"
    );

    println!("    ✓ getTransaction and getBlock return matching transaction data");
}
