use crate::rpc::{
    constants::PACKET_DATA_SIZE,
    error::{custom_error, INVALID_PARAMS_CODE, JSON_RPC_SERVER_ERROR},
    WriteDeps,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use bincode::Options;
use jsonrpsee::core::RpcResult;
use solana_rpc_client_types::config::RpcSendTransactionConfig;
use solana_runtime_transaction::runtime_transaction::RuntimeTransaction;
use solana_sdk::{
    message::{v0::LoadedAddresses, SimpleAddressLoader},
    transaction::{MessageHash, VersionedTransaction},
};
use std::collections::HashSet;
use tracing::{debug, info, warn};

pub async fn send_transaction_impl(
    write_deps: &WriteDeps,
    transaction: String,
    _config: Option<RpcSendTransactionConfig>,
) -> RpcResult<String> {
    // Decode the base64 transaction
    let tx_data = STANDARD.decode(&transaction).map_err(|e| {
        custom_error(
            INVALID_PARAMS_CODE,
            format!("Invalid base64 encoding: {}", e),
        )
    })?;

    if tx_data.len() > PACKET_DATA_SIZE {
        return Err(custom_error(
            INVALID_PARAMS_CODE,
            format!(
                "Transaction too large: {} bytes (max: {} bytes)",
                tx_data.len(),
                PACKET_DATA_SIZE
            ),
        ));
    }

    // Use bincode options matching Agave's decode_and_deserialize
    let bincode_options = bincode::options()
        .with_limit(PACKET_DATA_SIZE as u64)
        .with_fixint_encoding()
        .allow_trailing_bytes();

    // Try to deserialize as VersionedTransaction first (standard format)
    let versioned_tx = bincode_options
        .deserialize::<VersionedTransaction>(&tx_data)
        .map_err(|e| {
            custom_error(
                INVALID_PARAMS_CODE,
                format!("Failed to deserialize transaction: {}", e),
            )
        })?;

    let runtime_tx = RuntimeTransaction::try_create(
        versioned_tx,
        MessageHash::Compute,
        None,
        SimpleAddressLoader::Enabled(LoadedAddresses {
            writable: vec![],
            readonly: vec![],
        }),
        &HashSet::new(),
    )
    .map_err(|err| custom_error(INVALID_PARAMS_CODE, format!("invalid transaction: {err}")))?;
    let sanitized_tx = runtime_tx.into_inner_transaction();

    // Filter: only accept SPL token, ATA, System Program, and Withdraw Program transactions
    let is_allowed_transaction =
        sanitized_tx
            .message()
            .program_instructions_iter()
            .all(|(program_id, _)| {
                *program_id == spl_token::id()
                || *program_id == spl_associated_token_account::id()
                || *program_id == spl_memo::id()
                || *program_id == solana_sdk::system_program::id()
                || *program_id
                    == private_channel_withdraw_program_client::PRIVATE_CHANNEL_WITHDRAW_PROGRAM_ID
            });

    if !is_allowed_transaction {
        // Log which programs were found in the transaction
        let program_ids: Vec<String> = sanitized_tx
            .message()
            .program_instructions_iter()
            .map(|(program_id, _)| program_id.to_string())
            .collect();
        warn!(
            "Rejected transaction {}: programs used: {:?}",
            sanitized_tx.signature(),
            program_ids
        );
        return Err(custom_error(
            INVALID_PARAMS_CODE,
            "Only SPL token, ATA, Memo, System, and Withdraw program transactions are accepted",
        ));
    }

    // Get the signature before sending to channel
    let signature = sanitized_tx.signature().to_string();

    // Send to dedup channel (which forwards to sigverify after deduplication)
    info!("Sending transaction {} to dedup stage", signature);
    write_deps.dedup_tx.send(sanitized_tx).map_err(|_| {
        custom_error(
            JSON_RPC_SERVER_ERROR,
            "Internal error: dedup channel closed",
        )
    })?;

    debug!("Transaction {} sent to dedup stage", signature);
    Ok(signature)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::WriteDeps;
    use solana_sdk::{
        hash::Hash,
        instruction::Instruction,
        pubkey::Pubkey,
        signature::{Keypair, Signer},
        transaction::{SanitizedTransaction, Transaction},
    };
    use tokio::sync::mpsc;

    fn encode_tx(tx: &Transaction) -> String {
        let bytes = bincode::serialize(tx).unwrap();
        STANDARD.encode(&bytes)
    }

    /// Returns WriteDeps and the receiver (must be held alive for happy-path tests).
    fn make_write_deps() -> (WriteDeps, mpsc::UnboundedReceiver<SanitizedTransaction>) {
        let (dedup_tx, rx) = mpsc::unbounded_channel();
        (WriteDeps { dedup_tx }, rx)
    }

    #[tokio::test]
    async fn disallowed_program_rejected() {
        let payer = Keypair::new();
        let fake_program = Pubkey::new_unique();
        let ix = Instruction {
            program_id: fake_program,
            accounts: vec![],
            data: vec![1],
        };
        let tx = Transaction::new_signed_with_payer(
            &[ix],
            Some(&payer.pubkey()),
            &[&payer],
            Hash::default(),
        );
        let encoded = encode_tx(&tx);
        let (deps, _rx) = make_write_deps();

        let result = send_transaction_impl(&deps, encoded, None).await;
        assert!(result.is_err(), "disallowed program should be rejected");
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Only SPL token"),
            "expected allowlist error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn allowed_programs_accepted() {
        let payer = Keypair::new();
        let from_ata = Pubkey::new_unique();
        let to_ata = Pubkey::new_unique();
        let ix = spl_token::instruction::transfer(
            &spl_token::id(),
            &from_ata,
            &to_ata,
            &payer.pubkey(),
            &[],
            1_000,
        )
        .unwrap();
        let tx = Transaction::new_signed_with_payer(
            &[ix],
            Some(&payer.pubkey()),
            &[&payer],
            Hash::default(),
        );
        let encoded = encode_tx(&tx);
        // Keep _rx alive so the dedup channel send succeeds
        let (deps, _rx) = make_write_deps();

        let result = send_transaction_impl(&deps, encoded, None).await;
        assert!(result.is_ok(), "SPL token tx should pass allowlist");
    }

    #[tokio::test]
    async fn memo_program_accepted() {
        let payer = Keypair::new();
        let memo_ix = Instruction {
            program_id: spl_memo::id(),
            accounts: vec![],
            data: b"private_channel:mint-idempotency:42".to_vec(),
        };
        let tx = Transaction::new_signed_with_payer(
            &[memo_ix],
            Some(&payer.pubkey()),
            &[&payer],
            Hash::default(),
        );
        let encoded = encode_tx(&tx);
        let (deps, _rx) = make_write_deps();

        let result = send_transaction_impl(&deps, encoded, None).await;
        assert!(result.is_ok(), "Memo tx should pass allowlist");
    }
}
