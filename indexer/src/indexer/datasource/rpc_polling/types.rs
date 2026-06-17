use crate::indexer::datasource::common::types::CompiledInstruction;
use serde::Deserialize;

/// RPC block response types
#[derive(Debug, Deserialize, Clone)]
pub struct RpcBlock {
    pub blockhash: String,
    #[serde(rename = "parentSlot")]
    pub parent_slot: u64,
    pub transactions: Vec<RpcTransactionWithMeta>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RpcTransactionWithMeta {
    pub transaction: EncodedTransaction,
    pub meta: Option<TransactionMeta>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct EncodedTransaction {
    pub signatures: Vec<String>,
    pub message: EncodedMessage,
}

#[derive(Debug, Deserialize, Clone)]
pub struct EncodedMessage {
    #[serde(rename = "accountKeys")]
    pub account_keys: Vec<String>,
    pub instructions: Vec<CompiledInstruction>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TransactionMeta {
    pub err: Option<serde_json::Value>,
    #[serde(rename = "logMessages")]
    pub log_messages: Option<Vec<String>>,
    #[serde(rename = "innerInstructions")]
    pub inner_instructions: Option<Vec<InnerInstructions>>,
    /// ALT keys for a v0 transaction, appended after the static keys (writable then readonly) to rebuild the full account list.
    #[serde(rename = "loadedAddresses")]
    pub loaded_addresses: Option<solana_transaction_status::UiLoadedAddresses>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct InnerInstructions {
    pub index: u8,
    pub instructions: Vec<InnerInstruction>,
}

/// A single inner (CPI) instruction; `stack_height` is its CPI depth, used to match a deposit to the event it emitted.
#[derive(Debug, Deserialize, Clone)]
pub struct InnerInstruction {
    #[serde(flatten)]
    pub instruction: CompiledInstruction,
    #[serde(rename = "stackHeight")]
    pub stack_height: Option<u32>,
}
