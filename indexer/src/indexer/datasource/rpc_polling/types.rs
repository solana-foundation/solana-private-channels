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

/// A `getBlock` answer in the signatures view, which lists signatures instead of `transactions`.
#[derive(Debug, Deserialize, Clone)]
pub struct SignaturesBlock {
    pub blockhash: String,
    pub signatures: Vec<String>,
}

/// Outcome of fetching one slot's block. The domain has three states:
/// a proven-empty slot is safe to checkpoint past, but a slot the endpoint
/// cannot serve has unknown contents and must never
/// be checkpointed past. Transport and protocol failures stay on the outer
/// `Result::Err` so existing error handling is untouched.
#[derive(Debug, Clone)]
pub enum BlockFetch {
    Present(RpcBlock),
    /// A later block's `parentSlot` names the previous proven slot, which proves
    /// nothing was produced here: safe to advance.
    Skipped,
    /// A block exists at this slot and this endpoint will not serve it, so its
    /// contents are unknown: must not advance.
    Unavailable,
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
    /// ALT lookups of a v0 message; absent for legacy and v1, which load no addresses.
    #[serde(rename = "addressTableLookups", default)]
    pub address_table_lookups: Option<Vec<solana_transaction_status::UiAddressTableLookup>>,
}

/// A meta key the node must always send, which may still be null. Plain `Option` reads
/// a missing key and an explicit null both as `None`, so a response that omits `err`
/// would pass for a success.
#[derive(Debug, Clone, Default)]
pub enum Reported<T> {
    #[default]
    Missing,
    Present(Option<T>),
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for Reported<T> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Option::<T>::deserialize(deserializer).map(Self::Present)
    }
}

impl<T> Reported<T> {
    /// The value when the key was sent non-null; a missing key and a null both give `None`.
    pub fn present(&self) -> Option<&T> {
        match self {
            Self::Present(Some(value)) => Some(value),
            Self::Present(None) | Self::Missing => None,
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct TransactionMeta {
    #[serde(default)]
    pub err: Reported<serde_json::Value>,
    #[serde(rename = "logMessages")]
    pub log_messages: Option<Vec<String>>,
    #[serde(rename = "innerInstructions", default)]
    pub inner_instructions: Reported<Vec<InnerInstructions>>,
    /// ALT keys for a v0 transaction, appended after the static keys (writable then readonly) to rebuild the full account list.
    /// Never null from a node, so missing and null are both incomplete.
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
