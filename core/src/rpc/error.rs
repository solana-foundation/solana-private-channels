use jsonrpsee::types::ErrorObjectOwned;

pub use jsonrpsee::types::error::{
    INTERNAL_ERROR_CODE, INVALID_PARAMS_CODE, INVALID_REQUEST_CODE, PARSE_ERROR_CODE,
};

/// Generic JSON-RPC server error (base of the -32000..-32099 reserved range).
pub const JSON_RPC_SERVER_ERROR: i32 = -32000;

/// Solana's SlotSkipped: the slot produced no block. Numbered to match Agave so
/// tooling written against its contract reads a skipped slot correctly instead
/// of retrying a null forever.
pub const SLOT_SKIPPED_CODE: i32 = -32007;

/// Solana's BlockNotAvailable: the slot has not been produced yet, which is a
/// different answer from a slot the chain has passed without producing one.
pub const BLOCK_NOT_AVAILABLE_CODE: i32 = -32004;

/// Retryable: the write pipeline ingress queue is full; the tx was not accepted.
pub const NODE_AT_CAPACITY_CODE: i32 = -32003;

/// Solana's UnsupportedTransactionVersion: the caller asked for a transaction
/// whose version is above the ceiling it passed, or passed no ceiling at all.
/// Numbered to match Agave so a client library recognises it and retries with a
/// higher `maxSupportedTransactionVersion` instead of treating it as fatal.
pub const UNSUPPORTED_TRANSACTION_VERSION_CODE: i32 = -32015;

pub fn custom_error(code: i32, message: impl ToString) -> ErrorObjectOwned {
    ErrorObjectOwned::owned(code, message.to_string(), None::<()>)
}

pub fn read_not_enabled() -> ErrorObjectOwned {
    custom_error(-32002, "Read operations not enabled")
}

pub fn write_not_enabled() -> ErrorObjectOwned {
    custom_error(-32001, "Write operations not enabled")
}

pub fn node_at_capacity() -> ErrorObjectOwned {
    custom_error(NODE_AT_CAPACITY_CODE, "Node at capacity, retry shortly")
}

pub fn slot_skipped(slot: u64) -> ErrorObjectOwned {
    custom_error(
        SLOT_SKIPPED_CODE,
        format!("Slot {slot} was skipped, or missing due to ledger jump to recent snapshot"),
    )
}

pub fn block_not_available(slot: u64) -> ErrorObjectOwned {
    custom_error(
        BLOCK_NOT_AVAILABLE_CODE,
        format!("Block not available for slot {slot}"),
    )
}

/// Refuse to encode a transaction the caller's version ceiling excludes.
///
/// Encoding is the only place this can be detected, because the ceiling is a
/// per-request parameter while the stored row's version is fixed. Returning the
/// error rather than unwrapping keeps one oversized row from killing the
/// connection and taking every other in-flight request with it.
pub fn unsupported_transaction_version(version: impl std::fmt::Display) -> ErrorObjectOwned {
    custom_error(
        UNSUPPORTED_TRANSACTION_VERSION_CODE,
        format!(
            "Transaction version ({version}) is not supported by the requesting client. \
             Please try the request again with the following configuration parameter: \
             \"maxSupportedTransactionVersion\": {version}"
        ),
    )
}
