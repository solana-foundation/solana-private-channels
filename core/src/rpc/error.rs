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
