/// Errors from data source RPC operations (indexer)
/// Used for raw HTTP/gRPC operations in RPC polling and Yellowstone streaming
#[derive(Debug, thiserror::Error)]
pub enum DataSourceRpcError {
    #[error("RPC request failed after {attempts} attempts: {last_error}")]
    MaxRetriesExceeded { attempts: u32, last_error: String },

    #[error("HTTP request failed: {0}")]
    HttpRequest(#[from] reqwest::Error),

    #[error("JSON parsing failed: {0}")]
    JsonParse(#[from] serde_json::Error),

    #[error("RPC protocol error: {reason}")]
    Protocol { reason: String },

    /// The classifier could not determine a slot's contents. Distinct from a
    /// transport failure: it wedges the checkpoint until an operator acts, so it
    /// carries its own metric label and its own alert.
    #[error("slot contents could not be proven: {reason}")]
    Unproven { reason: String },
}
