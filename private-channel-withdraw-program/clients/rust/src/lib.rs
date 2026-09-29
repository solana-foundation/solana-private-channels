// Suppress warnings for generated code
#![allow(warnings)]

// Re-export generated code
pub mod generated;
pub use generated::*;

// Re-export commonly used items
pub use generated::errors::*;
pub use generated::programs::*;

/// Seeds of the per-mint `WithdrawConfig` PDA: `[WITHDRAW_CONFIG_SEED, mint]`.
pub const WITHDRAW_CONFIG_SEED: &[u8] = b"withdraw_config";
