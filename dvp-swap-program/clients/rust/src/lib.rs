// Suppress warnings for generated code
#![allow(warnings)]

// Re-export generated code
pub mod generated;
pub use generated::*;

// Handwritten checked, verify-before-fund helpers (survive client
// regeneration; the generated readers do not check owner or exact size).
pub mod verify;

#[cfg(test)]
mod cpi_flag_regression;

// Re-export commonly used items
pub use generated::errors::*;
pub use generated::programs::*;
