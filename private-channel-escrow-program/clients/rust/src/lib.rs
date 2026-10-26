// Suppress warnings for generated code
#![allow(warnings)]

// Re-export generated code
pub mod generated;
pub use generated::*;

// Re-export commonly used items
pub use generated::accounts::*;
pub use generated::errors::*;
pub use generated::programs::*;

/// Length of a 0.1.0 `Instance`, which ends at the admin.
pub const INSTANCE_V1_LEN: usize = 67;
/// Length of the current `Instance`.
pub const INSTANCE_LEN: usize = 133;

/// Decodes an `Instance` in either layout. A 0.1.0 instance reads with the whitelist off,
/// as the program reads it; `Instance::from_bytes` refuses it.
pub fn decode_instance(data: &[u8]) -> std::io::Result<Instance> {
    if data.len() == INSTANCE_V1_LEN {
        let mut current = data.to_vec();
        current.resize(INSTANCE_LEN, 0);
        return Instance::from_bytes(&current);
    }
    Instance::from_bytes(data)
}
