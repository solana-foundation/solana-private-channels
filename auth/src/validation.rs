//! Input caps shared by the credential routes.
//!
//! Both routes enforce the maximums before any Argon2 work. Only `register`
//! enforces the minimums; applying them at login would lock out accounts
//! created under an older policy.

pub const USERNAME_MIN_LEN: usize = 5;
pub const USERNAME_MAX_LEN: usize = 32;

pub const PASSWORD_MIN_LEN: usize = 6;
/// Capped in characters, not bytes. Login must measure the same way or a
/// multibyte password that registered fine would be rejected at login.
pub const PASSWORD_MAX_LEN: usize = 128;
