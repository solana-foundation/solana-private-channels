use once_cell::sync::Lazy;
use solana_keychain::{Signer, SignerError, SolanaSigner};
use solana_sdk::pubkey::Pubkey;
use std::env;
use tracing::{info, warn};

/// Environment variables for admin signer
const ADMIN_SIGNER: &str = "ADMIN_SIGNER";

/// Environment variables for operator signer
const OPERATOR_SIGNER: &str = "OPERATOR_SIGNER";

// In memory env vars (per-signer)
const ADMIN_PRIVATE_KEY: &str = "ADMIN_PRIVATE_KEY";
const OPERATOR_PRIVATE_KEY: &str = "OPERATOR_PRIVATE_KEY";

// Vault env vars (per-signer)
const ADMIN_VAULT_ADDR: &str = "ADMIN_VAULT_ADDR";
const ADMIN_VAULT_TOKEN: &str = "ADMIN_VAULT_TOKEN";
const ADMIN_VAULT_KEY_NAME: &str = "ADMIN_VAULT_KEY_NAME";
const ADMIN_VAULT_PUBKEY: &str = "ADMIN_VAULT_PUBKEY";
const OPERATOR_VAULT_ADDR: &str = "OPERATOR_VAULT_ADDR";
const OPERATOR_VAULT_TOKEN: &str = "OPERATOR_VAULT_TOKEN";
const OPERATOR_VAULT_KEY_NAME: &str = "OPERATOR_VAULT_KEY_NAME";
const OPERATOR_VAULT_PUBKEY: &str = "OPERATOR_VAULT_PUBKEY";

// Turnkey env vars (per-signer)
const ADMIN_TURNKEY_API_PUBLIC_KEY: &str = "ADMIN_TURNKEY_API_PUBLIC_KEY";
const ADMIN_TURNKEY_API_PRIVATE_KEY: &str = "ADMIN_TURNKEY_API_PRIVATE_KEY";
const ADMIN_TURNKEY_ORGANIZATION_ID: &str = "ADMIN_TURNKEY_ORGANIZATION_ID";
const ADMIN_TURNKEY_PRIVATE_KEY_ID: &str = "ADMIN_TURNKEY_PRIVATE_KEY_ID";
const ADMIN_TURNKEY_PUBKEY: &str = "ADMIN_TURNKEY_PUBKEY";
const OPERATOR_TURNKEY_API_PUBLIC_KEY: &str = "OPERATOR_TURNKEY_API_PUBLIC_KEY";
const OPERATOR_TURNKEY_API_PRIVATE_KEY: &str = "OPERATOR_TURNKEY_API_PRIVATE_KEY";
const OPERATOR_TURNKEY_ORGANIZATION_ID: &str = "OPERATOR_TURNKEY_ORGANIZATION_ID";
const OPERATOR_TURNKEY_PRIVATE_KEY_ID: &str = "OPERATOR_TURNKEY_PRIVATE_KEY_ID";
const OPERATOR_TURNKEY_PUBKEY: &str = "OPERATOR_TURNKEY_PUBKEY";

// Privy env vars (per-signer)
const ADMIN_PRIVY_APP_ID: &str = "ADMIN_PRIVY_APP_ID";
const ADMIN_PRIVY_APP_SECRET: &str = "ADMIN_PRIVY_APP_SECRET";
const ADMIN_PRIVY_WALLET_ID: &str = "ADMIN_PRIVY_WALLET_ID";
const OPERATOR_PRIVY_APP_ID: &str = "OPERATOR_PRIVY_APP_ID";
const OPERATOR_PRIVY_APP_SECRET: &str = "OPERATOR_PRIVY_APP_SECRET";
const OPERATOR_PRIVY_WALLET_ID: &str = "OPERATOR_PRIVY_WALLET_ID";

#[derive(Debug, Clone, Copy)]
enum SignerType {
    Memory,
    Vault,
    Turnkey,
    Privy,
}

impl SignerType {
    fn from_str(s: &str) -> Result<Self, SignerError> {
        match s.to_lowercase().as_str() {
            "memory" => Ok(Self::Memory),
            "vault" => Ok(Self::Vault),
            "turnkey" => Ok(Self::Turnkey),
            "privy" => Ok(Self::Privy),
            other => Err(SignerError::InvalidPrivateKey(format!(
                "Unsupported signer type: {}. Supported: memory, vault, turnkey, privy",
                other
            ))),
        }
    }
}

/// Signer role for selecting env var prefixes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SignerRole {
    Admin,
    Operator,
}

/// Global admin signer (required for both programs)
static ADMIN_SIGNER_INSTANCE: Lazy<Signer> =
    Lazy::new(|| load_signer(SignerRole::Admin).expect("ADMIN_SIGNER must be configured"));

/// Global operator signer (optional, only for release funds)
static OPERATOR_SIGNER_INSTANCE: Lazy<Option<Signer>> =
    Lazy::new(|| match load_signer(SignerRole::Operator) {
        Ok(signer) => Some(signer),
        Err(_) => {
            warn!("OPERATOR_SIGNER not configured - release funds will use admin as operator");
            None
        }
    });

/// Load signer from environment variables
fn load_signer(role: SignerRole) -> Result<Signer, SignerError> {
    let (role_name, type_var) = match role {
        SignerRole::Admin => ("admin", ADMIN_SIGNER),
        SignerRole::Operator => ("operator", OPERATOR_SIGNER),
    };

    let signer_type_str = env::var(type_var)
        .map_err(|_| SignerError::InvalidPrivateKey(format!("{} not set", type_var)))?;
    let signer_type = SignerType::from_str(&signer_type_str)?;

    let signer = match signer_type {
        SignerType::Memory => {
            let private_key_var = match role {
                SignerRole::Admin => ADMIN_PRIVATE_KEY,
                SignerRole::Operator => OPERATOR_PRIVATE_KEY,
            };
            let private_key = env::var(private_key_var).map_err(|_| {
                SignerError::InvalidPrivateKey(format!("{} not set", private_key_var))
            })?;
            // Reject a set-but-empty value: env::var returns Ok("") for a blank var.
            if private_key.trim().is_empty() {
                return Err(SignerError::InvalidPrivateKey(format!(
                    "{} is set but empty",
                    private_key_var
                )));
            }

            Signer::from_memory(&private_key)?
        }
        SignerType::Vault => {
            let (vault_addr_var, vault_token_var, key_name_var, pubkey_var) = match role {
                SignerRole::Admin => (
                    ADMIN_VAULT_ADDR,
                    ADMIN_VAULT_TOKEN,
                    ADMIN_VAULT_KEY_NAME,
                    ADMIN_VAULT_PUBKEY,
                ),
                SignerRole::Operator => (
                    OPERATOR_VAULT_ADDR,
                    OPERATOR_VAULT_TOKEN,
                    OPERATOR_VAULT_KEY_NAME,
                    OPERATOR_VAULT_PUBKEY,
                ),
            };
            let vault_addr = env::var(vault_addr_var).map_err(|_| {
                SignerError::InvalidPrivateKey(format!("{} not set", vault_addr_var))
            })?;
            let vault_token = env::var(vault_token_var).map_err(|_| {
                SignerError::InvalidPrivateKey(format!("{} not set", vault_token_var))
            })?;

            let key_name = env::var(key_name_var)
                .map_err(|_| SignerError::InvalidPrivateKey(format!("{} not set", key_name_var)))?;
            let pubkey = env::var(pubkey_var)
                .map_err(|_| SignerError::InvalidPrivateKey(format!("{} not set", pubkey_var)))?;
            Signer::from_vault(vault_addr, vault_token, key_name, pubkey)?
        }
        SignerType::Turnkey => {
            let (
                api_public_key_var,
                api_private_key_var,
                organization_id_var,
                pubkey_var,
                private_key_id_var,
            ) = match role {
                SignerRole::Admin => (
                    ADMIN_TURNKEY_API_PUBLIC_KEY,
                    ADMIN_TURNKEY_API_PRIVATE_KEY,
                    ADMIN_TURNKEY_ORGANIZATION_ID,
                    ADMIN_TURNKEY_PUBKEY,
                    ADMIN_TURNKEY_PRIVATE_KEY_ID,
                ),
                SignerRole::Operator => (
                    OPERATOR_TURNKEY_API_PUBLIC_KEY,
                    OPERATOR_TURNKEY_API_PRIVATE_KEY,
                    OPERATOR_TURNKEY_ORGANIZATION_ID,
                    OPERATOR_TURNKEY_PUBKEY,
                    OPERATOR_TURNKEY_PRIVATE_KEY_ID,
                ),
            };
            let api_public_key = env::var(api_public_key_var).map_err(|_| {
                SignerError::InvalidPrivateKey(format!("{} not set", api_public_key_var))
            })?;
            let api_private_key = env::var(api_private_key_var).map_err(|_| {
                SignerError::InvalidPrivateKey(format!("{} not set", api_private_key_var))
            })?;
            let organization_id = env::var(organization_id_var).map_err(|_| {
                SignerError::InvalidPrivateKey(format!("{} not set", organization_id_var))
            })?;
            let public_key = env::var(pubkey_var)
                .map_err(|_| SignerError::InvalidPrivateKey(format!("{} not set", pubkey_var)))?;
            let private_key_id = env::var(private_key_id_var).map_err(|_| {
                SignerError::InvalidPrivateKey(format!("{} not set", private_key_id_var))
            })?;
            Signer::from_turnkey(
                api_public_key,
                api_private_key,
                organization_id,
                private_key_id,
                public_key,
            )?
        }
        SignerType::Privy => {
            let (app_id_var, app_secret_var, wallet_id_var) = match role {
                SignerRole::Admin => (
                    ADMIN_PRIVY_APP_ID,
                    ADMIN_PRIVY_APP_SECRET,
                    ADMIN_PRIVY_WALLET_ID,
                ),
                SignerRole::Operator => (
                    OPERATOR_PRIVY_APP_ID,
                    OPERATOR_PRIVY_APP_SECRET,
                    OPERATOR_PRIVY_WALLET_ID,
                ),
            };
            let app_id = env::var(app_id_var)
                .map_err(|_| SignerError::InvalidPrivateKey(format!("{} not set", app_id_var)))?;
            let app_secret = env::var(app_secret_var).map_err(|_| {
                SignerError::InvalidPrivateKey(format!("{} not set", app_secret_var))
            })?;
            let wallet_id = env::var(wallet_id_var).map_err(|_| {
                SignerError::InvalidPrivateKey(format!("{} not set", wallet_id_var))
            })?;

            // Block on async initialization
            tokio::runtime::Handle::current()
                .block_on(Signer::from_privy(app_id, app_secret, wallet_id))?
        }
    };

    info!(
        "Loaded {} signer ({}): {}",
        role_name,
        signer_type_str,
        signer.pubkey()
    );
    Ok(signer)
}

pub struct SignerUtil;

impl SignerUtil {
    pub fn get_admin_pubkey() -> Pubkey {
        Self::admin_signer().pubkey()
    }

    pub fn get_operator_pubkey() -> Pubkey {
        Self::operator_signer().pubkey()
    }

    pub fn admin_signer() -> &'static Signer {
        &ADMIN_SIGNER_INSTANCE
    }

    pub fn operator_signer() -> &'static Signer {
        OPERATOR_SIGNER_INSTANCE
            .as_ref()
            .unwrap_or(&ADMIN_SIGNER_INSTANCE)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // serial_test ensures env-var-mutating tests run sequentially; cargo test
    // runs tests in parallel by default, which causes races on shared process
    // environment variables (set_var / remove_var).
    use serial_test::serial;

    /// Only "memory", "vault", "turnkey", and "privy" are valid signer types; any other
    /// string — including an empty one — must return an InvalidPrivateKey error.
    #[test]
    fn signer_type_from_str_unknown_errors() {
        let err = SignerType::from_str("unknown").unwrap_err();
        assert!(
            err.to_string().contains("Unsupported signer type"),
            "unexpected error: {err}"
        );
        assert!(SignerType::from_str("").is_err());
    }

    /// When ADMIN_SIGNER is absent, load_signer must fail immediately with a message
    /// naming the missing variable so the operator can identify the misconfiguration.
    #[test]
    #[serial]
    fn load_signer_admin_no_env_var_errors() {
        let original = env::var(ADMIN_SIGNER).ok();
        env::remove_var(ADMIN_SIGNER);

        let err = load_signer(SignerRole::Admin)
            .err()
            .expect("expected error");
        let msg = err.to_string();
        assert!(
            msg.contains("ADMIN_SIGNER") || msg.contains("not set"),
            "error should name the missing var, got: {msg}"
        );

        if let Some(val) = original {
            env::set_var(ADMIN_SIGNER, val);
        }
    }

    /// ADMIN_SIGNER=memory requires ADMIN_PRIVATE_KEY to be set; without it load_signer
    /// must fail and name the missing variable in the error message.
    #[test]
    #[serial]
    fn load_signer_memory_missing_private_key_errors() {
        let orig_type = env::var(ADMIN_SIGNER).ok();
        let orig_key = env::var(ADMIN_PRIVATE_KEY).ok();
        env::set_var(ADMIN_SIGNER, "memory");
        env::remove_var(ADMIN_PRIVATE_KEY);

        let err = load_signer(SignerRole::Admin)
            .err()
            .expect("expected error");
        let msg = err.to_string();
        assert!(
            msg.contains("ADMIN_PRIVATE_KEY") || msg.contains("not set"),
            "error should name the missing var, got: {msg}"
        );

        env::remove_var(ADMIN_SIGNER);
        if let Some(val) = orig_type {
            env::set_var(ADMIN_SIGNER, val);
        }
        if let Some(val) = orig_key {
            env::set_var(ADMIN_PRIVATE_KEY, val);
        }
    }

    /// ADMIN_SIGNER=memory with a set-but-empty ADMIN_PRIVATE_KEY (the blanked-secret case)
    /// must fail closed; env::var returns Ok("") so only an explicit emptiness check catches it.
    #[test]
    #[serial]
    fn load_signer_memory_empty_private_key_errors() {
        let orig_type = env::var(ADMIN_SIGNER).ok();
        let orig_key = env::var(ADMIN_PRIVATE_KEY).ok();
        env::set_var(ADMIN_SIGNER, "memory");

        for blank in ["", "   ", "\t\n"] {
            env::set_var(ADMIN_PRIVATE_KEY, blank);
            let err = load_signer(SignerRole::Admin)
                .err()
                .expect("set-but-empty private key must be rejected");
            let msg = err.to_string();
            assert!(
                msg.contains("ADMIN_PRIVATE_KEY") && msg.contains("is set but empty"),
                "error should flag the empty var, got: {msg}"
            );
        }

        env::remove_var(ADMIN_SIGNER);
        env::remove_var(ADMIN_PRIVATE_KEY);
        if let Some(val) = orig_type {
            env::set_var(ADMIN_SIGNER, val);
        }
        if let Some(val) = orig_key {
            env::set_var(ADMIN_PRIVATE_KEY, val);
        }
    }

    /// ADMIN_SIGNER=vault requires ADMIN_VAULT_ADDR as the first credential; the error
    /// message must identify the missing variable so misconfiguration is immediately obvious.
    #[test]
    #[serial]
    fn load_signer_vault_missing_vault_addr_errors() {
        let orig_type = env::var(ADMIN_SIGNER).ok();
        let orig_addr = env::var(ADMIN_VAULT_ADDR).ok();
        env::set_var(ADMIN_SIGNER, "vault");
        env::remove_var(ADMIN_VAULT_ADDR);

        let result = load_signer(SignerRole::Admin);
        assert!(result.is_err());
        let msg = format!("{}", result.err().expect("expected error"));
        assert!(
            msg.contains("ADMIN_VAULT_ADDR") || msg.contains("not set"),
            "Unexpected error: {}",
            msg
        );

        // Restore
        env::remove_var(ADMIN_SIGNER);
        if let Some(val) = orig_type {
            env::set_var(ADMIN_SIGNER, val);
        }
        if let Some(val) = orig_addr {
            env::set_var(ADMIN_VAULT_ADDR, val);
        }
    }

    /// ADMIN_SIGNER=turnkey requires ADMIN_TURNKEY_API_PUBLIC_KEY as the first credential;
    /// the error must name the exact missing variable rather than giving a generic message.
    #[test]
    #[serial]
    fn load_signer_turnkey_missing_api_public_key_errors() {
        let orig_type = env::var(ADMIN_SIGNER).ok();
        let orig_key = env::var(ADMIN_TURNKEY_API_PUBLIC_KEY).ok();
        env::set_var(ADMIN_SIGNER, "turnkey");
        env::remove_var(ADMIN_TURNKEY_API_PUBLIC_KEY);

        let result = load_signer(SignerRole::Admin);
        assert!(result.is_err());
        let msg = format!("{}", result.err().expect("expected error"));
        assert!(
            msg.contains("ADMIN_TURNKEY_API_PUBLIC_KEY") || msg.contains("not set"),
            "Unexpected error: {}",
            msg
        );

        // Restore
        env::remove_var(ADMIN_SIGNER);
        if let Some(val) = orig_type {
            env::set_var(ADMIN_SIGNER, val);
        }
        if let Some(val) = orig_key {
            env::set_var(ADMIN_TURNKEY_API_PUBLIC_KEY, val);
        }
    }

    /// ADMIN_SIGNER=privy requires ADMIN_PRIVY_APP_ID as the first credential; the error
    /// must name the missing variable so the operator knows which env var to supply.
    #[test]
    #[serial]
    fn load_signer_privy_missing_app_id_errors() {
        let orig_type = env::var(ADMIN_SIGNER).ok();
        let orig_app_id = env::var(ADMIN_PRIVY_APP_ID).ok();
        env::set_var(ADMIN_SIGNER, "privy");
        env::remove_var(ADMIN_PRIVY_APP_ID);

        let result = load_signer(SignerRole::Admin);
        assert!(result.is_err());
        let msg = format!("{}", result.err().expect("expected error"));
        assert!(
            msg.contains("ADMIN_PRIVY_APP_ID") || msg.contains("not set"),
            "Unexpected error: {}",
            msg
        );

        // Restore
        env::remove_var(ADMIN_SIGNER);
        if let Some(val) = orig_type {
            env::set_var(ADMIN_SIGNER, val);
        }
        if let Some(val) = orig_app_id {
            env::set_var(ADMIN_PRIVY_APP_ID, val);
        }
    }

    /// When OPERATOR_SIGNER is absent, load_signer returns an error so the caller
    /// (the global Lazy) can fall back to the admin signer and log a warning.
    #[test]
    #[serial]
    fn load_signer_operator_no_env_var_errors() {
        let orig = env::var(OPERATOR_SIGNER).ok();
        env::remove_var(OPERATOR_SIGNER);

        let err = load_signer(SignerRole::Operator)
            .err()
            .expect("expected error");
        let msg = err.to_string();
        assert!(
            msg.contains("OPERATOR_SIGNER") || msg.contains("not set"),
            "error should name the missing var, got: {msg}"
        );

        if let Some(val) = orig {
            env::set_var(OPERATOR_SIGNER, val);
        }
    }

    /// OPERATOR_SIGNER=memory requires OPERATOR_PRIVATE_KEY; without it load_signer must
    /// fail and name the missing variable so the caller can report a clear startup error.
    #[test]
    #[serial]
    fn load_signer_operator_memory_missing_key_errors() {
        let orig_type = env::var(OPERATOR_SIGNER).ok();
        let orig_key = env::var(OPERATOR_PRIVATE_KEY).ok();
        env::set_var(OPERATOR_SIGNER, "memory");
        env::remove_var(OPERATOR_PRIVATE_KEY);

        let err = load_signer(SignerRole::Operator)
            .err()
            .expect("expected error");
        let msg = err.to_string();
        assert!(
            msg.contains("OPERATOR_PRIVATE_KEY") || msg.contains("not set"),
            "error should name the missing var, got: {msg}"
        );

        env::remove_var(OPERATOR_SIGNER);
        if let Some(val) = orig_type {
            env::set_var(OPERATOR_SIGNER, val);
        }
        if let Some(val) = orig_key {
            env::set_var(OPERATOR_PRIVATE_KEY, val);
        }
    }
}
