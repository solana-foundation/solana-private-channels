use once_cell::sync::OnceCell;
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
    fn from_str(s: &str) -> Result<Self, LoadError> {
        match s.to_lowercase().as_str() {
            "memory" => Ok(Self::Memory),
            "vault" => Ok(Self::Vault),
            "turnkey" => Ok(Self::Turnkey),
            "privy" => Ok(Self::Privy),
            other => Err(LoadError::Config(format!(
                "Unsupported signer type: {}. Supported: memory, vault, turnkey, privy",
                other
            ))),
        }
    }
}

/// Why a signer failed to load. `Config` names only env vars, never their values, so it
/// is safe to print; the keychain redacts its own errors, which would hide the var name.
#[derive(Debug)]
enum LoadError {
    Config(String),
    Keychain(SignerError),
}

impl std::fmt::Display for LoadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Config(reason) => f.write_str(reason),
            Self::Keychain(e) => write!(f, "{e}"),
        }
    }
}

impl From<SignerError> for LoadError {
    fn from(e: SignerError) -> Self {
        Self::Keychain(e)
    }
}

/// Signer role for selecting env var prefixes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SignerRole {
    Admin,
    Operator,
}

/// What `load_signer` read: a built signer, or Privy settings still to be awaited,
/// since the Privy constructor is async.
enum Loaded {
    Built(Signer),
    Privy {
        app_id: String,
        app_secret: String,
        wallet_id: String,
    },
}

static ADMIN: OnceCell<Signer> = OnceCell::new();

/// `None` means release and rotation sign with admin.
static OPERATOR: OnceCell<Option<Signer>> = OnceCell::new();

/// Loads the admin signer once, awaiting Privy on the caller's runtime. Resync needs only
/// this; later calls do nothing.
pub async fn init_admin_signer() -> Result<(), String> {
    if ADMIN.get().is_none() {
        let _ = ADMIN.set(load_admin().await?);
    }
    Ok(())
}

/// Loads both signers once, awaiting Privy on the caller's runtime. Operator startup calls
/// this before it connects to anything; later calls do nothing.
pub async fn init_signers() -> Result<(), String> {
    init_admin_signer().await?;
    if OPERATOR.get().is_none() {
        let _ = OPERATOR.set(load_operator().await?);
    }
    Ok(())
}

/// Accessors reached before init (tests and tools) load synchronously.
fn admin() -> &'static Signer {
    ADMIN.get_or_init(|| load_admin_sync().unwrap_or_else(|e| panic!("{e}")))
}

fn operator() -> &'static Option<Signer> {
    OPERATOR.get_or_init(|| load_operator_sync().unwrap_or_else(|e| panic!("{e}")))
}

async fn load_admin() -> Result<Signer, String> {
    build(SignerRole::Admin).await.map_err(admin_error)
}

async fn load_operator() -> Result<Option<Signer>, String> {
    if operator_unset() {
        return Ok(None);
    }
    build(SignerRole::Operator)
        .await
        .map(Some)
        .map_err(operator_error)
}

/// The sync loaders refuse Privy rather than blocking on it inside a runtime.
fn load_admin_sync() -> Result<Signer, String> {
    match load_signer(SignerRole::Admin).map_err(admin_error)? {
        Loaded::Built(signer) => Ok(signer),
        Loaded::Privy { .. } => Err(privy_needs_init(ADMIN_SIGNER)),
    }
}

fn load_operator_sync() -> Result<Option<Signer>, String> {
    if operator_unset() {
        return Ok(None);
    }
    match load_signer(SignerRole::Operator).map_err(operator_error)? {
        Loaded::Built(signer) => Ok(Some(signer)),
        Loaded::Privy { .. } => Err(privy_needs_init(OPERATOR_SIGNER)),
    }
}

async fn build(role: SignerRole) -> Result<Signer, LoadError> {
    match load_signer(role)? {
        Loaded::Built(signer) => Ok(signer),
        Loaded::Privy {
            app_id,
            app_secret,
            wallet_id,
        } => {
            let signer = Signer::from_privy(app_id, app_secret, wallet_id, None).await?;
            let role_name = match role {
                SignerRole::Admin => "admin",
                SignerRole::Operator => "operator",
            };
            info!("Loaded {} signer (privy): {}", role_name, signer.pubkey());
            Ok(signer)
        }
    }
}

/// The admin stands in for the operator only when OPERATOR_SIGNER is unset; a set but
/// unloadable one is fatal, like the admin.
fn operator_unset() -> bool {
    if env::var_os(OPERATOR_SIGNER).is_some() {
        return false;
    }
    warn!("OPERATOR_SIGNER not set - release and rotation will use admin as operator");
    true
}

fn admin_error(e: LoadError) -> String {
    format!("ADMIN_SIGNER must be configured: {e}")
}

fn operator_error(e: LoadError) -> String {
    format!("OPERATOR_SIGNER is set but could not be loaded: {e}")
}

fn privy_needs_init(type_var: &str) -> String {
    format!("{type_var}=privy loads asynchronously; call init_signers at startup")
}

/// Load signer from environment variables
fn load_signer(role: SignerRole) -> Result<Loaded, LoadError> {
    let (role_name, type_var) = match role {
        SignerRole::Admin => ("admin", ADMIN_SIGNER),
        SignerRole::Operator => ("operator", OPERATOR_SIGNER),
    };

    let signer_type_str =
        env::var(type_var).map_err(|_| LoadError::Config(format!("{} not set", type_var)))?;
    let signer_type = SignerType::from_str(&signer_type_str)?;

    let signer = match signer_type {
        SignerType::Memory => {
            let private_key_var = match role {
                SignerRole::Admin => ADMIN_PRIVATE_KEY,
                SignerRole::Operator => OPERATOR_PRIVATE_KEY,
            };
            let private_key = env::var(private_key_var)
                .map_err(|_| LoadError::Config(format!("{} not set", private_key_var)))?;
            // Reject a set-but-empty value: env::var returns Ok("") for a blank var.
            if private_key.trim().is_empty() {
                return Err(LoadError::Config(format!(
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
            let vault_addr = env::var(vault_addr_var)
                .map_err(|_| LoadError::Config(format!("{} not set", vault_addr_var)))?;
            let vault_token = env::var(vault_token_var)
                .map_err(|_| LoadError::Config(format!("{} not set", vault_token_var)))?;

            let key_name = env::var(key_name_var)
                .map_err(|_| LoadError::Config(format!("{} not set", key_name_var)))?;
            let pubkey = env::var(pubkey_var)
                .map_err(|_| LoadError::Config(format!("{} not set", pubkey_var)))?;
            Signer::from_vault(vault_addr, vault_token, key_name, pubkey, None)?
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
            let api_public_key = env::var(api_public_key_var)
                .map_err(|_| LoadError::Config(format!("{} not set", api_public_key_var)))?;
            let api_private_key = env::var(api_private_key_var)
                .map_err(|_| LoadError::Config(format!("{} not set", api_private_key_var)))?;
            let organization_id = env::var(organization_id_var)
                .map_err(|_| LoadError::Config(format!("{} not set", organization_id_var)))?;
            let public_key = env::var(pubkey_var)
                .map_err(|_| LoadError::Config(format!("{} not set", pubkey_var)))?;
            let private_key_id = env::var(private_key_id_var)
                .map_err(|_| LoadError::Config(format!("{} not set", private_key_id_var)))?;
            Signer::from_turnkey(
                api_public_key,
                api_private_key,
                organization_id,
                private_key_id,
                public_key,
                None,
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
                .map_err(|_| LoadError::Config(format!("{} not set", app_id_var)))?;
            let app_secret = env::var(app_secret_var)
                .map_err(|_| LoadError::Config(format!("{} not set", app_secret_var)))?;
            let wallet_id = env::var(wallet_id_var)
                .map_err(|_| LoadError::Config(format!("{} not set", wallet_id_var)))?;

            return Ok(Loaded::Privy {
                app_id,
                app_secret,
                wallet_id,
            });
        }
    };

    info!(
        "Loaded {} signer ({}): {}",
        role_name,
        signer_type_str,
        signer.pubkey()
    );
    Ok(Loaded::Built(signer))
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
        admin()
    }

    pub fn operator_signer() -> &'static Signer {
        operator().as_ref().unwrap_or_else(admin)
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
    /// string, including an empty one, must return an InvalidPrivateKey error.
    #[test]
    fn signer_type_from_str_unknown_errors() {
        let err = SignerType::from_str("unknown").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("Unsupported signer type"),
            "unexpected error: {msg}"
        );
        assert!(SignerType::from_str("").is_err());
    }

    /// When ADMIN_SIGNER is absent, load_signer must fail immediately with a message
    /// naming the missing variable so the operator can identify the misconfiguration.
    #[test]
    #[serial]
    fn load_signer_admin_no_env_var_errors() {
        settle_globals();
        let original = env::var(ADMIN_SIGNER).ok();
        env::remove_var(ADMIN_SIGNER);

        let err = load_signer(SignerRole::Admin)
            .err()
            .expect("expected error");
        let msg = err.to_string();
        assert!(
            msg.contains("ADMIN_SIGNER") && msg.contains("not set"),
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
        settle_globals();
        let orig_type = env::var(ADMIN_SIGNER).ok();
        let orig_key = env::var(ADMIN_PRIVATE_KEY).ok();
        env::set_var(ADMIN_SIGNER, "memory");
        env::remove_var(ADMIN_PRIVATE_KEY);

        let err = load_signer(SignerRole::Admin)
            .err()
            .expect("expected error");
        let msg = err.to_string();
        assert!(
            msg.contains("ADMIN_PRIVATE_KEY") && msg.contains("not set"),
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
        settle_globals();
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
        settle_globals();
        let orig_type = env::var(ADMIN_SIGNER).ok();
        let orig_addr = env::var(ADMIN_VAULT_ADDR).ok();
        env::set_var(ADMIN_SIGNER, "vault");
        env::remove_var(ADMIN_VAULT_ADDR);

        let result = load_signer(SignerRole::Admin);
        assert!(result.is_err());
        let err = result.err().expect("expected error");
        let msg = err.to_string();
        assert!(
            msg.contains("ADMIN_VAULT_ADDR") && msg.contains("not set"),
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
        settle_globals();
        let orig_type = env::var(ADMIN_SIGNER).ok();
        let orig_key = env::var(ADMIN_TURNKEY_API_PUBLIC_KEY).ok();
        env::set_var(ADMIN_SIGNER, "turnkey");
        env::remove_var(ADMIN_TURNKEY_API_PUBLIC_KEY);

        let result = load_signer(SignerRole::Admin);
        assert!(result.is_err());
        let err = result.err().expect("expected error");
        let msg = err.to_string();
        assert!(
            msg.contains("ADMIN_TURNKEY_API_PUBLIC_KEY") && msg.contains("not set"),
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
        settle_globals();
        let orig_type = env::var(ADMIN_SIGNER).ok();
        let orig_app_id = env::var(ADMIN_PRIVY_APP_ID).ok();
        env::set_var(ADMIN_SIGNER, "privy");
        env::remove_var(ADMIN_PRIVY_APP_ID);

        let result = load_signer(SignerRole::Admin);
        assert!(result.is_err());
        let err = result.err().expect("expected error");
        let msg = err.to_string();
        assert!(
            msg.contains("ADMIN_PRIVY_APP_ID") && msg.contains("not set"),
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

    /// When OPERATOR_SIGNER is absent, load_signer names the missing variable.
    #[test]
    #[serial]
    fn load_signer_operator_no_env_var_errors() {
        settle_globals();
        let orig = env::var(OPERATOR_SIGNER).ok();
        env::remove_var(OPERATOR_SIGNER);

        let err = load_signer(SignerRole::Operator)
            .err()
            .expect("expected error");
        let msg = err.to_string();
        assert!(
            msg.contains("OPERATOR_SIGNER") && msg.contains("not set"),
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
        settle_globals();
        let orig_type = env::var(OPERATOR_SIGNER).ok();
        let orig_key = env::var(OPERATOR_PRIVATE_KEY).ok();
        env::set_var(OPERATOR_SIGNER, "memory");
        env::remove_var(OPERATOR_PRIVATE_KEY);

        let err = load_signer(SignerRole::Operator)
            .err()
            .expect("expected error");
        let msg = err.to_string();
        assert!(
            msg.contains("OPERATOR_PRIVATE_KEY") && msg.contains("not set"),
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

    /// Loads the global signers from a valid env before a test breaks it, so parallel
    /// tests that read them never load from the broken env.
    fn settle_globals() {
        crate::operator::sender::test_support::ensure_test_signer();
        SignerUtil::operator_signer();
    }

    /// Saves the named env vars, clears them, and restores them on drop.
    struct EnvGuard(Vec<(&'static str, Option<String>)>);

    impl EnvGuard {
        fn clear(names: &[&'static str]) -> Self {
            settle_globals();
            let saved = names.iter().map(|n| (*n, env::var(n).ok())).collect();
            for name in names {
                env::remove_var(name);
            }
            Self(saved)
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for (name, value) in &self.0 {
                match value {
                    Some(v) => env::set_var(name, v),
                    None => env::remove_var(name),
                }
            }
        }
    }

    const ALL_VARS: [&str; 10] = [
        ADMIN_SIGNER,
        ADMIN_PRIVATE_KEY,
        ADMIN_PRIVY_APP_ID,
        ADMIN_PRIVY_APP_SECRET,
        ADMIN_PRIVY_WALLET_ID,
        OPERATOR_SIGNER,
        OPERATOR_PRIVATE_KEY,
        OPERATOR_PRIVY_APP_ID,
        OPERATOR_PRIVY_APP_SECRET,
        OPERATOR_PRIVY_WALLET_ID,
    ];

    fn set_memory_admin() -> Pubkey {
        let keypair = solana_sdk::signer::keypair::Keypair::new();
        env::set_var(ADMIN_SIGNER, "memory");
        env::set_var(
            ADMIN_PRIVATE_KEY,
            bs58::encode(keypair.to_bytes()).into_string(),
        );
        solana_sdk::signer::Signer::pubkey(&keypair)
    }

    fn set_fake_privy(role: SignerRole) {
        let (type_var, app_id, app_secret, wallet_id) = match role {
            SignerRole::Admin => (
                ADMIN_SIGNER,
                ADMIN_PRIVY_APP_ID,
                ADMIN_PRIVY_APP_SECRET,
                ADMIN_PRIVY_WALLET_ID,
            ),
            SignerRole::Operator => (
                OPERATOR_SIGNER,
                OPERATOR_PRIVY_APP_ID,
                OPERATOR_PRIVY_APP_SECRET,
                OPERATOR_PRIVY_WALLET_ID,
            ),
        };
        env::set_var(type_var, "privy");
        env::set_var(app_id, "fake-app");
        env::set_var(app_secret, "fake-secret");
        env::set_var(wallet_id, "fake-wallet");
    }

    /// Complete Privy creds must reach Privy's own request on the running runtime, which
    /// the old nested `block_on` panicked on. A zero timeout stops it there with no network
    /// round trip; a config error means we refused Privy before calling it.
    #[tokio::test]
    #[serial]
    async fn privy_signer_is_awaited_on_the_runtime() {
        let _env = EnvGuard::clear(&ALL_VARS);
        set_fake_privy(SignerRole::Admin);

        let result =
            tokio::time::timeout(std::time::Duration::ZERO, build(SignerRole::Admin)).await;
        assert!(
            matches!(result, Err(_) | Ok(Err(LoadError::Keychain(_)))),
            "build must await Privy, got {:?}",
            result.map(|r| r.map(|s| s.pubkey()).map_err(|e| e.to_string()))
        );
    }

    /// A bad admin is a startup error, not a panic. Only an unset OPERATOR_SIGNER falls
    /// back to admin; a set but unloadable one is an error on both load paths, and never
    /// blocks the admin-only load resync uses.
    #[tokio::test]
    #[serial]
    async fn admin_errors_and_only_unset_operator_falls_back() {
        let _env = EnvGuard::clear(&ALL_VARS);
        let err = load_admin().await.err().expect("admin unset must fail");
        assert!(
            err.contains("ADMIN_SIGNER must be configured") && err.contains("ADMIN_SIGNER not set"),
            "got: {err}"
        );

        let admin = set_memory_admin();
        let unset = load_operator().await.expect("unset operator must load");
        assert!(unset.is_none(), "unset operator signs as admin");

        env::set_var(OPERATOR_SIGNER, "memory");
        let loaded = load_admin()
            .await
            .expect("a broken operator must not block admin");
        assert_eq!(loaded.pubkey(), admin);
        let errs = [
            load_operator().await.err().expect("async: broken operator"),
            load_operator_sync().err().expect("sync: broken operator"),
        ];
        for err in errs {
            assert!(
                err.contains("OPERATOR_SIGNER is set but could not be loaded")
                    && err.contains("OPERATOR_PRIVATE_KEY"),
                "got: {err}"
            );
        }
    }

    /// The sync fallback refuses Privy for either role instead of blocking on it.
    #[tokio::test]
    #[serial]
    async fn sync_loader_refuses_privy_for_either_role() {
        let _env = EnvGuard::clear(&ALL_VARS);
        set_fake_privy(SignerRole::Admin);
        let err = load_admin_sync()
            .err()
            .expect("privy admin must be refused");
        assert!(
            err.contains("ADMIN_SIGNER") && err.contains("init_signers"),
            "got: {err}"
        );

        set_memory_admin();
        set_fake_privy(SignerRole::Operator);
        let err = load_operator_sync()
            .err()
            .expect("privy operator must be refused");
        assert!(
            err.contains("OPERATOR_SIGNER") && err.contains("init_signers"),
            "got: {err}"
        );
    }
}
