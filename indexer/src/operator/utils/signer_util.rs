use solana_keychain::{Signer, SignerError, SolanaSigner};
use solana_sdk::pubkey::Pubkey;
use std::env;
use std::sync::OnceLock;
use tracing::info;

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

/// Key material vars for the operator role. Any of them set without `OPERATOR_SIGNER` is a
/// half-written config, refused rather than silently replaced by the admin signer.
const OPERATOR_KEY_VARS: [&str; 13] = [
    OPERATOR_PRIVATE_KEY,
    OPERATOR_VAULT_ADDR,
    OPERATOR_VAULT_TOKEN,
    OPERATOR_VAULT_KEY_NAME,
    OPERATOR_VAULT_PUBKEY,
    OPERATOR_TURNKEY_API_PUBLIC_KEY,
    OPERATOR_TURNKEY_API_PRIVATE_KEY,
    OPERATOR_TURNKEY_ORGANIZATION_ID,
    OPERATOR_TURNKEY_PRIVATE_KEY_ID,
    OPERATOR_TURNKEY_PUBKEY,
    OPERATOR_PRIVY_APP_ID,
    OPERATOR_PRIVY_APP_SECRET,
    OPERATOR_PRIVY_WALLET_ID,
];

/// Reads one env var. Tests pass a map so they never touch the process env.
type Env<'a> = &'a (dyn Fn(&str) -> Option<String> + Sync);

fn process_env(name: &str) -> Option<String> {
    env::var(name).ok()
}

/// Treats a blank value as unset, the repo-wide convention for rendered env files.
fn non_blank(env: Env, name: &str) -> Option<String> {
    env(name).filter(|v| !v.trim().is_empty())
}

fn required(env: Env, name: &str) -> Result<String, LoadError> {
    non_blank(env, name).ok_or_else(|| LoadError::Config(format!("{} not set", name)))
}

/// One role's signer settings, read from env before any key is parsed or any call made.
enum SignerSpec {
    Memory {
        private_key: String,
    },
    Vault {
        addr: String,
        token: String,
        key_name: String,
        pubkey: String,
    },
    Turnkey {
        api_public_key: String,
        api_private_key: String,
        organization_id: String,
        private_key_id: String,
        public_key: String,
    },
    Privy {
        app_id: String,
        app_secret: String,
        wallet_id: String,
    },
}

impl SignerSpec {
    fn label(&self) -> &'static str {
        match self {
            Self::Memory { .. } => "memory",
            Self::Vault { .. } => "vault",
            Self::Turnkey { .. } => "turnkey",
            Self::Privy { .. } => "privy",
        }
    }
}

/// Admin must be configured. Operator unset (with no `OPERATOR_*` key var) is `None` and
/// signs as admin; set means it must load.
fn read_role_spec(role: SignerRole, env: Env) -> Result<Option<SignerSpec>, LoadError> {
    let type_var = match role {
        SignerRole::Admin => ADMIN_SIGNER,
        SignerRole::Operator => OPERATOR_SIGNER,
    };
    let Some(signer_type_str) = non_blank(env, type_var) else {
        if role == SignerRole::Admin {
            return Err(LoadError::Config(format!("{} not set", type_var)));
        }
        return match OPERATOR_KEY_VARS
            .iter()
            .find(|v| non_blank(env, v).is_some())
        {
            Some(var) => Err(LoadError::Config(format!(
                "{} is set but {} is not; set {} or remove {}",
                var, OPERATOR_SIGNER, OPERATOR_SIGNER, var
            ))),
            None => Ok(None),
        };
    };

    let spec = match SignerType::from_str(signer_type_str.trim())? {
        SignerType::Memory => {
            let private_key_var = match role {
                SignerRole::Admin => ADMIN_PRIVATE_KEY,
                SignerRole::Operator => OPERATOR_PRIVATE_KEY,
            };
            let private_key = env(private_key_var)
                .ok_or_else(|| LoadError::Config(format!("{} not set", private_key_var)))?;
            // Reject a set-but-empty value: env::var returns Ok("") for a blank var.
            if private_key.trim().is_empty() {
                return Err(LoadError::Config(format!(
                    "{} is set but empty",
                    private_key_var
                )));
            }
            SignerSpec::Memory { private_key }
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
            SignerSpec::Vault {
                addr: required(env, vault_addr_var)?,
                token: required(env, vault_token_var)?,
                key_name: required(env, key_name_var)?,
                pubkey: required(env, pubkey_var)?,
            }
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
            SignerSpec::Turnkey {
                api_public_key: required(env, api_public_key_var)?,
                api_private_key: required(env, api_private_key_var)?,
                organization_id: required(env, organization_id_var)?,
                public_key: required(env, pubkey_var)?,
                private_key_id: required(env, private_key_id_var)?,
            }
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
            SignerSpec::Privy {
                app_id: required(env, app_id_var)?,
                app_secret: required(env, app_secret_var)?,
                wallet_id: required(env, wallet_id_var)?,
            }
        }
    };
    Ok(Some(spec))
}

/// Every backend except Privy, whose constructor is async and must be awaited.
fn build_sync(spec: SignerSpec) -> Result<Signer, LoadError> {
    Ok(match spec {
        SignerSpec::Memory { private_key } => Signer::from_memory(&private_key)?,
        SignerSpec::Vault {
            addr,
            token,
            key_name,
            pubkey,
        } => Signer::from_vault(addr, token, key_name, pubkey, None)?,
        SignerSpec::Turnkey {
            api_public_key,
            api_private_key,
            organization_id,
            private_key_id,
            public_key,
        } => Signer::from_turnkey(
            api_public_key,
            api_private_key,
            organization_id,
            private_key_id,
            public_key,
            None,
        )?,
        SignerSpec::Privy { .. } => {
            return Err(LoadError::Config(
                "privy signer loads asynchronously; call init_signers at startup".to_string(),
            ))
        }
    })
}

async fn build(spec: SignerSpec) -> Result<Signer, LoadError> {
    match spec {
        SignerSpec::Privy {
            app_id,
            app_secret,
            wallet_id,
        } => Ok(Signer::from_privy(app_id, app_secret, wallet_id, None).await?),
        other => build_sync(other),
    }
}

/// Both role signers. `operator: None` means the operator role signs with admin.
struct Signers {
    admin: Signer,
    operator: Option<Signer>,
}

/// Both specs are read before anything is built, so a bad operator config fails before
/// an admin key is loaded or a remote signer is called.
fn read_specs(env: Env) -> Result<(SignerSpec, Option<SignerSpec>), String> {
    let admin = read_role_spec(SignerRole::Admin, env)
        .map_err(|e| format!("admin signer: {e}"))?
        .ok_or_else(|| format!("admin signer: {} not set", ADMIN_SIGNER))?;
    let operator =
        read_role_spec(SignerRole::Operator, env).map_err(|e| format!("operator signer: {e}"))?;
    Ok((admin, operator))
}

fn log_loaded(role: &str, label: &str, signer: &Signer) {
    info!("Loaded {} signer ({}): {}", role, label, signer.pubkey());
}

fn log_operator_role(operator: &Option<Signer>) {
    if operator.is_none() {
        info!(
            "{} not set; the operator role signs with the admin signer",
            OPERATOR_SIGNER
        );
    }
}

async fn load_signers(env: Env<'_>) -> Result<Signers, String> {
    let (admin_spec, operator_spec) = read_specs(env)?;
    let label = admin_spec.label();
    let admin = build(admin_spec)
        .await
        .map_err(|e| format!("admin signer: {e}"))?;
    log_loaded("admin", label, &admin);
    let operator = match operator_spec {
        Some(spec) => {
            let label = spec.label();
            let signer = build(spec)
                .await
                .map_err(|e| format!("operator signer: {e}"))?;
            log_loaded("operator", label, &signer);
            Some(signer)
        }
        None => None,
    };
    log_operator_role(&operator);
    Ok(Signers { admin, operator })
}

fn load_signers_sync(env: Env) -> Result<Signers, String> {
    let (admin_spec, operator_spec) = read_specs(env)?;
    let label = admin_spec.label();
    let admin = build_sync(admin_spec).map_err(|e| format!("admin signer: {e}"))?;
    log_loaded("admin", label, &admin);
    let operator = match operator_spec {
        Some(spec) => {
            let label = spec.label();
            let signer = build_sync(spec).map_err(|e| format!("operator signer: {e}"))?;
            log_loaded("operator", label, &signer);
            Some(signer)
        }
        None => None,
    };
    log_operator_role(&operator);
    Ok(Signers { admin, operator })
}

static SIGNERS: OnceLock<Signers> = OnceLock::new();

/// Loads both role signers from env and installs them. Call at startup, before any
/// connection is opened, so a bad signer config is a startup error rather than a panic.
pub async fn init_signers() -> Result<(), String> {
    install(&SIGNERS, &process_env).await
}

/// First install wins and later calls are no-ops, so tests that start several operators
/// in one process keep one key set.
async fn install(cell: &OnceLock<Signers>, env: Env<'_>) -> Result<(), String> {
    if cell.get().is_some() {
        return Ok(());
    }
    let signers = load_signers(env).await?;
    let _ = cell.set(signers);
    Ok(())
}

/// Binaries call `init_signers` first; this sync fallback only serves tests and tools that
/// never do, and it refuses Privy instead of blocking inside the runtime.
fn signers() -> &'static Signers {
    SIGNERS.get_or_init(|| {
        load_signers_sync(&process_env)
            .unwrap_or_else(|e| panic!("signers not initialized and env does not load: {e}"))
    })
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
        &signers().admin
    }

    pub fn operator_signer() -> &'static Signer {
        let signers = signers();
        signers.operator.as_ref().unwrap_or(&signers.admin)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::signature::Keypair;
    use std::collections::HashMap;

    fn env_of(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |name| map.get(name).cloned()
    }

    fn b58(kp: &Keypair) -> String {
        bs58::encode(kp.to_bytes()).into_string()
    }

    fn spec_err(role: SignerRole, pairs: &[(&str, &str)]) -> String {
        read_role_spec(role, &env_of(pairs))
            .err()
            .expect("expected an error")
            .to_string()
    }

    /// Only "memory", "vault", "turnkey", and "privy" are valid signer types.
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

    #[test]
    fn admin_spec_requires_backend_and_key() {
        for pairs in [&[][..], &[(ADMIN_SIGNER, "  ")][..]] {
            let msg = spec_err(SignerRole::Admin, pairs);
            assert!(msg.contains("ADMIN_SIGNER not set"), "got: {msg}");
        }
        let msg = spec_err(SignerRole::Admin, &[(ADMIN_SIGNER, "memory")]);
        assert!(msg.contains("ADMIN_PRIVATE_KEY not set"), "got: {msg}");
        for blank in ["", "   ", "\t\n"] {
            let msg = spec_err(
                SignerRole::Admin,
                &[(ADMIN_SIGNER, "memory"), (ADMIN_PRIVATE_KEY, blank)],
            );
            assert!(
                msg.contains("ADMIN_PRIVATE_KEY is set but empty"),
                "got: {msg}"
            );
        }
    }

    /// Each remote backend names the first missing credential.
    #[test]
    fn remote_backends_name_the_missing_var() {
        for (backend, var) in [
            ("vault", ADMIN_VAULT_ADDR),
            ("turnkey", ADMIN_TURNKEY_API_PUBLIC_KEY),
            ("privy", ADMIN_PRIVY_APP_ID),
        ] {
            let msg = spec_err(SignerRole::Admin, &[(ADMIN_SIGNER, backend)]);
            assert!(msg.contains(&format!("{var} not set")), "got: {msg}");
        }
    }

    /// A blank remote credential is unset, like a blank memory key.
    #[test]
    fn blank_remote_credentials_are_unset() {
        let msg = spec_err(
            SignerRole::Admin,
            &[(ADMIN_SIGNER, "vault"), (ADMIN_VAULT_ADDR, "  ")],
        );
        assert!(msg.contains("ADMIN_VAULT_ADDR not set"), "got: {msg}");
    }

    #[test]
    fn unknown_backend_is_refused() {
        let msg = spec_err(SignerRole::Admin, &[(ADMIN_SIGNER, "hsm")]);
        assert!(msg.contains("Unsupported signer type"), "got: {msg}");
    }

    #[test]
    fn operator_spec_unset_is_none() {
        for pairs in [&[][..], &[(OPERATOR_SIGNER, "")][..]] {
            let spec = read_role_spec(SignerRole::Operator, &env_of(pairs))
                .unwrap_or_else(|e| panic!("unset operator must not error: {e}"));
            assert!(spec.is_none());
        }
    }

    /// The SOLA13-75 regression: a configured operator that will not load is an error,
    /// never a silent fallback to the admin key.
    #[test]
    fn operator_spec_set_must_load() {
        let msg = spec_err(SignerRole::Operator, &[(OPERATOR_SIGNER, "memory")]);
        assert!(msg.contains("OPERATOR_PRIVATE_KEY not set"), "got: {msg}");

        let admin = b58(&Keypair::new());
        let env = env_of(&[
            (ADMIN_SIGNER, "memory"),
            (ADMIN_PRIVATE_KEY, &admin),
            (OPERATOR_SIGNER, "memory"),
            (OPERATOR_PRIVATE_KEY, "garbage"),
        ]);
        let err = load_signers_sync(&env)
            .err()
            .expect("garbage key must fail");
        assert!(err.starts_with("operator signer:"), "got: {err}");
    }

    #[test]
    fn operator_spec_partial_config_is_refused() {
        for var in OPERATOR_KEY_VARS {
            let msg = spec_err(SignerRole::Operator, &[(var, "x")]);
            assert!(
                msg.contains(var) && msg.contains("OPERATOR_SIGNER is not"),
                "{var}: {msg}"
            );
        }
    }

    #[test]
    fn unset_operator_resolves_to_admin_and_set_operator_loads() {
        let admin = Keypair::new();
        let operator = Keypair::new();
        let admin_b58 = b58(&admin);
        let only_admin = env_of(&[(ADMIN_SIGNER, "memory"), (ADMIN_PRIVATE_KEY, &admin_b58)]);
        let signers = load_signers_sync(&only_admin).unwrap();
        assert_eq!(
            signers.admin.pubkey(),
            solana_sdk::signer::Signer::pubkey(&admin)
        );
        assert!(signers.operator.is_none());

        let operator_b58 = b58(&operator);
        let both = env_of(&[
            (ADMIN_SIGNER, "memory"),
            (ADMIN_PRIVATE_KEY, &admin_b58),
            (OPERATOR_SIGNER, "memory"),
            (OPERATOR_PRIVATE_KEY, &operator_b58),
        ]);
        let signers = load_signers_sync(&both).unwrap();
        assert_eq!(
            signers.operator.unwrap().pubkey(),
            solana_sdk::signer::Signer::pubkey(&operator)
        );
    }

    /// The SOLA13-17 regression: Privy on the sync path is a config error, not a nested
    /// `block_on` panic inside the running runtime.
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn privy_on_the_sync_path_errors_instead_of_panicking() {
        let env = env_of(&[
            (ADMIN_SIGNER, "privy"),
            (ADMIN_PRIVY_APP_ID, "app"),
            (ADMIN_PRIVY_APP_SECRET, "secret"),
            (ADMIN_PRIVY_WALLET_ID, "wallet"),
        ]);
        let err = load_signers_sync(&env).err().expect("privy is async only");
        assert!(err.contains("init_signers"), "got: {err}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn privy_init_returns_a_config_error_inside_a_runtime() {
        let env = env_of(&[(ADMIN_SIGNER, "privy")]);
        let err = load_signers(&env).await.err().expect("missing privy var");
        assert!(err.contains("ADMIN_PRIVY_APP_ID not set"), "got: {err}");
    }

    /// Complete credentials reach the awaited Privy constructor. A nested `block_on` would
    /// panic on the first poll; the fake wallet only ever yields an error or the timeout.
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn privy_init_awaits_the_constructor_inside_a_runtime() {
        let env = env_of(&[
            (ADMIN_SIGNER, "privy"),
            (ADMIN_PRIVY_APP_ID, "app"),
            (ADMIN_PRIVY_APP_SECRET, "secret"),
            (ADMIN_PRIVY_WALLET_ID, "wallet"),
        ]);
        if let Ok(result) =
            tokio::time::timeout(std::time::Duration::from_secs(5), load_signers(&env)).await
        {
            assert!(result.is_err(), "a fake privy wallet must not load");
        }
    }

    #[tokio::test]
    async fn install_is_idempotent_and_first_wins() {
        let cell = OnceLock::new();
        let first = Keypair::new();
        let first_b58 = b58(&first);
        let env = env_of(&[(ADMIN_SIGNER, "memory"), (ADMIN_PRIVATE_KEY, &first_b58)]);
        install(&cell, &env).await.unwrap();

        let second_b58 = b58(&Keypair::new());
        let env = env_of(&[(ADMIN_SIGNER, "memory"), (ADMIN_PRIVATE_KEY, &second_b58)]);
        install(&cell, &env).await.unwrap();
        assert_eq!(
            cell.get().unwrap().admin.pubkey(),
            solana_sdk::signer::Signer::pubkey(&first)
        );
    }

    #[tokio::test]
    async fn install_reports_a_bad_config_and_installs_nothing() {
        let cell = OnceLock::new();
        let env = env_of(&[(ADMIN_SIGNER, "memory")]);
        let err = install(&cell, &env).await.unwrap_err();
        assert!(err.contains("ADMIN_PRIVATE_KEY not set"), "got: {err}");
        assert!(cell.get().is_none());
    }
}
