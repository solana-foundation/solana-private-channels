use once_cell::sync::Lazy;
use solana_keychain::{GcpKmsSignerConfig, Signer, SignerError, SolanaSigner};
use solana_sdk::pubkey::Pubkey;
use std::{env, future::Future};
use tracing::{info, warn};

#[derive(Debug, thiserror::Error)]
enum SignerConfigError {
    // Local messages name variables, never their values.
    #[error("{0}")]
    InvalidConfig(String),
    // Preserve Keychain's redaction of backend errors and private key material.
    #[error(transparent)]
    Signer(#[from] SignerError),
}

#[derive(Clone, Copy)]
enum SignerRole {
    Admin,
    Operator,
}

impl SignerRole {
    fn prefix(self) -> &'static str {
        match self {
            Self::Admin => "ADMIN",
            Self::Operator => "OPERATOR",
        }
    }

    fn required_env(self, suffix: &str) -> Result<String, SignerConfigError> {
        let name = format!("{}_{suffix}", self.prefix());
        let value = env::var(&name)
            .map_err(|_| SignerConfigError::InvalidConfig(format!("{name} not set")))?;
        if value.trim().is_empty() {
            return Err(SignerConfigError::InvalidConfig(format!(
                "{name} is set but empty"
            )));
        }
        Ok(value)
    }
}

static ADMIN_SIGNER_INSTANCE: Lazy<Signer> =
    Lazy::new(|| load_signer(SignerRole::Admin).expect("ADMIN_SIGNER must be configured"));

static OPERATOR_SIGNER_INSTANCE: Lazy<Option<Signer>> =
    Lazy::new(|| load_operator_signer().expect("OPERATOR_SIGNER configuration is invalid"));

fn load_operator_signer() -> Result<Option<Signer>, SignerConfigError> {
    match env::var("OPERATOR_SIGNER") {
        Err(env::VarError::NotPresent) => {
            warn!("OPERATOR_SIGNER not configured - release funds will use admin as operator");
            Ok(None)
        }
        // A configured signer must never silently fall back to a different key.
        _ => load_signer(SignerRole::Operator).map(Some),
    }
}

fn gcp_kms_config(role: SignerRole) -> Result<GcpKmsSignerConfig, SignerConfigError> {
    let key_name = role.required_env("GCP_KMS_KEY_NAME")?;
    // Pin a concrete version: rotating a KMS version changes the Solana address.
    let parts: Vec<_> = key_name.split('/').collect();
    if !matches!(parts.as_slice(),
        ["projects", project, "locations", location, "keyRings", ring,
         "cryptoKeys", key, "cryptoKeyVersions", version]
        if [project, location, ring, key].iter().all(|part| !part.is_empty())
            && version.bytes().all(|byte| byte.is_ascii_digit())
            && version.parse::<u64>().is_ok_and(|version| version > 0))
    {
        return Err(SignerConfigError::InvalidConfig(format!(
            "{}_GCP_KMS_KEY_NAME must be a full GCP KMS cryptoKeyVersions/<number> resource name",
            role.prefix()
        )));
    }
    let public_key = role.required_env("GCP_KMS_PUBLIC_KEY")?;
    public_key.parse::<Pubkey>().map_err(|_| {
        SignerConfigError::InvalidConfig(format!(
            "{}_GCP_KMS_PUBLIC_KEY must be a base58 Solana public key",
            role.prefix()
        ))
    })?;
    Ok(GcpKmsSignerConfig {
        key_name,
        public_key,
    })
}

fn initialize_remote_signer(
    future: impl Future<Output = Result<Signer, SignerError>>,
) -> Result<Signer, SignerError> {
    // Called once during startup on the binary's multi-thread Tokio runtime.
    // Yield its worker before waiting; plain Handle::block_on would panic here.
    tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(future))
}

fn load_signer(role: SignerRole) -> Result<Signer, SignerConfigError> {
    let signer_type = role.required_env("SIGNER")?.to_lowercase();
    let signer = match signer_type.as_str() {
        "memory" => Signer::from_memory(&role.required_env("PRIVATE_KEY")?)?,
        "vault" => Signer::from_vault(
            role.required_env("VAULT_ADDR")?,
            role.required_env("VAULT_TOKEN")?,
            role.required_env("VAULT_KEY_NAME")?,
            role.required_env("VAULT_PUBKEY")?,
            None,
        )?,
        "turnkey" => Signer::from_turnkey(
            role.required_env("TURNKEY_API_PUBLIC_KEY")?,
            role.required_env("TURNKEY_API_PRIVATE_KEY")?,
            role.required_env("TURNKEY_ORGANIZATION_ID")?,
            role.required_env("TURNKEY_PRIVATE_KEY_ID")?,
            role.required_env("TURNKEY_PUBKEY")?,
            None,
        )?,
        "privy" => initialize_remote_signer(Signer::from_privy(
            role.required_env("PRIVY_APP_ID")?,
            role.required_env("PRIVY_APP_SECRET")?,
            role.required_env("PRIVY_WALLET_ID")?,
            None,
        ))?,
        "gcp_kms" => {
            let config = gcp_kms_config(role)?;
            initialize_remote_signer(Signer::from_gcp_kms(config.key_name, config.public_key))?
        }
        _ => {
            return Err(SignerConfigError::InvalidConfig(
                "Unsupported signer type. Supported: memory, vault, turnkey, privy, gcp_kms".into(),
            ))
        }
    };
    info!(
        "Loaded {} signer ({}): {}",
        role.prefix(),
        signer_type,
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
    use serial_test::serial;
    use solana_sdk::signature::{Keypair, Signer as _};
    use std::ffi::OsString;

    const KEY_NAME: &str =
        "projects/test/locations/us-central1/keyRings/channel/cryptoKeys/operator/cryptoKeyVersions/1";
    const PUBLIC_KEY: &str = "11111111111111111111111111111111";

    // Restore only touched variables, including when an assertion panics.
    struct TestEnv(Vec<(String, Option<OsString>)>);

    impl TestEnv {
        fn new(values: &[(&str, Option<&str>)]) -> Self {
            let original = values
                .iter()
                .map(|(key, _)| (key.to_string(), env::var_os(key)))
                .collect();
            for (key, value) in values {
                match value {
                    Some(value) => env::set_var(key, value),
                    None => env::remove_var(key),
                }
            }
            Self(original)
        }
    }

    impl Drop for TestEnv {
        fn drop(&mut self) {
            for (key, value) in &self.0 {
                match value {
                    Some(value) => env::set_var(key, value),
                    None => env::remove_var(key),
                }
            }
        }
    }

    #[test]
    #[serial]
    fn required_credentials_are_role_scoped_and_reject_missing_or_blank_values() {
        for role in [SignerRole::Admin, SignerRole::Operator] {
            let type_var = format!("{}_SIGNER", role.prefix());
            for (backend, suffix) in [
                ("memory", "SIGNER"),
                ("memory", "PRIVATE_KEY"),
                ("vault", "VAULT_ADDR"),
                ("turnkey", "TURNKEY_API_PUBLIC_KEY"),
                ("privy", "PRIVY_APP_ID"),
                ("gcp_kms", "GCP_KMS_KEY_NAME"),
                ("GCP_KMS", "GCP_KMS_KEY_NAME"),
            ] {
                let name = format!("{}_{suffix}", role.prefix());
                let _env = TestEnv::new(&[(&type_var, Some(backend))]);
                for value in [None, Some(""), Some(" \t\n")] {
                    let _invalid = TestEnv::new(&[(&name, value)]);
                    let error = load_signer(role).err().expect("invalid config must fail");
                    assert!(error.to_string().contains(&name), "{error}");
                }
            }
        }
    }

    #[test]
    #[serial]
    fn kms_configuration_requires_a_version_and_matching_public_key_format() {
        for role in [SignerRole::Admin, SignerRole::Operator] {
            let key_var = format!("{}_GCP_KMS_KEY_NAME", role.prefix());
            let pubkey_var = format!("{}_GCP_KMS_PUBLIC_KEY", role.prefix());
            let _env = TestEnv::new(&[(&key_var, Some(KEY_NAME)), (&pubkey_var, Some(PUBLIC_KEY))]);
            let config = gcp_kms_config(role).unwrap();
            assert_eq!(config.key_name, KEY_NAME);
            assert_eq!(config.public_key, PUBLIC_KEY);

            for key in [
                "projects/test",
                &KEY_NAME.replace("/1", "/latest"),
                &KEY_NAME.replace("/1", "/0"),
                &KEY_NAME.replace("/test/", "//"),
                &format!("{KEY_NAME}/extra"),
            ] {
                let _invalid = TestEnv::new(&[(&key_var, Some(key))]);
                assert!(gcp_kms_config(role)
                    .err()
                    .unwrap()
                    .to_string()
                    .contains(&key_var));
            }
            for pubkey in [None, Some(""), Some(" "), Some("invalid-public-key")] {
                let _invalid = TestEnv::new(&[(&pubkey_var, pubkey)]);
                let error = gcp_kms_config(role).err().unwrap().to_string();
                assert!(error.contains(&pubkey_var));
                assert!(!error.contains("invalid-public-key"));
            }
        }
    }

    #[test]
    #[serial]
    fn only_an_absent_operator_signer_falls_back() {
        let _env = TestEnv::new(&[("OPERATOR_SIGNER", None)]);
        assert!(load_operator_signer().unwrap().is_none());
        for signer_type in ["", "unknown", "gcp_kms", "memory"] {
            let _invalid = TestEnv::new(&[
                ("OPERATOR_SIGNER", Some(signer_type)),
                ("OPERATOR_GCP_KMS_KEY_NAME", None),
                ("OPERATOR_PRIVATE_KEY", None),
            ]);
            assert!(load_operator_signer().is_err());
        }
        let keypair = Keypair::new();
        let encoded_key = keypair.to_base58_string();
        let _valid = TestEnv::new(&[
            ("OPERATOR_SIGNER", Some("memory")),
            ("OPERATOR_PRIVATE_KEY", Some(&encoded_key)),
        ]);
        assert_eq!(
            load_operator_signer().unwrap().unwrap().pubkey(),
            keypair.pubkey()
        );
    }

    #[test]
    #[serial]
    fn backend_errors_do_not_expose_credentials() {
        let secret = "invalid-private-key-do-not-log";
        let _env = TestEnv::new(&[
            ("OPERATOR_SIGNER", Some("memory")),
            ("OPERATOR_PRIVATE_KEY", Some(secret)),
        ]);
        let error = load_operator_signer().err().unwrap();
        assert!(matches!(error, SignerConfigError::Signer(_)));
        assert!(!format!("{error} {error:?}").contains(secret));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn remote_initialization_yields_and_propagates_errors() {
        let error = initialize_remote_signer(async {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            Err(SignerError::RemoteApiError(
                "test initialization failed".into(),
            ))
        })
        .err()
        .unwrap();
        assert!(matches!(error, SignerError::RemoteApiError(_)));
    }

    #[tokio::test]
    async fn signing_preserves_both_transaction_signatures() {
        use solana_keychain::SignTransactionResult;
        use solana_sdk::{
            instruction::{AccountMeta, Instruction},
            message::Message,
            transaction::Transaction,
        };

        let admin = Signer::from_memory(&Keypair::new().to_base58_string()).unwrap();
        let operator = Signer::from_memory(&Keypair::new().to_base58_string()).unwrap();
        let instruction = Instruction::new_with_bytes(
            Pubkey::new_unique(),
            &[],
            vec![AccountMeta::new_readonly(operator.pubkey(), true)],
        );
        let mut tx = Transaction::new_unsigned(Message::new(&[instruction], Some(&admin.pubkey())));
        assert!(matches!(
            admin.sign_transaction(&mut tx).await.unwrap(),
            SignTransactionResult::Partial(_)
        ));
        assert!(matches!(
            operator.sign_transaction(&mut tx).await.unwrap(),
            SignTransactionResult::Complete(_)
        ));
        tx.verify().unwrap();
    }
}
