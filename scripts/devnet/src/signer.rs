//! Loads a signer for the admin CLIs from a keypair file, or from env vars that carry a
//! role prefix (`env:OLD_ADMIN` reads `OLD_ADMIN_SIGNER`, `OLD_ADMIN_VAULT_ADDR`, ...).
//! The var names per backend match the operators' `ADMIN_*` / `OPERATOR_*` ones.

use solana_keychain::{
    vault::reqwest::{Certificate, Client},
    HttpClientConfig, Signer, SolanaSigner, VaultSigner,
};
use solana_sdk::{pubkey::Pubkey, transaction::Transaction};

/// Where a CLI's signer comes from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SignerSource {
    KeypairFile(String),
    Env(String),
}

/// `env:<PREFIX>` selects env vars; anything else is a keypair file path.
pub fn parse_signer_arg(arg: &str) -> Result<SignerSource, String> {
    let Some(prefix) = arg.strip_prefix("env:") else {
        return Ok(SignerSource::KeypairFile(arg.to_string()));
    };
    let valid = !prefix.is_empty()
        && prefix
            .chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_');
    if !valid {
        return Err(format!(
            "signer prefix '{prefix}' must be upper-case letters, digits and _"
        ));
    }
    Ok(SignerSource::Env(prefix.to_string()))
}

/// One signer's settings, read before any key is parsed or any remote call made.
#[derive(PartialEq, Eq)]
pub enum SignerSpec {
    Memory {
        private_key: String,
    },
    Vault {
        addr: String,
        token: String,
        key_name: String,
        pubkey: String,
        /// Optional `<PREFIX>_VAULT_CA_CERT`: a PEM root trusted on top of the built-in ones.
        ca_cert: Option<String>,
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

/// Names the backend only, so a stray `{:?}` never prints a key or token.
impl std::fmt::Debug for SignerSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let backend = match self {
            Self::Memory { .. } => "memory",
            Self::Vault { .. } => "vault",
            Self::Turnkey { .. } => "turnkey",
            Self::Privy { .. } => "privy",
        };
        write!(f, "SignerSpec::{backend}(..)")
    }
}

/// Reads one env var; tests pass a map so they never touch the process env.
pub type Env<'a> = &'a dyn Fn(&str) -> Option<String>;

/// Errors name only the var, never its value. A blank value counts as unset.
fn required(env: Env, prefix: &str, suffix: &str) -> Result<String, String> {
    let name = format!("{prefix}_{suffix}");
    env(&name)
        .filter(|v| !v.trim().is_empty())
        .ok_or(format!("{name} not set"))
}

pub fn read_spec(prefix: &str, env: Env) -> Result<SignerSpec, String> {
    let backend = required(env, prefix, "SIGNER")?;
    let r = |suffix: &str| required(env, prefix, suffix);
    Ok(match backend.trim().to_lowercase().as_str() {
        "memory" => SignerSpec::Memory {
            private_key: r("PRIVATE_KEY")?,
        },
        "vault" => {
            let addr = r("VAULT_ADDR")?.trim().to_string();
            // The keychain's Vault client is https-only; say so before any call.
            if !addr.starts_with("https://") {
                return Err(format!("{prefix}_VAULT_ADDR must be an https:// URL"));
            }
            SignerSpec::Vault {
                addr,
                token: r("VAULT_TOKEN")?,
                key_name: r("VAULT_KEY_NAME")?,
                pubkey: r("VAULT_PUBKEY")?,
                ca_cert: r("VAULT_CA_CERT").ok(),
            }
        }
        "turnkey" => SignerSpec::Turnkey {
            api_public_key: r("TURNKEY_API_PUBLIC_KEY")?,
            api_private_key: r("TURNKEY_API_PRIVATE_KEY")?,
            organization_id: r("TURNKEY_ORGANIZATION_ID")?,
            private_key_id: r("TURNKEY_PRIVATE_KEY_ID")?,
            public_key: r("TURNKEY_PUBKEY")?,
        },
        "privy" => SignerSpec::Privy {
            app_id: r("PRIVY_APP_ID")?,
            app_secret: r("PRIVY_APP_SECRET")?,
            wallet_id: r("PRIVY_WALLET_ID")?,
        },
        other => {
            return Err(format!(
                "{prefix}_SIGNER={other} is not supported: memory, vault, turnkey or privy"
            ))
        }
    })
}

async fn build(spec: SignerSpec) -> Result<Signer, String> {
    let signer = match spec {
        SignerSpec::Memory { private_key } => Signer::from_memory(&private_key),
        SignerSpec::Vault {
            addr,
            token,
            key_name,
            pubkey,
            ca_cert: None,
        } => Signer::from_vault(addr, token, key_name, pubkey, None),
        SignerSpec::Vault {
            addr,
            token,
            key_name,
            pubkey,
            ca_cert: Some(path),
        } => {
            let client = vault_client_with_ca(&path)?;
            VaultSigner::with_client(client, addr, token, key_name, pubkey).map(Signer::Vault)
        }
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
        ),
        SignerSpec::Privy {
            app_id,
            app_secret,
            wallet_id,
        } => Signer::from_privy(app_id, app_secret, wallet_id, None).await,
    };
    signer.map_err(|e| e.to_string())
}

/// The keychain's own Vault client, plus one extra root (e.g. a private CA).
fn vault_client_with_ca(path: &str) -> Result<Client, String> {
    let pem = std::fs::read(path).map_err(|e| format!("VAULT_CA_CERT {path}: {e}"))?;
    let cert = Certificate::from_pem(&pem).map_err(|e| format!("VAULT_CA_CERT {path}: {e}"))?;
    let timeouts = HttpClientConfig::default();
    Client::builder()
        .timeout(timeouts.resolved_request_timeout())
        .connect_timeout(timeouts.resolved_connect_timeout())
        .https_only(true)
        .add_root_certificate(cert)
        .build()
        .map_err(|e| format!("Vault HTTP client: {e}"))
}

pub async fn load_signer(source: &SignerSource) -> Result<Signer, String> {
    match source {
        SignerSource::KeypairFile(path) => {
            Signer::from_memory_file(path).map_err(|e| format!("keypair {path}: {e}"))
        }
        SignerSource::Env(prefix) => {
            let spec = read_spec(prefix, &|name| std::env::var(name).ok())?;
            build(spec)
                .await
                .map_err(|e| format!("{prefix} signer: {e}"))
        }
    }
}

pub fn signer_pubkey(signer: &Signer) -> Pubkey {
    SolanaSigner::pubkey(signer)
}

/// Signs `tx` in place; the signer must be one of its required signers.
pub async fn sign(signer: &Signer, tx: &mut Transaction) -> Result<(), String> {
    signer
        .sign_transaction(tx)
        .await
        .map(|_| ())
        .map_err(|e| format!("signing with {}: {e}", signer_pubkey(signer)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn env_of(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn read(prefix: &str, vars: &HashMap<String, String>) -> Result<SignerSpec, String> {
        read_spec(prefix, &|n| vars.get(n).cloned())
    }

    const VAULT: [(&str, &str); 5] = [
        ("OLD_ADMIN_SIGNER", "vault"),
        ("OLD_ADMIN_VAULT_ADDR", "https://vault.invalid:8200"),
        ("OLD_ADMIN_VAULT_TOKEN", "SNTLtoken"),
        ("OLD_ADMIN_VAULT_KEY_NAME", "k"),
        ("OLD_ADMIN_VAULT_PUBKEY", "P"),
    ];
    const TURNKEY: [(&str, &str); 6] = [
        ("OLD_ADMIN_SIGNER", "turnkey"),
        ("OLD_ADMIN_TURNKEY_API_PUBLIC_KEY", "a"),
        ("OLD_ADMIN_TURNKEY_API_PRIVATE_KEY", "SNTLtk"),
        ("OLD_ADMIN_TURNKEY_ORGANIZATION_ID", "o"),
        ("OLD_ADMIN_TURNKEY_PRIVATE_KEY_ID", "i"),
        ("OLD_ADMIN_TURNKEY_PUBKEY", "P"),
    ];
    const PRIVY: [(&str, &str); 4] = [
        ("OLD_ADMIN_SIGNER", "privy"),
        ("OLD_ADMIN_PRIVY_APP_ID", "a"),
        ("OLD_ADMIN_PRIVY_APP_SECRET", "SNTLps"),
        ("OLD_ADMIN_PRIVY_WALLET_ID", "w"),
    ];
    const MEMORY: [(&str, &str); 2] = [
        ("OLD_ADMIN_SIGNER", "memory"),
        ("OLD_ADMIN_PRIVATE_KEY", "SNTLkey"),
    ];

    /// Each backend loads with its full set and names every var missing or blank.
    #[test]
    fn every_backend_requires_each_of_its_vars() {
        for set in [&VAULT[..], &TURNKEY[..], &PRIVY[..], &MEMORY[..]] {
            assert!(read("OLD_ADMIN", &env_of(set)).is_ok(), "{set:?}");
            for (name, _) in set {
                let mut vars = env_of(set);
                vars.remove(*name);
                let err = read("OLD_ADMIN", &vars).unwrap_err();
                assert_eq!(err, format!("{name} not set"));
                vars.insert(name.to_string(), "  ".into());
                assert_eq!(read("OLD_ADMIN", &vars).unwrap_err(), err);
            }
        }
    }

    #[test]
    fn reads_the_values_under_the_given_prefix_only() {
        let spec = read("OLD_ADMIN", &env_of(&VAULT)).unwrap();
        assert_eq!(
            spec,
            SignerSpec::Vault {
                addr: "https://vault.invalid:8200".into(),
                token: "SNTLtoken".into(),
                key_name: "k".into(),
                pubkey: "P".into(),
                ca_cert: None,
            }
        );
        // The operators' own vars never stand in for the prefixed ones.
        let ops = env_of(&[("ADMIN_SIGNER", "vault"), ("ADMIN_VAULT_ADDR", "https://x")]);
        assert_eq!(
            read("OLD_ADMIN", &ops).unwrap_err(),
            "OLD_ADMIN_SIGNER not set"
        );
    }

    #[test]
    fn refuses_an_unknown_backend_and_plain_http_vault() {
        let mut vars = env_of(&VAULT);
        vars.insert("OLD_ADMIN_SIGNER".into(), "kms".into());
        assert!(read("OLD_ADMIN", &vars)
            .unwrap_err()
            .contains("memory, vault, turnkey or privy"));
        let mut vars = env_of(&VAULT);
        vars.insert("OLD_ADMIN_VAULT_ADDR".into(), "http://vault.invalid".into());
        assert!(read("OLD_ADMIN", &vars).unwrap_err().contains("https://"));
    }

    #[tokio::test]
    async fn an_unreadable_vault_ca_is_refused_by_name() {
        let mut vars = env_of(&VAULT);
        vars.insert(
            "OLD_ADMIN_VAULT_CA_CERT".into(),
            "/nonexistent/ca.pem".into(),
        );
        let spec = read("OLD_ADMIN", &vars).unwrap();
        let Err(err) = build(spec).await else {
            panic!("an unreadable CA must be refused")
        };
        assert!(err.contains("VAULT_CA_CERT /nonexistent/ca.pem"), "{err}");
    }

    #[test]
    fn errors_never_carry_a_value() {
        for set in [&VAULT[..], &TURNKEY[..], &PRIVY[..], &MEMORY[..]] {
            for (name, _) in set.iter().skip(1) {
                let mut vars = env_of(set);
                vars.remove(*name);
                assert!(!read("OLD_ADMIN", &vars).unwrap_err().contains("SNTL"));
            }
        }
    }

    #[test]
    fn parses_the_signer_argument() {
        assert_eq!(
            parse_signer_arg("./keys/admin.json").unwrap(),
            SignerSource::KeypairFile("./keys/admin.json".into())
        );
        assert_eq!(
            parse_signer_arg("env:OLD_ADMIN").unwrap(),
            SignerSource::Env("OLD_ADMIN".into())
        );
        for bad in ["env:", "env:old_admin", "env:OLD-ADMIN", "env:OLD ADMIN"] {
            assert!(parse_signer_arg(bad).is_err(), "{bad}");
        }
    }

    #[tokio::test]
    async fn a_keypair_file_and_a_memory_env_load_the_same_key() {
        use solana_sdk::signature::{write_keypair_file, Keypair, Signer as _};
        let kp = Keypair::new();
        let dir = std::env::temp_dir().join(format!("signer-test-{}", kp.pubkey()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("k.json");
        write_keypair_file(&kp, &path).unwrap();
        let file = load_signer(&SignerSource::KeypairFile(path.to_str().unwrap().into()))
            .await
            .unwrap();
        assert_eq!(signer_pubkey(&file), kp.pubkey());
        let spec = SignerSpec::Memory {
            private_key: kp.to_base58_string(),
        };
        assert_eq!(signer_pubkey(&build(spec).await.unwrap()), kp.pubkey());

        let mut tx = Transaction::new_with_payer(
            &[solana_system_interface::instruction::transfer(
                &kp.pubkey(),
                &Pubkey::new_unique(),
                1,
            )],
            Some(&kp.pubkey()),
        );
        sign(&file, &mut tx).await.unwrap();
        assert!(tx.is_signed());
        std::fs::remove_dir_all(dir).unwrap();
    }
}
