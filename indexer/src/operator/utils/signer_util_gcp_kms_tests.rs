use super::*;
use base64::{engine::general_purpose::STANDARD, Engine};
use google_cloud_kms_v1::client::KeyManagementService;
use serial_test::serial;
use solana_keychain::GcpKmsSigner;
use solana_sdk::signature::{Keypair, Signer as _};
use std::ffi::OsString;

const KEY_NAME: &str =
    "projects/test/locations/us-central1/keyRings/channel/cryptoKeys/operator/cryptoKeyVersions/1";
const PUBLIC_KEY: &str = "11111111111111111111111111111111";

struct TestEnv(Vec<(&'static str, Option<OsString>)>);

impl TestEnv {
    fn new(values: &[(&'static str, Option<&str>)]) -> Self {
        let original = values
            .iter()
            .map(|(key, _)| (*key, env::var_os(key)))
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
fn signer_types_preserve_existing_backends_and_accept_kms() {
    for (name, expected) in [
        ("memory", SignerType::Memory),
        ("vault", SignerType::Vault),
        ("turnkey", SignerType::Turnkey),
        ("privy", SignerType::Privy),
        ("gcp_kms", SignerType::GcpKms),
        ("GCP_KMS", SignerType::GcpKms),
    ] {
        assert_eq!(SignerType::from_str(name).unwrap(), expected);
    }
}

#[test]
#[serial]
fn kms_configuration_is_role_scoped_and_validated_before_initialization() {
    for (role, signer_var, key_var, pubkey_var) in [
        (
            SignerRole::Admin,
            ADMIN_SIGNER,
            ADMIN_GCP_KMS_KEY_NAME,
            ADMIN_GCP_KMS_PUBLIC_KEY,
        ),
        (
            SignerRole::Operator,
            OPERATOR_SIGNER,
            OPERATOR_GCP_KMS_KEY_NAME,
            OPERATOR_GCP_KMS_PUBLIC_KEY,
        ),
    ] {
        let _env = TestEnv::new(&[
            (signer_var, Some("gcp_kms")),
            (key_var, Some(KEY_NAME)),
            (pubkey_var, Some(PUBLIC_KEY)),
        ]);
        let config = gcp_kms_config(role).unwrap();
        assert_eq!(config.key_name, KEY_NAME);
        assert_eq!(config.public_key, PUBLIC_KEY);

        let unversioned = KEY_NAME.split("/cryptoKeyVersions/").next().unwrap();
        let alias = format!("{unversioned}/cryptoKeyVersions/latest");
        let zero_version = format!("{unversioned}/cryptoKeyVersions/0");
        for bad_key in [
            None,
            Some(""),
            Some(" "),
            Some(unversioned),
            Some(&alias),
            Some(&zero_version),
        ] {
            let _invalid = TestEnv::new(&[(key_var, bad_key)]);
            let error = load_signer(role).err().expect("invalid KMS key must fail");
            assert!(error.to_string().contains(key_var));
        }
        for bad_pubkey in [None, Some(""), Some(" "), Some("not-a-public-key")] {
            let _invalid = TestEnv::new(&[(pubkey_var, bad_pubkey)]);
            let error = load_signer(role)
                .err()
                .expect("invalid public key must fail");
            assert!(error.to_string().contains(pubkey_var));
            assert!(!error.to_string().contains("not-a-public-key"));
        }
    }
}

#[test]
#[serial]
fn only_an_absent_operator_signer_falls_back() {
    let _env = TestEnv::new(&[(OPERATOR_SIGNER, None)]);
    assert!(load_operator_signer().unwrap().is_none());
    for signer_type in ["", "unknown", "gcp_kms", "memory"] {
        let _invalid = TestEnv::new(&[
            (OPERATOR_SIGNER, Some(signer_type)),
            (OPERATOR_GCP_KMS_KEY_NAME, None),
            (OPERATOR_PRIVATE_KEY, None),
        ]);
        assert!(load_operator_signer().is_err());
    }

    let keypair = Keypair::new();
    let encoded_key = keypair.to_base58_string();
    let _valid = TestEnv::new(&[
        (OPERATOR_SIGNER, Some("memory")),
        (OPERATOR_PRIVATE_KEY, Some(&encoded_key)),
    ]);
    assert_eq!(
        load_operator_signer().unwrap().unwrap().pubkey(),
        keypair.pubkey()
    );
}

#[test]
#[serial]
fn backend_configuration_errors_remain_redacted() {
    let invalid_secret = "invalid-private-key-do-not-log";
    let _env = TestEnv::new(&[
        (OPERATOR_SIGNER, Some("memory")),
        (OPERATOR_PRIVATE_KEY, Some(invalid_secret)),
    ]);
    let error = load_operator_signer().err().unwrap();
    assert!(matches!(error, SignerConfigError::Signer(_)));
    assert!(!format!("{error} {error:?}").contains(invalid_secret));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_initialization_can_yield_and_propagates_errors() {
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

// Exercise the same Keychain signer and synchronous initialization bridge used
// by the operator. All requests go to localhost with anonymous test credentials;
// no GCP account, private key file, or live transaction is needed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kms_signs_transactions_and_rejects_permission_or_signature_errors() {
    let mut server = mockito::Server::new_async().await;
    let keypair = Keypair::new();
    let signer = initialize_remote_signer(async {
        let client = KeyManagementService::builder()
            .with_endpoint(server.url())
            .with_credentials(google_cloud_auth::credentials::anonymous::Builder::new().build())
            .build()
            .await
            .unwrap();
        Ok(Signer::GcpKms(GcpKmsSigner::with_client(
            client,
            KEY_NAME.into(),
            keypair.pubkey().to_string(),
        )?))
    })
    .unwrap();
    let operator_key = Keypair::new();
    let operator = Signer::from_memory(&operator_key.to_base58_string()).unwrap();
    let instruction = solana_sdk::instruction::Instruction::new_with_bytes(
        Pubkey::new_unique(),
        &[],
        vec![solana_sdk::instruction::AccountMeta::new_readonly(
            operator.pubkey(),
            true,
        )],
    );
    let mut tx = solana_sdk::transaction::Transaction::new_unsigned(
        solana_sdk::message::Message::new(&[instruction], Some(&signer.pubkey())),
    );
    let signature = keypair.sign_message(&tx.message_data());
    let path = format!("/v1/{KEY_NAME}:asymmetricSign");
    let response = server
        .mock("POST", path.as_str())
        .match_query(mockito::Matcher::Any)
        .match_body(mockito::Matcher::PartialJson(serde_json::json!({
            "data": STANDARD.encode(tx.message_data()),
        })))
        .with_header("content-type", "application/json")
        .with_body(
            serde_json::json!({"signature": STANDARD.encode(signature.as_ref())}).to_string(),
        )
        .create_async()
        .await;
    let result = signer.sign_transaction(&mut tx).await;
    response.assert_async().await;
    assert!(matches!(
        result.unwrap(),
        solana_keychain::SignTransactionResult::Partial(_)
    ));
    assert!(matches!(
        operator.sign_transaction(&mut tx).await.unwrap(),
        solana_keychain::SignTransactionResult::Complete(_)
    ));
    tx.verify().unwrap();
    response.assert_async().await;
    response.remove_async().await;

    let denied = server
        .mock("POST", path.as_str())
        .match_query(mockito::Matcher::Any)
        .with_status(403)
        .with_header("content-type", "application/json")
        .with_body(
            r#"{"error":{"code":403,"message":"Permission denied","status":"PERMISSION_DENIED"}}"#,
        )
        .create_async()
        .await;
    assert!(matches!(
        signer.sign_transaction(&mut tx).await,
        Err(SignerError::RemoteApiError(_))
    ));
    denied.assert_async().await;
    denied.remove_async().await;

    let wrong_key = Keypair::new().sign_message(&tx.message_data());
    let invalid = server
        .mock("POST", path.as_str())
        .match_query(mockito::Matcher::Any)
        .with_header("content-type", "application/json")
        .with_body(
            serde_json::json!({"signature": STANDARD.encode(wrong_key.as_ref())}).to_string(),
        )
        .create_async()
        .await;
    assert!(matches!(
        signer.sign_transaction(&mut tx).await,
        Err(SignerError::SigningFailed(_))
    ));
    invalid.assert_async().await;
}
