//! Shared pieces of the confidential devnet tools: key files, account checks and the
//! channel's confidential withdraw instruction.

use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use private_channel_confidential::Keys;
use private_channel_escrow_program_client::{
    instructions::ConfigureConfidentialAccountBuilder, PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
};
use private_channel_withdraw_program_client::instructions::{
    ConfidentialWithdrawFunds, ConfidentialWithdrawFundsInstructionArgs, WithdrawFunds,
    WithdrawFundsInstructionArgs,
};
use solana_client::rpc_client::RpcClient;
use solana_sdk::{
    instruction::Instruction,
    message::{v1, VersionedMessage},
    pubkey::Pubkey,
    signature::{Keypair, Signature},
    signer::Signer,
    transaction::{Transaction, VersionedTransaction},
};
use solana_zk_elgamal_proof_interface::instruction::ProofInstruction;
use solana_zk_sdk::encryption::{
    auth_encryption::{AeCiphertext, AeKey},
    elgamal::{ElGamalKeypair, ElGamalSecretKey},
};
use solana_zk_sdk::zk_elgamal_proof_program::build_pubkey_validity_proof_data;
use spl_associated_token_account::get_associated_token_address_with_program_id;
use spl_token_2022_interface::{
    extension::{
        confidential_transfer::ConfidentialTransferAccount, BaseStateWithExtensions,
        StateWithExtensions,
    },
    state::{Account, AccountState},
};
use std::{error::Error, path::Path};
use zeroize::Zeroizing;

type Result<T> = std::result::Result<T, Box<dyn Error>>;

/// Largest amount one confidential release can carry (48 bits).
pub const MAX_CONFIDENTIAL_AMOUNT: u64 = private_channel_confidential::MAX_CONFIDENTIAL_AMOUNT;

/// An ElGamal key pair and AES key, as the Token-2022 proof builders take them.
pub struct ConfidentialKeyPair {
    pub elgamal: ElGamalKeypair,
    pub aes: AeKey,
    /// The same keys through the shared crate, for its decryption helpers.
    pub keys: Keys,
}

/// Fresh random keys as the JSON `ESCROW_CONFIDENTIAL_KEYS` holds.
pub fn generate_keys_json() -> Zeroizing<String> {
    let secret = ElGamalSecretKey::new_rand();
    let aes = AeKey::new_rand();
    let secret_bytes: Zeroizing<[u8; 32]> = Zeroizing::new((&secret).into());
    let aes_bytes: Zeroizing<[u8; 16]> = Zeroizing::new((&aes).into());
    Keys::from_bytes(&*secret_bytes, &*aes_bytes)
        .expect("fresh keys have valid lengths")
        .to_json()
}

/// `ESCROW_CONFIDENTIAL_PUBKEY=<base64>`, the line to set for the instance's keys.
pub fn pinned_pubkey_line(keys: &Keys) -> String {
    format!(
        "{}={}",
        private_channel_confidential::PUBKEY_ENV,
        BASE64.encode(keys.pubkey())
    )
}

/// Parse key JSON. Errors never include the value.
pub fn parse_keys(json: &str) -> Result<ConfidentialKeyPair> {
    let keys = Keys::from_json(json.trim()).map_err(|e| e.to_string())?;
    let parsed: serde_json::Value =
        serde_json::from_str(json.trim()).map_err(|_| "confidential keys are not JSON")?;
    let field = |name: &str| -> Result<Zeroizing<Vec<u8>>> {
        let text = parsed[name]
            .as_str()
            .ok_or("confidential keys field missing")?;
        Ok(Zeroizing::new(
            BASE64
                .decode(text)
                .map_err(|_| "confidential keys field is not base64")?,
        ))
    };
    let secret = ElGamalSecretKey::try_from(field("elgamal_secret")?.as_slice())
        .map_err(|_| "elgamal_secret is not a valid secret key")?;
    let aes = AeKey::try_from(field("aes_key")?.as_slice()).map_err(|_| "aes_key is invalid")?;
    let elgamal = ElGamalKeypair::new(secret);
    if elgamal.pubkey().to_bytes() != keys.pubkey() {
        return Err("confidential keys disagree with the shared loader".into());
    }
    Ok(ConfidentialKeyPair { elgamal, aes, keys })
}

/// Read keys from a file written by `confidential_keygen`.
pub fn read_keys_file(path: &Path) -> Result<ConfidentialKeyPair> {
    let json = Zeroizing::new(
        std::fs::read_to_string(path)
            .map_err(|e| format!("could not read keys file {}: {e}", path.display()))?,
    );
    parse_keys(&json).map_err(|e| format!("keys file {}: {e}", path.display()).into())
}

/// Write key JSON readable by the owner only. Refuses to overwrite.
pub fn write_keys_file(path: &Path, json: &str) -> Result<()> {
    use std::io::Write;
    #[cfg(unix)]
    use std::os::unix::fs::OpenOptionsExt;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);
    let mut file = options.open(path).map_err(|e| {
        format!(
            "could not create {} ({e}); it must not exist",
            path.display()
        )
    })?;
    file.write_all(json.as_bytes())?;
    file.write_all(b"\n")?;
    Ok(())
}

/// The confidential transfer state of a Token-2022 account, or `None` when not configured.
pub fn confidential_account(
    data: &[u8],
) -> Result<Option<(AccountState, ConfidentialTransferAccount)>> {
    let state = StateWithExtensions::<Account>::unpack(data)
        .map_err(|e| format!("not a Token-2022 account: {e}"))?;
    Ok(state
        .get_extension::<ConfidentialTransferAccount>()
        .ok()
        .map(|extension| (state.base.state, *extension)))
}

/// Check a keys file against the instance's pinned pubkey, the base64 alone or the whole
/// `ESCROW_CONFIDENTIAL_PUBKEY=` line `confidential_keygen` printed. Proves the ElGamal half only.
pub fn verify_against_pin(keys: &ConfidentialKeyPair, pin: &str) -> Result<()> {
    let pin = pin.trim();
    let value = pin
        .strip_prefix(private_channel_confidential::PUBKEY_ENV)
        .and_then(|rest| rest.strip_prefix('='))
        .unwrap_or(pin);
    let pinned = private_channel_confidential::parse_pubkey(value).map_err(|e| e.to_string())?;
    if pinned != keys.keys.pubkey() {
        return Err(format!(
            "the file's ElGamal pubkey {} does not match the pin {}",
            BASE64.encode(keys.keys.pubkey()),
            BASE64.encode(pinned)
        )
        .into());
    }
    Ok(())
}

/// Check a keys file against an escrow account's data, as `escrow_account_data` wrote it, so a
/// backup never meets a network. The data must be `instance`'s escrow account for `mint`, its
/// ElGamal pubkey the file's, and the file's AES key must decrypt its AES copy: both halves.
/// The decrypted balance is confidential, so it is dropped here.
pub fn verify_against_account(
    keys: &ConfidentialKeyPair,
    instance: &Pubkey,
    mint: &Pubkey,
    data: &[u8],
) -> Result<()> {
    let escrow_ata =
        get_associated_token_address_with_program_id(instance, mint, &spl_token_2022::ID);
    let account = StateWithExtensions::<Account>::unpack(data)
        .map_err(|e| format!("the account data is not a Token-2022 account: {e}"))?;
    if account.base.owner != *instance || account.base.mint != *mint {
        return Err(format!(
            "the account data is not the escrow account {escrow_ata} of mint {mint}"
        )
        .into());
    }
    let Some((_, extension)) = confidential_account(data)? else {
        return Err(format!(
            "escrow account {escrow_ata} is not configured for confidential transfers"
        )
        .into());
    };
    if extension.elgamal_pubkey.0 != keys.keys.pubkey() {
        return Err(format!(
            "escrow account {escrow_ata} is configured under another key ({}); this file is not \
             the instance key",
            BASE64.encode(extension.elgamal_pubkey.0)
        )
        .into());
    }
    decrypt_available(keys, &extension).map_err(|_| {
        format!("the file's aes_key does not decrypt escrow account {escrow_ata}'s AES copy")
    })?;
    Ok(())
}

/// Why the operator would park a confidential release to this account, if it would.
pub fn destination_not_ready(data: Option<&[u8]>) -> Option<String> {
    let Some(data) = data else {
        return Some("the destination token account does not exist".into());
    };
    let (state, extension) = match confidential_account(data) {
        Ok(Some(found)) => found,
        Ok(None) => {
            return Some("the destination is not configured for confidential transfers".into())
        }
        Err(e) => return Some(e.to_string()),
    };
    if state == AccountState::Frozen {
        return Some("the destination account is frozen".into());
    }
    if !bool::from(extension.approved) {
        return Some("the destination is not approved by the mint's authority".into());
    }
    if !bool::from(extension.allow_confidential_credits) {
        return Some("the destination does not allow confidential credits".into());
    }
    let counter = u64::from(extension.pending_balance_credit_counter);
    if counter >= u64::from(extension.maximum_pending_balance_credit_counter) {
        return Some(
            "the destination's pending credit counter is full; apply its pending balance".into(),
        );
    }
    None
}

/// What a deposit of `amount` needs first so that much sits in the available balance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TopUp {
    /// Public tokens to move into the pending balance.
    pub from_public: u64,
    /// Whether to apply the pending balance.
    pub apply: bool,
}

/// Plan the top up from the user's available, pending and public balances.
pub fn plan_top_up(available: u64, pending: u64, public: u64, amount: u64) -> Result<TopUp> {
    if available >= amount {
        return Ok(TopUp {
            from_public: 0,
            apply: false,
        });
    }
    let short = amount - available;
    let from_public = short.saturating_sub(pending);
    if from_public > public {
        return Err(format!(
            "need {amount}: available {available}, pending {pending}, public {public}"
        )
        .into());
    }
    Ok(TopUp {
        from_public,
        apply: true,
    })
}

/// The user's available balance from its AES copy.
pub fn decrypt_available(
    keys: &ConfidentialKeyPair,
    extension: &ConfidentialTransferAccount,
) -> Result<u64> {
    let copy = AeCiphertext::try_from(extension.decryptable_available_balance)
        .map_err(|_| "decryptable available balance is malformed")?;
    keys.aes
        .decrypt(&copy)
        .ok_or_else(|| "the AES copy does not decrypt under these keys".into())
}

/// The user's pending balance.
pub fn decrypt_pending(
    keys: &ConfidentialKeyPair,
    extension: &ConfidentialTransferAccount,
) -> Result<u64> {
    keys.keys
        .decrypt_pending(
            &extension.pending_balance_lo.0,
            &extension.pending_balance_hi.0,
        )
        .map_err(|e| e.to_string().into())
}

/// `ConfidentialWithdrawFunds` from the same accounts and args as `WithdrawFunds`.
pub fn confidential_withdraw_instruction(
    accounts: WithdrawFunds,
    args: WithdrawFundsInstructionArgs,
) -> Result<Instruction> {
    if args.amount > MAX_CONFIDENTIAL_AMOUNT {
        return Err(format!(
            "amount {} is above the confidential maximum {MAX_CONFIDENTIAL_AMOUNT}",
            args.amount
        )
        .into());
    }
    let accounts = ConfidentialWithdrawFunds {
        user: accounts.user,
        mint: accounts.mint,
        token_account: accounts.token_account,
        token_program: accounts.token_program,
        associated_token_program: accounts.associated_token_program,
        withdraw_config: accounts.withdraw_config,
        treasury_token_account: accounts.treasury_token_account,
    };
    Ok(
        accounts.instruction(ConfidentialWithdrawFundsInstructionArgs {
            amount: args.amount,
            destination: args.destination,
        }),
    )
}

/// Sign and send a v1 transaction, so inline proofs fit in 4,096 bytes.
pub fn send_v1(
    client: &RpcClient,
    payer: &Keypair,
    instructions: &[Instruction],
    signers: &[&Keypair],
) -> Result<Signature> {
    // Token-2022 alone loads about 1.3 MB; v1 reads a missing limit as zero.
    let config = v1::TransactionConfig::empty()
        .with_compute_unit_limit(1_400_000)
        .with_loaded_accounts_data_size_limit(4_000_000);
    let message = v1::Message::try_compile_with_config(
        &payer.pubkey(),
        instructions,
        client.get_latest_blockhash()?,
        config,
    )?;
    let mut all: Vec<&Keypair> = vec![payer];
    all.extend(signers.iter().filter(|s| s.pubkey() != payer.pubkey()));
    let transaction = VersionedTransaction::try_new(VersionedMessage::V1(message), &all)?;
    let size = transaction.message.serialize().len() + 64 * transaction.signatures.len();
    if size > v1::MAX_TRANSACTION_SIZE {
        return Err(format!(
            "transaction is {size} bytes, over {}",
            v1::MAX_TRANSACTION_SIZE
        )
        .into());
    }
    Ok(client.send_and_confirm_transaction(&transaction)?)
}

/// Whether `configure_escrow` sent a transaction or found the account already configured.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Configured {
    Sent(Signature),
    Already,
}

/// The INFO line indexer-solana writes once it indexed a configure under the instance key.
pub fn indexer_configure_log(mint: &Pubkey, pubkey: &str) -> String {
    format!("escrow account configured under the instance key mint={mint} elgamal_pubkey={pubkey}")
}

/// An existing configuration of the escrow account: `Already` under `pubkey`, an error under
/// any other key, `None` when the account is not configured.
pub fn existing_configuration(
    escrow_ata: &Pubkey,
    data: &[u8],
    pubkey: &str,
) -> Result<Option<Configured>> {
    let Some((_, extension)) = confidential_account(data)? else {
        return Ok(None);
    };
    let configured =
        solana_zk_sdk::encryption::elgamal::ElGamalPubkey::try_from(extension.elgamal_pubkey)
            .map(|key| BASE64.encode(key.to_bytes()))
            .unwrap_or_else(|_| "an invalid key".into());
    if configured == pubkey {
        return Ok(Some(Configured::Already));
    }
    Err(format!(
        "escrow account {escrow_ata} is already configured under another key ({configured})"
    )
    .into())
}

/// Configure the escrow account of `mint` for confidential transfers under `keys`
/// (`ConfigureConfidentialAccount`, signed by the instance admin). Refuses an account
/// already configured under another key.
pub fn configure_escrow(
    client: &RpcClient,
    admin: &Keypair,
    instance: &Pubkey,
    mint: &Pubkey,
    keys: &ConfidentialKeyPair,
) -> Result<Configured> {
    let pubkey = BASE64.encode(keys.keys.pubkey());
    let token_program = client.get_account(mint)?.owner;
    if token_program != spl_token_2022::ID {
        return Err(format!("mint {mint} is not a Token-2022 mint").into());
    }
    let escrow_ata = get_associated_token_address_with_program_id(instance, mint, &token_program);
    if let Ok(data) = client.get_account_data(&escrow_ata) {
        if let Some(existing) = existing_configuration(&escrow_ata, &data, &pubkey)? {
            return Ok(existing);
        }
    }

    let configure = ConfigureConfidentialAccountBuilder::new()
        .payer(admin.pubkey())
        .admin(admin.pubkey())
        .instance(*instance)
        .mint(*mint)
        .allowed_mint(pda::allowed_mint(instance, mint))
        .instance_ata(escrow_ata)
        .event_authority(pda::event_authority())
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .decryptable_zero_balance(keys.aes.encrypt(0).to_bytes())
        // The proof is the next instruction in this transaction.
        .proof_instruction_offset(1)
        .instruction();
    let proof = ProofInstruction::VerifyPubkeyValidity
        .encode_verify_proof(None, &build_pubkey_validity_proof_data(&keys.elgamal)?);

    let transaction = Transaction::new_signed_with_payer(
        &[configure, proof],
        Some(&admin.pubkey()),
        &[admin],
        client.get_latest_blockhash()?,
    );
    Ok(Configured::Sent(
        client.send_and_confirm_transaction(&transaction)?,
    ))
}

/// The escrow PDAs the confidential instructions take.
pub mod pda {
    use private_channel_escrow_program_client::PRIVATE_CHANNEL_ESCROW_PROGRAM_ID;
    use solana_sdk::pubkey::Pubkey;

    pub fn allowed_mint(instance: &Pubkey, mint: &Pubkey) -> Pubkey {
        find(&[b"allowed_mint", instance.as_ref(), mint.as_ref()])
    }

    pub fn allowed_wallet(instance: &Pubkey, wallet: &Pubkey) -> Pubkey {
        find(&[b"allowed_wallet", instance.as_ref(), wallet.as_ref()])
    }

    pub fn event_authority() -> Pubkey {
        find(&[b"event_authority"])
    }

    fn find(seeds: &[&[u8]]) -> Pubkey {
        Pubkey::find_program_address(seeds, &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID).0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use private_channel_confidential::test_support;
    use solana_sdk::pubkey::Pubkey;
    use spl_token_2022_interface::extension::confidential_transfer::ConfidentialTransferAccount;

    #[test]
    fn generated_keys_load_through_the_shared_crate() {
        let json = generate_keys_json();
        let shared = Keys::from_json(&json).expect("shared loader reads keygen output");
        let parsed = parse_keys(&json).expect("tools read keygen output");
        assert_eq!(parsed.elgamal.pubkey().to_bytes(), shared.pubkey());
        assert_ne!(*generate_keys_json(), *json, "each run makes new keys");
    }

    #[test]
    fn the_printed_pubkey_line_is_what_the_services_load() {
        let keys = test_support::keys();
        let line = pinned_pubkey_line(&keys);
        let (name, value) = line.split_once('=').unwrap();
        assert_eq!(name, private_channel_confidential::PUBKEY_ENV);
        assert_eq!(
            private_channel_confidential::parse_pubkey(value).unwrap(),
            keys.pubkey()
        );
    }

    #[test]
    fn parse_keys_matches_the_fixed_test_keys() {
        let parsed = parse_keys(&test_support::keys_json()).unwrap();
        assert_eq!(
            parsed.elgamal.pubkey().to_bytes(),
            test_support::keys().pubkey()
        );
        let ct = test_support::encrypt_aes(&test_support::keys(), 42);
        assert_eq!(
            parsed.aes.decrypt(&AeCiphertext::from_bytes(&ct).unwrap()),
            Some(42)
        );
    }

    #[test]
    fn parse_keys_errors_never_echo_the_value() {
        for bad in [
            "not json at all SECRETVALUE",
            r#"{"elgamal_secret":"SECRETVALUE","aes_key":"AAAAAAAAAAAAAAAAAAAAAA=="}"#,
            r#"{"elgamal_secret":"AAAA"}"#,
        ] {
            let err = parse_keys(bad).err().expect("refused").to_string();
            assert!(!err.contains("SECRETVALUE"), "{err}");
        }
    }

    #[test]
    fn keys_file_is_private_and_never_overwritten() {
        let dir = std::env::temp_dir().join(format!("ckeys-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("keys.json");
        let _ = std::fs::remove_file(&path);
        write_keys_file(&path, &test_support::keys_json()).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600);
        }
        assert!(write_keys_file(&path, "{}").is_err());
        let read = read_keys_file(&path).unwrap();
        assert_eq!(read.keys.pubkey(), test_support::keys().pubkey());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn top_up_uses_available_then_pending_then_public() {
        assert_eq!(
            plan_top_up(100, 0, 0, 100).unwrap(),
            TopUp {
                from_public: 0,
                apply: false
            }
        );
        assert_eq!(
            plan_top_up(40, 70, 0, 100).unwrap(),
            TopUp {
                from_public: 0,
                apply: true
            }
        );
        assert_eq!(
            plan_top_up(40, 10, 50, 100).unwrap(),
            TopUp {
                from_public: 50,
                apply: true
            }
        );
        assert!(plan_top_up(40, 10, 49, 100).is_err());
    }

    #[test]
    fn configure_refuses_an_account_under_another_key() {
        let ata = Pubkey::new_unique();
        let ours = BASE64.encode(test_support::keys().pubkey());
        let mut extension = ready_extension();
        extension.elgamal_pubkey = test_support::keys().pubkey().into();
        let under_ours = account_bytes(AccountState::Initialized, Some(extension));
        assert_eq!(
            existing_configuration(&ata, &under_ours, &ours).unwrap(),
            Some(Configured::Already)
        );
        extension.elgamal_pubkey = test_support::other_keys().pubkey().into();
        let foreign = account_bytes(AccountState::Initialized, Some(extension));
        let err = existing_configuration(&ata, &foreign, &ours).unwrap_err();
        assert!(err
            .to_string()
            .contains("already configured under another key"));
        let plain = account_bytes(AccountState::Initialized, None);
        assert_eq!(existing_configuration(&ata, &plain, &ours).unwrap(), None);
    }

    #[test]
    fn verify_against_pin_accepts_only_the_files_pubkey() {
        let keys = parse_keys(&test_support::keys_json()).unwrap();
        let pin_line = pinned_pubkey_line(&keys.keys);
        let (_, pin) = pin_line.split_once('=').unwrap();
        verify_against_pin(&keys, pin).unwrap();
        verify_against_pin(&keys, &pin_line).unwrap();
        let other_pin = BASE64.encode(test_support::other_keys().pubkey());
        let err = verify_against_pin(&keys, &other_pin).unwrap_err();
        assert!(err.to_string().contains("does not match"), "{err}");
        assert!(verify_against_pin(&keys, "not-a-pubkey").is_err());
    }

    #[test]
    fn verify_against_account_checks_both_halves() {
        let keys = parse_keys(&test_support::keys_json()).unwrap();
        let instance = Pubkey::new_unique();
        let mint = Pubkey::new_unique();
        let mut extension = ready_extension();
        extension.elgamal_pubkey = test_support::keys().pubkey().into();
        extension.decryptable_available_balance.0 =
            test_support::encrypt_aes(&test_support::keys(), 0);
        let configured = escrow_account_bytes(&instance, &mint, Some(extension));
        verify_against_account(&keys, &instance, &mint, &configured).unwrap();

        // Right ElGamal key, wrong AES key: only the account data can tell.
        let mut foreign_aes = extension;
        foreign_aes.decryptable_available_balance.0 =
            test_support::encrypt_aes(&test_support::other_keys(), 0);
        let foreign_aes = escrow_account_bytes(&instance, &mint, Some(foreign_aes));
        let err = verify_against_account(&keys, &instance, &mint, &foreign_aes).unwrap_err();
        assert!(err.to_string().contains("aes_key"), "{err}");

        let mut foreign_elgamal = extension;
        foreign_elgamal.elgamal_pubkey = test_support::other_keys().pubkey().into();
        let foreign_elgamal = escrow_account_bytes(&instance, &mint, Some(foreign_elgamal));
        let err = verify_against_account(&keys, &instance, &mint, &foreign_elgamal).unwrap_err();
        assert!(err.to_string().contains("another key"), "{err}");

        let plain = escrow_account_bytes(&instance, &mint, None);
        let err = verify_against_account(&keys, &instance, &mint, &plain).unwrap_err();
        assert!(err.to_string().contains("not configured"), "{err}");
    }

    /// A file holding any other account is refused before either key is checked, even one
    /// configured under the instance keys.
    #[test]
    fn verify_against_account_refuses_another_account() {
        let keys = parse_keys(&test_support::keys_json()).unwrap();
        let instance = Pubkey::new_unique();
        let mint = Pubkey::new_unique();
        let mut extension = ready_extension();
        extension.elgamal_pubkey = test_support::keys().pubkey().into();
        extension.decryptable_available_balance.0 =
            test_support::encrypt_aes(&test_support::keys(), 0);
        let other_mint = escrow_account_bytes(&instance, &Pubkey::new_unique(), Some(extension));
        let other_owner = escrow_account_bytes(&Pubkey::new_unique(), &mint, Some(extension));
        for data in [other_mint, other_owner] {
            let err = verify_against_account(&keys, &instance, &mint, &data).unwrap_err();
            assert!(err.to_string().contains("not the escrow account"), "{err}");
        }
        assert!(verify_against_account(&keys, &instance, &mint, &[1, 2, 3]).is_err());
    }

    // `account_bytes`, owned by `owner` for `mint` as the instance's escrow account is.
    fn escrow_account_bytes(
        owner: &Pubkey,
        mint: &Pubkey,
        extension: Option<ConfidentialTransferAccount>,
    ) -> Vec<u8> {
        use spl_token_2022_interface::extension::StateWithExtensionsMut;
        let mut data = account_bytes(AccountState::Initialized, extension);
        let mut account = StateWithExtensionsMut::<Account>::unpack(&mut data).unwrap();
        account.base.owner = *owner;
        account.base.mint = *mint;
        account.pack_base();
        data
    }

    #[test]
    fn the_configure_check_quotes_the_indexer_log_line() {
        let mint = Pubkey::new_from_array([3; 32]);
        assert_eq!(
            indexer_configure_log(&mint, "PK"),
            format!(
                "escrow account configured under the instance key mint={mint} elgamal_pubkey=PK"
            )
        );
    }

    fn ready_extension() -> ConfidentialTransferAccount {
        let mut extension: ConfidentialTransferAccount = bytemuck::Zeroable::zeroed();
        extension.approved = true.into();
        extension.allow_confidential_credits = true.into();
        extension.maximum_pending_balance_credit_counter = 65_536u64.into();
        extension
    }

    // A Token-2022 account with only the confidential extension, in `state`.
    fn account_bytes(
        state: AccountState,
        extension: Option<ConfidentialTransferAccount>,
    ) -> Vec<u8> {
        use solana_program_pack::Pack;
        use spl_token_2022_interface::extension::{
            BaseStateWithExtensionsMut, ExtensionType, StateWithExtensionsMut,
        };
        let types: Vec<ExtensionType> = extension
            .iter()
            .map(|_| ExtensionType::ConfidentialTransferAccount)
            .collect();
        let len = ExtensionType::try_calculate_account_len::<Account>(&types).unwrap();
        let mut data = vec![0u8; len];
        let mut account =
            StateWithExtensionsMut::<Account>::unpack_uninitialized(&mut data).unwrap();
        account.base = Account {
            state,
            ..Account::default()
        };
        account.pack_base();
        account.init_account_type().unwrap();
        if let Some(extension) = extension {
            *account
                .init_extension::<ConfidentialTransferAccount>(true)
                .unwrap() = extension;
        }
        let _ = Account::LEN;
        data
    }

    #[test]
    fn destination_readiness_matches_the_operator_parks() {
        assert_eq!(
            destination_not_ready(Some(&account_bytes(
                AccountState::Initialized,
                Some(ready_extension())
            ))),
            None
        );
        assert!(destination_not_ready(None)
            .unwrap()
            .contains("does not exist"));
        assert!(
            destination_not_ready(Some(&account_bytes(AccountState::Initialized, None)))
                .unwrap()
                .contains("not configured")
        );
        assert!(destination_not_ready(Some(&account_bytes(
            AccountState::Frozen,
            Some(ready_extension())
        )))
        .unwrap()
        .contains("frozen"));
        let mut unapproved = ready_extension();
        unapproved.approved = false.into();
        assert!(destination_not_ready(Some(&account_bytes(
            AccountState::Initialized,
            Some(unapproved)
        )))
        .unwrap()
        .contains("not approved"));
        let mut credits_off = ready_extension();
        credits_off.allow_confidential_credits = false.into();
        assert!(destination_not_ready(Some(&account_bytes(
            AccountState::Initialized,
            Some(credits_off)
        )))
        .unwrap()
        .contains("confidential credits"));
        let mut full = ready_extension();
        full.pending_balance_credit_counter = 65_536u64.into();
        assert!(
            destination_not_ready(Some(&account_bytes(AccountState::Initialized, Some(full))))
                .unwrap()
                .contains("counter is full")
        );
    }

    fn withdraw_accounts() -> WithdrawFunds {
        WithdrawFunds {
            user: Pubkey::new_unique(),
            mint: Pubkey::new_unique(),
            token_account: Pubkey::new_unique(),
            token_program: spl_token_2022_interface::ID,
            associated_token_program: Pubkey::new_unique(),
            withdraw_config: Pubkey::new_unique(),
            treasury_token_account: Pubkey::new_unique(),
        }
    }

    #[test]
    fn confidential_withdraw_is_withdraw_funds_with_its_own_tag() {
        let args = || WithdrawFundsInstructionArgs {
            amount: 5,
            destination: Some(Pubkey::new_from_array([9; 32])),
        };
        let accounts = withdraw_accounts();
        let public = WithdrawFunds {
            ..withdraw_accounts_like(&accounts)
        }
        .instruction(args());
        let confidential = confidential_withdraw_instruction(accounts, args()).unwrap();
        assert_eq!(confidential.data[0], 2);
        assert_eq!(confidential.data[1..], public.data[1..]);
        assert_eq!(confidential.accounts, public.accounts);
        assert_eq!(confidential.program_id, public.program_id);
    }

    #[test]
    fn confidential_withdraw_refuses_above_48_bits() {
        let at_max = WithdrawFundsInstructionArgs {
            amount: (1 << 48) - 1,
            destination: None,
        };
        assert!(confidential_withdraw_instruction(withdraw_accounts(), at_max).is_ok());
        let above = WithdrawFundsInstructionArgs {
            amount: 1 << 48,
            destination: None,
        };
        assert!(confidential_withdraw_instruction(withdraw_accounts(), above).is_err());
    }

    fn withdraw_accounts_like(a: &WithdrawFunds) -> WithdrawFunds {
        WithdrawFunds {
            user: a.user,
            mint: a.mint,
            token_account: a.token_account,
            token_program: a.token_program,
            associated_token_program: a.associated_token_program,
            withdraw_config: a.withdraw_config,
            treasury_token_account: a.treasury_token_account,
        }
    }
}
