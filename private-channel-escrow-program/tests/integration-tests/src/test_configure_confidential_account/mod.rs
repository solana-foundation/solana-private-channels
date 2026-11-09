use crate::{
    pda_utils::find_allowed_mint_pda,
    state_utils::{
        assert_get_or_allow_mint, assert_get_or_create_instance, setup_confidential_escrow,
    },
    utils::{
        assert_failed_first, confidential_state, create_confidential_mint, custom_error,
        event_data, set_legacy_confidential_fee_mint, set_mint_2022_basic, ConfidentialKeys,
        ConfidentialMintOptions, TestContext, INCORRECT_PROGRAM_ID_LOG, INVALID_ADMIN_ERROR,
        MINT_PROFILE_CHANGED_ERROR, PRIVATE_CHANNEL_ESCROW_PROGRAM_ID, TOKEN_2022_PROGRAM_ID,
        UNSUPPORTED_CONFIDENTIAL_MINT_ERROR,
    },
};
use private_channel_escrow_program_client::instructions::ConfigureConfidentialAccountBuilder;
use solana_sdk::signature::{Keypair, Signer};
use solana_zk_elgamal_proof_interface::instruction::ProofInstruction;
use solana_zk_sdk::zk_elgamal_proof_program::build_pubkey_validity_proof_data;
use spl_associated_token_account::get_associated_token_address_with_program_id;
use spl_token::ID as TOKEN_PROGRAM_ID;
use spl_token_2022::{
    error::TokenError,
    extension::{BaseStateWithExtensions, ExtensionType, StateWithExtensions},
    state::Account as Token2022Account,
};

#[test]
fn test_configure_confidential_account_success() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let instance_seed = Keypair::new();
    let keys = ConfidentialKeys::new();
    let mint = create_confidential_mint(&mut context, ConfidentialMintOptions::default());

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");
    let (allowed_mint_pda, _) = assert_get_or_allow_mint(
        &mut context,
        &admin,
        &instance_pda,
        &mint.address,
        false,
        false,
    )
    .expect("AllowMint should succeed");
    let escrow_ata = get_associated_token_address_with_program_id(
        &instance_pda,
        &mint.address,
        &TOKEN_2022_PROGRAM_ID,
    );

    let instruction = ConfigureConfidentialAccountBuilder::new()
        .payer(context.payer.pubkey())
        .admin(admin.pubkey())
        .instance(instance_pda)
        .mint(mint.address)
        .allowed_mint(allowed_mint_pda)
        .instance_ata(escrow_ata)
        .decryptable_zero_balance(keys.decryptable(0).0)
        .proof_instruction_offset(1)
        .instruction();
    let proof = ProofInstruction::VerifyPubkeyValidity.encode_verify_proof(
        None,
        &build_pubkey_validity_proof_data(&keys.elgamal).unwrap(),
    );

    let transaction_metadata = context
        .send_v1(&[instruction, proof], &[&admin])
        .expect("ConfigureConfidentialAccount should succeed");

    let state = confidential_state(&mut context, &escrow_ata);
    assert_eq!(state.elgamal_pubkey, (*keys.elgamal.pubkey()).into());
    assert!(bool::from(state.approved));
    // Confidential credits only arrive through the escrow's own instructions.
    assert!(!bool::from(state.allow_confidential_credits));
    assert!(bool::from(state.allow_non_confidential_credits));
    // After the instance seed and mint: the key the indexer must hold.
    let event = event_data(&transaction_metadata, 12);
    assert_eq!(&event[64..96], &keys.elgamal.pubkey().to_bytes());
}

// ConfigureAccount adds the fee extension too, so Reallocate has to make room for it.
#[test]
fn test_configure_confidential_account_fee_mint() {
    let mut context = TestContext::new();

    let escrow = setup_confidential_escrow(
        &mut context,
        ConfidentialMintOptions {
            transfer_fee: Some((100, 1_000_000)),
            ..Default::default()
        },
    );

    let data = context.get_account_data(&escrow.escrow_ata).unwrap();
    let extensions = StateWithExtensions::<Token2022Account>::unpack(&data)
        .unwrap()
        .get_extension_types()
        .unwrap();
    assert!(extensions.contains(&ExtensionType::ConfidentialTransferFeeAmount));
}

// The issuer's confidential authority has to approve the escrow account first.
#[test]
fn test_configure_confidential_account_manual_approval_mint_leaves_account_unapproved() {
    let mut context = TestContext::new();

    let escrow = setup_confidential_escrow(
        &mut context,
        ConfidentialMintOptions {
            manual_approval: true,
            ..Default::default()
        },
    );

    assert!(!bool::from(
        confidential_state(&mut context, &escrow.escrow_ata).approved
    ));
}

#[test]
fn test_configure_confidential_account_rejects_non_admin() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let attacker = Keypair::new();
    let instance_seed = Keypair::new();
    let keys = ConfidentialKeys::new();
    let mint = create_confidential_mint(&mut context, ConfidentialMintOptions::default());

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");
    let (allowed_mint_pda, _) = assert_get_or_allow_mint(
        &mut context,
        &admin,
        &instance_pda,
        &mint.address,
        false,
        false,
    )
    .expect("AllowMint should succeed");

    let instruction = ConfigureConfidentialAccountBuilder::new()
        .payer(context.payer.pubkey())
        .admin(attacker.pubkey())
        .instance(instance_pda)
        .mint(mint.address)
        .allowed_mint(allowed_mint_pda)
        .instance_ata(get_associated_token_address_with_program_id(
            &instance_pda,
            &mint.address,
            &TOKEN_2022_PROGRAM_ID,
        ))
        .decryptable_zero_balance(keys.decryptable(0).0)
        .proof_instruction_offset(1)
        .instruction();
    let proof = ProofInstruction::VerifyPubkeyValidity.encode_verify_proof(
        None,
        &build_pubkey_validity_proof_data(&keys.elgamal).unwrap(),
    );

    let result = context.send_v1(&[instruction, proof], &[&attacker]);

    assert_failed_first(
        result,
        &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
        &custom_error(INVALID_ADMIN_ERROR),
    );
}

#[test]
fn test_configure_confidential_account_rejects_spl_token_program() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let instance_seed = Keypair::new();
    let keys = ConfidentialKeys::new();
    let mint = create_confidential_mint(&mut context, ConfidentialMintOptions::default());

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");
    let (allowed_mint_pda, _) = assert_get_or_allow_mint(
        &mut context,
        &admin,
        &instance_pda,
        &mint.address,
        false,
        false,
    )
    .expect("AllowMint should succeed");

    let instruction = ConfigureConfidentialAccountBuilder::new()
        .payer(context.payer.pubkey())
        .admin(admin.pubkey())
        .instance(instance_pda)
        .mint(mint.address)
        .allowed_mint(allowed_mint_pda)
        .instance_ata(get_associated_token_address_with_program_id(
            &instance_pda,
            &mint.address,
            &TOKEN_2022_PROGRAM_ID,
        ))
        .token_program(TOKEN_PROGRAM_ID)
        .decryptable_zero_balance(keys.decryptable(0).0)
        .proof_instruction_offset(1)
        .instruction();
    let proof = ProofInstruction::VerifyPubkeyValidity.encode_verify_proof(
        None,
        &build_pubkey_validity_proof_data(&keys.elgamal).unwrap(),
    );

    let result = context.send_v1(&[instruction, proof], &[&admin]);

    assert_failed_first(
        result,
        &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
        INCORRECT_PROGRAM_ID_LOG,
    );
}

// The escrow refuses a mint without confidential transfers.
#[test]
fn test_configure_confidential_account_rejects_mint_without_extension() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let instance_seed = Keypair::new();
    let mint = Keypair::new();
    let keys = ConfidentialKeys::new();
    set_mint_2022_basic(&mut context, &mint.pubkey());

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");
    let (allowed_mint_pda, _) = assert_get_or_allow_mint(
        &mut context,
        &admin,
        &instance_pda,
        &mint.pubkey(),
        false,
        false,
    )
    .expect("AllowMint should succeed");

    let instruction = ConfigureConfidentialAccountBuilder::new()
        .payer(context.payer.pubkey())
        .admin(admin.pubkey())
        .instance(instance_pda)
        .mint(mint.pubkey())
        .allowed_mint(allowed_mint_pda)
        .instance_ata(get_associated_token_address_with_program_id(
            &instance_pda,
            &mint.pubkey(),
            &TOKEN_2022_PROGRAM_ID,
        ))
        .decryptable_zero_balance(keys.decryptable(0).0)
        .proof_instruction_offset(1)
        .instruction();
    let proof = ProofInstruction::VerifyPubkeyValidity.encode_verify_proof(
        None,
        &build_pubkey_validity_proof_data(&keys.elgamal).unwrap(),
    );

    let result = context.send_v1(&[instruction, proof], &[&admin]);

    assert_failed_first(
        result,
        &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
        &custom_error(UNSUPPORTED_CONFIDENTIAL_MINT_ERROR),
    );
}

// Token-2022 refuses every confidential transfer of a fee mint without the
// confidential fee config. It no longer creates such a mint, but an older one can
// still have it.
#[test]
fn test_configure_confidential_account_rejects_fee_mint_without_confidential_fee_config() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let instance_seed = Keypair::new();
    let mint = Keypair::new();
    let keys = ConfidentialKeys::new();
    set_legacy_confidential_fee_mint(&mut context, &mint.pubkey());

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");
    let (allowed_mint_pda, _) = assert_get_or_allow_mint(
        &mut context,
        &admin,
        &instance_pda,
        &mint.pubkey(),
        false,
        false,
    )
    .expect("AllowMint should succeed");

    let instruction = ConfigureConfidentialAccountBuilder::new()
        .payer(context.payer.pubkey())
        .admin(admin.pubkey())
        .instance(instance_pda)
        .mint(mint.pubkey())
        .allowed_mint(allowed_mint_pda)
        .instance_ata(get_associated_token_address_with_program_id(
            &instance_pda,
            &mint.pubkey(),
            &TOKEN_2022_PROGRAM_ID,
        ))
        .decryptable_zero_balance(keys.decryptable(0).0)
        .proof_instruction_offset(1)
        .instruction();
    let proof = ProofInstruction::VerifyPubkeyValidity.encode_verify_proof(
        None,
        &build_pubkey_validity_proof_data(&keys.elgamal).unwrap(),
    );

    let result = context.send_v1(&[instruction, proof], &[&admin]);

    assert_failed_first(
        result,
        &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
        &custom_error(UNSUPPORTED_CONFIDENTIAL_MINT_ERROR),
    );
}

// The mint was recreated at the same address after AllowMint, here with
// ConfidentialMintBurn added. Configure refuses it as Deposit does, instead of
// deciding from the extensions recorded at AllowMint.
#[test]
fn test_configure_confidential_account_rejects_recreated_mint() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let instance_seed = Keypair::new();
    let keys = ConfidentialKeys::new();
    let mint = create_confidential_mint(&mut context, ConfidentialMintOptions::default());

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");
    let (allowed_mint_pda, _) = assert_get_or_allow_mint(
        &mut context,
        &admin,
        &instance_pda,
        &mint.address,
        false,
        false,
    )
    .expect("AllowMint should succeed");
    let recreated_mint = create_confidential_mint(
        &mut context,
        ConfidentialMintOptions {
            confidential_mint_burn: true,
            ..Default::default()
        },
    );
    let recreated_mint_account = context.svm.get_account(&recreated_mint.address).unwrap();
    context
        .svm
        .set_account(mint.address, recreated_mint_account)
        .unwrap();

    let instruction = ConfigureConfidentialAccountBuilder::new()
        .payer(context.payer.pubkey())
        .admin(admin.pubkey())
        .instance(instance_pda)
        .mint(mint.address)
        .allowed_mint(allowed_mint_pda)
        .instance_ata(get_associated_token_address_with_program_id(
            &instance_pda,
            &mint.address,
            &TOKEN_2022_PROGRAM_ID,
        ))
        .decryptable_zero_balance(keys.decryptable(0).0)
        .proof_instruction_offset(1)
        .instruction();
    let proof = ProofInstruction::VerifyPubkeyValidity.encode_verify_proof(
        None,
        &build_pubkey_validity_proof_data(&keys.elgamal).unwrap(),
    );

    let result = context.send_v1(&[instruction, proof], &[&admin]);

    assert_failed_first(
        result,
        &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
        &custom_error(MINT_PROFILE_CHANGED_ERROR),
    );
}

// An account's ElGamal key never changes, so a second configure must fail.
#[test]
fn test_configure_confidential_account_twice_fails() {
    let mut context = TestContext::new();
    let escrow = setup_confidential_escrow(&mut context, ConfidentialMintOptions::default());
    let other_keys = ConfidentialKeys::new();

    let instruction = ConfigureConfidentialAccountBuilder::new()
        .payer(context.payer.pubkey())
        .admin(escrow.admin.pubkey())
        .instance(escrow.instance_pda)
        .mint(escrow.mint.address)
        .allowed_mint(find_allowed_mint_pda(&escrow.instance_pda, &escrow.mint.address).0)
        .instance_ata(escrow.escrow_ata)
        .decryptable_zero_balance(other_keys.decryptable(0).0)
        .proof_instruction_offset(1)
        .instruction();
    let proof = ProofInstruction::VerifyPubkeyValidity.encode_verify_proof(
        None,
        &build_pubkey_validity_proof_data(&other_keys.elgamal).unwrap(),
    );

    let result = context.send_v1(&[instruction, proof], &[&escrow.admin]);

    assert_failed_first(
        result,
        &TOKEN_2022_PROGRAM_ID,
        &custom_error(TokenError::ExtensionAlreadyInitialized as u32),
    );
    assert_eq!(
        confidential_state(&mut context, &escrow.escrow_ata).elgamal_pubkey,
        (*escrow.keys.elgamal.pubkey()).into()
    );
}
