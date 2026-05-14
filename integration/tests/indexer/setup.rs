/// Test setup utilities for integration tests
/// Creates instances, mints, and sets up accounts
use private_channel_escrow_program_client::{
    instructions::{AddOperatorBuilder, AllowMintBuilder, CreateInstanceBuilder},
    PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    pubkey::Pubkey,
    signature::{Keypair, Signer},
};
use solana_system_interface::program::ID as SYSTEM_PROGRAM_ID;
use spl_associated_token_account::{
    get_associated_token_address_with_program_id,
    instruction::create_associated_token_account_idempotent,
};
use spl_token::{instruction::mint_to, ID as TOKEN_PROGRAM_ID};

use super::helpers::{
    generate_mint, get_token_balance, mint_to_owner, send_and_confirm_instructions, setup_wallets,
};

const INSTANCE_SEED: &[u8] = b"instance";
const EVENT_AUTHORITY_SEED: &[u8] = b"event_authority";
const ALLOWED_MINT_SEED: &[u8] = b"allowed_mint";
const OPERATOR_SEED: &[u8] = b"operator";

pub const TEST_ADMIN_KEYPAIR: [u8; 64] = [
    153, 171, 234, 182, 220, 215, 41, 189, 53, 34, 53, 20, 142, 90, 108, 73, 104, 168, 58, 67, 78,
    154, 236, 205, 101, 133, 81, 122, 18, 49, 243, 211, 210, 207, 188, 101, 193, 22, 136, 109, 60,
    169, 124, 72, 137, 55, 0, 181, 51, 120, 85, 214, 67, 117, 151, 146, 177, 44, 178, 192, 111, 29,
    79, 26,
];

pub fn find_instance_pda(instance_seed: &Pubkey) -> (Pubkey, u8) {
    Pubkey::find_program_address(
        &[INSTANCE_SEED, instance_seed.as_ref()],
        &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
    )
}

pub fn find_event_authority_pda() -> (Pubkey, u8) {
    Pubkey::find_program_address(&[EVENT_AUTHORITY_SEED], &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
}

pub fn find_allowed_mint_pda(instance: &Pubkey, mint: &Pubkey) -> (Pubkey, u8) {
    Pubkey::find_program_address(
        &[ALLOWED_MINT_SEED, instance.as_ref(), mint.as_ref()],
        &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
    )
}

pub fn find_operator_pda(instance: &Pubkey, operator: &Pubkey) -> (Pubkey, u8) {
    Pubkey::find_program_address(
        &[OPERATOR_SEED, instance.as_ref(), operator.as_ref()],
        &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
    )
}

/// Complete test environment setup
pub struct TestEnvironment {
    pub users: Vec<Keypair>,
    pub mint: Pubkey,
    pub instance: Pubkey,
}

impl TestEnvironment {
    /// Setup a complete test environment with admin, users, mint, and instance
    pub async fn setup(
        client: &RpcClient,
        faucet_keypair: &Keypair,
        num_users: usize,
        initial_user_balance: u64,
        escrow_instance_id: Option<Keypair>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let admin = Keypair::try_from(&TEST_ADMIN_KEYPAIR[..])
            .map_err(|e| format!("Failed to create admin keypair: {}", e))?;
        let users: Vec<Keypair> = (0..num_users).map(|_| Keypair::new()).collect();

        let mut all_wallets = vec![&admin];
        all_wallets.extend(users.iter());
        setup_wallets(client, faucet_keypair, &all_wallets).await?;

        let mint_keypair = Keypair::new();
        let mint = generate_mint(client, &admin, &admin, &mint_keypair).await?;

        // batch all user ATA-creation + mint instructions into a
        // single transaction instead of one confirmation round-trip per user.
        // This reduces setup time from O(N × latency) to O(1 × latency).
        if !users.is_empty() && initial_user_balance > 0 {
            let mut batch_ixs = Vec::new();
            for user in &users {
                let ata = get_associated_token_address_with_program_id(
                    &user.pubkey(),
                    &mint,
                    &TOKEN_PROGRAM_ID,
                );
                batch_ixs.push(create_associated_token_account_idempotent(
                    &admin.pubkey(),
                    &user.pubkey(),
                    &mint,
                    &TOKEN_PROGRAM_ID,
                ));
                batch_ixs.push(mint_to(
                    &TOKEN_PROGRAM_ID,
                    &mint,
                    &ata,
                    &admin.pubkey(),
                    &[],
                    initial_user_balance,
                )?);
            }
            send_and_confirm_instructions(
                client,
                &batch_ixs,
                &admin,
                &[&admin],
                "Batch Mint to Users",
            )
            .await?;
            for user in &users {
                let balance = get_token_balance(client, &user.pubkey(), &mint).await?;
                println!(
                    "Minted {} tokens to {}. New balance: {} tokens",
                    initial_user_balance,
                    user.pubkey(),
                    balance
                );
            }
        } else {
            // Zero-balance case: still create ATAs so token accounts exist on-chain.
            for user in &users {
                mint_to_owner(client, &admin, mint, user.pubkey(), &admin, 0).await?;
                println!(
                    "Minted 0 tokens to {}. New balance: 0 tokens",
                    user.pubkey()
                );
            }
        }

        let (_, instance_pda) =
            Self::setup_instance(client, faucet_keypair, escrow_instance_id).await?;

        let (event_authority_pda, _) = find_event_authority_pda();

        // Allow mint
        let (allowed_mint_pda, bump) = find_allowed_mint_pda(&instance_pda, &mint);
        let instance_ata =
            get_associated_token_address_with_program_id(&instance_pda, &mint, &TOKEN_PROGRAM_ID);

        let allow_ix = AllowMintBuilder::new()
            .payer(admin.pubkey())
            .admin(admin.pubkey())
            .instance(instance_pda)
            .mint(mint)
            .allowed_mint(allowed_mint_pda)
            .instance_ata(instance_ata)
            .system_program(SYSTEM_PROGRAM_ID)
            .token_program(TOKEN_PROGRAM_ID)
            .associated_token_program(spl_associated_token_account::ID)
            .event_authority(event_authority_pda)
            .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
            .bump(bump)
            .instruction();

        send_and_confirm_instructions(client, &[allow_ix], &admin, &[&admin], "Allow Mint").await?;

        Ok(Self {
            users,
            mint,
            instance: instance_pda,
        })
    }

    pub async fn setup_instance(
        client: &RpcClient,
        faucet_keypair: &Keypair,
        escrow_instance_id: Option<Keypair>,
    ) -> Result<(Keypair, Pubkey), Box<dyn std::error::Error>> {
        let admin = Keypair::try_from(&TEST_ADMIN_KEYPAIR[..])
            .map_err(|e| format!("Failed to create admin keypair: {}", e))?;

        setup_wallets(client, faucet_keypair, &[&admin]).await?;

        let instance_seed = escrow_instance_id.unwrap_or(Keypair::new());
        let (instance_pda, bump) = find_instance_pda(&instance_seed.pubkey());
        let (event_authority_pda, _) = find_event_authority_pda();

        // If instance already exists, return it
        if (client.get_account(&instance_pda).await).is_ok() {
            return Ok((instance_seed, instance_pda));
        }

        let create_ix = CreateInstanceBuilder::new()
            .payer(admin.pubkey())
            .admin(admin.pubkey())
            .instance_seed(instance_seed.pubkey())
            .instance(instance_pda)
            .system_program(SYSTEM_PROGRAM_ID)
            .event_authority(event_authority_pda)
            .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
            .bump(bump)
            .instruction();

        send_and_confirm_instructions(
            client,
            &[create_ix],
            &admin,
            &[&admin, &instance_seed],
            "Create Instance",
        )
        .await?;

        Ok((instance_seed, instance_pda))
    }

    pub async fn setup_operator(
        client: &RpcClient,
        faucet_keypair: &Keypair,
        instance_pda: Pubkey,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let operator = Keypair::try_from(&TEST_ADMIN_KEYPAIR[..])
            .map_err(|e| format!("Failed to create operator keypair: {}", e))?;

        setup_wallets(client, faucet_keypair, &[&operator]).await?;

        let (operator_pda, bump) = find_operator_pda(&instance_pda, &operator.pubkey());
        let (event_authority_pda, _) = find_event_authority_pda();

        // If operator already exists, return
        if (client.get_account(&operator_pda).await).is_ok() {
            return Ok(());
        }

        let add_operator_ix = AddOperatorBuilder::new()
            .payer(operator.pubkey())
            .admin(operator.pubkey())
            .operator(operator.pubkey())
            .instance(instance_pda)
            .operator_pda(operator_pda)
            .system_program(SYSTEM_PROGRAM_ID)
            .event_authority(event_authority_pda)
            .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
            .bump(bump)
            .instruction();

        send_and_confirm_instructions(
            client,
            &[add_operator_ix],
            &operator,
            &[&operator],
            "Add Operator",
        )
        .await?;

        Ok(())
    }

    /// Setup for multi-user chaos testing
    #[allow(dead_code)]
    pub async fn setup_multi_user(
        client: &RpcClient,
        faucet_keypair: &Keypair,
        num_users: usize,
        escrow_instance_id: Option<Keypair>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        // Give each user 10M tokens (plenty for testing)
        Self::setup(
            client,
            faucet_keypair,
            num_users,
            10_000_000 * 10u64.pow(6),
            escrow_instance_id,
        )
        .await
    }
}
