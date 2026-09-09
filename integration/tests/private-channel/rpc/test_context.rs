use {
    anyhow::Result,
    solana_client::{
        nonblocking::rpc_client::RpcClient,
        rpc_response::{Response, RpcPerfSample, RpcSupply, RpcVoteAccountStatus},
    },
    solana_sdk::{
        clock::Slot,
        commitment_config::CommitmentConfig,
        epoch_info::EpochInfo,
        epoch_schedule::EpochSchedule,
        hash::Hash,
        program_pack::Pack,
        pubkey::Pubkey,
        signature::{Keypair, Signature},
        signer::Signer,
        transaction::Transaction,
    },
    solana_system_interface::instruction as system_instruction,
    solana_transaction_status::UiTransactionEncoding,
    spl_associated_token_account::get_associated_token_address,
    std::time::Duration,
    tokio::time::sleep,
};

use private_channel_indexer::storage::Storage;

pub struct PrivateChannelContext {
    pub write_client: RpcClient,
    pub read_client: RpcClient,
    pub operator_key: Keypair,
    pub mint: Pubkey,
    pub indexer_storage: Storage,
}

impl PrivateChannelContext {
    pub fn new(
        read_url: String,
        write_url: String,
        operator_key: Keypair,
        mint: Pubkey,
        indexer_storage: Storage,
    ) -> Self {
        Self {
            write_client: RpcClient::new(write_url),
            read_client: RpcClient::new(read_url),
            operator_key,
            mint,
            indexer_storage,
        }
    }

    pub async fn get_slot(&self) -> Result<u64> {
        self.read_client
            .get_slot()
            .await
            .map_err(anyhow::Error::from)
    }

    pub async fn get_block_height(&self) -> Result<u64> {
        self.read_client
            .get_block_height()
            .await
            .map_err(anyhow::Error::from)
    }

    pub async fn get_blockhash(&self) -> Result<Hash> {
        let (blockhash, _) = self
            .read_client
            .get_latest_blockhash_with_commitment(CommitmentConfig::confirmed())
            .await?;
        Ok(blockhash)
    }

    pub async fn send_transaction(&self, tx: &Transaction) -> Result<Signature> {
        self.write_client
            .send_transaction(tx)
            .await
            .map_err(anyhow::Error::from)
    }

    pub async fn get_transaction(
        &self,
        signature: &Signature,
    ) -> Result<Option<serde_json::Value>> {
        self.get_transaction_with_encoding(signature, UiTransactionEncoding::Json)
            .await
    }

    pub async fn get_transaction_with_encoding(
        &self,
        signature: &Signature,
        encoding: UiTransactionEncoding,
    ) -> Result<Option<serde_json::Value>> {
        if let Ok(tx) = self.read_client.get_transaction(signature, encoding).await {
            Ok(Some(serde_json::to_value(tx)?))
        } else {
            Ok(None)
        }
    }

    pub async fn send_and_check(
        &self,
        tx: &Transaction,
        wait_duration: Duration,
    ) -> Result<Option<Signature>> {
        let sig = self
            .send_transaction(tx)
            .await
            .map_err(|e| anyhow::anyhow!("Send transaction failed: {}", e))?;

        // Poll until the transaction is confirmed or wait_duration expires.
        // With blocktime_ms = 100 ms the transaction is typically settled in < 300 ms,
        // so this returns much earlier than the 1 s fixed sleep it replaces.
        let deadline = tokio::time::Instant::now() + wait_duration;
        loop {
            if let Ok(Some(_)) = self.get_transaction(&sig).await {
                return Ok(Some(sig));
            }
            if tokio::time::Instant::now() >= deadline {
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }

        self.get_transaction(&sig).await.map(|opt| opt.map(|_| sig))
    }

    pub async fn check_transaction_exists(&self, signature: Signature) {
        super::utils::confirm_transaction(&self.read_client, signature).await;
    }

    pub async fn get_token_balance(&self, token_account: &Pubkey) -> Result<u64> {
        super::utils::token_balance(&self.read_client, token_account).await
    }

    pub async fn get_transaction_count(&self) -> Result<u64> {
        self.read_client
            .get_transaction_count()
            .await
            .map_err(anyhow::Error::from)
    }

    pub async fn get_epoch_info(&self) -> Result<EpochInfo> {
        self.read_client
            .get_epoch_info()
            .await
            .map_err(anyhow::Error::from)
    }

    pub async fn get_epoch_schedule(&self) -> Result<EpochSchedule> {
        self.read_client
            .get_epoch_schedule()
            .await
            .map_err(anyhow::Error::from)
    }

    pub async fn get_first_available_block(&self) -> Result<Slot> {
        self.read_client
            .get_first_available_block()
            .await
            .map_err(anyhow::Error::from)
    }

    pub async fn get_blocks(&self, start_slot: Slot, end_slot: Option<Slot>) -> Result<Vec<Slot>> {
        self.read_client
            .get_blocks(start_slot, end_slot)
            .await
            .map_err(anyhow::Error::from)
    }

    pub async fn get_blocks_with_limit(&self, start_slot: Slot, limit: usize) -> Result<Vec<Slot>> {
        self.read_client
            .get_blocks_with_limit(start_slot, limit)
            .await
            .map_err(anyhow::Error::from)
    }

    pub async fn get_recent_performance_samples(
        &self,
        limit: Option<usize>,
    ) -> Result<Vec<RpcPerfSample>> {
        self.read_client
            .get_recent_performance_samples(limit)
            .await
            .map_err(anyhow::Error::from)
    }

    pub async fn get_block_time(&self, slot: Slot) -> Result<Option<i64>> {
        match self.read_client.get_block_time(slot).await {
            Ok(time) => Ok(Some(time)),
            Err(e) => {
                // Check if it's a block not found error
                let err_str = e.to_string();
                if err_str.contains("Block not available") || err_str.contains("Slot was skipped") {
                    Ok(None)
                } else {
                    Err(anyhow::Error::from(e))
                }
            }
        }
    }

    pub async fn get_vote_accounts(&self) -> Result<RpcVoteAccountStatus> {
        self.read_client
            .get_vote_accounts()
            .await
            .map_err(anyhow::Error::from)
    }

    pub async fn get_supply(&self) -> Result<Response<RpcSupply>> {
        self.read_client.supply().await.map_err(anyhow::Error::from)
    }

    pub async fn get_slot_leaders(&self, start_slot: Slot, limit: u64) -> Result<Vec<Pubkey>> {
        self.read_client
            .get_slot_leaders(start_slot, limit)
            .await
            .map_err(anyhow::Error::from)
    }
}

pub struct SolanaContext {
    pub client: RpcClient,
    pub operator_key: Keypair,
    pub faucet: Keypair,
    pub escrow_instance: Keypair,
    pub indexer_storage: Storage,
}

impl SolanaContext {
    pub fn new(
        validator_url: String,
        admin_key: Keypair,
        faucet: Keypair,
        escrow_instance: Keypair,
        indexer_storage: Storage,
    ) -> Self {
        Self {
            client: RpcClient::new_with_commitment(validator_url, CommitmentConfig::processed()),
            operator_key: admin_key,
            faucet,
            escrow_instance,
            indexer_storage,
        }
    }

    pub async fn get_latest_blockhash(&self) -> Result<Hash> {
        self.client
            .get_latest_blockhash()
            .await
            .map_err(anyhow::Error::from)
    }

    pub async fn get_balance(&self, pubkey: &Pubkey) -> Result<u64> {
        self.client
            .get_balance(pubkey)
            .await
            .map_err(anyhow::Error::from)
    }

    pub async fn get_token_balance(&self, token_account: &Pubkey) -> Result<u64> {
        self.client
            .get_token_account_balance(token_account)
            .await
            .map_err(anyhow::Error::from)?
            .amount
            .parse::<u64>()
            .map_err(anyhow::Error::from)
    }

    pub async fn send_transaction(&self, tx: &Transaction) -> Result<Signature> {
        self.client
            .send_and_confirm_transaction(tx)
            .await
            .map_err(anyhow::Error::from)
    }

    pub async fn fund_account(&self, pubkey: &Pubkey, lamports: u64) -> Result<Signature> {
        let blockhash = self.client.get_latest_blockhash().await?;

        let transfer_ix = system_instruction::transfer(&self.faucet.pubkey(), pubkey, lamports);

        let tx = Transaction::new_signed_with_payer(
            &[transfer_ix],
            Some(&self.faucet.pubkey()),
            &[&self.faucet],
            blockhash,
        );

        self.send_transaction(&tx).await
    }

    pub async fn create_mint(
        &self,
        mint_keypair: &Keypair,
        mint_authority: &Pubkey,
        decimals: u8,
    ) -> Result<Signature> {
        let blockhash = self.client.get_latest_blockhash().await?;
        let mint_rent = self
            .client
            .get_minimum_balance_for_rent_exemption(spl_token::state::Mint::LEN)
            .await?;

        let tx = Transaction::new_signed_with_payer(
            &[
                system_instruction::create_account(
                    &self.operator_key.pubkey(),
                    &mint_keypair.pubkey(),
                    mint_rent,
                    spl_token::state::Mint::LEN as u64,
                    &spl_token::id(),
                ),
                spl_token::instruction::initialize_mint(
                    &spl_token::id(),
                    &mint_keypair.pubkey(),
                    mint_authority,
                    None,
                    decimals,
                )?,
            ],
            Some(&self.operator_key.pubkey()),
            &[&self.operator_key, mint_keypair],
            blockhash,
        );

        self.client
            .send_and_confirm_transaction(&tx)
            .await
            .map_err(anyhow::Error::from)
    }

    pub async fn create_t22_mint(
        &self,
        mint_keypair: &Keypair,
        mint_authority: &Pubkey,
        decimals: u8,
    ) -> Result<Signature> {
        let blockhash = self.client.get_latest_blockhash().await?;
        let mint_rent = self
            .client
            .get_minimum_balance_for_rent_exemption(spl_token_2022::state::Mint::LEN)
            .await?;

        let tx = Transaction::new_signed_with_payer(
            &[
                system_instruction::create_account(
                    &self.operator_key.pubkey(),
                    &mint_keypair.pubkey(),
                    mint_rent,
                    spl_token_2022::state::Mint::LEN as u64,
                    &spl_token_2022::id(),
                ),
                spl_token_2022::instruction::initialize_mint(
                    &spl_token_2022::id(),
                    &mint_keypair.pubkey(),
                    mint_authority,
                    None,
                    decimals,
                )?,
            ],
            Some(&self.operator_key.pubkey()),
            &[&self.operator_key, mint_keypair],
            blockhash,
        );

        self.client
            .send_and_confirm_transaction(&tx)
            .await
            .map_err(anyhow::Error::from)
    }

    pub async fn create_token_accounts(
        &self,
        mint: &Pubkey,
        keypairs: &[&Keypair],
        token_program_id: &Pubkey,
    ) -> Result<()> {
        for keypair in keypairs {
            let token_account = get_associated_token_address(&keypair.pubkey(), mint);
            let blockhash = self.client.get_latest_blockhash().await?;

            let create_ata_ix =
                spl_associated_token_account::instruction::create_associated_token_account(
                    &keypair.pubkey(),
                    &keypair.pubkey(),
                    mint,
                    token_program_id,
                );

            let create_ata_tx = Transaction::new_signed_with_payer(
                &[create_ata_ix],
                Some(&keypair.pubkey()),
                &[keypair],
                blockhash,
            );

            self.client
                .send_and_confirm_transaction(&create_ata_tx)
                .await?;
            println!("  Created Solana token account: {}", token_account);
        }

        Ok(())
    }

    pub async fn mint_to(
        &self,
        mint: &Pubkey,
        token_account: &Pubkey,
        amount: u64,
        token_program_id: &Pubkey,
    ) -> Result<Signature> {
        let blockhash = self.client.get_latest_blockhash().await?;

        let mint_to_ix = if token_program_id == &spl_token::ID {
            spl_token::instruction::mint_to(
                token_program_id,
                mint,
                token_account,
                &self.operator_key.pubkey(),
                &[],
                amount,
            )?
        } else if token_program_id == &spl_token_2022::ID {
            spl_token_2022::instruction::mint_to(
                token_program_id,
                mint,
                token_account,
                &self.operator_key.pubkey(),
                &[],
                amount,
            )?
        } else {
            panic!("Unsupported token program ID: {}", token_program_id);
        };

        let tx = Transaction::new_signed_with_payer(
            &[mint_to_ix],
            Some(&self.operator_key.pubkey()),
            &[&self.operator_key],
            blockhash,
        );

        self.client
            .send_and_confirm_transaction(&tx)
            .await
            .map_err(anyhow::Error::from)
    }
}
