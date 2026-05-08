//! Deposit setup phase — Solana escrow + PrivateChannel mint preparation.
//!
//! Prepares all on-chain state the deposit load phase needs:
//!   1. Loads the admin keypair from disk.
//!   2. Generates a fresh instance-seed keypair and derives the escrow instance PDA.
//!   3. Creates the escrow instance on Solana (CreateInstance instruction).
//!   4. Generates N fresh depositor keypairs.
//!   5. Funds each depositor with SOL (via transfer from admin).
//!   6. Creates a fresh Solana SPL mint (admin is mint authority) and
//!      initialises it on **PrivateChannel** so the operator can mint immediately
//!      without JIT initialisation.
//!   7. Calls AllowMint — registers the mint with the instance, creating both
//!      the allowed_mint PDA and the instance ATA on Solana.
//!   8. Creates Solana ATAs for each depositor.
//!   9. Mints initial token balances to each depositor ATA.
//!  10. Fetches the current Solana blockhash and seeds `BenchState`.
//!
//! The function returns a `DepositConfig` that the deposit load phase uses
//! directly.

use {
    crate::{
        rpc::{poll_confirmations, send_parallel},
        types::{BenchState, DepositConfig, MINT_DECIMALS, SETUP_BATCH_SIZE},
    },
    anyhow::{Context, Result},
    private_channel_core::client::{
        create_admin_initialize_mint, create_admin_mint_to, create_ata_transaction,
    },
    private_channel_escrow_program_client::{
        instructions::{
            AllowMint, AllowMintInstructionArgs, CreateInstance, CreateInstanceInstructionArgs,
        },
        PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
    },
    rayon::prelude::*,
    solana_client::{nonblocking::rpc_client::RpcClient, rpc_config::RpcSendTransactionConfig},
    solana_sdk::{
        commitment_config::CommitmentConfig, pubkey::Pubkey, signature::Keypair, signer::Signer,
        transaction::Transaction,
    },
    solana_system_interface::instruction as system_instruction,
    solana_system_interface::program,
    spl_associated_token_account::get_associated_token_address,
    spl_token::{solana_program::program_pack::Pack, state::Mint as SplMint},
    std::{path::Path, sync::Arc, time::Instant},
    tokio::{sync::RwLock, time::Duration},
    tracing::{info, warn},
};

const INSTANCE_SEED_PREFIX: &[u8] = b"instance";
const ALLOWED_MINT_SEED_PREFIX: &[u8] = b"allowed_mint";
const EVENT_AUTHORITY_SEED: &[u8] = b"event_authority";

/// 10 SOL minimum
const MIN_ADMIN_LAMPORTS: u64 = 10_000_000_000;
/// 100 SOL top-up
const AIRDROP_LAMPORTS: u64 = 100_000_000_000;

/// Derive the escrow instance PDA from the instance-seed keypair's pubkey.
pub fn find_instance_pda(instance_seed: &Pubkey) -> (Pubkey, u8) {
    Pubkey::find_program_address(
        &[INSTANCE_SEED_PREFIX, instance_seed.as_ref()],
        &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
    )
}

/// Derive the allowed-mint PDA for a given (instance_pda, mint) pair.
fn find_allowed_mint_pda(instance_pda: &Pubkey, mint: &Pubkey) -> (Pubkey, u8) {
    Pubkey::find_program_address(
        &[
            ALLOWED_MINT_SEED_PREFIX,
            instance_pda.as_ref(),
            mint.as_ref(),
        ],
        &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
    )
}

/// Derive the Anchor event-authority PDA for the escrow program.
fn find_event_authority() -> (Pubkey, u8) {
    Pubkey::find_program_address(&[EVENT_AUTHORITY_SEED], &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
}

/// Run all deposit setup tasks and return the `DepositConfig` needed by the
/// deposit load phase.
///
/// `solana_rpc_url`  — Solana validator RPC endpoint
/// `private_channel_rpc_url`  — PrivateChannel gateway / write-node RPC endpoint
/// `admin_path`      — path to the admin keypair JSON file
/// `instance_seed_path` — optional path to save/load the instance-seed keypair;
///                        when `Some`, the keypair (and thus instance PDA) is
///                        reused across runs so the indexer can track it.
/// `num_accounts`    — number of depositor accounts to create
/// `initial_balance` — raw token units minted to each depositor ATA
pub async fn run_setup_deposit_phase(
    solana_rpc_url: &str,
    private_channel_rpc_url: &str,
    admin_path: &Path,
    instance_seed_path: Option<&Path>,
    num_accounts: usize,
    initial_balance: u64,
) -> Result<DepositConfig> {
    // ------------------------------------------------------------------
    // Task 1: Load admin keypair
    // ------------------------------------------------------------------
    let admin_keypair = Arc::new(
        private_channel_core::client::load_keypair(admin_path)
            .map_err(|e| anyhow::anyhow!("failed to load admin keypair: {e}"))?,
    );
    info!(pubkey = %admin_keypair.pubkey(), "Loaded admin keypair (deposit setup)");

    // ------------------------------------------------------------------
    // Task 2: Load or generate the instance-seed keypair and derive PDAs
    //
    // If `instance_seed_path` is given the keypair is loaded from (or saved to)
    // that file so the same instance PDA is reused across bench runs.  This is
    // required for the indexer/operator services (pre-configured with the PDA)
    // to observe the deposits.  When no path is given a fresh ephemeral keypair
    // is generated (useful for isolated tests).
    // ------------------------------------------------------------------
    let instance_seed_keypair: Keypair = match instance_seed_path {
        Some(path) if path.exists() => private_channel_core::client::load_keypair(path)
            .map_err(|e| anyhow::anyhow!("failed to load instance-seed keypair: {e}"))?,
        Some(path) => {
            let kp = Keypair::new();
            let bytes = kp.to_bytes();
            let json = serde_json::to_string(&bytes.to_vec())
                .context("serialize instance-seed keypair")?;
            std::fs::write(path, json).context("write instance-seed keypair")?;
            info!(path = %path.display(), "Generated and saved new instance-seed keypair");
            kp
        }
        None => Keypair::new(),
    };
    let instance_seed_pubkey = instance_seed_keypair.pubkey();
    let (instance_pda, instance_bump) = find_instance_pda(&instance_seed_pubkey);
    let (event_authority, _) = find_event_authority();
    info!(
        %instance_seed_pubkey,
        %instance_pda,
        "Instance-seed keypair ready, derived PDAs",
    );

    // Use Processed commitment throughout setup so that preflight simulations
    // and account queries reflect the latest state without waiting for
    // Finalized (which lags ~32 slots / ~13 s on a local validator).
    // Arc-wrapped so closures passed to send_parallel can share it cheaply.
    let rpc = Arc::new(RpcClient::new_with_commitment(
        solana_rpc_url.to_owned(),
        CommitmentConfig::processed(),
    ));
    let send_retry_delays: &[u64] = &[1, 2, 4, 8, 16, 30];

    // ------------------------------------------------------------------
    // Task 2b: Ensure admin has SOL on Solana
    //
    // The Solana local validator does not pre-fund the bench admin keypair.
    // We need enough lamports to cover: CreateInstance + AllowMint fees
    // + N depositor SOL transfers + ATA creation fees.
    // Only airdrop if the current balance is below the threshold so that
    // repeated runs don't hit the validator's airdrop rate limit.
    // ------------------------------------------------------------------
    let balance = rpc
        .get_balance(&admin_keypair.pubkey())
        .await
        .context("get_balance for admin on Solana")?;

    if balance < MIN_ADMIN_LAMPORTS {
        let sig = rpc
            .request_airdrop(&admin_keypair.pubkey(), AIRDROP_LAMPORTS)
            .await
            .context("airdrop to admin on Solana")?;
        for _ in 0..60u32 {
            tokio::time::sleep(Duration::from_millis(500)).await;
            if rpc.get_balance(&admin_keypair.pubkey()).await.unwrap_or(0) >= AIRDROP_LAMPORTS {
                break;
            }
        }
        if rpc.get_balance(&admin_keypair.pubkey()).await.unwrap_or(0) < MIN_ADMIN_LAMPORTS {
            return Err(anyhow::anyhow!(
                "airdrop timed out: admin balance still below minimum after 60 attempts"
            ));
        }
        info!(lamports = AIRDROP_LAMPORTS, sig = %sig, "Admin airdropped on Solana");
    } else {
        info!(balance, "Admin already funded on Solana, skipping airdrop");
    }

    // ------------------------------------------------------------------
    // Task 3: Create the escrow instance on Solana
    //
    // Both the admin keypair and the instance-seed keypair must sign.
    // ------------------------------------------------------------------
    let t3 = Instant::now();
    let create_sig = 'send: {
        let mut last_err = String::new();
        for (attempt, &delay_secs) in send_retry_delays.iter().enumerate() {
            match rpc.get_latest_blockhash().await {
                Err(e) => {
                    warn!(attempt, err = %e,
                        "get_latest_blockhash failed (create_instance), retrying in {delay_secs}s");
                    last_err = e.to_string();
                }
                Ok(blockhash) => {
                    let create_ix = CreateInstance {
                        payer: admin_keypair.pubkey(),
                        admin: admin_keypair.pubkey(),
                        instance_seed: instance_seed_pubkey,
                        instance: instance_pda,
                        system_program: program::id(),
                        event_authority,
                        private_channel_escrow_program: PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
                    }
                    .instruction(CreateInstanceInstructionArgs {
                        bump: instance_bump,
                    });
                    let tx = Transaction::new_signed_with_payer(
                        &[create_ix],
                        Some(&admin_keypair.pubkey()),
                        &[admin_keypair.as_ref(), &instance_seed_keypair],
                        blockhash,
                    );
                    match rpc.send_transaction(&tx).await {
                        Ok(sig) => break 'send sig,
                        Err(e) => {
                            warn!(attempt, err = %e, "create_instance send failed, retrying");
                            last_err = e.to_string();
                        }
                    }
                }
            }
            tokio::time::sleep(Duration::from_secs(delay_secs)).await;
        }
        return Err(anyhow::anyhow!(
            "create_instance: all retries exhausted: {last_err}"
        ));
    };
    let retry = poll_confirmations(&rpc, &[Some(create_sig)], "create_instance", 0, 1).await?;
    if !retry.is_empty() {
        return Err(anyhow::anyhow!(
            "create_instance failed to confirm on-chain"
        ));
    }
    info!(
        %instance_pda,
        elapsed_ms = t3.elapsed().as_millis(),
        "Escrow instance created on Solana",
    );

    // ------------------------------------------------------------------
    // Task 4: Generate N depositor keypairs (parallel)
    // ------------------------------------------------------------------
    let t4 = Instant::now();
    let keypairs: Vec<Arc<Keypair>> = (0..num_accounts)
        .into_par_iter()
        .map(|_| Arc::new(Keypair::new()))
        .collect();
    info!(
        count = keypairs.len(),
        elapsed_ms = t4.elapsed().as_millis(),
        "Generated depositor keypairs",
    );

    // ------------------------------------------------------------------
    // Task 5: Fund depositors with SOL from admin
    //
    // Each depositor needs enough lamports to cover:
    //   - ATA rent (~0.002 SOL)
    //   - transaction fees for deposit instructions
    // We send 0.01 SOL per depositor which is generous.
    // ------------------------------------------------------------------
    let t5 = Instant::now();
    let lamports_per_account: u64 = 10_000_000; // 0.01 SOL
    let total = keypairs.len();

    let mut to_fund: Vec<Arc<Keypair>> = keypairs.clone();
    let mut batch_num = 0usize;
    let mut confirmed_so_far = 0usize;

    while !to_fund.is_empty() {
        let mut next_round: Vec<Arc<Keypair>> = Vec::new();
        for batch in to_fund.chunks(SETUP_BATCH_SIZE) {
            batch_num += 1;
            let blockhash = rpc
                .get_latest_blockhash()
                .await
                .context("get_latest_blockhash (fund)")?;
            info!(
                batch = batch_num,
                size = batch.len(),
                total,
                "Sending SOL fund batch"
            );

            let sigs = send_parallel(
                solana_rpc_url,
                batch,
                blockhash,
                "fund-sol",
                |kp, _url, bh| {
                    let admin = Arc::clone(&admin_keypair);
                    let rpc = Arc::clone(&rpc);
                    let dest = kp.pubkey();
                    async move {
                        let ix = system_instruction::transfer(
                            &admin.pubkey(),
                            &dest,
                            lamports_per_account,
                        );
                        let tx = Transaction::new_signed_with_payer(
                            &[ix],
                            Some(&admin.pubkey()),
                            &[admin.as_ref()],
                            bh,
                        );
                        rpc.send_transaction(&tx).await
                    }
                },
            )
            .await;

            let retry_indices =
                poll_confirmations(&rpc, &sigs, "fund-sol", confirmed_so_far, total).await?;
            let confirmed = batch.len() - retry_indices.len();
            confirmed_so_far += confirmed;
            for i in retry_indices {
                next_round.push(Arc::clone(&batch[i]));
            }
        }
        to_fund = next_round;
        if !to_fund.is_empty() {
            warn!(
                count = to_fund.len(),
                "Retrying failed SOL fund transactions"
            );
        }
    }

    info!(
        total,
        elapsed_ms = t5.elapsed().as_millis(),
        "All depositors funded with SOL",
    );

    // ------------------------------------------------------------------
    // Task 6: Create and initialise Solana SPL mint
    //
    // On Solana the mint account must be explicitly allocated via
    // system_program::create_account before SPL token's initialize_mint
    // can write into it.
    // ------------------------------------------------------------------
    let t6 = Instant::now();
    let mint_keypair = Keypair::new();
    let mint = mint_keypair.pubkey();
    let mint_rent = rpc
        .get_minimum_balance_for_rent_exemption(SplMint::LEN)
        .await
        .context("get_minimum_balance_for_rent_exemption (mint)")?;
    let mint_sig = 'send: {
        let mut last_err = String::new();
        for (attempt, &delay_secs) in send_retry_delays.iter().enumerate() {
            match rpc.get_latest_blockhash().await {
                Err(e) => {
                    warn!(attempt, err = %e,
                        "get_latest_blockhash failed (mint init), retrying in {delay_secs}s");
                    last_err = e.to_string();
                }
                Ok(blockhash) => {
                    let create_account_ix = system_instruction::create_account(
                        &admin_keypair.pubkey(),
                        &mint,
                        mint_rent,
                        SplMint::LEN as u64,
                        &spl_token::id(),
                    );
                    let init_mint_ix = spl_token::instruction::initialize_mint(
                        &spl_token::id(),
                        &mint,
                        &admin_keypair.pubkey(),
                        None,
                        MINT_DECIMALS,
                    )
                    .unwrap();
                    let init_tx = Transaction::new_signed_with_payer(
                        &[create_account_ix, init_mint_ix],
                        Some(&admin_keypair.pubkey()),
                        &[admin_keypair.as_ref(), &mint_keypair],
                        blockhash,
                    );
                    match rpc.send_transaction(&init_tx).await {
                        Ok(sig) => break 'send sig,
                        Err(e) => {
                            warn!(attempt, err = %e, "initialize_mint send failed, retrying");
                            last_err = e.to_string();
                        }
                    }
                }
            }
            tokio::time::sleep(Duration::from_secs(delay_secs)).await;
        }
        return Err(anyhow::anyhow!(
            "initialize_mint (deposit): all retries exhausted: {last_err}"
        ));
    };
    let retry =
        poll_confirmations(&rpc, &[Some(mint_sig)], "initialize_mint(deposit)", 0, 1).await?;
    if !retry.is_empty() {
        return Err(anyhow::anyhow!(
            "initialize_mint (deposit) failed to confirm on-chain"
        ));
    }
    info!(%mint, elapsed_ms = t6.elapsed().as_millis(), "Solana mint initialized");

    // ------------------------------------------------------------------
    // Task 6b: Initialise the same mint on PrivateChannel
    //
    // The PrivateChannel write-node creates accounts implicitly (gasless), so
    // create_admin_initialize_mint only sends the initialize_mint
    // instruction — no preceding create_account is needed.
    //
    // Without this step the operator would attempt JIT initialisation for
    // every first deposit, blocking the sender loop ~200 ms each time.
    // ------------------------------------------------------------------
    let t6b = Instant::now();
    let private_channel_rpc = RpcClient::new_with_commitment(
        private_channel_rpc_url.to_string(),
        CommitmentConfig::confirmed(),
    );
    let private_channel_mint_sig = 'send: {
        let mut last_err = String::new();
        for (attempt, &delay_secs) in send_retry_delays.iter().enumerate() {
            match private_channel_rpc.get_latest_blockhash().await {
                Err(e) => {
                    warn!(attempt, err = %e,
                        "get_latest_blockhash failed (PrivateChannel mint init), retrying in {delay_secs}s");
                    last_err = e.to_string();
                }
                Ok(blockhash) => {
                    let init_tx = create_admin_initialize_mint(
                        &admin_keypair,
                        &mint,
                        MINT_DECIMALS,
                        blockhash,
                    );
                    match private_channel_rpc.send_transaction(&init_tx).await {
                        Ok(sig) => break 'send sig,
                        Err(e) => {
                            warn!(attempt, err = %e,
                                "PrivateChannel initialize_mint send failed, retrying in {delay_secs}s");
                            last_err = e.to_string();
                        }
                    }
                }
            }
            tokio::time::sleep(Duration::from_secs(delay_secs)).await;
        }
        return Err(anyhow::anyhow!(
            "PrivateChannel initialize_mint: all retries exhausted: {last_err}"
        ));
    };
    let retry = poll_confirmations(
        &private_channel_rpc,
        &[Some(private_channel_mint_sig)],
        "initialize_mint(private_channel)",
        0,
        1,
    )
    .await?;
    if !retry.is_empty() {
        return Err(anyhow::anyhow!(
            "PrivateChannel initialize_mint failed to confirm"
        ));
    }
    info!(
        %mint,
        elapsed_ms = t6b.elapsed().as_millis(),
        "PrivateChannel mint initialized",
    );

    // ------------------------------------------------------------------
    // Task 7: AllowMint — register the mint with the escrow instance
    //
    // This single instruction creates both the allowed_mint PDA and the
    // instance ATA on-chain, so no separate ATA creation is needed for
    // the escrow account.
    // ------------------------------------------------------------------
    let t7 = Instant::now();
    let (allowed_mint_pda, allow_bump) = find_allowed_mint_pda(&instance_pda, &mint);
    let instance_ata = get_associated_token_address(&instance_pda, &mint);

    let allow_sig = 'send: {
        let mut last_err = String::new();
        for (attempt, &delay_secs) in send_retry_delays.iter().enumerate() {
            match rpc.get_latest_blockhash().await {
                Err(e) => {
                    warn!(attempt, err = %e,
                        "get_latest_blockhash failed (allow_mint), retrying in {delay_secs}s");
                    last_err = e.to_string();
                }
                Ok(blockhash) => {
                    let allow_ix = AllowMint {
                        payer: admin_keypair.pubkey(),
                        admin: admin_keypair.pubkey(),
                        instance: instance_pda,
                        mint,
                        allowed_mint: allowed_mint_pda,
                        instance_ata,
                        system_program: program::id(),
                        token_program: spl_token::id(),
                        associated_token_program: spl_associated_token_account::id(),
                        event_authority,
                        private_channel_escrow_program: PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
                    }
                    .instruction(AllowMintInstructionArgs { bump: allow_bump });
                    let tx = Transaction::new_signed_with_payer(
                        &[allow_ix],
                        Some(&admin_keypair.pubkey()),
                        &[admin_keypair.as_ref()],
                        blockhash,
                    );
                    match rpc.send_transaction(&tx).await {
                        Ok(sig) => break 'send sig,
                        Err(e) => {
                            warn!(attempt, err = %e, "allow_mint send failed, retrying");
                            last_err = e.to_string();
                        }
                    }
                }
            }
            tokio::time::sleep(Duration::from_secs(delay_secs)).await;
        }
        return Err(anyhow::anyhow!(
            "allow_mint: all retries exhausted: {last_err}"
        ));
    };
    let retry = poll_confirmations(&rpc, &[Some(allow_sig)], "allow_mint", 0, 1).await?;
    if !retry.is_empty() {
        return Err(anyhow::anyhow!("allow_mint failed to confirm on-chain"));
    }
    info!(
        %allowed_mint_pda,
        %instance_ata,
        elapsed_ms = t7.elapsed().as_millis(),
        "AllowMint confirmed — allowed_mint PDA and instance ATA created",
    );

    // ------------------------------------------------------------------
    // Task 8: Create depositor ATAs in batches
    // ------------------------------------------------------------------
    let t8 = Instant::now();
    {
        let mut to_send: Vec<Arc<Keypair>> = keypairs.clone();
        let mut batch_num = 0usize;
        let mut confirmed_so_far = 0usize;

        while !to_send.is_empty() {
            let mut next_round: Vec<Arc<Keypair>> = Vec::new();
            for batch in to_send.chunks(SETUP_BATCH_SIZE) {
                batch_num += 1;
                let blockhash = rpc
                    .get_latest_blockhash()
                    .await
                    .context("get_latest_blockhash")?;
                info!(
                    batch = batch_num,
                    size = batch.len(),
                    total,
                    "Sending depositor ATA batch",
                );

                let sigs = send_parallel(
                    solana_rpc_url,
                    batch,
                    blockhash,
                    "create-ata(deposit)",
                    |kp, _url, bh| {
                        let admin = Arc::clone(&admin_keypair);
                        let rpc = Arc::clone(&rpc);
                        let owner = kp.pubkey();
                        let m = mint;
                        async move {
                            let tx = create_ata_transaction(&admin, &owner, &m, bh);
                            rpc.send_transaction(&tx).await
                        }
                    },
                )
                .await;

                let retry_indices =
                    poll_confirmations(&rpc, &sigs, "create-ata(deposit)", confirmed_so_far, total)
                        .await?;
                let confirmed = batch.len() - retry_indices.len();
                confirmed_so_far += confirmed;
                for i in retry_indices {
                    next_round.push(Arc::clone(&batch[i]));
                }
            }
            to_send = next_round;
            if !to_send.is_empty() {
                warn!(
                    count = to_send.len(),
                    "Retrying failed depositor ATA transactions",
                );
            }
        }
    }
    // ------------------------------------------------------------------
    // Task 8 verification: confirm the first ATA is visible on-chain at
    // the Confirmed commitment level before proceeding to mint-to.
    //
    // The default RpcClient uses Finalized commitment for preflight
    // simulation.  ATAs are confirmed (Processed) after Task 8, but may
    // not be Finalized yet.  Waiting here closes the window where mint-to
    // simulation would fail with "invalid account data".
    // ------------------------------------------------------------------
    if let Some(first_kp) = keypairs.first() {
        let first_ata = get_associated_token_address(&first_kp.pubkey(), &mint);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            match rpc
                .get_account_with_commitment(&first_ata, CommitmentConfig::confirmed())
                .await
            {
                Ok(resp) if resp.value.is_some() => {
                    info!(%first_ata, "First depositor ATA visible at Confirmed; proceeding to mint-to");
                    break;
                }
                _ => {}
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(anyhow::anyhow!(
                    "Timeout waiting for first depositor ATA to appear at Confirmed level"
                ));
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    info!(
        total,
        elapsed_ms = t8.elapsed().as_millis(),
        "All depositor ATAs confirmed",
    );

    // ------------------------------------------------------------------
    // Task 9: Mint initial token balances to depositor ATAs
    //
    // We use skip_preflight=true here because the ATAs were confirmed in
    // Task 8 but may not yet be Finalized.  The default RpcClient simulates
    // against Finalized state, which would return "invalid account data"
    // even though the ATA is confirmed on-chain.  Skipping preflight lets
    // the transactions proceed; on-chain failures (if any) are still caught
    // by poll_confirmations and retried.
    // ------------------------------------------------------------------
    let t9 = Instant::now();
    {
        let mut to_send: Vec<Arc<Keypair>> = keypairs.clone();
        let mut batch_num = 0usize;
        let mut confirmed_so_far = 0usize;

        while !to_send.is_empty() {
            let mut next_round: Vec<Arc<Keypair>> = Vec::new();
            for batch in to_send.chunks(SETUP_BATCH_SIZE) {
                batch_num += 1;
                let blockhash = rpc
                    .get_latest_blockhash()
                    .await
                    .context("get_latest_blockhash")?;
                info!(
                    batch = batch_num,
                    size = batch.len(),
                    total,
                    "Sending deposit mint-to batch",
                );

                let sigs = send_parallel(
                    solana_rpc_url,
                    batch,
                    blockhash,
                    "mint-to(deposit)",
                    |kp, _url, bh| {
                        let admin = Arc::clone(&admin_keypair);
                        let rpc = Arc::clone(&rpc);
                        let ata = get_associated_token_address(&kp.pubkey(), &mint);
                        async move {
                            let tx = create_admin_mint_to(&admin, &mint, &ata, initial_balance, bh);
                            rpc.send_transaction_with_config(
                                &tx,
                                RpcSendTransactionConfig {
                                    skip_preflight: true,
                                    ..Default::default()
                                },
                            )
                            .await
                        }
                    },
                )
                .await;

                let retry_indices =
                    poll_confirmations(&rpc, &sigs, "mint-to(deposit)", confirmed_so_far, total)
                        .await?;
                let confirmed = batch.len() - retry_indices.len();
                confirmed_so_far += confirmed;
                for i in retry_indices {
                    next_round.push(Arc::clone(&batch[i]));
                }
            }
            to_send = next_round;
            if !to_send.is_empty() {
                warn!(
                    count = to_send.len(),
                    "Retrying failed deposit mint-to transactions"
                );
            }
        }
    }
    info!(
        total,
        elapsed_ms = t9.elapsed().as_millis(),
        "All deposit mint-to confirmed",
    );

    // ------------------------------------------------------------------
    // Task 9 verification: spot-check the first depositor's token balance
    // to confirm mint-to landed on-chain (at Confirmed commitment level).
    // ------------------------------------------------------------------
    if let Some(first_kp) = keypairs.first() {
        let first_ata = get_associated_token_address(&first_kp.pubkey(), &mint);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            match rpc
                .get_token_account_balance_with_commitment(
                    &first_ata,
                    CommitmentConfig::confirmed(),
                )
                .await
            {
                Ok(resp) if resp.value.amount != "0" => {
                    info!(
                        %first_ata,
                        amount = %resp.value.amount,
                        "First depositor token balance confirmed after mint-to",
                    );
                    break;
                }
                Ok(resp) => {
                    warn!(
                        %first_ata,
                        amount = %resp.value.amount,
                        "First depositor ATA has 0 tokens — mint-to may still be in flight",
                    );
                }
                Err(_) => {
                    // Account not visible yet at Confirmed level — keep polling.
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(anyhow::anyhow!(
                    "mint-to verification timed out: first depositor ATA never showed a non-zero balance"
                ));
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
    }

    // ------------------------------------------------------------------
    // Task 10: Seed BenchState with the current Solana blockhash
    // ------------------------------------------------------------------
    let t10 = Instant::now();
    let initial_blockhash = rpc
        .get_latest_blockhash()
        .await
        .context("get_latest_blockhash (seed)")?;
    let state = Arc::new(BenchState {
        current_blockhash: RwLock::new(initial_blockhash),
    });
    info!(
        blockhash = %initial_blockhash,
        elapsed_ms = t10.elapsed().as_millis(),
        "Solana blockhash seeded — deposit setup complete",
    );

    Ok(DepositConfig {
        mint,
        instance_pda,
        allowed_mint_pda,
        instance_ata,
        event_authority,
        keypairs,
        state,
    })
}
