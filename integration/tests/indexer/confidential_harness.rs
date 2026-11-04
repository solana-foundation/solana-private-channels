//! Shared end to end harness for confidential deposits and releases: one test validator
//! with Token-2022 11.1.0 and the ZK ElGamal proof program plays Solana and the channel,
//! every service runs on its own runtime so a test can kill and restart it, and every
//! proof is real.

use crate::helpers::worker::Worker;
use crate::helpers::{
    execute_user_confidential_withdrawal_to, execute_user_withdrawal_to,
    init_extra_account_meta_list, send_and_confirm_instructions, setup_wallets,
};
use crate::setup::{allow_mint_with_withdraw_fee, TestEnvironment, TEST_ADMIN_KEYPAIR};
use devnet_scripts::confidential::{
    configure_escrow, decrypt_pending, generate_keys_json, parse_keys, ConfidentialKeyPair,
};
use private_channel_confidential::{Keys, KEYS_ENV};
use private_channel_indexer::config::OperatorConfig;
use private_channel_indexer::operator::confidential_custody::{
    resolve_confidential_custody, ConfidentialCustody,
};
use private_channel_indexer::operator::escrow_sweep::ConfidentialBalances;
use private_channel_indexer::operator::utils::mint_util::{
    decode_confidential_account, ConfidentialAccountRead, ConfidentialAccountView,
};
use private_channel_indexer::storage::{PostgresDb, Storage};
use private_channel_indexer::{PostgresConfig, ReconciliationConfig};
use private_channel_withdraw_program_client::{
    instructions::SetWithdrawConfigBuilder, PRIVATE_CHANNEL_WITHDRAW_PROGRAM_ID,
    WITHDRAW_CONFIG_SEED,
};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_client::RpcClient as BlockingRpcClient;
use solana_commitment_config::CommitmentConfig;
use solana_sdk::instruction::AccountMeta;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signature, Signer};
use solana_system_interface::instruction::create_account;
use solana_test_validator::TestValidator;
use spl_associated_token_account::{
    get_associated_token_address_with_program_id,
    instruction::create_associated_token_account_idempotent,
};
use spl_tlv_account_resolution::state::ExtraAccountMetaList;
use spl_token_2022::extension::ExtensionType;
use spl_token_2022::state::Mint as Token2022Mint;
use spl_token_2022::ID as TOKEN_2022;
use spl_token_2022_interface::extension::{
    confidential_transfer::instruction as confidential_instruction,
    confidential_transfer_fee::instruction::initialize_confidential_transfer_fee_config,
    transfer_fee::instruction::initialize_transfer_fee_config,
    transfer_hook::instruction::initialize as initialize_transfer_hook,
};
use spl_transfer_hook_interface::get_extra_account_metas_address;
use sqlx::{PgPool, Row as _};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use test_utils::channel_shim::ChannelShim;
use test_utils::indexer_helper::{start_private_channel_indexer, start_solana_indexer_rpc_polling};
use test_utils::operator_helper::{
    default_operator_config, start_private_channel_to_solana_operator_with_config,
    start_solana_to_private_channel_operator_with_config,
};
use test_utils::validator_helper::{
    start_test_validator_confidential, CPI_FORWARDER_PROGRAM_ID, HOOK_FIXTURE_PROGRAM_ID,
};
use testcontainers::runners::AsyncRunner;
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres::Postgres;

pub type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

/// How long a flow may take end to end on one test validator (the withdraw indexer reads
/// finalized blocks, about 13 s behind).
pub const FLOW_TIMEOUT: Duration = Duration::from_secs(180);

static INSTANCE_KEYS: OnceLock<String> = OnceLock::new();

/// Service logs, only when `RUST_LOG` is set.
fn init_tracing() {
    use tracing_subscriber::{filter::EnvFilter, fmt, prelude::*};
    static INIT: std::sync::Once = std::sync::Once::new();
    let Ok(filter) = EnvFilter::try_from_default_env() else {
        return;
    };
    INIT.call_once(|| {
        let _ = tracing_subscriber::registry()
            .with(filter)
            .with(fmt::layer())
            .try_init();
    });
}

/// The instance keys JSON every service in this binary loads, set in the environment once.
pub fn instance_keys_json() -> String {
    INSTANCE_KEYS
        .get_or_init(|| {
            let json = generate_keys_json().to_string();
            std::env::set_var(KEYS_ENV, &json);
            json
        })
        .clone()
}

pub fn instance_keys() -> Arc<ConfidentialKeyPair> {
    Arc::new(parse_keys(&instance_keys_json()).expect("instance keys parse"))
}

/// Keys no service knows, for a mint configured under a foreign key.
pub fn foreign_keys() -> Arc<ConfidentialKeyPair> {
    Arc::new(parse_keys(&generate_keys_json()).expect("foreign keys parse"))
}

pub fn admin() -> Keypair {
    Keypair::try_from(&TEST_ADMIN_KEYPAIR[..]).expect("admin keypair")
}

/// What kind of confidential mint to create.
#[derive(Clone, Copy, Debug, Default)]
pub struct MintSpec {
    /// Basis points and maximum fee.
    pub fee: Option<(u16, u64)>,
    pub hook: Hook,
    /// Leave the escrow account unconfigured for confidential transfers.
    pub unconfigured: bool,
}

/// The transfer hook a mint runs, all on the hook fixture.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Hook {
    #[default]
    None,
    /// A validation list declaring one extra; accepts every amount.
    WithExtra,
    /// No validation list yet: Token-2022 runs it with the base accounts, and it accepts.
    BaseOnly,
}

/// A user with SOL and their own confidential keys.
pub struct User {
    pub keypair: Keypair,
    pub keys: Arc<ConfidentialKeyPair>,
}

impl User {
    pub fn pubkey(&self) -> Pubkey {
        self.keypair.pubkey()
    }
}

pub struct Env {
    pub validator: TestValidator,
    pub rpc_url: String,
    pub client: Arc<RpcClient>,
    pub db_url: String,
    pub pool: PgPool,
    pub storage: Storage,
    pub instance: Pubkey,
    pub keys: Arc<ConfidentialKeyPair>,
    faucet: Keypair,
    _pg: ContainerAsync<Postgres>,
}

impl Env {
    pub async fn start(db_name: &str) -> TestResult<Self> {
        init_tracing();
        let keys = instance_keys();
        let (validator, faucet) = start_test_validator_confidential().await;
        let rpc_url = validator.rpc_url();
        let client = Arc::new(RpcClient::new_with_commitment(
            rpc_url.clone(),
            CommitmentConfig::confirmed(),
        ));
        let pg = Postgres::default()
            .with_db_name(db_name)
            .with_user("postgres")
            .with_password("password")
            .start()
            .await?;
        let db_url = format!(
            "postgres://postgres:password@{}:{}/{db_name}",
            pg.get_host().await?,
            pg.get_host_port_ipv4(5432).await?
        );
        let pool = PgPool::connect(&db_url).await?;
        let storage = Storage::Postgres(
            PostgresDb::new(&PostgresConfig {
                database_url: db_url.clone(),
                max_connections: 10,
            })
            .await?,
        );
        storage.init_schema().await?;
        let (_, instance) = TestEnvironment::setup_instance(&client, &faucet, None).await?;
        TestEnvironment::setup_operator(&client, &faucet, instance).await?;
        Ok(Self {
            validator,
            rpc_url,
            client,
            db_url,
            pool,
            storage,
            instance,
            keys,
            faucet,
            _pg: pg,
        })
    }

    fn blocking(&self) -> BlockingRpcClient {
        BlockingRpcClient::new_with_commitment(self.rpc_url.clone(), CommitmentConfig::confirmed())
    }

    /// A confidential mint, allowed with no withdraw fee (so channel burns move no tokens
    /// through a hook or a transfer fee), its channel withdraw config, and its escrow
    /// account configured under the instance keys.
    pub async fn mint(&self, spec: MintSpec) -> TestResult<Pubkey> {
        self.mint_under(spec, self.keys.clone()).await
    }

    /// Like `mint`, with the escrow account configured under `keys`.
    pub async fn mint_under(
        &self,
        spec: MintSpec,
        keys: Arc<ConfidentialKeyPair>,
    ) -> TestResult<Pubkey> {
        let admin = admin();
        let mint = create_confidential_mint(&self.client, &admin, spec).await?;
        if spec.hook == Hook::WithExtra {
            init_extra_account_meta_list(&self.client, &admin, &mint).await?;
        }
        allow_mint_with_withdraw_fee(&self.client, &admin, self.instance, mint, TOKEN_2022, 0)
            .await?;
        set_withdraw_config(&self.client, &admin, mint).await?;
        if spec.unconfigured {
            return Ok(mint);
        }
        let (rpc, instance) = (self.blocking(), self.instance);
        tokio::task::spawn_blocking(move || {
            configure_escrow(&rpc, &admin, &instance, &mint, &keys).map_err(|e| e.to_string())
        })
        .await??;
        Ok(mint)
    }

    /// A new user with SOL and fresh confidential keys.
    pub async fn user(&self) -> TestResult<User> {
        let keypair = Keypair::new();
        setup_wallets(&self.client, &self.faucet, &[&keypair]).await?;
        Ok(User {
            keypair,
            keys: foreign_keys(),
        })
    }

    /// Mint `amount` public tokens to `user` on Solana.
    pub async fn fund(&self, user: &User, mint: Pubkey, amount: u64) -> TestResult {
        let admin = admin();
        let ata = get_associated_token_address_with_program_id(&user.pubkey(), &mint, &TOKEN_2022);
        send_and_confirm_instructions(
            &self.client,
            &[
                create_associated_token_account_idempotent(
                    &admin.pubkey(),
                    &user.pubkey(),
                    &mint,
                    &TOKEN_2022,
                ),
                spl_token_2022::instruction::mint_to(
                    &TOKEN_2022,
                    &mint,
                    &ata,
                    &admin.pubkey(),
                    &[],
                    amount,
                )?,
            ],
            &admin,
            &[&admin],
            "Fund user",
        )
        .await?;
        Ok(())
    }

    /// `ConfidentialDeposit` of `amount` from `user` (funded publicly first), credited to
    /// `user` on the channel. `via_cpi` sends it through the CPI forwarder.
    pub async fn deposit_confidential(
        &self,
        user: &User,
        mint: Pubkey,
        amount: u64,
        via_cpi: bool,
    ) -> TestResult<Signature> {
        self.fund(user, mint, amount).await?;
        let (rpc, instance) = (self.blocking(), self.instance);
        let (payer, keys) = (user.keypair.insecure_clone(), user.keys.clone());
        let extras = hook_extras(&self.client, &mint).await?;
        let forwarder = via_cpi.then_some(CPI_FORWARDER_PROGRAM_ID);
        let signature = tokio::task::spawn_blocking(move || {
            devnet_scripts::confidential_deposit::deposit(
                &rpc, &payer, &keys, instance, mint, amount, None, &extras, forwarder,
            )
            .map_err(|e| e.to_string())
        })
        .await??;
        Ok(signature)
    }

    /// Public `Deposit` of `amount` from `user`.
    pub async fn deposit_public(
        &self,
        user: &User,
        mint: Pubkey,
        amount: u64,
    ) -> TestResult<Signature> {
        use private_channel_escrow_program_client::instructions::DepositBuilder;
        self.fund(user, mint, amount).await?;
        let user_ata =
            get_associated_token_address_with_program_id(&user.pubkey(), &mint, &TOKEN_2022);
        let allowed =
            devnet_scripts::confidential::pda::allowed_wallet(&self.instance, &user.pubkey());
        let mut builder = DepositBuilder::new();
        builder
            .payer(user.pubkey())
            .user(user.pubkey())
            .instance(self.instance)
            .mint(mint)
            .allowed_mint(devnet_scripts::confidential::pda::allowed_mint(
                &self.instance,
                &mint,
            ))
            .user_ata(user_ata)
            .instance_ata(self.escrow_ata(&mint))
            .system_program(solana_system_interface::program::ID)
            .token_program(TOKEN_2022)
            .associated_token_program(spl_associated_token_account::ID)
            .event_authority(devnet_scripts::confidential::pda::event_authority())
            .private_channel_escrow_program(
                private_channel_escrow_program_client::PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
            )
            .user_allowed_wallet_pda(allowed)
            .recipient_allowed_wallet_pda(allowed)
            .amount(amount);
        builder.add_remaining_accounts(&hook_extras(&self.client, &mint).await?);
        send_and_confirm_instructions(
            &self.client,
            &[builder.instruction()],
            &user.keypair,
            &[&user.keypair],
            "Public deposit",
        )
        .await
    }

    /// Burn on the channel with `ConfidentialWithdrawFunds` (or `WithdrawFunds`) to `user`.
    pub async fn withdraw(
        &self,
        user: &User,
        mint: Pubkey,
        amount: u64,
        confidential: bool,
    ) -> TestResult<String> {
        let sent = if confidential {
            execute_user_confidential_withdrawal_to(
                &self.client,
                &user.keypair,
                mint,
                amount,
                user.pubkey(),
            )
            .await?
        } else {
            execute_user_withdrawal_to(&self.client, &user.keypair, mint, amount, user.pubkey())
                .await?
        };
        Ok(sent.signature)
    }

    pub fn escrow_ata(&self, mint: &Pubkey) -> Pubkey {
        get_associated_token_address_with_program_id(&self.instance, mint, &TOKEN_2022)
    }

    /// The escrow account's confidential state at confirmed commitment.
    pub async fn escrow_view(&self, mint: &Pubkey) -> TestResult<ConfidentialAccountView> {
        let account = self.client.get_account(&self.escrow_ata(mint)).await?;
        match decode_confidential_account(Some(&account)) {
            ConfidentialAccountRead::Configured(view) => Ok(*view),
            _ => Err("escrow account is not configured".into()),
        }
    }

    /// Reconciliation's verdict for one mint's escrow account, read now.
    pub async fn custody_verdict(&self, mint: &Pubkey) -> TestResult<ConfidentialCustody> {
        let view = self.escrow_view(mint).await?;
        let balances = ConfidentialBalances::from(&view);
        Ok(resolve_confidential_custody(
            &self.storage,
            Some(&self.keys.keys),
            mint,
            view.public_amount,
            &balances,
        )
        .await?)
    }

    /// The escrow's verified available balance (AES copy confirmed by the opening check).
    pub async fn escrow_available(&self, mint: &Pubkey) -> TestResult<Option<u64>> {
        let view = self.escrow_view(mint).await?;
        let keys = &self.keys.keys;
        Ok(keys
            .decrypt_aes(&view.decryptable_available)
            .filter(|x| keys.opens_to(&view.available, *x)))
    }

    /// The user's confidential pending balance, decrypted with their own keys.
    pub async fn user_pending(&self, user: &User, mint: &Pubkey) -> TestResult<u64> {
        let ata = get_associated_token_address_with_program_id(&user.pubkey(), mint, &TOKEN_2022);
        let data = self.client.get_account_data(&ata).await?;
        let (_, extension) = devnet_scripts::confidential::confidential_account(&data)
            .map_err(|e| e.to_string())?
            .ok_or("user account is not configured")?;
        Ok(decrypt_pending(&user.keys, &extension).map_err(|e| e.to_string())?)
    }

    /// The user's public balance.
    pub async fn user_public(&self, user: &User, mint: &Pubkey) -> TestResult<u64> {
        let ata = get_associated_token_address_with_program_id(&user.pubkey(), mint, &TOKEN_2022);
        Ok(self
            .client
            .get_token_account_balance(&ata)
            .await?
            .amount
            .parse()?)
    }

    /// One manual reconciliation, as `indexer reconcile` runs it.
    /// Custody against the ledger with zero tolerance, as the indexer checks it at boot (the
    /// same judgement as manual and runtime reconciliation), with the escrow indexer paused
    /// so the ledger is still. It indexes at confirmed, and custody is read finalized at or
    /// past its checkpoint, so the read is retried until finalization catches up.
    pub async fn reconcile(&self, stack: &mut Stack) -> TestResult {
        let running = stack.is_running(Service::EscrowIndexer);
        stack.stop_service(Service::EscrowIndexer).await;
        let verdict = self.reconcile_still(&stack.channel.url()).await;
        if running {
            stack.start_service(Service::EscrowIndexer).await;
        }
        verdict
    }

    /// `reconcile` for a test that already stopped the escrow indexer.
    pub async fn reconcile_still(&self, channel_url: &str) -> TestResult {
        let config = ReconciliationConfig {
            mismatch_threshold_raw: 0,
            ..Default::default()
        };
        let deadline = Instant::now() + FLOW_TIMEOUT;
        loop {
            let verdict =
                private_channel_indexer::indexer::reconciliation::run_startup_reconciliation(
                    &config,
                    private_channel_indexer::ProgramType::Escrow,
                    &self.storage,
                    &self.rpc_url,
                    Some(channel_url),
                    &self.instance,
                    Some(&self.keys.keys),
                )
                .await;
            match verdict {
                Ok(()) => return Ok(()),
                Err(e) if Instant::now() < deadline && is_lag(&format!("{e} {e:?}")) => {
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
                Err(e) => return Err(e.into()),
            }
        }
    }

    /// Wait for the row with `signature` to reach `status`; returns the row.
    pub async fn wait_status(&self, signature: &str, status: &str) -> TestResult<Row> {
        wait_until(&format!("{signature} to reach {status}"), || async {
            Ok(self
                .row(signature)
                .await?
                .filter(|row| row.status == status))
        })
        .await
    }

    pub async fn row(&self, signature: &str) -> TestResult<Option<Row>> {
        let row = sqlx::query(
            "SELECT id, transaction_type::text AS kind, status::text AS status, amount::text AS amount, \
             confidential, confidential_undecrypted, withdrawal_nonce, counterpart_signature, mint \
             FROM transactions WHERE signature = $1",
        )
        .bind(signature)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| Row {
            id: r.get("id"),
            kind: r.get("kind"),
            status: r.get("status"),
            amount: r.get::<String, _>("amount").parse().unwrap_or(0),
            confidential: r.get("confidential"),
            undecrypted: r.get("confidential_undecrypted"),
            nonce: r.get("withdrawal_nonce"),
            counterpart: r.get("counterpart_signature"),
            mint: r.get("mint"),
        }))
    }

    /// The observed release for `nonce`: signature and amount (None while undecrypted).
    pub async fn observed_release(&self, nonce: i64) -> TestResult<Option<(String, Option<u64>)>> {
        let row = sqlx::query(
            "SELECT signature, amount::text AS amount FROM observed_releases WHERE withdrawal_nonce = $1",
        )
        .bind(nonce)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| {
            (
                r.get::<String, _>("signature"),
                r.get::<Option<String>, _>("amount")
                    .and_then(|a| a.parse().ok()),
            )
        }))
    }

    /// Releases that landed on Solana for `nonce`, counted from the escrow's history.
    pub async fn landed_releases(&self, nonce: i64) -> TestResult<usize> {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM observed_releases WHERE withdrawal_nonce = $1",
        )
        .bind(nonce)
        .fetch_one(&self.pool)
        .await?;
        Ok(count as usize)
    }
}

/// A reconciliation refusal that only means the reads are not caught up yet.
fn is_lag(error: &str) -> bool {
    [
        "CustodyStale",
        "CustodyBehindLedger",
        "CustodySlotUnsettled",
        "Minimum context slot",
        "not been reached",
    ]
    .iter()
    .any(|needle| error.contains(needle))
}

/// The columns these tests read from `transactions`.
#[derive(Debug, Clone)]
pub struct Row {
    pub id: i64,
    pub kind: String,
    pub status: String,
    pub amount: u64,
    pub confidential: bool,
    pub undecrypted: bool,
    pub nonce: Option<i64>,
    pub counterpart: Option<String>,
    pub mint: String,
}

/// Poll `check` until it yields a value or `FLOW_TIMEOUT` passes.
pub async fn wait_until<T, F, Fut>(what: &str, mut check: F) -> TestResult<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = TestResult<Option<T>>>,
{
    let deadline = Instant::now() + FLOW_TIMEOUT;
    loop {
        if let Some(value) = check().await? {
            return Ok(value);
        }
        if Instant::now() >= deadline {
            return Err(format!("timed out waiting for {what}").into());
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

/// The hook accounts a transfer of `mint` needs: declared extra, hook program, validation
/// account. Empty for a mint without a hook.
pub async fn hook_extras(client: &RpcClient, mint: &Pubkey) -> TestResult<Vec<AccountMeta>> {
    use spl_token_2022::extension::{
        transfer_hook::TransferHook, BaseStateWithExtensions, StateWithExtensions,
    };
    let data = client.get_account_data(mint).await?;
    let state = StateWithExtensions::<Token2022Mint>::unpack(&data)?;
    let hooked = state
        .get_extension::<TransferHook>()
        .is_ok_and(|hook| Option::<Pubkey>::from(hook.program_id).is_some());
    if !hooked {
        return Ok(Vec::new());
    }
    let hook = AccountMeta::new_readonly(HOOK_FIXTURE_PROGRAM_ID, false);
    let validation = get_extra_account_metas_address(mint, &HOOK_FIXTURE_PROGRAM_ID);
    let Ok(list) = client.get_account_data(&validation).await else {
        // No list: Token-2022 runs the hook with the base accounts.
        return Ok(vec![hook]);
    };
    let validation = AccountMeta::new_readonly(validation, false);
    if list.len() == ExtraAccountMetaList::size_of(0)? {
        return Ok(vec![hook, validation]);
    }
    Ok(vec![
        AccountMeta::new_readonly(solana_system_interface::program::ID, false),
        hook,
        validation,
    ])
}

/// A Token-2022 mint with confidential transfers (auto approve, an auditor), the admin as
/// mint authority and permanent delegate, and optionally a transfer fee and the hook fixture.
pub async fn create_confidential_mint(
    client: &RpcClient,
    admin: &Keypair,
    spec: MintSpec,
) -> TestResult<Pubkey> {
    let mint = Keypair::new();
    let auditor = foreign_keys();
    let withheld = foreign_keys();
    let mut extensions = vec![
        ExtensionType::ConfidentialTransferMint,
        ExtensionType::PermanentDelegate,
    ];
    if spec.fee.is_some() {
        extensions.push(ExtensionType::TransferFeeConfig);
        extensions.push(ExtensionType::ConfidentialTransferFeeConfig);
    }
    if spec.hook != Hook::None {
        extensions.push(ExtensionType::TransferHook);
    }
    let space = ExtensionType::try_calculate_account_len::<Token2022Mint>(&extensions)?;
    let rent = client.get_minimum_balance_for_rent_exemption(space).await?;
    let mut instructions = vec![
        create_account(
            &admin.pubkey(),
            &mint.pubkey(),
            rent,
            space as u64,
            &TOKEN_2022,
        ),
        confidential_instruction::initialize_mint(
            &TOKEN_2022,
            &mint.pubkey(),
            Some(admin.pubkey()),
            true,
            Some((*auditor.elgamal.pubkey()).into()),
        )?,
        spl_token_2022::instruction::initialize_permanent_delegate(
            &TOKEN_2022,
            &mint.pubkey(),
            &admin.pubkey(),
        )?,
    ];
    if let Some((basis_points, maximum_fee)) = spec.fee {
        instructions.push(initialize_transfer_fee_config(
            &TOKEN_2022,
            &mint.pubkey(),
            Some(&admin.pubkey()),
            Some(&admin.pubkey()),
            basis_points,
            maximum_fee,
        )?);
        instructions.push(initialize_confidential_transfer_fee_config(
            &TOKEN_2022,
            &mint.pubkey(),
            Some(admin.pubkey()),
            &(*withheld.elgamal.pubkey()).into(),
        )?);
    }
    if spec.hook != Hook::None {
        instructions.push(initialize_transfer_hook(
            &TOKEN_2022,
            &mint.pubkey(),
            Some(admin.pubkey()),
            Some(HOOK_FIXTURE_PROGRAM_ID),
        )?);
    }
    instructions.push(spl_token_2022::instruction::initialize_mint2(
        &TOKEN_2022,
        &mint.pubkey(),
        &admin.pubkey(),
        Some(&admin.pubkey()),
        6,
    )?);
    send_and_confirm_instructions(
        client,
        &instructions,
        admin,
        &[admin, &mint],
        "Confidential mint",
    )
    .await?;
    Ok(mint.pubkey())
}

/// The channel withdraw config the operator would write on the first deposit, with no fee.
async fn set_withdraw_config(client: &RpcClient, admin: &Keypair, mint: Pubkey) -> TestResult {
    let (withdraw_config, _) = Pubkey::find_program_address(
        &[WITHDRAW_CONFIG_SEED, mint.as_ref()],
        &PRIVATE_CHANNEL_WITHDRAW_PROGRAM_ID,
    );
    send_and_confirm_instructions(
        client,
        &[
            create_associated_token_account_idempotent(
                &admin.pubkey(),
                &admin.pubkey(),
                &mint,
                &TOKEN_2022,
            ),
            SetWithdrawConfigBuilder::new()
                .authority(admin.pubkey())
                .mint(mint)
                .withdraw_config(withdraw_config)
                .fee(0)
                .allow_mint_slot(0)
                .treasury(admin.pubkey())
                .min_withdraw_amount(1)
                .instruction(),
        ],
        admin,
        &[admin],
        "Set withdraw config",
    )
    .await?;
    Ok(())
}

/// The confirmation poll the resync e2e uses: this validator confirms in seconds.
pub fn e2e_operator_config() -> OperatorConfig {
    OperatorConfig {
        confirmation_poll_interval_ms: 4_000,
        ..default_operator_config()
    }
}

/// Which service of the stack.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Service {
    EscrowIndexer,
    WithdrawIndexer,
    EscrowOperator,
    WithdrawOperator,
}

/// Both indexers and both operators on one validator, each stoppable like a process.
pub struct Stack {
    pub alerts: Option<Alerts>,
    rpc_url: String,
    db_url: String,
    instance: Pubkey,
    pub channel: ChannelShim,
    alert_url: Option<String>,
    workers: Vec<(Service, Worker)>,
}

impl Stack {
    /// Started one at a time: concurrent schema creation races in Postgres. Operator alerts
    /// go to a local sink so a test can see which label a row parked with.
    pub async fn start(env: &Env) -> Self {
        let alerts = Alerts::start().await;
        let alert_url = Some(alerts.url());
        let channel = ChannelShim::start_mirroring_confidential_custody(
            &env.rpc_url,
            env.instance,
            Arc::new(Keys::from_json(&instance_keys_json()).expect("instance keys")),
        )
        .await;
        let mut stack = Self {
            alerts: Some(alerts),
            rpc_url: env.rpc_url.clone(),
            db_url: env.db_url.clone(),
            instance: env.instance,
            channel,
            alert_url,
            workers: Vec::new(),
        };
        for service in [
            Service::EscrowIndexer,
            Service::WithdrawIndexer,
            Service::EscrowOperator,
            Service::WithdrawOperator,
        ] {
            stack.start_service(service).await;
        }
        stack
    }

    pub async fn start_service(&mut self, service: Service) {
        let (rpc, db, instance) = (self.rpc_url.clone(), self.db_url.clone(), self.instance);
        let channel = self.channel.url();
        let config = OperatorConfig {
            alert_webhook_url: self.alert_url.clone(),
            ..e2e_operator_config()
        };
        let worker = match service {
            Service::EscrowIndexer => {
                Worker::spawn(move || async move {
                    start_solana_indexer_rpc_polling(rpc, db, Some(instance))
                        .await
                        .expect("start the escrow indexer");
                })
                .await
            }
            Service::WithdrawIndexer => {
                Worker::spawn(move || async move {
                    start_private_channel_indexer(rpc, db)
                        .await
                        .expect("start the withdraw indexer");
                })
                .await
            }
            Service::EscrowOperator => {
                Worker::spawn(move || async move {
                    start_solana_to_private_channel_operator_with_config(
                        channel,
                        rpc,
                        db,
                        admin(),
                        instance,
                        config,
                    )
                    .await
                    .expect("start the escrow operator");
                    // The helper returns before the operator creates its schema.
                    tokio::time::sleep(Duration::from_millis(500)).await;
                })
                .await
            }
            Service::WithdrawOperator => {
                Worker::spawn(move || async move {
                    start_private_channel_to_solana_operator_with_config(
                        rpc,
                        channel,
                        db,
                        admin(),
                        instance,
                        config,
                    )
                    .await
                    .expect("start the withdraw operator");
                    tokio::time::sleep(Duration::from_millis(500)).await;
                })
                .await
            }
        };
        self.workers.push((service, worker));
    }

    /// Stop `service` like a process exit: every task on its runtime ends.
    pub async fn stop_service(&mut self, service: Service) {
        if let Some(i) = self.workers.iter().position(|(s, _)| *s == service) {
            let (_, worker) = self.workers.remove(i);
            worker.stop().await;
        }
    }

    /// Wait for an operator alert mentioning `label`.
    pub async fn wait_alert(&mut self, label: &str) -> TestResult {
        self.alerts
            .as_mut()
            .ok_or("no alert sink")?
            .wait_for(label)
            .await
    }

    pub fn is_running(&self, service: Service) -> bool {
        self.workers.iter().any(|(s, _)| *s == service)
    }

    /// Stop every service, as before an offline maintenance command.
    pub async fn stop_all(&mut self) {
        while let Some((_, worker)) = self.workers.pop() {
            worker.stop().await;
        }
    }

    pub async fn start_all(&mut self) {
        for service in [
            Service::EscrowIndexer,
            Service::WithdrawIndexer,
            Service::EscrowOperator,
            Service::WithdrawOperator,
        ] {
            if !self.is_running(service) {
                self.start_service(service).await;
            }
        }
    }

    pub async fn restart_service(&mut self, service: Service) {
        self.stop_service(service).await;
        self.start_service(service).await;
    }

    pub async fn stop(mut self) {
        while let Some((_, worker)) = self.workers.pop() {
            worker.stop().await;
        }
    }
}

/// Operator alerts, captured to find which label a row parked with.
pub struct Alerts {
    server: mockito::ServerGuard,
    _any: mockito::Mock,
}

impl Alerts {
    pub async fn start() -> Self {
        let mut server = mockito::Server::new_async().await;
        let any = server
            .mock("POST", "/")
            .with_status(200)
            .expect_at_least(0)
            .create_async()
            .await;
        Self { server, _any: any }
    }

    pub fn url(&self) -> String {
        self.server.url()
    }

    /// Wait for an alert whose body mentions `needle`.
    pub async fn wait_for(&mut self, needle: &str) -> TestResult {
        let mock = self
            .server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(regex_escape(needle)))
            .with_status(200)
            .expect_at_least(1)
            .create_async()
            .await;
        // Alerts already delivered are not replayed, so also accept the row's state.
        let deadline = Instant::now() + FLOW_TIMEOUT;
        while !mock.matched_async().await {
            if Instant::now() >= deadline {
                return Err(format!("no alert mentioning {needle}").into());
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
        Ok(())
    }
}

fn regex_escape(text: &str) -> String {
    text.chars()
        .flat_map(|c| {
            let escape = "\\.+*?()|[]{}^$".contains(c);
            escape.then_some('\\').into_iter().chain(std::iter::once(c))
        })
        .collect()
}

impl Env {
    /// Configure `user`'s account of `mint` for confidential transfers.
    pub async fn configure_user_account(&self, user: &User, mint: Pubkey) -> TestResult {
        let rpc = self.blocking();
        let (payer, keys) = (user.keypair.insecure_clone(), user.keys.clone());
        tokio::task::spawn_blocking(move || {
            let ata =
                get_associated_token_address_with_program_id(&payer.pubkey(), &mint, &TOKEN_2022);
            devnet_scripts::confidential_deposit::ensure_configured(
                &rpc, &payer, &keys, &mint, &ata, false,
            )
            .map_err(|e| e.to_string())
        })
        .await??;
        Ok(())
    }

    /// Record a rebalance request as `indexer rebalance` does; returns its id.
    pub async fn request_rebalance(
        &self,
        mint: Pubkey,
        to_public: bool,
        amount: u64,
    ) -> TestResult<i64> {
        use private_channel_indexer::storage::common::storage::confidential_sender::RebalanceDirection;
        let rpc = private_channel_indexer::operator::RpcClientWithRetry::with_retry_config(
            self.rpc_url.clone(),
            Default::default(),
            CommitmentConfig::confirmed(),
        );
        let direction = if to_public {
            RebalanceDirection::ToPublic
        } else {
            RebalanceDirection::ToConfidential
        };
        Ok(
            private_channel_indexer::operator::sender::confidential::request_rebalance(
                &self.storage,
                &rpc,
                &self.keys.keys,
                &self.instance,
                &mint,
                direction,
                amount,
            )
            .await?,
        )
    }

    /// Wait for a rebalance request to reach `status`.
    pub async fn wait_rebalance(&self, id: i64, status: &str) -> TestResult {
        wait_until(&format!("rebalance {id} to reach {status}"), || async {
            let current: String =
                sqlx::query_scalar("SELECT status FROM rebalance_requests WHERE id = $1")
                    .bind(id)
                    .fetch_one(&self.pool)
                    .await?;
            Ok((current == status).then_some(()))
        })
        .await
    }

    /// The runbook's re-arm of a parked row; `publicly` also clears `confidential`.
    pub async fn rearm(&self, id: i64, publicly: bool) -> TestResult {
        let clear = if publicly {
            "confidential = FALSE,"
        } else {
            ""
        };
        let updated = sqlx::query(&format!(
            "UPDATE transactions SET {clear} status = 'pending', recovery_requeue_attempts = 0, \
             not_before = NULL, wait_started_at = NULL, confidential_park_label = NULL, \
             updated_at = NOW() \
             WHERE id = $1 AND status = 'manual_review' AND landed_remint_signature IS NULL"
        ))
        .bind(id)
        .execute(&self.pool)
        .await?
        .rows_affected();
        if updated != 1 {
            return Err(format!("row {id} was not parked").into());
        }
        Ok(())
    }

    /// Journaled release attempts of the row `id`.
    pub async fn journal(&self, id: i64) -> TestResult<Vec<String>> {
        Ok(sqlx::query_scalar(
            "SELECT signature FROM pending_release_signatures WHERE transaction_id = $1 ORDER BY id",
        )
        .bind(id)
        .fetch_all(&self.pool)
        .await?)
    }

    /// Rows that were reminted or left for review: none in a healthy run.
    pub async fn reminted_or_review(&self) -> TestResult<i64> {
        Ok(sqlx::query_scalar(
            "SELECT COUNT(*) FROM transactions WHERE status::text IN \
             ('failed_reminted', 'pending_remint', 'manual_review', 'failed')",
        )
        .fetch_one(&self.pool)
        .await?)
    }

    /// Reproduce a deposit racing an apply: read the escrow, let `depositor` deposit, then
    /// apply as the operator with the counter and AES value from the earlier read.
    pub async fn raced_apply(&self, mint: Pubkey, depositor: &User, amount: u64) -> TestResult {
        use private_channel_indexer::operator::utils::instruction_util::{
            apply_pending_balance_instruction, EscrowConfidentialAccounts,
        };
        use private_channel_indexer::operator::{find_event_authority_pda, find_operator_pda};
        let view = self.escrow_view(&mint).await?;
        let keys = &self.keys.keys;
        let available = keys
            .decrypt_aes(&view.decryptable_available)
            .filter(|x| keys.opens_to(&view.available, *x))
            .ok_or("escrow AES copy must verify before the race")?;
        let pending = keys.decrypt_pending(&view.pending_lo, &view.pending_hi)?;
        self.deposit_confidential(depositor, mint, amount, false)
            .await?;
        let operator = admin();
        let accounts = EscrowConfidentialAccounts {
            payer: operator.pubkey(),
            operator: operator.pubkey(),
            instance: self.instance,
            operator_pda: find_operator_pda(&self.instance, &operator.pubkey()),
            mint,
            instance_ata: self.escrow_ata(&mint),
            event_authority: find_event_authority_pda(),
        };
        let apply = apply_pending_balance_instruction(
            &accounts,
            view.pending_credit_counter,
            keys.encrypt_aes(available + pending),
        );
        send_and_confirm_instructions(
            &self.client,
            &[apply],
            &operator,
            &[&operator],
            "Raced apply",
        )
        .await?;
        let after = self.escrow_view(&mint).await?;
        if !after.aes_known_stale() {
            return Err("the apply did not race the deposit".into());
        }
        Ok(())
    }
}
