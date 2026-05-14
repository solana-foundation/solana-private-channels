use {
    axum::http::{HeaderValue, StatusCode},
    axum::{
        extract::{
            ws::{Message, WebSocket, WebSocketUpgrade},
            State,
        },
        response::IntoResponse,
        routing::get,
        Router,
    },
    clap::Parser,
    private_channel_core::accounts::{
        traits::{AccountsDB, BlockInfo},
        types::StoredTransaction,
    },
    serde::Serialize,
    solana_sdk::{message::VersionedMessage, pubkey::Pubkey, signature::Signature},
    solana_transaction_status_client_types::option_serializer::OptionSerializer,
    sqlx::{postgres::PgPoolOptions, FromRow, PgPool},
    std::{
        net::SocketAddr,
        sync::{
            atomic::{AtomicI64, Ordering},
            Arc,
        },
        time::{Duration, SystemTime, UNIX_EPOCH},
    },
    tokio::{signal, sync::broadcast},
    tower_http::cors::{AllowOrigin, Any, CorsLayer},
    tracing::{debug, error, info, warn},
};

/// /health is healthy if every running poller has updated its heartbeat within this window.
const HEALTH_STALE_THRESHOLD: i64 = 30;

fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Escrow program discriminators (matches admin-ui/src/hooks/useActivityFeed.ts)
// ---------------------------------------------------------------------------
const DISC_CREATE_INSTANCE: u8 = 0;
const DISC_ALLOW_MINT: u8 = 1;
const DISC_BLOCK_MINT: u8 = 2;
const DISC_ADD_OPERATOR: u8 = 3;
const DISC_REMOVE_OPERATOR: u8 = 4;
const DISC_SET_NEW_ADMIN: u8 = 5;
const DISC_DEPOSIT: u8 = 6;
const DISC_RELEASE_FUNDS: u8 = 7;
const DISC_RESET_SMT: u8 = 8;

/// Known program IDs
const ESCROW_PROGRAM_ID: &str = "GokvZqD2yP696rzNBNbQvcZ4VsLW7jNvFXU1kW9m7k83";
const SPL_TOKEN_PROGRAM_ID: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";

// ---------------------------------------------------------------------------
// Streamed transaction — JSON matches frontend `ActivityTransaction` type
// ---------------------------------------------------------------------------
#[derive(Serialize, Clone, Debug)]
struct StreamedTransaction {
    signature: String,
    chain: String,
    #[serde(rename = "type")]
    tx_type: String,
    from: String,
    to: String,
    amount: Option<String>,
    mint: Option<String>,
    timestamp: i64,
    status: String,
    slot: u64,
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------
#[derive(Parser, Debug)]
#[command(
    name = "private-channel-streamer",
    about = "WebSocket streamer for real-time PrivateChannel transactions"
)]
struct Args {
    /// Port to listen on
    #[arg(short, long, env = "STREAMER_PORT")]
    port: Option<u16>,

    /// PostgreSQL connection URL (PrivateChannel read replica — for mint/burn/transfer)
    #[arg(long, env = "STREAMER_ACCOUNTSDB_CONNECTION_URL")]
    accountsdb_connection_url: String,

    /// Indexer PostgreSQL connection URL (for escrow deposits/withdrawals)
    #[arg(long, env = "STREAMER_DATABASE_URL")]
    database_url: Option<String>,

    /// Poll interval in milliseconds
    #[arg(long, default_value_t = 700, env = "STREAMER_POLL_INTERVAL_MS")]
    poll_interval_ms: u64,

    /// CORS allowed origin
    #[arg(long, default_value = "*", env = "STREAMER_CORS_ALLOWED_ORIGIN")]
    cors_allowed_origin: String,

    /// Log level
    #[arg(long, default_value = "info", env = "STREAMER_LOG_LEVEL")]
    log_level: String,

    /// Enable JSON logging
    #[arg(long, env = "STREAMER_JSON_LOGS")]
    json_logs: bool,
}

// ---------------------------------------------------------------------------
// Shared state
// ---------------------------------------------------------------------------
struct AppState {
    tx_sender: broadcast::Sender<String>,
    /// Per-poller liveness heartbeats — Unix seconds of the last successful poll iteration.
    accounts_poll_at: Arc<AtomicI64>,
    /// `None` when STREAMER_DATABASE_URL is unset and the indexer poller is disabled.
    indexer_poll_at: Option<Arc<AtomicI64>>,
}

// ---------------------------------------------------------------------------
// Instruction parsing helpers
// ---------------------------------------------------------------------------

fn disc_to_type(disc: u8) -> &'static str {
    match disc {
        DISC_CREATE_INSTANCE => "create_instance",
        DISC_ALLOW_MINT => "allow_mint",
        DISC_BLOCK_MINT => "block_mint",
        DISC_ADD_OPERATOR => "add_operator",
        DISC_REMOVE_OPERATOR => "remove_operator",
        DISC_SET_NEW_ADMIN => "set_admin",
        DISC_DEPOSIT => "deposit",
        DISC_RELEASE_FUNDS => "release",
        DISC_RESET_SMT => "reset_smt",
        _ => "unknown",
    }
}

/// Resolve an account key from instruction account indices.
fn resolve_key(account_keys: &[Pubkey], ix_accounts: &[u8], idx: usize) -> String {
    ix_accounts
        .get(idx)
        .and_then(|&i| account_keys.get(i as usize))
        .map(|k| k.to_string())
        .unwrap_or_default()
}

struct ParsedInstruction {
    tx_type: String,
    from: String,
    to: String,
    amount: Option<String>,
    mint: Option<String>,
}

/// Parse an escrow program instruction.
///
/// Account layouts (from useActivityFeed.ts):
///   Deposit:  [payer, user, instance, mint, allowedMint, userAta, instanceAta, ...]
///   Release:  [payer, operator, instance, operatorPda, mint, allowedMint, userAta, instanceAta, ...]
fn parse_escrow_instruction(
    data: &[u8],
    account_keys: &[Pubkey],
    ix_accounts: &[u8],
) -> ParsedInstruction {
    if data.is_empty() {
        return ParsedInstruction {
            tx_type: "unknown".into(),
            from: String::new(),
            to: String::new(),
            amount: None,
            mint: None,
        };
    }

    let disc = data[0];
    let tx_type = disc_to_type(disc).to_string();

    let from;
    let mut to = String::new();
    let mut amount: Option<String> = None;
    let mut mint: Option<String> = None;

    match disc {
        DISC_DEPOSIT => {
            // data: [disc(1), amount(8), ...]
            if data.len() >= 9 {
                let amt = u64::from_le_bytes(data[1..9].try_into().unwrap_or([0u8; 8]));
                amount = Some(amt.to_string());
            }
            from = resolve_key(account_keys, ix_accounts, 1); // user
            to = resolve_key(account_keys, ix_accounts, 2); // instance
            mint = Some(resolve_key(account_keys, ix_accounts, 3));
        }
        DISC_RELEASE_FUNDS => {
            // data: [disc(1), amount(8), user(32), ...]
            if data.len() >= 9 {
                let amt = u64::from_le_bytes(data[1..9].try_into().unwrap_or([0u8; 8]));
                amount = Some(amt.to_string());
            }
            if data.len() >= 41 {
                if let Ok(user_key) = Pubkey::try_from(&data[9..41]) {
                    to = user_key.to_string();
                }
            }
            from = resolve_key(account_keys, ix_accounts, 1); // operator
            mint = Some(resolve_key(account_keys, ix_accounts, 4));
        }
        _ => {
            // Admin-type instructions: payer is from, subject is to
            from = resolve_key(account_keys, ix_accounts, 0);
            if ix_accounts.len() > 1 {
                to = resolve_key(account_keys, ix_accounts, 1);
            }
        }
    }

    ParsedInstruction {
        tx_type,
        from,
        to,
        amount,
        mint,
    }
}

/// Parse an SPL Token program instruction.
///
/// Supported: Transfer(3), MintTo(7), Burn(8), TransferChecked(12), MintToChecked(14), BurnChecked(15)
fn parse_spl_token_instruction(
    data: &[u8],
    account_keys: &[Pubkey],
    ix_accounts: &[u8],
    pre_token_balances: &Option<
        Vec<solana_transaction_status_client_types::UiTransactionTokenBalance>,
    >,
) -> Option<ParsedInstruction> {
    if data.is_empty() {
        return None;
    }

    let disc = data[0];
    match disc {
        // Transfer: disc=3, [source, dest, authority], data: [disc(1), amount(8)]
        3 if data.len() >= 9 => {
            let amt = u64::from_le_bytes(data[1..9].try_into().unwrap_or([0u8; 8]));
            let from = resolve_key(account_keys, ix_accounts, 2); // authority
            let to = resolve_key(account_keys, ix_accounts, 1); // destination
            let mint = pre_token_balances
                .as_ref()
                .and_then(|b| b.first())
                .map(|b| b.mint.clone());
            Some(ParsedInstruction {
                tx_type: "transfer".into(),
                from,
                to,
                amount: Some(amt.to_string()),
                mint,
            })
        }
        // MintTo: disc=7, [mint, dest, authority], data: [disc(1), amount(8)]
        7 if data.len() >= 9 => {
            let amt = u64::from_le_bytes(data[1..9].try_into().unwrap_or([0u8; 8]));
            let from = resolve_key(account_keys, ix_accounts, 2); // mint authority
            let to = resolve_key(account_keys, ix_accounts, 1); // destination token account
            let mint = Some(resolve_key(account_keys, ix_accounts, 0)); // mint
            Some(ParsedInstruction {
                tx_type: "mint_to".into(),
                from,
                to,
                amount: Some(amt.to_string()),
                mint,
            })
        }
        // Burn: disc=8, [source, mint, authority], data: [disc(1), amount(8)]
        8 if data.len() >= 9 => {
            let amt = u64::from_le_bytes(data[1..9].try_into().unwrap_or([0u8; 8]));
            let from = resolve_key(account_keys, ix_accounts, 2); // authority (burner)
            let to = resolve_key(account_keys, ix_accounts, 0); // source token account
            let mint = Some(resolve_key(account_keys, ix_accounts, 1)); // mint
            Some(ParsedInstruction {
                tx_type: "burn".into(),
                from,
                to,
                amount: Some(amt.to_string()),
                mint,
            })
        }
        // TransferChecked: disc=12, [source, mint, dest, authority], data: [disc(1), amount(8), decimals(1)]
        12 if data.len() >= 10 => {
            let amt = u64::from_le_bytes(data[1..9].try_into().unwrap_or([0u8; 8]));
            let from = resolve_key(account_keys, ix_accounts, 3); // authority
            let to = resolve_key(account_keys, ix_accounts, 2); // destination
            let mint = Some(resolve_key(account_keys, ix_accounts, 1));
            Some(ParsedInstruction {
                tx_type: "transfer".into(),
                from,
                to,
                amount: Some(amt.to_string()),
                mint,
            })
        }
        // MintToChecked: disc=14, [mint, dest, authority], data: [disc(1), amount(8), decimals(1)]
        14 if data.len() >= 10 => {
            let amt = u64::from_le_bytes(data[1..9].try_into().unwrap_or([0u8; 8]));
            let from = resolve_key(account_keys, ix_accounts, 2); // mint authority
            let to = resolve_key(account_keys, ix_accounts, 1); // destination token account
            let mint = Some(resolve_key(account_keys, ix_accounts, 0)); // mint
            Some(ParsedInstruction {
                tx_type: "mint_to".into(),
                from,
                to,
                amount: Some(amt.to_string()),
                mint,
            })
        }
        // BurnChecked: disc=15, [source, mint, authority], data: [disc(1), amount(8), decimals(1)]
        15 if data.len() >= 10 => {
            let amt = u64::from_le_bytes(data[1..9].try_into().unwrap_or([0u8; 8]));
            let from = resolve_key(account_keys, ix_accounts, 2); // authority (burner)
            let to = resolve_key(account_keys, ix_accounts, 0); // source token account
            let mint = Some(resolve_key(account_keys, ix_accounts, 1)); // mint
            Some(ParsedInstruction {
                tx_type: "burn".into(),
                from,
                to,
                amount: Some(amt.to_string()),
                mint,
            })
        }
        _ => None,
    }
}

/// Parse a `StoredTransaction` into the JSON-friendly `StreamedTransaction`.
fn parse_stored_transaction(
    stored_tx: &StoredTransaction,
    signature: &Signature,
) -> StreamedTransaction {
    let account_keys = stored_tx.transaction.message.static_account_keys();
    let instructions = match &stored_tx.transaction.message {
        VersionedMessage::Legacy(msg) => &msg.instructions,
        VersionedMessage::V0(msg) => &msg.instructions,
    };

    let escrow_program_id: Pubkey = ESCROW_PROGRAM_ID.parse().unwrap();
    let spl_token_program_id: Pubkey = SPL_TOKEN_PROGRAM_ID.parse().unwrap();

    let failed = stored_tx.meta.status.is_err();

    let mut result = ParsedInstruction {
        tx_type: "unknown".into(),
        from: account_keys
            .first()
            .map(|k| k.to_string())
            .unwrap_or_default(),
        to: String::new(),
        amount: None,
        mint: None,
    };

    // Extract pre_token_balances for SPL Token parsing
    let pre_token_balances = match &stored_tx.meta.pre_token_balances {
        OptionSerializer::Some(b) => Some(b.clone()),
        _ => None,
    };

    for ix in instructions {
        let prog_id = account_keys.get(ix.program_id_index as usize);

        if prog_id == Some(&escrow_program_id) {
            result = parse_escrow_instruction(&ix.data, account_keys, &ix.accounts);
            break;
        }

        if prog_id == Some(&spl_token_program_id) {
            if let Some(parsed) = parse_spl_token_instruction(
                &ix.data,
                account_keys,
                &ix.accounts,
                &pre_token_balances,
            ) {
                result = parsed;
                break;
            }
        }
    }

    StreamedTransaction {
        signature: signature.to_string(),
        chain: "private_channel".into(),
        tx_type: result.tx_type,
        from: result.from,
        to: result.to,
        amount: result.amount,
        mint: result.mint,
        timestamp: stored_tx.block_time,
        status: if failed { "failed" } else { "confirmed" }.into(),
        slot: stored_tx.slot,
    }
}

// ---------------------------------------------------------------------------
// Indexer row type (escrow deposits / withdrawals)
// ---------------------------------------------------------------------------
#[derive(FromRow, Debug)]
struct IndexerTxRow {
    id: i64,
    signature: String,
    slot: i64,
    initiator: String,
    recipient: String,
    mint: String,
    amount: i64,
    transaction_type: String,
    status: String,
    created_at_epoch: i64,
}

// ---------------------------------------------------------------------------
// Poller — indexer DB for escrow deposits / withdrawals
// ---------------------------------------------------------------------------

async fn poll_indexer(
    pool: PgPool,
    tx_sender: broadcast::Sender<String>,
    poll_interval: Duration,
    heartbeat: Arc<AtomicI64>,
) {
    let mut last_seen_id: i64 =
        sqlx::query_scalar::<_, Option<i64>>("SELECT MAX(id) FROM transactions")
            .fetch_one(&pool)
            .await
            .unwrap_or(Some(0))
            .unwrap_or(0);

    info!(
        "[indexer] Starting poller from transaction id {}",
        last_seen_id
    );

    loop {
        tokio::time::sleep(poll_interval).await;

        let rows = match sqlx::query_as::<_, IndexerTxRow>(
            r#"
            SELECT id, signature, slot, initiator, recipient, mint, amount,
                   transaction_type::text as transaction_type,
                   status::text as status,
                   EXTRACT(EPOCH FROM created_at)::bigint as created_at_epoch
            FROM transactions
            WHERE id > $1
            ORDER BY id ASC
            LIMIT 100
            "#,
        )
        .bind(last_seen_id)
        .fetch_all(&pool)
        .await
        {
            Ok(rows) => rows,
            Err(e) => {
                error!("[indexer] Failed to poll: {}", e);
                continue;
            }
        };
        // Record the loop iteration as a successful heartbeat regardless of row count —
        // a quiet DB is healthy idle, not a wedge.
        heartbeat.store(now_unix(), Ordering::Relaxed);

        if rows.is_empty() {
            continue;
        }

        for row in rows {
            last_seen_id = row.id;

            // Map DB enum "withdrawal" -> "withdraw" for frontend compatibility
            let tx_type = match row.transaction_type.as_str() {
                "withdrawal" => "withdraw",
                other => other,
            };

            let streamed = StreamedTransaction {
                signature: row.signature,
                chain: "private_channel".into(),
                tx_type: tx_type.to_string(),
                from: row.initiator,
                to: row.recipient,
                amount: Some(row.amount.to_string()),
                mint: Some(row.mint),
                timestamp: row.created_at_epoch,
                status: row.status,
                slot: row.slot as u64,
            };

            match serde_json::to_string(&streamed) {
                Ok(json) => {
                    debug!(
                        "[indexer] Broadcasting {}: {}",
                        streamed.signature, streamed.tx_type
                    );
                    let _ = tx_sender.send(json);
                }
                Err(e) => error!("Failed to serialize: {}", e),
            }
        }
        debug!("[indexer] Polled up to id {}", last_seen_id);
    }
}

// ---------------------------------------------------------------------------
// Poller — AccountsDB for PrivateChannel-internal transactions (mint/burn/transfer)
// ---------------------------------------------------------------------------

async fn poll_loop(
    accounts_db: AccountsDB,
    tx_sender: broadcast::Sender<String>,
    poll_interval: Duration,
    heartbeat: Arc<AtomicI64>,
) {
    // Initialise to the latest slot so we only stream new activity.
    let mut last_seen_slot: u64 = match accounts_db.get_latest_slot().await {
        Ok(Some(slot)) => {
            info!("Starting poller from slot {}", slot);
            slot
        }
        Ok(None) => {
            info!("No blocks found, starting poller from slot 0");
            0
        }
        Err(e) => {
            warn!("Failed to get latest slot, starting from 0: {}", e);
            0
        }
    };

    loop {
        tokio::time::sleep(poll_interval).await;

        let blocks = match accounts_db.get_blocks(last_seen_slot + 1, None).await {
            Ok(b) => b,
            Err(_) => continue,
        };
        // Record the loop iteration as a successful heartbeat — empty result is healthy idle.
        heartbeat.store(now_unix(), Ordering::Relaxed);

        if blocks.is_empty() {
            continue;
        }

        for slot in &blocks {
            let block_info: BlockInfo = match accounts_db.get_block(*slot).await {
                Some(info) => info,
                None => {
                    warn!("Block {} listed but not found", slot);
                    continue;
                }
            };

            for sig in &block_info.transaction_signatures {
                let stored_tx = match accounts_db.get_transaction(sig).await {
                    Some(tx) => tx,
                    None => {
                        warn!("Transaction {} not found in block {}", sig, slot);
                        continue;
                    }
                };

                let parsed = parse_stored_transaction(&stored_tx, sig);
                match serde_json::to_string(&parsed) {
                    Ok(json) => {
                        // send returns Err when there are no receivers — that's fine.
                        let _ = tx_sender.send(json);
                    }
                    Err(e) => {
                        error!("Failed to serialize transaction: {}", e);
                    }
                }
            }
        }

        last_seen_slot = *blocks.last().unwrap();
        debug!("Polled up to slot {}", last_seen_slot);
    }
}

// ---------------------------------------------------------------------------
// WebSocket handler
// ---------------------------------------------------------------------------

async fn ws_handler(ws: WebSocketUpgrade, State(state): State<Arc<AppState>>) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_ws_connection(socket, state))
}

async fn handle_ws_connection(mut socket: WebSocket, state: Arc<AppState>) {
    let mut rx = state.tx_sender.subscribe();
    // Send a ping every 20s to keep Railway's proxy from killing the connection.
    let mut ping_interval = tokio::time::interval(Duration::from_secs(20));
    ping_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    info!("New WebSocket client connected");

    loop {
        tokio::select! {
            msg = rx.recv() => {
                match msg {
                    Ok(json) => {
                        if socket.send(Message::Text(json.into())).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!("WebSocket client lagged, skipped {} messages", n);
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        break;
                    }
                }
            }
            _ = ping_interval.tick() => {
                if socket.send(Message::Ping(vec![].into())).await.is_err() {
                    break;
                }
            }
            msg = socket.recv() => {
                match msg {
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(Message::Ping(data))) => {
                        if socket.send(Message::Pong(data)).await.is_err() {
                            break;
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    info!("WebSocket client disconnected");
}

async fn health_handler(State(state): State<Arc<AppState>>) -> StatusCode {
    let now = now_unix();
    let accounts_fresh =
        now - state.accounts_poll_at.load(Ordering::Relaxed) < HEALTH_STALE_THRESHOLD;
    let indexer_fresh = state.indexer_poll_at.as_ref().map_or(true, |a| {
        now - a.load(Ordering::Relaxed) < HEALTH_STALE_THRESHOLD
    });
    if accounts_fresh && indexer_fresh {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

// ---------------------------------------------------------------------------
// Logging
// ---------------------------------------------------------------------------

fn init_logging(log_level: &str, json_logs: bool) {
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(log_level));

    if json_logs {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .json()
            .init();
    } else {
        tracing_subscriber::fmt().with_env_filter(env_filter).init();
    }
}

// ---------------------------------------------------------------------------
// Shutdown signal
// ---------------------------------------------------------------------------

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    let args = Args::parse();
    init_logging(&args.log_level, args.json_logs);

    // Port resolution: --port flag / PORT env (clap) -> STREAMER_PORT env -> 8902
    let port = args.port.unwrap_or_else(|| {
        std::env::var("STREAMER_PORT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(8902)
    });

    info!(
        "Starting PrivateChannel streamer v{}",
        env!("CARGO_PKG_VERSION")
    );

    // Connect to the read replica
    let accounts_db = AccountsDB::new(&args.accountsdb_connection_url, true)
        .await
        .expect("Failed to connect to accounts database");

    info!("Connected to accounts database");

    // Optionally connect to the indexer DB (for escrow deposits/withdrawals)
    let indexer_pool = if let Some(ref db_url) = args.database_url {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(db_url)
            .await
            .expect("Failed to connect to indexer database");
        info!("Connected to indexer database");
        Some(pool)
    } else {
        warn!("No STREAMER_DATABASE_URL set — escrow deposit/withdrawal streaming disabled");
        None
    };

    // Broadcast channel — 4096 buffered messages before lagging.
    // Keep _keep_alive so the channel never transitions to "closed"
    // (which would happen if all receivers are dropped).
    let (tx_sender, _keep_alive) = broadcast::channel::<String>(4096);

    let poll_interval = Duration::from_millis(args.poll_interval_ms);

    // Initialize heartbeats to "now" so /health doesn't 503 during the start_period
    // grace window before the first poll iteration completes.
    let accounts_poll_at = Arc::new(AtomicI64::new(now_unix()));
    let indexer_poll_at_opt = indexer_pool
        .as_ref()
        .map(|_| Arc::new(AtomicI64::new(now_unix())));

    // Spawn indexer poller (deposits / withdrawals)
    if let (Some(pool), Some(heartbeat)) = (indexer_pool, indexer_poll_at_opt.clone()) {
        let indexer_tx = tx_sender.clone();
        tokio::spawn(async move {
            poll_indexer(pool, indexer_tx, poll_interval, heartbeat).await;
        });
    }

    // Spawn AccountsDB poller (mint / burn / transfer on PrivateChannel)
    let poller_db = accounts_db.clone();
    let poller_tx = tx_sender.clone();
    let poller_heartbeat = accounts_poll_at.clone();
    tokio::spawn(async move {
        poll_loop(poller_db, poller_tx, poll_interval, poller_heartbeat).await;
    });

    // Build the axum app
    let state = Arc::new(AppState {
        tx_sender,
        accounts_poll_at,
        indexer_poll_at: indexer_poll_at_opt,
    });

    let cors_origin = if args.cors_allowed_origin == "*" {
        AllowOrigin::any()
    } else {
        let header_value = HeaderValue::from_str(&args.cors_allowed_origin)
            .expect("Invalid CORS allowed origin value");
        AllowOrigin::exact(header_value)
    };

    let cors = CorsLayer::new()
        .allow_origin(cors_origin)
        .allow_methods(Any)
        .allow_headers(Any);

    let app = Router::new()
        .route("/ws", get(ws_handler))
        .route("/health", get(health_handler))
        .layer(cors)
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    info!("Listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("Failed to bind");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .expect("Server error");

    info!("Streamer shut down");
}
