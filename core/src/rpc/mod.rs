pub mod api;
pub mod constants;
pub mod error;
mod get_account_info_impl;
mod get_block_impl;
mod get_block_time_impl;
mod get_blocks_impl;
mod get_epoch_info_impl;
mod get_epoch_schedule_impl;
mod get_first_available_block_impl;
mod get_latest_blockhash_impl;
mod get_recent_blockhash_impl;
mod get_recent_performance_samples_impl;
mod get_signature_statuses_impl;
mod get_signatures_for_address_impl;
mod get_slot_impl;
mod get_slot_leaders_impl;
mod get_supply_impl;
mod get_token_account_balance_impl;
mod get_transaction_count_impl;
mod get_transaction_impl;
mod get_vote_accounts_impl;
mod handler;
mod is_blockhash_valid_impl;
mod rpc_impl;
mod send_transaction_impl;
pub mod server;
mod simulate_transaction_impl;

pub use {
    api::PrivateChannelRpcServer,
    handler::{create_rpc_module, handle_request},
    rpc_impl::{ReadDeps, WriteDeps},
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accounts::{traits::BlockInfo, AccountsDB};
    use crate::test_helpers::{create_test_sanitized_transaction, flush_address_signatures_sync};
    use solana_rpc_client_types::response::RpcPerfSample;
    use solana_sdk::{
        account::AccountSharedData,
        hash::Hash,
        pubkey::Pubkey,
        signature::{Keypair, Signer},
    };
    use solana_svm::account_loader::LoadedTransaction;
    use solana_svm::transaction_execution_result::{
        ExecutedTransaction, TransactionExecutionDetails,
    };
    use solana_svm::transaction_processing_result::ProcessedTransaction;
    use std::collections::{HashMap, LinkedList};
    use std::sync::{Arc, RwLock};
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::postgres::Postgres;

    async fn start_pg() -> (AccountsDB, testcontainers::ContainerAsync<Postgres>) {
        let container = Postgres::default()
            .with_db_name("rpc_test")
            .with_user("postgres")
            .with_password("password")
            .start()
            .await
            .unwrap();
        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(5432).await.unwrap();
        let url = format!("postgres://postgres:password@{}:{}/rpc_test", host, port);
        let db = AccountsDB::new(&url, false)
            .await
            .unwrap_or_else(|e| panic!("Failed: {}", e));
        (db, container)
    }

    fn make_read_deps(db: AccountsDB) -> ReadDeps {
        ReadDeps {
            accounts_db: db,
            admin_keys: vec![],
            live_blockhashes: Arc::new(RwLock::new(LinkedList::new())),
        }
    }

    fn make_block_info(slot: u64, blockhash: Hash) -> BlockInfo {
        BlockInfo {
            slot,
            blockhash,
            previous_blockhash: Hash::default(),
            parent_slot: slot.saturating_sub(1),
            block_height: Some(slot),
            block_time: Some(1_700_000_000 + slot as i64),
            transaction_signatures: vec![],
            transaction_recent_blockhashes: vec![],
        }
    }

    fn make_executed_tx(accounts: Vec<(Pubkey, AccountSharedData)>) -> ProcessedTransaction {
        ProcessedTransaction::Executed(Box::new(ExecutedTransaction {
            loaded_transaction: LoadedTransaction {
                accounts,
                ..Default::default()
            },
            execution_details: TransactionExecutionDetails {
                status: Ok(()),
                log_messages: None,
                inner_instructions: None,
                return_data: None,
                executed_units: 0,
                accounts_data_len_delta: 0,
            },
            programs_modified_by_tx: HashMap::new(),
        }))
    }

    // Seed a block + blockhash so most read endpoints work
    async fn seed_db(db: &mut AccountsDB) {
        let block = make_block_info(10, Hash::new_unique());
        db.write_batch(&[], vec![], Some(block)).await.unwrap();
    }

    // ── get_slot ──────────────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_slot_impl() {
        let (mut db, _pg) = start_pg().await;
        seed_db(&mut db).await;
        let deps = make_read_deps(db);
        let slot = get_slot_impl::get_slot_impl(&deps, None).await.unwrap();
        assert_eq!(slot, 10);
    }

    // ── get_block_time ────────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_block_time_impl() {
        let (mut db, _pg) = start_pg().await;
        seed_db(&mut db).await;
        let deps = make_read_deps(db);
        let time = get_block_time_impl::get_block_time_impl(&deps, 10)
            .await
            .unwrap();
        assert!(time.is_some());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_block_time_impl_missing() {
        let (db, _pg) = start_pg().await;
        let deps = make_read_deps(db);
        let time = get_block_time_impl::get_block_time_impl(&deps, 999)
            .await
            .unwrap();
        assert!(time.is_none());
    }

    // ── get_blocks ────────────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_blocks_impl() {
        let (mut db, _pg) = start_pg().await;
        for slot in [5, 10, 15] {
            db.store_block(make_block_info(slot, Hash::new_unique()))
                .await
                .unwrap();
        }
        let deps = make_read_deps(db);
        let blocks = get_blocks_impl::get_blocks_impl(&deps, 5, Some(15), None)
            .await
            .unwrap();
        assert_eq!(blocks, vec![5, 10, 15]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_blocks_end_before_start() {
        let (db, _pg) = start_pg().await;
        let deps = make_read_deps(db);
        let result = get_blocks_impl::get_blocks_impl(&deps, 10, Some(5), None).await;
        assert!(result.is_err());
    }

    // ── get_epoch_info ────────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_epoch_info_impl() {
        let (mut db, _pg) = start_pg().await;
        seed_db(&mut db).await;
        let deps = make_read_deps(db);
        let info = get_epoch_info_impl::get_epoch_info_impl(&deps, None)
            .await
            .unwrap();
        assert_eq!(info.absolute_slot, 10);
        assert_eq!(info.epoch, 0);
    }

    // ── get_epoch_schedule ────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_epoch_schedule_impl() {
        let schedule = get_epoch_schedule_impl::get_epoch_schedule_impl()
            .await
            .unwrap();
        assert_eq!(schedule.slots_per_epoch, u64::MAX);
        assert!(!schedule.warmup);
    }

    // ── get_first_available_block ─────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_first_available_block_impl() {
        let (mut db, _pg) = start_pg().await;
        for slot in [5, 10] {
            db.store_block(make_block_info(slot, Hash::new_unique()))
                .await
                .unwrap();
        }
        let deps = make_read_deps(db);
        let first = get_first_available_block_impl::get_first_available_block_impl(&deps)
            .await
            .unwrap();
        assert_eq!(first, 5);
    }

    // ── get_latest_blockhash ──────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_latest_blockhash_impl() {
        let (mut db, _pg) = start_pg().await;
        let blockhash = Hash::new_unique();
        db.store_block(make_block_info(10, blockhash))
            .await
            .unwrap();
        let deps = make_read_deps(db);
        let resp = get_latest_blockhash_impl::get_latest_blockhash_impl(&deps, None)
            .await
            .unwrap();
        assert_eq!(resp.value.blockhash, blockhash.to_string());
        assert_eq!(resp.value.last_valid_block_height, 160); // slot 10 + 150
    }

    // ── get_recent_blockhash ──────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_recent_blockhash_impl() {
        let (mut db, _pg) = start_pg().await;
        seed_db(&mut db).await;
        let deps = make_read_deps(db);
        let resp = get_recent_blockhash_impl::get_recent_blockhash_impl(&deps)
            .await
            .unwrap();
        assert_eq!(resp.value.fee_calculator.lamports_per_signature, 5000);
        assert_eq!(resp.context.slot, 10);
    }

    // ── get_transaction_count ─────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_transaction_count_impl() {
        let (db, _pg) = start_pg().await;
        let deps = make_read_deps(db);
        let count = get_transaction_count_impl::get_transaction_count_impl(&deps, None)
            .await
            .unwrap();
        assert_eq!(count, 0);
    }

    // ── get_recent_performance_samples ────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_recent_performance_samples_impl() {
        let (mut db, _pg) = start_pg().await;
        db.store_performance_sample(RpcPerfSample {
            slot: 10,
            num_transactions: 50,
            num_slots: 5,
            sample_period_secs: 60,
            num_non_vote_transactions: Some(50),
        })
        .await
        .unwrap();
        let deps = make_read_deps(db);
        let samples = get_recent_performance_samples_impl::get_recent_performance_samples_impl(
            &deps,
            Some(1),
        )
        .await
        .unwrap();
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].slot, 10);
    }

    // ── get_supply ────────────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_supply_impl() {
        let (mut db, _pg) = start_pg().await;
        seed_db(&mut db).await;
        let deps = make_read_deps(db);
        let resp = get_supply_impl::get_supply_impl(&deps, None).await.unwrap();
        assert_eq!(resp.value.total, 0);
        assert_eq!(resp.context.slot, 10);
    }

    // ── get_vote_accounts ─────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_vote_accounts_impl() {
        let resp = get_vote_accounts_impl::get_vote_accounts_impl(None)
            .await
            .unwrap();
        assert!(resp.current.is_empty());
        assert!(resp.delinquent.is_empty());
    }

    // ── get_slot_leaders ──────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_slot_leaders_impl() {
        let leaders = get_slot_leaders_impl::get_slot_leaders_impl(0, 10)
            .await
            .unwrap();
        assert!(leaders.is_empty());
    }

    // ── is_blockhash_valid ────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_is_blockhash_valid_in_window() {
        let (mut db, _pg) = start_pg().await;
        seed_db(&mut db).await;

        let blockhash = Hash::new_unique();
        let deps = make_read_deps(db);
        deps.live_blockhashes.write().unwrap().push_back(blockhash);

        let resp =
            is_blockhash_valid_impl::is_blockhash_valid_impl(&deps, blockhash.to_string(), None)
                .await
                .unwrap();
        assert!(resp.value);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_is_blockhash_valid_not_in_window() {
        let (mut db, _pg) = start_pg().await;
        seed_db(&mut db).await;
        let deps = make_read_deps(db);

        let resp = is_blockhash_valid_impl::is_blockhash_valid_impl(
            &deps,
            Hash::new_unique().to_string(),
            None,
        )
        .await
        .unwrap();
        assert!(!resp.value);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_is_blockhash_valid_invalid_input() {
        let (db, _pg) = start_pg().await;
        let deps = make_read_deps(db);
        let result =
            is_blockhash_valid_impl::is_blockhash_valid_impl(&deps, "not_a_hash".to_string(), None)
                .await;
        assert!(result.is_err());
    }

    // ── get_block ─────────────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_block_impl_exists() {
        let (mut db, _pg) = start_pg().await;
        db.store_block(make_block_info(42, Hash::new_unique()))
            .await
            .unwrap();
        let deps = make_read_deps(db);
        let block = get_block_impl::get_block_impl(&deps, 42, None)
            .await
            .unwrap();
        assert!(block.is_some());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_block_impl_missing() {
        let (db, _pg) = start_pg().await;
        let deps = make_read_deps(db);
        let block = get_block_impl::get_block_impl(&deps, 999, None)
            .await
            .unwrap();
        assert!(block.is_none());
    }

    // ── get_transaction ───────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_transaction_impl_exists() {
        let (mut db, _pg) = start_pg().await;

        let from = Keypair::new();
        let to = Pubkey::new_unique();
        let tx = create_test_sanitized_transaction(&from, &to, 100);
        let sig = *tx.signature();
        let processed = make_executed_tx(vec![]);

        db.write_batch(
            &[],
            vec![(sig, &tx, 1, 1_700_000_000, &processed)],
            Some(make_block_info(1, Hash::new_unique())),
        )
        .await
        .unwrap();

        let deps = make_read_deps(db);
        let result = get_transaction_impl::get_transaction_impl(&deps, sig.to_string(), None)
            .await
            .unwrap();
        assert!(result.is_some());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_transaction_impl_missing() {
        let (db, _pg) = start_pg().await;
        let deps = make_read_deps(db);
        let sig = solana_sdk::signature::Signature::new_unique();
        let result = get_transaction_impl::get_transaction_impl(&deps, sig.to_string(), None)
            .await
            .unwrap();
        assert!(result.is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_transaction_impl_invalid_sig() {
        let (db, _pg) = start_pg().await;
        let deps = make_read_deps(db);
        let result =
            get_transaction_impl::get_transaction_impl(&deps, "not_a_sig".to_string(), None).await;
        assert!(result.is_err());
    }

    // ── get_signature_statuses ────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_signature_statuses_found() {
        let (mut db, _pg) = start_pg().await;

        let from = Keypair::new();
        let to = Pubkey::new_unique();
        let tx = create_test_sanitized_transaction(&from, &to, 100);
        let sig = *tx.signature();
        let processed = make_executed_tx(vec![]);

        db.write_batch(
            &[],
            vec![(sig, &tx, 1, 1_700_000_000, &processed)],
            Some(make_block_info(1, Hash::new_unique())),
        )
        .await
        .unwrap();

        let deps = make_read_deps(db);
        let resp = get_signature_statuses_impl::get_signature_statuses_impl(
            &deps,
            vec![sig.to_string()],
            None,
        )
        .await
        .unwrap();
        assert_eq!(resp.value.len(), 1);
        assert!(resp.value[0].is_some());
        let status = resp.value[0].as_ref().unwrap();
        assert!(status.status.is_ok());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_signature_statuses_not_found() {
        let (db, _pg) = start_pg().await;
        let deps = make_read_deps(db);
        let sig = solana_sdk::signature::Signature::new_unique();
        let resp = get_signature_statuses_impl::get_signature_statuses_impl(
            &deps,
            vec![sig.to_string()],
            None,
        )
        .await
        .unwrap();
        assert_eq!(resp.value.len(), 1);
        assert!(resp.value[0].is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_signature_statuses_invalid_sig() {
        let (db, _pg) = start_pg().await;
        let deps = make_read_deps(db);
        let resp = get_signature_statuses_impl::get_signature_statuses_impl(
            &deps,
            vec!["bad_sig".to_string()],
            None,
        )
        .await
        .unwrap();
        // Invalid signatures return None (not an error)
        assert_eq!(resp.value.len(), 1);
        assert!(resp.value[0].is_none());
    }

    // ── get_token_account_balance ─────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_token_account_balance_not_found() {
        let (db, _pg) = start_pg().await;
        let deps = make_read_deps(db);
        let result = get_token_account_balance_impl::get_token_account_balance_impl(
            &deps,
            Pubkey::new_unique().to_string(),
            None,
        )
        .await;
        assert!(result.is_err()); // "Account not found"
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_token_account_balance_wrong_owner() {
        let (mut db, _pg) = start_pg().await;
        let pk = Pubkey::new_unique();
        // Store an account owned by system program, not spl_token
        db.set_account(pk, AccountSharedData::new(100, 0, &Pubkey::new_unique()))
            .await;
        let deps = make_read_deps(db);
        let result = get_token_account_balance_impl::get_token_account_balance_impl(
            &deps,
            pk.to_string(),
            None,
        )
        .await;
        assert!(result.is_err()); // "not a token account"
    }

    // ── get_token_account_balance with valid SPL token account ────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_token_account_balance_valid() {
        use spl_token::solana_program::program_pack::Pack;
        let (mut db, _pg) = start_pg().await;

        // Store a block so get_latest_slot works
        seed_db(&mut db).await;

        // Create a mint account
        let mint_pk = Pubkey::new_unique();
        let mint_authority = Pubkey::new_unique();
        let mint = spl_token::state::Mint {
            mint_authority: spl_token::solana_program::program_option::COption::Some(
                mint_authority,
            ),
            supply: 1_000_000,
            decimals: 6,
            is_initialized: true,
            freeze_authority: spl_token::solana_program::program_option::COption::None,
        };
        let mut mint_data = vec![0u8; spl_token::state::Mint::LEN];
        spl_token::state::Mint::pack(mint, &mut mint_data).unwrap();
        let mut mint_account =
            AccountSharedData::new(1, spl_token::state::Mint::LEN, &spl_token::id());
        mint_account.set_data_from_slice(&mint_data);
        db.set_account(mint_pk, mint_account).await;

        // Create a token account
        let token_pk = Pubkey::new_unique();
        let owner = Pubkey::new_unique();
        let token_acct = spl_token::state::Account {
            mint: mint_pk,
            owner,
            amount: 500_000,
            delegate: spl_token::solana_program::program_option::COption::None,
            state: spl_token::state::AccountState::Initialized,
            is_native: spl_token::solana_program::program_option::COption::None,
            delegated_amount: 0,
            close_authority: spl_token::solana_program::program_option::COption::None,
        };
        let mut token_data = vec![0u8; spl_token::state::Account::LEN];
        spl_token::state::Account::pack(token_acct, &mut token_data).unwrap();
        let mut token_account =
            AccountSharedData::new(1, spl_token::state::Account::LEN, &spl_token::id());
        token_account.set_data_from_slice(&token_data);
        db.set_account(token_pk, token_account).await;

        let deps = make_read_deps(db);
        let resp = get_token_account_balance_impl::get_token_account_balance_impl(
            &deps,
            token_pk.to_string(),
            None,
        )
        .await
        .unwrap();
        assert_eq!(resp.value.amount, "500000");
        assert_eq!(resp.value.decimals, 6);
        assert_eq!(resp.value.ui_amount_string, "0.5");
    }

    // ── get_signatures_for_address ────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_signatures_for_address_found() {
        let (mut db, _pg) = start_pg().await;

        let from = Keypair::new();
        let to = Pubkey::new_unique();
        let tx = create_test_sanitized_transaction(&from, &to, 100);
        let sig = *tx.signature();
        let processed = make_executed_tx(vec![]);

        let addr_sig_rows = db
            .write_batch(
                &[],
                vec![(sig, &tx, 5, 1_700_000_000, &processed)],
                Some(make_block_info(5, Hash::new_unique())),
            )
            .await
            .unwrap();
        flush_address_signatures_sync(&db, &addr_sig_rows).await;

        let deps = make_read_deps(db);
        let sigs = get_signatures_for_address_impl::get_signatures_for_address_impl(
            &deps,
            from.pubkey().to_string(),
            None,
        )
        .await
        .unwrap();

        assert_eq!(sigs.len(), 1);
        assert_eq!(sigs[0].signature, sig.to_string());
        assert_eq!(sigs[0].slot, 5);
        assert!(sigs[0].err.is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_signatures_for_address_empty() {
        let (db, _pg) = start_pg().await;
        let deps = make_read_deps(db);
        let sigs = get_signatures_for_address_impl::get_signatures_for_address_impl(
            &deps,
            Pubkey::new_unique().to_string(),
            None,
        )
        .await
        .unwrap();
        assert!(sigs.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_signatures_for_address_invalid_address() {
        let (db, _pg) = start_pg().await;
        let deps = make_read_deps(db);
        let result = get_signatures_for_address_impl::get_signatures_for_address_impl(
            &deps,
            "not_a_pubkey".to_string(),
            None,
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_signatures_for_address_limit() {
        let (mut db, _pg) = start_pg().await;
        let from = Keypair::new();
        let processed = make_executed_tx(vec![]);

        for slot in [1u64, 2, 3] {
            let to = Pubkey::new_unique();
            let tx = create_test_sanitized_transaction(&from, &to, slot);
            let sig = *tx.signature();
            let addr_sig_rows = db
                .write_batch(
                    &[],
                    vec![(sig, &tx, slot, 1_700_000_000, &processed)],
                    Some(make_block_info(slot, Hash::new_unique())),
                )
                .await
                .unwrap();
            flush_address_signatures_sync(&db, &addr_sig_rows).await;
        }

        let deps = make_read_deps(db);
        let config = solana_rpc_client_types::config::RpcSignaturesForAddressConfig {
            limit: Some(2),
            ..Default::default()
        };
        let sigs = get_signatures_for_address_impl::get_signatures_for_address_impl(
            &deps,
            from.pubkey().to_string(),
            Some(config),
        )
        .await
        .unwrap();

        assert_eq!(sigs.len(), 2);
        assert!(sigs[0].slot >= sigs[1].slot);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_signatures_for_address_newest_first() {
        let (mut db, _pg) = start_pg().await;
        let from = Keypair::new();
        let processed = make_executed_tx(vec![]);

        for slot in [10u64, 20, 30] {
            let to = Pubkey::new_unique();
            let tx = create_test_sanitized_transaction(&from, &to, slot);
            let sig = *tx.signature();
            let addr_sig_rows = db
                .write_batch(
                    &[],
                    vec![(sig, &tx, slot, 1_700_000_000, &processed)],
                    Some(make_block_info(slot, Hash::new_unique())),
                )
                .await
                .unwrap();
            flush_address_signatures_sync(&db, &addr_sig_rows).await;
        }

        let deps = make_read_deps(db);
        let sigs = get_signatures_for_address_impl::get_signatures_for_address_impl(
            &deps,
            from.pubkey().to_string(),
            None,
        )
        .await
        .unwrap();

        assert_eq!(sigs.len(), 3);
        assert_eq!(sigs[0].slot, 30);
        assert_eq!(sigs[1].slot, 20);
        assert_eq!(sigs[2].slot, 10);
    }

    // ── rpc_impl PrivateChannelRpcImpl ────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn test_rpc_impl_no_read_deps() {
        use super::api::PrivateChannelRpcServer;
        let rpc = rpc_impl::PrivateChannelRpcImpl::new(None, None).await;
        // All read operations should return "read not enabled"
        let result = rpc.get_slot(None).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code(), -32002);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_rpc_impl_no_write_deps() {
        use super::api::PrivateChannelRpcServer;
        let rpc = rpc_impl::PrivateChannelRpcImpl::new(None, None).await;
        let result = rpc.send_transaction("test".to_string(), None).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code(), -32001);
    }
}
