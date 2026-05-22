mod test_batch_atomicity;
mod test_context;
// mod test_cors; // Disabled - CORS is now handled by the gateway
mod test_blockhash_validation;
mod test_dedup_persistence;
mod test_empty_transaction;
mod test_epoch_info;
mod test_epoch_schedule;
mod test_first_available_block;
mod test_get_block_time;
mod test_get_blocks;
mod test_get_signature_statuses;
mod test_get_slot_leaders;
mod test_get_supply;
mod test_get_transaction;
mod test_mixed_transaction;
mod test_non_admin;
mod test_performance_samples;
mod test_precompile_accounts;
mod test_spl_token;
mod test_swap;
mod test_transaction_count;
mod test_tx_replay;
mod test_vote_accounts;
mod utils;

mod test_blocks_in_range_boundaries;
mod test_health_endpoint;
mod test_oversized_body;
mod test_send_transaction_errors;
mod test_sig_statuses_search_depth;
mod test_simulate_transaction_account_writes;
mod test_simulate_transaction_preflight;

// admin-vm malformed InitializeMint coverage.
mod test_admin_vm_initialize_mint_malformed;

// parallel-SVM SnapshotCallback coverage.
mod test_parallel_svm_burst;

pub use test_blockhash_validation::run_blockhash_validation_test;
pub use test_context::{PrivateChannelContext, SolanaContext};
pub use test_dedup_persistence::run_dedup_persistence_test;
pub use test_empty_transaction::run_empty_transaction_test;
pub use test_epoch_info::run_epoch_info_test;
pub use test_epoch_schedule::run_epoch_schedule_test;
pub use test_first_available_block::run_first_available_block_test;
pub use test_get_block_time::run_get_block_time_test;
pub use test_get_blocks::run_get_blocks_test;
pub use test_get_signature_statuses::run_get_signature_statuses_test;
pub use test_get_slot_leaders::run_get_slot_leaders_test;
pub use test_get_supply::run_get_supply_test;
pub use test_get_transaction::run_get_transaction_test;
pub use test_mixed_transaction::run_mixed_transaction_test;
pub use test_non_admin::run_non_admin_sending_admin_instruction_test;
pub use test_performance_samples::run_performance_samples_test;
pub use test_precompile_accounts::run_precompile_accounts_test;
pub use test_spl_token::run_spl_token_test;
pub use test_swap::run_swap_clock_tests;
pub use test_transaction_count::run_transaction_count_test;
pub use test_tx_replay::run_tx_replay_test;
pub use test_vote_accounts::run_vote_accounts_test;
pub use utils::*;

pub use test_blocks_in_range_boundaries::run_blocks_in_range_boundaries_test;
pub use test_health_endpoint::run_health_endpoint_test;
pub use test_oversized_body::run_oversized_body_test;
pub use test_send_transaction_errors::run_send_transaction_errors_test;
pub use test_sig_statuses_search_depth::run_sig_statuses_search_depth_test;
pub use test_simulate_transaction_account_writes::run_simulate_transaction_account_writes_test;
pub use test_simulate_transaction_preflight::run_simulate_transaction_preflight_test;

pub use test_admin_vm_initialize_mint_malformed::run_admin_vm_initialize_mint_malformed_test;
pub use test_parallel_svm_burst::run_parallel_svm_burst_test;
