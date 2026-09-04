use crate::{
    accounts::precompiles,
    rpc::{
        error::{custom_error, INVALID_PARAMS_CODE, JSON_RPC_SERVER_ERROR},
        ReadDeps,
    },
};
use jsonrpsee::core::RpcResult;
use solana_account_decoder::encode_ui_account;
use solana_account_decoder_client_types::{UiAccount, UiAccountEncoding};
use solana_client::{
    rpc_config::RpcAccountInfoConfig,
    rpc_response::{Response, RpcResponseContext},
};
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use tracing::debug;

pub async fn get_account_info_impl(
    read_deps: &ReadDeps,
    pubkey: String,
    config: Option<RpcAccountInfoConfig>,
) -> RpcResult<Response<Option<UiAccount>>> {
    let pubkey = Pubkey::from_str(&pubkey)
        .map_err(|e| custom_error(INVALID_PARAMS_CODE, format!("Invalid pubkey: {}", e)))?;

    let config = config.unwrap_or_default();

    let slot = read_deps
        .accounts_db
        .get_current_slot()
        .await
        .map_err(|e| custom_error(JSON_RPC_SERVER_ERROR, format!("Failed to get slot: {}", e)))?
        .unwrap_or(0);

    // Precompiles short-circuit the DB; everything else reads from AccountsDB.
    // A store that cannot answer is a server error, not a null account.
    let account_data = match precompiles::get(&pubkey) {
        Some(account) => Some(account),
        None => read_deps
            .accounts_db
            .get_account_shared_data(&pubkey)
            .await
            .map_err(|e| custom_error(JSON_RPC_SERVER_ERROR, e.to_string()))?,
    };

    let encoding = config.encoding.unwrap_or(UiAccountEncoding::Base64);
    let value = account_data
        .map(|account| encode_ui_account(&pubkey, &account, encoding, None, config.data_slice));

    debug!("get_account_info pubkey={} hit={}", pubkey, value.is_some());

    Ok(Response {
        context: RpcResponseContext::new(slot),
        value,
    })
}
