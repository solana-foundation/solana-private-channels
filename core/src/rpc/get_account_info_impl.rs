use crate::{
    accounts::precompiles,
    rpc::{
        constants::{estimated_encoded_bytes, MAX_ACCOUNT_RESPONSE_BYTES},
        error::{custom_error, INVALID_PARAMS_CODE, INVALID_REQUEST_CODE, JSON_RPC_SERVER_ERROR},
        ReadDeps,
    },
};
use jsonrpsee::core::RpcResult;
use solana_account_decoder::{encode_ui_account, MAX_BASE58_BYTES};
use solana_account_decoder_client_types::{UiAccount, UiAccountEncoding};
use solana_client::{
    rpc_config::RpcAccountInfoConfig,
    rpc_response::{Response, RpcResponseContext},
};
use solana_sdk::{account::ReadableAccount, pubkey::Pubkey};
use std::{cmp::min, str::FromStr};
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
        .get_latest_slot()
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
    let data_slice = config.data_slice;
    let value = match account_data {
        Some(account) => {
            // Budgeted before anything encodes, so a refused request compresses
            // nothing. A dataSlice narrows what gets encoded, so it narrows the
            // estimate too.
            let selected = data_slice
                .map(|slice| {
                    min(
                        slice.length,
                        account.data().len().saturating_sub(slice.offset),
                    )
                })
                .unwrap_or(account.data().len());
            let estimated = estimated_encoded_bytes(selected);
            if estimated > MAX_ACCOUNT_RESPONSE_BYTES {
                return Err(custom_error(
                    INVALID_PARAMS_CODE,
                    format!(
                        "Account encodes to about {estimated} bytes (max: {MAX_ACCOUNT_RESPONSE_BYTES}); request a dataSlice"
                    ),
                ));
            }

            // The bs58 encoder swaps oversized data for an error string inside
            // an otherwise successful payload. Refuse the request like Agave.
            if matches!(
                encoding,
                UiAccountEncoding::Binary | UiAccountEncoding::Base58
            ) && selected > MAX_BASE58_BYTES
            {
                return Err(custom_error(
                    INVALID_REQUEST_CODE,
                    format!(
                        "Encoded binary (base 58) data should be less than {MAX_BASE58_BYTES} bytes, please use Base64 encoding."
                    ),
                ));
            }

            // Encoding is CPU-bound and the caller picks how expensive: zstd on
            // a precompile ELF costs ~400us against 55us for plain base64. Off
            // the async worker so a read cannot stall the ones beside it.
            Some(
                tokio::task::spawn_blocking(move || {
                    encode_ui_account(&pubkey, &account, encoding, None, data_slice)
                })
                .await
                .map_err(|e| {
                    custom_error(
                        JSON_RPC_SERVER_ERROR,
                        format!("Account encoding failed: {e}"),
                    )
                })?,
            )
        }
        None => None,
    };

    debug!("get_account_info pubkey={} hit={}", pubkey, value.is_some());

    Ok(Response {
        context: RpcResponseContext::new(slot),
        value,
    })
}
