use crate::{
    accounts::precompiles,
    rpc::{
        constants::{estimated_encoded_bytes, MAX_ACCOUNT_RESPONSE_BYTES},
        error::{custom_error, INVALID_PARAMS_CODE, INVALID_REQUEST_CODE, JSON_RPC_SERVER_ERROR},
        ReadDeps,
    },
};
use jsonrpsee::core::RpcResult;
use solana_account_decoder::{
    encode_ui_account,
    parse_account_data::{AccountAdditionalDataV3, SplTokenAdditionalDataV2},
    MAX_BASE58_BYTES,
};
use solana_account_decoder_client_types::{UiAccount, UiAccountEncoding};
use solana_client::{
    rpc_config::RpcAccountInfoConfig,
    rpc_response::{Response, RpcResponseContext},
};
use solana_sdk::{account::AccountSharedData, account::ReadableAccount, pubkey::Pubkey};
use spl_token::solana_program::program_pack::Pack;
use spl_token::state::{Account as TokenAccount, Mint};
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
    let data_slice = config.data_slice;
    let additional_data = if encoding == UiAccountEncoding::JsonParsed {
        build_token_additional_data(read_deps, account_data.as_ref()).await
    } else {
        None
    };
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
                    encode_ui_account(&pubkey, &account, encoding, additional_data, data_slice)
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

async fn build_token_additional_data(
    read_deps: &ReadDeps,
    account: Option<&AccountSharedData>,
) -> Option<AccountAdditionalDataV3> {
    let account = account?;
    if *account.owner() != spl_token::id() {
        return None;
    }
    let token_account = TokenAccount::unpack(account.data()).ok()?;
    let mint_account = read_deps
        .accounts_db
        .get_account_shared_data(&token_account.mint)
        .await
        .ok()
        .flatten()?;
    let mint = Mint::unpack(mint_account.data()).ok()?;
    Some(AccountAdditionalDataV3 {
        spl_token_additional_data: Some(SplTokenAdditionalDataV2 {
            decimals: mint.decimals,
            ..Default::default()
        }),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_account_decoder_client_types::UiAccountData;
    use spl_token::solana_program::program_option::COption;
    use spl_token::state::AccountState;
    use std::sync::Arc;

    fn spl_owned(data: Vec<u8>) -> AccountSharedData {
        AccountSharedData::from(solana_sdk::account::Account {
            lamports: 1,
            data,
            owner: spl_token::id(),
            executable: false,
            rent_epoch: 0,
        })
    }

    fn packed_mint(decimals: u8) -> Vec<u8> {
        let mut data = vec![0u8; Mint::LEN];
        Mint::pack(
            Mint {
                mint_authority: COption::None,
                supply: 1_000_000,
                decimals,
                is_initialized: true,
                freeze_authority: COption::None,
            },
            &mut data,
        )
        .expect("mint must pack");
        data
    }

    fn packed_token_account(mint: Pubkey, amount: u64) -> Vec<u8> {
        let mut data = vec![0u8; TokenAccount::LEN];
        TokenAccount::pack(
            TokenAccount {
                mint,
                owner: Pubkey::new_unique(),
                amount,
                delegate: COption::None,
                state: AccountState::Initialized,
                is_native: COption::None,
                delegated_amount: 0,
                close_authority: COption::None,
            },
            &mut data,
        )
        .expect("token account must pack");
        data
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn json_parsed_token_account_is_parsed_and_carries_mint_decimals() {
        let (postgres_db, _pg) = crate::test_helpers::start_test_postgres_raw().await;
        let mut db = crate::accounts::AccountsDB::Postgres(postgres_db);

        let mint_key = Pubkey::new_unique();
        let token_key = Pubkey::new_unique();
        db.set_account(mint_key, spl_owned(packed_mint(6))).await;
        db.set_account(
            token_key,
            spl_owned(packed_token_account(mint_key, 1_500_000)),
        )
        .await;

        let deps = ReadDeps {
            accounts_db: db,
            admin_keys: vec![],
            live_blockhashes: Arc::new(std::sync::RwLock::new(Default::default())),
            max_blockhashes: 150,
            simulation_permits: tokio::sync::Semaphore::new(1),
        };

        let response = get_account_info_impl(
            &deps,
            token_key.to_string(),
            Some(RpcAccountInfoConfig {
                encoding: Some(UiAccountEncoding::JsonParsed),
                ..Default::default()
            }),
        )
        .await
        .expect("the token account must be served");

        let account = response.value.expect("the token account must be present");
        match &account.data {
            UiAccountData::Json(parsed) => {
                assert_eq!(parsed.program, "spl-token");
                assert_eq!(
                    parsed.parsed["info"]["tokenAmount"]["decimals"].as_u64(),
                    Some(6),
                    "the mint decimals must reach the parser: {}",
                    parsed.parsed
                );
            }
            UiAccountData::Binary(_, encoding) => {
                panic!("jsonParsed fell back to {encoding:?} instead of parsing the token account")
            }
            other => panic!("unexpected account data: {other:?}"),
        }
    }
}
