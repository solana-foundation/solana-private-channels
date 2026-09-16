use crate::rpc::{
    error::{custom_error, INVALID_PARAMS_CODE, JSON_RPC_SERVER_ERROR},
    ReadDeps,
};
use jsonrpsee::core::RpcResult;
use serde::{Deserialize, Serialize};
use solana_rpc_client_api::response::RpcConfirmedTransactionStatusWithSignature;
use solana_rpc_client_types::config::RpcSignaturesForAddressConfig;
use solana_sdk::{pubkey::Pubkey, signature::Signature};
use std::str::FromStr;

// Solana RPC spec: limit must be between 1 and 1000 (inclusive).
const DEFAULT_LIMIT: usize = 1000;
const MAX_LIMIT: usize = 1000;

/// Solana's config for this method, plus the slot scoping the gateway applies
/// when a caller may only read part of an address's history.
///
/// Flattened so params keep Solana's `[address, config]` shape: a client that
/// has never heard of scoping sends exactly what it always did.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SignaturesForAddressConfig {
    #[serde(flatten)]
    pub base: RpcSignaturesForAddressConfig,
    /// Inclusive `(first, last)` slot windows the caller may read. Set by the
    /// gateway, which owns the ownership decision. It can only narrow a page,
    /// so a caller setting it themselves can only see less.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub private_channel_slot_ranges: Option<Vec<(i64, i64)>>,
}

pub async fn get_signatures_for_address_impl(
    read_deps: &ReadDeps,
    address: String,
    config: Option<SignaturesForAddressConfig>,
) -> RpcResult<Vec<RpcConfirmedTransactionStatusWithSignature>> {
    let slot_ranges = config
        .as_ref()
        .and_then(|c| c.private_channel_slot_ranges.clone());
    let config = config.map(|c| c.base);

    let pubkey = Pubkey::from_str(&address)
        .map_err(|e| custom_error(INVALID_PARAMS_CODE, format!("Invalid address: {}", e)))?;

    let limit = config
        .as_ref()
        .and_then(|c| c.limit)
        .unwrap_or(DEFAULT_LIMIT)
        .clamp(1, MAX_LIMIT);

    let before = config
        .as_ref()
        .and_then(|c| c.before.as_deref())
        .map(Signature::from_str)
        .transpose()
        .map_err(|e| custom_error(INVALID_PARAMS_CODE, format!("Invalid 'before': {}", e)))?;

    let until = config
        .as_ref()
        .and_then(|c| c.until.as_deref())
        .map(Signature::from_str)
        .transpose()
        .map_err(|e| custom_error(INVALID_PARAMS_CODE, format!("Invalid 'until': {}", e)))?;

    let signatures = read_deps
        .accounts_db
        .get_signatures_for_address(
            &pubkey,
            limit,
            before.as_ref(),
            until.as_ref(),
            slot_ranges.as_deref(),
        )
        .await
        .map_err(|e| {
            custom_error(
                JSON_RPC_SERVER_ERROR,
                format!("Failed to get signatures: {}", e),
            )
        })?;

    Ok(signatures)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A client that predates scoping sends Solana's config unchanged, and the
    /// flatten must not reject or swallow it.
    #[test]
    fn a_plain_solana_config_still_parses() {
        let config: SignaturesForAddressConfig = serde_json::from_value(serde_json::json!({
            "limit": 10,
            "before": "5j7s6NiJS3JAkvgkoc18WVAsiSaci2pxB2A6ueCJP4tprA2TFg9wSyTLeYouxPBJEMzJinENTkpA52YStRW5Dia7",
        }))
        .expect("Solana's own config shape must deserialize");

        assert_eq!(config.base.limit, Some(10));
        assert!(config.base.before.is_some());
        assert!(config.private_channel_slot_ranges.is_none());
    }

    /// The gateway's scoping rides alongside the standard fields.
    #[test]
    fn slot_ranges_parse_beside_the_standard_fields() {
        let config: SignaturesForAddressConfig = serde_json::from_value(serde_json::json!({
            "limit": 5,
            "privateChannelSlotRanges": [[0, 99], [201, 9223372036854775807i64]],
        }))
        .expect("scoped config must deserialize");

        assert_eq!(config.base.limit, Some(5));
        assert_eq!(
            config.private_channel_slot_ranges,
            Some(vec![(0, 99), (201, i64::MAX)])
        );
    }

    /// An empty scope is not the same as an absent one: it hides the page.
    #[test]
    fn an_empty_scope_survives_the_round_trip() {
        let config: SignaturesForAddressConfig =
            serde_json::from_value(serde_json::json!({"privateChannelSlotRanges": []}))
                .expect("empty scope must deserialize");

        assert_eq!(config.private_channel_slot_ranges, Some(Vec::new()));
    }
}
