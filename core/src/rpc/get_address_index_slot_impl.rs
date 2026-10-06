use crate::rpc::{
    error::{custom_error, JSON_RPC_SERVER_ERROR},
    ReadDeps,
};
use jsonrpsee::core::RpcResult;
use serde::{Deserialize, Serialize};

/// How far the address index is complete. Every block at or below `watermark`
/// is fully indexed, and `latest_block` is the newest committed block, both
/// read in one snapshot so `watermark >= latest_block` means the index is whole.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AddressIndexSlot {
    pub watermark: u64,
    pub latest_block: u64,
}

pub async fn get_address_index_slot_impl(read_deps: &ReadDeps) -> RpcResult<AddressIndexSlot> {
    let progress = read_deps
        .accounts_db
        .get_address_index_progress()
        .await
        .map_err(|e| {
            custom_error(
                JSON_RPC_SERVER_ERROR,
                format!("Failed to get address index progress: {}", e),
            )
        })?;
    address_index_reply(progress)
}

/// A missing key is an error, not a zero: callers gate destructive work on
/// this answer, so an unknown index must refuse rather than read as caught up.
fn address_index_reply(progress: Option<(i64, u64)>) -> RpcResult<AddressIndexSlot> {
    match progress {
        Some((watermark, latest_block)) if watermark >= 0 => Ok(AddressIndexSlot {
            watermark: watermark as u64,
            latest_block,
        }),
        _ => Err(custom_error(
            JSON_RPC_SERVER_ERROR,
            "address index progress unavailable",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn address_index_reply_cases() {
        let unavailable = address_index_reply(None).unwrap_err();
        assert_eq!(unavailable.code(), JSON_RPC_SERVER_ERROR);
        assert!(unavailable.message().contains("unavailable"));

        assert_eq!(
            address_index_reply(Some((7, 9))).unwrap(),
            AddressIndexSlot {
                watermark: 7,
                latest_block: 9
            }
        );
        assert_eq!(
            serde_json::to_value(address_index_reply(Some((7, 9))).unwrap()).unwrap(),
            serde_json::json!({"watermark": 7, "latestBlock": 9})
        );

        assert!(address_index_reply(Some((-1, 9))).is_err());
    }
}
