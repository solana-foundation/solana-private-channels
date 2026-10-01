use crate::config::{OperatorConfig, ProgramType};
use crate::error::OperatorError;
use crate::metrics::FEEPAYER_BALANCE_LAMPORTS;
use crate::operator::{RpcClientWithRetry, SignerUtil};
use private_channel_metrics::MetricLabel;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::operator::utils::instruction_util::RetryPolicy;

const LOW_BALANCE_LAMPORTS: u64 = 500_000_000; // 0.5 SOL

pub async fn run_feepayer_monitor(
    config: OperatorConfig,
    rpc_client: Arc<RpcClientWithRetry>,
    program_type: ProgramType,
    cancellation_token: CancellationToken,
) -> Result<(), OperatorError> {
    // The admin signs releases first, so it pays their fees, not the operator.
    let feepayer_pubkey = SignerUtil::get_admin_pubkey();
    let label = program_type.as_label();

    info!(
        "Starting feepayer balance monitor for {} (pubkey: {})",
        label, feepayer_pubkey
    );
    info!(
        "Feepayer monitor interval: {:?}",
        config.feepayer_monitor_interval
    );

    loop {
        if cancellation_token.is_cancelled() {
            info!("Feepayer monitor received cancellation signal, stopping...");
            break;
        }

        match rpc_client
            .with_retry("get_balance", RetryPolicy::Idempotent, || async {
                rpc_client.rpc_client.get_balance(&feepayer_pubkey).await
            })
            .await
        {
            Ok(lamports) => {
                FEEPAYER_BALANCE_LAMPORTS
                    .with_label_values(&[label])
                    .set(lamports as f64);

                let sol = lamports as f64 / 1e9;
                info!("Feepayer balance: {} lamports ({:.4} SOL)", lamports, sol);

                if lamports < LOW_BALANCE_LAMPORTS {
                    warn!(
                        "Feepayer balance is low: {} lamports ({:.4} SOL) — top up immediately",
                        lamports, sol
                    );
                }
            }
            Err(e) => {
                warn!("Failed to fetch feepayer balance: {}", e);
            }
        }

        tokio::select! {
            _ = tokio::time::sleep(config.feepayer_monitor_interval) => {},
            _ = cancellation_token.cancelled() => {
                info!("Feepayer monitor received cancellation signal during sleep, stopping...");
                break;
            }
        }
    }

    info!("Feepayer monitor stopped gracefully");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_run_feepayer_monitor_exists() {
        let _function = run_feepayer_monitor;
    }

    #[test]
    fn test_low_balance_threshold() {
        assert_eq!(LOW_BALANCE_LAMPORTS, 500_000_000);
    }
}
