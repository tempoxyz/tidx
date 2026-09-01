//! RPC-backed historical Earn share-price materialization.
//!
//! ClickHouse identifies confirmed 15-minute sample blocks from canonical
//! indexed history. The RPC supplies the one value that cannot be derived from
//! logs: `EarnVault.previewRedeem(10^18)` at each exact historical block. The
//! large fixed input reduces integer quantization and cancels when consumers
//! compare quote growth across two boundaries.

use std::collections::HashSet;
use std::time::Duration;

use alloy::primitives::U256;
use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Utc};
use futures::{StreamExt, stream};
use tokio::sync::broadcast;
use tracing::{info, warn};

use super::ch_sink::ClickHouseSink;
use super::fetcher::RpcClient;

/// Historical and current `EarnStackDeployed` event versions. The indexed
/// `earnVault` remains topic1 across releases.
pub(crate) const EARN_STACK_DEPLOYED_SELECTORS: &[&str] = &[
    // Initial release.
    "0x420632dca5c7b108ec3fe8f96f06644917f70c8682f8f66cadc3b03e1fe9301d",
    // Added transferPolicyId.
    "0xcc61445ed88cf5fcb2f6c3c1bf13f3abda240d2886154a4f3ff2aba30b866162",
    // Added maxManagedAssets.
    "0x950acdb981ae9ee0189b7bba6d347b4fb9b24f0e4f630a1b9a388bc5702fd67b",
];
pub(crate) const QUOTED_SHARES: u64 = 1_000_000_000_000_000_000;
const PREVIEW_REDEEM_CALLDATA: &str =
    "0x4cdad5060000000000000000000000000000000000000000000000000de0b6b3a7640000";
const SAMPLE_BATCH_SIZE: usize = 128;
const RPC_CONCURRENCY: usize = 8;
const ACTIVE_POLL_INTERVAL: Duration = Duration::from_secs(1);
const IDLE_POLL_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Clone, Debug)]
pub(crate) struct EarnVault {
    pub address: String,
    pub deployment_block: i64,
}

#[derive(Clone, Debug)]
pub(crate) struct EarnSharePriceCandidate {
    pub vault: String,
    pub bucket: DateTime<Utc>,
    pub block_num: i64,
    pub block_hash: String,
    pub block_timestamp: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub(crate) struct EarnSharePriceObservation {
    pub vault: String,
    pub bucket: DateTime<Utc>,
    pub block_num: i64,
    pub block_hash: String,
    pub block_timestamp: DateTime<Utc>,
    pub quoted_assets: U256,
}

pub struct EarnSharePriceMaterializer {
    sink: ClickHouseSink,
    rpc: RpcClient,
}

impl EarnSharePriceMaterializer {
    pub fn new(sink: ClickHouseSink, rpc_url: &str) -> Self {
        Self {
            sink,
            rpc: RpcClient::with_concurrency(rpc_url, RPC_CONCURRENCY),
        }
    }

    pub async fn run(self, mut shutdown: broadcast::Receiver<()>) {
        info!(
            database = %self.sink.database(),
            interval_minutes = 15,
            confirmation_seconds = 30,
            quoted_shares = %QUOTED_SHARES,
            "Starting Earn share-price materializer"
        );

        loop {
            let delay = match self.materialize_batch().await {
                Ok(0) => IDLE_POLL_INTERVAL,
                Ok(count) => {
                    info!(
                        database = %self.sink.database(),
                        observations = count,
                        "Materialized Earn share-price observations"
                    );
                    ACTIVE_POLL_INTERVAL
                }
                Err(error) => {
                    warn!(
                        error = %error,
                        database = %self.sink.database(),
                        "Earn share-price materialization failed; retrying"
                    );
                    IDLE_POLL_INTERVAL
                }
            };

            tokio::select! {
                () = tokio::time::sleep(delay) => {}
                _ = shutdown.recv() => break,
            }
        }
    }

    async fn materialize_batch(&self) -> Result<usize> {
        let vaults = self.sink.earn_vaults().await?;
        let per_vault = (SAMPLE_BATCH_SIZE / vaults.len().max(1)).max(1);
        let mut remaining = SAMPLE_BATCH_SIZE;
        let mut candidates = Vec::new();

        for vault in vaults {
            if remaining == 0 {
                break;
            }
            let mut next = self
                .sink
                .earn_share_price_candidates(&vault, remaining.min(per_vault))
                .await?;
            remaining -= next.len();
            candidates.append(&mut next);
        }

        let results = stream::iter(candidates)
            .map(|candidate| async move {
                let vault = candidate.vault.clone();
                let result = async {
                    let block_num = u64::try_from(candidate.block_num)
                        .context("Earn sample block number is negative")?;
                    let output = self
                        .rpc
                        .call_contract(&candidate.vault, PREVIEW_REDEEM_CALLDATA, block_num)
                        .await
                        .with_context(|| {
                            format!(
                                "previewRedeem failed for {} at block {}",
                                candidate.vault, candidate.block_num
                            )
                        })?;
                    let quoted_assets = decode_uint256(&output)?;
                    Ok::<_, anyhow::Error>(EarnSharePriceObservation {
                        vault: candidate.vault,
                        bucket: candidate.bucket,
                        block_num: candidate.block_num,
                        block_hash: candidate.block_hash,
                        block_timestamp: candidate.block_timestamp,
                        quoted_assets,
                    })
                }
                .await;
                (vault, result)
            })
            // Preserve candidate order so each vault only advances through a
            // contiguous successful prefix. A failed historical call must not
            // let a later observation move the durable cursor past the gap.
            .buffered(RPC_CONCURRENCY)
            .collect::<Vec<_>>()
            .await;

        let observations = contiguous_successes(results);

        self.sink.write_earn_share_prices(&observations).await
    }
}

fn contiguous_successes<T>(results: Vec<(String, Result<T>)>) -> Vec<T> {
    let mut failed_vaults = HashSet::new();
    let mut values = Vec::new();
    for (vault, result) in results {
        if failed_vaults.contains(&vault) {
            continue;
        }
        match result {
            Ok(value) => values.push(value),
            Err(error) => {
                failed_vaults.insert(vault);
                warn!(error = %error, "Earn share-price backfill paused at failed observation");
            }
        }
    }
    values
}

fn decode_uint256(output: &str) -> Result<U256> {
    let value = output
        .strip_prefix("0x")
        .ok_or_else(|| anyhow!("eth_call output is not 0x-prefixed"))?;
    if value.len() != 64 {
        return Err(anyhow!(
            "eth_call returned {} bytes for uint256, expected 32",
            value.len() / 2
        ));
    }
    U256::from_str_radix(value, 16).context("eth_call returned an invalid uint256")
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::consensus::BlockHeader as _;
    use alloy::primitives::Address;
    use clickhouse::Row;
    use serde::Deserialize;
    use sha3::{Digest, Keccak256};
    use std::str::FromStr;

    use crate::sync::decoder::decode_block;
    use crate::types::LogRow;

    #[test]
    fn deployment_selectors_match_supported_event_versions() {
        let signatures = [
            "EarnStackDeployed(address,address,address,address,address,address,bytes32,address,address,uint8,bytes32,bytes32,bytes32,bytes32)",
            "EarnStackDeployed(address,address,address,address,address,address,bytes32,address,address,uint8,uint64,bytes32,bytes32,bytes32,bytes32)",
            "EarnStackDeployed(address,address,address,address,address,address,bytes32,address,address,uint256,uint8,uint64,bytes32,bytes32,bytes32,bytes32)",
        ];
        let selectors = signatures
            .map(|signature| format!("0x{}", hex::encode(Keccak256::digest(signature.as_bytes()))));
        assert_eq!(selectors.as_slice(), EARN_STACK_DEPLOYED_SELECTORS);
    }

    #[test]
    fn preview_redeem_calldata_uses_the_precision_probe() {
        assert_eq!(&PREVIEW_REDEEM_CALLDATA[..10], "0x4cdad506");
        assert_eq!(
            U256::from_str_radix(&PREVIEW_REDEEM_CALLDATA[10..], 16).unwrap(),
            U256::from(QUOTED_SHARES)
        );
    }

    #[test]
    fn decodes_uint256_rpc_output_as_decimal() {
        assert_eq!(
            decode_uint256("0x000000000000000000000000000000000000000000000000112210f47de98115")
                .unwrap(),
            U256::from(1_234_567_890_123_456_789_u64)
        );
    }

    #[test]
    fn rejects_malformed_uint256_rpc_output() {
        assert!(decode_uint256("1").is_err());
        assert!(decode_uint256("0x01").is_err());
        assert!(decode_uint256(&format!("0x{}", "g".repeat(64))).is_err());
    }

    #[test]
    fn backfill_does_not_advance_a_vault_past_a_gap() {
        let results = vec![
            ("a".to_string(), Ok(1)),
            ("a".to_string(), Err(anyhow!("archive miss"))),
            ("a".to_string(), Ok(3)),
            ("b".to_string(), Ok(4)),
        ];
        assert_eq!(contiguous_successes(results), vec![1, 4]);
    }

    /// Manual smoke test against a live Tempo RPC and a real local ClickHouse.
    ///
    /// Required:
    /// - `TIDX_EARN_LIVE_VAULT`: a live EarnVault address
    /// - ClickHouse at `TIDX_CLICKHOUSE_URL` (defaults to localhost:8123)
    ///
    /// Optional: `TIDX_EARN_LIVE_RPC_URL` overrides the public Tempo testnet RPC;
    /// `TIDX_EARN_LIVE_SAMPLE_BLOCK` selects an older archive block; and
    /// `TIDX_CLICKHOUSE_USER` / `TIDX_CLICKHOUSE_PASSWORD` set local credentials.
    #[tokio::test]
    #[ignore = "requires local ClickHouse and a live Tempo RPC"]
    async fn live_rpc_materializes_preview_redeem_into_clickhouse() {
        let rpc_url = std::env::var("TIDX_EARN_LIVE_RPC_URL")
            .unwrap_or_else(|_| "https://rpc.testnet.tempo.xyz".to_string());
        let clickhouse_url = std::env::var("TIDX_CLICKHOUSE_URL")
            .unwrap_or_else(|_| "http://localhost:8123".to_string());
        let clickhouse_user = std::env::var("TIDX_CLICKHOUSE_USER").ok();
        let clickhouse_password = std::env::var("TIDX_CLICKHOUSE_PASSWORD").ok();
        let vault = std::env::var("TIDX_EARN_LIVE_VAULT")
            .expect("TIDX_EARN_LIVE_VAULT must name a live EarnVault");
        let vault_address = Address::from_str(&vault).expect("invalid TIDX_EARN_LIVE_VAULT");
        let database = format!("tidx_earn_live_{}", hex::encode(rand::random::<[u8; 8]>()));

        let sink = ClickHouseSink::new(
            &clickhouse_url,
            &database,
            clickhouse_user.as_deref(),
            clickhouse_password.as_deref(),
        )
        .expect("create live-smoke ClickHouse sink");
        sink.ensure_schema_only()
            .await
            .expect("initialize live-smoke ClickHouse schema");

        let materializer = EarnSharePriceMaterializer::new(sink.clone(), &rpc_url);
        let sample_block = match std::env::var("TIDX_EARN_LIVE_SAMPLE_BLOCK") {
            Ok(block_num) => materializer
                .rpc
                .get_block(
                    block_num
                        .parse()
                        .expect("invalid TIDX_EARN_LIVE_SAMPLE_BLOCK"),
                    false,
                )
                .await
                .expect("fetch configured live sample block"),
            Err(_) => latest_confirmed_boundary_block(&materializer.rpc)
                .await
                .expect("find live sample block"),
        };
        let block_row = decode_block(&sample_block);
        sink.write_blocks(std::slice::from_ref(&block_row))
            .await
            .expect("seed live sample block");

        let selector = hex::decode(
            EARN_STACK_DEPLOYED_SELECTORS
                .last()
                .unwrap()
                .trim_start_matches("0x"),
        )
        .unwrap();
        let mut vault_topic = vec![0_u8; 12];
        vault_topic.extend_from_slice(vault_address.as_slice());
        sink.write_logs(&[LogRow {
            block_num: block_row.num,
            block_timestamp: block_row.timestamp,
            log_idx: 0,
            tx_idx: 0,
            tx_hash: vec![0_u8; 32],
            address: vec![0_u8; 20],
            selector: Some(selector.clone()),
            topic0: Some(selector),
            topic1: Some(vault_topic),
            topic2: None,
            topic3: None,
            data: Vec::new(),
            is_virtual_forward: false,
        }])
        .await
        .expect("seed Earn deployment discovery log");

        assert_eq!(
            materializer.materialize_batch().await.unwrap(),
            1,
            "one live observation should be materialized"
        );

        let mut clickhouse_client = clickhouse::Client::default()
            .with_url(&clickhouse_url)
            .with_database(&database);
        if let Some(user) = &clickhouse_user {
            clickhouse_client = clickhouse_client.with_user(user);
        }
        if let Some(password) = &clickhouse_password {
            clickhouse_client = clickhouse_client.with_password(password);
        }
        let stored = clickhouse_client
            .query(
                "SELECT vault, toString(bucket) AS bucket, block_num, block_hash, \
                        toString(quoted_assets) AS quoted_assets \
                 FROM earn_share_prices FINAL",
            )
            .fetch_one::<LiveObservationRow>()
            .await
            .expect("read materialized live observation");
        let direct = materializer
            .rpc
            .call_contract(&vault, PREVIEW_REDEEM_CALLDATA, block_row.num as u64)
            .await
            .expect("repeat direct live previewRedeem call");

        assert_eq!(stored.vault, vault.to_lowercase());
        assert_eq!(stored.block_num, block_row.num);
        assert_eq!(
            stored.block_hash,
            format!("0x{}", hex::encode(&block_row.hash))
        );
        assert_eq!(
            stored.quoted_assets,
            decode_uint256(&direct).unwrap().to_string()
        );
        eprintln!(
            "live Earn quote verified: vault={}, bucket={}, block={}, assets={}",
            stored.vault, stored.bucket, stored.block_num, stored.quoted_assets
        );

        clickhouse_client
            .query(&format!("DROP DATABASE {database} SYNC"))
            .execute()
            .await
            .expect("drop live-smoke ClickHouse database");
    }

    async fn latest_confirmed_boundary_block(rpc: &RpcClient) -> Result<crate::tempo::Block> {
        let latest = rpc.latest_block_number().await?;
        let boundary = Utc::now().timestamp().div_euclid(15 * 60) * 15 * 60;
        let cutoff = u64::try_from(boundary - 30).context("sample cutoff predates Unix epoch")?;
        let mut low = 0_u64;
        let mut high = latest;

        while low < high {
            let mid = low + (high - low).div_ceil(2);
            let block = rpc.get_block(mid, false).await?;
            if block.header.timestamp() <= cutoff {
                low = mid;
            } else {
                high = mid - 1;
            }
        }

        rpc.get_block(low, false).await
    }

    #[derive(Row, Deserialize)]
    struct LiveObservationRow {
        vault: String,
        bucket: String,
        block_num: i64,
        block_hash: String,
        quoted_assets: String,
    }
}
