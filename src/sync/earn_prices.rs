//! Hourly Earn vault share price sampler.
//!
//! Writes `earn_vault_price_hours`: for every vault in `earn_vaults` and every
//! completed UTC hour since its deployment, `previewRedeem(1e18)` read with
//! `eth_call` at the last block of that hour. Each round samples the newest
//! missing hours first, so the hour that just completed is written within one
//! interval, and older hours are backfilled with historical (archive) calls.
//!
//! Only values read from the RPC are written: a failed, reverted or malformed
//! call leaves the hour missing and is retried with backoff. Writes are
//! idempotent because the table is a `ReplacingMergeTree` keyed on
//! `(vault, hour)`. Reorg cleanup prunes rows whose block was replaced, and
//! the next round writes them again.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use alloy::primitives::U256;
use anyhow::{Result, anyhow};
use clickhouse::Row;
use serde::Deserialize;
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

use crate::config::EarnPricesConfig;
use crate::metrics;
use crate::sync::ch_sink::ClickHouseSink;
use crate::sync::fetcher::RpcClient;

/// `previewRedeem(uint256)` with `1e18` shares.
const PREVIEW_REDEEM_1E18: &str =
    "0x4cdad5060000000000000000000000000000000000000000000000000de0b6b3a7640000";

const HOUR_SECS: u32 = 3600;
const MAX_BACKOFF: Duration = Duration::from_secs(3600);
const MAX_TRACKED_FAILURES: usize = 50_000;

/// Outcome of one sampling round.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SampleReport {
    /// Rows written to `earn_vault_price_hours`.
    pub written: usize,
    /// Samples whose `eth_call` failed; their hours stay missing.
    pub failed: usize,
    /// Missing hours skipped because the hour's last block isn't indexed yet
    /// or a recent failure is still backing off.
    pub deferred: usize,
}

pub struct EarnPriceSampler {
    sink: ClickHouseSink,
    rpc: RpcClient,
    chain_id: u64,
    config: EarnPricesConfig,
    failures: Mutex<HashMap<(String, u32), (u32, Instant)>>,
}

#[derive(Row, Deserialize)]
struct MissingRow {
    vault: String,
    hour: u32,
}

#[derive(Row, Deserialize)]
struct HourBlockRow {
    hour: u32,
    block_num: i64,
}

#[derive(Row, Deserialize)]
struct StatusRow {
    missing: u64,
    caught_up_through: u32,
}

impl EarnPriceSampler {
    pub fn new(
        sink: ClickHouseSink,
        rpc: RpcClient,
        chain_id: u64,
        config: EarnPricesConfig,
    ) -> Self {
        Self {
            sink,
            rpc,
            chain_id,
            config,
            failures: Mutex::default(),
        }
    }

    pub async fn run(self, mut shutdown: broadcast::Receiver<()>) {
        let interval = Duration::from_secs(self.config.interval_secs.max(1));
        loop {
            match self.run_once().await {
                Ok(report) if report.written > 0 || report.failed > 0 => info!(
                    chain_id = self.chain_id,
                    written = report.written,
                    failed = report.failed,
                    deferred = report.deferred,
                    "Earn price sampling round"
                ),
                Ok(_) => {}
                Err(e) => warn!(chain_id = self.chain_id, error = %e, "Earn price sampling failed"),
            }
            tokio::select! {
                _ = shutdown.recv() => {
                    info!(chain_id = self.chain_id, "Earn price sampler shutting down");
                    return;
                }
                () = tokio::time::sleep(interval) => {}
            }
        }
    }

    /// One sampling round: find missing (vault, hour) pairs, read their prices
    /// at each hour's last block, and write the ones that succeeded.
    pub async fn run_once(&self) -> Result<SampleReport> {
        let mut report = SampleReport::default();
        let Some(last_complete) = self.last_complete_hour().await? else {
            return Ok(report);
        };

        let tracked = self
            .failures
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .len();
        let limit = self.config.batch.max(1) + tracked.min(MAX_TRACKED_FAILURES);
        let missing = self.missing_pairs(last_complete, limit).await?;

        let now = Instant::now();
        let due: Vec<MissingRow> = {
            let failures = self.failures.lock().unwrap_or_else(|e| e.into_inner());
            missing
                .into_iter()
                .filter(|row| match failures.get(&(row.vault.clone(), row.hour)) {
                    Some((_, retry_at)) if *retry_at > now => {
                        report.deferred += 1;
                        false
                    }
                    _ => true,
                })
                .take(self.config.batch.max(1))
                .collect()
        };

        if !due.is_empty() {
            let hours: BTreeSet<u32> = due.iter().map(|row| row.hour).collect();
            let blocks = self.hour_last_blocks(&hours).await?;

            let calls: Vec<_> = due
                .into_iter()
                .filter_map(|row| match blocks.get(&row.hour) {
                    Some(&block_num) => Some((row, block_num)),
                    None => {
                        report.deferred += 1;
                        None
                    }
                })
                .collect();
            let mut values = Vec::new();
            let mut keys = Vec::new();
            // A backfill round may outlive its registry snapshot. Recheck the
            // small live set before dispatching each wave of archive reads.
            for wave in calls.chunks(self.config.concurrency.max(1)) {
                let active: HashSet<String> = self
                    .sink
                    .client()
                    .query("SELECT vault FROM earn_vaults")
                    .fetch_all::<String>()
                    .await?
                    .into_iter()
                    .collect();
                if active.is_empty() {
                    break;
                }
                let results = futures::future::join_all(
                    wave.iter()
                        .filter(|(row, _)| active.contains(&row.vault))
                        .map(|(row, block_num)| async move {
                            (
                                row,
                                *block_num,
                                self.preview_redeem(&row.vault, *block_num).await,
                            )
                        }),
                )
                .await;
                for (row, block_num, price) in results {
                    let key = (row.vault.clone(), row.hour);
                    match price {
                        Ok(price) => {
                            self.failures
                                .lock()
                                .unwrap_or_else(|e| e.into_inner())
                                .remove(&key);
                            // missing_pairs validates the address; the remaining literals
                            // are typed integers or a decoded uint256 in decimal.
                            values.push(format!(
                                "('{}', {}, {}, '{}')",
                                key.0, key.1, block_num, price
                            ));
                            keys.push(format!("('{}', toDateTime({}, 'UTC'))", key.0, key.1));
                        }
                        Err(e) => {
                            debug!(vault = %key.0, hour = key.1, block_num, error = %e, "previewRedeem failed");
                            report.failed += 1;
                            self.record_failure(key);
                        }
                    }
                }
            }

            if !values.is_empty() {
                // An RPC result may arrive after removal or lease expiry.
                // Apply membership in the insert itself, not only in Rust.
                let sql = format!(
                    "INSERT INTO earn_vault_price_hours (vault, hour, block_num, assets_per_1e18_shares) \
                     SELECT vault, toDateTime(h, 'UTC'), n, toUInt256(p) \
                     FROM values('vault String, h UInt32, n Int64, p String', {}) \
                     WHERE vault IN (SELECT vault FROM earn_vaults)",
                    values.join(", ")
                );
                self.sink
                    .client()
                    .query(&sql)
                    .execute()
                    .await
                    .map_err(|e| anyhow!("insert earn_vault_price_hours: {e}"))?;
                let count: u64 = self.sink.client().query(&format!(
                    "SELECT count() FROM earn_vault_price_hours FINAL WHERE (vault, hour) IN ({})",
                    keys.join(", ")
                )).fetch_one().await?;
                report.written = usize::try_from(count)?;
            }
        }

        metrics::record_earn_price_samples(self.chain_id, "written", report.written as u64);
        metrics::record_earn_price_samples(self.chain_id, "failed", report.failed as u64);
        self.record_status(last_complete).await?;
        Ok(report)
    }

    fn record_failure(&self, key: (String, u32)) {
        let mut failures = self.failures.lock().unwrap_or_else(|e| e.into_inner());
        if failures.len() >= MAX_TRACKED_FAILURES && !failures.contains_key(&key) {
            return;
        }
        let attempts = failures.get(&key).map_or(0, |(attempts, _)| *attempts) + 1;
        let backoff =
            Duration::from_secs(30u64.saturating_mul(1 << attempts.min(7))).min(MAX_BACKOFF);
        failures.insert(key, (attempts, Instant::now() + backoff));
    }

    /// The newest UTC hour whose last block is indexed: a block from a later
    /// hour exists.
    async fn last_complete_hour(&self) -> Result<Option<u32>> {
        let tip: u32 = self
            .sink
            .client()
            .query("SELECT toUInt32(toStartOfHour(toDateTime(max(timestamp), 'UTC'))) FROM blocks")
            .fetch_one()
            .await
            .map_err(|e| anyhow!("read block tip: {e}"))?;
        Ok(tip.checked_sub(HOUR_SECS).filter(|hour| *hour > 0))
    }

    fn vault_hours_sql(last_complete: u32) -> String {
        format!(
            "SELECT vault, arrayJoin(arrayMap(i -> first_hour + toIntervalHour(i), \
                 range(toUInt64(greatest(dateDiff('hour', first_hour, toDateTime({last_complete}, 'UTC')) + 1, 0))))) AS hour \
             FROM ( \
                 SELECT vault, toStartOfHour(toDateTime(min(deployed_at), 'UTC')) AS first_hour \
                 FROM earn_vaults GROUP BY vault \
             )"
        )
    }

    async fn missing_pairs(&self, last_complete: u32, limit: usize) -> Result<Vec<MissingRow>> {
        let sql = format!(
            "SELECT vault, toUInt32(hour) AS hour FROM ({}) \
             WHERE (vault, hour) NOT IN (SELECT vault, hour FROM earn_vault_price_hours FINAL) \
             ORDER BY hour DESC, vault LIMIT {limit}",
            Self::vault_hours_sql(last_complete)
        );
        let rows: Vec<MissingRow> = self
            .sink
            .client()
            .query(&sql)
            .fetch_all()
            .await
            .map_err(|e| anyhow!("find missing earn price hours: {e}"))?;
        Ok(rows
            .into_iter()
            .filter(|row| is_address(&row.vault))
            .collect())
    }

    /// The last block of each hour, for hours whose next block is also
    /// indexed, so a gap in `blocks` can never pick an earlier block.
    async fn hour_last_blocks(&self, hours: &BTreeSet<u32>) -> Result<HashMap<u32, i64>> {
        let (Some(&from), Some(&to)) = (hours.first(), hours.last()) else {
            return Ok(HashMap::new());
        };
        let to = to + HOUR_SECS;
        let sql = format!(
            "SELECT toUInt32(hour) AS hour, block_num FROM ( \
                 SELECT toStartOfHour(toDateTime(timestamp, 'UTC')) AS hour, max(num) AS block_num \
                 FROM blocks \
                 WHERE timestamp >= toDateTime64({from}, 3, 'UTC') AND timestamp < toDateTime64({to}, 3, 'UTC') \
                 GROUP BY hour \
             ) \
             WHERE block_num + 1 IN ( \
                 SELECT num FROM blocks \
                 WHERE timestamp >= toDateTime64({from}, 3, 'UTC') \
                   AND timestamp < toDateTime64({next}, 3, 'UTC') \
             )",
            next = to + HOUR_SECS
        );
        let rows: Vec<HourBlockRow> = self
            .sink
            .client()
            .query(&sql)
            .fetch_all()
            .await
            .map_err(|e| anyhow!("find hour boundary blocks: {e}"))?;
        Ok(rows
            .into_iter()
            .filter(|row| hours.contains(&row.hour))
            .map(|row| (row.hour, row.block_num))
            .collect())
    }

    async fn preview_redeem(&self, vault: &str, block_num: i64) -> Result<String> {
        let block = u64::try_from(block_num).map_err(|_| anyhow!("negative block {block_num}"))?;
        let data = self.rpc.eth_call(vault, PREVIEW_REDEEM_1E18, block).await?;
        decode_uint256(&data)
    }

    async fn record_status(&self, last_complete: u32) -> Result<()> {
        let sql = format!(
            "SELECT \
                 toUInt64(sum(greatest(toInt64(expected) - toInt64(have), 0))) AS missing, \
                 toUInt32(if(count() = 0, toDateTime({last_complete}, 'UTC'), min(newest))) AS caught_up_through \
             FROM ( \
                 SELECT \
                     v.expected AS expected, \
                     p.have AS have, \
                     if(p.newest = toDateTime(0, 'UTC'), v.first_hour - INTERVAL 1 HOUR, p.newest) AS newest \
                 FROM ( \
                     SELECT vault, \
                         toStartOfHour(toDateTime(min(deployed_at), 'UTC')) AS first_hour, \
                         toUInt64(greatest(dateDiff('hour', first_hour, toDateTime({last_complete}, 'UTC')) + 1, 0)) AS expected \
                     FROM earn_vaults GROUP BY vault \
                 ) AS v \
                 LEFT JOIN ( \
                     SELECT vault, count() AS have, max(hour) AS newest \
                     FROM earn_vault_price_hours FINAL \
                     WHERE hour <= toDateTime({last_complete}, 'UTC') \
                     GROUP BY vault \
                 ) AS p ON v.vault = p.vault \
             ) \
             SETTINGS join_use_nulls = 0"
        );
        let status: StatusRow = self
            .sink
            .client()
            .query(&sql)
            .fetch_one()
            .await
            .map_err(|e| anyhow!("read earn price status: {e}"))?;
        let caught_up_end = i64::from(status.caught_up_through) + i64::from(HOUR_SECS);
        let lag = (chrono::Utc::now().timestamp() - caught_up_end).max(0) as u64;
        metrics::set_earn_price_missing_hours(self.chain_id, status.missing);
        metrics::set_earn_price_lag(self.chain_id, lag);
        Ok(())
    }
}

fn is_address(value: &str) -> bool {
    value.len() == 42
        && value.starts_with("0x")
        && value[2..]
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// Decodes a single ABI `uint256` return value as a decimal string.
fn decode_uint256(data: &[u8]) -> Result<String> {
    if data.len() != 32 {
        return Err(anyhow!(
            "expected 32 bytes of return data, got {}",
            data.len()
        ));
    }
    Ok(U256::from_be_slice(data).to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preview_redeem_calldata_is_selector_and_1e18() {
        let selector = &alloy::primitives::keccak256("previewRedeem(uint256)")[..4];
        let expected = format!(
            "0x{}{:064x}",
            hex::encode(selector),
            1_000_000_000_000_000_000u128
        );
        assert_eq!(PREVIEW_REDEEM_1E18, expected);
    }

    #[test]
    fn decode_uint256_rejects_empty_and_short_return_data() {
        assert!(decode_uint256(&[]).is_err());
        assert!(decode_uint256(&[0; 31]).is_err());
        let mut word = [0u8; 32];
        word[24..].copy_from_slice(&1_000_123u64.to_be_bytes());
        assert_eq!(decode_uint256(&word).unwrap(), "1000123");
    }

    #[test]
    fn addresses_must_be_lowercase_hex() {
        assert!(is_address("0xd730394f3bb85a4828e35fc5e361dcc9f894fa4f"));
        assert!(!is_address("0xD730394f3bb85a4828e35fc5e361dcc9f894fa4f"));
        assert!(!is_address("0xd730394f3bb85a4828e35fc5e361dcc9f894fa4"));
        assert!(!is_address("0xd730394f3bb85a4828e35fc5e361dcc9f894fa4g"));
    }
}
