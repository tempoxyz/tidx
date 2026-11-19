//! Hourly Earn vault share price sampler.
//!
//! Writes `earn_vault_price_hours`: for every vault in `earn_vaults` and every
//! completed UTC hour containing a block since deployment, `previewRedeem(1e18)`
//! read with `eth_call` at that hour's last block. The head lane polls just
//! the latest completed hour. A separate repair lane pages backwards through
//! bounded windows and wraps at deployment, including after a reorg.
//!
//! Only values read from the RPC are written: a failed, reverted or malformed
//! call leaves the hour missing and is retried with backoff. Writes are
//! idempotent because the table is a `ReplacingMergeTree` keyed on
//! `(vault, hour)`. Reorg cleanup prunes rows whose block was replaced, and
//! the next visit to that hour can write them again.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use alloy::primitives::U256;
use anyhow::{Result, anyhow};
use clickhouse::Row;
use serde::Deserialize;
use tokio::sync::{Semaphore, broadcast};
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

// Bound the amount of block data examined even if every price is present.
const REPAIR_WINDOW_HOURS: u32 = 24;

#[derive(Debug, Copy, Clone)]
enum Lane {
    Head,
    Repair,
}

/// Visit at most one page per tick. Advance past *examined* pairs, not just
/// successes: an unavailable successor or a reverting contract cannot pin
/// the scan at the newest hole.
#[derive(Clone)]
struct Cursor {
    end: u32,
    after: Option<(u32, String)>,
}

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
    head: Mutex<Option<Cursor>>,
    repair: Mutex<Option<Cursor>>,
    slots: Semaphore,
    failures: Mutex<HashMap<(String, u32), (u32, Instant)>>,
}

#[derive(Row, Deserialize)]
struct MissingRow {
    vault: String,
    hour: u32,
    block_num: i64,
}

#[derive(Row, Deserialize)]
struct Successor {
    num: i64,
    hour: u32,
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
            slots: Semaphore::new(config.concurrency.max(1)),
            config,
            head: Mutex::default(),
            repair: Mutex::default(),
            failures: Mutex::default(),
        }
    }

    pub async fn run(self, mut shutdown: broadcast::Receiver<()>) {
        tokio::select! {
            _ = shutdown.recv() => info!(chain_id = self.chain_id, "Earn price sampler shutting down"),
            () = self.poll_loop(Lane::Head) => {},
            () = self.poll_loop(Lane::Repair) => {},
        }
    }

    async fn poll_loop(&self, lane: Lane) {
        let interval = Duration::from_secs(self.config.interval_secs.max(1));
        loop {
            match self.poll(lane).await {
                Ok(report) if report.written > 0 || report.failed > 0 => info!(
                    chain_id = self.chain_id,
                    ?lane,
                    written = report.written,
                    failed = report.failed,
                    deferred = report.deferred,
                    "Earn price sampling round"
                ),
                Ok(_) => {}
                Err(e) => {
                    warn!(chain_id = self.chain_id, ?lane, error = %e, "Earn price sampling failed");
                }
            }
            tokio::time::sleep(interval).await;
        }
    }

    /// Run the two lanes once in priority order (one head and one repair
    /// page). The long-running worker polls them independently.
    pub async fn run_once(&self) -> Result<SampleReport> {
        let mut report = self.poll(Lane::Head).await?;
        let repair = self.poll(Lane::Repair).await?;
        report.written += repair.written;
        report.failed += repair.failed;
        report.deferred += repair.deferred;
        Ok(report)
    }

    async fn poll(&self, lane: Lane) -> Result<SampleReport> {
        let rows = self.next_page(lane).await?;
        let report = self.sample_page(lane, rows).await?;
        let label = match lane {
            Lane::Head => "head",
            Lane::Repair => "repair",
        };
        metrics::set_earn_price_page_missing(
            self.chain_id,
            label,
            (report.deferred + report.failed) as u64,
        );
        Ok(report)
    }

    async fn sample_page(&self, lane: Lane, mut rows: Vec<MissingRow>) -> Result<SampleReport> {
        let mut report = SampleReport::default();
        let now = Instant::now();
        {
            let failures = self.failures.lock().unwrap_or_else(|e| e.into_inner());
            rows.retain(|r| {
                let ready = !matches!(failures.get(&(r.vault.clone(), r.hour)), Some((_, retry_at)) if *retry_at > now);
                if !ready { report.deferred += 1; }
                ready
            });
        }
        if rows.is_empty() {
            return Ok(report);
        }
        let due = rows.len();
        self.retain_confirmed_boundaries(&mut rows).await?;
        report.deferred += due - rows.len();

        // Reserve capacity for the head even if an entire archive wave
        // hangs. With a single configured slot Tokio's FIFO semaphore lets
        // the head run after the current archive RPC, not after the page.
        let width = match lane {
            Lane::Head => 1,
            Lane::Repair => self.config.concurrency.saturating_sub(1).max(1),
        };
        for wave in rows.chunks(width) {
            let slots = self.slots.acquire_many(u32::try_from(wave.len())?).await?;
            let active: HashSet<_> = self
                .sink
                .client()
                .query("SELECT vault FROM earn_vaults")
                .fetch_all::<String>()
                .await?
                .into_iter()
                .collect();
            let results = futures::future::join_all(
                wave.iter()
                    .filter(|r| active.contains(&r.vault))
                    .map(|row| async move {
                        (row, self.preview_redeem(&row.vault, row.block_num).await)
                    }),
            )
            .await;
            drop(slots);
            let mut values = Vec::new();
            let mut keys = Vec::new();
            for (row, price) in results {
                let key = (row.vault.clone(), row.hour);
                match price {
                    Ok(price) => {
                        self.failures
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .remove(&key);
                        values.push(format!(
                            "('{}', {}, {}, '{}')",
                            row.vault, row.hour, row.block_num, price
                        ));
                        keys.push(format!(
                            "('{}', toDateTime({}, 'UTC'))",
                            row.vault, row.hour
                        ));
                    }
                    Err(e) => {
                        debug!(vault = %key.0, hour = key.1, block_num = row.block_num, error = %e, "previewRedeem failed");
                        self.record_failure(key);
                        metrics::record_earn_price_samples(self.chain_id, "failed", 1);
                        report.failed += 1;
                    }
                }
            }
            if !values.is_empty() {
                // Registry edits/lease expiry can race the RPC: check again
                // inside the insert. Commit this wave before dispatching more.
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
                    "SELECT uniqExact(vault, hour) FROM earn_vault_price_hours WHERE (vault, hour) IN ({})",
                    keys.join(", ")
                )).fetch_one().await?;
                metrics::record_earn_price_samples(self.chain_id, "written", count);
                report.written += usize::try_from(count)?;
            }
        }
        Ok(report)
    }

    async fn next_page(&self, lane: Lane) -> Result<Vec<MissingRow>> {
        let Some(latest) = self.last_complete_hour().await? else {
            return Ok(Vec::new());
        };
        let (upper, span, state) = match lane {
            Lane::Head => {
                metrics::set_earn_price_head_hour(self.chain_id, latest);
                (latest, 1, &self.head)
            }
            Lane::Repair => {
                let Some(end) = latest.checked_sub(HOUR_SECS) else {
                    return Ok(Vec::new());
                };
                (end, REPAIR_WINDOW_HOURS, &self.repair)
            }
        };
        let oldest: Option<u32> = self.sink.client().query(
            "SELECT toUInt32(toStartOfHour(toDateTime(deployed_at, 'UTC'))) FROM earn_vaults ORDER BY deployed_at LIMIT 1"
        ).fetch_optional().await?;
        let Some(oldest) = oldest.filter(|h| *h <= upper) else {
            *state.lock().unwrap_or_else(|e| e.into_inner()) = None;
            return Ok(Vec::new());
        };
        let cursor = state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
            .filter(|c| {
                (oldest..=upper).contains(&c.end) && (!matches!(lane, Lane::Head) || c.end == upper)
            })
            .unwrap_or(Cursor {
                end: upper,
                after: None,
            });
        let start = oldest.max(cursor.end.saturating_sub((span - 1) * HOUR_SECS));
        let mut rows = self.missing_page(start, &cursor).await?;
        let limit = self.config.batch.max(1);
        let next = if rows.len() > limit {
            rows.truncate(limit);
            rows.last().map(|r| Cursor {
                end: cursor.end,
                after: Some((r.hour, r.vault.clone())),
            })
        } else if matches!(lane, Lane::Repair) && start > oldest {
            Some(Cursor {
                end: start - HOUR_SECS,
                after: None,
            })
        } else {
            None // Revisit the newest window, including holes and expired backoff.
        };
        *state.lock().unwrap_or_else(|e| e.into_inner()) = next;

        Ok(rows)
    }

    async fn retain_confirmed_boundaries(&self, rows: &mut Vec<MissingRow>) -> Result<()> {
        // The immediate successor can be days later during a chain halt.
        // Test its height and timestamp, not the existence of *any* later
        // block, and do not limit it to the next clock hour.
        let heights: HashSet<_> = rows
            .iter()
            .filter_map(|r| r.block_num.checked_add(1))
            .collect();
        let sql = "SELECT num, toUInt32(toStartOfHour(toDateTime(timestamp, 'UTC'))) AS hour FROM blocks WHERE num IN ?";
        let successors: HashMap<_, _> = self
            .sink
            .client()
            .query(sql)
            .bind(heights.into_iter().collect::<Vec<_>>())
            .fetch_all::<Successor>()
            .await?
            .into_iter()
            .map(|r| (r.num, r.hour))
            .collect();
        rows.retain(|row| {
            row.block_num
                .checked_add(1)
                .and_then(|n| successors.get(&n))
                .is_some_and(|h| *h > row.hour)
        });

        Ok(())
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

    /// The UTC hour immediately before the indexed tip's hour. The boundary
    /// check still has to prove which block, if any, closes this hour.
    async fn last_complete_hour(&self) -> Result<Option<u32>> {
        let tip: Option<u32> = self.sink.client().query(
            "SELECT toUInt32(toStartOfHour(toDateTime(timestamp, 'UTC'))) FROM blocks ORDER BY num DESC LIMIT 1"
        ).fetch_optional().await.map_err(|e| anyhow!("read block tip: {e}"))?;
        Ok(tip.and_then(|h| h.checked_sub(HOUR_SECS)))
    }

    /// Generate candidates from block-containing hours only. An hour with
    /// no indexed block has no price to read; a later indexing repair makes
    /// it appear on the next sweep. Price existence needs no FINAL and both
    /// sides of the anti-join are bounded to this window.
    async fn missing_page(&self, start: u32, cursor: &Cursor) -> Result<Vec<MissingRow>> {
        let end = cursor.end;
        let after = cursor
            .after
            .as_ref()
            .map_or_else(String::new, |(h, vault)| {
                // Only validated lowercase addresses reach the cursor.
                format!(" AND (b.h < {h} OR (b.h = {h} AND v.vault > '{vault}'))")
            });
        let sql = format!(
            "SELECT v.vault AS vault, b.h AS hour, b.block_num AS block_num FROM \
             (SELECT toUInt32(toStartOfHour(toDateTime(timestamp, 'UTC'))) AS h, max(num) AS block_num \
              FROM blocks WHERE timestamp >= toDateTime64({start}, 3, 'UTC') \
              AND timestamp < toDateTime64({exclusive_end}, 3, 'UTC') GROUP BY h) AS b \
             CROSS JOIN (SELECT vault, toUInt32(toStartOfHour(toDateTime(min(deployed_at), 'UTC'))) AS first_hour \
                         FROM earn_vaults WHERE match(vault, '^0x[0-9a-f]{{40}}$') GROUP BY vault) AS v \
             WHERE b.h >= v.first_hour {after} \
               AND (v.vault, toDateTime(b.h, 'UTC')) NOT IN ( \
                   SELECT vault, hour FROM earn_vault_price_hours \
                   WHERE vault IN (SELECT vault FROM earn_vaults) AND hour BETWEEN \
                       toDateTime({start}, 'UTC') AND toDateTime({end}, 'UTC')) \
             ORDER BY b.h DESC, v.vault LIMIT {limit}",
            exclusive_end = u64::from(end) + u64::from(HOUR_SECS),
            limit = self.config.batch.max(1).saturating_add(1)
        );
        let rows: Vec<MissingRow> = self
            .sink
            .client()
            .query(&sql)
            .fetch_all()
            .await
            .map_err(|e| anyhow!("find missing earn prices in {start}..={end}: {e}"))?;
        Ok(rows.into_iter().filter(|r| is_address(&r.vault)).collect())
    }

    async fn preview_redeem(&self, vault: &str, block_num: i64) -> Result<String> {
        let block = u64::try_from(block_num).map_err(|_| anyhow!("negative block {block_num}"))?;
        let data = self.rpc.eth_call(vault, PREVIEW_REDEEM_1E18, block).await?;
        decode_uint256(&data)
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
