use metrics::{counter, gauge, histogram};
use std::time::Instant;

// Per-chain metrics with chain_id label

pub fn record_blocks_indexed(chain_id: u64, count: u64) {
    let labels = [("chain_id", chain_id.to_string())];
    counter!("tidx_blocks_indexed_total", &labels).increment(count);
}

pub fn record_txs_indexed(chain_id: u64, count: u64) {
    let labels = [("chain_id", chain_id.to_string())];
    counter!("tidx_txs_indexed_total", &labels).increment(count);
}

pub fn record_logs_indexed(chain_id: u64, count: u64) {
    let labels = [("chain_id", chain_id.to_string())];
    counter!("tidx_logs_indexed_total", &labels).increment(count);
}

pub fn set_sync_head(chain_id: u64, block_num: u64) {
    let labels = [("chain_id", chain_id.to_string())];
    gauge!("tidx_sync_head_block", &labels).set(block_num as f64);
}

pub fn set_synced_block(chain_id: u64, block_num: u64) {
    let labels = [("chain_id", chain_id.to_string())];
    gauge!("tidx_synced_block", &labels).set(block_num as f64);
}

pub fn set_sync_lag(chain_id: u64, lag: u64) {
    let labels = [("chain_id", chain_id.to_string())];
    gauge!("tidx_sync_lag_blocks", &labels).set(lag as f64);
}

pub fn set_backfill_block(chain_id: u64, block_num: u64) {
    let labels = [("chain_id", chain_id.to_string())];
    gauge!("tidx_backfill_block", &labels).set(block_num as f64);
}

pub fn set_backfill_remaining(chain_id: u64, remaining: u64) {
    let labels = [("chain_id", chain_id.to_string())];
    gauge!("tidx_backfill_remaining_blocks", &labels).set(remaining as f64);
}

pub fn set_sync_rate(chain_id: u64, blocks_per_sec: f64) {
    let labels = [("chain_id", chain_id.to_string())];
    gauge!("tidx_sync_blocks_per_second", &labels).set(blocks_per_sec);
}

pub fn record_rpc_request(method: &str, duration: std::time::Duration, success: bool) {
    let labels = [
        ("method", method.to_string()),
        ("success", success.to_string()),
    ];
    counter!("tidx_rpc_requests_total", &labels).increment(1);
    histogram!("tidx_rpc_request_duration_seconds", &labels).record(duration.as_secs_f64());
}

pub fn record_query_duration(duration: std::time::Duration) {
    histogram!("tidx_query_duration_seconds").record(duration.as_secs_f64());
}

pub fn record_query_rows(count: u64) {
    histogram!("tidx_query_rows").record(count as f64);
}

/// Tracks sync progress and calculates rate/ETA
pub struct SyncProgress {
    chain_id: u64,
    last_report: Instant,
    last_block: u64,
    blocks_since_report: u64,
}

impl SyncProgress {
    pub fn new(chain_id: u64, start_block: u64) -> Self {
        Self {
            chain_id,
            last_report: Instant::now(),
            last_block: start_block,
            blocks_since_report: 0,
        }
    }

    pub fn update(&mut self, current_block: u64, blocks_synced: u64) {
        self.blocks_since_report += blocks_synced;

        let elapsed = self.last_report.elapsed();
        if elapsed.as_secs() >= 5 {
            let rate = self.blocks_since_report as f64 / elapsed.as_secs_f64();
            set_sync_rate(self.chain_id, rate);

            self.last_report = Instant::now();
            self.last_block = current_block;
            self.blocks_since_report = 0;
        }
    }

    pub fn report_backfill(&mut self, current_block: u64, target_block: u64, blocks_synced: u64) {
        self.blocks_since_report += blocks_synced;
        set_backfill_block(self.chain_id, current_block);
        set_backfill_remaining(self.chain_id, current_block.saturating_sub(target_block));

        let elapsed = self.last_report.elapsed();
        if elapsed.as_secs() >= 5 {
            let rate = self.blocks_since_report as f64 / elapsed.as_secs_f64();
            set_sync_rate(self.chain_id, rate);

            let remaining = current_block.saturating_sub(target_block);
            let eta_secs = if rate > 0.0 {
                remaining as f64 / rate
            } else {
                0.0
            };

            tracing::info!(
                chain_id = self.chain_id,
                block = current_block,
                remaining = remaining,
                rate = format!("{:.1} blk/s", rate),
                eta = format_eta(eta_secs),
                "Backfill progress"
            );

            self.last_report = Instant::now();
            self.last_block = current_block;
            self.blocks_since_report = 0;
        }
    }

    pub fn report_forward(&mut self, synced_block: u64, head_block: u64, blocks_synced: u64) {
        self.blocks_since_report += blocks_synced;
        set_synced_block(self.chain_id, synced_block);
        set_sync_head(self.chain_id, head_block);
        set_sync_lag(self.chain_id, head_block.saturating_sub(synced_block));

        let elapsed = self.last_report.elapsed();
        if elapsed.as_secs() >= 5 {
            let rate = self.blocks_since_report as f64 / elapsed.as_secs_f64();
            set_sync_rate(self.chain_id, rate);

            let lag = head_block.saturating_sub(synced_block);

            tracing::info!(
                chain_id = self.chain_id,
                synced = synced_block,
                head = head_block,
                lag = lag,
                rate = format!("{:.1} blk/s", rate),
                "Sync progress"
            );

            self.last_report = Instant::now();
            self.last_block = synced_block;
            self.blocks_since_report = 0;
        }
    }
}

// Sink metrics (dual-sink write path)

pub fn record_sink_write_duration(sink: &str, table: &str, duration: std::time::Duration) {
    let labels = [
        ("sink", sink.to_string()),
        ("table", table.to_string()),
    ];
    histogram!("tidx_sink_write_duration_seconds", &labels).record(duration.as_secs_f64());
}

pub fn record_sink_write_rows(sink: &str, table: &str, count: u64) {
    let labels = [
        ("sink", sink.to_string()),
        ("table", table.to_string()),
    ];
    counter!("tidx_sink_write_rows_total", &labels).increment(count);
}

pub fn record_sink_error(sink: &str) {
    let labels = [("sink", sink.to_string())];
    counter!("tidx_sink_errors_total", &labels).increment(1);
}

// ClickHouse OLAP metrics

pub fn record_clickhouse_query(duration: std::time::Duration, success: bool) {
    let labels = [("success", success.to_string())];
    counter!("tidx_clickhouse_queries_total", &labels).increment(1);
    histogram!("tidx_clickhouse_query_duration_seconds", &labels).record(duration.as_secs_f64());
}

pub fn record_clickhouse_rows(count: u64) {
    histogram!("tidx_clickhouse_query_rows").record(count as f64);
}

// ── Per-sink rolling write rate tracker ───────────────────────────────────

use std::sync::OnceLock;

struct RateWindow {
    last_reset: Instant,
    rows_since_reset: u64,
    current_rate: f64,
}

static SINK_RATES: OnceLock<std::sync::Mutex<std::collections::HashMap<String, RateWindow>>> =
    OnceLock::new();

/// Record that `count` blocks were written to `sink` (e.g., "postgres", "clickhouse").
pub fn update_sink_block_rate(sink: &str, count: u64) {
    let rates = SINK_RATES.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()));
    let mut map = rates.lock().unwrap();
    let entry = map
        .entry(sink.to_string())
        .or_insert_with(|| RateWindow {
            last_reset: Instant::now(),
            rows_since_reset: 0,
            current_rate: 0.0,
        });
    entry.rows_since_reset += count;
    let elapsed = entry.last_reset.elapsed();
    if elapsed.as_secs() >= 3 {
        entry.current_rate = entry.rows_since_reset as f64 / elapsed.as_secs_f64();
        entry.rows_since_reset = 0;
        entry.last_reset = Instant::now();
    }
}

/// Get the current write rate (blocks/sec) for a sink, or None if not yet measured.
pub fn get_sink_block_rate(sink: &str) -> Option<f64> {
    let rates = SINK_RATES.get()?;
    let map = rates.lock().unwrap();
    map.get(sink)
        .map(|w| w.current_rate)
        .filter(|r| *r > 0.0)
}

fn format_eta(secs: f64) -> String {
    if secs <= 0.0 || secs.is_nan() || secs.is_infinite() {
        return "unknown".to_string();
    }

    let secs = secs as u64;
    if secs < 60 {
        format!("{secs}s")
    } else if secs < 3600 {
        format!("{}m {}s", secs / 60, secs % 60)
    } else if secs < 86400 {
        format!("{}h {}m", secs / 3600, (secs % 3600) / 60)
    } else {
        format!("{}d {}h", secs / 86400, (secs % 86400) / 3600)
    }
}
