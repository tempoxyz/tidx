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

pub fn set_backfill_block(chain_id: u64, sink: &str, block_num: u64) {
    let labels = [("chain_id", chain_id.to_string()), ("sink", sink.to_string())];
    gauge!("tidx_backfill_block", &labels).set(block_num as f64);
}

pub fn set_backfill_remaining(chain_id: u64, sink: &str, remaining: u64) {
    let labels = [("chain_id", chain_id.to_string()), ("sink", sink.to_string())];
    gauge!("tidx_backfill_remaining_blocks", &labels).set(remaining as f64);
}

pub fn set_sync_rate(chain_id: u64, blocks_per_sec: f64) {
    let labels = [("chain_id", chain_id.to_string())];
    gauge!("tidx_sync_blocks_per_second", &labels).set(blocks_per_sec);
}

pub fn set_synced(chain_id: u64, synced: bool) {
    let labels = [("chain_id", chain_id.to_string())];
    gauge!("tidx_synced", &labels).set(if synced { 1.0 } else { 0.0 });
}

pub fn set_gap_blocks(chain_id: u64, sink: &str, blocks: u64) {
    let labels = [("chain_id", chain_id.to_string()), ("sink", sink.to_string())];
    gauge!("tidx_gap_blocks", &labels).set(blocks as f64);
}

pub fn set_gap_count(chain_id: u64, sink: &str, count: u64) {
    let labels = [("chain_id", chain_id.to_string()), ("sink", sink.to_string())];
    gauge!("tidx_gap_count", &labels).set(count as f64);
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
        set_backfill_block(self.chain_id, "postgres", current_block);
        set_backfill_remaining(self.chain_id, "postgres", current_block.saturating_sub(target_block));

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

// ── Per-sink watermarks (in-memory, no table scans) ──────────────────────
//
// Tracks the highest block number written to each table in each sink.
// Updated atomically on every write, queried by status endpoints for
// instant per-table progress without touching the actual tables.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

/// Per-table high-water marks for a single sink.
pub struct SinkWatermarks {
    pub blocks: AtomicI64,
    pub txs: AtomicI64,
    pub logs: AtomicI64,
    pub receipts: AtomicI64,
    // Cumulative row counts (since process start)
    pub blocks_rows: AtomicU64,
    pub txs_rows: AtomicU64,
    pub logs_rows: AtomicU64,
    pub receipts_rows: AtomicU64,
}

impl SinkWatermarks {
    fn new() -> Self {
        Self {
            blocks: AtomicI64::new(-1),
            txs: AtomicI64::new(-1),
            logs: AtomicI64::new(-1),
            receipts: AtomicI64::new(-1),
            blocks_rows: AtomicU64::new(0),
            txs_rows: AtomicU64::new(0),
            logs_rows: AtomicU64::new(0),
            receipts_rows: AtomicU64::new(0),
        }
    }

    fn get_table(&self, table: &str) -> &AtomicI64 {
        match table {
            "blocks" => &self.blocks,
            "txs" => &self.txs,
            "logs" => &self.logs,
            "receipts" => &self.receipts,
            _ => &self.blocks,
        }
    }

    fn get_row_counter(&self, table: &str) -> &AtomicU64 {
        match table {
            "blocks" => &self.blocks_rows,
            "txs" => &self.txs_rows,
            "logs" => &self.logs_rows,
            "receipts" => &self.receipts_rows,
            _ => &self.blocks_rows,
        }
    }
}

static PG_WATERMARKS: OnceLock<SinkWatermarks> = OnceLock::new();
static CH_WATERMARKS: OnceLock<SinkWatermarks> = OnceLock::new();

fn watermarks_for(sink: &str) -> &'static SinkWatermarks {
    match sink {
        "clickhouse" => CH_WATERMARKS.get_or_init(SinkWatermarks::new),
        _ => PG_WATERMARKS.get_or_init(SinkWatermarks::new),
    }
}

/// Update the high-water mark for a table in a sink.
/// Only increases (never decreases) to handle concurrent writers.
pub fn update_sink_watermark(sink: &str, table: &str, max_block: i64) {
    let wm = watermarks_for(sink);
    let atomic = wm.get_table(table);
    atomic.fetch_max(max_block, Ordering::Relaxed);
}

/// Get the high-water mark for a table in a sink, or None if no writes yet.
pub fn get_sink_watermark(sink: &str, table: &str) -> Option<i64> {
    let wm = watermarks_for(sink);
    let val = wm.get_table(table).load(Ordering::Relaxed);
    if val >= 0 { Some(val) } else { None }
}

/// Get all 4 table watermarks for a sink as (blocks, txs, logs, receipts).
pub fn get_sink_watermarks(sink: &str) -> (Option<i64>, Option<i64>, Option<i64>, Option<i64>) {
    let to_opt = |v: i64| if v >= 0 { Some(v) } else { None };
    let wm = watermarks_for(sink);
    (
        to_opt(wm.blocks.load(Ordering::Relaxed)),
        to_opt(wm.txs.load(Ordering::Relaxed)),
        to_opt(wm.logs.load(Ordering::Relaxed)),
        to_opt(wm.receipts.load(Ordering::Relaxed)),
    )
}

/// Increment the cumulative row count for a table in a sink.
pub fn increment_sink_row_count(sink: &str, table: &str, count: u64) {
    let wm = watermarks_for(sink);
    wm.get_row_counter(table).fetch_add(count, Ordering::Relaxed);
}

/// Get cumulative row counts for a sink as (blocks, txs, logs, receipts).
pub fn get_sink_row_counts(sink: &str) -> (u64, u64, u64, u64) {
    let wm = watermarks_for(sink);
    (
        wm.blocks_rows.load(Ordering::Relaxed),
        wm.txs_rows.load(Ordering::Relaxed),
        wm.logs_rows.load(Ordering::Relaxed),
        wm.receipts_rows.load(Ordering::Relaxed),
    )
}

// ── Per-sink rolling write rate tracker ───────────────────────────────────

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
