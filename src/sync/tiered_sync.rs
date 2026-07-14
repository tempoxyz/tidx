//! ClickHouse-first historical sync and PostgreSQL hot-window reconciliation.
//!
//! In a retention-enabled deployment, ClickHouse owns the full historical
//! backfill. PostgreSQL is reconciled from checkpointed ClickHouse archive
//! ranges to the configured hot window, so changing `pg_keep` can either prune
//! or restore PG partitions without replaying chain RPC or full history through
//! PostgreSQL. RPC is the fallback source when no ClickHouse sink is active.

use alloy::consensus::BlockHeader as _;
use anyhow::Result;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::task::JoinSet;
use tracing::{debug, error, info, warn};

use super::engine::sync_range_standalone_to;
use super::fetcher::RpcClient;
use super::sink::{SinkSet, WriteTarget};
use super::writer::{
    detect_all_gaps, has_gaps, load_archive_state, load_sync_state, save_archive_state,
    set_hot_boundary, update_synced_num,
};
use crate::config::RetentionConfig;
use crate::db::partitions::{week_index, week_start};
use crate::metrics;

#[derive(Clone)]
pub struct TieredSync {
    sinks: SinkSet,
    rpc: RpcClient,
    chain_id: u64,
    keep: ChronoDuration,
    reconcile_interval: Duration,
    require_clickhouse: bool,
    batch_size: u64,
    concurrency: usize,
}

impl TieredSync {
    pub fn new(
        sinks: SinkSet,
        rpc_url: &str,
        chain_id: u64,
        retention: &RetentionConfig,
        batch_size: u64,
        concurrency: usize,
    ) -> Result<Self> {
        let concurrency = concurrency.max(1);
        Ok(Self {
            sinks,
            rpc: RpcClient::with_concurrency(rpc_url, concurrency.saturating_mul(2).max(4)),
            chain_id,
            keep: retention.pg_keep_duration()?,
            reconcile_interval: retention.prune_interval_duration()?,
            require_clickhouse: retention.require_clickhouse,
            batch_size: batch_size.max(1),
            concurrency,
        })
    }

    pub async fn run(self, shutdown: broadcast::Receiver<()>) {
        let hot = self.clone();
        let hot_shutdown = shutdown.resubscribe();
        let hot_handle = tokio::spawn(async move { hot.run_hot_reconciler(hot_shutdown).await });

        let archive_handle = if self.sinks.has_clickhouse() {
            let archive = self.clone();
            let archive_shutdown = shutdown.resubscribe();
            Some(tokio::spawn(async move {
                archive.run_archive_backfill(archive_shutdown).await;
            }))
        } else {
            None
        };

        if self.require_clickhouse && archive_handle.is_none() {
            warn!(
                chain_id = self.chain_id,
                "Tiered sync requires ClickHouse; PostgreSQL will be hydrated but its cold boundary will not advance"
            );
        }

        if let Err(e) = hot_handle.await {
            error!(chain_id = self.chain_id, error = %e, "PostgreSQL hot reconciler exited");
        }
        if let Some(handle) = archive_handle
            && let Err(e) = handle.await
        {
            error!(chain_id = self.chain_id, error = %e, "ClickHouse archive backfill exited");
        }
    }

    async fn run_archive_backfill(&self, mut shutdown: broadcast::Receiver<()>) {
        info!(
            chain_id = self.chain_id,
            "ClickHouse archive backfill starting"
        );
        loop {
            tokio::select! {
                _ = shutdown.recv() => {
                    info!(chain_id = self.chain_id, "ClickHouse archive backfill shutting down");
                    return;
                }
                result = self.tick_archive() => {
                    match result {
                        Ok(true) => {}
                        Ok(false) => tokio::time::sleep(Duration::from_secs(2)).await,
                        Err(e) => {
                            error!(chain_id = self.chain_id, error = %e, "ClickHouse archive backfill tick failed");
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                }
            }
        }
    }

    async fn tick_archive(&self) -> Result<bool> {
        let head = self.rpc.latest_block_number().await?;
        let mut state = load_archive_state(self.sinks.pool(), self.chain_id).await?;
        let fresh_archive = state.backfill_num.is_none();
        let mut progressed = false;

        // Repair any interval opened while the process was offline. Realtime
        // writes may already have inserted some of these rows; CH inserts are
        // idempotent and the archive watermark advances only after the whole
        // adjacent window succeeds.
        if let Some(backfill_num) = state.backfill_num
            && state.tip_num < head
        {
            let ranges = ascending_ranges(
                state.tip_num.saturating_add(1).max(1),
                head,
                self.batch_size,
                self.concurrency,
            );
            if let Some((_, new_tip)) = range_hull(&ranges) {
                self.sync_ranges(ranges, WriteTarget::ClickHouse).await?;
                save_archive_state(self.sinks.pool(), self.chain_id, backfill_num, new_tip).await?;
                state.tip_num = new_tip;
                progressed = true;
            }
        }

        let end = state
            .backfill_num
            .map(|low| low.saturating_sub(1))
            .unwrap_or(head);
        if end >= 1 {
            let ranges = descending_ranges(end, 1, self.batch_size, self.concurrency);
            if let Some((new_low, _)) = range_hull(&ranges) {
                self.sync_ranges(ranges, WriteTarget::ClickHouse).await?;
                let archive_tip = if fresh_archive { head } else { state.tip_num };
                save_archive_state(self.sinks.pool(), self.chain_id, new_low, archive_tip).await?;
                metrics::set_backfill_block(self.chain_id, "clickhouse_archive", new_low);
                metrics::set_backfill_remaining(
                    self.chain_id,
                    "clickhouse_archive",
                    new_low.saturating_sub(1),
                );
                progressed = true;
                if new_low == 1 {
                    info!(
                        chain_id = self.chain_id,
                        archive_tip, "ClickHouse archive backfill reached genesis"
                    );
                }
            }
        }

        Ok(progressed)
    }

    async fn run_hot_reconciler(&self, mut shutdown: broadcast::Receiver<()>) {
        info!(
            chain_id = self.chain_id,
            pg_keep_secs = self.keep.num_seconds(),
            "PostgreSQL hot-window reconciler starting"
        );
        loop {
            let wait = tokio::select! {
                _ = shutdown.recv() => {
                    info!(chain_id = self.chain_id, "PostgreSQL hot-window reconciler shutting down");
                    return;
                }
                result = self.tick_hot_window() => {
                    match result {
                        Ok(wait) => wait,
                        Err(e) => {
                            error!(chain_id = self.chain_id, error = %e, "PostgreSQL hot-window reconcile tick failed");
                            Duration::from_secs(1)
                        }
                    }
                }
            };

            tokio::select! {
                _ = shutdown.recv() => return,
                () = tokio::time::sleep(wait) => {}
            }
        }
    }

    async fn tick_hot_window(&self) -> Result<Duration> {
        let head = self.rpc.latest_block_number().await?;
        if head == 0 {
            return Ok(Duration::from_secs(2));
        }

        let boundary_ts = hot_window_start(Utc::now(), self.keep);
        let floor = self.first_block_at_or_after(head, boundary_ts).await?;
        let boundary = floor.saturating_sub(1);

        if has_gaps(self.sinks.pool(), floor, head).await? {
            let gaps = detect_all_gaps(self.sinks.pool(), floor, head).await?;
            let ranges = if self.sinks.has_clickhouse() {
                let archive = load_archive_state(self.sinks.pool(), self.chain_id).await?;
                let Some(archive_low) = archive.backfill_num else {
                    debug!(
                        chain_id = self.chain_id,
                        "Waiting for ClickHouse archive before hydrating PostgreSQL"
                    );
                    return Ok(Duration::from_secs(2));
                };
                let archived_gaps =
                    intersect_gaps(&gaps, archive_low.max(1), archive.tip_num.min(head));
                newest_gap_ranges(&archived_gaps, self.batch_size, self.concurrency)
            } else {
                newest_gap_ranges(&gaps, self.batch_size, self.concurrency)
            };
            if !ranges.is_empty() {
                let blocks: u64 = ranges.iter().map(|(start, end)| end - start + 1).sum();
                if self.sinks.has_clickhouse() {
                    self.hydrate_postgres_ranges(ranges).await?;
                } else {
                    self.sync_ranges(ranges, WriteTarget::Postgres).await?;
                }
                metrics::record_blocks_indexed(self.chain_id, blocks);
                metrics::set_backfill_remaining(
                    self.chain_id,
                    "postgres_hot",
                    gaps.iter().map(|(start, end)| end - start + 1).sum(),
                );
                return Ok(Duration::ZERO);
            }
            if self.sinks.has_clickhouse() {
                debug!(
                    chain_id = self.chain_id,
                    "PostgreSQL gaps are outside the completed ClickHouse archive interval"
                );
                return Ok(Duration::from_secs(2));
            }
        }

        let state = load_sync_state(self.sinks.pool(), self.chain_id)
            .await?
            .unwrap_or_default();
        if state.pruned_below == boundary {
            update_synced_num(self.sinks.pool(), self.chain_id, head).await?;
            return Ok(self.reconcile_interval);
        }

        if self.require_clickhouse && boundary > 0 {
            let archive = load_archive_state(self.sinks.pool(), self.chain_id).await?;
            // The cold arm covers everything at/below the boundary. Do not
            // move the view or delete PG data until the CH archive has reached
            // genesis and extends through that boundary.
            if !archive.covers(1, boundary) {
                debug!(
                    chain_id = self.chain_id,
                    desired_boundary = boundary,
                    archive_low = archive.backfill_num,
                    archive_tip = archive.tip_num,
                    "Waiting for ClickHouse archive before moving PostgreSQL hot boundary"
                );
                return Ok(Duration::from_secs(2));
            }
        }

        set_hot_boundary(
            self.sinks.pool(),
            self.chain_id,
            boundary,
            (boundary > 0).then_some(boundary_ts),
        )
        .await?;
        update_synced_num(self.sinks.pool(), self.chain_id, head).await?;
        metrics::set_pruned_below(self.chain_id, boundary);
        crate::db::tiered::refresh_boundary(self.sinks.pool(), self.chain_id).await?;

        info!(
            chain_id = self.chain_id,
            old_boundary = state.pruned_below,
            new_boundary = boundary,
            boundary_ts = %boundary_ts,
            "PostgreSQL hot boundary reconciled"
        );
        Ok(self.reconcile_interval)
    }

    async fn first_block_at_or_after(&self, head: u64, target: DateTime<Utc>) -> Result<u64> {
        let first = self.rpc.get_block(1, false).await?;
        if super::decoder::timestamp_from_secs(first.header.timestamp()) >= target {
            return Ok(1);
        }

        let last = self.rpc.get_block(head, false).await?;
        if super::decoder::timestamp_from_secs(last.header.timestamp()) < target {
            return Ok(head.saturating_add(1));
        }

        let mut low = 1u64;
        let mut high = head;
        while low < high {
            let mid = low + (high - low) / 2;
            let block = self.rpc.get_block(mid, false).await?;
            let ts = super::decoder::timestamp_from_secs(block.header.timestamp());
            if ts < target {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        Ok(low)
    }

    async fn hydrate_postgres_ranges(&self, ranges: Vec<(u64, u64)>) -> Result<()> {
        let mut tasks = JoinSet::new();
        for (start, end) in ranges {
            let sinks = self.sinks.clone();
            tasks.spawn(async move {
                sinks
                    .hydrate_postgres_from_clickhouse(start, end)
                    .await
                    .map_err(|e| anyhow::anyhow!("hydrate PostgreSQL range {start}..={end}: {e}"))
            });
        }
        while let Some(result) = tasks.join_next().await {
            result??;
        }
        Ok(())
    }

    async fn sync_ranges(&self, ranges: Vec<(u64, u64)>, target: WriteTarget) -> Result<()> {
        let mut tasks = JoinSet::new();
        for (start, end) in ranges {
            let sinks = self.sinks.clone();
            let rpc = self.rpc.clone();
            tasks.spawn(async move {
                sync_range_standalone_to(&sinks, &rpc, start, end, target)
                    .await
                    .map_err(|e| anyhow::anyhow!("sync range {start}..={end}: {e}"))
            });
        }
        while let Some(result) = tasks.join_next().await {
            result??;
        }
        Ok(())
    }
}

fn hot_window_start(now: DateTime<Utc>, keep: ChronoDuration) -> DateTime<Utc> {
    week_start(week_index(now - keep))
}

fn descending_ranges(mut end: u64, floor: u64, batch_size: u64, limit: usize) -> Vec<(u64, u64)> {
    let mut ranges = Vec::new();
    while end >= floor && ranges.len() < limit {
        let start = end.saturating_sub(batch_size - 1).max(floor);
        ranges.push((start, end));
        if start == floor {
            break;
        }
        end = start - 1;
    }
    ranges
}

fn ascending_ranges(
    mut start: u64,
    ceiling: u64,
    batch_size: u64,
    limit: usize,
) -> Vec<(u64, u64)> {
    let mut ranges = Vec::new();
    while start <= ceiling && ranges.len() < limit {
        let end = start.saturating_add(batch_size - 1).min(ceiling);
        ranges.push((start, end));
        if end == ceiling {
            break;
        }
        start = end + 1;
    }
    ranges
}

fn newest_gap_ranges(gaps: &[(u64, u64)], batch_size: u64, limit: usize) -> Vec<(u64, u64)> {
    let mut ranges = Vec::new();
    for &(start, end) in gaps {
        for range in descending_ranges(end, start, batch_size, limit - ranges.len()) {
            ranges.push(range);
            if ranges.len() == limit {
                return ranges;
            }
        }
    }
    ranges
}

fn intersect_gaps(gaps: &[(u64, u64)], floor: u64, ceiling: u64) -> Vec<(u64, u64)> {
    if floor > ceiling {
        return Vec::new();
    }
    gaps.iter()
        .filter_map(|&(start, end)| {
            let start = start.max(floor);
            let end = end.min(ceiling);
            (start <= end).then_some((start, end))
        })
        .collect()
}

fn range_hull(ranges: &[(u64, u64)]) -> Option<(u64, u64)> {
    Some((
        ranges.iter().map(|(start, _)| *start).min()?,
        ranges.iter().map(|(_, end)| *end).max()?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn hot_window_aligns_to_week_boundary() {
        let now = Utc.with_ymd_and_hms(2026, 7, 14, 12, 0, 0).unwrap();
        let start = hot_window_start(now, ChronoDuration::days(30));
        assert_eq!(start.timestamp() % (7 * 24 * 60 * 60), 0);
        assert!(start <= now - ChronoDuration::days(30));
        assert!(start > now - ChronoDuration::days(37));
    }

    #[test]
    fn range_windows_are_contiguous() {
        assert_eq!(
            descending_ranges(100, 1, 10, 3),
            vec![(91, 100), (81, 90), (71, 80)]
        );
        assert_eq!(
            ascending_ranges(1, 100, 10, 3),
            vec![(1, 10), (11, 20), (21, 30)]
        );
    }

    #[test]
    fn newest_gaps_are_limited_by_concurrency() {
        let gaps = vec![(91, 100), (1, 50)];
        assert_eq!(
            newest_gap_ranges(&gaps, 5, 3),
            vec![(96, 100), (91, 95), (46, 50)]
        );
    }

    #[test]
    fn postgres_hydration_is_clamped_to_archived_ranges() {
        let gaps = vec![(91, 110), (1, 50)];
        assert_eq!(intersect_gaps(&gaps, 40, 100), vec![(91, 100), (40, 50)]);
        assert!(intersect_gaps(&gaps, 111, 120).is_empty());
    }
}
