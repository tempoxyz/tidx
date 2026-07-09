//! Tiered-storage pruner: drops Postgres partitions that lie entirely
//! outside the retention window once the ClickHouse archive durably
//! holds their data.
//!
//! A `blocks` partition (and its week-siblings in `logs`/`receipts`/`txs`)
//! is dropped only when ALL hold:
//! 1. its whole time range is older than `now - pg_keep`
//! 2. its highest block <= `synced_num` (PG contiguous through it)
//! 3. its highest block <= `ch_backfill_block` (CH consumed it from PG),
//!    when `require_clickhouse`
//! 4. its highest block <= `head - REORG_GUARD`
//!
//! `pruned_below` advances BEFORE partitions drop: a crash in between
//! leaves extra rows (harmless), never a watermark claiming data exists.

use anyhow::Result;
use chrono::{Duration, Utc};
use std::collections::HashSet;
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

use super::sink::{SinkSet, load_ch_backfill_cursor};
use super::writer::{load_sync_state, update_pruned_below};
use crate::config::RetentionConfig;
use crate::db::partitions::{self, partition_name, week_index};
use crate::metrics;

/// Never prune within this many blocks of the head (reorg safety).
const REORG_GUARD: u64 = 1_000;

/// Per-chain background pruner.
pub struct Pruner {
    sinks: SinkSet,
    chain_id: u64,
    keep: Duration,
    interval: std::time::Duration,
    require_clickhouse: bool,
}

impl Pruner {
    pub fn new(sinks: SinkSet, chain_id: u64, retention: &RetentionConfig) -> Result<Self> {
        Ok(Self {
            sinks,
            chain_id,
            keep: retention.pg_keep_duration()?,
            interval: retention.prune_interval_duration()?,
            require_clickhouse: retention.require_clickhouse,
        })
    }

    /// Run until shutdown. First tick waits one full interval so startup
    /// work (CH catch-up backfill) gets a head start.
    pub async fn run(self, mut shutdown: broadcast::Receiver<()>) {
        loop {
            tokio::select! {
                _ = shutdown.recv() => {
                    info!(chain_id = self.chain_id, "Pruner shutting down");
                    return;
                }
                () = tokio::time::sleep(self.interval) => {}
            }
            if let Err(e) = self.tick().await {
                warn!(chain_id = self.chain_id, error = %e, "Prune round failed");
            }
        }
    }

    /// One prune round. Returns the number of partitions dropped.
    pub async fn tick(&self) -> Result<usize> {
        let pool = self.sinks.pool();

        if !partitions::is_partitioned(pool).await? {
            warn!(
                chain_id = self.chain_id,
                "Retention configured but tables are not partitioned; skipping prune \
                 (re-bootstrap onto partitioned tables, see plans/tiered-storage-postgres-hot-clickhouse-archive.md)"
            );
            return Ok(0);
        }

        if self.require_clickhouse && !self.sinks.has_clickhouse() {
            warn!(
                chain_id = self.chain_id,
                "Retention requires ClickHouse but no CH sink is active; skipping prune"
            );
            return Ok(0);
        }

        // Candidate weeks: blocks partitions whose entire range is below cutoff.
        let cutoff = Utc::now() - self.keep;
        let candidates: Vec<_> = partitions::list_partitions(pool, "blocks")
            .await?
            .into_iter()
            .filter(|(_, start)| *start + Duration::weeks(1) <= cutoff)
            .collect();
        if candidates.is_empty() {
            debug!(
                chain_id = self.chain_id,
                "Prune: no partitions outside retention window"
            );
            return Ok(0);
        }

        // Advance the CH durability watermark: re-run the incremental PG→CH
        // backfill so ch_backfill_block covers everything currently in PG.
        if self.require_clickhouse {
            self.sinks.backfill_clickhouse(self.chain_id).await?;
        }

        let state = load_sync_state(pool, self.chain_id)
            .await?
            .unwrap_or_default();
        let mut allowed = state
            .synced_num
            .min(state.head_num.saturating_sub(REORG_GUARD));
        if self.require_clickhouse {
            let ch_cursor = load_ch_backfill_cursor(pool, self.chain_id).await?;
            allowed = allowed.min(ch_cursor.max(0) as u64);
        }

        // Walk candidates oldest→newest; a partition drops only if every
        // block in it is durable. Stop at the first that isn't.
        let mut weeks = Vec::new();
        let mut new_pruned_below = 0u64;
        {
            let conn = pool.get().await?;
            for (name, start) in &candidates {
                let row = conn
                    .query_one(&format!("SELECT MAX(num) FROM {name}"), &[])
                    .await?;
                match row.get::<_, Option<i64>>(0) {
                    // Empty partition: nothing to protect
                    None => weeks.push(week_index(*start)),
                    Some(max) if (max as u64) <= allowed => {
                        new_pruned_below = new_pruned_below.max(max as u64);
                        weeks.push(week_index(*start));
                    }
                    Some(max) => {
                        debug!(
                            chain_id = self.chain_id,
                            partition = %name,
                            max_block = max,
                            allowed,
                            "Prune: partition not yet durable; stopping"
                        );
                        break;
                    }
                }
            }
        }

        if weeks.is_empty() {
            return Ok(0);
        }

        // Watermark first (crash-safe order: extra data, never a lying floor).
        if new_pruned_below > state.pruned_below {
            update_pruned_below(pool, self.chain_id, new_pruned_below).await?;
            metrics::set_pruned_below(self.chain_id, new_pruned_below);
        }

        // Drop in dependency-safe order; blocks last.
        let mut dropped = 0usize;
        for table in ["logs", "receipts", "txs", "blocks"] {
            let existing: HashSet<String> = partitions::list_partitions(pool, table)
                .await?
                .into_iter()
                .map(|(name, _)| name)
                .collect();
            for &week in &weeks {
                let name = partition_name(table, week);
                if !existing.contains(&name) {
                    continue;
                }
                partitions::drop_partition(pool, table, &name).await?;
                info!(chain_id = self.chain_id, table, partition = %name, "Pruned partition");
                dropped += 1;
            }
        }

        // Dropped weeks invalidate the writer's partition-coverage cache.
        self.sinks.reset_partition_coverage().await;

        metrics::record_prune_partitions_dropped(self.chain_id, dropped as u64);
        metrics::set_last_prune(self.chain_id);
        info!(
            chain_id = self.chain_id,
            partitions = dropped,
            pruned_below = new_pruned_below,
            "Prune round complete"
        );
        Ok(dropped)
    }
}
