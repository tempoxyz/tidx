//! Tiered-storage pruner: drops Postgres partitions that lie entirely
//! outside the reconciled PostgreSQL hot window.
//!
//! A `blocks` partition (and its week-siblings in `logs`/`receipts`/`txs`)
//! is dropped only when ALL hold:
//! 1. its whole time range is older than `now - pg_keep`
//! 2. its highest block <= `pruned_below` (the reconciled tier boundary)
//!
//! The reconciler moves the boundary before shrink-time partition drops. A
//! crash in between leaves extra PostgreSQL rows hidden by the hot view arm.

use anyhow::Result;
use chrono::{Duration, Utc};
use std::collections::HashSet;
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

use super::sink::SinkSet;
use super::writer::load_sync_state;
use crate::config::RetentionConfig;
use crate::db::partitions::{self, partition_name, week_index};
use crate::metrics;

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

    /// Run until shutdown. First tick waits one full interval so the hot
    /// reconciler can establish the initial boundary.
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
                "Retention requires ClickHouse but no archive sink is active; skipping prune"
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

        let state = load_sync_state(pool, self.chain_id)
            .await?
            .unwrap_or_default();

        // Walk candidates oldest→newest; a partition drops only if every
        // block in it is durable. Stop at the first that isn't.
        let mut weeks = Vec::new();
        {
            let conn = pool.get().await?;
            for (name, start) in &candidates {
                let row = conn
                    .query_one(&format!("SELECT MAX(num) FROM {name}"), &[])
                    .await?;
                match row.get::<_, Option<i64>>(0) {
                    // Empty partition: nothing to protect
                    None => weeks.push(week_index(*start)),
                    Some(max) if (max as u64) <= state.pruned_below => {
                        weeks.push(week_index(*start));
                    }
                    Some(max) => {
                        debug!(
                            chain_id = self.chain_id,
                            partition = %name,
                            max_block = max,
                            hot_boundary = state.pruned_below,
                            "Prune: partition still belongs to the PostgreSQL hot tier"
                        );
                        break;
                    }
                }
            }
        }

        if weeks.is_empty() {
            return Ok(0);
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
                if table == "txs" {
                    let start = partitions::week_start(week);
                    let end = partitions::week_start(week + 1);
                    let conn = pool.get().await?;
                    conn.execute(
                        "DELETE FROM receipt_repair_queue \
                         WHERE block_timestamp >= $1 AND block_timestamp < $2",
                        &[&start, &end],
                    )
                    .await?;
                }
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
            hot_boundary = state.pruned_below,
            "Prune round complete"
        );
        Ok(dropped)
    }
}
