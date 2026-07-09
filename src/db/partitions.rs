//! Weekly range partitioning for the core PG tables.
//!
//! Fresh installs create `blocks`/`txs`/`logs`/`receipts` as partitioned
//! tables (see `db/*.sql`). Legacy deployments keep plain tables; every
//! entry point here no-ops when the tables are not partitioned.
//!
//! Partitions live on a fixed epoch-aligned weekly grid so all writers
//! agree on boundaries without coordination.

use anyhow::Result;
use chrono::{DateTime, TimeZone, Utc};
use std::sync::Arc;
use tokio::sync::{Mutex, OnceCell};

use super::Pool;

/// Tables partitioned by their block-timestamp column on fresh installs.
const PARTITIONED_TABLES: &[&str] = &["blocks", "txs", "logs", "receipts"];

const WEEK_SECS: i64 = 7 * 24 * 60 * 60;

/// Week index on the epoch-aligned grid for a timestamp.
pub fn week_index(ts: DateTime<Utc>) -> i64 {
    ts.timestamp().div_euclid(WEEK_SECS)
}

/// Start of an epoch-grid week.
pub fn week_start(week: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(week * WEEK_SECS, 0).unwrap()
}

/// Partition name for a table + week, e.g. `blocks_p20260702`.
pub fn partition_name(table: &str, week: i64) -> String {
    format!("{table}_p{}", week_start(week).format("%Y%m%d"))
}

/// Whether the core tables are natively partitioned (fresh installs).
pub async fn is_partitioned(pool: &Pool) -> Result<bool> {
    let conn = pool.get().await?;
    let row = conn
        .query_opt(
            "SELECT relkind = 'p' FROM pg_class
             WHERE relname = 'blocks' AND relnamespace = 'public'::regnamespace",
            &[],
        )
        .await?;
    Ok(row.map(|r| r.get::<_, bool>(0)).unwrap_or(false))
}

/// Create weekly partitions covering `[min_ts, max_ts]` for all core tables.
/// Idempotent. No-op on legacy (non-partitioned) deployments.
pub async fn ensure_partitions_covering(
    pool: &Pool,
    min_ts: DateTime<Utc>,
    max_ts: DateTime<Utc>,
) -> Result<()> {
    if !is_partitioned(pool).await? {
        return Ok(());
    }
    create_weeks(pool, week_index(min_ts), week_index(max_ts)).await
}

/// List attached weekly partitions of a table as `(name, week_start)`,
/// ordered oldest first. Parses bounds from the epoch-grid naming scheme.
pub async fn list_partitions(pool: &Pool, table: &str) -> Result<Vec<(String, DateTime<Utc>)>> {
    let conn = pool.get().await?;
    let rows = conn
        .query(
            "SELECT c.relname FROM pg_inherits i
             JOIN pg_class c ON c.oid = i.inhrelid
             JOIN pg_class p ON p.oid = i.inhparent
             WHERE p.relname = $1 AND p.relnamespace = 'public'::regnamespace
             ORDER BY c.relname",
            &[&table],
        )
        .await?;

    let prefix = format!("{table}_p");
    let mut out = Vec::new();
    for row in rows {
        let name: String = row.get(0);
        let Some(date) = name.strip_prefix(&prefix) else {
            continue;
        };
        let Ok(date) = chrono::NaiveDate::parse_from_str(date, "%Y%m%d") else {
            continue;
        };
        let start = Utc.from_utc_datetime(&date.and_hms_opt(0, 0, 0).unwrap());
        out.push((name, start));
    }
    Ok(out)
}

/// Detach + drop a single partition. Detach uses CONCURRENTLY so readers
/// and writers on other partitions are never blocked.
pub async fn drop_partition(pool: &Pool, table: &str, name: &str) -> Result<()> {
    let conn = pool.get().await?;
    // DETACH CONCURRENTLY cannot run inside a transaction block.
    conn.execute(
        &format!("ALTER TABLE {table} DETACH PARTITION {name} CONCURRENTLY"),
        &[],
    )
    .await?;
    conn.execute(&format!("DROP TABLE IF EXISTS {name}"), &[])
        .await?;
    Ok(())
}

async fn create_weeks(pool: &Pool, w0: i64, w1: i64) -> Result<()> {
    if w1 < w0 {
        return Ok(());
    }
    let conn = pool.get().await?;
    // Advisory lock serializes concurrent partition DDL across processes.
    let mut sql =
        String::from("BEGIN;\nSELECT pg_advisory_xact_lock(hashtext('tidx_partitions'));\n");
    for week in w0..=w1 {
        let from = week_start(week).format("%Y-%m-%d %H:%M:%S+00");
        let to = week_start(week + 1).format("%Y-%m-%d %H:%M:%S+00");
        for table in PARTITIONED_TABLES {
            let name = partition_name(table, week);
            sql.push_str(&format!(
                "CREATE TABLE IF NOT EXISTS {name} PARTITION OF {table} FOR VALUES FROM ('{from}') TO ('{to}');\n"
            ));
        }
    }
    sql.push_str("COMMIT;");
    conn.batch_execute(&sql).await?;
    Ok(())
}

/// Per-process cache of which weekly partitions exist so the hot write
/// path skips catalog checks and DDL entirely once a week is covered.
#[derive(Clone, Default)]
pub struct PartitionCoverage {
    partitioned: Arc<OnceCell<bool>>,
    /// Hull of week indices known to have partitions.
    covered: Arc<Mutex<Option<(i64, i64)>>>,
}

impl PartitionCoverage {
    /// Ensure weekly partitions exist for every timestamp in `[min_ts, max_ts]`.
    /// No-op on legacy (non-partitioned) deployments.
    pub async fn ensure(
        &self,
        pool: &Pool,
        min_ts: DateTime<Utc>,
        max_ts: DateTime<Utc>,
    ) -> Result<()> {
        let partitioned = self
            .partitioned
            .get_or_try_init(|| is_partitioned(pool))
            .await?;
        if !*partitioned {
            return Ok(());
        }

        let (w0, w1) = (week_index(min_ts), week_index(max_ts));
        let mut covered = self.covered.lock().await;
        let (lo, hi) = match *covered {
            Some((lo, hi)) if w0 >= lo && w1 <= hi => return Ok(()),
            Some((lo, hi)) => (lo.min(w0), hi.max(w1)),
            None => (w0, w1),
        };
        create_weeks(pool, lo, hi).await?;
        *covered = Some((lo, hi));
        Ok(())
    }

    /// Forget cached coverage (used after partitions are dropped).
    pub async fn reset(&self) {
        *self.covered.lock().await = None;
    }
}
