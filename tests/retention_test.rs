//! Tiered-storage retention tests
//!
//! Covers the prune-floor sync semantics (pruned history is not a gap) and
//! the Pruner itself: dropping PG partitions outside the reconciled hot
//! boundary, with the ClickHouse archive intact.
//!
//! Run with: cargo test --test retention_test
//! Requires: docker compose -f docker/local/docker-compose.yml up -d postgres clickhouse

mod common;

use chrono::{Duration, Utc};
use common::clickhouse::TestClickHouse;
use common::testdb::TestDb;
use serial_test::serial;

use tidx::clickhouse::ClickHouseEngine;
use tidx::config::{ClickHouseConfig, RetentionConfig};
use tidx::db::partitions::{ensure_partitions_covering, list_partitions};
use tidx::service::{QueryOptions, execute_query_tiered};
use tidx::sync::ch_sink::ClickHouseSink;
use tidx::sync::pruner::Pruner;
use tidx::sync::sink::SinkSet;
use tidx::sync::writer::{
    self, detect_all_gaps, load_archive_state, load_sync_state, save_archive_state,
    save_sync_state, set_hot_boundary,
};
use tidx::types::{BlockRow, LogRow, SyncState, TxRow};

const CHAIN_ID: u64 = 999;
const CH_DB: &str = "tidx_test_retention";
const OFFSET_CH_DB: &str = "tidx_test_offset";

// ── Test data helpers ──────────────────────────────────────────────────────

fn make_block(num: i64, ts: chrono::DateTime<Utc>) -> BlockRow {
    BlockRow {
        num,
        hash: vec![num as u8; 32],
        parent_hash: vec![(num.wrapping_sub(1)) as u8; 32],
        timestamp: ts,
        timestamp_ms: ts.timestamp_millis(),
        gas_limit: 30_000_000,
        gas_used: 21_000,
        miner: vec![0xaa; 20],
        extra_data: None,
        consensus_proposer: None,
    }
}

fn make_tx(block_num: i64, ts: chrono::DateTime<Utc>) -> TxRow {
    TxRow {
        block_num,
        block_timestamp: ts,
        idx: 0,
        hash: vec![block_num as u8; 32],
        tx_type: 2,
        from: vec![0x11; 20],
        to: Some(vec![0x22; 20]),
        value: "0".to_string(),
        input: vec![],
        gas_limit: 21_000,
        max_fee_per_gas: "0".to_string(),
        max_priority_fee_per_gas: "0".to_string(),
        gas_used: Some(21_000),
        nonce_key: vec![0x11; 20],
        nonce: block_num,
        fee_token: None,
        fee_payer: None,
        calls: None,
        call_count: 1,
        valid_before: None,
        valid_after: None,
        signature_type: None,
    }
}

fn make_log(block_num: i64, ts: chrono::DateTime<Utc>) -> LogRow {
    LogRow {
        block_num,
        block_timestamp: ts,
        log_idx: 0,
        tx_idx: 0,
        tx_hash: vec![block_num as u8; 32],
        address: vec![0xda; 20],
        selector: None,
        topic0: Some(vec![0xdd; 32]),
        topic1: None,
        topic2: None,
        topic3: None,
        data: vec![],
        is_virtual_forward: false,
    }
}

/// Insert blocks (+ one tx and one log each) covering `nums`, all stamped
/// starting at `base_ts` (one second apart). Creates partitions first.
async fn seed_range(
    pool: &tidx::db::Pool,
    nums: std::ops::RangeInclusive<i64>,
    base_ts: chrono::DateTime<Utc>,
) {
    let end_ts = base_ts + Duration::seconds(nums.end() - nums.start());
    ensure_partitions_covering(pool, base_ts, end_ts)
        .await
        .expect("Failed to ensure partitions");

    let rows: Vec<_> = nums
        .clone()
        .map(|n| {
            let ts = base_ts + Duration::seconds(n - nums.start());
            (make_block(n, ts), make_tx(n, ts), make_log(n, ts))
        })
        .collect();
    let blocks: Vec<_> = rows.iter().map(|(b, _, _)| b.clone()).collect();
    let txs: Vec<_> = rows.iter().map(|(_, t, _)| t.clone()).collect();
    let logs: Vec<_> = rows.iter().map(|(_, _, l)| l.clone()).collect();

    writer::write_blocks(pool, &blocks).await.expect("blocks");
    writer::write_txs(pool, &txs).await.expect("txs");
    writer::write_logs(pool, &logs).await.expect("logs");
}

async fn set_state(pool: &tidx::db::Pool, synced_num: u64, head_num: u64) {
    save_sync_state(
        pool,
        &SyncState {
            chain_id: CHAIN_ID,
            head_num,
            synced_num,
            tip_num: synced_num,
            backfill_num: Some(0),
            ..Default::default()
        },
    )
    .await
    .expect("Failed to save sync state");
}

fn retention(require_clickhouse: bool) -> RetentionConfig {
    RetentionConfig {
        pg_keep: "30d".to_string(),
        prune_interval: "1h".to_string(),
        require_clickhouse,
    }
}

/// Set up ClickHouse sink for archive tests. Returns None if CH unavailable.
async fn setup_clickhouse(database: &str) -> Option<(ClickHouseSink, TestClickHouse)> {
    let ch = TestClickHouse::new(database)
        .await
        .expect("Failed to create CH client");
    if ch.wait_for_ready().await.is_err() {
        println!("ClickHouse not available, skipping test");
        return None;
    }
    ch.reset_database().await.expect("Failed to reset CH db");
    let sink =
        ClickHouseSink::new(&ch.url, database, None, None).expect("Failed to create CH sink");
    sink.ensure_schema().await.expect("Failed to ensure schema");
    Some((sink, ch))
}

async fn pg_block_range(pool: &tidx::db::Pool) -> (Option<i64>, Option<i64>) {
    let conn = pool.get().await.unwrap();
    let row = conn
        .query_one("SELECT MIN(num), MAX(num) FROM blocks", &[])
        .await
        .unwrap();
    (row.get(0), row.get(1))
}

#[tokio::test]
#[serial(db)]
async fn test_tiered_offset_applies_after_stitching() {
    let Some((ch_sink, ch)) = setup_clickhouse(OFFSET_CH_DB).await else {
        return;
    };
    let db = TestDb::empty().await;
    db.truncate_all().await;

    let base_ts = Utc::now();
    seed_range(&db.pool, 1..=40, base_ts).await;
    set_state(&db.pool, 40, 40).await;
    set_hot_boundary(
        &db.pool,
        CHAIN_ID,
        20,
        Some(base_ts + Duration::seconds(20)),
    )
    .await
    .unwrap();

    let blocks: Vec<_> = (1..=40)
        .map(|num| make_block(num, base_ts + Duration::seconds(num - 1)))
        .collect();
    ch_sink.write_blocks(&blocks).await.unwrap();

    let engine = ClickHouseEngine::new(
        &ClickHouseConfig {
            enabled: true,
            url: ch.url.clone(),
            database: Some(OFFSET_CH_DB.to_string()),
            ..Default::default()
        },
        CHAIN_ID,
    )
    .unwrap();
    let result = execute_query_tiered(
        &db.pool,
        Some(&engine),
        CHAIN_ID,
        "SELECT num FROM blocks ORDER BY num DESC LIMIT 6 OFFSET 18",
        &[],
        &QueryOptions::default(),
    )
    .await
    .unwrap();
    let maximum_window = execute_query_tiered(
        &db.pool,
        Some(&engine),
        CHAIN_ID,
        "SELECT num FROM blocks ORDER BY num ASC LIMIT 10000 OFFSET 1",
        &[],
        &QueryOptions::default(),
    )
    .await
    .unwrap();

    assert_eq!(
        result.rows,
        [22, 21, 20, 19, 18, 17]
            .map(|num| vec![serde_json::json!(num)])
            .to_vec()
    );
    assert_eq!(
        maximum_window.rows,
        (2..=40)
            .map(|num| vec![serde_json::json!(num)])
            .collect::<Vec<_>>()
    );
}

// ── Prune-floor sync semantics ─────────────────────────────────────────────

#[tokio::test]
#[serial(db)]
async fn test_prune_floor_hides_pruned_history_from_gap_detection() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    // Only blocks 50..=100 exist (1..=49 intentionally pruned).
    seed_range(&db.pool, 50..=100, Utc::now()).await;

    // Without a prune floor, 1..=49 shows as a gap.
    let gaps = detect_all_gaps(&db.pool, 1, 100).await.unwrap();
    assert_eq!(gaps, vec![(1, 49)]);

    // With the prune floor at 50, history below it is not a gap.
    let gaps = detect_all_gaps(&db.pool, 50, 100).await.unwrap();
    assert!(gaps.is_empty(), "pruned history must not appear as gaps");
}

#[test]
fn test_backfill_complete_at_prune_floor() {
    let state = SyncState {
        backfill_num: Some(49),
        pruned_below: 49,
        tip_num: 100,
        ..Default::default()
    };
    assert!(state.backfill_complete());
    assert_eq!(state.prune_floor(), 50);
    assert_eq!(state.backfill_remaining(), 0);

    let state = SyncState {
        backfill_num: Some(60),
        pruned_below: 49,
        tip_num: 100,
        ..Default::default()
    };
    assert!(!state.backfill_complete());
    assert_eq!(state.backfill_remaining(), 11);
}

// ── Pruner integration ─────────────────────────────────────────────────────

/// Full tiered-storage flow: old blocks archived to CH, PG partitions
/// dropped, watermark set, no phantom gaps, recent data intact.
#[tokio::test]
#[serial(db)]
async fn test_pruner_drops_old_partitions_after_ch_archive() {
    let Some((ch_sink, ch)) = setup_clickhouse(CH_DB).await else {
        return;
    };
    let db = TestDb::empty().await;
    db.truncate_all().await;

    let old_ts = Utc::now() - Duration::weeks(10);
    seed_range(&db.pool, 1..=20, old_ts).await; // outside 30d window
    seed_range(&db.pool, 21..=40, Utc::now()).await; // inside window
    set_state(&db.pool, 40, 2000).await; // head far enough for reorg guard

    let old_partitions_before = list_partitions(&db.pool, "blocks").await.unwrap();
    assert!(
        old_partitions_before
            .iter()
            .any(|(_, start)| *start < Utc::now() - Duration::days(30)),
        "test setup must create an out-of-window partition"
    );

    let sinks = SinkSet::new(db.pool.clone()).with_clickhouse(ch_sink);
    sinks
        .backfill_clickhouse(CHAIN_ID)
        .await
        .expect("archive PG data before pruning");
    set_hot_boundary(
        &db.pool,
        CHAIN_ID,
        20,
        Some(Utc::now() - Duration::weeks(9)),
    )
    .await
    .unwrap();
    let pruner = Pruner::new(sinks, CHAIN_ID, &retention(true)).unwrap();
    let dropped = pruner.tick().await.expect("prune tick failed");
    assert!(dropped > 0, "should drop out-of-window partitions");

    // Old blocks gone from PG; recent window intact.
    assert_eq!(pg_block_range(&db.pool).await, (Some(21), Some(40)));
    let conn = db.pool.get().await.unwrap();
    let tx_min: Option<i64> = conn
        .query_one("SELECT MIN(block_num) FROM txs", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(tx_min, Some(21), "old txs pruned with their partition");

    // No partition older than the retention window remains.
    let remaining = list_partitions(&db.pool, "blocks").await.unwrap();
    assert!(
        remaining
            .iter()
            .all(|(_, start)| *start + Duration::weeks(1) > Utc::now() - Duration::days(30)),
        "all out-of-window partitions should be gone, got: {remaining:?}"
    );

    // Watermark advanced; pruned history is not a gap.
    let state = load_sync_state(&db.pool, CHAIN_ID).await.unwrap().unwrap();
    assert_eq!(state.pruned_below, 20);
    let gaps = detect_all_gaps(&db.pool, state.prune_floor(), 40)
        .await
        .unwrap();
    assert!(
        gaps.is_empty(),
        "pruned blocks must not reappear as gaps: {gaps:?}"
    );

    // The full archive (pruned + hot) lives in ClickHouse.
    let ch_blocks = ch.table_count("blocks").await.unwrap();
    assert_eq!(ch_blocks, 40, "CH archive must hold all blocks");

    // Second tick is a no-op.
    assert_eq!(0, {
        let sinks = SinkSet::new(db.pool.clone())
            .with_clickhouse(ClickHouseSink::new(&ch.url, CH_DB, None, None).unwrap());
        Pruner::new(sinks, CHAIN_ID, &retention(true))
            .unwrap()
            .tick()
            .await
            .unwrap()
    });
}

/// The pruner consumes, but never advances, the reconciled hot boundary.
#[tokio::test]
#[serial(db)]
async fn test_pruner_does_not_advance_hot_boundary() {
    let Some((ch_sink, _ch)) = setup_clickhouse(CH_DB).await else {
        return;
    };
    let db = TestDb::empty().await;
    db.truncate_all().await;

    let old_ts = Utc::now() - Duration::weeks(10);
    seed_range(&db.pool, 1..=20, old_ts).await;
    set_state(&db.pool, 10, 2000).await;

    let sinks = SinkSet::new(db.pool.clone()).with_clickhouse(ch_sink);
    Pruner::new(sinks, CHAIN_ID, &retention(true))
        .unwrap()
        .tick()
        .await
        .unwrap();

    // Leftover EMPTY partitions from other suites may be swept, but the
    // not-yet-durable data and the watermark must be untouched.
    assert_eq!(pg_block_range(&db.pool).await, (Some(1), Some(20)));
    let state = load_sync_state(&db.pool, CHAIN_ID).await.unwrap().unwrap();
    assert_eq!(state.pruned_below, 0, "watermark must not move");
}

/// A partition that extends above the reconciled boundary is retained.
#[tokio::test]
#[serial(db)]
async fn test_pruner_keeps_partition_above_hot_boundary() {
    let Some((ch_sink, _ch)) = setup_clickhouse(CH_DB).await else {
        return;
    };
    let db = TestDb::empty().await;
    db.truncate_all().await;

    let old_ts = Utc::now() - Duration::weeks(10);
    seed_range(&db.pool, 1..=20, old_ts).await;
    set_state(&db.pool, 20, 2000).await;
    set_hot_boundary(&db.pool, CHAIN_ID, 10, Some(old_ts))
        .await
        .unwrap();

    let sinks = SinkSet::new(db.pool.clone()).with_clickhouse(ch_sink);
    Pruner::new(sinks, CHAIN_ID, &retention(true))
        .unwrap()
        .tick()
        .await
        .unwrap();

    // The partition contains blocks above the boundary, so it stays attached.
    assert_eq!(pg_block_range(&db.pool).await, (Some(1), Some(20)));
    let state = load_sync_state(&db.pool, CHAIN_ID).await.unwrap().unwrap();
    assert_eq!(state.pruned_below, 10, "pruner must not move boundary");
}

/// require_clickhouse=true with no CH sink configured: refuse to prune.
#[tokio::test]
#[serial(db)]
async fn test_pruner_requires_clickhouse_sink() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    let old_ts = Utc::now() - Duration::weeks(10);
    seed_range(&db.pool, 1..=20, old_ts).await;
    set_state(&db.pool, 20, 2000).await;
    set_hot_boundary(&db.pool, CHAIN_ID, 20, Some(old_ts))
        .await
        .unwrap();

    let sinks = SinkSet::new(db.pool.clone()); // no CH
    let dropped = Pruner::new(sinks, CHAIN_ID, &retention(true))
        .unwrap()
        .tick()
        .await
        .unwrap();

    assert_eq!(dropped, 0, "must refuse to prune without a CH archive");
    assert_eq!(pg_block_range(&db.pool).await, (Some(1), Some(20)));
}

/// require_clickhouse=false: rolling window prunes on sync state alone.
#[tokio::test]
#[serial(db)]
async fn test_pruner_rolling_window_without_clickhouse() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    let old_ts = Utc::now() - Duration::weeks(10);
    seed_range(&db.pool, 1..=20, old_ts).await;
    seed_range(&db.pool, 21..=40, Utc::now()).await;
    set_state(&db.pool, 40, 2000).await;
    set_hot_boundary(&db.pool, CHAIN_ID, 20, Some(old_ts))
        .await
        .unwrap();

    let sinks = SinkSet::new(db.pool.clone()); // no CH
    let dropped = Pruner::new(sinks, CHAIN_ID, &retention(false))
        .unwrap()
        .tick()
        .await
        .unwrap();

    assert!(dropped > 0, "explicit opt-out must prune without CH");
    assert_eq!(pg_block_range(&db.pool).await, (Some(21), Some(40)));
    let state = load_sync_state(&db.pool, CHAIN_ID).await.unwrap().unwrap();
    assert_eq!(state.pruned_below, 20);
}

#[tokio::test]
#[serial(db)]
async fn test_hot_boundary_can_move_backwards_after_restore() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    set_hot_boundary(&db.pool, CHAIN_ID, 100, Some(Utc::now()))
        .await
        .unwrap();
    set_hot_boundary(
        &db.pool,
        CHAIN_ID,
        50,
        Some(Utc::now() - Duration::weeks(1)),
    )
    .await
    .unwrap();

    let state = load_sync_state(&db.pool, CHAIN_ID).await.unwrap().unwrap();
    assert_eq!(state.pruned_below, 50);
    assert_eq!(state.backfill_num, Some(50));
}

#[tokio::test]
#[serial(db)]
async fn test_archive_state_tracks_a_contiguous_interval() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    save_archive_state(&db.pool, CHAIN_ID, 91, 100)
        .await
        .unwrap();
    save_archive_state(&db.pool, CHAIN_ID, 91, 120)
        .await
        .unwrap();
    save_archive_state(&db.pool, CHAIN_ID, 50, 120)
        .await
        .unwrap();

    let state = load_archive_state(&db.pool, CHAIN_ID).await.unwrap();
    assert_eq!(state.backfill_num, Some(50));
    assert_eq!(state.tip_num, 120);
    assert!(state.covers(50, 120));
    assert!(!state.covers(1, 120));
}
