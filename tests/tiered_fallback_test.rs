//! Tiered-query degradation tests: a failing hot (PostgreSQL) arm must not
//! take down split queries the ClickHouse archive can serve.
//!
//! Run with: cargo test --test tiered_fallback_test
//! Requires: docker compose -f docker/local/docker-compose.yml up -d postgres clickhouse

mod common;

use chrono::{Duration, Utc};
use common::clickhouse::TestClickHouse;
use common::testdb::TestDb;
use serial_test::serial;

use tidx::clickhouse::ClickHouseEngine;
use tidx::config::ClickHouseConfig;
use tidx::service::{QueryOptions, execute_query_tiered};
use tidx::sync::ch_sink::ClickHouseSink;
use tidx::sync::writer::set_hot_boundary;
use tidx::types::BlockRow;

const CHAIN_ID: u64 = 999;
const CH_DB: &str = "tidx_repro_tiered_hot_arm";

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

/// A hot-arm failure on a descending split query must degrade to the
/// ClickHouse archive (it holds full history), not surface the error.
#[tokio::test]
#[serial(db)]
async fn test_desc_split_falls_back_to_clickhouse_when_hot_arm_fails() {
    let ch = TestClickHouse::new(CH_DB).await.expect("CH client");
    if ch.wait_for_ready().await.is_err() {
        println!("ClickHouse not available, skipping test");
        return;
    }
    ch.reset_database().await.expect("Failed to reset CH db");
    let sink = ClickHouseSink::new(&ch.url, CH_DB, None, None).expect("Failed to create CH sink");
    sink.ensure_schema_only().await.expect("CH schema");

    let db = TestDb::empty().await;
    db.truncate_all().await;

    // Full history (blocks 1..=20) is dual-written to the ClickHouse archive.
    let base_ts = Utc::now() - Duration::seconds(30);
    let blocks: Vec<_> = (1..=20)
        .map(|n| make_block(n, base_ts + Duration::seconds(n)))
        .collect();
    sink.write_blocks(&blocks).await.expect("CH blocks");
    set_hot_boundary(&db.pool, CHAIN_ID, 10, Some(base_ts + Duration::seconds(10)))
        .await
        .expect("set boundary");

    // Break only the hot PostgreSQL arm; ClickHouse can still serve the query.
    let conn = db.pool.get().await.expect("conn");
    conn.execute("DROP TABLE blocks CASCADE", &[])
        .await
        .expect("drop hot table");
    drop(conn);

    let engine = ClickHouseEngine::new(
        &ClickHouseConfig {
            enabled: true,
            url: ch.url.clone(),
            database: Some(CH_DB.to_string()),
            ..Default::default()
        },
        CHAIN_ID,
    )
    .expect("CH engine");

    // Split-eligible descending head page (see plan_tiered_split): the hot
    // arm runs first and its error currently propagates without fallback.
    let result = execute_query_tiered(
        &db.pool,
        Some(&engine),
        CHAIN_ID,
        "SELECT num FROM blocks ORDER BY num DESC LIMIT 5",
        &[],
        &QueryOptions {
            timeout_ms: 10_000,
            limit: 100,
        },
    )
    .await
    .expect("hot-arm failure must degrade to the ClickHouse arm, not error");

    let nums: Vec<i64> = result
        .rows
        .iter()
        .map(|r| r[0].as_i64().expect("num is an integer"))
        .collect();
    assert_eq!(
        nums,
        vec![20, 19, 18, 17, 16],
        "head page must come from the full ClickHouse history"
    );
}
