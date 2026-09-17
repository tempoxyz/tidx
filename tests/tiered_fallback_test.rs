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
use tidx::db::run_migrations;
use tidx::query::EventSignature;
use tidx::service::{QueryOptions, execute_query_tiered};
use tidx::sync::ch_sink::ClickHouseSink;
use tidx::sync::writer::{set_hot_boundary, write_logs};
use tidx::types::{BlockRow, LogRow};

const CHAIN_ID: u64 = 999;
const CH_DB: &str = "tidx_repro_tiered_hot_arm";
const STRING_EVENT: &str = "TokenCreated(string name)";

fn make_log(block_num: i64, ts: chrono::DateTime<Utc>, selector: Option<Vec<u8>>) -> LogRow {
    LogRow {
        block_num,
        block_timestamp: ts,
        tx_hash: vec![block_num as u8; 32],
        address: vec![0xbb; 20],
        selector,
        data: vec![0x01],
        ..Default::default()
    }
}

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

/// Seeds ClickHouse with blocks 1..=20 and sets the hot boundary at block 10.
/// Returns None when ClickHouse is unavailable.
async fn setup_tiered_store() -> Option<(TestClickHouse, TestDb, ClickHouseEngine)> {
    let ch = TestClickHouse::new(CH_DB).expect("CH client");
    if ch.wait_for_ready().await.is_err() {
        println!("ClickHouse not available, skipping test");
        return None;
    }
    ch.reset_database().await.expect("Failed to reset CH db");
    let sink = ClickHouseSink::new(&ch.url, CH_DB, None, None).expect("Failed to create CH sink");
    sink.ensure_schema_only().await.expect("CH schema");

    let db = TestDb::empty().await;
    // Each test drops the hot table; restore the schema before reuse.
    run_migrations(&db.pool).await.expect("migrations");
    db.truncate_all().await;

    // Full history (blocks 1..=20) is dual-written to the ClickHouse archive.
    let base_ts = Utc::now() - Duration::seconds(30);
    let blocks: Vec<_> = (1..=20)
        .map(|n| make_block(n, base_ts + Duration::seconds(n)))
        .collect();
    sink.write_blocks(&blocks).await.expect("CH blocks");
    set_hot_boundary(
        &db.pool,
        CHAIN_ID,
        10,
        Some(base_ts + Duration::seconds(10)),
    )
    .await
    .expect("set boundary");

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

    Some((ch, db, engine))
}

/// Sets up tiered storage, then removes the hot PostgreSQL blocks table.
async fn setup_broken_hot_arm() -> Option<(TestClickHouse, TestDb, ClickHouseEngine)> {
    let (ch, db, engine) = setup_tiered_store().await?;

    // Break only the hot PostgreSQL arm; ClickHouse can still serve the query.
    let conn = db.pool.get().await.expect("conn");
    conn.execute("DROP TABLE blocks CASCADE", &[])
        .await
        .expect("drop hot table");
    drop(conn);

    Some((ch, db, engine))
}

/// A hot-arm failure on a descending split query must degrade to the
/// ClickHouse archive (it holds full history), not surface the error.
#[tokio::test]
#[serial(db)]
async fn test_desc_split_falls_back_to_clickhouse_when_hot_arm_fails() {
    let Some((_ch, db, engine)) = setup_broken_hot_arm().await else {
        return;
    };

    // Split-eligible descending head page (see plan_tiered_split): the hot
    // arm runs first and its failure degrades to the ClickHouse arm.
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
    assert_eq!(result.engine.as_deref(), Some("tiered"));
}

/// The ascending split runs both arms concurrently; a hot-arm failure must
/// likewise degrade to the ClickHouse archive.
#[tokio::test]
#[serial(db)]
async fn test_asc_split_falls_back_to_clickhouse_when_hot_arm_fails() {
    let Some((_ch, db, engine)) = setup_broken_hot_arm().await else {
        return;
    };

    let result = execute_query_tiered(
        &db.pool,
        Some(&engine),
        CHAIN_ID,
        "SELECT num FROM blocks ORDER BY num ASC LIMIT 5",
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
        vec![1, 2, 3, 4, 5],
        "ascending page must come from the full ClickHouse history"
    );
    assert_eq!(result.engine.as_deref(), Some("tiered"));
}

fn malformed_abi_string_data() -> Vec<u8> {
    let mut data = vec![0; 96];
    data[31] = 32; // offset to the dynamic string payload
    data[63] = 1; // one-byte string
    data[64] = 0xff; // invalid UTF-8
    data
}

/// PostgreSQL decoding errors carry the user-visible query semantics. They
/// must surface instead of degrading to ClickHouse, which replaces bad UTF-8.
#[tokio::test]
#[serial(db)]
async fn test_split_surfaces_hot_postgres_abi_decode_error() {
    let Some((ch, db, engine)) = setup_tiered_store().await else {
        return;
    };

    let signature = EventSignature::parse(STRING_EVENT).expect("event signature");
    let mut log = make_log(20, Utc::now(), Some(signature.topic0.to_vec()));
    log.data = malformed_abi_string_data();

    let sink = ClickHouseSink::new(&ch.url, CH_DB, None, None).expect("CH sink");
    sink.write_logs(std::slice::from_ref(&log))
        .await
        .expect("CH log");
    write_logs(&db.pool, std::slice::from_ref(&log))
        .await
        .expect("PG log");

    let sql = "SELECT block_num, name FROM TokenCreated ORDER BY block_num DESC LIMIT 1";
    let cold = engine
        .query_user(sql, &[STRING_EVENT], 10_000, 100)
        .await
        .expect("ClickHouse accepts malformed UTF-8 bytes");
    assert_eq!(cold.row_count, 1);

    let err = execute_query_tiered(
        &db.pool,
        Some(&engine),
        CHAIN_ID,
        sql,
        &[STRING_EVENT],
        &QueryOptions {
            timeout_ms: 10_000,
            limit: 100,
        },
    )
    .await
    .expect_err("PostgreSQL ABI decoding error must surface");

    let message = err.to_string();
    assert!(
        message.contains("invalid byte sequence") && message.contains("22021"),
        "expected PostgreSQL UTF-8 decoding error, got: {message:?}"
    );
}

/// A degraded logs query must keep PostgreSQL's NULL-selector representation:
/// ClickHouse stores missing selectors as '', which the degrade path rewrites
/// back to NULL.
#[tokio::test]
#[serial(db)]
async fn test_degraded_split_preserves_null_selector() {
    let Some((ch, db, engine)) = setup_broken_hot_arm().await else {
        return;
    };

    // Full log history in the ClickHouse archive; block 20's log has no selector.
    let sink = ClickHouseSink::new(&ch.url, CH_DB, None, None).expect("CH sink");
    let base_ts = Utc::now() - Duration::seconds(30);
    let logs: Vec<_> = (1..=20)
        .map(|n| {
            make_log(
                n,
                base_ts + Duration::seconds(n),
                (n != 20).then(|| vec![0xdd; 4]),
            )
        })
        .collect();
    sink.write_logs(&logs).await.expect("CH logs");

    // Break the hot PostgreSQL arm for the logs shape too.
    let conn = db.pool.get().await.expect("conn");
    conn.execute("DROP TABLE logs CASCADE", &[])
        .await
        .expect("drop hot logs table");
    drop(conn);

    let result = execute_query_tiered(
        &db.pool,
        Some(&engine),
        CHAIN_ID,
        "SELECT block_num, selector FROM logs ORDER BY block_num DESC LIMIT 2",
        &[],
        &QueryOptions {
            timeout_ms: 10_000,
            limit: 100,
        },
    )
    .await
    .expect("hot-arm failure must degrade to the ClickHouse arm, not error");

    let rows: Vec<(i64, &serde_json::Value)> = result
        .rows
        .iter()
        .map(|r| (r[0].as_i64().expect("block_num is an integer"), &r[1]))
        .collect();
    assert_eq!(rows[0].0, 20);
    assert!(
        rows[0].1.is_null(),
        "missing selector must degrade as NULL, got {:?}",
        rows[0].1
    );
    assert_eq!(rows[1].0, 19);
    assert_eq!(rows[1].1.as_str(), Some("0xdddddddd"));
}
