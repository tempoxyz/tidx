//! TimescaleDB columnstore cold-tier integration tests
//!
//! Exercises hypertable setup, chunk compression behind the hot window,
//! point lookups / idempotent writes / reorg deletes against compressed
//! chunks — the full cold-tier lifecycle on a real TimescaleDB server.
//!
//! Run with: cargo test --test timescale_test
//! Requires: docker compose -f docker/local/docker-compose.yml up -d timescaledb

use chrono::{DateTime, TimeZone, Utc};
use tidx::config::TimescaleConfig;
use tidx::db::{Pool, create_pool, run_migrations, run_post_startup_migrations};
use tidx::sync::{timescale, writer};
use tidx::types::{BlockRow, LogRow, ReceiptRow, SyncState, TxRow};

const CHAIN_ID: u64 = 42431;

fn ts_config() -> TimescaleConfig {
    TimescaleConfig {
        enabled: true,
        chunk_blocks: 10,
        hot_window_blocks: 20,
        interval_secs: 1,
    }
}

/// Connect to the TimescaleDB test server, creating `db` if needed.
/// Returns None (test skips) when the server is unreachable.
async fn connect(db: &str) -> Option<Pool> {
    let base = std::env::var("TIMESCALE_DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://tidx:tidx@localhost:5434/tidx".to_string());
    let mut url = url::Url::parse(&base).expect("invalid TIMESCALE_DATABASE_URL");
    url.set_path(&format!("/{db}"));

    match create_pool(url.as_str()).await {
        Ok(pool) => Some(pool),
        Err(e) => {
            println!("TimescaleDB not available ({e}), skipping test");
            None
        }
    }
}

async fn reset(pool: &Pool) {
    let conn = pool.get().await.expect("get conn");
    conn.batch_execute("DROP TABLE IF EXISTS blocks, txs, logs, receipts, sync_state CASCADE")
        .await
        .expect("drop tables");
    drop(conn);
    run_migrations(pool).await.expect("run migrations");
}

fn ts(num: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(1_760_000_000 + num, 0).unwrap()
}

/// Deterministic 32-byte pseudo-hash from a domain tag and a seed.
fn hash32(tag: u8, seed: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(32);
    for i in 0..4u64 {
        out.extend_from_slice(
            &(seed
                .wrapping_mul(0x9E37_79B9_7F4A_7C15)
                .rotate_left(i as u32 * 8)
                ^ u64::from(tag))
            .to_be_bytes(),
        );
    }
    out
}

fn addr(tag: u8, seed: u64) -> Vec<u8> {
    hash32(tag, seed)[..20].to_vec()
}

fn block_row(num: i64) -> BlockRow {
    BlockRow {
        num,
        hash: hash32(1, num as u64),
        parent_hash: hash32(1, num as u64 - 1),
        timestamp: ts(num),
        timestamp_ms: ts(num).timestamp_millis(),
        gas_limit: 30_000_000,
        gas_used: 42_000,
        miner: addr(9, 0),
        extra_data: None,
        consensus_proposer: None,
    }
}

fn tx_row(num: i64, idx: i32) -> TxRow {
    TxRow {
        block_num: num,
        block_timestamp: ts(num),
        idx,
        hash: hash32(2, ((num as u64) << 8) | idx as u64),
        tx_type: 0,
        from: addr(3, num as u64 % 25),
        to: Some(addr(4, idx as u64 % 7)),
        value: "0".to_string(),
        input: vec![0xAB; 64],
        gas_limit: 21_000,
        max_fee_per_gas: "1".to_string(),
        max_priority_fee_per_gas: "1".to_string(),
        gas_used: Some(21_000),
        nonce_key: vec![0],
        nonce: num * 100 + idx as i64,
        call_count: 1,
        ..Default::default()
    }
}

fn log_row(num: i64, log_idx: i32) -> LogRow {
    LogRow {
        block_num: num,
        block_timestamp: ts(num),
        log_idx,
        tx_idx: log_idx / 2,
        tx_hash: hash32(2, ((num as u64) << 8) | (log_idx / 2) as u64),
        address: addr(5, num as u64 % 10),
        selector: Some(vec![0xDD; 4]),
        topic0: Some(hash32(6, 0)),
        topic1: Some(hash32(7, num as u64 % 25)),
        topic2: None,
        topic3: None,
        data: vec![0xEE; 96],
        is_virtual_forward: false,
    }
}

fn receipt_row(num: i64, tx_idx: i32) -> ReceiptRow {
    ReceiptRow {
        block_num: num,
        block_timestamp: ts(num),
        tx_idx,
        tx_hash: hash32(2, ((num as u64) << 8) | tx_idx as u64),
        from: addr(3, num as u64 % 25),
        to: Some(addr(4, tx_idx as u64 % 7)),
        contract_address: None,
        gas_used: 21_000,
        cumulative_gas_used: 21_000 * (tx_idx as i64 + 1),
        effective_gas_price: Some("1".to_string()),
        status: Some(1),
        fee_payer: None,
        ..Default::default()
    }
}

/// Write blocks `range` with 2 txs, 4 logs and 2 receipts each.
async fn write_range(pool: &Pool, range: std::ops::RangeInclusive<i64>) {
    let mut blocks = Vec::new();
    let mut txs = Vec::new();
    let mut logs = Vec::new();
    let mut receipts = Vec::new();
    for num in range {
        blocks.push(block_row(num));
        for idx in 0..2 {
            txs.push(tx_row(num, idx));
            receipts.push(receipt_row(num, idx));
        }
        for log_idx in 0..4 {
            logs.push(log_row(num, log_idx));
        }
    }
    writer::write_blocks(pool, &blocks)
        .await
        .expect("write blocks");
    writer::write_txs(pool, &txs).await.expect("write txs");
    writer::write_logs(pool, &logs).await.expect("write logs");
    writer::write_receipts(pool, &receipts)
        .await
        .expect("write receipts");
}

async fn count(pool: &Pool, table: &str) -> i64 {
    let conn = pool.get().await.expect("get conn");
    conn.query_one(&format!("SELECT count(*) FROM {table}"), &[])
        .await
        .expect("count")
        .get(0)
}

async fn compressed_chunks(pool: &Pool, table: &str) -> i64 {
    let conn = pool.get().await.expect("get conn");
    conn.query_one(
        "SELECT count(*) FROM timescaledb_information.chunks \
         WHERE hypertable_schema = 'public' AND hypertable_name = $1 AND is_compressed",
        &[&table],
    )
    .await
    .expect("chunk status")
    .get(0)
}

#[tokio::test]
async fn test_columnstore_cold_tier_end_to_end() {
    let Some(pool) = connect("tidx_timescale_test").await else {
        return;
    };
    reset(&pool).await;

    let cfg = ts_config();

    // Setup converts empty chain tables to hypertables — and is idempotent.
    assert!(timescale::setup(&pool, &cfg).await.expect("setup"));
    assert!(timescale::setup(&pool, &cfg).await.expect("setup again"));

    // Post-startup index migrations must work on hypertables (no CONCURRENTLY).
    run_post_startup_migrations(&pool)
        .await
        .expect("post-startup migrations on hypertables");

    // 100 blocks * (1 block + 2 txs + 4 logs + 2 receipts).
    write_range(&pool, 1..=100).await;
    assert_eq!(count(&pool, "blocks").await, 100);
    assert_eq!(count(&pool, "txs").await, 200);
    assert_eq!(count(&pool, "logs").await, 400);
    assert_eq!(count(&pool, "receipts").await, 200);

    // Nothing converts before sync state exists.
    assert!(
        timescale::run_pass(&pool, CHAIN_ID, &cfg)
            .await
            .expect("pass without sync state")
            .is_empty()
    );

    writer::save_sync_state(
        &pool,
        &SyncState {
            chain_id: CHAIN_ID,
            head_num: 100,
            synced_num: 100,
            tip_num: 100,
            backfill_num: Some(0),
            sync_rate: None,
            started_at: None,
        },
    )
    .await
    .expect("save sync state");

    // cold ceiling = min(synced=100, tip-hot_window=80) → chunks [0,10) … [70,80)
    // convert: 8 chunks per table across 4 tables.
    let converted = timescale::run_pass(&pool, CHAIN_ID, &cfg)
        .await
        .expect("cold-tier pass");
    assert_eq!(
        converted.len(),
        32,
        "expected 8 chunks x 4 tables: {converted:?}"
    );
    for table in ["blocks", "txs", "logs", "receipts"] {
        assert_eq!(compressed_chunks(&pool, table).await, 8, "table {table}");
    }

    // Row counts survive conversion.
    assert_eq!(count(&pool, "blocks").await, 100);
    assert_eq!(count(&pool, "txs").await, 200);
    assert_eq!(count(&pool, "logs").await, 400);
    assert_eq!(count(&pool, "receipts").await, 200);

    let conn = pool.get().await.expect("get conn");

    // Point lookup by tx hash inside a compressed chunk (block 42).
    let h = hash32(2, (42u64 << 8) | 1);
    let row = conn
        .query_one("SELECT block_num, idx FROM txs WHERE hash = $1", &[&h])
        .await
        .expect("point lookup on compressed chunk");
    assert_eq!(row.get::<_, i64>(0), 42);
    assert_eq!(row.get::<_, i32>(1), 1);

    // Log lookup by address equality across compressed chunks.
    let a = addr(5, 2);
    let hits: i64 = conn
        .query_one(
            "SELECT count(*) FROM logs WHERE address = $1 AND block_num <= 79",
            &[&a],
        )
        .await
        .expect("address lookup")
        .get(0);
    assert!(hits > 0, "expected address hits in compressed range");

    // Block lookup by hash inside a compressed chunk.
    let bh = hash32(1, 42);
    let bnum: i64 = conn
        .query_one("SELECT num FROM blocks WHERE hash = $1", &[&bh])
        .await
        .expect("block hash lookup")
        .get(0);
    assert_eq!(bnum, 42);
    drop(conn);

    // Idempotent re-write into compressed chunks (ON CONFLICT DO NOTHING).
    write_range(&pool, 40..=45).await;
    assert_eq!(count(&pool, "txs").await, 200);
    assert_eq!(count(&pool, "logs").await, 400);

    // Reorg-style delete against a compressed chunk, then re-sync the range.
    let conn = pool.get().await.expect("get conn");
    for table in ["txs", "logs", "receipts"] {
        conn.execute(
            &format!("DELETE FROM {table} WHERE block_num = ANY($1)"),
            &[&vec![42i64, 43]],
        )
        .await
        .expect("reorg delete on compressed chunk");
    }
    conn.execute(
        "DELETE FROM blocks WHERE num = ANY($1)",
        &[&vec![42i64, 43]],
    )
    .await
    .expect("reorg delete blocks");
    drop(conn);
    assert_eq!(count(&pool, "txs").await, 196);
    write_range(&pool, 42..=43).await;
    assert_eq!(count(&pool, "blocks").await, 100);
    assert_eq!(count(&pool, "txs").await, 200);
    assert_eq!(count(&pool, "logs").await, 400);
    assert_eq!(count(&pool, "receipts").await, 200);

    // A second pass has nothing new to convert.
    assert!(
        timescale::run_pass(&pool, CHAIN_ID, &cfg)
            .await
            .expect("idempotent pass")
            .is_empty()
    );
}

#[tokio::test]
async fn test_incomplete_ranges_stay_in_rowstore() {
    let Some(pool) = connect("tidx_timescale_gaps").await else {
        return;
    };
    reset(&pool).await;

    let cfg = ts_config();
    assert!(timescale::setup(&pool, &cfg).await.expect("setup"));

    // Blocks 1..=100 except 25: chunk [20,30) has a gap that backfill
    // still needs to fill.
    write_range(&pool, 1..=24).await;
    write_range(&pool, 26..=100).await;

    writer::save_sync_state(
        &pool,
        &SyncState {
            chain_id: CHAIN_ID,
            head_num: 100,
            synced_num: 24,
            tip_num: 100,
            backfill_num: None,
            sync_rate: None,
            started_at: None,
        },
    )
    .await
    .expect("save sync state");

    // Cold ceiling covers chunks [0,10) … [70,80), but [20,30) is not
    // fully populated: 7 chunks x 4 tables convert.
    let converted = timescale::run_pass(&pool, CHAIN_ID, &cfg)
        .await
        .expect("pass with gap");
    assert_eq!(
        converted.len(),
        28,
        "expected gap chunk skipped: {converted:?}"
    );
    assert!(
        !converted.iter().any(|(_, lo, _)| *lo == 20),
        "chunk [20,30) must stay in rowstore: {converted:?}"
    );

    // Backfill fills the hole; the next pass converts the remaining chunks.
    write_range(&pool, 25..=25).await;
    let converted = timescale::run_pass(&pool, CHAIN_ID, &cfg)
        .await
        .expect("pass after fill");
    assert_eq!(
        converted.len(),
        4,
        "expected [20,30) x 4 tables: {converted:?}"
    );
    assert!(converted.iter().all(|(_, lo, hi)| (*lo, *hi) == (20, 30)));
}

#[tokio::test]
async fn test_setup_refuses_existing_plain_tables_with_data() {
    let Some(pool) = connect("tidx_timescale_dirty").await else {
        return;
    };
    reset(&pool).await;

    // Data lands in plain (non-hypertable) tables first.
    write_range(&pool, 1..=5).await;

    let err = timescale::setup(&pool, &ts_config())
        .await
        .expect_err("setup must refuse non-empty plain tables");
    assert!(
        err.to_string().contains("fresh database"),
        "unexpected error: {err}"
    );
}
