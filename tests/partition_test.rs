//! Integration tests for block-range partitioning: fresh installs get
//! partitioned chain tables whose partitions are created on demand by the
//! writer; pre-partitioning deployments (regular tables) are untouched.

mod common;

use common::testdb::TestDb;

use serial_test::serial;
use tempo_alloy::primitives::transaction::{Call, TEMPO_TX_TYPE_ID};
use tidx::sync::writer::{delete_blocks_from, write_batch};
use tidx::types::{BlockRow, LogRow, ReceiptRow, TxRow};

use alloy::primitives::{Address, TxKind, U256};

/// Two blocks straddling the default 1M-block partition boundary.
const BLOCK_P0: i64 = 999_998;
const BLOCK_P1: i64 = 1_000_002;

async fn is_partitioned(pool: &tidx::db::Pool, table: &str) -> bool {
    let conn = pool.get().await.unwrap();
    conn.query_one(
        "SELECT EXISTS (
             SELECT 1 FROM pg_partitioned_table pt
             JOIN pg_class c ON c.oid = pt.partrelid
             JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE c.relname = $1 AND n.nspname = current_schema()
         )",
        &[&table],
    )
    .await
    .unwrap()
    .get(0)
}

async fn partitions_of(pool: &tidx::db::Pool, table: &str) -> Vec<String> {
    let conn = pool.get().await.unwrap();
    conn.query(
        "SELECT c.relname FROM pg_inherits i
         JOIN pg_class c ON c.oid = i.inhrelid
         JOIN pg_class p ON p.oid = i.inhparent
         WHERE p.relname = $1 ORDER BY c.relname",
        &[&table],
    )
    .await
    .unwrap()
    .iter()
    .map(|r| r.get(0))
    .collect()
}

fn make_block(num: i64) -> BlockRow {
    BlockRow {
        num,
        hash: vec![(num % 256) as u8; 32],
        parent_hash: vec![((num - 1) % 256) as u8; 32],
        timestamp: chrono::Utc::now(),
        timestamp_ms: chrono::Utc::now().timestamp_millis(),
        gas_limit: 30_000_000,
        gas_used: 15_000_000,
        miner: vec![0u8; 20],
        extra_data: None,
        consensus_proposer: None,
    }
}

fn make_tx(block_num: i64, calls: Vec<Call>) -> TxRow {
    TxRow {
        block_num,
        block_timestamp: chrono::Utc::now(),
        idx: 0,
        hash: vec![(block_num % 256) as u8; 32],
        tx_type: TEMPO_TX_TYPE_ID as i16,
        from: vec![1u8; 20],
        to: Some(vec![2u8; 20]),
        value: "0".to_string(),
        input: vec![0u8; 4],
        gas_limit: 21000,
        max_fee_per_gas: "0".to_string(),
        max_priority_fee_per_gas: "0".to_string(),
        gas_used: Some(21000),
        nonce_key: vec![0u8; 32],
        nonce: 0,
        fee_token: None,
        fee_payer: None,
        call_count: calls.len().max(1) as i16,
        calls,
        valid_before: None,
        valid_after: None,
        signature_type: Some(0),
    }
}

fn make_multicall() -> Vec<Call> {
    (0..2)
        .map(|i| Call {
            to: TxKind::Call(Address::repeat_byte(0xaa + i)),
            value: U256::from(i),
            input: vec![i].into(),
        })
        .collect()
}

fn make_log(block_num: i64) -> LogRow {
    LogRow {
        block_num,
        block_timestamp: chrono::Utc::now(),
        log_idx: 0,
        tx_idx: 0,
        tx_hash: vec![(block_num % 256) as u8; 32],
        address: vec![3u8; 20],
        selector: None,
        topic0: None,
        topic1: None,
        topic2: None,
        topic3: None,
        data: vec![0u8; 8],
        is_virtual_forward: false,
    }
}

fn make_receipt(block_num: i64) -> ReceiptRow {
    ReceiptRow {
        block_num,
        block_timestamp: chrono::Utc::now(),
        tx_idx: 0,
        tx_hash: vec![(block_num % 256) as u8; 32],
        from: vec![1u8; 20],
        to: Some(vec![2u8; 20]),
        contract_address: None,
        gas_used: 21000,
        cumulative_gas_used: 21000,
        effective_gas_price: Some("0".to_string()),
        status: Some(1),
        fee_payer: None,
        tx_type: None,
        fee_token: None,
    }
}

#[tokio::test]
#[serial(db)]
async fn test_partitions_created_across_boundary() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    if !is_partitioned(&db.pool, "blocks").await {
        println!(
            "blocks is not partitioned (pre-partitioning test database) - skipping; \
             recreate the test database for the partitioned layout"
        );
        return;
    }

    let blocks: Vec<BlockRow> = [BLOCK_P0, BLOCK_P1]
        .iter()
        .map(|&n| make_block(n))
        .collect();
    let txs: Vec<TxRow> = [BLOCK_P0, BLOCK_P1]
        .iter()
        .map(|&n| make_tx(n, make_multicall()))
        .collect();
    let logs: Vec<LogRow> = [BLOCK_P0, BLOCK_P1].iter().map(|&n| make_log(n)).collect();
    let receipts: Vec<ReceiptRow> = [BLOCK_P0, BLOCK_P1]
        .iter()
        .map(|&n| make_receipt(n))
        .collect();

    write_batch(&db.pool, &blocks, &txs, &logs, &receipts)
        .await
        .expect("write across partition boundary failed");

    // Every table got the two partitions the batch touched (other tests may
    // have created partitions for higher block ranges already; truncate
    // empties them but never drops them).
    for table in ["blocks", "txs", "tx_calls", "logs", "receipts"] {
        let parts = partitions_of(&db.pool, table).await;
        for expected in [format!("{table}_p0"), format!("{table}_p1")] {
            assert!(
                parts.contains(&expected),
                "expected partition {expected}, have {parts:?}"
            );
        }
    }

    // Rows landed on both sides and are visible through the parents.
    let conn = db.pool.get().await.unwrap();
    for (table, col) in [
        ("blocks", "num"),
        ("txs", "block_num"),
        ("tx_calls", "block_num"),
        ("logs", "block_num"),
        ("receipts", "block_num"),
    ] {
        let distinct: i64 = conn
            .query_one(&format!("SELECT COUNT(DISTINCT {col}) FROM {table}"), &[])
            .await
            .unwrap()
            .get(0);
        assert_eq!(distinct, 2, "expected rows in both partitions of {table}");
    }
    drop(conn);

    // Reorg deletes cross partition boundaries; partitions themselves remain.
    delete_blocks_from(&db.pool, BLOCK_P0 as u64).await.unwrap();

    let conn = db.pool.get().await.unwrap();
    for table in ["blocks", "txs", "tx_calls", "logs", "receipts"] {
        let count: i64 = conn
            .query_one(&format!("SELECT COUNT(*) FROM {table}"), &[])
            .await
            .unwrap()
            .get(0);
        assert_eq!(count, 0, "reorg delete must clear {table}");
    }
    let parts = partitions_of(&db.pool, "blocks").await;
    assert!(
        parts.contains(&"blocks_p0".to_string()) && parts.contains(&"blocks_p1".to_string()),
        "partitions survive reorg deletes"
    );
}

#[tokio::test]
#[serial(db)]
async fn test_ensure_block_partitions_noop_on_regular_table() {
    let db = TestDb::empty().await;

    let conn = db.pool.get().await.unwrap();
    conn.batch_execute(
        "DROP TABLE IF EXISTS partition_probe;
         CREATE TABLE partition_probe (block_num INT8 NOT NULL)",
    )
    .await
    .unwrap();

    // Pre-partitioning deployments: the function must not error and must not
    // create anything.
    conn.execute(
        "SELECT ensure_block_partitions('partition_probe', 0, 5000000)",
        &[],
    )
    .await
    .expect("ensure_block_partitions must no-op on a regular table");

    assert!(
        partitions_of(&db.pool, "partition_probe").await.is_empty(),
        "no partitions may be created for a regular table"
    );

    let conn = db.pool.get().await.unwrap();
    conn.batch_execute("DROP TABLE partition_probe")
        .await
        .unwrap();
}

#[tokio::test]
#[serial(db)]
async fn test_partition_width_locked_at_first_boot() {
    let db = TestDb::empty().await;

    // TestDb already ran migrations with the default width; re-running with a
    // different width must keep the locked value.
    tidx::db::run_migrations(&db.pool, 42)
        .await
        .expect("re-running migrations failed");

    let conn = db.pool.get().await.unwrap();
    let locked: i64 = conn
        .query_one("SELECT partition_blocks FROM storage_config", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        locked,
        tidx::db::DEFAULT_PARTITION_BLOCKS as i64,
        "partition width must stay locked at the first-boot value"
    );
}
