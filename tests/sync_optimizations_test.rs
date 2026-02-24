mod common;

use common::tempo::TempoNode;
use common::testdb::TestDb;

use tidx::db::ThrottledPool;
use tidx::sync::engine::SyncEngine;
use tidx::sync::sink::SinkSet;
use tidx::sync::writer::{write_blocks, write_logs, write_txs};
use tidx::types::{BlockRow, LogRow, TxRow};
use serial_test::serial;

fn generate_blocks(count: usize, offset: i64) -> Vec<BlockRow> {
    (0..count)
        .map(|i| {
            let n = offset + i as i64;
            BlockRow {
                num: n,
                hash: vec![(n % 256) as u8; 32],
                parent_hash: vec![((n - 1) % 256) as u8; 32],
                timestamp: chrono::Utc::now(),
                timestamp_ms: chrono::Utc::now().timestamp_millis(),
                gas_limit: 30_000_000,
                gas_used: 15_000_000,
                miner: vec![0u8; 20],
                extra_data: Some(vec![0u8; 32]),
            }
        })
        .collect()
}

fn generate_txs(count: usize, block_num: i64) -> Vec<TxRow> {
    (0..count)
        .map(|i| TxRow {
            block_num,
            block_timestamp: chrono::Utc::now(),
            idx: i as i32,
            hash: vec![(i % 256) as u8; 32],
            tx_type: 2,
            from: vec![1u8; 20],
            to: Some(vec![2u8; 20]),
            value: "0".to_string(),
            input: vec![0u8; 100],
            gas_limit: 21000,
            max_fee_per_gas: "1000000000".to_string(),
            max_priority_fee_per_gas: "100000000".to_string(),
            gas_used: Some(21000),
            nonce_key: vec![1u8; 20],
            nonce: i as i64,
            fee_token: None,
            fee_payer: None,
            calls: None,
            call_count: 1,
            valid_before: None,
            valid_after: None,
            signature_type: Some(0),
        })
        .collect()
}

fn generate_logs(count: usize, block_num: i64) -> Vec<LogRow> {
    (0..count)
        .map(|i| LogRow {
            block_num,
            block_timestamp: chrono::Utc::now(),
            log_idx: i as i32,
            tx_idx: (i % 100) as i32,
            tx_hash: vec![(i % 256) as u8; 32],
            address: vec![3u8; 20],
            selector: Some(vec![0xddu8; 32]),
            topic0: Some(vec![0xddu8; 32]),
            topic1: Some(vec![1u8; 32]),
            topic2: Some(vec![2u8; 32]),
            topic3: None,
            data: vec![0u8; 64],
        })
        .collect()
}

#[tokio::test]
#[serial(db)]
async fn test_batch_write_blocks() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    let blocks = generate_blocks(50, 10_000_000);
    write_blocks(&db.pool, &blocks).await.expect("Failed to write blocks");

    // Verify blocks in our range were written
    let conn = db.pool.get().await.unwrap();
    let count: i64 = conn
        .query_one("SELECT COUNT(*) FROM blocks WHERE num >= 10000000 AND num < 10000050", &[])
        .await
        .expect("Failed to count")
        .get(0);
    assert_eq!(count, 50, "Expected 50 blocks written in range");

    // Verify first and last block
    let conn = db.pool.get().await.unwrap();
    let first = conn
        .query_one("SELECT num FROM blocks WHERE num = 10000000", &[])
        .await
        .expect("First block not found");
    assert_eq!(first.get::<_, i64>(0), 10_000_000);

    let last = conn
        .query_one("SELECT num FROM blocks WHERE num = 10000049", &[])
        .await
        .expect("Last block not found");
    assert_eq!(last.get::<_, i64>(0), 10_000_049);
}

#[tokio::test]
#[serial(db)]
async fn test_batch_write_txs() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    // Need to write a block first for FK constraint
    let blocks = generate_blocks(1, 20_000_000);
    write_blocks(&db.pool, &blocks).await.unwrap();

    let txs = generate_txs(100, 20_000_000);
    write_txs(&db.pool, &txs).await.expect("Failed to write txs");

    // Verify type is preserved (count for this specific block)
    let conn = db.pool.get().await.unwrap();
    let row = conn
        .query_one(
            "SELECT type, COUNT(*) FROM txs WHERE block_num = 20000000 GROUP BY type",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, i16>(0), 2);
    assert_eq!(row.get::<_, i64>(1), 100);
}

#[tokio::test]
#[serial(db)]
async fn test_batch_write_logs() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    // Need block and tx for FK
    let blocks = generate_blocks(1, 30_000_000);
    write_blocks(&db.pool, &blocks).await.unwrap();

    let logs = generate_logs(500, 30_000_000);
    write_logs(&db.pool, &logs).await.expect("Failed to write logs");

    // Verify selector is preserved (count for this specific block)
    let conn = db.pool.get().await.unwrap();
    let row = conn
        .query_one(
            "SELECT COUNT(*) FROM logs WHERE block_num = 30000000 AND selector IS NOT NULL",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, i64>(0), 500);
}

#[tokio::test]
#[serial(db)]
async fn test_batch_write_mixed_realistic() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    // Simulate 10 blocks with 500 txs and 1000 logs each
    let blocks = generate_blocks(10, 40_000_000);
    let txs: Vec<_> = (0..10)
        .flat_map(|i| generate_txs(500, 40_000_000 + i))
        .collect();
    let logs: Vec<_> = (0..10)
        .flat_map(|i| generate_logs(1000, 40_000_000 + i))
        .collect();

    write_blocks(&db.pool, &blocks).await.unwrap();
    write_txs(&db.pool, &txs).await.unwrap();
    write_logs(&db.pool, &logs).await.unwrap();

    // Count for specific block range
    let conn = db.pool.get().await.unwrap();
    let block_count: i64 = conn
        .query_one("SELECT COUNT(*) FROM blocks WHERE num >= 40000000 AND num < 40000010", &[])
        .await
        .unwrap()
        .get(0);
    let tx_count: i64 = conn
        .query_one("SELECT COUNT(*) FROM txs WHERE block_num >= 40000000 AND block_num < 40000010", &[])
        .await
        .unwrap()
        .get(0);
    let log_count: i64 = conn
        .query_one("SELECT COUNT(*) FROM logs WHERE block_num >= 40000000 AND block_num < 40000010", &[])
        .await
        .unwrap()
        .get(0);

    assert_eq!(block_count, 10);
    assert_eq!(tx_count, 5000);
    assert_eq!(log_count, 10000);
}

#[tokio::test]
#[serial(db)]
async fn test_copy_large_batch_txs() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    // Test COPY with a large batch (5000 txs)
    let blocks = generate_blocks(1, 50_000_000);
    write_blocks(&db.pool, &blocks).await.unwrap();

    let txs = generate_txs(5000, 50_000_000);
    write_txs(&db.pool, &txs).await.expect("Failed to COPY txs");

    let conn = db.pool.get().await.unwrap();
    let count: i64 = conn
        .query_one("SELECT COUNT(*) FROM txs WHERE block_num = 50000000", &[])
        .await
        .unwrap()
        .get(0);

    assert_eq!(count, 5000);
}

#[tokio::test]
#[serial(db)]
async fn test_copy_large_batch_logs() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    // Test COPY with a large batch (10000 logs)
    let blocks = generate_blocks(1, 60_000_000);
    write_blocks(&db.pool, &blocks).await.unwrap();

    let logs = generate_logs(10000, 60_000_000);
    write_logs(&db.pool, &logs).await.expect("Failed to COPY logs");

    let conn = db.pool.get().await.unwrap();
    let count: i64 = conn
        .query_one("SELECT COUNT(*) FROM logs WHERE block_num = 60000000", &[])
        .await
        .unwrap()
        .get(0);

    assert_eq!(count, 10000);
}

#[tokio::test]
#[serial(db)]
async fn test_delete_copy_idempotent() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    // Test that rewriting the same block range is idempotent (DELETE + COPY)
    let blocks = generate_blocks(1, 70_000_000);
    write_blocks(&db.pool, &blocks).await.unwrap();

    let txs = generate_txs(100, 70_000_000);
    write_txs(&db.pool, &txs).await.unwrap();
    write_txs(&db.pool, &txs).await.unwrap(); // Second write should delete and reinsert

    let conn = db.pool.get().await.unwrap();
    let count: i64 = conn
        .query_one("SELECT COUNT(*) FROM txs WHERE block_num = 70000000", &[])
        .await
        .unwrap()
        .get(0);

    assert_eq!(count, 100, "Rewrite should have exactly 100 txs");
}

#[tokio::test]
#[serial(db)]
async fn test_delete_copy_overwrites_existing_data() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    let blocks = generate_blocks(1, 71_000_000);
    write_blocks(&db.pool, &blocks).await.unwrap();

    // Write initial logs with specific selector
    let mut logs = generate_logs(50, 71_000_000);
    for log in &mut logs {
        log.selector = Some(vec![0xaa, 0xbb, 0xcc, 0xdd]);
    }
    write_logs(&db.pool, &logs).await.unwrap();

    // Verify initial selector
    let conn = db.pool.get().await.unwrap();
    let initial: Vec<u8> = conn
        .query_one("SELECT selector FROM logs WHERE block_num = 71000000 LIMIT 1", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(initial, vec![0xaa, 0xbb, 0xcc, 0xdd]);

    // Rewrite with different selector - should DELETE old and INSERT new
    for log in &mut logs {
        log.selector = Some(vec![0x11, 0x22, 0x33, 0x44]);
    }
    write_logs(&db.pool, &logs).await.unwrap();

    // Verify data was replaced
    let updated: Vec<u8> = conn
        .query_one("SELECT selector FROM logs WHERE block_num = 71000000 LIMIT 1", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(updated, vec![0x11, 0x22, 0x33, 0x44], "Data should be overwritten");

    let count: i64 = conn
        .query_one("SELECT COUNT(*) FROM logs WHERE block_num = 71000000", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 50, "Should still have exactly 50 logs");
}

#[tokio::test]
#[serial(db)]
async fn test_delete_copy_handles_block_range() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    // Write blocks 72_000_000 to 72_000_009
    let blocks = generate_blocks(10, 72_000_000);
    write_blocks(&db.pool, &blocks).await.unwrap();

    // Write txs for blocks 0-4
    let txs_first: Vec<_> = (0..5)
        .flat_map(|i| generate_txs(10, 72_000_000 + i))
        .collect();
    write_txs(&db.pool, &txs_first).await.unwrap();

    // Write txs for blocks 3-7 (overlapping range)
    let txs_second: Vec<_> = (3..8)
        .flat_map(|i| generate_txs(20, 72_000_000 + i))
        .collect();
    write_txs(&db.pool, &txs_second).await.unwrap();

    let conn = db.pool.get().await.unwrap();

    // Blocks 0-2 should still have 10 txs each (untouched)
    let count_0_2: i64 = conn
        .query_one(
            "SELECT COUNT(*) FROM txs WHERE block_num >= 72000000 AND block_num <= 72000002",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(count_0_2, 30, "Blocks 0-2 should have 10 txs each");

    // Blocks 3-7 should have 20 txs each (overwritten)
    let count_3_7: i64 = conn
        .query_one(
            "SELECT COUNT(*) FROM txs WHERE block_num >= 72000003 AND block_num <= 72000007",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(count_3_7, 100, "Blocks 3-7 should have 20 txs each");
}

#[tokio::test]
#[serial(db)]
async fn test_pipelined_sync() {
    let tempo = TempoNode::from_env();
    tempo.wait_for_ready().await.expect("Tempo node not ready");

    let db = TestDb::empty().await;
    db.truncate_all().await;

    // Wait for some blocks
    tempo.wait_for_block(30).await.expect("Block 30 not reached");

    let sinks = SinkSet::new(db.pool.clone());
    let mut engine = SyncEngine::new(ThrottledPool::from_pool(db.pool.clone()), sinks, &tempo.rpc_url)
        .await
        .expect("Failed to create sync engine");

    // Create a shutdown channel
    let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);

    // Run engine in background, let it sync for a bit
    let engine_handle = tokio::spawn(async move {
        let _ = engine.run(shutdown_rx).await;
    });

    // Wait a bit for sync
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    // Signal shutdown
    let _ = shutdown_tx.send(());
    let _ = engine_handle.await;

    // Verify we synced some blocks
    let block_count = db.block_count().await;
    println!("Pipelined sync: {block_count} blocks synced");
    assert!(block_count > 0, "Expected some blocks to be synced");
}
