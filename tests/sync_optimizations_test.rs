mod common;

use common::tempo::TempoNode;
use common::testdb::TestDb;

use alloy::primitives::{Address, B256, Bloom};
use axum::{Json, Router, routing::post};
use serde_json::{Value, json};
use serial_test::serial;
use std::time::Duration;
use tidx::db::ThrottledPool;
use tidx::sync::engine::SyncEngine;
use tidx::sync::sink::SinkSet;
use tidx::sync::writer::{
    detect_blocks_missing_receipts, discover_legacy_receipt_repairs, finish_receipt_repair_attempt,
    write_batch, write_blocks, write_logs, write_txs,
};
use tidx::types::{BlockRow, LogRow, ReceiptRow, TxRow};

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
                consensus_proposer: None,
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
            is_virtual_forward: false,
        })
        .collect()
}

#[tokio::test]
#[serial(db)]
async fn test_batch_write_blocks() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    let blocks = generate_blocks(50, 10_000_000);
    write_blocks(&db.pool, &blocks)
        .await
        .expect("Failed to write blocks");

    // Verify blocks in our range were written
    let conn = db.pool.get().await.unwrap();
    let count: i64 = conn
        .query_one(
            "SELECT COUNT(*) FROM blocks WHERE num >= 10000000 AND num < 10000050",
            &[],
        )
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
    write_txs(&db.pool, &txs)
        .await
        .expect("Failed to write txs");

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
async fn test_receipt_repair_queue_tracks_only_incomplete_transactions() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    let blocks = generate_blocks(3, 21_000_000);
    write_blocks(&db.pool, &blocks).await.unwrap();

    let mut incomplete = generate_txs(1, 21_000_001);
    incomplete[0].gas_used = None;
    let complete = generate_txs(1, 21_000_002);
    let txs: Vec<_> = incomplete.into_iter().chain(complete).collect();
    write_txs(&db.pool, &txs).await.unwrap();

    let claimed = detect_blocks_missing_receipts(&db.pool, 100).await.unwrap();
    assert_eq!(claimed, vec![21_000_001]);

    let (completed, deferred) = finish_receipt_repair_attempt(&db.pool, &claimed)
        .await
        .unwrap();
    assert_eq!((completed, deferred), (0, 1));

    let conn = db.pool.get().await.unwrap();
    let row = conn
        .query_one(
            "SELECT attempts, next_attempt_at > NOW() \
             FROM receipt_repair_queue WHERE block_num = 21000001",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, i32>(0), 1);
    assert!(row.get::<_, bool>(1));

    conn.execute(
        "UPDATE txs SET gas_used = 21000 WHERE block_num = 21000001",
        &[],
    )
    .await
    .unwrap();
    let (completed, deferred) = finish_receipt_repair_attempt(&db.pool, &claimed)
        .await
        .unwrap();
    assert_eq!((completed, deferred), (1, 0));

    let queue_count: i64 = conn
        .query_one("SELECT COUNT(*) FROM receipt_repair_queue", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(queue_count, 0, "empty and complete blocks must not queue");
}

#[tokio::test]
#[serial(db)]
async fn test_receipt_repair_legacy_discovery_is_bounded_and_durable() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    let blocks = generate_blocks(2, 21_100_000);
    write_blocks(&db.pool, &blocks).await.unwrap();

    let mut incomplete = generate_txs(1, 21_100_000);
    incomplete[0].gas_used = None;
    let complete = generate_txs(1, 21_100_001);
    let txs: Vec<_> = incomplete.into_iter().chain(complete).collect();
    write_txs(&db.pool, &txs).await.unwrap();

    let conn = db.pool.get().await.unwrap();
    conn.execute("TRUNCATE receipt_repair_queue", &[])
        .await
        .unwrap();

    assert_eq!(
        discover_legacy_receipt_repairs(&db.pool, 42431, 1)
            .await
            .unwrap(),
        0,
        "first one-block window contains only the complete block"
    );
    assert_eq!(
        discover_legacy_receipt_repairs(&db.pool, 42431, 1)
            .await
            .unwrap(),
        1,
        "second one-block window discovers the legacy incomplete block"
    );

    let state = conn
        .query_one(
            "SELECT completed, next_block FROM receipt_repair_discovery WHERE chain_id = 42431",
            &[],
        )
        .await
        .unwrap();
    assert!(state.get::<_, bool>(0));
    assert_eq!(state.get::<_, Option<i64>>(1), None);

    assert_eq!(
        discover_legacy_receipt_repairs(&db.pool, 42431, 1)
            .await
            .unwrap(),
        0,
        "completed discovery must never rescan history"
    );
    assert_eq!(
        detect_blocks_missing_receipts(&db.pool, 100).await.unwrap(),
        vec![21_100_000]
    );
}

#[tokio::test]
#[serial(db)]
async fn test_receipt_backfill_skips_receipts_of_blocks_not_stored() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    // Two blocks stored without receipt data. The node serves the first one, but a
    // different block (another fork) at the second height.
    let mut blocks = generate_blocks(2, 21_200_000);
    blocks[1].hash = vec![0xff; 32];
    write_blocks(&db.pool, &blocks).await.unwrap();

    let mut txs: Vec<_> = blocks
        .iter()
        .flat_map(|block| generate_txs(1, block.num))
        .collect();
    for tx in &mut txs {
        tx.gas_used = None;
    }
    write_txs(&db.pool, &txs).await.unwrap();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("Failed to bind test RPC server");
    let rpc_url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(listener, Router::new().route("/", post(receipt_rpc)))
            .await
            .expect("RPC server failed");
    });

    let mut engine = SyncEngine::new(
        ThrottledPool::from_pool(db.pool.clone()),
        SinkSet::new(db.pool.clone()),
        &rpc_url,
    )
    .await
    .expect("Failed to create sync engine")
    .with_gapfill_enabled(false);

    let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
    let engine_handle = tokio::spawn(async move {
        let _ = engine.run(shutdown_rx).await;
    });

    // The first repair attempt is over once the block the node serves has left the queue.
    let conn = db.pool.get().await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let queued: i64 = conn
                .query_one(
                    "SELECT COUNT(*) FROM receipt_repair_queue WHERE block_num = $1",
                    &[&blocks[0].num],
                )
                .await
                .unwrap()
                .get(0);
            if queued == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("receipt backfill did not repair the stored block");

    let _ = shutdown_tx.send(());
    let _ = engine_handle.await;
    server.abort();

    let receipt_blocks: Vec<i64> = conn
        .query("SELECT block_num FROM receipts ORDER BY block_num", &[])
        .await
        .unwrap()
        .iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(
        receipt_blocks,
        vec![blocks[0].num],
        "receipts of a block that is not stored must not be written"
    );

    let queued: Vec<i64> = conn
        .query("SELECT block_num FROM receipt_repair_queue", &[])
        .await
        .unwrap()
        .iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(
        queued,
        vec![blocks[1].num],
        "a block without usable receipts must stay queued"
    );
}

/// JSON-RPC node at head 0 (keeps realtime sync idle) that serves one receipt per block,
/// carrying the block hash `generate_blocks` assigns to that height.
async fn receipt_rpc(Json(body): Json<Value>) -> Json<Value> {
    fn respond(req: &Value) -> Value {
        let result = match req["method"].as_str().expect("missing method") {
            "eth_chainId" => json!("0x1"),
            "eth_blockNumber" => json!("0x0"),
            "eth_getBlockReceipts" => {
                let block = req["params"][0].as_str().expect("missing block number");
                let num = u64::from_str_radix(block.trim_start_matches("0x"), 16).unwrap();
                json!([{
                    "type": "0x2",
                    "status": "0x1",
                    "cumulativeGasUsed": "0x5208",
                    "logs": [],
                    "logsBloom": Bloom::ZERO,
                    "transactionHash": B256::ZERO,
                    "transactionIndex": "0x0",
                    "blockHash": B256::repeat_byte((num % 256) as u8),
                    "blockNumber": block,
                    "gasUsed": "0x5208",
                    "from": Address::ZERO,
                    "to": null,
                    "contractAddress": null,
                    "feePayer": Address::ZERO,
                }])
            }
            method => panic!("unexpected RPC method {method}"),
        };
        json!({ "jsonrpc": "2.0", "id": req["id"], "result": result })
    }

    match body.as_array() {
        Some(batch) => Json(batch.iter().map(respond).collect()),
        None => Json(respond(&body)),
    }
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
    write_logs(&db.pool, &logs)
        .await
        .expect("Failed to write logs");

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
async fn test_write_logs_persists_virtual_forward_flag() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    let blocks = generate_blocks(1, 30_100_000);
    write_blocks(&db.pool, &blocks).await.unwrap();

    let mut logs = generate_logs(2, 30_100_000);
    logs[1].is_virtual_forward = true;
    write_logs(&db.pool, &logs)
        .await
        .expect("Failed to write logs");

    let conn = db.pool.get().await.unwrap();
    let rows = conn
        .query(
            "SELECT log_idx, is_virtual_forward FROM logs WHERE block_num = 30100000 ORDER BY log_idx",
            &[],
        )
        .await
        .unwrap();

    assert_eq!(rows.len(), 2);
    assert!(!rows[0].get::<_, bool>(1));
    assert!(rows[1].get::<_, bool>(1));
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
        .query_one(
            "SELECT COUNT(*) FROM blocks WHERE num >= 40000000 AND num < 40000010",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    let tx_count: i64 = conn
        .query_one(
            "SELECT COUNT(*) FROM txs WHERE block_num >= 40000000 AND block_num < 40000010",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    let log_count: i64 = conn
        .query_one(
            "SELECT COUNT(*) FROM logs WHERE block_num >= 40000000 AND block_num < 40000010",
            &[],
        )
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
    write_logs(&db.pool, &logs)
        .await
        .expect("Failed to COPY logs");

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
        .query_one(
            "SELECT selector FROM logs WHERE block_num = 71000000 LIMIT 1",
            &[],
        )
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
        .query_one(
            "SELECT selector FROM logs WHERE block_num = 71000000 LIMIT 1",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        updated,
        vec![0x11, 0x22, 0x33, 0x44],
        "Data should be overwritten"
    );

    let count: i64 = conn
        .query_one("SELECT COUNT(*) FROM logs WHERE block_num = 71000000", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 50, "Should still have exactly 50 logs");
}

#[tokio::test]
#[serial(db)]
async fn test_rewrite_replaces_rows_of_block_with_other_timestamp() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    let block_num = 72_000_000;
    let receipts = |timestamp| -> Vec<ReceiptRow> {
        (0..3)
            .map(|i| ReceiptRow {
                block_num,
                block_timestamp: timestamp,
                tx_idx: i,
                tx_hash: vec![i as u8; 32],
                from: vec![1u8; 20],
                gas_used: 21000,
                cumulative_gas_used: 21000,
                ..Default::default()
            })
            .collect()
    };
    let rows = |timestamp, txs: usize, logs: usize| {
        let mut block = generate_blocks(1, block_num);
        block[0].timestamp = timestamp;
        let mut tx_rows = generate_txs(txs, block_num);
        for tx in &mut tx_rows {
            tx.block_timestamp = timestamp;
        }
        let mut log_rows = generate_logs(logs, block_num);
        for log in &mut log_rows {
            log.block_timestamp = timestamp;
        }
        (block, tx_rows, log_rows)
    };

    // The block as first stored, then the version a reorg replaced it with,
    // produced a few seconds later and carrying fewer rows.
    let first = chrono::Utc::now() - chrono::Duration::minutes(5);
    let (blocks, txs, logs) = rows(first, 3, 6);
    write_batch(&db.pool, &blocks, &txs, &logs, &receipts(first))
        .await
        .unwrap();

    let second = first + chrono::Duration::seconds(3);
    let (blocks, txs, logs) = rows(second, 2, 4);
    write_batch(&db.pool, &blocks, &txs, &logs, &receipts(second)[..2])
        .await
        .unwrap();

    let conn = db.pool.get().await.unwrap();
    for (table, expected) in [("txs", 2), ("logs", 4), ("receipts", 2)] {
        let row = conn
            .query_one(
                &format!(
                    "SELECT COUNT(*), COUNT(*) FILTER (WHERE block_timestamp = $2) \
                     FROM {table} WHERE block_num = $1"
                ),
                &[&block_num, &second],
            )
            .await
            .unwrap();
        let (total, current): (i64, i64) = (row.get(0), row.get(1));
        assert_eq!(total, expected, "{table} should hold only the new version");
        assert_eq!(
            current, expected,
            "{table} rows should carry the new timestamp"
        );
    }
}

#[tokio::test]
#[serial(db)]
async fn test_write_batch_roundtrips_every_column() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    let base = chrono::Utc::now().timestamp() - 3600;
    let ts = |secs: i64| chrono::DateTime::from_timestamp(base + secs, 0).unwrap();
    let blocks = vec![
        BlockRow {
            num: 73_000_000,
            hash: vec![1; 32],
            parent_hash: vec![2; 32],
            timestamp: ts(0),
            timestamp_ms: 1_780_000_000_250,
            gas_limit: 30_000_000,
            gas_used: 21_000,
            miner: vec![3; 20],
            extra_data: Some(vec![4, 5]),
            consensus_proposer: Some(vec![6; 32]),
        },
        BlockRow {
            num: 73_000_001,
            hash: vec![7; 32],
            parent_hash: vec![1; 32],
            timestamp: ts(1),
            timestamp_ms: 1_780_000_001_000,
            gas_limit: 30_000_000,
            gas_used: 0,
            miner: vec![8; 20],
            extra_data: None,
            consensus_proposer: None,
        },
    ];
    let txs = vec![
        TxRow {
            block_num: 73_000_000,
            block_timestamp: ts(0),
            idx: 0,
            hash: vec![9; 32],
            tx_type: 118,
            from: vec![10; 20],
            to: Some(vec![11; 20]),
            value: "1000000000000000000000".to_string(),
            input: vec![0xa9, 0x05, 0x9c, 0xbb],
            gas_limit: 100_000,
            max_fee_per_gas: "20000000000".to_string(),
            max_priority_fee_per_gas: "1".to_string(),
            gas_used: Some(50_000),
            nonce_key: vec![12; 32],
            nonce: 7,
            fee_token: Some(vec![13; 20]),
            fee_payer: Some(vec![14; 20]),
            calls: Some(serde_json::json!([{ "to": "0x01", "input": "0x", "value": "0x0" }])),
            call_count: 1,
            valid_before: Some(1_780_000_100),
            valid_after: Some(1_779_999_900),
            signature_type: Some(2),
        },
        TxRow {
            block_num: 73_000_001,
            block_timestamp: ts(1),
            idx: 0,
            hash: vec![15; 32],
            tx_type: 0,
            from: vec![16; 20],
            to: None,
            value: "0".to_string(),
            input: vec![],
            gas_limit: 21_000,
            max_fee_per_gas: "1".to_string(),
            max_priority_fee_per_gas: "0".to_string(),
            gas_used: None,
            nonce_key: vec![0; 32],
            nonce: 0,
            fee_token: None,
            fee_payer: None,
            calls: None,
            call_count: 1,
            valid_before: None,
            valid_after: None,
            signature_type: None,
        },
    ];
    let logs = vec![
        LogRow {
            block_num: 73_000_000,
            block_timestamp: ts(0),
            log_idx: 0,
            tx_idx: 0,
            tx_hash: vec![9; 32],
            address: vec![17; 20],
            selector: Some(vec![18; 32]),
            topic0: Some(vec![18; 32]),
            topic1: Some(vec![19; 32]),
            topic2: Some(vec![20; 32]),
            topic3: Some(vec![21; 32]),
            data: vec![22; 64],
            is_virtual_forward: true,
        },
        LogRow {
            block_num: 73_000_000,
            block_timestamp: ts(0),
            log_idx: 1,
            tx_idx: 0,
            tx_hash: vec![9; 32],
            address: vec![23; 20],
            selector: None,
            topic0: None,
            topic1: None,
            topic2: None,
            topic3: None,
            data: vec![],
            is_virtual_forward: false,
        },
    ];
    let receipts = vec![
        ReceiptRow {
            block_num: 73_000_000,
            block_timestamp: ts(0),
            tx_idx: 0,
            tx_hash: vec![9; 32],
            from: vec![10; 20],
            to: Some(vec![11; 20]),
            contract_address: None,
            gas_used: 50_000,
            cumulative_gas_used: 50_000,
            effective_gas_price: Some("20000000000".to_string()),
            status: Some(1),
            fee_payer: Some(vec![14; 20]),
            ..Default::default()
        },
        ReceiptRow {
            block_num: 73_000_001,
            block_timestamp: ts(1),
            tx_idx: 0,
            tx_hash: vec![15; 32],
            from: vec![16; 20],
            to: None,
            contract_address: Some(vec![24; 20]),
            gas_used: 21_000,
            cumulative_gas_used: 21_000,
            effective_gas_price: None,
            status: None,
            fee_payer: None,
            ..Default::default()
        },
    ];

    write_batch(&db.pool, &blocks, &txs, &logs, &receipts)
        .await
        .unwrap();

    let conn = db.pool.get().await.unwrap();
    let stored_blocks: Vec<BlockRow> = conn
        .query("SELECT * FROM blocks ORDER BY num", &[])
        .await
        .unwrap()
        .iter()
        .map(|r| BlockRow {
            num: r.get("num"),
            hash: r.get("hash"),
            parent_hash: r.get("parent_hash"),
            timestamp: r.get("timestamp"),
            timestamp_ms: r.get("timestamp_ms"),
            gas_limit: r.get("gas_limit"),
            gas_used: r.get("gas_used"),
            miner: r.get("miner"),
            extra_data: r.get("extra_data"),
            consensus_proposer: r.get("consensus_proposer"),
        })
        .collect();
    assert_eq!(format!("{stored_blocks:?}"), format!("{blocks:?}"));

    let stored_txs: Vec<TxRow> = conn
        .query("SELECT * FROM txs ORDER BY block_num, idx", &[])
        .await
        .unwrap()
        .iter()
        .map(|r| TxRow {
            block_num: r.get("block_num"),
            block_timestamp: r.get("block_timestamp"),
            idx: r.get("idx"),
            hash: r.get("hash"),
            tx_type: r.get("type"),
            from: r.get("from"),
            to: r.get("to"),
            value: r.get("value"),
            input: r.get("input"),
            gas_limit: r.get("gas_limit"),
            max_fee_per_gas: r.get("max_fee_per_gas"),
            max_priority_fee_per_gas: r.get("max_priority_fee_per_gas"),
            gas_used: r.get("gas_used"),
            nonce_key: r.get("nonce_key"),
            nonce: r.get("nonce"),
            fee_token: r.get("fee_token"),
            fee_payer: r.get("fee_payer"),
            calls: r.get("calls"),
            call_count: r.get("call_count"),
            valid_before: r.get("valid_before"),
            valid_after: r.get("valid_after"),
            signature_type: r.get("signature_type"),
        })
        .collect();
    assert_eq!(format!("{stored_txs:?}"), format!("{txs:?}"));

    let stored_logs: Vec<LogRow> = conn
        .query("SELECT * FROM logs ORDER BY block_num, log_idx", &[])
        .await
        .unwrap()
        .iter()
        .map(|r| LogRow {
            block_num: r.get("block_num"),
            block_timestamp: r.get("block_timestamp"),
            log_idx: r.get("log_idx"),
            tx_idx: r.get("tx_idx"),
            tx_hash: r.get("tx_hash"),
            address: r.get("address"),
            selector: r.get("selector"),
            topic0: r.get("topic0"),
            topic1: r.get("topic1"),
            topic2: r.get("topic2"),
            topic3: r.get("topic3"),
            data: r.get("data"),
            is_virtual_forward: r.get("is_virtual_forward"),
        })
        .collect();
    assert_eq!(format!("{stored_logs:?}"), format!("{logs:?}"));

    let stored_receipts: Vec<ReceiptRow> = conn
        .query("SELECT * FROM receipts ORDER BY block_num, tx_idx", &[])
        .await
        .unwrap()
        .iter()
        .map(|r| ReceiptRow {
            block_num: r.get("block_num"),
            block_timestamp: r.get("block_timestamp"),
            tx_idx: r.get("tx_idx"),
            tx_hash: r.get("tx_hash"),
            from: r.get("from"),
            to: r.get("to"),
            contract_address: r.get("contract_address"),
            gas_used: r.get("gas_used"),
            cumulative_gas_used: r.get("cumulative_gas_used"),
            effective_gas_price: r.get("effective_gas_price"),
            status: r.get("status"),
            fee_payer: r.get("fee_payer"),
            ..Default::default()
        })
        .collect();
    assert_eq!(format!("{stored_receipts:?}"), format!("{receipts:?}"));

    // Only the block whose transaction lacks receipt data is queued for repair.
    let queued: Vec<(i64, chrono::DateTime<chrono::Utc>)> = conn
        .query(
            "SELECT block_num, block_timestamp FROM receipt_repair_queue ORDER BY block_num",
            &[],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    assert_eq!(queued, vec![(73_000_001, ts(1))]);
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
async fn test_delete_copy_preserves_non_contiguous_blocks() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    let blocks = generate_blocks(20, 73_000_000);
    write_blocks(&db.pool, &blocks).await.unwrap();

    let txs_first: Vec<_> = (0..20)
        .flat_map(|i| generate_txs(10, 73_000_000 + i))
        .collect();
    write_txs(&db.pool, &txs_first).await.unwrap();

    let txs_second: Vec<_> = [73_000_000, 73_000_010]
        .into_iter()
        .flat_map(|block_num| generate_txs(5, block_num))
        .collect();
    write_txs(&db.pool, &txs_second).await.unwrap();

    let conn = db.pool.get().await.unwrap();

    let count_middle: i64 = conn
        .query_one(
            "SELECT COUNT(*) FROM txs WHERE block_num > 73000000 AND block_num < 73000010",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        count_middle, 90,
        "blocks between sparse rewrites must be preserved"
    );

    let count_edges: i64 = conn
        .query_one(
            "SELECT COUNT(*) FROM txs WHERE block_num IN (73000000, 73000010)",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        count_edges, 10,
        "only explicitly rewritten blocks should be replaced"
    );
}

#[tokio::test]
#[serial(db)]
async fn test_pipelined_sync() {
    let tempo = TempoNode::from_env();
    tempo.wait_for_ready().await.expect("Tempo node not ready");

    let db = TestDb::empty().await;
    db.truncate_all().await;

    // Wait for some blocks
    tempo
        .wait_for_block(30)
        .await
        .expect("Block 30 not reached");

    let sinks = SinkSet::new(db.pool.clone());
    let mut engine = SyncEngine::new(
        ThrottledPool::from_pool(db.pool.clone()),
        sinks,
        &tempo.rpc_url,
    )
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
