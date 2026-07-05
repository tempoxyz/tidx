//! Integration tests for partition lifecycle maintenance: partitions ahead
//! of the head are pre-created, and partitions that leave the hot window are
//! sealed (CLUSTER + ANALYZE + FREEZE, recorded in partition_state) exactly
//! once.

mod common;

use common::testdb::TestDb;

use serial_test::serial;
use tidx::sync::maintenance::run_maintenance_pass;
use tidx::sync::writer::{save_sync_state, write_batch};
use tidx::types::{BlockRow, SyncState, TxRow};

const CHAIN_ID: u64 = 424_242;

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

fn make_tx(block_num: i64) -> TxRow {
    TxRow {
        block_num,
        block_timestamp: chrono::Utc::now(),
        idx: 0,
        hash: vec![(block_num % 256) as u8; 32],
        tx_type: 2,
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
        calls: Vec::new(),
        call_count: 1,
        valid_before: None,
        valid_after: None,
        signature_type: Some(0),
    }
}

fn make_sync_state(synced: u64, tip: u64) -> SyncState {
    SyncState {
        chain_id: CHAIN_ID,
        head_num: tip,
        synced_num: synced,
        tip_num: tip,
        backfill_num: Some(0),
        sync_rate: None,
        started_at: Some(chrono::Utc::now()),
    }
}

#[tokio::test]
#[serial(db)]
async fn test_seal_pass_seals_partitions_behind_hot_window() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    if !is_partitioned(&db.pool, "blocks").await {
        println!(
            "blocks is not partitioned (pre-partitioning test database) - skipping; \
             recreate the test database for the partitioned layout"
        );
        return;
    }

    // Data in p0, p1, and p2 (default width 1M).
    let blocks: Vec<BlockRow> = [500_000, 1_500_000, 2_500_000]
        .iter()
        .map(|&n| make_block(n))
        .collect();
    let txs: Vec<TxRow> = [500_000, 1_500_000, 2_500_000]
        .iter()
        .map(|&n| make_tx(n))
        .collect();
    write_batch(&db.pool, &blocks, &txs, &[], &[])
        .await
        .unwrap();

    // Synced through 2.6M; hot window 1M → seal bound = min(2.6M, 1.6M).
    // Only p0 (blocks 0..1M) lies fully below it.
    save_sync_state(&db.pool, &make_sync_state(2_600_000, 2_600_000))
        .await
        .unwrap();

    let sealed = run_maintenance_pass(&db.pool, CHAIN_ID, 1_000_000)
        .await
        .expect("maintenance pass failed");

    assert!(
        sealed.contains(&("blocks".to_string(), 0)),
        "blocks_p0 must be sealed, got {sealed:?}"
    );
    assert!(
        sealed.contains(&("txs".to_string(), 0)),
        "txs_p0 must be sealed, got {sealed:?}"
    );
    assert!(
        !sealed.iter().any(|(_, idx)| *idx > 0),
        "only p0 may be sealed (p1 is inside the hot window), got {sealed:?}"
    );

    // Seal state recorded; data intact.
    let conn = db.pool.get().await.unwrap();
    let recorded: i64 = conn
        .query_one(
            "SELECT COUNT(*) FROM partition_state WHERE table_name = 'blocks' AND partition_idx = 0",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(recorded, 1);

    let block_count: i64 = conn
        .query_one("SELECT COUNT(*) FROM blocks", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(block_count, 3, "sealing must not lose rows");
    drop(conn);

    // Sealing is one-shot: a second pass has nothing left to do.
    let resealed = run_maintenance_pass(&db.pool, CHAIN_ID, 1_000_000)
        .await
        .unwrap();
    assert!(
        resealed.is_empty(),
        "second pass must not re-seal, got {resealed:?}"
    );
}

#[tokio::test]
#[serial(db)]
async fn test_maintenance_precreates_partitions_ahead_of_head() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    if !is_partitioned(&db.pool, "blocks").await {
        println!("blocks is not partitioned - skipping");
        return;
    }

    // Head just below the p0/p1 boundary; nothing sealable (hot window
    // covers everything).
    save_sync_state(&db.pool, &make_sync_state(999_990, 999_990))
        .await
        .unwrap();

    let sealed = run_maintenance_pass(&db.pool, CHAIN_ID, 2_000_000)
        .await
        .unwrap();
    assert!(sealed.is_empty(), "nothing may be sealed, got {sealed:?}");

    // The upcoming partition (p1) exists before any write needs it.
    let conn = db.pool.get().await.unwrap();
    for table in ["blocks", "txs", "tx_calls", "logs", "receipts"] {
        let exists: bool = conn
            .query_one(
                "SELECT to_regclass($1) IS NOT NULL",
                &[&format!("{table}_p1")],
            )
            .await
            .unwrap()
            .get(0);
        assert!(exists, "{table}_p1 must be pre-created ahead of the head");
    }
}

#[tokio::test]
#[serial(db)]
async fn test_seal_pass_respects_incomplete_backfill() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    if !is_partitioned(&db.pool, "blocks").await {
        println!("blocks is not partitioned - skipping");
        return;
    }

    write_batch(&db.pool, &[make_block(500_000)], &[], &[], &[])
        .await
        .unwrap();

    // Tip far ahead but synced_num low (backfill incomplete): gap-fill may
    // still write below 100k, so nothing seals even though the hot window
    // would allow it.
    save_sync_state(&db.pool, &make_sync_state(100_000, 5_000_000))
        .await
        .unwrap();

    let sealed = run_maintenance_pass(&db.pool, CHAIN_ID, 1_000_000)
        .await
        .unwrap();
    assert!(
        sealed.is_empty(),
        "partitions above synced_num must not seal, got {sealed:?}"
    );
}
