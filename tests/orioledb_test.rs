//! Integration tests for the OrioleDB storage layout: when the orioledb
//! extension is installed, new chain-table partitions use the orioledb
//! access method with zstd compression, the writer works against them
//! unchanged, and sealing takes the ANALYZE-only path (no CLUSTER/VACUUM,
//! which orioledb doesn't support).
//!
//! Skipped on stock PostgreSQL (no orioledb extension), where partitions
//! fall back to heap — covered by partition_test.rs.

mod common;

use common::testdb::TestDb;

use serial_test::serial;
use tidx::sync::maintenance::run_maintenance_pass;
use tidx::sync::writer::{save_sync_state, write_batch};
use tidx::types::{BlockRow, SyncState};

const CHAIN_ID: u64 = 434_343;

async fn orioledb_installed(pool: &tidx::db::Pool) -> bool {
    let conn = pool.get().await.unwrap();
    conn.query_one(
        "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'orioledb')",
        &[],
    )
    .await
    .unwrap()
    .get(0)
}

async fn partition_am(
    pool: &tidx::db::Pool,
    partition: &str,
) -> Option<(String, Option<Vec<String>>)> {
    let conn = pool.get().await.unwrap();
    conn.query_opt(
        "SELECT am.amname, c.reloptions FROM pg_class c
         JOIN pg_am am ON am.oid = c.relam
         WHERE c.oid = to_regclass($1::text)",
        &[&partition],
    )
    .await
    .unwrap()
    .map(|r| (r.get(0), r.get(1)))
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
        extra_data: Some(vec![0xab; 64]),
        consensus_proposer: None,
    }
}

#[tokio::test]
#[serial(db)]
async fn test_partitions_use_orioledb_with_compression() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    if !orioledb_installed(&db.pool).await {
        println!("orioledb extension not installed - skipping (stock PostgreSQL)");
        return;
    }

    // A write at a fresh block range creates its partition through
    // ensure_block_partitions, which must pick orioledb + compression.
    write_batch(&db.pool, &[make_block(40_000_000)], &[], &[], &[])
        .await
        .expect("write into orioledb partition failed");

    let (am, options) = partition_am(&db.pool, "blocks_p40")
        .await
        .expect("blocks_p40 must exist");
    assert_eq!(am, "orioledb", "new partitions must use the orioledb AM");
    let options = options.unwrap_or_default();
    assert!(
        options.iter().any(|o| o == "compress=6"),
        "partition must be zstd-compressed, got {options:?}"
    );
    assert!(
        options.iter().any(|o| o == "toast_compress=12"),
        "partition must compress TOAST, got {options:?}"
    );

    // Rows are readable back through the parent.
    let conn = db.pool.get().await.unwrap();
    let count: i64 = conn
        .query_one("SELECT COUNT(*) FROM blocks WHERE num = 40000000", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 1);
}

/// Upstream correctness gate: DELETE + reinsert of the same primary keys in
/// one transaction (tidx's block-rewrite path: replace_txs/replace_logs/
/// replace_receipts, exercised by reorgs and overlapping gap-fill) must leave
/// the rows visible to sequential scans.
///
/// OrioleDB beta 16 FAILS this: the reinserted rows are visible through
/// index scans but invisible to sequential scans of the partition. Until an
/// OrioleDB release passes this test, the docker images must stay on stock
/// postgres — the capability-gated schema keeps everything on heap there.
#[tokio::test]
#[serial(db)]
async fn test_rewrite_rows_stay_visible_to_seq_scans() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    if !orioledb_installed(&db.pool).await {
        println!("orioledb extension not installed - skipping (stock PostgreSQL)");
        return;
    }

    let logs: Vec<tidx::types::LogRow> = (0..50)
        .map(|i| tidx::types::LogRow {
            block_num: 47_000_000,
            block_timestamp: chrono::Utc::now(),
            log_idx: i,
            tx_idx: 0,
            tx_hash: vec![3; 32],
            address: vec![4; 20],
            data: vec![0; 8],
            ..Default::default()
        })
        .collect();

    tidx::sync::writer::write_logs(&db.pool, &logs)
        .await
        .unwrap();
    // Rewrite the same rows: DELETE + reinsert of identical PKs in one txn
    // (replace_logs), as happens for any re-synced block.
    tidx::sync::writer::write_logs(&db.pool, &logs)
        .await
        .unwrap();

    let conn = db.pool.get().await.unwrap();
    let seq: i64 = conn
        .query_one("SELECT COUNT(*) FROM logs_p47", &[])
        .await
        .unwrap()
        .get(0);
    conn.batch_execute("SET enable_seqscan = off")
        .await
        .unwrap();
    let idx: i64 = conn
        .query_one("SELECT COUNT(*) FROM logs_p47 WHERE block_num >= 0", &[])
        .await
        .unwrap()
        .get(0);
    conn.batch_execute("RESET enable_seqscan").await.unwrap();

    assert_eq!(
        (seq, idx),
        (50, 50),
        "rows rewritten via DELETE + reinsert must stay visible to both \
         sequential and index scans (seq={seq}, idx={idx}); OrioleDB beta 16 \
         loses them from sequential scans"
    );
}

#[tokio::test]
#[serial(db)]
async fn test_seal_takes_analyze_only_path_on_orioledb() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    if !orioledb_installed(&db.pool).await {
        println!("orioledb extension not installed - skipping (stock PostgreSQL)");
        return;
    }

    write_batch(&db.pool, &[make_block(500_000)], &[], &[], &[])
        .await
        .unwrap();

    save_sync_state(
        &db.pool,
        &SyncState {
            chain_id: CHAIN_ID,
            head_num: 2_600_000,
            synced_num: 2_600_000,
            tip_num: 2_600_000,
            backfill_num: Some(0),
            sync_rate: None,
            started_at: Some(chrono::Utc::now()),
        },
    )
    .await
    .unwrap();

    // Sealing an orioledb partition must succeed without CLUSTER/VACUUM
    // (both unsupported by the AM) and be recorded.
    let sealed = run_maintenance_pass(&db.pool, CHAIN_ID, 1_000_000)
        .await
        .expect("seal pass on orioledb partition failed");
    assert!(
        sealed.contains(&("blocks".to_string(), 0)),
        "blocks_p0 must seal on orioledb, got {sealed:?}"
    );

    let conn = db.pool.get().await.unwrap();
    let count: i64 = conn
        .query_one("SELECT COUNT(*) FROM blocks", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 1, "sealing must not lose rows");
}
