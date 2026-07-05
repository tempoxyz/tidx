//! Integration tests for the normalized `tx_calls` table: multicall AA txs
//! persist their inner calls as typed rows (replacing the old `txs.calls`
//! JSONB column), single-call AA and non-AA txs don't, and reorg deletes
//! cascade.

mod common;

use common::testdb::TestDb;

use serial_test::serial;
use tempo_alloy::primitives::transaction::{Call, TEMPO_TX_TYPE_ID};
use tidx::sync::writer::{delete_blocks_from, write_batch, write_txs};
use tidx::types::TxRow;

use alloy::primitives::{Address, TxKind, U256};

const BLOCK_NUM: i64 = 20_000_000;

fn make_call(to: Option<[u8; 20]>, value: u64, input: &[u8]) -> Call {
    Call {
        to: match to {
            Some(bytes) => TxKind::Call(Address::from(bytes)),
            None => TxKind::Create,
        },
        value: U256::from(value),
        input: input.to_vec().into(),
    }
}

fn make_tx(idx: i32, tx_type: i16, calls: Vec<Call>) -> TxRow {
    TxRow {
        block_num: BLOCK_NUM,
        block_timestamp: chrono::Utc::now(),
        idx,
        hash: vec![idx as u8; 32],
        tx_type,
        from: vec![1u8; 20],
        to: Some(vec![2u8; 20]),
        value: "0".to_string(),
        input: vec![0u8; 4],
        gas_limit: 21000,
        max_fee_per_gas: "1000000000".to_string(),
        max_priority_fee_per_gas: "0".to_string(),
        gas_used: Some(21000),
        nonce_key: vec![0u8; 32],
        nonce: idx as i64,
        fee_token: None,
        fee_payer: None,
        call_count: calls.len().max(1) as i16,
        calls,
        valid_before: None,
        valid_after: None,
        signature_type: Some(0),
    }
}

/// One non-AA tx, one single-call AA tx, one 3-call AA multicall.
fn sample_txs() -> Vec<TxRow> {
    let recipient = [0xaa; 20];
    vec![
        make_tx(0, 2, Vec::new()),
        make_tx(
            1,
            TEMPO_TX_TYPE_ID as i16,
            vec![make_call(Some([0x11; 20]), 5, &[0x01])],
        ),
        make_tx(
            2,
            TEMPO_TX_TYPE_ID as i16,
            vec![
                make_call(Some(recipient), 100, &[0xde, 0xad]),
                make_call(None, 0, &[0x60, 0x80]),
                make_call(Some([0xbb; 20]), 7, &[]),
            ],
        ),
    ]
}

#[tokio::test]
#[serial(db)]
async fn test_multicall_txs_normalize_into_tx_calls() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    write_txs(&db.pool, &sample_txs()).await.unwrap();

    let conn = db.pool.get().await.unwrap();

    // Only the 3-call multicall produces rows; single-call AA and non-AA don't.
    let rows = conn
        .query(
            "SELECT tx_idx, call_idx, \"to\", value, input FROM tx_calls \
             WHERE block_num = $1 ORDER BY tx_idx, call_idx",
            &[&BLOCK_NUM],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 3, "expected one row per inner call");

    for (i, row) in rows.iter().enumerate() {
        assert_eq!(row.get::<_, i32>(0), 2, "all rows belong to the multicall");
        assert_eq!(row.get::<_, i16>(1), i as i16, "call_idx preserves order");
    }

    assert_eq!(rows[0].get::<_, Option<Vec<u8>>>(2), Some(vec![0xaa; 20]));
    assert_eq!(rows[0].get::<_, String>(3), "100");
    assert_eq!(rows[0].get::<_, Vec<u8>>(4), vec![0xde, 0xad]);

    // Contract-creation inner call stores NULL `to`.
    assert_eq!(rows[1].get::<_, Option<Vec<u8>>>(2), None);
    assert_eq!(rows[1].get::<_, String>(3), "0");

    assert_eq!(rows[2].get::<_, Option<Vec<u8>>>(2), Some(vec![0xbb; 20]));
    assert_eq!(rows[2].get::<_, Vec<u8>>(4), Vec::<u8>::new());
}

#[tokio::test]
#[serial(db)]
async fn test_inner_call_recipient_filter_via_exists() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    write_txs(&db.pool, &sample_txs()).await.unwrap();

    let conn = db.pool.get().await.unwrap();

    // The tempo-api2 `includeCallRecipients` replacement: root `to` OR an
    // inner call targeting the recipient.
    let recipient = vec![0xaau8; 20];
    let rows = conn
        .query(
            "SELECT idx FROM txs \
             WHERE (\"to\" = $1 OR EXISTS ( \
                 SELECT 1 FROM tx_calls c \
                 WHERE c.block_timestamp = txs.block_timestamp \
                   AND c.block_num = txs.block_num \
                   AND c.tx_idx = txs.idx \
                   AND c.\"to\" = $1)) \
               AND block_num = $2 \
             ORDER BY idx",
            &[&recipient, &BLOCK_NUM],
        )
        .await
        .unwrap();

    let matched: Vec<i32> = rows.iter().map(|r| r.get(0)).collect();
    assert_eq!(
        matched,
        vec![2],
        "only the multicall with an inner call to 0xaa… should match"
    );
}

/// Recreate a pre-normalization schema (txs.calls JSONB + GIN index) with
/// legacy-format rows, then verify the post-startup migration backfills
/// multicall rows into tx_calls and drops the column.
#[tokio::test]
#[serial(db)]
async fn test_legacy_jsonb_migration_backfills_tx_calls() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    let conn = db.pool.get().await.unwrap();

    // Restore the legacy schema.
    // (The legacy GIN index is not recreated: on orioledb partitions GIN
    // would go through the experimental bridge index, and the migration's
    // DROP INDEX IF EXISTS is trivially covered either way.)
    conn.batch_execute("ALTER TABLE txs ADD COLUMN calls JSONB")
        .await
        .unwrap();

    // Legacy rows, JSON exactly as old tidx wrote it (serde of tempo Call:
    // hex-quantity value, data:null). The multicall exercises odd-length
    // quantity hex ("0x5"), zero ("0x0"), and a contract-creation call.
    // (A real legacy txs table is regular; on the partitioned test layout the
    // raw INSERT needs its partition first — a no-op on regular tables.)
    conn.batch_execute(
        r#"
        SELECT ensure_block_partitions('txs', 30000000, 30000000);
        INSERT INTO txs (block_num, block_timestamp, idx, hash, type, "from", "to", value, input,
                         gas_limit, max_fee_per_gas, max_priority_fee_per_gas,
                         nonce_key, nonce, calls, call_count)
        VALUES
        (30000000, NOW(), 0, '\x00'::bytea, 2, '\x01'::bytea, '\x02'::bytea, '0', '\x'::bytea,
         21000, '0', '0', '\x00'::bytea, 0, NULL, 1),
        (30000000, NOW(), 1, '\x01'::bytea, 118, '\x01'::bytea, '\x11'::bytea, '5', '\x01'::bytea,
         21000, '0', '0', '\x00'::bytea, 1,
         '[{"to":"0x1111111111111111111111111111111111111111","value":"0x5","input":"0x01","data":null}]'::jsonb,
         1),
        (30000000, NOW(), 2, '\x02'::bytea, 118, '\x01'::bytea, '\xaa'::bytea, '105', '\xdead'::bytea,
         21000, '0', '0', '\x00'::bytea, 2,
         '[{"to":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","value":"0x64","input":"0xdead","data":null},
           {"to":null,"value":"0x0","input":"0x6080","data":null},
           {"to":"0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","value":"0x5","input":"0x","data":null}]'::jsonb,
         3)
        "#,
    )
    .await
    .unwrap();
    drop(conn);

    // Run the migration (idempotent: run it twice).
    tidx::db::run_post_startup_migrations(&db.pool)
        .await
        .unwrap();
    tidx::db::run_post_startup_migrations(&db.pool)
        .await
        .unwrap();

    let conn = db.pool.get().await.unwrap();

    // Only the multicall was backfilled, with converted values.
    let rows = conn
        .query(
            "SELECT tx_idx, call_idx, \"to\", value, input FROM tx_calls \
             WHERE block_num = 30000000 ORDER BY tx_idx, call_idx",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 3, "only the 3-call multicall is backfilled");

    assert_eq!(rows[0].get::<_, i32>(0), 2);
    assert_eq!(rows[0].get::<_, Option<Vec<u8>>>(2), Some(vec![0xaa; 20]));
    assert_eq!(rows[0].get::<_, String>(3), "100", "0x64 → decimal");
    assert_eq!(rows[0].get::<_, Vec<u8>>(4), vec![0xde, 0xad]);

    assert_eq!(rows[1].get::<_, Option<Vec<u8>>>(2), None, "create → NULL");
    assert_eq!(rows[1].get::<_, String>(3), "0", "0x0 → decimal");
    assert_eq!(rows[1].get::<_, Vec<u8>>(4), vec![0x60, 0x80]);

    assert_eq!(rows[2].get::<_, String>(3), "5", "odd-length 0x5 → decimal");
    assert_eq!(rows[2].get::<_, Vec<u8>>(4), Vec::<u8>::new());

    // The column and its GIN index are gone.
    let has_calls: bool = conn
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM information_schema.columns \
             WHERE table_name = 'txs' AND column_name = 'calls')",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert!(!has_calls, "txs.calls must be dropped after backfill");
}

#[tokio::test]
#[serial(db)]
async fn test_rewrite_and_reorg_delete_tx_calls() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    let txs = sample_txs();

    // Writing the same batch twice must not duplicate rows (delete + reinsert).
    write_txs(&db.pool, &txs).await.unwrap();
    write_txs(&db.pool, &txs).await.unwrap();

    let conn = db.pool.get().await.unwrap();
    let count: i64 = conn
        .query_one("SELECT COUNT(*) FROM tx_calls", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 3, "rewrite must not duplicate tx_calls rows");
    drop(conn);

    // write_batch covers the same path as realtime sync.
    let mut batch_txs = txs.clone();
    for tx in &mut batch_txs {
        tx.block_num = BLOCK_NUM + 1;
    }
    write_batch(&db.pool, &[], &batch_txs, &[], &[])
        .await
        .unwrap();

    let conn = db.pool.get().await.unwrap();
    let count: i64 = conn
        .query_one("SELECT COUNT(*) FROM tx_calls", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 6, "write_batch must also populate tx_calls");
    drop(conn);

    // Reorg delete cascades to tx_calls.
    delete_blocks_from(&db.pool, BLOCK_NUM as u64)
        .await
        .unwrap();

    let conn = db.pool.get().await.unwrap();
    let count: i64 = conn
        .query_one("SELECT COUNT(*) FROM tx_calls", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 0, "reorg delete must remove tx_calls rows");
}
