use anyhow::Result;
use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use std::collections::{BTreeMap, BTreeSet};
use std::task::Poll;
use std::time::Instant;
use tokio_postgres::types::{ToSql, Type};

use crate::db::Pool;
use crate::metrics;
use crate::types::{BlockRow, LogRow, ReceiptRow, SyncState, TxRow};

/// Margin around a batch's timestamps when deleting the rows it replaces.
///
/// Stored rows of a block carry the timestamp of the version of that block that
/// was written, which differs from the batch's only if a reorg replaced the
/// block, and then by seconds. The bound lets PostgreSQL prune the delete to the
/// weekly partitions around the batch instead of planning and scanning all of
/// them.
const DELETE_TIMESTAMP_MARGIN: chrono::TimeDelta = chrono::TimeDelta::days(1);

/// Block numbers and time span of the rows in a batch.
struct BlockSpan {
    block_nums: Vec<i64>,
    min_timestamp: DateTime<Utc>,
    max_timestamp: DateTime<Utc>,
}

impl BlockSpan {
    fn new(rows: impl Iterator<Item = (i64, DateTime<Utc>)>) -> Option<Self> {
        let mut block_nums = BTreeSet::new();
        let mut span: Option<(DateTime<Utc>, DateTime<Utc>)> = None;
        for (block_num, timestamp) in rows {
            block_nums.insert(block_num);
            span = Some(span.map_or((timestamp, timestamp), |(min, max)| {
                (min.min(timestamp), max.max(timestamp))
            }));
        }
        let (min_timestamp, max_timestamp) = span?;
        Some(Self {
            block_nums: block_nums.into_iter().collect(),
            min_timestamp: min_timestamp - DELETE_TIMESTAMP_MARGIN,
            max_timestamp: max_timestamp + DELETE_TIMESTAMP_MARGIN,
        })
    }
}

type Statement<'a> = BoxFuture<'a, Result<u64, tokio_postgres::Error>>;

fn execute<'a>(
    tx: &'a tokio_postgres::Transaction<'_>,
    sql: &'a str,
    params: Vec<(&'a (dyn ToSql + Sync), Type)>,
) -> Statement<'a> {
    Box::pin(async move { tx.execute_typed(sql, &params).await })
}

/// Sends `statements` in order without waiting for each other's results, then
/// waits for all of them. A statement is sent on its first poll, and
/// PostgreSQL executes and answers statements in the order they were sent.
async fn pipeline(statements: Vec<Statement<'_>>) -> Result<()> {
    let mut sent = Vec::with_capacity(statements.len());
    for mut statement in statements {
        if let Poll::Ready(result) = futures::poll!(statement.as_mut()) {
            result?;
        } else {
            sent.push(statement);
        }
    }
    for statement in sent {
        statement.await?;
    }
    Ok(())
}

/// Delete the rows of `table` stored for the blocks in `span`.
fn delete_blocks_exact<'a>(
    tx: &'a tokio_postgres::Transaction<'_>,
    sql: &'a str,
    span: &'a BlockSpan,
) -> Statement<'a> {
    execute(
        tx,
        sql,
        vec![
            (&span.block_nums, Type::INT8_ARRAY),
            (&span.min_timestamp, Type::TIMESTAMPTZ),
            (&span.max_timestamp, Type::TIMESTAMPTZ),
        ],
    )
}

const DELETE_TXS: &str = "DELETE FROM txs WHERE block_num = ANY($1) \
     AND block_timestamp >= $2 AND block_timestamp <= $3";
const DELETE_LOGS: &str = "DELETE FROM logs WHERE block_num = ANY($1) \
     AND block_timestamp >= $2 AND block_timestamp <= $3";
const DELETE_RECEIPTS: &str = "DELETE FROM receipts WHERE block_num = ANY($1) \
     AND block_timestamp >= $2 AND block_timestamp <= $3";

const INSERT_BLOCKS: &str = "INSERT INTO blocks (num, hash, parent_hash, timestamp, timestamp_ms, \
     gas_limit, gas_used, miner, extra_data, consensus_proposer) \
     SELECT * FROM unnest($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) \
     ON CONFLICT (timestamp, num) DO NOTHING";

const INSERT_TXS: &str = r#"INSERT INTO txs (block_num, block_timestamp, idx, hash, type, "from",
     "to", value, input, gas_limit, max_fee_per_gas, max_priority_fee_per_gas, gas_used,
     nonce_key, nonce, fee_token, fee_payer, calls, call_count, valid_before, valid_after,
     signature_type)
     SELECT * FROM unnest($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15,
     $16, $17, $18, $19, $20, $21, $22)
     ON CONFLICT DO NOTHING"#;

const INSERT_LOGS: &str = "INSERT INTO logs (block_num, block_timestamp, log_idx, tx_idx, \
     tx_hash, address, selector, topic0, topic1, topic2, topic3, data, is_virtual_forward) \
     SELECT * FROM unnest($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13) \
     ON CONFLICT DO NOTHING";

const INSERT_RECEIPTS: &str = r#"INSERT INTO receipts (block_num, block_timestamp, tx_idx,
     tx_hash, "from", "to", contract_address, gas_used, cumulative_gas_used,
     effective_gas_price, status, fee_payer)
     SELECT * FROM unnest($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
     ON CONFLICT DO NOTHING"#;

/// Replace the repair-queue state of the batch's blocks in the same
/// transaction as their transactions, so a crash cannot leave new incomplete
/// transactions unqueued.
const DELETE_REPAIR_QUEUE: &str = "DELETE FROM receipt_repair_queue WHERE block_num = ANY($1)";
const INSERT_REPAIR_QUEUE: &str = "INSERT INTO receipt_repair_queue (block_num, block_timestamp) \
     SELECT * FROM unnest($1, $2)";

struct BlockColumns<'a> {
    num: Vec<i64>,
    hash: Vec<&'a [u8]>,
    parent_hash: Vec<&'a [u8]>,
    timestamp: Vec<DateTime<Utc>>,
    timestamp_ms: Vec<i64>,
    gas_limit: Vec<i64>,
    gas_used: Vec<i64>,
    miner: Vec<&'a [u8]>,
    extra_data: Vec<Option<&'a [u8]>>,
    consensus_proposer: Vec<Option<&'a [u8]>>,
}

impl<'a> BlockColumns<'a> {
    fn new(rows: &'a [BlockRow]) -> Self {
        Self {
            num: rows.iter().map(|r| r.num).collect(),
            hash: rows.iter().map(|r| r.hash.as_slice()).collect(),
            parent_hash: rows.iter().map(|r| r.parent_hash.as_slice()).collect(),
            timestamp: rows.iter().map(|r| r.timestamp).collect(),
            timestamp_ms: rows.iter().map(|r| r.timestamp_ms).collect(),
            gas_limit: rows.iter().map(|r| r.gas_limit).collect(),
            gas_used: rows.iter().map(|r| r.gas_used).collect(),
            miner: rows.iter().map(|r| r.miner.as_slice()).collect(),
            extra_data: rows.iter().map(|r| r.extra_data.as_deref()).collect(),
            consensus_proposer: rows
                .iter()
                .map(|r| r.consensus_proposer.as_deref())
                .collect(),
        }
    }

    fn params(&self) -> Vec<(&(dyn ToSql + Sync), Type)> {
        vec![
            (&self.num, Type::INT8_ARRAY),
            (&self.hash, Type::BYTEA_ARRAY),
            (&self.parent_hash, Type::BYTEA_ARRAY),
            (&self.timestamp, Type::TIMESTAMPTZ_ARRAY),
            (&self.timestamp_ms, Type::INT8_ARRAY),
            (&self.gas_limit, Type::INT8_ARRAY),
            (&self.gas_used, Type::INT8_ARRAY),
            (&self.miner, Type::BYTEA_ARRAY),
            (&self.extra_data, Type::BYTEA_ARRAY),
            (&self.consensus_proposer, Type::BYTEA_ARRAY),
        ]
    }
}

struct TxColumns<'a> {
    block_num: Vec<i64>,
    block_timestamp: Vec<DateTime<Utc>>,
    idx: Vec<i32>,
    hash: Vec<&'a [u8]>,
    tx_type: Vec<i16>,
    from: Vec<&'a [u8]>,
    to: Vec<Option<&'a [u8]>>,
    value: Vec<&'a str>,
    input: Vec<&'a [u8]>,
    gas_limit: Vec<i64>,
    max_fee_per_gas: Vec<&'a str>,
    max_priority_fee_per_gas: Vec<&'a str>,
    gas_used: Vec<Option<i64>>,
    nonce_key: Vec<&'a [u8]>,
    nonce: Vec<i64>,
    fee_token: Vec<Option<&'a [u8]>>,
    fee_payer: Vec<Option<&'a [u8]>>,
    calls: Vec<Option<&'a serde_json::Value>>,
    call_count: Vec<i16>,
    valid_before: Vec<Option<i64>>,
    valid_after: Vec<Option<i64>>,
    signature_type: Vec<Option<i16>>,
}

impl<'a> TxColumns<'a> {
    fn new(rows: &'a [TxRow]) -> Self {
        Self {
            block_num: rows.iter().map(|r| r.block_num).collect(),
            block_timestamp: rows.iter().map(|r| r.block_timestamp).collect(),
            idx: rows.iter().map(|r| r.idx).collect(),
            hash: rows.iter().map(|r| r.hash.as_slice()).collect(),
            tx_type: rows.iter().map(|r| r.tx_type).collect(),
            from: rows.iter().map(|r| r.from.as_slice()).collect(),
            to: rows.iter().map(|r| r.to.as_deref()).collect(),
            value: rows.iter().map(|r| r.value.as_str()).collect(),
            input: rows.iter().map(|r| r.input.as_slice()).collect(),
            gas_limit: rows.iter().map(|r| r.gas_limit).collect(),
            max_fee_per_gas: rows.iter().map(|r| r.max_fee_per_gas.as_str()).collect(),
            max_priority_fee_per_gas: rows
                .iter()
                .map(|r| r.max_priority_fee_per_gas.as_str())
                .collect(),
            gas_used: rows.iter().map(|r| r.gas_used).collect(),
            nonce_key: rows.iter().map(|r| r.nonce_key.as_slice()).collect(),
            nonce: rows.iter().map(|r| r.nonce).collect(),
            fee_token: rows.iter().map(|r| r.fee_token.as_deref()).collect(),
            fee_payer: rows.iter().map(|r| r.fee_payer.as_deref()).collect(),
            calls: rows.iter().map(|r| r.calls.as_ref()).collect(),
            call_count: rows.iter().map(|r| r.call_count).collect(),
            valid_before: rows.iter().map(|r| r.valid_before).collect(),
            valid_after: rows.iter().map(|r| r.valid_after).collect(),
            signature_type: rows.iter().map(|r| r.signature_type).collect(),
        }
    }

    fn params(&self) -> Vec<(&(dyn ToSql + Sync), Type)> {
        vec![
            (&self.block_num, Type::INT8_ARRAY),
            (&self.block_timestamp, Type::TIMESTAMPTZ_ARRAY),
            (&self.idx, Type::INT4_ARRAY),
            (&self.hash, Type::BYTEA_ARRAY),
            (&self.tx_type, Type::INT2_ARRAY),
            (&self.from, Type::BYTEA_ARRAY),
            (&self.to, Type::BYTEA_ARRAY),
            (&self.value, Type::TEXT_ARRAY),
            (&self.input, Type::BYTEA_ARRAY),
            (&self.gas_limit, Type::INT8_ARRAY),
            (&self.max_fee_per_gas, Type::TEXT_ARRAY),
            (&self.max_priority_fee_per_gas, Type::TEXT_ARRAY),
            (&self.gas_used, Type::INT8_ARRAY),
            (&self.nonce_key, Type::BYTEA_ARRAY),
            (&self.nonce, Type::INT8_ARRAY),
            (&self.fee_token, Type::BYTEA_ARRAY),
            (&self.fee_payer, Type::BYTEA_ARRAY),
            (&self.calls, Type::JSONB_ARRAY),
            (&self.call_count, Type::INT2_ARRAY),
            (&self.valid_before, Type::INT8_ARRAY),
            (&self.valid_after, Type::INT8_ARRAY),
            (&self.signature_type, Type::INT2_ARRAY),
        ]
    }
}

struct LogColumns<'a> {
    block_num: Vec<i64>,
    block_timestamp: Vec<DateTime<Utc>>,
    log_idx: Vec<i32>,
    tx_idx: Vec<i32>,
    tx_hash: Vec<&'a [u8]>,
    address: Vec<&'a [u8]>,
    selector: Vec<Option<&'a [u8]>>,
    topic0: Vec<Option<&'a [u8]>>,
    topic1: Vec<Option<&'a [u8]>>,
    topic2: Vec<Option<&'a [u8]>>,
    topic3: Vec<Option<&'a [u8]>>,
    data: Vec<&'a [u8]>,
    is_virtual_forward: Vec<bool>,
}

impl<'a> LogColumns<'a> {
    fn new(rows: &'a [LogRow]) -> Self {
        Self {
            block_num: rows.iter().map(|r| r.block_num).collect(),
            block_timestamp: rows.iter().map(|r| r.block_timestamp).collect(),
            log_idx: rows.iter().map(|r| r.log_idx).collect(),
            tx_idx: rows.iter().map(|r| r.tx_idx).collect(),
            tx_hash: rows.iter().map(|r| r.tx_hash.as_slice()).collect(),
            address: rows.iter().map(|r| r.address.as_slice()).collect(),
            selector: rows.iter().map(|r| r.selector.as_deref()).collect(),
            topic0: rows.iter().map(|r| r.topic0.as_deref()).collect(),
            topic1: rows.iter().map(|r| r.topic1.as_deref()).collect(),
            topic2: rows.iter().map(|r| r.topic2.as_deref()).collect(),
            topic3: rows.iter().map(|r| r.topic3.as_deref()).collect(),
            data: rows.iter().map(|r| r.data.as_slice()).collect(),
            is_virtual_forward: rows.iter().map(|r| r.is_virtual_forward).collect(),
        }
    }

    fn params(&self) -> Vec<(&(dyn ToSql + Sync), Type)> {
        vec![
            (&self.block_num, Type::INT8_ARRAY),
            (&self.block_timestamp, Type::TIMESTAMPTZ_ARRAY),
            (&self.log_idx, Type::INT4_ARRAY),
            (&self.tx_idx, Type::INT4_ARRAY),
            (&self.tx_hash, Type::BYTEA_ARRAY),
            (&self.address, Type::BYTEA_ARRAY),
            (&self.selector, Type::BYTEA_ARRAY),
            (&self.topic0, Type::BYTEA_ARRAY),
            (&self.topic1, Type::BYTEA_ARRAY),
            (&self.topic2, Type::BYTEA_ARRAY),
            (&self.topic3, Type::BYTEA_ARRAY),
            (&self.data, Type::BYTEA_ARRAY),
            (&self.is_virtual_forward, Type::BOOL_ARRAY),
        ]
    }
}

struct ReceiptColumns<'a> {
    block_num: Vec<i64>,
    block_timestamp: Vec<DateTime<Utc>>,
    tx_idx: Vec<i32>,
    tx_hash: Vec<&'a [u8]>,
    from: Vec<&'a [u8]>,
    to: Vec<Option<&'a [u8]>>,
    contract_address: Vec<Option<&'a [u8]>>,
    gas_used: Vec<i64>,
    cumulative_gas_used: Vec<i64>,
    effective_gas_price: Vec<Option<&'a str>>,
    status: Vec<Option<i16>>,
    fee_payer: Vec<Option<&'a [u8]>>,
}

impl<'a> ReceiptColumns<'a> {
    fn new(rows: &'a [ReceiptRow]) -> Self {
        Self {
            block_num: rows.iter().map(|r| r.block_num).collect(),
            block_timestamp: rows.iter().map(|r| r.block_timestamp).collect(),
            tx_idx: rows.iter().map(|r| r.tx_idx).collect(),
            tx_hash: rows.iter().map(|r| r.tx_hash.as_slice()).collect(),
            from: rows.iter().map(|r| r.from.as_slice()).collect(),
            to: rows.iter().map(|r| r.to.as_deref()).collect(),
            contract_address: rows.iter().map(|r| r.contract_address.as_deref()).collect(),
            gas_used: rows.iter().map(|r| r.gas_used).collect(),
            cumulative_gas_used: rows.iter().map(|r| r.cumulative_gas_used).collect(),
            effective_gas_price: rows
                .iter()
                .map(|r| r.effective_gas_price.as_deref())
                .collect(),
            status: rows.iter().map(|r| r.status).collect(),
            fee_payer: rows.iter().map(|r| r.fee_payer.as_deref()).collect(),
        }
    }

    fn params(&self) -> Vec<(&(dyn ToSql + Sync), Type)> {
        vec![
            (&self.block_num, Type::INT8_ARRAY),
            (&self.block_timestamp, Type::TIMESTAMPTZ_ARRAY),
            (&self.tx_idx, Type::INT4_ARRAY),
            (&self.tx_hash, Type::BYTEA_ARRAY),
            (&self.from, Type::BYTEA_ARRAY),
            (&self.to, Type::BYTEA_ARRAY),
            (&self.contract_address, Type::BYTEA_ARRAY),
            (&self.gas_used, Type::INT8_ARRAY),
            (&self.cumulative_gas_used, Type::INT8_ARRAY),
            (&self.effective_gas_price, Type::TEXT_ARRAY),
            (&self.status, Type::INT2_ARRAY),
            (&self.fee_payer, Type::BYTEA_ARRAY),
        ]
    }
}

/// Blocks of a batch that hold transactions without receipt data, with the
/// earliest timestamp of each, for the receipt repair queue.
fn incomplete_blocks(txs: &[TxRow]) -> (Vec<i64>, Vec<DateTime<Utc>>) {
    let mut blocks: BTreeMap<i64, DateTime<Utc>> = BTreeMap::new();
    for tx in txs.iter().filter(|tx| tx.gas_used.is_none()) {
        blocks
            .entry(tx.block_num)
            .and_modify(|ts| *ts = (*ts).min(tx.block_timestamp))
            .or_insert(tx.block_timestamp);
    }
    blocks.into_iter().unzip()
}

pub async fn write_block(pool: &Pool, block: &BlockRow) -> Result<()> {
    write_blocks(pool, std::slice::from_ref(block)).await
}

pub async fn write_blocks(pool: &Pool, blocks: &[BlockRow]) -> Result<()> {
    if blocks.is_empty() {
        return Ok(());
    }
    write_batch(pool, blocks, &[], &[], &[]).await
}

pub async fn write_txs(pool: &Pool, txs: &[TxRow]) -> Result<()> {
    if txs.is_empty() {
        return Ok(());
    }
    write_batch(pool, &[], txs, &[], &[]).await
}

pub async fn write_logs(pool: &Pool, logs: &[LogRow]) -> Result<()> {
    if logs.is_empty() {
        return Ok(());
    }
    write_batch(pool, &[], &[], logs, &[]).await
}

pub async fn write_receipts(pool: &Pool, receipts: &[ReceiptRow]) -> Result<()> {
    if receipts.is_empty() {
        return Ok(());
    }
    write_batch(pool, &[], &[], &[], receipts).await
}

/// Batch insert blocks, txs, logs, and receipts in a single PG transaction.
///
/// Uses one connection, one transaction, one COMMIT, one WAL flush — instead of
/// four independent transactions when calling the individual write functions.
pub async fn write_batch(
    pool: &Pool,
    blocks: &[BlockRow],
    txs: &[TxRow],
    logs: &[LogRow],
    receipts: &[ReceiptRow],
) -> Result<()> {
    write_batch_inner(pool, blocks, txs, logs, receipts, None).await
}

pub async fn write_batch_with_application_name(
    pool: &Pool,
    blocks: &[BlockRow],
    txs: &[TxRow],
    logs: &[LogRow],
    receipts: &[ReceiptRow],
    application_name: &str,
) -> Result<()> {
    write_batch_inner(pool, blocks, txs, logs, receipts, Some(application_name)).await
}

/// Rows are passed as one array per column and expanded with `unnest`, so a
/// table needs one statement instead of a staging table, a COPY and an
/// `INSERT ... SELECT`. Statements are sent back to back without waiting for
/// each other's results; PostgreSQL runs them in order, so the whole batch
/// costs one round trip between `BEGIN` and `COMMIT`.
async fn write_batch_inner(
    pool: &Pool,
    blocks: &[BlockRow],
    txs: &[TxRow],
    logs: &[LogRow],
    receipts: &[ReceiptRow],
    application_name: Option<&str>,
) -> Result<()> {
    let start = Instant::now();

    let block_columns = BlockColumns::new(blocks);
    let tx_columns = TxColumns::new(txs);
    let log_columns = LogColumns::new(logs);
    let receipt_columns = ReceiptColumns::new(receipts);
    let tx_span = BlockSpan::new(txs.iter().map(|r| (r.block_num, r.block_timestamp)));
    let log_span = BlockSpan::new(logs.iter().map(|r| (r.block_num, r.block_timestamp)));
    let receipt_span = BlockSpan::new(receipts.iter().map(|r| (r.block_num, r.block_timestamp)));
    let (repair_blocks, repair_timestamps) = incomplete_blocks(txs);

    let mut conn = pool.get().await?;
    let tx = conn.transaction().await?;

    let mut statements: Vec<Statement<'_>> = Vec::new();
    if let Some(application_name) = &application_name {
        statements.push(execute(
            &tx,
            "SELECT set_config('application_name', $1, true)",
            vec![(application_name, Type::TEXT)],
        ));
    }
    if !blocks.is_empty() {
        statements.push(execute(&tx, INSERT_BLOCKS, block_columns.params()));
    }
    if let Some(span) = &tx_span {
        statements.push(delete_blocks_exact(&tx, DELETE_TXS, span));
        statements.push(execute(&tx, INSERT_TXS, tx_columns.params()));
        statements.push(execute(
            &tx,
            DELETE_REPAIR_QUEUE,
            vec![(&span.block_nums, Type::INT8_ARRAY)],
        ));
        if !repair_blocks.is_empty() {
            statements.push(execute(
                &tx,
                INSERT_REPAIR_QUEUE,
                vec![
                    (&repair_blocks, Type::INT8_ARRAY),
                    (&repair_timestamps, Type::TIMESTAMPTZ_ARRAY),
                ],
            ));
        }
    }
    if let Some(span) = &log_span {
        statements.push(delete_blocks_exact(&tx, DELETE_LOGS, span));
        statements.push(execute(&tx, INSERT_LOGS, log_columns.params()));
    }
    if let Some(span) = &receipt_span {
        statements.push(delete_blocks_exact(&tx, DELETE_RECEIPTS, span));
        statements.push(execute(&tx, INSERT_RECEIPTS, receipt_columns.params()));
    }
    pipeline(statements).await?;
    tx.commit().await?;

    // ── metrics ───────────────────────────────────────────────────────────
    let elapsed = start.elapsed();

    if !blocks.is_empty() {
        metrics::record_sink_write_duration("postgres", "blocks", elapsed);
        metrics::record_sink_write_rows("postgres", "blocks", blocks.len() as u64);
        metrics::update_sink_block_rate("postgres", blocks.len() as u64);
        metrics::increment_sink_row_count("postgres", "blocks", blocks.len() as u64);
        if let Some(max) = blocks.iter().map(|b| b.num).max() {
            metrics::update_sink_watermark("postgres", "blocks", max);
        }
    }

    if !txs.is_empty() {
        metrics::record_sink_write_duration("postgres", "txs", elapsed);
        metrics::record_sink_write_rows("postgres", "txs", txs.len() as u64);
        metrics::increment_sink_row_count("postgres", "txs", txs.len() as u64);
        if let Some(max) = txs.iter().map(|t| t.block_num).max() {
            metrics::update_sink_watermark("postgres", "txs", max);
        }
    }

    if !logs.is_empty() {
        metrics::record_sink_write_duration("postgres", "logs", elapsed);
        metrics::record_sink_write_rows("postgres", "logs", logs.len() as u64);
        metrics::increment_sink_row_count("postgres", "logs", logs.len() as u64);
        if let Some(max) = logs.iter().map(|l| l.block_num).max() {
            metrics::update_sink_watermark("postgres", "logs", max);
        }
    }

    if !receipts.is_empty() {
        metrics::record_sink_write_duration("postgres", "receipts", elapsed);
        metrics::record_sink_write_rows("postgres", "receipts", receipts.len() as u64);
        metrics::increment_sink_row_count("postgres", "receipts", receipts.len() as u64);
        if let Some(max) = receipts.iter().map(|r| r.block_num).max() {
            metrics::update_sink_watermark("postgres", "receipts", max);
        }
    }

    Ok(())
}

pub async fn load_sync_state(pool: &Pool, chain_id: u64) -> Result<Option<SyncState>> {
    let conn = pool.get().await?;

    let row = conn
        .query_opt(
            "SELECT chain_id, head_num, synced_num, tip_num, backfill_num, sync_rate, started_at, pruned_below FROM sync_state WHERE chain_id = $1",
            &[&(chain_id as i64)],
        )
        .await?;

    Ok(row.map(|r| SyncState {
        chain_id: r.get::<_, i64>(0) as u64,
        head_num: r.get::<_, i64>(1) as u64,
        synced_num: r.get::<_, i64>(2) as u64,
        tip_num: r.get::<_, i64>(3) as u64,
        backfill_num: r.get::<_, Option<i64>>(4).map(|n| n as u64),
        sync_rate: r.get(5),
        started_at: r.get(6),
        pruned_below: r.get::<_, i64>(7) as u64,
    }))
}

/// Load all sync states (for status display)
pub async fn load_all_sync_states(pool: &Pool) -> Result<Vec<SyncState>> {
    let conn = pool.get().await?;

    let rows = conn
        .query(
            "SELECT chain_id, head_num, synced_num, tip_num, backfill_num, sync_rate, started_at, pruned_below FROM sync_state ORDER BY chain_id",
            &[],
        )
        .await?;

    Ok(rows
        .iter()
        .map(|r| SyncState {
            chain_id: r.get::<_, i64>(0) as u64,
            head_num: r.get::<_, i64>(1) as u64,
            synced_num: r.get::<_, i64>(2) as u64,
            tip_num: r.get::<_, i64>(3) as u64,
            backfill_num: r.get::<_, Option<i64>>(4).map(|n| n as u64),
            sync_rate: r.get(5),
            started_at: r.get(6),
            pruned_below: r.get::<_, i64>(7) as u64,
        })
        .collect())
}

pub async fn save_sync_state(pool: &Pool, state: &SyncState) -> Result<()> {
    let conn = pool.get().await?;

    conn.execute(
        r#"
        INSERT INTO sync_state (chain_id, head_num, synced_num, tip_num, backfill_num, started_at, updated_at, pruned_below)
        VALUES ($1, $2, $3, $4, $5, COALESCE($6, NOW()), NOW(), $7)
        ON CONFLICT (chain_id) DO UPDATE SET
            head_num = GREATEST(sync_state.head_num, EXCLUDED.head_num),
            synced_num = GREATEST(sync_state.synced_num, EXCLUDED.synced_num),
            tip_num = GREATEST(sync_state.tip_num, EXCLUDED.tip_num),
            backfill_num = COALESCE(EXCLUDED.backfill_num, sync_state.backfill_num),
            started_at = COALESCE(sync_state.started_at, EXCLUDED.started_at),
            updated_at = NOW()
        "#,
        &[
            &(state.chain_id as i64),
            &(state.head_num as i64),
            &(state.synced_num as i64),
            &(state.tip_num as i64),
            &state.backfill_num.map(|n| n as i64),
            &state.started_at,
            &(state.pruned_below as i64),
        ],
    )
    .await?;

    Ok(())
}

/// Update only tip_num (for realtime sync - avoids clobbering synced_num)
pub async fn update_tip_num(pool: &Pool, chain_id: u64, tip_num: u64, head_num: u64) -> Result<()> {
    let conn = pool.get().await?;

    conn.execute(
        r#"
        INSERT INTO sync_state (chain_id, head_num, tip_num, synced_num, started_at, updated_at)
        VALUES ($1, $2, $3, 0, NOW(), NOW())
        ON CONFLICT (chain_id) DO UPDATE SET
            head_num = GREATEST(sync_state.head_num, EXCLUDED.head_num),
            tip_num = GREATEST(sync_state.tip_num, EXCLUDED.tip_num),
            updated_at = NOW()
        "#,
        &[&(chain_id as i64), &(head_num as i64), &(tip_num as i64)],
    )
    .await?;

    Ok(())
}

/// Rewind the sync pointers to the fork point of a reorg.
///
/// Every other writer of these pointers only raises them, so a reorg needs
/// its own path to move them back. `SinkSet::delete_from` has removed all rows
/// above `fork_block` from PostgreSQL and ClickHouse: `tip_num` is lowered so
/// realtime sync refetches that range, `synced_num` because the range is no
/// longer gap-free, and `archive_tip_num` so the archive verifies the range
/// again instead of counting the deleted ClickHouse rows as archived.
/// Pointers at or below the fork point are left unchanged.
pub async fn rewind_tip_num(pool: &Pool, chain_id: u64, fork_block: u64) -> Result<()> {
    let conn = pool.get().await?;

    conn.execute(
        r#"
        UPDATE sync_state
        SET tip_num = LEAST(tip_num, $1),
            synced_num = LEAST(synced_num, $1),
            archive_tip_num = LEAST(archive_tip_num, $1),
            updated_at = NOW()
        WHERE chain_id = $2
        "#,
        &[&(fork_block as i64), &(chain_id as i64)],
    )
    .await?;

    Ok(())
}

/// Record backfill progress without restoring stale realtime pointers after a reorg.
pub async fn update_backfill_num(pool: &Pool, chain_id: u64, block_num: u64) -> Result<()> {
    let conn = pool.get().await?;
    conn.execute(
        "UPDATE sync_state SET backfill_num = LEAST(backfill_num, $1), updated_at = NOW() WHERE chain_id = $2",
        &[&(block_num as i64), &(chain_id as i64)],
    ).await?;
    Ok(())
}

/// Advance a checked range only while the sync pointer still matches the snapshot.
/// A rewind invalidates the check; a missing tip must never be marked synced.
pub async fn advance_checked_synced_num(
    pool: &Pool,
    chain_id: u64,
    previous_synced_num: u64,
    checked_tip: u64,
) -> Result<()> {
    let conn = pool.get().await?;
    conn.execute(
        r#"UPDATE sync_state SET synced_num = $1, updated_at = NOW()
           WHERE chain_id = $2 AND synced_num = $3 AND tip_num >= $1
             AND EXISTS (SELECT 1 FROM blocks WHERE num = $1)"#,
        &[
            &(checked_tip as i64),
            &(chain_id as i64),
            &(previous_synced_num as i64),
        ],
    )
    .await?;
    Ok(())
}

/// Update only synced_num (for gap-fill sync - avoids clobbering tip_num)
pub async fn update_synced_num(pool: &Pool, chain_id: u64, synced_num: u64) -> Result<()> {
    let conn = pool.get().await?;

    conn.execute(
        r#"
        UPDATE sync_state
        SET synced_num = GREATEST(synced_num, $1),
            updated_at = NOW()
        WHERE chain_id = $2
        "#,
        &[&(synced_num as i64), &(chain_id as i64)],
    )
    .await?;

    Ok(())
}

/// ClickHouse interval known to be complete across all base tables.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ArchiveState {
    pub tip_num: u64,
    pub backfill_num: Option<u64>,
}

impl ArchiveState {
    pub fn covers(&self, from: u64, to: u64) -> bool {
        self.backfill_num.is_some_and(|low| low <= from) && self.tip_num >= to
    }
}

pub async fn load_archive_state(pool: &Pool, chain_id: u64) -> Result<ArchiveState> {
    let conn = pool.get().await?;
    let row = conn
        .query_opt(
            "SELECT archive_tip_num, archive_backfill_num FROM sync_state WHERE chain_id = $1",
            &[&(chain_id as i64)],
        )
        .await?;
    Ok(row
        .map(|r| ArchiveState {
            tip_num: r.get::<_, i64>(0).max(0) as u64,
            backfill_num: r.get::<_, Option<i64>>(1).map(|n| n.max(0) as u64),
        })
        .unwrap_or_default())
}

/// Persist a contiguous ClickHouse archive interval. The low watermark only
/// moves toward genesis and the high watermark only moves toward chain head.
pub async fn save_archive_state(
    pool: &Pool,
    chain_id: u64,
    backfill_num: u64,
    tip_num: u64,
) -> Result<()> {
    let conn = pool.get().await?;
    conn.execute(
        r#"
        INSERT INTO sync_state (chain_id, archive_tip_num, archive_backfill_num)
        VALUES ($1, $2, $3)
        ON CONFLICT (chain_id) DO UPDATE SET
            archive_tip_num = GREATEST(sync_state.archive_tip_num, EXCLUDED.archive_tip_num),
            archive_backfill_num = CASE
                WHEN sync_state.archive_backfill_num IS NULL THEN EXCLUDED.archive_backfill_num
                ELSE LEAST(sync_state.archive_backfill_num, EXCLUDED.archive_backfill_num)
            END,
            updated_at = NOW()
        "#,
        &[
            &(chain_id as i64),
            &(tip_num as i64),
            &(backfill_num as i64),
        ],
    )
    .await?;
    Ok(())
}

/// Set the current PostgreSQL hot-tier boundary.
///
/// Unlike the old prune watermark, this value is intentionally reversible:
/// increasing `pg_keep` first restores the missing PostgreSQL range and then
/// moves the boundary toward genesis.
pub async fn set_hot_boundary(
    pool: &Pool,
    chain_id: u64,
    boundary: u64,
    boundary_ts: Option<chrono::DateTime<chrono::Utc>>,
) -> Result<()> {
    let conn = pool.get().await?;

    conn.execute(
        r#"
        INSERT INTO sync_state (chain_id, pruned_below, pruned_below_ts, backfill_num)
        VALUES ($1, $2, $3, $2)
        ON CONFLICT (chain_id) DO UPDATE SET
            pruned_below = EXCLUDED.pruned_below,
            pruned_below_ts = EXCLUDED.pruned_below_ts,
            backfill_num = EXCLUDED.backfill_num,
            updated_at = NOW()
        "#,
        &[&(chain_id as i64), &(boundary as i64), &boundary_ts],
    )
    .await?;

    Ok(())
}

/// Update the current sync rate (rolling window average)
pub async fn update_sync_rate(pool: &Pool, chain_id: u64, rate: f64) -> Result<()> {
    let conn = pool.get().await?;

    conn.execute(
        r#"
        UPDATE sync_state
        SET sync_rate = $1,
            updated_at = NOW()
        WHERE chain_id = $2
        "#,
        &[&rate, &(chain_id as i64)],
    )
    .await?;

    Ok(())
}

/// Get block hash by block number (for parent hash validation)
pub async fn get_block_hash(pool: &Pool, block_num: u64) -> Result<Option<Vec<u8>>> {
    let conn = pool.get().await?;

    // Use LIMIT 1 to handle edge case of duplicate block nums (different timestamps)
    let row = conn
        .query_opt(
            "SELECT hash FROM blocks WHERE num = $1 ORDER BY timestamp DESC LIMIT 1",
            &[&(block_num as i64)],
        )
        .await?;

    Ok(row.map(|r| r.get(0)))
}

/// Fast check: are there any gaps in [from, to]?
/// Uses COUNT + btree index scan — O(range) not O(table).
pub async fn has_gaps(pool: &Pool, from: u64, to: u64) -> Result<bool> {
    if to < from {
        return Ok(false);
    }
    let conn = pool.get().await?;
    let row = conn
        .query_one(
            "SELECT COUNT(*) FROM blocks WHERE num >= $1 AND num <= $2",
            &[&(from as i64), &(to as i64)],
        )
        .await?;
    let count: i64 = row.get(0);
    let expected = (to - from + 1) as i64;
    Ok(count != expected)
}

/// Detect gaps in the block sequence (between existing blocks only)
/// Returns a list of (start, end) ranges that are missing.
/// `below` bounds the scan to `num <= below`, avoiding a full-table scan.
pub async fn detect_gaps(pool: &Pool, below: u64) -> Result<Vec<(u64, u64)>> {
    let conn = pool.get().await?;
    let below = below.min(i64::MAX as u64) as i64;

    let rows = conn
        .query(
            r#"
            WITH numbered AS (
                SELECT num, LAG(num) OVER (ORDER BY num) as prev_num
                FROM blocks
                WHERE num <= $1
            )
            SELECT prev_num + 1 as gap_start, num - 1 as gap_end
            FROM numbered
            WHERE num - prev_num > 1
            "#,
            &[&below],
        )
        .await?;

    Ok(rows
        .iter()
        .map(|r| (r.get::<_, i64>(0) as u64, r.get::<_, i64>(1) as u64))
        .collect())
}

/// Discover legacy incomplete transactions in one bounded block-number window.
///
/// The cursor is durable and each block range is visited once. The existing
/// `txs(block_num)` index bounds every read, so upgrades do not build a new
/// index or repeatedly scan transaction/receipt history.
pub async fn discover_legacy_receipt_repairs(
    pool: &Pool,
    chain_id: u64,
    block_window: i64,
) -> Result<usize> {
    let mut conn = pool.get().await?;
    let tx = conn.transaction().await?;
    let chain_id = chain_id as i64;
    let block_window = block_window.max(1);

    let state = tx
        .query_opt(
            "SELECT next_block, completed FROM receipt_repair_discovery \
             WHERE chain_id = $1 FOR UPDATE",
            &[&chain_id],
        )
        .await?;

    if state.as_ref().is_some_and(|row| row.get::<_, bool>(1)) {
        tx.commit().await?;
        return Ok(0);
    }

    let bounds = tx
        .query_one("SELECT MIN(block_num), MAX(block_num) FROM txs", &[])
        .await?;
    let min_block: Option<i64> = bounds.get(0);
    let max_block: Option<i64> = bounds.get(1);

    let (Some(min_block), Some(max_block)) = (min_block, max_block) else {
        tx.execute(
            r#"
            INSERT INTO receipt_repair_discovery (chain_id, next_block, completed)
            VALUES ($1, NULL, TRUE)
            ON CONFLICT (chain_id) DO UPDATE SET
                next_block = NULL,
                completed = TRUE,
                updated_at = NOW()
            "#,
            &[&chain_id],
        )
        .await?;
        tx.commit().await?;
        return Ok(0);
    };

    let next_block = state
        .as_ref()
        .and_then(|row| row.get::<_, Option<i64>>(0))
        .unwrap_or(max_block);

    if next_block < min_block {
        tx.execute(
            "UPDATE receipt_repair_discovery SET completed = TRUE, \
             next_block = NULL, updated_at = NOW() WHERE chain_id = $1",
            &[&chain_id],
        )
        .await?;
        tx.commit().await?;
        return Ok(0);
    }

    let from_block = (next_block - block_window + 1).max(min_block);
    let inserted = tx
        .execute(
            r#"
            INSERT INTO receipt_repair_queue (block_num, block_timestamp)
            SELECT block_num, MIN(block_timestamp)
            FROM txs
            WHERE block_num >= $1
              AND block_num <= $2
              AND gas_used IS NULL
            GROUP BY block_num
            ON CONFLICT (block_num) DO UPDATE SET
                block_timestamp = EXCLUDED.block_timestamp,
                updated_at = NOW()
            "#,
            &[&from_block, &next_block],
        )
        .await?;

    let completed = from_block == min_block;
    let following_block = (!completed).then_some(from_block - 1);
    tx.execute(
        r#"
        INSERT INTO receipt_repair_discovery (chain_id, next_block, completed)
        VALUES ($1, $2, $3)
        ON CONFLICT (chain_id) DO UPDATE SET
            next_block = EXCLUDED.next_block,
            completed = EXCLUDED.completed,
            updated_at = NOW()
        "#,
        &[&chain_id, &following_block, &completed],
    )
    .await?;
    tx.commit().await?;

    Ok(inserted as usize)
}

/// Return due blocks from the durable receipt-repair queue.
pub async fn detect_blocks_missing_receipts(pool: &Pool, limit: i64) -> Result<Vec<u64>> {
    let conn = pool.get().await?;

    let rows = conn
        .query(
            r#"
            WITH due AS MATERIALIZED (
                SELECT block_num
                FROM receipt_repair_queue
                WHERE next_attempt_at <= NOW()
                ORDER BY next_attempt_at, block_num DESC
                LIMIT $1
                FOR UPDATE SKIP LOCKED
            )
            UPDATE receipt_repair_queue q
            SET next_attempt_at = NOW() + INTERVAL '2 minutes',
                updated_at = NOW()
            FROM due
            WHERE q.block_num = due.block_num
            RETURNING q.block_num
            "#,
            &[&limit],
        )
        .await?;

    let mut blocks: Vec<u64> = rows.iter().map(|r| r.get::<_, i64>(0) as u64).collect();
    blocks.sort_unstable_by(|a, b| b.cmp(a));
    Ok(blocks)
}

/// Remove repaired queue entries and exponentially defer poison/incomplete
/// entries. Exact block timestamps keep the completion probe partition-pruned.
pub async fn finish_receipt_repair_attempt(pool: &Pool, block_nums: &[u64]) -> Result<(u64, u64)> {
    if block_nums.is_empty() {
        return Ok((0, 0));
    }

    let block_nums: Vec<i64> = block_nums.iter().map(|&block| block as i64).collect();
    let mut conn = pool.get().await?;
    let tx = conn.transaction().await?;
    let completed = tx
        .execute(
            r#"
            DELETE FROM receipt_repair_queue q
            WHERE q.block_num = ANY($1)
              AND NOT EXISTS (
                  SELECT 1
                  FROM txs t
                  WHERE t.block_timestamp = q.block_timestamp
                    AND t.block_num = q.block_num
                    AND t.gas_used IS NULL
              )
            "#,
            &[&block_nums],
        )
        .await?;
    let deferred = tx
        .execute(
            r#"
            UPDATE receipt_repair_queue
            SET attempts = attempts + 1,
                next_attempt_at = NOW() + make_interval(
                    secs => LEAST(3600, POWER(2, LEAST(attempts + 1, 12))::INT)
                ),
                last_error = 'receipt data still incomplete after RPC repair',
                updated_at = NOW()
            WHERE block_num = ANY($1)
            "#,
            &[&block_nums],
        )
        .await?;
    tx.commit().await?;

    Ok((completed, deferred))
}

/// Detect ALL gaps between `floor` and `tip_num`, including leading and trailing gaps
/// outside the stored block range.
/// `floor` is the lowest block expected in PG (`SyncState::prune_floor()`);
/// pass 1 when no pruning is configured (block 0 is genesis/empty).
/// Returns gaps sorted by end block descending (most recent first).
pub async fn detect_all_gaps(pool: &Pool, floor: u64, tip_num: u64) -> Result<Vec<(u64, u64)>> {
    let floor = floor.max(1);
    let conn = pool.get().await?;

    // Lowest stored block, unfiltered: a stored block at/below the floor
    // (e.g. genesis 0) means there is no leading gap; detect_gaps already
    // reports discontinuities above it.
    let bounds = conn
        .query_one(
            "SELECT MIN(num), MAX(num) FROM blocks WHERE num <= $1",
            &[&(tip_num as i64)],
        )
        .await?;
    let min_block: Option<i64> = bounds.get(0);
    let max_block: Option<i64> = bounds.get(1);

    let mut gaps = detect_gaps(pool, tip_num).await?;

    // Add gap from floor to first stored block
    if let Some(min) = min_block {
        if (min as u64) > floor {
            gaps.push((floor, min as u64 - 1));
        }
    } else if tip_num >= floor {
        // No blocks at all - entire range is a gap
        gaps.push((floor, tip_num));
    }

    // A reorg can remove the tail before realtime refetches it. LAG only
    // finds internal gaps, so explicitly include the missing trailing range.
    if let Some(max) = max_block {
        if (max as u64) < tip_num {
            gaps.push(((max as u64).saturating_add(1).max(floor), tip_num));
        }
    }

    // Clamp to [floor, tip_num]: anything below floor was intentionally pruned
    gaps.retain(|(_, end)| *end <= tip_num && *end >= floor);
    for gap in &mut gaps {
        gap.0 = gap.0.max(floor);
    }

    // Sort by end block descending (most recent gaps first)
    gaps.sort_by_key(|b| std::cmp::Reverse(b.1));

    Ok(gaps)
}

/// Delete all blocks (and related txs, logs, receipts) from a given block number onwards.
/// Used for reorg handling - removes orphaned blocks so they can be re-synced.
/// Returns the number of blocks deleted.
pub async fn delete_blocks_from(pool: &Pool, from_block: u64) -> Result<u64> {
    let conn = pool.get().await?;
    let from_block_i64 = from_block as i64;

    // Delete in order: logs, receipts, txs, blocks (foreign key order)
    conn.execute(
        "DELETE FROM receipt_repair_queue WHERE block_num >= $1",
        &[&from_block_i64],
    )
    .await?;
    conn.execute("DELETE FROM logs WHERE block_num >= $1", &[&from_block_i64])
        .await?;
    conn.execute(
        "DELETE FROM receipts WHERE block_num >= $1",
        &[&from_block_i64],
    )
    .await?;
    conn.execute("DELETE FROM txs WHERE block_num >= $1", &[&from_block_i64])
        .await?;
    let deleted = conn
        .execute("DELETE FROM blocks WHERE num >= $1", &[&from_block_i64])
        .await?;

    Ok(deleted)
}

/// Find the fork point by walking back from a mismatch until we find a matching hash.
/// Returns the last block number where the stored hash matches the chain.
/// If no match is found within max_depth, returns None.
pub async fn find_fork_point(
    pool: &Pool,
    rpc: &super::fetcher::RpcClient,
    mismatch_block: u64,
    max_depth: u64,
) -> Result<Option<u64>> {
    let min_block = mismatch_block.saturating_sub(max_depth).max(1);

    for block_num in (min_block..mismatch_block).rev() {
        let stored_hash = get_block_hash(pool, block_num).await?;

        if let Some(stored) = stored_hash {
            // Fetch the canonical hash from RPC
            let rpc_block = rpc.get_block(block_num, false).await?;
            let rpc_hash = rpc_block.header.hash.0.to_vec();

            if stored == rpc_hash {
                return Ok(Some(block_num));
            }
        } else {
            // No stored block at this height - this is the fork point
            return Ok(Some(block_num));
        }
    }

    Ok(None)
}
