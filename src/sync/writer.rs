use anyhow::Result;
use std::fmt::Write;
use std::pin::Pin;
use std::time::Instant;
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::types::Type;

use crate::db::Pool;
use crate::metrics;
use crate::types::{BlockRow, LogRow, ReceiptRow, SyncState, TxRow};

pub async fn write_block(pool: &Pool, block: &BlockRow) -> Result<()> {
    write_blocks(pool, std::slice::from_ref(block)).await
}

/// Batch insert multiple blocks in a single query
pub async fn write_blocks(pool: &Pool, blocks: &[BlockRow]) -> Result<()> {
    if blocks.is_empty() {
        return Ok(());
    }

    let start = Instant::now();
    let conn = pool.get().await?;

    // Build multi-row VALUES clause
    let mut query = String::from(
        "INSERT INTO blocks (num, hash, parent_hash, timestamp, timestamp_ms, gas_limit, gas_used, miner, extra_data) VALUES ",
    );

    for (i, _block) in blocks.iter().enumerate() {
        if i > 0 {
            query.push_str(", ");
        }
        let base = i * 9;
        write!(
            &mut query,
            "(${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${})",
            base + 1, base + 2, base + 3, base + 4, base + 5, base + 6, base + 7, base + 8, base + 9
        )?;
    }

    query.push_str(" ON CONFLICT (timestamp, num) DO NOTHING");

    // Collect params - need to store values to extend lifetime
    let param_values: Vec<_> = blocks
        .iter()
        .flat_map(|b| {
            vec![
                &b.num as &(dyn tokio_postgres::types::ToSql + Sync),
                &b.hash,
                &b.parent_hash,
                &b.timestamp,
                &b.timestamp_ms,
                &b.gas_limit,
                &b.gas_used,
                &b.miner,
                &b.extra_data,
            ]
        })
        .collect();

    conn.execute(&query, &param_values).await?;

    metrics::record_sink_write_duration("postgres", "blocks", start.elapsed());
    metrics::record_sink_write_rows("postgres", "blocks", blocks.len() as u64);
    metrics::update_sink_block_rate("postgres", blocks.len() as u64);

    Ok(())
}

/// Batch insert transactions using DELETE + COPY for maximum throughput
pub async fn write_txs(pool: &Pool, txs: &[TxRow]) -> Result<()> {
    if txs.is_empty() {
        return Ok(());
    }

    let start = Instant::now();
    let conn = pool.get().await?;
    conn.execute("SET statement_timeout = 0", &[]).await?;

    // Get block range and delete existing rows before COPY
    let min_block = txs.iter().map(|t| t.block_num).min().unwrap();
    let max_block = txs.iter().map(|t| t.block_num).max().unwrap();
    conn.execute(
        "DELETE FROM txs WHERE block_num >= $1 AND block_num <= $2",
        &[&min_block, &max_block],
    )
    .await?;

    let types = &[
        Type::INT8,       // block_num
        Type::TIMESTAMPTZ, // block_timestamp
        Type::INT4,       // idx
        Type::BYTEA,      // hash
        Type::INT2,       // type
        Type::BYTEA,      // from
        Type::BYTEA,      // to
        Type::TEXT,       // value
        Type::BYTEA,      // input
        Type::INT8,       // gas_limit
        Type::TEXT,       // max_fee_per_gas
        Type::TEXT,       // max_priority_fee_per_gas
        Type::INT8,       // gas_used
        Type::BYTEA,      // nonce_key
        Type::INT8,       // nonce
        Type::BYTEA,      // fee_token
        Type::BYTEA,      // fee_payer
        Type::JSONB,      // calls
        Type::INT2,       // call_count
        Type::INT8,       // valid_before
        Type::INT8,       // valid_after
        Type::INT2,       // signature_type
    ];

    let sink = conn
        .copy_in(
            r#"COPY txs (block_num, block_timestamp, idx, hash, type, "from", "to", value, input,
                gas_limit, max_fee_per_gas, max_priority_fee_per_gas, gas_used,
                nonce_key, nonce, fee_token, fee_payer, calls, call_count,
                valid_before, valid_after, signature_type) FROM STDIN BINARY"#,
        )
        .await?;

    let writer = BinaryCopyInWriter::new(sink, types);
    let mut pinned_writer: Pin<Box<BinaryCopyInWriter>> = Box::pin(writer);

    for tx in txs {
        pinned_writer
            .as_mut()
            .write(&[
                &tx.block_num,
                &tx.block_timestamp,
                &tx.idx,
                &tx.hash,
                &tx.tx_type,
                &tx.from,
                &tx.to,
                &tx.value,
                &tx.input,
                &tx.gas_limit,
                &tx.max_fee_per_gas,
                &tx.max_priority_fee_per_gas,
                &tx.gas_used,
                &tx.nonce_key,
                &tx.nonce,
                &tx.fee_token,
                &tx.fee_payer,
                &tx.calls,
                &tx.call_count,
                &tx.valid_before,
                &tx.valid_after,
                &tx.signature_type,
            ])
            .await?;
    }

    pinned_writer.as_mut().finish().await?;

    metrics::record_sink_write_duration("postgres", "txs", start.elapsed());
    metrics::record_sink_write_rows("postgres", "txs", txs.len() as u64);

    Ok(())
}

/// Batch insert logs using DELETE + COPY for maximum throughput
pub async fn write_logs(pool: &Pool, logs: &[LogRow]) -> Result<()> {
    if logs.is_empty() {
        return Ok(());
    }

    let start = Instant::now();
    let conn = pool.get().await?;
    conn.execute("SET statement_timeout = 0", &[]).await?;

    // Get block range and delete existing rows before COPY
    let min_block = logs.iter().map(|l| l.block_num).min().unwrap();
    let max_block = logs.iter().map(|l| l.block_num).max().unwrap();
    conn.execute(
        "DELETE FROM logs WHERE block_num >= $1 AND block_num <= $2",
        &[&min_block, &max_block],
    )
    .await?;

    let types = &[
        Type::INT8,       // block_num
        Type::TIMESTAMPTZ, // block_timestamp
        Type::INT4,       // log_idx
        Type::INT4,       // tx_idx
        Type::BYTEA,      // tx_hash
        Type::BYTEA,      // address
        Type::BYTEA,      // selector
        Type::BYTEA,      // topic0
        Type::BYTEA,      // topic1
        Type::BYTEA,      // topic2
        Type::BYTEA,      // topic3
        Type::BYTEA,      // data
    ];

    let sink = conn
        .copy_in(
            "COPY logs (block_num, block_timestamp, log_idx, tx_idx, tx_hash, address, selector, topic0, topic1, topic2, topic3, data) FROM STDIN BINARY",
        )
        .await?;

    let writer = BinaryCopyInWriter::new(sink, types);
    let mut pinned_writer: Pin<Box<BinaryCopyInWriter>> = Box::pin(writer);

    for log in logs {
        pinned_writer
            .as_mut()
            .write(&[
                &log.block_num,
                &log.block_timestamp,
                &log.log_idx,
                &log.tx_idx,
                &log.tx_hash,
                &log.address,
                &log.selector,
                &log.topic0,
                &log.topic1,
                &log.topic2,
                &log.topic3,
                &log.data,
            ])
            .await?;
    }

    pinned_writer.as_mut().finish().await?;

    metrics::record_sink_write_duration("postgres", "logs", start.elapsed());
    metrics::record_sink_write_rows("postgres", "logs", logs.len() as u64);

    Ok(())
}

/// Batch insert receipts using DELETE + COPY for maximum throughput
pub async fn write_receipts(pool: &Pool, receipts: &[ReceiptRow]) -> Result<()> {
    if receipts.is_empty() {
        return Ok(());
    }

    let start = Instant::now();
    let conn = pool.get().await?;
    conn.execute("SET statement_timeout = 0", &[]).await?;

    // Get block range and delete existing rows before COPY
    let min_block = receipts.iter().map(|r| r.block_num).min().unwrap();
    let max_block = receipts.iter().map(|r| r.block_num).max().unwrap();
    conn.execute(
        "DELETE FROM receipts WHERE block_num >= $1 AND block_num <= $2",
        &[&min_block, &max_block],
    )
    .await?;

    let types = &[
        Type::INT8,        // block_num
        Type::TIMESTAMPTZ, // block_timestamp
        Type::INT4,        // tx_idx
        Type::BYTEA,       // tx_hash
        Type::BYTEA,       // from
        Type::BYTEA,       // to
        Type::BYTEA,       // contract_address
        Type::INT8,        // gas_used
        Type::INT8,        // cumulative_gas_used
        Type::TEXT,        // effective_gas_price
        Type::INT2,        // status
        Type::BYTEA,       // fee_payer
    ];

    let sink = conn
        .copy_in(
            r#"COPY receipts (block_num, block_timestamp, tx_idx, tx_hash, "from", "to",
                contract_address, gas_used, cumulative_gas_used, effective_gas_price,
                status, fee_payer) FROM STDIN BINARY"#,
        )
        .await?;

    let writer = BinaryCopyInWriter::new(sink, types);
    let mut pinned_writer: Pin<Box<BinaryCopyInWriter>> = Box::pin(writer);

    for receipt in receipts {
        pinned_writer
            .as_mut()
            .write(&[
                &receipt.block_num,
                &receipt.block_timestamp,
                &receipt.tx_idx,
                &receipt.tx_hash,
                &receipt.from,
                &receipt.to,
                &receipt.contract_address,
                &receipt.gas_used,
                &receipt.cumulative_gas_used,
                &receipt.effective_gas_price,
                &receipt.status,
                &receipt.fee_payer,
            ])
            .await?;
    }

    pinned_writer.as_mut().finish().await?;

    metrics::record_sink_write_duration("postgres", "receipts", start.elapsed());
    metrics::record_sink_write_rows("postgres", "receipts", receipts.len() as u64);

    Ok(())
}

pub async fn load_sync_state(pool: &Pool, chain_id: u64) -> Result<Option<SyncState>> {
    let conn = pool.get().await?;

    let row = conn
        .query_opt(
            "SELECT chain_id, head_num, synced_num, tip_num, backfill_num, sync_rate, started_at FROM sync_state WHERE chain_id = $1",
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
    }))
}

/// Load all sync states (for status display)
pub async fn load_all_sync_states(pool: &Pool) -> Result<Vec<SyncState>> {
    let conn = pool.get().await?;

    let rows = conn
        .query(
            "SELECT chain_id, head_num, synced_num, tip_num, backfill_num, sync_rate, started_at FROM sync_state ORDER BY chain_id",
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
        })
        .collect())
}

pub async fn save_sync_state(pool: &Pool, state: &SyncState) -> Result<()> {
    let conn = pool.get().await?;

    conn.execute(
        r#"
        INSERT INTO sync_state (chain_id, head_num, synced_num, tip_num, backfill_num, started_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, COALESCE($6, NOW()), NOW())
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

/// Detect gaps in the block sequence (between existing blocks only)
/// Returns a list of (start, end) ranges that are missing
pub async fn detect_gaps(pool: &Pool) -> Result<Vec<(u64, u64)>> {
    let conn = pool.get().await?;

    let rows = conn
        .query(
            r#"
            WITH numbered AS (
                SELECT num, LAG(num) OVER (ORDER BY num) as prev_num
                FROM blocks
            )
            SELECT prev_num + 1 as gap_start, num - 1 as gap_end
            FROM numbered
            WHERE num - prev_num > 1
            "#,
            &[],
        )
        .await?;

    Ok(rows
        .iter()
        .map(|r| {
            (
                r.get::<_, i64>(0) as u64,
                r.get::<_, i64>(1) as u64,
            )
        })
        .collect())
}

/// Detect blocks that have no receipts (for deferred receipt backfill).
/// Returns block numbers that exist in blocks table but have no receipts.
/// Limited to a batch size and ordered by block_num DESC (most recent first).
pub async fn detect_blocks_missing_receipts(pool: &Pool, limit: i64) -> Result<Vec<u64>> {
    let conn = pool.get().await?;

    let rows = conn
        .query(
            r#"
            SELECT b.num
            FROM blocks b
            LEFT JOIN receipts r ON r.block_num = b.num
            WHERE r.block_num IS NULL
            ORDER BY b.num DESC
            LIMIT $1
            "#,
            &[&limit],
        )
        .await?;

    Ok(rows.iter().map(|r| r.get::<_, i64>(0) as u64).collect())
}

/// Detect ALL gaps including from genesis to first block
/// Returns gaps sorted by end block descending (most recent first)
pub async fn detect_all_gaps(pool: &Pool, tip_num: u64) -> Result<Vec<(u64, u64)>> {
    let conn = pool.get().await?;

    // Get the lowest block number we have
    let min_block: Option<i64> = conn
        .query_one("SELECT MIN(num) FROM blocks", &[])
        .await?
        .get(0);

    let mut gaps = detect_gaps(pool).await?;

    // Add gap from block 1 to first block (if we have any blocks and min > 1)
    // Block 0 is typically empty/genesis, so we start from block 1
    if let Some(min) = min_block {
        if min > 1 {
            gaps.push((1, min as u64 - 1));
        }
    } else if tip_num > 0 {
        // No blocks at all - entire range is a gap (starting from 1)
        gaps.push((1, tip_num));
    }

    // Filter to only gaps up to tip_num
    gaps.retain(|(_, end)| *end <= tip_num);

    // Sort by end block descending (most recent gaps first)
    gaps.sort_by(|a, b| b.1.cmp(&a.1));

    Ok(gaps)
}

/// Delete all blocks (and related txs, logs, receipts) from a given block number onwards.
/// Used for reorg handling - removes orphaned blocks so they can be re-synced.
/// Returns the number of blocks deleted.
pub async fn delete_blocks_from(pool: &Pool, from_block: u64) -> Result<u64> {
    let conn = pool.get().await?;
    let from_block_i64 = from_block as i64;

    // Delete in order: logs, receipts, txs, blocks (foreign key order)
    conn.execute("DELETE FROM logs WHERE block_num >= $1", &[&from_block_i64])
        .await?;
    conn.execute("DELETE FROM receipts WHERE block_num >= $1", &[&from_block_i64])
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
