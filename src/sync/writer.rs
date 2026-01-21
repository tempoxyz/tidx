use anyhow::Result;
use std::fmt::Write;
use std::pin::Pin;
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::types::Type;

use crate::db::Pool;
use crate::types::{BlockRow, LogRow, ReceiptRow, SyncState, TxRow};

pub async fn write_block(pool: &Pool, block: &BlockRow) -> Result<()> {
    write_blocks(pool, std::slice::from_ref(block)).await
}

/// Batch insert multiple blocks in a single query
pub async fn write_blocks(pool: &Pool, blocks: &[BlockRow]) -> Result<()> {
    if blocks.is_empty() {
        return Ok(());
    }

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

    Ok(())
}

/// Batch insert transactions using COPY for maximum throughput
pub async fn write_txs(pool: &Pool, txs: &[TxRow]) -> Result<()> {
    if txs.is_empty() {
        return Ok(());
    }

    let conn = pool.get().await?;

    let staging_table = format!("txs_staging_{}", std::process::id());
    
    conn.execute(
        &format!("CREATE TEMP TABLE IF NOT EXISTS {staging_table} (LIKE txs INCLUDING DEFAULTS)"),
        &[],
    )
    .await?;

    conn.execute(&format!("TRUNCATE {staging_table}"), &[]).await?;

    // Binary COPY into staging table
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
            &format!(r#"COPY {staging_table} (block_num, block_timestamp, idx, hash, type, "from", "to", value, input,
                gas_limit, max_fee_per_gas, max_priority_fee_per_gas, gas_used,
                nonce_key, nonce, fee_token, fee_payer, calls, call_count,
                valid_before, valid_after, signature_type) FROM STDIN BINARY"#),
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

    conn.execute(
        &format!(r#"INSERT INTO txs SELECT * FROM {staging_table} ON CONFLICT (block_timestamp, block_num, idx) DO NOTHING"#),
        &[],
    )
    .await?;

    Ok(())
}

/// Batch insert logs using COPY for maximum throughput
pub async fn write_logs(pool: &Pool, logs: &[LogRow]) -> Result<()> {
    if logs.is_empty() {
        return Ok(());
    }

    let conn = pool.get().await?;

    let staging_table = format!("logs_staging_{}", std::process::id());
    
    conn.execute(
        &format!("CREATE TEMP TABLE IF NOT EXISTS {staging_table} (LIKE logs INCLUDING DEFAULTS)"),
        &[],
    )
    .await?;

    conn.execute(&format!("TRUNCATE {staging_table}"), &[]).await?;

    // Binary COPY into staging table
    let types = &[
        Type::INT8,       // block_num
        Type::TIMESTAMPTZ, // block_timestamp
        Type::INT4,       // log_idx
        Type::INT4,       // tx_idx
        Type::BYTEA,      // tx_hash
        Type::BYTEA,      // address
        Type::BYTEA,      // selector
        Type::BYTEA_ARRAY, // topics
        Type::BYTEA,      // data
    ];

    let sink = conn
        .copy_in(
            &format!("COPY {staging_table} (block_num, block_timestamp, log_idx, tx_idx, tx_hash, address, selector, topics, data) FROM STDIN BINARY"),
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
                &log.topics,
                &log.data,
            ])
            .await?;
    }

    pinned_writer.as_mut().finish().await?;

    conn.execute(
        &format!("INSERT INTO logs SELECT * FROM {staging_table} ON CONFLICT (block_timestamp, block_num, log_idx) DO NOTHING"),
        &[],
    )
    .await?;

    Ok(())
}

/// Batch insert receipts using COPY for maximum throughput
pub async fn write_receipts(pool: &Pool, receipts: &[ReceiptRow]) -> Result<()> {
    if receipts.is_empty() {
        return Ok(());
    }

    let conn = pool.get().await?;

    conn.execute(
        "CREATE UNLOGGED TABLE IF NOT EXISTS receipts_staging (LIKE receipts INCLUDING DEFAULTS)",
        &[],
    )
    .await?;

    conn.execute("TRUNCATE receipts_staging", &[]).await?;

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
            r#"COPY receipts_staging (block_num, block_timestamp, tx_idx, tx_hash, "from", "to",
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

    conn.execute(
        "INSERT INTO receipts SELECT * FROM receipts_staging ON CONFLICT (block_timestamp, block_num, tx_idx) DO NOTHING",
        &[],
    )
    .await?;

    Ok(())
}

pub async fn load_sync_state(pool: &Pool, chain_id: u64) -> Result<Option<SyncState>> {
    let conn = pool.get().await?;

    let row = conn
        .query_opt(
            "SELECT chain_id, head_num, synced_num, tip_num, backfill_num, started_at FROM sync_state WHERE chain_id = $1",
            &[&(chain_id as i64)],
        )
        .await?;

    Ok(row.map(|r| SyncState {
        chain_id: r.get::<_, i64>(0) as u64,
        head_num: r.get::<_, i64>(1) as u64,
        synced_num: r.get::<_, i64>(2) as u64,
        tip_num: r.get::<_, i64>(3) as u64,
        backfill_num: r.get::<_, Option<i64>>(4).map(|n| n as u64),
        started_at: r.get(5),
    }))
}

/// Load all sync states (for status display)
pub async fn load_all_sync_states(pool: &Pool) -> Result<Vec<SyncState>> {
    let conn = pool.get().await?;

    let rows = conn
        .query(
            "SELECT chain_id, head_num, synced_num, tip_num, backfill_num, started_at FROM sync_state ORDER BY chain_id",
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
            started_at: r.get(5),
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

/// Detect gaps in the block sequence
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
