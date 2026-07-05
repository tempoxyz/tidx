use anyhow::Result;
use tracing::info;

use crate::db::Pool;
use crate::metrics;
use crate::tempo::{Call, TEMPO_TX_TYPE_ID};
use crate::types::{BlockRow, LogRow, ReceiptRow, TxRow};

use super::ch_sink::ClickHouseSink;
use super::writer;

/// Number of blocks worth of data to fetch per query during backfill.
/// Uses block-range pagination (no long-lived transactions).
const BACKFILL_BLOCK_BATCH: i64 = 5_000;

/// Fan-out writer that sends data to all configured sinks.
///
/// PostgreSQL is always present. ClickHouse is optional.
/// Write failures from either sink are fatal and propagate to the caller.
#[derive(Clone)]
pub struct SinkSet {
    pool: Pool,
    ch: Option<ClickHouseSink>,
}

impl SinkSet {
    pub fn new(pool: Pool) -> Self {
        Self { pool, ch: None }
    }

    pub fn with_clickhouse(mut self, ch: ClickHouseSink) -> Self {
        self.ch = Some(ch);
        self
    }

    /// Access the underlying PG pool for read operations (sync state, gap detection, etc.)
    pub fn pool(&self) -> &Pool {
        &self.pool
    }

    pub async fn write_blocks(&self, blocks: &[BlockRow]) -> Result<()> {
        if let Some(ch) = &self.ch {
            tokio::try_join!(
                writer::write_blocks(&self.pool, blocks),
                ch.write_blocks(blocks),
            )?;
        } else {
            writer::write_blocks(&self.pool, blocks).await?;
        }
        Ok(())
    }

    pub async fn write_txs(&self, txs: &[TxRow]) -> Result<()> {
        if let Some(ch) = &self.ch {
            tokio::try_join!(writer::write_txs(&self.pool, txs), ch.write_txs(txs),)?;
        } else {
            writer::write_txs(&self.pool, txs).await?;
        }
        Ok(())
    }

    pub async fn write_logs(&self, logs: &[LogRow]) -> Result<()> {
        if let Some(ch) = &self.ch {
            tokio::try_join!(writer::write_logs(&self.pool, logs), ch.write_logs(logs),)?;
        } else {
            writer::write_logs(&self.pool, logs).await?;
        }
        Ok(())
    }

    pub async fn write_receipts(&self, receipts: &[ReceiptRow]) -> Result<()> {
        if let Some(ch) = &self.ch {
            tokio::try_join!(
                writer::write_receipts(&self.pool, receipts),
                ch.write_receipts(receipts),
            )?;
        } else {
            writer::write_receipts(&self.pool, receipts).await?;
        }
        Ok(())
    }

    /// Write all four tables in a single PG transaction, with CH writes concurrent.
    pub async fn write_all(
        &self,
        blocks: &[BlockRow],
        txs: &[TxRow],
        logs: &[LogRow],
        receipts: &[ReceiptRow],
    ) -> Result<()> {
        self.write_all_inner(blocks, txs, logs, receipts, None)
            .await
    }

    pub async fn write_all_with_application_name(
        &self,
        blocks: &[BlockRow],
        txs: &[TxRow],
        logs: &[LogRow],
        receipts: &[ReceiptRow],
        application_name: &str,
    ) -> Result<()> {
        self.write_all_inner(blocks, txs, logs, receipts, Some(application_name))
            .await
    }

    async fn write_all_inner(
        &self,
        blocks: &[BlockRow],
        txs: &[TxRow],
        logs: &[LogRow],
        receipts: &[ReceiptRow],
        application_name: Option<&str>,
    ) -> Result<()> {
        if let Some(ch) = &self.ch {
            tokio::try_join!(
                async {
                    if let Some(application_name) = application_name {
                        writer::write_batch_with_application_name(
                            &self.pool,
                            blocks,
                            txs,
                            logs,
                            receipts,
                            application_name,
                        )
                        .await
                    } else {
                        writer::write_batch(&self.pool, blocks, txs, logs, receipts).await
                    }
                },
                ch.write_blocks(blocks),
                ch.write_txs(txs),
                ch.write_logs(logs),
                ch.write_receipts(receipts),
            )?;
        } else if let Some(application_name) = application_name {
            writer::write_batch_with_application_name(
                &self.pool,
                blocks,
                txs,
                logs,
                receipts,
                application_name,
            )
            .await?;
        } else {
            writer::write_batch(&self.pool, blocks, txs, logs, receipts).await?;
        }
        Ok(())
    }

    /// Delete all data from a given block number onwards (reorg support).
    /// Returns the number of blocks deleted from PostgreSQL.
    pub async fn delete_from(&self, block_num: u64) -> Result<u64> {
        if let Some(ch) = &self.ch {
            let (deleted, ()) = tokio::try_join!(
                writer::delete_blocks_from(&self.pool, block_num),
                ch.delete_from(block_num),
            )?;
            Ok(deleted)
        } else {
            writer::delete_blocks_from(&self.pool, block_num).await
        }
    }

    /// Automatically backfill ClickHouse from PostgreSQL if CH is behind.
    ///
    /// Uses a persistent cursor (`ch_backfill_block`) in the PG `sync_state` table
    /// to track progress. This avoids the race condition where realtime sync writes
    /// blocks ahead of the backfill position, causing `max(block_num)` to skip gaps.
    ///
    /// All four tables (blocks, txs, logs, receipts) are backfilled together per
    /// block range. The cursor advances only after all tables succeed for that range.
    /// Tables use ReplacingMergeTree so re-inserts after a crash are safe.
    pub async fn backfill_clickhouse(&self, chain_id: u64) -> Result<()> {
        let ch = match &self.ch {
            Some(ch) => ch,
            None => return Ok(()),
        };

        let pg_max = pg_max_block_num(&self.pool).await?;
        let pg_max = match pg_max {
            Some(n) => n,
            None => return Ok(()), // PG is empty, nothing to backfill
        };

        // Load persisted cursor from PG (survives restarts)
        let cursor = load_ch_backfill_cursor(&self.pool, chain_id).await?;
        let from_block = cursor + 1;

        if from_block > pg_max {
            info!(
                ch_backfill_block = cursor,
                pg_max, "ClickHouse backfill up to date"
            );
            metrics::set_backfill_remaining(chain_id, "clickhouse", 0);
            return Ok(());
        }

        let total = pg_max - from_block + 1;
        metrics::set_backfill_remaining(chain_id, "clickhouse", total as u64);
        info!(
            chain_id,
            from_block,
            pg_max,
            total_blocks = total,
            "Starting ClickHouse backfill"
        );

        let start = std::time::Instant::now();
        let mut blocks_written: i64 = 0;
        let mut current = from_block;

        // Pre-fetch the first batch so we can pipeline fetch(N+1) with write(N)
        let mut pending = {
            let batch_end = (current + BACKFILL_BLOCK_BATCH - 1).min(pg_max);
            let conn = self.pool.get().await?;
            let data = tokio::try_join!(
                fetch_blocks(&conn, current, batch_end),
                fetch_txs(&conn, current, batch_end),
                fetch_logs(&conn, current, batch_end),
                fetch_receipts(&conn, current, batch_end),
            )?;
            current = batch_end + 1;
            Some((batch_end, data))
        };

        while let Some((batch_end, (blocks, txs, logs, mut receipts))) = pending.take() {
            let block_count = blocks.len() as i64;

            // Postgres receipts lack the denormalized tx-level type/fee_token;
            // populate them from txs so the ClickHouse mirror matches the
            // live-sync path before writing.
            super::decoder::enrich_receipts_from_txs(&mut receipts, &txs);

            // Pipeline: fetch next batch from PG while writing current batch to CH
            let next_fetch = async {
                if current > pg_max {
                    return Ok(None);
                }
                let next_end = (current + BACKFILL_BLOCK_BATCH - 1).min(pg_max);
                let conn = self.pool.get().await?;
                let data = tokio::try_join!(
                    fetch_blocks(&conn, current, next_end),
                    fetch_txs(&conn, current, next_end),
                    fetch_logs(&conn, current, next_end),
                    fetch_receipts(&conn, current, next_end),
                )?;
                Ok::<_, anyhow::Error>(Some((next_end, data)))
            };

            let ch_write = async {
                tokio::try_join!(
                    async {
                        if !blocks.is_empty() {
                            ch.write_blocks(&blocks).await
                        } else {
                            Ok(())
                        }
                    },
                    async {
                        if !txs.is_empty() {
                            ch.write_txs(&txs).await
                        } else {
                            Ok(())
                        }
                    },
                    async {
                        if !logs.is_empty() {
                            ch.write_logs(&logs).await
                        } else {
                            Ok(())
                        }
                    },
                    async {
                        if !receipts.is_empty() {
                            ch.write_receipts(&receipts).await
                        } else {
                            Ok(())
                        }
                    },
                )
            };

            let (next_data, _) = tokio::try_join!(next_fetch, ch_write)?;

            // Advance cursor only after all tables written successfully
            save_ch_backfill_cursor(&self.pool, chain_id, batch_end).await?;

            blocks_written += block_count;
            let remaining = pg_max - batch_end;
            metrics::set_backfill_block(chain_id, "clickhouse", batch_end as u64);
            metrics::set_backfill_remaining(chain_id, "clickhouse", remaining as u64);

            if blocks_written % 100_000 < block_count {
                let pct = (((batch_end - from_block + 1) as f64 / total as f64) * 100.0) as u64;
                info!(
                    chain_id,
                    blocks_written, pct, batch_end, "ClickHouse backfill progress"
                );
            }

            if let Some(ref nd) = next_data {
                current = nd.0 + 1;
            }
            pending = next_data;
        }

        let elapsed = start.elapsed();
        if blocks_written > 0 {
            let rate = if elapsed.as_secs() > 0 {
                blocks_written as f64 / elapsed.as_secs_f64()
            } else {
                blocks_written as f64
            };
            info!(
                chain_id,
                blocks = blocks_written,
                elapsed_secs = elapsed.as_secs(),
                rate = format!("{rate:.0} blk/s"),
                "ClickHouse backfill complete"
            );
        }

        Ok(())
    }
}

/// Get the max block number in PostgreSQL, or None if empty.
async fn pg_max_block_num(pool: &Pool) -> Result<Option<i64>> {
    let conn = pool.get().await?;
    let row = conn.query_one("SELECT MAX(num) FROM blocks", &[]).await?;
    Ok(row.get::<_, Option<i64>>(0))
}

/// Load the CH backfill cursor for a chain. Returns 0 if no row exists.
async fn load_ch_backfill_cursor(pool: &Pool, chain_id: u64) -> Result<i64> {
    let conn = pool.get().await?;
    let row = conn
        .query_opt(
            "SELECT ch_backfill_block FROM sync_state WHERE chain_id = $1",
            &[&(chain_id as i64)],
        )
        .await?;
    Ok(row.map(|r| r.get::<_, i64>(0)).unwrap_or(0))
}

/// Save the CH backfill cursor for a chain (upsert, only advances).
async fn save_ch_backfill_cursor(pool: &Pool, chain_id: u64, block: i64) -> Result<()> {
    let conn = pool.get().await?;
    conn.execute(
        r#"
        INSERT INTO sync_state (chain_id, ch_backfill_block)
        VALUES ($1, $2)
        ON CONFLICT (chain_id) DO UPDATE SET
            ch_backfill_block = GREATEST(sync_state.ch_backfill_block, EXCLUDED.ch_backfill_block),
            updated_at = NOW()
        "#,
        &[&(chain_id as i64), &block],
    )
    .await?;
    Ok(())
}

// ── PG fetch functions (one query per batch, no cursors) ──────────────────

async fn fetch_blocks(
    conn: &deadpool_postgres::Object,
    from: i64,
    to: i64,
) -> Result<Vec<BlockRow>> {
    let rows = conn
        .query(
            "SELECT num, hash, parent_hash, timestamp, timestamp_ms, gas_limit, gas_used, miner, extra_data, consensus_proposer \
             FROM blocks WHERE num >= $1 AND num <= $2 ORDER BY num",
            &[&from, &to],
        )
        .await?;

    Ok(rows
        .iter()
        .map(|r| BlockRow {
            num: r.get(0),
            hash: r.get(1),
            parent_hash: r.get(2),
            timestamp: r.get(3),
            timestamp_ms: r.get(4),
            gas_limit: r.get(5),
            gas_used: r.get(6),
            miner: r.get(7),
            extra_data: r.get(8),
            consensus_proposer: r.get(9),
        })
        .collect())
}

async fn fetch_txs(conn: &deadpool_postgres::Object, from: i64, to: i64) -> Result<Vec<TxRow>> {
    let rows = conn
        .query(
            "SELECT block_num, block_timestamp, idx, hash, type, \"from\", \"to\", value, input, \
             gas_limit, max_fee_per_gas, max_priority_fee_per_gas, gas_used, \
             nonce_key, nonce, fee_token, fee_payer, call_count, \
             valid_before, valid_after, signature_type \
             FROM txs WHERE block_num >= $1 AND block_num <= $2 ORDER BY block_num, idx",
            &[&from, &to],
        )
        .await?;

    let mut txs: Vec<TxRow> = rows
        .iter()
        .map(|r| TxRow {
            block_num: r.get(0),
            block_timestamp: r.get(1),
            idx: r.get(2),
            hash: r.get(3),
            tx_type: r.get(4),
            from: r.get(5),
            to: r.get(6),
            value: r.get(7),
            input: r.get(8),
            gas_limit: r.get(9),
            max_fee_per_gas: r.get(10),
            max_priority_fee_per_gas: r.get(11),
            gas_used: r.get(12),
            nonce_key: r.get(13),
            nonce: r.get(14),
            fee_token: r.get(15),
            fee_payer: r.get(16),
            calls: Vec::new(),
            call_count: r.get(17),
            valid_before: r.get(18),
            valid_after: r.get(19),
            signature_type: r.get(20),
        })
        .collect();

    rebuild_aa_calls(conn, from, to, &mut txs).await?;

    Ok(txs)
}

/// Rebuild the typed inner-call array of AA txs so the ClickHouse `calls`
/// JSON column matches what live sync writes.
///
/// Multicall txs read their calls from `tx_calls`; single-call txs rebuild
/// the one call from the tx row itself, whose `to`/`value`/`input` mirror
/// calls[0] (the AA envelope derives them from the first call, and `value`
/// is the sum of a single call's value).
async fn rebuild_aa_calls(
    conn: &deadpool_postgres::Object,
    from: i64,
    to: i64,
    txs: &mut [TxRow],
) -> Result<()> {
    use std::collections::HashMap;

    let mut multicalls: HashMap<(i64, i32), Vec<Call>> = HashMap::new();
    let call_rows = conn
        .query(
            "SELECT block_num, tx_idx, \"to\", value, input FROM tx_calls \
             WHERE block_num >= $1 AND block_num <= $2 ORDER BY block_num, tx_idx, call_idx",
            &[&from, &to],
        )
        .await?;
    for r in &call_rows {
        multicalls
            .entry((r.get(0), r.get(1)))
            .or_default()
            .push(call_from_parts(r.get(2), r.get(3), r.get(4))?);
    }

    for tx in txs.iter_mut() {
        if tx.tx_type != TEMPO_TX_TYPE_ID as i16 {
            continue;
        }
        tx.calls = match tx.call_count {
            n if n > 1 => multicalls
                .remove(&(tx.block_num, tx.idx))
                .unwrap_or_default(),
            1 => vec![call_from_parts(
                tx.to.clone(),
                tx.value.clone(),
                tx.input.clone(),
            )?],
            _ => Vec::new(),
        };
    }

    Ok(())
}

/// Build a typed AA inner call from its stored parts (`to` bytes or NULL for
/// create, decimal-string value, raw input bytes).
fn call_from_parts(to: Option<Vec<u8>>, value: String, input: Vec<u8>) -> Result<Call> {
    use alloy::primitives::{Address, TxKind, U256};

    Ok(Call {
        to: match to {
            Some(bytes) => TxKind::Call(
                Address::try_from(bytes.as_slice())
                    .map_err(|e| anyhow::anyhow!("invalid tx_calls.to address: {e}"))?,
            ),
            None => TxKind::Create,
        },
        value: U256::from_str_radix(&value, 10)
            .map_err(|e| anyhow::anyhow!("invalid tx_calls.value {value:?}: {e}"))?,
        input: input.into(),
    })
}

async fn fetch_logs(conn: &deadpool_postgres::Object, from: i64, to: i64) -> Result<Vec<LogRow>> {
    let rows = conn
        .query(
            "SELECT block_num, block_timestamp, log_idx, tx_idx, tx_hash, address, \
             selector, topic0, topic1, topic2, topic3, data, is_virtual_forward \
             FROM logs WHERE block_num >= $1 AND block_num <= $2 ORDER BY block_num, log_idx",
            &[&from, &to],
        )
        .await?;

    Ok(rows
        .iter()
        .map(|r| LogRow {
            block_num: r.get(0),
            block_timestamp: r.get(1),
            log_idx: r.get(2),
            tx_idx: r.get(3),
            tx_hash: r.get(4),
            address: r.get(5),
            selector: r.get(6),
            topic0: r.get(7),
            topic1: r.get(8),
            topic2: r.get(9),
            topic3: r.get(10),
            data: r.get(11),
            is_virtual_forward: r.get(12),
        })
        .collect())
}

async fn fetch_receipts(
    conn: &deadpool_postgres::Object,
    from: i64,
    to: i64,
) -> Result<Vec<ReceiptRow>> {
    let rows = conn
        .query(
            "SELECT block_num, block_timestamp, tx_idx, tx_hash, \"from\", \"to\", \
             contract_address, gas_used, cumulative_gas_used, effective_gas_price, \
             status, fee_payer \
             FROM receipts WHERE block_num >= $1 AND block_num <= $2 ORDER BY block_num, tx_idx",
            &[&from, &to],
        )
        .await?;

    Ok(rows
        .iter()
        .map(|r| ReceiptRow {
            block_num: r.get(0),
            block_timestamp: r.get(1),
            tx_idx: r.get(2),
            tx_hash: r.get(3),
            from: r.get(4),
            to: r.get(5),
            contract_address: r.get(6),
            gas_used: r.get(7),
            cumulative_gas_used: r.get(8),
            effective_gas_price: r.get(9),
            status: r.get(10),
            fee_payer: r.get(11),
            // Postgres `receipts` has no type/fee_token; these are denormalized
            // from txs by `enrich_receipts_from_txs` before the ClickHouse write.
            tx_type: None,
            fee_token: None,
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::{Address, TxKind, U256};

    fn call_json(calls: &[Call]) -> String {
        serde_json::to_value(calls).unwrap().to_string()
    }

    /// Mirror of the writer's `insert_tx_calls` column derivation.
    fn to_parts(call: &Call) -> (Option<Vec<u8>>, String, Vec<u8>) {
        (
            match call.to {
                TxKind::Call(address) => Some(address.as_slice().to_vec()),
                TxKind::Create => None,
            },
            call.value.to_string(),
            call.input.to_vec(),
        )
    }

    #[test]
    fn call_round_trips_through_stored_parts() {
        let original = Call {
            to: TxKind::Call(Address::repeat_byte(0xab)),
            value: U256::from(123456789u64),
            input: vec![0xde, 0xad, 0xbe, 0xef].into(),
        };

        let (to, value, input) = to_parts(&original);
        let rebuilt = call_from_parts(to, value, input).unwrap();

        assert_eq!(rebuilt, original);
        assert_eq!(call_json(&[rebuilt]), call_json(&[original]));
    }

    #[test]
    fn create_call_round_trips_as_null_to() {
        let original = Call {
            to: TxKind::Create,
            value: U256::ZERO,
            input: vec![0x60, 0x80].into(),
        };

        let (to, value, input) = to_parts(&original);
        assert!(to.is_none());
        let rebuilt = call_from_parts(to, value, input).unwrap();

        assert_eq!(rebuilt, original);
    }

    #[test]
    fn rebuilt_json_matches_pre_normalization_format() {
        // Pin the exact JSON the ClickHouse `calls` column has always stored
        // (serde of tempo-alloy Call via serde_json::Value, declaration-order
        // keys — serde_json is built with preserve_order here).
        let call = Call {
            to: TxKind::Call(Address::repeat_byte(0x11)),
            value: U256::from(5u64),
            input: vec![0x01, 0x02].into(),
        };

        assert_eq!(
            call_json(&[call]),
            r#"[{"to":"0x1111111111111111111111111111111111111111","value":"0x5","input":"0x0102","data":null}]"#
        );
    }

    #[test]
    fn max_value_round_trips_via_decimal_string() {
        let original = Call {
            to: TxKind::Call(Address::ZERO),
            value: U256::MAX,
            input: Vec::new().into(),
        };

        let (to, value, input) = to_parts(&original);
        let rebuilt = call_from_parts(to, value, input).unwrap();

        assert_eq!(rebuilt, original);
    }
}
