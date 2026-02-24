use anyhow::Result;
use tracing::{error, warn};

use crate::db::Pool;
use crate::metrics;
use crate::types::{BlockRow, LogRow, ReceiptRow, TxRow};

use super::ch_sink::ClickHouseSink;
use super::writer;

/// Fan-out writer that sends data to all configured sinks.
///
/// PostgreSQL is always present and authoritative — PG write failures are fatal.
/// ClickHouse is optional — CH write failures are logged and swallowed.
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
        // PG is authoritative — failures propagate
        writer::write_blocks(&self.pool, blocks).await?;

        // CH is best-effort
        if let Some(ch) = &self.ch {
            if let Err(e) = ch.write_blocks(blocks).await {
                error!(error = %e, "ClickHouse write_blocks failed (non-fatal)");
                metrics::record_sink_error(ch.name());
            }
        }

        Ok(())
    }

    pub async fn write_txs(&self, txs: &[TxRow]) -> Result<()> {
        writer::write_txs(&self.pool, txs).await?;

        if let Some(ch) = &self.ch {
            if let Err(e) = ch.write_txs(txs).await {
                error!(error = %e, "ClickHouse write_txs failed (non-fatal)");
                metrics::record_sink_error(ch.name());
            }
        }

        Ok(())
    }

    pub async fn write_logs(&self, logs: &[LogRow]) -> Result<()> {
        writer::write_logs(&self.pool, logs).await?;

        if let Some(ch) = &self.ch {
            if let Err(e) = ch.write_logs(logs).await {
                error!(error = %e, "ClickHouse write_logs failed (non-fatal)");
                metrics::record_sink_error(ch.name());
            }
        }

        Ok(())
    }

    pub async fn write_receipts(&self, receipts: &[ReceiptRow]) -> Result<()> {
        writer::write_receipts(&self.pool, receipts).await?;

        if let Some(ch) = &self.ch {
            if let Err(e) = ch.write_receipts(receipts).await {
                error!(error = %e, "ClickHouse write_receipts failed (non-fatal)");
                metrics::record_sink_error(ch.name());
            }
        }

        Ok(())
    }

    /// Delete all data from a given block number onwards (reorg support).
    /// Returns the number of blocks deleted from PostgreSQL.
    pub async fn delete_from(&self, block_num: u64) -> Result<u64> {
        let deleted = writer::delete_blocks_from(&self.pool, block_num).await?;

        if let Some(ch) = &self.ch {
            if let Err(e) = ch.delete_from(block_num).await {
                warn!(error = %e, block_num, "ClickHouse delete_from failed (non-fatal)");
                metrics::record_sink_error(ch.name());
            }
        }

        Ok(deleted)
    }
}
