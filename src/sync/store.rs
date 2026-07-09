//! Sync-state store abstraction.
//!
//! The sync engine tracks its state (watermarks, gaps, reorg lookups) in
//! PostgreSQL when configured, otherwise directly in ClickHouse so chains can
//! run without a Postgres instance at all.

use std::collections::HashMap;

use anyhow::Result;
use chrono::{DateTime, Utc};

use crate::db::Pool;
use crate::types::SyncState;

use super::ch_sink::ClickHouseSink;
use super::fetcher::RpcClient;
use super::writer;

/// Which backend holds sync-engine state for a chain.
#[derive(Clone, Copy)]
pub enum SyncStore<'a> {
    Postgres(&'a Pool),
    ClickHouse(&'a ClickHouseSink),
}

impl SyncStore<'_> {
    /// Metrics label for this store.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Postgres(_) => "postgres",
            Self::ClickHouse(_) => "clickhouse",
        }
    }

    pub async fn load_sync_state(&self, chain_id: u64) -> Result<Option<SyncState>> {
        match self {
            Self::Postgres(pool) => writer::load_sync_state(pool, chain_id).await,
            Self::ClickHouse(sink) => sink.load_sync_state(chain_id).await,
        }
    }

    pub async fn save_sync_state(&self, state: &SyncState) -> Result<()> {
        match self {
            Self::Postgres(pool) => writer::save_sync_state(pool, state).await,
            Self::ClickHouse(sink) => sink.save_sync_state(state).await,
        }
    }

    pub async fn update_tip_num(&self, chain_id: u64, tip_num: u64, head_num: u64) -> Result<()> {
        match self {
            Self::Postgres(pool) => writer::update_tip_num(pool, chain_id, tip_num, head_num).await,
            Self::ClickHouse(sink) => sink.update_tip_num(chain_id, tip_num, head_num).await,
        }
    }

    pub async fn update_synced_num(&self, chain_id: u64, synced_num: u64) -> Result<()> {
        match self {
            Self::Postgres(pool) => writer::update_synced_num(pool, chain_id, synced_num).await,
            Self::ClickHouse(sink) => sink.update_synced_num(chain_id, synced_num).await,
        }
    }

    pub async fn update_sync_rate(&self, chain_id: u64, rate: f64) -> Result<()> {
        match self {
            Self::Postgres(pool) => writer::update_sync_rate(pool, chain_id, rate).await,
            Self::ClickHouse(sink) => sink.update_sync_rate(chain_id, rate).await,
        }
    }

    pub async fn get_block_hash(&self, block_num: u64) -> Result<Option<Vec<u8>>> {
        match self {
            Self::Postgres(pool) => writer::get_block_hash(pool, block_num).await,
            Self::ClickHouse(sink) => sink.get_block_hash(block_num).await,
        }
    }

    pub async fn has_gaps(&self, from: u64, to: u64) -> Result<bool> {
        match self {
            Self::Postgres(pool) => writer::has_gaps(pool, from, to).await,
            Self::ClickHouse(sink) => sink.has_gaps(from, to).await,
        }
    }

    pub async fn detect_all_gaps(&self, tip_num: u64) -> Result<Vec<(u64, u64)>> {
        match self {
            Self::Postgres(pool) => writer::detect_all_gaps(pool, tip_num).await,
            Self::ClickHouse(sink) => sink.detect_all_gaps(tip_num).await,
        }
    }

    pub async fn detect_blocks_missing_receipts(&self, limit: i64) -> Result<Vec<u64>> {
        match self {
            Self::Postgres(pool) => writer::detect_blocks_missing_receipts(pool, limit).await,
            Self::ClickHouse(sink) => sink.detect_blocks_missing_receipts(limit).await,
        }
    }

    /// Block timestamps for a range (used by receipt backfill decoding).
    pub async fn block_timestamps(
        &self,
        from: u64,
        to: u64,
    ) -> Result<HashMap<u64, DateTime<Utc>>> {
        match self {
            Self::Postgres(pool) => {
                let conn = pool.get().await?;
                let rows = conn
                    .query(
                        "SELECT num, timestamp FROM blocks WHERE num >= $1 AND num <= $2",
                        &[&(from as i64), &(to as i64)],
                    )
                    .await?;
                Ok(rows
                    .iter()
                    .map(|r| {
                        let num: i64 = r.get(0);
                        let ts: DateTime<Utc> = r.get(1);
                        (num as u64, ts)
                    })
                    .collect())
            }
            Self::ClickHouse(sink) => sink.block_timestamps(from, to).await,
        }
    }

    /// Denormalize receipt data onto txs after a receipt backfill.
    /// No-op on ClickHouse: its tx rows are enriched at write time, and
    /// re-writing existing rows would require mutations.
    pub async fn finalize_receipt_backfill(&self, from: u64, to: u64) -> Result<()> {
        match self {
            Self::Postgres(pool) => {
                let conn = pool.get().await?;
                conn.execute(
                    "UPDATE txs SET gas_used = r.gas_used, fee_payer = r.fee_payer \
                     FROM receipts r \
                     WHERE txs.block_num = r.block_num AND txs.idx = r.tx_idx \
                       AND txs.block_num >= $1 AND txs.block_num <= $2 \
                       AND txs.gas_used IS NULL",
                    &[&(from as i64), &(to as i64)],
                )
                .await?;
                Ok(())
            }
            Self::ClickHouse(_) => Ok(()),
        }
    }

    /// Find the fork point by walking back from a mismatch until a stored hash
    /// matches the canonical chain. Returns None if no match within max_depth.
    pub async fn find_fork_point(
        &self,
        rpc: &RpcClient,
        mismatch_block: u64,
        max_depth: u64,
    ) -> Result<Option<u64>> {
        let min_block = mismatch_block.saturating_sub(max_depth).max(1);

        for block_num in (min_block..mismatch_block).rev() {
            let stored_hash = self.get_block_hash(block_num).await?;

            if let Some(stored) = stored_hash {
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
}
