use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tracing::{debug, error, info};

use crate::broadcast::{BlockUpdate, Broadcaster};
use crate::db::Pool;
use crate::metrics::{self, SyncProgress};
use crate::types::SyncState;

use super::decoder::{decode_block, decode_log, decode_receipt, decode_transaction, timestamp_from_secs};
use super::fetcher::RpcClient;
use super::writer::{
    detect_gaps, get_block_hash, load_sync_state, save_sync_state, write_block, write_blocks,
    write_logs, write_receipts, write_txs,
};

pub struct SyncEngine {
    pool: Pool,
    rpc: RpcClient,
    chain_id: u64,
    broadcaster: Option<Arc<Broadcaster>>,
}

impl SyncEngine {
    pub async fn new(pool: Pool, rpc_url: &str) -> Result<Self> {
        let rpc = RpcClient::new(rpc_url);
        let chain_id = rpc.chain_id().await?;

        info!(chain_id = chain_id, "Connected to chain");

        Ok(Self {
            pool,
            rpc,
            chain_id,
            broadcaster: None,
        })
    }

    pub fn with_broadcaster(mut self, broadcaster: Arc<Broadcaster>) -> Self {
        self.broadcaster = Some(broadcaster);
        self
    }

    pub async fn run(&mut self, mut shutdown: broadcast::Receiver<()>) -> Result<()> {
        let state = load_sync_state(&self.pool).await?.unwrap_or_default();
        let mut progress = SyncProgress::new(self.chain_id, state.synced_num);

        loop {
            tokio::select! {
                _ = shutdown.recv() => {
                    info!("Shutting down sync engine");
                    break;
                }
                result = self.tick_pipelined(&mut progress) => {
                    if let Err(e) = result {
                        error!(error = %e, "Sync tick failed");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        }
        Ok(())
    }

    /// Pipelined sync: fetch next batch while writing current batch
    /// This overlaps network I/O with database I/O for better throughput
    async fn tick_pipelined(&mut self, progress: &mut SyncProgress) -> Result<()> {
        let state = load_sync_state(&self.pool).await?.unwrap_or_default();
        let remote_head = self.rpc.latest_block_number().await?;

        // If first sync for this chain, start at head (backfill handles history)
        let synced = if state.chain_id == self.chain_id && state.synced_num > 0 {
            state.synced_num
        } else {
            remote_head.saturating_sub(1) // Start at head-1 so we sync head block
        };

        if synced >= remote_head {
            tokio::time::sleep(Duration::from_millis(500)).await;
            return Ok(());
        }

        const BATCH_SIZE: u64 = 10;
        let mut current_from = synced + 1;

        // Fetch first batch
        let mut current_to = (current_from + BATCH_SIZE - 1).min(remote_head);
        let mut current_fetch = Some(self.fetch_range(current_from, current_to).await?);

        while current_from <= remote_head {
            let (blocks, block_rows, all_txs, all_logs, all_receipts) = current_fetch.take().unwrap();

            // Start fetching next batch while we write current batch
            let next_from = current_to + 1;
            let next_to = (next_from + BATCH_SIZE - 1).min(remote_head);
            let has_next = next_from <= remote_head;

            let next_fetch_future = if has_next {
                Some(self.fetch_range(next_from, next_to))
            } else {
                None
            };

            // Write current batch (overlapped with next fetch)
            let write_future = async {
                write_blocks(&self.pool, &block_rows).await?;
                write_txs(&self.pool, &all_txs).await?;
                write_logs(&self.pool, &all_logs).await?;
                write_receipts(&self.pool, &all_receipts).await?;
                Ok::<_, anyhow::Error>(())
            };

            // Run fetch and write concurrently
            if let Some(fetch_fut) = next_fetch_future {
                let (write_result, fetch_result) = tokio::join!(write_future, fetch_fut);
                write_result?;
                current_fetch = Some(fetch_result?);
            } else {
                write_future.await?;
            }

            // Update sync state (preserve backfill_num)
            let new_state = SyncState {
                chain_id: self.chain_id,
                head_num: remote_head,
                synced_num: current_to,
                backfill_num: state.backfill_num,
            };
            save_sync_state(&self.pool, &new_state).await?;

            let block_count = blocks.len() as u64;
            let tx_count = all_txs.len() as u64;
            let log_count = all_logs.len() as u64;

            metrics::record_blocks_indexed(self.chain_id, block_count);
            metrics::record_txs_indexed(self.chain_id, tx_count);
            metrics::record_logs_indexed(self.chain_id, log_count);
            progress.report_forward(current_to, remote_head, block_count);

            // Broadcast once per batch (last block in batch)
            if let Some(ref broadcaster) = self.broadcaster {
                if let Some(last_block) = blocks.last() {
                    broadcaster.send(BlockUpdate {
                        chain_id: self.chain_id,
                        block_num: last_block.number_u64(),
                        block_hash: format!("0x{}", hex::encode(last_block.hash.0)),
                        tx_count: tx_count,
                        log_count: log_count,
                        timestamp: last_block.timestamp_u64() as i64,
                    });
                }
            }

            debug!(
                from = current_from,
                to = current_to,
                blocks = block_count,
                txs = tx_count,
                logs = log_count,
                "Wrote batch"
            );

            // Move to next batch
            current_from = next_from;
            current_to = next_to;
        }

        info!(
            synced = remote_head,
            lag = 0,
            "Caught up to head"
        );

        Ok(())
    }

    /// Validate parent hash chain for a batch of blocks
    /// Returns Ok(()) if chain is valid, Err with details if not
    async fn validate_parent_chain(&self, blocks: &[crate::tempo::TempoBlock]) -> Result<()> {
        if blocks.is_empty() {
            return Ok(());
        }

        let first_block = &blocks[0];
        let first_num = first_block.number_u64();

        // Check parent hash against stored block (if not genesis)
        if first_num > 0
            && let Some(stored_hash) = get_block_hash(&self.pool, first_num - 1).await?
        {
            let expected_parent: [u8; 32] = stored_hash
                .try_into()
                .map_err(|_| anyhow::anyhow!("Invalid stored hash length"))?;
            if first_block.parent_hash.0 != expected_parent {
                return Err(anyhow::anyhow!(
                    "Parent hash mismatch at block {}: expected {:?}, got {:?}",
                    first_num,
                    hex::encode(expected_parent),
                    hex::encode(first_block.parent_hash.0)
                ));
            }
        }

        // Validate internal chain continuity
        for window in blocks.windows(2) {
            if window[1].parent_hash != window[0].hash {
                return Err(anyhow::anyhow!(
                    "Internal chain break at block {}: parent_hash {:?} != prev hash {:?}",
                    window[1].number_u64(),
                    hex::encode(window[1].parent_hash.0),
                    hex::encode(window[0].hash.0)
                ));
            }
        }

        Ok(())
    }

    /// Detect and fill any gaps in the indexed block sequence
    pub async fn fill_gaps(&self) -> Result<usize> {
        let gaps = detect_gaps(&self.pool).await?;
        let mut filled = 0;

        for (start, end) in gaps {
            info!(from = start, to = end, "Filling gap");
            self.sync_range(start, end).await?;
            filled += (end - start + 1) as usize;
        }

        Ok(filled)
    }

    /// Fetch and decode a range of blocks (used by pipelined sync)
    async fn fetch_range(
        &self,
        from: u64,
        to: u64,
    ) -> Result<(
        Vec<crate::tempo::TempoBlock>,
        Vec<crate::types::BlockRow>,
        Vec<crate::types::TxRow>,
        Vec<crate::types::LogRow>,
        Vec<crate::types::ReceiptRow>,
    )> {
        let (blocks, receipts) = tokio::try_join!(
            self.rpc.get_blocks_batch(from..=to),
            self.rpc.get_receipts_batch(from..=to)
        )?;

        // Validate parent hash chain
        self.validate_parent_chain(&blocks).await?;

        let block_timestamps: HashMap<u64, _> = blocks
            .iter()
            .map(|b| (b.number_u64(), timestamp_from_secs(b.timestamp_u64())))
            .collect();

        let block_rows: Vec<_> = blocks.iter().map(decode_block).collect();

        let all_txs: Vec<_> = blocks
            .iter()
            .flat_map(|block| {
                block
                    .transactions()
                    .enumerate()
                    .map(|(i, tx)| decode_transaction(tx, block, i as u32))
            })
            .collect();

        let all_logs: Vec<_> = receipts
            .iter()
            .flatten()
            .flat_map(|receipt| {
                let block_num = receipt.block_number.to::<u64>();
                block_timestamps
                    .get(&block_num)
                    .map(|&ts| receipt.logs.iter().map(move |log| decode_log(log, ts)))
                    .into_iter()
                    .flatten()
            })
            .collect();

        let all_receipts: Vec<_> = receipts
            .iter()
            .flatten()
            .filter_map(|receipt| {
                let block_num = receipt.block_number.to::<u64>();
                block_timestamps.get(&block_num).map(|&ts| decode_receipt(receipt, ts))
            })
            .collect();

        Ok((blocks, block_rows, all_txs, all_logs, all_receipts))
    }

    pub async fn sync_range(&self, from: u64, to: u64) -> Result<()> {
        // Fetch blocks and receipts in parallel (receipts contain logs)
        let (blocks, receipts) = tokio::try_join!(
            self.rpc.get_blocks_batch(from..=to),
            self.rpc.get_receipts_batch(from..=to)
        )?;

        let block_timestamps: HashMap<u64, _> = blocks
            .iter()
            .map(|b| (b.number_u64(), timestamp_from_secs(b.timestamp_u64())))
            .collect();

        // Decode all blocks, transactions, and logs upfront
        let block_rows: Vec<_> = blocks.iter().map(decode_block).collect();

        let all_txs: Vec<_> = blocks
            .iter()
            .flat_map(|block| {
                block
                    .transactions()
                    .enumerate()
                    .map(|(i, tx)| decode_transaction(tx, block, i as u32))
            })
            .collect();

        let all_logs: Vec<_> = receipts
            .iter()
            .flatten()
            .flat_map(|receipt| {
                let block_num = receipt.block_number.to::<u64>();
                block_timestamps
                    .get(&block_num)
                    .map(|&ts| receipt.logs.iter().map(move |log| decode_log(log, ts)))
                    .into_iter()
                    .flatten()
            })
            .collect();

        let all_receipts: Vec<_> = receipts
            .iter()
            .flatten()
            .filter_map(|receipt| {
                let block_num = receipt.block_number.to::<u64>();
                block_timestamps.get(&block_num).map(|&ts| decode_receipt(receipt, ts))
            })
            .collect();

        // Batch write all data (single query per table)
        write_blocks(&self.pool, &block_rows).await?;
        write_txs(&self.pool, &all_txs).await?;
        write_logs(&self.pool, &all_logs).await?;
        write_receipts(&self.pool, &all_receipts).await?;

        Ok(())
    }

    pub async fn sync_block(&self, num: u64) -> Result<()> {
        let (block, receipts) = tokio::try_join!(
            self.rpc.get_block(num, true),
            self.rpc.get_block_receipts(num)
        )?;

        let block_row = decode_block(&block);
        let block_ts = timestamp_from_secs(block.timestamp_u64());
        write_block(&self.pool, &block_row).await?;

        let txs: Vec<_> = block
            .transactions()
            .enumerate()
            .map(|(i, tx)| decode_transaction(tx, &block, i as u32))
            .collect();

        write_txs(&self.pool, &txs).await?;

        // Extract logs from receipts
        let log_rows: Vec<_> = receipts
            .iter()
            .flat_map(|r| r.logs.iter().map(|log| decode_log(log, block_ts)))
            .collect();
        write_logs(&self.pool, &log_rows).await?;

        // Extract receipt rows
        let receipt_rows: Vec<_> = receipts
            .iter()
            .map(|r| decode_receipt(r, block_ts))
            .collect();
        write_receipts(&self.pool, &receipt_rows).await?;

        // Update sync state
        let state = load_sync_state(&self.pool).await?.unwrap_or_default();
        let new_state = SyncState {
            chain_id: self.chain_id,
            head_num: num,
            synced_num: num,
            backfill_num: state.backfill_num,
        };
        save_sync_state(&self.pool, &new_state).await?;

        Ok(())
    }

    /// Backfill blocks going backwards from a starting point toward genesis
    /// Returns the number of blocks synced
    pub async fn backfill(
        &self,
        from: u64,
        to: u64,
        batch_size: u64,
        mut shutdown: broadcast::Receiver<()>,
    ) -> Result<u64> {
        if from < to {
            return Err(anyhow::anyhow!(
                "Backfill requires from ({}) >= to ({})",
                from,
                to
            ));
        }

        let mut state = load_sync_state(&self.pool).await?.unwrap_or_default();
        
        // Determine starting point for backfill
        let start_block = match state.backfill_num {
            Some(n) if n > to => n.saturating_sub(1), // Resume from where we left off
            Some(n) if n <= to => {
                info!(backfill_num = n, "Backfill already complete to target");
                return Ok(0);
            }
            None => from, // First time, start from specified block
            _ => from,
        };

        if start_block < to {
            return Ok(0);
        }

        info!(
            from = start_block,
            to = to,
            batch_size = batch_size,
            "Starting backfill"
        );

        let mut synced = 0u64;
        let mut current_end = start_block;
        let mut progress = SyncProgress::new(self.chain_id, start_block);

        while current_end >= to {
            // Check for shutdown
            if shutdown.try_recv().is_ok() {
                info!(stopped_at = current_end, "Backfill interrupted by shutdown");
                break;
            }

            let current_start = current_end.saturating_sub(batch_size - 1).max(to);

            // Sync the range (going backwards, but sync_range handles forward ordering)
            self.sync_range(current_start, current_end).await?;

            let batch_blocks = current_end - current_start + 1;
            synced += batch_blocks;

            // Update state with new backfill position
            state.backfill_num = Some(current_start);
            if state.chain_id == 0 {
                state.chain_id = self.chain_id;
            }
            save_sync_state(&self.pool, &state).await?;

            metrics::record_blocks_indexed(self.chain_id, batch_blocks);
            progress.report_backfill(current_start, to, batch_blocks);

            if current_start == to {
                break;
            }
            current_end = current_start.saturating_sub(1);
        }

        // Mark complete if we reached genesis
        if state.backfill_num == Some(to) && to == 0 {
            info!("Backfill complete to genesis");
        }

        Ok(synced)
    }

    /// Get current sync status
    pub async fn status(&self) -> Result<SyncState> {
        let state = load_sync_state(&self.pool).await?.unwrap_or_default();
        Ok(state)
    }

    /// Get current chain head from RPC
    pub async fn get_head(&self) -> Result<u64> {
        self.rpc.latest_block_number().await
    }

    pub fn chain_id(&self) -> u64 {
        self.chain_id
    }

    pub fn pool(&self) -> &Pool {
        &self.pool
    }
}
