use alloy::network::ReceiptResponse;
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
use super::replicator::ReplicatorHandle;
use super::writer::{
    detect_gaps, get_block_hash, load_sync_state, save_sync_state, update_synced_num,
    update_tip_num, write_block, write_blocks, write_logs, write_receipts, write_txs,
};

pub struct SyncEngine {
    pool: Pool,
    rpc: RpcClient,
    chain_id: u64,
    broadcaster: Option<Arc<Broadcaster>>,
    replicator: Option<ReplicatorHandle>,
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
            replicator: None,
        })
    }

    pub fn with_broadcaster(mut self, broadcaster: Arc<Broadcaster>) -> Self {
        self.broadcaster = Some(broadcaster);
        self
    }

    pub fn with_replicator(mut self, replicator: ReplicatorHandle) -> Self {
        self.replicator = Some(replicator);
        self
    }

    /// Run sync engine with two concurrent loops:
    /// - Realtime: always follows chain head immediately
    /// - Gap-fill: fills any gaps in background using detect_gaps
    pub async fn run(&mut self, shutdown: broadcast::Receiver<()>) -> Result<()> {
        let state = load_sync_state(&self.pool, self.chain_id).await?.unwrap_or_default();
        let mut realtime_progress = SyncProgress::new(self.chain_id, state.tip_num);

        let mut realtime_shutdown = shutdown.resubscribe();
        let gapfill_shutdown = shutdown.resubscribe();

        info!(
            chain_id = self.chain_id,
            tip_num = state.tip_num,
            synced_num = state.synced_num,
            "Starting sync engine with realtime + gap-fill"
        );

        // Spawn gap-fill as a separate background task
        let gapfill_pool = self.pool.clone();
        let gapfill_rpc = self.rpc.clone();
        let gapfill_chain_id = self.chain_id;
        let gapfill_handle = tokio::spawn(async move {
            run_gapfill_loop(gapfill_pool, gapfill_rpc, gapfill_chain_id, gapfill_shutdown).await
        });

        // Run realtime loop in foreground
        loop {
            tokio::select! {
                _ = realtime_shutdown.recv() => {
                    info!("Shutting down sync engine");
                    break;
                }
                result = self.tick_realtime(&mut realtime_progress) => {
                    if let Err(e) = result {
                        error!(error = %e, "Realtime sync tick failed");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        }

        // Wait for gap-fill to finish
        gapfill_handle.abort();
        Ok(())
    }

    /// Realtime sync: always follows chain head immediately
    /// Jumps to near head on startup, then follows new blocks with minimal lag
    /// Updates tip_num only, leaves synced_num for gap-fill
    async fn tick_realtime(&mut self, progress: &mut SyncProgress) -> Result<()> {
        let state = load_sync_state(&self.pool, self.chain_id).await?.unwrap_or_default();
        let remote_head = self.rpc.latest_block_number().await?;

        // TAIL_WINDOW: how many blocks behind head to start realtime sync
        // This ensures we're always near head, gap-fill handles the rest
        const TAIL_WINDOW: u64 = 10;

        // Jump to near head immediately, don't catch up sequentially
        let start_from = if state.tip_num >= remote_head.saturating_sub(TAIL_WINDOW) {
            // Already near head, continue from tip
            state.tip_num + 1
        } else {
            // Behind by more than TAIL_WINDOW, jump to near head
            // Gap-fill will handle the blocks we skip
            let jump_to = remote_head.saturating_sub(TAIL_WINDOW);
            if state.tip_num > 0 && jump_to > state.tip_num {
                info!(
                    old_tip = state.tip_num,
                    new_start = jump_to,
                    skipped = jump_to - state.tip_num,
                    "Realtime: jumping to near head, gap-fill will backfill"
                );
            }
            jump_to
        };

        if start_from > remote_head {
            progress.report_forward(state.tip_num, remote_head, 0);
            tokio::time::sleep(Duration::from_millis(100)).await;
            return Ok(());
        }

        const BATCH_SIZE: u64 = 10;
        let mut current_from = start_from;
        let mut current_to = (current_from + BATCH_SIZE - 1).min(remote_head);
        let mut current_fetch = Some(self.fetch_range(current_from, current_to).await?);

        while current_from <= remote_head {
            let (blocks, block_rows, all_txs, all_logs, all_receipts) = current_fetch.take().unwrap();

            let next_from = current_to + 1;
            let next_to = (next_from + BATCH_SIZE - 1).min(remote_head);
            let has_next = next_from <= remote_head;

            let next_fetch_future = if has_next {
                Some(self.fetch_range(next_from, next_to))
            } else {
                None
            };

            let replicator = self.replicator.clone();
            let block_rows_clone = block_rows.clone();
            let all_txs_clone = all_txs.clone();
            let all_logs_clone = all_logs.clone();
            let write_future = async {
                write_blocks(&self.pool, &block_rows).await?;
                write_txs(&self.pool, &all_txs).await?;
                write_logs(&self.pool, &all_logs).await?;
                write_receipts(&self.pool, &all_receipts).await?;

                if let Some(ref rep) = replicator {
                    rep.send_blocks(block_rows_clone).await.ok();
                    rep.send_txs(all_txs_clone).await.ok();
                    rep.send_logs(all_logs_clone).await.ok();
                }

                Ok::<_, anyhow::Error>(())
            };

            if let Some(fetch_fut) = next_fetch_future {
                let (write_result, fetch_result) = tokio::join!(write_future, fetch_fut);
                write_result?;
                current_fetch = Some(fetch_result?);
            } else {
                write_future.await?;
            }

            // Update only tip_num (partial update to avoid clobbering synced_num)
            update_tip_num(&self.pool, self.chain_id, current_to, remote_head).await?;

            let block_count = blocks.len() as u64;
            let tx_count = all_txs.len() as u64;
            let log_count = all_logs.len() as u64;

            metrics::record_blocks_indexed(self.chain_id, block_count);
            metrics::record_txs_indexed(self.chain_id, tx_count);
            metrics::record_logs_indexed(self.chain_id, log_count);
            progress.report_forward(current_to, remote_head, block_count);

            if let Some(ref broadcaster) = self.broadcaster {
                for block in &blocks {
                    broadcaster.send(BlockUpdate {
                        chain_id: self.chain_id,
                        block_num: block.header.number,
                        block_hash: format!("0x{}", hex::encode(block.header.hash)),
                        tx_count: block.transactions.len() as u64,
                        log_count: 0,
                        timestamp: block.header.timestamp as i64,
                    });
                }
            }

            debug!(
                from = current_from,
                to = current_to,
                blocks = block_count,
                txs = tx_count,
                logs = log_count,
                "Realtime: wrote batch"
            );

            current_from = next_from;
            current_to = next_to;
        }

        Ok(())
    }

    /// Validate parent hash chain for a batch of blocks
    /// Returns Ok(()) if chain is valid, Err with details if not
    async fn validate_parent_chain(&self, blocks: &[crate::tempo::Block]) -> Result<()> {
        if blocks.is_empty() {
            return Ok(());
        }

        let first_block = &blocks[0];
        let first_num = first_block.header.number;

        // Check parent hash against stored block (if not genesis)
        if first_num > 0
            && let Some(stored_hash) = get_block_hash(&self.pool, first_num - 1).await?
        {
            let expected_parent: [u8; 32] = stored_hash
                .try_into()
                .map_err(|_| anyhow::anyhow!("Invalid stored hash length"))?;
            if first_block.header.parent_hash.0 != expected_parent {
                return Err(anyhow::anyhow!(
                    "Parent hash mismatch at block {}: expected {:?}, got {:?}",
                    first_num,
                    hex::encode(expected_parent),
                    hex::encode(first_block.header.parent_hash.0)
                ));
            }
        }

        // Validate internal chain continuity
        for window in blocks.windows(2) {
            if window[1].header.parent_hash != window[0].header.hash {
                return Err(anyhow::anyhow!(
                    "Internal chain break at block {}: parent_hash {:?} != prev hash {:?}",
                    window[1].header.number,
                    hex::encode(window[1].header.parent_hash.0),
                    hex::encode(window[0].header.hash.0)
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
        Vec<crate::tempo::Block>,
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
            .map(|b| (b.header.number, timestamp_from_secs(b.header.timestamp)))
            .collect();

        let block_rows: Vec<_> = blocks.iter().map(decode_block).collect();

        let all_txs: Vec<_> = blocks
            .iter()
            .flat_map(|block| {
                block
                    .transactions
                    .txns()
                    .enumerate()
                    .map(|(i, tx)| decode_transaction(tx, block, i as u32))
            })
            .collect();

        let all_logs: Vec<_> = receipts
            .iter()
            .flatten()
            .flat_map(|receipt| {
                let block_num = receipt.block_number().unwrap_or(0);
                block_timestamps
                    .get(&block_num)
                    .map(|&ts| receipt.inner.logs().iter().map(move |log| decode_log(log, ts)))
                    .into_iter()
                    .flatten()
            })
            .collect();

        let all_receipts: Vec<_> = receipts
            .iter()
            .flatten()
            .filter_map(|receipt| {
                let block_num = receipt.block_number().unwrap_or(0);
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
            .map(|b| (b.header.number, timestamp_from_secs(b.header.timestamp)))
            .collect();

        // Decode all blocks, transactions, and logs upfront
        let block_rows: Vec<_> = blocks.iter().map(decode_block).collect();

        let all_txs: Vec<_> = blocks
            .iter()
            .flat_map(|block| {
                block
                    .transactions
                    .txns()
                    .enumerate()
                    .map(|(i, tx)| decode_transaction(tx, block, i as u32))
            })
            .collect();

        let all_logs: Vec<_> = receipts
            .iter()
            .flatten()
            .flat_map(|receipt| {
                let block_num = receipt.block_number().unwrap_or(0);
                block_timestamps
                    .get(&block_num)
                    .map(|&ts| receipt.inner.logs().iter().map(move |log| decode_log(log, ts)))
                    .into_iter()
                    .flatten()
            })
            .collect();

        let all_receipts: Vec<_> = receipts
            .iter()
            .flatten()
            .filter_map(|receipt| {
                let block_num = receipt.block_number().unwrap_or(0);
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
        let block_ts = timestamp_from_secs(block.header.timestamp);
        write_block(&self.pool, &block_row).await?;

        let txs: Vec<_> = block
            .transactions
            .txns()
            .enumerate()
            .map(|(i, tx)| decode_transaction(tx, &block, i as u32))
            .collect();

        write_txs(&self.pool, &txs).await?;

        // Extract logs from receipts
        let log_rows: Vec<_> = receipts
            .iter()
            .flat_map(|r| r.inner.logs().iter().map(|log| decode_log(log, block_ts)))
            .collect();
        write_logs(&self.pool, &log_rows).await?;

        // Extract receipt rows
        let receipt_rows: Vec<_> = receipts
            .iter()
            .map(|r| decode_receipt(r, block_ts))
            .collect();
        write_receipts(&self.pool, &receipt_rows).await?;

        // Update sync state
        let state = load_sync_state(&self.pool, self.chain_id).await?.unwrap_or_default();
        let new_state = SyncState {
            chain_id: self.chain_id,
            head_num: num,
            synced_num: num,
            tip_num: num,
            backfill_num: state.backfill_num,
            started_at: state.started_at,
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
                "Backfill requires from ({from}) >= to ({to})"
            ));
        }

        let mut state = load_sync_state(&self.pool, self.chain_id).await?.unwrap_or_default();
        
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
        let state = load_sync_state(&self.pool, self.chain_id).await?.unwrap_or_default();
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

/// Standalone gap-fill loop that runs in a separate task
/// Fills gaps detected in the blocks table
async fn run_gapfill_loop(
    pool: Pool,
    rpc: RpcClient,
    chain_id: u64,
    mut shutdown: broadcast::Receiver<()>,
) -> Result<()> {
    let state = load_sync_state(&pool, chain_id).await?.unwrap_or_default();
    let mut progress = SyncProgress::new(chain_id, state.synced_num);

    loop {
        tokio::select! {
            biased;

            _ = shutdown.recv() => {
                info!("Gap-fill: shutting down");
                break;
            }
            result = tick_gapfill(&pool, &rpc, chain_id, &mut progress) => {
                if let Err(e) = result {
                    error!(error = %e, "Gap-fill sync tick failed");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    Ok(())
}

/// Single tick of gap-fill: detect gaps and fill them
/// Handles both:
/// 1. Holes in the blocks table (from detect_gaps)
/// 2. Gap between synced_num and tip_num (from realtime jumping ahead)
async fn tick_gapfill(
    pool: &Pool,
    rpc: &RpcClient,
    chain_id: u64,
    progress: &mut SyncProgress,
) -> Result<()> {
    let state = load_sync_state(pool, chain_id).await?.unwrap_or_default();
    let all_gaps = detect_gaps(pool).await?;

    // Fill ALL gaps in the blocks table - don't filter by backfill_floor
    // Gaps can occur at any point (partial batch failures, race conditions)
    // and should be filled regardless of where backfill is
    let mut gaps: Vec<_> = all_gaps
        .into_iter()
        .filter(|(_start, end)| *end <= state.tip_num)
        .collect();

    // Also check for gap between synced_num and tip_num
    // This happens when realtime jumped ahead to follow head
    if state.tip_num > state.synced_num + 1 {
        let lag_gap_start = state.synced_num + 1;
        let lag_gap_end = state.tip_num.saturating_sub(1);
        if lag_gap_end >= lag_gap_start {
            let lag_gap = (lag_gap_start, lag_gap_end);
            if !gaps.iter().any(|(s, e)| *s <= lag_gap_start && *e >= lag_gap_end) {
                gaps.push(lag_gap);
                gaps.sort_by_key(|(s, _)| *s);
            }
        }
    }

    if gaps.is_empty() {
        // No gaps - update synced_num to match tip_num
        if state.synced_num < state.tip_num {
            update_synced_num(pool, chain_id, state.tip_num).await?;
            info!(synced_num = state.tip_num, "Gap-fill: fully caught up");
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
        return Ok(());
    }

    let total_gap_blocks: u64 = gaps.iter().map(|(s, e)| e - s + 1).sum();
    info!(
        gap_count = gaps.len(),
        total_blocks = total_gap_blocks,
        "Gap-fill: detected gaps to fill"
    );

    for (gap_start, gap_end) in gaps {
        let gap_size = gap_end - gap_start + 1;
        debug!(
            gap_start = gap_start,
            gap_end = gap_end,
            size = gap_size,
            "Gap-fill: filling gap"
        );

        // Fill gap in batches
        const BATCH_SIZE: u64 = 50;
        let mut current = gap_start;

        while current <= gap_end {
            let batch_end = (current + BATCH_SIZE - 1).min(gap_end);
            sync_range_standalone(pool, rpc, current, batch_end).await?;

            let batch_count = batch_end - current + 1;
            metrics::record_blocks_indexed(chain_id, batch_count);
            progress.report_backfill(current, gap_start, batch_count);

            debug!(
                from = current,
                to = batch_end,
                "Gap-fill: wrote batch"
            );

            current = batch_end + 1;
        }

        info!(
            gap_start = gap_start,
            gap_end = gap_end,
            "Gap-fill: closed gap"
        );
    }

    // After filling all gaps, update synced_num to the highest contiguous block
    let state = load_sync_state(pool, chain_id).await?.unwrap_or_default();
    let new_gaps = detect_gaps(pool).await?;
    if new_gaps.is_empty() && state.tip_num > 0 {
        update_synced_num(pool, chain_id, state.tip_num).await?;
    }

    Ok(())
}

/// Standalone sync_range for gap-fill (doesn't need SyncEngine self)
async fn sync_range_standalone(pool: &Pool, rpc: &RpcClient, from: u64, to: u64) -> Result<()> {
    use alloy::network::ReceiptResponse;
    use super::decoder::{decode_block, decode_log, decode_receipt, decode_transaction, timestamp_from_secs};

    let (blocks, receipts) = tokio::try_join!(
        rpc.get_blocks_batch(from..=to),
        rpc.get_receipts_batch(from..=to)
    )?;

    let block_timestamps: HashMap<u64, _> = blocks
        .iter()
        .map(|b| (b.header.number, timestamp_from_secs(b.header.timestamp)))
        .collect();

    let block_rows: Vec<_> = blocks.iter().map(decode_block).collect();

    let all_txs: Vec<_> = blocks
        .iter()
        .flat_map(|block| {
            block
                .transactions
                .txns()
                .enumerate()
                .map(|(i, tx)| decode_transaction(tx, block, i as u32))
        })
        .collect();

    let all_logs: Vec<_> = receipts
        .iter()
        .flatten()
        .flat_map(|receipt| {
            let block_num = receipt.block_number().unwrap_or(0);
            block_timestamps
                .get(&block_num)
                .map(|&ts| receipt.inner.logs().iter().map(move |log| decode_log(log, ts)))
                .into_iter()
                .flatten()
        })
        .collect();

    let all_receipts: Vec<_> = receipts
        .iter()
        .flatten()
        .filter_map(|receipt| {
            let block_num = receipt.block_number().unwrap_or(0);
            block_timestamps.get(&block_num).map(|&ts| decode_receipt(receipt, ts))
        })
        .collect();

    write_blocks(pool, &block_rows).await?;
    write_txs(pool, &all_txs).await?;
    write_logs(pool, &all_logs).await?;
    write_receipts(pool, &all_receipts).await?;

    Ok(())
}
