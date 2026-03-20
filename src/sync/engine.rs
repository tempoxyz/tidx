use alloy::network::ReceiptResponse;
use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tracing::{debug, error, info};

use crate::broadcast::{BlockUpdate, Broadcaster};
use crate::db::{Pool, ThrottledPool};
use crate::metrics::{self, SyncProgress};
use crate::types::SyncState;

use super::decoder::{decode_block, decode_log, decode_receipt, decode_transaction, enrich_txs_from_receipts, timestamp_from_secs};
use super::fetcher::RpcClient;
use super::sink::SinkSet;
use super::writer::{
    detect_all_gaps, detect_blocks_missing_receipts, find_fork_point,
    get_block_hash, load_sync_state, save_sync_state, update_sync_rate, update_synced_num,
    update_tip_num,
};

/// RPC concurrency limits
const REALTIME_RPC_CONCURRENCY: usize = 4;
const BACKFILL_RPC_CONCURRENCY: usize = 8;

pub struct SyncEngine {
    /// Throttled pool - shared by all, but backfill is rate-limited
    throttled_pool: ThrottledPool,
    /// Fan-out writer for all configured sinks (PG, and later CH)
    sinks: SinkSet,
    /// RPC client for realtime sync (guaranteed capacity)
    realtime_rpc: RpcClient,
    /// RPC client for backfill (separate limit, can't starve realtime)
    backfill_rpc: RpcClient,
    chain_id: u64,
    broadcaster: Option<Arc<Broadcaster>>,
    batch_size: u64,
    concurrency: usize,
    backfill_first: bool,
    /// Skip parent hash validation (trust RPC for reorg handling)
    trust_rpc: bool,
}

impl SyncEngine {
    /// Creates a sync engine with a throttled pool and pre-configured sinks.
    /// Uses separate RPC clients for realtime vs backfill to guarantee capacity.
    pub async fn new(throttled_pool: ThrottledPool, sinks: SinkSet, rpc_url: &str) -> Result<Self> {
        let realtime_rpc = RpcClient::with_concurrency(rpc_url, REALTIME_RPC_CONCURRENCY);
        let backfill_rpc = RpcClient::with_concurrency(rpc_url, BACKFILL_RPC_CONCURRENCY);
        let chain_id = realtime_rpc.chain_id().await?;

        info!(
            chain_id = chain_id,
            realtime_rpc_limit = REALTIME_RPC_CONCURRENCY,
            backfill_rpc_limit = BACKFILL_RPC_CONCURRENCY,
            "Connected to chain (split RPC clients)"
        );

        Ok(Self {
            throttled_pool,
            sinks,
            realtime_rpc,
            backfill_rpc,
            chain_id,
            broadcaster: None,
            batch_size: 100,
            concurrency: 4,
            backfill_first: false,
            trust_rpc: false,
        })
    }

    pub fn with_batch_size(mut self, batch_size: u64) -> Self {
        self.batch_size = batch_size;
        self
    }

    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency.max(1);
        self
    }

    pub fn with_broadcaster(mut self, broadcaster: Arc<Broadcaster>) -> Self {
        self.broadcaster = Some(broadcaster);
        self
    }

    pub fn with_backfill_first(mut self, backfill_first: bool) -> Self {
        self.backfill_first = backfill_first;
        self
    }

    pub fn with_trust_rpc(mut self, trust_rpc: bool) -> Self {
        self.trust_rpc = trust_rpc;
        self
    }

    /// Returns the underlying pool (for realtime/API operations).
    fn pool(&self) -> &Pool {
        self.throttled_pool.inner()
    }

    /// Returns the backfill semaphore for throttled operations.
    fn backfill_semaphore(&self) -> &std::sync::Arc<tokio::sync::Semaphore> {
        &self.throttled_pool.backfill_semaphore
    }

    /// Run sync engine with two concurrent loops:
    /// - Realtime: always follows chain head immediately
    /// - Gap-fill: fills any gaps in background using detect_gaps
    ///
    /// If backfill_first is true, completes all backfill before starting realtime.
    pub async fn run(&mut self, shutdown: broadcast::Receiver<()>) -> Result<()> {
        if self.backfill_first {
            self.run_backfill_first(shutdown).await
        } else {
            self.run_concurrent(shutdown).await
        }
    }

    /// Run backfill to completion, then switch to realtime sync.
    async fn run_backfill_first(&mut self, shutdown: broadcast::Receiver<()>) -> Result<()> {
        let state = load_sync_state(self.pool(), self.chain_id).await?.unwrap_or_default();
        let mut progress = SyncProgress::new(self.chain_id, state.synced_num);
        let mut shutdown_rx = shutdown.resubscribe();

        info!(
            chain_id = self.chain_id,
            tip_num = state.tip_num,
            synced_num = state.synced_num,
            "Starting sync engine in backfill-first mode"
        );

        // Phase 1: Complete all backfill
        loop {
            // Check for shutdown
            if shutdown_rx.try_recv().is_ok() {
                info!("Shutting down during backfill");
                return Ok(());
            }

            // Get current head to know our target
            let remote_head = self.realtime_rpc.latest_block_number().await?;
            update_tip_num(self.pool(), self.chain_id, remote_head, remote_head).await?;

            // Check for gaps
            let gaps = detect_all_gaps(self.pool(), remote_head).await?;
            if gaps.is_empty() {
                info!(
                    chain_id = self.chain_id,
                    head = remote_head,
                    "Backfill complete, switching to realtime sync"
                );
                break;
            }

            let total_gap_blocks: u64 = gaps.iter().map(|(s, e)| e - s + 1).sum();
            info!(
                chain_id = self.chain_id,
                gaps = gaps.len(),
                total_blocks = total_gap_blocks,
                head = remote_head,
                "Backfill in progress"
            );

            // Run one round of gap-fill (uses backfill RPC client)
            if let Err(e) = tick_gapfill_parallel_no_throttle(
                &self.sinks,
                &self.backfill_rpc,
                self.chain_id,
                self.batch_size,
                self.concurrency,
                &mut progress,
            )
            .await
            {
                error!(error = %e, "Backfill tick failed");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }

        // Phase 2: Run realtime sync (no gap-fill needed)
        let mut realtime_progress = SyncProgress::new(self.chain_id, state.tip_num);
        info!(chain_id = self.chain_id, "Starting realtime sync");

        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => {
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

        Ok(())
    }

    /// Run realtime, gap-fill, and receipt backfill concurrently (default mode).
    async fn run_concurrent(&mut self, shutdown: broadcast::Receiver<()>) -> Result<()> {
        let state = load_sync_state(self.pool(), self.chain_id).await?.unwrap_or_default();
        let mut realtime_progress = SyncProgress::new(self.chain_id, state.tip_num);

        let mut realtime_shutdown = shutdown.resubscribe();
        let gapfill_shutdown = shutdown.resubscribe();
        let receipt_shutdown = shutdown.resubscribe();

        info!(
            chain_id = self.chain_id,
            tip_num = state.tip_num,
            synced_num = state.synced_num,
            trust_rpc = self.trust_rpc,
            "Starting sync engine with realtime + gap-fill + receipt backfill"
        );

        // Spawn gap-fill as a separate background task (throttled by semaphore)
        let gapfill_sinks = self.sinks.clone();
        let gapfill_semaphore = self.backfill_semaphore().clone();
        let gapfill_rpc = self.backfill_rpc.clone();
        let gapfill_chain_id = self.chain_id;
        let gapfill_batch_size = self.batch_size;
        let gapfill_concurrency = self.concurrency;
        let gapfill_handle = tokio::spawn(async move {
            run_gapfill_loop(
                gapfill_sinks,
                gapfill_semaphore,
                gapfill_rpc,
                gapfill_chain_id,
                gapfill_batch_size,
                gapfill_concurrency,
                gapfill_shutdown,
            )
            .await
        });

        // Spawn receipt backfill as a separate background task
        // This fills in receipts/logs for blocks that were synced without them
        let receipt_sinks = self.sinks.clone();
        let receipt_rpc = self.backfill_rpc.clone();
        let receipt_chain_id = self.chain_id;
        let receipt_handle = tokio::spawn(async move {
            run_receipt_backfill_loop(
                receipt_sinks,
                receipt_rpc,
                receipt_chain_id,
                receipt_shutdown,
            )
            .await
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

        // Abort background tasks
        gapfill_handle.abort();
        receipt_handle.abort();
        Ok(())
    }

    /// Realtime sync: follows chain head with fast tip advancement.
    /// 
    /// Strategy: Write blocks + txs immediately to move tip forward fast.
    /// Receipts/logs are backfilled asynchronously by a separate task.
    /// This decouples tip advancement from slow receipt RPC calls.
    async fn tick_realtime(&mut self, progress: &mut SyncProgress) -> Result<()> {
        let state = load_sync_state(self.pool(), self.chain_id).await?.unwrap_or_default();
        let remote_head = self.realtime_rpc.latest_block_number().await?;

        // TAIL_WINDOW: how many blocks behind head to start realtime sync
        const TAIL_WINDOW: u64 = 10;

        // Jump to near head immediately, don't catch up sequentially
        let start_from = if state.tip_num >= remote_head.saturating_sub(TAIL_WINDOW) {
            state.tip_num + 1
        } else {
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

        // Fast path: fetch blocks only (no receipts) for tip advancement
        let fetch_start = std::time::Instant::now();
        let mut current_fetch = Some(self.fetch_blocks_only(current_from, current_to).await?);
        let initial_fetch_ms = fetch_start.elapsed().as_millis();
        if initial_fetch_ms > 1000 {
            tracing::warn!(
                chain_id = self.chain_id,
                fetch_ms = initial_fetch_ms,
                from = current_from,
                to = current_to,
                "Slow initial block fetch"
            );
        }

        while current_from <= remote_head {
            let batch_start = std::time::Instant::now();
            let (blocks, block_rows, all_txs) = current_fetch.take().unwrap();
            let tx_count = all_txs.len() as u64;

            let next_from = current_to + 1;
            let next_to = (next_from + BATCH_SIZE - 1).min(remote_head);
            let has_next = next_from <= remote_head;

            // Pipeline: fetch next batch while writing current
            let next_fetch_future = if has_next {
                Some(self.fetch_blocks_only(next_from, next_to))
            } else {
                None
            };

            let sinks = self.sinks.clone();
            let write_future = async move {
                let write_start = std::time::Instant::now();
                sinks.write_blocks(&block_rows).await?;
                sinks.write_txs(&all_txs).await?;
                let write_ms = write_start.elapsed().as_millis();
                Ok::<_, anyhow::Error>(write_ms)
            };

            let write_ms;
            let fetch_ms;
            if let Some(fetch_fut) = next_fetch_future {
                let fetch_start = std::time::Instant::now();
                let (write_result, fetch_result) = tokio::join!(write_future, fetch_fut);
                write_ms = write_result?;
                fetch_ms = fetch_start.elapsed().as_millis();
                current_fetch = Some(fetch_result?);
            } else {
                write_ms = write_future.await?;
                fetch_ms = 0;
            }

            // Update tip_num (receipts will be backfilled by receipt_backfill task)
            update_tip_num(self.pool(), self.chain_id, current_to, remote_head).await?;

            let batch_ms = batch_start.elapsed().as_millis();
            let block_count = blocks.len();
            if batch_ms > 2000 {
                tracing::warn!(
                    chain_id = self.chain_id,
                    batch_ms,
                    write_ms,
                    fetch_ms,
                    blocks = block_count,
                    from = current_from,
                    to = current_to,
                    "Slow realtime batch"
                );
            }

            let block_count = blocks.len() as u64;
            metrics::record_blocks_indexed(self.chain_id, block_count);
            metrics::record_txs_indexed(self.chain_id, tx_count);
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
                "Realtime: wrote blocks+txs (receipts deferred)"
            );

            current_from = next_from;
            current_to = next_to;
        }

        Ok(())
    }

    /// Validate parent hash chain for a batch of blocks.
    /// Returns Ok(()) if chain is valid, Err(ReorgDetected { block }) if a reorg is detected.
    /// Skipped entirely if trust_rpc is enabled.
    async fn validate_parent_chain(&self, blocks: &[crate::tempo::Block]) -> Result<()> {
        if blocks.is_empty() || self.trust_rpc {
            return Ok(());
        }

        let first_block = &blocks[0];
        let first_num = first_block.header.number;

        // Check parent hash against stored block (if not genesis)
        if first_num > 0
            && let Some(stored_hash) = get_block_hash(self.pool(), first_num - 1).await?
        {
            let expected_parent: [u8; 32] = stored_hash
                .try_into()
                .map_err(|_| anyhow::anyhow!("Invalid stored hash length"))?;
            if first_block.header.parent_hash.0 != expected_parent {
                // Reorg detected - handle it automatically
                return self.handle_reorg(first_num).await;
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

    /// Handle a chain reorganization by finding the fork point and deleting orphaned blocks.
    /// After this, the next sync tick will re-fetch the canonical chain.
    async fn handle_reorg(&self, mismatch_block: u64) -> Result<()> {
        const MAX_REORG_DEPTH: u64 = 128;

        info!(
            chain_id = self.chain_id,
            mismatch_block,
            "Reorg detected, finding fork point"
        );

        // Find where the chain diverged
        let fork_point = find_fork_point(
            self.pool(),
            &self.realtime_rpc,
            mismatch_block,
            MAX_REORG_DEPTH,
        )
        .await?;

        match fork_point {
            Some(fork_block) => {
                let delete_from = fork_block + 1;

                // Delete orphaned blocks from all sinks
                let deleted = self.sinks.delete_from(delete_from).await?;

                info!(
                    chain_id = self.chain_id,
                    fork_point = fork_block,
                    deleted_blocks = deleted,
                    "Reorg handled: deleted orphaned blocks"
                );

                // Update tip_num to fork point so realtime sync continues from there
                update_tip_num(self.pool(), self.chain_id, fork_block, fork_block).await?;

                Ok(())
            }
            None => {
                Err(anyhow::anyhow!(
                    "Could not find fork point within {} blocks of mismatch at block {}",
                    MAX_REORG_DEPTH,
                    mismatch_block
                ))
            }
        }
    }

    /// Detect and fill any gaps in the indexed block sequence
    pub async fn fill_gaps(&self) -> Result<usize> {
        let state = load_sync_state(self.pool(), self.chain_id).await?.unwrap_or_default();
        let gaps = detect_all_gaps(self.pool(), state.tip_num).await?;
        let mut filled = 0;

        for (start, end) in gaps {
            info!(from = start, to = end, "Filling gap");
            self.sync_range(start, end).await?;
            filled += (end - start + 1) as usize;
        }

        Ok(filled)
    }

    /// Fetch and decode blocks only (no receipts) - fast path for tip advancement
    async fn fetch_blocks_only(
        &self,
        from: u64,
        to: u64,
    ) -> Result<(Vec<crate::tempo::Block>, Vec<crate::types::BlockRow>, Vec<crate::types::TxRow>)>
    {
        let blocks = self.realtime_rpc.get_blocks_batch(from..=to).await?;

        // Validate parent hash chain
        self.validate_parent_chain(&blocks).await?;

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

        Ok((blocks, block_rows, all_txs))
    }

    /// Fetch and decode a range of blocks with receipts (full sync)
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
            self.realtime_rpc.get_blocks_batch(from..=to),
            self.realtime_rpc.get_receipts_batch(from..=to)
        )?;

        // Validate parent hash chain
        self.validate_parent_chain(&blocks).await?;

        let block_timestamps: HashMap<u64, _> = blocks
            .iter()
            .map(|b| (b.header.number, timestamp_from_secs(b.header.timestamp)))
            .collect();

        let block_rows: Vec<_> = blocks.iter().map(decode_block).collect();

        let mut all_txs: Vec<_> = blocks
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

        enrich_txs_from_receipts(&mut all_txs, &all_receipts);

        Ok((blocks, block_rows, all_txs, all_logs, all_receipts))
    }

    pub async fn sync_range(&self, from: u64, to: u64) -> Result<()> {
        let (_blocks, block_rows, all_txs, all_logs, all_receipts) =
            self.fetch_range(from, to).await?;

        tokio::try_join!(
            self.sinks.write_blocks(&block_rows),
            self.sinks.write_txs(&all_txs),
            self.sinks.write_logs(&all_logs),
            self.sinks.write_receipts(&all_receipts),
        )?;

        Ok(())
    }

    pub async fn sync_block(&self, num: u64) -> Result<()> {
        let (block, receipts) = tokio::try_join!(
            self.realtime_rpc.get_block(num, true),
            self.realtime_rpc.get_block_receipts(num)
        )?;

        let block_row = decode_block(&block);
        let block_ts = timestamp_from_secs(block.header.timestamp);
        let mut txs: Vec<_> = block
            .transactions
            .txns()
            .enumerate()
            .map(|(i, tx)| decode_transaction(tx, &block, i as u32))
            .collect();

        let log_rows: Vec<_> = receipts
            .iter()
            .flat_map(|r| r.inner.logs().iter().map(|log| decode_log(log, block_ts)))
            .collect();

        let receipt_rows: Vec<_> = receipts
            .iter()
            .map(|r| decode_receipt(r, block_ts))
            .collect();

        enrich_txs_from_receipts(&mut txs, &receipt_rows);

        tokio::try_join!(
            self.sinks.write_blocks(std::slice::from_ref(&block_row)),
            self.sinks.write_txs(&txs),
            self.sinks.write_logs(&log_rows),
            self.sinks.write_receipts(&receipt_rows),
        )?;

        // Update sync state
        let state = load_sync_state(self.pool(), self.chain_id).await?.unwrap_or_default();
        let new_state = SyncState {
            chain_id: self.chain_id,
            head_num: num,
            synced_num: num,
            tip_num: num,
            backfill_num: state.backfill_num,
            sync_rate: state.sync_rate,
            started_at: state.started_at,
        };
        save_sync_state(self.pool(), &new_state).await?;

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

        let mut state = load_sync_state(self.pool(), self.chain_id).await?.unwrap_or_default();
        
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
            save_sync_state(self.pool(), &state).await?;

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
        let state = load_sync_state(self.pool(), self.chain_id).await?.unwrap_or_default();
        Ok(state)
    }

    /// Get current chain head from RPC
    pub async fn get_head(&self) -> Result<u64> {
        self.realtime_rpc.latest_block_number().await
    }

    pub fn chain_id(&self) -> u64 {
        self.chain_id
    }
}

/// Standalone gap-fill loop that runs in a separate task
/// Fills gaps detected in the blocks table using parallel workers
/// Throttled by semaphore to not starve realtime/API
#[allow(clippy::too_many_arguments)]
async fn run_gapfill_loop(
    sinks: SinkSet,
    backfill_semaphore: Arc<tokio::sync::Semaphore>,
    rpc: RpcClient,
    chain_id: u64,
    batch_size: u64,
    concurrency: usize,
    mut shutdown: broadcast::Receiver<()>,
) -> Result<()> {
    let state = load_sync_state(sinks.pool(), chain_id).await?.unwrap_or_default();
    let mut progress = SyncProgress::new(chain_id, state.synced_num);

    info!(
        chain_id = chain_id,
        batch_size = batch_size,
        concurrency = concurrency,
        backfill_limit = backfill_semaphore.available_permits(),
        "Gap-fill: starting with parallel workers (throttled)"
    );

    loop {
        tokio::select! {
            biased;

            _ = shutdown.recv() => {
                info!("Gap-fill: shutting down");
                break;
            }
            result = tick_gapfill_parallel(&sinks, &backfill_semaphore, &rpc, chain_id, batch_size, concurrency, &mut progress) => {
                if let Err(e) = result {
                    error!(error = %e, "Gap-fill sync tick failed");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    Ok(())
}

/// Parallel gap-fill: spawns N concurrent workers to fetch and write block ranges
/// Workers are throttled by semaphore to not starve realtime/API
#[allow(clippy::too_many_arguments)]
async fn tick_gapfill_parallel(
    sinks: &SinkSet,
    backfill_semaphore: &Arc<tokio::sync::Semaphore>,
    rpc: &RpcClient,
    chain_id: u64,
    batch_size: u64,
    concurrency: usize,
    progress: &mut SyncProgress,
) -> Result<()> {
    let pool = sinks.pool();
    let state = load_sync_state(pool, chain_id).await?.unwrap_or_default();

    // Adaptive throttling: pause backfill when realtime lag is high
    // This ensures realtime sync always has priority
    let remote_head = rpc.latest_block_number().await.unwrap_or(state.tip_num);
    let realtime_lag = remote_head.saturating_sub(state.tip_num);
    
    const LAG_THRESHOLD: u64 = 10; // Pause backfill if lag exceeds this
    if realtime_lag > LAG_THRESHOLD {
        debug!(
            chain_id = chain_id,
            realtime_lag = realtime_lag,
            threshold = LAG_THRESHOLD,
            "Gap-fill: pausing to let realtime sync catch up"
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
        return Ok(());
    }

    // Detect ALL gaps including from genesis, sorted by end DESC (most recent first)
    let gaps = detect_all_gaps(pool, state.tip_num).await?;

    if gaps.is_empty() {
        // No gaps - fully synced from genesis to tip
        metrics::set_gap_blocks(chain_id, "postgres", 0);
        metrics::set_gap_count(chain_id, "postgres", 0);
        metrics::set_synced(chain_id, realtime_lag == 0);
        if state.synced_num < state.tip_num {
            update_synced_num(pool, chain_id, state.tip_num).await?;
            info!(synced_num = state.tip_num, "Gap sync: fully synced");
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
        return Ok(());
    }

    let total_gap_blocks: u64 = gaps.iter().map(|(s, e)| e - s + 1).sum();
    let gap_count = gaps.len();
    metrics::set_gap_blocks(chain_id, "postgres", total_gap_blocks);
    metrics::set_gap_count(chain_id, "postgres", gap_count as u64);
    metrics::set_synced(chain_id, false);

    // Collect all batch ranges to process (from most recent gaps first)
    let mut batch_ranges: Vec<(u64, u64)> = Vec::new();
    for (gap_start, gap_end) in &gaps {
        let mut current_end = *gap_end;
        while current_end >= *gap_start {
            let current_start = current_end.saturating_sub(batch_size - 1).max(*gap_start);
            batch_ranges.push((current_start, current_end));
            if current_start == *gap_start {
                break;
            }
            current_end = current_start.saturating_sub(1);
        }
    }

    let total_batches = batch_ranges.len();
    debug!(
        gap_count = gap_count,
        total_blocks = total_gap_blocks,
        total_batches = total_batches,
        concurrency = concurrency,
        available_permits = backfill_semaphore.available_permits(),
        "Gap sync: processing with parallel workers (throttled)"
    );

    metrics::set_backfill_remaining(chain_id, "postgres", total_gap_blocks);

    // Process batches with N concurrent workers using JoinSet
    // Each worker acquires a semaphore permit before getting a DB connection
    let mut join_set = tokio::task::JoinSet::new();
    let mut batch_iter = batch_ranges.into_iter();
    let mut completed = 0u64;
    let mut lowest_block = u64::MAX;
    let tick_start = std::time::Instant::now();

    // Seed initial concurrent tasks (limited by both concurrency and semaphore)
    for _ in 0..concurrency {
        if let Some((start, end)) = batch_iter.next() {
            let sinks = sinks.clone();
            let rpc = rpc.clone();
            let sem = backfill_semaphore.clone();
            join_set.spawn(async move {
                // Acquire semaphore permit before doing work (throttles backfill)
                let _permit = match sem.acquire().await {
                    Ok(p) => p,
                    Err(_) => return (start, end, Err(anyhow::anyhow!("Backfill semaphore closed"))),
                };
                let result = sync_range_standalone(&sinks, &rpc, start, end).await;
                (start, end, result)
            });
        }
    }

    // Process results and spawn new tasks as workers complete
    let mut last_lag_check = std::time::Instant::now();
    while let Some(join_result) = join_set.join_next().await {
        // Check lag every 5 seconds during backfill - abort early if realtime is falling behind
        if last_lag_check.elapsed().as_secs() >= 5 {
            last_lag_check = std::time::Instant::now();
            if let Ok(current_head) = rpc.latest_block_number().await {
                let current_state = load_sync_state(pool, chain_id).await.ok().flatten().unwrap_or_default();
                let current_lag = current_head.saturating_sub(current_state.tip_num);
                if current_lag > LAG_THRESHOLD {
                    info!(
                        chain_id = chain_id,
                        lag = current_lag,
                        completed = completed,
                        "Gap-fill: aborting round early to let realtime catch up"
                    );
                    join_set.abort_all();
                    break;
                }
            }
        }

        let (start, end, result) = join_result?;
        match result {
            Ok(()) => {
                let batch_count = end - start + 1;
                completed += batch_count;
                lowest_block = lowest_block.min(start);
                metrics::record_blocks_indexed(chain_id, batch_count);
                metrics::set_backfill_remaining(chain_id, "postgres", total_gap_blocks.saturating_sub(completed));
                progress.report_backfill(completed, total_gap_blocks, batch_count);

                debug!(
                    from = start,
                    to = end,
                    completed = completed,
                    "Gap sync: wrote batch"
                );
            }
            Err(e) => {
                let error_str = e.to_string();
                let is_batch_too_large = error_str.contains("too large") 
                    || error_str.contains("response size exceeded");
                
                // If batch is too large and we can split it, do so
                if is_batch_too_large && end > start {
                    let mid = start + (end - start) / 2;
                    info!(
                        from = start,
                        to = end,
                        mid = mid,
                        "Gap sync: batch too large, splitting"
                    );
                    
                    // Queue first half
                    let sinks1 = sinks.clone();
                    let rpc1 = rpc.clone();
                    let sem1 = backfill_semaphore.clone();
                    join_set.spawn(async move {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        let _permit = match sem1.acquire().await {
                            Ok(p) => p,
                            Err(_) => return (start, mid, Err(anyhow::anyhow!("Backfill semaphore closed"))),
                        };
                        let result = sync_range_standalone(&sinks1, &rpc1, start, mid).await;
                        (start, mid, result)
                    });
                    
                    // Queue second half
                    let sinks2 = sinks.clone();
                    let rpc2 = rpc.clone();
                    let sem2 = backfill_semaphore.clone();
                    join_set.spawn(async move {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        let _permit = match sem2.acquire().await {
                            Ok(p) => p,
                            Err(_) => return (mid + 1, end, Err(anyhow::anyhow!("Backfill semaphore closed"))),
                        };
                        let result = sync_range_standalone(&sinks2, &rpc2, mid + 1, end).await;
                        (mid + 1, end, result)
                    });
                } else {
                    error!(
                        from = start,
                        to = end,
                        error = %e,
                        "Gap sync: batch failed, will retry"
                    );
                    // Re-queue the failed batch
                    let sinks = sinks.clone();
                    let rpc = rpc.clone();
                    let sem = backfill_semaphore.clone();
                    join_set.spawn(async move {
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        let _permit = match sem.acquire().await {
                            Ok(p) => p,
                            Err(_) => return (start, end, Err(anyhow::anyhow!("Backfill semaphore closed"))),
                        };
                        let result = sync_range_standalone(&sinks, &rpc, start, end).await;
                        (start, end, result)
                    });
                }
                continue;
            }
        }

        // Spawn next batch if available
        if let Some((start, end)) = batch_iter.next() {
            let sinks = sinks.clone();
            let rpc = rpc.clone();
            let sem = backfill_semaphore.clone();
            join_set.spawn(async move {
                let _permit = match sem.acquire().await {
                    Ok(p) => p,
                    Err(_) => return (start, end, Err(anyhow::anyhow!("Backfill semaphore closed"))),
                };
                let result = sync_range_standalone(&sinks, &rpc, start, end).await;
                (start, end, result)
            });
        }
    }

    // Calculate and save the sync rate
    let elapsed = tick_start.elapsed().as_secs_f64();
    let rate = if elapsed > 0.0 {
        completed as f64 / elapsed
    } else {
        0.0
    };

    if rate > 0.0 {
        update_sync_rate(pool, chain_id, rate).await.ok();
    }

    info!(
        completed = completed,
        gap_count = gap_count,
        lowest_block = lowest_block,
        rate = format!("{:.1} blk/s", rate),
        "Gap sync: completed round"
    );

    // Update backfill_num to track progress (lowest block we've reached)
    if lowest_block < u64::MAX {
        let mut updated_state = state.clone();
        updated_state.backfill_num = Some(lowest_block);
        save_sync_state(pool, &updated_state).await?;
    }

    Ok(())
}

/// Same as tick_gapfill_parallel but without lag throttling (for backfill-first mode)
async fn tick_gapfill_parallel_no_throttle(
    sinks: &SinkSet,
    rpc: &RpcClient,
    chain_id: u64,
    batch_size: u64,
    concurrency: usize,
    progress: &mut SyncProgress,
) -> Result<()> {
    let pool = sinks.pool();
    let state = load_sync_state(pool, chain_id).await?.unwrap_or_default();

    // Detect ALL gaps including from genesis, sorted by end DESC (most recent first)
    let gaps = detect_all_gaps(pool, state.tip_num).await?;

    if gaps.is_empty() {
        if state.synced_num < state.tip_num {
            update_synced_num(pool, chain_id, state.tip_num).await?;
            info!(synced_num = state.tip_num, "Backfill: fully synced");
        }
        return Ok(());
    }

    let total_gap_blocks: u64 = gaps.iter().map(|(s, e)| e - s + 1).sum();
    let gap_count = gaps.len();

    // Collect all batch ranges to process (from most recent gaps first)
    let mut batch_ranges: Vec<(u64, u64)> = Vec::new();
    for (gap_start, gap_end) in &gaps {
        let mut current_end = *gap_end;
        while current_end >= *gap_start {
            let current_start = current_end.saturating_sub(batch_size - 1).max(*gap_start);
            batch_ranges.push((current_start, current_end));
            if current_start == *gap_start {
                break;
            }
            current_end = current_start.saturating_sub(1);
        }
    }

    let total_batches = batch_ranges.len();
    debug!(
        gap_count = gap_count,
        total_blocks = total_gap_blocks,
        total_batches = total_batches,
        concurrency = concurrency,
        "Backfill: processing with parallel workers"
    );

    metrics::set_backfill_remaining(chain_id, "postgres", total_gap_blocks);

    // Process batches with N concurrent workers using JoinSet
    let mut join_set = tokio::task::JoinSet::new();
    let mut batch_iter = batch_ranges.into_iter();
    let mut completed = 0u64;
    let mut lowest_block = u64::MAX;
    let tick_start = std::time::Instant::now();

    // Seed initial concurrent tasks
    for _ in 0..concurrency {
        if let Some((start, end)) = batch_iter.next() {
            let sinks = sinks.clone();
            let rpc = rpc.clone();
            join_set.spawn(async move {
                let result = sync_range_standalone(&sinks, &rpc, start, end).await;
                (start, end, result)
            });
        }
    }

    // Process results and spawn new tasks as workers complete
    while let Some(join_result) = join_set.join_next().await {
        let (start, end, result) = join_result?;
        match result {
            Ok(()) => {
                let batch_count = end - start + 1;
                completed += batch_count;
                lowest_block = lowest_block.min(start);
                metrics::record_blocks_indexed(chain_id, batch_count);
                metrics::set_backfill_remaining(chain_id, "postgres", total_gap_blocks.saturating_sub(completed));
                progress.report_backfill(completed, total_gap_blocks, batch_count);

                debug!(
                    from = start,
                    to = end,
                    completed = completed,
                    "Backfill: wrote batch"
                );
            }
            Err(e) => {
                let error_str = e.to_string();
                let is_batch_too_large = error_str.contains("too large") 
                    || error_str.contains("response size exceeded");
                
                // If batch is too large and we can split it, do so
                if is_batch_too_large && end > start {
                    let mid = start + (end - start) / 2;
                    info!(
                        from = start,
                        to = end,
                        mid = mid,
                        "Backfill: batch too large, splitting"
                    );
                    
                    // Queue first half
                    let sinks1 = sinks.clone();
                    let rpc1 = rpc.clone();
                    join_set.spawn(async move {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        let result = sync_range_standalone(&sinks1, &rpc1, start, mid).await;
                        (start, mid, result)
                    });
                    
                    // Queue second half
                    let sinks2 = sinks.clone();
                    let rpc2 = rpc.clone();
                    join_set.spawn(async move {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        let result = sync_range_standalone(&sinks2, &rpc2, mid + 1, end).await;
                        (mid + 1, end, result)
                    });
                } else {
                    error!(
                        from = start,
                        to = end,
                        error = %e,
                        "Backfill: batch failed, will retry"
                    );
                    let sinks = sinks.clone();
                    let rpc = rpc.clone();
                    join_set.spawn(async move {
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        let result = sync_range_standalone(&sinks, &rpc, start, end).await;
                        (start, end, result)
                    });
                }
                continue;
            }
        }

        // Spawn next batch if available
        if let Some((start, end)) = batch_iter.next() {
            let sinks = sinks.clone();
            let rpc = rpc.clone();
            join_set.spawn(async move {
                let result = sync_range_standalone(&sinks, &rpc, start, end).await;
                (start, end, result)
            });
        }
    }

    // Calculate and save the sync rate
    let elapsed = tick_start.elapsed().as_secs_f64();
    let rate = if elapsed > 0.0 {
        completed as f64 / elapsed
    } else {
        0.0
    };

    if rate > 0.0 {
        update_sync_rate(pool, chain_id, rate).await.ok();
    }

    info!(
        completed = completed,
        gap_count = gap_count,
        lowest_block = lowest_block,
        rate = format!("{:.1} blk/s", rate),
        "Backfill: completed round"
    );

    // Update backfill_num to track progress
    if lowest_block < u64::MAX {
        let mut updated_state = state.clone();
        updated_state.backfill_num = Some(lowest_block);
        save_sync_state(pool, &updated_state).await?;
    }

    Ok(())
}

/// Check if fully synced (no gaps from genesis to tip)
#[allow(dead_code)]
async fn is_fully_synced(pool: &Pool, tip_num: u64) -> Result<bool> {
    let gaps = detect_all_gaps(pool, tip_num).await?;
    Ok(gaps.is_empty())
}

/// Standalone sync_range for gap-fill (doesn't need SyncEngine self)
async fn sync_range_standalone(
    sinks: &SinkSet,
    rpc: &RpcClient,
    from: u64,
    to: u64,
) -> Result<()> {
    use alloy::network::ReceiptResponse;
    use super::decoder::{decode_block, decode_log, decode_receipt, decode_transaction, enrich_txs_from_receipts, timestamp_from_secs};

    let (blocks, receipts) = tokio::try_join!(
        rpc.get_blocks_batch(from..=to),
        rpc.get_receipts_batch(from..=to)
    )?;

    let block_timestamps: HashMap<u64, _> = blocks
        .iter()
        .map(|b| (b.header.number, timestamp_from_secs(b.header.timestamp)))
        .collect();

    let block_rows: Vec<_> = blocks.iter().map(decode_block).collect();

    let mut all_txs: Vec<_> = blocks
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

    enrich_txs_from_receipts(&mut all_txs, &all_receipts);

    tokio::try_join!(
        sinks.write_blocks(&block_rows),
        sinks.write_txs(&all_txs),
        sinks.write_logs(&all_logs),
        sinks.write_receipts(&all_receipts),
    )?;

    Ok(())
}

/// Receipt backfill loop: fills in receipts/logs for blocks synced without them.
/// 
/// This runs as a background task and handles blocks where realtime sync wrote
/// blocks + txs but skipped receipts for faster tip advancement.
async fn run_receipt_backfill_loop(
    sinks: SinkSet,
    rpc: RpcClient,
    chain_id: u64,
    mut shutdown: broadcast::Receiver<()>,
) -> Result<()> {
    info!(chain_id, "Receipt backfill: starting");

    loop {
        tokio::select! {
            biased;

            _ = shutdown.recv() => {
                info!(chain_id, "Receipt backfill: shutting down");
                break;
            }
            result = tick_receipt_backfill(&sinks, &rpc, chain_id) => {
                if let Err(e) = result {
                    error!(chain_id, error = %e, "Receipt backfill tick failed");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    Ok(())
}

/// One tick of receipt backfill: finds blocks missing receipts and fills them in.
async fn tick_receipt_backfill(
    sinks: &SinkSet,
    rpc: &RpcClient,
    chain_id: u64,
) -> Result<()> {
    use alloy::network::ReceiptResponse;
    use super::decoder::{decode_log, decode_receipt};

    const BATCH_LIMIT: i64 = 100;
    let pool = sinks.pool();

    // Find blocks that have no receipts (most recent first)
    let blocks_missing = detect_blocks_missing_receipts(pool, BATCH_LIMIT).await?;

    if blocks_missing.is_empty() {
        // All caught up, sleep before checking again
        tokio::time::sleep(Duration::from_secs(2)).await;
        return Ok(());
    }

    debug!(
        chain_id,
        count = blocks_missing.len(),
        first = blocks_missing.first().copied(),
        last = blocks_missing.last().copied(),
        "Receipt backfill: found blocks missing receipts"
    );

    // Group consecutive blocks into ranges for batch fetching
    let ranges = group_consecutive_blocks(&blocks_missing);

    let mut min_block: Option<u64> = None;
    let mut max_block: Option<u64> = None;

    for (from, to) in ranges {
        // Fetch receipts for this range, splitting on "too large" errors
        let receipts = match fetch_receipts_adaptive(rpc, chain_id, from, to).await {
            Ok(r) => r,
            Err(e) => {
                error!(chain_id, from, to, error = %e, "Receipt backfill: failed to fetch receipts");
                continue;
            }
        };

        // Get block timestamps from DB (blocks already exist)
        let conn = pool.get().await?;
        let rows = conn
            .query(
                "SELECT num, timestamp FROM blocks WHERE num >= $1 AND num <= $2",
                &[&(from as i64), &(to as i64)],
            )
            .await?;

        let block_timestamps: HashMap<u64, _> = rows
            .iter()
            .map(|r| {
                let num: i64 = r.get(0);
                let ts: chrono::DateTime<chrono::Utc> = r.get(1);
                (num as u64, ts)
            })
            .collect();

        // Decode logs and receipts
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

        let log_count = all_logs.len();
        let receipt_count = all_receipts.len();

        // Write to all sinks
        sinks.write_logs(&all_logs).await?;
        sinks.write_receipts(&all_receipts).await?;

        if receipt_count > 0 {
            min_block = Some(min_block.map_or(from, |m: u64| m.min(from)));
            max_block = Some(max_block.map_or(to, |m: u64| m.max(to)));
        }

        metrics::record_logs_indexed(chain_id, log_count as u64);

        debug!(
            chain_id,
            from,
            to,
            receipts = receipt_count,
            logs = log_count,
            "Receipt backfill: wrote receipts+logs"
        );
    }

    // Single UPDATE txs covering all processed ranges (instead of per-range)
    if let (Some(lo), Some(hi)) = (min_block, max_block) {
        let conn = pool.get().await?;
        conn.execute(
            "UPDATE txs SET gas_used = r.gas_used, fee_payer = r.fee_payer \
             FROM receipts r \
             WHERE txs.block_num = r.block_num AND txs.idx = r.tx_idx \
               AND txs.block_num >= $1 AND txs.block_num <= $2 \
               AND txs.gas_used IS NULL",
            &[&(lo as i64), &(hi as i64)],
        )
        .await?;
    }

    Ok(())
}

/// Fetch receipts for a block range, adaptively splitting on "too large" errors.
///
/// When the RPC rejects a batch as too large, the range is split in half and
/// retried recursively until individual blocks are fetched. This handles
/// receipt-heavy blocks that exceed RPC response size limits.
fn fetch_receipts_adaptive<'a>(
    rpc: &'a RpcClient,
    chain_id: u64,
    from: u64,
    to: u64,
) -> futures::future::BoxFuture<'a, Result<Vec<Vec<crate::tempo::Receipt>>>> {
    Box::pin(async move {
        match rpc.get_receipts_batch(from..=to).await {
            Ok(r) => Ok(r),
            Err(e) if e.to_string().contains("too large") => {
                if from == to {
                    anyhow::bail!("Single block {from} receipts exceed RPC response size limit");
                }

                let mid = from + (to - from) / 2;
                debug!(
                    chain_id,
                    from, to, mid, "Receipt backfill: response too large, splitting range"
                );

                let mut left = fetch_receipts_adaptive(rpc, chain_id, from, mid).await?;
                let right = fetch_receipts_adaptive(rpc, chain_id, mid + 1, to).await?;
                left.extend(right);
                Ok(left)
            }
            Err(e) => Err(e),
        }
    })
}

/// Group consecutive block numbers into ranges for batch fetching.
/// Input: [100, 99, 98, 50, 49, 10] (descending)
/// Output: [(98, 100), (49, 50), (10, 10)]
/// Max blocks per range to avoid RPC "batch response too large" errors.
const MAX_RANGE_SIZE: u64 = 10;

fn group_consecutive_blocks(blocks: &[u64]) -> Vec<(u64, u64)> {
    if blocks.is_empty() {
        return Vec::new();
    }

    let mut sorted: Vec<u64> = blocks.to_vec();
    sorted.sort_unstable();

    let mut ranges = Vec::new();
    let mut start = sorted[0];
    let mut end = sorted[0];

    for &num in &sorted[1..] {
        if num == end + 1 && end - start + 1 < MAX_RANGE_SIZE {
            end = num;
        } else {
            ranges.push((start, end));
            start = num;
            end = num;
        }
    }
    ranges.push((start, end));

    ranges
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_group_consecutive_blocks_empty() {
        assert_eq!(group_consecutive_blocks(&[]), Vec::<(u64, u64)>::new());
    }

    #[test]
    fn test_group_consecutive_blocks_single() {
        assert_eq!(group_consecutive_blocks(&[42]), vec![(42, 42)]);
    }

    #[test]
    fn test_group_consecutive_blocks_consecutive() {
        assert_eq!(group_consecutive_blocks(&[1, 2, 3, 4, 5]), vec![(1, 5)]);
    }

    #[test]
    fn test_group_consecutive_blocks_descending() {
        // Input is descending (as returned by detect_blocks_missing_receipts)
        assert_eq!(group_consecutive_blocks(&[100, 99, 98]), vec![(98, 100)]);
    }

    #[test]
    fn test_group_consecutive_blocks_gaps() {
        assert_eq!(
            group_consecutive_blocks(&[100, 99, 98, 50, 49, 10]),
            vec![(10, 10), (49, 50), (98, 100)]
        );
    }

    #[test]
    fn test_group_consecutive_blocks_unsorted() {
        assert_eq!(
            group_consecutive_blocks(&[5, 1, 3, 2, 4]),
            vec![(1, 5)]
        );
    }

    #[test]
    fn test_group_consecutive_blocks_splits_large_ranges() {
        // 20 consecutive blocks should be split into ranges of MAX_RANGE_SIZE
        let blocks: Vec<u64> = (1..=20).collect();
        let ranges = group_consecutive_blocks(&blocks);
        assert_eq!(ranges, vec![(1, 10), (11, 20)]);

        // 25 blocks → 10 + 10 + 5
        let blocks: Vec<u64> = (1..=25).collect();
        let ranges = group_consecutive_blocks(&blocks);
        assert_eq!(ranges, vec![(1, 10), (11, 20), (21, 25)]);
    }
}
