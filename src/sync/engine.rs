use alloy::consensus::BlockHeader as _;
use alloy::network::ReceiptResponse;
use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::broadcast;
use tracing::{debug, error, info};

use crate::broadcast::{BlockUpdate, Broadcaster};
use crate::db::{Pool, ThrottledPool};
use crate::metrics::{self, SyncProgress};
use crate::types::{LogRow, ReceiptRow, SyncState};

use super::decoder::{
    decode_block, decode_log, decode_receipt, decode_transaction, enrich_receipts_from_txs,
    enrich_txs_from_receipts, timestamp_from_secs, validate_receipts,
};
use super::fetcher::RpcClient;
use super::sink::{SinkSet, WriteTarget};
use super::writer::{
    advance_checked_synced_num, detect_all_gaps, detect_blocks_missing_receipts,
    discover_legacy_receipt_repairs, find_fork_point, finish_receipt_repair_attempt,
    get_block_hash, has_gaps, load_sync_state, rewind_tip_num, save_sync_state,
    update_backfill_num, update_sync_rate, update_tip_num,
};
use crate::virtual_address::mark_virtual_forward_hops;

/// RPC concurrency limits
const REALTIME_RPC_CONCURRENCY: usize = 4;
const BACKFILL_RPC_CONCURRENCY: usize = 8;
const RECEIPT_BACKFILL_BLOCK_LIMIT: i64 = 100;
const RECEIPT_BACKFILL_DISCOVERY_WINDOW: i64 = 1_000;
const RECEIPT_BACKFILL_POLL_INTERVAL: Duration = Duration::from_secs(2);
const RECEIPT_BACKFILL_MAX_WRITE_ROWS: usize = 50_000;
const RECEIPT_BACKFILL_INFO_ROWS: usize = 10_000;

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
    gapfill_enabled: bool,
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
            gapfill_enabled: true,
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

    /// Disable the PostgreSQL-driven historical gap-fill loop.
    ///
    /// Tiered deployments run independent ClickHouse archive and PostgreSQL
    /// hot-window reconcilers instead.
    pub fn with_gapfill_enabled(mut self, enabled: bool) -> Self {
        self.gapfill_enabled = enabled;
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
        let state = load_sync_state(self.pool(), self.chain_id)
            .await?
            .unwrap_or_default();
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

            // Check for gaps (reload state: pruner may advance the floor)
            let floor = load_sync_state(self.pool(), self.chain_id)
                .await?
                .unwrap_or_default()
                .prune_floor();
            let gaps = detect_all_gaps(self.pool(), floor, remote_head).await?;
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
        let state = load_sync_state(self.pool(), self.chain_id)
            .await?
            .unwrap_or_default();
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
        let gapfill_handle = self.gapfill_enabled.then(|| {
            tokio::spawn(async move {
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
            })
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
        if let Some(gapfill_handle) = gapfill_handle {
            gapfill_handle.abort();
        }
        receipt_handle.abort();
        Ok(())
    }

    /// Realtime sync: follows chain head with complete indexing.
    ///
    /// Strategy: fetch blocks, txs, receipts, and logs together, then commit
    /// them atomically before advancing the tip. This keeps log-backed views
    /// consistent with newly indexed transactions.
    async fn tick_realtime(&mut self, progress: &mut SyncProgress) -> Result<()> {
        let state = load_sync_state(self.pool(), self.chain_id)
            .await?
            .unwrap_or_default();
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

        // Fetch blocks + receipts + logs together for complete indexing
        let fetch_start = std::time::Instant::now();
        let mut current_fetch = Some(self.fetch_range(current_from, current_to).await?);
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
            let (blocks, block_rows, all_txs, all_logs, all_receipts) =
                current_fetch.take().unwrap();
            // Checked here rather than at fetch time: this batch was fetched while
            // the previous one was still being written, so its stored parent only
            // exists now that the previous batch is committed. After a handled
            // reorg the batch is stale. The tick continues from the fork point: the
            // next tick could jump ahead and leave the range to loops that may not run.
            if let Some(fork_block) = self.validate_parent_chain(&blocks).await? {
                current_from = fork_block + 1;
                current_to = (current_from + BATCH_SIZE - 1).min(remote_head);
                current_fetch = Some(self.fetch_range(current_from, current_to).await?);
                continue;
            }
            let tx_count = all_txs.len() as u64;
            let log_count = all_logs.len() as u64;
            let mut logs_per_block = HashMap::new();
            for log in &all_logs {
                *logs_per_block.entry(log.block_num).or_insert(0_u64) += 1;
            }

            let next_from = current_to + 1;
            let next_to = (next_from + BATCH_SIZE - 1).min(remote_head);
            let has_next = next_from <= remote_head;

            // Pipeline: fetch next batch while writing current
            let next_fetch_future = if has_next {
                Some(self.fetch_range(next_from, next_to))
            } else {
                None
            };

            let sinks = self.sinks.clone();
            let write_future = async move {
                let write_start = std::time::Instant::now();
                sinks
                    .write_all(&block_rows, &all_txs, &all_logs, &all_receipts)
                    .await?;
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
            metrics::record_logs_indexed(self.chain_id, log_count);
            progress.report_forward(current_to, remote_head, block_count);

            if let Some(ref broadcaster) = self.broadcaster {
                for block in &blocks {
                    broadcaster.send(BlockUpdate {
                        chain_id: self.chain_id,
                        block_num: block.header.number(),
                        block_hash: format!("0x{}", hex::encode(block.header.hash)),
                        tx_count: block.transactions.len() as u64,
                        log_count: logs_per_block
                            .get(&(block.header.number() as i64))
                            .copied()
                            .unwrap_or(0),
                        timestamp: block.header.timestamp() as i64,
                    });
                }
            }

            debug!(
                from = current_from,
                to = current_to,
                blocks = block_count,
                txs = tx_count,
                logs = log_count,
                "Realtime: wrote blocks+txs+receipts+logs"
            );

            current_from = next_from;
            current_to = next_to;
        }

        Ok(())
    }

    /// Validate parent hash chain for a batch of blocks.
    /// Returns Ok(None) if chain is valid, Ok(Some(fork_block)) if a reorg was detected and
    /// handled: the batch is then stale and must not be written.
    /// Skipped entirely if trust_rpc is enabled.
    async fn validate_parent_chain(&self, blocks: &[crate::tempo::Block]) -> Result<Option<u64>> {
        if blocks.is_empty() || self.trust_rpc {
            return Ok(None);
        }

        let first_block = &blocks[0];
        let first_num = first_block.header.number();

        // Check parent hash against stored block (if not genesis)
        if first_num > 0
            && let Some(stored_hash) = get_block_hash(self.pool(), first_num - 1).await?
        {
            let expected_parent: [u8; 32] = stored_hash
                .try_into()
                .map_err(|_| anyhow::anyhow!("Invalid stored hash length"))?;
            if first_block.header.parent_hash().0 != expected_parent {
                // Reorg detected - handle it automatically
                return self.handle_reorg(first_num).await.map(Some);
            }
        }

        // Validate internal chain continuity
        for window in blocks.windows(2) {
            if window[1].header.parent_hash() != window[0].header.hash {
                return Err(anyhow::anyhow!(
                    "Internal chain break at block {}: parent_hash {:?} != prev hash {:?}",
                    window[1].header.number(),
                    hex::encode(window[1].header.parent_hash().0),
                    hex::encode(window[0].header.hash.0)
                ));
            }
        }

        Ok(None)
    }

    /// Handle a chain reorganization by finding the fork point and deleting orphaned blocks.
    /// Returns the fork point, from which the caller re-fetches the canonical chain.
    async fn handle_reorg(&self, mismatch_block: u64) -> Result<u64> {
        const MAX_REORG_DEPTH: u64 = 128;

        info!(
            chain_id = self.chain_id,
            mismatch_block, "Reorg detected, finding fork point"
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

                // Rewind the pointers to the fork point so realtime sync continues from there.
                rewind_tip_num(self.pool(), self.chain_id, fork_block).await?;

                Ok(fork_block)
            }
            None => Err(anyhow::anyhow!(
                "Could not find fork point within {} blocks of mismatch at block {}",
                MAX_REORG_DEPTH,
                mismatch_block
            )),
        }
    }

    /// Detect and fill any gaps in the indexed block sequence
    pub async fn fill_gaps(&self) -> Result<usize> {
        let state = load_sync_state(self.pool(), self.chain_id)
            .await?
            .unwrap_or_default();
        let gaps = detect_all_gaps(self.pool(), state.prune_floor(), state.tip_num).await?;
        let mut filled = 0;

        for (start, end) in gaps {
            info!(from = start, to = end, "Filling gap");
            self.sync_range(start, end).await?;
            filled += (end - start + 1) as usize;
        }

        Ok(filled)
    }

    /// Fetch and decode a range of blocks with receipts (full sync)
    /// The parent hash chain is not checked here: callers validate right before writing.
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
            self.realtime_rpc.get_blocks_batch_adaptive(from..=to),
            self.realtime_rpc.get_receipts_batch_adaptive(from..=to)
        )?;
        validate_receipts(&blocks, &receipts)?;

        let block_timestamps: HashMap<u64, _> = blocks
            .iter()
            .map(|b| (b.header.number(), timestamp_from_secs(b.header.timestamp())))
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

        let mut all_logs: Vec<_> = receipts
            .iter()
            .flatten()
            .flat_map(|receipt| {
                let block_num = receipt.block_number().unwrap_or(0);
                block_timestamps
                    .get(&block_num)
                    .map(|&ts| {
                        receipt
                            .inner
                            .logs()
                            .iter()
                            .map(move |log| decode_log(log, ts))
                    })
                    .into_iter()
                    .flatten()
            })
            .collect();

        let mut all_receipts: Vec<_> = receipts
            .iter()
            .flatten()
            .filter_map(|receipt| {
                let block_num = receipt.block_number().unwrap_or(0);
                block_timestamps
                    .get(&block_num)
                    .map(|&ts| decode_receipt(receipt, ts))
            })
            .collect();

        enrich_txs_from_receipts(&mut all_txs, &all_receipts);
        enrich_receipts_from_txs(&mut all_receipts, &all_txs);

        // TIP-1022: mark virtual address forwarding hops
        let forward_marks = mark_virtual_forward_hops(&all_logs);
        for (log, is_forward) in all_logs.iter_mut().zip(forward_marks) {
            log.is_virtual_forward = is_forward;
        }

        Ok((blocks, block_rows, all_txs, all_logs, all_receipts))
    }

    pub async fn sync_range(&self, from: u64, to: u64) -> Result<()> {
        let (blocks, block_rows, all_txs, all_logs, all_receipts) =
            self.fetch_range(from, to).await?;

        if self.validate_parent_chain(&blocks).await?.is_some() {
            return Err(anyhow::anyhow!(
                "Reorg detected at block {from}: the stored chain was rewound to the fork point"
            ));
        }

        self.sinks
            .write_all(&block_rows, &all_txs, &all_logs, &all_receipts)
            .await?;

        Ok(())
    }

    pub async fn sync_block(&self, num: u64) -> Result<()> {
        let (block, receipts) = tokio::try_join!(
            self.realtime_rpc.get_block(num, true),
            self.realtime_rpc.get_block_receipts(num)
        )?;
        validate_receipts(
            std::slice::from_ref(&block),
            std::slice::from_ref(&receipts),
        )?;

        let block_row = decode_block(&block);
        let block_ts = timestamp_from_secs(block.header.timestamp());
        let mut txs: Vec<_> = block
            .transactions
            .txns()
            .enumerate()
            .map(|(i, tx)| decode_transaction(tx, &block, i as u32))
            .collect();

        let mut log_rows: Vec<_> = receipts
            .iter()
            .flat_map(|r| r.inner.logs().iter().map(|log| decode_log(log, block_ts)))
            .collect();

        let mut receipt_rows: Vec<_> = receipts
            .iter()
            .map(|r| decode_receipt(r, block_ts))
            .collect();

        enrich_txs_from_receipts(&mut txs, &receipt_rows);
        enrich_receipts_from_txs(&mut receipt_rows, &txs);

        // TIP-1022: mark virtual address forwarding hops
        let forward_marks = mark_virtual_forward_hops(&log_rows);
        for (log, is_forward) in log_rows.iter_mut().zip(forward_marks) {
            log.is_virtual_forward = is_forward;
        }

        self.sinks
            .write_all(
                std::slice::from_ref(&block_row),
                &txs,
                &log_rows,
                &receipt_rows,
            )
            .await?;

        // Update sync state
        let state = load_sync_state(self.pool(), self.chain_id)
            .await?
            .unwrap_or_default();
        let new_state = SyncState {
            chain_id: self.chain_id,
            head_num: num,
            synced_num: num,
            tip_num: num,
            backfill_num: state.backfill_num,
            sync_rate: state.sync_rate,
            started_at: state.started_at,
            pruned_below: state.pruned_below,
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

        let mut state = load_sync_state(self.pool(), self.chain_id)
            .await?
            .unwrap_or_default();

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
        let state = load_sync_state(self.pool(), self.chain_id)
            .await?
            .unwrap_or_default();
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
    let state = load_sync_state(sinks.pool(), chain_id)
        .await?
        .unwrap_or_default();
    let mut progress = SyncProgress::new(chain_id, state.synced_num);
    let mut gap_check = GapCheck::default();

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
            result = tick_gapfill_parallel(&sinks, &backfill_semaphore, &rpc, chain_id, batch_size, concurrency, &mut progress, &mut gap_check) => {
                if let Err(e) = result {
                    error!(error = %e, "Gap-fill sync tick failed");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    Ok(())
}

/// How often gap-fill verifies the whole range from the prune floor, although
/// only blocks above `synced_num` can be missing.
const FULL_GAP_CHECK_INTERVAL: Duration = Duration::from_secs(10 * 60);

/// Chooses the range that gap-fill checks for missing blocks.
///
/// `synced_num` is only raised after the blocks up to it were verified to be
/// contiguous, and a reorg rewinds it together with the blocks it deletes. The
/// check that runs every tick therefore only counts the blocks above
/// `synced_num` instead of every block since the prune floor. The full range is
/// still verified on start, so state written by older versions is not trusted,
/// and then every [`FULL_GAP_CHECK_INTERVAL`].
#[derive(Debug, Default)]
struct GapCheck {
    last_full_check: Option<Instant>,
}

impl GapCheck {
    /// First block of the range to check at `now`.
    fn check_from(&self, state: &SyncState, now: Instant) -> u64 {
        let floor = state.prune_floor();
        match self.last_full_check {
            Some(at) if now.saturating_duration_since(at) < FULL_GAP_CHECK_INTERVAL => {
                floor.max(state.synced_num.saturating_add(1))
            }
            _ => floor,
        }
    }

    /// Records that no block is missing from `from` through the tip.
    fn passed(&mut self, from: u64, state: &SyncState, now: Instant) {
        if from <= state.prune_floor() {
            self.last_full_check = Some(now);
        }
    }
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
    gap_check: &mut GapCheck,
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

    // Fast path: use COUNT-based check (btree index scan) to see if there
    // are any gaps at all. Only fall back to the expensive LAG() window
    // function when gaps actually exist and we need their exact ranges.
    // With 0.5s block time, tip_num races ahead of synced_num constantly,
    // so we usually only count the blocks above synced_num.
    let now = Instant::now();
    let check_from = gap_check.check_from(&state, now);
    if state.tip_num > 0 && !has_gaps(pool, check_from, state.tip_num).await? {
        gap_check.passed(check_from, &state, now);
        metrics::set_gap_ranges(chain_id, "postgres", &[]);
        metrics::set_synced(chain_id, realtime_lag == 0);
        if state.synced_num < state.tip_num {
            advance_checked_synced_num(pool, chain_id, state.synced_num, state.tip_num).await?;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
        return Ok(());
    }

    // Gaps exist — run the expensive window function to find exact ranges
    let gaps = detect_all_gaps(pool, state.prune_floor(), state.tip_num).await?;

    if gaps.is_empty() {
        // No gaps - fully synced from genesis to tip
        metrics::set_gap_ranges(chain_id, "postgres", &[]);
        metrics::set_synced(chain_id, realtime_lag == 0);
        if state.synced_num < state.tip_num {
            advance_checked_synced_num(pool, chain_id, state.synced_num, state.tip_num).await?;
            info!(synced_num = state.tip_num, "Gap sync: fully synced");
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
        return Ok(());
    }

    let total_gap_blocks: u64 = gaps.iter().map(|(s, e)| e - s + 1).sum();
    let gap_count = gaps.len();
    metrics::set_gap_ranges(chain_id, "postgres", &gaps);
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
                    Err(_) => {
                        return (
                            start,
                            end,
                            Err(anyhow::anyhow!("Backfill semaphore closed")),
                        );
                    }
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
                let current_state = load_sync_state(pool, chain_id)
                    .await
                    .ok()
                    .flatten()
                    .unwrap_or_default();
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
                metrics::set_backfill_remaining(
                    chain_id,
                    "postgres",
                    total_gap_blocks.saturating_sub(completed),
                );
                progress.report_gap_fill(completed, total_gap_blocks, batch_count);

                debug!(
                    from = start,
                    to = end,
                    completed = completed,
                    "Gap sync: wrote batch"
                );
            }
            Err(e) => {
                let error_str = e.to_string();
                let is_batch_too_large =
                    error_str.contains("too large") || error_str.contains("response size exceeded");

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
                            Err(_) => {
                                return (
                                    start,
                                    mid,
                                    Err(anyhow::anyhow!("Backfill semaphore closed")),
                                );
                            }
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
                            Err(_) => {
                                return (
                                    mid + 1,
                                    end,
                                    Err(anyhow::anyhow!("Backfill semaphore closed")),
                                );
                            }
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
                            Err(_) => {
                                return (
                                    start,
                                    end,
                                    Err(anyhow::anyhow!("Backfill semaphore closed")),
                                );
                            }
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
                    Err(_) => {
                        return (
                            start,
                            end,
                            Err(anyhow::anyhow!("Backfill semaphore closed")),
                        );
                    }
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
        update_backfill_num(pool, chain_id, lowest_block).await?;
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

    // Detect ALL gaps above the prune floor, sorted by end DESC (most recent first)
    let gaps = detect_all_gaps(pool, state.prune_floor(), state.tip_num).await?;

    if gaps.is_empty() {
        metrics::set_gap_ranges(chain_id, "postgres", &[]);
        if state.synced_num < state.tip_num {
            advance_checked_synced_num(pool, chain_id, state.synced_num, state.tip_num).await?;
            info!(synced_num = state.tip_num, "Backfill: fully synced");
        }
        return Ok(());
    }

    let total_gap_blocks: u64 = gaps.iter().map(|(s, e)| e - s + 1).sum();
    let gap_count = gaps.len();
    metrics::set_gap_ranges(chain_id, "postgres", &gaps);

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
                metrics::set_backfill_remaining(
                    chain_id,
                    "postgres",
                    total_gap_blocks.saturating_sub(completed),
                );
                progress.report_gap_fill(completed, total_gap_blocks, batch_count);

                debug!(
                    from = start,
                    to = end,
                    completed = completed,
                    "Backfill: wrote batch"
                );
            }
            Err(e) => {
                let error_str = e.to_string();
                let is_batch_too_large =
                    error_str.contains("too large") || error_str.contains("response size exceeded");

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
        update_backfill_num(pool, chain_id, lowest_block).await?;
    }

    Ok(())
}

/// Check if fully synced (no gaps from `floor` to tip)
#[allow(dead_code)]
async fn is_fully_synced(pool: &Pool, floor: u64, tip_num: u64) -> Result<bool> {
    let gaps = detect_all_gaps(pool, floor, tip_num).await?;
    Ok(gaps.is_empty())
}

/// Standalone sync_range for gap-fill (doesn't need SyncEngine self)
pub(crate) async fn sync_range_standalone_to(
    sinks: &SinkSet,
    rpc: &RpcClient,
    from: u64,
    to: u64,
    target: WriteTarget,
) -> Result<()> {
    use super::decoder::{
        decode_block, decode_log, decode_receipt, decode_transaction, enrich_receipts_from_txs,
        enrich_txs_from_receipts, timestamp_from_secs, validate_receipts,
    };
    use alloy::network::ReceiptResponse;

    let (blocks, receipts) = tokio::try_join!(
        rpc.get_blocks_batch_adaptive(from..=to),
        rpc.get_receipts_batch_adaptive(from..=to)
    )?;
    validate_receipts(&blocks, &receipts)?;

    let block_timestamps: HashMap<u64, _> = blocks
        .iter()
        .map(|b| (b.header.number(), timestamp_from_secs(b.header.timestamp())))
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

    let mut all_logs: Vec<_> = receipts
        .iter()
        .flatten()
        .flat_map(|receipt| {
            let block_num = receipt.block_number().unwrap_or(0);
            block_timestamps
                .get(&block_num)
                .map(|&ts| {
                    receipt
                        .inner
                        .logs()
                        .iter()
                        .map(move |log| decode_log(log, ts))
                })
                .into_iter()
                .flatten()
        })
        .collect();

    let mut all_receipts: Vec<_> = receipts
        .iter()
        .flatten()
        .filter_map(|receipt| {
            let block_num = receipt.block_number().unwrap_or(0);
            block_timestamps
                .get(&block_num)
                .map(|&ts| decode_receipt(receipt, ts))
        })
        .collect();

    enrich_txs_from_receipts(&mut all_txs, &all_receipts);
    enrich_receipts_from_txs(&mut all_receipts, &all_txs);

    // TIP-1022: mark virtual address forwarding hops
    let forward_marks = mark_virtual_forward_hops(&all_logs);
    for (log, is_forward) in all_logs.iter_mut().zip(forward_marks) {
        log.is_virtual_forward = is_forward;
    }

    match target {
        WriteTarget::All => {
            sinks
                .write_all(&block_rows, &all_txs, &all_logs, &all_receipts)
                .await?;
        }
        WriteTarget::Postgres => {
            sinks
                .write_all_postgres(&block_rows, &all_txs, &all_logs, &all_receipts)
                .await?;
        }
        WriteTarget::ClickHouse => {
            sinks
                .write_all_clickhouse(&block_rows, &all_txs, &all_logs, &all_receipts)
                .await?;
        }
    }

    Ok(())
}

async fn sync_range_standalone(sinks: &SinkSet, rpc: &RpcClient, from: u64, to: u64) -> Result<()> {
    sync_range_standalone_to(sinks, rpc, from, to, WriteTarget::All).await
}

/// Receipt backfill loop: repairs any blocks missing receipts/logs.
///
/// Realtime sync should already write complete batches. This loop remains as a
/// safety net for legacy gaps and crash recovery.
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
                } else {
                    tokio::time::sleep(RECEIPT_BACKFILL_POLL_INTERVAL).await;
                }
            }
        }
    }

    Ok(())
}

/// One tick of receipt backfill: finds blocks missing receipts and fills them in.
async fn tick_receipt_backfill(sinks: &SinkSet, rpc: &RpcClient, chain_id: u64) -> Result<()> {
    use super::decoder::{decode_log, decode_receipt};
    use alloy::network::ReceiptResponse;

    let pool = sinks.pool();

    let discovered =
        discover_legacy_receipt_repairs(pool, chain_id, RECEIPT_BACKFILL_DISCOVERY_WINDOW).await?;
    if discovered > 0 {
        info!(
            chain_id,
            discovered, "Receipt backfill: discovered legacy work"
        );
    }

    // Claim due work from the durable queue (most recent first).
    let blocks_missing = detect_blocks_missing_receipts(pool, RECEIPT_BACKFILL_BLOCK_LIMIT).await?;

    if blocks_missing.is_empty() {
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
        let receipts = match rpc.get_receipts_batch_adaptive(from..=to).await {
            Ok(r) => r,
            Err(e) => {
                error!(chain_id, from, to, error = %e, "Receipt backfill: failed to fetch receipts");
                continue;
            }
        };

        // Get block timestamps from DB (blocks already exist). They are keyed by hash so that
        // receipts of a block that is not stored (another fork at that height) are skipped.
        let conn = pool.get().await?;
        let rows = conn
            .query(
                "SELECT hash, timestamp FROM blocks WHERE num >= $1 AND num <= $2",
                &[&(from as i64), &(to as i64)],
            )
            .await?;

        let block_timestamps: HashMap<Vec<u8>, _> = rows
            .iter()
            .map(|r| {
                let hash: Vec<u8> = r.get(0);
                let ts: chrono::DateTime<chrono::Utc> = r.get(1);
                (hash, ts)
            })
            .collect();

        let mut chunk_logs: Vec<LogRow> = Vec::new();
        let mut chunk_receipts: Vec<ReceiptRow> = Vec::new();
        let mut chunk_min_block: Option<u64> = None;
        let mut chunk_max_block: Option<u64> = None;

        for block_receipts in receipts {
            let mut block_logs: Vec<_> = block_receipts
                .iter()
                .flat_map(|receipt| {
                    receipt
                        .block_hash()
                        .and_then(|hash| block_timestamps.get(hash.as_slice()))
                        .map(|&ts| {
                            receipt
                                .inner
                                .logs()
                                .iter()
                                .map(move |log| decode_log(log, ts))
                        })
                        .into_iter()
                        .flatten()
                })
                .collect();

            // TIP-1022: mark virtual address forwarding hops. Matching is scoped to a
            // transaction, so doing this one block at a time keeps chunking safe.
            let forward_marks = mark_virtual_forward_hops(&block_logs);
            for (log, is_forward) in block_logs.iter_mut().zip(forward_marks) {
                log.is_virtual_forward = is_forward;
            }

            let block_receipt_rows: Vec<_> = block_receipts
                .iter()
                .filter_map(|receipt| {
                    receipt
                        .block_hash()
                        .and_then(|hash| block_timestamps.get(hash.as_slice()))
                        .map(|&ts| decode_receipt(receipt, ts))
                })
                .collect();

            let block_rows = receipt_backfill_rows(block_logs.len(), block_receipt_rows.len());
            if should_flush_receipt_backfill_chunk(
                receipt_backfill_rows(chunk_logs.len(), chunk_receipts.len()),
                block_rows,
            ) {
                flush_receipt_backfill_chunk(
                    sinks,
                    chain_id,
                    chunk_min_block,
                    chunk_max_block,
                    &mut chunk_logs,
                    &mut chunk_receipts,
                )
                .await?;
                chunk_min_block = None;
                chunk_max_block = None;
            }

            for block_num in block_logs.iter().map(|log| log.block_num as u64).chain(
                block_receipt_rows
                    .iter()
                    .map(|receipt| receipt.block_num as u64),
            ) {
                chunk_min_block = Some(chunk_min_block.map_or(block_num, |m| m.min(block_num)));
                chunk_max_block = Some(chunk_max_block.map_or(block_num, |m| m.max(block_num)));
            }

            if let Some(block_num) = block_receipt_rows
                .iter()
                .map(|receipt| receipt.block_num as u64)
                .min()
            {
                min_block = Some(min_block.map_or(block_num, |m: u64| m.min(block_num)));
            }
            if let Some(block_num) = block_receipt_rows
                .iter()
                .map(|receipt| receipt.block_num as u64)
                .max()
            {
                max_block = Some(max_block.map_or(block_num, |m: u64| m.max(block_num)));
            }

            chunk_logs.extend(block_logs);
            chunk_receipts.extend(block_receipt_rows);
        }

        flush_receipt_backfill_chunk(
            sinks,
            chain_id,
            chunk_min_block,
            chunk_max_block,
            &mut chunk_logs,
            &mut chunk_receipts,
        )
        .await?;
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

    let (completed, deferred) = finish_receipt_repair_attempt(pool, &blocks_missing).await?;
    debug!(
        chain_id,
        completed, deferred, "Receipt backfill: repair attempt finalized"
    );

    Ok(())
}

fn receipt_backfill_rows(log_count: usize, receipt_count: usize) -> usize {
    log_count + receipt_count
}

fn should_flush_receipt_backfill_chunk(current_rows: usize, next_rows: usize) -> bool {
    current_rows > 0 && current_rows + next_rows > RECEIPT_BACKFILL_MAX_WRITE_ROWS
}

async fn flush_receipt_backfill_chunk(
    sinks: &SinkSet,
    chain_id: u64,
    from: Option<u64>,
    to: Option<u64>,
    logs: &mut Vec<LogRow>,
    receipts: &mut Vec<ReceiptRow>,
) -> Result<()> {
    if logs.is_empty() && receipts.is_empty() {
        return Ok(());
    }

    let log_count = logs.len();
    let receipt_count = receipts.len();
    let row_count = receipt_backfill_rows(log_count, receipt_count);
    let start = Instant::now();
    let application_name = format!("tidx receipt_backfill {chain_id}");

    sinks
        .write_all_with_application_name(&[], &[], logs, receipts, &application_name)
        .await?;

    let elapsed = start.elapsed();
    metrics::record_logs_indexed(chain_id, log_count as u64);

    if elapsed >= Duration::from_secs(10) || row_count >= RECEIPT_BACKFILL_INFO_ROWS {
        info!(
            chain_id,
            from,
            to,
            receipts = receipt_count,
            logs = log_count,
            rows = row_count,
            elapsed_ms = elapsed.as_millis() as u64,
            "Receipt backfill: wrote receipts+logs chunk"
        );
    } else {
        debug!(
            chain_id,
            from,
            to,
            receipts = receipt_count,
            logs = log_count,
            rows = row_count,
            elapsed_ms = elapsed.as_millis() as u64,
            "Receipt backfill: wrote receipts+logs chunk"
        );
    }

    logs.clear();
    receipts.clear();

    Ok(())
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

    fn synced_state(synced_num: u64, pruned_below: u64) -> SyncState {
        SyncState {
            synced_num,
            tip_num: synced_num + 10,
            pruned_below,
            ..Default::default()
        }
    }

    #[test]
    fn test_gap_check_verifies_full_range_first() {
        let check = GapCheck::default();
        assert_eq!(check.check_from(&synced_state(500, 0), Instant::now()), 1);
        assert_eq!(
            check.check_from(&synced_state(500, 99), Instant::now()),
            100
        );
    }

    #[test]
    fn test_gap_check_starts_above_synced_after_full_check() {
        let now = Instant::now();
        let state = synced_state(500, 0);
        let mut check = GapCheck::default();
        check.passed(check.check_from(&state, now), &state, now);

        assert_eq!(check.check_from(&state, now), 501);
        // The prune floor still bounds the range when it is above synced_num.
        assert_eq!(check.check_from(&synced_state(500, 700), now), 701);
    }

    #[test]
    fn test_gap_check_repeats_full_check_after_interval() {
        let now = Instant::now();
        let state = synced_state(500, 0);
        let mut check = GapCheck::default();
        check.passed(1, &state, now);

        assert_eq!(check.check_from(&state, now + FULL_GAP_CHECK_INTERVAL), 1);
    }

    #[test]
    fn test_gap_check_partial_check_does_not_count_as_full() {
        let now = Instant::now();
        let state = synced_state(500, 0);
        let mut check = GapCheck::default();
        check.passed(501, &state, now);

        assert_eq!(check.check_from(&state, now), 1);
    }

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
        assert_eq!(group_consecutive_blocks(&[5, 1, 3, 2, 4]), vec![(1, 5)]);
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

    #[test]
    fn test_receipt_backfill_chunk_does_not_flush_empty_chunk() {
        assert!(!should_flush_receipt_backfill_chunk(
            0,
            RECEIPT_BACKFILL_MAX_WRITE_ROWS + 1
        ));
    }

    #[test]
    fn test_receipt_backfill_chunk_flushes_before_row_limit() {
        assert!(!should_flush_receipt_backfill_chunk(
            RECEIPT_BACKFILL_MAX_WRITE_ROWS - 10,
            10
        ));
        assert!(should_flush_receipt_backfill_chunk(
            RECEIPT_BACKFILL_MAX_WRITE_ROWS - 10,
            11
        ));
    }
}
