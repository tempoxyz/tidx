//! Live-stream head poller for API-only processes.
//!
//! In `tidx up` the sync engine publishes each block it writes to the
//! in-process [`Broadcaster`], which drives live query streams. `tidx serve`
//! runs no sync engine, so this task polls the database the API reads from
//! and publishes each new block number instead.
//!
//! The head it follows is `sync_state.tip_num` capped at the highest block
//! in `blocks`. `tip_num` alone can run ahead of the data: backfill-first
//! sync sets it to the chain head before writing anything. Capping it at
//! `max(num)` means a block is only announced once its row is on this
//! database, which on a read replica is after it has replicated.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use deadpool_postgres::Pool;
use tokio_postgres::types::Type;
use tracing::{debug, warn};

use crate::broadcast::{BlockUpdate, Broadcaster};

/// Default interval between `sync_state` polls.
pub const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(250);

/// Most block updates published from one poll. Live streams cap their own
/// catch-up at 10 blocks, so a larger jump only needs its last blocks.
const MAX_BLOCKS_PER_POLL: u64 = 64;

/// Reads the chain's announced head: `sync_state.tip_num` capped at the
/// highest block stored in `blocks`.
pub async fn fetch_tip(pool: &Pool, chain_id: u64) -> Result<Option<u64>> {
    let conn = pool.get().await?;
    let row = conn
        .query_typed_opt(
            "SELECT LEAST(s.tip_num, COALESCE((SELECT max(num) FROM blocks), 0)) \
             FROM sync_state s WHERE s.chain_id = $1",
            &[(&(chain_id as i64), Type::INT8)],
        )
        .await?;
    Ok(row.map(|r| r.get::<_, i64>(0).max(0) as u64))
}

/// Publishes the block updates between two observed tips.
///
/// Returns the updates sent, oldest first. Only the last
/// [`MAX_BLOCKS_PER_POLL`] blocks of a large jump are published.
pub fn publish_range(
    broadcaster: &Broadcaster,
    chain_id: u64,
    previous: u64,
    current: u64,
) -> Vec<u64> {
    if current <= previous {
        return Vec::new();
    }
    let start = (previous + 1).max(current.saturating_sub(MAX_BLOCKS_PER_POLL - 1));
    let blocks: Vec<u64> = (start..=current).collect();
    for &block_num in &blocks {
        broadcaster.send(BlockUpdate {
            chain_id,
            block_num,
            // Only chain_id and block_num drive live streams; the poller
            // does not read block contents.
            block_hash: String::new(),
            tx_count: 0,
            log_count: 0,
            timestamp: 0,
        });
    }
    blocks
}

/// Spawns a task that polls `sync_state` for `chain_id` every `interval`
/// and publishes new blocks to `broadcaster` until `shutdown` fires.
pub fn spawn(
    pool: Pool,
    chain_id: u64,
    broadcaster: Arc<Broadcaster>,
    interval: Duration,
    mut shutdown: tokio::sync::broadcast::Receiver<()>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut last: Option<u64> = None;
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = shutdown.recv() => break,
                _ = ticker.tick() => {}
            }
            match fetch_tip(&pool, chain_id).await {
                Ok(Some(tip)) => {
                    match last {
                        // First observation sets the baseline: streams start
                        // from their own initial query, not from history.
                        None => last = Some(tip),
                        Some(prev) if tip > prev => {
                            let sent = publish_range(&broadcaster, chain_id, prev, tip);
                            debug!(
                                chain_id,
                                from = prev,
                                to = tip,
                                published = sent.len(),
                                "head poller advanced"
                            );
                            last = Some(tip);
                        }
                        // A reorg can rewind the head; follow it down so the
                        // replacement blocks are announced again.
                        Some(prev) if tip < prev => last = Some(tip),
                        Some(_) => {}
                    }
                }
                Ok(None) => {}
                Err(e) => warn!(chain_id, error = %e, "head poller failed to read sync_state"),
            }
        }
    })
}
