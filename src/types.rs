use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct BlockRow {
    pub num: i64,
    pub hash: Vec<u8>,
    pub parent_hash: Vec<u8>,
    pub timestamp: DateTime<Utc>,
    pub timestamp_ms: i64,
    pub gas_limit: i64,
    pub gas_used: i64,
    pub miner: Vec<u8>,
    pub extra_data: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Default)]
pub struct TxRow {
    pub block_num: i64,
    pub block_timestamp: DateTime<Utc>,
    pub idx: i32,
    pub hash: Vec<u8>,
    pub tx_type: i16,
    pub from: Vec<u8>,
    pub to: Option<Vec<u8>>,
    pub value: String,
    pub input: Vec<u8>,
    pub gas_limit: i64,
    pub max_fee_per_gas: String,
    pub max_priority_fee_per_gas: String,
    pub gas_used: Option<i64>,
    pub nonce_key: Vec<u8>,
    pub nonce: i64,
    pub fee_token: Option<Vec<u8>>,
    pub fee_payer: Option<Vec<u8>>,
    pub calls: Option<serde_json::Value>,
    pub call_count: i16,
    pub valid_before: Option<i64>,
    pub valid_after: Option<i64>,
    pub signature_type: Option<i16>,
}

#[derive(Debug, Clone, Default)]
pub struct LogRow {
    pub block_num: i64,
    pub block_timestamp: DateTime<Utc>,
    pub log_idx: i32,
    pub tx_idx: i32,
    pub tx_hash: Vec<u8>,
    pub address: Vec<u8>,
    pub selector: Option<Vec<u8>>,
    pub topic0: Option<Vec<u8>>,
    pub topic1: Option<Vec<u8>>,
    pub topic2: Option<Vec<u8>>,
    pub topic3: Option<Vec<u8>>,
    pub data: Vec<u8>,
    /// TIP-1022: true if this log is the forwarding hop of a virtual address
    /// Transfer pair (the second Transfer from virtualAddr → master).
    pub is_virtual_forward: bool,
}

#[derive(Debug, Clone, Default)]
pub struct ReceiptRow {
    pub block_num: i64,
    pub block_timestamp: DateTime<Utc>,
    pub tx_idx: i32,
    pub tx_hash: Vec<u8>,
    pub from: Vec<u8>,
    pub to: Option<Vec<u8>>,
    pub contract_address: Option<Vec<u8>>,
    pub gas_used: i64,
    pub cumulative_gas_used: i64,
    pub effective_gas_price: Option<String>,
    pub status: Option<i16>,
    pub fee_payer: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SyncState {
    pub chain_id: u64,
    /// Remote chain head block number
    pub head_num: u64,
    /// Highest contiguous block synced (no gaps from backfill_num to here)
    pub synced_num: u64,
    /// Highest block synced near chain head (may have gaps below)
    pub tip_num: u64,
    /// Lowest block synced going backwards (None = not started, Some(0) = complete)
    pub backfill_num: Option<u64>,
    /// Current sync rate in blocks/second (rolling window)
    #[serde(default)]
    pub sync_rate: Option<f64>,
    /// When sync started (for ETA calculations)
    #[serde(default)]
    pub started_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl SyncState {
    /// Returns true if backfill is complete (reached genesis)
    pub fn backfill_complete(&self) -> bool {
        self.backfill_num == Some(0)
    }

    /// Returns true if backfill has started
    pub fn backfill_started(&self) -> bool {
        self.backfill_num.is_some()
    }

    /// Returns the number of blocks remaining to backfill
    pub fn backfill_remaining(&self) -> u64 {
        match self.backfill_num {
            None => self.tip_num, // Haven't started, need to fill 0..tip_num
            Some(0) => 0,         // Complete
            Some(n) => n,         // Blocks 0..n remain
        }
    }

    /// Returns the indexed range (low, high).
    /// During backfill, we have blocks from backfill_num up to tip_num.
    /// After backfill completes (backfill_num=0), range is 0 to tip_num.
    pub fn indexed_range(&self) -> (u64, u64) {
        match self.backfill_num {
            Some(n) => (n, self.tip_num), // Backfill in progress: n..tip_num
            None => (self.tip_num, self.tip_num), // Not started: just the tip
        }
    }

    /// Returns total number of indexed blocks
    pub fn total_indexed(&self) -> u64 {
        let (low, high) = self.indexed_range();
        if high >= low { high - low + 1 } else { 0 }
    }

    /// Get the current sync rate (blocks per second)
    /// Uses the stored rolling rate if available, otherwise calculates from elapsed time
    pub fn current_rate(&self) -> Option<f64> {
        // Prefer the stored rolling rate (more accurate)
        if let Some(rate) = self.sync_rate {
            if rate > 0.0 {
                return Some(rate);
            }
        }
        // Fallback to calculating from total elapsed time
        let started = self.started_at?;
        let elapsed = chrono::Utc::now().signed_duration_since(started);
        let secs = elapsed.num_seconds() as f64;
        if secs > 0.0 {
            Some(self.total_indexed() as f64 / secs)
        } else {
            None
        }
    }

    /// Estimate time remaining for backfill to complete
    pub fn backfill_eta_secs(&self) -> Option<f64> {
        let rate = self.current_rate()?;
        if rate > 0.0 {
            let remaining = self.backfill_remaining();
            Some(remaining as f64 / rate)
        } else {
            None
        }
    }
}
