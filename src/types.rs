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
    /// Configured starting block for indexing (0 = genesis)
    #[serde(default)]
    pub start_block: u64,
    /// Current sync rate in blocks/second (rolling window)
    #[serde(default)]
    pub sync_rate: Option<f64>,
    /// When sync started (for ETA calculations)
    #[serde(default)]
    pub started_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl SyncState {
    /// Returns true if backfill is complete (reached start_block)
    pub fn backfill_complete(&self) -> bool {
        match self.backfill_num {
            Some(n) => n <= self.start_block,
            None => false,
        }
    }

    /// Returns true if backfill has started
    pub fn backfill_started(&self) -> bool {
        self.backfill_num.is_some()
    }

    /// Returns the number of blocks remaining to backfill
    pub fn backfill_remaining(&self) -> u64 {
        match self.backfill_num {
            None => self.tip_num.saturating_sub(self.start_block), // Haven't started
            Some(n) if n <= self.start_block => 0,                 // Complete
            Some(n) => n.saturating_sub(self.start_block),         // Blocks start_block..n remain
        }
    }

    /// Returns the indexed range (low, high).
    /// During backfill, we have blocks from backfill_num up to tip_num.
    /// After backfill completes (backfill_num=0), range is 0 to tip_num.
    pub fn indexed_range(&self) -> (u64, u64) {
        match self.backfill_num {
            Some(n) => (n, self.tip_num),             // Backfill in progress: n..tip_num
            None => (self.tip_num, self.tip_num),     // Not started: just the tip
        }
    }

    /// Returns total number of indexed blocks
    pub fn total_indexed(&self) -> u64 {
        let (low, high) = self.indexed_range();
        if high >= low {
            high - low + 1
        } else {
            0
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn state_with(start_block: u64, backfill_num: Option<u64>, tip_num: u64) -> SyncState {
        SyncState {
            chain_id: 1,
            head_num: tip_num,
            synced_num: tip_num,
            tip_num,
            backfill_num,
            start_block,
            sync_rate: None,
            started_at: None,
        }
    }

    #[test]
    fn test_backfill_complete_with_start_block() {
        // backfill_num at start_block => complete
        let s = state_with(1000, Some(1000), 5000);
        assert!(s.backfill_complete());

        // backfill_num below start_block => also complete
        let s = state_with(1000, Some(500), 5000);
        assert!(s.backfill_complete());

        // backfill_num above start_block => not complete
        let s = state_with(1000, Some(1500), 5000);
        assert!(!s.backfill_complete());

        // backfill not started => not complete
        let s = state_with(1000, None, 5000);
        assert!(!s.backfill_complete());
    }

    #[test]
    fn test_backfill_remaining_with_start_block() {
        // In progress: 2000 blocks above start_block remain
        let s = state_with(1000, Some(3000), 5000);
        assert_eq!(s.backfill_remaining(), 2000);

        // Complete: at start_block
        let s = state_with(1000, Some(1000), 5000);
        assert_eq!(s.backfill_remaining(), 0);

        // Not started: tip_num - start_block
        let s = state_with(1000, None, 5000);
        assert_eq!(s.backfill_remaining(), 4000);
    }

    #[test]
    fn test_backfill_remaining_genesis() {
        // start_block=0 preserves original behavior
        let s = state_with(0, Some(100), 5000);
        assert_eq!(s.backfill_remaining(), 100);

        let s = state_with(0, Some(0), 5000);
        assert_eq!(s.backfill_remaining(), 0);

        let s = state_with(0, None, 5000);
        assert_eq!(s.backfill_remaining(), 5000);
    }

    #[test]
    fn test_backfill_complete_genesis() {
        // start_block=0 preserves original behavior (backfill_num=0 => complete)
        let s = state_with(0, Some(0), 5000);
        assert!(s.backfill_complete());

        let s = state_with(0, Some(1), 5000);
        assert!(!s.backfill_complete());
    }
}
