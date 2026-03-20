use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::ops::RangeInclusive;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Semaphore;

use crate::metrics;
use crate::tempo::{Block, Log, Receipt};

/// Default max concurrent RPC requests (prevents overwhelming RPC endpoints)
const DEFAULT_MAX_CONCURRENT_REQUESTS: usize = 8;

#[derive(Clone)]
pub struct RpcClient {
    client: reqwest::Client,
    url: String,
    /// Adaptive chunk size for eth_getLogs (learned from RPC errors)
    log_chunk_size: Arc<AtomicU64>,
    /// Semaphore to limit concurrent RPC requests
    concurrency_limiter: Arc<Semaphore>,
}

#[derive(Serialize)]
struct RpcRequest<'a> {
    jsonrpc: &'static str,
    id: u64,
    method: &'a str,
    params: serde_json::Value,
}

#[derive(Deserialize)]
struct RpcResponse<T> {
    result: Option<T>,
    error: Option<RpcError>,
}

#[derive(Deserialize, Debug)]
struct RpcError {
    #[allow(dead_code)]
    code: i64,
    message: String,
}

impl RpcClient {
    pub fn new(url: &str) -> Self {
        Self::with_concurrency(url, DEFAULT_MAX_CONCURRENT_REQUESTS)
    }

    /// Creates an RPC client with a custom concurrency limit.
    pub fn with_concurrency(url: &str, max_concurrent: usize) -> Self {
        let client = reqwest::Client::builder()
            .gzip(true)
            .timeout(std::time::Duration::from_secs(30))
            .connect_timeout(std::time::Duration::from_secs(10))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("Failed to build HTTP client");

        Self {
            client,
            url: url.to_string(),
            log_chunk_size: Arc::new(AtomicU64::new(1000)),
            concurrency_limiter: Arc::new(Semaphore::new(max_concurrent)),
        }
    }

    /// Returns the number of available permits (for monitoring)
    pub fn available_permits(&self) -> usize {
        self.concurrency_limiter.available_permits()
    }

    pub async fn chain_id(&self) -> Result<u64> {
        let resp: RpcResponse<String> = self.call("eth_chainId", serde_json::json!([])).await?;
        let hex = resp
            .result
            .ok_or_else(|| anyhow!("No result for eth_chainId"))?;
        Ok(u64::from_str_radix(hex.trim_start_matches("0x"), 16)?)
    }

    pub async fn latest_block_number(&self) -> Result<u64> {
        let resp: RpcResponse<String> = self
            .call("eth_blockNumber", serde_json::json!([]))
            .await?;
        let hex = resp
            .result
            .ok_or_else(|| anyhow!("No result for eth_blockNumber"))?;
        Ok(u64::from_str_radix(hex.trim_start_matches("0x"), 16)?)
    }

    pub async fn get_block(&self, num: u64, full_txs: bool) -> Result<Block> {
        let resp: RpcResponse<Block> = self
            .call(
                "eth_getBlockByNumber",
                serde_json::json!([format!("0x{:x}", num), full_txs]),
            )
            .await?;
        resp.result
            .ok_or_else(|| anyhow!("Block {num} not found"))
    }

    pub async fn get_blocks_batch(&self, range: RangeInclusive<u64>) -> Result<Vec<Block>> {
        // Acquire permit (batch counts as one request for concurrency limiting)
        let _permit = self.concurrency_limiter.acquire().await
            .map_err(|_| anyhow!("RPC semaphore closed"))?;

        let batch: Vec<_> = range
            .clone()
            .enumerate()
            .map(|(i, n)| RpcRequest {
                jsonrpc: "2.0",
                id: i as u64,
                method: "eth_getBlockByNumber",
                params: serde_json::json!([format!("0x{:x}", n), true]),
            })
            .collect();

        let response = self
            .client
            .post(&self.url)
            .json(&batch)
            .send()
            .await?;

        let status = response.status();
        let bytes = response.bytes().await?;

        if !status.is_success() {
            let body_preview: String = String::from_utf8_lossy(&bytes[..bytes.len().min(500)]).into();
            tracing::error!(
                status = %status,
                body_preview = %body_preview,
                from = %range.start(),
                to = %range.end(),
                "RPC request failed"
            );
            anyhow::bail!("RPC request failed with status {}: {}", status, String::from_utf8_lossy(&bytes[..bytes.len().min(200)]));
        }

        // Check if the RPC returned a single error object instead of an array
        if let Ok(single_error) = serde_json::from_slice::<RpcResponse<serde_json::Value>>(&bytes) {
            if let Some(err) = single_error.error {
                anyhow::bail!("RPC batch error: {}", err.message);
            }
        }

        let responses: Vec<RpcResponse<Block>> = serde_json::from_slice(&bytes)
            .map_err(|e| {
                let body_preview: String = String::from_utf8_lossy(&bytes[..bytes.len().min(500)]).into();
                tracing::error!(
                    error = %e,
                    body_preview = %body_preview,
                    from = %range.start(),
                    to = %range.end(),
                    "Failed to decode blocks response"
                );
                anyhow!("Failed to decode blocks response: {}", e)
            })?;

        responses
            .into_iter()
            .enumerate()
            .map(|(i, r)| {
                r.result
                    .ok_or_else(|| anyhow!("Block {} not found", range.start() + i as u64))
            })
            .collect()
    }

    /// Fetch receipts for a block (includes logs)
    pub async fn get_block_receipts(&self, block_num: u64) -> Result<Vec<Receipt>> {
        let resp: RpcResponse<Vec<Receipt>> = self
            .call(
                "eth_getBlockReceipts",
                serde_json::json!([format!("0x{:x}", block_num)]),
            )
            .await?;
        Ok(resp.result.unwrap_or_default())
    }

    /// Fetch receipts for multiple blocks in a batch
    pub async fn get_receipts_batch(&self, range: RangeInclusive<u64>) -> Result<Vec<Vec<Receipt>>> {
        // Acquire permit (batch counts as one request for concurrency limiting)
        let _permit = self.concurrency_limiter.acquire().await
            .map_err(|_| anyhow!("RPC semaphore closed"))?;

        let batch: Vec<_> = range
            .clone()
            .enumerate()
            .map(|(i, n)| RpcRequest {
                jsonrpc: "2.0",
                id: i as u64,
                method: "eth_getBlockReceipts",
                params: serde_json::json!([format!("0x{:x}", n)]),
            })
            .collect();

        let response = self
            .client
            .post(&self.url)
            .json(&batch)
            .send()
            .await?;

        let status = response.status();
        let bytes = response.bytes().await?;

        if !status.is_success() {
            let body_preview: String = String::from_utf8_lossy(&bytes[..bytes.len().min(500)]).into();
            tracing::error!(
                status = %status,
                body_preview = %body_preview,
                from = %range.start(),
                to = %range.end(),
                "RPC receipts request failed"
            );
            anyhow::bail!("RPC receipts request failed with status {}: {}", status, String::from_utf8_lossy(&bytes[..bytes.len().min(200)]));
        }

        // Check if the RPC returned a single error object instead of an array
        // This happens when the entire batch request fails (e.g., response too large)
        if let Ok(single_error) = serde_json::from_slice::<RpcResponse<serde_json::Value>>(&bytes) {
            if let Some(err) = single_error.error {
                anyhow::bail!("RPC batch error: {}", err.message);
            }
        }

        let responses: Vec<RpcResponse<Vec<Receipt>>> = serde_json::from_slice(&bytes)
            .map_err(|e| {
                let body_preview: String = String::from_utf8_lossy(&bytes[..bytes.len().min(500)]).into();
                tracing::error!(
                    error = %e,
                    body_preview = %body_preview,
                    from = %range.start(),
                    to = %range.end(),
                    "Failed to decode receipts response"
                );
                anyhow!("Failed to decode receipts response: {}", e)
            })?;

        responses
            .into_iter()
            .map(|r| Ok(r.result.unwrap_or_default()))
            .collect()
    }

    #[allow(dead_code)]
    pub async fn get_logs(&self, from: u64, to: u64) -> Result<Vec<Log>> {
        let estimated_logs = ((to - from + 1) * 100) as usize;
        let mut all_logs = Vec::with_capacity(estimated_logs.min(10_000));
        let mut start = from;

        while start <= to {
            let chunk_size = self.log_chunk_size.load(Ordering::Relaxed);
            let end = (start + chunk_size - 1).min(to);

            let resp: RpcResponse<Vec<Log>> = self
                .call(
                    "eth_getLogs",
                    serde_json::json!([{
                        "fromBlock": format!("0x{:x}", start),
                        "toBlock": format!("0x{:x}", end)
                    }]),
                )
                .await?;

            if let Some(err) = resp.error {
                // Check if this is a range limit error and adapt
                if Self::is_range_limit_error(&err.message) {
                    let old_chunk = self.log_chunk_size.load(Ordering::Relaxed);
                    let new_chunk = if let Some(suggested) = Self::parse_range_limit(&err.message) {
                        suggested.max(1)
                    } else {
                        // Fallback: halve the chunk size
                        (old_chunk / 2).max(1)
                    };

                    if new_chunk != old_chunk {
                        self.log_chunk_size.store(new_chunk, Ordering::Relaxed);
                        tracing::info!(
                            old_chunk = old_chunk,
                            new_chunk = new_chunk,
                            "Adapted eth_getLogs chunk size based on RPC limit"
                        );
                    }
                    // Retry this range with smaller chunk
                    continue;
                }
                return Err(anyhow!("eth_getLogs failed: {}", err.message));
            }

            if let Some(logs) = resp.result {
                all_logs.extend(logs);
            }

            start = end + 1;
        }

        Ok(all_logs)
    }

    /// Parse RPC error messages like "query exceeds max results 20000, retry with the range 1-244"
    /// Returns the suggested chunk size (end - start of the range)
    fn parse_range_limit(message: &str) -> Option<u64> {
        // Look for patterns like "range 1-244" or "range 1280-1385"
        if let Some(idx) = message.find("range") {
            let after_range = &message[idx + 5..]; // Skip "range"
            let after_range = after_range.trim_start_matches(|c: char| !c.is_ascii_digit());

            // Parse start number
            let start_str: String = after_range.chars().take_while(|c| c.is_ascii_digit()).collect();
            let start: u64 = start_str.parse().ok()?;

            // Find and skip the dash
            let rest = &after_range[start_str.len()..];
            let rest = rest.trim_start_matches(|c: char| !c.is_ascii_digit());

            // Parse end number
            let end_str: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
            let end: u64 = end_str.parse().ok()?;

            // Return the range size
            if end > start {
                return Some(end - start);
            }
        }
        None
    }

    /// Check if an error message indicates a range limit exceeded
    fn is_range_limit_error(message: &str) -> bool {
        message.contains("exceeds max results") || message.contains("block range")
    }

    async fn call<T: for<'de> Deserialize<'de>>(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<RpcResponse<T>> {
        // Acquire permit before making request (limits concurrent RPC calls)
        let _permit = self.concurrency_limiter.acquire().await
            .map_err(|_| anyhow!("RPC semaphore closed"))?;

        let req = RpcRequest {
            jsonrpc: "2.0",
            id: 1,
            method,
            params,
        };

        let start = Instant::now();
        let result = self
            .client
            .post(&self.url)
            .json(&req)
            .send()
            .await?
            .json()
            .await;

        let duration = start.elapsed();
        let success = result.is_ok();
        metrics::record_rpc_request(method, duration, success);

        Ok(result?)
    }
}
