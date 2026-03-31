use anyhow::{Result, anyhow};
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
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
    id: Option<u64>,
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
        let resp: RpcResponse<String> = self.call("eth_blockNumber", serde_json::json!([])).await?;
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
        resp.result.ok_or_else(|| anyhow!("Block {num} not found"))
    }

    pub async fn get_blocks_batch(&self, range: RangeInclusive<u64>) -> Result<Vec<Block>> {
        // Acquire permit (batch counts as one request for concurrency limiting)
        let _permit = self
            .concurrency_limiter
            .acquire()
            .await
            .map_err(|_| anyhow!("RPC semaphore closed"))?;

        let expected_count = (range.end() - range.start() + 1) as usize;

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

        let response = self.client.post(&self.url).json(&batch).send().await?;

        let status = response.status();
        let body = response.text().await?;

        if !status.is_success() {
            tracing::error!(
                status = %status,
                body_preview = %body.chars().take(500).collect::<String>(),
                from = %range.start(),
                to = %range.end(),
                "RPC request failed"
            );
            anyhow::bail!(
                "RPC request failed with status {}: {}",
                status,
                body.chars().take(200).collect::<String>()
            );
        }

        // Check if the RPC returned a single error object instead of an array
        if let Ok(single_error) = serde_json::from_str::<RpcResponse<serde_json::Value>>(&body) {
            if let Some(err) = single_error.error {
                anyhow::bail!("RPC batch error: {}", err.message);
            }
        }

        let responses: Vec<RpcResponse<Block>> = serde_json::from_str(&body).map_err(|e| {
            tracing::error!(
                error = %e,
                body_preview = %body.chars().take(500).collect::<String>(),
                from = %range.start(),
                to = %range.end(),
                "Failed to decode blocks response"
            );
            anyhow!("Failed to decode blocks response: {}", e)
        })?;

        // Finding 3: Detect truncated batch responses
        if responses.len() != expected_count {
            anyhow::bail!(
                "Truncated block batch response: expected {} items, got {} (range {}..={})",
                expected_count,
                responses.len(),
                range.start(),
                range.end()
            );
        }

        // Finding 1: Match responses by JSON-RPC id field (not positional order)
        let mut blocks: Vec<Option<Block>> = vec![None; expected_count];
        for resp in responses {
            if let Some(err) = resp.error {
                anyhow::bail!(
                    "RPC error in block batch item id={:?}: {}",
                    resp.id,
                    err.message
                );
            }
            let id = resp
                .id
                .ok_or_else(|| anyhow!("Block batch response item missing id field"))?
                as usize;
            if id >= expected_count {
                anyhow::bail!(
                    "Block batch response id {id} out of range (expected 0..{expected_count})"
                );
            }
            let block = resp.result.ok_or_else(|| {
                anyhow!("Block {} not found", range.start() + id as u64)
            })?;
            // Finding 2: Verify block number matches the requested height
            let expected_num = range.start() + id as u64;
            if block.header.number != expected_num {
                anyhow::bail!(
                    "Block number mismatch: requested {} but RPC returned block {}",
                    expected_num,
                    block.header.number
                );
            }
            blocks[id] = Some(block);
        }

        blocks
            .into_iter()
            .enumerate()
            .map(|(i, b)| {
                b.ok_or_else(|| {
                    anyhow!(
                        "Missing response for block {} (duplicate id in batch?)",
                        range.start() + i as u64
                    )
                })
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
    pub async fn get_receipts_batch(
        &self,
        range: RangeInclusive<u64>,
    ) -> Result<Vec<Vec<Receipt>>> {
        // Acquire permit (batch counts as one request for concurrency limiting)
        let _permit = self
            .concurrency_limiter
            .acquire()
            .await
            .map_err(|_| anyhow!("RPC semaphore closed"))?;

        let expected_count = (range.end() - range.start() + 1) as usize;

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

        let response = self.client.post(&self.url).json(&batch).send().await?;

        let status = response.status();
        let body = response.text().await?;

        if !status.is_success() {
            tracing::error!(
                status = %status,
                body_preview = %body.chars().take(500).collect::<String>(),
                from = %range.start(),
                to = %range.end(),
                "RPC receipts request failed"
            );
            anyhow::bail!(
                "RPC receipts request failed with status {}: {}",
                status,
                body.chars().take(200).collect::<String>()
            );
        }

        // Check if the RPC returned a single error object instead of an array
        // This happens when the entire batch request fails (e.g., response too large)
        if let Ok(single_error) = serde_json::from_str::<RpcResponse<serde_json::Value>>(&body) {
            if let Some(err) = single_error.error {
                anyhow::bail!("RPC batch error: {}", err.message);
            }
        }

        let responses: Vec<RpcResponse<Vec<Receipt>>> =
            serde_json::from_str(&body).map_err(|e| {
                tracing::error!(
                    error = %e,
                    body_preview = %body.chars().take(500).collect::<String>(),
                    from = %range.start(),
                    to = %range.end(),
                    "Failed to decode receipts response"
                );
                anyhow!("Failed to decode receipts response: {}", e)
            })?;

        // Detect truncated batch responses
        if responses.len() != expected_count {
            anyhow::bail!(
                "Truncated receipt batch response: expected {} items, got {} (range {}..={})",
                expected_count,
                responses.len(),
                range.start(),
                range.end()
            );
        }

        // Match responses by JSON-RPC id field and check per-item errors
        let mut results: Vec<Option<Vec<Receipt>>> = vec![None; expected_count];
        for resp in responses {
            // Finding 4: Check per-item errors instead of silently dropping them
            if let Some(err) = resp.error {
                anyhow::bail!(
                    "RPC error in receipt batch item id={:?} (block {}): {}",
                    resp.id,
                    resp.id
                        .map(|id| range.start() + id)
                        .map_or("unknown".to_string(), |n| n.to_string()),
                    err.message
                );
            }
            let id = resp
                .id
                .ok_or_else(|| anyhow!("Receipt batch response item missing id field"))?
                as usize;
            if id >= expected_count {
                anyhow::bail!(
                    "Receipt batch response id {id} out of range (expected 0..{expected_count})"
                );
            }
            results[id] = Some(resp.result.unwrap_or_default());
        }

        results
            .into_iter()
            .enumerate()
            .map(|(i, r)| {
                r.ok_or_else(|| {
                    anyhow!(
                        "Missing receipt response for block {} (duplicate id in batch?)",
                        range.start() + i as u64
                    )
                })
            })
            .collect()
    }

    /// Fetch receipts for a range, recursively splitting when the batch
    /// response is too large for the RPC to serve in one request.
    pub fn get_receipts_batch_adaptive(
        &self,
        range: RangeInclusive<u64>,
    ) -> BoxFuture<'_, Result<Vec<Vec<Receipt>>>> {
        Box::pin(async move {
            let from = *range.start();
            let to = *range.end();

            match self.get_receipts_batch(range).await {
                Ok(receipts) => Ok(receipts),
                Err(e) if Self::is_receipts_batch_too_large(&e) => {
                    if from == to {
                        anyhow::bail!(
                            "Single block {from} receipts exceed RPC response size limit"
                        );
                    }

                    let mid = from + (to - from) / 2;
                    tracing::debug!(
                        from,
                        to,
                        mid,
                        "RPC receipts batch too large, splitting range"
                    );

                    let mut left = self.get_receipts_batch_adaptive(from..=mid).await?;
                    let right = self.get_receipts_batch_adaptive((mid + 1)..=to).await?;
                    left.extend(right);
                    Ok(left)
                }
                Err(e) => Err(e),
            }
        })
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
            let start_str: String = after_range
                .chars()
                .take_while(|c| c.is_ascii_digit())
                .collect();
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

    fn is_receipts_batch_too_large(err: &anyhow::Error) -> bool {
        let message = err.to_string().to_lowercase();
        message.contains("too large") || message.contains("response size exceeded")
    }

    async fn call<T: for<'de> Deserialize<'de>>(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<RpcResponse<T>> {
        // Acquire permit before making request (limits concurrent RPC calls)
        let _permit = self
            .concurrency_limiter
            .acquire()
            .await
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Json, Router, routing::post};
    use serde_json::{Value, json};
    use std::sync::Arc;
    use tokio::sync::Mutex;

    // ---- helpers ----

    /// Minimal valid block JSON for a given number
    fn fake_block_json(number: u64) -> Value {
        json!({
            "hash": format!("0x{:064x}", number),
            "number": format!("0x{:x}", number),
            "parentHash": format!("0x{:064x}", number.wrapping_sub(1)),
            "timestamp": "0x0",
            "gasLimit": "0x0",
            "gasUsed": "0x0",
            "miner": "0x0000000000000000000000000000000000000000",
            "extraData": "0x",
            "baseFeePerGas": "0x0",
            "difficulty": "0x0",
            "mixHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
            "nonce": "0x0000000000000000",
            "sha3Uncles": "0x0000000000000000000000000000000000000000000000000000000000000000",
            "logsBloom": "0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
            "receiptsRoot": "0x0000000000000000000000000000000000000000000000000000000000000000",
            "stateRoot": "0x0000000000000000000000000000000000000000000000000000000000000000",
            "transactionsRoot": "0x0000000000000000000000000000000000000000000000000000000000000000",
            "size": "0x0",
            "totalDifficulty": "0x0",
            "uncles": [],
            "transactions": []
        })
    }

    /// Spin up a test RPC server, returns (url, abort handle)
    macro_rules! test_server {
        ($app:expr) => {{
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("Failed to bind test RPC server");
            let addr = listener.local_addr().unwrap();
            let url = format!("http://127.0.0.1:{}", addr.port());
            let handle = tokio::spawn(async move {
                axum::serve(listener, $app).await.expect("RPC server failed");
            });
            (url, handle)
        }};
    }

    // ---- adaptive receipt split test ----

    #[tokio::test]
    async fn test_get_receipts_batch_adaptive_splits_and_preserves_order() {
        let request_sizes: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(Vec::new()));
        let sizes_clone = request_sizes.clone();

        let app = Router::new().route(
            "/",
            post(move |Json(body): Json<Value>| {
                let sizes = sizes_clone.clone();
                async move {
                    let requests = body.as_array().expect("expected batch request");
                    let batch_size = requests.len();
                    sizes.lock().await.push(batch_size);

                    if batch_size > 2 {
                        Json(json!({
                            "jsonrpc": "2.0",
                            "id": 0,
                            "error": { "code": -32000, "message": "response size exceeded" }
                        }))
                    } else {
                        let responses: Vec<Value> = requests
                            .iter()
                            .map(|req| json!({ "jsonrpc": "2.0", "id": req["id"], "result": [] }))
                            .collect();
                        Json(Value::Array(responses))
                    }
                }
            }),
        );

        let (url, server) = test_server!(app);
        let client = RpcClient::new(&url);
        let receipts = client
            .get_receipts_batch_adaptive(1..=5)
            .await
            .expect("adaptive receipt fetch should succeed");

        assert_eq!(receipts.len(), 5);
        assert!(receipts.iter().all(Vec::is_empty));

        let sizes = request_sizes.lock().await.clone();
        assert_eq!(sizes, vec![5, 3, 2, 1, 2]);
        server.abort();
    }

    // ---- Finding 1: Out-of-order responses matched by id ----

    #[tokio::test]
    async fn test_blocks_batch_reordered_responses_matched_by_id() {
        let app = Router::new().route(
            "/",
            post(|Json(body): Json<Value>| async move {
                let requests = body.as_array().expect("expected batch");
                let mut responses: Vec<Value> = requests
                    .iter()
                    .map(|req| {
                        let id = req["id"].as_u64().unwrap();
                        let block_hex = req["params"][0].as_str().unwrap();
                        let num = u64::from_str_radix(block_hex.trim_start_matches("0x"), 16)
                            .unwrap();
                        json!({ "jsonrpc": "2.0", "id": id, "result": fake_block_json(num) })
                    })
                    .collect();
                responses.reverse(); // respond in reverse order
                Json(Value::Array(responses))
            }),
        );

        let (url, server) = test_server!(app);
        let client = RpcClient::new(&url);

        let blocks = client
            .get_blocks_batch(10..=12)
            .await
            .expect("should handle reordered responses");

        assert_eq!(blocks.len(), 3);
        assert_eq!(blocks[0].header.number, 10);
        assert_eq!(blocks[1].header.number, 11);
        assert_eq!(blocks[2].header.number, 12);
        server.abort();
    }

    // ---- Happy path: in-order block responses ----

    #[tokio::test]
    async fn test_blocks_batch_happy_path_in_order() {
        let app = Router::new().route(
            "/",
            post(|Json(body): Json<Value>| async move {
                let requests = body.as_array().expect("expected batch");
                let responses: Vec<Value> = requests
                    .iter()
                    .map(|req| {
                        let id = req["id"].as_u64().unwrap();
                        let block_hex = req["params"][0].as_str().unwrap();
                        let num = u64::from_str_radix(block_hex.trim_start_matches("0x"), 16)
                            .unwrap();
                        json!({ "jsonrpc": "2.0", "id": id, "result": fake_block_json(num) })
                    })
                    .collect();
                Json(Value::Array(responses))
            }),
        );

        let (url, server) = test_server!(app);
        let client = RpcClient::new(&url);

        let blocks = client
            .get_blocks_batch(5..=9)
            .await
            .expect("happy path should succeed");

        assert_eq!(blocks.len(), 5);
        for (i, block) in blocks.iter().enumerate() {
            assert_eq!(block.header.number, 5 + i as u64);
        }
        server.abort();
    }

    // ---- Finding 2: Block number mismatch detection ----

    #[tokio::test]
    async fn test_blocks_batch_detects_number_mismatch() {
        let app = Router::new().route(
            "/",
            post(|Json(body): Json<Value>| async move {
                let requests = body.as_array().expect("expected batch");
                let responses: Vec<Value> = requests
                    .iter()
                    .map(|req| {
                        let id = req["id"].as_u64().unwrap();
                        // Return wrong block number (always block 999)
                        json!({ "jsonrpc": "2.0", "id": id, "result": fake_block_json(999) })
                    })
                    .collect();
                Json(Value::Array(responses))
            }),
        );

        let (url, server) = test_server!(app);
        let client = RpcClient::new(&url);

        let err = client
            .get_blocks_batch(10..=12)
            .await
            .expect_err("should detect block number mismatch");

        assert!(
            err.to_string().contains("Block number mismatch"),
            "expected mismatch error, got: {err}"
        );
        server.abort();
    }

    // ---- Finding 3: Truncated response detection ----

    #[tokio::test]
    async fn test_blocks_batch_detects_truncated_response() {
        let app = Router::new().route(
            "/",
            post(|Json(body): Json<Value>| async move {
                let requests = body.as_array().expect("expected batch");
                // Return only the first response
                let req = &requests[0];
                let id = req["id"].as_u64().unwrap();
                let block_hex = req["params"][0].as_str().unwrap();
                let num =
                    u64::from_str_radix(block_hex.trim_start_matches("0x"), 16).unwrap();
                Json(json!([{
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": fake_block_json(num)
                }]))
            }),
        );

        let (url, server) = test_server!(app);
        let client = RpcClient::new(&url);

        let err = client
            .get_blocks_batch(10..=14)
            .await
            .expect_err("should detect truncated response");

        assert!(
            err.to_string().contains("Truncated block batch response"),
            "expected truncation error, got: {err}"
        );
        server.abort();
    }

    // ---- Finding 4: Per-item receipt error detection ----

    #[tokio::test]
    async fn test_receipts_batch_detects_per_item_error() {
        let app = Router::new().route(
            "/",
            post(|Json(body): Json<Value>| async move {
                let requests = body.as_array().expect("expected batch");
                let responses: Vec<Value> = requests
                    .iter()
                    .map(|req| {
                        let id = req["id"].as_u64().unwrap();
                        if id == 1 {
                            json!({
                                "jsonrpc": "2.0",
                                "id": id,
                                "error": { "code": -32000, "message": "block not available" }
                            })
                        } else {
                            json!({ "jsonrpc": "2.0", "id": id, "result": [] })
                        }
                    })
                    .collect();
                Json(Value::Array(responses))
            }),
        );

        let (url, server) = test_server!(app);
        let client = RpcClient::new(&url);

        let err = client
            .get_receipts_batch(100..=102)
            .await
            .expect_err("should detect per-item receipt error");

        assert!(
            err.to_string().contains("block not available"),
            "expected per-item error propagated, got: {err}"
        );
        server.abort();
    }

    // ---- Receipt happy path with reordering ----

    #[tokio::test]
    async fn test_receipts_batch_reordered_responses() {
        let app = Router::new().route(
            "/",
            post(|Json(body): Json<Value>| async move {
                let requests = body.as_array().expect("expected batch");
                let mut responses: Vec<Value> = requests
                    .iter()
                    .map(|req| json!({ "jsonrpc": "2.0", "id": req["id"], "result": [] }))
                    .collect();
                responses.reverse();
                Json(Value::Array(responses))
            }),
        );

        let (url, server) = test_server!(app);
        let client = RpcClient::new(&url);

        let receipts = client
            .get_receipts_batch(1..=4)
            .await
            .expect("should handle reordered receipt responses");

        assert_eq!(receipts.len(), 4);
        assert!(receipts.iter().all(Vec::is_empty));
        server.abort();
    }
}
