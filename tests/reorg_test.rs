//! Reorg handling of realtime sync, driven by an in-process JSON-RPC node that
//! can switch between two forks of a chain of empty blocks.

mod common;

use common::testdb::TestDb;

use alloy::consensus::BlockHeader as _;
use alloy::primitives::B256;
use axum::{Json, Router, extract::State, routing::post};
use serde_json::{Value, json};
use serial_test::serial;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tempo_alloy::rpc::TempoHeaderResponse;
use tidx::db::{Pool, ThrottledPool};
use tidx::sync::engine::SyncEngine;
use tidx::sync::sink::SinkSet;
use tidx::sync::writer::{
    load_archive_state, load_sync_state, rewind_tip_num, save_archive_state, update_synced_num,
    update_tip_num,
};
use tidx::tempo::Block;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

const CHAIN_ID: u64 = 0x7e57;
const FORK_A: u8 = 0xa;
const FORK_B: u8 = 0xb;
const TIMEOUT: Duration = Duration::from_secs(30);

/// JSON-RPC node serving one canonical chain, indexed by block number.
#[derive(Default)]
struct Node {
    canonical: Vec<Block>,
    /// Becomes the canonical chain once the next block batch has been answered.
    after_next_block_batch: Option<Vec<Block>>,
    /// Hashes of the blocks returned by every block batch answered so far.
    block_batches: Vec<Vec<B256>>,
}

impl Node {
    fn respond(&self, request: &Value) -> Value {
        let result = match request["method"].as_str() {
            Some("eth_chainId") => json!(format!("0x{CHAIN_ID:x}")),
            Some("eth_blockNumber") => json!(format!("0x{:x}", self.canonical.len() - 1)),
            Some("eth_getBlockByNumber") => self
                .block(request)
                .map_or(Value::Null, |block| serde_json::to_value(block).unwrap()),
            // All blocks are empty.
            Some("eth_getBlockReceipts") => json!([]),
            _ => {
                return json!({
                    "jsonrpc": "2.0",
                    "id": request["id"],
                    "error": { "code": -32601, "message": "method not found" }
                });
            }
        };
        json!({ "jsonrpc": "2.0", "id": request["id"], "result": result })
    }

    fn block(&self, request: &Value) -> Option<&Block> {
        let number = request["params"][0].as_str()?.strip_prefix("0x")?;
        self.canonical.get(usize::from_str_radix(number, 16).ok()?)
    }
}

/// Serves `canonical` on a local port and returns the node with its URL.
async fn start_node(canonical: Vec<Block>) -> (Arc<Mutex<Node>>, String) {
    let node = Arc::new(Mutex::new(Node {
        canonical,
        ..Default::default()
    }));
    let app = Router::new()
        .route("/", post(rpc_handler))
        .with_state(node.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("Failed to bind mock RPC server");
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("Mock RPC server failed");
    });
    (node, url)
}

async fn rpc_handler(State(node): State<Arc<Mutex<Node>>>, Json(body): Json<Value>) -> Json<Value> {
    let mut node = node.lock().unwrap();
    let Value::Array(requests) = body else {
        return Json(node.respond(&body));
    };

    let responses = requests
        .iter()
        .map(|request| node.respond(request))
        .collect();
    if requests
        .first()
        .is_some_and(|request| request["method"] == "eth_getBlockByNumber")
    {
        let served = hashes(requests.iter().filter_map(|request| node.block(request)));
        node.block_batches.push(served);
        if let Some(chain) = node.after_next_block_batch.take() {
            node.canonical = chain;
        }
    }
    Json(Value::Array(responses))
}

fn hashes<'a>(blocks: impl IntoIterator<Item = &'a Block>) -> Vec<B256> {
    blocks.into_iter().map(|block| block.header.hash).collect()
}

/// Extends `chain` up to block `head` with empty blocks whose hashes carry `fork`.
///
/// Timestamps advance by one second per block, so both forks of a common prefix
/// use the same `(timestamp, num)` key per height and a replaced block can only
/// be stored once the orphaned one is deleted.
fn extend(mut chain: Vec<Block>, fork: u8, head: usize) -> Vec<Block> {
    while chain.len() <= head {
        let number = chain.len() as u64;
        let (parent_hash, timestamp) = chain.last().map_or_else(
            || (B256::ZERO, chrono::Utc::now().timestamp() as u64 - 3600),
            |parent| (parent.header.hash, parent.header.timestamp() + 1),
        );
        let mut hash = B256::ZERO;
        hash[0] = fork;
        hash[24..].copy_from_slice(&number.to_be_bytes());

        let mut header = TempoHeaderResponse {
            inner: alloy::rpc::types::Header::default(),
            timestamp_millis: timestamp * 1000,
        };
        header.inner.hash = hash;
        let consensus = &mut header.inner.inner.inner;
        consensus.number = number;
        consensus.timestamp = timestamp;
        consensus.parent_hash = parent_hash;
        chain.push(Block::empty(header));
    }
    chain
}

/// Runs realtime sync against `rpc_url` with gap-fill disabled, so that nothing
/// but realtime sync can repair a reorged range.
async fn start_engine(pool: &Pool, rpc_url: &str) -> (broadcast::Sender<()>, JoinHandle<()>) {
    let mut engine = new_engine(pool, rpc_url).await.with_gapfill_enabled(false);

    let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
    let handle = tokio::spawn(async move {
        engine.run(shutdown_rx).await.expect("Sync engine failed");
    });
    (shutdown_tx, handle)
}

async fn new_engine(pool: &Pool, rpc_url: &str) -> SyncEngine {
    SyncEngine::new(
        ThrottledPool::from_pool(pool.clone()),
        SinkSet::new(pool.clone()),
        rpc_url,
    )
    .await
    .expect("Failed to create sync engine")
}

async fn stop_engine(shutdown: broadcast::Sender<()>, engine: JoinHandle<()>) {
    shutdown.send(()).expect("Sync engine already stopped");
    engine.await.expect("Sync engine panicked");
}

async fn wait_for_tip(pool: &Pool, tip: u64) {
    let deadline = Instant::now() + TIMEOUT;
    loop {
        let current = load_sync_state(pool, CHAIN_ID)
            .await
            .expect("Failed to load sync state")
            .map_or(0, |state| state.tip_num);
        if current == tip {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "Timed out waiting for tip_num {tip}, still at {current}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Asserts that the stored blocks are exactly the blocks of `chain` from at or
/// below `fork_point` up to its head and that each links to the one before it.
async fn assert_stored_chain(pool: &Pool, chain: &[Block], fork_point: usize) {
    let conn = pool.get().await.expect("Failed to get connection");
    let rows = conn
        .query(
            "SELECT num, hash, parent_hash FROM blocks ORDER BY num, timestamp",
            &[],
        )
        .await
        .expect("Failed to query blocks");
    let stored: Vec<(usize, B256, B256)> = rows
        .iter()
        .map(|row| {
            (
                row.get::<_, i64>(0) as usize,
                B256::from_slice(row.get(1)),
                B256::from_slice(row.get(2)),
            )
        })
        .collect();

    let head = chain.len() - 1;
    let first = stored.first().expect("No blocks stored").0;
    assert!(
        first <= fork_point,
        "Stored chain starts at {first}, above the fork point {fork_point}"
    );
    assert_eq!(
        stored.iter().map(|block| block.0).collect::<Vec<_>>(),
        (first..=head).collect::<Vec<_>>(),
        "Stored block numbers are not contiguous up to the head"
    );
    for (i, (num, hash, parent_hash)) in stored.iter().enumerate() {
        assert_eq!(
            *hash, chain[*num].header.hash,
            "Stored block {num} is not the canonical block"
        );
        if i > 0 {
            assert_eq!(
                *parent_hash,
                stored[i - 1].1,
                "Stored block {num} does not link to the stored block before it"
            );
        }
    }
}

#[tokio::test]
#[serial(db)]
async fn test_reorg_below_tip_is_refetched_without_gapfill() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    // Fork B replaces blocks 7..=10 of fork A and is four blocks longer.
    let fork_point = 6;
    let fork_a = extend(Vec::new(), FORK_A, 10);
    let fork_b = extend(fork_a[..=fork_point].to_vec(), FORK_B, 14);

    let (node, rpc_url) = start_node(fork_a.clone()).await;
    let (shutdown, engine) = start_engine(&db.pool, &rpc_url).await;

    wait_for_tip(&db.pool, 10).await;
    assert_stored_chain(&db.pool, &fork_a, fork_point).await;

    node.lock().unwrap().canonical = fork_b.clone();
    wait_for_tip(&db.pool, 14).await;
    stop_engine(shutdown, engine).await;

    assert_stored_chain(&db.pool, &fork_b, fork_point).await;
}

#[tokio::test]
#[serial(db)]
async fn test_fork_switch_between_pipelined_batches() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    // With an empty database and a head of 20 the first tick starts at block 10
    // and pipelines two batches: 10..=19 and 20. The node switches to fork B,
    // which replaces blocks 15 and up, in between the two.
    let fork_point = 14;
    let fork_a = extend(Vec::new(), FORK_A, 20);
    let fork_b = extend(fork_a[..=fork_point].to_vec(), FORK_B, 22);

    let (node, rpc_url) = start_node(fork_a.clone()).await;
    node.lock().unwrap().after_next_block_batch = Some(fork_b.clone());
    let (shutdown, engine) = start_engine(&db.pool, &rpc_url).await;

    wait_for_tip(&db.pool, 22).await;
    stop_engine(shutdown, engine).await;

    // The tick must have fetched a fork A batch with a fork B batch right behind it.
    let block_batches = node.lock().unwrap().block_batches.clone();
    assert_eq!(block_batches[0], hashes(&fork_a[10..=19]));
    assert_eq!(block_batches[1], hashes(&fork_b[20..=20]));
    assert_stored_chain(&db.pool, &fork_b, fork_point).await;
}

#[tokio::test]
#[serial(db)]
async fn test_deep_reorg_is_refetched_without_gapfill() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    // Fork B replaces blocks 13..=30 of fork A, so the fork point is more than
    // ten blocks below the head when the reorg is noticed.
    let fork_point = 12;
    let fork_a = extend(Vec::new(), FORK_A, 30);
    let fork_b = extend(fork_a[..=fork_point].to_vec(), FORK_B, 34);

    // The head rises in steps of ten so that realtime sync never jumps ahead and
    // fork A is stored without gaps down to below the fork point.
    let (node, rpc_url) = start_node(fork_a[..=10].to_vec()).await;
    let (shutdown, engine) = start_engine(&db.pool, &rpc_url).await;
    wait_for_tip(&db.pool, 10).await;
    for head in [20, 30] {
        node.lock().unwrap().canonical = fork_a[..=head].to_vec();
        wait_for_tip(&db.pool, head as u64).await;
    }
    assert_stored_chain(&db.pool, &fork_a, fork_point).await;

    node.lock().unwrap().canonical = fork_b.clone();
    wait_for_tip(&db.pool, 34).await;
    stop_engine(shutdown, engine).await;

    assert_stored_chain(&db.pool, &fork_b, fork_point).await;
}

#[tokio::test]
#[serial(db)]
async fn test_sync_range_stops_at_reorg() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    let fork_point = 6;
    let fork_a = extend(Vec::new(), FORK_A, 10);
    let fork_b = extend(fork_a[..=fork_point].to_vec(), FORK_B, 14);

    let (node, rpc_url) = start_node(fork_a).await;
    let engine = new_engine(&db.pool, &rpc_url).await;
    engine
        .sync_range(1, 10)
        .await
        .expect("Failed to sync fork A");

    node.lock().unwrap().canonical = fork_b.clone();
    let err = engine
        .sync_range(11, 14)
        .await
        .expect_err("Fork B batch was written on top of fork A");
    assert_eq!(
        err.to_string(),
        "Reorg detected at block 11: the stored chain was rewound to the fork point"
    );
    assert_stored_chain(&db.pool, &fork_b[..=fork_point], fork_point).await;

    engine
        .sync_range(7, 14)
        .await
        .expect("Failed to sync fork B from the fork point");
    assert_stored_chain(&db.pool, &fork_b, fork_point).await;
}

#[tokio::test]
#[serial(db)]
async fn test_rewind_tip_num_only_lowers() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    // Without a row there is nothing to lower and none must be created.
    rewind_tip_num(&db.pool, CHAIN_ID, 80)
        .await
        .expect("Failed to rewind");
    let state = load_sync_state(&db.pool, CHAIN_ID)
        .await
        .expect("Failed to load sync state");
    assert!(state.is_none(), "Rewind created a sync state row");

    update_tip_num(&db.pool, CHAIN_ID, 100, 105)
        .await
        .expect("Failed to update tip_num");
    update_synced_num(&db.pool, CHAIN_ID, 90)
        .await
        .expect("Failed to update synced_num");
    save_archive_state(&db.pool, CHAIN_ID, 1, 95)
        .await
        .expect("Failed to save archive state");

    rewind_tip_num(&db.pool, CHAIN_ID, 80)
        .await
        .expect("Failed to rewind");
    assert_eq!(pointers(&db.pool).await, (80, 80, 80));

    // A fork point above a pointer leaves that pointer alone.
    update_tip_num(&db.pool, CHAIN_ID, 100, 105)
        .await
        .expect("Failed to update tip_num");
    rewind_tip_num(&db.pool, CHAIN_ID, 90)
        .await
        .expect("Failed to rewind");
    assert_eq!(pointers(&db.pool).await, (90, 80, 80));

    let state = load_sync_state(&db.pool, CHAIN_ID)
        .await
        .expect("Failed to load sync state")
        .expect("No sync state");
    assert_eq!(state.head_num, 105, "head_num must not be rewound");
    let archive = load_archive_state(&db.pool, CHAIN_ID)
        .await
        .expect("Failed to load archive state");
    assert_eq!(archive.backfill_num, Some(1));
}

/// Returns `(tip_num, synced_num, archive_tip_num)`.
async fn pointers(pool: &Pool) -> (u64, u64, u64) {
    let state = load_sync_state(pool, CHAIN_ID)
        .await
        .expect("Failed to load sync state")
        .expect("No sync state");
    let archive = load_archive_state(pool, CHAIN_ID)
        .await
        .expect("Failed to load archive state");
    (state.tip_num, state.synced_num, archive.tip_num)
}
