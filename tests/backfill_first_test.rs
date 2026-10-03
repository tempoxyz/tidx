//! Backfill-first mode tests: every block up to the chain head must end up in
//! PostgreSQL, including the blocks above the highest stored block after a
//! lagging restart and the blocks produced while realtime sync was stalled.
//!
//! The node is an in-process JSON-RPC server so the tests control the chain head.
//!
//! Run with: cargo test --test backfill_first_test
//! Requires: docker compose -f docker/local/docker-compose.yml up -d postgres

mod common;

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::{Json, Router, extract::State, routing::post};
use common::testdb::TestDb;
use serde_json::{Value, json};
use serial_test::serial;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

use tidx::broadcast::{BlockUpdate, Broadcaster};
use tidx::db::ThrottledPool;
use tidx::sync::engine::SyncEngine;
use tidx::sync::sink::SinkSet;
use tidx::tempo::Block;

const CHAIN_ID: u64 = 31_337;
const TIMEOUT: Duration = Duration::from_secs(30);

// ── Mock node ──────────────────────────────────────────────────────────────

struct Head {
    num: u64,
    /// Produce a new block after every `eth_blockNumber` call.
    advance_on_poll: bool,
}

/// Chain of empty blocks up to an adjustable head.
#[derive(Clone)]
struct Chain {
    head: Arc<Mutex<Head>>,
    /// Empty block in the node's JSON encoding; `block` fills in the per-block fields.
    template: Arc<Value>,
    /// Timestamp of block 0 in seconds.
    genesis_secs: u64,
}

impl Chain {
    fn respond(&self, request: &Value) -> Value {
        let result = match request["method"].as_str() {
            Some("eth_chainId") => json!(format!("0x{CHAIN_ID:x}")),
            Some("eth_blockNumber") => {
                let mut head = self.head.lock().unwrap();
                let num = head.num;
                if head.advance_on_poll {
                    head.num += 1;
                }
                json!(format!("0x{num:x}"))
            }
            Some("eth_getBlockByNumber") => self.block(block_param(request)),
            Some("eth_getBlockReceipts") => self.receipts(block_param(request)),
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

    /// Full block `num`, or null above the head like a real node.
    fn block(&self, num: u64) -> Value {
        if num > self.head.lock().unwrap().num {
            return Value::Null;
        }
        let secs = self.genesis_secs + num;
        let mut block = (*self.template).clone();
        block["number"] = json!(format!("0x{num:x}"));
        // Block n has hash n + 1, so its parent hash is n and genesis has a zero parent.
        block["hash"] = json!(format!("0x{:064x}", num + 1));
        block["parentHash"] = json!(format!("0x{num:064x}"));
        block["timestamp"] = json!(format!("0x{secs:x}"));
        block["timestampMillis"] = json!(format!("0x{:x}", secs * 1000));
        block
    }

    fn receipts(&self, num: u64) -> Value {
        if num > self.head.lock().unwrap().num {
            return Value::Null;
        }
        json!([])
    }
}

fn block_param(request: &Value) -> u64 {
    let hex = request["params"][0].as_str().expect("block number param");
    u64::from_str_radix(hex.trim_start_matches("0x"), 16).expect("hex block number")
}

async fn rpc_handler(State(chain): State<Chain>, Json(body): Json<Value>) -> Json<Value> {
    Json(match &body {
        Value::Array(batch) => Value::Array(batch.iter().map(|req| chain.respond(req)).collect()),
        request => chain.respond(request),
    })
}

/// In-process JSON-RPC node answering `eth_chainId`, `eth_blockNumber`,
/// `eth_getBlockByNumber` and `eth_getBlockReceipts`, single or batched.
struct MockNode {
    url: String,
    head: Arc<Mutex<Head>>,
}

impl MockNode {
    async fn start(head: u64) -> Self {
        let header = tempo_alloy::rpc::TempoHeaderResponse {
            inner: alloy::rpc::types::Header::default(),
            timestamp_millis: 0,
        };
        let head = Arc::new(Mutex::new(Head {
            num: head,
            advance_on_poll: false,
        }));
        let chain = Chain {
            head: head.clone(),
            template: Arc::new(serde_json::to_value(Block::empty(header)).unwrap()),
            // Recent timestamps keep the rows in the weekly partitions migrations create.
            genesis_secs: chrono::Utc::now().timestamp() as u64 - 3600,
        };

        let app = Router::new()
            .route("/", post(rpc_handler))
            .with_state(chain);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("Failed to bind mock RPC server");
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("Mock RPC server failed");
        });

        Self { url, head }
    }

    fn set_head(&self, num: u64) {
        self.head.lock().unwrap().num = num;
    }

    fn advance_on_poll(&self) {
        self.head.lock().unwrap().advance_on_poll = true;
    }

    /// Stops producing blocks and returns the final head.
    fn freeze(&self) -> u64 {
        let mut head = self.head.lock().unwrap();
        head.advance_on_poll = false;
        head.num
    }
}

// ── Engine and database helpers ────────────────────────────────────────────

/// A backfill-first engine running in its own task.
struct RunningEngine {
    shutdown: broadcast::Sender<()>,
    task: JoinHandle<anyhow::Result<()>>,
    /// Block updates, which only realtime sync (phase 2) broadcasts.
    updates: broadcast::Receiver<BlockUpdate>,
}

impl RunningEngine {
    async fn start(db: &TestDb, node: &MockNode) -> Self {
        let broadcaster = Arc::new(Broadcaster::new());
        let updates = broadcaster.subscribe();
        let mut engine = SyncEngine::new(
            ThrottledPool::from_pool(db.pool.clone()),
            SinkSet::new(db.pool.clone()),
            &node.url,
        )
        .await
        .expect("Failed to create sync engine")
        .with_backfill_first(true)
        .with_broadcaster(broadcaster);

        let (shutdown, shutdown_rx) = broadcast::channel(1);
        let task = tokio::spawn(async move { engine.run(shutdown_rx).await });

        Self {
            shutdown,
            task,
            updates,
        }
    }

    async fn stop(self) {
        let _ = self.shutdown.send(());
        tokio::time::timeout(TIMEOUT, self.task)
            .await
            .expect("Engine did not shut down")
            .expect("Engine task panicked")
            .expect("Engine returned an error");
    }
}

/// Waits until blocks `1..=head` are stored without holes, then checks that
/// nothing else is stored.
async fn wait_for_blocks(db: &TestDb, head: u64) {
    let conn = db.pool.get().await.expect("Failed to get connection");
    let deadline = Instant::now() + TIMEOUT;

    loop {
        let missing: Vec<i64> = conn
            .query(
                "SELECT n FROM generate_series(1, $1::INT8) AS n \
                 WHERE NOT EXISTS (SELECT 1 FROM blocks WHERE num = n)",
                &[&(head as i64)],
            )
            .await
            .expect("Failed to query missing blocks")
            .iter()
            .map(|row| row.get(0))
            .collect();
        if missing.is_empty() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "Blocks {missing:?} of 1..={head} are still missing after {TIMEOUT:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let row = conn
        .query_one("SELECT COUNT(*), MAX(num) FROM blocks", &[])
        .await
        .expect("Failed to count blocks");
    assert_eq!(
        (row.get::<_, i64>(0), row.get::<_, i64>(1)),
        (head as i64, head as i64),
        "Only blocks 1..={head} should be stored, each once"
    );
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[tokio::test]
#[serial(db)]
async fn test_backfill_first_restart_while_lagging() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    // First run: the indexer stores blocks 1..=5 and goes down.
    let node = MockNode::start(5).await;
    let engine = RunningEngine::start(&db, &node).await;
    wait_for_blocks(&db, 5).await;
    engine.stop().await;

    // The chain moves on while the indexer is down.
    node.set_head(30);

    let engine = RunningEngine::start(&db, &node).await;
    wait_for_blocks(&db, 30).await;
    engine.stop().await;
}

#[tokio::test]
#[serial(db)]
async fn test_backfill_first_realtime_stall() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    let node = MockNode::start(20).await;
    let mut engine = RunningEngine::start(&db, &node).await;
    wait_for_blocks(&db, 20).await;

    // Only realtime sync broadcasts. Produce blocks one at a time until it
    // reports one, which proves phase 2 is running before the stall.
    let mut head = 20;
    loop {
        head += 1;
        node.set_head(head);
        let update = tokio::time::timeout(Duration::from_secs(2), engine.updates.recv()).await;
        if matches!(update, Ok(Ok(_))) {
            break;
        }
        assert!(head < 30, "Realtime sync never started");
    }

    // Realtime sync falls more than its ten-block tail window behind at once.
    node.set_head(head + 30);
    wait_for_blocks(&db, head + 30).await;

    engine.stop().await;
}

#[tokio::test]
#[serial(db)]
async fn test_backfill_first_reaches_realtime_on_moving_head() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    // A new block shows up every time the engine asks for the head, as on a
    // chain that is faster than one backfill round.
    let node = MockNode::start(20).await;
    node.advance_on_poll();
    let mut engine = RunningEngine::start(&db, &node).await;

    tokio::time::timeout(TIMEOUT, engine.updates.recv())
        .await
        .expect("Backfill never handed over to realtime sync")
        .expect("Broadcaster closed");

    let head = node.freeze();
    wait_for_blocks(&db, head).await;

    engine.stop().await;
}
