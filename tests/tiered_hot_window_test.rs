mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::{Json, Router, routing::post};
use chrono::{DateTime, Utc};
use common::testdb::TestDb;
use serde_json::{Value, json};
use serial_test::serial;
use tidx::config::RetentionConfig;
use tidx::db::partitions::{week_index, week_start};
use tidx::sync::sink::SinkSet;
use tidx::sync::tiered_sync::TieredSync;
use tidx::sync::writer::{has_gaps, load_sync_state, update_tip_num};
use tidx::tempo::Block;

const CHAIN_ID: u64 = 7_777;
const HEAD: u64 = 2_000;

/// A chain whose blocks are evenly spread over the last two weeks.
#[derive(Clone)]
struct Chain {
    template: Value,
    first_timestamp: i64,
    spacing_secs: i64,
    /// `eth_getBlockByNumber` calls made outside batch requests.
    single_block_calls: Arc<AtomicUsize>,
    /// `eth_blockNumber` calls, one per reconciler tick.
    head_calls: Arc<AtomicUsize>,
}

impl Chain {
    fn new(now: DateTime<Utc>) -> Self {
        let header = tempo_alloy::rpc::TempoHeaderResponse {
            inner: alloy::rpc::types::Header::default(),
            timestamp_millis: 0,
        };
        let span = 14 * 24 * 60 * 60;
        Self {
            template: serde_json::to_value(Block::empty(header)).unwrap(),
            first_timestamp: now.timestamp() - span,
            spacing_secs: span / HEAD as i64,
            single_block_calls: Arc::default(),
            head_calls: Arc::default(),
        }
    }

    fn timestamp(&self, num: u64) -> i64 {
        self.first_timestamp + num as i64 * self.spacing_secs
    }

    fn block(&self, num: u64) -> Value {
        let mut block = self.template.clone();
        block["number"] = json!(format!("0x{num:x}"));
        block["hash"] = json!(format!("0x{num:064x}"));
        block["parentHash"] = json!(format!("0x{:064x}", num.saturating_sub(1)));
        block["timestamp"] = json!(format!("0x{:x}", self.timestamp(num)));
        block
    }

    fn respond(&self, request: &Value) -> Value {
        let result = match request["method"].as_str().unwrap() {
            "eth_chainId" => json!(format!("0x{CHAIN_ID:x}")),
            "eth_blockNumber" => {
                self.head_calls.fetch_add(1, Ordering::SeqCst);
                json!(format!("0x{HEAD:x}"))
            }
            "eth_getBlockByNumber" => {
                let num = request["params"][0].as_str().unwrap();
                self.block(u64::from_str_radix(num.trim_start_matches("0x"), 16).unwrap())
            }
            "eth_getBlockReceipts" => json!([]),
            method => panic!("unexpected RPC method {method}"),
        };
        json!({ "jsonrpc": "2.0", "id": request["id"], "result": result })
    }

    async fn serve(self) -> String {
        let app = Router::new().route(
            "/",
            post(move |Json(body): Json<Value>| {
                let chain = self.clone();
                async move {
                    Json(match body.as_array() {
                        Some(batch) => {
                            Value::Array(batch.iter().map(|r| chain.respond(r)).collect())
                        }
                        None => {
                            if body["method"] == "eth_getBlockByNumber" {
                                chain.single_block_calls.fetch_add(1, Ordering::SeqCst);
                            }
                            chain.respond(&body)
                        }
                    })
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        url
    }
}

#[tokio::test]
#[serial(db)]
async fn test_hot_window_hydrates_from_rpc_without_repeating_floor_search() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    let now = Utc::now();
    let chain = Chain::new(now);
    let single_block_calls = chain.single_block_calls.clone();
    let boundary_ts = week_start(week_index(now - chrono::Duration::days(3)));
    let floor = (1..=HEAD)
        .find(|&num| chain.timestamp(num) >= boundary_ts.timestamp())
        .unwrap();
    let rpc_url = chain.serve().await;
    // The reconciler only fills up to the tip realtime sync has reached.
    update_tip_num(&db.pool, CHAIN_ID, HEAD, HEAD)
        .await
        .unwrap();

    let retention = RetentionConfig {
        pg_keep: "3d".to_string(),
        prune_interval: "1h".to_string(),
        require_clickhouse: false,
    };
    let tiered = TieredSync::new(
        SinkSet::new(db.pool.clone()),
        &rpc_url,
        CHAIN_ID,
        &retention,
        20,
        2,
    )
    .unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);
    let handle = tokio::spawn(tiered.run(shutdown_rx));

    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let state = load_sync_state(&db.pool, CHAIN_ID).await.unwrap();
            if state.is_some_and(|s| s.pruned_below == floor - 1 && s.synced_num == HEAD) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("hot window was not hydrated");
    let _ = shutdown_tx.send(());
    let _ = handle.await;

    assert!(!has_gaps(&db.pool, floor, HEAD).await.unwrap());
    let below_floor: i64 = db
        .pool
        .get()
        .await
        .unwrap()
        .query_one(
            "SELECT COUNT(*) FROM blocks WHERE num < $1",
            &[&(floor as i64)],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        below_floor, 0,
        "blocks below the hot window must not be fetched"
    );

    // Locating the floor is a binary search over block timestamps. It only
    // depends on the week-aligned boundary, so it must not be repeated for
    // every hydrated batch.
    let calls = single_block_calls.load(Ordering::SeqCst);
    let one_search = 2 + (HEAD as f64).log2().ceil() as usize;
    assert!(
        calls <= one_search,
        "{calls} single-block RPC calls for {} hydrated blocks, one floor search takes {one_search}",
        HEAD - floor + 1
    );
}

#[tokio::test]
#[serial(db)]
async fn test_hot_window_without_clickhouse_stops_at_realtime_tip() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    let now = Utc::now();
    let chain = Chain::new(now);
    let boundary_ts = week_start(week_index(now - chrono::Duration::days(3)));
    let floor = (1..=HEAD)
        .find(|&num| chain.timestamp(num) >= boundary_ts.timestamp())
        .unwrap();
    let rpc_url = chain.serve().await;
    // Realtime sync is a few blocks below the RPC head.
    let tip = HEAD - 5;
    update_tip_num(&db.pool, CHAIN_ID, tip, HEAD).await.unwrap();

    let retention = RetentionConfig {
        pg_keep: "3d".to_string(),
        prune_interval: "1h".to_string(),
        require_clickhouse: false,
    };
    let tiered = TieredSync::new(
        SinkSet::new(db.pool.clone()),
        &rpc_url,
        CHAIN_ID,
        &retention,
        20,
        2,
    )
    .unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);
    let handle = tokio::spawn(tiered.run(shutdown_rx));

    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let state = load_sync_state(&db.pool, CHAIN_ID).await.unwrap();
            if state.is_some_and(|s| s.pruned_below == floor - 1 && s.synced_num == tip) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("hot window was not hydrated up to the realtime tip");
    let _ = shutdown_tx.send(());
    let _ = handle.await;

    assert!(!has_gaps(&db.pool, floor, tip).await.unwrap());
    let above_tip: i64 = db
        .pool
        .get()
        .await
        .unwrap()
        .query_one(
            "SELECT COUNT(*) FROM blocks WHERE num > $1",
            &[&(tip as i64)],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        above_tip, 0,
        "blocks above the realtime tip are not fetched"
    );
}

#[tokio::test]
#[serial(db)]
async fn test_hot_window_without_clickhouse_waits_for_stale_realtime_tip() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    let now = Utc::now();
    let chain = Chain::new(now);
    let head_calls = chain.head_calls.clone();
    let boundary_ts = week_start(week_index(now - chrono::Duration::days(3)));
    let floor = (1..=HEAD)
        .find(|&num| chain.timestamp(num) >= boundary_ts.timestamp())
        .unwrap();
    let rpc_url = chain.serve().await;
    // After a restart the stored tip is far behind the RPC head until realtime sync jumps
    // to the head's tail.
    let stale_tip = HEAD - 100;
    update_tip_num(&db.pool, CHAIN_ID, stale_tip, stale_tip)
        .await
        .unwrap();

    let retention = RetentionConfig {
        pg_keep: "3d".to_string(),
        prune_interval: "1h".to_string(),
        require_clickhouse: false,
    };
    let tiered = TieredSync::new(
        SinkSet::new(db.pool.clone()),
        &rpc_url,
        CHAIN_ID,
        &retention,
        20,
        2,
    )
    .unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);
    let handle = tokio::spawn(tiered.run(shutdown_rx));

    // A second tick means the first one returned a short wait instead of the one hour
    // reconcile interval.
    tokio::time::timeout(Duration::from_secs(30), async {
        while head_calls.load(Ordering::SeqCst) < 2 {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("reconciler did not tick again while realtime is behind");
    let state = load_sync_state(&db.pool, CHAIN_ID).await.unwrap().unwrap();
    assert_eq!(state.pruned_below, 0, "the hot boundary must not move");
    assert_eq!(state.synced_num, 0);
    let rows: i64 = db
        .pool
        .get()
        .await
        .unwrap()
        .query_one("SELECT COUNT(*) FROM blocks", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(rows, 0, "nothing is filled below a stale realtime tip");

    // Realtime sync jumped to within its tail window of the head, leaving everything below
    // it empty.
    let tip = HEAD - 5;
    update_tip_num(&db.pool, CHAIN_ID, tip, HEAD).await.unwrap();

    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let state = load_sync_state(&db.pool, CHAIN_ID).await.unwrap();
            if state.is_some_and(|s| s.pruned_below == floor - 1 && s.synced_num == tip) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("gap below the realtime tip was not filled");
    let _ = shutdown_tx.send(());
    let _ = handle.await;

    assert!(!has_gaps(&db.pool, floor, tip).await.unwrap());
}
