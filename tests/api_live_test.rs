mod common;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::connect_info::IntoMakeServiceWithConnectInfo;
use axum::http::{Request, StatusCode};
use tower::Service;

use common::testdb::TestDb;
use serial_test::serial;
use tidx::api::{self, inject_block_filter};
use tidx::broadcast::{BlockUpdate, Broadcaster};
use tidx::service::{PostgresQuery, QueryOptions};

fn make_pools(pool: tidx::db::Pool) -> (HashMap<u64, tidx::db::Pool>, u64) {
    let mut pools = HashMap::new();
    let chain_id = 1u64;
    pools.insert(chain_id, pool);
    (pools, chain_id)
}

/// Create a test service that includes ConnectInfo.
async fn make_test_service(
    pools: HashMap<u64, tidx::db::Pool>,
    chain_id: u64,
    broadcaster: Arc<Broadcaster>,
) -> impl Service<Request<Body>, Response = axum::response::Response, Error = std::convert::Infallible>
{
    let mut svc: IntoMakeServiceWithConnectInfo<Router, SocketAddr> =
        api::router(pools, chain_id, broadcaster)
            .unwrap()
            .into_make_service_with_connect_info::<SocketAddr>();
    svc.call(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .unwrap()
}

#[tokio::test]
#[serial(db)]
async fn test_health_endpoint() {
    let db = TestDb::empty().await;
    let broadcaster = Arc::new(Broadcaster::new());
    let (pools, chain_id) = make_pools(db.pool.clone());
    let mut app = make_test_service(pools, chain_id, broadcaster).await;

    let response = app
        .call(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(&body[..], b"OK");
}

#[tokio::test]
#[serial(db)]
async fn test_status_endpoint() {
    let db = TestDb::new().await;
    let broadcaster = Arc::new(Broadcaster::new());
    let (pools, chain_id) = make_pools(db.pool.clone());
    let mut app = make_test_service(pools, chain_id, broadcaster).await;

    let response = app
        .call(
            Request::builder()
                .uri("/status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["ok"], true);
    assert!(json["version"].is_string());
    assert_ne!(json["version"].as_str().unwrap(), "");
    assert!(json["rev"].is_string());
    assert_ne!(json["rev"].as_str().unwrap(), "");
    assert!(json["chains"].is_array());
}

#[tokio::test]
#[serial(db)]
async fn test_query_select_blocks() {
    let db = TestDb::new().await;
    let broadcaster = Arc::new(Broadcaster::new());
    let (pools, chain_id) = make_pools(db.pool.clone());
    let mut app = make_test_service(pools, chain_id, broadcaster).await;

    let response = app
        .call(
            Request::builder()
                .method("GET")
                .uri("/query?sql=SELECT%20num,%20hash%20FROM%20blocks%20ORDER%20BY%20num%20DESC%20LIMIT%205&chainId=1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["ok"], true);
    assert_eq!(json["columns"], serde_json::json!(["num", "hash"]));
    assert!(
        json["row_count"].as_u64().unwrap() > 0,
        "expected indexed blocks"
    );
}

#[tokio::test]
#[serial(db)]
async fn test_query_select_txs() {
    let db = TestDb::new().await;
    let broadcaster = Arc::new(Broadcaster::new());
    let (pools, chain_id) = make_pools(db.pool.clone());
    let mut app = make_test_service(pools, chain_id, broadcaster).await;

    let response = app
        .call(
            Request::builder()
                .method("GET")
                .uri("/query?sql=SELECT%20block_num,%20hash,%20%22from%22%20FROM%20txs%20LIMIT%2010&chainId=1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["ok"], true);
    assert_eq!(
        json["columns"],
        serde_json::json!(["block_num", "hash", "from"])
    );
}

#[tokio::test]
#[serial(db)]
async fn test_query_select_logs() {
    let db = TestDb::new().await;
    let broadcaster = Arc::new(Broadcaster::new());
    let (pools, chain_id) = make_pools(db.pool.clone());
    let mut app = make_test_service(pools, chain_id, broadcaster).await;

    let response = app
        .call(
            Request::builder()
                .method("GET")
                .uri("/query?sql=SELECT%20block_num,%20address,%20selector%20FROM%20logs%20LIMIT%2010&chainId=1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["ok"], true);
    // Logs table may be empty if no contracts emitted events
    let columns = json["columns"].as_array().unwrap();
    if !columns.is_empty() {
        assert_eq!(
            json["columns"],
            serde_json::json!(["block_num", "address", "selector"])
        );
    }
}

#[tokio::test]
#[serial(db)]
async fn test_query_with_signature_cte() {
    let db = TestDb::new().await;
    let broadcaster = Arc::new(Broadcaster::new());
    let (pools, chain_id) = make_pools(db.pool.clone());
    let mut app = make_test_service(pools, chain_id, broadcaster).await;

    // URL encode: spaces=%20, commas=%2C, parens=%28/%29
    let sig = "Transfer(address%20indexed%20from%2Caddress%20indexed%20to%2Cuint256%20value)";
    let uri =
        format!("/query?sql=SELECT%20*%20FROM%20Transfer%20LIMIT%205&chainId=1&signature={sig}");

    let response = app
        .call(
            Request::builder()
                .method("GET")
                .uri(&uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    // Signature CTE may fail if logs table is empty - 422 is acceptable
    if status == StatusCode::OK {
        assert_eq!(json["ok"], true);
        let columns = json["columns"].as_array().unwrap();
        if !columns.is_empty() {
            assert!(
                columns.iter().any(|c| c == "from"),
                "expected 'from' column"
            );
            assert!(columns.iter().any(|c| c == "to"), "expected 'to' column");
            assert!(
                columns.iter().any(|c| c == "value"),
                "expected 'value' column"
            );
        }
    } else {
        // 422 is acceptable if no matching logs exist
        assert!(
            status == StatusCode::UNPROCESSABLE_ENTITY,
            "unexpected status: {}, body: {}",
            status,
            json
        );
    }
}

#[tokio::test]
#[serial(db)]
async fn test_query_rejects_non_select() {
    let db = TestDb::empty().await;
    let broadcaster = Arc::new(Broadcaster::new());
    let (pools, chain_id) = make_pools(db.pool.clone());
    let mut app = make_test_service(pools, chain_id, broadcaster).await;

    let response = app
        .call(
            Request::builder()
                .method("GET")
                .uri("/query?sql=DELETE%20FROM%20blocks&chainId=1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["ok"], false);
    assert!(json["error"].as_str().unwrap().contains("SELECT"));
}

#[tokio::test]
#[serial(db)]
async fn test_query_chain_id_param() {
    let db = TestDb::new().await;
    let broadcaster = Arc::new(Broadcaster::new());
    let (pools, chain_id) = make_pools(db.pool.clone());
    let mut app = make_test_service(pools, chain_id, broadcaster).await;

    // Query with explicit chainId (use point lookup to route to Postgres)
    let response = app
        .call(
            Request::builder()
                .method("GET")
                .uri("/query?sql=SELECT%20*%20FROM%20blocks%20WHERE%20num%20%3D%201&chainId=1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["ok"], true);
}

#[tokio::test]
#[serial(db)]
async fn test_query_invalid_chain_id() {
    let db = TestDb::new().await;
    let broadcaster = Arc::new(Broadcaster::new());
    let (pools, chain_id) = make_pools(db.pool.clone());
    let mut app = make_test_service(pools, chain_id, broadcaster).await;

    let response = app
        .call(
            Request::builder()
                .method("GET")
                .uri("/query?sql=SELECT%201&chainId=99999")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["ok"], false);
    assert!(json["error"].as_str().unwrap().contains("99999"));
}

#[tokio::test]
#[serial(db)]
async fn test_query_live_returns_sse() {
    let db = TestDb::new().await;
    let broadcaster = Arc::new(Broadcaster::new());
    let (pools, chain_id) = make_pools(db.pool.clone());
    let mut app = make_test_service(pools, chain_id, broadcaster).await;

    let response = app
        .call(
            Request::builder()
                .method("GET")
                .uri("/query?sql=SELECT%20num%20FROM%20blocks%20LIMIT%201&chainId=1&live=true")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let content_type = response
        .headers()
        .get("content-type")
        .map(|v| v.to_str().unwrap_or(""));
    assert!(
        content_type.unwrap_or("").contains("text/event-stream"),
        "expected SSE content-type, got {content_type:?}"
    );
}

#[tokio::test]
#[serial(db)]
async fn test_query_live_rejects_when_stream_capacity_reached() {
    let broadcaster = Arc::new(Broadcaster::new());
    let _receivers: Vec<_> = (0..20).map(|_| broadcaster.subscribe()).collect();
    let mut app = make_test_service(HashMap::new(), 1, broadcaster).await;

    let response = app
        .call(
            Request::builder()
                .method("GET")
                .uri("/query?sql=SELECT%20num%20FROM%20blocks%20LIMIT%201&chainId=1&live=true")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = tokio::time::timeout(
        Duration::from_secs(1),
        axum::body::to_bytes(response.into_body(), usize::MAX),
    )
    .await
    .expect("capacity stream should end")
    .unwrap();
    let body = std::str::from_utf8(&body).unwrap();
    assert!(body.contains("event: error"), "got: {body}");
    assert!(body.contains("Live stream capacity reached"), "got: {body}");
}

#[tokio::test]
#[serial(db)]
async fn test_query_live_streams_each_new_block() {
    let db = TestDb::empty().await;
    db.truncate_all().await;
    let now = chrono::Utc::now();
    let blocks: Vec<tidx::types::BlockRow> = (1..=4)
        .map(|num| tidx::types::BlockRow {
            num,
            hash: vec![num as u8; 32],
            parent_hash: vec![0; 32],
            timestamp: now,
            timestamp_ms: now.timestamp_millis(),
            gas_limit: 1,
            gas_used: 1,
            miner: vec![0; 20],
            extra_data: None,
            consensus_proposer: None,
        })
        .collect();
    tidx::sync::writer::write_blocks(&db.pool, &blocks)
        .await
        .unwrap();
    tidx::sync::writer::save_sync_state(
        &db.pool,
        &tidx::types::SyncState {
            chain_id: 1,
            synced_num: 2,
            tip_num: 2,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let broadcaster = Arc::new(Broadcaster::new());
    let (pools, chain_id) = make_pools(db.pool.clone());
    let mut app = make_test_service(pools, chain_id, broadcaster.clone()).await;
    let response = app
        .call(
            Request::builder()
                .method("GET")
                .uri("/query?sql=SELECT%20num%20FROM%20blocks%20ORDER%20BY%20num&chainId=1&live=true")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Blocks 3 and 4 arrive after the stream caught up to block 2.
    broadcaster.send(BlockUpdate {
        chain_id: 1,
        block_num: 4,
        block_hash: String::new(),
        tx_count: 0,
        log_count: 0,
        timestamp: now.timestamp(),
    });

    let mut body = response.into_body().into_data_stream();
    let mut events = String::new();
    tokio::time::timeout(Duration::from_secs(10), async {
        while events.matches("event: result").count() < 3 {
            let chunk = futures::StreamExt::next(&mut body).await.unwrap().unwrap();
            events.push_str(std::str::from_utf8(&chunk).unwrap());
        }
    })
    .await
    .unwrap_or_else(|_| panic!("expected three results, got: {events}"));

    let rows: Vec<serde_json::Value> = events
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .map(|data| serde_json::from_str::<serde_json::Value>(data).unwrap()["rows"].clone())
        .collect();
    assert_eq!(
        rows,
        vec![
            serde_json::json!([[1], [2], [3], [4]]),
            serde_json::json!([[3]]),
            serde_json::json!([[4]]),
        ]
    );
}

#[test]
fn test_live_block_query_matches_literal_block_filter() {
    // The block number is bound as $1 instead of spliced in, so the rewritten
    // SQL must be the same as for a literal block number.
    let sql = r#"SELECT "from", value FROM Transfer WHERE "to" = '0x00000000000000000000000000000000000000aa' ORDER BY log_idx"#;
    let signatures = ["Transfer(address indexed from, address indexed to, uint256 value)"];
    let options = QueryOptions {
        timeout_ms: 5000,
        limit: 100,
    };

    let filtered = inject_block_filter(sql).unwrap();
    let parameterized = PostgresQuery::new(&filtered, &signatures, &options).unwrap();
    let literal =
        PostgresQuery::new(&filtered.replace("$1", "123"), &signatures, &options).unwrap();

    assert_eq!(parameterized.sql().matches("$1").count(), 1);
    assert_eq!(parameterized.sql().replace("$1", "123"), literal.sql());
}

// Unit tests for inject_block_filter (no DB required)

#[test]
fn test_inject_block_filter_blocks_table() {
    let sql = "SELECT num, hash FROM blocks ORDER BY num DESC LIMIT 1";
    let filtered = inject_block_filter(sql).unwrap();
    assert!(filtered.contains("blocks.num = $1"), "got: {filtered}");
    assert!(filtered.contains("ORDER BY"), "should preserve ORDER BY");
}

#[test]
fn test_inject_block_filter_txs_table() {
    let sql = "SELECT * FROM txs ORDER BY block_num DESC LIMIT 10";
    let filtered = inject_block_filter(sql).unwrap();
    assert!(filtered.contains("txs.block_num = $1"), "got: {filtered}");
}

#[test]
fn test_inject_block_filter_logs_table() {
    let sql = "SELECT * FROM logs WHERE address = '0x123' ORDER BY block_num DESC";
    let filtered = inject_block_filter(sql).unwrap();
    assert!(filtered.contains("logs.block_num = $1"), "got: {filtered}");
    assert!(
        filtered.contains("address = '0x123'"),
        "should preserve existing WHERE"
    );
}

#[test]
fn test_inject_block_filter_with_existing_where() {
    let sql = "SELECT * FROM txs WHERE gas_used > 21000 ORDER BY block_num DESC";
    let filtered = inject_block_filter(sql).unwrap();
    assert!(filtered.contains("txs.block_num = $1"), "got: {filtered}");
    assert!(
        filtered.contains("gas_used > 21000"),
        "should preserve existing condition"
    );
}

#[test]
fn test_inject_block_filter_with_user_cte() {
    let sql = "WITH filtered AS (SELECT * FROM txs WHERE gas_used > 21000) SELECT * FROM filtered";
    let filtered = inject_block_filter(sql).unwrap();
    assert!(
        filtered.contains("filtered.block_num = $1"),
        "got: {filtered}"
    );
    assert!(
        filtered.contains("WITH filtered AS"),
        "should preserve user CTE"
    );
}

#[test]
fn test_inject_block_filter_uses_table_alias() {
    let sql = "SELECT t.hash FROM txs AS t WHERE t.gas_used > 21000 ORDER BY t.block_num DESC";
    let filtered = inject_block_filter(sql, 460).unwrap();
    assert!(filtered.contains("t.block_num = 460"), "got: {filtered}");
    assert!(
        !filtered.contains("txs.block_num"),
        "must not qualify with the hidden table name: {filtered}"
    );
}

#[test]
fn test_inject_block_filter_uses_implicit_table_alias() {
    let sql = "SELECT b.num, b.hash FROM blocks b ORDER BY b.num DESC LIMIT 1";
    let filtered = inject_block_filter(sql, 470).unwrap();
    assert!(filtered.contains("b.num = 470"), "got: {filtered}");
    assert!(!filtered.contains("blocks.num"), "got: {filtered}");
}

#[test]
fn test_inject_block_filter_preserves_quoted_alias() {
    let sql = r#"SELECT "Tx".hash FROM txs AS "Tx""#;
    let filtered = inject_block_filter(sql, 480).unwrap();
    assert!(
        filtered.contains(r#""Tx".block_num = 480"#),
        "got: {filtered}"
    );
}

#[test]
fn test_inject_block_filter_no_order_by() {
    let sql = "SELECT COUNT(*) FROM blocks LIMIT 1";
    let filtered = inject_block_filter(sql).unwrap();
    assert!(filtered.contains("blocks.num = $1"), "got: {filtered}");
}

#[test]
fn test_inject_block_filter_rejects_union() {
    let sql = "SELECT * FROM txs UNION SELECT * FROM logs";
    assert!(inject_block_filter(sql).is_err());
}

#[test]
fn test_inject_block_filter_rejects_non_select() {
    let sql = "INSERT INTO txs VALUES (1)";
    assert!(inject_block_filter(sql).is_err());
}

#[test]
fn test_inject_block_filter_where_keyword_in_string_literal() {
    let sql = "SELECT * FROM txs WHERE input = 'WHERE clause test'";
    let filtered = inject_block_filter(sql).unwrap();
    assert!(filtered.contains("txs.block_num = $1"), "got: {filtered}");
    assert!(
        filtered.contains("'WHERE clause test'"),
        "should preserve string literal"
    );
}
