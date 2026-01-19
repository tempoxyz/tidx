mod common;

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use ak47::api::{self, inject_block_filter};
use ak47::broadcast::Broadcaster;
use common::testdb::TestDb;

#[tokio::test]
async fn test_query_post_returns_json() {
    let db = TestDb::empty().await;
    let broadcaster = Arc::new(Broadcaster::new());
    let app = api::router(db.pool.clone(), broadcaster);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/query")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"sql": "SELECT 1 as value"}"#))
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
    assert!(json["columns"].as_array().is_some());
}

#[tokio::test]
async fn test_query_get_returns_sse() {
    let db = TestDb::empty().await;
    let broadcaster = Arc::new(Broadcaster::new());
    let app = api::router(db.pool.clone(), broadcaster.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/query?sql=SELECT%201%20as%20value")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    // Check content-type is SSE
    let content_type = response
        .headers()
        .get("content-type")
        .map(|v| v.to_str().unwrap_or(""));
    assert!(
        content_type.unwrap_or("").contains("text/event-stream"),
        "expected SSE content-type, got {:?}",
        content_type
    );
}

#[tokio::test]
async fn test_query_post_with_live_param_returns_sse() {
    let db = TestDb::empty().await;
    let broadcaster = Arc::new(Broadcaster::new());
    let app = api::router(db.pool.clone(), broadcaster);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/query?live=true")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"sql": "SELECT 1 as value"}"#))
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
        "expected SSE content-type, got {:?}",
        content_type
    );
}

#[tokio::test]
async fn test_health_endpoint() {
    let db = TestDb::empty().await;
    let broadcaster = Arc::new(Broadcaster::new());
    let app = api::router(db.pool.clone(), broadcaster);

    let response = app
        .oneshot(
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
async fn test_status_endpoint() {
    let db = TestDb::empty().await;
    let broadcaster = Arc::new(Broadcaster::new());
    let app = api::router(db.pool.clone(), broadcaster);

    let response = app
        .oneshot(
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

    // ok should be present (true or false depending on sync state)
    assert!(json.get("ok").is_some());
}

#[test]
fn test_inject_block_filter_blocks_table() {
    // Blocks table uses 'num' column
    let sql = "SELECT num, hash FROM blocks ORDER BY num DESC LIMIT 1";
    let filtered = inject_block_filter(sql, 100);
    assert!(filtered.contains("num = 100"), "got: {}", filtered);
    assert!(filtered.contains("ORDER BY"), "should preserve ORDER BY");
}

#[test]
fn test_inject_block_filter_txs_table() {
    // Txs table uses 'block_num' column
    let sql = "SELECT * FROM txs ORDER BY block_num DESC LIMIT 10";
    let filtered = inject_block_filter(sql, 200);
    assert!(filtered.contains("block_num = 200"), "got: {}", filtered);
}

#[test]
fn test_inject_block_filter_logs_table() {
    // Logs table uses 'block_num' column
    let sql = "SELECT * FROM logs WHERE address = '0x123' ORDER BY block_num DESC";
    let filtered = inject_block_filter(sql, 300);
    assert!(filtered.contains("block_num = 300"), "got: {}", filtered);
    assert!(filtered.contains("address = '0x123'"), "should preserve existing WHERE");
}

#[test]
fn test_inject_block_filter_with_existing_where() {
    let sql = "SELECT * FROM txs WHERE gas_used > 21000 ORDER BY block_num DESC";
    let filtered = inject_block_filter(sql, 400);
    assert!(filtered.contains("block_num = 400"), "got: {}", filtered);
    assert!(filtered.contains("gas_used > 21000"), "should preserve existing condition");
}

#[test]
fn test_inject_block_filter_no_order_by() {
    let sql = "SELECT COUNT(*) FROM blocks LIMIT 1";
    let filtered = inject_block_filter(sql, 500);
    assert!(filtered.contains("num = 500"), "got: {}", filtered);
}
