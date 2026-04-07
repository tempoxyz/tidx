mod common;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::connect_info::IntoMakeServiceWithConnectInfo;
use axum::http::{Request, StatusCode};
use axum::Router;
use deadpool_postgres::{Config as PgConfig, Runtime};
use tokio_postgres::NoTls;
use tower::Service;

use tidx::api;
use tidx::broadcast::Broadcaster;
use tidx::metrics;
use common::testdb::TestDb;
use serial_test::serial;

fn make_pools(pool: tidx::db::Pool) -> (HashMap<u64, tidx::db::Pool>, u64) {
    let mut pools = HashMap::new();
    let chain_id = 1u64;
    pools.insert(chain_id, pool);
    (pools, chain_id)
}

async fn make_test_service(
    pools: HashMap<u64, tidx::db::Pool>,
    chain_id: u64,
    broadcaster: Arc<Broadcaster>,
) -> impl Service<Request<Body>, Response = axum::response::Response, Error = std::convert::Infallible>
{
    let mut svc: IntoMakeServiceWithConnectInfo<Router, SocketAddr> =
        api::router(pools, chain_id, broadcaster)
            .into_make_service_with_connect_info::<SocketAddr>();
    svc.call(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .unwrap()
}

// ============================================================================
// /status endpoint returns in-memory watermarks (no table scans)
// ============================================================================

#[tokio::test]
#[serial(db)]
async fn test_status_includes_postgres_watermarks() {
    let db = TestDb::new().await;
    let broadcaster = Arc::new(Broadcaster::new());
    let (pools, chain_id) = make_pools(db.pool.clone());
    let mut app = make_test_service(pools, chain_id, broadcaster).await;

    // Set watermarks via the same atomics the writer uses
    metrics::update_sink_watermark("postgres", "blocks", 999_000);
    metrics::update_sink_watermark("postgres", "txs", 999_000);
    metrics::update_sink_watermark("postgres", "logs", 998_000);
    metrics::update_sink_watermark("postgres", "receipts", 997_000);

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

    let chains = json["chains"].as_array().expect("chains should be array");
    assert!(!chains.is_empty(), "expected at least one chain");

    let chain = &chains[0];
    let pg = &chain["postgres"];
    assert!(!pg.is_null(), "expected postgres watermarks in response");
    assert!(
        pg["blocks"].as_i64().unwrap() >= 999_000,
        "blocks watermark should be >= 999_000, got {:?}",
        pg["blocks"]
    );
    assert!(
        pg["txs"].as_i64().unwrap() >= 999_000,
        "txs watermark should be >= 999_000, got {:?}",
        pg["txs"]
    );
    assert!(
        pg["logs"].as_i64().unwrap() >= 998_000,
        "logs watermark should be >= 998_000, got {:?}",
        pg["logs"]
    );
    assert!(
        pg["receipts"].as_i64().unwrap() >= 997_000,
        "receipts watermark should be >= 997_000, got {:?}",
        pg["receipts"]
    );
}

#[tokio::test]
#[serial(db)]
async fn test_status_includes_configured_chain_when_sync_state_is_empty() {
    let db = TestDb::empty().await;
    db.truncate_all().await;
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

    let chains = json["chains"].as_array().expect("chains should be array");
    assert_eq!(chains.len(), 1, "expected configured chain to be present");
    assert_eq!(chains[0]["chain_id"], 1);
}

#[tokio::test]
async fn test_status_surfaces_query_failures() {
    let broadcaster = Arc::new(Broadcaster::new());
    let mut pools = HashMap::new();
    let chain_id = 1u64;

    let mut cfg = PgConfig::new();
    cfg.url = Some("postgres://tidx:tidx@127.0.0.1:1/tidx?connect_timeout=1".to_string());
    let broken_pool = cfg
        .create_pool(Some(Runtime::Tokio1), NoTls)
        .expect("failed to create broken pool");
    pools.insert(chain_id, broken_pool);

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

    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["ok"], false);
    assert!(
        json["error"]
            .as_str()
            .unwrap_or_default()
            .contains("Failed to load status for chain 1"),
        "unexpected error body: {json:?}"
    );
}

// ============================================================================
// CLI HTTP proxy: when a real HTTP server is running, status is fetched via HTTP
// ============================================================================

#[tokio::test]
#[serial(db)]
async fn test_cli_proxy_via_http_server() {
    let db = TestDb::new().await;
    let broadcaster = Arc::new(Broadcaster::new());
    let (pools, chain_id) = make_pools(db.pool.clone());

    // Set watermarks so they appear in the response
    metrics::update_sink_watermark("postgres", "blocks", 1_000_000);
    metrics::update_sink_watermark("postgres", "txs", 1_000_000);

    let router = api::router(pools, chain_id, broadcaster)
        .into_make_service_with_connect_info::<SocketAddr>();

    // Bind to a random available port
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("Failed to bind");
    let addr = listener.local_addr().unwrap();

    // Spawn server in background
    let server = tokio::spawn(async move {
        axum::serve(listener, router)
            .await
            .expect("Server failed");
    });

    // Wait for server to be ready
    let client = reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_millis(200))
        .timeout(std::time::Duration::from_secs(2))
        .build()
        .unwrap();

    let base_url = format!("http://127.0.0.1:{}", addr.port());

    // Poll health endpoint until ready (simulates CLI auto-probe)
    let mut ready = false;
    for _ in 0..20 {
        if client
            .get(format!("{base_url}/health"))
            .send()
            .await
            .is_ok()
        {
            ready = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(ready, "HTTP server did not become ready");

    // Now fetch /status (same as CLI's run_via_http)
    let resp = client
        .get(format!("{base_url}/status"))
        .send()
        .await
        .expect("Failed to GET /status");

    assert_eq!(resp.status(), 200);

    let json: serde_json::Value = resp.json().await.expect("Failed to parse JSON");
    assert_eq!(json["ok"], true);

    let chains = json["chains"].as_array().expect("chains should be array");
    assert!(!chains.is_empty(), "expected at least one chain");

    // Verify watermarks are present (proving we got the fast in-memory path)
    let chain = &chains[0];
    let pg = &chain["postgres"];
    assert!(!pg.is_null(), "expected postgres watermarks via HTTP proxy");
    assert!(
        pg["blocks"].as_i64().unwrap() >= 1_000_000,
        "blocks watermark should be >= 1_000_000 via HTTP, got {:?}",
        pg["blocks"]
    );

    server.abort();
}

// ============================================================================
// CLI fallback: when no HTTP server is running, health probe fails gracefully
// ============================================================================

#[tokio::test]
async fn test_cli_health_probe_fails_when_no_server() {
    // Pick a port that nothing is listening on
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("Failed to bind");
    let port = listener.local_addr().unwrap().port();
    drop(listener); // Release the port immediately

    let client = reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_millis(200))
        .timeout(std::time::Duration::from_millis(500))
        .build()
        .unwrap();

    // This should fail — simulates the CLI's auto-probe check
    let result = client
        .get(format!("http://127.0.0.1:{port}/health"))
        .send()
        .await;

    assert!(
        result.is_err(),
        "expected connection failure when no server is running"
    );
}

// ============================================================================
// Watermark fetch_max semantics: only increases, never decreases
// ============================================================================

#[test]
fn test_watermark_fetch_max_semantics() {
    // Set a high watermark
    metrics::update_sink_watermark("postgres", "blocks", 2_000_000);
    let wm = metrics::get_sink_watermark("postgres", "blocks");
    assert!(wm.unwrap() >= 2_000_000);

    // Attempt to set a lower value — should be ignored (fetch_max)
    metrics::update_sink_watermark("postgres", "blocks", 100);
    let wm = metrics::get_sink_watermark("postgres", "blocks");
    assert!(
        wm.unwrap() >= 2_000_000,
        "watermark should not decrease, got {:?}",
        wm
    );

    // Set a higher value — should succeed
    metrics::update_sink_watermark("postgres", "blocks", 3_000_000);
    let wm = metrics::get_sink_watermark("postgres", "blocks");
    assert!(wm.unwrap() >= 3_000_000);
}

#[test]
fn test_watermark_all_tables() {
    metrics::update_sink_watermark("postgres", "blocks", 4_000_000);
    metrics::update_sink_watermark("postgres", "txs", 4_000_000);
    metrics::update_sink_watermark("postgres", "logs", 3_999_000);
    metrics::update_sink_watermark("postgres", "receipts", 3_998_000);

    let (blocks, txs, logs, receipts) = metrics::get_sink_watermarks("postgres");
    assert!(blocks.unwrap() >= 4_000_000);
    assert!(txs.unwrap() >= 4_000_000);
    assert!(logs.unwrap() >= 3_999_000);
    assert!(receipts.unwrap() >= 3_998_000);
}

// ============================================================================
// /status JSON shape: verify contract matches what CLI expects
// ============================================================================

#[tokio::test]
#[serial(db)]
async fn test_status_json_shape_for_cli() {
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

    // Verify the JSON shape that print_http_status expects
    assert!(json["ok"].is_boolean());
    assert!(json["chains"].is_array());

    let chains = json["chains"].as_array().unwrap();
    if !chains.is_empty() {
        let chain = &chains[0];
        // Fields the CLI's print_http_status reads
        assert!(chain["chain_id"].is_number(), "chain_id should be number");
        assert!(chain["tip_num"].is_number() || chain["tip_num"].is_null());
        assert!(chain["synced_num"].is_number());
        assert!(chain["lag"].is_number());
    }
}
