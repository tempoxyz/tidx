mod common;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::connect_info::IntoMakeServiceWithConnectInfo;
use axum::http::{Method, Request, StatusCode};
use serial_test::serial;
use tokio::sync::RwLock;
use tower::Service;

use common::testdb::TestDb;
use tidx::api::{self, ApiMode, head_poller};
use tidx::broadcast::Broadcaster;

/// A chain id no other test writes to sync_state.
const CHAIN_ID: u64 = 990_001;

async fn set_tip(pool: &tidx::db::Pool, tip: i64) {
    let conn = pool.get().await.unwrap();
    conn.execute(
        "INSERT INTO sync_state (chain_id, tip_num) VALUES ($1, $2)
         ON CONFLICT (chain_id) DO UPDATE SET tip_num = EXCLUDED.tip_num",
        &[&(CHAIN_ID as i64), &tip],
    )
    .await
    .unwrap();
}

async fn clear_state(pool: &tidx::db::Pool) {
    let conn = pool.get().await.unwrap();
    conn.execute(
        "DELETE FROM sync_state WHERE chain_id = $1",
        &[&(CHAIN_ID as i64)],
    )
    .await
    .unwrap();
}

async fn recv_blocks(
    rx: &mut tokio::sync::broadcast::Receiver<tidx::broadcast::BlockUpdate>,
    n: usize,
) -> Vec<u64> {
    let mut out = Vec::new();
    while out.len() < n {
        let update = tokio::time::timeout(Duration::from_secs(3), rx.recv())
            .await
            .expect("timed out waiting for a block update")
            .expect("broadcast channel closed");
        assert_eq!(update.chain_id, CHAIN_ID);
        out.push(update.block_num);
    }
    out
}

async fn max_block(pool: &tidx::db::Pool) -> i64 {
    pool.get()
        .await
        .unwrap()
        .query_one("SELECT max(num) FROM blocks", &[])
        .await
        .unwrap()
        .get(0)
}

#[tokio::test]
#[serial(db)]
async fn head_poller_announces_blocks_from_sync_state() {
    let db = TestDb::new().await;
    let top = max_block(&db.pool).await;
    assert!(top >= 6, "seeded database needs at least 6 blocks");
    clear_state(&db.pool).await;
    set_tip(&db.pool, top - 5).await;

    let broadcaster = Arc::new(Broadcaster::new());
    let mut rx = broadcaster.subscribe();
    let (shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(1);
    let handle = head_poller::spawn(
        db.pool.clone(),
        CHAIN_ID,
        broadcaster.clone(),
        Duration::from_millis(20),
        shutdown_tx.subscribe(),
    );

    // The first poll only sets the baseline: nothing before it is announced.
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(rx.try_recv().is_err(), "baseline tip must not be announced");

    let t = |d: i64| (top - d) as u64;
    set_tip(&db.pool, top - 2).await;
    assert_eq!(recv_blocks(&mut rx, 1).await, vec![t(2)]);

    // A reorg rewinds tip_num; the replacement blocks are announced again.
    set_tip(&db.pool, top - 4).await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(rx.try_recv().is_err(), "a rewind announces nothing");
    set_tip(&db.pool, top - 2).await;
    assert_eq!(recv_blocks(&mut rx, 1).await, vec![t(2)]);

    // tip_num can run ahead of the stored blocks (backfill-first sets it to
    // the chain head first); only blocks that exist are announced.
    set_tip(&db.pool, top + 1_000).await;
    assert_eq!(recv_blocks(&mut rx, 1).await, vec![t(0)]);
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(
        rx.try_recv().is_err(),
        "blocks above max(num) must not be announced"
    );

    let _ = shutdown_tx.send(());
    tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect("poller should stop on shutdown")
        .unwrap();
    clear_state(&db.pool).await;
}

#[test]
fn head_poller_announces_only_the_newest_block() {
    // Streams catch up on their own and cap catch-up at 10 blocks, so a
    // jump must arrive as a single update for that cap to apply.
    let broadcaster = Broadcaster::new();
    let mut rx = broadcaster.subscribe();
    assert!(head_poller::publish_head(&broadcaster, CHAIN_ID, 10, 1_000));
    assert_eq!(rx.try_recv().unwrap().block_num, 1_000);
    assert!(rx.try_recv().is_err());
    assert!(!head_poller::publish_head(&broadcaster, CHAIN_ID, 5, 5));
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
#[serial(db)]
async fn connect_pool_does_not_create_databases() {
    let _db = TestDb::empty().await;
    let base = std::env::var("DATABASE_URL").unwrap();
    let mut url = url::Url::parse(&base).unwrap();
    url.set_path("/tidx_serve_must_not_exist");
    assert!(tidx::db::connect_pool(url.as_str(), 2).await.is_err());

    let admin = tidx::db::create_pool(&base).await.unwrap();
    let exists: bool = admin
        .get()
        .await
        .unwrap()
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = 'tidx_serve_must_not_exist')",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert!(!exists);
}

async fn service(
    pool: tidx::db::Pool,
    mode: ApiMode,
) -> impl Service<Request<Body>, Response = axum::response::Response, Error = std::convert::Infallible>
{
    let mut pools = HashMap::new();
    pools.insert(1u64, pool);
    let router = api::router_shared_with_mode(
        Arc::new(RwLock::new(pools)),
        1,
        Arc::new(Broadcaster::new()),
        Arc::new(RwLock::new(HashMap::new())),
        Arc::new(RwLock::new(HashMap::new())),
        Arc::new(std::sync::RwLock::new(Vec::new())),
        mode,
    );
    let mut svc: IntoMakeServiceWithConnectInfo<Router, SocketAddr> =
        router.into_make_service_with_connect_info::<SocketAddr>();
    svc.call(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .unwrap()
}

async fn status_of(
    svc: &mut impl Service<
        Request<Body>,
        Response = axum::response::Response,
        Error = std::convert::Infallible,
    >,
    method: Method,
    uri: &str,
) -> StatusCode {
    let req = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from("{}"))
        .unwrap();
    svc.call(req).await.unwrap().status()
}

#[tokio::test]
#[serial(db)]
async fn api_modes_expose_the_right_routes() {
    let db = TestDb::empty().await;
    let query = "/query?sql=SELECT%201";

    let mut serve = service(db.pool.clone(), ApiMode::Serve).await;
    assert_eq!(
        status_of(&mut serve, Method::GET, "/health").await,
        StatusCode::OK
    );
    assert_ne!(
        status_of(&mut serve, Method::GET, query).await,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        status_of(&mut serve, Method::POST, "/views").await,
        StatusCode::METHOD_NOT_ALLOWED,
        "serve must not create ClickHouse views"
    );
    assert_eq!(
        status_of(&mut serve, Method::DELETE, "/views/x").await,
        StatusCode::METHOD_NOT_ALLOWED,
        "serve must not drop ClickHouse views"
    );

    let mut sync = service(db.pool.clone(), ApiMode::Sync).await;
    assert_eq!(
        status_of(&mut sync, Method::GET, "/health").await,
        StatusCode::OK
    );
    assert_eq!(
        status_of(&mut sync, Method::GET, query).await,
        StatusCode::NOT_FOUND
    );
    assert_ne!(
        status_of(&mut sync, Method::POST, "/views").await,
        StatusCode::METHOD_NOT_ALLOWED
    );

    let mut full = service(db.pool.clone(), ApiMode::Full).await;
    assert_ne!(
        status_of(&mut full, Method::GET, query).await,
        StatusCode::NOT_FOUND
    );
    assert_ne!(
        status_of(&mut full, Method::POST, "/views").await,
        StatusCode::METHOD_NOT_ALLOWED
    );
}
