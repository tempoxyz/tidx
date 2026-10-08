//! PostgreSQL-over-ClickHouse (`engine=postgres&source=clickhouse`) query tests.
//!
//! Run with: cargo test --test fdw_query_test
//! Requires: docker compose -f docker/local/docker-compose.yml up -d postgres clickhouse
//! (PostgreSQL with the pg_clickhouse extension). `TIDX_TEST_FDW_URL` is the
//! ClickHouse URL as seen from the PostgreSQL container (default
//! `http://clickhouse:8123`, the compose service).

mod common;

use common::clickhouse::TestClickHouse;
use common::testdb::TestDb;
use serial_test::serial;

use tidx::db::tiered::{FdwTarget, bootstrap};
use tidx::query::EventSignature;
use tidx::service::{QueryOptions, execute_query_postgres_via_clickhouse};

const CHAIN_ID: u64 = 999;
const CH_DB: &str = "tidx_test_fdw_query";
const FIXED_EVENT: &str = "Fixed(bytes4 indexed tag, bytes4 value, bytes32 word)";

/// Bootstrap `ch.*` foreign tables over a fresh ClickHouse database.
/// Returns `None` (skip) when ClickHouse or pg_clickhouse is unavailable.
async fn setup() -> Option<(TestDb, TestClickHouse)> {
    let ch = TestClickHouse::new(CH_DB).expect("Failed to create ClickHouse client");
    if ch.wait_for_ready().await.is_err() {
        println!("ClickHouse not available, skipping test");
        return None;
    }
    ch.reset_database().await.expect("Failed to reset database");
    ch.create_mock_logs_table()
        .await
        .expect("Failed to create logs table");

    let db = TestDb::empty().await;
    let fdw_url =
        std::env::var("TIDX_TEST_FDW_URL").unwrap_or_else(|_| "http://clickhouse:8123".into());
    let target = FdwTarget::new(&fdw_url, CH_DB.to_string(), None, None).unwrap();
    if let Err(e) = bootstrap(&db.pool, &target, None, CHAIN_ID).await {
        println!("pg_clickhouse not available ({e:#}), skipping test");
        teardown(&db).await;
        return None;
    }
    // bootstrap's DDL never contacts ClickHouse; probe the FDW connection.
    let conn = db.pool.get().await.unwrap();
    let probe = conn
        .query_one("SELECT count(*) FROM ch.logs", &[])
        .await
        .map_err(anyhow::Error::from);
    drop(conn);
    if let Err(e) = probe {
        println!("ClickHouse unreachable from PostgreSQL at {fdw_url} ({e:#}), skipping test");
        teardown(&db).await;
        return None;
    }
    Some((db, ch))
}

/// Drop the FDW server (and its foreign tables/views) so other suites see
/// plain PostgreSQL.
async fn teardown(db: &TestDb) {
    let conn = db.pool.get().await.unwrap();
    conn.batch_execute("DROP SERVER IF EXISTS tidx_clickhouse CASCADE")
        .await
        .unwrap();
}

#[tokio::test]
#[serial(db)]
async fn test_fdw_empty_event_result_keeps_columns() {
    let Some((db, _ch)) = setup().await else {
        return;
    };

    let result = execute_query_postgres_via_clickhouse(
        &db.pool,
        r#"SELECT block_num, "from", "to", "value" FROM Transfer WHERE block_num < 0"#,
        &["Transfer(address indexed from, address indexed to, uint256 value)"],
        &QueryOptions::default(),
    )
    .await;
    teardown(&db).await;

    let result = result.expect("FDW query failed");
    assert_eq!(result.row_count, 0);
    assert_eq!(result.columns, ["block_num", "from", "to", "value"]);
}

#[tokio::test]
#[serial(db)]
async fn test_fdw_fixed_bytes_returns_declared_width() {
    let Some((db, ch)) = setup().await else {
        return;
    };

    let selector = format!(
        "0x{}",
        EventSignature::parse(FIXED_EVENT).unwrap().topic0_hex()
    );
    let tag = format!("0xcafebabe{}", "00".repeat(28));
    let word = "11".repeat(32);
    let data = format!("0xdeadbeef{}{word}", "00".repeat(28));
    let zero = format!("0x{}", "00".repeat(32));
    let opts = QueryOptions::default();
    // Fallible body so teardown always runs before any assertion.
    let result = async {
        ch.insert_mock_log(
            1,
            0,
            0,
            &zero,
            "0x1111111111111111111111111111111111111111",
            &selector,
            &tag,
            &zero,
            &zero,
            &data,
        )
        .await?;
        let all = execute_query_postgres_via_clickhouse(
            &db.pool,
            r#"SELECT tag, "value", word FROM Fixed"#,
            &[FIXED_EVENT],
            &opts,
        )
        .await?;
        let filtered = execute_query_postgres_via_clickhouse(
            &db.pool,
            r#"SELECT "value" FROM Fixed WHERE "tag" = '0xcafebabe'"#,
            &[FIXED_EVENT],
            &opts,
        )
        .await?;
        let nested = execute_query_postgres_via_clickhouse(
            &db.pool,
            r#"SELECT * FROM (SELECT "tag" FROM Fixed) q WHERE "tag" = '0xcafebabe'"#,
            &[FIXED_EVENT],
            &opts,
        ).await?;
        let unrelated = execute_query_postgres_via_clickhouse(
            &db.pool,
            r#"WITH q AS (SELECT '0xcafebabe' AS tag) SELECT q.tag FROM q CROSS JOIN Fixed WHERE q."tag" = '0xcafebabe'"#,
            &[FIXED_EVENT],
            &opts,
        ).await?;
        let capped = execute_query_postgres_via_clickhouse(
            &db.pool,
            r#"SELECT count(*) FROM (SELECT DISTINCT tx_hash, log_idx FROM Fixed WHERE "tag" = '0xcafebabe' LIMIT 1000) capped"#,
            &[FIXED_EVENT],
            &opts,
        ).await?;
        anyhow::Ok((all, filtered, nested, unrelated, capped))
    }
    .await;
    teardown(&db).await;

    let (all, filtered, nested, unrelated, capped) = result.expect("FDW query failed");
    assert_eq!(
        all.rows,
        [vec![
            serde_json::json!("0xcafebabe"),
            serde_json::json!("0xdeadbeef"),
            serde_json::json!(format!("0x{word}")),
        ]]
    );
    assert_eq!(filtered.rows, [vec![serde_json::json!("0xdeadbeef")]]);
    assert_eq!(nested.rows, [vec![serde_json::json!("0xcafebabe")]]);
    assert_eq!(unrelated.rows, nested.rows);
    assert_eq!(capped.rows, [vec![serde_json::json!(1)]]);
}

const API_ROLE: &str = "tidx_test_fdw_api";

async fn create_api_role(db: &TestDb) {
    let conn = db.pool.get().await.unwrap();
    conn.batch_execute(&format!(
        "DO $$ BEGIN
           IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{API_ROLE}') THEN
             CREATE ROLE {API_ROLE} NOLOGIN;
           END IF;
         END $$"
    ))
    .await
    .unwrap();
}

async fn drop_api_role(db: &TestDb) {
    let conn = db.pool.get().await.unwrap();
    conn.batch_execute(&format!("DROP OWNED BY {API_ROLE}; DROP ROLE {API_ROLE}"))
        .await
        .unwrap();
}

/// Count `ch.logs` as the API role, the way a separate API pool queries.
async fn count_as_api_role(db: &TestDb) -> Result<i64, tokio_postgres::Error> {
    let mut conn = db.pool.get().await.unwrap();
    let tx = conn.transaction().await?;
    tx.batch_execute(&format!(
        "GRANT USAGE ON SCHEMA ch TO {API_ROLE};
         GRANT SELECT ON ch.logs TO {API_ROLE};
         SET LOCAL ROLE {API_ROLE}"
    ))
    .await?;
    let count = tx.query_one("SELECT count(*) FROM ch.logs", &[]).await?;
    Ok(count.get(0))
}

#[tokio::test]
#[serial(db)]
async fn test_fdw_api_role_needs_its_own_user_mapping() {
    let Some((db, _ch)) = setup().await else {
        return;
    };
    create_api_role(&db).await;
    let without_mapping = count_as_api_role(&db).await;

    // Re-bootstrap the way `tidx up` does when `postgres.api_url` is set.
    let fdw_url =
        std::env::var("TIDX_TEST_FDW_URL").unwrap_or_else(|_| "http://clickhouse:8123".into());
    let target = FdwTarget::new(&fdw_url, CH_DB.to_string(), None, None).unwrap();
    let rebootstrap = bootstrap(&db.pool, &target, Some(API_ROLE), CHAIN_ID).await;
    let with_mapping = count_as_api_role(&db).await;
    teardown(&db).await;
    drop_api_role(&db).await;

    let err = without_mapping.expect_err("API role must not read ch.* without a mapping");
    assert!(
        format!("{err:?}").contains("user mapping not found"),
        "unexpected error: {err:?}"
    );
    rebootstrap.expect("bootstrap with API role failed");
    assert_eq!(with_mapping.expect("API role FDW query failed"), 0);
}
