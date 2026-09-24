//! Real PostgreSQL/pg_clickhouse regression for dedicated API logins.
//! Requires the postgres and clickhouse services in docker/local/docker-compose.yml.
//! CLICKHOUSE_FDW_URL is the ClickHouse URL as seen from the PostgreSQL container.

mod common;

use common::{clickhouse::TestClickHouse, testdb::TestDb};
use tidx::{
    db::{
        create_pool,
        tiered::{FdwTarget, bootstrap, bootstrap_with_api_pool},
    },
    service::{QueryOptions, execute_query_postgres_via_clickhouse},
    sync::{ch_sink::ClickHouseSink, writer::set_hot_boundary},
    types::BlockRow,
};

#[tokio::test]
async fn dedicated_api_login_can_query_archive_after_bootstrap_and_restart() {
    const CHAIN_ID: u64 = 999;
    const CH_DB: &str = "tidx_test_api_mapping";
    let ch = TestClickHouse::new(CH_DB).expect("ClickHouse client");
    ch.wait_for_ready()
        .await
        .expect("ClickHouse must be running");
    ch.reset_database().await.expect("reset fixture archive");
    let sink = ClickHouseSink::new(&ch.url, CH_DB, None, None).expect("archive sink");
    sink.ensure_schema_only().await.expect("archive schema");
    sink.write_blocks(&[BlockRow {
        num: 1,
        hash: vec![1; 32],
        parent_hash: vec![0; 32],
        timestamp: chrono::Utc::now(),
        timestamp_ms: chrono::Utc::now().timestamp_millis(),
        gas_limit: 30_000_000,
        gas_used: 21_000,
        miner: vec![2; 20],
        extra_data: None,
        consensus_proposer: None,
    }])
    .await
    .expect("seed cold block");

    let db = TestDb::empty().await;
    set_hot_boundary(&db.pool, CHAIN_ID, 1, None)
        .await
        .expect("cold boundary");
    let role = format!("tidx_api\"reader_{}", rand::random::<u32>());
    let quoted_role = format!("\"{}\"", role.replace('"', "\"\""));
    let conn = db.pool.get().await.expect("owner connection");
    // Provision ordinary reader privileges, as deployment setup already does.
    // Bootstrap must add only the user mapping, not extra database privileges.
    conn.batch_execute(&format!(
        "CREATE ROLE {quoted_role} LOGIN PASSWORD 'test-only-password';
         CREATE SCHEMA IF NOT EXISTS ch;
         CREATE SCHEMA IF NOT EXISTS tiered;
         GRANT USAGE ON SCHEMA ch, tiered TO {quoted_role};
         GRANT SELECT ON ALL TABLES IN SCHEMA public TO {quoted_role};
         ALTER DEFAULT PRIVILEGES IN SCHEMA ch GRANT SELECT ON TABLES TO {quoted_role};
         ALTER DEFAULT PRIVILEGES IN SCHEMA tiered GRANT SELECT ON TABLES TO {quoted_role};"
    ))
    .await
    .expect("fixture reader role");
    drop(conn);

    let mut api_url = url::Url::parse(&std::env::var("DATABASE_URL").expect("DATABASE_URL"))
        .expect("database URL");
    api_url.set_username(&role).expect("API username");
    api_url
        .set_password(Some("test-only-password"))
        .expect("API password");
    let api_pool = create_pool(api_url.as_str())
        .await
        .expect("dedicated API pool");
    let target = FdwTarget::new(
        &std::env::var("CLICKHOUSE_FDW_URL").unwrap_or_else(|_| "http://clickhouse:8123".into()),
        CH_DB.into(),
        None,
        None,
    )
    .expect("FDW target");
    let options = QueryOptions {
        timeout_ms: 10_000,
        limit: 10,
    };
    let query = "SELECT num FROM blocks WHERE num = 1 LIMIT 1";

    // The old owner-only bootstrap leaves this real API query broken.
    bootstrap(&db.pool, &target, CHAIN_ID)
        .await
        .expect("owner bootstrap");
    let error = execute_query_postgres_via_clickhouse(&api_pool, query, &[], &options)
        .await
        .expect_err("owner mapping must not cover an unrelated API login");
    assert!(
        format!("{error:#}").contains("user mapping not found"),
        "{error:#}"
    );

    for _ in 0..2 {
        bootstrap_with_api_pool(&db.pool, &target, CHAIN_ID, &api_pool)
            .await
            .expect("bootstrap with API login");
        let result = execute_query_postgres_via_clickhouse(&api_pool, query, &[], &options)
            .await
            .expect("API archive query, including after rebootstrap");
        assert_eq!(result.rows, vec![vec![serde_json::json!(1)]]);
        let conn = db.pool.get().await.expect("owner connection");
        let public_mapping: bool = conn.query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_user_mapping m JOIN pg_foreign_server s ON s.oid = m.umserver WHERE s.srvname = 'tidx_clickhouse' AND m.umuser = 0)", &[],
        ).await.expect("mapping scope").get(0);
        assert!(!public_mapping, "must not create a PUBLIC mapping");
        let can_create: bool = conn
            .query_one("SELECT has_schema_privilege($1, 'ch', 'CREATE')", &[&role])
            .await
            .expect("reader privileges")
            .get(0);
        assert!(!can_create, "bootstrap must not broaden reader privileges");
    }

    // Shared sync/API credentials must remain supported without duplicate DDL.
    bootstrap_with_api_pool(&db.pool, &target, CHAIN_ID, &db.pool)
        .await
        .expect("same-login bootstrap");
    let result = execute_query_postgres_via_clickhouse(&db.pool, query, &[], &options)
        .await
        .expect("owner archive query");
    assert_eq!(result.rows, vec![vec![serde_json::json!(1)]]);
    api_pool.close();
    db.pool
        .get()
        .await
        .expect("owner connection")
        .batch_execute(&format!(
            "DROP OWNED BY {quoted_role}; DROP ROLE {quoted_role};"
        ))
        .await
        .expect("clean up fixture role");
}
