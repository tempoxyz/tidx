//! ClickHouse integration tests
//!
//! Tests CTE generation, query execution, materialized view creation,
//! and the ClickHouseSink direct-write path against a local ClickHouse instance.
//!
//! Run with: cargo test --test clickhouse_test
//! Requires: docker compose -f docker/local/docker-compose.yml up -d postgres clickhouse

mod common;

use common::clickhouse::TestClickHouse;
use serial_test::serial;
use tidx::clickhouse::ClickHouseEngine;
use tidx::clickhouse_schema::{base_objects, migrations, post_derived_migrations};
use tidx::config::ClickHouseConfig;
use tidx::query::{EventSignature, apply_event_signature_ctes_clickhouse};
use tidx::sync::ch_sink::ClickHouseSink;
use tidx::sync::sink::SinkSet;
use tidx::sync::writer;
use tidx::types::{BlockRow, LogRow, ReceiptRow, TxRow};

const TEST_DB: &str = "tidx_test";

// ============================================================================
// Basic ClickHouse Connection Tests
// ============================================================================

#[tokio::test]
#[serial(clickhouse)]
async fn test_clickhouse_connection() {
    let ch = TestClickHouse::new(TEST_DB)
        .await
        .expect("Failed to create ClickHouse client");

    if ch.wait_for_ready().await.is_err() {
        println!("ClickHouse not available, skipping test");
        return;
    }

    let result = ch.query_raw("SELECT 1").await.expect("Query failed");
    assert_eq!(result.trim(), "1");
}

#[tokio::test]
#[serial(clickhouse)]
async fn test_clickhouse_database_creation() {
    let ch = TestClickHouse::new(TEST_DB)
        .await
        .expect("Failed to create ClickHouse client");

    if ch.wait_for_ready().await.is_err() {
        println!("ClickHouse not available, skipping test");
        return;
    }

    ch.reset_database().await.expect("Failed to reset database");

    // Verify database exists
    let result = ch
        .query_raw(&format!(
            "SELECT count() FROM system.databases WHERE name = '{}'",
            TEST_DB
        ))
        .await
        .expect("Query failed");

    assert_eq!(result.trim(), "1");
}

// ============================================================================
// CTE SQL Generation Tests (ClickHouse format)
// ============================================================================

#[tokio::test]
#[serial(clickhouse)]
async fn test_transfer_cte_execution() {
    let ch = TestClickHouse::new(TEST_DB)
        .await
        .expect("Failed to create ClickHouse client");

    if ch.wait_for_ready().await.is_err() {
        println!("ClickHouse not available, skipping test");
        return;
    }

    ch.reset_database().await.expect("Failed to reset database");
    ch.create_mock_logs_table()
        .await
        .expect("Failed to create logs table");

    // Insert test Transfer logs
    ch.insert_transfer_log(
        1,
        0,
        "0xa975ba910c2ee169956f3df99ee2ece79d3887cf",
        "0xdead000000000000000000000000000000000000",
        1000000000000000000u128, // 1 ETH
    )
    .await
    .expect("Failed to insert log");

    ch.insert_transfer_log(
        2,
        0,
        "0xdead000000000000000000000000000000000000",
        "0xa975ba910c2ee169956f3df99ee2ece79d3887cf",
        500000000000000000u128, // 0.5 ETH
    )
    .await
    .expect("Failed to insert log");

    // Generate CTE and execute query
    let sig =
        EventSignature::parse("Transfer(address indexed from, address indexed to, uint256 value)")
            .expect("Failed to parse signature");

    let cte = sig.to_cte_sql_clickhouse();
    let sql = format!("WITH {} SELECT * FROM Transfer ORDER BY block_num", cte);

    let result = ch.query_json(&sql).await.expect("Query failed");
    let data = result.get("data").and_then(|d| d.as_array());

    assert!(data.is_some(), "Expected data array in response");
    assert_eq!(data.unwrap().len(), 2, "Expected 2 Transfer events");
}

#[tokio::test]
#[serial(clickhouse)]
async fn test_approval_cte_execution() {
    let ch = TestClickHouse::new(TEST_DB)
        .await
        .expect("Failed to create ClickHouse client");

    if ch.wait_for_ready().await.is_err() {
        println!("ClickHouse not available, skipping test");
        return;
    }

    ch.reset_database().await.expect("Failed to reset database");
    ch.create_mock_logs_table()
        .await
        .expect("Failed to create logs table");

    // Insert Approval log with proper topic0
    let approval_topic0 = "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925";
    let owner = "0x000000000000000000000000a975ba910c2ee169956f3df99ee2ece79d3887cf";
    let spender = "0x000000000000000000000000dAC17F958D2ee523a2206206994597C13D831ec7";
    let value_data = "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";

    ch.insert_mock_log(
        10,
        0,
        0,
        "0x0000000000000000000000000000000000000000000000000000000000000002",
        "0xdAC17F958D2ee523a2206206994597C13D831ec7",
        approval_topic0,
        owner,
        spender,
        "0x0000000000000000000000000000000000000000000000000000000000000000",
        value_data,
    )
    .await
    .expect("Failed to insert log");

    let sig = EventSignature::parse(
        "Approval(address indexed owner, address indexed spender, uint256 value)",
    )
    .expect("Failed to parse signature");

    let cte = sig.to_cte_sql_clickhouse();
    let sql = format!("WITH {} SELECT owner, spender, value FROM Approval", cte);

    let result = ch.query_json(&sql).await.expect("Query failed");
    let data = result.get("data").and_then(|d| d.as_array());

    assert!(data.is_some());
    assert_eq!(data.unwrap().len(), 1);
}

// ============================================================================
// Hex Literal Conversion Tests
// ============================================================================

#[tokio::test]
#[serial(clickhouse)]
async fn test_hex_literal_filter_execution() {
    let ch = TestClickHouse::new(TEST_DB)
        .await
        .expect("Failed to create ClickHouse client");

    if ch.wait_for_ready().await.is_err() {
        println!("ClickHouse not available, skipping test");
        return;
    }

    ch.reset_database().await.expect("Failed to reset database");
    ch.create_mock_logs_table()
        .await
        .expect("Failed to create logs table");

    // Insert logs with known address
    ch.insert_transfer_log(
        1,
        0,
        "0xa975ba910c2ee169956f3df99ee2ece79d3887cf",
        "0xdead000000000000000000000000000000000000",
        1000000000000000000u128,
    )
    .await
    .expect("Failed to insert log");

    ch.insert_transfer_log(
        2,
        0,
        "0xbeef000000000000000000000000000000000000",
        "0xdead000000000000000000000000000000000000",
        2000000000000000000u128,
    )
    .await
    .expect("Failed to insert log");

    // Build query with hex filter - test our hex literal conversion
    let sig =
        EventSignature::parse("Transfer(address indexed from, address indexed to, uint256 value)")
            .expect("Failed to parse signature");

    let user_query = r#"SELECT "from", "to", value FROM Transfer WHERE "from" = '0xa975ba910c2ee169956f3df99ee2ece79d3887cf'"#;

    // Apply CTE and predicate pushdown
    let normalized = sig.normalize_table_references(user_query);
    let pushed = sig.rewrite_filters_for_pushdown(&normalized);
    let cte = sig.to_cte_sql_clickhouse();
    let full_sql = format!("WITH {} {}", cte, pushed);

    println!("Final SQL: {}", full_sql);

    let result = ch.query_json(&full_sql).await.expect("Query failed");
    let data = result.get("data").and_then(|d| d.as_array());

    assert!(data.is_some());
    // Should only return 1 row matching the filter
    assert_eq!(data.unwrap().len(), 1);
}

// ============================================================================
// Aggregation Query Tests (OLAP workload)
// ============================================================================

#[tokio::test]
#[serial(clickhouse)]
async fn test_aggregation_query() {
    let ch = TestClickHouse::new(TEST_DB)
        .await
        .expect("Failed to create ClickHouse client");

    if ch.wait_for_ready().await.is_err() {
        println!("ClickHouse not available, skipping test");
        return;
    }

    ch.reset_database().await.expect("Failed to reset database");
    ch.create_mock_logs_table()
        .await
        .expect("Failed to create logs table");

    // Insert multiple transfers
    for i in 0..10 {
        ch.insert_transfer_log(
            i + 1,
            0,
            "0xa975ba910c2ee169956f3df99ee2ece79d3887cf",
            "0xdead000000000000000000000000000000000000",
            (i as u128 + 1) * 1000000000000000000u128,
        )
        .await
        .expect("Failed to insert log");
    }

    let sig =
        EventSignature::parse("Transfer(address indexed from, address indexed to, uint256 value)")
            .expect("Failed to parse signature");

    let cte = sig.to_cte_sql_clickhouse();
    let sql = format!(
        r#"WITH {} SELECT "from", COUNT(*) as transfer_count, SUM(value) as total_value FROM Transfer GROUP BY "from""#,
        cte
    );

    let result = ch.query_json(&sql).await.expect("Query failed");
    let data = result.get("data").and_then(|d| d.as_array());

    assert!(data.is_some());
    let rows = data.unwrap();
    assert_eq!(rows.len(), 1, "Expected 1 grouped row");

    // Check count (ClickHouse returns UInt64 as JSON number)
    let count = rows[0].get("transfer_count").and_then(|v| v.as_u64());
    assert_eq!(count, Some(10));
}

// ============================================================================
// Materialized View Creation Tests
// ============================================================================

#[tokio::test]
#[serial(clickhouse)]
async fn test_create_materialized_view_transfer_counts() {
    let ch = TestClickHouse::new(TEST_DB)
        .await
        .expect("Failed to create ClickHouse client");

    if ch.wait_for_ready().await.is_err() {
        println!("ClickHouse not available, skipping test");
        return;
    }

    ch.reset_database().await.expect("Failed to reset database");
    ch.create_mock_logs_table()
        .await
        .expect("Failed to create logs table");

    // Insert test data first
    for i in 0..5 {
        ch.insert_transfer_log(
            i + 1,
            0,
            "0xa975ba910c2ee169956f3df99ee2ece79d3887cf",
            "0xdead000000000000000000000000000000000000",
            1000000000000000000u128,
        )
        .await
        .expect("Failed to insert log");
    }

    let sig =
        EventSignature::parse("Transfer(address indexed from, address indexed to, uint256 value)")
            .expect("Failed to parse signature");

    let cte = sig.to_cte_sql_clickhouse();

    // Create target table
    let create_table = r#"CREATE TABLE transfer_counts (
            addr String,
            total_sent UInt64,
            total_received UInt64
        ) ENGINE = SummingMergeTree()
        ORDER BY addr"#;
    ch.query(&create_table)
        .await
        .expect("Failed to create table");

    // Create materialized view
    let create_mv = format!(
        r#"CREATE MATERIALIZED VIEW transfer_counts_mv TO transfer_counts AS
        WITH {}
        SELECT 
            addr,
            SUM(sent) as total_sent,
            SUM(received) as total_received
        FROM (
            SELECT "from" as addr, 1 as sent, 0 as received FROM Transfer
            UNION ALL
            SELECT "to" as addr, 0 as sent, 1 as received FROM Transfer
        )
        GROUP BY addr"#,
        cte
    );
    ch.query(&create_mv)
        .await
        .expect("Failed to create materialized view");

    // Backfill existing data
    let backfill = format!(
        r#"INSERT INTO transfer_counts
        WITH {}
        SELECT 
            addr,
            SUM(sent) as total_sent,
            SUM(received) as total_received
        FROM (
            SELECT "from" as addr, 1 as sent, 0 as received FROM Transfer
            UNION ALL
            SELECT "to" as addr, 0 as sent, 1 as received FROM Transfer
        )
        GROUP BY addr"#,
        cte
    );
    ch.query(&backfill).await.expect("Failed to backfill");

    // Query the materialized view
    let result = ch
        .query_json("SELECT * FROM transfer_counts ORDER BY addr")
        .await
        .expect("Query failed");

    let data = result.get("data").and_then(|d| d.as_array());
    assert!(data.is_some());
    assert!(
        data.unwrap().len() >= 2,
        "Expected at least 2 addresses (from and to)"
    );
}

#[tokio::test]
#[serial(clickhouse)]
async fn test_create_materialized_view_token_supply() {
    let ch = TestClickHouse::new(TEST_DB)
        .await
        .expect("Failed to create ClickHouse client");

    if ch.wait_for_ready().await.is_err() {
        println!("ClickHouse not available, skipping test");
        return;
    }

    ch.reset_database().await.expect("Failed to reset database");
    ch.create_mock_logs_table()
        .await
        .expect("Failed to create logs table");

    // Insert mint (from zero address)
    ch.insert_transfer_log(
        1,
        0,
        "0x0000000000000000000000000000000000000000",
        "0xa975ba910c2ee169956f3df99ee2ece79d3887cf",
        1000000000000000000000u128, // 1000 tokens
    )
    .await
    .expect("Failed to insert mint");

    // Insert burn (to zero address)
    ch.insert_transfer_log(
        2,
        0,
        "0xa975ba910c2ee169956f3df99ee2ece79d3887cf",
        "0x0000000000000000000000000000000000000000",
        100000000000000000000u128, // 100 tokens burned
    )
    .await
    .expect("Failed to insert burn");

    let sig =
        EventSignature::parse("Transfer(address indexed from, address indexed to, uint256 value)")
            .expect("Failed to parse signature");

    let cte = sig.to_cte_sql_clickhouse();

    // Create target table for token supply
    let create_table = r#"CREATE TABLE token_supply (
        token String,
        supply Int256
    ) ENGINE = SummingMergeTree()
    ORDER BY token"#;
    ch.query(create_table)
        .await
        .expect("Failed to create table");

    // Query to calculate supply (mints - burns)
    // Note: CTE extracts addresses with '0x' prefix, so we compare against '0x...' directly
    // Use toString() to avoid JSON float precision loss for large integers
    let supply_sql = format!(
        r#"WITH {}
        SELECT 
            address as token,
            toString(SUM(CASE 
                WHEN "from" = '0x0000000000000000000000000000000000000000' THEN CAST(value AS Int256)
                WHEN "to" = '0x0000000000000000000000000000000000000000' THEN -CAST(value AS Int256)
                ELSE 0
            END)) as supply
        FROM Transfer
        GROUP BY token"#,
        cte
    );

    let result = ch.query_json(&supply_sql).await.expect("Query failed");
    let data = result.get("data").and_then(|d| d.as_array());

    assert!(data.is_some());
    let rows = data.unwrap();
    assert_eq!(rows.len(), 1);

    // Supply should be 1000 - 100 = 900 tokens (in wei: 900 * 10^18)
    let supply = rows[0].get("supply").and_then(|v| v.as_str());
    assert_eq!(supply, Some("900000000000000000000"));
}

#[tokio::test]
#[serial(clickhouse)]
async fn test_create_materialized_view_daily_stats() {
    let ch = TestClickHouse::new(TEST_DB)
        .await
        .expect("Failed to create ClickHouse client");

    if ch.wait_for_ready().await.is_err() {
        println!("ClickHouse not available, skipping test");
        return;
    }

    ch.reset_database().await.expect("Failed to reset database");
    ch.create_mock_logs_table()
        .await
        .expect("Failed to create logs table");

    // Insert transfers
    for i in 0..3 {
        ch.insert_transfer_log(
            i + 1,
            0,
            "0xa975ba910c2ee169956f3df99ee2ece79d3887cf",
            "0xdead000000000000000000000000000000000000",
            1000000000000000000u128,
        )
        .await
        .expect("Failed to insert log");
    }

    let sig =
        EventSignature::parse("Transfer(address indexed from, address indexed to, uint256 value)")
            .expect("Failed to parse signature");

    let cte = sig.to_cte_sql_clickhouse();

    // Daily stats query
    let sql = format!(
        r#"WITH {}
        SELECT 
            toDate(block_timestamp) as day,
            address as token,
            COUNT(*) as transfer_count,
            COUNT(DISTINCT "from") as unique_senders,
            COUNT(DISTINCT "to") as unique_receivers,
            SUM(value) as total_volume
        FROM Transfer
        GROUP BY day, token"#,
        cte
    );

    let result = ch.query_json(&sql).await.expect("Query failed");
    let data = result.get("data").and_then(|d| d.as_array());

    assert!(data.is_some());
    let rows = data.unwrap();
    assert!(!rows.is_empty());

    // Check aggregations exist
    let first = &rows[0];
    assert!(first.get("transfer_count").is_some());
    assert!(first.get("unique_senders").is_some());
    assert!(first.get("total_volume").is_some());
}

// ============================================================================
// Complex Query Tests
// ============================================================================

#[tokio::test]
#[serial(clickhouse)]
async fn test_uniswap_swap_cte() {
    let ch = TestClickHouse::new(TEST_DB)
        .await
        .expect("Failed to create ClickHouse client");

    if ch.wait_for_ready().await.is_err() {
        println!("ClickHouse not available, skipping test");
        return;
    }

    ch.reset_database().await.expect("Failed to reset database");
    ch.create_mock_logs_table()
        .await
        .expect("Failed to create logs table");

    // Insert Swap event
    // Swap(address indexed sender, uint256 amount0In, uint256 amount1In, uint256 amount0Out, uint256 amount1Out, address indexed to)
    let swap_topic0 = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822";
    let sender = "0x0000000000000000000000007a250d5630B4cF539739dF2C5dAcb4c659F2488D";
    let to = "0x000000000000000000000000a975ba910c2ee169956f3df99ee2ece79d3887cf";

    // Data: 4 uint256 values (amount0In, amount1In, amount0Out, amount1Out)
    let data = "0x0000000000000000000000000000000000000000000000000de0b6b3a76400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001b1ae4d6e2ef500000";

    ch.insert_mock_log(
        100,
        0,
        0,
        "0x0000000000000000000000000000000000000000000000000000000000000003",
        "0x0d4a11d5EEaaC28EC3F61d100daF4d40471f1852", // USDT-WETH pair
        swap_topic0,
        sender,
        to,
        "0x0000000000000000000000000000000000000000000000000000000000000000",
        data,
    )
    .await
    .expect("Failed to insert swap");

    let sig = EventSignature::parse(
        "Swap(address indexed sender, uint256 amount0In, uint256 amount1In, uint256 amount0Out, uint256 amount1Out, address indexed to)"
    ).expect("Failed to parse signature");

    let cte = sig.to_cte_sql_clickhouse();
    let sql = format!(
        r#"WITH {}
        SELECT 
            address as pair,
            sender,
            "to",
            amount0In,
            amount1In,
            amount0Out,
            amount1Out
        FROM Swap"#,
        cte
    );

    let result = ch.query_json(&sql).await.expect("Query failed");
    let data = result.get("data").and_then(|d| d.as_array());

    assert!(data.is_some());
    assert_eq!(data.unwrap().len(), 1);
}

#[tokio::test]
#[serial(clickhouse)]
async fn test_role_granted_cte() {
    let ch = TestClickHouse::new(TEST_DB)
        .await
        .expect("Failed to create ClickHouse client");

    if ch.wait_for_ready().await.is_err() {
        println!("ClickHouse not available, skipping test");
        return;
    }

    ch.reset_database().await.expect("Failed to reset database");
    ch.create_mock_logs_table()
        .await
        .expect("Failed to create logs table");

    // RoleGranted(bytes32 indexed role, address indexed account, address indexed sender)
    let role_granted_topic0 = "0x2f8788117e7eff1d82e926ec794901d17c78024a50270940304540a733656f0d";
    let role = "0x0000000000000000000000000000000000000000000000000000000000000000"; // DEFAULT_ADMIN_ROLE
    let account = "0x000000000000000000000000a975ba910c2ee169956f3df99ee2ece79d3887cf";
    let sender = "0x0000000000000000000000000000000000000000000000000000000000000000";

    ch.insert_mock_log(
        50,
        0,
        0,
        "0x0000000000000000000000000000000000000000000000000000000000000004",
        "0x6B175474E89094C44Da98b954EescdKF63C02B94", // DAI
        role_granted_topic0,
        role,
        account,
        sender,
        "0x",
    )
    .await
    .expect("Failed to insert role granted");

    let sig = EventSignature::parse(
        "RoleGranted(bytes32 indexed role, address indexed account, address indexed sender)",
    )
    .expect("Failed to parse signature");

    let cte = sig.to_cte_sql_clickhouse();
    let sql = format!("WITH {} SELECT role, account, sender FROM RoleGranted", cte);

    let result = ch.query_json(&sql).await.expect("Query failed");
    let data = result.get("data").and_then(|d| d.as_array());

    assert!(data.is_some());
    assert_eq!(data.unwrap().len(), 1);
}

/// `query_user` (the public /query path) must execute parenthesized UNION arms
/// with a trailing ORDER BY/LIMIT, a shape valid in PostgreSQL. ClickHouse
/// grammar rejects it today (Code 62), so callers get an opaque error.
#[tokio::test]
#[serial(clickhouse)]
async fn test_query_user_union_with_trailing_order_by() {
    let ch = TestClickHouse::new("tidx_repro_union_ch")
        .await
        .expect("Failed to create ClickHouse client");

    if ch.wait_for_ready().await.is_err() {
        println!("ClickHouse not available, skipping test");
        return;
    }

    ch.reset_database().await.expect("Failed to reset database");
    ch.create_mock_blocks_table()
        .await
        .expect("Failed to create blocks table");
    ch.query("INSERT INTO blocks (num, hash) VALUES (1, '0x01'), (2, '0x02'), (3, '0x03')")
        .await
        .expect("Failed to insert blocks");

    let config = ClickHouseConfig {
        enabled: true,
        url: ch.url.clone(),
        database: Some(ch.database.clone()),
        ..Default::default()
    };
    let engine = ClickHouseEngine::new(&config, 4217).expect("Failed to create engine");

    let sql = "(SELECT num FROM blocks ORDER BY num DESC LIMIT 2) \
               UNION (SELECT num FROM blocks ORDER BY num DESC LIMIT 2) \
               ORDER BY num DESC LIMIT 2";
    let result = engine
        .query_user(sql, &[], 5_000, 100)
        .await
        .expect("union with trailing ORDER BY should execute");

    assert_eq!(result.rows.len(), 2);
}

// ============================================================================
// Predicate Pushdown Tests
// ============================================================================

#[tokio::test]
#[serial(clickhouse)]
async fn test_predicate_pushdown_indexed_param() {
    let ch = TestClickHouse::new(TEST_DB)
        .await
        .expect("Failed to create ClickHouse client");

    if ch.wait_for_ready().await.is_err() {
        println!("ClickHouse not available, skipping test");
        return;
    }

    ch.reset_database().await.expect("Failed to reset database");
    ch.create_mock_logs_table()
        .await
        .expect("Failed to create logs table");

    // Insert logs from two different addresses
    for i in 0..5 {
        ch.insert_transfer_log(
            i + 1,
            0,
            "0xa975ba910c2ee169956f3df99ee2ece79d3887cf",
            "0xdead000000000000000000000000000000000000",
            1000000000000000000u128,
        )
        .await
        .expect("Failed to insert log");
    }

    for i in 0..3 {
        ch.insert_transfer_log(
            i + 10,
            0,
            "0xbeef000000000000000000000000000000000000",
            "0xdead000000000000000000000000000000000000",
            2000000000000000000u128,
        )
        .await
        .expect("Failed to insert log");
    }

    let sig =
        EventSignature::parse("Transfer(address indexed from, address indexed to, uint256 value)")
            .expect("Failed to parse signature");

    // Query with filter on indexed param - should use predicate pushdown
    let user_sql = r#"SELECT COUNT(*) as cnt FROM Transfer WHERE "from" = '0xa975ba910c2ee169956f3df99ee2ece79d3887cf'"#;

    let normalized = sig.normalize_table_references(user_sql);
    let pushed = sig.rewrite_filters_for_pushdown(&normalized);
    let cte = sig.to_cte_sql_clickhouse();
    let full_sql = format!("WITH {} {}", cte, pushed);

    println!("Pushdown SQL: {}", full_sql);

    // Verify the topic1 filter is in the CTE WHERE clause
    assert!(
        full_sql.contains("topic1 ="),
        "Expected topic1 predicate pushdown"
    );

    let result = ch.query_json(&full_sql).await.expect("Query failed");
    let data = result.get("data").and_then(|d| d.as_array());

    assert!(data.is_some());
    let cnt = data.unwrap()[0].get("cnt").and_then(|v| v.as_u64());
    assert_eq!(cnt, Some(5), "Expected 5 transfers from address a975...");
}

// ============================================================================
// Case-Insensitive Table Reference Tests
// ============================================================================

#[tokio::test]
#[serial(clickhouse)]
async fn test_case_insensitive_table_reference() {
    let ch = TestClickHouse::new(TEST_DB)
        .await
        .expect("Failed to create ClickHouse client");

    if ch.wait_for_ready().await.is_err() {
        println!("ClickHouse not available, skipping test");
        return;
    }

    ch.reset_database().await.expect("Failed to reset database");
    ch.create_mock_logs_table()
        .await
        .expect("Failed to create logs table");

    ch.insert_transfer_log(1, 0, "0xabc", "0xdef", 1000u128)
        .await
        .unwrap();

    let sig =
        EventSignature::parse("Transfer(address indexed from, address indexed to, uint256 value)")
            .expect("Failed to parse signature");

    // Test with lowercase table name (signature is "Transfer" with capital T)
    let user_sql = "SELECT * FROM transfer";
    let normalized = sig.normalize_table_references(user_sql);

    // Should have replaced "transfer" with "Transfer" to match CTE
    let cte = sig.to_cte_sql_clickhouse();
    let full_sql = format!("WITH {} {}", cte, normalized);

    let result = ch.query_json(&full_sql).await.expect("Query failed");
    let data = result.get("data").and_then(|d| d.as_array());

    assert!(data.is_some());
    assert_eq!(data.unwrap().len(), 1);
}

// ============================================================================
// ClickHouseSink Direct-Write Tests
// ============================================================================

const SINK_DB: &str = "tidx_sink_test";
const TEST_CHAIN_ID: u64 = 99999;
const RETIRED_CLICKHOUSE_OBJECTS: &[&str] = &[
    "address_transfers",
    "address_transfers_mv",
    "address_txs",
    "address_txs_mv",
    "contract_creations",
    "contract_creations_mv",
    "token_approvals",
    "token_approvals_current",
    "token_approvals_mv",
    "balance_dirty_keys",
    "balance_dirty_keys_mv",
    "balance_reorg_keys",
    "balance_state",
    "balance_state_clean_mv",
    "balance_state_refresh",
];

async fn seed_retired_clickhouse_objects(ch: &TestClickHouse) {
    for table in [
        "address_transfers",
        "address_txs",
        "contract_creations",
        "token_approvals",
    ] {
        ch.query(&format!(
            "CREATE TABLE {table} (marker UInt8) ENGINE = MergeTree ORDER BY marker"
        ))
        .await
        .unwrap_or_else(|_| panic!("failed to seed {table}"));
    }

    for (view, target) in [
        ("address_transfers_mv", "address_transfers"),
        ("address_txs_mv", "address_txs"),
        ("contract_creations_mv", "contract_creations"),
        ("token_approvals_mv", "token_approvals"),
    ] {
        ch.query(&format!(
            "CREATE MATERIALIZED VIEW {view} TO {target} AS SELECT toUInt8(1) AS marker"
        ))
        .await
        .unwrap_or_else(|_| panic!("failed to seed {view}"));
    }
    ch.query("CREATE VIEW token_approvals_current AS SELECT marker FROM token_approvals")
        .await
        .expect("failed to seed token_approvals_current");

    seed_retired_balance_state_objects(ch).await;
}

async fn seed_retired_balance_state_objects(ch: &TestClickHouse) {
    ch.query(
        "CREATE TABLE balance_dirty_keys (token String, holder String) \
         ENGINE = MergeTree ORDER BY (token, holder)",
    )
    .await
    .expect("failed to seed balance_dirty_keys");
    ch.query(
        "CREATE TABLE balance_reorg_keys (from_block Int64, token String, holder String) \
         ENGINE = MergeTree ORDER BY (from_block, token, holder)",
    )
    .await
    .expect("failed to seed balance_reorg_keys");
    ch.query(
        "CREATE TABLE balance_state (token String, holder String, balance UInt256) \
         ENGINE = MergeTree ORDER BY (token, holder)",
    )
    .await
    .expect("failed to seed balance_state");
    ch.query("CREATE VIEW balance_dirty_keys_mv AS SELECT token, holder FROM balance_dirty_keys")
        .await
        .expect("failed to seed balance_dirty_keys_mv");
    ch.query("CREATE VIEW balance_state_clean_mv AS SELECT token, holder FROM balance_dirty_keys")
        .await
        .expect("failed to seed balance_state_clean_mv");
    ch.query(
        "CREATE VIEW balance_state_refresh AS SELECT token, holder, balance FROM balance_state",
    )
    .await
    .expect("failed to seed balance_state_refresh");
}

async fn assert_retired_clickhouse_objects_absent(ch: &TestClickHouse) {
    let names = RETIRED_CLICKHOUSE_OBJECTS
        .iter()
        .map(|name| format!("'{name}'"))
        .collect::<Vec<_>>()
        .join(", ");
    let retired = ch
        .query_json(&format!(
            "SELECT name FROM system.tables WHERE database = currentDatabase() AND name IN ({names})"
        ))
        .await
        .expect("failed to inspect retired objects");
    assert_eq!(retired["rows"], 0);
}

/// Helper: create a ClickHouseSink pointed at the test instance, with a clean DB.
async fn setup_sink() -> Option<(ClickHouseSink, TestClickHouse)> {
    let ch = TestClickHouse::new(SINK_DB)
        .await
        .expect("Failed to create CH client");
    if ch.wait_for_ready().await.is_err() {
        println!("ClickHouse not available, skipping test");
        return None;
    }
    // Drop any previous test state
    ch.reset_database().await.expect("Failed to reset database");

    let sink = ClickHouseSink::new(&ch.url, SINK_DB, None, None).expect("Failed to create sink");
    sink.ensure_schema().await.expect("Failed to ensure schema");
    Some((sink, ch))
}

fn make_block(num: i64) -> BlockRow {
    use chrono::TimeZone;
    let ts = chrono::Utc.with_ymd_and_hms(2025, 1, 15, 12, 0, 0).unwrap()
        + chrono::Duration::seconds(num);
    BlockRow {
        num,
        hash: vec![num as u8; 32],
        parent_hash: vec![(num.wrapping_sub(1)) as u8; 32],
        timestamp: ts,
        timestamp_ms: ts.timestamp_millis(),
        gas_limit: 30_000_000,
        gas_used: 21_000 * num,
        miner: vec![0xaa; 20],
        extra_data: Some(vec![0xbb, 0xcc]),
        consensus_proposer: None,
    }
}

fn make_tx(block_num: i64, idx: i32) -> TxRow {
    use chrono::TimeZone;
    let ts = chrono::Utc.with_ymd_and_hms(2025, 1, 15, 12, 0, 0).unwrap()
        + chrono::Duration::seconds(block_num);
    TxRow {
        block_num,
        block_timestamp: ts,
        idx,
        hash: {
            let mut h = vec![block_num as u8; 16];
            h.extend_from_slice(&[idx as u8; 16]);
            h
        },
        tx_type: 2,
        from: vec![0x11; 20],
        to: Some(vec![0x22; 20]),
        value: "1000000000000000000".to_string(),
        input: vec![0xa9, 0x05, 0x9c, 0xbb],
        gas_limit: 21_000,
        max_fee_per_gas: "100000000000".to_string(),
        max_priority_fee_per_gas: "1000000000".to_string(),
        gas_used: Some(21_000),
        nonce_key: vec![0x11; 20],
        nonce: block_num,
        fee_token: None,
        fee_payer: None,
        calls: None,
        call_count: 1,
        valid_before: None,
        valid_after: None,
        signature_type: None,
    }
}

fn make_log(block_num: i64, log_idx: i32) -> LogRow {
    use chrono::TimeZone;
    let ts = chrono::Utc.with_ymd_and_hms(2025, 1, 15, 12, 0, 0).unwrap()
        + chrono::Duration::seconds(block_num);
    LogRow {
        block_num,
        block_timestamp: ts,
        log_idx,
        tx_idx: 0,
        tx_hash: vec![block_num as u8; 32],
        address: vec![0xda; 20],
        selector: Some(vec![0xdd, 0xf2, 0x52, 0xad]),
        topic0: Some(vec![0xdd; 32]),
        topic1: Some(vec![0x11; 32]),
        topic2: Some(vec![0x22; 32]),
        topic3: None,
        data: vec![0x00; 32],
        is_virtual_forward: false,
    }
}

fn make_transfer_log(block_num: i64, log_idx: i32, from: &str, to: &str, value: u128) -> LogRow {
    use chrono::TimeZone;
    let ts = chrono::Utc.with_ymd_and_hms(2025, 1, 15, 12, 0, 0).unwrap()
        + chrono::Duration::seconds(block_num);
    let topic0 =
        hex::decode("ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef").unwrap();
    let mut data = vec![0; 16];
    data.extend_from_slice(&value.to_be_bytes());

    LogRow {
        block_num,
        block_timestamp: ts,
        log_idx,
        tx_idx: 0,
        tx_hash: vec![block_num as u8; 32],
        address: vec![0xda; 20],
        selector: Some(topic0.clone()),
        topic0: Some(topic0),
        topic1: Some(padded_topic_address(from)),
        topic2: Some(padded_topic_address(to)),
        topic3: None,
        data,
        is_virtual_forward: false,
    }
}

fn padded_topic_address(address: &str) -> Vec<u8> {
    let bytes = hex::decode(address.trim_start_matches("0x")).unwrap();
    assert_eq!(bytes.len(), 20);
    let mut topic = vec![0; 12];
    topic.extend_from_slice(&bytes);
    topic
}

fn make_receipt(block_num: i64, tx_idx: i32) -> ReceiptRow {
    use chrono::TimeZone;
    let ts = chrono::Utc.with_ymd_and_hms(2025, 1, 15, 12, 0, 0).unwrap()
        + chrono::Duration::seconds(block_num);
    ReceiptRow {
        block_num,
        block_timestamp: ts,
        tx_idx,
        tx_hash: vec![block_num as u8; 32],
        from: vec![0x11; 20],
        to: Some(vec![0x22; 20]),
        contract_address: None,
        gas_used: 21_000,
        cumulative_gas_used: 21_000,
        effective_gas_price: Some("100000000000".to_string()),
        status: Some(1),
        fee_payer: None,
        tx_type: None,
        fee_token: None,
    }
}

#[tokio::test]
#[serial(clickhouse)]
async fn test_sink_ensure_schema_creates_tables() {
    let Some((_, ch)) = setup_sink().await else {
        return;
    };

    for table in [
        "blocks",
        "txs",
        "logs",
        "receipts",
        "token_transfers",
        "token_holder_deltas",
        "token_balances_snapshot",
    ] {
        let count = ch
            .table_count(table)
            .await
            .unwrap_or_else(|_| panic!("Table {table} should exist"));
        assert_eq!(count, 0, "{table} should be empty after schema creation");

        let ddl = ch
            .query(&format!("SHOW CREATE TABLE {table}"))
            .await
            .unwrap_or_else(|_| panic!("Table {table} should expose its DDL"));
        assert!(ddl.contains("default_compression_codec"));
        assert!(
            ddl.contains("ZSTD(1)"),
            "table {table} should use ZSTD(1) by default: {ddl}"
        );
    }

    let tracked = ch
        .table_count("tidx_schema_objects")
        .await
        .expect("schema object tracking should exist");
    assert!(tracked > 0);

    let count = ch
        .table_count("token_balances")
        .await
        .expect("token_balances view should exist");
    assert_eq!(count, 0);

    let names = RETIRED_CLICKHOUSE_OBJECTS
        .iter()
        .map(|name| format!("'{name}'"))
        .collect::<Vec<_>>()
        .join(", ");
    let retired = ch
        .query_json(&format!(
            "SELECT name FROM system.tables WHERE database = currentDatabase() AND name IN ({names})"
        ))
        .await
        .expect("failed to inspect retired objects");
    assert_eq!(retired["rows"], 0);
}

#[tokio::test]
#[serial(clickhouse)]
async fn test_sink_ensure_schema_drops_retired_objects() {
    let ch = TestClickHouse::new(SINK_DB)
        .await
        .expect("Failed to create CH client");
    if ch.wait_for_ready().await.is_err() {
        println!("ClickHouse not available, skipping test");
        return;
    }
    ch.reset_database().await.expect("Failed to reset database");

    seed_retired_clickhouse_objects(&ch).await;

    let sink = ClickHouseSink::new(&ch.url, SINK_DB, None, None).expect("Failed to create sink");
    sink.ensure_schema()
        .await
        .expect("first ensure_schema failed");
    assert_retired_clickhouse_objects_absent(&ch).await;

    seed_retired_clickhouse_objects(&ch).await;
    sink.ensure_schema()
        .await
        .expect("second ensure_schema failed");
    assert_retired_clickhouse_objects_absent(&ch).await;
}

#[tokio::test]
#[serial(clickhouse)]
async fn test_sink_replaces_snapshot_after_incremental_state_schema() {
    let Some((sink, ch)) = setup_sink().await else {
        return;
    };

    // Model the important parts of the PR #271 transition: the canonical
    // snapshot needs replacement while a refresh-dependent child exists, and
    // the retired reducer objects are still present.
    ch.query("DROP VIEW IF EXISTS address_balances_snapshot SYNC")
        .await
        .expect("failed to remove address snapshot fixture");
    seed_retired_balance_state_objects(&ch).await;
    ch.query(
        "ALTER TABLE tidx_schema_objects DELETE WHERE name IN (\
            'balance_state_20260715_stop_refresh', \
            'token_holder_counts_20260715_drop_before_snapshot_replace', \
            'token_balances_snapshot'\
         ) SETTINGS mutations_sync = 1",
    )
    .await
    .expect("failed to reset transition tracking fixture");
    ch.query(
        "INSERT INTO tidx_schema_objects (name, checksum, kind) \
         VALUES \
            ('token_balances_snapshot', 'pre-rollback-definition', \
             'refreshable_materialized_view'), \
            ('balance_state_20260714_bootstrap', 'applied-by-pr-271', 'migration')",
    )
    .await
    .expect("failed to seed stale snapshot checksum");

    sink.ensure_schema_only()
        .await
        .expect("incremental state schema transition failed");

    assert_retired_clickhouse_objects_absent(&ch).await;
    let retired_bootstrap = ch
        .query(
            "SELECT count() FROM tidx_schema_objects FINAL \
             WHERE name = 'balance_state_20260714_bootstrap'",
        )
        .await
        .expect("failed to inspect retired bootstrap tracking");
    assert_eq!(retired_bootstrap.trim(), "0");
    for view in [
        "token_balances_snapshot",
        "address_balances_snapshot",
        "token_holder_counts",
    ] {
        let count = ch
            .query(&format!(
                "SELECT count() FROM system.tables WHERE database = currentDatabase() \
                 AND name = '{view}'"
            ))
            .await
            .unwrap_or_else(|_| panic!("failed to inspect recreated {view}"));
        assert_eq!(count.trim(), "1", "{view} should be recreated");
    }
}

#[tokio::test]
#[serial(clickhouse)]
async fn test_sink_write_blocks() {
    let Some((sink, ch)) = setup_sink().await else {
        return;
    };

    let blocks: Vec<BlockRow> = (1..=5).map(make_block).collect();
    sink.write_blocks(&blocks)
        .await
        .expect("write_blocks failed");

    let count = ch.table_count("blocks").await.expect("count failed");
    assert_eq!(count, 5);

    // Verify data roundtrips correctly
    let result = ch
        .query_json("SELECT num, gas_limit FROM blocks ORDER BY num")
        .await
        .unwrap();
    let rows = result["data"].as_array().unwrap();
    let num0 = rows[0]["num"]
        .as_i64()
        .or_else(|| rows[0]["num"].as_str().and_then(|s| s.parse().ok()))
        .unwrap();
    let num4 = rows[4]["num"]
        .as_i64()
        .or_else(|| rows[4]["num"].as_str().and_then(|s| s.parse().ok()))
        .unwrap();
    assert_eq!(num0, 1);
    assert_eq!(num4, 5);
}

/// A retried insert re-sends an identical part; the insert dedup window must
/// drop it instead of writing duplicate ReplacingMergeTree rows.
#[tokio::test]
#[serial(clickhouse)]
async fn test_retried_identical_insert_is_deduplicated() {
    let Some((sink, ch)) = setup_sink().await else {
        return;
    };

    // Block merges so surviving duplicates stay visible as physical rows.
    ch.query_raw(&format!("SYSTEM STOP MERGES {SINK_DB}.blocks"))
        .await
        .expect("stop merges failed");

    let blocks: Vec<BlockRow> = (1..=5).map(make_block).collect();
    sink.write_blocks(&blocks)
        .await
        .expect("write_blocks failed");
    sink.write_blocks(&blocks)
        .await
        .expect("write_blocks retry failed");

    assert_eq!(ch.table_count("blocks").await.unwrap(), 5);
}

#[tokio::test]
#[serial(clickhouse)]
async fn test_sink_write_txs() {
    let Some((sink, ch)) = setup_sink().await else {
        return;
    };

    let txs: Vec<TxRow> = (1..=3)
        .flat_map(|b| (0..2).map(move |i| make_tx(b, i)))
        .collect();
    sink.write_txs(&txs).await.expect("write_txs failed");

    let count = ch.table_count("txs").await.expect("count failed");
    assert_eq!(count, 6);
}

#[tokio::test]
#[serial(clickhouse)]
async fn test_sink_write_logs() {
    let Some((sink, ch)) = setup_sink().await else {
        return;
    };

    let logs: Vec<LogRow> = (1..=3)
        .flat_map(|b| (0..4).map(move |i| make_log(b, i)))
        .collect();
    sink.write_logs(&logs).await.expect("write_logs failed");

    let count = ch.table_count("logs").await.expect("count failed");
    assert_eq!(count, 12);
}

#[tokio::test]
#[serial(clickhouse)]
async fn test_sink_write_logs_roundtrips_virtual_forward_flag() {
    let Some((sink, ch)) = setup_sink().await else {
        return;
    };

    let mut logs: Vec<LogRow> = (1..=2).map(|b| make_log(b, 0)).collect();
    logs[1].is_virtual_forward = true;
    sink.write_logs(&logs).await.expect("write_logs failed");

    let result = ch
        .query_json("SELECT block_num, is_virtual_forward FROM logs ORDER BY block_num")
        .await
        .unwrap();
    let rows = result["data"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["is_virtual_forward"].as_u64(), Some(0));
    assert_eq!(rows[1]["is_virtual_forward"].as_u64(), Some(1));
}

#[tokio::test]
#[serial(clickhouse)]
async fn test_sink_token_transfers_decodes_transfers_and_reorgs() {
    let Some((sink, ch)) = setup_sink().await else {
        return;
    };

    let zero = "0x0000000000000000000000000000000000000000";
    let alice = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let bob = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    let mut logs = vec![
        make_transfer_log(1, 0, zero, alice, 100),
        make_transfer_log(2, 0, alice, bob, 40),
        make_transfer_log(3, 0, bob, zero, 10),
    ];
    logs[2].is_virtual_forward = true;

    sink.write_logs(&logs).await.expect("write_logs failed");

    let rows = token_transfers(&ch).await;
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].token, "0xdadadadadadadadadadadadadadadadadadadada");
    assert_eq!(rows[0].from, zero);
    assert_eq!(rows[0].to, alice);
    assert_eq!(rows[0].amount, "100");
    assert_eq!(rows[0].is_virtual_forward, 0);
    assert_eq!(rows[2].from, bob);
    assert_eq!(rows[2].to, zero);
    assert_eq!(rows[2].amount, "10");
    assert_eq!(rows[2].is_virtual_forward, 1);

    sink.delete_from(3).await.expect("delete_from failed");

    let rows = token_transfers(&ch).await;
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|row| row.block_num < 3));
}

#[tokio::test]
#[serial(clickhouse)]
async fn test_sink_token_balances_view_tracks_balances_and_reorgs() {
    let Some((sink, ch)) = setup_sink().await else {
        return;
    };

    let zero = "0x0000000000000000000000000000000000000000";
    let alice = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let bob = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    let logs = vec![
        make_transfer_log(1, 0, zero, alice, 100),
        make_transfer_log(2, 0, alice, bob, 40),
        make_transfer_log(3, 0, bob, zero, 10),
    ];
    sink.write_logs(&logs).await.expect("write_logs failed");

    let balances = token_holder_balances(&ch).await;
    assert_eq!(balances.get(alice).map(String::as_str), Some("60"));
    assert_eq!(balances.get(bob).map(String::as_str), Some("30"));
    assert!(!balances.contains_key(zero));

    // Replaying the source batch creates physical duplicates, but FINAL keeps
    // the canonical snapshot idempotent. The address snapshot must be the same
    // atomic balance generation with only its storage order changed.
    sink.write_logs(&logs)
        .await
        .expect("replayed write_logs failed");
    refresh_balance_snapshots(&ch).await;
    assert_eq!(
        balance_rows(&ch, "token_balances_snapshot").await,
        balance_rows(&ch, "address_balances_snapshot").await
    );
    assert_eq!(balance_rows(&ch, "token_balances_snapshot").await.len(), 2);

    sink.delete_from(3).await.expect("delete_from failed");

    let balances = token_holder_balances(&ch).await;
    assert_eq!(balances.get(alice).map(String::as_str), Some("60"));
    assert_eq!(balances.get(bob).map(String::as_str), Some("40"));

    refresh_balance_snapshots(&ch).await;
    assert_eq!(
        balance_rows(&ch, "token_balances_snapshot").await,
        balance_rows(&ch, "address_balances_snapshot").await
    );
    let snapshot = balance_rows(&ch, "token_balances_snapshot").await;
    assert!(
        snapshot
            .iter()
            .any(|(_, holder, balance)| { holder == bob && balance == "40" })
    );
}

#[tokio::test]
#[serial(clickhouse)]
async fn test_sink_ensure_schema_backfills_token_transfer_views() {
    let ch = TestClickHouse::new(SINK_DB)
        .await
        .expect("Failed to create CH client");
    if ch.wait_for_ready().await.is_err() {
        println!("ClickHouse not available, skipping test");
        return;
    }
    ch.reset_database().await.expect("Failed to reset database");

    for object in base_objects() {
        let ddl = object.ddl();
        ch.query(&ddl)
            .await
            .unwrap_or_else(|_| panic!("base object {} should be created", object.name));
    }
    for migration in migrations() {
        let ddl = migration.ddl();
        ch.query(&ddl)
            .await
            .unwrap_or_else(|_| panic!("migration {} should run", migration.name));
    }

    let sink = ClickHouseSink::new(&ch.url, SINK_DB, None, None).expect("Failed to create sink");
    let zero = "0x0000000000000000000000000000000000000000";
    let alice = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let bob = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    let logs = vec![
        make_transfer_log(1, 0, zero, alice, 100),
        make_transfer_log(2, 0, alice, bob, 40),
        make_transfer_log(3, 0, bob, zero, 10),
    ];
    sink.write_logs(&logs).await.expect("write_logs failed");

    sink.ensure_schema().await.expect("ensure_schema failed");

    let transfers = token_transfers(&ch).await;
    assert_eq!(transfers.len(), 3);

    let balances = token_holder_balances(&ch).await;
    assert_eq!(balances.get(alice).map(String::as_str), Some("60"));
    assert_eq!(balances.get(bob).map(String::as_str), Some("30"));
    assert!(!balances.contains_key(zero));
}

struct TokenTransferEvent {
    block_num: i64,
    token: String,
    from: String,
    to: String,
    amount: String,
    is_virtual_forward: u64,
}

async fn token_transfers(ch: &TestClickHouse) -> Vec<TokenTransferEvent> {
    let result = ch
        .query_json(
            r#"
            SELECT
                block_num,
                token,
                `from`,
                `to`,
                toString(amount) AS amount,
                is_virtual_forward
            FROM token_transfers
            ORDER BY block_num, log_idx
            "#,
        )
        .await
        .expect("token_transfers query failed");
    result["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| TokenTransferEvent {
            block_num: row["block_num"]
                .as_i64()
                .or_else(|| {
                    row["block_num"]
                        .as_str()
                        .and_then(|value| value.parse().ok())
                })
                .unwrap(),
            token: row["token"].as_str().unwrap().to_string(),
            from: row["from"].as_str().unwrap().to_string(),
            to: row["to"].as_str().unwrap().to_string(),
            amount: row["amount"].as_str().unwrap().to_string(),
            is_virtual_forward: row["is_virtual_forward"].as_u64().unwrap(),
        })
        .collect()
}

fn make_transfer_log_u256(
    block_num: i64,
    log_idx: i32,
    from: &str,
    to: &str,
    value_hex: &str,
) -> LogRow {
    use chrono::TimeZone;
    let ts = chrono::Utc.with_ymd_and_hms(2025, 1, 15, 12, 0, 0).unwrap()
        + chrono::Duration::seconds(block_num);
    let topic0 =
        hex::decode("ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef").unwrap();
    let data = hex::decode(value_hex).expect("value_hex must be valid hex");
    assert_eq!(data.len(), 32, "value_hex must encode 32 bytes");

    LogRow {
        block_num,
        block_timestamp: ts,
        log_idx,
        tx_idx: 0,
        tx_hash: vec![block_num as u8; 32],
        address: vec![0xda; 20],
        selector: Some(topic0.clone()),
        topic0: Some(topic0),
        topic1: Some(padded_topic_address(from)),
        topic2: Some(padded_topic_address(to)),
        topic3: None,
        data,
        is_virtual_forward: false,
    }
}

#[tokio::test]
#[serial(clickhouse)]
async fn test_sink_token_balances_handles_amount_above_int256_max() {
    let Some((sink, ch)) = setup_sink().await else {
        return;
    };

    let zero = "0x0000000000000000000000000000000000000000";
    let alice = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let bob = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    let a_hex = "8000000000000000000000000000000000000000000000000000000000000064";
    let c_hex = "8000000000000000000000000000000000000000000000000000000000000028";

    let logs = vec![
        make_transfer_log_u256(1, 0, zero, alice, a_hex),
        make_transfer_log_u256(2, 0, alice, bob, c_hex),
    ];
    sink.write_logs(&logs).await.expect("write_logs failed");

    let delta_type = ch
        .query(&format!(
            "SELECT type FROM system.columns WHERE database = '{SINK_DB}' \
             AND table = 'token_holder_deltas' AND name = 'balance_delta'"
        ))
        .await
        .expect("column type query failed");
    assert_eq!(delta_type.trim(), "UInt256");

    let balances = token_holder_balances(&ch).await;
    assert_eq!(balances.get(alice).map(String::as_str), Some("60"));
    assert_eq!(
        balances.get(bob).map(String::as_str),
        Some("57896044618658097711785492504343953926634992332820282019728792003956564820008")
    );
    assert!(!balances.contains_key(zero));
}

#[tokio::test]
#[serial(clickhouse)]
async fn test_post_derived_migrations_repair_legacy_signed_holder_deltas() {
    let ch = TestClickHouse::new(SINK_DB)
        .await
        .expect("Failed to create CH client");
    if ch.wait_for_ready().await.is_err() {
        println!("ClickHouse not available, skipping test");
        return;
    }
    ch.reset_database().await.expect("Failed to reset database");

    let zero = "0x0000000000000000000000000000000000000000";
    let alice = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let bob = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let token = "0xdadadadadadadadadadadadadadadadadadadada";
    let a = "57896044618658097711785492504343953926634992332820282019728792003956564820068";
    let c = "57896044618658097711785492504343953926634992332820282019728792003956564820008";

    ch.query(include_str!("../db/clickhouse/token_transfers.sql"))
        .await
        .expect("create token_transfers");
    ch.query(&format!(
        "INSERT INTO token_transfers \
         (block_num, block_timestamp, tx_idx, log_idx, tx_hash, token, `from`, `to`, amount, is_virtual_forward) VALUES \
         (1, '2025-01-15 12:00:01', 0, 0, 'a1', '{token}', '{zero}',  '{alice}', {a}, 0), \
         (2, '2025-01-15 12:00:02', 0, 0, 'a2', '{token}', '{alice}', '{bob}',   {c}, 0)"
    ))
    .await
    .expect("seed token_transfers");

    ch.query(
        "CREATE TABLE token_holder_deltas (
            block_num Int64, block_timestamp DateTime64(3, 'UTC'), tx_hash String, log_idx Int32,
            token String, holder String, leg Int8, balance_delta Int256
        ) ENGINE = ReplacingMergeTree()
        PARTITION BY toYYYYMM(block_timestamp)
        ORDER BY (token, holder, block_num, tx_hash, log_idx, leg)",
    )
    .await
    .expect("create legacy token_holder_deltas");

    ch.query(
        "INSERT INTO token_holder_deltas
         SELECT block_num, block_timestamp, tx_hash, log_idx, token,
             tupleElement(leg_tuple, 1) AS holder,
             tupleElement(leg_tuple, 2) AS leg,
             tupleElement(leg_tuple, 3) AS balance_delta
         FROM token_transfers
         ARRAY JOIN [
             (`to`,   CAST(1 AS Int8),  CAST(amount AS Int256)),
             (`from`, CAST(-1 AS Int8), -CAST(amount AS Int256))
         ] AS leg_tuple
         WHERE tupleElement(leg_tuple, 1) != '0x0000000000000000000000000000000000000000'",
    )
    .await
    .expect("seed legacy deltas");

    let legacy_bob = ch
        .query(&format!(
            "SELECT toString(sum(balance_delta)) FROM token_holder_deltas FINAL WHERE holder = '{bob}'"
        ))
        .await
        .expect("legacy balance query");
    assert!(
        legacy_bob.trim().starts_with('-'),
        "legacy signed delta for bob should wrap negative, got {}",
        legacy_bob.trim()
    );

    for name in [
        "token_holder_deltas_20260704_fix_signed",
        "token_holder_deltas_20260704_widen_uint256",
        "token_holder_deltas_20260704_reinsert_overflow",
    ] {
        let migration = post_derived_migrations()
            .iter()
            .find(|object| object.name == name)
            .unwrap_or_else(|| panic!("migration {name} should exist"));
        ch.query(&migration.ddl())
            .await
            .unwrap_or_else(|e| panic!("migration {name} failed: {e}"));
    }

    let delta_type = ch
        .query(&format!(
            "SELECT type FROM system.columns WHERE database = '{SINK_DB}' \
             AND table = 'token_holder_deltas' AND name = 'balance_delta'"
        ))
        .await
        .expect("column type query failed");
    assert_eq!(delta_type.trim(), "UInt256");

    ch.query(include_str!("../db/clickhouse/token_balances.sql"))
        .await
        .expect("create token_balances view");

    let balances = token_holder_balances(&ch).await;
    assert_eq!(balances.get(alice).map(String::as_str), Some("60"));
    assert_eq!(balances.get(bob).map(String::as_str), Some(c));
    assert!(!balances.contains_key(zero));
}

async fn token_holder_balances(ch: &TestClickHouse) -> std::collections::HashMap<String, String> {
    let result = ch
        .query_json(
            "SELECT holder, toString(balance) AS balance FROM token_balances ORDER BY holder",
        )
        .await
        .expect("token_balances query failed");
    result["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| {
            (
                row["holder"].as_str().unwrap().to_string(),
                row["balance"].as_str().unwrap().to_string(),
            )
        })
        .collect()
}

async fn refresh_balance_snapshots(ch: &TestClickHouse) {
    for view in [
        "token_balances_snapshot",
        "address_balances_snapshot",
        "token_holder_counts",
    ] {
        ch.query(&format!("SYSTEM REFRESH VIEW {view}"))
            .await
            .unwrap_or_else(|_| panic!("failed to refresh {view}"));
        ch.query(&format!("SYSTEM WAIT VIEW {view}"))
            .await
            .unwrap_or_else(|_| panic!("failed to wait for {view}"));
    }
}

async fn balance_rows(ch: &TestClickHouse, table: &str) -> Vec<(String, String, String)> {
    let result = ch
        .query_json(&format!(
            "SELECT token, holder, toString(balance) AS balance \
             FROM {table} ORDER BY token, holder"
        ))
        .await
        .unwrap_or_else(|_| panic!("failed to query {table}"));
    result["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| {
            (
                row["token"].as_str().unwrap().to_string(),
                row["holder"].as_str().unwrap().to_string(),
                row["balance"].as_str().unwrap().to_string(),
            )
        })
        .collect()
}

#[tokio::test]
#[serial(clickhouse)]
async fn test_sink_write_receipts() {
    let Some((sink, ch)) = setup_sink().await else {
        return;
    };

    let receipts: Vec<ReceiptRow> = (1..=3).map(|b| make_receipt(b, 0)).collect();
    sink.write_receipts(&receipts)
        .await
        .expect("write_receipts failed");

    let count = ch.table_count("receipts").await.expect("count failed");
    assert_eq!(count, 3);
}

#[tokio::test]
#[serial(clickhouse)]
async fn test_sink_write_empty_batches() {
    let Some((sink, _ch)) = setup_sink().await else {
        return;
    };

    // Empty writes should succeed without errors
    sink.write_blocks(&[]).await.expect("empty blocks failed");
    sink.write_txs(&[]).await.expect("empty txs failed");
    sink.write_logs(&[]).await.expect("empty logs failed");
    sink.write_receipts(&[])
        .await
        .expect("empty receipts failed");
}

#[tokio::test]
#[serial(clickhouse)]
async fn test_sink_delete_from_reorg() {
    let Some((sink, ch)) = setup_sink().await else {
        return;
    };

    // Write blocks 1-10
    let blocks: Vec<BlockRow> = (1..=10).map(make_block).collect();
    let txs: Vec<TxRow> = (1..=10).map(|b| make_tx(b, 0)).collect();
    let logs: Vec<LogRow> = (1..=10).map(|b| make_log(b, 0)).collect();
    let receipts: Vec<ReceiptRow> = (1..=10).map(|b| make_receipt(b, 0)).collect();

    sink.write_blocks(&blocks).await.unwrap();
    sink.write_txs(&txs).await.unwrap();
    sink.write_logs(&logs).await.unwrap();
    sink.write_receipts(&receipts).await.unwrap();

    assert_eq!(ch.table_count("blocks").await.unwrap(), 10);

    // Simulate reorg: delete from block 8 onwards
    // mutations_sync=1 means this returns only after the mutation completes — no sleep needed
    sink.delete_from(8).await.expect("delete_from failed");

    assert_eq!(ch.table_count("blocks").await.unwrap(), 7);
    assert_eq!(ch.table_count("txs").await.unwrap(), 7);
    assert_eq!(ch.table_count("logs").await.unwrap(), 7);
    assert_eq!(ch.table_count("receipts").await.unwrap(), 7);
}

/// Reorg correctness: write → delete → re-insert different data → verify new data wins.
#[tokio::test]
#[serial(clickhouse)]
async fn test_sink_reorg_reinsert_correctness() {
    let Some((sink, ch)) = setup_sink().await else {
        return;
    };

    // Write blocks 1-10 with gas_used = 21000 * num
    let blocks: Vec<BlockRow> = (1..=10).map(make_block).collect();
    let txs: Vec<TxRow> = (1..=10).map(|b| make_tx(b, 0)).collect();
    sink.write_blocks(&blocks).await.unwrap();
    sink.write_txs(&txs).await.unwrap();

    // Verify original block 9 gas_used
    let result = ch
        .query_json("SELECT gas_used FROM blocks WHERE num = 9")
        .await
        .unwrap();
    let original_gas = result["data"].as_array().unwrap()[0]["gas_used"]
        .as_i64()
        .or_else(|| {
            result["data"].as_array().unwrap()[0]["gas_used"]
                .as_str()
                .and_then(|s| s.parse().ok())
        })
        .unwrap();
    assert_eq!(original_gas, 21_000 * 9);

    // Simulate reorg: delete from block 8 onwards (synchronous mutation)
    sink.delete_from(8).await.unwrap();
    assert_eq!(ch.table_count("blocks").await.unwrap(), 7);

    // Re-insert blocks 8-10 with DIFFERENT data (gas_used = 99999)
    let reorged_blocks: Vec<BlockRow> = (8..=10)
        .map(|num| {
            let mut b = make_block(num);
            b.gas_used = 99999;
            b.hash = vec![0xff; 32]; // different hash
            b
        })
        .collect();
    let reorged_txs: Vec<TxRow> = (8..=10)
        .map(|b| {
            let mut tx = make_tx(b, 0);
            tx.value = "9999".to_string(); // different value
            tx
        })
        .collect();
    sink.write_blocks(&reorged_blocks).await.unwrap();
    sink.write_txs(&reorged_txs).await.unwrap();

    // Verify reorged data
    assert_eq!(ch.table_count("blocks").await.unwrap(), 10);

    let result = ch
        .query_json("SELECT gas_used, hash FROM blocks WHERE num = 9")
        .await
        .unwrap();
    let row = &result["data"].as_array().unwrap()[0];
    let new_gas = row["gas_used"]
        .as_i64()
        .or_else(|| row["gas_used"].as_str().and_then(|s| s.parse().ok()))
        .unwrap();
    assert_eq!(new_gas, 99999, "gas_used should reflect reorged block data");

    let hash = row["hash"].as_str().unwrap();
    assert!(hash.starts_with("0x"), "hash should be 0x-prefixed");
    assert!(
        hash.contains("ffffffff"),
        "hash should be the reorged hash (0xff bytes)"
    );

    // Verify txs also reflect reorged data
    let result = ch
        .query_json("SELECT value FROM txs WHERE block_num = 9")
        .await
        .unwrap();
    let value = result["data"].as_array().unwrap()[0]["value"]
        .as_str()
        .unwrap();
    assert_eq!(value, "9999", "tx value should reflect reorged data");

    // Verify blocks before fork point are untouched
    let result = ch
        .query_json("SELECT gas_used FROM blocks WHERE num = 7")
        .await
        .unwrap();
    let preserved_gas = result["data"].as_array().unwrap()[0]["gas_used"]
        .as_i64()
        .or_else(|| {
            result["data"].as_array().unwrap()[0]["gas_used"]
                .as_str()
                .and_then(|s| s.parse().ok())
        })
        .unwrap();
    assert_eq!(
        preserved_gas,
        21_000 * 7,
        "blocks before fork should be preserved"
    );
}

#[tokio::test]
#[serial(clickhouse)]
async fn test_sink_hex_encoding() {
    let Some((sink, ch)) = setup_sink().await else {
        return;
    };

    let block = make_block(1);
    sink.write_blocks(&[block]).await.unwrap();

    let result = ch
        .query_json("SELECT hash, miner FROM blocks WHERE num = 1")
        .await
        .unwrap();
    let row = &result["data"].as_array().unwrap()[0];

    let hash = row["hash"].as_str().unwrap();
    let miner = row["miner"].as_str().unwrap();

    // Should be 0x-prefixed hex
    assert!(
        hash.starts_with("0x"),
        "hash should be 0x-prefixed, got: {hash}"
    );
    assert!(
        miner.starts_with("0x"),
        "miner should be 0x-prefixed, got: {miner}"
    );
    assert_eq!(
        hash.len(),
        66,
        "hash should be 32 bytes = 66 hex chars with 0x prefix"
    );
    assert_eq!(
        miner.len(),
        42,
        "miner should be 20 bytes = 42 hex chars with 0x prefix"
    );
}

#[tokio::test]
#[serial(clickhouse)]
async fn test_sink_nullable_fields() {
    let Some((sink, ch)) = setup_sink().await else {
        return;
    };

    // Block with extra_data = None
    let mut block = make_block(1);
    block.extra_data = None;
    sink.write_blocks(&[block]).await.unwrap();

    let result = ch
        .query_json("SELECT extra_data FROM blocks WHERE num = 1")
        .await
        .unwrap();
    let row = &result["data"].as_array().unwrap()[0];
    assert!(row["extra_data"].is_null(), "extra_data should be null");

    // Tx with to = None (contract creation)
    let mut tx = make_tx(1, 0);
    tx.to = None;
    sink.write_txs(&[tx]).await.unwrap();

    let result = ch
        .query_json("SELECT `to` FROM txs WHERE block_num = 1")
        .await
        .unwrap();
    let row = &result["data"].as_array().unwrap()[0];
    assert!(
        row["to"].is_null(),
        "to should be null for contract creation"
    );
}

#[tokio::test]
#[serial(clickhouse)]
async fn test_sink_idempotent_schema() {
    let Some((sink, _ch)) = setup_sink().await else {
        return;
    };

    // Calling ensure_schema twice should not error
    sink.ensure_schema()
        .await
        .expect("second ensure_schema failed");
}

// ============================================================================
// End-to-end: CTE queries against ClickHouseSink-written data
// ============================================================================

/// Write Transfer event logs via ClickHouseSink, then query them with CTE decoding.
/// This verifies the full pipeline: sink writes 0x-prefixed data → CTE decodes it correctly.
#[tokio::test]
#[serial(clickhouse)]
async fn test_cte_query_against_sink_data() {
    let Some((sink, ch)) = setup_sink().await else {
        return;
    };

    // Construct a Transfer(address indexed from, address indexed to, uint256 value) log
    // topic0 = keccak256("Transfer(address,address,uint256)")
    let topic0 =
        hex::decode("ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef").unwrap();
    let from_addr =
        hex::decode("000000000000000000000000a975ba910c2ee169956f3df99ee2ece79d3887cf").unwrap();
    let to_addr =
        hex::decode("000000000000000000000000dead000000000000000000000000000000000000").unwrap();
    // value = 1 ETH = 10^18 = 0xde0b6b3a7640000
    let value_data =
        hex::decode("0000000000000000000000000000000000000000000000000de0b6b3a7640000").unwrap();

    let log = LogRow {
        block_num: 100,
        block_timestamp: chrono::Utc::now(),
        log_idx: 0,
        tx_idx: 0,
        tx_hash: vec![0x01; 32],
        address: vec![
            0xda, 0xc1, 0x7f, 0x95, 0x8d, 0x2e, 0xe5, 0x23, 0xa2, 0x20, 0x62, 0x06, 0x99, 0x45,
            0x97, 0xc1, 0x3d, 0x83, 0x1e, 0xc7,
        ],
        selector: Some(topic0.clone()), // full 32-byte topic0 (same as decoder produces)
        topic0: Some(topic0),
        topic1: Some(from_addr),
        topic2: Some(to_addr),
        topic3: None,
        data: value_data,
        is_virtual_forward: false,
    };

    sink.write_logs(&[log]).await.expect("write_logs failed");

    // Now query via CTE
    let sig = tidx::query::EventSignature::parse(
        "Transfer(address indexed from, address indexed to, uint256 value)",
    )
    .unwrap();

    let cte = sig.to_cte_sql_clickhouse();
    let sql = format!(r#"WITH {} SELECT "from", "to", value FROM Transfer"#, cte);

    let result = ch.query_json(&sql).await.expect("CTE query failed");
    let data = result["data"].as_array().expect("no data array");
    assert_eq!(data.len(), 1, "expected 1 Transfer event");

    let row = &data[0];
    let from = row["from"].as_str().unwrap();
    let to = row["to"].as_str().unwrap();
    assert_eq!(from, "0xa975ba910c2ee169956f3df99ee2ece79d3887cf");
    assert_eq!(to, "0xdead000000000000000000000000000000000000");
}

/// Write logs via sink, then query with predicate pushdown on indexed param.
#[tokio::test]
#[serial(clickhouse)]
async fn test_predicate_pushdown_against_sink_data() {
    let Some((sink, ch)) = setup_sink().await else {
        return;
    };

    let topic0 =
        hex::decode("ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef").unwrap();
    let addr_a =
        hex::decode("000000000000000000000000aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
    let addr_b =
        hex::decode("000000000000000000000000bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").unwrap();
    let to_addr =
        hex::decode("000000000000000000000000dead000000000000000000000000000000000000").unwrap();
    let value_data =
        hex::decode("0000000000000000000000000000000000000000000000000de0b6b3a7640000").unwrap();

    let contract = vec![0xcc; 20];

    // 5 logs from addr_a
    let mut logs = Vec::new();
    for i in 0..5 {
        logs.push(LogRow {
            block_num: i + 1,
            block_timestamp: chrono::Utc::now(),
            log_idx: 0,
            tx_idx: 0,
            tx_hash: vec![i as u8; 32],
            address: contract.clone(),
            selector: Some(topic0.clone()),
            topic0: Some(topic0.clone()),
            topic1: Some(addr_a.clone()),
            topic2: Some(to_addr.clone()),
            topic3: None,
            data: value_data.clone(),
            is_virtual_forward: false,
        });
    }
    // 3 logs from addr_b
    for i in 0..3 {
        logs.push(LogRow {
            block_num: i + 10,
            block_timestamp: chrono::Utc::now(),
            log_idx: 0,
            tx_idx: 0,
            tx_hash: vec![(i + 10) as u8; 32],
            address: contract.clone(),
            selector: Some(topic0.clone()),
            topic0: Some(topic0.clone()),
            topic1: Some(addr_b.clone()),
            topic2: Some(to_addr.clone()),
            topic3: None,
            data: value_data.clone(),
            is_virtual_forward: false,
        });
    }

    sink.write_logs(&logs).await.unwrap();
    assert_eq!(ch.table_count("logs").await.unwrap(), 8);

    let sig = tidx::query::EventSignature::parse(
        "Transfer(address indexed from, address indexed to, uint256 value)",
    )
    .unwrap();

    // Query with filter on indexed "from" param — should use predicate pushdown
    let user_sql = r#"SELECT COUNT(*) as cnt FROM Transfer WHERE "from" = '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'"#;
    let normalized = sig.normalize_table_references(user_sql);
    let pushed = sig.rewrite_filters_for_pushdown(&normalized);
    let cte = sig.to_cte_sql_clickhouse();
    let full_sql = format!("WITH {} {}", cte, pushed);

    // Verify pushdown is in the SQL
    assert!(
        full_sql.contains("topic1 ="),
        "expected topic1 predicate pushdown"
    );

    let result = ch
        .query_json(&full_sql)
        .await
        .expect("pushdown query failed");
    let cnt = result["data"].as_array().unwrap()[0]["cnt"]
        .as_u64()
        .unwrap();
    assert_eq!(cnt, 5, "expected 5 transfers from addr_a");

    let token_sql = apply_event_signature_ctes_clickhouse(
        r#"SELECT "from", "to", address, value, tx_hash, block_num, log_idx, block_timestamp
FROM Transfer
WHERE address = '0xcccccccccccccccccccccccccccccccccccccccc'
ORDER BY block_num DESC, log_idx DESC
LIMIT 6"#,
        &["Transfer(address indexed from, address indexed to, uint256 value)"],
    )
    .unwrap();
    assert!(token_sql.contains("logs.address = '0xcccccccccccccccccccccccccccccccccccccccc'"));

    let result = ch
        .query_json(&token_sql)
        .await
        .expect("token-scoped transfer query failed");
    let rows = result["data"].as_array().unwrap();
    assert_eq!(rows.len(), 6);
    assert_eq!(rows[0]["block_num"].as_i64(), Some(12));
    assert_eq!(rows[5]["block_num"].as_i64(), Some(3));
}

/// Verify hex literal filter works with sink-written 0x data (no conversion needed).
#[tokio::test]
#[serial(clickhouse)]
async fn test_hex_filter_against_sink_data() {
    let Some((sink, ch)) = setup_sink().await else {
        return;
    };

    let blocks: Vec<BlockRow> = (1..=3).map(make_block).collect();
    sink.write_blocks(&blocks).await.unwrap();

    // Query with 0x-prefixed hex filter — should match directly (no conversion)
    let miner_hex = format!("0x{}", hex::encode(vec![0xaa; 20]));
    let sql = format!(
        "SELECT num FROM blocks WHERE miner = '{}' ORDER BY num",
        miner_hex
    );
    let result = ch.query_json(&sql).await.expect("hex filter query failed");
    let data = result["data"].as_array().unwrap();
    assert_eq!(data.len(), 3, "all 3 blocks should match the miner filter");
}

// ============================================================================
// PG→CH Automatic Backfill Tests
// ============================================================================

/// Helper: set up both PG (TestDb) and CH (ClickHouseSink) for backfill tests.
/// Returns (PG pool, SinkSet with CH, ClickHouseSink, TestClickHouse) or None
/// if infrastructure is unavailable.
async fn setup_backfill() -> Option<(tidx::db::Pool, SinkSet, ClickHouseSink, TestClickHouse)> {
    // Set up CH
    let ch = TestClickHouse::new(SINK_DB)
        .await
        .expect("Failed to create CH client");
    if ch.wait_for_ready().await.is_err() {
        println!("ClickHouse not available, skipping test");
        return None;
    }
    ch.reset_database()
        .await
        .expect("Failed to reset CH database");

    let ch_sink =
        ClickHouseSink::new(&ch.url, SINK_DB, None, None).expect("Failed to create CH sink");
    ch_sink
        .ensure_schema()
        .await
        .expect("Failed to ensure CH schema");

    // Set up PG
    let pg_url = match std::env::var("DATABASE_URL") {
        Ok(url) => url,
        Err(_) => {
            println!("DATABASE_URL not set, skipping backfill test");
            return None;
        }
    };
    let pool = tidx::db::create_pool(&pg_url)
        .await
        .expect("Failed to create PG pool");
    tidx::db::run_migrations(&pool)
        .await
        .expect("Failed to run PG migrations");

    // Truncate PG tables for clean state
    let conn = pool.get().await.expect("Failed to get PG connection");
    conn.batch_execute("TRUNCATE blocks, txs, logs, receipts, sync_state CASCADE")
        .await
        .expect("Failed to truncate PG tables");

    // Tests below write via `writer::` directly (bypassing SinkSet partition
    // provisioning); pre-create partitions covering the fixed test timestamps.
    {
        use chrono::TimeZone;
        let ts0 = chrono::Utc.with_ymd_and_hms(2025, 1, 15, 0, 0, 0).unwrap();
        tidx::db::partitions::ensure_partitions_covering(
            &pool,
            ts0,
            ts0 + chrono::Duration::days(7),
        )
        .await
        .expect("Failed to ensure partitions");
    }

    let sinks = SinkSet::new(pool.clone()).with_clickhouse(ch_sink.clone());
    Some((pool, sinks, ch_sink, ch))
}

async fn set_ch_backfill_cursor(pool: &tidx::db::Pool, chain_id: u64, block: i64) {
    let conn = pool.get().await.expect("Failed to get PG connection");
    conn.execute(
        r#"
        INSERT INTO sync_state (chain_id, ch_backfill_block)
        VALUES ($1, $2)
        ON CONFLICT (chain_id) DO UPDATE SET
            ch_backfill_block = EXCLUDED.ch_backfill_block,
            updated_at = NOW()
        "#,
        &[&(chain_id as i64), &block],
    )
    .await
    .expect("Failed to set CH backfill cursor");
}

/// PostgreSQL hot-tier hydration reads canonical base rows from ClickHouse,
/// preserves encoded fields, and refuses a range that its archive cannot fill.
#[tokio::test]
#[serial(clickhouse)]
async fn test_hydrate_postgres_from_clickhouse() {
    let Some((pool, sinks, ch_sink, _ch)) = setup_backfill().await else {
        return;
    };

    let blocks: Vec<BlockRow> = (1..=5).map(make_block).collect();
    let mut txs: Vec<TxRow> = (1..=5)
        .flat_map(|b| (0..2).map(move |i| make_tx(b, i)))
        .collect();
    txs.iter_mut()
        .find(|tx| tx.block_num == 2 && tx.idx == 0)
        .unwrap()
        .calls = Some(serde_json::json!([{"to": "0x1234"}]));
    let mut logs: Vec<LogRow> = (1..=5).map(|b| make_log(b, 0)).collect();
    logs.iter_mut()
        .find(|log| log.block_num == 3)
        .unwrap()
        .is_virtual_forward = true;
    let receipts: Vec<ReceiptRow> = (1..=5).map(|b| make_receipt(b, 0)).collect();

    tokio::try_join!(
        ch_sink.write_blocks(&blocks),
        ch_sink.write_txs(&txs),
        ch_sink.write_logs(&logs),
        ch_sink.write_receipts(&receipts),
    )
    .expect("failed to seed ClickHouse archive");
    // Crash replay can leave identical ReplacingMergeTree rows. The archive
    // reader must collapse them before checking completeness and hydrating PG.
    ch_sink
        .write_blocks(&blocks)
        .await
        .expect("failed to seed replayed ClickHouse blocks");

    let err = sinks
        .hydrate_postgres_from_clickhouse(1, 6)
        .await
        .expect_err("incomplete ClickHouse range must be rejected");
    assert!(err.to_string().contains("returned 5 of 6 blocks"));

    sinks
        .hydrate_postgres_from_clickhouse(2, 4)
        .await
        .expect("ClickHouse to PostgreSQL hydration failed");

    let conn = pool.get().await.unwrap();
    let counts = conn
        .query_one(
            "SELECT (SELECT count(*) FROM blocks), (SELECT count(*) FROM txs), \
             (SELECT count(*) FROM logs), (SELECT count(*) FROM receipts)",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(counts.get::<_, i64>(0), 3);
    assert_eq!(counts.get::<_, i64>(1), 6);
    assert_eq!(counts.get::<_, i64>(2), 3);
    assert_eq!(counts.get::<_, i64>(3), 3);

    let block = conn
        .query_one("SELECT hash FROM blocks WHERE num = 2", &[])
        .await
        .unwrap();
    assert_eq!(block.get::<_, Vec<u8>>(0), vec![2; 32]);

    let tx = conn
        .query_one("SELECT calls FROM txs WHERE block_num = 2 AND idx = 0", &[])
        .await
        .unwrap();
    assert_eq!(
        tx.get::<_, Option<serde_json::Value>>(0),
        Some(serde_json::json!([{"to": "0x1234"}]))
    );

    let log = conn
        .query_one(
            "SELECT is_virtual_forward FROM logs WHERE block_num = 3",
            &[],
        )
        .await
        .unwrap();
    assert!(log.get::<_, bool>(0));
}

/// Backfill should copy all blocks/txs/logs/receipts from PG to an empty CH.
#[tokio::test]
#[serial(clickhouse)]
async fn test_backfill_pg_to_empty_clickhouse() {
    let Some((pool, sinks, _ch_sink, ch)) = setup_backfill().await else {
        return;
    };

    // Write data directly to PG only (bypass SinkSet to simulate pre-existing PG data)
    let blocks: Vec<BlockRow> = (1..=10).map(make_block).collect();
    let txs: Vec<TxRow> = (1..=10)
        .flat_map(|b| (0..2).map(move |i| make_tx(b, i)))
        .collect();
    let logs: Vec<LogRow> = (1..=10)
        .flat_map(|b| (0..3).map(move |i| make_log(b, i)))
        .collect();
    let receipts: Vec<ReceiptRow> = (1..=10)
        .flat_map(|b| (0..2).map(move |i| make_receipt(b, i)))
        .collect();

    writer::write_blocks(&pool, &blocks)
        .await
        .expect("PG write_blocks failed");
    writer::write_txs(&pool, &txs)
        .await
        .expect("PG write_txs failed");
    writer::write_logs(&pool, &logs)
        .await
        .expect("PG write_logs failed");
    writer::write_receipts(&pool, &receipts)
        .await
        .expect("PG write_receipts failed");

    // Verify PG has data, CH is empty
    assert_eq!(ch.table_count("blocks").await.unwrap(), 0);

    // Run backfill
    sinks
        .backfill_clickhouse(TEST_CHAIN_ID)
        .await
        .expect("backfill failed");

    // Verify CH now has matching data
    assert_eq!(ch.table_count("blocks").await.unwrap(), 10);
    assert_eq!(ch.table_count("txs").await.unwrap(), 20);
    assert_eq!(ch.table_count("logs").await.unwrap(), 30);
    assert_eq!(ch.table_count("receipts").await.unwrap(), 20);
}

#[tokio::test]
#[serial(clickhouse)]
async fn test_backfill_pg_to_clickhouse_preserves_virtual_forward_flag() {
    let Some((pool, sinks, _ch_sink, ch)) = setup_backfill().await else {
        return;
    };

    let blocks: Vec<BlockRow> = (1..=2).map(make_block).collect();
    let txs: Vec<TxRow> = (1..=2).map(|b| make_tx(b, 0)).collect();
    let mut logs: Vec<LogRow> = (1..=2).map(|b| make_log(b, 0)).collect();
    logs[1].is_virtual_forward = true;
    let receipts: Vec<ReceiptRow> = (1..=2).map(|b| make_receipt(b, 0)).collect();

    writer::write_blocks(&pool, &blocks).await.unwrap();
    writer::write_txs(&pool, &txs).await.unwrap();
    writer::write_logs(&pool, &logs).await.unwrap();
    writer::write_receipts(&pool, &receipts).await.unwrap();

    sinks.backfill_clickhouse(TEST_CHAIN_ID).await.unwrap();

    let result = ch
        .query_json("SELECT block_num, is_virtual_forward FROM logs ORDER BY block_num")
        .await
        .unwrap();
    let rows = result["data"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["is_virtual_forward"].as_u64(), Some(0));
    assert_eq!(rows[1]["is_virtual_forward"].as_u64(), Some(1));
}

/// A restarted backfill (stale cursor) must not duplicate blocks ClickHouse
/// already holds: each table's rows are filtered by that table's own
/// presence before each batch write.
#[tokio::test]
#[serial(clickhouse)]
async fn test_backfill_restart_does_not_duplicate_present_blocks() {
    let Some((pool, sinks, _ch_sink, ch)) = setup_backfill().await else {
        return;
    };

    // Two backfill batches: BACKFILL_BLOCK_BATCH blocks plus one.
    let blocks: Vec<BlockRow> = (1..=5001).map(make_block).collect();
    writer::write_blocks(&pool, &blocks)
        .await
        .expect("PG write_blocks failed");

    sinks
        .backfill_clickhouse(TEST_CHAIN_ID)
        .await
        .expect("backfill failed");
    assert_eq!(ch.table_count("blocks").await.unwrap(), 5001);

    // Block merges so surviving duplicates stay visible, then rewind the
    // cursor to simulate a restart with stale state.
    ch.query_raw(&format!("SYSTEM STOP MERGES {SINK_DB}.blocks"))
        .await
        .expect("stop merges failed");
    let conn = pool.get().await.unwrap();
    conn.execute(
        "UPDATE sync_state SET ch_backfill_block = 0 WHERE chain_id = $1",
        &[&(TEST_CHAIN_ID as i64)],
    )
    .await
    .expect("cursor rewind failed");
    drop(conn);

    sinks
        .backfill_clickhouse(TEST_CHAIN_ID)
        .await
        .expect("backfill rerun failed");
    assert_eq!(ch.table_count("blocks").await.unwrap(), 5001);
}

/// Backfill should resume from the persisted PG cursor.
#[tokio::test]
#[serial(clickhouse)]
async fn test_backfill_resumes_from_highwater_mark() {
    let Some((pool, sinks, _ch_sink, ch)) = setup_backfill().await else {
        return;
    };

    // Write 20 blocks to PG
    let blocks: Vec<BlockRow> = (1..=20).map(make_block).collect();
    let txs: Vec<TxRow> = (1..=20)
        .flat_map(|b| (0..1).map(move |i| make_tx(b, i)))
        .collect();

    writer::write_blocks(&pool, &blocks)
        .await
        .expect("PG write_blocks failed");
    writer::write_txs(&pool, &txs)
        .await
        .expect("PG write_txs failed");

    // Manually write first 10 blocks to CH (simulating a partial backfill)
    let ch_sink = ClickHouseSink::new(&ch.url, SINK_DB, None, None).unwrap();
    let first_10: Vec<BlockRow> = (1..=10).map(make_block).collect();
    let first_10_txs: Vec<TxRow> = (1..=10)
        .flat_map(|b| (0..1).map(move |i| make_tx(b, i)))
        .collect();
    ch_sink.write_blocks(&first_10).await.unwrap();
    ch_sink.write_txs(&first_10_txs).await.unwrap();
    set_ch_backfill_cursor(&pool, TEST_CHAIN_ID, 10).await;

    assert_eq!(ch.table_count("blocks").await.unwrap(), 10);

    // Run backfill — should only copy blocks 11-20
    sinks
        .backfill_clickhouse(TEST_CHAIN_ID)
        .await
        .expect("backfill failed");

    assert_eq!(ch.table_count_final("blocks").await.unwrap(), 20);
    assert_eq!(ch.table_count_final("txs").await.unwrap(), 20);
}

/// Backfill should be a no-op when the persisted cursor is already up to date.
#[tokio::test]
#[serial(clickhouse)]
async fn test_backfill_noop_when_up_to_date() {
    let Some((pool, sinks, _ch_sink, ch)) = setup_backfill().await else {
        return;
    };

    // Write 5 blocks to both PG and CH via SinkSet (normal dual-write path)
    let blocks: Vec<BlockRow> = (1..=5).map(make_block).collect();
    writer::write_blocks(&pool, &blocks)
        .await
        .expect("PG write failed");

    let ch_sink = ClickHouseSink::new(&ch.url, SINK_DB, None, None).unwrap();
    ch_sink.write_blocks(&blocks).await.unwrap();
    set_ch_backfill_cursor(&pool, TEST_CHAIN_ID, 5).await;

    assert_eq!(ch.table_count("blocks").await.unwrap(), 5);

    // Run backfill — cursor should make this a no-op
    sinks
        .backfill_clickhouse(TEST_CHAIN_ID)
        .await
        .expect("backfill failed");

    // Count should remain 5 (no duplicates)
    assert_eq!(ch.table_count("blocks").await.unwrap(), 5);
}

/// Backfill should be a no-op when PG is empty.
#[tokio::test]
#[serial(clickhouse)]
async fn test_backfill_noop_when_pg_empty() {
    let Some((_pool, sinks, _ch_sink, ch)) = setup_backfill().await else {
        return;
    };

    // PG is empty (truncated in setup), CH is empty
    sinks
        .backfill_clickhouse(TEST_CHAIN_ID)
        .await
        .expect("backfill failed");

    assert_eq!(ch.table_count("blocks").await.unwrap(), 0);
}

/// If CH is partially populated but the cursor was never advanced, backfill
/// should safely replay the full range and converge after deduplication.
#[tokio::test]
#[serial(clickhouse)]
async fn test_backfill_per_table_independent_highwater() {
    let Some((pool, sinks, _ch_sink, ch)) = setup_backfill().await else {
        return;
    };

    // Write full data to PG
    let blocks: Vec<BlockRow> = (1..=10).map(make_block).collect();
    let txs: Vec<TxRow> = (1..=10)
        .flat_map(|b| (0..2).map(move |i| make_tx(b, i)))
        .collect();
    let logs: Vec<LogRow> = (1..=10)
        .flat_map(|b| (0..3).map(move |i| make_log(b, i)))
        .collect();
    let receipts: Vec<ReceiptRow> = (1..=10)
        .flat_map(|b| (0..2).map(move |i| make_receipt(b, i)))
        .collect();

    writer::write_blocks(&pool, &blocks).await.unwrap();
    writer::write_txs(&pool, &txs).await.unwrap();
    writer::write_logs(&pool, &logs).await.unwrap();
    writer::write_receipts(&pool, &receipts).await.unwrap();

    // Pre-populate CH with ALL blocks but only txs for blocks 1-5 (simulate partial failure)
    let ch_sink = ClickHouseSink::new(&ch.url, SINK_DB, None, None).unwrap();
    ch_sink.write_blocks(&blocks).await.unwrap();
    let partial_txs: Vec<TxRow> = (1..=5)
        .flat_map(|b| (0..2).map(move |i| make_tx(b, i)))
        .collect();
    ch_sink.write_txs(&partial_txs).await.unwrap();
    // logs and receipts: nothing in CH

    assert_eq!(ch.table_count_final("blocks").await.unwrap(), 10);
    assert_eq!(ch.table_count_final("txs").await.unwrap(), 10); // only 5 blocks × 2 txs
    assert_eq!(ch.table_count("logs").await.unwrap(), 0);
    assert_eq!(ch.table_count("receipts").await.unwrap(), 0);

    // Run backfill — with a zero cursor, the implementation replays the full
    // range for all tables. ReplacingMergeTree should converge to the correct
    // deduplicated result.
    sinks
        .backfill_clickhouse(TEST_CHAIN_ID)
        .await
        .expect("backfill failed");

    assert_eq!(ch.table_count_final("blocks").await.unwrap(), 10);
    assert_eq!(ch.table_count_final("txs").await.unwrap(), 20);
    assert_eq!(ch.table_count_final("logs").await.unwrap(), 30);
    assert_eq!(ch.table_count_final("receipts").await.unwrap(), 20);
}

/// Backfill with >5000 blocks to exercise multi-batch range pagination.
#[tokio::test]
#[serial(clickhouse)]
async fn test_backfill_multi_batch_pagination() {
    let Some((pool, sinks, _ch_sink, ch)) = setup_backfill().await else {
        return;
    };

    // Write 6000 blocks to PG — forces at least 2 range-pagination batches (batch size = 5000)
    let block_count = 6000i64;
    // Write in chunks to avoid huge single inserts
    for chunk_start in (1..=block_count).step_by(500) {
        let chunk_end = (chunk_start + 499).min(block_count);
        let blocks: Vec<BlockRow> = (chunk_start..=chunk_end).map(make_block).collect();
        writer::write_blocks(&pool, &blocks).await.unwrap();
    }

    assert_eq!(ch.table_count("blocks").await.unwrap(), 0);

    // Run backfill
    sinks
        .backfill_clickhouse(TEST_CHAIN_ID)
        .await
        .expect("backfill failed");

    assert_eq!(
        ch.table_count_final("blocks").await.unwrap(),
        block_count as u64
    );

    // Verify first and last blocks roundtripped
    let result = ch
        .query_json("SELECT min(num), max(num) FROM blocks")
        .await
        .unwrap();
    let row = &result["data"].as_array().unwrap()[0];
    let min_num = row["min(num)"]
        .as_i64()
        .or_else(|| row["min(num)"].as_str().and_then(|s| s.parse().ok()))
        .unwrap();
    let max_num = row["max(num)"]
        .as_i64()
        .or_else(|| row["max(num)"].as_str().and_then(|s| s.parse().ok()))
        .unwrap();
    assert_eq!(min_num, 1);
    assert_eq!(max_num, block_count);
}

/// Backfill is idempotent — running twice should not duplicate data.
#[tokio::test]
#[serial(clickhouse)]
async fn test_backfill_idempotent() {
    let Some((pool, sinks, _ch_sink, ch)) = setup_backfill().await else {
        return;
    };

    let blocks: Vec<BlockRow> = (1..=10).map(make_block).collect();
    let txs: Vec<TxRow> = (1..=10).map(|b| make_tx(b, 0)).collect();
    writer::write_blocks(&pool, &blocks).await.unwrap();
    writer::write_txs(&pool, &txs).await.unwrap();

    // First backfill
    sinks
        .backfill_clickhouse(TEST_CHAIN_ID)
        .await
        .expect("first backfill failed");
    assert_eq!(ch.table_count("blocks").await.unwrap(), 10);
    assert_eq!(ch.table_count("txs").await.unwrap(), 10);

    // Second backfill — should be a no-op (cursor already advanced)
    sinks
        .backfill_clickhouse(TEST_CHAIN_ID)
        .await
        .expect("second backfill failed");
    assert_eq!(ch.table_count("blocks").await.unwrap(), 10);
    assert_eq!(ch.table_count("txs").await.unwrap(), 10);
}
