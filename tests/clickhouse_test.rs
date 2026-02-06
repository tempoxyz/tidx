//! ClickHouse integration tests
//!
//! Tests CTE generation, query execution, and materialized view creation
//! against a local ClickHouse instance.
//!
//! Run with: cargo test --test clickhouse_test
//! Requires: docker compose -f docker/local/docker-compose.yml up -d postgres clickhouse

mod common;

use common::clickhouse::TestClickHouse;
use serial_test::serial;
use tidx::query::{convert_hex_literals_clickhouse, EventSignature};

const TEST_DB: &str = "tidx_test";

// ============================================================================
// Basic ClickHouse Connection Tests
// ============================================================================

#[tokio::test]
#[serial(clickhouse)]
async fn test_clickhouse_connection() {
    let ch = TestClickHouse::new(TEST_DB).await.expect("Failed to create ClickHouse client");
    
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
    let ch = TestClickHouse::new(TEST_DB).await.expect("Failed to create ClickHouse client");
    
    if ch.wait_for_ready().await.is_err() {
        println!("ClickHouse not available, skipping test");
        return;
    }
    
    ch.reset_database().await.expect("Failed to reset database");
    
    // Verify database exists
    let result = ch.query_raw(&format!(
        "SELECT count() FROM system.databases WHERE name = '{}'",
        TEST_DB
    )).await.expect("Query failed");
    
    assert_eq!(result.trim(), "1");
}

// ============================================================================
// CTE SQL Generation Tests (ClickHouse format)
// ============================================================================

#[tokio::test]
#[serial(clickhouse)]
async fn test_transfer_cte_execution() {
    let ch = TestClickHouse::new(TEST_DB).await.expect("Failed to create ClickHouse client");
    
    if ch.wait_for_ready().await.is_err() {
        println!("ClickHouse not available, skipping test");
        return;
    }
    
    ch.reset_database().await.expect("Failed to reset database");
    ch.create_mock_logs_table().await.expect("Failed to create logs table");
    
    // Insert test Transfer logs
    ch.insert_transfer_log(
        1, 0,
        "0xa975ba910c2ee169956f3df99ee2ece79d3887cf",
        "0xdead000000000000000000000000000000000000",
        1000000000000000000u128, // 1 ETH
    ).await.expect("Failed to insert log");
    
    ch.insert_transfer_log(
        2, 0,
        "0xdead000000000000000000000000000000000000",
        "0xa975ba910c2ee169956f3df99ee2ece79d3887cf",
        500000000000000000u128, // 0.5 ETH
    ).await.expect("Failed to insert log");
    
    // Generate CTE and execute query
    let sig = EventSignature::parse(
        "Transfer(address indexed from, address indexed to, uint256 value)"
    ).expect("Failed to parse signature");
    
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
    let ch = TestClickHouse::new(TEST_DB).await.expect("Failed to create ClickHouse client");
    
    if ch.wait_for_ready().await.is_err() {
        println!("ClickHouse not available, skipping test");
        return;
    }
    
    ch.reset_database().await.expect("Failed to reset database");
    ch.create_mock_logs_table().await.expect("Failed to create logs table");
    
    // Insert Approval log with proper topic0
    let approval_topic0 = "\\x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925";
    let owner = "\\x000000000000000000000000a975ba910c2ee169956f3df99ee2ece79d3887cf";
    let spender = "\\x000000000000000000000000dAC17F958D2ee523a2206206994597C13D831ec7";
    let value_data = "\\xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
    
    ch.insert_mock_log(
        10, 0, 0,
        "\\x0000000000000000000000000000000000000000000000000000000000000002",
        "\\xdAC17F958D2ee523a2206206994597C13D831ec7",
        approval_topic0,
        owner,
        spender,
        "\\x0000000000000000000000000000000000000000000000000000000000000000",
        value_data,
    ).await.expect("Failed to insert log");
    
    let sig = EventSignature::parse(
        "Approval(address indexed owner, address indexed spender, uint256 value)"
    ).expect("Failed to parse signature");
    
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
    let ch = TestClickHouse::new(TEST_DB).await.expect("Failed to create ClickHouse client");
    
    if ch.wait_for_ready().await.is_err() {
        println!("ClickHouse not available, skipping test");
        return;
    }
    
    ch.reset_database().await.expect("Failed to reset database");
    ch.create_mock_logs_table().await.expect("Failed to create logs table");
    
    // Insert logs with known address
    ch.insert_transfer_log(
        1, 0,
        "0xa975ba910c2ee169956f3df99ee2ece79d3887cf",
        "0xdead000000000000000000000000000000000000",
        1000000000000000000u128,
    ).await.expect("Failed to insert log");
    
    ch.insert_transfer_log(
        2, 0,
        "0xbeef000000000000000000000000000000000000",
        "0xdead000000000000000000000000000000000000",
        2000000000000000000u128,
    ).await.expect("Failed to insert log");
    
    // Build query with hex filter - test our hex literal conversion
    let sig = EventSignature::parse(
        "Transfer(address indexed from, address indexed to, uint256 value)"
    ).expect("Failed to parse signature");
    
    let user_query = r#"SELECT "from", "to", value FROM Transfer WHERE "from" = '0xa975ba910c2ee169956f3df99ee2ece79d3887cf'"#;
    
    // Apply CTE and predicate pushdown
    let normalized = sig.normalize_table_references(user_query);
    let pushed = sig.rewrite_filters_for_pushdown(&normalized);
    let cte = sig.to_cte_sql_clickhouse();
    let full_sql = format!("WITH {} {}", cte, pushed);
    
    // Convert hex literals to ClickHouse format
    let final_sql = convert_hex_literals_clickhouse(&full_sql);
    
    println!("Final SQL: {}", final_sql);
    
    let result = ch.query_json(&final_sql).await.expect("Query failed");
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
    let ch = TestClickHouse::new(TEST_DB).await.expect("Failed to create ClickHouse client");
    
    if ch.wait_for_ready().await.is_err() {
        println!("ClickHouse not available, skipping test");
        return;
    }
    
    ch.reset_database().await.expect("Failed to reset database");
    ch.create_mock_logs_table().await.expect("Failed to create logs table");
    
    // Insert multiple transfers
    for i in 0..10 {
        ch.insert_transfer_log(
            i + 1, 0,
            "0xa975ba910c2ee169956f3df99ee2ece79d3887cf",
            "0xdead000000000000000000000000000000000000",
            (i as u128 + 1) * 1000000000000000000u128,
        ).await.expect("Failed to insert log");
    }
    
    let sig = EventSignature::parse(
        "Transfer(address indexed from, address indexed to, uint256 value)"
    ).expect("Failed to parse signature");
    
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
    let ch = TestClickHouse::new(TEST_DB).await.expect("Failed to create ClickHouse client");
    
    if ch.wait_for_ready().await.is_err() {
        println!("ClickHouse not available, skipping test");
        return;
    }
    
    ch.reset_database().await.expect("Failed to reset database");
    ch.create_mock_logs_table().await.expect("Failed to create logs table");
    
    // Insert test data first
    for i in 0..5 {
        ch.insert_transfer_log(
            i + 1, 0,
            "0xa975ba910c2ee169956f3df99ee2ece79d3887cf",
            "0xdead000000000000000000000000000000000000",
            1000000000000000000u128,
        ).await.expect("Failed to insert log");
    }
    
    let sig = EventSignature::parse(
        "Transfer(address indexed from, address indexed to, uint256 value)"
    ).expect("Failed to parse signature");
    
    let cte = sig.to_cte_sql_clickhouse();
    
    // Create target table
    let create_table = r#"CREATE TABLE transfer_counts (
            addr String,
            total_sent UInt64,
            total_received UInt64
        ) ENGINE = SummingMergeTree()
        ORDER BY addr"#;
    ch.query(&create_table).await.expect("Failed to create table");
    
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
    ch.query(&create_mv).await.expect("Failed to create materialized view");
    
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
    let result = ch.query_json("SELECT * FROM transfer_counts ORDER BY addr").await
        .expect("Query failed");
    
    let data = result.get("data").and_then(|d| d.as_array());
    assert!(data.is_some());
    assert!(data.unwrap().len() >= 2, "Expected at least 2 addresses (from and to)");
}

#[tokio::test]
#[serial(clickhouse)]
async fn test_create_materialized_view_token_supply() {
    let ch = TestClickHouse::new(TEST_DB).await.expect("Failed to create ClickHouse client");
    
    if ch.wait_for_ready().await.is_err() {
        println!("ClickHouse not available, skipping test");
        return;
    }
    
    ch.reset_database().await.expect("Failed to reset database");
    ch.create_mock_logs_table().await.expect("Failed to create logs table");
    
    // Insert mint (from zero address)
    ch.insert_transfer_log(
        1, 0,
        "0x0000000000000000000000000000000000000000",
        "0xa975ba910c2ee169956f3df99ee2ece79d3887cf",
        1000000000000000000000u128, // 1000 tokens
    ).await.expect("Failed to insert mint");
    
    // Insert burn (to zero address)
    ch.insert_transfer_log(
        2, 0,
        "0xa975ba910c2ee169956f3df99ee2ece79d3887cf",
        "0x0000000000000000000000000000000000000000",
        100000000000000000000u128, // 100 tokens burned
    ).await.expect("Failed to insert burn");
    
    let sig = EventSignature::parse(
        "Transfer(address indexed from, address indexed to, uint256 value)"
    ).expect("Failed to parse signature");
    
    let cte = sig.to_cte_sql_clickhouse();
    
    // Create target table for token supply
    let create_table = r#"CREATE TABLE token_supply (
        token String,
        supply Int256
    ) ENGINE = SummingMergeTree()
    ORDER BY token"#;
    ch.query(create_table).await.expect("Failed to create table");
    
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
    let ch = TestClickHouse::new(TEST_DB).await.expect("Failed to create ClickHouse client");
    
    if ch.wait_for_ready().await.is_err() {
        println!("ClickHouse not available, skipping test");
        return;
    }
    
    ch.reset_database().await.expect("Failed to reset database");
    ch.create_mock_logs_table().await.expect("Failed to create logs table");
    
    // Insert transfers
    for i in 0..3 {
        ch.insert_transfer_log(
            i + 1, 0,
            "0xa975ba910c2ee169956f3df99ee2ece79d3887cf",
            "0xdead000000000000000000000000000000000000",
            1000000000000000000u128,
        ).await.expect("Failed to insert log");
    }
    
    let sig = EventSignature::parse(
        "Transfer(address indexed from, address indexed to, uint256 value)"
    ).expect("Failed to parse signature");
    
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
    let ch = TestClickHouse::new(TEST_DB).await.expect("Failed to create ClickHouse client");
    
    if ch.wait_for_ready().await.is_err() {
        println!("ClickHouse not available, skipping test");
        return;
    }
    
    ch.reset_database().await.expect("Failed to reset database");
    ch.create_mock_logs_table().await.expect("Failed to create logs table");
    
    // Insert Swap event
    // Swap(address indexed sender, uint256 amount0In, uint256 amount1In, uint256 amount0Out, uint256 amount1Out, address indexed to)
    let swap_topic0 = "\\xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822";
    let sender = "\\x0000000000000000000000007a250d5630B4cF539739dF2C5dAcb4c659F2488D";
    let to = "\\x000000000000000000000000a975ba910c2ee169956f3df99ee2ece79d3887cf";
    
    // Data: 4 uint256 values (amount0In, amount1In, amount0Out, amount1Out)
    let data = "\\x0000000000000000000000000000000000000000000000000de0b6b3a76400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001b1ae4d6e2ef500000";
    
    ch.insert_mock_log(
        100, 0, 0,
        "\\x0000000000000000000000000000000000000000000000000000000000000003",
        "\\x0d4a11d5EEaaC28EC3F61d100daF4d40471f1852", // USDT-WETH pair
        swap_topic0,
        sender,
        to,
        "\\x0000000000000000000000000000000000000000000000000000000000000000",
        data,
    ).await.expect("Failed to insert swap");
    
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
    let ch = TestClickHouse::new(TEST_DB).await.expect("Failed to create ClickHouse client");
    
    if ch.wait_for_ready().await.is_err() {
        println!("ClickHouse not available, skipping test");
        return;
    }
    
    ch.reset_database().await.expect("Failed to reset database");
    ch.create_mock_logs_table().await.expect("Failed to create logs table");
    
    // RoleGranted(bytes32 indexed role, address indexed account, address indexed sender)
    let role_granted_topic0 = "\\x2f8788117e7eff1d82e926ec794901d17c78024a50270940304540a733656f0d";
    let role = "\\x0000000000000000000000000000000000000000000000000000000000000000"; // DEFAULT_ADMIN_ROLE
    let account = "\\x000000000000000000000000a975ba910c2ee169956f3df99ee2ece79d3887cf";
    let sender = "\\x0000000000000000000000000000000000000000000000000000000000000000";
    
    ch.insert_mock_log(
        50, 0, 0,
        "\\x0000000000000000000000000000000000000000000000000000000000000004",
        "\\x6B175474E89094C44Da98b954EescdKF63C02B94", // DAI
        role_granted_topic0,
        role,
        account,
        sender,
        "\\x",
    ).await.expect("Failed to insert role granted");
    
    let sig = EventSignature::parse(
        "RoleGranted(bytes32 indexed role, address indexed account, address indexed sender)"
    ).expect("Failed to parse signature");
    
    let cte = sig.to_cte_sql_clickhouse();
    let sql = format!("WITH {} SELECT role, account, sender FROM RoleGranted", cte);
    
    let result = ch.query_json(&sql).await.expect("Query failed");
    let data = result.get("data").and_then(|d| d.as_array());
    
    assert!(data.is_some());
    assert_eq!(data.unwrap().len(), 1);
}

// ============================================================================
// Predicate Pushdown Tests
// ============================================================================

#[tokio::test]
#[serial(clickhouse)]
async fn test_predicate_pushdown_indexed_param() {
    let ch = TestClickHouse::new(TEST_DB).await.expect("Failed to create ClickHouse client");
    
    if ch.wait_for_ready().await.is_err() {
        println!("ClickHouse not available, skipping test");
        return;
    }
    
    ch.reset_database().await.expect("Failed to reset database");
    ch.create_mock_logs_table().await.expect("Failed to create logs table");
    
    // Insert logs from two different addresses
    for i in 0..5 {
        ch.insert_transfer_log(
            i + 1, 0,
            "0xa975ba910c2ee169956f3df99ee2ece79d3887cf",
            "0xdead000000000000000000000000000000000000",
            1000000000000000000u128,
        ).await.expect("Failed to insert log");
    }
    
    for i in 0..3 {
        ch.insert_transfer_log(
            i + 10, 0,
            "0xbeef000000000000000000000000000000000000",
            "0xdead000000000000000000000000000000000000",
            2000000000000000000u128,
        ).await.expect("Failed to insert log");
    }
    
    let sig = EventSignature::parse(
        "Transfer(address indexed from, address indexed to, uint256 value)"
    ).expect("Failed to parse signature");
    
    // Query with filter on indexed param - should use predicate pushdown
    let user_sql = r#"SELECT COUNT(*) as cnt FROM Transfer WHERE "from" = '0xa975ba910c2ee169956f3df99ee2ece79d3887cf'"#;
    
    let normalized = sig.normalize_table_references(user_sql);
    let pushed = sig.rewrite_filters_for_pushdown(&normalized);
    let cte = sig.to_cte_sql_clickhouse();
    let full_sql = format!("WITH {} {}", cte, pushed);
    let final_sql = convert_hex_literals_clickhouse(&full_sql);
    
    println!("Pushdown SQL: {}", final_sql);
    
    // Verify the topic1 filter is in the CTE WHERE clause
    assert!(final_sql.contains("topic1 ="), "Expected topic1 predicate pushdown");
    
    let result = ch.query_json(&final_sql).await.expect("Query failed");
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
    let ch = TestClickHouse::new(TEST_DB).await.expect("Failed to create ClickHouse client");
    
    if ch.wait_for_ready().await.is_err() {
        println!("ClickHouse not available, skipping test");
        return;
    }
    
    ch.reset_database().await.expect("Failed to reset database");
    ch.create_mock_logs_table().await.expect("Failed to create logs table");
    
    ch.insert_transfer_log(1, 0, "0xabc", "0xdef", 1000u128).await.unwrap();
    
    let sig = EventSignature::parse(
        "Transfer(address indexed from, address indexed to, uint256 value)"
    ).expect("Failed to parse signature");
    
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
