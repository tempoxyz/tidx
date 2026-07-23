//! Head-page sort index coverage for hot-store lookups.
//!
//! Run with: cargo test --test head_page_sort_test
//! Requires: docker compose -f docker/local/docker-compose.yml up -d postgres

mod common;

use common::testdb::TestDb;
use serde_json::Value;
use tidx::db::run_post_startup_migrations;

/// Deterministic hot-window rows: 50 distinct senders/fee payers/indexed
/// addresses spread over 2000 blocks.
const SEED: &str = r#"
INSERT INTO txs (block_num, block_timestamp, idx, hash, type, "from", value, input,
                 gas_limit, max_fee_per_gas, max_priority_fee_per_gas, nonce_key, nonce, fee_payer)
SELECT g, now(), 0, decode(lpad(to_hex(g), 64, '0'), 'hex'), 0,
       decode(lpad(to_hex(g % 50 + 1), 40, '0'), 'hex'),
       '0', '\x', 21000, '0', '0', '\x00', g,
       decode(lpad(to_hex(g % 50 + 1), 40, '0'), 'hex')
FROM generate_series(1, 2000) g;

INSERT INTO receipts (block_num, block_timestamp, tx_idx, tx_hash, "from",
                      gas_used, cumulative_gas_used, status, fee_payer)
SELECT g, now(), 0, decode(lpad(to_hex(g), 64, '0'), 'hex'),
       decode(lpad(to_hex(g % 50 + 1), 40, '0'), 'hex'), 21000, 21000, 1,
       decode(lpad(to_hex(g % 50 + 1), 40, '0'), 'hex')
FROM generate_series(1, 2000) g;

INSERT INTO logs (block_num, block_timestamp, log_idx, tx_idx, tx_hash, address,
                  selector, topic0, topic1, topic2, data)
SELECT g, now(), 0, 0, decode(lpad(to_hex(g), 64, '0'), 'hex'),
       '\x2222222222222222222222222222222222222222',
       '\xddf252ad',
       '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef',
       decode(lpad(to_hex(g % 50 + 1), 64, '0'), 'hex'),
       decode(lpad(to_hex((g + 1) % 50 + 1), 64, '0'), 'hex'),
       '\x'
FROM generate_series(1, 2000) g;

ANALYZE txs; ANALYZE receipts; ANALYZE logs;
"#;

#[tokio::test]
async fn test_head_page_sort_served_by_ordered_index() {
    let db = TestDb::empty().await;
    run_post_startup_migrations(&db.pool)
        .await
        .expect("Failed to run post-startup migrations");
    db.truncate_all().await;

    let conn = db.pool.get().await.expect("Failed to get connection");
    conn.batch_execute(SEED).await.expect("Failed to seed rows");
    // Force index paths so plan shape depends only on available index order.
    conn.execute("SET enable_seqscan = off", &[])
        .await
        .expect("Failed to disable seqscan");

    let queries = [
        (
            "txs by from",
            r#"SELECT hash FROM txs
               WHERE "from" = '\x0000000000000000000000000000000000000001'
               ORDER BY block_num DESC, idx DESC LIMIT 10"#,
        ),
        (
            "txs by fee_payer",
            r#"SELECT hash FROM txs
               WHERE fee_payer = '\x0000000000000000000000000000000000000001'
               ORDER BY block_num DESC, idx DESC LIMIT 10"#,
        ),
        (
            "receipts by fee_payer",
            r#"SELECT tx_hash FROM receipts
               WHERE fee_payer = '\x0000000000000000000000000000000000000001'
               ORDER BY block_num DESC, tx_idx DESC LIMIT 10"#,
        ),
        (
            "logs by selector + indexed address (topic2)",
            r#"SELECT tx_hash FROM logs
               WHERE selector = '\xddf252ad'
                 AND topic2 = '\x0000000000000000000000000000000000000000000000000000000000000001'
               ORDER BY block_num DESC, log_idx DESC LIMIT 10"#,
        ),
    ];

    let mut sorted = Vec::new();
    for (name, sql) in queries {
        let row = conn
            .query_one(&format!("EXPLAIN (FORMAT JSON, COSTS OFF) {sql}"), &[])
            .await
            .expect("Failed to explain query");
        let plan: Value = row.get(0);
        let nodes = sort_nodes(&plan[0]["Plan"]);
        if !nodes.is_empty() {
            sorted.push(format!("{name}: {}", nodes.join(", ")));
        }
    }

    // Head pages paginate by (block_num, position); each lookup column needs an
    // index keyed to serve that order, so no plan should contain a Sort node.
    assert!(
        sorted.is_empty(),
        "head-page queries not served in index order (Sort required):\n{}",
        sorted.join("\n")
    );
}

/// Collects Sort / Incremental Sort node types in a JSON plan tree.
fn sort_nodes(plan: &Value) -> Vec<String> {
    let mut nodes = match plan["Node Type"].as_str() {
        Some(node) if node.contains("Sort") => vec![node.to_string()],
        _ => Vec::new(),
    };
    if let Some(children) = plan["Plans"].as_array() {
        for child in children {
            nodes.extend(sort_nodes(child));
        }
    }
    nodes
}
