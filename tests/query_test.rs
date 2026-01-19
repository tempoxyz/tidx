mod common;

use common::testdb::TestDb;
use ak47::query::EventSignature;

#[tokio::test]
async fn test_abi_uint_function() {
    let db = TestDb::empty().await;
    let conn = db.pool.get().await.expect("Failed to get connection");

    // Test abi_uint with a known value: 0x...01 = 1
    let result: String = conn
        .query_one(
            "SELECT abi_uint('\\x0000000000000000000000000000000000000000000000000000000000000001'::bytea)::text",
            &[],
        )
        .await
        .expect("Failed to execute abi_uint")
        .get(0);

    assert_eq!(result, "1");

    // Test with larger value: 256
    let result: String = conn
        .query_one(
            "SELECT abi_uint('\\x0000000000000000000000000000000000000000000000000000000000000100'::bytea)::text",
            &[],
        )
        .await
        .expect("Failed to execute abi_uint")
        .get(0);

    assert_eq!(result, "256");
}

#[tokio::test]
async fn test_abi_address_function() {
    let db = TestDb::empty().await;
    let conn = db.pool.get().await.expect("Failed to get connection");

    // Test abi_address: extracts last 20 bytes from 32-byte input
    // Input: 0x000000000000000000000000deadbeefdeadbeefdeadbeefdeadbeefdeadbeef
    let result: Vec<u8> = conn
        .query_one(
            "SELECT abi_address('\\x000000000000000000000000deadbeefdeadbeefdeadbeefdeadbeefdeadbeef'::bytea)",
            &[],
        )
        .await
        .expect("Failed to execute abi_address")
        .get(0);

    assert_eq!(hex::encode(&result), "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef");
}

#[tokio::test]
async fn test_abi_bool_function() {
    let db = TestDb::empty().await;
    let conn = db.pool.get().await.expect("Failed to get connection");

    // Test abi_bool: true
    let result: bool = conn
        .query_one(
            "SELECT abi_bool('\\x0000000000000000000000000000000000000000000000000000000000000001'::bytea)",
            &[],
        )
        .await
        .expect("Failed to execute abi_bool")
        .get(0);

    assert!(result);

    // Test abi_bool: false
    let result: bool = conn
        .query_one(
            "SELECT abi_bool('\\x0000000000000000000000000000000000000000000000000000000000000000'::bytea)",
            &[],
        )
        .await
        .expect("Failed to execute abi_bool")
        .get(0);

    assert!(!result);
}

#[tokio::test]
async fn test_event_signature_selector() {
    // Test Transfer selector matches known value
    let sig = EventSignature::parse("Transfer(address,address,uint256)").unwrap();
    assert_eq!(sig.selector_hex(), "ddf252ad");

    // Test Approval selector
    let sig = EventSignature::parse("Approval(address,address,uint256)").unwrap();
    assert_eq!(sig.selector_hex(), "8c5be1e5");
}

#[tokio::test]
async fn test_cte_query_syntax() {
    let db = TestDb::empty().await;
    let conn = db.pool.get().await.expect("Failed to get connection");

    let sig = EventSignature::parse("Transfer(address indexed from, address indexed to, uint256 value)").unwrap();
    let cte = sig.to_cte_sql();

    // Build a full query and verify it's valid SQL (should return 0 rows on empty DB)
    let sql = format!(
        r#"WITH {}
SELECT * FROM "Transfer"
WHERE block_timestamp > NOW() - INTERVAL '1 hour'
LIMIT 10"#,
        cte
    );

    let rows = conn
        .query(&sql, &[])
        .await
        .expect("CTE query should be valid SQL");

    // On an empty/truncated DB, should return 0 rows but execute successfully
    assert!(rows.len() <= 10);
}

#[tokio::test]
async fn test_format_address_function() {
    let db = TestDb::empty().await;
    let conn = db.pool.get().await.expect("Failed to get connection");

    let result: String = conn
        .query_one(
            "SELECT format_address('\\xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef'::bytea)",
            &[],
        )
        .await
        .expect("Failed to execute format_address")
        .get(0);

    assert_eq!(result, "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef");
}

#[tokio::test]
async fn test_abi_int_function() {
    let db = TestDb::empty().await;
    let conn = db.pool.get().await.expect("Failed to get connection");

    // Test positive int: 1
    let result: String = conn
        .query_one(
            "SELECT abi_int('\\x0000000000000000000000000000000000000000000000000000000000000001'::bytea)::text",
            &[],
        )
        .await
        .expect("Failed to execute abi_int")
        .get(0);

    assert_eq!(result, "1");

    // Test negative int: -1 (all 0xFF bytes in two's complement)
    let result: String = conn
        .query_one(
            "SELECT abi_int('\\xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff'::bytea)::text",
            &[],
        )
        .await
        .expect("Failed to execute abi_int")
        .get(0);

    assert_eq!(result, "-1");
}
