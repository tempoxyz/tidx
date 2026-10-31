mod common;

use common::testdb::TestDb;

use axum::{Json, Router, routing::post};
use serde_json::{Value, json};
use serial_test::serial;
use tidx::db::ThrottledPool;
use tidx::sync::engine::SyncEngine;
use tidx::sync::sink::SinkSet;

/// Block 218 of a local `ghcr.io/tempoxyz/tempo:sha-b6d1615` dev node (T14 active), as
/// returned by `eth_getBlockByNumber`, holding one configurable-account (TIP-1107)
/// `updateConfig` transaction, and its `eth_getBlockReceipts` response.
const BLOCK: &str = include_str!("fixtures/configurable_account_block.json");
const RECEIPTS: &str = include_str!("fixtures/configurable_account_receipts.json");

const BLOCK_NUM: i64 = 218;
const ACCOUNT: &str = "00a786b5dee39362feab935c9b9bcac7d6b36850";
const TX_HASH: &str = "9e61cde6322a3427a1a05ae424fc99c4e50acf7544cb631c8556d83f91126cb7";
/// `ConfigurableAccountConfigUpdated(address,bytes32,uint64,uint8,(address,uint8)[])`
const CONFIG_UPDATED: &str = "30a5bc758053b4e2cab86cf2eb8332ad2abe9f2a115bf06dd8144b7ef20c70f8";

#[tokio::test]
#[serial(db)]
async fn test_sync_configurable_account_block() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("Failed to bind test RPC server");
    let rpc_url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(listener, Router::new().route("/", post(captured_rpc)))
            .await
            .expect("RPC server failed");
    });

    let engine = SyncEngine::new(
        ThrottledPool::from_pool(db.pool.clone()),
        SinkSet::new(db.pool.clone()),
        &rpc_url,
    )
    .await
    .expect("Failed to create sync engine");
    engine
        .sync_block(BLOCK_NUM as u64)
        .await
        .expect("Failed to sync configurable account block");
    server.abort();

    let conn = db.pool.get().await.unwrap();
    let tx = conn
        .query_one(
            "SELECT hash, \"from\", type, signature_type, fee_payer FROM txs WHERE block_num = $1",
            &[&BLOCK_NUM],
        )
        .await
        .expect("configurable account transaction not stored");
    let account = hex::decode(ACCOUNT).unwrap();
    assert_eq!(tx.get::<_, Vec<u8>>("hash"), hex::decode(TX_HASH).unwrap());
    assert_eq!(tx.get::<_, Vec<u8>>("from"), account);
    assert_eq!(tx.get::<_, i16>("type"), 0x76);
    assert_eq!(tx.get::<_, Option<i16>>("signature_type"), Some(3));
    assert_eq!(
        tx.get::<_, Option<Vec<u8>>>("fee_payer"),
        Some(account.clone())
    );

    let topic1: Vec<u8> = conn
        .query_one(
            "SELECT topic1 FROM logs WHERE block_num = $1 AND topic0 = $2",
            &[&BLOCK_NUM, &hex::decode(CONFIG_UPDATED).unwrap()],
        )
        .await
        .expect("ConfigurableAccountConfigUpdated log not stored")
        .get(0);
    let mut account_word = vec![0u8; 12];
    account_word.extend_from_slice(&account);
    assert_eq!(topic1, account_word);
}

/// JSON-RPC node replaying the responses captured from the Tempo node.
async fn captured_rpc(Json(body): Json<Value>) -> Json<Value> {
    fn respond(req: &Value) -> Value {
        let result = match req["method"].as_str().expect("missing method") {
            "eth_chainId" => json!("0x539"),
            "eth_blockNumber" => json!(format!("0x{BLOCK_NUM:x}")),
            "eth_getBlockByNumber" => serde_json::from_str(BLOCK).unwrap(),
            "eth_getBlockReceipts" => serde_json::from_str(RECEIPTS).unwrap(),
            method => panic!("unexpected RPC method {method}"),
        };
        json!({ "jsonrpc": "2.0", "id": req["id"], "result": result })
    }

    match body.as_array() {
        Some(batch) => Json(batch.iter().map(respond).collect()),
        None => Json(respond(&body)),
    }
}
