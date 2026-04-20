mod common;

use common::testdb::TestDb;
use serial_test::serial;
use std::net::TcpListener;
use std::process::Stdio;
use std::time::{Duration, Instant};
use tidx::db::ThrottledPool;
use tidx::sync::engine::SyncEngine;
use tidx::sync::sink::SinkSet;

const ANVIL_ADDR: &str = "0x1000000000000000000000000000000000000001";
const SENDER: &str = "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266";
const VIRTUAL_ADDR: &str = "0x01020304fdfdfdfdfdfdfdfdfdfd0a0b0c0d0e0f";
const MASTER: &str = "0x1111111111111111111111111111111111111111";

// Runtime bytecode for:
// contract VirtualEmitter {
//   event Transfer(address indexed from, address indexed to, uint256 value);
//   function emitPair(address sender, address virtualAddr, address master, uint256 amount) external {
//     emit Transfer(sender, virtualAddr, amount);
//     emit Transfer(virtualAddr, master, amount);
//   }
// }
const VIRTUAL_EMITTER_RUNTIME: &str = "0x608060405234801561000f575f5ffd5b5060043610610029575f3560e01c8063b0aa806c1461002d575b5f5ffd5b610047600480360381019061004291906101aa565b610049565b005b8273ffffffffffffffffffffffffffffffffffffffff168473ffffffffffffffffffffffffffffffffffffffff167fddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef836040516100a6919061021d565b60405180910390a38173ffffffffffffffffffffffffffffffffffffffff168373ffffffffffffffffffffffffffffffffffffffff167fddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef8360405161010b919061021d565b60405180910390a350505050565b5f5ffd5b5f73ffffffffffffffffffffffffffffffffffffffff82169050919050565b5f6101468261011d565b9050919050565b6101568161013c565b8114610160575f5ffd5b50565b5f813590506101718161014d565b92915050565b5f819050919050565b61018981610177565b8114610193575f5ffd5b50565b5f813590506101a481610180565b92915050565b5f5f5f5f608085870312156101c2576101c1610119565b5b5f6101cf87828801610163565b94505060206101e087828801610163565b93505060406101f187828801610163565b925050606061020287828801610196565b91505092959194509250565b61021781610177565b82525050565b5f6020820190506102305f83018461020e565b9291505056fea2646970667358221220b6ee16f7da4a678ea16a8864c878fd1a1f3e57cc0e941248d413985e24470e6764736f6c63430008210033";

// cast calldata 'emitPair(address,address,address,uint256)' \
//   0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266 \
//   0x01020304fdfdfdfdfdfdfdfdfdfd0a0b0c0d0e0f \
//   0x1111111111111111111111111111111111111111 123
const EMIT_PAIR_CALLDATA: &str = "0xb0aa806c000000000000000000000000f39fd6e51aad88f6f4ce6ab8827279cfffb9226600000000000000000000000001020304fdfdfdfdfdfdfdfdfdfd0a0b0c0d0e0f0000000000000000000000001111111111111111111111111111111111111111000000000000000000000000000000000000000000000000000000000000007b";

#[tokio::test]
#[serial(db)]
#[ignore = "requires DATABASE_URL, foundry anvil in PATH, network access to Tempo testnet RPC, and FORK_BLOCK_NUMBER for deterministic replay"]
async fn test_virtual_address_forward_hop_e2e() {
    let port = reserve_port();
    let rpc_url = format!("http://127.0.0.1:{port}");

    let fork_block = fork_block_number().expect("FORK_BLOCK_NUMBER must be set");

    let mut anvil = tokio::process::Command::new("anvil")
        .arg("--tempo")
        .arg("--host")
        .arg("127.0.0.1")
        .arg("--port")
        .arg(port.to_string())
        .arg("--fork-url")
        .arg("https://rpc.testnet.tempo.xyz")
        .arg("--fork-block-number")
        .arg(fork_block.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn anvil --tempo");

    let result = run_virtual_address_e2e(&rpc_url).await;

    let _ = anvil.kill().await;
    result.expect("virtual address E2E failed");
}

async fn run_virtual_address_e2e(rpc_url: &str) -> anyhow::Result<()> {
    wait_for_rpc(rpc_url, Duration::from_secs(20)).await?;

    rpc_call::<serde_json::Value>(
        rpc_url,
        "anvil_setCode",
        serde_json::json!([ANVIL_ADDR, VIRTUAL_EMITTER_RUNTIME]),
    )
    .await?;

    let tx_hash: String = rpc_call(
        rpc_url,
        "eth_sendTransaction",
        serde_json::json!([{
            "from": SENDER,
            "to": ANVIL_ADDR,
            "data": EMIT_PAIR_CALLDATA,
        }]),
    )
    .await?;

    let receipt: serde_json::Value =
        wait_for_receipt(rpc_url, &tx_hash, Duration::from_secs(10)).await?;
    let block_num = u64::from_str_radix(
        receipt["blockNumber"]
            .as_str()
            .expect("receipt blockNumber missing")
            .trim_start_matches("0x"),
        16,
    )?;

    let db = TestDb::empty().await;
    db.truncate_all().await;

    let sinks = SinkSet::new(db.pool.clone());
    let engine = SyncEngine::new(ThrottledPool::from_pool(db.pool.clone()), sinks, rpc_url).await?;
    engine.sync_block(block_num).await?;

    let conn = db.pool.get().await?;
    let rows = conn
        .query(
            r#"
            SELECT
                block_num,
                encode(tx_hash, 'hex') AS tx_hash,
                log_idx,
                format_address(address) AS log_address,
                format_address(abi_address(topic1)) AS from_addr,
                format_address(abi_address(topic2)) AS to_addr,
                is_virtual_forward
            FROM logs
            WHERE tx_hash = decode($1, 'hex')
            ORDER BY log_idx
            "#,
            &[&tx_hash.trim_start_matches("0x")],
        )
        .await?;

    assert!(rows.len() >= 2, "expected at least 2 logs, got {}", rows.len());

    let contract_rows: Vec<_> = rows
        .iter()
        .filter(|r| {
            let log_address: String = r.get(3);
            log_address.eq_ignore_ascii_case(ANVIL_ADDR)
        })
        .collect();

    assert!(
        contract_rows.len() >= 2,
        "expected at least 2 logs from test contract, got {}",
        contract_rows.len()
    );

    let found_attribution = contract_rows.iter().any(|r| {
        let from_addr: String = r.get(4);
        let to_addr: String = r.get(5);
        let is_virtual_forward: bool = r.get(6);
        from_addr.eq_ignore_ascii_case(SENDER)
            && to_addr.eq_ignore_ascii_case(VIRTUAL_ADDR)
            && !is_virtual_forward
    });

    let found_forward = contract_rows.iter().any(|r| {
        let from_addr: String = r.get(4);
        let to_addr: String = r.get(5);
        let is_virtual_forward: bool = r.get(6);
        from_addr.eq_ignore_ascii_case(VIRTUAL_ADDR)
            && to_addr.eq_ignore_ascii_case(MASTER)
            && is_virtual_forward
    });

    let forward_count = contract_rows.iter().filter(|r| r.get::<_, bool>(6)).count();

    assert_eq!(forward_count, 1, "expected exactly one marked forward hop");
    assert!(found_attribution, "missing attribution hop log");
    assert!(found_forward, "missing marked forwarding hop log");

    Ok(())
}

fn fork_block_number() -> anyhow::Result<u64> {
    let value = std::env::var("FORK_BLOCK_NUMBER").map_err(|_| {
        anyhow::anyhow!("FORK_BLOCK_NUMBER must be set for deterministic E2E replay")
    })?;
    Ok(value.parse()?)
}

fn reserve_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral port")
        .local_addr()
        .expect("get local addr")
        .port()
}

async fn wait_for_rpc(rpc_url: &str, timeout: Duration) -> anyhow::Result<()> {
    let started = Instant::now();
    while started.elapsed() < timeout {
        if rpc_call::<String>(rpc_url, "eth_blockNumber", serde_json::json!([]))
            .await
            .is_ok()
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    anyhow::bail!("timed out waiting for anvil rpc at {rpc_url}")
}

async fn wait_for_receipt(
    rpc_url: &str,
    tx_hash: &str,
    timeout: Duration,
) -> anyhow::Result<serde_json::Value> {
    let started = Instant::now();
    while started.elapsed() < timeout {
        let receipt: serde_json::Value = rpc_call(
            rpc_url,
            "eth_getTransactionReceipt",
            serde_json::json!([tx_hash]),
        )
        .await?;
        if !receipt.is_null() {
            return Ok(receipt);
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    anyhow::bail!("timed out waiting for receipt for {tx_hash}")
}

async fn rpc_call<T: serde::de::DeserializeOwned>(
    rpc_url: &str,
    method: &str,
    params: serde_json::Value,
) -> anyhow::Result<T> {
    let client = reqwest::Client::new();
    let resp: serde_json::Value = client
        .post(rpc_url)
        .json(&serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        }))
        .send()
        .await?
        .json()
        .await?;

    if let Some(err) = resp.get("error") {
        anyhow::bail!("rpc {method} failed: {err}");
    }

    serde_json::from_value(resp["result"].clone()).map_err(Into::into)
}
