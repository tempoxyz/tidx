mod common;

use std::collections::HashMap;
use std::net::TcpListener;
use std::process::Stdio;
use std::time::{Duration, Instant};

use alloy::primitives::U256;
use common::clickhouse::TestClickHouse;
use common::testdb::TestDb;
use serial_test::serial;
use tidx::db::ThrottledPool;
use tidx::sync::ch_sink::ClickHouseSink;
use tidx::sync::engine::SyncEngine;
use tidx::sync::sink::SinkSet;

const CH_DB: &str = "tidx_overflow_e2e";
const TOKEN: &str = "0x1000000000000000000000000000000000000abc";
const ALICE: &str = "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266";
const BOB: &str = "0x70997970c51812dc3a010c7d01b50e0d17dc79c8";

const AMOUNT_A_HEX: &str = "8000000000000000000000000000000000000000000000000000000000000064";
const AMOUNT_C_HEX: &str = "8000000000000000000000000000000000000000000000000000000000000028";

const MOCK_ERC20_RUNTIME: &str = "0x608060405234801561000f575f5ffd5b506004361061003f575f3560e01c806340c10f191461004357806370a082311461005f578063a9059cbb1461008f575b5f5ffd5b61005d60048036038101906100589190610333565b6100bf565b005b61007960048036038101906100749190610371565b61017a565b60405161008691906103ab565b60405180910390f35b6100a960048036038101906100a49190610333565b61018e565b6040516100b691906103de565b60405180910390f35b805f5f8473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff1681526020019081526020015f205f82825461010a9190610424565b925050819055508173ffffffffffffffffffffffffffffffffffffffff165f73ffffffffffffffffffffffffffffffffffffffff167fddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef8360405161016e91906103ab565b60405180910390a35050565b5f602052805f5260405f205f915090505481565b5f815f5f3373ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff1681526020019081526020015f205f8282546101da9190610457565b92505081905550815f5f8573ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff1681526020019081526020015f205f82825461022c9190610424565b925050819055508273ffffffffffffffffffffffffffffffffffffffff163373ffffffffffffffffffffffffffffffffffffffff167fddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef8460405161029091906103ab565b60405180910390a36001905092915050565b5f5ffd5b5f73ffffffffffffffffffffffffffffffffffffffff82169050919050565b5f6102cf826102a6565b9050919050565b6102df816102c5565b81146102e9575f5ffd5b50565b5f813590506102fa816102d6565b92915050565b5f819050919050565b61031281610300565b811461031c575f5ffd5b50565b5f8135905061032d81610309565b92915050565b5f5f60408385031215610349576103486102a2565b5b5f610356858286016102ec565b92505060206103678582860161031f565b9150509250929050565b5f60208284031215610386576103856102a2565b5b5f610393848285016102ec565b91505092915050565b6103a581610300565b82525050565b5f6020820190506103be5f83018461039c565b92915050565b5f8115159050919050565b6103d8816103c4565b82525050565b5f6020820190506103f15f8301846103cf565b92915050565b7f4e487b71000000000000000000000000000000000000000000000000000000005f52601160045260245ffd5b5f61042e82610300565b915061043983610300565b9250828201905080821115610451576104506103f7565b5b92915050565b5f61046182610300565b915061046c83610300565b9250828203905081811115610484576104836103f7565b5b9291505056fea2646970667358221220a259c13837082d30624af08f548afb147fb402cd44a053fb87d01340cc1e210564736f6c63430008210033";

#[tokio::test]
#[serial(db)]
#[ignore = "requires DATABASE_URL, CLICKHOUSE_URL, foundry anvil in PATH, network access to Tempo testnet RPC, and FORK_BLOCK_NUMBER for deterministic replay"]
async fn test_overflow_balance_matches_onchain_e2e() {
    let Some(ch) = clickhouse().await else {
        println!("ClickHouse not available, skipping test");
        return;
    };
    run_with_forked_anvil(|rpc_url| async move { run_overflow_e2e(rpc_url, ch).await }).await;
}

async fn run_overflow_e2e(rpc_url: String, ch: TestClickHouse) -> anyhow::Result<()> {
    wait_for_rpc(&rpc_url, Duration::from_secs(20)).await?;

    rpc_call::<serde_json::Value>(
        &rpc_url,
        "anvil_setCode",
        serde_json::json!([TOKEN, MOCK_ERC20_RUNTIME]),
    )
    .await?;

    let mint_data = format!("0x40c10f19{}{}", pad_address(ALICE), AMOUNT_A_HEX);
    let mint_block = send_and_confirm(&rpc_url, ALICE, TOKEN, &mint_data).await?;

    let transfer_data = format!("0xa9059cbb{}{}", pad_address(BOB), AMOUNT_C_HEX);
    let transfer_block = send_and_confirm(&rpc_url, ALICE, TOKEN, &transfer_data).await?;

    let onchain_alice = balance_of(&rpc_url, ALICE).await?;
    let onchain_bob = balance_of(&rpc_url, BOB).await?;

    let int256_max = (U256::from(1) << 255) - U256::from(1);
    assert!(
        onchain_bob > int256_max,
        "bob's on-chain balance must exceed Int256::MAX to exercise the overflow path, got {onchain_bob}"
    );

    ch.reset_database().await?;
    let sink = ClickHouseSink::new(&ch.url, CH_DB, None, None)?;
    sink.ensure_schema().await?;

    let db = TestDb::empty().await;
    db.truncate_all().await;

    let sinks = SinkSet::new(db.pool.clone()).with_clickhouse(sink);
    let engine =
        SyncEngine::new(ThrottledPool::from_pool(db.pool.clone()), sinks, &rpc_url).await?;
    engine
        .sync_range(
            mint_block.min(transfer_block),
            mint_block.max(transfer_block),
        )
        .await?;

    let balances = token_balances(&ch, TOKEN).await;
    assert_eq!(
        balances.get(&ALICE.to_lowercase()),
        Some(&onchain_alice),
        "tidx alice balance must equal on-chain balanceOf(alice)"
    );
    assert_eq!(
        balances.get(&BOB.to_lowercase()),
        Some(&onchain_bob),
        "tidx bob balance must equal on-chain balanceOf(bob) (above Int256::MAX)"
    );

    Ok(())
}

async fn token_balances(ch: &TestClickHouse, token: &str) -> HashMap<String, U256> {
    let result = ch
        .query_json(&format!(
            "SELECT lower(holder) AS holder, toString(balance) AS balance \
             FROM token_balances WHERE lower(token) = lower('{token}') ORDER BY holder"
        ))
        .await
        .expect("token_balances query failed");
    result["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| {
            let holder = row["holder"].as_str().unwrap().to_string();
            let balance = U256::from_str_radix(row["balance"].as_str().unwrap(), 10)
                .expect("balance must be a base-10 U256");
            (holder, balance)
        })
        .collect()
}

async fn clickhouse() -> Option<TestClickHouse> {
    let ch = TestClickHouse::new(CH_DB).await.ok()?;
    ch.wait_for_ready().await.ok()?;
    Some(ch)
}

fn pad_address(address: &str) -> String {
    format!("{:0>64}", address.trim_start_matches("0x"))
}

async fn balance_of(rpc_url: &str, holder: &str) -> anyhow::Result<U256> {
    let data = format!("0x70a08231{}", pad_address(holder));
    let result: String = rpc_call(
        rpc_url,
        "eth_call",
        serde_json::json!([{ "to": TOKEN, "data": data }, "latest"]),
    )
    .await?;
    Ok(U256::from_str_radix(result.trim_start_matches("0x"), 16)?)
}

async fn send_and_confirm(rpc_url: &str, from: &str, to: &str, data: &str) -> anyhow::Result<u64> {
    let tx_hash: String = rpc_call(
        rpc_url,
        "eth_sendTransaction",
        serde_json::json!([{ "from": from, "to": to, "data": data }]),
    )
    .await?;
    let receipt = wait_for_receipt(rpc_url, &tx_hash, Duration::from_secs(10)).await?;
    let block_num = u64::from_str_radix(
        receipt["blockNumber"]
            .as_str()
            .expect("receipt blockNumber missing")
            .trim_start_matches("0x"),
        16,
    )?;
    Ok(block_num)
}

async fn run_with_forked_anvil<F, Fut>(f: F)
where
    F: FnOnce(String) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<()>>,
{
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

    let result = f(rpc_url).await;

    let _ = anvil.kill().await;
    result.expect("balance overflow E2E failed");
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
