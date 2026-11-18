//! Earn vault and hourly holder-balance history tables:
//! `address_balance_hours`, `earn_vaults`, `earn_flows`, and the
//! `earn_vault_price_hours` sampler.

mod common;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::{Json, Router, extract::State, routing::post};
use chrono::{DateTime, TimeZone, Utc};
use common::clickhouse::TestClickHouse;
use serde_json::{Value, json};
use serial_test::serial;
use tidx::config::EarnPricesConfig;
use tidx::sync::ch_sink::ClickHouseSink;
use tidx::sync::earn_prices::{EarnPriceSampler, SampleReport};
use tidx::sync::fetcher::RpcClient;
use tidx::types::{BlockRow, LogRow};

const DB: &str = "tidx_earn_test";

const TRANSFER: &str = "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
const STACK_DEPLOYED: &str = "420632dca5c7b108ec3fe8f96f06644917f70c8682f8f66cadc3b03e1fe9301d";
const STACK_DEPLOYED_V2: &str = "950acdb981ae9ee0189b7bba6d347b4fb9b24f0e4f630a1b9a388bc5702fd67b";
const DEPOSITED: &str = "f5681f9d0db1b911ac18ee83d515a1cf1051853a9eae418316a2fdf7dea427c5";
const REDEEMED: &str = "5cdf07ad0fc222442720b108e3ed4c4640f0fadc2ab2253e66f259a0fea83480";
const WITHDREW_EXACT: &str = "667b1fd1a4d0290e6423170325223d7815bf90da8b978f2ad2001b314ac49c12";
const REDEEM_REQUESTED: &str = "aa7dc873a7d37b94883eae5b185beafe0223de340014bc7a1690496363d5bb28";
const REDEEM_FINALIZED: &str = "d638999d611edc2680566b9afcd2c3ab1af17bf7438f625503928b75bd39e3cb";
const REDEEM_CANCELLED: &str = "ba481b3d46265a658a5106b2462d1fe3c6c50c858b33e5548f598073326b23ab";
const VENUE_SHARES_DEPOSITED: &str =
    "35b15b3f988e6b5a7c3c72c7f44307862e2a93754541f8c81992b9365997bbc0";

const ZERO: &str = "0x0000000000000000000000000000000000000000";
const ALICE: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const BOB: &str = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const TOKEN: &str = "0x20c0000000000000000000000000000000000001";
const FACTORY: &str = "0xfac0000000000000000000000000000000000001";
const VAULT: &str = "0x7a0170000000000000000000000000000000000a";
const BROKEN_VAULT: &str = "0x7a0170000000000000000000000000000000000b";
const SHARE: &str = "0x20c0000000000000000000000000000000000002";
const FEES: &str = "0xfee0000000000000000000000000000000000001";
const ENGINE: &str = "0xe000000000000000000000000000000000000001";
const OWNER: &str = "0x0000000000000000000000000000000000000077";

/// Blocks are 900 s apart from midnight, so each UTC hour holds four blocks:
/// hour h covers blocks 4h..=4h+3 and block 4h+3 is its last.
fn block_time(num: i64) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2025, 1, 15, 0, 0, 0).unwrap() + chrono::Duration::seconds(num * 900)
}

fn hour_start(hour: i64) -> i64 {
    Utc.with_ymd_and_hms(2025, 1, 15, 0, 0, 0)
        .unwrap()
        .timestamp()
        + hour * 3600
}

fn block(num: i64) -> BlockRow {
    let ts = block_time(num);
    BlockRow {
        num,
        hash: num.to_be_bytes().repeat(4),
        parent_hash: (num - 1).to_be_bytes().repeat(4),
        timestamp: ts,
        timestamp_ms: ts.timestamp_millis(),
        gas_limit: 30_000_000,
        gas_used: 21_000,
        miner: vec![0xaa; 20],
        extra_data: None,
        consensus_proposer: None,
    }
}

fn bytes(hex_str: &str) -> Vec<u8> {
    hex::decode(hex_str.trim_start_matches("0x")).unwrap()
}

fn word_address(address: &str) -> Vec<u8> {
    let mut word = vec![0; 12];
    word.extend_from_slice(&bytes(address));
    word
}

fn word_u128(value: u128) -> Vec<u8> {
    let mut word = vec![0; 16];
    word.extend_from_slice(&value.to_be_bytes());
    word
}

fn log(block_num: i64, log_idx: i32, address: &str, topics: &[Vec<u8>], data: Vec<u8>) -> LogRow {
    let topic = |i: usize| topics.get(i).cloned();
    LogRow {
        block_num,
        block_timestamp: block_time(block_num),
        log_idx,
        tx_idx: 0,
        tx_hash: vec![block_num as u8; 32],
        address: bytes(address),
        selector: topic(0),
        topic0: topic(0),
        topic1: topic(1),
        topic2: topic(2),
        topic3: topic(3),
        data,
        is_virtual_forward: false,
    }
}

fn transfer(block_num: i64, log_idx: i32, from: &str, to: &str, value: u128) -> LogRow {
    log(
        block_num,
        log_idx,
        TOKEN,
        &[bytes(TRANSFER), word_address(from), word_address(to)],
        word_u128(value),
    )
}

fn stack_deployed(block_num: i64, topic0: &str, vault: &str, data_words: usize) -> LogRow {
    let mut data = word_address(ENGINE);
    data.extend(word_address(TOKEN));
    data.extend(word_address(OWNER));
    data.resize(32 * data_words, 0x11);
    log(
        block_num,
        0,
        FACTORY,
        &[
            bytes(topic0),
            word_address(vault),
            word_address(SHARE),
            word_address(FEES),
        ],
        data,
    )
}

async fn setup() -> Option<(ClickHouseSink, TestClickHouse)> {
    let ch = TestClickHouse::new(DB).expect("ClickHouse client");
    if ch.wait_for_ready().await.is_err() {
        common::skip("ClickHouse not available");
        return None;
    }
    ch.reset_database().await.expect("reset database");
    let sink = ClickHouseSink::new(&ch.url, DB, None, None).expect("sink");
    sink.ensure_schema().await.expect("ensure_schema");
    Some((sink, ch))
}

async fn tsv(ch: &TestClickHouse, sql: &str) -> String {
    ch.query(&format!("{sql} FORMAT TSV"))
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
}

/// address_balance_hours, read back as "holder token hour delta" lines.
async fn balance_hours(ch: &TestClickHouse) -> String {
    tsv(
        ch,
        "SELECT holder, token, toUnixTimestamp(hour), toString(delta) \
         FROM address_balance_hours ORDER BY holder, token, hour",
    )
    .await
}

/// The same hourly sums, computed directly from address_holder_deltas.
async fn holder_delta_hours(ch: &TestClickHouse) -> String {
    tsv(
        ch,
        "SELECT holder, token, toUnixTimestamp(toStartOfHour(toDateTime(block_timestamp, 'UTC'))) AS h, \
             toString(toInt256(sumIf(balance_delta, leg = 1)) - toInt256(sumIf(balance_delta, leg = -1))) \
         FROM address_holder_deltas FINAL GROUP BY holder, token, h ORDER BY holder, token, h",
    )
    .await
}

async fn balance_at_end_of_hour(ch: &TestClickHouse, holder: &str, hour: i64) -> String {
    tsv(
        ch,
        &format!(
            "SELECT toString(sum(delta)) FROM address_balance_hours \
             WHERE holder = '{holder}' AND token = '{TOKEN}' AND hour <= toDateTime({}, 'UTC') \
             SETTINGS force_primary_key = 1",
            hour_start(hour)
        ),
    )
    .await
    .trim()
    .to_string()
}

fn balance_history_logs() -> Vec<LogRow> {
    vec![
        transfer(1, 0, ZERO, ALICE, 1_000), // hour 0
        transfer(2, 0, ALICE, BOB, 100),    // hour 0
        transfer(5, 0, ALICE, BOB, 50),     // hour 1
        transfer(6, 0, BOB, ALICE, 20),     // hour 1
        transfer(13, 0, ALICE, BOB, 200),   // hour 3
        transfer(17, 0, BOB, ZERO, 30),     // hour 4
        transfer(21, 0, ALICE, BOB, 5),     // hour 5
        transfer(22, 0, ALICE, BOB, 7),     // hour 5
    ]
}

#[tokio::test]
#[serial(clickhouse)]
async fn balance_hours_sum_to_canonical_holder_deltas() {
    let Some((sink, ch)) = setup().await else {
        return;
    };
    sink.write_logs(&balance_history_logs())
        .await
        .expect("write logs");

    let expected = holder_delta_hours(&ch).await;
    assert_eq!(balance_hours(&ch).await, expected);

    assert!(
        !balance_hours(&ch).await.contains(ZERO),
        "zero address is not a holder"
    );

    // Replayed inserts must not double-count movements.
    sink.write_logs(&balance_history_logs())
        .await
        .expect("replay logs");
    assert_eq!(balance_hours(&ch).await, expected);

    // Balances at the end of each hour.
    for (hour, alice, bob) in [
        (0, "900", "100"),
        (1, "870", "130"),
        (2, "870", "130"),
        (3, "670", "330"),
        (4, "670", "300"),
        (5, "658", "312"),
    ] {
        assert_eq!(
            balance_at_end_of_hour(&ch, ALICE, hour).await,
            alice,
            "alice hour {hour}"
        );
        assert_eq!(
            balance_at_end_of_hour(&ch, BOB, hour).await,
            bob,
            "bob hour {hour}"
        );
    }

    // The running sum ends at the current balance.
    assert_eq!(
        tsv(&ch, &format!("SELECT toString(balance) FROM address_balances WHERE holder = '{ALICE}' AND token = '{TOKEN}'"))
            .await
            .trim(),
        "658"
    );
}

#[tokio::test]
#[serial(clickhouse)]
async fn balance_hours_follow_reorg_pruning() {
    let Some((sink, ch)) = setup().await else {
        return;
    };
    sink.write_logs(&balance_history_logs())
        .await
        .expect("write logs");

    // A reorg at the tip shows up immediately.
    sink.delete_from(22).await.expect("delete_from");
    assert_eq!(balance_at_end_of_hour(&ch, ALICE, 5).await, "665");
    assert_eq!(balance_hours(&ch).await, holder_delta_hours(&ch).await);

    // Replacement blocks land in a different hour.
    sink.write_logs(&[transfer(24, 0, ALICE, BOB, 65)])
        .await
        .expect("write replacement");
    assert_eq!(balance_at_end_of_hour(&ch, ALICE, 5).await, "665");
    assert_eq!(balance_at_end_of_hour(&ch, ALICE, 6).await, "600");
    assert_eq!(balance_hours(&ch).await, holder_delta_hours(&ch).await);

    // A deeper repair also takes effect without a refresh.
    sink.delete_from(6).await.expect("deep delete_from");
    assert_eq!(balance_hours(&ch).await, holder_delta_hours(&ch).await);
    assert_eq!(balance_at_end_of_hour(&ch, ALICE, 5).await, "850");
}

#[tokio::test]
#[serial(clickhouse)]
async fn earn_vaults_decode_both_deployment_layouts() {
    let Some((sink, ch)) = setup().await else {
        return;
    };
    sink.write_logs(&[
        stack_deployed(3, STACK_DEPLOYED, VAULT, 11),
        stack_deployed(9, STACK_DEPLOYED_V2, BROKEN_VAULT, 13),
    ])
    .await
    .expect("write logs");

    assert_eq!(
        tsv(
            &ch,
            "SELECT vault, share_token, fees, engine, asset, owner, factory, deployed_block, \
                 toUnixTimestamp(deployed_at) FROM earn_vaults FINAL ORDER BY deployed_block",
        )
        .await,
        format!(
            "{VAULT}\t{SHARE}\t{FEES}\t{ENGINE}\t{TOKEN}\t{OWNER}\t{FACTORY}\t3\t{}\n\
             {BROKEN_VAULT}\t{SHARE}\t{FEES}\t{ENGINE}\t{TOKEN}\t{OWNER}\t{FACTORY}\t9\t{}\n",
            block_time(3).timestamp(),
            block_time(9).timestamp()
        )
    );

    sink.delete_from(9).await.expect("delete_from");
    assert_eq!(
        tsv(&ch, "SELECT vault FROM earn_vaults FINAL").await,
        format!("{VAULT}\n")
    );
}

#[tokio::test]
#[serial(clickhouse)]
async fn earn_flows_decode_each_event_and_follow_reorgs() {
    let Some((sink, ch)) = setup().await else {
        return;
    };
    let request_id = format!("0x{}", "ab".repeat(32));
    let caller = word_address(BOB);
    let receiver = word_address(ALICE);
    let data = |words: &[u128]| words.iter().flat_map(|w| word_u128(*w)).collect::<Vec<_>>();
    let mut finalized = word_u128(30);
    finalized.extend(word_address(TOKEN));
    finalized.extend(word_u128(31));

    let logs = vec![
        log(
            10,
            0,
            VAULT,
            &[bytes(DEPOSITED), caller.clone(), receiver.clone()],
            data(&[1_000, 990]),
        ),
        log(
            10,
            1,
            VAULT,
            &[bytes(REDEEMED), caller.clone(), receiver.clone()],
            data(&[100, 101]),
        ),
        log(
            11,
            0,
            VAULT,
            &[bytes(WITHDREW_EXACT), caller.clone(), receiver.clone()],
            data(&[50, 49]),
        ),
        log(
            11,
            1,
            VAULT,
            &[
                bytes(REDEEM_REQUESTED),
                bytes(&request_id),
                word_address(BOB),
                receiver.clone(),
            ],
            data(&[40]),
        ),
        log(
            12,
            0,
            VAULT,
            &[
                bytes(REDEEM_FINALIZED),
                bytes(&request_id),
                receiver.clone(),
            ],
            finalized,
        ),
        log(
            12,
            1,
            VAULT,
            &[
                bytes(REDEEM_CANCELLED),
                bytes(&request_id),
                receiver.clone(),
            ],
            data(&[10]),
        ),
        log(
            13,
            0,
            VAULT,
            &[
                bytes(VENUE_SHARES_DEPOSITED),
                caller.clone(),
                receiver.clone(),
            ],
            data(&[500, 499, 480]),
        ),
        // A transfer from the vault is not a flow.
        log(
            13,
            1,
            VAULT,
            &[bytes(TRANSFER), caller.clone(), receiver.clone()],
            data(&[1]),
        ),
    ];
    sink.write_logs(&logs).await.expect("write logs");
    sink.write_logs(&logs).await.expect("replay logs");

    let rows = "SELECT block_num, log_idx, vault, account, caller, request_id, kind, \
                    toString(assets), toString(shares) \
                FROM earn_flows FINAL ORDER BY block_num, log_idx";
    let r = &request_id;
    assert_eq!(
        tsv(&ch, rows).await,
        [
            format!("10\t0\t{VAULT}\t{ALICE}\t{BOB}\t\tdeposited\t1000\t990"),
            format!("10\t1\t{VAULT}\t{ALICE}\t{BOB}\t\tredeemed\t101\t100"),
            format!("11\t0\t{VAULT}\t{ALICE}\t{BOB}\t\twithdrew_exact\t50\t49"),
            format!("11\t1\t{VAULT}\t{BOB}\t\t{r}\tredeem_requested\t0\t40"),
            format!("12\t0\t{VAULT}\t{ALICE}\t\t{r}\tredeem_finalized\t31\t30"),
            format!("12\t1\t{VAULT}\t{ALICE}\t\t{r}\tredeem_cancelled\t0\t10"),
            format!("13\t0\t{VAULT}\t{ALICE}\t{BOB}\t\tvenue_shares_deposited\t499\t480"),
        ]
        .map(|line| line + "\n")
        .concat()
    );

    sink.delete_from(12).await.expect("delete_from");
    assert_eq!(
        tsv(
            &ch,
            "SELECT kind FROM earn_flows FINAL ORDER BY block_num, log_idx"
        )
        .await,
        "deposited\nredeemed\nwithdrew_exact\nredeem_requested\n"
    );
}

/// JSON-RPC stub: `previewRedeem` returns 1e18 + block number, except on
/// BROKEN_VAULT, which reverts. Records every (vault, block) it was asked for.
#[derive(Clone, Default)]
struct StubRpc {
    calls: Arc<Mutex<Vec<(String, u64)>>>,
    fail_all: Arc<Mutex<bool>>,
}

async fn stub_rpc(State(stub): State<StubRpc>, Json(req): Json<Value>) -> Json<Value> {
    let id = req["id"].clone();
    let vault = req["params"][0]["to"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    let block = u64::from_str_radix(
        req["params"][1].as_str().unwrap().trim_start_matches("0x"),
        16,
    )
    .unwrap();
    assert_eq!(
        req["params"][0]["data"],
        "0x4cdad5060000000000000000000000000000000000000000000000000de0b6b3a7640000"
    );
    stub.calls.lock().unwrap().push((vault.clone(), block));
    if *stub.fail_all.lock().unwrap() || vault == BROKEN_VAULT {
        return Json(
            json!({"jsonrpc": "2.0", "id": id, "error": {"code": 3, "message": "execution reverted"}}),
        );
    }
    let price = 1_000_000_000_000_000_000u128 + u128::from(block);
    Json(json!({"jsonrpc": "2.0", "id": id, "result": format!("0x{price:064x}")}))
}

async fn start_stub() -> (StubRpc, String) {
    let stub = StubRpc::default();
    let app = Router::new()
        .route("/", post(stub_rpc))
        .with_state(stub.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (stub, url)
}

fn sampler(sink: &ClickHouseSink, url: &str, factories: Vec<String>) -> EarnPriceSampler {
    EarnPriceSampler::new(
        sink.clone(),
        RpcClient::new(url),
        4217,
        EarnPricesConfig {
            factories,
            ..EarnPricesConfig::default()
        },
    )
    .unwrap()
}

async fn price_rows(ch: &TestClickHouse) -> String {
    tsv(
        ch,
        "SELECT vault, toUnixTimestamp(hour), block_num, toString(assets_per_1e18_shares) \
         FROM earn_vault_price_hours FINAL ORDER BY vault, hour",
    )
    .await
}

fn expected_price_rows(vault: &str, hours: std::ops::RangeInclusive<i64>) -> String {
    use std::fmt::Write;
    let mut rows = String::new();
    for h in hours {
        let last_block = 4 * h + 3;
        writeln!(
            rows,
            "{vault}\t{}\t{last_block}\t{}",
            hour_start(h),
            1_000_000_000_000_000_000u128 + last_block as u128
        )
        .unwrap();
    }
    rows
}

#[tokio::test]
#[serial(clickhouse)]
async fn price_sampler_writes_each_completed_hour_once_and_skips_failures() {
    let Some((sink, ch)) = setup().await else {
        return;
    };
    let (stub, url) = start_stub().await;

    // Vault deployed in hour 1, broken vault in hour 2; blocks through hour 5
    // (block 20 is the first of hour 5), so hours 1..=4 are complete.
    sink.write_blocks(&(0..=20).map(block).collect::<Vec<_>>())
        .await
        .expect("blocks");
    sink.write_logs(&[
        stack_deployed(5, STACK_DEPLOYED, VAULT, 11),
        stack_deployed(9, STACK_DEPLOYED_V2, BROKEN_VAULT, 13),
    ])
    .await
    .expect("deployments");

    let first = sampler(&sink, &url, vec![]);
    let report = first.run_once().await.expect("first round");
    assert_eq!(
        report,
        SampleReport {
            written: 4,
            failed: 3,
            deferred: 0
        }
    );
    // Only values the RPC returned are stored: nothing for the broken vault.
    assert_eq!(price_rows(&ch).await, expected_price_rows(VAULT, 1..=4));

    // Idempotent: a second round finds nothing new to write, and the failed
    // hours back off instead of being retried in a hot loop.
    let calls_before = stub.calls.lock().unwrap().len();
    let report = first.run_once().await.expect("second round");
    assert_eq!(
        report,
        SampleReport {
            written: 0,
            failed: 0,
            deferred: 3
        }
    );
    assert_eq!(stub.calls.lock().unwrap().len(), calls_before);
    assert_eq!(price_rows(&ch).await, expected_price_rows(VAULT, 1..=4));
    assert_eq!(
        tsv(&ch, "SELECT count() FROM earn_vault_price_hours").await,
        "4\n"
    );

    // Rewriting an hour (a fresh sampler retries everything) replaces, not
    // duplicates, under FINAL.
    ch.query("ALTER TABLE earn_vault_price_hours DELETE WHERE hour = toDateTime(0, 'UTC') SETTINGS mutations_sync = 2")
        .await
        .unwrap();
    *stub.fail_all.lock().unwrap() = true;
    sink.write_blocks(&(21..=24).map(block).collect::<Vec<_>>())
        .await
        .expect("blocks");
    let report = sampler(&sink, &url, vec![])
        .run_once()
        .await
        .expect("failing round");
    assert_eq!(report.written, 0, "an RPC failure writes nothing");
    assert_eq!(price_rows(&ch).await, expected_price_rows(VAULT, 1..=4));

    *stub.fail_all.lock().unwrap() = false;
    let report = sampler(&sink, &url, vec![])
        .run_once()
        .await
        .expect("recovery round");
    assert_eq!(
        report.written, 1,
        "hour 5 completes once block 24 is indexed"
    );
    assert_eq!(price_rows(&ch).await, expected_price_rows(VAULT, 1..=5));

    // Reorg pruning removes affected hours and the sampler writes them again.
    sink.delete_from(19).await.expect("delete_from");
    sink.write_blocks(&(19..=24).map(block).collect::<Vec<_>>())
        .await
        .expect("reorged blocks");
    assert_eq!(price_rows(&ch).await, expected_price_rows(VAULT, 1..=3));
    let report = sampler(&sink, &url, vec![])
        .run_once()
        .await
        .expect("after reorg");
    assert_eq!(report.written, 2);
    assert_eq!(price_rows(&ch).await, expected_price_rows(VAULT, 1..=5));
}

#[tokio::test]
#[serial(clickhouse)]
async fn price_sampler_waits_for_the_last_block_of_an_hour_and_honours_factories() {
    let Some((sink, ch)) = setup().await else {
        return;
    };
    let (_stub, url) = start_stub().await;

    // Block 11 (the last of hour 2) is missing, so hour 2 must not be sampled
    // at block 10.
    let blocks: Vec<_> = (0..=16).filter(|n| *n != 11).map(block).collect();
    sink.write_blocks(&blocks).await.expect("blocks");
    sink.write_logs(&[stack_deployed(5, STACK_DEPLOYED, VAULT, 11)])
        .await
        .expect("deployment");

    let other_factory = "0xfac0000000000000000000000000000000000002".to_string();
    let report = sampler(&sink, &url, vec![other_factory])
        .run_once()
        .await
        .unwrap();
    assert_eq!(report, SampleReport::default());
    assert_eq!(price_rows(&ch).await, "");

    let report = sampler(&sink, &url, vec![FACTORY.to_string()])
        .run_once()
        .await
        .unwrap();
    assert_eq!(
        report,
        SampleReport {
            written: 2,
            failed: 0,
            deferred: 1
        }
    );
    let mut expected = expected_price_rows(VAULT, 1..=1);
    expected.push_str(&expected_price_rows(VAULT, 3..=3));
    assert_eq!(price_rows(&ch).await, expected);

    sink.write_blocks(&[block(11)]).await.expect("gap filled");
    let report = sampler(&sink, &url, vec![]).run_once().await.unwrap();
    assert_eq!(report.written, 1);
    assert_eq!(price_rows(&ch).await, expected_price_rows(VAULT, 1..=3));
}

#[test]
fn rpc_stub_layout_matches_block_hours() {
    let mut by_hour: HashMap<i64, i64> = HashMap::new();
    for num in 0..40 {
        let hour = (block_time(num).timestamp() - hour_start(0)) / 3600;
        by_hour
            .entry(hour)
            .and_modify(|last| *last = (*last).max(num))
            .or_insert(num);
    }
    for (hour, last) in by_hour {
        assert_eq!(last, 4 * hour + 3);
    }
}
