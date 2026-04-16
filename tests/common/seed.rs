use anyhow::Result;
use rand::Rng;
use std::time::Instant;
use tracing::info;

use tidx::db::Pool;
use tidx::sync::writer::{write_blocks, write_logs, write_txs};
use tidx::types::{BlockRow, LogRow, TxRow};

/// Seed configuration
pub struct SeedConfig {
    pub txs: u64,
    pub txs_per_block: u64,
}

impl Default for SeedConfig {
    fn default() -> Self {
        Self {
            txs: 5_000_000,
            txs_per_block: 100,
        }
    }
}

impl SeedConfig {
    pub fn new(txs: u64, txs_per_block: u64) -> Self {
        Self { txs, txs_per_block }
    }
}

/// Seed the database with synthetic data for benchmarking
pub async fn seed(pool: &Pool, config: &SeedConfig) -> Result<(u64, i64, u64)> {
    let num_blocks = config.txs / config.txs_per_block;
    let avg_logs = config.txs * 2;

    info!(
        txs = config.txs,
        blocks = num_blocks,
        logs_approx = avg_logs,
        "Seeding synthetic data"
    );

    // Disable synchronous commit for bulk load (10x faster)
    {
        let conn = pool.get().await?;
        conn.execute("SET synchronous_commit = off", &[]).await?;
    }

    let mut rng = rand::rng();
    let batch_size = 50_000u64;
    let mut block_num = 0i64;
    let mut txs_written = 0u64;
    let mut logs_written = 0u64;
    let mut prev_block_hash = generate_hash(0, 0);
    let start_time = Instant::now();

    while txs_written < config.txs {
        let blocks_in_batch = (batch_size / config.txs_per_block).max(1);
        let mut blocks = Vec::with_capacity(blocks_in_batch as usize);
        let mut txs = Vec::with_capacity(batch_size as usize);
        let mut logs = Vec::with_capacity((batch_size * 4) as usize);

        for _ in 0..blocks_in_batch {
            if txs_written >= config.txs {
                break;
            }

            let timestamp = chrono::Utc::now()
                - chrono::Duration::seconds((config.txs - txs_written) as i64);
            let block_hash = generate_hash(block_num as u64, 0xB10C);

            blocks.push(BlockRow {
                num: block_num,
                hash: block_hash.clone(),
                parent_hash: prev_block_hash.clone(),
                timestamp,
                timestamp_ms: timestamp.timestamp_millis(),
                gas_limit: 30_000_000,
                gas_used: rng.random_range(10_000_000..25_000_000),
                miner: generate_address(block_num as u64),
                extra_data: None,
            });

            prev_block_hash = block_hash;

            for idx in 0..config.txs_per_block {
                if txs_written >= config.txs {
                    break;
                }

                let tx_hash = generate_hash(txs_written, 0x7AAA);
                let tx_type = rng.random_range(0..3) as i16;
                let from_addr = generate_address(rng.random_range(0..10000));
                let to_addr = generate_address(rng.random_range(0..10000));

                txs.push(TxRow {
                    block_num,
                    block_timestamp: timestamp,
                    idx: idx as i32,
                    hash: tx_hash.clone(),
                    tx_type,
                    from: from_addr.clone(),
                    to: Some(to_addr),
                    value: "0".to_string(),
                    input: generate_input(&mut rng),
                    gas_limit: 21000 + rng.random_range(0..100000),
                    max_fee_per_gas: "1000000000".to_string(),
                    max_priority_fee_per_gas: "100000000".to_string(),
                    gas_used: Some(21000 + rng.random_range(0..50000)),
                    nonce_key: from_addr,
                    nonce: i64::from(rng.random_range(0..10000)),
                    fee_token: None,
                    fee_payer: None,
                    calls: None,
                    call_count: 1,
                    valid_before: None,
                    valid_after: None,
                    signature_type: Some(0),
                });

                let num_logs = rng.random_range(1..=4);
                for log_offset in 0..num_logs {
                    logs.push(LogRow {
                        block_num,
                        block_timestamp: timestamp,
                        log_idx: (logs_written + log_offset) as i32,
                        tx_idx: idx as i32,
                        tx_hash: tx_hash.clone(),
                        address: generate_address(rng.random_range(0..1000)),
                        selector: Some(random_selector(&mut rng)),
                        topic0: Some(
                            random_selector(&mut rng)
                                .into_iter()
                                .chain(std::iter::repeat(0u8))
                                .take(32)
                                .collect(),
                        ),
                        topic1: Some(generate_hash(rng.random(), 0x10C1)),
                        topic2: Some(generate_hash(rng.random(), 0x10C2)),
                        topic3: None,
                        data: vec![0u8; rng.random_range(32..128)],
                        is_virtual_forward: false,
                    });
                }
                logs_written += num_logs as u64;

                txs_written += 1;
            }

            block_num += 1;
        }

        write_blocks(pool, &blocks).await?;
        write_txs(pool, &txs).await?;
        write_logs(pool, &logs).await?;

        if txs_written % 100_000 == 0 || txs_written >= config.txs {
            let elapsed = start_time.elapsed().as_secs();
            let pct = (txs_written * 100) / config.txs;
            let eta = (elapsed * 100)
                .checked_div(pct)
                .map(|total_estimated| {
                    let remaining = total_estimated.saturating_sub(elapsed);
                    format!("{}m {}s", remaining / 60, remaining % 60)
                })
                .unwrap_or_else(|| "calculating...".to_string());
            let rate = elapsed.checked_div(elapsed).map_or(0, |_| txs_written / elapsed);

            info!(
                txs = txs_written,
                blocks = block_num,
                logs = logs_written,
                pct = pct,
                rate = format!("{} txs/s", rate),
                eta = eta,
                "Progress"
            );
        }
    }

    let elapsed = start_time.elapsed().as_secs();
    info!(
        txs = txs_written,
        blocks = block_num,
        logs = logs_written,
        elapsed = format!("{}m {}s", elapsed / 60, elapsed % 60),
        "Seeding complete"
    );

    Ok((txs_written, block_num, logs_written))
}

fn generate_hash(seed: u64, salt: u64) -> Vec<u8> {
    let combined = seed.wrapping_mul(0x517cc1b727220a95).wrapping_add(salt);
    let mut hash = vec![0u8; 32];
    for (i, chunk) in hash.chunks_mut(8).enumerate() {
        let val = combined
            .wrapping_add(i as u64)
            .wrapping_mul(0x9e3779b97f4a7c15);
        chunk.copy_from_slice(&val.to_le_bytes());
    }
    hash
}

fn generate_address(seed: u64) -> Vec<u8> {
    let hash = generate_hash(seed, 0xADD0);
    hash[0..20].to_vec()
}

fn generate_input(rng: &mut impl Rng) -> Vec<u8> {
    let len = if rng.random_bool(0.3) {
        4
    } else {
        rng.random_range(4..256)
    };
    (0..len).map(|_| rng.random()).collect()
}

fn random_selector(rng: &mut impl Rng) -> Vec<u8> {
    let common: &[[u8; 4]] = &[
        [0xdd, 0xf2, 0x52, 0xad], // Transfer
        [0x8c, 0x5b, 0xe1, 0xe5], // Approval
        [0xd7, 0x8a, 0xd9, 0x5f], // Swap
        [0x1c, 0x41, 0x1e, 0x9a], // Mint
        [0xdc, 0xd1, 0xf1, 0x71], // Burn
    ];
    if rng.random_bool(0.8) {
        common[rng.random_range(0..common.len())].to_vec()
    } else {
        (0..4).map(|_| rng.random()).collect()
    }
}
