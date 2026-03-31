use alloy::consensus::transaction::Recovered;
use alloy::consensus::{Transaction as TransactionTrait, Typed2718};
use alloy::network::{ReceiptResponse, TransactionResponse};
use chrono::{DateTime, TimeZone, Utc};
use tempo_alloy::primitives::transaction::SignatureType;

use crate::tempo::{Block, Log, Receipt, Transaction, TempoTxEnvelope};
use crate::types::{BlockRow, LogRow, ReceiptRow, TxRow};

/// Validate that receipts match the transactions in their respective blocks.
///
/// Checks two invariants:
/// 1. Receipt count for each block matches the block's transaction count
/// 2. Every receipt's `transaction_hash` exists in the block's transaction list
///
/// `blocks` and `receipts_per_block` must be parallel slices (same length, same order).
pub fn validate_receipts(blocks: &[Block], receipts_per_block: &[Vec<Receipt>]) -> anyhow::Result<()> {
    use alloy::network::ReceiptResponse;
    use std::collections::HashSet;

    if blocks.len() != receipts_per_block.len() {
        anyhow::bail!(
            "Block/receipt batch length mismatch: {} blocks vs {} receipt groups",
            blocks.len(),
            receipts_per_block.len()
        );
    }

    for (block, block_receipts) in blocks.iter().zip(receipts_per_block.iter()) {
        let block_num = block.header.number;
        let tx_count = block.transactions.len();
        let receipt_count = block_receipts.len();

        // Check count match
        if receipt_count != tx_count {
            anyhow::bail!(
                "Block {block_num}: receipt count ({receipt_count}) != transaction count ({tx_count})"
            );
        }

        // Build set of tx hashes from the block
        let block_tx_hashes: HashSet<_> = block
            .transactions
            .txns()
            .map(|tx| tx.tx_hash())
            .collect();

        // Verify every receipt references a transaction in this block
        for receipt in block_receipts {
            let receipt_tx_hash = &receipt.transaction_hash();
            if !block_tx_hashes.contains(receipt_tx_hash) {
                anyhow::bail!(
                    "Block {block_num}: receipt references unknown tx hash 0x{}",
                    hex::encode(receipt_tx_hash)
                );
            }
        }
    }

    Ok(())
}

pub fn timestamp_from_secs(secs: u64) -> DateTime<Utc> {
    Utc.timestamp_opt(secs as i64, 0)
        .single()
        .unwrap_or_else(|| Utc.timestamp_opt(0, 0).single().unwrap())
}

pub fn decode_block(block: &Block) -> BlockRow {
    let timestamp_secs = block.header.timestamp;
    let timestamp = timestamp_from_secs(timestamp_secs);
    let timestamp_ms = (timestamp_secs * 1000) as i64;

    BlockRow {
        num: block.header.number as i64,
        hash: block.header.hash.as_slice().to_vec(),
        parent_hash: block.header.parent_hash.as_slice().to_vec(),
        timestamp,
        timestamp_ms,
        gas_limit: block.header.gas_limit as i64,
        gas_used: block.header.gas_used as i64,
        miner: block.header.beneficiary.as_slice().to_vec(),
        extra_data: Some(block.header.extra_data.to_vec()),
    }
}

pub fn decode_transaction(tx: &Transaction, block: &Block, idx: u32) -> TxRow {
    let block_timestamp = timestamp_from_secs(block.header.timestamp);
    let inner: &Recovered<TempoTxEnvelope> = &tx.inner;

    // Extract Tempo-specific fields if this is a 0x76 transaction
    let (nonce_key, fee_token, calls_json, call_count, valid_before, valid_after, signature_type) =
        if let TempoTxEnvelope::AA(aa_signed) = inner.as_ref() {
            let tempo_tx = aa_signed.tx();
            (
                tempo_tx.nonce_key.to_be_bytes_vec(),
                tempo_tx.fee_token.map(|a| a.as_slice().to_vec()),
                serde_json::to_value(&tempo_tx.calls).ok(),
                tempo_tx.calls.len() as i16,
                tempo_tx.valid_before.map(|v| v as i64),
                tempo_tx.valid_after.map(|v| v as i64),
                Some(match aa_signed.signature().signature_type() {
                    SignatureType::Secp256k1 => 0,
                    SignatureType::P256 => 1,
                    SignatureType::WebAuthn => 2,
                }),
            )
        } else {
            (vec![0u8; 32], None, None, 1, None, None, Some(0))
        };

    TxRow {
        block_num: block.header.number as i64,
        block_timestamp,
        idx: idx as i32,
        hash: tx.tx_hash().as_slice().to_vec(),
        tx_type: inner.ty() as i16,
        from: inner.signer().as_slice().to_vec(),
        to: inner.to().map(|a| a.as_slice().to_vec()),
        value: inner.value().to_string(),
        input: inner.input().to_vec(),
        gas_limit: inner.gas_limit() as i64,
        max_fee_per_gas: inner.max_fee_per_gas().to_string(),
        max_priority_fee_per_gas: inner.max_priority_fee_per_gas().map_or("0".into(), |v| v.to_string()),
        gas_used: None,
        nonce_key,
        nonce: inner.nonce() as i64,
        fee_token,
        fee_payer: None, // Recovered from receipt
        calls: calls_json,
        call_count,
        valid_before,
        valid_after,
        signature_type,
    }
}

pub fn decode_log(log: &Log, block_timestamp: DateTime<Utc>) -> LogRow {
    let topics = log.topics();
    let selector = topics.first().map(|s| s.as_slice().to_vec());

    LogRow {
        block_num: log.block_number.unwrap_or(0) as i64,
        block_timestamp,
        log_idx: log.log_index.unwrap_or(0) as i32,
        tx_idx: log.transaction_index.unwrap_or(0) as i32,
        tx_hash: log
            .transaction_hash
            .map(|h| h.as_slice().to_vec())
            .unwrap_or_default(),
        address: log.address().as_slice().to_vec(),
        selector,
        topic0: topics.first().map(|t| t.as_slice().to_vec()),
        topic1: topics.get(1).map(|t| t.as_slice().to_vec()),
        topic2: topics.get(2).map(|t| t.as_slice().to_vec()),
        topic3: topics.get(3).map(|t| t.as_slice().to_vec()),
        data: log.data().data.to_vec(),
    }
}

/// Enrich transaction rows with fields that come from receipts (gas_used, fee_payer).
/// Must be called after both txs and receipts are decoded.
pub fn enrich_txs_from_receipts(txs: &mut [TxRow], receipts: &[ReceiptRow]) {
    use std::collections::HashMap;
    let receipt_map: HashMap<(i64, i32), &ReceiptRow> = receipts
        .iter()
        .map(|r| ((r.block_num, r.tx_idx), r))
        .collect();
    for tx in txs.iter_mut() {
        if let Some(r) = receipt_map.get(&(tx.block_num, tx.idx)) {
            tx.gas_used = Some(r.gas_used);
            tx.fee_payer = r.fee_payer.clone();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_tx(block_num: i64, idx: i32) -> TxRow {
        TxRow {
            block_num,
            idx,
            ..Default::default()
        }
    }

    fn make_receipt(block_num: i64, tx_idx: i32, gas_used: i64, fee_payer: Option<Vec<u8>>) -> ReceiptRow {
        ReceiptRow {
            block_num,
            tx_idx,
            gas_used,
            fee_payer,
            ..Default::default()
        }
    }

    #[test]
    fn enrich_sets_gas_used_and_fee_payer() {
        let mut txs = vec![make_tx(1, 0), make_tx(1, 1)];
        let receipts = vec![
            make_receipt(1, 0, 21000, Some(vec![0xaa; 20])),
            make_receipt(1, 1, 50000, Some(vec![0xbb; 20])),
        ];

        enrich_txs_from_receipts(&mut txs, &receipts);

        assert_eq!(txs[0].gas_used, Some(21000));
        assert_eq!(txs[0].fee_payer, Some(vec![0xaa; 20]));
        assert_eq!(txs[1].gas_used, Some(50000));
        assert_eq!(txs[1].fee_payer, Some(vec![0xbb; 20]));
    }

    #[test]
    fn enrich_leaves_unmatched_txs_as_none() {
        let mut txs = vec![make_tx(1, 0), make_tx(2, 0)];
        let receipts = vec![make_receipt(1, 0, 21000, None)];

        enrich_txs_from_receipts(&mut txs, &receipts);

        assert_eq!(txs[0].gas_used, Some(21000));
        assert_eq!(txs[1].gas_used, None);
        assert_eq!(txs[1].fee_payer, None);
    }

    #[test]
    fn enrich_empty_receipts_is_noop() {
        let mut txs = vec![make_tx(1, 0)];
        enrich_txs_from_receipts(&mut txs, &[]);
        assert_eq!(txs[0].gas_used, None);
    }

    #[test]
    fn enrich_empty_txs_is_noop() {
        let mut txs: Vec<TxRow> = vec![];
        let receipts = vec![make_receipt(1, 0, 21000, None)];
        enrich_txs_from_receipts(&mut txs, &receipts);
        assert!(txs.is_empty());
    }

    // --- validate_receipts tests ---

    use alloy::primitives::B256;
    use crate::tempo::{Block, Receipt};

    /// Build a minimal Block with the given tx hashes via JSON deserialization.
    fn mock_block(number: u64, tx_hashes: &[B256]) -> Block {
        let txs: Vec<serde_json::Value> = tx_hashes
            .iter()
            .enumerate()
            .map(|(i, hash)| {
                serde_json::json!({
                    "hash": format!("0x{}", hex::encode(hash)),
                    "blockHash": "0x0000000000000000000000000000000000000000000000000000000000000001",
                    "blockNumber": format!("0x{:x}", number),
                    "transactionIndex": format!("0x{:x}", i),
                    "from": "0x0000000000000000000000000000000000000001",
                    "to": "0x0000000000000000000000000000000000000002",
                    "value": "0x0",
                    "input": "0x",
                    "nonce": "0x0",
                    "gas": "0x5208",
                    "gasPrice": "0x3b9aca00",
                    "maxFeePerGas": "0x3b9aca00",
                    "maxPriorityFeePerGas": "0x0",
                    "type": "0x2",
                    "chainId": "0x1",
                    "accessList": [],
                    "v": "0x0",
                    "r": "0x0000000000000000000000000000000000000000000000000000000000000001",
                    "s": "0x0000000000000000000000000000000000000000000000000000000000000001",
                    "yParity": "0x0"
                })
            })
            .collect();

        let block_json = serde_json::json!({
            "hash": "0x0000000000000000000000000000000000000000000000000000000000000001",
            "parentHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
            "number": format!("0x{:x}", number),
            "timestamp": "0x0",
            "gasLimit": "0x0",
            "gasUsed": "0x0",
            "miner": "0x0000000000000000000000000000000000000000",
            "sha3Uncles": "0x0000000000000000000000000000000000000000000000000000000000000000",
            "extraData": "0x",
            "logsBloom": "0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
            "transactionsRoot": "0x0000000000000000000000000000000000000000000000000000000000000000",
            "stateRoot": "0x0000000000000000000000000000000000000000000000000000000000000000",
            "receiptsRoot": "0x0000000000000000000000000000000000000000000000000000000000000000",
            "mixHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
            "nonce": "0x0000000000000000",
            "difficulty": "0x0",
            "totalDifficulty": "0x0",
            "size": "0x0",
            "uncles": [],
            "transactions": txs
        });
        serde_json::from_value(block_json).expect("valid block json")
    }

    /// Build a minimal Receipt referencing the given tx hash at the given block number.
    fn mock_receipt(block_num: u64, tx_hash: B256) -> Receipt {
        let json = serde_json::json!({
            "transactionHash": format!("0x{}", hex::encode(tx_hash)),
            "blockHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
            "blockNumber": format!("0x{:x}", block_num),
            "transactionIndex": "0x0",
            "gasUsed": "0x5208",
            "effectiveGasPrice": "0x3b9aca00",
            "from": "0x0000000000000000000000000000000000000000",
            "to": "0x0000000000000000000000000000000000000000",
            "contractAddress": null,
            "cumulativeGasUsed": "0x5208",
            "logs": [],
            "logsBloom": "0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
            "status": "0x1",
            "type": "0x2",
            "feePayer": "0x0000000000000000000000000000000000000000"
        });
        serde_json::from_value(json).expect("valid receipt json")
    }

    #[test]
    fn validate_receipts_happy_path() {
        let tx1 = B256::repeat_byte(0x11);
        let tx2 = B256::repeat_byte(0x22);
        let block = mock_block(1, &[tx1, tx2]);
        let receipts = vec![mock_receipt(1, tx1), mock_receipt(1, tx2)];

        assert!(validate_receipts(&[block], &[receipts]).is_ok());
    }

    #[test]
    fn validate_receipts_empty_block_no_receipts() {
        let block = mock_block(1, &[]);
        assert!(validate_receipts(&[block], &[vec![]]).is_ok());
    }

    #[test]
    fn validate_receipts_orphan_receipt_rejected() {
        let tx1 = B256::repeat_byte(0x11);
        let orphan = B256::repeat_byte(0xff);
        let block = mock_block(1, &[tx1]);
        let receipts = vec![mock_receipt(1, orphan)];

        let err = validate_receipts(&[block], &[receipts]).unwrap_err();
        assert!(
            err.to_string().contains("unknown tx hash"),
            "expected orphan receipt error, got: {err}"
        );
    }

    #[test]
    fn validate_receipts_fewer_receipts_than_txs() {
        let tx1 = B256::repeat_byte(0x11);
        let tx2 = B256::repeat_byte(0x22);
        let block = mock_block(1, &[tx1, tx2]);
        // Only one receipt for a block with two txs
        let receipts = vec![mock_receipt(1, tx1)];

        let err = validate_receipts(&[block], &[receipts]).unwrap_err();
        assert!(
            err.to_string().contains("receipt count (1) != transaction count (2)"),
            "expected count mismatch error, got: {err}"
        );
    }

    #[test]
    fn validate_receipts_more_receipts_than_txs() {
        let tx1 = B256::repeat_byte(0x11);
        let block = mock_block(1, &[tx1]);
        // Two receipts for a block with one tx
        let receipts = vec![mock_receipt(1, tx1), mock_receipt(1, tx1)];

        let err = validate_receipts(&[block], &[receipts]).unwrap_err();
        assert!(
            err.to_string().contains("receipt count (2) != transaction count (1)"),
            "expected count mismatch error, got: {err}"
        );
    }

    #[test]
    fn validate_receipts_batch_length_mismatch() {
        let block = mock_block(1, &[]);
        // Two receipt groups for one block
        let err = validate_receipts(&[block], &[vec![], vec![]]).unwrap_err();
        assert!(
            err.to_string().contains("batch length mismatch"),
            "expected batch length mismatch error, got: {err}"
        );
    }

    #[test]
    fn enrich_multi_block_batch() {
        let mut txs = vec![
            make_tx(10, 0),
            make_tx(10, 1),
            make_tx(11, 0),
        ];
        let receipts = vec![
            make_receipt(10, 0, 21000, Some(vec![0x01; 20])),
            make_receipt(10, 1, 42000, None),
            make_receipt(11, 0, 63000, Some(vec![0x02; 20])),
        ];

        enrich_txs_from_receipts(&mut txs, &receipts);

        assert_eq!(txs[0].gas_used, Some(21000));
        assert_eq!(txs[0].fee_payer, Some(vec![0x01; 20]));
        assert_eq!(txs[1].gas_used, Some(42000));
        assert_eq!(txs[1].fee_payer, None);
        assert_eq!(txs[2].gas_used, Some(63000));
        assert_eq!(txs[2].fee_payer, Some(vec![0x02; 20]));
    }
}

pub fn decode_receipt(receipt: &Receipt, block_timestamp: DateTime<Utc>) -> ReceiptRow {
    ReceiptRow {
        block_num: receipt.block_number().unwrap_or(0) as i64,
        block_timestamp,
        tx_idx: receipt.transaction_index().unwrap_or(0) as i32,
        tx_hash: receipt.transaction_hash().as_slice().to_vec(),
        from: receipt.from().as_slice().to_vec(),
        to: receipt.to().map(|a| a.as_slice().to_vec()),
        contract_address: receipt.contract_address().map(|a| a.as_slice().to_vec()),
        gas_used: receipt.gas_used() as i64,
        cumulative_gas_used: receipt.cumulative_gas_used() as i64,
        effective_gas_price: Some(receipt.effective_gas_price().to_string()),
        status: if receipt.status() { Some(1) } else { Some(0) },
        fee_payer: Some(receipt.fee_payer.as_slice().to_vec()),
    }
}
