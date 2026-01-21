use alloy::consensus::transaction::Recovered;
use alloy::consensus::{Transaction as TransactionTrait, Typed2718};
use alloy::network::{ReceiptResponse, TransactionResponse};
use chrono::{DateTime, TimeZone, Utc};
use tempo_alloy::primitives::transaction::SignatureType;

use crate::tempo::{Block, Log, Receipt, Transaction, TempoTxEnvelope};
use crate::types::{BlockRow, LogRow, ReceiptRow, TxRow};

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
    let selector = log.topics().first().map(|s| s.as_slice().to_vec());
    let topics: Vec<Vec<u8>> = log.topics().iter().map(|t| t.as_slice().to_vec()).collect();

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
        topics,
        data: log.data().data.to_vec(),
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
