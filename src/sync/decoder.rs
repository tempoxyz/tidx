use alloy::consensus::transaction::Recovered;
use alloy::consensus::{BlockHeader as _, Transaction as TransactionTrait, Typed2718};
use alloy::network::{ReceiptResponse, TransactionResponse};
use alloy::primitives::B256;
use chrono::{DateTime, TimeZone, Utc};
use tempo_alloy::primitives::transaction::SignatureType;

use crate::tempo::{Block, Log, Receipt, TempoTxEnvelope, Transaction};
use crate::types::{BlockRow, LogRow, ReceiptRow, TxRow};

use tempo_primitives::TempoConsensusContext;

/// TIP-1031: extract the 32-byte ed25519 proposer pubkey from an optional
/// consensus context. Returns `None` for pre-fork blocks where the header
/// carries no consensus context.
fn extract_consensus_proposer(ctx: Option<&TempoConsensusContext>) -> Option<Vec<u8>> {
    ctx.map(|c| B256::from(&c.proposer).0.to_vec())
}

pub fn timestamp_from_secs(secs: u64) -> DateTime<Utc> {
    Utc.timestamp_opt(secs as i64, 0)
        .single()
        .unwrap_or_else(|| Utc.timestamp_opt(0, 0).single().unwrap())
}

pub fn decode_block(block: &Block) -> BlockRow {
    let header = &block.header;
    let timestamp_secs = header.timestamp();
    let timestamp = timestamp_from_secs(timestamp_secs);
    let timestamp_ms = (timestamp_secs * 1000) as i64;

    let consensus_proposer = extract_consensus_proposer(header.consensus_context.as_ref());
    crate::metrics::record_block_consensus_context(consensus_proposer.is_some());

    BlockRow {
        num: header.number() as i64,
        hash: header.hash.as_slice().to_vec(),
        parent_hash: header.parent_hash().as_slice().to_vec(),
        timestamp,
        timestamp_ms,
        gas_limit: header.gas_limit() as i64,
        gas_used: header.gas_used() as i64,
        miner: header.beneficiary().as_slice().to_vec(),
        extra_data: Some(header.extra_data().to_vec()),
        consensus_proposer,
    }
}

pub fn decode_transaction(tx: &Transaction, block: &Block, idx: u32) -> TxRow {
    let block_timestamp = timestamp_from_secs(block.header.timestamp());
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
                tempo_tx.valid_before.map(|v| v.get() as i64),
                tempo_tx.valid_after.map(|v| v.get() as i64),
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
        block_num: block.header.number() as i64,
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
        max_priority_fee_per_gas: inner
            .max_priority_fee_per_gas()
            .map_or("0".into(), |v| v.to_string()),
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
        is_virtual_forward: false,
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
            tx.fee_payer.clone_from(&r.fee_payer);
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempo_primitives::ed25519::PublicKey;

    /// RFC 8032 ed25519 test vector #1 — known-valid 32-byte verification key.
    /// Hardcoded as bytes (rather than constructed via `PublicKey::from_seed`)
    /// because that constructor is gated on the `arbitrary` feature in
    /// tempo-primitives, which we don't enable from tidx.
    const RFC8032_TEST_VECTOR_1_PUBKEY: [u8; 32] = [
        0xd7, 0x5a, 0x98, 0x01, 0x82, 0xb1, 0x0a, 0xb7, 0xd5, 0x4b, 0xfe, 0xd3,
        0xc9, 0x64, 0x07, 0x3a, 0x0e, 0xe1, 0x72, 0xf3, 0xda, 0xa6, 0x23, 0x25,
        0xaf, 0x02, 0x1a, 0x68, 0xf7, 0x07, 0x51, 0x1a,
    ];

    #[test]
    fn extract_consensus_proposer_none_when_pre_fork() {
        assert_eq!(extract_consensus_proposer(None), None);
    }

    #[test]
    fn extract_consensus_proposer_returns_raw_32_bytes_when_present() {
        let proposer: PublicKey = B256::from(RFC8032_TEST_VECTOR_1_PUBKEY)
            .try_into()
            .expect("valid ed25519 pubkey from RFC 8032 test vector 1");
        let ctx = TempoConsensusContext {
            epoch: 7,
            view: 42,
            parent_view: 41,
            proposer,
        };
        let out = extract_consensus_proposer(Some(&ctx)).expect("some");
        assert_eq!(out.len(), 32);
        assert_eq!(out.as_slice(), RFC8032_TEST_VECTOR_1_PUBKEY.as_slice());
    }

    fn make_tx(block_num: i64, idx: i32) -> TxRow {
        TxRow {
            block_num,
            idx,
            ..Default::default()
        }
    }

    fn make_receipt(
        block_num: i64,
        tx_idx: i32,
        gas_used: i64,
        fee_payer: Option<Vec<u8>>,
    ) -> ReceiptRow {
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

    #[test]
    fn enrich_multi_block_batch() {
        let mut txs = vec![make_tx(10, 0), make_tx(10, 1), make_tx(11, 0)];
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
