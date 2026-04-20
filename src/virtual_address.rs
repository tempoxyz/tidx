//! TIP-1022: Virtual Address detection and decoding.
//!
//! Virtual addresses are 20-byte addresses with a reserved format:
//!   [4-byte masterId] [10-byte VIRTUAL_MAGIC] [6-byte userTag]
//!
//! When bytes [4:14] equal VIRTUAL_MAGIC (0xFDFDFDFDFDFDFDFDFDFD),
//! the address is virtual. TIP-20 transfers to virtual addresses emit
//! two Transfer events: one to the virtual address (attribution) and one
//! from the virtual address to the master (forwarding).
//!
//! This module detects virtual addresses and identifies the two-hop
//! Transfer event pairs so the indexer can annotate them.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

use alloy::primitives::Address;
use tempo_primitives::TempoAddressExt;

fn address_from_slice(address: &[u8]) -> Option<Address> {
    Some(Address::from(<[u8; 20]>::try_from(address).ok()?))
}

/// Check whether a 20-byte address is a TIP-1022 virtual address.
pub fn is_virtual_address(address: &[u8]) -> bool {
    address_from_slice(address).is_some_and(|addr| addr.is_virtual())
}

/// Decoded components of a virtual address.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VirtualAddressParts {
    /// 4-byte master identifier (registry lookup key).
    pub master_id: [u8; 4],
    /// 6-byte opaque user tag for deposit attribution.
    pub user_tag: [u8; 6],
}

/// Decode a virtual address into its components.
/// Returns `None` if the address is not virtual.
pub fn decode_virtual_address(address: &[u8]) -> Option<VirtualAddressParts> {
    let (master_id, user_tag) = address_from_slice(address)?.decode_virtual()?;
    Some(VirtualAddressParts {
        master_id: *master_id,
        user_tag: *user_tag,
    })
}

/// Transfer event topic0 selector: keccak256("Transfer(address,address,uint256)")
const TRANSFER_SELECTOR: [u8; 32] = {
    // 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef
    let mut b = [0u8; 32];
    b[0] = 0xdd;
    b[1] = 0xf2;
    b[2] = 0x52;
    b[3] = 0xad;
    b[4] = 0x1b;
    b[5] = 0xe2;
    b[6] = 0xc8;
    b[7] = 0x9b;
    b[8] = 0x69;
    b[9] = 0xc2;
    b[10] = 0xb0;
    b[11] = 0x68;
    b[12] = 0xfc;
    b[13] = 0x37;
    b[14] = 0x8d;
    b[15] = 0xaa;
    b[16] = 0x95;
    b[17] = 0x2b;
    b[18] = 0xa7;
    b[19] = 0xf1;
    b[20] = 0x63;
    b[21] = 0xc4;
    b[22] = 0xa1;
    b[23] = 0x16;
    b[24] = 0x28;
    b[25] = 0xf5;
    b[26] = 0x5a;
    b[27] = 0x4d;
    b[28] = 0xf5;
    b[29] = 0x23;
    b[30] = 0xb3;
    b[31] = 0xef;
    b
};

use crate::types::LogRow;

type TxHash = Vec<u8>;
type BucketKey = (Vec<u8>, [u8; 20], [u8; 32]);

#[derive(Debug, Clone, Copy)]
struct TransferCandidate {
    idx_in_logs: usize,
    log_idx: i32,
    from: [u8; 20],
    to: [u8; 20],
    is_attribution: bool,
    is_forward: bool,
}

/// Identifies which log rows are part of a TIP-1022 virtual forwarding pair.
///
/// A virtual forwarding pair is made of:
/// 1. `Transfer(sender, virtualAddress, amount)` — the attribution hop
/// 2. `Transfer(virtualAddress, masterAddress, amount)` — the forwarding hop
///
/// Both events have the same tx_hash, the same token (address), and the same amount.
/// The `to` of the first matches the `from` of the second, and that address is virtual.
///
/// Returns a vec of booleans parallel to `logs`, where `true` means the log is the
/// forwarding hop. This implementation does not require the two transfer logs to be
/// adjacent; unrelated events may appear between them.
///
/// The matcher is intentionally conservative:
/// - it only considers strictly well-formed `Transfer` logs (32-byte padded topics,
///   32-byte amount data)
/// - it matches only within a single transaction
/// - it fails closed when a `(token, virtualAddr, amount)` bucket forwards to more
///   than one destination in the same transaction
pub fn mark_virtual_forward_hops(logs: &[LogRow]) -> Vec<bool> {
    let mut marks = vec![false; logs.len()];
    let mut tx_groups: BTreeMap<TxHash, Vec<TransferCandidate>> = BTreeMap::new();

    for (idx, log) in logs.iter().enumerate() {
        let Some((from, to, _amount)) = parse_transfer_candidate(log) else {
            continue;
        };
        if log.tx_hash.is_empty() {
            continue;
        }

        tx_groups
            .entry(log.tx_hash.clone())
            .or_default()
            .push(TransferCandidate {
                idx_in_logs: idx,
                log_idx: log.log_idx,
                from,
                to,
                is_attribution: Address::from(to).is_virtual(),
                is_forward: Address::from(from).is_virtual(),
            });
    }

    for candidates in tx_groups.values_mut() {
        candidates.sort_by_key(|c| c.log_idx);

        let mut attribution_counts: HashMap<BucketKey, usize> = HashMap::new();
        let mut forward_counts: HashMap<BucketKey, usize> = HashMap::new();
        let mut forward_destinations: HashMap<BucketKey, HashSet<[u8; 20]>> = HashMap::new();

        for candidate in candidates.iter() {
            let log = &logs[candidate.idx_in_logs];
            let Some(amount) = extract_u256_data(&log.data) else {
                continue;
            };
            if candidate.is_attribution {
                *attribution_counts
                    .entry((log.address.clone(), candidate.to, amount))
                    .or_default() += 1;
            }
            if candidate.is_forward {
                let key = (log.address.clone(), candidate.from, amount);
                *forward_counts.entry(key.clone()).or_default() += 1;
                forward_destinations
                    .entry(key)
                    .or_default()
                    .insert(candidate.to);
            }
        }

        let mut pending: HashMap<BucketKey, VecDeque<usize>> = HashMap::new();

        for candidate in candidates.iter() {
            let log = &logs[candidate.idx_in_logs];
            let Some(amount) = extract_u256_data(&log.data) else {
                continue;
            };

            if candidate.is_attribution {
                let key = (log.address.clone(), candidate.to, amount);
                let forward_count = forward_counts.get(&key).copied().unwrap_or(0);
                let forward_dests = forward_destinations
                    .get(&key)
                    .map(HashSet::len)
                    .unwrap_or(0);
                if forward_count == 0 || forward_dests != 1 {
                    continue;
                }
                pending
                    .entry(key)
                    .or_default()
                    .push_back(candidate.idx_in_logs);
                continue;
            }

            if candidate.is_forward {
                let key = (log.address.clone(), candidate.from, amount);
                let attr_count = attribution_counts.get(&key).copied().unwrap_or(0);
                let forward_count = forward_counts.get(&key).copied().unwrap_or(0);
                let forward_dests = forward_destinations
                    .get(&key)
                    .map(HashSet::len)
                    .unwrap_or(0);

                if attr_count == 0 || attr_count != forward_count || forward_dests != 1 {
                    continue;
                }

                if let Some(queue) = pending.get_mut(&key)
                    && queue.pop_front().is_some()
                {
                    marks[candidate.idx_in_logs] = true;
                }
            }
        }
    }

    marks
}

fn is_transfer_log(log: &LogRow) -> bool {
    matches!(&log.topic0, Some(t) if t.as_slice() == TRANSFER_SELECTOR)
}

fn parse_transfer_candidate(log: &LogRow) -> Option<([u8; 20], [u8; 20], [u8; 32])> {
    if !is_transfer_log(log) {
        return None;
    }
    let from = extract_padded_address_from_topic(&log.topic1)?;
    let to = extract_padded_address_from_topic(&log.topic2)?;
    let amount = extract_u256_data(&log.data)?;
    Some((from, to, amount))
}

fn extract_padded_address_from_topic(topic: &Option<Vec<u8>>) -> Option<[u8; 20]> {
    let t = topic.as_ref()?;
    if t.len() != 32 || t[..12].iter().any(|b| *b != 0) {
        return None;
    }
    let mut addr = [0u8; 20];
    addr.copy_from_slice(&t[12..32]);
    Some(addr)
}

fn extract_u256_data(data: &[u8]) -> Option<[u8; 32]> {
    if data.len() != 32 {
        return None;
    }
    let mut amount = [0u8; 32];
    amount.copy_from_slice(data);
    Some(amount)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_virtual_address(master_id: [u8; 4], user_tag: [u8; 6]) -> [u8; 20] {
        Address::new_virtual(master_id.into(), user_tag.into()).into_array()
    }

    #[test]
    fn test_is_virtual_address() {
        let vaddr = make_virtual_address(
            [0x07, 0xA3, 0xB1, 0xC2],
            [0xD4, 0xE5, 0xA7, 0xC3, 0xF1, 0x9E],
        );
        assert!(is_virtual_address(&vaddr));
    }

    #[test]
    fn test_not_virtual_address() {
        let normal = [0u8; 20];
        assert!(!is_virtual_address(&normal));

        let almost = {
            let mut a = [0xFD; 20];
            a[4..14].copy_from_slice(&<Address as TempoAddressExt>::VIRTUAL_MAGIC);
            a[4] = 0x00; // break the magic
            a
        };
        assert!(!is_virtual_address(&almost));
    }

    #[test]
    fn test_decode_virtual_address() {
        let master_id = [0x07, 0xA3, 0xB1, 0xC2];
        let user_tag = [0xD4, 0xE5, 0xA7, 0xC3, 0xF1, 0x9E];
        let vaddr = make_virtual_address(master_id, user_tag);

        let parts = decode_virtual_address(&vaddr).unwrap();
        assert_eq!(parts.master_id, master_id);
        assert_eq!(parts.user_tag, user_tag);
    }

    #[test]
    fn test_decode_normal_address_returns_none() {
        assert!(decode_virtual_address(&[0u8; 20]).is_none());
    }

    fn pad_address(addr: &[u8; 20]) -> Vec<u8> {
        let mut topic = vec![0u8; 32];
        topic[12..32].copy_from_slice(addr);
        topic
    }

    fn pad_address_with_prefix(addr: &[u8; 20], prefix: [u8; 12]) -> Vec<u8> {
        let mut topic = prefix.to_vec();
        topic.extend_from_slice(addr);
        topic
    }

    fn make_transfer_log(
        tx_hash: Vec<u8>,
        token: Vec<u8>,
        from: &[u8; 20],
        to: &[u8; 20],
        amount: Vec<u8>,
        log_idx: i32,
    ) -> LogRow {
        LogRow {
            block_num: 1,
            block_timestamp: chrono::Utc::now(),
            log_idx,
            tx_idx: 0,
            tx_hash,
            address: token,
            selector: Some(TRANSFER_SELECTOR.to_vec()),
            topic0: Some(TRANSFER_SELECTOR.to_vec()),
            topic1: Some(pad_address(from)),
            topic2: Some(pad_address(to)),
            topic3: None,
            data: amount,
            is_virtual_forward: false,
        }
    }

    #[test]
    fn test_mark_virtual_forward_pair() {
        let sender = [0xAA; 20];
        let master = [0xBB; 20];
        let vaddr = make_virtual_address(
            [0x01, 0x02, 0x03, 0x04],
            [0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F],
        );
        let tx_hash = vec![0x11; 32];
        let token = vec![
            0x20, 0xc0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ];
        let amount = vec![0u8; 32]; // 0 amount for simplicity

        let logs = vec![
            // Hop 1: sender → virtualAddress
            make_transfer_log(
                tx_hash.clone(),
                token.clone(),
                &sender,
                &vaddr,
                amount.clone(),
                0,
            ),
            // Hop 2: virtualAddress → master
            make_transfer_log(
                tx_hash.clone(),
                token.clone(),
                &vaddr,
                &master,
                amount.clone(),
                1,
            ),
        ];

        let marks = mark_virtual_forward_hops(&logs);
        assert_eq!(marks, vec![false, true]);
    }

    #[test]
    fn test_normal_transfers_not_marked() {
        let a = [0xAA; 20];
        let b = [0xBB; 20];
        let tx_hash = vec![0x11; 32];
        let token = vec![0x20; 20];
        let amount = vec![0u8; 32];

        let logs = vec![
            make_transfer_log(tx_hash.clone(), token.clone(), &a, &b, amount.clone(), 0),
            make_transfer_log(tx_hash.clone(), token.clone(), &b, &a, amount.clone(), 1),
        ];

        let marks = mark_virtual_forward_hops(&logs);
        assert_eq!(marks, vec![false, false]);
    }

    #[test]
    fn test_different_amounts_not_marked() {
        let sender = [0xAA; 20];
        let master = [0xBB; 20];
        let vaddr = make_virtual_address(
            [0x01, 0x02, 0x03, 0x04],
            [0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F],
        );
        let tx_hash = vec![0x11; 32];
        let token = vec![0x20; 20];

        let mut amount1 = vec![0u8; 32];
        amount1[31] = 100;
        let mut amount2 = vec![0u8; 32];
        amount2[31] = 200;

        let logs = vec![
            make_transfer_log(tx_hash.clone(), token.clone(), &sender, &vaddr, amount1, 0),
            make_transfer_log(tx_hash.clone(), token.clone(), &vaddr, &master, amount2, 1),
        ];

        let marks = mark_virtual_forward_hops(&logs);
        assert_eq!(marks, vec![false, false]);
    }

    #[test]
    fn test_different_tx_not_marked() {
        let sender = [0xAA; 20];
        let master = [0xBB; 20];
        let vaddr = make_virtual_address(
            [0x01, 0x02, 0x03, 0x04],
            [0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F],
        );
        let token = vec![0x20; 20];
        let amount = vec![0u8; 32];

        let logs = vec![
            make_transfer_log(
                vec![0x11; 32],
                token.clone(),
                &sender,
                &vaddr,
                amount.clone(),
                0,
            ),
            make_transfer_log(
                vec![0x22; 32],
                token.clone(),
                &vaddr,
                &master,
                amount.clone(),
                1,
            ),
        ];

        let marks = mark_virtual_forward_hops(&logs);
        assert_eq!(marks, vec![false, false]);
    }

    #[test]
    fn test_empty_logs() {
        let empty: Vec<bool> = vec![];
        assert_eq!(mark_virtual_forward_hops(&[]), empty);
    }

    #[test]
    fn test_single_log() {
        let log = make_transfer_log(
            vec![0x11; 32],
            vec![0x20; 20],
            &[0xAA; 20],
            &[0xBB; 20],
            vec![0u8; 32],
            0,
        );
        assert_eq!(mark_virtual_forward_hops(&[log]), vec![false]);
    }

    #[test]
    fn test_intervening_event_still_marks_forward_hop() {
        let sender = [0xAA; 20];
        let master = [0xBB; 20];
        let other = [0xCC; 20];
        let vaddr = make_virtual_address(
            [0x01, 0x02, 0x03, 0x04],
            [0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F],
        );
        let tx_hash = vec![0x11; 32];
        let token = vec![0x20; 20];
        let amount = vec![0u8; 32];

        let mut approval_like = make_transfer_log(
            tx_hash.clone(),
            token.clone(),
            &sender,
            &other,
            amount.clone(),
            1,
        );
        approval_like.topic0 = Some(vec![0x99; 32]);
        approval_like.selector = Some(vec![0x99; 32]);

        let logs = vec![
            make_transfer_log(
                tx_hash.clone(),
                token.clone(),
                &sender,
                &vaddr,
                amount.clone(),
                0,
            ),
            approval_like,
            make_transfer_log(tx_hash, token, &vaddr, &master, amount, 2),
        ];

        let marks = mark_virtual_forward_hops(&logs);
        assert_eq!(marks, vec![false, false, true]);
    }

    #[test]
    fn test_ambiguous_multiple_forward_destinations_marks_none() {
        let sender = [0xAA; 20];
        let master = [0xBB; 20];
        let other = [0xCC; 20];
        let vaddr = make_virtual_address(
            [0x01, 0x02, 0x03, 0x04],
            [0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F],
        );
        let tx_hash = vec![0x11; 32];
        let token = vec![0x20; 20];
        let amount = vec![0u8; 32];

        let logs = vec![
            make_transfer_log(
                tx_hash.clone(),
                token.clone(),
                &sender,
                &vaddr,
                amount.clone(),
                0,
            ),
            make_transfer_log(
                tx_hash.clone(),
                token.clone(),
                &vaddr,
                &other,
                amount.clone(),
                1,
            ),
            make_transfer_log(tx_hash, token, &vaddr, &master, amount, 2),
        ];

        assert_eq!(mark_virtual_forward_hops(&logs), vec![false, false, false]);
    }

    #[test]
    fn test_multiple_matching_pairs_same_tx_marks_both_forwards() {
        let sender1 = [0xAA; 20];
        let sender2 = [0xAB; 20];
        let master = [0xBB; 20];
        let vaddr = make_virtual_address(
            [0x01, 0x02, 0x03, 0x04],
            [0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F],
        );
        let tx_hash = vec![0x11; 32];
        let token = vec![0x20; 20];
        let amount = vec![0u8; 32];

        let logs = vec![
            make_transfer_log(
                tx_hash.clone(),
                token.clone(),
                &sender1,
                &vaddr,
                amount.clone(),
                0,
            ),
            make_transfer_log(
                tx_hash.clone(),
                token.clone(),
                &vaddr,
                &master,
                amount.clone(),
                1,
            ),
            make_transfer_log(
                tx_hash.clone(),
                token.clone(),
                &sender2,
                &vaddr,
                amount.clone(),
                2,
            ),
            make_transfer_log(tx_hash, token, &vaddr, &master, amount, 3),
        ];

        assert_eq!(
            mark_virtual_forward_hops(&logs),
            vec![false, true, false, true]
        );
    }

    #[test]
    fn test_multiple_attributions_before_multiple_forwards_marks_fifo() {
        let sender1 = [0xAA; 20];
        let sender2 = [0xAB; 20];
        let master = [0xBB; 20];
        let vaddr = make_virtual_address(
            [0x01, 0x02, 0x03, 0x04],
            [0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F],
        );
        let tx_hash = vec![0x11; 32];
        let token = vec![0x20; 20];
        let amount = vec![0u8; 32];

        let logs = vec![
            make_transfer_log(
                tx_hash.clone(),
                token.clone(),
                &sender1,
                &vaddr,
                amount.clone(),
                0,
            ),
            make_transfer_log(
                tx_hash.clone(),
                token.clone(),
                &sender2,
                &vaddr,
                amount.clone(),
                1,
            ),
            make_transfer_log(
                tx_hash.clone(),
                token.clone(),
                &vaddr,
                &master,
                amount.clone(),
                2,
            ),
            make_transfer_log(tx_hash, token, &vaddr, &master, amount, 3),
        ];

        assert_eq!(
            mark_virtual_forward_hops(&logs),
            vec![false, false, true, true]
        );
    }

    #[test]
    fn test_multiple_tokens_same_tx_are_isolated() {
        let sender = [0xAA; 20];
        let master = [0xBB; 20];
        let vaddr = make_virtual_address(
            [0x01, 0x02, 0x03, 0x04],
            [0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F],
        );
        let tx_hash = vec![0x11; 32];
        let token_a = vec![0x20; 20];
        let token_b = vec![0x21; 20];
        let amount = vec![0u8; 32];

        let logs = vec![
            make_transfer_log(
                tx_hash.clone(),
                token_a.clone(),
                &sender,
                &vaddr,
                amount.clone(),
                0,
            ),
            make_transfer_log(
                tx_hash.clone(),
                token_b.clone(),
                &sender,
                &vaddr,
                amount.clone(),
                1,
            ),
            make_transfer_log(tx_hash.clone(), token_b, &vaddr, &master, amount.clone(), 2),
            make_transfer_log(tx_hash, token_a, &vaddr, &master, amount, 3),
        ];

        assert_eq!(
            mark_virtual_forward_hops(&logs),
            vec![false, false, true, true]
        );
    }

    #[test]
    fn test_mint_like_zero_sender_still_marks_forward_hop() {
        let zero = [0u8; 20];
        let master = [0xBB; 20];
        let vaddr = make_virtual_address(
            [0x01, 0x02, 0x03, 0x04],
            [0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F],
        );
        let tx_hash = vec![0x11; 32];
        let token = vec![0x20; 20];
        let amount = vec![0u8; 32];

        let logs = vec![
            make_transfer_log(
                tx_hash.clone(),
                token.clone(),
                &zero,
                &vaddr,
                amount.clone(),
                0,
            ),
            make_transfer_log(tx_hash, token, &vaddr, &master, amount, 1),
        ];

        assert_eq!(mark_virtual_forward_hops(&logs), vec![false, true]);
    }

    #[test]
    fn test_mint_with_intervening_non_transfer_events_still_marks_forward_hop() {
        let zero = [0u8; 20];
        let master = [0xBB; 20];
        let vaddr = make_virtual_address(
            [0x01, 0x02, 0x03, 0x04],
            [0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F],
        );
        let tx_hash = vec![0x11; 32];
        let token = vec![0x20; 20];
        let amount = vec![0u8; 32];

        let mut transfer_with_memo_like = make_transfer_log(
            tx_hash.clone(),
            token.clone(),
            &zero,
            &vaddr,
            amount.clone(),
            1,
        );
        transfer_with_memo_like.topic0 = Some(vec![0x77; 32]);
        transfer_with_memo_like.selector = Some(vec![0x77; 32]);

        let mut mint_like = make_transfer_log(
            tx_hash.clone(),
            token.clone(),
            &zero,
            &vaddr,
            amount.clone(),
            2,
        );
        mint_like.topic0 = Some(vec![0x88; 32]);
        mint_like.selector = Some(vec![0x88; 32]);

        let logs = vec![
            make_transfer_log(
                tx_hash.clone(),
                token.clone(),
                &zero,
                &vaddr,
                amount.clone(),
                0,
            ),
            transfer_with_memo_like,
            mint_like,
            make_transfer_log(tx_hash, token, &vaddr, &master, amount, 3),
        ];

        assert_eq!(mark_virtual_forward_hops(&logs), vec![false, false, false, true]);
    }

    #[test]
    fn test_self_forwarding_marks_only_second_hop() {
        let master = [0xBB; 20];
        let vaddr = make_virtual_address(
            [0x01, 0x02, 0x03, 0x04],
            [0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F],
        );
        let tx_hash = vec![0x11; 32];
        let token = vec![0x20; 20];
        let amount = vec![0u8; 32];

        let logs = vec![
            make_transfer_log(
                tx_hash.clone(),
                token.clone(),
                &master,
                &vaddr,
                amount.clone(),
                0,
            ),
            make_transfer_log(tx_hash, token, &vaddr, &master, amount, 1),
        ];

        assert_eq!(mark_virtual_forward_hops(&logs), vec![false, true]);
    }

    #[test]
    fn test_malformed_topic_padding_is_rejected() {
        let sender = [0xAA; 20];
        let master = [0xBB; 20];
        let vaddr = make_virtual_address(
            [0x01, 0x02, 0x03, 0x04],
            [0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F],
        );
        let tx_hash = vec![0x11; 32];
        let token = vec![0x20; 20];
        let amount = vec![0u8; 32];

        let mut malformed = make_transfer_log(
            tx_hash.clone(),
            token.clone(),
            &sender,
            &vaddr,
            amount.clone(),
            0,
        );
        malformed.topic1 = Some(pad_address_with_prefix(&sender, [0x01; 12]));

        let logs = vec![
            malformed,
            make_transfer_log(tx_hash, token, &vaddr, &master, amount, 1),
        ];

        assert_eq!(mark_virtual_forward_hops(&logs), vec![false, false]);
    }

    #[test]
    fn test_malformed_data_length_is_rejected() {
        let sender = [0xAA; 20];
        let master = [0xBB; 20];
        let vaddr = make_virtual_address(
            [0x01, 0x02, 0x03, 0x04],
            [0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F],
        );
        let tx_hash = vec![0x11; 32];
        let token = vec![0x20; 20];

        let logs = vec![
            make_transfer_log(
                tx_hash.clone(),
                token.clone(),
                &sender,
                &vaddr,
                vec![0u8; 31],
                0,
            ),
            make_transfer_log(tx_hash, token, &vaddr, &master, vec![0u8; 32], 1),
        ];

        assert_eq!(mark_virtual_forward_hops(&logs), vec![false, false]);
    }

    #[test]
    fn test_reverse_order_does_not_mark() {
        let sender = [0xAA; 20];
        let master = [0xBB; 20];
        let vaddr = make_virtual_address(
            [0x01, 0x02, 0x03, 0x04],
            [0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F],
        );
        let tx_hash = vec![0x11; 32];
        let token = vec![0x20; 20];
        let amount = vec![0u8; 32];

        let logs = vec![
            make_transfer_log(
                tx_hash.clone(),
                token.clone(),
                &vaddr,
                &master,
                amount.clone(),
                0,
            ),
            make_transfer_log(tx_hash, token, &sender, &vaddr, amount, 1),
        ];

        assert_eq!(mark_virtual_forward_hops(&logs), vec![false, false]);
    }

    #[test]
    fn test_missing_tx_hash_is_rejected() {
        let sender = [0xAA; 20];
        let master = [0xBB; 20];
        let vaddr = make_virtual_address(
            [0x01, 0x02, 0x03, 0x04],
            [0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F],
        );
        let token = vec![0x20; 20];
        let amount = vec![0u8; 32];

        let logs = vec![
            make_transfer_log(vec![], token.clone(), &sender, &vaddr, amount.clone(), 0),
            make_transfer_log(vec![], token, &vaddr, &master, amount, 1),
        ];

        assert_eq!(mark_virtual_forward_hops(&logs), vec![false, false]);
    }

    #[test]
    fn test_count_mismatch_fails_closed() {
        let sender1 = [0xAA; 20];
        let sender2 = [0xAB; 20];
        let master = [0xBB; 20];
        let vaddr = make_virtual_address(
            [0x01, 0x02, 0x03, 0x04],
            [0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F],
        );
        let tx_hash = vec![0x11; 32];
        let token = vec![0x20; 20];
        let amount = vec![0u8; 32];

        let logs = vec![
            make_transfer_log(
                tx_hash.clone(),
                token.clone(),
                &sender1,
                &vaddr,
                amount.clone(),
                0,
            ),
            make_transfer_log(
                tx_hash.clone(),
                token.clone(),
                &sender2,
                &vaddr,
                amount.clone(),
                1,
            ),
            make_transfer_log(tx_hash, token, &vaddr, &master, amount, 2),
        ];

        assert_eq!(mark_virtual_forward_hops(&logs), vec![false, false, false]);
    }
}
