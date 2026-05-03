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

use crate::types::LogRow;
use alloy::{
    primitives::{Address, B256, U256},
    sol,
    sol_types::SolEvent,
};
use std::collections::{HashMap, HashSet};
use tempo_primitives::TempoAddressExt;

sol! {
    event Transfer(address indexed from, address indexed to, uint256 amount);
}

type BucketKey = (Address, Address, U256);

#[derive(Debug, Clone, Copy)]
struct ParsedTransfer {
    from: Address,
    to: Address,
    amount: U256,
}

#[derive(Debug, Clone, Copy)]
struct TransferCandidate {
    idx_in_logs: usize,
    log_idx: i32,
    token: Address,
    from: Address,
    to: Address,
    amount: U256,
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
    let mut tx_groups: HashMap<B256, Vec<TransferCandidate>> = HashMap::new();

    for (idx, log) in logs.iter().enumerate() {
        let Ok(tx_hash) = B256::try_from(log.tx_hash.as_slice()) else {
            continue;
        };
        let Some(parsed) = parse_transfer_candidate(log) else {
            continue;
        };
        let Ok(token_bytes) = <[u8; 20]>::try_from(log.address.as_slice()) else {
            continue;
        };

        tx_groups
            .entry(tx_hash)
            .or_default()
            .push(TransferCandidate {
                idx_in_logs: idx,
                log_idx: log.log_idx,
                token: Address::from(token_bytes),
                from: parsed.from,
                to: parsed.to,
                amount: parsed.amount,
                is_attribution: parsed.to.is_virtual(),
                is_forward: parsed.from.is_virtual(),
            });
    }

    for candidates in tx_groups.values_mut() {
        candidates.sort_by_key(|c| c.log_idx);

        let mut attribution_counts: HashMap<BucketKey, usize> = HashMap::new();
        let mut forward_counts: HashMap<BucketKey, usize> = HashMap::new();
        let mut forward_destinations: HashMap<BucketKey, HashSet<Address>> = HashMap::new();

        for candidate in candidates.iter() {
            if candidate.is_attribution {
                *attribution_counts
                    .entry((candidate.token, candidate.to, candidate.amount))
                    .or_default() += 1;
            }
            if candidate.is_forward {
                let key = (candidate.token, candidate.from, candidate.amount);
                *forward_counts.entry(key).or_default() += 1;
                forward_destinations
                    .entry(key)
                    .or_default()
                    .insert(candidate.to);
            }
        }

        let mut pending: HashMap<BucketKey, Vec<usize>> = HashMap::new();
        for candidate in candidates.iter() {
            if candidate.is_attribution {
                let key = (candidate.token, candidate.to, candidate.amount);
                let forward_count = forward_counts.get(&key).copied().unwrap_or(0);
                let forward_dests = forward_destinations
                    .get(&key)
                    .map(HashSet::len)
                    .unwrap_or(0);
                if forward_count == 0 || forward_dests != 1 {
                    continue;
                }
                pending.entry(key).or_default().push(candidate.idx_in_logs);
                continue;
            }

            if candidate.is_forward {
                let key = (candidate.token, candidate.from, candidate.amount);
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
                    && !queue.is_empty()
                {
                    queue.remove(0);
                    marks[candidate.idx_in_logs] = true;
                }
            }
        }
    }

    marks
}

/// Decode a 32-byte ABI-encoded address topic into an `Address`.
/// Returns `None` if the slice is not exactly 32 bytes or the 12-byte
/// zero-padding is violated.
fn topic_to_address(topic: Option<&[u8]>) -> Option<Address> {
    let bytes = topic?;
    (bytes.len() == 32 && bytes[..12] == [0u8; 12]).then(|| Address::from_slice(&bytes[12..]))
}

fn parse_transfer_candidate(log: &LogRow) -> Option<ParsedTransfer> {
    if log.topic0.as_deref()? != Transfer::SIGNATURE_HASH.as_slice() {
        return None;
    }

    Some(ParsedTransfer {
        from: topic_to_address(log.topic1.as_deref())?,
        to: topic_to_address(log.topic2.as_deref())?,
        amount: B256::try_from(log.data.as_slice()).ok()?.into(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::FixedBytes;
    use tempo_alloy::contracts::precompiles::PATH_USD_ADDRESS;

    struct Fx {
        vaddr: Address,
        sender: Address,
        master: Address,
        tx_hash: B256,
        token: Address,
        amount: U256,
    }

    impl Fx {
        fn random() -> Self {
            Fx {
                vaddr: Address::new_virtual(FixedBytes::random(), FixedBytes::random()),
                sender: Address::random(),
                master: Address::random(),
                tx_hash: B256::random(),
                token: PATH_USD_ADDRESS,
                amount: U256::random(),
            }
        }
    }

    fn make_transfer_log(
        tx_hash: B256,
        token: Address,
        from: Address,
        to: Address,
        amount: U256,
        log_idx: i32,
    ) -> LogRow {
        LogRow {
            block_num: 1,
            block_timestamp: chrono::Utc::now(),
            log_idx,
            tx_idx: 0,
            tx_hash: tx_hash.to_vec(),
            address: token.to_vec(),
            selector: Some(Transfer::SIGNATURE_HASH.to_vec()),
            topic0: Some(Transfer::SIGNATURE_HASH.to_vec()),
            topic1: Some(from.into_word().to_vec()),
            topic2: Some(to.into_word().to_vec()),
            topic3: None,
            data: amount.to_be_bytes::<32>().to_vec(),
            is_virtual_forward: false,
        }
    }

    #[test]
    fn test_mark_virtual_forward_pair() {
        let f = Fx::random();
        // Use a realistic TIP-20 token address rather than the default test token.
        let token = Address::from([
            0x20, 0xc0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ]);

        let logs = vec![
            // Hop 1: sender → virtualAddress
            make_transfer_log(f.tx_hash, token, f.sender, f.vaddr, f.amount, 0),
            // Hop 2: virtualAddress → master
            make_transfer_log(f.tx_hash, token, f.vaddr, f.master, f.amount, 1),
        ];

        assert_eq!(mark_virtual_forward_hops(&logs), vec![false, true]);
    }

    #[test]
    fn test_normal_transfers_not_marked() {
        let f = Fx::random();

        let logs = vec![
            make_transfer_log(f.tx_hash, f.token, f.sender, f.master, f.amount, 0),
            make_transfer_log(f.tx_hash, f.token, f.master, f.sender, f.amount, 1),
        ];

        assert_eq!(mark_virtual_forward_hops(&logs), vec![false, false]);
    }

    #[test]
    fn test_different_amounts_not_marked() {
        let f = Fx::random();
        let amount1 = U256::from(100u64);
        let amount2 = U256::from(200u64);

        let logs = vec![
            make_transfer_log(f.tx_hash, f.token, f.sender, f.vaddr, amount1, 0),
            make_transfer_log(f.tx_hash, f.token, f.vaddr, f.master, amount2, 1),
        ];

        assert_eq!(mark_virtual_forward_hops(&logs), vec![false, false]);
    }

    #[test]
    fn test_different_tx_not_marked() {
        let f = Fx::random();

        let logs = vec![
            make_transfer_log(
                B256::repeat_byte(0x11),
                f.token,
                f.sender,
                f.vaddr,
                f.amount,
                0,
            ),
            make_transfer_log(
                B256::repeat_byte(0x22),
                f.token,
                f.vaddr,
                f.master,
                f.amount,
                1,
            ),
        ];

        assert_eq!(mark_virtual_forward_hops(&logs), vec![false, false]);
    }

    #[test]
    fn test_empty_logs() {
        let empty: Vec<bool> = vec![];
        assert_eq!(mark_virtual_forward_hops(&[]), empty);
    }

    #[test]
    fn test_single_log() {
        let log = make_transfer_log(
            B256::random(),
            Address::random(),
            Address::random(),
            Address::random(),
            U256::ZERO,
            0,
        );
        assert_eq!(mark_virtual_forward_hops(&[log]), vec![false]);
    }

    #[test]
    fn test_intervening_event_still_marks_forward_hop() {
        let f = Fx::random();
        let other = Address::random();

        let mut approval_like = make_transfer_log(f.tx_hash, f.token, f.sender, other, f.amount, 1);
        approval_like.topic0 = Some(vec![0x99; 32]);
        approval_like.selector = Some(vec![0x99; 32]);

        let logs = vec![
            make_transfer_log(f.tx_hash, f.token, f.sender, f.vaddr, f.amount, 0),
            approval_like,
            make_transfer_log(f.tx_hash, f.token, f.vaddr, f.master, f.amount, 2),
        ];

        assert_eq!(mark_virtual_forward_hops(&logs), vec![false, false, true]);
    }

    #[test]
    fn test_ambiguous_multiple_forward_destinations_marks_none() {
        let f = Fx::random();
        let other = Address::random();

        let logs = vec![
            make_transfer_log(f.tx_hash, f.token, f.sender, f.vaddr, f.amount, 0),
            make_transfer_log(f.tx_hash, f.token, f.vaddr, other, f.amount, 1),
            make_transfer_log(f.tx_hash, f.token, f.vaddr, f.master, f.amount, 2),
        ];

        assert_eq!(mark_virtual_forward_hops(&logs), vec![false, false, false]);
    }

    #[test]
    fn test_multiple_matching_pairs_same_tx_marks_both_forwards() {
        let f = Fx::random();
        let sender2 = Address::random();

        let logs = vec![
            make_transfer_log(f.tx_hash, f.token, f.sender, f.vaddr, f.amount, 0),
            make_transfer_log(f.tx_hash, f.token, f.vaddr, f.master, f.amount, 1),
            make_transfer_log(f.tx_hash, f.token, sender2, f.vaddr, f.amount, 2),
            make_transfer_log(f.tx_hash, f.token, f.vaddr, f.master, f.amount, 3),
        ];

        assert_eq!(
            mark_virtual_forward_hops(&logs),
            vec![false, true, false, true]
        );
    }

    #[test]
    fn test_multiple_attributions_before_multiple_forwards_marks_fifo() {
        let f = Fx::random();
        let sender2 = Address::random();

        let logs = vec![
            make_transfer_log(f.tx_hash, f.token, f.sender, f.vaddr, f.amount, 0),
            make_transfer_log(f.tx_hash, f.token, sender2, f.vaddr, f.amount, 1),
            make_transfer_log(f.tx_hash, f.token, f.vaddr, f.master, f.amount, 2),
            make_transfer_log(f.tx_hash, f.token, f.vaddr, f.master, f.amount, 3),
        ];

        assert_eq!(
            mark_virtual_forward_hops(&logs),
            vec![false, false, true, true]
        );
    }

    #[test]
    fn test_multiple_tokens_same_tx_are_isolated() {
        let f = Fx::random();
        let token_b = Address::repeat_byte(0x21);

        let logs = vec![
            make_transfer_log(f.tx_hash, f.token, f.sender, f.vaddr, f.amount, 0),
            make_transfer_log(f.tx_hash, token_b, f.sender, f.vaddr, f.amount, 1),
            make_transfer_log(f.tx_hash, token_b, f.vaddr, f.master, f.amount, 2),
            make_transfer_log(f.tx_hash, f.token, f.vaddr, f.master, f.amount, 3),
        ];

        assert_eq!(
            mark_virtual_forward_hops(&logs),
            vec![false, false, true, true]
        );
    }

    #[test]
    fn test_mint_like_zero_sender_still_marks_forward_hop() {
        let f = Fx::random();

        let logs = vec![
            make_transfer_log(f.tx_hash, f.token, Address::ZERO, f.vaddr, f.amount, 0),
            make_transfer_log(f.tx_hash, f.token, f.vaddr, f.master, f.amount, 1),
        ];

        assert_eq!(mark_virtual_forward_hops(&logs), vec![false, true]);
    }

    #[test]
    fn test_mint_with_intervening_non_transfer_events_still_marks_forward_hop() {
        let f = Fx::random();

        let mut transfer_with_memo_like =
            make_transfer_log(f.tx_hash, f.token, Address::ZERO, f.vaddr, f.amount, 1);
        transfer_with_memo_like.topic0 = Some(vec![0x77; 32]);
        transfer_with_memo_like.selector = Some(vec![0x77; 32]);

        let mut mint_like =
            make_transfer_log(f.tx_hash, f.token, Address::ZERO, f.vaddr, f.amount, 2);
        mint_like.topic0 = Some(vec![0x88; 32]);
        mint_like.selector = Some(vec![0x88; 32]);

        let logs = vec![
            make_transfer_log(f.tx_hash, f.token, Address::ZERO, f.vaddr, f.amount, 0),
            transfer_with_memo_like,
            mint_like,
            make_transfer_log(f.tx_hash, f.token, f.vaddr, f.master, f.amount, 3),
        ];

        assert_eq!(
            mark_virtual_forward_hops(&logs),
            vec![false, false, false, true]
        );
    }

    #[test]
    fn test_self_forwarding_marks_only_second_hop() {
        let f = Fx::random();

        let logs = vec![
            make_transfer_log(f.tx_hash, f.token, f.master, f.vaddr, f.amount, 0),
            make_transfer_log(f.tx_hash, f.token, f.vaddr, f.master, f.amount, 1),
        ];

        assert_eq!(mark_virtual_forward_hops(&logs), vec![false, true]);
    }

    #[test]
    fn test_malformed_topic_padding_is_rejected() {
        let f = Fx::random();

        let mut malformed = make_transfer_log(f.tx_hash, f.token, f.sender, f.vaddr, f.amount, 0);
        malformed.topic1 = Some({
            let mut topic = [0x01u8; 12].to_vec();
            topic.extend_from_slice(f.sender.as_slice());
            topic
        });

        let logs = vec![
            malformed,
            make_transfer_log(f.tx_hash, f.token, f.vaddr, f.master, f.amount, 1),
        ];

        assert_eq!(mark_virtual_forward_hops(&logs), vec![false, false]);
    }

    #[test]
    fn test_malformed_data_length_is_rejected() {
        let f = Fx::random();

        let mut log1 = make_transfer_log(f.tx_hash, f.token, f.sender, f.vaddr, f.amount, 0);
        log1.data = vec![0u8; 31]; // truncate to 31 bytes — must be rejected
        let log2 = make_transfer_log(f.tx_hash, f.token, f.vaddr, f.master, f.amount, 1);
        let logs = vec![log1, log2];

        assert_eq!(mark_virtual_forward_hops(&logs), vec![false, false]);
    }

    #[test]
    fn test_reverse_order_does_not_mark() {
        let f = Fx::random();

        let logs = vec![
            make_transfer_log(f.tx_hash, f.token, f.vaddr, f.master, f.amount, 0),
            make_transfer_log(f.tx_hash, f.token, f.sender, f.vaddr, f.amount, 1),
        ];

        assert_eq!(mark_virtual_forward_hops(&logs), vec![false, false]);
    }

    #[test]
    fn test_missing_tx_hash_is_rejected() {
        let f = Fx::random();

        let mut log1 = make_transfer_log(f.tx_hash, f.token, f.sender, f.vaddr, f.amount, 0);
        let mut log2 = make_transfer_log(f.tx_hash, f.token, f.vaddr, f.master, f.amount, 1);
        log1.tx_hash = vec![];
        log2.tx_hash = vec![];
        let logs = vec![log1, log2];

        assert_eq!(mark_virtual_forward_hops(&logs), vec![false, false]);
    }

    #[test]
    fn test_count_mismatch_fails_closed() {
        let f = Fx::random();
        let sender2 = Address::random();

        let logs = vec![
            make_transfer_log(f.tx_hash, f.token, f.sender, f.vaddr, f.amount, 0),
            make_transfer_log(f.tx_hash, f.token, sender2, f.vaddr, f.amount, 1),
            make_transfer_log(f.tx_hash, f.token, f.vaddr, f.master, f.amount, 2),
        ];

        assert_eq!(mark_virtual_forward_hops(&logs), vec![false, false, false]);
    }
}
