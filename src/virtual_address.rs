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

/// 10-byte magic value identifying virtual addresses (bytes [4:14]).
const VIRTUAL_MAGIC: [u8; 10] = [0xFD; 10];

/// Check whether a 20-byte address is a TIP-1022 virtual address.
pub fn is_virtual_address(address: &[u8]) -> bool {
    address.len() == 20 && address[4..14] == VIRTUAL_MAGIC
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
    if !is_virtual_address(address) {
        return None;
    }
    let mut master_id = [0u8; 4];
    master_id.copy_from_slice(&address[0..4]);
    let mut user_tag = [0u8; 6];
    user_tag.copy_from_slice(&address[14..20]);
    Some(VirtualAddressParts {
        master_id,
        user_tag,
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

pub const VIRTUAL_DEPOSITS_VIEW: &str = "virtual_deposits";
pub const VIRTUAL_DEPOSITS_ENGINE: &str = "ReplacingMergeTree()";
pub const VIRTUAL_DEPOSITS_ORDER_BY: &[&str] = &[
    "virtual_address",
    "token",
    "block_num",
    "attribution_log_idx",
];

/// ClickHouse SELECT used to build a `virtual_deposits` view from indexed TIP-1022
/// Transfer log pairs. Each row represents the attribution hop annotated with the
/// resolved master address from the forwarding hop.
pub fn virtual_deposits_sql_clickhouse() -> String {
    format!(
        r#"WITH transfer_logs AS (
    SELECT
        block_num,
        block_timestamp,
        log_idx,
        tx_idx,
        concat('0x', lower(substring(tx_hash, 3))) AS tx_hash,
        concat('0x', lower(substring(address, 3))) AS token,
        concat('0x', lower(substring(topic1, 27))) AS transfer_from,
        concat('0x', lower(substring(topic2, 27))) AS transfer_to,
        reinterpretAsUInt256(reverse(unhex(substring(data, 3, 64)))) AS amount,
        is_virtual_forward
    FROM logs
    WHERE selector = '0x{TRANSFER_SELECTOR_HEX}'
      AND topic1 IS NOT NULL
      AND topic2 IS NOT NULL
      AND length(data) >= 66
),
first_hops AS (
    SELECT
        block_num,
        block_timestamp,
        log_idx AS attribution_log_idx,
        tx_idx,
        tx_hash,
        token,
        transfer_from AS sender,
        transfer_to AS virtual_address,
        amount,
        concat('0x', lower(substring(transfer_to, 3, 8))) AS master_id,
        concat('0x', lower(substring(transfer_to, 31, 12))) AS user_tag,
        row_number() OVER (
            PARTITION BY tx_hash, token, transfer_to, amount
            ORDER BY log_idx
        ) AS hop_ordinal
    FROM transfer_logs
    WHERE is_virtual_forward = 0
      AND substring(transfer_to, 11, 20) = '{VIRTUAL_MAGIC_HEX}'
),
second_hops AS (
    SELECT
        tx_hash,
        token,
        log_idx AS forward_log_idx,
        transfer_from AS virtual_address,
        transfer_to AS master_address,
        amount,
        row_number() OVER (
            PARTITION BY tx_hash, token, transfer_from, amount
            ORDER BY log_idx
        ) AS hop_ordinal
    FROM transfer_logs
    WHERE is_virtual_forward = 1
)
SELECT
    first_hops.block_num,
    first_hops.block_timestamp,
    first_hops.tx_idx,
    first_hops.tx_hash,
    first_hops.token,
    assumeNotNull(first_hops.sender) AS sender,
    assumeNotNull(first_hops.virtual_address) AS virtual_address,
    assumeNotNull(second_hops.master_address) AS master_address,
    first_hops.amount,
    assumeNotNull(first_hops.master_id) AS master_id,
    assumeNotNull(first_hops.user_tag) AS user_tag,
    first_hops.attribution_log_idx,
    assumeNotNull(second_hops.forward_log_idx) AS forward_log_idx
FROM first_hops
INNER JOIN second_hops
    ON first_hops.tx_hash = second_hops.tx_hash
   AND first_hops.token = second_hops.token
   AND first_hops.virtual_address = second_hops.virtual_address
   AND first_hops.amount = second_hops.amount
   AND first_hops.hop_ordinal = second_hops.hop_ordinal"#
    )
}

const TRANSFER_SELECTOR_HEX: &str =
    "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
const VIRTUAL_MAGIC_HEX: &str = "fdfdfdfdfdfdfdfdfdfd";

/// Identifies which log rows are part of a TIP-1022 virtual forwarding pair.
///
/// A virtual forwarding pair is two consecutive Transfer events in the same tx where:
/// 1. First Transfer: `Transfer(sender, virtualAddress, amount)` — the attribution hop
/// 2. Second Transfer: `Transfer(virtualAddress, masterAddress, amount)` — the forwarding hop
///
/// Both events have the same tx_hash, the same token (address), and the same amount.
/// The "to" of the first matches the "from" of the second, and that address is virtual.
///
/// Returns a vec of booleans parallel to `logs`, where `true` means the log is the
/// *forwarding hop* (second Transfer of the pair). Consumers can use this to:
/// - Skip the forwarding hop when counting transfers
/// - Show the virtual address → master forwarding in the UI
/// - Avoid double-counting in holder/balance aggregations
pub fn mark_virtual_forward_hops(logs: &[LogRow]) -> Vec<bool> {
    let mut marks = vec![false; logs.len()];

    if logs.len() < 2 {
        return marks;
    }

    let mut i = 0;
    while i + 1 < logs.len() {
        // Both must be Transfer events in the same tx and same token
        if !is_transfer_log(&logs[i]) || !is_transfer_log(&logs[i + 1]) {
            i += 1;
            continue;
        }

        if logs[i].tx_hash != logs[i + 1].tx_hash || logs[i].address != logs[i + 1].address {
            i += 1;
            continue;
        }

        // Extract "to" from first log (topic2, padded to 32 bytes, address in last 20)
        let first_to = match extract_address_from_topic(&logs[i].topic2) {
            Some(addr) => addr,
            None => {
                i += 1;
                continue;
            }
        };

        // Extract "from" from second log (topic1, padded to 32 bytes, address in last 20)
        let second_from = match extract_address_from_topic(&logs[i + 1].topic1) {
            Some(addr) => addr,
            None => {
                i += 1;
                continue;
            }
        };

        // The intermediate address must be the same and must be virtual
        if first_to != second_from || !is_virtual_address(&first_to) {
            i += 1;
            continue;
        }

        // Amounts must match (both in data field, uint256)
        if logs[i].data != logs[i + 1].data {
            i += 1;
            continue;
        }

        // Mark the second log as the forwarding hop
        marks[i + 1] = true;
        // Skip past both logs
        i += 2;
    }

    marks
}

fn is_transfer_log(log: &LogRow) -> bool {
    matches!(&log.topic0, Some(t) if t.as_slice() == TRANSFER_SELECTOR)
}

fn extract_address_from_topic(topic: &Option<Vec<u8>>) -> Option<[u8; 20]> {
    let t = topic.as_ref()?;
    if t.len() != 32 {
        return None;
    }
    let mut addr = [0u8; 20];
    addr.copy_from_slice(&t[12..32]);
    Some(addr)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_virtual_address(master_id: [u8; 4], user_tag: [u8; 6]) -> [u8; 20] {
        let mut addr = [0u8; 20];
        addr[0..4].copy_from_slice(&master_id);
        addr[4..14].copy_from_slice(&VIRTUAL_MAGIC);
        addr[14..20].copy_from_slice(&user_tag);
        addr
    }

    #[test]
    fn test_is_virtual_address() {
        let vaddr = make_virtual_address([0x07, 0xA3, 0xB1, 0xC2], [0xD4, 0xE5, 0xA7, 0xC3, 0xF1, 0x9E]);
        assert!(is_virtual_address(&vaddr));
    }

    #[test]
    fn test_not_virtual_address() {
        let normal = [0u8; 20];
        assert!(!is_virtual_address(&normal));

        let almost = {
            let mut a = [0xFD; 20];
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
        let vaddr = make_virtual_address([0x01, 0x02, 0x03, 0x04], [0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F]);
        let tx_hash = vec![0x11; 32];
        let token = vec![0x20, 0xc0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        let amount = vec![0u8; 32]; // 0 amount for simplicity

        let logs = vec![
            // Hop 1: sender → virtualAddress
            make_transfer_log(tx_hash.clone(), token.clone(), &sender, &vaddr, amount.clone(), 0),
            // Hop 2: virtualAddress → master
            make_transfer_log(tx_hash.clone(), token.clone(), &vaddr, &master, amount.clone(), 1),
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
        let vaddr = make_virtual_address([0x01, 0x02, 0x03, 0x04], [0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F]);
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
        let vaddr = make_virtual_address([0x01, 0x02, 0x03, 0x04], [0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F]);
        let token = vec![0x20; 20];
        let amount = vec![0u8; 32];

        let logs = vec![
            make_transfer_log(vec![0x11; 32], token.clone(), &sender, &vaddr, amount.clone(), 0),
            make_transfer_log(vec![0x22; 32], token.clone(), &vaddr, &master, amount.clone(), 1),
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
    fn test_virtual_deposits_sql_includes_pairing_logic() {
        let sql = virtual_deposits_sql_clickhouse();
        assert!(sql.contains("is_virtual_forward = 0"));
        assert!(sql.contains("is_virtual_forward = 1"));
        assert!(sql.contains("INNER JOIN second_hops"));
        assert!(sql.contains("substring(transfer_to, 11, 20)"));
    }
}
