pub const VIRTUAL_DEPOSITS_VIEW: &str = "virtual_deposits";
pub const VIRTUAL_DEPOSITS_ENGINE: &str = "ReplacingMergeTree()";
pub const VIRTUAL_DEPOSITS_ORDER_BY: &[&str] = &[
    "virtual_address",
    "token",
    "block_num",
    "attribution_log_idx",
];

const TRANSFER_SELECTOR: &str =
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
const VIRTUAL_MAGIC: &str = "fdfdfdfdfdfdfdfdfdfd";

pub fn virtual_deposits_sql() -> String {
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
    WHERE selector = '{TRANSFER_SELECTOR}'
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
      AND substring(transfer_to, 11, 20) = '{VIRTUAL_MAGIC}'
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn virtual_deposits_sql_includes_virtual_pairing() {
        let sql = virtual_deposits_sql();
        assert!(sql.contains("is_virtual_forward = 0"));
        assert!(sql.contains("is_virtual_forward = 1"));
        assert!(sql.contains("substring(transfer_to, 11, 20)"));
        assert!(sql.contains("INNER JOIN second_hops"));
    }
}
