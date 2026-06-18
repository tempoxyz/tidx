use super::{BackfillPolicy, ClickHouseObject, ClickHouseObjectKind};

const TRANSFER_BLOCKED_SCHEMA: &str = include_str!("../../db/clickhouse/transfer_blocked.sql");
const TRANSFER_BLOCKED_SELECT: &str =
    include_str!("../../db/clickhouse/transfer_blocked_select.sql");
const RECEIPT_CLAIMED_SCHEMA: &str = include_str!("../../db/clickhouse/receipt_claimed.sql");
const RECEIPT_CLAIMED_SELECT: &str = include_str!("../../db/clickhouse/receipt_claimed_select.sql");
const RECEIPT_BURNED_SCHEMA: &str = include_str!("../../db/clickhouse/receipt_burned.sql");
const RECEIPT_BURNED_SELECT: &str = include_str!("../../db/clickhouse/receipt_burned_select.sql");
const RECEIVE_POLICY_UPDATED_SCHEMA: &str =
    include_str!("../../db/clickhouse/receive_policy_updated.sql");
const RECEIVE_POLICY_UPDATED_SELECT: &str =
    include_str!("../../db/clickhouse/receive_policy_updated_select.sql");
const ADMIN_KEY_AUTHORIZED_SCHEMA: &str =
    include_str!("../../db/clickhouse/admin_key_authorized.sql");
const ADMIN_KEY_AUTHORIZED_SELECT: &str =
    include_str!("../../db/clickhouse/admin_key_authorized_select.sql");

/// Decoded T6 event tables (TIP-1028 receive policies, TIP-1049 admin keys).
///
/// These events are otherwise only reachable through the runtime
/// signature-decoded CTE surface, which re-decodes raw `logs` on every request
/// and cannot extract the dynamic `bytes receipt` payload from `TransferBlocked`
/// (the generic decoder returns only the ABI offset word). These pre-decoded
/// `ReplacingMergeTree` tables make the held/claim/burn and policy/admin-key
/// feeds plain sort-key seeks, mirroring how `token_transfers` pre-decodes
/// `Transfer`. Each decodes from `logs` filtered by both selector and emitting
/// precompile address.
pub const OBJECTS: &[ClickHouseObject] = &[
    ClickHouseObject {
        name: "transfer_blocked",
        kind: ClickHouseObjectKind::Table(TRANSFER_BLOCKED_SCHEMA),
        depends_on: &["logs"],
        public_query: true,
        block_column: Some("block_num"),
        backfill: Some(BackfillPolicy::Ranged {
            select_sql: TRANSFER_BLOCKED_SELECT,
        }),
    },
    ClickHouseObject {
        name: "transfer_blocked_mv",
        kind: ClickHouseObjectKind::MaterializedView {
            target_table: "transfer_blocked",
            select_sql: TRANSFER_BLOCKED_SELECT,
        },
        depends_on: &["logs", "transfer_blocked"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "receipt_claimed",
        kind: ClickHouseObjectKind::Table(RECEIPT_CLAIMED_SCHEMA),
        depends_on: &["logs"],
        public_query: true,
        block_column: Some("block_num"),
        backfill: Some(BackfillPolicy::Ranged {
            select_sql: RECEIPT_CLAIMED_SELECT,
        }),
    },
    ClickHouseObject {
        name: "receipt_claimed_mv",
        kind: ClickHouseObjectKind::MaterializedView {
            target_table: "receipt_claimed",
            select_sql: RECEIPT_CLAIMED_SELECT,
        },
        depends_on: &["logs", "receipt_claimed"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "receipt_burned",
        kind: ClickHouseObjectKind::Table(RECEIPT_BURNED_SCHEMA),
        depends_on: &["logs"],
        public_query: true,
        block_column: Some("block_num"),
        backfill: Some(BackfillPolicy::Ranged {
            select_sql: RECEIPT_BURNED_SELECT,
        }),
    },
    ClickHouseObject {
        name: "receipt_burned_mv",
        kind: ClickHouseObjectKind::MaterializedView {
            target_table: "receipt_burned",
            select_sql: RECEIPT_BURNED_SELECT,
        },
        depends_on: &["logs", "receipt_burned"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "receive_policy_updated",
        kind: ClickHouseObjectKind::Table(RECEIVE_POLICY_UPDATED_SCHEMA),
        depends_on: &["logs"],
        public_query: true,
        block_column: Some("block_num"),
        backfill: Some(BackfillPolicy::Ranged {
            select_sql: RECEIVE_POLICY_UPDATED_SELECT,
        }),
    },
    ClickHouseObject {
        name: "receive_policy_updated_mv",
        kind: ClickHouseObjectKind::MaterializedView {
            target_table: "receive_policy_updated",
            select_sql: RECEIVE_POLICY_UPDATED_SELECT,
        },
        depends_on: &["logs", "receive_policy_updated"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "admin_key_authorized",
        kind: ClickHouseObjectKind::Table(ADMIN_KEY_AUTHORIZED_SCHEMA),
        depends_on: &["logs"],
        public_query: true,
        block_column: Some("block_num"),
        backfill: Some(BackfillPolicy::Ranged {
            select_sql: ADMIN_KEY_AUTHORIZED_SELECT,
        }),
    },
    ClickHouseObject {
        name: "admin_key_authorized_mv",
        kind: ClickHouseObjectKind::MaterializedView {
            target_table: "admin_key_authorized",
            select_sql: ADMIN_KEY_AUTHORIZED_SELECT,
        },
        depends_on: &["logs", "admin_key_authorized"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
];

#[cfg(test)]
mod tests {
    use super::*;

    fn object(name: &str) -> &'static ClickHouseObject {
        OBJECTS.iter().find(|object| object.name == name).unwrap()
    }

    #[test]
    fn decoded_tables_are_block_scoped_and_public() {
        for name in [
            "transfer_blocked",
            "receipt_claimed",
            "receipt_burned",
            "receive_policy_updated",
            "admin_key_authorized",
        ] {
            let table = object(name);
            assert!(table.is_table(), "{name} should be a table");
            assert!(table.public_query, "{name} should be public");
            assert_eq!(table.block_column, Some("block_num"), "{name} block scope");
            assert!(table.backfill.is_some(), "{name} should declare backfill");
        }
    }

    #[test]
    fn materialized_views_decode_from_logs_by_selector_and_address() {
        // (mv, target, selector, emitting precompile). Selectors are keccak256
        // of each canonical event signature; asserted here so an accidental edit
        // can't silently point an MV at the wrong event or contract.
        let guard = "0xb10c000000000000000000000000000000000000";
        let cases = [
            (
                "transfer_blocked_mv",
                "transfer_blocked",
                "0x361d86e46fd139dc3eac4148f16b53597f0f8ddd9aba772aae0034bda5531b1c",
                guard,
            ),
            (
                "receipt_claimed_mv",
                "receipt_claimed",
                "0xdfa88f3774430fdb1d282219332a663236ccc8035ba8b9e0df856b374a5db085",
                guard,
            ),
            (
                "receipt_burned_mv",
                "receipt_burned",
                "0x61d14663748cd685c80a8434fc260ea4f4b27e213768b94375aa33b3985bf952",
                guard,
            ),
            (
                "receive_policy_updated_mv",
                "receive_policy_updated",
                "0xf0d46e7e04f2bf4cc56ea683299f4145c2650ef690e276e069bc2b806d68b2ea",
                "0x403c000000000000000000000000000000000000",
            ),
            (
                "admin_key_authorized_mv",
                "admin_key_authorized",
                "0x493bc0240c1da6c792754dc5247d39ed76c71c99a43e16777538687f8d05e88e",
                "0xaaaaaaaa00000000000000000000000000000000",
            ),
        ];
        for (mv_name, target, selector, emitter) in cases {
            let ddl = object(mv_name).ddl();
            assert!(ddl.starts_with(&format!("CREATE MATERIALIZED VIEW IF NOT EXISTS {mv_name}")));
            assert!(ddl.contains(&format!("TO {target} AS\n")));
            assert!(ddl.contains("FROM logs"));
            assert!(
                ddl.contains(selector),
                "{mv_name} should filter on {selector}"
            );
            assert!(
                ddl.contains(&format!("address = '{emitter}'")),
                "{mv_name} should only decode {emitter} logs"
            );
        }
    }

    #[test]
    fn transfer_blocked_extracts_dynamic_receipt_bytes() {
        // The generic CTE decoder returns only the ABI offset word for dynamic
        // `bytes`; this table must follow the offset to the length word and read
        // the payload, so guard against a regression to a fixed-position read.
        let ddl = object("transfer_blocked_mv").ddl();
        assert!(ddl.contains("AS receipt"));
        // payload start = 3 + offset*2 + 64 (one word past the length word)
        assert!(ddl.contains("* 2 + 64"));
        assert!(ddl.contains("AS blocked_nonce"));
        assert!(ddl.contains("AS amount"));
    }
}
