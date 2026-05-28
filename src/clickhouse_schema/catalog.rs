use std::borrow::Cow;

/// A single ClickHouse object the indexer manages.
///
/// `depends_on` is informational — array order in `base::TABLES`,
/// `base::MIGRATIONS`, and the derived modules already encodes dependency
/// ordering. The list is asserted to be consistent in unit tests.
#[derive(Clone, Copy)]
pub struct ClickHouseObject {
    pub name: &'static str,
    pub kind: ClickHouseObjectKind,
    pub depends_on: &'static [&'static str],
    /// True for tables and views that the public `/query` HTTP surface may
    /// reference. Internal helpers (materialized views, migrations) are false.
    pub public_query: bool,
    /// Set for tables that store rows scoped to a single block. Used by
    /// `max_block_in_table` for sync watermarks and by reorg cleanup.
    pub block_column: Option<&'static str>,
    pub backfill: Option<BackfillPolicy>,
}

#[derive(Clone, Copy)]
pub enum ClickHouseObjectKind {
    /// Base table — created once with `CREATE TABLE IF NOT EXISTS`. Never
    /// re-created on definition drift; schema changes ride a `Migration`.
    Table(&'static str),
    /// One-shot DDL change tracked in `tidx_schema_objects` and replayed on
    /// every startup. Must itself be idempotent (e.g. `ADD COLUMN IF NOT
    /// EXISTS`).
    Migration(&'static str),
    /// `CREATE VIEW IF NOT EXISTS`. Dropped + recreated whenever the DDL
    /// checksum changes.
    View(&'static str),
    /// `CREATE MATERIALIZED VIEW … TO target AS select_sql`. Dropped +
    /// recreated whenever the select changes.
    MaterializedView {
        target_table: &'static str,
        select_sql: &'static str,
    },
}

impl ClickHouseObject {
    pub fn ddl(&self) -> Cow<'static, str> {
        match self.kind {
            ClickHouseObjectKind::Table(sql)
            | ClickHouseObjectKind::Migration(sql)
            | ClickHouseObjectKind::View(sql) => Cow::Borrowed(sql),
            ClickHouseObjectKind::MaterializedView {
                target_table,
                select_sql,
            } => Cow::Owned(format!(
                "CREATE MATERIALIZED VIEW IF NOT EXISTS {} TO {} AS\n{}",
                self.name, target_table, select_sql
            )),
        }
    }

    pub fn is_table(&self) -> bool {
        matches!(self.kind, ClickHouseObjectKind::Table(_))
    }

    pub fn is_view(&self) -> bool {
        matches!(self.kind, ClickHouseObjectKind::View(_))
    }

    pub fn is_materialized_view(&self) -> bool {
        matches!(self.kind, ClickHouseObjectKind::MaterializedView { .. })
    }

    /// DROP statement to run before re-creating a definition-drifted view/MV.
    pub fn drop_sql(&self) -> Option<String> {
        match self.kind {
            ClickHouseObjectKind::MaterializedView { .. } | ClickHouseObjectKind::View(_) => {
                Some(format!("DROP VIEW IF EXISTS {}", self.name))
            }
            _ => None,
        }
    }
}

#[derive(Clone, Copy)]
pub enum BackfillPolicy {
    /// Run the select if the target table is empty. The indexer captures a
    /// startup block cutoff and runs historical backfills through that cutoff;
    /// materialized views cover rows written after the plan is created.
    IfEmpty { select_sql: &'static str },
}

#[derive(Clone, Copy)]
pub struct BlockScopedTable {
    pub name: &'static str,
    pub block_column: &'static str,
}

impl BlockScopedTable {
    pub fn from_object(object: &ClickHouseObject) -> Option<Self> {
        object.block_column.map(|block_column| Self {
            name: object.name,
            block_column,
        })
    }
}
