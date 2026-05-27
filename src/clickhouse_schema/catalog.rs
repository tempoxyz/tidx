use std::borrow::Cow;

#[derive(Clone, Copy)]
pub struct ClickHouseObject {
    pub name: &'static str,
    pub kind: ClickHouseObjectKind,
    pub depends_on: &'static [&'static str],
    pub public_query: bool,
    pub block_column: Option<&'static str>,
    pub backfill: Option<BackfillPolicy>,
}

#[derive(Clone, Copy)]
pub enum ClickHouseObjectKind {
    Static(&'static str),
    MaterializedView {
        target_table: &'static str,
        select_sql: &'static str,
    },
}

impl ClickHouseObject {
    pub fn ddl(&self) -> Cow<'static, str> {
        match self.kind {
            ClickHouseObjectKind::Static(sql) => Cow::Borrowed(sql),
            ClickHouseObjectKind::MaterializedView {
                target_table,
                select_sql,
            } => Cow::Owned(format!(
                "CREATE MATERIALIZED VIEW IF NOT EXISTS {} TO {} AS\n{}",
                self.name, target_table, select_sql
            )),
        }
    }
}

#[derive(Clone, Copy)]
pub enum BackfillPolicy {
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
