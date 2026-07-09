use std::fmt;

/// The database engine to route a query to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryEngine {
    /// ClickHouse for analytical queries (OLAP)
    ClickHouse,
    /// PostgreSQL for transactional queries (OLTP)
    Postgres,
    /// PostgreSQL over `tiered.*` views: hot PG window + ClickHouse archive
    /// via pg_clickhouse (full history through one PG connection)
    Tiered,
    /// PostgreSQL over `ch.*` pg_clickhouse foreign tables: full ClickHouse
    /// archive through the PostgreSQL planner, no hot PostgreSQL arm
    PostgresViaClickHouse,
}

impl fmt::Display for QueryEngine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ClickHouse => write!(f, "clickhouse"),
            Self::Postgres => write!(f, "postgres"),
            Self::Tiered => write!(f, "tiered"),
            Self::PostgresViaClickHouse => write!(f, "postgres-via-clickhouse"),
        }
    }
}
