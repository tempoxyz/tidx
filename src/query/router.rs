use std::fmt;

/// The database engine to route a query to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryEngine {
    /// ClickHouse for analytical queries (OLAP)
    ClickHouse,
    /// PostgreSQL for transactional queries (OLTP)
    Postgres,
}

impl fmt::Display for QueryEngine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ClickHouse => write!(f, "clickhouse"),
            Self::Postgres => write!(f, "postgres"),
        }
    }
}
