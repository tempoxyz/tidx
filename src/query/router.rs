use std::fmt;

/// The database engine to route a query to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryEngine {
    /// ClickHouse for analytical queries (OLAP)
    ClickHouse,
    /// PostgreSQL for transactional queries (OLTP)
    Postgres,
    /// ClickHouse reached over the PostgreSQL wire protocol (pg_clickhouse).
    /// Same data as `ClickHouse`; queries are written in Postgres SQL.
    ClickHousePg,
}

impl QueryEngine {
    /// Parse an `engine=` query parameter. `None` means "not specified".
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "clickhouse" => Some(Self::ClickHouse),
            "postgres" => Some(Self::Postgres),
            "clickhouse_pg" => Some(Self::ClickHousePg),
            _ => None,
        }
    }
}

impl fmt::Display for QueryEngine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ClickHouse => write!(f, "clickhouse"),
            Self::Postgres => write!(f, "postgres"),
            Self::ClickHousePg => write!(f, "clickhouse_pg"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_known_engines() {
        assert_eq!(QueryEngine::parse("postgres"), Some(QueryEngine::Postgres));
        assert_eq!(
            QueryEngine::parse("clickhouse"),
            Some(QueryEngine::ClickHouse)
        );
        assert_eq!(
            QueryEngine::parse("clickhouse_pg"),
            Some(QueryEngine::ClickHousePg)
        );
    }

    #[test]
    fn rejects_unknown_engines() {
        assert_eq!(QueryEngine::parse("mysql"), None);
        assert_eq!(QueryEngine::parse(""), None);
        assert_eq!(QueryEngine::parse("Postgres"), None);
    }

    #[test]
    fn display_round_trips() {
        for engine in [
            QueryEngine::Postgres,
            QueryEngine::ClickHouse,
            QueryEngine::ClickHousePg,
        ] {
            assert_eq!(QueryEngine::parse(&engine.to_string()), Some(engine));
        }
    }
}
