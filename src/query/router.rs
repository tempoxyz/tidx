use std::fmt;

/// The query type classification for routing behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryEngine {
    /// Analytical queries (OLAP): aggregations, window functions, large joins
    Olap,
    /// Transactional queries (OLTP): point lookups on indexed keys
    Postgres,
}

impl fmt::Display for QueryEngine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Olap => write!(f, "olap"),
            Self::Postgres => write!(f, "postgres"),
        }
    }
}

/// Classifies a SQL query as OLAP or OLTP.
///
/// Uses heuristics to detect OLAP patterns (aggregations, window functions,
/// large joins) vs OLTP patterns (point lookups on indexed keys).
pub fn route_query(sql: &str) -> QueryEngine {
    let upper = sql.to_uppercase();

    // OLAP patterns
    if has_olap_patterns(&upper) {
        return QueryEngine::Olap;
    }

    // Point lookups on indexed keys → Postgres
    if has_point_lookup(&upper) {
        return QueryEngine::Postgres;
    }

    // Multi-join queries without selective predicates → OLAP
    let join_count = upper.matches(" JOIN ").count();
    if join_count >= 2 {
        return QueryEngine::Olap;
    }

    // Default to Postgres for simple queries
    QueryEngine::Postgres
}

/// Detects OLAP patterns that benefit from columnar execution.
fn has_olap_patterns(upper: &str) -> bool {
    const OLAP_KEYWORDS: &[&str] = &[
        // Grouping
        "GROUP BY",
        "HAVING",
        "ROLLUP",
        "CUBE",
        "GROUPING SETS",
        // Window functions
        " OVER(",
        " OVER (",
        "ROW_NUMBER(",
        "RANK(",
        "DENSE_RANK(",
        "NTILE(",
        "LAG(",
        "LEAD(",
        "FIRST_VALUE(",
        "LAST_VALUE(",
        "NTH_VALUE(",
        // Aggregate functions (without GROUP BY these are still analytical)
        "COUNT(",
        "SUM(",
        "AVG(",
        "STDDEV(",
        "VARIANCE(",
        "PERCENTILE",
        "APPROX_",
        // Set operations
        " UNION ",
        " INTERSECT ",
        " EXCEPT ",
    ];

    for keyword in OLAP_KEYWORDS {
        if upper.contains(keyword) {
            return true;
        }
    }

    false
}

/// Detects point lookups on indexed columns that Postgres handles efficiently.
fn has_point_lookup(upper: &str) -> bool {
    const INDEXED_LOOKUPS: &[&str] = &[
        // Block lookups
        "WHERE NUM =",
        "WHERE BLOCK_NUM =",
        "WHERE NUM=",
        "WHERE BLOCK_NUM=",
        // Hash lookups
        "WHERE HASH =",
        "WHERE HASH=",
        "WHERE TX_HASH =",
        "WHERE TX_HASH=",
        "WHERE PARENT_HASH =",
        "WHERE PARENT_HASH=",
        // Address lookups
        "WHERE ADDRESS =",
        "WHERE ADDRESS=",
        "WHERE \"FROM\" =",
        "WHERE \"TO\" =",
        "WHERE MINER =",
        // Selector lookups
        "WHERE SELECTOR =",
        "WHERE SELECTOR=",
    ];

    for pattern in INDEXED_LOOKUPS {
        if upper.contains(pattern) {
            return true;
        }
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_group_by_routes_to_olap() {
        assert_eq!(
            route_query("SELECT address, COUNT(*) FROM logs GROUP BY address"),
            QueryEngine::Olap
        );
    }

    #[test]
    fn test_window_function_routes_to_olap() {
        assert_eq!(
            route_query("SELECT *, ROW_NUMBER() OVER (PARTITION BY address) FROM txs"),
            QueryEngine::Olap
        );
    }

    #[test]
    fn test_aggregates_route_to_olap() {
        assert_eq!(
            route_query("SELECT SUM(gas_used) FROM blocks"),
            QueryEngine::Olap
        );
        assert_eq!(
            route_query("SELECT AVG(gas_limit) FROM blocks"),
            QueryEngine::Olap
        );
    }

    #[test]
    fn test_point_lookup_routes_to_postgres() {
        assert_eq!(
            route_query("SELECT * FROM txs WHERE hash = '\\x1234'"),
            QueryEngine::Postgres
        );
        assert_eq!(
            route_query("SELECT * FROM blocks WHERE num = 1000"),
            QueryEngine::Postgres
        );
        assert_eq!(
            route_query("SELECT * FROM logs WHERE address = '\\xabcd'"),
            QueryEngine::Postgres
        );
    }

    #[test]
    fn test_simple_select_routes_to_postgres() {
        assert_eq!(
            route_query("SELECT * FROM blocks ORDER BY num DESC LIMIT 10"),
            QueryEngine::Postgres
        );
    }

    #[test]
    fn test_multi_join_routes_to_olap() {
        assert_eq!(
            route_query(
                "SELECT * FROM blocks b JOIN txs t ON b.num = t.block_num JOIN logs l ON t.hash = l.tx_hash"
            ),
            QueryEngine::Olap
        );
    }

    #[test]
    fn test_union_routes_to_olap() {
        assert_eq!(
            route_query("SELECT address FROM logs UNION SELECT \"from\" FROM txs"),
            QueryEngine::Olap
        );
    }

    #[test]
    fn test_lowercase_keywords() {
        assert_eq!(
            route_query("select count(*) from logs group by address"),
            QueryEngine::Olap
        );
        assert_eq!(
            route_query("select * from txs where hash = '0x...'"),
            QueryEngine::Postgres
        );
        assert_eq!(
            route_query("select sum(gas_used), avg(gas_limit) from blocks"),
            QueryEngine::Olap
        );
    }
}
