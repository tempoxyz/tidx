use std::fmt;

/// Resolved execution route for a user query: which engine runs the SQL and
/// where the data lives.
///
/// Resolved from the user-facing `engine` and `source` parameters:
///
/// | `engine`     | `source`              | route                   |
/// |--------------|-----------------------|-------------------------|
/// | `postgres`*  | `postgres`*           | `Postgres`              |
/// | `postgres`*  | `clickhouse`          | `PostgresViaClickHouse` |
/// | `postgres`*  | `postgres-clickhouse` | `Tiered`                |
/// | `clickhouse` | `clickhouse`*         | `ClickHouse`            |
///
/// `*` = default when omitted. `engine=tiered` is accepted as a legacy alias
/// for `engine=postgres&source=postgres-clickhouse`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryRoute {
    /// PostgreSQL over native `public.*` tables (hot window once pruning is on)
    Postgres,
    /// ClickHouse directly (full archive)
    ClickHouse,
    /// PostgreSQL over `ch.*` pg_clickhouse foreign tables: full ClickHouse
    /// archive through the PostgreSQL planner, no hot PostgreSQL arm
    PostgresViaClickHouse,
    /// Hot PostgreSQL window + cold ClickHouse archive: native split at the
    /// prune boundary when provably safe, else `tiered.*` UNION ALL views
    Tiered,
}

impl QueryRoute {
    /// Resolve the user-facing `engine` and `source` parameters into a route.
    pub fn resolve(engine: Option<&str>, source: Option<&str>) -> Result<Self, String> {
        // Legacy alias: engine=tiered ≡ engine=postgres&source=postgres-clickhouse.
        if engine == Some("tiered") {
            return match source {
                None | Some("postgres-clickhouse") => Ok(Self::Tiered),
                Some(other) => Err(format!(
                    "engine=tiered implies source=postgres-clickhouse (got source={other})"
                )),
            };
        }
        match engine.unwrap_or("postgres") {
            "postgres" => match source.unwrap_or("postgres") {
                "postgres" => Ok(Self::Postgres),
                "clickhouse" => Ok(Self::PostgresViaClickHouse),
                "postgres-clickhouse" => Ok(Self::Tiered),
                other => Err(format!(
                    "unknown source '{other}' (expected postgres, clickhouse, or postgres-clickhouse)"
                )),
            },
            "clickhouse" => match source.unwrap_or("clickhouse") {
                "clickhouse" => Ok(Self::ClickHouse),
                "postgres" | "postgres-clickhouse" => Err(format!(
                    "engine=clickhouse only reads from ClickHouse (got source={})",
                    source.unwrap_or_default()
                )),
                other => Err(format!(
                    "unknown source '{other}' (expected postgres, clickhouse, or postgres-clickhouse)"
                )),
            },
            other => Err(format!(
                "unknown engine '{other}' (expected postgres or clickhouse)"
            )),
        }
    }
}

impl fmt::Display for QueryRoute {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ClickHouse => write!(f, "clickhouse"),
            Self::Postgres => write!(f, "postgres"),
            Self::PostgresViaClickHouse => write!(f, "postgres-via-clickhouse"),
            Self::Tiered => write!(f, "tiered"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_engine_source_matrix() {
        use QueryRoute::*;
        let cases = [
            (None, None, Postgres),
            (Some("postgres"), None, Postgres),
            (Some("postgres"), Some("postgres"), Postgres),
            (None, Some("postgres"), Postgres),
            (Some("postgres"), Some("clickhouse"), PostgresViaClickHouse),
            (None, Some("clickhouse"), PostgresViaClickHouse),
            (Some("postgres"), Some("postgres-clickhouse"), Tiered),
            (None, Some("postgres-clickhouse"), Tiered),
            (Some("clickhouse"), None, ClickHouse),
            (Some("clickhouse"), Some("clickhouse"), ClickHouse),
        ];
        for (engine, source, want) in cases {
            assert_eq!(
                QueryRoute::resolve(engine, source),
                Ok(want),
                "engine={engine:?} source={source:?}"
            );
        }
    }

    #[test]
    fn tiered_engine_is_legacy_alias() {
        assert_eq!(
            QueryRoute::resolve(Some("tiered"), None),
            Ok(QueryRoute::Tiered)
        );
        assert_eq!(
            QueryRoute::resolve(Some("tiered"), Some("postgres-clickhouse")),
            Ok(QueryRoute::Tiered)
        );
        assert!(QueryRoute::resolve(Some("tiered"), Some("postgres")).is_err());
    }

    #[test]
    fn rejects_invalid_combinations() {
        assert!(QueryRoute::resolve(Some("clickhouse"), Some("postgres")).is_err());
        assert!(QueryRoute::resolve(Some("clickhouse"), Some("postgres-clickhouse")).is_err());
        assert!(QueryRoute::resolve(Some("duckdb"), None).is_err());
        assert!(QueryRoute::resolve(None, Some("mysql")).is_err());
        assert!(QueryRoute::resolve(Some("postgres-via-clickhouse"), None).is_err());
    }
}
