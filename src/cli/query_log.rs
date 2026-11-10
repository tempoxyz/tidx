use anyhow::{Context, Result};
use clap::{Args as ClapArgs, ValueEnum};
use serde_json::{Map, Value, json};
use std::path::PathBuf;

use tidx::clickhouse::ClickHouseEngine;
use tidx::config::{ChainConfig, Config};
use tidx::db;

/// The most expensive API queries, from ClickHouse's system.query_log
/// (queries tagged by `tidx serve`/`tidx up` with route, caller and
/// request ID) and PostgreSQL's pg_stat_statements for the API role.
#[derive(ClapArgs)]
pub struct Args {
    /// Path to config file
    #[arg(short, long, default_value = "config.toml")]
    pub config: PathBuf,

    /// Chain name or ID (default: the first chain)
    #[arg(long)]
    pub chain: Option<String>,

    /// Look back this many minutes in system.query_log
    #[arg(long, default_value_t = 60)]
    pub since_minutes: u32,

    /// Rows per section
    #[arg(long, default_value_t = 20)]
    pub limit: u32,

    /// Group ClickHouse queries by query shape, route or caller
    #[arg(long, value_enum, default_value_t = GroupBy::Shape)]
    pub by: GroupBy,

    /// Read query_log from every replica of this ClickHouse cluster
    /// (clusterAllReplicas) instead of the connected server alone
    #[arg(long)]
    pub cluster: Option<String>,

    /// Which stores to read
    #[arg(long, value_enum, default_value_t = Store::All)]
    pub store: Store,

    /// Output as JSON
    #[arg(long)]
    pub json: bool,
}

#[derive(Clone, Copy, ValueEnum, PartialEq, Eq)]
pub enum GroupBy {
    Shape,
    Route,
    Caller,
}

#[derive(Clone, Copy, ValueEnum, PartialEq, Eq)]
pub enum Store {
    All,
    Clickhouse,
    Postgres,
}

pub async fn run(args: Args) -> Result<()> {
    let config = Config::load(&args.config)?;
    let chain = select_chain(&config, args.chain.as_deref())?;
    let mut report = Map::new();
    report.insert("chain".into(), json!(chain.name));
    report.insert("since_minutes".into(), json!(args.since_minutes));

    if args.store != Store::Postgres {
        let section = match &chain.clickhouse {
            Some(ch) if ch.enabled => {
                let engine = ClickHouseEngine::new(ch, chain.chain_id)?;
                let sql = clickhouse_sql(&args, engine.database());
                let result = engine.query(&sql, &[]).await.context(
                    "reading system.query_log (the user needs SELECT on system.query_log)",
                )?;
                rows_to_json(&result.columns, result.rows)
            }
            _ => json!({ "skipped": "ClickHouse is not enabled for this chain" }),
        };
        report.insert("clickhouse".into(), section);
    }

    if args.store != Store::Clickhouse {
        report.insert(
            "postgres".into(),
            postgres_section(chain, args.limit).await?,
        );
    }

    let report = Value::Object(report);
    if args.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        print_report(&report);
    }
    Ok(())
}

fn select_chain<'a>(config: &'a Config, wanted: Option<&str>) -> Result<&'a ChainConfig> {
    let Some(wanted) = wanted else {
        return config.chains.first().context("no chains in config");
    };
    config
        .chains
        .iter()
        .find(|c| c.name == wanted || c.chain_id.to_string() == wanted)
        .with_context(|| format!("no chain named {wanted} in config"))
}

/// Tagged queries (`tidx/<route>/<caller>/<request_id>`) on this chain's
/// database, most total time first.
fn clickhouse_sql(args: &Args, database: &str) -> String {
    let source = match &args.cluster {
        Some(cluster) => format!(
            "clusterAllReplicas('{}', system.query_log)",
            sql_string(cluster)
        ),
        None => "system.query_log".to_string(),
    };
    let route = "splitByChar('/', log_comment)[2]";
    let caller = "splitByChar('/', log_comment)[3]";
    let (key, extra) = match args.by {
        GroupBy::Shape => (
            "normalized_query_hash".to_string(),
            format!(
                "groupUniqArray(5)({route}) AS routes, groupUniqArray(5)({caller}) AS callers, \
                 substring(normalizeQuery(any(query)), 1, 300) AS example"
            ),
        ),
        GroupBy::Route => (
            format!("{route} AS route"),
            format!("groupUniqArray(5)({caller}) AS callers"),
        ),
        GroupBy::Caller => (
            format!("{caller} AS caller"),
            format!("groupUniqArray(5)({route}) AS routes"),
        ),
    };
    format!(
        "SELECT {key}, count() AS queries, \
         countIf(type != 'QueryFinish') AS errors, \
         round(sum(query_duration_ms) / 1000, 1) AS total_s, \
         round(quantile(0.5)(query_duration_ms)) AS p50_ms, \
         round(quantile(0.99)(query_duration_ms)) AS p99_ms, \
         max(query_duration_ms) AS max_ms, \
         sum(read_rows) AS read_rows, \
         formatReadableSize(sum(read_bytes)) AS read, \
         formatReadableSize(max(memory_usage)) AS max_memory, \
         {extra} \
         FROM {source} \
         WHERE event_time >= now() - INTERVAL {since} MINUTE \
           AND type IN ('QueryFinish', 'ExceptionBeforeStart', 'ExceptionWhileProcessing') \
           AND startsWith(log_comment, 'tidx/') \
           AND has(databases, '{db}') \
         GROUP BY {group} \
         ORDER BY sum(query_duration_ms) DESC \
         LIMIT {limit}",
        since = args.since_minutes,
        db = sql_string(database),
        group = key.split(" AS ").next().unwrap_or(&key),
        limit = args.limit,
    )
}

/// pg_stat_statements for the API role (or every role without one), most
/// total time first. pg_stat_statements has no application_name, so routes
/// and callers are ClickHouse-only.
async fn postgres_section(chain: &ChainConfig, limit: u32) -> Result<Value> {
    let pool = db::create_pool(&chain.resolved_pg_url()?).await?;
    let conn = pool.get().await?;
    let installed = conn
        .query_opt(
            "SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'",
            &[],
        )
        .await?
        .is_some();
    if !installed {
        return Ok(json!({
            "skipped": "pg_stat_statements is not installed (shared_preload_libraries and CREATE EXTENSION pg_stat_statements)"
        }));
    }
    let role = chain.api_pg_role()?;
    let rows = conn
        .query(
            "SELECT queryid::text AS queryid, calls, \
                    round(total_exec_time::numeric / 1000, 1)::float8 AS total_s, \
                    round(mean_exec_time::numeric, 1)::float8 AS mean_ms, \
                    round(max_exec_time::numeric, 1)::float8 AS max_ms, \
                    rows, shared_blks_read + temp_blks_read AS blocks_read, \
                    left(regexp_replace(query, '\\s+', ' ', 'g'), 300) AS example \
             FROM pg_stat_statements \
             WHERE queryid IS NOT NULL \
               AND dbid = (SELECT oid FROM pg_database WHERE datname = current_database()) \
               AND ($1::text IS NULL OR userid = (SELECT oid FROM pg_roles WHERE rolname = $1)) \
             ORDER BY total_exec_time DESC \
             LIMIT $2",
            &[&role, &i64::from(limit)],
        )
        .await
        .context("reading pg_stat_statements")?;
    if rows.is_empty() {
        // Other roles' statements have a NULL queryid unless this user is a
        // superuser or has pg_read_all_stats.
        return Ok(json!({
            "skipped": "no visible pg_stat_statements rows for the API role (the config's PostgreSQL user needs pg_read_all_stats to see other roles' statements)",
            "role": role,
        }));
    }
    let columns: Vec<String> = rows
        .first()
        .map(|r| r.columns().iter().map(|c| c.name().to_string()).collect())
        .unwrap_or_default();
    let values = rows
        .iter()
        .map(|row| {
            vec![
                json!(row.get::<_, Option<String>>(0)),
                json!(row.get::<_, Option<i64>>(1)),
                json!(row.get::<_, Option<f64>>(2)),
                json!(row.get::<_, Option<f64>>(3)),
                json!(row.get::<_, Option<f64>>(4)),
                json!(row.get::<_, Option<i64>>(5)),
                json!(row.get::<_, Option<i64>>(6)),
                json!(row.get::<_, Option<String>>(7)),
            ]
        })
        .collect();
    let mut section = rows_to_json(&columns, values);
    if let Value::Object(map) = &mut section {
        map.insert("role".into(), json!(role));
        map.insert(
            "note".into(),
            json!("cumulative since pg_stat_statements was last reset"),
        );
    }
    Ok(section)
}

fn rows_to_json(columns: &[String], rows: Vec<Vec<Value>>) -> Value {
    let rows: Vec<Value> = rows
        .into_iter()
        .map(|row| {
            Value::Object(
                columns
                    .iter()
                    .cloned()
                    .zip(row)
                    .collect::<Map<String, Value>>(),
            )
        })
        .collect();
    json!({ "rows": rows })
}

fn sql_string(value: &str) -> String {
    value.replace('\\', "\\\\").replace('\'', "\\'")
}

fn print_report(report: &Value) {
    for store in ["clickhouse", "postgres"] {
        let Some(section) = report.get(store) else {
            continue;
        };
        println!("== {store} ==");
        if let Some(skipped) = section.get("skipped").and_then(Value::as_str) {
            println!("skipped: {skipped}\n");
            continue;
        }
        let rows = section
            .get("rows")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        if rows.is_empty() {
            println!("no queries\n");
            continue;
        }
        for (i, row) in rows.iter().enumerate() {
            let Some(fields) = row.as_object() else {
                continue;
            };
            let line: Vec<String> = fields
                .iter()
                .filter(|(k, _)| k.as_str() != "example")
                .map(|(k, v)| format!("{k}={}", plain(v)))
                .collect();
            println!("{:>2}. {}", i + 1, line.join("  "));
            if let Some(example) = fields.get("example") {
                println!("    {}", plain(example));
            }
        }
        println!();
    }
}

fn plain(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}
