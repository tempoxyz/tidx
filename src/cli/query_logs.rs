use anyhow::Result;
use clap::Args as ClapArgs;

use crate::db;
use ak47::query::EventSignature;

#[derive(ClapArgs)]
pub struct Args {
    /// Database URL
    #[arg(long, env = "AK47_DATABASE_URL")]
    pub db: String,

    /// Event signature (e.g., "Transfer(address indexed from, address indexed to, uint256 value)")
    pub signature: String,

    /// Time filter (e.g., "1 hour", "24 hours", "7 days")
    #[arg(long, default_value = "24 hours")]
    pub after: String,

    /// Maximum number of results
    #[arg(long, default_value = "100")]
    pub limit: i64,

    /// Optional contract address filter (hex with 0x prefix)
    #[arg(long)]
    pub address: Option<String>,

    /// Output format (table, json, csv)
    #[arg(long, default_value = "table")]
    pub format: String,
}

pub async fn run(args: Args) -> Result<()> {
    let sig = EventSignature::parse(&args.signature)?;
    let cte = sig.to_cte_sql();

    let mut where_clauses = vec![format!(
        "block_timestamp > NOW() - INTERVAL '{}'",
        args.after
    )];

    if let Some(ref addr) = args.address {
        let addr_bytes = addr.strip_prefix("0x").unwrap_or(addr);
        where_clauses.push(format!("address = '\\x{}'", addr_bytes));
    }

    let sql = format!(
        r#"WITH {cte}
SELECT * FROM "{name}"
WHERE {where_clause}
ORDER BY block_num DESC, log_idx DESC
LIMIT {limit}"#,
        cte = cte,
        name = sig.name,
        where_clause = where_clauses.join(" AND "),
        limit = args.limit,
    );

    tracing::debug!(sql = %sql, "Executing query");

    let pool = db::create_pool(&args.db).await?;
    let conn = pool.get().await?;

    let rows = conn.query(&sql, &[]).await?;

    if rows.is_empty() {
        println!("No results found for {} in the last {}", sig.name, args.after);
        return Ok(());
    }

    let columns: Vec<_> = rows[0].columns().iter().map(|c| c.name()).collect();

    match args.format.as_str() {
        "json" => print_json(&rows, &columns)?,
        "csv" => print_csv(&rows, &columns)?,
        _ => print_table(&rows, &columns)?,
    }

    Ok(())
}

fn print_table(rows: &[tokio_postgres::Row], columns: &[&str]) -> Result<()> {
    println!("{}", columns.join(" | "));
    println!("{}", "-".repeat(columns.len() * 20));

    for row in rows {
        let values: Vec<String> = (0..columns.len())
            .map(|i| format_column(row, i))
            .collect();
        println!("{}", values.join(" | "));
    }

    println!("\n({} rows)", rows.len());
    Ok(())
}

fn print_json(rows: &[tokio_postgres::Row], columns: &[&str]) -> Result<()> {
    let mut result = Vec::new();

    for row in rows {
        let mut obj = serde_json::Map::new();
        for (i, col) in columns.iter().enumerate() {
            obj.insert(
                col.to_string(),
                serde_json::Value::String(format_column(row, i)),
            );
        }
        result.push(serde_json::Value::Object(obj));
    }

    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}

fn print_csv(rows: &[tokio_postgres::Row], columns: &[&str]) -> Result<()> {
    println!("{}", columns.join(","));

    for row in rows {
        let values: Vec<String> = (0..columns.len())
            .map(|i| format_column(row, i))
            .collect();
        println!("{}", values.join(","));
    }

    Ok(())
}

fn format_column(row: &tokio_postgres::Row, idx: usize) -> String {
    let col = &row.columns()[idx];

    match col.type_().name() {
        "int2" | "int4" | "int8" => {
            if let Ok(v) = row.try_get::<_, i64>(idx) {
                return v.to_string();
            }
            if let Ok(v) = row.try_get::<_, i32>(idx) {
                return v.to_string();
            }
            if let Ok(v) = row.try_get::<_, i16>(idx) {
                return v.to_string();
            }
        }
        "numeric" => {
            if let Ok(v) = row.try_get::<_, f64>(idx) {
                return v.to_string();
            }
        }
        "float4" | "float8" => {
            if let Ok(v) = row.try_get::<_, f64>(idx) {
                return v.to_string();
            }
        }
        "bytea" => {
            if let Ok(v) = row.try_get::<_, Vec<u8>>(idx) {
                return format!("0x{}", hex::encode(v));
            }
        }
        "text" | "varchar" | "name" => {
            if let Ok(v) = row.try_get::<_, String>(idx) {
                return v;
            }
        }
        "timestamptz" | "timestamp" => {
            if let Ok(v) = row.try_get::<_, chrono::DateTime<chrono::Utc>>(idx) {
                return v.to_rfc3339();
            }
        }
        "bool" => {
            if let Ok(v) = row.try_get::<_, bool>(idx) {
                return v.to_string();
            }
        }
        _ => {}
    }

    "NULL".to_string()
}
