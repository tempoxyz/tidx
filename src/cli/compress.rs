use anyhow::Result;
use clap::Args as ClapArgs;
use tracing::info;

use ak47::db;

#[derive(ClapArgs)]
pub struct Args {
    /// Database URL
    #[arg(long, env = "AK47_DATABASE_URL")]
    pub db: String,

    /// Only show what would be compressed (dry run)
    #[arg(long)]
    pub dry_run: bool,

    /// Specific table to compress (blocks, txs, logs). If not specified, compresses all.
    #[arg(long)]
    pub table: Option<String>,
}

pub async fn run(args: Args) -> Result<()> {
    let pool = db::create_pool(&args.db).await?;
    let conn = pool.get().await?;

    // Check if TimescaleDB is available and tables are hypertables
    let is_timescale: bool = conn
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'timescaledb')",
            &[],
        )
        .await?
        .get(0);

    if !is_timescale {
        info!("TimescaleDB extension not found, refreshing materialized views instead");
        return refresh_materialized_views(&pool, args.dry_run).await;
    }

    // Check if we have hypertables (not just partitioned tables)
    let hypertable_count: i64 = conn
        .query_one(
            "SELECT COUNT(*) FROM timescaledb_information.hypertables",
            &[],
        )
        .await
        .map(|r| r.get(0))
        .unwrap_or(0);

    if hypertable_count == 0 {
        info!("No hypertables found (using partitioned tables), refreshing materialized views");
        return refresh_materialized_views(&pool, args.dry_run).await;
    }

    // Get uncompressed chunks from TimescaleDB
    let table_filter = args
        .table
        .as_ref()
        .map(|t| format!("AND hypertable_name = '{}'", t))
        .unwrap_or_default();

    let query = format!(
        r#"
        SELECT 
            hypertable_schema,
            hypertable_name,
            chunk_schema,
            chunk_name,
            range_start,
            range_end
        FROM timescaledb_information.chunks 
        WHERE is_compressed = false 
        {}
        ORDER BY hypertable_name, range_start
        "#,
        table_filter
    );

    let chunks = conn.query(&query, &[]).await?;

    if chunks.is_empty() {
        info!("No uncompressed chunks found");
        return Ok(());
    }

    info!(count = chunks.len(), "Found uncompressed chunks");

    for chunk in &chunks {
        let hypertable: String = chunk.get("hypertable_name");
        let chunk_schema: String = chunk.get("chunk_schema");
        let chunk_name: String = chunk.get("chunk_name");
        let range_start: i64 = chunk.get("range_start");
        let range_end: i64 = chunk.get("range_end");

        let full_chunk_name = format!("{}.{}", chunk_schema, chunk_name);

        if args.dry_run {
            info!(
                hypertable,
                chunk = %full_chunk_name,
                range_start,
                range_end,
                "[DRY RUN] Would compress chunk"
            );
        } else {
            info!(
                hypertable,
                chunk = %full_chunk_name,
                range_start,
                range_end,
                "Compressing chunk"
            );

            conn.execute(
                &format!("SELECT compress_chunk('{}')", full_chunk_name),
                &[],
            )
            .await?;
        }
    }

    // Show compression stats
    if !args.dry_run {
        show_compression_stats(&pool).await?;
    }

    Ok(())
}

async fn refresh_materialized_views(pool: &db::Pool, dry_run: bool) -> Result<()> {
    let conn = pool.get().await?;

    // Check which materialized views exist
    let views: Vec<String> = conn
        .query(
            r#"
            SELECT matviewname 
            FROM pg_matviews 
            WHERE matviewname IN ('txs_hourly', 'txs_daily', 'blocks_hourly', 'blocks_daily', 'logs_hourly', 'logs_daily')
            "#,
            &[],
        )
        .await?
        .iter()
        .map(|r| r.get(0))
        .collect();

    if views.is_empty() {
        info!("No materialized views found to refresh");
        return Ok(());
    }

    for view in &views {
        if dry_run {
            info!(view, "[DRY RUN] Would refresh materialized view");
        } else {
            info!(view, "Refreshing materialized view");
            conn.execute(&format!("REFRESH MATERIALIZED VIEW {}", view), &[])
                .await?;
        }
    }

    Ok(())
}

async fn show_compression_stats(pool: &db::Pool) -> Result<()> {
    let conn = pool.get().await?;

    let stats = conn
        .query(
            r#"
            SELECT 
                hypertable_name,
                total_chunks,
                number_compressed_chunks,
                pg_size_pretty(before_compression_total_bytes) as before_size,
                pg_size_pretty(after_compression_total_bytes) as after_size,
                CASE 
                    WHEN before_compression_total_bytes > 0 
                    THEN round((1 - after_compression_total_bytes::numeric / before_compression_total_bytes) * 100, 1)
                    ELSE 0
                END as compression_ratio
            FROM timescaledb_information.compression_settings cs
            LEFT JOIN LATERAL (
                SELECT * FROM hypertable_compression_stats(cs.hypertable_schema || '.' || cs.hypertable_name)
            ) stats ON true
            WHERE total_chunks > 0
            "#,
            &[],
        )
        .await?;

    if !stats.is_empty() {
        info!("Compression statistics:");
        for row in &stats {
            let table: String = row.get("hypertable_name");
            let total: i64 = row.get("total_chunks");
            let compressed: i64 = row.get("number_compressed_chunks");
            let before: String = row.get("before_size");
            let after: String = row.get("after_size");
            let ratio: f64 = row.try_get("compression_ratio").unwrap_or(0.0);

            info!(
                table,
                total_chunks = total,
                compressed_chunks = compressed,
                before = %before,
                after = %after,
                compression_ratio = format!("{:.1}%", ratio),
                "Table stats"
            );
        }
    }

    Ok(())
}
