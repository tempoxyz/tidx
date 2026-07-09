use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Args as ClapArgs;
use metrics_exporter_prometheus::PrometheusBuilder;
use tokio::sync::RwLock;
use tracing::{error, info, warn};

use tidx::api::{
    self, ChainClickHouseConfig, SharedClickHouseConfigs, SharedClickHouseEngines, SharedPools,
};
use tidx::broadcast::Broadcaster;
use tidx::clickhouse::ClickHouseEngine;
use tidx::config::{ChainConfig, Config, ConfigWatcher, NewChainEvent};
use tidx::db::{self, Pool, ThrottledPool};
use tidx::sync::ch_sink::ClickHouseSink;
use tidx::sync::engine::SyncEngine;
use tidx::sync::sink::SinkSet;

const CLICKHOUSE_BACKFILL_RETRY_MAX_SECS: u64 = 10;
const CLICKHOUSE_DERIVED_REPAIR_RETRY_MAX_SECS: u64 = 300;
const CLICKHOUSE_PG_POOL_SIZE: usize = 8;

#[derive(ClapArgs)]
pub struct Args {
    /// Path to config file
    #[arg(short, long, default_value = "config.toml")]
    pub config: PathBuf,

    /// Disable config hot-reloading
    #[arg(long)]
    pub no_watch: bool,
}

/// Per-chain runtime built from its configured stores.
struct ChainRuntime {
    /// Fan-out sinks (PG and/or CH) for the sync engine.
    sinks: SinkSet,
    /// PostgreSQL pool for the HTTP API (None when postgres is not configured).
    api_pool: Option<Pool>,
    /// pg_clickhouse pool for `engine=clickhouse_pg` queries.
    clickhouse_pg_pool: Option<Pool>,
}

pub async fn run(args: Args) -> Result<()> {
    let config = Config::load(&args.config)?;

    info!(chains = config.chains.len(), "Loaded config");

    if config.prometheus.enabled {
        let metrics_addr: SocketAddr =
            format!("{}:{}", config.http.bind, config.prometheus.port).parse()?;
        info!(addr = %metrics_addr, "Starting Prometheus metrics server");
        PrometheusBuilder::new()
            .with_http_listener(metrics_addr)
            .install()?;
    }

    let broadcaster = Arc::new(Broadcaster::new());
    let (shutdown_tx, _shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);

    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let shutdown_tx_sigterm = shutdown_tx.clone();
        tokio::spawn(async move {
            let mut sigterm =
                signal(SignalKind::terminate()).expect("failed to register SIGTERM handler");
            sigterm.recv().await;
            info!("Received SIGTERM, shutting down...");
            let _ = shutdown_tx_sigterm.send(());
        });
    }

    tokio::spawn({
        let shutdown_tx = shutdown_tx.clone();
        async move {
            tokio::signal::ctrl_c().await.ok();
            info!("Received SIGINT, shutting down...");
            let _ = shutdown_tx.send(());
        }
    });

    let pools: SharedPools = Arc::new(RwLock::new(HashMap::new()));
    let clickhouse_pg_pools: SharedPools = Arc::new(RwLock::new(HashMap::new()));
    let clickhouse_configs: SharedClickHouseConfigs = Arc::new(RwLock::new(HashMap::new()));
    let clickhouse_engines: SharedClickHouseEngines = Arc::new(RwLock::new(HashMap::new()));
    let mut default_chain_id = 0u64;

    for chain in &config.chains {
        let runtime = initialize_chain(chain, Arc::clone(&clickhouse_configs)).await?;

        if default_chain_id == 0 {
            default_chain_id = chain.chain_id;
        }

        // Initialize ClickHouse query engine if configured (for each chain)
        if let Some(ch_config) = chain.clickhouse_enabled() {
            match ClickHouseEngine::new(ch_config, chain.chain_id) {
                Ok(engine) => {
                    let engine = Arc::new(engine);
                    clickhouse_engines
                        .write()
                        .await
                        .insert(chain.chain_id, engine);
                    info!(chain = %chain.name, chain_id = chain.chain_id, "ClickHouse OLAP engine initialized");
                }
                Err(e) => {
                    error!(error = %e, chain = %chain.name, "Failed to create ClickHouse engine");
                }
            }
        }

        if let Some(api_pool) = runtime.api_pool.clone() {
            pools.write().await.insert(chain.chain_id, api_pool);
        }
        if let Some(ch_pg_pool) = runtime.clickhouse_pg_pool.clone() {
            clickhouse_pg_pools
                .write()
                .await
                .insert(chain.chain_id, ch_pg_pool);
        }

        spawn_sync_engine(
            chain.clone(),
            runtime.sinks,
            broadcaster.clone(),
            shutdown_tx.subscribe(),
        );
    }

    let (chain_tx, mut chain_rx) = tokio::sync::mpsc::channel::<NewChainEvent>(16);

    if !args.no_watch {
        let watcher = ConfigWatcher::new(args.config.clone(), &config, chain_tx)?;
        let trusted_cidrs = watcher.trusted_cidrs();
        watcher.start()?;

        if config.http.enabled && default_chain_id != 0 {
            let addr: SocketAddr = format!("{}:{}", config.http.bind, config.http.port).parse()?;

            let router = api::router_shared(
                Arc::clone(&pools),
                Arc::clone(&clickhouse_pg_pools),
                default_chain_id,
                broadcaster.clone(),
                Arc::clone(&clickhouse_configs),
                Arc::clone(&clickhouse_engines),
                trusted_cidrs,
            );

            info!(addr = %addr, "Starting HTTP API server (hot-reload enabled)");

            let listener = tokio::net::TcpListener::bind(addr).await?;
            let mut shutdown_rx_api = shutdown_tx.subscribe();

            tokio::spawn(async move {
                axum::serve(
                    listener,
                    router.into_make_service_with_connect_info::<SocketAddr>(),
                )
                .with_graceful_shutdown(async move {
                    let _ = shutdown_rx_api.recv().await;
                })
                .await
                .ok();
            });
        }

        let pools_for_watcher = Arc::clone(&pools);
        let ch_pg_pools_for_watcher = Arc::clone(&clickhouse_pg_pools);
        let clickhouse_configs_for_watcher = Arc::clone(&clickhouse_configs);
        let broadcaster_for_watcher = broadcaster.clone();
        let shutdown_tx_for_watcher = shutdown_tx.clone();

        tokio::spawn(async move {
            while let Some(event) = chain_rx.recv().await {
                match initialize_chain(&event.chain, Arc::clone(&clickhouse_configs_for_watcher))
                    .await
                {
                    Ok(runtime) => {
                        if let Some(api_pool) = runtime.api_pool.clone() {
                            pools_for_watcher
                                .write()
                                .await
                                .insert(event.chain.chain_id, api_pool);
                        }
                        if let Some(ch_pg_pool) = runtime.clickhouse_pg_pool.clone() {
                            ch_pg_pools_for_watcher
                                .write()
                                .await
                                .insert(event.chain.chain_id, ch_pg_pool);
                        }

                        spawn_sync_engine(
                            event.chain,
                            runtime.sinks,
                            broadcaster_for_watcher.clone(),
                            shutdown_tx_for_watcher.subscribe(),
                        );
                    }
                    Err(e) => {
                        error!(error = %e, chain = %event.chain.name, "Failed to initialize new chain");
                    }
                }
            }
        });
    } else if config.http.enabled && default_chain_id != 0 {
        let addr: SocketAddr = format!("{}:{}", config.http.bind, config.http.port).parse()?;
        let router = api::router_with_options(
            pools.read().await.clone(),
            default_chain_id,
            broadcaster.clone(),
            clickhouse_configs.read().await.clone(),
            clickhouse_pg_pools.read().await.clone(),
            clickhouse_engines.read().await.clone(),
            &config.http,
        )?;

        info!(addr = %addr, "Starting HTTP API server");

        let listener = tokio::net::TcpListener::bind(addr).await?;
        let mut shutdown_rx_api = shutdown_tx.subscribe();

        tokio::spawn(async move {
            axum::serve(
                listener,
                router.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .with_graceful_shutdown(async move {
                let _ = shutdown_rx_api.recv().await;
            })
            .await
            .ok();
        });
    }

    let mut shutdown_rx = shutdown_tx.subscribe();
    let _ = shutdown_rx.recv().await;

    Ok(())
}

/// Initialize a chain's stores: PostgreSQL pool + migrations when configured,
/// ClickHouse sink + schema when enabled. At least one is required (validated
/// at config load). ClickHouse failures are fatal only when it is the sole store.
async fn initialize_chain(
    chain: &ChainConfig,
    clickhouse_configs: SharedClickHouseConfigs,
) -> Result<ChainRuntime> {
    let mut throttled_pool = None;

    if let Some(pg_config) = &chain.postgres {
        let pg_url = pg_config.resolved_url()?;
        info!(chain = %chain.name, "Connecting to database with throttled pool...");
        let pool = ThrottledPool::new(&pg_url).await?;

        info!(chain = %chain.name, "Running migrations...");
        db::run_migrations(&pool.pool).await?;

        {
            let pool = pool.pool.clone();
            let chain_name = chain.name.clone();
            tokio::spawn(async move {
                // Concurrent index creation can take longer than startup on
                // existing installations, so keep it outside the blocking boot path.
                match db::run_post_startup_migrations(&pool).await {
                    Ok(()) => info!(chain = %chain_name, "Post-startup migrations complete"),
                    Err(e) => warn!(
                        error = %e,
                        chain = %chain_name,
                        "Post-startup migrations failed"
                    ),
                }
            });
        }

        // Seed in-memory watermarks and row counts from existing DB data
        // so that status display is accurate immediately after restart.
        seed_metrics_from_db(&pool.pool).await;

        throttled_pool = Some(pool);
    }

    // Store ClickHouse config for this chain (if enabled)
    if let Some(ch_config) = chain.clickhouse_enabled() {
        let config = ChainClickHouseConfig {
            enabled: ch_config.enabled,
            url: ch_config.url.clone(),
            failover_urls: ch_config.failover_urls.clone(),
        };
        clickhouse_configs
            .write()
            .await
            .insert(chain.chain_id, config);
        info!(chain = %chain.name, "ClickHouse OLAP engine configured");
    }

    // Build the ClickHouse direct-write sink. With PostgreSQL present a CH
    // failure degrades to PG-only (current behavior); without PostgreSQL the
    // chain cannot run, so the error propagates.
    let ch_sink = match build_clickhouse_sink(chain).await {
        Ok(sink) => sink,
        Err(e) if chain.postgres.is_some() => {
            error!(
                error = %e,
                chain = %chain.name,
                "Failed to initialize ClickHouse sink (continuing without CH)"
            );
            None
        }
        Err(e) => return Err(e).with_context(|| {
            format!(
                "chain '{}' has no PostgreSQL configured and its ClickHouse sink failed to initialize",
                chain.name
            )
        }),
    };

    let sinks = match (&throttled_pool, ch_sink) {
        (Some(pool), Some(ch)) => SinkSet::new(pool.pool.clone()).with_clickhouse(ch),
        (Some(pool), None) => SinkSet::new(pool.pool.clone()),
        (None, Some(ch)) => SinkSet::clickhouse_only(ch),
        (None, None) => anyhow::bail!(
            "chain '{}': no storage engine available (validated config should prevent this)",
            chain.name
        ),
    };

    // Use a separate read-only API pool if API credentials are configured,
    // otherwise fall back to the shared pool.
    let api_pool = match &throttled_pool {
        Some(pool) => match resolve_api_pool(chain).await {
            Some(api_pool) => Some(api_pool),
            None => Some(pool.pool.clone()),
        },
        None => None,
    };

    // pg_clickhouse pool for engine=clickhouse_pg queries (best-effort).
    let clickhouse_pg_pool = match chain
        .clickhouse_enabled()
        .map(|ch| ch.resolved_pg_url())
        .transpose()?
        .flatten()
    {
        Some(url) => match db::connect_pool(&url, CLICKHOUSE_PG_POOL_SIZE).await {
            Ok(pool) => {
                info!(chain = %chain.name, "pg_clickhouse pool connected");
                Some(pool)
            }
            Err(e) => {
                error!(
                    error = %e,
                    chain = %chain.name,
                    "Failed to connect pg_clickhouse pool (engine=clickhouse_pg unavailable)"
                );
                None
            }
        },
        None => None,
    };

    Ok(ChainRuntime {
        sinks,
        api_pool,
        clickhouse_pg_pool,
    })
}

/// Build and schema-initialize the ClickHouse direct-write sink, if enabled.
async fn build_clickhouse_sink(chain: &ChainConfig) -> Result<Option<ClickHouseSink>> {
    let Some(ch_config) = chain.clickhouse_enabled() else {
        return Ok(None);
    };

    let database = ch_config
        .database
        .clone()
        .unwrap_or_else(|| format!("tidx_{}", chain.chain_id));

    let ch_password = ch_config.resolved_password()?;

    let sink = ClickHouseSink::new(
        &ch_config.url,
        &database,
        ch_config.user.as_deref(),
        ch_password.as_deref(),
    )?;
    sink.ensure_schema_only().await?;

    info!(
        chain = %chain.name,
        database = %database,
        "ClickHouse direct-write sink enabled"
    );

    seed_metrics_from_clickhouse(&sink).await;

    Ok(Some(sink))
}

/// Create the dedicated API pool when `postgres.api_url` is configured.
/// Falls back to the shared pool (None) on failure.
async fn resolve_api_pool(chain: &ChainConfig) -> Option<Pool> {
    let api_url = match chain.postgres.as_ref()?.resolved_api_url() {
        Ok(Some(url)) => url,
        Ok(None) => return None,
        Err(e) => {
            warn!(error = %e, chain = %chain.name, "Failed to resolve API pool URL, using shared pool");
            return None;
        }
    };

    info!(chain = %chain.name, "Creating separate API pool with dedicated credentials");
    match db::create_pool(&api_url).await {
        Ok(pool) => Some(pool),
        Err(e) => {
            warn!(error = %e, chain = %chain.name, "Failed to create API pool, using shared pool");
            None
        }
    }
}

fn spawn_sync_engine(
    chain: ChainConfig,
    sinks: SinkSet,
    broadcaster: Arc<Broadcaster>,
    shutdown_rx: tokio::sync::broadcast::Receiver<()>,
) {
    let rpc_url = match chain.resolved_rpc_url() {
        Ok(url) => url,
        Err(e) => {
            error!(
                error = %e,
                chain = %chain.name,
                "Failed to resolve RPC URL"
            );
            return;
        }
    };
    let redacted_rpc_url = tidx::config::redact_url_credentials(&rpc_url);

    info!(
        chain = %chain.name,
        chain_id = chain.chain_id,
        rpc = %redacted_rpc_url,
        postgres = chain.postgres.is_some(),
        clickhouse = sinks.clickhouse().is_some(),
        "Starting sync for chain"
    );

    let backfill_first = chain.backfill_first;
    let trust_rpc = chain.trust_rpc;

    tokio::spawn(async move {
        // ClickHouse maintenance: mirror-backfill from PG (no-op without PG),
        // then repair historical derived-table gaps. Runs in the background so
        // the sync engine starts immediately. Retries with exponential backoff.
        if sinks.clickhouse().is_some() {
            let derived_repair_sink = chain
                .clickhouse_enabled()
                .is_some_and(|ch| ch.repair_derived_on_startup)
                .then(|| sinks.clickhouse().cloned())
                .flatten();

            if derived_repair_sink.is_some() {
                info!(
                    chain = %chain.name,
                    "ClickHouse derived table repair scheduled after base backfill"
                );
            } else {
                info!(
                    chain = %chain.name,
                    "ClickHouse startup derived table repair disabled"
                );
            }

            let backfill_sinks = sinks.clone();
            let backfill_chain_name = chain.name.clone();
            let backfill_chain_id = chain.chain_id;
            tokio::spawn(async move {
                let mut attempt: u32 = 0;
                loop {
                    match backfill_sinks.backfill_clickhouse(backfill_chain_id).await {
                        Ok(()) => break,
                        Err(e) => {
                            attempt += 1;
                            let delay_secs =
                                retry_delay_secs(attempt, CLICKHOUSE_BACKFILL_RETRY_MAX_SECS);
                            error!(
                                error = %e,
                                chain = %backfill_chain_name,
                                attempt,
                                retry_in_secs = delay_secs,
                                "ClickHouse backfill failed, retrying"
                            );
                            tokio::time::sleep(std::time::Duration::from_secs(delay_secs)).await;
                        }
                    }
                }

                if let Some(derived_repair_sink) = derived_repair_sink {
                    run_clickhouse_derived_repair_loop(derived_repair_sink, backfill_chain_name)
                        .await;
                }
            });
        }

        // Create sync engine with configured sinks (retry on transient RPC failures)
        let mut engine = loop {
            match SyncEngine::new(sinks.clone(), &rpc_url).await {
                Ok(e) => {
                    break e
                        .with_broadcaster(broadcaster)
                        .with_batch_size(chain.batch_size)
                        .with_concurrency(chain.concurrency)
                        .with_backfill_first(backfill_first)
                        .with_trust_rpc(trust_rpc);
                }
                Err(e) => {
                    warn!(error = %e, chain = %chain.name, "Failed to create sync engine, retrying in 10s");
                    tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                }
            }
        };

        if let Err(e) = engine.run(shutdown_rx).await {
            error!(error = %e, chain = %chain.name, "Sync engine failed");
        }
    });
}

fn retry_delay_secs(attempt: u32, max_secs: u64) -> u64 {
    2u64.saturating_pow(attempt).min(max_secs)
}

async fn run_clickhouse_derived_repair_loop(sink: ClickHouseSink, chain_name: String) {
    info!(
        chain = %chain_name,
        database = %sink.database(),
        "Starting ClickHouse derived table repair"
    );

    let mut attempt: u32 = 0;
    loop {
        match sink.repair_derived_backfill_gaps().await {
            Ok(()) => {
                info!(
                    chain = %chain_name,
                    database = %sink.database(),
                    "ClickHouse derived table repair complete"
                );
                break;
            }
            Err(e) => {
                attempt += 1;
                let delay_secs =
                    retry_delay_secs(attempt, CLICKHOUSE_DERIVED_REPAIR_RETRY_MAX_SECS);
                warn!(
                    error = %e,
                    chain = %chain_name,
                    attempt,
                    retry_in_secs = delay_secs,
                    "ClickHouse derived table repair failed, backing off"
                );
                tokio::time::sleep(std::time::Duration::from_secs(delay_secs)).await;
            }
        }
    }
}

/// Seed in-memory ClickHouse watermarks and row counts from existing data.
async fn seed_metrics_from_clickhouse(sink: &ClickHouseSink) {
    for table in ["blocks", "txs", "logs", "receipts"] {
        if let Ok(Some(max)) = sink.max_block_in_table(table).await {
            tidx::metrics::update_sink_watermark("clickhouse", table, max);
        }
        if let Ok(count) = sink.row_count(table).await {
            if count > 0 {
                tidx::metrics::increment_sink_row_count("clickhouse", table, count);
            }
        }
    }
}

/// Seed in-memory watermarks and row counts from existing database data.
/// Uses index-only scans for watermarks (instant) and pg_stat for approximate row counts.
async fn seed_metrics_from_db(pool: &tidx::db::Pool) {
    let Ok(conn) = pool.get().await else { return };

    // Seed watermarks: MAX(block_num) per table (fast, index-only scans)
    for (table, col) in [
        ("blocks", "num"),
        ("txs", "block_num"),
        ("logs", "block_num"),
        ("receipts", "block_num"),
    ] {
        let query = format!("SELECT MAX({col}) FROM {table}");
        if let Ok(row) = conn.query_one(&query, &[]).await {
            if let Some(max) = row.get::<_, Option<i64>>(0) {
                tidx::metrics::update_sink_watermark("postgres", table, max);
            }
        }
    }

    // Seed approximate row counts from pg_stat (instant, no table scan)
    let query = "SELECT relname, n_live_tup FROM pg_stat_user_tables WHERE relname IN ('blocks', 'txs', 'logs', 'receipts')";
    if let Ok(rows) = conn.query(query, &[]).await {
        for row in rows {
            let table: String = row.get(0);
            let count: i64 = row.get(1);
            if count > 0 {
                tidx::metrics::increment_sink_row_count("postgres", &table, count as u64);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_delay_backs_off_until_cap() {
        assert_eq!(retry_delay_secs(1, 300), 2);
        assert_eq!(retry_delay_secs(2, 300), 4);
        assert_eq!(retry_delay_secs(8, 300), 256);
        assert_eq!(retry_delay_secs(9, 300), 300);
        assert_eq!(retry_delay_secs(127, 300), 300);
    }
}
