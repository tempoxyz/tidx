use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
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
use tidx::db::{self, ThrottledPool};
use tidx::sync::ch_sink::ClickHouseSink;
use tidx::sync::engine::SyncEngine;
use tidx::sync::pruner::Pruner;
use tidx::sync::sink::{ClickHouseBackfillPlan, SinkSet};
use tidx::sync::tiered_sync::TieredSync;

const CLICKHOUSE_BACKFILL_RETRY_MAX_SECS: u64 = 10;
const CLICKHOUSE_DERIVED_REPAIR_RETRY_MAX_SECS: u64 = 300;

#[derive(ClapArgs)]
pub struct Args {
    /// Path to config file
    #[arg(short, long, default_value = "config.toml")]
    pub config: PathBuf,

    /// Disable config hot-reloading
    #[arg(long)]
    pub no_watch: bool,
}

use std::path::PathBuf;

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
    let clickhouse_configs: SharedClickHouseConfigs = Arc::new(RwLock::new(HashMap::new()));
    let clickhouse_engines: SharedClickHouseEngines = Arc::new(RwLock::new(HashMap::new()));
    let mut default_chain_id = 0u64;

    for chain in &config.chains {
        let throttled_pool = initialize_chain(chain, Arc::clone(&clickhouse_configs)).await?;

        if default_chain_id == 0 {
            default_chain_id = chain.chain_id;
        }

        // Use a separate read-only API pool if API credentials are configured,
        // otherwise fall back to the shared pool.
        let api_pool = match chain.resolved_api_pg_url()? {
            Some(api_url) => {
                info!(chain = %chain.name, "Creating separate API pool with dedicated credentials");
                db::create_pool(&api_url).await?
            }
            None => throttled_pool.pool.clone(),
        };
        pools.write().await.insert(chain.chain_id, api_pool);

        spawn_sync_engine(
            chain.clone(),
            throttled_pool,
            broadcaster.clone(),
            Arc::clone(&clickhouse_engines),
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
        let clickhouse_configs_for_watcher = Arc::clone(&clickhouse_configs);
        let clickhouse_engines_for_watcher = Arc::clone(&clickhouse_engines);
        let broadcaster_for_watcher = broadcaster.clone();
        let shutdown_tx_for_watcher = shutdown_tx.clone();

        tokio::spawn(async move {
            while let Some(event) = chain_rx.recv().await {
                match initialize_chain(&event.chain, Arc::clone(&clickhouse_configs_for_watcher))
                    .await
                {
                    Ok(throttled_pool) => {
                        let api_pool = match event.chain.resolved_api_pg_url() {
                            Ok(Some(api_url)) => match db::create_pool(&api_url).await {
                                Ok(pool) => pool,
                                Err(_) => throttled_pool.pool.clone(),
                            },
                            _ => throttled_pool.pool.clone(),
                        };
                        pools_for_watcher
                            .write()
                            .await
                            .insert(event.chain.chain_id, api_pool);

                        spawn_sync_engine(
                            event.chain,
                            throttled_pool,
                            broadcaster_for_watcher.clone(),
                            Arc::clone(&clickhouse_engines_for_watcher),
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

async fn initialize_chain(
    chain: &ChainConfig,
    clickhouse_configs: SharedClickHouseConfigs,
) -> Result<ThrottledPool> {
    let pg_url = chain.resolved_pg_url()?;
    info!(chain = %chain.name, "Connecting to database with throttled pool...");
    let throttled_pool = ThrottledPool::new(&pg_url).await?;

    info!(chain = %chain.name, "Running migrations...");
    db::run_migrations(&throttled_pool.pool).await?;

    {
        let pool = throttled_pool.pool.clone();
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
    seed_metrics_from_db(&throttled_pool.pool).await;

    // Store ClickHouse config for this chain (if enabled)
    if let Some(ref ch_config) = chain.clickhouse {
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

    Ok(throttled_pool)
}

fn spawn_sync_engine(
    chain: ChainConfig,
    throttled_pool: ThrottledPool,
    broadcaster: Arc<Broadcaster>,
    clickhouse_engines: SharedClickHouseEngines,
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
        backfill_limit = throttled_pool.backfill_semaphore.available_permits(),
        "Starting sync for chain (throttled pool: 16 connections, backfill limited)"
    );

    let backfill_first = chain.backfill_first;
    let trust_rpc = chain.trust_rpc;

    tokio::spawn(async move {
        // Build SinkSet with PG (always) + optional ClickHouse direct-write sink
        let mut sinks = SinkSet::new(throttled_pool.inner().clone());
        let mut derived_repair_sink = None;

        if let Some(ref ch_config) = chain.clickhouse {
            if ch_config.enabled {
                let database = ch_config
                    .database
                    .clone()
                    .unwrap_or_else(|| format!("tidx_{}", chain.chain_id));

                let ch_password = match ch_config.resolved_password() {
                    Ok(p) => p,
                    Err(e) => {
                        error!(
                            error = %e,
                            chain = %chain.name,
                            "Failed to resolve ClickHouse password (continuing without CH)"
                        );
                        None
                    }
                };
                if ch_password.is_none() && ch_config.password_env.is_some() {
                    // password_env was set but resolution failed — skip CH
                } else {
                    match ClickHouseSink::new(
                        &ch_config.url,
                        &database,
                        ch_config.user.as_deref(),
                        ch_password.as_deref(),
                    )
                    .map(|sink| sink.with_replicated_database(ch_config.replicated_database))
                    {
                        Ok(ch_sink) => match ch_sink.ensure_schema_only().await {
                            Ok(()) => {
                                match ClickHouseEngine::new(ch_config, chain.chain_id) {
                                    Ok(engine) => {
                                        clickhouse_engines
                                            .write()
                                            .await
                                            .insert(chain.chain_id, Arc::new(engine));
                                        info!(
                                            chain = %chain.name,
                                            chain_id = chain.chain_id,
                                            "ClickHouse OLAP engine initialized"
                                        );
                                    }
                                    Err(e) => {
                                        error!(
                                            error = %e,
                                            chain = %chain.name,
                                            "Failed to create ClickHouse engine"
                                        );
                                    }
                                }
                                info!(
                                    chain = %chain.name,
                                    database = %database,
                                    "ClickHouse direct-write sink enabled"
                                );
                                if ch_config.repair_derived_on_startup {
                                    derived_repair_sink = Some(ch_sink.clone());
                                    info!(
                                        chain = %chain.name,
                                        database = %database,
                                        "ClickHouse derived table repair scheduled after base backfill"
                                    );
                                } else {
                                    info!(
                                        chain = %chain.name,
                                        database = %database,
                                        "ClickHouse startup derived table repair disabled"
                                    );
                                }
                                seed_metrics_from_clickhouse(&ch_sink).await;
                                sinks = sinks.with_clickhouse(ch_sink);

                                // Tiered storage: pg_clickhouse foreign tables
                                // + tiered.* views over hot PG and the CH
                                // archive. Requires retention (the boundary
                                // maintenance) and the pg_clickhouse extension.
                                if chain.retention.is_some() {
                                    let fdw_url =
                                        ch_config.fdw_url.as_deref().unwrap_or(&ch_config.url);
                                    let target = db::tiered::FdwTarget::new(
                                        fdw_url,
                                        database.clone(),
                                        ch_config.user.clone(),
                                        ch_password.clone(),
                                    );
                                    let result = match target {
                                        Ok(target) => {
                                            db::tiered::bootstrap(
                                                throttled_pool.inner(),
                                                &target,
                                                chain.chain_id,
                                            )
                                            .await
                                        }
                                        Err(e) => Err(e),
                                    };
                                    if let Err(e) = result {
                                        warn!(
                                            error = %e,
                                            chain = %chain.name,
                                            "Tiered storage bootstrap failed; engine=tiered unavailable \
                                             (is the pg_clickhouse extension installed in PostgreSQL?)"
                                        );
                                    }
                                }
                            }
                            Err(e) => {
                                error!(
                                    error = %e,
                                    chain = %chain.name,
                                    "Failed to initialize ClickHouse schema (continuing without CH sink)"
                                );
                            }
                        },
                        Err(e) => {
                            error!(
                                error = %e,
                                chain = %chain.name,
                                "Failed to create ClickHouse sink (continuing without CH)"
                            );
                        }
                    }
                }
            }
        }

        // Non-retention deployments retain the legacy PG→CH catch-up path.
        // Tiered deployments use the RPC→CH archive worker below instead, so
        // historical rows never need to pass through PostgreSQL.
        if chain.retention.is_none() {
            let backfill_sinks = sinks.clone();
            let backfill_chain_name = chain.name.clone();
            let backfill_chain_id = chain.chain_id;
            let derived_repair_sink = derived_repair_sink.clone();
            let backfill_plan = loop {
                match backfill_sinks
                    .clickhouse_backfill_plan(backfill_chain_id)
                    .await
                {
                    Ok(plan) => break plan,
                    Err(e) => {
                        error!(
                            error = %e,
                            chain = %backfill_chain_name,
                            "Failed to snapshot ClickHouse backfill boundary, retrying"
                        );
                        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                    }
                }
            };

            if backfill_plan.complete_before_realtime {
                info!(
                    chain = %backfill_chain_name,
                    upper_bound = ?backfill_plan.upper_bound,
                    "Completing initial ClickHouse catch-up before realtime sync"
                );
                run_legacy_clickhouse_backfill(
                    &backfill_sinks,
                    &backfill_chain_name,
                    backfill_chain_id,
                    backfill_plan,
                )
                .await;

                if let Some(derived_repair_sink) = derived_repair_sink {
                    tokio::spawn(run_clickhouse_derived_repair_loop(
                        derived_repair_sink,
                        backfill_chain_name,
                    ));
                }
            } else {
                tokio::spawn(async move {
                    run_legacy_clickhouse_backfill(
                        &backfill_sinks,
                        &backfill_chain_name,
                        backfill_chain_id,
                        backfill_plan,
                    )
                    .await;

                    if let Some(derived_repair_sink) = derived_repair_sink {
                        run_clickhouse_derived_repair_loop(
                            derived_repair_sink,
                            backfill_chain_name,
                        )
                        .await;
                    }
                });
            }
        } else if let Some(derived_repair_sink) = derived_repair_sink {
            let chain_name = chain.name.clone();
            tokio::spawn(async move {
                run_clickhouse_derived_repair_loop(derived_repair_sink, chain_name).await;
            });
        }

        // Retention-enabled deployments archive full history from RPC into
        // ClickHouse, then hydrate only the configured PostgreSQL hot window
        // from checkpointed archive ranges. The hot boundary can move in
        // either direction when pg_keep changes.
        if let Some(ref retention) = chain.retention {
            match TieredSync::new(
                sinks.clone(),
                &rpc_url,
                chain.chain_id,
                retention,
                chain.batch_size,
                chain.concurrency,
            ) {
                Ok(tiered_sync) => {
                    info!(
                        chain = %chain.name,
                        pg_keep = %retention.pg_keep,
                        "ClickHouse archive + PostgreSQL hot-window sync enabled"
                    );
                    tokio::spawn(tiered_sync.run(shutdown_rx.resubscribe()));
                }
                Err(e) => {
                    error!(error = %e, chain = %chain.name, "Invalid tiered sync config");
                }
            }
        }

        // Tiered-storage pruner: drops PG partitions outside the retention
        // window once the ClickHouse archive durably holds their data.
        if let Some(ref retention) = chain.retention {
            match Pruner::new(sinks.clone(), chain.chain_id, retention) {
                Ok(pruner) => {
                    info!(
                        chain = %chain.name,
                        pg_keep = %retention.pg_keep,
                        prune_interval = %retention.prune_interval,
                        require_clickhouse = retention.require_clickhouse,
                        "Retention pruner enabled"
                    );
                    tokio::spawn(pruner.run(shutdown_rx.resubscribe()));
                }
                Err(e) => {
                    error!(error = %e, chain = %chain.name, "Invalid retention config; pruner disabled");
                }
            }
        }

        // Create sync engine with throttled pool and configured sinks (retry on transient RPC failures)
        let mut engine = loop {
            match SyncEngine::new(throttled_pool.clone(), sinks.clone(), &rpc_url).await {
                Ok(e) => {
                    break e
                        .with_broadcaster(broadcaster)
                        .with_batch_size(chain.batch_size)
                        .with_concurrency(chain.concurrency)
                        .with_backfill_first(backfill_first && chain.retention.is_none())
                        .with_gapfill_enabled(chain.retention.is_none())
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

async fn run_legacy_clickhouse_backfill(
    sinks: &SinkSet,
    chain_name: &str,
    chain_id: u64,
    plan: ClickHouseBackfillPlan,
) {
    let mut attempt: u32 = 0;
    loop {
        match sinks.run_clickhouse_startup_backfill(chain_id, plan).await {
            Ok(()) => break,
            Err(e) => {
                attempt += 1;
                let delay_secs = retry_delay_secs(attempt, CLICKHOUSE_BACKFILL_RETRY_MAX_SECS);
                error!(
                    error = %e,
                    chain = %chain_name,
                    attempt,
                    retry_in_secs = delay_secs,
                    "ClickHouse backfill failed, retrying"
                );
                tokio::time::sleep(std::time::Duration::from_secs(delay_secs)).await;
            }
        }
    }
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
