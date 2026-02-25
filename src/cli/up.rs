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
use tidx::sync::sink::SinkSet;

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

        // Initialize ClickHouse if configured (for each chain)
        if let Some(ref ch_config) = chain.clickhouse {
            if ch_config.enabled {
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
            shutdown_tx.subscribe(),
        );
    }

    let (chain_tx, mut chain_rx) = tokio::sync::mpsc::channel::<NewChainEvent>(16);

    if !args.no_watch {
        let watcher = ConfigWatcher::new(args.config.clone(), &config, chain_tx);
        let http_config = watcher.http_config();
        watcher.start()?;

        if config.http.enabled && default_chain_id != 0 {
            let addr: SocketAddr = format!("{}:{}", config.http.bind, config.http.port).parse()?;

            let router = api::router_shared(
                Arc::clone(&pools),
                default_chain_id,
                broadcaster.clone(),
                Arc::clone(&clickhouse_configs),
                http_config,
                Arc::clone(&clickhouse_engines),
                config.http.trusted_cidrs.clone(),
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
        );

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
    shutdown_rx: tokio::sync::broadcast::Receiver<()>,
) {
    info!(
        chain = %chain.name,
        chain_id = chain.chain_id,
        rpc = %chain.rpc_url,
        backfill_limit = throttled_pool.backfill_semaphore.available_permits(),
        "Starting sync for chain (throttled pool: 16 connections, backfill limited)"
    );

    let backfill_first = chain.backfill_first;
    let trust_rpc = chain.trust_rpc;

    tokio::spawn(async move {
        // Build SinkSet with PG (always) + optional ClickHouse direct-write sink
        let mut sinks = SinkSet::new(throttled_pool.inner().clone());

        if let Some(ref ch_config) = chain.clickhouse {
            if ch_config.enabled {
                let database = ch_config
                    .database
                    .clone()
                    .unwrap_or_else(|| format!("tidx_{}", chain.chain_id));

                match ClickHouseSink::new(&ch_config.url, &database) {
                    Ok(ch_sink) => {
                        if let Err(e) = ch_sink.ensure_schema().await {
                            error!(
                                error = %e,
                                chain = %chain.name,
                                "Failed to initialize ClickHouse schema (continuing without CH sink)"
                            );
                        } else {
                            info!(
                                chain = %chain.name,
                                database = %database,
                                "ClickHouse direct-write sink enabled"
                            );
                            sinks = sinks.with_clickhouse(ch_sink);
                        }
                    }
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

        // Auto-backfill ClickHouse from PostgreSQL in background (non-blocking).
        // SyncEngine starts immediately so new blocks are indexed while CH catches up.
        {
            let backfill_sinks = sinks.clone();
            let backfill_chain_name = chain.name.clone();
            tokio::spawn(async move {
                if let Err(e) = backfill_sinks.backfill_clickhouse().await {
                    error!(
                        error = %e,
                        chain = %backfill_chain_name,
                        "ClickHouse backfill failed (CH will catch up during sync)"
                    );
                }
            });
        }

        // Create sync engine with throttled pool and configured sinks (retry on transient RPC failures)
        let mut engine = loop {
            match SyncEngine::new(throttled_pool.clone(), sinks.clone(), &chain.rpc_url).await {
                Ok(e) => break e
                    .with_broadcaster(broadcaster)
                    .with_batch_size(chain.batch_size)
                    .with_concurrency(chain.concurrency)
                    .with_backfill_first(backfill_first)
                    .with_trust_rpc(trust_rpc),
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

/// Seed in-memory watermarks and row counts from existing database data.
/// Uses index-only scans for watermarks (instant) and pg_stat for approximate row counts.
async fn seed_metrics_from_db(pool: &tidx::db::Pool) {
    let Ok(conn) = pool.get().await else { return };

    // Seed watermarks: MAX(block_num) per table (fast, index-only scans)
    for (table, col) in [("blocks", "num"), ("txs", "block_num"), ("logs", "block_num"), ("receipts", "block_num")] {
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
