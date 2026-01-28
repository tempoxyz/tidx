use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use clap::Args as ClapArgs;
use metrics_exporter_prometheus::PrometheusBuilder;
use tokio::sync::RwLock;
use tracing::{error, info};

use tidx::api::{self, SharedDuckDbPools, SharedPools};
use tidx::broadcast::Broadcaster;
use tidx::config::{ChainConfig, Config, ConfigWatcher, NewChainEvent};
use tidx::db::{self, DuckDbPool, ThrottledPool};
use tidx::sync::engine::SyncEngine;
use tidx::sync::{Replicator, ReplicatorHandle};

#[derive(ClapArgs)]
pub struct Args {
    /// Path to config file
    #[arg(short, long, default_value = "config.toml")]
    pub config: PathBuf,

    /// Disable config hot-reloading
    #[arg(long)]
    pub no_watch: bool,
}

pub async fn run(args: Args) -> Result<()> {
    let config = Config::load(&args.config)?;

    let duckdb_count = config.chains.iter().filter(|c| c.duckdb_path.is_some()).count();
    info!(
        chains = config.chains.len(),
        duckdb_chains = duckdb_count,
        "Loaded config"
    );

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

    tokio::spawn({
        let shutdown_tx = shutdown_tx.clone();
        async move {
            tokio::signal::ctrl_c().await.ok();
            info!("Shutting down...");
            let _ = shutdown_tx.send(());
        }
    });

    let pools: SharedPools = Arc::new(RwLock::new(HashMap::new()));
    let duckdb_pools: SharedDuckDbPools = Arc::new(RwLock::new(HashMap::new()));
    let mut default_chain_id = 0u64;

    for chain in &config.chains {
        let (throttled_pool, replicator_handle) = initialize_chain(
            chain,
            Arc::clone(&duckdb_pools),
        ).await?;

        if default_chain_id == 0 {
            default_chain_id = chain.chain_id;
        }

        // API uses the shared pool (backfill is throttled, so API isn't starved)
        pools.write().await.insert(chain.chain_id, throttled_pool.pool.clone());

        spawn_sync_engine(
            chain.clone(),
            throttled_pool,
            replicator_handle,
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
                Arc::clone(&duckdb_pools),
                http_config,
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
        let duckdb_pools_for_watcher = Arc::clone(&duckdb_pools);
        let broadcaster_for_watcher = broadcaster.clone();
        let shutdown_tx_for_watcher = shutdown_tx.clone();

        tokio::spawn(async move {
            while let Some(event) = chain_rx.recv().await {
                match initialize_chain(&event.chain, Arc::clone(&duckdb_pools_for_watcher)).await {
                    Ok((throttled_pool, replicator_handle)) => {
                        pools_for_watcher.write().await.insert(event.chain.chain_id, throttled_pool.pool.clone());

                        spawn_sync_engine(
                            event.chain,
                            throttled_pool,
                            replicator_handle,
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
            duckdb_pools.read().await.clone(),
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
    duckdb_pools: SharedDuckDbPools,
) -> Result<(ThrottledPool, Option<ReplicatorHandle>)> {
    info!(chain = %chain.name, db = %chain.pg_url, "Connecting to database with throttled pool...");
    let throttled_pool = ThrottledPool::new(&chain.pg_url).await?;

    info!(chain = %chain.name, "Running migrations...");
    db::run_migrations(&throttled_pool.pool).await?;

    let replicator_handle = if let Some(ref duckdb_path) = chain.duckdb_path {
        info!(chain = %chain.name, path = %duckdb_path, "Initializing DuckDB");
        let duckdb_pool = Arc::new(DuckDbPool::new(duckdb_path)?);

        // Replicator uses the shared pool (its gap-fill is lower priority)
        let (replicator, handle) = Replicator::new(
            duckdb_pool.clone(),
            throttled_pool.pool.clone(),
            10_000,
            chain.chain_id,
        );
        tokio::spawn(replicator.run());

        duckdb_pools.write().await.insert(chain.chain_id, duckdb_pool);
        Some(handle)
    } else {
        None
    };

    Ok((throttled_pool, replicator_handle))
}

fn spawn_sync_engine(
    chain: ChainConfig,
    throttled_pool: ThrottledPool,
    replicator_handle: Option<ReplicatorHandle>,
    broadcaster: Arc<Broadcaster>,
    shutdown_rx: tokio::sync::broadcast::Receiver<()>,
) {
    info!(
        chain = %chain.name,
        chain_id = chain.chain_id,
        rpc = %chain.rpc_url,
        duckdb = replicator_handle.is_some(),
        backfill_limit = throttled_pool.backfill_semaphore.available_permits(),
        "Starting sync for chain (throttled pool: 16 connections, backfill limited)"
    );

    let backfill_first = chain.backfill_first;

    tokio::spawn(async move {
        // Create sync engine with throttled pool
        let mut engine = match SyncEngine::new(throttled_pool, &chain.rpc_url).await {
            Ok(e) => e
                .with_broadcaster(broadcaster)
                .with_batch_size(chain.batch_size)
                .with_concurrency(chain.concurrency)
                .with_backfill_first(backfill_first),
            Err(e) => {
                error!(error = %e, chain = %chain.name, "Failed to create sync engine");
                return;
            }
        };

        if let Some(handle) = replicator_handle {
            engine = engine.with_replicator(handle);
        }

        // Run the sync engine - handles both realtime sync and gap sync
        // Gap sync fills ALL gaps from most recent to earliest (replaces backfill)
        if let Err(e) = engine.run(shutdown_rx).await {
            error!(error = %e, chain = %chain.name, "Sync engine failed");
        }
    });
}
