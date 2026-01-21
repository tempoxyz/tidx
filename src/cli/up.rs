use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use clap::Args as ClapArgs;
use metrics_exporter_prometheus::PrometheusBuilder;
use tracing::info;

use ak47::api;
use ak47::broadcast::Broadcaster;
use ak47::config::Config;
use ak47::db::{self, DuckDbPool};
use ak47::sync::engine::SyncEngine;
use ak47::sync::{backfill_from_postgres, Replicator, ReplicatorHandle};

#[derive(ClapArgs)]
pub struct Args {
    /// Path to config file
    #[arg(short, long, default_value = "config.toml")]
    pub config: PathBuf,
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
    let (shutdown_tx, _shutdown_rx) = tokio::sync::broadcast::channel(1);

    tokio::spawn({
        let shutdown_tx = shutdown_tx.clone();
        async move {
            tokio::signal::ctrl_c().await.ok();
            info!("Shutting down...");
            let _ = shutdown_tx.send(());
        }
    });

    // Create pools and run migrations for each chain
    let mut chain_pools = Vec::new();
    let mut pools_map = std::collections::HashMap::new();
    let mut duckdb_pools_map = std::collections::HashMap::new();
    let mut default_chain_id = 0u64;

    for chain in &config.chains {
        info!(chain = %chain.name, db = %chain.pg_url, "Connecting to database...");
        let pool = db::create_pool(&chain.pg_url).await?;

        info!(chain = %chain.name, "Running migrations...");
        db::run_migrations(&pool).await?;

        // Initialize per-chain DuckDB if configured
        let replicator_handle: Option<ReplicatorHandle> = if let Some(ref duckdb_path) = chain.duckdb_path {
            info!(chain = %chain.name, path = %duckdb_path, "Initializing DuckDB");
            let duckdb_pool = Arc::new(DuckDbPool::new(duckdb_path)?);

            // Backfill DuckDB from PostgreSQL (catches up any missed blocks)
            let pg_pool = pool.clone();
            let duck_pool = duckdb_pool.clone();
            tokio::spawn(async move {
                match backfill_from_postgres(&pg_pool, &duck_pool, 1000).await {
                    Ok(synced) if synced > 0 => {
                        info!(synced, "DuckDB backfill complete");
                    }
                    Ok(_) => {}
                    Err(e) => {
                        tracing::error!(error = %e, "DuckDB backfill failed");
                    }
                }
            });

            // Create replicator for syncing Postgres -> DuckDB
            let (replicator, handle) = Replicator::new(duckdb_pool.clone(), 1000);

            // Spawn replicator task
            tokio::spawn(replicator.run());

            duckdb_pools_map.insert(chain.chain_id, duckdb_pool);
            Some(handle)
        } else {
            None
        };

        if default_chain_id == 0 {
            default_chain_id = chain.chain_id;
        }
        pools_map.insert(chain.chain_id, pool.clone());
        chain_pools.push((chain.clone(), pool, replicator_handle));
    }

    // Start HTTP API with all chain pools
    if config.http.enabled {
        if !pools_map.is_empty() {
            let addr: SocketAddr = format!("{}:{}", config.http.bind, config.http.port).parse()?;
            let router = api::router_with_options(
                pools_map,
                default_chain_id,
                broadcaster.clone(),
                duckdb_pools_map,
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
    }

    // Spawn a sync engine for each chain
    let mut handles = Vec::new();

    for (chain, pool, replicator_handle) in chain_pools {
        let broadcaster = broadcaster.clone();
        let shutdown_rx = shutdown_tx.subscribe();
        let backfill_shutdown_rx = shutdown_tx.subscribe();

        info!(
            chain = %chain.name,
            chain_id = chain.chain_id,
            rpc = %chain.rpc_url,
            backfill = chain.backfill,
            duckdb = chain.duckdb_path.is_some(),
            "Starting sync for chain"
        );

        let handle = tokio::spawn(async move {
            let engine = SyncEngine::new(pool.clone(), &chain.rpc_url).await?;

            // Optionally attach replicator for DuckDB sync
            let engine = if let Some(ref handle) = replicator_handle {
                engine.with_replicator(handle.clone())
            } else {
                engine
            };

            // Start backfill if enabled and not complete
            if chain.backfill {
                let state = engine.status().await?;
                if !state.backfill_complete() {
                    info!(chain = %chain.name, "Starting background backfill");
                    let backfill_engine = SyncEngine::new(pool.clone(), &chain.rpc_url).await?;

                    let batch_size = chain.batch_size;
                    tokio::spawn(async move {
                        // Get chain head from RPC (don't rely on state which may not be set yet)
                        let head = match backfill_engine.get_head().await {
                            Ok(h) => h,
                            Err(e) => {
                                tracing::error!(error = %e, "Failed to get chain head for backfill");
                                return;
                            }
                        };

                        let state = backfill_engine.status().await.unwrap_or_default();
                        // Resume from backfill position, or start from head if fresh
                        let start = state.backfill_num.unwrap_or(head.saturating_sub(1));

                        if start == 0 {
                            info!("Backfill already complete");
                            return;
                        }

                        info!(start = start, target = 0, "Backfill starting from head");

                        match backfill_engine
                            .backfill(start, 0, batch_size, backfill_shutdown_rx)
                            .await
                        {
                            Ok(synced) if synced > 0 => {
                                info!(blocks = synced, "Backfill complete");
                            }
                            Ok(_) => {}
                            Err(e) => {
                                tracing::error!(error = %e, "Backfill failed");
                            }
                        }
                    });
                }
            }

            // Run forward sync with broadcaster for live updates
            let mut engine = SyncEngine::new(pool, &chain.rpc_url)
                .await?
                .with_broadcaster(broadcaster);

            if let Some(handle) = replicator_handle {
                engine = engine.with_replicator(handle);
            }

            engine.run(shutdown_rx).await
        });

        handles.push(handle);
    }

    // Wait for first engine to complete (usually due to shutdown)
    for handle in handles {
        if let Err(e) = handle.await? {
            tracing::error!(error = %e, "Sync engine failed");
        }
    }

    Ok(())
}
