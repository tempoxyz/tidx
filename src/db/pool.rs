use anyhow::{Context, Result};
use deadpool_postgres::{Config, Pool, Runtime};
use tokio_postgres::NoTls;
use url::Url;

/// Default pool for general use (16 connections)
pub async fn create_pool(database_url: &str) -> Result<Pool> {
    create_pool_with_size(database_url, 16).await
}

/// Creates a pool with custom max connections
pub async fn create_pool_with_size(database_url: &str, max_size: usize) -> Result<Pool> {
    ensure_database_exists(database_url).await?;

    // Kill idle-in-transaction connections after 60s to prevent lock contention on restart
    let url_with_timeout = if database_url.contains('?') {
        format!("{}&options=-c%20idle_in_transaction_session_timeout%3D60000", database_url)
    } else {
        format!("{}?options=-c%20idle_in_transaction_session_timeout%3D60000", database_url)
    };

    let mut config = Config::new();
    config.url = Some(url_with_timeout);
    config.pool = Some(deadpool_postgres::PoolConfig {
        max_size,
        ..Default::default()
    });

    let pool = config.create_pool(Some(Runtime::Tokio1), NoTls)?;
    let _ = pool.get().await?;

    Ok(pool)
}

use std::sync::Arc;
use tokio::sync::Semaphore;

/// Shared pool with backfill throttling.
/// 
/// Single pool shared by all (realtime, backfill, API), but backfill
/// must acquire a semaphore permit before getting a connection.
/// This ensures backfill can't starve realtime/API, and when backfill
/// completes, all connections are available for other workloads.
#[derive(Clone)]
pub struct ThrottledPool {
    /// Shared connection pool (16 connections)
    pub pool: Pool,
    /// Semaphore limiting concurrent backfill operations (default: 6)
    /// Leaves headroom for realtime (2) and API (8)
    pub backfill_semaphore: Arc<Semaphore>,
}

impl ThrottledPool {
    pub async fn new(database_url: &str) -> Result<Self> {
        // Keep headroom: pool 20, backfill 10 (leaves 10 for realtime/API)
        Self::with_limits(database_url, 20, 10).await
    }

    /// Create with custom pool size and backfill limit.
    /// - pool_size: total connections in the pool
    /// - backfill_limit: max concurrent backfill operations
    pub async fn with_limits(database_url: &str, pool_size: usize, backfill_limit: usize) -> Result<Self> {
        ensure_database_exists(database_url).await?;
        let pool = create_pool_with_size(database_url, pool_size).await?;
        
        Ok(Self {
            pool,
            backfill_semaphore: Arc::new(Semaphore::new(backfill_limit)),
        })
    }

    /// Wrap an existing pool (useful for tests).
    pub fn from_pool(pool: Pool) -> Self {
        Self {
            pool,
            backfill_semaphore: Arc::new(Semaphore::new(10)),
        }
    }

    /// Get a connection for realtime or API (no throttling).
    pub async fn get(&self) -> Result<deadpool_postgres::Object> {
        Ok(self.pool.get().await?)
    }

    /// Get a connection for backfill (throttled by semaphore).
    /// Blocks if backfill_limit concurrent operations are already running.
    pub async fn get_backfill(&self) -> Result<BackfillConnection> {
        let permit = self.backfill_semaphore.clone().acquire_owned().await
            .map_err(|_| anyhow::anyhow!("Backfill semaphore closed"))?;
        let conn = self.pool.get().await?;
        Ok(BackfillConnection { conn, _permit: permit })
    }

    /// Returns the underlying pool (for compatibility).
    pub fn inner(&self) -> &Pool {
        &self.pool
    }
}

/// Connection wrapper that holds a semaphore permit.
/// Permit is released when this is dropped.
pub struct BackfillConnection {
    conn: deadpool_postgres::Object,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl std::ops::Deref for BackfillConnection {
    type Target = deadpool_postgres::Object;
    fn deref(&self) -> &Self::Target {
        &self.conn
    }
}

impl std::ops::DerefMut for BackfillConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.conn
    }
}

/// Ensure the database exists, creating it if necessary.
/// Connects to the 'postgres' database to run CREATE DATABASE.
async fn ensure_database_exists(database_url: &str) -> Result<()> {
    let mut url = Url::parse(database_url).context("Invalid database URL")?;
    
    // Extract the target database name from the path (e.g., "/tidx_moderato" -> "tidx_moderato")
    let db_name = url.path().trim_start_matches('/').to_string();
    
    if db_name.is_empty() || db_name == "postgres" {
        return Ok(());
    }

    // Connect to 'postgres' database to create the target database
    url.set_path("/postgres");
    let postgres_url = url.as_str();

    let (client, connection) = match tokio_postgres::connect(postgres_url, NoTls).await {
        Ok(c) => c,
        Err(_) => return Ok(()), // Can't connect to postgres db, let the main connection fail with a better error
    };

    // Spawn connection handler
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::debug!("postgres connection error: {}", e);
        }
    });

    // Check if database exists
    let exists: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)",
            &[&db_name],
        )
        .await?
        .get(0);

    if !exists {
        // Use format! because CREATE DATABASE doesn't support parameterized queries
        let create_sql = format!("CREATE DATABASE \"{}\"", db_name.replace('"', "\"\""));
        client.execute(&create_sql, &[]).await?;
        tracing::info!(database = %db_name, "Created database");
    }

    Ok(())
}
