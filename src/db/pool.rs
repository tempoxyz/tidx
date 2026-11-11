use std::time::Duration;

use anyhow::{Context, Result};
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod, Runtime};
use tokio_postgres::NoTls;
use url::Url;

/// How long opening a connection may take, TCP and startup included.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// How long sent data may stay unacknowledged before the kernel drops the
/// connection. Without it, a write to a server that vanished (node lost,
/// failover) retransmits for about 15 minutes before the query fails.
const TCP_USER_TIMEOUT: Duration = Duration::from_secs(30);
/// Keepalive probes find a dead server on an idle connection: first probe
/// after 30s idle, then every 10s, dropping the connection after 3 misses.
const KEEPALIVE_IDLE: Duration = Duration::from_secs(30);
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);
const KEEPALIVE_RETRIES: u32 = 3;
/// libpq's keepalive idle default, which a URL that sets none leaves in place.
const DEFAULT_KEEPALIVE_IDLE: Duration = Duration::from_secs(2 * 60 * 60);
/// How long the pool may spend recycling (health-checking) one connection.
/// Creation has no overall timeout: `CONNECT_TIMEOUT` already bounds each
/// address tokio-postgres tries, and a URL with several hosts or a name with
/// several addresses needs that budget per address.
const POOL_RECYCLE_TIMEOUT: Duration = Duration::from_secs(10);

/// Default pool for general use (16 connections)
pub async fn create_pool(database_url: &str) -> Result<Pool> {
    create_pool_with_size(database_url, 16).await
}

/// Creates a pool with custom max connections
pub async fn create_pool_with_size(database_url: &str, max_size: usize) -> Result<Pool> {
    ensure_database_exists(database_url).await?;
    connect_pool(database_url, max_size).await
}

/// Creates a pool against an existing database without trying to create it.
///
/// Used by `tidx serve`, which may connect with read-only credentials to a
/// read replica where `CREATE DATABASE` is neither allowed nor wanted.
pub async fn connect_pool(database_url: &str, max_size: usize) -> Result<Pool> {
    connect_pool_with_wait(database_url, max_size, None).await
}

/// Like [`connect_pool`], failing a checkout that waits longer than `wait`
/// for a free connection instead of queueing until the query timeout.
pub async fn connect_pool_with_wait(
    database_url: &str,
    max_size: usize,
    wait: Option<std::time::Duration>,
) -> Result<Pool> {
    // Kill idle or active long-running statements in transactions after 60s to prevent
    // lock contention. API queries still use stricter SET LOCAL timeouts when requested,
    // and Clean recycling clears leaked session state.
    let url_with_timeout = if database_url.contains('?') {
        format!(
            "{}&options=-c%20idle_in_transaction_session_timeout%3D60000%20-c%20statement_timeout%3D60000",
            database_url
        )
    } else {
        format!(
            "{}?options=-c%20idle_in_transaction_session_timeout%3D60000%20-c%20statement_timeout%3D60000",
            database_url
        )
    };

    let mut pg_config: tokio_postgres::Config =
        url_with_timeout.parse().context("Invalid database URL")?;
    apply_dead_server_timeouts(&mut pg_config);

    let manager = Manager::from_config(
        pg_config,
        NoTls,
        ManagerConfig {
            recycling_method: RecyclingMethod::Clean,
        },
    );
    let pool = Pool::builder(manager)
        .max_size(max_size)
        .runtime(Runtime::Tokio1)
        .wait_timeout(wait)
        .recycle_timeout(Some(POOL_RECYCLE_TIMEOUT))
        .build()?;
    let _ = pool.get().await?;

    Ok(pool)
}

/// Makes connections to a server that stopped answering fail in seconds
/// instead of hanging, so callers see an error and retry on a fresh
/// connection. `statement_timeout` cannot help there: the server enforces it.
/// Options the database URL sets explicitly are kept, except an explicit
/// `keepalives_idle=7200`: tokio-postgres reports it exactly like the unset
/// default, so it becomes 30s too.
fn apply_dead_server_timeouts(config: &mut tokio_postgres::Config) {
    if config.get_connect_timeout().is_none() {
        config.connect_timeout(CONNECT_TIMEOUT);
    }
    if config.get_tcp_user_timeout().is_none() {
        config.tcp_user_timeout(TCP_USER_TIMEOUT);
    }
    if config.get_keepalives() {
        if config.get_keepalives_idle() == DEFAULT_KEEPALIVE_IDLE {
            config.keepalives_idle(KEEPALIVE_IDLE);
        }
        if config.get_keepalives_interval().is_none() {
            config.keepalives_interval(KEEPALIVE_INTERVAL);
        }
        if config.get_keepalives_retries().is_none() {
            config.keepalives_retries(KEEPALIVE_RETRIES);
        }
    }
}

use std::sync::Arc;
use tokio::sync::Semaphore;

/// Shared pool with backfill throttling.
///
/// Single pool shared by all (realtime, backfill, API), but backfill
/// must acquire a semaphore permit before getting a connection.
/// This ensures backfill can't starve realtime/API, and when backfill
/// completes, all connections are available for other workloads.
///
/// ## Connection budget (with parallel writes)
///
/// Each write operation uses up to 4 PG connections concurrently (one per table
/// via `tokio::try_join!` in `SinkSet` + `sync_range_standalone`). The budget:
///
/// | Worker               | Max connections         |
/// |----------------------|-------------------------|
/// | Realtime (1)         | 4 tables = 4            |
/// | Gap-fill (4 workers) | 4 × 4 tables = 16      |
/// | Receipt backfill (1) | 2                       |
/// | API queries          | ~8                      |
/// | Sync state / gaps    | 2                       |
/// | **Total peak**       | **~32**                 |
///
/// Backfill semaphore (6 permits) caps gap-fill to 6 × 4 = 24 connections,
/// keeping total peak within pool_size=48.
#[derive(Clone)]
pub struct ThrottledPool {
    pub pool: Pool,
    /// Semaphore limiting concurrent backfill operations.
    /// Each operation may use up to 4 connections (parallel table writes),
    /// so effective max backfill connections = backfill_limit × 4.
    pub backfill_semaphore: Arc<Semaphore>,
}

impl ThrottledPool {
    pub async fn new(database_url: &str) -> Result<Self> {
        // Pool 48: backfill 6×4=24 + realtime 4 + receipts 2 + API 8 + overhead 10
        // Backfill 6: 6 concurrent operations × 4 connections each = 24 max
        Self::with_limits(database_url, 48, 6).await
    }

    /// Create with custom pool size and backfill limit.
    /// - pool_size: total connections in the pool
    /// - backfill_limit: max concurrent backfill operations
    pub async fn with_limits(
        database_url: &str,
        pool_size: usize,
        backfill_limit: usize,
    ) -> Result<Self> {
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
        let permit = self
            .backfill_semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| anyhow::anyhow!("Backfill semaphore closed"))?;
        let conn = self.pool.get().await?;
        Ok(BackfillConnection {
            conn,
            _permit: permit,
        })
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

    let mut admin_config: tokio_postgres::Config = match postgres_url.parse() {
        Ok(config) => config,
        Err(_) => return Ok(()), // Let the main connection fail with a better error
    };
    apply_dead_server_timeouts(&mut admin_config);
    let (client, connection) = match admin_config.connect(NoTls).await {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dead_server_timeouts_fill_unset_options() {
        let mut config: tokio_postgres::Config = "postgres://tidx@localhost/tidx".parse().unwrap();
        apply_dead_server_timeouts(&mut config);
        assert_eq!(config.get_connect_timeout(), Some(&CONNECT_TIMEOUT));
        assert_eq!(config.get_tcp_user_timeout(), Some(&TCP_USER_TIMEOUT));
        assert!(config.get_keepalives());
        assert_eq!(config.get_keepalives_idle(), KEEPALIVE_IDLE);
        assert_eq!(config.get_keepalives_interval(), Some(KEEPALIVE_INTERVAL));
        assert_eq!(config.get_keepalives_retries(), Some(KEEPALIVE_RETRIES));
    }

    #[test]
    fn test_dead_server_timeouts_keep_url_options() {
        let mut config: tokio_postgres::Config = "postgres://tidx@localhost/tidx?connect_timeout=3&tcp_user_timeout=7&keepalives_idle=60"
            .parse()
            .unwrap();
        apply_dead_server_timeouts(&mut config);
        assert_eq!(config.get_connect_timeout(), Some(&Duration::from_secs(3)));
        assert_eq!(config.get_tcp_user_timeout(), Some(&Duration::from_secs(7)));
        assert_eq!(config.get_keepalives_idle(), Duration::from_secs(60));
    }

    #[test]
    fn test_dead_server_timeouts_respect_disabled_keepalives() {
        let mut config: tokio_postgres::Config = "postgres://tidx@localhost/tidx?keepalives=0"
            .parse()
            .unwrap();
        apply_dead_server_timeouts(&mut config);
        assert!(!config.get_keepalives());
        assert_eq!(config.get_keepalives_interval(), None);
    }
}
