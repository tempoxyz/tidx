use anyhow::{Context, Result};
use deadpool_postgres::{Config, Pool, Runtime};
use tokio_postgres::NoTls;
use url::Url;

pub async fn create_pool(database_url: &str) -> Result<Pool> {
    create_pool_with_size(database_url, 16).await
}

pub async fn create_pool_with_size(database_url: &str, max_size: usize) -> Result<Pool> {
    // Try to create the database if it doesn't exist
    ensure_database_exists(database_url).await?;

    let mut config = Config::new();
    config.url = Some(database_url.to_string());
    config.pool = Some(deadpool_postgres::PoolConfig {
        max_size,
        ..Default::default()
    });

    let pool = config.create_pool(Some(Runtime::Tokio1), NoTls)?;

    let _ = pool.get().await?;

    Ok(pool)
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
