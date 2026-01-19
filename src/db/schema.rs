use anyhow::Result;
use tracing::info;

use super::Pool;

pub async fn run_migrations(pool: &Pool) -> Result<()> {
    let conn = pool.get().await?;

    info!("Running TimescaleDB hypertable migrations");
    conn.batch_execute(include_str!(
        "../../migrations/006_timescale_hypertables.sql"
    ))
    .await?;

    info!("Running ABI function migrations");
    conn.batch_execute(include_str!(
        "../../migrations/007_abi_functions.sql"
    ))
    .await?;

    info!("Running backfill tracking migration");
    conn.batch_execute(include_str!(
        "../../migrations/008_backfill_tracking.sql"
    ))
    .await?;

    info!("Running receipts migration");
    conn.batch_execute(include_str!(
        "../../migrations/009_receipts.sql"
    ))
    .await?;

    Ok(())
}


