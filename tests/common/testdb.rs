use tidx::db::{Pool, ThrottledPool, create_pool, run_migrations};
use tidx::sync::engine::SyncEngine;
use tidx::sync::sink::SinkSet;
use tokio::sync::{Mutex, MutexGuard, OnceCell};

use super::tempo::TempoNode;

static MIGRATIONS_DONE: OnceCell<()> = OnceCell::const_new();
static TEST_LOCK: OnceCell<Mutex<()>> = OnceCell::const_new();

/// Test database wrapper.
/// Uses a global lock to prevent concurrent test execution (required for shared DB).
/// Tests that need isolation should use `truncate_all()` or unique data ranges.
pub struct TestDb {
    pub pool: Pool,
    _guard: MutexGuard<'static, ()>,
}

impl TestDb {
    /// Create a TestDb without seeding (for tests that need empty/controlled DB)
    pub async fn empty() -> Self {
        Self::init().await
    }

    /// Create a TestDb with auto-seeded data
    pub async fn new() -> Self {
        let db = Self::init().await;
        db.ensure_seeded().await;
        db
    }

    async fn init() -> Self {
        // Global lock ensures tests run sequentially (required for shared DB state)
        let lock = TEST_LOCK.get_or_init(|| async { Mutex::new(()) }).await;
        let guard = lock.lock().await;

        let url = get_test_db_url();
        let pool = create_pool(&url).await.expect("Failed to create pool");

        MIGRATIONS_DONE
            .get_or_init(|| async {
                run_migrations(&pool)
                    .await
                    .expect("Failed to run migrations");
            })
            .await;

        Self {
            pool,
            _guard: guard,
        }
    }

    async fn ensure_seeded(&self) {
        // Check if DB already has sufficient data
        if self.block_count().await >= 10 {
            return;
        }

        println!("Seeding test database...");

        let tempo = TempoNode::from_env();
        tempo.wait_for_ready().await.unwrap_or_else(|e| {
            panic!(
                "Tempo node is required for seeded tests but was not reachable at {}: {e}. \
Start the docker services with `docker compose -f docker/local/docker-compose.yml up -d postgres tempo` \
and ensure DATABASE_URL points at the Postgres container (for example \
`postgresql://tidx:tidx@localhost:5433/tidx`).",
                tempo.rpc_url
            )
        });

        // Wait for some blocks
        tempo.wait_for_block(50).await.ok();

        let sinks = SinkSet::new(self.pool.clone());
        let engine = SyncEngine::new(
            ThrottledPool::from_pool(self.pool.clone()),
            sinks,
            &tempo.rpc_url,
        )
        .await
        .expect("Failed to create sync engine");

        let head = tempo.block_number().await.unwrap_or(50).min(100);
        for block_num in 1..=head {
            if let Err(e) = engine.sync_block(block_num).await {
                println!("Warning: Failed to sync block {block_num}: {e}");
            }
        }

        println!(
            "Seeding complete: {} blocks, {} txs, {} logs",
            self.block_count().await,
            self.tx_count().await,
            self.log_count().await
        );
    }

    pub async fn truncate_all(&self) {
        let conn = self.pool.get().await.expect("Failed to get connection");
        conn.batch_execute("TRUNCATE blocks, txs, logs, receipts, sync_state CASCADE")
            .await
            .expect("Failed to truncate tables");
    }

    pub async fn block_count(&self) -> i64 {
        let conn = self.pool.get().await.expect("Failed to get connection");
        conn.query_one("SELECT COUNT(*) FROM blocks", &[])
            .await
            .expect("Failed to count blocks")
            .get(0)
    }

    pub async fn tx_count(&self) -> i64 {
        let conn = self.pool.get().await.expect("Failed to get connection");
        conn.query_one("SELECT COUNT(*) FROM txs", &[])
            .await
            .expect("Failed to count txs")
            .get(0)
    }

    pub async fn log_count(&self) -> i64 {
        let conn = self.pool.get().await.expect("Failed to get connection");
        conn.query_one("SELECT COUNT(*) FROM logs", &[])
            .await
            .expect("Failed to count logs")
            .get(0)
    }

    pub async fn receipt_count(&self) -> i64 {
        let conn = self.pool.get().await.expect("Failed to get connection");
        conn.query_one("SELECT COUNT(*) FROM receipts", &[])
            .await
            .expect("Failed to count receipts")
            .get(0)
    }
}

fn get_test_db_url() -> String {
    std::env::var("DATABASE_URL").expect("DATABASE_URL must be set")
}
