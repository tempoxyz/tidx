use futures::FutureExt;
use std::panic::AssertUnwindSafe;
use tidx::db::{create_pool, run_migrations, run_post_startup_migrations};
use tokio_postgres::NoTls;
use url::Url;

#[tokio::test]
async fn test_pg_upgrade_adds_missing_postgres_ddl() {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("DATABASE_URL not set, skipping migration upgrade test");
        return;
    };

    let temp_db = TempDb::create(&url)
        .await
        .expect("Failed to create temporary database");

    let pool = create_pool(&temp_db.database_url)
        .await
        .expect("Failed to create pool");

    let result = AssertUnwindSafe(async {
        let conn = pool.get().await.expect("Failed to get connection");

        conn.batch_execute(
            r#"
            DROP TABLE IF EXISTS blocks CASCADE;
            DROP TABLE IF EXISTS logs CASCADE;

            CREATE TABLE blocks (
                num             INT8 NOT NULL,
                hash            BYTEA NOT NULL,
                parent_hash     BYTEA NOT NULL,
                timestamp       TIMESTAMPTZ NOT NULL,
                timestamp_ms    INT8 NOT NULL,
                gas_limit       INT8 NOT NULL,
                gas_used        INT8 NOT NULL,
                miner           BYTEA NOT NULL,
                extra_data      BYTEA,
                PRIMARY KEY (timestamp, num)
            );

            CREATE TABLE logs (
                block_num       INT8 NOT NULL,
                block_timestamp TIMESTAMPTZ NOT NULL,
                log_idx         INT4 NOT NULL,
                tx_idx          INT4 NOT NULL,
                tx_hash         BYTEA NOT NULL,
                address         BYTEA NOT NULL,
                selector        BYTEA,
                topic0          BYTEA,
                topic1          BYTEA,
                topic2          BYTEA,
                topic3          BYTEA,
                data            BYTEA NOT NULL,
                PRIMARY KEY (block_timestamp, block_num, log_idx)
            );

            CREATE INDEX IF NOT EXISTS idx_logs_block_num ON logs (block_num DESC);
            CREATE INDEX IF NOT EXISTS idx_logs_tx_hash ON logs (tx_hash);
            CREATE INDEX IF NOT EXISTS idx_logs_selector ON logs (selector, block_timestamp DESC);
            CREATE INDEX IF NOT EXISTS idx_logs_address ON logs (address, block_timestamp DESC);
            CREATE INDEX IF NOT EXISTS idx_logs_address_topic1 ON logs (topic1, address, block_num DESC);
            CREATE INDEX IF NOT EXISTS idx_logs_topic2 ON logs (topic2);
            CREATE INDEX IF NOT EXISTS idx_logs_topic3 ON logs (topic3);
            "#,
        )
        .await
        .expect("Failed to create old schema");

        run_migrations(&pool)
            .await
            .expect("Failed to run migrations against old schema");
        run_migrations(&pool)
            .await
            .expect("Failed to rerun migrations against upgraded schema");
        run_post_startup_migrations(&pool)
            .await
            .expect("Failed to run post-startup migrations against old schema");
        run_post_startup_migrations(&pool)
            .await
            .expect("Failed to rerun post-startup migrations against upgraded schema");

        let conn = pool.get().await.expect("Failed to get post-migration connection");

        let col_exists: bool = conn
            .query_one(
                r#"
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = 'logs'
                      AND column_name = 'is_virtual_forward'
                )
                "#,
                &[],
            )
            .await
            .expect("Failed to query columns")
            .get(0);
        assert!(
            col_exists,
            "logs.is_virtual_forward column should exist after migration"
        );

        let consensus_proposer_exists: bool = conn
            .query_one(
                r#"
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = 'blocks'
                      AND column_name = 'consensus_proposer'
                )
                "#,
                &[],
            )
            .await
            .expect("Failed to query blocks columns")
            .get(0);
        assert!(
            consensus_proposer_exists,
            "blocks.consensus_proposer column should exist after migration"
        );

        let consensus_proposer_constraint_exists: bool = conn
            .query_one(
                r#"
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_constraint
                    WHERE conrelid = 'blocks'::regclass
                      AND contype = 'c'
                      AND pg_get_constraintdef(oid) LIKE '%consensus_proposer%'
                )
                "#,
                &[],
            )
            .await
            .expect("Failed to query blocks constraints")
            .get(0);
        assert!(
            consensus_proposer_constraint_exists,
            "blocks.consensus_proposer length constraint should exist after migration"
        );

        let indexes: Vec<String> = conn
            .query(
                r#"
                SELECT indexname
                FROM pg_indexes
                WHERE schemaname = 'public'
                  AND tablename = 'logs'
                  AND indexname IN (
                    'idx_logs_virtual_forward',
                    'idx_logs_tx_hash_virtual_forward'
                  )
                ORDER BY indexname
                "#,
                &[],
            )
            .await
            .expect("Failed to query indexes")
            .into_iter()
            .map(|r| r.get(0))
            .collect();

        assert_eq!(
            indexes,
            vec![
                "idx_logs_tx_hash_virtual_forward".to_string(),
                "idx_logs_virtual_forward".to_string(),
            ]
        );
    })
    .catch_unwind()
    .await;

    drop(pool);
    temp_db
        .cleanup()
        .await
        .expect("Failed to clean up temporary database");

    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}

struct TempDb {
    admin_url: String,
    database_name: String,
    database_url: String,
}

impl TempDb {
    async fn create(base_url: &str) -> anyhow::Result<Self> {
        let mut db_url = Url::parse(base_url)?;
        let database_name = format!("tidx_migration_test_{}", std::process::id());

        let mut admin_url = db_url.clone();
        admin_url.set_path("/postgres");
        let admin_url = admin_url.to_string();

        db_url.set_path(&format!("/{database_name}"));
        let database_url = db_url.to_string();

        let (admin_client, connection) = tokio_postgres::connect(&admin_url, NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });

        admin_client
            .execute(
                &format!("DROP DATABASE IF EXISTS \"{database_name}\";"),
                &[],
            )
            .await?;
        admin_client
            .execute(&format!("CREATE DATABASE \"{database_name}\";"), &[])
            .await?;

        Ok(Self {
            admin_url,
            database_name,
            database_url,
        })
    }

    async fn cleanup(self) -> anyhow::Result<()> {
        let (admin_client, connection) = tokio_postgres::connect(&self.admin_url, NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });

        admin_client
            .execute(
                "SELECT pg_terminate_backend(pid)
                 FROM pg_stat_activity
                 WHERE datname = $1 AND pid != pg_backend_pid()",
                &[&self.database_name],
            )
            .await?;

        admin_client
            .execute(
                &format!("DROP DATABASE IF EXISTS \"{}\";", self.database_name),
                &[],
            )
            .await?;

        Ok(())
    }
}
