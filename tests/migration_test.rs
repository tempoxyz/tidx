use futures::FutureExt;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU64, Ordering};
use tidx::db::{create_pool_with_size, run_migrations, run_post_startup_migrations};
use tokio_postgres::NoTls;
use url::Url;

const MANAGED_INDEX_NAMES: &[&str] = &[
    "idx_logs_address_topic0_block",
    "idx_logs_selector_indexed_address",
    "idx_logs_selector_topic1_block",
    "idx_logs_selector_topic2_block",
    "idx_logs_selector_topic3_block",
    "idx_logs_tx_hash_virtual_forward",
    "idx_logs_virtual_forward",
    "idx_receipts_fee_payer_block",
    "idx_receipts_from_block",
    "idx_txs_fee_payer_block",
    "idx_txs_from_block",
];

static TEMP_DB_COUNTER: AtomicU64 = AtomicU64::new(0);

#[tokio::test]
async fn test_pg_upgrade_adds_missing_postgres_ddl() {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("DATABASE_URL not set, skipping migration upgrade test");
        return;
    };

    let temp_db = TempDb::create(&url)
        .await
        .expect("Failed to create temporary database");

    let pool = create_pool_with_size(&temp_db.database_url, 1)
        .await
        .expect("Failed to create pool");

    let result = AssertUnwindSafe(async {
        let conn = pool.get().await.expect("Failed to get connection");

        conn.batch_execute(
            r#"
            DROP TABLE IF EXISTS blocks CASCADE;
            DROP TABLE IF EXISTS logs CASCADE;
            DROP TABLE IF EXISTS txs CASCADE;
            DROP TABLE IF EXISTS receipts CASCADE;

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

            CREATE TABLE txs (
                block_num               INT8 NOT NULL,
                block_timestamp         TIMESTAMPTZ NOT NULL,
                idx                     INT4 NOT NULL,
                hash                    BYTEA NOT NULL,
                type                    INT2 NOT NULL,
                "from"                  BYTEA NOT NULL,
                "to"                    BYTEA,
                value                   TEXT NOT NULL,
                input                   BYTEA NOT NULL,
                gas_limit               INT8 NOT NULL,
                max_fee_per_gas         TEXT NOT NULL,
                max_priority_fee_per_gas TEXT NOT NULL,
                gas_used                INT8,
                nonce_key               BYTEA NOT NULL,
                nonce                   INT8 NOT NULL,
                fee_token               BYTEA,
                fee_payer               BYTEA,
                calls                   JSONB,
                call_count              INT2 NOT NULL DEFAULT 1,
                valid_before            INT8,
                valid_after             INT8,
                signature_type          INT2,
                PRIMARY KEY (block_timestamp, block_num, idx)
            );

            CREATE TABLE receipts (
                block_num               INT8 NOT NULL,
                block_timestamp         TIMESTAMPTZ NOT NULL,
                tx_idx                  INT4 NOT NULL,
                tx_hash                 BYTEA NOT NULL,
                "from"                  BYTEA NOT NULL,
                "to"                    BYTEA,
                contract_address        BYTEA,
                gas_used                INT8 NOT NULL,
                cumulative_gas_used     INT8 NOT NULL,
                effective_gas_price     TEXT,
                status                  INT2,
                fee_payer               BYTEA,
                PRIMARY KEY (block_timestamp, block_num, tx_idx)
            );

            CREATE INDEX IF NOT EXISTS idx_logs_block_num ON logs (block_num DESC);
            CREATE INDEX IF NOT EXISTS idx_logs_tx_hash ON logs (tx_hash);
            CREATE INDEX IF NOT EXISTS idx_logs_selector ON logs (selector, block_timestamp DESC);
            CREATE INDEX IF NOT EXISTS idx_logs_address ON logs (address, block_timestamp DESC);
            CREATE INDEX IF NOT EXISTS idx_logs_address_topic1 ON logs (topic1, address, block_num DESC);
            CREATE INDEX IF NOT EXISTS idx_logs_topic2 ON logs (topic2);
            CREATE INDEX IF NOT EXISTS idx_logs_topic3 ON logs (topic3);
            CREATE INDEX IF NOT EXISTS idx_txs_from ON txs ("from", block_timestamp DESC);
            CREATE INDEX IF NOT EXISTS idx_receipts_fee_payer ON receipts (fee_payer, block_timestamp DESC) WHERE fee_payer IS NOT NULL;
            "#,
        )
        .await
        .expect("Failed to create old schema");
        drop(conn);

        run_migrations(&pool)
            .await
            .expect("Failed to run migrations against old schema");
        run_migrations(&pool)
            .await
            .expect("Failed to rerun migrations against upgraded schema");

        let conn = pool.get().await.expect("Failed to get connection");
        conn.batch_execute(
            r#"
            INSERT INTO logs (
                block_num, block_timestamp, log_idx, tx_idx, tx_hash,
                address, selector, data
            ) VALUES
                (1, NOW(), 0, 0, '\x01', '\x01', '\xdeadbeef', '\x'),
                (2, NOW(), 0, 0, '\x02', '\x02', '\xdeadbeef', '\x')
            "#,
        )
        .await
        .expect("Failed to seed duplicate values");

        // A failed concurrent build leaves its relation behind. IF NOT EXISTS
        // alone would skip it forever even though PostgreSQL cannot use it.
        let failed_build = conn
            .batch_execute(
                "CREATE UNIQUE INDEX CONCURRENTLY idx_logs_address_topic0_block \
                 ON logs (selector)",
            )
            .await;
        assert!(
            failed_build.is_err(),
            "duplicate selector values should fail the unique index build"
        );
        let invalid_before: bool = conn
            .query_one(
                r#"
                SELECT NOT index_state.indisvalid OR NOT index_state.indisready
                FROM pg_class relation
                JOIN pg_index index_state ON index_state.indexrelid = relation.oid
                WHERE relation.relname = 'idx_logs_address_topic0_block'
                "#,
                &[],
            )
            .await
            .expect("Failed to inspect failed concurrent index")
            .get(0);
        assert!(invalid_before, "failed concurrent index should be invalid");
        drop(conn);

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
                    'idx_logs_address_topic0_block',
                    'idx_logs_selector_indexed_address',
                    'idx_logs_selector_topic1_block',
                    'idx_logs_selector_topic2_block',
                    'idx_logs_selector_topic3_block',
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
                "idx_logs_address_topic0_block".to_string(),
                "idx_logs_selector_indexed_address".to_string(),
                "idx_logs_selector_topic1_block".to_string(),
                "idx_logs_selector_topic2_block".to_string(),
                "idx_logs_selector_topic3_block".to_string(),
                "idx_logs_tx_hash_virtual_forward".to_string(),
                "idx_logs_virtual_forward".to_string(),
            ]
        );

        let indexes: Vec<String> = conn
            .query(
                r#"
                SELECT indexname
                FROM pg_indexes
                WHERE schemaname = 'public'
                  AND tablename IN ('txs', 'receipts')
                  AND indexname IN (
                    'idx_txs_from_block',
                    'idx_txs_fee_payer_block',
                    'idx_receipts_fee_payer_block',
                    'idx_receipts_from_block'
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
                "idx_receipts_fee_payer_block".to_string(),
                "idx_receipts_from_block".to_string(),
                "idx_txs_fee_payer_block".to_string(),
                "idx_txs_from_block".to_string(),
            ]
        );

        assert_managed_indexes_valid(&conn).await;
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

#[tokio::test]
async fn test_partitioned_post_startup_indexes_attach_valid_children() {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("DATABASE_URL not set, skipping partitioned migration test");
        return;
    };

    let temp_db = TempDb::create(&url)
        .await
        .expect("Failed to create temporary database");
    let pool = create_pool_with_size(&temp_db.database_url, 1)
        .await
        .expect("Failed to create pool");

    let result = AssertUnwindSafe(async {
        run_migrations(&pool)
            .await
            .expect("Failed to create partitioned schema");
        run_post_startup_migrations(&pool)
            .await
            .expect("Failed to build partitioned indexes");
        run_post_startup_migrations(&pool)
            .await
            .expect("Failed to rerun partitioned index migrations");

        let conn = pool.get().await.expect("Failed to get connection");
        let statement_timeout: String = conn
            .query_one("SHOW statement_timeout", &[])
            .await
            .expect("Failed to inspect statement timeout")
            .get(0);
        assert_eq!(
            statement_timeout, "1min",
            "post-startup migrations should restore the pool timeout"
        );
        assert_managed_indexes_valid(&conn).await;

        let parent_indexes: Vec<String> = conn
            .query(
                r#"
                SELECT relation.relname
                FROM pg_class relation
                JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
                WHERE namespace.nspname = 'public'
                  AND relation.relname IN (
                    'idx_logs_address_topic0_block',
                    'idx_logs_selector_indexed_address',
                    'idx_logs_selector_topic1_block',
                    'idx_logs_selector_topic2_block',
                    'idx_logs_selector_topic3_block',
                    'idx_logs_tx_hash_virtual_forward',
                    'idx_logs_virtual_forward',
                    'idx_receipts_fee_payer_block',
                    'idx_receipts_from_block',
                    'idx_txs_fee_payer_block',
                    'idx_txs_from_block'
                  )
                  AND relation.relkind = 'I'
                ORDER BY relation.relname
                "#,
                &[],
            )
            .await
            .expect("Failed to inspect partitioned parent indexes")
            .into_iter()
            .map(|row| row.get(0))
            .collect();
        assert_eq!(
            parent_indexes.len(),
            MANAGED_INDEX_NAMES.len(),
            "every managed index should be a partitioned parent index"
        );

        let missing_children: Vec<String> = conn
            .query(
                r#"
                WITH managed(name) AS (
                    VALUES
                        ('idx_logs_address_topic0_block'),
                        ('idx_logs_selector_indexed_address'),
                        ('idx_logs_selector_topic1_block'),
                        ('idx_logs_selector_topic2_block'),
                        ('idx_logs_selector_topic3_block'),
                        ('idx_logs_tx_hash_virtual_forward'),
                        ('idx_logs_virtual_forward'),
                        ('idx_receipts_fee_payer_block'),
                        ('idx_receipts_from_block'),
                        ('idx_txs_fee_payer_block'),
                        ('idx_txs_from_block')
                )
                SELECT parent_index.relname || ':' || table_partition.relname
                FROM managed
                JOIN pg_class parent_index ON parent_index.relname = managed.name
                JOIN pg_namespace parent_namespace
                  ON parent_namespace.oid = parent_index.relnamespace
                 AND parent_namespace.nspname = 'public'
                JOIN pg_index parent_index_state
                  ON parent_index_state.indexrelid = parent_index.oid
                JOIN pg_inherits table_inheritance
                  ON table_inheritance.inhparent = parent_index_state.indrelid
                JOIN pg_class table_partition
                  ON table_partition.oid = table_inheritance.inhrelid
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM pg_inherits index_inheritance
                    JOIN pg_index child_index_state
                      ON child_index_state.indexrelid = index_inheritance.inhrelid
                    WHERE index_inheritance.inhparent = parent_index.oid
                      AND child_index_state.indrelid = table_partition.oid
                      AND child_index_state.indisvalid
                      AND child_index_state.indisready
                )
                ORDER BY parent_index.relname, table_partition.relname
                "#,
                &[],
            )
            .await
            .expect("Failed to inspect attached child indexes")
            .into_iter()
            .map(|row| row.get(0))
            .collect();
        assert!(
            missing_children.is_empty(),
            "managed indexes lack valid attached children: {missing_children:?}"
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

async fn assert_managed_indexes_valid(conn: &tokio_postgres::Client) {
    let invalid: Vec<String> = conn
        .query(
            r#"
            SELECT relation.relname
            FROM pg_class relation
            JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
            JOIN pg_index index_state ON index_state.indexrelid = relation.oid
            WHERE namespace.nspname = 'public'
              AND relation.relname IN (
                'idx_logs_address_topic0_block',
                'idx_logs_selector_indexed_address',
                'idx_logs_selector_topic1_block',
                'idx_logs_selector_topic2_block',
                'idx_logs_selector_topic3_block',
                'idx_logs_tx_hash_virtual_forward',
                'idx_logs_virtual_forward',
                'idx_receipts_fee_payer_block',
                'idx_receipts_from_block',
                'idx_txs_fee_payer_block',
                'idx_txs_from_block'
              )
              AND (NOT index_state.indisvalid OR NOT index_state.indisready)
            ORDER BY relation.relname
            "#,
            &[],
        )
        .await
        .expect("Failed to inspect index validity")
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert!(
        invalid.is_empty(),
        "managed indexes should all be valid and ready: {invalid:?}"
    );
}

struct TempDb {
    admin_url: String,
    database_name: String,
    database_url: String,
}

impl TempDb {
    async fn create(base_url: &str) -> anyhow::Result<Self> {
        let mut db_url = Url::parse(base_url)?;
        let sequence = TEMP_DB_COUNTER.fetch_add(1, Ordering::Relaxed);
        let database_name = format!("tidx_migration_test_{}_{}", std::process::id(), sequence);

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
