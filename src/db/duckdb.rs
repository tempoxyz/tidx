#![allow(unsafe_code)]

use anyhow::{Context, Result};
use duckdb::core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId};
use duckdb::ffi::duckdb_string_t;
use duckdb::types::DuckString;
use duckdb::vscalar::{ScalarFunctionSignature, VScalar};
use duckdb::vtab::arrow::WritableVector;
use duckdb::Connection;
use std::sync::RwLock;
use std::thread;

/// Cached sync status for fast status endpoint queries.
#[derive(Debug, Clone, Default)]
pub struct CachedSyncStatus {
    pub latest_block: i64,
    pub gaps: Vec<(i64, i64)>,
    pub gap_blocks: i64,
}

/// DuckDB connection pool for analytical queries.
///
/// DuckDB's Connection is !Sync (contains RefCell), so we run all operations
/// on a dedicated thread to avoid file descriptor corruption. The connection
/// is protected by a std::sync::Mutex and all operations are dispatched via
/// a channel to the dedicated thread.
pub struct DuckDbPool {
    /// Path to the DuckDB database file
    path: String,
    /// Sender to dispatch operations to the dedicated DuckDB thread
    op_tx: std::sync::mpsc::Sender<DuckDbOp>,
    /// Cached sync status (updated by background task)
    cached_status: RwLock<CachedSyncStatus>,
}

/// Operations that can be executed on the DuckDB thread.
enum DuckDbOp {
    /// Execute SQL that doesn't return results
    Execute {
        sql: String,
        result_tx: tokio::sync::oneshot::Sender<Result<usize>>,
    },
    /// Execute a query and return results as JSON
    Query {
        sql: String,
        result_tx: tokio::sync::oneshot::Sender<Result<(Vec<String>, Vec<Vec<serde_json::Value>>)>>,
    },
    /// Execute a streaming query
    QueryStreaming {
        sql: String,
        batch_size: usize,
        batch_tx: tokio::sync::mpsc::Sender<Result<Vec<Vec<serde_json::Value>>>>,
        result_tx: tokio::sync::oneshot::Sender<Result<Vec<String>>>,
    },
    /// Get min/max block range
    BlockRange {
        result_tx: tokio::sync::oneshot::Sender<Result<(Option<i64>, Option<i64>)>>,
    },
    /// Execute a custom closure (for complex operations like gap-fill)
    WithConnection {
        func: Box<dyn FnOnce(&Connection) -> Result<()> + Send>,
        result_tx: tokio::sync::oneshot::Sender<Result<()>>,
    },
    /// Execute a closure that returns a serializable result
    WithConnectionBoxed {
        func: Box<dyn FnOnce(&Connection) -> Result<Box<dyn std::any::Any + Send>> + Send>,
        result_tx: tokio::sync::oneshot::Sender<Result<Box<dyn std::any::Any + Send>>>,
    },
    /// Checkpoint the database
    Checkpoint {
        result_tx: tokio::sync::oneshot::Sender<Result<()>>,
    },
}

impl DuckDbPool {
    /// Creates a new DuckDB pool at the specified path.
    ///
    /// Creates the database file and schema if they don't exist.
    /// Spawns a dedicated thread for all DuckDB operations to avoid
    /// file descriptor corruption from cross-thread access.
    pub fn new(path: &str) -> Result<Self> {
        let (op_tx, op_rx) = std::sync::mpsc::channel::<DuckDbOp>();
        let path_owned = path.to_string();
        let path_for_thread = path.to_string();

        // Spawn dedicated thread for DuckDB operations
        thread::Builder::new()
            .name(format!("duckdb-{}", path))
            .spawn(move || {
                // Open connection on this thread
                let conn = match Connection::open(&path_for_thread) {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::error!(error = %e, "Failed to open DuckDB connection");
                        return;
                    }
                };

                // Performance tuning
                // Use 12GB per instance for gap-fill with high-tx blocks
                if let Err(e) = conn.execute_batch(
                    r#"
                    SET memory_limit = '12GB';
                    SET threads = 4;
                    SET checkpoint_threshold = '100MB';
                    SET preserve_insertion_order = false;
                    "#,
                ) {
                    tracing::error!(error = %e, "Failed to configure DuckDB");
                    return;
                }

                // Register native UDFs for fast hex parsing
                if let Err(e) = register_udfs(&conn) {
                    tracing::error!(error = %e, "Failed to register DuckDB UDFs");
                    return;
                }

                // Run schema migrations
                if let Err(e) = run_schema(&conn) {
                    tracing::error!(error = %e, "Failed to run DuckDB schema");
                    return;
                }

                tracing::info!(path = %path_for_thread, "DuckDB thread started");

                // Process operations
                while let Ok(op) = op_rx.recv() {
                    match op {
                        DuckDbOp::Execute { sql, result_tx } => {
                            let result = conn.execute(&sql, []).map_err(|e| anyhow::anyhow!("{}", e));
                            let _ = result_tx.send(result);
                        }
                        DuckDbOp::Query { sql, result_tx } => {
                            let result = execute_query(&conn, &sql);
                            let _ = result_tx.send(result);
                        }
                        DuckDbOp::QueryStreaming { sql, batch_size, batch_tx, result_tx } => {
                            let result = execute_query_streaming(&conn, &sql, batch_size, batch_tx);
                            let _ = result_tx.send(result);
                        }
                        DuckDbOp::BlockRange { result_tx } => {
                            let min: Option<i64> = conn.prepare("SELECT MIN(num) FROM blocks")
                                .ok()
                                .and_then(|mut stmt| stmt.query_row([], |row| row.get(0)).ok());
                            let max: Option<i64> = conn.prepare("SELECT MAX(num) FROM blocks")
                                .ok()
                                .and_then(|mut stmt| stmt.query_row([], |row| row.get(0)).ok());
                            let _ = result_tx.send(Ok((min, max)));
                        }
                        DuckDbOp::WithConnection { func, result_tx } => {
                            let result = func(&conn);
                            let _ = result_tx.send(result);
                        }
                        DuckDbOp::WithConnectionBoxed { func, result_tx } => {
                            let result = func(&conn);
                            let _ = result_tx.send(result);
                        }
                        DuckDbOp::Checkpoint { result_tx } => {
                            let result = conn.execute("CHECKPOINT", [])
                                .map(|_| ())
                                .map_err(|e| anyhow::anyhow!("{}", e));
                            let _ = result_tx.send(result);
                        }
                    }
                }

                tracing::info!(path = %path_for_thread, "DuckDB thread stopped");
            })?;

        Ok(Self {
            path: path_owned,
            op_tx,
            cached_status: RwLock::new(CachedSyncStatus::default()),
        })
    }

    /// Creates an in-memory DuckDB pool (useful for testing).
    pub fn in_memory() -> Result<Self> {
        Self::new(":memory:")
    }

    /// Opens a read-only connection to an existing DuckDB database.
    /// Use this for status queries when the main indexer may have the file locked.
    pub fn open_readonly(path: &str) -> Result<Self> {
        let (op_tx, op_rx) = std::sync::mpsc::channel::<DuckDbOp>();
        let path_owned = path.to_string();
        let path_for_thread = path.to_string();

        thread::Builder::new()
            .name(format!("duckdb-ro-{}", path))
            .spawn(move || {
                let conn = match Connection::open_with_flags(
                    &path_for_thread,
                    duckdb::Config::default().access_mode(duckdb::AccessMode::ReadOnly).unwrap(),
                ) {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::error!(error = %e, "Failed to open read-only DuckDB connection");
                        return;
                    }
                };

                if let Err(e) = register_udfs(&conn) {
                    tracing::error!(error = %e, "Failed to register DuckDB UDFs");
                    return;
                }

                while let Ok(op) = op_rx.recv() {
                    match op {
                        DuckDbOp::Execute { sql, result_tx } => {
                            let result = conn.execute(&sql, []).map_err(|e| anyhow::anyhow!("{}", e));
                            let _ = result_tx.send(result);
                        }
                        DuckDbOp::Query { sql, result_tx } => {
                            let result = execute_query(&conn, &sql);
                            let _ = result_tx.send(result);
                        }
                        DuckDbOp::QueryStreaming { sql, batch_size, batch_tx, result_tx } => {
                            let result = execute_query_streaming(&conn, &sql, batch_size, batch_tx);
                            let _ = result_tx.send(result);
                        }
                        DuckDbOp::BlockRange { result_tx } => {
                            let min: Option<i64> = conn.prepare("SELECT MIN(num) FROM blocks")
                                .ok()
                                .and_then(|mut stmt| stmt.query_row([], |row| row.get(0)).ok());
                            let max: Option<i64> = conn.prepare("SELECT MAX(num) FROM blocks")
                                .ok()
                                .and_then(|mut stmt| stmt.query_row([], |row| row.get(0)).ok());
                            let _ = result_tx.send(Ok((min, max)));
                        }
                        DuckDbOp::WithConnection { func, result_tx } => {
                            let result = func(&conn);
                            let _ = result_tx.send(result);
                        }
                        DuckDbOp::WithConnectionBoxed { func, result_tx } => {
                            let result = func(&conn);
                            let _ = result_tx.send(result);
                        }
                        DuckDbOp::Checkpoint { result_tx } => {
                            let _ = result_tx.send(Ok(()));
                        }
                    }
                }
            })?;

        Ok(Self {
            path: path_owned,
            op_tx,
            cached_status: RwLock::new(CachedSyncStatus::default()),
        })
    }

    /// Updates the cached sync status (called by background replicator task).
    pub fn update_cached_status(&self, latest_block: i64, gaps: Vec<(i64, i64)>, gap_blocks: i64) {
        if let Ok(mut status) = self.cached_status.write() {
            status.latest_block = latest_block;
            status.gaps = gaps;
            status.gap_blocks = gap_blocks;
        }
    }

    /// Gets the cached sync status (fast, no DB query).
    pub fn get_cached_status(&self) -> CachedSyncStatus {
        self.cached_status.read().map(|s| s.clone()).unwrap_or_default()
    }

    /// Returns true if this is an in-memory database.
    pub fn is_in_memory(&self) -> bool {
        self.path == ":memory:"
    }

    /// Returns the database path.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Execute SQL that doesn't return results.
    pub async fn execute(&self, sql: &str) -> Result<usize> {
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        self.op_tx
            .send(DuckDbOp::Execute {
                sql: sql.to_string(),
                result_tx,
            })
            .map_err(|_| anyhow::anyhow!("DuckDB thread stopped"))?;
        result_rx.await.map_err(|_| anyhow::anyhow!("DuckDB thread stopped"))?
    }

    /// Execute a closure with direct access to the connection.
    /// The closure runs on the dedicated DuckDB thread.
    pub async fn with_connection<F>(&self, func: F) -> Result<()>
    where
        F: FnOnce(&Connection) -> Result<()> + Send + 'static,
    {
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        self.op_tx
            .send(DuckDbOp::WithConnection {
                func: Box::new(func),
                result_tx,
            })
            .map_err(|_| anyhow::anyhow!("DuckDB thread stopped"))?;
        result_rx.await.map_err(|_| anyhow::anyhow!("DuckDB thread stopped"))?
    }

    /// Execute a closure with direct access to the connection that returns a value.
    /// The closure runs on the dedicated DuckDB thread.
    pub async fn with_connection_result<F, T>(&self, func: F) -> Result<T>
    where
        F: FnOnce(&Connection) -> Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        self.op_tx
            .send(DuckDbOp::WithConnectionBoxed {
                func: Box::new(move |conn| {
                    let result = func(conn)?;
                    Ok(Box::new(result) as Box<dyn std::any::Any + Send>)
                }),
                result_tx,
            })
            .map_err(|_| anyhow::anyhow!("DuckDB thread stopped"))?;
        let boxed = result_rx.await.map_err(|_| anyhow::anyhow!("DuckDB thread stopped"))??;
        boxed
            .downcast::<T>()
            .map(|b| *b)
            .map_err(|_| anyhow::anyhow!("Type mismatch in DuckDB result"))
    }

    /// Checkpoint the database to flush WAL.
    pub async fn checkpoint(&self) -> Result<()> {
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        self.op_tx
            .send(DuckDbOp::Checkpoint { result_tx })
            .map_err(|_| anyhow::anyhow!("DuckDB thread stopped"))?;
        result_rx.await.map_err(|_| anyhow::anyhow!("DuckDB thread stopped"))?
    }

    /// Execute multiple SQL statements in a batch.
    /// All statements run on the dedicated DuckDB thread.
    pub async fn execute_batch(&self, statements: Vec<String>) -> Result<()> {
        self.with_connection(move |conn| {
            for sql in statements {
                conn.execute(&sql, [])?;
            }
            Ok(())
        }).await
    }

    /// Delete all blocks (and related txs, logs, receipts) from a given block number onwards.
    /// Used for reorg handling - removes orphaned blocks so they can be re-synced.
    /// Returns the number of blocks deleted.
    pub async fn delete_blocks_from(&self, from_block: i64) -> Result<u64> {
        self.with_connection_result(move |conn| {
            // Delete in order: logs, receipts, txs, blocks
            conn.execute(
                &format!("DELETE FROM logs WHERE block_num >= {}", from_block),
                [],
            )?;
            conn.execute(
                &format!("DELETE FROM receipts WHERE block_num >= {}", from_block),
                [],
            )?;
            conn.execute(
                &format!("DELETE FROM txs WHERE block_num >= {}", from_block),
                [],
            )?;
            let deleted = conn.execute(
                &format!("DELETE FROM blocks WHERE num >= {}", from_block),
                [],
            )?;
            Ok(deleted as u64)
        })
        .await
    }

    /// Gets the latest synced block number from DuckDB.
    pub async fn latest_block(&self) -> Result<Option<i64>> {
        let (_min, max) = self.block_range().await?;
        Ok(max)
    }

    /// Gets the earliest synced block number from DuckDB.
    pub async fn earliest_block(&self) -> Result<Option<i64>> {
        let (min, _max) = self.block_range().await?;
        Ok(min)
    }

    /// Gets both min and max block numbers from DuckDB.
    pub async fn block_range(&self) -> Result<(Option<i64>, Option<i64>)> {
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        self.op_tx
            .send(DuckDbOp::BlockRange { result_tx })
            .map_err(|_| anyhow::anyhow!("DuckDB thread stopped"))?;
        result_rx.await.map_err(|_| anyhow::anyhow!("DuckDB thread stopped"))?
    }

    /// Executes a query and returns results as JSON.
    /// Runs on the dedicated DuckDB thread.
    pub async fn query(&self, sql: &str) -> Result<(Vec<String>, Vec<Vec<serde_json::Value>>)> {
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        self.op_tx
            .send(DuckDbOp::Query {
                sql: sql.to_string(),
                result_tx,
            })
            .map_err(|_| anyhow::anyhow!("DuckDB thread stopped"))?;
        result_rx.await.map_err(|_| anyhow::anyhow!("DuckDB thread stopped"))?
    }

    /// Executes a streaming query and sends batches through a channel.
    ///
    /// Returns column names immediately, then sends row batches through the channel.
    /// This allows SSE streaming of large result sets.
    /// Runs on the dedicated DuckDB thread.
    pub async fn query_streaming(
        &self,
        sql: &str,
        batch_size: usize,
        batch_tx: tokio::sync::mpsc::Sender<Result<Vec<Vec<serde_json::Value>>>>,
    ) -> Result<Vec<String>> {
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        self.op_tx
            .send(DuckDbOp::QueryStreaming {
                sql: sql.to_string(),
                batch_size,
                batch_tx,
                result_tx,
            })
            .map_err(|_| anyhow::anyhow!("DuckDB thread stopped"))?;
        result_rx.await.map_err(|_| anyhow::anyhow!("DuckDB thread stopped"))?
    }
}

/// Creates the DuckDB schema matching PostgreSQL tables.
fn run_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        -- Blocks table (hex strings for hashes)
        CREATE TABLE IF NOT EXISTS blocks (
            num             BIGINT NOT NULL PRIMARY KEY,
            hash            VARCHAR NOT NULL,
            parent_hash     VARCHAR NOT NULL,
            timestamp       TIMESTAMPTZ NOT NULL,
            timestamp_ms    BIGINT NOT NULL,
            gas_limit       BIGINT NOT NULL,
            gas_used        BIGINT NOT NULL,
            miner           VARCHAR NOT NULL,
            extra_data      VARCHAR
        );

        -- Transactions table
        CREATE TABLE IF NOT EXISTS txs (
            block_num               BIGINT NOT NULL,
            block_timestamp         TIMESTAMPTZ NOT NULL,
            idx                     INTEGER NOT NULL,
            hash                    VARCHAR NOT NULL,
            type                    SMALLINT NOT NULL,
            "from"                  VARCHAR NOT NULL,
            "to"                    VARCHAR,
            value                   VARCHAR NOT NULL,
            input                   VARCHAR NOT NULL,
            gas_limit               BIGINT NOT NULL,
            max_fee_per_gas         VARCHAR NOT NULL,
            max_priority_fee_per_gas VARCHAR NOT NULL,
            gas_used                BIGINT,
            nonce_key               VARCHAR NOT NULL,
            nonce                   BIGINT NOT NULL,
            fee_token               VARCHAR,
            fee_payer               VARCHAR,
            calls                   VARCHAR,
            call_count              SMALLINT NOT NULL DEFAULT 1,
            valid_before            BIGINT,
            valid_after             BIGINT,
            signature_type          SMALLINT,
            PRIMARY KEY (block_num, idx)
        );

        -- Logs table
        CREATE TABLE IF NOT EXISTS logs (
            block_num       BIGINT NOT NULL,
            block_timestamp TIMESTAMPTZ NOT NULL,
            log_idx         INTEGER NOT NULL,
            tx_idx          INTEGER NOT NULL,
            tx_hash         VARCHAR NOT NULL,
            address         VARCHAR NOT NULL,
            selector        VARCHAR,
            topic0          VARCHAR,
            topic1          VARCHAR,
            topic2          VARCHAR,
            topic3          VARCHAR,
            data            VARCHAR NOT NULL,
            PRIMARY KEY (block_num, log_idx)
        );

        -- Receipts table
        CREATE TABLE IF NOT EXISTS receipts (
            block_num               BIGINT NOT NULL,
            block_timestamp         TIMESTAMPTZ NOT NULL,
            tx_idx                  INTEGER NOT NULL,
            tx_hash                 VARCHAR NOT NULL,
            "from"                  VARCHAR NOT NULL,
            "to"                    VARCHAR,
            contract_address        VARCHAR,
            gas_used                BIGINT NOT NULL,
            cumulative_gas_used     BIGINT NOT NULL,
            effective_gas_price     VARCHAR,
            status                  SMALLINT,
            fee_payer               VARCHAR,
            PRIMARY KEY (block_num, tx_idx)
        );

        -- Sync state tracking
        CREATE TABLE IF NOT EXISTS duckdb_sync_state (
            id              INTEGER PRIMARY KEY DEFAULT 1,
            latest_block    BIGINT NOT NULL DEFAULT 0,
            updated_at      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        -- Initialize sync state if empty
        INSERT INTO duckdb_sync_state (id, latest_block)
        SELECT 1, 0
        WHERE NOT EXISTS (SELECT 1 FROM duckdb_sync_state WHERE id = 1);

        -- Create indexes for common query patterns
        CREATE INDEX IF NOT EXISTS idx_blocks_timestamp ON blocks (timestamp);
        CREATE INDEX IF NOT EXISTS idx_txs_hash ON txs (hash);
        CREATE INDEX IF NOT EXISTS idx_txs_from ON txs ("from");
        CREATE INDEX IF NOT EXISTS idx_txs_to ON txs ("to");
        CREATE INDEX IF NOT EXISTS idx_logs_address ON logs (address);
        CREATE INDEX IF NOT EXISTS idx_logs_selector ON logs (selector);
        CREATE INDEX IF NOT EXISTS idx_logs_tx_hash ON logs (tx_hash);
        CREATE INDEX IF NOT EXISTS idx_logs_topic0 ON logs (topic0);
        CREATE INDEX IF NOT EXISTS idx_logs_topic1 ON logs (topic1);
        CREATE INDEX IF NOT EXISTS idx_logs_topic2 ON logs (topic2);
        CREATE INDEX IF NOT EXISTS idx_logs_topic3 ON logs (topic3);

        -- ABI decoding macros (equivalent to PostgreSQL abi_* functions)
        -- DuckDB stores hex strings, so we work with substrings directly
        
        -- Helper: convert single hex char to int (0-15)
        CREATE OR REPLACE MACRO hex_char_to_int(c) AS (
            CASE 
                WHEN c BETWEEN '0' AND '9' THEN ascii(c) - 48
                WHEN c BETWEEN 'a' AND 'f' THEN ascii(c) - 87
                WHEN c BETWEEN 'A' AND 'F' THEN ascii(c) - 55
                ELSE 0
            END
        );
        
        -- Helper: convert 2-char hex byte to int (0-255)
        CREATE OR REPLACE MACRO hex_byte_to_int(hex2) AS (
            hex_char_to_int(substring(hex2, 1, 1)) * 16 + 
            hex_char_to_int(substring(hex2, 2, 1))
        );
        
        -- Convert 32-char hex string (128 bits / 16 bytes) to HUGEINT
        -- Properly handles values up to 2^127-1 (signed 128-bit)
        CREATE OR REPLACE MACRO hex16_to_hugeint(hex32) AS (
            hex_byte_to_int(substring(hex32, 1, 2))::HUGEINT   * 1329227995784915872903807060280344576 +
            hex_byte_to_int(substring(hex32, 3, 2))::HUGEINT   * 5192296858534827628530496329220096 +
            hex_byte_to_int(substring(hex32, 5, 2))::HUGEINT   * 20282409603651670423947251286016 +
            hex_byte_to_int(substring(hex32, 7, 2))::HUGEINT   * 79228162514264337593543950336 +
            hex_byte_to_int(substring(hex32, 9, 2))::HUGEINT   * 309485009821345068724781056 +
            hex_byte_to_int(substring(hex32, 11, 2))::HUGEINT  * 1208925819614629174706176 +
            hex_byte_to_int(substring(hex32, 13, 2))::HUGEINT  * 4722366482869645213696 +
            hex_byte_to_int(substring(hex32, 15, 2))::HUGEINT  * 18446744073709551616 +
            hex_byte_to_int(substring(hex32, 17, 2))::HUGEINT  * 72057594037927936 +
            hex_byte_to_int(substring(hex32, 19, 2))::HUGEINT  * 281474976710656 +
            hex_byte_to_int(substring(hex32, 21, 2))::HUGEINT  * 1099511627776 +
            hex_byte_to_int(substring(hex32, 23, 2))::HUGEINT  * 4294967296 +
            hex_byte_to_int(substring(hex32, 25, 2))::HUGEINT  * 16777216 +
            hex_byte_to_int(substring(hex32, 27, 2))::HUGEINT  * 65536 +
            hex_byte_to_int(substring(hex32, 29, 2))::HUGEINT  * 256 +
            hex_byte_to_int(substring(hex32, 31, 2))::HUGEINT
        );
        
        -- Extract lower 128 bits of uint256 as HUGEINT (proper 128-bit precision)
        -- For values exceeding 2^127, use abi_uint_hex for full 256-bit as string
        CREATE OR REPLACE MACRO abi_uint(hex_data, byte_offset) AS (
            CASE 
                WHEN length(hex_data) >= 2 + (byte_offset + 32) * 2
                THEN hex16_to_hugeint(substring(hex_data, 3 + byte_offset * 2 + 32, 32))
                ELSE NULL
            END
        );

        -- Extract uint256 as hex string (full 256-bit precision)
        CREATE OR REPLACE MACRO abi_uint_hex(hex_data, byte_offset) AS (
            CASE 
                WHEN length(hex_data) >= 2 + (byte_offset + 32) * 2
                THEN '0x' || substring(hex_data, 3 + byte_offset * 2, 64)
                ELSE NULL
            END
        );

        -- Extract address from hex data at byte offset (last 20 bytes of 32-byte slot)
        CREATE OR REPLACE MACRO abi_address(hex_data, byte_offset) AS (
            CASE 
                WHEN length(hex_data) >= 2 + (byte_offset + 32) * 2
                THEN '0x' || substring(hex_data, 3 + byte_offset * 2 + 24, 40)
                ELSE NULL
            END
        );

        -- Extract bool from hex data at byte offset
        CREATE OR REPLACE MACRO abi_bool(hex_data, byte_offset) AS (
            CASE 
                WHEN length(hex_data) >= 2 + (byte_offset + 32) * 2
                THEN substring(hex_data, 3 + byte_offset * 2 + 62, 2) != '00'
                ELSE NULL
            END
        );

        -- Extract bytes32 from hex data at byte offset
        CREATE OR REPLACE MACRO abi_bytes32(hex_data, byte_offset) AS (
            CASE 
                WHEN length(hex_data) >= 2 + (byte_offset + 32) * 2
                THEN '0x' || substring(hex_data, 3 + byte_offset * 2, 64)
                ELSE NULL
            END
        );

        -- Extract topic (already a hex string in topics array)
        CREATE OR REPLACE MACRO topic_address(topic) AS (
            '0x' || substring(topic, 27, 40)
        );

        -- Extract uint from topic (lower 128 bits as HUGEINT)
        CREATE OR REPLACE MACRO topic_uint(topic) AS (
            hex16_to_hugeint(substring(topic, 35, 32))
        );
        "#,
    )
    .context("Failed to create DuckDB schema")?;

    Ok(())
}

// ============================================================================
// Native Rust UDFs for fast hex parsing (replaces slow SQL macros)
// ============================================================================

/// Register native Rust UDFs for ABI decoding.
/// These are ~100x faster than the equivalent SQL macros.
fn register_udfs(conn: &Connection) -> Result<()> {
    conn.register_scalar_function::<HexToHugeint>("hex_to_hugeint")
        .context("Failed to register hex_to_hugeint UDF")?;
    conn.register_scalar_function::<AbiUint>("abi_uint_native")
        .context("Failed to register abi_uint_native UDF")?;
    conn.register_scalar_function::<TopicUint>("topic_uint_native")
        .context("Failed to register topic_uint_native UDF")?;
    conn.register_scalar_function::<TopicAddress>("topic_address_native")
        .context("Failed to register topic_address_native UDF")?;
    conn.register_scalar_function::<AbiAddress>("abi_address_native")
        .context("Failed to register abi_address_native UDF")?;
    Ok(())
}

/// Parse a 32-char hex string (128 bits) to HUGEINT.
/// Input: "00000000000000000000000000000064" (no 0x prefix)
struct HexToHugeint;

impl VScalar for HexToHugeint {
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        let input_vector = input.flat_vector(0);
        let input_strings = input_vector.as_slice_with_len::<duckdb_string_t>(input.len());
        let len = input.len();

        // First pass: compute values and track nulls
        let mut results: Vec<Option<i128>> = Vec::with_capacity(len);
        for i in 0..len {
            if input_vector.row_is_null(i as u64) {
                results.push(None);
                continue;
            }

            let mut duckdb_str = input_strings[i];
            let hex_str = DuckString::new(&mut duckdb_str).as_str();

            match i128::from_str_radix(hex_str.as_ref(), 16) {
                Ok(value) => results.push(Some(value)),
                Err(_) => results.push(None),
            }
        }

        // Second pass: write values
        let mut output_vector = output.flat_vector();
        {
            let output_data = output_vector.as_mut_slice::<i128>();
            for (i, result) in results.iter().enumerate() {
                if let Some(value) = result {
                    output_data[i] = *value;
                }
            }
        }

        // Third pass: set nulls
        for (i, result) in results.iter().enumerate() {
            if result.is_none() {
                output_vector.set_null(i);
            }
        }
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)],
            LogicalTypeHandle::from(LogicalTypeId::Hugeint),
        )]
    }
}

/// Extract uint from ABI-encoded data at byte offset (lower 128 bits).
/// Input: (hex_data with 0x prefix, byte_offset)
struct AbiUint;

impl VScalar for AbiUint {
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        let data_vector = input.flat_vector(0);
        let data_strings = data_vector.as_slice_with_len::<duckdb_string_t>(input.len());

        let offset_vector = input.flat_vector(1);
        let offsets = offset_vector.as_slice_with_len::<i32>(input.len());
        let len = input.len();

        // First pass: compute values
        let mut results: Vec<Option<i128>> = Vec::with_capacity(len);
        for i in 0..len {
            if data_vector.row_is_null(i as u64) || offset_vector.row_is_null(i as u64) {
                results.push(None);
                continue;
            }

            let mut duckdb_str = data_strings[i];
            let hex_data = DuckString::new(&mut duckdb_str).as_str();
            let offset = offsets[i] as usize;

            // hex_data format: 0x + 64 hex chars per 32-byte slot
            // We want lower 128 bits = last 32 hex chars of the slot
            let start = 2 + offset * 2 + 32; // skip 0x, skip to offset, skip high 128 bits
            let end = start + 32;

            if hex_data.as_ref().len() >= end {
                let hex_slice = &hex_data.as_ref()[start..end];
                match i128::from_str_radix(hex_slice, 16) {
                    Ok(value) => results.push(Some(value)),
                    Err(_) => results.push(None),
                }
            } else {
                results.push(None);
            }
        }

        // Second pass: write values
        let mut output_vector = output.flat_vector();
        {
            let output_data = output_vector.as_mut_slice::<i128>();
            for (i, result) in results.iter().enumerate() {
                if let Some(value) = result {
                    output_data[i] = *value;
                }
            }
        }

        // Third pass: set nulls
        for (i, result) in results.iter().enumerate() {
            if result.is_none() {
                output_vector.set_null(i);
            }
        }
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
                LogicalTypeHandle::from(LogicalTypeId::Integer),
            ],
            LogicalTypeHandle::from(LogicalTypeId::Hugeint),
        )]
    }
}

/// Extract uint from indexed topic (lower 128 bits).
/// Input: topic with 0x prefix (66 chars total)
struct TopicUint;

impl VScalar for TopicUint {
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        let input_vector = input.flat_vector(0);
        let input_strings = input_vector.as_slice_with_len::<duckdb_string_t>(input.len());
        let len = input.len();

        // First pass: compute values
        let mut results: Vec<Option<i128>> = Vec::with_capacity(len);
        for i in 0..len {
            if input_vector.row_is_null(i as u64) {
                results.push(None);
                continue;
            }

            let mut duckdb_str = input_strings[i];
            let topic = DuckString::new(&mut duckdb_str).as_str();

            // Topic is 0x + 64 hex chars, we want lower 128 bits (last 32 hex chars)
            if topic.as_ref().len() >= 66 {
                let hex_slice = &topic.as_ref()[34..66]; // positions 34-65 = last 32 chars
                match i128::from_str_radix(hex_slice, 16) {
                    Ok(value) => results.push(Some(value)),
                    Err(_) => results.push(None),
                }
            } else {
                results.push(None);
            }
        }

        // Second pass: write values
        let mut output_vector = output.flat_vector();
        {
            let output_data = output_vector.as_mut_slice::<i128>();
            for (i, result) in results.iter().enumerate() {
                if let Some(value) = result {
                    output_data[i] = *value;
                }
            }
        }

        // Third pass: set nulls
        for (i, result) in results.iter().enumerate() {
            if result.is_none() {
                output_vector.set_null(i);
            }
        }
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)],
            LogicalTypeHandle::from(LogicalTypeId::Hugeint),
        )]
    }
}

/// Extract address from indexed topic (last 40 hex chars = 20 bytes).
/// Input: topic with 0x prefix
/// Output: 0x + lowercase address
struct TopicAddress;

impl VScalar for TopicAddress {
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        let input_vector = input.flat_vector(0);
        let input_strings = input_vector.as_slice_with_len::<duckdb_string_t>(input.len());
        let len = input.len();

        // First pass: compute values
        let mut results: Vec<Option<String>> = Vec::with_capacity(len);
        for i in 0..len {
            if input_vector.row_is_null(i as u64) {
                results.push(None);
                continue;
            }

            let mut duckdb_str = input_strings[i];
            let topic = DuckString::new(&mut duckdb_str).as_str();

            // Topic is 0x + 64 hex chars, address is last 40 chars (positions 26-65)
            if topic.as_ref().len() >= 66 {
                let addr = format!("0x{}", &topic.as_ref()[26..66]);
                results.push(Some(addr));
            } else {
                results.push(None);
            }
        }
        
        // Second pass: write to output
        let mut output_vector = output.flat_vector();
        for (i, result) in results.iter().enumerate() {
            match result {
                Some(addr) => output_vector.insert(i, addr.as_str()),
                None => output_vector.set_null(i),
            }
        }
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)],
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        )]
    }
}

/// Extract address from ABI-encoded data at byte offset.
/// Input: (hex_data with 0x prefix, byte_offset)
struct AbiAddress;

impl VScalar for AbiAddress {
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        let data_vector = input.flat_vector(0);
        let data_strings = data_vector.as_slice_with_len::<duckdb_string_t>(input.len());

        let offset_vector = input.flat_vector(1);
        let offsets = offset_vector.as_slice_with_len::<i32>(input.len());
        let len = input.len();

        // First pass: compute values
        let mut results: Vec<Option<String>> = Vec::with_capacity(len);
        for i in 0..len {
            if data_vector.row_is_null(i as u64) || offset_vector.row_is_null(i as u64) {
                results.push(None);
                continue;
            }

            let mut duckdb_str = data_strings[i];
            let hex_data = DuckString::new(&mut duckdb_str).as_str();
            let offset = offsets[i] as usize;

            // Address is last 40 hex chars (20 bytes) of the 32-byte slot
            let start = 2 + offset * 2 + 24; // skip 0x, skip to offset, skip 12 bytes padding
            let end = start + 40;

            if hex_data.as_ref().len() >= end {
                let addr = format!("0x{}", &hex_data.as_ref()[start..end]);
                results.push(Some(addr));
            } else {
                results.push(None);
            }
        }

        // Second pass: write to output
        let mut output_vector = output.flat_vector();
        for (i, result) in results.iter().enumerate() {
            match result {
                Some(addr) => output_vector.insert(i, addr.as_str()),
                None => output_vector.set_null(i),
            }
        }
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
                LogicalTypeHandle::from(LogicalTypeId::Integer),
            ],
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        )]
    }
}

/// Executes a query on DuckDB and returns results as JSON values.
pub fn execute_query(
    conn: &Connection,
    sql: &str,
) -> Result<(Vec<String>, Vec<Vec<serde_json::Value>>)> {
    let mut stmt = conn.prepare(sql).with_context(|| format!("Failed to prepare DuckDB query: {sql}"))?;

    // Execute query and get rows iterator
    let mut rows_iter = stmt.query([]).with_context(|| format!("Failed to execute DuckDB query: {sql}"))?;

    // Get column info from the statement after execution
    let column_count = rows_iter.as_ref().map(|r| r.column_count()).unwrap_or(0);
    let columns: Vec<String> = if let Some(first_row) = rows_iter.as_ref() {
        (0..column_count)
            .map(|i| {
                first_row
                    .column_name(i)
                    .map(|s| s.to_string())
                    .unwrap_or_else(|_| "?".to_string())
            })
            .collect()
    } else {
        vec![]
    };

    // Collect all rows
    let mut result_rows = Vec::new();
    while let Some(row) = rows_iter.next()? {
        let mut values = Vec::with_capacity(column_count);
        for i in 0..column_count {
            let value = row_to_json_value(&row, i);
            values.push(value);
        }
        result_rows.push(values);
    }

    Ok((columns, result_rows))
}

/// Executes a streaming query on DuckDB, sending batches through a channel.
///
/// Returns column names immediately. Row batches are sent through the channel
/// as they are read, enabling SSE streaming of large result sets.
pub fn execute_query_streaming(
    conn: &Connection,
    sql: &str,
    batch_size: usize,
    tx: tokio::sync::mpsc::Sender<Result<Vec<Vec<serde_json::Value>>>>,
) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(sql).with_context(|| format!("Failed to prepare DuckDB query: {sql}"))?;

    // Execute query and get rows iterator
    let mut rows_iter = stmt.query([]).with_context(|| format!("Failed to execute DuckDB query: {sql}"))?;

    // Get column info from the statement after execution
    let column_count = rows_iter.as_ref().map(|r| r.column_count()).unwrap_or(0);
    let columns: Vec<String> = if let Some(first_row) = rows_iter.as_ref() {
        (0..column_count)
            .map(|i| {
                first_row
                    .column_name(i)
                    .map(|s| s.to_string())
                    .unwrap_or_else(|_| "?".to_string())
            })
            .collect()
    } else {
        vec![]
    };

    // Stream rows in batches
    let mut batch = Vec::with_capacity(batch_size);
    while let Some(row) = rows_iter.next()? {
        let mut values = Vec::with_capacity(column_count);
        for i in 0..column_count {
            let value = row_to_json_value(&row, i);
            values.push(value);
        }
        batch.push(values);

        if batch.len() >= batch_size {
            // Send batch (blocking - we're in sync context)
            if tx.blocking_send(Ok(std::mem::take(&mut batch))).is_err() {
                // Receiver dropped, stop processing
                break;
            }
            batch = Vec::with_capacity(batch_size);
        }
    }

    // Send remaining rows
    if !batch.is_empty() {
        let _ = tx.blocking_send(Ok(batch));
    }

    Ok(columns)
}

/// Converts a DuckDB row column to a JSON value.
fn row_to_json_value(row: &duckdb::Row<'_>, idx: usize) -> serde_json::Value {
    // Try HUGEINT (i128) first - common for decoded uint256 values
    if let Ok(v) = row.get::<_, i128>(idx) {
        // If it fits in i64, use native JSON number for better compatibility
        if v >= i64::MIN as i128 && v <= i64::MAX as i128 {
            return serde_json::Value::Number((v as i64).into());
        }
        // Otherwise return as string to preserve precision
        return serde_json::Value::String(v.to_string());
    }
    // Try i64 next (most common for block numbers, counts, etc.)
    if let Ok(v) = row.get::<_, i64>(idx) {
        return serde_json::Value::Number(v.into());
    }
    if let Ok(v) = row.get::<_, f64>(idx) {
        return serde_json::Number::from_f64(v)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null);
    }
    if let Ok(v) = row.get::<_, String>(idx) {
        return serde_json::Value::String(v);
    }
    if let Ok(v) = row.get::<_, bool>(idx) {
        return serde_json::Value::Bool(v);
    }
    if let Ok(v) = row.get::<_, i32>(idx) {
        return serde_json::Value::Number(v.into());
    }
    if let Ok(v) = row.get::<_, Option<String>>(idx) {
        return v
            .map(serde_json::Value::String)
            .unwrap_or(serde_json::Value::Null);
    }

    // Fallback to null
    serde_json::Value::Null
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_in_memory_pool() {
        let pool = DuckDbPool::in_memory().unwrap();
        assert_eq!(pool.path(), ":memory:");
    }

    #[tokio::test]
    async fn test_schema_creation() {
        let pool = DuckDbPool::in_memory().unwrap();

        // Verify tables exist
        let (_, rows) = pool.query(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'",
        ).await.unwrap();

        let table_names: Vec<&str> = rows
            .iter()
            .filter_map(|r| r.first().and_then(|v| v.as_str()))
            .collect();

        assert!(table_names.contains(&"blocks"));
        assert!(table_names.contains(&"txs"));
        assert!(table_names.contains(&"logs"));
        assert!(table_names.contains(&"duckdb_sync_state"));
    }

    #[tokio::test]
    async fn test_insert_and_query() {
        let pool = DuckDbPool::in_memory().unwrap();

        // Insert a block
        pool.execute(
            "INSERT INTO blocks (num, hash, parent_hash, timestamp, timestamp_ms, gas_limit, gas_used, miner)
             VALUES (1, '0xabc', '0x000', '2024-01-01 00:00:00+00', 1704067200000, 30000000, 21000, '0xminer')",
        ).await.unwrap();

        // Query it back
        let (_, rows) = pool.query("SELECT num, hash FROM blocks WHERE num = 1").await.unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], serde_json::json!(1));
        assert_eq!(rows[0][1], serde_json::json!("0xabc"));
    }

    #[tokio::test]
    async fn test_aggregation_query() {
        let pool = DuckDbPool::in_memory().unwrap();

        // Insert multiple blocks
        for i in 1..=10 {
            pool.execute(
                &format!(
                    "INSERT INTO blocks (num, hash, parent_hash, timestamp, timestamp_ms, gas_limit, gas_used, miner)
                     VALUES ({i}, '0x{i:03x}', '0x000', '2024-01-01 00:00:00+00', {}, 30000000, {}, '0xminer')",
                    1_704_067_200_000_i64 + i * 1000,
                    21000 * i
                ),
            ).await.unwrap();
        }

        // Run aggregation
        let (cols, rows) = pool.query("SELECT COUNT(*) as cnt, SUM(gas_used) as total FROM blocks")
            .await.unwrap();

        assert_eq!(cols, vec!["cnt", "total"]);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], serde_json::json!(10));
        // Sum of 21000 * (1+2+...+10) = 21000 * 55 = 1155000
        assert_eq!(rows[0][1], serde_json::json!(1155000));
    }

    // ========================================================================
    // Native UDF Tests
    // ========================================================================

    mod udf_tests {
        use super::*;

        async fn get_i128_async(pool: &DuckDbPool, sql: &str) -> Option<i128> {
            let sql = sql.to_string();
            pool.with_connection_result(move |conn| {
                let mut stmt = conn.prepare(&sql)?;
                let result: Option<i128> = stmt.query_row([], |row| row.get::<_, i128>(0)).ok();
                Ok(result)
            }).await.ok().flatten()
        }

        fn get_i128(pool: &DuckDbPool, sql: &str) -> Option<i128> {
            futures::executor::block_on(get_i128_async(pool, sql))
        }

        async fn get_string_async(pool: &DuckDbPool, sql: &str) -> Option<String> {
            let sql = sql.to_string();
            pool.with_connection_result(move |conn| {
                let mut stmt = conn.prepare(&sql)?;
                let result: Option<String> = stmt.query_row([], |row| row.get::<_, String>(0)).ok();
                Ok(result)
            }).await.ok().flatten()
        }

        fn get_string(pool: &DuckDbPool, sql: &str) -> Option<String> {
            futures::executor::block_on(get_string_async(pool, sql))
        }

        async fn is_null_async(pool: &DuckDbPool, sql: &str) -> bool {
            let sql = sql.to_string();
            pool.with_connection_result(move |conn| {
                let mut stmt = conn.prepare(&sql)?;
                let result: Option<Option<String>> = stmt.query_row([], |row| row.get::<_, Option<String>>(0)).ok();
                Ok(result.flatten().is_none())
            }).await.unwrap_or(true)
        }

        fn is_null(pool: &DuckDbPool, sql: &str) -> bool {
            futures::executor::block_on(is_null_async(pool, sql))
        }

        // --------------------------------------------------------------------
        // hex_to_hugeint tests
        // --------------------------------------------------------------------

        #[test]
        fn test_hex_to_hugeint_zero() {
            let pool = DuckDbPool::in_memory().unwrap();
            let result = get_i128(&pool, "SELECT hex_to_hugeint('00000000000000000000000000000000')");
            assert_eq!(result, Some(0));
        }

        #[test]
        fn test_hex_to_hugeint_one() {
            let pool = DuckDbPool::in_memory().unwrap();
            let result = get_i128(&pool, "SELECT hex_to_hugeint('00000000000000000000000000000001')");
            assert_eq!(result, Some(1));
        }

        #[test]
        fn test_hex_to_hugeint_small_value() {
            let pool = DuckDbPool::in_memory().unwrap();
            // 100 in hex = 0x64
            let result = get_i128(&pool, "SELECT hex_to_hugeint('00000000000000000000000000000064')");
            assert_eq!(result, Some(100));
        }

        #[test]
        fn test_hex_to_hugeint_large_value() {
            let pool = DuckDbPool::in_memory().unwrap();
            // 1_000_000_000_000_000_000 (1e18) = 0x0de0b6b3a7640000
            let result = get_i128(&pool, "SELECT hex_to_hugeint('00000000000000000de0b6b3a7640000')");
            assert_eq!(result, Some(1_000_000_000_000_000_000));
        }

        #[test]
        fn test_hex_to_hugeint_max_safe_value() {
            let pool = DuckDbPool::in_memory().unwrap();
            // Test a value near i128::MAX / 2 to ensure no overflow
            let result = get_i128(&pool, "SELECT hex_to_hugeint('0fffffffffffffffffffffffffffffff')");
            assert!(result.is_some());
            assert!(result.unwrap() > 0);
        }

        #[test]
        fn test_hex_to_hugeint_uppercase() {
            let pool = DuckDbPool::in_memory().unwrap();
            let result = get_i128(&pool, "SELECT hex_to_hugeint('0000000000000000000000000000ABCD')");
            assert_eq!(result, Some(0xABCD));
        }

        #[test]
        fn test_hex_to_hugeint_mixed_case() {
            let pool = DuckDbPool::in_memory().unwrap();
            let result = get_i128(&pool, "SELECT hex_to_hugeint('0000000000000000000000000000AbCd')");
            assert_eq!(result, Some(0xABCD));
        }

        #[test]
        fn test_hex_to_hugeint_null_input() {
            let pool = DuckDbPool::in_memory().unwrap();
            assert!(is_null(&pool, "SELECT hex_to_hugeint(NULL)"));
        }

        #[test]
        fn test_hex_to_hugeint_invalid_chars() {
            let pool = DuckDbPool::in_memory().unwrap();
            // 'g' is not valid hex
            assert!(is_null(&pool, "SELECT hex_to_hugeint('0000000000000000000000000000000g')"));
        }

        // --------------------------------------------------------------------
        // topic_uint_native tests
        // --------------------------------------------------------------------

        #[test]
        fn test_topic_uint_zero() {
            let pool = DuckDbPool::in_memory().unwrap();
            let topic = "0x0000000000000000000000000000000000000000000000000000000000000000";
            let result = get_i128(&pool, &format!("SELECT topic_uint_native('{topic}')"));
            assert_eq!(result, Some(0));
        }

        #[test]
        fn test_topic_uint_one() {
            let pool = DuckDbPool::in_memory().unwrap();
            let topic = "0x0000000000000000000000000000000000000000000000000000000000000001";
            let result = get_i128(&pool, &format!("SELECT topic_uint_native('{topic}')"));
            assert_eq!(result, Some(1));
        }

        #[test]
        fn test_topic_uint_100() {
            let pool = DuckDbPool::in_memory().unwrap();
            // 100 = 0x64
            let topic = "0x0000000000000000000000000000000000000000000000000000000000000064";
            let result = get_i128(&pool, &format!("SELECT topic_uint_native('{topic}')"));
            assert_eq!(result, Some(100));
        }

        #[test]
        fn test_topic_uint_1e18() {
            let pool = DuckDbPool::in_memory().unwrap();
            // 1e18 = 0x0de0b6b3a7640000
            let topic = "0x0000000000000000000000000000000000000000000000000de0b6b3a7640000";
            let result = get_i128(&pool, &format!("SELECT topic_uint_native('{topic}')"));
            assert_eq!(result, Some(1_000_000_000_000_000_000));
        }

        #[test]
        fn test_topic_uint_null_input() {
            let pool = DuckDbPool::in_memory().unwrap();
            assert!(is_null(&pool, "SELECT topic_uint_native(NULL)"));
        }

        #[test]
        fn test_topic_uint_short_input() {
            let pool = DuckDbPool::in_memory().unwrap();
            // Too short - should return null
            assert!(is_null(&pool, "SELECT topic_uint_native('0x1234')"));
        }

        // --------------------------------------------------------------------
        // topic_address_native tests
        // --------------------------------------------------------------------

        #[test]
        fn test_topic_address_zero() {
            let pool = DuckDbPool::in_memory().unwrap();
            let topic = "0x0000000000000000000000000000000000000000000000000000000000000000";
            let result = get_string(&pool, &format!("SELECT topic_address_native('{topic}')"));
            assert_eq!(result, Some("0x0000000000000000000000000000000000000000".to_string()));
        }

        #[test]
        fn test_topic_address_typical() {
            let pool = DuckDbPool::in_memory().unwrap();
            // Address padded to 32 bytes: 0x000...deadbeef...
            let topic = "0x000000000000000000000000deadbeefdeadbeefdeadbeefdeadbeefdeadbeef";
            let result = get_string(&pool, &format!("SELECT topic_address_native('{topic}')"));
            assert_eq!(result, Some("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef".to_string()));
        }

        #[test]
        fn test_topic_address_usdc() {
            let pool = DuckDbPool::in_memory().unwrap();
            // USDC address on Ethereum mainnet
            let topic = "0x000000000000000000000000a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48";
            let result = get_string(&pool, &format!("SELECT topic_address_native('{topic}')"));
            assert_eq!(result, Some("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48".to_string()));
        }

        #[test]
        fn test_topic_address_null_input() {
            let pool = DuckDbPool::in_memory().unwrap();
            assert!(is_null(&pool, "SELECT topic_address_native(NULL)"));
        }

        #[test]
        fn test_topic_address_short_input() {
            let pool = DuckDbPool::in_memory().unwrap();
            assert!(is_null(&pool, "SELECT topic_address_native('0x1234')"));
        }

        // --------------------------------------------------------------------
        // abi_uint_native tests
        // --------------------------------------------------------------------

        #[test]
        fn test_abi_uint_offset_0_zero() {
            let pool = DuckDbPool::in_memory().unwrap();
            // 32 bytes of zeros at offset 0
            let data = "0x0000000000000000000000000000000000000000000000000000000000000000";
            let result = get_i128(&pool, &format!("SELECT abi_uint_native('{data}', 0)"));
            assert_eq!(result, Some(0));
        }

        #[test]
        fn test_abi_uint_offset_0_value() {
            let pool = DuckDbPool::in_memory().unwrap();
            // 100 = 0x64 at offset 0
            let data = "0x0000000000000000000000000000000000000000000000000000000000000064";
            let result = get_i128(&pool, &format!("SELECT abi_uint_native('{data}', 0)"));
            assert_eq!(result, Some(100));
        }

        #[test]
        fn test_abi_uint_offset_32() {
            let pool = DuckDbPool::in_memory().unwrap();
            // Two 32-byte slots: first is 100 (0x64), second is 200 (0xc8)
            let data = "0x000000000000000000000000000000000000000000000000000000000000006400000000000000000000000000000000000000000000000000000000000000c8";
            let result = get_i128(&pool, &format!("SELECT abi_uint_native('{data}', 32)"));
            assert_eq!(result, Some(200)); // 0xc8 = 200
        }

        #[test]
        fn test_abi_uint_1e18() {
            let pool = DuckDbPool::in_memory().unwrap();
            // 1e18 at offset 0
            let data = "0x0000000000000000000000000000000000000000000000000de0b6b3a7640000";
            let result = get_i128(&pool, &format!("SELECT abi_uint_native('{data}', 0)"));
            assert_eq!(result, Some(1_000_000_000_000_000_000));
        }

        #[test]
        fn test_abi_uint_null_data() {
            let pool = DuckDbPool::in_memory().unwrap();
            assert!(is_null(&pool, "SELECT abi_uint_native(NULL, 0)"));
        }

        #[test]
        fn test_abi_uint_null_offset() {
            let pool = DuckDbPool::in_memory().unwrap();
            let data = "0x0000000000000000000000000000000000000000000000000000000000000064";
            assert!(is_null(&pool, &format!("SELECT abi_uint_native('{data}', NULL)")));
        }

        #[test]
        fn test_abi_uint_data_too_short() {
            let pool = DuckDbPool::in_memory().unwrap();
            // Data is too short for the requested offset
            let data = "0x0000000000000000000000000000000000000000000000000000000000000064";
            assert!(is_null(&pool, &format!("SELECT abi_uint_native('{data}', 64)")));
        }

        // --------------------------------------------------------------------
        // abi_address_native tests
        // --------------------------------------------------------------------

        #[test]
        fn test_abi_address_offset_0() {
            let pool = DuckDbPool::in_memory().unwrap();
            // Address at offset 0, padded to 32 bytes
            let data = "0x000000000000000000000000a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48";
            let result = get_string(&pool, &format!("SELECT abi_address_native('{data}', 0)"));
            assert_eq!(result, Some("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48".to_string()));
        }

        #[test]
        fn test_abi_address_offset_32() {
            let pool = DuckDbPool::in_memory().unwrap();
            // Two slots: first is zeros, second has address
            let data = "0x0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000deadbeefdeadbeefdeadbeefdeadbeefdeadbeef";
            let result = get_string(&pool, &format!("SELECT abi_address_native('{data}', 32)"));
            assert_eq!(result, Some("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef".to_string()));
        }

        #[test]
        fn test_abi_address_zero() {
            let pool = DuckDbPool::in_memory().unwrap();
            let data = "0x0000000000000000000000000000000000000000000000000000000000000000";
            let result = get_string(&pool, &format!("SELECT abi_address_native('{data}', 0)"));
            assert_eq!(result, Some("0x0000000000000000000000000000000000000000".to_string()));
        }

        #[test]
        fn test_abi_address_null_data() {
            let pool = DuckDbPool::in_memory().unwrap();
            assert!(is_null(&pool, "SELECT abi_address_native(NULL, 0)"));
        }

        #[test]
        fn test_abi_address_null_offset() {
            let pool = DuckDbPool::in_memory().unwrap();
            let data = "0x000000000000000000000000a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48";
            assert!(is_null(&pool, &format!("SELECT abi_address_native('{data}', NULL)")));
        }

        #[test]
        fn test_abi_address_data_too_short() {
            let pool = DuckDbPool::in_memory().unwrap();
            let data = "0x000000000000000000000000a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48";
            assert!(is_null(&pool, &format!("SELECT abi_address_native('{data}', 64)")));
        }

        // --------------------------------------------------------------------
        // Integration tests with real event data patterns
        // --------------------------------------------------------------------

        #[test]
        fn test_transfer_event_decoding() {
            let pool = DuckDbPool::in_memory().unwrap();
            
            // Simulate a real Transfer event:
            // topic[1] = from address (indexed)
            // topic[2] = to address (indexed)  
            // data = value (uint256)
            
            let from_topic = "0x000000000000000000000000aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
            let to_topic = "0x000000000000000000000000bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
            let value_data = "0x0000000000000000000000000000000000000000000000000de0b6b3a7640000"; // 1e18
            
            // Decode from address
            let from = get_string(&pool, &format!("SELECT topic_address_native('{from_topic}')"));
            assert_eq!(from, Some("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string()));
            
            // Decode to address
            let to = get_string(&pool, &format!("SELECT topic_address_native('{to_topic}')"));
            assert_eq!(to, Some("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string()));
            
            // Decode value
            let value = get_i128(&pool, &format!("SELECT abi_uint_native('{value_data}', 0)"));
            assert_eq!(value, Some(1_000_000_000_000_000_000));
        }

        #[test]
        fn test_approval_event_decoding() {
            let pool = DuckDbPool::in_memory().unwrap();
            
            // Simulate Approval event:
            // topic[1] = owner (indexed)
            // topic[2] = spender (indexed)
            // data = value (uint256)
            
            let owner_topic = "0x0000000000000000000000001111111111111111111111111111111111111111";
            let spender_topic = "0x0000000000000000000000002222222222222222222222222222222222222222";
            // Large but valid approval: 10^30 wei (fits in lower 128 bits)
            // 10^30 = 0xc9f2c9cd04674edea40000000
            let value_data = "0x000000000000000000000000000000000000000c9f2c9cd04674edea40000000";
            
            let owner = get_string(&pool, &format!("SELECT topic_address_native('{owner_topic}')"));
            assert_eq!(owner, Some("0x1111111111111111111111111111111111111111".to_string()));
            
            let spender = get_string(&pool, &format!("SELECT topic_address_native('{spender_topic}')"));
            assert_eq!(spender, Some("0x2222222222222222222222222222222222222222".to_string()));
            
            // 10^30
            let value = get_i128(&pool, &format!("SELECT abi_uint_native('{value_data}', 0)"));
            assert!(value.is_some());
            assert_eq!(value.unwrap(), 1_000_000_000_000_000_000_000_000_000_000_i128);
        }

        #[test]
        fn test_swap_event_decoding() {
            let pool = DuckDbPool::in_memory().unwrap();
            
            // Simulate Uniswap V2 Swap event with multiple data params:
            // data layout: amount0In (32 bytes), amount1In (32 bytes), amount0Out (32 bytes), amount1Out (32 bytes)
            let data = concat!(
                "0x",
                "0000000000000000000000000000000000000000000000000000000000000064", // amount0In = 100
                "0000000000000000000000000000000000000000000000000000000000000000", // amount1In = 0
                "0000000000000000000000000000000000000000000000000000000000000000", // amount0Out = 0
                "00000000000000000000000000000000000000000000000000000000000000c8"  // amount1Out = 200
            );
            
            let amount0_in = get_i128(&pool, &format!("SELECT abi_uint_native('{data}', 0)"));
            assert_eq!(amount0_in, Some(100));
            
            let amount1_in = get_i128(&pool, &format!("SELECT abi_uint_native('{data}', 32)"));
            assert_eq!(amount1_in, Some(0));
            
            let amount0_out = get_i128(&pool, &format!("SELECT abi_uint_native('{data}', 64)"));
            assert_eq!(amount0_out, Some(0));
            
            let amount1_out = get_i128(&pool, &format!("SELECT abi_uint_native('{data}', 96)"));
            assert_eq!(amount1_out, Some(200));
        }

        #[test]
        fn test_batch_processing() {
            let pool = DuckDbPool::in_memory().unwrap();
            
            // Create a table with multiple rows to test batch processing
            futures::executor::block_on(async {
                pool.execute("CREATE TEMP TABLE test_topics (id INT, topic VARCHAR)").await.unwrap();
                
                // Insert multiple topics
                for i in 1..=100 {
                    let topic = format!("0x{:064x}", i);
                    pool.execute(&format!("INSERT INTO test_topics VALUES ({i}, '{topic}')")).await.unwrap();
                }
                
                // Query all at once
                let (_, rows) = pool.query(
                    "SELECT id, topic_uint_native(topic) as value FROM test_topics ORDER BY id",
                ).await.unwrap();
                
                assert_eq!(rows.len(), 100);
                for (i, row) in rows.iter().enumerate() {
                    assert_eq!(row[0], serde_json::json!((i + 1) as i64));
                    assert_eq!(row[1], serde_json::json!((i + 1) as i64));
                }
            });
        }

        #[test]
        fn test_null_handling_in_batch() {
            let pool = DuckDbPool::in_memory().unwrap();
            
            futures::executor::block_on(async {
                pool.execute("CREATE TEMP TABLE test_mixed (id INT, topic VARCHAR)").await.unwrap();
                
                // Mix of valid and NULL values
                pool.execute("INSERT INTO test_mixed VALUES (1, '0x0000000000000000000000000000000000000000000000000000000000000001')").await.unwrap();
                pool.execute("INSERT INTO test_mixed VALUES (2, NULL)").await.unwrap();
                pool.execute("INSERT INTO test_mixed VALUES (3, '0x0000000000000000000000000000000000000000000000000000000000000003')").await.unwrap();
                pool.execute("INSERT INTO test_mixed VALUES (4, NULL)").await.unwrap();
                pool.execute("INSERT INTO test_mixed VALUES (5, '0x0000000000000000000000000000000000000000000000000000000000000005')").await.unwrap();
                
                let (_, rows) = pool.query(
                    "SELECT id, topic_uint_native(topic) as value FROM test_mixed ORDER BY id",
                ).await.unwrap();
                
                assert_eq!(rows.len(), 5);
                assert_eq!(rows[0][1], serde_json::json!(1));
                assert_eq!(rows[1][1], serde_json::Value::Null);
                assert_eq!(rows[2][1], serde_json::json!(3));
                assert_eq!(rows[3][1], serde_json::Value::Null);
                assert_eq!(rows[4][1], serde_json::json!(5));
            });
        }

        // --------------------------------------------------------------------
        // CTE Integration Tests (simulating real OLAP queries)
        // --------------------------------------------------------------------

        #[test]
        fn test_cte_transfer_event_group_by_to() {
            let pool = DuckDbPool::in_memory().unwrap();

            futures::executor::block_on(async {
                // Insert mock Transfer event logs
                // Transfer event selector: 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef
                let transfer_selector = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
                
                // Insert 100 Transfer events with varying from/to addresses and values
                for i in 0..100_u64 {
                    let from_addr = format!("{:040x}", i % 10); // 10 unique senders
                    let to_addr = format!("{:040x}", i % 5);    // 5 unique receivers
                    let value = (i + 1) * 1_000_000_000_u64; // smaller values to avoid overflow
                    
                    let topic0 = transfer_selector;
                    let topic1 = format!("0x000000000000000000000000{from_addr}");
                    let topic2 = format!("0x000000000000000000000000{to_addr}");
                    let data = format!("0x{:064x}", value);
                    
                    pool.execute(
                        &format!(
                            "INSERT INTO logs (block_num, block_timestamp, log_idx, tx_idx, tx_hash, address, selector, topic0, topic1, topic2, topic3, data)
                             VALUES ({i}, '2024-01-01 00:00:00+00', {i}, 0, '0xabc{i:03x}', '0xtoken', '{transfer_selector}', 
                                     '{topic0}', '{topic1}', '{topic2}', NULL, '{data}')"
                        ),
                    ).await.unwrap();
                }

                // Run the problematic CTE query: GROUP BY "to"
                let sql = r#"
                    WITH transfer AS (
                        SELECT block_num, block_timestamp, log_idx, tx_idx, tx_hash, address,
                               topic_address_native(topic1) AS "from",
                               topic_address_native(topic2) AS "to",
                               abi_uint_native(data, 0) AS "value"
                        FROM logs
                        WHERE selector = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
                    )
                    SELECT "to", COUNT(*) as cnt
                    FROM transfer
                    GROUP BY "to"
                    ORDER BY cnt DESC
                "#;

                let (cols, rows) = pool.query(sql).await.unwrap();
                
                assert_eq!(cols, vec!["to", "cnt"]);
                assert_eq!(rows.len(), 5); // 5 unique receivers
                
                // Each receiver should have 20 transfers (100 / 5)
                for row in &rows {
                    assert_eq!(row[1], serde_json::json!(20));
                }
            });
        }

        #[test]
        fn test_cte_transfer_event_sum_value() {
            let pool = DuckDbPool::in_memory().unwrap();

            futures::executor::block_on(async {
                let transfer_selector = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
                
                // Insert 10 Transfer events with known values (use smaller values to stay in i64 range)
                for i in 0..10_u64 {
                    let from_addr = format!("{:040x}", 1);
                    let to_addr = format!("{:040x}", 2);
                    let value = (i + 1) * 1_000_000_000_u64; // 1-10 Gwei (smaller to avoid overflow)
                    
                    let topic0 = transfer_selector;
                    let topic1 = format!("0x000000000000000000000000{from_addr}");
                    let topic2 = format!("0x000000000000000000000000{to_addr}");
                    let data = format!("0x{:064x}", value);
                    
                    pool.execute(
                        &format!(
                            "INSERT INTO logs (block_num, block_timestamp, log_idx, tx_idx, tx_hash, address, selector, topic0, topic1, topic2, topic3, data)
                             VALUES ({i}, '2024-01-01 00:00:00+00', {i}, 0, '0xabc{i:03x}', '0xtoken', '{transfer_selector}', 
                                     '{topic0}', '{topic1}', '{topic2}', NULL, '{data}')"
                        ),
                    ).await.unwrap();
                }

                // Run aggregation with SUM on decoded value
                let sql = r#"
                    WITH transfer AS (
                        SELECT block_num, block_timestamp, log_idx, tx_idx, tx_hash, address,
                               topic_address_native(topic1) AS "from",
                               topic_address_native(topic2) AS "to",
                               abi_uint_native(data, 0) AS "value"
                        FROM logs
                        WHERE selector = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
                    )
                    SELECT "to", SUM("value") as total
                    FROM transfer
                    GROUP BY "to"
                "#;

                let (cols, rows) = pool.query(sql).await.unwrap();
                
                assert_eq!(cols, vec!["to", "total"]);
                assert_eq!(rows.len(), 1);
                
                // Sum of 1+2+...+10 Gwei = 55 Gwei = 55 * 1e9 wei
                let expected_total: i64 = 55 * 1_000_000_000;
                assert_eq!(rows[0][1], serde_json::json!(expected_total));
            });
        }

        #[test]
        fn test_cte_transfer_event_filter_by_from() {
            let pool = DuckDbPool::in_memory().unwrap();

            futures::executor::block_on(async {
                let transfer_selector = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
                
                // Insert transfers from different addresses
                for i in 0..20 {
                    let from_addr = if i < 10 { 
                        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" 
                    } else { 
                        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" 
                    };
                    let to_addr = format!("{:040x}", i);
                    let value = 1_000_000_000_000_000_000_u64; // 1 ETH
                    
                    let topic0 = transfer_selector;
                    let topic1 = format!("0x000000000000000000000000{from_addr}");
                    let topic2 = format!("0x000000000000000000000000{to_addr}");
                    let data = format!("0x{:064x}", value);
                    
                    pool.execute(
                        &format!(
                            "INSERT INTO logs (block_num, block_timestamp, log_idx, tx_idx, tx_hash, address, selector, topic0, topic1, topic2, topic3, data)
                             VALUES ({i}, '2024-01-01 00:00:00+00', {i}, 0, '0xabc{i:03x}', '0xtoken', '{transfer_selector}', 
                                     '{topic0}', '{topic1}', '{topic2}', NULL, '{data}')"
                        ),
                    ).await.unwrap();
                }

                // Filter by specific "from" address
                let sql = r#"
                    WITH transfer AS (
                        SELECT block_num, block_timestamp, log_idx, tx_idx, tx_hash, address,
                               topic_address_native(topic1) AS "from",
                               topic_address_native(topic2) AS "to",
                               abi_uint_native(data, 0) AS "value"
                        FROM logs
                        WHERE selector = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
                    )
                    SELECT COUNT(*) as cnt
                    FROM transfer
                    WHERE "from" = '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
                "#;

                let (_, rows) = pool.query(sql).await.unwrap();
                
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], serde_json::json!(10)); // 10 transfers from address A
            });
        }

        #[test]
        fn test_cte_large_batch_performance() {
            let pool = DuckDbPool::in_memory().unwrap();

            futures::executor::block_on(async {
                let transfer_selector = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
                
                // Insert 1000 Transfer events (stress test)
                let start = std::time::Instant::now();
                for i in 0..1000 {
                    let from_addr = format!("{:040x}", i % 100);
                    let to_addr = format!("{:040x}", i % 50);
                    let value = (i + 1) as u64 * 1_000_000_000_u64;
                    
                    let topic0 = transfer_selector;
                    let topic1 = format!("0x000000000000000000000000{from_addr}");
                    let topic2 = format!("0x000000000000000000000000{to_addr}");
                    let data = format!("0x{:064x}", value);
                    
                    pool.execute(
                        &format!(
                            "INSERT INTO logs (block_num, block_timestamp, log_idx, tx_idx, tx_hash, address, selector, topic0, topic1, topic2, topic3, data)
                             VALUES ({i}, '2024-01-01 00:00:00+00', {i}, 0, '0x{i:064x}', '0xtoken', '{transfer_selector}', 
                                     '{topic0}', '{topic1}', '{topic2}', NULL, '{data}')"
                        ),
                    ).await.unwrap();
                }
                let insert_time = start.elapsed();

                // Run the full CTE query with GROUP BY
                let query_start = std::time::Instant::now();
                let sql = r#"
                    WITH transfer AS (
                        SELECT block_num, block_timestamp, log_idx, tx_idx, tx_hash, address,
                               topic_address_native(topic1) AS "from",
                               topic_address_native(topic2) AS "to",
                               abi_uint_native(data, 0) AS "value"
                        FROM logs
                        WHERE selector = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
                    )
                    SELECT "to", COUNT(*) as cnt, SUM("value") as total
                    FROM transfer
                    GROUP BY "to"
                    ORDER BY total DESC
                    LIMIT 10
                "#;

                let (cols, rows) = pool.query(sql).await.unwrap();
                let query_time = query_start.elapsed();
                
                assert_eq!(cols, vec!["to", "cnt", "total"]);
                assert_eq!(rows.len(), 10);
                
                // Each of 50 receivers gets 20 transfers (1000 / 50)
                for row in &rows {
                    assert_eq!(row[1], serde_json::json!(20));
                }

                // Performance assertion: query should complete in under 1 second
                assert!(query_time.as_millis() < 1000, 
                    "Query took too long: {:?} (insert: {:?})", query_time, insert_time);
            });
        }
    }
}
