//! Native DuckDB engine for Parquet queries
//!
//! Provides a clean DuckDB query interface for parquet files without the quirks
//! of pg_duckdb's r['col'] syntax. Uses the same abi_* UDFs as the DuckDB extension.

use alloy::primitives::{Address, FixedBytes, U256};
use anyhow::{anyhow, Result};
use duckdb::core::{DataChunkHandle, LogicalTypeHandle, LogicalTypeId};
use duckdb::vscalar::{ScalarFunctionSignature, VScalar};
use duckdb::vtab::arrow::WritableVector;
use duckdb::Connection;
use libduckdb_sys::duckdb_string_t;
use std::collections::HashMap;
use std::error::Error;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crate::query::{extract_column_references, EventSignature};

/// Native DuckDB engine for parquet queries.
/// Thread-safe via internal mutex (DuckDB connections are not Send).
pub struct DuckDbEngine {
    /// DuckDB connection (protected by mutex for thread safety)
    conn: Mutex<Connection>,
    /// Parquet data directory
    data_dir: PathBuf,
    /// Chain ID for this engine instance
    chain_id: u64,
}

impl DuckDbEngine {
    /// Create a new DuckDB engine for the given chain.
    pub fn new(data_dir: PathBuf, chain_id: u64) -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        
        let engine = Self {
            conn: Mutex::new(conn),
            data_dir,
            chain_id,
        };
        
        // Register UDFs and create views
        engine.initialize()?;
        
        Ok(engine)
    }
    
    /// Initialize the DuckDB connection with UDFs and views.
    fn initialize(&self) -> Result<()> {
        let conn = self.conn.lock().map_err(|e| anyhow!("Lock poisoned: {e}"))?;
        
        // Register ABI decoding UDFs
        register_abi_udfs(&conn)?;
        
        // Create views for each table pointing to parquet files (if files exist)
        let tables = ["logs", "blocks", "txs", "receipts"];
        for table in tables {
            let glob = self.parquet_glob(table);
            let glob_path = format!("{}/{}/{}_*.parquet", self.data_dir.display(), self.chain_id, table);
            
            // Check if any matching parquet files exist using glob
            let has_files = glob::glob(&glob_path)
                .map(|paths| paths.filter_map(Result::ok).next().is_some())
                .unwrap_or(false);
            
            if has_files {
                // Use CREATE OR REPLACE VIEW so we can reinitialize
                conn.execute(
                    &format!(
                        "CREATE OR REPLACE VIEW {table} AS SELECT * FROM read_parquet('{glob}')"
                    ),
                    [],
                )?;
            }
        }
        
        Ok(())
    }
    
    /// Get the parquet glob pattern for a table.
    fn parquet_glob(&self, table: &str) -> String {
        format!(
            "{}/{}/{}_*.parquet",
            self.data_dir.display(),
            self.chain_id,
            table
        )
    }
    
    /// Execute a query and return results.
    pub fn query(&self, sql: &str, signature: Option<&str>) -> Result<QueryResult> {
        let conn = self.conn.lock().map_err(|e| anyhow!("Lock poisoned: {e}"))?;
        
        // Generate CTE if signature provided
        let sql = if let Some(sig_str) = signature {
            let sig = EventSignature::parse(sig_str)?;
            let used_columns = extract_column_references(sql);
            let filter = if used_columns.is_empty() {
                None
            } else {
                Some(&used_columns)
            };
            let cte = sig.to_cte_sql_duckdb_filtered(filter);
            format!("WITH {cte} {sql}")
        } else {
            sql.to_string()
        };
        
        let start = std::time::Instant::now();
        let mut stmt = conn.prepare(&sql)?;
        
        // Execute to get column info
        stmt.execute([])?;
        
        // Get column names from statement
        let columns: Vec<String> = stmt
            .column_names()
            .into_iter()
            .map(String::from)
            .collect();
        let column_count = columns.len();
        
        // Re-prepare and query for rows (execute consumed the result)
        let mut stmt = conn.prepare(&sql)?;
        let mut result_rows = stmt.query([])?;
        
        let mut rows = Vec::new();
        while let Some(row) = result_rows.next()? {
            let mut values = Vec::with_capacity(column_count);
            for i in 0..column_count {
                let value = row_to_json_value(&row, i);
                values.push(value);
            }
            rows.push(values);
        }
        
        let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
        let row_count = rows.len();
        
        Ok(QueryResult {
            columns,
            rows,
            row_count,
            engine: Some("duckdb-native".to_string()),
            query_time_ms: Some(elapsed_ms),
        })
    }
    
    /// Refresh views (call after new parquet files are created).
    pub fn refresh(&self) -> Result<()> {
        self.initialize()
    }
}

/// Query result from DuckDB.
#[derive(Debug, Clone)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub row_count: usize,
    pub engine: Option<String>,
    pub query_time_ms: Option<f64>,
}

/// Convert a DuckDB row value at index to JSON.
fn row_to_json_value(row: &duckdb::Row, idx: usize) -> serde_json::Value {
    // Try each type in order of likelihood
    if let Ok(v) = row.get::<_, i64>(idx) {
        return serde_json::Value::Number(v.into());
    }
    if let Ok(v) = row.get::<_, i128>(idx) {
        // HUGEINT - convert to string to preserve precision
        return serde_json::Value::String(v.to_string());
    }
    if let Ok(v) = row.get::<_, f64>(idx) {
        return serde_json::Number::from_f64(v)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null);
    }
    if let Ok(v) = row.get::<_, String>(idx) {
        return serde_json::Value::String(v);
    }
    if let Ok(v) = row.get::<_, Vec<u8>>(idx) {
        // BLOB - encode as hex
        return serde_json::Value::String(format!("0x{}", hex::encode(v)));
    }
    if let Ok(v) = row.get::<_, bool>(idx) {
        return serde_json::Value::Bool(v);
    }
    // Null or unknown type
    serde_json::Value::Null
}

// ============================================================================
// ABI Decoding UDFs (VScalar trait implementations)
// ============================================================================

/// Helper to extract blob data from a DuckDB string_t pointer
unsafe fn get_blob_data(ptr: &duckdb_string_t) -> &[u8] { unsafe {
    let len = ptr.value.inlined.length as usize;
    if len <= 12 {
        std::slice::from_raw_parts(ptr.value.inlined.inlined.as_ptr().cast::<u8>(), len)
    } else {
        std::slice::from_raw_parts(ptr.value.pointer.ptr.cast::<u8>(), len)
    }
}}

/// abi_address: Decode a 32-byte topic/word to an Ethereum address
struct AbiAddress;

impl VScalar for AbiAddress {
    type State = ();

    unsafe fn invoke(
        _: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> std::result::Result<(), Box<dyn Error>> { unsafe {
        use duckdb::core::Inserter;
        
        let len = input.len();
        let mut output_vec = output.flat_vector();
        let blob_vec = input.flat_vector(0);
        let has_offset = input.num_columns() > 1;
        let offset_vec = if has_offset {
            Some(input.flat_vector(1))
        } else {
            None
        };

        for i in 0..len {
            if blob_vec.row_is_null(i as u64) {
                output_vec.set_null(i);
                continue;
            }

            let blob_ptr = blob_vec.as_slice_with_len::<duckdb_string_t>(len)[i];
            let blob_data = get_blob_data(&blob_ptr);

            let offset = if let Some(ref ov) = offset_vec {
                ov.as_slice_with_len::<i64>(len)[i] as usize
            } else {
                0
            };

            if blob_data.len() < offset + 32 {
                output_vec.set_null(i);
                continue;
            }

            let word = &blob_data[offset..offset + 32];
            let addr = Address::from_slice(&word[12..32]);
            output_vec.insert(i, addr.to_checksum(None).as_str());
        }

        Ok(())
    }}

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![
            ScalarFunctionSignature::exact(
                vec![LogicalTypeHandle::from(LogicalTypeId::Blob)],
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
            ),
            ScalarFunctionSignature::exact(
                vec![
                    LogicalTypeHandle::from(LogicalTypeId::Blob),
                    LogicalTypeHandle::from(LogicalTypeId::Bigint),
                ],
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
            ),
        ]
    }
}

/// abi_uint: Decode a 32-byte word to HUGEINT (128-bit, truncated from 256)
struct AbiUint;

impl VScalar for AbiUint {
    type State = ();

    unsafe fn invoke(
        _: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> std::result::Result<(), Box<dyn Error>> { unsafe {
        let len = input.len();
        let mut output_vec = output.flat_vector();
        let blob_vec = input.flat_vector(0);
        let has_offset = input.num_columns() > 1;
        let offset_vec = if has_offset {
            Some(input.flat_vector(1))
        } else {
            None
        };

        let mut null_indices = Vec::new();
        let mut values: Vec<(usize, i128)> = Vec::with_capacity(len);

        for i in 0..len {
            if blob_vec.row_is_null(i as u64) {
                null_indices.push(i);
                continue;
            }

            let blob_ptr = blob_vec.as_slice_with_len::<duckdb_string_t>(len)[i];
            let blob_data = get_blob_data(&blob_ptr);

            let offset = if let Some(ref ov) = offset_vec {
                ov.as_slice_with_len::<i64>(len)[i] as usize
            } else {
                0
            };

            if blob_data.len() < offset + 32 {
                null_indices.push(i);
                continue;
            }

            let word = &blob_data[offset..offset + 32];
            let bytes: [u8; 16] = word[16..32].try_into().unwrap();
            let value = u128::from_be_bytes(bytes);
            values.push((i, value as i128));
        }

        for idx in null_indices {
            output_vec.set_null(idx);
        }

        let output_data = output_vec.as_mut_slice::<i128>();
        for (i, value) in values {
            output_data[i] = value;
        }

        Ok(())
    }}

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![
            ScalarFunctionSignature::exact(
                vec![LogicalTypeHandle::from(LogicalTypeId::Blob)],
                LogicalTypeHandle::from(LogicalTypeId::Hugeint),
            ),
            ScalarFunctionSignature::exact(
                vec![
                    LogicalTypeHandle::from(LogicalTypeId::Blob),
                    LogicalTypeHandle::from(LogicalTypeId::Bigint),
                ],
                LogicalTypeHandle::from(LogicalTypeId::Hugeint),
            ),
        ]
    }
}

/// abi_uint256: Decode a 32-byte word to VARCHAR (full precision as decimal string)
struct AbiUint256;

impl VScalar for AbiUint256 {
    type State = ();

    unsafe fn invoke(
        _: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> std::result::Result<(), Box<dyn Error>> { unsafe {
        use duckdb::core::Inserter;
        
        let len = input.len();
        let mut output_vec = output.flat_vector();
        let blob_vec = input.flat_vector(0);
        let has_offset = input.num_columns() > 1;
        let offset_vec = if has_offset {
            Some(input.flat_vector(1))
        } else {
            None
        };

        for i in 0..len {
            if blob_vec.row_is_null(i as u64) {
                output_vec.set_null(i);
                continue;
            }

            let blob_ptr = blob_vec.as_slice_with_len::<duckdb_string_t>(len)[i];
            let blob_data = get_blob_data(&blob_ptr);

            let offset = if let Some(ref ov) = offset_vec {
                ov.as_slice_with_len::<i64>(len)[i] as usize
            } else {
                0
            };

            if blob_data.len() < offset + 32 {
                output_vec.set_null(i);
                continue;
            }

            let word: [u8; 32] = blob_data[offset..offset + 32].try_into().unwrap();
            let value = U256::from_be_bytes(word);
            output_vec.insert(i, format!("{value}").as_str());
        }

        Ok(())
    }}

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![
            ScalarFunctionSignature::exact(
                vec![LogicalTypeHandle::from(LogicalTypeId::Blob)],
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
            ),
            ScalarFunctionSignature::exact(
                vec![
                    LogicalTypeHandle::from(LogicalTypeId::Blob),
                    LogicalTypeHandle::from(LogicalTypeId::Bigint),
                ],
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
            ),
        ]
    }
}

/// abi_bool: Decode a 32-byte word to boolean
struct AbiBool;

impl VScalar for AbiBool {
    type State = ();

    unsafe fn invoke(
        _: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> std::result::Result<(), Box<dyn Error>> { unsafe {
        let len = input.len();
        let mut output_vec = output.flat_vector();
        let blob_vec = input.flat_vector(0);
        let has_offset = input.num_columns() > 1;
        let offset_vec = if has_offset {
            Some(input.flat_vector(1))
        } else {
            None
        };

        let mut null_indices = Vec::new();
        let mut values: Vec<(usize, bool)> = Vec::with_capacity(len);

        for i in 0..len {
            if blob_vec.row_is_null(i as u64) {
                null_indices.push(i);
                continue;
            }

            let blob_ptr = blob_vec.as_slice_with_len::<duckdb_string_t>(len)[i];
            let blob_data = get_blob_data(&blob_ptr);

            let offset = if let Some(ref ov) = offset_vec {
                ov.as_slice_with_len::<i64>(len)[i] as usize
            } else {
                0
            };

            if blob_data.len() < offset + 32 {
                null_indices.push(i);
                continue;
            }

            let word = &blob_data[offset..offset + 32];
            let is_valid = word[0..31].iter().all(|&b| b == 0) && (word[31] == 0 || word[31] == 1);
            if is_valid {
                values.push((i, word[31] == 1));
            } else {
                null_indices.push(i);
            }
        }

        for idx in null_indices {
            output_vec.set_null(idx);
        }

        let output_data = output_vec.as_mut_slice::<bool>();
        for (i, value) in values {
            output_data[i] = value;
        }

        Ok(())
    }}

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![
            ScalarFunctionSignature::exact(
                vec![LogicalTypeHandle::from(LogicalTypeId::Blob)],
                LogicalTypeHandle::from(LogicalTypeId::Boolean),
            ),
            ScalarFunctionSignature::exact(
                vec![
                    LogicalTypeHandle::from(LogicalTypeId::Blob),
                    LogicalTypeHandle::from(LogicalTypeId::Bigint),
                ],
                LogicalTypeHandle::from(LogicalTypeId::Boolean),
            ),
        ]
    }
}

/// abi_bytes32: Return 32-byte word as 0x-prefixed hex string
struct AbiBytes32;

impl VScalar for AbiBytes32 {
    type State = ();

    unsafe fn invoke(
        _: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> std::result::Result<(), Box<dyn Error>> { unsafe {
        use duckdb::core::Inserter;
        
        let len = input.len();
        let mut output_vec = output.flat_vector();
        let blob_vec = input.flat_vector(0);
        let has_offset = input.num_columns() > 1;
        let offset_vec = if has_offset {
            Some(input.flat_vector(1))
        } else {
            None
        };

        for i in 0..len {
            if blob_vec.row_is_null(i as u64) {
                output_vec.set_null(i);
                continue;
            }

            let blob_ptr = blob_vec.as_slice_with_len::<duckdb_string_t>(len)[i];
            let blob_data = get_blob_data(&blob_ptr);

            let offset = if let Some(ref ov) = offset_vec {
                ov.as_slice_with_len::<i64>(len)[i] as usize
            } else {
                0
            };

            if blob_data.len() < offset + 32 {
                output_vec.set_null(i);
                continue;
            }

            let word: [u8; 32] = blob_data[offset..offset + 32].try_into().unwrap();
            let bytes = FixedBytes::<32>::from(word);
            output_vec.insert(i, format!("{bytes}").as_str());
        }

        Ok(())
    }}

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![
            ScalarFunctionSignature::exact(
                vec![LogicalTypeHandle::from(LogicalTypeId::Blob)],
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
            ),
            ScalarFunctionSignature::exact(
                vec![
                    LogicalTypeHandle::from(LogicalTypeId::Blob),
                    LogicalTypeHandle::from(LogicalTypeId::Bigint),
                ],
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
            ),
        ]
    }
}

/// Register all ABI decoding UDFs as native Rust functions.
/// These use VScalar trait implementations for direct BLOB parsing.
fn register_abi_udfs(conn: &Connection) -> Result<()> {
    conn.register_scalar_function::<AbiAddress>("abi_address")
        .map_err(|e| anyhow!("Failed to register abi_address: {e}"))?;
    conn.register_scalar_function::<AbiUint>("abi_uint")
        .map_err(|e| anyhow!("Failed to register abi_uint: {e}"))?;
    conn.register_scalar_function::<AbiUint256>("abi_uint256")
        .map_err(|e| anyhow!("Failed to register abi_uint256: {e}"))?;
    conn.register_scalar_function::<AbiBool>("abi_bool")
        .map_err(|e| anyhow!("Failed to register abi_bool: {e}"))?;
    conn.register_scalar_function::<AbiBytes32>("abi_bytes32")
        .map_err(|e| anyhow!("Failed to register abi_bytes32: {e}"))?;
    Ok(())
}

/// Global engine registry for multiple chains.
pub struct DuckDbEngineRegistry {
    engines: Mutex<HashMap<u64, Arc<DuckDbEngine>>>,
    data_dir: PathBuf,
}

impl DuckDbEngineRegistry {
    /// Create a new registry.
    pub fn new(data_dir: PathBuf) -> Self {
        Self {
            engines: Mutex::new(HashMap::new()),
            data_dir,
        }
    }
    
    /// Get or create an engine for the given chain.
    pub fn get_or_create(&self, chain_id: u64) -> Result<Arc<DuckDbEngine>> {
        let mut engines = self.engines.lock().map_err(|e| anyhow!("Lock poisoned: {e}"))?;
        
        if let Some(engine) = engines.get(&chain_id) {
            return Ok(Arc::clone(engine));
        }
        
        let engine = Arc::new(DuckDbEngine::new(self.data_dir.clone(), chain_id)?);
        engines.insert(chain_id, Arc::clone(&engine));
        Ok(engine)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    
    #[test]
    fn test_engine_creation() {
        let temp_dir = TempDir::new().unwrap();
        let engine = DuckDbEngine::new(temp_dir.path().to_path_buf(), 1);
        assert!(engine.is_ok());
    }
    
    #[test]
    fn test_simple_query() {
        let temp_dir = TempDir::new().unwrap();
        let engine = DuckDbEngine::new(temp_dir.path().to_path_buf(), 1).unwrap();
        
        // Simple query that doesn't need parquet files
        let result = engine.query("SELECT 1 as num, 'hello' as msg", None);
        assert!(result.is_ok());
        
        let result = result.unwrap();
        assert_eq!(result.columns, vec!["num", "msg"]);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], serde_json::json!(1));
        assert_eq!(result.rows[0][1], serde_json::json!("hello"));
    }
    
    #[test]
    fn test_abi_address_udf() {
        let temp_dir = TempDir::new().unwrap();
        let engine = DuckDbEngine::new(temp_dir.path().to_path_buf(), 1).unwrap();
        
        // Test abi_address with a known address (use unhex for proper BLOB creation)
        // Input: 32-byte word with address in last 20 bytes
        let result = engine.query(
            "SELECT abi_address(unhex('000000000000000000000000dac17f958d2ee523a2206206994597c13d831ec7')) as addr",
            None
        );
        match &result {
            Ok(r) => println!("Result: {:?}", r.rows),
            Err(e) => println!("Error: {e}"),
        }
        assert!(result.is_ok(), "Query failed: {:?}", result.err());
        
        let result = result.unwrap();
        // Uses EIP-55 checksum encoding (mixed case)
        assert_eq!(result.rows[0][0], serde_json::json!("0xdAC17F958D2ee523a2206206994597C13D831ec7"));
    }
    
    #[test]
    fn test_abi_uint_udf() {
        let temp_dir = TempDir::new().unwrap();
        let engine = DuckDbEngine::new(temp_dir.path().to_path_buf(), 1).unwrap();
        
        // Test abi_uint with value 100000000 (0x5f5e100) - use unhex for proper BLOB
        let result = engine.query(
            "SELECT abi_uint(unhex('0000000000000000000000000000000000000000000000000000000005f5e100')) as value",
            None
        );
        match &result {
            Ok(r) => println!("Result: {:?}", r.rows),
            Err(e) => println!("Error: {e}"),
        }
        assert!(result.is_ok(), "Query failed: {:?}", result.err());
        
        let result = result.unwrap();
        // abi_uint returns HUGEINT (i128) which row_to_json_value handles via i128 -> string
        // But DuckDB's i64 path may hit first if value fits - check actual value
        assert_eq!(result.rows[0][0], serde_json::json!(100000000));
    }
    
    #[test]
    fn test_registry() {
        let temp_dir = TempDir::new().unwrap();
        let registry = DuckDbEngineRegistry::new(temp_dir.path().to_path_buf());
        
        let engine1 = registry.get_or_create(1).unwrap();
        let engine2 = registry.get_or_create(1).unwrap();
        
        // Should return the same engine
        assert!(Arc::ptr_eq(&engine1, &engine2));
        
        // Different chain should return different engine
        let engine3 = registry.get_or_create(42).unwrap();
        assert!(!Arc::ptr_eq(&engine1, &engine3));
    }
    
    #[test]
    fn test_abi_address_with_offset() {
        let temp_dir = TempDir::new().unwrap();
        let engine = DuckDbEngine::new(temp_dir.path().to_path_buf(), 1).unwrap();
        
        // Test with offset - 64 bytes with address at offset 32
        let result = engine.query(
            "SELECT abi_address(unhex('00000000000000000000000000000000000000000000000000000000000000000000000000000000000000001234567890abcdef1234567890abcdef12345678'), 32) as addr",
            None
        );
        assert!(result.is_ok(), "Query failed: {:?}", result.err());
        
        let result = result.unwrap();
        // Address should be extracted from bytes 44-64 (offset 32 + 12 leading zeros + 20 bytes)
        assert_eq!(result.rows[0][0], serde_json::json!("0x1234567890AbcdEF1234567890aBcdef12345678"));
    }
    
    #[test]
    fn test_abi_bool_udf() {
        let temp_dir = TempDir::new().unwrap();
        let engine = DuckDbEngine::new(temp_dir.path().to_path_buf(), 1).unwrap();
        
        // Test true
        let result = engine.query(
            "SELECT abi_bool(unhex('0000000000000000000000000000000000000000000000000000000000000001')) as val",
            None
        ).unwrap();
        assert_eq!(result.rows[0][0], serde_json::json!(true));
        
        // Test false
        let result = engine.query(
            "SELECT abi_bool(unhex('0000000000000000000000000000000000000000000000000000000000000000')) as val",
            None
        ).unwrap();
        assert_eq!(result.rows[0][0], serde_json::json!(false));
    }
    
    #[test]
    fn test_abi_bytes32_udf() {
        let temp_dir = TempDir::new().unwrap();
        let engine = DuckDbEngine::new(temp_dir.path().to_path_buf(), 1).unwrap();
        
        let result = engine.query(
            "SELECT abi_bytes32(unhex('ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef')) as topic",
            None
        ).unwrap();
        
        // Should return the full bytes32 as hex
        assert_eq!(result.rows[0][0], serde_json::json!("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"));
    }
}
