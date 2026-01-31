//! TIDX ABI - DuckDB extension for Ethereum ABI decoding
//!
//! Provides vectorized scalar functions for decoding Ethereum event log data:
//! - `abi_address(BLOB)` → VARCHAR - decode 32-byte topic/data to 0x-prefixed address
//! - `abi_address(BLOB, offset)` → VARCHAR - decode address at byte offset in data
//! - `abi_uint(BLOB)` → HUGEINT - decode 32-byte word to uint128 (truncated from uint256)
//! - `abi_uint(BLOB, offset)` → HUGEINT - decode uint at byte offset in data  
//! - `abi_uint256(BLOB)` → VARCHAR - decode 32-byte word to uint256 as hex string
//! - `abi_uint256(BLOB, offset)` → VARCHAR - decode uint256 at offset as hex string
//! - `abi_bool(BLOB)` → BOOLEAN - decode 32-byte word to bool
//! - `abi_bool(BLOB, offset)` → BOOLEAN - decode bool at byte offset in data
//! - `abi_bytes32(BLOB)` → VARCHAR - return 32-byte word as 0x-prefixed hex
//! - `abi_bytes32(BLOB, offset)` → VARCHAR - extract 32 bytes at offset as hex

use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    duckdb_entrypoint_c_api,
    vscalar::{ScalarFunctionSignature, VScalar},
    vtab::arrow::WritableVector,
    Connection, Result,
};
use libduckdb_sys::duckdb_string_t;
use std::error::Error;

// ============================================================================
// ABI Address Decoder
// ============================================================================

/// Decode a 32-byte topic/word to an Ethereum address (last 20 bytes)
struct AbiAddress;

impl VScalar for AbiAddress {
    type State = ();

    unsafe fn invoke(
        _: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> std::result::Result<(), Box<dyn Error>> {
        let len = input.len();
        let mut output_vec = output.flat_vector();

        // Get input blob vector
        let blob_vec = input.flat_vector(0);

        // Check if we have an offset parameter
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

            // Extract last 20 bytes of the 32-byte word (address is right-padded in ABI)
            let word = &blob_data[offset..offset + 32];
            let addr = &word[12..32]; // Last 20 bytes

            let hex = format!("0x{}", hex::encode(addr));
            output_vec.insert(i, hex.as_str());
        }

        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![
            // abi_address(blob) - decode entire blob
            ScalarFunctionSignature::exact(
                vec![LogicalTypeHandle::from(LogicalTypeId::Blob)],
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
            ),
            // abi_address(blob, offset) - decode at offset
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

// ============================================================================
// ABI Uint Decoder (returns HUGEINT, truncates to 128 bits)
// ============================================================================

struct AbiUint;

impl VScalar for AbiUint {
    type State = ();

    unsafe fn invoke(
        _: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> std::result::Result<(), Box<dyn Error>> {
        let len = input.len();
        let mut output_vec = output.flat_vector();

        let blob_vec = input.flat_vector(0);

        let has_offset = input.num_columns() > 1;
        let offset_vec = if has_offset {
            Some(input.flat_vector(1))
        } else {
            None
        };

        // Collect null indices first
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

            // Take last 16 bytes (truncate to 128 bits)
            let word = &blob_data[offset..offset + 32];
            let bytes: [u8; 16] = word[16..32].try_into().unwrap();
            let value = u128::from_be_bytes(bytes);

            // DuckDB HUGEINT is signed, but we store unsigned value
            values.push((i, value as i128));
        }

        // Set nulls
        for idx in null_indices {
            output_vec.set_null(idx);
        }

        // Set values
        let output_data = output_vec.as_mut_slice::<i128>();
        for (i, value) in values {
            output_data[i] = value;
        }

        Ok(())
    }

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

// ============================================================================
// ABI Uint256 Decoder (returns VARCHAR hex string for full precision)
// ============================================================================

struct AbiUint256;

impl VScalar for AbiUint256 {
    type State = ();

    unsafe fn invoke(
        _: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> std::result::Result<(), Box<dyn Error>> {
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
            let hex = format!("0x{}", hex::encode(word));
            output_vec.insert(i, hex.as_str());
        }

        Ok(())
    }

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

// ============================================================================
// ABI Bool Decoder
// ============================================================================

struct AbiBool;

impl VScalar for AbiBool {
    type State = ();

    unsafe fn invoke(
        _: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> std::result::Result<(), Box<dyn Error>> {
        let len = input.len();
        let mut output_vec = output.flat_vector();

        let blob_vec = input.flat_vector(0);

        let has_offset = input.num_columns() > 1;
        let offset_vec = if has_offset {
            Some(input.flat_vector(1))
        } else {
            None
        };

        // Collect null indices first
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

            // Bool is true if last byte is non-zero
            let word = &blob_data[offset..offset + 32];
            values.push((i, word[31] != 0));
        }

        // Set nulls
        for idx in null_indices {
            output_vec.set_null(idx);
        }

        // Set values
        let output_data = output_vec.as_mut_slice::<bool>();
        for (i, value) in values {
            output_data[i] = value;
        }

        Ok(())
    }

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

// ============================================================================
// ABI Bytes32 Decoder (return raw 32 bytes as hex)
// ============================================================================

struct AbiBytes32;

impl VScalar for AbiBytes32 {
    type State = ();

    unsafe fn invoke(
        _: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> std::result::Result<(), Box<dyn Error>> {
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
            let hex = format!("0x{}", hex::encode(word));
            output_vec.insert(i, hex.as_str());
        }

        Ok(())
    }

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

// ============================================================================
// Helpers
// ============================================================================

/// Extract raw bytes from a DuckDB string_t (which is used for BLOB too)
unsafe fn get_blob_data(ptr: &duckdb_string_t) -> &[u8] {
    // DuckDB string_t layout:
    // - If length <= 12: inline data in value.inlined.inlined[4..]
    // - If length > 12: pointer in value.pointer.ptr
    let len = (*ptr).value.inlined.length as usize;
    if len <= 12 {
        // Cast the i8 array to u8
        let inlined = &(*ptr).value.inlined.inlined;
        std::slice::from_raw_parts(inlined.as_ptr() as *const u8, len)
    } else {
        std::slice::from_raw_parts((*ptr).value.pointer.ptr as *const u8, len)
    }
}

// ============================================================================
// Extension Entry Point
// ============================================================================

#[duckdb_entrypoint_c_api()]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    con.register_scalar_function::<AbiAddress>("abi_address")?;
    con.register_scalar_function::<AbiUint>("abi_uint")?;
    con.register_scalar_function::<AbiUint256>("abi_uint256")?;
    con.register_scalar_function::<AbiBool>("abi_bool")?;
    con.register_scalar_function::<AbiBytes32>("abi_bytes32")?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hex_encoding() {
        let addr_bytes = [0u8; 20];
        let hex = format!("0x{}", hex::encode(&addr_bytes));
        assert_eq!(hex, "0x0000000000000000000000000000000000000000");
    }
}
