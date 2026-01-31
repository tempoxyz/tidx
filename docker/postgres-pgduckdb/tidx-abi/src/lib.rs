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

use alloy_primitives::{Address, FixedBytes, U256};
use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    vscalar::{ScalarFunctionSignature, VScalar},
    vtab::arrow::WritableVector,
    Connection, Result,
};
use duckdb_loadable_macros::duckdb_entrypoint_c_api;
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

            // Extract last 20 bytes of the 32-byte word (address is left-padded with zeros in ABI)
            let word = &blob_data[offset..offset + 32];
            let addr = Address::from_slice(&word[12..32]);
            output_vec.insert(i, addr.to_checksum(None).as_str());
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

            let word: [u8; 32] = blob_data[offset..offset + 32].try_into().unwrap();
            let value = U256::from_be_bytes(word);
            output_vec.insert(i, format!("{value}").as_str());
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

            let word: [u8; 32] = blob_data[offset..offset + 32].try_into().unwrap();
            let bytes = FixedBytes::<32>::from(word);
            output_vec.insert(i, format!("{bytes}").as_str());
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
fn tidx_abi_init(con: Connection) -> Result<(), Box<dyn Error>> {
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
    use alloy_primitives::hex;

    // ========================================================================
    // Address decoding tests
    // ========================================================================

    #[test]
    fn test_decode_address_from_word() {
        let mut word = [0u8; 32];
        word[12..32].copy_from_slice(&hex::decode("dAC17F958D2ee523a2206206994597C13D831ec7").unwrap());

        let addr = Address::from_slice(&word[12..32]);
        assert_eq!(
            addr.to_checksum(None),
            "0xdAC17F958D2ee523a2206206994597C13D831ec7"
        );
    }

    #[test]
    fn test_decode_address_zero() {
        let word = [0u8; 32];
        let addr = Address::from_slice(&word[12..32]);
        assert_eq!(
            addr.to_checksum(None),
            "0x0000000000000000000000000000000000000000"
        );
    }

    #[test]
    fn test_decode_address_all_ones() {
        let mut word = [0u8; 32];
        word[12..32].fill(0xff);
        let addr = Address::from_slice(&word[12..32]);
        assert_eq!(
            addr.to_checksum(None),
            "0xFFfFfFffFFfffFFfFFfFFFFFffFFFffffFfFFFfF"
        );
    }

    #[test]
    fn test_decode_address_with_padding() {
        let mut word = [0u8; 32];
        word[0..12].fill(0xff);
        word[12..32].copy_from_slice(&hex::decode("1234567890123456789012345678901234567890").unwrap());

        let addr = Address::from_slice(&word[12..32]);
        assert_eq!(
            addr.to_checksum(None),
            "0x1234567890123456789012345678901234567890"
        );
    }

    // ========================================================================
    // Uint decoding tests
    // ========================================================================

    #[test]
    fn test_decode_uint_zero() {
        let word = [0u8; 32];
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&word[16..32]);
        let value = u128::from_be_bytes(bytes);
        assert_eq!(value, 0);
    }

    #[test]
    fn test_decode_uint_one() {
        let mut word = [0u8; 32];
        word[31] = 1;
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&word[16..32]);
        let value = u128::from_be_bytes(bytes);
        assert_eq!(value, 1);
    }

    #[test]
    fn test_decode_uint_max_i128() {
        let mut word = [0u8; 32];
        word[16] = 0x7f;
        word[17..32].fill(0xff);
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&word[16..32]);
        let value = u128::from_be_bytes(bytes);
        assert_eq!(value, i128::MAX as u128);
    }

    #[test]
    fn test_decode_uint_overflow_returns_null() {
        let mut word = [0u8; 32];
        word[16] = 0x80;
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&word[16..32]);
        let value = u128::from_be_bytes(bytes);
        assert!(value > i128::MAX as u128);
    }

    #[test]
    fn test_decode_uint_large_value() {
        let mut word = [0u8; 32];
        word[24..32].copy_from_slice(&1_000_000_000_000_000_000u64.to_be_bytes());
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&word[16..32]);
        let value = u128::from_be_bytes(bytes);
        assert_eq!(value, 1_000_000_000_000_000_000u128);
    }

    #[test]
    fn test_decode_uint_truncates_high_bits() {
        let mut word = [0u8; 32];
        word[0..16].fill(0xff);
        word[16..32].fill(0x00);
        word[31] = 0x42;
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&word[16..32]);
        let value = u128::from_be_bytes(bytes);
        assert_eq!(value, 0x42);
    }

    // ========================================================================
    // Uint256 decoding tests
    // ========================================================================

    #[test]
    fn test_decode_uint256_zero() {
        let word = [0u8; 32];
        let value = U256::from_be_bytes(word);
        assert_eq!(format!("{value}"), "0");
    }

    #[test]
    fn test_decode_uint256_one() {
        let mut word = [0u8; 32];
        word[31] = 1;
        let value = U256::from_be_bytes(word);
        assert_eq!(format!("{value}"), "1");
    }

    #[test]
    fn test_decode_uint256_max() {
        let word = [0xff; 32];
        let value = U256::from_be_bytes(word);
        assert_eq!(
            format!("{value}"),
            "115792089237316195423570985008687907853269984665640564039457584007913129639935"
        );
    }

    #[test]
    fn test_decode_uint256_large_value() {
        let mut word = [0u8; 32];
        word[16..32].fill(0xff);
        let value = U256::from_be_bytes(word);
        assert_eq!(
            format!("{value}"),
            "340282366920938463463374607431768211455"
        );
    }

    // ========================================================================
    // Bool decoding tests
    // ========================================================================

    #[test]
    fn test_decode_bool_false() {
        let word = [0u8; 32];
        let is_valid = word[0..31].iter().all(|&b| b == 0) && (word[31] == 0 || word[31] == 1);
        assert!(is_valid);
        assert!(word[31] == 0);
    }

    #[test]
    fn test_decode_bool_true() {
        let mut word = [0u8; 32];
        word[31] = 1;
        let is_valid = word[0..31].iter().all(|&b| b == 0) && (word[31] == 0 || word[31] == 1);
        assert!(is_valid);
        assert!(word[31] == 1);
    }

    #[test]
    fn test_decode_bool_invalid_value() {
        let mut word = [0u8; 32];
        word[31] = 2;
        let is_valid = word[0..31].iter().all(|&b| b == 0) && (word[31] == 0 || word[31] == 1);
        assert!(!is_valid);
    }

    #[test]
    fn test_decode_bool_invalid_padding() {
        let mut word = [0u8; 32];
        word[0] = 1;
        word[31] = 1;
        let is_valid = word[0..31].iter().all(|&b| b == 0) && (word[31] == 0 || word[31] == 1);
        assert!(!is_valid);
    }

    #[test]
    fn test_decode_bool_nonzero_high_bytes() {
        let mut word = [0u8; 32];
        word[15] = 0xff;
        word[31] = 1;
        let is_valid = word[0..31].iter().all(|&b| b == 0) && (word[31] == 0 || word[31] == 1);
        assert!(!is_valid);
    }

    // ========================================================================
    // Bytes32 decoding tests
    // ========================================================================

    #[test]
    fn test_decode_bytes32_zero() {
        let word = [0u8; 32];
        let bytes = FixedBytes::<32>::from(word);
        assert_eq!(
            format!("{bytes}"),
            "0x0000000000000000000000000000000000000000000000000000000000000000"
        );
    }

    #[test]
    fn test_decode_bytes32_all_ones() {
        let word = [0xff; 32];
        let bytes = FixedBytes::<32>::from(word);
        assert_eq!(
            format!("{bytes}"),
            "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
        );
    }

    #[test]
    fn test_decode_bytes32_pattern() {
        let mut word = [0u8; 32];
        word[0] = 0xde;
        word[1] = 0xad;
        word[2] = 0xbe;
        word[3] = 0xef;
        let bytes = FixedBytes::<32>::from(word);
        assert_eq!(
            format!("{bytes}"),
            "0xdeadbeef00000000000000000000000000000000000000000000000000000000"
        );
    }

    // ========================================================================
    // Real-world event data tests
    // ========================================================================

    #[test]
    fn test_erc20_transfer_topic_decoding() {
        let from_topic = hex::decode("000000000000000000000000dac17f958d2ee523a2206206994597c13d831ec7").unwrap();
        let to_topic = hex::decode("000000000000000000000000a9d1e08c7793af67e9d92fe308d5697fb81d3e43").unwrap();

        let from_addr = Address::from_slice(&from_topic[12..32]);
        let to_addr = Address::from_slice(&to_topic[12..32]);

        assert_eq!(
            from_addr.to_checksum(None),
            "0xdAC17F958D2ee523a2206206994597C13D831ec7"
        );
        assert_eq!(
            to_addr.to_checksum(None),
            "0xA9D1e08C7793af67e9d92fe308d5697FB81d3E43"
        );
    }

    #[test]
    fn test_erc20_transfer_data_decoding() {
        let data = hex::decode("0000000000000000000000000000000000000000000000000000000005f5e100").unwrap();

        let value = U256::from_be_slice(&data);
        assert_eq!(format!("{value}"), "100000000");
    }

    #[test]
    fn test_uniswap_swap_data_decoding() {
        let data = hex::decode(concat!(
            "0000000000000000000000000000000000000000000000000de0b6b3a7640000", // amount0In
            "0000000000000000000000000000000000000000000000000000000000000000", // amount1In
            "0000000000000000000000000000000000000000000000000000000000000000", // amount0Out
            "00000000000000000000000000000000000000000000000006f05b59d3b20000"  // amount1Out
        ))
        .unwrap();

        let amount0_in = U256::from_be_slice(&data[0..32]);
        let amount1_in = U256::from_be_slice(&data[32..64]);
        let amount0_out = U256::from_be_slice(&data[64..96]);
        let amount1_out = U256::from_be_slice(&data[96..128]);

        assert_eq!(format!("{amount0_in}"), "1000000000000000000");
        assert_eq!(format!("{amount1_in}"), "0");
        assert_eq!(format!("{amount0_out}"), "0");
        assert_eq!(format!("{amount1_out}"), "500000000000000000");
    }

    // ========================================================================
    // Edge case tests
    // ========================================================================

    #[test]
    fn test_negative_offset_handling() {
        let neg: i64 = -1;
        let result = usize::try_from(neg);
        assert!(result.is_err());
    }
}
