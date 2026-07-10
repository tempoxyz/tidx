-- ABI decode helpers over tidx's '0x'-prefixed hex string columns.
-- Mirrors db/functions.sql (PostgreSQL bytea equivalents).
--
-- SQL lambda UDFs are substituted at parse time: zero runtime overhead,
-- and constant arguments still fold for index pruning.
--
-- Applied with CREATE OR REPLACE on every startup (see ch_sink.rs), so
-- definitions here are the single source of truth. Functions are
-- server-global, shared across databases. Note: CREATE FUNCTION is not
-- replicated by the Replicated database engine; multi-server deployments
-- must run tidx schema setup against each server (or configure
-- user_defined_zookeeper_path).

-- 32-byte word ('0x' + 64 hex chars) -> '0x' + 20-byte address
CREATE OR REPLACE FUNCTION abi_address AS (w) -> concat('0x', lower(substring(w, 27)));

-- 32-byte word -> UInt256 (big-endian)
CREATE OR REPLACE FUNCTION abi_uint AS (w) -> reinterpretAsUInt256(reverse(unhex(substring(w, 3))));

-- 32-byte word -> Int256 (big-endian two's complement)
CREATE OR REPLACE FUNCTION abi_int AS (w) -> reinterpretAsInt256(reverse(unhex(substring(w, 3))));

-- 32-byte word -> Bool (last byte non-zero)
CREATE OR REPLACE FUNCTION abi_bool AS (w) -> unhex(substring(w, 65, 2)) != unhex('00');

-- 32-byte word -> '0x' + 64 lowercase hex chars
CREATE OR REPLACE FUNCTION abi_bytes32 AS (w) -> concat('0x', lower(substring(w, 3)));

-- word at byte offset o of '0x'-hex data -> '0x' + 64 hex chars
CREATE OR REPLACE FUNCTION abi_word AS (d, o) -> concat('0x', substring(d, 3 + o * 2, 64));

-- word at byte offset o -> UInt64 (last 8 bytes; ABI offsets/lengths fit)
CREATE OR REPLACE FUNCTION abi_word_uint AS (d, o) -> reinterpretAsUInt64(reverse(unhex(substring(d, 3 + o * 2 + 48, 16))));

-- dynamic bytes at head slot o: offset word -> length word -> '0x'-hex payload
CREATE OR REPLACE FUNCTION abi_bytes AS (d, o) -> concat('0x', lower(substring(d, 3 + (abi_word_uint(d, o) + 32) * 2, abi_word_uint(d, abi_word_uint(d, o)) * 2)));

-- dynamic string at head slot o: same slice, unhexed to UTF-8
CREATE OR REPLACE FUNCTION abi_string AS (d, o) -> unhex(substring(d, 3 + (abi_word_uint(d, o) + 32) * 2, abi_word_uint(d, abi_word_uint(d, o)) * 2));
