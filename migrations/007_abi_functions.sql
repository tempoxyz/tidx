-- ABI helper functions for decoding log data
-- These functions decode ABI-encoded data from topics and data fields

-- Decode uint256 from bytea (big-endian)
CREATE OR REPLACE FUNCTION abi_uint(input BYTEA) RETURNS NUMERIC AS $$
DECLARE n NUMERIC := 0;
BEGIN
  FOR i IN 1..length(input) LOOP
    n := n * 256 + get_byte(input, i - 1);
  END LOOP;
  RETURN n;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

-- Decode int256 from bytea (two's complement big-endian)
CREATE OR REPLACE FUNCTION abi_int(input BYTEA) RETURNS NUMERIC AS $$
DECLARE
  n NUMERIC := 0;
  is_negative BOOLEAN;
BEGIN
  is_negative := get_byte(input, 0) >= 128;
  FOR i IN 1..length(input) LOOP
    n := n * 256 + get_byte(input, i - 1);
  END LOOP;
  IF is_negative THEN
    n := n - power(2::NUMERIC, length(input) * 8);
  END IF;
  RETURN n;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

-- Decode address from 32-byte padded bytea (last 20 bytes)
CREATE OR REPLACE FUNCTION abi_address(input BYTEA) RETURNS BYTEA AS $$
BEGIN
  RETURN substring(input FROM 13 FOR 20);
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

-- Decode bool from bytea (non-zero = true)
CREATE OR REPLACE FUNCTION abi_bool(input BYTEA) RETURNS BOOLEAN AS $$
BEGIN
  RETURN get_byte(input, length(input) - 1) != 0;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

-- Decode dynamic bytes from bytea (length prefix in first 32 bytes)
CREATE OR REPLACE FUNCTION abi_bytes(input BYTEA, offset_bytes INT DEFAULT 0) RETURNS BYTEA AS $$
DECLARE 
  data_offset INT;
  data_length INT;
BEGIN
  -- Read offset pointer (points to where the actual data starts)
  data_offset := abi_uint(substring(input FROM offset_bytes + 1 FOR 32))::INT;
  -- Read length at the offset position
  data_length := abi_uint(substring(input FROM data_offset + 1 FOR 32))::INT;
  -- Return the actual bytes
  RETURN substring(input FROM data_offset + 33 FOR data_length);
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

-- Decode string from bytea (same as bytes, but convert to UTF8)
CREATE OR REPLACE FUNCTION abi_string(input BYTEA, offset_bytes INT DEFAULT 0) RETURNS TEXT AS $$
BEGIN
  RETURN convert_from(abi_bytes(input, offset_bytes), 'UTF8');
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

-- Format address as checksummed hex string (lowercase for now)
CREATE OR REPLACE FUNCTION format_address(input BYTEA) RETURNS TEXT AS $$
BEGIN
  RETURN '0x' || encode(input, 'hex');
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

-- Format uint256 as decimal string
CREATE OR REPLACE FUNCTION format_uint(input BYTEA) RETURNS TEXT AS $$
BEGIN
  RETURN abi_uint(input)::TEXT;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;
