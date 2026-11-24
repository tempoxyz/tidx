---
tidx: patch
---

Fixed wide numeric `/query` results losing precision or turning into `null`. ClickHouse requests now always set `output_format_json_quote_64bit_integers=1`, so integers stay exact on ClickHouse 25.8 and later, where 128- and 256-bit values were rounded through floating point. On every route, `(U)Int64` values are exact JSON numbers and wider integers (ClickHouse `(U)Int128`/`(U)Int256`, PostgreSQL `NUMERIC` such as decoded uint256 event values) are exact decimal strings, instead of rounded numbers or `null` past 28 digits. Native ClickHouse 128- and 256-bit integer columns therefore change from JSON numbers to strings.
