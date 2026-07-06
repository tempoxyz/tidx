-- Build parameter sample tables for the OLTP suite from real synced data.
-- Run via bench.sh AFTER measure.sh (these tables add bytes to the DB).
DROP SCHEMA IF EXISTS bench CASCADE;
CREATE SCHEMA bench;

CREATE TABLE bench.tx_hashes AS
SELECT row_number() OVER () AS id, hash, block_num
FROM (
    SELECT hash, block_num
    FROM txs TABLESAMPLE SYSTEM (1)
    LIMIT 20000
) s;
ALTER TABLE bench.tx_hashes ADD PRIMARY KEY (id);

CREATE TABLE bench.addresses AS
SELECT row_number() OVER () AS id, address AS addr
FROM (
    SELECT DISTINCT address
    FROM logs TABLESAMPLE SYSTEM (1)
    LIMIT 5000
) s;
ALTER TABLE bench.addresses ADD PRIMARY KEY (id);

ANALYZE bench.tx_hashes;
ANALYZE bench.addresses;
