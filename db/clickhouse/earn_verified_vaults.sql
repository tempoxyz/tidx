-- One verified snapshot per chain database. The registry sync atomically
-- replaces the whole set. Expiry is enforced at read time, not by TTL merges.
CREATE TABLE IF NOT EXISTS earn_verified_vaults (
    vault          String,
    share_token    String,
    asset          String,
    deployed_block Int64,
    deployed_at    DateTime('UTC'),
    valid_until    DateTime('UTC')
) ENGINE = MergeTree()
ORDER BY vault
SETTINGS default_compression_codec = 'ZSTD(1)'
