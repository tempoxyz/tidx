-- Hourly Earn vault share price, written by the tidx Earn price sampler.
--
-- One row per vault per completed UTC hour since deployment:
-- assets_per_1e18_shares = vault.previewRedeem(1e18) read with eth_call at
-- block_num, the last block whose timestamp falls inside `hour`. The sampler
-- only writes values it read; failed or reverted calls are retried later and
-- never filled in. ReplacingMergeTree keyed on (vault, hour) keeps rewrites
-- idempotent; read it with FINAL. Rows are pruned with their block on reorg,
-- and the sampler writes them again.
CREATE TABLE IF NOT EXISTS earn_vault_price_hours (
    vault                  String,
    hour                   DateTime('UTC'),
    block_num              Int64,
    assets_per_1e18_shares UInt256,
    sampled_at             DateTime('UTC') DEFAULT now()
) ENGINE = ReplacingMergeTree(sampled_at)
ORDER BY (vault, hour)
SETTINGS default_compression_codec = 'ZSTD(1)'
