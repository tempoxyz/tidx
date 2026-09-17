-- Delete stale ReceivePolicyGuard (TIP-1028) rows that the old holder-delta
-- SELECT credited before the guard was excluded. No-op on fresh deployments;
-- `holder` is stored lowercase.
ALTER TABLE token_holder_deltas
    DELETE WHERE holder = '0xb10c000000000000000000000000000000000000'
    SETTINGS mutations_sync = 1
