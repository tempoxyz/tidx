# ClickHouse access-path rollout

Schema migration adds index metadata without rebuilding existing parts. New parts receive the indexes automatically. Backfill existing history one partition and one table at a time after other maintenance mutations finish.

Run these queries during an off-peak window against the chain database. Start with one older partition, confirm disk headroom and query plans, then continue sequentially.

```sql
SELECT
    table,
    partition,
    formatReadableSize(sum(bytes_on_disk)) AS bytes
FROM system.parts
WHERE active
  AND database = currentDatabase()
  AND table IN ('blocks', 'txs', 'receipts')
GROUP BY table, partition
ORDER BY partition, table;

ALTER TABLE blocks
    MATERIALIZE INDEX idx_timestamp IN PARTITION '202601'
    SETTINGS mutations_sync = 1;

ALTER TABLE txs
    MATERIALIZE INDEX idx_from_nonce_key_nonce IN PARTITION '202601'
    SETTINGS mutations_sync = 1;

ALTER TABLE receipts
    MATERIALIZE INDEX idx_to IN PARTITION '202601'
    SETTINGS mutations_sync = 1;
```

After each statement, confirm that no failed or pending mutation remains before starting the next one:

```sql
SELECT
    table,
    mutation_id,
    command,
    is_done,
    parts_to_do,
    latest_fail_reason
FROM system.mutations
WHERE database = currentDatabase()
ORDER BY create_time DESC;
```

If a mutation fails, stop the rollout and correct the cause. Cancel only the exact failed mutation before retrying its partition:

```sql
KILL MUTATION
WHERE database = currentDatabase()
  AND mutation_id = 'mutation_123.txt'
SYNC;
```

Partition-scoped rebuilds bound the mutation queue. A synchronous reorg delete can still wait behind the current partition rebuild, so monitor chain progress throughout the rollout.
