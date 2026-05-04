# [pgroll](https://pgroll.com) migrations

> [!INFO]
> install `pgroll`
> https://github.com/xataio/pgroll/releases

This directory is the source of truth for PostgreSQL pgroll migrations.

Production flow:

1. One-time bootstrap for an existing production database:
   - `pgroll init`
   - `pgroll baseline <baseline-version> <temporary-output-dir>`
   - use a temporary output directory for the bootstrap command; the canonical baseline file is already committed in this directory
2. Normal deploys after bootstrap:
   - `pgroll init`
   - `pgroll migrate db/pgroll --complete`

Rules:

- Keep migrations in filename order.
- Do not auto-edit old migration files after they have been applied in production.
- If database state and pgroll history drift, fail closed and reconcile explicitly.
- Baseline is a one-time operator step, not a normal deploy step.

Current planned production baseline version:

- `20260415_prod_baseline`

Operator commands from the repo root:

- One-time bootstrap for an existing DB:
  - `POSTGRES_URL=... make pgroll-bootstrap`
- Normal migration flow after bootstrap:
  - `POSTGRES_URL=... make pgroll-migrate`

Production Docker commands from `docker/prod`:

- One-time bootstrap for existing production DBs:
  - `PGROLL_POSTGRES_URLS="postgres://.../tidx_moderato?sslmode=disable postgres://.../tidx_mainnet?sslmode=disable" docker compose --profile migrations run --rm pgroll-bootstrap`
- Normal migration flow before starting/updating `tidx`:
  - `PGROLL_POSTGRES_URLS="postgres://.../tidx_moderato?sslmode=disable postgres://.../tidx_mainnet?sslmode=disable" docker compose --profile migrations run --rm pgroll-migrate`
  - `pgroll-migrate` verifies the final pgroll version and exits non-zero if migrations do not reach `20260430_add_blocks_consensus_proposer` (for example, if bootstrap was skipped on a non-empty database).

The production image published by GitHub Actions includes both `tidx pgroll` and the `/db/pgroll` migration files.

Post-baseline migrations in this directory:

- `20260416_add_is_virtual_forward_column.json`
- `20260417_add_logs_virtual_forward_indexes.json`
- `20260430_add_blocks_consensus_proposer.json`
