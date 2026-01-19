# AK47 Agent Instructions

## Project Overview
High-throughput Tempo blockchain indexer in Rust, inspired by golden-axe.

## Commands

### Build & Check
```bash
cargo check          # Fast type checking
cargo build          # Debug build
cargo build --release # Release build
```

### Test
```bash
# Start test infrastructure (TimescaleDB + Tempo node)
docker compose -f docker-compose.test.yml up -d

# Wait for services to be healthy
docker compose -f docker-compose.test.yml ps

# Run tests
cargo test

# Run specific test
cargo test smoke_test
```

### Generate Load (for benchmarking)
```bash
# Use tempo-bench to generate millions of transactions
docker run --rm --network host ghcr.io/tempoxyz/tempo-bench:latest \
  run-max-tps \
  --duration 60 \
  --tps 5000 \
  --accounts 10000 \
  --target-urls http://localhost:8545 \
  --faucet
```

### Run
```bash
# Start continuous head-following sync (requires DB)
cargo run -- up --rpc https://rpc.testnet.tempo.xyz --db postgres://ak47:ak47@localhost:5432/ak47

# Check sync status (shows forward progress, backfill progress, gaps)
cargo run -- status --db postgres://ak47:ak47@localhost:5432/ak47

# Backfill from current position toward genesis (resumes if interrupted)
cargo run -- sync --rpc https://rpc.testnet.tempo.xyz --db postgres://ak47:ak47@localhost:5432/ak47 backfill --to 0

# Backfill from specific block
cargo run -- sync --rpc https://rpc.testnet.tempo.xyz --db postgres://ak47:ak47@localhost:5432/ak47 backfill --from 10000 --to 0

# Show sync status via sync command
cargo run -- sync --rpc https://rpc.testnet.tempo.xyz --db postgres://ak47:ak47@localhost:5432/ak47 status
```

### Benchmarks
```bash
cargo bench
```

## Architecture

- `src/cli/` - CLI commands (up, status)
- `src/sync/` - Sync engine, RPC fetcher, decoder, writer
- `src/db/` - Database pool and schema management
- `src/types.rs` - Core data types (BlockRow, TxRow, LogRow)
- `migrations/` - SQL migrations
- `tests/common/` - Test infrastructure (real Tempo node, TestDb)

## Tempo Networks

| Network | Chain ID | RPC |
|---------|----------|-----|
| Presto (mainnet) | 4217 | https://rpc.presto.tempo.xyz |
| Andantino (testnet) | 42429 | https://rpc.testnet.tempo.xyz |
| Moderato | 42431 | https://rpc.moderato.tempo.xyz |

## Code Style
- Follow existing patterns in the codebase
- Use `anyhow::Result` for error handling
- Use `tracing` for logging
- Prefer `alloy` types for Ethereum primitives
- **Never use mocks** - always prefer real implementations over mocks

## Git Workflow
- **Commit incrementally** - never batch multiple features into one commit
- Commit after each logical change (new feature, optimization, refactor, test)
- Use conventional commit messages: `feat:`, `fix:`, `refactor:`, `test:`, `docs:`, `perf:`
- Each commit should be independently reviewable and revertable
