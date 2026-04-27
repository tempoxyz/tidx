.PHONY: help up down logs seed build check test bench bench-gen bench-open clean compose-up compose-down compose-logs \
	pgroll-init pgroll-bootstrap pgroll-baseline pgroll-migrate pgroll-start pgroll-complete pgroll-rollback pgroll-status pgroll-validate

.DEFAULT_GOAL := help

# Docker compose file selection
# Use LOCALNET=1 for local localnet (PostgreSQL + Tempo node)
# Default uses production compose (external RPCs)
ifeq ($(LOCALNET),1)
COMPOSE := docker compose -f docker/local/docker-compose.yml
else
COMPOSE := docker compose -f docker/prod/docker-compose.override.yml
endif

# Forked testnet compose at repo root (Anvil fork + PostgreSQL + ClickHouse + tidx)
FORK_COMPOSE := docker compose -f compose.yml

# Default seed parameters
DURATION ?= 30
TPS ?= 100

# pgroll
PGROLL_BIN ?= pgroll
PGROLL_BASELINE_VERSION ?= 20260415_prod_baseline
PGROLL_MIGRATIONS_DIR ?= db/pgroll
PGROLL_BOOTSTRAP_OUT_DIR ?= /tmp/pgroll-baseline

# ============================================================================
# Environment
# ============================================================================

# Start all services (PostgreSQL + Tempo + Indexer)
up:
	@$(COMPOSE) up -d
ifeq ($(LOCALNET),1)
	@echo "Waiting for PostgreSQL..."
	@until $(COMPOSE) exec -T postgres pg_isready -U tidx -d tidx > /dev/null 2>&1; do sleep 1; done
	@echo "✓ Ready."
endif

# Stop all services
down:
	@$(COMPOSE) down

# Tail indexer logs
logs:
	@$(COMPOSE) logs -f tidx

# Start forked testnet stack from compose.yml
compose-up:
	@$(FORK_COMPOSE) up -d --build

# Stop forked testnet stack from compose.yml
compose-down:
	@$(FORK_COMPOSE) down

# Tail forked testnet stack logs
compose-logs:
	@$(FORK_COMPOSE) logs -f

# ============================================================================
# Data
# ============================================================================

# Seed chain with transactions (uses dev mnemonic for pre-funded accounts)
seed:
	@echo "Seeding chain with $(TPS) TPS for $(DURATION) seconds..."
	@docker run --rm --network host ghcr.io/tempoxyz/tempo-bench:latest \
		run-max-tps --duration $(DURATION) --tps $(TPS) --accounts 10 \
		--target-urls http://localhost:8545 --disable-2d-nonces \
		--mnemonic "test test test test test test test test test test test junk"

# Heavy seed: ~1M+ txs with max variance (TIP-20, ERC-20, swaps, multicalls)
# Takes ~10 mins at 2000 TPS for 600 seconds
HEAVY_DURATION ?= 600
HEAVY_TPS ?= 2000
HEAVY_ACCOUNTS ?= 1000

seed-heavy:
	@echo "Heavy seeding: $(HEAVY_TPS) TPS for $(HEAVY_DURATION)s (~$$(($(HEAVY_TPS) * $(HEAVY_DURATION))) txs)"
	@docker run --rm --network host ghcr.io/tempoxyz/tempo-bench:latest \
		run-max-tps \
		--duration $(HEAVY_DURATION) \
		--tps $(HEAVY_TPS) \
		--accounts $(HEAVY_ACCOUNTS) \
		--target-urls http://localhost:8545 \
		--disable-2d-nonces \
		--mnemonic "test test test test test test test test test test test junk" \
		--tip20-weight 3 \
		--erc20-weight 2 \
		--swap-weight 2

# ============================================================================
# pgroll
# ============================================================================

pgroll-init:
	@test -n "$(POSTGRES_URL)" || (echo "POSTGRES_URL is required" && exit 1)
	@$(PGROLL_BIN) init --postgres-url '$(POSTGRES_URL)'

pgroll-bootstrap:
	@test -n "$(POSTGRES_URL)" || (echo "POSTGRES_URL is required" && exit 1)
	@mkdir -p $(PGROLL_BOOTSTRAP_OUT_DIR)
	@$(PGROLL_BIN) init --postgres-url '$(POSTGRES_URL)'
	@$(PGROLL_BIN) baseline $(PGROLL_BASELINE_VERSION) $(PGROLL_BOOTSTRAP_OUT_DIR) --yes --json --postgres-url '$(POSTGRES_URL)'

pgroll-baseline:
	@test -n "$(POSTGRES_URL)" || (echo "POSTGRES_URL is required" && exit 1)
	@mkdir -p $(PGROLL_MIGRATIONS_DIR)
	@$(PGROLL_BIN) baseline $(PGROLL_BASELINE_VERSION) $(PGROLL_MIGRATIONS_DIR) --yes --json --postgres-url '$(POSTGRES_URL)'

pgroll-migrate:
	@test -n "$(POSTGRES_URL)" || (echo "POSTGRES_URL is required" && exit 1)
	@$(PGROLL_BIN) migrate $(PGROLL_MIGRATIONS_DIR) --complete --postgres-url '$(POSTGRES_URL)'

pgroll-start:
	@test -n "$(POSTGRES_URL)" || (echo "POSTGRES_URL is required" && exit 1)
	@test -n "$(MIGRATION)" || (echo "MIGRATION is required" && exit 1)
	@$(PGROLL_BIN) --postgres-url '$(POSTGRES_URL)' start $(MIGRATION)

pgroll-complete:
	@test -n "$(POSTGRES_URL)" || (echo "POSTGRES_URL is required" && exit 1)
	@$(PGROLL_BIN) --postgres-url '$(POSTGRES_URL)' complete

pgroll-rollback:
	@test -n "$(POSTGRES_URL)" || (echo "POSTGRES_URL is required" && exit 1)
	@$(PGROLL_BIN) --postgres-url '$(POSTGRES_URL)' rollback

pgroll-status:
	@test -n "$(POSTGRES_URL)" || (echo "POSTGRES_URL is required" && exit 1)
	@$(PGROLL_BIN) status --postgres-url '$(POSTGRES_URL)'

pgroll-validate:
	@test -n "$(MIGRATION)" || (echo "MIGRATION is required" && exit 1)
	@$(PGROLL_BIN) validate $(MIGRATION)

# ============================================================================
# Build & Test
# ============================================================================

# Build Docker image
build:
	@$(COMPOSE) build tidx

# Run clippy lints
check:
	@cargo clippy --all-targets

# Localnet compose for tests
LOCALNET_COMPOSE := docker compose -f docker/local/docker-compose.yml

# Run tests (sequential execution due to shared DB)
test:
	@$(LOCALNET_COMPOSE) up -d postgres tempo clickhouse
	@echo "Waiting for PostgreSQL..."
	@until $(LOCALNET_COMPOSE) exec -T postgres pg_isready -U tidx -d postgres > /dev/null 2>&1; do sleep 1; done
	@$(LOCALNET_COMPOSE) exec -T postgres psql -U tidx -d postgres -c "CREATE DATABASE tidx" > /dev/null 2>&1 || true
	@echo "Waiting for ClickHouse..."
	@until curl -sf http://localhost:8123/ping > /dev/null 2>&1; do sleep 1; done
	@echo "Waiting for Tempo..."
	@until curl -s http://localhost:8545 -X POST -H "Content-Type: application/json" \
		-d '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' | grep -q result; do sleep 1; done
	@echo "Running tests..."
	@DATABASE_URL=postgres://tidx:tidx@localhost:5433/tidx RPC_URL=http://localhost:8545 \
		cargo test -- --test-threads=1 --nocapture

# Benchmark parameters
BENCH_TXS ?= 5000000
BENCH_ARTIFACT ?= .bench_seed.dump

# Check if benchmark data exists, restore from artifact or generate fresh
define check_bench_data
	@TX_COUNT=$$($(LOCALNET_COMPOSE) exec -T postgres psql -U tidx -d tidx -tAc "SELECT COUNT(*) FROM txs" 2>/dev/null || echo "0"); \
	if [ "$$TX_COUNT" -ge 1000000 ]; then \
		echo "Using existing data ($$TX_COUNT txs)"; \
	elif [ -f "$(BENCH_ARTIFACT)" ]; then \
		echo "Restoring from cached artifact..."; \
		$(MAKE) _bench_restore; \
	else \
		echo "No cached data found. Run 'make bench-gen' first, or seeding now..."; \
		$(MAKE) _bench_seed; \
	fi
endef

# Generate benchmark seed artifact (run once, reuse many times)
bench-gen:
	@echo "=== Generating benchmark seed artifact ==="
	@START_TIME=$$(date +%s); \
	$(LOCALNET_COMPOSE) up -d postgres; \
	echo "Waiting for PostgreSQL..."; \
	until $(LOCALNET_COMPOSE) exec -T postgres pg_isready -U tidx -d postgres > /dev/null 2>&1; do sleep 1; done; \
	$(LOCALNET_COMPOSE) exec -T postgres psql -U tidx -d postgres -c "DROP DATABASE IF EXISTS tidx_test WITH (FORCE)" > /dev/null 2>&1 || true; \
	$(LOCALNET_COMPOSE) exec -T postgres psql -U tidx -d postgres -c "CREATE DATABASE tidx_test" > /dev/null; \
	echo "Seeding $(BENCH_TXS) synthetic transactions..."; \
	SEED_TXS=$(BENCH_TXS) DATABASE_URL=postgres://tidx:tidx@localhost:5433/tidx_test \
		cargo test --release --test seed_bench -- --ignored --nocapture; \
	echo "Dumping to artifact..."; \
	$(LOCALNET_COMPOSE) exec -T postgres pg_dump -U tidx -Fc tidx_test > $(BENCH_ARTIFACT); \
	END_TIME=$$(date +%s); \
	ELAPSED=$$((END_TIME - START_TIME)); \
	MINS=$$((ELAPSED / 60)); \
	SECS=$$((ELAPSED % 60)); \
	echo ""; \
	echo "=== Completed in $${MINS}m $${SECS}s ==="; \
	echo "Artifact: $(BENCH_ARTIFACT) ($$(du -h $(BENCH_ARTIFACT) | cut -f1))"; \
	$(LOCALNET_COMPOSE) exec -T postgres psql -U tidx -d tidx_test -c "SELECT COUNT(*) as blocks FROM blocks; SELECT COUNT(*) as txs FROM txs; SELECT COUNT(*) as logs FROM logs;"

# Internal: restore from artifact (fast)
_bench_restore:
	@$(LOCALNET_COMPOSE) up -d postgres
	@until $(LOCALNET_COMPOSE) exec -T postgres pg_isready -U tidx -d postgres > /dev/null 2>&1; do sleep 1; done
	@$(LOCALNET_COMPOSE) exec -T postgres psql -U tidx -d postgres -c "DROP DATABASE IF EXISTS tidx WITH (FORCE)" > /dev/null 2>&1 || true
	@$(LOCALNET_COMPOSE) exec -T postgres psql -U tidx -d postgres -c "CREATE DATABASE tidx" > /dev/null
	@cat $(BENCH_ARTIFACT) | $(LOCALNET_COMPOSE) exec -T postgres pg_restore -U tidx -d tidx --no-owner --no-acl 2>/dev/null || true
	@TX_COUNT=$$($(LOCALNET_COMPOSE) exec -T postgres psql -U tidx -d tidx -tAc "SELECT COUNT(*) FROM txs"); \
	echo "Restored $$TX_COUNT txs from artifact"

# Internal: seed fresh (slow, used when no artifact exists)
_bench_seed:
	@$(LOCALNET_COMPOSE) up -d postgres
	@echo "Waiting for PostgreSQL..."
	@until $(LOCALNET_COMPOSE) exec -T postgres pg_isready -U tidx -d postgres > /dev/null 2>&1; do sleep 1; done
	@$(LOCALNET_COMPOSE) exec -T postgres psql -U tidx -d postgres -c "DROP DATABASE IF EXISTS tidx WITH (FORCE)" > /dev/null 2>&1 || true
	@$(LOCALNET_COMPOSE) exec -T postgres psql -U tidx -d postgres -c "CREATE DATABASE tidx" > /dev/null
	@echo "Seeding $(BENCH_TXS) synthetic transactions..."
	@SEED_TXS=$(BENCH_TXS) DATABASE_URL=postgres://tidx:tidx@localhost:5433/tidx \
		cargo test --release --test seed_bench -- --ignored --nocapture
	@$(LOCALNET_COMPOSE) exec -T postgres psql -U tidx -d tidx -c "SELECT COUNT(*) as blocks FROM blocks; SELECT COUNT(*) as txs FROM txs; SELECT COUNT(*) as logs FROM logs;"

# Run benchmarks (seeds 2M txs if data doesn't exist)
bench:
	@$(LOCALNET_COMPOSE) up -d postgres tempo
	@sleep 2
	$(call check_bench_data)
	@echo "=== Running Query Benchmarks ==="
	@DATABASE_URL=postgres://tidx:tidx@localhost:5433/tidx cargo bench --bench query_bench
	@echo "Report: target/criterion/report/index.html"

# Run benchmarks and open report
bench-open: bench
	@open target/criterion/report/index.html 2>/dev/null || xdg-open target/criterion/report/index.html 2>/dev/null || echo "Open target/criterion/report/index.html"

# Compare tidx vs golden-axe sync performance
# Both index from the same live tempo chain
# Requires: golden-axe repo at ~/git/golden-axe
GOLDEN_AXE_DIR ?= $(HOME)/git/golden-axe
COMPARE_TXS ?= 1000000
COMPARE_TPS ?= 3000

bench-vs-golden-axe:
	@echo "============================================"
	@echo "=== tidx vs golden-axe Sync Comparison ==="
	@echo "============================================"
	@echo ""
	@echo "=== Step 1: Starting Tempo node ==="
	@$(LOCALNET_COMPOSE) up -d tempo postgres
	@until curl -sf http://localhost:8545 > /dev/null 2>&1; do sleep 1; done
	@echo "Tempo node ready"
	@echo ""
	@echo "=== Step 2: Seeding chain with $(COMPARE_TXS) txs ==="
	@docker run --rm --network host ghcr.io/tempoxyz/tempo-bench:latest \
		run-max-tps \
		--duration $$(($(COMPARE_TXS) / $(COMPARE_TPS))) \
		--tps $(COMPARE_TPS) \
		--accounts 1000 \
		--target-urls http://localhost:8545 \
		--disable-2d-nonces \
		--mnemonic "test test test test test test test test test test test junk"
	@CHAIN_HEAD=$$(curl -s http://localhost:8545 -X POST -H "Content-Type: application/json" \
		-d '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' | jq -r '.result' | xargs printf "%d\n"); \
	echo "Chain seeded to block $$CHAIN_HEAD"
	@echo ""
	@echo "=== Step 3: Reset databases ==="
	@$(LOCALNET_COMPOSE) exec -T postgres psql -U tidx -c "DROP DATABASE IF EXISTS tidx" > /dev/null 2>&1 || true
	@$(LOCALNET_COMPOSE) exec -T postgres psql -U tidx -c "CREATE DATABASE tidx" > /dev/null
	@dropdb --if-exists ga_bench 2>/dev/null || true
	@createdb ga_bench 2>/dev/null || true
	@psql ga_bench -f $(GOLDEN_AXE_DIR)/be/src/sql/schema.sql > /dev/null 2>&1
	@psql ga_bench -f $(GOLDEN_AXE_DIR)/be/src/sql/indexes.sql > /dev/null 2>&1
	@psql ga_bench -c "DELETE FROM config WHERE chain = 31337" > /dev/null 2>&1 || true
	@psql ga_bench -c "INSERT INTO config (chain, url, enabled, batch_size, concurrency, start_block) \
		VALUES (31337, 'http://localhost:8545', true, 100, 4, 0)" > /dev/null 2>&1
	@echo "Databases reset"
	@echo ""
	@echo "=== Step 4: Benchmark tidx sync ==="
	@cargo build --release
	@echo "Starting tidx..."
	@START=$$(date +%s); \
	timeout 600 ./target/release/tidx up \
		--rpc http://localhost:8545 \
		--db postgres://tidx:tidx@localhost:5433/tidx 2>&1 | head -100 || true; \
	END=$$(date +%s); \
	TIDX_TIME=$$((END - START)); \
	TIDX_TXS=$$($(LOCALNET_COMPOSE) exec -T postgres psql -U tidx -d tidx -tAc "SELECT COUNT(*) FROM txs" 2>/dev/null || echo "0"); \
	TIDX_BLOCKS=$$($(LOCALNET_COMPOSE) exec -T postgres psql -U tidx -d tidx -tAc "SELECT COUNT(*) FROM blocks" 2>/dev/null || echo "0"); \
	echo "tidx: $$TIDX_TXS txs, $$TIDX_BLOCKS blocks in $${TIDX_TIME}s"; \
	echo "tidx: $$(echo "scale=0; $$TIDX_TXS / $$TIDX_TIME" | bc) txs/sec"; \
	echo "$$TIDX_TIME $$TIDX_TXS $$TIDX_BLOCKS" > /tmp/tidx_result.txt
	@echo ""
	@echo "=== Step 5: Benchmark golden-axe sync ==="
	@cd $(GOLDEN_AXE_DIR) && cargo build --release -p be
	@echo "Starting golden-axe..."
	@START=$$(date +%s); \
	cd $(GOLDEN_AXE_DIR) && timeout 600 DATABASE_URL=postgres://localhost/ga_bench \
		./target/release/be 2>&1 | head -100 || true; \
	END=$$(date +%s); \
	GA_TIME=$$((END - START)); \
	GA_TXS=$$(psql ga_bench -tAc "SELECT COUNT(*) FROM txs" 2>/dev/null || echo "0"); \
	GA_BLOCKS=$$(psql ga_bench -tAc "SELECT COUNT(*) FROM blocks" 2>/dev/null || echo "0"); \
	echo "golden-axe: $$GA_TXS txs, $$GA_BLOCKS blocks in $${GA_TIME}s"; \
	echo "golden-axe: $$(echo "scale=0; $$GA_TXS / $$GA_TIME" | bc) txs/sec"; \
	echo "$$GA_TIME $$GA_TXS $$GA_BLOCKS" > /tmp/ga_result.txt
	@echo ""
	@echo "============================================"
	@echo "=== Results ==="
	@echo "============================================"
	@TIDX=$$(cat /tmp/tidx_result.txt); GA=$$(cat /tmp/ga_result.txt); \
	TIDX_TIME=$$(echo $$TIDX | cut -d' ' -f1); TIDX_TXS=$$(echo $$TIDX | cut -d' ' -f2); \
	GA_TIME=$$(echo $$GA | cut -d' ' -f1); GA_TXS=$$(echo $$GA | cut -d' ' -f2); \
	echo "tidx:       $$TIDX_TXS txs in $${TIDX_TIME}s ($$(echo "scale=0; $$TIDX_TXS / $$TIDX_TIME" | bc) txs/sec)"; \
	echo "golden-axe: $$GA_TXS txs in $${GA_TIME}s ($$(echo "scale=0; $$GA_TXS / $$GA_TIME" | bc) txs/sec)"; \
	if [ $$TIDX_TIME -lt $$GA_TIME ]; then \
		SPEEDUP=$$(echo "scale=1; $$GA_TIME / $$TIDX_TIME" | bc); \
		echo ""; \
		echo "tidx is $${SPEEDUP}x faster"; \
	else \
		SPEEDUP=$$(echo "scale=1; $$TIDX_TIME / $$GA_TIME" | bc); \
		echo ""; \
		echo "golden-axe is $${SPEEDUP}x faster"; \
	fi

# Clean everything
clean:
	@$(COMPOSE) down -v
	@cargo clean

# ============================================================================
# Help
# ============================================================================

help:
	@echo "tidx Development"
	@echo ""
	@echo "  LOCALNET=1 make <cmd>  Use localnet compose (index local Tempo node)"
	@echo ""
	@echo "  make up                Start services (use LOCALNET=1 for localnet)"
	@echo "  make down              Stop all services"
	@echo "  make logs              Tail indexer logs"
	@echo "  make build             Build Docker image"
	@echo "  make compose-up        Start repo-root compose stack"
	@echo "  make compose-down      Stop repo-root compose stack"
	@echo "  make compose-logs      Tail repo-root compose logs"
	@echo ""
	@echo "  make test              Run all tests (PostgreSQL + ClickHouse)"
	@echo "  make check             Run clippy lints"
	@echo ""
	@echo "  make seed              Generate transactions (DURATION=30 TPS=100)"
	@echo "  make seed-heavy        Generate ~1M+ txs with max variance"
	@echo ""
	@echo "  make bench             Run benchmarks (restores from artifact)"
	@echo "  make bench-gen         Generate 5M tx seed artifact (run once)"
	@echo ""
	@echo "  make clean             Stop services and clean"
