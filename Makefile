COMPOSE_DEV  := docker compose -f deployments/hypercore-indexer-dev/docker-compose.yml
COMPOSE_TEST := docker compose -f deployments/hypercore-indexer-test/docker-compose.yml

.PHONY: dev serve check build test test-e2e test-all query clean clean-test

dev:           ## Start dev PostgreSQL (port 5432)
	@./scripts/dev.sh

serve:         ## Run the indexer
	cargo run

check:         ## Fast compile check
	cargo check

build:         ## Release build
	cargo build --release

test:          ## Unit + decode + pipeline tests (no Docker)
	cargo test

test-e2e:      ## Storage E2E: start test containers, run all storage tests, stop
	@echo "Starting test containers..."
	@$(COMPOSE_TEST) up -d --wait
	@echo "Running SQLite tests..."
	@cargo test --test storage_sqlite
	@echo "Running PostgreSQL tests..."
	@DATABASE_URL=postgres://postgres:postgres@localhost:5433/hypercore_test \
		cargo test --test storage_postgres -- --ignored --test-threads=1
	@echo "Running ClickHouse tests..."
	@CLICKHOUSE_URL=http://localhost:8124 \
		cargo test --test storage_clickhouse -- --ignored --test-threads=1
	@echo "Stopping test containers..."
	@$(COMPOSE_TEST) down
	@echo "All E2E storage tests passed."

test-all:      ## Run everything: unit + decode + pipeline + storage E2E
	@$(MAKE) test
	@$(MAKE) test-e2e

query:         ## Query dev PostgreSQL (row counts, latest blocks, system transfers)
	@./scripts/queries.sh

clean:         ## Tear down dev containers + volumes
	$(COMPOSE_DEV) down -v

clean-test:    ## Tear down test containers + volumes
	$(COMPOSE_TEST) down -v
