#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

COMPOSE="docker compose -f $ROOT/deployments/hypercore-indexer-dev/docker-compose.yml"

# Start services
$COMPOSE up -d

# Wait for PostgreSQL
echo -n "Waiting for PostgreSQL..."
until $COMPOSE exec -T postgres pg_isready -U postgres &>/dev/null; do
  echo -n "."
  sleep 1
done
echo " ready"

# Print endpoint table
echo ""
echo "═══════════════════════════════════════"
echo "  Service        │ URL"
echo "─────────────────┼─────────────────────"
echo "  PostgreSQL     │ postgres://postgres:postgres@localhost:5432/hypercore"
echo "═══════════════════════════════════════"
echo ""
echo "  export DATABASE_URL=postgres://postgres:postgres@localhost:5432/hypercore"
