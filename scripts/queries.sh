#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
COMPOSE="docker compose -f $ROOT/deployments/hypercore-indexer-dev/docker-compose.yml"
PSQL="$COMPOSE exec -T postgres psql -U postgres -d hypercore"

echo "=== PostgreSQL Health ==="
$PSQL -c "SELECT 1;" -t -q && echo "OK" || echo "FAILED"

echo ""
echo "=== Row Counts ==="
$PSQL -c "
SELECT 'blocks' AS table_name, COUNT(*) AS rows FROM blocks
UNION ALL
SELECT 'transactions', COUNT(*) FROM transactions
UNION ALL
SELECT 'system_transfers', COUNT(*) FROM system_transfers
UNION ALL
SELECT 'event_logs', COUNT(*) FROM event_logs
UNION ALL
SELECT 'indexer_cursor', COUNT(*) FROM indexer_cursor
UNION ALL
SELECT 'hip4_merkle_claims', COUNT(*) FROM hip4_merkle_claims
UNION ALL
SELECT 'hip4_finalizations', COUNT(*) FROM hip4_finalizations
UNION ALL
SELECT 'hip4_markets', COUNT(*) FROM hip4_markets
UNION ALL
SELECT 'hip4_prices', COUNT(*) FROM hip4_prices
UNION ALL
SELECT 'hip4_market_snapshots', COUNT(*) FROM hip4_market_snapshots
ORDER BY table_name;
"

echo ""
echo "=== Latest Blocks ==="
$PSQL -c "
SELECT block_number, to_timestamp(timestamp) AS time, tx_count, system_tx_count, gas_used
FROM blocks ORDER BY block_number DESC LIMIT 5;
"

echo ""
echo "=== Cursor ==="
$PSQL -c "SELECT * FROM indexer_cursor;"

echo ""
echo "=== Recent System Transfers ==="
$PSQL -c "
SELECT s.block_number, s.asset_type,
       '0x' || encode(s.recipient, 'hex') AS recipient,
       s.amount_wei::text AS amount,
       '0x' || encode(s.official_hash, 'hex') AS official_hash
FROM system_transfers s
ORDER BY s.block_number DESC LIMIT 10;
"
