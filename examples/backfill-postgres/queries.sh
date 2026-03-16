#!/usr/bin/env bash
set -euo pipefail

COMPOSE="docker compose"

echo "=== hypercore-indexer: PostgreSQL Query Examples ==="
echo ""

echo "--- Row Counts ---"
$COMPOSE exec -T postgres psql -U postgres -d hypercore -c \
  "SELECT
    (SELECT COUNT(*) FROM blocks) AS blocks,
    (SELECT COUNT(*) FROM transactions) AS transactions,
    (SELECT COUNT(*) FROM system_transfers) AS system_transfers,
    (SELECT COUNT(*) FROM event_logs) AS event_logs;"

echo ""
echo "--- Latest 5 Blocks ---"
$COMPOSE exec -T postgres psql -U postgres -d hypercore -c \
  "SELECT block_number, encode(block_hash, 'hex') AS hash, tx_count, to_timestamp(timestamp) AS time
   FROM blocks
   ORDER BY block_number DESC
   LIMIT 5;"

echo ""
echo "--- System Transfers ---"
$COMPOSE exec -T postgres psql -U postgres -d hypercore -c \
  "SELECT block_number, asset_type,
          encode(system_address, 'hex') AS system_addr,
          encode(recipient, 'hex') AS recipient,
          amount_wei::text AS amount
   FROM system_transfers
   ORDER BY block_number
   LIMIT 10;"

echo ""
echo "--- Transactions by Type ---"
$COMPOSE exec -T postgres psql -U postgres -d hypercore -c \
  "SELECT tx_type, COUNT(*) AS count
   FROM transactions
   GROUP BY tx_type
   ORDER BY count DESC;"

echo ""
echo "--- Cursor ---"
$COMPOSE exec -T postgres psql -U postgres -d hypercore -c \
  "SELECT * FROM indexer_cursor;"

echo ""
echo "=== Done ==="
