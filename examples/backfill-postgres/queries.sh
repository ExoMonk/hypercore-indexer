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
  "SELECT number, hash, tx_count, timestamp
   FROM blocks
   ORDER BY number DESC
   LIMIT 5;"

echo ""
echo "--- System Transfers (first 10) ---"
$COMPOSE exec -T postgres psql -U postgres -d hypercore -c \
  "SELECT block_number, system_address, recipient, amount, asset_type
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
echo "--- Find a Transaction by Hash ---"
echo "(replace the hash with one from your data)"
$COMPOSE exec -T postgres psql -U postgres -d hypercore -c \
  "SELECT tx_hash, block_number, tx_type, \"to\", value, success
   FROM transactions
   LIMIT 1;"

echo ""
echo "=== Done ==="
