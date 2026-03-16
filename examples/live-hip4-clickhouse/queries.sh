#!/usr/bin/env bash
set -euo pipefail

CH="docker compose exec -T clickhouse clickhouse-client --format Pretty"

echo "=== hypercore-indexer: HIP4 ClickHouse Queries ==="
echo ""

echo "--- Row Counts ---"
$CH --query "
SELECT
    (SELECT count() FROM blocks) AS blocks,
    (SELECT count() FROM transactions) AS transactions,
    (SELECT count() FROM system_transfers) AS system_transfers,
    (SELECT count() FROM event_logs) AS event_logs,
    (SELECT count() FROM hip4_deposits) AS hip4_deposits,
    (SELECT count() FROM hip4_claims) AS hip4_claims
"

echo ""
echo "--- Latest Blocks ---"
$CH --query "
SELECT block_number, tx_count, system_tx_count, gas_used
FROM blocks
ORDER BY block_number DESC
LIMIT 5
"

echo ""
echo "--- HIP4 Deposits ---"
$CH --query "
SELECT block_number, contest_id, side_id, depositor, amount_wei
FROM hip4_deposits
ORDER BY block_number DESC
LIMIT 10
"

echo ""
echo "--- HIP4 Markets (from API) ---"
$CH --query "
SELECT outcome_id, name, description
FROM hip4_markets FINAL
ORDER BY outcome_id
LIMIT 10
"

echo ""
echo "--- HIP4 Prices (latest per coin) ---"
$CH --query "
SELECT coin, mid_price, timestamp
FROM hip4_prices FINAL
ORDER BY timestamp DESC
LIMIT 14
"

echo ""
echo "--- Deposits per Contest ---"
$CH --query "
SELECT contest_id, count() AS deposits, sum(toDecimal128(amount_wei, 0)) AS total_wei
FROM hip4_deposits
GROUP BY contest_id
ORDER BY contest_id
"

echo ""
echo "--- Cursor ---"
$CH --query "SELECT * FROM indexer_cursor FINAL"

echo ""
echo "=== Done ==="
