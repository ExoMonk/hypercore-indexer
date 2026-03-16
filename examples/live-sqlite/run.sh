#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BINARY="$PROJECT_ROOT/target/release/hypercore-indexer"

echo "=== hypercore-indexer: Live SQLite Example ==="
echo ""

# Check AWS credentials
if [ -z "${AWS_ACCESS_KEY_ID:-}" ] || [ -z "${AWS_SECRET_ACCESS_KEY:-}" ]; then
    echo "AWS credentials not set. The S3 bucket is requester-pays."
    echo ""
    echo "  export AWS_ACCESS_KEY_ID=..."
    echo "  export AWS_SECRET_ACCESS_KEY=..."
    echo "  export AWS_REGION=ap-northeast-1"
    echo ""
    exit 1
fi

# Build if needed
if [ ! -f "$BINARY" ]; then
    echo "Building hypercore-indexer (release)..."
    (cd "$PROJECT_ROOT" && cargo build --release)
    echo ""
fi

echo "Step 1: Backfill blocks 5,000,000 - 5,001,000 into SQLite"
echo "  Config: $SCRIPT_DIR/hypercore.toml"
echo "  Database: $SCRIPT_DIR/hypercore.db"
echo ""

cd "$SCRIPT_DIR"
"$BINARY" --config hypercore.toml backfill --from 5000000 --to 5001000

echo ""
echo "=== Backfill complete ==="
echo ""
echo "Data is in: $SCRIPT_DIR/hypercore.db"
echo ""
echo "Quick check (requires sqlite3):"
echo "  sqlite3 hypercore.db 'SELECT COUNT(*) AS blocks FROM blocks;'"
echo "  sqlite3 hypercore.db 'SELECT COUNT(*) AS txs FROM transactions;'"
echo "  sqlite3 hypercore.db 'SELECT COUNT(*) AS transfers FROM system_transfers;'"
echo ""
echo "To start live indexing (follows chain tip):"
echo "  $BINARY --config hypercore.toml live"
echo ""
