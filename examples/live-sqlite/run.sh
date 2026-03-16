#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BINARY="$PROJECT_ROOT/target/release/hypercore-indexer"

echo "=== hypercore-indexer: Live SQLite Example ==="
echo ""

# Check AWS credentials (env vars or profile)
if ! aws sts get-caller-identity &>/dev/null 2>&1; then
    if [ -z "${AWS_ACCESS_KEY_ID:-}" ]; then
        echo "AWS credentials not configured. The S3 bucket is requester-pays."
        echo ""
        echo "  aws configure           # profile-based"
        echo "  # or: export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=..."
        echo ""
        exit 1
    fi
fi

# Build if needed
if [ ! -f "$BINARY" ]; then
    echo "Building hypercore-indexer (release)..."
    (cd "$PROJECT_ROOT" && cargo build --release)
    echo ""
fi

cd "$SCRIPT_DIR"

# That's it. The indexer handles everything:
# - First run (no cursor): discovers chain tip, starts indexing from there
# - Subsequent runs: resumes from last indexed block, catches up, follows tip
"$BINARY" --config hypercore.toml live
