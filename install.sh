#!/usr/bin/env bash
set -euo pipefail

echo "=== hypercore-indexer install ==="
echo ""

# Check for Rust/cargo
if ! command -v cargo &>/dev/null; then
    echo "Rust/cargo not found."
    echo ""
    echo "Install Rust first:"
    echo "  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh"
    echo ""
    echo "Then re-run this script."
    exit 1
fi

echo "Found cargo: $(cargo --version)"
echo ""

# Build from source
echo "Building hypercore-indexer (release mode)..."
cargo build --release

BINARY="./target/release/hypercore-indexer"

if [ ! -f "$BINARY" ]; then
    echo "Build failed — binary not found at $BINARY"
    exit 1
fi

echo ""
echo "Build complete: $BINARY"
echo ""

# Generate starter config if it doesn't exist
if [ -f "hypercore.toml" ]; then
    echo "hypercore.toml already exists — skipping init."
else
    echo "Generating starter config..."
    "$BINARY" init
fi

echo ""
echo "=== Next steps ==="
echo ""
echo "  1. Set AWS credentials (S3 bucket is requester-pays):"
echo "       export AWS_ACCESS_KEY_ID=..."
echo "       export AWS_SECRET_ACCESS_KEY=..."
echo "       export AWS_REGION=ap-northeast-1"
echo ""
echo "  2. Backfill some blocks:"
echo "       $BINARY backfill --from 5000000 --to 5001000"
echo ""
echo "  3. Check the data:"
echo "       make query"
echo ""
echo "  4. Follow the chain tip:"
echo "       $BINARY live"
echo ""
echo "  Tip: Add target/release to your PATH, or run:"
echo "       cargo install --path ."
echo ""
