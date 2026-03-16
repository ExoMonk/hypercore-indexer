# Live SQLite Example

Index Hyperliquid blocks into SQLite with zero external dependencies.

## Prerequisites

- Rust toolchain (1.91+)
- AWS credentials (the S3 bucket is requester-pays)

## Quick Start

```bash
# Set AWS credentials
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_REGION=ap-northeast-1

# Run the example (builds + backfills 1000 blocks)
./run.sh
```

## What It Does

1. Builds `hypercore-indexer` in release mode (if not already built)
2. Backfills blocks 5,000,000 to 5,001,000 from mainnet S3
3. Stores everything in a local `hypercore.db` SQLite file

## Check the Data

```bash
sqlite3 hypercore.db 'SELECT COUNT(*) AS blocks FROM blocks;'
sqlite3 hypercore.db 'SELECT COUNT(*) AS txs FROM transactions;'
sqlite3 hypercore.db 'SELECT number, timestamp FROM blocks ORDER BY number DESC LIMIT 5;'
```

## Start Live Mode

After backfilling, follow the chain tip:

```bash
../../target/release/hypercore-indexer --config hypercore.toml live
```

This will continuously poll S3 for new blocks and index them into the same SQLite database.

## Customize

Edit `hypercore.toml` to:
- Change the network (`mainnet` / `testnet`)
- Increase `workers` for faster backfill (256-512 recommended for remote)
- Enable HIP4 prediction market decoding
- Enable trade fills ingestion
