# Live SQLite Example

Index Hyperliquid blocks into SQLite with zero external dependencies.

## Prerequisites

- Rust toolchain (1.91+)
- AWS credentials configured (the S3 bucket is requester-pays)

## Quick Start

```bash
# AWS credentials — either profile-based or env vars
aws configure                  # option 1: ~/.aws/credentials
# export AWS_ACCESS_KEY_ID=... # option 2: env vars

# Run the example — backfills to chain tip, then follows it live
./run.sh
```

## What It Does

**First run:**
1. Builds `hypercore-indexer` in release mode (if not already built)
2. Discovers the chain tip on S3
3. Backfills from block 5,000,000 to the tip
4. Automatically switches to live mode (follows the chain)

**Subsequent runs:**
1. Detects existing cursor in `hypercore.db`
2. Goes straight to live mode (resumes from last indexed block)

## Custom Start Block

```bash
# Start from a more recent block (faster initial sync)
./run.sh 29000000
```

## Check the Data

```bash
sqlite3 hypercore.db 'SELECT COUNT(*) AS blocks FROM blocks;'
sqlite3 hypercore.db 'SELECT COUNT(*) AS txs FROM transactions;'
sqlite3 hypercore.db 'SELECT block_number, tx_count FROM blocks ORDER BY block_number DESC LIMIT 5;'
```

## Customize

Edit `hypercore.toml` to:
- Change the network (`mainnet` / `testnet`)
- Increase `workers` for faster backfill (256-512 recommended for remote)
- Enable HIP4 prediction market decoding
- Enable trade fills ingestion
