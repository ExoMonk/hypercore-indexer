# Live HIP4 Prediction Markets with ClickHouse

Index Hyperliquid's HIP4 prediction markets in real-time on testnet, storing everything in ClickHouse.

## What You Get

- **Block data**: all testnet blocks, transactions, event logs
- **System transfers**: HyperCore-to-HyperEVM bridge events with dual phantom hashes
- **HIP4 deposits**: who bet on which side of which contest, and how much
- **HIP4 claims**: settlement payouts with amounts
- **Market metadata**: outcome names, sides, descriptions (polled from API every 60s)
- **Price snapshots**: implied probabilities for all `#`-prefixed coins (polled every 5s)

## Prerequisites

- Docker and Docker Compose
- AWS credentials configured in `~/.aws/credentials`

## Quick Start

```bash
# Ensure AWS credentials
aws sts get-caller-identity

# Start ClickHouse + live indexer
docker compose up -d

# Watch the logs
docker compose logs -f indexer

# Check the data
./queries.sh
```

## How It Works

1. ClickHouse starts and waits for health check
2. The indexer discovers the testnet chain tip
3. Starts indexing blocks in real-time (adaptive polling, ~1 block/sec)
4. Decodes HIP4 contest events (deposits, claims, refunds, contest creations)
5. Polls the HyperCore API for market metadata and prices
6. Everything stored in ClickHouse with ReplacingMergeTree

The indexer runs continuously with `restart: unless-stopped`.

## Sample Queries

```bash
# Run all sample queries
./queries.sh

# Or connect directly
docker compose exec clickhouse clickhouse-client
```

```sql
-- Deposits per contest
SELECT contest_id, count() AS deposits
FROM hip4_deposits
GROUP BY contest_id
ORDER BY deposits DESC;

-- Price history for outcome #90 (Hypurr in 100m dash)
SELECT coin, mid_price, timestamp
FROM hip4_prices FINAL
WHERE coin = '#90'
ORDER BY timestamp DESC
LIMIT 20;

-- Market overview
SELECT outcome_id, name, description
FROM hip4_markets FINAL;
```

## Switch to Mainnet

When HIP4 launches on mainnet, update `hypercore.toml`:

```toml
[network]
name = "mainnet"

[hip4]
contest_address = "0x..."    # mainnet contest contract address
api_url = "https://api.hyperliquid.xyz/info"
```

Then restart: `docker compose restart indexer`

## Clean Up

```bash
docker compose down -v    # removes containers + ClickHouse data
```
