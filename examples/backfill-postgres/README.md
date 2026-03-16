# Backfill PostgreSQL Example

Backfill Hyperliquid blocks into PostgreSQL using Docker Compose.

## Prerequisites

- Docker and Docker Compose
- AWS credentials (the S3 bucket is requester-pays)

## Quick Start

```bash
# Set AWS credentials
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...

# Start PostgreSQL + run backfill
docker compose up

# In another terminal, check the data
./queries.sh
```

## What It Does

1. Starts a PostgreSQL 16 container
2. Builds hypercore-indexer from the project Dockerfile
3. Backfills blocks 5,000,000 to 5,001,000 into PostgreSQL
4. The indexer exits when backfill is complete

## Check the Data

```bash
# Run sample queries
./queries.sh

# Or connect directly
docker compose exec postgres psql -U postgres -d hypercore
```

## Customize the Range

Edit `docker-compose.yml` and change the indexer command:

```yaml
command: ["backfill", "--from", "1000000", "--to", "2000000"]
```

Then re-run:

```bash
docker compose up indexer
```

## Increase Throughput

Edit `hypercore.toml` and increase workers:

```toml
[pipeline]
workers = 256    # or 512 for maximum throughput
```

## Clean Up

```bash
docker compose down -v   # removes containers + data volume
```
