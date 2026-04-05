use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Sqlite, SqlitePool, Transaction};
use std::str::FromStr;
use tracing::info;

use crate::decode::types::{DecodedBlock, DecodedTx};
use crate::fills::types::FillRecord;
use crate::hip4::types::{Hip4BlockData, Hip4Market, Hip4MarketSnapshotRow, Hip4PriceRow};

use super::postgres::{asset_type_to_db, tx_type_to_smallint};
use super::Storage;

const SCHEMA_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS blocks (
    block_number    INTEGER PRIMARY KEY,
    block_hash      BLOB NOT NULL,
    parent_hash     BLOB NOT NULL,
    timestamp       INTEGER NOT NULL,
    gas_used        INTEGER NOT NULL,
    gas_limit       INTEGER NOT NULL,
    base_fee_per_gas INTEGER,
    tx_count        INTEGER NOT NULL,
    system_tx_count INTEGER NOT NULL,
    created_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_blocks_timestamp ON blocks (timestamp);

CREATE TABLE IF NOT EXISTS transactions (
    block_number    INTEGER NOT NULL REFERENCES blocks(block_number),
    tx_index        INTEGER NOT NULL,
    tx_hash         BLOB NOT NULL,
    tx_type         INTEGER NOT NULL,
    "from"          BLOB,
    "to"            BLOB,
    value           TEXT NOT NULL,
    input           BLOB NOT NULL,
    gas_limit       INTEGER NOT NULL,
    gas_used        INTEGER NOT NULL,
    success         INTEGER NOT NULL,
    PRIMARY KEY (block_number, tx_index)
);

CREATE INDEX IF NOT EXISTS idx_transactions_hash ON transactions (tx_hash);
CREATE INDEX IF NOT EXISTS idx_transactions_from ON transactions ("from") WHERE "from" IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_transactions_to ON transactions ("to") WHERE "to" IS NOT NULL;

CREATE TABLE IF NOT EXISTS system_transfers (
    block_number    INTEGER NOT NULL REFERENCES blocks(block_number),
    tx_index        INTEGER NOT NULL,
    official_hash   BLOB NOT NULL,
    explorer_hash   BLOB NOT NULL,
    system_address  BLOB NOT NULL,
    asset_type      TEXT NOT NULL,
    asset_index     INTEGER,
    recipient       BLOB NOT NULL,
    amount_wei      TEXT NOT NULL,
    PRIMARY KEY (block_number, tx_index)
);

CREATE INDEX IF NOT EXISTS idx_system_transfers_recipient ON system_transfers (recipient);
CREATE INDEX IF NOT EXISTS idx_system_transfers_official ON system_transfers (official_hash);
CREATE INDEX IF NOT EXISTS idx_system_transfers_explorer ON system_transfers (explorer_hash);

CREATE TABLE IF NOT EXISTS event_logs (
    block_number    INTEGER NOT NULL,
    tx_index        INTEGER NOT NULL,
    log_index       INTEGER NOT NULL,
    address         BLOB NOT NULL,
    topic0          BLOB,
    topic1          BLOB,
    topic2          BLOB,
    topic3          BLOB,
    data            BLOB NOT NULL,
    PRIMARY KEY (block_number, tx_index, log_index),
    FOREIGN KEY (block_number, tx_index) REFERENCES transactions(block_number, tx_index)
);

CREATE INDEX IF NOT EXISTS idx_event_logs_address_topic0 ON event_logs (address, topic0);
CREATE INDEX IF NOT EXISTS idx_event_logs_topic0 ON event_logs (topic0);

CREATE TABLE IF NOT EXISTS hip4_deposits (
    block_number    INTEGER NOT NULL,
    tx_index        INTEGER NOT NULL,
    log_index       INTEGER NOT NULL,
    contest_id      INTEGER NOT NULL,
    side_id         INTEGER NOT NULL,
    depositor       BLOB NOT NULL,
    amount_wei      TEXT NOT NULL,
    PRIMARY KEY (block_number, tx_index, log_index)
);

CREATE INDEX IF NOT EXISTS idx_hip4_deposits_contest ON hip4_deposits (contest_id, side_id);
CREATE INDEX IF NOT EXISTS idx_hip4_deposits_user ON hip4_deposits (depositor);

CREATE TABLE IF NOT EXISTS hip4_claims (
    block_number    INTEGER NOT NULL,
    tx_index        INTEGER NOT NULL,
    log_index       INTEGER NOT NULL,
    contest_id      INTEGER NOT NULL,
    side_id         INTEGER NOT NULL,
    claimer         BLOB NOT NULL,
    amount_wei      TEXT NOT NULL,
    PRIMARY KEY (block_number, tx_index, log_index)
);

CREATE INDEX IF NOT EXISTS idx_hip4_claims_contest ON hip4_claims (contest_id, side_id);
CREATE INDEX IF NOT EXISTS idx_hip4_claims_user ON hip4_claims (claimer);

CREATE TABLE IF NOT EXISTS hip4_contest_creations (
    block_number    INTEGER NOT NULL,
    tx_index        INTEGER NOT NULL,
    contest_id      INTEGER NOT NULL,
    param2          INTEGER NOT NULL,
    PRIMARY KEY (block_number, tx_index)
);

CREATE TABLE IF NOT EXISTS hip4_refunds (
    block_number    INTEGER NOT NULL,
    tx_index        INTEGER NOT NULL,
    contest_id      INTEGER NOT NULL,
    side_id         INTEGER NOT NULL,
    user_address    BLOB NOT NULL,
    PRIMARY KEY (block_number, tx_index)
);
CREATE INDEX IF NOT EXISTS idx_hip4_refunds_contest ON hip4_refunds (contest_id, side_id);
CREATE INDEX IF NOT EXISTS idx_hip4_refunds_user ON hip4_refunds (user_address);

CREATE TABLE IF NOT EXISTS hip4_sweeps (
    block_number    INTEGER NOT NULL,
    tx_index        INTEGER NOT NULL,
    contest_id      INTEGER NOT NULL,
    PRIMARY KEY (block_number, tx_index)
);

CREATE TABLE IF NOT EXISTS hip4_merkle_claims (
    block_number    INTEGER NOT NULL,
    tx_index        INTEGER NOT NULL,
    contest_id      INTEGER NOT NULL,
    side_id         INTEGER NOT NULL,
    user_address    BLOB NOT NULL,
    amount_wei      TEXT NOT NULL,
    proof_length    INTEGER NOT NULL,
    PRIMARY KEY (block_number, tx_index)
);
CREATE INDEX IF NOT EXISTS idx_hip4_merkle_claims_contest ON hip4_merkle_claims (contest_id, side_id);
CREATE INDEX IF NOT EXISTS idx_hip4_merkle_claims_user ON hip4_merkle_claims (user_address);

CREATE TABLE IF NOT EXISTS hip4_finalizations (
    block_number    INTEGER NOT NULL,
    tx_index        INTEGER NOT NULL,
    contest_id      INTEGER NOT NULL,
    PRIMARY KEY (block_number, tx_index)
);
CREATE INDEX IF NOT EXISTS idx_hip4_finalizations_contest ON hip4_finalizations (contest_id);

CREATE TABLE IF NOT EXISTS hip4_markets (
    outcome_id      INTEGER NOT NULL PRIMARY KEY,
    name            TEXT NOT NULL,
    description     TEXT NOT NULL,
    side_specs      TEXT NOT NULL,
    question_id     INTEGER,
    question_name   TEXT,
    desc_class      TEXT,
    desc_underlying TEXT,
    desc_expiry     TEXT,
    desc_target_price TEXT,
    desc_period     TEXT,
    question_description TEXT,
    settled_named_outcomes TEXT,
    fallback_outcome INTEGER,
    market_type TEXT NOT NULL DEFAULT 'custom',
    updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS hip4_prices (
    coin            TEXT NOT NULL,
    mid_price       TEXT NOT NULL,
    timestamp       TEXT NOT NULL,
    PRIMARY KEY (coin, timestamp)
);
CREATE INDEX IF NOT EXISTS idx_hip4_prices_time ON hip4_prices (timestamp);

CREATE TABLE IF NOT EXISTS hip4_market_snapshots (
    coin            TEXT NOT NULL,
    mark_px         TEXT,
    mid_px          TEXT,
    prev_day_px     TEXT,
    day_ntl_vlm     TEXT,
    day_base_vlm    TEXT,
    circulating_supply TEXT,
    total_supply    TEXT,
    timestamp       TEXT NOT NULL,
    PRIMARY KEY (coin, timestamp)
);

CREATE TABLE IF NOT EXISTS fills (
    trade_id        INTEGER NOT NULL,
    block_number    INTEGER NOT NULL,
    block_time      TEXT NOT NULL,
    user_address    TEXT NOT NULL,
    coin            TEXT NOT NULL,
    price           TEXT NOT NULL,
    size            TEXT NOT NULL,
    side            TEXT NOT NULL,
    direction       TEXT NOT NULL,
    closed_pnl      TEXT NOT NULL,
    hash            TEXT NOT NULL,
    order_id        INTEGER NOT NULL,
    crossed         INTEGER NOT NULL,
    fee             TEXT NOT NULL,
    fee_token       TEXT NOT NULL,
    fill_time       INTEGER NOT NULL,
    PRIMARY KEY (trade_id, user_address)
);

CREATE INDEX IF NOT EXISTS idx_fills_coin ON fills (coin, fill_time);
CREATE INDEX IF NOT EXISTS idx_fills_user ON fills (user_address, fill_time);
CREATE INDEX IF NOT EXISTS idx_fills_block ON fills (block_number);
CREATE INDEX IF NOT EXISTS idx_fills_time ON fills (fill_time);

CREATE TABLE IF NOT EXISTS hip4_trades (
    trade_id        INTEGER NOT NULL,
    block_number    INTEGER NOT NULL,
    block_time      TEXT NOT NULL,
    user_address    TEXT NOT NULL,
    coin            TEXT NOT NULL,
    price           TEXT NOT NULL,
    size            TEXT NOT NULL,
    side            TEXT NOT NULL,
    direction       TEXT NOT NULL,
    closed_pnl      TEXT NOT NULL,
    hash            TEXT NOT NULL,
    order_id        INTEGER NOT NULL,
    crossed         INTEGER NOT NULL,
    fee             TEXT NOT NULL,
    fee_token       TEXT NOT NULL,
    fill_time       INTEGER NOT NULL,
    PRIMARY KEY (trade_id, user_address)
);

CREATE INDEX IF NOT EXISTS idx_hip4_trades_coin ON hip4_trades (coin, fill_time);
CREATE INDEX IF NOT EXISTS idx_hip4_trades_user ON hip4_trades (user_address, fill_time);

CREATE TABLE IF NOT EXISTS indexer_cursor (
    network         TEXT PRIMARY KEY,
    last_block      INTEGER NOT NULL,
    updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
);
"#;

pub struct SqliteStorage {
    pool: SqlitePool,
}

impl SqliteStorage {
    /// Connect to a SQLite database. Creates the file if it doesn't exist.
    /// Use `sqlite::memory:` for in-memory databases (testing).
    pub async fn connect(database_url: &str) -> eyre::Result<Self> {
        let options = SqliteConnectOptions::from_str(database_url)
            .map_err(|e| eyre::eyre!("Invalid SQLite URL: {e}"))?
            .create_if_missing(true)
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
            .synchronous(sqlx::sqlite::SqliteSynchronous::Normal);

        let pool = SqlitePoolOptions::new()
            .max_connections(1) // SQLite writes are serialized anyway
            .connect_with(options)
            .await
            .map_err(|e| eyre::eyre!("Failed to connect to SQLite: {e}"))?;

        info!("Connected to SQLite");
        Ok(Self { pool })
    }

    /// Used by tests for direct queries.
    #[allow(dead_code)]
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    pub async fn ensure_schema(&self) -> eyre::Result<()> {
        // SQLite doesn't support multiple statements in one query easily,
        // so split and execute each statement individually
        for statement in SCHEMA_SQL.split(';') {
            let trimmed = statement.trim();
            if trimmed.is_empty() {
                continue;
            }
            sqlx::query(trimmed)
                .execute(&self.pool)
                .await
                .map_err(|e| {
                    eyre::eyre!("Failed to execute schema SQL: {e}\nStatement: {trimmed}")
                })?;
        }
        info!("SQLite schema ensured");
        Ok(())
    }

    async fn insert_block_row(
        tx: &mut Transaction<'_, Sqlite>,
        block: &DecodedBlock,
    ) -> eyre::Result<()> {
        sqlx::query(
            r#"INSERT OR IGNORE INTO blocks (block_number, block_hash, parent_hash, timestamp, gas_used, gas_limit, base_fee_per_gas, tx_count, system_tx_count)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
        )
        .bind(block.number as i64)
        .bind(block.hash.as_slice())
        .bind(block.parent_hash.as_slice())
        .bind(block.timestamp as i64)
        .bind(block.gas_used as i64)
        .bind(block.gas_limit as i64)
        .bind(block.base_fee_per_gas.map(|v| v as i64))
        .bind(block.transactions.len() as i32)
        .bind(block.system_transfers.len() as i32)
        .execute(&mut **tx)
        .await
        .map_err(|e| eyre::eyre!("Failed to insert block {}: {e}", block.number))?;

        Ok(())
    }

    async fn insert_transactions(
        tx: &mut Transaction<'_, Sqlite>,
        block_number: u64,
        transactions: &[DecodedTx],
    ) -> eyre::Result<()> {
        for dtx in transactions {
            sqlx::query(
                r#"INSERT OR IGNORE INTO transactions (block_number, tx_index, tx_hash, tx_type, "from", "to", value, input, gas_limit, gas_used, success)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
            )
            .bind(block_number as i64)
            .bind(dtx.tx_index as i32)
            .bind(dtx.hash.as_slice())
            .bind(tx_type_to_smallint(dtx.tx_type) as i32)
            .bind(dtx.from.as_ref().map(|a| a.as_slice().to_vec()))
            .bind(dtx.to.as_ref().map(|a| a.as_slice().to_vec()))
            .bind(dtx.value.to_string())
            .bind(dtx.input.as_ref())
            .bind(dtx.gas_limit as i64)
            .bind(dtx.gas_used as i64)
            .bind(dtx.success)
            .execute(&mut **tx)
            .await
            .map_err(|e| eyre::eyre!("Failed to insert tx {}/{}: {e}", block_number, dtx.tx_index))?;

            // Insert logs for this transaction
            for log in &dtx.logs {
                sqlx::query(
                    r#"INSERT OR IGNORE INTO event_logs (block_number, tx_index, log_index, address, topic0, topic1, topic2, topic3, data)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
                )
                .bind(block_number as i64)
                .bind(dtx.tx_index as i32)
                .bind(log.log_index as i32)
                .bind(log.address.as_slice())
                .bind(log.topics.first().map(|t| t.as_slice().to_vec()))
                .bind(log.topics.get(1).map(|t| t.as_slice().to_vec()))
                .bind(log.topics.get(2).map(|t| t.as_slice().to_vec()))
                .bind(log.topics.get(3).map(|t| t.as_slice().to_vec()))
                .bind(log.data.as_ref())
                .execute(&mut **tx)
                .await
                .map_err(|e| eyre::eyre!("Failed to insert log {}/{}/{}: {e}", block_number, dtx.tx_index, log.log_index))?;
            }
        }

        Ok(())
    }

    async fn insert_system_transfers(
        tx: &mut Transaction<'_, Sqlite>,
        block: &DecodedBlock,
    ) -> eyre::Result<()> {
        for (i, stx) in block.system_transfers.iter().enumerate() {
            let (asset_type_str, asset_index) = asset_type_to_db(&stx.asset_type);

            sqlx::query(
                r#"INSERT OR IGNORE INTO system_transfers (block_number, tx_index, official_hash, explorer_hash, system_address, asset_type, asset_index, recipient, amount_wei)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
            )
            .bind(block.number as i64)
            .bind(i as i32)
            .bind(stx.official_hash.as_slice())
            .bind(stx.explorer_hash.as_slice())
            .bind(stx.system_address.as_slice())
            .bind(asset_type_str)
            .bind(asset_index.map(|v| v as i32))
            .bind(stx.recipient.as_slice())
            .bind(stx.amount_wei.to_string())
            .execute(&mut **tx)
            .await
            .map_err(|e| eyre::eyre!("Failed to insert system transfer {}/{}: {e}", block.number, i))?;
        }

        Ok(())
    }

    async fn insert_hip4_deposits_in_tx(
        tx: &mut Transaction<'_, Sqlite>,
        data: &Hip4BlockData,
    ) -> eyre::Result<()> {
        for d in &data.deposits {
            sqlx::query(
                r#"INSERT OR IGNORE INTO hip4_deposits (block_number, tx_index, log_index, contest_id, side_id, depositor, amount_wei)
                   VALUES (?, ?, ?, ?, ?, ?, ?)"#,
            )
            .bind(d.block_number as i64)
            .bind(d.tx_index as i32)
            .bind(d.log_index as i32)
            .bind(d.contest_id as i64)
            .bind(d.side_id as i64)
            .bind(d.depositor.as_slice())
            .bind(d.amount_wei.to_string())
            .execute(&mut **tx)
            .await
            .map_err(|e| eyre::eyre!("Failed to insert hip4_deposit {}/{}/{}: {e}", d.block_number, d.tx_index, d.log_index))?;
        }
        Ok(())
    }

    async fn insert_hip4_claims_in_tx(
        tx: &mut Transaction<'_, Sqlite>,
        data: &Hip4BlockData,
    ) -> eyre::Result<()> {
        for c in &data.claims {
            sqlx::query(
                r#"INSERT OR IGNORE INTO hip4_claims (block_number, tx_index, log_index, contest_id, side_id, claimer, amount_wei)
                   VALUES (?, ?, ?, ?, ?, ?, ?)"#,
            )
            .bind(c.block_number as i64)
            .bind(c.tx_index as i32)
            .bind(c.log_index as i32)
            .bind(c.contest_id as i64)
            .bind(c.side_id as i64)
            .bind(c.claimer.as_slice())
            .bind(c.amount_wei.to_string())
            .execute(&mut **tx)
            .await
            .map_err(|e| eyre::eyre!("Failed to insert hip4_claim {}/{}/{}: {e}", c.block_number, c.tx_index, c.log_index))?;
        }
        Ok(())
    }

    async fn insert_hip4_contest_creations_in_tx(
        tx: &mut Transaction<'_, Sqlite>,
        data: &Hip4BlockData,
    ) -> eyre::Result<()> {
        for c in &data.contest_creations {
            sqlx::query(
                r#"INSERT OR IGNORE INTO hip4_contest_creations (block_number, tx_index, contest_id, param2)
                   VALUES (?, ?, ?, ?)"#,
            )
            .bind(c.block_number as i64)
            .bind(c.tx_index as i32)
            .bind(c.contest_id as i64)
            .bind(c.param2 as i64)
            .execute(&mut **tx)
            .await
            .map_err(|e| eyre::eyre!("Failed to insert hip4_contest_creation {}/{}: {e}", c.block_number, c.tx_index))?;
        }
        Ok(())
    }

    async fn insert_hip4_refunds_in_tx(
        tx: &mut Transaction<'_, Sqlite>,
        data: &Hip4BlockData,
    ) -> eyre::Result<()> {
        for r in &data.refunds {
            sqlx::query(
                r#"INSERT OR IGNORE INTO hip4_refunds (block_number, tx_index, contest_id, side_id, user_address)
                   VALUES (?, ?, ?, ?, ?)"#,
            )
            .bind(r.block_number as i64)
            .bind(r.tx_index as i32)
            .bind(r.contest_id as i64)
            .bind(r.side_id as i64)
            .bind(r.user.as_slice())
            .execute(&mut **tx)
            .await
            .map_err(|e| eyre::eyre!("Failed to insert hip4_refund {}/{}: {e}", r.block_number, r.tx_index))?;
        }
        Ok(())
    }

    async fn insert_hip4_sweeps_in_tx(
        tx: &mut Transaction<'_, Sqlite>,
        data: &Hip4BlockData,
    ) -> eyre::Result<()> {
        for s in &data.sweeps {
            sqlx::query(
                r#"INSERT OR IGNORE INTO hip4_sweeps (block_number, tx_index, contest_id)
                   VALUES (?, ?, ?)"#,
            )
            .bind(s.block_number as i64)
            .bind(s.tx_index as i32)
            .bind(s.contest_id as i64)
            .execute(&mut **tx)
            .await
            .map_err(|e| eyre::eyre!("Failed to insert hip4_sweep {}/{}: {e}", s.block_number, s.tx_index))?;
        }
        Ok(())
    }

    async fn insert_hip4_merkle_claims_in_tx(
        tx: &mut Transaction<'_, Sqlite>,
        data: &Hip4BlockData,
    ) -> eyre::Result<()> {
        for c in &data.merkle_claims {
            sqlx::query(
                r#"INSERT OR IGNORE INTO hip4_merkle_claims (block_number, tx_index, contest_id, side_id, user_address, amount_wei, proof_length)
                   VALUES (?, ?, ?, ?, ?, ?, ?)"#,
            )
            .bind(c.block_number as i64)
            .bind(c.tx_index as i32)
            .bind(c.contest_id as i64)
            .bind(c.side_id as i64)
            .bind(c.user.as_slice())
            .bind(c.amount_wei.to_string())
            .bind(c.proof_length as i32)
            .execute(&mut **tx)
            .await
            .map_err(|e| eyre::eyre!("Failed to insert hip4_merkle_claim {}/{}: {e}", c.block_number, c.tx_index))?;
        }
        Ok(())
    }

    async fn insert_hip4_finalizations_in_tx(
        tx: &mut Transaction<'_, Sqlite>,
        data: &Hip4BlockData,
    ) -> eyre::Result<()> {
        for f in &data.finalizations {
            sqlx::query(
                r#"INSERT OR IGNORE INTO hip4_finalizations (block_number, tx_index, contest_id)
                   VALUES (?, ?, ?)"#,
            )
            .bind(f.block_number as i64)
            .bind(f.tx_index as i32)
            .bind(f.contest_id as i64)
            .execute(&mut **tx)
            .await
            .map_err(|e| eyre::eyre!("Failed to insert hip4_finalization {}/{}: {e}", f.block_number, f.tx_index))?;
        }
        Ok(())
    }

    async fn upsert_hip4_markets_sqlite(
        pool: &SqlitePool,
        markets: &[Hip4Market],
    ) -> eyre::Result<()> {
        if markets.is_empty() {
            return Ok(());
        }

        let mut tx = pool
            .begin()
            .await
            .map_err(|e| eyre::eyre!("Failed to begin transaction: {e}"))?;

        for m in markets {
            sqlx::query(
                r#"INSERT INTO hip4_markets (outcome_id, name, description, side_specs, question_id, question_name,
                                             desc_class, desc_underlying, desc_expiry, desc_target_price, desc_period,
                                             question_description, settled_named_outcomes, fallback_outcome, market_type,
                                             updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                   ON CONFLICT (outcome_id) DO UPDATE SET
                     name = excluded.name,
                     description = excluded.description,
                     side_specs = excluded.side_specs,
                     question_id = excluded.question_id,
                     question_name = excluded.question_name,
                     desc_class = excluded.desc_class,
                     desc_underlying = excluded.desc_underlying,
                     desc_expiry = excluded.desc_expiry,
                     desc_target_price = excluded.desc_target_price,
                     desc_period = excluded.desc_period,
                     question_description = excluded.question_description,
                     settled_named_outcomes = excluded.settled_named_outcomes,
                     fallback_outcome = excluded.fallback_outcome,
                     market_type = excluded.market_type,
                     updated_at = datetime('now')"#,
            )
            .bind(m.outcome_id as i64)
            .bind(&m.name)
            .bind(&m.description)
            .bind(&m.side_specs)
            .bind(m.question_id.map(|v| v as i64))
            .bind(&m.question_name)
            .bind(&m.parsed.class)
            .bind(&m.parsed.underlying)
            .bind(&m.parsed.expiry)
            .bind(&m.parsed.target_price)
            .bind(&m.parsed.period)
            .bind(&m.question_description)
            .bind(&m.settled_named_outcomes)
            .bind(m.fallback_outcome.map(|v| v as i64))
            .bind(&m.market_type)
            .execute(&mut *tx)
            .await
            .map_err(|e| eyre::eyre!("Failed to upsert hip4_market {}: {e}", m.outcome_id))?;
        }

        tx.commit()
            .await
            .map_err(|e| eyre::eyre!("Failed to commit hip4_markets transaction: {e}"))?;

        Ok(())
    }

    async fn insert_hip4_prices_sqlite(
        pool: &SqlitePool,
        prices: &[Hip4PriceRow],
    ) -> eyre::Result<()> {
        if prices.is_empty() {
            return Ok(());
        }

        let mut tx = pool
            .begin()
            .await
            .map_err(|e| eyre::eyre!("Failed to begin transaction: {e}"))?;

        for p in prices {
            // Store timestamp as ISO-8601 string for SQLite
            let ts_secs = p.timestamp_ms / 1000;
            let ts_str = format!("{ts_secs}");
            sqlx::query(
                r#"INSERT OR IGNORE INTO hip4_prices (coin, mid_price, timestamp)
                   VALUES (?, ?, datetime(?, 'unixepoch'))"#,
            )
            .bind(&p.coin)
            .bind(&p.mid_price)
            .bind(&ts_str)
            .execute(&mut *tx)
            .await
            .map_err(|e| eyre::eyre!("Failed to insert hip4_price {}: {e}", p.coin))?;
        }

        tx.commit()
            .await
            .map_err(|e| eyre::eyre!("Failed to commit hip4_prices transaction: {e}"))?;

        Ok(())
    }

    async fn insert_hip4_market_snapshots_sqlite(
        pool: &SqlitePool,
        snapshots: &[Hip4MarketSnapshotRow],
    ) -> eyre::Result<()> {
        if snapshots.is_empty() {
            return Ok(());
        }

        let mut tx = pool
            .begin()
            .await
            .map_err(|e| eyre::eyre!("Failed to begin transaction: {e}"))?;

        for s in snapshots {
            let ts_secs = s.timestamp_ms / 1000;
            let ts_str = format!("{ts_secs}");
            sqlx::query(
                r#"INSERT OR IGNORE INTO hip4_market_snapshots
                   (coin, mark_px, mid_px, prev_day_px, day_ntl_vlm, day_base_vlm,
                    circulating_supply, total_supply, timestamp)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime(?, 'unixepoch'))"#,
            )
            .bind(&s.coin)
            .bind(&s.mark_px)
            .bind(&s.mid_px)
            .bind(&s.prev_day_px)
            .bind(&s.day_ntl_vlm)
            .bind(&s.day_base_vlm)
            .bind(&s.circulating_supply)
            .bind(&s.total_supply)
            .bind(&ts_str)
            .execute(&mut *tx)
            .await
            .map_err(|e| eyre::eyre!("Failed to insert hip4_market_snapshot {}: {e}", s.coin))?;
        }

        tx.commit()
            .await
            .map_err(|e| eyre::eyre!("Failed to commit hip4_market_snapshots transaction: {e}"))?;

        Ok(())
    }

    async fn set_cursor_in_tx(
        tx: &mut Transaction<'_, Sqlite>,
        network: &str,
        block_number: u64,
    ) -> eyre::Result<()> {
        sqlx::query(
            r#"INSERT INTO indexer_cursor (network, last_block, updated_at)
               VALUES (?, ?, datetime('now'))
               ON CONFLICT (network) DO UPDATE SET last_block = excluded.last_block, updated_at = datetime('now')"#,
        )
        .bind(network)
        .bind(block_number as i64)
        .execute(&mut **tx)
        .await
        .map_err(|e| eyre::eyre!("Failed to set cursor: {e}"))?;

        Ok(())
    }
}

#[async_trait::async_trait]
impl Storage for SqliteStorage {
    async fn insert_block(&self, block: &DecodedBlock) -> eyre::Result<()> {
        self.insert_batch(std::slice::from_ref(block)).await
    }

    async fn insert_batch(&self, blocks: &[DecodedBlock]) -> eyre::Result<()> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| eyre::eyre!("Failed to begin transaction: {e}"))?;

        for block in blocks {
            Self::insert_block_row(&mut tx, block).await?;
            Self::insert_transactions(&mut tx, block.number, &block.transactions).await?;
            Self::insert_system_transfers(&mut tx, block).await?;
        }

        tx.commit()
            .await
            .map_err(|e| eyre::eyre!("Failed to commit transaction: {e}"))?;

        Ok(())
    }

    async fn insert_batch_and_set_cursor(
        &self,
        blocks: &[DecodedBlock],
        network: &str,
        block_number: u64,
    ) -> eyre::Result<()> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| eyre::eyre!("Failed to begin transaction: {e}"))?;

        for block in blocks {
            Self::insert_block_row(&mut tx, block).await?;
            Self::insert_transactions(&mut tx, block.number, &block.transactions).await?;
            Self::insert_system_transfers(&mut tx, block).await?;
        }

        Self::set_cursor_in_tx(&mut tx, network, block_number).await?;

        tx.commit()
            .await
            .map_err(|e| eyre::eyre!("Failed to commit transaction: {e}"))?;

        Ok(())
    }

    async fn get_cursor(&self, network: &str) -> eyre::Result<Option<u64>> {
        let row: Option<(i64,)> =
            sqlx::query_as("SELECT last_block FROM indexer_cursor WHERE network = ?")
                .bind(network)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| eyre::eyre!("Failed to get cursor: {e}"))?;

        Ok(row.map(|(v,)| v as u64))
    }

    async fn set_cursor(&self, network: &str, block_number: u64) -> eyre::Result<()> {
        sqlx::query(
            r#"INSERT INTO indexer_cursor (network, last_block, updated_at)
               VALUES (?, ?, datetime('now'))
               ON CONFLICT (network) DO UPDATE SET last_block = excluded.last_block, updated_at = datetime('now')"#,
        )
        .bind(network)
        .bind(block_number as i64)
        .execute(&self.pool)
        .await
        .map_err(|e| eyre::eyre!("Failed to set cursor: {e}"))?;

        Ok(())
    }

    async fn insert_hip4_data(&self, data: &Hip4BlockData) -> eyre::Result<()> {
        if data.deposits.is_empty()
            && data.claims.is_empty()
            && data.contest_creations.is_empty()
            && data.refunds.is_empty()
            && data.sweeps.is_empty()
            && data.merkle_claims.is_empty()
            && data.finalizations.is_empty()
        {
            return Ok(());
        }

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| eyre::eyre!("Failed to begin transaction: {e}"))?;

        Self::insert_hip4_deposits_in_tx(&mut tx, data).await?;
        Self::insert_hip4_claims_in_tx(&mut tx, data).await?;
        Self::insert_hip4_contest_creations_in_tx(&mut tx, data).await?;
        Self::insert_hip4_refunds_in_tx(&mut tx, data).await?;
        Self::insert_hip4_sweeps_in_tx(&mut tx, data).await?;
        Self::insert_hip4_merkle_claims_in_tx(&mut tx, data).await?;
        Self::insert_hip4_finalizations_in_tx(&mut tx, data).await?;

        tx.commit()
            .await
            .map_err(|e| eyre::eyre!("Failed to commit hip4 transaction: {e}"))?;

        Ok(())
    }

    async fn upsert_hip4_markets(&self, markets: &[Hip4Market]) -> eyre::Result<()> {
        Self::upsert_hip4_markets_sqlite(&self.pool, markets).await
    }

    async fn insert_hip4_prices(&self, prices: &[Hip4PriceRow]) -> eyre::Result<()> {
        Self::insert_hip4_prices_sqlite(&self.pool, prices).await
    }

    async fn insert_hip4_market_snapshots(
        &self,
        snapshots: &[Hip4MarketSnapshotRow],
    ) -> eyre::Result<()> {
        Self::insert_hip4_market_snapshots_sqlite(&self.pool, snapshots).await
    }

    async fn insert_fills(&self, fills: &[FillRecord]) -> eyre::Result<()> {
        if fills.is_empty() {
            return Ok(());
        }

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| eyre::eyre!("Failed to begin transaction: {e}"))?;

        for f in fills {
            sqlx::query(
                r#"INSERT OR IGNORE INTO fills (trade_id, block_number, block_time, user_address, coin, price, size, side, direction, closed_pnl, hash, order_id, crossed, fee, fee_token, fill_time)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
            )
            .bind(f.trade_id)
            .bind(f.block_number)
            .bind(&f.block_time)
            .bind(&f.user_address)
            .bind(&f.coin)
            .bind(&f.price)
            .bind(&f.size)
            .bind(&f.side)
            .bind(&f.direction)
            .bind(&f.closed_pnl)
            .bind(&f.hash)
            .bind(f.order_id)
            .bind(f.crossed)
            .bind(&f.fee)
            .bind(&f.fee_token)
            .bind(f.fill_time)
            .execute(&mut *tx)
            .await
            .map_err(|e| eyre::eyre!("Failed to insert fill {}: {e}", f.trade_id))?;
        }

        tx.commit()
            .await
            .map_err(|e| eyre::eyre!("Failed to commit fills transaction: {e}"))?;

        Ok(())
    }

    async fn insert_hip4_trade_fills(&self, fills: &[&FillRecord]) -> eyre::Result<()> {
        if fills.is_empty() {
            return Ok(());
        }

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| eyre::eyre!("Failed to begin transaction: {e}"))?;

        for f in fills {
            sqlx::query(
                r#"INSERT OR IGNORE INTO hip4_trades (trade_id, block_number, block_time, user_address, coin, price, size, side, direction, closed_pnl, hash, order_id, crossed, fee, fee_token, fill_time)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
            )
            .bind(f.trade_id)
            .bind(f.block_number)
            .bind(&f.block_time)
            .bind(&f.user_address)
            .bind(&f.coin)
            .bind(&f.price)
            .bind(&f.size)
            .bind(&f.side)
            .bind(&f.direction)
            .bind(&f.closed_pnl)
            .bind(&f.hash)
            .bind(f.order_id)
            .bind(f.crossed)
            .bind(&f.fee)
            .bind(&f.fee_token)
            .bind(f.fill_time)
            .execute(&mut *tx)
            .await
            .map_err(|e| eyre::eyre!("Failed to insert hip4_trade fill {}: {e}", f.trade_id))?;
        }

        tx.commit()
            .await
            .map_err(|e| eyre::eyre!("Failed to commit hip4_trades transaction: {e}"))?;

        Ok(())
    }
}
