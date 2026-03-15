use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Sqlite, SqlitePool, Transaction};
use std::str::FromStr;
use tracing::info;

use crate::decode::types::{DecodedBlock, DecodedTx};
use crate::hip4::types::{Hip4BlockData, Hip4Market, Hip4PriceRow};

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

CREATE TABLE IF NOT EXISTS hip4_markets (
    outcome_id      INTEGER NOT NULL PRIMARY KEY,
    name            TEXT NOT NULL,
    description     TEXT NOT NULL,
    side_specs      TEXT NOT NULL,
    question_id     INTEGER,
    question_name   TEXT,
    updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS hip4_prices (
    coin            TEXT NOT NULL,
    mid_price       TEXT NOT NULL,
    timestamp       TEXT NOT NULL,
    PRIMARY KEY (coin, timestamp)
);
CREATE INDEX IF NOT EXISTS idx_hip4_prices_time ON hip4_prices (timestamp);

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
                r#"INSERT INTO hip4_markets (outcome_id, name, description, side_specs, question_id, question_name, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
                   ON CONFLICT (outcome_id) DO UPDATE SET
                     name = excluded.name,
                     description = excluded.description,
                     side_specs = excluded.side_specs,
                     question_id = excluded.question_id,
                     question_name = excluded.question_name,
                     updated_at = datetime('now')"#,
            )
            .bind(m.outcome_id as i64)
            .bind(&m.name)
            .bind(&m.description)
            .bind(&m.side_specs)
            .bind(m.question_id.map(|v| v as i64))
            .bind(&m.question_name)
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
        if data.deposits.is_empty() && data.claims.is_empty() {
            return Ok(());
        }

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| eyre::eyre!("Failed to begin transaction: {e}"))?;

        Self::insert_hip4_deposits_in_tx(&mut tx, data).await?;
        Self::insert_hip4_claims_in_tx(&mut tx, data).await?;

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
}
