use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Postgres, Transaction};
use tracing::info;

use crate::decode::types::{AssetType, DecodedBlock, TxType};

use super::Storage;

pub struct PostgresStorage {
    pool: PgPool,
}

impl PostgresStorage {
    pub async fn connect(database_url: &str) -> eyre::Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .acquire_timeout(std::time::Duration::from_secs(30))
            .connect(database_url)
            .await
            .map_err(|e| eyre::eyre!("Failed to connect to PostgreSQL: {e}"))?;

        info!("Connected to PostgreSQL");
        Ok(Self { pool })
    }

    /// Get a reference to the connection pool (for queries in tests, etc.).
    #[allow(dead_code)]
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Run init.sql to create tables if they don't exist.
    pub async fn ensure_schema(&self) -> eyre::Result<()> {
        let schema_sql = include_str!("../../deployments/hypercore-indexer-dev/init.sql");
        sqlx::raw_sql(schema_sql)
            .execute(&self.pool)
            .await
            .map_err(|e| eyre::eyre!("Failed to execute schema SQL: {e}"))?;
        info!("Schema ensured");
        Ok(())
    }

    /// Batch-insert block rows using UNNEST.
    async fn insert_block_rows(
        tx: &mut Transaction<'_, Postgres>,
        blocks: &[DecodedBlock],
    ) -> eyre::Result<()> {
        if blocks.is_empty() {
            return Ok(());
        }

        let mut block_numbers: Vec<i64> = Vec::with_capacity(blocks.len());
        let mut block_hashes: Vec<Vec<u8>> = Vec::with_capacity(blocks.len());
        let mut parent_hashes: Vec<Vec<u8>> = Vec::with_capacity(blocks.len());
        let mut timestamps: Vec<i64> = Vec::with_capacity(blocks.len());
        let mut gas_useds: Vec<i64> = Vec::with_capacity(blocks.len());
        let mut gas_limits: Vec<i64> = Vec::with_capacity(blocks.len());
        let mut base_fees: Vec<Option<i64>> = Vec::with_capacity(blocks.len());
        let mut tx_counts: Vec<i32> = Vec::with_capacity(blocks.len());
        let mut sys_tx_counts: Vec<i32> = Vec::with_capacity(blocks.len());

        for block in blocks {
            block_numbers.push(block.number as i64);
            block_hashes.push(block.hash.as_slice().to_vec());
            parent_hashes.push(block.parent_hash.as_slice().to_vec());
            timestamps.push(block.timestamp as i64);
            gas_useds.push(block.gas_used as i64);
            gas_limits.push(block.gas_limit as i64);
            base_fees.push(block.base_fee_per_gas.map(|v| v as i64));
            tx_counts.push(block.transactions.len() as i32);
            sys_tx_counts.push(block.system_transfers.len() as i32);
        }

        sqlx::query(
            r#"INSERT INTO blocks (block_number, block_hash, parent_hash, timestamp, gas_used, gas_limit, base_fee_per_gas, tx_count, system_tx_count)
               SELECT * FROM UNNEST($1::BIGINT[], $2::BYTEA[], $3::BYTEA[], $4::BIGINT[], $5::BIGINT[], $6::BIGINT[], $7::BIGINT[], $8::INTEGER[], $9::INTEGER[])
               ON CONFLICT (block_number) DO NOTHING"#,
        )
        .bind(&block_numbers)
        .bind(&block_hashes)
        .bind(&parent_hashes)
        .bind(&timestamps)
        .bind(&gas_useds)
        .bind(&gas_limits)
        .bind(&base_fees)
        .bind(&tx_counts)
        .bind(&sys_tx_counts)
        .execute(&mut **tx)
        .await
        .map_err(|e| eyre::eyre!("Failed to batch insert blocks: {e}"))?;

        Ok(())
    }

    /// Batch-insert all transactions across ALL blocks using a single UNNEST.
    async fn insert_all_transactions(
        tx: &mut Transaction<'_, Postgres>,
        blocks: &[DecodedBlock],
    ) -> eyre::Result<()> {
        let total: usize = blocks.iter().map(|b| b.transactions.len()).sum();
        if total == 0 {
            return Ok(());
        }

        let mut block_numbers: Vec<i64> = Vec::with_capacity(total);
        let mut tx_indexes: Vec<i32> = Vec::with_capacity(total);
        let mut tx_hashes: Vec<Vec<u8>> = Vec::with_capacity(total);
        let mut tx_types: Vec<i16> = Vec::with_capacity(total);
        let mut froms: Vec<Option<Vec<u8>>> = Vec::with_capacity(total);
        let mut tos: Vec<Option<Vec<u8>>> = Vec::with_capacity(total);
        let mut values: Vec<sqlx::types::BigDecimal> = Vec::with_capacity(total);
        let mut inputs: Vec<Vec<u8>> = Vec::with_capacity(total);
        let mut gas_limits: Vec<i64> = Vec::with_capacity(total);
        let mut gas_useds: Vec<i64> = Vec::with_capacity(total);
        let mut successes: Vec<bool> = Vec::with_capacity(total);

        for block in blocks {
            let bn = block.number as i64;
            for dtx in &block.transactions {
                block_numbers.push(bn);
                tx_indexes.push(dtx.tx_index as i32);
                tx_hashes.push(dtx.hash.as_slice().to_vec());
                tx_types.push(tx_type_to_smallint(dtx.tx_type));
                froms.push(dtx.from.as_ref().map(|a| a.as_slice().to_vec()));
                tos.push(dtx.to.as_ref().map(|a| a.as_slice().to_vec()));
                let value_numeric: sqlx::types::BigDecimal = dtx
                    .value
                    .to_string()
                    .parse()
                    .map_err(|e| eyre::eyre!("Failed to parse U256 as BigDecimal: {e}"))?;
                values.push(value_numeric);
                inputs.push(dtx.input.to_vec());
                gas_limits.push(dtx.gas_limit as i64);
                gas_useds.push(dtx.gas_used as i64);
                successes.push(dtx.success);
            }
        }

        sqlx::query(
            r#"INSERT INTO transactions (block_number, tx_index, tx_hash, tx_type, "from", "to", value, input, gas_limit, gas_used, success)
               SELECT * FROM UNNEST($1::BIGINT[], $2::INTEGER[], $3::BYTEA[], $4::SMALLINT[], $5::BYTEA[], $6::BYTEA[], $7::NUMERIC[], $8::BYTEA[], $9::BIGINT[], $10::BIGINT[], $11::BOOLEAN[])
               ON CONFLICT (block_number, tx_index) DO NOTHING"#,
        )
        .bind(&block_numbers)
        .bind(&tx_indexes)
        .bind(&tx_hashes)
        .bind(&tx_types)
        .bind(&froms)
        .bind(&tos)
        .bind(&values)
        .bind(&inputs)
        .bind(&gas_limits)
        .bind(&gas_useds)
        .bind(&successes)
        .execute(&mut **tx)
        .await
        .map_err(|e| eyre::eyre!("Failed to batch insert transactions: {e}"))?;

        Ok(())
    }

    /// Batch-insert all event logs across ALL blocks using a single UNNEST.
    async fn insert_all_logs(
        tx: &mut Transaction<'_, Postgres>,
        blocks: &[DecodedBlock],
    ) -> eyre::Result<()> {
        let total: usize = blocks
            .iter()
            .flat_map(|b| b.transactions.iter())
            .map(|t| t.logs.len())
            .sum();
        if total == 0 {
            return Ok(());
        }

        let mut block_numbers: Vec<i64> = Vec::with_capacity(total);
        let mut tx_indexes: Vec<i32> = Vec::with_capacity(total);
        let mut log_indexes: Vec<i32> = Vec::with_capacity(total);
        let mut addresses: Vec<Vec<u8>> = Vec::with_capacity(total);
        let mut topic0s: Vec<Option<Vec<u8>>> = Vec::with_capacity(total);
        let mut topic1s: Vec<Option<Vec<u8>>> = Vec::with_capacity(total);
        let mut topic2s: Vec<Option<Vec<u8>>> = Vec::with_capacity(total);
        let mut topic3s: Vec<Option<Vec<u8>>> = Vec::with_capacity(total);
        let mut datas: Vec<Vec<u8>> = Vec::with_capacity(total);

        for block in blocks {
            let bn = block.number as i64;
            for dtx in &block.transactions {
                for log in &dtx.logs {
                    block_numbers.push(bn);
                    tx_indexes.push(dtx.tx_index as i32);
                    log_indexes.push(log.log_index as i32);
                    addresses.push(log.address.as_slice().to_vec());
                    topic0s.push(log.topics.first().map(|t| t.as_slice().to_vec()));
                    topic1s.push(log.topics.get(1).map(|t| t.as_slice().to_vec()));
                    topic2s.push(log.topics.get(2).map(|t| t.as_slice().to_vec()));
                    topic3s.push(log.topics.get(3).map(|t| t.as_slice().to_vec()));
                    datas.push(log.data.to_vec());
                }
            }
        }

        sqlx::query(
            r#"INSERT INTO event_logs (block_number, tx_index, log_index, address, topic0, topic1, topic2, topic3, data)
               SELECT * FROM UNNEST($1::BIGINT[], $2::INTEGER[], $3::INTEGER[], $4::BYTEA[], $5::BYTEA[], $6::BYTEA[], $7::BYTEA[], $8::BYTEA[], $9::BYTEA[])
               ON CONFLICT (block_number, tx_index, log_index) DO NOTHING"#,
        )
        .bind(&block_numbers)
        .bind(&tx_indexes)
        .bind(&log_indexes)
        .bind(&addresses)
        .bind(&topic0s)
        .bind(&topic1s)
        .bind(&topic2s)
        .bind(&topic3s)
        .bind(&datas)
        .execute(&mut **tx)
        .await
        .map_err(|e| eyre::eyre!("Failed to batch insert logs: {e}"))?;

        Ok(())
    }

    /// Batch-insert all system transfers across ALL blocks using a single UNNEST.
    async fn insert_all_system_transfers(
        tx: &mut Transaction<'_, Postgres>,
        blocks: &[DecodedBlock],
    ) -> eyre::Result<()> {
        let total: usize = blocks.iter().map(|b| b.system_transfers.len()).sum();
        if total == 0 {
            return Ok(());
        }

        let mut block_numbers: Vec<i64> = Vec::with_capacity(total);
        let mut tx_indexes: Vec<i32> = Vec::with_capacity(total);
        let mut official_hashes: Vec<Vec<u8>> = Vec::with_capacity(total);
        let mut explorer_hashes: Vec<Vec<u8>> = Vec::with_capacity(total);
        let mut system_addresses: Vec<Vec<u8>> = Vec::with_capacity(total);
        let mut asset_types: Vec<String> = Vec::with_capacity(total);
        let mut asset_indexes: Vec<Option<i16>> = Vec::with_capacity(total);
        let mut recipients: Vec<Vec<u8>> = Vec::with_capacity(total);
        let mut amounts: Vec<sqlx::types::BigDecimal> = Vec::with_capacity(total);

        for block in blocks {
            let bn = block.number as i64;
            for (i, stx) in block.system_transfers.iter().enumerate() {
                let (asset_type_str, asset_index) = asset_type_to_db(&stx.asset_type);
                let amount_numeric: sqlx::types::BigDecimal = stx
                    .amount_wei
                    .to_string()
                    .parse()
                    .map_err(|e| eyre::eyre!("Failed to parse U256 as BigDecimal: {e}"))?;

                block_numbers.push(bn);
                tx_indexes.push(i as i32);
                official_hashes.push(stx.official_hash.as_slice().to_vec());
                explorer_hashes.push(stx.explorer_hash.as_slice().to_vec());
                system_addresses.push(stx.system_address.as_slice().to_vec());
                asset_types.push(asset_type_str.to_string());
                asset_indexes.push(asset_index);
                recipients.push(stx.recipient.as_slice().to_vec());
                amounts.push(amount_numeric);
            }
        }

        sqlx::query(
            r#"INSERT INTO system_transfers (block_number, tx_index, official_hash, explorer_hash, system_address, asset_type, asset_index, recipient, amount_wei)
               SELECT * FROM UNNEST($1::BIGINT[], $2::INTEGER[], $3::BYTEA[], $4::BYTEA[], $5::BYTEA[], $6::TEXT[], $7::SMALLINT[], $8::BYTEA[], $9::NUMERIC[])
               ON CONFLICT (block_number, tx_index) DO NOTHING"#,
        )
        .bind(&block_numbers)
        .bind(&tx_indexes)
        .bind(&official_hashes)
        .bind(&explorer_hashes)
        .bind(&system_addresses)
        .bind(&asset_types)
        .bind(&asset_indexes)
        .bind(&recipients)
        .bind(&amounts)
        .execute(&mut **tx)
        .await
        .map_err(|e| eyre::eyre!("Failed to batch insert system transfers: {e}"))?;

        Ok(())
    }

    /// Update cursor within an existing transaction.
    async fn set_cursor_in_tx(
        tx: &mut Transaction<'_, Postgres>,
        network: &str,
        block_number: u64,
    ) -> eyre::Result<()> {
        sqlx::query(
            r#"INSERT INTO indexer_cursor (network, last_block, updated_at)
               VALUES ($1, $2, NOW())
               ON CONFLICT (network) DO UPDATE SET last_block = $2, updated_at = NOW()"#,
        )
        .bind(network)
        .bind(block_number as i64)
        .execute(&mut **tx)
        .await
        .map_err(|e| eyre::eyre!("Failed to set cursor in transaction: {e}"))?;

        Ok(())
    }
}

#[async_trait::async_trait]
impl Storage for PostgresStorage {
    async fn insert_block(&self, block: &DecodedBlock) -> eyre::Result<()> {
        self.insert_batch(std::slice::from_ref(block)).await
    }

    async fn insert_batch(&self, blocks: &[DecodedBlock]) -> eyre::Result<()> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| eyre::eyre!("Failed to begin transaction: {e}"))?;

        // 4 UNNEST calls total regardless of batch size:
        // 1. blocks, 2. transactions+logs, 3. system_transfers
        Self::insert_block_rows(&mut tx, blocks).await?;
        Self::insert_all_transactions(&mut tx, blocks).await?;
        Self::insert_all_logs(&mut tx, blocks).await?;
        Self::insert_all_system_transfers(&mut tx, blocks).await?;

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

        Self::insert_block_rows(&mut tx, blocks).await?;
        Self::insert_all_transactions(&mut tx, blocks).await?;
        Self::insert_all_logs(&mut tx, blocks).await?;
        Self::insert_all_system_transfers(&mut tx, blocks).await?;
        Self::set_cursor_in_tx(&mut tx, network, block_number).await?;

        tx.commit()
            .await
            .map_err(|e| eyre::eyre!("Failed to commit transaction: {e}"))?;

        Ok(())
    }

    async fn get_cursor(&self, network: &str) -> eyre::Result<Option<u64>> {
        let row: Option<(i64,)> =
            sqlx::query_as("SELECT last_block FROM indexer_cursor WHERE network = $1")
                .bind(network)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| eyre::eyre!("Failed to get cursor: {e}"))?;

        Ok(row.map(|(v,)| v as u64))
    }

    async fn set_cursor(&self, network: &str, block_number: u64) -> eyre::Result<()> {
        sqlx::query(
            r#"INSERT INTO indexer_cursor (network, last_block, updated_at)
               VALUES ($1, $2, NOW())
               ON CONFLICT (network) DO UPDATE SET last_block = $2, updated_at = NOW()"#,
        )
        .bind(network)
        .bind(block_number as i64)
        .execute(&self.pool)
        .await
        .map_err(|e| eyre::eyre!("Failed to set cursor: {e}"))?;

        Ok(())
    }
}

/// Convert TxType to SMALLINT for storage.
pub fn tx_type_to_smallint(tx_type: TxType) -> i16 {
    match tx_type {
        TxType::Legacy => 0,
        TxType::Eip2930 => 1,
        TxType::Eip1559 => 2,
    }
}

/// Convert SMALLINT back to TxType.
/// Public API for consumers reading back from DB (M4+ query layer).
#[allow(dead_code)]
pub fn smallint_to_tx_type(val: i16) -> eyre::Result<TxType> {
    match val {
        0 => Ok(TxType::Legacy),
        1 => Ok(TxType::Eip2930),
        2 => Ok(TxType::Eip1559),
        _ => Err(eyre::eyre!("Unknown tx_type value: {val}")),
    }
}

/// Convert AssetType to (TEXT, Option<SMALLINT>) for storage.
pub fn asset_type_to_db(asset_type: &AssetType) -> (&'static str, Option<i16>) {
    match asset_type {
        AssetType::NativeHype => ("NativeHype", None),
        AssetType::SpotToken { asset_index } => ("SpotToken", Some(*asset_index as i16)),
    }
}

/// Convert (TEXT, Option<SMALLINT>) back to AssetType.
/// Public API for consumers reading back from DB (M4+ query layer).
#[allow(dead_code)]
pub fn db_to_asset_type(type_str: &str, asset_index: Option<i16>) -> eyre::Result<AssetType> {
    match type_str {
        "NativeHype" => Ok(AssetType::NativeHype),
        "SpotToken" => {
            let idx = asset_index.ok_or_else(|| eyre::eyre!("SpotToken requires asset_index"))?;
            Ok(AssetType::SpotToken {
                asset_index: idx as u16,
            })
        }
        _ => Err(eyre::eyre!("Unknown asset_type: {type_str}")),
    }
}

/// Type conversion round-trips for PostgreSQL storage:
/// - TxType ↔ SMALLINT (Legacy=0, Eip2930=1, Eip1559=2)
/// - AssetType ↔ (TEXT, Option<SMALLINT>)
/// - U256 → BigDecimal (including U256::MAX, lossless)
#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::U256;

    #[test]
    fn tx_type_roundtrip() {
        for (tt, expected_val) in [
            (TxType::Legacy, 0i16),
            (TxType::Eip2930, 1i16),
            (TxType::Eip1559, 2i16),
        ] {
            let val = tx_type_to_smallint(tt);
            assert_eq!(val, expected_val);
            let back = smallint_to_tx_type(val).unwrap();
            assert_eq!(back, tt);
        }
    }

    #[test]
    fn asset_type_roundtrip() {
        let (s, idx) = asset_type_to_db(&AssetType::NativeHype);
        assert_eq!(s, "NativeHype");
        assert_eq!(idx, None);
        let back = db_to_asset_type(s, idx).unwrap();
        assert_eq!(back, AssetType::NativeHype);

        let (s, idx) = asset_type_to_db(&AssetType::SpotToken { asset_index: 42 });
        assert_eq!(s, "SpotToken");
        assert_eq!(idx, Some(42));
        let back = db_to_asset_type(s, idx).unwrap();
        assert_eq!(back, AssetType::SpotToken { asset_index: 42 });
    }

    #[test]
    fn u256_to_numeric_conversion() {
        // Zero
        let val = U256::ZERO;
        let s = val.to_string();
        assert_eq!(s, "0");
        let _: sqlx::types::BigDecimal = s.parse().unwrap();

        // 1 ETH in wei
        let val = U256::from(1_000_000_000_000_000_000u64);
        let s = val.to_string();
        assert_eq!(s, "1000000000000000000");
        let _: sqlx::types::BigDecimal = s.parse().unwrap();

        // Max U256
        let val = U256::MAX;
        let s = val.to_string();
        let bd: sqlx::types::BigDecimal = s.parse().unwrap();
        assert_eq!(bd.to_string(), val.to_string());
    }
}
