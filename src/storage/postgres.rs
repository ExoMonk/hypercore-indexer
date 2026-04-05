use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Postgres, Transaction};
use tracing::info;

use crate::decode::types::{AssetType, DecodedBlock, TxType};
use crate::fills::types::FillRecord;
use crate::hip4::types::{Hip4BlockData, Hip4Market, Hip4MarketSnapshotRow, Hip4PriceRow};

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

    /// Batch-insert HIP4 deposits using UNNEST.
    async fn insert_hip4_deposits(pool: &PgPool, data: &Hip4BlockData) -> eyre::Result<()> {
        if data.deposits.is_empty() {
            return Ok(());
        }

        let len = data.deposits.len();
        let mut block_numbers: Vec<i64> = Vec::with_capacity(len);
        let mut tx_indexes: Vec<i32> = Vec::with_capacity(len);
        let mut log_indexes: Vec<i32> = Vec::with_capacity(len);
        let mut contest_ids: Vec<i64> = Vec::with_capacity(len);
        let mut side_ids: Vec<i64> = Vec::with_capacity(len);
        let mut depositors: Vec<Vec<u8>> = Vec::with_capacity(len);
        let mut amounts: Vec<sqlx::types::BigDecimal> = Vec::with_capacity(len);

        for d in &data.deposits {
            block_numbers.push(d.block_number as i64);
            tx_indexes.push(d.tx_index as i32);
            log_indexes.push(d.log_index as i32);
            contest_ids.push(d.contest_id as i64);
            side_ids.push(d.side_id as i64);
            depositors.push(d.depositor.as_slice().to_vec());
            let amount_numeric: sqlx::types::BigDecimal = d
                .amount_wei
                .to_string()
                .parse()
                .map_err(|e| eyre::eyre!("Failed to parse U256 as BigDecimal: {e}"))?;
            amounts.push(amount_numeric);
        }

        sqlx::query(
            r#"INSERT INTO hip4_deposits (block_number, tx_index, log_index, contest_id, side_id, depositor, amount_wei)
               SELECT * FROM UNNEST($1::BIGINT[], $2::INTEGER[], $3::INTEGER[], $4::BIGINT[], $5::BIGINT[], $6::BYTEA[], $7::NUMERIC[])
               ON CONFLICT (block_number, tx_index, log_index) DO NOTHING"#,
        )
        .bind(&block_numbers)
        .bind(&tx_indexes)
        .bind(&log_indexes)
        .bind(&contest_ids)
        .bind(&side_ids)
        .bind(&depositors)
        .bind(&amounts)
        .execute(pool)
        .await
        .map_err(|e| eyre::eyre!("Failed to batch insert hip4_deposits: {e}"))?;

        Ok(())
    }

    /// Batch-insert HIP4 claims using UNNEST.
    async fn insert_hip4_claims(pool: &PgPool, data: &Hip4BlockData) -> eyre::Result<()> {
        if data.claims.is_empty() {
            return Ok(());
        }

        let len = data.claims.len();
        let mut block_numbers: Vec<i64> = Vec::with_capacity(len);
        let mut tx_indexes: Vec<i32> = Vec::with_capacity(len);
        let mut log_indexes: Vec<i32> = Vec::with_capacity(len);
        let mut contest_ids: Vec<i64> = Vec::with_capacity(len);
        let mut side_ids: Vec<i64> = Vec::with_capacity(len);
        let mut claimers: Vec<Vec<u8>> = Vec::with_capacity(len);
        let mut amounts: Vec<sqlx::types::BigDecimal> = Vec::with_capacity(len);

        for c in &data.claims {
            block_numbers.push(c.block_number as i64);
            tx_indexes.push(c.tx_index as i32);
            log_indexes.push(c.log_index as i32);
            contest_ids.push(c.contest_id as i64);
            side_ids.push(c.side_id as i64);
            claimers.push(c.claimer.as_slice().to_vec());
            let amount_numeric: sqlx::types::BigDecimal = c
                .amount_wei
                .to_string()
                .parse()
                .map_err(|e| eyre::eyre!("Failed to parse U256 as BigDecimal: {e}"))?;
            amounts.push(amount_numeric);
        }

        sqlx::query(
            r#"INSERT INTO hip4_claims (block_number, tx_index, log_index, contest_id, side_id, claimer, amount_wei)
               SELECT * FROM UNNEST($1::BIGINT[], $2::INTEGER[], $3::INTEGER[], $4::BIGINT[], $5::BIGINT[], $6::BYTEA[], $7::NUMERIC[])
               ON CONFLICT (block_number, tx_index, log_index) DO NOTHING"#,
        )
        .bind(&block_numbers)
        .bind(&tx_indexes)
        .bind(&log_indexes)
        .bind(&contest_ids)
        .bind(&side_ids)
        .bind(&claimers)
        .bind(&amounts)
        .execute(pool)
        .await
        .map_err(|e| eyre::eyre!("Failed to batch insert hip4_claims: {e}"))?;

        Ok(())
    }

    /// Batch-insert HIP4 contest creations using UNNEST.
    async fn insert_hip4_contest_creations(pool: &PgPool, data: &Hip4BlockData) -> eyre::Result<()> {
        if data.contest_creations.is_empty() {
            return Ok(());
        }

        let len = data.contest_creations.len();
        let mut block_numbers: Vec<i64> = Vec::with_capacity(len);
        let mut tx_indexes: Vec<i32> = Vec::with_capacity(len);
        let mut contest_ids: Vec<i64> = Vec::with_capacity(len);
        let mut param2s: Vec<i64> = Vec::with_capacity(len);

        for c in &data.contest_creations {
            block_numbers.push(c.block_number as i64);
            tx_indexes.push(c.tx_index as i32);
            contest_ids.push(c.contest_id as i64);
            param2s.push(c.param2 as i64);
        }

        sqlx::query(
            r#"INSERT INTO hip4_contest_creations (block_number, tx_index, contest_id, param2)
               SELECT * FROM UNNEST($1::BIGINT[], $2::INTEGER[], $3::BIGINT[], $4::BIGINT[])
               ON CONFLICT (block_number, tx_index) DO NOTHING"#,
        )
        .bind(&block_numbers)
        .bind(&tx_indexes)
        .bind(&contest_ids)
        .bind(&param2s)
        .execute(pool)
        .await
        .map_err(|e| eyre::eyre!("Failed to batch insert hip4_contest_creations: {e}"))?;

        Ok(())
    }

    /// Batch-insert HIP4 refunds using UNNEST.
    async fn insert_hip4_refunds(pool: &PgPool, data: &Hip4BlockData) -> eyre::Result<()> {
        if data.refunds.is_empty() {
            return Ok(());
        }

        let len = data.refunds.len();
        let mut block_numbers: Vec<i64> = Vec::with_capacity(len);
        let mut tx_indexes: Vec<i32> = Vec::with_capacity(len);
        let mut contest_ids: Vec<i64> = Vec::with_capacity(len);
        let mut side_ids: Vec<i64> = Vec::with_capacity(len);
        let mut user_addresses: Vec<Vec<u8>> = Vec::with_capacity(len);

        for r in &data.refunds {
            block_numbers.push(r.block_number as i64);
            tx_indexes.push(r.tx_index as i32);
            contest_ids.push(r.contest_id as i64);
            side_ids.push(r.side_id as i64);
            user_addresses.push(r.user.as_slice().to_vec());
        }

        sqlx::query(
            r#"INSERT INTO hip4_refunds (block_number, tx_index, contest_id, side_id, user_address)
               SELECT * FROM UNNEST($1::BIGINT[], $2::INTEGER[], $3::BIGINT[], $4::BIGINT[], $5::BYTEA[])
               ON CONFLICT (block_number, tx_index) DO NOTHING"#,
        )
        .bind(&block_numbers)
        .bind(&tx_indexes)
        .bind(&contest_ids)
        .bind(&side_ids)
        .bind(&user_addresses)
        .execute(pool)
        .await
        .map_err(|e| eyre::eyre!("Failed to batch insert hip4_refunds: {e}"))?;

        Ok(())
    }

    /// Batch-insert HIP4 sweeps using UNNEST.
    async fn insert_hip4_sweeps(pool: &PgPool, data: &Hip4BlockData) -> eyre::Result<()> {
        if data.sweeps.is_empty() {
            return Ok(());
        }

        let len = data.sweeps.len();
        let mut block_numbers: Vec<i64> = Vec::with_capacity(len);
        let mut tx_indexes: Vec<i32> = Vec::with_capacity(len);
        let mut contest_ids: Vec<i64> = Vec::with_capacity(len);

        for s in &data.sweeps {
            block_numbers.push(s.block_number as i64);
            tx_indexes.push(s.tx_index as i32);
            contest_ids.push(s.contest_id as i64);
        }

        sqlx::query(
            r#"INSERT INTO hip4_sweeps (block_number, tx_index, contest_id)
               SELECT * FROM UNNEST($1::BIGINT[], $2::INTEGER[], $3::BIGINT[])
               ON CONFLICT (block_number, tx_index) DO NOTHING"#,
        )
        .bind(&block_numbers)
        .bind(&tx_indexes)
        .bind(&contest_ids)
        .execute(pool)
        .await
        .map_err(|e| eyre::eyre!("Failed to batch insert hip4_sweeps: {e}"))?;

        Ok(())
    }

    async fn insert_hip4_merkle_claims(pool: &PgPool, data: &Hip4BlockData) -> eyre::Result<()> {
        if data.merkle_claims.is_empty() {
            return Ok(());
        }

        let len = data.merkle_claims.len();
        let mut block_numbers: Vec<i64> = Vec::with_capacity(len);
        let mut tx_indexes: Vec<i32> = Vec::with_capacity(len);
        let mut contest_ids: Vec<i64> = Vec::with_capacity(len);
        let mut side_ids: Vec<i64> = Vec::with_capacity(len);
        let mut user_addresses: Vec<Vec<u8>> = Vec::with_capacity(len);
        let mut amounts: Vec<sqlx::types::BigDecimal> = Vec::with_capacity(len);
        let mut proof_lengths: Vec<i32> = Vec::with_capacity(len);

        for c in &data.merkle_claims {
            block_numbers.push(c.block_number as i64);
            tx_indexes.push(c.tx_index as i32);
            contest_ids.push(c.contest_id as i64);
            side_ids.push(c.side_id as i64);
            user_addresses.push(c.user.as_slice().to_vec());
            let amount: sqlx::types::BigDecimal = c
                .amount_wei
                .to_string()
                .parse()
                .map_err(|e| eyre::eyre!("Failed to parse amount_wei: {e}"))?;
            amounts.push(amount);
            proof_lengths.push(c.proof_length as i32);
        }

        sqlx::query(
            r#"INSERT INTO hip4_merkle_claims (block_number, tx_index, contest_id, side_id, user_address, amount_wei, proof_length)
               SELECT * FROM UNNEST($1::BIGINT[], $2::INTEGER[], $3::BIGINT[], $4::BIGINT[], $5::BYTEA[], $6::NUMERIC[], $7::INTEGER[])
               ON CONFLICT (block_number, tx_index) DO NOTHING"#,
        )
        .bind(&block_numbers)
        .bind(&tx_indexes)
        .bind(&contest_ids)
        .bind(&side_ids)
        .bind(&user_addresses)
        .bind(&amounts)
        .bind(&proof_lengths)
        .execute(pool)
        .await
        .map_err(|e| eyre::eyre!("Failed to batch insert hip4_merkle_claims: {e}"))?;

        Ok(())
    }

    async fn insert_hip4_finalizations(pool: &PgPool, data: &Hip4BlockData) -> eyre::Result<()> {
        if data.finalizations.is_empty() {
            return Ok(());
        }

        let len = data.finalizations.len();
        let mut block_numbers: Vec<i64> = Vec::with_capacity(len);
        let mut tx_indexes: Vec<i32> = Vec::with_capacity(len);
        let mut contest_ids: Vec<i64> = Vec::with_capacity(len);

        for f in &data.finalizations {
            block_numbers.push(f.block_number as i64);
            tx_indexes.push(f.tx_index as i32);
            contest_ids.push(f.contest_id as i64);
        }

        sqlx::query(
            r#"INSERT INTO hip4_finalizations (block_number, tx_index, contest_id)
               SELECT * FROM UNNEST($1::BIGINT[], $2::INTEGER[], $3::BIGINT[])
               ON CONFLICT (block_number, tx_index) DO NOTHING"#,
        )
        .bind(&block_numbers)
        .bind(&tx_indexes)
        .bind(&contest_ids)
        .execute(pool)
        .await
        .map_err(|e| eyre::eyre!("Failed to batch insert hip4_finalizations: {e}"))?;

        Ok(())
    }

    /// Batch-upsert HIP4 markets using UNNEST.
    async fn upsert_hip4_markets_pg(pool: &PgPool, markets: &[Hip4Market]) -> eyre::Result<()> {
        if markets.is_empty() {
            return Ok(());
        }

        let len = markets.len();
        let mut outcome_ids: Vec<i32> = Vec::with_capacity(len);
        let mut names: Vec<String> = Vec::with_capacity(len);
        let mut descriptions: Vec<String> = Vec::with_capacity(len);
        let mut side_specs_vec: Vec<String> = Vec::with_capacity(len);
        let mut question_ids: Vec<Option<i32>> = Vec::with_capacity(len);
        let mut question_names: Vec<Option<String>> = Vec::with_capacity(len);
        let mut desc_classes: Vec<Option<String>> = Vec::with_capacity(len);
        let mut desc_underlyings: Vec<Option<String>> = Vec::with_capacity(len);
        let mut desc_expiries: Vec<Option<String>> = Vec::with_capacity(len);
        let mut desc_target_prices: Vec<Option<String>> = Vec::with_capacity(len);
        let mut desc_periods: Vec<Option<String>> = Vec::with_capacity(len);
        let mut question_descriptions: Vec<Option<String>> = Vec::with_capacity(len);
        let mut settled_named_outcomes_vec: Vec<Option<String>> = Vec::with_capacity(len);
        let mut fallback_outcomes: Vec<Option<i32>> = Vec::with_capacity(len);
        let mut market_types: Vec<String> = Vec::with_capacity(len);

        for m in markets {
            outcome_ids.push(m.outcome_id as i32);
            names.push(m.name.clone());
            descriptions.push(m.description.clone());
            side_specs_vec.push(m.side_specs.clone());
            question_ids.push(m.question_id.map(|v| v as i32));
            question_names.push(m.question_name.clone());
            desc_classes.push(m.parsed.class.clone());
            desc_underlyings.push(m.parsed.underlying.clone());
            desc_expiries.push(m.parsed.expiry.clone());
            desc_target_prices.push(m.parsed.target_price.clone());
            desc_periods.push(m.parsed.period.clone());
            question_descriptions.push(m.question_description.clone());
            settled_named_outcomes_vec.push(m.settled_named_outcomes.clone());
            fallback_outcomes.push(m.fallback_outcome.map(|v| v as i32));
            market_types.push(m.market_type.clone());
        }

        sqlx::query(
            r#"INSERT INTO hip4_markets (outcome_id, name, description, side_specs, question_id, question_name,
                                         desc_class, desc_underlying, desc_expiry, desc_target_price, desc_period,
                                         question_description, settled_named_outcomes, fallback_outcome, market_type,
                                         updated_at)
               SELECT o, n, d, s, q, qn, dc, du, de, dtp, dp, qd, sno, fo, mt, NOW()
               FROM UNNEST($1::INTEGER[], $2::TEXT[], $3::TEXT[], $4::TEXT[], $5::INTEGER[], $6::TEXT[],
                           $7::TEXT[], $8::TEXT[], $9::TEXT[], $10::TEXT[], $11::TEXT[],
                           $12::TEXT[], $13::TEXT[], $14::INTEGER[], $15::TEXT[])
                    AS t(o, n, d, s, q, qn, dc, du, de, dtp, dp, qd, sno, fo, mt)
               ON CONFLICT (outcome_id) DO UPDATE SET
                 name = EXCLUDED.name,
                 description = EXCLUDED.description,
                 side_specs = EXCLUDED.side_specs,
                 question_id = EXCLUDED.question_id,
                 question_name = EXCLUDED.question_name,
                 desc_class = EXCLUDED.desc_class,
                 desc_underlying = EXCLUDED.desc_underlying,
                 desc_expiry = EXCLUDED.desc_expiry,
                 desc_target_price = EXCLUDED.desc_target_price,
                 desc_period = EXCLUDED.desc_period,
                 question_description = EXCLUDED.question_description,
                 settled_named_outcomes = EXCLUDED.settled_named_outcomes,
                 fallback_outcome = EXCLUDED.fallback_outcome,
                 market_type = EXCLUDED.market_type,
                 updated_at = NOW()"#,
        )
        .bind(&outcome_ids)
        .bind(&names)
        .bind(&descriptions)
        .bind(&side_specs_vec)
        .bind(&question_ids)
        .bind(&question_names)
        .bind(&desc_classes)
        .bind(&desc_underlyings)
        .bind(&desc_expiries)
        .bind(&desc_target_prices)
        .bind(&desc_periods)
        .bind(&question_descriptions)
        .bind(&settled_named_outcomes_vec)
        .bind(&fallback_outcomes)
        .bind(&market_types)
        .execute(pool)
        .await
        .map_err(|e| eyre::eyre!("Failed to upsert hip4_markets: {e}"))?;

        Ok(())
    }

    /// Batch-insert HIP4 prices using UNNEST.
    async fn insert_hip4_prices_pg(pool: &PgPool, prices: &[Hip4PriceRow]) -> eyre::Result<()> {
        if prices.is_empty() {
            return Ok(());
        }

        let len = prices.len();
        let mut coins: Vec<String> = Vec::with_capacity(len);
        let mut mid_prices: Vec<sqlx::types::BigDecimal> = Vec::with_capacity(len);
        let mut timestamps: Vec<i64> = Vec::with_capacity(len);

        for p in prices {
            coins.push(p.coin.clone());
            let price_numeric: sqlx::types::BigDecimal = p
                .mid_price
                .parse()
                .map_err(|e| eyre::eyre!("Failed to parse mid_price '{}' as BigDecimal: {e}", p.mid_price))?;
            mid_prices.push(price_numeric);
            // Convert ms to microseconds for PG TIMESTAMPTZ via to_timestamp()
            timestamps.push(p.timestamp_ms);
        }

        sqlx::query(
            r#"INSERT INTO hip4_prices (coin, mid_price, timestamp)
               SELECT c, p, to_timestamp(t::DOUBLE PRECISION / 1000.0)
               FROM UNNEST($1::TEXT[], $2::NUMERIC[], $3::BIGINT[]) AS t(c, p, t)
               ON CONFLICT (coin, timestamp) DO NOTHING"#,
        )
        .bind(&coins)
        .bind(&mid_prices)
        .bind(&timestamps)
        .execute(pool)
        .await
        .map_err(|e| eyre::eyre!("Failed to insert hip4_prices: {e}"))?;

        Ok(())
    }

    /// Batch-insert HIP4 market snapshots using UNNEST.
    async fn insert_hip4_market_snapshots_pg(
        pool: &PgPool,
        snapshots: &[Hip4MarketSnapshotRow],
    ) -> eyre::Result<()> {
        if snapshots.is_empty() {
            return Ok(());
        }

        let len = snapshots.len();
        let mut coins: Vec<String> = Vec::with_capacity(len);
        let mut mark_pxs: Vec<Option<String>> = Vec::with_capacity(len);
        let mut mid_pxs: Vec<Option<String>> = Vec::with_capacity(len);
        let mut prev_day_pxs: Vec<Option<String>> = Vec::with_capacity(len);
        let mut day_ntl_vlms: Vec<Option<String>> = Vec::with_capacity(len);
        let mut day_base_vlms: Vec<Option<String>> = Vec::with_capacity(len);
        let mut circulating_supplies: Vec<Option<String>> = Vec::with_capacity(len);
        let mut total_supplies: Vec<Option<String>> = Vec::with_capacity(len);
        let mut timestamps: Vec<i64> = Vec::with_capacity(len);

        for s in snapshots {
            coins.push(s.coin.clone());
            mark_pxs.push(s.mark_px.clone());
            mid_pxs.push(s.mid_px.clone());
            prev_day_pxs.push(s.prev_day_px.clone());
            day_ntl_vlms.push(s.day_ntl_vlm.clone());
            day_base_vlms.push(s.day_base_vlm.clone());
            circulating_supplies.push(s.circulating_supply.clone());
            total_supplies.push(s.total_supply.clone());
            timestamps.push(s.timestamp_ms);
        }

        sqlx::query(
            r#"INSERT INTO hip4_market_snapshots (coin, mark_px, mid_px, prev_day_px, day_ntl_vlm,
                                                   day_base_vlm, circulating_supply, total_supply, timestamp)
               SELECT c, mp, mdp, pdp, dnv, dbv, cs, ts, to_timestamp(t::DOUBLE PRECISION / 1000.0)
               FROM UNNEST($1::TEXT[], $2::TEXT[], $3::TEXT[], $4::TEXT[], $5::TEXT[],
                           $6::TEXT[], $7::TEXT[], $8::TEXT[], $9::BIGINT[])
                    AS t(c, mp, mdp, pdp, dnv, dbv, cs, ts, t)
               ON CONFLICT (coin, timestamp) DO NOTHING"#,
        )
        .bind(&coins)
        .bind(&mark_pxs)
        .bind(&mid_pxs)
        .bind(&prev_day_pxs)
        .bind(&day_ntl_vlms)
        .bind(&day_base_vlms)
        .bind(&circulating_supplies)
        .bind(&total_supplies)
        .bind(&timestamps)
        .execute(pool)
        .await
        .map_err(|e| eyre::eyre!("Failed to insert hip4_market_snapshots: {e}"))?;

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

    async fn insert_hip4_data(&self, data: &Hip4BlockData) -> eyre::Result<()> {
        Self::insert_hip4_deposits(&self.pool, data).await?;
        Self::insert_hip4_claims(&self.pool, data).await?;
        Self::insert_hip4_contest_creations(&self.pool, data).await?;
        Self::insert_hip4_refunds(&self.pool, data).await?;
        Self::insert_hip4_sweeps(&self.pool, data).await?;
        Self::insert_hip4_merkle_claims(&self.pool, data).await?;
        Self::insert_hip4_finalizations(&self.pool, data).await?;
        Ok(())
    }

    async fn upsert_hip4_markets(&self, markets: &[Hip4Market]) -> eyre::Result<()> {
        Self::upsert_hip4_markets_pg(&self.pool, markets).await
    }

    async fn insert_hip4_prices(&self, prices: &[Hip4PriceRow]) -> eyre::Result<()> {
        Self::insert_hip4_prices_pg(&self.pool, prices).await
    }

    async fn insert_hip4_market_snapshots(
        &self,
        snapshots: &[Hip4MarketSnapshotRow],
    ) -> eyre::Result<()> {
        Self::insert_hip4_market_snapshots_pg(&self.pool, snapshots).await
    }

    async fn insert_fills(&self, fills: &[FillRecord]) -> eyre::Result<()> {
        if fills.is_empty() {
            return Ok(());
        }

        let len = fills.len();
        let mut trade_ids: Vec<i64> = Vec::with_capacity(len);
        let mut block_numbers: Vec<i64> = Vec::with_capacity(len);
        let mut block_times: Vec<String> = Vec::with_capacity(len);
        let mut user_addresses: Vec<String> = Vec::with_capacity(len);
        let mut coins: Vec<String> = Vec::with_capacity(len);
        let mut prices: Vec<sqlx::types::BigDecimal> = Vec::with_capacity(len);
        let mut sizes: Vec<sqlx::types::BigDecimal> = Vec::with_capacity(len);
        let mut sides: Vec<String> = Vec::with_capacity(len);
        let mut directions: Vec<String> = Vec::with_capacity(len);
        let mut closed_pnls: Vec<sqlx::types::BigDecimal> = Vec::with_capacity(len);
        let mut hashes: Vec<String> = Vec::with_capacity(len);
        let mut order_ids: Vec<i64> = Vec::with_capacity(len);
        let mut crosseds: Vec<bool> = Vec::with_capacity(len);
        let mut fees: Vec<sqlx::types::BigDecimal> = Vec::with_capacity(len);
        let mut fee_tokens: Vec<String> = Vec::with_capacity(len);
        let mut fill_times: Vec<i64> = Vec::with_capacity(len);

        for f in fills {
            trade_ids.push(f.trade_id);
            block_numbers.push(f.block_number);
            block_times.push(f.block_time.clone());
            user_addresses.push(f.user_address.clone());
            coins.push(f.coin.clone());
            prices.push(f.price.parse().map_err(|e| eyre::eyre!("Invalid price '{}': {e}", f.price))?);
            sizes.push(f.size.parse().map_err(|e| eyre::eyre!("Invalid size '{}': {e}", f.size))?);
            sides.push(f.side.clone());
            directions.push(f.direction.clone());
            closed_pnls.push(f.closed_pnl.parse().map_err(|e| eyre::eyre!("Invalid closed_pnl '{}': {e}", f.closed_pnl))?);
            hashes.push(f.hash.clone());
            order_ids.push(f.order_id);
            crosseds.push(f.crossed);
            fees.push(f.fee.parse().map_err(|e| eyre::eyre!("Invalid fee '{}': {e}", f.fee))?);
            fee_tokens.push(f.fee_token.clone());
            fill_times.push(f.fill_time);
        }

        sqlx::query(
            r#"INSERT INTO fills (trade_id, block_number, block_time, user_address, coin, price, size, side, direction, closed_pnl, hash, order_id, crossed, fee, fee_token, fill_time)
               SELECT * FROM UNNEST($1::BIGINT[], $2::BIGINT[], $3::TEXT[], $4::TEXT[], $5::TEXT[], $6::NUMERIC[], $7::NUMERIC[], $8::TEXT[], $9::TEXT[], $10::NUMERIC[], $11::TEXT[], $12::BIGINT[], $13::BOOLEAN[], $14::NUMERIC[], $15::TEXT[], $16::BIGINT[])
               ON CONFLICT (trade_id, user_address) DO NOTHING"#,
        )
        .bind(&trade_ids)
        .bind(&block_numbers)
        .bind(&block_times)
        .bind(&user_addresses)
        .bind(&coins)
        .bind(&prices)
        .bind(&sizes)
        .bind(&sides)
        .bind(&directions)
        .bind(&closed_pnls)
        .bind(&hashes)
        .bind(&order_ids)
        .bind(&crosseds)
        .bind(&fees)
        .bind(&fee_tokens)
        .bind(&fill_times)
        .execute(&self.pool)
        .await
        .map_err(|e| eyre::eyre!("Failed to batch insert fills: {e}"))?;

        Ok(())
    }

    async fn insert_hip4_trade_fills(&self, fills: &[&FillRecord]) -> eyre::Result<()> {
        if fills.is_empty() {
            return Ok(());
        }

        let len = fills.len();
        let mut trade_ids: Vec<i64> = Vec::with_capacity(len);
        let mut block_numbers: Vec<i64> = Vec::with_capacity(len);
        let mut block_times: Vec<String> = Vec::with_capacity(len);
        let mut user_addresses: Vec<String> = Vec::with_capacity(len);
        let mut coins: Vec<String> = Vec::with_capacity(len);
        let mut prices: Vec<sqlx::types::BigDecimal> = Vec::with_capacity(len);
        let mut sizes: Vec<sqlx::types::BigDecimal> = Vec::with_capacity(len);
        let mut sides: Vec<String> = Vec::with_capacity(len);
        let mut directions: Vec<String> = Vec::with_capacity(len);
        let mut closed_pnls: Vec<sqlx::types::BigDecimal> = Vec::with_capacity(len);
        let mut hashes: Vec<String> = Vec::with_capacity(len);
        let mut order_ids: Vec<i64> = Vec::with_capacity(len);
        let mut crosseds: Vec<bool> = Vec::with_capacity(len);
        let mut fees: Vec<sqlx::types::BigDecimal> = Vec::with_capacity(len);
        let mut fee_tokens: Vec<String> = Vec::with_capacity(len);
        let mut fill_times: Vec<i64> = Vec::with_capacity(len);

        for f in fills {
            trade_ids.push(f.trade_id);
            block_numbers.push(f.block_number);
            block_times.push(f.block_time.clone());
            user_addresses.push(f.user_address.clone());
            coins.push(f.coin.clone());
            prices.push(f.price.parse().map_err(|e| eyre::eyre!("Invalid price '{}': {e}", f.price))?);
            sizes.push(f.size.parse().map_err(|e| eyre::eyre!("Invalid size '{}': {e}", f.size))?);
            sides.push(f.side.clone());
            directions.push(f.direction.clone());
            closed_pnls.push(f.closed_pnl.parse().map_err(|e| eyre::eyre!("Invalid closed_pnl '{}': {e}", f.closed_pnl))?);
            hashes.push(f.hash.clone());
            order_ids.push(f.order_id);
            crosseds.push(f.crossed);
            fees.push(f.fee.parse().map_err(|e| eyre::eyre!("Invalid fee '{}': {e}", f.fee))?);
            fee_tokens.push(f.fee_token.clone());
            fill_times.push(f.fill_time);
        }

        sqlx::query(
            r#"INSERT INTO hip4_trades (trade_id, block_number, block_time, user_address, coin, price, size, side, direction, closed_pnl, hash, order_id, crossed, fee, fee_token, fill_time)
               SELECT * FROM UNNEST($1::BIGINT[], $2::BIGINT[], $3::TEXT[], $4::TEXT[], $5::TEXT[], $6::NUMERIC[], $7::NUMERIC[], $8::TEXT[], $9::TEXT[], $10::NUMERIC[], $11::TEXT[], $12::BIGINT[], $13::BOOLEAN[], $14::NUMERIC[], $15::TEXT[], $16::BIGINT[])
               ON CONFLICT (trade_id, user_address) DO NOTHING"#,
        )
        .bind(&trade_ids)
        .bind(&block_numbers)
        .bind(&block_times)
        .bind(&user_addresses)
        .bind(&coins)
        .bind(&prices)
        .bind(&sizes)
        .bind(&sides)
        .bind(&directions)
        .bind(&closed_pnls)
        .bind(&hashes)
        .bind(&order_ids)
        .bind(&crosseds)
        .bind(&fees)
        .bind(&fee_tokens)
        .bind(&fill_times)
        .execute(&self.pool)
        .await
        .map_err(|e| eyre::eyre!("Failed to batch insert hip4_trades: {e}"))?;

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
