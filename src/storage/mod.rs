pub mod clickhouse;
pub mod postgres;
pub mod sqlite;

use crate::decode::types::DecodedBlock;
use crate::hip4::types::{Hip4BlockData, Hip4Market, Hip4PriceRow};

#[async_trait::async_trait]
#[allow(dead_code)]
pub trait Storage: Send + Sync {
    /// Insert a single decoded block with all its transactions, system transfers, and logs.
    async fn insert_block(&self, block: &DecodedBlock) -> eyre::Result<()>;

    /// Insert a batch of decoded blocks in a single transaction.
    async fn insert_batch(&self, blocks: &[DecodedBlock]) -> eyre::Result<()>;

    /// Insert a batch of decoded blocks and update the cursor atomically in one transaction.
    async fn insert_batch_and_set_cursor(
        &self,
        blocks: &[DecodedBlock],
        network: &str,
        block_number: u64,
    ) -> eyre::Result<()>;

    /// Get the last indexed block number for a network.
    async fn get_cursor(&self, network: &str) -> eyre::Result<Option<u64>>;

    /// Set the cursor for a network.
    async fn set_cursor(&self, network: &str, block_number: u64) -> eyre::Result<()>;

    /// Insert HIP4 decoded contest events (deposits and claims).
    async fn insert_hip4_data(&self, data: &Hip4BlockData) -> eyre::Result<()>;

    /// Upsert HIP4 market metadata from the outcomeMeta API.
    /// On conflict (outcome_id), updates name, description, side_specs, question fields.
    async fn upsert_hip4_markets(&self, markets: &[Hip4Market]) -> eyre::Result<()>;

    /// Insert HIP4 price snapshots from the allMids API.
    /// On conflict (coin, timestamp), does nothing (idempotent).
    async fn insert_hip4_prices(&self, prices: &[Hip4PriceRow]) -> eyre::Result<()>;
}
