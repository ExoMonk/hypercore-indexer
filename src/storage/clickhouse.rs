use clickhouse::Client;
use serde::Serialize;
use tracing::info;

use crate::decode::types::{AssetType, DecodedBlock, TxType};

use super::Storage;

const SCHEMA_SQL: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS blocks (
        block_number    UInt64,
        block_hash      String,
        parent_hash     String,
        timestamp       UInt64,
        gas_used        UInt64,
        gas_limit       UInt64,
        base_fee_per_gas Nullable(UInt64),
        tx_count        UInt32,
        system_tx_count UInt32,
        created_at      DateTime DEFAULT now()
    ) ENGINE = ReplacingMergeTree()
    ORDER BY block_number",
    "CREATE TABLE IF NOT EXISTS transactions (
        block_number    UInt64,
        tx_index        UInt32,
        tx_hash         String,
        tx_type         UInt8,
        from_addr       String DEFAULT '',
        to_addr         String DEFAULT '',
        value           String,
        input           String,
        gas_limit       UInt64,
        gas_used        UInt64,
        success         UInt8
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (block_number, tx_index)",
    "CREATE TABLE IF NOT EXISTS system_transfers (
        block_number    UInt64,
        tx_index        UInt32,
        official_hash   String,
        explorer_hash   String,
        system_address  String,
        asset_type      String,
        asset_index     Nullable(UInt16),
        recipient       String,
        amount_wei      String
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (block_number, tx_index)",
    "CREATE TABLE IF NOT EXISTS event_logs (
        block_number    UInt64,
        tx_index        UInt32,
        log_index       UInt32,
        address         String,
        topic0          String DEFAULT '',
        topic1          String DEFAULT '',
        topic2          String DEFAULT '',
        topic3          String DEFAULT '',
        data            String
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (block_number, tx_index, log_index)",
    "CREATE TABLE IF NOT EXISTS indexer_cursor (
        network         String,
        last_block      UInt64,
        updated_at      DateTime DEFAULT now()
    ) ENGINE = ReplacingMergeTree()
    ORDER BY network",
];

// Row types for the clickhouse crate's insert API.
// All binary data stored as hex strings for simplicity.

#[derive(Debug, Serialize, clickhouse::Row)]
struct BlockRow {
    block_number: u64,
    block_hash: String,
    parent_hash: String,
    timestamp: u64,
    gas_used: u64,
    gas_limit: u64,
    base_fee_per_gas: Option<u64>,
    tx_count: u32,
    system_tx_count: u32,
}

#[derive(Debug, Serialize, clickhouse::Row)]
struct TxRow {
    block_number: u64,
    tx_index: u32,
    tx_hash: String,
    tx_type: u8,
    from_addr: String,
    to_addr: String,
    value: String,
    input: String,
    gas_limit: u64,
    gas_used: u64,
    success: u8,
}

#[derive(Debug, Serialize, clickhouse::Row)]
struct SystemTransferRow {
    block_number: u64,
    tx_index: u32,
    official_hash: String,
    explorer_hash: String,
    system_address: String,
    asset_type: String,
    asset_index: Option<u16>,
    recipient: String,
    amount_wei: String,
}

#[derive(Debug, Serialize, clickhouse::Row)]
struct EventLogRow {
    block_number: u64,
    tx_index: u32,
    log_index: u32,
    address: String,
    topic0: String,
    topic1: String,
    topic2: String,
    topic3: String,
    data: String,
}

#[derive(Debug, Serialize, clickhouse::Row)]
struct CursorRow {
    network: String,
    last_block: u64,
}

fn to_hex(bytes: &[u8]) -> String {
    format!("0x{}", hex::encode(bytes))
}

pub struct ClickHouseStorage {
    client: Client,
}

impl ClickHouseStorage {
    pub async fn connect(url: &str) -> eyre::Result<Self> {
        let client = Client::default().with_url(url);

        client
            .query("SELECT 1")
            .execute()
            .await
            .map_err(|e| eyre::eyre!("Failed to connect to ClickHouse at {url}: {e}"))?;

        info!("Connected to ClickHouse");
        Ok(Self { client })
    }

    /// Used by tests and for direct queries.
    #[allow(dead_code)]
    pub fn client(&self) -> &Client {
        &self.client
    }

    pub async fn ensure_schema(&self) -> eyre::Result<()> {
        for ddl in SCHEMA_SQL {
            self.client
                .query(ddl)
                .execute()
                .await
                .map_err(|e| eyre::eyre!("Failed to execute DDL: {e}"))?;
        }
        info!("ClickHouse schema ensured");
        Ok(())
    }

    async fn insert_blocks(&self, blocks: &[DecodedBlock]) -> eyre::Result<()> {
        let mut insert = self
            .client
            .insert::<BlockRow>("blocks")
            .await
            .map_err(|e| eyre::eyre!("Failed to create block inserter: {e}"))?;

        for block in blocks {
            insert
                .write(&BlockRow {
                    block_number: block.number,
                    block_hash: to_hex(block.hash.as_slice()),
                    parent_hash: to_hex(block.parent_hash.as_slice()),
                    timestamp: block.timestamp,
                    gas_used: block.gas_used,
                    gas_limit: block.gas_limit,
                    base_fee_per_gas: block.base_fee_per_gas,
                    tx_count: block.transactions.len() as u32,
                    system_tx_count: block.system_transfers.len() as u32,
                })
                .await
                .map_err(|e| eyre::eyre!("Failed to write block row: {e}"))?;
        }

        insert
            .end()
            .await
            .map_err(|e| eyre::eyre!("Failed to flush block insert: {e}"))?;
        Ok(())
    }

    async fn insert_transactions(&self, blocks: &[DecodedBlock]) -> eyre::Result<()> {
        let total: usize = blocks.iter().map(|b| b.transactions.len()).sum();
        if total == 0 {
            return Ok(());
        }

        let mut insert = self
            .client
            .insert::<TxRow>("transactions")
            .await
            .map_err(|e| eyre::eyre!("Failed to create tx inserter: {e}"))?;

        for block in blocks {
            for dtx in &block.transactions {
                insert
                    .write(&TxRow {
                        block_number: block.number,
                        tx_index: dtx.tx_index as u32,
                        tx_hash: to_hex(dtx.hash.as_slice()),
                        tx_type: match dtx.tx_type {
                            TxType::Legacy => 0,
                            TxType::Eip2930 => 1,
                            TxType::Eip1559 => 2,
                        },
                        from_addr: dtx.from.map(|a| to_hex(a.as_slice())).unwrap_or_default(),
                        to_addr: dtx.to.map(|a| to_hex(a.as_slice())).unwrap_or_default(),
                        value: dtx.value.to_string(),
                        input: hex::encode(&dtx.input),
                        gas_limit: dtx.gas_limit,
                        gas_used: dtx.gas_used,
                        success: dtx.success as u8,
                    })
                    .await
                    .map_err(|e| eyre::eyre!("Failed to write tx row: {e}"))?;
            }
        }

        insert
            .end()
            .await
            .map_err(|e| eyre::eyre!("Failed to flush tx insert: {e}"))?;
        Ok(())
    }

    async fn insert_event_logs(&self, blocks: &[DecodedBlock]) -> eyre::Result<()> {
        let total: usize = blocks
            .iter()
            .flat_map(|b| b.transactions.iter())
            .map(|t| t.logs.len())
            .sum();
        if total == 0 {
            return Ok(());
        }

        let mut insert = self
            .client
            .insert::<EventLogRow>("event_logs")
            .await
            .map_err(|e| eyre::eyre!("Failed to create log inserter: {e}"))?;

        for block in blocks {
            for dtx in &block.transactions {
                for log in &dtx.logs {
                    insert
                        .write(&EventLogRow {
                            block_number: block.number,
                            tx_index: dtx.tx_index as u32,
                            log_index: log.log_index as u32,
                            address: to_hex(log.address.as_slice()),
                            topic0: log
                                .topics
                                .first()
                                .map(|t| to_hex(t.as_slice()))
                                .unwrap_or_default(),
                            topic1: log
                                .topics
                                .get(1)
                                .map(|t| to_hex(t.as_slice()))
                                .unwrap_or_default(),
                            topic2: log
                                .topics
                                .get(2)
                                .map(|t| to_hex(t.as_slice()))
                                .unwrap_or_default(),
                            topic3: log
                                .topics
                                .get(3)
                                .map(|t| to_hex(t.as_slice()))
                                .unwrap_or_default(),
                            data: hex::encode(&log.data),
                        })
                        .await
                        .map_err(|e| eyre::eyre!("Failed to write log row: {e}"))?;
                }
            }
        }

        insert
            .end()
            .await
            .map_err(|e| eyre::eyre!("Failed to flush log insert: {e}"))?;
        Ok(())
    }

    async fn insert_system_transfers(&self, blocks: &[DecodedBlock]) -> eyre::Result<()> {
        let total: usize = blocks.iter().map(|b| b.system_transfers.len()).sum();
        if total == 0 {
            return Ok(());
        }

        let mut insert = self
            .client
            .insert::<SystemTransferRow>("system_transfers")
            .await
            .map_err(|e| eyre::eyre!("Failed to create system_transfer inserter: {e}"))?;

        for block in blocks {
            for (i, stx) in block.system_transfers.iter().enumerate() {
                let (asset_type_str, asset_index) = match &stx.asset_type {
                    AssetType::NativeHype => ("NativeHype", None),
                    AssetType::SpotToken { asset_index } => ("SpotToken", Some(*asset_index)),
                };

                insert
                    .write(&SystemTransferRow {
                        block_number: block.number,
                        tx_index: i as u32,
                        official_hash: to_hex(stx.official_hash.as_slice()),
                        explorer_hash: to_hex(stx.explorer_hash.as_slice()),
                        system_address: to_hex(stx.system_address.as_slice()),
                        asset_type: asset_type_str.to_string(),
                        asset_index,
                        recipient: to_hex(stx.recipient.as_slice()),
                        amount_wei: stx.amount_wei.to_string(),
                    })
                    .await
                    .map_err(|e| eyre::eyre!("Failed to write system_transfer row: {e}"))?;
            }
        }

        insert
            .end()
            .await
            .map_err(|e| eyre::eyre!("Failed to flush system_transfer insert: {e}"))?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl Storage for ClickHouseStorage {
    async fn insert_block(&self, block: &DecodedBlock) -> eyre::Result<()> {
        self.insert_batch(std::slice::from_ref(block)).await
    }

    async fn insert_batch(&self, blocks: &[DecodedBlock]) -> eyre::Result<()> {
        self.insert_blocks(blocks).await?;
        self.insert_transactions(blocks).await?;
        self.insert_event_logs(blocks).await?;
        self.insert_system_transfers(blocks).await?;
        Ok(())
    }

    /// NOTE: ClickHouse has no multi-table transactions. Data and cursor are written
    /// as separate operations. If the process crashes after `insert_batch` but before
    /// `set_cursor`, blocks will be re-processed on restart (at-least-once semantics).
    /// ReplacingMergeTree deduplicates these on merge, so no data is lost or corrupted.
    async fn insert_batch_and_set_cursor(
        &self,
        blocks: &[DecodedBlock],
        network: &str,
        block_number: u64,
    ) -> eyre::Result<()> {
        self.insert_batch(blocks).await?;
        self.set_cursor(network, block_number).await?;
        Ok(())
    }

    async fn get_cursor(&self, network: &str) -> eyre::Result<Option<u64>> {
        let result = self
            .client
            .query("SELECT last_block FROM indexer_cursor FINAL WHERE network = ?")
            .bind(network)
            .fetch_optional::<u64>()
            .await
            .map_err(|e| eyre::eyre!("Failed to get cursor: {e}"))?;

        Ok(result)
    }

    async fn set_cursor(&self, network: &str, block_number: u64) -> eyre::Result<()> {
        let mut insert = self
            .client
            .insert::<CursorRow>("indexer_cursor")
            .await
            .map_err(|e| eyre::eyre!("Failed to create cursor inserter: {e}"))?;

        insert
            .write(&CursorRow {
                network: network.to_string(),
                last_block: block_number,
            })
            .await
            .map_err(|e| eyre::eyre!("Failed to write cursor: {e}"))?;

        insert
            .end()
            .await
            .map_err(|e| eyre::eyre!("Failed to flush cursor: {e}"))?;
        Ok(())
    }
}
