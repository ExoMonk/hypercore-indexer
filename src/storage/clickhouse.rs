use clickhouse::Client;
use serde::Serialize;
use tracing::info;

use crate::decode::types::{AssetType, DecodedBlock, TxType};
use crate::fills::types::FillRecord;
use crate::hip4::types::{Hip4BlockData, Hip4Market, Hip4PriceRow};

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
    "CREATE TABLE IF NOT EXISTS hip4_deposits (
        block_number    UInt64,
        tx_index        UInt32,
        log_index       UInt32,
        contest_id      UInt64,
        side_id         UInt64,
        depositor       String,
        amount_wei      String
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (block_number, tx_index, log_index)",
    "CREATE TABLE IF NOT EXISTS hip4_claims (
        block_number    UInt64,
        tx_index        UInt32,
        log_index       UInt32,
        contest_id      UInt64,
        side_id         UInt64,
        claimer         String,
        amount_wei      String
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (block_number, tx_index, log_index)",
    "CREATE TABLE IF NOT EXISTS hip4_contest_creations (
        block_number    UInt64,
        tx_index        UInt32,
        contest_id      UInt64,
        param2          UInt64
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (block_number, tx_index)",
    "CREATE TABLE IF NOT EXISTS hip4_refunds (
        block_number    UInt64,
        tx_index        UInt32,
        contest_id      UInt64,
        side_id         UInt64,
        user_address    String
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (block_number, tx_index)",
    "CREATE TABLE IF NOT EXISTS hip4_sweeps (
        block_number    UInt64,
        tx_index        UInt32,
        contest_id      UInt64
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (block_number, tx_index)",
    "CREATE TABLE IF NOT EXISTS hip4_merkle_claims (
        block_number    UInt64,
        tx_index        UInt32,
        contest_id      UInt64,
        side_id         UInt64,
        user_address    String,
        amount_wei      String,
        proof_length    UInt32
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (block_number, tx_index)",
    "CREATE TABLE IF NOT EXISTS hip4_finalizations (
        block_number    UInt64,
        tx_index        UInt32,
        contest_id      UInt64
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (block_number, tx_index)",
    "CREATE TABLE IF NOT EXISTS hip4_markets (
        outcome_id      UInt32,
        name            String,
        description     String,
        side_specs      String,
        question_id     Nullable(UInt32),
        question_name   Nullable(String),
        desc_class      Nullable(String),
        desc_underlying Nullable(String),
        desc_expiry     Nullable(String),
        desc_target_price Nullable(String),
        desc_period     Nullable(String),
        updated_at      DateTime DEFAULT now()
    ) ENGINE = ReplacingMergeTree(updated_at)
    ORDER BY outcome_id",
    "CREATE TABLE IF NOT EXISTS hip4_prices (
        coin            String,
        mid_price       String,
        timestamp       DateTime64(3),
        _dummy          UInt8 DEFAULT 0
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (coin, timestamp)",
    "CREATE TABLE IF NOT EXISTS fills (
        trade_id        Int64,
        block_number    Int64,
        block_time      String,
        user_address    String,
        coin            String,
        price           String,
        size            String,
        side            String,
        direction       String,
        closed_pnl      String,
        hash            String,
        order_id        Int64,
        crossed         UInt8,
        fee             String,
        fee_token       String,
        fill_time       Int64
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (trade_id, user_address)",
    "CREATE TABLE IF NOT EXISTS hip4_trades (
        trade_id        Int64,
        block_number    Int64,
        block_time      String,
        user_address    String,
        coin            String,
        price           String,
        size            String,
        side            String,
        direction       String,
        closed_pnl      String,
        hash            String,
        order_id        Int64,
        crossed         UInt8,
        fee             String,
        fee_token       String,
        fill_time       Int64
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (trade_id, user_address)",
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
struct Hip4DepositRow {
    block_number: u64,
    tx_index: u32,
    log_index: u32,
    contest_id: u64,
    side_id: u64,
    depositor: String,
    amount_wei: String,
}

#[derive(Debug, Serialize, clickhouse::Row)]
struct Hip4ClaimRow {
    block_number: u64,
    tx_index: u32,
    log_index: u32,
    contest_id: u64,
    side_id: u64,
    claimer: String,
    amount_wei: String,
}

#[derive(Debug, Serialize, clickhouse::Row)]
struct Hip4ContestCreationRow {
    block_number: u64,
    tx_index: u32,
    contest_id: u64,
    param2: u64,
}

#[derive(Debug, Serialize, clickhouse::Row)]
struct Hip4RefundRow {
    block_number: u64,
    tx_index: u32,
    contest_id: u64,
    side_id: u64,
    user_address: String,
}

#[derive(Debug, Serialize, clickhouse::Row)]
struct Hip4SweepRow {
    block_number: u64,
    tx_index: u32,
    contest_id: u64,
}

#[derive(Debug, Serialize, clickhouse::Row)]
struct Hip4MerkleClaimRow {
    block_number: u64,
    tx_index: u32,
    contest_id: u64,
    side_id: u64,
    user_address: String,
    amount_wei: String,
    proof_length: u32,
}

#[derive(Debug, Serialize, clickhouse::Row)]
struct Hip4FinalizationRow {
    block_number: u64,
    tx_index: u32,
    contest_id: u64,
}

#[derive(Debug, Serialize, clickhouse::Row)]
struct Hip4MarketRow {
    outcome_id: u32,
    name: String,
    description: String,
    side_specs: String,
    question_id: Option<u32>,
    question_name: Option<String>,
    desc_class: Option<String>,
    desc_underlying: Option<String>,
    desc_expiry: Option<String>,
    desc_target_price: Option<String>,
    desc_period: Option<String>,
}

#[derive(Debug, Serialize, clickhouse::Row)]
struct Hip4PriceChRow {
    coin: String,
    mid_price: String,
    #[serde(with = "clickhouse::serde::time::datetime64::millis")]
    timestamp: time::OffsetDateTime,
}

#[derive(Debug, Serialize, clickhouse::Row)]
struct FillChRow {
    trade_id: i64,
    block_number: i64,
    block_time: String,
    user_address: String,
    coin: String,
    price: String,
    size: String,
    side: String,
    direction: String,
    closed_pnl: String,
    hash: String,
    order_id: i64,
    crossed: u8,
    fee: String,
    fee_token: String,
    fill_time: i64,
}

impl FillChRow {
    fn from_record(f: &FillRecord) -> Self {
        Self {
            trade_id: f.trade_id,
            block_number: f.block_number,
            block_time: f.block_time.clone(),
            user_address: f.user_address.clone(),
            coin: f.coin.clone(),
            price: f.price.clone(),
            size: f.size.clone(),
            side: f.side.clone(),
            direction: f.direction.clone(),
            closed_pnl: f.closed_pnl.clone(),
            hash: f.hash.clone(),
            order_id: f.order_id,
            crossed: f.crossed as u8,
            fee: f.fee.clone(),
            fee_token: f.fee_token.clone(),
            fill_time: f.fill_time,
        }
    }
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

    async fn insert_hip4_deposits_ch(&self, data: &Hip4BlockData) -> eyre::Result<()> {
        if data.deposits.is_empty() {
            return Ok(());
        }

        let mut insert = self
            .client
            .insert::<Hip4DepositRow>("hip4_deposits")
            .await
            .map_err(|e| eyre::eyre!("Failed to create hip4_deposits inserter: {e}"))?;

        for d in &data.deposits {
            insert
                .write(&Hip4DepositRow {
                    block_number: d.block_number,
                    tx_index: d.tx_index as u32,
                    log_index: d.log_index as u32,
                    contest_id: d.contest_id,
                    side_id: d.side_id,
                    depositor: to_hex(d.depositor.as_slice()),
                    amount_wei: d.amount_wei.to_string(),
                })
                .await
                .map_err(|e| eyre::eyre!("Failed to write hip4_deposit row: {e}"))?;
        }

        insert
            .end()
            .await
            .map_err(|e| eyre::eyre!("Failed to flush hip4_deposits insert: {e}"))?;
        Ok(())
    }

    async fn insert_hip4_claims_ch(&self, data: &Hip4BlockData) -> eyre::Result<()> {
        if data.claims.is_empty() {
            return Ok(());
        }

        let mut insert = self
            .client
            .insert::<Hip4ClaimRow>("hip4_claims")
            .await
            .map_err(|e| eyre::eyre!("Failed to create hip4_claims inserter: {e}"))?;

        for c in &data.claims {
            insert
                .write(&Hip4ClaimRow {
                    block_number: c.block_number,
                    tx_index: c.tx_index as u32,
                    log_index: c.log_index as u32,
                    contest_id: c.contest_id,
                    side_id: c.side_id,
                    claimer: to_hex(c.claimer.as_slice()),
                    amount_wei: c.amount_wei.to_string(),
                })
                .await
                .map_err(|e| eyre::eyre!("Failed to write hip4_claim row: {e}"))?;
        }

        insert
            .end()
            .await
            .map_err(|e| eyre::eyre!("Failed to flush hip4_claims insert: {e}"))?;
        Ok(())
    }

    async fn insert_hip4_contest_creations_ch(&self, data: &Hip4BlockData) -> eyre::Result<()> {
        if data.contest_creations.is_empty() {
            return Ok(());
        }

        let mut insert = self
            .client
            .insert::<Hip4ContestCreationRow>("hip4_contest_creations")
            .await
            .map_err(|e| eyre::eyre!("Failed to create hip4_contest_creations inserter: {e}"))?;

        for c in &data.contest_creations {
            insert
                .write(&Hip4ContestCreationRow {
                    block_number: c.block_number,
                    tx_index: c.tx_index as u32,
                    contest_id: c.contest_id,
                    param2: c.param2,
                })
                .await
                .map_err(|e| eyre::eyre!("Failed to write hip4_contest_creation row: {e}"))?;
        }

        insert
            .end()
            .await
            .map_err(|e| eyre::eyre!("Failed to flush hip4_contest_creations insert: {e}"))?;
        Ok(())
    }

    async fn insert_hip4_refunds_ch(&self, data: &Hip4BlockData) -> eyre::Result<()> {
        if data.refunds.is_empty() {
            return Ok(());
        }

        let mut insert = self
            .client
            .insert::<Hip4RefundRow>("hip4_refunds")
            .await
            .map_err(|e| eyre::eyre!("Failed to create hip4_refunds inserter: {e}"))?;

        for r in &data.refunds {
            insert
                .write(&Hip4RefundRow {
                    block_number: r.block_number,
                    tx_index: r.tx_index as u32,
                    contest_id: r.contest_id,
                    side_id: r.side_id,
                    user_address: to_hex(r.user.as_slice()),
                })
                .await
                .map_err(|e| eyre::eyre!("Failed to write hip4_refund row: {e}"))?;
        }

        insert
            .end()
            .await
            .map_err(|e| eyre::eyre!("Failed to flush hip4_refunds insert: {e}"))?;
        Ok(())
    }

    async fn insert_hip4_sweeps_ch(&self, data: &Hip4BlockData) -> eyre::Result<()> {
        if data.sweeps.is_empty() {
            return Ok(());
        }

        let mut insert = self
            .client
            .insert::<Hip4SweepRow>("hip4_sweeps")
            .await
            .map_err(|e| eyre::eyre!("Failed to create hip4_sweeps inserter: {e}"))?;

        for s in &data.sweeps {
            insert
                .write(&Hip4SweepRow {
                    block_number: s.block_number,
                    tx_index: s.tx_index as u32,
                    contest_id: s.contest_id,
                })
                .await
                .map_err(|e| eyre::eyre!("Failed to write hip4_sweep row: {e}"))?;
        }

        insert
            .end()
            .await
            .map_err(|e| eyre::eyre!("Failed to flush hip4_sweeps insert: {e}"))?;
        Ok(())
    }

    async fn insert_hip4_merkle_claims_ch(&self, data: &Hip4BlockData) -> eyre::Result<()> {
        if data.merkle_claims.is_empty() {
            return Ok(());
        }

        let mut insert = self
            .client
            .insert::<Hip4MerkleClaimRow>("hip4_merkle_claims")
            .await
            .map_err(|e| eyre::eyre!("Failed to create hip4_merkle_claims inserter: {e}"))?;

        for c in &data.merkle_claims {
            insert
                .write(&Hip4MerkleClaimRow {
                    block_number: c.block_number,
                    tx_index: c.tx_index as u32,
                    contest_id: c.contest_id,
                    side_id: c.side_id,
                    user_address: to_hex(c.user.as_slice()),
                    amount_wei: c.amount_wei.to_string(),
                    proof_length: c.proof_length,
                })
                .await
                .map_err(|e| eyre::eyre!("Failed to write hip4_merkle_claim row: {e}"))?;
        }

        insert
            .end()
            .await
            .map_err(|e| eyre::eyre!("Failed to flush hip4_merkle_claims insert: {e}"))?;
        Ok(())
    }

    async fn insert_hip4_finalizations_ch(&self, data: &Hip4BlockData) -> eyre::Result<()> {
        if data.finalizations.is_empty() {
            return Ok(());
        }

        let mut insert = self
            .client
            .insert::<Hip4FinalizationRow>("hip4_finalizations")
            .await
            .map_err(|e| eyre::eyre!("Failed to create hip4_finalizations inserter: {e}"))?;

        for f in &data.finalizations {
            insert
                .write(&Hip4FinalizationRow {
                    block_number: f.block_number,
                    tx_index: f.tx_index as u32,
                    contest_id: f.contest_id,
                })
                .await
                .map_err(|e| eyre::eyre!("Failed to write hip4_finalization row: {e}"))?;
        }

        insert
            .end()
            .await
            .map_err(|e| eyre::eyre!("Failed to flush hip4_finalizations insert: {e}"))?;
        Ok(())
    }

    async fn upsert_hip4_markets_ch(&self, markets: &[Hip4Market]) -> eyre::Result<()> {
        if markets.is_empty() {
            return Ok(());
        }

        let mut insert = self
            .client
            .insert::<Hip4MarketRow>("hip4_markets")
            .await
            .map_err(|e| eyre::eyre!("Failed to create hip4_markets inserter: {e}"))?;

        for m in markets {
            insert
                .write(&Hip4MarketRow {
                    outcome_id: m.outcome_id as u32,
                    name: m.name.clone(),
                    description: m.description.clone(),
                    side_specs: m.side_specs.clone(),
                    question_id: m.question_id.map(|v| v as u32),
                    question_name: m.question_name.clone(),
                    desc_class: m.parsed.class.clone(),
                    desc_underlying: m.parsed.underlying.clone(),
                    desc_expiry: m.parsed.expiry.clone(),
                    desc_target_price: m.parsed.target_price.clone(),
                    desc_period: m.parsed.period.clone(),
                })
                .await
                .map_err(|e| eyre::eyre!("Failed to write hip4_market row: {e}"))?;
        }

        insert
            .end()
            .await
            .map_err(|e| eyre::eyre!("Failed to flush hip4_markets insert: {e}"))?;
        Ok(())
    }

    async fn insert_hip4_prices_ch(&self, prices: &[Hip4PriceRow]) -> eyre::Result<()> {
        if prices.is_empty() {
            return Ok(());
        }

        let mut insert = self
            .client
            .insert::<Hip4PriceChRow>("hip4_prices")
            .await
            .map_err(|e| eyre::eyre!("Failed to create hip4_prices inserter: {e}"))?;

        for p in prices {
            let ts = time::OffsetDateTime::from_unix_timestamp_nanos(p.timestamp_ms as i128 * 1_000_000)
                .map_err(|e| eyre::eyre!("Invalid timestamp_ms {}: {e}", p.timestamp_ms))?;

            insert
                .write(&Hip4PriceChRow {
                    coin: p.coin.clone(),
                    mid_price: p.mid_price.clone(),
                    timestamp: ts,
                })
                .await
                .map_err(|e| eyre::eyre!("Failed to write hip4_price row: {e}"))?;
        }

        insert
            .end()
            .await
            .map_err(|e| eyre::eyre!("Failed to flush hip4_prices insert: {e}"))?;
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

    async fn insert_hip4_data(&self, data: &Hip4BlockData) -> eyre::Result<()> {
        self.insert_hip4_deposits_ch(data).await?;
        self.insert_hip4_claims_ch(data).await?;
        self.insert_hip4_contest_creations_ch(data).await?;
        self.insert_hip4_refunds_ch(data).await?;
        self.insert_hip4_sweeps_ch(data).await?;
        self.insert_hip4_merkle_claims_ch(data).await?;
        self.insert_hip4_finalizations_ch(data).await?;
        Ok(())
    }

    async fn upsert_hip4_markets(&self, markets: &[Hip4Market]) -> eyre::Result<()> {
        self.upsert_hip4_markets_ch(markets).await
    }

    async fn insert_hip4_prices(&self, prices: &[Hip4PriceRow]) -> eyre::Result<()> {
        self.insert_hip4_prices_ch(prices).await
    }

    async fn insert_fills(&self, fills: &[FillRecord]) -> eyre::Result<()> {
        if fills.is_empty() {
            return Ok(());
        }

        let mut insert = self
            .client
            .insert::<FillChRow>("fills")
            .await
            .map_err(|e| eyre::eyre!("Failed to create fills inserter: {e}"))?;

        for f in fills {
            insert
                .write(&FillChRow::from_record(f))
                .await
                .map_err(|e| eyre::eyre!("Failed to write fill row: {e}"))?;
        }

        insert
            .end()
            .await
            .map_err(|e| eyre::eyre!("Failed to flush fills insert: {e}"))?;
        Ok(())
    }

    async fn insert_hip4_trade_fills(&self, fills: &[&FillRecord]) -> eyre::Result<()> {
        if fills.is_empty() {
            return Ok(());
        }

        let mut insert = self
            .client
            .insert::<FillChRow>("hip4_trades")
            .await
            .map_err(|e| eyre::eyre!("Failed to create hip4_trades inserter: {e}"))?;

        for f in fills {
            insert
                .write(&FillChRow::from_record(f))
                .await
                .map_err(|e| eyre::eyre!("Failed to write hip4_trade row: {e}"))?;
        }

        insert
            .end()
            .await
            .map_err(|e| eyre::eyre!("Failed to flush hip4_trades insert: {e}"))?;
        Ok(())
    }
}
