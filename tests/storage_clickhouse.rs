//! Integration tests for ClickHouse storage.
//!
//! These tests require a running ClickHouse instance at:
//!   http://localhost:8123
//!
//! Start one with:
//!   docker run -d --name ch-test -p 8123:8123 clickhouse/clickhouse-server:24.3
//!
//! Run with: DATABASE_URL=http://localhost:8123 cargo test --test storage_clickhouse -- --ignored --test-threads=1

use hypercore_indexer::decode;
use hypercore_indexer::s3::codec;
use hypercore_indexer::storage::clickhouse::ClickHouseStorage;
use hypercore_indexer::storage::Storage;

/// Uses port 8124 (test compose) to avoid conflicting with dev on 8123.
/// Override with CLICKHOUSE_URL env var if needed.
fn clickhouse_url() -> String {
    std::env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://localhost:8124".to_string())
}

fn load_fixture(name: &str) -> Vec<u8> {
    let path = format!("{}/tests/fixtures/{name}", env!("CARGO_MANIFEST_DIR"));
    std::fs::read(&path).unwrap_or_else(|e| panic!("Failed to read fixture {path}: {e}"))
}

fn decode_fixture(name: &str, chain_id: u64) -> decode::types::DecodedBlock {
    let compressed = load_fixture(name);
    let raw = codec::decode_block(&compressed).unwrap();
    decode::decode_block(&raw, chain_id).unwrap()
}

async fn setup() -> ClickHouseStorage {
    let ch = ClickHouseStorage::connect(&clickhouse_url()).await.unwrap();
    ch.ensure_schema().await.unwrap();

    // Clean test data — ClickHouse uses ALTER TABLE DELETE (async)
    // then OPTIMIZE to force merge
    let client = ch.client();
    for table in &[
        "event_logs",
        "transactions",
        "system_transfers",
        "blocks",
        "indexer_cursor",
    ] {
        client
            .query(&format!("TRUNCATE TABLE IF EXISTS {table}"))
            .execute()
            .await
            .unwrap();
    }
    ch
}

// ============================================================================
// Block insert + query with pinned values
// ============================================================================

#[tokio::test]
#[ignore]
async fn insert_and_query_block() {
    let ch = setup().await;
    let block = decode_fixture("block_5000038.rmp.lz4", 999);

    ch.insert_block(&block).await.unwrap();

    let row = ch.client()
        .query("SELECT block_number, timestamp, gas_used, gas_limit, tx_count, system_tx_count FROM blocks WHERE block_number = ?")
        .bind(5_000_038u64)
        .fetch_one::<(u64, u64, u64, u64, u32, u32)>()
        .await
        .unwrap();

    assert_eq!(row.0, 5_000_038);
    assert_eq!(row.1, 1_749_160_149); // pinned timestamp
    assert_eq!(row.2, 1_722_800); // pinned gas_used
    assert_eq!(row.3, 2_000_000); // pinned gas_limit
    assert_eq!(row.4, 8); // 8 txs
    assert_eq!(row.5, 1); // 1 system transfer
}

// ============================================================================
// Transaction insert + query
// ============================================================================

#[tokio::test]
#[ignore]
async fn insert_and_query_transactions() {
    let ch = setup().await;
    let block = decode_fixture("block_5000038.rmp.lz4", 999);

    ch.insert_block(&block).await.unwrap();

    // Query tx count
    let count = ch
        .client()
        .query("SELECT count() FROM transactions WHERE block_number = ?")
        .bind(5_000_038u64)
        .fetch_one::<u64>()
        .await
        .unwrap();
    assert_eq!(count, 8);

    // Query first tx by index — verify pinned gas values
    let row = ch.client()
        .query("SELECT tx_type, gas_used, success FROM transactions WHERE block_number = ? AND tx_index = 0")
        .bind(5_000_038u64)
        .fetch_one::<(u8, u64, u8)>()
        .await
        .unwrap();
    assert_eq!(row.0, 2); // Eip1559
    assert_eq!(row.1, 172_182); // pinned gas_used
    assert_eq!(row.2, 1); // success = true

    // Query reverted tx (index 3)
    let row = ch
        .client()
        .query("SELECT success FROM transactions WHERE block_number = ? AND tx_index = 3")
        .bind(5_000_038u64)
        .fetch_one::<u8>()
        .await
        .unwrap();
    assert_eq!(row, 0); // reverted
}

// ============================================================================
// System transfers with dual hash
// ============================================================================

#[tokio::test]
#[ignore]
async fn insert_and_query_system_transfers() {
    let ch = setup().await;
    let block = decode_fixture("block_5000038.rmp.lz4", 999);

    ch.insert_block(&block).await.unwrap();

    let stx = &block.system_transfers[0];

    // Query by recipient
    let row = ch.client()
        .query("SELECT block_number, official_hash, explorer_hash, asset_type FROM system_transfers WHERE recipient = ?")
        .bind(format!("0x{}", hex::encode(stx.recipient.as_slice())))
        .fetch_one::<(u64, String, String, String)>()
        .await
        .unwrap();

    assert_eq!(row.0, 5_000_038);
    assert_eq!(row.3, "SpotToken");
    // Dual hashes are different
    assert_ne!(row.1, row.2, "official and explorer hashes must differ");

    // Query by official hash
    let count = ch
        .client()
        .query("SELECT count() FROM system_transfers WHERE official_hash = ?")
        .bind(&row.1)
        .fetch_one::<u64>()
        .await
        .unwrap();
    assert_eq!(count, 1);

    // Query by explorer hash
    let count = ch
        .client()
        .query("SELECT count() FROM system_transfers WHERE explorer_hash = ?")
        .bind(&row.2)
        .fetch_one::<u64>()
        .await
        .unwrap();
    assert_eq!(count, 1);
}

// ============================================================================
// Event logs
// ============================================================================

#[tokio::test]
#[ignore]
async fn insert_and_query_event_logs() {
    let ch = setup().await;
    let block = decode_fixture("block_5000038.rmp.lz4", 999);

    ch.insert_block(&block).await.unwrap();

    let total = ch
        .client()
        .query("SELECT count() FROM event_logs WHERE block_number = ?")
        .bind(5_000_038u64)
        .fetch_one::<u64>()
        .await
        .unwrap();
    assert!(total > 0, "expected event logs");

    // First log is a Transfer event at log_index 0
    let row = ch.client()
        .query("SELECT tx_index, log_index, address FROM event_logs WHERE block_number = ? AND log_index = 0")
        .bind(5_000_038u64)
        .fetch_one::<(u32, u32, String)>()
        .await
        .unwrap();
    assert_eq!(row.0, 0); // tx_index
    assert_eq!(row.1, 0); // log_index
    assert!(!row.2.is_empty());
}

// ============================================================================
// Cursor
// ============================================================================

#[tokio::test]
#[ignore]
async fn cursor_round_trip() {
    let ch = setup().await;

    assert_eq!(ch.get_cursor("mainnet").await.unwrap(), None);

    ch.set_cursor("mainnet", 5_000_038).await.unwrap();
    assert_eq!(ch.get_cursor("mainnet").await.unwrap(), Some(5_000_038));

    ch.set_cursor("mainnet", 5_000_100).await.unwrap();
    assert_eq!(ch.get_cursor("mainnet").await.unwrap(), Some(5_000_100));

    assert_eq!(ch.get_cursor("testnet").await.unwrap(), None);
}

// ============================================================================
// Idempotent insert (ReplacingMergeTree handles dedup)
// ============================================================================

#[tokio::test]
#[ignore]
async fn idempotent_insert() {
    let ch = setup().await;
    let block = decode_fixture("block_5000038.rmp.lz4", 999);

    ch.insert_block(&block).await.unwrap();
    ch.insert_block(&block).await.unwrap(); // no error

    // ReplacingMergeTree deduplicates on OPTIMIZE FINAL
    ch.client()
        .query("OPTIMIZE TABLE blocks FINAL")
        .execute()
        .await
        .unwrap();
    ch.client()
        .query("OPTIMIZE TABLE transactions FINAL")
        .execute()
        .await
        .unwrap();

    let count = ch
        .client()
        .query("SELECT count() FROM blocks FINAL WHERE block_number = ?")
        .bind(5_000_038u64)
        .fetch_one::<u64>()
        .await
        .unwrap();
    assert_eq!(count, 1);
}

// ============================================================================
// Batch insert
// ============================================================================

#[tokio::test]
#[ignore]
async fn batch_insert() {
    let ch = setup().await;

    let block1 = decode_fixture("block_1.rmp.lz4", 999);
    let block2 = decode_fixture("block_5000038.rmp.lz4", 999);
    let block3 = decode_fixture("block_testnet_48186001.rmp.lz4", 998);

    ch.insert_batch(&[block1, block2, block3]).await.unwrap();

    let count = ch
        .client()
        .query("SELECT count() FROM blocks")
        .fetch_one::<u64>()
        .await
        .unwrap();
    assert_eq!(count, 3);

    let tx_count = ch
        .client()
        .query("SELECT count() FROM transactions")
        .fetch_one::<u64>()
        .await
        .unwrap();
    assert_eq!(tx_count, 8);
}

// ============================================================================
// Atomic batch + cursor
// ============================================================================

#[tokio::test]
#[ignore]
async fn batch_insert_with_cursor() {
    let ch = setup().await;

    let block1 = decode_fixture("block_1.rmp.lz4", 999);
    let block2 = decode_fixture("block_5000038.rmp.lz4", 999);

    ch.insert_batch_and_set_cursor(&[block1, block2], "mainnet", 5_000_038)
        .await
        .unwrap();

    let count = ch
        .client()
        .query("SELECT count() FROM blocks")
        .fetch_one::<u64>()
        .await
        .unwrap();
    assert_eq!(count, 2);

    assert_eq!(ch.get_cursor("mainnet").await.unwrap(), Some(5_000_038));
}
