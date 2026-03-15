//! Integration tests for SQLite storage.
//!
//! Uses in-memory SQLite — no external dependencies needed.
//! Run with: cargo test --test storage_sqlite

use hypercore_indexer::decode;
use hypercore_indexer::s3::codec;
use hypercore_indexer::storage::sqlite::SqliteStorage;
use hypercore_indexer::storage::Storage;

fn load_fixture(name: &str) -> Vec<u8> {
    let path = format!("{}/tests/fixtures/{name}", env!("CARGO_MANIFEST_DIR"));
    std::fs::read(&path).unwrap_or_else(|e| panic!("Failed to read fixture {path}: {e}"))
}

fn decode_fixture(name: &str, chain_id: u64) -> decode::types::DecodedBlock {
    let compressed = load_fixture(name);
    let raw = codec::decode_block(&compressed).unwrap();
    decode::decode_block(&raw, chain_id).unwrap()
}

async fn setup() -> SqliteStorage {
    let db = SqliteStorage::connect("sqlite::memory:").await.unwrap();
    db.ensure_schema().await.unwrap();
    db
}

// ============================================================================
// Block insert + query with pinned values
// ============================================================================

#[tokio::test]
async fn insert_and_query_block() {
    let db = setup().await;
    let block = decode_fixture("block_5000038.rmp.lz4", 999);

    db.insert_block(&block).await.unwrap();

    let row: (i64, Vec<u8>, Vec<u8>, i64, i64, i64, Option<i64>, i32, i32) = sqlx::query_as(
        "SELECT block_number, block_hash, parent_hash, timestamp, gas_used, gas_limit, base_fee_per_gas, tx_count, system_tx_count FROM blocks WHERE block_number = ?",
    )
    .bind(5_000_038i64)
    .fetch_one(db.pool())
    .await
    .unwrap();

    assert_eq!(row.0, 5_000_038);
    assert_eq!(row.1, block.hash.as_slice());
    assert_eq!(row.2, block.parent_hash.as_slice());
    assert_eq!(row.3, 1_749_160_149i64); // pinned timestamp
    assert_eq!(row.4, 1_722_800i64); // pinned gas_used
    assert_eq!(row.5, 2_000_000i64); // pinned gas_limit
    assert_eq!(row.6, Some(622_120_557i64)); // pinned base_fee
    assert_eq!(row.7, 8); // 8 transactions
    assert_eq!(row.8, 1); // 1 system transfer
}

// ============================================================================
// Transaction insert + query by hash with pinned values
// ============================================================================

#[tokio::test]
async fn insert_and_query_transactions() {
    let db = setup().await;
    let block = decode_fixture("block_5000038.rmp.lz4", 999);

    db.insert_block(&block).await.unwrap();

    // Query first tx by hash
    let tx0 = &block.transactions[0];
    let row: (i64, i32, Vec<u8>, i32, i64, i64, bool) = sqlx::query_as(
        r#"SELECT block_number, tx_index, tx_hash, tx_type, gas_limit, gas_used, success FROM transactions WHERE tx_hash = ?"#,
    )
    .bind(tx0.hash.as_slice())
    .fetch_one(db.pool())
    .await
    .unwrap();

    assert_eq!(row.0, 5_000_038);
    assert_eq!(row.1, 0);
    assert_eq!(row.2, tx0.hash.as_slice());
    assert_eq!(row.3, 2); // Eip1559
    assert_eq!(row.4, 750_000i64); // pinned gas_limit for tx0
    assert_eq!(row.5, 172_182i64); // pinned gas_used
    assert!(row.6); // success

    // Query reverted tx (index 3)
    let tx3 = &block.transactions[3];
    let row: (bool,) = sqlx::query_as("SELECT success FROM transactions WHERE tx_hash = ?")
        .bind(tx3.hash.as_slice())
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert!(!row.0, "tx 3 should be reverted");

    // Total tx count
    let (count,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM transactions WHERE block_number = ?")
            .bind(5_000_038i64)
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(count, 8);
}

// ============================================================================
// System transfers with dual hash query
// ============================================================================

#[tokio::test]
async fn insert_and_query_system_transfers() {
    let db = setup().await;
    let block = decode_fixture("block_5000038.rmp.lz4", 999);

    db.insert_block(&block).await.unwrap();

    let stx = &block.system_transfers[0];

    // Query by recipient
    let row: (i64, Vec<u8>, Vec<u8>, String, Option<i32>) = sqlx::query_as(
        "SELECT block_number, official_hash, explorer_hash, asset_type, asset_index FROM system_transfers WHERE recipient = ?",
    )
    .bind(stx.recipient.as_slice())
    .fetch_one(db.pool())
    .await
    .unwrap();

    assert_eq!(row.0, 5_000_038);
    assert_eq!(row.1, stx.official_hash.as_slice());
    assert_eq!(row.2, stx.explorer_hash.as_slice());
    assert_eq!(row.3, "SpotToken");
    assert_eq!(row.4, Some(0i32));

    // Dual hash divergence: both hashes stored, both queryable, and they differ
    assert_ne!(row.1, row.2, "official and explorer hashes must differ");

    // Query by official hash
    let (bn,): (i64,) =
        sqlx::query_as("SELECT block_number FROM system_transfers WHERE official_hash = ?")
            .bind(stx.official_hash.as_slice())
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(bn, 5_000_038);

    // Query by explorer hash
    let (bn,): (i64,) =
        sqlx::query_as("SELECT block_number FROM system_transfers WHERE explorer_hash = ?")
            .bind(stx.explorer_hash.as_slice())
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(bn, 5_000_038);
}

// ============================================================================
// Event logs with topic query
// ============================================================================

#[tokio::test]
async fn insert_and_query_event_logs() {
    let db = setup().await;
    let block = decode_fixture("block_5000038.rmp.lz4", 999);

    db.insert_block(&block).await.unwrap();

    // Transfer event topic
    let transfer_topic = block.transactions[0].logs[0].topics[0];
    let first_log = &block.transactions[0].logs[0];

    let rows: Vec<(i64, i32, i32, Vec<u8>, Option<Vec<u8>>, Vec<u8>)> = sqlx::query_as(
        "SELECT block_number, tx_index, log_index, address, topic0, data FROM event_logs WHERE topic0 = ? AND block_number = ? ORDER BY log_index",
    )
    .bind(transfer_topic.as_slice())
    .bind(5_000_038i64)
    .fetch_all(db.pool())
    .await
    .unwrap();

    assert!(!rows.is_empty(), "expected Transfer event logs");

    // First log pinned values
    assert_eq!(rows[0].0, 5_000_038);
    assert_eq!(rows[0].1, 0); // tx_index
    assert_eq!(rows[0].2, 0); // log_index
    assert_eq!(rows[0].3, first_log.address.as_slice());
    assert_eq!(rows[0].4.as_deref(), Some(transfer_topic.as_slice()));
    assert_eq!(rows[0].5, first_log.data.as_ref());

    // Total log count for block
    let (total,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM event_logs WHERE block_number = ?")
        .bind(5_000_038i64)
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert!(total > 0);
}

// ============================================================================
// Cursor round-trip
// ============================================================================

#[tokio::test]
async fn cursor_round_trip() {
    let db = setup().await;

    assert_eq!(db.get_cursor("mainnet").await.unwrap(), None);

    db.set_cursor("mainnet", 5_000_038).await.unwrap();
    assert_eq!(db.get_cursor("mainnet").await.unwrap(), Some(5_000_038));

    db.set_cursor("mainnet", 5_000_100).await.unwrap();
    assert_eq!(db.get_cursor("mainnet").await.unwrap(), Some(5_000_100));

    // Different network is independent
    assert_eq!(db.get_cursor("testnet").await.unwrap(), None);
}

// ============================================================================
// Idempotent insert (INSERT OR IGNORE)
// ============================================================================

#[tokio::test]
async fn idempotent_insert() {
    let db = setup().await;
    let block = decode_fixture("block_5000038.rmp.lz4", 999);

    db.insert_block(&block).await.unwrap();
    db.insert_block(&block).await.unwrap(); // no error

    let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM blocks WHERE block_number = ?")
        .bind(5_000_038i64)
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(count, 1);

    let (tx_count,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM transactions WHERE block_number = ?")
            .bind(5_000_038i64)
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(tx_count, 8);
}

// ============================================================================
// Batch insert with multiple blocks
// ============================================================================

#[tokio::test]
async fn batch_insert() {
    let db = setup().await;

    let block1 = decode_fixture("block_1.rmp.lz4", 999);
    let block2 = decode_fixture("block_5000038.rmp.lz4", 999);
    let block3 = decode_fixture("block_testnet_48186001.rmp.lz4", 998);

    db.insert_batch(&[block1, block2, block3]).await.unwrap();

    let rows: Vec<(i64, i32)> =
        sqlx::query_as("SELECT block_number, tx_count FROM blocks ORDER BY block_number")
            .fetch_all(db.pool())
            .await
            .unwrap();

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], (1, 0));
    assert_eq!(rows[1], (5_000_038, 8));
    assert_eq!(rows[2], (48_186_001, 0));

    // Total across all blocks
    let (tx_count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM transactions")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(tx_count, 8);
}

// ============================================================================
// Atomic batch insert with cursor
// ============================================================================

#[tokio::test]
async fn atomic_batch_insert_with_cursor() {
    let db = setup().await;

    let block1 = decode_fixture("block_1.rmp.lz4", 999);
    let block2 = decode_fixture("block_5000038.rmp.lz4", 999);

    assert_eq!(db.get_cursor("mainnet").await.unwrap(), None);

    db.insert_batch_and_set_cursor(&[block1, block2], "mainnet", 5_000_038)
        .await
        .unwrap();

    let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM blocks")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(count, 2);
    assert_eq!(db.get_cursor("mainnet").await.unwrap(), Some(5_000_038));
}

// ============================================================================
// Value preservation: U256 amounts survive round-trip
// ============================================================================

#[tokio::test]
async fn u256_amount_round_trip() {
    let db = setup().await;
    let block = decode_fixture("block_5000038.rmp.lz4", 999);

    db.insert_block(&block).await.unwrap();

    // System transfer amount should survive TEXT storage round-trip
    let stx = &block.system_transfers[0];
    let (amount_str,): (String,) =
        sqlx::query_as("SELECT amount_wei FROM system_transfers WHERE block_number = ?")
            .bind(5_000_038i64)
            .fetch_one(db.pool())
            .await
            .unwrap();

    assert_eq!(amount_str, stx.amount_wei.to_string());
    assert!(!amount_str.is_empty());
    assert_ne!(amount_str, "0");
}

// ============================================================================
// Empty block inserts cleanly (no txs, no logs, no system transfers)
// ============================================================================

#[tokio::test]
async fn empty_block_insert() {
    let db = setup().await;
    let block = decode_fixture("block_1.rmp.lz4", 999);

    db.insert_block(&block).await.unwrap();

    let (tx_count,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM transactions WHERE block_number = ?")
            .bind(1i64)
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(tx_count, 0);

    let (log_count,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM event_logs WHERE block_number = ?")
            .bind(1i64)
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(log_count, 0);

    let (stx_count,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM system_transfers WHERE block_number = ?")
            .bind(1i64)
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(stx_count, 0);
}
