//! Integration tests for PostgreSQL storage.
//!
//! These tests require a running PostgreSQL instance at:
//!   postgres://postgres:postgres@localhost:5432/hypercore
//!
//! Run with: cargo test -- --ignored --test-threads=1
//!
//! The --test-threads=1 flag is required because tests share the same database
//! and run schema migrations + data cleanup that must not overlap.

use hypercore_indexer::decode;
use hypercore_indexer::s3::codec;
use hypercore_indexer::storage::postgres::PostgresStorage;
use hypercore_indexer::storage::Storage;

/// Uses port 5433 (test compose) to avoid conflicting with dev on 5432.
/// Override with DATABASE_URL env var if needed.
fn database_url() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgres://postgres:postgres@localhost:5433/hypercore_test".to_string()
    })
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

/// Setup a clean test database. Tests must be run with --test-threads=1
/// to avoid concurrent schema creation and data cleanup races.
async fn setup() -> PostgresStorage {
    let pg = PostgresStorage::connect(&database_url()).await.unwrap();
    pg.ensure_schema().await.unwrap();
    // Clean up test data
    sqlx::raw_sql(
        "DELETE FROM event_logs; DELETE FROM transactions; DELETE FROM system_transfers; DELETE FROM blocks; DELETE FROM indexer_cursor;",
    )
    .execute(pg.pool())
    .await
    .unwrap();
    pg
}

// ============================================================================
// Insert and query block
// ============================================================================

#[tokio::test]
#[ignore]
async fn insert_and_query_block() {
    let pg = setup().await;
    let block = decode_fixture("block_5000038.rmp.lz4", 999);

    pg.insert_block(&block).await.unwrap();

    let row: (i64, Vec<u8>, Vec<u8>, i64, i64, i64, Option<i64>, i32, i32) = sqlx::query_as(
        "SELECT block_number, block_hash, parent_hash, timestamp, gas_used, gas_limit, base_fee_per_gas, tx_count, system_tx_count FROM blocks WHERE block_number = $1",
    )
    .bind(5_000_038i64)
    .fetch_one(pg.pool())
    .await
    .unwrap();

    assert_eq!(row.0, 5_000_038);
    assert_eq!(row.1, block.hash.as_slice());
    assert_eq!(row.2, block.parent_hash.as_slice());
    assert_eq!(row.3, block.timestamp as i64);
    assert_eq!(row.4, block.gas_used as i64);
    assert_eq!(row.5, block.gas_limit as i64);
    assert_eq!(row.6, block.base_fee_per_gas.map(|v| v as i64));
    assert_eq!(row.7, 8); // 8 transactions
    assert_eq!(row.8, 1); // 1 system transfer
}

// ============================================================================
// Insert and query transactions
// ============================================================================

#[tokio::test]
#[ignore]
async fn insert_and_query_transactions() {
    let pg = setup().await;
    let block = decode_fixture("block_5000038.rmp.lz4", 999);

    pg.insert_block(&block).await.unwrap();

    // Query first transaction by hash
    let tx0_hash = block.transactions[0].hash;
    let row: (i64, i32, Vec<u8>, i16, i64, i64, bool) = sqlx::query_as(
        r#"SELECT block_number, tx_index, tx_hash, tx_type, gas_limit, gas_used, success FROM transactions WHERE tx_hash = $1"#,
    )
    .bind(tx0_hash.as_slice())
    .fetch_one(pg.pool())
    .await
    .unwrap();

    assert_eq!(row.0, 5_000_038);
    assert_eq!(row.1, 0); // tx_index
    assert_eq!(row.2, tx0_hash.as_slice());
    assert_eq!(row.3, 2); // Eip1559
    assert!(row.4 > 0); // gas_limit
    assert_eq!(row.5, 172_182); // gas_used for first tx
    assert!(row.6); // success = true

    // Verify total transaction count
    let (count,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM transactions WHERE block_number = $1")
            .bind(5_000_038i64)
            .fetch_one(pg.pool())
            .await
            .unwrap();
    assert_eq!(count, 8);
}

// ============================================================================
// Insert and query system transfers
// ============================================================================

#[tokio::test]
#[ignore]
async fn insert_and_query_system_transfers() {
    let pg = setup().await;
    let block = decode_fixture("block_5000038.rmp.lz4", 999);

    pg.insert_block(&block).await.unwrap();

    let stx = &block.system_transfers[0];

    // Query by recipient
    let row: (i64, Vec<u8>, Vec<u8>, String, Option<i16>) = sqlx::query_as(
        "SELECT block_number, official_hash, explorer_hash, asset_type, asset_index FROM system_transfers WHERE recipient = $1",
    )
    .bind(stx.recipient.as_slice())
    .fetch_one(pg.pool())
    .await
    .unwrap();

    assert_eq!(row.0, 5_000_038);
    assert_eq!(row.1, stx.official_hash.as_slice());
    assert_eq!(row.2, stx.explorer_hash.as_slice());
    assert_eq!(row.3, "SpotToken");
    assert_eq!(row.4, Some(0i16));

    // Query by official hash
    let (bn,): (i64,) =
        sqlx::query_as("SELECT block_number FROM system_transfers WHERE official_hash = $1")
            .bind(stx.official_hash.as_slice())
            .fetch_one(pg.pool())
            .await
            .unwrap();
    assert_eq!(bn, 5_000_038);

    // Query by explorer hash
    let (bn,): (i64,) =
        sqlx::query_as("SELECT block_number FROM system_transfers WHERE explorer_hash = $1")
            .bind(stx.explorer_hash.as_slice())
            .fetch_one(pg.pool())
            .await
            .unwrap();
    assert_eq!(bn, 5_000_038);
}

// ============================================================================
// Insert and query event logs
// ============================================================================

#[tokio::test]
#[ignore]
async fn insert_and_query_event_logs() {
    let pg = setup().await;
    let block = decode_fixture("block_5000038.rmp.lz4", 999);

    pg.insert_block(&block).await.unwrap();

    // Transfer event signature: 0xddf252ad...
    let transfer_topic0 = block.transactions[0].logs[0].topics[0];

    let rows: Vec<(i64, i32, i32, Vec<u8>, Option<Vec<u8>>, Vec<u8>)> = sqlx::query_as(
        "SELECT block_number, tx_index, log_index, address, topic0, data FROM event_logs WHERE topic0 = $1 AND block_number = $2 ORDER BY log_index",
    )
    .bind(transfer_topic0.as_slice())
    .bind(5_000_038i64)
    .fetch_all(pg.pool())
    .await
    .unwrap();

    // Should have at least one Transfer log
    assert!(!rows.is_empty(), "expected Transfer event logs");

    // Verify first Transfer log matches fixture
    let first_log = &block.transactions[0].logs[0];
    let first_row = &rows[0];
    assert_eq!(first_row.0, 5_000_038);
    assert_eq!(first_row.1, 0); // tx_index
    assert_eq!(first_row.2, 0); // log_index
    assert_eq!(first_row.3, first_log.address.as_slice());
    assert_eq!(first_row.4.as_deref(), Some(transfer_topic0.as_slice()));
    assert_eq!(first_row.5, first_log.data.as_ref());
}

// ============================================================================
// Cursor round-trip
// ============================================================================

#[tokio::test]
#[ignore]
async fn cursor_round_trip() {
    let pg = setup().await;

    // No cursor initially
    let cursor = pg.get_cursor("mainnet").await.unwrap();
    assert_eq!(cursor, None);

    // Set cursor
    pg.set_cursor("mainnet", 5_000_038).await.unwrap();
    let cursor = pg.get_cursor("mainnet").await.unwrap();
    assert_eq!(cursor, Some(5_000_038));

    // Update cursor
    pg.set_cursor("mainnet", 5_000_100).await.unwrap();
    let cursor = pg.get_cursor("mainnet").await.unwrap();
    assert_eq!(cursor, Some(5_000_100));

    // Different network cursor is independent
    let cursor = pg.get_cursor("testnet").await.unwrap();
    assert_eq!(cursor, None);
}

// ============================================================================
// Idempotent insert
// ============================================================================

#[tokio::test]
#[ignore]
async fn idempotent_insert() {
    let pg = setup().await;
    let block = decode_fixture("block_5000038.rmp.lz4", 999);

    // Insert twice — should not error
    pg.insert_block(&block).await.unwrap();
    pg.insert_block(&block).await.unwrap();

    // Verify count is still 1
    let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM blocks WHERE block_number = $1")
        .bind(5_000_038i64)
        .fetch_one(pg.pool())
        .await
        .unwrap();
    assert_eq!(count, 1);

    let (tx_count,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM transactions WHERE block_number = $1")
            .bind(5_000_038i64)
            .fetch_one(pg.pool())
            .await
            .unwrap();
    assert_eq!(tx_count, 8);
}

// ============================================================================
// Batch insert
// ============================================================================

#[tokio::test]
#[ignore]
async fn batch_insert() {
    let pg = setup().await;

    let block1 = decode_fixture("block_1.rmp.lz4", 999);
    let block2 = decode_fixture("block_5000038.rmp.lz4", 999);
    // Decode testnet block with chain_id 998
    let block3 = decode_fixture("block_testnet_48186001.rmp.lz4", 998);

    let blocks = vec![block1, block2, block3];
    pg.insert_batch(&blocks).await.unwrap();

    // Verify all 3 blocks inserted
    let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM blocks")
        .fetch_one(pg.pool())
        .await
        .unwrap();
    assert_eq!(count, 3);

    // Verify ordering by querying block numbers
    let rows: Vec<(i64,)> = sqlx::query_as("SELECT block_number FROM blocks ORDER BY block_number")
        .fetch_all(pg.pool())
        .await
        .unwrap();
    assert_eq!(rows[0].0, 1);
    assert_eq!(rows[1].0, 5_000_038);
    assert_eq!(rows[2].0, 48_186_001);

    // Verify transactions: block 1 has 0, block 5000038 has 8, testnet block has 0
    let (tx_count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM transactions")
        .fetch_one(pg.pool())
        .await
        .unwrap();
    assert_eq!(tx_count, 8);
}

// ============================================================================
// Atomic batch insert with cursor
// ============================================================================

#[tokio::test]
#[ignore]
async fn atomic_batch_insert_with_cursor() {
    let pg = setup().await;

    let block1 = decode_fixture("block_1.rmp.lz4", 999);
    let block2 = decode_fixture("block_5000038.rmp.lz4", 999);

    // No cursor initially
    let cursor = pg.get_cursor("mainnet").await.unwrap();
    assert_eq!(cursor, None);

    // Atomic insert + cursor update
    let blocks = vec![block1, block2];
    pg.insert_batch_and_set_cursor(&blocks, "mainnet", 5_000_038)
        .await
        .unwrap();

    // Both data and cursor should be committed together
    let (block_count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM blocks")
        .fetch_one(pg.pool())
        .await
        .unwrap();
    assert_eq!(block_count, 2);

    let cursor = pg.get_cursor("mainnet").await.unwrap();
    assert_eq!(cursor, Some(5_000_038));

    // Verify transactions inserted correctly via batch UNNEST
    let (tx_count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM transactions")
        .fetch_one(pg.pool())
        .await
        .unwrap();
    assert_eq!(tx_count, 8); // block_1 has 0, block_5000038 has 8
}
