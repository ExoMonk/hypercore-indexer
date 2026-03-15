//! Integration tests for HIP4 contest event decoding and SQLite storage.
//!
//! Uses real testnet block fixture with known Deposit event.
//! Run with: cargo test --test hip4_sqlite

use hypercore_indexer::config::Hip4Config;
use hypercore_indexer::decode;
use hypercore_indexer::hip4;
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
// Decode testnet block with known Deposit event
// ============================================================================

#[test]
fn decode_testnet_block_extracts_deposit() {
    let block = decode_fixture("block_testnet_48192000.rmp.lz4", 998);

    let config = Hip4Config {
        enabled: true,
        contest_address: Some("0x4fd772e5708da2a7f097f51b3127e515a72744bd".to_string()),
        ..Default::default()
    };

    let hip4_data = hip4::process_block(&block, &config);

    assert_eq!(hip4_data.deposits.len(), 1, "expected exactly one deposit");
    assert!(hip4_data.claims.is_empty(), "expected no claims");

    let deposit = &hip4_data.deposits[0];
    assert_eq!(deposit.block_number, 48_192_000);
    assert_eq!(deposit.tx_index, 0);
    assert_eq!(deposit.log_index, 0);
    assert_eq!(deposit.contest_id, 595); // 0x253
    assert_eq!(deposit.side_id, 9);
    assert_eq!(
        format!("{:#x}", deposit.depositor),
        "0xabb5b9505df12a1863de8551d451b100555cfbd2"
    );
    assert_eq!(
        deposit.amount_wei.to_string(),
        "100000000000000000" // 0.1 ETH in wei
    );
}

// ============================================================================
// HIP4 disabled returns empty
// ============================================================================

#[test]
fn hip4_disabled_skips_deposit() {
    let block = decode_fixture("block_testnet_48192000.rmp.lz4", 998);

    let config = Hip4Config {
        enabled: false,
        contest_address: Some("0x4fd772e5708da2a7f097f51b3127e515a72744bd".to_string()),
        ..Default::default()
    };

    let hip4_data = hip4::process_block(&block, &config);
    assert!(hip4_data.deposits.is_empty());
    assert!(hip4_data.claims.is_empty());
}

// ============================================================================
// Wrong contest address finds no events
// ============================================================================

#[test]
fn wrong_contest_address_finds_nothing() {
    let block = decode_fixture("block_testnet_48192000.rmp.lz4", 998);

    let config = Hip4Config {
        enabled: true,
        contest_address: Some("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef".to_string()),
        ..Default::default()
    };

    let hip4_data = hip4::process_block(&block, &config);
    assert!(hip4_data.deposits.is_empty());
    assert!(hip4_data.claims.is_empty());
}

// ============================================================================
// Store to SQLite and query back
// ============================================================================

#[tokio::test]
async fn hip4_deposit_sqlite_round_trip() {
    let db = setup().await;

    // Insert the block first (for FK context, though hip4 tables don't have FK)
    let block = decode_fixture("block_testnet_48192000.rmp.lz4", 998);
    db.insert_block(&block).await.unwrap();

    // Process HIP4 data
    let config = Hip4Config {
        enabled: true,
        contest_address: Some("0x4fd772e5708da2a7f097f51b3127e515a72744bd".to_string()),
        ..Default::default()
    };
    let hip4_data = hip4::process_block(&block, &config);

    // Insert HIP4 data
    db.insert_hip4_data(&hip4_data).await.unwrap();

    // Query back
    let row: (i64, i32, i32, i64, i64, Vec<u8>, String) = sqlx::query_as(
        "SELECT block_number, tx_index, log_index, contest_id, side_id, depositor, amount_wei FROM hip4_deposits WHERE block_number = ?",
    )
    .bind(48_192_000i64)
    .fetch_one(db.pool())
    .await
    .unwrap();

    assert_eq!(row.0, 48_192_000);
    assert_eq!(row.1, 0); // tx_index
    assert_eq!(row.2, 0); // log_index
    assert_eq!(row.3, 595); // contest_id
    assert_eq!(row.4, 9); // side_id
    // depositor is stored as 20-byte blob
    assert_eq!(hex::encode(&row.5), "abb5b9505df12a1863de8551d451b100555cfbd2");
    assert_eq!(row.6, "100000000000000000");

    // No claims
    let (claim_count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM hip4_claims")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(claim_count, 0);
}

// ============================================================================
// Idempotent insert (INSERT OR IGNORE)
// ============================================================================

#[tokio::test]
async fn hip4_idempotent_insert() {
    let db = setup().await;

    let block = decode_fixture("block_testnet_48192000.rmp.lz4", 998);

    let config = Hip4Config {
        enabled: true,
        contest_address: Some("0x4fd772e5708da2a7f097f51b3127e515a72744bd".to_string()),
        ..Default::default()
    };

    let hip4_data = hip4::process_block(&block, &config);

    // Insert twice
    db.insert_hip4_data(&hip4_data).await.unwrap();
    db.insert_hip4_data(&hip4_data).await.unwrap();

    let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM hip4_deposits")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(count, 1);
}

// ============================================================================
// Block without contest events produces empty hip4 data
// ============================================================================

#[test]
fn block_without_contest_events_is_empty() {
    let block = decode_fixture("block_5000038.rmp.lz4", 999);

    let config = Hip4Config {
        enabled: true,
        contest_address: Some("0x4fd772e5708da2a7f097f51b3127e515a72744bd".to_string()),
        ..Default::default()
    };

    let hip4_data = hip4::process_block(&block, &config);
    assert!(hip4_data.deposits.is_empty());
    assert!(hip4_data.claims.is_empty());
}

// ============================================================================
// Query by contest_id index
// ============================================================================

#[tokio::test]
async fn hip4_query_by_contest_id() {
    let db = setup().await;

    let block = decode_fixture("block_testnet_48192000.rmp.lz4", 998);

    let config = Hip4Config {
        enabled: true,
        contest_address: Some("0x4fd772e5708da2a7f097f51b3127e515a72744bd".to_string()),
        ..Default::default()
    };

    let hip4_data = hip4::process_block(&block, &config);
    db.insert_hip4_data(&hip4_data).await.unwrap();

    // Query by contest_id and side_id (uses idx_hip4_deposits_contest index)
    let rows: Vec<(i64, String)> = sqlx::query_as(
        "SELECT block_number, amount_wei FROM hip4_deposits WHERE contest_id = ? AND side_id = ?",
    )
    .bind(595i64)
    .bind(9i64)
    .fetch_all(db.pool())
    .await
    .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 48_192_000);
    assert_eq!(rows[0].1, "100000000000000000");
}
