//! Full pipeline E2E tests: fixture LZ4 → decode → store → query → verify.
//!
//! Tests the complete data path from raw S3 block data through to queryable
//! storage, verifying exact field values at every stage. Uses SQLite in-memory
//! (no Docker needed) since the Storage trait is backend-agnostic.
//!
//! These tests act as regression guards: if deserialization, hash computation,
//! type conversion, or SQL mapping changes, they catch it.

use hypercore_indexer::decode;
use hypercore_indexer::decode::types::{AssetType, TxType};
use hypercore_indexer::s3::codec;
use hypercore_indexer::storage::sqlite::SqliteStorage;
use hypercore_indexer::storage::Storage;

fn load_fixture(name: &str) -> Vec<u8> {
    let path = format!("{}/tests/fixtures/{name}", env!("CARGO_MANIFEST_DIR"));
    std::fs::read(&path).unwrap_or_else(|e| panic!("Failed to read fixture {path}: {e}"))
}

async fn setup() -> SqliteStorage {
    let db = SqliteStorage::connect("sqlite::memory:").await.unwrap();
    db.ensure_schema().await.unwrap();
    db
}

// ============================================================================
// Full pipeline: LZ4 fixture → decode → hash → store → query back
// Block 5000038 (8 txs, 1 system transfer, known values throughout)
// ============================================================================

#[tokio::test]
async fn pipeline_block_5000038_blocks_table() {
    let db = setup().await;

    // Stage 1: Raw S3 decode (LZ4 + MessagePack)
    let compressed = load_fixture("block_5000038.rmp.lz4");
    let raw = codec::decode_block(&compressed).unwrap();

    // Stage 2: Full decode with hash computation
    let decoded = decode::decode_block(&raw, 999).unwrap();

    // Stage 3: Store to database
    db.insert_block(&decoded).await.unwrap();

    // Stage 4: Query back and verify exact values
    let row: (i64, Vec<u8>, Vec<u8>, i64, i64, i64, Option<i64>, i32, i32) = sqlx::query_as(
        "SELECT block_number, block_hash, parent_hash, timestamp, gas_used, gas_limit, base_fee_per_gas, tx_count, system_tx_count FROM blocks WHERE block_number = ?",
    )
    .bind(5_000_038i64)
    .fetch_one(db.pool())
    .await
    .unwrap();

    assert_eq!(row.0, 5_000_038);
    assert_eq!(hex::encode(&row.1), "6639e377dc4aba11f210dc95b0024f15840d0289a82abf883ef3825a85fa9508");
    assert_eq!(hex::encode(&row.2), "87b447ef1a1b8327b32aab6f7a671c0ad6239efcf56386fabaec87c909a198d5");
    assert_eq!(row.3, 1_749_160_149);
    assert_eq!(row.4, 1_722_800);
    assert_eq!(row.5, 2_000_000);
    assert_eq!(row.6, Some(622_120_557));
    assert_eq!(row.7, 8);
    assert_eq!(row.8, 1);
}

#[tokio::test]
async fn pipeline_block_5000038_all_tx_hashes_stored() {
    let db = setup().await;
    let compressed = load_fixture("block_5000038.rmp.lz4");
    let raw = codec::decode_block(&compressed).unwrap();
    let decoded = decode::decode_block(&raw, 999).unwrap();
    db.insert_block(&decoded).await.unwrap();

    // Query all tx hashes back in order
    let rows: Vec<(i32, Vec<u8>, i32, i64, bool)> = sqlx::query_as(
        "SELECT tx_index, tx_hash, tx_type, gas_used, success FROM transactions WHERE block_number = ? ORDER BY tx_index",
    )
    .bind(5_000_038i64)
    .fetch_all(db.pool())
    .await
    .unwrap();

    assert_eq!(rows.len(), 8);

    // Pin all 8 tx hashes — these are computed via RLP+keccak256
    let expected_hashes = [
        "1f912cb736959444532212379df30c07b78c8c1761200550bf92eff37cf6d998",
        "13d9d197fd9fe4b68358b4a12a2fd82d7ca24380472d7098244e0b8bbc00b738",
        "ee1faff87698deb24094eb3ec8e120e0053e5cf2d028f4f69d48935da2a7d152",
        "57d8b7f7c192e62622c3733fa9e11021f4bcdacf796e2eb81d8a12e0a475389e",
        "0b61c0800dfaed405ddc5b9a36ed0982b375913444e0f4aaab2f744f515e0860",
        "393d93d17ecc465ef1a981a52c9cd1238c23258cfddd7bce36dbaa2c3f8274b0",
        "c548e68042ecce4127454d652dad03303c5a816bb08065822b857e54316a60fa",
        "f5a15ba50dd767b473bf4aaadf6a1085e02f13b27cb1a45143d3980a2f04e9fc",
    ];
    let expected_types: [i32; 8] = [2, 2, 0, 2, 2, 2, 2, 0]; // Eip1559=2, Legacy=0
    let expected_gas: [i64; 8] = [172182, 191685, 36699, 70481, 88112, 70774, 85619, 1007248];
    let expected_success = [true, true, true, false, false, false, false, true];

    for (i, row) in rows.iter().enumerate() {
        assert_eq!(row.0, i as i32, "tx_index mismatch at {i}");
        assert_eq!(hex::encode(&row.1), expected_hashes[i], "tx_hash mismatch at {i}");
        assert_eq!(row.2, expected_types[i], "tx_type mismatch at {i}");
        assert_eq!(row.3, expected_gas[i], "gas_used mismatch at {i}");
        assert_eq!(row.4, expected_success[i], "success mismatch at {i}");
    }

    // Gas sum must equal block gas_used
    let total_gas: i64 = rows.iter().map(|r| r.3).sum();
    assert_eq!(total_gas, 1_722_800);
}

#[tokio::test]
async fn pipeline_block_5000038_system_transfer_dual_hashes() {
    let db = setup().await;
    let compressed = load_fixture("block_5000038.rmp.lz4");
    let raw = codec::decode_block(&compressed).unwrap();
    let decoded = decode::decode_block(&raw, 999).unwrap();
    db.insert_block(&decoded).await.unwrap();

    let row: (Vec<u8>, Vec<u8>, Vec<u8>, String, Option<i32>, Vec<u8>, String) = sqlx::query_as(
        "SELECT official_hash, explorer_hash, system_address, asset_type, asset_index, recipient, amount_wei FROM system_transfers WHERE block_number = ?",
    )
    .bind(5_000_038i64)
    .fetch_one(db.pool())
    .await
    .unwrap();

    // Pinned dual hashes — if RLP encoding or phantom hash logic changes, this breaks
    assert_eq!(
        hex::encode(&row.0),
        "3018ef9a2d5f37639c248632abc93d7a3328cc9497737b1b140cb3221dbc829c",
        "official_hash mismatch"
    );
    assert_eq!(
        hex::encode(&row.1),
        "355291ec82b3818450b5a7179faf8b5d973822b2ab193f209e0e0f3081c7088d",
        "explorer_hash mismatch"
    );
    assert_ne!(row.0, row.1, "dual hashes must differ");

    // System address is the token contract (PURR)
    assert_eq!(
        hex::encode(&row.2),
        "9b498c3c8a0b8cd8ba1d9851d40d186f1872b44e"
    );
    assert_eq!(row.3, "SpotToken");
    assert_eq!(row.4, Some(0));

    // Recipient
    assert_eq!(
        hex::encode(&row.5),
        "efd3ab65915e35105caa462442c9ecc1346728df"
    );

    // Amount is non-zero (stored as TEXT in SQLite)
    assert!(!row.6.is_empty());
    assert_ne!(row.6, "0");

    // Query by official hash — must find the same row
    let (bn,): (i64,) = sqlx::query_as(
        "SELECT block_number FROM system_transfers WHERE official_hash = ?",
    )
    .bind(&row.0)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(bn, 5_000_038);

    // Query by explorer hash — must find the same row
    let (bn,): (i64,) = sqlx::query_as(
        "SELECT block_number FROM system_transfers WHERE explorer_hash = ?",
    )
    .bind(&row.1)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(bn, 5_000_038);
}

#[tokio::test]
async fn pipeline_block_5000038_event_logs_with_topics() {
    let db = setup().await;
    let compressed = load_fixture("block_5000038.rmp.lz4");
    let raw = codec::decode_block(&compressed).unwrap();
    let decoded = decode::decode_block(&raw, 999).unwrap();
    db.insert_block(&decoded).await.unwrap();

    // Total logs stored
    let (total,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM event_logs WHERE block_number = ?",
    )
    .bind(5_000_038i64)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert!(total > 0, "block should have event logs");

    // First log: Transfer event from HYPE token (0x5555...)
    let row: (i32, i32, Vec<u8>, Option<Vec<u8>>, Vec<u8>) = sqlx::query_as(
        "SELECT tx_index, log_index, address, topic0, data FROM event_logs WHERE block_number = ? ORDER BY log_index LIMIT 1",
    )
    .bind(5_000_038i64)
    .fetch_one(db.pool())
    .await
    .unwrap();

    assert_eq!(row.0, 0); // tx_index
    assert_eq!(row.1, 0); // log_index
    assert_eq!(
        hex::encode(&row.2),
        "5555555555555555555555555555555555555555",
        "first log should be from HYPE token"
    );
    // Transfer event signature
    assert_eq!(
        hex::encode(row.3.as_deref().unwrap()),
        "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
        "topic0 should be Transfer event signature"
    );
    assert!(!row.4.is_empty(), "log data should be non-empty");

    // Query Transfer events by topic0 across all txs in block
    let transfer_topic = hex::decode("ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef").unwrap();
    let (transfer_count,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM event_logs WHERE block_number = ? AND topic0 = ?",
    )
    .bind(5_000_038i64)
    .bind(&transfer_topic)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert!(transfer_count > 0, "should have Transfer events");
}

// ============================================================================
// Full pipeline: empty block (no txs, no system transfers)
// ============================================================================

#[tokio::test]
async fn pipeline_empty_block() {
    let db = setup().await;
    let compressed = load_fixture("block_1.rmp.lz4");
    let raw = codec::decode_block(&compressed).unwrap();
    let decoded = decode::decode_block(&raw, 999).unwrap();
    db.insert_block(&decoded).await.unwrap();

    // Block stored
    let (bn, hash): (i64, Vec<u8>) = sqlx::query_as(
        "SELECT block_number, block_hash FROM blocks WHERE block_number = ?",
    )
    .bind(1i64)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(bn, 1);
    assert_eq!(
        hex::encode(&hash),
        "de151843548b88d06f201d86e860e45fbf07d49612f1934fba5746abd942fb01"
    );

    // No child rows
    let (tx_count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM transactions WHERE block_number = 1")
        .fetch_one(db.pool()).await.unwrap();
    let (log_count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM event_logs WHERE block_number = 1")
        .fetch_one(db.pool()).await.unwrap();
    let (stx_count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM system_transfers WHERE block_number = 1")
        .fetch_one(db.pool()).await.unwrap();
    assert_eq!(tx_count, 0);
    assert_eq!(log_count, 0);
    assert_eq!(stx_count, 0);
}

// ============================================================================
// Full pipeline: batch insert + cursor + resume
// ============================================================================

#[tokio::test]
async fn pipeline_batch_with_cursor_resume() {
    let db = setup().await;

    // Decode two blocks from fixtures
    let block1 = {
        let raw = codec::decode_block(&load_fixture("block_1.rmp.lz4")).unwrap();
        decode::decode_block(&raw, 999).unwrap()
    };
    let block2 = {
        let raw = codec::decode_block(&load_fixture("block_5000038.rmp.lz4")).unwrap();
        decode::decode_block(&raw, 999).unwrap()
    };

    // Batch insert with cursor
    db.insert_batch_and_set_cursor(&[block1, block2], "mainnet", 5_000_038)
        .await
        .unwrap();

    // Verify cursor
    let cursor = db.get_cursor("mainnet").await.unwrap();
    assert_eq!(cursor, Some(5_000_038));

    // Verify both blocks stored
    let rows: Vec<(i64, i32)> = sqlx::query_as(
        "SELECT block_number, tx_count FROM blocks ORDER BY block_number",
    )
    .fetch_all(db.pool())
    .await
    .unwrap();
    assert_eq!(rows, vec![(1, 0), (5_000_038, 8)]);

    // Simulate resume: cursor tells us to start from 5_000_039
    let next_block = cursor.unwrap() + 1;
    assert_eq!(next_block, 5_000_039);
}

// ============================================================================
// Full pipeline: idempotent re-processing
// ============================================================================

#[tokio::test]
async fn pipeline_idempotent_reprocess() {
    let db = setup().await;
    let compressed = load_fixture("block_5000038.rmp.lz4");
    let raw = codec::decode_block(&compressed).unwrap();
    let decoded = decode::decode_block(&raw, 999).unwrap();

    // Insert twice
    db.insert_block(&decoded).await.unwrap();
    db.insert_block(&decoded).await.unwrap();

    // Everything should still be count=1
    let (blocks,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM blocks").fetch_one(db.pool()).await.unwrap();
    let (txs,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM transactions").fetch_one(db.pool()).await.unwrap();
    let (stxs,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM system_transfers").fetch_one(db.pool()).await.unwrap();
    assert_eq!(blocks, 1);
    assert_eq!(txs, 8);
    assert_eq!(stxs, 1);

    // Hashes should still be correct (not doubled/corrupted)
    let (hash,): (Vec<u8>,) = sqlx::query_as("SELECT tx_hash FROM transactions WHERE tx_index = 0")
        .fetch_one(db.pool()).await.unwrap();
    assert_eq!(
        hex::encode(&hash),
        "1f912cb736959444532212379df30c07b78c8c1761200550bf92eff37cf6d998"
    );
}

// ============================================================================
// Full pipeline: testnet block (chain_id 998)
// ============================================================================

#[tokio::test]
async fn pipeline_testnet_block() {
    let db = setup().await;
    let compressed = load_fixture("block_testnet_48186001.rmp.lz4");
    let raw = codec::decode_block(&compressed).unwrap();
    // Chain ID 998 for testnet
    let decoded = decode::decode_block(&raw, 998).unwrap();
    db.insert_block(&decoded).await.unwrap();

    let (bn, hash): (i64, Vec<u8>) = sqlx::query_as(
        "SELECT block_number, block_hash FROM blocks WHERE block_number = ?",
    )
    .bind(48_186_001i64)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(bn, 48_186_001);
    assert_eq!(
        hex::encode(&hash),
        "acef176c39777f536aaf21e6ced1e27bc8d57e16c21a376957ebfb810b1777a8"
    );
}
