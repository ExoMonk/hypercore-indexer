//! Backfill pipeline E2E tests using local fixture files as mock S3 source.
//!
//! Tests the full flow: read LZ4 fixtures (same format as S3) → decode →
//! compute hashes → batch insert → query back with pinned values.
//!
//! Uses 11 contiguous blocks (5000035-5000045) in tests/fixtures/range/
//! stored in the same .rmp.lz4 format as the S3 bucket.

use hypercore_indexer::decode;
use hypercore_indexer::s3::codec;
use hypercore_indexer::storage::sqlite::SqliteStorage;
use hypercore_indexer::storage::Storage;

fn fixture_dir() -> String {
    format!("{}/tests/fixtures/range", env!("CARGO_MANIFEST_DIR"))
}

/// Load and decode a block from the local fixture range, same as S3 would deliver.
fn load_and_decode(block_number: u64) -> decode::types::DecodedBlock {
    let path = format!("{}/{block_number}.rmp.lz4", fixture_dir());
    let compressed = std::fs::read(&path)
        .unwrap_or_else(|e| panic!("Missing fixture {path}: {e}"));
    let raw = codec::decode_block(&compressed).unwrap();
    decode::decode_block(&raw, 999).unwrap()
}

/// Load all 11 blocks in order, simulating a backfill range.
fn load_all_blocks() -> Vec<decode::types::DecodedBlock> {
    (5_000_035..=5_000_045)
        .map(|bn| load_and_decode(bn))
        .collect()
}

async fn setup() -> SqliteStorage {
    let db = SqliteStorage::connect("sqlite::memory:").await.unwrap();
    db.ensure_schema().await.unwrap();
    db
}

// Known values for blocks 5000035-5000045 (from live backfill output)
const EXPECTED: [(u64, usize, usize, u64); 11] = [
    // (block_number, tx_count, system_tx_count, gas_used)
    (5_000_035, 5, 1, 892_097),
    (5_000_036, 7, 0, 1_045_898),
    (5_000_037, 8, 2, 414_293),
    (5_000_038, 8, 1, 1_722_800),
    (5_000_039, 3, 0, 340_118),
    (5_000_040, 4, 0, 711_907),
    (5_000_041, 4, 0, 1_273_193),
    (5_000_042, 5, 1, 1_731_924),
    (5_000_043, 4, 0, 427_758),
    (5_000_044, 4, 0, 424_957),
    (5_000_045, 2, 0, 172_825),
];

// ============================================================================
// Every fixture block decodes correctly
// ============================================================================

#[test]
fn all_fixture_blocks_decode() {
    let blocks = load_all_blocks();
    assert_eq!(blocks.len(), 11);

    for (i, block) in blocks.iter().enumerate() {
        let (expected_bn, expected_txs, expected_stxs, expected_gas) = EXPECTED[i];
        assert_eq!(block.number, expected_bn, "block number mismatch at index {i}");
        assert_eq!(block.transactions.len(), expected_txs, "tx count mismatch for block {expected_bn}");
        assert_eq!(block.system_transfers.len(), expected_stxs, "system_tx count mismatch for block {expected_bn}");
        assert_eq!(block.gas_used, expected_gas, "gas_used mismatch for block {expected_bn}");
    }
}

#[test]
fn all_fixture_blocks_have_unique_hashes() {
    let blocks = load_all_blocks();
    let hashes: Vec<_> = blocks.iter().map(|b| b.hash).collect();
    let unique: std::collections::HashSet<_> = hashes.iter().collect();
    assert_eq!(unique.len(), 11, "all block hashes should be unique");
}

#[test]
fn fixture_blocks_are_sequential_with_parent_chain() {
    let blocks = load_all_blocks();
    for w in blocks.windows(2) {
        assert_eq!(w[1].number, w[0].number + 1, "blocks should be sequential");
        assert_eq!(
            w[1].parent_hash, w[0].hash,
            "block {} parent_hash should match block {} hash",
            w[1].number, w[0].number
        );
    }
}

#[test]
fn fixture_blocks_timestamps_are_monotonic() {
    let blocks = load_all_blocks();
    for w in blocks.windows(2) {
        assert!(
            w[1].timestamp >= w[0].timestamp,
            "timestamps should be monotonic: block {} ({}) < block {} ({})",
            w[0].number, w[0].timestamp, w[1].number, w[1].timestamp
        );
    }
}

#[test]
fn fixture_range_has_system_transfers() {
    let blocks = load_all_blocks();
    let total_stx: usize = blocks.iter().map(|b| b.system_transfers.len()).sum();
    // Blocks 5000035, 5000037, 5000038, 5000042 have system txs = 1+2+1+1 = 5
    assert_eq!(total_stx, 5, "range should have 5 total system transfers");
}

#[test]
fn fixture_range_tx_hashes_are_all_unique() {
    let blocks = load_all_blocks();
    let all_hashes: Vec<_> = blocks
        .iter()
        .flat_map(|b| b.transactions.iter().map(|t| t.hash))
        .collect();
    let unique: std::collections::HashSet<_> = all_hashes.iter().collect();
    let total_txs: usize = EXPECTED.iter().map(|(_, txc, _, _)| txc).sum();
    assert_eq!(all_hashes.len(), total_txs);
    assert_eq!(unique.len(), total_txs, "all tx hashes across range should be unique");
}

#[test]
fn fixture_per_block_gas_sums_correctly() {
    let blocks = load_all_blocks();
    for block in &blocks {
        if block.transactions.is_empty() {
            continue;
        }
        let sum: u64 = block.transactions.iter().map(|t| t.gas_used).sum();
        assert_eq!(
            sum, block.gas_used,
            "per-tx gas sum should equal block gas_used for block {}",
            block.number
        );
    }
}

// ============================================================================
// Backfill → Storage: batch insert all 11 blocks, query back
// ============================================================================

#[tokio::test]
async fn backfill_batch_insert_all_blocks() {
    let db = setup().await;
    let blocks = load_all_blocks();

    db.insert_batch(&blocks).await.unwrap();

    // Verify all 11 blocks stored
    let rows: Vec<(i64, i32, i32, i64)> = sqlx::query_as(
        "SELECT block_number, tx_count, system_tx_count, gas_used FROM blocks ORDER BY block_number",
    )
    .fetch_all(db.pool())
    .await
    .unwrap();

    assert_eq!(rows.len(), 11);
    for (i, row) in rows.iter().enumerate() {
        let (expected_bn, expected_txs, expected_stxs, expected_gas) = EXPECTED[i];
        assert_eq!(row.0, expected_bn as i64, "block_number mismatch at {i}");
        assert_eq!(row.1, expected_txs as i32, "tx_count mismatch for block {}", expected_bn);
        assert_eq!(row.2, expected_stxs as i32, "system_tx_count mismatch for block {}", expected_bn);
        assert_eq!(row.3, expected_gas as i64, "gas_used mismatch for block {}", expected_bn);
    }
}

#[tokio::test]
async fn backfill_total_rows_correct() {
    let db = setup().await;
    let blocks = load_all_blocks();
    db.insert_batch(&blocks).await.unwrap();

    let total_txs: usize = EXPECTED.iter().map(|(_, txc, _, _)| txc).sum();
    let total_stxs: usize = EXPECTED.iter().map(|(_, _, stxc, _)| stxc).sum();

    let (tx_count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM transactions")
        .fetch_one(db.pool()).await.unwrap();
    let (stx_count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM system_transfers")
        .fetch_one(db.pool()).await.unwrap();
    let (log_count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM event_logs")
        .fetch_one(db.pool()).await.unwrap();

    assert_eq!(tx_count, total_txs as i64, "total transactions");
    assert_eq!(stx_count, total_stxs as i64, "total system transfers");
    assert!(log_count > 0, "should have event logs");
}

#[tokio::test]
async fn backfill_with_cursor_tracks_progress() {
    let db = setup().await;
    let blocks = load_all_blocks();

    // Simulate two batches like the real backfill does
    let batch1 = &blocks[..6]; // blocks 5000035-5000040
    let batch2 = &blocks[6..]; // blocks 5000041-5000045

    db.insert_batch_and_set_cursor(batch1, "mainnet", 5_000_040)
        .await
        .unwrap();
    assert_eq!(db.get_cursor("mainnet").await.unwrap(), Some(5_000_040));

    db.insert_batch_and_set_cursor(batch2, "mainnet", 5_000_045)
        .await
        .unwrap();
    assert_eq!(db.get_cursor("mainnet").await.unwrap(), Some(5_000_045));

    // All 11 blocks stored
    let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM blocks")
        .fetch_one(db.pool()).await.unwrap();
    assert_eq!(count, 11);
}

#[tokio::test]
async fn backfill_idempotent_reinsert() {
    let db = setup().await;
    let blocks = load_all_blocks();

    // Insert all twice
    db.insert_batch(&blocks).await.unwrap();
    db.insert_batch(&blocks).await.unwrap();

    let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM blocks")
        .fetch_one(db.pool()).await.unwrap();
    assert_eq!(count, 11, "idempotent insert should not duplicate");

    let total_txs: usize = EXPECTED.iter().map(|(_, txc, _, _)| txc).sum();
    let (tx_count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM transactions")
        .fetch_one(db.pool()).await.unwrap();
    assert_eq!(tx_count, total_txs as i64, "tx count should not duplicate");
}

// ============================================================================
// Backfill → Query: verify query patterns work across the range
// ============================================================================

#[tokio::test]
async fn backfill_query_system_transfers_by_type() {
    let db = setup().await;
    db.insert_batch(&load_all_blocks()).await.unwrap();

    let rows: Vec<(i64, String)> = sqlx::query_as(
        "SELECT block_number, asset_type FROM system_transfers ORDER BY block_number, tx_index",
    )
    .fetch_all(db.pool())
    .await
    .unwrap();

    assert_eq!(rows.len(), 5);
    // Known from earlier decode: 5000035 NativeHype, 5000037 NativeHype+SpotToken,
    // 5000038 SpotToken, 5000042 NativeHype
    let hype_count = rows.iter().filter(|r| r.1 == "NativeHype").count();
    let spot_count = rows.iter().filter(|r| r.1 == "SpotToken").count();
    assert!(hype_count > 0, "should have NativeHype transfers");
    assert!(spot_count > 0, "should have SpotToken transfers");
    assert_eq!(hype_count + spot_count, 5);
}

#[tokio::test]
async fn backfill_query_tx_by_hash() {
    let db = setup().await;
    let blocks = load_all_blocks();
    db.insert_batch(&blocks).await.unwrap();

    // Pick a known tx hash from block 5000038 (first tx)
    let tx0_hash = blocks[3].transactions[0].hash; // index 3 = block 5000038
    let (bn, idx): (i64, i32) = sqlx::query_as(
        "SELECT block_number, tx_index FROM transactions WHERE tx_hash = ?",
    )
    .bind(tx0_hash.as_slice())
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(bn, 5_000_038);
    assert_eq!(idx, 0);
}

#[tokio::test]
async fn backfill_query_logs_by_topic0() {
    let db = setup().await;
    db.insert_batch(&load_all_blocks()).await.unwrap();

    // Transfer event signature
    let transfer_topic = hex::decode(
        "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
    )
    .unwrap();

    let (count,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM event_logs WHERE topic0 = ?",
    )
    .bind(&transfer_topic)
    .fetch_one(db.pool())
    .await
    .unwrap();

    assert!(count > 0, "should find Transfer events across the range");
}

#[tokio::test]
async fn backfill_blocks_cover_full_range() {
    let db = setup().await;
    db.insert_batch(&load_all_blocks()).await.unwrap();

    let rows: Vec<(i64,)> = sqlx::query_as(
        "SELECT block_number FROM blocks ORDER BY block_number",
    )
    .fetch_all(db.pool())
    .await
    .unwrap();

    let block_numbers: Vec<i64> = rows.into_iter().map(|r| r.0).collect();
    let expected: Vec<i64> = (5_000_035..=5_000_045).collect();
    assert_eq!(block_numbers, expected, "should have contiguous range");
}
