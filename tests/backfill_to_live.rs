//! E2E test for the backfill-to-live transition flow.
//!
//! Simulates: `hypercore-indexer backfill --from 5000035` (no --to)
//! Expected: backfill all available blocks → cursor at tip → ready for live mode.
//!
//! Uses local fixtures as mock S3 source (11 blocks: 5000035-5000045).

use hypercore_indexer::decode;
use hypercore_indexer::s3::codec;
use hypercore_indexer::storage::sqlite::SqliteStorage;
use hypercore_indexer::storage::Storage;

fn fixture_dir() -> String {
    format!("{}/tests/fixtures/range", env!("CARGO_MANIFEST_DIR"))
}

fn load_and_decode(block_number: u64) -> decode::types::DecodedBlock {
    let path = format!("{}/{block_number}.rmp.lz4", fixture_dir());
    let compressed = std::fs::read(&path)
        .unwrap_or_else(|e| panic!("Missing fixture {path}: {e}"));
    let raw = codec::decode_block(&compressed).unwrap();
    decode::decode_block(&raw, 999).unwrap()
}

async fn setup() -> SqliteStorage {
    let db = SqliteStorage::connect("sqlite::memory:").await.unwrap();
    db.ensure_schema().await.unwrap();
    db
}

const ALL_BLOCKS: std::ops::RangeInclusive<u64> = 5_000_035..=5_000_045;

// ============================================================================
// Simulate: backfill --from 5000035 (no --to)
// The "tip" is 5000045 (last available fixture block).
// After backfill, cursor should be at 5000045, ready for live.
// ============================================================================

#[tokio::test]
async fn backfill_from_only_indexes_all_to_tip() {
    let db = setup().await;
    let from = 5_000_035u64;
    let tip = 5_000_045u64; // simulated S3 tip

    // Phase 1: Simulate backfill from `from` to discovered `tip`
    let blocks: Vec<_> = (from..=tip).map(load_and_decode).collect();
    assert_eq!(blocks.len(), 11);

    // Process all blocks sequentially (like backfill does)
    for (i, block) in blocks.iter().enumerate() {
        let cursor = from + i as u64;
        db.insert_batch_and_set_cursor(
            std::slice::from_ref(block),
            "mainnet",
            cursor,
        )
        .await
        .unwrap();
    }

    // Verify: all blocks stored
    let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM blocks")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(count, 11);

    // Verify: cursor is at tip
    let cursor = db.get_cursor("mainnet").await.unwrap();
    assert_eq!(cursor, Some(tip));

    // Phase 2: "Transition to live" — cursor is at tip
    // Live mode would start by trying to fetch tip+1 = 5000046
    // Since 5000046 doesn't exist in fixtures, it would backoff (S3 404)
    // This is correct behavior — we're at the tip
    let next_block = cursor.unwrap() + 1;
    assert_eq!(next_block, 5_000_046);
}

#[tokio::test]
async fn backfill_from_only_cursor_allows_live_resume() {
    let db = setup().await;

    // Simulate: backfill from 5000035 to tip (5000045)
    let blocks: Vec<_> = ALL_BLOCKS.map(load_and_decode).collect();
    db.insert_batch_and_set_cursor(&blocks, "mainnet", 5_000_045)
        .await
        .unwrap();

    // Simulate: process crashes or user restarts

    // Simulate: `hypercore-indexer live` reads cursor
    let cursor = db.get_cursor("mainnet").await.unwrap();
    assert_eq!(cursor, Some(5_000_045));

    // Live mode starts from cursor+1 = 5000046
    // This is the correct resume point after backfill-to-live transition
    let resume_from = cursor.unwrap() + 1;
    assert_eq!(resume_from, 5_000_046);

    // Verify data integrity: all 11 blocks with correct counts
    let rows: Vec<(i64, i32)> = sqlx::query_as(
        "SELECT block_number, tx_count FROM blocks ORDER BY block_number",
    )
    .fetch_all(db.pool())
    .await
    .unwrap();
    assert_eq!(rows.len(), 11);
    assert_eq!(rows.first().unwrap().0, 5_000_035);
    assert_eq!(rows.last().unwrap().0, 5_000_045);
}

#[tokio::test]
async fn backfill_from_only_contiguous_blocks_no_gaps() {
    let db = setup().await;

    // Simulate backfill processing blocks one at a time (like the pipeline does)
    for bn in ALL_BLOCKS {
        let block = load_and_decode(bn);
        db.insert_batch_and_set_cursor(
            std::slice::from_ref(&block),
            "mainnet",
            bn,
        )
        .await
        .unwrap();
    }

    // Verify contiguous: block_number sequence has no gaps
    let rows: Vec<(i64,)> = sqlx::query_as(
        "SELECT block_number FROM blocks ORDER BY block_number",
    )
    .fetch_all(db.pool())
    .await
    .unwrap();

    let block_numbers: Vec<i64> = rows.into_iter().map(|r| r.0).collect();
    let expected: Vec<i64> = (5_000_035..=5_000_045).collect();
    assert_eq!(block_numbers, expected, "block range must be contiguous");

    // Verify parent hash chain
    for window in block_numbers.windows(2) {
        let parent = load_and_decode(window[0] as u64);
        let child = load_and_decode(window[1] as u64);
        assert_eq!(
            child.parent_hash, parent.hash,
            "block {} parent_hash must match block {} hash",
            window[1], window[0]
        );
    }
}

#[tokio::test]
async fn backfill_from_only_total_data_correct() {
    let db = setup().await;

    // Backfill all 11 blocks
    let blocks: Vec<_> = ALL_BLOCKS.map(load_and_decode).collect();
    db.insert_batch_and_set_cursor(&blocks, "mainnet", 5_000_045)
        .await
        .unwrap();

    // Pinned totals from earlier tests
    let (tx_count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM transactions")
        .fetch_one(db.pool()).await.unwrap();
    let (stx_count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM system_transfers")
        .fetch_one(db.pool()).await.unwrap();
    let (log_count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM event_logs")
        .fetch_one(db.pool()).await.unwrap();

    assert_eq!(tx_count, 54, "total transactions across 11 blocks");
    assert_eq!(stx_count, 5, "total system transfers");
    assert!(log_count > 0, "should have event logs");

    // Gas totals make sense
    let (total_gas,): (i64,) = sqlx::query_as("SELECT SUM(gas_used) FROM blocks")
        .fetch_one(db.pool()).await.unwrap();
    assert!(total_gas > 0);
}

#[tokio::test]
async fn backfill_from_only_then_live_processes_new_block() {
    let db = setup().await;

    // Phase 1: Backfill 5000035-5000044 (stop 1 before tip)
    let blocks: Vec<_> = (5_000_035..=5_000_044).map(load_and_decode).collect();
    db.insert_batch_and_set_cursor(&blocks, "mainnet", 5_000_044)
        .await
        .unwrap();

    assert_eq!(db.get_cursor("mainnet").await.unwrap(), Some(5_000_044));

    // Phase 2: "Live mode" picks up cursor+1 = 5000045
    let next = load_and_decode(5_000_045);
    db.insert_batch_and_set_cursor(
        std::slice::from_ref(&next),
        "mainnet",
        5_000_045,
    )
    .await
    .unwrap();

    // Cursor advanced
    assert_eq!(db.get_cursor("mainnet").await.unwrap(), Some(5_000_045));

    // All 11 blocks now present
    let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM blocks")
        .fetch_one(db.pool()).await.unwrap();
    assert_eq!(count, 11);

    // No gaps
    let rows: Vec<(i64,)> = sqlx::query_as(
        "SELECT block_number FROM blocks ORDER BY block_number",
    )
    .fetch_all(db.pool())
    .await
    .unwrap();
    let expected: Vec<i64> = (5_000_035..=5_000_045).collect();
    assert_eq!(
        rows.into_iter().map(|r| r.0).collect::<Vec<_>>(),
        expected
    );
}

#[tokio::test]
async fn backfill_from_only_idempotent_on_restart() {
    let db = setup().await;

    // First run: backfill all 11 blocks
    let blocks: Vec<_> = ALL_BLOCKS.map(load_and_decode).collect();
    db.insert_batch_and_set_cursor(&blocks, "mainnet", 5_000_045)
        .await
        .unwrap();

    // Simulate restart: re-process last 3 blocks (overlap)
    let overlap: Vec<_> = (5_000_043..=5_000_045).map(load_and_decode).collect();
    db.insert_batch_and_set_cursor(&overlap, "mainnet", 5_000_045)
        .await
        .unwrap();

    // Still exactly 11 blocks (no duplicates)
    let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM blocks")
        .fetch_one(db.pool()).await.unwrap();
    assert_eq!(count, 11);

    // Cursor unchanged
    assert_eq!(db.get_cursor("mainnet").await.unwrap(), Some(5_000_045));
}
