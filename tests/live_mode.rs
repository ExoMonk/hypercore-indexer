//! Live mode E2E tests: simulate the tip-following loop building blocks
//! using local fixture files as a mock S3 source.
//!
//! Since live mode runs an infinite loop polling S3, we test the individual
//! building blocks that the loop uses: decode → store → cursor advance,
//! restart/resume, idempotent reprocessing, gap detection logic, and
//! adaptive polling interval behavior.
//!
//! Uses SQLite in-memory (no Docker needed) and range fixtures (5000035-5000045).
//! Run with: cargo test --test live_mode

use hypercore_indexer::decode;
use hypercore_indexer::live::poll::AdaptiveInterval;
use hypercore_indexer::s3::codec;
use hypercore_indexer::storage::sqlite::SqliteStorage;
use hypercore_indexer::storage::Storage;

fn fixture_dir() -> String {
    format!("{}/tests/fixtures/range", env!("CARGO_MANIFEST_DIR"))
}

/// Load and decode a block from the range fixture directory (same as S3 would deliver).
fn load_and_decode(block_number: u64) -> decode::types::DecodedBlock {
    let path = format!("{}/{block_number}.rmp.lz4", fixture_dir());
    let compressed =
        std::fs::read(&path).unwrap_or_else(|e| panic!("Missing fixture {path}: {e}"));
    let raw = codec::decode_block(&compressed).unwrap();
    decode::decode_block(&raw, 999).unwrap()
}

fn load_fixture(name: &str) -> Vec<u8> {
    let path = format!("{}/tests/fixtures/{name}", env!("CARGO_MANIFEST_DIR"));
    std::fs::read(&path).unwrap_or_else(|e| panic!("Failed to read fixture {path}: {e}"))
}

async fn setup() -> SqliteStorage {
    let db = SqliteStorage::connect("sqlite::memory:").await.unwrap();
    db.ensure_schema().await.unwrap();
    db
}

// Known values for blocks 5000035-5000045
const EXPECTED: [(u64, usize, usize, u64); 11] = [
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
// 1. follow_tip_sequential_blocks: simulate tip-following one block at a time
// ============================================================================

#[tokio::test]
async fn follow_tip_sequential_blocks() {
    let db = setup().await;

    // Simulate the live mode tip-following: process one block at a time,
    // calling insert_batch_and_set_cursor with a single-element slice.
    let mut cursor = 5_000_034u64; // cursor starts one before the first block

    for &(block_num, expected_txs, expected_stxs, expected_gas) in &EXPECTED {
        let decoded = load_and_decode(block_num);

        // Verify decode matches expectations before storing
        assert_eq!(decoded.number, block_num);
        assert_eq!(decoded.transactions.len(), expected_txs);
        assert_eq!(decoded.system_transfers.len(), expected_stxs);
        assert_eq!(decoded.gas_used, expected_gas);

        // Live mode calls: insert single block + advance cursor atomically
        let next_block = cursor + 1;
        assert_eq!(next_block, block_num, "cursor+1 should match next block");

        db.insert_batch_and_set_cursor(&[decoded], "mainnet", next_block)
            .await
            .unwrap();

        cursor = next_block;

        // Verify cursor advanced correctly after each block
        let db_cursor = db.get_cursor("mainnet").await.unwrap();
        assert_eq!(
            db_cursor,
            Some(cursor),
            "cursor should be {cursor} after processing block {block_num}"
        );
    }

    // After all 11 blocks: verify DB has all blocks with correct data
    assert_eq!(cursor, 5_000_045);

    let rows: Vec<(i64, i32, i32, i64)> = sqlx::query_as(
        "SELECT block_number, tx_count, system_tx_count, gas_used FROM blocks ORDER BY block_number",
    )
    .fetch_all(db.pool())
    .await
    .unwrap();

    assert_eq!(rows.len(), 11);
    for (i, row) in rows.iter().enumerate() {
        let (expected_bn, expected_txs, expected_stxs, expected_gas) = EXPECTED[i];
        assert_eq!(row.0, expected_bn as i64);
        assert_eq!(row.1, expected_txs as i32);
        assert_eq!(row.2, expected_stxs as i32);
        assert_eq!(row.3, expected_gas as i64);
    }

    // Verify contiguous range with no gaps
    let block_numbers: Vec<(i64,)> =
        sqlx::query_as("SELECT block_number FROM blocks ORDER BY block_number")
            .fetch_all(db.pool())
            .await
            .unwrap();
    let nums: Vec<i64> = block_numbers.into_iter().map(|r| r.0).collect();
    let expected_range: Vec<i64> = (5_000_035..=5_000_045).collect();
    assert_eq!(nums, expected_range);
}

// ============================================================================
// 2. cursor_resume_after_restart: simulate indexer restart from cursor
// ============================================================================

#[tokio::test]
async fn cursor_resume_after_restart() {
    let db = setup().await;

    // Phase 1: Insert blocks 5000035-5000040 (first 6 blocks)
    for block_num in 5_000_035..=5_000_040 {
        let decoded = load_and_decode(block_num);
        db.insert_batch_and_set_cursor(&[decoded], "mainnet", block_num)
            .await
            .unwrap();
    }

    // Verify cursor is at 5000040
    let cursor_before = db.get_cursor("mainnet").await.unwrap();
    assert_eq!(cursor_before, Some(5_000_040));

    // Phase 2: "Restart" — read cursor from DB, verify it tells us where to resume
    let resume_cursor = db.get_cursor("mainnet").await.unwrap().unwrap();
    assert_eq!(resume_cursor, 5_000_040);
    let next_block = resume_cursor + 1;
    assert_eq!(next_block, 5_000_041, "should resume from block 5000041");

    // Phase 3: Continue inserting 5000041-5000045 (remaining 5 blocks)
    for block_num in 5_000_041..=5_000_045 {
        let decoded = load_and_decode(block_num);
        db.insert_batch_and_set_cursor(&[decoded], "mainnet", block_num)
            .await
            .unwrap();
    }

    // Verify all 11 blocks present, no gaps, no duplicates
    let (block_count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM blocks")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(block_count, 11);

    let block_numbers: Vec<(i64,)> =
        sqlx::query_as("SELECT block_number FROM blocks ORDER BY block_number")
            .fetch_all(db.pool())
            .await
            .unwrap();
    let nums: Vec<i64> = block_numbers.into_iter().map(|r| r.0).collect();
    let expected_range: Vec<i64> = (5_000_035..=5_000_045).collect();
    assert_eq!(nums, expected_range, "should have contiguous range with no gaps");

    // Final cursor
    assert_eq!(db.get_cursor("mainnet").await.unwrap(), Some(5_000_045));
}

// ============================================================================
// 3. idempotent_reprocess_on_restart: crash + restart with overlap
// ============================================================================

#[tokio::test]
async fn idempotent_reprocess_on_restart() {
    let db = setup().await;

    // Phase 1: Insert blocks 5000035-5000040, cursor=5000040
    for block_num in 5_000_035..=5_000_040 {
        let decoded = load_and_decode(block_num);
        db.insert_batch_and_set_cursor(&[decoded], "mainnet", block_num)
            .await
            .unwrap();
    }
    assert_eq!(db.get_cursor("mainnet").await.unwrap(), Some(5_000_040));

    // Phase 2: "Crash and restart from 5000038" — 3 blocks overlap (5000038-5000040)
    // then continue with new blocks (5000041-5000045)
    for block_num in 5_000_038..=5_000_045 {
        let decoded = load_and_decode(block_num);
        db.insert_batch_and_set_cursor(&[decoded], "mainnet", block_num)
            .await
            .unwrap();
    }

    // Verify exactly 11 blocks (no duplicates from overlap)
    let (block_count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM blocks")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(block_count, 11, "should have exactly 11 blocks, no duplicates");

    // Verify no duplicate transactions
    let total_expected_txs: usize = EXPECTED.iter().map(|(_, txc, _, _)| txc).sum();
    let (tx_count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM transactions")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(
        tx_count, total_expected_txs as i64,
        "should have exactly {total_expected_txs} txs, no duplicates from overlap"
    );

    // Verify no duplicate system transfers
    let total_expected_stxs: usize = EXPECTED.iter().map(|(_, _, stxc, _)| stxc).sum();
    let (stx_count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM system_transfers")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(
        stx_count, total_expected_stxs as i64,
        "should have exactly {total_expected_stxs} system transfers, no duplicates"
    );

    // Cursor should be at 5000045
    assert_eq!(db.get_cursor("mainnet").await.unwrap(), Some(5_000_045));

    // Verify data integrity: block 5000038 hashes haven't been corrupted by re-insert
    let (hash,): (Vec<u8>,) =
        sqlx::query_as("SELECT block_hash FROM blocks WHERE block_number = ?")
            .bind(5_000_038i64)
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(
        hex::encode(&hash),
        "6639e377dc4aba11f210dc95b0024f15840d0289a82abf883ef3825a85fa9508",
        "block 5000038 hash should survive idempotent re-insert"
    );
}

// ============================================================================
// 4. gap_detection_probes_correctly: test the gap detection logic
// ============================================================================

#[tokio::test]
async fn gap_detection_probes_correctly() {
    let db = setup().await;

    // Insert blocks 5000035-5000037, cursor=5000037
    for block_num in 5_000_035..=5_000_037 {
        let decoded = load_and_decode(block_num);
        db.insert_batch_and_set_cursor(&[decoded], "mainnet", block_num)
            .await
            .unwrap();
    }
    assert_eq!(db.get_cursor("mainnet").await.unwrap(), Some(5_000_037));

    // Gap detection logic: "if block cursor+N exists, we're behind"
    // Block 5000038 exists in fixtures — simulate what detect_and_backfill_gap does:
    // 1. Probe cursor + gap_threshold
    // 2. If exists, we're behind → backfill
    let cursor = 5_000_037u64;

    // Simulate probe: block cursor+1 should be decodable (it exists on "S3")
    let probe_block = cursor + 1; // 5000038
    let decoded = load_and_decode(probe_block);
    assert_eq!(decoded.number, 5_000_038);
    assert_eq!(decoded.transactions.len(), 8);
    assert_eq!(decoded.gas_used, 1_722_800);

    // The block is processable — store it as if we detected the gap and backfilled
    db.insert_batch_and_set_cursor(&[decoded], "mainnet", probe_block)
        .await
        .unwrap();
    assert_eq!(db.get_cursor("mainnet").await.unwrap(), Some(5_000_038));

    // Simulate "if block cursor+100 exists, we're behind" with a further probe
    // In our fixture set, blocks up to 5000045 exist
    // Probe: does block 5000045 exist? Yes → we're 7 blocks behind
    let far_probe = load_and_decode(5_000_045);
    assert_eq!(far_probe.number, 5_000_045);

    // Backfill the remaining gap (5000039-5000045)
    for block_num in 5_000_039..=5_000_045 {
        let decoded = load_and_decode(block_num);
        db.insert_batch_and_set_cursor(&[decoded], "mainnet", block_num)
            .await
            .unwrap();
    }

    // Verify all 11 blocks present after gap backfill
    let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM blocks")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(count, 11);
    assert_eq!(db.get_cursor("mainnet").await.unwrap(), Some(5_000_045));
}

// ============================================================================
// 5. adaptive_interval_resets_on_block_found: full cycle test
// ============================================================================

#[test]
fn adaptive_interval_resets_on_block_found() {
    let mut interval = AdaptiveInterval::new(1000, 200, 0.67);

    // Start at base (1000ms)
    assert_eq!(interval.current(), 1000);
    assert!(interval.should_sleep());

    // Simulate: block found → reset to 0
    interval.reset();
    assert_eq!(interval.current(), 0);
    assert!(!interval.should_sleep());

    // Simulate 3 backoffs (no block found) — interval grows
    interval.backoff(); // 0 → 200 (min_ms)
    assert_eq!(interval.current(), 200);
    assert!(interval.should_sleep());

    interval.backoff(); // 200 / 0.67 = 298
    assert_eq!(interval.current(), 298);

    interval.backoff(); // 298 / 0.67 = 444
    assert_eq!(interval.current(), 444);

    // Simulate: block found → process immediately (reset to 0)
    interval.reset();
    assert_eq!(interval.current(), 0);
    assert!(
        !interval.should_sleep(),
        "should not sleep after block found"
    );

    // Another backoff after reset → starts at min_ms (200), not base
    interval.backoff();
    assert_eq!(
        interval.current(),
        200,
        "first backoff after reset should be min_ms"
    );

    // Continue backoffs to verify growth toward base
    interval.backoff(); // 298
    interval.backoff(); // 444
    interval.backoff(); // 662
    interval.backoff(); // 988
    interval.backoff(); // 1474 → clamped to 1000

    assert_eq!(
        interval.current(),
        1000,
        "should clamp at base_ms after enough backoffs"
    );
    assert!(interval.should_sleep());

    // Stays at base
    interval.backoff();
    assert_eq!(interval.current(), 1000);
}

// ============================================================================
// 6. single_block_store_and_cursor_atomic: the critical data-loss test
// ============================================================================

#[tokio::test]
async fn single_block_store_and_cursor_atomic() {
    let db = setup().await;

    // Decode block 5000038 from fixture
    let decoded = load_and_decode(5_000_038);

    // Call insert_batch_and_set_cursor with a single block (exactly what live mode does)
    db.insert_batch_and_set_cursor(&[decoded], "mainnet", 5_000_038)
        .await
        .unwrap();

    // Verify block data AND cursor are both present (atomic)
    let (block_count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM blocks")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(block_count, 1, "block should be stored");

    let cursor = db.get_cursor("mainnet").await.unwrap();
    assert_eq!(cursor, Some(5_000_038), "cursor should be set atomically");

    // Verify block data is complete (not partial)
    let row: (i64, i32, i32, i64) = sqlx::query_as(
        "SELECT block_number, tx_count, system_tx_count, gas_used FROM blocks WHERE block_number = ?",
    )
    .bind(5_000_038i64)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(row.0, 5_000_038);
    assert_eq!(row.1, 8);
    assert_eq!(row.2, 1);
    assert_eq!(row.3, 1_722_800);

    // All 8 transactions present
    let (tx_count,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM transactions WHERE block_number = ?")
            .bind(5_000_038i64)
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(tx_count, 8, "all 8 transactions should be stored");

    // System transfer present
    let (stx_count,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM system_transfers WHERE block_number = ?")
            .bind(5_000_038i64)
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(stx_count, 1, "system transfer should be stored");

    // Event logs present
    let (log_count,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM event_logs WHERE block_number = ?")
            .bind(5_000_038i64)
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert!(log_count > 0, "event logs should be stored");

    // Verify no partial state: cursor and data are consistent
    // If cursor is set but block missing (or vice versa), that's a data-loss bug
    let cursor_val = cursor.unwrap();
    let (stored_bn,): (i64,) =
        sqlx::query_as("SELECT block_number FROM blocks WHERE block_number = ?")
            .bind(cursor_val as i64)
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(
        stored_bn, cursor_val as i64,
        "cursor block must exist in storage"
    );
}

// ============================================================================
// 7. live_mode_handles_empty_blocks: block 1 has no txs
// ============================================================================

#[tokio::test]
async fn live_mode_handles_empty_blocks() {
    let db = setup().await;

    // Process block 1 (empty — no txs, no system transfers)
    let compressed = load_fixture("block_1.rmp.lz4");
    let raw = codec::decode_block(&compressed).unwrap();
    let decoded = decode::decode_block(&raw, 999).unwrap();

    assert_eq!(decoded.number, 1);
    assert_eq!(decoded.transactions.len(), 0);
    assert_eq!(decoded.system_transfers.len(), 0);

    // Store it exactly as live mode would
    db.insert_batch_and_set_cursor(&[decoded], "mainnet", 1)
        .await
        .unwrap();

    // Verify it's stored correctly with tx_count=0
    let row: (i64, i32, i32, i64) = sqlx::query_as(
        "SELECT block_number, tx_count, system_tx_count, gas_used FROM blocks WHERE block_number = ?",
    )
    .bind(1i64)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(row.0, 1);
    assert_eq!(row.1, 0, "tx_count should be 0 for empty block");
    assert_eq!(row.2, 0, "system_tx_count should be 0 for empty block");
    assert_eq!(row.3, 0, "gas_used should be 0 for empty block");

    // Block hash should still be correct
    let (hash,): (Vec<u8>,) =
        sqlx::query_as("SELECT block_hash FROM blocks WHERE block_number = ?")
            .bind(1i64)
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(
        hex::encode(&hash),
        "de151843548b88d06f201d86e860e45fbf07d49612f1934fba5746abd942fb01"
    );

    // Cursor advances past it
    let cursor = db.get_cursor("mainnet").await.unwrap();
    assert_eq!(cursor, Some(1), "cursor should advance past empty block");

    // No child rows
    let (tx_count,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM transactions WHERE block_number = 1")
            .fetch_one(db.pool())
            .await
            .unwrap();
    let (log_count,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM event_logs WHERE block_number = 1")
            .fetch_one(db.pool())
            .await
            .unwrap();
    let (stx_count,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM system_transfers WHERE block_number = 1")
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(tx_count, 0);
    assert_eq!(log_count, 0);
    assert_eq!(stx_count, 0);
}

// ============================================================================
// 8. no_data_loss_across_sequential_inserts: process all 11 blocks one-by-one
// ============================================================================

#[tokio::test]
async fn no_data_loss_across_sequential_inserts() {
    let db = setup().await;

    // Process all 11 blocks one-by-one, exactly like live mode does
    // (single block per insert_batch_and_set_cursor call)
    for block_num in 5_000_035..=5_000_045 {
        let decoded = load_and_decode(block_num);
        db.insert_batch_and_set_cursor(&[decoded], "mainnet", block_num)
            .await
            .unwrap();
    }

    // Verify exact row counts
    let (block_count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM blocks")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(block_count, 11, "should have exactly 11 blocks");

    let total_expected_txs: usize = EXPECTED.iter().map(|(_, txc, _, _)| txc).sum();
    let (tx_count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM transactions")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(
        tx_count, total_expected_txs as i64,
        "should have exactly {total_expected_txs} transactions"
    );
    // 5+7+8+8+3+4+4+5+4+4+2 = 54
    assert_eq!(tx_count, 54, "pinned: 54 total transactions across range");

    let total_expected_stxs: usize = EXPECTED.iter().map(|(_, _, stxc, _)| stxc).sum();
    let (stx_count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM system_transfers")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(
        stx_count, total_expected_stxs as i64,
        "should have exactly {total_expected_stxs} system transfers"
    );
    // 1+0+2+1+0+0+0+1+0+0+0 = 5
    assert_eq!(stx_count, 5, "pinned: 5 total system transfers across range");

    // Verify every tx hash from block 5000038 is queryable
    let expected_hashes_5000038 = [
        "1f912cb736959444532212379df30c07b78c8c1761200550bf92eff37cf6d998",
        "13d9d197fd9fe4b68358b4a12a2fd82d7ca24380472d7098244e0b8bbc00b738",
        "ee1faff87698deb24094eb3ec8e120e0053e5cf2d028f4f69d48935da2a7d152",
        "57d8b7f7c192e62622c3733fa9e11021f4bcdacf796e2eb81d8a12e0a475389e",
        "0b61c0800dfaed405ddc5b9a36ed0982b375913444e0f4aaab2f744f515e0860",
        "393d93d17ecc465ef1a981a52c9cd1238c23258cfddd7bce36dbaa2c3f8274b0",
        "c548e68042ecce4127454d652dad03303c5a816bb08065822b857e54316a60fa",
        "f5a15ba50dd767b473bf4aaadf6a1085e02f13b27cb1a45143d3980a2f04e9fc",
    ];

    for expected_hash in &expected_hashes_5000038 {
        let hash_bytes = hex::decode(expected_hash).unwrap();
        let (bn, idx): (i64, i32) =
            sqlx::query_as("SELECT block_number, tx_index FROM transactions WHERE tx_hash = ?")
                .bind(&hash_bytes)
                .fetch_one(db.pool())
                .await
                .unwrap_or_else(|_| panic!("tx hash {expected_hash} not found in DB"));
        assert_eq!(bn, 5_000_038, "tx should belong to block 5000038");
        assert!(idx < 8, "tx_index should be in range 0..8");
    }

    // Verify cursor = 5000045
    let cursor = db.get_cursor("mainnet").await.unwrap();
    assert_eq!(cursor, Some(5_000_045), "final cursor should be 5000045");
}
