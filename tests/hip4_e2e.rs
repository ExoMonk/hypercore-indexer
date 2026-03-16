//! HIP4 end-to-end tests using real testnet fixture blocks.
//!
//! Fixture blocks live in `tests/fixtures/hip4_range/` (LZ4+MessagePack).
//! All values are pinned to real testnet data (chain_id 998).
//!
//! Actual deposit distribution discovered from fixtures:
//!
//! | Block    | Contest | Side | Depositor                                  |
//! |----------|---------|------|--------------------------------------------|
//! | 48191995 | 595     | 9    | 0x9977c069e21f7eaa599aee6a599de53f1982b9fe |
//! | 48192000 | 595     | 9    | 0xabb5b9505df12a1863de8551d451b100555cfbd2 |
//! | 48192001 | 595     | 9    | 0xf930108d4fa13d1fc08adf6b55557b2ed6e9f97e |
//! | 48194499 | 596     | 5    | 0x537e16545a41f05bf8b665abfd44f4b6a6d71496 |
//! | 48194500 | 596     | 5    | 0xe392d3fb44fb0a9e8d20da63cd3e7a47620c8af5 |
//! | 48225000 | 604     | 9    | 0x296d9ddcffe5153e91c30fc922e574fd59ed7dca |
//! | 48240000 | 608     | 10   | 0xb81a89303ac68352885e423ae7fc2c4c377b7ed1 |
//!
//! All deposits are 0.1 ETH = 100000000000000000 wei.
//! Blocks without deposits: 48194501, 48224999, 48240001.

use alloy_primitives::{Address, U256};
use hypercore_indexer::config::Hip4Config;
use hypercore_indexer::decode;
use hypercore_indexer::hip4;
use hypercore_indexer::hip4::types::Hip4BlockData;
use hypercore_indexer::s3::codec;
use hypercore_indexer::storage::sqlite::SqliteStorage;
use hypercore_indexer::storage::Storage;

const CHAIN_ID: u64 = 998;

const HIP4_BLOCKS: [u64; 10] = [
    48191995, 48192000, 48192001, 48194499, 48194500, 48194501, 48224999, 48225000, 48240000,
    48240001,
];

/// Blocks that contain at least one HIP4 deposit (7 of 10).
const DEPOSIT_BLOCKS: [u64; 7] = [
    48191995, 48192000, 48192001, 48194499, 48194500, 48225000, 48240000,
];

/// Blocks with no HIP4 events (3 of 10).
const EMPTY_BLOCKS: [u64; 3] = [48194501, 48224999, 48240001];

const DEPOSIT_AMOUNT_WEI: u128 = 100_000_000_000_000_000; // 0.1 ETH

/// Total deposits across all 10 fixture blocks.
const TOTAL_DEPOSITS: usize = 7;

/// Distinct contest IDs across all deposits.
const DISTINCT_CONTESTS: [i64; 4] = [595, 596, 604, 608];

fn load_hip4_fixture(block_number: u64) -> Vec<u8> {
    let path = format!(
        "{}/tests/fixtures/hip4_range/{block_number}.rmp.lz4",
        env!("CARGO_MANIFEST_DIR")
    );
    std::fs::read(&path).unwrap_or_else(|e| panic!("failed to read fixture {path}: {e}"))
}

fn hip4_config() -> Hip4Config {
    Hip4Config {
        enabled: true,
        contest_address: Some(
            "0x4fd772e5708da2a7f097f51b3127e515a72744bd".to_string(),
        ),
        api_url: None,
        meta_poll_interval_s: None,
        price_poll_interval_s: None,
    }
}

/// Decode a fixture block through the full LZ4 -> MessagePack -> DecodedBlock pipeline.
fn decode_fixture(block_number: u64) -> hypercore_indexer::decode::types::DecodedBlock {
    let compressed = load_hip4_fixture(block_number);
    let raw = codec::decode_block(&compressed).unwrap_or_else(|e| {
        panic!("codec::decode_block failed for block {block_number}: {e}")
    });
    decode::decode_block(&raw, CHAIN_ID).unwrap_or_else(|e| {
        panic!("decode::decode_block failed for block {block_number}: {e}")
    })
}

/// Process a fixture block through the HIP4 pipeline with default config.
fn process_fixture(block_number: u64) -> Hip4BlockData {
    let decoded = decode_fixture(block_number);
    hip4::process_block(&decoded, &hip4_config())
}

/// Set up an in-memory SQLite storage with schema.
async fn setup_sqlite() -> SqliteStorage {
    let storage = SqliteStorage::connect("sqlite::memory:")
        .await
        .expect("failed to connect to in-memory SQLite");
    storage
        .ensure_schema()
        .await
        .expect("failed to ensure schema");
    storage
}

/// Decode, process HIP4, and store all 10 fixture blocks.
async fn load_all_blocks_into_sqlite(storage: &SqliteStorage, config: &Hip4Config) {
    for &bn in &HIP4_BLOCKS {
        let decoded = decode_fixture(bn);
        storage
            .insert_block(&decoded)
            .await
            .unwrap_or_else(|e| panic!("insert_block failed for {bn}: {e}"));
        let hip4_data = hip4::process_block(&decoded, config);
        storage
            .insert_hip4_data(&hip4_data)
            .await
            .unwrap_or_else(|e| panic!("insert_hip4_data failed for {bn}: {e}"));
    }
}

// ---------------------------------------------------------------------------
// Test 1: All 10 blocks decode correctly
// ---------------------------------------------------------------------------
#[test]
fn all_10_blocks_decode_correctly() {
    let mut total_tx_count = 0usize;
    for &bn in &HIP4_BLOCKS {
        let decoded = decode_fixture(bn);
        assert_eq!(decoded.number, bn, "block number mismatch for fixture {bn}");
        total_tx_count += decoded.transactions.len();
    }
    // All 10 blocks decode and produce at least some transactions in aggregate.
    assert!(
        total_tx_count > 0,
        "expected transactions across 10 blocks, got 0"
    );
}

// ---------------------------------------------------------------------------
// Test 2: Exactly 7 blocks have contest deposits
// ---------------------------------------------------------------------------
#[test]
fn exactly_7_blocks_have_contest_deposits() {
    let mut blocks_with_deposits: Vec<u64> = Vec::new();
    for &bn in &HIP4_BLOCKS {
        let data = process_fixture(bn);
        if !data.deposits.is_empty() {
            blocks_with_deposits.push(bn);
        }
    }
    assert_eq!(
        blocks_with_deposits,
        DEPOSIT_BLOCKS.to_vec(),
        "deposit block set mismatch"
    );
}

// ---------------------------------------------------------------------------
// Test 3: Deposit fields pinned for block 48192000
// ---------------------------------------------------------------------------
#[test]
fn deposit_fields_pinned_block_48192000() {
    let data = process_fixture(48192000);
    assert_eq!(data.deposits.len(), 1, "expected exactly 1 deposit");
    let d = &data.deposits[0];
    assert_eq!(d.contest_id, 595);
    assert_eq!(d.side_id, 9);
    assert_eq!(
        d.depositor,
        "0xabb5b9505df12a1863de8551d451b100555cfbd2"
            .parse::<Address>()
            .unwrap()
    );
    assert_eq!(d.amount_wei, U256::from(DEPOSIT_AMOUNT_WEI));
    assert_eq!(d.block_number, 48192000);
}

// ---------------------------------------------------------------------------
// Test 4: Deposit fields pinned for block 48194500
// ---------------------------------------------------------------------------
#[test]
fn deposit_fields_pinned_block_48194500() {
    let data = process_fixture(48194500);
    assert_eq!(data.deposits.len(), 1, "expected exactly 1 deposit");
    let d = &data.deposits[0];
    assert_eq!(d.contest_id, 596);
    assert_eq!(d.side_id, 5);
    assert_eq!(
        d.depositor,
        "0xe392d3fb44fb0a9e8d20da63cd3e7a47620c8af5"
            .parse::<Address>()
            .unwrap()
    );
    assert_eq!(d.amount_wei, U256::from(DEPOSIT_AMOUNT_WEI));
    assert_eq!(d.block_number, 48194500);
}

// ---------------------------------------------------------------------------
// Test 5: Deposit fields pinned for block 48225000
// ---------------------------------------------------------------------------
#[test]
fn deposit_fields_pinned_block_48225000() {
    let data = process_fixture(48225000);
    assert_eq!(data.deposits.len(), 1, "expected exactly 1 deposit");
    let d = &data.deposits[0];
    assert_eq!(d.contest_id, 604);
    assert_eq!(d.side_id, 9);
    assert_eq!(
        d.depositor,
        "0x296d9ddcffe5153e91c30fc922e574fd59ed7dca"
            .parse::<Address>()
            .unwrap()
    );
    assert_eq!(d.amount_wei, U256::from(DEPOSIT_AMOUNT_WEI));
    assert_eq!(d.block_number, 48225000);
}

// ---------------------------------------------------------------------------
// Test 6: Deposit fields pinned for block 48240000
// ---------------------------------------------------------------------------
#[test]
fn deposit_fields_pinned_block_48240000() {
    let data = process_fixture(48240000);
    assert_eq!(data.deposits.len(), 1, "expected exactly 1 deposit");
    let d = &data.deposits[0];
    assert_eq!(d.contest_id, 608);
    assert_eq!(d.side_id, 10);
    assert_eq!(
        d.depositor,
        "0xb81a89303ac68352885e423ae7fc2c4c377b7ed1"
            .parse::<Address>()
            .unwrap()
    );
    assert_eq!(d.amount_wei, U256::from(DEPOSIT_AMOUNT_WEI));
    assert_eq!(d.block_number, 48240000);
}

// ---------------------------------------------------------------------------
// Test 6b: Deposit fields pinned for block 48191995
// ---------------------------------------------------------------------------
#[test]
fn deposit_fields_pinned_block_48191995() {
    let data = process_fixture(48191995);
    assert_eq!(data.deposits.len(), 1, "expected exactly 1 deposit");
    let d = &data.deposits[0];
    assert_eq!(d.contest_id, 595);
    assert_eq!(d.side_id, 9);
    assert_eq!(
        d.depositor,
        "0x9977c069e21f7eaa599aee6a599de53f1982b9fe"
            .parse::<Address>()
            .unwrap()
    );
    assert_eq!(d.amount_wei, U256::from(DEPOSIT_AMOUNT_WEI));
    assert_eq!(d.block_number, 48191995);
}

// ---------------------------------------------------------------------------
// Test 6c: Deposit fields pinned for block 48192001
// ---------------------------------------------------------------------------
#[test]
fn deposit_fields_pinned_block_48192001() {
    let data = process_fixture(48192001);
    assert_eq!(data.deposits.len(), 1, "expected exactly 1 deposit");
    let d = &data.deposits[0];
    assert_eq!(d.contest_id, 595);
    assert_eq!(d.side_id, 9);
    assert_eq!(
        d.depositor,
        "0xf930108d4fa13d1fc08adf6b55557b2ed6e9f97e"
            .parse::<Address>()
            .unwrap()
    );
    assert_eq!(d.amount_wei, U256::from(DEPOSIT_AMOUNT_WEI));
    assert_eq!(d.block_number, 48192001);
}

// ---------------------------------------------------------------------------
// Test 6d: Deposit fields pinned for block 48194499
// ---------------------------------------------------------------------------
#[test]
fn deposit_fields_pinned_block_48194499() {
    let data = process_fixture(48194499);
    assert_eq!(data.deposits.len(), 1, "expected exactly 1 deposit");
    let d = &data.deposits[0];
    assert_eq!(d.contest_id, 596);
    assert_eq!(d.side_id, 5);
    assert_eq!(
        d.depositor,
        "0x537e16545a41f05bf8b665abfd44f4b6a6d71496"
            .parse::<Address>()
            .unwrap()
    );
    assert_eq!(d.amount_wei, U256::from(DEPOSIT_AMOUNT_WEI));
    assert_eq!(d.block_number, 48194499);
}

// ---------------------------------------------------------------------------
// Test 7: Non-contest blocks produce no HIP4 data
// ---------------------------------------------------------------------------
#[test]
fn non_contest_blocks_produce_no_hip4_data() {
    for &bn in &EMPTY_BLOCKS {
        let data = process_fixture(bn);
        assert!(
            data.deposits.is_empty(),
            "block {bn} should have 0 deposits, got {}",
            data.deposits.len()
        );
        assert!(
            data.claims.is_empty(),
            "block {bn} should have 0 claims, got {}",
            data.claims.len()
        );
    }
}

// ---------------------------------------------------------------------------
// Test 8: Full pipeline — store and query deposits
// ---------------------------------------------------------------------------
#[tokio::test]
async fn full_pipeline_store_and_query_deposits() {
    let storage = setup_sqlite().await;
    let config = hip4_config();
    load_all_blocks_into_sqlite(&storage, &config).await;

    // Exactly 7 deposit rows
    let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM hip4_deposits")
        .fetch_one(storage.pool())
        .await
        .unwrap();
    assert_eq!(row.0, TOTAL_DEPOSITS as i64, "expected 7 hip4_deposits rows");

    // Query by contest_id=595: 3 deposits (blocks 48191995, 48192000, 48192001)
    let rows: Vec<(Vec<u8>,)> =
        sqlx::query_as("SELECT depositor FROM hip4_deposits WHERE contest_id = 595 ORDER BY block_number")
            .fetch_all(storage.pool())
            .await
            .unwrap();
    assert_eq!(rows.len(), 3, "contest 595 should have 3 deposits");
    let expected_first: Address = "0x9977c069e21f7eaa599aee6a599de53f1982b9fe"
        .parse()
        .unwrap();
    assert_eq!(rows[0].0.as_slice(), expected_first.as_slice());

    // Query by contest_id=608: 1 deposit
    let rows: Vec<(Vec<u8>,)> =
        sqlx::query_as("SELECT depositor FROM hip4_deposits WHERE contest_id = 608")
            .fetch_all(storage.pool())
            .await
            .unwrap();
    assert_eq!(rows.len(), 1);
    let expected_depositor: Address = "0xb81a89303ac68352885e423ae7fc2c4c377b7ed1"
        .parse()
        .unwrap();
    assert_eq!(rows[0].0.as_slice(), expected_depositor.as_slice());
}

// ---------------------------------------------------------------------------
// Test 9: Query deposits by user address
// ---------------------------------------------------------------------------
#[tokio::test]
async fn query_deposits_by_user_address() {
    let storage = setup_sqlite().await;
    let config = hip4_config();
    load_all_blocks_into_sqlite(&storage, &config).await;

    let depositor: Address = "0xabb5b9505df12a1863de8551d451b100555cfbd2"
        .parse()
        .unwrap();

    let rows: Vec<(i64,)> =
        sqlx::query_as("SELECT contest_id FROM hip4_deposits WHERE depositor = ?")
            .bind(depositor.as_slice())
            .fetch_all(storage.pool())
            .await
            .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 595);

    // Unknown address returns 0 results
    let unknown: Address = "0x0000000000000000000000000000000000000001"
        .parse()
        .unwrap();
    let rows: Vec<(i64,)> =
        sqlx::query_as("SELECT contest_id FROM hip4_deposits WHERE depositor = ?")
            .bind(unknown.as_slice())
            .fetch_all(storage.pool())
            .await
            .unwrap();
    assert_eq!(rows.len(), 0);
}

// ---------------------------------------------------------------------------
// Test 10: Query all contests in range
// ---------------------------------------------------------------------------
#[tokio::test]
async fn query_all_contests_in_range() {
    let storage = setup_sqlite().await;
    let config = hip4_config();
    load_all_blocks_into_sqlite(&storage, &config).await;

    let rows: Vec<(i64,)> = sqlx::query_as(
        "SELECT DISTINCT contest_id FROM hip4_deposits ORDER BY contest_id",
    )
    .fetch_all(storage.pool())
    .await
    .unwrap();

    let contest_ids: Vec<i64> = rows.into_iter().map(|(id,)| id).collect();
    assert_eq!(contest_ids, DISTINCT_CONTESTS.to_vec());
}

// ---------------------------------------------------------------------------
// Test 11: Idempotent reprocess HIP4
// ---------------------------------------------------------------------------
#[tokio::test]
async fn idempotent_reprocess_hip4() {
    let storage = setup_sqlite().await;
    let config = hip4_config();

    // Process and store twice
    load_all_blocks_into_sqlite(&storage, &config).await;
    load_all_blocks_into_sqlite(&storage, &config).await;

    let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM hip4_deposits")
        .fetch_one(storage.pool())
        .await
        .unwrap();
    assert_eq!(
        row.0, TOTAL_DEPOSITS as i64,
        "idempotent reprocess should still yield {TOTAL_DEPOSITS} rows"
    );
}

// ---------------------------------------------------------------------------
// Test 12: HIP4 disabled stores nothing
// ---------------------------------------------------------------------------
#[tokio::test]
async fn hip4_disabled_stores_nothing() {
    let storage = setup_sqlite().await;
    let disabled_config = Hip4Config {
        enabled: false,
        contest_address: Some(
            "0x4fd772e5708da2a7f097f51b3127e515a72744bd".to_string(),
        ),
        api_url: None,
        meta_poll_interval_s: None,
        price_poll_interval_s: None,
    };

    load_all_blocks_into_sqlite(&storage, &disabled_config).await;

    // hip4_deposits should be empty
    let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM hip4_deposits")
        .fetch_one(storage.pool())
        .await
        .unwrap();
    assert_eq!(row.0, 0, "HIP4 disabled should store 0 deposits");

    // blocks table should still have 10 rows
    let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM blocks")
        .fetch_one(storage.pool())
        .await
        .unwrap();
    assert_eq!(row.0, 10, "blocks should still have 10 rows");
}

// ---------------------------------------------------------------------------
// Test 13: HIP4 wrong contract address stores nothing
// ---------------------------------------------------------------------------
#[tokio::test]
async fn hip4_wrong_contract_address_stores_nothing() {
    let storage = setup_sqlite().await;
    let wrong_config = Hip4Config {
        enabled: true,
        contest_address: Some(
            "0x0000000000000000000000000000000000000000".to_string(),
        ),
        api_url: None,
        meta_poll_interval_s: None,
        price_poll_interval_s: None,
    };

    load_all_blocks_into_sqlite(&storage, &wrong_config).await;

    let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM hip4_deposits")
        .fetch_one(storage.pool())
        .await
        .unwrap();
    assert_eq!(row.0, 0, "wrong contract address should store 0 deposits");
}

// ---------------------------------------------------------------------------
// Test 14: Endgoal query — deposits by contest and side
// ---------------------------------------------------------------------------
#[tokio::test]
async fn endgoal_query_deposits_by_contest_and_side() {
    let storage = setup_sqlite().await;
    let config = hip4_config();
    load_all_blocks_into_sqlite(&storage, &config).await;

    // SUM(CAST(...)) returns INTEGER in SQLite, so fetch as i64 not String.
    let rows: Vec<(i64, i64, i64, i64)> = sqlx::query_as(
        r#"SELECT contest_id, side_id, COUNT(*) as deposits, SUM(CAST(amount_wei AS INTEGER)) as total
           FROM hip4_deposits GROUP BY contest_id, side_id ORDER BY contest_id"#,
    )
    .fetch_all(storage.pool())
    .await
    .unwrap();

    assert_eq!(rows.len(), 4, "expected 4 contest/side groups");

    // (contest_id, side_id, deposit_count, total_wei)
    let expected: [(i64, i64, i64, i64); 4] = [
        (595, 9, 3, 3 * DEPOSIT_AMOUNT_WEI as i64),  // 3 deposits in contest 595
        (596, 5, 2, 2 * DEPOSIT_AMOUNT_WEI as i64),  // 2 deposits in contest 596
        (604, 9, 1, DEPOSIT_AMOUNT_WEI as i64),
        (608, 10, 1, DEPOSIT_AMOUNT_WEI as i64),
    ];

    for (i, (contest_id, side_id, count, total)) in rows.iter().enumerate() {
        assert_eq!(*contest_id, expected[i].0, "contest_id mismatch at row {i}");
        assert_eq!(*side_id, expected[i].1, "side_id mismatch at row {i}");
        assert_eq!(*count, expected[i].2, "deposit count mismatch at row {i}");
        assert_eq!(
            *total, expected[i].3,
            "total amount_wei mismatch at row {i}"
        );
    }
}

// ---------------------------------------------------------------------------
// Test 15: Endgoal query — top depositors
// ---------------------------------------------------------------------------
#[tokio::test]
async fn endgoal_query_top_depositors() {
    let storage = setup_sqlite().await;
    let config = hip4_config();
    load_all_blocks_into_sqlite(&storage, &config).await;

    let rows: Vec<(Vec<u8>, i64, i64)> = sqlx::query_as(
        r#"SELECT depositor, COUNT(DISTINCT contest_id) as contests, COUNT(*) as total_deposits
           FROM hip4_deposits GROUP BY depositor ORDER BY total_deposits DESC"#,
    )
    .fetch_all(storage.pool())
    .await
    .unwrap();

    assert_eq!(rows.len(), 7, "expected 7 unique depositors");

    // Each depositor has exactly 1 deposit in 1 contest
    for (i, (_depositor, contests, total)) in rows.iter().enumerate() {
        assert_eq!(*contests, 1, "depositor {i} should participate in 1 contest");
        assert_eq!(*total, 1, "depositor {i} should have 1 total deposit");
    }

    // Verify the depositor addresses are the expected set (all 7)
    let expected_depositors: Vec<Address> = vec![
        "0x9977c069e21f7eaa599aee6a599de53f1982b9fe".parse().unwrap(),
        "0xabb5b9505df12a1863de8551d451b100555cfbd2".parse().unwrap(),
        "0xf930108d4fa13d1fc08adf6b55557b2ed6e9f97e".parse().unwrap(),
        "0x537e16545a41f05bf8b665abfd44f4b6a6d71496".parse().unwrap(),
        "0xe392d3fb44fb0a9e8d20da63cd3e7a47620c8af5".parse().unwrap(),
        "0x296d9ddcffe5153e91c30fc922e574fd59ed7dca".parse().unwrap(),
        "0xb81a89303ac68352885e423ae7fc2c4c377b7ed1".parse().unwrap(),
    ];

    let mut actual_addresses: Vec<Vec<u8>> =
        rows.iter().map(|(d, _, _)| d.clone()).collect();
    actual_addresses.sort();

    let mut expected_bytes: Vec<Vec<u8>> = expected_depositors
        .iter()
        .map(|a| a.as_slice().to_vec())
        .collect();
    expected_bytes.sort();

    assert_eq!(actual_addresses, expected_bytes, "depositor set mismatch");
}
