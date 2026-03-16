//! Integration tests for node fills parsing and SQLite storage.
//!
//! Uses the 10-line fixture at tests/fixtures/node_fills_sample.jsonl.
//! Run with: cargo test --test fills_pipeline

use hypercore_indexer::fills::parser;
use hypercore_indexer::fills::types::FillRecord;
use hypercore_indexer::storage::sqlite::SqliteStorage;
use hypercore_indexer::storage::Storage;

fn load_fixture() -> String {
    let path = format!(
        "{}/tests/fixtures/node_fills_sample.jsonl",
        env!("CARGO_MANIFEST_DIR")
    );
    std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("Failed to read fixture {path}: {e}"))
}

async fn setup() -> SqliteStorage {
    let db = SqliteStorage::connect("sqlite::memory:").await.unwrap();
    db.ensure_schema().await.unwrap();
    db
}

// ============================================================================
// Parse fixture → insert into SQLite → query back fills by coin
// ============================================================================

#[tokio::test]
async fn fills_insert_and_query_by_coin() {
    let db = setup().await;
    let data = load_fixture();
    let fills = parser::parse_jsonl(&data).unwrap();

    // The fixture has 1388 events (buyer + seller for each trade).
    // With composite PK (trade_id, user_address), both sides are stored.
    assert_eq!(fills.len(), 1388);

    db.insert_fills(&fills).await.unwrap();

    // Query fills for "UNI" coin
    let rows: Vec<(i64, String, String)> = sqlx::query_as(
        "SELECT trade_id, price, size FROM fills WHERE coin = ? ORDER BY fill_time",
    )
    .bind("UNI")
    .fetch_all(db.pool())
    .await
    .unwrap();

    assert!(!rows.is_empty(), "expected UNI fills");
    // First UNI fill from fixture
    let first = &rows[0];
    assert_eq!(first.0, 868180814349127); // trade_id
    assert_eq!(first.1, "3.9457"); // price
    assert_eq!(first.2, "3.9"); // size
}

// ============================================================================
// Query fills by user_address
// ============================================================================

#[tokio::test]
async fn fills_query_by_user_address() {
    let db = setup().await;
    let data = load_fixture();
    let fills = parser::parse_jsonl(&data).unwrap();

    db.insert_fills(&fills).await.unwrap();

    let rows: Vec<(i64, String)> = sqlx::query_as(
        "SELECT trade_id, coin FROM fills WHERE user_address = ?",
    )
    .bind("0x010461c14e146ac35fe42271bdc1134ee31c703a")
    .fetch_all(db.pool())
    .await
    .unwrap();

    assert!(!rows.is_empty(), "expected fills for user address");
    // First fill from this user is UNI
    assert!(rows.iter().any(|(_, coin)| coin == "UNI"));
}

// ============================================================================
// Count unique coins
// ============================================================================

#[tokio::test]
async fn fills_unique_coins() {
    let db = setup().await;
    let data = load_fixture();
    let fills = parser::parse_jsonl(&data).unwrap();

    db.insert_fills(&fills).await.unwrap();

    let (count,): (i64,) = sqlx::query_as("SELECT COUNT(DISTINCT coin) FROM fills")
        .fetch_one(db.pool())
        .await
        .unwrap();

    // Spec says 39 coins in the 10-line fixture
    assert_eq!(count, 39, "expected 39 unique coins in fixture");
}

// ============================================================================
// HIP4 mirror: insert fills with # coin → verify in hip4_trades
// ============================================================================

#[tokio::test]
async fn fills_hip4_mirror() {
    let db = setup().await;

    // Create a synthetic fill with #90 coin
    let hip4_fill = FillRecord {
        trade_id: 999999,
        block_number: 1000,
        block_time: "2026-03-15T00:00:00.000Z".to_string(),
        user_address: "0xdeadbeef".to_string(),
        coin: "#90".to_string(),
        price: "1.5".to_string(),
        size: "100.0".to_string(),
        side: "B".to_string(),
        direction: "Open Long".to_string(),
        closed_pnl: "0.0".to_string(),
        hash: "0xabc123".to_string(),
        order_id: 12345,
        crossed: true,
        fee: "0.15".to_string(),
        fee_token: "USDC".to_string(),
        fill_time: 1773532800000,
    };

    // Insert into fills
    db.insert_fills(std::slice::from_ref(&hip4_fill)).await.unwrap();

    // Mirror to hip4_trades
    let hip4_fills: Vec<&FillRecord> = vec![&hip4_fill];
    db.insert_hip4_trade_fills(&hip4_fills).await.unwrap();

    // Verify in fills table
    let (fills_count,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM fills WHERE coin = '#90'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(fills_count, 1);

    // Verify in hip4_trades table
    let (hip4_count,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM hip4_trades WHERE coin = '#90'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(hip4_count, 1);

    // Verify the data matches
    let row: (i64, String, String, String) = sqlx::query_as(
        "SELECT trade_id, price, size, user_address FROM hip4_trades WHERE trade_id = ?",
    )
    .bind(999999i64)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(row.0, 999999);
    assert_eq!(row.1, "1.5");
    assert_eq!(row.2, "100.0");
    assert_eq!(row.3, "0xdeadbeef");
}

// ============================================================================
// Idempotent insert (same trade_id twice)
// ============================================================================

#[tokio::test]
async fn fills_idempotent_insert() {
    let db = setup().await;
    let data = load_fixture();
    let fills = parser::parse_jsonl(&data).unwrap();

    // Count unique (trade_id, user_address) pairs in the parsed fills
    let unique_pairs: std::collections::HashSet<(i64, &str)> =
        fills.iter().map(|f| (f.trade_id, f.user_address.as_str())).collect();
    let expected_unique = unique_pairs.len() as i64;

    // Insert twice — second insert should be fully idempotent
    db.insert_fills(&fills).await.unwrap();
    db.insert_fills(&fills).await.unwrap();

    let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM fills")
        .fetch_one(db.pool())
        .await
        .unwrap();

    assert_eq!(
        count, expected_unique,
        "idempotent insert should not duplicate rows"
    );
}
