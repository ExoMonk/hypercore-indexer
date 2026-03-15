#![allow(clippy::type_complexity)]
//! Integration tests for HIP4 Phase 2: API poller storage (markets + prices).
//!
//! Uses SQLite in-memory — no Docker or real API calls.
//! Run with: cargo test --test hip4_api

use hypercore_indexer::hip4::api::{
    outcome_meta_to_markets, parse_all_mids_hip4, parse_outcome_meta, prices_to_rows,
};
use hypercore_indexer::hip4::types::{Hip4Market, Hip4PriceRow};
use hypercore_indexer::storage::sqlite::SqliteStorage;
use hypercore_indexer::storage::Storage;

async fn setup() -> SqliteStorage {
    let db = SqliteStorage::connect("sqlite::memory:").await.unwrap();
    db.ensure_schema().await.unwrap();
    db
}

const OUTCOME_META_JSON: &str = r#"{
    "outcomes": [
        {
            "outcome": 90,
            "name": "BTC > 100k by June",
            "description": "class:priceBinary|underlying:BTC|expiry:2025-06-30",
            "sideSpecs": [{"name": "Yes"}, {"name": "No"}]
        },
        {
            "outcome": 91,
            "name": "ETH > 5k by June",
            "description": "class:priceBinary|underlying:ETH|expiry:2025-06-30",
            "sideSpecs": [{"name": "Yes"}, {"name": "No"}]
        }
    ],
    "questions": [
        {
            "question": 1,
            "name": "Crypto Predictions",
            "description": "Market predictions for crypto",
            "fallbackOutcome": null,
            "namedOutcomes": [90, 91],
            "settledNamedOutcomes": []
        }
    ]
}"#;

const ALL_MIDS_JSON: &str = r##"{"#90": "0.545", "#91": "0.320", "ETH": "4000.5", "BTC": "105000.0", "#11760": "0.001"}"##;

// ============================================================================
// Markets: upsert and query back
// ============================================================================

#[tokio::test]
async fn upsert_markets_and_query_back() {
    let db = setup().await;

    let resp = parse_outcome_meta(OUTCOME_META_JSON).unwrap();
    let markets = outcome_meta_to_markets(&resp);

    db.upsert_hip4_markets(&markets).await.unwrap();

    // Query back
    let rows: Vec<(i64, String, String, String, Option<i64>, Option<String>)> = sqlx::query_as(
        "SELECT outcome_id, name, description, side_specs, question_id, question_name FROM hip4_markets ORDER BY outcome_id",
    )
    .fetch_all(db.pool())
    .await
    .unwrap();

    assert_eq!(rows.len(), 2);

    assert_eq!(rows[0].0, 90);
    assert_eq!(rows[0].1, "BTC > 100k by June");
    assert!(rows[0].2.contains("priceBinary"));
    assert!(rows[0].3.contains("Yes"));
    assert_eq!(rows[0].4, Some(1));
    assert_eq!(rows[0].5.as_deref(), Some("Crypto Predictions"));

    assert_eq!(rows[1].0, 91);
    assert_eq!(rows[1].1, "ETH > 5k by June");
}

// ============================================================================
// Markets: upsert updates metadata on second call
// ============================================================================

#[tokio::test]
async fn upsert_markets_updates_on_conflict() {
    let db = setup().await;

    // First insert
    let markets_v1 = vec![Hip4Market {
        outcome_id: 90,
        name: "BTC > 100k".to_string(),
        description: "v1".to_string(),
        side_specs: r#"[{"name":"Yes"},{"name":"No"}]"#.to_string(),
        question_id: Some(1),
        question_name: Some("Q1".to_string()),
    }];
    db.upsert_hip4_markets(&markets_v1).await.unwrap();

    // Second insert with updated fields
    let markets_v2 = vec![Hip4Market {
        outcome_id: 90,
        name: "BTC > 100k UPDATED".to_string(),
        description: "v2".to_string(),
        side_specs: r#"[{"name":"Up"},{"name":"Down"}]"#.to_string(),
        question_id: Some(2),
        question_name: Some("Q2".to_string()),
    }];
    db.upsert_hip4_markets(&markets_v2).await.unwrap();

    // Should be only 1 row, with updated fields
    let rows: Vec<(i64, String, String)> =
        sqlx::query_as("SELECT outcome_id, name, description FROM hip4_markets")
            .fetch_all(db.pool())
            .await
            .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 90);
    assert_eq!(rows[0].1, "BTC > 100k UPDATED");
    assert_eq!(rows[0].2, "v2");
}

// ============================================================================
// Prices: insert and query back
// ============================================================================

#[tokio::test]
async fn insert_prices_and_query_back() {
    let db = setup().await;

    let prices = parse_all_mids_hip4(ALL_MIDS_JSON).unwrap();
    let rows = prices_to_rows(&prices, 1700000000000); // fixed timestamp

    db.insert_hip4_prices(&rows).await.unwrap();

    // Query back
    let db_rows: Vec<(String, String)> =
        sqlx::query_as("SELECT coin, mid_price FROM hip4_prices ORDER BY coin")
            .fetch_all(db.pool())
            .await
            .unwrap();

    assert_eq!(db_rows.len(), 3);
    // All should be #-prefixed
    for r in &db_rows {
        assert!(r.0.starts_with('#'), "unexpected coin: {}", r.0);
    }
}

// ============================================================================
// Prices: idempotent insert (same coin+timestamp ignored)
// ============================================================================

#[tokio::test]
async fn insert_prices_idempotent() {
    let db = setup().await;

    let rows = vec![Hip4PriceRow {
        coin: "#90".to_string(),
        mid_price: "0.545".to_string(),
        timestamp_ms: 1700000000000,
    }];

    // Insert twice
    db.insert_hip4_prices(&rows).await.unwrap();
    db.insert_hip4_prices(&rows).await.unwrap();

    let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM hip4_prices")
        .fetch_one(db.pool())
        .await
        .unwrap();

    assert_eq!(count, 1);
}

// ============================================================================
// Prices: different timestamps create separate rows
// ============================================================================

#[tokio::test]
async fn insert_prices_different_timestamps() {
    let db = setup().await;

    let rows1 = vec![Hip4PriceRow {
        coin: "#90".to_string(),
        mid_price: "0.545".to_string(),
        timestamp_ms: 1700000000000,
    }];
    let rows2 = vec![Hip4PriceRow {
        coin: "#90".to_string(),
        mid_price: "0.550".to_string(),
        timestamp_ms: 1700000005000, // 5 seconds later
    }];

    db.insert_hip4_prices(&rows1).await.unwrap();
    db.insert_hip4_prices(&rows2).await.unwrap();

    let (count,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM hip4_prices WHERE coin = '#90'")
            .fetch_one(db.pool())
            .await
            .unwrap();

    assert_eq!(count, 2);
}

// ============================================================================
// Empty inputs handled gracefully
// ============================================================================

#[tokio::test]
async fn upsert_empty_markets() {
    let db = setup().await;
    db.upsert_hip4_markets(&[]).await.unwrap();
}

#[tokio::test]
async fn insert_empty_prices() {
    let db = setup().await;
    db.insert_hip4_prices(&[]).await.unwrap();
}
