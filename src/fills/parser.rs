use eyre::Result;
use serde::Deserialize;

use super::types::FillRecord;

#[derive(Deserialize)]
struct RawBlock {
    block_number: i64,
    block_time: String,
    events: Vec<(String, RawFill)>,
}

#[derive(Deserialize)]
struct RawFill {
    coin: String,
    px: String,
    sz: String,
    side: String,
    time: i64,
    dir: String,
    #[serde(rename = "closedPnl")]
    closed_pnl: String,
    hash: String,
    oid: i64,
    crossed: bool,
    fee: String,
    tid: i64,
    #[serde(rename = "feeToken")]
    fee_token: String,
    // Optional fields we skip but must tolerate
    #[serde(rename = "startPosition")]
    #[allow(dead_code)]
    start_position: Option<String>,
    #[serde(rename = "twapId")]
    #[allow(dead_code)]
    twap_id: Option<serde_json::Value>,
    #[allow(dead_code)]
    cloid: Option<String>,
    #[allow(dead_code)]
    builder: Option<String>,
}

/// Parse a single JSONL line into FillRecords.
pub fn parse_line(line: &str) -> Result<Vec<FillRecord>> {
    let line = line.trim();
    if line.is_empty() {
        return Ok(vec![]);
    }

    let block: RawBlock =
        serde_json::from_str(line).map_err(|e| eyre::eyre!("Failed to parse JSONL line: {e}"))?;

    let mut fills = Vec::with_capacity(block.events.len());
    for (user_address, fill) in block.events {
        fills.push(FillRecord {
            trade_id: fill.tid,
            block_number: block.block_number,
            block_time: block.block_time.clone(),
            user_address,
            coin: fill.coin,
            price: fill.px,
            size: fill.sz,
            side: fill.side,
            direction: fill.dir,
            closed_pnl: fill.closed_pnl,
            hash: fill.hash,
            order_id: fill.oid,
            crossed: fill.crossed,
            fee: fill.fee,
            fee_token: fill.fee_token,
            fill_time: fill.time,
        });
    }

    Ok(fills)
}

/// Parse all lines from JSONL data into FillRecords.
pub fn parse_jsonl(data: &str) -> Result<Vec<FillRecord>> {
    let mut all_fills = Vec::new();
    for line in data.lines() {
        let fills = parse_line(line)?;
        all_fills.extend(fills);
    }
    Ok(all_fills)
}

/// Decompress LZ4 data and parse the resulting JSONL.
pub fn parse_lz4(compressed: &[u8]) -> Result<Vec<FillRecord>> {
    let decompressed = lz4_flex::decompress_size_prepended(compressed)
        .map_err(|e| eyre::eyre!("LZ4 decompression failed: {e}"))?;
    let text = std::str::from_utf8(&decompressed)
        .map_err(|e| eyre::eyre!("Decompressed data is not valid UTF-8: {e}"))?;
    parse_jsonl(text)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn load_fixture() -> String {
        let path = format!(
            "{}/tests/fixtures/node_fills_sample.jsonl",
            env!("CARGO_MANIFEST_DIR")
        );
        std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("Failed to read fixture {path}: {e}"))
    }

    #[test]
    fn parse_single_line() {
        let data = load_fixture();
        let first_line = data.lines().next().unwrap();
        let fills = parse_line(first_line).unwrap();
        assert!(!fills.is_empty(), "first line should have fills");

        // Verify first fill fields
        let f = &fills[0];
        assert_eq!(f.coin, "UNI");
        assert_eq!(f.price, "3.9457");
        assert_eq!(f.size, "3.9");
        assert_eq!(f.side, "B");
        assert_eq!(f.direction, "Open Long");
        assert_eq!(f.closed_pnl, "0.0");
        assert_eq!(f.block_number, 923950857);
        assert!(f.user_address.starts_with("0x"));
        assert!(f.trade_id > 0);
        assert!(f.fill_time > 0);
    }

    #[test]
    fn parse_full_fixture() {
        let data = load_fixture();
        let fills = parse_jsonl(&data).unwrap();
        // 10 lines, 1388 events total (includes buyer+seller for each trade)
        assert_eq!(fills.len(), 1388, "expected 1388 events from 10-line fixture");

        // Verify we see many different coins (39 unique in fixture)
        let coins: std::collections::HashSet<&str> = fills.iter().map(|f| f.coin.as_str()).collect();
        assert_eq!(coins.len(), 39, "expected 39 unique coins in fixture");
    }

    #[test]
    fn parse_empty_line() {
        let fills = parse_line("").unwrap();
        assert!(fills.is_empty());
    }

    #[test]
    fn parse_block_with_empty_events() {
        let json = r#"{"local_time":"2026-03-15T00:00:00.000Z","block_time":"2026-03-15T00:00:00.000Z","block_number":1,"events":[]}"#;
        let fills = parse_line(json).unwrap();
        assert!(fills.is_empty());
    }

    #[test]
    fn handles_optional_twap_id_null() {
        let data = load_fixture();
        let first_line = data.lines().next().unwrap();
        // All fills in the fixture have twapId: null — this should not error
        let fills = parse_line(first_line).unwrap();
        assert!(!fills.is_empty());
    }

    #[test]
    fn verify_specific_fill_fields() {
        let data = load_fixture();
        let fills = parse_jsonl(&data).unwrap();

        // Find a specific fill by trade_id from the first line
        let first = &fills[0];
        assert_eq!(first.trade_id, 868180814349127);
        assert_eq!(
            first.user_address,
            "0x010461c14e146ac35fe42271bdc1134ee31c703a"
        );
        assert_eq!(first.coin, "UNI");
        assert_eq!(first.fee, "0.0");
        assert_eq!(first.fee_token, "USDC");
        assert!(first.crossed);
    }
}
