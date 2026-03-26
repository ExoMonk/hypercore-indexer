use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::types::Hip4PriceRow;

/// Raw API response types — faithful to the HyperCore `/info` endpoint.

#[derive(Debug, Deserialize)]
pub struct OutcomeMetaResponse {
    pub outcomes: Vec<OutcomeEntry>,
    pub questions: Vec<QuestionEntry>,
}

#[derive(Debug, Deserialize)]
pub struct OutcomeEntry {
    pub outcome: u64,
    pub name: String,
    pub description: String,
    #[serde(rename = "sideSpecs")]
    pub side_specs: Vec<SideSpec>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct SideSpec {
    pub name: String,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct QuestionEntry {
    pub question: u64,
    pub name: String,
    pub description: String,
    #[serde(rename = "fallbackOutcome")]
    pub fallback_outcome: Option<u64>,
    #[serde(rename = "namedOutcomes")]
    pub named_outcomes: Vec<u64>,
    #[serde(rename = "settledNamedOutcomes")]
    pub settled_named_outcomes: Vec<u64>,
}

/// Filtered price from allMids — only `#`-prefixed coins.
#[derive(Debug, Clone)]
pub struct Hip4Price {
    pub coin: String,
    pub mid_price: String,
}

/// Client for the HyperCore `/info` REST API.
pub struct HyperCoreApiClient {
    url: String,
    client: reqwest::Client,
}

impl HyperCoreApiClient {
    pub fn new(url: &str) -> Self {
        Self {
            url: url.to_string(),
            client: reqwest::Client::new(),
        }
    }

    /// Fetch outcome market metadata.
    /// POST /info with `{"type":"outcomeMeta"}`
    pub async fn outcome_meta(&self) -> eyre::Result<OutcomeMetaResponse> {
        let body = serde_json::json!({"type": "outcomeMeta"});
        let resp = self
            .client
            .post(&self.url)
            .json(&body)
            .send()
            .await
            .map_err(|e| eyre::eyre!("outcomeMeta request failed: {e}"))?;

        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(eyre::eyre!(
                "outcomeMeta returned HTTP {status}: {text}"
            ));
        }

        let data: OutcomeMetaResponse = resp
            .json()
            .await
            .map_err(|e| eyre::eyre!("outcomeMeta JSON parse failed: {e}"))?;

        Ok(data)
    }

    /// Fetch all mid prices, filtered to `#`-prefixed coins (HIP4 outcomes).
    /// POST /info with `{"type":"allMids"}`
    pub async fn all_mids_hip4(&self) -> eyre::Result<Vec<Hip4Price>> {
        let body = serde_json::json!({"type": "allMids"});
        let resp = self
            .client
            .post(&self.url)
            .json(&body)
            .send()
            .await
            .map_err(|e| eyre::eyre!("allMids request failed: {e}"))?;

        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(eyre::eyre!("allMids returned HTTP {status}: {text}"));
        }

        let all_mids: HashMap<String, String> = resp
            .json()
            .await
            .map_err(|e| eyre::eyre!("allMids JSON parse failed: {e}"))?;

        let prices: Vec<Hip4Price> = all_mids
            .into_iter()
            .filter(|(k, _)| k.starts_with('#'))
            .map(|(coin, mid_price)| Hip4Price { coin, mid_price })
            .collect();

        Ok(prices)
    }
}

/// Parse an `OutcomeMetaResponse` from raw JSON (for testing or offline use).
#[allow(dead_code)]
pub fn parse_outcome_meta(json: &str) -> eyre::Result<OutcomeMetaResponse> {
    serde_json::from_str(json).map_err(|e| eyre::eyre!("Failed to parse outcomeMeta JSON: {e}"))
}

/// Parse allMids JSON and filter for `#`-prefixed coins.
#[allow(dead_code)]
pub fn parse_all_mids_hip4(json: &str) -> eyre::Result<Vec<Hip4Price>> {
    let all_mids: HashMap<String, String> =
        serde_json::from_str(json).map_err(|e| eyre::eyre!("Failed to parse allMids JSON: {e}"))?;

    let prices: Vec<Hip4Price> = all_mids
        .into_iter()
        .filter(|(k, _)| k.starts_with('#'))
        .map(|(coin, mid_price)| Hip4Price { coin, mid_price })
        .collect();

    Ok(prices)
}

/// Parse a pipe-delimited market description into structured fields.
/// Example: `class:priceBinary|underlying:BTC|expiry:20260327-0300|targetPrice:71169|period:1d`
/// Unknown keys are silently ignored. Descriptions without pipes return all fields as None.
pub fn parse_description(description: &str) -> super::types::ParsedDescription {
    let mut parsed = super::types::ParsedDescription::default();
    for segment in description.split('|') {
        if let Some((key, value)) = segment.split_once(':') {
            match key {
                "class" => parsed.class = Some(value.to_string()),
                "underlying" => parsed.underlying = Some(value.to_string()),
                "expiry" => parsed.expiry = Some(value.to_string()),
                "targetPrice" => parsed.target_price = Some(value.to_string()),
                "period" => parsed.period = Some(value.to_string()),
                _ => {}
            }
        }
    }
    parsed
}

/// Convert an `OutcomeMetaResponse` into storage-layer `Hip4Market` rows.
/// Joins outcomes with questions: for each outcome, find the question whose
/// `named_outcomes` contains this outcome_id.
pub fn outcome_meta_to_markets(
    resp: &OutcomeMetaResponse,
) -> Vec<super::types::Hip4Market> {
    resp.outcomes
        .iter()
        .map(|o| {
            // Find the question that references this outcome
            let question = resp.questions.iter().find(|q| q.named_outcomes.contains(&o.outcome));

            let side_specs_json =
                serde_json::to_string(&o.side_specs).unwrap_or_else(|_| "[]".to_string());

            let parsed = parse_description(&o.description);

            super::types::Hip4Market {
                outcome_id: o.outcome,
                name: o.name.clone(),
                description: o.description.clone(),
                side_specs: side_specs_json,
                question_id: question.map(|q| q.question),
                question_name: question.map(|q| q.name.clone()),
                parsed,
            }
        })
        .collect()
}

/// Convert `Hip4Price` list into storage-layer `Hip4PriceRow` rows with the given timestamp.
pub fn prices_to_rows(prices: &[Hip4Price], timestamp_ms: i64) -> Vec<Hip4PriceRow> {
    prices
        .iter()
        .map(|p| Hip4PriceRow {
            coin: p.coin.clone(),
            mid_price: p.mid_price.clone(),
            timestamp_ms,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn parse_outcome_meta_response() {
        let resp = parse_outcome_meta(OUTCOME_META_JSON).unwrap();
        assert_eq!(resp.outcomes.len(), 2);
        assert_eq!(resp.outcomes[0].outcome, 90);
        assert_eq!(resp.outcomes[0].name, "BTC > 100k by June");
        assert_eq!(resp.outcomes[0].side_specs.len(), 2);
        assert_eq!(resp.outcomes[0].side_specs[0].name, "Yes");
        assert_eq!(resp.questions.len(), 1);
        assert_eq!(resp.questions[0].question, 1);
        assert_eq!(resp.questions[0].named_outcomes, vec![90, 91]);
    }

    #[test]
    fn parse_all_mids_filters_hash_prefix() {
        let prices = parse_all_mids_hip4(ALL_MIDS_JSON).unwrap();
        // Should only have #-prefixed coins
        assert_eq!(prices.len(), 3);
        for p in &prices {
            assert!(p.coin.starts_with('#'), "unexpected coin: {}", p.coin);
        }
    }

    #[test]
    fn empty_outcome_meta_handled() {
        let json = r#"{"outcomes": [], "questions": []}"#;
        let resp = parse_outcome_meta(json).unwrap();
        assert!(resp.outcomes.is_empty());
        assert!(resp.questions.is_empty());
    }

    #[test]
    fn empty_all_mids_handled() {
        let json = r#"{}"#;
        let prices = parse_all_mids_hip4(json).unwrap();
        assert!(prices.is_empty());
    }

    #[test]
    fn all_mids_no_hip4_coins() {
        let json = r#"{"ETH": "4000.5", "BTC": "105000.0"}"#;
        let prices = parse_all_mids_hip4(json).unwrap();
        assert!(prices.is_empty());
    }

    #[test]
    fn outcome_meta_to_markets_joins_questions() {
        let resp = parse_outcome_meta(OUTCOME_META_JSON).unwrap();
        let markets = outcome_meta_to_markets(&resp);
        assert_eq!(markets.len(), 2);

        let m0 = &markets[0];
        assert_eq!(m0.outcome_id, 90);
        assert_eq!(m0.name, "BTC > 100k by June");
        assert_eq!(m0.question_id, Some(1));
        assert_eq!(m0.question_name.as_deref(), Some("Crypto Predictions"));
        // side_specs should be valid JSON
        let _: Vec<SideSpec> = serde_json::from_str(&m0.side_specs).unwrap();
    }

    #[test]
    fn prices_to_rows_sets_timestamp() {
        let prices = vec![
            Hip4Price { coin: "#90".to_string(), mid_price: "0.545".to_string() },
        ];
        let rows = prices_to_rows(&prices, 1700000000000);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].coin, "#90");
        assert_eq!(rows[0].mid_price, "0.545");
        assert_eq!(rows[0].timestamp_ms, 1700000000000);
    }

    // --- Description parser tests ---

    #[test]
    fn parse_description_full() {
        let desc = "class:priceBinary|underlying:BTC|expiry:20260327-0300|targetPrice:71169|period:1d";
        let parsed = parse_description(desc);
        assert_eq!(parsed.class.as_deref(), Some("priceBinary"));
        assert_eq!(parsed.underlying.as_deref(), Some("BTC"));
        assert_eq!(parsed.expiry.as_deref(), Some("20260327-0300"));
        assert_eq!(parsed.target_price.as_deref(), Some("71169"));
        assert_eq!(parsed.period.as_deref(), Some("1d"));
    }

    #[test]
    fn parse_description_partial_keys() {
        let desc = "class:priceBinary|underlying:ETH";
        let parsed = parse_description(desc);
        assert_eq!(parsed.class.as_deref(), Some("priceBinary"));
        assert_eq!(parsed.underlying.as_deref(), Some("ETH"));
        assert!(parsed.expiry.is_none());
        assert!(parsed.target_price.is_none());
        assert!(parsed.period.is_none());
    }

    #[test]
    fn parse_description_empty_string() {
        let parsed = parse_description("");
        assert_eq!(parsed, super::super::types::ParsedDescription::default());
    }

    #[test]
    fn parse_description_unknown_keys() {
        let desc = "class:priceBinary|foo:bar|baz:qux";
        let parsed = parse_description(desc);
        assert_eq!(parsed.class.as_deref(), Some("priceBinary"));
        assert!(parsed.underlying.is_none());
    }

    #[test]
    fn parse_description_multiple_colons() {
        let desc = "class:priceBinary|expiry:20260327-0300:extra";
        let parsed = parse_description(desc);
        assert_eq!(parsed.expiry.as_deref(), Some("20260327-0300:extra"));
    }

    #[test]
    fn parse_description_no_pipes() {
        let desc = "Just a plain description with no structure";
        let parsed = parse_description(desc);
        // No pipes means no key:value segments
        assert!(parsed.class.is_none());
        assert!(parsed.underlying.is_none());
    }

    #[test]
    fn outcome_meta_to_markets_parses_description() {
        let resp = parse_outcome_meta(OUTCOME_META_JSON).unwrap();
        let markets = outcome_meta_to_markets(&resp);
        // "class:priceBinary|underlying:BTC|expiry:2025-06-30"
        let m0 = &markets[0];
        assert_eq!(m0.parsed.class.as_deref(), Some("priceBinary"));
        assert_eq!(m0.parsed.underlying.as_deref(), Some("BTC"));
        assert_eq!(m0.parsed.expiry.as_deref(), Some("2025-06-30"));
    }
}
