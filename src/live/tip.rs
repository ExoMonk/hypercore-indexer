use std::sync::Arc;

use tracing::debug;

use crate::error::Result;
use crate::live::is_block_not_found;
use crate::s3::client::HyperEvmS3Client;

/// Get the current chain tip via RPC (eth_blockNumber).
/// Single HTTP request — instant compared to S3 probing.
pub async fn get_rpc_tip(rpc_url: &str) -> Result<u64> {
    let client = reqwest::Client::new();
    let resp = client
        .post(rpc_url)
        .json(&serde_json::json!({
            "jsonrpc": "2.0",
            "method": "eth_blockNumber",
            "params": [],
            "id": 1
        }))
        .send()
        .await
        .map_err(|e| eyre::eyre!("RPC request failed: {e}"))?;

    let body: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| eyre::eyre!("RPC response parse failed: {e}"))?;

    let hex = body["result"]
        .as_str()
        .ok_or_else(|| eyre::eyre!("RPC response missing 'result': {body}"))?;

    let tip = u64::from_str_radix(hex.trim_start_matches("0x"), 16)
        .map_err(|e| eyre::eyre!("Invalid block number '{hex}': {e}"))?;

    debug!(tip, rpc_url, "Chain tip from RPC");
    Ok(tip)
}

/// Fallback: find a high block on S3 by probing common starting points.
pub async fn find_existing_block(client: &Arc<HyperEvmS3Client>) -> Result<u64> {
    let probes = [
        50_000_000, 40_000_000, 30_000_000, 25_000_000, 20_000_000,
        18_000_000, 10_000_000, 1_000_000, 1,
    ];
    for probe in probes {
        if block_exists(client, probe).await.unwrap_or(false) {
            debug!(block = probe, "Found existing block on S3");
            return Ok(probe);
        }
    }
    Err(eyre::eyre!("Could not find any existing block on S3"))
}

/// Find the approximate latest block available on S3 via exponential probing + binary search.
///
/// Algorithm:
/// 1. From `known_block`, probe forward exponentially: +1000, +2000, +4000, ...
/// 2. When first 404 is hit, binary search between last success and first failure.
/// 3. Return the highest block number that exists on S3.
pub async fn find_s3_tip(client: &Arc<HyperEvmS3Client>, known_block: u64) -> Result<u64> {
    let mut lo = known_block;
    let mut step = 1000u64;
    let mut hi = known_block + step;

    // Phase 1: Exponential probe forward to find an upper bound
    loop {
        if block_exists(client, hi).await? {
            lo = hi;
            step *= 2;
            hi = lo + step;
            debug!(lo, hi, "S3 tip probe: block exists, doubling step");
        } else {
            debug!(lo, hi, "S3 tip probe: 404, starting binary search");
            break;
        }
    }

    // Phase 2: Binary search between lo (exists) and hi (404)
    while hi - lo > 1 {
        let mid = lo + (hi - lo) / 2;
        if block_exists(client, mid).await? {
            lo = mid;
        } else {
            hi = mid;
        }
    }

    debug!(tip = lo, "S3 tip discovery complete");
    Ok(lo)
}

/// Check whether a block exists on S3 by attempting to fetch it.
/// Returns Ok(true) if the block exists, Ok(false) on 404-like errors.
/// Propagates other errors.
async fn block_exists(client: &Arc<HyperEvmS3Client>, block_number: u64) -> Result<bool> {
    match client.fetch_block_raw(block_number).await {
        Ok(_) => Ok(true),
        Err(e) => {
            if is_block_not_found(&e) {
                Ok(false)
            } else {
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    /// Test the binary search logic with a mock predicate.
    /// We can't call real S3, so we test the algorithm in isolation.
    #[test]
    fn binary_search_finds_boundary() {
        // Simulate: blocks 0..=5042 exist, 5043+ don't
        let tip = 5042u64;
        let exists = |n: u64| n <= tip;

        // Phase 1: exponential probe
        let known = 4000u64;
        let mut lo = known;
        let mut step = 1000u64;
        let mut hi = known + step;

        loop {
            if exists(hi) {
                lo = hi;
                step *= 2;
                hi = lo + step;
            } else {
                break;
            }
        }

        // Phase 2: binary search
        while hi - lo > 1 {
            let mid = lo + (hi - lo) / 2;
            if exists(mid) {
                lo = mid;
            } else {
                hi = mid;
            }
        }

        assert_eq!(lo, tip);
    }

    #[test]
    fn binary_search_known_is_tip() {
        // Edge case: known_block is already the tip
        let tip = 4000u64;
        let exists = |n: u64| n <= tip;

        let known = 4000u64;
        let mut lo = known;
        let step = 1000u64;
        let mut hi = known + step;

        // First probe fails immediately
        if !exists(hi) {
            // Binary search between 4000 and 5000
            while hi - lo > 1 {
                let mid = lo + (hi - lo) / 2;
                if exists(mid) {
                    lo = mid;
                } else {
                    hi = mid;
                }
            }
        }

        assert_eq!(lo, tip);
    }

    #[test]
    fn binary_search_far_ahead() {
        // Tip is far ahead of known
        let tip = 100_000u64;
        let exists = |n: u64| n <= tip;

        let known = 1000u64;
        let mut lo = known;
        let mut step = 1000u64;
        let mut hi = known + step;

        loop {
            if exists(hi) {
                lo = hi;
                step *= 2;
                hi = lo + step;
            } else {
                break;
            }
        }

        while hi - lo > 1 {
            let mid = lo + (hi - lo) / 2;
            if exists(mid) {
                lo = mid;
            } else {
                hi = mid;
            }
        }

        assert_eq!(lo, tip);
    }
}
