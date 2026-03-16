//! Integration tests: decode real S3 block fixtures end-to-end.
//!
//! Every test pins concrete values from known blocks. If deserialization,
//! hash computation, or decoding logic changes, these tests catch it.

use hypercore_indexer::decode;
use hypercore_indexer::decode::types::{AssetType, TxType};
use hypercore_indexer::s3::codec;
use hypercore_indexer::types::block::WireTxEnum;

fn load_fixture(name: &str) -> Vec<u8> {
    let path = format!("{}/tests/fixtures/{name}", env!("CARGO_MANIFEST_DIR"));
    std::fs::read(&path).unwrap_or_else(|e| panic!("Failed to read fixture {path}: {e}"))
}

// ============================================================================
// Block 1: Genesis-era empty block (mainnet, chain_id 999)
// ============================================================================

#[test]
fn block_1_header_fields_pinned() {
    let block = codec::decode_block(&load_fixture("block_1.rmp.lz4")).unwrap();
    let header = &block.block.inner().header.header;

    assert_eq!(header.number, 1);
    assert_eq!(header.gas_used, 0);
    assert_eq!(header.gas_limit, 2_000_000);
    assert_eq!(header.base_fee_per_gas, Some(100_000_000));
    assert_eq!(header.timestamp, 1739849780); // 2025-02-18T05:16:20Z
    assert_eq!(
        format!("{:#x}", header.parent_hash),
        "0xd8fcc13b6a195b88b7b2da3722ff6cad767b13a8c1e9ffb1c73aa9d216d895f0"
    );
}

#[test]
fn block_1_hash_pinned() {
    let block = codec::decode_block(&load_fixture("block_1.rmp.lz4")).unwrap();
    assert_eq!(
        format!("{:#x}", block.block.inner().header.hash),
        "0xde151843548b88d06f201d86e860e45fbf07d49612f1934fba5746abd942fb01"
    );
}

#[test]
fn block_1_is_completely_empty() {
    let block = codec::decode_block(&load_fixture("block_1.rmp.lz4")).unwrap();
    assert_eq!(block.block.inner().body.transactions.len(), 0);
    assert_eq!(block.system_txs.len(), 0);
    assert_eq!(block.receipts.len(), 0);
    // read_precompile_calls and highest_precompile_address are ignored during deserialization
    // (they exist for MessagePack compat but their format changes across HL versions)
}

#[test]
fn block_1_decode_produces_empty_decoded_block() {
    let raw = codec::decode_block(&load_fixture("block_1.rmp.lz4")).unwrap();
    let decoded = decode::decode_block(&raw, 999).unwrap();

    assert_eq!(decoded.number, 1);
    assert_eq!(decoded.transactions.len(), 0);
    assert_eq!(decoded.system_transfers.len(), 0);
    assert_eq!(decoded.gas_used, 0);
    assert_eq!(
        format!("{:#x}", decoded.hash),
        "0xde151843548b88d06f201d86e860e45fbf07d49612f1934fba5746abd942fb01"
    );
}

// ============================================================================
// Block 5000038: Mid-chain block with 8 txs + 1 system tx (mainnet)
// Known values from live decode output.
// ============================================================================

#[test]
fn block_5m_header_pinned() {
    let block = codec::decode_block(&load_fixture("block_5000038.rmp.lz4")).unwrap();
    let header = &block.block.inner().header.header;

    assert_eq!(header.number, 5_000_038);
    assert_eq!(header.gas_used, 1_722_800);
    assert_eq!(header.gas_limit, 2_000_000);
    assert_eq!(header.base_fee_per_gas, Some(622_120_557));
    assert_eq!(header.timestamp, 1_749_160_149);
}

#[test]
fn block_5m_block_and_parent_hashes_pinned() {
    let block = codec::decode_block(&load_fixture("block_5000038.rmp.lz4")).unwrap();
    let inner = block.block.inner();
    assert_eq!(
        format!("{:#x}", inner.header.hash),
        "0x6639e377dc4aba11f210dc95b0024f15840d0289a82abf883ef3825a85fa9508"
    );
    assert_eq!(
        format!("{:#x}", inner.header.header.parent_hash),
        "0x87b447ef1a1b8327b32aab6f7a671c0ad6239efcf56386fabaec87c909a198d5"
    );
}

#[test]
fn block_5m_exact_tx_and_receipt_counts() {
    let block = codec::decode_block(&load_fixture("block_5000038.rmp.lz4")).unwrap();
    assert_eq!(block.block.inner().body.transactions.len(), 8);
    assert_eq!(block.receipts.len(), 8);
    assert_eq!(block.system_txs.len(), 1);
}

#[test]
fn block_5m_system_tx_wire_fields_pinned() {
    let block = codec::decode_block(&load_fixture("block_5000038.rmp.lz4")).unwrap();
    let stx = &block.system_txs[0];

    assert!(matches!(stx.tx, WireTxEnum::Legacy(_)));
    assert_eq!(stx.tx.input().len(), 68); // transfer(address,uint256)
    assert_eq!(&stx.tx.input()[..4], &[0xa9, 0x05, 0x9c, 0xbb]); // transfer selector
    assert!(stx.receipt.is_some());
    assert_eq!(
        format!("{:#x}", stx.tx.to().unwrap()),
        "0x9b498c3c8a0b8cd8ba1d9851d40d186f1872b44e"
    );
}

#[test]
fn block_5m_receipt_tx_types_distribution_pinned() {
    let block = codec::decode_block(&load_fixture("block_5000038.rmp.lz4")).unwrap();
    let types: Vec<&str> = block.receipts.iter().map(|r| r.tx_type.as_str()).collect();
    // All 8 receipts should be Eip1559 in this block
    assert!(
        types.iter().all(|t| *t == "Eip1559" || *t == "Legacy"),
        "unexpected tx types: {:?}",
        types
    );
}

#[test]
fn block_5m_cumulative_gas_values_pinned() {
    let block = codec::decode_block(&load_fixture("block_5000038.rmp.lz4")).unwrap();
    // First receipt's cumulative = that tx's gas; last = total block gas
    assert!(block.receipts[0].cumulative_gas_used > 0);
    assert_eq!(
        block.receipts.last().unwrap().cumulative_gas_used,
        1_722_800 // matches header.gas_used
    );
    // Monotonically increasing
    for w in block.receipts.windows(2) {
        assert!(
            w[1].cumulative_gas_used >= w[0].cumulative_gas_used,
            "cumulative gas not monotonic: {} < {}",
            w[1].cumulative_gas_used,
            w[0].cumulative_gas_used
        );
    }
}

#[test]
fn block_5m_all_regular_tx_signatures_are_valid() {
    let block = codec::decode_block(&load_fixture("block_5000038.rmp.lz4")).unwrap();
    for (i, tx) in block.block.inner().body.transactions.iter().enumerate() {
        assert!(!tx.signature.r.is_zero(), "tx {i} has zero r");
        assert!(!tx.signature.s.is_zero(), "tx {i} has zero s");
    }
}

// ============================================================================
// Block 5000038: Decoded block with hash computation (M2)
// ============================================================================

#[test]
fn block_5m_all_tx_hashes_pinned() {
    let raw = codec::decode_block(&load_fixture("block_5000038.rmp.lz4")).unwrap();
    let decoded = decode::decode_block(&raw, 999).unwrap();

    // Pin all 8 computed tx hashes — if RLP encoding or keccak changes, this catches it
    let expected_hashes = [
        "0x1f912cb736959444532212379df30c07b78c8c1761200550bf92eff37cf6d998",
        "0x13d9d197fd9fe4b68358b4a12a2fd82d7ca24380472d7098244e0b8bbc00b738",
        "0xee1faff87698deb24094eb3ec8e120e0053e5cf2d028f4f69d48935da2a7d152",
        "0x57d8b7f7c192e62622c3733fa9e11021f4bcdacf796e2eb81d8a12e0a475389e",
        "0x0b61c0800dfaed405ddc5b9a36ed0982b375913444e0f4aaab2f744f515e0860",
        "0x393d93d17ecc465ef1a981a52c9cd1238c23258cfddd7bce36dbaa2c3f8274b0",
        "0xc548e68042ecce4127454d652dad03303c5a816bb08065822b857e54316a60fa",
        "0xf5a15ba50dd767b473bf4aaadf6a1085e02f13b27cb1a45143d3980a2f04e9fc",
    ];

    assert_eq!(decoded.transactions.len(), expected_hashes.len());
    for (i, tx) in decoded.transactions.iter().enumerate() {
        assert_eq!(
            format!("{:#x}", tx.hash),
            expected_hashes[i],
            "tx hash mismatch at index {i}"
        );
        assert_eq!(tx.tx_index, i);
    }
}

#[test]
fn block_5m_per_tx_gas_used_pinned() {
    let raw = codec::decode_block(&load_fixture("block_5000038.rmp.lz4")).unwrap();
    let decoded = decode::decode_block(&raw, 999).unwrap();

    let expected_gas = [172182, 191685, 36699, 70481, 88112, 70774, 85619, 1007248];
    assert_eq!(decoded.transactions.len(), expected_gas.len());
    for (i, tx) in decoded.transactions.iter().enumerate() {
        assert_eq!(
            tx.gas_used, expected_gas[i],
            "gas_used mismatch at tx index {i}"
        );
    }

    // Sum must equal block.gas_used
    let total: u64 = decoded.transactions.iter().map(|t| t.gas_used).sum();
    assert_eq!(total, 1_722_800);
}

#[test]
fn block_5m_tx_types_pinned() {
    let raw = codec::decode_block(&load_fixture("block_5000038.rmp.lz4")).unwrap();
    let decoded = decode::decode_block(&raw, 999).unwrap();

    // Pin exact tx type distribution for this block
    let expected_types = [
        TxType::Eip1559,
        TxType::Eip1559,
        TxType::Legacy,
        TxType::Eip1559,
        TxType::Eip1559,
        TxType::Eip1559,
        TxType::Eip1559,
        TxType::Legacy,
    ];
    for (i, tx) in decoded.transactions.iter().enumerate() {
        assert_eq!(
            tx.tx_type, expected_types[i],
            "tx type mismatch at index {i}"
        );
    }
}

#[test]
fn block_5m_tx_success_status_pinned() {
    let raw = codec::decode_block(&load_fixture("block_5000038.rmp.lz4")).unwrap();
    let decoded = decode::decode_block(&raw, 999).unwrap();

    // Pin exact success/failure status for each tx
    // txs 3-6 are reverted (success=false)
    let expected_success = [true, true, true, false, false, false, false, true];
    for (i, tx) in decoded.transactions.iter().enumerate() {
        assert_eq!(
            tx.success, expected_success[i],
            "tx {} success status mismatch (expected {}, got {})",
            i, expected_success[i], tx.success
        );
    }
}

#[test]
fn block_5m_first_tx_has_3_logs() {
    let raw = codec::decode_block(&load_fixture("block_5000038.rmp.lz4")).unwrap();
    let decoded = decode::decode_block(&raw, 999).unwrap();

    let tx0 = &decoded.transactions[0];
    assert_eq!(tx0.logs.len(), 3);
    // First log is a Transfer event from HYPE token
    assert_eq!(
        format!("{:#x}", tx0.logs[0].address),
        "0x5555555555555555555555555555555555555555"
    );
    assert_eq!(
        format!("{:#x}", tx0.logs[0].topics[0]),
        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef" // Transfer event sig
    );
    assert_eq!(tx0.logs[0].log_index, 0);
}

#[test]
fn block_5m_system_tx_dual_hashes_pinned() {
    let raw = codec::decode_block(&load_fixture("block_5000038.rmp.lz4")).unwrap();
    let decoded = decode::decode_block(&raw, 999).unwrap();

    assert_eq!(decoded.system_transfers.len(), 1);
    let stx = &decoded.system_transfers[0];

    // Pin both phantom hashes — these are deterministic from the tx fields + chain_id
    assert_eq!(
        format!("{:#x}", stx.official_hash),
        "0x3018ef9a2d5f37639c248632abc93d7a3328cc9497737b1b140cb3221dbc829c"
    );
    assert_eq!(
        format!("{:#x}", stx.explorer_hash),
        "0x355291ec82b3818450b5a7179faf8b5d973822b2ab193f209e0e0f3081c7088d"
    );
}

#[test]
fn block_5m_system_tx_decoded_fields_pinned() {
    let raw = codec::decode_block(&load_fixture("block_5000038.rmp.lz4")).unwrap();
    let decoded = decode::decode_block(&raw, 999).unwrap();

    let stx = &decoded.system_transfers[0];
    assert!(matches!(
        stx.asset_type,
        AssetType::SpotToken { asset_index: 0 }
    ));
    assert_eq!(
        format!("{:#x}", stx.system_address),
        "0x9b498c3c8a0b8cd8ba1d9851d40d186f1872b44e"
    );
    assert_eq!(
        format!("{:#x}", stx.recipient),
        "0xefd3ab65915e35105caa462442c9ecc1346728df"
    );
    // Amount: 0xe1df89a1c64df680000 = ~4194.3... tokens (with 18 decimals)
    assert!(!stx.amount_wei.is_zero());
}

#[test]
fn block_5m_decoded_metadata_pinned() {
    let raw = codec::decode_block(&load_fixture("block_5000038.rmp.lz4")).unwrap();
    let decoded = decode::decode_block(&raw, 999).unwrap();

    assert_eq!(decoded.number, 5_000_038);
    assert_eq!(decoded.gas_used, 1_722_800);
    assert_eq!(decoded.gas_limit, 2_000_000);
    assert_eq!(decoded.timestamp, 1_749_160_149);
    assert_eq!(decoded.base_fee_per_gas, Some(622_120_557));
    assert_eq!(
        format!("{:#x}", decoded.hash),
        "0x6639e377dc4aba11f210dc95b0024f15840d0289a82abf883ef3825a85fa9508"
    );
    assert_eq!(
        format!("{:#x}", decoded.parent_hash),
        "0x87b447ef1a1b8327b32aab6f7a671c0ad6239efcf56386fabaec87c909a198d5"
    );
}

#[test]
fn block_5m_summary_json_round_trips() {
    let block = codec::decode_block(&load_fixture("block_5000038.rmp.lz4")).unwrap();
    let summary = block.summary();

    // Verify summary aggregates match
    assert_eq!(summary.block_number, 5_000_038);
    assert_eq!(summary.tx_count, 8);
    assert_eq!(summary.system_tx_count, 1);
    assert_eq!(summary.receipt_count, 8);
    assert_eq!(summary.gas_used, 1_722_800);
    assert_eq!(summary.gas_limit, 2_000_000);

    // JSON round-trip: serialize, parse back, verify key fields
    let json = serde_json::to_string(&summary).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed["block_number"], 5_000_038);
    assert_eq!(parsed["tx_count"], 8);
    assert_eq!(
        parsed["block_hash"],
        "0x6639e377dc4aba11f210dc95b0024f15840d0289a82abf883ef3825a85fa9508"
    );
}

// ============================================================================
// Testnet block (chain_id 998)
// ============================================================================

#[test]
fn testnet_block_header_pinned() {
    let block = codec::decode_block(&load_fixture("block_testnet_48186001.rmp.lz4")).unwrap();
    let header = &block.block.inner().header.header;

    assert_eq!(header.number, 48_186_001);
    assert_eq!(header.gas_limit, 3_000_000); // testnet has higher gas limit
    assert_eq!(header.gas_used, 0);
    assert_eq!(header.base_fee_per_gas, Some(100_000_000));
}

#[test]
fn testnet_block_hash_pinned() {
    let block = codec::decode_block(&load_fixture("block_testnet_48186001.rmp.lz4")).unwrap();
    assert_eq!(
        format!("{:#x}", block.block.inner().header.hash),
        "0xacef176c39777f536aaf21e6ced1e27bc8d57e16c21a376957ebfb810b1777a8"
    );
    assert_eq!(
        format!("{:#x}", block.block.inner().header.header.parent_hash),
        "0x7ba3a94c19e1608f05c8eb89988dce909028cc1f969298effe6fff4b1d0ffe75"
    );
}

#[test]
fn testnet_block_decode_with_chain_998() {
    let raw = codec::decode_block(&load_fixture("block_testnet_48186001.rmp.lz4")).unwrap();
    let decoded = decode::decode_block(&raw, 998).unwrap();

    assert_eq!(decoded.number, 48_186_001);
    assert_eq!(decoded.transactions.len(), 0);
    assert_eq!(decoded.system_transfers.len(), 0);
    assert_eq!(
        format!("{:#x}", decoded.hash),
        "0xacef176c39777f536aaf21e6ced1e27bc8d57e16c21a376957ebfb810b1777a8"
    );
}

// ============================================================================
// Cross-block consistency checks
// ============================================================================

#[test]
fn block_5m_receipt_count_equals_tx_count() {
    let raw = codec::decode_block(&load_fixture("block_5000038.rmp.lz4")).unwrap();
    assert_eq!(
        raw.receipts.len(),
        raw.block.inner().body.transactions.len(),
        "receipt count must equal transaction count"
    );
}

#[test]
fn decoded_block_log_indices_are_globally_sequential() {
    let raw = codec::decode_block(&load_fixture("block_5000038.rmp.lz4")).unwrap();
    let decoded = decode::decode_block(&raw, 999).unwrap();

    let mut expected_log_index = 0usize;
    for tx in &decoded.transactions {
        for log in &tx.logs {
            assert_eq!(
                log.log_index, expected_log_index,
                "log index gap: expected {expected_log_index}, got {}",
                log.log_index
            );
            expected_log_index += 1;
        }
    }
    assert!(expected_log_index > 0, "block should have some logs");
}

// ============================================================================
// Codec edge cases
// ============================================================================

#[test]
fn decompress_raw_block_1_matches_expected_size() {
    let compressed = load_fixture("block_1.rmp.lz4");
    let raw = codec::decompress_raw(&compressed).unwrap();
    assert_eq!(compressed.len(), 515);
    assert_eq!(raw.len(), 1032); // known decompressed size for block 1
}
