//! Integration tests: decode real S3 block fixtures end-to-end.
//! Fixtures are pre-downloaded .rmp.lz4 files from the S3 bucket.

use hypercore_indexer::s3::codec;
use hypercore_indexer::types::block::WireTxEnum;

fn load_fixture(name: &str) -> Vec<u8> {
    let path = format!("{}/tests/fixtures/{name}", env!("CARGO_MANIFEST_DIR"));
    std::fs::read(&path).unwrap_or_else(|e| panic!("Failed to read fixture {path}: {e}"))
}

// --- Block 1: Genesis-era empty block (mainnet) ---

#[test]
fn block_1_header_fields() {
    let block = codec::decode_block(&load_fixture("block_1.rmp.lz4")).unwrap();
    let header = &block.block.inner().header.header;

    assert_eq!(header.number, 1);
    assert_eq!(header.gas_used, 0);
    assert_eq!(header.gas_limit, 2_000_000);
    assert_eq!(header.base_fee_per_gas, Some(100_000_000));
    assert_eq!(header.timestamp, 1739849780);
}

#[test]
fn block_1_hash() {
    let block = codec::decode_block(&load_fixture("block_1.rmp.lz4")).unwrap();
    assert_eq!(
        format!("{:#x}", block.block.inner().header.hash),
        "0xde151843548b88d06f201d86e860e45fbf07d49612f1934fba5746abd942fb01"
    );
}

#[test]
fn block_1_empty_body() {
    let block = codec::decode_block(&load_fixture("block_1.rmp.lz4")).unwrap();
    assert!(block.block.inner().body.transactions.is_empty());
    assert!(block.system_txs.is_empty());
    assert!(block.receipts.is_empty());
}

#[test]
fn block_1_optional_fields_default() {
    let block = codec::decode_block(&load_fixture("block_1.rmp.lz4")).unwrap();
    assert!(block.read_precompile_calls.is_empty());
    assert!(block.highest_precompile_address.is_none());
}

// --- Block 5000038: Mid-chain block with txs + system tx (mainnet) ---

#[test]
fn block_5m_header_fields() {
    let block = codec::decode_block(&load_fixture("block_5000038.rmp.lz4")).unwrap();
    let header = &block.block.inner().header.header;

    assert_eq!(header.number, 5_000_038);
    assert_eq!(header.gas_used, 1_722_800);
    assert_eq!(header.gas_limit, 2_000_000);
    assert_eq!(header.base_fee_per_gas, Some(622_120_557));
    assert_eq!(header.timestamp, 1_749_160_149);
}

#[test]
fn block_5m_hashes() {
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
fn block_5m_tx_and_receipt_counts() {
    let block = codec::decode_block(&load_fixture("block_5000038.rmp.lz4")).unwrap();
    assert_eq!(block.block.inner().body.transactions.len(), 8);
    assert_eq!(block.receipts.len(), 8);
    assert_eq!(
        block.receipts.len(),
        block.block.inner().body.transactions.len(),
        "receipt count must match tx count"
    );
}

#[test]
fn block_5m_system_tx() {
    let block = codec::decode_block(&load_fixture("block_5000038.rmp.lz4")).unwrap();
    assert_eq!(block.system_txs.len(), 1);

    let stx = &block.system_txs[0];
    assert!(matches!(stx.tx, WireTxEnum::Legacy(_)));
    assert_eq!(stx.tx.input().len(), 68); // ERC20 transfer(address,uint256)
    assert!(stx.receipt.is_some());
    assert_eq!(
        format!("{:#x}", stx.tx.to().unwrap()),
        "0x9b498c3c8a0b8cd8ba1d9851d40d186f1872b44e"
    );
}

#[test]
fn block_5m_regular_txs_have_nonzero_signatures() {
    let block = codec::decode_block(&load_fixture("block_5000038.rmp.lz4")).unwrap();
    for tx in &block.block.inner().body.transactions {
        assert!(!tx.signature.r.is_zero());
        assert!(!tx.signature.s.is_zero());
    }
}

#[test]
fn block_5m_receipt_tx_types_are_valid() {
    let block = codec::decode_block(&load_fixture("block_5000038.rmp.lz4")).unwrap();
    for receipt in &block.receipts {
        assert!(
            ["Legacy", "Eip1559", "Eip2930"].contains(&receipt.tx_type.as_str()),
            "unexpected tx_type: {}",
            receipt.tx_type
        );
    }
}

#[test]
fn block_5m_cumulative_gas_is_monotonic() {
    let block = codec::decode_block(&load_fixture("block_5000038.rmp.lz4")).unwrap();
    let mut prev = 0u64;
    for receipt in &block.receipts {
        assert!(receipt.cumulative_gas_used >= prev);
        prev = receipt.cumulative_gas_used;
    }
}

#[test]
fn block_5m_summary_output() {
    let block = codec::decode_block(&load_fixture("block_5000038.rmp.lz4")).unwrap();
    let s = block.summary();
    assert_eq!(s.block_number, 5_000_038);
    assert_eq!(s.tx_count, 8);
    assert_eq!(s.system_tx_count, 1);
    assert_eq!(s.receipt_count, 8);
    assert_eq!(s.gas_used, 1_722_800);

    let json = serde_json::to_string(&s).unwrap();
    assert!(json.contains("5000038"));
    assert!(json.contains("6639e377"));
}

// --- Testnet block (chain ID 998) ---

#[test]
fn testnet_block_decodes() {
    let block = codec::decode_block(&load_fixture("block_testnet_48186001.rmp.lz4")).unwrap();
    let header = &block.block.inner().header.header;

    assert_eq!(header.number, 48_186_001);
    assert_eq!(header.gas_limit, 3_000_000);
    assert_eq!(
        format!("{:#x}", block.block.inner().header.hash),
        "0xacef176c39777f536aaf21e6ced1e27bc8d57e16c21a376957ebfb810b1777a8"
    );
}

// --- Decompression ---

#[test]
fn decompress_raw_produces_larger_output() {
    let compressed = load_fixture("block_1.rmp.lz4");
    let raw = codec::decompress_raw(&compressed).unwrap();
    assert!(!raw.is_empty());
    assert!(raw.len() > compressed.len());
}
