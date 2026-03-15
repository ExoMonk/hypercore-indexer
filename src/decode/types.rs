use alloy_primitives::{Address, Bytes, B256, U256};
use serde::Serialize;

/// A fully decoded block with hashes computed.
#[derive(Debug, Serialize)]
pub struct DecodedBlock {
    pub number: u64,
    pub hash: B256,
    pub parent_hash: B256,
    pub timestamp: u64,
    pub gas_used: u64,
    pub gas_limit: u64,
    pub base_fee_per_gas: Option<u64>,
    pub transactions: Vec<DecodedTx>,
    pub system_transfers: Vec<DecodedSystemTx>,
}

/// A decoded regular transaction with computed hash.
#[derive(Debug, Serialize)]
pub struct DecodedTx {
    pub hash: B256,
    pub tx_index: usize,
    pub tx_type: TxType,
    pub from: Option<Address>,
    pub to: Option<Address>,
    pub value: U256,
    pub input: Bytes,
    pub gas_limit: u64,
    pub success: bool,
    pub gas_used: u64,
    pub logs: Vec<DecodedLog>,
}

/// Transaction type enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum TxType {
    Legacy,
    Eip2930,
    Eip1559,
}

/// A decoded log entry.
#[derive(Debug, Serialize)]
pub struct DecodedLog {
    pub log_index: usize,
    pub address: Address,
    pub topics: Vec<B256>,
    pub data: Bytes,
}

/// Dual hash for system transactions.
#[derive(Debug, Clone, Serialize)]
pub struct DualHash {
    /// Hash using Hyperliquid RPC convention (v=chainId*2+35, r=0, s=0).
    pub official: B256,
    /// Hash using nanoreth/explorer convention (v=chainId*2+36, r=1, s=fromAddress).
    pub explorer: B256,
}

/// A decoded system transaction (bridge transfer from HyperCore to HyperEVM).
#[derive(Debug, Serialize)]
pub struct DecodedSystemTx {
    pub official_hash: B256,
    pub explorer_hash: B256,
    pub system_address: Address,
    pub asset_type: AssetType,
    pub recipient: Address,
    pub amount_wei: U256,
}

/// Type of asset being bridged in a system transaction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum AssetType {
    /// Native HYPE transfer from 0x2222...2222 (empty input, value > 0).
    NativeHype,
    /// Spot token ERC20 transfer from 0x2000...{assetIndex}.
    SpotToken { asset_index: u16 },
}
