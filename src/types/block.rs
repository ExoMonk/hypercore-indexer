use alloy_primitives::{Address, Bloom, Bytes, B256, U256};
use serde::{Deserialize, Serialize};

/// Top-level wrapper: the S3 payload is `Vec<BlockAndReceipts>` (always length 1).
#[derive(Debug, Deserialize)]
pub struct BlockAndReceipts {
    pub block: EvmBlock,
    pub receipts: Vec<TransactionReceipt>,
    #[serde(default)]
    pub system_txs: Vec<SystemTx>,
    /// MessagePack compat — field format changes across Hyperliquid versions.
    /// We never read this, so skip whatever type it is.
    #[serde(default, deserialize_with = "deserialize_ignored")]
    #[allow(dead_code)]
    pub read_precompile_calls: (),
    /// MessagePack compat — may be absent in older blocks.
    #[serde(default, deserialize_with = "deserialize_ignored")]
    #[allow(dead_code)]
    pub highest_precompile_address: (),
}

/// Tagged enum — always "Reth115" variant containing a SealedBlock-like structure.
#[derive(Debug, Deserialize)]
pub enum EvmBlock {
    Reth115(WireBlock),
}

impl EvmBlock {
    pub fn inner(&self) -> &WireBlock {
        match self {
            EvmBlock::Reth115(b) => b,
        }
    }
}

/// Matches reth's SealedBlock serde layout: header (SealedHeader) + body.
#[derive(Debug, Deserialize)]
pub struct WireBlock {
    pub header: WireSealedHeader,
    pub body: WireBlockBody,
}

/// SealedHeader: reth serializes as { hash, header } — hash comes first.
#[derive(Debug, Deserialize)]
pub struct WireSealedHeader {
    pub hash: B256,
    pub header: WireHeader,
}

/// Ethereum block header fields.
///
/// Wire format uses camelCase field names (confirmed by live S3 Python decode):
/// parentHash, sha3Uncles, miner, stateRoot, transactionsRoot, receiptsRoot,
/// logsBloom, difficulty, number, gasLimit, gasUsed, timestamp, extraData,
/// mixHash, nonce, baseFeePerGas, withdrawalsRoot, blobGasUsed, excessBlobGas,
/// parentBeaconBlockRoot.
/// All fields required for MessagePack deserialization compatibility, even if not all are read in Rust.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct WireHeader {
    pub parent_hash: B256,
    #[serde(rename = "sha3Uncles")]
    pub ommers_hash: B256,
    #[serde(rename = "miner")]
    pub beneficiary: Address,
    pub state_root: B256,
    pub transactions_root: B256,
    pub receipts_root: B256,
    pub logs_bloom: Bloom,
    pub difficulty: U256,
    #[serde(deserialize_with = "deserialize_u64_from_bytes")]
    pub number: u64,
    #[serde(deserialize_with = "deserialize_u64_from_bytes")]
    pub gas_limit: u64,
    #[serde(deserialize_with = "deserialize_u64_from_bytes")]
    pub gas_used: u64,
    #[serde(deserialize_with = "deserialize_u64_from_bytes")]
    pub timestamp: u64,
    pub extra_data: Bytes,
    pub mix_hash: Option<B256>,
    #[serde(default, deserialize_with = "deserialize_option_u64_from_bytes")]
    pub nonce: Option<u64>,
    #[serde(default, deserialize_with = "deserialize_option_u64_from_bytes")]
    pub base_fee_per_gas: Option<u64>,
    #[serde(default)]
    pub withdrawals_root: Option<B256>,
    #[serde(default, deserialize_with = "deserialize_option_u64_from_bytes")]
    pub blob_gas_used: Option<u64>,
    #[serde(default, deserialize_with = "deserialize_option_u64_from_bytes")]
    pub excess_blob_gas: Option<u64>,
    #[serde(default)]
    pub parent_beacon_block_root: Option<B256>,
    #[serde(default)]
    pub requests_hash: Option<B256>,
}

/// Block body: transactions + ommers + withdrawals.
#[derive(Debug, Deserialize)]
pub struct WireBlockBody {
    pub transactions: Vec<WireSignedTx>,
    /// Required for MessagePack deserialization compatibility.
    #[serde(default)]
    #[allow(dead_code)]
    pub ommers: Vec<serde_json::Value>,
    /// Required for MessagePack deserialization compatibility.
    #[serde(default)]
    #[allow(dead_code)]
    pub withdrawals: Option<Vec<serde_json::Value>>,
}

/// A signed transaction: inner tx + signature.
/// Note: S3 data does NOT include pre-computed tx hashes. Hash must be computed
/// from the raw fields (RLP encode then keccak256). This is handled in M2.
#[derive(Debug, Deserialize)]
pub struct WireSignedTx {
    pub transaction: WireTxEnum,
    pub signature: WireSignature,
}

/// Transaction type enum matching reth's EthereumTypedTransaction serde.
#[derive(Debug, Deserialize)]
pub enum WireTxEnum {
    Legacy(WireLegacyTx),
    Eip2930(WireEip2930Tx),
    Eip1559(WireEip1559Tx),
}

impl WireTxEnum {
    pub fn to(&self) -> Option<Address> {
        match self {
            WireTxEnum::Legacy(tx) => tx.to,
            WireTxEnum::Eip2930(tx) => tx.to,
            WireTxEnum::Eip1559(tx) => tx.to,
        }
    }

    /// Future use for signer recovery (M4+).
    #[allow(dead_code)]
    pub fn sender(&self) -> Option<Address> {
        // Transaction doesn't contain from — it's recovered from signature
        None
    }

    pub fn value(&self) -> U256 {
        match self {
            WireTxEnum::Legacy(tx) => tx.value,
            WireTxEnum::Eip2930(tx) => tx.value,
            WireTxEnum::Eip1559(tx) => tx.value,
        }
    }

    pub fn nonce(&self) -> u64 {
        match self {
            WireTxEnum::Legacy(tx) => tx.nonce,
            WireTxEnum::Eip2930(tx) => tx.nonce,
            WireTxEnum::Eip1559(tx) => tx.nonce,
        }
    }

    pub fn input(&self) -> &Bytes {
        match self {
            WireTxEnum::Legacy(tx) => &tx.input,
            WireTxEnum::Eip2930(tx) => &tx.input,
            WireTxEnum::Eip1559(tx) => &tx.input,
        }
    }

    pub fn gas_limit(&self) -> u64 {
        match self {
            WireTxEnum::Legacy(tx) => tx.gas_limit,
            WireTxEnum::Eip2930(tx) => tx.gas_limit,
            WireTxEnum::Eip1559(tx) => tx.gas_limit,
        }
    }

    pub fn tx_type_name(&self) -> &'static str {
        match self {
            WireTxEnum::Legacy(_) => "Legacy",
            WireTxEnum::Eip2930(_) => "Eip2930",
            WireTxEnum::Eip1559(_) => "Eip1559",
        }
    }
}

/// Wire format uses camelCase. The gas field is "gas" in the wire format
/// (confirmed by coredrain: `txData.gas`), not "gasLimit".
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WireLegacyTx {
    #[serde(deserialize_with = "deserialize_u64_from_bytes")]
    pub nonce: u64,
    #[serde(deserialize_with = "deserialize_u128_from_bytes")]
    pub gas_price: u128,
    #[serde(rename = "gas", deserialize_with = "deserialize_u64_from_bytes")]
    pub gas_limit: u64,
    pub to: Option<Address>,
    pub value: U256,
    pub input: Bytes,
    #[serde(default, deserialize_with = "deserialize_option_u64_from_bytes")]
    pub chain_id: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WireEip2930Tx {
    #[serde(deserialize_with = "deserialize_u64_from_bytes")]
    pub chain_id: u64,
    #[serde(deserialize_with = "deserialize_u64_from_bytes")]
    pub nonce: u64,
    #[serde(deserialize_with = "deserialize_u128_from_bytes")]
    pub gas_price: u128,
    #[serde(rename = "gas", deserialize_with = "deserialize_u64_from_bytes")]
    pub gas_limit: u64,
    pub to: Option<Address>,
    pub value: U256,
    pub input: Bytes,
    #[serde(default)]
    pub access_list: Vec<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WireEip1559Tx {
    #[serde(deserialize_with = "deserialize_u64_from_bytes")]
    pub chain_id: u64,
    #[serde(deserialize_with = "deserialize_u64_from_bytes")]
    pub nonce: u64,
    #[serde(deserialize_with = "deserialize_u128_from_bytes")]
    pub max_priority_fee_per_gas: u128,
    #[serde(deserialize_with = "deserialize_u128_from_bytes")]
    pub max_fee_per_gas: u128,
    #[serde(rename = "gas", deserialize_with = "deserialize_u64_from_bytes")]
    pub gas_limit: u64,
    pub to: Option<Address>,
    pub value: U256,
    pub input: Bytes,
    #[serde(default)]
    pub access_list: Vec<serde_json::Value>,
}

/// Signature: wire format is a positional list [r, s, v].
/// Using a custom deserializer to handle both positional (seq) and named (map) formats.
#[derive(Debug)]
pub struct WireSignature {
    pub r: U256,
    pub s: U256,
    pub v: bool,
}

impl<'de> Deserialize<'de> for WireSignature {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de;

        struct SigVisitor;

        impl<'de> de::Visitor<'de> for SigVisitor {
            type Value = WireSignature;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(f, "a signature as [r, s, v] sequence or {{r, s, v}} map")
            }

            fn visit_seq<A: de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> Result<WireSignature, A::Error> {
                let r: U256 = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(0, &self))?;
                let s: U256 = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(1, &self))?;
                let v: bool = seq
                    .next_element_seed(VParitySeed)?
                    .ok_or_else(|| de::Error::invalid_length(2, &self))?;
                Ok(WireSignature { r, s, v })
            }

            fn visit_map<A: de::MapAccess<'de>>(
                self,
                mut map: A,
            ) -> Result<WireSignature, A::Error> {
                let mut r = None;
                let mut s = None;
                let mut v = None;

                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "r" => r = Some(map.next_value()?),
                        "s" => s = Some(map.next_value()?),
                        "v" => v = Some(map.next_value_seed(VParitySeed)?),
                        _ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }

                Ok(WireSignature {
                    r: r.ok_or_else(|| de::Error::missing_field("r"))?,
                    s: s.ok_or_else(|| de::Error::missing_field("s"))?,
                    v: v.ok_or_else(|| de::Error::missing_field("v"))?,
                })
            }
        }

        deserializer.deserialize_any(SigVisitor)
    }
}

/// System transaction: bare tx (no signature wrapper) + optional receipt.
#[derive(Debug, Deserialize)]
pub struct SystemTx {
    pub tx: WireTxEnum,
    pub receipt: Option<TransactionReceipt>,
}

/// Receipt matching the wire format. tx_type is a string ("Legacy", "Eip1559").
/// cumulative_gas_used is a native msgpack integer.
///
/// Reth's Receipt serde uses: tx_type, success, cumulative_gas_used, logs.
/// No rename_all needed — these are the actual field names.
#[derive(Debug, Deserialize)]
pub struct TransactionReceipt {
    /// Required for MessagePack deserialization compatibility.
    #[serde(deserialize_with = "deserialize_tx_type_string")]
    #[allow(dead_code)]
    pub tx_type: String,
    pub success: bool,
    pub cumulative_gas_used: u64,
    pub logs: Vec<WireLog>,
}

#[derive(Debug, Deserialize)]
pub struct WireLog {
    pub address: Address,
    pub data: WireLogData,
}

#[derive(Debug, Deserialize)]
pub struct WireLogData {
    pub topics: Vec<B256>,
    pub data: Bytes,
}

/// JSON-serializable block summary for display output.
#[derive(Debug, Serialize)]
pub struct BlockSummary {
    pub block_number: u64,
    pub block_hash: String,
    pub parent_hash: String,
    pub timestamp: u64,
    pub gas_used: u64,
    pub gas_limit: u64,
    pub base_fee_per_gas: Option<u64>,
    pub tx_count: usize,
    pub system_tx_count: usize,
    pub receipt_count: usize,
    pub system_txs: Vec<SystemTxSummary>,
}

#[derive(Debug, Serialize)]
pub struct SystemTxSummary {
    pub tx_type: String,
    pub to: Option<String>,
    pub value: String,
    pub input_size: usize,
    pub has_receipt: bool,
}

impl BlockAndReceipts {
    pub fn summary(&self) -> BlockSummary {
        let block = self.block.inner();
        let header = &block.header.header;

        let system_txs = self
            .system_txs
            .iter()
            .map(|stx| SystemTxSummary {
                tx_type: stx.tx.tx_type_name().to_string(),
                to: stx.tx.to().map(|a| format!("{a:#x}")),
                value: format!("{}", stx.tx.value()),
                input_size: stx.tx.input().len(),
                has_receipt: stx.receipt.is_some(),
            })
            .collect();

        BlockSummary {
            block_number: header.number,
            block_hash: format!("{:#x}", block.header.hash),
            parent_hash: format!("{:#x}", header.parent_hash),
            timestamp: header.timestamp,
            gas_used: header.gas_used,
            gas_limit: header.gas_limit,
            base_fee_per_gas: header.base_fee_per_gas,
            tx_count: block.body.transactions.len(),
            system_tx_count: self.system_txs.len(),
            receipt_count: self.receipts.len(),
            system_txs,
        }
    }
}

// --- Custom deserializers ---

/// Deserialize u64 from msgpack bytes (alloy serializes integers as big-endian bytes).
fn deserialize_u64_from_bytes<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de;

    struct U64Visitor;

    impl<'de> de::Visitor<'de> for U64Visitor {
        type Value = u64;

        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "a u64 as integer or bytes")
        }

        fn visit_u64<E: de::Error>(self, v: u64) -> Result<u64, E> {
            Ok(v)
        }

        fn visit_u32<E: de::Error>(self, v: u32) -> Result<u64, E> {
            Ok(v as u64)
        }

        fn visit_u16<E: de::Error>(self, v: u16) -> Result<u64, E> {
            Ok(v as u64)
        }

        fn visit_u8<E: de::Error>(self, v: u8) -> Result<u64, E> {
            Ok(v as u64)
        }

        fn visit_i64<E: de::Error>(self, v: i64) -> Result<u64, E> {
            Ok(v as u64)
        }

        fn visit_bytes<E: de::Error>(self, v: &[u8]) -> Result<u64, E> {
            if v.is_empty() {
                return Ok(0);
            }
            if v.len() > 8 {
                return Err(E::custom(format!(
                    "byte array too long for u64: {} bytes",
                    v.len()
                )));
            }
            let mut buf = [0u8; 8];
            buf[8 - v.len()..].copy_from_slice(v);
            Ok(u64::from_be_bytes(buf))
        }

        fn visit_byte_buf<E: de::Error>(self, v: Vec<u8>) -> Result<u64, E> {
            self.visit_bytes(&v)
        }
    }

    deserializer.deserialize_any(U64Visitor)
}

/// Deserialize u128 from msgpack bytes (gas_price is bytes[16], max_fee_per_gas is bytes[16]).
fn deserialize_u128_from_bytes<'de, D>(deserializer: D) -> Result<u128, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de;

    struct U128Visitor;

    impl<'de> de::Visitor<'de> for U128Visitor {
        type Value = u128;

        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "a u128 as integer or bytes")
        }

        fn visit_u64<E: de::Error>(self, v: u64) -> Result<u128, E> {
            Ok(v as u128)
        }

        fn visit_u32<E: de::Error>(self, v: u32) -> Result<u128, E> {
            Ok(v as u128)
        }

        fn visit_u16<E: de::Error>(self, v: u16) -> Result<u128, E> {
            Ok(v as u128)
        }

        fn visit_u8<E: de::Error>(self, v: u8) -> Result<u128, E> {
            Ok(v as u128)
        }

        fn visit_i64<E: de::Error>(self, v: i64) -> Result<u128, E> {
            Ok(v as u128)
        }

        fn visit_bytes<E: de::Error>(self, v: &[u8]) -> Result<u128, E> {
            if v.is_empty() {
                return Ok(0);
            }
            if v.len() > 16 {
                return Err(E::custom(format!(
                    "byte array too long for u128: {} bytes",
                    v.len()
                )));
            }
            let mut buf = [0u8; 16];
            buf[16 - v.len()..].copy_from_slice(v);
            Ok(u128::from_be_bytes(buf))
        }

        fn visit_byte_buf<E: de::Error>(self, v: Vec<u8>) -> Result<u128, E> {
            self.visit_bytes(&v)
        }
    }

    deserializer.deserialize_any(U128Visitor)
}

fn deserialize_option_u64_from_bytes<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de;

    struct OptU64Visitor;

    impl<'de> de::Visitor<'de> for OptU64Visitor {
        type Value = Option<u64>;

        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "an optional u64 as integer, bytes, or null")
        }

        fn visit_none<E: de::Error>(self) -> Result<Option<u64>, E> {
            Ok(None)
        }

        fn visit_unit<E: de::Error>(self) -> Result<Option<u64>, E> {
            Ok(None)
        }

        fn visit_some<D2: serde::Deserializer<'de>>(
            self,
            deserializer: D2,
        ) -> Result<Option<u64>, D2::Error> {
            deserialize_u64_from_bytes(deserializer).map(Some)
        }

        fn visit_u64<E: de::Error>(self, v: u64) -> Result<Option<u64>, E> {
            Ok(Some(v))
        }

        fn visit_u32<E: de::Error>(self, v: u32) -> Result<Option<u64>, E> {
            Ok(Some(v as u64))
        }

        fn visit_u8<E: de::Error>(self, v: u8) -> Result<Option<u64>, E> {
            Ok(Some(v as u64))
        }

        fn visit_i64<E: de::Error>(self, v: i64) -> Result<Option<u64>, E> {
            Ok(Some(v as u64))
        }

        fn visit_bytes<E: de::Error>(self, v: &[u8]) -> Result<Option<u64>, E> {
            if v.is_empty() {
                return Ok(Some(0));
            }
            if v.len() > 8 {
                return Err(E::custom(format!(
                    "byte array too long for u64: {} bytes",
                    v.len()
                )));
            }
            let mut buf = [0u8; 8];
            buf[8 - v.len()..].copy_from_slice(v);
            Ok(Some(u64::from_be_bytes(buf)))
        }

        fn visit_byte_buf<E: de::Error>(self, v: Vec<u8>) -> Result<Option<u64>, E> {
            self.visit_bytes(&v)
        }
    }

    deserializer.deserialize_any(OptU64Visitor)
}

/// DeserializeSeed for v parity — used by WireSignature's custom deserializer.
struct VParitySeed;

impl<'de> serde::de::DeserializeSeed<'de> for VParitySeed {
    type Value = bool;

    fn deserialize<D: serde::Deserializer<'de>>(self, deserializer: D) -> Result<bool, D::Error> {
        deserialize_v_parity(deserializer)
    }
}

/// Deserialize v parity from bytes, bool, or integer.
fn deserialize_v_parity<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de;

    struct VParityVisitor;

    impl<'de> de::Visitor<'de> for VParityVisitor {
        type Value = bool;

        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "v parity as bool, integer, or bytes")
        }

        fn visit_bool<E: de::Error>(self, v: bool) -> Result<bool, E> {
            Ok(v)
        }

        fn visit_u64<E: de::Error>(self, v: u64) -> Result<bool, E> {
            Ok(v != 0)
        }

        fn visit_u8<E: de::Error>(self, v: u8) -> Result<bool, E> {
            Ok(v != 0)
        }

        fn visit_i64<E: de::Error>(self, v: i64) -> Result<bool, E> {
            Ok(v != 0)
        }

        fn visit_bytes<E: de::Error>(self, v: &[u8]) -> Result<bool, E> {
            Ok(v.iter().any(|&b| b != 0))
        }

        fn visit_byte_buf<E: de::Error>(self, v: Vec<u8>) -> Result<bool, E> {
            self.visit_bytes(&v)
        }
    }

    deserializer.deserialize_any(VParityVisitor)
}

/// tx_type in receipts is a string ("Legacy", "Eip1559"), not integer.
fn deserialize_tx_type_string<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de;

    struct TxTypeVisitor;

    impl<'de> de::Visitor<'de> for TxTypeVisitor {
        type Value = String;

        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "a tx type as string or integer")
        }

        fn visit_str<E: de::Error>(self, v: &str) -> Result<String, E> {
            Ok(v.to_string())
        }

        fn visit_string<E: de::Error>(self, v: String) -> Result<String, E> {
            Ok(v)
        }

        fn visit_u8<E: de::Error>(self, v: u8) -> Result<String, E> {
            self.visit_u64(v as u64)
        }

        fn visit_u32<E: de::Error>(self, v: u32) -> Result<String, E> {
            self.visit_u64(v as u64)
        }

        fn visit_u64<E: de::Error>(self, v: u64) -> Result<String, E> {
            Ok(match v {
                0 => "Legacy".to_string(),
                1 => "Eip2930".to_string(),
                2 => "Eip1559".to_string(),
                _ => format!("Unknown({v})"),
            })
        }
    }

    deserializer.deserialize_any(TxTypeVisitor)
}

/// Skip any MessagePack value without caring about its type.
/// Used for fields we need for serde compatibility but never read.
/// Handles bytes, arrays, maps, strings, integers — anything.
fn deserialize_ignored<'de, D>(deserializer: D) -> Result<(), D::Error>
where
    D: serde::Deserializer<'de>,
{
    serde::de::IgnoredAny::deserialize(deserializer)?;
    Ok(())
}

/// Wire format deserializer correctness:
/// - u64/u128 from big-endian bytes and native msgpack ints
/// - Option<u64> from null, int, and missing fields
/// - v parity from bool/int/bytes
/// - tx_type from string ("Legacy") and integer (0, 2)
/// - WireTxEnum accessors (to, value, nonce, input, gas_limit, tx_type_name)
/// - Network config: bucket names, chain IDs, FromStr parsing
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn u64_from_8_byte_be() {
        // Simulate msgpack bin[8] for value 2_000_000 (0x001E8480)
        #[derive(Deserialize)]
        struct W {
            #[serde(deserialize_with = "deserialize_u64_from_bytes")]
            v: u64,
        }
        let mp = rmp_serde::to_vec(&serde_json::json!({"v": 2_000_000u64})).unwrap();
        let w: W = rmp_serde::from_slice(&mp).unwrap();
        assert_eq!(w.v, 2_000_000);
    }

    #[test]
    fn u64_from_native_int() {
        #[derive(Deserialize)]
        struct W {
            #[serde(deserialize_with = "deserialize_u64_from_bytes")]
            v: u64,
        }
        // Small int packed natively by msgpack
        let mp = rmp_serde::to_vec(&serde_json::json!({"v": 42u64})).unwrap();
        let w: W = rmp_serde::from_slice(&mp).unwrap();
        assert_eq!(w.v, 42);
    }

    #[test]
    fn u128_from_16_byte_be() {
        #[derive(Deserialize)]
        struct W {
            #[serde(deserialize_with = "deserialize_u128_from_bytes")]
            v: u128,
        }
        let mp = rmp_serde::to_vec(&serde_json::json!({"v": 1_000_000_000u64})).unwrap();
        let w: W = rmp_serde::from_slice(&mp).unwrap();
        assert_eq!(w.v, 1_000_000_000);
    }

    #[test]
    fn u128_from_small_int() {
        #[derive(Deserialize)]
        struct W {
            #[serde(deserialize_with = "deserialize_u128_from_bytes")]
            v: u128,
        }
        let mp = rmp_serde::to_vec(&serde_json::json!({"v": 0u64})).unwrap();
        let w: W = rmp_serde::from_slice(&mp).unwrap();
        assert_eq!(w.v, 0);
    }

    #[test]
    fn option_u64_none_from_null() {
        #[derive(Deserialize)]
        struct W {
            #[serde(default, deserialize_with = "deserialize_option_u64_from_bytes")]
            v: Option<u64>,
        }
        let mp = rmp_serde::to_vec(&serde_json::json!({"v": null})).unwrap();
        let w: W = rmp_serde::from_slice(&mp).unwrap();
        assert_eq!(w.v, None);
    }

    #[test]
    fn option_u64_some_from_int() {
        #[derive(Deserialize)]
        struct W {
            #[serde(default, deserialize_with = "deserialize_option_u64_from_bytes")]
            v: Option<u64>,
        }
        let mp = rmp_serde::to_vec(&serde_json::json!({"v": 100_000_000u64})).unwrap();
        let w: W = rmp_serde::from_slice(&mp).unwrap();
        assert_eq!(w.v, Some(100_000_000));
    }

    #[test]
    fn option_u64_defaults_when_missing() {
        #[derive(Deserialize)]
        struct W {
            #[serde(default, deserialize_with = "deserialize_option_u64_from_bytes")]
            v: Option<u64>,
        }
        // Empty map — field absent
        let mp = rmp_serde::to_vec(&serde_json::json!({})).unwrap();
        let w: W = rmp_serde::from_slice(&mp).unwrap();
        assert_eq!(w.v, None);
    }

    #[test]
    fn v_parity_false_from_zero() {
        #[derive(Deserialize)]
        struct W {
            #[serde(deserialize_with = "deserialize_v_parity")]
            v: bool,
        }
        let mp = rmp_serde::to_vec(&serde_json::json!({"v": 0u64})).unwrap();
        let w: W = rmp_serde::from_slice(&mp).unwrap();
        assert!(!w.v);
    }

    #[test]
    fn v_parity_true_from_one() {
        #[derive(Deserialize)]
        struct W {
            #[serde(deserialize_with = "deserialize_v_parity")]
            v: bool,
        }
        let mp = rmp_serde::to_vec(&serde_json::json!({"v": 1u64})).unwrap();
        let w: W = rmp_serde::from_slice(&mp).unwrap();
        assert!(w.v);
    }

    #[test]
    fn tx_type_from_string() {
        #[derive(Deserialize)]
        struct W {
            #[serde(deserialize_with = "deserialize_tx_type_string")]
            t: String,
        }
        let mp = rmp_serde::to_vec(&serde_json::json!({"t": "Legacy"})).unwrap();
        let w: W = rmp_serde::from_slice(&mp).unwrap();
        assert_eq!(w.t, "Legacy");
    }

    #[test]
    fn tx_type_from_int_0_is_legacy() {
        #[derive(Deserialize)]
        struct W {
            #[serde(deserialize_with = "deserialize_tx_type_string")]
            t: String,
        }
        let mp = rmp_serde::to_vec(&serde_json::json!({"t": 0u64})).unwrap();
        let w: W = rmp_serde::from_slice(&mp).unwrap();
        assert_eq!(w.t, "Legacy");
    }

    #[test]
    fn tx_type_from_int_2_is_eip1559() {
        #[derive(Deserialize)]
        struct W {
            #[serde(deserialize_with = "deserialize_tx_type_string")]
            t: String,
        }
        let mp = rmp_serde::to_vec(&serde_json::json!({"t": 2u64})).unwrap();
        let w: W = rmp_serde::from_slice(&mp).unwrap();
        assert_eq!(w.t, "Eip1559");
    }

    // --- WireTxEnum accessor tests ---

    #[test]
    fn wire_tx_enum_type_names() {
        assert_eq!(
            WireTxEnum::Legacy(WireLegacyTx {
                nonce: 0,
                gas_price: 0,
                gas_limit: 0,
                to: None,
                value: U256::ZERO,
                input: Bytes::new(),
                chain_id: None,
            })
            .tx_type_name(),
            "Legacy"
        );
    }

    #[test]
    fn wire_tx_enum_to_returns_none_for_contract_creation() {
        let tx = WireTxEnum::Legacy(WireLegacyTx {
            nonce: 0,
            gas_price: 0,
            gas_limit: 21000,
            to: None,
            value: U256::ZERO,
            input: Bytes::from_static(&[0x60, 0x80]),
            chain_id: Some(999),
        });
        assert!(tx.to().is_none());
    }

    #[test]
    fn wire_tx_enum_accessors_consistent() {
        let addr = "0x9b498c3c8a0b8cd8ba1d9851d40d186f1872b44e"
            .parse::<Address>()
            .unwrap();
        let tx = WireTxEnum::Eip1559(WireEip1559Tx {
            chain_id: 999,
            nonce: 42,
            max_priority_fee_per_gas: 100,
            max_fee_per_gas: 1_000_000_000,
            gas_limit: 21000,
            to: Some(addr),
            value: U256::from(1_000_000u64),
            input: Bytes::new(),
            access_list: vec![],
        });
        assert_eq!(tx.nonce(), 42);
        assert_eq!(tx.to(), Some(addr));
        assert_eq!(tx.value(), U256::from(1_000_000u64));
        assert!(tx.input().is_empty());
        assert_eq!(tx.tx_type_name(), "Eip1559");
    }

    // --- Network ---

    #[test]
    fn network_from_str() {
        assert!(matches!(
            "mainnet".parse::<crate::s3::client::Network>().unwrap(),
            crate::s3::client::Network::Mainnet
        ));
        assert!(matches!(
            "testnet".parse::<crate::s3::client::Network>().unwrap(),
            crate::s3::client::Network::Testnet
        ));
        assert!(matches!(
            "Mainnet".parse::<crate::s3::client::Network>().unwrap(),
            crate::s3::client::Network::Mainnet
        ));
        assert!("invalid".parse::<crate::s3::client::Network>().is_err());
    }

    #[test]
    fn network_bucket_names() {
        use crate::s3::client::Network;
        assert_eq!(Network::Mainnet.s3_bucket(), "hl-mainnet-evm-blocks");
        assert_eq!(Network::Testnet.s3_bucket(), "hl-testnet-evm-blocks");
    }

    #[test]
    fn network_chain_ids() {
        use crate::s3::client::Network;
        assert_eq!(Network::Mainnet.chain_id(), 999);
        assert_eq!(Network::Testnet.chain_id(), 998);
    }
}
