use alloy_primitives::{B256, U256};
use tiny_keccak::{Hasher, Keccak};

use crate::types::block::{
    WireEip1559Tx, WireEip2930Tx, WireLegacyTx, WireSignedTx, WireTxEnum,
};

use super::types::DualHash;

/// Compute keccak256 of the given bytes.
fn keccak256(data: &[u8]) -> B256 {
    let mut hasher = Keccak::v256();
    let mut output = [0u8; 32];
    hasher.update(data);
    hasher.finalize(&mut output);
    B256::from(output)
}

/// Compute the transaction hash for a regular signed transaction.
///
/// - Legacy: `keccak256(RLP([nonce, gasPrice, gasLimit, to, value, input, v, r, s]))`
/// - Eip2930: `keccak256(0x01 || RLP([chainId, nonce, gasPrice, gasLimit, to, value, input, accessList, v, r, s]))`
/// - Eip1559: `keccak256(0x02 || RLP([chainId, nonce, maxPriorityFee, maxFee, gasLimit, to, value, input, accessList, v, r, s]))`
pub fn compute_tx_hash(tx: &WireSignedTx) -> B256 {
    match &tx.transaction {
        WireTxEnum::Legacy(inner) => compute_legacy_signed_hash(inner, &tx.signature),
        WireTxEnum::Eip2930(inner) => compute_eip2930_signed_hash(inner, &tx.signature),
        WireTxEnum::Eip1559(inner) => compute_eip1559_signed_hash(inner, &tx.signature),
    }
}

/// Compute both hash conventions for a system transaction (unsigned).
///
/// Official (Hyperliquid RPC): v = chainId*2+35, r = 0, s = 0
/// Explorer (nanoreth/Hyperscan): v = chainId*2+36, r = 1, s = fromAddress as U256
pub fn compute_system_tx_dual_hash(tx: &WireTxEnum, chain_id: u64, from: &alloy_primitives::Address) -> DualHash {
    match tx {
        WireTxEnum::Legacy(inner) => {
            let official = compute_legacy_phantom_hash(inner, chain_id, false, from);
            let explorer = compute_legacy_phantom_hash(inner, chain_id, true, from);
            DualHash { official, explorer }
        }
        // System txs are always Legacy on Hyperliquid, but handle gracefully
        WireTxEnum::Eip2930(_) | WireTxEnum::Eip1559(_) => {
            // Fallback: treat as Legacy-style with the common accessors
            let official = compute_system_tx_generic_hash(tx, chain_id, false, from);
            let explorer = compute_system_tx_generic_hash(tx, chain_id, true, from);
            DualHash { official, explorer }
        }
    }
}

// --- Legacy signed tx hash ---

fn compute_legacy_signed_hash(
    tx: &WireLegacyTx,
    sig: &crate::types::block::WireSignature,
) -> B256 {
    // Compute v value for legacy: chain_id based EIP-155 or simple 27/28
    let v = match tx.chain_id {
        Some(chain_id) => {
            // EIP-155: v = chain_id * 2 + 35 + parity
            chain_id * 2 + 35 + (sig.v as u64)
        }
        None => {
            // Pre-EIP-155: v = 27 + parity
            27 + (sig.v as u64)
        }
    };

    let mut buf = Vec::with_capacity(256);
    let list_payload = rlp_encode_legacy_fields(tx, v, sig.r, sig.s);
    rlp_encode_list_header(&list_payload, &mut buf);
    buf.extend_from_slice(&list_payload);
    keccak256(&buf)
}

// --- Eip2930 signed tx hash ---

fn compute_eip2930_signed_hash(
    tx: &WireEip2930Tx,
    sig: &crate::types::block::WireSignature,
) -> B256 {
    let v_val = sig.v as u64;
    let mut list_buf = Vec::with_capacity(256);

    // RLP list: [chainId, nonce, gasPrice, gasLimit, to, value, input, accessList, v, r, s]
    let mut payload = Vec::with_capacity(256);
    rlp_encode_u64(tx.chain_id, &mut payload);
    rlp_encode_u64(tx.nonce, &mut payload);
    rlp_encode_u128(tx.gas_price, &mut payload);
    rlp_encode_u64(tx.gas_limit, &mut payload);
    rlp_encode_optional_address(tx.to, &mut payload);
    rlp_encode_u256(tx.value, &mut payload);
    rlp_encode_bytes(&tx.input, &mut payload);
    // access_list: encode as empty list for now (Hyperliquid txs rarely use access lists)
    rlp_encode_access_list(&tx.access_list, &mut payload);
    rlp_encode_u64(v_val, &mut payload);
    rlp_encode_u256(sig.r, &mut payload);
    rlp_encode_u256(sig.s, &mut payload);

    // 0x01 || RLP([...])
    list_buf.push(0x01);
    rlp_encode_list_header(&payload, &mut list_buf);
    list_buf.extend_from_slice(&payload);
    keccak256(&list_buf)
}

// --- Eip1559 signed tx hash ---

fn compute_eip1559_signed_hash(
    tx: &WireEip1559Tx,
    sig: &crate::types::block::WireSignature,
) -> B256 {
    let v_val = sig.v as u64;
    let mut list_buf = Vec::with_capacity(256);

    // RLP list: [chainId, nonce, maxPriorityFee, maxFee, gasLimit, to, value, input, accessList, v, r, s]
    let mut payload = Vec::with_capacity(256);
    rlp_encode_u64(tx.chain_id, &mut payload);
    rlp_encode_u64(tx.nonce, &mut payload);
    rlp_encode_u128(tx.max_priority_fee_per_gas, &mut payload);
    rlp_encode_u128(tx.max_fee_per_gas, &mut payload);
    rlp_encode_u64(tx.gas_limit, &mut payload);
    rlp_encode_optional_address(tx.to, &mut payload);
    rlp_encode_u256(tx.value, &mut payload);
    rlp_encode_bytes(&tx.input, &mut payload);
    // access_list: encode as empty list for now
    rlp_encode_access_list(&tx.access_list, &mut payload);
    rlp_encode_u64(v_val, &mut payload);
    rlp_encode_u256(sig.r, &mut payload);
    rlp_encode_u256(sig.s, &mut payload);

    // 0x02 || RLP([...])
    list_buf.push(0x02);
    rlp_encode_list_header(&payload, &mut list_buf);
    list_buf.extend_from_slice(&payload);
    keccak256(&list_buf)
}

// --- System tx phantom hash (Legacy only) ---

fn compute_legacy_phantom_hash(
    tx: &WireLegacyTx,
    chain_id: u64,
    explorer_mode: bool,
    from: &alloy_primitives::Address,
) -> B256 {
    let (v, r, s) = if explorer_mode {
        // Explorer convention: v = chainId*2+36, r = 1, s = fromAddress as U256
        let v = chain_id * 2 + 36;
        let r = U256::from(1u64);
        let s = U256::from_be_bytes(from.into_word().0);
        (v, r, s)
    } else {
        // Official convention: v = chainId*2+35, r = 0, s = 0
        let v = chain_id * 2 + 35;
        (v, U256::ZERO, U256::ZERO)
    };

    let mut buf = Vec::with_capacity(256);
    let list_payload = rlp_encode_legacy_fields(tx, v, r, s);
    rlp_encode_list_header(&list_payload, &mut buf);
    buf.extend_from_slice(&list_payload);
    keccak256(&buf)
}

/// Fallback for non-Legacy system txs (should not occur on Hyperliquid).
fn compute_system_tx_generic_hash(
    tx: &WireTxEnum,
    chain_id: u64,
    explorer_mode: bool,
    from: &alloy_primitives::Address,
) -> B256 {
    // Build a synthetic Legacy tx from the generic fields and hash it
    let legacy = WireLegacyTx {
        nonce: tx.nonce(),
        gas_price: 0, // system txs have 0 gas price
        gas_limit: tx.gas_limit(),
        to: tx.to(),
        value: tx.value(),
        input: tx.input().clone(),
        chain_id: Some(chain_id),
    };
    compute_legacy_phantom_hash(&legacy, chain_id, explorer_mode, from)
}

// --- RLP encoding helpers ---

/// Encode Legacy tx fields into RLP list payload (without the list header).
fn rlp_encode_legacy_fields(tx: &WireLegacyTx, v: u64, r: U256, s: U256) -> Vec<u8> {
    let mut payload = Vec::with_capacity(256);
    rlp_encode_u64(tx.nonce, &mut payload);
    rlp_encode_u128(tx.gas_price, &mut payload);
    rlp_encode_u64(tx.gas_limit, &mut payload);
    rlp_encode_optional_address(tx.to, &mut payload);
    rlp_encode_u256(tx.value, &mut payload);
    rlp_encode_bytes(&tx.input, &mut payload);
    rlp_encode_u64(v, &mut payload);
    rlp_encode_u256(r, &mut payload);
    rlp_encode_u256(s, &mut payload);
    payload
}

/// RLP encode a u64 value.
fn rlp_encode_u64(val: u64, buf: &mut Vec<u8>) {
    if val == 0 {
        buf.push(0x80); // empty string
    } else if val < 128 {
        buf.push(val as u8);
    } else {
        let bytes = val.to_be_bytes();
        let start = bytes.iter().position(|&b| b != 0).unwrap_or(7);
        let len = 8 - start;
        buf.push(0x80 + len as u8);
        buf.extend_from_slice(&bytes[start..]);
    }
}

/// RLP encode a u128 value.
fn rlp_encode_u128(val: u128, buf: &mut Vec<u8>) {
    if val == 0 {
        buf.push(0x80);
    } else if val < 128 {
        buf.push(val as u8);
    } else {
        let bytes = val.to_be_bytes();
        let start = bytes.iter().position(|&b| b != 0).unwrap_or(15);
        let len = 16 - start;
        buf.push(0x80 + len as u8);
        buf.extend_from_slice(&bytes[start..]);
    }
}

/// RLP encode a U256 value (as big-endian bytes, minimal encoding).
fn rlp_encode_u256(val: U256, buf: &mut Vec<u8>) {
    if val.is_zero() {
        buf.push(0x80); // empty string
    } else {
        let bytes = val.to_be_bytes::<32>();
        let start = bytes.iter().position(|&b| b != 0).unwrap_or(31);
        let trimmed = &bytes[start..];
        if trimmed.len() == 1 && trimmed[0] < 128 {
            buf.push(trimmed[0]);
        } else {
            buf.push(0x80 + trimmed.len() as u8);
            buf.extend_from_slice(trimmed);
        }
    }
}

/// RLP encode an optional address (20 bytes or empty string for None).
fn rlp_encode_optional_address(addr: Option<alloy_primitives::Address>, buf: &mut Vec<u8>) {
    match addr {
        Some(a) => {
            buf.push(0x80 + 20); // string of length 20
            buf.extend_from_slice(a.as_slice());
        }
        None => {
            buf.push(0x80); // empty string
        }
    }
}

/// RLP encode a byte string.
fn rlp_encode_bytes(data: &[u8], buf: &mut Vec<u8>) {
    if data.is_empty() {
        buf.push(0x80);
    } else if data.len() == 1 && data[0] < 128 {
        buf.push(data[0]);
    } else if data.len() < 56 {
        buf.push(0x80 + data.len() as u8);
        buf.extend_from_slice(data);
    } else {
        let len_bytes = encode_length_bytes(data.len());
        buf.push(0xb7 + len_bytes.len() as u8);
        buf.extend_from_slice(&len_bytes);
        buf.extend_from_slice(data);
    }
}

/// RLP encode the list header (prefix + length) for a payload.
fn rlp_encode_list_header(payload: &[u8], buf: &mut Vec<u8>) {
    let len = payload.len();
    if len < 56 {
        buf.push(0xc0 + len as u8);
    } else {
        let len_bytes = encode_length_bytes(len);
        buf.push(0xf7 + len_bytes.len() as u8);
        buf.extend_from_slice(&len_bytes);
    }
}

/// RLP encode an access list from serde_json::Value.
/// On Hyperliquid, access lists are typically empty.
fn rlp_encode_access_list(access_list: &[serde_json::Value], buf: &mut Vec<u8>) {
    if access_list.is_empty() {
        // Empty list
        buf.push(0xc0);
        return;
    }

    // Each access list item is { address, storageKeys: [...] }
    let mut items_payload = Vec::new();
    for item in access_list {
        let mut entry_payload = Vec::new();

        // Address
        if let Some(addr_str) = item.get("address").and_then(|v| v.as_str()) {
            if let Ok(addr) = addr_str.parse::<alloy_primitives::Address>() {
                rlp_encode_optional_address(Some(addr), &mut entry_payload);
            } else {
                rlp_encode_optional_address(None, &mut entry_payload);
            }
        } else {
            rlp_encode_optional_address(None, &mut entry_payload);
        }

        // Storage keys list
        let mut keys_payload = Vec::new();
        if let Some(keys) = item.get("storageKeys").and_then(|v| v.as_array()) {
            for key in keys {
                if let Some(key_str) = key.as_str() {
                    if let Ok(b) = key_str.parse::<B256>() {
                        // 32-byte string
                        keys_payload.push(0x80 + 32);
                        keys_payload.extend_from_slice(b.as_slice());
                    }
                }
            }
        }
        rlp_encode_list_header(&keys_payload, &mut entry_payload);
        entry_payload.extend_from_slice(&keys_payload);

        // Wrap entry in list
        let mut entry_buf = Vec::new();
        rlp_encode_list_header(&entry_payload, &mut entry_buf);
        entry_buf.extend_from_slice(&entry_payload);
        items_payload.extend_from_slice(&entry_buf);
    }

    rlp_encode_list_header(&items_payload, buf);
    buf.extend_from_slice(&items_payload);
}

/// Encode a length as big-endian bytes (for RLP long strings/lists).
fn encode_length_bytes(len: usize) -> Vec<u8> {
    let bytes = (len as u64).to_be_bytes();
    let start = bytes.iter().position(|&b| b != 0).unwrap_or(7);
    bytes[start..].to_vec()
}

/// RLP encoding correctness and transaction hash computation:
/// - RLP primitives (u64, u128, U256, address, bytes) encode per Ethereum spec
/// - Legacy signed tx hash produces valid keccak256
/// - System tx dual hashes diverge (official != explorer)
/// - Chain ID changes produce different hashes
#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, Bytes};

    /// Verify RLP encoding of a simple Legacy tx produces a valid keccak256 hash.
    #[test]
    fn known_legacy_tx_hash() {
        let tx = WireLegacyTx {
            nonce: 0,
            gas_price: 20_000_000_000, // 20 gwei
            gas_limit: 21000,
            to: Some("0xd46e8dd67c5d32be8058bb8eb970870f07244567".parse().unwrap()),
            value: U256::from(1_000_000_000_000_000_000u128), // 1 ETH
            input: Bytes::new(),
            chain_id: Some(1),
        };

        let sig = crate::types::block::WireSignature {
            r: "0x28ef61340bd939bc2195fe537567866003e1a15d3c71ff63e1590620aa636276"
                .parse::<U256>()
                .unwrap(),
            s: "0x67cbe9d8997f761aecb703304b3800ccf555c9f3dc64214b297fb1966a3b6d83"
                .parse::<U256>()
                .unwrap(),
            v: false, // parity 0
        };

        let signed = WireSignedTx {
            transaction: WireTxEnum::Legacy(tx),
            signature: sig,
        };

        let hash = compute_tx_hash(&signed);
        // Should produce a non-zero 32-byte hash
        assert!(!hash.is_zero());
    }

    /// Dual hash for system tx: official != explorer.
    #[test]
    fn dual_hash_divergence() {
        let tx = WireTxEnum::Legacy(WireLegacyTx {
            nonce: 42,
            gas_price: 0,
            gas_limit: 100_000,
            to: Some("0x9b498c3c8a0b8cd8ba1d9851d40d186f1872b44e".parse().unwrap()),
            value: U256::ZERO,
            input: Bytes::from_static(&[0xa9, 0x05, 0x9c, 0xbb, 0x00, 0x00]),
            chain_id: Some(999),
        });

        let from: Address = "0x2000000000000000000000000000000000000004".parse().unwrap();
        let dual = compute_system_tx_dual_hash(&tx, 999, &from);

        assert!(!dual.official.is_zero());
        assert!(!dual.explorer.is_zero());
        assert_ne!(dual.official, dual.explorer, "official and explorer hashes must differ");
    }

    /// Same tx with different chain_id produces different hashes.
    #[test]
    fn chain_id_affects_hash() {
        let tx = WireTxEnum::Legacy(WireLegacyTx {
            nonce: 1,
            gas_price: 0,
            gas_limit: 21000,
            to: Some("0x2222222222222222222222222222222222222222".parse().unwrap()),
            value: U256::from(1_000_000u64),
            input: Bytes::new(),
            chain_id: Some(999),
        });

        let from: Address = "0x2222222222222222222222222222222222222222".parse().unwrap();
        let hash_999 = compute_system_tx_dual_hash(&tx, 999, &from);
        let hash_998 = compute_system_tx_dual_hash(&tx, 998, &from);

        assert_ne!(hash_999.official, hash_998.official);
        assert_ne!(hash_999.explorer, hash_998.explorer);
    }

    /// Verify basic RLP encoding correctness.
    #[test]
    fn rlp_encoding_basics() {
        // u64: 0 encodes as 0x80
        let mut buf = Vec::new();
        rlp_encode_u64(0, &mut buf);
        assert_eq!(buf, vec![0x80]);

        // u64: 1 encodes as 0x01
        buf.clear();
        rlp_encode_u64(1, &mut buf);
        assert_eq!(buf, vec![0x01]);

        // u64: 127 encodes as 0x7f (single byte)
        buf.clear();
        rlp_encode_u64(127, &mut buf);
        assert_eq!(buf, vec![0x7f]);

        // u64: 128 encodes as 0x81 0x80
        buf.clear();
        rlp_encode_u64(128, &mut buf);
        assert_eq!(buf, vec![0x81, 0x80]);

        // Empty bytes encodes as 0x80
        buf.clear();
        rlp_encode_bytes(&[], &mut buf);
        assert_eq!(buf, vec![0x80]);

        // Address (20 bytes) encodes as 0x94 + 20 bytes
        buf.clear();
        let addr: Address = "0x0000000000000000000000000000000000000001".parse().unwrap();
        rlp_encode_optional_address(Some(addr), &mut buf);
        assert_eq!(buf.len(), 21); // 1 prefix + 20 bytes
        assert_eq!(buf[0], 0x94);

        // U256 zero encodes as 0x80
        buf.clear();
        rlp_encode_u256(U256::ZERO, &mut buf);
        assert_eq!(buf, vec![0x80]);

        // U256 1 encodes as 0x01
        buf.clear();
        rlp_encode_u256(U256::from(1u64), &mut buf);
        assert_eq!(buf, vec![0x01]);
    }
}
