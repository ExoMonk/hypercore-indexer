use alloy_primitives::{Address, U256};

use crate::types::block::SystemTx;

use super::hash::compute_system_tx_dual_hash;
use super::types::{AssetType, DecodedSystemTx};

/// The HYPE native bridge address.
const HYPE_SYSTEM_ADDRESS: Address = Address::new([
    0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22,
    0x22, 0x22, 0x22, 0x22, 0x22,
]);

/// Spot token bridge address prefix: 0x2000000000000000000000000000000000000{assetIndex}
/// The last 2 bytes encode the asset index.
const SPOT_ADDRESS_PREFIX: [u8; 18] = [
    0x20, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00,
];

/// ERC20 `transfer(address,uint256)` function selector.
const TRANSFER_SELECTOR: [u8; 4] = [0xa9, 0x05, 0x9c, 0xbb];

/// Decode a system transaction into an enriched struct with dual hashes and asset identification.
pub fn decode_system_tx(
    stx: &SystemTx,
    chain_id: u64,
) -> eyre::Result<DecodedSystemTx> {
    let to = stx
        .tx
        .to()
        .ok_or_else(|| eyre::eyre!("system tx has no `to` address"))?;

    // Determine the system address and asset type
    let (system_address, asset_type, recipient, amount_wei) = identify_system_tx(stx, to)?;

    // Compute dual hashes — `from` is the system_address for phantom hashing
    let dual = compute_system_tx_dual_hash(&stx.tx, chain_id, &system_address);

    Ok(DecodedSystemTx {
        official_hash: dual.official,
        explorer_hash: dual.explorer,
        system_address,
        asset_type,
        recipient,
        amount_wei,
    })
}

/// Identify the type of system transaction and extract transfer details.
fn identify_system_tx(
    stx: &SystemTx,
    to: Address,
) -> eyre::Result<(Address, AssetType, Address, U256)> {
    let input = stx.tx.input();
    let value = stx.tx.value();

    // System txs don't have a `from` field in the S3 wire format — we infer it.
    //
    // Convention:
    // - If input is empty and value > 0: HYPE native transfer.
    //   system_address = 0x2222..., recipient = `to`, amount = value
    // - If input starts with 0xa9059cbb (transfer selector): spot token transfer.
    //   system_address = `to` (the token contract), recipient + amount decoded from input.
    //   The `to` address may follow the 0x2000...{assetIndex} pattern for known spot
    //   tokens, but this is not guaranteed for all token contracts on Hyperliquid.

    if input.is_empty() && !value.is_zero() {
        // HYPE native transfer
        return Ok((HYPE_SYSTEM_ADDRESS, AssetType::NativeHype, to, value));
    }

    if input.len() >= 4 && input[..4] == TRANSFER_SELECTOR {
        // Spot token ERC20 transfer
        let (recipient, amount) = decode_transfer_input(input)?;

        // Try to extract asset index from the `to` address (0x2000...{assetIndex} pattern).
        // If the address doesn't match the pattern, default to asset_index 0.
        let asset_index = extract_spot_asset_index(to).unwrap_or(0);

        return Ok((
            to, // The token contract IS the system address
            AssetType::SpotToken { asset_index },
            recipient,
            amount,
        ));
    }

    // Unknown system tx pattern — still return something useful
    Err(eyre::eyre!(
        "unrecognized system tx pattern: input_len={}, value={}, to={:#x}",
        input.len(),
        value,
        to
    ))
}

/// Extract the asset index from a spot token system address (0x2000...{2 bytes}).
fn extract_spot_asset_index(addr: Address) -> eyre::Result<u16> {
    let bytes = addr.as_slice();

    // Verify the prefix matches 0x2000...00 (first 18 bytes)
    if bytes[..18] != SPOT_ADDRESS_PREFIX {
        return Err(eyre::eyre!(
            "address {:#x} does not match spot token prefix 0x2000...",
            addr
        ));
    }

    // Last 2 bytes are the asset index (big-endian)
    let asset_index = u16::from_be_bytes([bytes[18], bytes[19]]);
    Ok(asset_index)
}

/// Decode ERC20 `transfer(address,uint256)` call data.
///
/// Layout: 4 bytes selector + 32 bytes address (left-padded) + 32 bytes uint256
fn decode_transfer_input(input: &[u8]) -> eyre::Result<(Address, U256)> {
    if input.len() < 68 {
        return Err(eyre::eyre!(
            "transfer input too short: {} bytes (need 68)",
            input.len()
        ));
    }

    // Skip 4-byte selector
    // Next 32 bytes: address (last 20 bytes of the 32-byte word)
    let recipient = Address::from_slice(&input[16..36]);

    // Next 32 bytes: uint256 amount
    let amount = U256::from_be_slice(&input[36..68]);

    Ok((recipient, amount))
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::Bytes;
    use crate::types::block::{WireLegacyTx, WireTxEnum};

    fn make_system_tx(to: Address, value: U256, input: Bytes) -> SystemTx {
        SystemTx {
            tx: WireTxEnum::Legacy(WireLegacyTx {
                nonce: 0,
                gas_price: 0,
                gas_limit: 100_000,
                to: Some(to),
                value,
                input,
                chain_id: Some(999),
            }),
            receipt: None,
        }
    }

    #[test]
    fn hype_detection() {
        let recipient: Address = "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef".parse().unwrap();
        let stx = make_system_tx(
            recipient,
            U256::from(1_000_000_000_000_000_000u128), // 1 HYPE
            Bytes::new(),
        );

        let decoded = decode_system_tx(&stx, 999).unwrap();
        assert_eq!(decoded.asset_type, AssetType::NativeHype);
        assert_eq!(decoded.system_address, HYPE_SYSTEM_ADDRESS);
        assert_eq!(decoded.recipient, recipient);
        assert_eq!(decoded.amount_wei, U256::from(1_000_000_000_000_000_000u128));
    }

    #[test]
    fn spot_token_detection() {
        // Spot token with asset index 4
        let token_addr: Address = "0x2000000000000000000000000000000000000004".parse().unwrap();
        let recipient: Address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".parse().unwrap();
        let amount = U256::from(500_000u64);

        // Build transfer(address,uint256) calldata
        let mut input = Vec::with_capacity(68);
        input.extend_from_slice(&TRANSFER_SELECTOR);
        // address padded to 32 bytes
        input.extend_from_slice(&[0u8; 12]);
        input.extend_from_slice(recipient.as_slice());
        // uint256 amount
        let amount_bytes = amount.to_be_bytes::<32>();
        input.extend_from_slice(&amount_bytes);

        let stx = make_system_tx(token_addr, U256::ZERO, Bytes::from(input));
        let decoded = decode_system_tx(&stx, 999).unwrap();

        assert_eq!(decoded.asset_type, AssetType::SpotToken { asset_index: 4 });
        assert_eq!(decoded.system_address, token_addr);
        assert_eq!(decoded.recipient, recipient);
        assert_eq!(decoded.amount_wei, amount);
    }

    #[test]
    fn transfer_abi_decode_known_bytes() {
        let recipient: Address = "0x1234567890abcdef1234567890abcdef12345678".parse().unwrap();
        let amount = U256::from(42_000_000u64);

        let mut input = Vec::with_capacity(68);
        input.extend_from_slice(&TRANSFER_SELECTOR);
        input.extend_from_slice(&[0u8; 12]);
        input.extend_from_slice(recipient.as_slice());
        input.extend_from_slice(&amount.to_be_bytes::<32>());

        let (dec_recipient, dec_amount) = decode_transfer_input(&input).unwrap();
        assert_eq!(dec_recipient, recipient);
        assert_eq!(dec_amount, amount);
    }

    #[test]
    fn invalid_short_input_fails() {
        // Too short for transfer decode
        let result = decode_transfer_input(&[0xa9, 0x05, 0x9c, 0xbb, 0x00]);
        assert!(result.is_err());
    }

    #[test]
    fn extract_spot_index_valid() {
        let addr: Address = "0x2000000000000000000000000000000000000004".parse().unwrap();
        assert_eq!(extract_spot_asset_index(addr).unwrap(), 4);

        let addr2: Address = "0x2000000000000000000000000000000000000100".parse().unwrap();
        assert_eq!(extract_spot_asset_index(addr2).unwrap(), 256);
    }

    #[test]
    fn extract_spot_index_invalid_prefix() {
        let addr: Address = "0x3000000000000000000000000000000000000004".parse().unwrap();
        assert!(extract_spot_asset_index(addr).is_err());
    }
}
