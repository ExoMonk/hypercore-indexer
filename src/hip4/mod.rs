pub mod api;
pub mod decoder;
pub mod poller;
pub mod types;

use alloy_primitives::Address;

use crate::config::Hip4Config;
use crate::decode::types::DecodedBlock;
use types::Hip4BlockData;

/// Process a decoded block and extract HIP4 contest events (deposits and claims).
///
/// Scans all transaction logs from the configured contest address for known events.
/// Returns empty data if HIP4 is disabled or no contest address is configured.
pub fn process_block(block: &DecodedBlock, config: &Hip4Config) -> Hip4BlockData {
    if !config.enabled {
        return Hip4BlockData::default();
    }

    let contest_addr = match &config.contest_address {
        Some(addr) => match addr.parse::<Address>() {
            Ok(a) => a,
            Err(_) => {
                tracing::warn!(address = %addr, "invalid contest address in config, skipping HIP4");
                return Hip4BlockData::default();
            }
        },
        None => return Hip4BlockData::default(),
    };

    let mut data = Hip4BlockData::default();

    for tx in &block.transactions {
        // Reverted transactions have empty logs in EVM receipts, but guard explicitly
        // to prevent storing events from failed txs if upstream data is ever malformed.
        if !tx.success {
            continue;
        }
        for log in &tx.logs {
            // Only process logs emitted by the contest contract
            if log.address != contest_addr {
                continue;
            }

            if let Some(deposit) = decoder::decode_deposit(log, block.number, tx.tx_index) {
                data.deposits.push(deposit);
            } else if let Some(claim) = decoder::decode_claim(log, block.number, tx.tx_index) {
                data.claims.push(claim);
            }
        }
    }

    data
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decode::types::{DecodedLog, DecodedTx, TxType};
    use alloy_primitives::{Address, B256, Bytes, U256};

    fn empty_block() -> DecodedBlock {
        DecodedBlock {
            number: 100,
            hash: B256::ZERO,
            parent_hash: B256::ZERO,
            timestamp: 1000,
            gas_used: 0,
            gas_limit: 2_000_000,
            base_fee_per_gas: None,
            transactions: vec![],
            system_transfers: vec![],
        }
    }

    #[test]
    fn disabled_returns_empty() {
        let block = empty_block();
        let config = Hip4Config {
            enabled: false,
            contest_address: Some("0x4fd772e5708da2a7f097f51b3127e515a72744bd".to_string()),
            ..Default::default()
        };
        let result = process_block(&block, &config);
        assert!(result.deposits.is_empty());
        assert!(result.claims.is_empty());
    }

    #[test]
    fn no_address_returns_empty() {
        let block = empty_block();
        let config = Hip4Config {
            enabled: true,
            contest_address: None,
            ..Default::default()
        };
        let result = process_block(&block, &config);
        assert!(result.deposits.is_empty());
        assert!(result.claims.is_empty());
    }

    #[test]
    fn block_with_deposit_extracts_it() {
        let contest_addr: Address = "0x4fd772e5708da2a7f097f51b3127e515a72744bd".parse().unwrap();
        let depositor: Address = "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef".parse().unwrap();
        let amount = U256::from(1_000_000u64);

        // Build Deposit event data
        let mut event_data = vec![0u8; 64];
        event_data[12..32].copy_from_slice(depositor.as_slice());
        event_data[32..64].copy_from_slice(&amount.to_be_bytes::<32>());

        // Deposit topic0
        let deposit_topic0 = B256::new([
            0xb3, 0xe6, 0x92, 0x9b, 0xbc, 0x65, 0x4f, 0x9c,
            0x87, 0xcd, 0x60, 0x1f, 0xc5, 0xa6, 0x2d, 0x03,
            0x40, 0x6b, 0x85, 0xac, 0xbb, 0xb5, 0x09, 0xc5,
            0x7e, 0x54, 0xec, 0xf2, 0x98, 0xeb, 0x8c, 0x41,
        ]);

        let contest_topic = B256::from(U256::from(42u64).to_be_bytes::<32>());
        let side_topic = B256::from(U256::from(1u64).to_be_bytes::<32>());

        let log = DecodedLog {
            log_index: 0,
            address: contest_addr,
            topics: vec![deposit_topic0, contest_topic, side_topic],
            data: Bytes::from(event_data),
        };

        let tx = DecodedTx {
            hash: B256::ZERO,
            tx_index: 0,
            tx_type: TxType::Eip1559,
            from: None,
            to: Some(contest_addr),
            value: U256::ZERO,
            input: Bytes::new(),
            gas_limit: 100_000,
            success: true,
            gas_used: 50_000,
            logs: vec![log],
        };

        let mut block = empty_block();
        block.transactions.push(tx);

        let config = Hip4Config {
            enabled: true,
            contest_address: Some("0x4fd772e5708da2a7f097f51b3127e515a72744bd".to_string()),
            ..Default::default()
        };

        let result = process_block(&block, &config);
        assert_eq!(result.deposits.len(), 1);
        assert!(result.claims.is_empty());
        assert_eq!(result.deposits[0].contest_id, 42);
        assert_eq!(result.deposits[0].side_id, 1);
        assert_eq!(result.deposits[0].depositor, depositor);
        assert_eq!(result.deposits[0].amount_wei, amount);
    }

    #[test]
    fn reverted_tx_deposit_is_ignored() {
        let contest_addr: Address = "0x4fd772e5708da2a7f097f51b3127e515a72744bd".parse().unwrap();
        let depositor: Address = "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef".parse().unwrap();
        let amount = U256::from(1_000_000u64);

        // Build Deposit event data
        let mut event_data = vec![0u8; 64];
        event_data[12..32].copy_from_slice(depositor.as_slice());
        event_data[32..64].copy_from_slice(&amount.to_be_bytes::<32>());

        let deposit_topic0 = B256::new([
            0xb3, 0xe6, 0x92, 0x9b, 0xbc, 0x65, 0x4f, 0x9c,
            0x87, 0xcd, 0x60, 0x1f, 0xc5, 0xa6, 0x2d, 0x03,
            0x40, 0x6b, 0x85, 0xac, 0xbb, 0xb5, 0x09, 0xc5,
            0x7e, 0x54, 0xec, 0xf2, 0x98, 0xeb, 0x8c, 0x41,
        ]);

        let contest_topic = B256::from(U256::from(42u64).to_be_bytes::<32>());
        let side_topic = B256::from(U256::from(1u64).to_be_bytes::<32>());

        let log = DecodedLog {
            log_index: 0,
            address: contest_addr,
            topics: vec![deposit_topic0, contest_topic, side_topic],
            data: Bytes::from(event_data),
        };

        // Transaction REVERTED (success = false) — deposit should be ignored
        let tx = DecodedTx {
            hash: B256::ZERO,
            tx_index: 0,
            tx_type: TxType::Eip1559,
            from: None,
            to: Some(contest_addr),
            value: U256::ZERO,
            input: Bytes::new(),
            gas_limit: 100_000,
            success: false,
            gas_used: 50_000,
            logs: vec![log],
        };

        let mut block = empty_block();
        block.transactions.push(tx);

        let config = Hip4Config {
            enabled: true,
            contest_address: Some("0x4fd772e5708da2a7f097f51b3127e515a72744bd".to_string()),
            ..Default::default()
        };

        let result = process_block(&block, &config);
        assert!(result.deposits.is_empty(), "reverted tx should not produce HIP4 deposits");
        assert!(result.claims.is_empty(), "reverted tx should not produce HIP4 claims");
    }
}
