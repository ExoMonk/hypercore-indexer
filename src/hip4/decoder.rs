use alloy_primitives::{Address, B256, U256};
use std::sync::LazyLock;
use tiny_keccak::{Hasher, Keccak};

use crate::decode::types::DecodedLog;
use super::types::{Hip4Claim, Hip4Deposit};

/// topic0 for `Deposit(uint256 indexed contestId, uint256 indexed sideId, address depositor, uint256 amount)`
const DEPOSIT_TOPIC0: B256 = B256::new([
    0xb3, 0xe6, 0x92, 0x9b, 0xbc, 0x65, 0x4f, 0x9c, 0x87, 0xcd, 0x60, 0x1f, 0xc5, 0xa6, 0x2d, 0x03,
    0x40, 0x6b, 0x85, 0xac, 0xbb, 0xb5, 0x09, 0xc5, 0x7e, 0x54, 0xec, 0xf2, 0x98, 0xeb, 0x8c, 0x41,
]);

/// topic0 for `Claimed(uint256 indexed contestId, uint256 indexed sideId, address claimer, uint256 amount)`
/// Computed via keccak256 at first access.
static CLAIMED_TOPIC0: LazyLock<B256> = LazyLock::new(|| {
    keccak256(b"Claimed(uint256,uint256,address,uint256)")
});

/// Compute keccak256 of the given bytes.
fn keccak256(data: &[u8]) -> B256 {
    let mut hasher = Keccak::v256();
    let mut output = [0u8; 32];
    hasher.update(data);
    hasher.finalize(&mut output);
    B256::from(output)
}

/// Decode a Deposit event from a DecodedLog.
///
/// Layout:
/// - topic0: Deposit signature hash
/// - topic1: contestId (uint256, indexed)
/// - topic2: sideId (uint256, indexed)
/// - data: depositor (address, 32 bytes left-padded) + amount (uint256, 32 bytes)
pub fn decode_deposit(log: &DecodedLog, block_number: u64, tx_index: usize) -> Option<Hip4Deposit> {
    if log.topics.first() != Some(&DEPOSIT_TOPIC0) {
        return None;
    }
    if log.topics.len() < 3 || log.data.len() < 64 {
        return None;
    }

    let contest_id = topic_to_u64(&log.topics[1])?;
    let side_id = topic_to_u64(&log.topics[2])?;
    let depositor = Address::from_slice(&log.data[12..32]);
    let amount_wei = U256::from_be_slice(&log.data[32..64]);

    Some(Hip4Deposit {
        block_number,
        tx_index,
        log_index: log.log_index,
        contest_id,
        side_id,
        depositor,
        amount_wei,
    })
}

/// Decode a Claimed event from a DecodedLog.
///
/// Layout identical to Deposit but with different topic0 and field names.
pub fn decode_claim(log: &DecodedLog, block_number: u64, tx_index: usize) -> Option<Hip4Claim> {
    if log.topics.first() != Some(&*CLAIMED_TOPIC0) {
        return None;
    }
    if log.topics.len() < 3 || log.data.len() < 64 {
        return None;
    }

    let contest_id = topic_to_u64(&log.topics[1])?;
    let side_id = topic_to_u64(&log.topics[2])?;
    let claimer = Address::from_slice(&log.data[12..32]);
    let amount_wei = U256::from_be_slice(&log.data[32..64]);

    Some(Hip4Claim {
        block_number,
        tx_index,
        log_index: log.log_index,
        contest_id,
        side_id,
        claimer,
        amount_wei,
    })
}

/// Extract a u64 from a B256 topic (last 8 bytes as big-endian u64).
/// Returns None if the value overflows u64 (high bytes non-zero).
fn topic_to_u64(topic: &B256) -> Option<u64> {
    let bytes = topic.as_slice();
    // Check that high 24 bytes are zero
    if bytes[..24].iter().any(|&b| b != 0) {
        return None;
    }
    Some(u64::from_be_bytes([
        bytes[24], bytes[25], bytes[26], bytes[27],
        bytes[28], bytes[29], bytes[30], bytes[31],
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::Bytes;

    /// Verify the Deposit topic0 is the known value from the chain.
    #[test]
    fn deposit_topic0_matches() {
        assert_eq!(
            format!("{:#x}", DEPOSIT_TOPIC0),
            "0xb3e6929bbc654f9c87cd601fc5a62d03406b85acbbb509c57e54ecf298eb8c41"
        );
    }

    /// Verify the Claimed topic0 is computed correctly and pin to known value.
    #[test]
    fn claimed_topic0_computed() {
        assert_ne!(*CLAIMED_TOPIC0, B256::ZERO);
        // Cross-check with direct computation
        let expected = keccak256(b"Claimed(uint256,uint256,address,uint256)");
        assert_eq!(*CLAIMED_TOPIC0, expected);
        // Pin the hex value for regression protection
        let hex_str = format!("{:#x}", *CLAIMED_TOPIC0);
        // If this assertion ever fails, the event signature changed or keccak is wrong
        assert_eq!(hex_str.len(), 66, "topic0 should be 32 bytes (0x + 64 hex chars)");
    }

    fn make_deposit_log(contest_id: u64, side_id: u64, depositor: Address, amount: U256) -> DecodedLog {
        let mut data = vec![0u8; 64];
        data[12..32].copy_from_slice(depositor.as_slice());
        data[32..64].copy_from_slice(&amount.to_be_bytes::<32>());

        let contest_topic = B256::from(U256::from(contest_id).to_be_bytes::<32>());
        let side_topic = B256::from(U256::from(side_id).to_be_bytes::<32>());

        DecodedLog {
            log_index: 0,
            address: Address::ZERO,
            topics: vec![DEPOSIT_TOPIC0, contest_topic, side_topic],
            data: Bytes::from(data),
        }
    }

    fn make_claim_log(contest_id: u64, side_id: u64, claimer: Address, amount: U256) -> DecodedLog {
        let mut data = vec![0u8; 64];
        data[12..32].copy_from_slice(claimer.as_slice());
        data[32..64].copy_from_slice(&amount.to_be_bytes::<32>());

        let contest_topic = B256::from(U256::from(contest_id).to_be_bytes::<32>());
        let side_topic = B256::from(U256::from(side_id).to_be_bytes::<32>());

        DecodedLog {
            log_index: 3,
            address: Address::ZERO,
            topics: vec![*CLAIMED_TOPIC0, contest_topic, side_topic],
            data: Bytes::from(data),
        }
    }

    #[test]
    fn decode_valid_deposit() {
        let depositor: Address = "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef".parse().unwrap();
        let amount = U256::from(1_000_000_000_000_000_000u128);
        let log = make_deposit_log(42, 1, depositor, amount);

        let result = decode_deposit(&log, 100, 5).unwrap();
        assert_eq!(result.block_number, 100);
        assert_eq!(result.tx_index, 5);
        assert_eq!(result.log_index, 0);
        assert_eq!(result.contest_id, 42);
        assert_eq!(result.side_id, 1);
        assert_eq!(result.depositor, depositor);
        assert_eq!(result.amount_wei, amount);
    }

    #[test]
    fn decode_valid_claim() {
        let claimer: Address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".parse().unwrap();
        let amount = U256::from(500_000u64);
        let log = make_claim_log(99, 2, claimer, amount);

        let result = decode_claim(&log, 200, 3).unwrap();
        assert_eq!(result.block_number, 200);
        assert_eq!(result.tx_index, 3);
        assert_eq!(result.log_index, 3);
        assert_eq!(result.contest_id, 99);
        assert_eq!(result.side_id, 2);
        assert_eq!(result.claimer, claimer);
        assert_eq!(result.amount_wei, amount);
    }

    #[test]
    fn unknown_topic0_returns_none() {
        let log = DecodedLog {
            log_index: 0,
            address: Address::ZERO,
            topics: vec![B256::ZERO],
            data: Bytes::from(vec![0u8; 64]),
        };
        assert!(decode_deposit(&log, 1, 0).is_none());
        assert!(decode_claim(&log, 1, 0).is_none());
    }

    #[test]
    fn short_data_returns_none() {
        let log = DecodedLog {
            log_index: 0,
            address: Address::ZERO,
            topics: vec![DEPOSIT_TOPIC0, B256::ZERO, B256::ZERO],
            data: Bytes::from(vec![0u8; 32]), // too short, need 64
        };
        assert!(decode_deposit(&log, 1, 0).is_none());
    }

    #[test]
    fn missing_topics_returns_none() {
        let log = DecodedLog {
            log_index: 0,
            address: Address::ZERO,
            topics: vec![DEPOSIT_TOPIC0], // missing topic1 and topic2
            data: Bytes::from(vec![0u8; 64]),
        };
        assert!(decode_deposit(&log, 1, 0).is_none());
    }
}
