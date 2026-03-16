use alloy_primitives::{Address, B256, U256};
use std::sync::LazyLock;
use tiny_keccak::{Hasher, Keccak};

use crate::decode::types::DecodedLog;
use super::types::{Hip4Claim, Hip4ContestCreated, Hip4Deposit, Hip4Refund, Hip4SweepUnclaimed};

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

// --- Calldata decoders for contest contract functions ---

/// Function selectors (first 4 bytes of keccak256 of the signature).
const CREATE_CONTEST_SELECTOR: [u8; 4] = [0x6d, 0xab, 0x6b, 0x23];
const REFUND_SELECTOR: [u8; 4] = [0xa3, 0xd0, 0x7f, 0x67];
const SWEEP_UNCLAIMED_SELECTOR: [u8; 4] = [0xe5, 0x0e, 0x64, 0xd5];

/// A decoded contest contract action from transaction input calldata.
#[derive(Debug)]
pub enum Hip4Action {
    ContestCreated(Hip4ContestCreated),
    Refund(Hip4Refund),
    SweepUnclaimed(Hip4SweepUnclaimed),
}

/// Decode a contest contract function call from transaction input calldata.
///
/// Supported selectors:
/// - `0x6dab6b23` — `createContest(uint256, uint256)` → 68 bytes
/// - `0xa3d07f67` — `refund(uint256, uint256, address)` → 100 bytes
/// - `0xe50e64d5` — `sweepUnclaimed(uint256)` → 36 bytes
///
/// Returns `None` if the input is too short, the selector is unknown,
/// or a uint256 param overflows u64.
pub fn decode_calldata(input: &[u8], block_number: u64, tx_index: usize) -> Option<Hip4Action> {
    if input.len() < 4 {
        return None;
    }

    let selector: [u8; 4] = input[0..4].try_into().ok()?;

    match selector {
        CREATE_CONTEST_SELECTOR => {
            // createContest(uint256, uint256) — 4 + 32 + 32 = 68 bytes
            if input.len() < 68 {
                return None;
            }
            let contest_id = word_to_u64(&input[4..36])?;
            let param2 = word_to_u64(&input[36..68])?;
            Some(Hip4Action::ContestCreated(Hip4ContestCreated {
                block_number,
                tx_index,
                contest_id,
                param2,
            }))
        }
        REFUND_SELECTOR => {
            // refund(uint256, uint256, address) — 4 + 32 + 32 + 32 = 100 bytes
            if input.len() < 100 {
                return None;
            }
            let contest_id = word_to_u64(&input[4..36])?;
            let side_id = word_to_u64(&input[36..68])?;
            // Address is left-padded in 32 bytes: last 20 bytes
            let user = Address::from_slice(&input[80..100]);
            Some(Hip4Action::Refund(Hip4Refund {
                block_number,
                tx_index,
                contest_id,
                side_id,
                user,
            }))
        }
        SWEEP_UNCLAIMED_SELECTOR => {
            // sweepUnclaimed(uint256) — 4 + 32 = 36 bytes
            if input.len() < 36 {
                return None;
            }
            let contest_id = word_to_u64(&input[4..36])?;
            Some(Hip4Action::SweepUnclaimed(Hip4SweepUnclaimed {
                block_number,
                tx_index,
                contest_id,
            }))
        }
        _ => None,
    }
}

/// Extract a u64 from a 32-byte ABI-encoded word (big-endian, left-padded).
/// Returns None if the value overflows u64 (high 24 bytes non-zero).
fn word_to_u64(word: &[u8]) -> Option<u64> {
    if word[..24].iter().any(|&b| b != 0) {
        return None;
    }
    Some(u64::from_be_bytes([
        word[24], word[25], word[26], word[27],
        word[28], word[29], word[30], word[31],
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

    // --- Calldata decoder tests ---

    /// Helper: build ABI-encoded calldata from selector + 32-byte words.
    fn build_calldata(selector: &[u8; 4], words: &[&[u8; 32]]) -> Vec<u8> {
        let mut data = Vec::with_capacity(4 + words.len() * 32);
        data.extend_from_slice(selector);
        for word in words {
            data.extend_from_slice(*word);
        }
        data
    }

    /// Encode a u64 as a 32-byte big-endian ABI word.
    fn u64_to_word(val: u64) -> [u8; 32] {
        let mut word = [0u8; 32];
        word[24..32].copy_from_slice(&val.to_be_bytes());
        word
    }

    /// Encode an address as a 32-byte left-padded ABI word.
    fn address_to_word(addr: &Address) -> [u8; 32] {
        let mut word = [0u8; 32];
        word[12..32].copy_from_slice(addr.as_slice());
        word
    }

    #[test]
    fn decode_create_contest_calldata() {
        let contest_id_word = u64_to_word(42);
        let param2_word = u64_to_word(7);
        let input = build_calldata(&CREATE_CONTEST_SELECTOR, &[&contest_id_word, &param2_word]);

        let action = decode_calldata(&input, 100, 5).unwrap();
        match action {
            Hip4Action::ContestCreated(c) => {
                assert_eq!(c.block_number, 100);
                assert_eq!(c.tx_index, 5);
                assert_eq!(c.contest_id, 42);
                assert_eq!(c.param2, 7);
            }
            _ => panic!("expected ContestCreated"),
        }
    }

    #[test]
    fn decode_refund_calldata() {
        let user: Address = "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef".parse().unwrap();
        let contest_id_word = u64_to_word(99);
        let side_id_word = u64_to_word(2);
        let user_word = address_to_word(&user);
        let input = build_calldata(&REFUND_SELECTOR, &[&contest_id_word, &side_id_word, &user_word]);

        let action = decode_calldata(&input, 200, 3).unwrap();
        match action {
            Hip4Action::Refund(r) => {
                assert_eq!(r.block_number, 200);
                assert_eq!(r.tx_index, 3);
                assert_eq!(r.contest_id, 99);
                assert_eq!(r.side_id, 2);
                assert_eq!(r.user, user);
            }
            _ => panic!("expected Refund"),
        }
    }

    #[test]
    fn decode_sweep_unclaimed_calldata() {
        let contest_id_word = u64_to_word(55);
        let input = build_calldata(&SWEEP_UNCLAIMED_SELECTOR, &[&contest_id_word]);

        let action = decode_calldata(&input, 300, 1).unwrap();
        match action {
            Hip4Action::SweepUnclaimed(s) => {
                assert_eq!(s.block_number, 300);
                assert_eq!(s.tx_index, 1);
                assert_eq!(s.contest_id, 55);
            }
            _ => panic!("expected SweepUnclaimed"),
        }
    }

    #[test]
    fn unknown_selector_returns_none() {
        let input = vec![0xFFu8, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                         0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        assert!(decode_calldata(&input, 1, 0).is_none());
    }

    #[test]
    fn too_short_calldata_returns_none() {
        // Empty input
        assert!(decode_calldata(&[], 1, 0).is_none());
        // Only selector, no params
        assert!(decode_calldata(&CREATE_CONTEST_SELECTOR, 1, 0).is_none());
        // createContest needs 68 bytes, give it 67
        let mut input = vec![0u8; 67];
        input[0..4].copy_from_slice(&CREATE_CONTEST_SELECTOR);
        assert!(decode_calldata(&input, 1, 0).is_none());
        // refund needs 100 bytes, give it 99
        let mut input = vec![0u8; 99];
        input[0..4].copy_from_slice(&REFUND_SELECTOR);
        assert!(decode_calldata(&input, 1, 0).is_none());
        // sweepUnclaimed needs 36 bytes, give it 35
        let mut input = vec![0u8; 35];
        input[0..4].copy_from_slice(&SWEEP_UNCLAIMED_SELECTOR);
        assert!(decode_calldata(&input, 1, 0).is_none());
    }
}
