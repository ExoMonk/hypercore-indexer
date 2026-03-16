use alloy_primitives::{Address, U256};

/// A decoded HIP4 Deposit event from the contest contract.
#[derive(Debug)]
pub struct Hip4Deposit {
    pub block_number: u64,
    pub tx_index: usize,
    pub log_index: usize,
    pub contest_id: u64,
    pub side_id: u64,
    pub depositor: Address,
    pub amount_wei: U256,
}

/// A decoded HIP4 Claimed event from the contest contract.
#[derive(Debug)]
pub struct Hip4Claim {
    pub block_number: u64,
    pub tx_index: usize,
    pub log_index: usize,
    pub contest_id: u64,
    pub side_id: u64,
    pub claimer: Address,
    pub amount_wei: U256,
}

/// A decoded createContest call from transaction calldata.
#[derive(Debug)]
pub struct Hip4ContestCreated {
    pub block_number: u64,
    pub tx_index: usize,
    pub contest_id: u64,
    pub param2: u64,
}

/// A decoded refund call from transaction calldata.
#[derive(Debug)]
pub struct Hip4Refund {
    pub block_number: u64,
    pub tx_index: usize,
    pub contest_id: u64,
    pub side_id: u64,
    pub user: Address,
}

/// A decoded sweepUnclaimed call from transaction calldata.
#[derive(Debug)]
pub struct Hip4SweepUnclaimed {
    pub block_number: u64,
    pub tx_index: usize,
    pub contest_id: u64,
}

/// Aggregated HIP4 data extracted from a single block.
#[derive(Debug, Default)]
pub struct Hip4BlockData {
    pub deposits: Vec<Hip4Deposit>,
    pub claims: Vec<Hip4Claim>,
    pub contest_creations: Vec<Hip4ContestCreated>,
    pub refunds: Vec<Hip4Refund>,
    pub sweeps: Vec<Hip4SweepUnclaimed>,
}

// --- Phase 2: API poller types ---

/// A market entry for storage (flattened from outcomeMeta + questions).
#[derive(Debug, Clone)]
pub struct Hip4Market {
    pub outcome_id: u64,
    pub name: String,
    pub description: String,
    /// JSON string of side_specs array, e.g. `[{"name":"Yes"},{"name":"No"}]`
    pub side_specs: String,
    pub question_id: Option<u64>,
    pub question_name: Option<String>,
}

/// A price snapshot row for storage.
#[derive(Debug, Clone)]
pub struct Hip4PriceRow {
    pub coin: String,
    pub mid_price: String,
    /// Unix timestamp in milliseconds.
    pub timestamp_ms: i64,
}
