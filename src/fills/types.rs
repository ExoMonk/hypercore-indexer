/// A single trade fill record from the node_fills JSONL data.
///
/// All numeric fields are stored as String — the database handles NUMERIC conversion.
#[derive(Debug, Clone)]
pub struct FillRecord {
    /// Unique trade identifier (tid from JSONL)
    pub trade_id: i64,
    /// L1 block number
    pub block_number: i64,
    /// ISO 8601 block time as-is from JSONL
    pub block_time: String,
    /// Hex address of the user (e.g. "0x010461c1...")
    pub user_address: String,
    /// Coin identifier (e.g. "BTC", "@230", "cash:WTI", "#90")
    pub coin: String,
    /// Price as string (px)
    pub price: String,
    /// Size as string (sz)
    pub size: String,
    /// Side: "B" (buy) or "A" (ask/sell)
    pub side: String,
    /// Direction: "Open Long", "Close Short", "Buy", "Sell", etc.
    pub direction: String,
    /// Closed PnL as string
    pub closed_pnl: String,
    /// L1 transaction hash
    pub hash: String,
    /// Order ID (oid)
    pub order_id: i64,
    /// Whether the order crossed the spread
    pub crossed: bool,
    /// Fee amount as string
    pub fee: String,
    /// Fee token (e.g. "USDC", "USDT0", "+90")
    pub fee_token: String,
    /// Fill timestamp in unix milliseconds
    pub fill_time: i64,
}
