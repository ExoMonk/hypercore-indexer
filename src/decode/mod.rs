pub mod hash;
pub mod system_tx;
pub mod types;

use crate::types::block::BlockAndReceipts;
use types::{DecodedBlock, DecodedLog, DecodedTx, TxType};

/// Decode a raw `BlockAndReceipts` into a fully enriched `DecodedBlock`.
///
/// This function:
/// 1. Computes hashes for all regular transactions
/// 2. Computes dual hashes for all system transactions
/// 3. Decodes system tx input data (HYPE vs spot token transfer)
/// 4. Pairs transactions with receipts (by index)
/// 5. Computes per-tx gas used from cumulative gas in receipts
pub fn decode_block(raw: &BlockAndReceipts, chain_id: u64) -> eyre::Result<DecodedBlock> {
    let block = raw.block.inner();
    let header = &block.header.header;

    // Decode regular transactions
    let mut transactions = Vec::with_capacity(block.body.transactions.len());
    let mut global_log_index = 0usize;

    for (i, signed_tx) in block.body.transactions.iter().enumerate() {
        let tx_hash = hash::compute_tx_hash(signed_tx);

        // Get the receipt for this tx
        let receipt = raw
            .receipts
            .get(i)
            .ok_or_else(|| eyre::eyre!("missing receipt for tx index {i}"))?;

        // Compute per-tx gas: difference from previous cumulative
        let prev_cumulative = if i == 0 {
            0
        } else {
            raw.receipts[i - 1].cumulative_gas_used
        };
        let gas_used = receipt.cumulative_gas_used.saturating_sub(prev_cumulative);

        // Decode logs
        let mut logs = Vec::with_capacity(receipt.logs.len());
        for log in &receipt.logs {
            logs.push(DecodedLog {
                log_index: global_log_index,
                address: log.address,
                topics: log.data.topics.clone(),
                data: log.data.data.clone(),
            });
            global_log_index += 1;
        }

        // Determine tx type
        let tx_type = match &signed_tx.transaction {
            crate::types::block::WireTxEnum::Legacy(_) => TxType::Legacy,
            crate::types::block::WireTxEnum::Eip2930(_) => TxType::Eip2930,
            crate::types::block::WireTxEnum::Eip1559(_) => TxType::Eip1559,
        };

        transactions.push(DecodedTx {
            hash: tx_hash,
            tx_index: i,
            tx_type,
            from: None, // Signer recovery deferred to future milestone
            to: signed_tx.transaction.to(),
            value: signed_tx.transaction.value(),
            input: signed_tx.transaction.input().clone(),
            gas_limit: signed_tx.transaction.gas_limit(),
            success: receipt.success,
            gas_used,
            logs,
        });
    }

    // Decode system transactions
    let mut system_transfers = Vec::with_capacity(raw.system_txs.len());
    for stx in &raw.system_txs {
        match system_tx::decode_system_tx(stx, chain_id) {
            Ok(decoded) => system_transfers.push(decoded),
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "failed to decode system tx, skipping"
                );
            }
        }
    }

    Ok(DecodedBlock {
        number: header.number,
        hash: block.header.hash,
        parent_hash: header.parent_hash,
        timestamp: header.timestamp,
        gas_used: header.gas_used,
        gas_limit: header.gas_limit,
        base_fee_per_gas: header.base_fee_per_gas,
        transactions,
        system_transfers,
    })
}
