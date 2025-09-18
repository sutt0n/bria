use bdk::blockchain::Blockchain;
use serde::{Deserialize, Serialize};
use tracing::instrument;

use std::collections::HashMap;

use super::error::JobError;
use crate::{
    app::BlockchainConfig, batch::*, bdk::error::BdkError,
    electrum_client_pool::ElectrumClientPool, primitives::*,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchBroadcastingData {
    pub(super) account_id: AccountId,
    pub(super) batch_id: BatchId,
    #[serde(flatten)]
    pub(super) tracing_data: HashMap<String, String>,
}

#[instrument(
    name = "job.batch_broadcasting",
    skip(batches, electrum_pool),
    fields(txid, broadcast = false),
    err
)]
pub async fn execute(
    data: BatchBroadcastingData,
    _blockchain_cfg: BlockchainConfig,
    electrum_pool: ElectrumClientPool,
    batches: Batches,
) -> Result<BatchBroadcastingData, JobError> {
    let conn = electrum_pool.acquire().await?;
    let blockchain = conn.blockchain();
    let batch = batches.find_by_id(data.account_id, data.batch_id).await?;
    let span = tracing::Span::current();
    span.record("txid", tracing::field::display(batch.bitcoin_tx_id));
    if batch.accounting_complete() {
        if let Some(tx) = batch.signed_tx {
            blockchain.broadcast(&tx).map_err(BdkError::BdkLibError)?;
            span.record("broadcast", true);
        }
    }
    Ok(data)
}
