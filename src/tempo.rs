//! Tempo type aliases.
//!
//! JSON-RPC payloads decode into upstream `TempoHeaderResponse` / `TempoHeader`
//! so the TIP-1031 consensus context, millisecond timestamp, and Tempo-specific
//! gas limits are preserved.

use alloy::network::Network;
use tempo_alloy::TempoNetwork;

pub use tempo_alloy::primitives::TempoTxEnvelope;
pub use tempo_alloy::rpc::TempoTransactionReceipt;

pub type Block = <TempoNetwork as Network>::BlockResponse;
pub type Transaction = <TempoNetwork as Network>::TransactionResponse;
pub type Log = alloy::rpc::types::Log;
pub type Receipt = TempoTransactionReceipt;
