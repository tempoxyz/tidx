pub use tempo_alloy::primitives::TempoTxEnvelope;
pub use tempo_alloy::rpc::TempoTransactionReceipt;
pub use tempo_alloy::TempoNetwork;

pub type Block = alloy::rpc::types::Block<Transaction>;
pub type Transaction = alloy::rpc::types::Transaction<TempoTxEnvelope>;
pub type Log = alloy::rpc::types::Log;
pub type Receipt = TempoTransactionReceipt;
