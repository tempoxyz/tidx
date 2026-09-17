use std::time::Duration;

pub struct TempoNode {
    pub rpc_url: String,
}

impl TempoNode {
    pub fn from_env() -> Self {
        let rpc_url =
            std::env::var("TEMPO_RPC_URL").unwrap_or_else(|_| "http://localhost:8545".to_string());
        Self { rpc_url }
    }

    pub async fn wait_for_ready(&self) -> anyhow::Result<()> {
        let client = reqwest::Client::new();

        for _ in 0..30 {
            let resp = client
                .post(&self.rpc_url)
                .json(&serde_json::json!({
                    "jsonrpc": "2.0",
                    "method": "eth_blockNumber",
                    "params": [],
                    "id": 1
                }))
                .send()
                .await;

            if resp.is_ok() {
                return Ok(());
            }

            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        anyhow::bail!("Tempo node not ready after 30 seconds")
    }

    pub async fn chain_id(&self) -> anyhow::Result<u64> {
        let client = reqwest::Client::new();
        let resp: serde_json::Value = client
            .post(&self.rpc_url)
            .json(&serde_json::json!({
                "jsonrpc": "2.0",
                "method": "eth_chainId",
                "params": [],
                "id": 1
            }))
            .send()
            .await?
            .json()
            .await?;

        let hex = resp["result"].as_str().unwrap();
        Ok(u64::from_str_radix(hex.trim_start_matches("0x"), 16)?)
    }

    pub async fn block_number(&self) -> anyhow::Result<u64> {
        let client = reqwest::Client::new();
        let resp: serde_json::Value = client
            .post(&self.rpc_url)
            .json(&serde_json::json!({
                "jsonrpc": "2.0",
                "method": "eth_blockNumber",
                "params": [],
                "id": 1
            }))
            .send()
            .await?
            .json()
            .await?;

        let hex = resp["result"].as_str().unwrap();
        Ok(u64::from_str_radix(hex.trim_start_matches("0x"), 16)?)
    }

    pub async fn wait_for_block(&self, target: u64) -> anyhow::Result<()> {
        for _ in 0..60 {
            let current = self.block_number().await?;
            if current >= target {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        anyhow::bail!("Timed out waiting for block {target}")
    }
}
