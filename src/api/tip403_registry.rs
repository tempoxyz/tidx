use anyhow::Result as AnyhowResult;
use axum::{Json, extract::Query, extract::State};
use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::db::Pool;
use crate::query::EventSignature;

use super::{ApiError, AppState};

const TIP403_REGISTRY_ADDRESS: [u8; 20] = [
    0x40, 0x3c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00,
];
const TIP403_REGISTRY_ADDRESS_HEX: &str = "0x403c000000000000000000000000000000000000";

#[derive(Deserialize)]
pub struct PolicyDataParams {
    #[serde(alias = "chain_id")]
    #[serde(rename = "chainId")]
    chain_id: u64,
    #[serde(alias = "policy_id")]
    #[serde(rename = "policyId")]
    policy_id: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "lowercase")]
enum Tip403PolicyType {
    Whitelist,
    Blacklist,
    Compound,
    Unknown,
}

impl Tip403PolicyType {
    fn from_u8(value: u8) -> Self {
        match value {
            0 => Self::Whitelist,
            1 => Self::Blacklist,
            2 => Self::Compound,
            _ => Self::Unknown,
        }
    }

    fn member_event_signature(&self) -> Option<&'static str> {
        match self {
            Self::Whitelist => Some(
                "WhitelistUpdated(uint64 indexed policyId,address indexed updater,address indexed account,bool allowed)",
            ),
            Self::Blacklist => Some(
                "BlacklistUpdated(uint64 indexed policyId,address indexed updater,address indexed account,bool restricted)",
            ),
            Self::Compound | Self::Unknown => None,
        }
    }
}

#[derive(Serialize)]
struct PolicyMetadata {
    chain_id: u64,
    policy_id: u64,
    registry: &'static str,
    policy_type: Tip403PolicyType,
    admin: Option<String>,
    created_by: Option<String>,
    created_at: Option<chrono::DateTime<Utc>>,
    last_updated_at: Option<chrono::DateTime<Utc>>,
}

#[derive(Serialize)]
pub struct PolicyDataResponse {
    ok: bool,
    metadata: PolicyMetadata,
    members: Vec<String>,
}

pub async fn get_policy_data(
    State(state): State<AppState>,
    Query(params): Query<PolicyDataParams>,
) -> Result<Json<PolicyDataResponse>, ApiError> {
    let pool = state
        .get_pool(Some(params.chain_id))
        .await
        .ok_or_else(|| ApiError::BadRequest(format!("Unknown chain_id: {}", params.chain_id)))?;

    let metadata = load_tip403_policy_metadata(&pool, params.chain_id, params.policy_id)
        .await
        .map_err(|e| ApiError::QueryError(e.to_string()))?
        .ok_or_else(|| {
            ApiError::NotFound(format!("TIP-403 policy not found: {}", params.policy_id))
        })?;

    let members = if let Some(signature) = metadata.policy_type.member_event_signature() {
        load_tip403_policy_members(&pool, params.policy_id, signature)
            .await
            .map_err(|e| ApiError::QueryError(e.to_string()))?
    } else {
        Vec::new()
    };

    Ok(Json(PolicyDataResponse {
        ok: true,
        metadata,
        members,
    }))
}

fn tip403_topic0(signature: &str) -> AnyhowResult<Vec<u8>> {
    Ok(EventSignature::parse(signature)?.topic0.to_vec())
}

fn tip403_policy_topic(policy_id: u64) -> Vec<u8> {
    let mut topic = vec![0u8; 32];
    topic[24..32].copy_from_slice(&policy_id.to_be_bytes());
    topic
}

fn hex_prefixed(bytes: &[u8]) -> String {
    format!("0x{}", hex::encode(bytes))
}

fn topic_address(topic: &[u8]) -> String {
    if topic.len() >= 32 {
        hex_prefixed(&topic[12..32])
    } else {
        hex_prefixed(topic)
    }
}

fn abi_bool_word(data: &[u8]) -> bool {
    data.get(31).copied().unwrap_or_default() != 0
}

fn abi_u8_word(data: &[u8]) -> u8 {
    data.get(31).copied().unwrap_or_default()
}

async fn load_tip403_policy_metadata(
    pool: &Pool,
    chain_id: u64,
    policy_id: u64,
) -> AnyhowResult<Option<PolicyMetadata>> {
    let conn = pool.get().await?;
    let selector = tip403_topic0(
        "PolicyCreated(uint64 indexed policyId,address indexed updater,uint8 policyType)",
    )?;
    let policy_topic = tip403_policy_topic(policy_id);
    let registry = TIP403_REGISTRY_ADDRESS.to_vec();

    let row = conn
        .query_opt(
            r#"
            SELECT topic2, data, block_timestamp
            FROM logs
            WHERE address = $1 AND selector = $2 AND topic1 = $3
            ORDER BY block_num DESC, tx_idx DESC, log_idx DESC
            LIMIT 1
            "#,
            &[&registry, &selector, &policy_topic],
        )
        .await?;

    let Some(row) = row else { return Ok(None) };
    let updater_topic: Option<Vec<u8>> = row.get(0);
    let data: Vec<u8> = row.get(1);

    Ok(Some(PolicyMetadata {
        chain_id,
        policy_id,
        registry: TIP403_REGISTRY_ADDRESS_HEX,
        policy_type: Tip403PolicyType::from_u8(abi_u8_word(&data)),
        admin: current_tip403_policy_admin(&conn, policy_id)
            .await?
            .or_else(|| updater_topic.as_deref().map(topic_address)),
        created_by: updater_topic.as_deref().map(topic_address),
        created_at: Some(row.get(2)),
        last_updated_at: latest_tip403_policy_update_at(&conn, policy_id).await?,
    }))
}

async fn current_tip403_policy_admin(
    conn: &deadpool_postgres::Object,
    policy_id: u64,
) -> AnyhowResult<Option<String>> {
    let selector = tip403_topic0(
        "PolicyAdminUpdated(uint64 indexed policyId,address indexed updater,address indexed admin)",
    )?;
    let policy_topic = tip403_policy_topic(policy_id);
    let registry = TIP403_REGISTRY_ADDRESS.to_vec();

    let row = conn
        .query_opt(
            r#"
            SELECT topic3
            FROM logs
            WHERE address = $1 AND selector = $2 AND topic1 = $3 AND topic3 IS NOT NULL
            ORDER BY block_num DESC, tx_idx DESC, log_idx DESC
            LIMIT 1
            "#,
            &[&registry, &selector, &policy_topic],
        )
        .await?;

    let Some(row) = row else { return Ok(None) };
    let admin_topic: Vec<u8> = row.get(0);
    Ok(Some(topic_address(&admin_topic)))
}

async fn latest_tip403_policy_update_at(
    conn: &deadpool_postgres::Object,
    policy_id: u64,
) -> AnyhowResult<Option<chrono::DateTime<Utc>>> {
    let selectors = vec![
        tip403_topic0(
            "PolicyCreated(uint64 indexed policyId,address indexed updater,uint8 policyType)",
        )?,
        tip403_topic0(
            "PolicyAdminUpdated(uint64 indexed policyId,address indexed updater,address indexed admin)",
        )?,
        tip403_topic0(
            "WhitelistUpdated(uint64 indexed policyId,address indexed updater,address indexed account,bool allowed)",
        )?,
        tip403_topic0(
            "BlacklistUpdated(uint64 indexed policyId,address indexed updater,address indexed account,bool restricted)",
        )?,
    ];
    let policy_topic = tip403_policy_topic(policy_id);
    let registry = TIP403_REGISTRY_ADDRESS.to_vec();

    let row = conn
        .query_opt(
            r#"
            SELECT block_timestamp
            FROM logs
            WHERE address = $1 AND selector = ANY($2) AND topic1 = $3
            ORDER BY block_num DESC, tx_idx DESC, log_idx DESC
            LIMIT 1
            "#,
            &[&registry, &selectors, &policy_topic],
        )
        .await?;

    Ok(row.map(|row| row.get(0)))
}

async fn load_tip403_policy_members(
    pool: &Pool,
    policy_id: u64,
    signature: &str,
) -> AnyhowResult<Vec<String>> {
    let conn = pool.get().await?;
    let selector = tip403_topic0(signature)?;
    let policy_topic = tip403_policy_topic(policy_id);
    let registry = TIP403_REGISTRY_ADDRESS.to_vec();

    let rows = conn
        .query(
            r#"
            SELECT DISTINCT ON (topic3)
                topic3, data
            FROM logs
            WHERE address = $1 AND selector = $2 AND topic1 = $3 AND topic3 IS NOT NULL
            ORDER BY topic3, block_num DESC, tx_idx DESC, log_idx DESC
            "#,
            &[&registry, &selector, &policy_topic],
        )
        .await?;

    let mut members = Vec::new();
    for row in rows {
        let data: Vec<u8> = row.get(1);
        if !abi_bool_word(&data) {
            continue;
        }

        let account_topic: Vec<u8> = row.get(0);
        members.push(topic_address(&account_topic));
    }

    members.sort();
    Ok(members)
}
