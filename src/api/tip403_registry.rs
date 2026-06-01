use anyhow::Result as AnyhowResult;
use axum::{Json, extract::Query, extract::State};
use chrono::Utc;
use serde::{Deserialize, Serialize, Serializer};

use alloy::sol_types::SolEvent;
use tempo_contracts::precompiles::{ITIP403Registry, TIP403_REGISTRY_ADDRESS};

use crate::db::Pool;

use super::{ApiError, AppState};

#[derive(Deserialize)]
pub struct PolicyDataParams {
    #[serde(alias = "chain_id")]
    #[serde(rename = "chainId")]
    chain_id: u64,
    #[serde(alias = "policy_id")]
    #[serde(rename = "policyId")]
    policy_id: u64,
}

type PolicyType = ITIP403Registry::PolicyType;

fn member_event_selector(policy: &PolicyType) -> Option<Vec<u8>> {
    match policy {
        PolicyType::WHITELIST => Some(event_selector::<ITIP403Registry::WhitelistUpdated>()),
        PolicyType::BLACKLIST => Some(event_selector::<ITIP403Registry::BlacklistUpdated>()),
        _ => None,
    }
}

#[derive(Serialize)]
struct PolicyMetadata {
    chain_id: u64,
    policy_id: u64,
    registry: String,
    #[serde(serialize_with = "serialize_policy_type")]
    policy_type: PolicyType,
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

    let members = if let Some(selector) = member_event_selector(&metadata.policy_type) {
        load_tip403_policy_members(&pool, params.policy_id, &selector)
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

fn event_selector<E: SolEvent>() -> Vec<u8> {
    E::SIGNATURE_HASH.as_slice().to_vec()
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

fn policy_type_from_data(data: &[u8]) -> PolicyType {
    match abi_u8_word(data) {
        0 => PolicyType::WHITELIST,
        1 => PolicyType::BLACKLIST,
        2 => PolicyType::COMPOUND,
        _ => PolicyType::WHITELIST,
    }
}

fn serialize_policy_type<S>(policy_type: &PolicyType, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(match policy_type {
        PolicyType::WHITELIST => "whitelist",
        PolicyType::BLACKLIST => "blacklist",
        PolicyType::COMPOUND => "compound",
        _ => "unknown",
    })
}

async fn load_tip403_policy_metadata(
    pool: &Pool,
    chain_id: u64,
    policy_id: u64,
) -> AnyhowResult<Option<PolicyMetadata>> {
    let conn = pool.get().await?;
    let selector = event_selector::<ITIP403Registry::PolicyCreated>();
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
        registry: TIP403_REGISTRY_ADDRESS.to_string(),
        policy_type: policy_type_from_data(&data),
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
    let selector = event_selector::<ITIP403Registry::PolicyAdminUpdated>();
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
        event_selector::<ITIP403Registry::PolicyCreated>(),
        event_selector::<ITIP403Registry::PolicyAdminUpdated>(),
        event_selector::<ITIP403Registry::WhitelistUpdated>(),
        event_selector::<ITIP403Registry::BlacklistUpdated>(),
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
    selector: &[u8],
) -> AnyhowResult<Vec<String>> {
    let conn = pool.get().await?;
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
