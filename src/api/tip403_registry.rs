use anyhow::{Result as AnyhowResult, anyhow};
use axum::{Json, extract::Query, extract::State};
use chrono::Utc;
use serde::{Deserialize, Serialize};

use alloy::{primitives::B256, sol_types::SolEvent};
use tempo_contracts::precompiles::{ITIP403Registry, TIP403_REGISTRY_ADDRESS};

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

#[derive(Serialize)]
struct PolicyMetadata {
    chain_id: u64,
    policy_id: u64,
    registry: String,
    policy_type: PolicyType,
    admin: Option<String>,
    created_by: Option<String>,
    created_at: Option<chrono::DateTime<Utc>>,
    last_updated_at: Option<chrono::DateTime<Utc>>,
}

impl PolicyMetadata {
    fn has_direct_members(&self) -> bool {
        matches!(
            self.policy_type,
            PolicyType::WHITELIST | PolicyType::BLACKLIST
        )
    }
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

    let conn = pool
        .get()
        .await
        .map_err(|e| ApiError::QueryError(e.to_string()))?;

    let metadata = load_tip403_policy_metadata(&conn, params.chain_id, params.policy_id)
        .await
        .map_err(|e| ApiError::QueryError(e.to_string()))?
        .ok_or_else(|| {
            ApiError::NotFound(format!("TIP-403 policy not found: {}", params.policy_id))
        })?;

    let members = if metadata.has_direct_members() {
        load_tip403_policy_members(&conn, params.policy_id, &metadata.policy_type)
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

fn decode_event<E: SolEvent>(topics: &[&[u8]], data: &[u8]) -> AnyhowResult<E> {
    let topics = topics
        .iter()
        .map(|topic| B256::try_from(*topic))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(E::decode_raw_log_validate(topics, data)?)
}

fn tip403_policy_topic(policy_id: u64) -> Vec<u8> {
    let mut topic = vec![0u8; 32];
    topic[24..32].copy_from_slice(&policy_id.to_be_bytes());
    topic
}

async fn load_tip403_policy_metadata(
    conn: &deadpool_postgres::Object,
    chain_id: u64,
    policy_id: u64,
) -> AnyhowResult<Option<PolicyMetadata>> {
    let selector = ITIP403Registry::PolicyCreated::SIGNATURE_HASH.as_slice();
    let policy_topic = tip403_policy_topic(policy_id);
    let registry = TIP403_REGISTRY_ADDRESS.as_slice();

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
    let updater_topic: Vec<u8> = row
        .get::<_, Option<Vec<u8>>>(0)
        .ok_or_else(|| anyhow!("PolicyCreated log missing updater topic"))?;
    let data: Vec<u8> = row.get(1);
    let event = decode_event::<ITIP403Registry::PolicyCreated>(
        &[selector, &policy_topic, &updater_topic],
        &data,
    )?;
    let created_by = event.updater.to_string();

    Ok(Some(PolicyMetadata {
        chain_id,
        policy_id,
        registry: TIP403_REGISTRY_ADDRESS.to_string(),
        policy_type: event.policyType,
        admin: current_tip403_policy_admin(conn, policy_id)
            .await?
            .or_else(|| Some(created_by.clone())),
        created_by: Some(created_by),
        created_at: Some(row.get(2)),
        last_updated_at: latest_tip403_policy_update_at(conn, policy_id).await?,
    }))
}

async fn current_tip403_policy_admin(
    conn: &deadpool_postgres::Object,
    policy_id: u64,
) -> AnyhowResult<Option<String>> {
    let selector = ITIP403Registry::PolicyAdminUpdated::SIGNATURE_HASH.as_slice();
    let policy_topic = tip403_policy_topic(policy_id);
    let registry = TIP403_REGISTRY_ADDRESS.as_slice();

    let row = conn
        .query_opt(
            r#"
            SELECT topic2, topic3
            FROM logs
            WHERE address = $1 AND selector = $2 AND topic1 = $3 AND topic3 IS NOT NULL
            ORDER BY block_num DESC, tx_idx DESC, log_idx DESC
            LIMIT 1
            "#,
            &[&registry, &selector, &policy_topic],
        )
        .await?;

    let Some(row) = row else { return Ok(None) };
    let updater_topic: Vec<u8> = row.get(0);
    let admin_topic: Vec<u8> = row.get(1);
    let event = decode_event::<ITIP403Registry::PolicyAdminUpdated>(
        &[selector, &policy_topic, &updater_topic, &admin_topic],
        &[],
    )?;
    Ok(Some(event.admin.to_string()))
}

async fn latest_tip403_policy_update_at(
    conn: &deadpool_postgres::Object,
    policy_id: u64,
) -> AnyhowResult<Option<chrono::DateTime<Utc>>> {
    let selectors: [&[u8]; 4] = [
        ITIP403Registry::PolicyCreated::SIGNATURE_HASH.as_slice(),
        ITIP403Registry::PolicyAdminUpdated::SIGNATURE_HASH.as_slice(),
        ITIP403Registry::WhitelistUpdated::SIGNATURE_HASH.as_slice(),
        ITIP403Registry::BlacklistUpdated::SIGNATURE_HASH.as_slice(),
    ];
    let policy_topic = tip403_policy_topic(policy_id);
    let registry = TIP403_REGISTRY_ADDRESS.as_slice();

    let row = conn
        .query_opt(
            r#"
            SELECT block_timestamp
            FROM logs
            WHERE address = $1 AND selector = ANY($2) AND topic1 = $3
            ORDER BY block_num DESC, tx_idx DESC, log_idx DESC
            LIMIT 1
            "#,
            &[&registry, &&selectors[..], &policy_topic],
        )
        .await?;

    Ok(row.map(|row| row.get(0)))
}

async fn load_tip403_policy_members(
    conn: &deadpool_postgres::Object,
    policy_id: u64,
    policy_type: &PolicyType,
) -> AnyhowResult<Vec<String>> {
    let selector = match policy_type {
        PolicyType::WHITELIST => ITIP403Registry::WhitelistUpdated::SIGNATURE_HASH.as_slice(),
        PolicyType::BLACKLIST => ITIP403Registry::BlacklistUpdated::SIGNATURE_HASH.as_slice(),
        _ => return Ok(Vec::new()),
    };
    let policy_topic = tip403_policy_topic(policy_id);
    let registry = TIP403_REGISTRY_ADDRESS.as_slice();

    let rows = conn
        .query(
            r#"
            SELECT DISTINCT ON (topic3)
                topic2, topic3, data
            FROM logs
            WHERE address = $1 AND selector = $2 AND topic1 = $3 AND topic3 IS NOT NULL
            ORDER BY topic3, block_num DESC, tx_idx DESC, log_idx DESC
            "#,
            &[&registry, &selector, &policy_topic],
        )
        .await?;

    let mut members = Vec::new();
    for row in rows {
        let updater_topic: Vec<u8> = row.get(0);
        let account_topic: Vec<u8> = row.get(1);
        let data: Vec<u8> = row.get(2);

        let active_member = match policy_type {
            PolicyType::WHITELIST => {
                let event = decode_event::<ITIP403Registry::WhitelistUpdated>(
                    &[selector, &policy_topic, &updater_topic, &account_topic],
                    &data,
                )?;
                event.allowed.then(|| event.account.to_string())
            }
            PolicyType::BLACKLIST => {
                let event = decode_event::<ITIP403Registry::BlacklistUpdated>(
                    &[selector, &policy_topic, &updater_topic, &account_topic],
                    &data,
                )?;
                event.restricted.then(|| event.account.to_string())
            }
            _ => None,
        };

        if let Some(account) = active_member {
            members.push(account);
        }
    }

    Ok(members)
}
