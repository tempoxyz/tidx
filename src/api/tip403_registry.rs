use anyhow::{Result as AnyhowResult, anyhow};
use axum::{Json, extract::Query, extract::State};
use chrono::Utc;
use serde::{Deserialize, Serialize};

use alloy::{primitives::B256, sol_types::SolEvent};
use tempo_contracts::precompiles::{ITIP403Registry, TIP403_REGISTRY_ADDRESS};

use super::{ApiError, AppState};

const DEFAULT_MEMBER_LIMIT: i64 = 100;
const MAX_MEMBER_LIMIT: i64 = 500;

#[derive(Deserialize)]
pub struct PolicyDataParams {
    #[serde(alias = "chain_id")]
    #[serde(rename = "chainId")]
    chain_id: u64,
    #[serde(alias = "policy_id")]
    #[serde(rename = "policyId")]
    policy_id: u64,
    cursor: Option<String>,
    limit: Option<i64>,
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

#[derive(Serialize)]
struct CompoundPolicyRefs {
    sender_policy_id: u64,
    recipient_policy_id: u64,
    mint_recipient_policy_id: u64,
}

#[derive(Serialize)]
struct ChildPolicyData {
    metadata: PolicyMetadata,
    members: Vec<String>,
    next_member_cursor: Option<String>,
}

#[derive(Serialize)]
struct CompoundPolicyData {
    sender_policy: ChildPolicyData,
    recipient_policy: ChildPolicyData,
    mint_recipient_policy: ChildPolicyData,
}

struct LoadedPolicyData {
    metadata: PolicyMetadata,
    compound_policy_refs: Option<CompoundPolicyRefs>,
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
    compound_policy: Option<CompoundPolicyData>,
    members: Vec<String>,
    member_limit: i64,
    next_member_cursor: Option<String>,
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

    let policy_data = load_tip403_policy_metadata(&conn, params.chain_id, params.policy_id)
        .await
        .map_err(|e| ApiError::QueryError(e.to_string()))?
        .ok_or_else(|| {
            ApiError::NotFound(format!("TIP-403 policy not found: {}", params.policy_id))
        })?;

    let member_limit = params
        .limit
        .unwrap_or(DEFAULT_MEMBER_LIMIT)
        .clamp(1, MAX_MEMBER_LIMIT);
    let member_cursor = params
        .cursor
        .as_deref()
        .map(tip403_member_cursor_topic)
        .transpose()
        .map_err(|e| ApiError::BadRequest(e.to_string()))?;

    let (members, next_member_cursor) = if policy_data.metadata.has_direct_members() {
        load_tip403_policy_members(
            &conn,
            params.policy_id,
            &policy_data.metadata.policy_type,
            member_limit,
            member_cursor.as_deref(),
        )
        .await
        .map_err(|e| ApiError::QueryError(e.to_string()))?
    } else {
        (Vec::new(), None)
    };

    let compound_policy = if let Some(refs) = policy_data.compound_policy_refs {
        Some(
            resolve_tip403_compound_policy(&conn, params.chain_id, refs, member_limit)
                .await
                .map_err(|e| ApiError::QueryError(e.to_string()))?,
        )
    } else {
        None
    };

    Ok(Json(PolicyDataResponse {
        ok: true,
        metadata: policy_data.metadata,
        compound_policy,
        members,
        member_limit,
        next_member_cursor,
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

fn tip403_registry_address() -> Vec<u8> {
    TIP403_REGISTRY_ADDRESS.as_slice().to_vec()
}

fn tip403_member_cursor_topic(cursor: &str) -> AnyhowResult<Vec<u8>> {
    let cursor = cursor.strip_prefix("0x").unwrap_or(cursor);
    if cursor.len() != 40 {
        return Err(anyhow!("cursor must be a 20-byte hex address"));
    }

    let mut topic = vec![0u8; 32];
    hex::decode_to_slice(cursor, &mut topic[12..32])?;
    Ok(topic)
}

async fn load_tip403_policy_metadata(
    conn: &deadpool_postgres::Object,
    chain_id: u64,
    policy_id: u64,
) -> AnyhowResult<Option<LoadedPolicyData>> {
    let policy_created_selector = ITIP403Registry::PolicyCreated::SIGNATURE_HASH.as_slice();
    let compound_created_selector =
        ITIP403Registry::CompoundPolicyCreated::SIGNATURE_HASH.as_slice();
    let selectors: [&[u8]; 2] = [policy_created_selector, compound_created_selector];
    let registry_address = tip403_registry_address();
    let policy_topic = tip403_policy_topic(policy_id);

    let row = conn
        .query_opt(
            r#"
            SELECT selector, topic2, data, block_timestamp
            FROM logs
            WHERE address = $1
              AND selector = ANY($2) AND topic1 = $3
            ORDER BY block_num DESC, tx_idx DESC, log_idx DESC
            LIMIT 1
            "#,
            &[&registry_address, &&selectors[..], &policy_topic],
        )
        .await?;

    let Some(row) = row else { return Ok(None) };
    let selector: Vec<u8> = row.get(0);
    let updater_topic: Vec<u8> = row
        .get::<_, Option<Vec<u8>>>(1)
        .ok_or_else(|| anyhow!("TIP-403 creation log missing updater topic"))?;
    let data: Vec<u8> = row.get(2);
    let (policy_type, created_by, default_admin, compound_policy_refs) =
        if selector == policy_created_selector {
            let event = decode_event::<ITIP403Registry::PolicyCreated>(
                &[policy_created_selector, &policy_topic, &updater_topic],
                &data,
            )?;
            let created_by = event.updater.to_string();
            (event.policyType, created_by.clone(), Some(created_by), None)
        } else if selector == compound_created_selector {
            let event = decode_event::<ITIP403Registry::CompoundPolicyCreated>(
                &[compound_created_selector, &policy_topic, &updater_topic],
                &data,
            )?;
            (
                PolicyType::COMPOUND,
                event.creator.to_string(),
                None,
                Some(CompoundPolicyRefs {
                    sender_policy_id: event.senderPolicyId,
                    recipient_policy_id: event.recipientPolicyId,
                    mint_recipient_policy_id: event.mintRecipientPolicyId,
                }),
            )
        } else {
            return Err(anyhow!("unknown TIP-403 creation selector"));
        };

    Ok(Some(LoadedPolicyData {
        metadata: PolicyMetadata {
            chain_id,
            policy_id,
            registry: TIP403_REGISTRY_ADDRESS.to_string(),
            policy_type,
            admin: current_tip403_policy_admin(conn, policy_id)
                .await?
                .or(default_admin),
            created_by: Some(created_by),
            created_at: Some(row.get(3)),
            last_updated_at: latest_tip403_policy_update_at(conn, policy_id).await?,
        },
        compound_policy_refs,
    }))
}

async fn resolve_tip403_compound_policy(
    conn: &deadpool_postgres::Object,
    chain_id: u64,
    refs: CompoundPolicyRefs,
    member_limit: i64,
) -> AnyhowResult<CompoundPolicyData> {
    Ok(CompoundPolicyData {
        sender_policy: load_tip403_child_policy(
            conn,
            chain_id,
            refs.sender_policy_id,
            member_limit,
        )
        .await?,
        recipient_policy: load_tip403_child_policy(
            conn,
            chain_id,
            refs.recipient_policy_id,
            member_limit,
        )
        .await?,
        mint_recipient_policy: load_tip403_child_policy(
            conn,
            chain_id,
            refs.mint_recipient_policy_id,
            member_limit,
        )
        .await?,
    })
}

async fn load_tip403_child_policy(
    conn: &deadpool_postgres::Object,
    chain_id: u64,
    policy_id: u64,
    member_limit: i64,
) -> AnyhowResult<ChildPolicyData> {
    let policy_data = load_tip403_policy_metadata(conn, chain_id, policy_id)
        .await?
        .ok_or_else(|| anyhow!("compound child policy not found: {policy_id}"))?;

    let (members, next_member_cursor) = if policy_data.metadata.has_direct_members() {
        load_tip403_policy_members(
            conn,
            policy_id,
            &policy_data.metadata.policy_type,
            member_limit,
            None,
        )
        .await?
    } else {
        (Vec::new(), None)
    };

    Ok(ChildPolicyData {
        metadata: policy_data.metadata,
        members,
        next_member_cursor,
    })
}

async fn current_tip403_policy_admin(
    conn: &deadpool_postgres::Object,
    policy_id: u64,
) -> AnyhowResult<Option<String>> {
    let selector = ITIP403Registry::PolicyAdminUpdated::SIGNATURE_HASH.as_slice();
    let registry_address = tip403_registry_address();
    let policy_topic = tip403_policy_topic(policy_id);

    let row = conn
        .query_opt(
            r#"
            SELECT topic2, topic3
            FROM logs
            WHERE address = $1
              AND selector = $2 AND topic1 = $3 AND topic3 IS NOT NULL
            ORDER BY block_num DESC, tx_idx DESC, log_idx DESC
            LIMIT 1
            "#,
            &[&registry_address, &selector, &policy_topic],
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
    let selectors: [&[u8]; 5] = [
        ITIP403Registry::PolicyCreated::SIGNATURE_HASH.as_slice(),
        ITIP403Registry::CompoundPolicyCreated::SIGNATURE_HASH.as_slice(),
        ITIP403Registry::PolicyAdminUpdated::SIGNATURE_HASH.as_slice(),
        ITIP403Registry::WhitelistUpdated::SIGNATURE_HASH.as_slice(),
        ITIP403Registry::BlacklistUpdated::SIGNATURE_HASH.as_slice(),
    ];
    let registry_address = tip403_registry_address();
    let policy_topic = tip403_policy_topic(policy_id);

    let row = conn
        .query_opt(
            r#"
            SELECT block_timestamp
            FROM logs
            WHERE address = $1
              AND selector = ANY($2) AND topic1 = $3
            ORDER BY block_num DESC, tx_idx DESC, log_idx DESC
            LIMIT 1
            "#,
            &[&registry_address, &&selectors[..], &policy_topic],
        )
        .await?;

    Ok(row.map(|row| row.get(0)))
}

async fn load_tip403_policy_members(
    conn: &deadpool_postgres::Object,
    policy_id: u64,
    policy_type: &PolicyType,
    limit: i64,
    cursor: Option<&[u8]>,
) -> AnyhowResult<(Vec<String>, Option<String>)> {
    let selector = match policy_type {
        PolicyType::WHITELIST => ITIP403Registry::WhitelistUpdated::SIGNATURE_HASH.as_slice(),
        PolicyType::BLACKLIST => ITIP403Registry::BlacklistUpdated::SIGNATURE_HASH.as_slice(),
        _ => return Ok((Vec::new(), None)),
    };
    let registry_address = tip403_registry_address();
    let policy_topic = tip403_policy_topic(policy_id);
    let empty_cursor = vec![0u8; 32];
    let cursor = cursor.unwrap_or(&empty_cursor);
    let query_limit = limit + 1;

    let rows = conn
        .query(
            r#"
            SELECT DISTINCT ON (topic3)
                topic2, topic3, data
            FROM logs
            WHERE address = $1
              AND selector = $2 AND topic1 = $3 AND topic3 IS NOT NULL AND topic3 > $4
            ORDER BY topic3, block_num DESC, tx_idx DESC, log_idx DESC
            LIMIT $5
            "#,
            &[
                &registry_address,
                &selector,
                &policy_topic,
                &cursor,
                &query_limit,
            ],
        )
        .await?;

    let mut members = Vec::new();
    let mut next_member_cursor = None;
    for row in rows.iter().take(limit as usize) {
        let updater_topic: Vec<u8> = row.get(0);
        let account_topic: Vec<u8> = row.get(1);
        let data: Vec<u8> = row.get(2);
        next_member_cursor = Some(format!("0x{}", hex::encode(&account_topic[12..32])));

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

    Ok((
        members,
        if rows.len() as i64 > limit {
            next_member_cursor
        } else {
            None
        },
    ))
}
