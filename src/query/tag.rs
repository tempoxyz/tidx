//! Query tags: who asked (caller), through which API route, and the request
//! ID, attached to every database query an API request runs so it can be
//! found again in ClickHouse's `system.query_log` (`log_comment`) and in
//! PostgreSQL (`application_name`).
//!
//! The tag is a task-local set around the request's handler, so the query
//! functions need no extra parameters and queries outside an API request
//! (sync, CLI) carry none.
//!
//! Format: `tidx/<route>/<caller>/<request_id>`. Every part is limited to
//! `[A-Za-z0-9._-]`, so the tag is safe in a URL, a SQL string literal and
//! pg_clickhouse's `session_settings` list without escaping.

use std::future::Future;

tokio::task_local! {
    static CURRENT: QueryTag;
}

/// Request header naming the calling service, such as `explorer`.
pub const CALLER_HEADER: &str = "x-tidx-caller";
/// Request header carrying a request ID to reuse (Envoy sets it); echoed on
/// the response.
pub const REQUEST_ID_HEADER: &str = "x-request-id";

const MAX_ROUTE_LEN: usize = 32;
const MAX_CALLER_LEN: usize = 32;
const MAX_REQUEST_ID_LEN: usize = 64;
/// PostgreSQL truncates `application_name` to NAMEDATALEN - 1 bytes.
const MAX_APPLICATION_NAME_LEN: usize = 63;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryTag {
    route: String,
    caller: String,
    request_id: String,
}

impl QueryTag {
    pub const CALLER_HEADER_NAME: &'static str = CALLER_HEADER;
    pub const REQUEST_ID_HEADER_NAME: &'static str = REQUEST_ID_HEADER;

    /// Build a tag from untrusted header values: unsafe characters become
    /// `_`, parts are truncated, a missing caller is `-` and a missing
    /// request ID is generated.
    pub fn new(route: &str, caller: Option<&str>, request_id: Option<&str>) -> Self {
        let request_id = request_id
            .map(|id| sanitize(id, MAX_REQUEST_ID_LEN))
            .filter(|id| id != "-")
            .unwrap_or_else(new_request_id);
        Self {
            route: sanitize(route, MAX_ROUTE_LEN),
            caller: caller.map_or_else(|| "-".to_string(), |c| sanitize(c, MAX_CALLER_LEN)),
            request_id,
        }
    }

    pub fn route(&self) -> &str {
        &self.route
    }

    pub fn caller(&self) -> &str {
        &self.caller
    }

    pub fn request_id(&self) -> &str {
        &self.request_id
    }

    /// The tag as one string, for ClickHouse's `log_comment`.
    pub fn comment(&self) -> String {
        format!("tidx/{}/{}/{}", self.route, self.caller, self.request_id)
    }

    /// The tag cut to PostgreSQL's `application_name` length.
    pub fn application_name(&self) -> String {
        let mut name = self.comment();
        name.truncate(MAX_APPLICATION_NAME_LEN);
        name
    }

    /// The tag of the API request this task is serving, if any.
    pub fn current() -> Option<Self> {
        CURRENT.try_with(Clone::clone).ok()
    }

    /// Run `f` with this tag as the current one.
    pub async fn scope<F: Future>(self, f: F) -> F::Output {
        CURRENT.scope(self, f).await
    }
}

fn sanitize(value: &str, max_len: usize) -> String {
    let cleaned: String = value
        .trim()
        .chars()
        .take(max_len)
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                c
            } else {
                '_'
            }
        })
        .collect();
    if cleaned.is_empty() {
        "-".to_string()
    } else {
        cleaned
    }
}

fn new_request_id() -> String {
    format!("{:032x}", rand::random::<u128>())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitizes_untrusted_parts() {
        let tag = QueryTag::new("query.tiered", Some("my app/1.0'; --"), Some("abc-123"));
        assert_eq!(tag.comment(), "tidx/query.tiered/my_app_1.0___--/abc-123");
    }

    #[test]
    fn fills_missing_caller_and_request_id() {
        let tag = QueryTag::new("query.postgres", None, Some("  "));
        assert_eq!(tag.caller(), "-");
        assert_eq!(tag.request_id().len(), 32);
        assert!(tag.request_id().chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn truncates_long_parts() {
        let long = "x".repeat(200);
        let tag = QueryTag::new("query.clickhouse", Some(&long), Some(&long));
        assert_eq!(tag.caller().len(), MAX_CALLER_LEN);
        assert_eq!(tag.request_id().len(), MAX_REQUEST_ID_LEN);
        assert_eq!(tag.application_name().len(), MAX_APPLICATION_NAME_LEN);
    }

    #[tokio::test]
    async fn current_is_set_only_inside_scope() {
        assert_eq!(QueryTag::current(), None);
        let tag = QueryTag::new("query.postgres", Some("svc"), Some("r1"));
        let seen = tag.clone().scope(async { QueryTag::current() }).await;
        assert_eq!(seen, Some(tag));
        assert_eq!(QueryTag::current(), None);
    }
}
