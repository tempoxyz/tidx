//! The ordinary /query route is also used by passthrough proxies. A credential
//! on that route must never elevate caller-supplied SQL.
use axum::{
    extract::{Request, State},
    http::{HeaderMap, HeaderValue, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{MethodRouter, get},
};
use sha3::{Digest, Sha3_256};
use subtle::ConstantTimeEq;
use tower::limit::GlobalConcurrencyLimitLayer;

use super::{ApiError, AppState, handle_query};
use crate::query::ClickHouseReadAccess;

#[derive(Clone)]
pub(super) struct Credential([u8; 32]);

impl Credential {
    pub(super) fn new(token: &str) -> Option<Self> {
        // A dedicated opaque Bearer secret, never a Basic-auth user:password or
        // a blank/malformed value. Deployments should use 32 random bytes or more.
        if token.len() < 32
            || !token
                .bytes()
                .all(|b| b.is_ascii_graphic() && !matches!(b, b':' | b','))
        {
            return None;
        }
        Some(Self(Sha3_256::digest(token.as_bytes()).into()))
    }

    fn authenticates(&self, headers: &HeaderMap) -> bool {
        let mut values = headers.get_all(header::AUTHORIZATION).iter();
        let Some(token) = values
            .next()
            .and_then(|v| v.as_bytes().strip_prefix(b"Bearer "))
        else {
            return false;
        };
        values.next().is_none() && bool::from(self.0.ct_eq(&Sha3_256::digest(token)))
    }
}

pub(super) fn route(
    reader: Credential,
    concurrency: GlobalConcurrencyLimitLayer,
) -> MethodRouter<AppState> {
    get(handle_query)
        .layer(concurrency)
        .route_layer(middleware::from_fn_with_state(reader, authorize))
}

async fn authorize(State(reader): State<Credential>, mut request: Request, next: Next) -> Response {
    let mut response = if reader.authenticates(request.headers()) {
        request.extensions_mut().insert(ClickHouseReadAccess::Earn);
        next.run(request).await
    } else {
        ApiError::Forbidden("Earn reader credential required".to_string()).into_response()
    };
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("private, no-store"),
    );
    response
        .headers_mut()
        .append(header::VARY, HeaderValue::from_static("Authorization"));
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::{ApiMode, build_router_with_reader};
    use axum::{
        Router,
        body::{Body, to_bytes},
        http::StatusCode,
    };
    use std::sync::{Arc, RwLock as StdRwLock};
    use tokio::sync::{RwLock, Semaphore};
    use tower::ServiceExt;

    const TOKEN: &str = "local-test-earn-reader-credential-001";
    const AUTH: &str = "Bearer local-test-earn-reader-credential-001";
    const QUERY: &str = "?chainId=4217&engine=clickhouse&sql=SELECT%20*%20FROM%20earn_vaults";

    fn router(
        reader: Option<Credential>,
        engine: Option<crate::clickhouse::ClickHouseEngine>,
    ) -> Router {
        // Native ClickHouse reads select a configured chain but do not check out PostgreSQL.
        let pool = crate::db::Pool::builder(deadpool_postgres::Manager::new(
            tokio_postgres::Config::new(),
            tokio_postgres::NoTls,
        ))
        .build()
        .unwrap();
        let state = AppState {
            pools: Arc::new(RwLock::new([(4217, pool)].into())),
            default_chain_id: 4217,
            broadcaster: Arc::new(crate::broadcast::Broadcaster::new()),
            clickhouse_configs: Arc::default(),
            clickhouse_engines: Arc::new(RwLock::new(
                engine.into_iter().map(|e| (4217, Arc::new(e))).collect(),
            )),
            trusted_cidrs: Arc::new(StdRwLock::new(vec![])),
            query_permits: Arc::new(Semaphore::new(4)),
            max_live_connections: 1,
        };
        build_router_with_reader(state, ApiMode::Serve, reader)
    }

    async fn request(app: &Router, uri: &str, credentials: &[&str]) -> Response {
        let mut req = Request::builder().uri(uri);
        for value in credentials {
            req = req.header(header::AUTHORIZATION, *value);
        }
        app.clone()
            .oneshot(req.body(Body::empty()).unwrap())
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn earn_reader_requires_separate_route_and_secret() {
        let path = format!("/internal/earn/query{QUERY}");
        assert!(Credential::new("").is_none());
        assert!(Credential::new("short").is_none());
        assert_eq!(
            request(&router(None, None), &path, &[AUTH]).await.status(),
            StatusCode::NOT_FOUND
        );

        let engine = crate::clickhouse::ClickHouseEngine::new(
            &crate::config::ClickHouseConfig::default(),
            4217,
        )
        .unwrap();
        let app = router(Credential::new(TOKEN), Some(engine));
        for headers in [&[][..], &["Bearer wrong"][..], &[AUTH, AUTH][..]] {
            let denied = request(&app, &path, headers).await;
            assert_eq!(denied.status(), StatusCode::FORBIDDEN);
            assert_eq!(denied.headers()[header::CACHE_CONTROL], "private, no-store");
        }
        // Public /query has no capability even when a proxy forwards the correct bearer.
        let public = request(&app, &format!("/query{QUERY}"), &[AUTH]).await;
        assert_eq!(public.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let message = to_bytes(public.into_body(), 4096).await.unwrap();
        assert!(
            std::str::from_utf8(&message)
                .unwrap()
                .contains("Access to table 'earn_vaults' is not allowed")
        );

        let raw = request(
            &app,
            &path.replace("earn_vaults", "earn_verified_vaults"),
            &[AUTH],
        )
        .await;
        assert_eq!(raw.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let message = to_bytes(raw.into_body(), 4096).await.unwrap();
        assert!(
            std::str::from_utf8(&message)
                .unwrap()
                .contains("Access to table 'earn_verified_vaults' is not allowed")
        );
        assert_eq!(
            request(&app, &format!("{path}&live=true"), &[AUTH])
                .await
                .status(),
            StatusCode::BAD_REQUEST
        );
    }

    #[tokio::test]
    #[ignore = "requires local ClickHouse (CLICKHOUSE_URL)"]
    async fn earn_reader_http_reads_derived_rows_without_shared_caching() {
        let cfg = crate::config::ClickHouseConfig {
            url: std::env::var("CLICKHOUSE_URL")
                .unwrap_or_else(|_| "http://127.0.0.1:8123".to_owned()),
            database: Some("default".into()),
            ..Default::default()
        };
        let admin = crate::clickhouse::ClickHouseEngine::new(&cfg, 4217).unwrap();
        let db = format!("tidx_earn_reader_test_{}", rand::random::<u64>());
        admin
            .query(&format!("CREATE DATABASE {db}"), &[])
            .await
            .unwrap();
        let engine = crate::clickhouse::ClickHouseEngine::new(
            &crate::config::ClickHouseConfig {
                database: Some(db.clone()),
                ..cfg
            },
            4217,
        )
        .unwrap();
        engine
            .query(
                "CREATE VIEW earn_vaults AS SELECT 'test-vault' AS vault",
                &[],
            )
            .await
            .unwrap();
        let app = router(Credential::new(TOKEN), Some(engine));
        let resp = request(&app, &format!("/internal/earn/query{QUERY}"), &[AUTH]).await;
        admin
            .query(&format!("DROP DATABASE {db} SYNC"), &[])
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.headers()[header::CACHE_CONTROL], "private, no-store");
        assert!(
            resp.headers()
                .get_all(header::VARY)
                .iter()
                .any(|v| v == "Authorization")
        );
        let body: serde_json::Value =
            serde_json::from_slice(&to_bytes(resp.into_body(), 4096).await.unwrap()).unwrap();
        assert_eq!(body["rows"][0][0], "test-vault");
    }
}
