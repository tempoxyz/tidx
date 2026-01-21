use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{
    extract::{ConnectInfo, Request, State},
    http::{HeaderMap, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use tokio::sync::RwLock;

use crate::config::RateLimitConfig;

/// Rate limiter state shared across requests.
#[derive(Clone)]
pub struct RateLimiter {
    /// Per-IP request counts and window start times
    buckets: Arc<RwLock<HashMap<IpAddr, TokenBucket>>>,
    /// Per-IP SSE connection counts
    sse_connections: Arc<RwLock<HashMap<IpAddr, u32>>>,
    /// Valid API keys (requests with these bypass rate limiting)
    api_keys: Arc<Vec<String>>,
    /// Configuration
    config: RateLimitConfig,
}

struct TokenBucket {
    tokens: u32,
    last_refill: Instant,
}

impl RateLimiter {
    pub fn new(config: RateLimitConfig, api_keys: Vec<String>) -> Self {
        Self {
            buckets: Arc::new(RwLock::new(HashMap::new())),
            sse_connections: Arc::new(RwLock::new(HashMap::new())),
            api_keys: Arc::new(api_keys),
            config,
        }
    }

    /// Check if request has a valid API key via Authorization: Bearer header.
    pub fn has_valid_api_key(&self, headers: &HeaderMap) -> bool {
        if self.api_keys.is_empty() {
            return false;
        }

        if let Some(auth) = headers.get("authorization").and_then(|v| v.to_str().ok()) {
            if let Some(key) = auth.strip_prefix("Bearer ") {
                return self.api_keys.contains(&key.to_string());
            }
        }

        false
    }

    /// Check and consume a token for the given IP.
    /// Returns Ok(remaining) if allowed, Err(retry_after_secs) if rate limited.
    pub async fn check_rate_limit(&self, ip: IpAddr) -> Result<u32, u64> {
        if !self.config.enabled {
            return Ok(u32::MAX);
        }

        let mut buckets = self.buckets.write().await;
        let now = Instant::now();
        let window = Duration::from_secs(self.config.window_secs);

        let bucket = buckets.entry(ip).or_insert_with(|| TokenBucket {
            tokens: self.config.requests_per_window,
            last_refill: now,
        });

        // Refill tokens if window has passed
        if now.duration_since(bucket.last_refill) >= window {
            bucket.tokens = self.config.requests_per_window;
            bucket.last_refill = now;
        }

        if bucket.tokens > 0 {
            bucket.tokens -= 1;
            Ok(bucket.tokens)
        } else {
            let elapsed = now.duration_since(bucket.last_refill);
            let retry_after = window.saturating_sub(elapsed).as_secs().max(1);
            Err(retry_after)
        }
    }

    /// Try to acquire an SSE connection slot for the given IP.
    pub async fn acquire_sse_connection(&self, ip: IpAddr) -> Result<SseConnectionGuard, ()> {
        if !self.config.enabled {
            return Ok(SseConnectionGuard::new(ip, Arc::clone(&self.sse_connections)));
        }

        let mut connections = self.sse_connections.write().await;
        let count = connections.entry(ip).or_insert(0);

        if *count >= self.config.max_sse_connections {
            return Err(());
        }

        *count += 1;
        Ok(SseConnectionGuard::new(ip, Arc::clone(&self.sse_connections)))
    }

    /// Periodically clean up stale entries.
    pub async fn cleanup(&self) {
        let now = Instant::now();
        let window = Duration::from_secs(self.config.window_secs * 2);

        let mut buckets = self.buckets.write().await;
        buckets.retain(|_, bucket| now.duration_since(bucket.last_refill) < window);
    }

    /// Get current config for rate limit headers.
    pub fn config(&self) -> &RateLimitConfig {
        &self.config
    }
}

/// RAII guard that decrements SSE connection count on drop.
pub struct SseConnectionGuard {
    ip: Option<IpAddr>,
    connections: Option<Arc<RwLock<HashMap<IpAddr, u32>>>>,
}

impl SseConnectionGuard {
    /// Create a guard for a rate-limited connection.
    fn new(ip: IpAddr, connections: Arc<RwLock<HashMap<IpAddr, u32>>>) -> Self {
        Self {
            ip: Some(ip),
            connections: Some(connections),
        }
    }

    /// Create a guard that doesn't track connections (for API key holders).
    pub fn new_unlimited() -> Self {
        Self {
            ip: None,
            connections: None,
        }
    }
}

impl Drop for SseConnectionGuard {
    fn drop(&mut self) {
        if let (Some(ip), Some(connections)) = (self.ip, self.connections.take()) {
            tokio::spawn(async move {
                let mut conns = connections.write().await;
                if let Some(count) = conns.get_mut(&ip) {
                    *count = count.saturating_sub(1);
                    if *count == 0 {
                        conns.remove(&ip);
                    }
                }
            });
        }
    }
}

/// Extract client IP from request (supports X-Forwarded-For).
pub fn extract_client_ip(headers: &HeaderMap, connect_info: Option<&std::net::SocketAddr>) -> IpAddr {
    // Try X-Forwarded-For first (for proxied requests)
    if let Some(forwarded) = headers.get("x-forwarded-for").and_then(|v| v.to_str().ok()) {
        if let Some(first_ip) = forwarded.split(',').next() {
            if let Ok(ip) = first_ip.trim().parse::<IpAddr>() {
                return ip;
            }
        }
    }

    // Try X-Real-IP
    if let Some(real_ip) = headers.get("x-real-ip").and_then(|v| v.to_str().ok()) {
        if let Ok(ip) = real_ip.trim().parse::<IpAddr>() {
            return ip;
        }
    }

    // Fall back to connection info
    connect_info
        .map(|addr| addr.ip())
        .unwrap_or_else(|| IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED))
}

/// Middleware that enforces rate limiting on requests.
pub async fn rate_limit_middleware(
    State(limiter): State<RateLimiter>,
    ConnectInfo(addr): ConnectInfo<std::net::SocketAddr>,
    request: Request,
    next: Next,
) -> Response {
    let headers = request.headers();

    // API key holders bypass rate limiting
    if limiter.has_valid_api_key(headers) {
        return next.run(request).await;
    }

    let ip = extract_client_ip(headers, Some(&addr));

    match limiter.check_rate_limit(ip).await {
        Ok(remaining) => {
            let mut response = next.run(request).await;
            let headers = response.headers_mut();
            headers.insert(
                "ratelimit-limit",
                limiter.config().requests_per_window.into(),
            );
            headers.insert("ratelimit-remaining", remaining.into());
            response
        }
        Err(retry_after) => {
            let body = serde_json::json!({
                "ok": false,
                "error": "Rate limit exceeded",
                "retry_after_secs": retry_after
            });
            (
                StatusCode::TOO_MANY_REQUESTS,
                [
                    ("retry-after", retry_after.to_string()),
                    ("ratelimit-limit", limiter.config().requests_per_window.to_string()),
                    ("ratelimit-remaining", "0".to_string()),
                ],
                Json(body),
            )
                .into_response()
        }
    }
}

/// Spawn a background task to periodically clean up rate limiter state.
pub fn spawn_cleanup_task(limiter: RateLimiter) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        loop {
            interval.tick().await;
            limiter.cleanup().await;
        }
    });
}
