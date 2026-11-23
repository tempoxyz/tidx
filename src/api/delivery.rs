//! Response delivery: query permits that last until the body is sent, and
//! connections that are closed when the peer stops accepting writes.

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, ready};
use std::time::Duration;

use axum::body::{Body, Bytes, HttpBody};
use axum::extract::{Request, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::serve::{Listener, ListenerExt, TapIo};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::Sleep;

/// How long a connection may go without any write progress before it is
/// closed. Matches the longest query timeout a request can ask for.
pub const WRITE_STALL_TIMEOUT: Duration = Duration::from_secs(30);

/// Size of the frames a buffered query response is sent in. hyper queues at
/// most 16 frames per connection, so this bounds what stays queued after the
/// last frame is handed over.
const RESPONSE_CHUNK_BYTES: usize = 64 * 1024;

/// Runs a query request under one of `permits` and keeps that permit until
/// the response body has been handed to the connection, rather than only
/// until the handler returns.
///
/// A fully buffered response is resent in copied chunks, so its full
/// allocation is freed together with the permit. Streaming responses (live
/// queries) pass through unchanged: each of their statements takes its own
/// permit.
pub async fn hold_query_permit(
    State(permits): State<Arc<Semaphore>>,
    request: Request,
    next: Next,
) -> Response {
    let Ok(permit) = permits.acquire_owned().await else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };

    let response = next.run(request).await;
    if response.body().size_hint().exact().is_none() {
        return response;
    }

    let (mut parts, body) = response.into_parts();
    let bytes = match axum::body::to_bytes(body, usize::MAX).await {
        Ok(bytes) => bytes,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    parts
        .headers
        .insert(header::CONTENT_LENGTH, HeaderValue::from(bytes.len()));

    Response::from_parts(parts, Body::from_stream(chunked(bytes, permit)))
}

/// Yields copies of `bytes` in chunks, dropping `bytes` and `permit` once the
/// last chunk has been taken.
fn chunked(
    bytes: Bytes,
    permit: OwnedSemaphorePermit,
) -> impl futures::Stream<Item = Result<Bytes, std::convert::Infallible>> + Send {
    futures::stream::unfold(Some((bytes, 0usize, permit)), |state| async move {
        let (bytes, offset, permit) = state?;
        if offset >= bytes.len() {
            return None;
        }
        let end = (offset + RESPONSE_CHUNK_BYTES).min(bytes.len());
        let chunk = Bytes::copy_from_slice(&bytes[offset..end]);
        let next = (end < bytes.len()).then_some((bytes, end, permit));
        Some((Ok(chunk), next))
    })
}

/// A listener whose connections fail once a write makes no progress for
/// `timeout`, so the server drops them and everything they hold.
pub struct StallTimeoutListener {
    inner: TcpListener,
    timeout: Duration,
}

/// Wraps `listener` for `axum::serve`. The no-op tap keeps
/// `ConnectInfo<SocketAddr>` available to handlers.
pub fn api_listener(
    listener: TcpListener,
    timeout: Duration,
) -> TapIo<StallTimeoutListener, fn(&mut StallTimeoutStream)> {
    StallTimeoutListener {
        inner: listener,
        timeout,
    }
    .tap_io(|_| {})
}

impl Listener for StallTimeoutListener {
    type Io = StallTimeoutStream;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        let (stream, addr) = Listener::accept(&mut self.inner).await;
        (StallTimeoutStream::new(stream, self.timeout), addr)
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        Listener::local_addr(&self.inner)
    }
}

/// A TCP stream whose writes and flushes fail with `TimedOut` after waiting
/// `timeout` without progress.
pub struct StallTimeoutStream {
    inner: TcpStream,
    timeout: Duration,
    stalled: Option<Pin<Box<Sleep>>>,
}

impl StallTimeoutStream {
    fn new(inner: TcpStream, timeout: Duration) -> Self {
        Self {
            inner,
            timeout,
            stalled: None,
        }
    }

    fn track<T>(&mut self, cx: &mut Context<'_>, poll: Poll<io::Result<T>>) -> Poll<io::Result<T>> {
        if poll.is_ready() {
            self.stalled = None;
            return poll;
        }
        let timeout = self.timeout;
        let sleep = self
            .stalled
            .get_or_insert_with(|| Box::pin(tokio::time::sleep(timeout)));
        ready!(sleep.as_mut().poll(cx));
        Poll::Ready(Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "connection made no write progress",
        )))
    }
}

impl AsyncRead for StallTimeoutStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for StallTimeoutStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let poll = Pin::new(&mut self.inner).poll_write(cx, buf);
        self.track(cx, poll)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let poll = Pin::new(&mut self.inner).poll_write_vectored(cx, bufs);
        self.track(cx, poll)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let poll = Pin::new(&mut self.inner).poll_flush(cx);
        self.track(cx, poll)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::routing::get;
    use axum::{Json, Router, middleware};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpSocket;

    const BODY_BYTES: usize = 8 * 1024 * 1024;

    async fn large() -> Json<serde_json::Value> {
        Json(serde_json::json!({ "data": "x".repeat(BODY_BYTES) }))
    }

    async fn start(permits: Arc<Semaphore>, timeout: Duration) -> SocketAddr {
        let router = Router::new().route(
            "/query",
            get(large).layer(middleware::from_fn_with_state(permits, hold_query_permit)),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(
                api_listener(listener, timeout),
                router.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            .unwrap();
        });
        addr
    }

    /// Sends a request and never reads the response.
    async fn stalled_client(addr: SocketAddr) -> TcpStream {
        let socket = TcpSocket::new_v4().unwrap();
        socket.set_recv_buffer_size(4096).unwrap();
        let mut stream = socket.connect(addr).await.unwrap();
        stream
            .write_all(b"GET /query HTTP/1.1\r\nHost: x\r\n\r\n")
            .await
            .unwrap();
        stream
    }

    async fn full_response(addr: SocketAddr) -> Vec<u8> {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"GET /query HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut out = Vec::new();
        stream.read_to_end(&mut out).await.unwrap();
        out
    }

    #[tokio::test]
    async fn sends_whole_body_with_content_length() {
        let addr = start(Arc::new(Semaphore::new(1)), WRITE_STALL_TIMEOUT).await;
        let response = full_response(addr).await;
        let split = response.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
        let head = String::from_utf8_lossy(&response[..split]).to_lowercase();
        let body = &response[split + 4..];
        assert!(
            head.contains(&format!("content-length: {}", body.len())),
            "{head}"
        );
        let value: serde_json::Value = serde_json::from_slice(body).unwrap();
        assert_eq!(value["data"].as_str().unwrap().len(), BODY_BYTES);
    }

    #[tokio::test]
    async fn permit_held_until_body_is_sent() {
        let permits = Arc::new(Semaphore::new(1));
        let addr = start(permits.clone(), Duration::from_secs(2)).await;

        let _stalled = stalled_client(addr).await;
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(
            permits.available_permits(),
            0,
            "permit released before delivery"
        );

        // A second request waits for the permit, and is served once the
        // stalled connection is closed for making no write progress.
        let started = std::time::Instant::now();
        let response = tokio::time::timeout(Duration::from_secs(10), full_response(addr))
            .await
            .expect("second request never served");
        assert!(started.elapsed() >= Duration::from_secs(1));
        assert!(response.starts_with(b"HTTP/1.1 200"));
        assert_eq!(permits.available_permits(), 1);
    }
}
