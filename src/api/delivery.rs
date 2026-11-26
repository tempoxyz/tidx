//! Response delivery: query permits that last until the body is sent (up to
//! a deadline), and connections that are closed when the peer stops
//! accepting writes.

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
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
use tokio::task::AbortHandle;
use tokio::time::Sleep;

/// How long a connection may go without any write progress before it is
/// closed. Matches the longest query timeout a request can ask for.
pub const WRITE_STALL_TIMEOUT: Duration = Duration::from_secs(30);

/// How long a buffered query response holds its permit while it is being
/// sent. Past it the permit is released, and the response keeps going for as
/// long as the client keeps up with `MIN_DELIVERY_RATE`. Matches the longest
/// query timeout.
pub const DELIVERY_DEADLINE: Duration = Duration::from_secs(30);

/// Past the deadline, the slowest delivery that is still allowed to finish,
/// in bytes per second. Slower deliveries are dropped and their body ends
/// with an error.
pub const MIN_DELIVERY_RATE: usize = 64 * 1024;

/// How often a delivery's progress is measured once past the deadline.
const DELIVERY_PROGRESS_WINDOW: Duration = Duration::from_secs(10);

/// Size of the frames a buffered query response is sent in. hyper queues at
/// most 16 frames per HTTP/1 connection, so this bounds what stays queued
/// once the response itself is released.
const RESPONSE_CHUNK_BYTES: usize = 16 * 1024;

/// When a buffered response gives up its permit, and how fast it must keep
/// being sent after that.
#[derive(Clone, Copy, Debug)]
struct DeliveryLimits {
    deadline: Duration,
    window: Duration,
    min_bytes_per_window: usize,
}

impl Default for DeliveryLimits {
    fn default() -> Self {
        Self {
            deadline: DELIVERY_DEADLINE,
            window: DELIVERY_PROGRESS_WINDOW,
            min_bytes_per_window: MIN_DELIVERY_RATE * DELIVERY_PROGRESS_WINDOW.as_secs() as usize,
        }
    }
}

/// Runs a query request under one of `permits` and keeps that permit until
/// the response body has been handed to the connection, or until
/// `DELIVERY_DEADLINE` passes, rather than only until the handler returns.
///
/// A fully buffered response is resent in copied chunks, so its full
/// allocation is freed once it is sent, or once a delivery past the deadline
/// falls below `MIN_DELIVERY_RATE`. Streaming responses (live queries) pass
/// through unchanged: each of their statements takes its own permit.
pub async fn hold_query_permit(
    State(permits): State<Arc<Semaphore>>,
    request: Request,
    next: Next,
) -> Response {
    hold_query_permit_with(permits, request, next, DeliveryLimits::default()).await
}

async fn hold_query_permit_with(
    permits: Arc<Semaphore>,
    request: Request,
    next: Next,
    limits: DeliveryLimits,
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

    let body = Delivery::start(bytes, permit, limits);
    Response::from_parts(parts, Body::from_stream(body.into_stream()))
}

/// What a buffered response still holds while it is being sent.
struct Pending {
    /// The response, until it is sent or dropped for being too slow.
    bytes: Option<Bytes>,
    /// Bytes handed to the connection so far.
    offset: usize,
    /// The query permit, until the response is sent or the deadline passes.
    permit: Option<OwnedSemaphorePermit>,
}

/// A buffered response being sent. A timer releases its permit at the
/// deadline, then drops the response if it falls below the minimum rate,
/// even if the connection never polls the body again, as happens when the
/// peer stops reading.
struct Delivery {
    pending: Arc<Mutex<Pending>>,
    total: usize,
    timer: AbortHandle,
}

fn lock(pending: &Mutex<Pending>) -> std::sync::MutexGuard<'_, Pending> {
    pending.lock().unwrap_or_else(|e| e.into_inner())
}

impl Delivery {
    fn start(bytes: Bytes, permit: OwnedSemaphorePermit, limits: DeliveryLimits) -> Self {
        let total = bytes.len();
        let pending = Arc::new(Mutex::new(Pending {
            bytes: Some(bytes),
            offset: 0,
            permit: Some(permit),
        }));
        let watched = Arc::downgrade(&pending);
        let timer = tokio::spawn(async move {
            tokio::time::sleep(limits.deadline).await;
            let Some(pending) = watched.upgrade() else {
                return;
            };
            let (permit, mut last) = {
                let mut p = lock(&pending);
                (p.permit.take(), p.offset)
            };
            drop(permit);
            drop(pending);

            loop {
                tokio::time::sleep(limits.window).await;
                let Some(pending) = watched.upgrade() else {
                    return;
                };
                let mut p = lock(&pending);
                if p.bytes.is_none() {
                    return;
                }
                if p.offset - last < limits.min_bytes_per_window {
                    let dropped = p.bytes.take();
                    drop(p);
                    drop(dropped);
                    return;
                }
                last = p.offset;
            }
        })
        .abort_handle();
        Self {
            pending,
            total,
            timer,
        }
    }

    /// The next chunk, or None once the response was dropped for being too
    /// slow.
    fn next_chunk(&self) -> Option<Bytes> {
        let mut p = lock(&self.pending);
        let bytes = p.bytes.as_ref()?;
        let end = (p.offset + RESPONSE_CHUNK_BYTES).min(bytes.len());
        let chunk = Bytes::copy_from_slice(&bytes[p.offset..end]);
        p.offset = end;
        if end >= self.total {
            // Release the response and its permit with the last chunk.
            p.bytes = None;
            p.permit = None;
        }
        Some(chunk)
    }

    fn into_stream(self) -> impl futures::Stream<Item = io::Result<Bytes>> + Send {
        let total = self.total;
        futures::stream::unfold((self, 0usize), move |(delivery, sent)| async move {
            if sent >= total {
                return None;
            }
            match delivery.next_chunk() {
                Some(chunk) => {
                    let sent = sent + chunk.len();
                    Some((Ok(chunk), (delivery, sent)))
                }
                None => Some((
                    Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "response delivery fell below the minimum rate",
                    )),
                    (delivery, total),
                )),
            }
        })
    }
}

impl Drop for Delivery {
    fn drop(&mut self) {
        self.timer.abort();
    }
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

    /// Limits that release the permit after `deadline` and drop deliveries
    /// slower than 64 KiB per 500 ms after that.
    fn limits(deadline: Duration) -> DeliveryLimits {
        DeliveryLimits {
            deadline,
            window: Duration::from_millis(500),
            min_bytes_per_window: 64 * 1024,
        }
    }

    async fn start(permits: Arc<Semaphore>, stall: Duration, limits: DeliveryLimits) -> SocketAddr {
        let router = Router::new().route(
            "/query",
            get(large).layer(middleware::from_fn(move |request, next| {
                hold_query_permit_with(permits.clone(), request, next, limits)
            })),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(
                api_listener(listener, stall),
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
        let addr = start(
            Arc::new(Semaphore::new(1)),
            WRITE_STALL_TIMEOUT,
            DeliveryLimits::default(),
        )
        .await;
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
        let addr = start(
            permits.clone(),
            Duration::from_secs(2),
            DeliveryLimits::default(),
        )
        .await;

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

    #[tokio::test]
    async fn deadline_releases_permit_while_connection_stays_open() {
        let permits = Arc::new(Semaphore::new(1));
        // The stall timeout never fires here, so only the deadline can
        // release the permit of a response the peer never reads.
        let addr = start(
            permits.clone(),
            Duration::from_secs(3600),
            limits(Duration::from_secs(1)),
        )
        .await;

        let _stalled = stalled_client(addr).await;
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(permits.available_permits(), 0);

        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(
            permits.available_permits(),
            1,
            "deadline did not release the permit"
        );

        let response = tokio::time::timeout(Duration::from_secs(10), full_response(addr))
            .await
            .expect("second request never served");
        assert!(response.starts_with(b"HTTP/1.1 200"));
    }

    /// Reads the whole response `chunk` bytes at a time, pausing between
    /// reads, until the server ends or resets the connection.
    async fn paced_response(addr: SocketAddr, chunk: usize, pause: Duration) -> Vec<u8> {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"GET /query HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut out = Vec::new();
        let mut buf = vec![0u8; chunk];
        loop {
            match stream.read(&mut buf).await {
                Ok(0) | Err(_) => return out,
                Ok(n) => out.extend_from_slice(&buf[..n]),
            }
            tokio::time::sleep(pause).await;
        }
    }

    fn body_len(response: &[u8]) -> usize {
        let split = response.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
        response.len() - split - 4
    }

    #[tokio::test]
    async fn slow_reader_past_deadline_gets_whole_body() {
        let permits = Arc::new(Semaphore::new(1));
        let addr = start(
            permits.clone(),
            WRITE_STALL_TIMEOUT,
            limits(Duration::from_millis(300)),
        )
        .await;

        // About 3 MB/s: takes well past the deadline, but stays above the
        // minimum rate.
        let response = paced_response(addr, 64 * 1024, Duration::from_millis(20)).await;
        let expected = serde_json::to_vec(&serde_json::json!({ "data": "x".repeat(BODY_BYTES) }))
            .unwrap()
            .len();
        assert_eq!(
            body_len(&response),
            expected,
            "slow reader's body was cut off"
        );
        assert_eq!(permits.available_permits(), 1);
    }

    /// Takes chunks from a delivery, pausing between them.
    async fn drain(
        stream: &mut (impl futures::Stream<Item = io::Result<Bytes>> + Unpin),
        pause: Duration,
    ) -> (usize, bool) {
        use futures::StreamExt;
        let mut sent = 0;
        while let Some(item) = stream.next().await {
            match item {
                Ok(chunk) => sent += chunk.len(),
                Err(_) => return (sent, false),
            }
            tokio::time::sleep(pause).await;
        }
        (sent, true)
    }

    #[tokio::test]
    async fn delivery_below_minimum_rate_is_dropped() {
        let permits = Arc::new(Semaphore::new(1));
        let permit = permits.clone().acquire_owned().await.unwrap();
        let bytes = Bytes::from(vec![b'x'; 4 * 1024 * 1024]);
        let delivery = Delivery::start(bytes, permit, limits(Duration::from_millis(300)));
        let mut stream = Box::pin(delivery.into_stream());

        // One 16 KiB chunk per 200 ms is 40 KiB per window, below 64 KiB.
        let (sent, finished) = drain(&mut stream, Duration::from_millis(200)).await;
        assert!(!finished, "slow delivery was not dropped");
        assert!(sent < 4 * 1024 * 1024);
        assert_eq!(permits.available_permits(), 1);
    }

    #[tokio::test]
    async fn delivery_above_minimum_rate_finishes_past_deadline() {
        let permits = Arc::new(Semaphore::new(1));
        let permit = permits.clone().acquire_owned().await.unwrap();
        let bytes = Bytes::from(vec![b'x'; 1024 * 1024]);
        let delivery = Delivery::start(bytes, permit, limits(Duration::from_millis(300)));
        let mut stream = Box::pin(delivery.into_stream());

        // One 16 KiB chunk per 20 ms is 400 KiB per window: about 1.3 s in
        // all, well past the deadline.
        let (sent, finished) = drain(&mut stream, Duration::from_millis(20)).await;
        assert!(finished);
        assert_eq!(sent, 1024 * 1024);
        assert_eq!(permits.available_permits(), 1);
    }
}
