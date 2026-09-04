use std::collections::HashMap;
use std::net::{IpAddr, Ipv6Addr};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::Body;
use axum::extract::ConnectInfo;
use axum::Router;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::Request;
use hyper_util::rt::{TokioIo, TokioTimer};
use socket2::{SockRef, TcpKeepalive};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tower::ServiceExt;
use tracing::{debug, info, warn};

/// Pause after an accept error that isn't per-connection. Out of open sockets,
/// accept fails instantly, and retrying with no pause spins the CPU.
const ACCEPT_BACKOFF: Duration = Duration::from_millis(100);

/// How often to report connections refused at the caps, if there were any.
const REFUSAL_REPORT_INTERVAL: Duration = Duration::from_secs(10);

/// Tunable resource limits for the serve loop. Defaults match the gateway's.
#[derive(Clone, Copy)]
pub struct Limits {
    /// Max concurrent client connections. Connections past this are dropped so a
    /// flood cannot exhaust file descriptors or memory.
    pub max_connections: NonZeroUsize,
    /// Max concurrent connections from a single client IP, so one host cannot
    /// consume the whole global connection budget.
    pub max_connections_per_ip: NonZeroUsize,
    /// Max time a client may take to send the full request header block.
    /// Slowloris header-trickle connections are closed after this.
    pub header_read_timeout: Duration,
    /// Idle time before the OS starts sending TCP keepalive probes.
    ///
    /// A backstop. At the default timeouts nothing lives long enough to be
    /// probed: `header_read_timeout` re-arms per request, so it closes an idle
    /// connection first. It earns its place once that timeout is raised for a
    /// long-pooling client, which is when a peer that vanished without a FIN
    /// could otherwise hold a slot for as long as the raised value.
    pub tcp_keepalive_idle: Duration,
    /// Interval between TCP keepalive probes.
    pub tcp_keepalive_interval: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_connections: NonZeroUsize::new(1024).unwrap(),
            max_connections_per_ip: NonZeroUsize::new(64).unwrap(),
            header_read_timeout: Duration::from_secs(10),
            tcp_keepalive_idle: Duration::from_secs(60),
            tcp_keepalive_interval: Duration::from_secs(15),
        }
    }
}

/// Tracks how many connections each client IP currently holds. Entries are
/// removed when an IP's count reaches zero, so the map only holds IPs with a
/// live connection and stays bounded by the global connection cap.
type IpConnCounts = Arc<Mutex<HashMap<IpAddr, usize>>>;

/// True for accept errors that reflect one failed connection (the peer went
/// away before we accepted it) rather than a problem with the listener. These
/// are safe to skip without any backoff.
fn is_connection_error(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        std::io::ErrorKind::ConnectionRefused
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::ConnectionReset
    )
}

/// Collapses a peer address to the key both per-IP budgets count by: the
/// connection cap here and the request limiter in `throttle`. IPv4 is used
/// as-is; IPv6 is masked to its /64 prefix. A single client is routinely handed
/// a whole /64, so without masking it could spray addresses across that range
/// and get a fresh budget for each one.
pub fn client_key(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V4(_) => ip,
        IpAddr::V6(v6) => {
            let mut octets = v6.octets();
            octets[8..].fill(0);
            IpAddr::V6(Ipv6Addr::from(octets))
        }
    }
}

/// Increments the connection count for `ip` when it is below `max`, returning a
/// guard that decrements it on drop. Returns None when the IP is at the cap.
fn try_acquire_ip(counts: &IpConnCounts, ip: IpAddr, max: usize) -> Option<IpConnGuard> {
    let mut map = counts.lock().unwrap();
    let count = map.entry(ip).or_insert(0);
    if *count >= max {
        return None;
    }
    *count += 1;
    Some(IpConnGuard {
        counts: Arc::clone(counts),
        ip,
    })
}

/// Releases one per-IP connection slot on drop.
struct IpConnGuard {
    counts: IpConnCounts,
    ip: IpAddr,
}

impl Drop for IpConnGuard {
    fn drop(&mut self) {
        let mut map = self.counts.lock().unwrap();
        if let Some(count) = map.get_mut(&self.ip) {
            *count -= 1;
            if *count == 0 {
                map.remove(&self.ip);
            }
        }
    }
}

/// Serves `app` on `listener` under `limits`.
///
/// Replaces `axum::serve`, which accepts without bound and has no header
/// timeout, so idle or header-trickling connections pile up before any route
/// ever runs.
pub async fn serve(listener: TcpListener, app: Router, limits: Limits) -> std::io::Result<()> {
    info!("Auth listening on http://{}", listener.local_addr()?);

    // Cap total concurrent connections. A connection past the cap is dropped at
    // once rather than queued, so a flood can't pile up resources.
    let connection_slots = Arc::new(Semaphore::new(limits.max_connections.get()));
    // Per-IP connection counts, so one host can't consume the global budget.
    let ip_counts: IpConnCounts = Arc::new(Mutex::new(HashMap::new()));

    // Refusals are counted and reported on a timer rather than logged one by
    // one: a line per refused connection would amplify the very flood the caps
    // exist to absorb, and bury real errors while doing it. The addresses go out
    // at debug, so raising the level gives back per-connection detail without
    // changing what a default-level operator sees.
    let mut refused_at_global_cap: u64 = 0;
    let mut refused_at_ip_cap: u64 = 0;
    // Bumped from the connection tasks, so this one is shared.
    let closed_on_header_timeout = Arc::new(AtomicU64::new(0));
    let mut refusal_report = tokio::time::interval(REFUSAL_REPORT_INTERVAL);

    loop {
        // Both branches are cancel-safe, which is what makes losing the race
        // harmless: `accept` keeps no state across a cancelled poll, so no
        // connection is dropped. Anything added here has to hold that.
        let (stream, peer_addr) = tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok(conn) => conn,
                Err(e) if is_connection_error(&e) => continue,
                Err(e) => {
                    // Anything else must not kill the listener and restart the
                    // process, so log it and retry after a pause.
                    warn!("accept() failed: {e}; backing off before retrying");
                    tokio::time::sleep(ACCEPT_BACKOFF).await;
                    continue;
                }
            },
            _ = refusal_report.tick() => {
                // Also covers the header timeout, so every path that sheds a
                // connection is visible in one line at the default level.
                let header_timeouts = closed_on_header_timeout.swap(0, Ordering::Relaxed);
                if refused_at_global_cap > 0 || refused_at_ip_cap > 0 || header_timeouts > 0 {
                    warn!(
                        global_cap = refused_at_global_cap,
                        per_ip_cap = refused_at_ip_cap,
                        header_timeout = header_timeouts,
                        "shed connections"
                    );
                    refused_at_global_cap = 0;
                    refused_at_ip_cap = 0;
                }
                continue;
            }
        };
        let ip = peer_addr.ip();

        // Take a global slot. None free means we are at the cap, so drop the
        // socket immediately.
        let permit = match Arc::clone(&connection_slots).try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                refused_at_global_cap += 1;
                debug!("At the global connection cap, dropping {peer_addr}");
                continue;
            }
        };

        // Take a per-IP slot. None means this IP is at its cap; continue to drop
        // the socket and release the global permit.
        let ip_guard = match try_acquire_ip(
            &ip_counts,
            client_key(ip),
            limits.max_connections_per_ip.get(),
        ) {
            Some(guard) => guard,
            None => {
                refused_at_ip_cap += 1;
                debug!("At the per-IP connection cap for {ip}, dropping {peer_addr}");
                continue;
            }
        };

        // Enable OS TCP keepalive so a peer that vanishes without a close (its
        // network dropped) is detected and the socket reclaimed. Best-effort:
        // log and keep serving if it fails.
        let keepalive = TcpKeepalive::new()
            .with_time(limits.tcp_keepalive_idle)
            .with_interval(limits.tcp_keepalive_interval);
        if let Err(e) = SockRef::from(&stream).set_tcp_keepalive(&keepalive) {
            warn!("Failed to set TCP keepalive for {ip}: {e}");
        }

        let io = TokioIo::new(stream);
        let app = app.clone();
        let header_timeouts = Arc::clone(&closed_on_header_timeout);

        tokio::spawn(async move {
            // Hold both slots for the connection's lifetime; released on drop.
            let _permit = permit;
            let _ip_guard = ip_guard;

            let service = service_fn(move |req: Request<Incoming>| {
                let mut req = req.map(Body::new);
                // The per-IP throttle reads the peer address out of ConnectInfo.
                // axum's make-service normally inserts it, so do it here.
                req.extensions_mut().insert(ConnectInfo(peer_addr));
                // oneshot runs one request through the router: it waits for
                // readiness, then calls it once. It consumes the router, hence
                // the clone.
                app.clone().oneshot(req)
            });

            // HTTP/1 only, matching the gateway. Clients arrive over h1 or reach
            // us through a proxy that terminates h2, so the auto builder's h2c
            // detection would only add surface.
            //
            // The timer is required for header_read_timeout to take effect; it
            // closes clients that trickle their headers (slowloris).
            let conn = http1::Builder::new()
                .timer(TokioTimer::new())
                .header_read_timeout(limits.header_read_timeout)
                .serve_connection(io, service);
            // Debug, not error: these are client-caused (a hangup, or the header
            // timeout firing) and every slowloris connection we shed lands here.
            // A header timeout is counted so the shed still shows up at the
            // default level, in the periodic report.
            if let Err(e) = conn.await {
                if e.is_timeout() {
                    header_timeouts.fetch_add(1, Ordering::Relaxed);
                }
                debug!("Error serving connection from {peer_addr}: {e:?}");
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;
    use axum::routing::{get, post};
    use std::net::SocketAddr;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    /// Spawns the serve loop over a stub router on an OS-assigned port. The
    /// guards under test are connection-level, so no AppState is needed.
    async fn start_server(limits: Limits) -> SocketAddr {
        let app = Router::new()
            .route("/health", get(|| async { StatusCode::OK }))
            // Echoes the body back, so a body lost or carried over between
            // requests is visible in the response.
            .route("/echo", post(|body: String| async move { body }));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(listener, app, limits));

        addr
    }

    /// Holds one slot with a keep-alive connection, then asserts a second
    /// connection is refused rather than served. Shared by the two connection
    /// caps, which both cap the second connection opened from this IP.
    async fn assert_second_connection_refused(addr: SocketAddr) {
        // Reading the 200 back proves the first connection was accepted and its
        // slot taken. The socket then stays open, holding that slot.
        let mut held = TcpStream::connect(addr).await.unwrap();
        held.write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        let mut buf = [0u8; 1024];
        let n = held.read(&mut buf).await.unwrap();
        assert!(String::from_utf8_lossy(&buf[..n]).contains("200 OK"));

        // The second connection is over the cap. Send it a real request: a
        // broken cap would serve it and return a response, failing the test. A
        // working cap drops the socket, so the read ends in EOF or a reset.
        let mut over = TcpStream::connect(addr).await.unwrap();
        // Best-effort: the socket may already be closed by the time we write.
        let _ = over
            .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await;
        let mut buf = [0u8; 64];
        let read = tokio::time::timeout(Duration::from_secs(2), over.read(&mut buf))
            .await
            .expect("over-cap connection should resolve promptly, not hang");
        match read {
            Ok(0) | Err(_) => {} // EOF or reset: the connection was refused.
            Ok(n) => panic!(
                "connection over the cap was served, got {n} bytes: {:?}",
                String::from_utf8_lossy(&buf[..n])
            ),
        }
    }

    /// The caps only hold if both slots come back when a connection ends. A
    /// guard that failed to decrement would pass every other test here and then
    /// refuse all traffic once the cap had been reached one time.
    #[tokio::test]
    async fn closing_a_connection_releases_its_slots() {
        // Cap of 1 on both budgets, so a leaked slot of either kind means the
        // second request can never be served.
        let addr = start_server(Limits {
            max_connections: NonZeroUsize::new(1).unwrap(),
            max_connections_per_ip: NonZeroUsize::new(1).unwrap(),
            ..Default::default()
        })
        .await;

        for attempt in 1..=3 {
            let mut conn = TcpStream::connect(addr).await.unwrap();
            conn.write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                .await
                .unwrap();

            let mut response = String::new();
            tokio::time::timeout(Duration::from_secs(2), conn.read_to_string(&mut response))
                .await
                .unwrap_or_else(|_| panic!("attempt {attempt} hung, a slot was not released"))
                .unwrap();
            assert!(
                response.contains("200 OK"),
                "attempt {attempt} was refused, a slot was not released: {response:?}"
            );

            // Dropped here, which is what has to hand both slots back.
            drop(conn);
        }
    }

    #[tokio::test]
    async fn header_read_timeout_closes_slow_client() {
        let addr = start_server(Limits {
            header_read_timeout: Duration::from_millis(200),
            ..Default::default()
        })
        .await;

        // Slowloris: send a partial request and never finish the header block,
        // so there is no terminating blank line.
        let mut conn = TcpStream::connect(addr).await.unwrap();
        conn.write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\n")
            .await
            .unwrap();

        // With the timeout the server closes it ~200ms in, so the read resolves.
        // Without it the server waits for headers forever and this read hangs;
        // the 2s bound turns that regression into a failure, not a stuck test.
        let mut buf = [0u8; 64];
        let closed = tokio::time::timeout(Duration::from_secs(2), conn.read(&mut buf)).await;
        assert!(
            closed.is_ok(),
            "server should close the slow client, not hang"
        );
    }

    // The request timeout is layered in `build_app`, so its coverage belongs
    // with the router rather than here.

    /// A pooled client sends several requests over one kept-alive connection,
    /// which is how the service is really used: register then login. Each has to
    /// arrive with its own body intact.
    #[tokio::test]
    async fn pooled_requests_keep_their_own_bodies() {
        let addr = start_server(Limits::default()).await;
        let client = reqwest::Client::new();

        for body in ["first", "second", "third"] {
            let res = client
                .post(format!("http://{addr}/echo"))
                .body(body)
                .send()
                .await
                .unwrap();
            assert_eq!(res.status(), 200);
            assert_eq!(res.text().await.unwrap(), body);
        }
    }

    #[tokio::test]
    async fn connection_cap_refuses_over_limit() {
        let addr = start_server(Limits {
            max_connections: NonZeroUsize::new(1).unwrap(),
            ..Default::default()
        })
        .await;
        assert_second_connection_refused(addr).await;
    }

    #[tokio::test]
    async fn per_ip_connection_cap_refuses_over_limit() {
        // Global cap stays high; only the per-IP cap of 1 is under test, and all
        // test connections come from 127.0.0.1.
        let addr = start_server(Limits {
            max_connections_per_ip: NonZeroUsize::new(1).unwrap(),
            ..Default::default()
        })
        .await;
        assert_second_connection_refused(addr).await;
    }

    #[test]
    fn client_key_masks_ipv6_to_64() {
        // Two addresses sharing a /64 collapse to one key, so a client handed a
        // whole /64 cannot spray it to dodge either per-IP budget.
        let first: IpAddr = "2001:db8:1:2::1".parse().unwrap();
        let second: IpAddr = "2001:db8:1:2:ffff:ffff:ffff:ffff".parse().unwrap();
        assert_eq!(client_key(first), client_key(second));

        // A different /64 stays a distinct key.
        let other: IpAddr = "2001:db8:1:3::1".parse().unwrap();
        assert_ne!(client_key(first), client_key(other));
    }
}
