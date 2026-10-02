//! `MockRpcServer` — local HTTP server returning scripted JSON-RPC responses.
//!
//! Bridges integration tests to the `solana_client::RpcClient` surface
//! without touching production code. Construct a `MockRpcServer`, queue
//! the responses the mock should return for each method name, then build
//! a real `RpcClient` pointed at `server.url()` and pass it into whatever
//! operator / pipeline code is under test.
//!
//! # Example
//! ```ignore
//! use test_utils::mock_rpc::{MockRpcServer, Reply};
//!
//! let mock = MockRpcServer::start().await;
//! mock.enqueue("getLatestBlockhash", Reply::result(json!({"value":{"blockhash":"111..1","lastValidBlockHeight":1}})));
//! mock.enqueue("getSignatureStatuses", Reply::result(json!({"value":[null]})));
//! mock.enqueue("getSignatureStatuses", Reply::result(json!({"value":[null]})));
//! mock.enqueue("getSignatureStatuses", Reply::result(json!({"value":[{"slot":1,"confirmations":null,"err":null,"confirmationStatus":"finalized"}]})));
//!
//! let rpc = solana_client::nonblocking::rpc_client::RpcClient::new(mock.url());
//! // ... run operator logic against `rpc`, assert it does 1 submission + 3 polls ...
//! assert_eq!(mock.call_count("getSignatureStatuses"), 3);
//! ```
//!
//! # Design notes
//!
//! * The server accepts JSON-RPC over HTTP POST on `/`. It parses the
//!   request body to extract the `method` field, dequeues the next
//!   scripted `Reply` for that method, and returns it as a standard
//!   JSON-RPC response (`{"jsonrpc":"2.0","id":...,"result":...}` or
//!   `...,"error":...}`). Request id is echoed from the request when
//!   present; this mimics real JSON-RPC 2.0.
//!
//! * If a method has no scripted responses enqueued when called, the
//!   server returns `-32603 Internal error: mock has no script for <method>`.
//!   Tests that want to assert "no unexpected calls" should check
//!   `server.call_count(method) == expected_count` at end.
//!
//! * Timing: the server records the `Instant` of every dispatched reply
//!   per method. `server.call_timestamps(method)` returns the vector,
//!   enabling retry-interval assertions (e.g. `±180ms` bounds).
//!
//! * The server runs in a spawned task; the `MockRpcServer` handle
//!   drops the cancellation token on `shutdown()`. Tests that leak the
//!   handle will see the server continue running until process exit —
//!   harmless but not tidy.

use http_body_util::{BodyExt, Full};
use hyper::{body::Bytes, service::service_fn, Request, Response, StatusCode};
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto::Builder as AutoBuilder,
};
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Instant,
};
use tokio::{net::TcpListener, sync::Notify, task::JoinHandle};

// ── Public API ──────────────────────────────────────────────────────────────

/// Closure type for `Reply::Dynamic` — receives the parsed JSON-RPC request
/// and returns the value to place under `result`.
pub type DynamicHandler = Arc<dyn Fn(&Value) -> Value + Send + Sync>;

/// A scripted JSON-RPC response that the server returns for one call.
#[derive(Clone)]
pub enum Reply {
    /// Success: body is placed under the `result` field of the JSON-RPC envelope.
    Result(Value),
    /// Error: body becomes the JSON-RPC `error` object.
    /// `code` and `message` are the standard JSON-RPC 2.0 error fields;
    /// `data` is an optional extension.
    Error {
        code: i32,
        message: String,
        data: Option<Value>,
    },
    /// Dynamic: the closure inspects the incoming request and produces the
    /// `result` value. Used when the response must depend on the request —
    /// e.g. `sendTransaction` must echo back the signature embedded in the
    /// transaction bytes (real validators do this; the client self-checks).
    Dynamic(DynamicHandler),
}

impl Reply {
    pub fn result(value: Value) -> Self {
        Reply::Result(value)
    }

    pub fn error(code: i32, message: impl Into<String>) -> Self {
        Reply::Error {
            code,
            message: message.into(),
            data: None,
        }
    }

    /// Build a reply whose `result` value is computed from the incoming
    /// JSON-RPC request. The closure receives the full request object
    /// (`{jsonrpc, id, method, params}`) and returns the value to place
    /// under `result`.
    pub fn dynamic<F>(f: F) -> Self
    where
        F: Fn(&Value) -> Value + Send + Sync + 'static,
    {
        Reply::Dynamic(Arc::new(f))
    }
}

/// Handle to a running mock JSON-RPC server.
///
/// The accept loop is aborted when the handle is dropped — even on
/// panic-unwind — so tests that fail before reaching
/// `mock.shutdown().await` don't leak the TCP fd until process exit.
/// Call `shutdown().await` if you want to wait for the task to
/// actually finish.
pub struct MockRpcServer {
    addr: SocketAddr,
    state: Arc<MockState>,
    stop: Arc<Notify>,
    /// `Option` so `shutdown` can `take()` ownership and `await` the
    /// handle without conflicting with this struct's `Drop` impl.
    task: Option<JoinHandle<()>>,
}

impl Drop for MockRpcServer {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

impl MockRpcServer {
    /// Bind to `127.0.0.1:0` and start the dispatcher. Returns when the
    /// server is ready to accept connections.
    pub async fn start() -> Self {
        Self::start_with_state(MockState::default()).await
    }

    /// Like `start`, but a method with no scripted reply is forwarded to
    /// `upstream` instead of failing, so a test can override a few methods of
    /// a real node. Forwarded calls still count in `call_count`.
    pub async fn start_with_upstream(upstream: String) -> Self {
        Self::start_with_state(MockState {
            upstream: Some(upstream),
            ..MockState::default()
        })
        .await
    }

    async fn start_with_state(state: MockState) -> Self {
        let state = Arc::new(state);
        let stop = Arc::new(Notify::new());
        let listener = TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("mock rpc: bind 127.0.0.1:0");
        let addr = listener.local_addr().expect("mock rpc: local_addr");

        let state_for_task = state.clone();
        let stop_for_task = stop.clone();
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = stop_for_task.notified() => return,
                    accepted = listener.accept() => {
                        let (stream, _peer) = match accepted {
                            Ok(conn) => conn,
                            Err(_) => continue,
                        };
                        let state = state_for_task.clone();
                        tokio::spawn(async move {
                            let io = TokioIo::new(stream);
                            let svc = service_fn(move |req| {
                                let state = state.clone();
                                async move { handle_request(state, req).await }
                            });
                            let _ = AutoBuilder::new(TokioExecutor::new())
                                .serve_connection(io, svc)
                                .await;
                        });
                    }
                }
            }
        });

        Self {
            addr,
            state,
            stop,
            task: Some(task),
        }
    }

    /// Base URL suitable for `RpcClient::new(mock.url())`.
    pub fn url(&self) -> String {
        format!("http://{}/", self.addr)
    }

    /// Enqueue a reply the server will return for the next call to `method`.
    /// Multiple replies for the same method are served in FIFO order.
    pub fn enqueue(&self, method: impl Into<String>, reply: Reply) {
        self.state
            .scripts
            .lock()
            .unwrap()
            .entry(method.into())
            .or_default()
            .push(reply);
    }

    /// Convenience: enqueue several replies for the same method at once.
    pub fn enqueue_sequence<I>(&self, method: impl Into<String>, replies: I)
    where
        I: IntoIterator<Item = Reply>,
    {
        let method = method.into();
        let mut scripts = self.state.scripts.lock().unwrap();
        let entry = scripts.entry(method).or_default();
        for r in replies {
            entry.push(r);
        }
    }

    /// Number of times a given method has been invoked on this server.
    pub fn call_count(&self, method: &str) -> usize {
        self.state
            .calls
            .lock()
            .unwrap()
            .get(method)
            .copied()
            .unwrap_or(0)
    }

    /// Timestamps of every dispatched reply for `method`. Empty if
    /// the method has never been called. Enables timing assertions
    /// (e.g., "the 2nd and 3rd poll are at least 200 ms apart").
    pub fn call_timestamps(&self, method: &str) -> Vec<Instant> {
        self.state
            .timestamps
            .lock()
            .unwrap()
            .get(method)
            .cloned()
            .unwrap_or_default()
    }

    /// Remaining scripted replies for a method (useful for asserting
    /// that a test consumed exactly what it scripted).
    pub fn remaining_scripted(&self, method: &str) -> usize {
        self.state
            .scripts
            .lock()
            .unwrap()
            .get(method)
            .map(Vec::len)
            .unwrap_or(0)
    }

    /// Stop the server and wait for the accept loop to exit.
    ///
    /// `Notify::notify_waiters` does not buffer a permit, so if the accept
    /// loop hasn't yet polled `notified()` at the moment shutdown fires
    /// (common when no requests were ever served), the notification is
    /// lost and the loop sits forever in `listener.accept()`. We abort the
    /// task as a safety net — graceful shutdown isn't a contract we need
    /// to honour for a mock.
    pub async fn shutdown(mut self) {
        self.stop.notify_waiters();
        if let Some(task) = self.task.take() {
            task.abort();
            let _ = task.await; // swallows the JoinError from abort
        }
        // The `Drop` impl below sees `task = None` after this and is a no-op.
    }
}

// ── Internals ──────────────────────────────────────────────────────────────

#[derive(Default)]
struct MockState {
    /// FIFO queue of scripted replies per JSON-RPC method.
    scripts: Mutex<HashMap<String, Vec<Reply>>>,
    /// Total call count per method (for `call_count`).
    calls: Mutex<HashMap<String, usize>>,
    /// Timestamp of each dispatch per method (for timing assertions).
    timestamps: Mutex<HashMap<String, Vec<Instant>>>,
    /// Node that answers unscripted methods, if any.
    upstream: Option<String>,
}

async fn handle_request(
    state: Arc<MockState>,
    req: Request<hyper::body::Incoming>,
) -> Result<Response<Full<Bytes>>, hyper::Error> {
    // Read the full request body (bounded; tests never send >64 KiB).
    let body_bytes = match req.collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(_) => {
            return Ok(http_error(
                StatusCode::BAD_REQUEST,
                -32700,
                "parse error (unreadable body)",
            ));
        }
    };

    // Parse incoming JSON-RPC envelope. Support both single calls and
    // batches (batches get serialised per-call and wrapped in an array).
    let req_value: Value = match serde_json::from_slice(&body_bytes) {
        Ok(v) => v,
        Err(_) => {
            return Ok(http_error(
                StatusCode::OK,
                -32700,
                "parse error (not valid JSON)",
            ));
        }
    };

    let response = if let Some(arr) = req_value.as_array() {
        let mut replies = Vec::with_capacity(arr.len());
        for req in arr {
            replies.push(reply_to(&state, req).await);
        }
        serde_json::Value::Array(replies)
    } else {
        reply_to(&state, &req_value).await
    };

    let body = serde_json::to_vec(&response).expect("serialize mock response");
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/json")
        .body(Full::new(Bytes::from(body)))
        .expect("response builder"))
}

/// The scripted reply, else the upstream's, else the no-script error.
async fn reply_to(state: &MockState, req: &Value) -> Value {
    if let Some(reply) = dispatch(state, req) {
        return reply;
    }
    let id = req.get("id").cloned().unwrap_or(json!(null));
    let method = req
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or("<missing>");
    if let Some(upstream) = &state.upstream {
        let forwarded = async {
            reqwest::Client::new()
                .post(upstream)
                .json(req)
                .send()
                .await?
                .json::<Value>()
                .await
        };
        return match forwarded.await {
            Ok(reply) => reply,
            Err(e) => json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": {
                    "code": -32603,
                    "message": format!("mock rpc: upstream failed for method `{method}`: {e}"),
                },
            }),
        };
    }
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {
            "code": -32603,
            "message": format!("mock rpc: no scripted response for method `{}`", method),
        },
    })
}

/// Record the call and pop its scripted reply, or `None` when nothing is scripted.
fn dispatch(state: &MockState, req: &Value) -> Option<Value> {
    let method = req
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or("<missing>");
    let id = req.get("id").cloned().unwrap_or(json!(null));

    // Record the call and timestamp.
    state
        .calls
        .lock()
        .unwrap()
        .entry(method.to_string())
        .and_modify(|c| *c += 1)
        .or_insert(1);
    state
        .timestamps
        .lock()
        .unwrap()
        .entry(method.to_string())
        .or_default()
        .push(Instant::now());

    // Pop the next scripted reply for this method.
    let mut scripts = state.scripts.lock().unwrap();
    let reply = scripts.get_mut(method).and_then(|q| {
        if q.is_empty() {
            None
        } else {
            Some(q.remove(0))
        }
    });

    let reply = reply?;
    Some(match reply {
        Reply::Result(value) => json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": value,
        }),
        Reply::Dynamic(handler) => {
            let value = handler(req);
            json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": value,
            })
        }
        Reply::Error {
            code,
            message,
            data,
        } => {
            let mut err_obj = json!({"code": code, "message": message});
            if let Some(d) = data {
                err_obj["data"] = d;
            }
            json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": err_obj,
            })
        }
    })
}

fn http_error(status: StatusCode, code: i32, message: &str) -> Response<Full<Bytes>> {
    let body = json!({
        "jsonrpc": "2.0",
        "id": null,
        "error": { "code": code, "message": message },
    });
    Response::builder()
        .status(status)
        .header("Content-Type", "application/json")
        .body(Full::new(Bytes::from(serde_json::to_vec(&body).unwrap())))
        .expect("response builder")
}

// ── Unit tests ─────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    async fn post(url: &str, body: &Value) -> Value {
        let client = reqwest::Client::new();
        let resp = client
            .post(url)
            .json(body)
            .send()
            .await
            .expect("post")
            .json::<Value>()
            .await
            .expect("parse");
        resp
    }

    #[tokio::test]
    async fn returns_scripted_result() {
        let mock = MockRpcServer::start().await;
        mock.enqueue("getSlot", Reply::result(json!(42)));

        let resp = post(
            &mock.url(),
            &json!({"jsonrpc":"2.0","id":1,"method":"getSlot"}),
        )
        .await;
        assert_eq!(resp["result"], json!(42));
        assert_eq!(resp["id"], json!(1));
        assert_eq!(mock.call_count("getSlot"), 1);
        assert_eq!(mock.remaining_scripted("getSlot"), 0);
    }

    #[tokio::test]
    async fn returns_error_when_no_script() {
        let mock = MockRpcServer::start().await;
        let resp = post(
            &mock.url(),
            &json!({"jsonrpc":"2.0","id":7,"method":"getSlot"}),
        )
        .await;
        assert_eq!(resp["error"]["code"], json!(-32603));
        assert!(resp["error"]["message"]
            .as_str()
            .unwrap()
            .contains("getSlot"));
    }

    #[tokio::test]
    async fn serves_fifo_sequence() {
        let mock = MockRpcServer::start().await;
        mock.enqueue_sequence(
            "getX",
            vec![
                Reply::result(json!(1)),
                Reply::result(json!(2)),
                Reply::result(json!(3)),
            ],
        );
        for expected in [1, 2, 3] {
            let resp = post(
                &mock.url(),
                &json!({"jsonrpc":"2.0","id":expected,"method":"getX"}),
            )
            .await;
            assert_eq!(resp["result"], json!(expected));
        }
        assert_eq!(mock.call_count("getX"), 3);
    }

    #[tokio::test]
    async fn records_timestamps_in_order() {
        let mock = MockRpcServer::start().await;
        mock.enqueue_sequence(
            "getY",
            vec![Reply::result(json!(1)), Reply::result(json!(2))],
        );
        let _r1 = post(
            &mock.url(),
            &json!({"jsonrpc":"2.0","id":1,"method":"getY"}),
        )
        .await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let _r2 = post(
            &mock.url(),
            &json!({"jsonrpc":"2.0","id":2,"method":"getY"}),
        )
        .await;

        let stamps = mock.call_timestamps("getY");
        assert_eq!(stamps.len(), 2);
        let gap = stamps[1].duration_since(stamps[0]);
        assert!(
            gap >= std::time::Duration::from_millis(40),
            "gap should be ~50ms, got {gap:?}"
        );
    }

    #[tokio::test]
    async fn forwards_unscripted_methods_to_upstream() {
        let upstream = MockRpcServer::start().await;
        upstream.enqueue("getSlot", Reply::result(json!(7)));
        let mock = MockRpcServer::start_with_upstream(upstream.url()).await;
        mock.enqueue("getX", Reply::result(json!(1)));

        let scripted = post(
            &mock.url(),
            &json!({"jsonrpc":"2.0","id":1,"method":"getX"}),
        )
        .await;
        assert_eq!(scripted["result"], json!(1));
        let forwarded = post(
            &mock.url(),
            &json!({"jsonrpc":"2.0","id":2,"method":"getSlot"}),
        )
        .await;
        assert_eq!(forwarded["result"], json!(7));
        assert_eq!(forwarded["id"], json!(2));
        assert_eq!(mock.call_count("getSlot"), 1);
        assert_eq!(upstream.call_count("getSlot"), 1);
        assert_eq!(upstream.call_count("getX"), 0);
    }
}
