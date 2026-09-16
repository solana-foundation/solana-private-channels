//! `MockYellowstoneServer` — local stub for a Yellowstone Geyser gRPC
//! endpoint. Lets integration tests script `SubscribeUpdate` streams and
//! unary responses for the `YellowstoneSource` datasource without needing a
//! real Yellowstone node.
//!
//! # Shape
//!
//! API mirrors `mock_rpc::MockRpcServer` — same verbs (`start`, `url`,
//! `enqueue`, `enqueue_sequence`, `call_count`, `call_timestamps`,
//! `remaining_scripted`, `shutdown`) so the two mocks feel identical.
//!
//! # What it implements
//!
//! * Full `yellowstone_grpc_proto::geyser::geyser_server::Geyser` trait
//!   (subscribe + 7 unary methods).
//! * `subscribe`: returns a `ReceiverStream` fed by a scripted FIFO queue
//!   of `Update` entries. Tests push updates, server emits them.
//! * Unary methods: scripted-reply FIFO keyed by method name, mirroring
//!   `MockRpcServer::enqueue`.
//! * Malformed-bytes variant (`Update::Malformed`) drops the stream mid-flight
//!   by returning a `tonic::Status` error — exercises the defensive branches
//!   in `YellowstoneSource` without needing byte-level protocol corruption.
//! * `drop_stream()` helper closes the active subscribe stream to simulate a
//!   mid-subscription disconnect (used by the reconnect-gap recovery test).
//!
//! Feature-gated on `test-mock-yellowstone` so this module (and its tonic
//! + proto deps) is a no-op unless explicitly requested.

use futures::Stream;
use std::{
    collections::HashMap,
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Instant,
};
use tokio::{
    net::TcpListener,
    sync::{mpsc, Notify},
    task::JoinHandle,
};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status, Streaming};
use yellowstone_grpc_proto::geyser::{
    geyser_server::{Geyser, GeyserServer},
    GetBlockHeightRequest, GetBlockHeightResponse, GetLatestBlockhashRequest,
    GetLatestBlockhashResponse, GetSlotRequest, GetSlotResponse, GetVersionRequest,
    GetVersionResponse, IsBlockhashValidRequest, IsBlockhashValidResponse, PingRequest,
    PongResponse, SubscribeDeshredRequest, SubscribeGossipRequest, SubscribeReplayInfoRequest,
    SubscribeReplayInfoResponse, SubscribeRequest, SubscribeUpdate, SubscribeUpdateDeshred,
    SubscribeUpdateGossip,
};

// ── Public API ──────────────────────────────────────────────────────────────

/// A scripted entry the server should emit on the `subscribe` stream.
#[derive(Debug, Clone)]
pub enum Update {
    /// Well-formed `SubscribeUpdate` — forwarded to the client as
    /// `Ok(update)`.
    Ok(Box<SubscribeUpdate>),
    /// Returns `Err(Status::invalid_argument(reason))` on the stream.
    /// Models a corrupt / malformed update that gRPC layer surfaces as a
    /// stream error. The indexer's production contract on such errors is
    /// to `error!()` + reconnect — see `source.rs::connect_and_stream`.
    Malformed(String),
}

impl Update {
    /// Shorthand for `Update::Ok(Box::new(u))`.
    pub fn ok(u: SubscribeUpdate) -> Self {
        Update::Ok(Box::new(u))
    }

    /// Shorthand for `Update::Malformed(reason.into())`.
    pub fn malformed(reason: impl Into<String>) -> Self {
        Update::Malformed(reason.into())
    }
}

/// Placeholder matcher for future request-shape filtering. Kept as a unit
/// type for API symmetry with `mock_rpc` and forward-compat.
#[derive(Debug, Clone, Default)]
pub struct UpdateMatcher;

/// Scripted reply for a unary gRPC method on the stub.
#[derive(Debug, Clone)]
pub enum UnaryReply {
    GetLatestBlockhash(GetLatestBlockhashResponse),
    GetBlockHeight(GetBlockHeightResponse),
    GetSlot(GetSlotResponse),
    IsBlockhashValid(IsBlockhashValidResponse),
    GetVersion(GetVersionResponse),
    Pong(PongResponse),
    ReplayInfo(SubscribeReplayInfoResponse),
    Error { code: tonic::Code, message: String },
}

/// Handle to a running mock Yellowstone server.
pub struct MockYellowstoneServer {
    addr: SocketAddr,
    state: Arc<MockState>,
    stop: Arc<Notify>,
    task: JoinHandle<()>,
}

impl MockYellowstoneServer {
    /// Bind to `127.0.0.1:0` and start serving. Returns when the server is
    /// ready to accept connections.
    pub async fn start() -> Self {
        let state = Arc::new(MockState::default());

        let listener = TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("mock yellowstone: bind 127.0.0.1:0");
        let addr = listener.local_addr().expect("mock yellowstone: local_addr");

        let stop = Arc::new(Notify::new());
        let stop_for_task = stop.clone();

        let service = MockGeyserService {
            state: state.clone(),
        };

        let listener_stream = tokio_stream::wrappers::TcpListenerStream::new(listener);

        let task = tokio::spawn(async move {
            let shutdown = async move {
                stop_for_task.notified().await;
            };
            let _ = tonic::transport::Server::builder()
                .add_service(GeyserServer::new(service))
                .serve_with_incoming_shutdown(listener_stream, shutdown)
                .await;
        });

        Self {
            addr,
            state,
            stop,
            task,
        }
    }

    /// gRPC endpoint string usable by `GeyserGrpcClient::build_from_shared(...)`.
    pub fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// Alias matching the scaffold's original `.endpoint()` naming.
    pub fn endpoint(&self) -> String {
        self.url()
    }

    /// Enqueue one `Update` to be emitted on the next `subscribe` call's
    /// stream (or the active stream if one is already open). FIFO.
    pub fn enqueue(&self, _matcher: UpdateMatcher, update: Update) {
        self.state.subscribe_queue.lock().unwrap().push(update);
        // Wake any active stream so it can pick up the new entry.
        self.state.subscribe_notify.notify_waiters();
    }

    /// Convenience: enqueue several updates at once.
    pub fn enqueue_sequence<I>(&self, updates: I)
    where
        I: IntoIterator<Item = Update>,
    {
        {
            let mut queue = self.state.subscribe_queue.lock().unwrap();
            for u in updates {
                queue.push(u);
            }
        }
        self.state.subscribe_notify.notify_waiters();
    }

    /// Enqueue a reply for a unary method. `method` must match one of:
    /// `"ping"`, `"get_latest_blockhash"`, `"get_block_height"`,
    /// `"get_slot"`, `"is_blockhash_valid"`, `"get_version"`,
    /// `"subscribe_replay_info"`.
    pub fn enqueue_unary(&self, method: impl Into<String>, reply: UnaryReply) {
        self.state
            .unary_scripts
            .lock()
            .unwrap()
            .entry(method.into())
            .or_default()
            .push(reply);
    }

    /// Number of times a given gRPC method has been invoked.
    ///
    /// For the streaming method pass `"subscribe"` — the count increments
    /// once per successful `subscribe` RPC handshake (i.e. once per client
    /// connection to the streaming endpoint).
    pub fn call_count(&self, method: &str) -> usize {
        self.state
            .calls
            .lock()
            .unwrap()
            .get(method)
            .copied()
            .unwrap_or(0)
    }

    /// Timestamps of each dispatched unary reply, or each `subscribe` RPC
    /// handshake, for the named method.
    pub fn call_timestamps(&self, method: &str) -> Vec<Instant> {
        self.state
            .timestamps
            .lock()
            .unwrap()
            .get(method)
            .cloned()
            .unwrap_or_default()
    }

    /// Remaining scripted `Update` entries not yet consumed by a
    /// `subscribe` stream.
    pub fn remaining_scripted(&self) -> usize {
        self.state.subscribe_queue.lock().unwrap().len()
    }

    /// Remaining scripted unary replies for a method.
    pub fn remaining_unary(&self, method: &str) -> usize {
        self.state
            .unary_scripts
            .lock()
            .unwrap()
            .get(method)
            .map(Vec::len)
            .unwrap_or(0)
    }

    /// Force the currently-open subscribe stream (if any) to close. New
    /// connections will reopen a fresh stream. Simulates a Yellowstone
    /// disconnect mid-subscription.
    pub fn drop_stream(&self) {
        self.state
            .stream_cancel
            .load(std::sync::atomic::Ordering::SeqCst);
        // Swap the cancel token: cancel the live one, install a fresh one
        // for the next subscription.
        let new_token = CancellationToken::new();
        let mut guard = self.state.current_stream_token.lock().unwrap();
        if let Some(old) = guard.take() {
            old.cancel();
        }
        *guard = Some(new_token);
    }

    /// Shut down the server and wait for graceful teardown.
    ///
    /// Cancels any active subscribe stream first so the pump task exits
    /// and tonic's graceful-shutdown can drain the active RPC. Without
    /// the cancellation, tonic waits for the subscribe stream to end,
    /// the stream waits for the pump task, and the pump task is idle
    /// waiting on `subscribe_notify` — deadlock.
    pub async fn shutdown(self) {
        if let Some(t) = self.state.current_stream_token.lock().unwrap().take() {
            t.cancel();
        }
        self.stop.notify_waiters();
        let _ = self.task.await;
    }
}

// ── Internals ──────────────────────────────────────────────────────────────

#[derive(Default)]
struct MockState {
    /// FIFO queue of scripted updates for `subscribe`.
    subscribe_queue: Mutex<Vec<Update>>,
    /// Notify waiters (the subscribe task) when new entries arrive.
    subscribe_notify: Notify,
    /// Scripted unary replies keyed by method name.
    unary_scripts: Mutex<HashMap<String, Vec<UnaryReply>>>,
    /// Per-method call counters.
    calls: Mutex<HashMap<String, usize>>,
    /// Timestamp per dispatched call.
    timestamps: Mutex<HashMap<String, Vec<Instant>>>,
    /// Cancellation token for the in-flight subscribe stream.
    current_stream_token: Mutex<Option<CancellationToken>>,
    /// Unused atomic placeholder — kept so `drop_stream` can be extended.
    stream_cancel: std::sync::atomic::AtomicBool,
}

impl MockState {
    fn record_call(&self, method: &str) {
        self.calls
            .lock()
            .unwrap()
            .entry(method.to_string())
            .and_modify(|c| *c += 1)
            .or_insert(1);
        self.timestamps
            .lock()
            .unwrap()
            .entry(method.to_string())
            .or_default()
            .push(Instant::now());
    }

    fn pop_unary(&self, method: &str) -> Option<UnaryReply> {
        let mut scripts = self.unary_scripts.lock().unwrap();
        scripts.get_mut(method).and_then(|q| {
            if q.is_empty() {
                None
            } else {
                Some(q.remove(0))
            }
        })
    }
}

struct MockGeyserService {
    state: Arc<MockState>,
}

type SubscribeStream =
    Pin<Box<dyn Stream<Item = Result<SubscribeUpdate, Status>> + Send + 'static>>;

// `subscribe_deshred` was added to the Geyser trait in yellowstone-grpc-proto
// v12. The indexer does not consume this RPC, so the mock stubs it out with
// `Unimplemented` and an empty stream type to satisfy the trait bound.
type SubscribeDeshredStream =
    Pin<Box<dyn Stream<Item = Result<SubscribeUpdateDeshred, Status>> + Send + 'static>>;

// `subscribe_gossip` arrived alongside it and is stubbed for the same reason.
type SubscribeGossipStream =
    Pin<Box<dyn Stream<Item = Result<SubscribeUpdateGossip, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl Geyser for MockGeyserService {
    type SubscribeStream = SubscribeStream;
    type SubscribeDeshredStream = SubscribeDeshredStream;
    type SubscribeGossipStream = SubscribeGossipStream;

    async fn subscribe_deshred(
        &self,
        _request: Request<Streaming<SubscribeDeshredRequest>>,
    ) -> Result<Response<Self::SubscribeDeshredStream>, Status> {
        Err(Status::unimplemented(
            "subscribe_deshred is not implemented by the mock",
        ))
    }

    async fn subscribe_gossip(
        &self,
        _request: Request<SubscribeGossipRequest>,
    ) -> Result<Response<Self::SubscribeGossipStream>, Status> {
        Err(Status::unimplemented(
            "subscribe_gossip is not implemented by the mock",
        ))
    }

    async fn subscribe(
        &self,
        _request: Request<Streaming<SubscribeRequest>>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        self.state.record_call("subscribe");

        let (tx, rx) = mpsc::channel::<Result<SubscribeUpdate, Status>>(32);

        let token = CancellationToken::new();
        {
            let mut guard = self.state.current_stream_token.lock().unwrap();
            // Cancel any pre-existing active stream (shouldn't normally
            // happen, but be defensive).
            if let Some(old) = guard.take() {
                old.cancel();
            }
            *guard = Some(token.clone());
        }

        let state = self.state.clone();

        // Pump scripted updates into the channel. Runs until:
        //   (a) the cancellation token fires (drop_stream() called), or
        //   (b) the receiver is gone (client disconnected), or
        //   (c) a malformed update is popped (send the Status error and
        //       then exit — matching real Yellowstone's "stream ends on
        //       first protocol error" behaviour).
        tokio::spawn(async move {
            loop {
                if token.is_cancelled() {
                    return;
                }

                // Pop one entry if available.
                let next = {
                    let mut q = state.subscribe_queue.lock().unwrap();
                    if q.is_empty() {
                        None
                    } else {
                        Some(q.remove(0))
                    }
                };

                match next {
                    Some(Update::Ok(update)) => {
                        if tx.send(Ok(*update)).await.is_err() {
                            return; // client gone
                        }
                    }
                    Some(Update::Malformed(reason)) => {
                        let _ = tx.send(Err(Status::invalid_argument(reason))).await;
                        return;
                    }
                    None => {
                        // Nothing to emit — wait for an enqueue, a
                        // per-stream cancellation, OR the receiver being
                        // dropped (client disconnect / server shutdown).
                        // Without the `tx.closed()` arm the task leaks
                        // when the stream is abandoned, which blocks
                        // tonic's graceful shutdown forever.
                        tokio::select! {
                            _ = state.subscribe_notify.notified() => {}
                            _ = token.cancelled() => return,
                            _ = tx.closed() => return,
                        }
                    }
                }
            }
        });

        let stream: SubscribeStream = Box::pin(ReceiverStream::new(rx));
        Ok(Response::new(stream))
    }

    async fn subscribe_replay_info(
        &self,
        _request: Request<SubscribeReplayInfoRequest>,
    ) -> Result<Response<SubscribeReplayInfoResponse>, Status> {
        self.state.record_call("subscribe_replay_info");
        match self.state.pop_unary("subscribe_replay_info") {
            Some(UnaryReply::ReplayInfo(r)) => Ok(Response::new(r)),
            Some(UnaryReply::Error { code, message }) => Err(Status::new(code, message)),
            Some(_) => Err(Status::internal(
                "wrong reply variant for subscribe_replay_info",
            )),
            None => Ok(Response::new(SubscribeReplayInfoResponse::default())),
        }
    }

    async fn ping(&self, _request: Request<PingRequest>) -> Result<Response<PongResponse>, Status> {
        self.state.record_call("ping");
        match self.state.pop_unary("ping") {
            Some(UnaryReply::Pong(p)) => Ok(Response::new(p)),
            Some(UnaryReply::Error { code, message }) => Err(Status::new(code, message)),
            Some(_) => Err(Status::internal("wrong reply variant for ping")),
            None => Ok(Response::new(PongResponse { count: 0 })),
        }
    }

    async fn get_latest_blockhash(
        &self,
        _request: Request<GetLatestBlockhashRequest>,
    ) -> Result<Response<GetLatestBlockhashResponse>, Status> {
        self.state.record_call("get_latest_blockhash");
        match self.state.pop_unary("get_latest_blockhash") {
            Some(UnaryReply::GetLatestBlockhash(r)) => Ok(Response::new(r)),
            Some(UnaryReply::Error { code, message }) => Err(Status::new(code, message)),
            Some(_) => Err(Status::internal("wrong reply variant")),
            None => Ok(Response::new(GetLatestBlockhashResponse::default())),
        }
    }

    async fn get_block_height(
        &self,
        _request: Request<GetBlockHeightRequest>,
    ) -> Result<Response<GetBlockHeightResponse>, Status> {
        self.state.record_call("get_block_height");
        match self.state.pop_unary("get_block_height") {
            Some(UnaryReply::GetBlockHeight(r)) => Ok(Response::new(r)),
            Some(UnaryReply::Error { code, message }) => Err(Status::new(code, message)),
            Some(_) => Err(Status::internal("wrong reply variant")),
            None => Ok(Response::new(GetBlockHeightResponse::default())),
        }
    }

    async fn get_slot(
        &self,
        _request: Request<GetSlotRequest>,
    ) -> Result<Response<GetSlotResponse>, Status> {
        self.state.record_call("get_slot");
        match self.state.pop_unary("get_slot") {
            Some(UnaryReply::GetSlot(r)) => Ok(Response::new(r)),
            Some(UnaryReply::Error { code, message }) => Err(Status::new(code, message)),
            Some(_) => Err(Status::internal("wrong reply variant")),
            None => Ok(Response::new(GetSlotResponse::default())),
        }
    }

    async fn is_blockhash_valid(
        &self,
        _request: Request<IsBlockhashValidRequest>,
    ) -> Result<Response<IsBlockhashValidResponse>, Status> {
        self.state.record_call("is_blockhash_valid");
        match self.state.pop_unary("is_blockhash_valid") {
            Some(UnaryReply::IsBlockhashValid(r)) => Ok(Response::new(r)),
            Some(UnaryReply::Error { code, message }) => Err(Status::new(code, message)),
            Some(_) => Err(Status::internal("wrong reply variant")),
            None => Ok(Response::new(IsBlockhashValidResponse::default())),
        }
    }

    async fn get_version(
        &self,
        _request: Request<GetVersionRequest>,
    ) -> Result<Response<GetVersionResponse>, Status> {
        self.state.record_call("get_version");
        match self.state.pop_unary("get_version") {
            Some(UnaryReply::GetVersion(r)) => Ok(Response::new(r)),
            Some(UnaryReply::Error { code, message }) => Err(Status::new(code, message)),
            Some(_) => Err(Status::internal("wrong reply variant")),
            None => Ok(Response::new(GetVersionResponse {
                version: "mock-0.0.0".to_string(),
            })),
        }
    }
}

// ── Unit tests ─────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use yellowstone_grpc_proto::geyser::{
        subscribe_update::UpdateOneof, SubscribeUpdate, SubscribeUpdateBlockMeta,
    };

    fn install_crypto_provider() {
        // `rustls` (pulled in transitively by tonic) needs a crypto backend
        // installed exactly once per process. This unit module is the only
        // place that spins up gRPC clients in this crate, so installing in
        // each test is fine — subsequent calls are no-ops.
        let _ = rustls::crypto::ring::default_provider().install_default();
    }

    fn block_meta_update(slot: u64) -> SubscribeUpdate {
        SubscribeUpdate {
            filters: vec!["all_blocks_meta".to_string()],
            update_oneof: Some(UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
                slot,
                blockhash: format!("hash-{slot}"),
                ..Default::default()
            })),
            created_at: None,
        }
    }

    #[tokio::test]
    async fn binds_and_shuts_down() {
        let server = MockYellowstoneServer::start().await;
        let url = server.url();
        assert!(url.starts_with("http://127.0.0.1:"));
        // TCP-level check: port is listening.
        let _ = tokio::net::TcpStream::connect(&url.trim_start_matches("http://").to_string())
            .await
            .expect("port should accept TCP connections");
        server.shutdown().await;
    }

    #[tokio::test]
    async fn subscribe_delivers_fifo() {
        use futures::StreamExt;
        use yellowstone_grpc_client::GeyserGrpcClient;
        use yellowstone_grpc_proto::geyser::SubscribeRequest;

        install_crypto_provider();
        let server = MockYellowstoneServer::start().await;
        server.enqueue(UpdateMatcher, Update::ok(block_meta_update(100)));
        server.enqueue(UpdateMatcher, Update::ok(block_meta_update(101)));
        server.enqueue(UpdateMatcher, Update::ok(block_meta_update(102)));

        // Plain HTTP endpoint — don't configure TLS (the real indexer does
        // via `with_native_roots`; a plain connect is fine here).
        let mut client = tokio::time::timeout(
            Duration::from_secs(5),
            GeyserGrpcClient::build_from_shared(server.url())
                .unwrap()
                .connect(),
        )
        .await
        .expect("connect timed out")
        .expect("connect");

        let (_tx, mut stream) = tokio::time::timeout(
            Duration::from_secs(5),
            client.subscribe_with_request(Some(SubscribeRequest::default())),
        )
        .await
        .expect("subscribe timed out")
        .expect("subscribe");

        let mut slots = vec![];
        for _ in 0..3 {
            let msg = tokio::time::timeout(Duration::from_secs(3), stream.next())
                .await
                .expect("timeout")
                .expect("stream ended")
                .expect("stream error");
            if let Some(UpdateOneof::BlockMeta(bm)) = msg.update_oneof {
                slots.push(bm.slot);
            }
        }
        assert_eq!(slots, vec![100, 101, 102]);
        assert_eq!(server.remaining_scripted(), 0);
        assert_eq!(server.call_count("subscribe"), 1);

        server.shutdown().await;
    }

    #[tokio::test]
    async fn unary_fifo_works() {
        use yellowstone_grpc_client::GeyserGrpcClient;

        install_crypto_provider();
        let server = MockYellowstoneServer::start().await;
        server.enqueue_unary(
            "get_slot",
            UnaryReply::GetSlot(GetSlotResponse { slot: 42 }),
        );
        server.enqueue_unary(
            "get_slot",
            UnaryReply::GetSlot(GetSlotResponse { slot: 43 }),
        );

        let mut client = GeyserGrpcClient::build_from_shared(server.url())
            .unwrap()
            .connect()
            .await
            .expect("connect");

        let s1 = client
            .get_slot(Some(
                yellowstone_grpc_proto::geyser::CommitmentLevel::Processed,
            ))
            .await
            .expect("get_slot 1");
        let s2 = client
            .get_slot(Some(
                yellowstone_grpc_proto::geyser::CommitmentLevel::Processed,
            ))
            .await
            .expect("get_slot 2");

        assert_eq!(s1.slot, 42);
        assert_eq!(s2.slot, 43);
        assert_eq!(server.call_count("get_slot"), 2);
        assert_eq!(server.remaining_unary("get_slot"), 0);

        server.shutdown().await;
    }
}
