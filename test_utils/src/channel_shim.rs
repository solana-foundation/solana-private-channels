//! Test-only channel stand-in for harnesses where a Solana validator plays the channel.
//! It forwards every call and answers `getSignatureStatusSnapshot` from the validator's height,
//! statuses and floor, read in that order, which one forward-only validator keeps consistent.

use http_body_util::{BodyExt, Full};
use hyper::{body::Bytes, service::service_fn, Request, Response};
use hyper_util::{
    client::legacy::Client,
    rt::{TokioExecutor, TokioIo},
    server::conn::auto::Builder as AutoBuilder,
};
use serde_json::{json, Value};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_commitment_config::CommitmentConfig;
use solana_sdk::signature::Signature;
use std::{convert::Infallible, net::SocketAddr, str::FromStr, sync::Arc};
use tokio::{net::TcpListener, task::JoinHandle};

/// A running shim. Dropping it stops the server.
pub struct ChannelShim {
    addr: SocketAddr,
    task: JoinHandle<()>,
}

impl ChannelShim {
    /// Start a shim in front of `upstream_url` on a free local port.
    pub async fn start(upstream_url: &str) -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("channel shim: bind 127.0.0.1:0");
        let addr = listener.local_addr().expect("channel shim: local_addr");
        let upstream = Arc::new(Upstream {
            url: upstream_url.to_string(),
            rpc: RpcClient::new_with_commitment(
                upstream_url.to_string(),
                CommitmentConfig::confirmed(),
            ),
            http: Client::builder(TokioExecutor::new()).build_http(),
        });
        let task = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    continue;
                };
                let upstream = upstream.clone();
                tokio::spawn(async move {
                    let svc = service_fn(move |req| {
                        let upstream = upstream.clone();
                        async move { Ok::<_, Infallible>(upstream.handle(req).await) }
                    });
                    let _ = AutoBuilder::new(TokioExecutor::new())
                        .serve_connection(TokioIo::new(stream), svc)
                        .await;
                });
            }
        });
        Self { addr, task }
    }

    /// Base URL to hand to the operator as its channel endpoint.
    pub fn url(&self) -> String {
        format!("http://{}/", self.addr)
    }
}

impl Drop for ChannelShim {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct Upstream {
    url: String,
    rpc: RpcClient,
    http: Client<hyper_util::client::legacy::connect::HttpConnector, Full<Bytes>>,
}

impl Upstream {
    async fn handle(&self, req: Request<hyper::body::Incoming>) -> Response<Full<Bytes>> {
        let body = match req.into_body().collect().await {
            Ok(collected) => collected.to_bytes(),
            Err(e) => return json_response(rpc_error(Value::Null, &format!("read body: {e}"))),
        };
        let parsed: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
        if parsed.get("method").and_then(Value::as_str) == Some("getSignatureStatusSnapshot") {
            let id = parsed.get("id").cloned().unwrap_or(Value::Null);
            return json_response(match self.snapshot(&parsed).await {
                Ok(result) => json!({"jsonrpc": "2.0", "id": id, "result": result}),
                Err(message) => rpc_error(id, &message),
            });
        }
        self.forward(body).await
    }

    async fn snapshot(&self, request: &Value) -> Result<Value, String> {
        let signatures = request
            .pointer("/params/0")
            .and_then(Value::as_array)
            .ok_or("missing signature list")?
            .iter()
            .map(|s| {
                s.as_str()
                    .and_then(|s| Signature::from_str(s).ok())
                    .ok_or_else(|| "Invalid signature".to_string())
            })
            .collect::<Result<Vec<_>, _>>()?;
        let height = self
            .rpc
            .get_block_height()
            .await
            .map_err(|e| e.to_string())?;
        let statuses = self
            .rpc
            .get_signature_statuses_with_history(&signatures)
            .await
            .map_err(|e| e.to_string())?
            .value;
        let floor = self
            .rpc
            .get_first_available_block()
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!({"blockHeight": height, "firstAvailableBlock": floor, "value": statuses}))
    }

    async fn forward(&self, body: Bytes) -> Response<Full<Bytes>> {
        let request = Request::post(&self.url)
            .header("content-type", "application/json")
            .body(Full::new(body))
            .expect("channel shim: build upstream request");
        match self.http.request(request).await {
            Ok(response) => {
                let (parts, incoming) = response.into_parts();
                let bytes = incoming
                    .collect()
                    .await
                    .map(|c| c.to_bytes())
                    .unwrap_or_default();
                Response::from_parts(parts, Full::new(bytes))
            }
            Err(e) => json_response(rpc_error(Value::Null, &format!("upstream: {e}"))),
        }
    }
}

fn rpc_error(id: Value, message: &str) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "error": {"code": -32000, "message": message}})
}

fn json_response(body: Value) -> Response<Full<Bytes>> {
    Response::builder()
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(body.to_string())))
        .expect("channel shim: build response")
}
