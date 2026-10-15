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
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use std::{convert::Infallible, net::SocketAddr, str::FromStr, sync::Arc};
use tokio::{net::TcpListener, task::JoinHandle};

/// A running shim. Dropping it stops the server.
pub struct ChannelShim {
    addr: SocketAddr,
    task: JoinHandle<()>,
}

/// Token programs whose mint accounts carry the supply at bytes 36..44.
const TOKEN_PROGRAMS: [&str; 2] = [
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
    "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",
];

impl ChannelShim {
    /// Start a shim in front of `upstream_url` on a free local port.
    pub async fn start(upstream_url: &str) -> Self {
        Self::start_with(upstream_url, None).await
    }

    /// Like `start`, but every mint account it serves reports a supply equal to what escrow
    /// `instance` holds of that mint. A harness that plays the channel on the same validator
    /// needs this: every token outside custody would otherwise read as unbacked channel supply.
    pub async fn start_mirroring_custody(upstream_url: &str, instance: Pubkey) -> Self {
        Self::start_with(upstream_url, Some(instance)).await
    }

    async fn start_with(upstream_url: &str, custody_of: Option<Pubkey>) -> Self {
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
            custody_of,
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
    custody_of: Option<Pubkey>,
}

impl Upstream {
    async fn handle(&self, req: Request<hyper::body::Incoming>) -> Response<Full<Bytes>> {
        let body = match req.into_body().collect().await {
            Ok(collected) => collected.to_bytes(),
            Err(e) => return json_response(rpc_error(Value::Null, &format!("read body: {e}"))),
        };
        let parsed: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
        let method = parsed.get("method").and_then(Value::as_str);
        if method == Some("getSignatureStatusSnapshot") {
            let id = parsed.get("id").cloned().unwrap_or(Value::Null);
            return json_response(match self.snapshot(&parsed).await {
                Ok(result) => json!({"jsonrpc": "2.0", "id": id, "result": result}),
                Err(message) => rpc_error(id, &message),
            });
        }
        if let (Some("getAccountInfo"), Some(instance)) = (method, self.custody_of) {
            return match self.forward_bytes(body).await {
                Ok(bytes) => json_response(self.mirror_supply(&parsed, bytes, instance).await),
                Err(message) => json_response(rpc_error(Value::Null, &message)),
            };
        }
        self.forward(body).await
    }

    /// Rewrite a base64 mint account so its supply equals the escrow custody of that mint.
    /// Anything that is not a readable token mint passes through untouched.
    async fn mirror_supply(&self, request: &Value, upstream: Bytes, instance: Pubkey) -> Value {
        use base64::Engine;
        let mut response: Value = match serde_json::from_slice(&upstream) {
            Ok(v) => v,
            Err(_) => return Value::Null,
        };
        let mint = request
            .pointer("/params/0")
            .and_then(Value::as_str)
            .map(str::to_string);
        let account = response.pointer_mut("/result/value");
        let (Some(mint), Some(account)) = (mint, account) else {
            return response;
        };
        let owner = account.get("owner").and_then(Value::as_str).unwrap_or("");
        if !TOKEN_PROGRAMS.contains(&owner) {
            return response;
        }
        let engine = base64::engine::general_purpose::STANDARD;
        // A mint is exactly 82 bytes, or longer with the Token-2022 account type byte set to 1.
        let is_mint = |d: &Vec<u8>| d.len() == 82 || (d.len() > 165 && d[165] == 1);
        let Some(mut data) = account
            .pointer("/data/0")
            .and_then(Value::as_str)
            .and_then(|d| engine.decode(d).ok())
            .filter(is_mint)
        else {
            return response;
        };
        let custody = self.custody_balance(&instance, &mint).await;
        data[36..44].copy_from_slice(&custody.to_le_bytes());
        if let Some(slot) = account.pointer_mut("/data/0") {
            *slot = Value::String(engine.encode(data));
        }
        response
    }

    /// What `instance` holds of `mint` across its token accounts, 0 when it holds none.
    async fn custody_balance(&self, instance: &Pubkey, mint: &str) -> u64 {
        let body = json!({
            "jsonrpc": "2.0", "id": 1, "method": "getTokenAccountsByOwner",
            "params": [instance.to_string(), {"mint": mint}, {"encoding": "jsonParsed"}]
        });
        let Ok(bytes) = self.forward_bytes(Bytes::from(body.to_string())).await else {
            return 0;
        };
        let parsed: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        parsed
            .pointer("/result/value")
            .and_then(Value::as_array)
            .map(|accounts| {
                accounts
                    .iter()
                    .filter_map(|a| {
                        a.pointer("/account/data/parsed/info/tokenAmount/amount")
                            .and_then(Value::as_str)
                            .and_then(|s| s.parse::<u64>().ok())
                    })
                    .sum()
            })
            .unwrap_or(0)
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

    /// Forward one request and return the upstream body alone, for a reply the shim rewrites.
    async fn forward_bytes(&self, body: Bytes) -> Result<Bytes, String> {
        let request = Request::post(&self.url)
            .header("content-type", "application/json")
            .body(Full::new(body))
            .expect("channel shim: build upstream request");
        let response = self
            .http
            .request(request)
            .await
            .map_err(|e| format!("upstream: {e}"))?;
        Ok(response
            .into_body()
            .collect()
            .await
            .map(|c| c.to_bytes())
            .unwrap_or_default())
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
