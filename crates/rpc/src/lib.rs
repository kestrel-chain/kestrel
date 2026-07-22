//! Hardened public JSON-RPC, health, readiness, and Prometheus metrics surfaces.

use std::{
    collections::BTreeMap,
    future::Future,
    net::{IpAddr, SocketAddr},
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use axum::{
    Router,
    body::Bytes,
    extract::{ConnectInfo, DefaultBodyLimit, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use state::StateTree;
use thiserror::Error;
use tokio::net::TcpListener;
use tower::ServiceBuilder;
use types::Hash;

const JSON_RPC_VERSION: &str = "2.0";

/// Resource and abuse limits for the public RPC listener.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RpcConfig {
    pub maximum_body_bytes: usize,
    pub maximum_batch_length: usize,
    pub requests_per_window: u32,
    pub rate_window: Duration,
}

impl Default for RpcConfig {
    fn default() -> Self {
        Self {
            maximum_body_bytes: 512 * 1024,
            maximum_batch_length: 64,
            requests_per_window: 1_000,
            rate_window: Duration::from_secs(1),
        }
    }
}

impl RpcConfig {
    /// Validates all limits before binding a socket.
    ///
    /// # Errors
    ///
    /// Rejects zero limits and excessively large batch limits.
    pub fn validate(self) -> Result<Self, RpcError> {
        if self.maximum_body_bytes == 0
            || self.maximum_batch_length == 0
            || self.maximum_batch_length > 1_024
            || self.requests_per_window == 0
            || self.rate_window.is_zero()
        {
            return Err(RpcError::InvalidConfiguration);
        }
        Ok(self)
    }
}

/// Public chain status. `ready` is false while bootstrap/state sync is incomplete.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NodeStatus {
    pub chain_id: String,
    pub genesis_hash: Hash,
    pub finalized_height: u64,
    pub finalized_block: Hash,
    pub state_root: Hash,
    pub peer_count: usize,
    pub ready: bool,
    pub finality_latency_ms: Option<u64>,
    pub view_changes: u64,
}

/// Sink for a signed transaction envelope submitted over RPC. Implemented by
/// the node binary's production transaction-admission path (see
/// `node::Stage2PipelineHandle`); the RPC crate itself has no admission logic
/// and never accepts a transaction without a submitter configured.
pub trait TransactionSubmitter: Send + Sync {
    /// Validates, admits, and gossips one canonically encoded transaction.
    ///
    /// # Errors
    ///
    /// Returns a caller-facing message describing why admission was refused.
    fn submit(&self, bytes: Vec<u8>) -> Result<Hash, String>;
}

/// Lock-free counters exported in Prometheus text format.
#[derive(Debug, Default)]
pub struct RpcMetrics {
    requests: AtomicU64,
    rejected: AtomicU64,
    errors: AtomicU64,
    latency_micros: AtomicU64,
}

impl RpcMetrics {
    #[must_use]
    pub fn render(&self, status: &NodeStatus) -> String {
        format!(
            concat!(
                "# TYPE kestrel_rpc_requests_total counter\n",
                "kestrel_rpc_requests_total {}\n",
                "# TYPE kestrel_rpc_rejected_total counter\n",
                "kestrel_rpc_rejected_total {}\n",
                "# TYPE kestrel_rpc_errors_total counter\n",
                "kestrel_rpc_errors_total {}\n",
                "# TYPE kestrel_rpc_latency_microseconds_total counter\n",
                "kestrel_rpc_latency_microseconds_total {}\n",
                "# TYPE kestrel_finalized_height gauge\n",
                "kestrel_finalized_height {}\n",
                "# TYPE kestrel_peer_count gauge\n",
                "kestrel_peer_count {}\n",
                "# TYPE kestrel_node_ready gauge\n",
                "kestrel_node_ready {}\n",
                "# TYPE kestrel_finality_latency_milliseconds gauge\n",
                "kestrel_finality_latency_milliseconds {}\n",
                "# TYPE kestrel_consensus_view_changes gauge\n",
                "kestrel_consensus_view_changes {}\n"
            ),
            self.requests.load(Ordering::Relaxed),
            self.rejected.load(Ordering::Relaxed),
            self.errors.load(Ordering::Relaxed),
            self.latency_micros.load(Ordering::Relaxed),
            status.finalized_height,
            status.peer_count,
            u8::from(status.ready),
            status.finality_latency_ms.unwrap_or_default(),
            status.view_changes,
        )
    }
}

#[derive(Clone)]
pub struct RpcService {
    inner: Arc<RpcInner>,
}

struct RpcInner {
    config: RpcConfig,
    status: Arc<RwLock<NodeStatus>>,
    state: Arc<RwLock<StateTree>>,
    metrics: Arc<RpcMetrics>,
    limiter: Mutex<BTreeMap<IpAddr, RateWindow>>,
    submitter: Option<Arc<dyn TransactionSubmitter>>,
}

#[derive(Clone, Copy)]
struct RateWindow {
    started: Instant,
    requests: u32,
}

impl RpcService {
    /// Creates a service around live node status and canonical object state.
    ///
    /// # Errors
    ///
    /// Rejects unsafe zero or unbounded configuration values.
    pub fn new(
        config: RpcConfig,
        status: Arc<RwLock<NodeStatus>>,
        state: Arc<RwLock<StateTree>>,
        submitter: Option<Arc<dyn TransactionSubmitter>>,
    ) -> Result<Self, RpcError> {
        Ok(Self {
            inner: Arc::new(RpcInner {
                config: config.validate()?,
                status,
                state,
                metrics: Arc::new(RpcMetrics::default()),
                limiter: Mutex::new(BTreeMap::new()),
                submitter,
            }),
        })
    }

    #[must_use]
    pub fn metrics(&self) -> Arc<RpcMetrics> {
        Arc::clone(&self.inner.metrics)
    }

    pub fn router(&self) -> Router {
        Router::new()
            .route("/", post(json_rpc))
            .route("/healthz", get(health))
            .route("/readyz", get(readiness))
            .route("/metrics", get(metrics))
            .layer(
                ServiceBuilder::new()
                    .layer(DefaultBodyLimit::max(self.inner.config.maximum_body_bytes)),
            )
            .with_state(self.clone())
    }

    /// Serves until the supplied shutdown future completes.
    ///
    /// # Errors
    ///
    /// Returns listener/HTTP serving failures.
    pub async fn serve<F>(&self, listener: TcpListener, shutdown: F) -> Result<(), std::io::Error>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        axum::serve(
            listener,
            self.router()
                .into_make_service_with_connect_info::<SocketAddr>(),
        )
        .with_graceful_shutdown(shutdown)
        .await
    }

    fn allow(&self, address: IpAddr) -> Result<bool, RpcError> {
        let mut windows = self
            .inner
            .limiter
            .lock()
            .map_err(|_| RpcError::LockPoisoned)?;
        let now = Instant::now();
        windows
            .retain(|_, window| now.duration_since(window.started) < self.inner.config.rate_window);
        let window = windows.entry(address).or_insert(RateWindow {
            started: now,
            requests: 0,
        });
        if now.duration_since(window.started) >= self.inner.config.rate_window {
            *window = RateWindow {
                started: now,
                requests: 0,
            };
        }
        if window.requests >= self.inner.config.requests_per_window {
            return Ok(false);
        }
        window.requests += 1;
        Ok(true)
    }
}

#[derive(Debug, Error)]
pub enum RpcError {
    #[error("RPC limits are invalid")]
    InvalidConfiguration,
    #[error("shared RPC state lock was poisoned")]
    LockPoisoned,
}

#[derive(Debug, Deserialize)]
struct JsonRpcRequest {
    jsonrpc: String,
    method: String,
    #[serde(default)]
    params: Value,
    #[serde(default)]
    id: Value,
}

async fn json_rpc(
    State(service): State<RpcService>,
    ConnectInfo(connection): ConnectInfo<SocketAddr>,
    bytes: Bytes,
) -> Response {
    let started = Instant::now();
    service
        .inner
        .metrics
        .requests
        .fetch_add(1, Ordering::Relaxed);
    let address = connection.ip();
    if !matches!(service.allow(address), Ok(true)) {
        service
            .inner
            .metrics
            .rejected
            .fetch_add(1, Ordering::Relaxed);
        return (StatusCode::TOO_MANY_REQUESTS, "rate limit exceeded").into_response();
    }

    let response = match serde_json::from_slice::<Value>(&bytes) {
        Ok(Value::Array(requests)) if requests.is_empty() => {
            error_response(&Value::Null, -32600, "invalid request")
        }
        Ok(Value::Array(requests))
            if requests.len() > service.inner.config.maximum_batch_length =>
        {
            service
                .inner
                .metrics
                .rejected
                .fetch_add(1, Ordering::Relaxed);
            error_response(&Value::Null, -32600, "batch limit exceeded")
        }
        Ok(Value::Array(requests)) => Value::Array(
            requests
                .into_iter()
                .map(|request| dispatch(&service, request))
                .collect(),
        ),
        Ok(request) => dispatch(&service, request),
        Err(_) => error_response(&Value::Null, -32700, "parse error"),
    };
    if response.get("error").is_some() {
        service.inner.metrics.errors.fetch_add(1, Ordering::Relaxed);
    }
    let elapsed = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
    service
        .inner
        .metrics
        .latency_micros
        .fetch_add(elapsed, Ordering::Relaxed);
    axum::Json(response).into_response()
}

fn dispatch(service: &RpcService, value: Value) -> Value {
    let Ok(request) = serde_json::from_value::<JsonRpcRequest>(value) else {
        return error_response(&Value::Null, -32600, "invalid request");
    };
    if request.jsonrpc != JSON_RPC_VERSION || request.method.len() > 128 {
        return error_response(&request.id, -32600, "invalid request");
    }
    let span = tracing::info_span!("rpc", method = %request.method);
    let _guard = span.enter();
    match request.method.as_str() {
        "kestrel_getStatus" if params_empty(&request.params) => {
            let Ok(status) = service.inner.status.read() else {
                return error_response(&request.id, -32603, "internal error");
            };
            success_response(
                &request.id,
                &json!({
                    "chainId": status.chain_id,
                    "genesisHash": status.genesis_hash.to_string(),
                    "finalizedHeight": status.finalized_height,
                    "finalizedBlock": status.finalized_block.to_string(),
                    "stateRoot": status.state_root.to_string(),
                    "peerCount": status.peer_count,
                    "ready": status.ready,
                    "finalityLatencyMs": status.finality_latency_ms,
                    "viewChanges": status.view_changes,
                }),
            )
        }
        "kestrel_getObject" => match object_id_param(&request.params) {
            Ok(id) => {
                let Ok(state) = service.inner.state.read() else {
                    return error_response(&request.id, -32603, "internal error");
                };
                match state.object(&id) {
                    Some(object) => success_response(
                        &request.id,
                        &json!({
                            "id": object.id.to_string(),
                            "owner": object.owner,
                            "type": object.type_tag,
                            "version": object.version,
                            "data": hex::encode(&object.data),
                            "rentBalance": object.rent_balance,
                        }),
                    ),
                    None => error_response(&request.id, -32004, "object not found"),
                }
            }
            Err(()) => error_response(&request.id, -32602, "invalid params"),
        },
        "kestrel_submitTransaction" => match transaction_bytes_param(&request.params) {
            Ok(bytes) => match &service.inner.submitter {
                Some(submitter) => match submitter.submit(bytes) {
                    Ok(id) => {
                        success_response(&request.id, &json!({ "transactionId": id.to_string() }))
                    }
                    Err(message) => error_response(&request.id, -32000, &message),
                },
                None => error_response(&request.id, -32601, "method not found"),
            },
            Err(()) => error_response(&request.id, -32602, "invalid params"),
        },
        _ => error_response(&request.id, -32601, "method not found"),
    }
}

fn params_empty(params: &Value) -> bool {
    params.is_null()
        || params.as_array().is_some_and(Vec::is_empty)
        || params.as_object().is_some_and(serde_json::Map::is_empty)
}

fn object_id_param(params: &Value) -> Result<Hash, ()> {
    let encoded = params
        .get("id")
        .and_then(Value::as_str)
        .or_else(|| params.as_array()?.first()?.as_str())
        .ok_or(())?;
    let decoded = hex::decode(encoded.strip_prefix("0x").unwrap_or(encoded)).map_err(|_| ())?;
    let bytes: [u8; 32] = decoded.try_into().map_err(|_| ())?;
    Ok(Hash::from_bytes(bytes))
}

fn transaction_bytes_param(params: &Value) -> Result<Vec<u8>, ()> {
    let encoded = params
        .get("transaction")
        .and_then(Value::as_str)
        .or_else(|| params.as_array()?.first()?.as_str())
        .ok_or(())?;
    hex::decode(encoded.strip_prefix("0x").unwrap_or(encoded)).map_err(|_| ())
}

fn success_response(id: &Value, result: &Value) -> Value {
    json!({"jsonrpc": JSON_RPC_VERSION, "result": result, "id": id})
}

fn error_response(id: &Value, code: i32, message: &str) -> Value {
    json!({"jsonrpc": JSON_RPC_VERSION, "error": {"code": code, "message": message}, "id": id})
}

async fn health() -> StatusCode {
    StatusCode::OK
}

async fn readiness(State(service): State<RpcService>) -> StatusCode {
    match service.inner.status.read() {
        Ok(status) if status.ready => StatusCode::OK,
        _ => StatusCode::SERVICE_UNAVAILABLE,
    }
}

async fn metrics(State(service): State<RpcService>) -> Response {
    match service.inner.status.read() {
        Ok(status) => (
            [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
            service.inner.metrics.render(&status),
        )
            .into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        net::SocketAddr,
        sync::{Arc, RwLock},
    };

    use axum::{
        body::{Body, to_bytes},
        http::Request,
    };
    use state::{StateConfig, StateTree};
    use tower::ServiceExt;
    use types::{Address, Hash, Object, Owner};

    use super::{NodeStatus, RpcConfig, RpcService, TransactionSubmitter};

    #[tokio::test]
    async fn rpc_enforces_readiness_rate_and_batch_limits() {
        let (service, object) = fixture(RpcConfig {
            maximum_body_bytes: 512,
            maximum_batch_length: 1,
            requests_per_window: 2,
            ..RpcConfig::default()
        });
        let router = service.router();
        let readiness = router
            .clone()
            .oneshot(Request::get("/readyz").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(readiness.status(), 503);

        let oversized = router
            .clone()
            .oneshot(
                Request::post("/")
                    .header("content-type", "application/json")
                    .extension(axum::extract::ConnectInfo(SocketAddr::from((
                        [127, 0, 0, 1],
                        1,
                    ))))
                    .body(Body::from(vec![b' '; 513]))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(oversized.status(), 413);

        let response = request(
            router.clone(),
            format!(
                r#"{{"jsonrpc":"2.0","method":"kestrel_getObject","params":{{"id":"{}"}},"id":1}}"#,
                object.id
            ),
        )
        .await;
        assert_eq!(response["result"]["data"], "70686173652d736978");

        let batch = request(
            router.clone(),
            r#"[{"jsonrpc":"2.0","method":"kestrel_getStatus","id":1},{"jsonrpc":"2.0","method":"kestrel_getStatus","id":2}]"#.to_owned(),
        )
        .await;
        assert_eq!(batch["error"]["message"], "batch limit exceeded");

        service.inner.status.write().unwrap().ready = true;
        let readiness = router
            .clone()
            .oneshot(Request::get("/readyz").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(readiness.status(), 200);
        let metrics = router
            .clone()
            .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
            .await
            .unwrap();
        let metrics = to_bytes(metrics.into_body(), 64 * 1024).await.unwrap();
        assert!(
            std::str::from_utf8(&metrics)
                .unwrap()
                .contains("kestrel_finalized_height 7")
        );

        let limited = router
            .oneshot(
                Request::post("/")
                    .header("content-type", "application/json")
                    .extension(axum::extract::ConnectInfo(SocketAddr::from((
                        [127, 0, 0, 1],
                        1,
                    ))))
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(limited.status(), 429);
    }

    async fn request(router: axum::Router, body: String) -> serde_json::Value {
        let response = router
            .oneshot(
                Request::post("/")
                    .header("content-type", "application/json")
                    .extension(axum::extract::ConnectInfo(SocketAddr::from((
                        [127, 0, 0, 1],
                        1,
                    ))))
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        serde_json::from_slice(&to_bytes(response.into_body(), 1024 * 1024).await.unwrap()).unwrap()
    }

    struct StubSubmitter;

    impl TransactionSubmitter for StubSubmitter {
        fn submit(&self, bytes: Vec<u8>) -> Result<Hash, String> {
            if bytes == b"reject-me" {
                return Err("rejected by stub submitter".to_owned());
            }
            Ok(Hash::digest(bytes))
        }
    }

    #[tokio::test]
    async fn submit_transaction_routes_through_the_configured_submitter() {
        let (service, _object) = fixture(RpcConfig::default());
        let response = request(
            service.router(),
            r#"{"jsonrpc":"2.0","method":"kestrel_submitTransaction","params":{"transaction":"010203"},"id":1}"#.to_owned(),
        )
        .await;
        assert_eq!(
            response["result"]["transactionId"],
            Hash::digest([1_u8, 2, 3]).to_string()
        );

        let rejected = request(
            service.router(),
            format!(
                r#"{{"jsonrpc":"2.0","method":"kestrel_submitTransaction","params":{{"transaction":"{}"}},"id":1}}"#,
                hex::encode(b"reject-me")
            ),
        )
        .await;
        assert_eq!(rejected["error"]["message"], "rejected by stub submitter");
    }

    #[tokio::test]
    async fn submit_transaction_is_unsupported_without_a_configured_submitter() {
        let (service, _object) = fixture(RpcConfig::default());
        // Rebuild without a submitter to prove the method is refused, not silently accepted.
        let service = RpcService::new(
            RpcConfig::default(),
            Arc::clone(&service.inner.status),
            Arc::clone(&service.inner.state),
            None,
        )
        .unwrap();
        let response = request(
            service.router(),
            r#"{"jsonrpc":"2.0","method":"kestrel_submitTransaction","params":{"transaction":"01"},"id":1}"#.to_owned(),
        )
        .await;
        assert_eq!(response["error"]["message"], "method not found");
    }

    fn fixture(config: RpcConfig) -> (RpcService, Object) {
        let object = Object {
            id: Hash::digest(b"rpc-object"),
            owner: Owner::Single(Address::from_bytes([1; 32])),
            type_tag: "rpc::Object".to_owned(),
            version: 0,
            data: b"phase-six".to_vec(),
            rent_balance: 100,
        };
        let mut state = StateTree::new(StateConfig::default()).unwrap();
        state.create_object(object.clone()).unwrap();
        let status = NodeStatus {
            chain_id: "kestrel-testnet-1".to_owned(),
            genesis_hash: Hash::digest(b"genesis"),
            finalized_height: 7,
            finalized_block: Hash::digest(b"block"),
            state_root: state.root().unwrap(),
            peer_count: 3,
            ready: false,
            finality_latency_ms: None,
            view_changes: 0,
        };
        (
            RpcService::new(
                config,
                Arc::new(RwLock::new(status)),
                Arc::new(RwLock::new(state)),
                Some(Arc::new(StubSubmitter)),
            )
            .unwrap(),
            object,
        )
    }
}
