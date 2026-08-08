//! gRPC handlers for the MPP [`ShuffleService`].

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use parking_lot::Mutex;
use tokio::runtime::Handle;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};

use crate::error::{Result, TakyonicError};
use crate::mpp::{FragmentDispatcher, FragmentSpec, Worker, WorkerEndpoint};
use crate::network::proto::shuffle_service_client::ShuffleServiceClient;
use crate::network::proto::shuffle_service_server::ShuffleService;
use crate::network::proto::{
    CloseShuffleRequest, CloseShuffleResponse, ExecuteFragmentRequest, ExecuteFragmentResponse,
    FetchShuffleRequest, FetchShuffleResponse, PushShuffleRequest, PushShuffleResponse,
};
use crate::schema::Record;
use crate::shuffle::{ShuffleKey, ShuffleManager, decode_rows, encode_rows};
use crate::types::Value;

/// Shuffle + fragment RPC service backed by a local [`Worker`].
pub struct ShuffleGrpcService {
    worker: Arc<Worker>,
}

impl ShuffleGrpcService {
    /// Construct from a shared worker.
    pub fn new(worker: Arc<Worker>) -> Self {
        Self { worker }
    }

    fn map_err(e: crate::error::TakyonicError) -> Status {
        Status::internal(e.to_string())
    }
}

#[tonic::async_trait]
impl ShuffleService for ShuffleGrpcService {
    async fn push_shuffle(
        &self,
        request: Request<PushShuffleRequest>,
    ) -> std::result::Result<Response<PushShuffleResponse>, Status> {
        let req = request.into_inner();
        let key = ShuffleKey {
            query_id: req.query_id,
            shuffle_id: req.shuffle_id,
        };
        let rows = decode_rows(&req.rows).map_err(Self::map_err)?;
        let accepted = self
            .worker
            .shuffle()
            .try_push(key, req.partition, &rows, req.eos)
            .map_err(Self::map_err)?;
        Ok(Response::new(PushShuffleResponse {
            accepted,
            retry_after_ms: if accepted { 0 } else { 5 },
        }))
    }

    async fn fetch_shuffle(
        &self,
        request: Request<FetchShuffleRequest>,
    ) -> std::result::Result<Response<FetchShuffleResponse>, Status> {
        let req = request.into_inner();
        let key = ShuffleKey {
            query_id: req.query_id,
            shuffle_id: req.shuffle_id,
        };
        let (rows, eos) = self
            .worker
            .shuffle()
            .try_fetch(key, req.partition)
            .map_err(Self::map_err)?;
        Ok(Response::new(FetchShuffleResponse {
            rows: encode_rows(&rows)
                .into_iter()
                .map(|b| b.to_vec())
                .collect(),
            eos,
        }))
    }

    async fn execute_fragment(
        &self,
        request: Request<ExecuteFragmentRequest>,
    ) -> std::result::Result<Response<ExecuteFragmentResponse>, Status> {
        let req = request.into_inner();
        let spec = FragmentSpec::decode(Bytes::from(req.fragment)).map_err(Self::map_err)?;
        let rows = self.worker.execute_fragment(&spec).map_err(Self::map_err)?;
        Ok(Response::new(ExecuteFragmentResponse {
            rows: encode_rows(&rows)
                .into_iter()
                .map(|b| b.to_vec())
                .collect(),
        }))
    }

    async fn close_shuffle(
        &self,
        request: Request<CloseShuffleRequest>,
    ) -> std::result::Result<Response<CloseShuffleResponse>, Status> {
        let req = request.into_inner();
        let key = ShuffleKey {
            query_id: req.query_id,
            shuffle_id: req.shuffle_id,
        };
        self.worker.shuffle().close(key);
        Ok(Response::new(CloseShuffleResponse {}))
    }
}

/// Helper used by tests / dispatchers: decode Encode rows from gRPC bytes.
#[allow(dead_code)]
pub fn records_from_bytes(rows: &[Vec<u8>]) -> crate::error::Result<Vec<Record>> {
    rows.iter()
        .map(|b| Record::decode(&Value::new(Bytes::copy_from_slice(b))))
        .collect()
}

/// Shared shuffle manager accessor for wiring.
pub fn ensure_shuffle(mgr: &Arc<ShuffleManager>, key: ShuffleKey, partitions: u32) {
    mgr.open_shuffle(key, partitions);
}

/// Whether `address` looks like a reachable gRPC `host:port` (not `local-*` sim).
pub fn is_grpc_worker_address(address: &str) -> bool {
    !address.starts_with("local") && address.contains(':')
}

/// Cluster [`FragmentDispatcher`] that calls `ShuffleService.ExecuteFragment` over tonic.
///
/// Local node id (when set) runs in-process to avoid self-RPC deadlocks.
pub struct GrpcFragmentDispatcher {
    endpoints: HashMap<u64, String>,
    local_node: Option<u64>,
    local_worker: Option<Arc<Worker>>,
    clients: Mutex<HashMap<u64, ShuffleServiceClient<Channel>>>,
}

impl GrpcFragmentDispatcher {
    /// Build from the coordinator worker directory.
    pub fn new(
        workers: &[WorkerEndpoint],
        local_node: Option<u64>,
        local_worker: Option<Arc<Worker>>,
    ) -> Self {
        let mut endpoints = HashMap::new();
        for w in workers {
            endpoints.insert(w.node_id, w.address.clone());
        }
        Self {
            endpoints,
            local_node,
            local_worker,
            clients: Mutex::new(HashMap::new()),
        }
    }

    /// True when any worker has a real gRPC address.
    pub fn useful_for(workers: &[WorkerEndpoint]) -> bool {
        workers.iter().any(|w| is_grpc_worker_address(&w.address))
    }

    fn block_on<F, T>(&self, fut: F) -> Result<T>
    where
        F: std::future::Future<Output = Result<T>>,
    {
        match Handle::try_current() {
            Ok(handle) => tokio::task::block_in_place(|| handle.block_on(fut)),
            Err(_) => {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|e| TakyonicError::Network(format!("shuffle runtime: {e}")))?;
                rt.block_on(fut)
            }
        }
    }

    async fn client(&self, node_id: u64) -> Result<ShuffleServiceClient<Channel>> {
        if let Some(c) = self.clients.lock().get(&node_id).cloned() {
            return Ok(c);
        }
        let addr = self
            .endpoints
            .get(&node_id)
            .cloned()
            .ok_or_else(|| TakyonicError::Network(format!("unknown MPP worker {node_id}")))?;
        if !is_grpc_worker_address(&addr) {
            return Err(TakyonicError::Network(format!(
                "worker {node_id} address `{addr}` is not a gRPC endpoint"
            )));
        }
        let uri = format!("http://{addr}");
        let channel = Channel::from_shared(uri.clone())
            .map_err(|e| TakyonicError::Network(e.to_string()))?
            .connect_timeout(Duration::from_millis(200))
            .timeout(Duration::from_secs(5))
            .connect()
            .await
            .map_err(|e| TakyonicError::Network(format!("shuffle connect {uri}: {e}")))?;
        let client = ShuffleServiceClient::new(channel)
            .max_decoding_message_size(32 * 1024 * 1024)
            .max_encoding_message_size(32 * 1024 * 1024);
        self.clients.lock().insert(node_id, client.clone());
        Ok(client)
    }
}

impl FragmentDispatcher for GrpcFragmentDispatcher {
    fn execute_remote(&self, node_id: u64, fragment: &FragmentSpec) -> Result<Vec<Record>> {
        if Some(node_id) == self.local_node {
            if let Some(w) = &self.local_worker {
                return w.execute_fragment(fragment);
            }
        }
        let encoded = fragment.encode().to_vec();
        self.block_on(async {
            let mut client = self.client(node_id).await?;
            let resp = client
                .execute_fragment(Request::new(ExecuteFragmentRequest {
                    fragment: encoded,
                }))
                .await
                .map_err(|e| TakyonicError::Network(e.to_string()))?
                .into_inner();
            decode_rows(&resp.rows)
        })
    }
}

/// Client for `ShuffleService.PushShuffle` with retry on full-buffer backpressure.
pub struct RemoteShuffleClient {
    endpoint: String,
}

impl RemoteShuffleClient {
    /// Target a single worker `host:port`.
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
        }
    }

    fn block_on<F, T>(&self, fut: F) -> Result<T>
    where
        F: std::future::Future<Output = Result<T>>,
    {
        match Handle::try_current() {
            Ok(handle) => tokio::task::block_in_place(|| handle.block_on(fut)),
            Err(_) => {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|e| TakyonicError::Network(format!("shuffle runtime: {e}")))?;
                rt.block_on(fut)
            }
        }
    }

    async fn connect(&self) -> Result<ShuffleServiceClient<Channel>> {
        let uri = format!("http://{}", self.endpoint);
        let channel = Channel::from_shared(uri.clone())
            .map_err(|e| TakyonicError::Network(e.to_string()))?
            .connect_timeout(Duration::from_millis(200))
            .timeout(Duration::from_secs(5))
            .connect()
            .await
            .map_err(|e| TakyonicError::Network(format!("shuffle connect {uri}: {e}")))?;
        Ok(ShuffleServiceClient::new(channel)
            .max_decoding_message_size(32 * 1024 * 1024)
            .max_encoding_message_size(32 * 1024 * 1024))
    }

    /// One non-blocking push; returns `(accepted, retry_after_ms)`.
    pub fn try_push(
        &self,
        key: ShuffleKey,
        partition: u32,
        rows: &[Record],
        eos: bool,
    ) -> Result<(bool, u32)> {
        let payload: Vec<Vec<u8>> = encode_rows(rows).into_iter().map(|b| b.to_vec()).collect();
        self.block_on(async {
            let mut client = self.connect().await?;
            let resp = client
                .push_shuffle(Request::new(PushShuffleRequest {
                    query_id: key.query_id,
                    shuffle_id: key.shuffle_id,
                    partition,
                    rows: payload,
                    eos,
                }))
                .await
                .map_err(|e| TakyonicError::Network(e.to_string()))?
                .into_inner();
            Ok((resp.accepted, resp.retry_after_ms))
        })
    }

    /// Push until accepted, sleeping `retry_after_ms` between attempts.
    pub fn push_blocking(
        &self,
        key: ShuffleKey,
        partition: u32,
        rows: &[Record],
        eos: bool,
    ) -> Result<()> {
        loop {
            let (accepted, retry_ms) = self.try_push(key, partition, rows, eos)?;
            if accepted {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(u64::from(retry_ms.max(1))));
        }
    }

    /// Fetch available rows from a remote partition.
    pub fn try_fetch(
        &self,
        key: ShuffleKey,
        partition: u32,
    ) -> Result<(Vec<Record>, bool)> {
        self.block_on(async {
            let mut client = self.connect().await?;
            let resp = client
                .fetch_shuffle(Request::new(FetchShuffleRequest {
                    query_id: key.query_id,
                    shuffle_id: key.shuffle_id,
                    partition,
                }))
                .await
                .map_err(|e| TakyonicError::Network(e.to_string()))?
                .into_inner();
            Ok((decode_rows(&resp.rows)?, resp.eos))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::engine::TakyonicEngine;
    use crate::network::proto::shuffle_service_server::ShuffleServiceServer;
    use crate::telemetry::EngineMetrics;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tonic::transport::Server;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shuffle_grpc_backpressure_retry_e2e() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("takyonic-shuffle-bp-{nanos}"));
        let _ = std::fs::remove_dir_all(&root);
        let cfg = Config::default()
            .data_dir(root.join("data"))
            .wal_dir(root.join("wal"))
            .memtable_size_bytes(64 * 1024 * 1024)
            .l0_rapid_pool_threads(1)
            .ln_haul_pool_threads(1)
            .compaction_write_bytes_per_sec(1024 * 1024 * 1024)
            .metrics_enabled(true)
            .metrics_bind("127.0.0.1:0");
        let engine = Arc::new(TakyonicEngine::open(cfg).unwrap());
        let metrics = Arc::clone(engine.metrics());
        // Capacity 1: second push without drain must backpressure.
        let shuffle = Arc::new(ShuffleManager::new(1, Some(Arc::clone(&metrics))));
        let worker = Arc::new(Worker::new(
            Arc::clone(&engine),
            Arc::clone(&shuffle),
            Arc::clone(&metrics),
        ));
        let svc = ShuffleGrpcService::new(Arc::clone(&worker));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(ShuffleServiceServer::new(svc))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        let endpoint = addr.to_string();
        let client = RemoteShuffleClient::new(endpoint);
        let key = ShuffleKey {
            query_id: 42,
            shuffle_id: 7,
        };
        let batch = vec![Record::new().set("id", "1").set("v", "a")];

        // Fill the single-slot buffer.
        let (ok, _) = client.try_push(key, 0, &batch, false).unwrap();
        assert!(ok, "first push must be accepted");
        assert!(metrics.mpp_shuffle_sent() > 0);

        // Second push must hit backpressure.
        let (ok2, retry_ms) = client.try_push(key, 0, &batch, false).unwrap();
        assert!(!ok2, "full buffer must reject");
        assert!(retry_ms > 0);
        assert!(
            metrics.mpp_shuffle_backpressure() > 0,
            "server must record backpressure"
        );

        // Drain in background while producer retries with push_blocking.
        let drain_client = RemoteShuffleClient::new(addr.to_string());
        let drain_key = key;
        let drain = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(20));
            let (rows, _) = drain_client.try_fetch(drain_key, 0).unwrap();
            assert!(!rows.is_empty());
        });

        client
            .push_blocking(key, 0, &batch, true)
            .expect("retry after drain must succeed");
        drain.join().unwrap();

        assert!(metrics.mpp_shuffle_recv() > 0);
        assert!(metrics.mpp_shuffle_sent() >= 2);

        server.abort();
        engine.close().unwrap();
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn local_try_push_records_backpressure_metric() {
        let metrics = Arc::new(EngineMetrics::new());
        let mgr = ShuffleManager::new(1, Some(Arc::clone(&metrics)));
        let key = ShuffleKey {
            query_id: 1,
            shuffle_id: 1,
        };
        let row = vec![Record::new().set("x", "1")];
        assert!(mgr.try_push(key, 0, &row, false).unwrap());
        assert!(!mgr.try_push(key, 0, &row, false).unwrap());
        assert_eq!(metrics.mpp_shuffle_backpressure(), 1);
        let (got, _) = mgr.try_fetch(key, 0).unwrap();
        assert_eq!(got.len(), 1);
        assert!(mgr.try_push(key, 0, &row, true).unwrap());
    }
}

