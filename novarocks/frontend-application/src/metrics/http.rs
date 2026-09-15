// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::net::{SocketAddr, TcpListener};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread::JoinHandle;

use tokio::net::TcpListener as TokioTcpListener;
use tokio::sync::watch;

use crate::query_execution::lifecycle_diagnostics::{
    QueryLifecycleConvergenceReader, QueryLifecycleConvergenceSnapshot,
};
use crate::topology::BackendIslandSnapshotReader;
use crate::workload_lifecycle::FrontendServingSnapshotReader;

use super::FrontendMetricsRegistry;

/// Frontend-owned management HTTP listener.
pub(crate) struct MetricsHttpServer {
    shutdown_tx: Option<watch::Sender<bool>>,
    failure_rx: mpsc::Receiver<String>,
    join_handle: Option<JoinHandle<()>>,
    stop_requested: Arc<AtomicBool>,
}

/// Preserves the debug route shape while the management listener starts before
/// the coordinator exists. Installing the real reader is one-way.
#[derive(Default)]
pub(crate) struct LateBoundQueryLifecycleConvergenceReader {
    reader: Mutex<Option<Arc<dyn QueryLifecycleConvergenceReader>>>,
}

impl LateBoundQueryLifecycleConvergenceReader {
    pub(crate) fn install(
        &self,
        reader: Arc<dyn QueryLifecycleConvergenceReader>,
    ) -> Result<(), String> {
        let mut slot = self
            .reader
            .lock()
            .expect("late-bound convergence reader lock poisoned");
        if slot.is_some() {
            return Err("frontend lifecycle convergence reader is already installed".to_string());
        }
        *slot = Some(reader);
        Ok(())
    }
}

impl QueryLifecycleConvergenceReader for LateBoundQueryLifecycleConvergenceReader {
    fn latest_convergence_snapshot(&self) -> Option<QueryLifecycleConvergenceSnapshot> {
        self.reader
            .lock()
            .expect("late-bound convergence reader lock poisoned")
            .as_ref()
            .and_then(|reader| reader.latest_convergence_snapshot())
    }
}

impl MetricsHttpServer {
    pub(crate) fn start(
        host: &str,
        port: u16,
        registry: Arc<FrontendMetricsRegistry>,
        serving_reader: Arc<dyn FrontendServingSnapshotReader>,
        island_reader: Arc<dyn BackendIslandSnapshotReader>,
        convergence_reader: Option<Arc<dyn QueryLifecycleConvergenceReader>>,
        memory_authority: Arc<novarocks_memory::MemoryAuthority>,
    ) -> Result<Self, String> {
        let bind_addr = parse_metrics_bind_addr(host, port)
            .map_err(|error| format!("parse frontend metrics HTTP bind address failed: {error}"))?;
        let listener = TcpListener::bind(bind_addr).map_err(|error| {
            format!("bind frontend metrics HTTP listener on {bind_addr} failed: {error}")
        })?;
        listener.set_nonblocking(true).map_err(|error| {
            format!("configure frontend metrics HTTP listener on {bind_addr} failed: {error}")
        })?;
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let (failure_tx, failure_rx) = mpsc::channel();
        let stop_requested = Arc::new(AtomicBool::new(false));
        let thread_stop_requested = Arc::clone(&stop_requested);
        let join_handle = std::thread::spawn(move || {
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|error| {
                        format!("build frontend management HTTP runtime failed: {error}")
                    })?;
                runtime.block_on(async move {
                    let listener = TokioTcpListener::from_std(listener).map_err(|error| {
                        format!("create frontend management HTTP listener failed: {error}")
                    })?;
                    let app = super::management::frontend_management_router_with_readers(
                        registry,
                        serving_reader,
                        island_reader,
                        convergence_reader,
                        memory_authority,
                        crate::native::report_server::lifecycle_convergence_debug_enabled(),
                    );
                    axum::serve(listener, app)
                        .with_graceful_shutdown(async move {
                            while !*shutdown_rx.borrow() {
                                if shutdown_rx.changed().await.is_err() {
                                    break;
                                }
                            }
                        })
                        .await
                        .map_err(|error| {
                            format!("frontend management HTTP server exited with error: {error}")
                        })
                })
            }));
            if thread_stop_requested.load(Ordering::Acquire) {
                return;
            }
            let error = match outcome {
                Ok(Ok(())) => "frontend management HTTP server exited unexpectedly".to_string(),
                Ok(Err(error)) => error,
                Err(payload) => payload
                    .downcast_ref::<String>()
                    .cloned()
                    .or_else(|| {
                        payload
                            .downcast_ref::<&str>()
                            .map(|value| (*value).to_string())
                    })
                    .unwrap_or_else(|| "frontend management HTTP server panicked".to_string()),
            };
            let _ = failure_tx.send(error);
        });
        Ok(Self {
            shutdown_tx: Some(shutdown_tx),
            failure_rx,
            join_handle: Some(join_handle),
            stop_requested,
        })
    }

    pub(crate) fn poll_failure(&mut self) -> Result<Option<String>, String> {
        match self.failure_rx.try_recv() {
            Ok(error) => Ok(Some(error)),
            Err(mpsc::TryRecvError::Empty) | Err(mpsc::TryRecvError::Disconnected) => Ok(None),
        }
    }

    pub(crate) fn stop(&mut self) -> Result<(), String> {
        self.stop_requested.store(true, Ordering::Release);
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(true);
        }
        if let Some(join_handle) = self.join_handle.take() {
            join_handle
                .join()
                .map_err(|_| "frontend metrics HTTP server thread panicked".to_string())?;
        }
        Ok(())
    }
}

impl Drop for MetricsHttpServer {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

fn parse_metrics_bind_addr(host: &str, port: u16) -> Result<SocketAddr, String> {
    let bare = if host.starts_with('[') && host.ends_with(']') {
        &host[1..host.len() - 1]
    } else {
        host
    };
    if let Ok(ip) = bare.parse::<std::net::IpAddr>() {
        return Ok(SocketAddr::new(ip, port));
    }
    let formatted = if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    };
    formatted
        .parse::<SocketAddr>()
        .map_err(|error| format!("parse frontend metrics bind addr '{formatted}' failed: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_bind_addr_accepts_ipv4_and_ipv6_literals() {
        assert_eq!(
            parse_metrics_bind_addr("127.0.0.1", 9070).expect("parse IPv4"),
            "127.0.0.1:9070"
                .parse::<SocketAddr>()
                .expect("IPv4 address")
        );
        assert_eq!(
            parse_metrics_bind_addr("::1", 9070).expect("parse bare IPv6"),
            "[::1]:9070".parse::<SocketAddr>().expect("IPv6 address")
        );
    }
}
