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

//! Read-only role-management HTTP listener.
//!
//! Metric values remain with their role-local domain owners. This adapter
//! owns only the listener, HTTP representation, and its bounded stop/failure
//! handle so Server role supervision can observe the resource without
//! importing a role implementation into the adapter.

use std::collections::HashMap;
use std::net::{SocketAddr, TcpListener};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::thread::JoinHandle;
use std::time::Duration;

use axum::extract::{Query, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::{Router, routing::get};
use tokio::net::TcpListener as TokioTcpListener;
use tokio::sync::watch;

/// Role-local metrics exposed through the read-only management surface.
///
/// The adapter never registers, mutates, or retains a metric family. The
/// owning role provides the two already-rendered representations at request
/// time, preserving all-in-one role isolation.
pub trait RoleMetricsRenderer: Send + Sync {
    fn render_prometheus(&self) -> Result<String, String>;

    fn render_json(&self) -> Result<String, String>;
}

/// Dedicated HTTP listener for one role's management metrics.
pub struct MetricsHttpServer {
    bound_addr: SocketAddr,
    shutdown_tx: Option<watch::Sender<bool>>,
    failure_rx: mpsc::Receiver<String>,
    join_handle: Option<JoinHandle<()>>,
    stop_requested: Arc<AtomicBool>,
}

impl MetricsHttpServer {
    #[expect(
        clippy::disallowed_methods,
        reason = "The management listener owns its service worker; see ADR-0218."
    )]
    pub fn start(
        host: &str,
        port: u16,
        metrics: Arc<dyn RoleMetricsRenderer>,
    ) -> Result<Self, String> {
        let bind_addr = parse_metrics_bind_addr(host, port)
            .map_err(|error| format!("parse metrics HTTP bind address failed: {error}"))?;
        let listener = TcpListener::bind(bind_addr).map_err(|error| {
            format!("bind metrics HTTP listener on {bind_addr} failed: {error}")
        })?;
        let bound_addr = listener
            .local_addr()
            .map_err(|error| format!("read metrics HTTP listener address failed: {error}"))?;
        listener.set_nonblocking(true).map_err(|error| {
            format!("configure metrics HTTP listener on {bind_addr} failed: {error}")
        })?;
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let (failure_tx, failure_rx) = mpsc::channel();
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let ready_success_tx = ready_tx.clone();
        let stop_requested = Arc::new(AtomicBool::new(false));
        let thread_stop_requested = Arc::clone(&stop_requested);
        let join_handle = std::thread::Builder::new()
            .name("backend-management-http".to_string())
            .spawn(move || {
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .map_err(|error| {
                            format!("build backend management HTTP runtime: {error}")
                        })?;
                    runtime.block_on(async move {
                        let listener = TokioTcpListener::from_std(listener).map_err(|error| {
                            format!("create Tokio backend management HTTP listener: {error}")
                        })?;
                        let app = Router::new()
                            .route("/metrics", get(handle_metrics))
                            .with_state(metrics);
                        let _ = ready_success_tx.send(Ok(()));
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
                                format!("backend management HTTP serve future failed: {error}")
                            })
                    })
                }));
                if thread_stop_requested.load(Ordering::Acquire) {
                    return;
                }
                let error = match outcome {
                    Ok(Ok(())) => "backend management HTTP server exited unexpectedly".to_string(),
                    Ok(Err(error)) => error,
                    Err(payload) => payload
                        .downcast_ref::<String>()
                        .cloned()
                        .or_else(|| {
                            payload
                                .downcast_ref::<&str>()
                                .map(|value| (*value).to_string())
                        })
                        .unwrap_or_else(|| "backend management HTTP server panicked".to_string()),
                };
                let _ = ready_tx.send(Err(error.clone()));
                let _ = failure_tx.send(error);
            })
            .map_err(|error| format!("spawn backend management HTTP server: {error}"))?;
        match ready_rx.recv_timeout(Duration::from_secs(5)) {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                let _ = join_handle.join();
                return Err(error);
            }
            Err(error) => {
                stop_requested.store(true, Ordering::Release);
                let _ = shutdown_tx.send(true);
                let _ = join_handle.join();
                return Err(format!("wait for metrics HTTP listener readiness: {error}"));
            }
        }
        Ok(Self {
            bound_addr,
            shutdown_tx: Some(shutdown_tx),
            failure_rx,
            join_handle: Some(join_handle),
            stop_requested,
        })
    }

    pub const fn bound_addr(&self) -> SocketAddr {
        self.bound_addr
    }

    pub fn poll_failure(&mut self) -> Result<Option<String>, String> {
        match self.failure_rx.try_recv() {
            Ok(error) => Ok(Some(error)),
            Err(mpsc::TryRecvError::Empty) | Err(mpsc::TryRecvError::Disconnected) => Ok(None),
        }
    }

    pub fn stop(mut self) -> Result<(), String> {
        self.stop_requested.store(true, Ordering::Release);
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(true);
        }
        if let Some(join_handle) = self.join_handle.take() {
            join_handle
                .join()
                .map_err(|_| "metrics HTTP server thread panicked".to_string())?;
        }
        Ok(())
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
        .map_err(|error| format!("parse metrics bind addr '{formatted}' failed: {error}"))
}

async fn handle_metrics(
    State(metrics): State<Arc<dyn RoleMetricsRenderer>>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    if params
        .get("type")
        .is_some_and(|value| value.eq_ignore_ascii_case("json"))
    {
        return match metrics.render_json() {
            Ok(body) => ([(header::CONTENT_TYPE, "application/json")], body).into_response(),
            Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error).into_response(),
        };
    }

    match metrics.render_prometheus() {
        Ok(body) => ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], body).into_response(),
        Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error).into_response(),
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpStream};
    use std::sync::Arc;

    use super::{MetricsHttpServer, RoleMetricsRenderer, parse_metrics_bind_addr};

    struct StaticMetrics;

    impl RoleMetricsRenderer for StaticMetrics {
        fn render_prometheus(&self) -> Result<String, String> {
            Ok("role_metric 1\n".to_string())
        }

        fn render_json(&self) -> Result<String, String> {
            Ok("[{\"metric\":\"role_metric\"}]".to_string())
        }
    }

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
        assert_eq!(
            parse_metrics_bind_addr("[::]", 9070).expect("parse bracketed IPv6"),
            "[::]:9070".parse::<SocketAddr>().expect("IPv6 wildcard")
        );
    }

    #[test]
    fn listener_reads_only_the_role_renderer() {
        let server = MetricsHttpServer::start("127.0.0.1", 0, Arc::new(StaticMetrics))
            .expect("start listener");
        let mut stream = TcpStream::connect(server.bound_addr()).expect("connect listener");
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .expect("write metrics request");
        let mut response = String::new();
        stream
            .read_to_string(&mut response)
            .expect("read metrics response");
        assert!(
            response.starts_with("HTTP/1.1 200"),
            "response: {response:?}"
        );
        assert!(response.contains("role_metric 1"));
        server.stop().expect("stop listener");
    }
}
