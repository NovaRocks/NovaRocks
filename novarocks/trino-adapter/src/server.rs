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

use crate::{TrinoListenerSettings, host::require_local_host};
use axum::{Json, Router, extract::State, middleware, routing::get};
use futures::FutureExt;
use novarocks_query_application::serving_admission::{
    FrontendServingAdmission, FrontendServingState,
};
use std::{
    io,
    net::{Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Instant,
};
use tokio::{net::TcpListener, sync::watch, task::JoinHandle};

#[derive(Clone)]
struct InfoState {
    version: Arc<str>,
    admission: FrontendServingAdmission,
    started: Instant,
}
async fn info(State(state): State<InfoState>) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "nodeVersion": {"version": state.version.as_ref()},
        "environment": "novarocks",
        "coordinator": true,
        "starting": state.admission.snapshot().state == FrontendServingState::Starting,
        "uptime": format!("{}ms", state.started.elapsed().as_millis()),
    }))
}

/// FE owns this listener's readiness, drain, failure observation and actual exit.
pub struct TrinoServerHandle {
    bound: SocketAddr,
    registry: Arc<crate::registry::TrinoQueryRegistry>,
    shutdown: watch::Sender<bool>,
    join: Option<JoinHandle<io::Result<()>>>,
}
impl TrinoServerHandle {
    pub async fn bind(
        settings: &TrinoListenerSettings,
        version: String,
        admission: FrontendServingAdmission,
        sessions: Arc<dyn novarocks_query_application::session::QuerySessionFactory>,
        control: novarocks_query_application::session_control::QueryControlService,
    ) -> io::Result<Self> {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, settings.port)).await?;
        let bound = listener.local_addr()?;
        let registry = crate::registry::TrinoQueryRegistry::new(
            sessions,
            control,
            settings.user.clone(),
            settings.client_timeout,
        );
        let state = InfoState {
            version: version.into(),
            admission,
            started: Instant::now(),
        };
        let router = Router::new()
            .route("/v1/info", get(info))
            .with_state(state)
            .merge(crate::routes::routes(Arc::clone(&registry)))
            .layer(middleware::from_fn(require_local_host));
        let (shutdown, mut receiver) = watch::channel(false);
        let task_registry = Arc::clone(&registry);
        let join = tokio::spawn(async move {
            let serving = crate::transport::serve(
                listener,
                router,
                receiver.clone(),
                task_registry.observation.clone(),
            );
            tokio::pin!(serving);
            tokio::select! {
                result = &mut serving => { task_registry.finalize().await; result }
                _ = async {
                    while !*receiver.borrow() {
                        if receiver.changed().await.is_err() { break; }
                    }
                } => {
                    task_registry.finalize().await;
                    serving.await
                }
            }
        });
        Ok(Self {
            bound,
            registry,
            shutdown,
            join: Some(join),
        })
    }
    pub fn observation(&self) -> crate::observation::TrinoProtocolObservation {
        self.registry.observation.clone()
    }
    pub fn bound_addr(&self) -> SocketAddr {
        self.bound
    }
    pub fn begin_drain(&self) {
        self.registry.begin_drain();
    }
    pub fn finalize(&self) {
        self.begin_drain();
        let _ = self.shutdown.send(true);
    }
    pub fn poll_failure(&mut self) -> Option<String> {
        if !self.join.as_ref().is_some_and(|join| join.is_finished()) {
            return None;
        }
        let result = self
            .join
            .take()
            .expect("finished listener")
            .now_or_never()
            .expect("finished join");
        Some(match result {
            Ok(Ok(())) => "Trino listener exited unexpectedly".into(),
            Ok(Err(error)) => format!("Trino listener failed: {error}"),
            Err(error) => format!("Trino listener task failed: {error}"),
        })
    }
    pub async fn wait_stopped(&mut self) -> io::Result<()> {
        match self.join.take() {
            Some(join) => join.await.map_err(io::Error::other)?,
            None => Ok(()),
        }
    }
}
impl Drop for TrinoServerHandle {
    fn drop(&mut self) {
        let _ = self.shutdown.send(true);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    struct InfoOnlyFactory;
    impl novarocks_query_application::session::QuerySessionFactory for InfoOnlyFactory {
        fn open_statement_session(
            &self,
            _: novarocks_query_application::session::StatementSessionRequest,
        ) -> Result<
            Arc<dyn novarocks_query_application::session::QuerySession>,
            novarocks_query_application::session_value::StatementSessionError,
        > {
            unreachable!("info-only test")
        }
        fn open_session(
            &self,
            _: novarocks_query_application::session::QuerySessionOpenRequest,
        ) -> Result<
            Arc<dyn novarocks_query_application::session::QuerySession>,
            novarocks_query_application::session_error::QueryServiceError,
        > {
            unreachable!("info-only test")
        }
        fn cancel_all(
            &self,
            _: novarocks_query_application::cancellation::QueryCancellationReason,
        ) {
        }
    }
    async fn request(address: SocketAddr, path: &str, host: Option<&str>) -> String {
        let mut socket = tokio::net::TcpStream::connect(address).await.unwrap();
        let host = host.map_or(String::new(), |host| format!("Host: {host}\r\n"));
        let request = format!("GET {path} HTTP/1.0\r\n{host}Connection: close\r\n\r\n");
        socket.write_all(request.as_bytes()).await.unwrap();
        let mut response = Vec::new();
        socket.read_to_end(&mut response).await.unwrap();
        String::from_utf8(response).unwrap()
    }
    #[tokio::test]
    async fn info_uses_real_lifecycle_and_host_guard_precedes_routes() {
        let admission = FrontendServingAdmission::new();
        let mut listener = TrinoServerHandle::bind(
            &TrinoListenerSettings {
                port: 0,
                ..Default::default()
            },
            "test-version".into(),
            admission.clone(),
            Arc::new(InfoOnlyFactory),
            novarocks_query_application::session_control::QueryControlService::new(Arc::new(
                novarocks_query_application::query_control::QueryApplicationControl::new(
                    novarocks_types::FrontendProcessId::new_v7(),
                ),
            )),
        )
        .await
        .unwrap();
        assert!(listener.bound_addr().ip().is_loopback());
        let starting = request(listener.bound_addr(), "/v1/info", Some("localhost")).await;
        assert!(starting.starts_with("HTTP/1.0 200"));
        let body: serde_json::Value =
            serde_json::from_str(starting.split_once("\r\n\r\n").unwrap().1).unwrap();
        assert_eq!(body["starting"], true);
        assert_eq!(body["nodeVersion"]["version"], "test-version");
        admission.mark_ready().unwrap();
        let ready = request(listener.bound_addr(), "/v1/info", Some("127.0.0.1:8080")).await;
        assert!(ready.contains("\"starting\":false"));
        for host in [None, Some("evil.example")] {
            let response = request(listener.bound_addr(), "/v1/info", host).await;
            assert!(response.starts_with("HTTP/1.0 403"), "{response}");
        }
        listener.begin_drain();
        listener.finalize();
        listener.wait_stopped().await.unwrap();
    }
}
