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

use std::future::Future;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context;
use novarocks::common::app_config::NovaRocksConfig;
use novarocks_backend::{BackendApplicationHost, BackendServerConfig};
use novarocks_frontend::{FrontendGrpcEndpointOwnership, FrontendServerConfig};

const BACKEND_SUPERVISION_POLL_INTERVAL: Duration = Duration::from_millis(50);

pub fn run_all_in_one(
    config: NovaRocksConfig,
    config_path: Option<PathBuf>,
    port_override: Option<u16>,
) -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(novarocks::runtime::global_async_runtime::WORKER_STACK_SIZE_BYTES)
        .build()
        .context("build all-in-one Tokio runtime")?;

    runtime.block_on(run_all_in_one_until(
        config,
        config_path,
        port_override,
        async {
            tokio::signal::ctrl_c()
                .await
                .map_err(|error| format!("Ctrl-C listener failed: {error}"))
        },
    ))
}

async fn run_all_in_one_until<F>(
    config: NovaRocksConfig,
    config_path: Option<PathBuf>,
    port_override: Option<u16>,
    shutdown: F,
) -> anyhow::Result<()>
where
    F: Future<Output = Result<(), String>> + Send,
{
    let frontend_config = FrontendServerConfig {
        config: config.clone(),
        config_path: config_path.clone(),
        port_override,
        grpc_endpoint: FrontendGrpcEndpointOwnership::ExternallyHosted,
    };
    let frontend = novarocks_frontend::open_frontend_application_for_server(&frontend_config)
        .await
        .map_err(|error| anyhow::anyhow!("open all-in-one frontend application failed: {error}"))?;
    let mut backend =
        match BackendApplicationHost::open_with_native_report_handler_and_terminal_ingress(
            BackendServerConfig {
                config: config.clone(),
            },
            frontend.native_report_handler(),
            Some(frontend.terminal_ingress()),
        ) {
            Ok(backend) => backend,
            Err(error) => {
                let frontend_cleanup = frontend.shutdown().await;
                return Err(anyhow::anyhow!(
                    "open all-in-one backend application failed: {error}; frontend cleanup: {:?}",
                    frontend_cleanup.err()
                ));
            }
        };
    let dml = frontend.dml_service();
    let services = novarocks_frontend::standalone_open_services_for_server(&frontend);

    let (server_shutdown_tx, server_shutdown_rx) = tokio::sync::oneshot::channel();
    let query_control = services.query_control.clone();
    let query_execution = services.query_execution.clone();
    let topology = services.backend_topology.clone();
    let role = services.execution_role;
    let server =
        novarocks::server::run_standalone_server_with_config_until_shutdown_with_session_factory(
            config,
            config_path,
            port_override,
            novarocks::server::StandaloneGrpcEndpointOwnership::ExternallyHosted,
            services,
            move |engine| {
                let insert_engine = engine.insert_engine();
                Ok(std::sync::Arc::new(
                    novarocks_frontend::FrontendQueryService::new(
                        engine,
                        query_control,
                        query_execution,
                        role,
                        topology,
                        dml,
                        insert_engine,
                    ),
                ))
            },
            async move {
                let _ = server_shutdown_rx.await;
            },
        );
    tokio::pin!(server);
    tokio::pin!(shutdown);

    let mut server_completed = false;
    let primary = loop {
        tokio::select! {
            server_result = &mut server => {
                server_completed = true;
                break server_result;
            }
            shutdown_result = &mut shutdown => break shutdown_result,
            _ = tokio::time::sleep(BACKEND_SUPERVISION_POLL_INTERVAL) => {
                match backend.poll_failure() {
                    Ok(Some(error)) | Err(error) => break Err(error.to_string()),
                    Ok(None) => {}
                }
            }
        }
    };

    let server_cleanup = if server_completed {
        Ok(())
    } else {
        let _ = server_shutdown_tx.send(());
        server.await
    };
    let backend_cleanup = backend.shutdown().map_err(|error| error.to_string());
    let frontend_cleanup = frontend.shutdown().await.map_err(|error| error.to_string());
    combine_primary_and_cleanup(primary, server_cleanup, backend_cleanup, frontend_cleanup)
        .map_err(anyhow::Error::msg)
}

fn combine_primary_and_cleanup(
    primary: Result<(), String>,
    server_cleanup: Result<(), String>,
    backend_cleanup: Result<(), String>,
    frontend_cleanup: Result<(), String>,
) -> Result<(), String> {
    let cleanup_errors = [
        server_cleanup.err(),
        backend_cleanup.err(),
        frontend_cleanup.err(),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>();

    match (primary, cleanup_errors.is_empty()) {
        (Ok(()), true) => Ok(()),
        (Ok(()), false) => Err(format!("cleanup failed: {}", cleanup_errors.join("; "))),
        (Err(primary), true) => Err(primary),
        (Err(primary), false) => Err(format!(
            "{primary}; cleanup failed: {}",
            cleanup_errors.join("; ")
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::combine_primary_and_cleanup;

    #[test]
    fn primary_failure_remains_primary_when_all_cleanup_steps_fail() {
        let error = combine_primary_and_cleanup(
            Err("backend failed".to_string()),
            Err("server cleanup failed".to_string()),
            Err("backend cleanup failed".to_string()),
            Err("frontend cleanup failed".to_string()),
        )
        .expect_err("backend failure must be returned");

        assert!(error.contains("backend failed"), "{error}");
        assert!(error.contains("server cleanup failed"), "{error}");
        assert!(error.contains("frontend cleanup failed"), "{error}");
        assert!(error.contains("backend cleanup failed"), "{error}");
    }
}
