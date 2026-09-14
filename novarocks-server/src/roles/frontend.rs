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

//! Production supervision for the native Frontend role.

use std::future::Future;
use std::sync::Arc;

use novarocks_frontend::{
    FrontendApplicationError, FrontendApplicationOpenConfig, FrontendManagementConfig,
    FrontendServingConfig, open_frontend_application_for_server,
    serve_ready_frontend_session_factory, shutdown_frontend_application_to_convergence,
    start_frontend_management_server,
};
use tokio::runtime::Handle;

#[derive(Clone)]
pub struct FrontendRoleConfig {
    pub application: FrontendApplicationOpenConfig,
    /// The one memory capacity authority this OS process was given.
    ///
    /// Under `all-in-one` this is the very same handle the backend role holds:
    /// two roles in one address space share one bound.
    pub memory_authority: Arc<novarocks_memory::MemoryAuthority>,
    pub management: FrontendManagementConfig,
    pub serving: FrontendServingConfig,
    pub mv_storage_observation: Arc<dyn novarocks_spi::connector::MvStorageObservationPort>,
}

/// Runs one already-composed Frontend role until its process owner requests
/// shutdown or a role-owned listener reports a failure.
pub async fn run_until_shutdown<F>(
    config: FrontendRoleConfig,
    data_runtime: Handle,
    shutdown: F,
) -> Result<(), FrontendApplicationError>
where
    F: Future<Output = ()> + Send,
{
    let mv_storage_observation = Arc::clone(&config.mv_storage_observation);
    let cleanup_timeout = config.serving.frontend_cleanup_timeout;
    let mut management_server = start_frontend_management_server(&config.management)?;
    let mut host =
        match open_frontend_application_for_server(&config.application, data_runtime).await {
            Ok(host) => host,
            Err(error) => {
                let cleanup = management_server
                    .stop()
                    .map_err(FrontendApplicationError::server);
                return combine(Err(error), cleanup);
            }
        };
    if let Err(error) = management_server.install(&host) {
        let shutdown =
            shutdown_frontend_application_to_convergence(&mut host, cleanup_timeout).await;
        let cleanup = management_server
            .stop()
            .map_err(FrontendApplicationError::server);
        return combine(Err(error), combine(shutdown, cleanup));
    }
    let serving = serve_ready_frontend_session_factory(
        config.serving,
        &mut host,
        mv_storage_observation,
        shutdown,
        &mut management_server,
    )
    .await;
    let shutdown = shutdown_frontend_application_to_convergence(&mut host, cleanup_timeout).await;
    let cleanup = management_server
        .stop()
        .map_err(FrontendApplicationError::server);
    combine(combine(serving, shutdown), cleanup)
}

fn combine(
    primary: Result<(), FrontendApplicationError>,
    cleanup: Result<(), FrontendApplicationError>,
) -> Result<(), FrontendApplicationError> {
    match (primary, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(primary), Ok(())) => Err(primary),
        (Ok(()), Err(cleanup)) => Err(cleanup),
        (Err(primary), Err(cleanup)) => Err(primary.with_cleanup_context(cleanup)),
    }
}

#[cfg(test)]
mod tests {
    use super::combine;
    use novarocks_frontend::FrontendApplicationError;

    #[test]
    fn cleanup_failure_keeps_the_primary_role_failure() {
        let error = combine(
            Err(FrontendApplicationError::server("serve failed")),
            Err(FrontendApplicationError::server("cleanup failed")),
        )
        .expect_err("primary failure must remain visible");

        assert_eq!(
            error.kind(),
            novarocks_frontend::FrontendApplicationErrorKind::Server
        );
        assert!(error.to_string().contains("serve failed"));
        assert!(error.to_string().contains("cleanup failed"));
    }
}
