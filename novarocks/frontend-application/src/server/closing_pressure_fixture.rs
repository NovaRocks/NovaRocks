// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Independent default-off Closing pressure composition in the original FE owner.

use super::*;
use novarocks_mysql_adapter::closing_pressure_fixture::ClosingPressureFixture;
use novarocks_native_trust::NativeProcessIdentity;

pub(super) fn bind_authorized(
    trust: &NativeTrust,
    authorization: novarocks_mysql_adapter::test_fixture_authorization::MysqlFixtureAuthorization,
    workload: novarocks_workload_control::WorkloadObservationHandle,
) -> Result<ClosingPressureFixture, FrontendApplicationError> {
    let Some(NativeProcessIdentity::Frontend(frontend)) = trust.local_process_identity() else {
        return Err(FrontendApplicationError::server(
            "Closing pressure requires the original bound FE process identity",
        ));
    };
    let mut fixture = ClosingPressureFixture::bind_authorized(authorization, frontend, workload)
        .map_err(FrontendApplicationError::server_pressure)?;
    if let Err(cause) = write_frontend_identity_marker(&mut std::io::stdout().lock(), frontend) {
        return Err(FrontendApplicationError::server_pressure(
            fixture.fail_startup_projection(cause),
        ));
    }
    Ok(fixture)
}

fn write_frontend_identity_marker(
    output: &mut impl std::io::Write,
    frontend: novarocks_types::FrontendProcessId,
) -> std::io::Result<()> {
    use std::io::Write;
    let mut bytes = [0u8; 128];
    let length = {
        let mut target = std::io::Cursor::new(&mut bytes[..]);
        writeln!(
            target,
            "NOVAROCKS_MEM_1_M07_CLOSING_PRESSURE_FE frontend_process_id={frontend}",
        )?;
        target.position() as usize
    };
    output.write_all(&bytes[..length])?;
    output.flush()
}

enum Exit {
    Mysql(Result<(), String>),
    Shutdown,
    Listener(String),
    Control(novarocks_mysql_adapter::closing_pressure_fixture::ClosingPressureFixtureError),
    OriginalTask,
}

pub(super) async fn serve<F>(
    mut fixture: ClosingPressureFixture,
    mysql_listener: ResolvedMysqlListenerSettings,
    server_version: String,
    session_factory: Arc<dyn QuerySessionFactory>,
    client_connections: Arc<MysqlClientConnectionRegistry>,
    shutdown: F,
    report_server: &mut crate::native::report_server::FrontendReportServerHandle,
    management_server: &mut FrontendManagementServer,
    host: &FrontendApplicationHost,
    drain_timeout: Duration,
    cleanup_timeout: Duration,
    trino_server: &mut novarocks_trino_adapter::TrinoServerHandle,
) -> Result<(), FrontendApplicationError>
where
    F: Future<Output = ()> + Send,
{
    let binding = match fixture.listener_binding() {
        Ok(binding) => binding,
        Err(source) => {
            fixture.fail_and_stop();
            let close = fixture
                .close_control()
                .map_err(FrontendApplicationError::server_pressure);
            return combine_server_and_shutdown(
                Err(FrontendApplicationError::server_pressure_binding(source)),
                close,
            );
        }
    };
    let original_failures = fixture.failure_observation();
    let (drain_tx, drain_rx) = tokio::sync::watch::channel(false);
    let (finalize_tx, finalize_rx) = tokio::sync::watch::channel(false);
    let wait_for_signal = |mut receiver: tokio::sync::watch::Receiver<bool>| async move {
        while !*receiver.borrow() {
            if receiver.changed().await.is_err() {
                break;
            }
        }
    };
    let ready_user = mysql_listener.user().to_string();
    let trino_port = trino_server.bound_addr().port();
    let mysql_server = novarocks_mysql_adapter::closing_pressure_fixture_listener(
        mysql_listener,
        server_version,
        Arc::clone(&session_factory),
        Arc::clone(&client_connections),
        wait_for_signal(drain_rx),
        wait_for_signal(finalize_rx),
        cleanup_timeout,
        move |addr| emit_frontend_mysql_ready(addr, &ready_user, trino_port),
        binding,
    );
    tokio::pin!(mysql_server);
    tokio::pin!(shutdown);
    let cancellation = fixture.control_cancellation().expect("authorized fixture");
    let mut control_completed = false;
    let exit = loop {
        // Only this borrow is cancelled at a sibling exit. The controller and original
        // MySQL future remain in this owner. Successful Stop disables this branch forever.
        let event = {
            let control = fixture.run_control();
            tokio::pin!(control);
            tokio::select! {
                result = &mut mysql_server => Some(Exit::Mysql(result)),
                _ = &mut shutdown => Some(Exit::Shutdown),
                _ = cancellation.cancelled() => Some(Exit::Shutdown),
                error = wait_for_frontend_listener_failure(report_server, management_server, trino_server, host)
                    => Some(Exit::Listener(error)),
                _ = original_failures.wait_for_failure() => Some(Exit::OriginalTask),
                result = &mut control, if !control_completed => match result {
                    Ok(()) => None,
                    Err(error) => Some(Exit::Control(error)),
                },
            }
        };
        if let Some(exit) = event {
            break exit;
        }
        control_completed = true;
    };
    if !matches!(&exit, Exit::Shutdown) || !control_completed {
        fixture.fail_and_stop();
    }
    let close = fixture
        .close_control()
        .map_err(FrontendApplicationError::server_pressure);
    let result = match exit {
        // The original future is already completed; never poll it again.
        Exit::Mysql(result) => {
            session_factory.cancel_all(QueryCancellationReason::ServerShutdown);
            client_connections.terminate_all(ClientConnectionTerminationReason::ServerShutdown);
            host.serving_lifecycle().mark_stopping();
            result.map_err(FrontendApplicationError::server)
        }
        exit => {
            host.begin_serving_drain(drain_timeout);
            trino_server.begin_drain();
            let _ = drain_tx.send(true);
            if matches!(&exit, Exit::Shutdown) {
                let graceful = tokio::time::timeout(
                    drain_timeout,
                    host.workload_observation()
                        .wait_until_no_root_responsibilities(),
                )
                .await;
                if graceful.is_err() {
                    host.cancel_governed_work_at_drain_deadline();
                    let _ = tokio::time::timeout(
                        cleanup_timeout,
                        host.workload_observation()
                            .wait_until_no_root_responsibilities(),
                    )
                    .await;
                }
            } else {
                host.cancel_governed_work_at_drain_deadline();
            }
            session_factory.cancel_all(QueryCancellationReason::ServerShutdown);
            client_connections.terminate_all(ClientConnectionTerminationReason::ServerShutdown);
            host.serving_lifecycle().mark_stopping();
            let _ = finalize_tx.send(true);
            let mysql = mysql_server.await.map_err(FrontendApplicationError::server);
            match exit {
                Exit::Shutdown => mysql,
                Exit::Listener(error) => {
                    combine_server_and_shutdown(Err(FrontendApplicationError::server(error)), mysql)
                }
                Exit::Control(error) => combine_server_and_shutdown(
                    Err(FrontendApplicationError::server_pressure(error)),
                    mysql,
                ),
                Exit::OriginalTask => combine_server_and_shutdown(
                    Err(FrontendApplicationError::server(
                        "original pressure MySQL task failed; retained join evidence follows",
                    )),
                    mysql,
                ),
                Exit::Mysql(_) => unreachable!("completed MySQL handled above"),
            }
        }
    };
    // Registry drain retains the original watcher/connection lifetime, including
    // an accept failure where the listener skipped its finalize hook.
    let drained = fixture
        .verify_original_connection_drain(&client_connections)
        .await
        .map_err(FrontendApplicationError::server_pressure_registry);
    let owners = fixture
        .finish_after_original_listener_join()
        .map_err(FrontendApplicationError::server_pressure);
    combine_server_and_shutdown(
        combine_server_and_shutdown(combine_server_and_shutdown(result, close), drained),
        owners,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn marker_projects_only_original_frontend_uuid() {
        let mut bytes = Vec::new();
        write_frontend_identity_marker(
            &mut bytes,
            "01890f6e-7a00-7123-8123-456789abcdef".parse().unwrap(),
        )
        .unwrap();
        assert_eq!(bytes,
            b"NOVAROCKS_MEM_1_M07_CLOSING_PRESSURE_FE frontend_process_id=01890f6e-7a00-7123-8123-456789abcdef\n");
    }

    #[derive(Debug)]
    struct OriginalSource(Arc<()>);
    impl std::fmt::Display for OriginalSource {
        fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            panic!("original IO source formatter must not run");
        }
    }
    impl std::error::Error for OriginalSource {}
    struct FailingOutput(Arc<()>);
    impl std::io::Write for FailingOutput {
        fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
            Err(std::io::Error::other(OriginalSource(self.0.clone())))
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    #[test]
    fn original_projection_io_retained_without_source_formatting() {
        let identity = Arc::new(());
        let source = write_frontend_identity_marker(
            &mut FailingOutput(identity.clone()),
            "01890f6e-7a00-7123-8123-456789abcdef".parse().unwrap(),
        )
        .unwrap_err();
        // Startup uses fail_startup_projection in the actual bound fixture. This
        // isolated error box test does not fabricate a successful fixture bind.
        let error = FrontendApplicationError::server_pressure_binding(source);
        let original = std::error::Error::source(&error)
            .unwrap()
            .downcast_ref::<std::io::Error>()
            .unwrap()
            .get_ref()
            .unwrap()
            .downcast_ref::<OriginalSource>()
            .unwrap();
        assert!(Arc::ptr_eq(&original.0, &identity));
        assert!(!format!("{error:?} {error}").contains("nonce"));
    }
}
