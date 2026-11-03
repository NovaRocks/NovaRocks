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

use anyhow::anyhow;
use tokio::sync::watch;
type RoleJobResult = anyhow::Result<()>;

enum PrimaryRole {
    Frontend(RoleJobResult),
    Backend(RoleJobResult),
    Shutdown,
}

fn role_outcome(role: &str, result: RoleJobResult) -> Result<(), String> {
    match result {
        Ok(()) => Ok(()),
        Err(error) => Err(format!("{role} role failed: {error:#}")),
    }
}

fn unexpected_exit(role: &str, result: Result<(), String>) -> String {
    match result {
        Ok(()) => format!("{role} role stopped unexpectedly"),
        Err(error) => error,
    }
}

/// Run the normal frontend and backend applications as one supervised process.
///
/// The first role failure (or termination signal) is the sole shutdown
/// authority. A normal termination drains the frontend before it stops the
/// backend, preserving the deployable FE/BE shutdown ordering even in the
/// all-in-one test topology.
pub async fn supervise_all_in_one<Frontend, Backend, Shutdown>(
    frontend: Frontend,
    backend: Backend,
    frontend_stop_tx: watch::Sender<bool>,
    backend_stop_tx: watch::Sender<bool>,
    shutdown: Shutdown,
) -> anyhow::Result<()>
where
    Frontend: Future<Output = anyhow::Result<()>>,
    Backend: Future<Output = anyhow::Result<()>>,
    Shutdown: Future<Output = ()>,
{
    // Design: ADR-0108 (docs/adr/ADR-0108-native-role-launch-and-management-surfaces.md)
    tokio::pin!(frontend);
    tokio::pin!(backend);
    let primary = tokio::select! {
        result = &mut frontend => PrimaryRole::Frontend(result),
        result = &mut backend => PrimaryRole::Backend(result),
        _ = shutdown => PrimaryRole::Shutdown,
    };
    match primary {
        PrimaryRole::Frontend(result) => {
            let primary = unexpected_exit("frontend", role_outcome("frontend", result));
            let _ = backend_stop_tx.send(true);
            match role_outcome("backend cleanup", backend.await) {
                Ok(()) => Err(anyhow!(primary)),
                Err(cleanup) => Err(anyhow!("{primary}; cleanup: {cleanup}")),
            }
        }
        PrimaryRole::Backend(result) => {
            let primary = unexpected_exit("backend", role_outcome("backend", result));
            let _ = frontend_stop_tx.send(true);
            match role_outcome("frontend cleanup", frontend.await) {
                Ok(()) => Err(anyhow!(primary)),
                Err(cleanup) => Err(anyhow!("{primary}; cleanup: {cleanup}")),
            }
        }
        PrimaryRole::Shutdown => {
            // The FE owns admission. Let it stop new work and finish (or apply
            // its own deadline cancellation) before withdrawing BE execution.
            let _ = frontend_stop_tx.send(true);
            let frontend = role_outcome("frontend cleanup", frontend.await);
            let _ = backend_stop_tx.send(true);
            let backend = role_outcome("backend cleanup", backend.await);
            match (frontend, backend) {
                (Ok(()), Ok(())) => Ok(()),
                (Err(frontend), Ok(())) => Err(anyhow!("{frontend}")),
                (Ok(()), Err(backend)) => Err(anyhow!("{backend}")),
                (Err(frontend), Err(backend)) => Err(anyhow!("{frontend}; cleanup: {backend}")),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::supervise_all_in_one;

    async fn wait_for_stop(mut receiver: tokio::sync::watch::Receiver<bool>) {
        while !*receiver.borrow() {
            receiver.changed().await.expect("supervisor owns sender");
        }
    }

    #[test]
    fn frontend_failure_stops_backend_before_returning() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        runtime.block_on(async {
            let (frontend_stop_tx, _frontend_stop_rx) = tokio::sync::watch::channel(false);
            let (backend_stop_tx, mut backend_stop_rx) = tokio::sync::watch::channel(false);
            let (stopped_tx, stopped_rx) = tokio::sync::oneshot::channel();
            let result = supervise_all_in_one(
                async { Err(anyhow::anyhow!("frontend listener failed")) },
                async move {
                    while !*backend_stop_rx.borrow() {
                        backend_stop_rx
                            .changed()
                            .await
                            .expect("supervisor owns sender");
                    }
                    stopped_tx.send(()).expect("record backend stop");
                    Ok(())
                },
                frontend_stop_tx,
                backend_stop_tx,
                future::pending(),
            )
            .await;

            assert!(result.is_err());
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("frontend listener failed")
            );
            stopped_rx
                .await
                .expect("backend must observe stop before return");
        });
    }

    #[test]
    fn backend_failure_stops_frontend_before_returning() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        runtime.block_on(async {
            let (frontend_stop_tx, mut frontend_stop_rx) = tokio::sync::watch::channel(false);
            let (backend_stop_tx, _backend_stop_rx) = tokio::sync::watch::channel(false);
            let (stopped_tx, stopped_rx) = tokio::sync::oneshot::channel();
            let result = supervise_all_in_one(
                async move {
                    while !*frontend_stop_rx.borrow() {
                        frontend_stop_rx
                            .changed()
                            .await
                            .expect("supervisor owns sender");
                    }
                    stopped_tx.send(()).expect("record frontend stop");
                    Ok(())
                },
                async { Err(anyhow::anyhow!("backend listener failed")) },
                frontend_stop_tx,
                backend_stop_tx,
                future::pending(),
            )
            .await;

            assert!(result.is_err());
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("backend listener failed")
            );
            stopped_rx
                .await
                .expect("frontend must observe stop before return");
        });
    }

    #[test]
    fn shutdown_waits_for_both_roles() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        runtime.block_on(async {
            let (frontend_stop_tx, frontend_stop_rx) = tokio::sync::watch::channel(false);
            let (backend_stop_tx, backend_stop_rx) = tokio::sync::watch::channel(false);
            let frontend_drained = Arc::new(AtomicBool::new(false));
            let backend_checked_drain = Arc::new(AtomicBool::new(false));
            let (frontend_stopped_tx, frontend_stopped_rx) = tokio::sync::oneshot::channel();
            let (backend_stopped_tx, backend_stopped_rx) = tokio::sync::oneshot::channel();
            let frontend_drained_for_task = Arc::clone(&frontend_drained);
            let backend_checked_drain_for_task = Arc::clone(&backend_checked_drain);
            let result = supervise_all_in_one(
                async move {
                    wait_for_stop(frontend_stop_rx).await;
                    frontend_drained_for_task.store(true, Ordering::SeqCst);
                    frontend_stopped_tx.send(()).expect("record frontend stop");
                    Ok(())
                },
                async move {
                    wait_for_stop(backend_stop_rx).await;
                    assert!(
                        frontend_drained.load(Ordering::SeqCst),
                        "backend must stop only after frontend drain completes"
                    );
                    backend_checked_drain_for_task.store(true, Ordering::SeqCst);
                    backend_stopped_tx.send(()).expect("record backend stop");
                    Ok(())
                },
                frontend_stop_tx,
                backend_stop_tx,
                async {},
            )
            .await;

            assert!(result.is_ok(), "{result:?}");
            frontend_stopped_rx.await.expect("frontend stopped");
            backend_stopped_rx.await.expect("backend stopped");
            assert!(backend_checked_drain.load(Ordering::SeqCst));
        });
    }

    #[test]
    fn primary_failure_keeps_cleanup_failure_as_diagnostic() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        runtime.block_on(async {
            let (frontend_stop_tx, _frontend_stop_rx) = tokio::sync::watch::channel(false);
            let (backend_stop_tx, mut backend_stop_rx) = tokio::sync::watch::channel(false);
            let error = supervise_all_in_one(
                async { Err(anyhow::anyhow!("frontend failed first")) },
                async move {
                    while !*backend_stop_rx.borrow() {
                        backend_stop_rx
                            .changed()
                            .await
                            .expect("supervisor owns sender");
                    }
                    Err(anyhow::anyhow!("backend cleanup failed"))
                },
                frontend_stop_tx,
                backend_stop_tx,
                future::pending(),
            )
            .await
            .expect_err("primary failure must be returned");
            let diagnostic = error.to_string();
            assert!(diagnostic.contains("frontend failed first"), "{diagnostic}");
            assert!(
                diagnostic.contains("backend cleanup failed"),
                "{diagnostic}"
            );
        });
    }
}
