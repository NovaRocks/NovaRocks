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

use std::collections::HashMap;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Instant as StdInstant;

use foundationdb::Database;
use foundationdb::api::{FdbApiBuilder, NetworkRunner, NetworkStop};
use foundationdb::options::NetworkOption;
use novarocks_state_store_api::{
    StateStore, StateStoreError, StateStoreErrorKind, StateStoreOpenRequest,
};
use tokio::sync::Notify;
use tokio::time::{Instant, timeout_at};
use uuid::Uuid;

use super::FoundationDbStateStore;
use crate::FoundationDbClientConfig;

const RUNTIME_PID_ERROR: &str = "state store runtime belongs to a different process";
const FOUNDATIONDB_API_VERSION: i32 = 730;

struct ShutdownDeferredLogFields {
    lifecycle: &'static str,
    reason: &'static str,
}

const fn shutdown_deferred_log_fields() -> ShutdownDeferredLogFields {
    ShutdownDeferredLogFields {
        lifecycle: "shutdown_deferred",
        reason: "handles_not_drained",
    }
}

#[derive(Clone)]
enum ProcessNetworkState {
    Never,
    Starting {
        pid: u32,
        config: FoundationDbClientConfig,
    },
    Running {
        pid: u32,
        config: FoundationDbClientConfig,
    },
    Stopped {
        pid: u32,
    },
    Failed {
        pid: u32,
        error: StateStoreError,
    },
}

#[cfg(feature = "foundationdb-provider")]
static PROCESS_NETWORK: Mutex<ProcessNetworkState> = Mutex::new(ProcessNetworkState::Never);

#[cfg(feature = "foundationdb-provider")]
pub(super) struct FoundationDbRuntime {
    shared: Arc<FoundationDbRuntimeShared>,
    network: FoundationDbNetworkLifecycle,
}

#[cfg(feature = "foundationdb-provider")]
struct FoundationDbRuntimeShared {
    pid: u32,
    accepting: AtomicBool,
    in_flight: AtomicUsize,
    provider_handles: AtomicUsize,
    next_database_id: AtomicU64,
    databases: Mutex<HashMap<u64, Arc<Database>>>,
    drained: Notify,
}

#[cfg(feature = "foundationdb-provider")]
struct FoundationDbNetworkOwner {
    stop: Option<Box<dyn NetworkStopAction>>,
    thread: Option<JoinHandle<Result<(), StateStoreError>>>,
}

#[cfg(feature = "foundationdb-provider")]
enum FoundationDbNetworkLifecycle {
    Running(FoundationDbNetworkOwner),
    Stopping {
        thread: Option<JoinHandle<Result<(), StateStoreError>>>,
    },
    Failed {
        error: StateStoreError,
        thread: Option<JoinHandle<Result<(), StateStoreError>>>,
    },
    Stopped,
}

#[cfg(feature = "foundationdb-provider")]
trait NetworkStopAction: Send {
    fn stop(self: Box<Self>) -> Result<(), StateStoreError>;
}

#[cfg(feature = "foundationdb-provider")]
struct NativeNetworkStop(NetworkStop);

#[cfg(feature = "foundationdb-provider")]
impl NetworkStopAction for NativeNetworkStop {
    fn stop(self: Box<Self>) -> Result<(), StateStoreError> {
        self.0.stop().map_err(|_| {
            StateStoreError::new(
                StateStoreErrorKind::ProviderUnavailable,
                "FoundationDB network stop failed",
            )
        })
    }
}

#[cfg(feature = "foundationdb-provider")]
impl FoundationDbRuntime {
    pub(super) fn boot(config: FoundationDbClientConfig) -> Result<Self, StateStoreError> {
        config.validate().map_err(|_| {
            StateStoreError::new(
                StateStoreErrorKind::InvalidConfiguration,
                "FoundationDB client configuration is invalid",
            )
        })?;
        let pid = std::process::id();
        {
            let mut process = process_network_state()?;
            match &*process {
                ProcessNetworkState::Never => {
                    *process = ProcessNetworkState::Starting {
                        pid,
                        config: config.clone(),
                    };
                }
                ProcessNetworkState::Starting {
                    pid: owner_pid,
                    config: owner_config,
                }
                | ProcessNetworkState::Running {
                    pid: owner_pid,
                    config: owner_config,
                } => {
                    if *owner_pid != pid {
                        return Err(StateStoreError::new(
                            StateStoreErrorKind::InvalidConfiguration,
                            "FoundationDB network was initialized in a different process",
                        ));
                    }
                    if owner_config != &config {
                        return Err(StateStoreError::new(
                            StateStoreErrorKind::InvalidConfiguration,
                            "a different FoundationDB client configuration is already active",
                        ));
                    }
                    return Err(StateStoreError::new(
                        StateStoreErrorKind::InvalidConfiguration,
                        "FoundationDB network is already running in this process",
                    ));
                }
                ProcessNetworkState::Stopped { pid: owner_pid } => {
                    if *owner_pid != pid {
                        return Err(StateStoreError::new(
                            StateStoreErrorKind::InvalidConfiguration,
                            "FoundationDB network was stopped in a different process",
                        ));
                    }
                    return Err(StateStoreError::new(
                        StateStoreErrorKind::InvalidConfiguration,
                        "FoundationDB network is stopped and cannot restart in this process",
                    ));
                }
                ProcessNetworkState::Failed {
                    pid: owner_pid,
                    error,
                } => {
                    if *owner_pid != pid {
                        return Err(StateStoreError::new(
                            StateStoreErrorKind::InvalidConfiguration,
                            "FoundationDB network failed in a different process",
                        ));
                    }
                    return Err(error.clone());
                }
            }
        }

        let owner = match start_foundationdb_network(&config) {
            Ok(owner) => owner,
            Err(error) => {
                mark_process_network_failed(pid, error.clone());
                return Err(error);
            }
        };
        {
            let mut process = process_network_state()?;
            *process = ProcessNetworkState::Running {
                pid,
                config: config.clone(),
            };
        }
        tracing::info!(
            provider = "foundationdb",
            lifecycle = "started",
            process_id = pid,
            "FoundationDB state store runtime started"
        );

        Ok(Self {
            shared: Arc::new(FoundationDbRuntimeShared {
                pid,
                accepting: AtomicBool::new(true),
                in_flight: AtomicUsize::new(0),
                provider_handles: AtomicUsize::new(0),
                next_database_id: AtomicU64::new(1),
                databases: Mutex::new(HashMap::new()),
                drained: Notify::new(),
            }),
            network: FoundationDbNetworkLifecycle::Running(owner),
        })
    }

    pub(super) fn open_store(
        &self,
        cluster_file: &Path,
        keyspace_id: Uuid,
        request: StateStoreOpenRequest,
    ) -> futures::future::BoxFuture<'static, Result<Arc<dyn StateStore>, StateStoreError>> {
        let prepared = (|| {
            if StdInstant::now() >= request.deadline {
                return Err(shutdown_deadline_error());
            }
            let opening = self.shared.acquire_operation()?;
            let path = cluster_file.to_str().ok_or_else(|| {
                StateStoreError::new(
                    StateStoreErrorKind::InvalidConfiguration,
                    "FoundationDB cluster file path must be valid UTF-8",
                )
            })?;
            let database = Database::from_path(path).map_err(|_| {
                StateStoreError::new(
                    StateStoreErrorKind::ProviderUnavailable,
                    "FoundationDB database creation failed",
                )
            })?;
            let lease = ProviderHandle::new(Arc::clone(&self.shared), Arc::new(database))?;
            Ok((opening, lease, request))
        })();
        Box::pin(async move {
            let (opening, lease, request) = prepared?;
            let deadline = Instant::from_std(request.deadline);
            let store = timeout_at(
                deadline,
                FoundationDbStateStore::open(
                    lease,
                    request.limits,
                    request.cluster_id,
                    keyspace_id,
                ),
            )
            .await
            .map_err(|_| shutdown_deadline_error())??;
            drop(opening);
            Ok(Arc::new(store) as Arc<dyn StateStore>)
        })
    }

    pub(super) async fn shutdown_until(
        &mut self,
        deadline: StdInstant,
    ) -> Result<(), StateStoreError> {
        self.shared.validate_pid()?;
        if let Some(result) = self.network.terminal_result() {
            return result;
        }
        if self.shared.accepting.swap(false, Ordering::AcqRel) {
            tracing::info!(
                provider = "foundationdb",
                lifecycle = "stopping",
                process_id = self.shared.pid,
                "FoundationDB state store runtime stopping"
            );
        }

        let deadline = Instant::from_std(deadline);
        while !self.shared.is_drained() {
            if timeout_at(deadline, self.shared.drained.notified())
                .await
                .is_err()
            {
                let fields = shutdown_deferred_log_fields();
                tracing::warn!(
                    provider = "foundationdb",
                    lifecycle = fields.lifecycle,
                    reason = fields.reason,
                    process_id = self.shared.pid,
                    in_flight = self.shared.in_flight.load(Ordering::Acquire),
                    provider_handles = self.shared.provider_handles.load(Ordering::Acquire),
                    "FoundationDB state store runtime shutdown deferred"
                );
                return Err(shutdown_deadline_error());
            }
        }
        if Instant::now() >= deadline {
            return Err(shutdown_deadline_error());
        }

        self.shared.drop_database_registry();
        match self.network.stop_and_join_until(deadline).await {
            Ok(()) => {
                mark_process_network_stopped(self.shared.pid);
                tracing::info!(
                    provider = "foundationdb",
                    lifecycle = "stopped",
                    process_id = self.shared.pid,
                    "FoundationDB state store runtime stopped"
                );
                Ok(())
            }
            Err(error) => {
                if error.kind() != StateStoreErrorKind::DeadlineExceeded {
                    mark_process_network_failed(self.shared.pid, error.clone());
                }
                tracing::warn!(
                    provider = "foundationdb",
                    lifecycle = "stop_failed",
                    process_id = self.shared.pid,
                    error_kind = ?error.kind(),
                    "FoundationDB state store runtime stop failed"
                );
                Err(error)
            }
        }
    }

    #[cfg(test)]
    async fn shutdown(&mut self) -> Result<(), StateStoreError> {
        self.shutdown_until(StdInstant::now() + std::time::Duration::from_secs(5))
            .await
    }
}

fn shutdown_deadline_error() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::DeadlineExceeded,
        "FoundationDB state store provider deadline exceeded",
    )
}

#[cfg(feature = "foundationdb-provider")]
impl Drop for FoundationDbRuntime {
    fn drop(&mut self) {
        let shutdown_required_reason = match &mut self.network {
            FoundationDbNetworkLifecycle::Running(_) => Some("runtime_still_running"),
            FoundationDbNetworkLifecycle::Stopping { thread }
                if thread.as_ref().is_some_and(|thread| !thread.is_finished()) =>
            {
                Some("network_thread_still_stopping")
            }
            FoundationDbNetworkLifecycle::Stopping { thread } => {
                FoundationDbNetworkLifecycle::reap_finished_thread(thread);
                None
            }
            FoundationDbNetworkLifecycle::Failed { thread, .. }
                if thread.as_ref().is_some_and(|thread| !thread.is_finished()) =>
            {
                Some("network_thread_still_running")
            }
            FoundationDbNetworkLifecycle::Failed { thread, .. } => {
                FoundationDbNetworkLifecycle::reap_finished_thread(thread);
                None
            }
            FoundationDbNetworkLifecycle::Stopped => None,
        };
        if let Some(reason) = shutdown_required_reason {
            tracing::error!(
                provider = "foundationdb",
                lifecycle = "shutdown_required",
                reason,
                process_id = self.shared.pid,
                "FoundationDB state store runtime dropped before shutdown"
            );
            std::process::abort();
        }
    }
}

#[cfg(feature = "foundationdb-provider")]
impl FoundationDbNetworkLifecycle {
    fn terminal_result(&mut self) -> Option<Result<(), StateStoreError>> {
        match self {
            Self::Running(_) => None,
            Self::Stopping { .. } => None,
            Self::Stopped => Some(Ok(())),
            Self::Failed { error, thread } => {
                Self::reap_finished_thread(thread);
                Some(Err(error.clone()))
            }
        }
    }

    async fn stop_and_join_until(&mut self, deadline: Instant) -> Result<(), StateStoreError> {
        if let Self::Running(owner) = self {
            let stop = match owner.stop.take() {
                Some(stop) => stop,
                None => {
                    let error = StateStoreError::new(
                        StateStoreErrorKind::Internal,
                        "FoundationDB runtime lost its stop handle",
                    );
                    let thread = owner.thread.take();
                    *self = Self::Failed {
                        error: error.clone(),
                        thread,
                    };
                    return Err(error);
                }
            };
            let stop_result = catch_unwind(AssertUnwindSafe(|| stop.stop())).map_err(|_| {
                StateStoreError::new(
                    StateStoreErrorKind::ProviderUnavailable,
                    "FoundationDB network stop panicked",
                )
            });
            let stop_result = match stop_result {
                Ok(result) => result,
                Err(error) => Err(error),
            };
            if let Err(error) = stop_result {
                let thread = owner.thread.take();
                *self = Self::Failed {
                    error: error.clone(),
                    thread,
                };
                return Err(error);
            }
            let thread = owner.thread.take();
            *self = Self::Stopping { thread };
        }
        match self {
            Self::Stopped => return Ok(()),
            Self::Failed { error, thread } => {
                Self::reap_finished_thread(thread);
                return Err(error.clone());
            }
            Self::Running(_) => unreachable!("running lifecycle transitions before join"),
            Self::Stopping { .. } => {}
        }
        let thread = match self {
            Self::Stopping { thread } => thread,
            _ => unreachable!("network lifecycle checked above"),
        };
        let Some(handle) = thread.as_ref() else {
            let error = StateStoreError::new(
                StateStoreErrorKind::Internal,
                "FoundationDB runtime lost its network thread",
            );
            *self = Self::Failed {
                error: error.clone(),
                thread: None,
            };
            return Err(error);
        };
        while !handle.is_finished() {
            if Instant::now() >= deadline {
                return Err(shutdown_deadline_error());
            }
            tokio::task::yield_now().await;
        }
        let thread = match self {
            Self::Stopping { thread } => thread
                .take()
                .expect("stopping FoundationDB lifecycle owns its network thread"),
            _ => unreachable!("network lifecycle remains stopping until join"),
        };
        let joined = catch_unwind(AssertUnwindSafe(|| thread.join()));
        let result = match joined {
            Err(_) => Err(StateStoreError::new(
                StateStoreErrorKind::ProviderUnavailable,
                "FoundationDB network join panicked",
            )),
            Ok(Err(_)) => Err(StateStoreError::new(
                StateStoreErrorKind::ProviderUnavailable,
                "FoundationDB network thread panicked",
            )),
            Ok(Ok(Err(error))) => Err(error),
            Ok(Ok(Ok(()))) => Ok(()),
        };
        match result {
            Ok(()) => {
                *self = Self::Stopped;
                Ok(())
            }
            Err(error) => {
                *self = Self::Failed {
                    error: error.clone(),
                    thread: None,
                };
                Err(error)
            }
        }
    }

    fn reap_finished_thread(thread: &mut Option<JoinHandle<Result<(), StateStoreError>>>) {
        if thread.as_ref().is_some_and(JoinHandle::is_finished)
            && let Some(handle) = thread.take()
        {
            let _ = handle.join();
        }
    }

    #[cfg(test)]
    fn retains_join_ownership(&self) -> bool {
        matches!(
            self,
            Self::Running(FoundationDbNetworkOwner {
                thread: Some(_),
                ..
            })
        ) || matches!(self, Self::Stopping { thread: Some(_) })
            || matches!(
                self,
                Self::Failed {
                    thread: Some(_),
                    ..
                }
            )
    }

    #[cfg(test)]
    fn failed_thread_is_finished(&self) -> bool {
        matches!(self, Self::Failed { thread: Some(thread), .. } if thread.is_finished())
    }
}

#[cfg(feature = "foundationdb-provider")]
impl FoundationDbRuntimeShared {
    fn validate_pid(&self) -> Result<(), StateStoreError> {
        if self.pid != std::process::id() {
            return Err(StateStoreError::new(
                StateStoreErrorKind::InvalidConfiguration,
                RUNTIME_PID_ERROR,
            ));
        }
        Ok(())
    }

    fn validate_accepting(&self) -> Result<(), StateStoreError> {
        self.validate_pid()?;
        if !self.accepting.load(Ordering::Acquire) {
            return Err(StateStoreError::new(
                StateStoreErrorKind::ProviderUnavailable,
                "FoundationDB runtime is not accepting new handles",
            ));
        }
        Ok(())
    }

    fn is_drained(&self) -> bool {
        self.in_flight.load(Ordering::Acquire) == 0
            && self.provider_handles.load(Ordering::Acquire) == 0
    }

    fn drop_database_registry(&self) {
        match self.databases.lock() {
            Ok(mut databases) => databases.clear(),
            Err(poisoned) => poisoned.into_inner().clear(),
        }
    }

    #[allow(dead_code)]
    fn acquire_operation(self: &Arc<Self>) -> Result<OperationHandle, StateStoreError> {
        self.validate_accepting()?;
        self.in_flight.fetch_add(1, Ordering::AcqRel);
        if !self.accepting.load(Ordering::Acquire) || self.pid != std::process::id() {
            self.in_flight.fetch_sub(1, Ordering::AcqRel);
            self.drained.notify_one();
            return Err(StateStoreError::new(
                StateStoreErrorKind::ProviderUnavailable,
                "FoundationDB runtime stopped accepting operations",
            ));
        }
        Ok(OperationHandle {
            shared: Arc::clone(self),
        })
    }
}

#[cfg(feature = "foundationdb-provider")]
pub(super) struct ProviderHandle {
    shared: Arc<FoundationDbRuntimeShared>,
    database_id: u64,
}

#[cfg(feature = "foundationdb-provider")]
impl ProviderHandle {
    fn new(
        shared: Arc<FoundationDbRuntimeShared>,
        database: Arc<Database>,
    ) -> Result<Self, StateStoreError> {
        shared.validate_accepting()?;
        let database_id = shared.next_database_id.fetch_add(1, Ordering::Relaxed);
        shared.provider_handles.fetch_add(1, Ordering::AcqRel);
        match shared.databases.lock() {
            Ok(mut databases) => {
                databases.insert(database_id, database);
            }
            Err(poisoned) => {
                poisoned.into_inner().insert(database_id, database);
            }
        }
        if !shared.accepting.load(Ordering::Acquire) || shared.pid != std::process::id() {
            match shared.databases.lock() {
                Ok(mut databases) => {
                    databases.remove(&database_id);
                }
                Err(poisoned) => {
                    poisoned.into_inner().remove(&database_id);
                }
            }
            shared.provider_handles.fetch_sub(1, Ordering::AcqRel);
            shared.drained.notify_one();
            return Err(StateStoreError::new(
                StateStoreErrorKind::ProviderUnavailable,
                "FoundationDB runtime stopped accepting provider handles",
            ));
        }
        Ok(Self {
            shared,
            database_id,
        })
    }

    #[allow(dead_code)]
    pub(super) fn database(&self) -> Result<Arc<Database>, StateStoreError> {
        let databases = self.shared.databases.lock().map_err(|_| {
            StateStoreError::new(
                StateStoreErrorKind::Internal,
                "FoundationDB database registry is poisoned",
            )
        })?;
        databases.get(&self.database_id).cloned().ok_or_else(|| {
            StateStoreError::new(
                StateStoreErrorKind::ProviderUnavailable,
                "FoundationDB database handle is closed",
            )
        })
    }

    #[allow(dead_code)]
    pub(super) fn acquire_operation(&self) -> Result<OperationHandle, StateStoreError> {
        self.shared.acquire_operation()
    }
}

#[cfg(feature = "foundationdb-provider")]
impl Drop for ProviderHandle {
    fn drop(&mut self) {
        let database = match self.shared.databases.lock() {
            Ok(mut databases) => databases.remove(&self.database_id),
            Err(poisoned) => poisoned.into_inner().remove(&self.database_id),
        };
        drop(database);
        self.shared.provider_handles.fetch_sub(1, Ordering::AcqRel);
        self.shared.drained.notify_one();
    }
}

#[cfg(feature = "foundationdb-provider")]
#[allow(dead_code)]
pub(super) struct OperationHandle {
    shared: Arc<FoundationDbRuntimeShared>,
}

#[cfg(feature = "foundationdb-provider")]
impl Drop for OperationHandle {
    fn drop(&mut self) {
        self.shared.in_flight.fetch_sub(1, Ordering::AcqRel);
        self.shared.drained.notify_one();
    }
}

#[cfg(feature = "foundationdb-provider")]
fn start_foundationdb_network(
    config: &FoundationDbClientConfig,
) -> Result<FoundationDbNetworkOwner, StateStoreError> {
    let (max_api_version, selected_api_version) =
        foundationdb_api_versions(foundationdb::api::get_max_api_version())?;
    tracing::info!(
        provider = "foundationdb",
        api_max_version = max_api_version,
        api_selected_version = selected_api_version,
        "FoundationDB API version selected"
    );

    let initialized = catch_unwind(AssertUnwindSafe(|| {
        let mut network = FdbApiBuilder::default()
            .set_runtime_version(selected_api_version)
            .build()
            .map_err(|_| ())?;
        network = network
            .set_option(NetworkOption::DisableMultiVersionClientApi)
            .map_err(|_| ())?;
        if let Some(password) = config.tls_password.as_ref() {
            network = network
                .set_option(NetworkOption::TLSPassword(
                    password.expose_secret().to_owned(),
                ))
                .map_err(|_| ())?;
        }
        if let Some(path) = config.tls_key_path.as_deref() {
            network = network
                .set_option(NetworkOption::TLSKeyPath(
                    path.to_str().ok_or(())?.to_owned(),
                ))
                .map_err(|_| ())?;
        }
        if let Some(path) = config.tls_cert_path.as_deref() {
            network = network
                .set_option(NetworkOption::TLSCertPath(
                    path.to_str().ok_or(())?.to_owned(),
                ))
                .map_err(|_| ())?;
        }
        if let Some(path) = config.tls_ca_path.as_deref() {
            network = network
                .set_option(NetworkOption::TLSCaPath(
                    path.to_str().ok_or(())?.to_owned(),
                ))
                .map_err(|_| ())?;
        }
        if let Some(peers) = config.tls_verify_peers.as_deref() {
            network = network
                .set_option(NetworkOption::TLSVerifyPeers(peers.as_bytes().to_vec()))
                .map_err(|_| ())?;
        }
        network.build().map_err(|_| ())
    }))
    .map_err(|_| {
        StateStoreError::new(
            StateStoreErrorKind::ProviderUnavailable,
            "FoundationDB network initialization panicked",
        )
    })?
    .map_err(|_| {
        StateStoreError::new(
            StateStoreErrorKind::ProviderUnavailable,
            "FoundationDB network initialization failed",
        )
    })?;
    let (runner, wait) = initialized;
    let thread = std::thread::Builder::new()
        .name("novarocks-foundationdb-network".to_owned())
        .spawn(move || {
            catch_unwind(AssertUnwindSafe(|| unsafe { NetworkRunner::run(runner) }))
                .map_err(|_| {
                    StateStoreError::new(
                        StateStoreErrorKind::ProviderUnavailable,
                        "FoundationDB network runner panicked",
                    )
                })?
                .map_err(|_| {
                    StateStoreError::new(
                        StateStoreErrorKind::ProviderUnavailable,
                        "FoundationDB network runner failed",
                    )
                })
        })
        .map_err(|_| {
            StateStoreError::new(
                StateStoreErrorKind::ProviderUnavailable,
                "FoundationDB network thread could not be started",
            )
        })?;
    finish_foundationdb_network_start(thread, || {
        catch_unwind(AssertUnwindSafe(|| wait.wait()))
            .map(|stop| Box::new(NativeNetworkStop(stop)) as Box<dyn NetworkStopAction>)
            .map_err(|_| {
                StateStoreError::new(
                    StateStoreErrorKind::ProviderUnavailable,
                    "FoundationDB network startup wait panicked",
                )
            })
    })
}

#[cfg(feature = "foundationdb-provider")]
fn foundationdb_api_versions(max_api_version: i32) -> Result<(i32, i32), StateStoreError> {
    if max_api_version < FOUNDATIONDB_API_VERSION {
        return Err(StateStoreError::new(
            StateStoreErrorKind::InvalidConfiguration,
            "FoundationDB client does not support API version 730",
        ));
    }
    Ok((max_api_version, FOUNDATIONDB_API_VERSION))
}

#[cfg(feature = "foundationdb-provider")]
fn finish_foundationdb_network_start(
    thread: JoinHandle<Result<(), StateStoreError>>,
    wait: impl FnOnce() -> Result<Box<dyn NetworkStopAction>, StateStoreError>,
) -> Result<FoundationDbNetworkOwner, StateStoreError> {
    match wait() {
        Ok(stop) => Ok(FoundationDbNetworkOwner {
            stop: Some(stop),
            thread: Some(thread),
        }),
        Err(error) => {
            // NetworkWait only panics when its shared mutex is poisoned. The runner
            // uses that same mutex before entering the native loop, so it has already
            // terminated and can be joined without a stop handle.
            let _ = catch_unwind(AssertUnwindSafe(|| thread.join()));
            Err(error)
        }
    }
}

#[cfg(feature = "foundationdb-provider")]
fn process_network_state()
-> Result<std::sync::MutexGuard<'static, ProcessNetworkState>, StateStoreError> {
    PROCESS_NETWORK.lock().map_err(|_| {
        StateStoreError::new(
            StateStoreErrorKind::Internal,
            "FoundationDB process runtime state is poisoned",
        )
    })
}

#[cfg(feature = "foundationdb-provider")]
fn mark_process_network_stopped(pid: u32) {
    match PROCESS_NETWORK.lock() {
        Ok(mut process) => *process = ProcessNetworkState::Stopped { pid },
        Err(poisoned) => *poisoned.into_inner() = ProcessNetworkState::Stopped { pid },
    }
}

#[cfg(feature = "foundationdb-provider")]
fn mark_process_network_failed(pid: u32, error: StateStoreError) {
    match PROCESS_NETWORK.lock() {
        Ok(mut process) => *process = ProcessNetworkState::Failed { pid, error },
        Err(poisoned) => *poisoned.into_inner() = ProcessNetworkState::Failed { pid, error },
    }
}

#[cfg(all(test, feature = "foundationdb-provider"))]
mod tests {
    #![allow(
        clippy::await_holding_lock,
        reason = "tests serialize process-global FoundationDB lifecycle state across await points"
    )]

    use super::*;
    use std::process::Command;
    use std::sync::mpsc;
    use std::time::Duration;

    static TEST_PROCESS_STATE: Mutex<()> = Mutex::new(());

    struct TestNetworkStop {
        action: Box<dyn FnOnce() -> Result<(), StateStoreError> + Send>,
    }

    impl NetworkStopAction for TestNetworkStop {
        fn stop(self: Box<Self>) -> Result<(), StateStoreError> {
            (self.action)()
        }
    }

    fn test_stop(
        action: impl FnOnce() -> Result<(), StateStoreError> + Send + 'static,
    ) -> Box<dyn NetworkStopAction> {
        Box::new(TestNetworkStop {
            action: Box::new(action),
        })
    }

    fn test_runtime(
        stop: Box<dyn NetworkStopAction>,
        thread: JoinHandle<Result<(), StateStoreError>>,
    ) -> FoundationDbRuntime {
        FoundationDbRuntime {
            shared: Arc::new(FoundationDbRuntimeShared {
                pid: std::process::id(),
                accepting: AtomicBool::new(true),
                in_flight: AtomicUsize::new(0),
                provider_handles: AtomicUsize::new(0),
                next_database_id: AtomicU64::new(1),
                databases: Mutex::new(HashMap::new()),
                drained: Notify::new(),
            }),
            network: FoundationDbNetworkLifecycle::Running(FoundationDbNetworkOwner {
                stop: Some(stop),
                thread: Some(thread),
            }),
        }
    }

    fn reset_process_state() {
        *PROCESS_NETWORK
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = ProcessNetworkState::Never;
    }

    #[test]
    fn foundationdb_api_selection_reports_max_and_fixed_selected_version() {
        assert_eq!(
            foundationdb_api_versions(740).expect("supported API"),
            (740, 730)
        );
        let error = foundationdb_api_versions(729).expect_err("old client must fail closed");
        assert_eq!(error.kind(), StateStoreErrorKind::InvalidConfiguration);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn shutdown_stop_failure_is_stable_and_retains_join_ownership() {
        let _guard = TEST_PROCESS_STATE.lock().unwrap();
        reset_process_state();
        let (release_tx, release_rx) = mpsc::channel();
        let thread = std::thread::spawn(move || {
            release_rx.recv().expect("release failed network thread");
            Ok(())
        });
        let expected = StateStoreError::new(
            StateStoreErrorKind::ProviderUnavailable,
            "injected network stop failure",
        );
        let injected = expected.clone();
        let mut runtime = test_runtime(test_stop(move || Err(injected)), thread);

        let first = runtime
            .shutdown()
            .await
            .expect_err("stop failure must surface");
        assert_eq!(first, expected);
        assert!(runtime.network.retains_join_ownership());
        let second = runtime
            .shutdown()
            .await
            .expect_err("repeated shutdown must return the stable failure");
        assert_eq!(second, expected);
        assert!(runtime.network.retains_join_ownership());

        release_tx.send(()).expect("release failed network thread");
        while !runtime.network.failed_thread_is_finished() {
            std::thread::yield_now();
        }
        let third = runtime
            .shutdown()
            .await
            .expect_err("reaping must preserve the stable failure");
        assert_eq!(third, expected);
        assert!(!runtime.network.retains_join_ownership());
        reset_process_state();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn shutdown_join_failure_is_stable_instead_of_becoming_success() {
        let _guard = TEST_PROCESS_STATE.lock().unwrap();
        reset_process_state();
        let expected = StateStoreError::new(
            StateStoreErrorKind::ProviderUnavailable,
            "injected network runner failure",
        );
        let injected = expected.clone();
        let thread = std::thread::spawn(move || Err(injected));
        let mut runtime = test_runtime(test_stop(|| Ok(())), thread);

        let first = runtime
            .shutdown()
            .await
            .expect_err("join failure must surface");
        assert_eq!(first, expected);
        let second = runtime
            .shutdown()
            .await
            .expect_err("consumed join failure must remain terminal");
        assert_eq!(second, expected);
        reset_process_state();
    }

    #[test]
    fn startup_wait_failure_joins_the_spawned_thread_and_poison_is_stable() {
        let _guard = TEST_PROCESS_STATE.lock().unwrap();
        reset_process_state();
        let completed = Arc::new(AtomicBool::new(false));
        let thread_completed = Arc::clone(&completed);
        let thread = std::thread::spawn(move || {
            thread_completed.store(true, Ordering::Release);
            Err(StateStoreError::new(
                StateStoreErrorKind::ProviderUnavailable,
                "injected startup runner failure",
            ))
        });
        let wait_error = StateStoreError::new(
            StateStoreErrorKind::ProviderUnavailable,
            "injected startup wait failure",
        );

        let result = finish_foundationdb_network_start(thread, || Err(wait_error.clone()));
        let result = match result {
            Ok(_) => panic!("startup wait failure must surface"),
            Err(error) => error,
        };
        assert_eq!(result, wait_error);
        assert!(completed.load(Ordering::Acquire));
        mark_process_network_failed(std::process::id(), wait_error.clone());
        let repeated = FoundationDbRuntime::boot(FoundationDbClientConfig {
            disable_multi_version_client: true,
            tls_cert_path: None,
            tls_key_path: None,
            tls_ca_path: None,
            tls_verify_peers: None,
            tls_password: None,
        });
        let repeated = match repeated {
            Ok(_) => panic!("poisoned process runtime must reject later boot"),
            Err(error) => error,
        };
        assert_eq!(repeated, wait_error);
        reset_process_state();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn operation_handle_blocks_five_second_shutdown_then_allows_retry() {
        let _guard = TEST_PROCESS_STATE.lock().unwrap();
        reset_process_state();
        let (stop_tx, stop_rx) = mpsc::channel();
        let thread = std::thread::spawn(move || {
            stop_rx.recv().expect("network stop signal");
            Ok(())
        });
        let mut runtime = test_runtime(
            test_stop(move || {
                stop_tx.send(()).expect("signal network stop");
                Ok(())
            }),
            thread,
        );
        let operation = runtime
            .shared
            .acquire_operation()
            .expect("acquire real operation handle");

        let started = StdInstant::now();
        let timeout = runtime
            .shutdown()
            .await
            .expect_err("live operation must block shutdown");
        assert_eq!(timeout.kind(), StateStoreErrorKind::DeadlineExceeded);
        assert!(started.elapsed() >= Duration::from_millis(4_900));
        drop(operation);
        runtime
            .shutdown()
            .await
            .expect("shutdown must retry after operation drain");
        reset_process_state();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn shutdown_deadline_retains_stopping_thread_for_retry() {
        let _guard = TEST_PROCESS_STATE.lock().unwrap();
        reset_process_state();
        let (release_tx, release_rx) = mpsc::channel();
        let thread = std::thread::spawn(move || {
            release_rx.recv().expect("release stopping network thread");
            Ok(())
        });
        let mut runtime = test_runtime(test_stop(|| Ok(())), thread);

        let timeout = runtime
            .shutdown_until(StdInstant::now() + Duration::from_millis(20))
            .await
            .expect_err("unfinished network thread must respect shutdown deadline");
        assert_eq!(timeout.kind(), StateStoreErrorKind::DeadlineExceeded);
        assert!(runtime.network.retains_join_ownership());

        release_tx
            .send(())
            .expect("release stopping network thread");
        runtime
            .shutdown_until(StdInstant::now() + Duration::from_secs(1))
            .await
            .expect("retry must join the retained network thread");
        reset_process_state();
    }

    #[test]
    fn failed_runtime_with_running_thread_drop_fails_fast() {
        let child = Command::new(std::env::current_exe().expect("current test binary"))
            .args([
                "--ignored",
                "--exact",
                "state_store::foundationdb::runtime::tests::failed_runtime_with_running_thread_drop_child",
                "--nocapture",
                "--test-threads=1",
            ])
            .status()
            .expect("exec failed runtime drop child");
        assert!(
            !child.success(),
            "dropping a failed runtime with a live network thread must fail fast"
        );
    }

    #[test]
    fn running_runtime_drop_fails_fast() {
        let child = Command::new(std::env::current_exe().expect("current test binary"))
            .args([
                "--ignored",
                "--exact",
                "state_store::foundationdb::runtime::tests::running_runtime_drop_child",
                "--nocapture",
                "--test-threads=1",
            ])
            .status()
            .expect("exec running runtime drop child");
        assert!(
            !child.success(),
            "dropping a running runtime must fail fast instead of detaching the network"
        );
    }

    #[test]
    #[ignore = "exec helper used by running_runtime_drop_fails_fast"]
    fn running_runtime_drop_child() {
        let runtime = test_runtime(test_stop(|| Ok(())), std::thread::spawn(|| Ok(())));
        drop(runtime);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stopped_runtime_drop_is_safe() {
        let _guard = TEST_PROCESS_STATE.lock().unwrap();
        reset_process_state();
        let (stop_tx, stop_rx) = mpsc::channel();
        let mut runtime = test_runtime(
            test_stop(move || {
                stop_tx.send(()).expect("signal network stop");
                Ok(())
            }),
            std::thread::spawn(move || {
                stop_rx.recv().expect("network stop signal");
                Ok(())
            }),
        );
        runtime.shutdown().await.expect("stop test runtime");
        drop(runtime);
        reset_process_state();
    }

    #[tokio::test(flavor = "current_thread")]
    #[ignore = "exec helper used by failed_runtime_with_running_thread_drop_fails_fast"]
    async fn failed_runtime_with_running_thread_drop_child() {
        let stop_calls = Arc::new(AtomicUsize::new(0));
        let observed_stop_calls = Arc::clone(&stop_calls);
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let thread = std::thread::spawn(move || {
            release_rx.recv().expect("release failed network thread");
            Ok(())
        });
        let mut runtime = test_runtime(
            test_stop(move || {
                observed_stop_calls.fetch_add(1, Ordering::AcqRel);
                Err(StateStoreError::new(
                    StateStoreErrorKind::ProviderUnavailable,
                    "injected network stop failure",
                ))
            }),
            thread,
        );
        runtime
            .shutdown()
            .await
            .expect_err("injected stop failure must enter Failed");
        assert_eq!(stop_calls.load(Ordering::Acquire), 1);
        std::mem::forget(release_tx);
        drop(runtime);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn failed_runtime_with_finished_thread_drop_is_safe() {
        let stop_calls = Arc::new(AtomicUsize::new(0));
        let observed_stop_calls = Arc::clone(&stop_calls);
        let (release_tx, release_rx) = mpsc::channel();
        let thread = std::thread::spawn(move || {
            release_rx.recv().expect("release failed network thread");
            Ok(())
        });
        let mut runtime = test_runtime(
            test_stop(move || {
                observed_stop_calls.fetch_add(1, Ordering::AcqRel);
                Err(StateStoreError::new(
                    StateStoreErrorKind::ProviderUnavailable,
                    "injected network stop failure",
                ))
            }),
            thread,
        );
        runtime
            .shutdown()
            .await
            .expect_err("injected stop failure must enter Failed");
        release_tx.send(()).expect("release failed network thread");
        while !runtime.network.failed_thread_is_finished() {
            std::thread::yield_now();
        }
        drop(runtime);
        assert_eq!(stop_calls.load(Ordering::Acquire), 1);
    }
}
