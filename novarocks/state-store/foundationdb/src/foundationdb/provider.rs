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

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::future::BoxFuture;
use novarocks_spi::state_store::{
    MAX_KEY_BYTES, StateStore, StateStoreError, StateStoreErrorKind, StateStoreOpenRequest,
    StateStoreProviderDescriptor, StateStoreProviderFactory, StateStoreProviderInstance,
    StateStoreProviderLifecycle,
};
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use super::runtime::FoundationDbRuntime;
use crate::{
    FOUNDATIONDB_STATE_STORE_PROVIDER_ID, FoundationDbClientConfig, FoundationDbProviderConfig,
};
#[cfg(feature = "state-store-test-hooks")]
use crate::{
    FoundationDbTestProviderConfig, FoundationDbTestStoreConfig, test_config::resolve_test_limits,
};

trait FoundationDbProviderRuntime: Send + 'static {
    fn open_store(
        &self,
        cluster_file: PathBuf,
        keyspace_id: Uuid,
        request: StateStoreOpenRequest,
    ) -> BoxFuture<'static, Result<Arc<dyn StateStore>, StateStoreError>>;

    fn shutdown_until(&mut self, deadline: Instant) -> BoxFuture<'_, Result<(), StateStoreError>>;
}

impl FoundationDbProviderRuntime for FoundationDbRuntime {
    fn open_store(
        &self,
        cluster_file: PathBuf,
        keyspace_id: Uuid,
        request: StateStoreOpenRequest,
    ) -> BoxFuture<'static, Result<Arc<dyn StateStore>, StateStoreError>> {
        FoundationDbRuntime::open_store(self, &cluster_file, keyspace_id, request)
    }

    fn shutdown_until(&mut self, deadline: Instant) -> BoxFuture<'_, Result<(), StateStoreError>> {
        Box::pin(FoundationDbRuntime::shutdown_until(self, deadline))
    }
}

struct FoundationDbRuntimeOwner {
    commands: Option<mpsc::Sender<FoundationDbShutdownCommand>>,
    terminal: Arc<Mutex<Option<Result<(), StateStoreError>>>>,
}

struct FoundationDbShutdownCommand {
    deadline: Instant,
    response: oneshot::Sender<Result<(), StateStoreError>>,
}

type OpenedFoundationDbProvider =
    Result<(Arc<dyn StateStore>, FoundationDbRuntimeOwner), StateStoreError>;

impl FoundationDbRuntimeOwner {
    async fn shutdown(&mut self, deadline: Instant) -> Result<(), StateStoreError> {
        let Some(commands) = self.commands.as_ref() else {
            return self.terminal_result().unwrap_or(Ok(()));
        };
        let (response, receiver) = oneshot::channel();
        if commands
            .send(FoundationDbShutdownCommand { deadline, response })
            .await
            .is_err()
        {
            return self.terminal_result().unwrap_or_else(|| {
                Err(StateStoreError::new(
                    StateStoreErrorKind::ProviderUnavailable,
                    "FoundationDB runtime owner stopped unexpectedly",
                ))
            });
        }
        match receiver.await {
            Ok(Ok(())) => {
                self.commands.take();
                Ok(())
            }
            Ok(Err(error)) => Err(error),
            Err(_) => self.terminal_result().unwrap_or_else(|| {
                Err(StateStoreError::new(
                    StateStoreErrorKind::ProviderUnavailable,
                    "FoundationDB runtime shutdown owner stopped unexpectedly",
                ))
            }),
        }
    }

    fn terminal_result(&self) -> Option<Result<(), StateStoreError>> {
        match self.terminal.lock() {
            Ok(result) => result.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        }
    }
}

async fn open_provider_with_runtime<R: FoundationDbProviderRuntime>(
    runtime: R,
    cluster_file: PathBuf,
    keyspace_id: Uuid,
    request: StateStoreOpenRequest,
) -> Result<(Arc<dyn StateStore>, FoundationDbRuntimeOwner), StateStoreError> {
    let (opened, receiver) = oneshot::channel();
    tokio::spawn(run_foundationdb_runtime_owner(
        runtime,
        cluster_file,
        keyspace_id,
        request,
        opened,
    ));
    receiver.await.map_err(|_| {
        StateStoreError::new(
            StateStoreErrorKind::ProviderUnavailable,
            "FoundationDB runtime owner stopped during open",
        )
    })?
}

async fn run_foundationdb_runtime_owner<R: FoundationDbProviderRuntime>(
    mut runtime: R,
    cluster_file: PathBuf,
    keyspace_id: Uuid,
    request: StateStoreOpenRequest,
    opened: oneshot::Sender<OpenedFoundationDbProvider>,
) {
    let deadline = request.deadline;
    let open = tokio::time::timeout_at(
        tokio::time::Instant::from_std(deadline),
        runtime.open_store(cluster_file, keyspace_id, request),
    )
    .await
    .unwrap_or_else(|_| Err(provider_deadline_error()));
    let store = match open {
        Ok(store) => store,
        Err(open) => {
            let cleanup = runtime.shutdown_until(deadline).await;
            let error = match cleanup {
                Ok(()) => open,
                Err(cleanup) => open.with_cleanup_context(cleanup),
            };
            let cleanup_failed = error.cleanup_context().is_some();
            let _ = opened.send(Err(error));
            if cleanup_failed {
                retain_foundationdb_runtime_until_safe(runtime).await;
            }
            return;
        }
    };

    let (commands, mut command_receiver) = mpsc::channel(1);
    let terminal = Arc::new(Mutex::new(None));
    let owner = FoundationDbRuntimeOwner {
        commands: Some(commands),
        terminal: Arc::clone(&terminal),
    };
    if let Err(returned) = opened.send(Ok((store, owner))) {
        drop(returned);
        retain_foundationdb_runtime_until_safe(runtime).await;
        return;
    }

    while let Some(command) = command_receiver.recv().await {
        let result = runtime.shutdown_until(command.deadline).await;
        if result.is_ok() {
            match terminal.lock() {
                Ok(mut terminal) => *terminal = Some(Ok(())),
                Err(poisoned) => *poisoned.into_inner() = Some(Ok(())),
            }
        }
        let stopped = result.is_ok();
        let _ = command.response.send(result);
        if stopped {
            return;
        }
    }
    retain_foundationdb_runtime_until_safe(runtime).await;
}

async fn retain_foundationdb_runtime_until_safe<R: FoundationDbProviderRuntime>(mut runtime: R) {
    loop {
        match runtime
            .shutdown_until(Instant::now() + Duration::from_secs(5))
            .await
        {
            Ok(()) => return,
            Err(error) if error.kind() == StateStoreErrorKind::DeadlineExceeded => {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(error) => {
                tracing::error!(
                    provider = "foundationdb",
                    lifecycle = "terminal_failure_quarantined",
                    error_kind = ?error.kind(),
                    "FoundationDB runtime retained for process lifetime after terminal shutdown failure"
                );
                quarantine_foundationdb_runtime(runtime);
                return;
            }
        }
    }
}

fn quarantine_foundationdb_runtime<R: FoundationDbProviderRuntime>(runtime: R) {
    // A terminal stop/join failure can retain a live native network thread whose
    // Drop contract aborts the process. The process-global FDB state is already
    // poisoned and cannot restart, so retain the owner for the remaining process
    // lifetime without polling or retrying a stable failure.
    let _ = Box::leak(Box::new(runtime));
}

pub struct FoundationDbStateStoreProviderFactory {
    descriptor: StateStoreProviderDescriptor,
    cluster_file: PathBuf,
    keyspace_id: Uuid,
    client: FoundationDbClientConfig,
}

impl FoundationDbStateStoreProviderFactory {
    pub fn new(
        config: FoundationDbProviderConfig,
        client: FoundationDbClientConfig,
    ) -> Result<Self, StateStoreError> {
        config.validate().map_err(|_| {
            StateStoreError::new(
                StateStoreErrorKind::InvalidConfiguration,
                "FoundationDB state store provider configuration is invalid",
            )
        })?;
        client.validate().map_err(|_| {
            StateStoreError::new(
                StateStoreErrorKind::InvalidConfiguration,
                "FoundationDB state store client configuration is invalid",
            )
        })?;
        Ok(Self {
            descriptor: StateStoreProviderDescriptor::new(
                FOUNDATIONDB_STATE_STORE_PROVIDER_ID,
                MAX_KEY_BYTES,
            ),
            cluster_file: config.cluster_file,
            keyspace_id: config.keyspace_id,
            client,
        })
    }
}

#[async_trait]
impl StateStoreProviderFactory for FoundationDbStateStoreProviderFactory {
    fn descriptor(&self) -> &StateStoreProviderDescriptor {
        &self.descriptor
    }

    async fn open(
        self: Box<Self>,
        request: StateStoreOpenRequest,
    ) -> Result<Box<dyn StateStoreProviderInstance>, StateStoreError> {
        if Instant::now() >= request.deadline {
            return Err(provider_deadline_error());
        }
        let runtime = FoundationDbRuntime::boot(self.client)?;
        let (state_store, runtime) =
            open_provider_with_runtime(runtime, self.cluster_file, self.keyspace_id, request)
                .await?;
        Ok(Box::new(FoundationDbStateStoreProviderInstance {
            descriptor: self.descriptor,
            lifecycle: StateStoreProviderLifecycle::Ready,
            state_store: Some(state_store),
            runtime: Some(runtime),
        }))
    }
}

fn provider_deadline_error() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::DeadlineExceeded,
        "FoundationDB state store provider deadline exceeded",
    )
}

pub(super) struct FoundationDbStateStoreProviderInstance {
    descriptor: StateStoreProviderDescriptor,
    lifecycle: StateStoreProviderLifecycle,
    state_store: Option<Arc<dyn StateStore>>,
    runtime: Option<FoundationDbRuntimeOwner>,
}

#[async_trait]
impl StateStoreProviderInstance for FoundationDbStateStoreProviderInstance {
    fn descriptor(&self) -> &StateStoreProviderDescriptor {
        &self.descriptor
    }

    fn lifecycle(&self) -> StateStoreProviderLifecycle {
        self.lifecycle
    }

    fn state_store(&self) -> Option<Arc<dyn StateStore>> {
        if self.lifecycle == StateStoreProviderLifecycle::Ready {
            self.state_store.clone()
        } else {
            None
        }
    }

    async fn shutdown(&mut self, deadline: Instant) -> Result<(), StateStoreError> {
        if self.lifecycle == StateStoreProviderLifecycle::Stopped {
            return Ok(());
        }
        self.lifecycle = StateStoreProviderLifecycle::Draining;
        self.state_store.take();
        let Some(runtime) = self.runtime.as_mut() else {
            self.lifecycle = StateStoreProviderLifecycle::Stopped;
            return Ok(());
        };
        runtime.shutdown(deadline).await?;
        self.runtime.take();
        self.lifecycle = StateStoreProviderLifecycle::Stopped;
        Ok(())
    }
}

#[cfg(feature = "state-store-test-hooks")]
#[doc(hidden)]
pub struct FoundationDbProviderTestHarness {
    runtime: Option<FoundationDbRuntime>,
}

#[cfg(feature = "state-store-test-hooks")]
impl FoundationDbProviderTestHarness {
    pub fn boot(config: FoundationDbClientConfig) -> Result<Self, StateStoreError> {
        Ok(Self {
            runtime: Some(FoundationDbRuntime::boot(config)?),
        })
    }

    pub async fn open_store(
        &self,
        config: FoundationDbTestStoreConfig,
        deadline: Instant,
    ) -> Result<Arc<dyn StateStore>, StateStoreError> {
        let FoundationDbTestProviderConfig::Foundationdb {
            cluster_file,
            keyspace_id,
        } = config.provider;
        let limits = resolve_test_limits(&config.limits).map_err(|_| {
            StateStoreError::new(
                StateStoreErrorKind::InvalidConfiguration,
                "FoundationDB state store limits are invalid",
            )
        })?;
        self.runtime
            .as_ref()
            .ok_or_else(|| {
                StateStoreError::new(
                    StateStoreErrorKind::ProviderUnavailable,
                    "FoundationDB test harness is stopped",
                )
            })?
            .open_store(
                &cluster_file,
                keyspace_id,
                StateStoreOpenRequest {
                    cluster_id: config.cluster_id,
                    limits,
                    deadline,
                },
            )
            .await
    }

    pub async fn shutdown(&mut self, deadline: Instant) -> Result<(), StateStoreError> {
        let Some(runtime) = self.runtime.as_mut() else {
            return Ok(());
        };
        runtime.shutdown_until(deadline).await?;
        self.runtime.take();
        Ok(())
    }
}

#[cfg(all(test, feature = "provider-internal-tests"))]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use async_trait::async_trait;
    use bytes::Bytes;
    use futures::future::BoxFuture;
    use tempfile::TempDir;
    use tokio::sync::Notify;
    use uuid::Uuid;

    use novarocks_spi::state_store::{
        ChangePage, ChangePollRequest, CommitResolution, ReadTransaction, StateStore,
        StateStoreError, StateStoreErrorKind, StateStoreLimits, StateStoreMetricsSnapshot,
        StateStoreOpenRequest, StateStoreProviderFactory, StateStoreProviderInstance,
        StoreIdentity, TransactionId, WriteTransaction,
    };

    use crate::{
        FOUNDATIONDB_STATE_STORE_PROVIDER_ID, FoundationDbClientConfig, FoundationDbProviderConfig,
    };

    #[test]
    fn foundationdb_factory_binds_the_typed_descriptor_without_network_start() {
        let temp = TempDir::new().expect("FoundationDB bind temp dir");
        let cluster_file = temp.path().join("fdb.cluster");
        std::fs::write(&cluster_file, b"test:test@127.0.0.1:4500")
            .expect("write FoundationDB cluster file");
        let factory = super::FoundationDbStateStoreProviderFactory::new(
            FoundationDbProviderConfig {
                cluster_file,
                keyspace_id: Uuid::nil(),
            },
            FoundationDbClientConfig {
                disable_multi_version_client: true,
                tls_cert_path: None,
                tls_key_path: None,
                tls_ca_path: None,
                tls_verify_peers: None,
                tls_password: None,
            },
        )
        .expect("FoundationDB factory");
        assert_eq!(
            factory.descriptor().id,
            FOUNDATIONDB_STATE_STORE_PROVIDER_ID
        );
        assert_foundationdb_instance_contract::<super::FoundationDbStateStoreProviderInstance>();
    }

    fn assert_foundationdb_instance_contract<T: StateStoreProviderInstance>() {}

    #[derive(Clone)]
    enum FakeOpen {
        Block(Arc<Notify>),
        Fail,
    }

    struct FakeRuntime {
        open: FakeOpen,
        open_entered: Arc<AtomicBool>,
        shutdown_calls: Arc<AtomicUsize>,
        shutdown_failures: Arc<AtomicUsize>,
        shutdown_failure_kind: StateStoreErrorKind,
        dropped: Arc<AtomicBool>,
        unsafe_drop: Arc<AtomicBool>,
        stopped: bool,
    }

    impl Drop for FakeRuntime {
        fn drop(&mut self) {
            if !self.stopped {
                self.unsafe_drop.store(true, Ordering::Release);
            }
            self.dropped.store(true, Ordering::Release);
        }
    }

    impl super::FoundationDbProviderRuntime for FakeRuntime {
        fn open_store(
            &self,
            _cluster_file: std::path::PathBuf,
            _keyspace_id: Uuid,
            _request: StateStoreOpenRequest,
        ) -> BoxFuture<'static, Result<Arc<dyn StateStore>, StateStoreError>> {
            self.open_entered.store(true, Ordering::Release);
            let open = self.open.clone();
            Box::pin(async move {
                match open {
                    FakeOpen::Block(release) => {
                        release.notified().await;
                        Ok(Arc::new(FakeStore) as Arc<dyn StateStore>)
                    }
                    FakeOpen::Fail => Err(StateStoreError::new(
                        StateStoreErrorKind::Corruption,
                        "injected FoundationDB open failure",
                    )),
                }
            })
        }

        fn shutdown_until(
            &mut self,
            _deadline: Instant,
        ) -> BoxFuture<'_, Result<(), StateStoreError>> {
            self.shutdown_calls.fetch_add(1, Ordering::AcqRel);
            let should_fail = self
                .shutdown_failures
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok();
            if !should_fail {
                self.stopped = true;
            }
            Box::pin(async move {
                if should_fail {
                    Err(StateStoreError::new(
                        self.shutdown_failure_kind,
                        "injected FoundationDB cleanup failure",
                    ))
                } else {
                    Ok(())
                }
            })
        }
    }

    struct FakeStore;

    #[async_trait]
    impl StateStore for FakeStore {
        fn limits(&self) -> &StateStoreLimits {
            static LIMITS: std::sync::LazyLock<StateStoreLimits> =
                std::sync::LazyLock::new(StateStoreLimits::default);
            &LIMITS
        }

        fn metrics_snapshot(&self) -> StateStoreMetricsSnapshot {
            panic!("unused fake store operation")
        }

        async fn begin_read(&self) -> Result<Box<dyn ReadTransaction>, StateStoreError> {
            panic!("unused fake store operation")
        }

        async fn begin_write(
            &self,
            _transaction_id: TransactionId,
            _purpose: &str,
        ) -> Result<Box<dyn WriteTransaction>, StateStoreError> {
            panic!("unused fake store operation")
        }

        async fn poll_changes(
            &self,
            _request: &ChangePollRequest,
        ) -> Result<ChangePage, StateStoreError> {
            panic!("unused fake store operation")
        }

        async fn identity(&self) -> Result<StoreIdentity, StateStoreError> {
            panic!("unused fake store operation")
        }

        async fn resolve_commit(
            &self,
            _transaction_id: &TransactionId,
        ) -> Result<CommitResolution, StateStoreError> {
            panic!("unused fake store operation")
        }
    }

    struct FakeRuntimeControl {
        open_entered: Arc<AtomicBool>,
        shutdown_calls: Arc<AtomicUsize>,
        dropped: Arc<AtomicBool>,
        unsafe_drop: Arc<AtomicBool>,
    }

    fn fake_runtime(open: FakeOpen, shutdown_failures: usize) -> (FakeRuntime, FakeRuntimeControl) {
        fake_runtime_with_shutdown_failure(
            open,
            shutdown_failures,
            StateStoreErrorKind::DeadlineExceeded,
        )
    }

    fn fake_runtime_with_shutdown_failure(
        open: FakeOpen,
        shutdown_failures: usize,
        shutdown_failure_kind: StateStoreErrorKind,
    ) -> (FakeRuntime, FakeRuntimeControl) {
        let control = FakeRuntimeControl {
            open_entered: Arc::new(AtomicBool::new(false)),
            shutdown_calls: Arc::new(AtomicUsize::new(0)),
            dropped: Arc::new(AtomicBool::new(false)),
            unsafe_drop: Arc::new(AtomicBool::new(false)),
        };
        (
            FakeRuntime {
                open,
                open_entered: Arc::clone(&control.open_entered),
                shutdown_calls: Arc::clone(&control.shutdown_calls),
                shutdown_failures: Arc::new(AtomicUsize::new(shutdown_failures)),
                shutdown_failure_kind,
                dropped: Arc::clone(&control.dropped),
                unsafe_drop: Arc::clone(&control.unsafe_drop),
                stopped: false,
            },
            control,
        )
    }

    fn open_request(deadline: Instant) -> StateStoreOpenRequest {
        StateStoreOpenRequest {
            cluster_id: "cluster-a".to_owned(),
            limits: StateStoreLimits::default(),
            deadline,
        }
    }

    async fn wait_for_count(counter: &AtomicUsize, expected: usize) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while counter.load(Ordering::Acquire) < expected {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("background FoundationDB owner made progress");
    }

    async fn wait_for_true(flag: &AtomicBool) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while !flag.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("background FoundationDB owner released safely");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancelled_open_waiter_hands_runtime_to_background_cleanup_owner() {
        let release = Arc::new(Notify::new());
        let (runtime, control) = fake_runtime(FakeOpen::Block(Arc::clone(&release)), 0);
        let waiter = tokio::spawn(super::open_provider_with_runtime(
            runtime,
            std::path::PathBuf::from("test.cluster"),
            Uuid::nil(),
            open_request(Instant::now() + Duration::from_secs(1)),
        ));
        while !control.open_entered.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }

        waiter.abort();
        let cancelled = match waiter.await {
            Ok(_) => panic!("open waiter must be cancelled"),
            Err(error) => error,
        };
        assert!(cancelled.is_cancelled());
        release.notify_one();
        wait_for_count(control.shutdown_calls.as_ref(), 1).await;
        wait_for_true(control.dropped.as_ref()).await;

        assert!(!control.unsafe_drop.load(Ordering::Acquire));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn open_primary_error_is_returned_while_failed_cleanup_owner_retries_safely() {
        let (runtime, control) = fake_runtime(FakeOpen::Fail, 1);

        let error = match super::open_provider_with_runtime(
            runtime,
            std::path::PathBuf::from("test.cluster"),
            Uuid::nil(),
            open_request(Instant::now() + Duration::from_secs(1)),
        )
        .await
        {
            Ok(_) => panic!("injected open must fail"),
            Err(error) => error,
        };

        assert_eq!(error.kind(), StateStoreErrorKind::Corruption);
        assert_eq!(
            error.cleanup_context().map(StateStoreError::kind),
            Some(StateStoreErrorKind::DeadlineExceeded)
        );
        wait_for_count(control.shutdown_calls.as_ref(), 2).await;
        wait_for_true(control.dropped.as_ref()).await;
        assert!(!control.unsafe_drop.load(Ordering::Acquire));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn terminal_cleanup_failure_is_quarantined_without_polling() {
        let (runtime, control) = fake_runtime_with_shutdown_failure(
            FakeOpen::Fail,
            usize::MAX,
            StateStoreErrorKind::ProviderUnavailable,
        );

        let error = match super::open_provider_with_runtime(
            runtime,
            std::path::PathBuf::from("test.cluster"),
            Uuid::nil(),
            open_request(Instant::now() + Duration::from_secs(1)),
        )
        .await
        {
            Ok(_) => panic!("fake FoundationDB open must fail"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), StateStoreErrorKind::Corruption);
        assert_eq!(
            error.cleanup_context().map(StateStoreError::kind),
            Some(StateStoreErrorKind::ProviderUnavailable)
        );

        wait_for_count(control.shutdown_calls.as_ref(), 2).await;
        let terminal_calls = control.shutdown_calls.load(Ordering::Acquire);
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert_eq!(
            control.shutdown_calls.load(Ordering::Acquire),
            terminal_calls,
            "stable terminal failure must not schedule another shutdown call"
        );
        assert!(
            !control.dropped.load(Ordering::Acquire),
            "unsafe terminal runtime must remain owned by quarantine"
        );
        assert!(!control.unsafe_drop.load(Ordering::Acquire));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn absolute_open_deadline_cancels_slow_runtime_open_and_cleans_owner() {
        let release = Arc::new(Notify::new());
        let (runtime, control) = fake_runtime(FakeOpen::Block(release), 0);

        let error = match super::open_provider_with_runtime(
            runtime,
            std::path::PathBuf::from("test.cluster"),
            Uuid::nil(),
            open_request(Instant::now() + Duration::from_millis(20)),
        )
        .await
        {
            Ok(_) => panic!("absolute deadline must bound runtime open"),
            Err(error) => error,
        };

        assert_eq!(error.kind(), StateStoreErrorKind::DeadlineExceeded);
        wait_for_count(control.shutdown_calls.as_ref(), 1).await;
        wait_for_true(control.dropped.as_ref()).await;
        assert!(!control.unsafe_drop.load(Ordering::Acquire));
    }
}
