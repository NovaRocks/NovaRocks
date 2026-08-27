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

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::future::BoxFuture;
use novarocks_spi::state_store::{
    StateStore, StateStoreError, StateStoreErrorKind, StateStoreOpenRequest,
    StateStoreProviderDescriptor, StateStoreProviderFactory, StateStoreProviderInstance,
    StateStoreProviderLifecycle,
};
use tokio::sync::{mpsc, oneshot};

use super::runtime::MysqlRuntime;
use crate::{MYSQL_MAX_KEY_BYTES, MYSQL_STATE_STORE_PROVIDER_ID, MySqlClientConfig};

trait MysqlProviderRuntime: Send + 'static {
    fn start_open_store(
        &self,
        database: String,
        request: StateStoreOpenRequest,
    ) -> Result<MysqlProviderOpen, StateStoreError>;

    fn shutdown_until(&mut self, deadline: Instant) -> BoxFuture<'_, Result<(), StateStoreError>>;
}

impl MysqlProviderRuntime for MysqlRuntime {
    fn start_open_store(
        &self,
        database: String,
        request: StateStoreOpenRequest,
    ) -> Result<MysqlProviderOpen, StateStoreError> {
        MysqlRuntime::start_open_store(self, database, request)
    }

    fn shutdown_until(&mut self, deadline: Instant) -> BoxFuture<'_, Result<(), StateStoreError>> {
        Box::pin(MysqlRuntime::shutdown_until(self, deadline))
    }
}

pub(super) struct MysqlProviderOpen {
    pub(super) future: BoxFuture<'static, Result<Arc<dyn StateStore>, StateStoreError>>,
    cancel: Option<Box<dyn FnOnce() + Send>>,
}

impl MysqlProviderOpen {
    pub(super) fn new(
        future: BoxFuture<'static, Result<Arc<dyn StateStore>, StateStoreError>>,
        cancel: impl FnOnce() + Send + 'static,
    ) -> Self {
        Self {
            future,
            cancel: Some(Box::new(cancel)),
        }
    }

    fn cancel(&mut self) {
        if let Some(cancel) = self.cancel.take() {
            cancel();
        }
    }

    pub(super) fn complete(&mut self) {
        self.cancel.take();
    }
}

impl Drop for MysqlProviderOpen {
    fn drop(&mut self) {
        self.cancel();
    }
}

struct MysqlRuntimeOwner {
    commands: Option<mpsc::Sender<MysqlShutdownCommand>>,
    terminal: Arc<std::sync::Mutex<Option<Result<(), StateStoreError>>>>,
}

type MysqlOpenResult = Result<(Arc<dyn StateStore>, MysqlRuntimeOwner), StateStoreError>;

struct MysqlShutdownCommand {
    deadline: Instant,
    response: oneshot::Sender<Result<(), StateStoreError>>,
}

impl MysqlRuntimeOwner {
    async fn shutdown(&mut self, deadline: Instant) -> Result<(), StateStoreError> {
        let Some(commands) = self.commands.as_ref() else {
            return self.terminal_result().unwrap_or(Ok(()));
        };
        let (response, receiver) = oneshot::channel();
        if commands
            .send(MysqlShutdownCommand { deadline, response })
            .await
            .is_err()
        {
            return self.terminal_result().unwrap_or_else(|| {
                Err(StateStoreError::new(
                    StateStoreErrorKind::ProviderUnavailable,
                    "MySQL runtime owner stopped unexpectedly",
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
                    "MySQL runtime shutdown owner stopped unexpectedly",
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

struct MysqlOpenWaiterGuard {
    cancellation: Option<oneshot::Sender<()>>,
}

impl MysqlOpenWaiterGuard {
    fn complete(mut self) {
        self.cancellation.take();
    }
}

impl Drop for MysqlOpenWaiterGuard {
    fn drop(&mut self) {
        if let Some(cancellation) = self.cancellation.take() {
            let _ = cancellation.send(());
        }
    }
}

async fn open_provider_with_runtime<R: MysqlProviderRuntime>(
    runtime: R,
    database: String,
    request: StateStoreOpenRequest,
) -> MysqlOpenResult {
    let (opened, receiver) = oneshot::channel();
    let (cancellation, cancelled) = oneshot::channel();
    tokio::spawn(run_mysql_open_owner(
        runtime, database, request, cancelled, opened,
    ));
    let waiter = MysqlOpenWaiterGuard {
        cancellation: Some(cancellation),
    };
    let result = receiver.await.map_err(|_| {
        StateStoreError::new(
            StateStoreErrorKind::ProviderUnavailable,
            "MySQL runtime owner stopped during open",
        )
    });
    waiter.complete();
    result?
}

async fn run_mysql_open_owner<R: MysqlProviderRuntime>(
    runtime: R,
    database: String,
    request: StateStoreOpenRequest,
    mut cancelled: oneshot::Receiver<()>,
    opened: oneshot::Sender<MysqlOpenResult>,
) {
    let deadline = request.deadline;
    let mut opening = match runtime.start_open_store(database, request) {
        Ok(opening) => opening,
        Err(open) => {
            finish_failed_mysql_open(runtime, open, deadline, Some(opened)).await;
            return;
        }
    };
    let deadline_timer = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline));
    tokio::pin!(deadline_timer);
    let open = tokio::select! {
        result = &mut opening.future => {
            opening.complete();
            result
        }
        _ = &mut cancelled => {
            opening.cancel();
            let _ = tokio::time::timeout_at(
                tokio::time::Instant::from_std(deadline),
                &mut opening.future,
            ).await;
            opening.complete();
            finish_cancelled_mysql_open(runtime, deadline).await;
            return;
        }
        _ = &mut deadline_timer => {
            opening.cancel();
            let _ = tokio::time::timeout_at(
                tokio::time::Instant::from_std(deadline),
                &mut opening.future,
            ).await;
            opening.complete();
            finish_failed_mysql_open(
                runtime,
                mysql_provider_deadline_error(),
                deadline,
                Some(opened),
            ).await;
            return;
        }
    };
    let store = match open {
        Ok(store) => store,
        Err(open) => {
            finish_failed_mysql_open(runtime, open, deadline, Some(opened)).await;
            return;
        }
    };

    let owner = spawn_mysql_runtime_owner(runtime);
    if let Err(returned) = opened.send(Ok((store, owner))) {
        drop(returned);
    }
}

async fn finish_cancelled_mysql_open<R: MysqlProviderRuntime>(mut runtime: R, deadline: Instant) {
    if runtime.shutdown_until(deadline).await.is_err() {
        retain_mysql_runtime_until_safe(runtime).await;
    }
}

async fn finish_failed_mysql_open<R: MysqlProviderRuntime>(
    mut runtime: R,
    open: StateStoreError,
    deadline: Instant,
    opened: Option<oneshot::Sender<MysqlOpenResult>>,
) {
    let cleanup = runtime.shutdown_until(deadline).await;
    let error = match cleanup {
        Ok(()) => open,
        Err(cleanup) => open.with_cleanup_context(cleanup),
    };
    let cleanup_failed = error.cleanup_context().is_some();
    if let Some(opened) = opened {
        let _ = opened.send(Err(error));
    }
    if cleanup_failed {
        retain_mysql_runtime_until_safe(runtime).await;
    }
}

fn spawn_mysql_runtime_owner<R: MysqlProviderRuntime>(runtime: R) -> MysqlRuntimeOwner {
    let (commands, command_receiver) = mpsc::channel(1);
    let terminal = Arc::new(std::sync::Mutex::new(None));
    tokio::spawn(run_mysql_runtime_owner(
        runtime,
        command_receiver,
        Arc::clone(&terminal),
    ));
    MysqlRuntimeOwner {
        commands: Some(commands),
        terminal,
    }
}

async fn run_mysql_runtime_owner<R: MysqlProviderRuntime>(
    mut runtime: R,
    mut commands: mpsc::Receiver<MysqlShutdownCommand>,
    terminal: Arc<std::sync::Mutex<Option<Result<(), StateStoreError>>>>,
) {
    while let Some(command) = commands.recv().await {
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
    retain_mysql_runtime_until_safe(runtime).await;
}

async fn retain_mysql_runtime_until_safe<R: MysqlProviderRuntime>(mut runtime: R) {
    loop {
        if runtime
            .shutdown_until(Instant::now() + Duration::from_secs(5))
            .await
            .is_ok()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn mysql_provider_deadline_error() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::DeadlineExceeded,
        "MySQL state store provider deadline exceeded",
    )
}

pub struct MysqlStateStoreProviderFactory {
    descriptor: StateStoreProviderDescriptor,
    database: String,
    client: MySqlClientConfig,
}

impl MysqlStateStoreProviderFactory {
    pub fn new(database: String, client: MySqlClientConfig) -> Self {
        Self {
            descriptor: StateStoreProviderDescriptor::new(
                MYSQL_STATE_STORE_PROVIDER_ID,
                MYSQL_MAX_KEY_BYTES,
            ),
            database,
            client,
        }
    }

    pub fn try_new(database: String, client: MySqlClientConfig) -> Result<Self, StateStoreError> {
        validate_database(&database)?;
        client.validate().map_err(|_| {
            StateStoreError::new(
                StateStoreErrorKind::InvalidConfiguration,
                "MySQL client configuration is invalid",
            )
        })?;
        Ok(Self::new(database, client))
    }
}

#[async_trait]
impl StateStoreProviderFactory for MysqlStateStoreProviderFactory {
    fn descriptor(&self) -> &StateStoreProviderDescriptor {
        &self.descriptor
    }

    async fn open(
        self: Box<Self>,
        request: StateStoreOpenRequest,
    ) -> Result<Box<dyn StateStoreProviderInstance>, StateStoreError> {
        if Instant::now() >= request.deadline {
            return Err(mysql_provider_deadline_error());
        }
        validate_database(&self.database)?;
        let runtime = MysqlRuntime::boot(self.client)?;
        let (store, runtime) = open_provider_with_runtime(runtime, self.database, request).await?;
        Ok(Box::new(MysqlStateStoreProviderInstance {
            descriptor: self.descriptor,
            lifecycle: StateStoreProviderLifecycle::Ready,
            state_store: Some(store),
            runtime: Some(runtime),
        }))
    }
}

fn validate_database(database: &str) -> Result<(), StateStoreError> {
    if database.is_empty()
        || database.len() > 64
        || !database
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return Err(StateStoreError::new(
            StateStoreErrorKind::InvalidConfiguration,
            "MySQL state store database is invalid",
        ));
    }
    Ok(())
}

pub(super) struct MysqlStateStoreProviderInstance {
    descriptor: StateStoreProviderDescriptor,
    lifecycle: StateStoreProviderLifecycle,
    state_store: Option<Arc<dyn StateStore>>,
    runtime: Option<MysqlRuntimeOwner>,
}

#[async_trait]
impl StateStoreProviderInstance for MysqlStateStoreProviderInstance {
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

#[cfg(test)]
mod tests {
    #[cfg(feature = "state-store-test-hooks")]
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    #[cfg(feature = "state-store-test-hooks")]
    use std::time::Duration;

    #[cfg(feature = "state-store-test-hooks")]
    #[cfg(feature = "state-store-test-hooks")]
    use futures::future::BoxFuture;
    #[cfg(feature = "state-store-test-hooks")]
    use novarocks_spi::state_store::{
        ChangePage, ChangePollRequest, CommitResolution, ReadTransaction, StateStoreErrorKind,
        StateStoreLimits, StateStoreMetricsSnapshot, StateStoreOpenRequest, StoreIdentity,
        TransactionId, WriteTransaction,
    };
    use novarocks_spi::state_store::{StateStoreProviderFactory, StateStoreProviderInstance};
    #[cfg(feature = "state-store-test-hooks")]
    use tokio::sync::Notify;

    #[cfg(feature = "state-store-test-hooks")]
    use super::*;
    #[cfg(feature = "state-store-test-hooks")]
    use crate::client::{MysqlPoolConnection, PoolLifecycle};
    #[cfg(feature = "state-store-test-hooks")]
    use crate::error::MysqlNativeError;
    #[cfg(feature = "state-store-test-hooks")]
    use crate::runtime::test_mysql_runtime_with_pool;

    use crate::{
        MYSQL_MAX_KEY_BYTES, MYSQL_STATE_STORE_PROVIDER_ID, MySqlClientConfig, MySqlTlsMode,
    };
    use novarocks_secret::SecretValue;

    #[cfg(feature = "state-store-test-hooks")]
    struct FailOncePool {
        disconnects: Arc<AtomicUsize>,
    }

    #[cfg(feature = "state-store-test-hooks")]
    impl PoolLifecycle for FailOncePool {
        fn get_conn<'a>(
            &'a self,
            _deadline: tokio::time::Instant,
        ) -> BoxFuture<'a, Result<MysqlPoolConnection, MysqlNativeError>> {
            Box::pin(async { Err(MysqlNativeError::provider_unavailable()) })
        }

        fn disconnect(self: Arc<Self>) -> BoxFuture<'static, Result<(), MysqlNativeError>> {
            Box::pin(async move {
                let attempt = self.disconnects.fetch_add(1, Ordering::AcqRel);
                if attempt == 0 {
                    Err(MysqlNativeError::provider_unavailable())
                } else {
                    Ok(())
                }
            })
        }
    }

    #[cfg(feature = "state-store-test-hooks")]
    struct FakeStore;

    #[cfg(feature = "state-store-test-hooks")]
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

    #[cfg(feature = "state-store-test-hooks")]
    #[derive(Clone, Copy)]
    enum FakeProviderOpen {
        BlockUntilCancelled,
        Fail,
        Succeed,
    }

    #[cfg(feature = "state-store-test-hooks")]
    struct FakeProviderRuntime {
        open: FakeProviderOpen,
        open_entered: Arc<AtomicBool>,
        open_cancelled: Arc<AtomicBool>,
        open_disposed: Arc<AtomicBool>,
        open_cancelled_notify: Arc<Notify>,
        shutdown_calls: Arc<AtomicUsize>,
        shutdown_failures: Arc<AtomicUsize>,
        dropped: Arc<AtomicBool>,
        unsafe_drop: Arc<AtomicBool>,
        stopped: bool,
    }

    #[cfg(feature = "state-store-test-hooks")]
    impl Drop for FakeProviderRuntime {
        fn drop(&mut self) {
            if !self.stopped {
                self.unsafe_drop.store(true, Ordering::Release);
            }
            self.dropped.store(true, Ordering::Release);
        }
    }

    #[cfg(feature = "state-store-test-hooks")]
    impl super::MysqlProviderRuntime for FakeProviderRuntime {
        fn start_open_store(
            &self,
            _database: String,
            _request: StateStoreOpenRequest,
        ) -> Result<super::MysqlProviderOpen, StateStoreError> {
            self.open_entered.store(true, Ordering::Release);
            let open = self.open;
            let cancelled = Arc::clone(&self.open_cancelled);
            let disposed = Arc::clone(&self.open_disposed);
            let notify = Arc::clone(&self.open_cancelled_notify);
            let cancel_flag = Arc::clone(&self.open_cancelled);
            let cancel_notify = Arc::clone(&self.open_cancelled_notify);
            Ok(super::MysqlProviderOpen::new(
                Box::pin(async move {
                    let result = match open {
                        FakeProviderOpen::BlockUntilCancelled => {
                            loop {
                                let notified = notify.notified();
                                tokio::pin!(notified);
                                notified.as_mut().enable();
                                if cancelled.load(Ordering::Acquire) {
                                    break;
                                }
                                notified.await;
                            }
                            Err(StateStoreError::new(
                                StateStoreErrorKind::ProviderUnavailable,
                                "injected cancelled MySQL open",
                            ))
                        }
                        FakeProviderOpen::Fail => Err(StateStoreError::new(
                            StateStoreErrorKind::Corruption,
                            "injected MySQL open failure",
                        )),
                        FakeProviderOpen::Succeed => Ok(Arc::new(FakeStore) as Arc<dyn StateStore>),
                    };
                    disposed.store(true, Ordering::Release);
                    result
                }),
                move || {
                    cancel_flag.store(true, Ordering::Release);
                    cancel_notify.notify_waiters();
                },
            ))
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
                        StateStoreErrorKind::DeadlineExceeded,
                        "injected MySQL cleanup deadline",
                    ))
                } else {
                    Ok(())
                }
            })
        }
    }

    #[cfg(feature = "state-store-test-hooks")]
    struct FakeProviderRuntimeControl {
        open_entered: Arc<AtomicBool>,
        open_cancelled: Arc<AtomicBool>,
        open_disposed: Arc<AtomicBool>,
        shutdown_calls: Arc<AtomicUsize>,
        dropped: Arc<AtomicBool>,
        unsafe_drop: Arc<AtomicBool>,
    }

    #[cfg(feature = "state-store-test-hooks")]
    fn fake_provider_runtime(
        open: FakeProviderOpen,
        shutdown_failures: usize,
    ) -> (FakeProviderRuntime, FakeProviderRuntimeControl) {
        let control = FakeProviderRuntimeControl {
            open_entered: Arc::new(AtomicBool::new(false)),
            open_cancelled: Arc::new(AtomicBool::new(false)),
            open_disposed: Arc::new(AtomicBool::new(false)),
            shutdown_calls: Arc::new(AtomicUsize::new(0)),
            dropped: Arc::new(AtomicBool::new(false)),
            unsafe_drop: Arc::new(AtomicBool::new(false)),
        };
        (
            FakeProviderRuntime {
                open,
                open_entered: Arc::clone(&control.open_entered),
                open_cancelled: Arc::clone(&control.open_cancelled),
                open_disposed: Arc::clone(&control.open_disposed),
                open_cancelled_notify: Arc::new(Notify::new()),
                shutdown_calls: Arc::clone(&control.shutdown_calls),
                shutdown_failures: Arc::new(AtomicUsize::new(shutdown_failures)),
                dropped: Arc::clone(&control.dropped),
                unsafe_drop: Arc::clone(&control.unsafe_drop),
                stopped: false,
            },
            control,
        )
    }

    #[cfg(feature = "state-store-test-hooks")]
    fn provider_open_request(deadline: Instant) -> StateStoreOpenRequest {
        StateStoreOpenRequest {
            cluster_id: "cluster-a".to_owned(),
            limits: StateStoreLimits::default(),
            deadline,
        }
    }

    #[cfg(feature = "state-store-test-hooks")]
    async fn wait_for_count(counter: &AtomicUsize, expected: usize) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while counter.load(Ordering::Acquire) < expected {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("background MySQL owner made progress");
    }

    #[cfg(feature = "state-store-test-hooks")]
    async fn wait_for_true(flag: &AtomicBool) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while !flag.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("background MySQL owner made progress");
    }

    #[cfg(feature = "state-store-test-hooks")]
    #[tokio::test(flavor = "current_thread")]
    async fn cancelled_factory_open_explicitly_cancels_and_cleans_runtime_owner() {
        let (runtime, control) = fake_provider_runtime(FakeProviderOpen::BlockUntilCancelled, 0);
        let waiter = tokio::spawn(super::open_provider_with_runtime(
            runtime,
            "control_plane".to_owned(),
            provider_open_request(Instant::now() + Duration::from_secs(1)),
        ));
        wait_for_true(control.open_entered.as_ref()).await;

        waiter.abort();
        let cancellation = match waiter.await {
            Ok(_) => panic!("factory waiter must abort"),
            Err(error) => error,
        };
        assert!(cancellation.is_cancelled());
        wait_for_true(control.open_cancelled.as_ref()).await;
        wait_for_true(control.open_disposed.as_ref()).await;
        wait_for_count(control.shutdown_calls.as_ref(), 1).await;
        wait_for_true(control.dropped.as_ref()).await;

        assert!(!control.unsafe_drop.load(Ordering::Acquire));
    }

    #[cfg(feature = "state-store-test-hooks")]
    #[tokio::test(flavor = "current_thread")]
    async fn successful_factory_open_hands_runtime_to_retryable_owner() {
        let (runtime, control) = fake_provider_runtime(FakeProviderOpen::Succeed, 1);
        let (_store, mut owner) = super::open_provider_with_runtime(
            runtime,
            "control_plane".to_owned(),
            provider_open_request(Instant::now() + Duration::from_secs(1)),
        )
        .await
        .expect("fake MySQL open must succeed");

        let first = owner
            .shutdown(Instant::now() + Duration::from_millis(20))
            .await
            .expect_err("first owner shutdown must preserve deadline failure");
        assert_eq!(first.kind(), StateStoreErrorKind::DeadlineExceeded);
        owner
            .shutdown(Instant::now() + Duration::from_secs(1))
            .await
            .expect("owner shutdown retry must succeed");
        wait_for_true(control.dropped.as_ref()).await;

        assert_eq!(control.shutdown_calls.load(Ordering::Acquire), 2);
        assert!(!control.unsafe_drop.load(Ordering::Acquire));
    }

    #[cfg(feature = "state-store-test-hooks")]
    #[tokio::test(flavor = "current_thread")]
    async fn failed_factory_open_keeps_primary_and_cleanup_while_owner_retries() {
        let (runtime, control) = fake_provider_runtime(FakeProviderOpen::Fail, 1);

        let error = match super::open_provider_with_runtime(
            runtime,
            "control_plane".to_owned(),
            provider_open_request(Instant::now() + Duration::from_secs(1)),
        )
        .await
        {
            Ok(_) => panic!("fake MySQL open must fail"),
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

    #[test]
    fn mysql_factory_exposes_the_typed_descriptor_without_connecting() {
        let factory = super::MysqlStateStoreProviderFactory::new(
            "novarocks_control_plane".to_owned(),
            MySqlClientConfig {
                host: "mysql.internal.example".to_owned(),
                port: 3306,
                username: "novarocks_state_store".to_owned(),
                password: SecretValue::new("bind-test-password"),
                tls_mode: MySqlTlsMode::Required,
                tls_ca_path: None,
                tls_cert_path: None,
                tls_key_path: None,
                connect_timeout_ms: 1_000,
                pool_min: 1,
                pool_max: 16,
                inactive_connection_ttl_ms: 30_000,
            },
        );
        assert_eq!(factory.descriptor().id, MYSQL_STATE_STORE_PROVIDER_ID);
        assert_eq!(factory.descriptor().max_key_bytes, MYSQL_MAX_KEY_BYTES);
        assert_mysql_instance_contract::<super::MysqlStateStoreProviderInstance>();
    }

    #[cfg(feature = "state-store-test-hooks")]
    #[tokio::test(flavor = "current_thread")]
    async fn disconnect_failure_keeps_instance_draining_and_retryable() {
        let disconnects = Arc::new(AtomicUsize::new(0));
        let pool: Arc<dyn PoolLifecycle> = Arc::new(FailOncePool {
            disconnects: Arc::clone(&disconnects),
        });
        let runtime = test_mysql_runtime_with_pool(pool);
        let runtime = spawn_mysql_runtime_owner(runtime);
        let mut instance = MysqlStateStoreProviderInstance {
            descriptor: StateStoreProviderDescriptor::new(
                MYSQL_STATE_STORE_PROVIDER_ID,
                MYSQL_MAX_KEY_BYTES,
            ),
            lifecycle: StateStoreProviderLifecycle::Ready,
            state_store: Some(Arc::new(FakeStore)),
            runtime: Some(runtime),
        };
        assert!(instance.state_store().is_some());

        let first = instance
            .shutdown(Instant::now() + Duration::from_secs(1))
            .await
            .expect_err("first disconnect must surface its native error");

        assert_eq!(first.kind(), StateStoreErrorKind::ProviderUnavailable);
        assert_eq!(instance.lifecycle(), StateStoreProviderLifecycle::Draining);
        assert!(instance.state_store().is_none());
        assert!(
            instance.runtime.is_some(),
            "runtime owner retained for retry"
        );

        instance
            .shutdown(Instant::now() + Duration::from_secs(1))
            .await
            .expect("second disconnect must finish shutdown");

        assert_eq!(disconnects.load(Ordering::Acquire), 2);
        assert_eq!(instance.lifecycle(), StateStoreProviderLifecycle::Stopped);
        assert!(instance.state_store().is_none());
        assert!(instance.runtime.is_none());
    }

    fn assert_mysql_instance_contract<T: StateStoreProviderInstance>() {}
}
