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

use super::*;
use crate::query_execution::completion_facts::{
    CompletionFactOwners, FrontendStatisticsFacts, StatementFactScope,
};
use novarocks_query_application::admitted_query_context::QueryResultCapacityBinding;
use novarocks_query_application::preparation::StatisticsFactPort;
use novarocks_workload_control::{CancellationReason, ResultWindowClass};
use std::sync::atomic::AtomicBool;
use std::sync::mpsc;

struct HeldReadExit {
    entered: mpsc::Sender<()>,
    release: mpsc::Receiver<()>,
    exited: Arc<AtomicBool>,
}
impl Drop for HeldReadExit {
    fn drop(&mut self) {
        let _ = self.entered.send(());
        let _ = self.release.recv();
        self.exited.store(true, Ordering::Release);
    }
}

struct HeldStatisticsReader {
    identity: Arc<ContextObservingProvider>,
    started: mpsc::Sender<()>,
    release: Arc<tokio::sync::Notify>,
    exit: Mutex<Option<HeldReadExit>>,
    reads: AtomicUsize,
    stop_seen: AtomicBool,
    exact_request: AtomicBool,
}
impl StatisticsReader for HeldStatisticsReader {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.identity.descriptor
    }
    fn incarnation(&self) -> ProviderBindingEpoch {
        self.identity.incarnation
    }
    fn read_statistics(
        &self,
        _: StatisticsReadRequest,
    ) -> Result<StatisticsEvidence, ConnectorError> {
        panic!("the real async statistics port must not fall back to a synchronous reader")
    }
    fn read_statistics_async(
        &self,
        request: StatisticsReadRequest,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<StatisticsEvidence, ConnectorError>>
                + Send
                + '_,
        >,
    > {
        Box::pin(async move {
            self.reads.fetch_add(1, Ordering::AcqRel);
            let _exit = self.exit.lock().unwrap().take();
            self.exact_request.store(
                request.table.owner() == &self.identity.descriptor.instance_id
                    && request.table.payload().as_ref() == b"orders"
                    && request.data_version
                        == StatisticsDataVersion::try_new(Bytes::from_static(b"data-v1")).unwrap()
                    && request.metrics.metrics() == [StatisticsMetric::RowCount]
                    && request.narrowed_read.is_none(),
                Ordering::Release,
            );
            let _ = self.started.send(());
            self.release.notified().await;
            let stopped = request.context.is_cancelled();
            self.stop_seen.store(stopped, Ordering::Release);
            Err(ConnectorError::new(
                if stopped {
                    ConnectorErrorKind::Cancelled
                } else {
                    ConnectorErrorKind::Internal
                },
                "held statistics reader reached its actual stop check",
            ))
        })
    }
}
impl ConnectorStatistics for HeldStatisticsReader {}

fn held_statistics_binding(provider: Arc<HeldStatisticsReader>) -> QueryTableBinding {
    let identity = &provider.identity;
    let control = Arc::new(
        ConnectorControlBinding::try_new_with_statistics(
            identity.descriptor.clone(),
            identity.incarnation,
            identity.clone(),
            identity.clone(),
            identity.clone(),
            None,
            Some(provider.clone()),
        )
        .expect("exact statistics fixture control"),
    );
    let mut binding = local_binding("ice.main", "db", "orders", 173);
    binding.statistics_pin = Some(crate::connector::backend::ResolvedTableStatisticsPin {
        table: ConnectorTableHandle::try_new(
            identity.descriptor.instance_id.clone(),
            Bytes::from_static(b"orders"),
        )
        .unwrap(),
        data_version: StatisticsDataVersion::try_new(Bytes::from_static(b"data-v1")).unwrap(),
    });
    binding.admission =
        QueryTableBindingAdmission::Exact(ConnectorControlPlanningLease::new(control, || {}));
    binding
}

fn empty_completion_owners(
    blocking: crate::task_execution::blocking_io::ConnectorBlockingIoSupervisor,
    runtime: tokio::runtime::Handle,
) -> CompletionFactOwners {
    // MV inventory is unrelated to the held statistics port, but it still
    // uses the real keyed StateStore owner rather than a legacy adapter.
    let (product, repository) =
        runtime.block_on(crate::mv::domain::test_admission::keyed_product());
    let inventory = crate::mv::domain::readiness::MvReadinessPort::from_product(
        product.keyed_readiness_service().unwrap(),
        runtime,
        product.management_entrance().unwrap(),
    );
    drop(repository);
    drop(product);
    CompletionFactOwners::new(
        Arc::new(crate::catalog_application::query_catalog::new_query_catalog_service()),
        None,
        Arc::new(novarocks_catalog_application::ConnectorControlHost::new()),
        Arc::new(UnifiedStatisticsResolver::default()),
        inventory.candidate_reader(),
        Arc::new(novarocks_spi::connector::UnavailableMvStorageObservationPort),
        blocking,
    )
}

#[derive(Debug)]
struct StatisticsNoNativeStart;
impl novarocks_query_application::api::LogicalExecutionNativePort for StatisticsNoNativeStart {
    fn bind_in(
        &self,
        _: &mut Option<novarocks_query_application::api::LogicalNativeOpenRequest>,
        _: &mut Option<novarocks_query_application::api::LogicalNativeSession>,
    ) -> Result<(), novarocks_query_application::api::LogicalNativeOpenError> {
        panic!("statistics completion facts cannot start Native work")
    }
}
struct ActualStatisticsAction {
    original: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
}
impl novarocks_query_application::session_control::GovernedStatementAction
    for ActualStatisticsAction
{
    fn poll_action(
        &mut self,
        _: &mut novarocks_query_application::session_control::GovernedActionContext<'_>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<
        Result<
            novarocks_query_application::session_control::StatementActionValue,
            novarocks_query_application::session_error::QueryServiceError,
        >,
    > {
        self.original.as_mut().poll(cx).map(|()| {
            Ok(
                novarocks_query_application::session_control::StatementActionValue::Ok(
                    novarocks_query_application::session_control::StatementCommandCompletion::new(
                        0,
                    ),
                ),
            )
        })
    }
}

fn observe_real_statistics_port(class: ResultWindowClass) {
    use novarocks_query_application::{
        client_connection::ClientConnectionToken,
        coordination::{LogicalExecutionRowsConfig, LogicalExecutionSupervisorConfig},
        query_control::QueryApplicationControl,
        session_control::{QueryControlService, SessionIdentity},
    };
    use std::num::{NonZeroU32, NonZeroUsize};
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let blocking = crate::task_execution::blocking_io::ConnectorBlockingIoSupervisor::new(
        runtime.handle().clone(),
    );
    let service = QueryControlService::new(Arc::new(QueryApplicationControl::default()));
    let session = service
        .register_session(SessionIdentity::new(
            ClientConnectionToken::new(7, 1).unwrap(),
            "root",
        ))
        .unwrap();
    let (mut fixture, client) =
        crate::query_execution::logical_execution_test_profile::process_with_statement_control(
            runtime.handle().clone(),
            Arc::new(StatisticsNoNativeStart),
            novarocks_types::identity::QueryProcessNamespace::new(0x9b1),
            novarocks_types::identity::FrontendProcessId::new_v7(),
            LogicalExecutionSupervisorConfig::new(
                NonZeroUsize::new(8).unwrap(),
                NonZeroUsize::MIN,
                NonZeroUsize::MIN,
                LogicalExecutionRowsConfig::new(
                    NonZeroUsize::MIN,
                    NonZeroU32::MIN,
                    Duration::from_secs(1),
                ),
            ),
            service.clone(),
        );
    let mut owner = runtime
        .block_on(service.begin_queued_governed_query_statement_with_result(
            session.token(),
            &fixture.control().root_admission(),
            None,
            None,
            None,
            class,
        ))
        .unwrap();
    runtime.block_on(fixture.bind_local_root(owner.execution_owner_slot().as_ref().unwrap()));
    let original_id = owner.scope().id();
    let cancel = owner
        .execution_owner_slot()
        .as_ref()
        .unwrap()
        .cancellation_requester();
    let observation = fixture.control().observation();
    let capacity =
        QueryResultCapacityBinding::try_new(owner.scope(), owner.result_window_alias().unwrap())
            .unwrap();
    if class == ResultWindowClass::Internal {
        // Original Internal statement admission now pins its configured Root
        // byte owner, not a computation position or a fabricated payload.
        let retention = capacity.try_window_alias().unwrap();
        assert!(retention.is_for_scope(owner.scope()));
        assert!(retention.is_root_backed_internal());
        assert_eq!(retention.class(), ResultWindowClass::Internal);
        assert!(retention.internal_manifest_binding().unwrap().is_none());
        assert_eq!(observation.snapshot().result_windows.held_positions, [0; 4]);
    }
    let (started, entered) = mpsc::channel();
    let release_call = Arc::new(tokio::sync::Notify::new());
    let (exit_started, exit_entered) = mpsc::channel();
    let (release_exit, held_exit) = mpsc::channel();
    let exited = Arc::new(AtomicBool::new(false));
    // Rescue exists before the actual Reader future is submitted. It retains
    // only gates, never the provider, action, result window or Root owner.
    let (disarm, watch) = mpsc::channel();
    let rescue_call = Arc::clone(&release_call);
    let rescue_exit = release_exit.clone();
    let watchdog = std::thread::spawn(move || {
        if watch.recv_timeout(Duration::from_secs(6)).is_err() {
            rescue_call.notify_one();
            let _ = rescue_exit.send(());
        }
    });
    let provider = Arc::new(HeldStatisticsReader {
        identity: observing_provider(),
        started,
        release: Arc::clone(&release_call),
        exit: Mutex::new(Some(HeldReadExit {
            entered: exit_started,
            release: held_exit,
            exited: Arc::clone(&exited),
        })),
        reads: AtomicUsize::new(0),
        stop_seen: AtomicBool::new(false),
        exact_request: AtomicBool::new(false),
    });
    let bindings = Arc::new(QueryTableBindingStore::try_new().unwrap());
    let binding = held_statistics_binding(provider.clone());
    let binding_id = bindings
        .resolve_or_insert(
            crate::catalog_application::query_bindings::QueryTableBindingKey::strict_base(
                "ice.main", "db", "orders",
            ),
            || Ok(binding),
        )
        .expect("original binding store token");
    let stop = novarocks_spi::connector::ConnectorStopOwner::new();
    let context = ConnectorRequestContext::try_new(
        Instant::now() + Duration::from_secs(10),
        stop.view(),
        novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        novarocks_spi::connector::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    )
    .unwrap();
    let scope = StatementFactScope::binding_only(
        bindings.clone(),
        context,
        Some("ice.main"),
        owner.scope(),
        capacity.clone(),
    )
    .unwrap();
    let owners = empty_completion_owners(blocking.clone(), runtime.handle().clone());
    let port = FrontendStatisticsFacts::new(owners, scope);
    let need = novarocks_sql::compiler::fixtures::statistics_need(
        1,
        binding_id,
        vec![StatisticsMetric::RowCount],
    )
    .unwrap();
    let original = Box::pin(async move {
        let answer = port.resolve_statistics(&[need]).await;
        drop(answer);
    });
    let mut owner = Some(owner);
    let mut action: Option<
        Box<dyn novarocks_query_application::session_control::GovernedStatementAction>,
    > = Some(Box::new(ActualStatisticsAction { original }));
    let mut receive = None;
    service
        .publish_statement_action_in(&mut owner, &mut action, &mut receive)
        .unwrap()
        .finish()
        .unwrap();
    assert!(owner.is_none() && action.is_none());
    let receive = receive.take().unwrap();
    let delivered = Arc::new(AtomicBool::new(false));
    let published = Arc::clone(&delivered);
    // This original JoinHandle is ONLY the UI observer. The already-installed
    // same supervisor drives the actual Statistics F in its stable Source.
    let mut waiter = runtime.spawn(async move {
        let answer = receive.await;
        drop(answer);
        published.store(true, Ordering::Release);
    });
    let call_entered = entered.recv_timeout(Duration::from_secs(2)).is_ok();
    stop.request_stop();
    cancel.request(CancellationReason::Requested).unwrap();
    fixture.control().expire_deadlines();
    waiter.abort();
    let joined =
        runtime.block_on(async { tokio::time::timeout(Duration::from_secs(2), &mut waiter).await });
    let waiter_cancelled = matches!(joined, Ok(Err(ref e)) if e.is_cancelled());
    drop(capacity);
    let during_call = observation.snapshot();
    release_call.notify_one();
    let drop_entered = exit_entered.recv_timeout(Duration::from_secs(2)).is_ok();
    let during_exit = observation.snapshot();
    let still_held = !exited.load(Ordering::Acquire);
    let _ = release_exit.send(());
    let _ = disarm.send(());
    let watchdog_join = watchdog.join();
    let watchdog_joined = watchdog_join.is_ok();
    drop(cancel);
    drop(session);
    drop(service);
    drop(client);
    let shutdown =
        runtime.block_on(fixture.shutdown_until(Instant::now() + Duration::from_secs(2)));
    let after = observation.snapshot();
    let expected = if class == ResultWindowClass::Local {
        [0, 1, 0, 0]
    } else {
        // No computation was dispatched by these fact-only callbacks. The
        // exact Root retention pin below must still survive actual exit.
        [0; 4]
    };
    let held = [&during_call, &during_exit].iter().all(|facts| {
        facts.root_responsibilities == 1
            && facts.result_windows.held_positions == expected
            && facts.scopes.iter().any(|s| {
                s.id == original_id
                    && s.parent.is_none()
                    && !s.own_completed
                    && s.resource_holders > 0
            })
    });
    let success = call_entered
        && drop_entered
        && still_held
        && waiter_cancelled
        && watchdog_joined
        && held
        && provider.reads.load(Ordering::Acquire) == 1
        && provider.exact_request.load(Ordering::Acquire)
        && provider.stop_seen.load(Ordering::Acquire)
        && exited.load(Ordering::Acquire)
        && !delivered.load(Ordering::Acquire)
        && after.result_windows.held_positions == [0; 4]
        && after.scopes.is_empty()
        && shutdown.is_ok();
    struct Failure {
        _waiter: Result<Result<(), tokio::task::JoinError>, tokio::time::error::Elapsed>,
        _waiter_handle: tokio::task::JoinHandle<()>,
        _watchdog: std::thread::Result<()>,
        _shutdown: Result<
            (),
            novarocks_query_application::coordination::LogicalExecutionSupervisorShutdownError,
        >,
        _blocking: crate::task_execution::blocking_io::ConnectorBlockingIoSupervisor,
        _fixture: crate::query_execution::logical_execution_test_profile::IdentityTestOwner,
        _runtime: tokio::runtime::Runtime,
    }
    if !success {
        eprintln!(
            "statistics held phases: entered={call_entered} drop={drop_entered} held={still_held} cancelled={waiter_cancelled} root_held={held} watchdog={watchdog_joined} shutdown_ok={}",
            shutdown.is_ok()
        );
        std::panic::panic_any(Failure {
            _waiter: joined,
            _waiter_handle: waiter,
            _watchdog: watchdog_join,
            _shutdown: shutdown,
            _blocking: blocking,
            _fixture: fixture,
            _runtime: runtime,
        });
    }
    // Same old responsibility predicates, now tied to the actual GovRoot.
    // A retired QB child is not recreated just to satisfy the old mock shape.
    assert!(call_entered, "original provider callback was not entered");
    assert!(
        drop_entered,
        "original provider exit destructor was not entered"
    );
    assert!(still_held, "original provider exit was not held");
    assert!(
        waiter_cancelled,
        "original caller did not join as cancelled"
    );
    assert!(watchdog_joined, "rescue watchdog did not join");
    assert!(
        held,
        "same original statement Root/result owner was not retained"
    );
    assert_eq!(provider.reads.load(Ordering::Acquire), 1);
    assert!(provider.exact_request.load(Ordering::Acquire));
    assert!(provider.stop_seen.load(Ordering::Acquire));
    assert!(exited.load(Ordering::Acquire));
    assert!(
        !delivered.load(Ordering::Acquire),
        "abandoned statement accepted a late statistics fact"
    );
    assert_eq!(after.result_windows.held_positions, [0; 4]);
    assert!(after.scopes.is_empty());
    assert!(shutdown.is_ok(), "same original workload did not drain");
    drop(waiter);
    drop(bindings);
    drop(blocking);
    drop(fixture);
    drop(runtime);
}

#[test]
fn actual_statistics_port_keeps_local_window_through_stop_and_original_callback_exit() {
    observe_real_statistics_port(ResultWindowClass::Local);
}
#[test]
fn actual_statistics_port_keeps_root_retention_through_stop_and_original_callback_exit() {
    observe_real_statistics_port(ResultWindowClass::Internal);
}
