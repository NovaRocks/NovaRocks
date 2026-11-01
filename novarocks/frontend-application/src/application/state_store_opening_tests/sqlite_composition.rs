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

//! Actual SQLite composition through the production FA original receivers.
//! This exercises the GC observation accelerator, not Iceberg provenance/CAS
//! discovery or the TableMaintenanceProduct selection/session boundary.

use super::*;
use crate::query_execution::maintenance::io_admission::MetadataIoAdmission;
use novarocks_resource_contract::SharedDependency;
use novarocks_state_store_api::{StateStoreLimits, StateStoreProviderFactory};
use novarocks_state_store_runtime::{
    StateStoreHostInput, StateStoreProviderRegistration, StateStoreProviderRegistry,
};
use novarocks_state_store_sqlite::{
    SQLITE_STATE_STORE_PROVIDER_ID, SqliteStateStoreProviderFactory,
};
use novarocks_table_maintenance::gc_observation::{
    GcOwnedRefObservation, GcOwnedRefObservationAccelerator, GcOwnedRefObservationDecision,
};
use novarocks_workload_control::{
    ControlStorageConfig, ResultCapacityConfig, RootWork, WorkClass, WorkOwner, WorkRequest,
    WorkloadConfig, WorkloadControl,
    joint_admission::{JointProgress, JointProgressDriver, ResponsibilityConfig},
};
use std::{future::Future, path::Path, task::Poll};

fn prepare_sqlite(path: &Path) -> FrontendStateStoreOpening {
    // Register the actual selected provider. No persistent-memory substitute
    // and no legacy open() can satisfy this prepared/original receiver.
    let path = path.to_owned();
    let descriptor = SqliteStateStoreProviderFactory::new(path.clone())
        .descriptor()
        .to_owned();
    let mut registry = StateStoreProviderRegistry::new();
    registry
        .register(StateStoreProviderRegistration::new(descriptor, move |_| {
            Ok(Box::new(SqliteStateStoreProviderFactory::new(path.clone())))
        }))
        .unwrap();
    let prepared = StateStoreHost::prepare_original_open(
        &registry,
        StateStoreHostInput {
            cluster_id: "actual-sqlite-original-composition".into(),
            provider_id: SQLITE_STATE_STORE_PROVIDER_ID,
            limits: StateStoreLimits::default(),
            run_policy: StateStoreRunPolicy::default(),
        },
        Instant::now() + Duration::from_secs(5),
    )
    .unwrap();
    assert_eq!(
        prepared.original_blocking_dependency_facts(),
        &[SharedDependency::LocalHost]
    );
    FrontendStateStoreOpening::new(prepared)
}

async fn open_sqlite(
    opening: &mut FrontendStateStoreOpening,
    executor: &QueryBlockingExecutor,
    ready: &mut Option<StateStoreHost>,
) {
    opening.start(executor).unwrap();
    tokio::time::timeout(
        Duration::from_secs(5),
        std::future::poll_fn(|cx| opening.poll_open(cx, ready)),
    )
    .await
    .expect("actual SQLite Open phase and QA footer")
    .unwrap();
    assert_eq!(
        ready.as_ref().unwrap().original_blocking_dependency_facts(),
        Some([SharedDependency::LocalHost].as_slice())
    );
}

fn original_control() -> (WorkloadControl, JointProgressDriver) {
    let control = WorkloadControl::try_new_counted(WorkloadConfig {
        root_limit: 4,
        business_limit: 4,
        scope_records_limit: 4,
        ..WorkloadConfig::default()
    })
    .unwrap()
    .owner;
    let (_, driver) = control
        .configure_responsibility(ResponsibilityConfig {
            work_per_turn: NonZeroUsize::new(1).unwrap(),
        })
        .unwrap();
    control
        .configure_control_storage(ControlStorageConfig::new(4, &[]))
        .unwrap();
    control
        .configure_result_capacity(ResultCapacityConfig::V1)
        .unwrap();
    control.mark_ready().unwrap();
    (control, driver)
}

fn original_root() -> (WorkloadControl, JointProgressDriver, RootWork) {
    let (control, driver) = original_control();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    (control, driver, root)
}

fn drive_one(driver: &mut JointProgressDriver, cx: &Context<'_>) {
    if matches!(driver.step(), JointProgress::More) {
        cx.waker().wake_by_ref();
    }
}

// Borrow the real production inventory and driver; this helper implements no
// provider phase, receiving, retry, publication, or physical-exit algorithm.
async fn with_original_progress<F: Future>(
    original: &StateStoreOriginalOwner,
    io: &Arc<MetadataIoAdmission>,
    root: &WorkOwner,
    driver: &mut JointProgressDriver,
    future: F,
) -> F::Output {
    tokio::pin!(future);
    tokio::time::timeout(
        Duration::from_secs(5),
        std::future::poll_fn(|cx| {
            original.poll_progress(cx).unwrap();
            io.poll_progress(root, cx).unwrap();
            let result = future.as_mut().poll(cx);
            drive_one(driver, cx);
            result
        }),
    )
    .await
    .expect("same Root/provider/ordinary queue made progress")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_sqlite_original_observation_and_pinned_host_close_share_production_receivers() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("actual-original.sqlite");
    let mut workers = QueryBlockingExecutorOwner::try_new(QueryBlockingExecutorConfig::new(
        NonZeroUsize::new(1).unwrap(),
        NonZeroUsize::new(1).unwrap(),
    ))
    .unwrap();
    let executor = workers.executor();
    // Stable receiving precedes the first borrowed Open poll.
    let mut opening = prepare_sqlite(&path);
    let mut ready = None;
    open_sqlite(&mut opening, &executor, &mut ready).await;
    let host = ready.as_ref().unwrap();
    let (vocabulary, instance) = host.original_blocking_sources().unwrap();
    let dependencies = host.original_blocking_dependency_facts().unwrap().to_vec();
    let store = host.state_store().unwrap();
    let policy = host.run_policy();
    let (_control, mut driver, root) = original_root();
    let io = MetadataIoAdmission::for_original_source(&root.owner, dependencies).unwrap();
    let mut original = StateStoreOriginalOwner::for_root(
        &executor,
        vocabulary,
        instance,
        root.owner.scope(),
        policy,
    )
    .unwrap();
    let context = original.context(Instant::now() + Duration::from_secs(5));
    with_original_progress(
        &original,
        &io,
        &root.owner,
        &mut driver,
        std::future::poll_fn(|cx| {
            if io.poll_progress(&root.owner, cx).unwrap() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }),
    )
    .await;
    let observations = GcOwnedRefObservationAccelerator::open(store.clone(), policy)
        .await
        .unwrap();
    // These are accelerator input facts, not invented proof that Iceberg
    // discovered a candidate or successfully changed a catalog ref.
    let observation = GcOwnedRefObservation::try_new(
        uuid::Uuid::from_u128(0x73716c697465),
        "__novarocks_owned_ref_component".into(),
        101,
        2,
        [7; 32],
        1,
    )
    .unwrap();
    let first = with_original_progress(
        &original,
        &io,
        &root.owner,
        &mut driver,
        observations.observe_in(&context, observation.clone(), 1_000, 500),
    )
    .await
    .unwrap();
    assert_eq!(
        first,
        GcOwnedRefObservationDecision::NotMature {
            first_observed_at_ms: 1_000
        }
    );
    let second = with_original_progress(
        &original,
        &io,
        &root.owner,
        &mut driver,
        observations.observe_in(&context, observation, 1_501, 500),
    )
    .await
    .unwrap();
    assert_eq!(
        second,
        GcOwnedRefObservationDecision::Mature {
            first_observed_at_ms: 1_000
        }
    );
    // Business completion does not retire serial transaction/evidence phases.
    tokio::time::timeout(
        Duration::from_secs(5),
        std::future::poll_fn(|cx| {
            io.poll_progress(&root.owner, cx).unwrap();
            let result = original.poll_closed(cx);
            drive_one(&mut driver, cx);
            result
        }),
    )
    .await
    .expect("both actual transaction/evidence tails exited")
    .unwrap();
    assert!(original.local_health_failure().is_none());
    drop(context);
    drop(original);
    tokio::time::timeout(
        Duration::from_secs(5),
        std::future::poll_fn(|cx| {
            let result = io.poll_quiescent(&root.owner, cx);
            drive_one(&mut driver, cx);
            result
        }),
    )
    .await
    .expect("same Root metadata source actually quiesced")
    .unwrap();
    drop(io);
    root.owner.complete();
    root.business.release();
    // This real instance-issued observation pins the real evidence connection
    // and file lifetime. It has never been dispatched, so cancellation is exact.
    let (attempt, pin) = store.attempts().reserve().unwrap();
    assert_eq!(attempt.id().scope(), instance);
    attempt.cancel_before_dispatch().unwrap();
    drop(attempt);
    drop(observations);
    drop(store);
    let deadline = Instant::now() + Duration::from_secs(5);
    let held = tokio::time::timeout(
        Duration::from_millis(100),
        std::future::poll_fn(|cx| opening.poll_close(&mut ready, &executor, deadline, cx)),
    )
    .await;
    assert!(
        held.is_err(),
        "an actual Observation still pins SQLite close"
    );
    assert!(
        ready.is_some(),
        "timeout cannot clear the actual Host receiver"
    );
    assert!(opening.instance_close.as_ref().unwrap().original.is_some());
    assert_eq!(
        ready.as_ref().unwrap().lifecycle(),
        novarocks_state_store_runtime::StateStoreHostLifecycle::Draining
    );
    // Release only AFTER the resumed borrower is already Pending. The
    // observer cannot poll Source or notify its waker; no hand-authored receipt
    // or manual repoll can substitute for the owner's policy timer.
    let (release_tx, release_rx) = mpsc::channel();
    let observer = std::thread::spawn(move || {
        let waiting = release_rx.recv_timeout(Duration::from_secs(1)).is_ok();
        std::thread::sleep(Duration::from_millis(50));
        drop(pin);
        waiting
    });
    let mut release_tx = Some(release_tx);
    let closed = tokio::time::timeout(
        Duration::from_secs(5),
        std::future::poll_fn(|cx| {
            let result = opening.poll_close(&mut ready, &executor, deadline, cx);
            if result.is_pending()
                && let Some(notice) = release_tx.take()
            {
                notice.send(()).unwrap();
            }
            result
        }),
    )
    .await;
    let released_after_pending = observer.join().unwrap();
    assert!(
        released_after_pending,
        "pin release followed a real Pending poll"
    );
    closed
        .expect("original owner backoff completed Store/Evidence/File close")
        .unwrap();
    assert!(
        ready.is_none(),
        "receiver and QA footers precede Host removal"
    );
    assert!(opening.original_failure().is_none());
    // Same-path actual OriginalOpen proves the process-exclusive file lease
    // was released. A legacy open or a new in-memory store cannot prove this.
    let mut reopened = prepare_sqlite(&path);
    open_sqlite(&mut reopened, &executor, &mut ready).await;
    let (_, new_instance) = ready.as_ref().unwrap().original_blocking_sources().unwrap();
    assert_ne!(new_instance, instance);
    let reopened_deadline = Instant::now() + Duration::from_secs(5);
    tokio::time::timeout(
        Duration::from_secs(5),
        std::future::poll_fn(|cx| {
            reopened.poll_close(&mut ready, &executor, reopened_deadline, cx)
        }),
    )
    .await
    .expect("reopened actual instance closed")
    .unwrap();
    assert!(ready.is_none());
    workers
        .shutdown_until(Instant::now() + Duration::from_secs(5))
        .await
        .expect("same ordinary worker actually joined");
}

// Catalog-specific composition. The product fixture delegates persistence to
// the real CA repository and SQLite; it does not emulate a provider transaction,
// original receiver, physical queue or commit observation. Local materialization
// is outside this fixture, so its read observation conveys no Connector grant.
struct RepositoryCatalogApplication {
    repository: novarocks_catalog_application::CatalogAttachmentRepository,
    latest: Mutex<Option<novarocks_catalog_application::CatalogAttachmentVersioned>>,
    fail_after_create: bool,
}
impl RepositoryCatalogApplication {
    fn observation(
        value: &novarocks_catalog_application::CatalogAttachmentVersioned,
    ) -> novarocks_catalog_application::CatalogRuntimeObservation {
        novarocks_catalog_application::CatalogRuntimeObservation {
            attachment_id: value.attachment.attachment_id,
            instance_id: value.attachment.instance_id.clone(),
            provider_id: value.attachment.provider_id.clone(),
            generation: 1,
        }
    }
    fn failure(
        error: novarocks_catalog_application::CatalogAttachmentError,
    ) -> novarocks_catalog_application::CatalogApplicationError {
        novarocks_catalog_application::CatalogApplicationError::from_domain(
            novarocks_catalog_application::CatalogApplicationErrorKind::Unavailable,
            error,
        )
    }
}
impl novarocks_catalog_application::CatalogApplicationPort for RepositoryCatalogApplication {
    fn require_catalog_mutation_admission(
        &self,
    ) -> Result<(), novarocks_catalog_application::CatalogApplicationError> {
        Ok(())
    }
    fn create_catalog(
        &self,
        _: novarocks_catalog_application::CatalogCreateCommand,
    ) -> Result<
        novarocks_catalog_application::CatalogRuntimeObservation,
        novarocks_catalog_application::CatalogApplicationError,
    > {
        panic!("original Catalog Action must not call synchronous create")
    }
    fn drop_catalog(
        &self,
        _: novarocks_catalog_application::CatalogDropCommand,
    ) -> Result<(), novarocks_catalog_application::CatalogApplicationError> {
        panic!("original Catalog Action must not call synchronous drop")
    }
    fn create_catalog_in<'a>(
        &'a self,
        command: novarocks_catalog_application::CatalogCreateCommand,
        execution: &'a novarocks_state_store_api::StateStoreExecutionContext,
        receipt: &'a novarocks_catalog_application::CatalogMutationReceipt,
    ) -> futures::future::BoxFuture<
        'a,
        Result<
            novarocks_catalog_application::CatalogRuntimeObservation,
            novarocks_catalog_application::CatalogApplicationError,
        >,
    > {
        Box::pin(async move {
            if let Some(existing) = self
                .repository
                .get_in(&command.instance_id, execution)
                .await
                .map_err(Self::failure)?
            {
                assert!(command.if_not_exists, "fixture only repeats IF NOT EXISTS");
                *self.latest.lock().unwrap() = Some(existing.clone());
                return Ok(Self::observation(&existing));
            }
            let mut properties = command.properties;
            properties.sort();
            let created = self
                .repository
                .create_in_receiving(
                    novarocks_catalog_application::CatalogAttachment {
                        attachment_id: uuid::Uuid::now_v7(),
                        instance_id: command.instance_id,
                        provider_id: novarocks_spi::connector::ConnectorProviderId::parse(
                            "iceberg",
                        )
                        .unwrap(),
                        display_name: command.display_name,
                        durable_properties: properties,
                        credential_bindings: Vec::new(),
                        created_at_ms: 1,
                    },
                    execution,
                    receipt,
                )
                .await
                .map_err(Self::failure)?;
            *self.latest.lock().unwrap() = Some(created.clone());
            if self.fail_after_create {
                return Err(novarocks_catalog_application::CatalogApplicationError::new(
                    novarocks_catalog_application::CatalogApplicationErrorKind::Unavailable,
                    "required catalog materialization failed after the actual durable commit",
                ));
            }
            // The actual repository authored the durable fact in the same
            // receiver; this does not forge Connector materialization success.
            Ok(Self::observation(&created))
        })
    }
    fn drop_catalog_in<'a>(
        &'a self,
        command: novarocks_catalog_application::CatalogDropCommand,
        execution: &'a novarocks_state_store_api::StateStoreExecutionContext,
        receipt: &'a novarocks_catalog_application::CatalogMutationReceipt,
    ) -> futures::future::BoxFuture<
        'a,
        Result<(), novarocks_catalog_application::CatalogApplicationError>,
    > {
        Box::pin(async move {
            let existing = self
                .repository
                .get_in(&command.instance_id, execution)
                .await
                .map_err(Self::failure)?
                .expect("the test drops its exact persisted attachment");
            self.repository
                .drop_exact_in_receiving(existing, execution, receipt)
                .await
                .map_err(Self::failure)?;
            assert!(
                self.repository
                    .get_in(&command.instance_id, execution)
                    .await
                    .map_err(Self::failure)?
                    .is_none()
            );
            self.latest.lock().unwrap().take();
            Ok(())
        })
    }
    fn admit_catalog(
        &self,
        _: &novarocks_spi::connector::ConnectorInstanceId,
    ) -> novarocks_catalog_application::CatalogAdmission {
        panic!("this fixture never manufactures a Connector runtime admission")
    }
}

// A capability refusal may have crossed a domain wrapper before the Catalog
// consumer sees it. The actual original producer fact, not the outer type or a
// message match, distinguishes this from a best-effort unreadable accelerator.
struct WrappedUnsupportedReferences;
impl novarocks_catalog_application::CatalogReferenceReader for WrappedUnsupportedReferences {
    fn observe_references<'a>(
        &'a self,
        _: &'a dyn novarocks_state_store_api::StateStore,
        _: &'a novarocks_spi::connector::ConnectorInstanceId,
        _: usize,
    ) -> futures::future::BoxFuture<'a, Result<Option<&'static str>, String>> {
        panic!("explicit original refusal cannot call legacy references")
    }
    fn observe_references_in<'a>(
        &'a self,
        _: &'a dyn novarocks_state_store_api::StateStore,
        _: &'a novarocks_spi::connector::ConnectorInstanceId,
        _: usize,
        _: &'a novarocks_state_store_api::StateStoreExecutionContext,
    ) -> futures::future::BoxFuture<
        'a,
        Result<Option<&'static str>, novarocks_state_store_api::StateStoreExecutionFailure>,
    > {
        Box::pin(async {
            Err(novarocks_failure_contract::SharedFailureCause::new(
                novarocks_catalog_application::CatalogApplicationError::from_domain(
                    novarocks_catalog_application::CatalogApplicationErrorKind::Unavailable,
                    novarocks_state_store_api::StateStoreOriginalUnsupported,
                ),
            ))
        })
    }
}

async fn bootstrap_catalog<T>(
    opening: &mut FrontendStateStoreOpening,
    future: impl Future<Output = T>,
) -> T {
    tokio::pin!(future);
    tokio::time::timeout(
        Duration::from_secs(5),
        std::future::poll_fn(|cx| {
            opening.poll_bootstrap_read(cx).unwrap();
            let result = future.as_mut().poll(cx);
            opening.poll_bootstrap_read(cx).unwrap();
            result
        }),
    )
    .await
    .expect("Host bootstrap read uses the actual original ordinary worker")
}
async fn close_catalog_reader(opening: &mut FrontendStateStoreOpening) {
    tokio::time::timeout(
        Duration::from_secs(5),
        std::future::poll_fn(|cx| opening.poll_bootstrap_read_closed(cx)),
    )
    .await
    .expect("bootstrap original sessions and physical footers closed")
    .unwrap();
}
async fn close_sqlite(
    opening: &mut FrontendStateStoreOpening,
    ready: &mut Option<StateStoreHost>,
    executor: &QueryBlockingExecutor,
) {
    let deadline = Instant::now() + Duration::from_secs(5);
    tokio::time::timeout(
        Duration::from_secs(5),
        std::future::poll_fn(|cx| opening.poll_close(ready, executor, deadline, cx)),
    )
    .await
    .expect("real SQLite evidence connection and file holder closed")
    .unwrap();
    assert!(ready.is_none());
    assert!(opening.original_failure().is_none());
}
fn catalog_create(if_not_exists: bool) -> novarocks_sql::semantic::CatalogSqlCommand {
    use novarocks_sql::semantic::command::{CatalogCreateCommand, CommandLiteral, CommandProperty};
    novarocks_sql::semantic::CatalogSqlCommand::CreateCatalog(CatalogCreateCommand {
        external: true,
        if_not_exists,
        name: "catalog_original".into(),
        comment: None,
        properties: vec![CommandProperty {
            key: CommandLiteral::String("type".into()),
            value: CommandLiteral::String("iceberg".into()),
        }],
    })
}
async fn run_catalog_action(
    control: &WorkloadControl,
    driver: &mut JointProgressDriver,
    application: Arc<dyn novarocks_catalog_application::CatalogApplicationPort>,
    binding: crate::state_store::execution::StateStoreOriginalBinding,
    command: novarocks_sql::semantic::CatalogSqlCommand,
    generation: u64,
) -> Result<(), novarocks_query_application::session_error::QueryServiceError> {
    use novarocks_query_application::{
        api::CommandContext,
        session_control::{
            GovernedActionContext, GovernedStatementResidual, SessionToken, StatementActionValue,
            StatementToken,
        },
    };
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let scope = root.owner.scope();
    let cancellation = scope.cancellation().unwrap();
    let (query_permit, window) = scope
        .admit_query_with_result(novarocks_workload_control::ResultWindowClass::Local)
        .unwrap()
        .await
        .unwrap();
    let alias = window.retain_alias();
    let stop = novarocks_spi::connector::ConnectorStopOwner::new();
    let connector = novarocks_spi::connector::ConnectorRequestContext::try_new(
        Instant::now() + Duration::from_secs(5),
        stop.view(),
        64 * 1024,
        1024 * 1024,
    )
    .unwrap();
    let command_context = CommandContext::new(
        scope.clone(),
        connector,
        StatementToken::new(
            SessionToken::new(
                novarocks_query_application::session_control::ClientSessionKey::new(
                    (710_u64 << 32) | 1_u64,
                )
                .expect("nonzero fixture session"),
            ),
            generation,
        ),
        "catalog-original-test",
    )
    .with_result_window(alias.clone())
    .unwrap();
    // No outer QueryBlocking call. With one real worker, nesting this action
    // under a blocking(block_on) wrapper would deadlock this exact test.
    let mut action = crate::catalog_application::statement_action::prepare_for_application(
        Some(application),
        Some(binding),
        command,
        command_context,
    )
    .unwrap();
    let mut owner = Some(root.owner);
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        std::future::poll_fn(|cx| {
            let mut context = GovernedActionContext::new(
                &scope,
                &mut owner,
                &cancellation,
                Some(5_000),
                Some(&alias),
            );
            let result = action.poll_action(&mut context, cx);
            drive_one(driver, cx);
            result
        }),
    )
    .await
    .expect("Catalog Action made progress with the sole ordinary worker");
    let result = result.map(|value| assert!(matches!(value, StatementActionValue::Ok(_))));
    assert!(action.local_health_failure().is_none());
    let mut residual: Option<Box<dyn GovernedStatementResidual>> = None;
    action.handoff_residual_in(&mut residual).unwrap();
    drop(action);
    tokio::time::timeout(
        Duration::from_secs(5),
        std::future::poll_fn(|cx| {
            let result = residual.as_mut().unwrap().poll_retirement(cx);
            drive_one(driver, cx);
            result
        }),
    )
    .await
    .expect("same Catalog residual received the real physical exit")
    .unwrap();
    drop(residual);
    drop(alias);
    assert!(!window.has_retained_aliases());
    drop((query_permit, window));
    owner.take().unwrap().complete();
    root.business.release();
    result
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_sqlite_catalog_action_uses_original_qb_one_and_bootstrap_reopens_its_record() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("catalog-action-original.sqlite");
    let mut workers = QueryBlockingExecutorOwner::try_new(QueryBlockingExecutorConfig::new(
        NonZeroUsize::new(1).unwrap(),
        NonZeroUsize::new(1).unwrap(),
    ))
    .unwrap();
    let executor = workers.executor();
    let mut opening = prepare_sqlite(&path);
    let mut ready = None;
    open_sqlite(&mut opening, &executor, &mut ready).await;
    // There is no WorkloadControl/Root at this point. The finite Host bootstrap
    // reader is the actual owner before process readiness, not a fake Root or
    // Evidence-only purpose widened to business transactions.
    let request = opening
        .bootstrap_read_execution(ready.as_ref().unwrap(), &executor)
        .unwrap();
    let context =
        novarocks_state_store_api::StateStoreReadExecution::read_transaction_context(&request)
            .unwrap();
    let repository = bootstrap_catalog(
        &mut opening,
        novarocks_catalog_application::CatalogAttachmentRepository::open_in(
            ready.as_ref().unwrap().state_store().unwrap(),
            ready.as_ref().unwrap().run_policy(),
            &context,
        ),
    )
    .await
    .unwrap();
    assert!(
        bootstrap_catalog(&mut opening, repository.list_in(&context))
            .await
            .unwrap()
            .is_empty()
    );
    let unavailable = repository
        .observe_references_before_drop_in(
            &WrappedUnsupportedReferences,
            &novarocks_spi::connector::ConnectorInstanceId::parse("catalog_original").unwrap(),
            1,
            &context,
        )
        .await
        .expect_err("wrapped missing capability cannot be treated as no references");
    use novarocks_failure_contract::FailureProjection;
    assert_eq!(
        unavailable.failure_fact(),
        novarocks_state_store_api::StateStoreOriginalUnsupported.failure_fact()
    );
    assert!(
        unavailable
            .source_failure()
            .unwrap()
            .downcast_ref::<novarocks_catalog_application::CatalogApplicationError>()
            .is_some()
    );
    close_catalog_reader(&mut opening).await;
    drop(context);
    let (vocabulary, instance) = ready.as_ref().unwrap().original_blocking_sources().unwrap();
    let binding = crate::state_store::execution::StateStoreOriginalBinding {
        transaction_deadline: ready
            .as_ref()
            .unwrap()
            .state_store()
            .unwrap()
            .limits()
            .transaction_deadline,
        dependencies: ready
            .as_ref()
            .unwrap()
            .original_blocking_dependency_facts()
            .unwrap()
            .to_vec(),
        vocabulary,
        instance,
        executor: executor.clone(),
        policy: ready.as_ref().unwrap().run_policy(),
    };
    let (control, mut driver) = original_control();
    let application = Arc::new(RepositoryCatalogApplication {
        repository,
        latest: Mutex::new(None),
        fail_after_create: false,
    });
    run_catalog_action(
        &control,
        &mut driver,
        application.clone(),
        binding.clone(),
        catalog_create(false),
        1,
    )
    .await
    .unwrap();
    let created = application.latest.lock().unwrap().clone().unwrap();
    assert_eq!(application.repository.published_wakeups(), 1);
    run_catalog_action(
        &control,
        &mut driver,
        application.clone(),
        binding,
        catalog_create(true),
        2,
    )
    .await
    .unwrap();
    assert_eq!(application.latest.lock().unwrap().as_ref(), Some(&created));
    assert_eq!(application.repository.published_wakeups(), 1);
    drop(application);
    close_sqlite(&mut opening, &mut ready, &executor).await;

    let mut reopened = prepare_sqlite(&path);
    open_sqlite(&mut reopened, &executor, &mut ready).await;
    let (vocabulary, reopened_instance) =
        ready.as_ref().unwrap().original_blocking_sources().unwrap();
    assert_ne!(reopened_instance, instance);
    let request = reopened
        .bootstrap_read_execution(ready.as_ref().unwrap(), &executor)
        .unwrap();
    let context =
        novarocks_state_store_api::StateStoreReadExecution::read_transaction_context(&request)
            .unwrap();
    let repository = bootstrap_catalog(
        &mut reopened,
        novarocks_catalog_application::CatalogAttachmentRepository::open_in(
            ready.as_ref().unwrap().state_store().unwrap(),
            ready.as_ref().unwrap().run_policy(),
            &context,
        ),
    )
    .await
    .unwrap();
    assert_eq!(
        bootstrap_catalog(
            &mut reopened,
            repository.list_with_page_size_in(1, &context)
        )
        .await
        .unwrap(),
        vec![created]
    );
    close_catalog_reader(&mut reopened).await;
    drop(context);
    let binding = crate::state_store::execution::StateStoreOriginalBinding {
        transaction_deadline: ready
            .as_ref()
            .unwrap()
            .state_store()
            .unwrap()
            .limits()
            .transaction_deadline,
        dependencies: ready
            .as_ref()
            .unwrap()
            .original_blocking_dependency_facts()
            .unwrap()
            .to_vec(),
        vocabulary,
        instance: reopened_instance,
        executor: executor.clone(),
        policy: ready.as_ref().unwrap().run_policy(),
    };
    let application = Arc::new(RepositoryCatalogApplication {
        repository,
        latest: Mutex::new(None),
        fail_after_create: false,
    });
    run_catalog_action(
        &control,
        &mut driver,
        application.clone(),
        binding,
        novarocks_sql::semantic::CatalogSqlCommand::DropCatalog {
            name: "catalog_original".into(),
            if_exists: false,
        },
        3,
    )
    .await
    .unwrap();
    assert!(application.latest.lock().unwrap().is_none());
    assert_eq!(application.repository.published_wakeups(), 1);
    drop(application);
    close_sqlite(&mut reopened, &mut ready, &executor).await;
    workers
        .shutdown_until(Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_actual_catalog_bootstrap_borrow_still_closes_sqlite_before_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("catalog-bootstrap-abandoned.sqlite");
    let mut workers = QueryBlockingExecutorOwner::try_new(QueryBlockingExecutorConfig::new(
        NonZeroUsize::new(1).unwrap(),
        NonZeroUsize::new(1).unwrap(),
    ))
    .unwrap();
    let executor = workers.executor();
    let mut opening = prepare_sqlite(&path);
    let mut ready = None;
    open_sqlite(&mut opening, &executor, &mut ready).await;
    let request = opening
        .bootstrap_read_execution(ready.as_ref().unwrap(), &executor)
        .unwrap();
    let context =
        novarocks_state_store_api::StateStoreReadExecution::read_transaction_context(&request)
            .unwrap();
    let mut read = Box::pin(
        novarocks_catalog_application::CatalogAttachmentRepository::open_in(
            ready.as_ref().unwrap().state_store().unwrap(),
            ready.as_ref().unwrap().run_policy(),
            &context,
        ),
    );
    let pending = std::future::poll_fn(|cx| Poll::Ready(read.as_mut().poll(cx).is_pending())).await;
    assert!(
        pending,
        "the borrowed read registered an actual original phase before any owner dispatch"
    );
    // Do not hand-poll a fictitious completion or release a provider resource.
    // The original Host retains this bound Source after both borrowers leave.
    drop(read);
    drop(context);
    close_sqlite(&mut opening, &mut ready, &executor).await;
    let mut reopened = prepare_sqlite(&path);
    open_sqlite(&mut reopened, &executor, &mut ready).await;
    close_sqlite(&mut reopened, &mut ready, &executor).await;
    workers
        .shutdown_until(Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn actual_sqlite_catalog_commit_survives_required_materialization_failure() {
    use novarocks_query_application::api::StatementExternalEffectDisposition;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("catalog-known-commit.sqlite");
    let mut workers = QueryBlockingExecutorOwner::try_new(QueryBlockingExecutorConfig::new(
        NonZeroUsize::new(1).unwrap(),
        NonZeroUsize::new(1).unwrap(),
    ))
    .unwrap();
    let executor = workers.executor();
    let mut opening = prepare_sqlite(&path);
    let mut ready = None;
    open_sqlite(&mut opening, &executor, &mut ready).await;
    let request = opening
        .bootstrap_read_execution(ready.as_ref().unwrap(), &executor)
        .unwrap();
    let context =
        novarocks_state_store_api::StateStoreReadExecution::read_transaction_context(&request)
            .unwrap();
    let repository = bootstrap_catalog(
        &mut opening,
        novarocks_catalog_application::CatalogAttachmentRepository::open_in(
            ready.as_ref().unwrap().state_store().unwrap(),
            ready.as_ref().unwrap().run_policy(),
            &context,
        ),
    )
    .await
    .unwrap();
    close_catalog_reader(&mut opening).await;
    let (vocabulary, instance) = ready.as_ref().unwrap().original_blocking_sources().unwrap();
    let binding = crate::state_store::execution::StateStoreOriginalBinding {
        transaction_deadline: ready
            .as_ref()
            .unwrap()
            .state_store()
            .unwrap()
            .limits()
            .transaction_deadline,
        dependencies: ready
            .as_ref()
            .unwrap()
            .original_blocking_dependency_facts()
            .unwrap()
            .to_vec(),
        vocabulary,
        instance,
        executor: executor.clone(),
        policy: ready.as_ref().unwrap().run_policy(),
    };
    let application = Arc::new(RepositoryCatalogApplication {
        repository,
        latest: Mutex::new(None),
        fail_after_create: true,
    });
    let (control, mut driver) = original_control();
    let error = run_catalog_action(
        &control,
        &mut driver,
        application.clone(),
        binding,
        catalog_create(false),
        1,
    )
    .await
    .unwrap_err();
    assert_eq!(
        error.external_effect_disposition(),
        Some(StatementExternalEffectDisposition::KnownCommitted)
    );
    assert!(error.message().contains("do not retry statement"));
    assert!(
        error.publication_terminal().is_none(),
        "a local StateStore commit cannot mint a Lake terminal"
    );
    assert!(
        error
            .failure_cause()
            .unwrap()
            .downcast_ref::<novarocks_catalog_application::CatalogApplicationError>()
            .is_some()
    );
    let actual = application.latest.lock().unwrap().clone().unwrap();
    drop(application);
    close_sqlite(&mut opening, &mut ready, &executor).await;
    // Independent durable oracle: reopen the actual file with a fresh instance.
    let mut reopened = prepare_sqlite(&path);
    open_sqlite(&mut reopened, &executor, &mut ready).await;
    let request = reopened
        .bootstrap_read_execution(ready.as_ref().unwrap(), &executor)
        .unwrap();
    let context =
        novarocks_state_store_api::StateStoreReadExecution::read_transaction_context(&request)
            .unwrap();
    let repository = bootstrap_catalog(
        &mut reopened,
        novarocks_catalog_application::CatalogAttachmentRepository::open_in(
            ready.as_ref().unwrap().state_store().unwrap(),
            ready.as_ref().unwrap().run_policy(),
            &context,
        ),
    )
    .await
    .unwrap();
    let rows = bootstrap_catalog(&mut reopened, repository.list_in(&context))
        .await
        .unwrap();
    assert_eq!(rows, vec![actual]);
    close_catalog_reader(&mut reopened).await;
    drop(repository);
    close_sqlite(&mut reopened, &mut ready, &executor).await;
    workers
        .shutdown_until(Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
}
