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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use super::{MvKeyedProductService, MvRefreshTransition};
use crate::activity::{CanonicalMvTarget, MvActivityGateError, MvActivityOwner};
use crate::persistence::definition::CreateMvDefinitionRequest;
use crate::ports::{
    MvCreateCatalogRegistrationPort, MvCreateProviderPort, MvProviderFailure,
    MvProviderFailureKind, MvRefreshProgressPort,
};
use crate::process_runtime::ProcessRuntime;
use crate::product::{
    MvCommand, MvCreateCommand, MvCreatedTarget, MvOperationContext, MvProductErrorKind,
    MvProductResult, MvStagedTarget, MvTarget,
};
use crate::publication::{
    MvRefreshPublicationBase, MvRefreshPublicationFinalizationFacts, MvRefreshPublicationIntent,
    MvRefreshPublicationTechnique,
};
use crate::readiness::MvDropReadiness;
use crate::repository::InitialMvRefreshConfiguration;
use crate::scheduler::MvSchedulerConfig;
use crate::state_store_repository::StateStoreMvRepository;
use bytes::Bytes;
use novarocks_query_application::persisted_query_definition::{
    PersistedQueryDefinition, PersistedQueryDialect,
};
use novarocks_spi::connector::{
    ConnectorCommittedVersion, ConnectorTableObjectId, LakePublicationId,
};
use novarocks_state_store_testkit::testing::InMemoryStateStore;
use uuid::Uuid;

#[derive(Default)]
struct CreateEffects {
    events: Mutex<Vec<&'static str>>,
    fail_stage: bool,
    fail_abort: bool,
    fail_publish: Option<MvProviderFailureKind>,
    fail_projection: bool,
    fail_known_committed_projection: bool,
    drop_absent: bool,
}

impl CreateEffects {
    fn record(&self, event: &'static str) {
        self.events.lock().expect("event lock").push(event);
    }

    fn events(&self) -> Vec<&'static str> {
        self.events.lock().expect("event lock").clone()
    }
}

impl MvCreateProviderPort for CreateEffects {
    fn stage_target(
        &self,
        operation: MvOperationContext,
        command: &MvCommand,
    ) -> Result<MvStagedTarget, MvProviderFailure> {
        self.record("stage");
        let MvCommand::Create(create) = command else {
            return Err(MvProviderFailure::new(
                MvProviderFailureKind::InvalidRequest,
                "CREATE adapter received a non-CREATE command",
            ));
        };
        if self.fail_stage {
            return Err(MvProviderFailure::new(
                MvProviderFailureKind::Unavailable,
                "stage failed",
            ));
        }
        Ok(MvStagedTarget {
            target: create.target.clone(),
            staged_operation_id: operation.operation_id,
        })
    }

    fn publish_staged_target(
        &self,
        _operation: MvOperationContext,
        staged: &MvStagedTarget,
    ) -> Result<MvCreatedTarget, MvProviderFailure> {
        self.record("publish");
        if let Some(kind) = self.fail_publish {
            return Err(MvProviderFailure::new(kind, "publish failed"));
        }
        Ok(MvCreatedTarget {
            target: staged.target.clone(),
            object_id: ConnectorTableObjectId::try_new(Bytes::from_static(b"created-object"))
                .expect("test object ID"),
        })
    }

    fn abort_staged_target(
        &self,
        _operation: MvOperationContext,
        _staged: &MvStagedTarget,
    ) -> Result<(), MvProviderFailure> {
        self.record("abort");
        if self.fail_abort {
            return Err(MvProviderFailure::new(
                MvProviderFailureKind::Unavailable,
                "abort failed",
            ));
        }
        Ok(())
    }

    fn install_created_projection(
        &self,
        _operation: MvOperationContext,
        _target: &MvCreatedTarget,
    ) -> Result<(), MvProviderFailure> {
        self.record("install");
        if self.fail_projection {
            return Err(MvProviderFailure::new(
                MvProviderFailureKind::Unavailable,
                "projection failed",
            ));
        }
        Ok(())
    }
}

impl MvCreateCatalogRegistrationPort for CreateEffects {
    fn register_target(
        &self,
        _operation: MvOperationContext,
        _target: &MvCreatedTarget,
    ) -> Result<(), MvProviderFailure> {
        self.record("register");
        Ok(())
    }
}

struct RefreshExecutionEffects {
    effects: Arc<CreateEffects>,
    published: MvRefreshPublicationFinalizationFacts,
    execution: Option<Result<(), MvProviderFailure>>,
    projection: Option<Result<(), MvProviderFailure>>,
    execute_released: bool,
    projection_released: bool,
    execution_started: bool,
    projection_started: bool,
    dispatches: usize,
    projections: usize,
    execution_polls: usize,
    projection_polls: usize,
    drops: Arc<AtomicUsize>,
    observed: Option<(MvTarget, crate::product::MvRefreshAttemptIdentity)>,
    execution_panic: Option<Arc<str>>,
    projection_panic: Option<Arc<str>>,
}

impl RefreshExecutionEffects {
    fn new(effects: Arc<CreateEffects>, published: MvRefreshPublicationFinalizationFacts) -> Self {
        Self {
            effects,
            published,
            execution: None,
            projection: None,
            execute_released: true,
            projection_released: true,
            execution_started: false,
            projection_started: false,
            dispatches: 0,
            projections: 0,
            execution_polls: 0,
            projection_polls: 0,
            drops: Arc::new(AtomicUsize::new(0)),
            observed: None,
            execution_panic: None,
            projection_panic: None,
        }
    }
}

impl Drop for RefreshExecutionEffects {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

impl MvRefreshProgressPort for RefreshExecutionEffects {
    fn poll_execute_refresh(
        &mut self,
        _cx: &mut Context<'_>,
        target: &MvTarget,
        attempt: &crate::product::MvRefreshAttemptIdentity,
    ) -> Poll<()> {
        self.execution_polls += 1;
        if !self.execution_started {
            self.execution_started = true;
            self.dispatches += 1;
            self.observed = Some((target.clone(), attempt.clone()));
        }
        assert_eq!(
            self.observed.as_ref(),
            Some(&(target.clone(), attempt.clone()))
        );
        if !self.execute_released {
            return Poll::Pending;
        }
        if self.execution.is_none() {
            self.execution = Some(Ok(()));
        }
        if let Some(cause) = &self.execution_panic {
            std::panic::panic_any(Arc::clone(cause));
        }
        Poll::Ready(())
    }

    fn execution_outcome(
        &self,
    ) -> Option<Result<&MvRefreshPublicationFinalizationFacts, &MvProviderFailure>> {
        self.execution.as_ref().map(|result| match result {
            Ok(()) => Ok(&self.published),
            Err(error) => Err(error),
        })
    }

    fn poll_project_known_committed(
        &mut self,
        _cx: &mut Context<'_>,
        _target: &MvTarget,
    ) -> Poll<()> {
        self.projection_polls += 1;
        if !self.projection_started {
            self.projection_started = true;
            self.projections += 1;
            self.effects.record("project_known_committed");
        }
        if !self.projection_released {
            return Poll::Pending;
        }
        self.projection = Some(if self.effects.fail_known_committed_projection {
            Err(MvProviderFailure::new(
                MvProviderFailureKind::Unavailable,
                "known-committed projection failed",
            ))
        } else {
            Ok(())
        });
        if let Some(cause) = &self.projection_panic {
            std::panic::panic_any(Arc::clone(cause));
        }
        Poll::Ready(())
    }

    fn projection_outcome(&self) -> Option<&Result<(), MvProviderFailure>> {
        self.projection.as_ref()
    }
}

fn finish_refresh(
    service: &MvKeyedProductService,
    target: MvTarget,
    attempt: crate::product::MvRefreshAttemptIdentity,
    execution: &mut dyn MvRefreshProgressPort,
) -> Result<MvProductResult, crate::product::MvProductError> {
    let mut transition = MvRefreshTransition::new(target, attempt);
    let mut cx = Context::from_waker(std::task::Waker::noop());
    assert!(
        service
            .poll_refresh(&mut cx, &mut transition, execution)
            .is_ready()
    );
    transition.result().expect("stable refresh result").clone()
}

fn finalization_facts(
    target: &MvTarget,
    attempt: &crate::product::MvRefreshAttemptIdentity,
) -> MvRefreshPublicationFinalizationFacts {
    let target_object_id = ConnectorTableObjectId::try_new(Bytes::from_static(b"mv-target-object"))
        .expect("target object ID");
    let intent = MvRefreshPublicationIntent::try_new(
        attempt.publication_id,
        target_object_id.clone(),
        Some(7),
        crate::test_admission::publication_admission(
            target.catalog().unwrap_or("ice"),
            target.namespace(),
            target.name(),
            attempt.publication_id,
            &target_object_id,
        ),
        MvRefreshPublicationTechnique::Full,
        vec![
            MvRefreshPublicationBase::try_new(
                0,
                "ice.db.base".to_string(),
                ConnectorTableObjectId::try_new(Bytes::from_static(b"base-object"))
                    .expect("base object ID"),
                None,
                7,
            )
            .expect("base fact"),
        ],
        "definition-fingerprint".to_string(),
        target.catalog().expect("test target catalog").to_string(),
        target.namespace().to_string(),
        target.name().to_string(),
    )
    .expect("publication intent");
    MvRefreshPublicationFinalizationFacts::try_new(
        intent,
        ConnectorCommittedVersion::try_new(Bytes::from_static(b"mv-publication-version"), Some(42))
            .expect("publication version"),
    )
    .expect("published facts")
}

fn management() -> Arc<crate::management::ManagementEntrance> {
    Arc::new(crate::management::ManagementEntrance::new(
        crate::management::DeploymentOwner::parse("test-deployment").expect("owner"),
        crate::management::ProcessIncarnation::parse("test-process").expect("incarnation"),
    ))
}

async fn repository() -> Arc<StateStoreMvRepository> {
    StateStoreMvRepository::open(
        Arc::new(InMemoryStateStore::new("keyed-service-tests")),
        Default::default(),
    )
    .await
    .expect("open real StateStore MV owner")
}

async fn service_with_refresh_readiness() -> MvKeyedProductService {
    MvKeyedProductService::new_with_keyed_management_continuation(
        MvSchedulerConfig::default(),
        repository().await,
        Arc::<ProcessRuntime<MvTarget, LakePublicationId>>::default(),
        management(),
        Default::default(),
    )
}

// Product effect-order tests use the port's generic witness, never forge
// a native readiness/CAS guard. Real keyed guards are tested by readiness.
struct EffectOrderDropGuard(MvTarget);

fn create_command() -> MvCreateCommand {
    MvCreateCommand {
        target: MvTarget::from_parts(Some("iceberg"), "db", "mv"),
        if_not_exists: false,
        definition: CreateMvDefinitionRequest {
            query_definition: PersistedQueryDefinition::new(
                "SELECT 1",
                PersistedQueryDialect::StarRocks,
                "iceberg",
                "db",
            )
            .expect("valid persisted query"),
            base_table_refs: Vec::new(),
            primary_key_columns: Vec::new(),
            storage_engine: "iceberg".to_string(),
            target_catalog: Some("iceberg".to_string()),
            target_namespace: Some("db".to_string()),
            target_table: Some("mv".to_string()),
            created_at_ms: 1,
        },
        refresh: InitialMvRefreshConfiguration::default(),
        dependencies: Vec::new(),
    }
}

fn operation() -> MvOperationContext {
    MvOperationContext {
        operation_id: Uuid::nil(),
    }
}

#[test]
fn stopping_product_service_rejects_new_background_activity() {
    let service = MvKeyedProductService::default();
    service.begin_stopping();

    assert!(matches!(
        service.request_activity(
            CanonicalMvTarget::from_parts(Some("ice"), "sales", "mv_orders"),
            MvActivityOwner::ScheduledRefresh,
        ),
        Err(MvActivityGateError::Stopping)
    ));
}

#[test]
fn product_service_retains_the_configured_scheduler_instance() {
    let service = MvKeyedProductService::new(MvSchedulerConfig::new(true, 73, 2, 11, 99));

    assert_eq!(service.scheduler_tick_interval_ms(), 73);
    assert!(service.with_refresh_scheduler(|scheduler| scheduler.enabled()));
    let target = MvTarget::from_parts(Some("ice"), "sales", "mv");
    let projection = crate::persistence::keyed_documents::StoredKeyedMvProjection {
        mv_id: 7,
        facts: crate::persistence::test_support::sample_keyed_projection(target.clone(), Some(11)),
    };
    service.with_refresh_scheduler(|scheduler| {
        scheduler.observe_projection(projection, 1_700_000_061_000);
    });
    let ready = service.with_refresh_scheduler(|scheduler| scheduler.take_ready());
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].projection().mv_id, 7);
    assert_eq!(ready[0].target(), &target);
    assert!(
        service
            .with_refresh_scheduler(|scheduler| scheduler.take_ready())
            .is_empty()
    );
}

#[tokio::test]
async fn serving_product_retains_the_exact_management_authority() {
    let management = management();
    let runtime = Arc::<ProcessRuntime<MvTarget, LakePublicationId>>::default();
    let service = MvKeyedProductService::new_with_keyed_management_continuation(
        MvSchedulerConfig::default(),
        repository().await,
        Arc::clone(&runtime),
        Arc::clone(&management),
        Default::default(),
    );

    assert!(Arc::ptr_eq(
        &service.management_entrance().expect("serving authority"),
        &management,
    ));
    let readiness = service
        .keyed_readiness_service()
        .expect("sole keyed readiness");
    assert!(service.management_continuation().is_some());
    assert!(!runtime.has_active_publications());
    let target = create_command().target;
    let attempt = service.reserve_refresh_attempt();
    let lease = service
        .begin_refresh_publication(&target, &attempt)
        .unwrap();
    assert!(runtime.has_active_publications());
    assert!(readiness.ensure_no_active_publications().is_err());
    drop(lease);
    assert!(!runtime.has_active_publications());
    readiness.ensure_no_active_publications().unwrap();
}

#[test]
fn create_runs_product_effects_in_required_order() {
    let service = MvKeyedProductService::default();
    let effects = CreateEffects::default();

    let result = service
        .create(operation(), create_command(), &effects, &effects)
        .expect("create succeeds");

    assert!(matches!(result, MvProductResult::Created(_)));
    // One atomic point: the target and its documents become visible
    // together. No visible empty table, no post-create descriptor sync.
    assert_eq!(
        effects.events(),
        ["stage", "publish", "install", "register"]
    );
}

#[tokio::test]
async fn known_committed_refresh_projection_is_product_finalization() {
    let service = service_with_refresh_readiness().await;
    let effects = Arc::new(CreateEffects::default());
    let target = MvTarget::from_parts(Some("iceberg"), "db", "mv");
    let attempt = service.reserve_refresh_attempt();
    let published = finalization_facts(&target, &attempt);

    let mut execution = RefreshExecutionEffects::new(Arc::clone(&effects), published);
    let result =
        finish_refresh(&service, target, attempt, &mut execution).expect("projection succeeds");

    assert!(matches!(result, MvProductResult::Acknowledged));
    assert_eq!(effects.events(), ["project_known_committed"]);
}

#[tokio::test]
async fn known_committed_refresh_rejects_a_different_publication_before_projection() {
    let service = service_with_refresh_readiness().await;
    let effects = Arc::new(CreateEffects::default());
    let target = MvTarget::from_parts(Some("iceberg"), "db", "mv");
    let attempt = service.reserve_refresh_attempt();
    let other_attempt = service.reserve_refresh_attempt();
    let published = finalization_facts(&target, &other_attempt);

    let mut execution = RefreshExecutionEffects::new(Arc::clone(&effects), published);
    let error = finish_refresh(&service, target, attempt, &mut execution)
        .expect_err("different publication identity must be rejected");

    assert_eq!(error.kind(), MvProductErrorKind::Corruption);
    assert!(effects.events().is_empty());
    use novarocks_failure_contract::{
        FailureClass, FailureProjection, FailureReason, SharedFailureCause, domain,
    };
    let fact = error.failure_fact();
    assert_eq!(fact.class(), FailureClass::Data);
    assert_eq!(
        fact.reason(),
        FailureReason::new(domain::METADATA, crate::product::failure_reason::CORRUPTION)
    );
    let cause = SharedFailureCause::new(error);
    assert_eq!(cause.failure_fact(), fact);
    assert_eq!(
        cause
            .downcast_ref::<crate::product::MvProductError>()
            .unwrap()
            .kind(),
        MvProductErrorKind::Corruption
    );
    assert_eq!(execution.dispatches, 1);
    assert_eq!(execution.projections, 0);
}

#[tokio::test]
async fn product_owns_refresh_publication_admission_and_lease_release() {
    let service = service_with_refresh_readiness().await;
    let readiness = service.keyed_readiness_service().expect("keyed readiness");
    let target = MvTarget::from_parts(Some("iceberg"), "db", "mv");
    let first = service.reserve_refresh_attempt();
    let lease = service
        .begin_refresh_publication(&target, &first)
        .expect("product begins the first publication");
    let second = service.reserve_refresh_attempt();
    assert!(readiness.ensure_no_active_publications().is_err());

    let Err(error) = service.begin_refresh_publication(&target, &second) else {
        panic!("a concurrent publication must be rejected");
    };
    assert_eq!(error.kind(), MvProductErrorKind::Conflict);

    drop(lease);
    readiness.ensure_no_active_publications().unwrap();
    service
        .begin_refresh_publication(&target, &second)
        .expect("dropping the product lease releases the target");
}

#[tokio::test]
async fn known_committed_refresh_keeps_external_commit_when_projection_fails() {
    let service = service_with_refresh_readiness().await;
    let effects = Arc::new(CreateEffects {
        fail_known_committed_projection: true,
        ..Default::default()
    });
    let target = MvTarget::from_parts(Some("iceberg"), "db", "mv");
    let attempt = service.reserve_refresh_attempt();
    let published = finalization_facts(&target, &attempt);

    let mut execution = RefreshExecutionEffects::new(Arc::clone(&effects), published);
    let error = finish_refresh(&service, target, attempt, &mut execution)
        .expect_err("projection finalization fails");

    assert_eq!(
        error.kind(),
        MvProductErrorKind::KnownCommittedFinalizeFailed
    );
    assert_eq!(effects.events(), ["project_known_committed"]);
}

#[test]
fn a_failed_stage_leaves_nothing_to_compensate() {
    let service = MvKeyedProductService::default();
    let effects = CreateEffects {
        fail_stage: true,
        ..Default::default()
    };

    let error = service
        .create(operation(), create_command(), &effects, &effects)
        .expect_err("stage failure ends the statement");

    assert_eq!(error.kind(), MvProductErrorKind::Unavailable);
    assert_eq!(effects.events(), ["stage"], "nothing to abort or drop");
}

#[test]
fn a_proven_unpublished_stage_is_discarded() {
    let service = MvKeyedProductService::default();
    let effects = CreateEffects {
        fail_publish: Some(MvProviderFailureKind::KnownUncommitted),
        ..Default::default()
    };

    let error = service
        .create(operation(), create_command(), &effects, &effects)
        .expect_err("publish failed");

    assert_eq!(error.kind(), MvProductErrorKind::ProviderKnownUncommitted);
    assert_eq!(effects.events(), ["stage", "publish", "abort"]);
}

#[test]
fn an_unknown_publication_keeps_its_stage() {
    let service = MvKeyedProductService::default();
    let effects = CreateEffects {
        fail_publish: Some(MvProviderFailureKind::CommitUnknown),
        ..Default::default()
    };

    let error = service
        .create(operation(), create_command(), &effects, &effects)
        .expect_err("publish outcome is unknown");

    assert_eq!(error.kind(), MvProductErrorKind::CommitUnknown);
    assert_eq!(
        effects.events(),
        ["stage", "publish"],
        "aborting could delete a create that actually succeeded"
    );
}

#[test]
fn a_proven_committed_publication_finalization_failure_keeps_its_stage() {
    let service = MvKeyedProductService::default();
    let effects = CreateEffects {
        fail_publish: Some(MvProviderFailureKind::KnownCommittedFinalizeFailed),
        ..Default::default()
    };
    let error = service
        .create(operation(), create_command(), &effects, &effects)
        .unwrap_err();
    assert_eq!(
        error.kind(),
        MvProductErrorKind::KnownCommittedFinalizeFailed
    );
    assert_eq!(effects.events(), ["stage", "publish"]);
}

#[test]
fn create_marks_projection_failure_after_target_commit() {
    let service = MvKeyedProductService::default();
    let effects = CreateEffects {
        fail_projection: true,
        ..Default::default()
    };

    let error = service
        .create(operation(), create_command(), &effects, &effects)
        .expect_err("projection failed after target commit");

    assert_eq!(
        error.kind(),
        MvProductErrorKind::KnownCommittedFinalizeFailed
    );
    assert_eq!(
        effects.events(),
        ["stage", "publish", "install"],
        "a finalization failure must not undo the published create"
    );
}

#[test]
fn drop_orders_provider_projection_and_catalog_effects() {
    let service = MvKeyedProductService::default();
    let effects = CreateEffects::default();
    let target = MvTarget::from_parts(Some("iceberg"), "db", "mv");

    let mut transition = super::MvDropTransition::new(operation(), target, false);
    let mut execution = SuspendedDrop {
        effects: &effects,
        yielded: [true; 4],
        guard: None,
    };
    let mut cx = Context::from_waker(std::task::Waker::noop());
    assert!(
        service
            .poll_drop(&mut cx, &mut transition, &mut execution)
            .is_ready()
    );
    let result = transition
        .result()
        .expect("DROP transition completed")
        .as_ref()
        .expect("drop succeeds");

    assert!(matches!(result, MvProductResult::Dropped));
    assert_eq!(
        effects.events(),
        ["prepare_drop", "drop", "delete_projection", "unregister"]
    );
}

#[test]
fn drop_if_exists_absence_emits_no_external_effect() {
    let service = MvKeyedProductService::default();
    let effects = CreateEffects {
        drop_absent: true,
        ..Default::default()
    };
    let target = MvTarget::from_parts(Some("iceberg"), "db", "missing_mv");

    let mut transition = super::MvDropTransition::new(operation(), target, true);
    let mut execution = SuspendedDrop {
        effects: &effects,
        yielded: [true; 4],
        guard: None,
    };
    let mut cx = Context::from_waker(std::task::Waker::noop());
    assert!(
        service
            .poll_drop(&mut cx, &mut transition, &mut execution)
            .is_ready()
    );
    let result = transition
        .result()
        .expect("DROP transition completed")
        .as_ref()
        .expect("missing IF EXISTS target is acknowledged");

    assert!(matches!(result, MvProductResult::Acknowledged));
    assert_eq!(effects.events(), ["prepare_drop"]);
}
#[test]
fn foreground_and_background_activity_share_the_product_fifo() {
    let service = MvKeyedProductService::default();
    let target = create_command().target;
    let foreground = service
        .acquire_foreground(target.clone(), MvActivityOwner::ManualRefresh, || false)
        .unwrap();
    let mut scheduled = service
        .request_activity(target.clone(), MvActivityOwner::ScheduledRefresh)
        .unwrap();
    let mut maintenance = service
        .request_activity(target, MvActivityOwner::AutomaticMaintenance)
        .unwrap();
    assert!(scheduled.try_acquire().unwrap().is_none());
    assert!(maintenance.try_acquire().unwrap().is_none());
    drop(foreground);
    assert!(maintenance.try_acquire().unwrap().is_none());
    let scheduled_lease = scheduled.try_acquire().unwrap().expect("FIFO head");
    assert!(maintenance.try_acquire().unwrap().is_none());
    drop(scheduled_lease);
    maintenance
        .try_acquire()
        .unwrap()
        .expect("shared FIFO advances");
}

#[test]
fn unit_only_product_refuses_refresh_without_readiness() {
    let service = MvKeyedProductService::default();
    let attempt = service.reserve_refresh_attempt();
    let result = service.begin_refresh_publication(&create_command().target, &attempt);
    assert!(matches!(result, Err(e) if e.kind() == MvProductErrorKind::Unavailable));
}

#[test]
fn a_typed_admission_refusal_does_not_skip_staged_target_abort() {
    use novarocks_functions::{FunctionBindingError, FunctionOverloadId};
    use novarocks_sql::compiler::SqlCompileError;
    let SqlCompileError::Analyze(analyze) =
        SqlCompileError::from(FunctionBindingError::UnavailableImplementation(
            FunctionOverloadId::try_new("fixture/mv/stage/missing-pure-v1").unwrap(),
        ))
    else {
        panic!("missing implementation must retain analysis identity");
    };
    let message = analyze.to_string();
    for fail_abort in [false, true] {
        let effects = CreateEffects {
            fail_abort,
            ..Default::default()
        };
        let primary = MvProviderFailure::new(MvProviderFailureKind::InvalidRequest, &message)
            .with_unavailable_implementation(Some(analyze.clone()));
        let staged = MvStagedTarget {
            target: create_command().target,
            staged_operation_id: operation().operation_id,
        };
        let aborted = effects.abort_staged_target(operation(), &staged);
        let failure = super::settle_stage_abort(primary.into_product_error(), aborted);
        assert_eq!(effects.events(), ["abort"]);
        assert_eq!(failure.kind(), MvProductErrorKind::InvalidRequest);
        assert_eq!(failure.compile_control_error(), None);
        if fail_abort {
            assert_eq!(failure.unavailable_implementation(), None);
            assert_eq!(
                failure.message(),
                format!("{message}; staged target abort failed: abort failed")
            );
        } else {
            assert_eq!(failure.unavailable_implementation(), Some(&analyze));
            assert_eq!(failure.message(), message);
        }
    }
}

#[test]
fn a_committed_finalization_failure_cannot_become_a_pure_admission_refusal() {
    use novarocks_functions::{FunctionBindingError, FunctionOverloadId};
    use novarocks_sql::compiler::SqlCompileError;
    let SqlCompileError::Analyze(analyze) =
        SqlCompileError::from(FunctionBindingError::UnavailableImplementation(
            FunctionOverloadId::try_new("fixture/mv/finalization/missing-pure-v1").unwrap(),
        ))
    else {
        panic!("missing implementation must retain analysis identity");
    };
    let message = analyze.to_string();
    let failure = super::known_committed_finalize_failure(
        MvProviderFailure::new(MvProviderFailureKind::InvalidRequest, &message)
            .with_unavailable_implementation(Some(analyze)),
    );
    assert_eq!(
        failure.kind(),
        MvProductErrorKind::KnownCommittedFinalizeFailed
    );
    assert_eq!(failure.unavailable_implementation(), None);
    assert_eq!(failure.message(), message);
}

#[tokio::test]
async fn cancelling_a_borrowed_refresh_wait_retains_the_original_execution_and_lease() {
    let service = service_with_refresh_readiness().await;
    let readiness = service.keyed_readiness_service().expect("keyed readiness");
    let effects = Arc::new(CreateEffects::default());
    let target = MvTarget::from_parts(Some("iceberg"), "db", "borrowed");
    let attempt = service.reserve_refresh_attempt();
    let published = finalization_facts(&target, &attempt);
    let mut execution = RefreshExecutionEffects::new(Arc::clone(&effects), published);
    execution.execute_released = false;
    let drops = Arc::clone(&execution.drops);
    let mut transition = MvRefreshTransition::new(target.clone(), attempt.clone());
    let mut cx = Context::from_waker(std::task::Waker::noop());
    {
        let mut borrowed = Box::pin(std::future::poll_fn(|cx| {
            service.poll_refresh(cx, &mut transition, &mut execution)
        }));
        assert!(borrowed.as_mut().poll(&mut cx).is_pending());
        // Only the borrowing wait is cancelled; the original Action slots remain.
        drop(borrowed);
    }
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert!(transition.result().is_none());
    assert!(readiness.ensure_no_active_publications().is_err());
    let second = service.reserve_refresh_attempt();
    let Err(error) = service.begin_refresh_publication(&target, &second) else {
        panic!("the first borrowed execution still owns publication admission");
    };
    assert_eq!(error.kind(), MvProductErrorKind::Conflict);
    assert!(
        service
            .poll_refresh(&mut cx, &mut transition, &mut execution)
            .is_pending()
    );
    assert_eq!(execution.dispatches, 1);
    assert_eq!(execution.observed.as_ref(), Some(&(target, attempt)));
    assert!(effects.events().is_empty());
    execution.execute_released = true;
    assert!(
        service
            .poll_refresh(&mut cx, &mut transition, &mut execution)
            .is_ready()
    );
    assert!(matches!(
        transition.result(),
        Some(Ok(MvProductResult::Acknowledged))
    ));
    readiness.ensure_no_active_publications().unwrap();
    let polls = (execution.execution_polls, execution.projection_polls);
    assert!(
        service
            .poll_refresh(&mut cx, &mut transition, &mut execution)
            .is_ready()
    );
    assert_eq!(
        (execution.execution_polls, execution.projection_polls),
        polls
    );
    assert_eq!(execution.dispatches, 1);
    assert_eq!(execution.projections, 1);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(execution);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cancelling_a_projection_wait_retains_the_same_known_commit_without_reexecution() {
    let service = service_with_refresh_readiness().await;
    let readiness = service.keyed_readiness_service().expect("keyed readiness");
    let effects = Arc::new(CreateEffects::default());
    let target = MvTarget::from_parts(Some("iceberg"), "db", "projection_wait");
    let attempt = service.reserve_refresh_attempt();
    let published = finalization_facts(&target, &attempt);
    let mut execution = RefreshExecutionEffects::new(Arc::clone(&effects), published);
    execution.projection_released = false;
    let original_facts = &execution.published as *const _;
    let drops = Arc::clone(&execution.drops);
    let mut transition = MvRefreshTransition::new(target, attempt);
    let mut cx = Context::from_waker(std::task::Waker::noop());
    {
        let mut borrowed = Box::pin(std::future::poll_fn(|cx| {
            service.poll_refresh(cx, &mut transition, &mut execution)
        }));
        assert!(borrowed.as_mut().poll(&mut cx).is_pending());
        drop(borrowed);
    }
    assert_eq!(execution.dispatches, 1);
    assert_eq!(execution.projections, 1);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert!(transition.result().is_none());
    assert!(readiness.ensure_no_active_publications().is_err());
    assert_eq!(
        execution.execution_outcome().unwrap().unwrap() as *const _,
        original_facts
    );
    let execution_polls = execution.execution_polls;
    assert!(
        service
            .poll_refresh(&mut cx, &mut transition, &mut execution)
            .is_pending()
    );
    assert_eq!(execution.execution_polls, execution_polls);
    execution.projection_released = true;
    assert!(
        service
            .poll_refresh(&mut cx, &mut transition, &mut execution)
            .is_ready()
    );
    assert!(matches!(
        transition.result(),
        Some(Ok(MvProductResult::Acknowledged))
    ));
    assert_eq!(execution.dispatches, 1);
    assert_eq!(execution.projections, 1);
    assert_eq!(effects.events(), ["project_known_committed"]);
    readiness.ensure_no_active_publications().unwrap();
}

#[tokio::test]
async fn a_foreign_refresh_unwind_retains_the_received_fact_and_never_repolls_or_finishes() {
    for during_projection in [false, true] {
        let service = service_with_refresh_readiness().await;
        let readiness = service.keyed_readiness_service().expect("keyed readiness");
        let effects = Arc::new(CreateEffects::default());
        let target = MvTarget::from_parts(Some("iceberg"), "db", "poisoned");
        let attempt = service.reserve_refresh_attempt();
        let published = finalization_facts(&target, &attempt);
        let mut execution = RefreshExecutionEffects::new(Arc::clone(&effects), published);
        let cause: Arc<str> = Arc::from("original foreign refresh panic");
        if during_projection {
            execution.projection_panic = Some(Arc::clone(&cause));
        } else {
            execution.execution_panic = Some(Arc::clone(&cause));
        }
        let drops = Arc::clone(&execution.drops);
        let mut transition = MvRefreshTransition::new(target, attempt);
        let mut cx = Context::from_waker(std::task::Waker::noop());
        let raw = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            service.poll_refresh(&mut cx, &mut transition, &mut execution)
        }))
        .expect_err("the original Source receives the foreign panic");
        let raw = raw.downcast::<Arc<str>>().expect("same raw cause type");
        assert!(Arc::ptr_eq(raw.as_ref(), &cause));
        assert!(execution.execution_outcome().unwrap().is_ok());
        assert_eq!(execution.projection.is_some(), during_projection);
        assert!(transition.result().is_none());
        assert!(readiness.ensure_no_active_publications().is_err());
        let polls = (execution.execution_polls, execution.projection_polls);
        assert!(
            service
                .poll_refresh(&mut cx, &mut transition, &mut execution)
                .is_pending()
        );
        assert_eq!(
            (execution.execution_polls, execution.projection_polls),
            polls
        );
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        // This fixture owns no physical request. Explicit disposal ends the test;
        // production retains the same poisoned Action through the existing QA owner.
        drop(execution);
        drop(transition);
        readiness.ensure_no_active_publications().unwrap();
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn original_operations_and_service_share_the_same_fifo_and_scheduler() {
    let service = MvKeyedProductService::new(MvSchedulerConfig::new(true, 73, 2, 11, 99));
    let original = service.original_operations();
    assert!(Arc::ptr_eq(&original, &service.original_operations()));
    let target = create_command().target;
    let held = original
        .acquire_foreground(target.clone(), MvActivityOwner::Create, || false)
        .unwrap();
    let mut waiting = service
        .request_activity(target.clone(), MvActivityOwner::Drop)
        .unwrap();
    assert!(waiting.try_acquire().unwrap().is_none());
    drop(held);
    let handed = waiting
        .try_acquire()
        .unwrap()
        .expect("same FIFO release reaches the service borrower");
    let mut later = original
        .request_activity(target.clone(), MvActivityOwner::Alter)
        .unwrap();
    assert!(later.try_acquire().unwrap().is_none());
    drop(handed);
    drop(
        later
            .try_acquire()
            .unwrap()
            .expect("same FIFO source reaches the original borrower"),
    );

    let projection = crate::persistence::keyed_documents::StoredKeyedMvProjection {
        mv_id: 7,
        facts: crate::persistence::test_support::sample_keyed_projection(target.clone(), Some(11)),
    };
    service.with_refresh_scheduler(|scheduler| {
        scheduler.observe_projection(projection, 1_700_000_061_000)
    });
    let ready = original.with_refresh_scheduler(|scheduler| scheduler.take_ready());
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].projection().mv_id, 7);
    assert_eq!(ready[0].target(), &target);
    assert!(
        service
            .with_refresh_scheduler(|scheduler| scheduler.take_ready())
            .is_empty(),
        "there is one scheduler, not a copied queue"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn active_original_operations_do_not_retain_the_service_or_joined_runtime() {
    use crate::process_runtime::{MvBackgroundRuntime, MvBackgroundTasks};
    use std::time::{Duration, Instant};

    let service = Arc::new(service_with_refresh_readiness().await);
    let weak_service = Arc::downgrade(&service);
    let original = service.original_operations();
    let weak_operations = Arc::downgrade(&original);
    let target = create_command().target;
    let attempt = original.reserve_refresh_attempt();
    let publication = original
        .begin_refresh_publication(&target, &attempt)
        .unwrap();
    let readiness = original.keyed_readiness_service().unwrap();
    assert!(readiness.ensure_no_active_publications().is_err());
    let activity = original
        .acquire_foreground(target, MvActivityOwner::Create, || false)
        .unwrap();

    let refresh_operations = original.clone();
    let maintenance_operations = original.clone();
    let (refresh_started, refresh_observed) = tokio::sync::oneshot::channel();
    let (maintenance_started, maintenance_observed) = tokio::sync::oneshot::channel();
    let mut refresh_started = Some(refresh_started);
    let mut maintenance_started = Some(maintenance_started);
    let runtime = MvBackgroundRuntime::start(
        &tokio::runtime::Handle::current(),
        crate::process_runtime::test_health_port(),
        Duration::from_secs(60),
        Duration::from_secs(60),
        MvBackgroundTasks::new(
            Box::new(move |_, _| {
                let original = refresh_operations.clone();
                let started = refresh_started.take();
                Box::pin(async move {
                    assert_eq!(
                        original.scheduler_tick_interval_ms(),
                        MvSchedulerConfig::default().tick_interval_ms()
                    );
                    if let Some(started) = started {
                        let _ = started.send(());
                    }
                })
            }),
            Box::new(move |_| {
                let original = maintenance_operations.clone();
                let started = maintenance_started.take();
                Box::pin(async move {
                    assert!(original.keyed_readiness_service().is_some());
                    if let Some(started) = started {
                        let _ = started.send(());
                    }
                })
            }),
        ),
    )
    .unwrap();
    service
        .begin_background_start()
        .unwrap()
        .install(runtime)
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        refresh_observed.await.unwrap();
        maintenance_observed.await.unwrap();
    })
    .await
    .expect("both original runtime callbacks must actually run");
    service.begin_stopping();
    assert!(matches!(
        original.request_activity(create_command().target, MvActivityOwner::Drop),
        Err(MvActivityGateError::Stopping)
    ));
    service
        .shutdown_background_workers_until(Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    // A successful second start reservation proves the sole installed runtime
    // was actually joined and retired, while its business source is still held.
    drop(service.begin_background_start().unwrap());
    drop(service);
    assert!(
        weak_service.upgrade().is_none(),
        "an original business source cannot retain the runtime wrapper"
    );
    assert!(weak_operations.upgrade().is_some());
    assert!(
        readiness.ensure_no_active_publications().is_err(),
        "joining the workers did not end the original publication"
    );
    drop(publication);
    readiness.ensure_no_active_publications().unwrap();
    drop(activity);
    drop(original);
    assert!(weak_operations.upgrade().is_none());
}

/// These are the original CREATE effects with a real borrowed suspension
/// before each phase. Expected events come from the existing product contract.
struct SuspendedCreate<'a> {
    effects: &'a CreateEffects,
    yielded: [bool; 5],
    panic_stage: bool,
    publish_noop: bool,
}
impl SuspendedCreate<'_> {
    fn yield_once(&mut self, index: usize, cx: &mut Context<'_>) -> bool {
        if self.yielded[index] {
            return false;
        }
        self.yielded[index] = true;
        cx.waker().wake_by_ref();
        true
    }
}
impl crate::ports::MvCreateProgressPort for SuspendedCreate<'_> {
    fn poll_stage_target(
        &mut self,
        cx: &mut Context<'_>,
        operation: MvOperationContext,
        command: &MvCommand,
    ) -> Poll<Result<MvStagedTarget, MvProviderFailure>> {
        if self.yield_once(0, cx) {
            return Poll::Pending;
        }
        if self.panic_stage {
            self.effects.record("stage-unwind");
            panic!("original CREATE stage did not return");
        }
        Poll::Ready(self.effects.stage_target(operation, command))
    }
    fn poll_publish_staged_target(
        &mut self,
        cx: &mut Context<'_>,
        operation: MvOperationContext,
        staged: &MvStagedTarget,
    ) -> Poll<Result<crate::product::MvCreatePublicationOutcome, MvProviderFailure>> {
        if self.yield_once(1, cx) {
            return Poll::Pending;
        }
        if self.publish_noop {
            self.effects.record("publish-noop");
            Poll::Ready(Ok(crate::product::MvCreatePublicationOutcome::NoOp))
        } else {
            Poll::Ready(
                self.effects
                    .publish_staged_target(operation, staged)
                    .map(crate::product::MvCreatePublicationOutcome::Applied),
            )
        }
    }
    fn poll_abort_staged_target(
        &mut self,
        cx: &mut Context<'_>,
        operation: MvOperationContext,
        staged: &MvStagedTarget,
    ) -> Poll<Result<(), MvProviderFailure>> {
        if self.yield_once(2, cx) {
            return Poll::Pending;
        }
        Poll::Ready(self.effects.abort_staged_target(operation, staged))
    }
    fn poll_install_created_projection(
        &mut self,
        cx: &mut Context<'_>,
        operation: MvOperationContext,
        target: &MvCreatedTarget,
    ) -> Poll<Result<(), MvProviderFailure>> {
        if self.yield_once(3, cx) {
            return Poll::Pending;
        }
        Poll::Ready(self.effects.install_created_projection(operation, target))
    }
    fn poll_register_target(
        &mut self,
        cx: &mut Context<'_>,
        operation: MvOperationContext,
        target: &MvCreatedTarget,
    ) -> Poll<Result<(), MvProviderFailure>> {
        if self.yield_once(4, cx) {
            return Poll::Pending;
        }
        Poll::Ready(self.effects.register_target(operation, target))
    }
}

#[test]
fn borrowed_create_wait_drop_keeps_original_product_order_and_single_dispatch() {
    let product = MvKeyedProductService::default();
    let effects = CreateEffects::default();
    let mut execution = SuspendedCreate {
        effects: &effects,
        yielded: [false; 5],
        panic_stage: false,
        publish_noop: false,
    };
    let mut transition = super::MvCreateTransition::new(operation(), create_command());
    let mut cx = Context::from_waker(std::task::Waker::noop());
    for expected in [
        vec![],
        vec!["stage"],
        vec!["stage", "publish"],
        vec!["stage", "publish", "install"],
    ] {
        let mut waiting = Box::pin(std::future::poll_fn(|cx| {
            product.poll_create(cx, &mut transition, &mut execution)
        }));
        assert!(waiting.as_mut().poll(&mut cx).is_pending());
        drop(waiting);
        assert_eq!(effects.events(), expected);
        assert!(transition.result().is_none());
    }
    assert!(
        product
            .poll_create(&mut cx, &mut transition, &mut execution)
            .is_ready()
    );
    assert!(matches!(
        transition.result(),
        Some(Ok(MvProductResult::Created(_)))
    ));
    let expected = vec!["stage", "publish", "install", "register"];
    assert_eq!(effects.events(), expected);
    assert!(
        product
            .poll_create(&mut cx, &mut transition, &mut execution)
            .is_ready()
    );
    assert_eq!(effects.events(), expected);
}

#[test]
fn create_foreign_stage_unwind_cannot_replay_or_advance_publication() {
    let product = MvKeyedProductService::default();
    let effects = CreateEffects::default();
    let mut execution = SuspendedCreate {
        effects: &effects,
        yielded: [true; 5],
        panic_stage: true,
        publish_noop: false,
    };
    let mut transition = super::MvCreateTransition::new(operation(), create_command());
    let mut cx = Context::from_waker(std::task::Waker::noop());
    let original = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        product.poll_create(&mut cx, &mut transition, &mut execution)
    }));
    assert!(original.is_err());
    assert!(transition.result().is_none());
    assert!(
        product
            .poll_create(&mut cx, &mut transition, &mut execution)
            .is_pending()
    );
    assert_eq!(effects.events(), vec!["stage-unwind"]);
}

struct SuspendedDrop<'a> {
    effects: &'a CreateEffects,
    yielded: [bool; 4],
    guard: Option<EffectOrderDropGuard>,
}
impl SuspendedDrop<'_> {
    fn yield_once(&mut self, index: usize, cx: &mut Context<'_>) -> bool {
        if self.yielded[index] {
            return false;
        }
        self.yielded[index] = true;
        cx.waker().wake_by_ref();
        true
    }
}
impl crate::ports::MvDropProgressPort<EffectOrderDropGuard> for SuspendedDrop<'_> {
    fn poll_prepare_drop(
        &mut self,
        cx: &mut Context<'_>,
        _operation: MvOperationContext,
        target: &MvTarget,
        _if_exists: bool,
    ) -> Poll<Result<MvDropReadiness<EffectOrderDropGuard>, MvProviderFailure>> {
        if self.yield_once(0, cx) {
            return Poll::Pending;
        }
        self.effects.record("prepare_drop");
        Poll::Ready(Ok(if self.effects.drop_absent {
            MvDropReadiness::AlreadyAbsent
        } else {
            MvDropReadiness::ReadyToDrop(EffectOrderDropGuard(target.clone()))
        }))
    }
    fn poll_drop_target(
        &mut self,
        cx: &mut Context<'_>,
        _operation: MvOperationContext,
        _target: &MvTarget,
    ) -> Poll<Result<(), MvProviderFailure>> {
        if self.yield_once(1, cx) {
            return Poll::Pending;
        }
        self.effects.record("drop");
        Poll::Ready(Ok(()))
    }
    fn poll_delete_after_provider_drop(
        &mut self,
        cx: &mut Context<'_>,
        _operation: MvOperationContext,
        guard: &mut Option<EffectOrderDropGuard>,
    ) -> Poll<Result<(), MvProviderFailure>> {
        if self.guard.is_none() {
            self.guard = guard.take();
        }
        if self.yield_once(2, cx) {
            return Poll::Pending;
        }
        let guard = self
            .guard
            .take()
            .expect("same DROP guard in original Source");
        assert_eq!(guard.0, create_command().target);
        self.effects.record("delete_projection");
        Poll::Ready(Ok(()))
    }
    fn poll_unregister_target(
        &mut self,
        cx: &mut Context<'_>,
        _operation: MvOperationContext,
        _target: &MvTarget,
    ) -> Poll<Result<(), MvProviderFailure>> {
        if self.yield_once(3, cx) {
            return Poll::Pending;
        }
        self.effects.record("unregister");
        Poll::Ready(Ok(()))
    }
}
#[test]
fn borrowed_drop_wait_drop_preserves_exact_guard_and_provider_before_projection() {
    let product = MvKeyedProductService::default();
    let effects = CreateEffects::default();
    let mut execution = SuspendedDrop {
        effects: &effects,
        yielded: [false; 4],
        guard: None,
    };
    let mut transition = super::MvDropTransition::new(operation(), create_command().target, false);
    let mut cx = Context::from_waker(std::task::Waker::noop());
    for (index, expected) in [
        vec![],
        vec!["prepare_drop"],
        vec!["prepare_drop", "drop"],
        vec!["prepare_drop", "drop", "delete_projection"],
    ]
    .into_iter()
    .enumerate()
    {
        let mut waiting = Box::pin(std::future::poll_fn(|cx| {
            product.poll_drop(cx, &mut transition, &mut execution)
        }));
        assert!(waiting.as_mut().poll(&mut cx).is_pending());
        drop(waiting);
        assert_eq!(effects.events(), expected);
        assert!(transition.result().is_none());
        if index == 2 {
            assert!(execution.guard.is_some());
        }
    }
    assert!(
        product
            .poll_drop(&mut cx, &mut transition, &mut execution)
            .is_ready()
    );
    assert!(matches!(
        transition.result(),
        Some(Ok(MvProductResult::Dropped))
    ));
    assert!(execution.guard.is_none());
    assert_eq!(
        effects.events(),
        vec!["prepare_drop", "drop", "delete_projection", "unregister"]
    );
}

#[test]
fn create_noop_preserves_the_verdict_without_projection_registration_or_abort() {
    for if_not_exists in [false, true] {
        let product = MvKeyedProductService::default();
        let effects = CreateEffects::default();
        let mut execution = SuspendedCreate {
            effects: &effects,
            yielded: [false; 5],
            panic_stage: false,
            publish_noop: true,
        };
        let mut command = create_command();
        command.if_not_exists = if_not_exists;
        let mut transition = super::MvCreateTransition::new(operation(), command);
        let mut cx = Context::from_waker(std::task::Waker::noop());
        while product
            .poll_create(&mut cx, &mut transition, &mut execution)
            .is_pending()
        {}
        if if_not_exists {
            assert!(matches!(
                transition.result(),
                Some(Ok(MvProductResult::Acknowledged))
            ));
        } else {
            assert!(
                matches!(transition.result(), Some(Err(error)) if error.kind() == MvProductErrorKind::Corruption)
            );
        }
        assert_eq!(effects.events(), vec!["stage", "publish-noop"]);
        assert!(
            product
                .poll_create(&mut cx, &mut transition, &mut execution)
                .is_ready()
        );
        assert_eq!(effects.events(), vec!["stage", "publish-noop"]);
    }
}
