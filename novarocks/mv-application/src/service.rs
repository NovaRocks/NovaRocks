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

//! Product-owned process lifecycle for materialized-view activity and workers.

use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::activity::{
    MvActivityAdmissionError, MvActivityGate, MvActivityLease, MvActivityOwner, MvActivityTicket,
};
use crate::ports::{
    MvCreateCatalogRegistrationPort, MvCreateProviderPort, MvDropCatalogRegistrationPort,
    MvDropProjectionPort, MvDropProviderPort, MvProviderFailure, MvRefreshExecutionPort,
};
use crate::process_runtime::{
    MvBackgroundRuntimeLifecycleError, MvBackgroundRuntimeOwner, MvBackgroundRuntimeStart,
    ProcessRuntime,
};
use crate::product::{
    MvCommand, MvCreateCommand, MvOperationContext, MvProductError, MvProductErrorKind,
    MvProductResult, MvRefreshAttemptIdentity, MvStagedTarget, MvTarget,
};
use crate::publication::MvRefreshPublicationFinalizationFacts;
use crate::readiness::{MvDropReadiness, MvReadinessService, MvRuntimePublicationLease};
use crate::repository::{MvRepository, MvRepositoryError, MvRepositoryErrorKind};
use crate::scheduler::{MvRefreshScheduler, MvSchedulerConfig};

/// The one process-local MV owner for activity admission and background-worker
/// lifecycle. Hosts may inject effect callbacks, but cannot own a parallel
/// gate, shutdown state, or worker supervisor.
pub struct MvProductService {
    activity_gate: MvActivityGate,
    background: MvBackgroundRuntimeOwner,
    scheduler_config: MvSchedulerConfig,
    scheduler: Mutex<MvRefreshScheduler>,
    readiness: Option<MvReadinessService>,
    management: Option<Arc<crate::management::ManagementEntrance>>,
    continuation: Option<Arc<crate::management::ManagementContinuationService>>,
}

impl Default for MvProductService {
    fn default() -> Self {
        Self::new(MvSchedulerConfig::default())
    }
}

impl MvProductService {
    /// Construct the one product owner for this process' MV scheduler state.
    /// The host may use the tick interval to arrange a wakeup, but the queue,
    /// source revisions, retry ledger and terminal transitions remain here.
    pub fn new(scheduler_config: MvSchedulerConfig) -> Self {
        Self {
            activity_gate: MvActivityGate::default(),
            background: MvBackgroundRuntimeOwner::default(),
            scheduler: Mutex::new(MvRefreshScheduler::new(scheduler_config.clone())),
            scheduler_config,
            readiness: None,
            management: None,
            continuation: None,
        }
    }

    /// Construct the process product with its product-owned readiness runtime.
    /// Unit-only products may omit this dependency, but a serving composition
    /// must use this constructor so refresh publication admission cannot be
    /// started by an outer adapter.
    pub fn new_with_readiness(
        scheduler_config: MvSchedulerConfig,
        readiness: MvReadinessService,
    ) -> Self {
        let mut service = Self::new(scheduler_config);
        service.readiness = Some(readiness);
        service
    }

    /// Construct the serving product together with its sole readiness state.
    ///
    /// Role adapters may receive a clone through [`Self::readiness_service`],
    /// but they cannot construct a second repository/runtime pair.
    pub fn new_with_readiness_runtime(
        scheduler_config: MvSchedulerConfig,
        repository: Arc<dyn MvRepository>,
        runtime: Arc<ProcessRuntime<MvTarget, novarocks_spi::connector::LakePublicationId>>,
    ) -> Self {
        Self::new_with_readiness(
            scheduler_config,
            MvReadinessService::new(repository, runtime),
        )
    }

    /// Compose document-management authority into the same process product.
    pub fn new_with_management_readiness_runtime(
        scheduler_config: MvSchedulerConfig,
        repository: Arc<dyn MvRepository>,
        runtime: Arc<ProcessRuntime<MvTarget, novarocks_spi::connector::LakePublicationId>>,
        management: Arc<crate::management::ManagementEntrance>,
    ) -> Self {
        Self::new_with_management_continuation(
            scheduler_config,
            repository,
            runtime,
            management,
            crate::management::RemoteEffectPolicy::default(),
        )
    }

    /// Compose the serving product with its management authority and the
    /// remote-effect guarantees this deployment actually has.
    ///
    /// The continuation service is created here because there must be exactly
    /// one per process: the challenges it issues are only meaningful against
    /// the single entrance's state, and a second evaluator would let a stale
    /// declaration be replayed through it.
    pub fn new_with_management_continuation(
        scheduler_config: MvSchedulerConfig,
        repository: Arc<dyn MvRepository>,
        runtime: Arc<ProcessRuntime<MvTarget, novarocks_spi::connector::LakePublicationId>>,
        management: Arc<crate::management::ManagementEntrance>,
        remote_effect_policy: crate::management::RemoteEffectPolicy,
    ) -> Self {
        let mut service = Self::new_with_readiness_runtime(scheduler_config, repository, runtime);
        service.continuation = Some(Arc::new(
            crate::management::ManagementContinuationService::new(
                management.as_ref().clone(),
                remote_effect_policy,
            ),
        ));
        service.management = Some(management);
        service
    }

    /// Borrow the product-owned authority for exact provider-port adaptation.
    pub fn management_entrance(&self) -> Option<Arc<crate::management::ManagementEntrance>> {
        self.management.clone()
    }

    /// The single process-local owner of management continuation. Absent on
    /// unit-only products, which have no management authority to continue.
    pub fn management_continuation(
        &self,
    ) -> Option<Arc<crate::management::ManagementContinuationService>> {
        self.continuation.clone()
    }

    /// Return the serving product's readiness adapter input.
    ///
    /// Unit-only products deliberately have no readiness state. A production
    /// composition must construct the product through
    /// [`Self::new_with_readiness_runtime`].
    pub fn readiness_service(&self) -> Option<MvReadinessService> {
        self.readiness.clone()
    }

    pub fn scheduler_tick_interval_ms(&self) -> u64 {
        self.scheduler_config.tick_interval_ms()
    }

    /// Run one provider-observation adapter transition against the single
    /// product scheduler. The callback receives no clone or handle that could
    /// retain scheduler state outside this process product owner.
    pub fn with_refresh_scheduler<T>(
        &self,
        operation: impl FnOnce(&mut MvRefreshScheduler) -> T,
    ) -> T {
        let mut scheduler = self
            .scheduler
            .lock()
            .expect("MV product scheduler state lock poisoned");
        operation(&mut scheduler)
    }

    /// Reserve the one immutable identity for a product refresh publication.
    /// Query and provider adapters may derive their wire values from it but
    /// cannot mint a second identity for the same product transition.
    pub fn reserve_refresh_attempt(&self) -> MvRefreshAttemptIdentity {
        MvRefreshAttemptIdentity::reserve()
    }

    /// Begin the product-owned process publication lifetime before any
    /// provider effect. The returned lease releases the exact target and
    /// publication identity when the refresh transition unwinds.
    pub fn begin_refresh_publication(
        &self,
        target: &MvTarget,
        attempt: &MvRefreshAttemptIdentity,
    ) -> Result<MvRuntimePublicationLease, MvProductError> {
        let readiness = self.readiness.as_ref().ok_or_else(|| {
            MvProductError::new(
                MvProductErrorKind::Unavailable,
                "MV product has no configured readiness runtime",
            )
        })?;
        readiness
            .begin_publication(target.clone(), attempt.publication_id)
            .map_err(readiness_error)
    }

    /// Execute and finalize one product-owned refresh transition. The outer
    /// port is consumed once, so it cannot retain a parallel lifecycle or
    /// replay a publication after the product has classified its outcome.
    pub fn execute_refresh<'a>(
        &self,
        target: &MvTarget,
        attempt: &MvRefreshAttemptIdentity,
        execution: Box<dyn MvRefreshExecutionPort + 'a>,
    ) -> Result<MvProductResult, MvProductError> {
        let _publication = self.begin_refresh_publication(target, attempt)?;
        let known_committed = execution
            .execute_refresh(target, attempt)
            .map_err(MvProviderFailure::into_product_error)?;
        validate_published_refresh(target, attempt, known_committed.finalization_facts())?;
        known_committed
            .project_known_committed(target)
            .map_err(known_committed_finalize_failure)?;
        Ok(MvProductResult::Acknowledged)
    }

    /// Execute the product-owned CREATE state machine. The outer adapter owns
    /// SQL lowering and provider capabilities, while this product owns the
    /// effect order and the distinction between pre-commit cleanup and a
    /// known-committed finalization failure.
    pub fn create(
        &self,
        operation: MvOperationContext,
        create: MvCreateCommand,
        provider: &dyn MvCreateProviderPort,
        catalog_registration: &dyn MvCreateCatalogRegistrationPort,
    ) -> Result<MvProductResult, MvProductError> {
        let command = MvCommand::Create(create);
        let target = match &command {
            MvCommand::Create(create) => create.target.clone(),
            MvCommand::Drop { .. } | MvCommand::Refresh { .. } | MvCommand::Show { .. } => {
                unreachable!("CREATE product path constructed a non-CREATE command")
            }
        };
        // Staging has no catalog-visible effect, so a failure here ends the
        // statement with nothing to compensate.
        let staged = provider
            .stage_target(operation, &command)
            .map_err(MvProviderFailure::into_product_error)?;
        if staged.target != target {
            return Err(MvProductError::new(
                MvProductErrorKind::InvalidRequest,
                "MV CREATE provider staged a different target than the command named",
            ));
        }
        // The one atomic point: the target and its canonical documents become
        // visible together.
        let created = match provider.publish_staged_target(operation, &staged) {
            Ok(created) => created,
            Err(primary) => {
                return Err(settle_unpublished_stage(
                    operation, provider, &staged, primary,
                ));
            }
        };
        if created.target != target {
            return Err(MvProductError::new(
                MvProductErrorKind::Corruption,
                "MV CREATE publish reported a different target than it staged",
            ));
        }
        // Everything below is post-commit. The create is done and must not be
        // undone by a finalization failure.
        provider
            .install_created_projection(operation, &created)
            .map_err(known_committed_finalize_failure)?;
        catalog_registration
            .register_target(operation, &created)
            .map_err(known_committed_finalize_failure)?;
        Ok(MvProductResult::Created(created))
    }

    /// Execute the product-owned DROP state machine. The projection port
    /// supplies only exact durable facts; provider and catalog ports carry the
    /// outer effects without owning their order or terminal interpretation.
    pub fn drop(
        &self,
        operation: MvOperationContext,
        target: MvTarget,
        if_exists: bool,
        projection: &dyn MvDropProjectionPort,
        provider: &dyn MvDropProviderPort,
        catalog_registration: &dyn MvDropCatalogRegistrationPort,
    ) -> Result<MvProductResult, MvProductError> {
        match projection
            .prepare_drop(operation, &target, if_exists)
            .map_err(MvProviderFailure::into_product_error)?
        {
            MvDropReadiness::AlreadyAbsent => Ok(MvProductResult::Acknowledged),
            MvDropReadiness::ReadyToDrop(guard) => {
                provider
                    .drop_target(operation, &target)
                    .map_err(MvProviderFailure::into_product_error)?;
                projection
                    .delete_after_provider_drop(operation, guard)
                    .map_err(known_committed_finalize_failure)?;
                catalog_registration
                    .unregister_target(operation, &target)
                    .map_err(known_committed_finalize_failure)?;
                Ok(MvProductResult::Dropped)
            }
        }
    }

    pub fn acquire_foreground(
        &self,
        target: MvTarget,
        owner: MvActivityOwner,
        cancelled: impl Fn() -> bool,
    ) -> Result<MvActivityLease, MvActivityAdmissionError> {
        self.activity_gate
            .acquire_foreground(target, owner, cancelled)
    }

    /// Register background work with the shared product FIFO gate. The host
    /// retains only its effect callback and must release the returned ticket or
    /// lease; the product retains the state machine itself.
    pub fn request_activity(
        &self,
        target: MvTarget,
        owner: MvActivityOwner,
    ) -> Result<MvActivityTicket, crate::activity::MvActivityGateError> {
        self.activity_gate.request(target, owner)
    }

    pub fn begin_background_start(
        &self,
    ) -> Result<MvBackgroundRuntimeStart<'_>, MvBackgroundRuntimeLifecycleError> {
        self.background.begin_start()
    }

    pub fn begin_stopping(&self) {
        self.activity_gate.begin_stopping();
    }

    pub async fn shutdown_background_workers_until(&self, deadline: Instant) -> Result<(), String> {
        self.background.shutdown_until(deadline).await
    }

    pub fn request_background_stop_for_process_exit(&self) {
        self.background.request_stop_for_process_exit();
    }
}

/// Settle a publish that did not report a committed target.
///
/// A staged target is discarded only when the publish proved it never became
/// visible. An unknown publication keeps its stage: aborting it could delete
/// the objects of a create that actually succeeded, and re-running it could
/// publish a second time. That case stays unsettled for the management owner
/// to adjudicate against the provider.
fn settle_unpublished_stage(
    operation: MvOperationContext,
    provider: &dyn MvCreateProviderPort,
    staged: &MvStagedTarget,
    primary: MvProviderFailure,
) -> MvProductError {
    let primary = primary.into_product_error();
    if matches!(
        primary.kind(),
        MvProductErrorKind::CommitUnknown | MvProductErrorKind::KnownCommittedFinalizeFailed
    ) {
        return primary;
    }
    match provider.abort_staged_target(operation, staged) {
        Ok(()) => primary,
        Err(abort) => MvProductError::new(
            primary.kind(),
            format!("{}; staged target abort failed: {abort}", primary.message()),
        )
        .with_compile_control(primary.compile_control_error()),
    }
}

fn known_committed_finalize_failure(failure: MvProviderFailure) -> MvProductError {
    MvProductError::new(
        MvProductErrorKind::KnownCommittedFinalizeFailed,
        failure.message(),
    )
    .with_compile_control(failure.compile_control_error())
}

fn validate_published_refresh(
    target: &MvTarget,
    attempt: &MvRefreshAttemptIdentity,
    published: &MvRefreshPublicationFinalizationFacts,
) -> Result<(), MvProductError> {
    let intent = published.intent();
    if intent.publication_id() != attempt.publication_id {
        return Err(MvProductError::new(
            MvProductErrorKind::Corruption,
            "MV known-committed refresh facts do not match the reserved publication identity",
        ));
    }
    if target.catalog() != Some(intent.target_catalog())
        || target.namespace() != intent.target_namespace()
        || target.name() != intent.target_name()
    {
        return Err(MvProductError::new(
            MvProductErrorKind::Corruption,
            "MV known-committed refresh facts do not match the product target",
        ));
    }
    Ok(())
}

fn readiness_error(error: MvRepositoryError) -> MvProductError {
    let kind = match error.kind() {
        MvRepositoryErrorKind::InvalidRequest => MvProductErrorKind::InvalidRequest,
        MvRepositoryErrorKind::NotFound => MvProductErrorKind::TargetReplaced,
        MvRepositoryErrorKind::Conflict => MvProductErrorKind::Conflict,
        MvRepositoryErrorKind::Corruption => MvProductErrorKind::Corruption,
        MvRepositoryErrorKind::Unavailable => MvProductErrorKind::Unavailable,
        MvRepositoryErrorKind::CommitUnknown => MvProductErrorKind::CommitUnknown,
    };
    MvProductError::new(kind, error.to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::MvProductService;
    use crate::activity::{CanonicalMvTarget, MvActivityGateError, MvActivityOwner};
    use crate::persistence::{
        definition::{CreateMvDefinitionRequest, MvDesiredRefreshPolicy},
        descriptor::MvDescriptorV3,
        schema::{
            BaseContract, BaseSchemaSnapshot, HiddenApplyKeyContract, MvSchemaContract,
            OutputContract, TargetContract,
        },
        semantic::MvRefreshDesiredConfiguration,
    };
    use crate::ports::{
        MvCreateCatalogRegistrationPort, MvCreateProviderPort, MvDropCatalogRegistrationPort,
        MvDropProjectionPort, MvDropProviderPort, MvProviderFailure, MvProviderFailureKind,
        MvRefreshExecutionPort, MvRefreshKnownCommittedPort,
    };
    use crate::process_runtime::ProcessRuntime;
    use crate::product::{
        MvCommand, MvCreateCommand, MvCreatedTarget, MvOperationContext, MvProductErrorKind,
        MvProductResult, MvStagedTarget, MvTarget,
    };
    use crate::publication::{
        MvRefreshPublicationBase, MvRefreshPublicationFinalizationFacts,
        MvRefreshPublicationIntent, MvRefreshPublicationTechnique,
    };
    use crate::readiness::{MvDropReadiness, MvReadinessService};
    use crate::repository::InitialMvRefreshConfiguration;
    use crate::scheduler::MvSchedulerConfig;
    use crate::test_repository::InMemoryMvRepository;
    use bytes::Bytes;
    use novarocks_query_application::persisted_query_definition::{
        PersistedQueryDefinition, PersistedQueryDialect,
    };
    use novarocks_spi::connector::{
        ConnectorCommittedVersion, ConnectorTableObjectId, LakePublicationId,
    };
    use novarocks_sql::planning::mv::ApplyKeySource;
    use uuid::Uuid;

    #[derive(Default)]
    struct CreateEffects {
        events: Mutex<Vec<&'static str>>,
        fail_stage: bool,
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

    impl MvDropProviderPort for CreateEffects {
        fn drop_target(
            &self,
            _operation: MvOperationContext,
            _target: &MvTarget,
        ) -> Result<(), MvProviderFailure> {
            self.record("drop");
            Ok(())
        }
    }

    impl MvDropCatalogRegistrationPort for CreateEffects {
        fn unregister_target(
            &self,
            _operation: MvOperationContext,
            _target: &MvTarget,
        ) -> Result<(), MvProviderFailure> {
            self.record("unregister");
            Ok(())
        }
    }

    struct RefreshExecutionEffects {
        effects: Arc<CreateEffects>,
        published: MvRefreshPublicationFinalizationFacts,
    }

    struct KnownCommittedEffects {
        effects: Arc<CreateEffects>,
        published: MvRefreshPublicationFinalizationFacts,
    }

    impl MvRefreshExecutionPort for RefreshExecutionEffects {
        fn execute_refresh(
            self: Box<Self>,
            _target: &MvTarget,
            _attempt: &crate::product::MvRefreshAttemptIdentity,
        ) -> Result<Box<dyn MvRefreshKnownCommittedPort>, MvProviderFailure> {
            Ok(Box::new(KnownCommittedEffects {
                effects: self.effects,
                published: self.published,
            }))
        }
    }

    impl MvRefreshKnownCommittedPort for KnownCommittedEffects {
        fn finalization_facts(&self) -> &MvRefreshPublicationFinalizationFacts {
            &self.published
        }

        fn project_known_committed(
            self: Box<Self>,
            _target: &MvTarget,
        ) -> Result<(), MvProviderFailure> {
            self.effects.record("project_known_committed");
            if self.effects.fail_known_committed_projection {
                return Err(MvProviderFailure::new(
                    MvProviderFailureKind::Unavailable,
                    "known-committed projection failed",
                ));
            }
            Ok(())
        }
    }

    fn finalization_facts(
        target: &MvTarget,
        attempt: &crate::product::MvRefreshAttemptIdentity,
    ) -> MvRefreshPublicationFinalizationFacts {
        let target_object_id =
            ConnectorTableObjectId::try_new(Bytes::from_static(b"mv-target-object"))
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
            ConnectorCommittedVersion::try_new(
                Bytes::from_static(b"mv-publication-version"),
                Some(42),
            )
            .expect("publication version"),
        )
        .expect("published facts")
    }

    fn service_with_refresh_readiness() -> MvProductService {
        MvProductService::new_with_readiness(
            MvSchedulerConfig::default(),
            MvReadinessService::new(
                Arc::new(InMemoryMvRepository::default()),
                Arc::<ProcessRuntime<MvTarget, LakePublicationId>>::default(),
            ),
        )
    }

    impl MvDropProjectionPort for CreateEffects {
        fn prepare_drop(
            &self,
            _operation: MvOperationContext,
            target: &MvTarget,
            _if_exists: bool,
        ) -> Result<MvDropReadiness, MvProviderFailure> {
            self.record("prepare_drop");
            Ok(if self.drop_absent {
                MvDropReadiness::AlreadyAbsent
            } else {
                MvDropReadiness::ReadyToDrop(crate::readiness::MvProjectionDeleteGuard::for_test(
                    target.clone(),
                ))
            })
        }

        fn delete_after_provider_drop(
            &self,
            _operation: MvOperationContext,
            _guard: crate::readiness::MvProjectionDeleteGuard,
        ) -> Result<(), MvProviderFailure> {
            self.record("delete_projection");
            Ok(())
        }
    }

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

    fn test_descriptor() -> MvDescriptorV3 {
        MvDescriptorV3 {
            descriptor_version: 3,
            package_id: "test.mv".to_string(),
            query_definition: PersistedQueryDefinition::new(
                "SELECT 1",
                PersistedQueryDialect::StarRocks,
                "iceberg",
                "db",
            )
            .expect("valid persisted query"),
            visible_columns: Vec::new(),
            hidden_columns: Vec::new(),
            base_dependencies: Vec::new(),
            primary_key_columns: Vec::new(),
            schema_contract: MvSchemaContract {
                contract_version: 1,
                base: BaseContract {
                    table_fqn: "iceberg.db.base".to_string(),
                    table_object_id: ConnectorTableObjectId::try_new(Bytes::from_static(b"base"))
                        .expect("valid object ID"),
                    alias_at_create: None,
                    schema_id_at_create: 0,
                    schema_at_create: BaseSchemaSnapshot { fields: Vec::new() },
                },
                bases: Vec::new(),
                output: OutputContract {
                    columns: Vec::new(),
                    filter: None,
                },
                join: None,
                aggregate: None,
                branch: None,
                target: TargetContract {
                    table_fqn: "iceberg.db.mv".to_string(),
                    table_uuid: "created-table".to_string(),
                    schema_id_at_create: 0,
                    visible_columns: Vec::new(),
                    hidden_apply_key: HiddenApplyKeyContract {
                        column_name: "__nova_base_row_id".to_string(),
                        target_field_id: 1,
                        source: ApplyKeySource::BaseRowId,
                    },
                    partition: None,
                },
            },
            refresh: MvRefreshDesiredConfiguration::new(
                MvDesiredRefreshPolicy::Manual,
                false,
                None,
                None,
            )
            .expect("manual refresh configuration"),
            created_at_ms: 1,
        }
    }

    #[test]
    fn stopping_product_service_rejects_new_background_activity() {
        let service = MvProductService::default();
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
        let service = MvProductService::new(MvSchedulerConfig::new(true, 73, 2, 11, 99));

        assert_eq!(service.scheduler_tick_interval_ms(), 73);
        assert!(service.with_refresh_scheduler(|scheduler| scheduler.enabled()));
    }

    #[test]
    fn serving_product_retains_the_exact_management_authority() {
        let management = Arc::new(crate::management::ManagementEntrance::new(
            crate::management::DeploymentOwner::parse("test-deployment").expect("owner"),
            crate::management::ProcessIncarnation::parse("test-process").expect("incarnation"),
        ));
        let service = MvProductService::new_with_management_readiness_runtime(
            MvSchedulerConfig::default(),
            Arc::new(InMemoryMvRepository::default()),
            Arc::<ProcessRuntime<MvTarget, LakePublicationId>>::default(),
            Arc::clone(&management),
        );

        assert!(Arc::ptr_eq(
            &service.management_entrance().expect("serving authority"),
            &management,
        ));
        assert!(service.readiness_service().is_some());
    }

    #[test]
    fn create_runs_product_effects_in_required_order() {
        let service = MvProductService::default();
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

    #[test]
    fn known_committed_refresh_projection_is_product_finalization() {
        let service = service_with_refresh_readiness();
        let effects = Arc::new(CreateEffects::default());
        let target = MvTarget::from_parts(Some("iceberg"), "db", "mv");
        let attempt = service.reserve_refresh_attempt();
        let published = finalization_facts(&target, &attempt);

        let result = service
            .execute_refresh(
                &target,
                &attempt,
                Box::new(RefreshExecutionEffects {
                    effects: Arc::clone(&effects),
                    published,
                }),
            )
            .expect("projection succeeds");

        assert!(matches!(result, MvProductResult::Acknowledged));
        assert_eq!(effects.events(), ["project_known_committed"]);
    }

    #[test]
    fn known_committed_refresh_rejects_a_different_publication_before_projection() {
        let service = service_with_refresh_readiness();
        let effects = Arc::new(CreateEffects::default());
        let target = MvTarget::from_parts(Some("iceberg"), "db", "mv");
        let attempt = service.reserve_refresh_attempt();
        let other_attempt = service.reserve_refresh_attempt();
        let published = finalization_facts(&target, &other_attempt);

        let error = service
            .execute_refresh(
                &target,
                &attempt,
                Box::new(RefreshExecutionEffects {
                    effects: Arc::clone(&effects),
                    published,
                }),
            )
            .expect_err("different publication identity must be rejected");

        assert_eq!(error.kind(), MvProductErrorKind::Corruption);
        assert!(effects.events().is_empty());
    }

    #[test]
    fn product_owns_refresh_publication_admission_and_lease_release() {
        let readiness = MvReadinessService::new(
            Arc::new(InMemoryMvRepository::default()),
            Arc::<ProcessRuntime<MvTarget, LakePublicationId>>::default(),
        );
        let service = MvProductService::new_with_readiness(MvSchedulerConfig::default(), readiness);
        let target = MvTarget::from_parts(Some("iceberg"), "db", "mv");
        let first = service.reserve_refresh_attempt();
        let lease = service
            .begin_refresh_publication(&target, &first)
            .expect("product begins the first publication");
        let second = service.reserve_refresh_attempt();

        let Err(error) = service.begin_refresh_publication(&target, &second) else {
            panic!("a concurrent publication must be rejected");
        };
        assert_eq!(error.kind(), MvProductErrorKind::Conflict);

        drop(lease);
        service
            .begin_refresh_publication(&target, &second)
            .expect("dropping the product lease releases the target");
    }

    #[test]
    fn known_committed_refresh_keeps_external_commit_when_projection_fails() {
        let service = service_with_refresh_readiness();
        let effects = Arc::new(CreateEffects {
            fail_known_committed_projection: true,
            ..Default::default()
        });
        let target = MvTarget::from_parts(Some("iceberg"), "db", "mv");
        let attempt = service.reserve_refresh_attempt();
        let published = finalization_facts(&target, &attempt);

        let error = service
            .execute_refresh(
                &target,
                &attempt,
                Box::new(RefreshExecutionEffects {
                    effects: Arc::clone(&effects),
                    published,
                }),
            )
            .expect_err("projection finalization fails");

        assert_eq!(
            error.kind(),
            MvProductErrorKind::KnownCommittedFinalizeFailed
        );
        assert_eq!(effects.events(), ["project_known_committed"]);
    }

    #[test]
    fn a_failed_stage_leaves_nothing_to_compensate() {
        let service = MvProductService::default();
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
        let service = MvProductService::default();
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
        let service = MvProductService::default();
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
        let service = MvProductService::default();
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
        let service = MvProductService::default();
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
        let service = MvProductService::default();
        let effects = CreateEffects::default();
        let target = MvTarget::from_parts(Some("iceberg"), "db", "mv");

        let result = service
            .drop(operation(), target, false, &effects, &effects, &effects)
            .expect("drop succeeds");

        assert!(matches!(result, MvProductResult::Dropped));
        assert_eq!(
            effects.events(),
            ["prepare_drop", "drop", "delete_projection", "unregister"]
        );
    }

    #[test]
    fn drop_if_exists_absence_emits_no_external_effect() {
        let service = MvProductService::default();
        let effects = CreateEffects {
            drop_absent: true,
            ..Default::default()
        };
        let target = MvTarget::from_parts(Some("iceberg"), "db", "missing_mv");

        let result = service
            .drop(operation(), target, true, &effects, &effects, &effects)
            .expect("missing IF EXISTS target is acknowledged");

        assert!(matches!(result, MvProductResult::Acknowledged));
        assert_eq!(effects.events(), ["prepare_drop"]);
    }
}
