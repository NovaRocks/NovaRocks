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

//! Synchronous frontend runtime helper for automatic MV maintenance.
//!
//! This is deliberately a small host-facing loop rather than another Core
//! coordinator.  It inventories frontend MV definitions, asks the typed Core
//! background port for provider-neutral facts, then consumes a maintenance
//! permit in [`MaintenanceCoordinator`]. Each action acquires the shared
//! management entrance from a fresh exact provider observation. The runner
//! only invokes the durable table-maintenance service routes.
//!
//! Automatic optimize uses the table-maintenance service's synchronous durable
//! lifecycle route.  That route must claim, execute, and terminally persist a
//! job before this worker settles its management effect and permit.

use novarocks_spi::connector::ConnectorControlRegistry;
use novarocks_workload_control::{
    QueryConcurrencyPermit, RootAdmissionHandle, WorkClass, WorkOwner, WorkRequest,
};
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::background::MvBackgroundEngine;
use crate::mv::domain::readiness::MvReadinessPort;
use crate::query_execution::maintenance::{
    AutomaticMaintenanceContext, TableMaintenanceEngine, TableMaintenanceService,
};
use novarocks_mv_application::persistence::projection::StoredMvProjection;
use novarocks_mv_application::repository::MvRepositoryError;
use novarocks_mv_application::{
    maintenance::{
        AutomaticMaintenanceRunner, MaintenanceAdmission, MaintenanceCoordinator,
        MaintenanceCoordinatorConfig, MaintenanceExecutionReport, MvBackgroundEngineError,
        MvBackgroundEngineErrorKind, MvMaintenanceRuntime, OptimizeDurableOutcome,
    },
    service::MvProductService,
};
use novarocks_table_maintenance::{
    MaintenanceActionOutcome, MaintenanceActionRequest, MaintenanceTarget,
};

/// Dependencies bound by the frontend host after Core has completed restore,
/// recovery, provider binding and table-maintenance recovery.
#[derive(Clone)]
pub(crate) struct FrontendMaintenanceWorkerDependencies {
    pub(crate) readiness: Arc<MvReadinessPort>,
    pub(crate) background_engine: Arc<dyn MvBackgroundEngine>,
    pub(crate) table_maintenance_engine: Arc<dyn TableMaintenanceEngine>,
    pub(crate) table_maintenance_service: Arc<dyn TableMaintenanceService>,
    pub(crate) connector_control: Arc<dyn ConnectorControlRegistry>,
    pub(crate) product_service: Arc<MvProductService>,
    pub(crate) root_admission: RootAdmissionHandle,
    pub(crate) coordinator_config: MaintenanceCoordinatorConfig,
    pub(crate) attempt_timeout: Duration,
    /// Captured at construction because the worker runs on bare threads that
    /// have no runtime of their own.
    pub(crate) runtime: tokio::runtime::Handle,
}

/// A maintenance pass is intentionally observable, including every complete
/// policy evaluation and durable action outcome.
#[derive(Clone, Debug, Default)]
pub(crate) struct FrontendMaintenancePassReport {
    pub(crate) attempts: Vec<FrontendMaintenanceAttemptReport>,
    pub(crate) skipped: Vec<FrontendMaintenanceSkip>,
}

#[derive(Clone, Debug)]
pub(crate) struct FrontendMaintenanceAttemptReport {
    #[allow(
        dead_code,
        reason = "Retained for staged materialized-view integration and recovery wiring."
    )]
    pub(crate) mv_id: i64,
    #[allow(
        dead_code,
        reason = "Retained for staged materialized-view integration and recovery wiring."
    )]
    pub(crate) target: MaintenanceTarget,
    #[allow(
        dead_code,
        reason = "Retained for staged materialized-view integration and recovery wiring."
    )]
    pub(crate) execution: MaintenanceExecutionReport,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum FrontendMaintenanceSkip {
    MissingCanonicalTarget {
        mv_id: i64,
    },
    FactsFailed {
        mv_id: i64,
        kind: MvBackgroundEngineErrorKind,
    },
    Stopping {
        mv_id: i64,
    },
    Admission {
        mv_id: i64,
        admission: MaintenanceAdmission,
    },
}

/// Frontend repository/provider/native adapter for automatic maintenance.
/// The MV product owns its coordinator, permits, cooldown and backoff state;
/// this adapter only inventories definitions and invokes durable effects.
pub(crate) struct FrontendMaintenanceWorker {
    dependencies: FrontendMaintenanceWorkerDependencies,
    runtime: MvMaintenanceRuntime,
}

impl FrontendMaintenanceWorker {
    pub(crate) fn new(dependencies: FrontendMaintenanceWorkerDependencies) -> Self {
        let mut coordinator_config = dependencies.coordinator_config.clone();
        if coordinator_config.enabled {
            tracing::warn!(
                "automatic MV maintenance is unavailable until every output snapshot preserves its canonical publication and document retention"
            );
            coordinator_config.enabled = false;
        }
        Self {
            runtime: MvMaintenanceRuntime::new(coordinator_config),
            dependencies,
        }
    }

    #[allow(
        dead_code,
        reason = "Retained for staged materialized-view integration and recovery wiring."
    )]
    pub(crate) fn config(&self) -> MaintenanceCoordinatorConfig {
        self.runtime.config()
    }

    /// Evaluate every current frontend MV definition once. Every durable
    /// action enters the management FIFO independently, so a configuration
    /// write can take a turn between maintenance actions.
    pub(crate) fn run_once(
        &self,
        now_ms: i64,
    ) -> Result<FrontendMaintenancePassReport, MvRepositoryError> {
        let definitions = self
            .dependencies
            .readiness
            .list_ready_projections()?
            .into_iter()
            .map(|loaded| loaded.projection)
            .collect::<Vec<_>>();
        // One frontend event loop owns the pass.  The coordinator still owns
        // admission policy, but no definition creates its own OS thread; each
        // terminal transition completes before the next event is admitted.
        let mut pass = FrontendMaintenancePassReport::default();
        for definition in definitions {
            self.run_definition(definition, now_ms, &mut pass);
        }
        Ok(pass)
    }

    fn run_definition(
        &self,
        definition: StoredMvProjection,
        now_ms: i64,
        pass: &mut FrontendMaintenancePassReport,
    ) {
        let target = match canonical_target(&definition) {
            Some(target) => target,
            None => {
                pass.skipped
                    .push(FrontendMaintenanceSkip::MissingCanonicalTarget {
                        mv_id: definition.mv_id,
                    });
                return;
            }
        };
        let facts = match self
            .dependencies
            .background_engine
            .maintenance_facts(&target)
        {
            Ok(facts) => facts,
            Err(error) => {
                pass.skipped.push(FrontendMaintenanceSkip::FactsFailed {
                    mv_id: definition.mv_id,
                    kind: error.kind(),
                });
                return;
            }
        };

        let AdmittedAutomaticQuery {
            owner,
            query_concurrency,
        } = match admit_automatic_query(
            &self.dependencies.root_admission,
            &self.dependencies.runtime,
        ) {
            Ok(work) => work,
            Err(_) => {
                pass.skipped.push(FrontendMaintenanceSkip::Stopping {
                    mv_id: definition.mv_id,
                });
                return;
            }
        };
        let cancellation = match owner.scope().cancellation() {
            Ok(view) => novarocks_query_application::cancellation::QueryCancellationView::governed(
                view, None,
            ),
            Err(_) => {
                finish_automatic_root(owner, query_concurrency);
                pass.skipped.push(FrontendMaintenanceSkip::Stopping {
                    mv_id: definition.mv_id,
                });
                return;
            }
        };
        if cancellation.is_cancelled() {
            finish_automatic_root(owner, query_concurrency);
            pass.skipped.push(FrontendMaintenanceSkip::Stopping {
                mv_id: definition.mv_id,
            });
            return;
        }

        let attempt = match self
            .runtime
            .try_begin(definition.mv_id, target.clone(), &facts, now_ms)
        {
            Ok(attempt) => attempt,
            Err(admission) => {
                finish_automatic_root(owner, query_concurrency);
                pass.skipped.push(FrontendMaintenanceSkip::Admission {
                    mv_id: definition.mv_id,
                    admission,
                });
                return;
            }
        };
        let mut runner = TableMaintenanceAutomaticRunner {
            engine: Arc::clone(&self.dependencies.table_maintenance_engine),
            service: Arc::clone(&self.dependencies.table_maintenance_service),
            connector_control: Arc::clone(&self.dependencies.connector_control),
            readiness: Arc::clone(&self.dependencies.readiness),
            product_service: Arc::clone(&self.dependencies.product_service),
            target: target.clone(),
            context: AutomaticMaintenanceContext::with_deadline(
                cancellation,
                Instant::now() + self.dependencies.attempt_timeout,
            ),
            handle: self.dependencies.runtime.clone(),
        };
        let execution = MaintenanceCoordinator::execute_attempt(&attempt, &mut runner);
        self.runtime.finish(attempt, &execution, now_ms);
        finish_automatic_root(owner, query_concurrency);
        pass.attempts.push(FrontendMaintenanceAttemptReport {
            mv_id: definition.mv_id,
            target,
            execution,
        });
    }
}

struct AdmittedAutomaticQuery {
    owner: WorkOwner,
    query_concurrency: QueryConcurrencyPermit,
}

fn admit_automatic_query(
    root_admission: &RootAdmissionHandle,
    runtime: &tokio::runtime::Handle,
) -> Result<AdmittedAutomaticQuery, novarocks_workload_control::WorkError> {
    let root =
        root_admission.begin_warehouse_root(WorkRequest::new(WorkClass::MaterializedView))?;
    let query_concurrency = runtime.block_on(async { root.owner.scope().admit_query()?.await })?;
    Ok(AdmittedAutomaticQuery {
        owner: root.owner,
        query_concurrency,
    })
}

fn finish_automatic_root(owner: WorkOwner, query_concurrency: QueryConcurrencyPermit) {
    drop(query_concurrency);
    owner.complete();
}

/// Narrow adapter from automatic policy actions to the frontend durable
/// table-maintenance service and the shared MV management entrance.
struct TableMaintenanceAutomaticRunner {
    engine: Arc<dyn TableMaintenanceEngine>,
    service: Arc<dyn TableMaintenanceService>,
    connector_control: Arc<dyn ConnectorControlRegistry>,
    readiness: Arc<MvReadinessPort>,
    product_service: Arc<MvProductService>,
    target: MaintenanceTarget,
    context: AutomaticMaintenanceContext,
    /// Captured where the worker thread is spawned, because the thread itself
    /// has no runtime of its own.
    handle: tokio::runtime::Handle,
}

struct PreparedAutomaticAction {
    entrance: Arc<novarocks_mv_application::management::ManagementEntrance>,
    management: novarocks_mv_application::management::ManagementEntranceLease,
    managed_target: novarocks_mv_application::management::ManagedMvTarget,
    catalog_handle: novarocks_spi::connector::CatalogHandle,
    product_target: novarocks_mv_application::product::MvTarget,
    operation_uuid: uuid::Uuid,
    scope: novarocks_mv_application::management::EffectScope,
    context: novarocks_spi::connector::ConnectorRequestContext,
    retained_statistics:
        Option<novarocks_mv_application::persistence::projection::MvOutputStatistics>,
}

impl TableMaintenanceAutomaticRunner {
    /// Capture a fresh exact document set for one action, enter the shared MV
    /// FIFO, then validate the same physical object and Current documents a
    /// second time. The old policy observation never grants write authority.
    fn prepare_managed_action(
        &self,
        scope: novarocks_mv_application::management::EffectScope,
    ) -> Result<PreparedAutomaticAction, MvBackgroundEngineError> {
        use novarocks_mv_application::management::{
            AutomaticMaintenanceEffect, EffectIdentity, ManagedMvTarget,
        };
        use novarocks_mv_application::persistence::projection::MvPublicationState;
        use novarocks_spi::connector::document_storage::{
            ConnectorDocumentObservationRequest, ConnectorDocumentStorageBudget,
            ConnectorDocumentStorageLimits,
        };
        use novarocks_spi::connector::{
            ConnectorControlResolver, ConnectorInstanceId, ConnectorTableIdentity,
            ConnectorTableObjectCaptureRequest, ConnectorTableObjectSelector,
            ConnectorTableResolution,
        };

        self.context.ensure_active().map_err(|error| {
            MvBackgroundEngineError::new(
                if self.context.is_cancelled() {
                    MvBackgroundEngineErrorKind::ShutdownCancelled
                } else {
                    MvBackgroundEngineErrorKind::TransientUnavailable
                },
                error,
            )
        })?;
        let product_target = novarocks_mv_application::product::MvTarget::from_parts(
            Some(&self.target.catalog),
            &self.target.namespace,
            &self.target.table,
        );
        let ready = self
            .readiness
            .load_ready(&novarocks_sql::planning::mv::SqlMvTarget {
                catalog: Some(self.target.catalog.clone()),
                database: self.target.namespace.clone(),
                name: self.target.table.clone(),
            })
            .map_err(|error| automatic_pre_dispatch_error(error.to_string()))?
            .ok_or_else(|| automatic_pre_dispatch_error("MV target is no longer ready"))?;
        let instance_id = ConnectorInstanceId::parse(&self.target.catalog)
            .map_err(|error| automatic_pre_dispatch_error(error.to_string()))?;
        let table = ConnectorTableIdentity {
            instance_id: instance_id.clone(),
            namespace: Arc::from(self.target.namespace.as_str()),
            table: Arc::from(self.target.table.as_str()),
        };
        let context = self
            .context
            .connector_request_context()
            .map_err(automatic_pre_dispatch_error)?;
        let control = ConnectorControlResolver::acquire_current(
            self.connector_control.as_ref(),
            &instance_id,
        )
        .map_err(|error| automatic_pre_dispatch_error(error.to_string()))?;
        let catalog_handle = control
            .binding()
            .catalog_handle()
            .map_err(|error| automatic_pre_dispatch_error(error.to_string()))?
            .clone();
        let capture = |observation_context| {
            control
                .binding()
                .metadata()
                .capture_table_object_binding(ConnectorTableObjectCaptureRequest {
                    table: table.clone(),
                    resolution: ConnectorTableResolution::StrictBaseTable,
                    selector: ConnectorTableObjectSelector::Current,
                    context: observation_context,
                })
                .map_err(|error| automatic_pre_dispatch_error(error.to_string()))
        };
        let binding = capture(context.clone())?;
        if binding.metadata.identity != table
            || binding.object_id != ready.projection.facts.source_revision().target_object_id
        {
            return Err(automatic_pre_dispatch_error(
                "MV maintenance target changed before admission",
            ));
        }
        let document_lease = control
            .derive_document_storage_lease()
            .map_err(|error| automatic_pre_dispatch_error(error.to_string()))?;
        let observe = |observation_context| {
            let request = ConnectorDocumentObservationRequest::try_new(
                document_lease.owner().clone(),
                document_lease.catalog_handle().clone(),
                table.clone(),
                binding.object_id.clone(),
                ConnectorDocumentStorageBudget::new(ConnectorDocumentStorageLimits::spec_default()),
                observation_context,
            )
            .map_err(|error| automatic_pre_dispatch_error(error.to_string()))?;
            novarocks_mv_application::persistence::documents::observe_current_management_document_set(
                &document_lease,
                request,
                novarocks_mv_application::persistence::validation::PersistenceDecodeBudget::default(),
            )
            .map(|observed| observed.into_parts())
            .map_err(|error| automatic_pre_dispatch_error(error.to_string()))
        };
        let entrance = self
            .product_service
            .management_entrance()
            .ok_or_else(|| automatic_pre_dispatch_error("MV management entrance is unavailable"))?;
        let (first_observation, first_documents) = observe(context.clone())?;
        if entrance.close_on_current_incarnation_mismatch(&first_observation) {
            tracing::warn!(target = ?table, "fresh Current MV marker names another incarnation; management closed");
            return Err(automatic_pre_dispatch_error(
                "MV Current marker names another process incarnation; management is closed",
            ));
        }
        let dependencies = first_documents.management_dependencies(control.control_runtime_id());
        if dependencies
            != ready
                .projection
                .facts
                .management_dependencies(control.control_runtime_id())
            || first_documents.configuration() != ready.projection.facts.configuration()
        {
            return Err(automatic_stale_policy_error(
                "MV maintenance policy observation is stale",
            ));
        }
        let operation_uuid = uuid::Uuid::now_v7();
        let managed_target =
            ManagedMvTarget::from_observation(&first_observation).map_err(|error| {
                automatic_pre_dispatch_error(format!("name MV maintenance target: {error:?}"))
            })?;
        let management = entrance
            .acquire_automatic_maintenance(
                AutomaticMaintenanceEffect::new(
                    managed_target.clone(),
                    dependencies.clone(),
                    EffectIdentity::from_bytes(*operation_uuid.as_bytes()),
                    scope,
                ),
                || self.context.is_cancelled(),
            )
            .map_err(|error| {
                MvBackgroundEngineError::new(
                    match error {
                        novarocks_mv_application::management::ManagementAdmissionError::Stopping
                        | novarocks_mv_application::management::ManagementAdmissionError::Cancelled => {
                            MvBackgroundEngineErrorKind::ShutdownCancelled
                        }
                        _ => MvBackgroundEngineErrorKind::TerminalFailure,
                    },
                    format!(
                        "admit MV maintenance action: {error:?}; phase={:?}",
                        entrance.management_phase(&table)
                    ),
                )
            })?;
        let after_wait = context.clone().after_external_effect();
        let rebound = capture(after_wait.clone())?;
        let (observation, documents) = observe(after_wait)?;
        if entrance.close_on_current_incarnation_mismatch(&observation) {
            tracing::warn!(target = ?table, "fresh Current MV marker names another incarnation; management closed");
            return Err(automatic_pre_dispatch_error(
                "MV Current marker names another process incarnation; management is closed",
            ));
        }
        if rebound.metadata.identity != table
            || rebound.object_id != binding.object_id
            || observation.object_id() != &binding.object_id
            || observation.marker().owner() != entrance.owner().as_str()
            || observation.marker().incarnation() != entrance.incarnation().as_str()
        {
            return Err(automatic_pre_dispatch_error(
                "MV maintenance target or owner changed during admission",
            ));
        }
        if documents.management_dependencies(control.control_runtime_id()) != dependencies
            || documents.configuration() != first_documents.configuration()
        {
            return Err(automatic_stale_policy_error(
                "MV maintenance documents changed during admission",
            ));
        }
        let retained_statistics = match ready.projection.facts.publication() {
            MvPublicationState::Published(published) => published.storage_rows().map(|rows| {
                novarocks_mv_application::persistence::projection::MvOutputStatistics {
                    object_id: ready
                        .projection
                        .facts
                        .source_revision()
                        .target_object_id
                        .clone(),
                    output_version: published.output_version().clone(),
                    storage_rows: rows,
                }
            }),
            MvPublicationState::NeverPublished => None,
        };
        Ok(PreparedAutomaticAction {
            entrance,
            management,
            managed_target,
            catalog_handle,
            product_target,
            operation_uuid,
            scope,
            context,
            retained_statistics,
        })
    }
}

impl PreparedAutomaticAction {
    fn mark_dispatched(&mut self) -> Result<(), MvBackgroundEngineError> {
        use novarocks_mv_application::management::{
            EffectIdentity, EffectResponsibility, ManagementTimestamp,
        };

        let timestamp = u64::try_from(now_unix_millis())
            .map_err(|_| automatic_pre_dispatch_error("system clock precedes Unix epoch"))?;
        self.management
            .mark_dispatched(EffectResponsibility::new(
                EffectIdentity::from_bytes(*self.operation_uuid.as_bytes()),
                self.managed_target.clone(),
                self.entrance.incarnation().clone(),
                self.scope,
                ManagementTimestamp::from_unix_millis(timestamp),
            ))
            .map_err(|error| {
                automatic_pre_dispatch_error(format!(
                    "mark MV maintenance action dispatched: {error:?}"
                ))
            })
    }

    fn record_terminal(
        self,
        disposition: novarocks_mv_application::management::EffectDisposition,
        readiness: &MvReadinessPort,
        connector_control: &dyn ConnectorControlRegistry,
    ) -> Result<(), MvBackgroundEngineError> {
        use novarocks_mv_application::management::EffectDisposition;

        self.management
            .record_terminal(disposition)
            .map_err(|error| {
                automatic_pre_dispatch_error(format!("record MV maintenance terminal: {error:?}"))
            })?;
        if disposition == EffectDisposition::KnownCommitted {
            crate::mv::domain::staged_create::install_configured_current_projection(
                self.entrance.as_ref(),
                readiness,
                connector_control,
                self.catalog_handle,
                self.product_target,
                self.operation_uuid,
                self.retained_statistics,
                self.context.after_external_effect(),
            )
            .map_err(|error| {
                automatic_pre_dispatch_error(format!(
                    "converge committed MV maintenance action: {error}"
                ))
            })?;
        }
        Ok(())
    }
}

fn automatic_pre_dispatch_error(message: impl Into<String>) -> MvBackgroundEngineError {
    MvBackgroundEngineError::new(MvBackgroundEngineErrorKind::TerminalFailure, message)
}

fn automatic_stale_policy_error(message: impl Into<String>) -> MvBackgroundEngineError {
    MvBackgroundEngineError::new(MvBackgroundEngineErrorKind::TransientUnavailable, message)
}

impl TableMaintenanceAutomaticRunner {
    fn run_managed_action(
        &self,
        request: MaintenanceActionRequest,
        scope: novarocks_mv_application::management::EffectScope,
    ) -> Result<MaintenanceActionOutcome, MvBackgroundEngineError> {
        use novarocks_mv_application::management::EffectDisposition;
        use novarocks_table_maintenance::{AutomaticMaintenanceOutcome, MaintenanceEffectId};

        let mut action = self.prepare_managed_action(scope)?;
        action.mark_dispatched()?;
        let effect_id = MaintenanceEffectId::from_bytes(*action.operation_uuid.as_bytes());
        let future = self.service.execute_automatic_action_with_effect_id(
            self.engine.as_ref(),
            request,
            effect_id,
            &self.context,
        );
        let result = match tokio::runtime::Handle::try_current() {
            Ok(handle) => tokio::task::block_in_place(|| handle.block_on(future)),
            Err(_) => self.handle.block_on(future),
        };
        let disposition = match &result {
            Ok(AutomaticMaintenanceOutcome::KnownCommitted(_)) => EffectDisposition::KnownCommitted,
            Ok(AutomaticMaintenanceOutcome::NoOpWithoutCommit(_)) => {
                EffectDisposition::KnownUncommitted
            }
            Err(error) => automatic_terminal_disposition(error.state),
        };
        action.record_terminal(
            disposition,
            self.readiness.as_ref(),
            self.connector_control.as_ref(),
        )?;
        result
            .map(AutomaticMaintenanceOutcome::into_action_outcome)
            .map_err(automatic_terminal_error)
    }
}

fn automatic_terminal_disposition(
    state: novarocks_table_maintenance::runtime::MaintenanceJobState,
) -> novarocks_mv_application::management::EffectDisposition {
    use novarocks_mv_application::management::EffectDisposition;
    use novarocks_table_maintenance::runtime::MaintenanceJobState;

    match state {
        MaintenanceJobState::KnownUncommitted
        | MaintenanceJobState::PreDispatchFailed
        | MaintenanceJobState::CancelledBeforeDispatch
        | MaintenanceJobState::TargetReplaced => EffectDisposition::KnownUncommitted,
        MaintenanceJobState::KnownCommittedFinalizationFailed => EffectDisposition::KnownCommitted,
        MaintenanceJobState::CommitUnknown
        | MaintenanceJobState::Failed
        | MaintenanceJobState::Pending
        | MaintenanceJobState::Running
        | MaintenanceJobState::Finished => EffectDisposition::CommitUnknown,
    }
}

fn automatic_terminal_error(
    error: novarocks_table_maintenance::runtime::TerminalError,
) -> MvBackgroundEngineError {
    MvBackgroundEngineError::new(
        if error.state
            == novarocks_table_maintenance::runtime::MaintenanceJobState::CancelledBeforeDispatch
        {
            MvBackgroundEngineErrorKind::ShutdownCancelled
        } else {
            MvBackgroundEngineErrorKind::TerminalFailure
        },
        format!(
            "automatic maintenance action ended as {}: {}",
            error.state.as_str(),
            error.message
        ),
    )
}

impl AutomaticMaintenanceRunner for TableMaintenanceAutomaticRunner {
    fn expire_snapshots_durably(
        &mut self,
        request: MaintenanceActionRequest,
    ) -> Result<MaintenanceActionOutcome, MvBackgroundEngineError> {
        self.run_managed_action(
            request,
            novarocks_mv_application::management::EffectScope::CATALOG_AND_OBJECT_DELETION,
        )
    }

    fn rewrite_position_deletes_durably(
        &mut self,
        request: MaintenanceActionRequest,
    ) -> Result<MaintenanceActionOutcome, MvBackgroundEngineError> {
        self.run_managed_action(
            request,
            novarocks_mv_application::management::EffectScope::CATALOG_COMMIT,
        )
    }

    fn optimize_durably(
        &mut self,
        target: MaintenanceTarget,
    ) -> Result<OptimizeDurableOutcome, MvBackgroundEngineError> {
        use novarocks_mv_application::management::{EffectDisposition, EffectScope};
        use novarocks_table_maintenance::{AutomaticOptimizeOutcome, MaintenanceEffectId};

        let mut action = self.prepare_managed_action(EffectScope::CATALOG_AND_OBJECT_DELETION)?;
        action.mark_dispatched()?;
        let effect_id = MaintenanceEffectId::from_bytes(*action.operation_uuid.as_bytes());
        let result = self.service.execute_automatic_optimize_with_effect_id(
            self.engine.as_ref(),
            target,
            effect_id,
            &self.context,
        );
        let disposition = match &result {
            Ok(AutomaticOptimizeOutcome::Finished {
                commit_occurred: true,
                ..
            }) => EffectDisposition::KnownCommitted,
            Ok(
                AutomaticOptimizeOutcome::Finished {
                    commit_occurred: false,
                    ..
                }
                | AutomaticOptimizeOutcome::AlreadyActive,
            ) => EffectDisposition::KnownUncommitted,
            Err(error) => automatic_terminal_disposition(error.state),
        };
        action.record_terminal(
            disposition,
            self.readiness.as_ref(),
            self.connector_control.as_ref(),
        )?;
        match result.map_err(automatic_terminal_error)? {
            AutomaticOptimizeOutcome::Finished { handle, .. } => {
                Ok(OptimizeDurableOutcome::Finished { handle })
            }
            AutomaticOptimizeOutcome::AlreadyActive => Ok(OptimizeDurableOutcome::AlreadyActive),
        }
    }
}

fn canonical_target(projection: &StoredMvProjection) -> Option<MaintenanceTarget> {
    let target = projection.facts.target();
    Some(MaintenanceTarget {
        catalog: target.catalog()?.to_owned(),
        namespace: target.namespace().to_owned(),
        table: target.name().to_owned(),
    })
}

fn now_unix_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_mv_application::management::EffectDisposition;
    use novarocks_mv_application::persistence::test_support::ProjectionFixture;
    use novarocks_mv_application::product::MvTarget;
    use novarocks_table_maintenance::runtime::MaintenanceJobState;

    #[test]
    fn automatic_terminal_accounting_preserves_committed_and_unknown_effects() {
        assert_eq!(
            automatic_terminal_disposition(MaintenanceJobState::PreDispatchFailed),
            EffectDisposition::KnownUncommitted
        );
        assert_eq!(
            automatic_terminal_disposition(MaintenanceJobState::KnownUncommitted),
            EffectDisposition::KnownUncommitted
        );
        assert_eq!(
            automatic_terminal_disposition(MaintenanceJobState::KnownCommittedFinalizationFailed),
            EffectDisposition::KnownCommitted
        );
        assert_eq!(
            automatic_terminal_disposition(MaintenanceJobState::CommitUnknown),
            EffectDisposition::CommitUnknown
        );
        assert_eq!(
            automatic_terminal_disposition(MaintenanceJobState::Failed),
            EffectDisposition::CommitUnknown
        );
    }

    #[test]
    fn cancelled_before_dispatch_does_not_trip_maintenance_circuit_breaker() {
        let error = automatic_terminal_error(
            novarocks_table_maintenance::runtime::TerminalError::cancelled_before_dispatch(
                "worker is stopping",
            ),
        );
        assert_eq!(error.kind(), MvBackgroundEngineErrorKind::ShutdownCancelled);
    }

    #[test]
    fn stale_policy_observation_remains_retryable_before_dispatch() {
        let error =
            automatic_stale_policy_error("MV maintenance documents changed during admission");
        assert_eq!(
            error.kind(),
            MvBackgroundEngineErrorKind::TransientUnavailable
        );
    }

    #[test]
    fn maintenance_target_uses_projection_target_not_source_occurrences() {
        let projection = StoredMvProjection {
            mv_id: 1,
            facts: ProjectionFixture::new(
                MvTarget::from_parts(Some("target_catalog"), "target_namespace", "target_mv"),
                None,
            )
            .build()
            .expect("projection"),
        };
        let target = canonical_target(&projection).expect("validated target");
        assert_eq!(target.catalog, "target_catalog");
        assert_eq!(target.namespace, "target_namespace");
        assert_eq!(target.table, "target_mv");
    }
}
