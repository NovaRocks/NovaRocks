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
//! background port for provider-neutral facts, obtains the shared activity
//! gate, and only then consumes a maintenance permit in
//! [`MaintenanceCoordinator`].  The runner only invokes the durable
//! table-maintenance service routes; it never calls
//! [`TableMaintenanceEngine::execute_action`].
//!
//! Automatic optimize uses the table-maintenance service's synchronous durable
//! lifecycle route.  That route must claim, execute, and terminally persist a
//! job before this worker releases its activity lease and maintenance permit.

use novarocks_workload_control::{
    BusinessPermit, RootAdmissionHandle, RootWork, WorkClass, WorkOwner, WorkRequest,
};
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::background::MvBackgroundEngine;
use crate::mv::domain::readiness::MvReadinessPort;
use crate::query_execution::maintenance::{
    AutomaticMaintenanceContext, TableMaintenanceEngine, TableMaintenanceService,
};
use novarocks_mv_application::persistence::definition::StoredMvDefinition;
use novarocks_mv_application::repository::MvRepositoryError;
use novarocks_mv_application::{
    activity::{CanonicalMvTarget, MvActivityGate, MvActivityGateError, MvActivityOwner},
    maintenance::{
        AutomaticMaintenanceRunner, MaintenanceAdmission, MaintenanceCoordinator,
        MaintenanceCoordinatorConfig, MaintenanceExecutionReport, MvBackgroundEngineError,
        MvBackgroundEngineErrorKind, MvMaintenanceRuntime,
    },
};
use novarocks_table_maintenance::{
    MaintenanceActionOutcome, MaintenanceActionRequest, MaintenanceTarget, OptimizeSubmission,
};

/// Dependencies bound by the frontend host after Core has completed restore,
/// recovery, provider binding and table-maintenance recovery.
#[derive(Clone)]
pub(crate) struct FrontendMaintenanceWorkerDependencies {
    pub(crate) readiness: Arc<MvReadinessPort>,
    pub(crate) background_engine: Arc<dyn MvBackgroundEngine>,
    pub(crate) table_maintenance_engine: Arc<dyn TableMaintenanceEngine>,
    pub(crate) table_maintenance_service: Arc<dyn TableMaintenanceService>,
    pub(crate) activity_gate: MvActivityGate,
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
    GateBusy {
        mv_id: i64,
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
        Self {
            runtime: MvMaintenanceRuntime::new(dependencies.coordinator_config.clone()),
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

    /// Evaluate every current frontend MV definition once.  A ticket waiting
    /// behind manual/scheduled activity is reported as `GateBusy` and does not
    /// consume a maintenance permit.  This method keeps an admitted attempt's
    /// permit and lease around its entire action sequence.
    pub(crate) fn run_once(
        &self,
        now_ms: i64,
    ) -> Result<FrontendMaintenancePassReport, MvRepositoryError> {
        let definitions = self
            .dependencies
            .readiness
            .list_ready_projections()?
            .into_iter()
            .map(|projection| projection.definition)
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
        definition: StoredMvDefinition,
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

        let RootWork { owner, business } = match self
            .dependencies
            .root_admission
            .try_begin_root(WorkRequest::new(WorkClass::MaterializedView))
        {
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
                finish_automatic_root(owner, business);
                pass.skipped.push(FrontendMaintenanceSkip::Stopping {
                    mv_id: definition.mv_id,
                });
                return;
            }
        };
        let mut ticket = match self.dependencies.activity_gate.request(
            CanonicalMvTarget::from_parts(Some(&target.catalog), &target.namespace, &target.table),
            MvActivityOwner::AutomaticMaintenance,
        ) {
            Ok(ticket) => ticket,
            Err(MvActivityGateError::Stopping) => {
                finish_automatic_root(owner, business);
                pass.skipped.push(FrontendMaintenanceSkip::Stopping {
                    mv_id: definition.mv_id,
                });
                return;
            }
        };
        let lease = match ticket.try_acquire() {
            Ok(Some(lease)) => lease,
            Ok(None) => {
                finish_automatic_root(owner, business);
                pass.skipped.push(FrontendMaintenanceSkip::GateBusy {
                    mv_id: definition.mv_id,
                });
                return;
            }
            Err(MvActivityGateError::Stopping) => {
                finish_automatic_root(owner, business);
                pass.skipped.push(FrontendMaintenanceSkip::Stopping {
                    mv_id: definition.mv_id,
                });
                return;
            }
        };

        // Reject pre-dispatch cancellation before acquiring the durable route.
        // A cancellation that races an external commit is retained by that
        // route as recovery evidence rather than turned into a retry.
        if cancellation.is_cancelled()
            || lease
                .cancellation()
                .is_some_and(|cancellation| cancellation.is_cancelled())
        {
            drop(lease);
            finish_automatic_root(owner, business);
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
                drop(lease);
                finish_automatic_root(owner, business);
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
            context: AutomaticMaintenanceContext::with_deadline(
                cancellation,
                Instant::now() + self.dependencies.attempt_timeout,
            ),
            handle: self.dependencies.runtime.clone(),
        };
        let execution = MaintenanceCoordinator::execute_attempt(&attempt, &mut runner);
        self.runtime.finish(attempt, &execution, now_ms);
        // Release the activity lease only after all durable calls and the
        // coordinator's terminal transition have completed, then finish the
        // governed root.
        drop(lease);
        finish_automatic_root(owner, business);
        pass.attempts.push(FrontendMaintenanceAttemptReport {
            mv_id: definition.mv_id,
            target,
            execution,
        });
    }
}

fn finish_automatic_root(owner: WorkOwner, business: BusinessPermit) {
    drop(business);
    owner.complete();
}

/// Narrow adapter from automatic policy actions to the existing frontend
/// durable table-maintenance service.  Any opaque service error is treated as
/// `TerminalFailure`, not parsed as text and not retried as a guessed
/// transient failure, because a durable external mutation may be unknown.
struct TableMaintenanceAutomaticRunner {
    engine: Arc<dyn TableMaintenanceEngine>,
    service: Arc<dyn TableMaintenanceService>,
    context: AutomaticMaintenanceContext,
    /// Captured where the worker thread is spawned, because the thread itself
    /// has no runtime of its own.
    handle: tokio::runtime::Handle,
}

impl TableMaintenanceAutomaticRunner {
    /// Runs one durable maintenance action from a bare worker thread.
    ///
    /// Automatic maintenance fans out over `std::thread` rather than tasks, so
    /// this adapter has no async context to await in. The maintenance service
    /// is async because its work is durable I/O, and the adaptation therefore
    /// happens here — in the type whose whole purpose is to present that
    /// service to a thread-based runner — and in no domain contract. It goes
    /// away when the maintenance workers become tasks.
    fn run_durably(
        &self,
        request: MaintenanceActionRequest,
    ) -> Result<MaintenanceActionOutcome, MvBackgroundEngineError> {
        let future = self.service.execute_automatic_action_with_context(
            self.engine.as_ref(),
            request,
            &self.context,
        );
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => tokio::task::block_in_place(|| handle.block_on(future)),
            Err(_) => self.handle.block_on(future),
        }
        .map_err(durable_service_error)
    }
}

impl AutomaticMaintenanceRunner for TableMaintenanceAutomaticRunner {
    fn expire_snapshots_durably(
        &mut self,
        request: MaintenanceActionRequest,
    ) -> Result<MaintenanceActionOutcome, MvBackgroundEngineError> {
        self.run_durably(request)
    }

    fn rewrite_position_deletes_durably(
        &mut self,
        request: MaintenanceActionRequest,
    ) -> Result<MaintenanceActionOutcome, MvBackgroundEngineError> {
        self.run_durably(request)
    }

    fn optimize_durably(
        &mut self,
        target: MaintenanceTarget,
    ) -> Result<OptimizeSubmission, MvBackgroundEngineError> {
        self.service
            .execute_automatic_optimize_durably_with_context(
                self.engine.as_ref(),
                target,
                &self.context,
            )
            .map_err(durable_service_error)
    }
}

fn canonical_target(definition: &StoredMvDefinition) -> Option<MaintenanceTarget> {
    Some(MaintenanceTarget {
        catalog: definition.target_catalog.clone()?,
        namespace: definition.target_namespace.clone()?,
        table: definition.target_table.clone()?,
    })
}

fn durable_service_error(error: String) -> MvBackgroundEngineError {
    MvBackgroundEngineError::new(
        MvBackgroundEngineErrorKind::TerminalFailure,
        format!("automatic maintenance durable lifecycle returned an opaque error: {error}"),
    )
}

fn now_unix_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
}
