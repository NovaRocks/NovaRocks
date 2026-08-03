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

use std::sync::mpsc::{Receiver, TryRecvError};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use novarocks::engine::table_maintenance::{
    AutomaticMaintenanceContext, MaintenanceActionOutcome, MaintenanceActionRequest,
    MaintenanceTarget, OptimizeSubmission, TableMaintenanceEngine, TableMaintenanceService,
};
use novarocks::mv::background::{
    MvBackgroundEngine, MvBackgroundEngineError, MvBackgroundEngineErrorKind,
};
use novarocks::mv::persistence::definition::StoredMvDefinition;
use novarocks::mv::repository::{MvRepository, MvRepositoryError};
use novarocks::query_execution::cancellation::QueryCancellationSource;

use super::activity::{CanonicalMvTarget, MvActivityGate, MvActivityGateError, MvActivityOwner};
use super::maintenance::{
    AutomaticMaintenanceRunner, MaintenanceAdmission, MaintenanceCoordinator,
    MaintenanceCoordinatorConfig, MaintenanceExecutionReport,
};

const MAINTENANCE_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(30 * 60);

/// Dependencies bound by the frontend host after Core has completed restore,
/// recovery, provider binding and table-maintenance recovery.
#[derive(Clone)]
pub(crate) struct FrontendMaintenanceWorkerDependencies {
    pub(crate) repository: Arc<dyn MvRepository>,
    pub(crate) background_engine: Arc<dyn MvBackgroundEngine>,
    pub(crate) table_maintenance_engine: Arc<dyn TableMaintenanceEngine>,
    pub(crate) table_maintenance_service: Arc<dyn TableMaintenanceService>,
    pub(crate) activity_gate: MvActivityGate,
    pub(crate) coordinator_config: MaintenanceCoordinatorConfig,
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
    pub(crate) mv_id: i64,
    pub(crate) target: MaintenanceTarget,
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

/// A frontend-owned, synchronous automatic-maintenance runtime.  A host may
/// call [`Self::run_once`] from its bounded worker executor or use
/// [`Self::run_until_stopped`] from a dedicated worker thread.  The latter is
/// intentionally only a loop helper: it does not hide lifecycle ownership or
/// manufacture an all-in-one execution path.
pub(crate) struct FrontendMaintenanceWorker {
    dependencies: FrontendMaintenanceWorkerDependencies,
    coordinator: Mutex<MaintenanceCoordinator>,
}

impl FrontendMaintenanceWorker {
    pub(crate) fn new(dependencies: FrontendMaintenanceWorkerDependencies) -> Self {
        let coordinator = MaintenanceCoordinator::new(dependencies.coordinator_config.clone());
        Self {
            dependencies,
            coordinator: Mutex::new(coordinator),
        }
    }

    pub(crate) fn config(&self) -> MaintenanceCoordinatorConfig {
        self.coordinator
            .lock()
            .expect("frontend MV maintenance coordinator lock poisoned")
            .config()
            .clone()
    }

    /// Evaluate every current frontend MV definition once.  A ticket waiting
    /// behind manual/scheduled activity is reported as `GateBusy` and does not
    /// consume a maintenance permit.  This method keeps an admitted attempt's
    /// permit and lease around its entire action sequence.
    pub(crate) fn run_once(
        &self,
        now_ms: i64,
    ) -> Result<FrontendMaintenancePassReport, MvRepositoryError> {
        let definitions = self.dependencies.repository.list_definitions()?;
        let pass = Mutex::new(FrontendMaintenancePassReport::default());
        // Admission is synchronized inside `MaintenanceCoordinator`, but the
        // durable operations run outside that lock.  The coordinator's active
        // set therefore bounds real concurrent work across different MVs.
        std::thread::scope(|scope| {
            for definition in definitions {
                let pass = &pass;
                scope.spawn(move || self.run_definition(definition, now_ms, pass));
            }
        });
        Ok(pass
            .into_inner()
            .expect("frontend MV maintenance pass lock poisoned"))
    }

    /// A simple process-local runtime loop.  Shutdown is owned by the host:
    /// it calls `MvActivityGate::begin_stopping`, signals `stop_rx`, and joins
    /// this thread with the application shutdown deadline.
    pub(crate) fn run_until_stopped(
        &self,
        stop_rx: &Receiver<()>,
        wake_rx: &Receiver<()>,
        interval: Duration,
    ) {
        loop {
            if stop_rx.try_recv().is_ok() {
                return;
            }
            let now_ms = now_unix_millis();
            if let Err(error) = self.run_once(now_ms) {
                tracing::warn!(error = %error, "frontend MV maintenance inventory failed");
            }
            let wait_until = std::time::Instant::now() + interval.max(Duration::from_millis(1));
            loop {
                if stop_rx.try_recv().is_ok() {
                    return;
                }
                match wake_rx.try_recv() {
                    Ok(()) => break,
                    Err(TryRecvError::Disconnected) => return,
                    Err(TryRecvError::Empty) => {}
                }
                let remaining = wait_until.saturating_duration_since(std::time::Instant::now());
                if remaining.is_zero() {
                    break;
                }
                std::thread::sleep(remaining.min(Duration::from_millis(25)));
            }
        }
    }

    fn run_definition(
        &self,
        definition: StoredMvDefinition,
        now_ms: i64,
        pass: &Mutex<FrontendMaintenancePassReport>,
    ) {
        let target = match canonical_target(&definition) {
            Some(target) => target,
            None => {
                pass.lock()
                    .expect("frontend MV maintenance pass lock poisoned")
                    .skipped
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
                pass.lock()
                    .expect("frontend MV maintenance pass lock poisoned")
                    .skipped
                    .push(FrontendMaintenanceSkip::FactsFailed {
                        mv_id: definition.mv_id,
                        kind: error.kind(),
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
                pass.lock()
                    .expect("frontend MV maintenance pass lock poisoned")
                    .skipped
                    .push(FrontendMaintenanceSkip::Stopping {
                        mv_id: definition.mv_id,
                    });
                return;
            }
        };
        let lease = match ticket.try_acquire() {
            Ok(Some(lease)) => lease,
            Ok(None) => {
                pass.lock()
                    .expect("frontend MV maintenance pass lock poisoned")
                    .skipped
                    .push(FrontendMaintenanceSkip::GateBusy {
                        mv_id: definition.mv_id,
                    });
                return;
            }
            Err(MvActivityGateError::Stopping) => {
                pass.lock()
                    .expect("frontend MV maintenance pass lock poisoned")
                    .skipped
                    .push(FrontendMaintenanceSkip::Stopping {
                        mv_id: definition.mv_id,
                    });
                return;
            }
        };

        // Reject pre-dispatch cancellation before acquiring the durable route.
        // A cancellation that races an external commit is retained by that
        // route as recovery evidence rather than turned into a retry.
        if lease
            .cancellation()
            .is_some_and(|cancellation| cancellation.is_cancelled())
        {
            pass.lock()
                .expect("frontend MV maintenance pass lock poisoned")
                .skipped
                .push(FrontendMaintenanceSkip::Stopping {
                    mv_id: definition.mv_id,
                });
            return;
        }

        let attempt = match self
            .coordinator
            .lock()
            .expect("frontend MV maintenance coordinator lock poisoned")
            .try_begin(definition.mv_id, target.clone(), &facts, now_ms)
        {
            Ok(attempt) => attempt,
            Err(admission) => {
                pass.lock()
                    .expect("frontend MV maintenance pass lock poisoned")
                    .skipped
                    .push(FrontendMaintenanceSkip::Admission {
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
                lease
                    .cancellation()
                    .unwrap_or_else(|| QueryCancellationSource::new().view()),
                Instant::now() + MAINTENANCE_ATTEMPT_TIMEOUT,
            ),
        };
        let execution = MaintenanceCoordinator::execute_attempt(&attempt, &mut runner);
        self.coordinator
            .lock()
            .expect("frontend MV maintenance coordinator lock poisoned")
            .finish_attempt(attempt, &execution, now_ms);
        // Keep `lease` live until all durable calls and the coordinator's
        // terminal transition have completed.  Its Drop wakes the next FIFO
        // request for this MV target.
        let _lease = lease;
        pass.lock()
            .expect("frontend MV maintenance pass lock poisoned")
            .attempts
            .push(FrontendMaintenanceAttemptReport {
                mv_id: definition.mv_id,
                target,
                execution,
            });
    }
}

/// Narrow adapter from automatic policy actions to the existing frontend
/// durable table-maintenance service.  Any opaque service error is treated as
/// `RecoveryRequired`, not parsed as text and not retried as a guessed
/// transient failure, because a durable external mutation may be unknown.
struct TableMaintenanceAutomaticRunner {
    engine: Arc<dyn TableMaintenanceEngine>,
    service: Arc<dyn TableMaintenanceService>,
    context: AutomaticMaintenanceContext,
}

impl AutomaticMaintenanceRunner for TableMaintenanceAutomaticRunner {
    fn expire_snapshots_durably(
        &mut self,
        request: MaintenanceActionRequest,
    ) -> Result<MaintenanceActionOutcome, MvBackgroundEngineError> {
        self.service
            .execute_automatic_action_with_context(self.engine.as_ref(), request, &self.context)
            .map_err(durable_service_error)
    }

    fn rewrite_position_deletes_durably(
        &mut self,
        request: MaintenanceActionRequest,
    ) -> Result<MaintenanceActionOutcome, MvBackgroundEngineError> {
        self.service
            .execute_automatic_action_with_context(self.engine.as_ref(), request, &self.context)
            .map_err(durable_service_error)
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
        MvBackgroundEngineErrorKind::RecoveryRequired,
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
