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

//! Frontend adapters for the product-owned OPTIMIZE worker.

use std::sync::{Arc, Mutex, Weak};

use novarocks_spi::connector::{
    ConnectorCleanupCandidate, ConnectorCleanupOperationId, ConnectorCleanupOwnedRefSelection,
};
use novarocks_table_maintenance::job_service::{CapturedOptimizeTarget, OptimizeTargetCapturePort};
use novarocks_table_maintenance::product::TableMaintenanceProduct;
use novarocks_table_maintenance::product::{
    CleanupCandidate, CleanupOwnedRefFact, CleanupSession, CleanupTerminal,
    DistributedRewriteSession, RewriteIntent, TableMaintenanceEffectPort,
};
use novarocks_table_maintenance::runtime::TerminalError as OptimizeTerminalError;
use novarocks_table_maintenance::worker::{
    OptimizeJobAdmission, OptimizeJobAdmissionPort, OptimizeJobExecution, OptimizeJobExecutionPort,
    OptimizeJobOriginals, OptimizeJobScope,
};
use novarocks_table_maintenance::{
    AutomaticMaintenanceOutcome, MaintenanceActionOutcome, MaintenanceActionRequest,
    MaintenanceEffectId, MaintenanceTarget, MaintenanceTargetRebind, OptimizeJob,
};
use novarocks_workload_control::{RootAdmissionHandle, WorkClass, WorkError, WorkRequest};

use crate::query_execution::maintenance::TableMaintenanceEngine;

/// Frontend provider/native-query adapter for the neutral maintenance product.
/// It keeps opaque connector sessions and fragment encoding here, while the
/// product crate owns all target-gate, cohort and terminal business decisions.
pub(crate) struct LegacyMaintenanceEffects<'a> {
    engine: &'a dyn TableMaintenanceEngine,
    state: Option<&'a crate::query_execution::maintenance::MaintenanceActionState>,
}

impl<'a> LegacyMaintenanceEffects<'a> {
    pub(crate) fn new(
        engine: &'a dyn TableMaintenanceEngine,
        state: &'a crate::query_execution::maintenance::MaintenanceActionState,
    ) -> Self {
        Self {
            engine,
            state: Some(state),
        }
    }
}

impl TableMaintenanceEffectPort for LegacyMaintenanceEffects<'_> {
    fn observe_cleanup_owned_ref<'a>(
        &'a self,
        observations: &'a novarocks_table_maintenance::gc_observation::GcOwnedRefObservationAccelerator,
        observation: novarocks_table_maintenance::gc_observation::GcOwnedRefObservation,
        now_ms: i64,
        safe_gc_age_ms: i64,
    ) -> novarocks_table_maintenance::product::MaintenanceFuture<
        'a,
        Result<
            novarocks_table_maintenance::gc_observation::GcOwnedRefObservationDecision,
            OptimizeTerminalError,
        >,
    > {
        // Only this explicitly legacy command adapter calls the old runner.
        // Governed CleanupEffects always requires its Original context.
        Box::pin(async move {
            observations
                .observe(observation, now_ms, safe_gc_age_ms)
                .await
                .map_err(|error| {
                    OptimizeTerminalError::pre_dispatch_failed(format!(
                        "record GC owned-ref observation failed: {error}"
                    ))
                })
        })
    }

    fn reject_user_action_on_mv<'a>(
        &'a self,
        target: &'a MaintenanceTarget,
    ) -> novarocks_table_maintenance::product::MaintenanceFuture<
        'a,
        Result<(), novarocks_table_maintenance::runtime::TerminalError>,
    > {
        Box::pin(async move {
            self.engine
                .reject_user_action_on_mv(target)
                .await
                .map_err(crate::query_execution::maintenance::rewrite_job::guard_terminal_error)
        })
    }

    fn execute_metadata<'a>(
        &'a self,
        request: MaintenanceActionRequest,
    ) -> novarocks_table_maintenance::product::MaintenanceFuture<
        'a,
        Result<MaintenanceActionOutcome, String>,
    > {
        Box::pin(async move {
            let state = self
                .state
                .ok_or("rewrite effect cannot dispatch metadata")?;
            self.engine.execute_action(state, request).await
        })
    }

    fn execute_metadata_with_id<'a>(
        &'a self,
        request: MaintenanceActionRequest,
        effect_id: MaintenanceEffectId,
    ) -> novarocks_table_maintenance::product::MaintenanceFuture<
        'a,
        Result<AutomaticMaintenanceOutcome, OptimizeTerminalError>,
    > {
        Box::pin(async move {
            let state = self.state.ok_or_else(|| {
                OptimizeTerminalError::pre_dispatch_failed(
                    "rewrite effect cannot dispatch metadata",
                )
            })?;
            self.engine
                .execute_automatic_metadata_action(state, request, effect_id)
                .await
        })
    }

    fn begin_rewrite<'a>(
        &'a self,
        _target: &'a MaintenanceTarget,
        _intent: RewriteIntent,
    ) -> novarocks_table_maintenance::product::MaintenanceFuture<
        'a,
        Result<Box<dyn DistributedRewriteSession + 'a>, String>,
    > {
        // Metadata commands cannot create the governed rewrite Source.
        Box::pin(async {
            Err("metadata command effect cannot dispatch a distributed rewrite".into())
        })
    }

    fn begin_cleanup<'a>(
        &'a self,
        target: &'a MaintenanceTarget,
        older_than_ms: i64,
    ) -> novarocks_table_maintenance::product::MaintenanceFuture<
        'a,
        Result<Box<dyn CleanupSession + 'a>, novarocks_table_maintenance::runtime::TerminalError>,
    > {
        Box::pin(async move {
            let session = self.engine.plan_cleanup_maintenance(
                target,
                ConnectorCleanupOperationId::new(),
                older_than_ms,
            )?;
            let raw_candidates = cleanup_candidates_first_page(self.engine, &session)?;
            let candidates = raw_candidates
                .iter()
                .map(cleanup_candidate_fact)
                .collect::<Result<Vec<_>, String>>()?;
            Ok(Box::new(FrontendCleanupSession {
                engine: self.engine,
                target: target.clone(),
                older_than_ms,
                session,
                raw_candidates,
                candidates,
            }) as Box<dyn CleanupSession + 'a>)
        })
    }
}

pub(crate) fn rewrite_compile_terminal(
    error: novarocks_sql::compiler::SqlCompileError,
) -> OptimizeTerminalError {
    match error {
        novarocks_sql::compiler::SqlCompileError::Cancelled => {
            OptimizeTerminalError::from_compile_control(
                novarocks_type_contract::CompileControlError::Cancelled,
            )
        }
        novarocks_sql::compiler::SqlCompileError::DeadlineExceeded => {
            OptimizeTerminalError::from_compile_control(
                novarocks_type_contract::CompileControlError::DeadlineExceeded,
            )
        }
        novarocks_sql::compiler::SqlCompileError::ResourceExhausted => {
            OptimizeTerminalError::from_compile_control(
                novarocks_type_contract::CompileControlError::ResourceExhausted,
            )
        }
        novarocks_sql::compiler::SqlCompileError::Analyze(error) => match error.control_error() {
            Some(error) => OptimizeTerminalError::from_compile_control(error),
            None => OptimizeTerminalError::pre_dispatch_failed(error.to_string()),
        },
        novarocks_sql::compiler::SqlCompileError::CompilationSource(source) => {
            OptimizeTerminalError::from_compile_control(
                novarocks_type_contract::CompileControlError::Source(source),
            )
        }
        error => OptimizeTerminalError::pre_dispatch_failed(error.to_string()),
    }
}

struct FrontendCleanupSession<'a> {
    engine: &'a dyn TableMaintenanceEngine,
    target: MaintenanceTarget,
    older_than_ms: i64,
    session: crate::connector::cleanup_maintenance::CleanupMaintenanceSession,
    raw_candidates: Vec<ConnectorCleanupCandidate>,
    candidates: Vec<CleanupCandidate>,
}

impl CleanupSession for FrontendCleanupSession<'_> {
    fn require_scanned_object_gc(&self) -> Result<(), String> {
        self.session
            .require_scanned_object_gc()
            .map_err(|error| error.to_string())
    }
    fn candidates(&self) -> &[CleanupCandidate] {
        &self.candidates
    }

    fn select_owned_refs<'a>(
        &'a mut self,
        candidate_indexes: &'a [usize],
    ) -> novarocks_table_maintenance::product::MaintenanceFuture<
        'a,
        Result<(), novarocks_table_maintenance::runtime::TerminalError>,
    > {
        Box::pin(async move {
            let identities = candidate_indexes
                .iter()
                .map(|index| {
                    self.raw_candidates
                        .get(*index)
                        .and_then(ConnectorCleanupCandidate::owned_ref_identity)
                        .ok_or_else(|| {
                            "owned-ref candidate has no valid exact identity".to_string()
                        })
                })
                .collect::<Result<Vec<_>, String>>()?;
            let selection =
                ConnectorCleanupOwnedRefSelection::try_new(identities).map_err(|error| {
                    format!("build mature owned-ref cleanup selection failed: {error}")
                })?;
            self.session = self.engine.plan_selected_owned_ref_cleanup_maintenance(
                &self.target,
                ConnectorCleanupOperationId::new(),
                self.older_than_ms,
                selection,
            )?;
            Ok(())
        })
    }

    fn execute<'a>(
        &'a mut self,
    ) -> novarocks_table_maintenance::product::MaintenanceFuture<
        'a,
        Result<CleanupTerminal, novarocks_table_maintenance::runtime::TerminalError>,
    > {
        Box::pin(async move {
            // The report is the candidate set, which the manifest fixes before
            // any batch runs. Collect it first, under the local result bound, so
            // a report too large to show is refused before anything is deleted.
            let locations = cleanup_candidate_locations(self.engine, &self.session)?;
            let batches = self.session.plan_ref().summary().batch_count();
            for ordinal in 0..batches {
                let prepared = self.engine.prepare_cleanup_batch(&self.session, ordinal)?;
                match self.engine.execute_cleanup_batch(&self.session, prepared)? {
                    crate::connector::cleanup_maintenance::CleanupBatchExecution::Receipt(
                        receipt,
                    ) => {
                        if receipt.summary().unknown() != 0 {
                            return Ok(CleanupTerminal::CommitUnknown {
                                failure: "provider reported an unknown cleanup batch outcome"
                                    .to_string(),
                            });
                        }
                    }
                    crate::connector::cleanup_maintenance::CleanupBatchExecution::Uncertain(
                        error,
                    ) => {
                        return Ok(CleanupTerminal::CommitUnknown {
                            failure: error.to_string(),
                        });
                    }
                }
            }
            match self.engine.finalize_cleanup_terminal(&self.session) {
                Ok(()) => Ok(CleanupTerminal::KnownCommitted { locations }),
                Err(error) => {
                    Ok(CleanupTerminal::KnownCommittedFinalizationFailed { failure: error })
                }
            }
        })
    }
}

fn cleanup_candidate_fact(
    candidate: &ConnectorCleanupCandidate,
) -> Result<CleanupCandidate, String> {
    match candidate {
        ConnectorCleanupCandidate::Object { .. } => Ok(CleanupCandidate::Object),
        ConnectorCleanupCandidate::OwnedRef {
            table_uuid,
            name,
            head_snapshot_id,
            provenance_version,
            provenance_digest,
            ..
        } => Ok(CleanupCandidate::OwnedRef(CleanupOwnedRefFact {
            table_uuid: *table_uuid,
            ref_name: name.to_string(),
            head_snapshot_id: *head_snapshot_id,
            provenance_version: *provenance_version,
            provenance_digest: *provenance_digest,
        })),
    }
}

fn cleanup_candidates_first_page(
    engine: &dyn TableMaintenanceEngine,
    session: &crate::connector::cleanup_maintenance::CleanupMaintenanceSession,
) -> Result<Vec<ConnectorCleanupCandidate>, String> {
    let page = engine.read_cleanup_candidate_page(session, 0, 1024)?;
    if page.candidates().is_empty() && !page.complete() {
        return Err("cleanup discovery returned a non-terminal empty candidate page".to_string());
    }
    Ok(page.candidates().to_vec())
}

/// Reads the operation's candidate locations, one bounded page at a time,
/// into the one-column report under [`LocalResultBound::V1`]: the candidate
/// count is checked from the plan before the first page, and every page's
/// locations before they are kept.
fn cleanup_candidate_locations(
    engine: &dyn TableMaintenanceEngine,
    session: &crate::connector::cleanup_maintenance::CleanupMaintenanceSession,
) -> Result<Vec<String>, String> {
    use novarocks_query_application::api::LocalResultBound;

    let bound = LocalResultBound::V1;
    let candidates =
        usize::try_from(session.plan_ref().summary().candidate_count()).unwrap_or(usize::MAX);
    bound
        .admit(candidates, 0)
        .map_err(|error| format!("orphan cleanup report: {error}"))?;
    let mut offset = 0_u64;
    let mut locations = Vec::with_capacity(candidates);
    let mut bytes = 0_usize;
    loop {
        let page = engine.read_cleanup_candidate_page(session, offset, 1024)?;
        for key in page.display_keys() {
            bytes = bytes.saturating_add(LocalResultBound::cell_bytes(key.len()));
            bound
                .admit(locations.len() + 1, bytes)
                .map_err(|error| format!("orphan cleanup report: {error}"))?;
            locations.push(key.to_string());
        }
        if page.complete() {
            return Ok(locations);
        }
        offset = offset
            .checked_add(page.candidates().len() as u64)
            .ok_or_else(|| "orphan cleanup candidate page offset overflow".to_string())?;
    }
}

/// Frontend-only provider binding capture for a product-gated OPTIMIZE job.
pub(crate) struct FrontendOptimizeTargetCapturePort<'a> {
    engine: &'a dyn TableMaintenanceEngine,
    statement: bool,
    #[cfg(novarocks_dev_controls)]
    receipt: Option<&'a super::test_controls::SubmissionReceipt>,
}

impl<'a> FrontendOptimizeTargetCapturePort<'a> {
    pub(crate) fn for_statement(engine: &'a dyn TableMaintenanceEngine) -> Self {
        Self {
            engine,
            statement: true,
            #[cfg(novarocks_dev_controls)]
            receipt: None,
        }
    }
}

#[cfg(novarocks_dev_controls)]
impl<'a> FrontendOptimizeTargetCapturePort<'a> {
    pub(crate) fn with_test_receipt(
        mut self,
        receipt: Option<&'a super::test_controls::SubmissionReceipt>,
    ) -> Self {
        self.receipt = receipt;
        self
    }
}

impl OptimizeTargetCapturePort for FrontendOptimizeTargetCapturePort<'_> {
    fn capture(
        &self,
        target: &novarocks_table_maintenance::MaintenanceTarget,
    ) -> Result<CapturedOptimizeTarget, String> {
        if self.statement {
            let captured = self.engine.capture_admitted_optimize_target(target)?;
            if captured.completion
                == novarocks_table_maintenance::OptimizeCompletionMode::AwaitTerminal
                && self.engine.statement_cancellation().is_none()
            {
                return Err(
                    "OPTIMIZE requires the admitted statement cancellation scope".to_string(),
                );
            }
            #[cfg(novarocks_dev_controls)]
            if let Some(receipt) = self.receipt {
                receipt.capture(&captured.object_id)?;
            }
            return Ok(captured);
        }
        let object_id = self.engine.capture_target_object_id(target)?;
        Ok(CapturedOptimizeTarget {
            object_id: object_id.as_bytes().to_vec(),
            base_snapshot_id: self.engine.current_snapshot_id(target)?,
            completion: novarocks_table_maintenance::OptimizeCompletionMode::Detached,
        })
    }
}

pub(crate) struct FrontendOptimizeJobAdmissionPort {
    root_admission: RootAdmissionHandle,
    engine: Weak<dyn TableMaintenanceEngine>,
}

impl FrontendOptimizeJobAdmissionPort {
    pub(crate) fn new(
        root_admission: RootAdmissionHandle,
        engine: Weak<dyn TableMaintenanceEngine>,
    ) -> Self {
        Self {
            root_admission,
            engine,
        }
    }
}

impl OptimizeJobAdmissionPort for FrontendOptimizeJobAdmissionPort {
    fn begin(&self) -> novarocks_table_maintenance::worker::OptimizeJobAdmissionFuture<'_> {
        Box::pin(admit_original_optimize(
            self.root_admission.clone(),
            self.engine.clone(),
        ))
    }
}
async fn admit_original_optimize(
    root_admission: RootAdmissionHandle,
    engine: Weak<dyn TableMaintenanceEngine>,
) -> Result<OptimizeJobAdmission, String> {
    let root =
        match root_admission.begin_warehouse_root(WorkRequest::new(WorkClass::TableMaintenance)) {
            Ok(root) => root,
            Err(WorkError::NotReady)
            | Err(WorkError::Capacity(_))
            | Err(WorkError::CapacityWaitTimeout) => return Ok(OptimizeJobAdmission::RetryLater),
            Err(WorkError::Closed) => return Ok(OptimizeJobAdmission::Closed),
            Err(error) => return Err(format!("admit governed optimize root failed: {error}")),
        };
    let permit = root
        .owner
        .scope()
        .admit_query()
        .map_err(|error| error.to_string())?
        .await
        .map_err(|error| error.to_string())?;
    if engine.upgrade().is_none() {
        return Err("Optimize original execution owner has exited".into());
    }
    let mut owner = Some(root.owner);
    let scope = owner.as_ref().unwrap().scope();
    let mut permit = Some(permit);
    let originals = OptimizeJobOriginals::take_in(&mut owner, &mut permit)?;
    Ok(OptimizeJobAdmission::Acquired(Box::new(
        FrontendOptimizeJobScope {
            work: Some(originals),
            scope,
        },
    )))
}

struct FrontendOptimizeJobScope {
    work: Option<OptimizeJobOriginals>,
    scope: novarocks_workload_control::WorkScope,
}
impl OptimizeJobScope for FrontendOptimizeJobScope {
    fn take_originals(&mut self) -> Result<OptimizeJobOriginals, String> {
        self.work
            .take()
            .ok_or_else(|| "Optimize original admission was already moved".into())
    }
    fn original_owner(&self) -> Result<&novarocks_workload_control::WorkOwner, String> {
        self.work
            .as_ref()
            .and_then(|o| o.owner.as_ref())
            .ok_or_else(|| "Optimize original Root is empty".into())
    }
    fn is_cancelled(&self) -> Result<bool, String> {
        self.scope
            .cancellation()
            .map(|view| view.reason().is_some())
            .map_err(|e| e.to_string())
    }
}

pub(crate) struct FrontendOptimizeJobExecutionPort {
    engine: Weak<dyn TableMaintenanceEngine>,
    product: Weak<TableMaintenanceProduct>,
    #[cfg(novarocks_dev_controls)]
    controls: Option<Arc<super::test_controls::MaintenanceTestControls>>,
}

impl FrontendOptimizeJobExecutionPort {
    pub(crate) fn new(
        engine: Weak<dyn TableMaintenanceEngine>,
        product: Weak<TableMaintenanceProduct>,
        #[cfg(novarocks_dev_controls)] controls: Option<
            Arc<super::test_controls::MaintenanceTestControls>,
        >,
    ) -> Self {
        Self {
            engine,
            product,
            #[cfg(novarocks_dev_controls)]
            controls,
        }
    }
}

impl OptimizeJobExecutionPort for FrontendOptimizeJobExecutionPort {
    fn is_available(&self) -> bool {
        self.engine.strong_count() != 0 && self.product.strong_count() != 0
    }

    fn acquire(
        &self,
        owner: &novarocks_workload_control::WorkOwner,
    ) -> Result<Option<Box<dyn OptimizeJobExecution>>, String> {
        let Some((engine, product)) = self.engine.upgrade().zip(self.product.upgrade()) else {
            return Ok(None);
        };
        let engine = engine.for_original_background_root(owner)?;
        Ok(Some(Box::new(FrontendOptimizeJobExecution {
            mounted: None,
            engine,
            product,
            #[cfg(novarocks_dev_controls)]
            test: self
                .controls
                .as_ref()
                .map(|owner| MaintenanceExecutionControls {
                    owner: owner.clone(),
                    observation: Mutex::new(None),
                }),
        })))
    }
}

pub(crate) struct FrontendOptimizeJobExecution {
    pub(crate) mounted: Option<crate::query_execution::maintenance::rewrite_job::RewriteDomainView>,
    pub(crate) engine: crate::query_execution::maintenance::RequestScopedMaintenanceEngine,
    pub(crate) product: Arc<TableMaintenanceProduct>,
    #[cfg(novarocks_dev_controls)]
    test: Option<MaintenanceExecutionControls>,
}

#[cfg(novarocks_dev_controls)]
struct MaintenanceExecutionControls {
    owner: Arc<super::test_controls::MaintenanceTestControls>,
    observation: Mutex<Option<super::test_controls::AuthorizedJob>>,
}
#[cfg(novarocks_dev_controls)]
impl FrontendOptimizeJobExecution {
    fn record_test_dispatch(
        &self,
        job: &OptimizeJob,
        capacity: &novarocks_query_application::admitted_query_context::QueryResultCapacityBinding,
    ) -> Result<(), String> {
        if let Some(test) = &self.test {
            let mut observation = test
                .observation
                .lock()
                .map_err(|_| "maintenance dispatch observation lock failed")?;
            if let Some(observation) = observation.as_mut() {
                observation.record_dispatch(job, capacity)?;
            }
        }
        Ok(())
    }
}

impl OptimizeJobExecution for FrontendOptimizeJobExecution {
    fn run_original_job<'a>(
        self: Box<Self>,
        jobs: Arc<novarocks_table_maintenance::OptimizeProcessRuntime>,
        originals: OptimizeJobOriginals,
        job: OptimizeJob,
    ) -> novarocks_table_maintenance::product::MaintenanceFuture<
        'a,
        Result<novarocks_table_maintenance::OptimizeJobOutcome, OptimizeTerminalError>,
    > {
        Box::pin(original_job_run_original_job(self, jobs, originals, job))
    }

    fn original_effect<'a>(
        &'a self,
    ) -> novarocks_table_maintenance::product::MaintenanceFuture<
        'a,
        Option<novarocks_coordination_contract::ExecutionEffect>,
    > {
        Box::pin(original_job_original_effect(self))
    }
    fn rebind_target<'a>(
        &'a self,
        job: &'a OptimizeJob,
    ) -> novarocks_table_maintenance::product::MaintenanceFuture<
        'a,
        Result<MaintenanceTargetRebind, String>,
    > {
        Box::pin(original_job_rebind_target(self, job))
    }

    fn execute<'a>(
        &'a self,
        job: &'a OptimizeJob,
        capacity: &'a novarocks_query_application::admitted_query_context::QueryResultCapacityBinding,
    ) -> novarocks_table_maintenance::product::MaintenanceFuture<
        'a,
        Result<MaintenanceActionOutcome, OptimizeTerminalError>,
    > {
        Box::pin(original_job_execute(self, job, capacity))
    }

    fn execute_automatic<'a>(
        &'a self,
        job: &'a OptimizeJob,
        effect_id: MaintenanceEffectId,
        capacity: &'a novarocks_query_application::admitted_query_context::QueryResultCapacityBinding,
    ) -> novarocks_table_maintenance::product::MaintenanceFuture<
        'a,
        Result<AutomaticMaintenanceOutcome, OptimizeTerminalError>,
    > {
        Box::pin(original_job_execute_automatic(
            self, job, effect_id, capacity,
        ))
    }
}

async fn original_job_run_original_job(
    this: Box<FrontendOptimizeJobExecution>,
    jobs: Arc<novarocks_table_maintenance::OptimizeProcessRuntime>,
    originals: OptimizeJobOriginals,
    job: OptimizeJob,
) -> Result<novarocks_table_maintenance::OptimizeJobOutcome, OptimizeTerminalError> {
    crate::query_execution::maintenance::rewrite_job::mount_original(this, jobs, originals, job)
        .await
}
async fn original_job_original_effect(
    this: &FrontendOptimizeJobExecution,
) -> Option<novarocks_coordination_contract::ExecutionEffect> {
    crate::query_execution::maintenance::rewrite_job::actual_original_effect(this.mounted.as_ref()?)
        .await
}
async fn original_job_rebind_target<'a>(
    this: &'a FrontendOptimizeJobExecution,
    job: &'a OptimizeJob,
) -> Result<MaintenanceTargetRebind, String> {
    #[cfg(novarocks_dev_controls)]
    if let Some(test) = &this.test {
        let capacity = this
            .engine
            .original_result_capacity()
            .cloned()
            .ok_or("Optimize MIO capacity is absent")?;
        let observation = test.owner.before_rebind(job, &capacity).await?;
        *test
            .observation
            .lock()
            .map_err(|_| "maintenance rebind observation lock failed")? = observation;
    }
    let original = this
        .mounted
        .as_ref()
        .ok_or("Optimize original background Source is not mounted")?;
    crate::query_execution::maintenance::rewrite_job::rebind_original(original, job).await
}
async fn original_job_execute<'a>(
    this: &'a FrontendOptimizeJobExecution,
    job: &'a OptimizeJob,
    capacity: &'a novarocks_query_application::admitted_query_context::QueryResultCapacityBinding,
) -> Result<MaintenanceActionOutcome, OptimizeTerminalError> {
    let original = this.mounted.as_ref().ok_or_else(|| {
        OptimizeTerminalError::pre_dispatch_failed(
            "Optimize original background Source is not mounted",
        )
    })?;
    #[cfg(novarocks_dev_controls)]
    this.record_test_dispatch(job, capacity)
        .map_err(OptimizeTerminalError::pre_dispatch_failed)?;
    this.product
        .execute_rewrite_terminal(
            &crate::query_execution::maintenance::rewrite_job::GovernedRewriteEffectPort::new(
                original.clone(),
            ),
            &job.target,
            RewriteIntent::DataFiles { rewrite_all: true },
        )
        .await
}
async fn original_job_execute_automatic<'a>(
    this: &'a FrontendOptimizeJobExecution,
    job: &'a OptimizeJob,
    effect_id: MaintenanceEffectId,
    capacity: &'a novarocks_query_application::admitted_query_context::QueryResultCapacityBinding,
) -> Result<AutomaticMaintenanceOutcome, OptimizeTerminalError> {
    let original = this.mounted.as_ref().ok_or_else(|| {
        OptimizeTerminalError::pre_dispatch_failed(
            "Optimize original background Source is not mounted",
        )
    })?;
    #[cfg(novarocks_dev_controls)]
    this.record_test_dispatch(job, capacity)
        .map_err(OptimizeTerminalError::pre_dispatch_failed)?;
    this.product
        .execute_automatic_rewrite_terminal(
            &crate::query_execution::maintenance::rewrite_job::GovernedRewriteEffectPort::new(
                original.clone(),
            ),
            &job.target,
            effect_id,
        )
        .await
}

#[cfg(test)]
mod rewrite_compile_projection_tests {
    use super::*;
    #[test]
    fn rewrite_preparation_keeps_original_source_and_control_without_text_guessing() {
        use novarocks_sql::compiler::SqlCompileError;
        use novarocks_type_contract::CompileControlError;
        let source = novarocks_failure_contract::SharedFailureCause::new(
            novarocks_local_compiler::FragmentCompileError::Unsupported {
                node: None,
                feature: "provider relation other than a table",
            },
        );
        let error = rewrite_compile_terminal(SqlCompileError::CompilationSource(source.clone()));
        assert_eq!(
            error.compile_control,
            Some(CompileControlError::Source(source.clone()))
        );
        assert_eq!(error.execution_failure, None);
        let plain = rewrite_compile_terminal(SqlCompileError::Compilation(source.to_string()));
        assert_eq!(plain.compile_control, None);
        for (compile, expected) in [
            (SqlCompileError::Cancelled, CompileControlError::Cancelled),
            (
                SqlCompileError::DeadlineExceeded,
                CompileControlError::DeadlineExceeded,
            ),
            (
                SqlCompileError::ResourceExhausted,
                CompileControlError::ResourceExhausted,
            ),
        ] {
            assert_eq!(
                rewrite_compile_terminal(compile).compile_control,
                Some(expected)
            );
        }
    }
}
