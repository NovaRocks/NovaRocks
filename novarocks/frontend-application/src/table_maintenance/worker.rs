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

use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorCleanupCandidate, ConnectorCleanupOperationId, ConnectorCleanupOwnedRefSelection,
    ConnectorTableObjectId, ConnectorWriteOperationId, ExternalMutationFinalization,
    ExternalMutationOutcome,
};
use novarocks_table_maintenance::job_service::{CapturedOptimizeTarget, OptimizeTargetCapturePort};
use novarocks_table_maintenance::product::TableMaintenanceProduct;
use novarocks_table_maintenance::product::{
    CleanupCandidate, CleanupOwnedRefFact, CleanupSession, CleanupTerminal,
    DistributedRewriteSession, RewriteCommit, RewriteIntent, RewritePlanFacts, RewriteReceiptFacts,
    TableMaintenanceEffectPort,
};
use novarocks_table_maintenance::runtime::TerminalError as OptimizeTerminalError;
use novarocks_table_maintenance::worker::{
    OptimizeJobAdmission, OptimizeJobAdmissionPort, OptimizeJobExecution, OptimizeJobExecutionPort,
    OptimizeJobScope,
};
use novarocks_table_maintenance::{
    AutomaticMaintenanceOutcome, MaintenanceActionOutcome, MaintenanceActionRequest,
    MaintenanceEffectId, MaintenanceTarget, MaintenanceTargetRebind, OptimizeJob,
};
use novarocks_workload_control::{
    QueryConcurrencyPermit, RootAdmissionHandle, WorkClass, WorkError, WorkOwner, WorkRequest,
};

use crate::query_execution::maintenance::TableMaintenanceEngine;

use crate::connector::distributed_rewrite_application::DistributedRewriteIntent;

/// Frontend provider/native-query adapter for the neutral maintenance product.
/// It keeps opaque connector sessions and fragment encoding here, while the
/// product crate owns all target-gate, cohort and terminal business decisions.
pub(crate) struct FrontendMaintenanceEffectPort<'a> {
    engine: &'a dyn TableMaintenanceEngine,
}

impl<'a> FrontendMaintenanceEffectPort<'a> {
    pub(crate) fn new(engine: &'a dyn TableMaintenanceEngine) -> Self {
        Self { engine }
    }
}

impl TableMaintenanceEffectPort for FrontendMaintenanceEffectPort<'_> {
    fn reject_user_action_on_mv(&self, target: &MaintenanceTarget) -> Result<(), String> {
        self.engine.reject_user_action_on_mv(target)
    }

    fn execute_metadata(
        &self,
        request: MaintenanceActionRequest,
    ) -> Result<MaintenanceActionOutcome, String> {
        self.engine.execute_action(request)
    }

    fn execute_metadata_with_id(
        &self,
        request: MaintenanceActionRequest,
        effect_id: MaintenanceEffectId,
    ) -> Result<AutomaticMaintenanceOutcome, OptimizeTerminalError> {
        self.engine
            .execute_automatic_metadata_action(request, effect_id)
    }

    fn begin_rewrite<'a>(
        &'a self,
        target: &MaintenanceTarget,
        intent: RewriteIntent,
    ) -> Result<Box<dyn DistributedRewriteSession + 'a>, String> {
        let intent = match intent {
            RewriteIntent::DataFiles { rewrite_all } => {
                DistributedRewriteIntent::DataFiles { rewrite_all }
            }
            RewriteIntent::PositionDeletes {
                rewrite_all,
                min_input_files,
            } => DistributedRewriteIntent::PositionDeletes {
                rewrite_all,
                min_input_files,
            },
        };
        let session = self.engine.plan_distributed_rewrite(
            target,
            ConnectorWriteOperationId::new(),
            intent,
        )?;
        Ok(Box::new(FrontendDistributedRewriteSession {
            engine: self.engine,
            session,
            committed_receipt: None,
        }))
    }

    fn begin_rewrite_with_id<'a>(
        &'a self,
        target: &MaintenanceTarget,
        intent: RewriteIntent,
        effect_id: MaintenanceEffectId,
    ) -> Result<Box<dyn DistributedRewriteSession + 'a>, OptimizeTerminalError> {
        let intent = match intent {
            RewriteIntent::DataFiles { rewrite_all } => {
                DistributedRewriteIntent::DataFiles { rewrite_all }
            }
            RewriteIntent::PositionDeletes {
                rewrite_all,
                min_input_files,
            } => DistributedRewriteIntent::PositionDeletes {
                rewrite_all,
                min_input_files,
            },
        };
        let session = self
            .engine
            .plan_distributed_rewrite(
                target,
                ConnectorWriteOperationId::from_bytes(effect_id.to_bytes()),
                intent,
            )
            .map_err(OptimizeTerminalError::pre_dispatch_failed)?;
        Ok(Box::new(FrontendDistributedRewriteSession {
            engine: self.engine,
            session,
            committed_receipt: None,
        }))
    }

    fn begin_cleanup<'a>(
        &'a self,
        target: &MaintenanceTarget,
        older_than_ms: i64,
    ) -> Result<Box<dyn CleanupSession + 'a>, String> {
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
        }))
    }
}

struct FrontendDistributedRewriteSession<'a> {
    engine: &'a dyn TableMaintenanceEngine,
    session: crate::query_execution::distributed_rewrite::DistributedRewriteMaintenanceSession,
    committed_receipt: Option<novarocks_spi::connector::ConnectorWriteReceipt>,
}

impl DistributedRewriteSession for FrontendDistributedRewriteSession<'_> {
    fn plan_facts(&self) -> RewritePlanFacts {
        RewritePlanFacts {
            noop: self.session.is_noop(),
            cohort_count: self.session.plan().cohorts().len(),
            input_bytes: self.session.plan().summary().input_bytes,
        }
    }

    fn execute_cohort(&mut self, ordinal: usize) -> Result<(), String> {
        let cohort =
            self.session.plan().cohorts().get(ordinal).ok_or_else(|| {
                format!("rewrite cohort ordinal {ordinal} is not in the frozen plan")
            })?;
        let prepared = self
            .engine
            .prepare_distributed_rewrite_cohort(&self.session, cohort.cohort_id())?;
        let completion = prepared.finish()?;
        self.engine
            .accumulate_distributed_rewrite_group(&self.session, completion)
    }

    fn commit(&mut self) -> Result<RewriteCommit, String> {
        match self.engine.commit_distributed_rewrite(&self.session)? {
            ExternalMutationOutcome::KnownCommitted {
                receipt,
                finalization,
                ..
            } => {
                self.committed_receipt = Some(receipt);
                Ok(RewriteCommit::KnownCommitted {
                    finalization_failed: match finalization {
                        ExternalMutationFinalization::Failed(error) => Some(error.to_string()),
                        _ => None,
                    },
                })
            }
            ExternalMutationOutcome::KnownUncommitted { failure } => {
                Ok(RewriteCommit::KnownUncommitted {
                    failure: failure.to_string(),
                })
            }
            ExternalMutationOutcome::CommitUnknown { failure, .. } => {
                Ok(RewriteCommit::CommitUnknown {
                    failure: failure.to_string(),
                })
            }
        }
    }

    fn finalize_committed(&mut self) -> Result<RewriteReceiptFacts, String> {
        let receipt = self.committed_receipt.as_ref().ok_or_else(|| {
            "distributed rewrite has no committed receipt to finalize".to_string()
        })?;
        let receipt = self
            .engine
            .finalize_distributed_rewrite(&self.session, receipt)?;
        let summary = receipt.summary();
        Ok(RewriteReceiptFacts {
            target_snapshot_id: summary.target_version,
            input_data_files: summary.input_data_files,
            input_delete_files: summary.input_delete_files,
            output_data_files: summary.output_data_files,
            output_delete_files: summary.output_delete_files,
            output_rows: summary.output_rows,
        })
    }

    fn abort(&mut self, _reason: String) -> Result<(), String> {
        self.engine
            .abort_distributed_rewrite(&self.session)
            .map(|_| ())
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
    fn candidates(&self) -> &[CleanupCandidate] {
        &self.candidates
    }

    fn select_owned_refs(&mut self, candidate_indexes: &[usize]) -> Result<(), String> {
        let identities = candidate_indexes
            .iter()
            .map(|index| {
                self.raw_candidates
                    .get(*index)
                    .and_then(ConnectorCleanupCandidate::owned_ref_identity)
                    .ok_or_else(|| "owned-ref candidate has no valid exact identity".to_string())
            })
            .collect::<Result<Vec<_>, String>>()?;
        let selection = ConnectorCleanupOwnedRefSelection::try_new(identities)
            .map_err(|error| format!("build mature owned-ref cleanup selection failed: {error}"))?;
        self.session = self.engine.plan_selected_owned_ref_cleanup_maintenance(
            &self.target,
            ConnectorCleanupOperationId::new(),
            self.older_than_ms,
            selection,
        )?;
        Ok(())
    }

    fn execute(&mut self) -> Result<CleanupTerminal, String> {
        let batches = self.session.plan_ref().summary().batch_count();
        for ordinal in 0..batches {
            let prepared = self.engine.prepare_cleanup_batch(&self.session, ordinal)?;
            match self.engine.execute_cleanup_batch(&self.session, prepared)? {
                crate::connector::cleanup_maintenance::CleanupBatchExecution::Receipt(receipt) => {
                    if receipt.summary().unknown() != 0 {
                        return Ok(CleanupTerminal::CommitUnknown {
                            failure: "provider reported an unknown cleanup batch outcome"
                                .to_string(),
                        });
                    }
                }
                crate::connector::cleanup_maintenance::CleanupBatchExecution::Uncertain(error) => {
                    return Ok(CleanupTerminal::CommitUnknown {
                        failure: error.to_string(),
                    });
                }
            }
        }
        let locations = cleanup_candidate_locations(self.engine, &self.session)?;
        match self.engine.finalize_cleanup_terminal(&self.session) {
            Ok(()) => Ok(CleanupTerminal::KnownCommitted { locations }),
            Err(error) => Ok(CleanupTerminal::KnownCommittedFinalizationFailed { failure: error }),
        }
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

fn cleanup_candidate_locations(
    engine: &dyn TableMaintenanceEngine,
    session: &crate::connector::cleanup_maintenance::CleanupMaintenanceSession,
) -> Result<Vec<String>, String> {
    let mut offset = 0_u64;
    let mut locations = Vec::new();
    loop {
        let page = engine.read_cleanup_candidate_page(session, offset, 1024)?;
        locations.extend(page.display_keys().iter().map(ToString::to_string));
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
}

impl<'a> FrontendOptimizeTargetCapturePort<'a> {
    pub(crate) fn new(engine: &'a dyn TableMaintenanceEngine) -> Self {
        Self { engine }
    }
}

impl OptimizeTargetCapturePort for FrontendOptimizeTargetCapturePort<'_> {
    fn capture(
        &self,
        target: &novarocks_table_maintenance::MaintenanceTarget,
    ) -> Result<CapturedOptimizeTarget, String> {
        let object_id = self.engine.capture_target_object_id(target)?;
        Ok(CapturedOptimizeTarget {
            object_id: object_id.as_bytes().to_vec(),
            base_snapshot_id: self.engine.current_snapshot_id(target)?,
        })
    }
}

pub(crate) struct FrontendOptimizeJobAdmissionPort {
    root_admission: RootAdmissionHandle,
}

impl FrontendOptimizeJobAdmissionPort {
    pub(crate) fn new(root_admission: RootAdmissionHandle) -> Self {
        Self { root_admission }
    }
}

#[async_trait::async_trait]
impl OptimizeJobAdmissionPort for FrontendOptimizeJobAdmissionPort {
    async fn begin(&self) -> Result<OptimizeJobAdmission, String> {
        let root = match self
            .root_admission
            .begin_warehouse_root(WorkRequest::new(WorkClass::TableMaintenance))
        {
            Ok(root) => root,
            Err(WorkError::NotReady)
            | Err(WorkError::Capacity(_))
            | Err(WorkError::CapacityWaitTimeout) => return Ok(OptimizeJobAdmission::RetryLater),
            Err(WorkError::Closed) => return Ok(OptimizeJobAdmission::Closed),
            Err(error) => return Err(format!("admit governed optimize root failed: {error}")),
        };
        let permit = match root.owner.scope().admit_query() {
            Ok(admission) => match admission.await {
                Ok(permit) => permit,
                Err(WorkError::Cancelled(_))
                | Err(WorkError::Capacity(_))
                | Err(WorkError::CapacityWaitTimeout) => {
                    return Ok(OptimizeJobAdmission::RetryLater);
                }
                Err(WorkError::Closed) => return Ok(OptimizeJobAdmission::Closed),
                Err(error) => {
                    return Err(format!("admit warehouse optimize query failed: {error}"));
                }
            },
            Err(WorkError::Closed) => return Ok(OptimizeJobAdmission::Closed),
            Err(error) => {
                return Err(format!(
                    "register warehouse optimize admission failed: {error}"
                ));
            }
        };
        Ok(OptimizeJobAdmission::Acquired(Box::new(
            FrontendOptimizeJobScope::new(root.owner, permit),
        )))
    }
}

struct FrontendOptimizeJobScope {
    work: Mutex<Option<AdmittedOptimizeJob>>,
}

struct AdmittedOptimizeJob {
    owner: WorkOwner,
    query_concurrency: QueryConcurrencyPermit,
}

impl FrontendOptimizeJobScope {
    fn new(owner: WorkOwner, query_concurrency: QueryConcurrencyPermit) -> Self {
        Self {
            work: Mutex::new(Some(AdmittedOptimizeJob {
                owner,
                query_concurrency,
            })),
        }
    }
}

impl OptimizeJobScope for FrontendOptimizeJobScope {
    fn is_cancelled(&self) -> Result<bool, String> {
        let work = self
            .work
            .lock()
            .map_err(|error| format!("lock governed optimize root scope: {error}"))?;
        let work = work
            .as_ref()
            .ok_or_else(|| "governed optimize root scope was released".to_string())?;
        let cancellation =
            work.owner.scope().cancellation().map_err(|error| {
                format!("observe governed optimize cancellation failed: {error}")
            })?;
        Ok(cancellation.reason().is_some())
    }
}

impl Drop for FrontendOptimizeJobScope {
    fn drop(&mut self) {
        let Some(AdmittedOptimizeJob {
            owner,
            query_concurrency,
        }) = self
            .work
            .get_mut()
            .unwrap_or_else(|error| error.into_inner())
            .take()
        else {
            return;
        };
        drop(query_concurrency);
        owner.complete();
    }
}

pub(crate) struct FrontendOptimizeJobExecutionPort {
    engine: Weak<dyn TableMaintenanceEngine>,
    product: Weak<TableMaintenanceProduct>,
}

impl FrontendOptimizeJobExecutionPort {
    pub(crate) fn new(
        engine: Weak<dyn TableMaintenanceEngine>,
        product: Weak<TableMaintenanceProduct>,
    ) -> Self {
        Self { engine, product }
    }
}

impl OptimizeJobExecutionPort for FrontendOptimizeJobExecutionPort {
    fn is_available(&self) -> bool {
        self.engine.strong_count() != 0 && self.product.strong_count() != 0
    }

    fn acquire(&self) -> Option<Box<dyn OptimizeJobExecution>> {
        self.engine
            .upgrade()
            .zip(self.product.upgrade())
            .map(|(engine, product)| {
                Box::new(FrontendOptimizeJobExecution { engine, product })
                    as Box<dyn OptimizeJobExecution>
            })
    }
}

struct FrontendOptimizeJobExecution {
    engine: Arc<dyn TableMaintenanceEngine>,
    product: Arc<TableMaintenanceProduct>,
}

impl OptimizeJobExecution for FrontendOptimizeJobExecution {
    fn rebind_target(&self, job: &OptimizeJob) -> Result<MaintenanceTargetRebind, String> {
        stat2f_before_rebind_barrier(job.job_id)?;
        let expected_object_id = ConnectorTableObjectId::try_new(Bytes::copy_from_slice(
            &job.object_id,
        ))
        .map_err(|error| {
            format!(
                "restore optimize job {} target object ID failed: {error}",
                job.job_id
            )
        })?;
        self.engine
            .rebind_target_object(&job.target, &expected_object_id)
    }

    fn execute(
        &self,
        job: &OptimizeJob,
    ) -> Result<MaintenanceActionOutcome, OptimizeTerminalError> {
        stat2f_record_provider_dispatch(job.job_id).map_err(OptimizeTerminalError::failed)?;
        let _diagnostic_scope = crate::preparation_diagnostics::enter_product_work(
            format!("maintenance-job:{}", job.job_id),
            format!("maintenance-job:{}", job.job_id),
        );
        self.product.execute_rewrite_terminal(
            &FrontendMaintenanceEffectPort::new(self.engine.as_ref()),
            &job.target,
            RewriteIntent::DataFiles { rewrite_all: true },
        )
    }

    fn execute_automatic(
        &self,
        job: &OptimizeJob,
        effect_id: MaintenanceEffectId,
    ) -> Result<AutomaticMaintenanceOutcome, OptimizeTerminalError> {
        stat2f_record_provider_dispatch(job.job_id)
            .map_err(OptimizeTerminalError::pre_dispatch_failed)?;
        let _diagnostic_scope = crate::preparation_diagnostics::enter_product_work(
            format!("maintenance-job:{}", job.job_id),
            format!("maintenance-job:{}", job.job_id),
        );
        self.product.execute_automatic_rewrite_terminal(
            &FrontendMaintenanceEffectPort::new(self.engine.as_ref()),
            &job.target,
            effect_id,
        )
    }
}

/// Runner-owned test root for the STAT-2F cross-process maintenance race.
#[cfg(debug_assertions)]
const STAT2F_TEST_ROOT_ENV: &str = "NOVAROCKS_STAT2F_MAINTENANCE_TEST_DIR";
#[cfg(debug_assertions)]
const STAT2F_TEST_BARRIER_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

#[cfg(debug_assertions)]
fn stat2f_before_rebind_barrier(job_id: i64) -> Result<(), String> {
    use std::time::Instant;

    let Some(root) = std::env::var_os(STAT2F_TEST_ROOT_ENV) else {
        return Ok(());
    };
    let paths = stat2f_test_paths(std::path::Path::new(&root), job_id);
    if paths.resume.exists() {
        return Err(format!(
            "STAT-2F maintenance test resume trigger already exists: {}",
            paths.resume.display()
        ));
    }
    std::fs::write(&paths.dispatch_count, "0\n").map_err(|error| {
        format!(
            "write STAT-2F maintenance dispatch counter {}: {error}",
            paths.dispatch_count.display()
        )
    })?;
    std::fs::write(
        &paths.ready,
        format!("job_id={job_id}\nphase=after-claim-before-rebind\n"),
    )
    .map_err(|error| {
        format!(
            "write STAT-2F maintenance ready marker {}: {error}",
            paths.ready.display()
        )
    })?;

    let deadline = Instant::now() + STAT2F_TEST_BARRIER_TIMEOUT;
    while !paths.resume.exists() && Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    if paths.resume.exists() {
        Ok(())
    } else {
        Err(format!(
            "timed out waiting for STAT-2F maintenance resume trigger {}",
            paths.resume.display()
        ))
    }
}

#[cfg(not(debug_assertions))]
fn stat2f_before_rebind_barrier(_job_id: i64) -> Result<(), String> {
    Ok(())
}

#[cfg(debug_assertions)]
fn stat2f_record_provider_dispatch(job_id: i64) -> Result<(), String> {
    let Some(root) = std::env::var_os(STAT2F_TEST_ROOT_ENV) else {
        return Ok(());
    };
    let paths = stat2f_test_paths(std::path::Path::new(&root), job_id);
    let previous = std::fs::read_to_string(&paths.dispatch_count).map_err(|error| {
        format!(
            "read STAT-2F maintenance dispatch counter {}: {error}",
            paths.dispatch_count.display()
        )
    })?;
    let previous = previous.trim().parse::<u64>().map_err(|error| {
        format!(
            "parse STAT-2F maintenance dispatch counter {}: {error}",
            paths.dispatch_count.display()
        )
    })?;
    std::fs::write(&paths.dispatch_count, format!("{}\n", previous + 1)).map_err(|error| {
        format!(
            "write STAT-2F maintenance dispatch counter {}: {error}",
            paths.dispatch_count.display()
        )
    })
}

#[cfg(not(debug_assertions))]
fn stat2f_record_provider_dispatch(_job_id: i64) -> Result<(), String> {
    Ok(())
}

#[cfg(debug_assertions)]
struct Stat2fTestPaths {
    ready: std::path::PathBuf,
    resume: std::path::PathBuf,
    dispatch_count: std::path::PathBuf,
}

#[cfg(debug_assertions)]
fn stat2f_test_paths(root: &std::path::Path, job_id: i64) -> Stat2fTestPaths {
    let stem = format!("stat2f-maintenance-optimize-{job_id}");
    Stat2fTestPaths {
        ready: root.join(format!("{stem}.before-rebind.ready")),
        resume: root.join(format!("{stem}.before-rebind.resume")),
        dispatch_count: root.join(format!("{stem}.dispatch-count")),
    }
}

#[cfg(all(test, debug_assertions))]
mod stat2f_test_hook_tests {
    use std::ffi::OsString;
    use std::sync::{Mutex, OnceLock};
    use std::time::{Duration, Instant};

    use tempfile::TempDir;

    use super::{
        STAT2F_TEST_ROOT_ENV, stat2f_before_rebind_barrier, stat2f_record_provider_dispatch,
        stat2f_test_paths,
    };

    static TEST_ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    struct ScopedTestEnv {
        prior: Option<OsString>,
    }

    impl ScopedTestEnv {
        fn set(root: &std::path::Path) -> Self {
            let prior = std::env::var_os(STAT2F_TEST_ROOT_ENV);
            // The process-global environment is serialized by TEST_ENV_LOCK.
            unsafe { std::env::set_var(STAT2F_TEST_ROOT_ENV, root) };
            Self { prior }
        }
    }

    impl Drop for ScopedTestEnv {
        fn drop(&mut self) {
            // The process-global environment is serialized by TEST_ENV_LOCK.
            unsafe {
                if let Some(prior) = self.prior.take() {
                    std::env::set_var(STAT2F_TEST_ROOT_ENV, prior);
                } else {
                    std::env::remove_var(STAT2F_TEST_ROOT_ENV);
                }
            }
        }
    }

    #[test]
    fn barrier_reports_zero_then_one_dispatch_and_resumes_only_on_trigger() {
        let _environment = TEST_ENV_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("lock STAT-2F test environment");
        let temporary = TempDir::new().expect("create test root");
        let root = temporary.path().join("stat2f-hook");
        std::fs::create_dir(&root).expect("create hook directory");
        let _scope = ScopedTestEnv::set(&root);
        let job_id = 19;
        let paths = stat2f_test_paths(&root, job_id);

        let waiter = std::thread::spawn(move || stat2f_before_rebind_barrier(job_id));
        let deadline = Instant::now() + Duration::from_secs(1);
        while !paths.ready.exists() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(paths.ready.exists(), "the barrier exposes its ready marker");
        assert_eq!(
            std::fs::read_to_string(&paths.dispatch_count).unwrap(),
            "0\n"
        );
        std::fs::write(&paths.resume, "resume\n").expect("release barrier");
        waiter
            .join()
            .expect("barrier joins")
            .expect("barrier resumes");
        stat2f_record_provider_dispatch(job_id).expect("record first provider dispatch");
        assert_eq!(
            std::fs::read_to_string(&paths.dispatch_count).unwrap(),
            "1\n"
        );
    }
}
