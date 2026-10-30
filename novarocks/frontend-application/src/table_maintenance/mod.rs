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

//! Frontend-owned, current-process table maintenance service.
//!
//! No table-maintenance job, operation, lease, checkpoint, or recovery fact
//! survives a frontend restart. The sole durable exception is the separately
//! named GC first-observation accelerator; it never owns a catalog mutation.

use std::future::Future;
use std::sync::Arc;
use std::time::Instant;

use novarocks_state_store_api::StateStore;
use novarocks_state_store_runtime::StateStoreRunPolicy;
use novarocks_workload_control::RootAdmissionHandle;
use tokio::runtime::Handle;

pub(crate) use self::admission::{
    ParsedMaintenanceAction, ParsedMaintenanceStatement, ParsedShowOptimize,
    lower_semantic_maintenance_statement, lower_semantic_show_optimize,
};
use self::result::{action_result, optimize_jobs_result};
use self::worker::{
    FrontendOptimizeJobAdmissionPort, FrontendOptimizeJobExecutionPort,
    FrontendOptimizeTargetCapturePort, LegacyMaintenanceEffects,
};
use crate::query_execution::maintenance::{
    MaintenanceRequestContext, MaintenanceStatementResult, TableMaintenanceEngine,
    TableMaintenanceService,
};
use novarocks_table_maintenance::product::TableMaintenanceProduct;
use novarocks_table_maintenance::runtime::{MaintenanceJobState, TerminalError};
use novarocks_table_maintenance::{
    MaintenanceActionRequest, MaintenanceTarget, OptimizeSubmission,
};

// Both public OPTIMIZE and automatic callers consume this same original
// record. They do not reconstruct its selected failure from a state label.
fn optimize_terminal_record_error(
    handle: novarocks_table_maintenance::runtime::JobHandle,
    job: novarocks_table_maintenance::OptimizeJob,
    owner: &str,
) -> TerminalError {
    let message = job.error_message.unwrap_or_else(|| {
        format!(
            "{owner} job {} ended in state {} without an error detail",
            handle.job_id(),
            job.state.as_str(),
        )
    });
    let state = match job.state {
        MaintenanceJobState::PreDispatchFailed
        | MaintenanceJobState::TargetReplaced
        | MaintenanceJobState::CancelledBeforeDispatch => MaintenanceJobState::KnownUncommitted,
        other => other,
    };
    TerminalError {
        effect: job.effect,
        state,
        message,
        compile_control: job.compile_control,
        execution_failure: job.execution_failure,
        failure_cause: job.failure_cause,
    }
}

pub(crate) fn optimize_statement_terminal_result(
    handle: novarocks_table_maintenance::runtime::JobHandle,
    job: novarocks_table_maintenance::OptimizeJob,
    cancelled: bool,
    cancellation_error: Option<String>,
) -> Result<(), TerminalError> {
    // The actual product chose this business result. Applied and NoOp successes
    // both survive a later observer stop or a failed cancellation request.
    if job.state == MaintenanceJobState::Finished {
        return Ok(());
    }
    let has_selected_cause = job.failure_cause.is_some()
        || job.execution_failure.is_some()
        || matches!(
            &job.compile_control,
            Some(novarocks_type_contract::CompileControlError::Source(_))
        );
    if has_selected_cause {
        return Err(optimize_terminal_record_error(handle, job, "optimize"));
    }
    // The observer waits for the original business conclusion. Actual Source
    // exit remains a separate fact; a stop never manufactures convergence.
    let caller_error = cancellation_error.or_else(|| {
        cancelled.then(|| {
            "OPTIMIZE statement cancelled; optimize job business result is available".to_string()
        })
    });
    if let Some(message) = caller_error {
        let mut error = TerminalError::from(message);
        error.effect = job.effect;
        // These are the actual publication owner's verdicts, never inferred
        // from Finished (which may be an effect-free plan) or from a refusal.
        if matches!(
            job.state,
            MaintenanceJobState::CommitUnknown
                | MaintenanceJobState::KnownCommittedFinalizationFailed
        ) {
            error.state = job.state;
        }
        return Err(error);
    }
    Err(optimize_terminal_record_error(handle, job, "optimize"))
}

pub mod admission;
pub mod result;
#[cfg(novarocks_dev_controls)]
pub(crate) mod test_controls;
pub mod worker;

// Design: ADR-0111 (docs/adr/ADR-0111-frontend-process-runtime-jobs-and-gc-observation-accelerator.md)
pub struct FrontendTableMaintenanceService {
    product: Arc<TableMaintenanceProduct>,
    runtime: Handle,
    root_admission: RootAdmissionHandle,
    #[cfg(novarocks_dev_controls)]
    controls: Option<Arc<test_controls::MaintenanceTestControls>>,
}

impl FrontendTableMaintenanceService {
    /// Opens the service.
    ///
    /// The store and the policy that governs its use arrive together, so a
    /// half-configured owner holding one without the other cannot be built.
    pub async fn open(
        durable: Option<(Arc<dyn StateStore>, StateStoreRunPolicy)>,
        runtime: Handle,
        root_admission: RootAdmissionHandle,
    ) -> Result<Self, String> {
        Self::open_inner(durable, runtime, root_admission).await
    }

    async fn open_inner(
        durable: Option<(Arc<dyn StateStore>, StateStoreRunPolicy)>,
        runtime: Handle,
        root_admission: RootAdmissionHandle,
    ) -> Result<Self, String> {
        let product = TableMaintenanceProduct::open(durable).await?;
        Ok(Self {
            product: Arc::new(product),
            runtime,
            root_admission,
            #[cfg(novarocks_dev_controls)]
            controls: None,
        })
    }

    pub fn with_lake_publication_runtime_policy(
        self,
        policy: novarocks_query_application::publication::LakePublicationRuntimePolicy,
    ) -> Self {
        Self {
            product: Arc::new(
                Arc::try_unwrap(self.product)
                    .unwrap_or_else(|_| {
                        panic!("table-maintenance product is uniquely configured before service publication")
                    })
                    .with_safe_gc_age(policy.safe_gc_age()),
            ),
            runtime: self.runtime,
            root_admission: self.root_admission,
            #[cfg(novarocks_dev_controls)]
            controls: self.controls,
        }
    }

    #[cfg(novarocks_dev_controls)]
    pub(crate) fn with_test_controls(
        mut self,
        controls: Option<Arc<test_controls::MaintenanceTestControls>>,
    ) -> Self {
        self.controls = controls;
        self
    }

    // This synchronous helper retains the receipt only across the existing
    // block_on submission; no non-Send mutex guard crosses an async await.
    fn submit_statement_optimize(
        &self,
        engine: &dyn TableMaintenanceEngine,
        target: MaintenanceTarget,
    ) -> Result<OptimizeSubmission, String> {
        let capture = FrontendOptimizeTargetCapturePort::for_statement(engine);
        #[cfg(novarocks_dev_controls)]
        let receipt = match self.controls.as_ref() {
            Some(owner) => match engine.test_statement_nomination() {
                Some(nomination) => owner.begin_submission(&nomination, &target)?,
                None => None,
            },
            None => None,
        };
        #[cfg(novarocks_dev_controls)]
        let submission = {
            let capture = capture.with_test_receipt(receipt.as_ref());
            self.block_on(self.product.submit_optimize(target, &capture))
        };
        #[cfg(not(novarocks_dev_controls))]
        let submission = self.block_on(self.product.submit_optimize(target, &capture));
        #[cfg(novarocks_dev_controls)]
        {
            if let (Some(receipt), Ok(OptimizeSubmission::Submitted { job_id, .. })) =
                (receipt, &submission)
            {
                receipt.bind(*job_id)?;
            }
        }
        submission
    }

    fn block_on<F: Future>(&self, future: F) -> F::Output {
        if Handle::try_current().is_ok() {
            tokio::task::block_in_place(|| self.runtime.block_on(future))
        } else {
            self.runtime.block_on(future)
        }
    }

    fn show_optimize(
        &self,
        statement: ParsedShowOptimize,
        context: MaintenanceRequestContext<'_>,
    ) -> Result<MaintenanceStatementResult, String> {
        let mut jobs = self.block_on(self.product.list_optimize())?;
        if let Some(catalog) = statement.catalog.as_deref().or(context.current_catalog) {
            jobs.retain(|job| job.target.catalog == catalog);
        }
        jobs.retain(|job| {
            job.target.namespace
                == statement
                    .database
                    .as_deref()
                    .unwrap_or(context.current_database)
        });
        if let Some(table) = statement.table_name.as_deref() {
            jobs.retain(|job| job.target.table == table);
        }
        if statement.order_by_create_time_desc {
            jobs.sort_by_key(|job| std::cmp::Reverse(job.created_at_ms));
        }
        if let Some(limit) = statement.limit {
            jobs.truncate(limit);
        }
        optimize_jobs_result(jobs)
    }

    pub(crate) async fn shutdown_until(&self, deadline: Instant) -> Result<(), String> {
        self.product.shutdown_until(deadline).await
    }

    pub(crate) fn request_shutdown_for_process_exit(&self) {
        self.product.request_shutdown_for_process_exit();
    }
}

#[async_trait::async_trait]
impl TableMaintenanceService for FrontendTableMaintenanceService {
    fn original_optimize_host(
        &self,
    ) -> Result<crate::query_execution::maintenance::OriginalOptimizeHost, String> {
        Ok(crate::query_execution::maintenance::OriginalOptimizeHost {
            product: self.product.clone(),
            roots: self.root_admission.clone(),
            #[cfg(novarocks_dev_controls)]
            controls: self.controls.clone(),
        })
    }
    fn start(&self, engine: Arc<dyn TableMaintenanceEngine>) -> Result<(), String> {
        self.product.start(
            &self.runtime,
            Arc::new(FrontendOptimizeJobAdmissionPort::new(
                self.root_admission.clone(),
                Arc::downgrade(&engine),
            )),
            Arc::new(FrontendOptimizeJobExecutionPort::new(
                Arc::downgrade(&engine),
                Arc::downgrade(&self.product),
                #[cfg(novarocks_dev_controls)]
                self.controls.clone(),
            )),
        )
    }

    async fn handle_typed_statement(
        &self,
        engine: &dyn TableMaintenanceEngine,
        state: &crate::query_execution::maintenance::MaintenanceActionState,
        statement: ParsedMaintenanceStatement,
        spark_procedure: bool,
        context: MaintenanceRequestContext<'_>,
    ) -> Result<MaintenanceStatementResult, TerminalError> {
        match statement {
            ParsedMaintenanceStatement::Execute { name_parts, action } => {
                let target = engine.resolve_target(&name_parts, context)?;
                let request = action.into_request(engine, target)?;
                let outcome = self
                    .product
                    .execute_user_action(&LegacyMaintenanceEffects::new(engine, state), request)
                    .await?;
                if spark_procedure {
                    action_result(outcome).map_err(TerminalError::from)
                } else {
                    Ok(MaintenanceStatementResult::Ok)
                }
            }
            ParsedMaintenanceStatement::SubmitOptimize { name_parts } => {
                let target = engine.resolve_target(&name_parts, context)?;
                engine.reject_user_action_on_mv(&target).await.map_err(
                    crate::query_execution::maintenance::rewrite_job::guard_terminal_error,
                )?;
                let submission = self.submit_statement_optimize(engine, target)?;
                if matches!(
                    submission,
                    OptimizeSubmission::Submitted {
                        completion:
                            novarocks_table_maintenance::OptimizeCompletionMode::AwaitTerminal,
                        ..
                    }
                ) && let Some(handle) = submission.handle()
                {
                    let control = engine
                        .statement_cancellation()
                        .expect("AwaitTerminal verified statement control before submission");
                    let wait = self.product.wait_optimize_terminal_record(handle);
                    tokio::pin!(wait);
                    let mut cancelled = false;
                    let mut cancellation_error = None;
                    let terminal = tokio::select! {
                        terminal = &mut wait => terminal,
                        _ = control.0.stopped() => {
                            cancelled = true;
                            cancellation_error = self.product.request_cancel_optimize(handle).await.err();
                            wait.await
                        },
                        _ = tokio::time::sleep_until(control.1.into()) => {
                            cancelled = true;
                            cancellation_error = self.product.request_cancel_optimize(handle).await.err();
                            wait.await
                        },
                    }?;
                    optimize_statement_terminal_result(
                        handle,
                        terminal,
                        cancelled,
                        cancellation_error,
                    )?;
                }
                Ok(MaintenanceStatementResult::Ok)
            }
            ParsedMaintenanceStatement::ShowOptimize => Err(
                "SHOW ALTER TABLE OPTIMIZE belongs to the read-only maintenance owner"
                    .to_string()
                    .into(),
            ),
        }
    }

    fn handle_typed_show_optimize(
        &self,
        statement: ParsedShowOptimize,
        context: MaintenanceRequestContext<'_>,
    ) -> Result<MaintenanceStatementResult, String> {
        self.show_optimize(statement, context)
    }

    async fn shutdown_until(&self, deadline: Instant) -> Result<(), String> {
        FrontendTableMaintenanceService::shutdown_until(self, deadline).await
    }

    fn request_shutdown_for_process_exit(&self) {
        FrontendTableMaintenanceService::request_shutdown_for_process_exit(self);
    }
}

impl ParsedMaintenanceAction {
    fn into_request(
        self,
        engine: &dyn TableMaintenanceEngine,
        target: MaintenanceTarget,
    ) -> Result<MaintenanceActionRequest, String> {
        match self {
            Self::RewriteDataFiles {
                options,
                branch,
                where_clause,
            } => Ok(MaintenanceActionRequest::RewriteDataFiles {
                target: target.clone(),
                base_snapshot_id: engine.current_snapshot_id(&target)?,
                job_id: None,
                options,
                branch,
                where_clause,
            }),
            Self::RewriteManifests {
                use_caching,
                spec_id,
            } => Ok(MaintenanceActionRequest::RewriteManifests {
                target,
                use_caching,
                spec_id,
            }),
            Self::ExpireSnapshots {
                older_than_ms,
                retain_last,
            } => Ok(MaintenanceActionRequest::ExpireSnapshots {
                target,
                older_than_ms,
                retain_last,
            }),
            Self::RemoveOrphanFiles { older_than_ms } => {
                Ok(MaintenanceActionRequest::RemoveOrphanFiles {
                    target,
                    older_than_ms,
                })
            }
            Self::RewritePositionDeleteFiles {
                options,
                where_clause,
            } => Ok(MaintenanceActionRequest::RewritePositionDeleteFiles {
                target,
                options,
                where_clause,
            }),
        }
    }
}

#[cfg(test)]
mod optimize_terminal_record_projection_tests {
    use super::*;
    use novarocks_table_maintenance::runtime::JobHandle;
    fn record(state: MaintenanceJobState) -> novarocks_table_maintenance::OptimizeJob {
        novarocks_table_maintenance::runtime::JobRecord {
            effect: Some(novarocks_coordination_contract::ExecutionEffect::None),
            job_id: 71,
            target: MaintenanceTarget {
                catalog: "ice".into(),
                namespace: "db".into(),
                table: "orders".into(),
            },
            effect_id: None,
            object_id: vec![1],
            base_snapshot_id: 2,
            state,
            outcome: None,
            error_message: Some("original owner failure".into()),
            compile_control: None,
            execution_failure: None,
            failure_cause: None,
            created_at_ms: 1,
            started_at_ms: Some(2),
            finished_at_ms: Some(3),
        }
    }
    #[test]
    fn ordinary_optimize_consumes_original_selected_record_even_after_local_cancel() {
        let input =
            novarocks_query_application::session_error::QueryServiceError::from_compile_control(
                novarocks_type_contract::CompileControlError::ResourceExhausted,
            )
            .execution_failure()
            .unwrap()
            .clone();
        for cancelled in [false, true] {
            let mut job = record(MaintenanceJobState::CommitUnknown);
            job.execution_failure = Some(input.clone());
            let error =
                optimize_statement_terminal_result(JobHandle::new(71), job, cancelled, None)
                    .unwrap_err();
            assert_eq!(error.execution_failure, Some(input.clone()));
            assert_eq!(error.state, MaintenanceJobState::CommitUnknown);
            assert_eq!(error.message, "original owner failure");
        }
    }
    #[test]
    fn ordinary_optimize_keeps_caller_fallback_and_real_publication_without_inventing_from_finished()
     {
        let error = optimize_statement_terminal_result(
            JobHandle::new(71),
            record(MaintenanceJobState::CommitUnknown),
            true,
            None,
        )
        .unwrap_err();
        assert_eq!(error.execution_failure, None);
        assert_eq!(error.state, MaintenanceJobState::CommitUnknown);
        assert_eq!(
            error.message,
            "OPTIMIZE statement cancelled; optimize job business result is available"
        );
        assert!(
            optimize_statement_terminal_result(
                JobHandle::new(71),
                record(MaintenanceJobState::Finished),
                false,
                None
            )
            .is_ok()
        );
        for effect in [
            Some(novarocks_coordination_contract::ExecutionEffect::External),
            Some(novarocks_coordination_contract::ExecutionEffect::None),
        ] {
            let mut job = record(MaintenanceJobState::Finished);
            job.effect = effect;
            assert!(
                optimize_statement_terminal_result(
                    JobHandle::new(71),
                    job,
                    true,
                    Some("late cancellation request failed".into()),
                )
                .is_ok()
            );
        }
    }
    #[test]
    fn ordinary_optimize_original_source_precedes_late_cancel_request_error() {
        use novarocks_type_contract::CompileControlError;
        let source = novarocks_failure_contract::SharedFailureCause::new(
            novarocks_local_compiler::FragmentCompileError::Unsupported {
                node: None,
                feature: "provider relation other than a table",
            },
        );
        let mut job = record(MaintenanceJobState::KnownUncommitted);
        job.compile_control = Some(CompileControlError::Source(source.clone()));
        let error = optimize_statement_terminal_result(
            JobHandle::new(71),
            job,
            true,
            Some("late cancellation request failed".into()),
        )
        .unwrap_err();
        assert_eq!(
            error.compile_control,
            Some(CompileControlError::Source(source))
        );
        assert_eq!(error.state, MaintenanceJobState::KnownUncommitted);
        assert_eq!(error.message, "original owner failure");
    }
}
