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
use novarocks_coordination_contract::ExecutionEffect;
use novarocks_execution_contract::failure::FailureProjection;
use novarocks_table_maintenance::{
    MaintenanceTargetRebind,
    activity::MaintenanceActivityFamily,
    product::{
        CleanupSession, DistributedRewriteSession, MaintenanceFuture, RewriteAbort, RewriteCommit,
        RewriteFailure, RewriteIntent, RewritePlanFacts, RewriteReceiptFacts,
        TableMaintenanceEffectPort, TableMaintenanceProduct,
    },
    runtime::{JobCreate, MaintenanceJobState},
    worker::{OptimizeJobExecution, execute_claimed_job},
};
use novarocks_workload_control::{
    ResourceConfig, ResultCapacityConfig, ResultWindowClass, WorkClass, WorkRequest,
    WorkloadConfig, WorkloadControl,
};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

// A Provider boundary fixture, not a fabricated QA Ready/exit capability.
// The actual Product must call its necessary finalizer and cannot return while
// that original borrowed call is Pending.
struct Finalizer {
    allow: AtomicBool,
    calls: AtomicUsize,
    cohorts: AtomicUsize,
    outcome: ProviderOutcome,
    publication: Option<RewriteCommit>,
}
#[derive(Clone, Copy, Debug, PartialEq)]
enum ProviderOutcome {
    Complete,
    FinalizationFailure,
    CommitUnknown,
    DerivedAbortUnknown,
}
struct ProviderSession {
    effect: ExecutionEffect,
    finalizer: Arc<Finalizer>,
}
impl DistributedRewriteSession for ProviderSession {
    fn plan_facts(&self) -> RewritePlanFacts {
        RewritePlanFacts {
            noop: false,
            cohort_count: 1,
            input_bytes: 0,
        }
    }
    fn execute_cohort<'a>(
        &'a mut self,
        ordinal: usize,
    ) -> MaintenanceFuture<'a, Result<(), TerminalError>> {
        Box::pin(async move {
            assert_eq!(ordinal, 0);
            self.finalizer.cohorts.fetch_add(1, Ordering::SeqCst);
            if self.finalizer.outcome == ProviderOutcome::DerivedAbortUnknown {
                use novarocks_query_application::execution_error::{
                    ExecutionFailureInput, ExecutionFailureSite, QueryFailureReason,
                };
                return Err(TerminalError::failed("original logical stop observation")
                    .with_execution_failure(Some(ExecutionFailureInput::new(
                        QueryFailureReason::LogicalOwnerStopped.failure_fact(),
                        novarocks_execution_contract::SafeDetail::truncating(
                            "original logical stop observation",
                        ),
                        ExecutionFailureSite::Coordinator,
                    ))));
            }
            Ok(())
        })
    }
    fn commit<'a>(&'a mut self) -> MaintenanceFuture<'a, Result<RewriteCommit, RewriteFailure>> {
        if let Some(publication) = self.finalizer.publication.clone() {
            return Box::pin(async move { Ok(publication) });
        }
        if self.finalizer.outcome == ProviderOutcome::CommitUnknown {
            self.finalizer.calls.fetch_add(1, Ordering::SeqCst);
            return Box::pin(std::future::poll_fn(move |_| {
                if !self.finalizer.allow.load(Ordering::SeqCst) {
                    return Poll::Pending;
                }
                Poll::Ready(Ok(RewriteCommit::CommitUnknown {
                    failure: "original provider commit observation is unknown".into(),
                }))
            }));
        }
        Box::pin(async move {
            Ok(RewriteCommit::KnownCommitted {
                effect: self.effect,
                finalization_failed: None,
            })
        })
    }
    fn finalize_committed<'a>(
        &'a mut self,
    ) -> MaintenanceFuture<'a, Result<RewriteReceiptFacts, RewriteFailure>> {
        self.finalizer.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(std::future::poll_fn(move |_| {
            if !self.finalizer.allow.load(Ordering::SeqCst) {
                return Poll::Pending;
            }
            Poll::Ready(
                if self.finalizer.outcome == ProviderOutcome::FinalizationFailure {
                    Err("original necessary finalizer failed".into())
                } else {
                    Ok(RewriteReceiptFacts {
                        output_rows: Some(if self.effect == ExecutionEffect::External {
                            7
                        } else {
                            0
                        }),
                        ..Default::default()
                    })
                },
            )
        }))
    }
    fn abort<'a>(
        &'a mut self,
        _: String,
    ) -> MaintenanceFuture<'a, Result<RewriteAbort, RewriteFailure>> {
        if let Some(publication) = self.finalizer.publication.clone() {
            return Box::pin(async move {
                Ok(match publication {
                    RewriteCommit::CommitUnknown { failure } => {
                        RewriteAbort::CommitUnknown { failure }
                    }
                    _ => RewriteAbort::KnownUncommitted,
                })
            });
        }
        assert_eq!(self.finalizer.outcome, ProviderOutcome::DerivedAbortUnknown);
        self.finalizer.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(std::future::poll_fn(move |_| {
            if !self.finalizer.allow.load(Ordering::SeqCst) {
                return Poll::Pending;
            }
            Poll::Ready(Ok(RewriteAbort::CommitUnknown {
                failure: "original abort observation is unknown".into(),
            }))
        }))
    }
}
struct ProductExecution {
    product: TableMaintenanceProduct,
    effect: ExecutionEffect,
    finalizer: Arc<Finalizer>,
}
impl TableMaintenanceEffectPort for ProductExecution {
    fn observe_cleanup_owned_ref<'a>(
        &'a self,
        _: &'a novarocks_table_maintenance::gc_observation::GcOwnedRefObservationAccelerator,
        _: novarocks_table_maintenance::gc_observation::GcOwnedRefObservation,
        _: i64,
        _: i64,
    ) -> novarocks_table_maintenance::product::MaintenanceFuture<
        'a,
        Result<
            novarocks_table_maintenance::gc_observation::GcOwnedRefObservationDecision,
            novarocks_table_maintenance::runtime::TerminalError,
        >,
    > {
        Box::pin(async {
            Err(
                novarocks_table_maintenance::runtime::TerminalError::pre_dispatch_failed(
                    "this effect source cannot observe owned-ref cleanup",
                ),
            )
        })
    }
    fn reject_user_action_on_mv<'a>(
        &'a self,
        _: &'a MaintenanceTarget,
    ) -> MaintenanceFuture<'a, Result<(), novarocks_table_maintenance::runtime::TerminalError>>
    {
        Box::pin(async { panic!("the job already owns its original activity") })
    }
    fn execute_metadata<'a>(
        &'a self,
        _: MaintenanceActionRequest,
    ) -> MaintenanceFuture<'a, Result<MaintenanceActionOutcome, String>> {
        Box::pin(async { panic!("the original worker dispatches Rewrite only") })
    }
    fn begin_rewrite<'a>(
        &'a self,
        _: &'a MaintenanceTarget,
        _: RewriteIntent,
    ) -> MaintenanceFuture<'a, Result<Box<dyn DistributedRewriteSession + 'a>, String>> {
        Box::pin(async move {
            Ok(Box::new(ProviderSession {
                effect: self.effect,
                finalizer: self.finalizer.clone(),
            }) as Box<dyn DistributedRewriteSession>)
        })
    }
    fn begin_cleanup<'a>(
        &'a self,
        _: &'a MaintenanceTarget,
        _: i64,
    ) -> MaintenanceFuture<
        'a,
        Result<Box<dyn CleanupSession + 'a>, novarocks_table_maintenance::runtime::TerminalError>,
    > {
        Box::pin(async { panic!("business terminal does not run unrelated GC") })
    }
}
impl OptimizeJobExecution for ProductExecution {
    fn run_original_job<'a>(
        self: Box<Self>,
        _: Arc<OptimizeProcessRuntime>,
        _: OptimizeJobOriginals,
        _: OptimizeJob,
    ) -> MaintenanceFuture<'a, Result<OptimizeJobOutcome, TerminalError>> {
        Box::pin(async { panic!("this component calls the original claimed-job author directly") })
    }
    fn original_effect<'a>(&'a self) -> MaintenanceFuture<'a, Option<ExecutionEffect>> {
        Box::pin(async move { Some(self.effect) })
    }
    fn rebind_target<'a>(
        &'a self,
        _: &'a OptimizeJob,
    ) -> MaintenanceFuture<'a, Result<MaintenanceTargetRebind, String>> {
        Box::pin(async { Ok(MaintenanceTargetRebind::Bound) })
    }
    fn execute<'a>(
        &'a self,
        job: &'a OptimizeJob,
        _: &'a QueryResultCapacityBinding,
    ) -> MaintenanceFuture<'a, Result<MaintenanceActionOutcome, TerminalError>> {
        Box::pin(self.product.execute_rewrite_terminal(
            self,
            &job.target,
            RewriteIntent::DataFiles { rewrite_all: true },
        ))
    }
}

#[tokio::test]
async fn job_product_ready_survives_a_real_late_capacity_refusal() {
    for effect in [ExecutionEffect::External, ExecutionEffect::None] {
        exercise_original_terminal(effect, ProviderOutcome::Complete).await;
    }
}

#[tokio::test]
async fn necessary_finalize_error_keeps_the_original_effect_and_failure() {
    for effect in [ExecutionEffect::External, ExecutionEffect::None] {
        exercise_original_terminal(effect, ProviderOutcome::FinalizationFailure).await;
    }
}

#[tokio::test]
async fn product_commit_unknown_survives_a_real_late_capacity_refusal() {
    exercise_original_terminal(ExecutionEffect::None, ProviderOutcome::CommitUnknown).await;
}

#[tokio::test]
async fn product_abort_unknown_refines_stop_cause_without_erasing_publication_fact() {
    exercise_original_terminal(ExecutionEffect::None, ProviderOutcome::DerivedAbortUnknown).await;
}

async fn exercise_original_terminal(effect: ExecutionEffect, outcome: ProviderOutcome) {
    // The same WC/window author used by the Direct and original worker
    // components. No Ready token, exit receipt or grant is fabricated.
    let control = WorkloadControl::try_new(
        WorkloadConfig::default(),
        ResourceConfig {
            total_bytes: 1024 * 1024,
            control_bytes: 1024,
            per_scope_bytes: 1024 * 1024 - 1024,
        },
    )
    .unwrap();
    let mut result_config = ResultCapacityConfig::V1;
    result_config.all_objects_bytes.fill(64);
    control.configure_result_capacity(result_config).unwrap();
    control.mark_ready().unwrap();
    let root = control
        .root_admission()
        .begin_warehouse_root(WorkRequest::new(WorkClass::TableMaintenance))
        .unwrap();
    let scope = root.owner.scope();
    let (query, window) = scope
        .admit_query_with_result(ResultWindowClass::Internal)
        .unwrap()
        .await
        .unwrap();
    let capacity = QueryResultCapacityBinding::try_new(&scope, window.retain_alias()).unwrap();
    let execution = ProductExecution {
        product: TableMaintenanceProduct::new(None),
        effect,
        finalizer: Arc::new(Finalizer {
            allow: AtomicBool::new(false),
            calls: AtomicUsize::new(0),
            cohorts: AtomicUsize::new(0),
            outcome,
            publication: None,
        }),
    };
    let target = MaintenanceTarget {
        catalog: "catalog".into(),
        namespace: "namespace".into(),
        table: "table".into(),
    };
    let activity = execution
        .product
        .acquire_activity(&target, MaintenanceActivityFamily::Optimize)
        .unwrap();
    let jobs = OptimizeProcessRuntime::new();
    jobs.submit(
        JobCreate {
            target,
            effect_id: None,
            object_id: vec![1],
            base_snapshot_id: 1,
            created_at_ms: 1,
        },
        activity,
    )
    .await
    .unwrap();
    let job = jobs.claim_next(2).await.unwrap().unwrap();
    let id = job.job_id;
    let original = execute_claimed_job(&jobs, &execution, &scope, capacity.clone(), job);
    tokio::pin!(original);
    assert!(futures::poll!(original.as_mut()).is_pending());
    assert_eq!(execution.finalizer.calls.load(Ordering::SeqCst), 1);
    assert_eq!(execution.finalizer.cohorts.load(Ordering::SeqCst), 1);
    assert_eq!(
        jobs.get(id).await.unwrap().unwrap().state,
        MaintenanceJobState::Running
    );
    execution.finalizer.allow.store(true, Ordering::SeqCst);
    let result = original.await.map(OriginalProductOutcome::Job);
    assert_eq!(execution.finalizer.calls.load(Ordering::SeqCst), 1);

    // The Product terminal is already returned. The same original result
    // owner has an actual retained buffer; a refused append changes no bytes.
    let original_bytes = capacity.window_alias().reserve_payload_bytes(64).unwrap();
    let original_buffer = vec![0u8; 64];
    let refused = capacity
        .window_alias()
        .reserve_payload_bytes(1)
        .unwrap_err();
    assert!(matches!(
        &refused,
        WorkError::Capacity("retained result bytes")
    ));
    assert_eq!(original_bytes.bytes(), 64);
    assert_eq!(capacity.window_alias().retained_payload_bytes(), 64);
    let mut selected = Some(TerminalError::from_original_cause(refused));
    let mut stopped = selected.clone();
    let selected_before = selected.clone();
    if outcome == ProviderOutcome::DerivedAbortUnknown {
        let error = result.as_ref().err().unwrap();
        assert_eq!(error.state, MaintenanceJobState::CommitUnknown);
        assert_eq!(
            error.execution_failure.as_ref().unwrap().fact().evidence(),
            novarocks_execution_contract::failure::FailureEvidence::StopObservation
        );
    }
    let terminal = select_product_terminal(result, &mut selected, &mut stopped, Some(effect));
    let terminal = match terminal {
        Ok(OriginalProductOutcome::Job(outcome)) => Ok(outcome),
        Ok(OriginalProductOutcome::Direct(_)) => panic!("the actual worker returned a Job outcome"),
        Err(error) => Err(error),
    };
    if outcome == ProviderOutcome::DerivedAbortUnknown {
        let error = terminal.as_ref().unwrap_err();
        let original = selected_before.as_ref().unwrap();
        assert_eq!(error.state, MaintenanceJobState::CommitUnknown);
        assert_eq!(error.effect, None);
        assert_eq!(error.message, original.message);
        assert_eq!(error.compile_control, original.compile_control);
        assert!(error.execution_failure.is_none());
    } else if outcome == ProviderOutcome::CommitUnknown {
        let error = terminal.as_ref().unwrap_err();
        assert_eq!(error.state, MaintenanceJobState::CommitUnknown);
        assert_eq!(error.effect, None);
        assert_eq!(
            error.message,
            "distributed rewrite commit outcome is unknown: original provider commit observation is unknown; do not retry automatically"
        );
        assert!(error.compile_control.is_none());
    } else if outcome == ProviderOutcome::FinalizationFailure {
        let error = terminal.as_ref().unwrap_err();
        assert_eq!(
            error.state,
            MaintenanceJobState::KnownCommittedFinalizationFailed
        );
        assert_eq!(error.effect, Some(effect));
        assert_eq!(error.message, "original necessary finalizer failed");
    } else {
        let outcome = terminal.as_ref().unwrap();
        assert_eq!(
            outcome.commit_occurred,
            Some(effect == ExecutionEffect::External)
        );
        assert_eq!(
            outcome.output_record_count,
            Some(if effect == ExecutionEffect::External {
                7
            } else {
                0
            })
        );
    }
    // The real ordinary refusal is still available to residual cleanup for
    // every authoritative Product terminal, not merely the success branch.
    if outcome == ProviderOutcome::DerivedAbortUnknown {
        // The original typed cause moved into the business terminal; the
        // independently retained stop slot still owns the same observation.
        assert!(selected.is_none());
    } else {
        assert_eq!(selected, selected_before);
    }
    assert_eq!(stopped, selected_before);
    let observed_effect = match outcome {
        ProviderOutcome::Complete | ProviderOutcome::FinalizationFailure => Some(effect),
        ProviderOutcome::CommitUnknown | ProviderOutcome::DerivedAbortUnknown => None,
    };
    let record = jobs
        .finish_with_effect(id, terminal, observed_effect, 3)
        .await
        .unwrap();
    assert_eq!(
        record.state,
        match outcome {
            ProviderOutcome::Complete => MaintenanceJobState::Finished,
            ProviderOutcome::FinalizationFailure =>
                MaintenanceJobState::KnownCommittedFinalizationFailed,
            ProviderOutcome::CommitUnknown | ProviderOutcome::DerivedAbortUnknown =>
                MaintenanceJobState::CommitUnknown,
        }
    );
    assert_eq!(record.effect, observed_effect);
    drop(original_buffer);
    drop(original_bytes);
    assert_eq!(capacity.window_alias().retained_payload_bytes(), 0);
    drop((capacity, query, window));
    root.owner.complete();
    control.close_admission();
    assert!(control.shutdown().is_ok());
}

// The actual Provider outcome feeds the production projection and Product.
// No Ready/Native/physical-exit capability is fabricated by this component.
#[tokio::test]
async fn originating_mutation_failure_keeps_one_cause_through_product_job_attached_and_direct() {
    use novarocks_failure_contract::SharedFailureCause;
    use novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion;
    use novarocks_spi::connector::{
        ConnectorError, ConnectorMutationFailure, ConnectorWriteAbortOutcome,
        ExternalMutationEffect, ExternalMutationFinalization, ExternalMutationOutcome,
        LakePublicationFamily, LakePublicationId, LakePublicationTarget,
    };
    use novarocks_table_maintenance::runtime::JobHandle;
    use novarocks_type_contract::{
        CompileControlError, owned_resources::layout::LayoutResourceError,
    };
    let control =
        CompileControlError::Source(SharedFailureCause::new(LayoutResourceError::Arithmetic));
    let mutation = ConnectorMutationFailure::from_connector(ConnectorError::compile_interruption(
        control.clone(),
    ));
    let expected = mutation.failure_fact();
    for case in 0..6 {
        let attempt = crate::dml::attempt::DmlPublicationAttempt::new(
            LakePublicationId::new_v7(),
            LakePublicationFamily::DataMutation,
            LakePublicationTarget::try_new(
                "catalog".into(),
                "namespace".into(),
                Some("table".into()),
                None,
            )
            .unwrap(),
            None,
        );
        let mut publication = StatementPublication::new(attempt);
        let effect = if case == 3 {
            ExecutionEffect::None
        } else {
            ExecutionEffect::External
        };
        let expected_state = match case {
            0 => {
                publication.outcome =
                    Some(Ok(ConnectorWritePublicationCompletion::KnownUncommitted {
                        failure: mutation.clone(),
                        cleanup: ExternalMutationFinalization::Complete,
                    }));
                MaintenanceJobState::KnownUncommitted
            }
            1 | 4 | 5 => {
                let ExternalMutationOutcome::CommitUnknown { evidence, .. } =
                    crate::query_execution::write_session::tests::commit_unknown()
                else {
                    unreachable!()
                };
                if case == 1 {
                    publication.outcome =
                        Some(Ok(ConnectorWritePublicationCompletion::CommitUnknown {
                            failure: mutation.clone(),
                            evidence,
                        }));
                } else {
                    publication.abort_outcome =
                        Some(Ok(ConnectorWriteAbortOutcome::CommitUnknown {
                            failure: mutation.clone(),
                            evidence,
                        }));
                }
                MaintenanceJobState::CommitUnknown
            }
            _ => {
                let ExternalMutationOutcome::KnownCommitted { receipt, .. } =
                    crate::query_execution::write_session::tests::known_committed()
                else {
                    unreachable!()
                };
                publication.outcome = Some(Ok(
                    ConnectorWritePublicationCompletion::from_committed_frontier(
                        if case == 3 {
                            ExternalMutationEffect::NoOp
                        } else {
                            ExternalMutationEffect::Applied
                        },
                        receipt,
                        ExternalMutationFinalization::Failed(mutation.clone()),
                    ),
                ));
                MaintenanceJobState::KnownCommittedFinalizationFailed
            }
        };
        let mut receiving = None;
        let terminal = project_rewrite_publication(&publication, &mut receiving).unwrap();
        let cause = receiving.as_ref().unwrap().clone();
        // A later commit/abort observation borrows the same stable receiving.
        let _ = project_rewrite_publication(&publication, &mut receiving).unwrap();
        assert_eq!(receiving.as_ref(), Some(&cause));
        assert_eq!(cause.failure_fact(), expected);
        let original = cause.downcast_ref::<ConnectorMutationFailure>().unwrap();
        let source = std::error::Error::source(original.connector_source().unwrap()).unwrap();
        assert_eq!(
            source
                .downcast_ref::<SharedFailureCause>()
                .unwrap()
                .downcast_ref::<CompileControlError>(),
            Some(&control)
        );
        let execution = ProductExecution {
            product: TableMaintenanceProduct::new(None),
            effect,
            finalizer: Arc::new(Finalizer {
                allow: AtomicBool::new(true),
                calls: AtomicUsize::new(0),
                cohorts: AtomicUsize::new(0),
                outcome: if case == 5 {
                    ProviderOutcome::DerivedAbortUnknown
                } else {
                    ProviderOutcome::Complete
                },
                publication: Some(terminal),
            }),
        };
        let target = MaintenanceTarget {
            catalog: "catalog".into(),
            namespace: "namespace".into(),
            table: "table".into(),
        };
        let activity = execution
            .product
            .acquire_activity(&target, MaintenanceActivityFamily::Optimize)
            .unwrap();
        let jobs = OptimizeProcessRuntime::new();
        let submitted = jobs
            .submit(
                JobCreate {
                    target: target.clone(),
                    effect_id: None,
                    object_id: vec![1],
                    base_snapshot_id: 1,
                    created_at_ms: 1,
                },
                activity,
            )
            .await
            .unwrap();
        jobs.claim_next(2).await.unwrap().unwrap();
        let error = execution
            .product
            .execute_rewrite_terminal(
                &execution,
                &target,
                RewriteIntent::DataFiles { rewrite_all: true },
            )
            .await
            .unwrap_err();
        let expected_effect = match case {
            1 | 4 | 5 => None,
            2 => Some(ExecutionEffect::External),
            _ => Some(ExecutionEffect::None),
        };
        assert_eq!(error.state, expected_state);
        assert_eq!(error.effect, expected_effect);
        assert_eq!(error.failure_cause.as_ref(), Some(&cause));
        assert_eq!(error.selected_failure_fact(), Some(expected));
        assert!(error.compile_control.is_none());
        let mut late = Some(TerminalError::cancelled_before_dispatch("late caller stop"));
        let mut stopped = late.clone();
        let error = match select_product_terminal(Err(error), &mut late, &mut stopped, Some(effect))
        {
            Err(error) => error,
            Ok(_) => panic!("the actual Product failure must remain terminal"),
        };
        assert_eq!(error.failure_cause.as_ref(), Some(&cause));
        assert_eq!(error.state, expected_state);
        jobs.finish(submitted.job_id, Err(error), 3).await.unwrap();
        let job = jobs.wait_for_completion(submitted.job_id).await.unwrap();
        assert_eq!(job.failure_cause.as_ref(), Some(&cause));
        assert_eq!(job.effect, expected_effect);
        let attached = crate::table_maintenance::optimize_statement_terminal_result(
            JobHandle::new(job.job_id),
            job,
            true,
            Some("late cancellation request".into()),
        )
        .unwrap_err();
        assert_eq!(attached.failure_cause.as_ref(), Some(&cause));
        assert_eq!(attached.state, expected_state);
        assert_eq!(attached.effect, expected_effect);
        let service = computation_service_error(attached);
        assert_eq!(service.failure_cause(), Some(&cause));
        assert_eq!(service.execution_failure().unwrap().fact(), expected);
        assert!(service.compile_control_error().is_none());
        let sidecar = std::error::Error::source(&service)
            .unwrap()
            .downcast_ref::<SharedFailureCause>()
            .unwrap();
        assert_eq!(sidecar, &cause);
        assert_eq!(
            computation_error(service.clone()).failure_cause.as_ref(),
            Some(&cause)
        );
        assert_eq!(service.clone().failure_cause(), Some(&cause));
    }
}

#[tokio::test]
async fn provider_terminal_message_and_original_cause_survive_the_actual_job_record() {
    use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
    use novarocks_table_maintenance::runtime::ProcessRuntime;
    for message in [
        "Iceberg Variant writes are not supported yet: table ice.ns.orders, column v",
        "the original provider refuses this different exact operation",
    ] {
        let error = ConnectorError::new(ConnectorErrorKind::Unsupported, message)
            .with_public_failure_detail();
        let fact = error.failure_fact();
        let terminal = from_connector_cause(error.clone());
        assert_eq!(terminal.message, message);
        assert_eq!(terminal.selected_failure_fact(), Some(fact));
        let cause = terminal.failure_cause.clone().unwrap();
        assert_eq!(cause.downcast_ref::<ConnectorError>(), Some(&error));
        assert_eq!(
            cause.detail(),
            "table-maintenance original operation failed"
        );
        let runtime: ProcessRuntime<String, (), ()> = ProcessRuntime::new();
        let job = runtime
            .submit(
                JobCreate {
                    target: "orders".into(),
                    effect_id: None,
                    object_id: vec![1],
                    base_snapshot_id: 1,
                    created_at_ms: 1,
                },
                (),
            )
            .await
            .unwrap();
        runtime.claim_next(2).await.unwrap().unwrap();
        runtime.finish(job.job_id, Err(terminal), 3).await.unwrap();
        let record = runtime.wait_for_completion(job.job_id).await.unwrap();
        assert_eq!(record.error_message.as_deref(), Some(message));
        assert_eq!(record.state, MaintenanceJobState::PreDispatchFailed);
        assert_eq!(record.failure_cause.as_ref(), Some(&cause));
        assert_eq!(record.failure_cause.as_ref().unwrap().failure_fact(), fact);
        assert!(record.execution_failure.is_none());
        assert!(record.compile_control.is_none());
    }
}

#[test]
fn generic_terminal_failure_keeps_its_neutral_detail_without_formatting() {
    #[derive(Debug)]
    struct PrivateFailure;
    impl std::fmt::Display for PrivateFailure {
        fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            panic!("a generic failure must not be formatted for a public job message")
        }
    }
    impl std::error::Error for PrivateFailure {}
    impl FailureProjection for PrivateFailure {
        fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
            novarocks_spi::connector::ConnectorErrorKind::Unsupported.failure_fact()
        }
    }
    let fact = PrivateFailure.failure_fact();
    let terminal = TerminalError::from_original_cause(PrivateFailure);
    assert_eq!(
        terminal.message,
        "table-maintenance original operation failed"
    );
    assert_eq!(terminal.selected_failure_fact(), Some(fact));
    let cause = terminal.failure_cause.unwrap();
    assert!(cause.downcast_ref::<PrivateFailure>().is_some());
    assert_eq!(
        cause.detail(),
        "table-maintenance original operation failed"
    );
}

#[test]
fn unmarked_connector_diagnostics_cannot_replace_the_neutral_terminal_detail() {
    use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
    let error = ConnectorError::new(
        ConnectorErrorKind::Unsupported,
        "private SDK response context must remain retained",
    );
    assert_eq!(error.public_failure_detail(), None);
    let fact = error.failure_fact();
    let terminal = from_connector_cause(error.clone());
    assert_eq!(
        terminal.message,
        "table-maintenance original operation failed"
    );
    assert_eq!(terminal.selected_failure_fact(), Some(fact));
    let cause = terminal.failure_cause.unwrap();
    assert_eq!(cause.downcast_ref::<ConnectorError>(), Some(&error));
    assert_eq!(
        cause.detail(),
        "table-maintenance original operation failed"
    );
}
