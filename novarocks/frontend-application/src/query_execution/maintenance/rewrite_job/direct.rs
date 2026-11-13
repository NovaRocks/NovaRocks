// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Direct SQL borrows the same original Rewrite Domain and poll kernel.
use super::*;
use novarocks_query_application::{
    admitted_query_context::QueryExecutionContext,
    api::LocalResultProducer,
    session_control::{GovernedStatementAction, GovernedStatementResidual, StatementActionValue},
};
use novarocks_table_maintenance::{
    activity::{MaintenanceActivityFamily, MaintenanceActivityPermit},
    product::{RewriteIntent, TableMaintenanceProduct},
};
use novarocks_workload_control::ResultWindowClass;

struct Starting {
    engine: RequestScopedMaintenanceEngine,
    target: MaintenanceTarget,
    intent: RewriteIntent,
    rewrite: SharedConnectorDistributedRewrite,
    product: Arc<TableMaintenanceProduct>,
    dependencies: Vec<novarocks_spi::connector::SharedDependency>,
    #[cfg(novarocks_dev_controls)]
    observation: Option<crate::preparation_diagnostics::StatementObservationHandle>,
    #[cfg(novarocks_dev_controls)]
    controls: Option<Arc<crate::table_maintenance::test_controls::MaintenanceTestControls>>,
}
// The F borrows this exact immutable input; cancellation never moves its
// unique target/product facts into an otherwise unobserved async frame.
struct DirectInput {
    rewrite: SharedConnectorDistributedRewrite,
    engine: RequestScopedMaintenanceEngine,
    target: MaintenanceTarget,
    intent: RewriteIntent,
    product: Arc<TableMaintenanceProduct>,
    #[cfg(novarocks_dev_controls)]
    controls: Option<Arc<crate::table_maintenance::test_controls::MaintenanceTestControls>>,
}
// Design: ADR-0231 (docs/adr/ADR-0231-statement-owned-maintenance-statistics-and-state-surfaces.md)
struct DirectRewriteAction {
    family: novarocks_result_contract::ClientPresentationFamily,
    starting: Option<Starting>,
    input: Option<Arc<DirectInput>>,
    state: Option<RewriteActionState>,
    io: Option<Arc<MetadataIoAdmission>>,
    activity: Option<MaintenanceActivityPermit>,
    local: Option<LocalResultProducer>,
    delivered: bool,
    optimize: bool,
}
struct DirectResidual {
    original: DirectRewriteAction,
}

fn closed_intent(
    action: crate::table_maintenance::ParsedMaintenanceAction,
) -> Result<RewriteIntent, TerminalError> {
    match action {
        crate::table_maintenance::ParsedMaintenanceAction::RewriteDataFiles { .. } => {
            // The semantic lowerer already validated rewrite-all, branch and
            // predicate support. Keep the original Product intent unchanged.
            Ok(RewriteIntent::DataFiles { rewrite_all: true })
        }
        crate::table_maintenance::ParsedMaintenanceAction::RewritePositionDeleteFiles {
            options,
            where_clause,
        } => novarocks_table_maintenance::product::rewrite_position_delete_intent(
            &options,
            where_clause.as_deref(),
        )
        .map_err(TerminalError::pre_dispatch_failed),
        _ => Err(TerminalError::pre_dispatch_failed(
            "not a direct Rewrite command",
        )),
    }
}

pub(crate) fn prepare(
    kernel: &crate::query_execution::kernels::MaintenanceExecutionKernel,
    names: &[String],
    action: crate::table_maintenance::ParsedMaintenanceAction,
    context: MaintenanceRequestContext<'_>,
    execution: &QueryExecutionContext,
    connector: &ConnectorRequestContext,
    family: novarocks_result_contract::ClientPresentationFamily,
    #[cfg(novarocks_dev_controls)] observation: Option<
        crate::preparation_diagnostics::StatementObservationHandle,
    >,
) -> Result<Box<dyn GovernedStatementAction>, TerminalError> {
    let intent = closed_intent(action)?;
    prepare_intent(
        kernel,
        names,
        intent,
        context,
        execution,
        connector,
        family,
        #[cfg(novarocks_dev_controls)]
        observation,
    )
}

pub(crate) fn prepare_optimize(
    kernel: &crate::query_execution::kernels::MaintenanceExecutionKernel,
    names: &[String],
    context: MaintenanceRequestContext<'_>,
    execution: &QueryExecutionContext,
    connector: &ConnectorRequestContext,
    family: novarocks_result_contract::ClientPresentationFamily,
    #[cfg(novarocks_dev_controls)] observation: Option<
        crate::preparation_diagnostics::StatementObservationHandle,
    >,
) -> Result<Box<dyn GovernedStatementAction>, TerminalError> {
    prepare_intent(
        kernel,
        names,
        RewriteIntent::Optimize {
            file_size_threshold_bytes: 100 * 1024 * 1024,
        },
        context,
        execution,
        connector,
        family,
        #[cfg(novarocks_dev_controls)]
        observation,
    )
}

fn prepare_intent(
    kernel: &crate::query_execution::kernels::MaintenanceExecutionKernel,
    names: &[String],
    intent: RewriteIntent,
    context: MaintenanceRequestContext<'_>,
    execution: &QueryExecutionContext,
    connector: &ConnectorRequestContext,
    family: novarocks_result_contract::ClientPresentationFamily,
    #[cfg(novarocks_dev_controls)] observation: Option<
        crate::preparation_diagnostics::StatementObservationHandle,
    >,
) -> Result<Box<dyn GovernedStatementAction>, TerminalError> {
    let engine =
        RequestScopedMaintenanceEngine::new(kernel.clone(), execution.clone(), connector.clone());
    let target = engine
        .resolve_target(names, context)
        .map_err(TerminalError::from)?;
    let capacity = execution.result_capacity().ok_or_else(|| {
        TerminalError::pre_dispatch_failed("direct Rewrite has no original SQL Root capacity")
    })?;
    let origin = capacity
        .try_window_alias()
        .map_err(TerminalError::from_original_cause)?;
    if !origin.is_for_scope(capacity.root_scope()) || origin.class() != ResultWindowClass::Internal
    {
        return Err(TerminalError::pre_dispatch_failed(
            "direct Rewrite requires its original SQL Internal declaration",
        ));
    }
    let identity = RequestScopedMaintenanceEngine::target_identity(&target)
        .map_err(TerminalError::pre_dispatch_failed)?;
    let rewrite = kernel
        .connector_control()
        .acquire_current_distributed_rewrite(&identity.instance_id)
        .map_err(TerminalError::from_original_cause)?;
    let mut dependencies = rewrite
        .planning_initial_dependencies()
        .map_err(TerminalError::from_original_cause)?;
    let write = kernel
        .typed_connector_control()
        .acquire_exact_write_stack(rewrite.planning_pin().control_runtime_id())
        .map_err(TerminalError::from_original_cause)?;
    extend_original_dependencies(
        &mut dependencies,
        &write
            .session()
            .rewrite_begin_initial_dependencies()
            .map_err(TerminalError::from_original_cause)?,
    );
    // Borrow the existing Product; this neither creates nor submits a job.
    let host = kernel
        .service()
        .original_optimize_host()
        .map_err(TerminalError::pre_dispatch_failed)?;
    let product = host.product;
    let optimize = matches!(intent, RewriteIntent::Optimize { .. });
    Ok(Box::new(DirectRewriteAction {
        family,
        optimize,
        starting: Some(Starting {
            engine,
            target,
            intent,
            rewrite,
            product,
            dependencies,
            #[cfg(novarocks_dev_controls)]
            observation,
            #[cfg(novarocks_dev_controls)]
            controls: kernel.service().statement_test_controls(),
        }),
        input: None,
        state: None,
        io: None,
        activity: None,
        local: None,
        delivered: false,
    }))
}

async fn run_direct_product(
    input: Arc<DirectInput>,
    source: RewriteDomainView,
) -> Result<MaintenanceActionOutcome, TerminalError> {
    let binding_cpu = {
        let domain = upgrade_original(&source)?;
        let original = domain.lock().await;
        if original.stopping {
            return Err(TerminalError::pre_dispatch_failed(
                "direct Rewrite original Source is closing",
            ));
        }
        original.binding_cpu.clone()
    };
    let effects = GovernedRewriteEffectPort::new(source.clone());
    // Same original Product checks and rewrite algorithm as the Job path;
    // no legacy coordinator, callback runner or session accumulation bridge.
    input
        .engine
        .reject_direct_rewrite_on_mv_governed(
            &input.target,
            &input.rewrite.planning_pin(),
            binding_cpu.clone(),
        )
        .await
        .map_err(crate::query_execution::maintenance::rewrite_job::guard_terminal_error)?;
    if matches!(input.intent, RewriteIntent::Optimize { .. }) {
        let identity = RequestScopedMaintenanceEngine::target_identity(&input.target)
            .map_err(TerminalError::pre_dispatch_failed)?;
        let metadata = input.rewrite.metadata().clone();
        let (binding, _snapshot) = metadata
            .capture_table_object_with_snapshot_cpu(
                novarocks_spi::connector::ConnectorTableObjectCaptureRequest {
                    table: identity,
                    resolution: novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
                    selector: novarocks_spi::connector::ConnectorTableObjectSelector::Current,
                    context: input.engine.connector_context.clone(),
                },
                binding_cpu.clone(),
            )
            .await
            .map_err(from_connector_cause)?;
        let domain = upgrade_original(&source)?;
        domain.lock().await.expected_object_id = Some(binding.object_id.clone());
        #[cfg(novarocks_dev_controls)]
        if let Some(controls) = &input.controls
            && let Some(nomination) = input.engine.execution.test_statement_nomination()
        {
            let capacity = input.engine.execution.result_capacity().ok_or_else(|| {
                TerminalError::pre_dispatch_failed("OPTIMIZE lost its original result capacity")
            })?;
            let observed = controls
                .claim_statement(
                    nomination.as_ref(),
                    &input.target,
                    &binding.object_id,
                    capacity,
                )
                .map_err(TerminalError::pre_dispatch_failed)?;
            let mut original = domain.lock().await;
            original.maintenance_observation = observed;
            if let Some(observed) = original.maintenance_observation.as_mut() {
                observed
                    .wait_before_planning()
                    .await
                    .map_err(TerminalError::pre_dispatch_failed)?;
            }
        }
    }
    input
        .product
        .execute_rewrite_terminal(&effects, &input.target, input.intent.clone())
        .await
}

impl DirectRewriteAction {
    fn poll_start(
        &mut self,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), QueryServiceError>> {
        if self.state.is_some() {
            return Poll::Ready(Ok(()));
        }
        let work = owner
            .execution_owner_slot()
            .as_ref()
            .ok_or_else(|| service("direct Rewrite lost its original SQL owner"))?;
        if self.io.is_none() {
            let starting = self
                .starting
                .as_ref()
                .ok_or_else(|| service("direct Rewrite original input is empty"))?;
            self.io = Some(
                MetadataIoAdmission::for_original_source(work, starting.dependencies.clone())
                    .map_err(|error| {
                        trace_original_work_refusal("tm.direct.mio_create", &error);
                        computation_service_error(TerminalError::from_original_cause(error))
                    })?,
            );
        }
        let io = self.io.as_ref().unwrap().clone();
        match io.poll_progress(work, cx) {
            Err(error) => {
                trace_original_work_refusal("tm.direct.mio_progress", &error);
                return Poll::Ready(Err(computation_service_error(
                    TerminalError::from_original_cause(error),
                )));
            }
            Ok(false) => return Poll::Pending,
            Ok(true) => {}
        }
        // Local output is separately admitted under THIS Root before any
        // write dispatch. Internal computation capacity is not a Local ticket.
        if self.local.is_none() {
            let grant = owner
                .scope()
                .result_capacity()
                .and_then(|h| h.try_acquire(owner.scope(), ResultWindowClass::Local))
                .map_err(|e| {
                    trace_original_work_refusal("tm.direct.local", &e);
                    computation_service_error(TerminalError::from_original_cause(e))
                })?;
            self.local = Some(
                LocalResultProducer::try_new(owner.scope(), grant.retain_alias(), self.family)
                    .map_err(service)?,
            );
            drop(grant);
        }
        let original = self
            .starting
            .as_mut()
            .ok_or_else(|| service("direct Rewrite starting slot is empty"))?;
        if self.activity.is_none() {
            self.activity = Some(
                original
                    .product
                    .acquire_activity(
                        &original.target,
                        if self.optimize {
                            MaintenanceActivityFamily::Optimize
                        } else {
                            MaintenanceActivityFamily::Metadata
                        },
                    )
                    .map_err(service)?,
            );
        }
        let domain = form_original_domain(
            original.engine.clone(),
            owner.scope().clone(),
            original.rewrite.clone(),
            io.clone(),
            &original.target,
            #[cfg(novarocks_dev_controls)]
            original.observation.clone(),
        )
        .map_err(computation_service_error)?;
        let retained_engine = domain
            .try_lock()
            .map_err(|_| service("new direct Rewrite Domain is borrowed"))?
            .engine
            .clone();
        let input = Arc::new(DirectInput {
            rewrite: original.rewrite.clone(),
            engine: retained_engine,
            target: original.target.clone(),
            intent: original.intent.clone(),
            product: original.product.clone(),
            #[cfg(novarocks_dev_controls)]
            controls: original.controls.clone(),
        });
        self.input = Some(input.clone());
        let wake = domain
            .try_lock()
            .map_err(|_| service("new direct Rewrite Domain is borrowed"))?
            .wake
            .clone();
        let product: ProductFuture = Box::pin(run_direct_product(input, Arc::downgrade(&domain)));
        self.state = Some(RewriteActionState::new(domain, io.clone(), wake, product));
        // The whole replacement is stable before old source references exit.
        drop(self.starting.take());
        Poll::Ready(Ok(()))
    }
    fn poll_sql(
        &mut self,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<StatementActionValue, QueryServiceError>> {
        if self.delivered {
            return Poll::Ready(Err(service("direct Rewrite result was already delivered")));
        }
        std::task::ready!(self.poll_start(owner, cx))?;
        let state = self.state.as_mut().unwrap();
        let result = std::task::ready!(state.poll_product(owner, cx));
        // OPTIMIZE retains its exact target gate through actual Source exit.
        // Direct procedure custody otherwise keeps its existing release frontier.
        if !self.optimize {
            drop(self.activity.take());
        }
        result?;
        let mut domain = state
            .domain
            .try_lock()
            .map_err(|_| service("direct Rewrite completed Domain is borrowed"))?;
        let outcome = match domain.result.take() {
            Some(Ok(outcome)) => outcome,
            Some(other) => {
                domain.result = Some(other);
                return Poll::Ready(Err(service(
                    "direct Rewrite received a failed product outcome",
                )));
            }
            None => {
                return Poll::Ready(Err(service(
                    "direct Rewrite has no original Product outcome",
                )));
            }
        };
        let producer = self
            .local
            .take()
            .ok_or_else(|| service("direct Rewrite has no positive Local producer"))?;
        self.delivered = true;
        drop(domain);
        // Every Arrow/schema/result graph is formed only inside the original
        // positively admitted Local factory, using the original result author.
        Poll::Ready(
            produce_local(producer, outcome, self.optimize)
                .map(StatementActionValue::Local)
                .map_err(service),
        )
    }
    fn poll_exit(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        if let Some(state) = self.state.as_mut() {
            std::task::ready!(state.poll_close(cx))?;
            drop(self.activity.take());
            return Poll::Ready(Ok(()));
        }
        if let Some(io) = self.io.as_ref() {
            match io
                .withdraw_unstarted_in()
                .map_err(|error| health("direct Rewrite bootstrap withdrawal is unproven", error))?
            {
                Some(observed) => {
                    let _ordinary = observed.into_ordinary_failure();
                }
                None => {
                    let _ordinary = std::task::ready!(io.poll_actual_quiescent_continuing(cx))?;
                }
            }
        }
        drop(self.activity.take());
        Poll::Ready(Ok(()))
    }
}
impl GovernedStatementAction for DirectRewriteAction {
    fn poll_action(
        &mut self,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<StatementActionValue, QueryServiceError>> {
        self.poll_sql(owner, cx)
    }
    fn external_effect_disposition(
        &self,
    ) -> Option<novarocks_query_application::api::StatementExternalEffectDisposition> {
        self.state
            .as_ref()
            .and_then(|s| s.disposition)
            .map(crate::query::external_effect_from_lake)
    }
    fn local_health_failure(&self) -> Option<StatementNativeHealthFailure> {
        self.state
            .as_ref()
            .and_then(RewriteActionState::local_health)
            .or_else(|| self.io.as_ref().and_then(|io| io.health_failure()))
    }
    fn handoff_residual_in(
        &mut self,
        target: &mut Option<Box<dyn GovernedStatementResidual>>,
    ) -> Result<(), StatementNativeHealthFailure> {
        if target.is_some() {
            return Err(health(
                "direct Rewrite residual receiver is occupied",
                WorkError::Conflict,
            ));
        }
        let original = Self {
            family: self.family,
            optimize: self.optimize,
            starting: self.starting.take(),
            input: self.input.take(),
            state: self.state.take(),
            io: self.io.clone(),
            activity: self.activity.take(),
            local: None,
            delivered: self.delivered,
        };
        *target = Some(Box::new(DirectResidual { original }));
        // The positive Local reservation owns no graph on an error/cancel.
        // Stable residual precedes its notification/last-alias release.
        drop(self.local.take());
        Ok(())
    }
}
impl GovernedStatementResidual for DirectResidual {
    fn poll_retirement(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        self.original.poll_exit(cx)
    }
}

fn produce_local(
    producer: LocalResultProducer,
    outcome: MaintenanceActionOutcome,
    optimize: bool,
) -> Result<novarocks_query_application::api::OwnedLocalResult, String> {
    producer.produce(|| {
        match if optimize {
            crate::table_maintenance::result::optimize_result(outcome)?
        } else {
            crate::table_maintenance::result::action_result(outcome)?
        } {
            MaintenanceStatementResult::Query(result) => Ok(result),
            MaintenanceStatementResult::Ok => Err("direct Rewrite has no local row result".into()),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_workload_control::{
        ResourceConfig, ResultCapacityConfig, WorkClass, WorkRequest, WorkloadConfig,
        WorkloadControl,
    };

    #[test]
    fn optimize_gate_follows_residual_until_actual_metadata_work_and_alias_exit() {
        use novarocks_resource_contract::SharedDependency;
        use novarocks_spi::connector::{
            ConnectorError, ConnectorIoAdmissionPort, ConnectorIoCleanupFuture,
        };
        use novarocks_table_maintenance::activity::TableMaintenanceActivity;
        use novarocks_workload_control::{
            CancellationReason, ControlStorageConfig, ResponsibilityConfig,
            joint_admission::{JointProgress, JointProgressDriver},
        };
        use std::{
            future::Future,
            num::NonZeroUsize,
            pin::Pin,
            sync::atomic::{AtomicBool, AtomicUsize, Ordering},
            task::Waker,
        };
        fn drive(driver: &mut JointProgressDriver) {
            for _ in 0..100000 {
                if matches!(driver.step(), JointProgress::Idle(_)) {
                    return;
                }
            }
            panic!("original responsibility driver must quiesce");
        }
        struct ActualCleanup {
            released: Arc<AtomicBool>,
            exits: Arc<AtomicUsize>,
        }
        impl Future for ActualCleanup {
            type Output = Result<(), ConnectorError>;
            fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
                if self.released.load(Ordering::SeqCst) {
                    Poll::Ready(Ok(()))
                } else {
                    Poll::Pending
                }
            }
        }
        impl Drop for ActualCleanup {
            fn drop(&mut self) {
                self.exits.fetch_add(1, Ordering::SeqCst);
            }
        }
        let count = 8;
        let control = WorkloadControl::try_new_counted(WorkloadConfig {
            root_limit: count,
            business_limit: count,
            query_concurrency_limit: count,
            scope_records_limit: count,
            ..WorkloadConfig::default()
        })
        .unwrap()
        .owner;
        control
            .configure_result_capacity(ResultCapacityConfig::V1)
            .unwrap();
        let (_, mut driver) = control
            .configure_responsibility(ResponsibilityConfig {
                work_per_turn: NonZeroUsize::new(7).unwrap(),
            })
            .unwrap();
        control
            .configure_control_storage(ControlStorageConfig::new(count, &[]))
            .unwrap();
        control.mark_ready().unwrap();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let io = MetadataIoAdmission::for_original_source(
            &root.owner,
            vec![SharedDependency::LocalHost],
        )
        .unwrap();
        let mut cx = Context::from_waker(Waker::noop());
        let mut ready = false;
        for _ in 0..256 {
            ready = io.poll_progress(&root.owner, &mut cx).unwrap();
            if ready {
                break;
            }
            drive(&mut driver);
        }
        assert!(ready);
        let mut request = Box::pin(io.request(SharedDependency::LocalHost).unwrap().admitted());
        let mut permit = None;
        for _ in 0..256 {
            drive(&mut driver);
            io.poll_progress(&root.owner, &mut cx).unwrap();
            if let Poll::Ready(result) = request.as_mut().poll(&mut cx) {
                permit = Some(result.unwrap());
                break;
            }
        }
        let permit = permit.expect("same original AsyncIo grant");
        drop(request);
        let released = Arc::new(AtomicBool::new(false));
        let exits = Arc::new(AtomicUsize::new(0));
        let mut cleanup: Option<ConnectorIoCleanupFuture> = Some(Box::pin(ActualCleanup {
            released: released.clone(),
            exits: exits.clone(),
        }));
        permit
            .begin_stream()
            .unwrap()
            .install_cleanup_in(&mut cleanup)
            .unwrap_or_else(|_| panic!("same cleanup receiver"));
        let target = MaintenanceTarget {
            catalog: "ice".into(),
            namespace: "db".into(),
            table: "t".into(),
        };
        let activity = TableMaintenanceActivity::default();
        let mut action = DirectRewriteAction {
            family: novarocks_result_contract::ClientPresentationFamily::TrinoJson(
                novarocks_result_contract::TrinoJsonFacts {
                    parametric_datetime: true,
                    variant: true,
                    variant_binary: true,
                },
            ),
            starting: None,
            input: None,
            state: None,
            io: Some(io.clone()),
            activity: Some(
                activity
                    .acquire(&target, MaintenanceActivityFamily::Optimize)
                    .unwrap(),
            ),
            local: None,
            delivered: false,
            optimize: true,
        };
        let mut retirement = None;
        action.handoff_residual_in(&mut retirement).unwrap();
        drop(action);
        root.owner.cancel(CancellationReason::Requested);
        let residual = retirement.as_mut().unwrap();
        assert!(residual.poll_retirement(&mut cx).is_pending());
        assert!(
            activity
                .acquire(&target, MaintenanceActivityFamily::Optimize)
                .is_err()
        );
        assert_eq!(exits.load(Ordering::SeqCst), 0);
        released.store(true, Ordering::SeqCst);
        io.poll_continuing(&mut cx).unwrap();
        assert_eq!(exits.load(Ordering::SeqCst), 1);
        assert!(
            residual.poll_retirement(&mut cx).is_pending(),
            "an original I/O permit alias is still live"
        );
        assert!(
            activity
                .acquire(&target, MaintenanceActivityFamily::Optimize)
                .is_err()
        );
        drop(permit);
        assert!(matches!(
            residual.poll_retirement(&mut cx),
            Poll::Ready(Ok(()))
        ));
        drop(
            activity
                .acquire(&target, MaintenanceActivityFamily::Optimize)
                .unwrap(),
        );
        drop(io);
        // The residual's original Source exits before its Root can retire.
        drop(retirement.take());
        root.business.release();
        root.owner.complete_after_terminal_cancel_settled();
        control.close_admission();
        drive(&mut driver);
        assert!(control.shutdown().is_ok());
    }

    #[test]
    fn optimize_metric_rows_preserve_noop_zeroes_and_unknown_output() {
        use arrow::array::{Array, Int64Array, StringArray};
        for output in [Some(0), None] {
            let MaintenanceStatementResult::Query(result) =
                crate::table_maintenance::result::optimize_result(
                    MaintenanceActionOutcome::RewriteDataFiles {
                        target_snapshot_id: None,
                        rewritten_data_files_count: 0,
                        added_data_files_count: output,
                        added_delete_files_count: None,
                        rewritten_bytes_count: 0,
                        failed_data_files_count: 0,
                        removed_delete_files_count: 0,
                        output_record_count: None,
                    },
                )
                .unwrap()
            else {
                panic!("optimize is a metric rowset")
            };
            assert_eq!(result.row_count(), 3);
            let batch = &result.batches[0];
            let names = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(
                names.iter().collect::<Vec<_>>(),
                vec![
                    Some("rewritten_data_files_count"),
                    Some("removed_delete_files_count"),
                    Some("added_data_files_count")
                ]
            );
            let counts = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert_eq!(counts.value(0), 0);
            assert_eq!(counts.value(1), 0);
            assert_eq!(counts.is_null(2), output.is_none());
            if output.is_some() {
                assert_eq!(counts.value(2), 0);
            }
        }
    }

    #[tokio::test]
    async fn direct_result_uses_a_separate_positive_local_ticket_on_the_same_sql_root() {
        let control = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1024 * 1024,
                control_bytes: 1024,
                per_scope_bytes: 1024 * 1024 - 1024,
            },
        )
        .unwrap();
        let capacity = control
            .configure_result_capacity(ResultCapacityConfig::V1)
            .unwrap();
        control.mark_ready().unwrap();
        // Match the actual Internal SQL admission: one registered Query Root,
        // a byte-retention owner and the original warehouse queue permit.
        // A nonqueued try_begin_root_with_result cannot admit Query work.
        let root = control
            .root_admission()
            .begin_query_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let scope = root.owner.scope();
        let internal = novarocks_query_application::admitted_query_context::QueryResultCapacityBinding::from_root_owner(&root.owner).unwrap();
        let query_permit = scope.admit_query().unwrap().await.unwrap();
        assert!(internal.is_root_backed_internal());
        assert!(
            internal
                .try_window_alias()
                .unwrap()
                .internal_manifest_binding()
                .unwrap()
                .is_none()
        );
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        let local = capacity
            .try_acquire(&scope, ResultWindowClass::Local)
            .unwrap();
        assert!(local.is_for_scope(&scope));
        assert_eq!(local.class(), ResultWindowClass::Local);
        assert_eq!(
            internal.try_window_alias().unwrap().class(),
            ResultWindowClass::Internal
        );
        assert_eq!(capacity.snapshot().held_positions, [0, 1, 0, 0]);
        let producer = LocalResultProducer::try_new(
            &scope,
            local.retain_alias(),
            novarocks_result_contract::ClientPresentationFamily::TrinoJson(
                novarocks_result_contract::TrinoJsonFacts {
                    parametric_datetime: true,
                    variant: true,
                    variant_binary: true,
                },
            ),
        )
        .unwrap();
        let result = produce_local(
            producer,
            MaintenanceActionOutcome::RewriteDataFiles {
                target_snapshot_id: None,
                rewritten_data_files_count: 0,
                added_data_files_count: None,
                added_delete_files_count: None,
                rewritten_bytes_count: 0,
                failed_data_files_count: 0,
                removed_delete_files_count: 0,
                output_record_count: None,
            },
            false,
        )
        .unwrap();
        assert_eq!(result.row_count(), 1);
        assert_eq!(result.columns().len(), 5);
        drop((local, internal));
        root.owner.complete();
        query_permit.release();
        control.close_admission();
        let failure = match control.shutdown() {
            Err(failure) => failure,
            Ok(_) => {
                panic!("the actual Local result alias must remain paid after business completion")
            }
        };
        let (reason, control) = failure.into_parts();
        assert_eq!(
            reason,
            novarocks_workload_control::WorkloadShutdownError::NotDrained
        );
        drop(result);
        assert!(control.shutdown().is_ok());
    }

    #[test]
    fn direct_position_delete_intent_uses_the_original_product_validator() {
        let options = std::collections::BTreeMap::new();
        let action =
            crate::table_maintenance::ParsedMaintenanceAction::RewritePositionDeleteFiles {
                options,
                where_clause: None,
            };
        let actual = closed_intent(action).unwrap();
        assert_eq!(
            actual,
            novarocks_table_maintenance::product::rewrite_position_delete_intent(
                &std::collections::BTreeMap::new(),
                None
            )
            .unwrap()
        );
    }
}
