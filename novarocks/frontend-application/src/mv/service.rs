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

use novarocks_workload_control::{
    QueryConcurrencyPermit, RootAdmissionHandle, WorkClass, WorkOwner, WorkRequest,
};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use super::background::MvBackgroundBindings;
use crate::mv::domain::application::MvApplicationError;
use crate::mv::domain::readiness::MvReadinessPort;
use novarocks_mv_application::{
    maintenance::{
        MaintenanceCoordinatorConfig, MvBackgroundEngineError, MvBackgroundEngineErrorKind,
    },
    process_runtime::{MvBackgroundRuntime, MvBackgroundStop, MvBackgroundTasks},
    service::MvKeyedProductService,
};
use novarocks_query_application::admitted_query_context::{RequestAdmission, RequestContext};
use novarocks_query_application::api::BackendTopologyService;
use novarocks_spi::connector::ConnectorRequestContext;
use novarocks_sql::compiler::SessionOptimizerSettings;

use super::scheduler::{self, ScheduledRefreshDisposition, ScheduledRefreshRequest};

#[path = "service/background_original.rs"]
mod background_original;

/// Frontend composition and statement adapter for materialized-view products.
///
/// MVX-1 owns only Iceberg CREATE sequencing. Other MV statement classes
/// deliberately return `None` so their existing core routes remain active.
pub struct FrontendMvProductAdapter {
    readiness: Arc<MvReadinessPort>,
    provider_activation:
        Arc<dyn crate::query_execution::mv_native_write::MvRefreshProviderActivation>,
    product_service: Arc<MvKeyedProductService>,
    maintenance_config: MaintenanceCoordinatorConfig,
    execution_role: novarocks_types::ClusterRole,
    topology: BackendTopologyService,
    /// Cost budget frozen from `[runtime]`; the MV worker has no session, so it
    /// carries the value that statement admission would otherwise have to guess.
    optimizer_query_mem_limit_bytes: u64,
    attempt_timeout: Duration,
    root_admission: RootAdmissionHandle,
    /// The deployment's statement that a previous incarnation is isolated,
    /// when it configured one. Absent means every barrier a restart left
    /// waits for an operator.
    startup_isolation: Option<crate::mv::startup_isolation_file::StartupIsolationSource>,
}

impl FrontendMvProductAdapter {
    pub(crate) fn with_refresh_dependencies(
        readiness: Arc<MvReadinessPort>,
        provider_activation: Arc<
            dyn crate::query_execution::mv_native_write::MvRefreshProviderActivation,
        >,
        execution_role: novarocks_types::ClusterRole,
        topology: BackendTopologyService,
        product_service: Arc<MvKeyedProductService>,
        maintenance_config: MaintenanceCoordinatorConfig,
        optimizer_query_mem_limit_bytes: u64,
        attempt_timeout: Duration,
        root_admission: RootAdmissionHandle,
        startup_isolation: Option<crate::mv::startup_isolation_file::StartupIsolationSource>,
    ) -> Self {
        Self {
            provider_activation,
            readiness: Arc::clone(&readiness),
            product_service,
            maintenance_config,
            execution_role,
            topology,
            optimizer_query_mem_limit_bytes,
            attempt_timeout,
            root_admission,
            startup_isolation,
        }
    }

    pub(crate) fn readiness_port(&self) -> Arc<MvReadinessPort> {
        Arc::clone(&self.readiness)
    }

    pub(crate) fn original_refresh_ports(
        &self,
    ) -> (
        Arc<MvKeyedProductService>,
        Arc<dyn crate::query_execution::mv_native_write::MvRefreshProviderActivation>,
    ) {
        (
            self.product_service.clone(),
            self.provider_activation.clone(),
        )
    }

    pub(crate) fn product_service(&self) -> &MvKeyedProductService {
        self.product_service.as_ref()
    }

    pub(crate) async fn shutdown_background_workers_until(
        &self,
        deadline: Instant,
    ) -> Result<(), String> {
        self.product_service
            .shutdown_background_workers_until(deadline)
            .await
    }

    pub(crate) fn request_background_stop_for_process_exit(&self) {
        self.product_service
            .request_background_stop_for_process_exit();
    }

    pub(crate) fn start_background_workers(
        &self,
        bindings: MvBackgroundBindings,
    ) -> Result<(), MvBackgroundEngineError> {
        let reservation = self
            .product_service
            .begin_background_start()
            .map_err(lifecycle_error)?;
        let topology = self.topology.clone();
        let runtime = start_background_workers(RefreshWorkerDependencies {
            readiness: Arc::clone(&self.readiness),
            provider_activation: Arc::clone(&self.provider_activation),
            ports: bindings.ports,
            execution: bindings.execution,
            topology,
            role: self.execution_role,
            maintenance_config: self.maintenance_config.clone(),
            product_service: Arc::clone(&self.product_service),
            root_admission: self.root_admission.clone(),
            optimizer_query_mem_limit_bytes: self.optimizer_query_mem_limit_bytes,
            attempt_timeout: self.attempt_timeout,
            runtime: tokio::runtime::Handle::current(),
            startup_isolation: self.startup_isolation.clone(),
        })?;
        reservation.install(runtime).map_err(lifecycle_error)
    }
}

#[derive(Clone)]
struct RefreshWorkerDependencies {
    readiness: Arc<MvReadinessPort>,
    provider_activation:
        Arc<dyn crate::query_execution::mv_native_write::MvRefreshProviderActivation>,
    ports: crate::mv::domain::iceberg_refresh::IcebergMvCorePorts,
    execution: crate::query::mv_action::MvExecutionServices,
    topology: BackendTopologyService,
    role: novarocks_types::ClusterRole,
    maintenance_config: MaintenanceCoordinatorConfig,
    product_service: Arc<MvKeyedProductService>,
    root_admission: RootAdmissionHandle,
    optimizer_query_mem_limit_bytes: u64,
    attempt_timeout: Duration,
    /// The existing FE runtime hosts the two original serial asynchronous
    /// tasks and the shared warehouse queue; no worker or local queue is added.
    runtime: tokio::runtime::Handle,
    startup_isolation: Option<crate::mv::startup_isolation_file::StartupIsolationSource>,
}

struct MvBackgroundHealth(novarocks_query_application::api::QueryExecutionClient);
impl novarocks_mv_application::process_runtime::MvBackgroundHealthPort for MvBackgroundHealth {
    fn record_failure(&self, cause: Arc<dyn std::error::Error + Send + Sync>) {
        self.0.report_process_failure(
            novarocks_query_application::api::StatementNativeHealthFailure::actual_exit_unproven(
                "MV background original source failed before actual closure",
                cause,
            ),
        );
    }
}

fn start_background_workers(
    dependencies: RefreshWorkerDependencies,
) -> Result<MvBackgroundRuntime, MvBackgroundEngineError> {
    let interval = Duration::from_millis(
        dependencies
            .product_service
            .scheduler_tick_interval_ms()
            .max(1),
    );
    let maintenance_interval =
        Duration::from_millis(dependencies.maintenance_config.tick_interval_ms.max(1));
    let scheduler = background_original::SchedulerOriginal::new(
        background_original::OriginalWorkerDependencies::from(&dependencies),
    );
    let maintenance = background_original::MaintenanceOriginal::new(&dependencies);
    MvBackgroundRuntime::start(
        &dependencies.runtime,
        Arc::new(MvBackgroundHealth(dependencies.execution.client.clone())),
        interval,
        maintenance_interval,
        MvBackgroundTasks::new(
            Box::new(move |stop, maintenance_wakeup_tx| {
                Box::pin(scheduler.clone().run(stop, maintenance_wakeup_tx))
            }),
            Box::new(move |stop| Box::pin(maintenance.clone().run(stop))),
        ),
    )
    .map_err(|error| {
        MvBackgroundEngineError::new(MvBackgroundEngineErrorKind::TransientUnavailable, error)
    })
}

fn lifecycle_error(error: impl std::fmt::Display) -> MvBackgroundEngineError {
    MvBackgroundEngineError::new(
        MvBackgroundEngineErrorKind::InvariantViolation,
        error.to_string(),
    )
}

fn scheduler_outcome_log_fields(
    disposition: &ScheduledRefreshDisposition,
) -> Option<(&'static str, &str)> {
    match disposition {
        ScheduledRefreshDisposition::TransientUnavailable(reason) => {
            Some(("transient_unavailable", reason.message()))
        }
        ScheduledRefreshDisposition::InvalidDefinition(reason) => {
            Some(("invalid_definition", reason.message()))
        }
        ScheduledRefreshDisposition::TerminalFailure(reason) => {
            Some(("terminal_failure", reason.message()))
        }
        ScheduledRefreshDisposition::Corruption(reason) => Some(("corruption", reason.message())),
        ScheduledRefreshDisposition::InvariantViolation(reason) => {
            Some(("invariant_violation", reason.message()))
        }
        ScheduledRefreshDisposition::TargetGone(_) => {
            Some(("target_gone", "MV target no longer exists"))
        }
        ScheduledRefreshDisposition::Completed
        | ScheduledRefreshDisposition::NoOp
        | ScheduledRefreshDisposition::AlreadyActive(_)
        | ScheduledRefreshDisposition::ShutdownCancelled(_) => None,
    }
}

fn application_disposition(error: MvApplicationError) -> ScheduledRefreshDisposition {
    use crate::mv::domain::application::MvApplicationErrorKind;
    // The selected domain cause and compiler sidecar are independent facts.
    // Use the same neutral projector as the service path; a Provider failure
    // must not disappear merely because domain policy was already selected.
    let execution = error.original_failure().and_then(|cause| {
        novarocks_query_application::session_error::QueryServiceError::from_failure_cause(
            cause.clone(),
        )
        .execution_failure()
        .cloned()
    });
    let failure = novarocks_mv_application::scheduler_runtime::MvRefreshFailure::new(
        error.message(),
        error.compile_control_error(),
    )
    .with_execution_failure(execution);
    match error.kind() {
        MvApplicationErrorKind::AlreadyActive => {
            ScheduledRefreshDisposition::AlreadyActive(failure)
        }
        MvApplicationErrorKind::Unavailable | MvApplicationErrorKind::BindingInvalidated => {
            ScheduledRefreshDisposition::TransientUnavailable(failure)
        }
        MvApplicationErrorKind::InvalidRequest => {
            ScheduledRefreshDisposition::InvalidDefinition(failure)
        }
        MvApplicationErrorKind::CommitUnknown
        | MvApplicationErrorKind::KnownCommittedFinalizeFailed => {
            ScheduledRefreshDisposition::TerminalFailure(failure)
        }
        MvApplicationErrorKind::ShutdownCancelled => {
            ScheduledRefreshDisposition::ShutdownCancelled(failure)
        }
        MvApplicationErrorKind::Engine => ScheduledRefreshDisposition::InvariantViolation(failure),
    }
}

fn now_unix_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
}

#[cfg(test)]
mod shutdown_tests {
    use std::time::{Duration, Instant};

    use super::{MvBackgroundRuntime, application_disposition};
    use crate::mv::domain::application::{MvApplicationError, MvApplicationErrorKind};
    use crate::mv::scheduler::ScheduledRefreshDisposition;

    #[test]
    fn pre_dispatch_binding_invalidation_is_reprepared_not_recorded_as_unknown() {
        let disposition = application_disposition(MvApplicationError::new(
            MvApplicationErrorKind::BindingInvalidated,
            "test target generation changed",
        ));

        assert!(matches!(
            disposition,
            ScheduledRefreshDisposition::TransientUnavailable(_)
        ));
    }

    #[tokio::test]
    async fn shared_deadline_retains_the_same_mv_worker_join_for_retry() {
        let (maintenance_wakeup_tx, _maintenance_wakeup_rx) = tokio::sync::mpsc::channel(1);
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let refresh_worker = tokio::spawn(async move {
            let _ = release_rx.await;
        });
        let maintenance_worker = tokio::spawn(async {});
        let mut runtime = MvBackgroundRuntime::new(
            novarocks_mv_application::process_runtime::MvBackgroundStop::default(),
            refresh_worker,
            maintenance_wakeup_tx,
            maintenance_worker,
        );
        let error = runtime
            .stop_and_join_until(Instant::now() + Duration::from_millis(10))
            .await
            .expect_err("blocked MV worker must respect the shared deadline");
        assert!(error.contains("shared shutdown deadline"));
        release_tx.send(()).unwrap();
        runtime
            .stop_and_join_until(Instant::now() + Duration::from_secs(1))
            .await
            .expect("the retained MV worker join remains retryable");
    }
    #[tokio::test]
    async fn retry_still_joins_mv_maintenance_after_refresh_already_stopped() {
        let (maintenance_wakeup_tx, _maintenance_wakeup_rx) = tokio::sync::mpsc::channel(1);
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let maintenance_worker = tokio::spawn(async move {
            let _ = release_rx.await;
        });
        let refresh_worker = tokio::spawn(async {});
        let mut runtime = MvBackgroundRuntime::new(
            novarocks_mv_application::process_runtime::MvBackgroundStop::default(),
            refresh_worker,
            maintenance_wakeup_tx,
            maintenance_worker,
        );
        let error = runtime
            .stop_and_join_until(Instant::now() + Duration::from_millis(10))
            .await
            .expect_err("blocked MV maintenance must respect the shared deadline");
        assert!(error.contains("shared shutdown deadline"));
        release_tx.send(()).unwrap();
        runtime
            .stop_and_join_until(Instant::now() + Duration::from_secs(1))
            .await
            .expect("retry must join the retained MV maintenance worker");
    }
}

#[cfg(test)]
mod compile_control_tests {
    use crate::mv::domain::application::MvApplicationErrorKind;
    use crate::query_execution::mv_assembly::refresh_preparation::async_facts::PreparationError;
    use novarocks_type_contract::CompileControlError;

    #[test]
    fn foreground_preparation_terminal_retains_control_without_changing_kind() {
        for control in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let error = PreparationError::from(novarocks_sql::compiler::SqlCompileError::from(
                control.clone(),
            ))
            .0;
            assert_eq!(error.compile_control_error(), Some(control.clone()));
            assert_eq!(error.kind(), MvApplicationErrorKind::InvalidRequest);
            let command = crate::mv::command::mv_application_command_error(error);
            assert_eq!(command.compile_control_error(), Some(control.clone()));
            assert_eq!(command.to_string(), control.to_string());
            assert_eq!(
                command.kind(),
                if control == CompileControlError::Cancelled {
                    novarocks_query_application::api::CommandErrorKind::Cancelled
                } else {
                    novarocks_query_application::api::CommandErrorKind::Failed
                }
            );
        }
    }
    #[test]
    fn application_compile_control_cause_reaches_actual_scheduler_terminal() {
        use super::{application_disposition, scheduler_outcome_log_fields};
        use novarocks_mv_application::scheduler::MvSchedulerConfig;
        use novarocks_mv_application::scheduler_runtime::{
            MvRefreshRuntimeDecision, MvRefreshSchedulerRuntime,
        };
        for control in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
            CompileControlError::Source(novarocks_failure_contract::SharedFailureCause::new(
                novarocks_plan_codec::PhysicalEncodeError::UnsupportedCapability("router".into()),
            )),
            CompileControlError::Source(novarocks_failure_contract::SharedFailureCause::new(
                novarocks_plan_codec::PhysicalEncodeError::Invalid("unsupported diagnostic".into()),
            )),
        ] {
            let error = PreparationError::from(novarocks_sql::compiler::SqlCompileError::from(
                control.clone(),
            ))
            .0;
            let message = error.message().to_owned();
            let disposition = application_disposition(error);
            assert_eq!(disposition.compile_control_error(), Some(control.clone()));
            assert_eq!(
                scheduler_outcome_log_fields(&disposition),
                Some(("invalid_definition", message.as_str()))
            );
            let mut runtime = MvRefreshSchedulerRuntime::<i64, ()>::new(MvSchedulerConfig::new(
                true, 1, 1, 10, 40,
            ));
            let decision = runtime.record(&7, disposition, 100);
            assert!(matches!(decision, MvRefreshRuntimeDecision::Blocked { .. }));
            assert_eq!(decision.compile_control_error(), Some(control));
            assert!(runtime.is_suppressed(&7, 100));
        }
    }

    #[test]
    fn selected_connector_cause_reaches_blocked_scheduler_without_compile_relabeling() {
        use super::{MvApplicationError, ScheduledRefreshDisposition, application_disposition};
        use novarocks_failure_contract::FailureProjection;
        use novarocks_mv_application::scheduler::MvSchedulerConfig;
        use novarocks_mv_application::scheduler_runtime::{
            MvRefreshRuntimeDecision, MvRefreshSchedulerRuntime,
        };
        use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};

        for original in [
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "missing original composed capability",
            ),
            ConnectorError::from_domain(
                ConnectorErrorKind::Unsupported,
                novarocks_local_compiler::FragmentCompileError::Invalid(
                    "unsupported text does not select the original category",
                ),
            ),
        ] {
            let fact = original.failure_fact();
            let domain =
                MvApplicationError::from_connector(MvApplicationErrorKind::Engine, original);
            let selected = domain.original_failure().unwrap().clone();
            let service =
                novarocks_query_application::session_error::QueryServiceError::from_failure_cause(
                    selected.clone(),
                );
            let disposition = application_disposition(domain);
            assert!(matches!(
                disposition,
                ScheduledRefreshDisposition::InvariantViolation(_)
            ));
            assert_eq!(disposition.compile_control_error(), None);
            assert_eq!(
                disposition.execution_failure().unwrap(),
                service.execution_failure().unwrap()
            );
            assert_eq!(
                disposition.execution_failure().unwrap().fact(),
                selected.failure_fact()
            );
            let mut runtime = MvRefreshSchedulerRuntime::<i64, ()>::new(MvSchedulerConfig::new(
                true, 1, 1, 10, 40,
            ));
            let decision = runtime.record(&7, disposition, 100);
            assert!(matches!(decision, MvRefreshRuntimeDecision::Blocked { .. }));
            assert_eq!(decision.compile_control_error(), None);
            assert_eq!(decision.execution_failure().unwrap().fact(), fact);
            assert_eq!(
                decision.execution_failure().unwrap(),
                service.execution_failure().unwrap()
            );
            assert!(runtime.is_suppressed(&7, i64::MAX));
        }
    }

    #[test]
    fn retryable_preparation_error_preserves_its_typed_disposition() {
        use super::{application_disposition, scheduler_outcome_log_fields};
        use novarocks_failure_contract::FailureProjection;
        use novarocks_mv_application::scheduler::MvSchedulerConfig;
        use novarocks_mv_application::scheduler_runtime::{
            MvRefreshRuntimeDecision, MvRefreshSchedulerRuntime,
        };
        use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};

        let error = PreparationError::from(ConnectorError::new(
            ConnectorErrorKind::Unavailable,
            "connector is starting",
        ))
        .0;
        let cause = error
            .original_failure()
            .expect("provider cause is retained")
            .clone();
        let fact = cause.failure_fact();
        let command = crate::mv::command::mv_application_command_error(error.clone());
        assert_eq!(command.failure_cause(), Some(&cause));
        assert_eq!(command.failure_cause().unwrap().failure_fact(), fact);
        let message = error.message().to_owned();
        let disposition = application_disposition(error);
        assert_eq!(
            scheduler_outcome_log_fields(&disposition),
            Some(("transient_unavailable", message.as_str()))
        );
        let mut runtime =
            MvRefreshSchedulerRuntime::<i64, ()>::new(MvSchedulerConfig::new(true, 1, 1, 10, 40));
        let MvRefreshRuntimeDecision::TransientBackoff { error, retry_at_ms } =
            runtime.record(&7, disposition, 100)
        else {
            panic!("provider unavailability must remain retryable");
        };
        assert_eq!(error.message(), message);
        assert!(retry_at_ms > 100);
        assert!(runtime.is_suppressed(&7, 100));
        assert!(!runtime.is_suppressed(&7, retry_at_ms));
    }

    #[test]
    fn definition_preparation_error_remains_blocked() {
        use super::{application_disposition, scheduler_outcome_log_fields};
        use novarocks_mv_application::scheduler::MvSchedulerConfig;
        use novarocks_mv_application::scheduler_runtime::{
            MvRefreshRuntimeDecision, MvRefreshSchedulerRuntime,
        };

        let error = PreparationError::user("stored MV contract is incompatible").0;
        let message = error.message().to_owned();
        let disposition = application_disposition(error);
        assert_eq!(
            scheduler_outcome_log_fields(&disposition),
            Some(("invalid_definition", message.as_str()))
        );
        let mut runtime =
            MvRefreshSchedulerRuntime::<i64, ()>::new(MvSchedulerConfig::new(true, 1, 1, 10, 40));
        let MvRefreshRuntimeDecision::Blocked { error } = runtime.record(&7, disposition, 100)
        else {
            panic!("invalid stored definitions must remain blocked");
        };
        assert_eq!(error.message(), message);
        assert!(runtime.is_suppressed(&7, 100));
        assert!(runtime.is_suppressed(&7, i64::MAX));
    }

    #[test]
    fn background_preparation_terminal_retains_control_without_changing_policy_kind() {
        use super::{application_disposition, scheduler_outcome_log_fields};
        use novarocks_mv_application::scheduler::MvSchedulerConfig;
        use novarocks_mv_application::scheduler_runtime::{
            MvRefreshRuntimeDecision, MvRefreshSchedulerRuntime,
        };
        for control in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let error = PreparationError::from(novarocks_sql::compiler::SqlCompileError::from(
                control.clone(),
            ))
            .0;
            let message = error.message().to_owned();
            let disposition = application_disposition(error);
            assert_eq!(disposition.compile_control_error(), Some(control.clone()));
            assert_eq!(
                scheduler_outcome_log_fields(&disposition),
                Some(("invalid_definition", message.as_str()))
            );
            let mut runtime = MvRefreshSchedulerRuntime::<i64, ()>::new(MvSchedulerConfig::new(
                true, 1, 1, 10, 40,
            ));
            let decision = runtime.record(&7, disposition, 100);
            assert!(matches!(decision, MvRefreshRuntimeDecision::Blocked { .. }));
            assert_eq!(decision.compile_control_error(), Some(control));
            assert!(runtime.is_suppressed(&7, i64::MAX));
        }
    }
}
