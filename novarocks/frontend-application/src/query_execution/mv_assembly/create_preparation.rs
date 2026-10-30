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

//! CREATE pure preparation suspended only at the existing named I/O gate.
use super::refresh_preparation::{
    async_facts::PreparationError,
    async_original::{MvCatalogCaptureInput, MvCatalogCaptureQuery, capture_catalog},
    io_gate::MvRefreshPreparationIo,
};
use crate::mv::domain::{
    application::{MvApplicationError, MvCreateStatement},
    iceberg_refresh::{
        IcebergMvCorePorts, IcebergMvCreatePlan, finish_mv_create_preparation,
        prepare_mv_create_properties, prepare_mv_create_shape, prepare_mv_create_target,
    },
};
use novarocks_spi::connector::{
    ConnectorControlPlanningLease, ConnectorErrorKind, ConnectorOperationControl,
    ConnectorRequestContext, ConnectorTableResolution,
};
use std::{collections::BTreeMap, sync::Arc};

/// Original SQL and the already selected application leaves only. No stage,
/// writer, publication identity or refresh admission enters this reader.
pub(crate) struct MvCreatePreparationInput {
    pub(crate) ports: IcebergMvCorePorts,
    pub(crate) planning: ConnectorControlPlanningLease,
    pub(crate) current_catalog: Option<String>,
    pub(crate) current_database: String,
    pub(crate) statement: MvCreateStatement,
    pub(crate) context: ConnectorRequestContext,
}

pub(crate) async fn prepare_create(
    input: MvCreatePreparationInput,
    io: &MvRefreshPreparationIo,
) -> Result<Arc<IcebergMvCreatePlan>, MvApplicationError> {
    prepare_create_inner(input, io)
        .await
        .map_err(|error| error.0)
}
async fn prepare_create_inner(
    input: MvCreatePreparationInput,
    io: &MvRefreshPreparationIo,
) -> Result<Arc<IcebergMvCreatePlan>, PreparationError> {
    input.context.original_operation_control().check_active()?;
    let target = prepare_mv_create_target(
        input.current_catalog.as_deref(),
        &input.current_database,
        &input.statement,
    )?;
    let lease = input.planning;
    if lease.binding().descriptor().instance_id.as_str() != target.catalog {
        return Err("CREATE reader planning lease does not match its original target".into());
    }
    if lease.binding().descriptor().provider_id.as_str() != "iceberg" {
        return Err(
            "storage_engine='iceberg' requires current catalog to be an Iceberg catalog".into(),
        );
    }
    match io
        .metadata(
            &lease,
            input.context.clone(),
            &target.namespace,
            &target.table,
            ConnectorTableResolution::StrictBaseTable,
        )
        .await
    {
        Ok(_) => {
            return Err(PreparationError::user(format!(
                "Iceberg MV target table {}.{}.{} already exists",
                target.catalog, target.namespace, target.table
            )));
        }
        Err(error) if error.kind() == ConnectorErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let canonical = crate::mv::domain::analysis::canonicalize_iceberg_mv_select_query(
        &input.statement.select_query,
        input.current_catalog.as_deref(),
        &input.current_database,
    );
    // Keep the original raw-clause admission before any analysis lookup.
    crate::mv::domain::analysis::prepare_mv_select_for_catalog_provider(
        &canonical,
        input.current_catalog.as_deref(),
        &input.current_database,
    )?;
    let control = crate::query_execution::planning::sql_compile_control_from_connector_request(
        &input.context,
    );
    let captured = capture_catalog(
        MvCatalogCaptureInput {
            query: MvCatalogCaptureQuery::Query(Box::new(canonical.clone())),
            ports: input.ports.clone(),
            current_catalog: input.current_catalog.clone(),
            current_database: input.current_database.clone(),
            context: input.context.clone(),
            control: control.clone(),
            bindings: Arc::new(
                crate::catalog_application::query_bindings::QueryTableBindingStore::try_new()?,
            ),
            overlays: Vec::new(),
        },
        io,
    )
    .await?;
    let analysis = crate::mv::domain::analysis_adapter::analyze_mv_select_with_captured_catalog(
        input.current_catalog.as_deref(),
        &captured.catalog,
        &input.current_database,
        &canonical,
        input.ports.function_catalog().as_ref(),
        input.ports.constant_policy(),
        control.clone(),
    )?;
    let shape = prepare_mv_create_shape(target, &input.statement, canonical, analysis, &control)?;
    let dependency_target = novarocks_mv_application::dependency::iceberg_mv_dependency_ref(
        &shape.target.catalog,
        &shape.target.namespace,
        &shape.target.table,
    );
    let (projections, dependencies) = io
        .refresh_graph(input.ports.readiness().clone())
        .await?
        .into_parts();
    crate::mv::domain::dependency_resolver::validate_no_create_cycle_from_facts(
        &dependency_target,
        &shape.resolved_dependencies.dependencies,
        projections,
        dependencies,
    )
    .map_err(|message| {
        PreparationError::user(format!(
            "cannot create materialized view {}.{}.{}: {message}",
            shape.target.catalog, shape.target.namespace, shape.target.table
        ))
    })?;
    let (property, persistence) = prepare_mv_create_properties(&shape, &control)?;
    let (observations, binding_document) =
        crate::mv::domain::persistence::source_bindings::observe_mv_create_source_bindings_original(
            input.ports.storage_observation(), &persistence, &captured.bindings, input.context.clone())?;
    let mut base_observations = BTreeMap::new();
    for base in &shape.resolved_dependencies.base_refs {
        base_observations.insert(base.fqn(), {
            let binding = captured
                .bindings
                .strict_base_binding(&base.catalog, &base.namespace, &base.table)
                .ok_or("CREATE base schema has no original catalog binding")?;
            let metadata = binding
                .source_metadata
                .as_ref()
                .ok_or("CREATE base schema has no original metadata")?;
            let lease = binding.admission.exact_planning_lease()?;
            crate::mv::domain::storage_observation::observe_source_schema(
                input.ports.storage_observation(),
                &lease,
                metadata,
                input.context.clone(),
            )?
        });
    }
    finish_mv_create_preparation(
        shape,
        &input.statement,
        property,
        persistence,
        observations,
        binding_document,
        base_observations,
    )
    .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::super::refresh_preparation::{
        async_original::MvRefreshPreparationOriginal, driver::MvRefreshTurnCustody,
        io_gate::MvRefreshPreparationServices,
    };
    use super::*;
    use crate::query_execution::{
        internal_result_cpu::InternalResultCpuOwner, maintenance::io_admission::MetadataIoAdmission,
    };
    use crate::task_execution::status_intake::StatusIntakeWake;
    use novarocks_query_application::{
        cancellation::QueryCancellationView, cpu::QueryBlockingExecutorConfig,
    };
    use novarocks_state_store_api::{
        StateStoreExecutionContext, StateStoreExecutionFailure, StateStoreExecutionRequests,
        StateStoreReadExecution,
    };
    use novarocks_workload_control::{
        ControlStorageConfig, ResultCapacityConfig, WorkClass, WorkRequest, WorkloadConfig,
        WorkloadControl,
        joint_admission::{JointProgress, ResponsibilityConfig},
    };
    use std::{
        num::NonZeroUsize,
        task::Poll,
        time::{Duration, Instant},
    };

    struct NoRepositoryIo;
    impl StateStoreReadExecution for NoRepositoryIo {
        fn read_transaction_context(
            &self,
        ) -> Result<StateStoreExecutionContext, StateStoreExecutionFailure> {
            panic!("cancelled CREATE must not request repository I/O")
        }
    }
    impl StateStoreExecutionRequests for NoRepositoryIo {
        fn write_operation_context(
            &self,
        ) -> Result<StateStoreExecutionContext, StateStoreExecutionFailure> {
            panic!("pure CREATE preparation must not request a repository write")
        }
    }
    #[derive(Default)]
    struct Wake(futures::task::AtomicWaker);
    impl std::fmt::Debug for Wake {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("CreateWake")
        }
    }
    impl StatusIntakeWake for Wake {
        fn wake(&self) {
            self.0.wake();
        }
    }

    async fn with_driver<F: std::future::Future>(
        control: &WorkloadControl,
        driver: &mut novarocks_workload_control::joint_admission::JointProgressDriver,
        future: F,
    ) -> F::Output {
        // Same event-driven responsibility owner used by the existing Current
        // and domain CPU components. A later QB admission changes this revision
        // even when it has not yet installed a consumer waker.
        tokio::pin!(future);
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match driver.step() {
                    JointProgress::More => tokio::task::yield_now().await,
                    JointProgress::Idle(revision) => tokio::select! {
                        biased;
                        result = &mut future => return result,
                        _ = control.wait_progress(revision) => {},
                    },
                }
            }
        })
        .await
        .expect("original CREATE CPU/component watchdog")
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancelled_create_keeps_the_original_typed_result_until_the_real_cpu_footer() {
        use novarocks_failure_contract::{FailureClass, FailureProjection};
        let mut control = WorkloadControl::try_new_counted(WorkloadConfig {
            root_limit: 8,
            business_limit: 8,
            query_concurrency_limit: 8,
            scope_records_limit: 8,
            ..WorkloadConfig::default()
        })
        .unwrap()
        .owner;
        let (_, mut driver) = control
            .configure_responsibility(ResponsibilityConfig {
                work_per_turn: NonZeroUsize::new(7).unwrap(),
            })
            .unwrap();
        control
            .configure_control_storage(ControlStorageConfig::new(8, &[]))
            .unwrap();
        control
            .configure_result_capacity(ResultCapacityConfig::V1)
            .unwrap();
        let mut cpu = InternalResultCpuOwner::try_new(
            QueryBlockingExecutorConfig::new(
                NonZeroUsize::new(1).unwrap(),
                NonZeroUsize::new(1).unwrap(),
            ),
            &mut control,
        )
        .unwrap();
        control.mark_ready().unwrap();
        let root = control
            .root_admission()
            .try_begin_root(WorkRequest::new(WorkClass::Management))
            .unwrap();
        let scope = root.owner.scope();
        let io = MetadataIoAdmission::for_original_source(&root.owner, Vec::new()).unwrap();
        with_driver(
            &control,
            &mut driver,
            std::future::poll_fn(|cx| {
                if io.poll_progress(&root.owner, cx).unwrap() {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }),
        )
        .await;
        let capacity = io.root_capacity().unwrap();
        let window = capacity.try_window_alias().unwrap();
        let stop = novarocks_spi::connector::ConnectorStopOwner::new();
        let context = ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(5),
            stop.view(),
            4096,
            64 * 1024,
        )
        .unwrap()
        .with_io_admission(io.clone());
        let original = io.original_result_scope(&context).unwrap();
        let wake = Arc::new(Wake::default());
        let cancellation = QueryCancellationView::governed(scope.cancellation().unwrap(), None);
        let gate = Arc::new(MvRefreshPreparationIo::new(
            MvRefreshPreparationServices {
                cpu: cpu.runtime(),
                scope: scope.clone(),
                window: window.clone(),
                cancellation: cancellation.clone(),
                runtime: tokio::runtime::Handle::current(),
                wake: wake.clone(),
                requests: Arc::new(NoRepositoryIo),
            },
            original,
        ));
        let (_product, repository) = crate::mv::domain::test_admission::keyed_product().await;
        let entrance = crate::mv::domain::test_admission::management_entrance();
        let readiness = Arc::new(crate::mv::domain::readiness::MvReadinessPort::from_product(
            novarocks_mv_application::readiness::MvKeyedReadinessService::new(
                repository,
                Arc::default(),
            ),
            tokio::runtime::Handle::current(),
            entrance.clone(),
        ));
        let host = Arc::new(novarocks_catalog_application::ConnectorControlHost::new());
        let ports = IcebergMvCorePorts::new_with_management_entrance(
            Arc::new(novarocks_sql::compiler::build_builtin_engine_function_catalog().unwrap()),
            Arc::new(crate::catalog_application::query_catalog::new_query_catalog_service()),
            None,
            host.clone(),
            host,
            readiness,
            Arc::new(novarocks_spi::connector::UnavailableMvStorageObservationPort),
            entrance,
            crate::application::test_constant_policy(),
        );
        let sql = "SELECT 1 AS id FROM ice.db.orders";
        let statement = MvCreateStatement {
            name_parts: vec!["mv".into()],
            if_not_exists: false,
            partition_by: None,
            distribution: None,
            refresh_policy: crate::mv::domain::application::MvCreateRefreshPolicy::Manual,
            select_sql: sql.into(),
            select_query: crate::mv::domain::refresh::definition::parse_mv_select_query(sql)
                .unwrap(),
            properties: Vec::new(),
            primary_key: None,
        };
        let source = MvRefreshPreparationOriginal::for_create(
            MvCreatePreparationInput {
                ports,
                planning: ConnectorControlPlanningLease::new(
                    Arc::new(
                        novarocks_catalog_application::test_support::test_control_binding_for(
                            novarocks_spi::connector::ConnectorInstanceId::parse("ice").unwrap(),
                            8,
                        ),
                    ),
                    || {},
                ),
                current_catalog: Some("ice".into()),
                current_database: "db".into(),
                statement,
                context,
            },
            gate.clone(),
        );
        let mut turn = MvRefreshTurnCustody::new(
            gate.clone(),
            cpu.runtime(),
            scope.clone(),
            window.clone(),
            cancellation,
            tokio::runtime::Handle::current(),
            wake.clone(),
        );
        assert!(turn.install_preparation(source.clone()).is_ok());
        // Stop only after the original input/reader and its strong owner exist.
        stop.request_stop();
        with_driver(
            &control,
            &mut driver,
            std::future::poll_fn(|cx| {
                wake.0.register(cx.waker());
                turn.poll_ready(cx)
            }),
        )
        .await
        .unwrap();
        assert!(turn.local_health_failure().is_none());
        let mut result = None;
        assert!(turn.preparation_original().take_result_in(&mut result));
        let Err(error) = result.as_ref().unwrap() else {
            panic!("stopped input cannot produce a plan");
        };
        let cause = error.original_failure().unwrap();
        assert_eq!(cause.failure_fact().class(), FailureClass::Control);
        assert_eq!(
            cause
                .downcast_ref::<novarocks_spi::connector::ConnectorError>()
                .unwrap()
                .kind(),
            ConnectorErrorKind::Cancelled
        );
        assert_eq!(control.snapshot().result_windows.held_positions, [0; 4]);
        with_driver(
            &control,
            &mut driver,
            std::future::poll_fn(|cx| turn.poll_closed(cx)),
        )
        .await
        .unwrap();
        assert!(!source.foreign_exit_unproven());
        // The receiving keeps the same result independently of Source retirement.
        let retained = cause.clone();
        drop(turn);
        drop(source);
        drop(gate);
        drop(result);
        assert_eq!(retained.failure_fact().class(), FailureClass::Control);
        let _ = with_driver(
            &control,
            &mut driver,
            std::future::poll_fn(|cx| io.poll_actual_quiescent_continuing(cx)),
        )
        .await
        .unwrap();
        drop(window);
        drop(capacity);
        drop(io);
        drop(scope);
        root.owner.complete();
        root.business.release();
        drop(driver);
        drop(control);
        cpu.shutdown_until(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
    }
}
