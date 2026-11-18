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

//! Real local Hadoop + planning CPU + governed metadata CPU/I/O composition.

use super::*;
use crate::query::{
    property_cpu_context::PropertyCpuContext, table_property_computation::TablePropertyCpuInput,
};
use crate::query_execution::{
    completion_facts::FrontendPlanningCpuSource,
    internal_consumer::domain_cpu::root_action_tests::{drive, root_control},
    internal_result_cpu::InternalResultCpuOwner,
};
use crate::task_execution::blocking_io::tests::{OriginalExecutorsFixture, original_executors};
use novarocks_catalog_application::ConnectorControlHost;
use novarocks_connector_iceberg::{
    access_binding::IcebergReadBinding,
    connector_factory::IcebergConnectorFactory,
    iceberg::spec::{FormatVersion, Literal, PrimitiveType, TableMetadata, Transform, Type},
    novarocks_fs,
    resources::IcebergMetadataResources,
};
use novarocks_query_application::{
    api::CommandContext,
    cancellation::QueryCancellationView,
    session_control::{ClientSessionKey, SessionToken, StatementToken},
    session_error::QueryServiceErrorKind,
};
use novarocks_spi::connector::*;
use novarocks_sql::compiler::{SqlCancellationObservation, SqlCompileControl};
use novarocks_workload_control::{
    BusinessPermit, CancellationReason, ResultWindowAlias, WorkClass, WorkOwner, WorkRequest,
    WorkScope, WorkloadControl, joint_admission::JointProgressDriver,
};
use std::{
    sync::{Mutex, RwLock, Weak},
    task::Waker,
    thread::ThreadId,
    time::{Duration, Instant},
};

mod like_read_tests;

const DDL: &str = "CREATE TABLE ice.atomic.events (id BIGINT NOT NULL DEFAULT 7, event_day DATE, amount DECIMAL(10,2) DEFAULT '12.30') PARTITION BY (month(event_day)) PROPERTIES ('format-version'='3') COMMENT 'owned source'";

struct CpuWitness {
    cancellation: QueryCancellationView,
    first_thread: Mutex<Option<ThreadId>>,
}
impl SqlCancellationObservation for CpuWitness {
    fn is_cancelled(&self) -> bool {
        self.first_thread
            .lock()
            .unwrap()
            .get_or_insert_with(|| std::thread::current().id());
        self.cancellation.is_cancelled()
    }
}

struct Fixture {
    warehouse: tempfile::TempDir,
    host: ConnectorControlHost,
    key: ConnectorProviderBindingKey,
    binding_weak: Weak<ConnectorControlBinding>,
    metadata_weak: Weak<dyn ConnectorMetadata>,
    control: WorkloadControl,
    driver: JointProgressDriver,
    creator: InternalResultCpuOwner,
    physical: OriginalExecutorsFixture,
    planning: Option<Arc<FrontendPlanningCpuSource>>,
    owner: Option<WorkOwner>,
    business: Option<BusinessPermit>,
    scope: WorkScope,
    window: Option<ResultWindowAlias>,
    cancellation: novarocks_workload_control::CancellationView,
    query_cancellation: QueryCancellationView,
    command: Option<CommandContext>,
    retired: bool,
}
impl Fixture {
    fn new() -> Self {
        Self::new_with_provider_setup(|_, _| {})
    }

    fn new_with_provider_setup(
        seed_source: impl FnOnce(&ConnectorCatalogMutationSession, &ConnectorInstanceId),
    ) -> Self {
        let runtime = tokio::runtime::Handle::current();
        let warehouse = tempfile::tempdir().unwrap();
        let resources = IcebergMetadataResources::new(
            IcebergReadBinding::new(
                None,
                novarocks_fs::FsAccessResolver::new(),
                Arc::new(novarocks_fs::TokioFileIoRuntime::new(runtime.clone())),
                novarocks_fs::TokioRefreshExecutor::shared(runtime.clone()),
                Arc::new(novarocks_fs::TokioFileIoSpawner::new(runtime.clone())),
            ),
            runtime.clone(),
        );
        let factory = IcebergConnectorFactory::new(resources);
        let instance = ConnectorInstanceId::parse("ice").unwrap();
        let properties = vec![(
            "iceberg.catalog.warehouse".to_owned(),
            warehouse.path().display().to_string(),
        )];
        let catalog_properties = CatalogProperties::new(
            CatalogHandle::new(instance.clone(), CatalogVersion::from_bytes([0x39; 32])),
            factory.provider_id().clone(),
            1,
            properties
                .iter()
                .map(|(name, value)| CatalogProperty::new(name, value).unwrap())
                .collect(),
            Vec::new(),
        )
        .unwrap();
        let request = ConnectorControlFactoryRequest::try_new(
            factory.provider_id().clone(),
            instance.clone(),
            properties,
        )
        .unwrap()
        .with_catalog_properties(catalog_properties.clone())
        .unwrap();
        let (binding, _durable) = factory.create_control(request).unwrap().into_parts();
        let host = ConnectorControlHost::new();
        host.register(binding.with_catalog_properties(catalog_properties).unwrap())
            .unwrap();
        // Real Provider setup precedes this statement's Root admission. It is
        // not counted as the Table Action's execution or resource evidence.
        {
            let setup = host
                .acquire_current(&instance)
                .unwrap()
                .derive_mutation_lease()
                .unwrap();
            let outcome = setup
                .execute_operation(
                    ConnectorMutationOperationId::new(),
                    ConnectorCatalogMutationOperation::CreateNamespace {
                        properties: Vec::new(),
                        namespace: ConnectorNamespaceIdentity {
                            instance_id: instance.clone(),
                            namespace: Arc::from("atomic"),
                        },
                        policy: CreatePolicy::FailIfExists,
                    },
                    crate::connector::test_request_context(),
                )
                .unwrap();
            assert!(matches!(
                outcome,
                ExternalMutationOutcome::KnownCommitted {
                    effect: ExternalMutationEffect::Applied,
                    finalization: ExternalMutationFinalization::Complete,
                    cleanup: ExternalMutationFinalization::Complete,
                    ..
                }
            ));
            seed_source(&setup, &instance);
        }
        let planning_pin = host.acquire_current(&instance).unwrap();
        let key = ConnectorProviderBindingKey {
            instance_id: instance,
            incarnation: planning_pin.binding().incarnation(),
        };
        let binding_weak = Arc::downgrade(planning_pin.binding());
        let metadata_weak = Arc::downgrade(planning_pin.binding().metadata());
        drop(planning_pin);
        let mut creator = InternalResultCpuOwner::try_new_for_test().unwrap();
        let (control, _handle, driver) = root_control(&mut creator);
        let (root, window) = control
            .root_admission()
            .try_begin_root_with_result(
                WorkRequest::new(WorkClass::Management),
                novarocks_workload_control::ResultWindowClass::Local,
            )
            .unwrap();
        let scope = root.owner.scope();
        assert!(matches!(
            creator.runtime().provider_creator_source(&scope),
            Err(WorkError::Conflict)
        ));
        let cancellation = scope.cancellation().unwrap();
        let query_cancellation = QueryCancellationView::governed(cancellation.clone(), None);
        let connector_context = crate::connector::connector_request_context_for_query_on_runtime(
            &runtime,
            None,
            query_cancellation.clone(),
        )
        .unwrap();
        let window = window.retain_alias();
        let command = CommandContext::new(
            scope.clone(),
            connector_context,
            StatementToken::new(SessionToken::new(ClientSessionKey::new(71).unwrap()), 1),
            "fixture-user",
        )
        .with_result_window(window.clone())
        .unwrap();
        let (physical, supervisor) = original_executors(runtime);
        supervisor
            .install_provider_creator_cpu(creator.runtime())
            .unwrap();
        let planning = FrontendPlanningCpuSource::new(supervisor);
        Self {
            warehouse,
            host,
            key,
            binding_weak,
            metadata_weak,
            control,
            driver,
            creator,
            physical,
            planning: Some(planning),
            owner: Some(root.owner),
            business: Some(root.business),
            scope,
            window: Some(window),
            cancellation,
            query_cancellation,
            command: Some(command),
            retired: false,
        }
    }
    async fn action(&mut self) -> (Action, ConnectorMutationOperationId) {
        self.action_for(DDL, None, None).await
    }

    async fn action_for(
        &mut self,
        ddl: &str,
        like_columns: Option<Vec<novarocks_sql::semantic::TableColumnDef>>,
        continued_io: Option<Arc<MetadataIoAdmission>>,
    ) -> (Action, ConnectorMutationOperationId) {
        let planning_pin = self.host.acquire_current(&self.key.instance_id).unwrap();
        assert_eq!(planning_pin.binding().incarnation(), self.key.incarnation);
        let binding = CatalogTablePropertyBinding::try_new(
            ConnectorTableIdentity {
                instance_id: self.key.instance_id.clone(),
                namespace: Arc::from("atomic"),
                table: Arc::from("events"),
            },
            planning_pin,
        )
        .unwrap();
        let statement = novarocks_query_application::sql::parse_single_statement(ddl).unwrap();
        let novarocks_parser::ast::Statement::Table(novarocks_parser::ast::TableStatement::Create(
            statement,
        )) = statement
        else {
            panic!("real old-parser CREATE TABLE input");
        };
        let witness = Arc::new(CpuWitness {
            cancellation: self.query_cancellation.clone(),
            first_thread: Mutex::new(None),
        });
        let input=TablePropertyCpuInput {
            source:Arc::from(ddl),statement,binding,command:self.command.as_ref().unwrap().clone(),like_columns,
            context:PropertyCpuContext {
                current_database:"atomic".into(),sql_semantics:Default::default(),
                interpreter:Arc::new(crate::query_execution::required_constant_eval::test_required_constant_interpreter()),
                control:SqlCompileControl::new(None,witness.clone()),scope:self.scope.clone(),
                window:self.window.as_ref().unwrap().clone(),catalog:Arc::new(RwLock::new(Default::default())),
                functions:Arc::new(novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog().unwrap()),
                policy:crate::application::test_constant_policy(),
            },
        };
        let source = self.planning.as_ref().unwrap().clone();
        let prepared =
            tokio::time::timeout(Duration::from_secs(10), source.table_properties(input))
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        assert_eq!(
            self.planning
                .as_ref()
                .unwrap()
                .local_health_failure()
                .is_none(),
            true
        );
        assert_ne!(
            witness
                .first_thread
                .lock()
                .unwrap()
                .expect("real property control observed"),
            std::thread::current().id()
        );
        // Observe the actual original planning Source close before handing its
        // CPU-produced sealed input to the Action. No try_new call occurs here.
        tokio::time::timeout(Duration::from_secs(10), source.close())
            .await
            .unwrap()
            .unwrap();
        assert!(source.local_health_failure().is_none());
        let operation_id = prepared.original_request.operation_id;
        assert_eq!(prepared.execute_request.operation_id, operation_id);
        assert_eq!(prepared.original_request.target, self.key);
        let action = prepare_owned(
            prepared,
            continued_io,
            self.command.as_ref().unwrap().clone(),
            self.creator.runtime(),
            self.query_cancellation.clone(),
            tokio::runtime::Handle::current(),
        )
        .unwrap();
        (action, operation_id)
    }
    fn poll(
        &mut self,
        action: &mut Action,
        cx: &mut Context<'_>,
    ) -> Poll<Result<StatementActionValue, QueryServiceError>> {
        let mut context = GovernedActionContext::new(
            &self.scope,
            &mut self.owner,
            &self.cancellation,
            None,
            self.window.as_ref(),
        );
        action.poll_action(&mut context, cx)
    }
    async fn finish_action(
        &mut self,
        action: &mut Action,
    ) -> Result<StatementActionValue, QueryServiceError> {
        tokio::time::timeout(
            Duration::from_secs(10),
            std::future::poll_fn(|cx| {
                drive(&mut self.driver);
                let result = self.poll(action, cx);
                drive(&mut self.driver);
                if result.is_pending() {
                    cx.waker().wake_by_ref();
                }
                result
            }),
        )
        .await
        .expect("same Action/Provider/CPU/IO must actually finish")
    }
    fn retire_current(&mut self) {
        assert!(!self.retired);
        self.host.retire_current(&self.key.instance_id).unwrap();
        self.retired = true;
        assert!(self.host.acquire_current(&self.key.instance_id).is_err());
    }
    async fn close(mut self, mut action: Action, cancelled: bool) {
        assert!(action.delivered);
        assert!(action.local_health_failure().is_none());
        assert_actual_closed(&action);
        if !self.retired {
            self.retire_current();
        }
        assert!(
            self.host.take_ready_retires().unwrap().is_empty(),
            "original Source still owns its actual lease"
        );
        let mut residual = None;
        action.handoff_residual_in(&mut residual).unwrap();
        tokio::time::timeout(
            Duration::from_secs(10),
            std::future::poll_fn(|cx| residual.as_mut().unwrap().poll_retirement(cx)),
        )
        .await
        .unwrap()
        .unwrap();
        drop(residual);
        drop(action);
        self.close_retired_resources(cancelled).await;
    }

    async fn close_retired_resources(mut self, cancelled: bool) {
        let retires = self.host.take_ready_retires().unwrap();
        assert_eq!(retires.len(), 1);
        assert_eq!(retires[0].key, self.key);
        assert!(self.binding_weak.upgrade().is_none());
        assert!(self.metadata_weak.upgrade().is_none());
        self.planning.as_ref().unwrap().close().await.unwrap();
        drop(self.planning.take());
        self.creator
            .shutdown_until(Instant::now() + Duration::from_secs(10))
            .await
            .unwrap();
        self.physical.shutdown().await;
        assert_eq!(self.window.as_ref().unwrap().retained_payload_bytes(), 0);
        drop(self.command.take());
        drop(self.window.take());
        self.business.take().unwrap().release();
        let owner = self.owner.take().unwrap();
        if cancelled {
            owner.complete_after_terminal_cancel_settled();
        } else {
            owner.complete();
        }
        // These fixture-owned aliases retain the original cancellation cell
        // even after the action and Root have released their own aliases.
        drop(self.query_cancellation);
        drop(self.cancellation);
        self.driver.recheck_retired_backing();
        drive(&mut self.driver);
        let observed = self.control.snapshot();
        assert_eq!(observed.root_responsibilities, 0);
        assert_eq!(observed.businesses, 0);
        assert_eq!(observed.obligations, 0);
        assert_eq!(observed.result_windows.held_positions, [0; 4]);
        let mut control = self.control;
        control.close_admission();
        // Dropping the request aborts its cancellation relay. Its real exit
        // can release the final cancellation view on a different worker.
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let before = control.progress_revision();
                self.driver.recheck_retired_backing();
                drive(&mut self.driver);
                match control.shutdown() {
                    Ok(_) => break,
                    Err(failure) => {
                        let (error, original) = failure.into_parts();
                        assert_eq!(
                            error,
                            novarocks_workload_control::WorkloadShutdownError::NotDrained
                        );
                        control = original;
                    }
                }
                control.wait_progress(before).await;
            }
        })
        .await
        .expect("same governor must actually drain");
    }
}

fn assert_actual_closed(action: &Action) {
    let source = action.source.as_ref().unwrap();
    assert!(source.closed);
    assert!(source.execute.closed());
    assert!(source.reconcile.closed());
    assert!(source.health().is_none());
    let mut cx = Context::from_waker(Waker::noop());
    if let Some(cpu) = source.metadata_cpu.as_ref() {
        assert!(matches!(cpu.poll_closed(&mut cx), Poll::Ready(Ok(()))));
    } else {
        assert!(!source.execute.started);
    }
    let io = source.io.as_ref().unwrap();
    if matches!(io.root_capacity(), Err(WorkError::Released)) {
        // The same admission already recorded its withdrawal. Reobserving
        // this receipt cannot install or close an I/O source.
        assert!(!source.execute.started);
        assert!(io.withdraw_unstarted_in().unwrap().is_some());
    } else {
        assert!(matches!(
            io.poll_actual_quiescent_continuing(&mut cx),
            Poll::Ready(Ok(_))
        ));
    }
}
fn committed_metadata(
    action: &Action,
    operation_id: ConnectorMutationOperationId,
) -> TableMetadata {
    let source = action.source.as_ref().unwrap();
    let receipt = {
        let observed = source.execute.receiving.observation();
        assert!(observed.provider_exited());
        assert!(!source.execute.receiving.foreign_exit_unproven());
        let Some(Ok(Outcome::KnownCommitted {
            effect: ExternalMutationEffect::Applied,
            receipt,
            finalization: ExternalMutationFinalization::Complete,
            cleanup: ExternalMutationFinalization::Complete,
        })) = observed.validated_outcome()
        else {
            panic!("real Hadoop CREATE must supply an Applied committed outcome");
        };
        assert_eq!(receipt.operation_id(), operation_id);
        assert_eq!(receipt.operation_kind(), "create-table");
        assert_eq!(Some(receipt), observed.applied_receipt());
        assert!(observed.original_owner().is_some());
        receipt.clone()
    };
    let version: serde_json::Value =
        serde_json::from_slice(receipt.provider_version().unwrap()).unwrap();
    let location = version["metadata_location"].as_str().unwrap();
    let file = if location.starts_with("file:") {
        url::Url::parse(location).unwrap().to_file_path().unwrap()
    } else {
        std::path::PathBuf::from(location)
    };
    let metadata: TableMetadata = serde_json::from_slice(&std::fs::read(file).unwrap()).unwrap();
    assert_eq!(
        version["table_uuid"].as_str().unwrap(),
        metadata.uuid().to_string()
    );
    metadata
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_hadoop_create_keeps_same_source_after_borrowed_wait_drop_and_releases_owners() {
    let mut fixture = Fixture::new();
    let (mut action, operation_id) = fixture.action().await;
    // Drive existing facts only before each poll, so a new Provider/CPU/IO
    // request cannot be mistaken for an already-observed completion.
    tokio::time::timeout(
        Duration::from_secs(10),
        std::future::poll_fn(|cx| {
            drive(&mut fixture.driver);
            let result = fixture.poll(&mut action, cx);
            assert!(
                result.is_pending(),
                "the test must observe a real pending Provider cut"
            );
            let source = action.source.as_ref().unwrap();
            if source.execute.started && !source.execute.closed() {
                Poll::Ready(())
            } else {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }),
    )
    .await
    .unwrap();
    {
        let mut borrowed = Box::pin(std::future::poll_fn(|cx| fixture.poll(&mut action, cx)));
        assert!(
            borrowed
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
    }
    assert!(action.source.as_ref().unwrap().execute.started);
    assert!(!action.source.as_ref().unwrap().execute.closed());
    assert!(
        !action
            .source
            .as_ref()
            .unwrap()
            .execute
            .receiving
            .observation()
            .provider_exited()
    );
    fixture.retire_current();
    assert!(fixture.host.take_ready_retires().unwrap().is_empty());
    let result = fixture.finish_action(&mut action).await.unwrap();
    assert!(matches!(
        result,
        StatementActionValue::Ok(StatementCommandCompletion::Empty)
    ));
    assert_eq!(
        action.external_effect_disposition(),
        Some(Effect::KnownCommitted)
    );
    assert_actual_closed(&action);
    let metadata = committed_metadata(&action, operation_id);
    assert_eq!(metadata.format_version(), FormatVersion::V3);
    assert_eq!(
        metadata.properties().get("comment").map(String::as_str),
        Some("owned source")
    );
    let schema = metadata.current_schema();
    assert_eq!(
        schema
            .as_struct()
            .fields()
            .iter()
            .map(|f| f.name.as_str())
            .collect::<Vec<_>>(),
        ["id", "event_day", "amount"]
    );
    let id = schema.field_by_name("id").unwrap();
    assert_eq!(
        id.field_type.as_ref(),
        &Type::Primitive(PrimitiveType::Long)
    );
    assert!(id.required);
    assert_eq!(id.write_default, Some(Literal::long(7)));
    let date = schema.field_by_name("event_day").unwrap();
    assert_eq!(
        date.field_type.as_ref(),
        &Type::Primitive(PrimitiveType::Date)
    );
    assert!(!date.required);
    let amount = schema.field_by_name("amount").unwrap();
    assert_eq!(
        amount.field_type.as_ref(),
        &Type::Primitive(PrimitiveType::Decimal {
            precision: 10,
            scale: 2
        })
    );
    assert_eq!(amount.write_default, Some(Literal::decimal(1230)));
    let partition = metadata.default_partition_spec().fields();
    assert_eq!(partition.len(), 1);
    assert_eq!(partition[0].source_id, date.id);
    assert_eq!(partition[0].transform, Transform::Month);
    fixture.close(action, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_uninstalled_bootstrap_withdraws_without_provider_entry() {
    let mut fixture = Fixture::new();
    let (mut action, _) = fixture.action().await;
    assert!(
        fixture
            .poll(&mut action, &mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    {
        let source = action.source.as_ref().unwrap();
        assert!(source.io.is_some());
        assert!(!source.bootstrap_ready);
        assert!(!source.execute.started);
        assert!(source.io.as_ref().unwrap().root_capacity().is_err());
    }
    // Granting a reply is not installing it in the Source.
    drive(&mut fixture.driver);
    fixture
        .owner
        .as_ref()
        .unwrap()
        .cancel(CancellationReason::DeadlineExceeded);
    let error = fixture
        .finish_action(&mut action)
        .await
        .err()
        .expect("original cancellation");
    assert_eq!(error.kind(), QueryServiceErrorKind::Timeout);
    assert_eq!(
        error.user_error().unwrap().code().as_str(),
        "query.execution.deadline_exceeded"
    );
    let source = action.source.as_ref().unwrap();
    assert!(!source.execute.started);
    assert!(!source.execute.receiving.observation().dispatched());
    assert!(matches!(
        source.io.as_ref().unwrap().root_capacity(),
        Err(WorkError::Released)
    ));
    assert_actual_closed(&action);
    assert!(!fixture.warehouse.path().join("atomic/events").exists());
    fixture.close(action, true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_installed_bootstrap_requires_actual_quiescence_after_cancel() {
    let mut fixture = Fixture::new();
    let (mut action, _) = fixture.action().await;
    tokio::time::timeout(
        Duration::from_secs(10),
        std::future::poll_fn(|cx| {
            drive(&mut fixture.driver);
            let result = fixture.poll(&mut action, cx);
            assert!(result.is_pending());
            let source = action.source.as_ref().unwrap();
            if source
                .io
                .as_ref()
                .is_some_and(|io| io.root_capacity().is_ok())
            {
                assert!(
                    !source.execute.started,
                    "this cut is before the first IO grant, not Provider execution"
                );
                Poll::Ready(())
            } else {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }),
    )
    .await
    .unwrap();
    assert!(
        action
            .source
            .as_ref()
            .unwrap()
            .io
            .as_ref()
            .unwrap()
            .withdraw_unstarted_in()
            .unwrap()
            .is_none()
    );
    fixture
        .owner
        .as_ref()
        .unwrap()
        .cancel(CancellationReason::DeadlineExceeded);
    let error = fixture
        .finish_action(&mut action)
        .await
        .err()
        .expect("original cancellation");
    assert_eq!(error.kind(), QueryServiceErrorKind::Timeout);
    assert_eq!(
        error.user_error().unwrap().code().as_str(),
        "query.execution.deadline_exceeded"
    );
    assert_actual_closed(&action);
    assert!(!action.source.as_ref().unwrap().execute.started);
    assert!(!fixture.warehouse.path().join("atomic/events").exists());
    fixture.close(action, true).await;
}
