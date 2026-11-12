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

//! Large snapshots cross the real FE fact rounds and funded package encoder,
//! the Native funded receiver, LocalCompiler, and actual compiled pipeline.
//! The collector substitutes only the producer's external sink; socket delivery
//! and distributed admission remain separate Native integration obligations.
use super::completion::{NoExternalScans, request};
use super::*;
use arrow::{
    array::{Array, StringArray},
    datatypes::DataType,
};
use novarocks_plan_codec::physical_package_v2::{
    FROZEN_PACKAGE_ADMISSION, PackageDecodeLimits, PackageEncodeLimits,
};
use novarocks_query_application::{
    preparation::{FinalPlanCompletionDriver, ReadAccessSink},
    system_tables::{SystemTableRegistration, SystemTableRegistry, SystemTableScope},
};
use novarocks_sql::{compiler::*, system_table::SystemTableColumn};
use novarocks_type_contract::FunctionValueType;
use prost::Message;
use std::collections::BTreeMap;

fn registry(entries: usize, padding: &str) -> Arc<SystemTableRegistry> {
    let columns: Arc<[SystemTableColumn]> = Arc::from([SystemTableColumn {
        name: "value".into(),
        value_type: FunctionValueType::new(DataType::Utf8, false),
    }]);
    Arc::new(
        SystemTableRegistry::try_new((0..entries).map(|index| SystemTableRegistration {
            scope: SystemTableScope::System,
            schema: "runtime".into(),
            table: format!("entry_{index:05}_{padding}"),
            columns: columns.clone(),
            key_columns: Arc::from([]),
        }))
        .unwrap(),
    )
}
fn functions() -> novarocks_functions::PureEngineFunctionCatalog {
    use novarocks_functions::*;
    let actual = builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
    let mut builder = EngineFunctionCatalogBuilder::new();
    builder
        .register(
            actual
                .definition("rand", FunctionKind::Scalar)
                .unwrap()
                .clone(),
        )
        .unwrap();
    builder
        .seal_pure(
            [
                "builtin.scalar/rand/()->f64;strict;legacy",
                "builtin.scalar/rand/(i64)->f64;strict;legacy",
            ]
            .into_iter()
            .map(|overload| InstalledPureKernel {
                function: FunctionId::try_new("builtin.scalar/rand/v1").unwrap(),
                kind: FunctionKind::Scalar,
                implementation: PureImplementationDeclaration {
                    overload: FunctionOverloadId::try_new(overload).unwrap(),
                    implementation: PureImplementationId::try_new(
                        "builtin.scalar/rand/selected-v1",
                    )
                    .unwrap(),
                    abi: PureKernelAbi::ScalarV1,
                },
                aggregate_state_format: None,
            }),
        )
        .unwrap()
}
fn receive_and_run(
    bytes: &[u8],
    control: &SqlCompileControl,
) -> Vec<novarocks_execution::exec::chunk::Chunk> {
    use novarocks_execution::runtime::{
        execution_runtime::{ExecutionRuntime, ExecutionRuntimeConfig},
        function_set::ExecutionFunctionSetBuilder,
        mem_tracker::MemTracker,
        query_memory::QueryMemoryBinding,
    };
    use novarocks_memory::{
        AccountKind, AuthorityConfig, ExternalRef, LimitDimension, MemoryAuthority,
    };
    use novarocks_native_adapter::backend_task_execution::*;
    use novarocks_types::{
        QueryId,
        identity::{AttemptId, BackendProcessId, QueryExecutionId, StageId, TaskId},
    };
    let authority = Arc::new(
        MemoryAuthority::new(AuthorityConfig::new(512 << 20, 384 << 20, 128 << 20)).unwrap(),
    );
    let account = authority
        .create_account(AccountKind::Work, ExternalRef::NONE)
        .unwrap();
    account.install_policy(256 << 20, LimitDimension::Work);
    let execution =
        QueryExecutionId::new(QueryId::new(53, 65536), AttemptId::new(1).unwrap()).unwrap();
    let binding = QueryMemoryBinding::try_new(execution, authority.clone(), account).unwrap();
    let task = novarocks_execution_contract::TaskIdentity::new(
        execution,
        StageId::new(1).unwrap(),
        TaskId::new(1).unwrap(),
        BackendProcessId::new_v7(),
    );
    let original = node_test_support::NativeNodePreparation::new(task);
    let loan = original.loan();
    let mut type_journal = TypeMaterializationJournal::default();
    let mut project_journal = ProjectMetadataJournal::default();
    let mut type_host = TypeMaterializationHost::new(&binding, &loan, &mut type_journal);
    let mut project_host = ProjectMetadataHost::new(&binding, &loan, &mut project_journal);
    let model =
        novarocks_plan_codec::resource_preflight_v2::FragmentDecodeResourceModel::try_new(control)
            .unwrap();
    let providers = novarocks_connector_contract::PureProviderProgramCatalog::<
        std::convert::Infallible,
    >::try_new(&[], vec![], control)
    .unwrap();
    let compiler = CompiledPackageInterpreter::new(
        model,
        PackageDecodeLimits::FROZEN,
        Arc::new(functions()),
        Arc::new(providers),
        crate::application::test_constant_policy(),
    );
    let tracker = MemTracker::new_root("system snapshot Native component");
    tracker.install_limit_once(256 << 20).unwrap();
    let received = compile_native_package_with_funding_for_test(
        &compiler,
        bytes,
        CompiledTaskOptions {
            pipeline_dop: NonZeroUsize::MIN,
        },
        tracker.clone(),
        control,
        &mut type_host,
        &mut project_host,
    )
    .unwrap();
    let mut builder = ExecutionFunctionSetBuilder::new();
    novarocks_sql::compiler::contribute_builtin_functions(builder.catalog_builder_mut()).unwrap();
    let runtime = Arc::new(
        ExecutionRuntime::new(
            ExecutionRuntimeConfig {
                driver_threads: 1,
                exchange_io_max_inflight_bytes: 1 << 20,
                exchange_max_transmit_batched_bytes: 1 << 20,
                operator_buffer_chunks: 1,
                local_exchange_buffer_mem_limit_per_driver: 1 << 20,
                local_exchange_max_buffered_rows: 4096,
                runtime_filter_scan_wait_time_ms_override: None,
                runtime_filter_wait_timeout_ms_override: None,
                sink_io_worker_threads: 1,
                sink_io_max_blocking_threads: 1,
            },
            Arc::new(builder.seal().unwrap()),
            authority,
        )
        .unwrap(),
    );
    let chunks = novarocks_execution::exec::pipeline::executor::run_compiled_component_for_test(
        Arc::new(received.into_local_program_for_test()),
        runtime.clone(),
        tracker,
    )
    .unwrap();
    runtime.shutdown_driver_execution().unwrap();
    runtime.shutdown_sink_io().unwrap();
    // Retain the original preparation loan through real driver and sink shutdown.
    drop(loan);
    drop(original);
    chunks
}
async fn round_trip(rows: usize, padding_bytes: usize) {
    let padding = "a".repeat(padding_bytes);
    let provider = ListingFixture::new(FailurePoint::Views, ConnectorErrorKind::Unavailable);
    let mut config = ResultCapacityConfig::V1;
    // These explicit component grants cover the retained source, the locked
    // IPC operation's complete requests, and both coexisting prost buffers.
    // They are fixture inputs, not new product defaults or system-table caps.
    config.all_objects_bytes[0] = if padding_bytes == 0 {
        16 << 20
    } else {
        192 << 20
    };
    let mut fixture = Fixture::new_in(
        &provider,
        ResultWindowClass::Client,
        registry(rows - 1, &padding),
        config,
        Duration::from_secs(30),
    )
    .await;
    let source = fixture.source();
    let cpu = source
        .scope
        .planning
        .as_ref()
        .unwrap()
        .cpu
        .as_ref()
        .unwrap()
        .clone();
    let control = source.scope.planning.as_ref().unwrap().control.clone();
    let facts = Arc::new(statement_fact_source(
        source.owners.clone(),
        source.scope.clone(),
        Arc::new(NoExternalScans),
        source.clone(),
    ));
    let driver = FinalPlanCompletionDriver::new(facts.clone());
    let taken = ReadAccessSink::new();
    let completed = tokio::time::timeout(
        Duration::from_secs(30),
        driven(
            &fixture.control,
            &mut fixture.driver,
            driver.complete(
                request(
                    "SELECT table_name FROM system.information_schema.tables",
                    SqlCompileIntent::Query,
                    control.clone(),
                ),
                source.scope.capacity.scope(),
                &taken,
                cpu.as_ref(),
            ),
        ),
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    assert!(taken.into_taken().is_empty());
    let packages = crate::query_execution::package_freeze::extract_checked_packages(
        &completed,
        &BTreeMap::new(),
        &BTreeMap::new(),
        crate::application::test_constant_policy(),
        &FROZEN_PACKAGE_ADMISSION,
        &control,
    )
    .unwrap();
    let carrier = crate::query_execution::package_freeze::CompiledPackageCarrier::for_test(
        FROZEN_PACKAGE_ADMISSION,
        PackageEncodeLimits::FROZEN,
    );
    let mut producers = 0;
    for package in packages.values() {
        let Some(source_batches) = package
            .fragment()
            .nodes()
            .values()
            .find_map(|node| match &node.kind {
                novarocks_physical_plan::NodeKind::SystemRelation {
                    data: novarocks_physical_plan::SystemRelationData::Batches { batches, .. },
                    ..
                } => Some(batches),
                _ => None,
            })
        else {
            continue;
        };
        producers += 1;
        let sizes = source_batches
            .iter()
            .map(|batch| batch.num_rows())
            .collect::<Vec<_>>();
        assert!(sizes.len() > 1);
        assert_eq!(sizes.iter().sum::<usize>(), rows);
        let requests = std::cell::RefCell::new(Vec::new());
        let window = source.scope.capacity.window_alias();
        let before = window.retained_payload_bytes();
        let artifact = crate::query_execution::required_constant_eval::with_root_value_creation(
            source.scope.capacity.root_scope(), &window, &control,
            |creation| {
                let storage = creation.storage_lifetime();
                let original = std::cell::RefCell::new(creation);
                let mut request = |layout: std::alloc::Layout| {
                    requests.borrow_mut().push(layout.size());
                    original.borrow_mut().request_layout(layout)
                };
                let mut opaque = |bytes| {
                    requests.borrow_mut().push(bytes);
                    original.borrow_mut().observe_opaque_upper_bound(bytes)
                };
                let mut observed = novarocks_type_contract::ValueSchemaCreation::new(storage, &mut request, &mut opaque);
                crate::native::fragment_encoder::frozen::FragmentArtifact::freeze_package_with_creator(
                    package, completed.plan().edges(), &carrier, &control, &mut observed,
                )
            },
        ).unwrap();
        if artifact.is_err() {
            let mut largest = requests.borrow().clone();
            largest.sort_unstable_by(|a, b| b.cmp(a));
            eprintln!(
                "system encoding capacity: rows={rows}, budget={}, before={before}, request_count={}, requested={}, largest={:?}",
                window.payload_capacity_bytes(),
                requests.borrow().len(),
                requests.borrow().iter().sum::<usize>(),
                &largest[..largest.len().min(12)]
            );
        }
        let artifact = artifact.unwrap();
        let envelope = novarocks_proto_models::novarocks::FrozenFragment::decode(
            artifact.content().bytes().clone(),
        )
        .unwrap();
        if padding_bytes != 0 {
            assert!(
                envelope.package.len() > 16 << 20,
                "this real FE package crosses the retired descriptor ceiling"
            );
            assert!(
                artifact.content().bytes().len() < novarocks_execution_contract::native_result_support::NATIVE_GRPC_DECODED_MESSAGE_MAX_BYTES,
                "default Native message geometry includes the indivisible package"
            );
        }
        let chunks = receive_and_run(&envelope.package, &control);
        assert_eq!(
            chunks.iter().map(|chunk| chunk.len()).collect::<Vec<_>>(),
            sizes,
            "the actual compiled Source keeps every original batch"
        );
        let mut ordinal = 0;
        for chunk in &chunks {
            let column = chunk
                .record_batch()
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for row in 0..column.len() {
                assert!(!column.is_null(row));
                let expected = if ordinal == 0 {
                    "materialized_views".into()
                } else {
                    format!("entry_{:05}_{padding}", ordinal - 1)
                };
                assert_eq!(column.value(row), expected);
                ordinal += 1;
            }
        }
        assert_eq!(ordinal, rows);
        drop(chunks);
        drop(envelope);
        drop(artifact);
    }
    assert_eq!(producers, 1);
    assert!(provider.calls().is_empty());
    drop(packages);
    drop(completed);
    drop(driver);
    drop(facts);
    drop(cpu);
    drop(source);
    fixture.finish(false).await;
}
#[tokio::test]
async fn original_fe_snapshot_65536_rows_reaches_funded_native_receiver_and_actual_cursor() {
    round_trip(65_536, 0).await;
}
#[tokio::test]
async fn original_fe_system_package_above_16_mib_reaches_funded_native_receiver_and_actual_cursor()
{
    round_trip(32_768, 512).await;
}

#[tokio::test]
async fn indivisible_system_value_above_the_empty_original_window_keeps_typed_resource_refusal() {
    let provider = ListingFixture::new(FailurePoint::Views, ConnectorErrorKind::Unavailable);
    let fixture = Fixture::new(&provider, ResultWindowClass::Client).await;
    let source = fixture.source();
    let declaration = match source.owners.system_relations().declare(
        &novarocks_sql::system_table::SystemTableBinding::System,
        "metadata",
        "materialized_views",
    ) {
        novarocks_query_application::system_tables::SystemTableLookup::Declared(declaration) => {
            declaration
        }
        _ => panic!("standard materialized-view declaration"),
    };
    source.begin_snapshot(declaration, &[9]).unwrap();
    let input = "x".repeat(9 << 20);
    let mut values = [None; 10];
    values[9] = Some(input.as_str());
    let error = source.stage_row(&values).unwrap_err();
    let SqlCompileError::CompilationSource(cause) = sql_error(error) else {
        panic!("original resource author must survive the snapshot adapter")
    };
    assert_eq!(
        cause.failure_fact().class(),
        novarocks_failure_contract::FailureClass::Resource
    );
    assert_eq!(
        input.len(),
        9 << 20,
        "the borrowed input is unchanged by refusal"
    );
    assert!(provider.calls().is_empty());
    drop(source);
    fixture.finish(false).await;
}
