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

//! The frozen sequence node through LocalCompiler, batch construction and driver exit.

use arrow::{
    array::{Array, Int64Array},
    datatypes::DataType,
};
use novarocks_connector_contract::PureProviderProgramCatalog;
use novarocks_execution::{
    exec::pipeline::executor::run_compiled_component_for_test,
    runtime::{
        ExecutionRuntime, ExecutionRuntimeConfig, function_set::ExecutionFunctionSetBuilder,
        mem_tracker::MemTracker,
    },
};
use novarocks_expression_contract::{ExprKind, RootScope};
use novarocks_functions::ConstantValue;
use novarocks_local_compiler::{compile_fragment, validate_fragment_providers};
use novarocks_memory::{AuthorityConfig, MemoryAuthority};
use novarocks_physical_plan::{
    ConstantPoolId, ConstantPools, ConstantReference, FragmentBuilder, FragmentCuts, FragmentId,
    FragmentPackage, FragmentPackageAdmission, FragmentPackageInput, FragmentSink,
    FrozenFragmentPruning, PipelineDopDomain, PlanLimits, PlanVersionId,
    PropertyProofProjectionLimits, RequiredContracts, ResultField, ResultPort, ResultValueDomain,
    StagePlacementPolicy, ValueOrigin,
};
use novarocks_test_compile_to_execute::compiler_results::compile_options;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, EvaluationDemand, FunctionValueType,
    PureCompileControl, SemanticParameters,
};
use std::{collections::BTreeMap, sync::Arc};

struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        Ok(())
    }
}

fn package(bounds: [i64; 3], limit: Option<u64>) -> Arc<FragmentPackage> {
    let id = FragmentId::new(41);
    let mut builder = FragmentBuilder::new(id);
    let series = builder.reserve_node_id().unwrap();
    let ty = FunctionValueType::new(DataType::Int64, false);
    let value = builder
        .add_value(
            ty.clone(),
            ValueOrigin::NodeOutput {
                node: series,
                output_ordinal: 0,
            },
        )
        .unwrap();
    let policy = compile_options().constants;
    let mut constants = ConstantPools::empty();
    let mut ordinal = 0;
    let [start, stop, step] = bounds.map(|bound| {
        let constant = ConstantValue::from_i64(
            Arc::new(ty.try_to_field("bound").unwrap()),
            ty.clone(),
            bound,
            policy,
            CompilePhase::Validate,
            &Control,
        )
        .unwrap();
        let pool = ConstantPoolId::new(ordinal);
        ordinal += 1;
        constants.insert(pool, constant.pool().clone()).unwrap();
        let unit = builder.unit(series).unwrap();
        let definition = unit
            .add(
                None,
                ty.clone(),
                ExprKind::Constant(ConstantReference {
                    pool,
                    ordinal: constant.ordinal(),
                }),
            )
            .unwrap();
        unit.root(definition, EvaluationDemand::Value, RootScope::AllPorts)
            .unwrap()
    });
    builder
        .add_generate_series(series, start, stop, Some(step), value)
        .unwrap();

    let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Validate).unwrap();
    let ports = builder.node_ports(series).unwrap();
    let unit = builder
        .take_unit_builder(series)
        .unwrap()
        .seal(ports, &mut work)
        .unwrap()
        .finish(BTreeMap::new(), &mut work)
        .unwrap();
    builder.install_unit(series, Arc::new(unit)).unwrap();
    work.finish().unwrap();

    let root = if let Some(limit) = limit {
        let root = builder.reserve_node_id().unwrap();
        builder.add_limit(root, series, Some(limit), 0).unwrap();
        root
    } else {
        series
    };
    let fragment = builder
        .finish_definition(
            root,
            FragmentSink::Result,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
            StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let result = ResultPort {
        scalar_schema: None,
        fragment: id,
        output: fragment.nodes()[&root].output.clone(),
        fields: Box::from([ResultField {
            domain: ResultValueDomain::Plain,
            name: "sequential_number".into(),
            alias: None,
            value,
            ty,
        }]),
    };
    Arc::new(
        FragmentPackage::try_new(
            FragmentPackageInput {
                constants,
                version: PlanVersionId::try_new([41; 16]).unwrap(),
                required: RequiredContracts::default(),
                fragment,

                pruning: FrozenFragmentPruning::try_new(id, vec![], &Control).unwrap(),
                cuts: FragmentCuts::default(),
                result: Some(result),
                parameters: SemanticParameters::try_new([]).unwrap(),
                scans: BTreeMap::new(),
                writes: BTreeMap::new(),
                annotations: Box::default(),
                layouts: Default::default(),
            },
            FragmentPackageAdmission {
                plan_limits: PlanLimits::FROZEN,
                source_retained_bytes: 1 << 20,
                property_projection_limits: PropertyProofProjectionLimits {
                    max_request_bytes: 1 << 20,
                    max_coexisting_bytes: 2 << 20,
                    max_projection_work: 1 << 20,
                },
            },
            &Control,
        )
        .unwrap(),
    )
}

fn values(bounds: [i64; 3], limit: Option<u64>) -> (Vec<i64>, Vec<usize>) {
    let providers =
        PureProviderProgramCatalog::<std::io::Error>::try_new(&[], vec![], &Control).unwrap();
    let package =
        validate_fragment_providers(package(bounds, limit), &providers, &Control).unwrap();
    let mut functions = ExecutionFunctionSetBuilder::new();
    novarocks_sql::compiler::contribute_builtin_functions(functions.catalog_builder_mut()).unwrap();
    let functions = Arc::new(functions.seal().unwrap());
    let program = compile_fragment(
        package,
        &novarocks_sql_testkit::installed_builtin_owner_catalogue(),
        compile_options(),
        &Control,
    )
    .unwrap();

    let memory =
        Arc::new(MemoryAuthority::new(AuthorityConfig::new(64 << 20, 48 << 20, 16 << 20)).unwrap());
    let runtime = Arc::new(
        ExecutionRuntime::new(
            ExecutionRuntimeConfig {
                driver_threads: 1,
                exchange_io_max_inflight_bytes: 1024,
                exchange_max_transmit_batched_bytes: 1024,
                operator_buffer_chunks: 1,
                local_exchange_buffer_mem_limit_per_driver: 1024,
                local_exchange_max_buffered_rows: 1024,
                runtime_filter_scan_wait_time_ms_override: None,
                runtime_filter_wait_timeout_ms_override: None,
                sink_io_worker_threads: 1,
                sink_io_max_blocking_threads: 1,
            },
            functions,
            memory,
        )
        .unwrap(),
    );
    let tracker = MemTracker::new_root("table sequence fixture");
    tracker.install_limit_once(8 << 20).unwrap();
    let chunks =
        run_compiled_component_for_test(Arc::new(program), runtime.clone(), tracker.clone());
    runtime.shutdown_driver_execution().unwrap();
    runtime.shutdown_sink_io().unwrap();
    let chunks = chunks.unwrap();
    let mut values = Vec::new();
    let mut batch_lengths = Vec::new();
    for chunk in chunks {
        let schema = chunk.schema();
        assert_eq!(schema.field(0).name(), "sequential_number");
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert!(!schema.field(0).is_nullable());
        let batch = chunk.record_batch();
        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        values.extend(array.values().iter().copied());
        batch_lengths.push(array.len());
    }
    assert_eq!(tracker.current(), 0);
    (values, batch_lengths)
}

#[test]
fn table_sequence_compiled_batches_preserve_a_non_divisible_endpoint() {
    let (actual, batches) = values([20_017, 0, -2], None);
    let mut expected: Vec<_> = (1_i64..=20_017).step_by(2).collect();
    expected.reverse();
    assert_eq!(actual, expected);
    assert_eq!(batches, [8192, 1817]);
}

#[test]
fn table_sequence_limit_stops_a_full_bigint_range_at_its_prefix() {
    let (actual, batches) = values([i64::MIN, i64::MAX, 1], Some(3));
    assert_eq!(actual, [i64::MIN, i64::MIN + 1, i64::MIN + 2]);
    assert_eq!(batches, [3]);
}

#[test]
fn table_sequence_minimum_step_and_empty_internal_ranges() {
    let (actual, _) = values([i64::MAX, i64::MIN, i64::MIN], None);
    assert_eq!(actual, [i64::MAX, -1]);
    for endpoint in [i64::MIN, i64::MAX] {
        let (actual, _) = values([endpoint, endpoint, 1], None);
        assert_eq!(actual, [endpoint]);
    }
    let (actual, _) = values([i64::MAX, i64::MIN, 1], None);
    assert!(actual.is_empty());
}
