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

//! Actual private Union catalogue -> Physical freeze -> compiler -> factories.
use super::*;
use arrow::datatypes::DataType;
use novarocks_functions::{
    EngineFunctionCatalogBuilder, FunctionKind, InstalledPureKernel, PureEngineFunctionCatalog,
};
use novarocks_type_contract::FunctionValueType;

fn union_catalog() -> PureEngineFunctionCatalog {
    let actual = novarocks_functions::builtin::catalogue::percentile_union_private_test_catalog();
    let definition = actual
        .definition("percentile_union", FunctionKind::Aggregate)
        .unwrap();
    let declaration = definition.binding_declaration().unwrap();
    let installed = declaration
        .overloads()
        .iter()
        .map(|overload| {
            let actual_implementation = actual
                .pure_overload_declaration_observed(
                    declaration.function_id(),
                    declaration.kind(),
                    &overload.identity,
                    &FixtureControl,
                )
                .unwrap();
            InstalledPureKernel {
                function: declaration.function_id().clone(),
                kind: declaration.kind(),
                implementation: actual_implementation.implementation().clone(),
                aggregate_state_format: overload
                    .aggregate
                    .as_ref()
                    .map(|state| state.state_format.clone()),
            }
        })
        .collect::<Vec<_>>();
    let mut builder = EngineFunctionCatalogBuilder::new();
    builder.register(definition.clone()).unwrap();
    builder.seal_pure(installed).unwrap()
}
fn union_program(ty: DataType, two_phase: bool) -> Arc<LocalProgram> {
    let fixture_sources = crate::exec::operators::family_fixture::constant_sources::Sources::new();
    let parameters = novarocks_type_contract::SemanticParameters::try_new([]).unwrap();

    let catalog = union_catalog();
    let bound = bind(
        &catalog,
        "percentile_union",
        &[FunctionValueType::new(ty.clone(), true)],
    );
    let fragment = FragmentId::new(731);
    let mut builder =
        crate::exec::operators::family_fixture::constant_sources::FragmentBuilder::new(
            fragment,
            &fixture_sources,
        );
    let source = builder.reserve_node_id().unwrap();
    let input = values(
        &mut builder,
        source,
        &[FunctionValueType::new(ty, true)],
        &[],
    );
    let sequence = AggregateSequenceId::new(1);
    let (first, state) = add_aggregate(
        &mut builder,
        source,
        &[],
        &[CallSpec {
            bound: &bound,
            phase: if two_phase {
                AggregatePhase::Partial { sequence }
            } else {
                AggregatePhase::Single
            },
            id: AggregateCallId::new(1),
            arguments: input,
            distinct: false,
        }],
        if two_phase {
            AggregateGrouping::Partial
        } else {
            AggregateGrouping::Complete
        },
    );
    let root = if two_phase {
        add_aggregate(
            &mut builder,
            first,
            &[],
            &[CallSpec {
                bound: &bound,
                phase: AggregatePhase::Final { sequence },
                id: AggregateCallId::new(2),
                arguments: state,
                distinct: false,
            }],
            AggregateGrouping::Complete,
        )
        .0
    } else {
        first
    };
    let definition = finish(
        builder,
        root,
        FragmentSink::Result,
        2,
        &catalog,
        &parameters,
    );
    let output = definition.nodes()[&root].output.clone();
    let mut plan = crate::exec::operators::family_fixture::constant_sources::PlanBuilder::new(
        PlanVersionId::try_new([73; 16]).unwrap(),
        &fixture_sources,
    )
    .with_semantic_parameters(parameters.clone());
    plan.add_fragment(definition).unwrap();
    plan.set_result_port(ResultPort {
        scalar_schema: None,
        fragment,
        fields: output
            .columns
            .iter()
            .map(|value| ResultField {
                domain: crate::test_result_domain::result_value_domain(&bound.result_type()),
                name: "state".into(),
                alias: None,
                value: *value,
                ty: bound.result_type(),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice(),
        output,
    })
    .unwrap();
    let plan = plan.finish_observed(&FixtureControl).unwrap();
    compile(
        packages(&plan).remove(&fragment).unwrap(),
        &catalog,
        2,
        true,
    )
}
fn union_chunk(program: &LocalProgram, array: ArrayRef) -> Chunk {
    let node = program
        .graph()
        .nodes()
        .iter()
        .find(|node| matches!(node.kind(), ProgramNodeKind::Values { .. }))
        .unwrap();
    let schema = ChunkSchema::from_compiled_layout(node.output_layout()).unwrap();
    // Fixture ingress performs the same representation-only normalization as
    // an actual source author. Real Struct names and declared semantics survive.
    let array = novarocks_functions::array_projection::normalize_execution_array(&array).unwrap();
    Chunk::try_new_with_chunk_schema(
        RecordBatch::try_new(schema.arrow_schema_ref(), vec![array]).unwrap(),
        schema,
    )
    .unwrap()
}
fn encoded(count: usize) -> Vec<u8> {
    let mut state = novarocks_functions::approx_percentile_core::PercentileState::default();
    for n in 0..count {
        novarocks_functions::approx_percentile_core::add_value(&mut state, n as f64).unwrap();
    }
    novarocks_functions::approx_percentile_core::encode_state(&state)
        .expect("percentile codec fixture")
}
#[test]
fn percentile_union_private_real_factory_single_partial_final_binary_exact() {
    use novarocks_functions::AggregateKernelPhase as P;
    let bytes = encoded(2);
    let array = Arc::new(BinaryArray::from(
        (0..321)
            .map(|n| {
                if n % 7 == 0 {
                    None
                } else {
                    Some(bytes.as_slice())
                }
            })
            .collect::<Vec<_>>(),
    )) as ArrayRef;
    let tracker = MemTracker::new_root("UnionActualFactories");
    let single = union_program(DataType::Binary, false);
    let chunk = union_chunk(&single, array.clone());
    let actual = run(&factory(&single, P::Single), &[chunk], tracker.clone());
    let mut expected = novarocks_functions::approx_percentile_core::PercentileState::default();
    for n in 0..321 {
        novarocks_functions::approx_percentile_aggregate_core::merge_row(&mut expected,&array,n,
        novarocks_functions::approx_percentile_aggregate_core::ApproxPercentileDiagnostic::UnweightedUpdate).unwrap();
    }
    assert_eq!(
        actual
            .record_batch()
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(0),
        novarocks_functions::approx_percentile_core::encode_state(&expected)
            .expect("percentile codec fixture")
    );
    // The later Final output is a separate owner. Release this already checked
    // Single output before shadowing its binding and testing the final last ref.
    drop(actual);
    let stages = union_program(DataType::Binary, true);
    let chunk = union_chunk(&stages, array);
    let left = run(
        &factory(&stages, P::Partial),
        &[Chunk::new_like(chunk.record_batch().slice(0, 161), &chunk)],
        tracker.clone(),
    );
    let right = run(
        &factory(&stages, P::Partial),
        &[Chunk::new_like(
            chunk.record_batch().slice(161, 160),
            &chunk,
        )],
        tracker.clone(),
    );
    let actual = run(
        &factory(&stages, P::Final),
        &[left.clone(), right.clone()],
        tracker.clone(),
    );
    let mut expected = novarocks_functions::approx_percentile_core::PercentileState::default();
    for partial in [&left, &right] {
        novarocks_functions::approx_percentile_aggregate_core::merge_row(&mut expected,partial.record_batch().column(0),0,
        novarocks_functions::approx_percentile_aggregate_core::ApproxPercentileDiagnostic::UnweightedMerge).unwrap();
    }
    assert_eq!(
        actual
            .record_batch()
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(0),
        novarocks_functions::approx_percentile_core::encode_state(&expected)
            .expect("percentile codec fixture")
    );
    drop(actual);
    drop(left);
    drop(right);
    assert_eq!(tracker.current(), 0);
}
#[test]
fn percentile_union_private_real_factory_empty_encodes_non_null_state() {
    use novarocks_functions::AggregateKernelPhase as P;
    let program = union_program(DataType::Binary, false);
    let tracker = MemTracker::new_root("UnionEmptyFactory");
    let actual = run(&factory(&program, P::Single), &[], tracker.clone());
    let array = actual
        .record_batch()
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert_eq!(array.len(), 1);
    assert!(!array.is_null(0));
    assert_eq!(array.value(0), encoded(0));
    drop(actual);
    assert_eq!(tracker.current(), 0);
}
#[test]
fn percentile_union_private_real_factory_long_shape_is_invariant_and_wire_value_is_data() {
    use novarocks_functions::AggregateKernelPhase as P;
    let ty = DataType::List(Arc::new(arrow::datatypes::Field::new(
        "item",
        DataType::Struct(
            vec![Arc::new(arrow::datatypes::Field::new(
                "original-key".repeat(80),
                DataType::UInt32,
                true,
            ))]
            .into(),
        ),
        true,
    )));
    use novarocks_failure_contract::{FailureClass, FailureProjection, domain};
    let mut invalid_wire = encoded(1);
    invalid_wire.truncate(2);
    for (ty, array, class, reason) in [
        (
            ty.clone(),
            arrow::array::new_null_array(&ty, 1),
            FailureClass::Invariant,
            395,
        ),
        (
            DataType::Binary,
            Arc::new(BinaryArray::from_vec(vec![invalid_wire.as_slice()])) as ArrayRef,
            FailureClass::Data,
            42,
        ),
    ] {
        let program = union_program(ty, false);
        let expected=novarocks_functions::approx_percentile_aggregate_core::merge_row(
        &mut novarocks_functions::approx_percentile_core::PercentileState::default(),&array,0,
        novarocks_functions::approx_percentile_aggregate_core::ApproxPercentileDiagnostic::UnweightedUpdate).unwrap_err();
        let chunk = union_chunk(&program, array);
        let tracker = MemTracker::new_root("UnionLongFactoryData");
        {
            let factory = factory(&program, P::Single);
            let mut operator = factory.create(1, 0);
            operator.set_construction_factory(aggregate_fixture::runtime_construction_factory(
                tracker.clone(),
                factory.error.clone(),
            ));
            operator.set_mem_tracker(tracker.clone());
            operator.prepare().expect("prepare actual aggregate schema");
            let result = crate::exec::pipeline::test_driver::TestDriver::new(
                operator.as_mut(),
                &RuntimeState::default(),
            )
            .input_without_output(chunk)
            .unwrap_err();
            let fact = result.failure_fact();
            assert_eq!(fact, expected.failure_fact());
            assert_eq!(fact.class(), class);
            assert_eq!(fact.reason().domain(), domain::KERNEL.get());
            assert_eq!(fact.reason().code(), reason);
            assert!(fact.resource().is_none());
            assert!(fact.representation().is_none());
            match class {
                FailureClass::Invariant => {
                    assert_eq!(result.detail(), expected.to_string());
                    assert!(result.detail().len() <= 512);
                }
                FailureClass::Data => {
                    assert_eq!(
                        result.to_string(),
                        novarocks_functions::aggregate_format::AggregateFailureStage::Update
                            .message(&expected)
                            .to_string()
                    );
                }
                _ => unreachable!(),
            }
            assert!(
                crate::exec::pipeline::test_driver::TestDriver::new(
                    operator.as_mut(),
                    &RuntimeState::default()
                )
                .advance_to_boundary()
                .unwrap_err()
                .same_occurrence(&result)
            );
        }
        assert_eq!(tracker.current(), 0);
    }
}
