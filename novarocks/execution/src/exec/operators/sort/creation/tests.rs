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

//! A real SystemRelation payload follows the original Sort constructors.
use super::*;
use arrow::{
    array::{Array, BooleanArray, ListArray, StringArray, UInt32Array},
    buffer::{NullBuffer, OffsetBuffer, ScalarBuffer},
    datatypes::{DataType, Field, Schema},
};
use novarocks_physical_plan::{
    ConstantPools, ExprKind, FragmentBuilder, FragmentId, NodeId, NullOrdering, SortDirection,
    SortExpr, SortMode, SystemRelationData, ValueOrigin,
};
use novarocks_type_contract::{FunctionValueType, ValueTypeStorageLifetime};
use std::{error::Error, sync::Arc};

fn cause_chain(error: &(dyn Error + 'static)) -> String {
    let mut lines = vec![error.to_string()];
    let mut next = error.source();
    while let Some(source) = next {
        lines.push(source.to_string());
        next = source.source();
    }
    lines.join("\ncaused by: ")
}

fn list_program(
    sort: bool,
    filter_project: bool,
    separate_batches: bool,
    nullable_row: bool,
    wire_roundtrip: bool,
) -> Arc<novarocks_local_program::LocalProgram> {
    use crate::exec::operators::family_fixture::{compile, package};
    let item = Arc::new(Field::new("item", DataType::Utf8, false));
    let types = [
        FunctionValueType::new(DataType::Utf8, false),
        FunctionValueType::new(DataType::List(item.clone()), true),
        FunctionValueType::new(DataType::Boolean, false),
    ];
    let fields: Arc<[Arc<Field>]> = Arc::from([
        Arc::new(types[0].try_to_field("name").unwrap()),
        Arc::new(
            types[1]
                .try_to_field("upstream_materialized_views")
                .unwrap(),
        ),
        Arc::new(types[2].try_to_field("keep_row").unwrap()),
    ]);
    let lists = ListArray::try_new(
        item,
        OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 0, 0])),
        Arc::new(StringArray::from(Vec::<&str>::new())),
        nullable_row.then(|| NullBuffer::from(vec![true, false])),
    )
    .unwrap();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(fields.to_vec())),
        vec![
            Arc::new(StringArray::from(vec!["state_manual", "state_interval"])),
            Arc::new(lists),
            Arc::new(BooleanArray::from(vec![true, true])),
        ],
    )
    .unwrap();
    let batches = if separate_batches {
        vec![batch.slice(0, 1), batch.slice(1, 1)]
    } else {
        vec![batch]
    };
    let mut builder = FragmentBuilder::new(FragmentId::new(94));
    let source = NodeId::new(0);
    let sort_node = NodeId::new(1);
    let mut values = types
        .iter()
        .cloned()
        .enumerate()
        .map(|(ordinal, ty)| {
            builder
                .add_value(
                    ty,
                    ValueOrigin::NodeOutput {
                        node: source,
                        output_ordinal: ordinal as u32,
                    },
                )
                .unwrap()
        })
        .collect::<Vec<_>>();
    builder
        .add_system_relation(
            source,
            values.clone().into_boxed_slice(),
            fields,
            SystemRelationData::Batches {
                batches: batches.into(),
                storage: ValueTypeStorageLifetime::pure(),
            },
        )
        .unwrap();
    let input = if filter_project {
        let filter = NodeId::new(2);
        let project = NodeId::new(3);
        let predicate = builder
            .add_expression(
                filter,
                FunctionValueType::new(DataType::Boolean, false),
                ExprKind::Value(values[2]),
            )
            .unwrap();
        builder
            .add_filter(filter, source, Box::from([predicate]))
            .unwrap();
        let mut assignments = Vec::new();
        let mut outputs = Vec::new();
        for (source_value, ty) in values.iter().zip(&types).take(2) {
            let expression = builder
                .add_expression(project, ty.clone(), ExprKind::Value(*source_value))
                .unwrap();
            let output = builder
                .add_value(
                    ty.clone(),
                    ValueOrigin::Expr {
                        node: project,
                        expr: expression,
                    },
                )
                .unwrap();
            assignments.push((expression, output));
            outputs.push(output);
        }
        builder
            .add_project(
                project,
                filter,
                assignments.into_boxed_slice(),
                outputs.clone().into_boxed_slice(),
            )
            .unwrap();
        values = outputs;
        project
    } else {
        source
    };
    let root = if sort {
        let key = builder
            .add_expression(
                sort_node,
                FunctionValueType::new(DataType::Utf8, false),
                ExprKind::Value(values[0]),
            )
            .unwrap();
        builder
            .add_sort(
                sort_node,
                input,
                Box::from([SortExpr {
                    expr: key,
                    direction: SortDirection::Ascending,
                    null_ordering: NullOrdering::Last,
                }]),
                SortMode::Global,
            )
            .unwrap();
        sort_node
    } else {
        input
    };
    let package = package(builder, root, ConstantPools::empty(), 4);
    let package = if wire_roundtrip {
        use crate::exec::operators::family_fixture::FixtureControl;
        use novarocks_plan_codec::physical_package_v2::{
            PackageDecodeLimits, PackageEncodeLimits, decode_fragment_package,
            encode_fragment_package,
        };
        use novarocks_plan_codec::resource_preflight_v2::FragmentDecodeResourceModel;
        use prost::Message;
        let wire = encode_fragment_package(&package, &PackageEncodeLimits::FROZEN, &FixtureControl)
            .unwrap();
        Arc::new(
            decode_fragment_package(
                &wire.encode_to_vec(),
                &FragmentDecodeResourceModel::try_new(&FixtureControl).unwrap(),
                &PackageDecodeLimits::FROZEN,
                &FixtureControl,
            )
            .unwrap(),
        )
    } else {
        package
    };
    compile(package, 4)
}

#[test]
fn system_relation_list_payload_keeps_its_exact_field_through_sort_constructors() {
    use crate::exec::operators::{aggregate_fixture, family_fixture};
    use crate::runtime::{mem_tracker::MemTracker, runtime_state::RuntimeErrorState};
    for separate_batches in [false, true] {
        for nullable_row in [false, true] {
            let program = list_program(false, false, separate_batches, nullable_row, false);
            let chunks = family_fixture::run(&program);
            let output = chunks[0].chunk_schema_ref();
            let factory = aggregate_fixture::runtime_construction_factory(
                MemTracker::new_root("system relation original sort constructors"),
                Arc::new(RuntimeErrorState::default()),
            );
            let creation = SortCreation::new(factory.as_ref(), &output);
            let owner = creation.begin(&chunks[0]).unwrap();
            let batch = creation
                .concat(&chunks, owner.as_ref())
                .unwrap_or_else(|error| panic!("original concat: {}", cause_chain(&error)));
            let selected = creation
                .take_batch(
                    &batch,
                    &UInt32Array::from(vec![1_u32, 0]),
                    &chunks[0],
                    owner.as_ref(),
                )
                .unwrap_or_else(|error| panic!("original take: {}", cause_chain(&error)));
            let actual = creation
                .publish(selected, &chunks[0], owner.as_ref())
                .unwrap_or_else(|error| panic!("original publish: {}", cause_chain(&error)));
            assert_eq!(
                actual.record_batch().column(1).data_type(),
                output.slots()[1].data_type()
            );
            let names = actual
                .record_batch()
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(
                names.iter().collect::<Vec<_>>(),
                [Some("state_interval"), Some("state_manual")]
            );
            let arrays = actual
                .record_batch()
                .column(1)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            assert_eq!(arrays.null_count(), usize::from(nullable_row));
            assert_eq!(arrays.values().null_count(), 0);
        }
    }
}

#[test]
fn system_relation_list_payload_runs_through_compiled_name_sort() {
    use crate::exec::operators::family_fixture;
    for separate_batches in [false, true] {
        for nullable_row in [false, true] {
            let program = list_program(true, false, separate_batches, nullable_row, false);
            let chunks = family_fixture::run(&program);
            let names = chunks
                .iter()
                .flat_map(|chunk| {
                    chunk
                        .record_batch()
                        .column(0)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .iter()
                })
                .collect::<Vec<_>>();
            assert_eq!(names, [Some("state_interval"), Some("state_manual")]);
        }
    }
}

#[test]
fn system_relation_list_payload_runs_through_filter_project_and_name_sort() {
    use crate::exec::operators::family_fixture;
    for wire_roundtrip in [false, true] {
        for separate_batches in [false, true] {
            for nullable_row in [false, true] {
                let program =
                    list_program(true, true, separate_batches, nullable_row, wire_roundtrip);
                let chunks = family_fixture::run(&program);
                let names = chunks
                    .iter()
                    .flat_map(|chunk| {
                        chunk
                            .record_batch()
                            .column(0)
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap()
                            .iter()
                    })
                    .collect::<Vec<_>>();
                assert_eq!(names, [Some("state_interval"), Some("state_manual")]);
                for chunk in chunks {
                    assert_eq!(
                        chunk.record_batch().column(1).data_type(),
                        &DataType::List(Arc::new(Field::new("item", DataType::Utf8, false)))
                    );
                }
            }
        }
    }
}

#[test]
fn system_relation_list_project_representation_keeps_declared_binding_through_sort() {
    use crate::exec::operators::project_processor::{
        ProjectEncodingSource, materialize_project_output_owned, project_reserve,
    };
    use crate::exec::operators::{aggregate_fixture, family_fixture};
    use crate::runtime::exchange::online_unit;
    use crate::runtime::{mem_tracker::MemTracker, runtime_state::RuntimeErrorState};

    let source = family_fixture::run(&list_program(false, true, false, false, true)).remove(0);
    let declared = source.chunk_schema_ref();
    let factory = aggregate_fixture::runtime_construction_factory(
        MemTracker::new_root("exchanged system relation sort"),
        Arc::new(RuntimeErrorState::default()),
    );
    let recipe = online_unit::prepare_recipe(&declared, &[], factory.as_ref()).unwrap();
    let bytes = online_unit::encode(
        &recipe,
        &source,
        factory.as_ref(),
        &family_fixture::FixtureControl,
    )
    .unwrap();
    let input = online_unit::decode(&recipe, &bytes, &declared, factory.as_ref())
        .unwrap()
        .remove(0);
    assert_eq!(
        input.chunk_schema().slots()[1].typed_binding(),
        declared.slots()[1].typed_binding()
    );
    assert_eq!(
        input.record_batch().column(1).data_type(),
        &DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
    );
    assert_eq!(
        declared.slots()[1].data_type(),
        &DataType::List(Arc::new(Field::new("item", DataType::Utf8, false)))
    );
    let owner = factory.begin_typed_construction(&declared).unwrap();
    let storage = input
        .source_storage_lifetime_observed(owner.storage_lifetime(), |layout| owner.request(layout))
        .unwrap();
    let mut columns = project_reserve(input.columns().len(), owner.as_ref()).unwrap();
    columns.extend(input.columns().iter().cloned());
    let projected = materialize_project_output_owned(
        columns,
        input.len(),
        declared.clone(),
        |ordinal| Ok(input.chunk_schema().slots()[ordinal].typed_binding()),
        |ordinal| {
            Ok(ProjectEncodingSource::OriginalField(
                input.chunk_schema().slots()[ordinal].field(),
            ))
        },
        storage,
        owner.as_ref(),
    )
    .unwrap();
    // The actual Exchange author permits nullable carrier children while the
    // complete declaration still forbids NULL values. Project preserves that
    // original representation and the same signed binding independently.
    assert_eq!(
        projected.chunk_schema().slots()[1].typed_binding(),
        declared.slots()[1].typed_binding()
    );
    assert_ne!(
        projected.record_batch().column(1).data_type(),
        declared.slots()[1].data_type()
    );
    let creation = SortCreation::new(factory.as_ref(), &declared);
    let narrow = creation.slice(&source, 0, 1).unwrap();
    let wide = creation.slice(&projected, 1, 1).unwrap();
    let scenarios = [
        vec![projected.clone()],
        vec![narrow.clone(), wide.clone()],
        vec![wide, narrow],
    ];
    for (ordinal, chunks) in scenarios.into_iter().enumerate() {
        let owner = creation.begin(&chunks[0]).unwrap();
        let batch = creation
            .concat(&chunks, owner.as_ref())
            .unwrap_or_else(|error| panic!("projected concat: {}", cause_chain(&error)));
        let indices = UInt32Array::from(if ordinal == 2 {
            vec![0_u32, 1]
        } else {
            vec![1_u32, 0]
        });
        let selected = creation
            .take_batch(&batch, &indices, &chunks[0], owner.as_ref())
            .unwrap_or_else(|error| panic!("projected take: {}", cause_chain(&error)));
        let actual = creation
            .publish(selected, &chunks[0], owner.as_ref())
            .unwrap_or_else(|error| panic!("projected publish: {}", cause_chain(&error)));
        assert_sorted_list_output(std::slice::from_ref(&actual), &declared);
        // Exercise the production final author, not just the constructor probe.
        let actual = run_name_sort(&chunks, factory.clone());
        assert_sorted_list_output(&actual, &declared);
    }
}

fn assert_sorted_list_output(chunks: &[Chunk], declared: &crate::exec::chunk::ChunkSchemaRef) {
    let mut names = Vec::new();
    for actual in chunks {
        assert_eq!(
            actual.chunk_schema().slots()[1].typed_binding(),
            declared.slots()[1].typed_binding()
        );
        assert_eq!(
            actual.record_batch().column(1).data_type(),
            &DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
        );
        names.extend(
            actual
                .record_batch()
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter(),
        );
    }
    assert_eq!(names, [Some("state_interval"), Some("state_manual")]);
}

fn run_name_sort(chunks: &[Chunk], construction: Arc<dyn InputConstructionFactory>) -> Vec<Chunk> {
    use crate::exec::operators::compiled_sort::CompiledSortProcessorFactory;
    use crate::exec::pipeline::{operator_factory::OperatorFactory, test_driver::TestDriver};
    use crate::runtime::runtime_state::RuntimeState;
    use novarocks_local_program::{ProgramNodeId, ProgramNodeKind};
    let program = list_program(true, true, false, false, true);
    let sort = program
        .graph()
        .nodes()
        .iter()
        .position(|node| matches!(node.kind(), ProgramNodeKind::Sort { .. }))
        .unwrap();
    let tracker = crate::runtime::mem_tracker::MemTracker::new_root("actual exchanged list sort");
    let state = RuntimeState::for_local_execution(
        None,
        None,
        None,
        None,
        None,
        Some(tracker.clone()),
        Some(crate::runtime::execution_runtime::test_execution_runtime()),
    );
    let factory = CompiledSortProcessorFactory::try_new(
        program,
        ProgramNodeId::new(sort),
        state.error_state(),
    )
    .unwrap();
    let mut operator = factory.create(1, 0);
    operator.set_mem_tracker(tracker);
    operator.set_construction_factory(construction);
    operator.prepare().unwrap();
    operator.bind_runtime_state(&state).unwrap();
    let mut driver = TestDriver::new(operator.as_mut(), &state);
    for chunk in chunks {
        driver.input_without_output(chunk.clone()).unwrap();
    }
    driver.signal_eos().unwrap();
    let mut output = Vec::new();
    while let Some(chunk) = driver
        .advance_to_boundary()
        .unwrap_or_else(|error| panic!("actual name sort: {}", cause_chain(&error)))
        .output
    {
        output.push(chunk);
    }
    output
}

#[test]
fn sort_carrier_merge_preserves_original_creation_and_cancel_failures() {
    use novarocks_functions::array_projection::{
        ArrayNormalizationStep, merge_execution_carriers_observed,
    };
    let narrow = DataType::List(Arc::new(Field::new("item", DataType::Utf8, false)));
    let wide = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
    for original in [
        ConstructionFailure::MissingOwner,
        ConstructionFailure::Control(novarocks_type_contract::CompileControlError::Cancelled),
    ] {
        let mut rejected = false;
        let error = merge_execution_carriers_observed::<ConstructionFailure, _>(
            &narrow,
            &wide,
            &mut |step| {
                assert!(!rejected, "no constructor runs after the original refusal");
                if matches!(step, ArrayNormalizationStep::RebuildField { .. }) {
                    rejected = true;
                    return Err(original.clone());
                }
                Ok(())
            },
        )
        .unwrap_err();
        assert!(
            rejected,
            "the field creator must be consulted before allocation"
        );
        assert_eq!(error, original);
    }
    let bad = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    let error =
        merge_execution_carriers_observed::<ConstructionFailure, _>(&narrow, &bad, &mut |_| Ok(()))
            .unwrap_err();
    let mut source: &dyn Error = &error;
    loop {
        if let Some(original) =
            source.downcast_ref::<novarocks_functions::array_projection::ArrayProjectionFailure>()
        {
            assert!(matches!(
                original,
                novarocks_functions::array_projection::ArrayProjectionFailure::Contract(
                    novarocks_functions::array_projection::ArrayProjectionViolation::ScalarChanged
                )
            ));
            break;
        }
        source = source.source().expect("the original carrier refusal");
    }
}

#[test]
fn sort_preserves_masked_child_nulls_and_rejects_live_required_nulls() {
    use crate::exec::operators::{aggregate_fixture, family_fixture};
    use crate::runtime::exchange::online_unit;
    use crate::runtime::{mem_tracker::MemTracker, runtime_state::RuntimeErrorState};
    use novarocks_failure_contract::FailureProjection;
    use novarocks_value_arrays::{ValueError, ValueViolation};
    let source = family_fixture::run(&list_program(false, true, false, false, true)).remove(0);
    let declared = source.chunk_schema_ref();
    let factory = aggregate_fixture::runtime_construction_factory(
        MemTracker::new_root("sort hidden required child nulls"),
        Arc::new(RuntimeErrorState::default()),
    );
    let recipe = online_unit::prepare_recipe(&declared, &[], factory.as_ref()).unwrap();
    let bytes = online_unit::encode(
        &recipe,
        &source,
        factory.as_ref(),
        &family_fixture::FixtureControl,
    )
    .unwrap();
    let exchanged = online_unit::decode(&recipe, &bytes, &declared, factory.as_ref())
        .unwrap()
        .remove(0);
    for masked in [true, false] {
        // Literal fixture data uses the actual permissive Exchange carrier.
        // Logical child requiredness still comes from the unchanged binding.
        let array = ListArray::try_new(
            Arc::new(Field::new("item", DataType::Utf8, true)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 0, 1])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            masked.then(|| NullBuffer::from(vec![true, false])),
        )
        .unwrap();
        let owner = factory.begin_typed_construction(&declared).unwrap();
        owner
            .observe(
                0,
                &[],
                crate::exec::chunk::ArrayNormalizationStep::CreateChunkColumns { channels: 2 },
            )
            .unwrap();
        let input = Chunk::try_new_typed_with_columns_owned_observed::<ConstructionFailure, _>(
            exchanged.chunk_schema_ref(),
            vec![exchanged.columns()[0].clone(), Arc::new(array)],
            2,
            owner.storage_lifetime(),
            |step| owner.observe(0, &[], step),
        );
        if masked {
            let input = input.expect("the parent NULL masks the required child NULL");
            let creation = SortCreation::new(factory.as_ref(), &declared);
            let batch = creation
                .concat(std::slice::from_ref(&input), owner.as_ref())
                .unwrap();
            let actual = creation.publish(batch, &input, owner.as_ref()).unwrap();
            assert_eq!(
                actual.chunk_schema().slots()[1].typed_binding(),
                declared.slots()[1].typed_binding()
            );
            assert_eq!(
                actual.record_batch().column(1).data_type(),
                exchanged.record_batch().column(1).data_type()
            );
            assert_eq!(actual.record_batch().column(1).null_count(), 1);
            let sorted = run_name_sort(std::slice::from_ref(&input), factory.clone());
            assert_sorted_list_output(&sorted, &declared);
            assert_eq!(
                sorted
                    .iter()
                    .map(|chunk| chunk.columns()[1].null_count())
                    .sum::<usize>(),
                1
            );
        } else {
            let error = input.expect_err("a live required child NULL remains forbidden");
            assert_eq!(
                error.failure_fact(),
                ValueError::Invariant(ValueViolation::RequiredNull).failure_fact()
            );
            let mut source: &dyn Error = &error;
            loop {
                if let Some(original) = source.downcast_ref::<ValueError>() {
                    assert!(matches!(
                        original,
                        ValueError::Invariant(ValueViolation::RequiredNull)
                    ));
                    break;
                }
                source = source
                    .source()
                    .expect("the original required child failure");
            }
        }
    }
}

#[test]
#[allow(
    deprecated,
    reason = "The fixture preserves actual Arrow IPC dictionary descriptors."
)]
fn sort_carrier_merge_keeps_map_union_and_dictionary_parameters() {
    use arrow::datatypes::{UnionFields, UnionMode};
    use novarocks_functions::array_projection::merge_execution_carriers_observed;
    let merge = |a: &DataType, b: &DataType| {
        merge_execution_carriers_observed::<ConstructionFailure, _>(a, b, &mut |_| Ok(()))
            .map(|value| value.into_owned())
    };
    let list = |nullable| DataType::List(Arc::new(Field::new("item", DataType::Utf8, nullable)));
    let map = |key_nullable, value_nullable| {
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Arc::new(Field::new("key", DataType::Utf8, key_nullable)),
                        Arc::new(Field::new("value", list(value_nullable), value_nullable)),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        )
    };
    assert_eq!(
        merge(&map(false, false), &map(false, true)).unwrap(),
        map(false, true)
    );
    assert!(merge(&map(false, false), &map(true, true)).is_err());
    let union = |id, mode, nullable| {
        DataType::Union(
            UnionFields::try_new(
                [1_i8, id],
                [
                    Arc::new(Field::new("s", DataType::Utf8, nullable)),
                    Arc::new(Field::new("l", list(nullable), nullable)),
                ],
            )
            .unwrap(),
            mode,
        )
    };
    assert_eq!(
        merge(
            &union(3, UnionMode::Dense, false),
            &union(3, UnionMode::Dense, true)
        )
        .unwrap(),
        union(3, UnionMode::Dense, true)
    );
    assert!(
        merge(
            &union(3, UnionMode::Dense, false),
            &union(4, UnionMode::Dense, true)
        )
        .is_err()
    );
    assert!(
        merge(
            &union(3, UnionMode::Dense, false),
            &union(3, UnionMode::Sparse, true)
        )
        .is_err()
    );
    let dictionary = |key, nullable| {
        DataType::Dictionary(
            Box::new(key),
            Box::new(DataType::Struct(
                vec![Arc::new(Field::new("v", DataType::Utf8, nullable))].into(),
            )),
        )
    };
    assert_eq!(
        merge(
            &dictionary(DataType::Int16, false),
            &dictionary(DataType::Int16, true)
        )
        .unwrap(),
        dictionary(DataType::Int16, true)
    );
    assert!(
        merge(
            &dictionary(DataType::Int16, false),
            &dictionary(DataType::Int32, true)
        )
        .is_err()
    );
    let occurrence = |id, nullable| {
        DataType::Struct(
            vec![Arc::new(Field::new_dict(
                "v",
                DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8)),
                nullable,
                id,
                false,
            ))]
            .into(),
        )
    };
    assert_eq!(
        merge(&occurrence(7, false), &occurrence(7, true)).unwrap(),
        occurrence(7, true)
    );
    assert!(merge(&occurrence(7, false), &occurrence(8, true)).is_err());
}
