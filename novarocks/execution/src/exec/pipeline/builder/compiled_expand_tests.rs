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

//! Compiled row-expanding families: Repeat (grouping sets), Unpivot and
//! ChangeEventExpand, each compiled by local-compiler and run through the
//! compiled pipeline. Every oracle is computed from the literal rows.

use std::sync::Arc;

use arrow::array::{Array, Int8Array, Int32Array, Int64Array, ListArray, MapArray, StringArray};
use arrow::datatypes::{DataType, Field};
use novarocks_connector_contract::ConnectorRowMutationEffect;
use novarocks_functions::ConstantPool;
use novarocks_local_program::{LocalProgram, ProgramNodeId, ProgramNodeKind};
use novarocks_physical_plan::{
    ChangeEventSpec, ConstantPoolId, ConstantPools, ConstantReference, ExprKind, FragmentBuilder,
    FragmentId, GroupingOutput, LiteralValue, NodeId, NodeKind, UnpivotConstant, UnpivotSpec,
    UnpivotValueMapping, ValueId, ValueOrigin,
};
use novarocks_type_contract::{CompilePhase, FunctionValueType};

use super::family_fixture::{
    FixtureControl, boolean, cell, compile, constant_policy, int64, int64_rows, package, run,
    values,
};
use crate::exec::chunk::Chunk;
use crate::exec::operators::compiled_change_events::CompiledChangeEventProcessorFactory;
use crate::exec::operators::compiled_repeat::CompiledRepeatProcessorFactory;
use crate::exec::operators::compiled_unpivot::CompiledUnpivotProcessorFactory;
use crate::runtime::runtime_state::RuntimeErrorState;

fn node_output(
    builder: &mut FragmentBuilder,
    node: NodeId,
    ordinal: u32,
    ty: FunctionValueType,
) -> ValueId {
    builder
        .add_value(
            ty,
            ValueOrigin::NodeOutput {
                node,
                output_ordinal: ordinal,
            },
        )
        .unwrap()
}

// ---------------------------------------------------------------- Repeat

const REPEAT_ROWS: [(i64, i64, i64); 2] = [(1, 10, 100), (2, 20, 200)];

/// `Values(k1, k2, v) -> Repeat(ROLLUP(k1, k2)) -> Result` with outputs
/// `(k1', k2', v, GROUPING_ID(k1, k2), GROUPING(k2))`.
fn repeat_program() -> Arc<LocalProgram> {
    let mut builder = FragmentBuilder::new(FragmentId::new(61));
    let source = NodeId::new(0);
    let repeat = NodeId::new(1);
    let rows = REPEAT_ROWS
        .iter()
        .map(|(k1, k2, v)| vec![cell(Some(*k1)), cell(Some(*k2)), cell(Some(*v))])
        .collect::<Vec<_>>();
    let columns = values(
        &mut builder,
        source,
        &[int64(false), int64(false), int64(false)],
        &rows,
    );
    let (k1, k2, v) = (columns[0], columns[1], columns[2]);
    let k1_null = builder
        .add_value(
            int64(true),
            ValueOrigin::NullExtended {
                node: repeat,
                of: k1,
            },
        )
        .unwrap();
    let k2_null = builder
        .add_value(
            int64(true),
            ValueOrigin::NullExtended {
                node: repeat,
                of: k2,
            },
        )
        .unwrap();
    let grouping_id = node_output(&mut builder, repeat, 3, int64(false));
    let grouping_k2 = node_output(&mut builder, repeat, 4, int64(false));
    builder
        .add_repeat(
            repeat,
            source,
            Box::from([k1, k2]),
            Box::from([
                Box::from([k1, k2]),
                Box::from([k1]),
                Box::<[ValueId]>::default(),
            ]),
            Box::from([(k1, k1_null), (k2, k2_null)]),
            Box::from([
                GroupingOutput {
                    output: grouping_id,
                    arguments: Box::from([k1, k2]),
                },
                GroupingOutput {
                    output: grouping_k2,
                    arguments: Box::from([k2]),
                },
            ]),
            Box::from([k1_null, k2_null, v, grouping_id, grouping_k2]),
        )
        .unwrap();
    compile(package(builder, repeat, ConstantPools::empty(), 1), 1)
}

/// Each row once per grouping set; an omitted key is NULL and GROUPING bits
/// are 1 for omitted arguments, the last argument in the low bit.
fn repeat_oracle() -> Vec<Vec<Option<i64>>> {
    let sets: [&[usize]; 3] = [&[0, 1], &[0], &[]];
    let mut rows = Vec::new();
    for set in sets {
        let omitted = |key: usize| i64::from(!set.contains(&key));
        for (k1, k2, v) in REPEAT_ROWS {
            rows.push(vec![
                set.contains(&0).then_some(k1),
                set.contains(&1).then_some(k2),
                Some(v),
                Some((omitted(0) << 1) | omitted(1)),
                Some(omitted(1)),
            ]);
        }
    }
    rows
}

#[test]
fn compiled_repeat_expands_each_row_per_grouping_set_with_exact_grouping_values() {
    let program = repeat_program();
    let ProgramNodeKind::Repeat {
        repeat_times,
        grouping_list,
        ..
    } = program.graph().nodes()[1].kind()
    else {
        panic!("the compiler emits a local Repeat");
    };
    assert_eq!(*repeat_times, 3);
    assert_eq!(grouping_list, &vec![vec![0, 1, 3], vec![0, 1, 1]]);
    let chunks = run(&program);
    // The output is exactly the frozen layout: widened keys, Int64 groupings.
    let layout = program.graph().nodes()[1].output_layout();
    for chunk in &chunks {
        assert_eq!(chunk.batch.schema(), layout.schema().clone());
    }
    let mut rows = int64_rows(&chunks);
    let mut expected = repeat_oracle();
    rows.sort();
    expected.sort();
    assert_eq!(rows, expected);
}

#[test]
fn compiled_repeat_factory_refuses_a_node_that_is_not_a_repeat() {
    let program = repeat_program();
    let error = match CompiledRepeatProcessorFactory::try_new(&program, ProgramNodeId::new(0)) {
        Ok(_) => panic!("a Values node is not a Repeat"),
        Err(error) => error,
    };
    assert!(error.contains("is not a Repeat"), "{error}");
}

#[test]
fn compiled_repeat_retains_original_column_metadata_through_the_last_set() {
    use crate::exec::pipeline::operator_factory::OperatorFactory;
    use crate::runtime::mem_tracker::MemTracker;
    use crate::runtime::runtime_state::RuntimeState;
    for drain in [false, true] {
        let program = repeat_program();
        let tracker = MemTracker::new_root("repeat-input-column-owner");
        let chunk = super::family_fixture::paid_input(&program, &tracker);
        let charged = tracker.current();
        let factory =
            CompiledRepeatProcessorFactory::try_new(&program, ProgramNodeId::new(1)).unwrap();
        let mut operator = factory.create(1, 0);
        let state = RuntimeState::default();
        let processor = operator.as_processor_mut().unwrap();
        processor.push_chunk(&state, chunk).unwrap();
        assert_eq!(tracker.current(), charged);
        if drain {
            let mut output = Vec::new();
            for ordinal in 0..3 {
                output.push(processor.pull_chunk(&state).unwrap().unwrap());
                if ordinal < 2 {
                    assert_eq!(tracker.current(), charged);
                }
            }
            assert_eq!(tracker.current(), 0, "last set released the actual input");
            let mut actual = int64_rows(&output);
            let mut expected = repeat_oracle();
            actual.sort();
            expected.sort();
            assert_eq!(actual, expected);
        }
        drop(operator);
        assert_eq!(tracker.current(), 0);
    }
}

// ---------------------------------------------------------------- Unpivot

const UNPIVOT_ROWS: [(i64, Option<i64>, i64); 3] =
    [(1, Some(10), 11), (2, None, 21), (3, Some(30), 31)];

fn list_type() -> DataType {
    DataType::List(Arc::new(Field::new("item", DataType::Int32, false)))
}

fn map_type() -> DataType {
    DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Arc::new(Field::new("key", DataType::Utf8, false)),
                    Arc::new(Field::new("value", DataType::Utf8, false)),
                ]
                .into(),
            ),
            false,
        )),
        false,
    )
}

/// Rows `[[0], [7, 8], [9]]` of a checked non-null Int32 list pool.
fn list_pool() -> ConstantPool {
    let DataType::List(item) = list_type() else {
        unreachable!()
    };
    let raw = ListArray::from_iter_primitive::<arrow::datatypes::Int32Type, _, _>([
        Some(vec![Some(0)]),
        Some(vec![Some(7), Some(8)]),
        Some(vec![Some(9)]),
    ]);
    let array =
        ListArray::try_new(item, raw.offsets().clone(), raw.values().clone(), None).unwrap();
    let ty = FunctionValueType::new(list_type(), false);
    ConstantPool::try_new(
        Arc::new(ty.try_to_field("codes-pool").unwrap()),
        ty,
        array.to_data(),
        constant_policy(),
        CompilePhase::Validate,
        &FixtureControl,
    )
    .unwrap()
}

/// Rows `[{}, {a: x}, {b: y, c: z}]` of a checked non-null Utf8 map pool.
fn map_pool() -> ConstantPool {
    let ty = FunctionValueType::new(map_type(), false);
    let DataType::Map(entries, _) = &ty.data_type else {
        unreachable!()
    };
    let DataType::Struct(fields) = entries.data_type() else {
        unreachable!()
    };
    let children = arrow::array::StructArray::try_new(
        fields.clone(),
        vec![
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
            Arc::new(StringArray::from(vec!["x", "y", "z"])),
        ],
        None,
    )
    .unwrap();
    let array = MapArray::try_new(
        entries.clone(),
        arrow::buffer::OffsetBuffer::from_lengths([0, 1, 2]),
        children,
        None,
        false,
    )
    .unwrap();
    ConstantPool::try_new(
        Arc::new(ty.try_to_field("attrs-pool").unwrap()),
        ty,
        array.to_data(),
        constant_policy(),
        CompilePhase::Validate,
        &FixtureControl,
    )
    .unwrap()
}

/// `Values(id, v1, v2) -> Unpivot -> Result` with outputs
/// `(id, value, label, codes, attrs)` and a two-row output budget.
fn unpivot_program() -> Arc<LocalProgram> {
    let mut builder = FragmentBuilder::new(FragmentId::new(62));
    let source = NodeId::new(0);
    let unpivot = NodeId::new(1);
    let rows = UNPIVOT_ROWS
        .iter()
        .map(|(id, v1, v2)| vec![cell(Some(*id)), cell(*v1), cell(Some(*v2))])
        .collect::<Vec<_>>();
    let columns = values(
        &mut builder,
        source,
        &[int64(false), int64(true), int64(false)],
        &rows,
    );
    let utf8 = FunctionValueType::new(DataType::Utf8, false);
    let labels = ["first", "second"].map(|label| {
        builder
            .add_expression(
                unpivot,
                utf8.clone(),
                ExprKind::Literal(LiteralValue::Utf8(label.into())),
            )
            .unwrap()
    });
    let outputs = [
        int64(false),
        int64(true),
        utf8,
        FunctionValueType::new(list_type(), false),
        FunctionValueType::new(map_type(), false),
    ]
    .into_iter()
    .enumerate()
    .map(|(ordinal, ty)| node_output(&mut builder, unpivot, ordinal as u32, ty))
    .collect::<Vec<_>>();
    let (lists, maps) = (ConstantPoolId::new(1), ConstantPoolId::new(2));
    let reference = |pool, ordinal| ConstantReference { pool, ordinal };
    let spec = UnpivotSpec {
        passthrough: Box::from([(columns[0], outputs[0])]),
        value_output: outputs[1],
        literal_outputs: Box::from([outputs[2], outputs[3], outputs[4]]),
        mappings: Box::from([
            UnpivotValueMapping {
                input: columns[1],
                constants: Box::from([
                    UnpivotConstant::Scalar(labels[0]),
                    UnpivotConstant::Int32List(reference(lists, 1)),
                    UnpivotConstant::Utf8Map(reference(maps, 1)),
                ]),
            },
            UnpivotValueMapping {
                input: columns[2],
                constants: Box::from([
                    UnpivotConstant::Scalar(labels[1]),
                    UnpivotConstant::Int32List(reference(lists, 2)),
                    UnpivotConstant::Utf8Map(reference(maps, 2)),
                ]),
            },
        ]),
        max_output_rows: 2,
        max_output_bytes: 1 << 20,
    };
    let passthrough = [(columns[0], outputs[0])].into_iter().collect();
    builder
        .add_row_rewriting(
            unpivot,
            source,
            Some(&passthrough),
            outputs.into_boxed_slice(),
            NodeKind::Unpivot { spec },
        )
        .unwrap();
    let mut constants = ConstantPools::empty();
    constants.insert(lists, list_pool()).unwrap();
    constants.insert(maps, map_pool()).unwrap();
    compile(package(builder, unpivot, constants, 1), 1)
}

type UnpivotRow = (i64, Option<i64>, String, Vec<i32>, Vec<(String, String)>);

fn unpivot_rows(chunks: &[Chunk]) -> Vec<UnpivotRow> {
    let mut rows = Vec::new();
    for chunk in chunks {
        let column = |ordinal: usize| chunk.batch.column(ordinal).clone();
        let ids = column(0);
        let ids = ids.as_any().downcast_ref::<Int64Array>().unwrap();
        let values = column(1);
        let values = values.as_any().downcast_ref::<Int64Array>().unwrap();
        let labels = column(2);
        let labels = labels.as_any().downcast_ref::<StringArray>().unwrap();
        let codes = column(3);
        let codes = codes.as_any().downcast_ref::<ListArray>().unwrap();
        let attrs = column(4);
        let attrs = attrs.as_any().downcast_ref::<MapArray>().unwrap();
        for row in 0..chunk.len() {
            let list = codes.value(row);
            let list = list.as_any().downcast_ref::<Int32Array>().unwrap();
            let entries = attrs.value(row);
            let keys = entries
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let items = entries
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            rows.push((
                ids.value(row),
                (!values.is_null(row)).then(|| values.value(row)),
                labels.value(row).to_string(),
                list.values().to_vec(),
                (0..keys.len())
                    .map(|entry| {
                        (
                            keys.value(entry).to_string(),
                            items.value(entry).to_string(),
                        )
                    })
                    .collect(),
            ));
        }
    }
    rows
}

#[test]
fn compiled_unpivot_expands_every_mapping_within_the_frozen_row_budget() {
    let program = unpivot_program();
    let ProgramNodeKind::Unpivot {
        value_mappings,
        max_output_rows,
        ..
    } = program.graph().nodes()[1].kind()
    else {
        panic!("the compiler emits a local Unpivot");
    };
    assert_eq!((value_mappings.len(), *max_output_rows), (2, 2));
    let chunks = run(&program);
    assert!(chunks.iter().all(|chunk| chunk.len() <= 2), "row budget");
    let layout = program.graph().nodes()[1].output_layout();
    for chunk in &chunks {
        assert_eq!(chunk.batch.schema(), layout.schema().clone());
    }
    let mut rows = unpivot_rows(&chunks);
    let pair = |key: &str, value: &str| (key.to_string(), value.to_string());
    let mut expected = Vec::new();
    for (id, v1, v2) in UNPIVOT_ROWS {
        expected.push((
            id,
            v1,
            "first".to_string(),
            vec![7, 8],
            vec![pair("a", "x")],
        ));
        expected.push((
            id,
            Some(v2),
            "second".to_string(),
            vec![9],
            vec![pair("b", "y"), pair("c", "z")],
        ));
    }
    rows.sort();
    expected.sort();
    assert_eq!(rows, expected);
}

#[test]
fn compiled_unpivot_factory_refuses_a_node_that_is_not_an_unpivot() {
    let program = unpivot_program();
    let error = match CompiledUnpivotProcessorFactory::try_new(
        Arc::clone(&program),
        ProgramNodeId::new(0),
        Arc::new(RuntimeErrorState::default()),
    ) {
        Ok(_) => panic!("a Values node is not an Unpivot"),
        Err(error) => error,
    };
    assert!(error.contains("is not an Unpivot"), "{error}");
}

#[test]
fn compiled_unpivot_retains_original_column_metadata_through_actual_input_exit() {
    use crate::exec::pipeline::operator_factory::OperatorFactory;
    use crate::runtime::mem_tracker::MemTracker;
    use crate::runtime::runtime_state::RuntimeState;
    for drain in [false, true] {
        let program = unpivot_program();
        let tracker = MemTracker::new_root("unpivot-input-column-owner");
        let chunk = super::family_fixture::paid_input(&program, &tracker);
        let charged = tracker.current();
        let factory = CompiledUnpivotProcessorFactory::try_new(
            Arc::clone(&program),
            ProgramNodeId::new(1),
            Arc::new(RuntimeErrorState::default()),
        )
        .unwrap();
        let mut operator = factory.create(1, 0);
        let state = RuntimeState::default();
        let processor = operator.as_processor_mut().unwrap();
        processor.push_chunk(&state, chunk).unwrap();
        assert_eq!(tracker.current(), charged);
        if drain {
            let mut output = Vec::new();
            while processor.has_output() {
                output.push(processor.pull_chunk(&state).unwrap().unwrap());
            }
            assert_eq!(tracker.current(), 0);
            assert_eq!(
                output.iter().map(Chunk::len).sum::<usize>(),
                UNPIVOT_ROWS.len() * 2,
                "every literal input row has both frozen mappings"
            );
        }
        drop(operator);
        assert_eq!(tracker.current(), 0);
    }
}

// ------------------------------------------------------ ChangeEventExpand

const CHANGE_ROWS: [(i64, Option<i64>, Option<bool>); 4] = [
    (1, Some(10), Some(true)),
    (2, None, Some(false)),
    (3, Some(30), None),
    (4, Some(40), Some(true)),
];

/// `Values(k, v, flag) -> ChangeEventExpand -> Result` with outputs
/// `(k', v', effect)`: a Delete event for rows whose `flag` is TRUE that
/// assigns only `k'`, then an unconditional Insert event assigning both.
fn change_program() -> Arc<LocalProgram> {
    let mut builder = FragmentBuilder::new(FragmentId::new(63));
    let source = NodeId::new(0);
    let expand = NodeId::new(1);
    let rows = CHANGE_ROWS
        .iter()
        .map(|(k, v, flag)| {
            vec![
                cell(Some(*k)),
                cell(*v),
                flag.map_or(LiteralValue::Null, LiteralValue::Boolean),
            ]
        })
        .collect::<Vec<_>>();
    let columns = values(
        &mut builder,
        source,
        &[int64(false), int64(true), boolean(true)],
        &rows,
    );
    let flag = builder
        .add_expression(expand, boolean(true), ExprKind::Value(columns[2]))
        .unwrap();
    let delete_k = builder
        .add_expression(expand, int64(false), ExprKind::Value(columns[0]))
        .unwrap();
    let insert_k = builder
        .add_expression(expand, int64(false), ExprKind::Value(columns[0]))
        .unwrap();
    let insert_v = builder
        .add_expression(expand, int64(true), ExprKind::Value(columns[1]))
        .unwrap();
    let k_out = node_output(&mut builder, expand, 0, int64(true));
    let v_out = node_output(&mut builder, expand, 1, int64(true));
    let effect = node_output(
        &mut builder,
        expand,
        2,
        FunctionValueType::new(DataType::Int8, false),
    );
    builder
        .add_row_rewriting(
            expand,
            source,
            None,
            Box::from([k_out, v_out, effect]),
            NodeKind::ChangeEventExpand {
                events: Box::from([
                    ChangeEventSpec {
                        predicate: Some(flag),
                        effect: ConnectorRowMutationEffect::Delete,
                        assignments: Box::from([(k_out, Some(delete_k)), (v_out, None)]),
                    },
                    ChangeEventSpec {
                        predicate: None,
                        effect: ConnectorRowMutationEffect::Insert,
                        assignments: Box::from([(k_out, Some(insert_k)), (v_out, Some(insert_v))]),
                    },
                ]),
                effect_output: effect,
            },
        )
        .unwrap();
    compile(package(builder, expand, ConstantPools::empty(), 1), 1)
}

#[test]
fn compiled_change_events_emit_selected_rows_per_event_with_exact_effects() {
    let program = change_program();
    assert!(matches!(
        program.graph().nodes()[1].kind(),
        ProgramNodeKind::ChangeEventExpand { .. }
    ));
    let chunks = run(&program);
    let mut rows = Vec::new();
    for chunk in &chunks {
        let k = chunk
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let v = chunk
            .batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let effect = chunk
            .batch
            .column(2)
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap();
        for row in 0..chunk.len() {
            rows.push((
                (!k.is_null(row)).then(|| k.value(row)),
                (!v.is_null(row)).then(|| v.value(row)),
                effect.value(row),
            ));
        }
    }
    // Events run in frozen order per input batch; each keeps input order.
    let delete = ConnectorRowMutationEffect::Delete as i8;
    let insert = ConnectorRowMutationEffect::Insert as i8;
    let mut expected = CHANGE_ROWS
        .iter()
        .filter(|(_, _, flag)| *flag == Some(true))
        .map(|(k, _, _)| (Some(*k), None, delete))
        .collect::<Vec<_>>();
    expected.extend(CHANGE_ROWS.iter().map(|(k, v, _)| (Some(*k), *v, insert)));
    assert_eq!(rows, expected);
}

#[test]
fn compiled_change_event_factory_refuses_a_node_that_is_not_an_expansion() {
    let program = change_program();
    let error = match CompiledChangeEventProcessorFactory::try_new(
        Arc::clone(&program),
        ProgramNodeId::new(0),
        Arc::new(RuntimeErrorState::default()),
    ) {
        Ok(_) => panic!("a Values node is not a ChangeEventExpand"),
        Err(error) => error,
    };
    assert!(error.contains("is not a ChangeEventExpand"), "{error}");
}
