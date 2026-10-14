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

//! Actual compiled Partial -> Grouped Intermediate -> Final owners.
//! A separate complete BTreeMap/SQL key order oracle proves all contributions;
//! the N bound is checked during consumption, not inferred from final output.
//! Declared local Values/catalog/tracker inputs need no external fixture or
//! processes. Execution owns this affected-change component lane. Native SQL,
//! remaining key domains and variable state resources have separate gates.

use super::aggregate_fixture::{
    CallSpec, add_aggregate, aggregate_catalog, bind, compile, finish, packages, values,
};
use super::family_fixture::{FixtureControl, cell, int64};
use super::*;
use crate::exec::operators::compiled_aggregate::CompiledAggregateProcessorFactory;
use crate::runtime::fragment::ExecutionFailureCause;
use arrow::array::{
    BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray, Int8Array, Int16Array,
    Int32Array, Int64Array, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use novarocks_functions::{ConstantPool, PureKernelAbi};
use novarocks_physical_plan::{
    AggregateCall, AggregateCallId, AggregateGrouping, AggregatePhase, AggregateSequenceId,
    ConstantPoolId, ConstantPools, ConstantReference, ExprKind, FragmentBuilder, FragmentId,
    FragmentSink, LiteralValue, NodeId, NodeKind, NullOrdering, OrderingKey, OutputPort,
    PhysicalNode, PlanBuilder, PlanVersionId, ResultField, ResultPort, SortDirection, SortExpr,
    TopNPhase, TopNReduction, TopNSequenceId, ValueOrigin,
};
use novarocks_type_contract::{CompilePhase, FunctionValueType, ValueLogicalType};
use std::collections::BTreeMap;

type Input = (Option<i64>, Option<i64>);
type Output = (Option<i64>, i64, Option<i64>, Option<i64>, Option<i64>);
const ROWS: &[Input] = &[
    (Some(8), Some(80)),
    (Some(3), Some(30)),
    (Some(8), Some(8)),
    (Some(1), Some(10)),
    (None, Some(7)),
    (Some(2), None),
    (Some(1), Some(11)),
    (Some(9), Some(90)),
    (None, None),
    (Some(2), Some(20)),
    (Some(1), None),
    (Some(7), Some(70)),
    (None, Some(-2)),
    (Some(2), Some(22)),
    (Some(1), Some(-3)),
];

struct Fixture {
    program: Arc<LocalProgram>,
    input: Chunk,
    partial: ProgramNodeId,
    grouped: ProgramNodeId,
    final_node: ProgramNodeId,
}
fn fixture(limit: usize, asc: bool, nulls_first: bool) -> Fixture {
    fixture_with_keys(
        &[int64(true)],
        &ROWS
            .iter()
            .map(|(key, value)| vec![cell(*key), cell(*value)])
            .collect::<Vec<_>>(),
        &[(0, asc, nulls_first)],
        limit,
    )
}

/// These declarations change the legal key domain and rank permutation only;
/// all calls, frozen effects and runtime state phases still use the real owners.
fn fixture_with_keys(
    key_types: &[FunctionValueType],
    rows: &[Vec<LiteralValue>],
    ranking: &[(usize, bool, bool)],
    limit: usize,
) -> Fixture {
    fixture_with_calls(key_types, rows, ranking, limit, false)
}

fn largeint_type() -> FunctionValueType {
    FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        true,
        ValueLogicalType::LargeInt,
    )
    .unwrap()
}

fn fixture_with_calls(
    key_types: &[FunctionValueType],
    rows: &[Vec<LiteralValue>],
    ranking: &[(usize, bool, bool)],
    limit: usize,
    n_lists: bool,
) -> Fixture {
    fixture_with_independent_limits(key_types, rows, ranking, limit, n_lists, false)
}

fn fixture_with_independent_limits(
    key_types: &[FunctionValueType],
    rows: &[Vec<LiteralValue>],
    ranking: &[(usize, bool, bool)],
    limit: usize,
    n_lists: bool,
    independent_n_limits: bool,
) -> Fixture {
    assert!(!independent_n_limits || n_lists);
    let groups = key_types.len();
    assert_eq!(ranking.len(), groups);
    let owners: &[(&str, PureKernelAbi)] = if n_lists {
        &[
            ("min_n", PureKernelAbi::AggregateV1),
            ("max_n", PureKernelAbi::AggregateV1),
        ]
    } else {
        &[
            ("count", PureKernelAbi::AggregateWindowV1),
            ("sum", PureKernelAbi::AggregateWindowV1),
            ("min", PureKernelAbi::AggregateWindowV1),
            ("max", PureKernelAbi::AggregateWindowV1),
        ]
    };
    let catalog = aggregate_catalog(owners);
    let bounds = owners
        .iter()
        .map(|(name, _)| {
            let arguments = if n_lists {
                vec![largeint_type(), int64(false)]
            } else if *name == "count" {
                vec![]
            } else {
                vec![int64(true)]
            };
            bind(&catalog, name, &arguments)
        })
        .collect::<Vec<_>>();
    let mut builder = FragmentBuilder::new(FragmentId::new(1));
    let source = builder.reserve_node_id().unwrap();
    let mut types = key_types.to_vec();
    if n_lists {
        types.extend([largeint_type(), int64(false)]);
        if independent_n_limits {
            types.push(int64(false));
        }
    } else {
        types.push(int64(true));
    }
    let (columns, constants) = declared_values(&mut builder, source, &types, rows);
    let sequence = |ordinal: usize| AggregateSequenceId::new(41 + ordinal as u32);
    let partial_calls = bounds
        .iter()
        .enumerate()
        .map(|(ordinal, bound)| CallSpec {
            bound,
            id: AggregateCallId::new(10 + ordinal as u32),
            phase: AggregatePhase::Partial {
                sequence: sequence(ordinal),
            },
            distinct: false,
            arguments: if n_lists {
                let limit_column = groups + 1 + usize::from(independent_n_limits && ordinal == 1);
                vec![columns[groups], columns[limit_column]]
            } else if ordinal == 0 {
                vec![]
            } else {
                vec![columns[groups]]
            },
        })
        .collect::<Vec<_>>();
    let (partial, partial_output) = add_aggregate(
        &mut builder,
        source,
        &columns[..groups],
        &partial_calls,
        AggregateGrouping::Partial,
    );
    let grouped = builder.reserve_node_id().unwrap();
    let keys = key_types
        .iter()
        .zip(&columns)
        .map(|(ty, value)| {
            builder
                .add_expression(grouped, ty.clone(), ExprKind::Value(*value))
                .unwrap()
        })
        .collect::<Vec<_>>();
    let mut outputs = columns[..groups].to_vec();
    let mut calls = Vec::new();
    for (ordinal, bound) in bounds.iter().enumerate() {
        let id = AggregateCallId::new(20 + ordinal as u32);
        let phase = AggregatePhase::Intermediate {
            sequence: sequence(ordinal),
        };
        let argument = builder
            .add_expression(
                grouped,
                bound.intermediate_type(),
                ExprKind::Value(partial_output[ordinal + groups]),
            )
            .unwrap();
        let output = builder
            .add_value(
                bound.output_type(phase),
                ValueOrigin::AggregateState { call: id, phase },
            )
            .unwrap();
        outputs.push(output);
        calls.push(AggregateCall {
            id,
            binding: bound.binding(phase),
            arguments: Box::from([argument]),
            distinct: false,
            order_by: Box::default(),
            output,
        });
    }
    let direction = |asc| {
        if asc {
            SortDirection::Ascending
        } else {
            SortDirection::Descending
        }
    };
    let null_ordering = |first| {
        if first {
            NullOrdering::First
        } else {
            NullOrdering::Last
        }
    };
    let required = builder.node_output_properties(partial).unwrap().clone();
    let mut produced = required.clone();
    produced.ordering = ranking
        .iter()
        .map(|&(ordinal, asc, first)| OrderingKey {
            value: columns[ordinal],
            direction: direction(asc),
            null_ordering: null_ordering(first),
        })
        .collect();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: grouped,
            inputs: Box::from([partial]),
            required_inputs: Box::from([required]),
            output_properties: produced,
            output: OutputPort {
                node: grouped,
                columns: outputs.clone().into_boxed_slice(),
            },
            kind: NodeKind::TopN {
                order_by: ranking
                    .iter()
                    .map(|&(ordinal, asc, first)| SortExpr {
                        expr: keys[ordinal],
                        direction: direction(asc),
                        null_ordering: null_ordering(first),
                    })
                    .collect(),
                limit: limit as u64,
                offset: 0,
                phase: TopNPhase::Partial {
                    sequence: TopNSequenceId::new(42),
                },
                reduction: TopNReduction::GroupedStates {
                    group_by: keys.iter().copied().zip(columns.iter().copied()).collect(),
                    calls: calls.into_boxed_slice(),
                    comparator:
                        novarocks_type_contract::OrderedComparisonAlgorithm::NativeScalarOrderV1,
                },
            },
        })
        .unwrap();
    let final_calls = bounds
        .iter()
        .enumerate()
        .map(|(ordinal, bound)| CallSpec {
            bound,
            id: AggregateCallId::new(30 + ordinal as u32),
            phase: AggregatePhase::Final {
                sequence: sequence(ordinal),
            },
            distinct: false,
            arguments: vec![outputs[ordinal + groups]],
        })
        .collect::<Vec<_>>();
    let (final_node, _) = add_aggregate(
        &mut builder,
        grouped,
        &columns[..groups],
        &final_calls,
        AggregateGrouping::Complete,
    );
    let topn = builder.reserve_node_id().unwrap();
    let order_by = ranking
        .iter()
        .map(|&(ordinal, asc, first)| {
            let key = builder
                .add_expression(
                    topn,
                    key_types[ordinal].clone(),
                    ExprKind::Value(columns[ordinal]),
                )
                .unwrap();
            SortExpr {
                expr: key,
                direction: direction(asc),
                null_ordering: null_ordering(first),
            }
        })
        .collect();
    builder
        .add_top_n(
            topn,
            final_node,
            order_by,
            limit as u64,
            0,
            TopNPhase::Final {
                sequence: TopNSequenceId::new(42),
            },
        )
        .unwrap();
    let definition = finish(builder, topn, FragmentSink::Result, 2);
    let output = definition.nodes()[&topn].output.clone();
    let fields = output
        .columns
        .iter()
        .enumerate()
        .map(|(ordinal, value)| ResultField {
            domain: crate::test_result_domain::result_value_domain(&definition.values()[value].ty),
            name: format!("field-{ordinal}").into(),
            alias: None,
            value: *value,
            ty: definition.values()[value].ty.clone(),
        })
        .collect();
    let mut plan =
        PlanBuilder::new(PlanVersionId::try_new([121; 16]).unwrap()).with_constant_pools(constants);
    plan.add_fragment(definition).unwrap();
    plan.set_result_port(ResultPort {
        scalar_schema: None,
        fragment: FragmentId::new(1),
        output,
        fields,
    })
    .unwrap();
    let plan = plan.finish_observed(&FixtureControl).unwrap();
    let package = packages(&plan, &catalog)
        .remove(&FragmentId::new(1))
        .unwrap();
    let program = compile(package, &catalog, 2, true);
    let mut source = None;
    let mut partial = None;
    let mut grouped = None;
    let mut final_node = None;
    for (index, node) in program.graph().nodes().iter().enumerate() {
        match node.kind() {
            ProgramNodeKind::Values { values } => {
                source = Some(Chunk::new_with_chunk_schema(
                    values.batch().unwrap().clone(),
                    ChunkSchema::from_compiled_layout(node.output_layout()).unwrap(),
                ))
            }
            ProgramNodeKind::Aggregate { need_finalize, .. } => {
                if *need_finalize {
                    final_node = Some(ProgramNodeId::new(index));
                } else {
                    partial = Some(ProgramNodeId::new(index));
                }
            }
            ProgramNodeKind::GroupedTopN { .. } => grouped = Some(ProgramNodeId::new(index)),
            _ => {}
        }
    }
    Fixture {
        program,
        input: source.unwrap(),
        partial: partial.unwrap(),
        grouped: grouped.unwrap(),
        final_node: final_node.unwrap(),
    }
}

/// Physical literal Int64 is exactly BIGINT, so narrow signed cells use the
/// same checked constant-pool author as other compiled Values fixtures.
fn declared_values(
    builder: &mut FragmentBuilder,
    source: NodeId,
    types: &[FunctionValueType],
    rows: &[Vec<LiteralValue>],
) -> (Vec<novarocks_physical_plan::ValueId>, ConstantPools) {
    if !types.iter().any(|ty| {
        matches!(
            ty.data_type,
            DataType::Int8 | DataType::Int16 | DataType::Int32
        )
    }) {
        return (values(builder, source, types, rows), ConstantPools::empty());
    }
    let mut pools = ConstantPools::empty();
    let mut pool_ids = Vec::new();
    for (column, ty) in types.iter().enumerate() {
        if !matches!(
            ty.data_type,
            DataType::Int8 | DataType::Int16 | DataType::Int32
        ) {
            pool_ids.push(None);
            continue;
        }
        let row_values = rows
            .iter()
            .map(|row| match row[column] {
                LiteralValue::Null => None,
                LiteralValue::Int64(value) => Some(value),
                _ => panic!("narrow signed fixture literal"),
            })
            .collect::<Vec<_>>();
        // Repeated rows refer to the same declared pool cell; keep the original
        // small-fixture policy instead of widening its constant row limit.
        let mut distinct = row_values.clone();
        distinct.sort();
        distinct.dedup();
        let ordinals = row_values
            .iter()
            .map(|value| u32::try_from(distinct.binary_search(value).unwrap()).unwrap())
            .collect::<Vec<_>>();
        let values = distinct.into_iter();
        let array: ArrayRef = match ty.data_type {
            DataType::Int8 => Arc::new(Int8Array::from_iter(
                values.map(|value| value.map(|value| i8::try_from(value).unwrap())),
            )),
            DataType::Int16 => Arc::new(Int16Array::from_iter(
                values.map(|value| value.map(|value| i16::try_from(value).unwrap())),
            )),
            DataType::Int32 => Arc::new(Int32Array::from_iter(
                values.map(|value| value.map(|value| i32::try_from(value).unwrap())),
            )),
            _ => unreachable!("only declared narrow integer pools"),
        };
        let pool = ConstantPool::try_new(
            Arc::new(ty.try_to_field("declared-narrow-source").unwrap()),
            ty.clone(),
            array.to_data(),
            super::family_fixture::constant_policy(),
            CompilePhase::Validate,
            &FixtureControl,
        )
        .unwrap();
        let id = ConstantPoolId::new(column as u32 + 1);
        pools.insert(id, pool).unwrap();
        pool_ids.push(Some((id, ordinals)));
    }
    let columns = types
        .iter()
        .enumerate()
        .map(|(ordinal, ty)| {
            builder
                .add_value(
                    ty.clone(),
                    ValueOrigin::NodeOutput {
                        node: source,
                        output_ordinal: ordinal as u32,
                    },
                )
                .unwrap()
        })
        .collect::<Vec<_>>();
    let cells = rows
        .iter()
        .enumerate()
        .map(|(ordinal, row)| {
            row.iter()
                .enumerate()
                .map(|(column, literal)| {
                    let kind = match &pool_ids[column] {
                        Some((pool, ordinals)) => ExprKind::Constant(ConstantReference {
                            pool: *pool,
                            ordinal: ordinals[ordinal],
                        }),
                        None => ExprKind::Literal(literal.clone()),
                    };
                    builder
                        .add_expression(source, types[column].clone(), kind)
                        .unwrap()
                })
                .collect::<Box<[_]>>()
        })
        .collect();
    builder
        .add_values(source, cells, columns.clone().into_boxed_slice())
        .unwrap();
    (columns, pools)
}

fn partial_chunks(f: &Fixture, offset: usize, len: usize) -> Vec<Chunk> {
    let factory = CompiledAggregateProcessorFactory::try_new(
        Arc::clone(&f.program),
        f.partial,
        Arc::new(RuntimeErrorState::default()),
    )
    .unwrap();
    let mut operator = factory.create(1, 0);
    operator.set_mem_tracker(MemTracker::new_root("grouped-fixture-partial"));
    let state = RuntimeState::default();
    let processor = operator.as_processor_mut().unwrap();
    processor
        .push_chunk(
            &state,
            Chunk::new_like(f.input.batch.slice(offset, len), &f.input),
        )
        .unwrap();
    processor.set_finishing(&state).unwrap();
    let mut output = Vec::new();
    while let Some(chunk) = processor.pull_chunk(&state).unwrap() {
        output.push(chunk);
    }
    output
}
fn oracle(limit: usize, asc: bool, nulls_first: bool) -> Vec<Output> {
    let mut all = BTreeMap::<Option<i64>, (i64, Option<i64>, Option<i64>, Option<i64>)>::new();
    for &(key, value) in ROWS {
        let state = all.entry(key).or_insert((0, None, None, None));
        state.0 += 1;
        if let Some(value) = value {
            state.1 = Some(state.1.unwrap_or(0) + value);
            state.2 = Some(state.2.map_or(value, |old| old.min(value)));
            state.3 = Some(state.3.map_or(value, |old| old.max(value)));
        }
    }
    let mut rows = all
        .into_iter()
        .map(|(key, (count, sum, min, max))| (key, count, sum, min, max))
        .collect::<Vec<_>>();
    rows.sort_by(|a, b| match (a.0, b.0) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => {
            if nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (Some(_), None) => {
            if nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        (Some(a), Some(b)) => {
            if asc {
                a.cmp(&b)
            } else {
                b.cmp(&a)
            }
        }
    });
    rows.truncate(limit);
    rows
}
fn final_chunks(f: &Fixture, chunks: Vec<Chunk>) -> Vec<Chunk> {
    final_chunks_tracked(f, chunks, MemTracker::new_root("grouped-fixture-final"))
}
fn final_chunks_tracked(f: &Fixture, chunks: Vec<Chunk>, tracker: Arc<MemTracker>) -> Vec<Chunk> {
    try_final_chunks_tracked(f, chunks, tracker).unwrap()
}
fn try_final_chunks_tracked(
    f: &Fixture,
    chunks: Vec<Chunk>,
    tracker: Arc<MemTracker>,
) -> ExecutionResult<Vec<Chunk>> {
    let factory = CompiledAggregateProcessorFactory::try_new(
        Arc::clone(&f.program),
        f.final_node,
        Arc::new(RuntimeErrorState::default()),
    )?;
    let mut operator = factory.create(1, 0);
    operator.set_mem_tracker(tracker);
    let state = RuntimeState::default();
    let processor = operator.as_processor_mut().unwrap();
    for chunk in chunks {
        processor.push_chunk(&state, chunk)?;
    }
    processor.set_finishing(&state)?;
    let mut output = Vec::new();
    while let Some(chunk) = processor.pull_chunk(&state)? {
        output.push(chunk);
    }
    Ok(output)
}

fn collect_final(f: &Fixture, chunks: Vec<Chunk>) -> Vec<Output> {
    read_final_rows(final_chunks(f, chunks))
}
fn read_final_rows(chunks: Vec<Chunk>) -> Vec<Output> {
    let mut result = Vec::new();
    for chunk in chunks {
        let columns = chunk
            .batch
            .columns()
            .iter()
            .map(|array| array.as_any().downcast_ref::<Int64Array>().unwrap())
            .collect::<Vec<_>>();
        let value = |column: usize, row: usize| {
            if columns[column].is_null(row) {
                None
            } else {
                Some(columns[column].value(row))
            }
        };
        for row in 0..chunk.len() {
            result.push((
                value(0, row),
                value(1, row).unwrap(),
                value(2, row),
                value(3, row),
                value(4, row),
            ));
        }
    }
    result
}

#[test]
fn compiled_grouped_topn_keeps_all_contributions_within_its_distinct_key_bound() {
    for (limit, asc, nulls_first) in [
        (0, true, true),
        (1, true, false),
        (2, true, true),
        (2, false, false),
        (2, false, true),
    ] {
        let f = fixture(limit, asc, nulls_first);
        let factory = CompiledGroupedTopNProcessorFactory::try_new(
            Arc::clone(&f.program),
            f.grouped,
            Arc::new(RuntimeErrorState::default()),
        )
        .unwrap();
        let mut processor = factory.processor();
        let tracker = MemTracker::new_root("grouped-fixture-consumer");
        processor.set_mem_tracker(tracker.clone());
        let state = RuntimeState::default();
        for (offset, len) in [(0, 3), (3, 2), (5, 4), (9, 2), (11, 4)] {
            for chunk in partial_chunks(&f, offset, len) {
                processor.push_chunk(&state, chunk).unwrap();
                assert!(
                    processor
                        .candidates
                        .as_ref()
                        .map_or(0, |set| set.entries.len())
                        <= limit
                );
                assert!(
                    !processor.has_output(),
                    "no unfinished state prefix escapes"
                );
            }
        }
        processor.set_finishing(&state).unwrap();
        let mut output = Vec::new();
        while let Some(chunk) = processor.pull_chunk(&state).unwrap() {
            output.push(chunk);
        }
        assert_eq!(collect_final(&f, output), oracle(limit, asc, nulls_first));
        assert!(processor.is_finished());
        assert_eq!(tracker.current(), 0);
    }
}

#[test]
fn compiled_grouped_topn_cancel_destroys_live_owners_and_keeps_the_first_cause() {
    let f = fixture(2, true, true);
    let error = Arc::new(RuntimeErrorState::default());
    let factory = CompiledGroupedTopNProcessorFactory::try_new(
        Arc::clone(&f.program),
        f.grouped,
        error.clone(),
    )
    .unwrap();
    let mut processor = factory.processor();
    let tracker = MemTracker::new_root("grouped-fixture-cancel");
    processor.set_mem_tracker(tracker.clone());
    let state = RuntimeState::default();
    for chunk in partial_chunks(&f, 0, 3) {
        processor.push_chunk(&state, chunk).unwrap();
    }
    assert!(tracker.current() > 0);
    error.set_error("stop the actual grouped consumer after accumulation");
    let first = processor.push_chunk(&state, f.input.clone()).unwrap_err();
    assert_eq!(
        first.cause(),
        &ExecutionFailureCause::Kernel(KernelFailure::Cancelled)
    );
    assert_eq!(tracker.current(), 0);
    assert_eq!(processor.pull_chunk(&state).unwrap_err(), first);
    assert_eq!(processor.set_finishing(&state).unwrap_err(), first);
    processor.cancel();
    assert_eq!(processor.close().unwrap_err(), first);
}

/// Independent declared scalar values. No Arrow row encoding or candidate
/// selection participates in the complete-map oracle; strings use UTF-8 order.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum KeyCell {
    Boolean(bool),
    Integer(i128),
    Text(String),
}
fn literal_key(literal: &LiteralValue) -> Option<KeyCell> {
    Some(match literal {
        LiteralValue::Null => return None,
        LiteralValue::Boolean(value) => KeyCell::Boolean(*value),
        LiteralValue::Int64(value) | LiteralValue::Timestamp(value) => {
            KeyCell::Integer(*value as i128)
        }
        LiteralValue::Date32(value) => KeyCell::Integer(*value as i128),
        LiteralValue::LargeInt(value) | LiteralValue::Decimal128(value) => KeyCell::Integer(*value),
        LiteralValue::Utf8(value) => KeyCell::Text(value.to_string()),
        _ => panic!("undeclared key oracle literal"),
    })
}
fn array_key(array: &ArrayRef, row: usize) -> Option<KeyCell> {
    if array.is_null(row) {
        return None;
    }
    macro_rules! integer {
        ($array:ty) => {
            KeyCell::Integer(array.as_any().downcast_ref::<$array>().unwrap().value(row) as i128)
        };
    }
    Some(match array.data_type() {
        DataType::Boolean => KeyCell::Boolean(
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(row),
        ),
        DataType::Int8 => integer!(Int8Array),
        DataType::Int16 => integer!(Int16Array),
        DataType::Int32 => integer!(Int32Array),
        DataType::Int64 => integer!(Int64Array),
        DataType::Date32 => integer!(Date32Array),
        DataType::Decimal128(_, _) => integer!(Decimal128Array),
        DataType::Timestamp(TimeUnit::Second, _) => integer!(TimestampSecondArray),
        DataType::Timestamp(TimeUnit::Millisecond, _) => integer!(TimestampMillisecondArray),
        DataType::Timestamp(TimeUnit::Microsecond, _) => integer!(TimestampMicrosecondArray),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => integer!(TimestampNanosecondArray),
        DataType::Utf8 => KeyCell::Text(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(row)
                .into(),
        ),
        DataType::FixedSizeBinary(16) => KeyCell::Integer(i128::from_be_bytes(
            array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap()
                .value(row)
                .try_into()
                .unwrap(),
        )),
        _ => panic!("undeclared key oracle carrier"),
    })
}
type KeyRow = Vec<Option<KeyCell>>;
type Measures = (i64, Option<i64>, Option<i64>, Option<i64>);
fn complete_key_oracle(
    rows: &[Vec<LiteralValue>],
    ranking: &[(usize, bool, bool)],
    limit: usize,
) -> Vec<(KeyRow, Measures)> {
    let groups = ranking.len();
    let mut all = BTreeMap::<KeyRow, Measures>::new();
    for row in rows {
        let key = row[..groups].iter().map(literal_key).collect();
        let aggregate = all.entry(key).or_insert((0, None, None, None));
        aggregate.0 += 1;
        match row[groups] {
            LiteralValue::Int64(value) => {
                aggregate.1 = Some(aggregate.1.unwrap_or(0) + value);
                aggregate.2 = Some(aggregate.2.map_or(value, |old| old.min(value)));
                aggregate.3 = Some(aggregate.3.map_or(value, |old| old.max(value)));
            }
            LiteralValue::Null => (),
            _ => panic!("undeclared aggregate input"),
        }
    }
    let mut result = all.into_iter().collect::<Vec<_>>();
    result.sort_by(|(a, _), (b, _)| {
        for &(ordinal, asc, first) in ranking {
            let order = match (&a[ordinal], &b[ordinal]) {
                (None, None) => Ordering::Equal,
                (None, Some(_)) => {
                    if first {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    }
                }
                (Some(_), None) => {
                    if first {
                        Ordering::Greater
                    } else {
                        Ordering::Less
                    }
                }
                (Some(a), Some(b)) => {
                    if asc {
                        a.cmp(b)
                    } else {
                        b.cmp(a)
                    }
                }
            };
            if order != Ordering::Equal {
                return order;
            }
        }
        Ordering::Equal
    });
    result.truncate(limit);
    result
}
fn key_domains() -> Vec<(FunctionValueType, Vec<LiteralValue>)> {
    let mut domains = vec![(
        FunctionValueType::new(DataType::Boolean, true),
        vec![
            LiteralValue::Null,
            LiteralValue::Boolean(false),
            LiteralValue::Boolean(true),
        ],
    )];
    for (ty, min, max) in [
        (DataType::Int8, i8::MIN as i64, i8::MAX as i64),
        (DataType::Int16, i16::MIN as i64, i16::MAX as i64),
        (DataType::Int32, i32::MIN as i64, i32::MAX as i64),
        (DataType::Int64, i64::MIN, i64::MAX),
    ] {
        domains.push((
            FunctionValueType::new(ty, true),
            vec![
                LiteralValue::Null,
                LiteralValue::Int64(min),
                LiteralValue::Int64(-1),
                LiteralValue::Int64(0),
                LiteralValue::Int64(max),
            ],
        ));
    }
    domains.push((
        FunctionValueType::new(DataType::Utf8, true),
        vec![
            LiteralValue::Null,
            LiteralValue::Utf8("".into()),
            LiteralValue::Utf8("a".into()),
            LiteralValue::Utf8("é".into()),
            LiteralValue::Utf8("中".into()),
        ],
    ));
    domains.push((
        FunctionValueType::new(DataType::Date32, true),
        vec![
            LiteralValue::Null,
            LiteralValue::Date32(i32::MIN),
            LiteralValue::Date32(-1),
            LiteralValue::Date32(0),
            LiteralValue::Date32(i32::MAX),
        ],
    ));
    for unit in [
        TimeUnit::Second,
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ] {
        for zone in [None, Some("UTC".into()), Some("Asia/Shanghai".into())] {
            domains.push((
                FunctionValueType::new(DataType::Timestamp(unit.clone(), zone), true),
                vec![
                    LiteralValue::Null,
                    LiteralValue::Timestamp(i64::MIN),
                    LiteralValue::Timestamp(-1),
                    LiteralValue::Timestamp(0),
                    LiteralValue::Timestamp(i64::MAX),
                ],
            ));
        }
    }
    for (precision, scale, bound) in [(38, 9, 10_i128.pow(38) - 1), (10, -2, 9_999_999_999)] {
        domains.push((
            FunctionValueType::new(DataType::Decimal128(precision, scale), true),
            vec![
                LiteralValue::Null,
                LiteralValue::Decimal128(-bound),
                LiteralValue::Decimal128(-1),
                LiteralValue::Decimal128(0),
                LiteralValue::Decimal128(bound),
            ],
        ));
    }
    domains.push((
        FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            true,
            ValueLogicalType::LargeInt,
        )
        .unwrap(),
        vec![
            LiteralValue::Null,
            LiteralValue::LargeInt(i128::MIN),
            LiteralValue::LargeInt(-1),
            LiteralValue::LargeInt(0),
            LiteralValue::LargeInt(i128::MAX),
        ],
    ));
    domains
}

/// Lowest sufficient lane for the complete NativeScalarOrderV1 domain and
/// group/rank permutation: local Values, real Partial/Intermediate/Final owners, full-map
/// oracle, no processes or external input. Run with affected Execution changes;
/// the small Cartesian declarations add semantic boundaries, no performance gate.
#[test]
fn compiled_grouped_topn_complete_key_domains_preserve_order_types_and_contributions() {
    for (ty, literals) in key_domains() {
        let mut rows = Vec::new();
        // Each complete key contributes through separate partial processors.
        // A NULL-only group also keeps SUM/MIN/MAX NULL while COUNT is nonzero.
        for pass in [2_i64, 0, 1] {
            for (ordinal, literal) in literals.iter().enumerate().rev() {
                for second in [None, Some(-2), Some(1)] {
                    let payload = if ordinal == 0 || pass == 1 {
                        None
                    } else {
                        Some(pass * 10 - ordinal as i64)
                    };
                    rows.push(vec![literal.clone(), cell(second), cell(payload)]);
                }
            }
        }
        for (limit, ranking) in [
            (0, [(0, true, true), (1, false, false)]),
            (1, [(1, true, false), (0, false, true)]),
            (2, [(0, true, false), (1, false, true)]),
            (2, [(1, false, true), (0, true, false)]),
            (2, [(0, false, false), (1, true, true)]),
            (2, [(1, false, false), (0, false, false)]),
            (2, [(0, true, true), (1, true, true)]),
        ] {
            let f = fixture_with_keys(&[ty.clone(), int64(true)], &rows, &ranking, limit);
            let factory = CompiledGroupedTopNProcessorFactory::try_new(
                Arc::clone(&f.program),
                f.grouped,
                Arc::new(RuntimeErrorState::default()),
            )
            .unwrap();
            let mut processor = factory.processor();
            let tracker = MemTracker::new_root("grouped-complete-key-domain");
            processor.set_mem_tracker(tracker.clone());
            let state = RuntimeState::default();
            let batch_size = rows.len() / 3;
            for offset in (0..rows.len()).step_by(batch_size) {
                for chunk in partial_chunks(&f, offset, batch_size) {
                    processor.push_chunk(&state, chunk).unwrap();
                    assert!(
                        processor
                            .candidates
                            .as_ref()
                            .map_or(0, |set| set.entries.len())
                            <= limit
                    );
                }
            }
            processor.set_finishing(&state).unwrap();
            let mut intermediate = Vec::new();
            while let Some(chunk) = processor.pull_chunk(&state).unwrap() {
                intermediate.push(chunk);
            }
            let expected = complete_key_oracle(&rows, &ranking, limit);
            let intermediate_keys = intermediate
                .iter()
                .flat_map(|chunk| {
                    (0..chunk.len()).map(|row| {
                        (0..2)
                            .map(|column| array_key(chunk.batch.column(column), row))
                            .collect::<KeyRow>()
                    })
                })
                .collect::<Vec<_>>();
            assert_eq!(
                intermediate_keys,
                expected
                    .iter()
                    .map(|(key, _)| key.clone())
                    .collect::<Vec<_>>(),
                "intermediate rank type={ty:?}, ranking={ranking:?}"
            );
            let mut actual = Vec::new();
            for chunk in final_chunks(&f, intermediate) {
                for ordinal in 0..2 {
                    let input_field = f.input.batch.schema().field(ordinal).clone();
                    let output_field = chunk.batch.schema().field(ordinal).clone();
                    assert_eq!(output_field.data_type(), input_field.data_type());
                    assert_eq!(output_field.metadata(), input_field.metadata());
                    assert_eq!(output_field.is_nullable(), input_field.is_nullable());
                }
                for row in 0..chunk.len() {
                    let key = (0..2)
                        .map(|column| array_key(chunk.batch.column(column), row))
                        .collect();
                    let measure = |column| {
                        let array = chunk
                            .batch
                            .column(column)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap();
                        (!array.is_null(row)).then(|| array.value(row))
                    };
                    actual.push((
                        key,
                        (measure(2).unwrap(), measure(3), measure(4), measure(5)),
                    ));
                }
            }
            // Final aggregate is unordered; compare complete values in rank order.
            actual.sort_by_key(|(key, _)| {
                expected
                    .iter()
                    .position(|(other, _)| key == other)
                    .expect("unexpected complete key")
            });
            assert_eq!(
                actual, expected,
                "type={ty:?}, ranking={ranking:?}, limit={limit}"
            );
            assert_eq!(tracker.current(), 0, "type={ty:?}, ranking={ranking:?}");
        }
    }
}

#[path = "compiled_grouped_topn_state_tests.rs"]
mod state_tests;

#[path = "compiled_grouped_topn_exchange_tests.rs"]
mod exchange_tests;

#[path = "compiled_grouped_topn_error_tests.rs"]
mod error_tests;
