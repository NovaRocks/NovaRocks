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

//! Compiled global Sort and Single TopN, compiled by local-compiler and run
//! through compiled SortOrder roots only. The oracle is an independent stable
//! Rust sort over the literal rows.

use crate::exec::pipeline::{step::NextStep, test_driver::TestDriver};
use std::cmp::Ordering;
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::record_batch::RecordBatch;
use novarocks_local_program::{LocalProgram, ProgramNodeId, ProgramNodeKind};
use novarocks_physical_plan::{
    ConstantPools, ExprKind, FragmentBuilder, FragmentId, NodeId, NullOrdering, SortDirection,
    SortExpr, SortMode, TopNPhase, TopNSequenceId,
};

use super::family_fixture::{cell, compile, int64, int64_rows, package, run, try_compile, values};
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::operators::compiled_sort::CompiledSortProcessorFactory;
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::runtime_state::{RuntimeErrorState, RuntimeState};

/// Literal rows `(a, b)`: `a` has NULLs and ties, `b` records arrival order.
const ROWS: [(Option<i64>, i64); 8] = [
    (Some(3), 1),
    (None, 2),
    (Some(1), 3),
    (Some(3), 4),
    (Some(2), 5),
    (None, 6),
    (Some(1), 7),
    (Some(-4), 8),
];

#[derive(Clone, Copy)]
struct Key {
    column: usize,
    ascending: bool,
    nulls_first: bool,
}

enum Shape {
    Sort,
    TopN { limit: u64, offset: u64 },
}

fn sort_expr(expr: novarocks_physical_plan::ExprId, key: Key) -> SortExpr {
    SortExpr {
        expr,
        direction: if key.ascending {
            SortDirection::Ascending
        } else {
            SortDirection::Descending
        },
        null_ordering: if key.nulls_first {
            NullOrdering::First
        } else {
            NullOrdering::Last
        },
    }
}

/// `Values(a, b) -> Sort|TopN(keys) -> Result`.
fn program(keys: &[Key], shape: Shape) -> Arc<LocalProgram> {
    let mut builder = FragmentBuilder::new(FragmentId::new(31));
    let source = NodeId::new(0);
    let sort = NodeId::new(1);
    let rows = ROWS
        .iter()
        .map(|(a, b)| vec![cell(*a), cell(Some(*b))])
        .collect::<Vec<_>>();
    let columns = values(&mut builder, source, &[int64(true), int64(false)], &rows);
    let order = keys
        .iter()
        .map(|key| {
            let ty = int64(key.column == 0);
            let expr = builder
                .add_expression(sort, ty, ExprKind::Value(columns[key.column]))
                .unwrap();
            sort_expr(expr, *key)
        })
        .collect::<Box<[_]>>();
    match shape {
        Shape::Sort => builder
            .add_sort(sort, source, order, SortMode::Global)
            .unwrap(),
        Shape::TopN { limit, offset } => builder
            .add_top_n(sort, source, order, limit, offset, TopNPhase::Single)
            .unwrap(),
    }
    compile(package(builder, sort, ConstantPools::empty(), 1), 1)
}

fn compare(left: Option<i64>, right: Option<i64>, key: Key) -> Ordering {
    match (left, right) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) if key.nulls_first => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (Some(_), None) if key.nulls_first => Ordering::Greater,
        (Some(_), None) => Ordering::Less,
        (Some(l), Some(r)) if key.ascending => l.cmp(&r),
        (Some(l), Some(r)) => r.cmp(&l),
    }
}

/// Independent stable sort of `rows` by `keys`, then `[offset, offset+limit)`.
fn oracle(
    rows: &[(Option<i64>, i64)],
    keys: &[Key],
    limit: Option<usize>,
    offset: usize,
) -> Vec<Vec<Option<i64>>> {
    let mut sorted = rows.to_vec();
    sorted.sort_by(|left, right| {
        for key in keys {
            let pick = |row: &(Option<i64>, i64)| {
                if key.column == 0 { row.0 } else { Some(row.1) }
            };
            let ordering = compare(pick(left), pick(right), *key);
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        Ordering::Equal
    });
    sorted
        .into_iter()
        .skip(offset)
        .take(limit.unwrap_or(usize::MAX))
        .map(|(a, b)| vec![a, Some(b)])
        .collect()
}

const A_DESC_NULLS_FIRST: Key = Key {
    column: 0,
    ascending: false,
    nulls_first: true,
};
const A_ASC_NULLS_LAST: Key = Key {
    column: 0,
    ascending: true,
    nulls_first: false,
};
const A_ASC_NULLS_FIRST: Key = Key {
    column: 0,
    ascending: true,
    nulls_first: true,
};
const B_DESC: Key = Key {
    column: 1,
    ascending: false,
    nulls_first: false,
};

#[test]
fn compiled_global_sort_orders_by_every_key_direction_and_null_placement() {
    let keys = [A_DESC_NULLS_FIRST, B_DESC];
    let program = program(&keys, Shape::Sort);
    let ProgramNodeKind::Sort {
        mode: novarocks_local_program::LocalSortMode::Full,
        ..
    } = program.graph().nodes()[1].kind()
    else {
        panic!("the compiler emits a local Sort");
    };
    let rows = int64_rows(&run(&program));
    assert_eq!(rows, oracle(&ROWS, &keys, None, 0));
    // Spot-check the independent oracle itself: NULLs first, then 3, 3 with
    // the larger arrival-order b first under b DESC.
    assert_eq!(rows[0], vec![None, Some(6)]);
    assert_eq!(rows[2], vec![Some(3), Some(4)]);
}

#[test]
fn compiled_global_sort_keeps_arrival_order_between_equal_keys() {
    let keys = [A_ASC_NULLS_LAST];
    let rows = int64_rows(&run(&program(&keys, Shape::Sort)));
    assert_eq!(rows, oracle(&ROWS, &keys, None, 0));
    assert_eq!(
        rows.iter().map(|row| row[1].unwrap()).collect::<Vec<_>>(),
        [8, 3, 7, 5, 1, 4, 2, 6]
    );
}

#[test]
fn compiled_single_topn_returns_the_exact_offset_window() {
    let keys = [A_ASC_NULLS_FIRST];
    let program = program(
        &keys,
        Shape::TopN {
            limit: 3,
            offset: 2,
        },
    );
    let ProgramNodeKind::Sort {
        mode: novarocks_local_program::LocalSortMode::RowsTopN { limit, offset },
        ..
    } = program.graph().nodes()[1].kind()
    else {
        panic!("the compiler lowers Single TopN into the local Sort owner");
    };
    assert_eq!((*limit, *offset), (3, 2));
    let rows = int64_rows(&run(&program));
    assert_eq!(rows, oracle(&ROWS, &keys, Some(3), 2));
    assert_eq!(rows.len(), 3);
}

#[test]
fn compiled_topn_with_offset_past_input_or_zero_limit_emits_nothing() {
    for (limit, offset) in [(0, 0), (4, 8), (2, 100)] {
        let rows = int64_rows(&run(&program(
            &[A_ASC_NULLS_FIRST],
            Shape::TopN { limit, offset },
        )));
        assert!(rows.is_empty(), "limit={limit} offset={offset}");
    }
}

/// Drive the compiled TopN operator directly with a tiny prune threshold so
/// its buffer is pruned to `offset + limit` rows between chunks.
#[test]
fn compiled_topn_pruning_keeps_exactly_the_rows_a_full_sort_would() {
    let keys = [A_DESC_NULLS_FIRST];
    let (limit, offset) = (3, 1);
    let program = program(
        &keys,
        Shape::TopN {
            limit: limit as u64,
            offset: offset as u64,
        },
    );
    let factory = CompiledSortProcessorFactory::try_new(
        Arc::clone(&program),
        ProgramNodeId::new(1),
        Arc::new(RuntimeErrorState::default()),
    )
    .unwrap()
    .with_prune_rows(2);
    let mut operator = factory.create(1, 0);
    let tracker = crate::runtime::mem_tracker::MemTracker::new_root("original TopN construction");
    tracker.install_limit_once(64 * 1024 * 1024).unwrap();
    operator.set_mem_tracker(tracker.clone());
    operator.set_construction_factory(super::aggregate_fixture::runtime_construction_factory(
        tracker,
        Arc::new(RuntimeErrorState::default()),
    ));
    operator.prepare().unwrap();
    let state = RuntimeState::default();
    let mut processor = TestDriver::new(operator.as_mut(), &state);
    let layout = program.graph().nodes()[0].output_layout();
    let schema = ChunkSchema::from_compiled_layout(layout).unwrap();
    // Twelve rows in four chunks; ties on `a` resolve by arrival (`b`).
    let input = [
        (Some(5), 1),
        (Some(9), 2),
        (None, 3),
        (Some(9), 4),
        (Some(1), 5),
        (Some(7), 6),
        (None, 7),
        (Some(9), 8),
        (Some(8), 9),
        (Some(5), 10),
        (Some(2), 11),
        (Some(9), 12),
    ];
    for part in input.chunks(3) {
        let batch = RecordBatch::try_new(
            layout.schema().clone(),
            vec![
                Arc::new(Int64Array::from(
                    part.iter().map(|row| row.0).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    part.iter().map(|row| row.1).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        let chunk = Chunk::try_new_with_chunk_schema(batch, Arc::clone(&schema)).unwrap();
        processor.input_without_output(chunk).unwrap();
    }
    processor.signal_eos().unwrap();
    let mut chunks = Vec::new();
    while let Some(chunk) = processor
        .advance_to_boundary()
        .map(|step| step.output)
        .unwrap()
    {
        chunks.push(chunk);
    }
    assert!(matches!(processor.next(), NextStep::Finished));
    assert_eq!(
        int64_rows(&chunks),
        oracle(&input, &keys, Some(limit), offset)
    );
    assert_eq!(
        int64_rows(&chunks),
        [
            vec![None, Some(7)],
            vec![Some(9), Some(2)],
            vec![Some(9), Some(4)]
        ]
    );
}

#[test]
fn compiled_sort_factory_refuses_a_node_that_is_not_a_sort() {
    let program = program(&[A_ASC_NULLS_LAST], Shape::Sort);
    let error = match CompiledSortProcessorFactory::try_new(
        Arc::clone(&program),
        ProgramNodeId::new(0),
        Arc::new(RuntimeErrorState::default()),
    ) {
        Ok(_) => panic!("a Values node is not a Sort"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("is not a Sort"), "{error}");
}

#[test]
fn compiled_sort_retains_original_column_metadata_until_input_or_processor_exit() {
    use crate::runtime::mem_tracker::MemTracker;
    for finish in [false, true] {
        let program = program(&[A_ASC_NULLS_LAST], Shape::Sort);
        let tracker = MemTracker::new_root("sort-input-column-owner");
        let chunk = super::family_fixture::paid_input(&program, &tracker);
        let charged = tracker.current();
        assert!(charged > 0);
        let state = RuntimeState::default();
        let factory = CompiledSortProcessorFactory::try_new(
            Arc::clone(&program),
            ProgramNodeId::new(1),
            state.error_state(),
        )
        .unwrap();
        let mut operator = factory.create(1, 0);
        // Construction has its own owner; the tracker above observes input metadata.
        let construction_tracker = MemTracker::new_root("sort-typed-construction-fixture");
        let construction = crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
            construction_tracker.clone(),
            state.error_state(),
        )
        .unwrap();
        operator.set_mem_tracker(construction_tracker);
        operator.set_construction_factory(construction);
        operator.prepare().unwrap();
        operator.bind_runtime_state(&state).unwrap();
        let mut processor = TestDriver::new(operator.as_mut(), &state);
        processor.input_without_output(chunk).unwrap();
        assert_eq!(
            tracker.current(),
            charged,
            "the input container is still live"
        );
        if finish {
            processor.signal_eos().unwrap();
            let mut output = Vec::new();
            while let Some(chunk) = processor
                .advance_to_boundary()
                .map(|step| step.output)
                .unwrap()
            {
                output.push(chunk);
            }
            assert_eq!(
                int64_rows(&output),
                oracle(&ROWS, &[A_ASC_NULLS_LAST], None, 0)
            );
            assert_eq!(tracker.current(), 0, "original runs have actually exited");
        }
        drop(processor);
        drop(operator);
        assert_eq!(tracker.current(), 0);
    }
}

// A row-count partial TopN is the ordinary local TopN of its own instance:
// the top `limit` rows with offset 0, emitted as one ordered stream. Its
// pairing with a Final is a whole-plan fact (see `compiled_topn_split_tests`);
// a grouped-state partial is refused by the compiler.
#[test]
fn partial_topn_runs_as_its_instance_prefix() {
    let mut builder = FragmentBuilder::new(FragmentId::new(32));
    let source = NodeId::new(0);
    let topn = NodeId::new(1);
    let rows = ROWS
        .iter()
        .map(|(a, b)| vec![cell(*a), cell(Some(*b))])
        .collect::<Vec<_>>();
    let columns = values(&mut builder, source, &[int64(true), int64(false)], &rows);
    let key = builder
        .add_expression(topn, int64(true), ExprKind::Value(columns[0]))
        .unwrap();
    builder
        .add_top_n(
            topn,
            source,
            Box::from([sort_expr(key, A_ASC_NULLS_FIRST)]),
            3,
            0,
            TopNPhase::Partial {
                sequence: TopNSequenceId::new(1),
            },
        )
        .unwrap();
    let program = try_compile(package(builder, topn, ConstantPools::empty(), 1), 1)
        .unwrap_or_else(|error| panic!("a row-count partial TopN compiles: {error}"));
    let ProgramNodeKind::Sort {
        mode: novarocks_local_program::LocalSortMode::RowsTopN { limit, offset },
        ..
    } = program.graph().nodes()[1].kind()
    else {
        panic!("the compiler lowers a partial TopN into the local Sort owner");
    };
    assert_eq!((*limit, *offset), (3, 0));
    let rows = int64_rows(&run(&program));
    assert_eq!(rows, oracle(&ROWS, &[A_ASC_NULLS_FIRST], Some(3), 0));
}

type PartitionRow = (Option<i64>, Option<i64>, i64);

fn partition_program(
    rows: &[PartitionRow],
    kind: novarocks_physical_plan::PartitionTopNType,
    limit: u64,
    order: Option<Key>,
) -> Arc<LocalProgram> {
    let mut builder = FragmentBuilder::new(FragmentId::new(35));
    let source = NodeId::new(0);
    let sort = NodeId::new(1);
    let cells = rows
        .iter()
        .map(|(g, s, id)| vec![cell(*g), cell(*s), cell(Some(*id))])
        .collect::<Vec<_>>();
    let columns = values(
        &mut builder,
        source,
        &[int64(true), int64(true), int64(false)],
        &cells,
    );
    let partition = builder
        .add_expression(sort, int64(true), ExprKind::Value(columns[0]))
        .unwrap();
    let order_by = order
        .map(|key| {
            let expr = builder
                .add_expression(sort, int64(true), ExprKind::Value(columns[1]))
                .unwrap();
            sort_expr(expr, key)
        })
        .into_iter()
        .collect::<Box<[_]>>();
    builder
        .add_sort(
            sort,
            source,
            order_by,
            SortMode::PartitionTopN {
                partition_by: Box::from([sort_expr(
                    partition,
                    Key {
                        column: 0,
                        ascending: true,
                        nulls_first: true,
                    },
                )]),
                limit,
                kind,
            },
        )
        .unwrap();
    compile(package(builder, sort, ConstantPools::empty(), 1), 1)
}

/// Count all strictly preceding rows/values in the complete input, then use
/// an independent stable Rust sort. No pruning or incremental rank state.
fn partition_oracle(
    rows: &[PartitionRow],
    kind: novarocks_physical_plan::PartitionTopNType,
    limit: usize,
    order: Option<Key>,
) -> Vec<Vec<Option<i64>>> {
    let cmp = |a, b| order.map_or(Ordering::Equal, |key| compare(a, b, key));
    let mut sorted = rows.iter().copied().enumerate().collect::<Vec<_>>();
    sorted.sort_by(|(_, a), (_, b)| a.0.cmp(&b.0).then_with(|| cmp(a.1, b.1)));
    sorted
        .into_iter()
        .filter(|(arrival, row)| {
            let ahead = rows
                .iter()
                .filter(|other| other.0 == row.0 && cmp(other.1, row.1) == Ordering::Less)
                .count();
            let rank = match kind {
                novarocks_physical_plan::PartitionTopNType::RowNumber => {
                    1 + ahead
                        + rows[..*arrival]
                            .iter()
                            .filter(|other| {
                                other.0 == row.0 && cmp(other.1, row.1) == Ordering::Equal
                            })
                            .count()
                }
                novarocks_physical_plan::PartitionTopNType::Rank => 1 + ahead,
                novarocks_physical_plan::PartitionTopNType::DenseRank => {
                    1 + rows
                        .iter()
                        .filter(|other| other.0 == row.0 && cmp(other.1, row.1) == Ordering::Less)
                        .map(|other| other.1)
                        .collect::<std::collections::BTreeSet<_>>()
                        .len()
                }
            };
            rank <= limit
        })
        .map(|(_, row)| vec![row.0, row.1, Some(row.2)])
        .collect()
}

fn partition_batches(
    program: &Arc<LocalProgram>,
    rows: &[PartitionRow],
    sizes: impl IntoIterator<Item = usize>,
) -> Vec<Vec<Option<i64>>> {
    let state = RuntimeState::default();
    let factory = CompiledSortProcessorFactory::try_new(
        Arc::clone(program),
        ProgramNodeId::new(1),
        state.error_state(),
    )
    .unwrap()
    .with_prune_rows(2);
    let mut operator = factory.create(1, 0);
    let tracker = crate::runtime::mem_tracker::MemTracker::new_root("partition TopN construction");
    tracker.install_limit_once(64 * 1024 * 1024).unwrap();
    operator.set_mem_tracker(tracker.clone());
    operator.set_construction_factory(super::aggregate_fixture::runtime_construction_factory(
        tracker,
        state.error_state(),
    ));
    operator.prepare().unwrap();
    operator.bind_runtime_state(&state).unwrap();
    let layout = program.graph().nodes()[0].output_layout();
    let schema = ChunkSchema::from_compiled_layout(layout).unwrap();
    let mut processor = TestDriver::new(operator.as_mut(), &state);
    let mut start = 0;
    for size in sizes {
        if start == rows.len() {
            break;
        }
        let end = (start + size).min(rows.len());
        let part = &rows[start..end];
        let batch = RecordBatch::try_new(
            layout.schema().clone(),
            vec![
                Arc::new(Int64Array::from(
                    part.iter().map(|row| row.0).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    part.iter().map(|row| row.1).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    part.iter().map(|row| row.2).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        processor
            .input_without_output(
                Chunk::try_new_with_chunk_schema(batch, Arc::clone(&schema)).unwrap(),
            )
            .unwrap();
        start = end;
    }
    assert_eq!(start, rows.len());
    processor.signal_eos().unwrap();
    let mut chunks = Vec::new();
    while let Some(chunk) = processor.advance_to_boundary().unwrap().output {
        chunks.push(chunk);
    }
    assert!(matches!(processor.next(), NextStep::Finished));
    int64_rows(&chunks)
}

#[test]
fn compiled_partition_topn_matches_rank_definitions_across_batches_and_pruning() {
    use novarocks_physical_plan::PartitionTopNType::{DenseRank, Rank, RowNumber};
    let mut seed = 0x7654_3210_1357_2468_u64;
    let rows = (0..96)
        .map(|index| {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let group = match (seed >> 32) % 5 {
                0 => None,
                g => Some(g as i64),
            };
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let score = match (seed >> 32) % 5 {
                0 => None,
                s => Some(s as i64 * 10),
            };
            (group, score, i64::from(index) + 1)
        })
        .collect::<Vec<_>>();
    for kind in [RowNumber, Rank, DenseRank] {
        for order in [
            None,
            Some(Key {
                column: 1,
                ascending: true,
                nulls_first: true,
            }),
            Some(Key {
                column: 1,
                ascending: false,
                nulls_first: false,
            }),
        ] {
            for limit in [1, 2, 200] {
                let program = partition_program(&rows, kind, limit as u64, order);
                let expected = partition_oracle(&rows, kind, limit, order);
                for split in [1, 0x1122_3344, 0xffff_eeee] {
                    assert_eq!(
                        partition_batches(&program, &rows, random_batch_sizes(split)),
                        expected,
                        "kind={kind:?} limit={limit} split={split}"
                    );
                }
            }
        }
    }
}

fn random_batch_sizes(mut seed: u64) -> impl Iterator<Item = usize> {
    std::iter::from_fn(move || {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        Some(1 + ((seed >> 32) as usize % 11))
    })
}

#[test]
fn compiled_partition_topn_keeps_late_peers_after_several_prunes() {
    use novarocks_physical_plan::PartitionTopNType::{DenseRank, Rank, RowNumber};
    let scores = [
        8, 9, 2, 3, 4, 5, 6, 7, 1, 1, 1, 4, 4, 4, 0, 0, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10,
        10, 0, 0,
    ];
    let rows = scores
        .into_iter()
        .enumerate()
        .map(|(i, score)| (Some(1), Some(score), i as i64 + 1))
        .collect::<Vec<_>>();
    let order = Some(Key {
        column: 1,
        ascending: true,
        nulls_first: true,
    });
    for kind in [RowNumber, Rank, DenseRank] {
        let program = partition_program(&rows, kind, 2, order);
        let actual = partition_batches(&program, &rows, [2, 6, 6, 14, 2]);
        assert_eq!(actual, partition_oracle(&rows, kind, 2, order));
        let ids = actual.iter().map(|row| row[2].unwrap()).collect::<Vec<_>>();
        assert_eq!(
            ids,
            match kind {
                RowNumber => vec![15, 16],
                Rank => vec![15, 16, 29, 30],
                DenseRank => vec![15, 16, 29, 30, 9, 10, 11],
            }
        );
    }
}

#[test]
fn compiled_partition_topn_preserves_refusal_and_releases_retained_input_on_exit() {
    use crate::runtime::{mem_tracker::MemTracker, task::ExecutionFailure};
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    use novarocks_functions::KernelFailure;
    use novarocks_physical_plan::PartitionTopNType::DenseRank;
    let rows = [
        (Some(1), Some(3), 1),
        (Some(1), Some(2), 2),
        (Some(1), Some(1), 3),
    ];
    for cancel in [false, true] {
        let program = partition_program(&rows, DenseRank, 2, Some(A_ASC_NULLS_FIRST));
        let input_tracker = MemTracker::new_root("partition input metadata");
        let chunk = super::family_fixture::paid_input(&program, &input_tracker);
        let tracker = MemTracker::new_root("partition refusal construction");
        let state = RuntimeState::default();
        let factory = CompiledSortProcessorFactory::try_new(
            program,
            ProgramNodeId::new(1),
            state.error_state(),
        )
        .unwrap()
        .with_prune_rows(2);
        let mut operator = factory.create(1, 0);
        operator.set_mem_tracker(tracker.clone());
        operator.set_construction_factory(super::aggregate_fixture::runtime_construction_factory(
            tracker.clone(),
            state.error_state(),
        ));
        operator.prepare().unwrap();
        operator.bind_runtime_state(&state).unwrap();
        let mut driver = TestDriver::new(operator.as_mut(), &state);
        driver.input_without_output(chunk).unwrap();
        let original = if cancel {
            let original = ExecutionFailure::from(KernelFailure::Cancelled);
            state.error_state().set_error(original.clone());
            Some(original)
        } else {
            tracker.install_limit_once(tracker.current()).unwrap();
            None
        };
        // Cancellation can be observed by either protocol entry; neither is
        // evidence that retained construction has already exited.
        let failure = match driver.signal_eos() {
            Err(failure) => failure,
            Ok(()) => driver.advance_to_boundary().unwrap_err(),
        };
        assert_eq!(
            failure.failure_fact().class(),
            if cancel {
                FailureClass::Control
            } else {
                FailureClass::Resource
            }
        );
        if let Some(original) = original {
            assert_eq!(failure, original);
        }
        assert_eq!(
            driver.advance_to_boundary().unwrap_err(),
            failure,
            "the first refusal remains authoritative"
        );
        drop(driver);
        if cancel {
            operator.cancel();
        } else {
            operator.begin_exit().unwrap();
        }
        assert_eq!(
            input_tracker.current(),
            0,
            "input metadata exits with actual retained runs"
        );
        drop(operator);
        assert_eq!(
            tracker.current(),
            0,
            "construction responsibility ends after actual operator exit"
        );
    }
}
