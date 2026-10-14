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
        use_top_n, limit, ..
    } = program.graph().nodes()[1].kind()
    else {
        panic!("the compiler emits a local Sort");
    };
    assert!(!use_top_n);
    assert_eq!(*limit, None);
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
        use_top_n,
        limit,
        offset,
        ..
    } = program.graph().nodes()[1].kind()
    else {
        panic!("the compiler lowers Single TopN into the local Sort owner");
    };
    assert_eq!((*use_top_n, *limit, *offset), (true, Some(3), 2));
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
    let processor = operator.as_processor_mut().unwrap();
    let state = RuntimeState::default();
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
        processor.push_chunk(&state, chunk).unwrap();
    }
    processor.set_finishing(&state).unwrap();
    let mut chunks = Vec::new();
    while let Some(chunk) = processor.pull_chunk(&state).unwrap() {
        chunks.push(chunk);
    }
    assert!(processor.is_finished());
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
    assert!(error.contains("is not a Sort"), "{error}");
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
        let factory = CompiledSortProcessorFactory::try_new(
            Arc::clone(&program),
            ProgramNodeId::new(1),
            Arc::new(RuntimeErrorState::default()),
        )
        .unwrap();
        let mut operator = factory.create(1, 0);
        let state = RuntimeState::default();
        let processor = operator.as_processor_mut().unwrap();
        processor.push_chunk(&state, chunk).unwrap();
        assert_eq!(
            tracker.current(),
            charged,
            "the input container is still live"
        );
        if finish {
            processor.set_finishing(&state).unwrap();
            assert_eq!(tracker.current(), 0, "original runs have actually exited");
            let mut output = Vec::new();
            while let Some(chunk) = processor.pull_chunk(&state).unwrap() {
                output.push(chunk);
            }
            assert_eq!(
                int64_rows(&output),
                oracle(&ROWS, &[A_ASC_NULLS_LAST], None, 0)
            );
        }
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
        use_top_n,
        limit,
        offset,
        ..
    } = program.graph().nodes()[1].kind()
    else {
        panic!("the compiler lowers a partial TopN into the local Sort owner");
    };
    assert_eq!((*use_top_n, *limit, *offset), (true, Some(3), 0));
    let rows = int64_rows(&run(&program));
    assert_eq!(rows, oracle(&ROWS, &[A_ASC_NULLS_FIRST], Some(3), 0));
}
