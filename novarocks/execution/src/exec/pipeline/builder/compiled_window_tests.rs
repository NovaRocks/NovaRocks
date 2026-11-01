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

//! Compiled window (Analytic) packages compiled by local-compiler and run
//! through compiled roots and pure window kernels only. The oracle is an
//! independent Rust evaluation of each function over the literal rows: a
//! stable sort by the partition and order keys, explicit per-row frame
//! membership, and each function's SQL definition.

use crate::exec::pipeline::{step::NextStep, test_driver::TestDriver};
use std::cmp::Ordering;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array, Int64Array};
use arrow::record_batch::RecordBatch;
use novarocks_local_program::{LocalProgram, ProgramNodeId, ProgramNodeKind};
use novarocks_type_contract::{WindowBound, WindowFrameUnits};

use super::family_fixture::try_run;
use super::window_fixture::{Arg, Call, Key, Shape, compile, installed_catalog, package};
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::operators::compiled_window::CompiledWindowProcessorFactory;
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::runtime_state::{RuntimeErrorState, RuntimeState};

type Row = Vec<Option<i64>>;

/// `(p, o, x)`: partitions with NULL keys, order-key ties and NULLs, and NULL
/// values. Arrival order is deliberately not sorted.
fn rows() -> Vec<Row> {
    [
        [Some(1), Some(10), Some(5)],
        [Some(2), Some(20), None],
        [Some(1), Some(30), Some(7)],
        [None, Some(1), Some(3)],
        [Some(1), Some(10), Some(6)],
        [Some(2), None, Some(8)],
        [None, Some(1), None],
        [Some(2), Some(20), Some(9)],
        [Some(1), None, Some(1)],
        [Some(3), Some(5), None],
        [Some(1), Some(30), None],
    ]
    .iter()
    .map(|row| row.to_vec())
    .collect()
}

const P: usize = 0;
const O: usize = 1;
const X: usize = 2;

fn key(column: usize, ascending: bool, nulls_first: bool) -> Key {
    Key {
        column,
        ascending,
        nulls_first,
    }
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

/// One output cell as text: integers exactly, floats to six places.
fn text(column: &ArrayRef, row: usize) -> Option<String> {
    if column.is_null(row) {
        return None;
    }
    if let Some(values) = column.as_any().downcast_ref::<Int64Array>() {
        return Some(values.value(row).to_string());
    }
    let values = column
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("window outputs are BIGINT or DOUBLE");
    Some(format!("{:.6}", values.value(row)))
}

fn table(batches: &[crate::exec::chunk::ChunkBatchProjection]) -> Vec<Vec<Option<String>>> {
    let mut rows = Vec::new();
    for batch in batches {
        let batch = batch.record_batch();
        for row in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|column| text(column, row))
                    .collect(),
            );
        }
    }
    rows
}

fn int(value: Option<i64>) -> Option<String> {
    value.map(|value| value.to_string())
}

fn float(value: f64) -> Option<String> {
    Some(format!("{value:.6}"))
}

/// The independent oracle: sorted rows, each with one value per call.
fn oracle(rows: &[Row], shape: &Shape) -> Vec<Vec<Option<String>>> {
    let mut sorted = rows.to_vec();
    let partition_key = |column| key(column, true, true);
    sorted.sort_by(|left, right| {
        for &column in &shape.partition {
            let ordering = compare(left[column], right[column], partition_key(column));
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        for key in &shape.order {
            let ordering = compare(left[key.column], right[key.column], *key);
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        Ordering::Equal
    });
    let mut output = sorted
        .iter()
        .map(|row| row.iter().map(|value| int(*value)).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    let mut start = 0;
    while start < sorted.len() {
        let mut end = start + 1;
        while end < sorted.len()
            && shape
                .partition
                .iter()
                .all(|&column| sorted[end][column] == sorted[start][column])
        {
            end += 1;
        }
        let partition = &sorted[start..end];
        for (offset, values) in evaluate_partition(partition, shape).into_iter().enumerate() {
            output[start + offset].extend(values);
        }
        start = end;
    }
    output
}

/// Every call's value for every row of one sorted partition.
fn evaluate_partition(partition: &[Row], shape: &Shape) -> Vec<Vec<Option<String>>> {
    let n = partition.len();
    let peers = |left: usize, right: usize| {
        shape
            .order
            .iter()
            .all(|key| partition[left][key.column] == partition[right][key.column])
    };
    let peer_start = |row: usize| {
        (0..=row)
            .rev()
            .take_while(|&r| peers(r, row))
            .last()
            .unwrap()
    };
    let peer_end = |row: usize| (row..n).take_while(|&r| peers(r, row)).last().unwrap() + 1;
    let mut values = vec![Vec::new(); n];
    for call in &shape.calls {
        for (row, output) in values.iter_mut().enumerate() {
            // The rows of this row's frame, by definition.
            let frame = |frame: Option<super::window_fixture::Frame>| -> Vec<usize> {
                let Some(frame) = frame else {
                    return if shape.order.is_empty() {
                        (0..n).collect()
                    } else {
                        (0..peer_end(row)).collect()
                    };
                };
                let range = frame.units == WindowFrameUnits::Range;
                let start = match frame.start {
                    WindowBound::UnboundedPreceding => 0,
                    WindowBound::CurrentRow if range => peer_start(row) as i64,
                    WindowBound::CurrentRow => row as i64,
                    WindowBound::Preceding(n) => row as i64 - n as i64,
                    WindowBound::Following(n) => row as i64 + n as i64,
                    WindowBound::UnboundedFollowing => unreachable!(),
                };
                let last = match frame.end {
                    WindowBound::UnboundedFollowing => n as i64 - 1,
                    WindowBound::CurrentRow if range => peer_end(row) as i64 - 1,
                    WindowBound::CurrentRow => row as i64,
                    WindowBound::Preceding(n) => row as i64 - n as i64,
                    WindowBound::Following(n) => row as i64 + n as i64,
                    WindowBound::UnboundedPreceding => unreachable!(),
                };
                (start.max(0)..=last.min(n as i64 - 1))
                    .map(|row| row as usize)
                    .collect()
            };
            let column = |arg: &Arg| match arg {
                Arg::Column(column) => *column,
                _ => panic!("the oracle reads plain columns"),
            };
            let constant = |arg: &Arg| match arg {
                Arg::Constant(value) => *value,
                _ => panic!("a column is no constant"),
            };
            let value = match call.name {
                "row_number" => int(Some(row as i64 + 1)),
                "rank" => int(Some(peer_start(row) as i64 + 1)),
                "dense_rank" => {
                    let groups = (1..=peer_start(row)).filter(|&r| !peers(r - 1, r)).count();
                    int(Some(groups as i64 + 1))
                }
                "cume_dist" => float(peer_end(row) as f64 / n as f64),
                "percent_rank" if n > 1 => float(peer_start(row) as f64 / (n - 1) as f64),
                "percent_rank" => float(0.0),
                "first_value" | "last_value" => {
                    let source = column(&call.args[0]);
                    let mut members = frame(call.frame)
                        .into_iter()
                        .map(|member| partition[member][source])
                        .filter(|value| !call.ignore_nulls || value.is_some())
                        .collect::<Vec<_>>();
                    if call.name == "last_value" {
                        members.reverse();
                    }
                    int(members.first().copied().flatten())
                }
                "ntile" => {
                    let buckets = constant(&call.args[0]);
                    let small = n as i64 / buckets;
                    let large = small + 1;
                    let large_rows = (n as i64 % buckets) * large;
                    let row = row as i64;
                    int(Some(if row < large_rows {
                        row / large + 1
                    } else {
                        n as i64 % buckets + (row - large_rows) / small + 1
                    }))
                }
                "lead" | "lag" => {
                    let source = column(&call.args[0]);
                    let offset = call.args.get(1).map_or(1, constant);
                    let target = if call.name == "lead" {
                        row as i64 + offset
                    } else {
                        row as i64 - offset
                    };
                    if (0..n as i64).contains(&target) {
                        int(partition[target as usize][source])
                    } else {
                        int(call.args.get(2).map(constant))
                    }
                }
                "sum" | "min" | "max" => {
                    let source = column(&call.args[0]);
                    let members = frame(call.frame)
                        .into_iter()
                        .filter_map(|member| partition[member][source])
                        .collect::<Vec<_>>();
                    int(match call.name {
                        "sum" => (!members.is_empty()).then(|| {
                            let sum: i128 = members.iter().map(|&value| i128::from(value)).sum();
                            i64::try_from(sum).expect("oracle sums fit BIGINT")
                        }),
                        "min" => members.iter().min().copied(),
                        _ => members.iter().max().copied(),
                    })
                }
                "count" => {
                    let members = frame(call.frame);
                    let count = match call.args.first() {
                        None => members.len(),
                        Some(arg) => members
                            .iter()
                            .filter(|&&member| partition[member][column(arg)].is_some())
                            .count(),
                    };
                    int(Some(count as i64))
                }
                other => panic!("no oracle for {other}"),
            };
            output.push(value);
        }
    }
    values
}

fn run(program: &Arc<LocalProgram>) -> Vec<crate::exec::chunk::ChunkBatchProjection> {
    try_run(program)
        .unwrap_or_else(|error| panic!("compiled window runs: {error}"))
        .into_iter()
        .map(Chunk::into_batch_projection)
        .collect()
}

/// Compile and run `shape` over `rows` at every DOP, against the oracle.
fn check(rows: &[Row], shape: &Shape) {
    let catalog = installed_catalog();
    let expected = oracle(rows, shape);
    for dop in [1, 4] {
        let program = compile(package(rows, 3, shape, &catalog, 4), &catalog, dop);
        assert_eq!(table(&run(&program)), expected, "DOP {dop}");
    }
}

fn rows_frame(
    start: WindowBound<u64>,
    end: WindowBound<u64>,
) -> (WindowFrameUnits, WindowBound<u64>, WindowBound<u64>) {
    (WindowFrameUnits::Rows, start, end)
}

#[test]
fn ranking_functions_number_ties_as_peers_with_either_null_placement() {
    let calls = [
        "row_number",
        "rank",
        "dense_rank",
        "cume_dist",
        "percent_rank",
    ]
    .into_iter()
    .map(|name| Call::window(name, &[]))
    .collect::<Vec<_>>();
    for order in [key(O, true, false), key(O, false, true)] {
        check(
            &rows(),
            &Shape {
                partition: vec![P],
                order: vec![order],
                calls: calls.clone(),
            },
        );
    }
    // Without PARTITION BY the globally sorted input is one partition.
    check(
        &rows(),
        &Shape {
            partition: vec![],
            order: vec![key(O, true, true), key(X, false, false)],
            calls,
        },
    );
}

#[test]
fn value_and_count_calls_each_keep_their_own_frame() {
    use WindowBound::{CurrentRow, Following, Preceding, UnboundedFollowing, UnboundedPreceding};
    let framed = |call: Call, (units, start, end)| call.framed(units, start, end);
    let x = [Arg::Column(X)];
    let calls = vec![
        // The default frame with ORDER BY extends over the current row's peers.
        Call::window("first_value", &x),
        Call::window("last_value", &x),
        Call::aggregate("count", &[]),
        Call::aggregate("count", &x),
        framed(
            Call::window("first_value", &x),
            rows_frame(Preceding(1), Following(1)),
        ),
        framed(
            Call::window("last_value", &x),
            rows_frame(Preceding(2), Preceding(1)),
        ),
        framed(
            Call::window("first_value", &x).ignoring_nulls(),
            rows_frame(UnboundedPreceding, CurrentRow),
        ),
        framed(
            Call::window("last_value", &x).ignoring_nulls(),
            rows_frame(CurrentRow, Following(2)),
        ),
        framed(
            Call::aggregate("count", &[]),
            (WindowFrameUnits::Range, CurrentRow, UnboundedFollowing),
        ),
        framed(
            Call::window("last_value", &x),
            (WindowFrameUnits::Range, CurrentRow, UnboundedFollowing),
        ),
        framed(
            Call::aggregate("count", &x),
            (WindowFrameUnits::Range, CurrentRow, CurrentRow),
        ),
        framed(
            Call::aggregate("count", &[]),
            rows_frame(Following(1), Following(3)),
        ),
    ];
    check(
        &rows(),
        &Shape {
            partition: vec![P],
            order: vec![key(O, true, false)],
            calls: calls.clone(),
        },
    );
    // Without ORDER BY the default frame is the whole partition.
    check(
        &rows(),
        &Shape {
            partition: vec![P],
            order: vec![],
            calls: vec![
                Call::window("first_value", &x),
                Call::window("last_value", &x),
                Call::aggregate("count", &x),
            ],
        },
    );
}

#[test]
fn range_current_row_to_unbounded_following_starts_at_the_first_peer() {
    use WindowBound::{CurrentRow, UnboundedFollowing};
    // One partition ordered by `o`: [10, 10, 30, 30, NULL] (NULLS LAST).
    let rows = vec![
        vec![Some(1), Some(30), Some(1)],
        vec![Some(1), Some(10), Some(2)],
        vec![Some(1), None, Some(3)],
        vec![Some(1), Some(10), Some(4)],
        vec![Some(1), Some(30), Some(5)],
    ];
    let shape = Shape {
        partition: vec![P],
        order: vec![key(O, true, false)],
        calls: vec![
            Call::aggregate("count", &[]).framed(
                WindowFrameUnits::Range,
                CurrentRow,
                UnboundedFollowing,
            ),
            Call::window("first_value", &[Arg::Column(X)]).framed(
                WindowFrameUnits::Range,
                CurrentRow,
                UnboundedFollowing,
            ),
        ],
    };
    let catalog = installed_catalog();
    let program = compile(package(&rows, 3, &shape, &catalog, 1), &catalog, 1);
    let actual = table(&run(&program))
        .into_iter()
        .map(|row| (row[1].clone(), row[3].clone(), row[4].clone()))
        .collect::<Vec<_>>();
    let s = |value: &str| Some(value.to_string());
    // Each row's frame starts at its first peer, never at the current row
    // and never at the partition start.
    assert_eq!(
        actual,
        vec![
            (s("10"), s("5"), s("2")),
            (s("10"), s("5"), s("2")),
            (s("30"), s("3"), s("1")),
            (s("30"), s("3"), s("1")),
            (None, s("1"), s("3")),
        ]
    );
    assert_eq!(table(&run(&program)), oracle(&rows, &shape));
}

#[test]
fn lead_lag_and_ntile_read_their_checked_constants() {
    let x = Arg::Column(X);
    check(
        &rows(),
        &Shape {
            partition: vec![P],
            order: vec![key(O, true, true), key(X, true, true)],
            calls: vec![
                Call::window("lead", &[x]),
                Call::window("lead", &[x, Arg::Constant(2)]),
                Call::window("lead", &[x, Arg::Constant(1), Arg::Constant(-1)]),
                Call::window("lag", &[x, Arg::Constant(1), Arg::Constant(0)]),
                Call::window("lag", &[x, Arg::Constant(3)]),
                Call::window("ntile", &[Arg::Constant(3)]),
                Call::window("ntile", &[Arg::Constant(1)]),
            ],
        },
    );
}

#[test]
fn window_without_keys_is_one_partition_in_arrival_order() {
    let x = Arg::Column(X);
    check(
        &rows(),
        &Shape {
            partition: vec![],
            order: vec![],
            calls: vec![
                Call::window("row_number", &[]),
                Call::window("rank", &[]),
                Call::aggregate("count", &[x]),
                Call::window("first_value", &[x]),
                Call::window("last_value", &[x]).framed(
                    WindowFrameUnits::Rows,
                    WindowBound::UnboundedPreceding,
                    WindowBound::CurrentRow,
                ),
                Call::window("lag", &[x]),
            ],
        },
    );
}

/// The program's one Analytic node and the layout of its input.
fn analytic(program: &LocalProgram) -> (ProgramNodeId, Arc<arrow::datatypes::Schema>) {
    let nodes = program.graph().nodes();
    let (index, node) = nodes
        .iter()
        .enumerate()
        .find(|(_, node)| matches!(node.kind(), ProgramNodeKind::Analytic { .. }))
        .expect("one Analytic node");
    let ProgramNodeKind::Analytic { input, .. } = node.kind() else {
        unreachable!()
    };
    (
        ProgramNodeId::new(index),
        Arc::clone(nodes[input.index()].output_layout().schema()),
    )
}

/// Push `batch` into a fresh processor of the program's Analytic node in
/// slices of `step` rows, then finish it; return every emitted batch.
fn drive(
    program: &Arc<LocalProgram>,
    batch: &RecordBatch,
    step: usize,
) -> Vec<crate::exec::chunk::ChunkBatchProjection> {
    let (node, _) = analytic(program);
    let input = &program.graph().nodes()[match program.graph().nodes()[node.index()].kind() {
        ProgramNodeKind::Analytic { input, .. } => input.index(),
        _ => unreachable!(),
    }];
    let schema = ChunkSchema::from_compiled_layout(input.output_layout()).unwrap();
    let factory = CompiledWindowProcessorFactory::try_new(
        Arc::clone(program),
        node,
        Arc::new(RuntimeErrorState::default()),
    )
    .unwrap();
    let mut operator = factory.create(1, 0);
    let tracker = crate::runtime::mem_tracker::MemTracker::new_root("window oracle construction");
    tracker.install_limit_once(64 * 1024 * 1024).unwrap();
    operator.set_construction_factory(super::aggregate_fixture::runtime_construction_factory(
        tracker,
        Arc::new(RuntimeErrorState::default()),
    ));
    operator.prepare().unwrap();
    let state = RuntimeState::default();
    let mut processor = TestDriver::new(operator.as_mut(), &state);
    let mut output = Vec::new();
    let mut offset = 0;
    while offset < batch.num_rows() {
        let len = step.min(batch.num_rows() - offset);
        processor
            .accept_without_output(
                Chunk::try_new_with_chunk_schema(batch.slice(offset, len), Arc::clone(&schema))
                    .unwrap(),
            )
            .unwrap();
        while let Some(chunk) = processor
            .advance_to_boundary()
            .map(|step| step.output)
            .unwrap()
        {
            output.push(chunk.into_batch_projection());
        }
        offset += len;
    }
    processor.signal_eos().unwrap();
    while let Some(chunk) = processor
        .advance_to_boundary()
        .map(|step| step.output)
        .unwrap()
    {
        output.push(chunk.into_batch_projection());
    }
    assert!(matches!(processor.next(), NextStep::Finished));
    output
}

/// The sorted input batch of the oracle's row order in `schema`.
fn sorted_input(rows: &[Row], shape: &Shape, schema: Arc<arrow::datatypes::Schema>) -> RecordBatch {
    let sorted = oracle(
        rows,
        &Shape {
            calls: vec![],
            ..shape.clone()
        },
    );
    let columns = (0..3)
        .map(|column| {
            Arc::new(Int64Array::from(
                sorted
                    .iter()
                    .map(|row| {
                        row[column]
                            .as_ref()
                            .map(|value| value.parse::<i64>().unwrap())
                    })
                    .collect::<Vec<_>>(),
            )) as ArrayRef
        })
        .collect::<Vec<_>>();
    RecordBatch::try_new(schema, columns).unwrap()
}

#[test]
fn partitions_and_peer_groups_spanning_chunks_match_one_whole_chunk() {
    use WindowBound::{CurrentRow, Following, Preceding};
    let x = Arg::Column(X);
    let shape = Shape {
        partition: vec![P],
        order: vec![key(O, true, false)],
        calls: vec![
            Call::window("row_number", &[]),
            Call::window("rank", &[]),
            Call::window("cume_dist", &[]),
            Call::aggregate("count", &[]),
            Call::window("first_value", &[x]).framed(
                WindowFrameUnits::Rows,
                Preceding(1),
                Following(1),
            ),
            Call::window("lead", &[x]),
            Call::window("last_value", &[x]).framed(
                WindowFrameUnits::Range,
                CurrentRow,
                CurrentRow,
            ),
        ],
    };
    let catalog = installed_catalog();
    let program = compile(package(&rows(), 3, &shape, &catalog, 1), &catalog, 1);
    let (_, schema) = analytic(&program);
    let batch = sorted_input(&rows(), &shape, schema);
    let expected = oracle(&rows(), &shape);
    for step in [1, 2, 3, 5, batch.num_rows()] {
        let output = drive(&program, &batch, step);
        assert_eq!(table(&output), expected, "{step}-row chunks");
        // One arrival emits at most one chunk, in partition order.
        assert!(output.len() <= batch.num_rows().div_ceil(step) + 1);
    }
}

#[test]
fn empty_input_emits_no_rows() {
    let shape = Shape {
        partition: vec![P],
        order: vec![key(O, true, false)],
        calls: vec![
            Call::window("row_number", &[]),
            Call::aggregate("count", &[]),
        ],
    };
    let catalog = installed_catalog();
    let program = compile(package(&rows(), 3, &shape, &catalog, 1), &catalog, 1);
    let (_, schema) = analytic(&program);
    let empty = RecordBatch::new_empty(schema);
    assert!(drive(&program, &empty, 1).is_empty());
}

#[test]
fn aggregate_over_without_a_window_kernel_is_refused_before_running() {
    use novarocks_functions::{FunctionKind, PureKernelAbi};
    use novarocks_local_compiler::{
        FragmentCompileError, LocalCompileOptions, compile_fragment, validate_fragment_providers,
    };

    use super::family_fixture::{FixtureControl, constant_policy};
    use super::window_fixture::{WINDOW, window_catalog};

    // Install the real aggregate-only owner. Its legitimate metadata and
    // effects reach the missing window ABI boundary instead of failing an
    // earlier unknown-function lookup.
    let catalog = window_catalog(&[("any_value", FunctionKind::Aggregate)]);
    let declaration = catalog
        .metadata()
        .definition("any_value", FunctionKind::Aggregate)
        .unwrap()
        .binding_declaration()
        .unwrap();
    let installed = catalog
        .metadata()
        .pure_overload_declaration_observed(
            declaration.function_id(),
            FunctionKind::Aggregate,
            &declaration.overloads()[0].identity,
            &FixtureControl,
        )
        .unwrap();
    assert_eq!(installed.implementation().abi, PureKernelAbi::AggregateV1);
    let shape = Shape {
        partition: vec![P],
        order: vec![key(O, true, false)],
        calls: vec![Call::aggregate("any_value", &[Arg::Column(X)])],
    };
    let providers =
        novarocks_connector_contract::PureProviderProgramCatalog::<std::io::Error>::try_new(
            &[],
            vec![],
            &FixtureControl,
        )
        .unwrap();
    let validated = validate_fragment_providers(
        package(&rows(), 3, &shape, &catalog, 1),
        &providers,
        &FixtureControl,
    )
    .unwrap();
    let error = compile_fragment(
        validated,
        &catalog,
        LocalCompileOptions {
            pipeline_dop: std::num::NonZeroUsize::MIN,
            root_sink_dop: Some(std::num::NonZeroUsize::MIN),
            kernel_abi: novarocks_local_program::KernelAbiVersion::CURRENT,
            constants: constant_policy(),
        },
        &FixtureControl,
    )
    .unwrap_err();
    assert!(
        matches!(
            error,
            FragmentCompileError::Unsupported {
                node: Some(node),
                feature: "aggregate OVER without an installed pure window kernel",
            } if node == WINDOW
        ),
        "{error}"
    );
}

#[test]
fn a_row_error_of_an_argument_root_fails_the_query() {
    // 1 * i64::MAX fits; 5 * i64::MAX overflows under ALLOW_THROW. A NULL
    // argument row raises nothing, so the error comes from a value row.
    let shape = Shape {
        partition: vec![P],
        order: vec![key(O, true, false)],
        calls: vec![Call::window("first_value", &[Arg::Overflowing(X)])],
    };
    let catalog = installed_catalog();
    for dop in [1, 4] {
        let program = compile(package(&rows(), 3, &shape, &catalog, 4), &catalog, dop);
        let error = try_run(&program).unwrap_err();
        assert!(error.to_lowercase().contains("overflow"), "{error}");
    }
    // Only NULL and fitting values: the same root runs to completion.
    let fitting = vec![
        vec![Some(1), Some(1), Some(1)],
        vec![Some(1), Some(2), None],
        vec![Some(2), Some(1), Some(-1)],
    ];
    let program = compile(package(&fitting, 3, &shape, &catalog, 1), &catalog, 1);
    let max = Some(i64::MAX.to_string());
    let output = table(&run(&program))
        .into_iter()
        .map(|row| row[3].clone())
        .collect::<Vec<_>>();
    assert_eq!(
        output,
        vec![max.clone(), max, Some((-i64::MAX).to_string())]
    );
}

/// `rows` rows over four partitions (one of them NULL), order keys with
/// ties and NULLs, and values with NULLs; arrival order is scrambled.
fn generated_rows(rows: usize) -> Vec<Row> {
    let mut state = 0x9e37_79b9_7f4a_7c15u64;
    let mut next = |bound: u64| {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (state >> 33) % bound
    };
    (0..rows)
        .map(|_| {
            let partition = next(4);
            let order = next(9);
            let value = next(201) as i64 - 100;
            vec![
                (partition != 3).then_some(partition as i64),
                (order != 8).then_some(order as i64),
                (next(5) != 0).then_some(value),
            ]
        })
        .collect()
}

fn aggregate_over_calls() -> Vec<Call> {
    use WindowBound::{CurrentRow, Following, Preceding, UnboundedFollowing, UnboundedPreceding};
    let x = [Arg::Column(X)];
    vec![
        // The default frame with ORDER BY extends over the current row's peers.
        Call::aggregate("sum", &x),
        Call::aggregate("min", &x),
        Call::aggregate("max", &x),
        // Running frames.
        Call::aggregate("sum", &x).framed(WindowFrameUnits::Rows, UnboundedPreceding, CurrentRow),
        Call::aggregate("max", &x).framed(WindowFrameUnits::Rows, UnboundedPreceding, CurrentRow),
        // Sliding frames, refolded per row.
        Call::aggregate("sum", &x).framed(WindowFrameUnits::Rows, Preceding(2), Following(2)),
        Call::aggregate("min", &x).framed(WindowFrameUnits::Rows, Preceding(2), Following(2)),
        Call::aggregate("max", &x).framed(WindowFrameUnits::Rows, Preceding(2), Following(2)),
        // Frames that end empty or start empty.
        Call::aggregate("sum", &x).framed(WindowFrameUnits::Rows, Following(1), Following(3)),
        Call::aggregate("min", &x).framed(WindowFrameUnits::Rows, UnboundedPreceding, Preceding(1)),
        Call::aggregate("sum", &x).framed(WindowFrameUnits::Range, CurrentRow, UnboundedFollowing),
        Call::aggregate("max", &x).framed(WindowFrameUnits::Range, CurrentRow, CurrentRow),
    ]
}

#[test]
fn sum_min_max_over_running_sliding_and_peer_frames_match_the_oracle() {
    for rows in [rows(), generated_rows(157)] {
        check(
            &rows,
            &Shape {
                partition: vec![P],
                order: vec![key(O, true, false)],
                calls: aggregate_over_calls(),
            },
        );
        // Descending NULLS FIRST order keys and a second key.
        check(
            &rows,
            &Shape {
                partition: vec![P],
                order: vec![key(O, false, true), key(X, true, true)],
                calls: aggregate_over_calls(),
            },
        );
        // Without ORDER BY the default frame is the whole partition.
        let x = [Arg::Column(X)];
        check(
            &rows,
            &Shape {
                partition: vec![P],
                order: vec![],
                calls: vec![
                    Call::aggregate("sum", &x),
                    Call::aggregate("min", &x),
                    Call::aggregate("max", &x),
                ],
            },
        );
    }
}

#[test]
fn sum_min_max_over_partitions_spanning_chunks_match_one_whole_chunk() {
    let rows = generated_rows(61);
    let shape = Shape {
        partition: vec![P],
        order: vec![key(O, true, false)],
        calls: aggregate_over_calls(),
    };
    let catalog = installed_catalog();
    let program = compile(package(&rows, 3, &shape, &catalog, 1), &catalog, 1);
    let (_, schema) = analytic(&program);
    let batch = sorted_input(&rows, &shape, schema);
    let expected = oracle(&rows, &shape);
    for step in [1, 2, 3, 7, batch.num_rows()] {
        assert_eq!(
            table(&drive(&program, &batch, step)),
            expected,
            "{step}-row chunks"
        );
    }
}

#[test]
fn sum_over_fails_the_query_only_for_a_frame_whose_result_overflows_bigint() {
    use WindowBound::{CurrentRow, UnboundedPreceding};
    let catalog = installed_catalog();
    let x = [Arg::Column(X)];
    // ROWS UNBOUNDED PRECEDING: the frame of the second row holds MAX + 1.
    let distinct_order = vec![
        vec![Some(1), Some(1), Some(i64::MAX)],
        vec![Some(1), Some(2), Some(1)],
        vec![Some(1), Some(3), Some(-1)],
    ];
    let running = Shape {
        partition: vec![P],
        order: vec![key(O, true, false)],
        calls: vec![Call::aggregate("sum", &x).framed(
            WindowFrameUnits::Rows,
            UnboundedPreceding,
            CurrentRow,
        )],
    };
    for dop in [1, 4] {
        let program = compile(
            package(&distinct_order, 3, &running, &catalog, 4),
            &catalog,
            dop,
        );
        let error = try_run(&program).unwrap_err();
        assert!(error.contains("sum result overflows BIGINT"), "{error}");
    }
    // The same rows one at a time: no frame overflows.
    let current = Shape {
        calls: vec![Call::aggregate("sum", &x).framed(
            WindowFrameUnits::Rows,
            CurrentRow,
            CurrentRow,
        )],
        ..running.clone()
    };
    check(&distinct_order, &current);
    // The default frame over peers {1} and {2, 2}: the exact running state
    // passes MAX + 1 between two frames, and neither frame overflows.
    let peers = vec![
        vec![Some(1), Some(1), Some(i64::MAX)],
        vec![Some(1), Some(2), Some(1)],
        vec![Some(1), Some(2), Some(-1)],
    ];
    let default = Shape {
        calls: vec![Call::aggregate("sum", &x)],
        ..running
    };
    for dop in [1, 4] {
        let program = compile(package(&peers, 3, &default, &catalog, 4), &catalog, dop);
        let sums = table(&run(&program))
            .into_iter()
            .map(|row| row[3].clone())
            .collect::<Vec<_>>();
        assert_eq!(sums, vec![Some(i64::MAX.to_string()); 3], "DOP {dop}");
    }
}

#[path = "compiled_window_crosscall_order_probe.rs"]
mod compiled_window_crosscall_order_probe;

#[path = "compiled_window_tracked_by_tests.rs"]
mod compiled_window_tracked_by_tests;
#[path = "compiled_window_whole_invocation_order_tests.rs"]
mod compiled_window_whole_invocation_order_tests;

#[path = "compiled_window_memory_binding_tests.rs"]
mod memory_binding_tests;
