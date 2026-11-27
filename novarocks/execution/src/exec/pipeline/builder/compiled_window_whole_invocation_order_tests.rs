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

//! Actual compiled window first-Data witness, through the original physical
//! fixture emitter, real binding, LocalCompiler, runtime allocator and driver.
//! This permanent original-equivalence expectation must not be changed to #1.
use super::super::window_fixture::package_typed;
use super::*;
use crate::exec::pipeline::test_driver::TestDriver;
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::task::ExecutionFailureCause;
use arrow::datatypes::DataType;
use novarocks_functions::FunctionKind;

use novarocks_type_contract::{FunctionValueType, OperatorFunction};
fn whole_invocation_probe(step: usize, later_root_error: bool) {
    use WindowBound::{CurrentRow, UnboundedPreceding};
    let shape = Shape {
        partition: vec![3],
        order: vec![key(4, true, true)],
        calls: vec![
            Call::aggregate("max_by", &[Arg::Column(0), Arg::Column(1)]).framed(
                WindowFrameUnits::Rows,
                UnboundedPreceding,
                CurrentRow,
            ),
            Call::aggregate(
                "min_by",
                &[
                    if later_root_error {
                        Arg::Overflowing(5)
                    } else {
                        Arg::Column(5)
                    },
                    Arg::Column(2),
                ],
            )
            .framed(WindowFrameUnits::Rows, UnboundedPreceding, CurrentRow),
        ],
    };
    let types = [
        DataType::Int32,
        DataType::Float64,
        DataType::Float64,
        DataType::Int64,
        DataType::Int64,
        DataType::Int64,
    ]
    .into_iter()
    .map(|dtype| FunctionValueType::new(dtype, true))
    .collect::<Vec<_>>();
    let key0 = [1.0f64, 2.0, 1.0, f64::NAN];
    let key1 = [1.0f64, f64::NAN, 1.0, 2.0];
    let literal_rows = (0..4)
        .map(|row| {
            vec![
                crate::exec::operators::family_fixture::constant_sources::i64(
                    [10, 11, 20, 21][row],
                ),
                crate::exec::operators::family_fixture::constant_sources::f64_bits(
                    key0[row].to_bits(),
                ),
                crate::exec::operators::family_fixture::constant_sources::f64_bits(
                    key1[row].to_bits(),
                ),
                crate::exec::operators::family_fixture::constant_sources::i64(if row < 2 {
                    1
                } else {
                    2
                }),
                crate::exec::operators::family_fixture::constant_sources::i64(row as i64),
                crate::exec::operators::family_fixture::constant_sources::i64(2),
            ]
        })
        .collect::<Vec<_>>();
    let actual =
        novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
    let catalogue =
        crate::exec::pipeline::builder::compiled::window_fixture::window_catalog_from_actual(
            &[
                ("max_by", FunctionKind::Aggregate),
                ("min_by", FunctionKind::Aggregate),
                (
                    OperatorFunction::Multiply.catalog_name(),
                    FunctionKind::Scalar,
                ),
            ],
            &actual,
        );
    let program = compile(
        package_typed(&literal_rows, &types, &shape, &catalogue, 1),
        &catalogue,
        1,
    );
    let (node, schema) = analytic(&program);
    let columns: Vec<ArrayRef> = vec![
        Arc::new(arrow::array::Int32Array::from(vec![10, 11, 20, 21])),
        Arc::new(Float64Array::from(key0.to_vec())),
        Arc::new(Float64Array::from(key1.to_vec())),
        Arc::new(Int64Array::from(vec![1, 1, 2, 2])),
        Arc::new(Int64Array::from(vec![0, 1, 2, 3])),
        Arc::new(Int64Array::from(vec![2, 2, 2, 2])),
    ];
    let batch = RecordBatch::try_new(schema, columns).unwrap();
    let ProgramNodeKind::Analytic { input, .. } = program.graph().nodes()[node.index()].kind()
    else {
        unreachable!()
    };
    let chunk_schema =
        ChunkSchema::from_compiled_layout(program.graph().nodes()[input.index()].output_layout())
            .unwrap();
    let factory = CompiledWindowProcessorFactory::try_new(
        program,
        node,
        Arc::new(RuntimeErrorState::default()),
    )
    .unwrap();
    let mut operator = factory.create(1, 0);
    let tracker = MemTracker::new_root("original function-major BY probe");
    tracker.install_limit_once(64 * 1024 * 1024).unwrap();
    operator.set_mem_tracker(tracker.clone());
    operator.set_construction_factory(
        super::super::aggregate_fixture::runtime_construction_factory(
            tracker.clone(),
            Arc::new(RuntimeErrorState::default()),
        ),
    );
    let state = RuntimeState::default().with_test_query_memory();
    operator.bind_runtime_state(&state).unwrap();
    operator.prepare().unwrap();
    let mut processor = TestDriver::new(operator.as_mut(), &state);
    let mut earlier = None;
    for start in (0..4).step_by(step) {
        let input = Chunk::try_new_with_chunk_schema(
            batch.slice(start, step.min(4 - start)),
            chunk_schema.clone(),
        )
        .unwrap();
        if let Err(cause) = processor.input_without_output(input) {
            earlier = Some(cause);
            break;
        }
    }
    if !later_root_error {
        assert!(earlier.is_none());
        processor.signal_eos().unwrap();
        let output = processor.collect_until_finished().unwrap();
        let mut winners = Vec::new();
        for chunk in &output {
            let values = chunk
                .record_batch()
                .column(6)
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .unwrap();
            winners.extend_from_slice(values.values());
        }
        assert_eq!(winners, [10, 11, 20, 20]);
        assert!(state.error().is_none());
        drop(output);
        drop(processor);
        drop(operator);
        assert_eq!(tracker.current(), 0);
        return;
    }
    let first = earlier.unwrap_or_else(|| {
        processor.signal_eos().unwrap();
        processor.collect_until_finished().unwrap_err()
    });
    match first.cause() {
        ExecutionFailureCause::RequiredRow(row) => {
            assert_eq!(row.batch_row(), 0);
            assert!(
                row.error()
                    .message()
                    .to_string()
                    .contains("Arithmetic overflow")
            );
        }
        other => panic!("expected the original later root failure: {other:?}"),
    }
    assert_eq!(
        processor
            .advance_to_boundary()
            .map(|step| step.output)
            .unwrap_err(),
        first
    );
    assert_eq!(processor.signal_eos().unwrap_err(), first);
    assert_eq!(state.error(), Some(first.clone()));
    drop(processor);
    drop(operator);
    drop(first);
    drop(state);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn by_window_compiled_nan_does_not_mask_later_root_data_for_original_chunk_shapes() {
    for step in [1, 2, 4] {
        whole_invocation_probe(step, true);
    }
}
#[test]
fn by_window_compiled_nan_keeps_winners_across_original_chunk_shapes() {
    for step in [1, 2, 4] {
        whole_invocation_probe(step, false);
    }
}
