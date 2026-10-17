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
use super::super::window_fixture::{package_typed, window_catalog};
use super::*;
use crate::runtime::fragment::ExecutionFailureCause;
use crate::runtime::mem_tracker::MemTracker;
use arrow::datatypes::DataType;
use novarocks_functions::{FunctionKind, KernelFailure};
use novarocks_physical_plan::LiteralValue;
use novarocks_type_contract::FunctionValueType;
#[test]
fn by_window_compiled_function_major_error_order_across_partitions() {
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
            Call::aggregate("min_by", &[Arg::Column(0), Arg::Column(2)]).framed(
                WindowFrameUnits::Rows,
                UnboundedPreceding,
                CurrentRow,
            ),
        ],
    };
    let types = [
        DataType::Int32,
        DataType::Float64,
        DataType::Float64,
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
                LiteralValue::Int64([10, 11, 20, 21][row]),
                LiteralValue::Float64Bits(key0[row].to_bits()),
                LiteralValue::Float64Bits(key1[row].to_bits()),
                LiteralValue::Int64(if row < 2 { 1 } else { 2 }),
                LiteralValue::Int64(row as i64),
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
    operator.prepare().unwrap();
    let processor = operator.as_processor_mut().unwrap();
    let state = RuntimeState::default();
    let first = match processor.push_chunk(
        &state,
        Chunk::try_new_with_chunk_schema(batch, chunk_schema).unwrap(),
    ) {
        Err(first) => first,
        Ok(()) => processor.set_finishing(&state).unwrap_err(),
    };
    match first.cause() {
        ExecutionFailureCause::WindowInvocationData(data) => {
            assert_eq!(
                data.message(),
                "window function #0: update aggregate state: float comparison is not ordered"
            );
            assert_eq!(data.call_ordinal(), 0);
        }
        ExecutionFailureCause::Kernel(KernelFailure::Operational(error)) => {
            panic!("raw Data must not be bounded/Operational: {error:?}")
        }
        other => panic!("actual original whole-call Data: {other:?}"),
    }
    assert_eq!(processor.pull_chunk(&state).unwrap_err(), first);
    assert_eq!(processor.set_finishing(&state).unwrap_err(), first);
    drop(operator);
    drop(first);
    assert_eq!(tracker.current(), 0);
}
