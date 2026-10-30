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
//! Actual physical Series roots compiled and executed through the production
//! Values opening Frame and compiled pipeline. The original source gate is
//! the expected pre-install refusal; these are permanent after-install probes.

use super::family_fixture::{
    FixtureControl, cell, compile, constant_policy, int64_rows, package, run, try_run,
};
use crate::exec::pipeline::test_driver::TestDriver;
use arrow::array::{Array, ArrayRef, Int64Array};
use arrow::datatypes::DataType;
use novarocks_functions::{ConstantPool, largeint};
use novarocks_local_program::{
    LocalProgram, ProgramExpressionRootSite, ProgramNodeExpressionRole, ProgramNodeId,
    ProgramNodeKind,
};
use novarocks_physical_plan::{
    ConstantPoolId, ConstantPools, ConstantReference, ExprKind, FragmentBuilder, FragmentId,
    FragmentPackage, NodeId, ValueOrigin,
};
use novarocks_type_contract::{CompilePhase, FunctionValueType, ValueLogicalType};
use std::sync::Arc;

pub(super) const SOURCE: NodeId = NodeId::new(0xf1234567);

pub(super) fn literal_source(
    values: [Option<i64>; 3],
    explicit_step: bool,
    nullable: bool,
) -> Arc<FragmentPackage> {
    let mut builder = FragmentBuilder::new(FragmentId::new(61));
    let ty = FunctionValueType::new(DataType::Int64, nullable);
    let output = builder
        .add_value(
            ty.clone(),
            ValueOrigin::NodeOutput {
                node: SOURCE,
                output_ordinal: 0,
            },
        )
        .unwrap();
    let start = builder
        .add_expression(SOURCE, ty.clone(), ExprKind::Literal(cell(values[0])))
        .unwrap();
    let stop = builder
        .add_expression(SOURCE, ty.clone(), ExprKind::Literal(cell(values[1])))
        .unwrap();
    let step = explicit_step.then(|| {
        builder
            .add_expression(SOURCE, ty, ExprKind::Literal(cell(values[2])))
            .unwrap()
    });
    builder
        .add_generate_series(SOURCE, start, stop, step, output)
        .unwrap();
    package(builder, SOURCE, ConstantPools::empty(), 8)
}

pub(super) fn pooled_source(
    ty: FunctionValueType,
    values: &[Option<i128>],
    explicit_step: bool,
) -> Arc<FragmentPackage> {
    assert_eq!(values.len(), 3);
    macro_rules! ints {
        ($array:ty,$int:ty) => {
            Arc::new(<$array>::from(
                values
                    .iter()
                    .map(|value| value.map(|value| <$int>::try_from(value).unwrap()))
                    .collect::<Vec<_>>(),
            )) as ArrayRef
        };
    }
    let array = match ty.data_type() {
        DataType::Int8 => ints!(arrow::array::Int8Array, i8),
        DataType::Int16 => ints!(arrow::array::Int16Array, i16),
        DataType::Int32 => ints!(arrow::array::Int32Array, i32),
        DataType::Int64 => Arc::new(Int64Array::new(
            arrow::buffer::ScalarBuffer::from(
                values
                    .iter()
                    .map(|value| {
                        value
                            .map(|value| i64::try_from(value).unwrap())
                            .unwrap_or(i64::MAX)
                    })
                    .collect::<Vec<_>>(),
            ),
            Some(arrow::buffer::NullBuffer::from(
                values.iter().map(Option::is_some).collect::<Vec<_>>(),
            )),
        )) as ArrayRef,
        DataType::UInt8 => ints!(arrow::array::UInt8Array, u8),
        DataType::UInt16 => ints!(arrow::array::UInt16Array, u16),
        DataType::UInt32 => ints!(arrow::array::UInt32Array, u32),
        DataType::UInt64 => ints!(arrow::array::UInt64Array, u64),
        DataType::FixedSizeBinary(16) => largeint::array_from_i128(values).unwrap(),
        _ => panic!("fixture has no original integer carrier"),
    };
    let pool = ConstantPool::try_new(
        Arc::new(ty.try_to_field("source-pool").unwrap()),
        ty.clone(),
        array.to_data(),
        constant_policy(),
        CompilePhase::Validate,
        &FixtureControl,
    )
    .unwrap();
    let id = ConstantPoolId::new(0xf2345678);
    let mut constants = ConstantPools::empty();
    constants.insert(id, pool).unwrap();
    let mut builder = FragmentBuilder::new(FragmentId::new(62));
    let output = builder
        .add_value(
            ty.clone(),
            ValueOrigin::NodeOutput {
                node: SOURCE,
                output_ordinal: 0,
            },
        )
        .unwrap();
    // Start and stop consume nonzero pool ordinals; ordinal zero is the step.
    let start = builder
        .add_expression(
            SOURCE,
            ty.clone(),
            ExprKind::Constant(ConstantReference {
                pool: id,
                ordinal: 1,
            }),
        )
        .unwrap();
    let stop = builder
        .add_expression(
            SOURCE,
            ty.clone(),
            ExprKind::Constant(ConstantReference {
                pool: id,
                ordinal: 2,
            }),
        )
        .unwrap();
    let step = explicit_step.then(|| {
        builder
            .add_expression(
                SOURCE,
                ty,
                ExprKind::Constant(ConstantReference {
                    pool: id,
                    ordinal: 0,
                }),
            )
            .unwrap()
    });
    builder
        .add_generate_series(SOURCE, start, stop, step, output)
        .unwrap();
    package(builder, SOURCE, constants, 8)
}

// Pure expected-data construction: callers supply fixed values/ranges. This
// helper only packs those values in the declared fixture's Arrow carrier.
fn integer_array(data_type: &DataType, values: &[Option<i128>]) -> ArrayRef {
    macro_rules! ints {
        ($array:ty, $int:ty) => {
            Arc::new(<$array>::from(
                values
                    .iter()
                    .map(|value| value.map(|value| <$int>::try_from(value).unwrap()))
                    .collect::<Vec<_>>(),
            )) as ArrayRef
        };
    }
    match data_type {
        DataType::Int8 => ints!(arrow::array::Int8Array, i8),
        DataType::Int16 => ints!(arrow::array::Int16Array, i16),
        DataType::Int32 => ints!(arrow::array::Int32Array, i32),
        DataType::Int64 => ints!(arrow::array::Int64Array, i64),
        DataType::FixedSizeBinary(16) => largeint::array_from_i128(values).unwrap(),
        other => panic!("expected fixture has no output carrier {other:?}"),
    }
}

// Read only an observed Arrow scalar; no series core or value calculator is
// used to produce expected bounds. NULL masks its original hidden payload.
fn observed_integer(array: &ArrayRef, row: usize) -> Option<i128> {
    if array.is_null(row) {
        return None;
    }
    macro_rules! read {
        ($array:ty) => {
            Some(i128::from(
                array.as_any().downcast_ref::<$array>().unwrap().value(row),
            ))
        };
    }
    match array.data_type() {
        DataType::Int8 => read!(arrow::array::Int8Array),
        DataType::Int16 => read!(arrow::array::Int16Array),
        DataType::Int32 => read!(arrow::array::Int32Array),
        DataType::Int64 => read!(arrow::array::Int64Array),
        DataType::UInt8 => read!(arrow::array::UInt8Array),
        DataType::UInt16 => read!(arrow::array::UInt16Array),
        DataType::UInt32 => read!(arrow::array::UInt32Array),
        DataType::UInt64 => read!(arrow::array::UInt64Array),
        DataType::FixedSizeBinary(16) => {
            let array = array
                .as_any()
                .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
                .unwrap();
            Some(i128::from_be_bytes(array.value(row).try_into().unwrap()))
        }
        other => panic!("observed fixture has no input carrier {other:?}"),
    }
}

/// Keep the actual compiled Values/Frame opening probe separately from value
/// expectations: exactly one bound row is emitted and never replayed.
fn assert_opened_bounds_once(program: &Arc<LocalProgram>, expected: &[Option<i128>]) {
    use crate::exec::pipeline::operator_factory::OperatorFactory;
    use crate::runtime::runtime_state::{RuntimeErrorState, RuntimeState};
    let nodes = program.graph().nodes();
    let input = nodes[0].output_layout();
    let mut source = super::values_source::CompiledValuesSourceFactory::try_new(
        Arc::clone(program),
        ProgramNodeId::new(0),
        Arc::new(RuntimeErrorState::default()),
    )
    .unwrap()
    .create(8, 0);
    let state = RuntimeState::default();
    let tracker =
        crate::runtime::mem_tracker::MemTracker::new_root("series compiled bounds creator");
    source.set_mem_tracker(tracker.clone());
    source.set_construction_factory(super::aggregate_fixture::runtime_construction_factory(
        tracker,
        Arc::new(RuntimeErrorState::default()),
    ));
    source.prepare().unwrap();
    source.bind_runtime_state(&state).unwrap();
    let chunk = TestDriver::new(source.as_mut(), &state)
        .advance_to_boundary()
        .map(|step| step.output)
        .unwrap()
        .unwrap();
    assert!(
        TestDriver::new(source.as_mut(), &state)
            .advance_to_boundary()
            .map(|step| step.output)
            .unwrap()
            .is_none(),
        "opening evaluates once"
    );
    assert_eq!(chunk.len(), 1);
    assert_eq!(chunk.record_batch().num_columns(), expected.len());
    for (column, expected) in expected.iter().enumerate() {
        let array = chunk.record_batch().column(column);
        assert_eq!(array.data_type(), input.schema().field(column).data_type());
        assert_eq!(observed_integer(array, 0), *expected);
    }
}

fn assert_series_values(
    program: &Arc<LocalProgram>,
    data_type: &DataType,
    expected: &[Option<i128>],
) {
    let chunks = run(program);
    let actual = if chunks.is_empty() {
        arrow::array::new_empty_array(data_type)
    } else {
        for chunk in &chunks {
            assert_eq!(chunk.record_batch().column(0).data_type(), data_type);
        }
        arrow::compute::concat(
            &chunks
                .iter()
                .map(|chunk| chunk.record_batch().column(0).as_ref())
                .collect::<Vec<_>>(),
        )
        .unwrap()
    };
    assert_eq!(
        actual.to_data(),
        integer_array(data_type, expected).to_data()
    );
}

fn roots_preserved(source: &FragmentPackage, program: &LocalProgram, width: usize) {
    assert_eq!(program.graph().nodes().len(), 2);
    let bounds = ProgramNodeId::new(0);
    let ProgramNodeKind::Values { values } = program.graph().nodes()[bounds.index()].kind() else {
        panic!("subordinate bounds Values")
    };
    assert_eq!(values.num_rows(), 1);
    assert_eq!(
        values.dynamic_cells().len(),
        width,
        "literals and pool values stay Frame roots"
    );
    assert!(values.batch().is_none());
    let snapshot = program
        .checked()
        .channels()
        .expressions()
        .resolved_calls()
        .snapshot();
    for (site, use_id) in source.expression_uses().bindings() {
        let column = match site.role {
            novarocks_physical_plan::ExpressionRootRole::SeriesStart => 0,
            novarocks_physical_plan::ExpressionRootRole::SeriesStop => 1,
            novarocks_physical_plan::ExpressionRootRole::SeriesStep => 2,
            _ => panic!("only original series roots"),
        };
        assert_eq!(
            snapshot.bindings().get(&ProgramExpressionRootSite::Node {
                node: bounds,
                role: ProgramNodeExpressionRole::ValuesCell { row: 0, column }
            }),
            Some(use_id)
        );
    }
    let root = program.graph().root();
    assert_eq!(root.index(), 1);
    assert_eq!(
        program.graph().nodes()[root.index()].physical_sources(),
        &[novarocks_local_program::DiagnosticSourceNodeId::new(
            SOURCE.get()
        )]
    );
}

#[test]
fn compiled_generate_series_source_literal_bounds_are_original_uses_once_and_public_nonnull() {
    for explicit in [false, true] {
        let source = literal_source([Some(-3), Some(3), Some(1)], explicit, false);
        let program = compile(Arc::clone(&source), 8);
        roots_preserved(&source, &program, if explicit { 3 } else { 2 });
        assert_opened_bounds_once(
            &program,
            if explicit {
                &[Some(-3), Some(3), Some(1)]
            } else {
                &[Some(-3), Some(3)]
            },
        );
        // Default or explicit +1 over the inclusive interval [-3, 3].
        assert_series_values(
            &program,
            &DataType::Int64,
            &(-3_i128..=3).map(Some).collect::<Vec<_>>(),
        );
        let result = run(&program);
        assert_eq!(
            int64_rows(&result),
            (-3..=3).map(|value| vec![Some(value)]).collect::<Vec<_>>()
        );
        for chunk in result {
            assert!(!chunk.schema().field(0).is_nullable());
            assert_eq!(chunk.schema().field(0).name(), "c0");
        }
    }
}
#[test]
fn compiled_generate_series_source_descending_empty_null_and_int64_endpoints() {
    for (values, expected) in [
        ([Some(5), Some(-1), Some(-2)], vec![5, 3, 1, -1]),
        ([Some(5), Some(1), Some(1)], vec![]),
        ([Some(i64::MIN), Some(i64::MIN), Some(1)], vec![i64::MIN]),
        ([Some(i64::MAX), Some(i64::MAX), Some(1)], vec![i64::MAX]),
    ] {
        let result = run(&compile(literal_source(values, true, false), 4));
        assert_eq!(
            int64_rows(&result),
            expected
                .into_iter()
                .map(|value| vec![Some(value)])
                .collect::<Vec<_>>()
        );
    }
    for null in 0..3 {
        let mut values = [Some(1), Some(3), Some(1)];
        values[null] = None;
        assert!(run(&compile(literal_source(values, true, true), 4)).is_empty());
    }
}
#[test]
fn compiled_generate_series_source_pooled_complete_reader_domain_and_original_result_errors() {
    let types = [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
        DataType::FixedSizeBinary(16),
    ];
    for dtype in types {
        for nullable in [false, true] {
            let ty = FunctionValueType::new(dtype.clone(), nullable);
            let source = pooled_source(ty, &[Some(2), Some(1), Some(5)], true);
            let program = compile(Arc::clone(&source), 8);
            roots_preserved(&source, &program, 3);
            assert_opened_bounds_once(&program, &[Some(1), Some(5), Some(2)]);
            match dtype {
                DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                    // All four unsigned carriers are admitted by the bound
                    // reader but are outside the declared result converter.
                    let expected = format!(
                        "table function generate_series return type expects TINYINT/SMALLINT/INT/BIGINT/LARGEINT, got {dtype:?}"
                    );
                    let actual = try_run(&program).unwrap_err();
                    assert!(actual.contains(&expected), "{actual:?} vs {expected:?}");
                }
                _ => {
                    // Inclusive start=1, stop=5, step=2 has exactly [1,3,5].
                    assert_series_values(&program, &dtype, &[Some(1), Some(3), Some(5)]);
                }
            }
        }
    }
    let nominal = FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        false,
        ValueLogicalType::LargeInt,
    )
    .unwrap();
    assert_eq!(
        run(&compile(
            pooled_source(nominal, &[Some(1), Some(-1), Some(1)], true),
            2
        ))
        .iter()
        .map(|c| c.len())
        .sum::<usize>(),
        3
    );
}
#[test]
fn compiled_generate_series_source_original_step_zero_and_cap_are_full_pipeline_data() {
    for (values, message) in [
        (
            [Some(1), Some(3), Some(0)],
            "table function generate_series step size cannot equal zero",
        ),
        (
            [Some(0), Some(i64::from(u32::MAX)), Some(1)],
            "table function output too large",
        ),
    ] {
        let error = try_run(&compile(literal_source(values, true, false), 2)).unwrap_err();
        assert!(error.contains(message), "{error}");
    }
}

#[test]
fn compiled_generate_series_source_complete_signed_endpoints_nonzero_pool_and_hidden_null() {
    for (dtype, min, max) in [
        (DataType::Int8, i128::from(i8::MIN), i128::from(i8::MAX)),
        (DataType::Int16, i128::from(i16::MIN), i128::from(i16::MAX)),
        (DataType::Int32, i128::from(i32::MIN), i128::from(i32::MAX)),
        (DataType::Int64, i128::from(i64::MIN), i128::from(i64::MAX)),
    ] {
        for endpoint in [min, max] {
            for explicit in [false, true] {
                let program = compile(
                    pooled_source(
                        FunctionValueType::new(dtype.clone(), false),
                        &[Some(1), Some(endpoint), Some(endpoint)],
                        explicit,
                    ),
                    8,
                );
                let bounds = if explicit {
                    vec![Some(endpoint), Some(endpoint), Some(1)]
                } else {
                    vec![Some(endpoint), Some(endpoint)]
                };
                assert_opened_bounds_once(&program, &bounds);
                // Equal endpoints yield the singleton without incrementing it.
                assert_series_values(&program, &dtype, &[Some(endpoint)]);
            }
        }
    }
    for null in 0..3 {
        let mut values = [Some(1), Some(1), Some(3)];
        values[null] = None;
        let program = compile(
            pooled_source(FunctionValueType::new(DataType::Int64, true), &values, true),
            8,
        );
        assert_opened_bounds_once(&program, &[values[1], values[2], values[0]]);
        assert!(run(&program).is_empty());
    }
    let ty = FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        false,
        ValueLogicalType::LargeInt,
    )
    .unwrap();
    let program = compile(
        pooled_source(ty, &[Some(1), Some(i128::MAX), Some(i128::MAX)], true),
        2,
    );
    assert_opened_bounds_once(&program, &[Some(i128::MAX), Some(i128::MAX), Some(1)]);
    let actual = run(&program);
    assert_eq!(actual.iter().map(|chunk| chunk.len()).sum::<usize>(), 1);
    let values = actual[0].record_batch();
    let value = values
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(
        largeint::i128_from_be_bytes(value.value(0)).unwrap(),
        i128::MAX
    );
    assert_series_values(&program, &DataType::FixedSizeBinary(16), &[Some(i128::MAX)]);
}

#[test]
fn compiled_generate_series_compile_three_causes_return_at_original_callback_prefix() {
    use novarocks_connector_contract::PureProviderProgramCatalog;
    use novarocks_local_compiler::{
        FragmentCompileError, LocalCompileOptions, compile_fragment, validate_fragment_providers,
    };
    use novarocks_local_program::KernelAbiVersion;
    use novarocks_type_contract::{CompileControlError, PureCompileControl};
    use std::{num::NonZeroUsize, sync::Mutex, time::Duration};
    struct Observe {
        trace: Mutex<Vec<(CompilePhase, u32)>>,
        at: Option<usize>,
        cause: CompileControlError,
    }
    impl PureCompileControl for Observe {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            let mut trace = self.trace.lock().unwrap();
            let at = trace.len();
            trace.push((phase, units));
            if self.at == Some(at) {
                Err(self.cause.clone())
            } else {
                Ok(())
            }
        }
    }
    let source = literal_source([Some(1), Some(7), Some(2)], true, false);
    let functions = crate::exec::expr::compiled_program::tests::rng_subset();
    let providers =
        PureProviderProgramCatalog::<std::io::Error>::try_new(&[], vec![], &FixtureControl)
            .unwrap();
    let options = LocalCompileOptions {
        pipeline_dop: NonZeroUsize::new(8).unwrap(),
        root_sink_dop: Some(NonZeroUsize::new(1).unwrap()),
        kernel_abi: KernelAbiVersion::CURRENT,
        constants: constant_policy(),
        exchange_wait: Duration::from_secs(120),
    };
    let baseline = Observe {
        trace: Mutex::new(vec![]),
        at: None,
        cause: CompileControlError::Cancelled,
    };
    let validated =
        validate_fragment_providers(Arc::clone(&source), &providers, &FixtureControl).unwrap();
    compile_fragment(validated, &functions, options, &baseline).unwrap();
    let trace = baseline.trace.lock().unwrap().clone();
    assert!(!trace.is_empty());
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at in 0..trace.len() {
            let control = Observe {
                trace: Mutex::new(vec![]),
                at: Some(at),
                cause: cause.clone(),
            };
            let validated =
                validate_fragment_providers(Arc::clone(&source), &providers, &FixtureControl)
                    .unwrap();
            let error = compile_fragment(validated, &functions, options, &control).unwrap_err();
            assert!(matches!(error,FragmentCompileError::Control(actual) if actual==cause));
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
        }
    }
}

#[test]
fn compiled_series_schema_and_slice_use_actual_creator_without_retaining_program() {
    use crate::exec::chunk::{Chunk, ChunkSchema};
    use crate::exec::operators::compiled_generate_series::CompiledGenerateSeriesProcessorFactory;
    use crate::exec::pipeline::operator::ConstructionFailure;
    use crate::exec::pipeline::operator_factory::OperatorFactory;
    use crate::runtime::fragment::ExecutionFailureCause;
    use crate::runtime::mem_tracker::MemTracker;
    use crate::runtime::runtime_state::{RuntimeErrorState, RuntimeState};
    let program = compile(literal_source([Some(1), Some(3), Some(1)], true, false), 8);
    let weak = Arc::downgrade(&program);
    let factory = CompiledGenerateSeriesProcessorFactory::try_new(
        Arc::clone(&program),
        program.graph().root(),
        Arc::new(RuntimeErrorState::default()),
    )
    .unwrap();
    let mut missing = factory.create(1, 0);
    assert!(matches!(
        missing.prepare().unwrap_err().cause(),
        ExecutionFailureCause::Construction(ConstructionFailure::MissingOwner)
    ));
    drop(missing);
    let tracker = MemTracker::new_root("series schema and slice creator");
    tracker.install_limit_once(64 * 1024 * 1024).unwrap();
    let mut operator = factory.create(1, 0);
    operator.set_construction_factory(super::aggregate_fixture::runtime_construction_factory(
        tracker.clone(),
        Arc::new(RuntimeErrorState::default()),
    ));
    operator.prepare().unwrap();
    let layout = program.graph().nodes()[0].output_layout();
    let batch = arrow::record_batch::RecordBatch::try_new(
        layout.schema().clone(),
        [1, 3, 1]
            .into_iter()
            .map(|value| Arc::new(Int64Array::from(vec![value])) as ArrayRef)
            .collect(),
    )
    .unwrap();
    let input =
        Chunk::try_new_with_chunk_schema(batch, ChunkSchema::from_compiled_layout(layout).unwrap())
            .unwrap();
    drop(factory);
    drop(program);
    assert!(weak.upgrade().is_none());
    let state = RuntimeState::default();
    operator.bind_runtime_state(&state).unwrap();
    let mut processor = TestDriver::new(operator.as_mut(), &state);
    processor.accept_without_output(input).unwrap();
    let output = processor
        .advance_to_boundary()
        .map(|step| step.output)
        .unwrap()
        .unwrap();
    assert_eq!(
        int64_rows(std::slice::from_ref(&output)),
        vec![vec![Some(1)], vec![Some(2)], vec![Some(3)]]
    );
    let schema = output.chunk_schema_ref();
    assert!(schema.is_typed());
    assert!(schema.slots()[0].field_ref().metadata().as_arc().is_none());
    drop(processor);
    drop(operator);
    assert!(tracker.current() > 0);
    drop(output);
    assert!(tracker.current() > 0);
    drop(schema);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn compiled_series_prepare_keeps_typed_first_memory_refusal_without_later_construction() {
    use crate::exec::operators::compiled_generate_series::CompiledGenerateSeriesProcessorFactory;
    use crate::exec::pipeline::operator::ConstructionFailure;
    use crate::exec::pipeline::operator_factory::OperatorFactory;
    use crate::runtime::fragment::ExecutionFailureCause;
    use crate::runtime::mem_tracker::MemTracker;
    use crate::runtime::runtime_state::RuntimeErrorState;
    let program = compile(literal_source([Some(1), Some(3), Some(1)], true, false), 8);
    let factory = CompiledGenerateSeriesProcessorFactory::try_new(
        Arc::clone(&program),
        program.graph().root(),
        Arc::new(RuntimeErrorState::default()),
    )
    .unwrap();
    let tracker = MemTracker::new_root("series first schema refusal");
    let mut operator = factory.create(1, 0);
    operator.set_construction_factory(super::aggregate_fixture::runtime_construction_factory(
        tracker.clone(),
        Arc::new(RuntimeErrorState::default()),
    ));
    // Install the real factory first. One byte of remaining headroom rejects
    // preparation's first allocation, rather than failing fixture setup.
    let installed_bytes = tracker.current();
    assert!(installed_bytes > 0);
    tracker.install_limit_once(installed_bytes + 1).unwrap();
    let error = operator.prepare().unwrap_err();
    assert!(matches!(
        error.cause(),
        ExecutionFailureCause::Construction(ConstructionFailure::Memory(_))
    ));
    assert_eq!(tracker.current(), installed_bytes);
    assert_eq!(operator.prepare().unwrap_err(), error);
    assert!(
        operator
            .drive_for_test(&crate::runtime::runtime_state::RuntimeState::default())
            .is_err()
    );
    assert_eq!(tracker.current(), installed_bytes);
    drop(operator);
    assert_eq!(tracker.current(), 0);
}
