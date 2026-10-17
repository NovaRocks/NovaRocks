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
//! Frozen window parameters must read the selected immutable pool ordinal.

use super::*;
use arrow::array::{
    Date32Array, Decimal128Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
    NullArray, StringArray, TimestampMillisecondArray, UInt64Array,
};
use arrow::datatypes::Field;
use novarocks_functions::{ConstantPolicy, ConstantPool, ConstantValue};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, FunctionValueType, PureCompileControl,
};
use novarocks_types::SlotId;

struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}

fn selected(array: ArrayRef) -> ConstantValue {
    assert_eq!(array.len(), 3);
    let ty = FunctionValueType::new(array.data_type().clone(), array.logical_null_count() != 0);
    let pool = ConstantPool::try_new(
        Arc::new(Field::new(
            "parameter",
            array.data_type().clone(),
            ty.nullable(),
        )),
        ty,
        array.to_data(),
        ConstantPolicy {
            max_rows: 3,
            max_array_nodes: 16,
            max_logical_elements: 64,
            max_retained_buffer_bytes: 1 << 16,
            max_type_depth: 8,
            max_type_nodes: 32,
            max_dictionary_depth: 4,
            max_metadata_bytes: 1 << 16,
            max_library_validation_work: 1 << 16,
            max_library_validation_bytes: 1 << 16,
        },
        CompilePhase::Validate,
        &Control,
    )
    .expect("valid immutable parameter pool");
    let value = pool.value(1).expect("nonzero ordinal");
    assert_eq!(value.ordinal(), 1);
    value
}

fn integer_parameters(value: i64) -> Vec<ConstantValue> {
    vec![
        selected(Arc::new(Int32Array::from(vec![
            -99,
            i32::try_from(value).unwrap(),
            99,
        ]))),
        selected(Arc::new(Int64Array::from(vec![-999, value, 999]))),
    ]
}

fn parameter(arena: &mut ExprArena, value: ConstantValue) -> ExprId {
    let ty = value.value_type().clone();
    arena.push_value_typed(ExprNode::Constant(value), ty)
}

fn spec(kind: WindowFunctionKind, args: Vec<ExprId>) -> WindowFunctionSpec {
    WindowFunctionSpec {
        kind,
        args,
        return_type: DataType::Int64,
        aggregate_binding: None,
    }
}

fn values(actual: ArrayRef) -> Vec<Option<i64>> {
    actual
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("BIGINT output")
        .iter()
        .collect()
}

fn run(
    kind: WindowFunctionKind,
    parameter_value: ConstantValue,
    input: Vec<Option<i64>>,
    partitions: &[(usize, usize)],
    window: Option<&WindowFrame>,
) -> Result<ArrayRef, String> {
    let mut arena = ExprArena::default();
    let input_id = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int64);
    let parameter_id = parameter(&mut arena, parameter_value);
    let default_id = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(-1)), DataType::Int64);
    let rows = input.len();
    let args = if matches!(kind, WindowFunctionKind::Ntile) {
        vec![parameter_id]
    } else if matches!(
        kind,
        WindowFunctionKind::Lead { .. } | WindowFunctionKind::Lag { .. }
    ) {
        vec![input_id, parameter_id, default_id]
    } else {
        vec![input_id, parameter_id]
    };
    let order = Arc::new(Int64Array::from_iter_values(0..rows as i64)) as ArrayRef;
    let ctx = PartitionWindowContext::new(partitions, &[order], window).unwrap();
    // Runtime argument rows cannot substitute for the frozen parameter node.
    let runtime_args = vec![
        Arc::new(Int64Array::from(input)) as ArrayRef,
        Arc::new(Int64Array::from(vec![777; rows])) as ArrayRef,
        Arc::new(Int64Array::from(vec![-1; rows])) as ArrayRef,
    ];
    compute_window_function(
        &arena,
        &spec(kind, args),
        None,
        &runtime_args,
        &ctx,
        rows,
        None,
    )
}

#[test]
fn ntile_reads_nonzero_ordinal_int32_and_int64() {
    for parameter in integer_parameters(3) {
        assert_eq!(
            values(
                run(
                    WindowFunctionKind::Ntile,
                    parameter,
                    vec![Some(0); 8],
                    &[(0, 5), (5, 8)],
                    None
                )
                .unwrap()
            ),
            vec![
                Some(1),
                Some(1),
                Some(2),
                Some(2),
                Some(3),
                Some(1),
                Some(2),
                Some(3)
            ]
        );
    }
}

#[test]
fn lead_and_lag_read_nonzero_ordinal_and_preserve_partition_defaults_and_nulls() {
    for parameter in integer_parameters(2) {
        let input = vec![Some(10), None, Some(30), Some(40), Some(50), Some(60)];
        assert_eq!(
            values(
                run(
                    WindowFunctionKind::Lag {
                        ignore_nulls: false
                    },
                    parameter.clone(),
                    input.clone(),
                    &[(0, 4), (4, 6)],
                    None
                )
                .unwrap()
            ),
            vec![Some(-1), Some(-1), Some(10), None, Some(-1), Some(-1)]
        );
        assert_eq!(
            values(
                run(
                    WindowFunctionKind::Lead {
                        ignore_nulls: false
                    },
                    parameter,
                    input,
                    &[(0, 4), (4, 6)],
                    None
                )
                .unwrap()
            ),
            vec![Some(30), Some(40), Some(-1), Some(-1), Some(-1), Some(-1)]
        );
    }
}

#[test]
fn session_number_reads_nonzero_ordinal_and_preserves_null_rows_and_partitions() {
    for parameter in integer_parameters(3) {
        assert_eq!(
            values(
                run(
                    WindowFunctionKind::SessionNumber,
                    parameter,
                    vec![Some(1), Some(3), None, Some(8), Some(9), Some(20)],
                    &[(0, 4), (4, 6)],
                    None
                )
                .unwrap()
            ),
            vec![Some(1), Some(1), None, Some(2), Some(1), Some(2)]
        );
    }
}

#[test]
fn first_value_rewrite_reads_nonzero_ordinal_padding() {
    let window = WindowFrame {
        window_type: WindowType::Rows,
        start: None,
        end: Some(WindowBoundary::Preceding(2)),
    };
    for parameter in integer_parameters(2) {
        assert_eq!(
            values(
                run(
                    WindowFunctionKind::FirstValueRewrite {
                        ignore_nulls: false
                    },
                    parameter,
                    vec![Some(10), Some(20), Some(30), Some(40)],
                    &[(0, 4)],
                    Some(&window)
                )
                .unwrap()
            ),
            vec![None, None, Some(10), Some(20)]
        );
    }
}

#[test]
fn session_number_selected_typed_and_logical_null_are_null_outputs() {
    for parameter in [
        selected(Arc::new(Int32Array::from(vec![Some(123), None, Some(-9)]))),
        selected(Arc::new(Int64Array::from(vec![Some(123), None, Some(-9)]))),
        selected(Arc::new(NullArray::new(3))),
    ] {
        assert_eq!(
            values(
                run(
                    WindowFunctionKind::SessionNumber,
                    parameter,
                    vec![Some(1), None, Some(8)],
                    &[(0, 3)],
                    None
                )
                .unwrap()
            ),
            vec![None, None, None]
        );
    }
}

#[test]
fn selected_null_is_rejected_by_required_integer_callers() {
    for parameter in [
        selected(Arc::new(Int32Array::from(vec![Some(123), None, Some(3)]))),
        selected(Arc::new(Int64Array::from(vec![Some(123), None, Some(3)]))),
        selected(Arc::new(NullArray::new(3))),
    ] {
        for kind in [
            WindowFunctionKind::Ntile,
            WindowFunctionKind::Lead {
                ignore_nulls: false,
            },
            WindowFunctionKind::Lag {
                ignore_nulls: false,
            },
            WindowFunctionKind::FirstValueRewrite {
                ignore_nulls: false,
            },
        ] {
            assert!(run(kind, parameter.clone(), vec![Some(1); 3], &[(0, 3)], None).is_err());
        }
    }
}

#[test]
fn constant_parameters_keep_original_range_validation() {
    for parameter in integer_parameters(0)
        .into_iter()
        .chain(integer_parameters(-1))
    {
        assert_eq!(
            run(
                WindowFunctionKind::Ntile,
                parameter,
                vec![Some(1); 3],
                &[(0, 3)],
                None
            )
            .unwrap_err(),
            "ntile buckets must be positive"
        );
    }
    for parameter in integer_parameters(-1) {
        for kind in [
            WindowFunctionKind::Lead {
                ignore_nulls: false,
            },
            WindowFunctionKind::Lag {
                ignore_nulls: false,
            },
        ] {
            assert_eq!(
                run(kind, parameter.clone(), vec![Some(1); 3], &[(0, 3)], None).unwrap_err(),
                "lead/lag offset must be non-negative"
            );
        }
    }
}

#[test]
fn integer_reader_preserves_full_int64_extremes_without_narrowing() {
    for expected in [i64::MIN, i64::MAX] {
        let mut arena = ExprArena::default();
        let id = parameter(
            &mut arena,
            selected(Arc::new(Int64Array::from(vec![0, expected, 1]))),
        );
        assert_eq!(parse_int64_literal(&arena, id).unwrap(), expected);
    }
}

#[test]
fn forbidden_physical_types_and_their_nulls_are_not_integer_parameters() {
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(Int8Array::from(vec![1, 2, 3])),
        Arc::new(Int16Array::from(vec![1, 2, 3])),
        Arc::new(UInt64Array::from(vec![1, 2, 3])),
        Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])),
        Arc::new(StringArray::from(vec!["1", "2", "3"])),
        Arc::new(Date32Array::from(vec![1, 2, 3])),
        Arc::new(TimestampMillisecondArray::from(vec![1, 2, 3])),
        Arc::new(
            Decimal128Array::from(vec![1, 2, 3])
                .with_precision_and_scale(10, 0)
                .unwrap(),
        ),
    ];
    for array in arrays {
        for array in [array.clone(), new_null_array(array.data_type(), 3)] {
            let parameter = selected(array);
            for kind in [
                WindowFunctionKind::Ntile,
                WindowFunctionKind::Lead {
                    ignore_nulls: false,
                },
                WindowFunctionKind::FirstValueRewrite {
                    ignore_nulls: false,
                },
                WindowFunctionKind::SessionNumber,
            ] {
                assert!(
                    run(kind, parameter.clone(), vec![Some(1); 3], &[(0, 3)], None).is_err(),
                    "forbidden parameter: {:?}",
                    parameter.value_type()
                );
            }
        }
    }
}

#[test]
fn dynamic_slot_cast_and_noninteger_literal_are_not_constant_parameters() {
    let mut arena = ExprArena::default();
    let slot = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int64);
    let float = arena.push_typed(
        ExprNode::Literal(LiteralValue::Float64(2.0)),
        DataType::Float64,
    );
    let integer = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
    let cast = arena.push_typed(
        ExprNode::Cast(
            integer,
            novarocks_type_contract::DecimalOverflowPolicy::ReportError,
        ),
        DataType::Int64,
    );
    assert!(parse_int64_literal(&arena, cast).is_err());
    assert!(parse_int64_literal(&arena, slot).is_err());
    assert!(parse_int64_literal(&arena, float).is_err());
}

#[test]
fn shared_state_evaluates_selected_pool_parameter_and_preserves_chunk_boundaries() {
    use crate::exec::chunk::{ChunkSchema, ChunkSlotSchema};
    fn schema(slot: u32, name: &str) -> ChunkSchemaRef {
        Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                SlotId::new(slot),
                Field::new(name, DataType::Int64, true),
                None,
                None,
            )])
            .unwrap(),
        )
    }
    let input_schema = schema(1, "input");
    let chunks = vec![
        Chunk::try_new_with_columns(
            input_schema.clone(),
            vec![Arc::new(Int64Array::from(vec![10, 20])) as ArrayRef],
        )
        .unwrap(),
        Chunk::try_new_with_columns(
            input_schema,
            vec![Arc::new(Int64Array::from(vec![30, 40, 50])) as ArrayRef],
        )
        .unwrap(),
    ];
    let mut arena = ExprArena::default();
    let id = parameter(
        &mut arena,
        selected(Arc::new(Int64Array::from(vec![-99, 3, 99]))),
    );
    let state = AnalyticSharedState::new(
        Arc::new(arena),
        vec![],
        vec![],
        vec![spec(WindowFunctionKind::Ntile, vec![id])],
        None,
        vec![AnalyticOutputColumn::Window(0)],
        schema(3, "ntile"),
        crate::exec::expr::agg::test_builtin_execution_function_set(),
        1,
    )
    .unwrap();
    let output = state.compute_outputs(&chunks, None).unwrap();
    assert_eq!(
        output.iter().map(Chunk::len).collect::<Vec<_>>(),
        vec![2, 3]
    );
    assert_eq!(
        values(output[0].columns()[0].clone()),
        vec![Some(1), Some(1)]
    );
    assert_eq!(
        values(output[1].columns()[0].clone()),
        vec![Some(2), Some(2), Some(3)]
    );
}
