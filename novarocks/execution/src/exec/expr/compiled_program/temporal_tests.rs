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
//! Actual prepared owners, frozen source edges and the sole compiled evaluator.
use super::*;
use arrow::array::{ArrayRef, Date32Array, StringArray, TimestampMicrosecondArray};
use novarocks_type_contract::{TemporalSourceKind as Kind, TemporalSourceShape as Source};

#[derive(Clone, Copy)]
enum Fixture {
    PlainSec,
    PlainFormat,
    FormatOverride,
    Roundtrip,
    SparseIf,
    CastOther,
    SparseCastOther,
}
fn allow_parameter() -> novarocks_type_contract::SemanticParameterRef {
    novarocks_type_contract::SemanticParameterRef {
        id: novarocks_type_contract::SemanticParameterId::new(0),
        expected_key: novarocks_type_contract::SemanticParameterKey::AllowThrowException,
    }
}
fn catalogue() -> PureEngineFunctionCatalog {
    let actual =
        novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
    let mut builder = EngineFunctionCatalogBuilder::new();
    let mut manifest = Vec::new();
    for (name, abi) in [
        ("time_to_sec", PureKernelAbi::ControlIntrinsicV1),
        ("time_format", PureKernelAbi::ControlIntrinsicV1),
        ("if", PureKernelAbi::ControlIntrinsicV1),
        ("sec_to_time", PureKernelAbi::ScalarV1),
    ] {
        let definition = actual.definition(name, FunctionKind::Scalar).unwrap();
        builder.register(definition.clone()).unwrap();
        for signature in definition.canonical_signatures() {
            manifest.push(InstalledPureKernel {
                function: FunctionId::try_new(format!("builtin.scalar/{name}/v1")).unwrap(),
                kind: FunctionKind::Scalar,
                implementation: PureImplementationDeclaration {
                    overload: FunctionOverloadId::try_new(format!(
                        "builtin.scalar/{name}/{signature}"
                    ))
                    .unwrap(),
                    implementation: PureImplementationId::try_new(format!(
                        "builtin.scalar/{name}/selected-v1"
                    ))
                    .unwrap(),
                    abi,
                },
                aggregate_state_format: None,
            });
        }
    }
    builder.seal_pure(manifest).unwrap()
}
fn program(fixture: Fixture, dtype: DataType) -> Arc<LocalProgram> {
    let functions = catalogue();
    let source = NodeId::new(u32::MAX);
    let input = NodeId::new(41);
    let output = NodeId::new(0);
    let fragment_id = FragmentId::new(193);
    let value = ValueId::new(901);
    let format_value = ValueId::new(3);
    let flag = ValueId::new(71);
    let ty = FunctionValueType::new(dtype.clone(), true);
    let string = FunctionValueType::new(DataType::Utf8, true);
    let boolean = FunctionValueType::new(DataType::Boolean, true);
    let mut builder = FragmentBuilder::new(fragment_id);
    builder
        .add_values(source, Box::from([Box::default()]), Box::default())
        .unwrap();
    let mut items = Vec::new();
    let mut outputs = Vec::new();
    for (id, ty) in [
        (value, ty.clone()),
        (format_value, string.clone()),
        (flag, boolean.clone()),
    ] {
        let expr = builder
            .add_expression(input, ty.clone(), ExprKind::Literal(LiteralValue::Null))
            .unwrap();
        builder
            .insert_value(ValueDef {
                id,
                ty,
                origin: ValueOrigin::Expr { node: input, expr },
            })
            .unwrap();
        items.push((expr, id));
        outputs.push(id);
    }
    builder
        .add_project(
            input,
            source,
            items.into_boxed_slice(),
            outputs.into_boxed_slice(),
        )
        .unwrap();
    let source_expr = builder
        .add_expression(output, ty.clone(), ExprKind::Value(value))
        .unwrap();
    let format_expr = if matches!(fixture, Fixture::PlainFormat | Fixture::FormatOverride) {
        Some(
            builder
                .add_expression(output, string.clone(), ExprKind::Value(format_value))
                .unwrap(),
        )
    } else {
        None
    };
    let mut authors = BTreeMap::new();
    let mut normal = source_expr;
    let normal_type;
    let (name, shape) = match fixture {
        Fixture::PlainSec | Fixture::SparseIf => {
            normal_type = ty.clone();
            ("time_to_sec", Source::SecondsDirect)
        }
        Fixture::CastOther | Fixture::SparseCastOther => {
            // The frozen raw source-error baseline uses an identity cast. A
            // Date32-to-timestamp cast can panic before this source phase.
            normal_type = ty.clone();
            normal = builder
                .add_expression(
                    output,
                    normal_type.clone(),
                    ExprKind::Cast {
                        array_failure_mode: None,
                        expr: source_expr,
                        target: normal_type.data_type().clone(),
                        decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                        allow_throw_exception: allow_parameter(),
                    },
                )
                .unwrap();
            ("time_to_sec", Source::SecondsCastOther)
        }
        Fixture::PlainFormat => {
            normal_type = ty.clone();
            ("time_format", Source::FormatOrdinary)
        }
        Fixture::FormatOverride => {
            normal_type = FunctionValueType::new(
                DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
                true,
            );
            normal = builder
                .add_expression(
                    output,
                    normal_type.clone(),
                    ExprKind::Cast {
                        array_failure_mode: None,
                        expr: source_expr,
                        target: normal_type.data_type().clone(),
                        decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                        allow_throw_exception: allow_parameter(),
                    },
                )
                .unwrap();
            ("time_format", Source::FormatUtf8Override)
        }
        Fixture::Roundtrip => {
            normal_type = string.clone();
            normal = call(
                &mut builder,
                &mut authors,
                author(
                    &functions,
                    "sec_to_time",
                    vec![argument(ty.clone(), None)],
                    ControlShape::Eager,
                ),
                vec![source_expr],
            );
            // The unsupported transformed TIME child must stay undemanded.
            normal = builder
                .add_expression(
                    output,
                    FunctionValueType::new(
                        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
                        true,
                    ),
                    ExprKind::Cast {
                        array_failure_mode: None,
                        expr: normal,
                        target: DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
                        decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                        allow_throw_exception: allow_parameter(),
                    },
                )
                .unwrap();
            ("time_to_sec", Source::SecondsRoundtrip)
        }
    };
    let mut arguments = vec![argument(
        if matches!(fixture, Fixture::Roundtrip) {
            FunctionValueType::new(
                DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
                true,
            )
        } else {
            normal_type
        },
        None,
    )];
    let mut args = vec![normal];
    if shape.kind() == Kind::TimeFormat {
        arguments.push(argument(string, None));
        args.push(format_expr.expect("format fixture has its reachable source"));
    }
    let mut root_expr = call(
        &mut builder,
        &mut authors,
        author(
            &functions,
            name,
            arguments,
            ControlShape::TemporalSource(shape),
        ),
        args,
    );
    if matches!(fixture, Fixture::SparseIf | Fixture::SparseCastOther) {
        let flag_expr = builder
            .add_expression(output, boolean.clone(), ExprKind::Value(flag))
            .unwrap();
        let integer = FunctionValueType::new(DataType::Int64, true);
        let null = builder
            .add_expression(
                output,
                integer.clone(),
                ExprKind::Literal(LiteralValue::Null),
            )
            .unwrap();
        root_expr = call(
            &mut builder,
            &mut authors,
            author(
                &functions,
                "if",
                vec![
                    argument(boolean, None),
                    argument(integer.clone(), None),
                    argument(integer, None),
                ],
                ControlShape::If,
            ),
            vec![flag_expr, root_expr, null],
        );
    }
    let result_type = authors[&root_expr].result();
    let result_value = builder
        .add_value(
            result_type.clone(),
            ValueOrigin::Expr {
                node: output,
                expr: root_expr,
            },
        )
        .unwrap();
    builder
        .add_project(
            output,
            input,
            Box::from([(root_expr, result_value)]),
            Box::from([result_value]),
        )
        .unwrap();
    let fragment = builder
        .finish_definition(
            output,
            FragmentSink::Result,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let result = ResultPort {
        scalar_schema: None,
        fragment: fragment_id,
        output: fragment.nodes()[&output].output.clone(),
        fields: Box::from([ResultField {
            domain: crate::test_result_domain::result_value_domain(&result_type),
            name: "temporal_result".into(),
            alias: None,
            value: result_value,
            ty: result_type,
        }]),
    };
    compile_checked_fragment_with_parameters(
        &functions,
        fragment,
        &authors,
        result,
        if matches!(
            fixture,
            Fixture::PlainSec | Fixture::PlainFormat | Fixture::SparseIf
        ) {
            SemanticParameters::try_new([]).unwrap()
        } else {
            SemanticParameters::try_new([(
                novarocks_type_contract::SemanticParameterId::new(0),
                novarocks_type_contract::SemanticParameterValue::AllowThrowException(false),
            )])
            .unwrap()
        },
    )
}
fn batch(
    program: &LocalProgram,
    values: ArrayRef,
    formats: Vec<Option<&str>>,
    flags: Vec<Option<bool>>,
) -> RecordBatch {
    RecordBatch::try_new(
        program.graph().nodes()[1].output_layout().schema().clone(),
        vec![
            values,
            Arc::new(StringArray::from(formats)),
            Arc::new(BooleanArray::from(flags)),
        ],
    )
    .unwrap()
}
#[test]
fn actual_each_registered_profile_uses_explicit_source_receipt_and_original_math() {
    let timestamp = DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None);
    for (dtype, values, expected) in [
        (
            DataType::Utf8,
            Arc::new(StringArray::from(vec![Some("12:34:56"), None, Some("bad")])) as ArrayRef,
            vec![Some(45296), None, None],
        ),
        (
            DataType::Date32,
            Arc::new(Date32Array::from(vec![Some(1), None, Some(-1)])) as ArrayRef,
            vec![Some(0), None, Some(0)],
        ),
        (
            timestamp,
            Arc::new(TimestampMicrosecondArray::from(vec![
                Some(45296000000),
                None,
                Some(1000000),
            ])) as ArrayRef,
            vec![Some(45296), None, Some(1)],
        ),
    ] {
        let seconds = program(Fixture::PlainSec, dtype.clone());
        let input = batch(
            &seconds,
            values.clone(),
            vec![Some("%f"); 3],
            vec![Some(true); 3],
        );
        let result = instance(&seconds)
            .evaluate(&input, Selection::all(3), &Control)
            .unwrap();
        assert_eq!(
            result
                .values()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            expected
        );
        let format = program(Fixture::PlainFormat, dtype);
        let input = batch(
            &format,
            values,
            vec![Some("%H:%i:%s/%f"); 3],
            vec![Some(true); 3],
        );
        let result = instance(&format)
            .evaluate(&input, Selection::all(3), &Control)
            .unwrap();
        let expected = expected
            .iter()
            .map(|value| value.map(|v| format!("00:00:00/{v:06}")))
            .collect::<Vec<_>>();
        assert_eq!(
            result
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .map(|s| s.map(str::to_owned))
                .collect::<Vec<_>>(),
            expected
        );
    }
}
#[test]
fn utf8_override_preserves_raw_clock_result_and_two_distinct_source_occurrences() {
    let program = program(Fixture::FormatOverride, DataType::Utf8);
    let calls = program.checked().channels().expressions().resolved_calls();
    let source = calls
        .calls()
        .values()
        .find_map(|call| call.call_contract().temporal_source())
        .unwrap();
    assert_eq!(source.facts.shape(), Source::FormatUtf8Override);
    assert_ne!(
        source.sources[0].context.use_id,
        source.sources[1].context.use_id
    );
    let input = batch(
        &program,
        Arc::new(StringArray::from(vec![Some("12:34:56"), Some("bad"), None])),
        vec![Some("%f"); 3],
        vec![Some(true); 3],
    );
    let result = instance(&program)
        .evaluate(&input, Selection::all(3), &Control)
        .unwrap();
    assert_eq!(
        result
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some("045296"), None, None]
    );
}
#[test]
fn sec_roundtrip_bypasses_unprepared_transformed_child_and_keeps_saturation() {
    let program = program(Fixture::Roundtrip, DataType::Int64);
    let input = batch(
        &program,
        Arc::new(Int64Array::from(vec![Some(-12), Some(4000000), None])),
        vec![None; 3],
        vec![None; 3],
    );
    let result = instance(&program)
        .evaluate(&input, Selection::all(3), &Control)
        .unwrap();
    assert_eq!(
        result
            .values()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(-12), Some(3023999), None]
    );
}
#[test]
fn sparse_if_invocation_ignores_inactive_original_rows_and_empty_never_demands_sources() {
    let program = program(Fixture::SparseIf, DataType::Utf8);
    let input = batch(
        &program,
        Arc::new(StringArray::from(vec![Some("bad"), Some("12:34:56"), None])),
        vec![None; 3],
        vec![Some(false), Some(true), Some(false)],
    );
    let result = instance(&program)
        .evaluate(&input, Selection::all(3), &Control)
        .unwrap();
    assert_eq!(
        result
            .values()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![None, Some(45296), None]
    );
    let empty = [];
    let result = instance(&program)
        .evaluate(&input, Selection::try_sparse(3, &empty).unwrap(), &Control)
        .unwrap();
    assert!(result.values().is_empty());
}
#[test]
fn every_actual_temporal_constructor_and_evaluation_callback_preserves_seven_causes_and_failed_latch()
 {
    for fixture in [
        Fixture::PlainSec,
        Fixture::PlainFormat,
        Fixture::FormatOverride,
        Fixture::Roundtrip,
        Fixture::SparseIf,
        Fixture::CastOther,
    ] {
        let dtype = match fixture {
            Fixture::Roundtrip => DataType::Int64,
            Fixture::CastOther => DataType::Date32,
            _ => DataType::Utf8,
        };
        let program = program(fixture, dtype);
        let rows = if matches!(fixture, Fixture::Roundtrip | Fixture::CastOther) {
            320
        } else {
            8
        };
        let long_text = "12:34:56".repeat(40);
        let values: ArrayRef = if matches!(fixture, Fixture::Roundtrip) {
            Arc::new(Int64Array::from(vec![Some(45296); rows]))
        } else if matches!(fixture, Fixture::CastOther) {
            Arc::new(Date32Array::from(vec![Some(i32::MAX); rows]))
        } else {
            Arc::new(StringArray::from(vec![Some(long_text.as_str()); rows]))
        };
        let input = batch(
            &program,
            values,
            vec![Some("%H:%i:%s/%f"); rows],
            vec![Some(true); rows],
        );
        let recorder = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
        CompiledExpressionInstance::try_new(program.clone(), root(), &recorder).unwrap();
        let constructor = recorder.trace.lock().unwrap().clone();
        for index in 1..=constructor.len() {
            for cause in causes() {
                let control = CallbackControl::new(cause.clone(), index);
                assert!(
                    matches!(CompiledExpressionInstance::try_new(program.clone(),root(),&control),Err(actual) if actual==cause)
                );
                assert_eq!(*control.trace.lock().unwrap(), constructor[..index]);
            }
        }
        let recorder = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
        instance(&program)
            .evaluate(&input, Selection::all(rows), &recorder)
            .unwrap();
        let trace = recorder.trace.lock().unwrap().clone();
        assert!(trace.contains(&256));
        assert!(trace.iter().all(|units| *units <= 256));
        for index in 1..=trace.len() {
            for cause in causes() {
                let mut evaluator = instance(&program);
                let control = CallbackControl::new(cause.clone(), index);
                assert!(
                    matches!(evaluator.evaluate(&input,Selection::all(rows),&control),Err(actual) if actual==cause)
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..index]);
                let after = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
                assert!(matches!(
                    evaluator.evaluate(&input, Selection::all(rows), &after),
                    Err(KernelFailure::InstanceFailed)
                ));
                assert!(after.trace.lock().unwrap().is_empty());
            }
        }
    }
}

#[test]
fn deepest_data_error_projects_across_successful_current_invocation_and_sparse_guard() {
    for (dtype, array, expected) in [
        (
            DataType::Date32,
            Arc::new(Date32Array::from(vec![0, i32::MAX])) as ArrayRef,
            "Cast error: Failed to convert 2147483647 to temporal for Date32",
        ),
        (
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
            Arc::new(TimestampMicrosecondArray::from(vec![0, i64::MAX])) as ArrayRef,
            "Cast error: Failed to convert 9223372036854775807 to datetime for Timestamp(µs)",
        ),
    ] {
        let ordinary = program(Fixture::CastOther, dtype.clone());
        let input = batch(&ordinary, array.clone(), vec![None; 2], vec![None; 2]);
        let mut evaluator = instance(&ordinary);
        let output = evaluator
            .evaluate(&input, Selection::all(2), &Control)
            .unwrap();
        assert_eq!(
            output
                .errors()
                .iter()
                .map(|e| (e.selected_ordinal(), e.message()))
                .collect::<Vec<_>>(),
            vec![(0, expected), (1, expected)]
        );
        assert!(output.values().is_null(0));
        assert!(output.values().is_null(1));
        // SQL data errors do not latch the runtime-control failure path.
        let sparse = [0];
        let output = evaluator
            .evaluate(&input, Selection::try_sparse(2, &sparse).unwrap(), &Control)
            .unwrap();
        assert!(output.errors().is_empty());
        assert_eq!(
            output
                .values()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            0
        );
        let guarded = program(Fixture::SparseCastOther, dtype);
        let input = batch(
            &guarded,
            array,
            vec![None; 2],
            vec![Some(true), Some(false)],
        );
        let output = instance(&guarded)
            .evaluate(&input, Selection::all(2), &Control)
            .unwrap();
        assert!(output.errors().is_empty());
        assert_eq!(
            output
                .values()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(0), None]
        );
    }
}
