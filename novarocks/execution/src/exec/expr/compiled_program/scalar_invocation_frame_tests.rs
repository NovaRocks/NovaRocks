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

// The real dynamic ARRAY binder admits "id" against authored "ID" by its
// original case-insensitive rule. The ONE original runtime projection then
// rejects the byte-sensitive name. No request/runtime value is substituted.
// The private factory funds only original diagnostics; it publishes no ARRAY
// success capability. These probes verify the actual compiled failure route.
use super::*;
use crate::runtime::fragment::ExecutionFailure;
use crate::runtime::fragment::ExecutionFailureCause;
use crate::runtime::mem_tracker::MemTracker;
use arrow::array::{ArrayRef, Int32Array, ListArray, StructArray};
use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::Field;
use novarocks_functions::ScalarInvocationActivation as Activation;

fn actual_functions() -> PureEngineFunctionCatalog {
    static CATALOG: std::sync::OnceLock<PureEngineFunctionCatalog> = std::sync::OnceLock::new();
    CATALOG.get_or_init(|| {
        let actual = novarocks_functions::builtin::catalogue::array_projection_diagnostic_private_test_catalog();
        let builtin = novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
        let mut builder = EngineFunctionCatalogBuilder::new();
        let mut installed = Vec::new();
        for (catalog, definition) in actual.definitions().iter().map(|definition| (&actual,definition)).chain(std::iter::once((&builtin,builtin.definition("rand",FunctionKind::Scalar).unwrap()))) {
            let binding = definition.binding_declaration().unwrap();
            builder.register(definition.clone()).unwrap();
            for overload in binding.overloads() {
                let loan = catalog.pure_overload_declaration_observed(binding.function_id(),definition.kind(),&overload.identity,&Control).unwrap();
                installed.push(InstalledPureKernel {function:binding.function_id().clone(),kind:definition.kind(),implementation:loan.implementation().clone(),aggregate_state_format:None});
            }
        }
        // Every record comes from the same real installed adapter. RAND is an
        // actually volatile, stateful, fully bound tail, never a toy counter.
        builder.seal_pure(installed).unwrap()
    }).clone()
}

fn input_type() -> FunctionValueType {
    FunctionValueType::new(
        DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(vec![Arc::new(Field::new("ID", DataType::Int32, true))].into()),
            true,
        ))),
        true,
    )
}
#[derive(Clone, Copy)]
enum Guard {
    Direct,
    IfThen,
    IfElse,
    And,
    Or,
    Filter,
}
fn invocation_program(count: usize, guard: Guard) -> Arc<LocalProgram> {
    invocation_program_with(count, guard, actual_functions(), "id", SourceMode::Slot)
}
#[derive(Clone, Copy, PartialEq)]
enum SourceMode {
    Slot,
    Computed,
    RequiredError,
    IfThen,
    IfElse,
    NestedIf,
}
fn invocation_program_with(
    count: usize,
    guard: Guard,
    functions: PureEngineFunctionCatalog,
    field_name: &str,
    source_mode: SourceMode,
) -> Arc<LocalProgram> {
    let id = FragmentId::new(271);
    let source = NodeId::new(u32::MAX);
    let input = NodeId::new(41);
    let output = NodeId::new(0);
    let source_type = input_type();
    let flag_type = FunctionValueType::new(DataType::Boolean, true);
    let tail_type = FunctionValueType::new(DataType::Int64, true);
    let columns = [
        (ValueId::new(1), source_type.clone()),
        (ValueId::new(2), flag_type.clone()),
        (ValueId::new(3), tail_type.clone()),
    ];
    let mut builder = FragmentBuilder::new(id);
    let mut authors = BTreeMap::new();
    builder
        .add_values(source, Box::from([Box::default()]), Box::default())
        .unwrap();
    let mut items = Vec::new();
    for (value, ty) in &columns {
        let expr = builder
            .add_expression(input, ty.clone(), ExprKind::Literal(LiteralValue::Null))
            .unwrap();
        builder
            .insert_value(ValueDef {
                id: *value,
                ty: ty.clone(),
                origin: ValueOrigin::Expr { node: input, expr },
            })
            .unwrap();
        items.push((expr, *value));
    }
    builder
        .add_project(
            input,
            source,
            items.into_boxed_slice(),
            columns
                .iter()
                .map(|(value, _)| *value)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        )
        .unwrap();
    let mut data = builder
        .add_expression(output, source_type.clone(), ExprKind::Value(columns[0].0))
        .unwrap();
    let name_type = FunctionValueType::new(DataType::Utf8, false);
    let name = if source_mode == SourceMode::Slot {
        ConstantValue::from_utf8(
            Arc::new(name_type.try_to_field("actual field name").unwrap()),
            name_type.clone(),
            field_name,
            options().constants,
            CompilePhase::Validate,
            &Control,
        )
        .unwrap()
    } else {
        let array: ArrayRef = Arc::new(arrow::array::StringArray::from(vec![
            "poison_not_a_field",
            field_name,
        ]));
        novarocks_functions::ConstantPool::try_new(
            Arc::new(name_type.try_to_field("actual field name").unwrap()),
            name_type.clone(),
            array.to_data(),
            options().constants,
            CompilePhase::Validate,
            &Control,
        )
        .unwrap()
        .value(1)
        .unwrap()
    };
    let name_expr = builder
        .add_expression(
            output,
            name_type.clone(),
            ExprKind::Constant(novarocks_physical_plan::ConstantReference {
                pool: novarocks_physical_plan::ConstantPoolId::new(0),
                ordinal: name.ordinal(),
            }),
        )
        .unwrap();
    let allow_ref = novarocks_type_contract::SemanticParameterRef {
        id: novarocks_type_contract::SemanticParameterId::new(u32::MAX),
        expected_key: novarocks_type_contract::SemanticParameterKey::AllowThrowException,
    };
    if source_mode == SourceMode::Computed {
        data = builder
            .add_expression(
                output,
                source_type.clone(),
                ExprKind::Cast {
                    expr: data,
                    target: source_type.data_type().clone(),
                    decimal_overflow_policy:
                        novarocks_type_contract::DecimalOverflowPolicy::ReportError,
                    allow_throw_exception: allow_ref,
                },
            )
            .unwrap();
    } else if source_mode == SourceMode::RequiredError {
        let tail = builder
            .add_expression(output, tail_type.clone(), ExprKind::Value(columns[2].0))
            .unwrap();
        let one_ty = FunctionValueType::new(DataType::Int64, false);
        let one = builder
            .add_expression(
                output,
                one_ty.clone(),
                ExprKind::Literal(LiteralValue::Int64(1)),
            )
            .unwrap();
        // Fractional Divide(1, 0) is a successful NULL by the frozen
        // arithmetic contract. Integer Modulo(1, 0) is the real RowData
        // failure required to test transport from outside outer demand.
        let remainder_ty = novarocks_type_contract::arithmetic_result_value_type_with_op(
            &one_ty,
            &tail_type,
            novarocks_type_contract::ArithmeticOperator::Modulo,
        )
        .unwrap();
        let remainder = builder
            .add_expression(
                output,
                remainder_ty,
                ExprKind::Binary {
                    op: novarocks_physical_plan::BinaryOperator::Modulo,
                    left: one,
                    right: tail,
                    decimal_overflow_policy:
                        novarocks_type_contract::DecimalOverflowPolicy::ReportError,
                    allow_throw_exception: Some(allow_ref),
                },
            )
            .unwrap();
        let bool_ty = FunctionValueType::new(DataType::Boolean, true);
        let condition = builder
            .add_expression(
                output,
                bool_ty.clone(),
                ExprKind::Cast {
                    expr: remainder,
                    target: DataType::Boolean,
                    decimal_overflow_policy:
                        novarocks_type_contract::DecimalOverflowPolicy::ReportError,
                    allow_throw_exception: allow_ref,
                },
            )
            .unwrap();
        data = call(
            &mut builder,
            &mut authors,
            author(
                &functions,
                "if",
                vec![
                    argument(bool_ty, None),
                    argument(source_type.clone(), None),
                    argument(source_type.clone(), None),
                ],
                ControlShape::If,
            ),
            vec![condition, data, data],
        );
    }
    if matches!(
        source_mode,
        SourceMode::IfThen | SourceMode::IfElse | SourceMode::NestedIf
    ) {
        let flag = builder
            .add_expression(output, flag_type.clone(), ExprKind::Value(columns[1].0))
            .unwrap();
        let null = builder
            .add_expression(
                output,
                source_type.clone(),
                ExprKind::Literal(LiteralValue::Null),
            )
            .unwrap();
        if source_mode == SourceMode::NestedIf {
            let tail = builder
                .add_expression(output, tail_type.clone(), ExprKind::Value(columns[2].0))
                .unwrap();
            let inner = builder
                .add_expression(
                    output,
                    flag_type.clone(),
                    ExprKind::Cast {
                        expr: tail,
                        target: DataType::Boolean,
                        decimal_overflow_policy:
                            novarocks_type_contract::DecimalOverflowPolicy::ReportError,
                        allow_throw_exception: allow_ref,
                    },
                )
                .unwrap();
            data = call(
                &mut builder,
                &mut authors,
                author(
                    &functions,
                    "if",
                    vec![
                        argument(flag_type.clone(), None),
                        argument(source_type.clone(), None),
                        argument(source_type.clone(), None),
                    ],
                    ControlShape::If,
                ),
                vec![inner, data, null],
            );
        }
        let (left, right) = if source_mode == SourceMode::IfElse {
            (null, data)
        } else {
            (data, null)
        };
        data = call(
            &mut builder,
            &mut authors,
            author(
                &functions,
                "if",
                vec![
                    argument(flag_type.clone(), None),
                    argument(source_type.clone(), None),
                    argument(source_type.clone(), None),
                ],
                ControlShape::If,
            ),
            vec![flag, left, right],
        );
    }
    let mut arguments = vec![argument(source_type, None), argument(name_type, Some(name))];
    let mut definitions = vec![data, name_expr];
    // Complete bound volatile/math tail definitions are retained. The first
    // real RAND would create a mutable instance if demanded. Other tails retain
    // original divide-zero semantics; their value is never evaluated here.
    for ordinal in 2..count {
        if ordinal == 2 {
            let random = call(
                &mut builder,
                &mut authors,
                author(&functions, "rand", vec![], ControlShape::Eager),
                vec![],
            );
            let ty = builder.expressions().get(random).unwrap().ty.clone();
            definitions.push(random);
            arguments.push(argument(ty, None));
            continue;
        }
        let tail = builder
            .add_expression(output, tail_type.clone(), ExprKind::Value(columns[2].0))
            .unwrap();
        let zero = builder
            .add_expression(
                output,
                FunctionValueType::new(DataType::Int64, false),
                ExprKind::Literal(LiteralValue::Int64(0)),
            )
            .unwrap();
        let divided_type = novarocks_type_contract::arithmetic_result_value_type_with_op(
            &tail_type,
            &FunctionValueType::new(DataType::Int64, false),
            novarocks_type_contract::ArithmeticOperator::Divide,
        )
        .unwrap();
        let divided = builder
            .add_expression(
                output,
                divided_type.clone(),
                ExprKind::Binary {
                    op: novarocks_physical_plan::BinaryOperator::Divide,
                    left: tail,
                    right: zero,
                    decimal_overflow_policy:
                        novarocks_type_contract::DecimalOverflowPolicy::ReportError,
                    allow_throw_exception: Some(novarocks_type_contract::SemanticParameterRef {
                        id: novarocks_type_contract::SemanticParameterId::new(u32::MAX),
                        expected_key:
                            novarocks_type_contract::SemanticParameterKey::AllowThrowException,
                    }),
                },
            )
            .unwrap();
        definitions.push(divided);
        arguments.push(argument(divided_type, None));
    }
    let demand = if novarocks_functions::invocation_arity::ARRAY_STRUCT_SUBFIELD_ARITY
        .failure(count)
        .is_some()
    {
        ControlShape::NoArguments
    } else {
        ControlShape::Eager
    };
    let projected = call(
        &mut builder,
        &mut authors,
        author(&functions, "__array_struct_subfield", arguments, demand),
        definitions,
    );
    let ty = builder.expressions().get(projected).unwrap().ty.clone();
    let expr = match guard {
        Guard::Direct => projected,
        Guard::And | Guard::Or => {
            let predicate = builder
                .add_expression(
                    output,
                    FunctionValueType::new(DataType::Boolean, false),
                    ExprKind::IsNull {
                        expr: projected,
                        negated: false,
                    },
                )
                .unwrap();
            let decisive = builder
                .add_expression(
                    output,
                    FunctionValueType::new(DataType::Boolean, false),
                    ExprKind::Literal(LiteralValue::Boolean(matches!(guard, Guard::Or))),
                )
                .unwrap();
            let kind = if matches!(guard, Guard::And) {
                ExprKind::Conjunction {
                    args: Box::from([predicate, decisive]),
                }
            } else {
                ExprKind::Disjunction {
                    args: Box::from([predicate, decisive]),
                }
            };
            builder
                .add_expression(
                    output,
                    FunctionValueType::new(DataType::Boolean, false),
                    kind,
                )
                .unwrap()
        }
        Guard::Filter => builder
            .add_expression(
                output,
                FunctionValueType::new(DataType::Boolean, false),
                ExprKind::IsNull {
                    expr: projected,
                    negated: false,
                },
            )
            .unwrap(),
        Guard::IfThen | Guard::IfElse => {
            // Original IS NULL is a non-null Boolean consumer. Whole ARRAY
            // Data escapes before it can mask or produce a Boolean result.
            let predicate_type = FunctionValueType::new(DataType::Boolean, false);
            let predicate = builder
                .add_expression(
                    output,
                    predicate_type.clone(),
                    ExprKind::IsNull {
                        expr: projected,
                        negated: false,
                    },
                )
                .unwrap();
            let otherwise_type = FunctionValueType::new(DataType::Boolean, true);
            let flag = builder
                .add_expression(output, flag_type.clone(), ExprKind::Value(columns[1].0))
                .unwrap();
            let otherwise = builder
                .add_expression(
                    output,
                    otherwise_type.clone(),
                    ExprKind::Literal(LiteralValue::Null),
                )
                .unwrap();
            let (then_expr, else_expr, then_type, else_type) = if matches!(guard, Guard::IfThen) {
                (predicate, otherwise, predicate_type, otherwise_type)
            } else {
                (otherwise, predicate, otherwise_type, predicate_type)
            };
            call(
                &mut builder,
                &mut authors,
                author(
                    &functions,
                    "if",
                    vec![
                        argument(flag_type, None),
                        argument(then_type, None),
                        argument(else_type, None),
                    ],
                    ControlShape::If,
                ),
                vec![flag, then_expr, else_expr],
            )
        }
    };
    let (value, ty) = if matches!(guard, Guard::Filter) {
        builder
            .add_filter(output, input, Box::from([expr]))
            .unwrap();
        (columns[0].0, columns[0].1.clone())
    } else {
        let ty = builder.expressions().get(expr).unwrap().ty.clone();
        let value = builder
            .add_value(ty.clone(), ValueOrigin::Expr { node: output, expr })
            .unwrap();
        builder
            .add_project(
                output,
                input,
                Box::from([(expr, value)]),
                Box::from([value]),
            )
            .unwrap();
        (value, ty)
    };
    let fragment = builder
        .finish_definition(
            output,
            FragmentSink::Result,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
        )
        .unwrap();
    let result = ResultPort {
        scalar_schema: None,
        fragment: id,
        output: fragment.nodes()[&output].output.clone(),
        fields: if matches!(guard, Guard::Filter) {
            // Filter preserves the complete three-column original input port.
            columns
                .iter()
                .enumerate()
                .map(|(ordinal, (value, ty))| ResultField {
                    domain: crate::test_result_domain::result_value_domain(&ty),
                    name: format!("original_input_{ordinal}").into(),
                    alias: None,
                    value: *value,
                    ty: ty.clone(),
                })
                .collect::<Vec<_>>()
                .into_boxed_slice()
        } else {
            Box::from([ResultField {
                domain: crate::test_result_domain::result_value_domain(&ty),
                name: "original_array_failure".into(),
                alias: None,
                value,
                ty,
            }])
        },
    };
    // The complete dead arithmetic definition retains its explicit original
    // arithmetic fixture policy and semantic parameter, even with demand zero.
    let parameters = if count >= 5
        || matches!(
            source_mode,
            SourceMode::Computed | SourceMode::RequiredError | SourceMode::NestedIf
        ) {
        SemanticParameters::try_new([(
            novarocks_type_contract::SemanticParameterId::new(u32::MAX),
            novarocks_type_contract::SemanticParameterValue::AllowThrowException(true),
        )])
        .unwrap()
    } else {
        SemanticParameters::try_new([]).unwrap()
    };
    compile_checked_fragment_with_parameters(&functions, fragment, &authors, result, parameters)
}
fn input(program: &LocalProgram, flags: Vec<Option<bool>>, root_null: bool) -> RecordBatch {
    let rows = flags.len();
    let struct_field = Arc::new(Field::new("ID", DataType::Int32, true));
    let values = Arc::new(StructArray::new(
        vec![struct_field].into(),
        vec![Arc::new(Int32Array::from(vec![Some(7); rows])) as ArrayRef],
        None,
    )) as ArrayRef;
    let offsets = OffsetBuffer::new(ScalarBuffer::from(
        (0..=rows)
            .map(|r| i32::try_from(r).unwrap())
            .collect::<Vec<_>>(),
    ));
    let declared_type = input_type();
    let DataType::List(item) = declared_type.data_type() else {
        unreachable!()
    };
    let list = Arc::new(ListArray::new(
        item.clone(),
        offsets,
        values,
        root_null.then(|| NullBuffer::new_null(rows)),
    )) as ArrayRef;
    RecordBatch::try_new(
        program.graph().nodes()[1].output_layout().schema().clone(),
        vec![
            list,
            Arc::new(BooleanArray::from(flags)),
            Arc::new(Int64Array::from(vec![Some(1); rows])),
        ],
    )
    .unwrap()
}
fn tracked_instance(program: &Arc<LocalProgram>) -> (CompiledExpressionInstance, Arc<MemTracker>) {
    let tracker = MemTracker::new_root("actual-array-invocation-probe");
    let host =
        crate::exec::operators::compiled_aggregate::expression_allocation_host(tracker.clone());
    (
        CompiledExpressionInstance::try_new_with_allocator(
            program.clone(),
            root(),
            &Control,
            Some(host),
        )
        .unwrap(),
        tracker,
    )
}
fn data(error: ExecutionFailure, message: &str) -> novarocks_functions::ScalarInvocationData {
    let ExecutionFailureCause::ScalarInvocationData(data) = error.cause() else {
        panic!("expected original atomic Data, got {error:?}")
    };
    assert_eq!(data.message(), message);
    data.clone()
}
#[test]
fn scalar_invocation_original_required_full_data_keeps_actual_domain_and_clone_drop() {
    let program = invocation_program(2, Guard::Direct);
    for null in [false, true] {
        let batch = input(&program, vec![Some(true); 3], null);
        let (mut instance, tracker) = tracked_instance(&program);
        let error = crate::exec::operators::compiled_expression::evaluate_all(
            &mut instance,
            root(),
            &batch,
            &crate::exec::operators::compiled_expression::RuntimeKernelControl::new(Arc::default()),
        )
        .unwrap_err();
        let ExecutionFailureCause::ScalarInvocationData(actual) = error.cause() else {
            panic!("required root reclassified original Data: {error:?}")
        };
        assert_eq!(
            actual.message(),
            "__array_struct_subfield field 'id' does not exist"
        );
        assert_eq!(actual.batch_rows(), 3);
        assert_eq!(actual.source_len(), 3);
        assert_eq!(actual.source_row(0), Some(0));
        assert_eq!(actual.source_row(2), Some(2));
        let clone = error.clone();
        assert!(tracker.current() > 0);
        drop(error);
        drop(instance);
        assert!(tracker.current() > 0);
        drop(clone);
        assert_eq!(tracker.current(), 0);
    }
}
#[test]
fn scalar_invocation_noarguments_keeps_full_definition_n_but_invokes_no_children() {
    for count in [3, 5] {
        let program = invocation_program(count, Guard::Direct);
        let batch = input(&program, vec![Some(true); 4], false);
        let (mut instance, _tracker) = tracked_instance(&program);
        let actual = data(
            instance
                .evaluate_evaluation(&batch, Selection::all(4), Activation::Activated, &Control)
                .unwrap_err(),
            &format!("array_struct_subfield expects 2 to 2 arguments, got {count}"),
        );
        assert_eq!(actual.contract().call().logical_argument_count(), count);
        assert_eq!(actual.contract().selected().argument_types.len(), count);
        assert!(actual.contract().value_argument_types().len() == 0);
        assert_eq!(actual.source_len(), 4);
        assert_eq!(
            actual.contract().effects().argument_control,
            novarocks_type_contract::ArgumentControl::NoArguments
        );
        assert_eq!(
            instance.instances.len(),
            1,
            "no tail kernel may be instantiated"
        );
    }
}
#[test]
fn scalar_invocation_actual_empty_if_else_is_activated_and_validation_root_is_not() {
    let program = invocation_program(2, Guard::IfElse);
    let batch = input(&program, vec![], false);
    let (mut instance, _) = tracked_instance(&program);
    data(
        instance
            .evaluate_evaluation(&batch, Selection::all(0), Activation::Activated, &Control)
            .unwrap_err(),
        "__array_struct_subfield field-name argument is empty",
    );
    let (mut structural, _) = tracked_instance(&program);
    let out = structural
        .evaluate_evaluation(
            &batch,
            Selection::all(0),
            Activation::ValidateOnly,
            &Control,
        )
        .unwrap();
    assert_eq!(out.values().len(), 0);
    assert!(out.errors().is_empty());
    assert!(structural.instances.is_empty());
}
#[test]
fn scalar_invocation_guarded_data_is_atomic_and_inactive_branch_never_instantiates() {
    let program = invocation_program(3, Guard::IfThen);
    let inactive = input(&program, vec![Some(false), None, Some(false)], false);
    let (mut instance, _) = tracked_instance(&program);
    let out = instance
        .evaluate_evaluation(
            &inactive,
            Selection::all(3),
            Activation::Activated,
            &Control,
        )
        .unwrap();
    assert!(out.errors().is_empty());
    assert!(instance.instances.is_empty());
    assert_eq!(out.values().null_count(), 3);
    let mixed = input(&program, vec![Some(false), Some(true), None], false);
    let (mut instance, _) = tracked_instance(&program);
    let actual = data(
        instance
            .evaluate_evaluation(&mixed, Selection::all(3), Activation::Activated, &Control)
            .unwrap_err(),
        "array_struct_subfield expects 2 to 2 arguments, got 3",
    );
    assert_eq!(actual.batch_rows(), 3);
    assert_eq!(actual.source_len(), 1);
    assert_eq!(actual.source_row(0), Some(1));
    let after = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
    assert!(matches!(
        instance.evaluate_evaluation(&mixed, Selection::all(3), Activation::Activated, &after),
        Err(error) if matches!(error.cause(), ExecutionFailureCause::Kernel(KernelFailure::InstanceFailed))
    ));
    assert!(after.trace.lock().unwrap().is_empty());
}
#[test]
fn scalar_invocation_old_kernel_adapter_rejects_before_leaf_or_control() {
    let program = invocation_program(3, Guard::Direct);
    let batch = input(&program, vec![Some(true)], false);
    let (mut instance, _) = tracked_instance(&program);
    let control = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
    assert!(matches!(
        instance.evaluate(&batch, Selection::all(1), &control),
        Err(KernelFailure::InvalidProgram(_))
    ));
    assert!(control.trace.lock().unwrap().is_empty());
    assert!(instance.instances.is_empty());
}
#[test]
fn scalar_invocation_every_original_checkpoint_cause_has_no_tail_and_failed_reentry() {
    let program = invocation_program(3, Guard::Direct);
    let batch = input(&program, vec![Some(true); 320], false);
    let trace_control = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
    let (mut instance, _) = tracked_instance(&program);
    data(
        instance
            .evaluate_evaluation(
                &batch,
                Selection::all(320),
                Activation::Activated,
                &trace_control,
            )
            .unwrap_err(),
        "array_struct_subfield expects 2 to 2 arguments, got 3",
    );
    let trace = trace_control.trace.lock().unwrap().clone();
    assert!(!trace.is_empty());
    assert!(trace.iter().all(|n| *n <= 256));
    for stop in 1..=trace.len() {
        for cause in causes() {
            let (mut instance, _) = tracked_instance(&program);
            let control = CallbackControl::new(cause.clone(), stop);
            assert!(
                matches!(instance.evaluate_evaluation(&batch,Selection::all(320),Activation::Activated,&control),Err(error) if matches!(error.cause(), ExecutionFailureCause::Kernel(actual) if actual==&cause))
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..stop]);
            let after = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
            assert!(matches!(
                instance.evaluate_evaluation(
                    &batch,
                    Selection::all(320),
                    Activation::Activated,
                    &after
                ),
                Err(error) if matches!(error.cause(), ExecutionFailureCause::Kernel(KernelFailure::InstanceFailed))
            ));
            assert!(after.trace.lock().unwrap().is_empty());
        }
    }
}

#[test]
fn scalar_invocation_boolean_truth_only_and_filter_keep_atomic_original_data() {
    for guard in [Guard::And, Guard::Or] {
        let program = invocation_program(2, guard);
        let batch = input(&program, vec![Some(true); 3], false);
        let (mut instance, _) = tracked_instance(&program);
        data(
            instance
                .evaluate_evaluation(&batch, Selection::all(3), Activation::Activated, &Control)
                .unwrap_err(),
            "__array_struct_subfield field 'id' does not exist",
        );
    }
    let program = invocation_program(2, Guard::Filter);
    let batch = input(&program, vec![Some(true); 3], false);
    let tracker = MemTracker::new_root("actual-filter-invocation-probe");
    let host = crate::exec::operators::compiled_aggregate::expression_allocation_host(tracker);
    let mut filter = super::super::CompiledFilterConjunctionInstance::try_new_with_allocator(
        program,
        ProgramNodeId::new(2),
        &Control,
        Some(host),
    )
    .unwrap();
    let error = filter
        .evaluate_required(&batch, Selection::all(3), &Control)
        .unwrap_err();
    assert!(
        matches!(error.cause(),ExecutionFailureCause::ScalarInvocationData(data) if data.message()=="__array_struct_subfield field 'id' does not exist")
    );
    let after = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
    let error = filter
        .evaluate_required(&batch, Selection::all(3), &after)
        .unwrap_err();
    assert!(matches!(
        error.cause(),
        ExecutionFailureCause::Kernel(KernelFailure::InstanceFailed)
    ));
    assert!(after.trace.lock().unwrap().is_empty());
}
#[test]
fn scalar_invocation_three_compile_causes_remain_typed_at_actual_declaration_loan() {
    struct Stop(CompileControlError);
    impl PureCompileControl for Stop {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            Err(self.0.clone())
        }
    }
    let functions = actual_functions();
    let program = invocation_program(3, Guard::Direct);
    let call = program
        .checked()
        .channels()
        .expressions()
        .resolved_calls()
        .calls()
        .values()
        .find(|call| {
            call.call_contract().function_id().as_str()
                == "builtin.scalar/__array_struct_subfield/v1"
        })
        .expect("the actual ARRAY call must remain in the frozen source");
    let contract = call.call_contract();
    assert_eq!(
        contract.function_id().as_str(),
        "builtin.scalar/__array_struct_subfield/v1"
    );
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        assert!(
            matches!(functions.metadata().pure_overload_declaration_observed(contract.function_id(),FunctionKind::Scalar,&contract.selected().overload,&Stop(cause.clone())),Err(novarocks_functions::FunctionSpecializationFailure::Control(actual)) if actual==cause)
        );
    }
}

#[test]
fn scalar_invocation_entry_validation_preserves_first_cause_without_footer_or_leaf() {
    let program = invocation_program(3, Guard::Direct);
    let batch = input(&program, vec![Some(true); 3], false);
    let (mut instance, _tracker) = tracked_instance(&program);
    assert!(instance.has_invocation_data);
    let control = CallbackControl::new(KernelFailure::Cancelled, 2);
    let error = instance
        .evaluate_evaluation(&batch, Selection::all(2), Activation::Activated, &control)
        .unwrap_err();
    let ExecutionFailureCause::Kernel(cause) = error.cause() else {
        panic!("input validation must preserve its originating Kernel cause: {error:?}")
    };
    assert_eq!(
        cause,
        &KernelFailure::InvalidProgram(KernelDiagnostic::new(
            "selection differs from actual input batch rows",
        ))
    );
    assert_eq!(*control.trace.lock().unwrap(), vec![0]);
    assert!(instance.instances.is_empty());
    let after = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
    assert!(matches!(
        instance.evaluate_evaluation(&batch, Selection::all(3), Activation::Activated, &after),
        Err(error) if matches!(error.cause(), ExecutionFailureCause::Kernel(KernelFailure::InstanceFailed))
    ));
    assert!(after.trace.lock().unwrap().is_empty());
    assert!(instance.instances.is_empty());
}

// Public production Functions catalogue, checked compiler and the original
// Frame instantiate the new owner. No diagnostic/private replacement owner.
fn original_source_program(field_name: &str, guard: Guard, mode: SourceMode) -> Arc<LocalProgram> {
    let catalog =
        novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
    let mut builder = EngineFunctionCatalogBuilder::new();
    let mut installed = Vec::new();
    for name in ["__array_struct_subfield", "if", "rand"] {
        let definition = catalog.definition(name, FunctionKind::Scalar).unwrap();
        let binding = definition.binding_declaration().unwrap();
        builder.register(definition.clone()).unwrap();
        for overload in binding.overloads() {
            let declaration = catalog
                .pure_overload_declaration_observed(
                    binding.function_id(),
                    definition.kind(),
                    &overload.identity,
                    &Control,
                )
                .unwrap();
            installed.push(InstalledPureKernel {
                function: binding.function_id().clone(),
                kind: definition.kind(),
                implementation: declaration.implementation().clone(),
                aggregate_state_format: None,
            });
        }
    }
    invocation_program_with(
        2,
        guard,
        builder.seal_pure(installed).unwrap(),
        field_name,
        mode,
    )
}

#[test]
fn original_domain_computed_child_covers_actual_three_and_projects_only_outer_two() {
    let program = original_source_program("ID", Guard::Direct, SourceMode::Computed);
    let batch = input(&program, vec![Some(true); 6], false);
    let actual = [1, 3, 5];
    let demand = [3, 5];
    let (mut frame, _) = tracked_instance(&program);
    let output = frame
        .evaluate_scoped_evaluation(
            &batch,
            novarocks_functions::ScalarInvocationScope::new(
                Selection::try_sparse(6, &actual).unwrap(),
                Selection::try_sparse(6, &demand).unwrap(),
            ),
            Activation::Activated,
            &Control,
        )
        .unwrap();
    assert_eq!(output.selection().iter().collect::<Vec<_>>(), vec![3, 5]);
    assert!(output.errors().is_empty());
    let list = output
        .values()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(list.value_offsets(), &[0, 1, 2]);
    assert_eq!(
        list.values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(7), Some(7)]
    );
}

#[test]
fn original_domain_whole_data_uses_actual_three_not_outer_two_and_failed_replay_is_first() {
    let program = original_source_program("id", Guard::Direct, SourceMode::Computed);
    let batch = input(&program, vec![Some(true); 6], false);
    let actual = [1, 3, 5];
    let demand = [3, 5];
    let (mut frame, _) = tracked_instance(&program);
    let error = frame
        .evaluate_scoped_evaluation(
            &batch,
            novarocks_functions::ScalarInvocationScope::new(
                Selection::try_sparse(6, &actual).unwrap(),
                Selection::try_sparse(6, &demand).unwrap(),
            ),
            Activation::Activated,
            &Control,
        )
        .unwrap_err();
    let ExecutionFailureCause::ScalarInvocationData(data) = error.cause() else {
        panic!("original whole Data must remain atomic: {error:?}")
    };
    assert_eq!(
        data.message(),
        "__array_struct_subfield field 'id' does not exist"
    );
    assert_eq!(data.batch_rows(), 6);
    assert_eq!(data.source_len(), 3);
    assert_eq!(
        (0..3).map(|i| data.source_row(i)).collect::<Vec<_>>(),
        vec![Some(1), Some(3), Some(5)]
    );
    let after = CallbackControl::new(KernelFailure::DeadlineExceeded, 0);
    let error = frame
        .evaluate_scoped_evaluation(
            &batch,
            novarocks_functions::ScalarInvocationScope::new(Selection::all(6), Selection::all(6)),
            Activation::Activated,
            &after,
        )
        .unwrap_err();
    assert_eq!(
        error.cause(),
        &ExecutionFailureCause::Kernel(KernelFailure::InstanceFailed)
    );
    assert!(after.trace.lock().unwrap().is_empty());
}

#[test]
fn original_domain_required_child_row_outside_demand_stops_before_parent_instantiation() {
    let program = original_source_program("ID", Guard::Direct, SourceMode::RequiredError);
    let original = input(&program, vec![Some(true); 6], false);
    let mut columns = original.columns().to_vec();
    columns[2] = Arc::new(Int64Array::from(vec![1, 0, 1, 1, 1, 1]));
    let batch = RecordBatch::try_new(original.schema(), columns).unwrap();
    let actual = [1, 3, 5];
    let demand = [3, 5];
    let (mut frame, _) = tracked_instance(&program);
    let error = frame
        .evaluate_scoped_evaluation(
            &batch,
            novarocks_functions::ScalarInvocationScope::new(
                Selection::try_sparse(6, &actual).unwrap(),
                Selection::try_sparse(6, &demand).unwrap(),
            ),
            Activation::Activated,
            &Control,
        )
        .unwrap_err();
    let ExecutionFailureCause::RequiredRow(row) = error.cause() else {
        panic!("required child cannot be parent Data or invariant: {error:?}")
    };
    assert_eq!(row.root(), root());
    assert_eq!(row.batch_row(), 1);
    assert_eq!(row.error().selected_ordinal(), 0);
    assert!(row.error().message().contains("zero"));
    assert!(
        frame.instances.is_empty(),
        "required child failure must precede parent creation/core, not merely suppress its output"
    );
}

fn sliced_full_child_batch(program: &LocalProgram, flags: Vec<Option<bool>>) -> RecordBatch {
    assert_eq!(flags.len(), 2);
    let structure = Arc::new(StructArray::new(
        vec![Arc::new(Field::new("ID", DataType::Int32, true))].into(),
        vec![Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50]))],
        None,
    )) as ArrayRef;
    let DataType::List(item) = input_type().data_type().clone() else {
        unreachable!()
    };
    // These are real sliced offsets and unused children, not a prediction from
    // the implementation's selected output or a normalized fixture.
    let list = Arc::new(ListArray::new(
        item,
        OffsetBuffer::new(ScalarBuffer::from(vec![2i32, 4, 5])),
        structure,
        None,
    ));
    RecordBatch::try_new(
        program.graph().nodes()[1].output_layout().schema().clone(),
        vec![
            list,
            Arc::new(BooleanArray::from(flags)),
            Arc::new(Int64Array::from(vec![1, 1])),
        ],
    )
    .unwrap()
}

#[test]
fn original_domain_if_real_all_true_and_all_false_keep_sliced_full_child_carrier() {
    for (mode, flags) in [
        (SourceMode::IfThen, vec![Some(true), Some(true)]),
        (SourceMode::IfElse, vec![Some(false), Some(false)]),
    ] {
        let program = original_source_program("ID", Guard::Direct, mode);
        let batch = sliced_full_child_batch(&program, flags);
        let (mut frame, _) = tracked_instance(&program);
        let output = frame
            .evaluate_evaluation(&batch, Selection::all(2), Activation::Activated, &Control)
            .unwrap();
        let list = output
            .values()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(
            list.value_offsets(),
            &[2, 4, 5],
            "IF fast path must not turn the original slice into a take/interleave"
        );
        assert_eq!(
            list.values()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(10), Some(20), Some(30), Some(40), Some(50)]
        );
    }
}

#[test]
fn original_domain_if_actual_mixed_mask_filters_even_a_contiguous_true_branch() {
    let program = original_source_program("ID", Guard::Direct, SourceMode::IfThen);
    let batch = sliced_full_child_batch(&program, vec![Some(true), Some(false)]);
    let (mut frame, _) = tracked_instance(&program);
    let output = frame
        .evaluate_evaluation(&batch, Selection::all(2), Activation::Activated, &Control)
        .unwrap();
    let list = output
        .values()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(list.value_offsets(), &[0, 2, 2]);
    assert_eq!(
        list.iter().map(|row| row.is_some()).collect::<Vec<_>>(),
        vec![true, false]
    );
    assert_eq!(
        list.values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(30), Some(40)]
    );
}

#[test]
fn original_domain_nested_if_uses_its_immediate_filtered_parent_rows() {
    let success = original_source_program("ID", Guard::Direct, SourceMode::NestedIf);
    let original = input(
        &success,
        vec![Some(true), Some(false), Some(true), Some(true)],
        false,
    );
    let mut columns = original.columns().to_vec();
    columns[2] = Arc::new(Int64Array::from(vec![1, 1, 0, 1]));
    let batch = RecordBatch::try_new(original.schema(), columns).unwrap();
    let (mut frame, _) = tracked_instance(&success);
    let result = frame
        .evaluate_evaluation(&batch, Selection::all(4), Activation::Activated, &Control)
        .unwrap();
    let list = result
        .values()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(list.value_offsets(), &[0, 1, 1, 1, 2]);
    assert_eq!(
        list.iter().map(|row| row.is_some()).collect::<Vec<_>>(),
        vec![true, false, false, true]
    );
    assert_eq!(
        list.values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(7), Some(7)]
    );

    let program = original_source_program("id", Guard::IfThen, SourceMode::NestedIf);
    let original = input(
        &program,
        vec![Some(true), Some(false), Some(true), Some(true)],
        false,
    );
    let mut columns = original.columns().to_vec();
    columns[2] = Arc::new(Int64Array::from(vec![1, 1, 0, 1]));
    let batch = RecordBatch::try_new(original.schema(), columns).unwrap();
    let (mut frame, _) = tracked_instance(&program);
    let error = frame
        .evaluate_evaluation(&batch, Selection::all(4), Activation::Activated, &Control)
        .unwrap_err();
    let ExecutionFailureCause::ScalarInvocationData(data) = error.cause() else {
        panic!("original whole error: {error:?}")
    };
    // The outer actual IF selects global0/2/3. Its nested IF is evaluated in
    // that same 3-row parent domain; C03 sees the complete required result3.
    assert_eq!(data.source_len(), 3);
    assert_eq!(
        (0..3).map(|i| data.source_row(i)).collect::<Vec<_>>(),
        vec![Some(0), Some(2), Some(3)]
    );
}

#[test]
fn original_domain_mixed_and_selected_root_copies_keep_finite_source_until_last_child_buffer() {
    for selected in [false, true] {
        let program = original_source_program("ID", Guard::Direct, SourceMode::IfThen);
        let batch = sliced_full_child_batch(&program, vec![Some(true), Some(false)]);
        let schema = crate::exec::chunk::ChunkSchema::from_compiled_layout(
            program.graph().nodes()[1].output_layout(),
        )
        .unwrap();
        let mut chunk =
            crate::exec::chunk::Chunk::try_new_with_chunk_schema(batch, schema).unwrap();
        let source = MemTracker::new_root("actual-mixed-source-custody");
        chunk.try_transfer_to_typed(&source).unwrap();
        let bytes = source.current();
        let (mut frame, _) = tracked_instance(&program);
        let selected_rows = [0];
        let rows = if selected {
            Selection::try_sparse(2, &selected_rows).unwrap()
        } else {
            Selection::all(2)
        };
        let result = frame
            .evaluate_input_controlled::<ExecutionFailure>(
                super::super::input_source::CompiledInputLoan::from_chunk(&chunk),
                rows,
                rows,
                Activation::Activated,
                &Control,
                None,
            )
            .unwrap();
        let list = result
            .values()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(
            list.values()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(30), Some(40)]
        );
        assert_eq!(
            list.value_offsets(),
            if selected {
                &[0, 2][..]
            } else {
                &[0, 2, 2][..]
            }
        );
        let last = list.values().to_data().buffers()[0].clone();
        drop((result, chunk, frame));
        assert_eq!(
            source.current(),
            bytes,
            "copied/assembled output must retain the original finite chain"
        );
        std::thread::spawn(move || drop(last)).join().unwrap();
        assert_eq!(source.current(), 0);
        assert_eq!(source.allocated(), source.deallocated());
    }
}

#[derive(Default)]
struct ActualScopeSpy {
    selected: Vec<Vec<usize>>,
    scoped: Vec<(Vec<usize>, Vec<usize>)>,
}
impl crate::runtime::scalar_memory::RuntimeScalarOperationScope for ActualScopeSpy {
    fn evaluate<'a>(
        &mut self,
        instance: &mut novarocks_functions::ScalarEvaluationInstance,
        selection: Selection<'a>,
        arguments: &'a [novarocks_functions::EvaluatedArgument<'a>],
        control: &dyn KernelEvaluationControl,
    ) -> Result<
        novarocks_functions::SelectedValues<'a>,
        crate::runtime::scalar_memory::RuntimeScalarEvaluationFailure,
    > {
        self.selected.push(selection.iter().collect());
        instance
            .evaluate(selection, arguments, control)
            .map_err(Into::into)
    }
    fn evaluate_invocation<'a>(
        &mut self,
        instance: &mut novarocks_functions::InvocationScalarEvaluationInstance,
        selection: Selection<'a>,
        arguments: &'a [novarocks_functions::EvaluatedArgument<'a>],
        activation: Activation,
        _allocator: Option<&Arc<dyn novarocks_functions::AggregateStateAllocator>>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<
        novarocks_functions::SelectedValues<'a>,
        crate::runtime::scalar_memory::RuntimeScalarEvaluationFailure,
    > {
        self.selected.push(selection.iter().collect());
        instance
            .evaluate(selection, arguments, activation, control)
            .map_err(Into::into)
    }
    fn evaluate_scoped_invocation<'a>(
        &mut self,
        instance: &mut novarocks_functions::InvocationScalarEvaluationInstance,
        scope: novarocks_functions::ScalarInvocationScope<'a>,
        sources: &'a [novarocks_functions::ScalarInvocationSource<'a>],
        activation: Activation,
        _allocator: Option<&Arc<dyn novarocks_functions::AggregateStateAllocator>>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<
        novarocks_functions::SelectedValues<'a>,
        crate::runtime::scalar_memory::RuntimeScalarEvaluationFailure,
    > {
        self.scoped.push((
            scope.actual_domain().iter().collect(),
            scope.demand().iter().collect(),
        ));
        instance
            .evaluate_scoped(scope, sources, activation, control)
            .map_err(Into::into)
    }
}

#[test]
fn original_domain_policy_does_not_expand_old_volatile_if_inside_coalesce() {
    let program = super::program(super::Shape::Random);
    let batch = super::batch(&program, vec![Some(true); 3], vec![Some(42); 3]);
    let mut frame = super::instance(&program);
    let demand = [0, 2];
    let mut spy = ActualScopeSpy::default();
    let output = frame
        .evaluate_scoped_runtime(
            &batch,
            novarocks_functions::ScalarInvocationScope::new(
                Selection::all(3),
                Selection::try_sparse(3, &demand).unwrap(),
            ),
            Activation::Activated,
            &Control,
            &mut spy,
        )
        .unwrap();
    assert_eq!(output.selection().iter().collect::<Vec<_>>(), vec![0, 2]);
    assert_eq!(
        spy.selected,
        vec![vec![0, 2]],
        "real old RAND must evaluate only the two demanded Coalesce/IF rows"
    );
    assert!(spy.scoped.is_empty());
}

#[test]
fn original_domain_policy_conditional_mask_uses_actual_parent_not_outer_demand() {
    let program = original_source_program("id", Guard::IfThen, SourceMode::Computed);
    let batch = input(
        &program,
        vec![
            Some(false),
            Some(true),
            Some(false),
            Some(false),
            Some(false),
            Some(true),
        ],
        false,
    );
    let actual = [1, 3, 5];
    let demand = [3, 5];
    let (mut frame, _) = tracked_instance(&program);
    let mut spy = ActualScopeSpy::default();
    let error = frame
        .evaluate_scoped_runtime(
            &batch,
            novarocks_functions::ScalarInvocationScope::new(
                Selection::try_sparse(6, &actual).unwrap(),
                Selection::try_sparse(6, &demand).unwrap(),
            ),
            Activation::Activated,
            &Control,
            &mut spy,
        )
        .unwrap_err();
    assert_eq!(
        spy.scoped,
        vec![(vec![1, 5], vec![1, 5])],
        "typed original owner needs the actual IF true-mask, including global1 outside output demand"
    );
    let ExecutionFailureCause::ScalarInvocationData(data) = error.cause() else {
        panic!("real original whole Data: {error:?}")
    };
    assert_eq!(data.source_len(), 2);
    assert_eq!((data.source_row(0), data.source_row(1)), (Some(1), Some(5)));
}
