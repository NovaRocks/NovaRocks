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
use crate::runtime::fragment::ExecutionFailureCause;
use crate::runtime::mem_tracker::MemTracker;
use arrow::array::{ArrayRef, Int32Array, ListArray, StructArray};
use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::Field;
use novarocks_functions::{ScalarInvocationActivation as Activation, ScalarInvocationFailure};

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
    let functions = actual_functions();
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
    let data = builder
        .add_expression(output, source_type.clone(), ExprKind::Value(columns[0].0))
        .unwrap();
    let name_type = FunctionValueType::new(DataType::Utf8, false);
    let name = ConstantValue::from_utf8(
        Arc::new(name_type.try_to_field("actual field name").unwrap()),
        name_type.clone(),
        "id",
        options().constants,
        CompilePhase::Validate,
        &Control,
    )
    .unwrap();
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
    let mut arguments = vec![argument(source_type, None), argument(name_type, Some(name))];
    let mut definitions = vec![data, name_expr];
    // Complete bound volatile/math tail definitions are retained. The first
    // real RAND would create a mutable instance if demanded. Other tails retain
    // original divide-zero semantics; their value is never evaluated here.
    let mut authors = BTreeMap::new();
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
    let parameters = if count >= 5 {
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
    let DataType::List(item) = input_type().data_type else {
        unreachable!()
    };
    let list = Arc::new(ListArray::new(
        item,
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
fn data(
    error: ScalarInvocationFailure,
    message: &str,
) -> novarocks_functions::ScalarInvocationData {
    let ScalarInvocationFailure::Data(data) = error else {
        panic!("expected original atomic Data, got {error:?}")
    };
    assert_eq!(data.message(), message);
    data
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
        Err(ScalarInvocationFailure::Kernel(
            KernelFailure::InstanceFailed
        ))
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
                matches!(instance.evaluate_evaluation(&batch,Selection::all(320),Activation::Activated,&control),Err(ScalarInvocationFailure::Kernel(actual)) if actual==cause)
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
                Err(ScalarInvocationFailure::Kernel(
                    KernelFailure::InstanceFailed
                ))
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
    let ScalarInvocationFailure::Kernel(cause) = error else {
        panic!("input validation must preserve its originating Kernel cause: {error:?}")
    };
    assert_eq!(
        cause,
        KernelFailure::InvalidProgram(KernelDiagnostic::new(
            "selection differs from actual input batch rows",
        ))
    );
    assert_eq!(*control.trace.lock().unwrap(), vec![0]);
    assert!(instance.instances.is_empty());
    let after = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
    assert!(matches!(
        instance.evaluate_evaluation(&batch, Selection::all(3), Activation::Activated, &after),
        Err(ScalarInvocationFailure::Kernel(
            KernelFailure::InstanceFailed
        ))
    ));
    assert!(after.trace.lock().unwrap().is_empty());
    assert!(instance.instances.is_empty());
}
