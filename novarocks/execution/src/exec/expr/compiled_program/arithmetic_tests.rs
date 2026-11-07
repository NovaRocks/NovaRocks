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

// Child of the real guarded-owner fixture module. All arithmetic is authored
// by the production recipe and executed through the checked local compiler.
use super::*;
use arrow::array::{ArrayRef, Int8Array, Int16Array, Int32Array};
use novarocks_functions::PreparedArithmeticRecipe;
use novarocks_local_program::{ProgramUseRef, StaticExprKind};
use novarocks_physical_plan::BinaryOperator;
use novarocks_type_contract::{
    ArithmeticOperator, SemanticParameterId, SemanticParameterKey, SemanticParameterRef,
    SemanticParameterValue, arithmetic_result_value_type_with_op,
};

#[derive(Clone, Copy, Debug)]
enum Wrap {
    Bare,
    IsNull,
    Coalesce,
    If,
    And,
    Or,
    NullParent,
    ConstantRight,
}
fn operation(op: BinaryOperator) -> ArithmeticOperator {
    match op {
        BinaryOperator::Add => ArithmeticOperator::Add,
        BinaryOperator::Subtract => ArithmeticOperator::Subtract,
        BinaryOperator::Multiply => ArithmeticOperator::Multiply,
        BinaryOperator::Divide => ArithmeticOperator::Divide,
        BinaryOperator::Modulo => ArithmeticOperator::Modulo,
        _ => panic!("exact five arithmetic operators only"),
    }
}
fn allow_ref() -> SemanticParameterRef {
    SemanticParameterRef {
        id: SemanticParameterId::new(u32::MAX),
        expected_key: SemanticParameterKey::AllowThrowException,
    }
}
fn literal(builder: &mut FragmentBuilder, ty: &FunctionValueType, value: LiteralValue) -> ExprId {
    builder
        .add_expression(NodeId::new(0), ty.clone(), ExprKind::Literal(value))
        .unwrap()
}
fn arithmetic(
    builder: &mut FragmentBuilder,
    op: BinaryOperator,
    left: ExprId,
    right: ExprId,
    result: &FunctionValueType,
) -> ExprId {
    builder
        .add_expression(
            NodeId::new(0),
            result.clone(),
            ExprKind::Binary {
                op,
                left,
                right,
                decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                allow_throw_exception: Some(allow_ref()),
            },
        )
        .unwrap()
}

fn compile_arithmetic(
    functions: &PureEngineFunctionCatalog,
    fragment: Fragment,
    authors: &BTreeMap<ExprId, Author>,
    result: ResultPort,
    allow: bool,
) -> Arc<LocalProgram> {
    let (fragment, constants) = original_request_sources(fragment, authors);
    let fragment_id = fragment.id();
    let roots = PhysicalExpressionRoots::try_new(&fragment, &Control).unwrap();
    let mut author = FlowAuthor::new();
    let mut bindings = Vec::new();
    for (&site, root) in roots.sites() {
        bindings.push((
            site,
            author.visit(
                &fragment,
                authors,
                root.expr,
                EvaluationDomainId::new(u32::MAX),
                root.demand,
            ),
        ));
    }
    let ordered = author.uses;
    let flow = ExpressionControlFlow::try_new(
        author.domains,
        ordered.clone(),
        fragment.expressions(),
        CompilePhase::Validate,
        &Control,
    )
    .unwrap();
    let uses = PhysicalRootUses::try_new(&fragment, flow.clone(), bindings, &Control).unwrap();
    let parameters = SemanticParameters::try_new([(
        allow_ref().id,
        SemanticParameterValue::AllowThrowException(allow),
    )])
    .unwrap();
    let mut summaries = BTreeMap::new();
    let mut frozen = Vec::new();
    for invocation in ordered {
        let context = invocation.context;
        let definition = fragment.expressions().get(invocation.definition).unwrap();
        let mut children = ScopedExpressionEffects::pure_value(context);
        for (ordinal, child) in invocation.arguments.iter().enumerate() {
            children = children
                .join_control_argument(summaries[child], &flow, ordinal)
                .unwrap();
        }
        let summary = if let Some(owner) = authors.get(&invocation.definition) {
            let argument_uses = invocation
                .arguments
                .iter()
                .map(|id| Some(*id))
                .collect::<Vec<_>>();
            let token = functions
                .prepare_fresh(
                    CallEffectInput {
                        context,
                        argument_uses: novarocks_functions::CallArgumentUses::SelectedChannels(
                            &argument_uses,
                        ),
                        function_id: &owner.function.function_id,
                        kind: FunctionKind::Scalar,
                        selected: owner.selected.as_ref(),
                        request: owner.request(),
                        environment: &[],
                        parameters: &parameters,
                        decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                        proof_scope: CallProofScope::Domain(context.domain),
                    },
                    owner.selected.clone(),
                    if matches!(owner.shape, ControlShape::If | ControlShape::Coalesce) {
                        PureCallPreparation::ControlIntrinsic {
                            arguments: children,
                        }
                    } else {
                        PureCallPreparation::Scalar {
                            arguments: children,
                        }
                    },
                    &Control,
                )
                .unwrap();
            frozen.push(FrozenPhysicalCall {
                materialized_writer_input: None,
                regexp_count_pattern_source: None,
                to_base64_byte_source: None,
                temporal_source: None,
                site: PhysicalCallSite::Expression(context.use_id),
                context,
                effects: token.call_contract().effects().clone(),
                decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
            });
            token.effects()
        } else if let ExprKind::Binary {
            op,
            left,
            right,
            decimal_overflow_policy,
            allow_throw_exception: Some(reference),
        } = &definition.kind
        {
            let value = parameters.require(*reference).unwrap();
            let SemanticParameterValue::AllowThrowException(allow) = value else {
                panic!("exact intrinsic parameter key")
            };
            let recipe = PreparedArithmeticRecipe::try_new(
                operation(*op),
                &fragment.expressions().get(*left).unwrap().ty,
                &fragment.expressions().get(*right).unwrap().ty,
                &definition.ty,
                *decimal_overflow_policy,
                *allow,
                &Control,
            )
            .unwrap();
            recipe
                .own_effects(context)
                .join_same_domain(children)
                .unwrap()
        } else {
            children
        };
        summaries.insert(context.use_id, summary);
    }
    let calls = FrozenFragmentCalls::try_new(&fragment, &uses, frozen, &Control).unwrap();
    let package = Arc::new(
        FragmentPackage::try_new(
            FragmentPackageInput {
                version: PlanVersionId::try_new([213; 16]).unwrap(),
                required: RequiredContracts::default(),
                constants,
                fragment,
                expression_uses: uses,
                calls,
                pruning: FrozenFragmentPruning::try_new(fragment_id, Vec::new(), &Control).unwrap(),
                cuts: FragmentCuts::default(),
                result: Some(result),
                parameters,
                scans: BTreeMap::new(),
                writes: BTreeMap::new(),
                annotations: Box::default(),
            },
            package_admission(),
            &Control,
        )
        .unwrap(),
    );
    let providers =
        PureProviderProgramCatalog::<std::io::Error>::try_new(&[], Vec::new(), &Control).unwrap();
    Arc::new(
        compile_fragment(
            validate_fragment_providers(package, &providers, &Control).unwrap(),
            functions,
            options(),
            &Control,
        )
        .unwrap(),
    )
}

fn build_program(
    op: BinaryOperator,
    left_carrier: DataType,
    right_carrier: DataType,
    wrap: Wrap,
    allow: bool,
) -> Arc<LocalProgram> {
    let functions = catalogue(Shape::Decimal);
    let fragment_id = FragmentId::new(213);
    let source = NodeId::new(u32::MAX);
    let input = NodeId::new(41);
    let output = NodeId::new(0);
    let left_type = FunctionValueType::new(left_carrier, true);
    let right_type = FunctionValueType::new(right_carrier, true);
    let boolean = FunctionValueType::new(DataType::Boolean, true);
    let columns = [
        (ValueId::new(91), left_type.clone()),
        (ValueId::new(7), right_type.clone()),
        (ValueId::new(333), boolean.clone()),
    ];
    let mut builder = FragmentBuilder::new(fragment_id);
    builder
        .add_values(source, Box::from([Box::default()]), Box::default())
        .unwrap();
    let mut projections = Vec::new();
    for (id, ty) in &columns {
        let expression = builder
            .add_expression(input, ty.clone(), ExprKind::Literal(LiteralValue::Null))
            .unwrap();
        builder
            .insert_value(ValueDef {
                id: *id,
                ty: ty.clone(),
                origin: ValueOrigin::Expr {
                    node: input,
                    expr: expression,
                },
            })
            .unwrap();
        projections.push((expression, *id));
    }
    builder
        .add_project(
            input,
            source,
            projections.into_boxed_slice(),
            Box::from([columns[0].0, columns[1].0, columns[2].0]),
        )
        .unwrap();
    let left = builder
        .add_expression(output, left_type.clone(), ExprKind::Value(columns[0].0))
        .unwrap();
    let right = if matches!(wrap, Wrap::ConstantRight) {
        literal(&mut builder, &right_type, LiteralValue::Int64(2))
    } else {
        builder
            .add_expression(output, right_type.clone(), ExprKind::Value(columns[1].0))
            .unwrap()
    };
    let mut result_type =
        arithmetic_result_value_type_with_op(&left_type, &right_type, operation(op)).unwrap();
    result_type = result_type
        .with_nullable(true)
        .expect("valid test root nullability");
    let computed = arithmetic(&mut builder, op, left, right, &result_type);
    let mut authors = BTreeMap::new();
    let root = match wrap {
        Wrap::Bare | Wrap::ConstantRight => computed,
        Wrap::NullParent => {
            let null = literal(&mut builder, &result_type, LiteralValue::Null);
            arithmetic(
                &mut builder,
                BinaryOperator::Add,
                null,
                computed,
                &result_type,
            )
        }
        Wrap::Coalesce | Wrap::If => {
            let fallback = literal(&mut builder, &result_type, LiteralValue::Int64(71));
            let (name, shape, arguments, defs) = if matches!(wrap, Wrap::If) {
                let flag = builder
                    .add_expression(output, boolean.clone(), ExprKind::Value(columns[2].0))
                    .unwrap();
                (
                    "if",
                    ControlShape::If,
                    vec![
                        argument(boolean, None),
                        argument(result_type.clone(), None),
                        argument(result_type.clone(), None),
                    ],
                    vec![flag, computed, fallback],
                )
            } else {
                (
                    "coalesce",
                    ControlShape::Coalesce,
                    vec![
                        argument(result_type.clone(), None),
                        argument(result_type.clone(), None),
                    ],
                    vec![computed, fallback],
                )
            };
            call(
                &mut builder,
                &mut authors,
                author(&functions, name, arguments, shape),
                defs,
            )
        }
        Wrap::IsNull | Wrap::And | Wrap::Or => {
            result_type = FunctionValueType::new(DataType::Boolean, false);
            let is_null = builder
                .add_expression(
                    output,
                    result_type.clone(),
                    ExprKind::IsNull {
                        expr: computed,
                        negated: false,
                    },
                )
                .unwrap();
            if matches!(wrap, Wrap::IsNull) {
                is_null
            } else {
                let decide = literal(
                    &mut builder,
                    &result_type,
                    LiteralValue::Boolean(matches!(wrap, Wrap::Or)),
                );
                builder
                    .add_expression(
                        output,
                        result_type.clone(),
                        if matches!(wrap, Wrap::Or) {
                            ExprKind::Disjunction {
                                args: Box::from([decide, is_null]),
                            }
                        } else {
                            ExprKind::Conjunction {
                                args: Box::from([decide, is_null]),
                            }
                        },
                    )
                    .unwrap()
            }
        }
    };
    let value = builder
        .add_value(
            result_type.clone(),
            ValueOrigin::Expr {
                node: output,
                expr: root,
            },
        )
        .unwrap();
    builder
        .add_project(
            output,
            input,
            Box::from([(root, value)]),
            Box::from([value]),
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
            name: "arithmetic_result".into(),
            alias: None,
            value,
            ty: result_type,
        }]),
    };
    compile_arithmetic(&functions, fragment, &authors, result, allow)
}
fn signed(carrier: &DataType, values: &[Option<i64>]) -> ArrayRef {
    match carrier {
        DataType::Int8 => Arc::new(Int8Array::from_iter(
            values.iter().map(|v| v.map(|v| i8::try_from(v).unwrap())),
        )),
        DataType::Int16 => Arc::new(Int16Array::from_iter(
            values.iter().map(|v| v.map(|v| i16::try_from(v).unwrap())),
        )),
        DataType::Int32 => Arc::new(Int32Array::from_iter(
            values.iter().map(|v| v.map(|v| i32::try_from(v).unwrap())),
        )),
        DataType::Int64 => Arc::new(Int64Array::from(values.to_vec())),
        _ => panic!("signed fixture only"),
    }
}
fn input(
    program: &LocalProgram,
    left: &[Option<i64>],
    right: &[Option<i64>],
    flags: &[Option<bool>],
) -> RecordBatch {
    let schema = program.graph().nodes()[1].output_layout().schema().clone();
    let left = signed(schema.field(0).data_type(), left);
    let right = signed(schema.field(1).data_type(), right);
    RecordBatch::try_new(
        schema,
        vec![left, right, Arc::new(BooleanArray::from(flags.to_vec()))],
    )
    .unwrap()
}
fn integral(output: &novarocks_functions::SelectedValues<'_>) -> Vec<Option<i64>> {
    let array = output.values();
    match array.data_type() {
        DataType::Int8 => array
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap()
            .iter()
            .map(|v| v.map(i64::from))
            .collect(),
        DataType::Int16 => array
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap()
            .iter()
            .map(|v| v.map(i64::from))
            .collect(),
        DataType::Int32 => array
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .map(|v| v.map(i64::from))
            .collect(),
        DataType::Int64 => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect(),
        _ => panic!("exact frozen integral output"),
    }
}
fn errors(output: &novarocks_functions::SelectedValues<'_>) -> Vec<usize> {
    output
        .errors()
        .iter()
        .map(|error| error.selected_ordinal())
        .collect()
}

#[test]
fn five_signed_operators_keep_frozen_mixed_widths_and_sparse_selected_addresses() {
    for (left, right, expected_carrier) in [
        (DataType::Int8, DataType::Int8, DataType::Int8),
        (DataType::Int16, DataType::Int8, DataType::Int16),
        (DataType::Int16, DataType::Int32, DataType::Int32),
        (DataType::Int32, DataType::Int64, DataType::Int64),
    ] {
        for (op, integral_expected) in [
            (BinaryOperator::Add, vec![Some(4), Some(-4), None]),
            (BinaryOperator::Subtract, vec![Some(10), Some(-10), None]),
            (BinaryOperator::Multiply, vec![Some(-21), Some(-21), None]),
            (BinaryOperator::Divide, vec![Some(-2), Some(-2), None]),
            (BinaryOperator::Modulo, vec![Some(1), Some(-1), None]),
        ] {
            let program = build_program(op, left.clone(), right.clone(), Wrap::Bare, true);
            let batch = input(
                &program,
                &[Some(99), Some(7), Some(-7), None, Some(8)],
                &[None, Some(-3), Some(3), Some(0), Some(2)],
                &[None; 5],
            );
            let rows = [1, 2, 3];
            let selection = Selection::try_sparse(5, &rows).unwrap();
            let output = instance(&program)
                .evaluate(&batch, selection, &Control)
                .unwrap();
            assert_eq!(output.selection(), selection);
            assert_eq!(output.values().data_type(), &expected_carrier);
            assert_eq!(integral(&output), integral_expected);
            assert!(output.errors().is_empty());
            let resolved = program
                .checked()
                .channels()
                .expressions()
                .resolved_calls()
                .snapshot();
            let site = root();
            let occurrence = ProgramUseRef {
                arena: site.arena(),
                use_id: resolved.bindings()[&site],
            };
            let recipe = program.arithmetic_recipe(occurrence).unwrap();
            assert_eq!(recipe.operator(), operation(op));
            assert_eq!(recipe.left_type().data_type().clone(), left);
            assert_eq!(recipe.right_type().data_type().clone(), right);
            assert!(recipe.allow_throw_exception());
            assert_eq!(
                recipe.decimal_overflow_policy(),
                DecimalOverflowPolicy::ReportError
            );
            assert!(matches!(
                resolved.roots().arenas()[&site.arena()]
                    .node(resolved.flows()[&site.arena()].uses()[&occurrence.use_id].definition)
                    .unwrap()
                    .kind(),
                StaticExprKind::PreparedArithmetic { .. }
            ));
        }
    }
}

#[test]
fn signed_overflow_and_modulo_faults_use_selected_row_errors_independent_of_allow_flag() {
    for allow in [false, true] {
        for (op, left, right, expected, error_ordinals) in [
            (
                BinaryOperator::Add,
                vec![Some(8), Some(i64::MAX), None, Some(i64::MIN), Some(7)],
                vec![Some(9), Some(1), Some(1), Some(-1), Some(-3)],
                vec![None, None, None, Some(4)],
                vec![0, 2],
            ),
            (
                BinaryOperator::Subtract,
                vec![Some(8), Some(i64::MIN), None, Some(i64::MAX), Some(7)],
                vec![Some(9), Some(1), Some(1), Some(-1), Some(-3)],
                vec![None, None, None, Some(10)],
                vec![0, 2],
            ),
            (
                BinaryOperator::Multiply,
                vec![Some(8), Some(i64::MAX), None, Some(i64::MIN), Some(7)],
                vec![Some(9), Some(2), Some(2), Some(-1), Some(-3)],
                vec![None, None, None, Some(-21)],
                vec![0, 2],
            ),
            (
                BinaryOperator::Modulo,
                vec![Some(8), Some(i64::MIN), None, Some(7), Some(7)],
                vec![Some(9), Some(-1), Some(0), Some(0), Some(-3)],
                vec![Some(0), None, None, Some(1)],
                vec![2],
            ),
        ] {
            let program = build_program(op, DataType::Int64, DataType::Int64, Wrap::Bare, allow);
            let batch = input(&program, &left, &right, &[None; 5]);
            let rows = [1, 2, 3, 4];
            let output = instance(&program)
                .evaluate(&batch, Selection::try_sparse(5, &rows).unwrap(), &Control)
                .unwrap();
            assert_eq!(integral(&output), expected);
            assert_eq!(errors(&output), error_ordinals);
            assert!(
                output
                    .errors()
                    .iter()
                    .all(|error| !error.message().is_empty())
            );
        }
        let program = build_program(
            BinaryOperator::Divide,
            DataType::Int64,
            DataType::Int64,
            Wrap::Bare,
            allow,
        );
        let batch = input(
            &program,
            &[Some(i64::MIN), Some(7), None, Some(7)],
            &[Some(-1), Some(0), Some(0), None],
            &[None; 4],
        );
        let output = instance(&program)
            .evaluate(&batch, Selection::all(4), &Control)
            .unwrap();
        assert_eq!(integral(&output), vec![None, None, None, None]);
        assert_eq!(errors(&output), vec![0, 1]);
        assert_eq!(
            output.errors()[0].reason(),
            novarocks_functions::DataFailureReason::NumericOutOfRange
        );
        assert_eq!(
            output.errors()[1].reason(),
            novarocks_functions::DataFailureReason::DivisionByZero
        );
    }
}

#[test]
fn narrow_signed_boundaries_reach_original_selected_error_journal() {
    for (carrier, min, max) in [
        (DataType::Int8, i64::from(i8::MIN), i64::from(i8::MAX)),
        (DataType::Int16, i64::from(i16::MIN), i64::from(i16::MAX)),
        (DataType::Int32, i64::from(i32::MIN), i64::from(i32::MAX)),
    ] {
        for allow in [false, true] {
            for (op, left, right, reason) in [
                (
                    BinaryOperator::Add,
                    max,
                    1,
                    novarocks_functions::DataFailureReason::NumericOutOfRange,
                ),
                (
                    BinaryOperator::Subtract,
                    min,
                    1,
                    novarocks_functions::DataFailureReason::NumericOutOfRange,
                ),
                (
                    BinaryOperator::Multiply,
                    max,
                    2,
                    novarocks_functions::DataFailureReason::NumericOutOfRange,
                ),
                (
                    BinaryOperator::Divide,
                    min,
                    -1,
                    novarocks_functions::DataFailureReason::NumericOutOfRange,
                ),
                (
                    BinaryOperator::Divide,
                    7,
                    0,
                    novarocks_functions::DataFailureReason::DivisionByZero,
                ),
                (
                    BinaryOperator::Modulo,
                    7,
                    0,
                    novarocks_functions::DataFailureReason::DivisionByZero,
                ),
            ] {
                let program =
                    build_program(op, carrier.clone(), carrier.clone(), Wrap::Bare, allow);
                let batch = input(
                    &program,
                    &[Some(2), Some(left), None],
                    &[Some(1), Some(right), Some(0)],
                    &[None; 3],
                );
                let selected_rows = [1, 2];
                let selection = Selection::try_sparse(3, &selected_rows).unwrap();
                let output = instance(&program)
                    .evaluate(&batch, selection, &Control)
                    .unwrap();
                assert_eq!(output.values().data_type(), &carrier);
                assert_eq!(output.selection(), selection);
                assert_eq!(integral(&output), vec![None, None]);
                assert_eq!(errors(&output), vec![0]);
                assert_eq!(output.errors()[0].reason(), reason);
            }
        }
    }
}

#[test]
fn literal_broadcast_and_sliced_columns_use_real_compiled_constant_and_original_rows() {
    let program = build_program(
        BinaryOperator::Add,
        DataType::Int64,
        DataType::Int64,
        Wrap::ConstantRight,
        false,
    );
    let backing: ArrayRef = Arc::new(Int64Array::from(vec![
        Some(i64::MAX),
        Some(7),
        None,
        Some(-5),
        Some(i64::MAX),
    ]));
    let schema = program.graph().nodes()[1].output_layout().schema().clone();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            backing.slice(1, 3),
            Arc::new(Int64Array::from(vec![Some(i64::MAX); 3])),
            Arc::new(BooleanArray::from(vec![None; 3])),
        ],
    )
    .unwrap();
    let rows = [0, 2];
    let output = instance(&program)
        .evaluate(&batch, Selection::try_sparse(3, &rows).unwrap(), &Control)
        .unwrap();
    assert_eq!(integral(&output), vec![Some(9), Some(-3)]);
    assert!(output.errors().is_empty());
}

#[test]
fn eager_required_child_error_survives_parent_strict_null_and_is_null_or_coalesce() {
    for wrap in [Wrap::NullParent, Wrap::IsNull, Wrap::Coalesce] {
        let program = build_program(
            BinaryOperator::Add,
            DataType::Int64,
            DataType::Int64,
            wrap,
            true,
        );
        let batch = input(
            &program,
            &[Some(i64::MAX), None, Some(7)],
            &[Some(1), Some(0), Some(3)],
            &[None; 3],
        );
        let output = instance(&program)
            .evaluate(&batch, Selection::all(3), &Control)
            .unwrap();
        assert_eq!(errors(&output), vec![0]);
        match wrap {
            Wrap::NullParent => assert_eq!(integral(&output), vec![None, None, None]),
            Wrap::Coalesce => assert_eq!(integral(&output), vec![None, Some(71), Some(10)]),
            Wrap::IsNull => assert_eq!(
                output
                    .values()
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
                vec![None, Some(true), Some(false)]
            ),
            _ => unreachable!(),
        }
    }
}

#[test]
fn actual_if_and_pure_boolean_domains_control_arithmetic_error_demand() {
    let program = build_program(
        BinaryOperator::Add,
        DataType::Int64,
        DataType::Int64,
        Wrap::If,
        true,
    );
    let batch = input(
        &program,
        &[Some(i64::MAX), Some(i64::MAX), None, Some(7)],
        &[Some(1), Some(1), Some(0), Some(3)],
        &[Some(false), Some(true), Some(true), None],
    );
    let output = instance(&program)
        .evaluate(&batch, Selection::all(4), &Control)
        .unwrap();
    assert_eq!(integral(&output), vec![Some(71), None, None, Some(71)]);
    assert_eq!(errors(&output), vec![1]);
    for wrap in [Wrap::And, Wrap::Or] {
        let program = build_program(
            BinaryOperator::Add,
            DataType::Int64,
            DataType::Int64,
            wrap,
            true,
        );
        let batch = input(
            &program,
            &[Some(i64::MAX), None, Some(7)],
            &[Some(1), Some(0), Some(3)],
            &[None; 3],
        );
        let output = instance(&program)
            .evaluate(&batch, Selection::all(3), &Control)
            .unwrap();
        assert_eq!(
            output
                .values()
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(matches!(wrap, Wrap::Or)); 3]
        );
        assert!(output.errors().is_empty());
    }
}

#[test]
fn actual_arithmetic_rows_preserve_primary_controls_every_quantum_and_failed_instance_latch() {
    let program = build_program(
        BinaryOperator::Add,
        DataType::Int64,
        DataType::Int64,
        Wrap::Coalesce,
        false,
    );
    let left = (0..320)
        .map(|row| match row % 3 {
            0 => Some(i64::MAX),
            1 => None,
            _ => Some(7),
        })
        .collect::<Vec<_>>();
    let batch = input(&program, &left, &[Some(1); 320], &[None; 320]);
    let recorder = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
    let output = instance(&program)
        .evaluate(&batch, Selection::all(320), &recorder)
        .unwrap();
    assert_eq!(errors(&output).len(), 107);
    let trace = recorder.trace.lock().unwrap().clone();
    assert!(trace.contains(&256));
    assert!(trace.iter().all(|units| *units <= 256));
    for stop_at in 1..=trace.len() {
        for cause in causes() {
            let mut evaluator = instance(&program);
            let control = CallbackControl::new(cause.clone(), stop_at);
            assert!(
                matches!(evaluator.evaluate(&batch, Selection::all(320), &control), Err(actual) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..stop_at]);
            let after = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
            assert!(matches!(
                evaluator.evaluate(&batch, Selection::all(320), &after),
                Err(KernelFailure::InstanceFailed)
            ));
            assert!(after.trace.lock().unwrap().is_empty());
        }
    }
}

#[path = "numeric_arithmetic_tests.rs"]
mod numeric_arithmetic_tests;

#[path = "cast_tests.rs"]
mod cast_tests;

#[path = "nullsafe_tests.rs"]
mod nullsafe_tests;

// Conservative retained-source invoice and independent projection ceilings for
// these small fixtures only; this is not a production default or a MEM grant.
fn package_admission() -> novarocks_physical_plan::FragmentPackageAdmission {
    novarocks_physical_plan::FragmentPackageAdmission {
        plan_limits: novarocks_physical_plan::PlanLimits::FROZEN,
        source_retained_bytes: 64 * 1024 * 1024,
        property_projection_limits: novarocks_physical_plan::PropertyProofProjectionLimits {
            max_request_bytes: 16 * 1024 * 1024,
            max_coexisting_bytes: 256 * 1024 * 1024,
            max_projection_work: 16 * 1024 * 1024,
        },
    }
}

#[path = "float_arithmetic_required_frame_tests.rs"]
mod float_required;
