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

// Public recipe checks reuse the actual frozen physical compiler fixture.
// Reuse only its real physical authors, frozen package and compiler fixture.
use super::*;
use novarocks_expression::{
    PrimitiveError as ProgramPrimitiveError, prepare_arithmetic_node, prepare_cast_node,
    prepare_comparison_node, prepare_null_safe_comparison_node,
};
use novarocks_functions::{
    ArithmeticPrepareError, CastOperation, CastPrepareError, ComparisonPrepareError,
};
use novarocks_type_contract::{ArithmeticOperator, ComparisonOperator};

// The parent fixture authors only Boolean binary results. Arithmetic requires
// its own honest physical result, plus the admitted ALLOW_THROW parameter.
// This remains a test-only FragmentBuilder fixture, not a product constructor.
fn arithmetic_fixture(
    operator: BinaryOperator,
    result: FunctionValueType,
) -> Result<Fixture, FixtureError> {
    let mut constants = novarocks_physical_plan::ConstantPools::empty();
    let mut builder = FragmentBuilder::new(FragmentId::new(101));
    builder
        .add_values(
            NodeId::new(u32::MAX),
            Box::from([Box::default()]),
            Box::default(),
        )
        .unwrap();
    let input = FunctionValueType::new(DataType::Int64, false);
    let mut inputs = vec![];
    for value in [20, 22] {
        let literal = builder
            .fixture_value_root(
                NodeId::new(44),
                input.clone(),
                crate::test_constants::i64(&mut constants, &input, value, options().constants),
            )
            .unwrap();
        let value = builder
            .add_value(
                input.clone(),
                ValueOrigin::Expr {
                    node: NodeId::new(44),
                    root: literal,
                },
            )
            .unwrap();
        inputs.push((literal, value));
    }
    builder
        .add_project(
            NodeId::new(44),
            NodeId::new(u32::MAX),
            inputs.clone().into_boxed_slice(),
            inputs
                .iter()
                .map(|(_, value)| *value)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        )
        .unwrap();
    let a = builder
        .fixture_definition(NodeId::new(0), input.clone(), ExprKind::Value(inputs[0].1))
        .unwrap();
    let b = builder
        .fixture_definition(NodeId::new(0), input, ExprKind::Value(inputs[1].1))
        .unwrap();
    let expression = builder
        .fixture_definition(
            NodeId::new(0),
            result.clone(),
            ExprKind::Binary {
                left: a,
                op: operator,
                right: b,
                decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                allow_throw_exception: Some(novarocks_type_contract::SemanticParameterRef {
                    id: novarocks_type_contract::SemanticParameterId::new(7),
                    expected_key:
                        novarocks_type_contract::SemanticParameterKey::AllowThrowException,
                }),
            },
        )
        .unwrap();
    let root = builder
        .fixture_occurrence(
            NodeId::new(0),
            expression,
            EvaluationDemand::Value,
            RootScope::Port(0),
        )
        .unwrap();
    let value = builder
        .add_value(
            result,
            ValueOrigin::Expr {
                node: NodeId::new(0),
                root,
            },
        )
        .unwrap();
    builder
        .add_project(
            NodeId::new(0),
            NodeId::new(44),
            Box::from([(root, value)]),
            Box::from([value]),
        )
        .unwrap();
    seal_units(
        &mut builder,
        &functions(),
        None,
        None,
        &[],
        options().constants,
    )?;
    let fragment = builder.finish_definition(
        NodeId::new(0),
        FragmentSink::Result,
        PipelineDopDomain {
            min: 1,
            max: 1,
            requires_power_of_two: false,
        },
        novarocks_physical_plan::StagePlacementPolicy::Singleton,
    )?;
    Ok(Fixture {
        constants,
        fragment,
        expression,
        ordered: vec![a, b],
        rand_selected: None,
        rand_request: None,
        rand_definitions: vec![],
        constant_policy: options().constants,
        // The parent package helper's flag installs parameter 7 only. It does
        // not alter the expression, operation, target or result declaration.
        carrier_cast: true,
    })
}

#[test]
fn public_comparison_recipe_equals_frozen_program_recipe_for_all_ordinary_operators() {
    let functions = functions();
    for (physical, pure) in [
        (BinaryOperator::Eq, ComparisonOperator::Eq),
        (BinaryOperator::NotEq, ComparisonOperator::Ne),
        (BinaryOperator::Lt, ComparisonOperator::Lt),
        (BinaryOperator::LtEq, ComparisonOperator::Le),
        (BinaryOperator::Gt, ComparisonOperator::Gt),
        (BinaryOperator::GtEq, ComparisonOperator::Ge),
    ] {
        for (left_nullable, right_nullable) in
            [(false, false), (false, true), (true, false), (true, true)]
        {
            let left = FunctionValueType::new(DataType::Int64, left_nullable);
            let right = FunctionValueType::new(DataType::Int64, right_nullable);
            let program = compile(
                package(
                    &functions,
                    fixture(
                        &functions,
                        Shape::Binary(physical),
                        Mode::Project,
                        left.clone(),
                        right.clone(),
                        OutputClaim::Accurate,
                        None,
                    )
                    .unwrap(),
                ),
                &functions,
                &Control,
            )
            .unwrap();
            let direct = prepare_comparison_node(pure, &left, &right, &Control).unwrap();
            assert_eq!(
                Some(&direct),
                unit(&program)
                    .recipes()
                    .comparison(ProgramComparisonSite::Binary(occurrence(&program)))
            );
        }
    }
}

#[test]
fn public_null_safe_recipe_equals_its_separate_frozen_program_recipe() {
    let functions = functions();
    for (left_nullable, right_nullable) in
        [(false, false), (false, true), (true, false), (true, true)]
    {
        let left = FunctionValueType::new(DataType::Int64, left_nullable);
        let right = FunctionValueType::new(DataType::Int64, right_nullable);
        let program = compile(
            package(
                &functions,
                fixture(
                    &functions,
                    Shape::Binary(BinaryOperator::EqForNull),
                    Mode::Project,
                    left.clone(),
                    right.clone(),
                    OutputClaim::Accurate,
                    None,
                )
                .unwrap(),
            ),
            &functions,
            &Control,
        )
        .unwrap();
        let direct = prepare_null_safe_comparison_node(&left, &right, &Control).unwrap();
        let site = occurrence(&program);
        assert_eq!(
            Some(&direct),
            unit(&program).recipes().null_safe_comparison(site)
        );
        assert!(
            unit(&program)
                .recipes()
                .comparison(ProgramComparisonSite::Binary(site))
                .is_none()
        );
    }
}

#[test]
fn public_cast_recipe_equals_frozen_program_recipe_including_policy_and_allow() {
    let functions = functions();
    let input = FunctionValueType::new(DataType::Int64, false);
    let result = FunctionValueType::new(DataType::Float64, true);
    let program = compile(
        package(
            &functions,
            fixture(
                &functions,
                Shape::CarrierCast(None),
                Mode::Project,
                input.clone(),
                result.clone(),
                OutputClaim::Accurate,
                None,
            )
            .unwrap(),
        ),
        &functions,
        &Control,
    )
    .unwrap();
    let direct = prepare_cast_node(
        CastOperation::Carrier,
        &input,
        &result,
        DecimalOverflowPolicy::ReportError,
        true,
        None,
        &Control,
    )
    .unwrap();
    assert_eq!(
        Some(&direct),
        unit(&program).recipes().cast(occurrence(&program))
    );
}

#[test]
fn public_arithmetic_recipe_equals_frozen_program_recipe_for_every_operator() {
    let functions = functions();
    let input = FunctionValueType::new(DataType::Int64, false);
    for (physical, pure, carrier) in [
        (
            BinaryOperator::Add,
            ArithmeticOperator::Add,
            DataType::Int64,
        ),
        (
            BinaryOperator::Subtract,
            ArithmeticOperator::Subtract,
            DataType::Int64,
        ),
        (
            BinaryOperator::Multiply,
            ArithmeticOperator::Multiply,
            DataType::Int64,
        ),
        (
            BinaryOperator::Divide,
            ArithmeticOperator::Divide,
            DataType::Int64,
        ),
        (
            BinaryOperator::Modulo,
            ArithmeticOperator::Modulo,
            DataType::Int64,
        ),
    ] {
        // Frozen arithmetic recipes admit successful NULL independently of
        // whether these two literal operands happen to be non-NULL.
        let result = FunctionValueType::new(carrier, true);
        let program = compile(
            package(
                &functions,
                arithmetic_fixture(physical, result.clone()).unwrap(),
            ),
            &functions,
            &Control,
        )
        .unwrap();
        let direct = prepare_arithmetic_node(
            pure,
            &input,
            &input,
            &result,
            DecimalOverflowPolicy::OutputNull,
            true,
            &Control,
        )
        .unwrap();
        assert_eq!(
            Some(&direct),
            unit(&program).recipes().arithmetic(occurrence(&program))
        );
    }
}

#[test]
fn public_wrappers_preserve_each_family_unsupported_and_type_mismatch() {
    use ProgramPrimitiveError as P;
    let integer = FunctionValueType::new(DataType::Int64, false);
    let nullable_integer = FunctionValueType::new(DataType::Int64, true);
    let boolean = FunctionValueType::new(DataType::Boolean, false);
    let date = FunctionValueType::new(DataType::Date32, true);
    let tiny = FunctionValueType::new(DataType::Int8, true);
    let nested = FunctionValueType::new(
        DataType::List(Arc::new(arrow_schema::Field::new(
            "item",
            DataType::Float32,
            true,
        ))),
        true,
    );
    assert_eq!(
        prepare_comparison_node(ComparisonOperator::Eq, &nested, &nested, &Control),
        Err(P::Comparison(ComparisonPrepareError::Unsupported))
    );
    assert_eq!(
        prepare_null_safe_comparison_node(&nested, &nested, &Control),
        Err(P::Comparison(ComparisonPrepareError::Unsupported))
    );
    assert_eq!(
        prepare_cast_node(
            CastOperation::Carrier,
            &date,
            &tiny,
            DecimalOverflowPolicy::ReportError,
            true,
            None,
            &Control
        ),
        Err(P::Cast(CastPrepareError::Unsupported))
    );
    assert_eq!(
        prepare_arithmetic_node(
            ArithmeticOperator::Add,
            &boolean,
            &boolean,
            &nullable_integer,
            DecimalOverflowPolicy::OutputNull,
            true,
            &Control
        ),
        Err(P::Arithmetic(ArithmeticPrepareError::Unsupported))
    );
    assert_eq!(
        prepare_comparison_node(ComparisonOperator::Eq, &integer, &boolean, &Control),
        Err(P::Comparison(ComparisonPrepareError::TypeMismatch))
    );
    assert_eq!(
        prepare_null_safe_comparison_node(&integer, &boolean, &Control),
        Err(P::Comparison(ComparisonPrepareError::TypeMismatch))
    );
    assert_eq!(
        prepare_cast_node(
            CastOperation::Carrier,
            &nullable_integer,
            &integer,
            DecimalOverflowPolicy::ReportError,
            true,
            None,
            &Control
        ),
        Err(P::Cast(CastPrepareError::TypeMismatch))
    );
    assert_eq!(
        prepare_arithmetic_node(
            ArithmeticOperator::Add,
            &integer,
            &integer,
            &integer,
            DecimalOverflowPolicy::OutputNull,
            true,
            &Control
        ),
        Err(P::Arithmetic(ArithmeticPrepareError::TypeMismatch))
    );
}

fn lowering_primitive(error: &FragmentCompileError) -> ProgramPrimitiveError {
    let FragmentCompileError::Owner {
        phase: "expressions",
        error,
    } = error
    else {
        panic!("expected genuine expression owner: {error}")
    };
    match error
        .downcast_ref::<crate::expressions::ExpressionLoweringError>()
        .expect("exact expression lowering error")
    {
        crate::expressions::ExpressionLoweringError::Unit(
            novarocks_expression::ExprCompileError::Arithmetic(e),
        ) => e.clone().into(),
        crate::expressions::ExpressionLoweringError::Unit(
            novarocks_expression::ExprCompileError::Cast(e),
        ) => e.clone().into(),
        crate::expressions::ExpressionLoweringError::Unit(
            novarocks_expression::ExprCompileError::Comparison(e),
        ) => e.clone().into(),
        other => panic!("not a primitive refusal: {other:?}"),
    }
}

#[test]
fn frozen_program_refusals_keep_the_exact_public_wrapper_refusal() {
    let functions = functions();
    let nested = FunctionValueType::new(
        DataType::List(Arc::new(arrow_schema::Field::new(
            "item",
            DataType::Float32,
            true,
        ))),
        true,
    );
    for physical in [BinaryOperator::Eq, BinaryOperator::EqForNull] {
        let error = compile(
            package(
                &functions,
                fixture(
                    &functions,
                    Shape::Binary(physical),
                    Mode::Project,
                    nested.clone(),
                    nested.clone(),
                    OutputClaim::Accurate,
                    None,
                )
                .unwrap(),
            ),
            &functions,
            &Control,
        )
        .unwrap_err();
        let public = if physical == BinaryOperator::EqForNull {
            prepare_null_safe_comparison_node(&nested, &nested, &Control).unwrap_err()
        } else {
            prepare_comparison_node(ComparisonOperator::Eq, &nested, &nested, &Control).unwrap_err()
        };
        assert_eq!(lowering_primitive(&error), public);
    }
    let source = FunctionValueType::new(DataType::Date32, true);
    let result = FunctionValueType::new(DataType::Int8, true);
    let error = compile(
        package(
            &functions,
            fixture(
                &functions,
                Shape::CarrierCast(None),
                Mode::Project,
                source.clone(),
                result.clone(),
                OutputClaim::Accurate,
                None,
            )
            .unwrap(),
        ),
        &functions,
        &Control,
    )
    .unwrap_err();
    assert_eq!(
        lowering_primitive(&error),
        prepare_cast_node(
            CastOperation::Carrier,
            &source,
            &result,
            DecimalOverflowPolicy::ReportError,
            true,
            None,
            &Control
        )
        .unwrap_err()
    );
    // Boolean arithmetic is illegal before recipe preparation in a frozen
    // Fragment. Its direct Unsupported is covered above, without pretending
    // an invalid source can reach the compiled unit author.
}

#[test]
fn malformed_frozen_claims_fail_at_the_real_author_before_a_program_exists() {
    fn author_error(result: Result<Fixture, FixtureError>) -> FixtureError {
        match result {
            Err(error) => error,
            Ok(_) => panic!("invalid result was accepted by the physical author"),
        }
    }
    let functions = functions();
    let integer = FunctionValueType::new(DataType::Int64, true);
    for physical in [BinaryOperator::Eq, BinaryOperator::EqForNull] {
        assert!(
            author_error(fixture(
                &functions,
                Shape::Binary(physical),
                Mode::Project,
                integer.clone(),
                integer.clone(),
                OutputClaim::NonBoolean,
                None
            ))
            .is_producer_defect()
        );
    }
    assert!(
        author_error(fixture(
            &functions,
            Shape::Binary(BinaryOperator::Eq),
            Mode::Project,
            integer.clone(),
            integer,
            OutputClaim::Narrow,
            None
        ))
        .is_producer_defect()
    );
    assert!(
        author_error(arithmetic_fixture(
            BinaryOperator::Add,
            FunctionValueType::new(DataType::Boolean, true)
        ))
        .is_producer_defect()
    );
    // ProgramPrimitiveError::Invalid protects inconsistent internal checked
    // data. There is intentionally no public bypass constructor for that state.
}

fn check_refusals<T: std::fmt::Debug>(
    mut prepare: impl FnMut(&dyn PureCompileControl) -> Result<T, ProgramPrimitiveError>,
) {
    let recorder = RefusingControl::new(CompileControlError::Cancelled, usize::MAX);
    prepare(&recorder).unwrap();
    let trace = recorder.trace.lock().unwrap().clone();
    assert!(!trace.is_empty());
    for stop_at in 1..=trace.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = RefusingControl::new(cause.clone(), stop_at);
            assert!(
                matches!(prepare(&control), Err(ProgramPrimitiveError::Control(actual)) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..stop_at]);
        }
    }
}

#[test]
fn each_public_family_and_checked_expression_program_keep_original_control_causes() {
    let functions = functions();
    let input = FunctionValueType::new(DataType::Int64, false);
    let output = FunctionValueType::new(DataType::Int64, true);
    check_refusals(|c| prepare_comparison_node(ComparisonOperator::Eq, &input, &input, c));
    check_refusals(|c| prepare_null_safe_comparison_node(&input, &input, c));
    check_refusals(|c| {
        prepare_cast_node(
            CastOperation::Carrier,
            &input,
            &output,
            DecimalOverflowPolicy::ReportError,
            true,
            None,
            c,
        )
    });
    check_refusals(|c| {
        prepare_arithmetic_node(
            ArithmeticOperator::Add,
            &input,
            &input,
            &output,
            DecimalOverflowPolicy::OutputNull,
            true,
            c,
        )
    });
    let mut sources = vec![];
    for shape in [
        Shape::Binary(BinaryOperator::Eq),
        Shape::Binary(BinaryOperator::EqForNull),
        Shape::CarrierCast(None),
    ] {
        let right = if matches!(shape, Shape::CarrierCast(_)) {
            output.clone()
        } else {
            input.clone()
        };
        sources.push(package(
            &functions,
            fixture(
                &functions,
                shape,
                Mode::Project,
                input.clone(),
                right,
                OutputClaim::Accurate,
                None,
            )
            .unwrap(),
        ));
    }
    sources.push(package(
        &functions,
        arithmetic_fixture(BinaryOperator::Add, output).unwrap(),
    ));
    for source in sources {
        let recorder = RefusingControl::new(CompileControlError::Cancelled, usize::MAX);
        compile(source.clone(), &functions, &recorder).unwrap();
        let trace = recorder.trace.lock().unwrap().clone();
        assert!(!trace.is_empty());
        for stop_at in 1..=trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = RefusingControl::new(cause.clone(), stop_at);
                assert!(
                    matches!(compile(source.clone(),&functions,&control),Err(FragmentCompileError::Control(actual)) if actual==cause)
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..stop_at]);
            }
        }
    }
}

#[test]
fn actual_arithmetic_type_mismatch_is_not_a_capability_refusal_or_invalid_request() {
    let functions = functions();
    let input = FunctionValueType::new(DataType::Int64, false);
    // The physical arithmetic rule admits this exact carrier and nullability:
    // neither operand admits NULL. The recipe separately requires nullable
    // output for its successful-NULL arithmetic behavior.
    let source = package(
        &functions,
        arithmetic_fixture(BinaryOperator::Add, input.clone()).unwrap(),
    );
    let error = compile(source, &functions, &Control).unwrap_err();
    assert!(!error.is_unsupported());
    let actual = lowering_primitive(&error);
    let direct = prepare_arithmetic_node(
        ArithmeticOperator::Add,
        &input,
        &input,
        &input,
        DecimalOverflowPolicy::OutputNull,
        true,
        &Control,
    )
    .unwrap_err();
    assert_eq!(actual, direct);
    assert_eq!(
        actual,
        ProgramPrimitiveError::Arithmetic(ArithmeticPrepareError::TypeMismatch)
    );
}

#[test]
fn explicit_array_cast_modes_match_public_recipes_and_remain_distinct() {
    use novarocks_type_contract::ArrayCastFailureMode as ArrayMode;
    let functions = functions();
    let list = |item| {
        FunctionValueType::new(
            DataType::List(Arc::new(arrow_schema::Field::new(
                "declared-item",
                item,
                true,
            ))),
            true,
        )
    };
    let source = list(DataType::Int32);
    let target = list(DataType::Int64);
    let mut recipes = vec![];
    for mode in [Some(ArrayMode::ReportError), Some(ArrayMode::OutputNull)] {
        let fixture = fixture(
            &functions,
            Shape::CarrierCast(mode),
            Mode::Project,
            source.clone(),
            target.clone(),
            OutputClaim::Accurate,
            None,
        )
        .unwrap();
        let program = compile(package(&functions, fixture), &functions, &Control).unwrap();
        let direct = prepare_cast_node(
            CastOperation::Carrier,
            &source,
            &target,
            DecimalOverflowPolicy::ReportError,
            true,
            mode,
            &Control,
        )
        .unwrap();
        assert_eq!(direct.array_failure_mode(), mode);
        assert_eq!(
            Some(&direct),
            unit(&program).recipes().cast(occurrence(&program))
        );
        recipes.push(direct);
    }
    assert_ne!(recipes[0], recipes[1]);
    let identity_source = list(DataType::Int32);
    let legacy = prepare_cast_node(
        CastOperation::Carrier,
        &identity_source,
        &identity_source,
        DecimalOverflowPolicy::ReportError,
        true,
        None,
        &Control,
    )
    .unwrap();
    for mode in [ArrayMode::ReportError, ArrayMode::OutputNull] {
        let explicit = prepare_cast_node(
            CastOperation::Carrier,
            &identity_source,
            &identity_source,
            DecimalOverflowPolicy::ReportError,
            true,
            Some(mode),
            &Control,
        )
        .unwrap();
        assert_ne!(legacy, explicit);
    }
}

#[test]
fn explicit_json_text_public_recipes_preserve_required_child_contracts() {
    use novarocks_type_contract::{
        ArrayCastFailureMode, ValueKind, ValueLayout, ValueLogicalType, ValueTypeRef,
        ValueTypeStorageLifetime,
    };
    let storage = ValueTypeStorageLifetime::pure();
    let item = ValueTypeRef::try_new(
        ValueKind::Nominal(ValueLogicalType::Json),
        false,
        storage.clone(),
    )
    .unwrap();
    let value = ValueTypeRef::try_new(ValueKind::List(item), true, storage.clone()).unwrap();
    let layout = ValueLayout::try_new(
        &DataType::List(Arc::new(arrow_schema::Field::new(
            "json",
            DataType::Utf8,
            false,
        ))),
        storage,
    )
    .unwrap();
    let json = FunctionValueType::try_from_value(value, layout).unwrap();
    let text = |nullable| {
        FunctionValueType::new(
            DataType::List(Arc::new(arrow_schema::Field::new(
                "text",
                DataType::Utf8,
                nullable,
            ))),
            true,
        )
    };
    for mode in [
        ArrayCastFailureMode::ReportError,
        ArrayCastFailureMode::OutputNull,
    ] {
        assert_eq!(
            prepare_cast_node(
                CastOperation::Carrier,
                &json,
                &text(false),
                DecimalOverflowPolicy::ReportError,
                true,
                Some(mode),
                &Control
            ),
            Err(ProgramPrimitiveError::Cast(CastPrepareError::TypeMismatch))
        );
        assert_eq!(
            prepare_cast_node(
                CastOperation::Carrier,
                &text(true),
                &json,
                DecimalOverflowPolicy::ReportError,
                true,
                Some(mode),
                &Control
            ),
            Err(ProgramPrimitiveError::Cast(CastPrepareError::TypeMismatch))
        );
        for (source, result) in [(json.clone(), text(true)), (text(false), json.clone())] {
            let direct = prepare_cast_node(
                CastOperation::Carrier,
                &source,
                &result,
                DecimalOverflowPolicy::ReportError,
                true,
                Some(mode),
                &Control,
            )
            .unwrap();
            let functions = functions();
            let fixture = fixture(
                &functions,
                Shape::CarrierCast(Some(mode)),
                Mode::Project,
                source,
                result,
                OutputClaim::Accurate,
                None,
            )
            .unwrap();
            let program = compile(package(&functions, fixture), &functions, &Control).unwrap();
            assert_eq!(
                Some(&direct),
                unit(&program).recipes().cast(occurrence(&program))
            );
        }
    }
}
