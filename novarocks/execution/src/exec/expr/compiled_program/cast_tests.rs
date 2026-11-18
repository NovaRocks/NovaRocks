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

// Complete Physical packages using the production owners from the parent fixture.
use super::*;
use crate::exec::expr::compiled_program::checked_constants;
use arrow::array::Float32Array;
use novarocks_functions::CastOperation;
use novarocks_local_compiler::FragmentCompileError;
use novarocks_type_contract::{ExpressionEffectContext, ExpressionUseId};

fn package(
    functions: &PureEngineFunctionCatalog,
    mut builder: FragmentBuilder,
    root_node: NodeId,
    authors: &BTreeMap<ExprId, Author>,
    result: ResultPort,
    allow: bool,
    source_pools: novarocks_physical_plan::ConstantPools,
) -> Arc<FragmentPackage> {
    let fragment_id = builder.id();
    let parameters = SemanticParameters::try_new([(
        allow_ref().id,
        SemanticParameterValue::AllowThrowException(allow),
    )])
    .unwrap();
    let constants = finish_fixture_units(
        functions,
        &mut builder,
        root_node,
        authors,
        &parameters,
        source_pools,
        |unit, invocation, children| {
            let definition = unit.definition(invocation.definition).unwrap();
            if let ExprKind::Binary {
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
                    &unit.definition(*left).unwrap().ty,
                    &unit.definition(*right).unwrap().ty,
                    &definition.ty,
                    *decimal_overflow_policy,
                    *allow,
                    &Control,
                )
                .unwrap();
                recipe
                    .own_effects(invocation.context)
                    .join_same_region(children)
                    .unwrap()
            } else {
                children
            }
        },
    );
    let fragment = builder
        .finish_definition(
            root_node,
            FragmentSink::Result,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    Arc::new(
        FragmentPackage::try_new(
            FragmentPackageInput {
                version: PlanVersionId::try_new([213; 16]).unwrap(),
                required: RequiredContracts::default(),
                constants,
                fragment,
                statement_constant_policy: options().constants,
                pruning: FrozenFragmentPruning::try_new(fragment_id, Vec::new(), &Control).unwrap(),
                cuts: FragmentCuts::default(),
                result: Some(result),
                parameters,
                scans: BTreeMap::new(),
                writes: BTreeMap::new(),
                annotations: Box::default(),
                layouts: Default::default(),
            },
            package_admission(),
            &Control,
        )
        .unwrap(),
    )
}

fn cast_root_use(program: &LocalProgram) -> ExpressionUseId {
    let reference = root(program);
    root_unit(program).root(reference.root).unwrap().use_id
}

#[derive(Clone, Copy)]
enum Source {
    Column,
    Constant,
    Add,
    Round,
    RandCondition,
}
fn result_literal(
    builder: &mut FragmentBuilder,
    result: &FunctionValueType,
    policy: DecimalOverflowPolicy,
    source_pools: &mut novarocks_physical_plan::ConstantPools,
) -> ExprId {
    if matches!(result.data_type(), DataType::Timestamp(_, None)) {
        return literal(
            builder,
            result,
            checked_constants::timestamp(result, 71, options().constants),
            source_pools,
        );
    }
    if *result.data_type() == DataType::Int64 {
        return literal(
            builder,
            result,
            checked_constants::i64(result, 71, options().constants),
            source_pools,
        );
    }
    let source = FunctionValueType::new(DataType::Int64, false);
    let value = literal(
        builder,
        &source,
        checked_constants::i64(&source, 71, options().constants),
        source_pools,
    );
    builder
        .unit(NodeId::new(0))
        .unwrap()
        .add(
            None,
            result.clone(),
            ExprKind::Cast {
                expr: value,
                target: result.data_type().clone(),
                decimal_overflow_policy: policy,
                allow_throw_exception: allow_ref(),
            },
        )
        .unwrap()
}

fn fixture(
    source_type: FunctionValueType,
    result_type: FunctionValueType,
    source: Source,
    wrap: Wrap,
    policy: DecimalOverflowPolicy,
    allow: bool,
) -> (PureEngineFunctionCatalog, Arc<FragmentPackage>) {
    let mut source_pools = novarocks_physical_plan::ConstantPools::empty();
    let functions = catalogue(Shape::Decimal);
    let fid = FragmentId::new(213);
    let start = NodeId::new(u32::MAX);
    let input_node = NodeId::new(41);
    let output_node = NodeId::new(0);
    let boolean = FunctionValueType::new(DataType::Boolean, true);
    let seed_type = FunctionValueType::new(DataType::Int64, true);
    let columns = [
        (ValueId::new(91), source_type.clone()),
        (ValueId::new(7), seed_type.clone()),
        (ValueId::new(333), boolean.clone()),
    ];
    let mut builder = FragmentBuilder::new(fid);
    builder
        .add_values(start, Box::from([Box::default()]), Box::default())
        .unwrap();
    let mut projections = Vec::new();
    for (id, ty) in &columns {
        let definition = if !ty.nullable() && *ty.data_type() == DataType::Float32 {
            let exact = builder
                .unit(input_node)
                .unwrap()
                .add(
                    None,
                    (FunctionValueType::new(DataType::Float64, false)).clone(),
                    checked_constants::kind(
                        &mut source_pools,
                        checked_constants::f64_bits(
                            &(FunctionValueType::new(DataType::Float64, false)),
                            0.0_f64.to_bits(),
                            options().constants,
                        ),
                    ),
                )
                .unwrap();
            builder
                .unit(input_node)
                .unwrap()
                .add(
                    None,
                    ty.clone(),
                    ExprKind::Cast {
                        expr: exact,
                        target: DataType::Float32,
                        decimal_overflow_policy: policy,
                        allow_throw_exception: allow_ref(),
                    },
                )
                .unwrap()
        } else {
            let value = if ty.nullable() {
                checked_constants::null(ty, options().constants)
            } else {
                match ty.data_type() {
                    DataType::Binary => checked_constants::binary(
                        ty,
                        &(b"native producer".to_vec().into_boxed_slice()),
                        options().constants,
                    ),
                    DataType::Decimal128(..) => {
                        checked_constants::decimal128(ty, 0, options().constants)
                    }
                    DataType::Decimal256(..) => checked_constants::decimal256(
                        ty,
                        arrow_buffer::i256::ZERO.to_be_bytes(),
                        options().constants,
                    ),
                    DataType::Boolean => checked_constants::boolean(ty, false, options().constants),
                    DataType::Date32 => checked_constants::date32(ty, 0, options().constants),
                    DataType::FixedSizeBinary(16)
                        if ty.logical_type()
                            == novarocks_type_contract::ValueLogicalType::LargeInt =>
                    {
                        checked_constants::largeint(ty, 0, options().constants)
                    }
                    DataType::Int64 => checked_constants::i64(ty, 0, options().constants),
                    DataType::UInt64 => checked_constants::u64(ty, 0, options().constants),
                    DataType::Timestamp(_, None) => {
                        checked_constants::timestamp(ty, 0, options().constants)
                    }
                    DataType::Float64 => {
                        checked_constants::f64_bits(ty, 0.0_f64.to_bits(), options().constants)
                    }
                    _ => {
                        panic!("nonnullable fixture requires an implemented exact numeric literal")
                    }
                }
            };
            builder
                .unit(input_node)
                .unwrap()
                .add(
                    None,
                    ty.clone(),
                    checked_constants::kind(&mut source_pools, value),
                )
                .unwrap()
        };
        let definition = builder
            .unit(input_node)
            .unwrap()
            .root(definition, EvaluationDemand::Value, RootScope::Port(0))
            .unwrap();
        builder
            .insert_value(ValueDef {
                id: *id,
                ty: ty.clone(),
                origin: ValueOrigin::Expr {
                    node: input_node,
                    root: definition,
                },
            })
            .unwrap();
        projections.push((definition, *id));
    }
    builder
        .add_project(
            input_node,
            start,
            projections.into_boxed_slice(),
            Box::from([columns[0].0, columns[1].0, columns[2].0]),
        )
        .unwrap();
    let mut authors = BTreeMap::new();
    let mut child = if matches!(source, Source::Constant) {
        if *source_type.data_type() == DataType::Float32 {
            let exact = literal(
                &mut builder,
                &FunctionValueType::new(DataType::Float64, false),
                checked_constants::f64_bits(
                    &FunctionValueType::new(DataType::Float64, false),
                    (-0.0_f64).to_bits(),
                    options().constants,
                ),
                &mut source_pools,
            );
            builder
                .unit(output_node)
                .unwrap()
                .add(
                    None,
                    source_type.clone(),
                    ExprKind::Cast {
                        expr: exact,
                        target: DataType::Float32,
                        decimal_overflow_policy: policy,
                        allow_throw_exception: allow_ref(),
                    },
                )
                .unwrap()
        } else {
            let value = match source_type.data_type() {
                DataType::Binary => checked_constants::binary(
                    &source_type,
                    &(b"native\0producer".to_vec().into_boxed_slice()),
                    options().constants,
                ),
                DataType::Decimal128(..) => {
                    checked_constants::decimal128(&source_type, 1, options().constants)
                }
                DataType::Decimal256(..) => checked_constants::decimal256(
                    &source_type,
                    arrow_buffer::i256::from_i128(1).to_be_bytes(),
                    options().constants,
                ),
                DataType::Boolean => {
                    checked_constants::boolean(&source_type, true, options().constants)
                }
                DataType::Date32 => {
                    checked_constants::date32(&source_type, 71, options().constants)
                }
                DataType::FixedSizeBinary(16)
                    if source_type.logical_type()
                        == novarocks_type_contract::ValueLogicalType::LargeInt =>
                {
                    checked_constants::largeint(&source_type, i128::MIN, options().constants)
                }
                DataType::Int64 => checked_constants::i64(&source_type, 71, options().constants),
                DataType::UInt64 => {
                    checked_constants::u64(&source_type, u64::MAX, options().constants)
                }
                DataType::Timestamp(_, None) => {
                    checked_constants::timestamp(&source_type, 71, options().constants)
                }
                DataType::Float64 => checked_constants::f64_bits(
                    &source_type,
                    (-0.0_f64).to_bits(),
                    options().constants,
                ),
                _ => panic!("constant fixture requires an implemented exact numeric literal"),
            };
            literal(&mut builder, &source_type, value, &mut source_pools)
        }
    } else {
        builder
            .unit(output_node)
            .unwrap()
            .add(None, source_type.clone(), ExprKind::Value(columns[0].0))
            .unwrap()
    };
    if matches!(source, Source::Add) {
        let rhs = literal(
            &mut builder,
            &source_type,
            checked_constants::i64(&source_type, 1, options().constants),
            &mut source_pools,
        );
        child = arithmetic(&mut builder, BinaryOperator::Add, child, rhs, &source_type);
    }
    if matches!(source, Source::Round) {
        child = call(
            &mut builder,
            &mut authors,
            author(
                &functions,
                "round",
                vec![argument(source_type.clone(), None)],
                ControlShape::Eager,
            ),
            vec![child],
        );
        assert_eq!(authors[&child].result(), source_type);
    }
    let cast = builder
        .unit(output_node)
        .unwrap()
        .add(
            None,
            result_type.clone(),
            ExprKind::Cast {
                expr: child,
                target: result_type.data_type().clone(),
                decimal_overflow_policy: policy,
                allow_throw_exception: allow_ref(),
            },
        )
        .unwrap();
    let mut root_type = result_type.clone();
    let expression = match wrap {
        Wrap::Bare | Wrap::ConstantRight => cast,
        Wrap::Coalesce | Wrap::If => {
            let fallback = result_literal(&mut builder, &result_type, policy, &mut source_pools);
            if matches!(wrap, Wrap::Coalesce) {
                call(
                    &mut builder,
                    &mut authors,
                    author(
                        &functions,
                        "coalesce",
                        vec![
                            argument(result_type.clone(), None),
                            argument(result_type.clone(), None),
                        ],
                        ControlShape::Coalesce,
                    ),
                    vec![cast, fallback],
                )
            } else {
                let flag = if matches!(source, Source::RandCondition) {
                    let seed = builder
                        .unit(output_node)
                        .unwrap()
                        .add(None, seed_type.clone(), ExprKind::Value(columns[1].0))
                        .unwrap();
                    let random = call(
                        &mut builder,
                        &mut authors,
                        author(
                            &functions,
                            "rand",
                            vec![argument(seed_type, None)],
                            ControlShape::Eager,
                        ),
                        vec![seed],
                    );
                    builder
                        .unit(output_node)
                        .unwrap()
                        .add(
                            None,
                            FunctionValueType::new(DataType::Boolean, false),
                            ExprKind::IsNull {
                                expr: random,
                                negated: true,
                            },
                        )
                        .unwrap()
                } else {
                    builder
                        .unit(output_node)
                        .unwrap()
                        .add(None, boolean.clone(), ExprKind::Value(columns[2].0))
                        .unwrap()
                };
                let flag_type = if matches!(source, Source::RandCondition) {
                    FunctionValueType::new(DataType::Boolean, false)
                } else {
                    boolean.clone()
                };
                call(
                    &mut builder,
                    &mut authors,
                    author(
                        &functions,
                        "if",
                        vec![
                            argument(flag_type, None),
                            argument(result_type.clone(), None),
                            argument(result_type.clone(), None),
                        ],
                        ControlShape::If,
                    ),
                    vec![flag, cast, fallback],
                )
            }
        }
        Wrap::IsNull | Wrap::And | Wrap::Or => {
            root_type = FunctionValueType::new(DataType::Boolean, false);
            let is_null = builder
                .unit(output_node)
                .unwrap()
                .add(
                    None,
                    root_type.clone(),
                    ExprKind::IsNull {
                        expr: cast,
                        negated: false,
                    },
                )
                .unwrap();
            if matches!(wrap, Wrap::IsNull) {
                is_null
            } else {
                let decide = literal(
                    &mut builder,
                    &root_type,
                    checked_constants::boolean(
                        &root_type,
                        matches!(wrap, Wrap::Or),
                        options().constants,
                    ),
                    &mut source_pools,
                );
                builder
                    .unit(output_node)
                    .unwrap()
                    .add(
                        None,
                        root_type.clone(),
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
        Wrap::NullParent => panic!("this fixture has no null arithmetic parent"),
    };
    let expression = builder
        .unit(output_node)
        .unwrap()
        .root(expression, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    let value = builder
        .add_value(
            root_type.clone(),
            ValueOrigin::Expr {
                node: output_node,
                root: expression,
            },
        )
        .unwrap();
    builder
        .add_project(
            output_node,
            input_node,
            Box::from([(expression, value)]),
            Box::from([value]),
        )
        .unwrap();
    let result = ResultPort {
        scalar_schema: None,
        fragment: fid,
        output: builder
            .construction_node(output_node)
            .unwrap()
            .output
            .clone(),
        fields: Box::from([ResultField {
            domain: crate::test_result_domain::result_value_domain(&root_type),
            name: "cast_result".into(),
            alias: None,
            value,
            ty: root_type,
        }]),
    };
    let package = package(
        &functions,
        builder,
        output_node,
        &authors,
        result,
        allow,
        source_pools,
    );
    (functions, package)
}
fn compile(
    functions: &PureEngineFunctionCatalog,
    package: Arc<FragmentPackage>,
    control: &dyn PureCompileControl,
) -> Result<Arc<LocalProgram>, FragmentCompileError> {
    let providers =
        PureProviderProgramCatalog::<std::io::Error>::try_new(&[], Vec::new(), &Control).unwrap();
    let validated = validate_fragment_providers(package, &providers, &Control).unwrap();
    compile_fragment(validated, functions, options(), control).map(Arc::new)
}
fn compiled(
    source: FunctionValueType,
    result: FunctionValueType,
    mode: Source,
    wrap: Wrap,
    policy: DecimalOverflowPolicy,
    allow: bool,
) -> Arc<LocalProgram> {
    let (functions, package) = fixture(source.clone(), result.clone(), mode, wrap, policy, allow);
    let program = compile(&functions, package, &Control).unwrap();
    let mut count = 0;
    for unit in program.units() {
        for (&id, invocation) in unit.control_flow().uses() {
            let definition = unit.definitions().node(invocation.definition).unwrap();
            if matches!(definition.kind(), StaticExprKind::PreparedCast { .. }) {
                let recipe = unit.recipes().cast(id).unwrap();
                assert_eq!(recipe.operation(), CastOperation::Carrier);
                // A narrowed ELSE is itself an authored I64 -> target Cast.
                // Identify the primary occurrence by both complete source facts.
                let primary = recipe.source_type() == &source && recipe.result_type() == &result;
                assert_eq!(recipe.decimal_overflow_policy(), policy);
                assert_eq!(recipe.allow_throw_exception(), allow);
                count += usize::from(primary);
            }
        }
    }
    assert_eq!(count, 1);
    program
}
fn bound(ty: &DataType) -> (i64, i64) {
    match ty {
        DataType::Int8 => (i8::MIN.into(), i8::MAX.into()),
        DataType::Int16 => (i16::MIN.into(), i16::MAX.into()),
        DataType::Int32 => (i32::MIN.into(), i32::MAX.into()),
        DataType::Int64 => (i64::MIN, i64::MAX),
        _ => panic!("signed type"),
    }
}
fn signed_values(output: &novarocks_functions::SelectedValues<'_>) -> Vec<Option<i64>> {
    if output.values().data_type() == &DataType::Int8 {
        output
            .values()
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap()
            .iter()
            .map(|v| v.map(i64::from))
            .collect()
    } else {
        integral(output)
    }
}
fn sliced_batch(program: &LocalProgram, values: &[Option<i64>]) -> RecordBatch {
    let schema = program.graph().nodes()[1].output_layout().schema().clone();
    let mut padded = vec![Some(99)];
    padded.extend_from_slice(values);
    padded.push(Some(-99));
    let source = signed(schema.field(0).data_type(), &padded).slice(1, values.len());
    RecordBatch::try_new(
        schema,
        vec![
            source,
            Arc::new(Int64Array::from(vec![Some(42); values.len()])),
            Arc::new(BooleanArray::from(vec![Some(true); values.len()])),
        ],
    )
    .unwrap()
}

#[test]
fn all_twenty_four_signed_cast_profiles_freeze_full_types_policy_allow_and_sparse_sliced_rows() {
    for source in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
    ] {
        let (min, max) = bound(&source);
        for target in [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
        ] {
            for policy in [
                DecimalOverflowPolicy::OutputNull,
                DecimalOverflowPolicy::ReportError,
            ] {
                for allow in [false, true] {
                    let program = compiled(
                        FunctionValueType::new(source.clone(), true),
                        FunctionValueType::new(target.clone(), true),
                        Source::Column,
                        Wrap::Bare,
                        policy,
                        allow,
                    );
                    let values = [Some(17), Some(min), None, Some(max), Some(-7), Some(19)];
                    let batch = sliced_batch(&program, &values);
                    let rows = [1, 2, 3, 4];
                    let selection = Selection::try_sparse(6, &rows).unwrap();
                    let mut evaluator = instance(&program);
                    let output = evaluator.evaluate(&batch, selection, &Control).unwrap();
                    assert_eq!(output.selection(), selection);
                    assert_eq!(output.values().data_type(), &target);
                    assert!(output.errors().is_empty());
                    match target {
                        DataType::Float32 => {
                            let actual = output
                                .values()
                                .as_any()
                                .downcast_ref::<Float32Array>()
                                .unwrap();
                            for (ordinal, row) in rows.into_iter().enumerate() {
                                assert_eq!(actual.is_null(ordinal), values[row].is_none());
                                if let Some(value) = values[row] {
                                    assert_eq!(
                                        actual.value(ordinal).to_bits(),
                                        (value as f32).to_bits()
                                    );
                                }
                            }
                        }
                        DataType::Float64 => {
                            let actual = output
                                .values()
                                .as_any()
                                .downcast_ref::<Float64Array>()
                                .unwrap();
                            for (ordinal, row) in rows.into_iter().enumerate() {
                                assert_eq!(actual.is_null(ordinal), values[row].is_none());
                                if let Some(value) = values[row] {
                                    assert_eq!(
                                        actual.value(ordinal).to_bits(),
                                        (value as f64).to_bits()
                                    );
                                }
                            }
                        }
                        _ => {
                            let (low, high) = bound(&target);
                            assert_eq!(
                                signed_values(&output),
                                rows.map(
                                    |row| values[row].filter(|value| (low..=high).contains(value))
                                )
                                .to_vec()
                            );
                        }
                    }
                    assert!(evaluator.instances.is_empty());
                }
            }
        }
    }
}

#[test]
fn truthful_nonnullable_widening_is_preserved_and_complete_package_rejects_false_narrowing_claim() {
    // The current Physical scalar literal author can produce a nonempty I64
    // source. I8(false) source production needs the later CV/channel slice;
    // neither a retagged I64 literal nor a NULL(false) proves it here.
    let values = [
        Some(i64::MIN),
        Some(-128),
        Some(0),
        Some(127),
        Some(i64::MAX),
    ];
    for target in [DataType::Int64, DataType::Float32, DataType::Float64] {
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            for allow in [false, true] {
                let program = compiled(
                    FunctionValueType::new(DataType::Int64, false),
                    FunctionValueType::new(target.clone(), false),
                    Source::Column,
                    Wrap::Bare,
                    policy,
                    allow,
                );
                let batch = sliced_batch(&program, &values);
                let output = instance(&program)
                    .evaluate(&batch, Selection::all(values.len()), &Control)
                    .unwrap();
                assert_eq!(output.values().null_count(), 0);
                match target {
                    DataType::Int64 => assert_eq!(signed_values(&output), values.to_vec()),
                    DataType::Float32 => {
                        let actual = output
                            .values()
                            .as_any()
                            .downcast_ref::<Float32Array>()
                            .unwrap();
                        for (row, value) in values.iter().enumerate() {
                            assert_eq!(
                                actual.value(row).to_bits(),
                                (value.unwrap() as f32).to_bits()
                            );
                        }
                    }
                    DataType::Float64 => {
                        let actual = output
                            .values()
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .unwrap();
                        for (row, value) in values.iter().enumerate() {
                            assert_eq!(
                                actual.value(row).to_bits(),
                                (value.unwrap() as f64).to_bits()
                            );
                        }
                    }
                    _ => unreachable!(),
                }
            }
        }
    }
    let (functions, package) = fixture(
        FunctionValueType::new(DataType::Int64, false),
        FunctionValueType::new(DataType::Int8, false),
        Source::Column,
        Wrap::Bare,
        DecimalOverflowPolicy::ReportError,
        true,
    );
    // The original structural package accepts this declaration. The real pure
    // compiler must reject the missing successful-NULL promise before runtime.
    let error = compile(&functions, package, &Control).unwrap_err();
    assert!(!matches!(error, FragmentCompileError::Control(_)));
    assert!(error.to_string().contains("successful-NULL"));
}

#[test]
fn cast_joins_real_required_child_errors_and_guarded_control_never_turns_them_into_successful_null()
{
    for wrap in [
        Wrap::Bare,
        Wrap::IsNull,
        Wrap::Coalesce,
        Wrap::If,
        Wrap::And,
        Wrap::Or,
    ] {
        let program = compiled(
            FunctionValueType::new(DataType::Int64, true),
            FunctionValueType::new(DataType::Int8, true),
            Source::Add,
            wrap,
            DecimalOverflowPolicy::OutputNull,
            false,
        );
        let batch = input(
            &program,
            &[Some(1), Some(i64::MAX), None, Some(7), Some(i64::MAX)],
            &[Some(42); 5],
            &[None, Some(true), Some(true), Some(true), Some(false)],
        );
        let rows = [1, 2, 3, 4];
        let selection = Selection::try_sparse(5, &rows).unwrap();
        let output = instance(&program)
            .evaluate(&batch, selection, &Control)
            .unwrap();
        match wrap {
            Wrap::And | Wrap::Or => {
                assert!(output.errors().is_empty());
                let values = output
                    .values()
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap();
                assert_eq!(
                    values.iter().collect::<Vec<_>>(),
                    vec![Some(matches!(wrap, Wrap::Or)); 4]
                );
            }
            Wrap::If => {
                assert_eq!(errors(&output), vec![0]);
                assert_eq!(signed_values(&output), vec![None, None, Some(8), Some(71)]);
            }
            Wrap::Coalesce => {
                assert_eq!(errors(&output), vec![0, 3]);
                assert_eq!(signed_values(&output), vec![None, Some(71), Some(8), None]);
            }
            Wrap::Bare => {
                assert_eq!(errors(&output), vec![0, 3]);
                assert_eq!(signed_values(&output), vec![None, None, Some(8), None]);
            }
            Wrap::IsNull => {
                assert_eq!(errors(&output), vec![0, 3]);
                let values = output
                    .values()
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap();
                assert!(values.is_null(0));
                assert!(values.value(1));
                assert!(!values.value(2));
                assert!(values.is_null(3));
            }
            _ => unreachable!(),
        }
        for error in output.errors() {
            assert_eq!(
                error.message(),
                "Arithmetic overflow: Overflow happened on: 9223372036854775807 + 1"
            );
        }
    }
}

#[test]
fn cast_literal_broadcast_empty_selection_and_real_round_rand_owners_keep_exact_effects_and_instances()
 {
    let constant = compiled(
        FunctionValueType::new(DataType::Int64, false),
        FunctionValueType::new(DataType::Int16, true),
        Source::Constant,
        Wrap::Bare,
        DecimalOverflowPolicy::ReportError,
        true,
    );
    let batch = sliced_batch(&constant, &[Some(7); 5]);
    let mut evaluator = instance(&constant);
    let empty = [];
    let output = evaluator
        .evaluate(&batch, Selection::try_sparse(5, &empty).unwrap(), &Control)
        .unwrap();
    assert!(output.values().is_empty());
    assert!(evaluator.instances.is_empty());
    let rows = [0, 2, 4];
    let output = evaluator
        .evaluate(&batch, Selection::try_sparse(5, &rows).unwrap(), &Control)
        .unwrap();
    assert_eq!(signed_values(&output), vec![Some(71); 3]);
    assert!(evaluator.instances.is_empty());
    for source in [Source::Round, Source::RandCondition] {
        let program = compiled(
            FunctionValueType::new(DataType::Int64, true),
            FunctionValueType::new(DataType::Int8, true),
            source,
            if matches!(source, Source::RandCondition) {
                Wrap::If
            } else {
                Wrap::Bare
            },
            DecimalOverflowPolicy::ReportError,
            true,
        );
        let calls = root_unit(&program).calls();
        assert!(
            calls
                .values()
                .any(|call| call.call_contract().function_id().as_str()
                    == if matches!(source, Source::Round) {
                        "builtin.scalar/round/v1"
                    } else {
                        "builtin.scalar/rand/v1"
                    })
        );
        if matches!(source, Source::RandCondition) {
            assert!(calls.values().any(|call| {
                let effects = call.effects();
                effects.for_use(effects.context()).unwrap().value_stability
                    == novarocks_type_contract::FunctionVolatility::Volatile
            }));
        }
        let batch = sliced_batch(&program, &[Some(9), None, Some(130), Some(-7), Some(11)]);
        let mut evaluator = instance(&program);
        let empty_output = evaluator
            .evaluate(&batch, Selection::try_sparse(5, &empty).unwrap(), &Control)
            .unwrap();
        assert!(empty_output.values().is_empty());
        assert!(evaluator.instances.is_empty());
        let selected = evaluator
            .evaluate(&batch, Selection::try_sparse(5, &rows).unwrap(), &Control)
            .unwrap();
        assert_eq!(signed_values(&selected), vec![Some(9), None, Some(11)]);
        assert!(selected.errors().is_empty());
        assert_eq!(evaluator.instances.len(), 1);
    }
}

#[test]
fn cast_runtime_all_original_failure_categories_stop_at_exact_callback_and_latch_without_replay() {
    let program = compiled(
        FunctionValueType::new(DataType::Int64, true),
        FunctionValueType::new(DataType::Int8, true),
        Source::Column,
        Wrap::Coalesce,
        DecimalOverflowPolicy::ReportError,
        true,
    );
    let source = (0..320)
        .map(|row| match row % 3 {
            0 => Some(130),
            1 => None,
            _ => Some(-7),
        })
        .collect::<Vec<_>>();
    let batch = sliced_batch(&program, &source);
    let constructor = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
    CompiledExpressionInstance::try_new(
        root_unit(&program).clone(),
        root(&program).root,
        &constructor,
    )
    .unwrap();
    let constructor_trace = constructor.trace.lock().unwrap().clone();
    for stop_at in 1..=constructor_trace.len() {
        for cause in causes() {
            let control = CallbackControl::new(cause.clone(), stop_at);
            assert!(
                matches!(CompiledExpressionInstance::try_new(root_unit(&program).clone(), root(&program).root, &control), Err(actual) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), constructor_trace[..stop_at]);
        }
    }
    let recorder = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
    let output = instance(&program)
        .evaluate(&batch, Selection::all(320), &recorder)
        .unwrap();
    assert!(output.errors().is_empty());
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

struct CompileCallbacks {
    cause: CompileControlError,
    stop_at: usize,
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refused: Mutex<bool>,
}
impl CompileCallbacks {
    fn new(cause: CompileControlError, stop_at: usize) -> Self {
        Self {
            cause,
            stop_at,
            trace: Mutex::new(Vec::new()),
            refused: Mutex::new(false),
        }
    }
}
impl PureCompileControl for CompileCallbacks {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut refused = self.refused.lock().unwrap();
        assert!(!*refused, "no compile callback after refusal");
        let mut trace = self.trace.lock().unwrap();
        trace.push((phase, units));
        if trace.len() == self.stop_at {
            *refused = true;
            Err(self.cause.clone())
        } else {
            Ok(())
        }
    }
}
#[test]
fn cast_compile_preserves_all_original_refusals_and_observes_ordinary_narrowing_rejection_tail() {
    for valid in [true, false] {
        let (functions, package) = fixture(
            FunctionValueType::new(DataType::Int64, false),
            FunctionValueType::new(DataType::Int8, valid),
            Source::Column,
            Wrap::Bare,
            DecimalOverflowPolicy::ReportError,
            true,
        );
        assert!(matches!(
            package.parameters().require(allow_ref()).unwrap(),
            SemanticParameterValue::AllowThrowException(true)
        ));
        let recorder = CompileCallbacks::new(CompileControlError::Cancelled, usize::MAX);
        let result = compile(&functions, package.clone(), &recorder);
        assert_eq!(result.is_ok(), valid);
        let trace = recorder.trace.lock().unwrap().clone();
        assert!(!trace.is_empty());
        for stop_at in 1..=trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = CompileCallbacks::new(cause.clone(), stop_at);
                assert!(
                    matches!(compile(&functions, package.clone(), &control), Err(FragmentCompileError::Control(actual)) if actual == cause)
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..stop_at]);
            }
        }
    }
}

#[path = "cast_float_tests.rs"]
mod cast_float_tests;

#[path = "cast_bool_tests.rs"]
mod cast_bool_tests;

#[path = "cast_unsigned_tests.rs"]
mod cast_unsigned_tests;

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

#[cfg(test)]
#[path = "cast_timestamp_tests.rs"]
mod cast_timestamp_tests;

#[cfg(test)]
#[path = "cast_temporal_carrier_tests.rs"]
mod cast_temporal_carrier_tests;

#[path = "cast_decimal_text_tests.rs"]
mod decimal_text_profile_tests;

#[path = "cast_largeint_text_tests.rs"]
mod largeint_text_profile_tests;

#[path = "cast_date_float_tests.rs"]
mod date_float_profile_tests;

#[path = "cast_float_date_tests.rs"]
mod float_date_profile_tests;

#[path = "cast_date_float_review_tests.rs"]
mod date_float_review_probe_tests;

#[path = "cast_binary_text_tests.rs"]
mod binary_text_profile_tests;

#[path = "cast_decimal_float_tests.rs"]
mod decimal_float_profile_tests;

#[path = "cast_decimal_float32_tests.rs"]
mod decimal_float32_profile_tests;

#[path = "cast_text_time_tests.rs"]
mod cast_text_time_tests;

#[path = "cast_decimal128_rescale_tests.rs"]
mod decimal128_rescale_profile_tests;

#[path = "cast_observed_list_tests.rs"]
mod observed_list_cast_profile_tests;

#[cfg(test)]
#[path = "cast_observed_list_after_tests.rs"]
mod observed_list_after_tests;

#[path = "cast_integral_decimal128_tests.rs"]
mod integral_decimal128_profile_tests;

#[path = "compiled_float64_decimal128_tests.rs"]
mod float64_decimal128_profile_tests;

#[test]
fn identity_cast_keeps_a_required_child_binding_and_permissive_source_carrier() {
    use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSlotSchema};
    use crate::exec::expr::compiled_program::input_source::CompiledInputLoan;
    use crate::exec::pipeline::operator::input_construction::{
        ConstructionFailure, try_runtime_input_construction_factory,
    };
    use crate::runtime::{mem_tracker::MemTracker, runtime_state::RuntimeErrorState};
    use arrow::array::{ArrayRef, FixedSizeBinaryArray, StructArray};
    use arrow::buffer::NullBuffer;
    use arrow::datatypes::Field;
    use novarocks_type_contract::ValueTypeStorageLifetime;

    let expected =
        DataType::Struct(vec![Field::new("f", DataType::FixedSizeBinary(4), false)].into());
    let ty = FunctionValueType::new(expected, true);
    let program = compiled(
        ty.clone(),
        ty,
        Source::Column,
        Wrap::Bare,
        DecimalOverflowPolicy::OutputNull,
        false,
    );
    let occurrence = cast_root_use(&program);
    assert!(
        root_unit(&program)
            .recipes()
            .cast(occurrence)
            .unwrap()
            .is_identity()
    );
    let layout = program.graph().nodes()[1].output_layout();
    let binding = layout.value_binding_at(0).unwrap();
    let expected = binding.data_type().clone();
    let DataType::Struct(fields) = &expected else {
        panic!("frozen source Struct")
    };
    assert!(!fields[0].is_nullable());
    let actual: ArrayRef = Arc::new(
        StructArray::try_new(
            vec![Arc::new(fields[0].as_ref().clone().with_nullable(true))].into(),
            vec![Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    [Some(b"abcd".as_slice()), None].into_iter(),
                    4,
                )
                .unwrap(),
            )],
            Some(NullBuffer::from(vec![true, false])),
        )
        .unwrap(),
    );
    let actual_type = actual.data_type().clone();
    let columns: Vec<ArrayRef> = vec![
        actual,
        Arc::new(Int64Array::from(vec![Some(42), Some(42)])),
        Arc::new(BooleanArray::from(vec![Some(true), Some(true)])),
    ];
    let pure = ValueTypeStorageLifetime::pure();
    let slots = columns
        .iter()
        .enumerate()
        .map(|(ordinal, column)| {
            ChunkSlotSchema::try_new_typed(
                layout.slots()[ordinal],
                Arc::new(
                    layout
                        .schema()
                        .field(ordinal)
                        .clone()
                        .with_data_type(column.data_type().clone()),
                ),
                layout.value_binding_at(ordinal).unwrap().clone(),
                None,
                pure.clone(),
            )
            .unwrap()
        })
        .collect();
    let schema = Arc::new(ChunkSchema::try_new_typed(slots, pure).unwrap());
    assert_eq!(schema.slots()[0].typed_binding(), binding);
    let factory = try_runtime_input_construction_factory(
        crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
        MemTracker::new_root("permissive identity Cast source construction"),
        Arc::new(RuntimeErrorState::default()),
    )
    .unwrap();
    let owner = factory.begin_typed_construction(&schema).unwrap();
    owner
        .observe(
            0,
            &[],
            crate::exec::chunk::ArrayNormalizationStep::CreateChunkColumns { channels: 3 },
        )
        .unwrap();
    let mut chunk = Chunk::try_new_typed_with_columns_owned_observed::<ConstructionFailure, _>(
        schema,
        columns,
        2,
        owner.storage_lifetime(),
        |step| owner.observe(0, &[], step),
    )
    .unwrap();
    let source_tracker = MemTracker::new_root("permissive identity Cast source payload");
    chunk.try_transfer_to(&source_tracker).unwrap();
    for selected in [vec![0, 1], vec![1]] {
        let selection = if selected.len() == 2 {
            Selection::all(2)
        } else {
            Selection::try_sparse(2, &selected).unwrap()
        };
        let host = crate::runtime::tracked_state_allocator::expression_allocation_host(
            MemTracker::new_root("permissive identity Cast result"),
        );
        let mut evaluator = CompiledExpressionInstance::try_new_with_allocator(
            root_unit(&program).clone(),
            root(&program).root,
            &Control,
            Some(host),
        )
        .unwrap();
        let result: crate::runtime::task::ExecutionResult<_> = evaluator.evaluate_input_controlled(
            CompiledInputLoan::from_chunk(&chunk),
            selection,
            selection,
            novarocks_functions::ScalarInvocationActivation::Activated,
            &Control,
            None,
        );
        let result = result.expect("an existing identity Cast preserves the legal source carrier");
        assert_eq!(result.values().data_type(), &actual_type);
        assert!(result.errors().is_empty());
        let values = result
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let child = values
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        for (ordinal, source_row) in selected.iter().copied().enumerate() {
            assert_eq!(values.is_null(ordinal), source_row == 1);
            if source_row == 0 {
                assert_eq!(child.value(ordinal), b"abcd");
            } else {
                assert!(child.is_null(ordinal));
            }
        }
    }
    drop(chunk);
    assert_eq!(source_tracker.current(), 0);
}
