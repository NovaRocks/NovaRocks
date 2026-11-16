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

use super::*;
use arrow_schema::TimeUnit;
use novarocks_physical_plan::ExprId;
use novarocks_type_contract::ValueLogicalType;

fn literal_package(ty: FunctionValueType, literal: LiteralValue) -> Arc<FragmentPackage> {
    let mut builder = FragmentBuilder::new(FragmentId::new(63));
    builder
        .add_values(NodeId::new(99), Box::from([Box::default()]), Box::default())
        .unwrap();
    let expression = builder
        .add_expression(NodeId::new(8), ty.clone(), ExprKind::Literal(literal))
        .unwrap();
    let value = builder
        .add_value(
            ty.clone(),
            ValueOrigin::Expr {
                node: NodeId::new(8),
                expr: expression,
            },
        )
        .unwrap();
    builder
        .add_project(
            NodeId::new(8),
            NodeId::new(99),
            Box::from([(expression, value)]),
            Box::from([value]),
        )
        .unwrap();
    let fragment = builder
        .finish_definition(
            NodeId::new(8),
            FragmentSink::Result,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let roots = PhysicalExpressionRoots::try_new(&fragment, &FixtureControl).unwrap();
    let domain = EvaluationDomainId::new(17);
    let mut invocations = vec![];
    let bindings = roots
        .sites()
        .iter()
        .enumerate()
        .map(|(ordinal, (&site, root))| {
            let use_id = ExpressionUseId::new(ordinal as u32 + 42);
            invocations.push(ExpressionInvocation {
                context: ExpressionEffectContext {
                    use_id,
                    domain,
                    demand: root.demand,
                },
                definition: root.expr,
                control: ControlShape::Eager,
                arguments: Box::default(),
            });
            (site, use_id)
        })
        .collect();
    let flow = ExpressionControlFlow::<ExprId>::try_new(
        vec![ExpressionEvaluationDomain {
            id: domain,
            parent: None,
            guard: None,
        }],
        invocations,
        fragment.expressions(),
        CompilePhase::Validate,
        &FixtureControl,
    )
    .unwrap();
    let expression_uses =
        PhysicalRootUses::try_new(&fragment, flow, bindings, &FixtureControl).unwrap();
    let calls =
        FrozenFragmentCalls::try_new(&fragment, &expression_uses, vec![], &FixtureControl).unwrap();
    let result = ResultPort {
        scalar_schema: None,
        fragment: fragment.id(),
        output: fragment.nodes()[&fragment.root()].output.clone(),
        fields: Box::from([ResultField {
            domain: novarocks_physical_plan::ResultValueDomain::Plain,
            name: "literal".into(),
            alias: None,
            value,
            ty,
        }]),
    };
    let pruning = FrozenFragmentPruning::try_new(fragment.id(), vec![], &FixtureControl).unwrap();
    Arc::new(
        FragmentPackage::try_new(
            FragmentPackageInput {
                version: PlanVersionId::try_new([63; 16]).unwrap(),
                required: RequiredContracts::default(),
                constants: novarocks_physical_plan::ConstantPools::empty(),
                fragment,
                expression_uses,
                calls,
                cuts: FragmentCuts::default(),
                result: Some(result),
                parameters: SemanticParameters::try_new([]).unwrap(),
                scans: BTreeMap::new(),
                writes: BTreeMap::new(),
                annotations: Box::default(),
                pruning,
                layouts: Default::default(),
            },
            package_admission(),
            &FixtureControl,
        )
        .unwrap(),
    )
}
fn compile_literal(
    package: Arc<FragmentPackage>,
    control: &dyn PureCompileControl,
) -> Result<novarocks_local_program::LocalProgram, FragmentCompileError> {
    let providers =
        PureProviderProgramCatalog::<std::io::Error>::try_new(&[], vec![], &FixtureControl)
            .unwrap();
    compile_fragment(
        validate_fragment_providers(package, &providers, &FixtureControl).unwrap(),
        &rng_subset(),
        options(1),
        control,
    )
}
fn cases() -> Vec<(FunctionValueType, LiteralValue)> {
    let mut cases = vec![
        (
            FunctionValueType::new(DataType::Boolean, false),
            LiteralValue::Boolean(true),
        ),
        (
            FunctionValueType::new(DataType::Int64, false),
            LiteralValue::Int64(i64::MIN),
        ),
        (
            FunctionValueType::new(DataType::UInt64, false),
            LiteralValue::UInt64(u64::MAX),
        ),
        (
            FunctionValueType::new(DataType::Float64, false),
            LiteralValue::Float64Bits(0x7ff8_0000_0000_0042),
        ),
        (
            FunctionValueType::try_with_logical_type(
                DataType::FixedSizeBinary(16),
                false,
                ValueLogicalType::LargeInt,
            )
            .unwrap(),
            LiteralValue::LargeInt(i128::MIN),
        ),
        (
            FunctionValueType::new(DataType::Decimal128(38, -3), false),
            LiteralValue::Decimal128(-123),
        ),
        (
            FunctionValueType::new(DataType::Decimal256(76, 12), false),
            LiteralValue::Decimal256([255; 32]),
        ),
        (
            FunctionValueType::new(DataType::Date32, false),
            LiteralValue::Date32(-1234),
        ),
    ];
    cases.push((
        FunctionValueType::try_with_logical_type(DataType::Utf8, false, ValueLogicalType::Json)
            .unwrap(),
        LiteralValue::Utf8("{\"x\":17}".into()),
    ));
    cases.push((
        FunctionValueType::try_with_logical_type(
            DataType::LargeBinary,
            false,
            ValueLogicalType::Variant,
        )
        .unwrap(),
        LiteralValue::Binary(vec![0, 17, 255].into()),
    ));
    for carrier in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
        cases.push((
            FunctionValueType::new(carrier, false),
            LiteralValue::Utf8("µ\0雪".repeat(80).into()),
        ));
    }
    for carrier in [
        DataType::Binary,
        DataType::LargeBinary,
        DataType::BinaryView,
    ] {
        cases.push((
            FunctionValueType::new(carrier, false),
            LiteralValue::Binary([0, 255, 17, 128].repeat(80).into()),
        ));
    }
    for unit in [TimeUnit::Microsecond, TimeUnit::Nanosecond] {
        cases.push((
            FunctionValueType::new(DataType::Time64(unit), false),
            LiteralValue::Time64(-17),
        ));
    }
    for unit in [
        TimeUnit::Second,
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ] {
        for zone in [None, Some("+05:30".into())] {
            cases.push((
                FunctionValueType::new(DataType::Timestamp(unit, zone), false),
                LiteralValue::Timestamp(-7654),
            ));
        }
    }
    for (months, days, nanoseconds) in [
        (i32::MIN, i32::MAX, i64::MIN),
        (i32::MAX, i32::MIN, i64::MAX),
        (-17, -31, -123456789),
        (0, 0, 0),
    ] {
        cases.push((
            FunctionValueType::new(
                DataType::Interval(arrow_schema::IntervalUnit::MonthDayNano),
                false,
            ),
            LiteralValue::IntervalMonthDayNano {
                months,
                days,
                nanoseconds,
            },
        ));
    }
    cases
}
#[test]
fn exact_scalar_literals_compile_through_the_complete_package_owner() {
    for (ty, literal) in cases() {
        for nullable in [false, true] {
            let ty = (ty.clone())
                .with_nullable(nullable)
                .expect("valid value root nullability transform");
            let program = compile_literal(
                literal_package(ty.clone(), literal.clone()),
                &FixtureControl,
            )
            .unwrap();
            let snapshot = program
                .checked()
                .channels()
                .expressions()
                .resolved_calls()
                .snapshot();
            let arena = &snapshot.roots().arenas()[&ProgramExpressionArena::Main];
            let constants = arena
                .nodes()
                .iter()
                .filter_map(|node| match node.kind() {
                    StaticExprKind::Constant(value) => Some(value),
                    _ => None,
                })
                .collect::<Vec<_>>();
            assert_eq!(constants.len(), 1);
            let value = constants[0];
            assert_eq!(value.value_type(), &ty);
            assert_eq!(value.pool().array().data_type(), ty.data_type());
            assert_eq!(value.pool().array().len(), 1);
            assert!(!value.pool().array().is_null(0));
            match &literal {
                LiteralValue::Boolean(x) => assert_eq!(value.try_boolean().unwrap(), Some(*x)),
                LiteralValue::Int64(x) => assert_eq!(value.try_i64().unwrap(), Some(*x)),
                LiteralValue::UInt64(x) => assert_eq!(value.try_u64().unwrap(), Some(*x)),
                LiteralValue::Float64Bits(x) => assert_eq!(value.try_f64_bits().unwrap(), Some(*x)),
                LiteralValue::LargeInt(x) => assert_eq!(value.try_largeint().unwrap(), Some(*x)),
                LiteralValue::Decimal128(x) => {
                    assert_eq!(value.try_decimal128().unwrap(), Some(*x))
                }
                LiteralValue::Decimal256(x) => {
                    assert_eq!(value.try_decimal256_be().unwrap(), Some(*x))
                }
                LiteralValue::Utf8(x) => assert_eq!(value.try_utf8().unwrap(), Some(x.as_ref())),
                LiteralValue::Binary(x) => {
                    assert_eq!(value.try_binary().unwrap(), Some(x.as_ref()))
                }
                LiteralValue::Date32(x) => assert_eq!(value.try_date32().unwrap(), Some(*x)),
                LiteralValue::Time64(x) => assert_eq!(value.try_time64().unwrap(), Some(*x)),
                LiteralValue::Timestamp(x) => assert_eq!(value.try_timestamp().unwrap(), Some(*x)),
                LiteralValue::IntervalMonthDayNano {
                    months,
                    days,
                    nanoseconds,
                } => {
                    assert_eq!(
                        value.try_interval_month_day_nano().unwrap(),
                        Some((*months, *days, *nanoseconds))
                    );
                }
                _ => panic!("fixture has a ready non-NULL scalar source"),
            }
        }
    }
}
#[test]
fn null_literals_preserve_the_declared_complete_type() {
    for (mut ty, _) in cases() {
        ty = ty.with_nullable(true).unwrap();
        let program = compile_literal(
            literal_package(ty.clone(), LiteralValue::Null),
            &FixtureControl,
        )
        .unwrap();
        let snapshot = program
            .checked()
            .channels()
            .expressions()
            .resolved_calls()
            .snapshot();
        let arena = &snapshot.roots().arenas()[&ProgramExpressionArena::Main];
        let value = arena
            .nodes()
            .iter()
            .find_map(|node| match node.kind() {
                StaticExprKind::Constant(value) => Some(value),
                _ => None,
            })
            .unwrap();
        assert_eq!(value.value_type(), &ty);
        assert!(
            value
                .is_null_observed(CompilePhase::Validate, &FixtureControl)
                .unwrap()
        );
    }
}
#[test]
fn explicit_interval_components_reach_the_actual_project_root_without_packed_integer_inference() {
    let ty = FunctionValueType::new(
        DataType::Interval(arrow_schema::IntervalUnit::MonthDayNano),
        false,
    );
    let program = compile_literal(
        literal_package(
            ty.clone(),
            LiteralValue::IntervalMonthDayNano {
                months: -7,
                days: 23,
                nanoseconds: i64::MIN,
            },
        ),
        &FixtureControl,
    )
    .unwrap();
    let snapshot = program
        .checked()
        .channels()
        .expressions()
        .resolved_calls()
        .snapshot();
    let site = ProgramExpressionRootSite::Node {
        node: ProgramNodeId::new(1),
        role: ProgramNodeExpressionRole::ProjectOutput { expression: 0 },
    };
    let root = snapshot.roots().sites().get(&site).unwrap();
    assert_eq!(root.demand, EvaluationDemand::Value);
    let arena = &snapshot.roots().arenas()[&ProgramExpressionArena::Main];
    let node = arena.node(root.definition).unwrap();
    let StaticExprKind::Constant(value) = node.kind() else {
        panic!("actual interval root must retain its authored ConstantValue");
    };
    assert_eq!(value.value_type(), &ty);
    assert_eq!(
        value.try_interval_month_day_nano().unwrap(),
        Some((-7, 23, i64::MIN))
    );
    let root_use = snapshot.bindings().get(&site).unwrap();
    let invocation = &snapshot.flows()[&ProgramExpressionArena::Main].uses()[root_use];
    assert_eq!(invocation.definition, root.definition);
    assert!(invocation.arguments.is_empty());
}

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
