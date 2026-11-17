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
use novarocks_functions::ConstantValue;
use novarocks_physical_plan::ExprId;
use novarocks_type_contract::ValueLogicalType;

fn literal_package(value: ConstantValue) -> Arc<FragmentPackage> {
    use novarocks_physical_plan::{ConstantPoolId, ConstantPools, ConstantReference};
    let ty = value.value_type().clone();
    let pool = ConstantPoolId::new(0);
    let mut constants = ConstantPools::empty();
    constants.insert(pool, value.pool().clone()).unwrap();
    let mut builder = FragmentBuilder::new(FragmentId::new(63));
    builder
        .add_values(NodeId::new(99), Box::from([Box::default()]), Box::default())
        .unwrap();
    let expression = builder
        .add_expression(
            NodeId::new(8),
            ty.clone(),
            ExprKind::Constant(ConstantReference {
                pool,
                ordinal: value.ordinal(),
            }),
        )
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
    let domain = EvaluationRegionId::new(17);
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
                    region: domain,
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
        vec![ExpressionEvaluationRegion {
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
                constants,
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
struct ScalarCase {
    value: ConstantValue,
    check: Box<dyn Fn(&ConstantValue)>,
}
fn cases(nullable: bool) -> Vec<ScalarCase> {
    let mut cases = Vec::new();
    macro_rules! case {
        ($ty:expr, $factory:ident($($payload:expr),*), $getter:ident, $expected:expr) => {{
            let ty = $ty.with_nullable(nullable).unwrap();
            let value = ConstantValue::$factory(
                Arc::new(ty.try_to_field("source").unwrap()),
                ty,
                $($payload,)*
                options(1).constants,
                CompilePhase::Validate,
                &FixtureControl,
            ).unwrap();
            let expected = $expected;
            cases.push(ScalarCase {
                value,
                check: Box::new(move |actual| assert_eq!(actual.$getter().unwrap(), Some(expected))),
            });
        }};
    }
    case!(
        FunctionValueType::new(DataType::Boolean, false),
        from_boolean(true),
        try_boolean,
        true
    );
    case!(
        FunctionValueType::new(DataType::Int64, false),
        from_i64(i64::MIN),
        try_i64,
        i64::MIN
    );
    case!(
        FunctionValueType::new(DataType::UInt64, false),
        from_u64(u64::MAX),
        try_u64,
        u64::MAX
    );
    case!(
        FunctionValueType::new(DataType::Float64, false),
        from_f64_bits(0x7ff8_0000_0000_0042),
        try_f64_bits,
        0x7ff8_0000_0000_0042
    );
    case!(
        FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            false,
            ValueLogicalType::LargeInt
        )
        .unwrap(),
        from_largeint(i128::MIN),
        try_largeint,
        i128::MIN
    );
    case!(
        FunctionValueType::new(DataType::Decimal128(38, -3), false),
        from_decimal128(-123),
        try_decimal128,
        -123
    );
    case!(
        FunctionValueType::new(DataType::Decimal256(76, 12), false),
        from_decimal256_be([255; 32]),
        try_decimal256_be,
        [255; 32]
    );
    case!(
        FunctionValueType::new(DataType::Date32, false),
        from_date32(-1234),
        try_date32,
        -1234
    );
    case!(
        FunctionValueType::try_with_logical_type(DataType::Utf8, false, ValueLogicalType::Json)
            .unwrap(),
        from_utf8("{\"x\":17}"),
        try_utf8,
        "{\"x\":17}"
    );
    case!(
        FunctionValueType::try_with_logical_type(
            DataType::LargeBinary,
            false,
            ValueLogicalType::Variant
        )
        .unwrap(),
        from_binary(&[0, 17, 255]),
        try_binary,
        &[0, 17, 255][..]
    );
    for carrier in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
        let ty = FunctionValueType::new(carrier, nullable);
        let expected = "µ\0雪".repeat(80);
        let value = ConstantValue::from_utf8(
            Arc::new(ty.try_to_field("source").unwrap()),
            ty,
            &expected,
            options(1).constants,
            CompilePhase::Validate,
            &FixtureControl,
        )
        .unwrap();
        cases.push(ScalarCase {
            value,
            check: Box::new(move |actual| {
                assert_eq!(actual.try_utf8().unwrap(), Some(expected.as_str()))
            }),
        });
    }
    for carrier in [
        DataType::Binary,
        DataType::LargeBinary,
        DataType::BinaryView,
    ] {
        let ty = FunctionValueType::new(carrier, nullable);
        let expected = [0, 255, 17, 128].repeat(80);
        let value = ConstantValue::from_binary(
            Arc::new(ty.try_to_field("source").unwrap()),
            ty,
            &expected,
            options(1).constants,
            CompilePhase::Validate,
            &FixtureControl,
        )
        .unwrap();
        cases.push(ScalarCase {
            value,
            check: Box::new(move |actual| {
                assert_eq!(actual.try_binary().unwrap(), Some(expected.as_slice()))
            }),
        });
    }
    for unit in [TimeUnit::Microsecond, TimeUnit::Nanosecond] {
        case!(
            FunctionValueType::new(DataType::Time64(unit), false),
            from_time64(-17),
            try_time64,
            -17
        );
    }
    for unit in [
        TimeUnit::Second,
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ] {
        for zone in [None, Some("+05:30".into())] {
            case!(
                FunctionValueType::new(DataType::Timestamp(unit, zone), false),
                from_timestamp(-7654),
                try_timestamp,
                -7654
            );
        }
    }
    for (months, days, nanoseconds) in [
        (i32::MIN, i32::MAX, i64::MIN),
        (i32::MAX, i32::MIN, i64::MAX),
        (-17, -31, -123456789),
        (0, 0, 0),
    ] {
        case!(
            FunctionValueType::new(
                DataType::Interval(arrow_schema::IntervalUnit::MonthDayNano),
                false
            ),
            from_interval_month_day_nano((months, days, nanoseconds)),
            try_interval_month_day_nano,
            (months, days, nanoseconds)
        );
    }
    cases
}
#[test]
fn exact_scalar_literals_compile_through_the_complete_package_owner() {
    for nullable in [false, true] {
        for case in cases(nullable) {
            let ty = case.value.value_type().clone();
            let program = compile_literal(literal_package(case.value), &FixtureControl).unwrap();
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
            (case.check)(value);
        }
    }
}
#[test]
fn null_literals_preserve_the_declared_complete_type() {
    for case in cases(true) {
        let ty = case.value.value_type().clone();
        let null = ConstantValue::null(
            Arc::new(ty.try_to_field("source").unwrap()),
            ty.clone(),
            options(1).constants,
            CompilePhase::Validate,
            &FixtureControl,
        )
        .unwrap();
        let program = compile_literal(literal_package(null), &FixtureControl).unwrap();
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
            ConstantValue::from_interval_month_day_nano(
                Arc::new(ty.try_to_field("source").unwrap()),
                ty.clone(),
                (-7, 23, i64::MIN),
                options(1).constants,
                CompilePhase::Validate,
                &FixtureControl,
            )
            .unwrap(),
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
