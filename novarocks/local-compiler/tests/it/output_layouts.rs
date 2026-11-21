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

use super::fragment_fixture::{compile, install_claims, numeric_catalogue, options, package};
use arrow_schema::{DataType, IntervalUnit, TimeUnit};
use novarocks_constant_contract::ConstantPools;
use novarocks_expression::testkit::{
    ClaimAuthor, HarnessControl, add_call, checked_constants, register_constant,
};
use novarocks_expression_contract::{ExprKind, RootScope};
use novarocks_functions::builtin::value_conversion::{
    VALUE_CONVERSION_NAME, value_conversion_definition,
};
use novarocks_functions::testkit::rng_subset;
use novarocks_functions::{
    ConstantValue, EngineFunctionCatalogBuilder, FunctionArgument, FunctionId, FunctionKind,
    FunctionOverloadId, InstalledPureKernel, PureEngineFunctionCatalog,
    PureImplementationDeclaration, PureImplementationId, PureKernelAbi,
};
use novarocks_local_program::{ProgramChannelLayoutRole, ProgramChannelSite, ProgramNodeId};
use novarocks_physical_plan::{
    FragmentBuilder, FragmentId, NodeId, ResultField, ResultValueDomain, ValueDef, ValueId,
    ValueOrigin,
};
use novarocks_type_contract::{
    CompilePhase, ControlShape, EvaluationDemand, FunctionValueType, SemanticParameters,
    ValueLogicalType,
};
use std::{collections::BTreeMap, sync::Arc};

fn literal(
    ty: FunctionValueType,
    build: impl FnOnce(arrow_schema::FieldRef, FunctionValueType) -> ConstantValue,
) -> ConstantValue {
    build(Arc::new(ty.try_to_field("source").unwrap()), ty)
}

macro_rules! checked_value {
    ($ty:expr, $factory:ident($($payload:expr),* $(,)?) $(,)?) => {{
        literal($ty, |field, ty| {
            ConstantValue::$factory(
                field, ty, $($payload,)* options().constants, CompilePhase::Validate, &HarnessControl,
            ).unwrap()
        })
    }};
}

fn with_typed_nulls(cases: Vec<ConstantValue>) -> Vec<ConstantValue> {
    cases
        .into_iter()
        .flat_map(|value| {
            let ty = value.value_type().clone().with_nullable(true).unwrap();
            [value, checked_value!(ty, null())]
        })
        .collect()
}

fn literal_results(values: Vec<ConstantValue>) {
    let fragment = FragmentId::new(93);
    let source = NodeId::new(u32::MAX);
    let project = NodeId::new(31);
    let mut builder = FragmentBuilder::new(fragment);
    let mut constants = ConstantPools::empty();
    builder
        .add_values(source, Box::from([Box::default()]), Box::default())
        .unwrap();
    let mut output = Vec::new();
    let mut assignments = Vec::new();
    let mut fields = Vec::new();
    for (ordinal, constant) in values.iter().enumerate() {
        let ty = constant.value_type();
        let value = ValueId::new(900 + u32::try_from(ordinal).unwrap());
        let definition = builder
            .unit(project)
            .unwrap()
            .add(
                None,
                ty.clone(),
                ExprKind::Constant(register_constant(&mut constants, constant)),
            )
            .unwrap();
        let root = builder
            .unit(project)
            .unwrap()
            .root(definition, EvaluationDemand::Value, RootScope::Port(0))
            .unwrap();
        builder
            .insert_value(ValueDef {
                id: value,
                ty: ty.clone(),
                origin: ValueOrigin::Expr {
                    node: project,
                    root,
                },
            })
            .unwrap();
        assignments.push((root, value));
        output.push(value);
        fields.push(ResultField {
            domain: match ty.logical_type() {
                ValueLogicalType::Physical | ValueLogicalType::LargeInt => ResultValueDomain::Plain,
                ValueLogicalType::Json => ResultValueDomain::Json,
                ValueLogicalType::Variant => ResultValueDomain::Variant,
                _ => panic!("explicit literal domain"),
            },
            name: format!("literal_{ordinal}").into(),
            alias: None,
            value,
            ty: ty.clone(),
        });
    }
    builder
        .add_project(
            project,
            source,
            assignments.into_boxed_slice(),
            output.into_boxed_slice(),
        )
        .unwrap();
    let package = package(
        93,
        builder,
        project,
        fields.into_boxed_slice(),
        SemanticParameters::try_new([]).unwrap(),
        constants,
        options().constants,
    );
    let program = compile(&rng_subset(), package, &HarnessControl).unwrap();
    assert!(
        program.graph().nodes()[0]
            .output_layout()
            .schema()
            .fields()
            .is_empty()
    );
    for (ordinal, constant) in values.iter().enumerate() {
        assert_eq!(
            program
                .channels()
                .channel_type(ProgramChannelSite::Layout {
                    node: ProgramNodeId::new(1),
                    role: ProgramChannelLayoutRole::NodeOutput,
                    ordinal: u32::try_from(ordinal).unwrap(),
                })
                .unwrap(),
            constant.value_type(),
        );
    }
}

#[test]
fn literal_project_outputs_keep_declared_carrier_nullability_and_nominal_type() {
    let mut negative_2pow200 = [0; 32];
    negative_2pow200[..7].fill(0xff);
    let largeint = FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        false,
        ValueLogicalType::LargeInt,
    )
    .unwrap();
    let mut values = vec![
        checked_value!(
            FunctionValueType::new(DataType::UInt64, false),
            from_u64(u64::MAX)
        ),
        checked_value!(largeint, from_largeint(i128::MIN)),
        checked_value!(
            FunctionValueType::new(DataType::Decimal128(38, -2), false),
            from_decimal128(-123456789)
        ),
        checked_value!(
            FunctionValueType::new(DataType::Decimal256(76, -3), false),
            from_decimal256_be(negative_2pow200)
        ),
    ];
    for bits in [(-0.0_f64).to_bits(), 0x7ff8_1234_5678_9abc] {
        values.push(checked_value!(
            FunctionValueType::new(DataType::Float64, false),
            from_f64_bits(bits)
        ));
    }
    values.extend([
        checked_value!(
            FunctionValueType::new(DataType::Date32, false),
            from_date32(-7)
        ),
        checked_value!(
            FunctionValueType::new(DataType::Time64(TimeUnit::Microsecond), false),
            from_time64(86399999999)
        ),
        checked_value!(
            FunctionValueType::new(DataType::Time64(TimeUnit::Nanosecond), false),
            from_time64(123456789)
        ),
    ]);
    for parts in [
        (i32::MIN, i32::MAX, i64::MIN),
        (i32::MAX, i32::MIN, i64::MAX),
        (-17, -31, -123456789),
        (0, 0, 0),
    ] {
        values.push(checked_value!(
            FunctionValueType::new(DataType::Interval(IntervalUnit::MonthDayNano), false),
            from_interval_month_day_nano(parts),
        ));
    }
    for unit in [
        TimeUnit::Second,
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ] {
        for zone in [None, Some(Arc::<str>::from("Europe/Berlin"))] {
            values.push(checked_value!(
                FunctionValueType::new(DataType::Timestamp(unit, zone), false),
                from_timestamp(-1234567),
            ));
        }
    }
    literal_results(with_typed_nulls(values));

    let text = format!("prefix\0é🙂{}suffix", "x".repeat(2048));
    let binary = [vec![0, 0xff, 0x80], vec![0x5a; 4096], vec![0]].concat();
    let mut values = Vec::new();
    for carrier in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
        values.push(checked_value!(
            FunctionValueType::new(carrier, false),
            from_utf8(&text)
        ));
    }
    for carrier in [
        DataType::Binary,
        DataType::LargeBinary,
        DataType::BinaryView,
    ] {
        values.push(checked_value!(
            FunctionValueType::new(carrier, false),
            from_binary(&binary)
        ));
    }
    let json =
        FunctionValueType::try_with_logical_type(DataType::Utf8, false, ValueLogicalType::Json)
            .unwrap();
    let variant = FunctionValueType::try_with_logical_type(
        DataType::LargeBinary,
        false,
        ValueLogicalType::Variant,
    )
    .unwrap();
    values.push(checked_value!(json, from_utf8("{\"value\":42}")));
    values.push(checked_value!(variant, from_binary(&[0, 0xff, 0x32, 0x80])));
    literal_results(with_typed_nulls(values));
}

fn conversion_catalogue() -> PureEngineFunctionCatalog {
    let actual = numeric_catalogue();
    let mut builder = EngineFunctionCatalogBuilder::new();
    let mut manifest = Vec::new();
    for (name, overloads, abi) in [
        (
            "if",
            &["(bool,any<T>,any<T>)->any<T>;widen;legacy"][..],
            PureKernelAbi::ControlIntrinsicV1,
        ),
        (
            "coalesce",
            &["(any<T>...)->any<T>;widen;legacy"][..],
            PureKernelAbi::ControlIntrinsicV1,
        ),
        ("round", &["dynamic-v1"][..], PureKernelAbi::ScalarV1),
    ] {
        builder
            .register(
                actual
                    .metadata()
                    .definition(name, FunctionKind::Scalar)
                    .unwrap()
                    .clone(),
            )
            .unwrap();
        for overload in overloads {
            manifest.push(InstalledPureKernel {
                function: FunctionId::try_new(format!("builtin.scalar/{name}/v1")).unwrap(),
                kind: FunctionKind::Scalar,
                implementation: PureImplementationDeclaration {
                    overload: FunctionOverloadId::try_new(format!(
                        "builtin.scalar/{name}/{overload}"
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
    builder
        .register(value_conversion_definition().unwrap())
        .unwrap();
    // This independent installed manifest covers every implementation
    // of the original hidden definition, without claiming a Server catalogue.
    for stem in [
        "json_text_same_structure",
        "opaque_to_binary_same_structure",
        "signed_to_largeint",
        "boolean_to_largeint",
        "decimal128_to_largeint",
        "decimal256_to_largeint",
        "float32_to_largeint_null_invalid",
        "float64_to_largeint_null_invalid",
        "largeint_to_signed_null_overflow",
        "largeint_to_decimal_exact",
        "largeint_to_float_round",
        "largeint_to_utf8_text",
        "utf8_to_largeint_null_invalid",
        "null_to_typed_nullable",
        "array_common_coercion",
        "nested_temporal_write_assignment",
        "temporal",
        "temporal_clock",
        "temporal_widen",
        "temporal_zone",
        "try_temporal",
        "try_temporal_clock",
        "try_temporal_zone",
    ] {
        manifest.push(InstalledPureKernel {
            function: FunctionId::try_new("builtin.scalar/value_domain_conversion/v1").unwrap(),
            kind: FunctionKind::Scalar,
            implementation: PureImplementationDeclaration {
                overload: FunctionOverloadId::try_new(format!(
                    "builtin.scalar/value_domain_conversion/{stem}/v1"
                ))
                .unwrap(),
                implementation: PureImplementationId::try_new(format!(
                    "builtin.scalar/value_domain_conversion/{stem}/selected-v1"
                ))
                .unwrap(),
                abi: PureKernelAbi::ScalarV1,
            },
            aggregate_state_format: None,
        });
    }
    builder.seal_pure(manifest).unwrap()
}

#[test]
fn conversion_project_output_layout_keeps_largeint_after_each_signed_width() {
    let functions = conversion_catalogue();
    let target = FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        true,
        ValueLogicalType::LargeInt,
    )
    .unwrap();
    for carrier in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
    ] {
        let mut constants = ConstantPools::empty();
        let parameters = SemanticParameters::try_new([]).unwrap();
        let source_type = FunctionValueType::new(carrier, true);
        let id = FragmentId::new(229);
        let source = NodeId::new(u32::MAX);
        let input = NodeId::new(41);
        let output = NodeId::new(0);
        let mut builder = FragmentBuilder::new(id);
        builder
            .add_values(source, Box::from([Box::default()]), Box::default())
            .unwrap();
        let null = checked_constants::kind(
            &mut constants,
            checked_constants::null(&source_type, options().constants),
        );
        let definition = builder
            .unit(input)
            .unwrap()
            .add(None, source_type.clone(), null)
            .unwrap();
        let input_root = builder
            .unit(input)
            .unwrap()
            .root(definition, EvaluationDemand::Value, RootScope::Port(0))
            .unwrap();
        let input_value = ValueId::new(901);
        builder
            .insert_value(ValueDef {
                id: input_value,
                ty: source_type.clone(),
                origin: ValueOrigin::Expr {
                    node: input,
                    root: input_root,
                },
            })
            .unwrap();
        builder
            .add_project(
                input,
                source,
                Box::from([(input_root, input_value)]),
                Box::from([input_value]),
            )
            .unwrap();
        let draft = builder.unit(output).unwrap();
        let leaf = draft
            .add(None, source_type.clone(), ExprKind::Value(input_value))
            .unwrap();
        let author = ClaimAuthor::with_legacy_binding(
            &functions,
            VALUE_CONVERSION_NAME,
            vec![FunctionArgument::Value {
                value_type: source_type,
                constant: None,
            }],
            ControlShape::Eager,
            Some(target.clone()),
            options().constants,
            &HarnessControl,
        );
        let result_type = author.result();
        let mut authors = BTreeMap::new();
        let conversion = add_call(draft, &mut authors, author, vec![leaf]);
        let output_root = draft
            .root(conversion, EvaluationDemand::Value, RootScope::Port(0))
            .unwrap();
        let value = builder
            .add_value(
                result_type.clone(),
                ValueOrigin::Expr {
                    node: output,
                    root: output_root,
                },
            )
            .unwrap();
        builder
            .add_project(
                output,
                input,
                Box::from([(output_root, value)]),
                Box::from([value]),
            )
            .unwrap();
        install_claims(
            &mut builder,
            output,
            &mut constants,
            &functions,
            &authors,
            &parameters,
        );
        let fields = Box::from([ResultField {
            domain: ResultValueDomain::Plain,
            name: "conversion_result".into(),
            alias: None,
            value,
            ty: result_type,
        }]);
        let package = package(
            193,
            builder,
            output,
            fields,
            parameters,
            constants,
            options().constants,
        );
        let program = compile(&functions, package, &HarnessControl).unwrap();
        let binding = program.graph().nodes()[2]
            .output_layout()
            .value_binding_at(0)
            .unwrap();
        let actual = FunctionValueType::try_from_typed_binding(binding.clone()).unwrap();
        assert_eq!(actual, target);
        assert_eq!(
            program
                .channels()
                .channel_type(ProgramChannelSite::Layout {
                    node: ProgramNodeId::new(2),
                    role: ProgramChannelLayoutRole::NodeOutput,
                    ordinal: 0,
                })
                .unwrap(),
            &target,
        );
    }
}
