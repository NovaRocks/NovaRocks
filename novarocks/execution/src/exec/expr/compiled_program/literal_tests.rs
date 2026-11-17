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
use arrow::{
    array::{
        ArrayRef, BinaryArray, BinaryViewArray, Date32Array, Decimal128Array, Decimal256Array,
        FixedSizeBinaryArray, IntervalMonthDayNanoArray, LargeBinaryArray, LargeStringArray,
        StringArray, StringViewArray, Time64MicrosecondArray, Time64NanosecondArray,
        TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
        TimestampSecondArray, UInt64Array,
    },
    datatypes::{IntervalUnit, TimeUnit},
    record_batch::RecordBatchOptions,
};
use novarocks_local_program::{ProgramChannelLayoutRole, ProgramChannelSite};
use novarocks_type_contract::ValueLogicalType;

fn literal_program(cases: &[(FunctionValueType, LiteralValue)]) -> Arc<LocalProgram> {
    let fragment_id = FragmentId::new(93);
    let source = NodeId::new(u32::MAX);
    let project = NodeId::new(31);
    let mut builder = FragmentBuilder::new(fragment_id);
    builder
        .add_values(source, Box::from([Box::default()]), Box::default())
        .unwrap();
    let mut output = Vec::new();
    let mut assignments = Vec::new();
    let mut result_fields = Vec::new();
    for (ordinal, (ty, literal)) in cases.iter().enumerate() {
        let value = ValueId::new(900 + u32::try_from(ordinal).unwrap());
        let expr = builder
            .add_expression(project, ty.clone(), ExprKind::Literal(literal.clone()))
            .unwrap();
        builder
            .insert_value(ValueDef {
                id: value,
                ty: ty.clone(),
                origin: ValueOrigin::Expr {
                    node: project,
                    expr,
                },
            })
            .unwrap();
        assignments.push((expr, value));
        output.push(value);
        result_fields.push(ResultField {
            domain: crate::test_result_domain::result_value_domain(ty),
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
    let fragment = builder
        .finish_definition(
            project,
            FragmentSink::Result,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let actual_roots = PhysicalExpressionRoots::try_new(&fragment, &FixtureControl).unwrap();
    let mut next = 700;
    let mut uses = Vec::new();
    let roots = actual_roots
        .sites()
        .iter()
        .map(|(site, root)| {
            (
                *site,
                invocation(&fragment, root.expr, root.demand, &mut next, &mut uses),
            )
        })
        .collect();
    let flow = ExpressionControlFlow::try_new(
        vec![ExpressionEvaluationRegion {
            id: EvaluationRegionId::new(u32::MAX),
            parent: None,
            guard: None,
        }],
        uses,
        fragment.expressions(),
        CompilePhase::Validate,
        &FixtureControl,
    )
    .unwrap();
    let expression_uses =
        PhysicalRootUses::try_new(&fragment, flow, roots, &FixtureControl).unwrap();
    // There are no function calls: retain an explicitly checked empty call table.
    let calls =
        FrozenFragmentCalls::try_new(&fragment, &expression_uses, vec![], &FixtureControl).unwrap();
    let result = ResultPort {
        scalar_schema: None,
        fragment: fragment_id,
        output: fragment.nodes()[&project].output.clone(),
        fields: result_fields.into_boxed_slice(),
    };
    let package = Arc::new(
        FragmentPackage::try_new(
            FragmentPackageInput {
                version: PlanVersionId::try_new([93; 16]).unwrap(),
                required: RequiredContracts::default(),
                constants: novarocks_physical_plan::ConstantPools::empty(),
                pruning: FrozenFragmentPruning::try_new(fragment_id, vec![], &FixtureControl)
                    .unwrap(),
                fragment,
                expression_uses,
                calls,
                cuts: FragmentCuts::default(),
                result: Some(result),
                parameters: SemanticParameters::try_new([]).unwrap(),
                scans: BTreeMap::new(),
                writes: BTreeMap::new(),
                annotations: Box::default(),
                layouts: Default::default(),
            },
            package_admission(),
            &FixtureControl,
        )
        .unwrap(),
    );
    let providers =
        PureProviderProgramCatalog::<std::io::Error>::try_new(&[], vec![], &FixtureControl)
            .unwrap();
    let validated = validate_fragment_providers(package, &providers, &FixtureControl).unwrap();
    // The genuine installed RAND subset is sufficient for this no-call package;
    // this does not manufacture an empty catalogue seal or a kernel receipt.
    Arc::new(compile_fragment(validated, &rng_subset(), options(1), &FixtureControl).unwrap())
}

fn assert_literal_payload(array: &ArrayRef, ty: &DataType, literal: &LiteralValue, rows: usize) {
    assert_eq!(array.len(), rows);
    assert_eq!(array.data_type(), ty);
    for row in 0..rows {
        if matches!(literal, LiteralValue::Null) {
            assert!(array.is_null(row));
            continue;
        }
        assert!(!array.is_null(row));
        macro_rules! value {
            ($array:ty, $expected:expr) => {
                assert_eq!(
                    array.as_any().downcast_ref::<$array>().unwrap().value(row),
                    $expected
                )
            };
        }
        match literal {
            LiteralValue::UInt64(expected) => value!(UInt64Array, *expected),
            LiteralValue::Float64Bits(expected) => {
                assert_eq!(
                    array
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap()
                        .value(row)
                        .to_bits(),
                    *expected
                );
            }
            LiteralValue::LargeInt(expected) => {
                value!(FixedSizeBinaryArray, expected.to_be_bytes().as_slice())
            }
            LiteralValue::Decimal128(expected) => value!(Decimal128Array, *expected),
            LiteralValue::Decimal256(expected) => {
                assert_eq!(
                    array
                        .as_any()
                        .downcast_ref::<Decimal256Array>()
                        .unwrap()
                        .value(row)
                        .to_be_bytes(),
                    *expected
                );
            }
            LiteralValue::IntervalMonthDayNano {
                months,
                days,
                nanoseconds,
            } => {
                let actual = array
                    .as_any()
                    .downcast_ref::<IntervalMonthDayNanoArray>()
                    .unwrap()
                    .value(row);
                assert_eq!(actual.months, *months);
                assert_eq!(actual.days, *days);
                assert_eq!(actual.nanoseconds, *nanoseconds);
            }
            LiteralValue::Date32(expected) => value!(Date32Array, *expected),
            LiteralValue::Time64(expected) => match ty {
                DataType::Time64(TimeUnit::Microsecond) => {
                    value!(Time64MicrosecondArray, *expected)
                }
                DataType::Time64(TimeUnit::Nanosecond) => value!(Time64NanosecondArray, *expected),
                _ => panic!("unexpected literal time carrier"),
            },
            LiteralValue::Timestamp(expected) => match ty {
                DataType::Timestamp(TimeUnit::Second, _) => value!(TimestampSecondArray, *expected),
                DataType::Timestamp(TimeUnit::Millisecond, _) => {
                    value!(TimestampMillisecondArray, *expected)
                }
                DataType::Timestamp(TimeUnit::Microsecond, _) => {
                    value!(TimestampMicrosecondArray, *expected)
                }
                DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                    value!(TimestampNanosecondArray, *expected)
                }
                _ => panic!("unexpected literal timestamp carrier"),
            },
            LiteralValue::Utf8(expected) => match ty {
                DataType::Utf8 => value!(StringArray, expected.as_ref()),
                DataType::LargeUtf8 => value!(LargeStringArray, expected.as_ref()),
                DataType::Utf8View => value!(StringViewArray, expected.as_ref()),
                _ => panic!("unexpected literal text carrier"),
            },
            LiteralValue::Binary(expected) => match ty {
                DataType::Binary => value!(BinaryArray, expected.as_ref()),
                DataType::LargeBinary => value!(LargeBinaryArray, expected.as_ref()),
                DataType::BinaryView => value!(BinaryViewArray, expected.as_ref()),
                _ => panic!("unexpected literal binary carrier"),
            },
            _ => panic!("literal fixture has no independent payload oracle"),
        }
    }
}

fn check_literal_roots(cases: Vec<(FunctionValueType, LiteralValue)>) {
    let program = literal_program(&cases);
    let child_schema = program.graph().nodes()[0].output_layout().schema().clone();
    assert!(child_schema.fields().is_empty());
    let batch = RecordBatch::try_new_with_options(
        child_schema,
        vec![],
        &RecordBatchOptions::new().with_row_count(Some(6)),
    )
    .unwrap();
    let original_rows = [1, 4];
    let selection = Selection::try_sparse(6, &original_rows).unwrap();
    for (ordinal, (ty, literal)) in cases.iter().enumerate() {
        let ordinal = u32::try_from(ordinal).unwrap();
        assert_eq!(
            program
                .checked()
                .channels()
                .channel_type(ProgramChannelSite::Layout {
                    node: ProgramNodeId::new(1),
                    role: ProgramChannelLayoutRole::NodeOutput,
                    ordinal,
                })
                .unwrap(),
            ty
        );
        let root = ProgramExpressionRootSite::Node {
            node: ProgramNodeId::new(1),
            role: ProgramNodeExpressionRole::ProjectOutput {
                expression: ordinal,
            },
        };
        let mut evaluator =
            CompiledExpressionInstance::try_new(program.clone(), root, &RuntimeControl).unwrap();
        for _ in 0..2 {
            let result = evaluator
                .evaluate(&batch, selection, &RuntimeControl)
                .unwrap();
            assert_eq!(result.selection(), selection);
            assert!(result.errors().is_empty());
            assert_literal_payload(result.values(), ty.data_type(), literal, 2);
        }
        let result = evaluator
            .evaluate(
                &batch,
                Selection::try_sparse(6, &[]).unwrap(),
                &RuntimeControl,
            )
            .unwrap();
        assert!(result.selection().is_empty());
        assert!(result.errors().is_empty());
        assert_literal_payload(result.values(), ty.data_type(), literal, 0);
        assert!(evaluator.instances.is_empty());
    }
}

fn with_typed_nulls(
    cases: Vec<(FunctionValueType, LiteralValue)>,
) -> Vec<(FunctionValueType, LiteralValue)> {
    cases
        .into_iter()
        .flat_map(|(ty, literal)| {
            let mut null_type = ty.clone();
            null_type = null_type
                .with_nullable(true)
                .expect("valid test root nullability");
            [(ty, literal), (null_type, LiteralValue::Null)]
        })
        .collect()
}

#[test]
fn actual_compiled_literal_numeric_temporal_bits_and_typed_nulls_survive_sparse_batches() {
    let mut negative_2pow200 = [0; 32];
    negative_2pow200[..7].fill(0xff);
    let largeint = FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        false,
        ValueLogicalType::LargeInt,
    )
    .unwrap();
    let mut cases = vec![
        (
            FunctionValueType::new(DataType::UInt64, false),
            LiteralValue::UInt64(u64::MAX),
        ),
        (largeint, LiteralValue::LargeInt(i128::MIN)),
        (
            FunctionValueType::new(DataType::Decimal128(38, -2), false),
            LiteralValue::Decimal128(-123456789),
        ),
        (
            FunctionValueType::new(DataType::Decimal256(76, -3), false),
            LiteralValue::Decimal256(negative_2pow200),
        ),
        (
            FunctionValueType::new(DataType::Float64, false),
            LiteralValue::Float64Bits((-0.0f64).to_bits()),
        ),
        (
            FunctionValueType::new(DataType::Float64, false),
            LiteralValue::Float64Bits(0x7ff8_1234_5678_9abc),
        ),
        (
            FunctionValueType::new(DataType::Date32, false),
            LiteralValue::Date32(-7),
        ),
        (
            FunctionValueType::new(DataType::Time64(TimeUnit::Microsecond), false),
            LiteralValue::Time64(86399999999),
        ),
        (
            FunctionValueType::new(DataType::Time64(TimeUnit::Nanosecond), false),
            LiteralValue::Time64(123456789),
        ),
    ];
    for (months, days, nanoseconds) in [
        (i32::MIN, i32::MAX, i64::MIN),
        (i32::MAX, i32::MIN, i64::MAX),
        (-17, -31, -123456789),
        (0, 0, 0),
    ] {
        cases.push((
            FunctionValueType::new(DataType::Interval(IntervalUnit::MonthDayNano), false),
            LiteralValue::IntervalMonthDayNano {
                months,
                days,
                nanoseconds,
            },
        ));
    }
    for unit in [
        TimeUnit::Second,
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ] {
        for zone in [None, Some(Arc::<str>::from("Europe/Berlin"))] {
            cases.push((
                FunctionValueType::new(DataType::Timestamp(unit, zone), false),
                LiteralValue::Timestamp(-1234567),
            ));
        }
    }
    check_literal_roots(with_typed_nulls(cases));
}

#[test]
fn actual_compiled_literal_offset_view_and_nominal_bytes_keep_payload_and_typed_nulls() {
    let text = format!("prefix\0é🙂{}suffix", "x".repeat(2048));
    let binary = [vec![0, 0xff, 0x80], vec![0x5a; 4096], vec![0]].concat();
    let mut cases = Vec::new();
    for carrier in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
        cases.push((
            FunctionValueType::new(carrier, false),
            LiteralValue::Utf8(text.clone().into_boxed_str()),
        ));
    }
    for carrier in [
        DataType::Binary,
        DataType::LargeBinary,
        DataType::BinaryView,
    ] {
        cases.push((
            FunctionValueType::new(carrier, false),
            LiteralValue::Binary(binary.clone().into_boxed_slice()),
        ));
    }
    cases.push((
        FunctionValueType::try_with_logical_type(DataType::Utf8, false, ValueLogicalType::Json)
            .unwrap(),
        LiteralValue::Utf8("{\"value\":42}".into()),
    ));
    cases.push((
        FunctionValueType::try_with_logical_type(
            DataType::LargeBinary,
            false,
            ValueLogicalType::Variant,
        )
        .unwrap(),
        LiteralValue::Binary(vec![0, 0xff, 0x32, 0x80].into_boxed_slice()),
    ));
    check_literal_roots(with_typed_nulls(cases));
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
