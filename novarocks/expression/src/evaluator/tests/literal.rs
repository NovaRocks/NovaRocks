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

use crate::testkit::{
    HarnessControl, PassThroughCallHost, checked_constants, compile_fixture_unit, fixture_scope,
    guarded_fixtures::{fixture_constant_policy, unit_draft},
};
use crate::{
    CompiledExprSet, CompiledExpressionInstance, EvaluationEnvironment, EvaluationInput,
    ScopeLayouts,
};
use arrow::{
    array::{
        Array, ArrayRef, BinaryArray, BinaryViewArray, Date32Array, Decimal128Array,
        Decimal256Array, FixedSizeBinaryArray, Float64Array, IntervalMonthDayNanoArray,
        LargeBinaryArray, LargeStringArray, StringArray, StringViewArray, Time64MicrosecondArray,
        Time64NanosecondArray, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray, UInt64Array,
    },
    datatypes::{DataType, IntervalUnit, Schema, TimeUnit},
    record_batch::{RecordBatch, RecordBatchOptions},
};
use novarocks_constant_contract::{ConstantPools, ConstantValue};
use novarocks_expression_contract::{ExprPort, RootOrdinal, RootScope};
use novarocks_functions::{
    KernelEvaluationControl, KernelFailure, ScalarInvocationActivation, ScalarInvocationScope,
    Selection,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, EvaluationDemand, FunctionValueType, SemanticParameters,
    ValueLogicalType,
};
use std::{collections::BTreeMap, sync::Arc, time::Duration};

struct RuntimeControl;

impl KernelEvaluationControl for RuntimeControl {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("compiled RAND expression must not wait")
    }
}

fn literal_unit(cases: &[LiteralCase]) -> Arc<CompiledExprSet> {
    let mut builder = unit_draft();
    let mut constants = ConstantPools::empty();
    let port = ExprPort {
        values: Box::default(),
    };
    let scope = fixture_scope(&port, Arc::new(Schema::empty()));
    for case in cases {
        let expr = builder
            .add(
                None,
                case.value.value_type().clone(),
                checked_constants::kind(&mut constants, case.value.clone()),
            )
            .unwrap();
        builder
            .root(expr, EvaluationDemand::Value, RootScope::Port(0))
            .unwrap();
    }
    let mut work = CompileCheckpoints::try_new(&HarnessControl, CompilePhase::Validate).unwrap();
    let unit = builder
        .seal(vec![port], &mut work)
        .unwrap()
        .finish(BTreeMap::new(), &mut work)
        .unwrap();
    work.finish().unwrap();
    let parameters = SemanticParameters::try_new([]).unwrap();
    compile_fixture_unit(
        &unit,
        EvaluationEnvironment {
            constants: &constants,
            parameters: &parameters,
        },
        ScopeLayouts {
            ports: std::slice::from_ref(&scope),
            all_ports: Some(&scope),
        },
        fixture_constant_policy(),
        &novarocks_functions::testkit::rng_subset(),
    )
}

type LiteralCheck = Box<dyn Fn(&ArrayRef, usize)>;

struct LiteralCase {
    value: ConstantValue,
    check: LiteralCheck,
    is_null: bool,
}
fn literal_case(value: ConstantValue, check: impl Fn(&ArrayRef, usize) + 'static) -> LiteralCase {
    LiteralCase {
        value,
        check: Box::new(check),
        is_null: false,
    }
}
macro_rules! checked_value {
    ($ty:expr, $factory:ident($($payload:expr),* $(,)?) $(,)?) => {{
        let ty = $ty;
        ConstantValue::$factory(
            Arc::new(ty.try_to_field("source").unwrap()), ty,
            $($payload,)* fixture_constant_policy(), CompilePhase::Validate, &HarnessControl,
        ).unwrap()
    }};
}
macro_rules! scalar_case {
    ($ty:expr, $factory:ident($($payload:expr),* $(,)?), $array:ty, $expected:expr $(,)?) => {{
        let expected = $expected;
        literal_case(checked_value!($ty, $factory($($payload),*)), move |array, rows| {
            let values = array.as_any().downcast_ref::<$array>().unwrap();
            for row in 0..rows { assert_eq!(values.value(row), expected); }
        })
    }};
}
fn assert_literal_payload(array: &ArrayRef, case: &LiteralCase, rows: usize) {
    assert_eq!(array.len(), rows);
    assert_eq!(array.data_type(), case.value.value_type().data_type());
    for row in 0..rows {
        assert_eq!(array.is_null(row), case.is_null);
    }
    (case.check)(array, rows);
}
fn check_literal_roots(cases: Vec<LiteralCase>) {
    let unit = literal_unit(&cases);
    let child_schema = unit.scope(RootScope::Port(0)).unwrap().schema().clone();
    assert!(child_schema.fields().is_empty());
    let batch = RecordBatch::try_new_with_options(
        child_schema,
        vec![],
        &RecordBatchOptions::new().with_row_count(Some(6)),
    )
    .unwrap();
    let original_rows = [1, 4];
    let selection = Selection::try_sparse(6, &original_rows).unwrap();
    for (ordinal, case) in cases.iter().enumerate() {
        let root = RootOrdinal::new(u32::try_from(ordinal).unwrap());
        let definition = unit.root(root).unwrap().definition;
        assert_eq!(
            unit.definitions().node(definition).unwrap().value_type(),
            Some(case.value.value_type()),
        );
        let mut evaluator =
            CompiledExpressionInstance::try_new(unit.clone(), root, &RuntimeControl).unwrap();
        for _ in 0..2 {
            let result = evaluator
                .evaluate(
                    EvaluationInput::Batch {
                        batch: &batch,
                        source: None,
                    },
                    ScalarInvocationScope::new(selection, selection),
                    ScalarInvocationActivation::Activated,
                    &RuntimeControl,
                    &mut PassThroughCallHost,
                )
                .unwrap();
            assert_eq!(result.selection(), selection);
            assert!(result.errors().is_empty());
            assert_literal_payload(result.values(), case, 2);
        }
        let result = evaluator
            .evaluate(
                EvaluationInput::Batch {
                    batch: &batch,
                    source: None,
                },
                {
                    let selection = Selection::try_sparse(6, &[]).unwrap();
                    ScalarInvocationScope::new(selection, selection)
                },
                ScalarInvocationActivation::Activated,
                &RuntimeControl,
                &mut PassThroughCallHost,
            )
            .unwrap();
        assert!(result.selection().is_empty());
        assert!(result.errors().is_empty());
        assert_literal_payload(result.values(), case, 0);
        assert!(evaluator.instances.is_empty());
    }
}
fn with_typed_nulls(cases: Vec<LiteralCase>) -> Vec<LiteralCase> {
    cases
        .into_iter()
        .flat_map(|case| {
            let ty = case.value.value_type().clone().with_nullable(true).unwrap();
            let null = checked_value!(ty, null());
            let mut null_case = literal_case(null, |array, rows| {
                for row in 0..rows {
                    assert!(array.is_null(row));
                }
            });
            null_case.is_null = true;
            [case, null_case]
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
    let large = checked_value!(largeint, from_largeint(i128::MIN));
    let mut cases = vec![
        scalar_case!(
            FunctionValueType::new(DataType::UInt64, false),
            from_u64(u64::MAX),
            UInt64Array,
            u64::MAX
        ),
        literal_case(large, |array, rows| {
            let values = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap();
            for row in 0..rows {
                assert_eq!(values.value(row), i128::MIN.to_be_bytes().as_slice());
            }
        }),
        scalar_case!(
            FunctionValueType::new(DataType::Decimal128(38, -2), false),
            from_decimal128(-123456789),
            Decimal128Array,
            -123456789
        ),
        literal_case(
            checked_value!(
                FunctionValueType::new(DataType::Decimal256(76, -3), false),
                from_decimal256_be(negative_2pow200)
            ),
            move |array, rows| {
                let values = array.as_any().downcast_ref::<Decimal256Array>().unwrap();
                for row in 0..rows {
                    assert_eq!(values.value(row).to_be_bytes(), negative_2pow200);
                }
            },
        ),
    ];
    for bits in [(-0.0f64).to_bits(), 0x7ff8_1234_5678_9abc] {
        cases.push(literal_case(
            checked_value!(
                FunctionValueType::new(DataType::Float64, false),
                from_f64_bits(bits)
            ),
            move |array, rows| {
                let values = array.as_any().downcast_ref::<Float64Array>().unwrap();
                for row in 0..rows {
                    assert_eq!(values.value(row).to_bits(), bits);
                }
            },
        ));
    }
    cases.extend([
        scalar_case!(
            FunctionValueType::new(DataType::Date32, false),
            from_date32(-7),
            Date32Array,
            -7
        ),
        scalar_case!(
            FunctionValueType::new(DataType::Time64(TimeUnit::Microsecond), false),
            from_time64(86399999999),
            Time64MicrosecondArray,
            86399999999
        ),
        scalar_case!(
            FunctionValueType::new(DataType::Time64(TimeUnit::Nanosecond), false),
            from_time64(123456789),
            Time64NanosecondArray,
            123456789
        ),
    ]);
    for (months, days, nanoseconds) in [
        (i32::MIN, i32::MAX, i64::MIN),
        (i32::MAX, i32::MIN, i64::MAX),
        (-17, -31, -123456789),
        (0, 0, 0),
    ] {
        cases.push(literal_case(
            checked_value!(
                FunctionValueType::new(DataType::Interval(IntervalUnit::MonthDayNano), false),
                from_interval_month_day_nano((months, days, nanoseconds))
            ),
            move |array, rows| {
                let values = array
                    .as_any()
                    .downcast_ref::<IntervalMonthDayNanoArray>()
                    .unwrap();
                for row in 0..rows {
                    let actual = values.value(row);
                    assert_eq!(
                        (actual.months, actual.days, actual.nanoseconds),
                        (months, days, nanoseconds)
                    );
                }
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
            cases.push(literal_case(
                checked_value!(
                    FunctionValueType::new(DataType::Timestamp(unit, zone), false),
                    from_timestamp(-1234567)
                ),
                move |array, rows| {
                    for row in 0..rows {
                        let actual = match unit {
                            TimeUnit::Second => array
                                .as_any()
                                .downcast_ref::<TimestampSecondArray>()
                                .unwrap()
                                .value(row),
                            TimeUnit::Millisecond => array
                                .as_any()
                                .downcast_ref::<TimestampMillisecondArray>()
                                .unwrap()
                                .value(row),
                            TimeUnit::Microsecond => array
                                .as_any()
                                .downcast_ref::<TimestampMicrosecondArray>()
                                .unwrap()
                                .value(row),
                            TimeUnit::Nanosecond => array
                                .as_any()
                                .downcast_ref::<TimestampNanosecondArray>()
                                .unwrap()
                                .value(row),
                        };
                        assert_eq!(actual, -1234567);
                    }
                },
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
        let expected = text.clone();
        let ty = FunctionValueType::new(carrier.clone(), false);
        let value = checked_value!(ty, from_utf8(&expected));
        cases.push(literal_case(value, move |array, rows| {
            for row in 0..rows {
                let actual = match carrier {
                    DataType::Utf8 => array
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .value(row),
                    DataType::LargeUtf8 => array
                        .as_any()
                        .downcast_ref::<LargeStringArray>()
                        .unwrap()
                        .value(row),
                    DataType::Utf8View => array
                        .as_any()
                        .downcast_ref::<StringViewArray>()
                        .unwrap()
                        .value(row),
                    _ => unreachable!(),
                };
                assert_eq!(actual, expected.as_str());
            }
        }));
    }
    for carrier in [
        DataType::Binary,
        DataType::LargeBinary,
        DataType::BinaryView,
    ] {
        let expected = binary.clone();
        let ty = FunctionValueType::new(carrier.clone(), false);
        let value = checked_value!(ty, from_binary(&expected));
        cases.push(literal_case(value, move |array, rows| {
            for row in 0..rows {
                let actual = match carrier {
                    DataType::Binary => array
                        .as_any()
                        .downcast_ref::<BinaryArray>()
                        .unwrap()
                        .value(row),
                    DataType::LargeBinary => array
                        .as_any()
                        .downcast_ref::<LargeBinaryArray>()
                        .unwrap()
                        .value(row),
                    DataType::BinaryView => array
                        .as_any()
                        .downcast_ref::<BinaryViewArray>()
                        .unwrap()
                        .value(row),
                    _ => unreachable!(),
                };
                assert_eq!(actual, expected.as_slice());
            }
        }));
    }
    let json_type = FunctionValueType::try_with_logical_type(
        DataType::LargeBinary,
        false,
        ValueLogicalType::Json,
    )
    .unwrap();
    cases.push(literal_case(
        checked_value!(json_type, from_binary(b"\x01\x80\x01\x00")),
        |array, rows| {
            let values = array.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            for row in 0..rows {
                assert_eq!(values.value(row), b"\x01\x80\x01\x00");
            }
        },
    ));
    let variant_type = FunctionValueType::try_with_logical_type(
        DataType::LargeBinary,
        false,
        ValueLogicalType::Variant,
    )
    .unwrap();
    cases.push(literal_case(
        checked_value!(variant_type, from_binary(&[0, 0xff, 0x32, 0x80])),
        |array, rows| {
            let values = array.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            for row in 0..rows {
                assert_eq!(values.value(row), &[0, 0xff, 0x32, 0x80]);
            }
        },
    ));
    check_literal_roots(with_typed_nulls(cases));
}
