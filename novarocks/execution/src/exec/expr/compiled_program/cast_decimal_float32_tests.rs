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

//! Actual LocalCompiler -> checked LocalProgram -> Frame with independent IEEE bits.
//! Fixed corpus tables replace the removed evaluator/reference calculator.
use super::*;
use arrow::array::{Decimal128Array, Decimal256Array, Float32Array};
use novarocks_expression::EvaluationInput;
use novarocks_expression::testkit::PassThroughCallHost;
use novarocks_functions::{ScalarInvocationActivation, ScalarInvocationScope};

// Pure type/data construction only; this helper never evaluates a CAST.
fn input(wide: bool, p: u8, s: i8, raw: Vec<Option<i128>>) -> ArrayRef {
    if wide {
        Arc::new(
            Decimal256Array::from(
                raw.into_iter()
                    .map(|x| x.map(arrow_buffer::i256::from_i128))
                    .collect::<Vec<_>>(),
            )
            .with_precision_and_scale(p, s)
            .unwrap(),
        )
    } else {
        Arc::new(
            Decimal128Array::from(raw)
                .with_precision_and_scale(p, s)
                .unwrap(),
        )
    }
}

// Mathematical worksheet: raw1, raw0, raw-1, rawMAX, at the profile's scale.
// Raw r denotes r*10^-scale; precision is a frozen declaration and does not
// silently trim full physical carrier payloads supplied by this corpus.
// The independent integer-rational IEEE worksheet is retained with the draft.
// Float32 entries include the final binary32 narrowing. Decimal128's
// non-finite sanitizer produces NULL; Decimal256 preserves infinities and
// signed underflow zero. In particular, MAX at negative scale -3 becomes
// NULL only for Decimal128. It is Float32 range overflow, not i128 scaling.
fn expected_bits(wide: bool, scale: i8) -> [Option<u32>; 4] {
    match (wide, scale) {
        (false, -38) => [Some(0x7e967699), Some(0), Some(0xfe967699), None],
        (false, -3) => [Some(0x447a0000), Some(0), Some(0xc47a0000), None],
        (false, 0) | (true, 0) => [
            Some(0x3f800000),
            Some(0),
            Some(0xbf800000),
            Some(0x7f000000),
        ],
        (false, 1) | (true, 1) => [
            Some(0x3dcccccd),
            Some(0),
            Some(0xbdcccccd),
            Some(0x7d4ccccd),
        ],
        (false, 38) => [
            Some(0x006ce3ee),
            Some(0),
            Some(0x806ce3ee),
            Some(0x3fd9c7dd),
        ],
        (true, -76) => [
            Some(0x7f800000),
            Some(0),
            Some(0xff800000),
            Some(0x7f800000),
        ],
        (true, -3) => [
            Some(0x447a0000),
            Some(0),
            Some(0xc47a0000),
            Some(0x7f800000),
        ],
        (true, 76) => [Some(0), Some(0), Some(0x80000000), Some(0x00b94471)],
        _ => panic!("the fixed corpus must have an explicit handwritten IEEE table"),
    }
}
fn assert_bits(array: &ArrayRef, expected: &[Option<u32>]) {
    assert_eq!(array.data_type(), &DataType::Float32);
    let array = array.as_any().downcast_ref::<Float32Array>().unwrap();
    let actual = (0..array.len())
        .map(|row| (!array.is_null(row)).then(|| array.value(row).to_bits()))
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
}
fn batch(program: &LocalProgram, a: ArrayRef) -> RecordBatch {
    let len = a.len();
    RecordBatch::try_new(
        program.graph().nodes()[1].output_layout().schema().clone(),
        vec![
            a,
            Arc::new(Int64Array::from(vec![42; len])),
            Arc::new(BooleanArray::from(vec![true; len])),
        ],
    )
    .unwrap()
}
fn full(wide: bool) {
    for p in [1, if wide { 76 } else { 38 }] {
        // Retain the original physical precision/scale domain and repeated
        // precision-one scale1 profile; these are not inferred from displays.
        let max_scale = if wide { 76 } else { 38 };
        for scale in [-max_scale, -3, 0, 1, p as i8] {
            for nullable in [false, true] {
                for policy in [
                    DecimalOverflowPolicy::OutputNull,
                    DecimalOverflowPolicy::ReportError,
                ] {
                    for allow in [false, true] {
                        let a = input(
                            wide,
                            p,
                            scale,
                            if nullable {
                                vec![Some(0), Some(1), None, Some(-1), Some(i128::MAX)]
                            } else {
                                vec![Some(0), Some(1), Some(0), Some(-1), Some(i128::MAX)]
                            },
                        )
                        .slice(1, 4);
                        let mut expected = expected_bits(wide, scale);
                        if nullable {
                            expected[1] = None;
                        }
                        let program = compiled(
                            FunctionValueType::new(a.data_type().clone(), nullable),
                            FunctionValueType::new(
                                DataType::Float32,
                                nullable || (!wide && scale < 0),
                            ),
                            Source::Column,
                            Wrap::Bare,
                            policy,
                            allow,
                        );
                        let b = batch(&program, a.clone());
                        let rows = [0, 1, 3];
                        let selected = Selection::try_sparse(4, &rows).unwrap();
                        let out = instance(&program)
                            .evaluate(
                                EvaluationInput::Batch {
                                    batch: &b,
                                    source: None,
                                },
                                ScalarInvocationScope::new(selected, selected),
                                ScalarInvocationActivation::Activated,
                                &Control,
                                &mut PassThroughCallHost,
                            )
                            .unwrap();
                        assert_eq!(out.selection(), selected);
                        assert_bits(out.values(), &[expected[0], expected[1], expected[3]]);
                        assert!(out.errors().is_empty());
                        // Full selection also covers raw-1, including wide
                        // Float32 -infinity and negative-underflow-zero cases.
                        let all = instance(&program)
                            .evaluate(
                                EvaluationInput::Batch {
                                    batch: &b,
                                    source: None,
                                },
                                ScalarInvocationScope::new(Selection::all(4), Selection::all(4)),
                                ScalarInvocationActivation::Activated,
                                &Control,
                                &mut PassThroughCallHost,
                            )
                            .unwrap();
                        assert_eq!(all.selection(), Selection::all(4));
                        assert_bits(all.values(), &expected);
                        assert!(all.errors().is_empty());
                        let empty_selected = Selection::try_sparse(4, &[]).unwrap();
                        let empty = instance(&program)
                            .evaluate(
                                EvaluationInput::Batch {
                                    batch: &b,
                                    source: None,
                                },
                                ScalarInvocationScope::new(empty_selected, empty_selected),
                                ScalarInvocationActivation::Activated,
                                &Control,
                                &mut PassThroughCallHost,
                            )
                            .unwrap();
                        assert_eq!(empty.selection(), empty_selected);
                        assert_eq!(empty.values().data_type(), &DataType::Float32);
                        assert_eq!(empty.values().len(), 0);
                        assert!(empty.errors().is_empty());
                        let constant = compiled(
                            FunctionValueType::new(a.data_type().clone(), false),
                            FunctionValueType::new(DataType::Float32, !wide && scale < 0),
                            Source::Constant,
                            Wrap::Bare,
                            policy,
                            allow,
                        );
                        let cb = batch(&constant, input(wide, p, scale, vec![Some(0); 4]));
                        let out = instance(&constant)
                            .evaluate(
                                EvaluationInput::Batch {
                                    batch: &cb,
                                    source: None,
                                },
                                ScalarInvocationScope::new(selected, selected),
                                ScalarInvocationActivation::Activated,
                                &Control,
                                &mut PassThroughCallHost,
                            )
                            .unwrap();
                        assert_eq!(out.selection(), selected);
                        // The genuine compiled fixture authors raw1, so its
                        // fixed expected bit is the first entry of this table.
                        assert_bits(out.values(), &[expected_bits(wide, scale)[0]; 3]);
                        assert!(out.errors().is_empty());
                    }
                }
            }
        }
    }
}

#[test]
fn decimal_float32_actual_compiler_decimal128_float32() {
    full(false);
}
#[test]
fn decimal_float32_actual_compiler_decimal256_float32() {
    full(true);
}

#[test]
fn decimal_float32_actual_physical_source_rejects_out_of_range_scale_before_compile() {
    for wide in [false, true] {
        let max = if wide { 76 } else { 38 };
        let source = if wide {
            DataType::Decimal256(1, -127)
        } else {
            DataType::Decimal128(1, -127)
        };
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            fixture(
                FunctionValueType::new(source, true),
                FunctionValueType::new(DataType::Float32, true),
                Source::Column,
                Wrap::Bare,
                DecimalOverflowPolicy::ReportError,
                true,
            )
        }));
        let Err(failure) = result else {
            panic!("original Physical package must reject its illegal source scale");
        };
        let message = failure
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| failure.downcast_ref::<&str>().copied())
            .expect("original unwrap string payload");
        assert!(
            message.contains(&format!(
                "Arrow decimal precision/scale (1, -127) is outside 1..={max} and -{max}..={max}"
            )),
            "{message}"
        );
    }
}

#[test]
fn decimal_float32_actual_constant_author_rejects_precision_one_raw_71() {
    for wide in [false, true] {
        for scale in [-1, 0, 1] {
            let source = if wide {
                DataType::Decimal256(1, scale)
            } else {
                DataType::Decimal128(1, scale)
            };
            let ty = FunctionValueType::new(source, false);
            let field = Arc::new(ty.try_to_field("actual-decimal-constant").unwrap());
            // Precision one admits raw -9..=9. These fixed diagnostics use
            // raw71's decimal spelling and the maximum raw9 at each scale;
            // the expected error is not obtained from an Arrow validator.
            let (value, maximum) = match scale {
                -1 => ("710", "90"),
                0 => ("71", "9"),
                1 => ("7.1", "0.9"),
                _ => unreachable!(),
            };
            let kind = if wide { "Decimal256" } else { "Decimal128" };
            let expected = format!(
                "Invalid argument error: {value} is too large to store in a {kind} of precision 1. Max is {maximum}"
            );
            let result = if wide {
                ConstantValue::from_decimal256(
                    field,
                    ty,
                    arrow_buffer::i256::from_i128(71),
                    options().constants,
                    CompilePhase::FunctionSpecialization,
                    &Control,
                )
            } else {
                ConstantValue::from_decimal128(
                    field,
                    ty,
                    71,
                    options().constants,
                    CompilePhase::FunctionSpecialization,
                    &Control,
                )
            };
            let Err(error) = result else {
                panic!("original real constant owner cannot admit raw 71 at precision one");
            };
            assert_eq!(error.to_string(), expected);
        }
    }
}
