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
//! Float64-to-Decimal128 profiles with independent fixed coefficients and dispositions.
use arrow::array::{Array, ArrayRef, Float64Array};
use arrow::datatypes::DataType;
use novarocks_functions::{
    CastOperation, CastRowResult, ConstantPool, EvaluatedArgument, KernelEvaluationControl,
    KernelFailure, PreparedCastRecipe, SelectedValues, Selection,
};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, DecimalOverflowPolicy, FunctionValueType, PureCompileControl,
};
use std::{sync::Arc, time::Duration};
struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, n: u32) -> Result<(), CompileControlError> {
        assert!(n <= 256);
        Ok(())
    }
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, n: u32) -> Result<(), KernelFailure> {
        assert!(n <= 256);
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("Float Decimal CAST never waits")
    }
}
pub(super) fn input(values: Vec<Option<f64>>) -> ArrayRef {
    Arc::new(Float64Array::from(values))
}
pub(super) fn modes() -> [(DecimalOverflowPolicy, bool); 4] {
    [
        (DecimalOverflowPolicy::OutputNull, false),
        (DecimalOverflowPolicy::OutputNull, true),
        (DecimalOverflowPolicy::ReportError, false),
        (DecimalOverflowPolicy::ReportError, true),
    ]
}
const OVERFLOW: &str =
    "Expr evaluate meet error: The numeric type cast involving decimal overflows";
// Fixed IEEE binary64 representation of integer 10^scale, then its exact
// integer coefficient. A +1/-1 input uses this coefficient (not ideal 10^s);
// adding a half for rounding does not cross an integer at these scales.
const UNIT_COEFFICIENT: [i128; 39] = [
    1,                                      // scale 0
    10,                                     // scale 1
    100,                                    // scale 2
    1000,                                   // scale 3
    10000,                                  // scale 4
    100000,                                 // scale 5
    1000000,                                // scale 6
    10000000,                               // scale 7
    100000000,                              // scale 8
    1000000000,                             // scale 9
    10000000000,                            // scale 10
    100000000000,                           // scale 11
    1000000000000,                          // scale 12
    10000000000000,                         // scale 13
    100000000000000,                        // scale 14
    1000000000000000,                       // scale 15
    10000000000000000,                      // scale 16
    100000000000000000,                     // scale 17
    1000000000000000000,                    // scale 18
    10000000000000000000,                   // scale 19
    100000000000000000000,                  // scale 20
    1000000000000000000000,                 // scale 21
    10000000000000000000000,                // scale 22
    99999999999999991611392,                // scale 23
    999999999999999983222784,               // scale 24
    10000000000000000905969664,             // scale 25
    100000000000000004764729344,            // scale 26
    1000000000000000013287555072,           // scale 27
    9999999999999999583119736832,           // scale 28
    99999999999999991433150857216,          // scale 29
    1000000000000000019884624838656,        // scale 30
    9999999999999999635896294965248,        // scale 31
    100000000000000005366162204393472,      // scale 32
    999999999999999945575230987042816,      // scale 33
    9999999999999999455752309870428160,     // scale 34
    99999999999999996863366107917975552,    // scale 35
    1000000000000000042420637374017961984,  // scale 36
    9999999999999999538762658202121142272,  // scale 37
    99999999999999997748809823456034029568, // scale 38
];
#[derive(Clone, Copy, Debug)]
enum Expected {
    Value(i128),
    Null,
    Overflow,
    ScaleError,
}
fn expected(a: &ArrayRef, row: usize, p: u8, scale: i8) -> Expected {
    if scale < -38 {
        return Expected::ScaleError;
    }
    if a.is_null(row) {
        return Expected::Null;
    }
    let value = a
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(row);
    if !value.is_finite() {
        return Expected::Null;
    }
    let coefficient = match value.to_bits() {
        0x0000000000000000 | 0x8000000000000000 => 0,
        0x3ff0000000000000 => {
            if scale < 0 {
                0
            } else {
                UNIT_COEFFICIENT[scale as usize]
            }
        }
        0xbff0000000000000 => {
            if scale < 0 {
                0
            } else {
                -UNIT_COEFFICIENT[scale as usize]
            }
        }
        0x3ff4000000000000 | 0xbff4000000000000 => {
            let magnitude = match (p, scale) {
                (1, 0) => 1,
                (18, 4) => 12500,
                (38, -38) => 0,
                (38, 38) => return Expected::Overflow,
                _ => panic!("fixed 1.25 profile needs a coefficient literal"),
            };
            if value.is_sign_negative() {
                -magnitude
            } else {
                magnitude
            }
        }
        // Exact -2^127 exceeds precision38. It must follow the same
        // finite-overflow policy without a signed-abs panic.
        0xc7e0000000000000 => {
            assert_eq!((p, scale), (38, 0));
            return Expected::Overflow;
        }
        _ => panic!("fixed float corpus needs an explicit coefficient"),
    };
    // Independent decimal declaration law; no conversion algorithm here.
    if coefficient.unsigned_abs() < 10_u128.pow(u32::from(p)) {
        Expected::Value(coefficient)
    } else {
        Expected::Overflow
    }
}
fn recipe(
    p: u8,
    s: i8,
    nullable: bool,
    policy: DecimalOverflowPolicy,
    allow: bool,
) -> PreparedCastRecipe {
    PreparedCastRecipe::try_new(
        CastOperation::Carrier,
        &FunctionValueType::new(DataType::Float64, nullable),
        &FunctionValueType::new(DataType::Decimal128(p, s), true),
        policy,
        allow,
        None,
        &Control,
    )
    .unwrap()
}
fn compare(
    r: &PreparedCastRecipe,
    a: &ArrayRef,
    ordinal: usize,
    row: usize,
    p: u8,
    s: i8,
    policy: DecimalOverflowPolicy,
    allow: bool,
) {
    let _ = allow;
    let pure = r
        .evaluate_row(EvaluatedArgument::Column(a), ordinal, row, &Control)
        .unwrap();
    assert_expected(pure, expected(a, row, p, s), ordinal, p, s, policy);
}
fn assert_expected(
    pure: CastRowResult,
    expected: Expected,
    ordinal: usize,
    p: u8,
    s: i8,
    policy: DecimalOverflowPolicy,
) {
    match expected {
        Expected::Value(value) => assert_eq!(pure, CastRowResult::Decimal128(value)),
        Expected::Null => assert_eq!(pure, CastRowResult::Null),
        Expected::Overflow if policy == DecimalOverflowPolicy::OutputNull => {
            assert_eq!(pure, CastRowResult::Null)
        }
        Expected::Overflow | Expected::ScaleError => {
            let CastRowResult::RowError(error) = pure else {
                panic!("fixed failure disposition requires row error");
            };
            assert_eq!(error.selected_ordinal(), ordinal);
            let message = if matches!(expected, Expected::ScaleError) {
                format!(
                    "CAST failed: from Float64 to Decimal128({p}, {s}): decimal scale overflow while casting float to DECIMAL: scale={s}"
                )
            } else {
                OVERFLOW.to_string()
            };
            assert_eq!(error.message(), message);
        }
    }
}

#[test]
fn float64_decimal128_permanent_full_precision_signed_scale_profile() {
    // Every legal Arrow/FVT p/s is admitted. Static policy-independent math is
    // checked over the complete axis; all four original policies are separately
    // checked on boundary/representative profiles below rather than repeated here.
    for p in 1..=38 {
        for s in i8::MIN..=p as i8 {
            for nullable in [false, true] {
                let r = recipe(p, s, nullable, DecimalOverflowPolicy::OutputNull, false);
                if s == i8::MIN && ![1, 38].contains(&p) {
                    continue;
                }
                let a = input(if nullable {
                    vec![Some(1.0), Some(-1.0), None, Some(f64::NAN)]
                } else {
                    vec![Some(1.0), Some(-1.0), Some(0.0), Some(f64::INFINITY)]
                });
                for row in 0..a.len() {
                    compare(
                        &r,
                        &a,
                        row,
                        row,
                        p,
                        s,
                        DecimalOverflowPolicy::OutputNull,
                        false,
                    );
                }
            }
        }
    }
}
#[test]
fn float64_decimal128_permanent_policy_selected_slice_scalar_constant_and_original_min() {
    for (policy, allow) in modes() {
        for (p, s) in [(1, 0), (18, 4), (38, -38), (38, 38), (38, -39), (1, -128)] {
            let a = input(vec![
                Some(99.0),
                Some(1.25),
                None,
                Some(-1.25),
                Some(f64::INFINITY),
            ])
            .slice(1, 4);
            let r = recipe(p, s, true, policy, allow);
            for (ordinal, row) in Selection::try_sparse(4, &[0, 1, 3])
                .unwrap()
                .iter()
                .enumerate()
            {
                compare(&r, &a, ordinal, row, p, s, policy, allow);
            }
            if s != i8::MIN {
                let selection = Selection::try_sparse(4, &[0, 1, 3]).unwrap();
                let compact = SelectedValues::try_new(
                    selection,
                    &DataType::Float64,
                    input(vec![Some(1.25), None, Some(f64::INFINITY)]),
                    Box::default(),
                )
                .unwrap();
                for (ordinal, row) in selection.iter().enumerate() {
                    let got = r
                        .evaluate_row(
                            EvaluatedArgument::SelectedColumn(&compact),
                            ordinal,
                            row,
                            &Control,
                        )
                        .unwrap();
                    assert_expected(got, expected(&a, row, p, s), ordinal, p, s, policy);
                }
                let scalar = a.slice(0, 1);
                let got = r
                    .evaluate_row(EvaluatedArgument::Scalar(&scalar), 88, 99, &Control)
                    .unwrap();
                assert_expected(got, expected(&a, 0, p, s), 88, p, s, policy);
            }
        }
        let a = input(vec![Some(i128::MIN as f64)]);
        compare(
            &recipe(38, 0, false, policy, allow),
            &a,
            0,
            0,
            38,
            0,
            policy,
            allow,
        );
        let ty = FunctionValueType::new(DataType::Float64, false);
        let a = input(vec![Some(i128::MIN as f64), Some(1.25)]);
        let pool = ConstantPool::try_new(
            Arc::new(ty.try_to_field("exact-pool").unwrap()),
            ty.clone(),
            a.to_data(),
            super::test_support::constant_policy(),
            CompilePhase::FunctionSpecialization,
            &Control,
        )
        .unwrap();
        let r = recipe(18, 1, false, policy, allow);
        assert_eq!(
            r.evaluate_row(
                EvaluatedArgument::Constant(&pool.value(1).unwrap()),
                5,
                100,
                &Control
            )
            .unwrap(),
            CastRowResult::Decimal128(13)
        );
    }
}
