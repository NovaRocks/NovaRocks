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
//! Fixed value expectations retained before deleting the recipe differential.
use crate::{
    CastOperation, CastRowResult, EvaluatedArgument, FunctionValueType, KernelEvaluationControl,
    KernelFailure, NativeNegateRowResult, PreparedCastRecipe, PreparedNativeNegateRecipe,
};
use arrow_array::{Array, ArrayRef, Date32Array, Int32Array, TimestampMicrosecondArray};
use arrow_schema::{DataType, TimeUnit};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, DecimalOverflowPolicy, PureCompileControl,
};
use std::{sync::Arc, time::Duration};
struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("recipes do not wait")
    }
}
fn recipe(
    source: &ArrayRef,
    target: DataType,
    policy: DecimalOverflowPolicy,
    allow: bool,
) -> PreparedCastRecipe {
    PreparedCastRecipe::try_new(
        CastOperation::Carrier,
        &FunctionValueType::new(source.data_type().clone(), true),
        &FunctionValueType::new(target, true),
        policy,
        allow,
        None,
        &Control,
    )
    .unwrap()
}
#[test]
fn f3_calendar_time_recipe_uses_handwritten_midnight_and_negative_epoch_values() {
    // DATE has midnight time. Timestamp -1us is 23:59:59 on the preceding
    // day; the frozen TIME conversion discards subsecond units.
    let cases: Vec<(ArrayRef, Vec<CastRowResult>)> = vec![
        (
            Arc::new(Date32Array::from(vec![Some(-1), Some(0), None])),
            vec![
                CastRowResult::Signed(0),
                CastRowResult::Signed(0),
                CastRowResult::Null,
            ],
        ),
        (
            Arc::new(TimestampMicrosecondArray::from(vec![
                Some(-1),
                Some(1_234_567),
                None,
            ])),
            vec![
                CastRowResult::Signed(86_399_000_000),
                CastRowResult::Signed(1_000_000),
                CastRowResult::Null,
            ],
        ),
    ];
    for (source, expected) in cases {
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            for allow in [false, true] {
                let r = recipe(
                    &source,
                    DataType::Time64(TimeUnit::Microsecond),
                    policy,
                    allow,
                );
                for (row, expected) in expected.iter().enumerate() {
                    assert_eq!(
                        r.evaluate_row(EvaluatedArgument::Column(&source), row, row, &Control)
                            .unwrap(),
                        *expected
                    );
                }
            }
        }
    }
}
#[test]
fn f3_date_float_recipe_has_independent_calendar_decimal_and_ieee_expectations() {
    // Date32 zero is 1970-01-01, projected as decimal YYYYMMDD=19700101.
    // f32 spacing here is two: round-to-nearest-even gives 19700100.
    let source: ArrayRef = Arc::new(Date32Array::from(vec![Some(0), None]));
    for (target, expected) in [
        (DataType::Float64, CastRowResult::Float64(19700101.0)),
        (DataType::Float32, CastRowResult::Float32(19700100.0)),
    ] {
        let r = recipe(&source, target, DecimalOverflowPolicy::OutputNull, false);
        assert_eq!(
            r.evaluate_row(EvaluatedArgument::Column(&source), 0, 0, &Control)
                .unwrap(),
            expected
        );
        assert_eq!(
            r.evaluate_row(EvaluatedArgument::Column(&source), 1, 1, &Control)
                .unwrap(),
            CastRowResult::Null
        );
    }
}
#[test]
fn f3_native_negate_integer_value_is_the_handwritten_additive_inverse() {
    // Additive inverse of -2 is 2, and SQL NULL propagates unchanged.
    let source: ArrayRef = Arc::new(Int32Array::from(vec![Some(-2), None]));
    let ty = FunctionValueType::new(DataType::Int32, true);
    let r = PreparedNativeNegateRecipe::try_new(&ty, &ty, &Control).unwrap();
    for (row, expected) in [Some(2), None].into_iter().enumerate() {
        let NativeNegateRowResult::Value(value) = r
            .evaluate_row(EvaluatedArgument::Column(&source), row, row, &Control)
            .unwrap()
        else {
            panic!("ordinary integer negate must succeed")
        };
        assert_eq!(
            value
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![expected]
        );
    }
}
