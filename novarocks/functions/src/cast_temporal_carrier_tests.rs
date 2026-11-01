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

use super::*;
use crate::Selection;
use arrow_array::{ArrayRef, Date32Array};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
#[derive(Default)]
struct Control {
    calls: Mutex<Vec<u32>>,
    refusal: Option<(usize, KernelFailure)>,
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, work: u32) -> Result<(), KernelFailure> {
        assert!(work <= 256);
        let mut calls = self.calls.lock().unwrap();
        let at = calls.len();
        if let Some((stop, _)) = &self.refusal {
            assert!(at <= *stop, "callback after refusal");
        }
        calls.push(work);
        if let Some((stop, cause)) = &self.refusal
            && at == *stop
        {
            return Err(cause.clone());
        }
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("temporal casts must not wait")
    }
}
fn prepare(
    from: DataType,
    to: DataType,
    nullable: bool,
    policy: DecimalOverflowPolicy,
    allow: bool,
) -> PreparedCastRecipe {
    PreparedCastRecipe::try_new(
        CastOperation::Carrier,
        &FunctionValueType::from_legacy_declared_projection(from, nullable),
        &FunctionValueType::from_legacy_declared_projection(to, nullable),
        policy,
        allow,
        crate::binding_test_control(),
    )
    .unwrap()
}
#[test]
fn temporal_date_carrier_sparse_scalar_and_exact_nullability_profiles() {
    let dates: ArrayRef = Arc::new(Date32Array::from(vec![Some(0), Some(-1), None, Some(1)]));
    let rows = [1, 3];
    let sparse = Selection::try_sparse(4, &rows).unwrap();
    let scalar = dates.slice(1, 1);
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        for allow in [false, true] {
            for nullable in [false, true] {
                for (unit, scale) in [
                    (TimeUnit::Second, 86400),
                    (TimeUnit::Millisecond, 86400000),
                    (TimeUnit::Microsecond, 86400000000),
                    (TimeUnit::Nanosecond, 86400000000000),
                ] {
                    let recipe = prepare(
                        DataType::Date32,
                        DataType::Timestamp(unit, None),
                        nullable,
                        policy,
                        allow,
                    );
                    for (ordinal, row) in sparse.iter().enumerate() {
                        let value = recipe
                            .evaluate_row(
                                EvaluatedArgument::Column(&dates),
                                ordinal,
                                row,
                                &Control::default(),
                            )
                            .unwrap();
                        assert_eq!(
                            value,
                            CastRowResult::Timestamp(if row == 1 { -scale } else { scale })
                        );
                        let constant = recipe
                            .evaluate_row(
                                EvaluatedArgument::Scalar(&scalar),
                                ordinal,
                                row,
                                &Control::default(),
                            )
                            .unwrap();
                        assert_eq!(constant, CastRowResult::Timestamp(-scale));
                    }
                    if nullable {
                        assert_eq!(
                            recipe
                                .evaluate_row(
                                    EvaluatedArgument::Column(&dates),
                                    0,
                                    2,
                                    &Control::default()
                                )
                                .unwrap(),
                            CastRowResult::Null
                        );
                    }
                }
                let text = prepare(DataType::Date32, DataType::Utf8, nullable, policy, allow);
                assert_eq!(
                    text.evaluate_row(
                        EvaluatedArgument::Scalar(&scalar),
                        0,
                        3,
                        &Control::default()
                    )
                    .unwrap(),
                    CastRowResult::Text("1969-12-31".into())
                );
                // Adding an admitted source must not displace the existing Date32 identity.
                assert!(
                    prepare(DataType::Date32, DataType::Date32, nullable, policy, allow)
                        .is_identity()
                );
            }
        }
    }
}
#[test]
fn temporal_timestamp_date_negative_fraction_and_full_row_diagnostics() {
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(TimestampSecondArray::from(vec![-1])),
        Arc::new(TimestampMillisecondArray::from(vec![-1])),
        Arc::new(TimestampMicrosecondArray::from(vec![-1])),
        Arc::new(TimestampNanosecondArray::from(vec![-1])),
    ];
    for array in arrays {
        let recipe = prepare(
            array.data_type().clone(),
            DataType::Date32,
            false,
            DecimalOverflowPolicy::ReportError,
            true,
        );
        assert_eq!(
            recipe
                .evaluate_row(EvaluatedArgument::Scalar(&array), 0, 9, &Control::default())
                .unwrap(),
            CastRowResult::Signed(-1)
        );
    }
    let bad: ArrayRef = Arc::new(TimestampSecondArray::from(vec![i64::MAX]));
    let recipe = prepare(
        bad.data_type().clone(),
        DataType::Date32,
        false,
        DecimalOverflowPolicy::OutputNull,
        false,
    );
    let result = recipe
        .evaluate_row(EvaluatedArgument::Column(&bad), 7, 0, &Control::default())
        .unwrap();
    match result {
        CastRowResult::RowError(error) => {
            assert_eq!(error.selected_ordinal(), 7);
            assert_eq!(
                error.message(),
                "Cast error: Cannot convert arrow_array::types::TimestampSecondType 9223372036854775807 to datetime"
            );
        }
        other => panic!("expected original row error, got {other:?}"),
    }
}
#[test]
fn temporal_date_carrier_checks_active_range_and_preserves_hidden_null_payload() {
    let dates: ArrayRef = Arc::new(Date32Array::new(
        vec![i32::MAX, 0].into(),
        Some(arrow_buffer::NullBuffer::from(vec![false, true])),
    ));
    for unit in [TimeUnit::Microsecond, TimeUnit::Nanosecond] {
        let recipe = prepare(
            DataType::Date32,
            DataType::Timestamp(unit, None),
            true,
            DecimalOverflowPolicy::OutputNull,
            false,
        );
        assert_eq!(
            recipe
                .evaluate_row(EvaluatedArgument::Column(&dates), 0, 0, &Control::default())
                .unwrap(),
            CastRowResult::Null
        );
        assert_eq!(
            recipe
                .evaluate_row(EvaluatedArgument::Column(&dates), 1, 1, &Control::default())
                .unwrap(),
            CastRowResult::Timestamp(0)
        );
        let active: ArrayRef = Arc::new(Date32Array::from(vec![i32::MAX]));
        let value = recipe
            .evaluate_row(
                EvaluatedArgument::Column(&active),
                7,
                0,
                &Control::default(),
            )
            .unwrap();
        let CastRowResult::RowError(error) = value else {
            panic!("active overflow must be a numeric data cause");
        };
        assert_eq!(error.selected_ordinal(), 7);
        assert_eq!(error.reason(), crate::DataFailureReason::NumericOutOfRange);
    }
}

#[test]
fn temporal_date_carrier_every_control_refusal_retains_original_typed_cause() {
    let dates: ArrayRef = Arc::new(Date32Array::from(vec![0]));
    let recipe = prepare(
        DataType::Date32,
        DataType::Utf8,
        false,
        DecimalOverflowPolicy::OutputNull,
        false,
    );
    let baseline = Control::default();
    recipe
        .evaluate_row(EvaluatedArgument::Column(&dates), 0, 0, &baseline)
        .unwrap();
    let count = baseline.calls.lock().unwrap().len();
    assert!(count > 0);
    for at in 0..count {
        for cause in [
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::ResourceExhausted,
        ] {
            let control = Control {
                refusal: Some((at, cause.clone())),
                ..Default::default()
            };
            assert_eq!(
                recipe
                    .evaluate_row(EvaluatedArgument::Column(&dates), 0, 0, &control)
                    .unwrap_err(),
                cause
            );
        }
    }
}

#[test]
fn timestamp_date_extremes_keep_original_row_error_under_every_policy() {
    let source: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![Some(i64::MAX), None]));
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        for allow in [false, true] {
            for nullable in [false, true] {
                let recipe = prepare(
                    source.data_type().clone(),
                    DataType::Date32,
                    nullable,
                    policy,
                    allow,
                );
                let value = recipe
                    .evaluate_row(
                        EvaluatedArgument::Column(&source),
                        27,
                        0,
                        &Control::default(),
                    )
                    .unwrap();
                let CastRowResult::RowError(error) = value else {
                    panic!("original calendar row error");
                };
                assert_eq!(error.selected_ordinal(), 27);
                assert_eq!(error.reason(), crate::DataFailureReason::InvalidValue);
                assert_eq!(
                    error.message(),
                    "Cast error: Cannot convert arrow_array::types::TimestampMicrosecondType 9223372036854775807 to datetime"
                );
            }
        }
    }
}
