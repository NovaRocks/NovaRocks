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
//! Calendar-to-TIME values follow calendar/time-of-day arithmetic, independently
//! of the retired arena evaluator.
use arrow::array::*;
use arrow::datatypes::{DataType, TimeUnit};
use novarocks_functions::{
    CastOperation, CastRowResult, EvaluatedArgument, KernelEvaluationControl, KernelFailure,
    PreparedCastRecipe, Selection,
};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, DecimalOverflowPolicy, FunctionValueType, PureCompileControl,
};
use std::sync::Arc;
use std::time::Duration;
pub(super) fn modes() -> impl Iterator<Item = (bool, DecimalOverflowPolicy)> {
    [false, true].into_iter().flat_map(|allow| {
        [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ]
        .into_iter()
        .map(move |policy| (allow, policy))
    })
}
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
        panic!("calendar TIME cast never waits")
    }
}
pub(super) fn corpus(dtype: &DataType) -> ArrayRef {
    if dtype == &DataType::Date32 {
        return Arc::new(Date32Array::from(vec![
            Some(i32::MIN),
            Some(i32::MAX),
            Some(-1),
            Some(0),
            Some(1),
            Some(7),
            Some(-7),
            None,
        ]));
    }
    let DataType::Timestamp(unit, zone) = dtype else {
        panic!("exact calendar profile")
    };
    let multiplier = match unit {
        TimeUnit::Second => 1,
        TimeUnit::Millisecond => 1000,
        TimeUnit::Microsecond => 1000000,
        TimeUnit::Nanosecond => 1000000000,
    };
    let v = vec![
        Some(i64::MIN),
        Some(i64::MAX),
        Some(-1),
        Some(0),
        Some(1),
        Some(86401 * multiplier),
        Some(-86401 * multiplier),
        None,
    ];
    match unit {
        TimeUnit::Second => Arc::new(TimestampSecondArray::from(v).with_timezone_opt(zone.clone())),
        TimeUnit::Millisecond => {
            Arc::new(TimestampMillisecondArray::from(v).with_timezone_opt(zone.clone()))
        }
        TimeUnit::Microsecond => {
            Arc::new(TimestampMicrosecondArray::from(v).with_timezone_opt(zone.clone()))
        }
        TimeUnit::Nanosecond => {
            Arc::new(TimestampNanosecondArray::from(v).with_timezone_opt(zone.clone()))
        }
    }
}
fn expected(dtype: &DataType) -> Vec<Option<i64>> {
    match dtype {
        DataType::Date32 => vec![
            Some(0),
            Some(0),
            Some(0),
            Some(0),
            Some(0),
            Some(0),
            Some(0),
            None,
        ],
        DataType::Timestamp(TimeUnit::Second, _) => vec![
            None,
            None,
            Some(86399000000),
            Some(0),
            Some(1000000),
            Some(1000000),
            Some(86399000000),
            None,
        ],
        DataType::Timestamp(TimeUnit::Millisecond | TimeUnit::Microsecond, _) => vec![
            None,
            None,
            Some(86399000000),
            Some(0),
            Some(0),
            Some(1000000),
            Some(86399000000),
            None,
        ],
        DataType::Timestamp(TimeUnit::Nanosecond, _) => vec![
            Some(763000000),
            Some(85636000000),
            Some(0),
            Some(0),
            Some(0),
            Some(1000000),
            Some(86399000000),
            None,
        ],
        _ => unreachable!(),
    }
}
fn raw(dtype: DataType) {
    for (allow, policy) in modes() {
        for operation in [CastOperation::Time, CastOperation::TimeFromDatetime] {
            let source = FunctionValueType::new(dtype.clone(), true);
            let target = FunctionValueType::new(DataType::Time64(TimeUnit::Microsecond), true);
            let recipe = PreparedCastRecipe::try_new(
                operation, &source, &target, policy, allow, None, &Control,
            )
            .unwrap();
            let input = corpus(&dtype);
            let expected = expected(&dtype);
            for (array, values) in [
                (input.clone(), expected.clone()),
                (input.slice(1, 6), expected[1..7].to_vec()),
                (input.slice(0, 0), vec![]),
            ] {
                let actual = (0..array.len())
                    .map(|row| {
                        match recipe
                            .evaluate_row(EvaluatedArgument::Column(&array), row, row, &Control)
                            .unwrap()
                        {
                            CastRowResult::Null => None,
                            CastRowResult::Signed(value) => Some(value),
                            other => panic!("calendar TIME row kind: {other:?}"),
                        }
                    })
                    .collect::<Vec<_>>();
                assert_eq!(actual, values);
            }
        }
    }
}
fn profiles(unit: TimeUnit) -> impl Iterator<Item = DataType> {
    [
        None,
        Some("UTC".into()),
        Some("Asia/Shanghai".into()),
        Some("+08:00".into()),
        Some("opaque-original-zone".into()),
    ]
    .into_iter()
    .map(move |z| DataType::Timestamp(unit, z))
}
fn differential(dtype: DataType) {
    for (allow, policy) in modes() {
        for nullable in [false, true] {
            for operation in [
                CastOperation::Carrier,
                CastOperation::Time,
                CastOperation::TimeFromDatetime,
            ] {
                let source = FunctionValueType::new(dtype.clone(), nullable);
                let target = FunctionValueType::new(DataType::Time64(TimeUnit::Microsecond), true);
                let recipe = PreparedCastRecipe::try_new(
                    operation, &source, &target, policy, allow, None, &Control,
                )
                .unwrap();
                let expected = expected(&dtype);
                for (a, expect) in [
                    (corpus(&dtype), expected.clone()),
                    (corpus(&dtype).slice(1, 6), expected[1..7].to_vec()),
                    (corpus(&dtype).slice(0, 0), vec![]),
                ] {
                    let rows = (0..a.len())
                        .filter(|r| nullable || !a.is_null(*r))
                        .collect::<Vec<_>>();
                    for rows in [
                        rows.clone(),
                        rows.into_iter().filter(|r| r % 3 == 0).collect(),
                    ] {
                        for (ordinal, row) in Selection::try_sparse(a.len(), &rows)
                            .unwrap()
                            .iter()
                            .enumerate()
                        {
                            let got = match recipe
                                .evaluate_row(EvaluatedArgument::Column(&a), ordinal, row, &Control)
                                .unwrap()
                            {
                                CastRowResult::Null => None,
                                CastRowResult::Signed(v) => Some(v),
                                x => panic!("different exact result: {x:?}"),
                            };
                            assert_eq!(got, expect[row]);
                        }
                    }
                }
                assert_eq!(recipe.source_type(), &source);
                assert_eq!(recipe.result_type(), &target);
            }
        }
    }
}
#[test]
fn calendar_time_original_date() {
    raw(DataType::Date32);
}
#[test]
fn calendar_time_original_second() {
    for t in profiles(TimeUnit::Second) {
        raw(t);
    }
}
#[test]
fn calendar_time_original_millisecond() {
    for t in profiles(TimeUnit::Millisecond) {
        raw(t);
    }
}
#[test]
fn calendar_time_original_microsecond() {
    for t in profiles(TimeUnit::Microsecond) {
        raw(t);
    }
}
#[test]
fn calendar_time_original_nanosecond() {
    for t in profiles(TimeUnit::Nanosecond) {
        raw(t);
    }
}
#[test]
fn calendar_time_recipe_differential_date() {
    differential(DataType::Date32);
}
#[test]
fn calendar_time_recipe_differential_second() {
    for t in profiles(TimeUnit::Second) {
        differential(t);
    }
}
#[test]
fn calendar_time_recipe_differential_millisecond() {
    for t in profiles(TimeUnit::Millisecond) {
        differential(t);
    }
}
#[test]
fn calendar_time_recipe_differential_microsecond() {
    for t in profiles(TimeUnit::Microsecond) {
        differential(t);
    }
}
#[test]
fn calendar_time_recipe_differential_nanosecond() {
    for t in profiles(TimeUnit::Nanosecond) {
        differential(t);
    }
}
