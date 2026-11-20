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

//! Complete Date32 float profiles with independent fixture-authored calendar literals.
use arrow::array::{Array, ArrayRef, Date32Array};
use arrow::datatypes::DataType;
use novarocks_functions::{
    CastOperation, CastRowResult, EvaluatedArgument, KernelEvaluationControl, KernelFailure,
    PreparedCastRecipe, Selection,
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
        panic!("date float does not wait")
    }
}
// Pure date fixture author. Expected YYYYMMDD literals are authored alongside
// the original calendar inputs below, never by converting a Date32 back.
pub(super) fn day(year: i32, month: u32, day: u32) -> i32 {
    use chrono::{Datelike, NaiveDate};
    NaiveDate::from_ymd_opt(year, month, day)
        .unwrap()
        .num_days_from_ce()
        - 719163
}
fn expected_float(target: &DataType, literal: i64) -> CastRowResult {
    match target {
        DataType::Float32 => CastRowResult::Float32(literal as f32),
        DataType::Float64 => CastRowResult::Float64(literal as f64),
        _ => panic!("exact floating target required"),
    }
}

fn recipe(
    source: DataType,
    target: DataType,
    source_nullable: bool,
    result_nullable: bool,
    policy: DecimalOverflowPolicy,
    allow: bool,
) -> PreparedCastRecipe {
    PreparedCastRecipe::try_new(
        CastOperation::Carrier,
        &FunctionValueType::new(source, source_nullable),
        &FunctionValueType::new(target, result_nullable),
        policy,
        allow,
        None,
        &Control,
    )
    .unwrap()
}
fn compare(
    input: &ArrayRef,
    expected: &[Option<i64>],
    target: DataType,
    source_nullable: bool,
    result_nullable: bool,
    policy: DecimalOverflowPolicy,
    allow: bool,
) {
    let r = recipe(
        DataType::Date32,
        target.clone(),
        source_nullable,
        result_nullable,
        policy,
        allow,
    );
    assert_eq!(input.len(), expected.len());
    for (row, literal) in expected.iter().enumerate() {
        let expected = literal.map_or(CastRowResult::Null, |literal| {
            expected_float(&target, literal)
        });
        assert_eq!(
            r.evaluate_row(EvaluatedArgument::Column(input), row, row, &Control)
                .unwrap(),
            expected
        );
    }
}

fn complete_profile(target: DataType) {
    // Sample across the whole valid chrono span, including both arithmetic overflow classes below.
    // This fixture is evidence, not a narrower admitted source type or runtime filter.
    let mut corpus = vec![
        (0, 19700101_i64),
        (-1, 19691231),
        (day(0, 1, 1), 101),
        (day(-1, 12, 31), -8769),
        (day(1900, 2, 28), 19000228),
        (day(2000, 2, 29), 20000229),
        (day(2024, 2, 29), 20240229),
        (day(214748, 12, 31), 2147481231),
        (day(-214748, 12, 31), -2147478769),
    ];
    for year in (-214748..=214748).step_by(4093) {
        for (month, date) in [(1, 1), (6, 30), (12, 31)] {
            corpus.push((
                day(year, month, date),
                i64::from(year) * 10000 + i64::from(month) * 100 + i64::from(date),
            ));
        }
    }
    for source_nullable in [false, true] {
        let mut rows = corpus
            .iter()
            .map(|(days, _)| Some(*days))
            .collect::<Vec<_>>();
        let mut expected = corpus
            .iter()
            .map(|(_, literal)| Some(*literal))
            .collect::<Vec<_>>();
        if source_nullable {
            rows.push(None);
            expected.push(None);
        }
        let input: ArrayRef = Arc::new(Date32Array::from(rows));
        for result_nullable in [false, true] {
            if source_nullable && !result_nullable {
                continue;
            }
            for policy in [
                DecimalOverflowPolicy::OutputNull,
                DecimalOverflowPolicy::ReportError,
            ] {
                for allow in [false, true] {
                    compare(
                        &input,
                        &expected,
                        target.clone(),
                        source_nullable,
                        result_nullable,
                        policy,
                        allow,
                    );
                    compare(
                        &input.slice(1, input.len() - 1),
                        &expected[1..],
                        target.clone(),
                        source_nullable,
                        result_nullable,
                        policy,
                        allow,
                    );
                    compare(
                        &input.slice(0, 0),
                        &[],
                        target.clone(),
                        source_nullable,
                        result_nullable,
                        policy,
                        allow,
                    );
                    let r = recipe(
                        DataType::Date32,
                        target.clone(),
                        source_nullable,
                        result_nullable,
                        policy,
                        allow,
                    );
                    let constant = input.slice(1, 1);
                    for (ordinal, row) in Selection::try_sparse(input.len(), &[0, 3, 7])
                        .unwrap()
                        .iter()
                        .enumerate()
                    {
                        let expected = match target {
                            DataType::Float32 => CastRowResult::Float32(19691231_i32 as f32),
                            _ => CastRowResult::Float64(19691231.0),
                        };
                        assert_eq!(
                            r.evaluate_row(
                                EvaluatedArgument::Scalar(&constant),
                                ordinal,
                                row,
                                &Control
                            )
                            .unwrap(),
                            expected
                        );
                    }
                }
            }
        }
    }
    let hidden: ArrayRef = Arc::new(Date32Array::new(
        vec![i32::MAX, 0].into(),
        Some(arrow_buffer::NullBuffer::from(vec![false, true])),
    ));
    compare(
        &hidden,
        &[None, Some(19700101)],
        target.clone(),
        true,
        true,
        DecimalOverflowPolicy::ReportError,
        true,
    );
    let input: ArrayRef = Arc::new(Date32Array::from(vec![
        i32::MAX,
        0,
        i32::MIN,
        day(2024, 2, 29),
    ]));
    let r = recipe(
        DataType::Date32,
        target.clone(),
        false,
        true,
        DecimalOverflowPolicy::OutputNull,
        false,
    );
    // Genuine demand skips the outside-calendar rows; selected error uses selected ordinal, not logical row.
    for (ordinal, row) in Selection::try_sparse(4, &[1, 3])
        .unwrap()
        .iter()
        .enumerate()
    {
        assert!(matches!(
            r.evaluate_row(EvaluatedArgument::Column(&input), ordinal, row, &Control)
                .unwrap(),
            CastRowResult::Float32(_) | CastRowResult::Float64(_)
        ));
    }
    let expected_bad_message =
        format!("CAST failed: from Date32 to {target:?}: invalid Date32 value -2147483648");
    match r
        .evaluate_row(EvaluatedArgument::Column(&input), 7, 2, &Control)
        .unwrap()
    {
        CastRowResult::RowError(e) => {
            assert_eq!(e.selected_ordinal(), 7);
            assert_eq!(e.message(), expected_bad_message);
        }
        other => panic!("expected preserved original Date32 error, got {other:?}"),
    }
    // Checked calendar arithmetic handles legal years wider than i32 YYYYMMDD.
    // Literal expectations are independent of both adapters' shared author.
    for (days, literal) in [
        (day(262142, 12, 31), 2_621_421_231_i64),
        (day(-262143, 1, 1), -2_621_429_899),
        (day(214749, 1, 1), 2_147_490_101),
        (day(-214749, 1, 1), -2_147_489_899),
    ] {
        let input: ArrayRef = Arc::new(Date32Array::from(vec![days]));
        let pure = r
            .evaluate_row(EvaluatedArgument::Column(&input), 0, 0, &Control)
            .unwrap();
        assert_eq!(pure, expected_float(&target, literal));
    }
    let outside: ArrayRef = Arc::new(Date32Array::from(vec![i32::MAX]));
    use novarocks_failure_contract::FailureProjection;
    let CastRowResult::RowError(error) = r
        .evaluate_row(EvaluatedArgument::Column(&outside), 3, 0, &Control)
        .unwrap()
    else {
        panic!("out-of-calendar Date32 must be a row Data error");
    };
    assert_eq!(error.selected_ordinal(), 3);
    assert_eq!(
        error.failure_fact(),
        novarocks_functions::DataFailureReason::NumericOutOfRange.failure_fact()
    );
    assert_eq!(
        error.message(),
        format!("CAST failed: from Date32 to {target:?}: invalid Date32 value 2147483647")
    );
}
#[test]
fn date_float_oracle_complete_date32_float32_profile() {
    complete_profile(DataType::Float32);
}
#[test]
fn date_float_oracle_complete_date32_float64_profile() {
    complete_profile(DataType::Float64);
}
