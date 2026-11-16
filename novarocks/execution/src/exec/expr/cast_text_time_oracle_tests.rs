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
//! Exact prepared TIME profiles with independent duration and Arrow parser values.
use arrow::array::{Array, ArrayRef, LargeStringArray, StringArray, StringViewArray};
use arrow::datatypes::{DataType, TimeUnit};
use novarocks_functions::{
    CastOperation, CastRowResult, EvaluatedArgument, KernelEvaluationControl, KernelFailure,
    PreparedCastRecipe, Selection,
};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, DecimalOverflowPolicy, FunctionValueType, PureCompileControl,
};
use std::{sync::Arc, time::Duration};
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
pub(super) fn input(dtype: &DataType, strings: &[Option<&str>]) -> ArrayRef {
    match dtype {
        DataType::Utf8 => Arc::new(StringArray::from(strings.to_vec())),
        DataType::LargeUtf8 => Arc::new(LargeStringArray::from(strings.to_vec())),
        DataType::Utf8View => Arc::new(StringViewArray::from(strings.to_vec())),
        _ => panic!("fixture requires one actual text carrier"),
    }
}
pub(super) fn corpus(dtype: &DataType) -> ArrayRef {
    input(
        dtype,
        &[
            Some("00:00:00"),
            Some("25:00:00"),
            Some("+01:02:03"),
            Some("-01:02:03"),
            Some("01:02:03.456789"),
            Some("1"),
            Some("+1"),
            Some("-1"),
            Some("12:10"),
            Some("23:59:60"),
            Some("1970-01-01 01:01:01"),
            Some(" 12 : 34 : 56 "),
            Some(""),
            Some("9223372036854775807:00:00"),
            Some("2562047788015:00:00"),
            None,
        ],
    )
}
// UTF8 duration parsing accepts whole unsigned h:m:s (hours may exceed 24).
// Large/view carriers use the locked Arrow microsecond parser; literal numbers
// are microseconds. These tables are independent of any execution evaluator.
fn expected(dtype: &DataType) -> Vec<Option<i64>> {
    if dtype == &DataType::Utf8 {
        vec![
            Some(0),
            Some(90_000_000_000),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(45_296_000_000),
            None,
            None,
            None,
            None,
        ]
    } else {
        vec![
            Some(0),
            None,
            None,
            None,
            Some(3_723_456_789),
            Some(1),
            Some(1),
            Some(-1),
            Some(43_800_000_000),
            Some(86_400_000_000),
            None,
            None,
            None,
            None,
            None,
            None,
        ]
    }
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
        panic!("TIME cast does not wait")
    }
}
fn row_value(
    recipe: &PreparedCastRecipe,
    argument: EvaluatedArgument<'_>,
    ordinal: usize,
    row: usize,
) -> Option<i64> {
    match recipe
        .evaluate_row(argument, ordinal, row, &Control)
        .unwrap()
    {
        CastRowResult::Null => None,
        CastRowResult::Signed(value) => Some(value),
        other => panic!("TIME recipe returned a different frozen row category: {other:?}"),
    }
}
fn compare(dtype: DataType) {
    for (allow, policy) in modes() {
        for nullable in [true, false] {
            let source = FunctionValueType::new(dtype.clone(), nullable);
            let result = FunctionValueType::new(DataType::Time64(TimeUnit::Microsecond), true);
            // Same actual SQL effect-author operation; no default operation replacement.
            let recipe = PreparedCastRecipe::try_new(
                CastOperation::Carrier,
                &source,
                &result,
                policy,
                allow,
                None,
                &Control,
            )
            .unwrap();
            let expected = expected(&dtype);
            for (array, expected) in [
                (corpus(&dtype), expected.clone()),
                (corpus(&dtype).slice(1, 7), expected[1..8].to_vec()),
                (input(&dtype, &[]), vec![]),
            ] {
                for (ordinal, row) in Selection::all(array.len()).iter().enumerate() {
                    if !nullable && array.is_null(row) {
                        continue;
                    }
                    assert_eq!(
                        row_value(&recipe, EvaluatedArgument::Column(&array), ordinal, row),
                        expected[row]
                    );
                }
                let rows: Vec<_> = (0..array.len())
                    .filter(|row| row % 3 == 0 && (nullable || !array.is_null(*row)))
                    .collect();
                let selection = Selection::try_sparse(array.len(), &rows).unwrap();
                for (ordinal, row) in selection.iter().enumerate() {
                    assert_eq!(
                        row_value(&recipe, EvaluatedArgument::Column(&array), ordinal, row),
                        expected[row]
                    );
                }
            }
            let scalar: ArrayRef = input(&dtype, &[Some("01:02:03")]);
            let expected = Some(3_723_000_000); // 1 hour + 2 minutes + 3 seconds.
            for row in 0..17 {
                assert_eq!(
                    row_value(&recipe, EvaluatedArgument::Scalar(&scalar), row, row),
                    expected
                );
            }
            assert_eq!(recipe.source_type(), &source);
            assert_eq!(recipe.result_type(), &result);
        }
    }
}
#[test]
fn text_time_recipe_differential_utf8_exact_profile() {
    compare(DataType::Utf8);
}
#[test]
fn text_time_recipe_differential_large_utf8_exact_profile() {
    compare(DataType::LargeUtf8);
}
#[test]
fn text_time_recipe_differential_utf8_view_exact_profile() {
    compare(DataType::Utf8View);
}
