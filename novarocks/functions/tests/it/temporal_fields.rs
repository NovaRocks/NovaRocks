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

use std::sync::Arc;

use arrow_array::{ArrayRef, FixedSizeBinaryArray, Int32Array, Int64Array};
use arrow_schema::DataType;
use novarocks_functions::{
    EvaluatedArgument, FunctionArgument, FunctionArgumentType, FunctionBindingRequest,
    FunctionKind, FunctionResultType, Selection, testkit::shared_installed_builtin_owner_catalogue,
};
use novarocks_type_contract::{
    FunctionValueType, IntervalField, IntervalQualifier, PICOS_PER_DAY, TemporalCarrier,
    TemporalMicros, TemporalPrecision, TemporalType, TemporalValue, ValueKind, ValueLayout,
    ValueTypeRef, ValueTypeStorageLifetime,
};

use super::scalar_rows::{Control, prepare_bound};

fn check_fields(
    qualifier: IntervalQualifier,
    input: &ArrayRef,
    expected_magnitudes: &[(&str, i64)],
) {
    let temporal = TemporalType::Interval(qualifier);
    let storage = ValueTypeStorageLifetime::pure();
    let input_type = FunctionValueType::try_from_value(
        ValueTypeRef::try_new(ValueKind::Temporal(temporal), true, storage.clone()).unwrap(),
        ValueLayout::try_new(&temporal.carrier(), storage).unwrap(),
    )
    .unwrap();
    let family = if qualifier.is_year_month() {
        "interval_year_month"
    } else {
        "interval_day_time"
    };
    let arguments = [FunctionArgument::Value {
        value_type: input_type.clone(),
        constant: None,
    }];
    let request = FunctionBindingRequest {
        arguments: &arguments,
        logical_argument_count: arguments.len(),
        expected_result_type: None,
    };
    let installed = shared_installed_builtin_owner_catalogue();
    let catalog = installed.metadata();
    let selection = Selection::try_sparse(input.len(), &[0, 2, 3]).unwrap();
    let values = [EvaluatedArgument::Column(input)];
    for &(name, magnitude) in expected_magnitudes {
        let bound = catalog
            .resolve_bound_user(name, FunctionKind::Scalar, request, &Control)
            .unwrap();
        assert_eq!(
            bound.selected.overload.as_str(),
            format!("builtin.scalar/{name}/{family}/v1")
        );
        assert_eq!(
            bound.selected.argument_types.as_ref(),
            &[FunctionArgumentType::Value(input_type.clone())],
        );
        assert_eq!(
            bound.selected.result_type,
            FunctionResultType::Scalar(FunctionValueType::new(DataType::Int64, true)),
        );
        let output = prepare_bound(catalog, bound, request)
            .evaluate(selection, &values, &Control)
            .unwrap();
        assert!(output.errors().is_empty(), "{name} {qualifier:?}");
        assert_eq!(
            output
                .values()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(magnitude), Some(-magnitude), None],
            "{name} {qualifier:?}",
        );
    }
}

#[test]
fn year_month_fields_keep_the_month_qualifier_and_sign() {
    use IntervalField::{Month, Year};
    let input: ArrayRef = Arc::new(Int32Array::from(vec![Some(25), Some(0), Some(-25), None]));
    for (start, month) in [(Month, 25), (Year, 1)] {
        let qualifier =
            IntervalQualifier::new(start, Month, 9, TemporalPrecision::WHOLE_SECONDS).unwrap();
        check_fields(qualifier, &input, &[("year", 2), ("month", month)]);
    }
}

#[test]
fn compact_day_time_fields_derive_totals_and_components_from_the_start() {
    use IntervalField::{Day, Hour, Minute, Second};
    let day_to_second =
        IntervalQualifier::new(Day, Second, 9, TemporalPrecision::MICROSECONDS).unwrap();
    // 25 hours, 2 minutes and 3.5 seconds in the compact microsecond carrier.
    let micros = 90_123_500_000;
    let input: ArrayRef = Arc::new(Int64Array::from(vec![
        Some(micros),
        Some(0),
        Some(-micros),
        None,
    ]));
    check_fields(
        day_to_second,
        &input,
        &[
            ("day", 1),
            ("day_of_month", 1),
            ("hour", 1),
            ("minute", 2),
            ("second", 3),
        ],
    );

    for (start, micros, expected) in [
        (Hour, 129_600_000_000, &[("hour", 36), ("day", 1)][..]),
        (Minute, 90_000_000_000, &[("minute", 1500), ("hour", 1)][..]),
    ] {
        let input: ArrayRef = Arc::new(Int64Array::from(vec![
            Some(micros),
            Some(0),
            Some(-micros),
            None,
        ]));
        let qualifier =
            IntervalQualifier::new(start, start, 9, TemporalPrecision::WHOLE_SECONDS).unwrap();
        check_fields(qualifier, &input, expected);
    }
}

#[test]
fn wide_second_truncates_toward_zero_at_the_day_boundary() {
    use IntervalField::{Day, Second};
    for (start, second) in [(Day, 59), (Second, 86_399)] {
        let qualifier =
            IntervalQualifier::new(start, Second, 9, TemporalPrecision::PICOSECONDS).unwrap();
        let temporal = TemporalType::Interval(qualifier);
        let before_boundary = i128::from(PICOS_PER_DAY) - 1;
        let carrier = |picos| {
            let value = TemporalValue::IntervalDayTime(TemporalMicros::from_picos(picos).unwrap());
            let TemporalCarrier::Wide(bytes) = value.to_carrier(temporal).unwrap() else {
                panic!("picosecond interval requires the wide carrier");
            };
            bytes
        };
        let input: ArrayRef = Arc::new(
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                [
                    Some(carrier(before_boundary)),
                    None,
                    Some(carrier(-before_boundary)),
                    None,
                ]
                .into_iter(),
                12,
            )
            .unwrap(),
        );
        check_fields(qualifier, &input, &[("second", second)]);
    }
}
