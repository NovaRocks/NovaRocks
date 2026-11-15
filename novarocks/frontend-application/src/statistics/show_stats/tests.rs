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

//! Pure input vectors protect the Trino normalization and formatting contract.
//! The oracle is Trino 484's captured A03/A07/E03/F03/F06/F09/X04/X06/X10
//! results, not a candidate output. These local tests need no external fixture.
use super::*;
use arrow::array::Array;
use novarocks_type_contract::FunctionValueType;

fn column(
    kind: Kind,
    nulls: Option<u64>,
    ndv: Option<u64>,
    size: Option<u64>,
    range: Option<(f64, f64)>,
) -> StatisticsSummaryColumn {
    StatisticsSummaryColumn {
        name: Arc::from("k"),
        value_type: FunctionValueType::new(DataType::Int64, true),
        column_type: kind,
        null_count: nulls,
        ndv,
        column_size: size,
        lower_bound: range.map(|range| range.0),
        upper_bound: range.map(|range| range.1),
    }
}
#[test]
fn captured_varchar_size_and_fraction_preserve_java_operation_order() {
    let output = normalize(3, &column(Kind::Varchar, Some(1), Some(2), Some(45), None)).unwrap();
    assert_eq!(output.data_size, Some(121.00000000000001));
    assert_eq!(output.nulls, Some(0.3333333333333333));
    assert_eq!(output.ndv, Some(2.0));
    assert_eq!(
        normalize(4, &column(Kind::Varchar, Some(1), Some(3), Some(46), None))
            .unwrap()
            .nulls,
        Some(0.25)
    );
    assert_eq!(
        normalize(
            4,
            &column(Kind::Varbinary, Some(0), Some(4), Some(45), None)
        )
        .unwrap()
        .data_size,
        Some(62.0)
    );
}
#[test]
fn absent_partial_and_ancestor_ndv_keep_their_explicit_facts() {
    let mut value = column(Kind::Integer, Some(0), None, Some(40), Some((1.0, 4.0)));
    assert_eq!(normalize(4, &value).unwrap().ndv, None);
    value.ndv = Some(3);
    assert_eq!(normalize(4, &value).unwrap().ndv, Some(3.0));
    assert_eq!(normalize(5, &value).unwrap().ndv, Some(3.0));
    let absent = normalize(4, &column(Kind::Complex, None, None, None, None)).unwrap();
    assert_eq!(
        absent,
        ColumnOutput {
            data_size: None,
            ndv: None,
            nulls: None,
            low: None,
            high: None
        }
    );
}
#[test]
fn empty_and_collected_all_null_columns_have_zero_statistics() {
    for kind in [Kind::Integer, Kind::Varchar, Kind::Complex] {
        assert_eq!(
            normalize(0, &column(kind, None, None, None, None)).unwrap(),
            ColumnOutput::zero()
        );
    }
    assert_eq!(
        normalize(2, &column(Kind::Integer, Some(2), Some(0), Some(8), None)).unwrap(),
        ColumnOutput::zero()
    );
}
#[test]
fn missing_null_counts_use_the_distinct_count_inference() {
    assert_eq!(
        normalize(100, &column(Kind::Integer, None, Some(50), None, None))
            .unwrap()
            .nulls,
        Some(0.1)
    );
    for ndv in [100, 150] {
        assert_eq!(
            normalize(100, &column(Kind::Integer, None, Some(ndv), None, None))
                .unwrap()
                .nulls,
            Some(0.0)
        );
    }
}
#[test]
fn normalization_limits_discrete_ranges_and_balances_non_null_values() {
    assert_eq!(
        normalize(
            100,
            &column(Kind::Integer, Some(0), Some(99), None, Some((1.0, 3.0)))
        )
        .unwrap()
        .ndv,
        Some(3.0)
    );
    assert_eq!(
        normalize(
            100,
            &column(
                Kind::Decimal { scale: 2 },
                Some(0),
                Some(99),
                None,
                Some((1.25, 1.27))
            )
        )
        .unwrap()
        .ndv,
        Some(3.0)
    );
    assert_eq!(
        normalize(
            100,
            &column(Kind::Double, Some(0), Some(99), None, Some((1.5, 1.5)))
        )
        .unwrap()
        .ndv,
        Some(1.0)
    );
    let output = normalize(100, &column(Kind::Integer, Some(50), Some(80), None, None)).unwrap();
    assert_eq!(output.ndv, Some(65.0));
    assert_eq!(output.nulls, Some(0.35));
}
#[test]
fn captured_bound_spellings_are_exact() {
    for (kind, value, expected) in [
        (Kind::Integer, 1.0, "1"),
        (
            Kind::Integer,
            9007199254740993u64 as f64,
            "9007199254740992",
        ),
        (Kind::Double, 1.5, "1.5"),
        (Kind::Double, 1e-5, "1.0E-5"),
        (Kind::Double, 12345678.9, "1.23456789E7"),
        (Kind::Decimal { scale: 2 }, 1.25, "1.25"),
        (Kind::Decimal { scale: 1 }, 2.5, "2.5"),
        (Kind::Decimal { scale: 4 }, -0.0001, "-1.0E-4"),
        (Kind::Decimal { scale: 4 }, 12345678.9012, "1.23456789012E7"),
        (
            Kind::Decimal { scale: 2 },
            1234567890123456800.0,
            "1.2345678901234568E18",
        ),
        (Kind::Real, 1.5, "1.5"),
        (Kind::Real, f64::from(3.4e10f32), "3.4E10"),
        (Kind::Boolean, 0.0, "false"),
        (Kind::Boolean, 1.0, "true"),
        (Kind::Date, 20454.0, "2026-01-01"),
        (
            Kind::Timestamp {
                precision: 6,
                with_time_zone: false,
            },
            1767323045123456.0,
            "2026-01-02 03:04:05.123456",
        ),
        (
            Kind::Timestamp {
                precision: 6,
                with_time_zone: true,
            },
            1767326400123.0,
            "2026-01-02 04:00:00.123 UTC",
        ),
    ] {
        assert_eq!(
            render_bound(kind, value).unwrap().as_deref(),
            Some(expected),
            "{kind:?} {value}"
        );
    }
    assert_eq!(java_round(-1.5), -1);
    assert_eq!(java_round(-1.6), -2);
}
#[test]
fn non_finite_or_incomplete_ranges_and_unordered_types_render_no_bounds() {
    for range in [
        (f64::NAN, 1.0),
        (1.0, f64::INFINITY),
        (f64::NEG_INFINITY, 1.0),
    ] {
        let value = normalize(
            2,
            &column(Kind::Double, Some(0), Some(2), None, Some(range)),
        )
        .unwrap();
        assert_eq!((value.low, value.high), (None, None));
    }
    for kind in [
        Kind::Varchar,
        Kind::Varbinary,
        Kind::Uuid,
        Kind::Time,
        Kind::Complex,
    ] {
        assert_eq!(render_bound(kind, 1.0).unwrap(), None);
    }
}
#[test]
fn result_has_seven_typed_columns_and_a_last_summary_row() {
    let value = column(Kind::Integer, Some(0), Some(3), None, Some((1.0, 3.0)));
    let batch = render_show_stats(&TableStatisticsSummary {
        row_count: 3,
        columns: vec![value],
    })
    .unwrap();
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(
        batch
            .schema()
            .fields()
            .iter()
            .map(|field| (field.name().as_str(), field.data_type()))
            .collect::<Vec<_>>(),
        vec![
            ("column_name", &DataType::Utf8),
            ("data_size", &DataType::Float64),
            ("distinct_values_count", &DataType::Float64),
            ("nulls_fraction", &DataType::Float64),
            ("row_count", &DataType::Float64),
            ("low_value", &DataType::Utf8),
            ("high_value", &DataType::Utf8)
        ]
    );
    let counts = batch
        .column(4)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert!(counts.is_null(0));
    assert_eq!(counts.value(1), 3.0);
    for ordinal in [0, 1, 2, 3, 5, 6] {
        assert!(batch.column(ordinal).is_null(1));
    }
}

#[test]
fn calendar_formatting_preserves_the_actual_date_and_timestamp_carrier_ranges() {
    // Independent expectations use java.time.LocalDate.ofEpochDay and
    // LocalDateTime.ofEpochSecond, formatted with uuuu-MM-dd HH:mm:ss.SSSSSS.
    for (days, expected) in [
        (0, "1970-01-01"),
        (-719528, "0000-01-01"),
        (2932897, "+10000-01-01"),
        (i64::from(i32::MIN), "-5877641-06-23"),
        (i64::from(i32::MAX), "+5881580-07-11"),
    ] {
        assert_eq!(calendar_date(days), expected);
    }
    assert_eq!(
        render_bound(
            Kind::Timestamp {
                precision: 6,
                with_time_zone: false
            },
            i64::MIN as f64
        )
        .unwrap()
        .as_deref(),
        Some("-290308-12-21 19:59:05.224192")
    );
    assert_eq!(
        render_bound(
            Kind::Timestamp {
                precision: 6,
                with_time_zone: false
            },
            i64::MAX as f64
        )
        .unwrap()
        .as_deref(),
        Some("+294247-01-10 04:00:54.775807")
    );
}

#[test]
fn all_null_without_ndv_preserves_unknown_distinct_count() {
    // Trino 484 TableScanStatsRule sets average size to zero when non-null
    // rows are zero. StatsNormalizer preserves an unknown NDV; only a known
    // zero NDV takes SymbolStatsEstimate.zero().
    for kind in [Kind::Integer, Kind::Varchar] {
        assert_eq!(
            normalize(2, &column(kind, Some(2), None, Some(8), None)).unwrap(),
            ColumnOutput {
                data_size: Some(0.0),
                ndv: None,
                nulls: Some(1.0),
                low: None,
                high: None,
            }
        );
        assert_eq!(
            normalize(2, &column(kind, Some(2), Some(0), Some(8), None)).unwrap(),
            ColumnOutput::zero()
        );
    }
}
