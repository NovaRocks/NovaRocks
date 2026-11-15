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

//! SHOW STATS uses Trino's connector-to-symbol normalization and presentation.
//! Expected values come from Trino 484's TableScanStatsRule, StatsNormalizer,
//! ShowStatsRewrite and the independently captured A/X/F/E statement groups.
use arrow::{
    array::{ArrayRef, Float64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    error::ArrowError,
    record_batch::RecordBatch,
};
use novarocks_spi::connector::{
    StatisticsSummaryColumn, StatisticsSummaryColumnType as Kind, TableStatisticsSummary,
};
use std::sync::Arc;

#[derive(Debug, PartialEq)]
struct ColumnOutput {
    data_size: Option<f64>,
    ndv: Option<f64>,
    nulls: Option<f64>,
    low: Option<String>,
    high: Option<String>,
}
impl ColumnOutput {
    fn zero() -> Self {
        Self {
            data_size: Some(0.0),
            ndv: Some(0.0),
            nulls: Some(1.0),
            low: None,
            high: None,
        }
    }
}

pub(crate) fn render_show_stats(
    summary: &TableStatisticsSummary,
) -> Result<RecordBatch, ArrowError> {
    let rows = summary
        .columns
        .iter()
        .map(|column| normalize(summary.row_count, column))
        .collect::<Result<Vec<_>, _>>()?;
    let names = StringArray::from(
        summary
            .columns
            .iter()
            .map(|column| Some(column.name.as_ref()))
            .chain(std::iter::once(None))
            .collect::<Vec<_>>(),
    );
    let doubles = |column: fn(&ColumnOutput) -> Option<f64>| -> ArrayRef {
        Arc::new(Float64Array::from(
            rows.iter()
                .map(column)
                .chain(std::iter::once(None))
                .collect::<Vec<_>>(),
        ))
    };
    let text = |column: fn(&ColumnOutput) -> Option<&str>| -> ArrayRef {
        Arc::new(StringArray::from(
            rows.iter()
                .map(column)
                .chain(std::iter::once(None))
                .collect::<Vec<_>>(),
        ))
    };
    let row_counts = Float64Array::from(
        std::iter::repeat_n(None, rows.len())
            .chain(std::iter::once(Some(summary.row_count as f64)))
            .collect::<Vec<_>>(),
    );
    let fields = [
        ("column_name", DataType::Utf8),
        ("data_size", DataType::Float64),
        ("distinct_values_count", DataType::Float64),
        ("nulls_fraction", DataType::Float64),
        ("row_count", DataType::Float64),
        ("low_value", DataType::Utf8),
        ("high_value", DataType::Utf8),
    ]
    .into_iter()
    .map(|(name, ty)| Field::new(name, ty, true))
    .collect::<Vec<_>>();
    RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        vec![
            Arc::new(names),
            doubles(|row| row.data_size),
            doubles(|row| row.ndv),
            doubles(|row| row.nulls),
            Arc::new(row_counts),
            text(|row| row.low.as_deref()),
            text(|row| row.high.as_deref()),
        ],
    )
}

fn finite(value: f64) -> Option<f64> {
    value.is_finite().then_some(value)
}
fn normalize(rows: u64, column: &StatisticsSummaryColumn) -> Result<ColumnOutput, ArrowError> {
    if rows == 0 {
        return Ok(ColumnOutput::zero());
    }
    let rows = rows as f64;
    let mut nulls = column
        .null_count
        .map(|count| count as f64 / rows)
        .unwrap_or(f64::NAN);
    let mut ndv = column.ndv.map(|count| count as f64).unwrap_or(f64::NAN);
    if nulls.is_nan() && !ndv.is_nan() {
        nulls = if ndv >= rows {
            0.0
        } else {
            0.1f64.min((rows - ndv) / rows)
        };
    }
    let non_nulls = rows * (1.0 - nulls);
    let size = match column.column_type {
        Kind::Varchar => column
            .column_size
            .map(|size| (size as f64 * 2.7) as i64 as f64),
        Kind::Varbinary => column
            .column_size
            .map(|size| (size as f64 * 1.4) as i64 as f64),
        Kind::FixedBinary { width } => Some(width as f64),
        _ => None,
    }
    .unwrap_or(f64::NAN);
    let average = if non_nulls == 0.0 {
        0.0
    } else {
        size / non_nulls
    };
    let range = column
        .lower_bound
        .zip(column.upper_bound)
        .filter(|(low, high)| low.is_finite() && high.is_finite());
    if !ndv.is_nan() {
        if let Some((low, high)) = range {
            let width = if low == high {
                Some(1.0)
            } else {
                match column.column_type {
                    Kind::Integer | Kind::Boolean | Kind::Date => Some((high - low + 1.0).floor()),
                    Kind::Decimal { scale } => {
                        Some(((high - low) * 10f64.powf(f64::from(scale)) + 1.0).floor())
                    }
                    _ => None,
                }
            };
            if let Some(width) = width
                && ndv > width
            {
                ndv = width;
            }
        }
        if ndv > rows {
            ndv = rows;
        }
        let mut non_nulls = rows * (1.0 - nulls);
        if ndv > non_nulls {
            let difference = ndv - non_nulls;
            ndv -= difference / 2.0;
            non_nulls += difference / 2.0;
            nulls = 1.0 - non_nulls / rows;
        }
    }
    if ndv == 0.0 {
        return Ok(ColumnOutput::zero());
    }
    let (low, high) = match range {
        Some((low, high)) => (
            render_bound(column.column_type, low)?,
            render_bound(column.column_type, high)?,
        ),
        None => (None, None),
    };
    Ok(ColumnOutput {
        data_size: finite(average * rows * (1.0 - nulls)),
        ndv: finite(ndv),
        nulls: finite(nulls),
        low,
        high,
    })
}

fn render_bound(kind: Kind, value: f64) -> Result<Option<String>, ArrowError> {
    let failure = |message: String| ArrowError::ComputeError(message);
    let value = match kind {
        Kind::Boolean if value == 0.0 => "false".into(),
        Kind::Boolean if value == 1.0 => "true".into(),
        Kind::Boolean | Kind::Double | Kind::Decimal { .. } => {
            novarocks_result_render::java_double_text(value)
                .map_err(|error| failure(error.to_string()))?
        }
        Kind::Real => novarocks_result_render::java_float_text(value as f32)
            .map_err(|error| failure(error.to_string()))?,
        Kind::Integer => java_round(value).to_string(),
        Kind::Date => calendar_date(java_round(value)),
        Kind::Timestamp {
            precision,
            with_time_zone,
        } => {
            let raw = value as i64;
            let (factor, precision) = if with_time_zone {
                (1_000i64, precision.min(3))
            } else {
                (1_000_000i64, precision.min(6))
            };
            let seconds = raw.div_euclid(factor);
            let nanos = (raw.rem_euclid(factor) * (1_000_000_000 / factor)) as u32;
            let within_day = seconds.rem_euclid(86_400);
            let mut text = format!(
                "{} {:02}:{:02}:{:02}",
                calendar_date(seconds.div_euclid(86_400)),
                within_day / 3600,
                within_day / 60 % 60,
                within_day % 60
            );
            if precision != 0 {
                text.push_str(&format!(
                    ".{:0width$}",
                    nanos / 10u32.pow(9 - u32::from(precision)),
                    width = usize::from(precision)
                ));
            }
            if with_time_zone {
                text.push_str(" UTC");
            }
            text
        }
        _ => return Ok(None),
    };
    Ok(Some(value))
}
// Decompose Gregorian 400-year cycles with Euclidean division. The original
// Date32 and signed-microsecond carriers extend beyond chrono's calendar range;
// i128 intermediates preserve them without adding a presentation limit.
fn calendar_date(days: i64) -> String {
    let shifted = i128::from(days) + 719_468;
    let era = shifted.div_euclid(146_097);
    let day_of_era = shifted.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let march_month = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * march_month + 2) / 5 + 1;
    let month = march_month + if march_month < 10 { 3 } else { -9 };
    let year = year_of_era + era * 400 + i128::from(month <= 2);
    let year = if year < 0 {
        format!("-{:04}", -year)
    } else if year > 9999 {
        format!("+{year}")
    } else {
        format!("{year:04}")
    };
    format!("{year}-{month:02}-{day:02}")
}

fn java_round(value: f64) -> i64 {
    let floor = value.floor();
    (if value - floor >= 0.5 {
        floor + 1.0
    } else {
        floor
    }) as i64
}

#[cfg(test)]
mod tests;
