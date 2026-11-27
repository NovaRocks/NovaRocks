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

use std::borrow::Cow;

use novarocks_spi::connector::ConnectorError;

use super::corrupt;
use crate::iceberg::spec::{Datum, PrimitiveLiteral, PrimitiveType, Type};

/// Interpret a bound already decoded with its manifest writer's schema in the
/// current scan's type. Missing bounds are handled by the caller.
pub(super) fn promote<'a>(
    datum: &'a Datum,
    target: &PrimitiveType,
) -> Result<Cow<'a, Datum>, ConnectorError> {
    use PrimitiveType::{Date, Decimal, Double, Float, Int, Long, Timestamp, TimestampNs};

    let source = datum.data_type();
    if source == target {
        return Ok(Cow::Borrowed(datum));
    }

    let promoted = match (source, target, datum.literal()) {
        (Int, Long, _) | (Float, Double, _) => datum.clone().to(&Type::Primitive(target.clone())),
        (
            Decimal { precision, scale },
            Decimal {
                precision: target_precision,
                scale: target_scale,
            },
            _,
        ) if scale == target_scale && precision < target_precision => {
            datum.clone().to(&Type::Primitive(target.clone()))
        }
        (Date, Timestamp | TimestampNs, PrimitiveLiteral::Int(days)) => {
            let units_per_day = if target == &Timestamp {
                86_400_000_000
            } else {
                86_400_000_000_000
            };
            let value = i64::from(*days)
                .checked_mul(units_per_day)
                .ok_or_else(|| corrupt("Iceberg date bound exceeds the target timestamp range"))?;
            return Ok(Cow::Owned(if target == &Timestamp {
                Datum::timestamp_micros(value)
            } else {
                Datum::timestamp_nanos(value)
            }));
        }
        _ => {
            return Err(corrupt(format!(
                "Iceberg bound cannot be promoted from {source} to {target}"
            )));
        }
    };

    promoted
        .map(Cow::Owned)
        .map_err(|error| corrupt(format!("Iceberg bound promotion failed: {error}")))
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum BoundText {
    Human,
    Json,
}

/// Iceberg Java uses ISO strings for temporal values and UUIDs. The binary
/// representation is base64 for the identity transform and uppercase hex for
/// SingleValueParser.
pub(super) fn text_value(
    datum: &Datum,
    presentation: BoundText,
) -> Result<Cow<'_, str>, ConnectorError> {
    use PrimitiveLiteral::*;
    use PrimitiveType as P;

    let text = match (datum.data_type(), datum.literal()) {
        (P::String, String(value)) => return Ok(Cow::Borrowed(value)),
        (P::Uuid, UInt128(value)) => uuid::Uuid::from_u128(*value).hyphenated().to_string(),
        (P::Date, Int(days)) => day_string(i64::from(*days), presentation, false)?,
        (P::Time, Long(micros)) => {
            if !(0..86_400_000_000).contains(micros) {
                return Err(corrupt("Iceberg time bound is outside the day"));
            }
            time_string(
                micros / 1_000_000,
                (micros % 1_000_000) * 1000,
                presentation,
            )
        }
        (P::Timestamp | P::Timestamptz | P::TimestampNs | P::TimestamptzNs, Long(value)) => {
            let nanos = matches!(datum.data_type(), P::TimestampNs | P::TimestamptzNs);
            let zoned = matches!(datum.data_type(), P::Timestamptz | P::TimestamptzNs);
            let per_second = if nanos { 1_000_000_000 } else { 1_000_000 };
            let seconds = value.div_euclid(per_second);
            let nanos = value.rem_euclid(per_second) * (1_000_000_000 / per_second);
            let date = day_string(seconds.div_euclid(86_400), presentation, true)?;
            let time = time_string(seconds.rem_euclid(86_400), nanos, BoundText::Json);
            let suffix = if zoned { "+00:00" } else { "" };
            format!("{date}T{time}{suffix}")
        }
        (P::Fixed(_) | P::Binary, Binary(bytes)) => {
            if presentation == BoundText::Human {
                use base64::Engine as _;
                base64::engine::general_purpose::STANDARD.encode(bytes)
            } else {
                const HEX: &[u8; 16] = b"0123456789ABCDEF";
                let mut text = std::string::String::new();
                for byte in bytes {
                    text.push(char::from(HEX[usize::from(byte >> 4)]));
                    text.push(char::from(HEX[usize::from(byte & 0x0f)]));
                }
                text
            }
        }
        (P::Decimal { scale, .. }, Int128(coefficient)) => {
            decimal_string(*coefficient, *scale as usize, presentation)
        }
        (P::Boolean, Boolean(value)) => value.to_string(),
        (P::Int, Int(value)) => value.to_string(),
        (P::Long, Long(value)) => value.to_string(),
        _ => return Err(corrupt("Iceberg bound has no matching textual declaration")),
    };
    Ok(Cow::Owned(text))
}

fn day_string(
    days: i64,
    presentation: BoundText,
    in_timestamp: bool,
) -> Result<std::string::String, ConnectorError> {
    // Gregorian leap patterns repeat exactly every 400 years. Reduce the day
    // before invoking Arrow's calendar helper so that every Iceberg date fits.
    const CYCLE_DAYS: i64 = 146_097;
    let date = arrow::temporal_conversions::date32_to_datetime(days.rem_euclid(CYCLE_DAYS) as i32)
        .ok_or_else(|| corrupt("Iceberg date could not be represented"))?
        .date();
    let year = date
        .format("%Y")
        .to_string()
        .parse::<i64>()
        .map_err(|error| corrupt(format!("Iceberg date year is invalid: {error}")))?
        + 400 * days.div_euclid(CYCLE_DAYS);
    let month_day = date.format("%m-%d");
    if presentation == BoundText::Human && !in_timestamp {
        Ok(format!("{year:04}-{month_day}"))
    } else if year > 9999 {
        Ok(format!("+{year}-{month_day}"))
    } else if year < 0 {
        Ok(format!("-{:04}-{month_day}", -year))
    } else {
        Ok(format!("{year:04}-{month_day}"))
    }
}

fn time_string(seconds: i64, nanos: i64, presentation: BoundText) -> std::string::String {
    let hours = seconds / 3600;
    let minutes = seconds / 60 % 60;
    let seconds = seconds % 60;
    if presentation == BoundText::Human && seconds == 0 && nanos == 0 {
        return format!("{hours:02}:{minutes:02}");
    }
    if nanos == 0 {
        return format!("{hours:02}:{minutes:02}:{seconds:02}");
    }
    let fraction = format!("{nanos:09}");
    let fraction_len = fraction.trim_end_matches('0').len();
    let fraction_len = if presentation == BoundText::Human {
        fraction_len.div_ceil(3) * 3
    } else {
        fraction_len
    };
    format!(
        "{hours:02}:{minutes:02}:{seconds:02}.{}",
        &fraction[..fraction_len]
    )
}

fn decimal_string(coefficient: i128, scale: usize, presentation: BoundText) -> std::string::String {
    let digits = coefficient.unsigned_abs().to_string();
    let sign = if coefficient < 0 { "-" } else { "" };
    if scale == 0 {
        return format!("{sign}{digits}");
    }
    if presentation == BoundText::Human && scale > digits.len() + 5 {
        let exponent = scale - digits.len() + 1;
        return if digits.len() == 1 {
            format!("{sign}{digits}E-{exponent}")
        } else {
            format!("{sign}{}.{}E-{exponent}", &digits[..1], &digits[1..])
        };
    }
    if digits.len() > scale {
        let point = digits.len() - scale;
        format!("{sign}{}.{}", &digits[..point], &digits[point..])
    } else {
        format!("{sign}0.{digits:0>scale$}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_exact_bounds_and_promotes_using_the_writer_type() {
        let input = Datum::int(-7);
        assert!(matches!(
            promote(&input, &PrimitiveType::Int).unwrap(),
            Cow::Borrowed(_)
        ));
        assert_eq!(
            promote(&input, &PrimitiveType::Long).unwrap().as_ref(),
            &Datum::long(-7)
        );
        assert_eq!(
            promote(&Datum::date(-1), &PrimitiveType::Timestamp)
                .unwrap()
                .as_ref(),
            &Datum::timestamp_micros(-86_400_000_000)
        );
        assert_eq!(
            promote(&Datum::date(2), &PrimitiveType::TimestampNs)
                .unwrap()
                .as_ref(),
            &Datum::timestamp_nanos(172_800_000_000_000)
        );
        assert!(promote(&Datum::date(1), &PrimitiveType::Timestamptz).is_err());
        assert!(promote(&Datum::date(i32::MAX), &PrimitiveType::TimestampNs).is_err());
        assert!(promote(&Datum::long(1), &PrimitiveType::Int).is_err());
    }
    #[test]
    fn reports_java_text_and_json_bound_encodings() {
        for (value, human, json) in [
            (Datum::binary([0xfb, 0xff]), "+/8=", "FBFF"),
            (
                Datum::time_micros(1_200_000).unwrap(),
                "00:00:01.200",
                "00:00:01.2",
            ),
            (Datum::date(-1), "1969-12-31", "1969-12-31"),
            (
                Datum::timestamptz_micros(-1),
                "1969-12-31T23:59:59.999999+00:00",
                "1969-12-31T23:59:59.999999+00:00",
            ),
        ] {
            assert_eq!(text_value(&value, BoundText::Human).unwrap(), human);
            assert_eq!(text_value(&value, BoundText::Json).unwrap(), json);
        }
    }
}
