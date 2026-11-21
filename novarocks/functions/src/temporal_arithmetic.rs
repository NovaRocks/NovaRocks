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

//! Exact interval and datetime arithmetic after selected binding.

use novarocks_type_contract::{
    IntervalField, IntervalQualifier, OperatorFunction, PICOS_PER_DAY, PICOS_PER_MICROSECOND,
    PICOS_PER_MINUTE, TemporalMicros, TemporalPrecision, TemporalType, TemporalValue,
};

use crate::{
    datetime_value::date32_to_gregorian,
    temporal_value::gregorian_epoch_day,
    temporal_zone_rules::{TemporalZoneSource, joda_local_to_utc, utc_to_local},
};

#[derive(Clone, Copy, Debug)]
pub(crate) enum Value {
    Date(i32),
    Temporal(TemporalValue),
    Bigint(i64),
    Double(f64),
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum ArithmeticError {
    Range,
    DateSubday(bool),
    MultiplyNaN,
    InvalidDivisor(f64),
    Invariant,
}

impl std::fmt::Display for ArithmeticError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Range => f.write_str("Temporal arithmetic result is out of range"),
            Self::DateSubday(false) => f.write_str("Cannot add hour, minutes or seconds to a date"),
            Self::DateSubday(true) => {
                f.write_str("Cannot subtract hour, minutes or seconds from a date")
            }
            Self::MultiplyNaN => f.write_str("Cannot multiply by double NaN"),
            Self::InvalidDivisor(value) => write!(f, "Cannot divide by double {value:?}"),
            Self::Invariant => f.write_str("Temporal arithmetic received a foreign operand"),
        }
    }
}

impl std::error::Error for ArithmeticError {}

impl novarocks_failure_contract::FailureProjection for ArithmeticError {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact};
        use novarocks_function_contract::kernel_failure::{
            INVALID_FUNCTION_ARGUMENT, NUMERIC_OUT_OF_RANGE,
        };
        match self {
            Self::Range => FailureFact::new(FailureClass::Data, NUMERIC_OUT_OF_RANGE),
            Self::DateSubday(_) | Self::MultiplyNaN | Self::InvalidDivisor(_) => {
                FailureFact::new(FailureClass::Data, INVALID_FUNCTION_ARGUMENT)
            }
            Self::Invariant => novarocks_failure_contract::FailureProjection::failure_fact(
                &crate::kernel_control::invalid("Temporal arithmetic received a foreign operand"),
            ),
        }
    }
}

pub(crate) fn evaluate(
    operator: OperatorFunction,
    result: Option<TemporalType>,
    left: Value,
    right: Option<Value>,
) -> Result<Value, ArithmeticError> {
    use OperatorFunction::*;
    use TemporalValue::*;
    use Value as V;
    let result_value = match (operator, left, right) {
        (TemporalNegation, V::Temporal(IntervalYearMonth(months)), None) => V::Temporal(
            IntervalYearMonth(months.checked_neg().ok_or(ArithmeticError::Range)?),
        ),
        (TemporalNegation, V::Temporal(IntervalDayTime(delta)), None) => {
            V::Temporal(IntervalDayTime(micros(-delta.as_picos())?))
        }
        (op @ (TemporalAdd | TemporalSubtract), left, Some(right)) => {
            add_or_subtract(left, right, op == TemporalSubtract)?
        }
        (TemporalMultiply, V::Temporal(interval), Some(V::Bigint(factor)))
        | (TemporalMultiply, V::Bigint(factor), Some(V::Temporal(interval))) => {
            let value = match interval {
                IntervalYearMonth(months) => month_value(i128::from(months) * i128::from(factor))?,
                IntervalDayTime(delta) => IntervalDayTime(micros(
                    delta
                        .as_picos()
                        .checked_mul(i128::from(factor))
                        .ok_or(ArithmeticError::Range)?,
                )?),
                _ => return Err(ArithmeticError::Invariant),
            };
            V::Temporal(value)
        }
        (
            op @ (TemporalMultiply | TemporalDivide),
            V::Temporal(interval),
            Some(V::Double(factor)),
        )
        | (op @ TemporalMultiply, V::Double(factor), Some(V::Temporal(interval))) => {
            let Some(TemporalType::Interval(qualifier)) = result else {
                return Err(ArithmeticError::Invariant);
            };
            V::Temporal(scale_double(
                interval,
                qualifier,
                factor,
                op == TemporalDivide,
            )?)
        }
        _ => return Err(ArithmeticError::Invariant),
    };
    if let (V::Temporal(value), Some(ty)) = (result_value, result) {
        value.validate(ty).map_err(|_| ArithmeticError::Range)?;
    }
    Ok(result_value)
}

fn add_or_subtract(left: Value, right: Value, subtract: bool) -> Result<Value, ArithmeticError> {
    use TemporalValue::*;
    use Value as V;
    let apply = |left: i128, right: i128| if subtract { left - right } else { left + right };
    Ok(match (left, right) {
        (V::Temporal(IntervalYearMonth(lhs)), V::Temporal(IntervalYearMonth(rhs))) => {
            V::Temporal(month_value(apply(lhs.into(), rhs.into()))?)
        }
        (V::Temporal(IntervalDayTime(lhs)), V::Temporal(IntervalDayTime(rhs))) => V::Temporal(
            IntervalDayTime(micros(apply(lhs.as_picos(), rhs.as_picos()))?),
        ),
        (V::Date(day), V::Temporal(IntervalDayTime(delta))) => {
            let picos = delta.as_picos();
            if picos % i128::from(PICOS_PER_DAY) != 0 {
                return Err(ArithmeticError::DateSubday(subtract));
            }
            V::Date(
                i32::try_from(apply(day.into(), picos / i128::from(PICOS_PER_DAY)))
                    .map_err(|_| ArithmeticError::Range)?,
            )
        }
        (V::Date(day), V::Temporal(IntervalYearMonth(months))) => {
            V::Date(shift_months(day, apply(0, months.into()))?)
        }
        (V::Temporal(Time(picos)), V::Temporal(IntervalDayTime(delta))) => V::Temporal(Time(
            apply(picos.into(), delta.as_picos()).rem_euclid(PICOS_PER_DAY.into()) as i64,
        )),
        (
            V::Temporal(TimeWithTimeZone {
                picos,
                offset_minutes,
            }),
            V::Temporal(IntervalDayTime(delta)),
        ) => V::Temporal(TimeWithTimeZone {
            picos: apply(picos.into(), delta.as_picos()).rem_euclid(PICOS_PER_DAY.into()) as i64,
            offset_minutes,
        }),
        (V::Temporal(Timestamp(value)), V::Temporal(IntervalDayTime(delta))) => V::Temporal(
            Timestamp(micros(apply(value.as_picos(), delta.as_picos()))?),
        ),
        (
            V::Temporal(TimestampWithTimeZone { instant, zone }),
            V::Temporal(IntervalDayTime(delta)),
        ) => V::Temporal(TimestampWithTimeZone {
            instant: micros(apply(instant.as_picos(), delta.as_picos()))?,
            zone,
        }),
        (V::Temporal(Timestamp(value)), V::Temporal(IntervalYearMonth(months))) => V::Temporal(
            Timestamp(timestamp_add_months(value, apply(0, months.into()))?),
        ),
        (
            V::Temporal(TimestampWithTimeZone { instant, zone }),
            V::Temporal(IntervalYearMonth(months)),
        ) => {
            let local = utc_to_local(zone, TemporalZoneSource::Joda, instant)
                .map_err(|_| ArithmeticError::Range)?;
            let local = timestamp_add_months(local, apply(0, months.into()))?;
            V::Temporal(TimestampWithTimeZone {
                instant: joda_local_to_utc(zone, local, Some(instant))
                    .map_err(|_| ArithmeticError::Range)?,
                zone,
            })
        }
        (lhs @ V::Temporal(IntervalYearMonth(_) | IntervalDayTime(_)), rhs) if !subtract => {
            return add_or_subtract(rhs, lhs, false);
        }
        (V::Temporal(lhs), V::Temporal(rhs)) if subtract => V::Temporal(IntervalDayTime(micros(
            clock_picos(lhs)? - clock_picos(rhs)?,
        )?)),
        _ => return Err(ArithmeticError::Invariant),
    })
}

fn clock_picos(value: TemporalValue) -> Result<i128, ArithmeticError> {
    use TemporalValue::*;
    Ok(match value {
        Time(picos) => picos.into(),
        TimeWithTimeZone {
            picos,
            offset_minutes,
        } => i128::from(
            (picos - i64::from(offset_minutes) * PICOS_PER_MINUTE).rem_euclid(PICOS_PER_DAY),
        ),
        Timestamp(value) | TimestampWithTimeZone { instant: value, .. } => value.as_picos(),
        _ => return Err(ArithmeticError::Invariant),
    })
}

fn micros(picos: i128) -> Result<TemporalMicros, ArithmeticError> {
    TemporalMicros::from_picos(picos).ok_or(ArithmeticError::Range)
}

fn month_value(months: i128) -> Result<TemporalValue, ArithmeticError> {
    Ok(TemporalValue::IntervalYearMonth(
        i32::try_from(months).map_err(|_| ArithmeticError::Range)?,
    ))
}

fn shift_months(days: i32, delta: i128) -> Result<i32, ArithmeticError> {
    let (year, month, day) = date32_to_gregorian(days);
    let months = i128::from(year) * 12 + i128::from(month) - 1 + delta;
    let year = i64::try_from(months.div_euclid(12)).map_err(|_| ArithmeticError::Range)?;
    let month = months.rem_euclid(12) as i64 + 1;
    let (next_year, next_month) = if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    };
    let epoch =
        |year, month| gregorian_epoch_day(year, month, 1).map_err(|_| ArithmeticError::Range);
    let start = epoch(year, month)?;
    let last_day = epoch(next_year, next_month)? - start;
    i32::try_from(start + i64::from(day).min(last_day) - 1).map_err(|_| ArithmeticError::Range)
}

fn timestamp_add_months(
    value: TemporalMicros,
    delta: i128,
) -> Result<TemporalMicros, ArithmeticError> {
    let picos = value.as_picos();
    let day = i32::try_from(picos.div_euclid(PICOS_PER_DAY.into()))
        .map_err(|_| ArithmeticError::Range)?;
    let shifted = shift_months(day, delta)?;
    micros(i128::from(shifted) * i128::from(PICOS_PER_DAY) + picos.rem_euclid(PICOS_PER_DAY.into()))
}

fn scale_double(
    interval: TemporalValue,
    qualifier: IntervalQualifier,
    factor: f64,
    divide: bool,
) -> Result<TemporalValue, ArithmeticError> {
    use TemporalValue::*;
    if divide && (factor == 0.0 || factor.is_nan()) {
        return Err(ArithmeticError::InvalidDivisor(factor));
    }
    if !divide && factor.is_nan() {
        return Err(ArithmeticError::MultiplyNaN);
    }
    if factor == 1.0 {
        return Ok(interval);
    }
    let scale = |value: f64| {
        if divide {
            value / factor
        } else {
            value * factor
        }
    };
    match interval {
        IntervalYearMonth(months) => {
            let months = scale(f64::from(months)) as i64;
            let months = if qualifier.end() == IntervalField::Year {
                months / 12 * 12
            } else {
                months
            };
            month_value(months.into())
        }
        IntervalDayTime(value) => {
            let fraction = value.picoseconds_of_microsecond();
            let fraction_base = PICOS_PER_MICROSECOND as f64;
            let double_micros = if value.micros() < 0 && fraction > 0 {
                (value.micros() + 1) as f64 - (fraction_base - f64::from(fraction)) / fraction_base
            } else {
                value.micros() as f64 + f64::from(fraction) / fraction_base
            };
            let scaled = scale(double_micros);
            if !scaled.is_finite() || scaled < i64::MIN as f64 || scaled >= -(i64::MIN as f64) {
                return Err(ArithmeticError::Range);
            }
            let picos = if qualifier.fractional_precision() > TemporalPrecision::MICROSECONDS {
                let whole = scaled.floor();
                let quantum = qualifier.fractional_precision().picosecond_step();
                let picos =
                    ((scaled - whole) * (fraction_base / quantum as f64) + 0.5).floor() as i64;
                (whole as i128) * i128::from(PICOS_PER_MICROSECOND)
                    + i128::from(picos) * i128::from(quantum)
            } else if qualifier.end() == IntervalField::Second {
                let unit =
                    qualifier.fractional_precision().picosecond_step() / PICOS_PER_MICROSECOND;
                let rounded = (scaled / unit as f64 + 0.5).floor() as i128;
                rounded * i128::from(unit) * i128::from(PICOS_PER_MICROSECOND)
            } else {
                let unit = match qualifier.end() {
                    IntervalField::Day => i128::from(PICOS_PER_DAY),
                    IntervalField::Hour => i128::from(PICOS_PER_MINUTE) * 60,
                    IntervalField::Minute => i128::from(PICOS_PER_MINUTE),
                    _ => return Err(ArithmeticError::Invariant),
                };
                ((scaled as i128) * i128::from(PICOS_PER_MICROSECOND)) / unit * unit
            };
            Ok(IntervalDayTime(micros(picos)?))
        }
        _ => Err(ArithmeticError::Invariant),
    }
}
