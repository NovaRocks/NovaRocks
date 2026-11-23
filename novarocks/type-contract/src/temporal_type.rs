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

use arrow_schema::{DataType, TimeUnit};

// Design: ADR-0253 (docs/adr/ADR-0253-exact-temporal-values-and-frozen-statement-time.md)
/// Trino's fractional precision describes the smallest represented part of a second.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TemporalPrecision(u8);

impl TemporalPrecision {
    pub const DEFAULT: Self = Self(3);
    pub const MICROSECONDS: Self = Self(6);
    pub const NANOSECONDS: Self = Self(9);
    pub const PICOSECONDS: Self = Self(12);
    pub const WHOLE_SECONDS: Self = Self(0);

    pub const fn new(digits: u8) -> Result<Self, TemporalTypeError> {
        if digits <= Self::PICOSECONDS.digits() {
            Ok(Self(digits))
        } else {
            Err(TemporalTypeError::FractionalPrecision)
        }
    }

    pub const fn digits(self) -> u8 {
        self.0
    }

    pub const fn picosecond_step(self) -> i64 {
        10_i64.pow((Self::PICOSECONDS.0 - self.0) as u32)
    }

    pub const fn fits_microseconds(self) -> bool {
        self.0 <= Self::MICROSECONDS.0
    }
}

/// Ordered SQL interval fields. The stable codes follow the SQL field order.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub enum IntervalField {
    Year = 0,
    Month = 1,
    Day = 2,
    Hour = 3,
    Minute = 4,
    Second = 5,
}

impl IntervalField {
    pub const fn max_leading_precision(self) -> u8 {
        match self {
            Self::Year | Self::Day => 9,
            Self::Month | Self::Hour => 10,
            Self::Minute => 12,
            Self::Second => 13,
        }
    }

    pub const fn default_fractional_precision(self) -> TemporalPrecision {
        match self {
            Self::Second => TemporalPrecision::MICROSECONDS,
            _ => TemporalPrecision::WHOLE_SECONDS,
        }
    }

    fn units_per_day(self) -> u64 {
        match self {
            Self::Day => 1,
            Self::Hour => 24,
            Self::Minute => 24 * 60,
            Self::Second => 24 * 60 * 60,
            Self::Year | Self::Month => unreachable!("day-time interval field"),
        }
    }
}

/// A complete interval type. Structural precision is deliberately independent
/// of the leading field's assignment limit during overload resolution.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct IntervalQualifier {
    start: IntervalField,
    end: IntervalField,
    leading_precision: u8,
    fractional_precision: TemporalPrecision,
}

impl IntervalQualifier {
    pub fn new(
        start: IntervalField,
        end: IntervalField,
        leading_precision: u8,
        fractional_precision: TemporalPrecision,
    ) -> Result<Self, TemporalTypeError> {
        let max_leading_precision = if start <= IntervalField::Month && end <= IntervalField::Month
        {
            10
        } else if start >= IntervalField::Day {
            13
        } else {
            return Err(TemporalTypeError::IntervalQualifier);
        };
        if start > end || end != IntervalField::Second && fractional_precision.digits() != 0 {
            return Err(TemporalTypeError::IntervalQualifier);
        }
        if !(1..=max_leading_precision).contains(&leading_precision) {
            return Err(TemporalTypeError::LeadingPrecision);
        }
        Ok(Self {
            start,
            end,
            leading_precision,
            fractional_precision,
        })
    }

    /// SQL declarations and assignments must honor the bound of their actual
    /// leading field. Derived intermediate types use the structural bound.
    pub fn validate_declared_precision(self) -> Result<(), TemporalTypeError> {
        if self.leading_precision > self.start.max_leading_precision() {
            return Err(TemporalTypeError::LeadingPrecision);
        }
        Ok(())
    }

    pub const fn start(self) -> IntervalField {
        self.start
    }

    pub const fn end(self) -> IntervalField {
        self.end
    }

    pub const fn leading_precision(self) -> u8 {
        self.leading_precision
    }

    pub const fn fractional_precision(self) -> TemporalPrecision {
        self.fractional_precision
    }

    pub const fn is_year_month(self) -> bool {
        (self.start as u8) <= IntervalField::Month as u8
    }
}

/// A SQL temporal declaration with a single canonical Arrow layout.
/// The layout can also carry other SQL types, so callers retain this witness.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum TemporalType {
    Time(TemporalPrecision),
    TimeWithTimeZone(TemporalPrecision),
    Timestamp(TemporalPrecision),
    TimestampWithTimeZone(TemporalPrecision),
    Interval(IntervalQualifier),
}

impl TemporalType {
    pub const fn precision(self) -> TemporalPrecision {
        match self {
            Self::Time(p)
            | Self::TimeWithTimeZone(p)
            | Self::Timestamp(p)
            | Self::TimestampWithTimeZone(p) => p,
            Self::Interval(qualifier) => qualifier.fractional_precision(),
        }
    }

    pub const fn carrier(self) -> DataType {
        match self {
            Self::Time(_) => DataType::Int64,
            Self::TimeWithTimeZone(_) => DataType::FixedSizeBinary(10),
            Self::Timestamp(p) if p.fits_microseconds() => {
                DataType::Timestamp(TimeUnit::Microsecond, None)
            }
            Self::Timestamp(_) => DataType::FixedSizeBinary(12),
            Self::TimestampWithTimeZone(_) => DataType::FixedSizeBinary(14),
            Self::Interval(qualifier) if qualifier.is_year_month() => DataType::Int32,
            Self::Interval(qualifier) if qualifier.fractional_precision().fits_microseconds() => {
                DataType::Int64
            }
            Self::Interval(_) => DataType::FixedSizeBinary(12),
        }
    }

    /// The target admits every value of this datetime declaration unchanged.
    pub fn is_lossless_widening_to(self, target: Self) -> bool {
        use TemporalType::*;
        matches!(
            (self, target),
            (Time(_), Time(_))
                | (TimeWithTimeZone(_), TimeWithTimeZone(_))
                | (Timestamp(_), Timestamp(_))
                | (TimestampWithTimeZone(_), TimestampWithTimeZone(_))
        ) && self.precision() <= target.precision()
    }

    /// Trino's implicit common type for a pair of temporal declarations.
    pub fn common_type(self, other: Self) -> Option<Self> {
        use TemporalType::*;
        if self == other {
            return Some(self);
        }
        let precision = self.precision().max(other.precision());
        Some(match (self, other) {
            (Time(_), Time(_)) => Time(precision),
            (Time(_) | TimeWithTimeZone(_), TimeWithTimeZone(_))
            | (TimeWithTimeZone(_), Time(_)) => TimeWithTimeZone(precision),
            (Timestamp(_), Timestamp(_)) => Timestamp(precision),
            (Timestamp(_) | TimestampWithTimeZone(_), TimestampWithTimeZone(_))
            | (TimestampWithTimeZone(_), Timestamp(_)) => TimestampWithTimeZone(precision),
            (Interval(left), Interval(right)) if left.is_year_month() == right.is_year_month() => {
                let start = left.start().min(right.start());
                let end = left.end().max(right.end());
                let width = |source: IntervalQualifier| {
                    let ratio = if source.is_year_month() {
                        if source.start() == start { 1 } else { 12 }
                    } else {
                        source.start().units_per_day() / start.units_per_day()
                    };
                    let bound = (10_u64.pow(u32::from(source.leading_precision())) - 1) / ratio;
                    let digits = if bound == 0 {
                        1
                    } else {
                        bound.ilog10() as u8 + 1
                    };
                    digits.min(start.max_leading_precision())
                };
                Interval(
                    IntervalQualifier::new(start, end, width(left).max(width(right)), precision)
                        .ok()?,
                )
            }
            _ => return None,
        })
    }

    pub(crate) const fn identity_bytes(self) -> [u8; 4] {
        match self {
            Self::Time(p) => [0, p.digits(), 0, 0],
            Self::TimeWithTimeZone(p) => [1, p.digits(), 0, 0],
            Self::Timestamp(p) => [2, p.digits(), 0, 0],
            Self::TimestampWithTimeZone(p) => [3, p.digits(), 0, 0],
            Self::Interval(q) => [
                4,
                q.fractional_precision().digits(),
                (q.start() as u8) << 4 | q.end() as u8,
                q.leading_precision(),
            ],
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TemporalTypeError {
    FractionalPrecision,
    LeadingPrecision,
    IntervalQualifier,
}

impl std::fmt::Display for TemporalTypeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::FractionalPrecision => "Temporal fractional precision must be between 0 and 12",
            Self::LeadingPrecision => "Invalid precision for interval leading field",
            Self::IntervalQualifier => "Invalid interval field qualifier",
        })
    }
}

impl std::error::Error for TemporalTypeError {}
