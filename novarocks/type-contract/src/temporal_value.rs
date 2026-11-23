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

use crate::{TemporalType, TimeZoneKey};

pub const PICOS_PER_MICROSECOND: i64 = 1_000_000;
pub const PICOS_PER_SECOND: i64 = 1_000_000_000_000;
pub const PICOS_PER_MINUTE: i64 = 60 * PICOS_PER_SECOND;
pub const PICOS_PER_DAY: i64 = 24 * 60 * PICOS_PER_MINUTE;

pub const MIN_ZONED_TIMESTAMP_MILLIS: i64 = -(1 << 51);
pub const MAX_ZONED_TIMESTAMP_MILLIS: i64 = (1 << 51) - 1;

/// Euclidean representation for a wall clock, UTC instant or elapsed duration.
/// The sub-microsecond part is always nonnegative, even before the epoch.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TemporalMicros {
    micros: i64,
    fraction: u32,
}

impl TemporalMicros {
    pub const fn new(micros: i64, fraction: u32) -> Option<Self> {
        if fraction < PICOS_PER_MICROSECOND as u32 {
            Some(Self { micros, fraction })
        } else {
            None
        }
    }

    pub const fn whole_micros(micros: i64) -> Self {
        Self {
            micros,
            fraction: 0,
        }
    }

    pub fn from_picos(picos: i128) -> Option<Self> {
        let divisor = i128::from(PICOS_PER_MICROSECOND);
        let micros = i64::try_from(picos.div_euclid(divisor)).ok()?;
        let fraction = picos.rem_euclid(divisor) as u32;
        Some(Self { micros, fraction })
    }

    pub const fn micros(self) -> i64 {
        self.micros
    }

    pub const fn picoseconds_of_microsecond(self) -> u32 {
        self.fraction
    }

    pub const fn floor_millis(self) -> i64 {
        self.micros.div_euclid(1_000)
    }

    pub const fn as_picos(self) -> i128 {
        self.micros as i128 * PICOS_PER_MICROSECOND as i128 + self.fraction as i128
    }

    fn is_aligned(self, step: i64) -> bool {
        if step <= PICOS_PER_MICROSECOND {
            i64::from(self.fraction) % step == 0
        } else {
            self.fraction == 0 && self.micros % (step / PICOS_PER_MICROSECOND) == 0
        }
    }

    fn read_le(bytes: &[u8; 12]) -> Option<Self> {
        let (micros, fraction) = bytes.split_at(8);
        Self::new(
            i64::from_le_bytes(micros.try_into().expect("eight microsecond bytes")),
            u32::from_le_bytes(fraction.try_into().expect("four fraction bytes")),
        )
    }

    fn write_le(self, bytes: &mut [u8; 12]) {
        bytes[..8].copy_from_slice(&self.micros.to_le_bytes());
        bytes[8..].copy_from_slice(&self.fraction.to_le_bytes());
    }
}

/// Full observable value. Relation equality uses `TemporalValue::relation_key`
/// with a concrete declaration, leaving this value available for presentation.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TemporalValue {
    Time(i64),
    TimeWithTimeZone {
        picos: i64,
        offset_minutes: i16,
    },
    Timestamp(TemporalMicros),
    TimestampWithTimeZone {
        instant: TemporalMicros,
        zone: TimeZoneKey,
    },
    IntervalYearMonth(i32),
    IntervalDayTime(TemporalMicros),
}

impl TemporalValue {
    /// Produce canonical carrier bytes for a SQL equality key. Row payloads
    /// and stored representatives continue to use the full observable value.
    pub fn relation_carrier(self, ty: TemporalType) -> Result<TemporalCarrier, TemporalValueError> {
        let normalized = match self.validate(ty)? {
            Self::TimeWithTimeZone {
                picos,
                offset_minutes,
            } => Self::TimeWithTimeZone {
                picos: (picos - i64::from(offset_minutes) * PICOS_PER_MINUTE)
                    .rem_euclid(PICOS_PER_DAY),
                offset_minutes: 0,
            },
            Self::TimestampWithTimeZone { instant, .. } => Self::TimestampWithTimeZone {
                instant,
                zone: TimeZoneKey::UTC,
            },
            value => value,
        };
        normalized.to_carrier(ty)
    }

    pub fn validate(self, ty: TemporalType) -> Result<Self, TemporalValueError> {
        let step = ty.precision().picosecond_step();
        let time_valid = |picos: i64| (0..PICOS_PER_DAY).contains(&picos) && picos % step == 0;
        match (ty, self) {
            (TemporalType::Time(_), Self::Time(picos)) if time_valid(picos) => {}
            (
                TemporalType::TimeWithTimeZone(_),
                Self::TimeWithTimeZone {
                    picos,
                    offset_minutes,
                },
            ) if time_valid(picos)
                && TimeZoneKey::from_offset_minutes(offset_minutes).is_some() => {}
            (TemporalType::Timestamp(_), Self::Timestamp(value)) if value.is_aligned(step) => {}
            (
                TemporalType::TimestampWithTimeZone(_),
                Self::TimestampWithTimeZone { instant, .. },
            ) if instant.is_aligned(step)
                && (MIN_ZONED_TIMESTAMP_MILLIS..=MAX_ZONED_TIMESTAMP_MILLIS)
                    .contains(&instant.floor_millis()) => {}
            (TemporalType::Interval(q), Self::IntervalYearMonth(_)) if q.is_year_month() => {}
            (TemporalType::Interval(q), Self::IntervalDayTime(value))
                if !q.is_year_month() && value.is_aligned(step) => {}
            _ => return Err(TemporalValueError::OutOfRange),
        }
        Ok(self)
    }

    pub fn from_i32_carrier(ty: TemporalType, value: i32) -> Result<Self, TemporalValueError> {
        Self::IntervalYearMonth(value).validate(ty)
    }

    /// A primitive 64-bit carrier. The caller has already matched its Arrow
    /// kind, including Timestamp(Microsecond, None) for the compact timestamp.
    pub fn from_i64_carrier(ty: TemporalType, value: i64) -> Result<Self, TemporalValueError> {
        let value = match ty {
            TemporalType::Time(_) => Self::Time(value),
            TemporalType::Timestamp(p) if p.fits_microseconds() => {
                Self::Timestamp(TemporalMicros::whole_micros(value))
            }
            TemporalType::Interval(q)
                if !q.is_year_month() && q.fractional_precision().fits_microseconds() =>
            {
                Self::IntervalDayTime(TemporalMicros::whole_micros(value))
            }
            _ => return Err(TemporalValueError::Carrier),
        };
        value.validate(ty)
    }

    pub fn from_fixed_carrier(ty: TemporalType, bytes: &[u8]) -> Result<Self, TemporalValueError> {
        let wrong_carrier = |_| TemporalValueError::Carrier;
        let fraction = |bytes: &[u8]| {
            TemporalMicros::read_le(bytes.try_into().map_err(wrong_carrier)?)
                .ok_or(TemporalValueError::OutOfRange)
        };
        let value = match ty {
            TemporalType::TimeWithTimeZone(_) => {
                let bytes: &[u8; 10] = bytes.try_into().map_err(wrong_carrier)?;
                Self::TimeWithTimeZone {
                    picos: i64::from_le_bytes(bytes[..8].try_into().expect("eight time bytes")),
                    offset_minutes: i16::from_le_bytes(
                        bytes[8..].try_into().expect("two minute bytes"),
                    ),
                }
            }
            TemporalType::Timestamp(p) if !p.fits_microseconds() => {
                Self::Timestamp(fraction(bytes)?)
            }
            TemporalType::TimestampWithTimeZone(_) => {
                let bytes: &[u8; 14] = bytes.try_into().map_err(wrong_carrier)?;
                let zone = u16::from_le_bytes(bytes[12..].try_into().expect("two zone key bytes"));
                Self::TimestampWithTimeZone {
                    instant: fraction(&bytes[..12])?,
                    zone: TimeZoneKey::from_raw(zone).ok_or(TemporalValueError::TimeZoneKey)?,
                }
            }
            TemporalType::Interval(q) if !q.fractional_precision().fits_microseconds() => {
                Self::IntervalDayTime(fraction(bytes)?)
            }
            _ => return Err(TemporalValueError::Carrier),
        };
        value.validate(ty)
    }

    pub fn to_carrier(self, ty: TemporalType) -> Result<TemporalCarrier, TemporalValueError> {
        let valid = self.validate(ty)?;
        let fixed = |value: TemporalMicros| {
            let mut bytes = [0; 12];
            value.write_le(&mut bytes);
            TemporalCarrier::Wide(bytes)
        };
        Ok(match valid {
            Self::Time(picos) => TemporalCarrier::I64(picos),
            Self::TimeWithTimeZone {
                picos,
                offset_minutes,
            } => {
                let mut bytes = [0; 10];
                bytes[..8].copy_from_slice(&picos.to_le_bytes());
                bytes[8..].copy_from_slice(&offset_minutes.to_le_bytes());
                TemporalCarrier::ZonedTime(bytes)
            }
            Self::Timestamp(value) if ty.precision().fits_microseconds() => {
                TemporalCarrier::TimestampMicros(value.micros())
            }
            Self::Timestamp(value) => fixed(value),
            Self::TimestampWithTimeZone { instant, zone } => {
                let mut bytes = [0; 14];
                instant.write_le(
                    (&mut bytes[..12])
                        .try_into()
                        .expect("twelve timestamp bytes"),
                );
                bytes[12..].copy_from_slice(&zone.raw().to_le_bytes());
                TemporalCarrier::ZonedTimestamp(bytes)
            }
            Self::IntervalYearMonth(months) => TemporalCarrier::I32(months),
            Self::IntervalDayTime(value) if ty.precision().fits_microseconds() => {
                TemporalCarrier::I64(value.micros())
            }
            Self::IntervalDayTime(value) => fixed(value),
        })
    }

    /// The relation key is meaningful only under the declared common type.
    /// Zone projection survives separately in the original row or constant.
    pub fn relation_key(self, ty: TemporalType) -> Result<TemporalRelationKey, TemporalValueError> {
        let value = self.validate(ty)?;
        let coarse = match value {
            Self::Time(picos) => picos,
            Self::TimeWithTimeZone {
                picos,
                offset_minutes,
            } => (picos - i64::from(offset_minutes) * PICOS_PER_MINUTE).rem_euclid(PICOS_PER_DAY),
            Self::Timestamp(value)
            | Self::TimestampWithTimeZone { instant: value, .. }
            | Self::IntervalDayTime(value) => {
                return Ok(TemporalRelationKey {
                    coarse: value.micros,
                    fraction: value.fraction,
                });
            }
            Self::IntervalYearMonth(months) => i64::from(months),
        };
        Ok(TemporalRelationKey {
            coarse,
            fraction: 0,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TemporalCarrier {
    I32(i32),
    I64(i64),
    TimestampMicros(i64),
    ZonedTime([u8; 10]),
    Wide([u8; 12]),
    ZonedTimestamp([u8; 14]),
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TemporalRelationKey {
    coarse: i64,
    fraction: u32,
}

impl TemporalRelationKey {
    /// Check an encoded relational key against its frozen declaration. Its
    /// normalized zoned representative uses UTC or offset zero by definition.
    pub fn from_order_bytes(ty: TemporalType, bytes: [u8; 12]) -> Result<Self, TemporalValueError> {
        let (sign_mapped, fraction) = bytes.split_at(8);
        let coarse = (u64::from_be_bytes(sign_mapped.try_into().expect("eight order bytes"))
            ^ (1_u64 << 63)) as i64;
        let fraction = u32::from_be_bytes(fraction.try_into().expect("four order bytes"));
        let time = || TemporalMicros::new(coarse, fraction).ok_or(TemporalValueError::OutOfRange);
        let value = match ty {
            TemporalType::Time(_) => TemporalValue::Time(coarse),
            TemporalType::TimeWithTimeZone(_) => TemporalValue::TimeWithTimeZone {
                picos: coarse,
                offset_minutes: 0,
            },
            TemporalType::Timestamp(_) => TemporalValue::Timestamp(time()?),
            TemporalType::TimestampWithTimeZone(_) => TemporalValue::TimestampWithTimeZone {
                instant: time()?,
                zone: TimeZoneKey::UTC,
            },
            TemporalType::Interval(q) if q.is_year_month() => TemporalValue::IntervalYearMonth(
                i32::try_from(coarse).map_err(|_| TemporalValueError::OutOfRange)?,
            ),
            TemporalType::Interval(_) => TemporalValue::IntervalDayTime(time()?),
        };
        let key = value.relation_key(ty)?;
        if key.coarse == coarse && key.fraction == fraction {
            Ok(key)
        } else {
            Err(TemporalValueError::OutOfRange)
        }
    }

    /// The key's fixed-width order encoding is independent of the payload
    /// encoding. It does not carry the observable time zone or storage bytes.
    pub fn order_bytes(self) -> [u8; 12] {
        let mut bytes = [0; 12];
        bytes[..8].copy_from_slice(&((self.coarse as u64) ^ (1_u64 << 63)).to_be_bytes());
        bytes[8..].copy_from_slice(&self.fraction.to_be_bytes());
        bytes
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TemporalValueError {
    Carrier,
    OutOfRange,
    TimeZoneKey,
}

impl std::fmt::Display for TemporalValueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Carrier => "Temporal value has the wrong carrier",
            Self::OutOfRange => "Temporal value is outside the declared range or precision",
            Self::TimeZoneKey => "Temporal value uses an unknown time-zone key",
        })
    }
}

impl std::error::Error for TemporalValueError {}
