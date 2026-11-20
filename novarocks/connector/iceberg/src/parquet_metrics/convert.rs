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

use std::cmp::Ordering;

use parquet::basic::{ConvertedType, LogicalType, TimeUnit, Type as PhysicalType};
use parquet::file::statistics::Statistics;
use parquet::schema::types::ColumnDescriptor;

use crate::iceberg::spec::PrimitiveType;

use super::truncate::{BoundBytes, binary_bound, string_bound};

#[derive(Clone, Copy, PartialEq, PartialOrd)]
pub(super) enum Value<'a> {
    Boolean(bool),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    Decimal(i128),
    Bytes(&'a [u8]),
}

impl Value<'_> {
    fn compare(self, other: Self) -> Option<Ordering> {
        match (self, other) {
            (Self::Float(a), Self::Float(b)) => Some(a.total_cmp(&b)),
            (Self::Double(a), Self::Double(b)) => Some(a.total_cmp(&b)),
            _ => self.partial_cmp(&other),
        }
    }

    pub(super) fn min(self, other: Self) -> Self {
        if self.compare(other) == Some(Ordering::Greater) {
            other
        } else {
            self
        }
    }

    pub(super) fn max(self, other: Self) -> Self {
        if self.compare(other) == Some(Ordering::Less) {
            other
        } else {
            self
        }
    }
}

#[derive(Clone, Copy)]
enum Encoding {
    Natural,
    Integer { width: u8, signed: bool },
    Decimal { precision: u32 },
    Time(TimeUnit),
    TimestampMillis,
}

pub(super) struct Conversion {
    pub(super) output_type: PrimitiveType,
    encoding: Encoding,
}

impl Conversion {
    pub(super) fn new(column: &ColumnDescriptor, target: Option<&PrimitiveType>) -> Option<Self> {
        let (source, encoding) = source_type(column)?;
        let output_type = match target {
            None => source,
            Some(target) if promote(&source, target) => target.clone(),
            _ => return None,
        };
        Some(Self {
            output_type,
            encoding,
        })
    }

    pub(super) fn decode_bounds<'a>(
        &self,
        stats: &'a Statistics,
    ) -> Option<(Value<'a>, Value<'a>)> {
        use Statistics::*;
        let (low, high) = match stats {
            Boolean(stats) => (
                Value::Boolean(*stats.min_opt()?),
                Value::Boolean(*stats.max_opt()?),
            ),
            Int32(stats) => (
                self.integer((*stats.min_opt()?).into())?,
                self.integer((*stats.max_opt()?).into())?,
            ),
            Int64(stats) => (
                self.integer(*stats.min_opt()?)?,
                self.integer(*stats.max_opt()?)?,
            ),
            Float(stats) => (
                self.float(*stats.min_opt()?)?,
                self.float(*stats.max_opt()?)?,
            ),
            Double(stats) => {
                let (low, high) = (*stats.min_opt()?, *stats.max_opt()?);
                if low.is_nan() || high.is_nan() {
                    return None;
                }
                (Value::Double(low), Value::Double(high))
            }
            ByteArray(_) | FixedLenByteArray(_) => {
                if stats.is_min_max_deprecated() {
                    return None;
                }
                (
                    self.bytes(stats.min_bytes_opt()?)?,
                    self.bytes(stats.max_bytes_opt()?)?,
                )
            }
            Int96(_) => return None,
        };
        (low.compare(high)? != Ordering::Greater).then_some((low, high))
    }

    fn integer(&self, value: i64) -> Option<Value<'static>> {
        let value = match self.encoding {
            Encoding::Integer {
                width,
                signed: true,
            } => match width {
                8 => i64::from(i8::try_from(value).ok()?),
                16 => i64::from(i16::try_from(value).ok()?),
                32 => i64::from(i32::try_from(value).ok()?),
                64 => value,
                _ => return None,
            },
            Encoding::Integer {
                width,
                signed: false,
            } => match width {
                8 => i64::from(u8::try_from(value).ok()?),
                16 => i64::from(u16::try_from(value).ok()?),
                32 => i64::from(i32::try_from(value).ok()? as u32),
                _ => return None,
            },
            Encoding::Decimal { precision } => return decimal(value.into(), precision),
            Encoding::Time(unit) => {
                let micros = match unit {
                    TimeUnit::MILLIS => value.checked_mul(1_000)?,
                    TimeUnit::MICROS => value,
                    TimeUnit::NANOS if value % 1_000 == 0 => value / 1_000,
                    _ => return None,
                };
                if !(0..86_400_000_000).contains(&micros) {
                    return None;
                }
                micros
            }
            Encoding::TimestampMillis => value.checked_mul(1_000)?,
            Encoding::Natural => value,
        };
        match self.output_type {
            PrimitiveType::Int | PrimitiveType::Date => {
                Some(Value::Int(i32::try_from(value).ok()?))
            }
            PrimitiveType::Long
            | PrimitiveType::Time
            | PrimitiveType::Timestamp
            | PrimitiveType::Timestamptz
            | PrimitiveType::TimestampNs
            | PrimitiveType::TimestamptzNs => Some(Value::Long(value)),
            _ => None,
        }
    }

    fn float(&self, value: f32) -> Option<Value<'static>> {
        if value.is_nan() {
            return None;
        }
        if self.output_type == PrimitiveType::Double {
            Some(Value::Double(f64::from(value)))
        } else {
            Some(Value::Float(value))
        }
    }

    fn bytes<'a>(&self, bytes: &'a [u8]) -> Option<Value<'a>> {
        match self.output_type {
            PrimitiveType::String => {
                std::str::from_utf8(bytes).ok()?;
            }
            PrimitiveType::Uuid => {
                <&[u8; 16]>::try_from(bytes).ok()?;
            }
            PrimitiveType::Fixed(width) => {
                if bytes.len() as u64 != width {
                    return None;
                }
            }
            PrimitiveType::Decimal { .. } => {
                let Encoding::Decimal { precision } = self.encoding else {
                    return None;
                };
                return decimal(signed_big_endian(bytes)?, precision);
            }
            PrimitiveType::Binary => {}
            _ => return None,
        }
        Some(Value::Bytes(bytes))
    }

    pub(super) fn encode<'a>(&self, value: Value<'a>, upper: bool) -> Option<BoundBytes<'a>> {
        let bytes = match value {
            Value::Boolean(value) => BoundBytes::inline(&[u8::from(value)]),
            Value::Int(value) => BoundBytes::inline(&value.to_le_bytes()),
            Value::Long(value) => BoundBytes::inline(&value.to_le_bytes()),
            Value::Float(value) => BoundBytes::inline(&value.to_le_bytes()),
            Value::Double(value) => BoundBytes::inline(&value.to_le_bytes()),
            Value::Decimal(value) => {
                let bytes = value.to_be_bytes();
                BoundBytes::inline(minimal_signed(&bytes))
            }
            Value::Bytes(bytes) => match self.output_type {
                PrimitiveType::String => {
                    return string_bound(std::str::from_utf8(bytes).ok()?, upper);
                }
                PrimitiveType::Binary => return binary_bound(bytes, upper),
                _ => BoundBytes::Borrowed(bytes),
            },
        };
        Some(bytes)
    }
}

fn promote(source: &PrimitiveType, target: &PrimitiveType) -> bool {
    use PrimitiveType::*;
    source == target
        || match (source, target) {
            (Int, Long) | (Float, Double) | (Fixed(16), Uuid) | (Uuid, Fixed(16)) => true,
            (
                Decimal {
                    precision: a,
                    scale: sa,
                },
                Decimal {
                    precision: b,
                    scale: sb,
                },
            ) => sa == sb && a <= b && *b <= 38,
            _ => false,
        }
}

fn decimal(value: i128, precision: u32) -> Option<Value<'static>> {
    (value.unsigned_abs() < 10u128.checked_pow(precision)?).then_some(Value::Decimal(value))
}

fn signed_big_endian(bytes: &[u8]) -> Option<i128> {
    let bytes = minimal_signed(bytes);
    if bytes.is_empty() || bytes.len() > 16 {
        return None;
    }
    let mut widened = [if bytes[0] & 0x80 == 0 { 0 } else { 0xff }; 16];
    widened[16 - bytes.len()..].copy_from_slice(bytes);
    Some(i128::from_be_bytes(widened))
}

fn minimal_signed(bytes: &[u8]) -> &[u8] {
    let head = bytes
        .windows(2)
        .take_while(|pair| {
            pair[0] == 0 && pair[1] & 0x80 == 0 || pair[0] == 0xff && pair[1] & 0x80 != 0
        })
        .count();
    &bytes[head..]
}

fn source_type(column: &ColumnDescriptor) -> Option<(PrimitiveType, Encoding)> {
    use ConvertedType as C;
    use LogicalType as L;
    use PhysicalType as P;
    use PrimitiveType as I;

    let physical = column.physical_type();
    let natural = match (column.logical_type_ref(), column.converted_type()) {
        (Some(L::Integer(integer)), _) => {
            return integer_type(
                physical,
                u8::try_from(integer.bit_width).ok()?,
                integer.is_signed,
            );
        }
        (Some(L::Decimal(decimal)), _) => {
            return decimal_type(physical, decimal.precision, decimal.scale);
        }
        (None, C::DECIMAL) => {
            return decimal_type(physical, column.type_precision(), column.type_scale());
        }
        (Some(L::Time(time)), _) => return time_type(physical, time.unit),
        (None, C::TIME_MILLIS) => return time_type(physical, TimeUnit::MILLIS),
        (None, C::TIME_MICROS) => return time_type(physical, TimeUnit::MICROS),
        (Some(L::Timestamp(timestamp)), _) => {
            return timestamp_type(physical, timestamp.unit, timestamp.is_adjusted_to_u_t_c);
        }
        (None, C::TIMESTAMP_MILLIS) => return timestamp_type(physical, TimeUnit::MILLIS, true),
        (None, C::TIMESTAMP_MICROS) => return timestamp_type(physical, TimeUnit::MICROS, true),
        (None, C::INT_8) => return integer_type(physical, 8, true),
        (None, C::INT_16) => return integer_type(physical, 16, true),
        (None, C::INT_32) => return integer_type(physical, 32, true),
        (None, C::INT_64) => return integer_type(physical, 64, true),
        (None, C::UINT_8) => return integer_type(physical, 8, false),
        (None, C::UINT_16) => return integer_type(physical, 16, false),
        (None, C::UINT_32) => return integer_type(physical, 32, false),
        (Some(L::Date), _) | (None, C::DATE) if physical == P::INT32 => I::Date,
        (Some(L::String | L::Enum | L::Json), _) | (None, C::UTF8 | C::ENUM | C::JSON)
            if physical == P::BYTE_ARRAY =>
        {
            I::String
        }
        (Some(L::Bson), _) | (None, C::BSON) if physical == P::BYTE_ARRAY => I::Binary,
        (Some(L::Uuid), _) if physical == P::FIXED_LEN_BYTE_ARRAY && column.type_length() == 16 => {
            I::Uuid
        }
        (None, C::NONE) => match physical {
            P::BOOLEAN => I::Boolean,
            P::INT32 => I::Int,
            P::INT64 => I::Long,
            P::FLOAT => I::Float,
            P::DOUBLE => I::Double,
            P::BYTE_ARRAY => I::Binary,
            P::FIXED_LEN_BYTE_ARRAY => I::Fixed(u64::try_from(column.type_length()).ok()?),
            P::INT96 => return None,
        },
        _ => return None,
    };
    Some((natural, Encoding::Natural))
}

fn integer_type(
    physical: PhysicalType,
    width: u8,
    signed: bool,
) -> Option<(PrimitiveType, Encoding)> {
    use PhysicalType as P;
    let ty = match (physical, width, signed) {
        (P::INT32, 8 | 16, _) | (P::INT32, 32, true) => PrimitiveType::Int,
        (P::INT32, 32, false) | (P::INT64, 64, true) => PrimitiveType::Long,
        _ => return None,
    };
    Some((ty, Encoding::Integer { width, signed }))
}

fn decimal_type(
    physical: PhysicalType,
    precision: i32,
    scale: i32,
) -> Option<(PrimitiveType, Encoding)> {
    if !matches!(
        physical,
        PhysicalType::INT32
            | PhysicalType::INT64
            | PhysicalType::BYTE_ARRAY
            | PhysicalType::FIXED_LEN_BYTE_ARRAY
    ) || !(1..=38).contains(&precision)
        || !(0..=precision).contains(&scale)
    {
        return None;
    }
    let precision = precision as u32;
    Some((
        PrimitiveType::Decimal {
            precision,
            scale: scale as u32,
        },
        Encoding::Decimal { precision },
    ))
}

fn time_type(physical: PhysicalType, unit: TimeUnit) -> Option<(PrimitiveType, Encoding)> {
    match (physical, unit) {
        (PhysicalType::INT32, TimeUnit::MILLIS)
        | (PhysicalType::INT64, TimeUnit::MICROS | TimeUnit::NANOS) => {
            Some((PrimitiveType::Time, Encoding::Time(unit)))
        }
        _ => None,
    }
}

fn timestamp_type(
    physical: PhysicalType,
    unit: TimeUnit,
    utc: bool,
) -> Option<(PrimitiveType, Encoding)> {
    use PrimitiveType as I;
    if physical != PhysicalType::INT64 {
        return None;
    }
    match (unit, utc) {
        (TimeUnit::MILLIS, true) => Some((I::Timestamptz, Encoding::TimestampMillis)),
        (TimeUnit::MILLIS, false) => Some((I::Timestamp, Encoding::TimestampMillis)),
        (TimeUnit::MICROS, true) => Some((I::Timestamptz, Encoding::Natural)),
        (TimeUnit::MICROS, false) => Some((I::Timestamp, Encoding::Natural)),
        (TimeUnit::NANOS, true) => Some((I::TimestamptzNs, Encoding::Natural)),
        (TimeUnit::NANOS, false) => Some((I::TimestampNs, Encoding::Natural)),
    }
}
