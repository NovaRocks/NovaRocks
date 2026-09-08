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

//! Typed value vocabulary shared by the connector read stack.
//!
//! The predicate algebra in [`super::predicate`] is defined over these values.
//! They are transport-neutral: no provider, file format, Arrow, or generated
//! wire type appears here. Every value carries its exact type, and comparison
//! is only defined between two values of the same exact type.

use std::cmp::Ordering;
use std::sync::Arc;

use crate::connector::{ConnectorError, ConnectorErrorKind};

/// Maximum decimal precision expressible by a connector value.
pub const MAX_CONNECTOR_DECIMAL_PRECISION: u8 = 38;

/// The exact type of a [`ConnectorValue`].
///
/// Two values are only comparable when their types are equal, including
/// decimal precision/scale and fixed-length width. There is no implicit
/// widening, unit conversion, collation, or time-zone normalization.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ConnectorValueType {
    Boolean,
    /// An eight-bit signed integer. Only an engine-derived column has this
    /// type; no Iceberg field does.
    TinyInt,
    SmallInt,
    Integer,
    BigInt,
    Real,
    Double,
    Decimal {
        precision: u8,
        scale: i8,
    },
    Date,
    TimeMicros,
    TimestampMicros,
    TimestampMillis,
    TimestampTzMicros,
    TimestampNanos,
    TimestampTzNanos,
    Varchar,
    Varbinary,
    Uuid,
    Fixed {
        length: u32,
    },
    /// A column whose engine type has no comparable counterpart: ROW, ARRAY,
    /// MAP, and the binary-carried variant types.
    ///
    /// It may only type an assignment or the expression variable that names
    /// one. A [`ConnectorValue`] of this type does not exist, so a
    /// [`super::predicate::Domain`] over it is unconstructible in the values it
    /// would need; the wire decoder refuses one outright.
    NonComparable,
}

impl ConnectorValueType {
    /// Whether this type orders its values, so a [`super::predicate::Range`]
    /// can be expressed over it.
    pub const fn is_orderable(self) -> bool {
        !matches!(self, Self::Boolean | Self::NonComparable)
    }

    /// Whether values of this type can be compared at all, so a
    /// [`super::predicate::Domain`] can be expressed over it.
    pub const fn is_comparable(self) -> bool {
        !matches!(self, Self::NonComparable)
    }
}

/// One exactly typed, non-null value.
///
/// `NULL` is never a `ConnectorValue`; nullability is carried by
/// [`super::predicate::Domain`] instead.
#[derive(Clone, Debug)]
pub enum ConnectorValue {
    Boolean(bool),
    TinyInt(i8),
    SmallInt(i16),
    Integer(i32),
    BigInt(i64),
    Real(f32),
    Double(f64),
    Decimal {
        unscaled: i128,
        precision: u8,
        scale: i8,
    },
    /// Days since the Unix epoch.
    Date(i32),
    /// Microseconds since midnight.
    TimeMicros(i64),
    /// Microseconds since the Unix epoch, without a time zone.
    TimestampMicros(i64),
    TimestampMillis(i64),
    /// Microseconds since the Unix epoch, in UTC.
    TimestampTzMicros(i64),
    /// Nanoseconds since the Unix epoch, without a time zone.
    TimestampNanos(i64),
    /// Nanoseconds since the Unix epoch, in UTC.
    TimestampTzNanos(i64),
    Varchar(Arc<str>),
    Varbinary(Arc<[u8]>),
    Uuid([u8; 16]),
    Fixed(Arc<[u8]>),
}

impl ConnectorValue {
    pub fn value_type(&self) -> ConnectorValueType {
        match self {
            Self::Boolean(_) => ConnectorValueType::Boolean,
            Self::TinyInt(_) => ConnectorValueType::TinyInt,
            Self::SmallInt(_) => ConnectorValueType::SmallInt,
            Self::Integer(_) => ConnectorValueType::Integer,
            Self::BigInt(_) => ConnectorValueType::BigInt,
            Self::Real(_) => ConnectorValueType::Real,
            Self::Double(_) => ConnectorValueType::Double,
            Self::Decimal {
                precision, scale, ..
            } => ConnectorValueType::Decimal {
                precision: *precision,
                scale: *scale,
            },
            Self::Date(_) => ConnectorValueType::Date,
            Self::TimeMicros(_) => ConnectorValueType::TimeMicros,
            Self::TimestampMicros(_) => ConnectorValueType::TimestampMicros,
            Self::TimestampMillis(_) => ConnectorValueType::TimestampMillis,
            Self::TimestampTzMicros(_) => ConnectorValueType::TimestampTzMicros,
            Self::TimestampNanos(_) => ConnectorValueType::TimestampNanos,
            Self::TimestampTzNanos(_) => ConnectorValueType::TimestampTzNanos,
            Self::Varchar(_) => ConnectorValueType::Varchar,
            Self::Varbinary(_) => ConnectorValueType::Varbinary,
            Self::Uuid(_) => ConnectorValueType::Uuid,
            Self::Fixed(value) => ConnectorValueType::Fixed {
                length: value.len() as u32,
            },
        }
    }

    /// Number of payload bytes this value retains, for bounds accounting.
    pub fn payload_bytes(&self) -> usize {
        match self {
            Self::Boolean(_) | Self::TinyInt(_) => 1,
            Self::SmallInt(_) => 2,
            Self::Integer(_) | Self::Date(_) | Self::Real(_) => 4,
            Self::BigInt(_)
            | Self::Double(_)
            | Self::TimeMicros(_)
            | Self::TimestampMillis(_)
            | Self::TimestampMicros(_)
            | Self::TimestampTzMicros(_)
            | Self::TimestampNanos(_)
            | Self::TimestampTzNanos(_) => 8,
            Self::Decimal { .. } | Self::Uuid(_) => 16,
            Self::Varchar(value) => value.len(),
            Self::Varbinary(value) | Self::Fixed(value) => value.len(),
        }
    }

    /// A decimal value whose precision and scale are validated up front.
    pub fn try_decimal(unscaled: i128, precision: u8, scale: i8) -> Result<Self, ConnectorError> {
        if precision == 0 || precision > MAX_CONNECTOR_DECIMAL_PRECISION {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector decimal precision is out of range",
            ));
        }
        if scale < 0 || scale > precision as i8 {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector decimal scale is out of range for its precision",
            ));
        }
        Ok(Self::Decimal {
            unscaled,
            precision,
            scale,
        })
    }

    /// Total order within one exact type.
    ///
    /// Returns `None` when the two values have different exact types, or when
    /// either side is a floating-point NaN. NaN has no position in a range, so
    /// callers must fail closed rather than guess one.
    pub fn try_compare_same_type(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Self::Boolean(left), Self::Boolean(right)) => Some(left.cmp(right)),
            (Self::TinyInt(left), Self::TinyInt(right)) => Some(left.cmp(right)),
            (Self::SmallInt(left), Self::SmallInt(right)) => Some(left.cmp(right)),
            (Self::Integer(left), Self::Integer(right)) => Some(left.cmp(right)),
            (Self::BigInt(left), Self::BigInt(right)) => Some(left.cmp(right)),
            (Self::Real(left), Self::Real(right)) => {
                if left.is_nan() || right.is_nan() {
                    return None;
                }
                left.partial_cmp(right)
            }
            (Self::Double(left), Self::Double(right)) => {
                if left.is_nan() || right.is_nan() {
                    return None;
                }
                left.partial_cmp(right)
            }
            (
                Self::Decimal {
                    unscaled: left,
                    precision: left_precision,
                    scale: left_scale,
                },
                Self::Decimal {
                    unscaled: right,
                    precision: right_precision,
                    scale: right_scale,
                },
            ) => {
                if left_precision != right_precision || left_scale != right_scale {
                    return None;
                }
                Some(left.cmp(right))
            }
            (Self::Date(left), Self::Date(right)) => Some(left.cmp(right)),
            (Self::TimeMicros(left), Self::TimeMicros(right))
            | (Self::TimestampMillis(left), Self::TimestampMillis(right))
            | (Self::TimestampMicros(left), Self::TimestampMicros(right))
            | (Self::TimestampTzMicros(left), Self::TimestampTzMicros(right))
            | (Self::TimestampNanos(left), Self::TimestampNanos(right))
            | (Self::TimestampTzNanos(left), Self::TimestampTzNanos(right)) => {
                Some(left.cmp(right))
            }
            (Self::Varchar(left), Self::Varchar(right)) => {
                Some(left.as_bytes().cmp(right.as_bytes()))
            }
            (Self::Varbinary(left), Self::Varbinary(right)) => {
                Some(left.as_ref().cmp(right.as_ref()))
            }
            (Self::Uuid(left), Self::Uuid(right)) => Some(left.cmp(right)),
            (Self::Fixed(left), Self::Fixed(right)) => {
                if left.len() != right.len() {
                    return None;
                }
                Some(left.as_ref().cmp(right.as_ref()))
            }
            _ => None,
        }
    }
}

impl PartialEq for ConnectorValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Real(left), Self::Real(right)) => left.to_bits() == right.to_bits(),
            (Self::Double(left), Self::Double(right)) => left.to_bits() == right.to_bits(),
            _ => {
                self.value_type() == other.value_type()
                    && self.try_compare_same_type(other) == Some(Ordering::Equal)
            }
        }
    }
}

impl Eq for ConnectorValue {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_only_compare_within_one_exact_type() {
        assert_eq!(
            ConnectorValue::Integer(1).try_compare_same_type(&ConnectorValue::Integer(2)),
            Some(Ordering::Less)
        );
        assert_eq!(
            ConnectorValue::Integer(1).try_compare_same_type(&ConnectorValue::BigInt(1)),
            None
        );
    }

    #[test]
    fn nan_has_no_ordering_position() {
        let nan = ConnectorValue::Double(f64::NAN);
        assert_eq!(
            nan.try_compare_same_type(&ConnectorValue::Double(0.0)),
            None
        );
        assert_eq!(nan.try_compare_same_type(&nan), None);
        assert_eq!(nan, ConnectorValue::Double(f64::NAN));
    }

    #[test]
    fn decimals_only_compare_at_the_same_precision_and_scale() {
        let left = ConnectorValue::try_decimal(100, 10, 2).expect("valid decimal");
        let right = ConnectorValue::try_decimal(100, 10, 3).expect("valid decimal");
        assert_eq!(left.try_compare_same_type(&right), None);
        assert!(ConnectorValue::try_decimal(1, 0, 0).is_err());
        assert!(ConnectorValue::try_decimal(1, 10, 11).is_err());
    }

    #[test]
    fn fixed_width_values_only_compare_at_equal_width() {
        let left = ConnectorValue::Fixed(Arc::from(&[1_u8, 2][..]));
        let right = ConnectorValue::Fixed(Arc::from(&[1_u8, 2, 3][..]));
        assert_eq!(left.try_compare_same_type(&right), None);
        assert_ne!(left.value_type(), right.value_type());
    }
}
