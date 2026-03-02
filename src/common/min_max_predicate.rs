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

/// Comparable scalar used by min/max predicates.
///
/// Core variants mirror parquet statistics value families:
/// - `Boolean`, `Int32`, `Int64`, `Float`, `Double`, `ByteArray`, `FixedLenByteArray`
///
/// Compatibility extensions used by StarRocks lowering/runtime:
/// - `Date32`, `DateTimeMicros`, `LargeInt`, `Decimal128`
#[derive(Clone, Debug, PartialEq)]
pub enum MinMaxPredicateValue {
    Boolean(bool),
    Int32(i32),
    Int64(i64),
    Float(f32),
    Double(f64),
    ByteArray(Vec<u8>),
    FixedLenByteArray(Vec<u8>),
    Date32(i32),
    DateTimeMicros(i64),
    LargeInt(i128),
    Decimal128 {
        value: i128,
        precision: u8,
        scale: i8,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub enum MinMaxPredicate {
    Le {
        column: String,
        value: MinMaxPredicateValue,
    }, // <=
    Ge {
        column: String,
        value: MinMaxPredicateValue,
    }, // >=
    Lt {
        column: String,
        value: MinMaxPredicateValue,
    }, // <
    Gt {
        column: String,
        value: MinMaxPredicateValue,
    }, // >
    Eq {
        column: String,
        value: MinMaxPredicateValue,
    }, // =
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MinMaxPredicateOp {
    Le,
    Ge,
    Lt,
    Gt,
    Eq,
}

impl MinMaxPredicate {
    pub(crate) fn column(&self) -> &str {
        match self {
            MinMaxPredicate::Le { column, .. }
            | MinMaxPredicate::Ge { column, .. }
            | MinMaxPredicate::Lt { column, .. }
            | MinMaxPredicate::Gt { column, .. }
            | MinMaxPredicate::Eq { column, .. } => column,
        }
    }

    pub(crate) fn value(&self) -> &MinMaxPredicateValue {
        match self {
            MinMaxPredicate::Le { value, .. }
            | MinMaxPredicate::Ge { value, .. }
            | MinMaxPredicate::Lt { value, .. }
            | MinMaxPredicate::Gt { value, .. }
            | MinMaxPredicate::Eq { value, .. } => value,
        }
    }

    pub(crate) fn op(&self) -> MinMaxPredicateOp {
        match self {
            MinMaxPredicate::Le { .. } => MinMaxPredicateOp::Le,
            MinMaxPredicate::Ge { .. } => MinMaxPredicateOp::Ge,
            MinMaxPredicate::Lt { .. } => MinMaxPredicateOp::Lt,
            MinMaxPredicate::Gt { .. } => MinMaxPredicateOp::Gt,
            MinMaxPredicate::Eq { .. } => MinMaxPredicateOp::Eq,
        }
    }
}

impl MinMaxPredicateValue {
    pub(crate) fn as_i32(&self) -> Option<i32> {
        match self {
            MinMaxPredicateValue::Int32(v) => Some(*v),
            MinMaxPredicateValue::Int64(v) => i32::try_from(*v).ok(),
            MinMaxPredicateValue::Date32(v) => Some(*v),
            _ => None,
        }
    }

    pub(crate) fn as_i64(&self) -> Option<i64> {
        match self {
            MinMaxPredicateValue::Int32(v) => Some(i64::from(*v)),
            MinMaxPredicateValue::Int64(v) => Some(*v),
            MinMaxPredicateValue::Date32(v) => Some(i64::from(*v)),
            MinMaxPredicateValue::DateTimeMicros(v) => Some(*v),
            MinMaxPredicateValue::LargeInt(v) => i64::try_from(*v).ok(),
            _ => None,
        }
    }

    pub(crate) fn as_f32(&self) -> Option<f32> {
        match self {
            MinMaxPredicateValue::Float(v) => Some(*v),
            MinMaxPredicateValue::Double(v) => Some(*v as f32),
            _ => None,
        }
    }

    pub(crate) fn as_f64(&self) -> Option<f64> {
        match self {
            MinMaxPredicateValue::Float(v) => Some(f64::from(*v)),
            MinMaxPredicateValue::Double(v) => Some(*v),
            _ => None,
        }
    }

    pub(crate) fn as_bool(&self) -> Option<bool> {
        match self {
            MinMaxPredicateValue::Boolean(v) => Some(*v),
            _ => None,
        }
    }

    pub(crate) fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            MinMaxPredicateValue::ByteArray(v) => Some(v.as_slice()),
            MinMaxPredicateValue::FixedLenByteArray(v) => Some(v.as_slice()),
            _ => None,
        }
    }

    pub(crate) fn as_date32(&self) -> Option<i32> {
        match self {
            MinMaxPredicateValue::Date32(v) => Some(*v),
            MinMaxPredicateValue::Int32(v) => Some(*v),
            MinMaxPredicateValue::Int64(v) => i32::try_from(*v).ok(),
            _ => None,
        }
    }

    pub(crate) fn as_datetime_micros(&self) -> Option<i64> {
        match self {
            MinMaxPredicateValue::DateTimeMicros(v) => Some(*v),
            MinMaxPredicateValue::Date32(v) => i64::from(*v).checked_mul(86_400_000_000),
            MinMaxPredicateValue::Int64(v) => Some(*v),
            _ => None,
        }
    }

    pub(crate) fn as_decimal128_scaled(&self, target_scale: i8) -> Option<i128> {
        match self {
            MinMaxPredicateValue::Decimal128 { value, scale, .. } => {
                align_decimal_scale(*value, *scale, target_scale)
            }
            MinMaxPredicateValue::Int32(v) => scale_integer(i128::from(*v), target_scale),
            MinMaxPredicateValue::Int64(v) => scale_integer(i128::from(*v), target_scale),
            MinMaxPredicateValue::LargeInt(v) => scale_integer(*v, target_scale),
            _ => None,
        }
    }
}

fn align_decimal_scale(value: i128, from_scale: i8, to_scale: i8) -> Option<i128> {
    if from_scale == to_scale {
        return Some(value);
    }
    if from_scale < 0 || to_scale < 0 {
        return None;
    }
    let from_scale = u32::try_from(from_scale).ok()?;
    let to_scale = u32::try_from(to_scale).ok()?;
    if from_scale < to_scale {
        let factor = pow10_i128(to_scale - from_scale)?;
        value.checked_mul(factor)
    } else {
        let factor = pow10_i128(from_scale - to_scale)?;
        if factor == 0 || value % factor != 0 {
            return None;
        }
        value.checked_div(factor)
    }
}

fn scale_integer(value: i128, target_scale: i8) -> Option<i128> {
    if target_scale < 0 {
        return None;
    }
    let target_scale = u32::try_from(target_scale).ok()?;
    let factor = pow10_i128(target_scale)?;
    value.checked_mul(factor)
}

fn pow10_i128(exp: u32) -> Option<i128> {
    let mut out = 1i128;
    for _ in 0..exp {
        out = out.checked_mul(10)?;
    }
    Some(out)
}
