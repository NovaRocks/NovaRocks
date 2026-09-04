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

use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
    Float32Array, Float64Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray,
    StringArray, Time64MicrosecondArray, TimestampMicrosecondArray, TimestampNanosecondArray,
};
use arrow_schema::{DataType, TimeUnit};

use crate::theta::IcebergThetaError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CanonicalKind {
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Decimal,
    Date,
    TimeMicros,
    TimestampMicros,
    TimestampNanos,
    String,
    LargeString,
    Binary,
    LargeBinary,
    Fixed,
}

impl CanonicalKind {
    pub(crate) fn from_data_type(data_type: &DataType) -> Result<Self, IcebergThetaError> {
        match data_type {
            DataType::Boolean => Ok(Self::Boolean),
            DataType::Int32 => Ok(Self::Int),
            DataType::Int64 => Ok(Self::Long),
            DataType::Float32 => Ok(Self::Float),
            DataType::Float64 => Ok(Self::Double),
            DataType::Decimal128(precision, scale)
                if *precision <= 38 && *scale >= 0 && (*scale as u8) <= *precision =>
            {
                Ok(Self::Decimal)
            }
            DataType::Date32 => Ok(Self::Date),
            DataType::Time64(TimeUnit::Microsecond) => Ok(Self::TimeMicros),
            DataType::Timestamp(TimeUnit::Microsecond, _) => Ok(Self::TimestampMicros),
            DataType::Timestamp(TimeUnit::Nanosecond, _) => Ok(Self::TimestampNanos),
            DataType::Utf8 => Ok(Self::String),
            DataType::LargeUtf8 => Ok(Self::LargeString),
            DataType::Binary => Ok(Self::Binary),
            DataType::LargeBinary => Ok(Self::LargeBinary),
            DataType::FixedSizeBinary(width) if *width > 0 => Ok(Self::Fixed),
            _ => Err(IcebergThetaError::UnsupportedInputType(data_type.clone())),
        }
    }

    pub(crate) const fn identity_suffix(self) -> &'static str {
        match self {
            Self::Boolean => "boolean",
            Self::Int => "int",
            Self::Long => "long",
            Self::Float => "float",
            Self::Double => "double",
            Self::Decimal => "decimal",
            Self::Date => "date",
            Self::TimeMicros => "time-micros",
            Self::TimestampMicros => "timestamp-micros",
            Self::TimestampNanos => "timestamp-nanos",
            Self::String => "string",
            Self::LargeString => "large-string",
            Self::Binary => "binary",
            Self::LargeBinary => "large-binary",
            Self::Fixed => "fixed",
        }
    }

    pub(crate) const fn pattern(self) -> &'static str {
        match self {
            Self::Boolean => "boolean",
            Self::Int => "int32",
            Self::Long => "int64",
            Self::Float => "float32",
            Self::Double => "float64",
            Self::Decimal => "decimal128(p<=38,0<=s<=p)",
            Self::Date => "date32",
            Self::TimeMicros => "time64(microsecond)",
            Self::TimestampMicros => "timestamp(microsecond,timezone?)",
            Self::TimestampNanos => "timestamp(nanosecond,timezone?)",
            Self::String => "utf8",
            Self::LargeString => "large-utf8",
            Self::Binary => "binary",
            Self::LargeBinary => "large-binary",
            Self::Fixed => "fixed-size-binary(width>0)",
        }
    }
}

#[doc(hidden)]
pub enum PreparedCanonicalBatch<'a> {
    Boolean(&'a BooleanArray),
    Int(&'a Int32Array),
    Long(&'a Int64Array),
    Float(&'a Float32Array),
    Double(&'a Float64Array),
    Decimal(&'a Decimal128Array),
    Date(&'a Date32Array),
    TimeMicros(&'a Time64MicrosecondArray),
    TimestampMicros(&'a TimestampMicrosecondArray),
    TimestampNanos(&'a TimestampNanosecondArray),
    String(&'a StringArray),
    LargeString(&'a LargeStringArray),
    Binary(&'a BinaryArray),
    LargeBinary(&'a LargeBinaryArray),
    Fixed(&'a FixedSizeBinaryArray),
}

impl<'a> PreparedCanonicalBatch<'a> {
    pub(crate) fn try_new(array: &'a dyn Array) -> Result<Self, IcebergThetaError> {
        macro_rules! downcast {
            ($kind:ident, $ty:ty) => {
                array
                    .as_any()
                    .downcast_ref::<$ty>()
                    .map(Self::$kind)
                    .ok_or_else(|| IcebergThetaError::ArrayTypeMismatch(array.data_type().clone()))
            };
        }
        match CanonicalKind::from_data_type(array.data_type())? {
            CanonicalKind::Boolean => downcast!(Boolean, BooleanArray),
            CanonicalKind::Int => downcast!(Int, Int32Array),
            CanonicalKind::Long => downcast!(Long, Int64Array),
            CanonicalKind::Float => downcast!(Float, Float32Array),
            CanonicalKind::Double => downcast!(Double, Float64Array),
            CanonicalKind::Decimal => downcast!(Decimal, Decimal128Array),
            CanonicalKind::Date => downcast!(Date, Date32Array),
            CanonicalKind::TimeMicros => downcast!(TimeMicros, Time64MicrosecondArray),
            CanonicalKind::TimestampMicros => {
                downcast!(TimestampMicros, TimestampMicrosecondArray)
            }
            CanonicalKind::TimestampNanos => {
                downcast!(TimestampNanos, TimestampNanosecondArray)
            }
            CanonicalKind::String => downcast!(String, StringArray),
            CanonicalKind::LargeString => downcast!(LargeString, LargeStringArray),
            CanonicalKind::Binary => downcast!(Binary, BinaryArray),
            CanonicalKind::LargeBinary => downcast!(LargeBinary, LargeBinaryArray),
            CanonicalKind::Fixed => downcast!(Fixed, FixedSizeBinaryArray),
        }
    }

    pub(crate) fn canonical_bytes(&self, row: usize) -> Option<CanonicalBytes<'_>> {
        macro_rules! fixed {
            ($array:expr, $value:expr) => {{
                let array = $array;
                (!array.is_null(row)).then(|| CanonicalBytes::Fixed($value))
            }};
        }
        match self {
            Self::Boolean(array) => fixed!(array, [u8::from(array.value(row)); 16]),
            Self::Int(array) => {
                fixed!(array, padded(array.value(row).to_le_bytes().as_slice()))
            }
            Self::Long(array) => {
                fixed!(array, padded(array.value(row).to_le_bytes().as_slice()))
            }
            Self::Float(array) => {
                fixed!(
                    array,
                    padded(array.value(row).to_bits().to_le_bytes().as_slice())
                )
            }
            Self::Double(array) => {
                fixed!(
                    array,
                    padded(array.value(row).to_bits().to_le_bytes().as_slice())
                )
            }
            Self::Decimal(array) => {
                if array.is_null(row) {
                    None
                } else {
                    let bytes = array.value(row).to_be_bytes();
                    let start = minimal_twos_complement_start(&bytes);
                    Some(CanonicalBytes::Decimal { bytes, start })
                }
            }
            Self::Date(array) => {
                fixed!(array, padded(array.value(row).to_le_bytes().as_slice()))
            }
            Self::TimeMicros(array) => {
                fixed!(array, padded(array.value(row).to_le_bytes().as_slice()))
            }
            Self::TimestampMicros(array) => {
                fixed!(array, padded(array.value(row).to_le_bytes().as_slice()))
            }
            Self::TimestampNanos(array) => {
                fixed!(array, padded(array.value(row).to_le_bytes().as_slice()))
            }
            Self::String(array) => {
                (!array.is_null(row)).then(|| CanonicalBytes::Borrowed(array.value(row).as_bytes()))
            }
            Self::LargeString(array) => {
                (!array.is_null(row)).then(|| CanonicalBytes::Borrowed(array.value(row).as_bytes()))
            }
            Self::Binary(array) => {
                (!array.is_null(row)).then(|| CanonicalBytes::Borrowed(array.value(row)))
            }
            Self::LargeBinary(array) => {
                (!array.is_null(row)).then(|| CanonicalBytes::Borrowed(array.value(row)))
            }
            Self::Fixed(array) => {
                (!array.is_null(row)).then(|| CanonicalBytes::Borrowed(array.value(row)))
            }
        }
    }
}

pub(crate) enum CanonicalBytes<'a> {
    Borrowed(&'a [u8]),
    Fixed([u8; 16]),
    Decimal { bytes: [u8; 16], start: usize },
}

impl CanonicalBytes<'_> {
    pub(crate) fn as_slice(&self, width: usize) -> &[u8] {
        match self {
            Self::Borrowed(bytes) => bytes,
            Self::Fixed(bytes) => &bytes[..width],
            Self::Decimal { bytes, start } => &bytes[*start..],
        }
    }
}

pub(crate) const fn canonical_width(batch: &PreparedCanonicalBatch<'_>) -> usize {
    match batch {
        PreparedCanonicalBatch::Boolean(_) => 1,
        PreparedCanonicalBatch::Int(_)
        | PreparedCanonicalBatch::Float(_)
        | PreparedCanonicalBatch::Date(_) => 4,
        PreparedCanonicalBatch::Long(_)
        | PreparedCanonicalBatch::Double(_)
        | PreparedCanonicalBatch::TimeMicros(_)
        | PreparedCanonicalBatch::TimestampMicros(_)
        | PreparedCanonicalBatch::TimestampNanos(_) => 8,
        PreparedCanonicalBatch::Decimal(_)
        | PreparedCanonicalBatch::String(_)
        | PreparedCanonicalBatch::LargeString(_)
        | PreparedCanonicalBatch::Binary(_)
        | PreparedCanonicalBatch::LargeBinary(_)
        | PreparedCanonicalBatch::Fixed(_) => 0,
    }
}

fn padded(bytes: &[u8]) -> [u8; 16] {
    let mut output = [0; 16];
    output[..bytes.len()].copy_from_slice(bytes);
    output
}

fn minimal_twos_complement_start(bytes: &[u8; 16]) -> usize {
    let mut start = 0;
    while start < bytes.len() - 1 {
        let current = bytes[start];
        let next = bytes[start + 1];
        if (current == 0 && next & 0x80 == 0) || (current == 0xff && next & 0x80 != 0) {
            start += 1;
        } else {
            break;
        }
    }
    start
}

#[cfg(test)]
mod tests {
    use super::minimal_twos_complement_start;

    #[test]
    fn decimal_encoding_is_minimal_signed_big_endian() {
        for (value, expected) in [
            (0_i128, &[0][..]),
            (127, &[0x7f]),
            (128, &[0, 0x80]),
            (-1, &[0xff]),
            (-128, &[0x80]),
            (-129, &[0xff, 0x7f]),
        ] {
            let bytes = value.to_be_bytes();
            assert_eq!(&bytes[minimal_twos_complement_start(&bytes)..], expected);
        }
    }
}
