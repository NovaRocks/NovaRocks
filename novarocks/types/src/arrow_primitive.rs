#![allow(dead_code)]
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

use arrow::datatypes::{DataType, Field, TimeUnit};

use crate::decimal::{LEGACY_DECIMALV2_PRECISION, LEGACY_DECIMALV2_SCALE};
use crate::largeint;
use crate::logical::{LogicalType, logical_type_of_field};
use crate::primitive::PrimitiveType;

pub(crate) fn logical_type_to_primitive(logical_type: LogicalType) -> PrimitiveType {
    match logical_type {
        LogicalType::Json => PrimitiveType::Json,
        LogicalType::Hll => PrimitiveType::Hll,
        LogicalType::Bitmap | LogicalType::Object => PrimitiveType::Object,
        LogicalType::Percentile => PrimitiveType::Percentile,
    }
}

pub(crate) fn field_logical_primitive(field: &Field) -> Option<PrimitiveType> {
    logical_type_of_field(field).map(logical_type_to_primitive)
}

pub fn arrow_field_to_primitive(field: &Field) -> Option<PrimitiveType> {
    field_logical_primitive(field).or_else(|| arrow_type_to_primitive(field.data_type()).ok())
}

/// Returns the Arrow storage type when `primitive` carries enough type detail.
pub fn primitive_to_arrow_type(primitive: PrimitiveType) -> Option<DataType> {
    let data_type = match primitive {
        PrimitiveType::Null => DataType::Null,
        PrimitiveType::Boolean => DataType::Boolean,
        PrimitiveType::TinyInt => DataType::Int8,
        PrimitiveType::SmallInt => DataType::Int16,
        PrimitiveType::Int => DataType::Int32,
        PrimitiveType::BigInt => DataType::Int64,
        PrimitiveType::LargeInt => DataType::FixedSizeBinary(largeint::LARGEINT_BYTE_WIDTH),
        PrimitiveType::Float => DataType::Float32,
        PrimitiveType::Double => DataType::Float64,
        PrimitiveType::Date => DataType::Date32,
        PrimitiveType::DateTime => DataType::Timestamp(TimeUnit::Microsecond, None),
        PrimitiveType::Time => DataType::Time64(TimeUnit::Microsecond),
        PrimitiveType::Binary | PrimitiveType::Varbinary => DataType::Binary,
        PrimitiveType::Hll | PrimitiveType::Object | PrimitiveType::Percentile => DataType::Binary,
        PrimitiveType::Char
        | PrimitiveType::Varchar
        | PrimitiveType::Json
        | PrimitiveType::Function => DataType::Utf8,
        PrimitiveType::Variant => DataType::LargeBinary,
        PrimitiveType::DecimalV2 => {
            DataType::Decimal128(LEGACY_DECIMALV2_PRECISION, LEGACY_DECIMALV2_SCALE)
        }
        PrimitiveType::Decimal
        | PrimitiveType::Decimal32
        | PrimitiveType::Decimal64
        | PrimitiveType::Decimal128
        | PrimitiveType::Decimal256
        | PrimitiveType::Int256
        | PrimitiveType::Invalid => return None,
    };
    Some(data_type)
}

pub(crate) fn arrow_type_to_primitive(data_type: &DataType) -> Result<PrimitiveType, String> {
    match data_type {
        DataType::Null => Ok(PrimitiveType::Null),
        DataType::Boolean => Ok(PrimitiveType::Boolean),
        DataType::Int8 => Ok(PrimitiveType::TinyInt),
        DataType::Int16 => Ok(PrimitiveType::SmallInt),
        DataType::Int32 => Ok(PrimitiveType::Int),
        DataType::Int64 => Ok(PrimitiveType::BigInt),
        DataType::Float32 => Ok(PrimitiveType::Float),
        DataType::Float64 => Ok(PrimitiveType::Double),
        DataType::Utf8 | DataType::LargeUtf8 => Ok(PrimitiveType::Varchar),
        DataType::Binary => Ok(PrimitiveType::Varbinary),
        DataType::LargeBinary => Ok(PrimitiveType::Variant),
        DataType::Date32 => Ok(PrimitiveType::Date),
        DataType::Timestamp(_, _) => Ok(PrimitiveType::DateTime),
        DataType::Decimal128(_, _) => Ok(PrimitiveType::Decimal128),
        DataType::Decimal256(_, _) => Ok(PrimitiveType::Decimal256),
        DataType::FixedSizeBinary(16) => Ok(PrimitiveType::LargeInt),
        DataType::Time64(_) => Ok(PrimitiveType::Time),
        other => Err(format!(
            "Arrow-to-native primitive conversion does not support data type {other:?}"
        )),
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field};

    use super::*;
    use crate::logical::{LogicalType, field_with_logical_type};

    #[test]
    fn arrow_field_to_primitive_honors_json_metadata() {
        let field = field_with_logical_type(
            Field::new("payload", DataType::Utf8, true),
            LogicalType::Json,
        );

        assert_eq!(arrow_field_to_primitive(&field), Some(PrimitiveType::Json));
    }

    #[test]
    fn arrow_field_to_primitive_falls_back_to_storage_type() {
        let field = Field::new("plain", DataType::Utf8, true);

        assert_eq!(
            arrow_field_to_primitive(&field),
            Some(PrimitiveType::Varchar)
        );
    }

    #[test]
    fn primitive_to_arrow_type_preserves_the_complete_legacy_mapping() {
        let cases = [
            (PrimitiveType::Invalid, None),
            (PrimitiveType::Null, Some(DataType::Null)),
            (PrimitiveType::Boolean, Some(DataType::Boolean)),
            (PrimitiveType::TinyInt, Some(DataType::Int8)),
            (PrimitiveType::SmallInt, Some(DataType::Int16)),
            (PrimitiveType::Int, Some(DataType::Int32)),
            (PrimitiveType::BigInt, Some(DataType::Int64)),
            (PrimitiveType::LargeInt, Some(DataType::FixedSizeBinary(16))),
            (PrimitiveType::Int256, None),
            (PrimitiveType::Float, Some(DataType::Float32)),
            (PrimitiveType::Double, Some(DataType::Float64)),
            (PrimitiveType::Date, Some(DataType::Date32)),
            (
                PrimitiveType::DateTime,
                Some(DataType::Timestamp(TimeUnit::Microsecond, None)),
            ),
            (
                PrimitiveType::Time,
                Some(DataType::Time64(TimeUnit::Microsecond)),
            ),
            (PrimitiveType::Decimal, None),
            (PrimitiveType::DecimalV2, Some(DataType::Decimal128(27, 9))),
            (PrimitiveType::Decimal32, None),
            (PrimitiveType::Decimal64, None),
            (PrimitiveType::Decimal128, None),
            (PrimitiveType::Decimal256, None),
            (PrimitiveType::Char, Some(DataType::Utf8)),
            (PrimitiveType::Varchar, Some(DataType::Utf8)),
            (PrimitiveType::Binary, Some(DataType::Binary)),
            (PrimitiveType::Varbinary, Some(DataType::Binary)),
            (PrimitiveType::Json, Some(DataType::Utf8)),
            (PrimitiveType::Hll, Some(DataType::Binary)),
            (PrimitiveType::Object, Some(DataType::Binary)),
            (PrimitiveType::Percentile, Some(DataType::Binary)),
            (PrimitiveType::Function, Some(DataType::Utf8)),
            (PrimitiveType::Variant, Some(DataType::LargeBinary)),
        ];

        for (primitive, expected) in cases {
            assert_eq!(
                primitive_to_arrow_type(primitive),
                expected,
                "{primitive:?}"
            );
        }
    }
}
