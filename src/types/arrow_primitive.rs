#![allow(dead_code)]

use arrow::datatypes::{DataType, Field};

use crate::types::logical::{LogicalType, logical_type_of_field};
use crate::types::primitive::PrimitiveType;

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

pub(crate) fn arrow_field_to_primitive(field: &Field) -> Option<PrimitiveType> {
    field_logical_primitive(field).or_else(|| arrow_type_to_primitive(field.data_type()).ok())
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
    use crate::types::logical::{LogicalType, field_with_logical_type};

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
}
