use arrow::datatypes::DataType;

use crate::lower::thrift::type_lowering::scalar_type_desc;
use crate::types;

/// Convert Arrow DataType to Thrift TTypeDesc.
pub(crate) fn arrow_type_to_type_desc(data_type: &DataType) -> Result<types::TTypeDesc, String> {
    match data_type {
        DataType::Decimal128(p, s) => {
            let scalar = types::TScalarType::new(
                types::TPrimitiveType::DECIMAL128,
                None::<i32>,
                Some(i32::from(*p)),
                Some(i32::from(*s)),
            );
            Ok(types::TTypeDesc::new(vec![types::TTypeNode::new(
                types::TTypeNodeType::SCALAR,
                scalar,
                None,
                None,
            )]))
        }
        DataType::Decimal256(p, s) => {
            let scalar = types::TScalarType::new(
                types::TPrimitiveType::DECIMAL256,
                None::<i32>,
                Some(i32::from(*p)),
                Some(i32::from(*s)),
            );
            Ok(types::TTypeDesc::new(vec![types::TTypeNode::new(
                types::TTypeNodeType::SCALAR,
                scalar,
                None,
                None,
            )]))
        }
        _ => {
            let primitive = arrow_type_to_primitive(data_type)?;
            Ok(scalar_type_desc(primitive))
        }
    }
}

pub(crate) fn arrow_type_to_primitive(
    data_type: &DataType,
) -> Result<types::TPrimitiveType, String> {
    match data_type {
        DataType::Boolean => Ok(types::TPrimitiveType::BOOLEAN),
        DataType::Int8 => Ok(types::TPrimitiveType::TINYINT),
        DataType::Int16 => Ok(types::TPrimitiveType::SMALLINT),
        DataType::Int32 => Ok(types::TPrimitiveType::INT),
        DataType::Int64 => Ok(types::TPrimitiveType::BIGINT),
        DataType::Float32 => Ok(types::TPrimitiveType::FLOAT),
        DataType::Float64 => Ok(types::TPrimitiveType::DOUBLE),
        DataType::Utf8 | DataType::LargeUtf8 => Ok(types::TPrimitiveType::VARCHAR),
        DataType::Binary | DataType::LargeBinary => Ok(types::TPrimitiveType::VARBINARY),
        DataType::Date32 => Ok(types::TPrimitiveType::DATE),
        DataType::Timestamp(_, _) => Ok(types::TPrimitiveType::DATETIME),
        DataType::Decimal128(_, _) => Ok(types::TPrimitiveType::DECIMAL128),
        DataType::Decimal256(_, _) => Ok(types::TPrimitiveType::DECIMAL256),
        DataType::FixedSizeBinary(16) => Ok(types::TPrimitiveType::LARGEINT),
        DataType::Time64(_) => Ok(types::TPrimitiveType::TIME),
        DataType::Null => Ok(types::TPrimitiveType::NULL_TYPE),
        other => Err(format!(
            "ThriftPlanBuilder does not support data type {:?}",
            other
        )),
    }
}

pub(crate) use crate::sql::types::{arithmetic_result_type, wider_type};
