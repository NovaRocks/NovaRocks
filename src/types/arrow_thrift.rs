use std::sync::Arc;

use arrow::datatypes::{DataType, Field, TimeUnit};

use crate::common::decimal::{LEGACY_DECIMALV2_PRECISION, LEGACY_DECIMALV2_SCALE};
use crate::thrift::types;
use crate::types::logical::{LogicalType, field_with_logical_type};

const THRIFT_TIME_UNIT_MICROS: i32 = 2;
pub(crate) const THRIFT_TIME_UNIT_NANOS: i32 = 3;

pub(crate) fn thrift_time_unit_for_arrow(unit: TimeUnit) -> Result<Option<i32>, String> {
    match unit {
        TimeUnit::Microsecond => Ok(None),
        TimeUnit::Nanosecond => Ok(Some(THRIFT_TIME_UNIT_NANOS)),
        other => Err(format!(
            "unsupported timestamp unit {other:?} for thrift descriptor; only Microsecond/Nanosecond supported"
        )),
    }
}

pub(crate) fn thrift_type_desc_from_primitive(
    primitive: types::TPrimitiveType,
) -> types::TTypeDesc {
    types::TTypeDesc::new(vec![types::TTypeNode::new(
        types::TTypeNodeType::SCALAR,
        types::TScalarType::new(primitive, None, None, None, None),
        None,
        None,
    )])
}

pub(crate) fn thrift_desc_to_arrow_type(desc: &types::TTypeDesc) -> Option<DataType> {
    let types = desc.types.as_ref()?;
    let mut cursor = 0usize;
    thrift_nodes_to_arrow_type(types, &mut cursor)
}

pub(crate) fn thrift_desc_to_arrow_field(
    name: &str,
    nullable: bool,
    desc: &types::TTypeDesc,
) -> Option<Field> {
    let types = desc.types.as_ref()?;
    let mut cursor = 0usize;
    thrift_nodes_to_arrow_field(types, &mut cursor, name, nullable)
}

fn thrift_nodes_to_arrow_type(types: &[types::TTypeNode], cursor: &mut usize) -> Option<DataType> {
    let node = types.get(*cursor)?;
    *cursor += 1;
    match node.type_ {
        t if t == types::TTypeNodeType::SCALAR => {
            let scalar = node.scalar_type.as_ref()?;
            let data_type = match scalar.type_ {
                t if t == types::TPrimitiveType::NULL_TYPE => DataType::Null,
                t if t == types::TPrimitiveType::BOOLEAN => DataType::Boolean,
                t if t == types::TPrimitiveType::TINYINT => DataType::Int8,
                t if t == types::TPrimitiveType::SMALLINT => DataType::Int16,
                t if t == types::TPrimitiveType::INT => DataType::Int32,
                t if t == types::TPrimitiveType::BIGINT => DataType::Int64,
                t if t == types::TPrimitiveType::LARGEINT => DataType::FixedSizeBinary(16),
                t if t == types::TPrimitiveType::FLOAT => DataType::Float32,
                t if t == types::TPrimitiveType::DOUBLE => DataType::Float64,
                t if t == types::TPrimitiveType::DATE => DataType::Date32,
                t if t == types::TPrimitiveType::DATETIME => {
                    let unit = match scalar.time_unit {
                        None => TimeUnit::Microsecond,
                        Some(c) if c == THRIFT_TIME_UNIT_MICROS => TimeUnit::Microsecond,
                        Some(c) if c == THRIFT_TIME_UNIT_NANOS => TimeUnit::Nanosecond,
                        Some(_) => return None,
                    };
                    DataType::Timestamp(unit, None)
                }
                t if t == types::TPrimitiveType::TIME => DataType::Time64(TimeUnit::Microsecond),
                t if t == types::TPrimitiveType::DECIMALV2 => {
                    DataType::Decimal128(LEGACY_DECIMALV2_PRECISION, LEGACY_DECIMALV2_SCALE)
                }
                t if t == types::TPrimitiveType::DECIMAL32
                    || t == types::TPrimitiveType::DECIMAL64
                    || t == types::TPrimitiveType::DECIMAL128
                    || t == types::TPrimitiveType::DECIMAL256
                    || t == types::TPrimitiveType::DECIMAL =>
                {
                    let precision = scalar.precision.and_then(|v| u8::try_from(v).ok())?;
                    let scale = scalar.scale.and_then(|v| i8::try_from(v).ok())?;
                    if scalar.type_ == types::TPrimitiveType::DECIMAL256 || precision > 38 {
                        DataType::Decimal256(precision, scale)
                    } else {
                        DataType::Decimal128(precision, scale)
                    }
                }
                t if t == types::TPrimitiveType::BINARY
                    || t == types::TPrimitiveType::VARBINARY =>
                {
                    DataType::Binary
                }
                t if t == types::TPrimitiveType::HLL
                    || t == types::TPrimitiveType::OBJECT
                    || t == types::TPrimitiveType::PERCENTILE =>
                {
                    DataType::Binary
                }
                t if t == types::TPrimitiveType::CHAR
                    || t == types::TPrimitiveType::VARCHAR
                    || t == types::TPrimitiveType::JSON
                    || t == types::TPrimitiveType::FUNCTION =>
                {
                    DataType::Utf8
                }
                t if t == types::TPrimitiveType::VARIANT => DataType::LargeBinary,
                _ => return None,
            };
            Some(data_type)
        }
        t if t == types::TTypeNodeType::STRUCT => {
            let fields = node.struct_fields.as_ref()?;
            let mut out_fields = Vec::with_capacity(fields.len());
            for field in fields {
                let name = field.name.clone()?;
                out_fields.push(thrift_nodes_to_arrow_field(types, cursor, &name, true)?);
            }
            Some(DataType::Struct(out_fields.into()))
        }
        t if t == types::TTypeNodeType::ARRAY => {
            let item_field = Arc::new(thrift_nodes_to_arrow_field(types, cursor, "item", true)?);
            Some(DataType::List(item_field))
        }
        t if t == types::TTypeNodeType::MAP => {
            let key_field = thrift_nodes_to_arrow_field(types, cursor, "key", true)?;
            let value_field = thrift_nodes_to_arrow_field(types, cursor, "value", true)?;
            let entries = Arc::new(Field::new(
                "entries",
                DataType::Struct(vec![key_field, value_field].into()),
                false,
            ));
            Some(DataType::Map(entries, false))
        }
        _ => None,
    }
}

fn thrift_nodes_to_arrow_field(
    types: &[types::TTypeNode],
    cursor: &mut usize,
    name: &str,
    nullable: bool,
) -> Option<Field> {
    let node_start = *cursor;
    let data_type = thrift_nodes_to_arrow_type(types, cursor)?;
    let field = Field::new(name, data_type, nullable);
    Some(match logical_type_from_node(types.get(node_start)?) {
        Some(logical_type) => field_with_logical_type(field, logical_type),
        None => field,
    })
}

fn logical_type_from_node(node: &types::TTypeNode) -> Option<LogicalType> {
    if node.type_ != types::TTypeNodeType::SCALAR {
        return None;
    }
    match node.scalar_type.as_ref()?.type_ {
        t if t == types::TPrimitiveType::JSON => Some(LogicalType::Json),
        t if t == types::TPrimitiveType::HLL => Some(LogicalType::Hll),
        t if t == types::TPrimitiveType::OBJECT => Some(LogicalType::Object),
        t if t == types::TPrimitiveType::PERCENTILE => Some(LogicalType::Percentile),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::*;
    use crate::types::logical::logical_type_of_field;

    #[test]
    fn thrift_desc_to_arrow_field_tags_json_metadata() {
        let desc = scalar_desc(types::TPrimitiveType::JSON, None, None);

        let field =
            thrift_desc_to_arrow_field("payload", true, &desc).expect("json thrift desc lowers");

        assert_eq!(field.data_type(), &DataType::Utf8);
        assert_eq!(logical_type_of_field(&field), Some(LogicalType::Json));
    }

    #[test]
    fn thrift_desc_to_arrow_type_routes_wide_decimal_to_decimal256() {
        let desc = scalar_desc(types::TPrimitiveType::DECIMAL128, Some(40), Some(8));

        assert_eq!(
            thrift_desc_to_arrow_type(&desc),
            Some(DataType::Decimal256(40, 8))
        );
    }

    fn scalar_desc(
        primitive: types::TPrimitiveType,
        precision: Option<i32>,
        scale: Option<i32>,
    ) -> types::TTypeDesc {
        types::TTypeDesc::new(vec![types::TTypeNode::new(
            types::TTypeNodeType::SCALAR,
            types::TScalarType::new(primitive, None, precision, scale, None),
            None,
            None,
        )])
    }
}
