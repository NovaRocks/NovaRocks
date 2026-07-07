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
use crate::common::decimal::{LEGACY_DECIMALV2_PRECISION, LEGACY_DECIMALV2_SCALE};
use crate::common::util::FieldRenderSchema;
use crate::thrift::types;
use crate::types::PrimitiveType;
pub(crate) use crate::types::arrow_thrift::{THRIFT_TIME_UNIT_NANOS, thrift_time_unit_for_arrow};
use crate::types::arrow_thrift::{
    thrift_desc_to_arrow_field, thrift_desc_to_arrow_type, thrift_type_desc_from_primitive,
};
use arrow::datatypes::{DataType, Field, TimeUnit};

/// Extract primitive type from TExprNode.
pub(crate) fn primitive_type_from_node(
    node: &crate::thrift::exprs::TExprNode,
) -> Option<types::TPrimitiveType> {
    primitive_type_from_desc(&node.type_)
}

pub(crate) fn primitive_type_from_desc(desc: &types::TTypeDesc) -> Option<types::TPrimitiveType> {
    let nodes = desc.types.as_ref()?;
    let first = nodes.first()?;
    if first.type_ != types::TTypeNodeType::SCALAR {
        return None;
    }
    let scalar = first.scalar_type.as_ref()?;
    Some(scalar.type_)
}

pub(crate) fn native_primitive_from_thrift(
    primitive: types::TPrimitiveType,
) -> Option<PrimitiveType> {
    let native = match primitive {
        t if t == types::TPrimitiveType::INVALID_TYPE => PrimitiveType::Invalid,
        t if t == types::TPrimitiveType::NULL_TYPE => PrimitiveType::Null,
        t if t == types::TPrimitiveType::BOOLEAN => PrimitiveType::Boolean,
        t if t == types::TPrimitiveType::TINYINT => PrimitiveType::TinyInt,
        t if t == types::TPrimitiveType::SMALLINT => PrimitiveType::SmallInt,
        t if t == types::TPrimitiveType::INT => PrimitiveType::Int,
        t if t == types::TPrimitiveType::BIGINT => PrimitiveType::BigInt,
        t if t == types::TPrimitiveType::LARGEINT => PrimitiveType::LargeInt,
        t if t == types::TPrimitiveType::INT256 => PrimitiveType::Int256,
        t if t == types::TPrimitiveType::FLOAT => PrimitiveType::Float,
        t if t == types::TPrimitiveType::DOUBLE => PrimitiveType::Double,
        t if t == types::TPrimitiveType::DATE => PrimitiveType::Date,
        t if t == types::TPrimitiveType::DATETIME => PrimitiveType::DateTime,
        t if t == types::TPrimitiveType::TIME => PrimitiveType::Time,
        t if t == types::TPrimitiveType::DECIMAL => PrimitiveType::Decimal,
        t if t == types::TPrimitiveType::DECIMALV2 => PrimitiveType::DecimalV2,
        t if t == types::TPrimitiveType::DECIMAL32 => PrimitiveType::Decimal32,
        t if t == types::TPrimitiveType::DECIMAL64 => PrimitiveType::Decimal64,
        t if t == types::TPrimitiveType::DECIMAL128 => PrimitiveType::Decimal128,
        t if t == types::TPrimitiveType::DECIMAL256 => PrimitiveType::Decimal256,
        t if t == types::TPrimitiveType::CHAR => PrimitiveType::Char,
        t if t == types::TPrimitiveType::VARCHAR => PrimitiveType::Varchar,
        t if t == types::TPrimitiveType::BINARY => PrimitiveType::Binary,
        t if t == types::TPrimitiveType::VARBINARY => PrimitiveType::Varbinary,
        t if t == types::TPrimitiveType::JSON => PrimitiveType::Json,
        t if t == types::TPrimitiveType::HLL => PrimitiveType::Hll,
        t if t == types::TPrimitiveType::OBJECT => PrimitiveType::Object,
        t if t == types::TPrimitiveType::PERCENTILE => PrimitiveType::Percentile,
        t if t == types::TPrimitiveType::FUNCTION => PrimitiveType::Function,
        t if t == types::TPrimitiveType::VARIANT => PrimitiveType::Variant,
        _ => return None,
    };
    Some(native)
}

pub(crate) fn thrift_primitive_from_native(primitive: PrimitiveType) -> types::TPrimitiveType {
    match primitive {
        PrimitiveType::Invalid => types::TPrimitiveType::INVALID_TYPE,
        PrimitiveType::Null => types::TPrimitiveType::NULL_TYPE,
        PrimitiveType::Boolean => types::TPrimitiveType::BOOLEAN,
        PrimitiveType::TinyInt => types::TPrimitiveType::TINYINT,
        PrimitiveType::SmallInt => types::TPrimitiveType::SMALLINT,
        PrimitiveType::Int => types::TPrimitiveType::INT,
        PrimitiveType::BigInt => types::TPrimitiveType::BIGINT,
        PrimitiveType::LargeInt => types::TPrimitiveType::LARGEINT,
        PrimitiveType::Int256 => types::TPrimitiveType::INT256,
        PrimitiveType::Float => types::TPrimitiveType::FLOAT,
        PrimitiveType::Double => types::TPrimitiveType::DOUBLE,
        PrimitiveType::Date => types::TPrimitiveType::DATE,
        PrimitiveType::DateTime => types::TPrimitiveType::DATETIME,
        PrimitiveType::Time => types::TPrimitiveType::TIME,
        PrimitiveType::Decimal => types::TPrimitiveType::DECIMAL,
        PrimitiveType::DecimalV2 => types::TPrimitiveType::DECIMALV2,
        PrimitiveType::Decimal32 => types::TPrimitiveType::DECIMAL32,
        PrimitiveType::Decimal64 => types::TPrimitiveType::DECIMAL64,
        PrimitiveType::Decimal128 => types::TPrimitiveType::DECIMAL128,
        PrimitiveType::Decimal256 => types::TPrimitiveType::DECIMAL256,
        PrimitiveType::Char => types::TPrimitiveType::CHAR,
        PrimitiveType::Varchar => types::TPrimitiveType::VARCHAR,
        PrimitiveType::Binary => types::TPrimitiveType::BINARY,
        PrimitiveType::Varbinary => types::TPrimitiveType::VARBINARY,
        PrimitiveType::Json => types::TPrimitiveType::JSON,
        PrimitiveType::Hll => types::TPrimitiveType::HLL,
        PrimitiveType::Object => types::TPrimitiveType::OBJECT,
        PrimitiveType::Percentile => types::TPrimitiveType::PERCENTILE,
        PrimitiveType::Function => types::TPrimitiveType::FUNCTION,
        PrimitiveType::Variant => types::TPrimitiveType::VARIANT,
    }
}

pub(crate) fn native_primitive_type_from_desc(desc: &types::TTypeDesc) -> Option<PrimitiveType> {
    primitive_type_from_desc(desc).and_then(native_primitive_from_thrift)
}

pub(crate) fn render_schema_from_type_desc(
    desc: &types::TTypeDesc,
) -> Result<FieldRenderSchema, String> {
    let nodes = desc
        .types
        .as_ref()
        .ok_or_else(|| "render field type desc missing nodes".to_string())?;
    let (schema, next) = render_schema_from_desc_nodes(nodes, 0)?;
    if next != nodes.len() {
        return Err(format!(
            "render field type desc has trailing nodes: consumed={} total={}",
            next,
            nodes.len()
        ));
    }
    Ok(schema)
}

fn render_schema_from_desc_nodes(
    nodes: &[types::TTypeNode],
    start: usize,
) -> Result<(FieldRenderSchema, usize), String> {
    let node = nodes
        .get(start)
        .ok_or_else(|| format!("render field type desc ended unexpectedly at node {start}"))?;

    match node.type_ {
        t if t == types::TTypeNodeType::SCALAR => {
            let primitive = node
                .scalar_type
                .as_ref()
                .and_then(|scalar| native_primitive_from_thrift(scalar.type_));
            Ok((FieldRenderSchema::scalar(primitive), start + 1))
        }
        t if t == types::TTypeNodeType::STRUCT => {
            let struct_fields = node
                .struct_fields
                .as_ref()
                .ok_or_else(|| "render struct type desc missing struct_fields".to_string())?;
            let mut cursor = start + 1;
            let mut children = Vec::with_capacity(struct_fields.len());
            for _ in struct_fields {
                let (child, next) = render_schema_from_desc_nodes(nodes, cursor)?;
                cursor = next;
                children.push(child);
            }
            Ok((FieldRenderSchema::complex(children), cursor))
        }
        t if t == types::TTypeNodeType::ARRAY => {
            let (item, next) = render_schema_from_desc_nodes(nodes, start + 1)?;
            Ok((FieldRenderSchema::complex(vec![item]), next))
        }
        t if t == types::TTypeNodeType::MAP => {
            let (key, next) = render_schema_from_desc_nodes(nodes, start + 1)?;
            let (value, next) = render_schema_from_desc_nodes(nodes, next)?;
            Ok((FieldRenderSchema::complex(vec![key, value]), next))
        }
        other => Err(format!("unsupported render type desc node {:?}", other)),
    }
}

pub(crate) fn scalar_type_desc(primitive: types::TPrimitiveType) -> types::TTypeDesc {
    thrift_type_desc_from_primitive(primitive)
}

/// Convert TPrimitiveType to Arrow DataType when precision/scale is not required.
///
/// This is mainly used by expression fields like `TExprNode.child_type` where FE already decides
/// a comparable type for both children, and BE executes comparison with that single logical type.
pub(crate) fn arrow_type_from_primitive(primitive: types::TPrimitiveType) -> Option<DataType> {
    let data_type = match primitive {
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
            DataType::Timestamp(TimeUnit::Microsecond, None)
        }
        t if t == types::TPrimitiveType::TIME => DataType::Time64(TimeUnit::Microsecond),
        t if t == types::TPrimitiveType::BINARY || t == types::TPrimitiveType::VARBINARY => {
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
        t if t == types::TPrimitiveType::DECIMALV2 => {
            DataType::Decimal128(LEGACY_DECIMALV2_PRECISION, LEGACY_DECIMALV2_SCALE)
        }
        // Decimal requires precision/scale from TTypeDesc; without that metadata we cannot build a
        // correct Arrow decimal type, except for legacy DECIMALV2 which has a fixed BE shape.
        _ => return None,
    };
    Some(data_type)
}

/// Convert TTypeDesc to Arrow DataType.
pub(crate) fn arrow_type_from_desc(desc: &types::TTypeDesc) -> Option<DataType> {
    thrift_desc_to_arrow_type(desc)
}

pub(crate) fn arrow_field_from_desc(
    name: &str,
    nullable: bool,
    desc: &types::TTypeDesc,
) -> Option<Field> {
    thrift_desc_to_arrow_field(name, nullable, desc)
}

// Keeping `decimal_params_from_desc` for potential future use when we need
// explicit decimal precision/scale, but suppress dead_code warning for now.
#[allow(dead_code)]
pub(crate) fn decimal_params_from_desc(desc: &types::TTypeDesc) -> Option<(u8, i8)> {
    let types = desc.types.as_ref()?;
    let first = types.first()?;
    if first.type_ != types::TTypeNodeType::SCALAR {
        return None;
    }
    let scalar = first.scalar_type.as_ref()?;
    let precision = scalar.precision.and_then(|v| u8::try_from(v).ok())?;
    let scale = scalar.scale.and_then(|v| i8::try_from(v).ok())?;
    Some((precision, scale))
}

#[cfg(test)]
mod tests {
    use super::{arrow_type_from_desc, arrow_type_from_primitive};
    use crate::thrift::types::TPrimitiveType;
    use crate::thrift::types::{TScalarType, TTypeDesc, TTypeNode, TTypeNodeType};
    use arrow::datatypes::DataType;

    #[test]
    fn object_family_primitives_lower_to_binary() {
        assert_eq!(
            arrow_type_from_primitive(TPrimitiveType::HLL),
            Some(DataType::Binary)
        );
        assert_eq!(
            arrow_type_from_primitive(TPrimitiveType::OBJECT),
            Some(DataType::Binary)
        );
        assert_eq!(
            arrow_type_from_primitive(TPrimitiveType::PERCENTILE),
            Some(DataType::Binary)
        );
    }

    #[test]
    fn largeint_primitive_lowers_to_fixed_size_binary() {
        assert_eq!(
            arrow_type_from_primitive(TPrimitiveType::LARGEINT),
            Some(DataType::FixedSizeBinary(16))
        );
    }

    #[test]
    fn decimalv2_primitive_lowers_to_legacy_decimal128() {
        assert_eq!(
            arrow_type_from_primitive(TPrimitiveType::DECIMALV2),
            Some(DataType::Decimal128(27, 9))
        );
    }

    #[test]
    fn decimalv2_desc_ignores_fe_default_precision_scale() {
        let desc = TTypeDesc {
            types: Some(vec![TTypeNode {
                type_: TTypeNodeType::SCALAR,
                scalar_type: Some(TScalarType {
                    type_: TPrimitiveType::DECIMALV2,
                    len: None,
                    precision: Some(9),
                    scale: Some(0),
                    time_unit: None,
                }),
                is_named: None,
                struct_fields: None,
            }]),
        };

        assert_eq!(
            arrow_type_from_desc(&desc),
            Some(DataType::Decimal128(27, 9))
        );
    }

    #[test]
    fn datetime_desc_without_time_unit_defaults_to_microsecond() {
        use arrow::datatypes::TimeUnit;
        // An FE-style descriptor never sets time_unit; it must stay microsecond.
        let desc = TTypeDesc {
            types: Some(vec![TTypeNode {
                type_: TTypeNodeType::SCALAR,
                scalar_type: Some(TScalarType {
                    type_: TPrimitiveType::DATETIME,
                    len: None,
                    precision: None,
                    scale: None,
                    time_unit: None,
                }),
                is_named: None,
                struct_fields: None,
            }]),
        };
        assert_eq!(
            arrow_type_from_desc(&desc),
            Some(DataType::Timestamp(TimeUnit::Microsecond, None))
        );
    }
}
