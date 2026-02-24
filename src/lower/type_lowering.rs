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
use crate::types;
use arrow::datatypes::{DataType, Field, TimeUnit};
use std::collections::HashMap;
use std::sync::Arc;

pub(crate) const FIELD_META_PRIMITIVE_TYPE: &str = "novarocks.primitive_type";
pub(crate) const FIELD_META_PRIMITIVE_JSON: &str = "JSON";

/// Extract primitive type from TExprNode.
pub(crate) fn primitive_type_from_node(
    node: &crate::exprs::TExprNode,
) -> Option<types::TPrimitiveType> {
    let types = node.type_.types.as_ref()?;
    let first = types.first()?;
    if first.type_ != types::TTypeNodeType::SCALAR {
        return None;
    }
    let scalar = first.scalar_type.as_ref()?;
    Some(scalar.type_)
}

pub(crate) fn primitive_type_from_desc(desc: &types::TTypeDesc) -> Option<types::TPrimitiveType> {
    let types = desc.types.as_ref()?;
    let first = types.first()?;
    if first.type_ != types::TTypeNodeType::SCALAR {
        return None;
    }
    let scalar = first.scalar_type.as_ref()?;
    Some(scalar.type_)
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
        t if t == types::TPrimitiveType::LARGEINT => DataType::Decimal128(38, 0),
        t if t == types::TPrimitiveType::FLOAT => DataType::Float32,
        t if t == types::TPrimitiveType::DOUBLE => DataType::Float64,
        t if t == types::TPrimitiveType::DATE => DataType::Date32,
        t if t == types::TPrimitiveType::DATETIME || t == types::TPrimitiveType::TIME => {
            DataType::Timestamp(TimeUnit::Microsecond, None)
        }
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
        // Decimal requires precision/scale from TTypeDesc; without that metadata we cannot build a
        // correct Arrow decimal type.
        _ => return None,
    };
    Some(data_type)
}

/// Convert TTypeDesc to Arrow DataType.
pub(crate) fn arrow_type_from_desc(desc: &types::TTypeDesc) -> Option<DataType> {
    let types = desc.types.as_ref()?;
    let mut cursor = 0usize;
    arrow_type_from_nodes(types, &mut cursor)
}

/// Convert TTypeNode array to Arrow DataType.
/// This function recursively processes the type nodes, handling SCALAR/STRUCT/ARRAY/MAP types.
pub(crate) fn arrow_type_from_nodes(
    types: &[types::TTypeNode],
    cursor: &mut usize,
) -> Option<DataType> {
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
                t if t == types::TPrimitiveType::DATETIME || t == types::TPrimitiveType::TIME => {
                    DataType::Timestamp(TimeUnit::Microsecond, None)
                }
                t if t == types::TPrimitiveType::DECIMAL32
                    || t == types::TPrimitiveType::DECIMAL64
                    || t == types::TPrimitiveType::DECIMAL128
                    || t == types::TPrimitiveType::DECIMAL256
                    || t == types::TPrimitiveType::DECIMAL
                    || t == types::TPrimitiveType::DECIMALV2 =>
                {
                    let precision = scalar.precision.and_then(|v| u8::try_from(v).ok())?;
                    let scale = scalar.scale.and_then(|v| i8::try_from(v).ok())?;
                    // FE may annotate wide decimal targets with DECIMAL128 primitive while still
                    // carrying precision > 38 in the descriptor. Route by precision to avoid
                    // constructing invalid Decimal128(>38) and silently nulling casts.
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
                out_fields.push(arrow_field_from_nodes_with_name(
                    types, cursor, &name, true,
                )?);
            }
            Some(DataType::Struct(out_fields.into()))
        }
        t if t == types::TTypeNodeType::ARRAY => {
            let item_field = Arc::new(arrow_field_from_nodes_with_name(
                types, cursor, "item", true,
            )?);
            Some(DataType::List(item_field))
        }
        t if t == types::TTypeNodeType::MAP => {
            let key_field = arrow_field_from_nodes_with_name(types, cursor, "key", true)?;
            let value_field = arrow_field_from_nodes_with_name(types, cursor, "value", true)?;
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

fn arrow_field_from_nodes_with_name(
    types: &[types::TTypeNode],
    cursor: &mut usize,
    name: &str,
    nullable: bool,
) -> Option<Field> {
    let child_node = types.get(*cursor)?;
    let data_type = arrow_type_from_nodes(types, cursor)?;
    let mut field = Field::new(name, data_type, nullable);
    if let Some(metadata) = primitive_field_metadata(child_node) {
        field = field.with_metadata(metadata);
    }
    Some(field)
}

fn primitive_field_metadata(node: &types::TTypeNode) -> Option<HashMap<String, String>> {
    if node.type_ != types::TTypeNodeType::SCALAR {
        return None;
    }
    let scalar = node.scalar_type.as_ref()?;
    if scalar.type_ != types::TPrimitiveType::JSON {
        return None;
    }
    let mut metadata = HashMap::new();
    metadata.insert(
        FIELD_META_PRIMITIVE_TYPE.to_string(),
        FIELD_META_PRIMITIVE_JSON.to_string(),
    );
    Some(metadata)
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
    use super::arrow_type_from_primitive;
    use crate::types::TPrimitiveType;
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
}
