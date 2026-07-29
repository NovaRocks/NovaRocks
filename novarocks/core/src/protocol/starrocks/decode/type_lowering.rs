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
use crate::common::util::FieldRenderSchema;
use crate::protocol::starrocks::type_mapping::{
    thrift_desc_to_arrow_field, thrift_desc_to_arrow_type, thrift_type_desc_from_primitive,
};
use crate::thrift::types;
use arrow::datatypes::{DataType, Field};
use novarocks_types::PrimitiveType;
use novarocks_types::arrow_primitive::primitive_to_arrow_type;

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
    native_primitive_from_thrift(primitive).and_then(primitive_to_arrow_type)
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
