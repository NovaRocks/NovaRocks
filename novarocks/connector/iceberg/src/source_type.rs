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

use crate::iceberg::spec::{PrimitiveType, Type};
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorOperationControl, IcebergSourcePrimitive,
    IcebergSourceType, IcebergSourceTypeNode, ProviderSourceType,
};

pub(crate) fn project_source_type(
    source: &Type,
    control: &dyn ConnectorOperationControl,
    mut reserve: impl FnMut(usize) -> Result<(), ConnectorError>,
) -> Result<ProviderSourceType, ConnectorError> {
    let mut nodes = Vec::new();
    append_nodes(source, control, &mut reserve, &mut nodes)?;
    Ok(ProviderSourceType::Iceberg(IcebergSourceType::try_new(
        nodes,
    )?))
}

fn append_nodes(
    source: &Type,
    control: &dyn ConnectorOperationControl,
    reserve: &mut impl FnMut(usize) -> Result<(), ConnectorError>,
    nodes: &mut Vec<IcebergSourceTypeNode>,
) -> Result<(), ConnectorError> {
    use IcebergSourcePrimitive as Primitive;
    use IcebergSourceTypeNode as Node;

    control.check_active()?;
    reserve(std::mem::size_of::<Node>())?;
    match source {
        Type::Primitive(primitive) => nodes.push(Node::Primitive(project_primitive(primitive))),
        Type::Variant(_) => nodes.push(Node::Primitive(Primitive::Variant)),
        Type::Struct(structure) => {
            let fields = structure.fields();
            let field_count = u32::try_from(fields.len()).map_err(|_| {
                ConnectorError::new(
                    ConnectorErrorKind::Unsupported,
                    "Iceberg source struct field count exceeds its identity representation",
                )
            })?;
            nodes.push(Node::Struct { field_count });
            for field in fields {
                reserve(std::mem::size_of::<Node>())?;
                reserve(field.name.len())?;
                nodes.push(Node::Field {
                    id: field.id,
                    name: field.name.as_str().into(),
                    required: field.required,
                });
                append_nodes(&field.field_type, control, reserve, nodes)?;
            }
        }
        Type::List(list) => {
            let element = &list.element_field;
            nodes.push(Node::List {
                element_id: element.id,
                element_required: element.required,
            });
            append_nodes(&element.field_type, control, reserve, nodes)?;
        }
        Type::Map(map) => {
            nodes.push(Node::Map {
                key_id: map.key_field.id,
                value_id: map.value_field.id,
                value_required: map.value_field.required,
            });
            append_nodes(&map.key_field.field_type, control, reserve, nodes)?;
            append_nodes(&map.value_field.field_type, control, reserve, nodes)?;
        }
    }
    Ok(())
}

fn project_primitive(primitive: &PrimitiveType) -> IcebergSourcePrimitive {
    use IcebergSourcePrimitive as Source;
    use PrimitiveType as Iceberg;

    match *primitive {
        Iceberg::Boolean => Source::Boolean,
        Iceberg::Int => Source::Int,
        Iceberg::Long => Source::Long,
        Iceberg::Float => Source::Float,
        Iceberg::Double => Source::Double,
        Iceberg::Decimal { precision, scale } => Source::Decimal { precision, scale },
        Iceberg::Date => Source::Date,
        Iceberg::Time => Source::Time,
        Iceberg::Timestamp => Source::Timestamp,
        Iceberg::Timestamptz => Source::Timestamptz,
        Iceberg::TimestampNs => Source::TimestampNs,
        Iceberg::TimestamptzNs => Source::TimestamptzNs,
        Iceberg::String => Source::String,
        Iceberg::Uuid => Source::Uuid,
        Iceberg::Fixed(width) => Source::Fixed(width),
        Iceberg::Binary => Source::Binary,
    }
}
