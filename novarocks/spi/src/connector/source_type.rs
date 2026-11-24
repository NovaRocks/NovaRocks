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

//! Structural source type identity owned by the observing provider.

use super::{ConnectorError, ConnectorErrorKind};
use novarocks_type_contract::CanonicalFactWriter;
use sha2::{Digest, Sha256};
use std::fmt;
use std::sync::Arc;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProviderSourceType {
    Iceberg(IcebergSourceType),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IcebergSourceType {
    nodes: Arc<[IcebergSourceTypeNode]>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IcebergSourcePrimitive {
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Decimal { precision: u32, scale: u32 },
    Date,
    Time,
    Timestamp,
    Timestamptz,
    TimestampNs,
    TimestamptzNs,
    String,
    Uuid,
    Fixed(u64),
    Binary,
    Variant,
}

/// A preorder node. Struct children are field nodes followed by their type.
/// List and map children are type nodes, with key preceding value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IcebergSourceTypeNode {
    Primitive(IcebergSourcePrimitive),
    Struct {
        field_count: u32,
    },
    Field {
        id: i32,
        name: Arc<str>,
        required: bool,
    },
    List {
        element_id: i32,
        element_required: bool,
    },
    Map {
        key_id: i32,
        value_id: i32,
        value_required: bool,
    },
}

#[derive(Clone, Copy)]
enum ExpectedNode {
    Type,
    Field,
}

impl IcebergSourceType {
    pub const FORMAT_VERSION: u32 = 1;

    pub fn try_new(nodes: Vec<IcebergSourceTypeNode>) -> Result<Self, ConnectorError> {
        use IcebergSourceTypeNode as Node;
        let mut pending = vec![(ExpectedNode::Type, 1)];
        for node in &nodes {
            let (expected, count) = pending.pop().ok_or_else(invalid_structure)?;
            if count > 1 {
                pending.push((expected, count - 1));
            }
            match (expected, node) {
                (ExpectedNode::Field, Node::Field { .. })
                | (ExpectedNode::Type, Node::List { .. }) => {
                    pending.push((ExpectedNode::Type, 1));
                }
                (ExpectedNode::Type, Node::Map { .. }) => {
                    pending.push((ExpectedNode::Type, 2));
                }
                (ExpectedNode::Type, Node::Struct { field_count }) => {
                    if *field_count != 0 {
                        pending.push((ExpectedNode::Field, *field_count));
                    }
                }
                (ExpectedNode::Type, Node::Primitive(_)) => {}
                _ => return Err(invalid_structure()),
            }
        }
        if !pending.is_empty() {
            return Err(invalid_structure());
        }
        Ok(Self {
            nodes: nodes.into(),
        })
    }

    pub fn nodes(&self) -> &[IcebergSourceTypeNode] {
        &self.nodes
    }
}

fn invalid_structure() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::CorruptData,
        "Iceberg source type is not a complete preorder type tree",
    )
}

impl ProviderSourceType {
    pub fn canonical_bytes(&self) -> Vec<u8> {
        match self {
            Self::Iceberg(source) => {
                let mut writer = CanonicalFactWriter::new(
                    "novarocks.iceberg.source-type",
                    IcebergSourceType::FORMAT_VERSION,
                );
                writer.sequence(source.nodes.len());
                for node in source.nodes() {
                    write_node(&mut writer, node);
                }
                writer.finish()
            }
        }
    }

    pub fn sub_digest(&self) -> [u8; 32] {
        Sha256::digest(self.canonical_bytes()).into()
    }

    pub fn retained_bytes(&self) -> usize {
        match self {
            Self::Iceberg(source) => {
                source
                    .nodes()
                    .iter()
                    .fold(
                        std::mem::size_of_val(source.nodes()),
                        |bytes, node| match node {
                            IcebergSourceTypeNode::Field { name, .. } => {
                                bytes.saturating_add(name.len())
                            }
                            _ => bytes,
                        },
                    )
            }
        }
    }

    /// Compare across tables without assigning meaning to their separate ID spaces.
    pub fn shape(&self) -> ProviderSourceTypeShape<'_> {
        ProviderSourceTypeShape(self)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ProviderSourceTypeShape<'a>(&'a ProviderSourceType);

impl PartialEq for ProviderSourceTypeShape<'_> {
    fn eq(&self, other: &Self) -> bool {
        match (self.0, other.0) {
            (ProviderSourceType::Iceberg(left), ProviderSourceType::Iceberg(right)) => {
                left.nodes.len() == right.nodes.len()
                    && left
                        .nodes
                        .iter()
                        .zip(right.nodes.iter())
                        .all(|(left, right)| {
                            use IcebergSourceTypeNode as Node;
                            match (left, right) {
                                (
                                    Node::Field {
                                        name: left_name,
                                        required: left_required,
                                        ..
                                    },
                                    Node::Field {
                                        name: right_name,
                                        required: right_required,
                                        ..
                                    },
                                ) => left_name == right_name && left_required == right_required,
                                (
                                    Node::List {
                                        element_required: left,
                                        ..
                                    },
                                    Node::List {
                                        element_required: right,
                                        ..
                                    },
                                )
                                | (
                                    Node::Map {
                                        value_required: left,
                                        ..
                                    },
                                    Node::Map {
                                        value_required: right,
                                        ..
                                    },
                                ) => left == right,
                                _ => left == right,
                            }
                        })
            }
        }
    }
}

impl Eq for ProviderSourceTypeShape<'_> {}

fn write_node(writer: &mut CanonicalFactWriter, node: &IcebergSourceTypeNode) {
    use IcebergSourceTypeNode as Node;
    match node {
        Node::Primitive(primitive) => {
            writer.u32(1);
            primitive.write_identity(writer);
        }
        Node::Struct { field_count } => {
            writer.u32(2);
            writer.u32(*field_count);
        }
        Node::Field { id, name, required } => {
            writer.u32(3);
            writer.i32(*id);
            writer.text(name);
            writer.boolean(*required);
        }
        Node::List {
            element_id,
            element_required,
        } => {
            writer.u32(4);
            writer.i32(*element_id);
            writer.boolean(*element_required);
        }
        Node::Map {
            key_id,
            value_id,
            value_required,
        } => {
            writer.u32(5);
            writer.i32(*key_id);
            writer.i32(*value_id);
            writer.boolean(*value_required);
        }
    }
}

impl IcebergSourcePrimitive {
    fn write_identity(self, writer: &mut CanonicalFactWriter) {
        writer.u32(match self {
            Self::Boolean => 1,
            Self::Int => 2,
            Self::Long => 3,
            Self::Float => 4,
            Self::Double => 5,
            Self::Decimal { .. } => 6,
            Self::Date => 7,
            Self::Time => 8,
            Self::Timestamp => 9,
            Self::Timestamptz => 10,
            Self::TimestampNs => 11,
            Self::TimestamptzNs => 12,
            Self::String => 13,
            Self::Uuid => 14,
            Self::Fixed(_) => 15,
            Self::Binary => 16,
            Self::Variant => 17,
        });
        match self {
            Self::Decimal { precision, scale } => {
                writer.u32(precision);
                writer.u32(scale);
            }
            Self::Fixed(width) => writer.u64(width),
            _ => {}
        }
    }
}

impl fmt::Display for IcebergSourcePrimitive {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match *self {
            Self::Boolean => "boolean",
            Self::Int => "int",
            Self::Long => "long",
            Self::Float => "float",
            Self::Double => "double",
            Self::Decimal { precision, scale } => {
                return write!(f, "decimal({precision}, {scale})");
            }
            Self::Date => "date",
            Self::Time => "time",
            Self::Timestamp => "timestamp",
            Self::Timestamptz => "timestamptz",
            Self::TimestampNs => "timestamp_ns",
            Self::TimestamptzNs => "timestamptz_ns",
            Self::String => "string",
            Self::Uuid => "uuid",
            Self::Fixed(width) => return write!(f, "fixed({width})"),
            Self::Binary => "binary",
            Self::Variant => "variant",
        })
    }
}

impl fmt::Display for ProviderSourceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Iceberg(source) => write_type_display(f, &mut source.nodes().iter()),
        }
    }
}

fn write_type_display(
    f: &mut fmt::Formatter<'_>,
    nodes: &mut std::slice::Iter<'_, IcebergSourceTypeNode>,
) -> fmt::Result {
    use IcebergSourceTypeNode as Node;
    match nodes.next().ok_or(fmt::Error)? {
        Node::Primitive(value) => write!(f, "{value}"),
        Node::Struct { field_count } => {
            f.write_str("struct<")?;
            for _ in 0..*field_count {
                if !matches!(nodes.next(), Some(Node::Field { .. })) {
                    return Err(fmt::Error);
                }
                write_type_display(f, nodes)?;
            }
            f.write_str(">")
        }
        Node::List { .. } => {
            skip_display_types(nodes, 1)?;
            f.write_str("list")
        }
        Node::Map { .. } => {
            skip_display_types(nodes, 2)?;
            f.write_str("map")
        }
        Node::Field { .. } => Err(fmt::Error),
    }
}

fn skip_display_types(
    nodes: &mut std::slice::Iter<'_, IcebergSourceTypeNode>,
    mut remaining: usize,
) -> fmt::Result {
    use IcebergSourceTypeNode as Node;
    while remaining != 0 {
        remaining -= 1;
        let children = match nodes.next().ok_or(fmt::Error)? {
            Node::Primitive(_) => 0,
            Node::Field { .. } | Node::List { .. } => 1,
            Node::Map { .. } => 2,
            Node::Struct { field_count } => *field_count as usize,
        };
        remaining = remaining.checked_add(children).ok_or(fmt::Error)?;
    }
    Ok(())
}
