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

//! Root purpose projection. Unknown kinds and profiles have no fallback.
use crate::{FieldPath, ProtocolError, ProtocolErrorKind};
use novarocks_proto_models::result as wire;
use novarocks_result_contract::{InternalResultDomain, RootOutputKind, RootProfileId};
fn invalid(path: FieldPath, message: &str) -> ProtocolError {
    ProtocolError::new(path, ProtocolErrorKind::InvalidValue, message)
}
pub fn decode_profile(value: u32, path: FieldPath) -> Result<RootProfileId, ProtocolError> {
    RootProfileId::try_from_wire(value)
        .map_err(|_| invalid(path, "unsupported root-result profile"))
}
pub fn encode_kind(value: RootOutputKind) -> wire::RootOutputKind {
    use wire::root_output_kind::Kind;
    let kind = match value {
        RootOutputKind::ClientRows => Kind::ClientRows(true),
        RootOutputKind::CountOnly => Kind::CountOnly(true),
        RootOutputKind::InternalFacts(domain) => Kind::InternalFacts(match domain {
            InternalResultDomain::ScalarValueV1 => wire::InternalResultDomain::ScalarValueV1,
            InternalResultDomain::CowSelectionArrowV1 => {
                wire::InternalResultDomain::CowSelectionArrowV1
            }
            InternalResultDomain::StatisticsArtifactV1 => {
                wire::InternalResultDomain::StatisticsArtifactV1
            }
            InternalResultDomain::PreparedWriteCommitV1 => {
                wire::InternalResultDomain::PreparedWriteCommitV1
            }
        } as i32),
    };
    wire::RootOutputKind { kind: Some(kind) }
}
pub fn decode_kind(
    value: &wire::RootOutputKind,
    path: FieldPath,
) -> Result<RootOutputKind, ProtocolError> {
    use wire::root_output_kind::Kind;
    match value.kind.as_ref() {
        Some(Kind::ClientRows(true)) => Ok(RootOutputKind::ClientRows),
        Some(Kind::CountOnly(true)) => Ok(RootOutputKind::CountOnly),
        Some(Kind::InternalFacts(domain)) => {
            let domain = match wire::InternalResultDomain::try_from(*domain) {
                Ok(wire::InternalResultDomain::ScalarValueV1) => {
                    InternalResultDomain::ScalarValueV1
                }
                Ok(wire::InternalResultDomain::CowSelectionArrowV1) => {
                    InternalResultDomain::CowSelectionArrowV1
                }
                Ok(wire::InternalResultDomain::StatisticsArtifactV1) => {
                    InternalResultDomain::StatisticsArtifactV1
                }
                Ok(wire::InternalResultDomain::PreparedWriteCommitV1) => {
                    InternalResultDomain::PreparedWriteCommitV1
                }
                _ => {
                    return Err(invalid(
                        path.field("internal_facts"),
                        "unknown internal root-result domain",
                    ));
                }
            };
            Ok(RootOutputKind::InternalFacts(domain))
        }
        _ => Err(invalid(
            path,
            "root-result purpose requires one closed kind",
        )),
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn purposes_are_closed_and_profiles_explicit() {
        for kind in [
            RootOutputKind::ClientRows,
            RootOutputKind::CountOnly,
            RootOutputKind::InternalFacts(InternalResultDomain::ScalarValueV1),
            RootOutputKind::InternalFacts(InternalResultDomain::CowSelectionArrowV1),
            RootOutputKind::InternalFacts(InternalResultDomain::StatisticsArtifactV1),
            RootOutputKind::InternalFacts(InternalResultDomain::PreparedWriteCommitV1),
        ] {
            assert_eq!(
                decode_kind(&encode_kind(kind), FieldPath::root("kind")).unwrap(),
                kind
            );
        }
        for value in [
            wire::RootOutputKind { kind: None },
            wire::RootOutputKind {
                kind: Some(wire::root_output_kind::Kind::ClientRows(false)),
            },
            wire::RootOutputKind {
                kind: Some(wire::root_output_kind::Kind::InternalFacts(100)),
            },
        ] {
            assert!(decode_kind(&value, FieldPath::root("kind")).is_err());
        }
        assert!(decode_profile(0, FieldPath::root("profile")).is_err());
        assert!(decode_profile(2, FieldPath::root("profile")).is_err());
    }
}

use novarocks_result_contract::{
    ClientPresentationFamily, ClientRenderSchema, NamedRenderField, NativeRenderType,
    OpaqueRenderType, RenderColumn, RenderField, RenderPresentation, RenderTimeUnit, RootProfileV1,
    TrinoJsonFacts,
};
use prost::Message;

pub fn encode_client_schema(schema: &ClientRenderSchema) -> wire::ClientRenderSchema {
    let mut nodes = Vec::new();
    let columns = schema
        .columns()
        .iter()
        .map(|column| wire::RenderColumn {
            source_ordinal: column.source_ordinal,
            source_slot: column.source_slot,
            name: column.name.clone(),
            field_node_id: encode_field(&column.field, &mut nodes),
        })
        .collect();
    wire::ClientRenderSchema {
        columns,
        field_nodes: nodes,
        family: Some(match schema.family() {
            ClientPresentationFamily::TrinoJson(facts) => {
                wire::client_render_schema::Family::TrinoJson(wire::TrinoJsonFamily {
                    parametric_datetime: facts.parametric_datetime,
                    variant: facts.variant,
                    variant_binary: facts.variant_binary,
                })
            }
        }),
    }
}
fn encode_field(field: &RenderField, nodes: &mut Vec<wire::RenderField>) -> u32 {
    let id = (nodes.len() + 1) as u32;
    nodes.push(wire::RenderField::default());
    let presentation = match field.presentation {
        RenderPresentation::TrinoValue => wire::RenderPresentation::TrinoValue,
    };
    let native_type = encode_type(&field.native_type, nodes);
    nodes[id as usize - 1] = wire::RenderField {
        nullable: field.nullable,
        native_type: Some(native_type),
        presentation: presentation as i32,
    };
    id
}
fn encode_type(
    ty: &NativeRenderType,
    nodes: &mut Vec<wire::RenderField>,
) -> wire::NativeRenderType {
    use wire::RenderTypeKind as K;
    let mut out = wire::NativeRenderType::default();
    let kind = match ty {
        NativeRenderType::Null => K::NullValue,
        NativeRenderType::Boolean => K::Boolean,
        NativeRenderType::SignedInteger(bits) => {
            out.bits = u32::from(*bits);
            K::SignedInteger
        }
        NativeRenderType::UnsignedInteger(bits) => {
            out.bits = u32::from(*bits);
            K::UnsignedInteger
        }
        NativeRenderType::LargeInt => K::LargeInt,
        NativeRenderType::Float32 => K::Float32,
        NativeRenderType::Float64 => K::Float64,
        NativeRenderType::Decimal {
            bits,
            precision,
            scale,
        } => {
            out.bits = u32::from(*bits);
            out.precision = u32::from(*precision);
            out.scale = i32::from(*scale);
            K::Decimal
        }
        NativeRenderType::String => K::String,
        NativeRenderType::Binary => K::Binary,
        NativeRenderType::Date => K::Date,
        NativeRenderType::Time { unit } => {
            out.time_unit = encode_time_unit(*unit);
            K::Time
        }
        NativeRenderType::Timestamp { unit, timezone } => {
            out.time_unit = encode_time_unit(*unit);
            out.time_zone = timezone.clone();
            K::Timestamp
        }
        NativeRenderType::Json => K::Json,
        NativeRenderType::Variant => K::Variant,
        NativeRenderType::Opaque(kind) => {
            out.opaque_kind = match kind {
                OpaqueRenderType::Hll => wire::OpaqueKind::Hll,
                OpaqueRenderType::Bitmap => wire::OpaqueKind::Bitmap,
                OpaqueRenderType::Object => wire::OpaqueKind::Object,
                OpaqueRenderType::Percentile => wire::OpaqueKind::Percentile,
            } as i32;
            K::Opaque
        }
        NativeRenderType::List(child) => {
            out.children.push(wire::NamedRenderField {
                name: "item".into(),
                field_node_id: encode_field(child, nodes),
            });
            K::List
        }
        NativeRenderType::Map { key, value } => {
            out.children = vec![
                wire::NamedRenderField {
                    name: "key".into(),
                    field_node_id: encode_field(key, nodes),
                },
                wire::NamedRenderField {
                    name: "value".into(),
                    field_node_id: encode_field(value, nodes),
                },
            ];
            K::Map
        }
        NativeRenderType::Struct(fields) => {
            out.children = fields
                .iter()
                .map(|child| wire::NamedRenderField {
                    name: child.name.clone(),
                    field_node_id: encode_field(&child.field, nodes),
                })
                .collect();
            K::Struct
        }
    };
    out.kind = kind as i32;
    out
}
fn encode_time_unit(unit: RenderTimeUnit) -> i32 {
    (match unit {
        RenderTimeUnit::Second => wire::RenderTimeUnit::Second,
        RenderTimeUnit::Millisecond => wire::RenderTimeUnit::Millisecond,
        RenderTimeUnit::Microsecond => wire::RenderTimeUnit::Microsecond,
        RenderTimeUnit::Nanosecond => wire::RenderTimeUnit::Nanosecond,
    }) as i32
}
fn decode_time_unit(unit: i32, path: FieldPath) -> Result<RenderTimeUnit, ProtocolError> {
    match wire::RenderTimeUnit::try_from(unit) {
        Ok(wire::RenderTimeUnit::Second) => Ok(RenderTimeUnit::Second),
        Ok(wire::RenderTimeUnit::Millisecond) => Ok(RenderTimeUnit::Millisecond),
        Ok(wire::RenderTimeUnit::Microsecond) => Ok(RenderTimeUnit::Microsecond),
        Ok(wire::RenderTimeUnit::Nanosecond) => Ok(RenderTimeUnit::Nanosecond),
        _ => Err(invalid(path, "unsupported frozen render time unit")),
    }
}

/// Preflight the decoded DTO before allocating the neutral tree. Transport
/// additionally preflights raw protobuf before prost and guards both owners.
pub fn decode_client_schema(
    src: &wire::ClientRenderSchema,
    input_columns: usize,
    path: FieldPath,
) -> Result<ClientRenderSchema, ProtocolError> {
    let family = match src.family.as_ref() {
        Some(wire::client_render_schema::Family::TrinoJson(facts)) => {
            ClientPresentationFamily::TrinoJson(TrinoJsonFacts {
                parametric_datetime: facts.parametric_datetime,
                variant: facts.variant,
                variant_binary: facts.variant_binary,
            })
        }
        None => return Err(invalid(path, "client schema requires presentation family")),
    };
    if src.columns.is_empty()
        || src.columns.len() > RootProfileV1::MAX_COLUMNS
        || src.field_nodes.is_empty()
        || src.field_nodes.len() > RootProfileV1::SCHEMA_TYPE_NODES
        || src.encoded_len() > RootProfileV1::SCHEMA_WIRE_BYTES
    {
        return Err(invalid(path, "client schema exceeds its frozen profile"));
    }
    // DTO and neutral trees coexist during projection. Count actual DTO
    // capacities and exact prospective neutral allocations, not payload len.
    let mut backing = size_of::<wire::ClientRenderSchema>() + size_of::<ClientRenderSchema>();
    add_backing(
        &mut backing,
        src.columns
            .capacity()
            .checked_mul(size_of::<wire::RenderColumn>())
            .ok_or_else(|| invalid(path.clone(), "render capacity overflow"))?,
        path.clone(),
    )?;
    add_backing(
        &mut backing,
        src.field_nodes
            .capacity()
            .checked_mul(size_of::<wire::RenderField>())
            .ok_or_else(|| invalid(path.clone(), "render capacity overflow"))?,
        path.clone(),
    )?;
    add_backing(
        &mut backing,
        src.columns.len() * size_of::<RenderColumn>() + src.field_nodes.len(),
        path.clone(),
    )?;
    for column in &src.columns {
        add_backing(
            &mut backing,
            column.name.capacity() + column.name.len(),
            path.clone(),
        )?;
        if column.name.len() > RootProfileV1::MAX_NAME_BYTES {
            return Err(invalid(path, "render column name exceeds profile"));
        }
    }
    for field in &src.field_nodes {
        let ty = field
            .native_type
            .as_ref()
            .ok_or_else(|| invalid(path.clone(), "render field requires native type"))?;
        if ty.children.len() > RootProfileV1::MAX_COLUMNS {
            return Err(invalid(path, "render child count exceeds profile"));
        }
        add_backing(
            &mut backing,
            ty.children
                .capacity()
                .checked_mul(size_of::<wire::NamedRenderField>())
                .ok_or_else(|| invalid(path.clone(), "render capacity overflow"))?,
            path.clone(),
        )?;
        if let Some(zone) = &ty.time_zone {
            add_backing(&mut backing, zone.capacity() + zone.len(), path.clone())?;
        }
        // Root fields are inline in RenderColumn; nested fields are owned by
        // one exact List/Map box or by a Struct's exactly reserved field Vec.
        let neutral_children = match wire::RenderTypeKind::try_from(ty.kind) {
            Ok(wire::RenderTypeKind::List) => size_of::<RenderField>(),
            Ok(wire::RenderTypeKind::Map) => 2 * size_of::<RenderField>(),
            Ok(wire::RenderTypeKind::Struct) => ty.children.len() * size_of::<NamedRenderField>(),
            _ => 0,
        };
        add_backing(&mut backing, neutral_children, path.clone())?;
        for child in &ty.children {
            add_backing(&mut backing, child.name.capacity(), path.clone())?;
            if ty.kind == wire::RenderTypeKind::Struct as i32 {
                add_backing(&mut backing, child.name.len(), path.clone())?;
            }
            if child.name.len() > RootProfileV1::MAX_NAME_BYTES {
                return Err(invalid(path, "render child name exceeds profile"));
            }
        }
    }
    // Visit before constructing the neutral tree: shared or unreachable nodes
    // cannot trigger repeated materialization, and cycles cannot recurse.
    let mut seen = vec![false; src.field_nodes.len()];
    for column in &src.columns {
        visit_field(src, column.field_node_id, 1, &mut seen, path.clone())?;
    }
    if seen.iter().any(|seen| !seen) {
        return Err(invalid(path, "render schema contains unreachable nodes"));
    }
    let mut columns = Vec::new();
    columns
        .try_reserve_exact(src.columns.len())
        .map_err(|_| invalid(path.clone(), "cannot reserve bounded render columns"))?;
    for (index, column) in src.columns.iter().enumerate() {
        columns.push(RenderColumn {
            source_ordinal: column.source_ordinal,
            source_slot: column.source_slot,
            name: column.name.clone(),
            field: decode_field(
                src,
                column.field_node_id,
                1,
                path.clone().field("columns").index(index),
            )?,
        });
    }
    ClientRenderSchema::try_new(family, columns, input_columns)
        .map_err(|_| invalid(path, "invalid frozen client schema"))
}
fn add_backing(backing: &mut usize, count: usize, path: FieldPath) -> Result<(), ProtocolError> {
    *backing = backing
        .checked_add(count)
        .ok_or_else(|| invalid(path.clone(), "render schema size overflow"))?;
    if *backing > RootProfileV1::SCHEMA_BACKING_BYTES {
        return Err(invalid(path, "render schema backing exceeds profile"));
    }
    Ok(())
}
fn node_at(
    schema: &wire::ClientRenderSchema,
    id: u32,
    path: FieldPath,
) -> Result<&wire::RenderField, ProtocolError> {
    id.checked_sub(1)
        .and_then(|index| schema.field_nodes.get(index as usize))
        .ok_or_else(|| invalid(path, "render schema node reference is zero or out of range"))
}
fn visit_field(
    schema: &wire::ClientRenderSchema,
    id: u32,
    depth: usize,
    seen: &mut [bool],
    path: FieldPath,
) -> Result<(), ProtocolError> {
    if depth > RootProfileV1::MAX_DEPTH {
        return Err(invalid(path, "render schema exceeds depth profile"));
    }
    let node = node_at(schema, id, path.clone())?;
    let visited = &mut seen[id as usize - 1];
    if *visited {
        return Err(invalid(path, "render schema node is shared or cyclic"));
    }
    *visited = true;
    let ty = node
        .native_type
        .as_ref()
        .ok_or_else(|| invalid(path.clone(), "render field requires native type"))?;
    for child in &ty.children {
        visit_field(schema, child.field_node_id, depth + 1, seen, path.clone())?;
    }
    Ok(())
}
fn decode_field(
    schema: &wire::ClientRenderSchema,
    id: u32,
    depth: usize,
    path: FieldPath,
) -> Result<RenderField, ProtocolError> {
    if depth > RootProfileV1::MAX_DEPTH {
        return Err(invalid(path, "render schema exceeds depth profile"));
    }
    let src = node_at(schema, id, path.clone())?;
    let presentation = match wire::RenderPresentation::try_from(src.presentation) {
        Ok(wire::RenderPresentation::TrinoValue) => RenderPresentation::TrinoValue,
        _ => return Err(invalid(path, "render presentation is required and closed")),
    };
    let ty = src
        .native_type
        .as_ref()
        .ok_or_else(|| invalid(path.clone(), "render native type is required"))?;
    let native_type = decode_type(schema, ty, depth, path.clone())?;
    Ok(RenderField {
        presentation,
        nullable: src.nullable,
        native_type,
    })
}
fn decode_type(
    schema: &wire::ClientRenderSchema,
    src: &wire::NativeRenderType,
    depth: usize,
    path: FieldPath,
) -> Result<NativeRenderType, ProtocolError> {
    use wire::RenderTypeKind as K;
    let bits =
        || u16::try_from(src.bits).map_err(|_| invalid(path.clone(), "render width overflow"));
    let child = |index: usize| -> Result<RenderField, ProtocolError> {
        decode_field(
            schema,
            src.children
                .get(index)
                .ok_or_else(|| invalid(path.clone(), "render child is required"))?
                .field_node_id,
            depth + 1,
            path.clone().field("children").index(index),
        )
    };
    let kind = K::try_from(src.kind).map_err(|_| invalid(path.clone(), "unknown render type"))?;
    let conflict = (!matches!(kind, K::SignedInteger | K::UnsignedInteger | K::Decimal)
        && src.bits != 0)
        || (kind != K::Decimal && (src.precision != 0 || src.scale != 0))
        || (!matches!(kind, K::Time | K::Timestamp) && src.time_unit != 0)
        || (kind != K::Timestamp && src.time_zone.is_some())
        || (kind != K::Opaque && src.opaque_kind != 0)
        || (!matches!(kind, K::List | K::Map | K::Struct) && !src.children.is_empty())
        || (kind == K::List && (src.children.len() != 1 || src.children[0].name != "item"))
        || (kind == K::Map
            && (src.children.len() != 2
                || src.children[0].name != "key"
                || src.children[1].name != "value"));
    if conflict {
        return Err(invalid(
            path,
            "render type contains conflicting semantic fields",
        ));
    }
    let ty = match kind {
        K::NullValue => NativeRenderType::Null,
        K::Boolean => NativeRenderType::Boolean,
        K::SignedInteger => NativeRenderType::SignedInteger(bits()?),
        K::UnsignedInteger => NativeRenderType::UnsignedInteger(bits()?),
        K::LargeInt => NativeRenderType::LargeInt,
        K::Float32 => NativeRenderType::Float32,
        K::Float64 => NativeRenderType::Float64,
        K::Decimal => NativeRenderType::Decimal {
            bits: bits()?,
            precision: u8::try_from(src.precision)
                .map_err(|_| invalid(path.clone(), "render precision overflow"))?,
            scale: i8::try_from(src.scale)
                .map_err(|_| invalid(path.clone(), "render scale overflow"))?,
        },
        K::String => NativeRenderType::String,
        K::Binary => NativeRenderType::Binary,
        K::Date => NativeRenderType::Date,
        K::Time => NativeRenderType::Time {
            unit: decode_time_unit(src.time_unit, path.clone())?,
        },
        K::Timestamp => NativeRenderType::Timestamp {
            unit: decode_time_unit(src.time_unit, path.clone())?,
            timezone: src.time_zone.clone(),
        },
        K::Json => NativeRenderType::Json,
        K::Variant => NativeRenderType::Variant,
        K::Opaque => NativeRenderType::Opaque(match wire::OpaqueKind::try_from(src.opaque_kind) {
            Ok(wire::OpaqueKind::Hll) => OpaqueRenderType::Hll,
            Ok(wire::OpaqueKind::Bitmap) => OpaqueRenderType::Bitmap,
            Ok(wire::OpaqueKind::Object) => OpaqueRenderType::Object,
            Ok(wire::OpaqueKind::Percentile) => OpaqueRenderType::Percentile,
            _ => return Err(invalid(path, "unknown opaque render kind")),
        }),
        K::List => NativeRenderType::List(Box::new(child(0)?)),
        K::Map => NativeRenderType::Map {
            key: Box::new(child(0)?),
            value: Box::new(child(1)?),
        },
        K::Struct => {
            let mut fields = Vec::new();
            fields
                .try_reserve_exact(src.children.len())
                .map_err(|_| invalid(path.clone(), "cannot reserve bounded render fields"))?;
            for (index, c) in src.children.iter().enumerate() {
                fields.push(NamedRenderField {
                    name: c.name.clone(),
                    field: child(index)?,
                });
            }
            NativeRenderType::Struct(fields)
        }
        K::Unspecified => return Err(invalid(path, "render native type is unspecified")),
    };
    Ok(ty)
}

#[cfg(test)]
mod schema_tests {
    use super::*;
    fn col(
        source_ordinal: u32,
        native_type: NativeRenderType,
        presentation: RenderPresentation,
    ) -> RenderColumn {
        RenderColumn {
            source_ordinal,
            source_slot: Some(source_ordinal + 10),
            name: "duplicate".into(),
            field: RenderField {
                nullable: true,
                native_type,
                presentation,
            },
        }
    }
    #[test]
    fn trino_facts_round_trip_and_missing_or_retired_wire_values_are_refused() {
        for mask in 0..8 {
            let facts = TrinoJsonFacts {
                parametric_datetime: mask & 1 != 0,
                variant: mask & 2 != 0,
                variant_binary: mask & 4 != 0,
            };
            let schema = ClientRenderSchema::try_new(
                ClientPresentationFamily::TrinoJson(facts),
                vec![col(
                    0,
                    NativeRenderType::String,
                    RenderPresentation::TrinoValue,
                )],
                1,
            )
            .unwrap();
            let bytes = encode_client_schema(&schema).encode_to_vec();
            let encoded = wire::ClientRenderSchema::decode(bytes.as_slice()).unwrap();
            assert_eq!(
                decode_client_schema(&encoded, 1, FieldPath::root("schema")).unwrap(),
                schema
            );
            let mut missing = encoded.clone();
            missing.family = None;
            assert!(decode_client_schema(&missing, 1, FieldPath::root("schema")).is_err());
            let mut retired_family = encoded.clone();
            retired_family.family = None;
            let mut bytes = retired_family.encode_to_vec();
            // The retired family occupied length-delimited field 3. It must
            // remain absent after decoding, never selecting a default family.
            bytes.extend_from_slice(&[0x1a, 0x00]);
            let retired_family = wire::ClientRenderSchema::decode(bytes.as_slice()).unwrap();
            assert!(decode_client_schema(&retired_family, 1, FieldPath::root("schema")).is_err());
            for presentation in (0..=8).chain([i32::MAX]) {
                let mut invalid = encoded.clone();
                invalid.field_nodes[0].presentation = presentation;
                assert!(decode_client_schema(&invalid, 1, FieldPath::root("schema")).is_err());
            }
        }
    }
    #[test]
    fn schema_preserves_exact_width_precision_zone_and_occurrences() {
        let columns = vec![
            col(
                0,
                NativeRenderType::SignedInteger(64),
                RenderPresentation::TrinoValue,
            ),
            col(
                1,
                NativeRenderType::Decimal {
                    bits: 256,
                    precision: 38,
                    scale: 9,
                },
                RenderPresentation::TrinoValue,
            ),
            col(
                2,
                NativeRenderType::Timestamp {
                    unit: RenderTimeUnit::Nanosecond,
                    timezone: Some("UTC".into()),
                },
                RenderPresentation::TrinoValue,
            ),
            col(3, NativeRenderType::Variant, RenderPresentation::TrinoValue),
            col(4, NativeRenderType::Binary, RenderPresentation::TrinoValue),
        ];
        let schema = ClientRenderSchema::try_new(
            ClientPresentationFamily::TrinoJson(TrinoJsonFacts {
                parametric_datetime: true,
                variant: true,
                variant_binary: true,
            }),
            columns,
            5,
        )
        .unwrap();
        let encoded = encode_client_schema(&schema);
        assert_eq!(encoded.columns[0].source_slot, Some(10));
        let decimal = encoded.field_nodes[encoded.columns[1].field_node_id as usize - 1]
            .native_type
            .as_ref()
            .unwrap();
        assert_eq!(
            (decimal.bits, decimal.precision, decimal.scale),
            (256, 38, 9)
        );
        assert_eq!(
            decode_client_schema(&encoded, 5, FieldPath::root("schema")).unwrap(),
            schema
        );
    }
    #[test]
    fn nested_trino_fields_preserve_native_timezone_and_capabilities() {
        let schema = ClientRenderSchema::try_new(
            ClientPresentationFamily::TrinoJson(TrinoJsonFacts {
                parametric_datetime: true,
                variant: true,
                variant_binary: true,
            }),
            vec![col(
                0,
                NativeRenderType::Struct(vec![
                    NamedRenderField {
                        name: "variant".into(),
                        field: RenderField {
                            nullable: true,
                            native_type: NativeRenderType::Variant,
                            presentation: RenderPresentation::TrinoValue,
                        },
                    },
                    NamedRenderField {
                        name: "timestamp".into(),
                        field: RenderField {
                            nullable: true,
                            native_type: NativeRenderType::Timestamp {
                                unit: RenderTimeUnit::Nanosecond,
                                timezone: Some("UTC".into()),
                            },
                            presentation: RenderPresentation::TrinoValue,
                        },
                    },
                ]),
                RenderPresentation::TrinoValue,
            )],
            1,
        )
        .unwrap();
        let encoded = encode_client_schema(&schema);
        assert_eq!(
            decode_client_schema(&encoded, 1, FieldPath::root("schema")).unwrap(),
            schema
        );
        assert_eq!(
            schema.family(),
            &ClientPresentationFamily::TrinoJson(TrinoJsonFacts {
                parametric_datetime: true,
                variant: true,
                variant_binary: true
            })
        );
    }

    #[test]
    fn missing_extra_or_conflicting_semantics_fail_before_binding() {
        let schema = ClientRenderSchema::try_new(
            ClientPresentationFamily::TrinoJson(TrinoJsonFacts::default()),
            vec![col(
                0,
                NativeRenderType::String,
                RenderPresentation::TrinoValue,
            )],
            1,
        )
        .unwrap();
        let base = encode_client_schema(&schema);
        let mut wire = base.clone();
        wire.columns[0].field_node_id = 0;
        assert!(decode_client_schema(&wire, 1, FieldPath::root("schema")).is_err());
        let mut wire = base.clone();
        wire.field_nodes[0].native_type.as_mut().unwrap().precision = 10;
        assert!(decode_client_schema(&wire, 1, FieldPath::root("schema")).is_err());
        let mut wire = base.clone();
        wire.field_nodes[0].presentation = 0;
        assert!(decode_client_schema(&wire, 1, FieldPath::root("schema")).is_err());
        let mut wire = base.clone();
        wire.columns[0].source_ordinal = 1;
        assert!(decode_client_schema(&wire, 1, FieldPath::root("schema")).is_err());
        let mut wire = base;
        wire.columns[0].name = "x".repeat(RootProfileV1::MAX_NAME_BYTES + 1);
        assert!(decode_client_schema(&wire, 1, FieldPath::root("schema")).is_err());
    }
    #[test]
    fn flat_schema_supports_64_semantic_levels_through_real_prost() {
        let mut field = RenderField {
            nullable: true,
            native_type: NativeRenderType::String,
            presentation: RenderPresentation::TrinoValue,
        };
        for _ in 1..RootProfileV1::MAX_DEPTH {
            field = RenderField {
                nullable: true,
                native_type: NativeRenderType::List(Box::new(field)),
                presentation: RenderPresentation::TrinoValue,
            };
        }
        let schema = ClientRenderSchema::try_new(
            ClientPresentationFamily::TrinoJson(TrinoJsonFacts::default()),
            vec![RenderColumn {
                source_ordinal: 0,
                source_slot: Some(10),
                name: "deep".into(),
                field,
            }],
            1,
        )
        .unwrap();
        let contract = RootOutputContract::new(
            RootProfileId::V1,
            FrozenRootOutput::ClientRows(schema.clone()),
        );
        let bytes = encode_root_contract(&contract).encode_to_vec();
        let decoded = wire::RootOutputContract::decode(bytes.as_slice()).unwrap();
        assert_eq!(
            decode_root_contract(&decoded, 1, FieldPath::root("root")).unwrap(),
            contract
        );
        let mut too_deep = encode_client_schema(&schema);
        too_deep
            .field_nodes
            .push(too_deep.field_nodes.last().unwrap().clone());
        let leaf = too_deep.field_nodes.len() - 2;
        too_deep.field_nodes[leaf] = wire::RenderField {
            nullable: true,
            presentation: wire::RenderPresentation::TrinoValue as i32,
            native_type: Some(wire::NativeRenderType {
                kind: wire::RenderTypeKind::List as i32,
                children: vec![wire::NamedRenderField {
                    name: "item".into(),
                    field_node_id: (leaf + 2) as u32,
                }],
                ..Default::default()
            }),
        };
        assert!(decode_client_schema(&too_deep, 1, FieldPath::root("schema")).is_err());
    }
    #[test]
    fn flat_schema_rejects_graph_expansion_cycles_and_unused_nodes() {
        let schema = ClientRenderSchema::try_new(
            ClientPresentationFamily::TrinoJson(TrinoJsonFacts::default()),
            vec![col(
                0,
                NativeRenderType::String,
                RenderPresentation::TrinoValue,
            )],
            1,
        )
        .unwrap();
        let base = encode_client_schema(&schema);
        for id in [0, 2, u32::MAX] {
            let mut broken = base.clone();
            broken.columns[0].field_node_id = id;
            assert!(decode_client_schema(&broken, 1, FieldPath::root("schema")).is_err());
        }
        let mut shared = base.clone();
        shared.columns.push(shared.columns[0].clone());
        assert!(decode_client_schema(&shared, 1, FieldPath::root("schema")).is_err());
        let mut unused = base.clone();
        unused.field_nodes.push(unused.field_nodes[0].clone());
        assert!(decode_client_schema(&unused, 1, FieldPath::root("schema")).is_err());
        let mut cycle = base;
        cycle.field_nodes[0].native_type = Some(wire::NativeRenderType {
            kind: wire::RenderTypeKind::List as i32,
            children: vec![wire::NamedRenderField {
                name: "item".into(),
                field_node_id: 1,
            }],
            ..Default::default()
        });
        assert!(decode_client_schema(&cycle, 1, FieldPath::root("schema")).is_err());
    }
}

use novarocks_result_contract::{FrozenRootOutput, RootOutputContract};
pub fn encode_root_contract(value: &RootOutputContract) -> wire::RootOutputContract {
    wire::RootOutputContract {
        profile_id: value.profile().get(),
        output_kind: Some(encode_kind(value.kind())),
        scalar_schema: match value.output() {
            FrozenRootOutput::ScalarValue(schema) => {
                Some(crate::scalar_result::encode_scalar_schema(schema))
            }
            _ => None,
        },
        client_schema: match value.output() {
            FrozenRootOutput::ClientRows(schema) => Some(encode_client_schema(schema)),
            _ => None,
        },
    }
}
pub fn decode_root_contract(
    value: &wire::RootOutputContract,
    input_columns: usize,
    path: FieldPath,
) -> Result<RootOutputContract, ProtocolError> {
    decode_root_contract_with_scalar_decoder(
        value,
        input_columns,
        path,
        crate::scalar_result::decode_scalar_schema,
    )
}

/// Decode the task-independent package declaration. Local compilation supplies
/// the sole physical source slot; native transport retains its strict decoder.
pub fn decode_semantic_root_contract(
    value: &wire::RootOutputContract,
    input_columns: usize,
    path: FieldPath,
) -> Result<RootOutputContract, ProtocolError> {
    decode_root_contract_with_scalar_decoder(
        value,
        input_columns,
        path,
        crate::scalar_result::decode_semantic_result_scalar_schema,
    )
}

fn decode_root_contract_with_scalar_decoder(
    value: &wire::RootOutputContract,
    input_columns: usize,
    path: FieldPath,
    decode_scalar: fn(
        &wire::ScalarSchema,
        usize,
        FieldPath,
    ) -> Result<novarocks_result_contract::ScalarSchema, ProtocolError>,
) -> Result<RootOutputContract, ProtocolError> {
    let profile = decode_profile(value.profile_id, path.clone().field("profile_id"))?;
    let kind = decode_kind(
        value
            .output_kind
            .as_ref()
            .ok_or_else(|| invalid(path.clone(), "root output purpose is required"))?,
        path.clone().field("output_kind"),
    )?;
    let output = match (
        kind,
        value.client_schema.as_ref(),
        value.scalar_schema.as_ref(),
    ) {
        (RootOutputKind::ClientRows, Some(schema), None) => FrozenRootOutput::ClientRows(
            decode_client_schema(schema, input_columns, path.clone().field("client_schema"))?,
        ),
        (
            RootOutputKind::InternalFacts(InternalResultDomain::ScalarValueV1),
            None,
            Some(schema),
        ) => FrozenRootOutput::ScalarValue(decode_scalar(
            schema,
            input_columns,
            path.clone().field("scalar_schema"),
        )?),
        (RootOutputKind::InternalFacts(domain), None, None)
            if domain != InternalResultDomain::ScalarValueV1 =>
        {
            FrozenRootOutput::InternalFacts(domain)
        }
        (RootOutputKind::CountOnly, None, None) => FrozenRootOutput::CountOnly,
        _ => {
            return Err(invalid(
                path,
                "root schema does not match its frozen purpose",
            ));
        }
    };
    Ok(RootOutputContract::new(profile, output))
}

#[cfg(test)]
mod root_contract_binding_tests {
    use super::*;
    use novarocks_result_contract::{ScalarField, ScalarSchema, ScalarValueType};

    fn scalar() -> RootOutputContract {
        RootOutputContract::new(
            RootProfileId::V1,
            FrozenRootOutput::ScalarValue(
                ScalarSchema::try_new(ScalarField {
                    nullable: true,
                    value_type: ScalarValueType::SignedInteger(64),
                })
                .unwrap(),
            ),
        )
    }

    #[test]
    fn package_root_defers_scalar_slot_binding_and_native_transport_requires_it() {
        let expected = scalar();
        let unbound = encode_root_contract(&expected);
        let path = FieldPath::root("root");
        assert_eq!(
            decode_semantic_root_contract(&unbound, 1, path.clone()).unwrap(),
            expected
        );
        assert!(decode_root_contract(&unbound, 1, path.clone()).is_err());
        let FrozenRootOutput::ScalarValue(schema) = expected.output() else {
            panic!("scalar root");
        };
        let bound = RootOutputContract::new(
            RootProfileId::V1,
            FrozenRootOutput::ScalarValue(schema.clone().bind_native_slots(&[37]).unwrap()),
        );
        let encoded = encode_root_contract(&bound);
        assert_eq!(
            decode_root_contract(&encoded, 1, path.clone()).unwrap(),
            bound
        );
        assert!(decode_semantic_root_contract(&encoded, 1, path).is_err());
    }

    #[test]
    fn semantic_root_keeps_the_exact_width_profile_and_purpose_guards() {
        let valid = encode_root_contract(&scalar());
        for width in [0, 2] {
            assert!(decode_semantic_root_contract(&valid, width, FieldPath::root("root")).is_err());
        }
        let mut invalid_profile = valid.clone();
        invalid_profile.profile_id = 0;
        let mut wrong_purpose = valid.clone();
        wrong_purpose.output_kind = Some(encode_kind(RootOutputKind::CountOnly));
        let mut missing_tree = valid;
        missing_tree
            .scalar_schema
            .as_mut()
            .unwrap()
            .field_nodes
            .clear();
        for invalid in [invalid_profile, wrong_purpose, missing_tree] {
            assert!(decode_semantic_root_contract(&invalid, 1, FieldPath::root("root")).is_err());
        }
    }
}
