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

//! Exact ScalarValueV1 schema projection, independent of client presentation.
//! Transport must preflight raw protobuf and fund its DTO before invoking this
//! decoder. This module bounds DTO/neutral-tree/seen overlap, not outer owners.

use crate::{FieldPath, ProtocolError, ProtocolErrorKind};
use novarocks_proto_models::result as wire;
use novarocks_result_contract::{
    NamedScalarField, RootProfileV1 as P, ScalarField, ScalarOpaqueType as O, ScalarSchema,
    ScalarTimestampUnit as U, ScalarValueType as S,
};
use prost::Message;

fn invalid(path: &FieldPath, message: &'static str) -> ProtocolError {
    ProtocolError::new(path.clone(), ProtocolErrorKind::InvalidValue, message)
}

/// Project an already protected semantic schema. This allocates a wire DTO;
/// the caller remains responsible for its complete owner and backing pregrant.
pub fn encode_scalar_schema(schema: &ScalarSchema) -> wire::ScalarSchema {
    let mut nodes = Vec::with_capacity(schema.type_nodes());
    let root_field_node_id = encode_field(schema.field(), &mut nodes);
    let out = wire::ScalarSchema {
        source_slot: schema.source_slot(),
        root_field_node_id,
        field_nodes: nodes,
    };
    debug_assert!(out.encoded_len() <= schema.wire_capacity_bytes());
    out
}
fn encode_field(field: &ScalarField, nodes: &mut Vec<wire::ScalarField>) -> u32 {
    let id = (nodes.len() + 1) as u32;
    nodes.push(wire::ScalarField::default());
    let mut ty = wire::ScalarValueType::default();
    use wire::ScalarTypeKind as K;
    let kind = match &field.value_type {
        S::Null => K::ScalarNull,
        S::Boolean => K::ScalarBoolean,
        S::SignedInteger(bits) => {
            ty.bits = u32::from(*bits);
            K::ScalarSignedInteger
        }
        S::LargeInt => K::ScalarLargeInt,
        S::Float32 => K::ScalarFloat32,
        S::Float64 => K::ScalarFloat64,
        S::Decimal {
            bits,
            precision,
            scale,
        } => {
            ty.bits = u32::from(*bits);
            ty.precision = u32::from(*precision);
            ty.scale = u32::from(*scale);
            K::ScalarDecimal
        }
        S::String => K::ScalarString,
        S::Binary => K::ScalarBinary,
        S::Date => K::ScalarDate,
        S::TimeMicros => K::ScalarTimeMicros,
        S::Timestamp { unit, timezone } => {
            ty.timestamp_unit = match unit {
                U::Microsecond => wire::ScalarTimestampUnit::ScalarMicrosecond,
                U::Nanosecond => wire::ScalarTimestampUnit::ScalarNanosecond,
            } as i32;
            ty.time_zone = timezone.clone();
            K::ScalarTimestamp
        }
        S::Json => K::ScalarNativeJson,
        S::Variant => K::ScalarVariant,
        S::Opaque(kind) => {
            ty.opaque_kind = match kind {
                O::Hll => wire::OpaqueKind::Hll,
                O::Bitmap => wire::OpaqueKind::Bitmap,
                O::Object => wire::OpaqueKind::Object,
                O::Percentile => wire::OpaqueKind::Percentile,
            } as i32;
            K::ScalarOpaque
        }
        S::List(child) => {
            ty.children.push(wire::NamedScalarField {
                name: "item".into(),
                field_node_id: encode_field(child, nodes),
            });
            K::ScalarList
        }
        S::Map { key, value } => {
            ty.children = vec![
                wire::NamedScalarField {
                    name: "key".into(),
                    field_node_id: encode_field(key, nodes),
                },
                wire::NamedScalarField {
                    name: "value".into(),
                    field_node_id: encode_field(value, nodes),
                },
            ];
            K::ScalarMap
        }
        S::Struct(fields) => {
            ty.children = fields
                .iter()
                .map(|child| wire::NamedScalarField {
                    name: child.name.clone(),
                    field_node_id: encode_field(&child.field, nodes),
                })
                .collect();
            K::ScalarStruct
        }
    };
    ty.kind = kind as i32;
    nodes[id as usize - 1] = wire::ScalarField {
        nullable: field.nullable,
        value_type: Some(ty),
    };
    id
}

fn add(total: &mut usize, bytes: usize, path: &FieldPath) -> Result<(), ProtocolError> {
    *total = total
        .checked_add(bytes)
        .filter(|n| *n <= P::SCHEMA_BACKING_BYTES)
        .ok_or_else(|| invalid(path, "scalar schema backing overlap exceeds profile"))?;
    Ok(())
}
fn allocation<T>(capacity: usize, path: &FieldPath) -> Result<usize, ProtocolError> {
    capacity
        .checked_mul(size_of::<T>())
        .ok_or_else(|| invalid(path, "scalar schema capacity overflow"))
}
fn checked_name(name: &str, path: &FieldPath) -> Result<(), ProtocolError> {
    if name.len() > P::MAX_NAME_BYTES {
        return Err(invalid(path, "scalar name exceeds profile"));
    }
    Ok(())
}

/// Require the closed type's exclusive properties before seen/tree allocation.
fn validate_type(
    ty: &wire::ScalarValueType,
    path: &FieldPath,
) -> Result<wire::ScalarTypeKind, ProtocolError> {
    use wire::ScalarTypeKind as K;
    let kind = K::try_from(ty.kind).map_err(|_| invalid(path, "unknown scalar type"))?;
    let conflict = (!matches!(kind, K::ScalarSignedInteger | K::ScalarDecimal) && ty.bits != 0)
        || (kind != K::ScalarDecimal && (ty.precision != 0 || ty.scale != 0))
        || (kind != K::ScalarTimestamp && (ty.timestamp_unit != 0 || ty.time_zone.is_some()))
        || (kind != K::ScalarOpaque && ty.opaque_kind != 0)
        || (!matches!(kind, K::ScalarList | K::ScalarMap | K::ScalarStruct)
            && !ty.children.is_empty())
        || (kind == K::ScalarList && (ty.children.len() != 1 || ty.children[0].name != "item"))
        || (kind == K::ScalarMap
            && (ty.children.len() != 2
                || ty.children[0].name != "key"
                || ty.children[1].name != "value"));
    if conflict {
        return Err(invalid(path, "scalar type contains conflicting properties"));
    }
    match kind {
        K::Unspecified => return Err(invalid(path, "scalar type is unspecified")),
        K::ScalarSignedInteger if !matches!(ty.bits, 8 | 16 | 32 | 64) => {
            return Err(invalid(path, "unsupported scalar integer width"));
        }
        K::ScalarDecimal => {
            let max = match ty.bits {
                128 => 38,
                256 => 76,
                _ => return Err(invalid(path, "unsupported scalar decimal width")),
            };
            if ty.precision == 0 || ty.precision > max || ty.scale > ty.precision {
                return Err(invalid(path, "invalid scalar decimal precision or scale"));
            }
        }
        K::ScalarTimestamp => {
            if !matches!(
                wire::ScalarTimestampUnit::try_from(ty.timestamp_unit),
                Ok(wire::ScalarTimestampUnit::ScalarMicrosecond
                    | wire::ScalarTimestampUnit::ScalarNanosecond)
            ) {
                return Err(invalid(path, "unsupported scalar timestamp unit"));
            }
            if ty.time_zone.as_ref().is_some_and(String::is_empty) {
                return Err(invalid(path, "empty scalar timestamp timezone"));
            }
        }
        K::ScalarOpaque
            if !matches!(
                wire::OpaqueKind::try_from(ty.opaque_kind),
                Ok(wire::OpaqueKind::Hll
                    | wire::OpaqueKind::Bitmap
                    | wire::OpaqueKind::Object
                    | wire::OpaqueKind::Percentile)
            ) =>
        {
            return Err(invalid(path, "unknown scalar opaque identity"));
        }
        _ => {}
    }
    Ok(kind)
}

/// Validate bounded DTO capacities before computing encoded_len, allocating
/// seen flags, or constructing any neutral String, Vec, or Box. Raw protobuf
/// decoding and the caller's error/path/future metadata have separate owners.
pub fn decode_scalar_schema(
    src: &wire::ScalarSchema,
    input_columns: usize,
    path: FieldPath,
) -> Result<ScalarSchema, ProtocolError> {
    decode_scalar_schema_with_binding(src, input_columns, path, SchemaBinding::NativeSource)
}

/// Decode the compiler ResultPort's semantic field tree before local native
/// slots exist. This entry accepts only an explicitly unbound source; it never
/// manufactures a slot or substitutes for the bound native schema contract.
pub fn decode_semantic_result_scalar_schema(
    src: &wire::ScalarSchema,
    input_columns: usize,
    path: FieldPath,
) -> Result<ScalarSchema, ProtocolError> {
    decode_scalar_schema_with_binding(src, input_columns, path, SchemaBinding::SemanticResult)
}

#[derive(Clone, Copy)]
enum SchemaBinding {
    NativeSource,
    SemanticResult,
}

fn decode_scalar_schema_with_binding(
    src: &wire::ScalarSchema,
    input_columns: usize,
    path: FieldPath,
    binding: SchemaBinding,
) -> Result<ScalarSchema, ProtocolError> {
    let invalid_source = match binding {
        SchemaBinding::NativeSource => src.source_slot.is_none(),
        SchemaBinding::SemanticResult => src.source_slot.is_some(),
    };
    if input_columns != 1
        || invalid_source
        || src.field_nodes.is_empty()
        || src.field_nodes.len() > P::SCHEMA_TYPE_NODES
    {
        return Err(invalid(
            &path,
            match binding {
                SchemaBinding::NativeSource => {
                    "scalar schema requires one bound source and bounded nodes"
                }
                SchemaBinding::SemanticResult => {
                    "semantic result scalar schema requires one unbound source and bounded nodes"
                }
            },
        ));
    }
    let mut overlap =
        size_of::<wire::ScalarSchema>() + size_of::<ScalarSchema>() + size_of::<Vec<bool>>();
    add(
        &mut overlap,
        allocation::<wire::ScalarField>(src.field_nodes.capacity(), &path)?,
        &path,
    )?;
    add(&mut overlap, src.field_nodes.len(), &path)?;
    for node in &src.field_nodes {
        let ty = node
            .value_type
            .as_ref()
            .ok_or_else(|| invalid(&path, "scalar field requires a value type"))?;
        if ty.children.len() > P::MAX_COLUMNS {
            return Err(invalid(&path, "scalar child count exceeds profile"));
        }
        let kind = validate_type(ty, &path)?;
        add(
            &mut overlap,
            allocation::<wire::NamedScalarField>(ty.children.capacity(), &path)?,
            &path,
        )?;
        if let Some(zone) = &ty.time_zone {
            checked_name(zone, &path)?;
            add(&mut overlap, zone.capacity(), &path)?;
            add(&mut overlap, zone.len(), &path)?;
        }
        use wire::ScalarTypeKind as K;
        let neutral = match kind {
            K::ScalarList => size_of::<ScalarField>(),
            K::ScalarMap => 2 * size_of::<ScalarField>(),
            K::ScalarStruct => allocation::<NamedScalarField>(ty.children.len(), &path)?,
            _ => 0,
        };
        add(&mut overlap, neutral, &path)?;
        for child in &ty.children {
            checked_name(&child.name, &path)?;
            add(&mut overlap, child.name.capacity(), &path)?;
            if kind == K::ScalarStruct {
                add(&mut overlap, child.name.len(), &path)?;
            }
        }
    }
    // Every loop and prost encoded_len traversal is finite after capacity and
    // cardinality checks; no unbounded child table can reach this computation.
    if src.encoded_len() > P::SCHEMA_WIRE_BYTES {
        return Err(invalid(&path, "scalar schema wire exceeds profile"));
    }
    let mut seen = Vec::new();
    seen.try_reserve_exact(src.field_nodes.len())
        .map_err(|_| invalid(&path, "cannot reserve scalar seen flags"))?;
    seen.resize(src.field_nodes.len(), false);
    visit(src, src.root_field_node_id, 1, &mut seen, &path)?;
    if seen.iter().any(|visited| !visited) {
        return Err(invalid(&path, "scalar schema contains unreachable nodes"));
    }
    let root = decode_field(src, src.root_field_node_id, &path)?;
    let schema =
        ScalarSchema::try_new(root).map_err(|_| invalid(&path, "invalid bounded scalar schema"))?;
    match binding {
        SchemaBinding::NativeSource => schema
            .bind_native_slots(&[src.source_slot.expect("preflight checked source slot")])
            .map_err(|_| invalid(&path, "invalid scalar source binding")),
        SchemaBinding::SemanticResult => Ok(schema),
    }
}
fn node_at<'a>(
    src: &'a wire::ScalarSchema,
    id: u32,
    path: &FieldPath,
) -> Result<&'a wire::ScalarField, ProtocolError> {
    id.checked_sub(1)
        .and_then(|i| src.field_nodes.get(i as usize))
        .ok_or_else(|| invalid(path, "scalar node reference is zero or out of range"))
}
fn visit(
    src: &wire::ScalarSchema,
    id: u32,
    depth: usize,
    seen: &mut [bool],
    path: &FieldPath,
) -> Result<(), ProtocolError> {
    if depth > P::MAX_DEPTH {
        return Err(invalid(path, "scalar schema exceeds semantic depth"));
    }
    let node = node_at(src, id, path)?;
    let visited = &mut seen[id as usize - 1];
    if *visited {
        return Err(invalid(path, "scalar node is shared or cyclic"));
    }
    *visited = true;
    let ty = node
        .value_type
        .as_ref()
        .ok_or_else(|| invalid(path, "scalar type is missing"))?;
    for child in &ty.children {
        visit(src, child.field_node_id, depth + 1, seen, path)?;
    }
    Ok(())
}
fn copy_string(src: &str, path: &FieldPath) -> Result<String, ProtocolError> {
    let mut out = String::new();
    out.try_reserve_exact(src.len())
        .map_err(|_| invalid(path, "cannot reserve scalar string"))?;
    out.push_str(src);
    Ok(out)
}
fn boxed(field: ScalarField, path: &FieldPath) -> Result<Box<ScalarField>, ProtocolError> {
    let layout = std::alloc::Layout::new::<ScalarField>();
    // SAFETY: ScalarField is nonzero-sized. The checked allocation either
    // returns an aligned unique pointer or fails before initialization. The
    // initialized field is transferred exactly once to its matching Box layout.
    let ptr = unsafe { std::alloc::alloc(layout) }.cast::<ScalarField>();
    if ptr.is_null() {
        return Err(invalid(path, "cannot reserve scalar child box"));
    }
    unsafe {
        ptr.write(field);
        Ok(Box::from_raw(ptr))
    }
}
fn decode_field(
    src: &wire::ScalarSchema,
    id: u32,
    path: &FieldPath,
) -> Result<ScalarField, ProtocolError> {
    let node = node_at(src, id, path)?;
    let ty = node
        .value_type
        .as_ref()
        .ok_or_else(|| invalid(path, "scalar type is missing"))?;
    use wire::ScalarTypeKind as K;
    let kind = validate_type(ty, path)?;
    let child = |i: usize| decode_field(src, ty.children[i].field_node_id, path);
    let value_type = match kind {
        K::ScalarNull => S::Null,
        K::ScalarBoolean => S::Boolean,
        K::ScalarSignedInteger => S::SignedInteger(ty.bits as u16),
        K::ScalarLargeInt => S::LargeInt,
        K::ScalarFloat32 => S::Float32,
        K::ScalarFloat64 => S::Float64,
        K::ScalarDecimal => S::Decimal {
            bits: ty.bits as u16,
            precision: ty.precision as u8,
            scale: ty.scale as u8,
        },
        K::ScalarString => S::String,
        K::ScalarBinary => S::Binary,
        K::ScalarDate => S::Date,
        K::ScalarTimeMicros => S::TimeMicros,
        K::ScalarTimestamp => S::Timestamp {
            unit: match wire::ScalarTimestampUnit::try_from(ty.timestamp_unit) {
                Ok(wire::ScalarTimestampUnit::ScalarMicrosecond) => U::Microsecond,
                Ok(wire::ScalarTimestampUnit::ScalarNanosecond) => U::Nanosecond,
                _ => return Err(invalid(path, "invalid scalar timestamp unit")),
            },
            timezone: ty
                .time_zone
                .as_deref()
                .map(|zone| copy_string(zone, path))
                .transpose()?,
        },
        K::ScalarNativeJson => S::Json,
        K::ScalarVariant => S::Variant,
        K::ScalarOpaque => S::Opaque(match wire::OpaqueKind::try_from(ty.opaque_kind) {
            Ok(wire::OpaqueKind::Hll) => O::Hll,
            Ok(wire::OpaqueKind::Bitmap) => O::Bitmap,
            Ok(wire::OpaqueKind::Object) => O::Object,
            Ok(wire::OpaqueKind::Percentile) => O::Percentile,
            _ => return Err(invalid(path, "invalid scalar opaque identity")),
        }),
        K::ScalarList => S::List(boxed(child(0)?, path)?),
        K::ScalarMap => S::Map {
            key: boxed(child(0)?, path)?,
            value: boxed(child(1)?, path)?,
        },
        K::ScalarStruct => {
            let mut fields = Vec::new();
            fields
                .try_reserve_exact(ty.children.len())
                .map_err(|_| invalid(path, "cannot reserve scalar struct fields"))?;
            for (i, c) in ty.children.iter().enumerate() {
                fields.push(NamedScalarField {
                    name: copy_string(&c.name, path)?,
                    field: child(i)?,
                });
            }
            S::Struct(fields)
        }
        K::Unspecified => return Err(invalid(path, "unspecified scalar type")),
    };
    Ok(ScalarField {
        nullable: node.nullable,
        value_type,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;

    struct Allocator;
    thread_local! {
        static TRACK: Cell<bool> = const { Cell::new(false) };
        static MAX_REQUEST: Cell<usize> = const { Cell::new(0) };
        static FAIL_BOX: Cell<bool> = const { Cell::new(false) };
    }
    #[global_allocator]
    static ALLOCATOR: Allocator = Allocator;
    unsafe impl GlobalAlloc for Allocator {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            let fail = FAIL_BOX
                .try_with(|flag| {
                    flag.get() && layout == Layout::new::<ScalarField>() && flag.replace(false)
                })
                .unwrap_or(false);
            if fail {
                return std::ptr::null_mut();
            }
            if TRACK.try_with(Cell::get).unwrap_or(false) {
                let _ = MAX_REQUEST.try_with(|size| size.set(size.get().max(layout.size())));
            }
            unsafe { System.alloc(layout) }
        }
        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            unsafe { System.dealloc(ptr, layout) }
        }
        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            if TRACK.try_with(Cell::get).unwrap_or(false) {
                let _ = MAX_REQUEST.try_with(|size| size.set(size.get().max(new_size)));
            }
            unsafe { System.realloc(ptr, layout, new_size) }
        }
    }
    fn f(value_type: S, nullable: bool) -> ScalarField {
        ScalarField {
            value_type,
            nullable,
        }
    }
    fn schema(value_type: S) -> ScalarSchema {
        ScalarSchema::try_new(f(value_type, true))
            .unwrap()
            .bind_native_slots(&[42])
            .unwrap()
    }
    fn decode(src: &wire::ScalarSchema) -> Result<ScalarSchema, ProtocolError> {
        decode_scalar_schema(src, 1, FieldPath::root("scalar_schema"))
    }
    fn primitive() -> wire::ScalarSchema {
        encode_scalar_schema(&schema(S::Boolean))
    }
    fn roundtrip(value_type: S) {
        let expected = schema(value_type);
        let encoded = encode_scalar_schema(&expected);
        assert!(encoded.encoded_len() <= expected.wire_capacity_bytes());
        let raw = encoded.encode_to_vec();
        let wire = wire::ScalarSchema::decode(raw.as_slice()).unwrap();
        assert_eq!(decode(&wire).unwrap(), expected);
    }

    #[test]
    fn native_json_uses_its_own_scalar_kind() {
        let expected = schema(S::Json);
        let mut encoded = encode_scalar_schema(&expected);
        assert_eq!(
            encoded.field_nodes[0].value_type.as_ref().unwrap().kind,
            wire::ScalarTypeKind::ScalarNativeJson as i32
        );
        assert_eq!(decode(&encoded).unwrap(), expected);
        encoded.field_nodes[0].value_type.as_mut().unwrap().kind = 13;
        assert!(decode(&encoded).is_err());
    }

    #[test]
    fn all_closed_scalar_leaf_types_roundtrip_exact_native_semantics() {
        for ty in [
            S::Null,
            S::Boolean,
            S::SignedInteger(8),
            S::SignedInteger(16),
            S::SignedInteger(32),
            S::SignedInteger(64),
            S::LargeInt,
            S::Float32,
            S::Float64,
            S::String,
            S::Binary,
            S::Date,
            S::TimeMicros,
            S::Json,
            S::Variant,
            S::Opaque(O::Hll),
            S::Opaque(O::Bitmap),
            S::Opaque(O::Object),
            S::Opaque(O::Percentile),
            S::Decimal {
                bits: 128,
                precision: 38,
                scale: 38,
            },
            S::Decimal {
                bits: 256,
                precision: 76,
                scale: 76,
            },
        ] {
            roundtrip(ty);
        }
        for unit in [U::Microsecond, U::Nanosecond] {
            for timezone in [None, Some("UTC".into()), Some("Asia/Shanghai".into())] {
                roundtrip(S::Timestamp { unit, timezone });
            }
        }
    }

    #[test]
    fn nested_fields_keep_names_order_nullability_and_nullable_map_key() {
        let expected = schema(S::Struct(vec![
            NamedScalarField {
                name: "same".into(),
                field: f(S::List(Box::new(f(S::Json, false))), true),
            },
            NamedScalarField {
                name: "same".into(),
                field: f(
                    S::Map {
                        key: Box::new(f(S::Binary, true)),
                        value: Box::new(f(
                            S::Struct(vec![
                                NamedScalarField {
                                    name: "雪".into(),
                                    field: f(S::Opaque(O::Bitmap), false),
                                },
                                NamedScalarField {
                                    name: "".into(),
                                    field: f(S::Variant, true),
                                },
                            ]),
                            false,
                        )),
                    },
                    true,
                ),
            },
        ]));
        let encoded = encode_scalar_schema(&expected);
        assert_eq!(decode(&encoded).unwrap(), expected);
        assert!(
            encoded
                .field_nodes
                .iter()
                .all(|field| field.value_type.is_some())
        );
        let mut altered = encoded;
        altered.field_nodes[0]
            .value_type
            .as_mut()
            .unwrap()
            .children
            .swap(0, 1);
        assert_ne!(decode(&altered).unwrap(), expected);
    }

    #[test]
    fn source_is_exactly_one_explicit_native_slot_and_valid_root_reference() {
        let mut wire = primitive();
        for width in [0, 2, usize::MAX] {
            assert!(decode_scalar_schema(&wire, width, FieldPath::root("schema")).is_err());
        }
        for slot in [0, u32::MAX] {
            wire.source_slot = Some(slot);
            assert_eq!(decode(&wire).unwrap().source_slot(), Some(slot));
            assert_eq!(decode(&wire).unwrap().source_ordinal(), 0);
        }
        wire.source_slot = None;
        assert!(decode(&wire).is_err());
        wire.source_slot = Some(42);
        for id in [0, 2, u32::MAX] {
            wire.root_field_node_id = id;
            assert!(decode(&wire).is_err());
        }
    }

    #[test]
    fn unknown_missing_and_extraneous_leaf_properties_are_refused() {
        let base = primitive();
        let mut absent = base.clone();
        absent.field_nodes[0].value_type = None;
        assert!(decode(&absent).is_err());
        for kind in [0, 20, -1, i32::MAX] {
            let mut wire = base.clone();
            wire.field_nodes[0].value_type.as_mut().unwrap().kind = kind;
            assert!(decode(&wire).is_err());
        }
        for property in 0..7 {
            let mut wire = base.clone();
            let ty = wire.field_nodes[0].value_type.as_mut().unwrap();
            match property {
                0 => ty.bits = 1,
                1 => ty.precision = 1,
                2 => ty.scale = 1,
                3 => ty.timestamp_unit = 1,
                4 => ty.time_zone = Some("UTC".into()),
                5 => ty.opaque_kind = 1,
                6 => ty.children.push(wire::NamedScalarField {
                    name: "item".into(),
                    field_node_id: 1,
                }),
                _ => unreachable!(),
            }
            assert!(decode(&wire).is_err());
        }
    }

    #[test]
    fn decimal_integer_timestamp_and_opaque_wire_parameters_are_closed() {
        let mut wire = primitive();
        let cases = [
            wire::ScalarValueType {
                kind: wire::ScalarTypeKind::ScalarSignedInteger as i32,
                bits: 128,
                ..Default::default()
            },
            wire::ScalarValueType {
                kind: wire::ScalarTypeKind::ScalarDecimal as i32,
                bits: 128,
                precision: 39,
                ..Default::default()
            },
            wire::ScalarValueType {
                kind: wire::ScalarTypeKind::ScalarDecimal as i32,
                bits: 256,
                precision: 77,
                ..Default::default()
            },
            wire::ScalarValueType {
                kind: wire::ScalarTypeKind::ScalarDecimal as i32,
                bits: 128,
                precision: 0,
                ..Default::default()
            },
            wire::ScalarValueType {
                kind: wire::ScalarTypeKind::ScalarDecimal as i32,
                bits: 256,
                precision: 76,
                scale: 77,
                ..Default::default()
            },
            wire::ScalarValueType {
                kind: wire::ScalarTypeKind::ScalarDecimal as i32,
                bits: 256,
                precision: 76,
                scale: u32::MAX,
                ..Default::default()
            },
            wire::ScalarValueType {
                kind: wire::ScalarTypeKind::ScalarTimestamp as i32,
                timestamp_unit: 0,
                ..Default::default()
            },
            wire::ScalarValueType {
                kind: wire::ScalarTypeKind::ScalarTimestamp as i32,
                timestamp_unit: 3,
                ..Default::default()
            },
            wire::ScalarValueType {
                kind: wire::ScalarTypeKind::ScalarTimestamp as i32,
                timestamp_unit: 1,
                time_zone: Some(String::new()),
                ..Default::default()
            },
            wire::ScalarValueType {
                kind: wire::ScalarTypeKind::ScalarOpaque as i32,
                opaque_kind: 0,
                ..Default::default()
            },
            wire::ScalarValueType {
                kind: wire::ScalarTypeKind::ScalarOpaque as i32,
                opaque_kind: 5,
                ..Default::default()
            },
        ];
        for ty in cases {
            wire.field_nodes[0].value_type = Some(ty);
            assert!(decode(&wire).is_err());
        }
    }

    #[test]
    fn canonical_container_shape_does_not_rename_wire_children() {
        let valid = encode_scalar_schema(&schema(S::Map {
            key: Box::new(f(S::String, true)),
            value: Box::new(f(S::Boolean, false)),
        }));
        for index in 0..2 {
            let mut wire = valid.clone();
            wire.field_nodes[0].value_type.as_mut().unwrap().children[index].name = "wrong".into();
            assert!(decode(&wire).is_err());
        }
        let mut wire = valid.clone();
        wire.field_nodes[0]
            .value_type
            .as_mut()
            .unwrap()
            .children
            .reverse();
        assert!(decode(&wire).is_err());
        let mut wire = valid;
        wire.field_nodes[0]
            .value_type
            .as_mut()
            .unwrap()
            .children
            .pop();
        assert!(decode(&wire).is_err());
        let mut wire = encode_scalar_schema(&schema(S::List(Box::new(f(S::Null, true)))));
        wire.field_nodes[0].value_type.as_mut().unwrap().children[0]
            .name
            .clear();
        assert!(decode(&wire).is_err());
    }

    #[test]
    fn cycles_shared_nodes_unreachable_nodes_and_arbitrary_table_order() {
        let mut cycle = encode_scalar_schema(&schema(S::List(Box::new(f(S::Boolean, false)))));
        cycle.field_nodes[0].value_type.as_mut().unwrap().children[0].field_node_id = 1;
        assert!(decode(&cycle).is_err());
        let mut shared = encode_scalar_schema(&schema(S::Map {
            key: Box::new(f(S::String, true)),
            value: Box::new(f(S::String, true)),
        }));
        shared.field_nodes[0].value_type.as_mut().unwrap().children[1].field_node_id = 2;
        assert!(decode(&shared).is_err());
        let mut unreachable = primitive();
        unreachable
            .field_nodes
            .push(unreachable.field_nodes[0].clone());
        assert!(decode(&unreachable).is_err());
        let expected = schema(S::List(Box::new(f(S::Boolean, false))));
        let mut reordered = encode_scalar_schema(&expected);
        reordered.field_nodes.swap(0, 1);
        reordered.root_field_node_id = 2;
        reordered.field_nodes[1]
            .value_type
            .as_mut()
            .unwrap()
            .children[0]
            .field_node_id = 1;
        assert_eq!(decode(&reordered).unwrap(), expected);
    }

    #[test]
    fn shared_two_node_tree_refuses_without_cycle_or_unreachable_node() {
        let mut wire = encode_scalar_schema(&schema(S::Map {
            key: Box::new(f(S::String, true)),
            value: Box::new(f(S::Boolean, false)),
        }));
        wire.field_nodes[0].value_type.as_mut().unwrap().children[1].field_node_id = 2;
        wire.field_nodes.pop();
        assert_eq!(wire.field_nodes.len(), 2);
        // Both table nodes are reachable, depth is two, and there is no cycle.
        // Only the repeated reference can reject this finite graph before it
        // is materialized as two distinct neutral children.
        assert!(decode(&wire).unwrap_err().detail().contains("shared"));
    }

    #[test]
    fn dto_neutral_and_seen_overlap_refuses_before_target_allocation() {
        let expected = schema(S::Struct(
            (0..1024)
                .map(|_| NamedScalarField {
                    name: "n".repeat(96),
                    field: f(S::Boolean, true),
                })
                .collect(),
        ));
        let mut wire = encode_scalar_schema(&expected);
        let neutral = expected.backing_bytes();
        // Count the actual already-created DTO allocations. The wire field
        // table's spare capacity is independent of its semantic node count.
        let mut dto_other = size_of::<wire::ScalarSchema>();
        for field in &wire.field_nodes {
            let ty = field.value_type.as_ref().unwrap();
            dto_other += ty.children.capacity() * size_of::<wire::NamedScalarField>();
            dto_other += ty
                .children
                .iter()
                .map(|child| child.name.capacity())
                .sum::<usize>();
            dto_other += ty.time_zone.as_ref().map_or(0, String::capacity);
        }
        let desired_capacity =
            (P::SCHEMA_BACKING_BYTES - dto_other - neutral / 2) / size_of::<wire::ScalarField>();
        assert!(desired_capacity > wire.field_nodes.capacity());
        wire.field_nodes
            .reserve_exact(desired_capacity - wire.field_nodes.len());
        let dto = dto_other + wire.field_nodes.capacity() * size_of::<wire::ScalarField>();
        let seen = size_of::<Vec<bool>>() + wire.field_nodes.len();
        assert!(dto < P::SCHEMA_BACKING_BYTES);
        assert!(neutral < P::SCHEMA_BACKING_BYTES);
        assert!(dto + neutral + seen > P::SCHEMA_BACKING_BYTES);
        assert!(wire.encoded_len() <= P::SCHEMA_WIRE_BYTES);
        assert_eq!(wire.field_nodes.len(), 1025);
        let path = FieldPath::root("schema");
        MAX_REQUEST.with(|v| v.set(0));
        TRACK.with(|v| v.set(true));
        let refused = decode_scalar_schema(&wire, 1, path);
        TRACK.with(|v| v.set(false));
        assert!(refused.unwrap_err().detail().contains("backing"));
        // The separately scoped ProtocolError allocates its short detail/path.
        // The 1025B seen table and the much larger target Struct/name backings
        // must never be requested, despite each family's separate legal cap.
        assert!(MAX_REQUEST.with(Cell::get) <= 128);
    }

    #[test]
    fn semantic_depth_maximum_is_accepted_and_plus_one_is_refused() {
        let mut field = f(S::Boolean, true);
        for _ in 1..P::MAX_DEPTH {
            field = f(S::List(Box::new(field)), false);
        }
        let expected = ScalarSchema::try_new(field)
            .unwrap()
            .bind_native_slots(&[42])
            .unwrap();
        let mut wire = encode_scalar_schema(&expected);
        assert_eq!(decode(&wire).unwrap(), expected);
        let id = wire.field_nodes.len() as u32 + 1;
        wire.field_nodes.push(wire::ScalarField {
            nullable: false,
            value_type: Some(wire::ScalarValueType {
                kind: wire::ScalarTypeKind::ScalarList as i32,
                children: vec![wire::NamedScalarField {
                    name: "item".into(),
                    field_node_id: 1,
                }],
                ..Default::default()
            }),
        });
        wire.root_field_node_id = id;
        assert!(decode(&wire).is_err());
    }

    #[test]
    fn name_nodes_and_wire_size_caps_are_checked_without_recursive_prost_depth() {
        roundtrip(S::Struct(vec![NamedScalarField {
            name: "n".repeat(P::MAX_NAME_BYTES),
            field: f(S::Boolean, true),
        }]));
        let mut wire = primitive();
        wire.field_nodes[0].value_type = Some(wire::ScalarValueType {
            kind: wire::ScalarTypeKind::ScalarTimestamp as i32,
            timestamp_unit: 1,
            time_zone: Some("z".repeat(P::MAX_NAME_BYTES + 1)),
            ..Default::default()
        });
        assert!(decode(&wire).is_err());
        let mut wire = primitive();
        wire.field_nodes
            .resize(P::SCHEMA_TYPE_NODES + 1, wire.field_nodes[0].clone());
        assert!(decode(&wire).is_err());
        // Timestamp names have no child-name copy: DTO and target strings
        // remain under the backing ceiling while their aggregate wire is over.
        let mut wire = primitive();
        wire.field_nodes.clear();
        wire.field_nodes.push(wire::ScalarField {
            nullable: true,
            value_type: Some(wire::ScalarValueType {
                kind: wire::ScalarTypeKind::ScalarStruct as i32,
                children: (0..4)
                    .map(|i| wire::NamedScalarField {
                        name: "".into(),
                        field_node_id: i + 2,
                    })
                    .collect(),
                ..Default::default()
            }),
        });
        for _ in 0..4 {
            wire.field_nodes.push(wire::ScalarField {
                nullable: false,
                value_type: Some(wire::ScalarValueType {
                    kind: wire::ScalarTypeKind::ScalarTimestamp as i32,
                    timestamp_unit: 1,
                    time_zone: Some("z".repeat(P::MAX_NAME_BYTES)),
                    ..Default::default()
                }),
            });
        }
        assert!(wire.encoded_len() > P::SCHEMA_WIRE_BYTES);
        assert!(decode(&wire).unwrap_err().detail().contains("wire"));
    }

    #[test]
    fn dto_spare_capacity_refuses_before_seen_or_neutral_tree_allocation() {
        let mut wire = primitive();
        let mut children =
            Vec::with_capacity(P::SCHEMA_BACKING_BYTES / size_of::<wire::NamedScalarField>() + 1);
        children.push(wire::NamedScalarField {
            name: "item".into(),
            field_node_id: 2,
        });
        wire.field_nodes[0].value_type = Some(wire::ScalarValueType {
            kind: wire::ScalarTypeKind::ScalarList as i32,
            children,
            ..Default::default()
        });
        wire.field_nodes.push(primitive().field_nodes.remove(0));
        let path = FieldPath::root("schema");
        MAX_REQUEST.with(|v| v.set(0));
        TRACK.with(|v| v.set(true));
        let refused = decode_scalar_schema(&wire, 1, path);
        TRACK.with(|v| v.set(false));
        assert!(refused.unwrap_err().detail().contains("backing"));
        // Only separately scoped ProtocolError path/detail allocations occur;
        // the huge DTO capacity is not copied or materialized as a tree.
        assert!(MAX_REQUEST.with(Cell::get) <= 128);

        let expected = schema(S::Struct(vec![NamedScalarField {
            name: "a".into(),
            field: f(S::Binary, true),
        }]));
        let compact = encode_scalar_schema(&expected);
        let mut spare = compact.clone();
        spare.field_nodes.reserve_exact(64);
        spare.field_nodes[0]
            .value_type
            .as_mut()
            .unwrap()
            .children
            .reserve_exact(64);
        assert_eq!(decode(&spare).unwrap(), decode(&compact).unwrap());
        let mut spare_name = String::with_capacity(P::SCHEMA_BACKING_BYTES);
        spare_name.push('a');
        spare.field_nodes[0].value_type.as_mut().unwrap().children[0].name = spare_name;
        assert!(decode(&spare).is_err());
    }

    #[test]
    fn child_box_allocation_failure_is_an_explicit_error() {
        let field = f(
            S::Timestamp {
                unit: U::Microsecond,
                timezone: Some("UTC".into()),
            },
            true,
        );
        let path = FieldPath::root("schema");
        FAIL_BOX.with(|v| v.set(true));
        let result = boxed(field, &path);
        FAIL_BOX.with(|v| v.set(false));
        assert!(result.unwrap_err().detail().contains("child box"));
    }
    #[test]
    fn semantic_result_scalar_schema_preserves_unbound_tree_and_native_refusal() {
        let expected = ScalarSchema::try_new(f(S::String, true)).unwrap();
        let wire = encode_scalar_schema(&expected);
        assert_eq!(wire.source_slot, None);
        let path = FieldPath::root("result.scalar_schema");
        let decoded = decode_semantic_result_scalar_schema(&wire, 1, path.clone()).unwrap();
        assert_eq!(decoded, expected);
        let original = decode_scalar_schema(&wire, 1, path).unwrap_err();
        assert!(
            original
                .to_string()
                .contains("scalar schema requires one bound source and bounded nodes")
        );
        let bound = decoded.bind_native_slots(&[37]).unwrap();
        let bound_wire = encode_scalar_schema(&bound);
        assert_eq!(decode(&bound_wire).unwrap(), bound);
        assert!(
            decode_semantic_result_scalar_schema(
                &bound_wire,
                1,
                FieldPath::root("result.scalar_schema")
            )
            .is_err()
        );
    }

    #[test]
    fn semantic_result_scalar_schema_keeps_original_width_and_node_guards() {
        let expected = ScalarSchema::try_new(f(S::String, true)).unwrap();
        let valid = encode_scalar_schema(&expected);
        for columns in [0, 2] {
            assert!(
                decode_semantic_result_scalar_schema(
                    &valid,
                    columns,
                    FieldPath::root("result.scalar_schema")
                )
                .is_err()
            );
        }
        let mut empty = valid.clone();
        empty.field_nodes.clear();
        let mut over = valid.clone();
        over.field_nodes
            .resize(P::SCHEMA_TYPE_NODES + 1, wire::ScalarField::default());
        let mut unreachable = valid.clone();
        unreachable
            .field_nodes
            .push(unreachable.field_nodes[0].clone());
        let mut invalid_root = valid.clone();
        invalid_root.root_field_node_id = 0;
        for invalid in [empty, over, unreachable, invalid_root] {
            assert!(
                decode_semantic_result_scalar_schema(
                    &invalid,
                    1,
                    FieldPath::root("result.scalar_schema")
                )
                .is_err()
            );
        }
    }
}
