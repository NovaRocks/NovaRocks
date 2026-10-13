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

//! Project exact completed root occurrences into a bounded client render schema.
//! Borrowed preflight covers the whole schema before constructing any output.

use arrow::datatypes::{DataType as D, Field, TimeUnit};
use novarocks_physical_plan::{PhysicalPlan, ResultField, ResultValueDomain as Domain};
use novarocks_result_contract::{
    ClientRenderSchema, NamedRenderField, NativeRenderType as N, OpaqueRenderType as O,
    RenderColumn, RenderField, RenderPresentation as R, RenderTimeUnit as U, RootProfileV1 as P,
};
use novarocks_type_contract::ValueLogicalType as V;
use std::mem::size_of;

/// The application supplies the frontend's frozen presentation offset. No
/// renderer consults a backend clock or infers logical domains from names.
pub fn client_render_schema(
    plan: &PhysicalPlan,
    timezone_offset_seconds: i32,
) -> Result<ClientRenderSchema, String> {
    client_render_schema_from_port(plan.result_port(), timezone_offset_seconds)
}

/// Render the same published SQL public declaration, without using computed nullability.
pub fn client_render_schema_from_port(
    port: Option<&novarocks_physical_plan::ResultPort>,
    timezone_offset_seconds: i32,
) -> Result<ClientRenderSchema, String> {
    let result = port.ok_or_else(invalid)?;
    schema(&result.fields, timezone_offset_seconds)
}

fn schema(fields: &[ResultField], offset: i32) -> Result<ClientRenderSchema, String> {
    validate_public_fields(fields)?;
    if fields.is_empty() || fields.len() > P::MAX_COLUMNS || !(-86_399..=86_399).contains(&offset) {
        return Err(invalid());
    }
    let mut bounds = Bounds {
        backing: size_of::<ClientRenderSchema>(),
        wire: 0,
        nodes: 0,
    };
    bounds.add(
        fields
            .len()
            .checked_mul(size_of::<RenderColumn>())
            .ok_or_else(limited)?,
        0,
    )?;
    for field in fields {
        if !field.domain.matches_storage(&field.ty.data_type)
            || matches!(
                (&field.domain, &field.ty.data_type),
                (Domain::Plain, D::LargeBinary)
            )
        {
            return Err(invalid());
        }
        bounds.name(field.alias.as_deref().unwrap_or(&field.name))?;
        preflight(
            &field.ty.data_type,
            domain(field.domain),
            true,
            1,
            &mut bounds,
        )?;
    }
    let mut columns = Vec::with_capacity(fields.len());
    for (ordinal, field) in fields.iter().enumerate() {
        columns.push(RenderColumn {
            source_ordinal: u32::try_from(ordinal).map_err(|_| limited())?,
            source_slot: None,
            name: field.alias.as_deref().unwrap_or(&field.name).to_owned(),
            field: build(
                &field.ty.data_type,
                field.ty.nullable,
                domain(field.domain),
                true,
                offset,
            )?,
        });
    }
    ClientRenderSchema::try_new(columns, fields.len()).map_err(|error| error.to_string())
}

/// Qualify complete public identities without imposing ClientRows' carrier
/// profile on ScalarValue or reconstructing a missing legacy declaration.
pub fn validate_public_result_logical_identities(
    port: Option<&novarocks_physical_plan::ResultPort>,
) -> Result<(), String> {
    if let Some(port) = port {
        validate_public_fields(&port.fields)?;
    }
    Ok(())
}

fn validate_public_fields(fields: &[ResultField]) -> Result<(), String> {
    fn uuid() -> String {
        "public output logical type UUID is unsupported".into()
    }
    fn walk(data_type: &D, depth: usize, nodes: &mut usize) -> Result<(), String> {
        *nodes = nodes.checked_add(1).ok_or_else(limited)?;
        if depth > P::MAX_DEPTH || *nodes > P::SCHEMA_TYPE_NODES {
            return Err(limited());
        }
        fn check_marker(field: &Field) -> Result<(), String> {
            // Other invalid metadata still belongs to the original schema
            // author; this narrow public gate protects complete UUID facts.
            if field
                .metadata()
                .get(novarocks_type_contract::NR_LOGICAL_TYPE_KEY)
                .is_some_and(|value| {
                    value.len() <= P::MAX_NAME_BYTES
                        && V::from_legacy_compatible_metadata_value(value) == Ok(V::Uuid)
                })
            {
                return Err(uuid());
            }
            Ok(())
        }
        fn child(field: &Field, depth: usize, nodes: &mut usize) -> Result<(), String> {
            check_marker(field)?;
            walk(field.data_type(), depth, nodes)
        }
        match data_type {
            D::List(field) | D::LargeList(field) | D::FixedSizeList(field, _) => {
                child(field, depth + 1, nodes)?;
            }
            D::Map(entries, _) => {
                check_marker(entries)?;
                // Entries is the existing Arrow carrier, not a semantic
                // render node. Use the original key/value profile geometry.
                if let D::Struct(fields) = entries.data_type()
                    && fields.len() == 2
                {
                    child(&fields[0], depth + 1, nodes)?;
                    child(&fields[1], depth + 1, nodes)?;
                }
            }
            D::Struct(fields) => {
                for field in fields {
                    child(field, depth + 1, nodes)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    let mut nodes = 0;
    for field in fields {
        if field.ty.logical_type == V::Uuid {
            return Err(uuid());
        }
        walk(&field.ty.data_type, 1, &mut nodes)?;
    }
    Ok(())
}

fn invalid() -> String {
    "client render identity or storage is unsupported or inconsistent".into()
}
fn limited() -> String {
    "client render schema exceeds its bounded profile".into()
}
fn domain(domain: Domain) -> Option<V> {
    match domain {
        Domain::Plain | Domain::Variant => None,
        Domain::Json => Some(V::Json),
        Domain::Hll => Some(V::Hll),
        Domain::Bitmap => Some(V::Bitmap),
        Domain::Object => Some(V::Object),
        Domain::Percentile => Some(V::Percentile),
    }
}
struct Bounds {
    backing: usize,
    wire: usize,
    nodes: usize,
}
impl Bounds {
    fn add(&mut self, backing: usize, wire: usize) -> Result<(), String> {
        self.backing = self.backing.checked_add(backing).ok_or_else(limited)?;
        self.wire = self.wire.checked_add(wire).ok_or_else(limited)?;
        if self.backing > P::SCHEMA_BACKING_BYTES || self.wire > P::SCHEMA_WIRE_BYTES {
            return Err(limited());
        }
        Ok(())
    }
    fn name(&mut self, name: &str) -> Result<(), String> {
        if name.len() > P::MAX_NAME_BYTES {
            return Err(limited());
        }
        self.add(name.len(), name.len().checked_add(16).ok_or_else(limited)?)
    }
    fn node(&mut self, depth: usize) -> Result<(), String> {
        self.nodes = self.nodes.checked_add(1).ok_or_else(limited)?;
        if depth > P::MAX_DEPTH || self.nodes > P::SCHEMA_TYPE_NODES {
            return Err(limited());
        }
        self.add(0, 32)
    }
}

enum Kind {
    Leaf(N),
    Timestamp(U),
    List,
    Map,
    Struct,
}
fn unit(unit: TimeUnit) -> U {
    match unit {
        TimeUnit::Second => U::Second,
        TimeUnit::Millisecond => U::Millisecond,
        TimeUnit::Microsecond => U::Microsecond,
        TimeUnit::Nanosecond => U::Nanosecond,
    }
}
fn kind(data_type: &D, marker: Option<V>) -> Result<Kind, String> {
    if let Some(marker) = marker {
        return Ok(Kind::Leaf(match (marker, data_type) {
            (V::Json, D::Utf8) => N::Json,
            (V::LargeInt, D::FixedSizeBinary(16)) => N::LargeInt,
            (V::Variant, D::LargeBinary) => N::Variant,
            (V::Uuid, _) => return Err("public output logical type UUID is unsupported".into()),
            (V::Hll, D::Binary) => N::Opaque(O::Hll),
            (V::Bitmap, D::Binary) => N::Opaque(O::Bitmap),
            (V::Object, D::Binary) => N::Opaque(O::Object),
            (V::Percentile, D::Binary) => N::Opaque(O::Percentile),
            _ => return Err(invalid()),
        }));
    }
    Ok(match data_type {
        D::Null => Kind::Leaf(N::Null),
        D::Boolean => Kind::Leaf(N::Boolean),
        D::Int8 => Kind::Leaf(N::SignedInteger(8)),
        D::Int16 => Kind::Leaf(N::SignedInteger(16)),
        D::Int32 => Kind::Leaf(N::SignedInteger(32)),
        D::Int64 => Kind::Leaf(N::SignedInteger(64)),
        D::UInt8 => Kind::Leaf(N::UnsignedInteger(8)),
        D::UInt16 => Kind::Leaf(N::UnsignedInteger(16)),
        D::UInt32 => Kind::Leaf(N::UnsignedInteger(32)),
        D::UInt64 => Kind::Leaf(N::UnsignedInteger(64)),
        D::FixedSizeBinary(16) => Kind::Leaf(N::LargeInt),
        D::Float32 => Kind::Leaf(N::Float32),
        D::Float64 => Kind::Leaf(N::Float64),
        D::Decimal128(precision, scale) | D::Decimal256(precision, scale) => {
            let bits = if matches!(data_type, D::Decimal128(..)) {
                128
            } else {
                256
            };
            let max = if bits == 128 { 38 } else { 76 };
            if *precision == 0 || *precision > max || *scale < 0 || *scale as u8 > *precision {
                return Err(invalid());
            }
            Kind::Leaf(N::Decimal {
                bits,
                precision: *precision,
                scale: *scale,
            })
        }
        D::Utf8 => Kind::Leaf(N::String),
        D::Binary => Kind::Leaf(N::Binary),
        // LargeBinary is Native's frozen Variant carrier. Top-level domains
        // were checked separately against ResultField; nested slots carry the
        // Native type codec's existing exact identity.
        D::LargeBinary => Kind::Leaf(N::Variant),
        D::Date32 => Kind::Leaf(N::Date),
        D::Time32(u @ (TimeUnit::Second | TimeUnit::Millisecond))
        | D::Time64(u @ (TimeUnit::Microsecond | TimeUnit::Nanosecond)) => {
            Kind::Leaf(N::Time { unit: unit(*u) })
        }
        D::Timestamp(u, _) => Kind::Timestamp(unit(*u)),
        D::List(_) => Kind::List,
        D::Map(_, false) => Kind::Map,
        D::Struct(_) => Kind::Struct,
        _ => return Err(invalid()),
    })
}
fn child_preflight(field: &Field, depth: usize, bounds: &mut Bounds) -> Result<(), String> {
    preflight(
        field.data_type(),
        super::root_scalar_type::marker(field)?,
        false,
        depth,
        bounds,
    )
}
fn preflight(
    data_type: &D,
    marker: Option<V>,
    top: bool,
    depth: usize,
    bounds: &mut Bounds,
) -> Result<(), String> {
    bounds.node(depth)?;
    // Existing MySQL metadata/row handling has no top-level Decimal256
    // support. Nested container formatting does, so preserve that distinction.
    if top && matches!(data_type, D::Decimal256(..)) {
        return Err(invalid());
    }
    match kind(data_type, marker)? {
        Kind::Leaf(_) => {}
        Kind::Timestamp(_) => {
            let D::Timestamp(_, zone) = data_type else {
                unreachable!()
            };
            if let Some(zone) = zone {
                if zone.is_empty() {
                    return Err(invalid());
                }
                bounds.name(zone)?;
            }
        }
        Kind::List => {
            let D::List(child) = data_type else {
                unreachable!()
            };
            if child.name() != "item" {
                return Err(invalid());
            }
            bounds.add(size_of::<RenderField>(), 0)?;
            child_preflight(child, depth + 1, bounds)?;
        }
        Kind::Map => {
            let D::Map(entries, _) = data_type else {
                unreachable!()
            };
            let (key, value) = super::root_scalar_type::map_children(entries)?;
            bounds.add(2 * size_of::<RenderField>(), 0)?;
            child_preflight(key, depth + 1, bounds)?;
            child_preflight(value, depth + 1, bounds)?;
        }
        Kind::Struct => {
            let D::Struct(fields) = data_type else {
                unreachable!()
            };
            if fields.is_empty() || fields.len() > P::MAX_COLUMNS {
                return Err(invalid());
            }
            bounds.add(
                fields
                    .len()
                    .checked_mul(size_of::<NamedRenderField>())
                    .ok_or_else(limited)?,
                0,
            )?;
            for field in fields {
                bounds.name(field.name())?;
                child_preflight(field, depth + 1, bounds)?;
            }
        }
    }
    Ok(())
}
fn child_build(field: &Field, offset: i32) -> Result<RenderField, String> {
    build(
        field.data_type(),
        field.is_nullable(),
        super::root_scalar_type::marker(field)?,
        false,
        offset,
    )
}
fn build(
    data_type: &D,
    nullable: bool,
    marker: Option<V>,
    top: bool,
    offset: i32,
) -> Result<RenderField, String> {
    let native_type = match kind(data_type, marker)? {
        Kind::Leaf(ty) => ty,
        Kind::Timestamp(unit) => {
            let D::Timestamp(_, zone) = data_type else {
                unreachable!()
            };
            N::Timestamp {
                unit,
                timezone: zone.as_ref().map(|zone| zone.to_string()),
            }
        }
        Kind::List => {
            let D::List(child) = data_type else {
                unreachable!()
            };
            N::List(Box::new(child_build(child, offset)?))
        }
        Kind::Map => {
            let D::Map(entries, _) = data_type else {
                unreachable!()
            };
            let (key, value) = super::root_scalar_type::map_children(entries)?;
            N::Map {
                key: Box::new(child_build(key, offset)?),
                value: Box::new(child_build(value, offset)?),
            }
        }
        Kind::Struct => {
            let D::Struct(fields) = data_type else {
                unreachable!()
            };
            let mut out = Vec::with_capacity(fields.len());
            for field in fields {
                out.push(NamedRenderField {
                    name: field.name().to_owned(),
                    field: child_build(field, offset)?,
                });
            }
            N::Struct(out)
        }
    };
    let presentation = match &native_type {
        N::Timestamp { .. } if top => R::TimestampUtcMicros,
        N::Timestamp { .. } => R::TimestampContainerText,
        N::Json => R::JsonText,
        N::Variant if top => R::VariantSerializedBytes,
        N::Variant => R::VariantJson {
            timezone_offset_seconds: offset,
        },
        N::Opaque(_) => R::OpaqueNull,
        N::List(_) | N::Map { .. } | N::Struct(_) => R::MysqlContainer,
        _ => R::ScalarText,
    };
    Ok(RenderField {
        nullable,
        presentation,
        native_type,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_physical_plan::{ValueId, ValueType};
    use novarocks_types::logical::LogicalType as L;
    use novarocks_types::logical::field_with_logical_type;
    use std::sync::Arc;
    fn output(name: &str, data_type: D, domain: Domain) -> ResultField {
        ResultField {
            name: name.into(),
            alias: None,
            value: ValueId::new(1),
            domain,
            ty: ValueType::new(data_type, true),
        }
    }
    #[test]
    fn public_identity_gate_preserves_original_wide_map_semantic_profile() {
        let map = D::Map(
            Arc::new(Field::new(
                "entries",
                D::Struct(
                    vec![
                        Arc::new(Field::new("key", D::Int32, false)),
                        Arc::new(Field::new("value", D::Int32, true)),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );
        // The original render profile counts each Map and its key/value,
        // without adding the synthetic Arrow entries Struct as a fourth node.
        let fields = (0..2049)
            .map(|_| output("m", map.clone(), Domain::Plain))
            .collect::<Vec<_>>();
        assert!(fields.len() <= P::MAX_COLUMNS);
        assert!(3 * fields.len() <= P::SCHEMA_TYPE_NODES);
        assert!(4 * fields.len() > P::SCHEMA_TYPE_NODES);
        assert_eq!(schema(&fields, 0).unwrap().columns().len(), fields.len());
    }

    #[test]
    fn complete_public_identities_keep_original_declarations_and_refuse_uuid() {
        let mut large = output("large", D::FixedSizeBinary(16), Domain::Plain);
        large.ty.logical_type = V::LargeInt;
        assert_eq!(
            schema(&[large], 0).unwrap().columns()[0].field.native_type,
            N::LargeInt
        );
        let physical = output("uuid", D::FixedSizeBinary(16), Domain::Plain);
        assert_eq!(
            schema(&[physical], 0).unwrap().columns()[0]
                .field
                .native_type,
            N::LargeInt
        );
        let mut json_plain = output("j", D::Utf8, Domain::Plain);
        json_plain.ty.logical_type = V::Json;
        assert_eq!(
            schema(&[json_plain], 0).unwrap().columns()[0]
                .field
                .native_type,
            N::String
        );
        let mut uuid = output("u", D::FixedSizeBinary(16), Domain::Plain);
        uuid.ty.logical_type = V::Uuid;
        let error = schema(&[uuid], 0).unwrap_err();
        assert!(error.contains("UUID") && error.contains("unsupported"));
    }

    #[test]
    fn complete_nested_variant_uses_original_renderer_and_uuid_is_named_refusal() {
        use novarocks_type_contract::NR_LOGICAL_TYPE_KEY;
        for (marker, storage) in [
            ("variant", D::LargeBinary),
            ("uuid", D::FixedSizeBinary(16)),
        ] {
            let child = Field::new("item", storage, true)
                .with_metadata([(NR_LOGICAL_TYPE_KEY.into(), marker.into())].into());
            let field = output("items", D::List(Arc::new(child)), Domain::Plain);
            if marker == "uuid" {
                let error = schema(&[field], 0).unwrap_err();
                assert!(error.contains("UUID") && error.contains("unsupported"));
            } else {
                let result = schema(&[field], 0).unwrap();
                let N::List(child) = &result.columns()[0].field.native_type else {
                    panic!("expected list")
                };
                assert_eq!(child.native_type, N::Variant);
                assert_eq!(
                    child.presentation,
                    R::VariantJson {
                        timezone_offset_seconds: 0
                    }
                );
            }
        }
    }

    #[test]
    fn repeated_values_keep_ordered_occurrences_and_names_never_infer_domains() {
        let mut fields = vec![
            output("bitmap", D::Binary, Domain::Plain),
            output("same", D::Binary, Domain::Bitmap),
            output("same", D::Utf8, Domain::Json),
        ];
        fields[1].alias = Some("display".into());
        let actual = schema(&fields, 0).unwrap();
        assert_eq!(
            actual
                .columns()
                .iter()
                .map(|c| c.source_ordinal)
                .collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        assert!(actual.columns().iter().all(|c| c.source_slot.is_none()));
        assert_eq!(actual.columns()[0].field.native_type, N::Binary);
        assert_eq!(actual.columns()[1].field.native_type, N::Opaque(O::Bitmap));
        assert_eq!(actual.columns()[1].name, "display");
        assert_eq!(actual.columns()[2].field.presentation, R::JsonText);
    }
    #[test]
    fn nested_exact_domains_and_presentation_are_frozen() {
        let nested = D::Struct(
            vec![
                Arc::new(field_with_logical_type(
                    Field::new("j", D::Utf8, true),
                    L::Json,
                )),
                Arc::new(field_with_logical_type(
                    Field::new("h", D::Binary, true),
                    L::Hll,
                )),
                Arc::new(Field::new("v", D::LargeBinary, true)),
                Arc::new(Field::new(
                    "ts",
                    D::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                    true,
                )),
            ]
            .into(),
        );
        let fields = [
            output(
                "top_ts",
                D::Timestamp(TimeUnit::Nanosecond, None),
                Domain::Plain,
            ),
            output("nested", nested, Domain::Plain),
            output("top_v", D::LargeBinary, Domain::Variant),
        ];
        let actual = schema(&fields, 28_800).unwrap();
        assert_eq!(
            actual.columns()[0].field.presentation,
            R::TimestampUtcMicros
        );
        assert_eq!(
            actual.columns()[2].field.presentation,
            R::VariantSerializedBytes
        );
        let N::Struct(children) = &actual.columns()[1].field.native_type else {
            panic!("struct")
        };
        assert_eq!(children[0].field.native_type, N::Json);
        assert_eq!(children[1].field.presentation, R::OpaqueNull);
        assert_eq!(
            children[2].field.presentation,
            R::VariantJson {
                timezone_offset_seconds: 28_800
            }
        );
        assert_eq!(children[3].field.presentation, R::TimestampContainerText);
    }
    #[test]
    fn client_support_keeps_top_level_decimal256_refusal_and_nested_support() {
        assert!(schema(&[output("top", D::Decimal256(40, 4), Domain::Plain)], 0).is_err());
        let nested = D::List(Arc::new(Field::new("item", D::Decimal256(40, 4), true)));
        assert!(schema(&[output("nested", nested, Domain::Plain)], 0).is_ok());
        assert!(
            schema(
                &[output(
                    "invalid_time",
                    D::Time32(TimeUnit::Microsecond),
                    Domain::Plain
                )],
                0
            )
            .is_err()
        );
    }

    #[test]
    fn borrowed_preflight_refuses_the_whole_schema_before_any_owned_build() {
        let mut bounds = Bounds {
            backing: size_of::<ClientRenderSchema>(),
            wire: 0,
            nodes: 0,
        };
        let oversized = "x".repeat(P::MAX_NAME_BYTES + 1);
        assert!(bounds.name(&oversized).is_err());
        let fields = (0..5)
            .map(|_| output(&"x".repeat(P::MAX_NAME_BYTES), D::Utf8, Domain::Plain))
            .collect::<Vec<_>>();
        assert!(schema(&fields, 0).is_err());
        let mut deep = D::Int64;
        for _ in 0..P::MAX_DEPTH {
            deep = D::List(Arc::new(Field::new("item", deep, true)));
        }
        assert!(schema(&[output("deep", deep, Domain::Plain)], 0).is_err());
        assert!(schema(&[output("wrong", D::Binary, Domain::Json)], 0).is_err());
        assert!(
            schema(
                &[output("unproven_variant", D::LargeBinary, Domain::Plain)],
                0
            )
            .is_err()
        );
        assert!(schema(&[output("unsupported", D::Utf8View, Domain::Plain)], 0).is_err());
        let marked = Field::new("item", D::Binary, true).with_metadata(
            [(
                novarocks_types::logical::NR_LOGICAL_TYPE_KEY.into(),
                "unknown".into(),
            )]
            .into(),
        );
        assert!(
            schema(
                &[output("bad", D::List(Arc::new(marked)), Domain::Plain)],
                0
            )
            .is_err()
        );
    }
}
