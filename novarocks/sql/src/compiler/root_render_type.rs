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

//! Project complete immutable root declarations into a bounded render purpose.
//! Borrowed preflight covers the whole schema before constructing any output.

use arrow::datatypes::{DataType as D, Field, TimeUnit};
use novarocks_physical_plan::{PhysicalPlan, ResultField, ResultValueDomain as Domain};
use novarocks_result_contract::{
    ClientRenderSchema, NamedRenderField, NativeRenderType as N, OpaqueRenderType as O,
    RenderColumn, RenderField, RenderPresentation as R, RenderTimeUnit as U, RootProfileV1 as P,
};
use novarocks_type_contract::{ValueKind, ValueLogicalType as V, ValueTypeRef};
use std::mem::size_of;

/// Frozen presentation input authored by the original client session.
#[derive(Clone, Copy, Debug)]
pub enum RenderFamilyRequest<'a> {
    MysqlText { timezone_offset_seconds: i32 },
    TrinoJson(&'a novarocks_result_contract::TrinoJsonFacts),
}

/// A protocol type rejection remains distinct from invalid public declarations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClientRenderSchemaError {
    Invalid(String),
    UnsupportedClientType { column: String, native_type: N },
}
impl From<String> for ClientRenderSchemaError {
    fn from(message: String) -> Self {
        Self::Invalid(message)
    }
}
impl std::fmt::Display for ClientRenderSchemaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid(message) => f.write_str(message),
            Self::UnsupportedClientType {
                column,
                native_type,
            } => write!(
                f,
                "column '{column}' has type {native_type:?}, which is unsupported by the Trino client protocol"
            ),
        }
    }
}
impl std::error::Error for ClientRenderSchemaError {}

/// The application supplies the frozen family and, for MySQL, its local offset.
pub fn client_render_schema(
    plan: &PhysicalPlan,
    family: RenderFamilyRequest<'_>,
) -> Result<ClientRenderSchema, ClientRenderSchemaError> {
    client_render_schema_from_port(plan.result_port(), family)
}

pub fn client_render_schema_from_port(
    port: Option<&novarocks_physical_plan::ResultPort>,
    family: RenderFamilyRequest<'_>,
) -> Result<ClientRenderSchema, ClientRenderSchemaError> {
    let result = port.ok_or_else(invalid)?;
    family_schema(&result.fields, family)
}

fn root_domain_matches(field: &ResultField) -> bool {
    // This legacy root summary can check consistency but cannot author or
    // replace any node of the complete declaration.
    matches!(
        (field.domain, field.ty.logical_type()),
        (Domain::Plain, V::Physical | V::LargeInt | V::Uuid)
            | (Domain::Json, V::Json)
            | (Domain::Variant, V::Variant)
            | (Domain::Hll, V::Hll)
            | (Domain::Bitmap, V::Bitmap)
            | (Domain::Object, V::Object)
            | (Domain::Percentile, V::Percentile)
    )
}
// Design: ADR-0203 (docs/adr/ADR-0203-client-result-presentation-and-trino-paged-delivery.md)
fn family_schema(
    fields: &[ResultField],
    family: RenderFamilyRequest<'_>,
) -> Result<ClientRenderSchema, ClientRenderSchemaError> {
    validate_public_fields(fields)?;
    if fields.is_empty()
        || fields.len() > P::MAX_COLUMNS
        || matches!(family, RenderFamilyRequest::MysqlText { timezone_offset_seconds } if !(-86_399..=86_399).contains(&timezone_offset_seconds))
    {
        return Err(invalid().into());
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
        if !root_domain_matches(field) {
            return Err(invalid().into());
        }
        bounds.name(field.alias.as_deref().unwrap_or(&field.name))?;
        preflight(
            field.ty.value_type(),
            field.ty.data_type(),
            true,
            1,
            &mut bounds,
        )?;
    }
    if matches!(family, RenderFamilyRequest::TrinoJson(_)) {
        for field in fields {
            validate_trino_type(
                field.ty.value_type(),
                field.ty.data_type(),
                field.alias.as_deref().unwrap_or(&field.name),
            )?;
        }
    }
    let mut columns = Vec::with_capacity(fields.len());
    for (ordinal, field) in fields.iter().enumerate() {
        columns.push(RenderColumn {
            source_ordinal: u32::try_from(ordinal).map_err(|_| limited())?,
            source_slot: None,
            name: field.alias.as_deref().unwrap_or(&field.name).to_owned(),
            field: build(field.ty.value_type(), field.ty.data_type(), true, family)?,
        });
    }
    ClientRenderSchema::try_new(
        match family {
            RenderFamilyRequest::MysqlText { .. } => {
                novarocks_result_contract::ClientPresentationFamily::MysqlText
            }
            RenderFamilyRequest::TrinoJson(facts) => {
                novarocks_result_contract::ClientPresentationFamily::TrinoJson(*facts)
            }
        },
        columns,
        fields.len(),
    )
    .map_err(|error| ClientRenderSchemaError::Invalid(error.to_string()))
}
/// Qualify the complete public declaration without imposing a renderer's
/// carrier profile or recovering domains from source annotations.
pub fn validate_public_result_logical_identities(
    port: Option<&novarocks_physical_plan::ResultPort>,
) -> Result<(), String> {
    if let Some(port) = port {
        validate_public_fields(&port.fields)?;
    }
    Ok(())
}

fn validate_public_fields(fields: &[ResultField]) -> Result<(), String> {
    fn walk(value: &ValueTypeRef, depth: usize, nodes: &mut usize) -> Result<(), String> {
        *nodes = nodes.checked_add(1).ok_or_else(limited)?;
        if depth > P::MAX_DEPTH || *nodes > P::SCHEMA_TYPE_NODES {
            return Err(limited());
        }
        if matches!(value.kind(), ValueKind::Nominal(V::Uuid)) {
            return Err("public output logical type UUID is unsupported".into());
        }
        for index in 0..value.child_count() {
            walk(
                value.child_type_at(index).expect("validated child ordinal"),
                depth + 1,
                nodes,
            )?;
        }
        Ok(())
    }
    let mut nodes = 0;
    for field in fields {
        walk(field.ty.value_type(), 1, &mut nodes)?;
    }
    Ok(())
}

fn invalid() -> String {
    "client render identity or storage is unsupported or inconsistent".into()
}
fn limited() -> String {
    "client render schema exceeds its bounded profile".into()
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
fn kind(value: &ValueTypeRef, layout: &D) -> Result<Kind, String> {
    Ok(match value.kind() {
        ValueKind::String if matches!(layout, D::Utf8 | D::LargeUtf8) => Kind::Leaf(N::String),
        ValueKind::Binary if matches!(layout, D::Binary | D::LargeBinary) => Kind::Leaf(N::Binary),
        ValueKind::Nominal(domain) => Kind::Leaf(match (domain, layout) {
            (V::Json, D::Utf8) => N::Json,
            (V::Variant, D::LargeBinary) => N::Variant,
            (V::LargeInt, D::FixedSizeBinary(16)) => N::LargeInt,
            (V::Hll, D::Binary) => N::Opaque(O::Hll),
            (V::Bitmap, D::Binary) => N::Opaque(O::Bitmap),
            (V::Object, D::Binary) => N::Opaque(O::Object),
            (V::Percentile, D::Binary) => N::Opaque(O::Percentile),
            _ => return Err(invalid()),
        }),
        ValueKind::Scalar(scalar) => match scalar {
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
            D::Float32 => Kind::Leaf(N::Float32),
            D::Float64 => Kind::Leaf(N::Float64),
            D::Decimal128(precision, scale) => Kind::Leaf(N::Decimal {
                bits: 128,
                precision: *precision,
                scale: *scale,
            }),
            D::Decimal256(precision, scale) => Kind::Leaf(N::Decimal {
                bits: 256,
                precision: *precision,
                scale: *scale,
            }),
            D::Date32 => Kind::Leaf(N::Date),
            D::Time32(u @ (TimeUnit::Second | TimeUnit::Millisecond))
            | D::Time64(u @ (TimeUnit::Microsecond | TimeUnit::Nanosecond)) => {
                Kind::Leaf(N::Time { unit: unit(*u) })
            }
            D::Timestamp(u, _) => Kind::Timestamp(unit(*u)),
            _ => return Err(invalid()),
        },
        ValueKind::List(_) if matches!(layout, D::List(_) | D::LargeList(_)) => Kind::List,
        ValueKind::Map { .. } if matches!(layout, D::Map(_, false)) => Kind::Map,
        ValueKind::Struct(_) if matches!(layout, D::Struct(_)) => Kind::Struct,
        // Complete value identity does not install a renderer capability.
        ValueKind::FixedSizeList { .. } | ValueKind::Union(_) => return Err(invalid()),
        _ => return Err(invalid()),
    })
}
fn list_layout(layout: &D) -> Result<&Field, String> {
    match layout {
        D::List(field) | D::LargeList(field) if field.name() == "item" => Ok(field),
        _ => Err(invalid()),
    }
}
fn preflight(
    value: &ValueTypeRef,
    layout: &D,
    top: bool,
    depth: usize,
    bounds: &mut Bounds,
) -> Result<(), String> {
    bounds.node(depth)?;
    if top && matches!(value.kind(), ValueKind::Scalar(D::Decimal256(..))) {
        return Err(invalid());
    }
    match kind(value, layout)? {
        Kind::Leaf(_) => {}
        Kind::Timestamp(_) => {
            let D::Timestamp(_, zone) = layout else {
                return Err(invalid());
            };
            if let Some(zone) = zone {
                if zone.is_empty() {
                    return Err(invalid());
                }
                bounds.name(zone)?;
            }
        }
        Kind::List => {
            let ValueKind::List(child) = value.kind() else {
                return Err(invalid());
            };
            let field = list_layout(layout)?;
            bounds.add(size_of::<RenderField>(), 0)?;
            preflight(child, field.data_type(), false, depth + 1, bounds)?;
        }
        Kind::Map => {
            let (ValueKind::Map { key, value }, D::Map(entries, false)) = (value.kind(), layout)
            else {
                return Err(invalid());
            };
            let (key_layout, value_layout) = super::root_scalar_type::map_children(entries)?;
            bounds.add(2 * size_of::<RenderField>(), 0)?;
            preflight(key, key_layout.data_type(), false, depth + 1, bounds)?;
            preflight(value, value_layout.data_type(), false, depth + 1, bounds)?;
        }
        Kind::Struct => {
            let (ValueKind::Struct(fields), D::Struct(layouts)) = (value.kind(), layout) else {
                return Err(invalid());
            };
            if fields.is_empty() || fields.len() > P::MAX_COLUMNS || fields.len() != layouts.len() {
                return Err(invalid());
            }
            bounds.add(
                fields
                    .len()
                    .checked_mul(size_of::<NamedRenderField>())
                    .ok_or_else(limited)?,
                0,
            )?;
            for (field, layout) in fields.iter().zip(layouts) {
                bounds.name(field.name())?;
                preflight(
                    field.value_type(),
                    layout.data_type(),
                    false,
                    depth + 1,
                    bounds,
                )?;
            }
        }
    }
    Ok(())
}
fn validate_trino_type(
    value: &ValueTypeRef,
    layout: &D,
    column: &str,
) -> Result<(), ClientRenderSchemaError> {
    match kind(value, layout)? {
        Kind::Leaf(native_type) => {
            if matches!(
                native_type,
                N::LargeInt | N::UnsignedInteger(_) | N::Opaque(_)
            ) || matches!(native_type, N::Decimal { precision, .. } if precision > 38)
            {
                return Err(ClientRenderSchemaError::UnsupportedClientType {
                    column: column.to_owned(),
                    native_type,
                });
            }
        }
        Kind::Timestamp(_) => {}
        Kind::List => {
            let ValueKind::List(child) = value.kind() else {
                return Err(invalid().into());
            };
            validate_trino_type(child, list_layout(layout)?.data_type(), column)?;
        }
        Kind::Map => {
            let (ValueKind::Map { key, value }, D::Map(entries, false)) = (value.kind(), layout)
            else {
                return Err(invalid().into());
            };
            let (key_layout, value_layout) = super::root_scalar_type::map_children(entries)?;
            validate_trino_type(key, key_layout.data_type(), column)?;
            validate_trino_type(value, value_layout.data_type(), column)?;
        }
        Kind::Struct => {
            let (ValueKind::Struct(fields), D::Struct(layouts)) = (value.kind(), layout) else {
                return Err(invalid().into());
            };
            for (field, layout) in fields.iter().zip(layouts) {
                validate_trino_type(field.value_type(), layout.data_type(), column)?;
            }
        }
    }
    Ok(())
}
fn build(
    value: &ValueTypeRef,
    layout: &D,
    top: bool,
    family: RenderFamilyRequest<'_>,
) -> Result<RenderField, String> {
    let native_type = match kind(value, layout)? {
        Kind::Leaf(value) => value,
        Kind::Timestamp(unit) => {
            let D::Timestamp(_, zone) = layout else {
                return Err(invalid());
            };
            N::Timestamp {
                unit,
                timezone: zone.as_ref().map(|zone| zone.to_string()),
            }
        }
        Kind::List => {
            let ValueKind::List(child) = value.kind() else {
                return Err(invalid());
            };
            N::List(Box::new(build(
                child,
                list_layout(layout)?.data_type(),
                false,
                family,
            )?))
        }
        Kind::Map => {
            let (ValueKind::Map { key, value }, D::Map(entries, false)) = (value.kind(), layout)
            else {
                return Err(invalid());
            };
            let (key_layout, value_layout) = super::root_scalar_type::map_children(entries)?;
            N::Map {
                key: Box::new(build(key, key_layout.data_type(), false, family)?),
                value: Box::new(build(value, value_layout.data_type(), false, family)?),
            }
        }
        Kind::Struct => {
            let (ValueKind::Struct(fields), D::Struct(layouts)) = (value.kind(), layout) else {
                return Err(invalid());
            };
            let mut out = Vec::with_capacity(fields.len());
            for (field, layout) in fields.iter().zip(layouts) {
                out.push(NamedRenderField {
                    name: field.name().to_owned(),
                    field: build(field.value_type(), layout.data_type(), false, family)?,
                });
            }
            N::Struct(out)
        }
    };
    let presentation = match family {
        RenderFamilyRequest::TrinoJson(_) => R::TrinoValue,
        RenderFamilyRequest::MysqlText {
            timezone_offset_seconds: offset,
        } => match &native_type {
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
        },
    };
    Ok(RenderField {
        nullable: value.nullable(),
        presentation,
        native_type,
    })
}
#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_physical_plan::{ValueId, ValueType};
    use novarocks_type_contract::{ValueField, ValueLayout, ValueTypeStorageLifetime};
    use std::sync::Arc;
    fn schema(fields: &[ResultField], offset: i32) -> Result<ClientRenderSchema, String> {
        family_schema(
            fields,
            RenderFamilyRequest::MysqlText {
                timezone_offset_seconds: offset,
            },
        )
        .map_err(|error| error.to_string())
    }
    fn output_type(name: &str, ty: ValueType, domain: Domain) -> ResultField {
        ResultField {
            name: name.into(),
            alias: None,
            value: ValueId::new(1),
            domain,
            ty,
        }
    }
    fn output(name: &str, data_type: D, domain: Domain) -> ResultField {
        let logical = match domain {
            Domain::Plain => V::Physical,
            Domain::Json => V::Json,
            Domain::Variant => V::Variant,
            Domain::Hll => V::Hll,
            Domain::Bitmap => V::Bitmap,
            Domain::Object => V::Object,
            Domain::Percentile => V::Percentile,
        };
        output_type(
            name,
            ValueType::try_with_logical_type(data_type, true, logical).unwrap(),
            domain,
        )
    }
    fn structure(children: &[(&str, ValueType)]) -> ValueType {
        let storage = ValueTypeStorageLifetime::pure();
        let semantic = ValueTypeRef::try_new(
            ValueKind::Struct(
                children
                    .iter()
                    .map(|(name, ty)| {
                        ValueField::new(*name, ty.value_type().clone(), storage.clone())
                    })
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            ),
            true,
            storage.clone(),
        )
        .unwrap();
        let layout = D::Struct(
            children
                .iter()
                .map(|(name, ty)| Field::new(*name, ty.data_type().clone(), ty.nullable()))
                .collect::<Vec<_>>()
                .into(),
        );
        ValueType::try_from_value(semantic, ValueLayout::try_new(&layout, storage).unwrap())
            .unwrap()
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
        large.ty =
            ValueType::try_with_logical_type(D::FixedSizeBinary(16), true, V::LargeInt).unwrap();
        assert_eq!(
            schema(&[large], 0).unwrap().columns()[0].field.native_type,
            N::LargeInt
        );
        let physical = output_type(
            "large",
            ValueType::try_with_logical_type(D::FixedSizeBinary(16), true, V::LargeInt).unwrap(),
            Domain::Plain,
        );
        assert_eq!(
            schema(&[physical], 0).unwrap().columns()[0]
                .field
                .native_type,
            N::LargeInt
        );
        let json_plain = output("j", D::Utf8, Domain::Plain);
        assert_eq!(
            schema(&[json_plain], 0).unwrap().columns()[0]
                .field
                .native_type,
            N::String
        );
        let mut uuid = output("u", D::FixedSizeBinary(16), Domain::Plain);
        uuid.ty = ValueType::try_with_logical_type(D::FixedSizeBinary(16), true, V::Uuid).unwrap();
        let error = schema(&[uuid], 0).unwrap_err();
        assert!(error.contains("UUID") && error.contains("unsupported"));
    }

    #[test]
    fn complete_nested_variant_uses_original_renderer_and_uuid_is_named_refusal() {
        for (marker, storage, nominal) in [
            ("variant", D::LargeBinary, V::Variant),
            ("uuid", D::FixedSizeBinary(16), V::Uuid),
        ] {
            let hold = ValueTypeStorageLifetime::pure();
            let child = ValueType::try_with_logical_type(storage.clone(), true, nominal).unwrap();
            let semantic = ValueTypeRef::try_new(
                ValueKind::List(child.value_type().clone()),
                true,
                hold.clone(),
            )
            .unwrap();
            let layout = D::List(Arc::new(Field::new("item", storage, true)));
            let ty =
                ValueType::try_from_value(semantic, ValueLayout::try_new(&layout, hold).unwrap())
                    .unwrap();
            let field = output_type("items", ty, Domain::Plain);
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
        let nested = structure(&[
            (
                "j",
                ValueType::try_with_logical_type(D::Utf8, false, V::Json).unwrap(),
            ),
            (
                "h",
                ValueType::try_with_logical_type(D::Binary, true, V::Hll).unwrap(),
            ),
            (
                "v",
                ValueType::try_with_logical_type(D::LargeBinary, true, V::Variant).unwrap(),
            ),
            (
                "ts",
                ValueType::new(D::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())), true),
            ),
        ]);
        let fields = [
            output(
                "top_ts",
                D::Timestamp(TimeUnit::Nanosecond, None),
                Domain::Plain,
            ),
            output_type("nested", nested, Domain::Plain),
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
        assert!(!children[0].field.nullable);
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
            ValueType::try_with_logical_type(D::Time32(TimeUnit::Microsecond), true, V::Physical)
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
        assert!(bounds.name(&"x".repeat(P::MAX_NAME_BYTES + 1)).is_err());
        let fields = (0..5)
            .map(|_| output(&"x".repeat(P::MAX_NAME_BYTES), D::Utf8, Domain::Plain))
            .collect::<Vec<_>>();
        assert!(schema(&fields, 0).is_err());
        let mut deep = D::Int64;
        for _ in 0..P::MAX_DEPTH {
            deep = D::List(Arc::new(Field::new("item", deep, true)));
        }
        assert!(ValueType::try_with_logical_type(deep, true, V::Physical).is_err());
        let wrong = output_type("wrong", ValueType::new(D::Binary, true), Domain::Json);
        assert!(schema(&[wrong], 0).is_err());
        assert!(schema(&[output("unsupported", D::Utf8View, Domain::Plain)], 0).is_err());
    }
    #[test]
    fn complete_semantics_distinguish_equal_carriers_without_metadata_or_width_guesses() {
        let raw = ValueType::new(D::FixedSizeBinary(16), true);
        let integer =
            ValueType::try_with_logical_type(D::FixedSizeBinary(16), true, V::LargeInt).unwrap();
        assert_eq!(raw.data_type(), integer.data_type());
        assert!(schema(&[output_type("bytes", raw, Domain::Plain)], 0).is_err());
        assert_eq!(
            schema(&[output_type("integer", integer, Domain::Plain)], 0)
                .unwrap()
                .columns()[0]
                .field
                .native_type,
            N::LargeInt
        );
        assert_eq!(
            schema(&[output("bytes", D::LargeBinary, Domain::Plain)], 0)
                .unwrap()
                .columns()[0]
                .field
                .native_type,
            N::Binary
        );
        assert_eq!(
            schema(&[output("variant", D::LargeBinary, Domain::Variant)], 0)
                .unwrap()
                .columns()[0]
                .field
                .native_type,
            N::Variant
        );
        let marked =
            Field::new("item", D::Binary, true).with_metadata(std::collections::HashMap::<
                String,
                String,
            >::from([(
                novarocks_type_contract::NR_LOGICAL_TYPE_KEY.into(),
                "unknown-source-attribute".into(),
            )]));
        let actual = schema(
            &[output("source", D::List(Arc::new(marked)), Domain::Plain)],
            0,
        )
        .unwrap();
        let N::List(child) = &actual.columns()[0].field.native_type else {
            panic!("list")
        };
        assert_eq!(child.native_type, N::Binary);
    }
    #[test]
    fn metadata_free_struct_map_list_children_keep_domains_and_null_contracts() {
        let storage = ValueTypeStorageLifetime::pure();
        let json = ValueType::try_with_logical_type(D::Utf8, false, V::Json).unwrap();
        let key = ValueType::new(D::Utf8, false);
        let list_layout = D::LargeList(Arc::new(Field::new("item", D::Utf8, false)));
        let list = ValueType::try_from_value(
            ValueTypeRef::try_new(
                ValueKind::List(json.value_type().clone()),
                true,
                storage.clone(),
            )
            .unwrap(),
            ValueLayout::try_new(&list_layout, storage.clone()).unwrap(),
        )
        .unwrap();
        let map_layout = D::Map(
            Arc::new(Field::new(
                "entries",
                D::Struct(
                    vec![
                        Field::new("key", D::Utf8, false),
                        Field::new("value", list_layout, true),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );
        let map = ValueType::try_from_value(
            ValueTypeRef::try_new(
                ValueKind::Map {
                    key: key.value_type().clone(),
                    value: list.value_type().clone(),
                },
                true,
                storage.clone(),
            )
            .unwrap(),
            ValueLayout::try_new(&map_layout, storage).unwrap(),
        )
        .unwrap();
        let nested = structure(&[("pairs", map)]);
        let plain = ValueType::new(nested.data_type().clone(), nested.nullable());
        assert_eq!(plain.data_type(), nested.data_type());
        let field = |ty| {
            schema(&[output_type("container", ty, Domain::Plain)], 0)
                .unwrap()
                .columns()[0]
                .field
                .clone()
        };
        let nominal = field(nested);
        let physical = field(plain);
        assert_ne!(nominal, physical);
        let N::Struct(fields) = nominal.native_type else {
            panic!("struct")
        };
        let N::Map { key, value } = &fields[0].field.native_type else {
            panic!("map")
        };
        assert_eq!(key.native_type, N::String);
        assert!(!key.nullable);
        let N::List(child) = &value.native_type else {
            panic!("list")
        };
        assert_eq!(child.native_type, N::Json);
        assert!(!child.nullable);
        assert!(value.nullable);
    }

    #[test]
    fn typed_uninstalled_uuid_fixed_list_and_union_fail_at_authoring() {
        let uuid = ValueType::try_with_logical_type(D::FixedSizeBinary(16), true, V::Uuid).unwrap();
        let fixed = ValueType::new(
            D::FixedSizeList(Arc::new(Field::new("item", D::Int64, true)), 2),
            true,
        );
        let union = ValueType::new(
            D::Union(
                arrow::datatypes::UnionFields::try_new([2], [Field::new("item", D::Int64, true)])
                    .unwrap(),
                arrow::datatypes::UnionMode::Dense,
            ),
            true,
        );
        for ty in [uuid, fixed, union] {
            assert!(schema(&[output_type("unsupported", ty, Domain::Plain)], 0).is_err());
        }
    }

    fn port(fields: Vec<ResultField>) -> novarocks_physical_plan::ResultPort {
        novarocks_physical_plan::ResultPort {
            scalar_schema: None,
            fragment: novarocks_physical_plan::FragmentId::new(1),
            output: novarocks_physical_plan::OutputPort {
                node: novarocks_physical_plan::NodeId::new(1),
                columns: fields.iter().map(|field| field.value).collect(),
            },
            fields: fields.into_boxed_slice(),
        }
    }

    #[test]
    fn same_public_port_freezes_client_family_and_every_nested_presentation() {
        let nested = structure(&[
            (
                "json",
                ValueType::try_with_logical_type(D::Utf8, false, V::Json).unwrap(),
            ),
            (
                "variant",
                ValueType::try_with_logical_type(D::LargeBinary, true, V::Variant).unwrap(),
            ),
            (
                "timestamps",
                ValueType::new(
                    D::List(Arc::new(Field::new(
                        "item",
                        D::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                        false,
                    ))),
                    true,
                ),
            ),
            (
                "pairs",
                ValueType::new(
                    D::Map(
                        Arc::new(Field::new(
                            "entries",
                            D::Struct(
                                vec![
                                    Field::new("key", D::Utf8, false),
                                    Field::new("value", D::Decimal128(38, 6), true),
                                ]
                                .into(),
                            ),
                            false,
                        )),
                        false,
                    ),
                    true,
                ),
            ),
        ]);
        let mut aliased = output_type("source_name", nested, Domain::Plain);
        aliased.alias = Some("display_name".into());
        let port = port(vec![
            output(
                "time",
                D::Timestamp(TimeUnit::Nanosecond, None),
                Domain::Plain,
            ),
            aliased,
            output("variant", D::LargeBinary, Domain::Variant),
        ]);
        let facts = novarocks_result_contract::TrinoJsonFacts {
            parametric_datetime: true,
            variant: true,
            variant_binary: true,
        };
        let mysql = client_render_schema_from_port(
            Some(&port),
            RenderFamilyRequest::MysqlText {
                timezone_offset_seconds: 28_800,
            },
        )
        .unwrap();
        let trino =
            client_render_schema_from_port(Some(&port), RenderFamilyRequest::TrinoJson(&facts))
                .unwrap();
        assert_eq!(
            mysql.family(),
            &novarocks_result_contract::ClientPresentationFamily::MysqlText
        );
        assert_eq!(
            trino.family(),
            &novarocks_result_contract::ClientPresentationFamily::TrinoJson(facts)
        );
        assert_eq!(mysql.columns()[0].field.presentation, R::TimestampUtcMicros);
        assert_eq!(mysql.columns()[1].field.presentation, R::MysqlContainer);
        assert_eq!(
            mysql.columns()[2].field.presentation,
            R::VariantSerializedBytes
        );
        assert_eq!(trino.columns()[1].name, "display_name");
        assert_eq!(trino.columns()[1].source_ordinal, 1);
        fn assert_trino(field: &RenderField) {
            assert_eq!(field.presentation, R::TrinoValue);
            match &field.native_type {
                N::List(child) => assert_trino(child),
                N::Map { key, value } => {
                    assert_trino(key);
                    assert_trino(value);
                }
                N::Struct(fields) => fields.iter().for_each(|field| assert_trino(&field.field)),
                _ => {}
            }
        }
        for column in trino.columns() {
            assert_trino(&column.field);
        }
        let N::Struct(fields) = &trino.columns()[1].field.native_type else {
            panic!("struct")
        };
        assert_eq!(fields[0].field.native_type, N::Json);
        assert!(!fields[0].field.nullable);
        assert_eq!(fields[1].field.native_type, N::Variant);
        let N::List(child) = &fields[2].field.native_type else {
            panic!("list")
        };
        assert!(!child.nullable);
        assert_eq!(
            child.native_type,
            N::Timestamp {
                unit: U::Nanosecond,
                timezone: Some("UTC".into())
            }
        );
    }

    #[test]
    fn trino_d13_rejections_keep_native_type_and_aliased_output_column() {
        let mut cases = vec![
            (
                ValueType::try_with_logical_type(D::FixedSizeBinary(16), true, V::LargeInt)
                    .unwrap(),
                Domain::Plain,
                N::LargeInt,
            ),
            (
                ValueType::new(D::UInt8, true),
                Domain::Plain,
                N::UnsignedInteger(8),
            ),
            (
                ValueType::new(D::UInt16, true),
                Domain::Plain,
                N::UnsignedInteger(16),
            ),
            (
                ValueType::new(D::UInt32, true),
                Domain::Plain,
                N::UnsignedInteger(32),
            ),
            (
                ValueType::new(D::UInt64, true),
                Domain::Plain,
                N::UnsignedInteger(64),
            ),
        ];
        for (logical, domain, opaque) in [
            (V::Hll, Domain::Hll, O::Hll),
            (V::Bitmap, Domain::Bitmap, O::Bitmap),
            (V::Object, Domain::Object, O::Object),
            (V::Percentile, Domain::Percentile, O::Percentile),
        ] {
            cases.push((
                ValueType::try_with_logical_type(D::Binary, true, logical).unwrap(),
                domain,
                N::Opaque(opaque),
            ));
        }
        cases.push((
            ValueType::new(
                D::List(Arc::new(Field::new("item", D::Decimal256(40, 4), true))),
                true,
            ),
            Domain::Plain,
            N::Decimal {
                bits: 256,
                precision: 40,
                scale: 4,
            },
        ));
        let facts = novarocks_result_contract::TrinoJsonFacts::default();
        for (ty, domain, expected) in cases {
            for nested in [false, true] {
                let mut field = if nested {
                    output_type(
                        "raw_name",
                        structure(&[("inner", ty.clone())]),
                        Domain::Plain,
                    )
                } else {
                    output_type("raw_name", ty.clone(), domain)
                };
                field.alias = Some("displayed_alias".into());
                let port = port(vec![field]);
                assert!(
                    client_render_schema_from_port(
                        Some(&port),
                        RenderFamilyRequest::MysqlText {
                            timezone_offset_seconds: 0
                        }
                    )
                    .is_ok()
                );
                let error = client_render_schema_from_port(
                    Some(&port),
                    RenderFamilyRequest::TrinoJson(&facts),
                )
                .unwrap_err();
                assert_eq!(
                    error,
                    ClientRenderSchemaError::UnsupportedClientType {
                        column: "displayed_alias".into(),
                        native_type: expected.clone(),
                    }
                );
                let message = error.to_string();
                assert!(message.contains("displayed_alias"));
                assert!(message.contains(&format!("{expected:?}")));
                assert!(message.contains("Trino client protocol"));
            }
        }
    }

    #[test]
    fn trino_decimal_admission_uses_precision_without_changing_nested_carrier() {
        let facts = novarocks_result_contract::TrinoJsonFacts::default();
        for bits in [128, 256] {
            let decimal = if bits == 128 {
                D::Decimal128(38, 6)
            } else {
                D::Decimal256(38, 6)
            };
            let port = port(vec![output(
                "bounded_decimal",
                D::List(Arc::new(Field::new("item", decimal, true))),
                Domain::Plain,
            )]);
            let schema =
                client_render_schema_from_port(Some(&port), RenderFamilyRequest::TrinoJson(&facts))
                    .unwrap();
            let N::List(child) = &schema.columns()[0].field.native_type else {
                panic!("list")
            };
            assert_eq!(
                child.native_type,
                N::Decimal {
                    bits,
                    precision: 38,
                    scale: 6
                }
            );
            assert_eq!(child.presentation, R::TrinoValue);
        }
    }

    #[test]
    fn client_family_preserves_existing_unsupported_declaration_errors() {
        let facts = novarocks_result_contract::TrinoJsonFacts::default();
        let types = [
            ValueType::try_with_logical_type(D::FixedSizeBinary(16), true, V::Uuid).unwrap(),
            ValueType::new(
                D::FixedSizeList(Arc::new(Field::new("item", D::Int64, true)), 2),
                true,
            ),
            ValueType::new(
                D::Union(
                    arrow::datatypes::UnionFields::try_new(
                        [2],
                        [Field::new("item", D::Int64, true)],
                    )
                    .unwrap(),
                    arrow::datatypes::UnionMode::Dense,
                ),
                true,
            ),
            ValueType::new(D::Decimal256(40, 4), true),
        ];
        for ty in types {
            let port = port(vec![output_type("original", ty, Domain::Plain)]);
            let mysql = client_render_schema_from_port(
                Some(&port),
                RenderFamilyRequest::MysqlText {
                    timezone_offset_seconds: 0,
                },
            )
            .unwrap_err();
            let trino =
                client_render_schema_from_port(Some(&port), RenderFamilyRequest::TrinoJson(&facts))
                    .unwrap_err();
            assert!(matches!(mysql, ClientRenderSchemaError::Invalid(_)));
            assert_eq!(trino, mysql);
        }
    }
}
