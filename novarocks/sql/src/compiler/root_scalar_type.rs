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

//! SQL-owned exact ScalarValueV1 type projection. Borrowed preflight validates
//! the complete source before constructing output strings, vectors or boxes.
//! These are schema limits, not a runtime allocation grant.

use arrow::datatypes::{DataType as D, Field, TimeUnit};
use novarocks_result_contract::{
    NamedScalarField, RootProfileV1 as P, ScalarField, ScalarOpaqueType as O, ScalarSchema,
    ScalarTimestampUnit as U, ScalarValueType as S,
};
use novarocks_type_contract::{NR_LOGICAL_TYPE_KEY, ValueLogicalType as V};
use novarocks_types::schema::SqlType as T;

pub(super) fn scalar_field(
    data_type: &D,
    nullable: bool,
    logical_type: Option<&T>,
) -> Result<ScalarField, String> {
    let mut bounds = Bounds {
        nodes: 0,
        backing: size_of::<ScalarSchema>(),
        wire: 32,
    };
    preflight(data_type, logical_type, None, true, 1, &mut bounds)?;
    build(data_type, nullable, logical_type, None, true)
}

/// Check the final carrier's nested semantic facts against the already-owned
/// proof. Storage compatibility is checked separately; no second tree is built.
pub(super) fn nested_domains_match(field: &ScalarField, data_type: &D) -> bool {
    fn child(expected: &ScalarField, actual: &Field) -> bool {
        let domain = match expected.value_type {
            S::LargeInt => Some(V::LargeInt),
            S::Variant => Some(V::Variant),
            S::Json => Some(V::Json),
            S::Opaque(O::Hll) => Some(V::Hll),
            S::Opaque(O::Bitmap) => Some(V::Bitmap),
            S::Opaque(O::Object) => Some(V::Object),
            S::Opaque(O::Percentile) => Some(V::Percentile),
            _ => None,
        };
        marker(actual).is_ok_and(|actual| {
            actual == domain
                || (actual.is_none() && matches!(expected.value_type, S::LargeInt | S::Variant))
        }) && nested_domains_match(expected, actual.data_type())
    }
    match (&field.value_type, data_type) {
        (S::List(expected), D::List(actual)) => child(expected, actual),
        (S::Map { key, value }, D::Map(entries, false)) => {
            let Ok((actual_key, actual_value)) = map_children(entries) else {
                return false;
            };
            child(key, actual_key) && child(value, actual_value)
        }
        (S::Struct(expected), D::Struct(actual)) => {
            expected.len() == actual.len()
                && expected
                    .iter()
                    .zip(actual)
                    .all(|(expected, actual)| child(&expected.field, actual))
        }
        _ => true,
    }
}

fn invalid() -> String {
    "scalar logical identity or storage is unsupported or inconsistent".to_owned()
}
fn limited() -> String {
    "scalar schema exceeds its bounded profile".to_owned()
}

struct Bounds {
    nodes: usize,
    backing: usize,
    wire: usize,
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
    fn node(&mut self, depth: usize) -> Result<(), String> {
        self.nodes = self.nodes.checked_add(1).ok_or_else(limited)?;
        if depth > P::MAX_DEPTH || self.nodes > P::SCHEMA_TYPE_NODES {
            return Err(limited());
        }
        self.add(0, 32)
    }
    fn name(&mut self, name: &str) -> Result<(), String> {
        if name.len() > P::MAX_NAME_BYTES {
            return Err(limited());
        }
        self.add(name.len(), name.len().checked_add(8).ok_or_else(limited)?)
    }
}

enum Kind {
    Leaf(S),
    Timestamp(U),
    List,
    Map,
    Struct,
}

// Match the existing logical helper's trim/case normalization without a
// temporary String during source preflight. Unknown markers never become a
// plain field. Other provider decorations are not semantic type facts.
pub(super) fn marker(field: &Field) -> Result<Option<V>, String> {
    let Some(value) = field.metadata().get(NR_LOGICAL_TYPE_KEY) else {
        return Ok(None);
    };
    if value.len() > P::MAX_NAME_BYTES {
        return Err(limited());
    }
    V::from_legacy_compatible_metadata_value(value)
        .map(Some)
        .map_err(|_| invalid())
}

fn sql_domain(logical: &T) -> Option<V> {
    match logical {
        T::LargeInt => Some(V::LargeInt),
        T::Variant => Some(V::Variant),
        T::Json => Some(V::Json),
        T::Hll => Some(V::Hll),
        T::Bitmap => Some(V::Bitmap),
        T::Object => Some(V::Object),
        T::Percentile => Some(V::Percentile),
        _ => None,
    }
}

fn sql_storage_matches(logical: &T, data_type: &D) -> bool {
    match (logical, data_type) {
        (T::TinyInt, D::Int8)
        | (T::SmallInt, D::Int16)
        | (T::Int, D::Int32)
        | (T::BigInt, D::Int64)
        | (T::LargeInt, D::FixedSizeBinary(16))
        | (T::Float, D::Float32)
        | (T::Double, D::Float64)
        | (T::String | T::Json, D::Utf8)
        | (T::Binary | T::Hll | T::Bitmap | T::Object | T::Percentile, D::Binary)
        | (T::Boolean, D::Boolean)
        | (T::Date, D::Date32)
        | (T::DateTime, D::Timestamp(TimeUnit::Microsecond, _))
        | (T::DateTimeNs, D::Timestamp(TimeUnit::Nanosecond, _))
        | (T::Time, D::Time64(TimeUnit::Microsecond))
        | (T::Variant, D::LargeBinary)
        | (T::Array(_), D::List(_))
        | (T::Map(_, _), D::Map(_, false))
        | (T::Struct(_), D::Struct(_)) => true,
        (T::Decimal { precision, scale }, D::Decimal128(p, s) | D::Decimal256(p, s)) => {
            precision == p && scale == s
        }
        _ => false,
    }
}

fn kind(data_type: &D, logical: Option<&T>, nested: Option<V>, top: bool) -> Result<Kind, String> {
    if matches!(logical, Some(T::Uuid)) || nested == Some(V::Uuid) {
        return Err("scalar logical type UUID is unsupported".into());
    }
    if logical.is_some_and(|logical| !sql_storage_matches(logical, data_type)) {
        return Err(invalid());
    }
    let domain = if let Some(logical) = logical {
        let expected = sql_domain(logical);
        let historical_unmarked =
            nested.is_none() && matches!(expected, Some(V::LargeInt | V::Variant));
        if !historical_unmarked
            && nested != expected
            && (nested.is_some() || (!top && expected.is_some()))
        {
            return Err(invalid());
        }
        expected
    } else {
        nested
    };
    if let Some(domain) = domain {
        return Ok(Kind::Leaf(match (domain, data_type) {
            (V::Json, D::Utf8) => S::Json,
            (V::LargeInt, D::FixedSizeBinary(16)) => S::LargeInt,
            (V::Variant, D::LargeBinary) => S::Variant,
            (V::Hll, D::Binary) => S::Opaque(O::Hll),
            (V::Bitmap, D::Binary) => S::Opaque(O::Bitmap),
            (V::Object, D::Binary) => S::Opaque(O::Object),
            (V::Percentile, D::Binary) => S::Opaque(O::Percentile),
            _ => return Err(invalid()),
        }));
    }
    Ok(match data_type {
        D::Null => Kind::Leaf(S::Null),
        D::Boolean => Kind::Leaf(S::Boolean),
        D::Int8 => Kind::Leaf(S::SignedInteger(8)),
        D::Int16 => Kind::Leaf(S::SignedInteger(16)),
        D::Int32 => Kind::Leaf(S::SignedInteger(32)),
        D::Int64 => Kind::Leaf(S::SignedInteger(64)),
        D::FixedSizeBinary(16) => Kind::Leaf(S::LargeInt),
        D::Float32 => Kind::Leaf(S::Float32),
        D::Float64 => Kind::Leaf(S::Float64),
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
            Kind::Leaf(S::Decimal {
                bits,
                precision: *precision,
                scale: *scale as u8,
            })
        }
        D::Utf8 => Kind::Leaf(S::String),
        D::Binary => Kind::Leaf(S::Binary),
        // Native's existing nested type codec maps unmarked LargeBinary only
        // to Variant. The top-level scalar still requires its trusted fact.
        D::LargeBinary if !top || matches!(logical, Some(T::Variant)) => Kind::Leaf(S::Variant),
        D::Date32 => Kind::Leaf(S::Date),
        D::Time64(TimeUnit::Microsecond) => Kind::Leaf(S::TimeMicros),
        D::Timestamp(TimeUnit::Microsecond, _) => Kind::Timestamp(U::Microsecond),
        D::Timestamp(TimeUnit::Nanosecond, _) => Kind::Timestamp(U::Nanosecond),
        D::List(_) => Kind::List,
        D::Map(_, false) => Kind::Map,
        D::Struct(_) => Kind::Struct,
        _ => return Err(invalid()),
    })
}

fn child_logical(logical: Option<&T>, index: usize) -> Option<&T> {
    match logical {
        Some(T::Array(child)) => Some(child),
        Some(T::Map(key, _)) if index == 0 => Some(key),
        Some(T::Map(_, value)) => Some(value),
        Some(T::Struct(fields)) => fields.get(index).map(|(_, ty)| ty),
        _ => None,
    }
}

pub(super) fn map_children(entries: &Field) -> Result<(&Field, &Field), String> {
    if entries.name() != "entries" || entries.is_nullable() || marker(entries)?.is_some() {
        return Err(invalid());
    }
    let D::Struct(fields) = entries.data_type() else {
        return Err(invalid());
    };
    if fields.len() != 2 || fields[0].name() != "key" || fields[1].name() != "value" {
        return Err(invalid());
    }
    Ok((&fields[0], &fields[1]))
}

fn preflight_field(
    field: &Field,
    logical: Option<&T>,
    depth: usize,
    bounds: &mut Bounds,
) -> Result<(), String> {
    preflight(
        field.data_type(),
        logical,
        marker(field)?,
        false,
        depth,
        bounds,
    )
}

fn preflight(
    data_type: &D,
    logical: Option<&T>,
    nested: Option<V>,
    top: bool,
    depth: usize,
    bounds: &mut Bounds,
) -> Result<(), String> {
    bounds.node(depth)?;
    match kind(data_type, logical, nested, top)? {
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
            bounds.add(size_of::<ScalarField>(), 0)?;
            preflight_field(child, child_logical(logical, 0), depth + 1, bounds)?;
        }
        Kind::Map => {
            let D::Map(entries, _) = data_type else {
                unreachable!()
            };
            let (key, value) = map_children(entries)?;
            bounds.add(2 * size_of::<ScalarField>(), 0)?;
            preflight_field(key, child_logical(logical, 0), depth + 1, bounds)?;
            preflight_field(value, child_logical(logical, 1), depth + 1, bounds)?;
        }
        Kind::Struct => {
            let D::Struct(fields) = data_type else {
                unreachable!()
            };
            if fields.len() > P::MAX_COLUMNS {
                return Err(limited());
            }
            if let Some(T::Struct(expected)) = logical
                && expected.len() != fields.len()
            {
                return Err(invalid());
            }
            bounds.add(
                fields
                    .len()
                    .checked_mul(size_of::<NamedScalarField>())
                    .ok_or_else(limited)?,
                8,
            )?;
            for (index, field) in fields.iter().enumerate() {
                bounds.name(field.name())?;
                if let Some(T::Struct(expected)) = logical
                    && expected[index].0 != *field.name()
                {
                    return Err(invalid());
                }
                preflight_field(field, child_logical(logical, index), depth + 1, bounds)?;
            }
        }
    }
    Ok(())
}

fn build_field(field: &Field, logical: Option<&T>) -> Result<ScalarField, String> {
    build(
        field.data_type(),
        field.is_nullable(),
        logical,
        marker(field)?,
        false,
    )
}

fn build(
    data_type: &D,
    nullable: bool,
    logical: Option<&T>,
    nested: Option<V>,
    top: bool,
) -> Result<ScalarField, String> {
    let value_type = match kind(data_type, logical, nested, top)? {
        Kind::Leaf(leaf) => leaf,
        Kind::Timestamp(unit) => {
            let D::Timestamp(_, zone) = data_type else {
                unreachable!()
            };
            S::Timestamp {
                unit,
                timezone: zone.as_ref().map(|zone| zone.to_string()),
            }
        }
        Kind::List => {
            let D::List(child) = data_type else {
                unreachable!()
            };
            S::List(Box::new(build_field(child, child_logical(logical, 0))?))
        }
        Kind::Map => {
            let D::Map(entries, _) = data_type else {
                unreachable!()
            };
            let (key, value) = map_children(entries)?;
            S::Map {
                key: Box::new(build_field(key, child_logical(logical, 0))?),
                value: Box::new(build_field(value, child_logical(logical, 1))?),
            }
        }
        Kind::Struct => {
            let D::Struct(fields) = data_type else {
                unreachable!()
            };
            let mut out = Vec::with_capacity(fields.len());
            for (index, field) in fields.iter().enumerate() {
                out.push(NamedScalarField {
                    name: field.name().to_owned(),
                    field: build_field(field, child_logical(logical, index))?,
                });
            }
            S::Struct(out)
        }
    };
    Ok(ScalarField {
        nullable,
        value_type,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_types::logical::LogicalType as L;
    use novarocks_types::logical::field_with_logical_type;
    use std::sync::Arc;

    fn project(data_type: &D, logical: Option<&T>) -> ScalarField {
        let field = scalar_field(data_type, true, logical).unwrap();
        ScalarSchema::try_new(field.clone()).unwrap();
        field
    }
    fn list(field: Field) -> D {
        D::List(Arc::new(field))
    }
    fn marked(name: &str, storage: D, nullable: bool, logical: L) -> Field {
        field_with_logical_type(Field::new(name, storage, nullable), logical)
    }
    fn map(key: Field, value: Field) -> D {
        D::Map(
            Arc::new(Field::new(
                "entries",
                D::Struct(vec![Arc::new(key), Arc::new(value)].into()),
                false,
            )),
            false,
        )
    }

    #[test]
    fn complete_scalar_markers_keep_original_unmarked_new_domains_and_refuse_uuid() {
        for (marker, storage, logical, expected) in [
            ("largeint", D::FixedSizeBinary(16), T::LargeInt, S::LargeInt),
            ("variant", D::LargeBinary, T::Variant, S::Variant),
        ] {
            let declared = T::Array(Box::new(logical));
            for marked in [false, true] {
                let mut child = Field::new("item", storage.clone(), true);
                if marked {
                    child =
                        child.with_metadata([(NR_LOGICAL_TYPE_KEY.into(), marker.into())].into());
                }
                let data_type = list(child);
                let projected = scalar_field(&data_type, true, Some(&declared)).unwrap();
                let S::List(ref child) = projected.value_type else {
                    panic!("expected List")
                };
                assert_eq!(child.value_type, expected);
                assert!(nested_domains_match(&projected, &data_type));
            }
        }
        let error = scalar_field(&D::FixedSizeBinary(16), true, Some(&T::Uuid)).unwrap_err();
        assert!(error.contains("UUID") && error.contains("unsupported"));
        let nested = list(
            Field::new("item", D::FixedSizeBinary(16), true)
                .with_metadata([(NR_LOGICAL_TYPE_KEY.into(), "uuid".into())].into()),
        );
        let error = scalar_field(&nested, true, None).unwrap_err();
        assert!(error.contains("UUID") && error.contains("unsupported"));
        assert_eq!(
            scalar_field(&D::FixedSizeBinary(16), true, None)
                .unwrap()
                .value_type,
            S::LargeInt
        );
    }

    #[test]
    fn m07_scalar_common_type_cannot_erase_an_invalid_domain_into_a_value() {
        use novarocks_types::wider_type;
        for marker in ["unknown", "hll"] {
            let invalid = list(
                Field::new("item", D::Utf8, false)
                    .with_metadata([(NR_LOGICAL_TYPE_KEY.to_owned(), marker.to_owned())].into()),
            );
            let widened = wider_type(&invalid, &list(Field::new("item", D::Utf8, true)));
            assert!(scalar_field(&widened, true, None).is_err(), "{marker}");
        }
        let opaque = |domain| {
            list(novarocks_types::logical::field_with_logical_type(
                Field::new("item", D::LargeBinary, false),
                domain,
            ))
        };
        let conflicting = wider_type(&opaque(L::Hll), &opaque(L::Bitmap));
        assert!(scalar_field(&conflicting, true, None).is_err());
        let json = list(novarocks_types::logical::field_with_logical_type(
            Field::new("item", D::Utf8, false),
            L::Json,
        ));
        let plain = list(Field::new("item", D::Utf8, true));
        let actual = scalar_field(&wider_type(&json, &plain), true, None).unwrap();
        let S::List(child) = actual.value_type else {
            panic!("expected List");
        };
        assert_eq!(child.value_type, S::String);
    }

    #[test]
    fn m07_scalar_map_container_marker_survives_normalization_as_a_refusal() {
        use novarocks_types::{undecorated_nested_type, wider_type};
        let map = |nullable, marker: Option<&str>| {
            let entries = Field::new(
                "entries",
                D::Struct(
                    vec![
                        Arc::new(Field::new("key", D::Int32, false)),
                        Arc::new(Field::new("value", D::Utf8, nullable)),
                    ]
                    .into(),
                ),
                false,
            );
            let entries = match marker {
                Some(marker) => entries
                    .with_metadata([(NR_LOGICAL_TYPE_KEY.to_owned(), marker.to_owned())].into()),
                None => entries,
            };
            D::Map(Arc::new(entries), false)
        };
        for marker in ["unknown", "json", "hll", "bitmap", "object", "percentile"] {
            let invalid = map(false, Some(marker));
            assert!(scalar_field(&invalid, true, None).is_err());
            assert!(scalar_field(&undecorated_nested_type(&invalid), true, None).is_err());
            for widened in [
                wider_type(&invalid, &map(true, None)),
                wider_type(&map(true, None), &invalid),
            ] {
                assert!(scalar_field(&widened, true, None).is_err(), "{marker}");
            }
        }
        assert!(scalar_field(&wider_type(&map(false, None), &map(true, None)), true, None).is_ok());
        let D::Map(entries, sorted) = map(false, None) else {
            unreachable!()
        };
        let decorated = D::Map(
            Arc::new(
                entries
                    .as_ref()
                    .clone()
                    .with_metadata([("PARQUET:field_id".to_owned(), "37".to_owned())].into()),
            ),
            sorted,
        );
        assert!(scalar_field(&undecorated_nested_type(&decorated), true, None).is_ok());
    }

    #[test]
    fn top_domains_require_trusted_facts_and_exact_storage() {
        assert_eq!(project(&D::Utf8, None).value_type, S::String);
        assert_eq!(project(&D::Binary, None).value_type, S::Binary);
        assert!(scalar_field(&D::LargeBinary, true, None).is_err());
        for (storage, logical, expected) in [
            (D::Utf8, T::Json, S::Json),
            (D::Binary, T::Hll, S::Opaque(O::Hll)),
            (D::Binary, T::Bitmap, S::Opaque(O::Bitmap)),
            (D::Binary, T::Object, S::Opaque(O::Object)),
            (D::Binary, T::Percentile, S::Opaque(O::Percentile)),
            (D::LargeBinary, T::Variant, S::Variant),
        ] {
            let actual = scalar_field(&storage, false, Some(&logical)).unwrap();
            assert_eq!(
                actual,
                ScalarField {
                    nullable: false,
                    value_type: expected
                }
            );
            assert!(scalar_field(&D::Int64, false, Some(&logical)).is_err());
        }
        assert!(scalar_field(&D::Utf8, true, Some(&T::Binary)).is_err());
        assert!(scalar_field(&D::LargeBinary, true, Some(&T::Binary)).is_err());
    }

    #[test]
    fn signed_and_fixed_scalar_vocabulary_preserves_width_and_nullability() {
        for (storage, expected) in [
            (D::Null, S::Null),
            (D::Boolean, S::Boolean),
            (D::Int8, S::SignedInteger(8)),
            (D::Int16, S::SignedInteger(16)),
            (D::Int32, S::SignedInteger(32)),
            (D::Int64, S::SignedInteger(64)),
            (D::FixedSizeBinary(16), S::LargeInt),
            (D::Float32, S::Float32),
            (D::Float64, S::Float64),
            (D::Date32, S::Date),
            (D::Time64(TimeUnit::Microsecond), S::TimeMicros),
        ] {
            for nullable in [false, true] {
                assert_eq!(
                    scalar_field(&storage, nullable, None).unwrap(),
                    ScalarField {
                        nullable,
                        value_type: expected.clone()
                    }
                );
            }
        }
        assert!(scalar_field(&D::Int64, true, Some(&T::Int)).is_err());
    }

    #[test]
    fn decimal_and_timestamp_metadata_are_exact_and_closed() {
        for (storage, expected) in [
            (
                D::Decimal128(38, 17),
                S::Decimal {
                    bits: 128,
                    precision: 38,
                    scale: 17,
                },
            ),
            (
                D::Decimal256(76, 70),
                S::Decimal {
                    bits: 256,
                    precision: 76,
                    scale: 70,
                },
            ),
        ] {
            assert_eq!(project(&storage, None).value_type, expected);
        }
        for unit in [TimeUnit::Microsecond, TimeUnit::Nanosecond] {
            for zone in [None, Some(Arc::<str>::from("Asia/Shanghai"))] {
                let actual = project(&D::Timestamp(unit, zone.clone()), None);
                assert_eq!(
                    actual.value_type,
                    S::Timestamp {
                        unit: if unit == TimeUnit::Microsecond {
                            U::Microsecond
                        } else {
                            U::Nanosecond
                        },
                        timezone: zone.map(|zone| zone.to_string()),
                    }
                );
            }
        }
        for invalid in [
            D::Decimal128(0, 0),
            D::Decimal128(39, 0),
            D::Decimal256(77, 0),
            D::Decimal128(10, -1),
            D::Decimal256(10, 11),
            D::Timestamp(TimeUnit::Second, None),
            D::Timestamp(TimeUnit::Microsecond, Some("".into())),
        ] {
            assert!(scalar_field(&invalid, true, None).is_err());
        }
        assert!(
            scalar_field(
                &D::Decimal128(12, 3),
                true,
                Some(&T::Decimal {
                    precision: 13,
                    scale: 3
                })
            )
            .is_err()
        );
        assert!(
            scalar_field(
                &D::Timestamp(TimeUnit::Nanosecond, None),
                true,
                Some(&T::DateTime)
            )
            .is_err()
        );
    }

    #[test]
    fn nested_markers_preserve_each_opaque_domain_and_native_variant_rule() {
        for (domain, expected) in [
            (L::Json, S::Json),
            (L::Hll, S::Opaque(O::Hll)),
            (L::Bitmap, S::Opaque(O::Bitmap)),
            (L::Object, S::Opaque(O::Object)),
            (L::Percentile, S::Opaque(O::Percentile)),
        ] {
            let storage = if domain == L::Json {
                D::Utf8
            } else {
                D::Binary
            };
            let actual = project(&list(marked("item", storage, false, domain)), None);
            assert_eq!(
                actual.value_type,
                S::List(Box::new(ScalarField {
                    nullable: false,
                    value_type: expected
                }))
            );
        }
        let actual = project(&list(Field::new("item", D::LargeBinary, true)), None);
        assert_eq!(
            actual.value_type,
            S::List(Box::new(ScalarField {
                nullable: true,
                value_type: S::Variant
            }))
        );
        assert_eq!(
            project(&list(Field::new("item", D::Binary, true)), None).value_type,
            S::List(Box::new(ScalarField {
                nullable: true,
                value_type: S::Binary
            }))
        );
    }

    #[test]
    fn unknown_or_physically_incompatible_markers_are_errors() {
        for (storage, marker_value) in [
            (D::Utf8, "unknown"),
            (D::Utf8, "hll"),
            (D::Binary, "json"),
            (D::Int64, "json"),
            (D::LargeBinary, "object"),
        ] {
            let field = Field::new("item", storage, true)
                .with_metadata([(NR_LOGICAL_TYPE_KEY.to_owned(), marker_value.to_owned())].into());
            assert!(scalar_field(&list(field), true, None).is_err());
        }
        let normalized = Field::new("item", D::Utf8, true).with_metadata(
            [
                (NR_LOGICAL_TYPE_KEY.to_owned(), " JSON ".to_owned()),
                ("PARQUET:field_id".to_owned(), "17".to_owned()),
            ]
            .into(),
        );
        assert_eq!(
            project(&list(normalized), None).value_type,
            S::List(Box::new(ScalarField {
                nullable: true,
                value_type: S::Json
            }))
        );
    }

    #[test]
    fn nested_struct_map_order_names_and_nullability_survive() {
        let data_type = D::Struct(
            vec![
                Arc::new(Field::new(
                    "first",
                    list(marked("item", D::Utf8, false, L::Json)),
                    true,
                )),
                Arc::new(Field::new(
                    "second",
                    map(
                        Field::new("key", D::Int32, false),
                        marked("value", D::Binary, true, L::Bitmap),
                    ),
                    false,
                )),
            ]
            .into(),
        );
        let actual = project(&data_type, None);
        assert_eq!(
            actual.value_type,
            S::Struct(vec![
                NamedScalarField {
                    name: "first".to_owned(),
                    field: ScalarField {
                        nullable: true,
                        value_type: S::List(Box::new(ScalarField {
                            nullable: false,
                            value_type: S::Json
                        }))
                    }
                },
                NamedScalarField {
                    name: "second".to_owned(),
                    field: ScalarField {
                        nullable: false,
                        value_type: S::Map {
                            key: Box::new(ScalarField {
                                nullable: false,
                                value_type: S::SignedInteger(32)
                            }),
                            value: Box::new(ScalarField {
                                nullable: true,
                                value_type: S::Opaque(O::Bitmap)
                            })
                        }
                    }
                },
            ])
        );
        let trusted = T::Struct(vec![
            ("first".to_owned(), T::Array(Box::new(T::Json))),
            (
                "second".to_owned(),
                T::Map(Box::new(T::Int), Box::new(T::Bitmap)),
            ),
        ]);
        assert_eq!(project(&data_type, Some(&trusted)), actual);
        let wrong_order = T::Struct(vec![
            ("second".to_owned(), T::Array(Box::new(T::Json))),
            (
                "first".to_owned(),
                T::Map(Box::new(T::Int), Box::new(T::Bitmap)),
            ),
        ]);
        assert!(scalar_field(&data_type, true, Some(&wrong_order)).is_err());
        let conflicting = T::Array(Box::new(T::String));
        assert!(
            scalar_field(
                &list(marked("item", D::Utf8, true, L::Json)),
                true,
                Some(&conflicting)
            )
            .is_err()
        );
        assert!(
            scalar_field(
                &list(Field::new("item", D::Utf8, true)),
                true,
                Some(&T::Array(Box::new(T::Json)))
            )
            .is_err()
        );
    }

    #[test]
    fn noncanonical_container_shapes_are_rejected() {
        let entries = Field::new(
            "entries",
            D::Struct(
                vec![
                    Arc::new(Field::new("key", D::Int32, false)),
                    Arc::new(Field::new("value", D::Utf8, true)),
                ]
                .into(),
            ),
            false,
        );
        for invalid in [
            list(Field::new("element", D::Int32, true)),
            D::Map(Arc::new(entries.clone()), true),
            D::Map(Arc::new(entries.clone().with_nullable(true)), false),
            D::Map(Arc::new(entries.with_name("pairs")), false),
            map(
                Field::new("bad_key", D::Int32, false),
                Field::new("value", D::Utf8, true),
            ),
        ] {
            assert!(scalar_field(&invalid, true, None).is_err());
        }
    }

    #[test]
    fn unsupported_carriers_are_not_downgraded() {
        for invalid in [
            D::UInt8,
            D::UInt16,
            D::UInt32,
            D::UInt64,
            D::LargeUtf8,
            D::Utf8View,
            D::BinaryView,
            D::FixedSizeBinary(8),
            D::Dictionary(Box::new(D::Int32), Box::new(D::Utf8)),
            D::LargeList(Arc::new(Field::new("item", D::Int32, true))),
            D::FixedSizeList(Arc::new(Field::new("item", D::Int32, true)), 2),
            D::Time32(TimeUnit::Second),
        ] {
            assert!(scalar_field(&invalid, true, None).is_err());
        }
    }

    #[test]
    fn borrowed_preflight_enforces_depth_fields_names_zones_and_aggregate_sizes() {
        let mut depth64 = D::Int32;
        for _ in 1..64 {
            depth64 = list(Field::new("item", depth64, true));
        }
        assert!(scalar_field(&depth64, true, None).is_ok());
        assert!(scalar_field(&list(Field::new("item", depth64, true)), true, None).is_err());
        let flat = |count| {
            D::Struct(
                (0..count)
                    .map(|_| Arc::new(Field::new("field", D::Int32, true)))
                    .collect::<Vec<_>>()
                    .into(),
            )
        };
        assert!(scalar_field(&flat(P::MAX_COLUMNS), true, None).is_ok());
        assert!(scalar_field(&flat(P::MAX_COLUMNS + 1), true, None).is_err());
        let named =
            |len| D::Struct(vec![Arc::new(Field::new("n".repeat(len), D::Int32, true))].into());
        assert!(scalar_field(&named(P::MAX_NAME_BYTES), true, None).is_ok());
        assert!(scalar_field(&named(P::MAX_NAME_BYTES + 1), true, None).is_err());
        let zoned = |len| D::Timestamp(TimeUnit::Nanosecond, Some("z".repeat(len).into()));
        assert!(scalar_field(&zoned(P::MAX_NAME_BYTES), true, None).is_ok());
        assert!(scalar_field(&zoned(P::MAX_NAME_BYTES + 1), true, None).is_err());
        let excessive_names = D::Struct(
            (0..4)
                .map(|_| Arc::new(Field::new("n".repeat(P::MAX_NAME_BYTES), D::Int32, true)))
                .collect::<Vec<_>>()
                .into(),
        );
        assert!(scalar_field(&excessive_names, true, None).is_err());
        // Each branch is individually legal; the complete tree exceeds the
        // aggregate semantic/wire allowance and must fail before construction.
        let many_nodes = D::Struct(
            (0..3)
                .map(|_| Arc::new(Field::new("branch", flat(3000), true)))
                .collect::<Vec<_>>()
                .into(),
        );
        assert!(scalar_field(&many_nodes, true, None).is_err());
    }
}
