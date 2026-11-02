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

//! SQL-owned exact ScalarValueV1 projection of a complete execution binding.
//! Borrowed preflight keeps the existing scalar profile before any output tree
//! construction. These schema bounds are not an FE allocation grant.

use arrow::datatypes::{DataType as D, Field, TimeUnit};
use novarocks_result_contract::{
    NamedScalarField, RootProfileV1 as P, ScalarField, ScalarOpaqueType as O, ScalarSchema,
    ScalarTimestampUnit as U, ScalarValueType as S,
};
use novarocks_type_contract::{FunctionValueType, ValueKind, ValueLogicalType as V, ValueTypeRef};

pub(super) fn scalar_field(value: &FunctionValueType) -> Result<ScalarField, String> {
    let mut bounds = Bounds {
        nodes: 0,
        backing: size_of::<ScalarSchema>(),
        wire: 32,
    };
    preflight(value.value_type(), value.data_type(), 1, &mut bounds)?;
    build(value.value_type(), value.data_type())
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

fn kind(value: &ValueTypeRef, layout: &D) -> Result<Kind, String> {
    Ok(match (value.kind(), layout) {
        (ValueKind::Nominal(V::Json), D::Utf8) => Kind::Leaf(S::Json),
        (ValueKind::Nominal(V::Variant), D::LargeBinary) => Kind::Leaf(S::Variant),
        (ValueKind::Nominal(V::LargeInt), D::FixedSizeBinary(16)) => Kind::Leaf(S::LargeInt),
        (ValueKind::Nominal(V::Hll), D::Binary) => Kind::Leaf(S::Opaque(O::Hll)),
        (ValueKind::Nominal(V::Bitmap), D::Binary) => Kind::Leaf(S::Opaque(O::Bitmap)),
        (ValueKind::Nominal(V::Object), D::Binary) => Kind::Leaf(S::Opaque(O::Object)),
        (ValueKind::Nominal(V::Percentile), D::Binary) => Kind::Leaf(S::Opaque(O::Percentile)),
        (ValueKind::String, D::Utf8) => Kind::Leaf(S::String),
        (ValueKind::Binary, D::Binary) => Kind::Leaf(S::Binary),
        (ValueKind::Scalar(source), actual) if source == actual => match actual {
            D::Null => Kind::Leaf(S::Null),
            D::Boolean => Kind::Leaf(S::Boolean),
            D::Int8 => Kind::Leaf(S::SignedInteger(8)),
            D::Int16 => Kind::Leaf(S::SignedInteger(16)),
            D::Int32 => Kind::Leaf(S::SignedInteger(32)),
            D::Int64 => Kind::Leaf(S::SignedInteger(64)),
            D::Float32 => Kind::Leaf(S::Float32),
            D::Float64 => Kind::Leaf(S::Float64),
            D::Date32 => Kind::Leaf(S::Date),
            D::Time64(TimeUnit::Microsecond) => Kind::Leaf(S::TimeMicros),
            D::Timestamp(TimeUnit::Microsecond, _) => Kind::Timestamp(U::Microsecond),
            D::Timestamp(TimeUnit::Nanosecond, _) => Kind::Timestamp(U::Nanosecond),
            D::Decimal128(precision, scale) | D::Decimal256(precision, scale) => {
                let bits = if matches!(actual, D::Decimal128(..)) {
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
            _ => return Err(invalid()),
        },
        (ValueKind::List(_), D::List(_)) => Kind::List,
        (ValueKind::Map { .. }, D::Map(_, false)) => Kind::Map,
        (ValueKind::Struct(_), D::Struct(_)) => Kind::Struct,
        // Complete declarations do not add ScalarValueV1 capabilities. UUID
        // and physical FixedBinary/LargeBinary never acquire another domain.
        _ => return Err(invalid()),
    })
}

/// Shared borrowed role/shape check used by the scalar and renderer authors.
/// Generic metadata has no authority over these execution roles.
pub(super) fn map_children(entries: &Field) -> Result<(&Field, &Field), String> {
    if entries.name() != "entries" || entries.is_nullable() {
        return Err(invalid());
    }
    let D::Struct(fields) = entries.data_type() else {
        return Err(invalid());
    };
    if fields.len() != 2
        || fields[0].name() != "key"
        || fields[1].name() != "value"
        || fields[0].is_nullable()
    {
        return Err(invalid());
    }
    Ok((&fields[0], &fields[1]))
}

fn preflight(
    value: &ValueTypeRef,
    layout: &D,
    depth: usize,
    bounds: &mut Bounds,
) -> Result<(), String> {
    bounds.node(depth)?;
    match kind(value, layout)? {
        Kind::Leaf(_) => {}
        Kind::Timestamp(_) => {
            let D::Timestamp(_, zone) = layout else {
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
            let (ValueKind::List(item), D::List(field)) = (value.kind(), layout) else {
                unreachable!()
            };
            if field.name() != "item" {
                return Err(invalid());
            }
            bounds.add(size_of::<ScalarField>(), 0)?;
            preflight(item, field.data_type(), depth + 1, bounds)?;
        }
        Kind::Map => {
            let (ValueKind::Map { key, value }, D::Map(entries, _)) = (value.kind(), layout) else {
                unreachable!()
            };
            let (key_layout, value_layout) = map_children(entries)?;
            bounds.add(2 * size_of::<ScalarField>(), 0)?;
            preflight(key, key_layout.data_type(), depth + 1, bounds)?;
            preflight(value, value_layout.data_type(), depth + 1, bounds)?;
        }
        Kind::Struct => {
            let (ValueKind::Struct(fields), D::Struct(layout)) = (value.kind(), layout) else {
                unreachable!()
            };
            if fields.len() > P::MAX_COLUMNS {
                return Err(limited());
            }
            if fields.len() != layout.len() {
                return Err(invalid());
            }
            bounds.add(
                fields
                    .len()
                    .checked_mul(size_of::<NamedScalarField>())
                    .ok_or_else(limited)?,
                8,
            )?;
            for (field, layout) in fields.iter().zip(layout) {
                if field.name() != layout.name() {
                    return Err(invalid());
                }
                bounds.name(field.name())?;
                preflight(field.value_type(), layout.data_type(), depth + 1, bounds)?;
            }
        }
    }
    Ok(())
}

fn build(value: &ValueTypeRef, layout: &D) -> Result<ScalarField, String> {
    let value_type = match kind(value, layout)? {
        Kind::Leaf(leaf) => leaf,
        Kind::Timestamp(unit) => {
            let D::Timestamp(_, zone) = layout else {
                unreachable!()
            };
            S::Timestamp {
                unit,
                timezone: zone.as_ref().map(|zone| zone.to_string()),
            }
        }
        Kind::List => {
            let (ValueKind::List(item), D::List(field)) = (value.kind(), layout) else {
                unreachable!()
            };
            S::List(Box::new(build(item, field.data_type())?))
        }
        Kind::Map => {
            let (ValueKind::Map { key, value }, D::Map(entries, _)) = (value.kind(), layout) else {
                unreachable!()
            };
            let (key_layout, value_layout) = map_children(entries)?;
            S::Map {
                key: Box::new(build(key, key_layout.data_type())?),
                value: Box::new(build(value, value_layout.data_type())?),
            }
        }
        Kind::Struct => {
            let (ValueKind::Struct(fields), D::Struct(layout)) = (value.kind(), layout) else {
                unreachable!()
            };
            let mut out = Vec::with_capacity(fields.len());
            for (field, layout) in fields.iter().zip(layout) {
                out.push(NamedScalarField {
                    name: field.name().to_owned(),
                    field: build(field.value_type(), layout.data_type())?,
                });
            }
            S::Struct(out)
        }
    };
    Ok(ScalarField {
        nullable: value.nullable(),
        value_type,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_type_contract::{ValueField, ValueLayout, ValueTypeStorageLifetime as Hold};
    use std::sync::Arc;

    fn physical(layout: D, nullable: bool) -> FunctionValueType {
        FunctionValueType::new(layout, nullable)
    }
    fn nominal(domain: V, layout: D, nullable: bool) -> FunctionValueType {
        FunctionValueType::try_with_logical_type(layout, nullable, domain).unwrap()
    }
    fn typed(kind: ValueKind, layout: D, nullable: bool) -> FunctionValueType {
        FunctionValueType::try_from_value(
            ValueTypeRef::try_new(kind, nullable, Hold::pure()).unwrap(),
            ValueLayout::try_new(&layout, Hold::pure()).unwrap(),
        )
        .unwrap()
    }
    fn list(child: &FunctionValueType, nullable: bool) -> FunctionValueType {
        typed(
            ValueKind::List(child.value_type().clone()),
            D::List(Arc::new(Field::new(
                "item",
                child.data_type().clone(),
                child.nullable(),
            ))),
            nullable,
        )
    }
    fn project(value: &FunctionValueType) -> ScalarField {
        let field = scalar_field(value).unwrap();
        assert!(
            novarocks_type_contract::result_scalar_type::scalar_field_matches_binding(
                &field,
                value.typed_binding()
            )
        );
        assert!(
            novarocks_type_contract::result_scalar_type::scalar_field_matches_storage(
                &field,
                value.data_type(),
                value.nullable()
            )
        );
        ScalarSchema::try_new(field.clone()).unwrap();
        field
    }

    #[test]
    fn complete_nominal_domains_require_their_original_value_author() {
        for (domain, layout, expected) in [
            (V::Json, D::Utf8, S::Json),
            (V::Variant, D::LargeBinary, S::Variant),
            (V::LargeInt, D::FixedSizeBinary(16), S::LargeInt),
            (V::Hll, D::Binary, S::Opaque(O::Hll)),
            (V::Bitmap, D::Binary, S::Opaque(O::Bitmap)),
            (V::Object, D::Binary, S::Opaque(O::Object)),
            (V::Percentile, D::Binary, S::Opaque(O::Percentile)),
        ] {
            let ty = nominal(domain, layout.clone(), false);
            assert_eq!(project(&ty).value_type, expected);
            let scalar = project(&list(&ty, true));
            assert_eq!(
                scalar.value_type,
                S::List(Box::new(ScalarField {
                    nullable: false,
                    value_type: expected
                }))
            );
        }
        assert!(scalar_field(&physical(D::FixedSizeBinary(16), false)).is_err());
        assert!(scalar_field(&physical(D::LargeBinary, true)).is_err());
        assert!(scalar_field(&list(&physical(D::LargeBinary, true), false)).is_err());
        assert!(scalar_field(&nominal(V::Uuid, D::FixedSizeBinary(16), true)).is_err());
    }

    #[test]
    fn signed_scalar_widths_and_root_nullability_remain_exact() {
        for (layout, expected) in [
            (D::Null, S::Null),
            (D::Boolean, S::Boolean),
            (D::Int8, S::SignedInteger(8)),
            (D::Int16, S::SignedInteger(16)),
            (D::Int32, S::SignedInteger(32)),
            (D::Int64, S::SignedInteger(64)),
            (D::Float32, S::Float32),
            (D::Float64, S::Float64),
            (D::Utf8, S::String),
            (D::Binary, S::Binary),
            (D::Date32, S::Date),
            (D::Time64(TimeUnit::Microsecond), S::TimeMicros),
        ] {
            for nullable in [false, true] {
                assert_eq!(
                    project(&physical(layout.clone(), nullable)),
                    ScalarField {
                        nullable,
                        value_type: expected.clone()
                    }
                );
            }
        }
    }

    #[test]
    fn decimal_and_timestamp_value_parameters_remain_exact_and_closed() {
        for (layout, expected) in [
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
            assert_eq!(project(&physical(layout, true)).value_type, expected);
        }
        for unit in [TimeUnit::Microsecond, TimeUnit::Nanosecond] {
            for timezone in [None, Some(Arc::<str>::from("Asia/Shanghai"))] {
                let ty = physical(D::Timestamp(unit, timezone.clone()), true);
                assert_eq!(
                    project(&ty).value_type,
                    S::Timestamp {
                        unit: if unit == TimeUnit::Microsecond {
                            U::Microsecond
                        } else {
                            U::Nanosecond
                        },
                        timezone: timezone.map(|z| z.to_string()),
                    }
                );
            }
        }
        for layout in [D::Decimal128(10, -1), D::Timestamp(TimeUnit::Second, None)] {
            assert!(scalar_field(&physical(layout, true)).is_err());
        }
    }

    #[test]
    fn struct_map_order_names_child_nulls_and_nominal_domains_are_preserved() {
        let key = physical(D::Int32, false);
        let val = nominal(V::Bitmap, D::Binary, true);
        let map = typed(
            ValueKind::Map {
                key: key.value_type().clone(),
                value: val.value_type().clone(),
            },
            D::Map(
                Arc::new(Field::new(
                    "entries",
                    D::Struct(
                        vec![
                            Arc::new(Field::new("key", D::Int32, false)),
                            Arc::new(Field::new("value", D::Binary, true)),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            false,
        );
        let first = list(&nominal(V::Json, D::Utf8, false), true);
        let value = typed(
            ValueKind::Struct(
                vec![
                    ValueField::new("first", first.value_type().clone(), Hold::pure()),
                    ValueField::new("second", map.value_type().clone(), Hold::pure()),
                ]
                .into_boxed_slice(),
            ),
            D::Struct(
                vec![
                    Arc::new(Field::new("first", first.data_type().clone(), true)),
                    Arc::new(Field::new("second", map.data_type().clone(), false)),
                ]
                .into(),
            ),
            true,
        );
        let projected = project(&value);
        let S::Struct(fields) = projected.value_type else {
            panic!("Struct")
        };
        assert_eq!(
            [fields[0].name.as_str(), fields[1].name.as_str()],
            ["first", "second"]
        );
        assert_eq!(fields[0].field.value_type, project(&first).value_type);
        assert_eq!(fields[1].field.value_type, project(&map).value_type);
        let wrong = physical(value.data_type().clone(), true);
        let projected = project(&value);
        assert!(
            !novarocks_type_contract::result_scalar_type::scalar_field_matches_binding(
                &projected,
                wrong.typed_binding()
            )
        );
    }

    #[test]
    fn opaque_source_annotations_do_not_author_scalar_children() {
        for annotation in ["json", "unknown", " HLL "] {
            let field =
                Field::new("item", D::Utf8, true).with_metadata(std::collections::HashMap::<
                    String,
                    String,
                >::from([
                    (
                        novarocks_type_contract::NR_LOGICAL_TYPE_KEY.to_string(),
                        annotation.to_string(),
                    ),
                    ("PARQUET:field_id".to_string(), "17".to_string()),
                ]));
            let ty = physical(D::List(Arc::new(field.clone())), true);
            assert_eq!(
                project(&ty).value_type,
                S::List(Box::new(ScalarField {
                    nullable: true,
                    value_type: S::String
                }))
            );
            assert_eq!(
                field.metadata().len(),
                2,
                "original source facts are preserved independently"
            );
            let D::List(clean) = ty.data_type() else {
                unreachable!()
            };
            assert!(clean.metadata().as_arc().is_none());
        }
    }

    #[test]
    fn unsupported_value_layouts_do_not_install_scalar_capabilities() {
        for layout in [
            D::UInt8,
            D::UInt16,
            D::UInt32,
            D::UInt64,
            D::LargeUtf8,
            D::Utf8View,
            D::BinaryView,
            D::FixedSizeBinary(8),
            D::LargeList(Arc::new(Field::new("item", D::Int32, true))),
            D::FixedSizeList(Arc::new(Field::new("item", D::Int32, true)), 2),
            D::Time32(TimeUnit::Second),
        ] {
            assert!(scalar_field(&physical(layout, true)).is_err());
        }
    }

    #[test]
    fn scalar_source_profile_still_preflights_depth_names_and_aggregate_backing() {
        let mut value = physical(D::Int32, true);
        for _ in 1..P::MAX_DEPTH {
            value = list(&value, true);
        }
        assert!(scalar_field(&value).is_ok());
        let too_deep = D::List(Arc::new(Field::new(
            "item",
            value.data_type().clone(),
            true,
        )));
        assert!(FunctionValueType::try_with_logical_type(too_deep, true, V::Physical).is_err());
        // Modern complete type admission has its own original 1024-byte
        // name/timezone limit. Use genuinely admitted fields to exercise the
        // unchanged scalar aggregate bound, rather than constructing an
        // impossible modern author from the retired bare-Arrow entry point.
        let make_names = |count| {
            physical(
                D::Struct(
                    (0..count)
                        .map(|_| {
                            Arc::new(Field::new(
                                "n".repeat(novarocks_type_contract::MAX_ARROW_FIELD_NAME_BYTES),
                                D::Int32,
                                true,
                            ))
                        })
                        .collect::<Vec<_>>()
                        .into(),
                ),
                true,
            )
        };
        assert!(scalar_field(&make_names(100)).is_ok());
        assert!(scalar_field(&make_names(300)).is_err());
        let zone = physical(
            D::Timestamp(
                TimeUnit::Nanosecond,
                Some(
                    "z".repeat(novarocks_type_contract::MAX_ARROW_TIMESTAMP_TIMEZONE_BYTES)
                        .into(),
                ),
            ),
            true,
        );
        assert!(scalar_field(&zone).is_ok());
    }
}
