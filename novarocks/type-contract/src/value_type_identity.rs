// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Canonical semantic identity stream over the original immutable value tree.
//!
//! The sink owns its digest, allocation and control policy. This writer creates
//! no shadow tree and includes the same semantic fields as ValueTypeRef Eq.

use crate::{ValueKind, ValueLogicalType, ValueTypeRef};
use arrow_schema::{DataType, IntervalUnit, TimeUnit};

/// Emit an unambiguous semantic identity. Exact physical layouts and binding
/// aliases are separate inputs for an owner that needs physical port identity.
/// A sink refusal returns immediately, before any later node or byte is emitted.
pub fn write_value_type_identity_observed<E>(
    value: &ValueTypeRef,
    mut write: impl FnMut(&[u8]) -> Result<(), E>,
) -> Result<(), E> {
    write(b"novarocks-value-type-v1")?;
    node(value, &mut write)
}

fn bytes<E>(value: &[u8], write: &mut impl FnMut(&[u8]) -> Result<(), E>) -> Result<(), E> {
    write(&(value.len() as u64).to_le_bytes())?;
    for chunk in value.chunks(1024) {
        write(chunk)?;
    }
    Ok(())
}

/// Emit exact normalized value-port identity from the validated immutable
/// layout. Runtime dictionary/run-end wrappers were explicitly peeled or
/// refused by ValueLayout construction; they establish no value identity here.
/// Source annotations and dictionary IPC attributes belong to other namespaces.
/// Refusal stops before every subsequent field/node/byte; no tree is copied.
pub fn write_value_layout_identity_observed<E>(
    layout: &crate::ValueLayout,
    mut write: impl FnMut(&[u8]) -> Result<(), E>,
) -> Result<(), E> {
    write(b"novarocks-value-layout-v1")?;
    layout_node(layout.data_type(), &mut write)
}
fn layout_field<E>(
    field: &arrow_schema::Field,
    write: &mut impl FnMut(&[u8]) -> Result<(), E>,
) -> Result<(), E> {
    bytes(field.name().as_bytes(), write)?;
    write(&[u8::from(field.is_nullable())])?;
    layout_node(field.data_type(), write)
}
fn layout_node<E>(ty: &DataType, write: &mut impl FnMut(&[u8]) -> Result<(), E>) -> Result<(), E> {
    match ty {
        DataType::Utf8 => write(&[1])?,
        DataType::LargeUtf8 => write(&[2])?,
        DataType::Utf8View => write(&[3])?,
        DataType::Binary => write(&[4])?,
        DataType::LargeBinary => write(&[5])?,
        DataType::BinaryView => write(&[6])?,
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field) => {
            write(&[match ty {
                DataType::List(_) => 7,
                DataType::LargeList(_) => 8,
                DataType::ListView(_) => 9,
                _ => 10,
            }])?;
            layout_field(field, write)?;
        }
        DataType::FixedSizeList(field, length) => {
            write(&[11])?;
            write(&length.to_le_bytes())?;
            layout_field(field, write)?;
        }
        DataType::Struct(fields) => {
            write(&[12])?;
            write(&(fields.len() as u64).to_le_bytes())?;
            for field in fields {
                layout_field(field, write)?;
            }
        }
        DataType::Map(entries, sorted) => {
            write(&[13, u8::from(*sorted)])?;
            layout_field(entries, write)?;
        }
        DataType::Union(fields, mode) => {
            write(&[
                14,
                match mode {
                    arrow_schema::UnionMode::Sparse => 0,
                    arrow_schema::UnionMode::Dense => 1,
                },
            ])?;
            write(&(fields.len() as u64).to_le_bytes())?;
            for (id, field) in fields.iter() {
                write(&[id as u8])?;
                layout_field(field, write)?;
            }
        }
        DataType::Dictionary(_, _) | DataType::RunEndEncoded(_, _) => {
            unreachable!("validated value layout excludes runtime encoding")
        }
        scalar_type => {
            write(&[0])?;
            scalar(scalar_type, write)?;
        }
    }
    Ok(())
}

fn unit(unit: &TimeUnit) -> u8 {
    match unit {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 1,
        TimeUnit::Microsecond => 2,
        TimeUnit::Nanosecond => 3,
    }
}

fn scalar<E>(ty: &DataType, write: &mut impl FnMut(&[u8]) -> Result<(), E>) -> Result<(), E> {
    let tag: u8 = match ty {
        DataType::Null => 0,
        DataType::Boolean => 1,
        DataType::Int8 => 2,
        DataType::Int16 => 3,
        DataType::Int32 => 4,
        DataType::Int64 => 5,
        DataType::UInt8 => 6,
        DataType::UInt16 => 7,
        DataType::UInt32 => 8,
        DataType::UInt64 => 9,
        DataType::Float16 => 10,
        DataType::Float32 => 11,
        DataType::Float64 => 12,
        DataType::FixedSizeBinary(_) => 13,
        DataType::Date32 => 14,
        DataType::Date64 => 15,
        DataType::Timestamp(_, _) => 16,
        DataType::Time32(_) => 17,
        DataType::Time64(_) => 18,
        DataType::Duration(_) => 19,
        DataType::Interval(_) => 20,
        DataType::Decimal32(_, _) => 21,
        DataType::Decimal64(_, _) => 22,
        DataType::Decimal128(_, _) => 23,
        DataType::Decimal256(_, _) => 24,
        _ => unreachable!("checked semantic scalar excludes containers and byte layouts"),
    };
    write(&[tag])?;
    match ty {
        DataType::FixedSizeBinary(width) => write(&width.to_le_bytes())?,
        DataType::Timestamp(time, timezone) => {
            write(&[unit(time), u8::from(timezone.is_some())])?;
            if let Some(timezone) = timezone {
                bytes(timezone.as_bytes(), write)?;
            }
        }
        DataType::Time32(time) | DataType::Time64(time) | DataType::Duration(time) => {
            write(&[unit(time)])?
        }
        DataType::Interval(interval) => write(&[match interval {
            IntervalUnit::YearMonth => 0,
            IntervalUnit::DayTime => 1,
            IntervalUnit::MonthDayNano => 2,
        }])?,
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale)
        | DataType::Decimal256(precision, scale) => write(&[*precision, *scale as u8])?,
        _ => {}
    }
    Ok(())
}

fn node<E>(value: &ValueTypeRef, write: &mut impl FnMut(&[u8]) -> Result<(), E>) -> Result<(), E> {
    write(&[u8::from(value.nullable())])?;
    match value.kind() {
        ValueKind::Scalar(ty) => {
            write(&[0])?;
            scalar(ty, write)?;
        }
        ValueKind::String => write(&[1])?,
        ValueKind::Binary => write(&[2])?,
        ValueKind::Nominal(domain) => write(&[
            3,
            match domain {
                ValueLogicalType::Json => 0,
                ValueLogicalType::Variant => 1,
                ValueLogicalType::Hll => 2,
                ValueLogicalType::Bitmap => 3,
                ValueLogicalType::Object => 4,
                ValueLogicalType::Percentile => 5,
                ValueLogicalType::LargeInt => 6,
                ValueLogicalType::Uuid => 7,
                ValueLogicalType::Physical => unreachable!("checked nominal excludes Physical"),
            },
        ])?,
        ValueKind::List(item) => {
            write(&[4])?;
            node(item, write)?;
        }
        ValueKind::FixedSizeList { item, length } => {
            write(&[5])?;
            write(&length.to_le_bytes())?;
            node(item, write)?;
        }
        ValueKind::Struct(fields) => {
            write(&[6])?;
            write(&(fields.len() as u64).to_le_bytes())?;
            for field in fields {
                bytes(field.name().as_bytes(), write)?;
                node(field.value_type(), write)?;
            }
        }
        ValueKind::Map { key, value } => {
            write(&[7])?;
            node(key, write)?;
            node(value, write)?;
        }
        ValueKind::Union(fields) => {
            write(&[8])?;
            write(&(fields.len() as u64).to_le_bytes())?;
            for field in fields {
                write(&[field.type_id as u8])?;
                bytes(field.field.name().as_bytes(), write)?;
                node(field.field.value_type(), write)?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ValueField, ValueTypeStorageLifetime};
    fn ty(kind: ValueKind, nullable: bool) -> ValueTypeRef {
        ValueTypeRef::try_new(kind, nullable, ValueTypeStorageLifetime::pure()).unwrap()
    }
    fn identity(ty: &ValueTypeRef) -> Vec<u8> {
        let mut bytes = Vec::new();
        write_value_type_identity_observed::<std::convert::Infallible>(ty, |part| {
            bytes.extend(part);
            Ok(())
        })
        .unwrap();
        bytes
    }
    #[test]
    fn independent_equal_values_have_equal_identity() {
        let author = || {
            ty(
                ValueKind::Struct(
                    vec![ValueField::new(
                        "semantic",
                        ty(
                            ValueKind::List(ty(ValueKind::Nominal(ValueLogicalType::Json), false)),
                            true,
                        ),
                        ValueTypeStorageLifetime::pure(),
                    )]
                    .into(),
                ),
                false,
            )
        };
        let a = author();
        let b = author();
        assert!(!a.shares_storage(&b));
        assert_eq!(a, b);
        assert_eq!(identity(&a), identity(&b));
    }
    #[test]
    fn null_names_domains_parameters_and_edges_are_distinct() {
        let variants = vec![
            ty(ValueKind::String, false),
            ty(ValueKind::String, true),
            ty(ValueKind::Nominal(ValueLogicalType::Json), false),
            ty(ValueKind::Binary, false),
            ty(ValueKind::Scalar(DataType::Int32), false),
            ty(ValueKind::Scalar(DataType::UInt32), false),
            ty(ValueKind::Scalar(DataType::Decimal128(12, 2)), false),
            ty(ValueKind::Scalar(DataType::Decimal128(12, 3)), false),
            ty(ValueKind::List(ty(ValueKind::String, false)), false),
            ty(
                ValueKind::FixedSizeList {
                    item: ty(ValueKind::String, false),
                    length: 1,
                },
                false,
            ),
            ty(
                ValueKind::Struct(
                    vec![ValueField::new(
                        "a",
                        ty(ValueKind::String, false),
                        ValueTypeStorageLifetime::pure(),
                    )]
                    .into(),
                ),
                false,
            ),
            ty(
                ValueKind::Struct(
                    vec![ValueField::new(
                        "b",
                        ty(ValueKind::String, false),
                        ValueTypeStorageLifetime::pure(),
                    )]
                    .into(),
                ),
                false,
            ),
        ];
        for (i, a) in variants.iter().enumerate() {
            for b in &variants[i + 1..] {
                assert_ne!(a, b);
                assert_ne!(identity(a), identity(b));
            }
        }
    }
    #[test]
    fn first_sink_refusal_stops_without_later_bytes() {
        let value = ty(
            ValueKind::Struct(
                vec![ValueField::new(
                    "名".repeat(2048),
                    ty(ValueKind::String, true),
                    ValueTypeStorageLifetime::pure(),
                )]
                .into(),
            ),
            false,
        );
        let mut count = 0;
        write_value_type_identity_observed::<()>(&value, |_| {
            count += 1;
            Ok(())
        })
        .unwrap();
        for stop in 1..=count {
            let mut seen = 0;
            let result = write_value_type_identity_observed(&value, |_| {
                seen += 1;
                if seen == stop { Err(stop) } else { Ok(()) }
            });
            assert_eq!(result, Err(stop));
            assert_eq!(seen, stop);
        }
    }
    fn layout_identity(data_type: DataType) -> Vec<u8> {
        let layout =
            crate::ValueLayout::try_new(&data_type, ValueTypeStorageLifetime::pure()).unwrap();
        let mut encoded = Vec::new();
        write_value_layout_identity_observed::<std::convert::Infallible>(&layout, |bytes| {
            encoded.extend_from_slice(bytes);
            Ok(())
        })
        .unwrap();
        encoded
    }
    #[test]
    fn exact_layout_identity_separates_offsets_nulls_union_modes_and_parameters() {
        use arrow_schema::{Field, UnionFields, UnionMode};
        use std::sync::Arc;
        let item = Arc::new(Field::new("item", DataType::Utf8, true));
        let variants = [
            DataType::Utf8,
            DataType::LargeUtf8,
            DataType::Utf8View,
            DataType::Binary,
            DataType::LargeBinary,
            DataType::BinaryView,
            DataType::List(item.clone()),
            DataType::LargeList(item.clone()),
            DataType::ListView(item.clone()),
            DataType::LargeListView(item.clone()),
            DataType::FixedSizeList(item.clone(), 2),
            DataType::FixedSizeList(item, 3),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            DataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            DataType::Decimal128(12, 2),
            DataType::Decimal128(12, 3),
            DataType::Struct(vec![Field::new("a", DataType::Int32, true)].into()),
            DataType::Struct(vec![Field::new("a", DataType::Int32, false)].into()),
            DataType::Struct(vec![Field::new("b", DataType::Int32, true)].into()),
            DataType::Union(
                UnionFields::try_new(
                    [2, 7],
                    [
                        Field::new("a", DataType::Int32, true),
                        Field::new("b", DataType::Utf8, false),
                    ],
                )
                .unwrap(),
                UnionMode::Sparse,
            ),
            DataType::Union(
                UnionFields::try_new(
                    [2, 7],
                    [
                        Field::new("a", DataType::Int32, true),
                        Field::new("b", DataType::Utf8, false),
                    ],
                )
                .unwrap(),
                UnionMode::Dense,
            ),
            DataType::Union(
                UnionFields::try_new(
                    [7, 2],
                    [
                        Field::new("b", DataType::Utf8, false),
                        Field::new("a", DataType::Int32, true),
                    ],
                )
                .unwrap(),
                UnionMode::Sparse,
            ),
        ];
        let identities = variants
            .into_iter()
            .map(layout_identity)
            .collect::<Vec<_>>();
        for (i, a) in identities.iter().enumerate() {
            for (j, b) in identities.iter().enumerate() {
                assert_eq!(a == b, i == j, "layout identities {i} and {j}");
            }
        }
    }
    #[test]
    fn normalized_layout_identity_excludes_annotations_and_explicitly_peeled_encoding() {
        use arrow_schema::Field;
        use std::sync::Arc;
        let plain = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
        let annotated = DataType::List(Arc::new(
            Field::new("original_item", DataType::Utf8, true).with_metadata(
                [(
                    crate::NR_LOGICAL_TYPE_KEY.into(),
                    "unknown-provider-attribute".into(),
                )]
                .into_iter()
                .collect::<std::collections::HashMap<String, String>>(),
            ),
        ));
        assert_eq!(layout_identity(plain), layout_identity(annotated));
        assert_eq!(
            layout_identity(DataType::Utf8),
            layout_identity(DataType::Dictionary(
                Box::new(DataType::Int16),
                Box::new(DataType::Utf8)
            ))
        );
        assert!(
            crate::ValueLayout::try_from_normalized_arrow(
                DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8)),
                ValueTypeStorageLifetime::pure()
            )
            .is_err()
        );
    }
    #[test]
    fn exact_layout_identity_returns_every_first_sink_refusal_without_later_emission() {
        let layout = crate::ValueLayout::try_new(
            &DataType::Struct(
                vec![arrow_schema::Field::new(
                    "名".repeat(256),
                    DataType::List(std::sync::Arc::new(arrow_schema::Field::new(
                        "item",
                        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                        true,
                    ))),
                    false,
                )]
                .into(),
            ),
            ValueTypeStorageLifetime::pure(),
        )
        .unwrap();
        let mut expected = Vec::new();
        write_value_layout_identity_observed::<()>(&layout, |bytes| {
            expected.push(bytes.to_vec());
            Ok(())
        })
        .unwrap();
        for stop in 1..=expected.len() {
            let mut seen = Vec::new();
            let result = write_value_layout_identity_observed(&layout, |bytes| {
                seen.push(bytes.to_vec());
                if seen.len() == stop {
                    Err(stop)
                } else {
                    Ok(())
                }
            });
            assert_eq!(result, Err(stop));
            assert_eq!(seen, &expected[..stop]);
        }
    }
}
