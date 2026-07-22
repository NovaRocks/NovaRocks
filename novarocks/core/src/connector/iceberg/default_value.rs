// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0

//! Default value helpers shared by DDL, schema transport, parquet read path,
//! and INSERT write path.

use iceberg::spec::{
    FormatVersion, Literal as IcebergLiteral, Map as IcebergMap, PrimitiveLiteral, PrimitiveType,
    Type,
};

use novarocks_catalog::schema::{ColumnDefault, validate_column_default};

pub(crate) fn iceberg_literal_to_column_default(
    literal: &IcebergLiteral,
    iceberg_type: &Type,
) -> Result<ColumnDefault, String> {
    let value = iceberg_literal_to_nested_column_default(Some(literal), iceberg_type)?;
    validate_column_default(&value)?;
    Ok(value)
}

fn iceberg_literal_to_nested_column_default(
    literal: Option<&IcebergLiteral>,
    iceberg_type: &Type,
) -> Result<ColumnDefault, String> {
    let Some(literal) = literal else {
        return Ok(ColumnDefault::Null);
    };

    match (literal, iceberg_type) {
        (
            IcebergLiteral::Primitive(PrimitiveLiteral::Boolean(value)),
            Type::Primitive(PrimitiveType::Boolean),
        ) => Ok(ColumnDefault::Boolean(*value)),
        (
            IcebergLiteral::Primitive(PrimitiveLiteral::Int(value)),
            Type::Primitive(PrimitiveType::Int),
        ) => Ok(ColumnDefault::Int32(*value)),
        (
            IcebergLiteral::Primitive(PrimitiveLiteral::Long(value)),
            Type::Primitive(PrimitiveType::Long),
        ) => Ok(ColumnDefault::Int64(*value)),
        (
            IcebergLiteral::Primitive(PrimitiveLiteral::Float(value)),
            Type::Primitive(PrimitiveType::Float),
        ) => Ok(ColumnDefault::Float32 {
            bits: value.0.to_bits(),
        }),
        (
            IcebergLiteral::Primitive(PrimitiveLiteral::Double(value)),
            Type::Primitive(PrimitiveType::Double),
        ) => Ok(ColumnDefault::Float64 {
            bits: value.0.to_bits(),
        }),
        (
            IcebergLiteral::Primitive(PrimitiveLiteral::Int128(unscaled)),
            Type::Primitive(PrimitiveType::Decimal { precision, scale }),
        ) => Ok(ColumnDefault::Decimal {
            unscaled: *unscaled,
            precision: u8::try_from(*precision)
                .map_err(|_| format!("Iceberg DECIMAL precision {precision} does not fit u8"))?,
            scale: i8::try_from(*scale)
                .map_err(|_| format!("Iceberg DECIMAL scale {scale} does not fit i8"))?,
        }),
        (
            IcebergLiteral::Primitive(PrimitiveLiteral::String(value)),
            Type::Primitive(PrimitiveType::String),
        ) => Ok(ColumnDefault::String(value.clone())),
        (
            IcebergLiteral::Primitive(PrimitiveLiteral::Binary(value)),
            Type::Primitive(PrimitiveType::Binary),
        ) => Ok(ColumnDefault::Binary(value.clone())),
        (
            IcebergLiteral::Primitive(PrimitiveLiteral::Int(days_since_epoch)),
            Type::Primitive(PrimitiveType::Date),
        ) => Ok(ColumnDefault::Date {
            days_since_epoch: *days_since_epoch,
        }),
        (
            IcebergLiteral::Primitive(PrimitiveLiteral::Long(micros_since_midnight)),
            Type::Primitive(PrimitiveType::Time),
        ) => Ok(ColumnDefault::TimeMicros {
            micros_since_midnight: *micros_since_midnight,
        }),
        (
            IcebergLiteral::Primitive(PrimitiveLiteral::Long(micros_since_epoch)),
            Type::Primitive(PrimitiveType::Timestamp),
        ) => Ok(ColumnDefault::TimestampMicros {
            micros_since_epoch: *micros_since_epoch,
        }),
        (
            IcebergLiteral::Primitive(PrimitiveLiteral::Long(micros_since_epoch)),
            Type::Primitive(PrimitiveType::Timestamptz),
        ) => Ok(ColumnDefault::TimestamptzMicros {
            micros_since_epoch: *micros_since_epoch,
        }),
        (
            IcebergLiteral::Primitive(PrimitiveLiteral::Long(nanos_since_epoch)),
            Type::Primitive(PrimitiveType::TimestampNs),
        ) => Ok(ColumnDefault::TimestampNanos {
            nanos_since_epoch: *nanos_since_epoch,
        }),
        (
            IcebergLiteral::Primitive(PrimitiveLiteral::Long(nanos_since_epoch)),
            Type::Primitive(PrimitiveType::TimestamptzNs),
        ) => Ok(ColumnDefault::TimestamptzNanos {
            nanos_since_epoch: *nanos_since_epoch,
        }),
        (
            IcebergLiteral::Primitive(PrimitiveLiteral::UInt128(value)),
            Type::Primitive(PrimitiveType::Uuid),
        ) => Ok(ColumnDefault::Uuid(value.to_be_bytes())),
        (
            IcebergLiteral::Primitive(PrimitiveLiteral::Binary(bytes)),
            Type::Primitive(PrimitiveType::Fixed(size)),
        ) => {
            let byte_len = u64::try_from(bytes.len())
                .map_err(|_| "FIXED default byte length does not fit u64".to_string())?;
            if byte_len != *size {
                return Err(format!(
                    "FIXED default size {size} does not match byte length {byte_len}"
                ));
            }
            Ok(ColumnDefault::Fixed {
                size: *size,
                bytes: bytes.clone(),
            })
        }
        (IcebergLiteral::Struct(value), Type::Struct(struct_type)) => {
            if value.fields().len() != struct_type.fields().len() {
                return Err(format!(
                    "Iceberg struct default has {} fields but type has {} fields",
                    value.fields().len(),
                    struct_type.fields().len()
                ));
            }
            let fields = value
                .iter()
                .zip(struct_type.fields())
                .map(|(field_value, field)| {
                    Ok((
                        field.name.clone(),
                        iceberg_literal_to_nested_column_default(
                            field_value,
                            field.field_type.as_ref(),
                        )?,
                    ))
                })
                .collect::<Result<Vec<_>, String>>()?;
            Ok(ColumnDefault::Struct(fields))
        }
        (IcebergLiteral::List(elements), Type::List(list_type)) => Ok(ColumnDefault::Array(
            elements
                .iter()
                .map(|element| {
                    iceberg_literal_to_nested_column_default(
                        element.as_ref(),
                        list_type.element_field.field_type.as_ref(),
                    )
                })
                .collect::<Result<Vec<_>, _>>()?,
        )),
        (IcebergLiteral::Map(map), Type::Map(map_type)) => Ok(ColumnDefault::Map(
            map.clone()
                .into_iter()
                .map(|(key, value)| {
                    Ok((
                        iceberg_literal_to_nested_column_default(
                            Some(&key),
                            map_type.key_field.field_type.as_ref(),
                        )?,
                        iceberg_literal_to_nested_column_default(
                            value.as_ref(),
                            map_type.value_field.field_type.as_ref(),
                        )?,
                    ))
                })
                .collect::<Result<Vec<_>, String>>()?,
        )),
        (IcebergLiteral::Primitive(PrimitiveLiteral::AboveMax | PrimitiveLiteral::BelowMin), _) => {
            Err("Iceberg bound sentinel cannot be used as a column default".to_string())
        }
        (_, Type::Primitive(PrimitiveType::Variant)) => {
            Err("Iceberg Variant column defaults are not supported".to_string())
        }
        (literal, iceberg_type) => Err(format!(
            "Iceberg default literal type does not match authoritative type: literal={literal:?} type={iceberg_type:?}"
        )),
    }
}

pub(crate) fn column_default_to_iceberg_literal(
    value: &ColumnDefault,
    iceberg_type: &Type,
) -> Result<IcebergLiteral, String> {
    validate_column_default(value)?;
    column_default_to_nested_iceberg_literal(value, iceberg_type)?.ok_or_else(|| {
        "top-level column default cannot be converted to an Iceberg NULL literal".to_string()
    })
}

fn column_default_to_nested_iceberg_literal(
    value: &ColumnDefault,
    iceberg_type: &Type,
) -> Result<Option<IcebergLiteral>, String> {
    let primitive = match (value, iceberg_type) {
        (ColumnDefault::Null, _) => return Ok(None),
        (ColumnDefault::Boolean(value), Type::Primitive(PrimitiveType::Boolean)) => {
            PrimitiveLiteral::Boolean(*value)
        }
        (ColumnDefault::Int32(value), Type::Primitive(PrimitiveType::Int)) => {
            PrimitiveLiteral::Int(*value)
        }
        (ColumnDefault::Int64(value), Type::Primitive(PrimitiveType::Long)) => {
            PrimitiveLiteral::Long(*value)
        }
        (ColumnDefault::Float32 { bits }, Type::Primitive(PrimitiveType::Float)) => {
            PrimitiveLiteral::Float(ordered_float::OrderedFloat(f32::from_bits(*bits)))
        }
        (ColumnDefault::Float64 { bits }, Type::Primitive(PrimitiveType::Double)) => {
            PrimitiveLiteral::Double(ordered_float::OrderedFloat(f64::from_bits(*bits)))
        }
        (
            ColumnDefault::Decimal {
                unscaled,
                precision,
                scale,
            },
            Type::Primitive(PrimitiveType::Decimal {
                precision: iceberg_precision,
                scale: iceberg_scale,
            }),
        ) => {
            if *scale < 0 {
                return Err(format!("negative DECIMAL scale {scale} is not supported"));
            }
            if u32::from(*precision) != *iceberg_precision
                || u32::try_from(*scale).ok() != Some(*iceberg_scale)
            {
                return Err(format!(
                    "column DECIMAL({precision},{scale}) does not match Iceberg DECIMAL({iceberg_precision},{iceberg_scale})"
                ));
            }
            PrimitiveLiteral::Int128(*unscaled)
        }
        (ColumnDefault::String(value), Type::Primitive(PrimitiveType::String)) => {
            PrimitiveLiteral::String(value.clone())
        }
        (ColumnDefault::Binary(value), Type::Primitive(PrimitiveType::Binary)) => {
            PrimitiveLiteral::Binary(value.clone())
        }
        (ColumnDefault::Date { days_since_epoch }, Type::Primitive(PrimitiveType::Date)) => {
            PrimitiveLiteral::Int(*days_since_epoch)
        }
        (
            ColumnDefault::TimeMicros {
                micros_since_midnight,
            },
            Type::Primitive(PrimitiveType::Time),
        ) => PrimitiveLiteral::Long(*micros_since_midnight),
        (
            ColumnDefault::TimestampMicros { micros_since_epoch },
            Type::Primitive(PrimitiveType::Timestamp),
        ) => PrimitiveLiteral::Long(*micros_since_epoch),
        (
            ColumnDefault::TimestamptzMicros { micros_since_epoch },
            Type::Primitive(PrimitiveType::Timestamptz),
        ) => PrimitiveLiteral::Long(*micros_since_epoch),
        (
            ColumnDefault::TimestampNanos { nanos_since_epoch },
            Type::Primitive(PrimitiveType::TimestampNs),
        ) => PrimitiveLiteral::Long(*nanos_since_epoch),
        (
            ColumnDefault::TimestamptzNanos { nanos_since_epoch },
            Type::Primitive(PrimitiveType::TimestamptzNs),
        ) => PrimitiveLiteral::Long(*nanos_since_epoch),
        (ColumnDefault::Uuid(bytes), Type::Primitive(PrimitiveType::Uuid)) => {
            PrimitiveLiteral::UInt128(u128::from_be_bytes(*bytes))
        }
        (
            ColumnDefault::Fixed { size, bytes },
            Type::Primitive(PrimitiveType::Fixed(iceberg_size)),
        ) => {
            if size != iceberg_size {
                return Err(format!(
                    "FIXED default size {size} does not match Iceberg FIXED size {iceberg_size}"
                ));
            }
            PrimitiveLiteral::Binary(bytes.clone())
        }
        (ColumnDefault::Struct(fields), Type::Struct(struct_type)) => {
            if fields.len() != struct_type.fields().len() {
                return Err(format!(
                    "column struct default has {} fields but Iceberg type has {} fields",
                    fields.len(),
                    struct_type.fields().len()
                ));
            }
            let mut values = Vec::with_capacity(fields.len());
            for ((name, field_value), field) in fields.iter().zip(struct_type.fields()) {
                if name != &field.name {
                    return Err(format!(
                        "column struct default field {name:?} does not match Iceberg field {:?}",
                        field.name
                    ));
                }
                values.push(column_default_to_nested_iceberg_literal(
                    field_value,
                    field.field_type.as_ref(),
                )?);
            }
            return Ok(Some(IcebergLiteral::Struct(values.into_iter().collect())));
        }
        (ColumnDefault::Array(elements), Type::List(list_type)) => {
            let values = elements
                .iter()
                .map(|element| {
                    column_default_to_nested_iceberg_literal(
                        element,
                        list_type.element_field.field_type.as_ref(),
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            return Ok(Some(IcebergLiteral::List(values)));
        }
        (ColumnDefault::Map(entries), Type::Map(map_type)) => {
            let mut map = IcebergMap::new();
            for (key, value) in entries {
                let key = column_default_to_nested_iceberg_literal(
                    key,
                    map_type.key_field.field_type.as_ref(),
                )?
                .ok_or_else(|| "map key cannot be NULL".to_string())?;
                let value = column_default_to_nested_iceberg_literal(
                    value,
                    map_type.value_field.field_type.as_ref(),
                )?;
                if map.insert(key, value).is_some() {
                    return Err("duplicate map key after Iceberg conversion".to_string());
                }
            }
            return Ok(Some(IcebergLiteral::Map(map)));
        }
        (_, Type::Primitive(PrimitiveType::Variant)) => {
            return Err("Iceberg Variant column defaults are not supported".to_string());
        }
        (value, iceberg_type) => {
            return Err(format!(
                "column default type does not match authoritative Iceberg type: default={value:?} type={iceberg_type:?}"
            ));
        }
    };
    Ok(Some(IcebergLiteral::Primitive(primitive)))
}

pub(crate) fn require_v3_for_column_default(
    format_version: FormatVersion,
    default: Option<&ColumnDefault>,
) -> Result<(), String> {
    if default.is_some() && !matches!(format_version, FormatVersion::V3) {
        return Err("non-NULL DEFAULT requires Iceberg format-version 3; \
             set TBLPROPERTIES('format-version'='3')"
            .to_string());
    }
    Ok(())
}

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int32Array, Int64Array, LargeBinaryArray, ListArray, StringArray, TimestampMicrosecondArray,
    TimestampNanosecondArray,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, TimeUnit};

/// Build an Arrow constant array of length `row_count` whose every element is
/// the value encoded by `literal`. The literal's runtime type must agree with
/// `target_type`; mismatches fail fast.
pub(crate) fn literal_to_constant_array(
    literal: &IcebergLiteral,
    target_type: &DataType,
    row_count: usize,
) -> Result<ArrayRef, String> {
    // Handle non-primitive Iceberg literals (List, Map) before the primitive match.
    match (literal, target_type) {
        (IcebergLiteral::List(elems), DataType::List(element_field)) => {
            if !elems.is_empty() {
                return Err(
                    "non-empty List initial-default is not yet supported by the read path"
                        .to_string(),
                );
            }
            // Build a ListArray of `row_count` empty lists.
            let inner_type = element_field.data_type();
            let values = arrow::array::new_empty_array(inner_type);
            // Offsets: 0, 0, 0, … (row_count+1 zeros → all lists are empty)
            let offsets: Vec<i32> = vec![0; row_count + 1];
            let list_array = ListArray::new(
                element_field.clone(),
                OffsetBuffer::new(offsets.into()),
                values,
                None,
            );
            return Ok(Arc::new(list_array) as ArrayRef);
        }
        (IcebergLiteral::Map(map), DataType::Map(entries_field, _)) => {
            if !map.is_empty() {
                return Err(
                    "non-empty Map initial-default is not yet supported by the read path"
                        .to_string(),
                );
            }
            // Build a MapArray of `row_count` empty maps.
            // A MapArray uses a StructArray of (keys, values) wrapped with an
            // offsets buffer.  For all-empty maps the struct has 0 rows.
            use arrow::array::{MapArray, StructArray};
            let DataType::Struct(entry_fields) = entries_field.data_type() else {
                return Err(format!(
                    "unexpected Map entry field type: {:?}",
                    entries_field.data_type()
                ));
            };
            let empty_columns: Vec<ArrayRef> = entry_fields
                .iter()
                .map(|f| arrow::array::new_empty_array(f.data_type()))
                .collect();
            let entries_struct = StructArray::new(entry_fields.clone(), empty_columns, None);
            let offsets: Vec<i32> = vec![0; row_count + 1];
            let map_array = MapArray::new(
                entries_field.clone(),
                OffsetBuffer::new(offsets.into()),
                entries_struct,
                None,
                false,
            );
            return Ok(Arc::new(map_array) as ArrayRef);
        }
        _ => {}
    }

    let IcebergLiteral::Primitive(prim) = literal else {
        return Err(format!(
            "unsupported initial-default literal kind: {literal:?}"
        ));
    };
    Ok(match (prim, target_type) {
        (PrimitiveLiteral::Boolean(v), DataType::Boolean) => {
            Arc::new(BooleanArray::from(vec![*v; row_count])) as ArrayRef
        }
        (PrimitiveLiteral::Int(v), DataType::Int32) => {
            Arc::new(Int32Array::from(vec![*v; row_count])) as ArrayRef
        }
        (PrimitiveLiteral::Long(v), DataType::Int64) => {
            Arc::new(Int64Array::from(vec![*v; row_count])) as ArrayRef
        }
        (PrimitiveLiteral::Float(v), DataType::Float32) => {
            Arc::new(Float32Array::from(vec![v.0; row_count])) as ArrayRef
        }
        (PrimitiveLiteral::Double(v), DataType::Float64) => {
            Arc::new(Float64Array::from(vec![v.0; row_count])) as ArrayRef
        }
        (PrimitiveLiteral::Int128(v), DataType::Decimal128(precision, scale)) => Arc::new(
            Decimal128Array::from(vec![*v; row_count])
                .with_precision_and_scale(*precision, *scale)
                .map_err(|e| format!("decimal default cast: {e}"))?,
        )
            as ArrayRef,
        (PrimitiveLiteral::String(s), DataType::Utf8) => {
            Arc::new(StringArray::from(vec![s.as_str(); row_count])) as ArrayRef
        }
        (PrimitiveLiteral::Int(v), DataType::Date32) => {
            Arc::new(Date32Array::from(vec![*v; row_count])) as ArrayRef
        }
        (PrimitiveLiteral::Long(v), DataType::Timestamp(TimeUnit::Nanosecond, _)) => {
            Arc::new(TimestampNanosecondArray::from(vec![*v; row_count])) as ArrayRef
        }
        (PrimitiveLiteral::Long(v), DataType::Timestamp(TimeUnit::Microsecond, _)) => {
            Arc::new(TimestampMicrosecondArray::from(vec![*v; row_count])) as ArrayRef
        }
        (PrimitiveLiteral::Binary(b), DataType::Binary) => {
            let slice = b.as_slice();
            Arc::new(BinaryArray::from(vec![slice; row_count])) as ArrayRef
        }
        (PrimitiveLiteral::Binary(b), DataType::LargeBinary) => {
            let slice = b.as_slice();
            Arc::new(LargeBinaryArray::from(vec![slice; row_count])) as ArrayRef
        }
        (prim, ty) => {
            return Err(format!(
                "unsupported initial-default literal {prim:?} for arrow type {ty:?}"
            ));
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::spec::{ListType, MapType, NestedField, PrimitiveType, Struct, StructType, Type};
    use novarocks_catalog::schema::ColumnDefault;

    fn assert_iceberg_default_round_trip(
        literal: IcebergLiteral,
        iceberg_type: Type,
        expected: ColumnDefault,
    ) {
        assert_eq!(
            iceberg_literal_to_column_default(&literal, &iceberg_type).unwrap(),
            expected
        );
        assert_eq!(
            column_default_to_iceberg_literal(&expected, &iceberg_type).unwrap(),
            literal
        );
    }

    #[test]
    fn iceberg_default_primitive_types_round_trip() {
        let cases = [
            (
                IcebergLiteral::Primitive(PrimitiveLiteral::Boolean(true)),
                Type::Primitive(PrimitiveType::Boolean),
                ColumnDefault::Boolean(true),
            ),
            (
                IcebergLiteral::Primitive(PrimitiveLiteral::Int(i32::MIN)),
                Type::Primitive(PrimitiveType::Int),
                ColumnDefault::Int32(i32::MIN),
            ),
            (
                IcebergLiteral::Primitive(PrimitiveLiteral::Long(i64::MAX)),
                Type::Primitive(PrimitiveType::Long),
                ColumnDefault::Int64(i64::MAX),
            ),
            (
                IcebergLiteral::Primitive(PrimitiveLiteral::Int128(-12_345)),
                Type::Primitive(PrimitiveType::Decimal {
                    precision: 10,
                    scale: 2,
                }),
                ColumnDefault::Decimal {
                    unscaled: -12_345,
                    precision: 10,
                    scale: 2,
                },
            ),
            (
                IcebergLiteral::Primitive(PrimitiveLiteral::String("value".to_string())),
                Type::Primitive(PrimitiveType::String),
                ColumnDefault::String("value".to_string()),
            ),
            (
                IcebergLiteral::Primitive(PrimitiveLiteral::Binary(vec![0x00, 0x80, 0xff])),
                Type::Primitive(PrimitiveType::Binary),
                ColumnDefault::Binary(vec![0x00, 0x80, 0xff]),
            ),
            (
                IcebergLiteral::Primitive(PrimitiveLiteral::Int(-1)),
                Type::Primitive(PrimitiveType::Date),
                ColumnDefault::Date {
                    days_since_epoch: -1,
                },
            ),
            (
                IcebergLiteral::Primitive(PrimitiveLiteral::Long(86_399_999_999)),
                Type::Primitive(PrimitiveType::Time),
                ColumnDefault::TimeMicros {
                    micros_since_midnight: 86_399_999_999,
                },
            ),
            (
                IcebergLiteral::Primitive(PrimitiveLiteral::Long(-1)),
                Type::Primitive(PrimitiveType::Timestamp),
                ColumnDefault::TimestampMicros {
                    micros_since_epoch: -1,
                },
            ),
            (
                IcebergLiteral::Primitive(PrimitiveLiteral::Long(1)),
                Type::Primitive(PrimitiveType::Timestamptz),
                ColumnDefault::TimestamptzMicros {
                    micros_since_epoch: 1,
                },
            ),
            (
                IcebergLiteral::Primitive(PrimitiveLiteral::Long(-2)),
                Type::Primitive(PrimitiveType::TimestampNs),
                ColumnDefault::TimestampNanos {
                    nanos_since_epoch: -2,
                },
            ),
            (
                IcebergLiteral::Primitive(PrimitiveLiteral::Long(2)),
                Type::Primitive(PrimitiveType::TimestamptzNs),
                ColumnDefault::TimestamptzNanos {
                    nanos_since_epoch: 2,
                },
            ),
            (
                IcebergLiteral::Primitive(PrimitiveLiteral::Binary(vec![0x00, 0x7f, 0xff])),
                Type::Primitive(PrimitiveType::Fixed(3)),
                ColumnDefault::Fixed {
                    size: 3,
                    bytes: vec![0x00, 0x7f, 0xff],
                },
            ),
        ];

        for (literal, iceberg_type, expected) in cases {
            assert_iceberg_default_round_trip(literal, iceberg_type, expected);
        }
    }

    #[test]
    fn iceberg_default_float_bits_round_trip_including_non_finite() {
        for bits in [(-0.0_f32).to_bits(), 0x7fc0_1234, f32::INFINITY.to_bits()] {
            assert_iceberg_default_round_trip(
                IcebergLiteral::Primitive(PrimitiveLiteral::Float(ordered_float::OrderedFloat(
                    f32::from_bits(bits),
                ))),
                Type::Primitive(PrimitiveType::Float),
                ColumnDefault::Float32 { bits },
            );
            let IcebergLiteral::Primitive(PrimitiveLiteral::Float(outbound)) =
                column_default_to_iceberg_literal(
                    &ColumnDefault::Float32 { bits },
                    &Type::Primitive(PrimitiveType::Float),
                )
                .unwrap()
            else {
                panic!("expected Iceberg float literal");
            };
            assert_eq!(outbound.0.to_bits(), bits);
        }
        for bits in [
            (-0.0_f64).to_bits(),
            0x7ff8_0000_0000_1234,
            f64::NEG_INFINITY.to_bits(),
        ] {
            assert_iceberg_default_round_trip(
                IcebergLiteral::Primitive(PrimitiveLiteral::Double(ordered_float::OrderedFloat(
                    f64::from_bits(bits),
                ))),
                Type::Primitive(PrimitiveType::Double),
                ColumnDefault::Float64 { bits },
            );
            let IcebergLiteral::Primitive(PrimitiveLiteral::Double(outbound)) =
                column_default_to_iceberg_literal(
                    &ColumnDefault::Float64 { bits },
                    &Type::Primitive(PrimitiveType::Double),
                )
                .unwrap()
            else {
                panic!("expected Iceberg double literal");
            };
            assert_eq!(outbound.0.to_bits(), bits);
        }
    }

    #[test]
    fn iceberg_default_uuid_uses_network_byte_order() {
        let parsed = uuid::Uuid::parse_str("00112233-4455-6677-8899-aabbccddeeff").unwrap();
        let bytes = [
            0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd,
            0xee, 0xff,
        ];
        assert_eq!(parsed.into_bytes(), bytes);
        let iceberg_type = Type::Primitive(PrimitiveType::Uuid);
        let literal = IcebergLiteral::Primitive(PrimitiveLiteral::UInt128(parsed.as_u128()));
        assert_iceberg_default_round_trip(
            literal.clone(),
            iceberg_type.clone(),
            ColumnDefault::Uuid(bytes),
        );
        assert_eq!(
            literal.try_into_json(&iceberg_type).unwrap(),
            serde_json::Value::String("00112233-4455-6677-8899-aabbccddeeff".to_string())
        );
    }

    #[test]
    fn iceberg_default_rejects_timezone_cross_targets() {
        let cases = [
            (
                ColumnDefault::TimestampMicros {
                    micros_since_epoch: 1,
                },
                Type::Primitive(PrimitiveType::Timestamptz),
            ),
            (
                ColumnDefault::TimestamptzMicros {
                    micros_since_epoch: 1,
                },
                Type::Primitive(PrimitiveType::Timestamp),
            ),
            (
                ColumnDefault::TimestampNanos {
                    nanos_since_epoch: 1,
                },
                Type::Primitive(PrimitiveType::TimestamptzNs),
            ),
            (
                ColumnDefault::TimestamptzNanos {
                    nanos_since_epoch: 1,
                },
                Type::Primitive(PrimitiveType::TimestampNs),
            ),
        ];

        for (value, iceberg_type) in cases {
            assert!(
                column_default_to_iceberg_literal(&value, &iceberg_type)
                    .unwrap_err()
                    .contains("does not match authoritative Iceberg type"),
                "value={value:?} iceberg_type={iceberg_type:?}"
            );
        }
    }

    #[test]
    fn iceberg_default_nested_round_trip_preserves_name_order_and_nulls() {
        let map_type = Type::Map(MapType::new(
            Arc::new(NestedField::required(
                4,
                "key",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                5,
                "value",
                Type::Primitive(PrimitiveType::Int),
            )),
        ));
        let list_type = Type::List(ListType::new(Arc::new(NestedField::optional(
            3,
            "element",
            map_type.clone(),
        ))));
        let struct_type = Type::Struct(StructType::new(vec![
            Arc::new(NestedField::optional(
                1,
                "id",
                Type::Primitive(PrimitiveType::Int),
            )),
            Arc::new(NestedField::optional(2, "items", list_type)),
        ]));

        let mut map = IcebergMap::new();
        map.insert(
            IcebergLiteral::Primitive(PrimitiveLiteral::String("first".to_string())),
            Some(IcebergLiteral::Primitive(PrimitiveLiteral::Int(1))),
        );
        map.insert(
            IcebergLiteral::Primitive(PrimitiveLiteral::String("second".to_string())),
            None,
        );
        let literal = IcebergLiteral::Struct(Struct::from_iter([
            Some(IcebergLiteral::Primitive(PrimitiveLiteral::Int(7))),
            Some(IcebergLiteral::List(vec![
                Some(IcebergLiteral::Map(map)),
                None,
            ])),
        ]));
        let expected = ColumnDefault::Struct(vec![
            ("id".to_string(), ColumnDefault::Int32(7)),
            (
                "items".to_string(),
                ColumnDefault::Array(vec![
                    ColumnDefault::Map(vec![
                        (
                            ColumnDefault::String("first".to_string()),
                            ColumnDefault::Int32(1),
                        ),
                        (
                            ColumnDefault::String("second".to_string()),
                            ColumnDefault::Null,
                        ),
                    ]),
                    ColumnDefault::Null,
                ]),
            ),
        ]);

        assert_iceberg_default_round_trip(literal, struct_type, expected);
    }

    #[test]
    fn iceberg_default_rejects_invalid_type_shape_and_map_keys() {
        assert!(
            iceberg_literal_to_column_default(
                &IcebergLiteral::Primitive(PrimitiveLiteral::AboveMax),
                &Type::Primitive(PrimitiveType::Int),
            )
            .unwrap_err()
            .contains("bound sentinel")
        );
        assert!(
            iceberg_literal_to_column_default(
                &IcebergLiteral::Primitive(PrimitiveLiteral::Binary(vec![1, 2])),
                &Type::Primitive(PrimitiveType::Fixed(3)),
            )
            .unwrap_err()
            .contains("FIXED")
        );
        assert!(
            column_default_to_iceberg_literal(
                &ColumnDefault::Decimal {
                    unscaled: 1,
                    precision: 10,
                    scale: -1,
                },
                &Type::Primitive(PrimitiveType::Decimal {
                    precision: 10,
                    scale: 0,
                }),
            )
            .unwrap_err()
            .contains("negative DECIMAL scale")
        );

        let map_type = Type::Map(MapType::new(
            Arc::new(NestedField::required(
                1,
                "key",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                2,
                "value",
                Type::Primitive(PrimitiveType::Int),
            )),
        ));
        assert!(
            column_default_to_iceberg_literal(
                &ColumnDefault::Map(vec![(ColumnDefault::Null, ColumnDefault::Int32(1))]),
                &map_type,
            )
            .unwrap_err()
            .contains("map key")
        );
        assert!(
            column_default_to_iceberg_literal(
                &ColumnDefault::Map(vec![
                    (
                        ColumnDefault::String("duplicate".to_string()),
                        ColumnDefault::Int32(1),
                    ),
                    (
                        ColumnDefault::String("duplicate".to_string()),
                        ColumnDefault::Int32(2),
                    ),
                ]),
                &map_type,
            )
            .unwrap_err()
            .contains("duplicate map key")
        );
        assert!(
            iceberg_literal_to_column_default(
                &IcebergLiteral::Primitive(PrimitiveLiteral::Binary(Vec::new())),
                &Type::Primitive(PrimitiveType::Variant),
            )
            .unwrap_err()
            .contains("Variant")
        );
    }

    #[test]
    fn iceberg_default_rejects_struct_shape_and_positive_decimal_mismatches() {
        let struct_type = Type::Struct(StructType::new(vec![Arc::new(NestedField::optional(
            1,
            "expected",
            Type::Primitive(PrimitiveType::Int),
        ))]));
        assert!(
            column_default_to_iceberg_literal(&ColumnDefault::Struct(Vec::new()), &struct_type)
                .unwrap_err()
                .contains("fields")
        );
        assert!(
            column_default_to_iceberg_literal(
                &ColumnDefault::Struct(vec![("actual".to_string(), ColumnDefault::Int32(1),)]),
                &struct_type,
            )
            .unwrap_err()
            .contains("does not match Iceberg field")
        );

        let decimal = ColumnDefault::Decimal {
            unscaled: 123,
            precision: 10,
            scale: 2,
        };
        assert!(
            column_default_to_iceberg_literal(
                &decimal,
                &Type::Primitive(PrimitiveType::Decimal {
                    precision: 11,
                    scale: 2,
                }),
            )
            .unwrap_err()
            .contains("does not match Iceberg DECIMAL")
        );
        assert!(
            column_default_to_iceberg_literal(
                &decimal,
                &Type::Primitive(PrimitiveType::Decimal {
                    precision: 10,
                    scale: 3,
                }),
            )
            .unwrap_err()
            .contains("does not match Iceberg DECIMAL")
        );
    }

    #[test]
    fn require_v3_for_column_default_preserves_legacy_error() {
        assert_eq!(
            require_v3_for_column_default(FormatVersion::V2, Some(&ColumnDefault::Int32(1)),)
                .unwrap_err(),
            "non-NULL DEFAULT requires Iceberg format-version 3; set TBLPROPERTIES('format-version'='3')"
        );
        require_v3_for_column_default(FormatVersion::V3, Some(&ColumnDefault::Int32(1))).unwrap();
        require_v3_for_column_default(FormatVersion::V2, None).unwrap();
    }

    use arrow::array::{Array, Int32Array, StringArray};
    use arrow::datatypes::DataType;

    #[test]
    fn literal_to_constant_array_int32() {
        let lit = IcebergLiteral::Primitive(PrimitiveLiteral::Int(5));
        let arr = literal_to_constant_array(&lit, &DataType::Int32, 3).expect("array");
        let i32arr = arr.as_any().downcast_ref::<Int32Array>().expect("i32");
        assert_eq!(i32arr.len(), 3);
        assert_eq!(i32arr.value(0), 5);
        assert_eq!(i32arr.value(2), 5);
    }

    #[test]
    fn literal_to_constant_array_string() {
        let lit = IcebergLiteral::Primitive(PrimitiveLiteral::String("hi".into()));
        let arr = literal_to_constant_array(&lit, &DataType::Utf8, 2).expect("array");
        let strarr = arr.as_any().downcast_ref::<StringArray>().expect("str");
        assert_eq!(strarr.value(0), "hi");
        assert_eq!(strarr.value(1), "hi");
    }

    #[test]
    fn literal_to_constant_array_zero_rows() {
        let lit = IcebergLiteral::Primitive(PrimitiveLiteral::Int(5));
        let arr = literal_to_constant_array(&lit, &DataType::Int32, 0).expect("array");
        assert_eq!(arr.len(), 0);
    }

    #[test]
    fn literal_to_constant_array_unsupported_type_fails_fast() {
        // Use a (Long, Float64) mismatch — Long should not produce a Float64 array.
        let lit = IcebergLiteral::Primitive(PrimitiveLiteral::Long(5));
        let err =
            literal_to_constant_array(&lit, &DataType::Float64, 1).expect_err("type mismatch");
        assert!(err.contains("unsupported"), "unexpected error: {err}");
    }

    // --- literal_to_constant_array for List and Map ---

    #[test]
    fn literal_to_constant_array_empty_list() {
        use arrow::array::ListArray;
        use arrow::datatypes::Field;

        let lit = IcebergLiteral::List(vec![]);
        let element_field = Arc::new(Field::new("item", DataType::Int32, true));
        let list_type = DataType::List(element_field);
        let arr = literal_to_constant_array(&lit, &list_type, 3).expect("empty list array");
        assert_eq!(arr.len(), 3);
        let list_arr = arr.as_any().downcast_ref::<ListArray>().expect("ListArray");
        // All 3 rows should be empty lists.
        for i in 0..3 {
            let row = list_arr.value(i);
            assert_eq!(row.len(), 0, "row {i} should be an empty list");
        }
    }

    #[test]
    fn long_default_for_nanosecond_target_builds_nanosecond_array() {
        use arrow::array::TimestampNanosecondArray;
        use arrow::datatypes::TimeUnit;
        let lit = IcebergLiteral::Primitive(PrimitiveLiteral::Long(1_704_164_645_123_456_789));
        let arr =
            literal_to_constant_array(&lit, &DataType::Timestamp(TimeUnit::Nanosecond, None), 2)
                .expect("nanosecond array");
        let a = arr
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("expected TimestampNanosecondArray, got a different type");
        assert_eq!(a.value(0), 1_704_164_645_123_456_789);
        assert_eq!(a.len(), 2);
    }

    #[test]
    fn long_default_for_microsecond_target_builds_microsecond_array() {
        use arrow::array::TimestampMicrosecondArray;
        use arrow::datatypes::TimeUnit;
        let lit = IcebergLiteral::Primitive(PrimitiveLiteral::Long(1_704_110_400_000_000));
        let arr =
            literal_to_constant_array(&lit, &DataType::Timestamp(TimeUnit::Microsecond, None), 2)
                .expect("microsecond array");
        let a = arr
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("expected TimestampMicrosecondArray, got a different type");
        assert_eq!(a.value(0), 1_704_110_400_000_000);
        assert_eq!(a.len(), 2);
    }

    #[test]
    fn literal_to_constant_array_empty_map() {
        use arrow::array::MapArray;
        use arrow::datatypes::{Field, Fields};

        let lit = IcebergLiteral::Map(IcebergMap::new());
        let entry_fields = Fields::from(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Int32, true),
        ]);
        let entries_field = Arc::new(Field::new("entries", DataType::Struct(entry_fields), false));
        let map_type = DataType::Map(entries_field, false);
        let arr = literal_to_constant_array(&lit, &map_type, 2).expect("empty map array");
        assert_eq!(arr.len(), 2);
        let map_arr = arr.as_any().downcast_ref::<MapArray>().expect("MapArray");
        for i in 0..2 {
            let row = map_arr.value(i);
            assert_eq!(row.len(), 0, "row {i} should be an empty map");
        }
    }
}
