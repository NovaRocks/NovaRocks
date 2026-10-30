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

use std::sync::Arc;

use arrow_schema::{DataType, Field, Fields};

fn is_integer(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    )
}

fn wider_decimal_type(
    left_precision: u8,
    left_scale: i8,
    left_is_256: bool,
    right_precision: u8,
    right_scale: i8,
    right_is_256: bool,
) -> DataType {
    let scale = left_scale.max(right_scale);
    let left_int_digits = i16::from(left_precision) - i16::from(left_scale);
    let right_int_digits = i16::from(right_precision) - i16::from(right_scale);
    let precision =
        (left_int_digits.max(right_int_digits).max(0) + i16::from(scale)).clamp(1, 76) as u8;

    if left_is_256 || right_is_256 || precision > 38 {
        DataType::Decimal256(precision, scale)
    } else {
        DataType::Decimal128(precision, scale)
    }
}

/// Determine the wider type for unifying two types (comparisons, CASE, UNION, etc.).
pub fn wider_type(a: &DataType, b: &DataType) -> DataType {
    wider_type_with_construction::<std::convert::Infallible>(a, b, None).expect("pure wider type")
}
/// The controlled sibling consumes validated metadata-free type carriers.
/// Annotation imports remain with their original external author.
pub fn wider_type_with_construction<E>(
    a: &DataType,
    b: &DataType,
    observer: Option<&mut dyn FnMut(crate::ValueConstructionRequest) -> Result<(), E>>,
) -> Result<DataType, crate::ValueConstructionFailure<E>> {
    wider_type_core(
        a,
        b,
        &mut crate::value_construction::ValueConstruction::new(observer),
    )
}
fn wider_type_core<E>(
    a: &DataType,
    b: &DataType,
    creation: &mut crate::value_construction::ValueConstruction<'_, E>,
) -> Result<DataType, crate::ValueConstructionFailure<E>> {
    if a == b {
        return creation.cloned_data_type(a);
    }
    Ok(match (a, b) {
        (DataType::Null, other) | (other, DataType::Null) => creation.cloned_data_type(other)?,
        (l, r)
            if (crate::is_largeint_data_type(l) && is_integer(r))
                || (is_integer(l) && crate::is_largeint_data_type(r)) =>
        {
            DataType::FixedSizeBinary(crate::LARGEINT_BYTE_WIDTH)
        }
        (DataType::List(left_field), DataType::List(right_field)) => DataType::List(
            wider_nested_field(left_field.name(), left_field, right_field, creation)?,
        ),
        (DataType::Map(left_entries, _), DataType::Map(right_entries, _)) => {
            wider_map_type(left_entries, right_entries, creation)?
        }
        (DataType::Struct(left_fields), DataType::Struct(right_fields))
            if left_fields.len() == right_fields.len() =>
        {
            if let Some(fields) = wider_struct_fields_by_name(left_fields, right_fields, creation)?
            {
                return Ok(DataType::Struct(fields));
            }
            creation.collected_arc_slice::<Arc<Field>>(left_fields.len())?;
            DataType::Struct(Fields::from(
                left_fields
                    .iter()
                    .zip(right_fields.iter())
                    .map(|(left_field, right_field)| {
                        wider_nested_field(left_field.name(), left_field, right_field, creation)
                    })
                    .collect::<Result<Vec<_>, crate::ValueConstructionFailure<E>>>()?,
            ))
        }
        // VARCHAR wins before DECIMAL, matching StarRocks TypeManager:
        // getAssignmentCompatibleType handles string pairs before decimal
        // pairs, and ARRAY/MAP/STRUCT common types recurse through this rule.
        (DataType::Utf8, _) | (_, DataType::Utf8) => DataType::Utf8,
        (DataType::LargeUtf8, _) | (_, DataType::LargeUtf8) => DataType::Utf8,
        // Decimal + Decimal -> wider Decimal. Promote to Decimal256 when the
        // common precision exceeds Decimal128 or either side is already wide.
        (DataType::Decimal128(p1, s1), DataType::Decimal128(p2, s2)) => {
            wider_decimal_type(*p1, *s1, false, *p2, *s2, false)
        }
        (DataType::Decimal128(p1, s1), DataType::Decimal256(p2, s2)) => {
            wider_decimal_type(*p1, *s1, false, *p2, *s2, true)
        }
        (DataType::Decimal256(p1, s1), DataType::Decimal128(p2, s2)) => {
            wider_decimal_type(*p1, *s1, true, *p2, *s2, false)
        }
        (DataType::Decimal256(p1, s1), DataType::Decimal256(p2, s2)) => {
            wider_decimal_type(*p1, *s1, true, *p2, *s2, true)
        }
        // Decimal + Integer -> Decimal with room for both integer ranges.
        // Literal narrowing has already selected the integer width; copying
        // only the decimal metadata could understate a mixed array's precision.
        (
            DataType::Decimal128(_, _) | DataType::Decimal256(_, _),
            DataType::Int64 | DataType::Int32 | DataType::Int16 | DataType::Int8,
        )
        | (
            DataType::Int64 | DataType::Int32 | DataType::Int16 | DataType::Int8,
            DataType::Decimal128(_, _) | DataType::Decimal256(_, _),
        ) => match (a, b) {
            (DataType::Decimal128(p, s), integer) | (integer, DataType::Decimal128(p, s)) => {
                wider_decimal_type(*p, *s, false, integer_decimal_precision(integer), 0, false)
            }
            (DataType::Decimal256(p, s), integer) | (integer, DataType::Decimal256(p, s)) => {
                wider_decimal_type(*p, *s, true, integer_decimal_precision(integer), 0, false)
            }
            _ => unreachable!(),
        },
        // Decimal + Float -> Float64 (StarRocks FE: promote to Double).
        (
            DataType::Decimal128(_, _) | DataType::Decimal256(_, _),
            DataType::Float64 | DataType::Float32,
        )
        | (
            DataType::Float64 | DataType::Float32,
            DataType::Decimal128(_, _) | DataType::Decimal256(_, _),
        ) => DataType::Float64,
        // Decimal + other -> Decimal.
        (DataType::Decimal128(p, s), _) | (_, DataType::Decimal128(p, s)) => {
            DataType::Decimal128(*p, *s)
        }
        (DataType::Decimal256(p, s), _) | (_, DataType::Decimal256(p, s)) => {
            DataType::Decimal256(*p, *s)
        }
        // DATE + DATETIME -> DATETIME (StarRocks: only DATETIME signatures exist
        // for comparison/greatest/least/coalesce with mixed date+datetime input).
        (DataType::Timestamp(u, tz), DataType::Date32)
        | (DataType::Date32, DataType::Timestamp(u, tz)) => DataType::Timestamp(*u, tz.clone()),
        (DataType::Float64, _) | (_, DataType::Float64) => DataType::Float64,
        (DataType::Float32, _) | (_, DataType::Float32) => DataType::Float64,
        (DataType::Int64, _) | (_, DataType::Int64) => DataType::Int64,
        (DataType::Int32, _) | (_, DataType::Int32) => DataType::Int64,
        (DataType::Int16, _) | (_, DataType::Int16) => DataType::Int16,
        _ => creation.cloned_data_type(a)?,
    })
}

fn integer_decimal_precision(data_type: &DataType) -> u8 {
    match data_type {
        DataType::Int8 => 3,
        DataType::Int16 => 5,
        DataType::Int32 => 10,
        DataType::Int64 => 19,
        _ => unreachable!("only signed integer carriers need decimal range widening"),
    }
}

fn logical_storage_matches(logical: crate::ValueLogicalType, storage: &DataType) -> bool {
    match logical {
        crate::ValueLogicalType::Json => matches!(storage, DataType::Utf8 | DataType::LargeUtf8),
        crate::ValueLogicalType::Hll
        | crate::ValueLogicalType::Bitmap
        | crate::ValueLogicalType::Object
        | crate::ValueLogicalType::Percentile => {
            matches!(storage, DataType::Binary | DataType::LargeBinary)
        }
        _ => false,
    }
}

fn compatible_field_logical_type<E>(
    field: &Field,
    creation: &mut crate::value_construction::ValueConstruction<'_, E>,
) -> Result<Option<crate::ValueLogicalType>, crate::ValueConstructionFailure<E>> {
    let Some(value) = field.metadata().get(crate::NR_LOGICAL_TYPE_KEY) else {
        return Ok(None);
    };
    creation.name(value.trim().len())?;
    Ok(
        crate::ValueLogicalType::from_metadata_value(&value.trim().to_ascii_lowercase())
            .ok()
            .filter(|logical| logical_storage_matches(*logical, field.data_type())),
    )
}
fn wider_nested_field<E>(
    name: &str,
    left: &Field,
    right: &Field,
    creation: &mut crate::value_construction::ValueConstruction<'_, E>,
) -> Result<Arc<Field>, crate::ValueConstructionFailure<E>> {
    let data_type = wider_type_core(left.data_type(), right.data_type(), creation)?;
    let logical = match (left.data_type(), right.data_type()) {
        (DataType::Null, _) => compatible_field_logical_type(right, creation)?,
        (_, DataType::Null) => compatible_field_logical_type(left, creation)?,
        _ => {
            let a = compatible_field_logical_type(left, creation)?;
            let b = compatible_field_logical_type(right, creation)?;
            a.filter(|logical| b == Some(*logical))
        }
    }
    .filter(|logical| logical_storage_matches(*logical, &data_type));
    let mut invalid_source = false;
    for field in [left, right] {
        if !matches!(field.data_type(), DataType::Null)
            && field.metadata().contains_key(crate::NR_LOGICAL_TYPE_KEY)
            && compatible_field_logical_type(field, creation)?.is_none()
        {
            invalid_source = true;
            break;
        }
    }
    let mut erased_opaque = false;
    if matches!(data_type, DataType::LargeBinary) && logical.is_none() {
        for field in [left, right] {
            if compatible_field_logical_type(field, creation)?.is_some() {
                erased_opaque = true;
                break;
            }
        }
    }
    creation.name(name.len())?;
    let field = Field::new(name, data_type, left.is_nullable() || right.is_nullable());
    let field = if invalid_source || erased_opaque {
        field_marker(field, "invalid", creation)?
    } else {
        match logical {
            Some(logical) => field_marker(
                field,
                logical
                    .metadata_value()
                    .expect("compatible logical type is explicit"),
                creation,
            )?,
            None => field,
        }
    };
    creation
        .exact(|| crate::owned_resources::layout::arc_layout(std::alloc::Layout::new::<Field>()))?;
    Ok(Arc::new(field))
}
fn field_marker<E>(
    field: Field,
    value: &str,
    creation: &mut crate::value_construction::ValueConstruction<'_, E>,
) -> Result<Field, crate::ValueConstructionFailure<E>> {
    creation.name(crate::NR_LOGICAL_TYPE_KEY.len())?;
    creation.name(value.len())?;
    creation.table::<String, String>(1)?;
    creation.metadata_conversion(1, crate::NR_LOGICAL_TYPE_KEY.len())?;
    Ok(field.with_metadata(std::collections::HashMap::from([(
        crate::NR_LOGICAL_TYPE_KEY.to_owned(),
        value.to_owned(),
    )])))
}
fn wider_struct_fields_by_name<E>(
    left_fields: &Fields,
    right_fields: &Fields,
    creation: &mut crate::value_construction::ValueConstruction<'_, E>,
) -> Result<Option<Fields>, crate::ValueConstructionFailure<E>> {
    creation.table::<&str, &Arc<Field>>(right_fields.len())?;
    let right_by_name = right_fields
        .iter()
        .map(|field| (field.name().as_str(), field))
        .collect::<std::collections::HashMap<_, _>>();
    if left_fields
        .iter()
        .any(|field| !right_by_name.contains_key(field.name().as_str()))
    {
        return Ok(None);
    }
    creation.collected_arc_slice::<Arc<Field>>(left_fields.len())?;
    let fields = left_fields
        .iter()
        .map(|left_field| {
            let Some(right_field) = right_by_name.get(left_field.name().as_str()) else {
                return Ok(None);
            };
            wider_nested_field(left_field.name(), left_field, right_field, creation).map(Some)
        })
        .collect::<Result<Option<Vec<_>>, crate::ValueConstructionFailure<E>>>()?;
    Ok(fields.map(Fields::from))
}
fn wider_map_type<E>(
    left_entries: &Field,
    right_entries: &Field,
    creation: &mut crate::value_construction::ValueConstruction<'_, E>,
) -> Result<DataType, crate::ValueConstructionFailure<E>> {
    let malformed = |creation: &mut crate::value_construction::ValueConstruction<'_, E>| {
        creation.name(left_entries.name().len())?;
        creation.cloned_annotations(left_entries.metadata().len())?;
        // Field::clone copies its owned name and DataType Boxes; Metadata shares its Arc.
        creation.cloned_type_backing(left_entries.data_type())?;
        creation.exact(|| {
            crate::owned_resources::layout::arc_layout(std::alloc::Layout::new::<Field>())
        })?;
        Ok(DataType::Map(Arc::new(left_entries.clone()), false))
    };
    let DataType::Struct(left_fields) = left_entries.data_type() else {
        return malformed(creation);
    };
    let DataType::Struct(right_fields) = right_entries.data_type() else {
        return malformed(creation);
    };
    if left_fields.len() != 2 || right_fields.len() != 2 {
        return malformed(creation);
    };
    creation.array::<Arc<Field>>(2)?;
    let fields = vec![
        wider_nested_field("key", &left_fields[0], &right_fields[0], creation)?,
        wider_nested_field("value", &left_fields[1], &right_fields[1], creation)?,
    ];
    creation.arc_slice::<Arc<Field>>(2)?;
    let fields = DataType::Struct(fields.into());
    creation.name("entries".len())?;
    let field = Field::new("entries", fields, false);
    let field = if [left_entries, right_entries]
        .iter()
        .any(|field| field.metadata().contains_key(crate::NR_LOGICAL_TYPE_KEY))
    {
        field_marker(field, "invalid", creation)?
    } else {
        field
    };
    creation
        .exact(|| crate::owned_resources::layout::arc_layout(std::alloc::Layout::new::<Field>()))?;
    Ok(DataType::Map(Arc::new(field), false))
}

/// Exact successful scalar NOT domain inherited from the original SQL kernel.
/// Physical carrier membership never establishes a nominal logical domain.
pub fn supports_boolean_negation_value_type(value: &crate::FunctionValueType) -> bool {
    value.logical_type() == crate::ValueLogicalType::Physical
        && matches!(
            value.data_type(),
            DataType::Boolean
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Float32
                | DataType::Float64
                | DataType::Decimal128(..)
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::TimeUnit;

    fn list(name: &str, ty: DataType, nullable: bool) -> DataType {
        DataType::List(Arc::new(Field::new(name, ty, nullable)))
    }

    fn map(key: DataType, value: DataType, key_nullable: bool, value_nullable: bool) -> DataType {
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Field::new("key", key, key_nullable),
                        Field::new("value", value, value_nullable),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        )
    }

    #[test]
    fn decimal_precision_scale_promotion_and_existing_clamp() {
        for (left, right, expected) in [
            (
                DataType::Decimal128(10, 2),
                DataType::Decimal128(18, 4),
                DataType::Decimal128(18, 4),
            ),
            (
                DataType::Decimal128(38, 0),
                DataType::Decimal128(38, 20),
                DataType::Decimal256(58, 20),
            ),
            (
                DataType::Decimal256(76, 0),
                DataType::Decimal128(38, 38),
                DataType::Decimal256(76, 38),
            ),
            (
                DataType::Decimal128(10, -2),
                DataType::Decimal128(12, 3),
                DataType::Decimal128(15, 3),
            ),
            (
                DataType::Decimal256(39, 9),
                DataType::Int64,
                DataType::Decimal256(39, 9),
            ),
            (
                DataType::Decimal128(18, 6),
                DataType::Float32,
                DataType::Float64,
            ),
            (DataType::Decimal128(18, 6), DataType::Utf8, DataType::Utf8),
        ] {
            assert_eq!(wider_type(&left, &right), expected);
            assert_eq!(wider_type(&right, &left), expected);
        }
    }

    #[test]
    fn list_recursion_keeps_left_name_and_joins_nullability() {
        let left = list("left_item", DataType::Int32, false);
        let right = list("right_item", DataType::Int64, true);
        assert_eq!(
            wider_type(&left, &right),
            list("left_item", DataType::Int64, true)
        );
        assert_eq!(
            wider_type(&right, &left),
            list("right_item", DataType::Int64, true)
        );
    }

    #[test]
    fn struct_names_align_reordered_fields_before_type_merge() {
        let left = DataType::Struct(
            vec![
                Field::new("number", DataType::Int32, false),
                Field::new("text", DataType::Utf8, false),
            ]
            .into(),
        );
        let right = DataType::Struct(
            vec![
                Field::new("text", DataType::Utf8, true),
                Field::new("number", DataType::Int64, false),
            ]
            .into(),
        );
        let expected = DataType::Struct(
            vec![
                Field::new("number", DataType::Int64, false),
                Field::new("text", DataType::Utf8, true),
            ]
            .into(),
        );
        assert_eq!(wider_type(&left, &right), expected);
    }

    #[test]
    fn struct_unmatched_names_keep_existing_positional_fallback() {
        let left = DataType::Struct(vec![Field::new("left", DataType::Int32, false)].into());
        let right = DataType::Struct(vec![Field::new("right", DataType::Int64, true)].into());
        assert_eq!(
            wider_type(&left, &right),
            DataType::Struct(vec![Field::new("left", DataType::Int64, true)].into())
        );
    }

    #[test]
    fn map_merges_both_channels_and_their_nullability() {
        let left = map(DataType::Int32, DataType::Decimal128(10, 2), false, false);
        let right = map(DataType::Int64, DataType::Decimal128(18, 4), true, true);
        assert_eq!(
            wider_type(&left, &right),
            map(DataType::Int64, DataType::Decimal128(18, 4), true, true)
        );
    }

    #[test]
    fn malformed_map_keeps_existing_left_carrier_fallback() {
        let entries = Arc::new(Field::new("opaque", DataType::Int32, true));
        let left = DataType::Map(Arc::clone(&entries), true);
        let right = map(DataType::Int64, DataType::Utf8, false, true);
        assert_eq!(wider_type(&left, &right), DataType::Map(entries, false));
    }

    #[test]
    fn null_largeint_and_timestamp_follow_original_carrier_rules() {
        let largeint = DataType::FixedSizeBinary(crate::LARGEINT_BYTE_WIDTH);
        assert_eq!(wider_type(&largeint, &DataType::Int64), largeint);
        let timestamp = DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()));
        assert_eq!(wider_type(&DataType::Date32, &timestamp), timestamp);
        let nested = list("item", DataType::Utf8, true);
        assert_eq!(wider_type(&DataType::Null, &nested), nested);
    }

    #[test]
    fn reconstruction_preserves_main_logical_domain_and_nullability() {
        let tagged = Arc::new(Field::new("item", DataType::Utf8, false).with_metadata(
            std::collections::HashMap::<String, String>::from([(
                crate::NR_LOGICAL_TYPE_KEY.to_string(),
                "json".to_string(),
            )]),
        ));
        let left = DataType::List(Arc::clone(&tagged));
        // Identical carriers retain the existing exact object shape.
        assert_eq!(wider_type(&left, &left), left);
        // MEM-1 M07 preserves a shared semantic domain across a Null carrier.
        // This is the upstream common-type contract, not a provider metadata loan.
        let right = list("item", DataType::Null, true);
        assert_eq!(
            wider_type(&left, &right),
            DataType::List(Arc::new(
                Field::new("item", DataType::Utf8, true).with_metadata(
                    std::collections::HashMap::<String, String>::from([(
                        crate::NR_LOGICAL_TYPE_KEY.to_string(),
                        "json".to_string()
                    )])
                )
            ))
        );
    }
}
