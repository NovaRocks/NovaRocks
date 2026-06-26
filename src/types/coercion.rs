use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields};

use crate::types::predicate::{is_integer, is_largeint};

/// Determine the wider type for unifying two types (comparisons, CASE, UNION, etc.).
pub(crate) fn wider_type(a: &DataType, b: &DataType) -> DataType {
    if a == b {
        return a.clone();
    }
    match (a, b) {
        (DataType::Null, other) | (other, DataType::Null) => other.clone(),
        (l, r) if (is_largeint(l) && is_integer(r)) || (is_integer(l) && is_largeint(r)) => {
            DataType::FixedSizeBinary(crate::common::largeint::LARGEINT_BYTE_WIDTH)
        }
        (DataType::List(left_field), DataType::List(right_field)) => {
            DataType::List(Arc::new(Field::new(
                left_field.name(),
                wider_type(left_field.data_type(), right_field.data_type()),
                left_field.is_nullable() || right_field.is_nullable(),
            )))
        }
        (DataType::Map(left_entries, _), DataType::Map(right_entries, _)) => {
            wider_map_type(left_entries, right_entries)
        }
        (DataType::Struct(left_fields), DataType::Struct(right_fields))
            if left_fields.len() == right_fields.len() =>
        {
            if let Some(fields) = wider_struct_fields_by_name(left_fields, right_fields) {
                return DataType::Struct(fields);
            }
            DataType::Struct(Fields::from(
                left_fields
                    .iter()
                    .zip(right_fields.iter())
                    .map(|(left_field, right_field)| {
                        Arc::new(Field::new(
                            left_field.name(),
                            wider_type(left_field.data_type(), right_field.data_type()),
                            left_field.is_nullable() || right_field.is_nullable(),
                        ))
                    })
                    .collect::<Vec<_>>(),
            ))
        }
        // VARCHAR wins before DECIMAL, matching StarRocks TypeManager:
        // getAssignmentCompatibleType handles string pairs before decimal
        // pairs, and ARRAY/MAP/STRUCT common types recurse through this rule.
        (DataType::Utf8, _) | (_, DataType::Utf8) => DataType::Utf8,
        (DataType::LargeUtf8, _) | (_, DataType::LargeUtf8) => DataType::Utf8,
        // Decimal + Decimal -> wider Decimal
        (DataType::Decimal128(p1, s1), DataType::Decimal128(p2, s2)) => {
            let scale = (*s1).max(*s2);
            let precision = ((*p1 as i8 - *s1).max(*p2 as i8 - *s2) + scale).min(38) as u8;
            DataType::Decimal128(precision, scale)
        }
        // Decimal + Integer -> Decimal
        (
            DataType::Decimal128(_, _),
            DataType::Int64 | DataType::Int32 | DataType::Int16 | DataType::Int8,
        )
        | (
            DataType::Int64 | DataType::Int32 | DataType::Int16 | DataType::Int8,
            DataType::Decimal128(_, _),
        ) => {
            let (p, s) = match (a, b) {
                (DataType::Decimal128(p, s), _) | (_, DataType::Decimal128(p, s)) => (*p, *s),
                _ => unreachable!(),
            };
            DataType::Decimal128(p, s)
        }
        // Decimal + Float -> Float64 (StarRocks FE: promote to Double)
        (DataType::Decimal128(_, _), DataType::Float64 | DataType::Float32)
        | (DataType::Float64 | DataType::Float32, DataType::Decimal128(_, _)) => DataType::Float64,
        // Decimal + other -> Decimal
        (DataType::Decimal128(_, _), _) | (_, DataType::Decimal128(_, _)) => {
            let (p, s) = match (a, b) {
                (DataType::Decimal128(p, s), _) | (_, DataType::Decimal128(p, s)) => (*p, *s),
                _ => unreachable!(),
            };
            DataType::Decimal128(p, s)
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
        _ => a.clone(),
    }
}

fn wider_struct_fields_by_name(left_fields: &Fields, right_fields: &Fields) -> Option<Fields> {
    let right_by_name = right_fields
        .iter()
        .map(|field| (field.name().as_str(), field))
        .collect::<std::collections::HashMap<_, _>>();
    if left_fields
        .iter()
        .any(|field| !right_by_name.contains_key(field.name().as_str()))
    {
        return None;
    }
    Some(Fields::from(
        left_fields
            .iter()
            .map(|left_field| {
                let right_field = right_by_name.get(left_field.name().as_str())?;
                Some(Arc::new(Field::new(
                    left_field.name(),
                    wider_type(left_field.data_type(), right_field.data_type()),
                    left_field.is_nullable() || right_field.is_nullable(),
                )))
            })
            .collect::<Option<Vec<_>>>()?,
    ))
}

fn wider_map_type(left_entries: &Field, right_entries: &Field) -> DataType {
    let DataType::Struct(left_fields) = left_entries.data_type() else {
        return DataType::Map(Arc::new(left_entries.clone()), false);
    };
    let DataType::Struct(right_fields) = right_entries.data_type() else {
        return DataType::Map(Arc::new(left_entries.clone()), false);
    };
    if left_fields.len() != 2 || right_fields.len() != 2 {
        return DataType::Map(Arc::new(left_entries.clone()), false);
    }

    let key_type = wider_type(left_fields[0].data_type(), right_fields[0].data_type());
    let value_type = wider_type(left_fields[1].data_type(), right_fields[1].data_type());
    DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Arc::new(Field::new(
                        "key",
                        key_type,
                        left_fields[0].is_nullable() || right_fields[0].is_nullable(),
                    )),
                    Arc::new(Field::new(
                        "value",
                        value_type,
                        left_fields[1].is_nullable() || right_fields[1].is_nullable(),
                    )),
                ]
                .into(),
            ),
            false,
        )),
        false,
    )
}

/// Comparison operand common type for `col op col`, aligned with the execution
/// backstop `exec::expr::comparison::normalize_comparison_types`. Returns the
/// type BOTH operands should be cast to, or `None` when no cast is needed
/// (equal types) or this layer does not coerce the pair (non-numeric: string /
/// date-ts / cross-family / incompatible — left to literal coercion or the
/// execution-time normalizer). Numeric/decimal only — kept in lock-step with
/// `normalize_comparison_types`' numeric/decimal arms so the materialized cast
/// type equals the execution-time comparison type.
pub(crate) fn comparison_common_type(left: &DataType, right: &DataType) -> Option<DataType> {
    if left == right {
        return None;
    }
    let is_int = |dt: &DataType| {
        matches!(
            dt,
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
        )
    };
    let is_float = |dt: &DataType| matches!(dt, DataType::Float32 | DataType::Float64);

    if is_int(left) && is_int(right) {
        return Some(DataType::Int64);
    }
    if (is_int(left) && is_float(right)) || (is_float(left) && is_int(right)) {
        return Some(DataType::Float64);
    }
    if is_float(left) && is_float(right) {
        return Some(DataType::Float64);
    }
    if let (DataType::Decimal128(lp, ls), DataType::Decimal128(rp, rs)) = (left, right) {
        let target_scale: i8 = (*ls).max(*rs);
        let lhs_int_digits: i16 = (*lp as i16) - (*ls as i16);
        let rhs_int_digits: i16 = (*rp as i16) - (*rs as i16);
        let int_digits: i16 = lhs_int_digits.max(rhs_int_digits).max(0);
        let target_precision: i16 = int_digits + (target_scale as i16);
        // Out-of-range precision is left to the execution normalizer to error
        // on (this layer only opportunistically pre-casts the safe cases).
        if target_precision <= 0 || target_precision > 38 {
            return None;
        }
        let target = DataType::Decimal128(target_precision as u8, target_scale);
        if &target == left && &target == right {
            return None;
        }
        return Some(target);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;

    #[test]
    fn comparison_common_type_numeric_and_decimal() {
        // equal -> None (no cast needed)
        assert_eq!(
            comparison_common_type(&DataType::Int32, &DataType::Int32),
            None
        );
        // int width mismatch -> both Int64 (aligned with normalize_comparison_types,
        // NOT wider_type's Int16/Int8 behavior)
        assert_eq!(
            comparison_common_type(&DataType::Int32, &DataType::Int64),
            Some(DataType::Int64)
        );
        assert_eq!(
            comparison_common_type(&DataType::Int16, &DataType::Int8),
            Some(DataType::Int64)
        );
        // int <-> float / float x float -> both Float64
        assert_eq!(
            comparison_common_type(&DataType::Int32, &DataType::Float64),
            Some(DataType::Float64)
        );
        assert_eq!(
            comparison_common_type(&DataType::Float32, &DataType::Float64),
            Some(DataType::Float64)
        );
        // decimal x decimal -> common decimal: scale=max(2,4)=4,
        // int_digits=max(10-2,18-4)=14, precision=14+4=18
        assert_eq!(
            comparison_common_type(&DataType::Decimal128(10, 2), &DataType::Decimal128(18, 4)),
            Some(DataType::Decimal128(18, 4))
        );
        assert_eq!(
            comparison_common_type(&DataType::Decimal128(10, 2), &DataType::Decimal128(10, 2)),
            None
        );
        // non-numeric (string / cross-family) -> None (out of scope here)
        assert_eq!(
            comparison_common_type(&DataType::Utf8, &DataType::Int32),
            None
        );
        assert_eq!(
            comparison_common_type(&DataType::Utf8, &DataType::Utf8),
            None
        );
    }

    #[test]
    fn wider_type_decimal_vs_float64_returns_float64() {
        let result = wider_type(&DataType::Decimal128(7, 2), &DataType::Float64);
        assert_eq!(result, DataType::Float64);
    }

    #[test]
    fn wider_type_float32_vs_decimal_returns_float64() {
        let result = wider_type(&DataType::Float32, &DataType::Decimal128(18, 6));
        assert_eq!(result, DataType::Float64);
    }

    #[test]
    fn wider_type_string_vs_decimal_returns_string() {
        let result = wider_type(&DataType::Utf8, &DataType::Decimal128(26, 2));
        assert_eq!(result, DataType::Utf8);
    }

    #[test]
    fn wider_type_array_string_vs_decimal_returns_array_string() {
        let left = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
        let right = DataType::List(Arc::new(Field::new(
            "item",
            DataType::Decimal128(26, 2),
            true,
        )));

        let result = wider_type(&left, &right);
        let DataType::List(item) = result else {
            panic!("expected array type");
        };
        assert_eq!(item.data_type(), &DataType::Utf8);
    }

    #[test]
    fn wider_type_promotes_map_key_and_value_types() {
        let left = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Arc::new(Field::new("key", DataType::Null, true)),
                        Arc::new(Field::new("value", DataType::Null, true)),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );
        let right = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Arc::new(Field::new("key", DataType::Int64, true)),
                        Arc::new(Field::new("value", DataType::Int64, true)),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );

        let widened = wider_type(&left, &right);
        let DataType::Map(entries, _) = widened else {
            panic!("expected map type");
        };
        let DataType::Struct(fields) = entries.data_type() else {
            panic!("expected entries struct");
        };
        assert_eq!(fields[0].data_type(), &DataType::Int64);
        assert_eq!(fields[1].data_type(), &DataType::Int64);
    }
}
