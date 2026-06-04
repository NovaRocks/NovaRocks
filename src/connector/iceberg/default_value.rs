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
    FormatVersion, Literal as IcebergLiteral, Map as IcebergMap, PrimitiveLiteral,
};

use crate::sql::parser::ast::{DefaultLiteral, Literal as AstLiteral, SqlType};

/// Convert an AST `DefaultLiteral` to an `iceberg::spec::Literal` validated
/// against the column's SqlType.  Returns `Ok(None)` for `DefaultLiteral::Null`
/// (which is not persisted) and `Err` when the literal does not fit the
/// column's type or the type itself is unsupported.
pub(crate) fn default_literal_to_iceberg(
    literal: &DefaultLiteral,
    column_type: &SqlType,
) -> Result<Option<IcebergLiteral>, String> {
    if matches!(literal, DefaultLiteral::Null) {
        return Ok(None);
    }

    // Array and Map defaults are stored as JSON strings in DefaultLiteral::String
    // (produced by parse_string_default).  Handle them before the primitive match.
    match (literal, column_type) {
        (DefaultLiteral::String(s), SqlType::Array(_)) => {
            // parse_string_default already validated the value is a JSON array.
            let v: serde_json::Value =
                serde_json::from_str(s).map_err(|e| format!("invalid ARRAY DEFAULT JSON: {e}"))?;
            let arr = v
                .as_array()
                .ok_or_else(|| format!("ARRAY DEFAULT must be a JSON array, got: {s:?}"))?;
            if !arr.is_empty() {
                return Err(
                    "non-empty ARRAY DEFAULT literals are not yet supported; use '[]'".to_string(),
                );
            }
            return Ok(Some(IcebergLiteral::List(vec![])));
        }
        (DefaultLiteral::String(s), SqlType::Map(_, _)) => {
            // parse_string_default already validated the value is a JSON object.
            let v: serde_json::Value =
                serde_json::from_str(s).map_err(|e| format!("invalid MAP DEFAULT JSON: {e}"))?;
            let obj = v
                .as_object()
                .ok_or_else(|| format!("MAP DEFAULT must be a JSON object, got: {s:?}"))?;
            if !obj.is_empty() {
                return Err(
                    "non-empty MAP DEFAULT literals are not yet supported; use '{}'".to_string(),
                );
            }
            return Ok(Some(IcebergLiteral::Map(IcebergMap::new())));
        }
        _ => {}
    }

    let prim = match (literal, column_type) {
        (DefaultLiteral::Bool(b), SqlType::Boolean) => PrimitiveLiteral::Boolean(*b),
        (DefaultLiteral::Int(v), SqlType::TinyInt) => {
            i8::try_from(*v).map_err(|_| out_of_range("TINYINT", *v))?;
            PrimitiveLiteral::Int(*v as i32)
        }
        (DefaultLiteral::Int(v), SqlType::SmallInt) => {
            i16::try_from(*v).map_err(|_| out_of_range("SMALLINT", *v))?;
            PrimitiveLiteral::Int(*v as i32)
        }
        (DefaultLiteral::Int(v), SqlType::Int) => {
            i32::try_from(*v).map_err(|_| out_of_range("INT", *v))?;
            PrimitiveLiteral::Int(*v as i32)
        }
        (DefaultLiteral::Int(v), SqlType::BigInt) => PrimitiveLiteral::Long(*v),
        (DefaultLiteral::Float(v), SqlType::Float) => {
            PrimitiveLiteral::Float(ordered_float::OrderedFloat(*v as f32))
        }
        (DefaultLiteral::Float(v), SqlType::Double) => {
            PrimitiveLiteral::Double(ordered_float::OrderedFloat(*v))
        }
        (
            DefaultLiteral::Decimal { unscaled, scale },
            SqlType::Decimal {
                scale: col_scale, ..
            },
        ) => {
            if *scale != *col_scale {
                return Err(format!(
                    "DEFAULT value scale {scale} does not match column scale {col_scale}"
                ));
            }
            PrimitiveLiteral::Int128(*unscaled)
        }
        (DefaultLiteral::String(s), SqlType::String | SqlType::Json) => {
            PrimitiveLiteral::String(s.clone())
        }
        (DefaultLiteral::Date(d), SqlType::Date) => PrimitiveLiteral::Int(*d),
        (DefaultLiteral::DateTime(t), SqlType::DateTime) => PrimitiveLiteral::Long(*t),
        (DefaultLiteral::DateTime(t), SqlType::DateTimeNs) => PrimitiveLiteral::Long(*t),
        (DefaultLiteral::Binary(b), SqlType::Binary | SqlType::Bitmap | SqlType::Hll) => {
            PrimitiveLiteral::Binary(b.clone())
        }
        (lit, ty) => {
            return Err(format!(
                "DEFAULT value type does not match column type: literal={lit:?} column={ty:?}"
            ));
        }
    };
    Ok(Some(IcebergLiteral::Primitive(prim)))
}

fn out_of_range(type_name: &str, value: i64) -> String {
    format!("DEFAULT value {value} is out of range for {type_name}")
}

/// Format an unscaled `i128` with the given decimal `scale` as a human-readable
/// decimal string (e.g. `unscaled=999, scale=2` → `"9.99"`).
fn i128_unscaled_to_decimal_string(unscaled: i128, scale: u32) -> String {
    if scale == 0 {
        return unscaled.to_string();
    }
    let negative = unscaled < 0;
    let abs = unscaled.unsigned_abs();
    let factor = 10_u128.pow(scale);
    let int_part = abs / factor;
    let frac_part = abs % factor;
    // Zero-pad the fractional part to exactly `scale` digits.
    let s = format!("{int_part}.{frac_part:0>scale$}", scale = scale as usize);
    if negative { format!("-{s}") } else { s }
}

/// Convert an `iceberg::spec::Literal` back to an AST `Literal` for use in the
/// INSERT write path (filling omitted columns with their write_default).
///
/// Returns `Err` for types that are not yet supported by the INSERT path
/// (Binary/Bitmap/HLL).  Decimal and DateTime are fully supported and
/// return `AstLiteral::String` that the downstream `build_local_literal_array`
/// can parse.
pub(crate) fn iceberg_literal_to_ast(
    literal: &IcebergLiteral,
    column_type: &SqlType,
) -> Result<AstLiteral, String> {
    match (literal, column_type) {
        (IcebergLiteral::Primitive(PrimitiveLiteral::Boolean(b)), SqlType::Boolean) => {
            Ok(AstLiteral::Bool(*b))
        }
        (
            IcebergLiteral::Primitive(PrimitiveLiteral::Int(v)),
            SqlType::TinyInt | SqlType::SmallInt | SqlType::Int,
        ) => Ok(AstLiteral::Int(*v as i64)),
        (IcebergLiteral::Primitive(PrimitiveLiteral::Long(v)), SqlType::BigInt) => {
            Ok(AstLiteral::Int(*v))
        }
        (IcebergLiteral::Primitive(PrimitiveLiteral::Float(v)), SqlType::Float) => {
            Ok(AstLiteral::Float(v.0 as f64))
        }
        (IcebergLiteral::Primitive(PrimitiveLiteral::Double(v)), SqlType::Double) => {
            Ok(AstLiteral::Float(v.0))
        }
        (
            IcebergLiteral::Primitive(PrimitiveLiteral::String(s)),
            SqlType::String | SqlType::Json,
        ) => Ok(AstLiteral::String(s.clone())),
        (IcebergLiteral::Primitive(PrimitiveLiteral::Int(days)), SqlType::Date) => {
            // Convert days-since-epoch back to "YYYY-MM-DD" string.
            use chrono::NaiveDate;
            const UNIX_EPOCH_DAY_OFFSET: i32 = 719163;
            let date = NaiveDate::from_num_days_from_ce_opt(UNIX_EPOCH_DAY_OFFSET + days)
                .ok_or_else(|| {
                    format!("write-default date value {days} is out of representable range")
                })?;
            Ok(AstLiteral::Date(date.format("%Y-%m-%d").to_string()))
        }
        (
            IcebergLiteral::Primitive(PrimitiveLiteral::Int128(unscaled)),
            SqlType::Decimal { scale, .. },
        ) => {
            // Convert unscaled i128 + scale back to a decimal string like "9.99".
            // build_local_literal_array handles Literal::String for Decimal128 columns.
            let s = i128_unscaled_to_decimal_string(*unscaled, *scale as u32);
            Ok(AstLiteral::String(s))
        }
        (IcebergLiteral::Primitive(PrimitiveLiteral::Long(micros)), SqlType::DateTime) => {
            // Convert microseconds-since-epoch back to "YYYY-MM-DD HH:MM:SS" string.
            // build_local_literal_array handles Literal::String for Timestamp columns.
            use chrono::DateTime as ChronoDateTime;
            let dt = ChronoDateTime::from_timestamp_micros(*micros).ok_or_else(|| {
                format!("write-default datetime value {micros} µs is out of representable range")
            })?;
            Ok(AstLiteral::String(
                dt.naive_utc().format("%Y-%m-%d %H:%M:%S").to_string(),
            ))
        }
        (IcebergLiteral::Primitive(PrimitiveLiteral::Long(nanos)), SqlType::DateTimeNs) => {
            // Convert nanoseconds-since-epoch back to "YYYY-MM-DD HH:MM:SS.nnnnnnnnn" string.
            // build_local_literal_array handles Literal::String for Timestamp columns.
            use chrono::DateTime as ChronoDateTime;
            let dt = ChronoDateTime::from_timestamp_nanos(*nanos);
            Ok(AstLiteral::String(
                dt.naive_utc().format("%Y-%m-%d %H:%M:%S%.9f").to_string(),
            ))
        }
        (
            IcebergLiteral::Primitive(PrimitiveLiteral::Binary(b)),
            SqlType::Binary | SqlType::Bitmap | SqlType::Hll,
        ) => {
            // Represent the byte slice as a Latin-1 string so that
            // build_local_literal_array (DataType::Binary/LargeBinary) can
            // round-trip it via latin1_string_to_bytes.  Only bytes in 0..=255
            // are valid Latin-1; since they are all u8 this is always safe.
            let s: String = b.iter().map(|&byte| byte as char).collect();
            Ok(AstLiteral::String(s))
        }
        // Empty ARRAY/MAP defaults: produce the empty collection AST node.
        // Non-empty collection defaults are not yet supported.
        (IcebergLiteral::List(elems), SqlType::Array(_)) => {
            if !elems.is_empty() {
                return Err(format!(
                    "non-empty ARRAY write-default is not yet supported ({} elements)",
                    elems.len()
                ));
            }
            Ok(AstLiteral::Array(vec![]))
        }
        (IcebergLiteral::Map(map), SqlType::Map(_, _)) => {
            if !map.is_empty() {
                return Err(format!(
                    "non-empty MAP write-default is not yet supported ({} entries)",
                    map.len()
                ));
            }
            Ok(AstLiteral::Map(vec![]))
        }
        (lit, ty) => Err(format!(
            "write-default literal type does not match column type: literal={lit:?} column={ty:?}"
        )),
    }
}

/// Reject non-NULL defaults on tables whose format-version is not v3.
/// `None` is the no-default case and is always accepted.
pub(crate) fn require_v3_for_default(
    format_version: FormatVersion,
    default: &Option<IcebergLiteral>,
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

    #[test]
    fn bool_default_round_trips() {
        let lit = default_literal_to_iceberg(&DefaultLiteral::Bool(true), &SqlType::Boolean)
            .expect("bool default")
            .expect("not null");
        assert!(matches!(
            lit,
            IcebergLiteral::Primitive(PrimitiveLiteral::Boolean(true))
        ));
    }

    #[test]
    fn int_overflow_rejected_for_tinyint() {
        let err = default_literal_to_iceberg(&DefaultLiteral::Int(200), &SqlType::TinyInt)
            .expect_err("overflow");
        assert!(err.contains("TINYINT"));
    }

    #[test]
    fn decimal_scale_mismatch_rejected() {
        let err = default_literal_to_iceberg(
            &DefaultLiteral::Decimal {
                unscaled: 1234,
                scale: 3,
            },
            &SqlType::Decimal {
                precision: 10,
                scale: 2,
            },
        )
        .expect_err("scale mismatch");
        assert!(err.contains("scale"));
    }

    #[test]
    fn null_returns_none() {
        let lit =
            default_literal_to_iceberg(&DefaultLiteral::Null, &SqlType::Int).expect("null default");
        assert!(lit.is_none());
    }

    #[test]
    fn type_mismatch_rejected() {
        let err = default_literal_to_iceberg(&DefaultLiteral::String("x".into()), &SqlType::Int)
            .expect_err("type mismatch");
        assert!(err.contains("type does not match"));
    }

    #[test]
    fn iceberg_to_ast_literal_int() {
        use crate::sql::parser::ast::Literal as AstLiteral;
        let iceberg = IcebergLiteral::Primitive(PrimitiveLiteral::Int(7));
        let ast = iceberg_literal_to_ast(&iceberg, &SqlType::Int).expect("convert");
        assert_eq!(ast, AstLiteral::Int(7));
    }

    #[test]
    fn iceberg_to_ast_literal_string() {
        use crate::sql::parser::ast::Literal as AstLiteral;
        let iceberg = IcebergLiteral::Primitive(PrimitiveLiteral::String("hi".into()));
        let ast = iceberg_literal_to_ast(&iceberg, &SqlType::String).expect("convert");
        assert_eq!(ast, AstLiteral::String("hi".into()));
    }

    #[test]
    fn iceberg_to_ast_literal_unsupported_type_errors() {
        // Binary write-default is now supported via Latin-1 byte-to-char encoding.
        // This test verifies that a Binary literal round-trips through iceberg_literal_to_ast.
        let iceberg = IcebergLiteral::Primitive(PrimitiveLiteral::Binary(b"abc".to_vec()));
        let ast = iceberg_literal_to_ast(&iceberg, &SqlType::Binary)
            .expect("binary write-default should now succeed");
        assert_eq!(ast, AstLiteral::String("abc".to_string()));
    }

    #[test]
    fn write_default_decimal_fills_correct_value() {
        // 9.99 → unscaled 999 at scale 2
        let iceberg = IcebergLiteral::Primitive(PrimitiveLiteral::Int128(999));
        let ast = iceberg_literal_to_ast(
            &iceberg,
            &SqlType::Decimal {
                precision: 10,
                scale: 2,
            },
        )
        .expect("decimal write-default");
        assert_eq!(ast, AstLiteral::String("9.99".to_string()));

        // Negative: -0.01 → unscaled -1 at scale 2
        let neg = IcebergLiteral::Primitive(PrimitiveLiteral::Int128(-1));
        let ast_neg = iceberg_literal_to_ast(
            &neg,
            &SqlType::Decimal {
                precision: 10,
                scale: 2,
            },
        )
        .expect("negative decimal");
        assert_eq!(ast_neg, AstLiteral::String("-0.01".to_string()));

        // Zero scale: integer decimal
        let int_dec = IcebergLiteral::Primitive(PrimitiveLiteral::Int128(42));
        let ast_int = iceberg_literal_to_ast(
            &int_dec,
            &SqlType::Decimal {
                precision: 5,
                scale: 0,
            },
        )
        .expect("zero-scale decimal");
        assert_eq!(ast_int, AstLiteral::String("42".to_string()));
    }

    #[test]
    fn write_default_date_fills_correct_value() {
        // Days since Unix epoch: 2024-01-01 = 19723 days after 1970-01-01
        // Verify by computing: days from 1970-01-01 to 2024-01-01
        // 2024 - 1970 = 54 years, accounting for leap years: 19723
        let days_2024_01_01: i32 = 19723;
        let iceberg = IcebergLiteral::Primitive(PrimitiveLiteral::Int(days_2024_01_01));
        let ast = iceberg_literal_to_ast(&iceberg, &SqlType::Date).expect("date write-default");
        assert_eq!(ast, AstLiteral::Date("2024-01-01".to_string()));

        // Epoch itself
        let epoch = IcebergLiteral::Primitive(PrimitiveLiteral::Int(0));
        let ast_epoch = iceberg_literal_to_ast(&epoch, &SqlType::Date).expect("epoch");
        assert_eq!(ast_epoch, AstLiteral::Date("1970-01-01".to_string()));
    }

    #[test]
    fn write_default_datetime_fills_correct_value() {
        // 2024-01-01 12:00:00 UTC in microseconds since epoch
        // = 19723 days * 86400 s/day + 12*3600 s = 1704110400 s = 1704110400_000000 µs
        let micros_2024_01_01_noon: i64 = 1_704_110_400_000_000;
        let iceberg = IcebergLiteral::Primitive(PrimitiveLiteral::Long(micros_2024_01_01_noon));
        let ast =
            iceberg_literal_to_ast(&iceberg, &SqlType::DateTime).expect("datetime write-default");
        assert_eq!(ast, AstLiteral::String("2024-01-01 12:00:00".to_string()));

        // Epoch
        let epoch = IcebergLiteral::Primitive(PrimitiveLiteral::Long(0));
        let ast_epoch = iceberg_literal_to_ast(&epoch, &SqlType::DateTime).expect("epoch datetime");
        assert_eq!(
            ast_epoch,
            AstLiteral::String("1970-01-01 00:00:00".to_string())
        );
    }

    #[test]
    fn iceberg_to_ast_literal_date_round_trips() {
        let epoch = IcebergLiteral::Primitive(PrimitiveLiteral::Int(0));
        let ast = iceberg_literal_to_ast(&epoch, &SqlType::Date).expect("epoch");
        assert_eq!(ast, AstLiteral::Date("1970-01-01".to_string()));

        let day_before = IcebergLiteral::Primitive(PrimitiveLiteral::Int(-1));
        let ast = iceberg_literal_to_ast(&day_before, &SqlType::Date).expect("pre-epoch");
        assert_eq!(ast, AstLiteral::Date("1969-12-31".to_string()));
    }

    #[test]
    fn iceberg_to_ast_literal_struct_against_decimal_reports_type_mismatch() {
        // Catch-all must surface "type does not match" rather than the
        // not-yet-supported branch when the literal is structurally wrong
        // for the column.
        let iceberg = IcebergLiteral::Primitive(PrimitiveLiteral::String("oops".into()));
        let err = iceberg_literal_to_ast(
            &iceberg,
            &SqlType::Decimal {
                precision: 10,
                scale: 2,
            },
        )
        .expect_err("type mismatch");
        assert!(err.contains("does not match"));
    }

    #[test]
    fn v2_rejects_non_null_default() {
        let err = require_v3_for_default(
            iceberg::spec::FormatVersion::V2,
            &Some(IcebergLiteral::Primitive(PrimitiveLiteral::Int(5))),
        )
        .expect_err("v2 reject");
        assert!(err.contains("format-version 3"));
    }

    #[test]
    fn v3_accepts_non_null_default() {
        require_v3_for_default(
            iceberg::spec::FormatVersion::V3,
            &Some(IcebergLiteral::Primitive(PrimitiveLiteral::Int(5))),
        )
        .expect("v3 accept");
    }

    #[test]
    fn v2_accepts_null_default() {
        require_v3_for_default(iceberg::spec::FormatVersion::V2, &None).expect("v2 + null ok");
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

    // --- Binary default ---

    #[test]
    fn binary_default_from_string_literal() {
        // DefaultLiteral::Binary constructed from raw bytes (as parse_string_default would do)
        // should convert to an Iceberg PrimitiveLiteral::Binary.
        let bytes = b"abc".to_vec();
        let lit =
            default_literal_to_iceberg(&DefaultLiteral::Binary(bytes.clone()), &SqlType::Binary)
                .expect("binary default")
                .expect("not null");
        assert!(
            matches!(lit, IcebergLiteral::Primitive(PrimitiveLiteral::Binary(ref b)) if *b == bytes),
            "unexpected literal: {lit:?}"
        );
    }

    #[test]
    fn binary_default_empty_string() {
        let lit = default_literal_to_iceberg(&DefaultLiteral::Binary(vec![]), &SqlType::Binary)
            .expect("empty binary default")
            .expect("not null");
        assert!(
            matches!(lit, IcebergLiteral::Primitive(PrimitiveLiteral::Binary(ref b)) if b.is_empty()),
            "expected empty binary literal: {lit:?}"
        );
    }

    // --- Array empty default ---

    #[test]
    fn array_empty_default_from_string_literal() {
        // parse_string_default stores '[]' as DefaultLiteral::String("[]") for Array types.
        // default_literal_to_iceberg must convert it to IcebergLiteral::List(vec![]).
        let lit = default_literal_to_iceberg(
            &DefaultLiteral::String("[]".to_string()),
            &SqlType::Array(Box::new(SqlType::Int)),
        )
        .expect("array empty default")
        .expect("not null");
        assert!(
            matches!(lit, IcebergLiteral::List(ref v) if v.is_empty()),
            "expected empty List literal: {lit:?}"
        );
    }

    #[test]
    fn array_non_empty_literal_rejected() {
        let err = default_literal_to_iceberg(
            &DefaultLiteral::String("[1,2,3]".to_string()),
            &SqlType::Array(Box::new(SqlType::Int)),
        )
        .expect_err("non-empty array should be rejected");
        assert!(err.contains("non-empty ARRAY"), "unexpected error: {err}");
    }

    // --- Map empty default ---

    #[test]
    fn map_empty_default_from_string_literal() {
        // parse_string_default stores '{}' as DefaultLiteral::String("{}") for Map types.
        // default_literal_to_iceberg must convert it to IcebergLiteral::Map(empty).
        let lit = default_literal_to_iceberg(
            &DefaultLiteral::String("{}".to_string()),
            &SqlType::Map(Box::new(SqlType::String), Box::new(SqlType::Int)),
        )
        .expect("map empty default")
        .expect("not null");
        assert!(
            matches!(lit, IcebergLiteral::Map(ref m) if m.is_empty()),
            "expected empty Map literal: {lit:?}"
        );
    }

    #[test]
    fn map_non_empty_literal_rejected() {
        let err = default_literal_to_iceberg(
            &DefaultLiteral::String(r#"{"k":1}"#.to_string()),
            &SqlType::Map(Box::new(SqlType::String), Box::new(SqlType::Int)),
        )
        .expect_err("non-empty map should be rejected");
        assert!(err.contains("non-empty MAP"), "unexpected error: {err}");
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
