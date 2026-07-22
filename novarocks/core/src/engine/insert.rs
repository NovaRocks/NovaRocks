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

//! Row-reorder + Arrow-batch builder helpers shared across INSERT backends.
//!
//! `reorder_insert_rows` maps user-supplied `Literal` rows onto the target
//! table's column order, filling missing columns with NULL. `build_local_insert_batch`
//! then materialises the reordered rows into an Arrow `RecordBatch` with the
//! types the StarRocks table write path expects.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;

use crate::formats::parquet::local_io::normalize_map_entries_nullability;
use crate::sql::literal::{
    latin1_string_to_bytes, literal_to_i128_for_integer, parse_date_string_to_days,
    parse_datetime_string_to_micros, parse_datetime_string_to_nanos,
};
use crate::sql::parser::ast::Literal;
use novarocks_catalog::identifier::normalize_identifier;
use novarocks_catalog::schema::ColumnDef;

pub(crate) fn reorder_insert_rows(
    rows: &[Vec<Literal>],
    insert_columns: &[String],
    target_columns: &[ColumnDef],
) -> Result<Vec<Vec<Literal>>, String> {
    if insert_columns.is_empty() {
        return Ok(rows.to_vec());
    }
    for row in rows {
        if row.len() != insert_columns.len() {
            return Err(format!(
                "insert column count mismatch: expected {} values for column list, got {}",
                insert_columns.len(),
                row.len()
            ));
        }
    }
    let mapping = build_insert_column_mapping(insert_columns, target_columns)?;
    rows.iter()
        .map(|row| reorder_insert_row(row, &mapping, target_columns))
        .collect()
}

fn build_insert_column_mapping(
    insert_columns: &[String],
    target_columns: &[ColumnDef],
) -> Result<Vec<(usize, Option<usize>)>, String> {
    let mut insert_index_by_name = HashMap::with_capacity(insert_columns.len());
    for (idx, column) in insert_columns.iter().enumerate() {
        let key = normalize_identifier(column)?;
        if insert_index_by_name.insert(key, idx).is_some() {
            return Err(format!("duplicate INSERT column `{column}`"));
        }
    }

    let mut mapping = Vec::with_capacity(target_columns.len());
    for (target_idx, column) in target_columns.iter().enumerate() {
        let key = normalize_identifier(&column.name)?;
        mapping.push((target_idx, insert_index_by_name.remove(&key)));
    }
    if let Some((name, _)) = insert_index_by_name.into_iter().next() {
        return Err(format!("unknown INSERT column `{name}`"));
    }
    Ok(mapping)
}

fn reorder_insert_row(
    row: &[Literal],
    mapping: &[(usize, Option<usize>)],
    target_columns: &[ColumnDef],
) -> Result<Vec<Literal>, String> {
    if row.len() > target_columns.len() {
        return Err(format!(
            "insert column count mismatch: expected at most {} values, got {}",
            target_columns.len(),
            row.len()
        ));
    }
    let mut reordered = Vec::with_capacity(target_columns.len());
    for (target_idx, source_idx) in mapping {
        match source_idx {
            Some(idx) => {
                let value = row.get(*idx).cloned().ok_or_else(|| {
                    format!("insert value for column position {} is missing", idx + 1)
                })?;
                reordered.push(value);
            }
            None => {
                let column = &target_columns[*target_idx];
                let literal = match &column.write_default {
                    Some(write_default) => {
                        let sql_type =
                            arrow_data_type_to_sql_type(&column.data_type).map_err(|e| {
                                format!("INSERT write-default for `{}`: {e}", column.name)
                            })?;
                        crate::sql::literal::column_default_to_ast_literal(
                            write_default,
                            &sql_type,
                        )?
                    }
                    None => Literal::Null,
                };
                reordered.push(literal);
            }
        }
    }
    Ok(reordered)
}

fn arrow_data_type_to_sql_type(
    dt: &arrow::datatypes::DataType,
) -> Result<novarocks_catalog::schema::SqlType, String> {
    use arrow::datatypes::{DataType, TimeUnit};
    use novarocks_catalog::schema::SqlType;
    Ok(match dt {
        DataType::Boolean => SqlType::Boolean,
        DataType::Int8 => SqlType::TinyInt,
        DataType::Int16 => SqlType::SmallInt,
        DataType::Int32 => SqlType::Int,
        DataType::Int64 => SqlType::BigInt,
        DataType::Float32 => SqlType::Float,
        DataType::Float64 => SqlType::Double,
        DataType::Decimal128(precision, scale) => SqlType::Decimal {
            precision: *precision,
            scale: *scale,
        },
        DataType::Utf8 => SqlType::String,
        DataType::Date32 => SqlType::Date,
        DataType::Timestamp(TimeUnit::Nanosecond, _) => SqlType::DateTimeNs,
        DataType::Timestamp(TimeUnit::Microsecond, _) => SqlType::DateTime,
        DataType::Binary | DataType::LargeBinary => SqlType::Binary,
        DataType::List(element_field) => {
            let inner = arrow_data_type_to_sql_type(element_field.data_type())?;
            SqlType::Array(Box::new(inner))
        }
        DataType::Map(entries_field, _) => {
            // entries_field is Struct(key: KeyType, value: ValueType)
            if let DataType::Struct(fields) = entries_field.data_type() {
                let key_type = arrow_data_type_to_sql_type(fields[0].data_type())?;
                let value_type = arrow_data_type_to_sql_type(fields[1].data_type())?;
                SqlType::Map(Box::new(key_type), Box::new(value_type))
            } else {
                return Err(format!(
                    "MAP Arrow type has unexpected entries field type: {:?}",
                    entries_field.data_type()
                ));
            }
        }
        other => {
            return Err(format!(
                "unsupported Arrow type for write-default conversion: {other:?}"
            ));
        }
    })
}

/// Build a RecordBatch from literal value rows for a local table.
pub(crate) fn build_local_insert_batch(
    columns: &[ColumnDef],
    rows: &[Vec<Literal>],
) -> Result<RecordBatch, String> {
    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .map(|c| {
                Field::new(
                    &c.name,
                    normalize_map_entries_nullability(&c.data_type),
                    c.nullable,
                )
            })
            .collect::<Vec<_>>(),
    ));

    for row in rows {
        if row.len() != columns.len() {
            return Err(format!(
                "insert column count mismatch: expected {} values, got {}",
                columns.len(),
                row.len()
            ));
        }
    }

    let mut arrays = Vec::with_capacity(columns.len());
    for (idx, column) in columns.iter().enumerate() {
        let values: Vec<&Literal> = rows.iter().map(|row| &row[idx]).collect();
        arrays.push(build_local_literal_array(
            &column.data_type,
            &values,
            column.nullable,
        )?);
    }

    RecordBatch::try_new(schema, arrays)
        .map_err(|e| format!("build local insert batch failed: {e}"))
}

/// Build an Arrow array from literal values for local table insertion.
///
/// `nullable` models the target column/field nullability and is used to mirror
/// StarRocks' assignment semantics: overflowing a narrow integer literal into
/// a nullable target produces NULL, while overflow on a NOT NULL target fails
/// fast with an error.
fn build_local_literal_array(
    data_type: &DataType,
    values: &[&Literal],
    nullable: bool,
) -> Result<ArrayRef, String> {
    use arrow::array::*;
    use arrow_buffer::{NullBufferBuilder, OffsetBuffer};

    let data_type = normalize_map_entries_nullability(data_type);
    match &data_type {
        DataType::Int8 => Ok(Arc::new(Int8Array::from(
            values
                .iter()
                .map(|literal| match literal {
                    Literal::Null => Ok(None),
                    _ => {
                        literal_to_i128_for_integer(literal, "TINYINT")?.map_or(Ok(None), |value| {
                            match i8::try_from(value) {
                                Ok(v) => Ok(Some(v)),
                                Err(_) if nullable => Ok(None),
                                Err(_) => {
                                    Err(format!("literal {value} is out of range for TINYINT"))
                                }
                            }
                        })
                    }
                })
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        DataType::Int16 => Ok(Arc::new(Int16Array::from(
            values
                .iter()
                .map(|literal| match literal {
                    Literal::Null => Ok(None),
                    _ => literal_to_i128_for_integer(literal, "SMALLINT")?.map_or(
                        Ok(None),
                        |value| match i16::try_from(value) {
                            Ok(v) => Ok(Some(v)),
                            Err(_) if nullable => Ok(None),
                            Err(_) => Err(format!("literal {value} is out of range for SMALLINT")),
                        },
                    ),
                })
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        DataType::Int32 => Ok(Arc::new(Int32Array::from(
            values
                .iter()
                .map(|literal| match literal {
                    Literal::Null => Ok(None),
                    _ => literal_to_i128_for_integer(literal, "INT")?.map_or(Ok(None), |value| {
                        match i32::try_from(value) {
                            Ok(v) => Ok(Some(v)),
                            Err(_) if nullable => Ok(None),
                            Err(_) => Err(format!("literal {value} is out of range for INT")),
                        }
                    }),
                })
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        DataType::Int64 => Ok(Arc::new(Int64Array::from(
            values
                .iter()
                .map(|literal| match literal {
                    Literal::Null => Ok(None),
                    _ => {
                        literal_to_i128_for_integer(literal, "BIGINT")?.map_or(Ok(None), |value| {
                            match i64::try_from(value) {
                                Ok(v) => Ok(Some(v)),
                                Err(_) if nullable => Ok(None),
                                Err(_) => {
                                    Err(format!("literal {value} is out of range for BIGINT"))
                                }
                            }
                        })
                    }
                })
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        DataType::FixedSizeBinary(width)
            if *width == novarocks_types::largeint::LARGEINT_BYTE_WIDTH =>
        {
            let parsed = values
                .iter()
                .map(|literal| literal_to_i128_for_integer(literal, "LARGEINT"))
                .collect::<Result<Vec<_>, _>>()?;
            novarocks_types::largeint::array_from_i128(&parsed)
        }
        DataType::Float32 => Ok(Arc::new(Float32Array::from(
            values
                .iter()
                .map(|literal| match literal {
                    Literal::Null => Ok(None),
                    Literal::Float(v) => Ok(Some(*v as f32)),
                    Literal::Int(v) => Ok(Some(*v as f32)),
                    other => Err(format!("literal {:?} is not valid for FLOAT", other)),
                })
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        DataType::Float64 => Ok(Arc::new(Float64Array::from(
            values
                .iter()
                .map(|literal| match literal {
                    Literal::Null => Ok(None),
                    Literal::Float(v) => Ok(Some(*v)),
                    Literal::Int(v) => Ok(Some(*v as f64)),
                    Literal::String(s) => s
                        .trim()
                        .parse::<f64>()
                        .map(Some)
                        .map_err(|_| format!("literal `{s}` is not valid for DOUBLE")),
                    other => Err(format!("literal {:?} is not valid for DOUBLE", other)),
                })
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        DataType::Decimal128(precision, scale) => {
            let parsed = values
                .iter()
                .map(|literal| match literal {
                    Literal::Null => Ok(None),
                    Literal::Int(v) => {
                        let factor = 10_i128
                            .checked_pow(*scale as u32)
                            .ok_or_else(|| format!("decimal scale {} is too large", scale))?;
                        Ok(Some(i128::from(*v) * factor))
                    }
                    Literal::Float(v) => {
                        let factor = 10_f64.powi(*scale as i32);
                        Ok(Some((v * factor).round() as i128))
                    }
                    Literal::String(s) => parse_decimal_string_to_i128(s, *scale).map(Some),
                    other => Err(format!("literal {:?} is not valid for DECIMAL", other)),
                })
                .collect::<Result<Vec<_>, _>>()?;
            let array = arrow::array::Decimal128Array::from(parsed)
                .with_precision_and_scale(*precision, *scale)
                .map_err(|e| format!("build DECIMAL array failed: {e}"))?;
            Ok(Arc::new(array))
        }
        DataType::Binary => {
            let mut builder = BinaryBuilder::new();
            for literal in values {
                match literal {
                    Literal::Null => builder.append_null(),
                    Literal::String(v) | Literal::Date(v) => {
                        builder.append_value(latin1_string_to_bytes(v)?)
                    }
                    Literal::Int(v) => builder.append_value(v.to_string().into_bytes()),
                    Literal::Float(v) => builder.append_value(v.to_string().into_bytes()),
                    Literal::Bool(v) => {
                        builder.append_value(if *v { b"1".as_slice() } else { b"0".as_slice() })
                    }
                    other => {
                        return Err(format!("literal {:?} is not valid for VARBINARY", other));
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::LargeBinary => {
            // VARIANT lowers to LargeBinary; INSERT VALUES carries the encoded
            // variant bytes packed into Literal::String via Latin-1 (see
            // `parse_json` / `to_binary` in `sql::literal`).
            let mut builder = LargeBinaryBuilder::new();
            for literal in values {
                match literal {
                    Literal::Null => builder.append_null(),
                    Literal::String(v) | Literal::Date(v) => {
                        builder.append_value(latin1_string_to_bytes(v)?)
                    }
                    other => {
                        return Err(format!(
                            "literal {:?} is not valid for VARIANT/LARGEBINARY",
                            other
                        ));
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Utf8 => Ok(Arc::new(StringArray::from(
            values
                .iter()
                .map(|literal| match literal {
                    Literal::Null => Ok(None),
                    Literal::String(v) | Literal::Date(v) => Ok(Some(v.clone())),
                    Literal::Int(v) => Ok(Some(v.to_string())),
                    Literal::Float(v) => Ok(Some(v.to_string())),
                    Literal::Bool(v) => {
                        Ok(Some(if *v { "1".to_string() } else { "0".to_string() }))
                    }
                    other => Err(format!("literal {:?} is not valid for STRING", other)),
                })
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        DataType::Boolean => Ok(Arc::new(BooleanArray::from(
            values
                .iter()
                .map(|literal| match literal {
                    Literal::Null => Ok(None),
                    Literal::Bool(v) => Ok(Some(*v)),
                    Literal::Int(v) => Ok(Some(*v != 0)),
                    other => Err(format!("literal {:?} is not valid for BOOLEAN", other)),
                })
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        DataType::Date32 => Ok(Arc::new(Date32Array::from(
            values
                .iter()
                .map(|literal| match literal {
                    Literal::Null => Ok(None),
                    Literal::Date(v) | Literal::String(v) => parse_date_string_to_days(v).map(Some),
                    // Non-temporal literals cannot be parsed as DATE; mirror
                    // StarRocks' cast-to-date semantic by yielding NULL for
                    // nullable columns instead of failing fast.
                    _ if nullable => Ok(None),
                    other => Err(format!("literal {:?} is not valid for DATE", other)),
                })
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            Ok(Arc::new(TimestampMicrosecondArray::from(
                values
                    .iter()
                    .map(|literal| match literal {
                        Literal::Null => Ok(None),
                        Literal::String(v) | Literal::Date(v) => {
                            parse_datetime_string_to_micros(v).map(Some)
                        }
                        _ if nullable => Ok(None),
                        other => Err(format!("literal {:?} is not valid for DATETIME", other)),
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            )))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            Ok(Arc::new(TimestampNanosecondArray::from(
                values
                    .iter()
                    .map(|literal| match literal {
                        Literal::Null => Ok(None),
                        Literal::String(v) | Literal::Date(v) => {
                            parse_datetime_string_to_nanos(v).map(Some)
                        }
                        _ if nullable => Ok(None),
                        other => Err(format!("literal {:?} is not valid for DATETIME", other)),
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            )))
        }
        DataType::List(field) => {
            let mut offsets = Vec::with_capacity(values.len() + 1);
            let mut nulls = NullBufferBuilder::new(values.len());
            let mut flattened = Vec::new();
            offsets.push(0_i32);

            for literal in values {
                match literal {
                    Literal::Null => {
                        nulls.append(false);
                        offsets.push(i32::try_from(flattened.len()).map_err(|_| {
                            "local table insert list value count exceeds i32 range".to_string()
                        })?);
                    }
                    Literal::Array(items) => {
                        nulls.append(true);
                        flattened.extend(items.iter());
                        offsets.push(i32::try_from(flattened.len()).map_err(|_| {
                            "local table insert list value count exceeds i32 range".to_string()
                        })?);
                    }
                    other => {
                        return Err(format!(
                            "literal {:?} is not valid for ARRAY<{:?}>",
                            other,
                            field.data_type()
                        ));
                    }
                }
            }

            let values =
                build_local_literal_array(field.data_type(), &flattened, field.is_nullable())?;
            Ok(Arc::new(ListArray::new(
                field.clone(),
                OffsetBuffer::new(offsets.into()),
                values,
                nulls.finish(),
            )))
        }
        DataType::Struct(fields) => {
            let mut struct_nulls = NullBufferBuilder::new(values.len());
            let mut child_values = vec![Vec::with_capacity(values.len()); fields.len()];
            for literal in values {
                match literal {
                    Literal::Null => {
                        struct_nulls.append(false);
                        for child in &mut child_values {
                            child.push(Literal::Null);
                        }
                    }
                    Literal::Struct(items) => {
                        if items.len() != fields.len() {
                            return Err(format!(
                                "literal {:?} does not match STRUCT field count {}",
                                literal,
                                fields.len()
                            ));
                        }
                        struct_nulls.append(true);
                        for (idx, item) in items.iter().enumerate() {
                            child_values[idx].push(item.clone());
                        }
                    }
                    other => {
                        return Err(format!(
                            "literal {:?} is not valid for STRUCT<{:?}>",
                            other, fields
                        ));
                    }
                }
            }

            let child_arrays = fields
                .iter()
                .enumerate()
                .map(|(idx, field)| {
                    if matches!(field.data_type(), DataType::List(_)) {
                        let normalized = child_values[idx]
                            .iter()
                            .map(|literal| match literal {
                                Literal::Null | Literal::Array(_) => literal.clone(),
                                other => Literal::Array(vec![other.clone()]),
                            })
                            .collect::<Vec<_>>();
                        let refs = normalized.iter().collect::<Vec<_>>();
                        build_local_literal_array(field.data_type(), &refs, field.is_nullable())
                    } else {
                        let refs = child_values[idx].iter().collect::<Vec<_>>();
                        build_local_literal_array(field.data_type(), &refs, field.is_nullable())
                    }
                })
                .collect::<Result<Vec<_>, String>>()?;
            Ok(Arc::new(StructArray::new(
                fields.clone(),
                child_arrays,
                struct_nulls.finish(),
            )))
        }
        DataType::Map(entries_field, ordered) => {
            let DataType::Struct(entry_fields) = entries_field.data_type() else {
                return Err(format!(
                    "local table insert map entries must be STRUCT, got {:?}",
                    entries_field.data_type()
                ));
            };
            if entry_fields.len() != 2 {
                return Err(format!(
                    "local table insert map entries must have 2 fields, got {}",
                    entry_fields.len()
                ));
            }

            let mut offsets = Vec::with_capacity(values.len() + 1);
            let mut map_nulls = NullBufferBuilder::new(values.len());
            let mut flattened_keys = Vec::new();
            let mut flattened_values = Vec::new();
            offsets.push(0_i32);

            // Arrow's stock Map layout requires `entries.key` to be
            // non-nullable, but the StarRocks managed-lake backend used by
            // NovaRocks supports nullable map keys (and downstream tooling
            // depends on `map{1:a, null:b}` being a distinct group from
            // `map{1:a}`). Honour the *target schema* the catalog hands us:
            // when the key field is declared nullable, preserve the NULL
            // entries; when it isn't, drop them (legacy behaviour, matches
            // the Arrow-strict path used by some other writers).
            let drop_null_keys = !entry_fields[0].is_nullable();
            for literal in values {
                match literal {
                    Literal::Null => {
                        map_nulls.append(false);
                    }
                    Literal::Map(items) => {
                        map_nulls.append(true);
                        for (key, value) in items {
                            if drop_null_keys && matches!(key, Literal::Null) {
                                continue;
                            }
                            flattened_keys.push(key.clone());
                            flattened_values.push(value.clone());
                        }
                    }
                    other => {
                        return Err(format!(
                            "literal {:?} is not valid for MAP<{:?}, {:?}>",
                            other,
                            entry_fields[0].data_type(),
                            entry_fields[1].data_type()
                        ));
                    }
                }
                offsets.push(i32::try_from(flattened_keys.len()).map_err(|_| {
                    "local table insert map entry count exceeds i32 range".to_string()
                })?);
            }

            let key_refs = flattened_keys.iter().collect::<Vec<_>>();
            let value_refs = flattened_values.iter().collect::<Vec<_>>();
            let key_array = build_local_literal_array(
                entry_fields[0].data_type(),
                &key_refs,
                entry_fields[0].is_nullable(),
            )?;
            let value_array = build_local_literal_array(
                entry_fields[1].data_type(),
                &value_refs,
                entry_fields[1].is_nullable(),
            )?;
            let entries =
                StructArray::new(entry_fields.clone(), vec![key_array, value_array], None);
            Ok(Arc::new(MapArray::new(
                entries_field.clone(),
                OffsetBuffer::new(offsets.into()),
                entries,
                map_nulls.finish(),
                *ordered,
            )))
        }
        other => Err(format!(
            "local table insert does not support column type {:?}",
            other
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Fields};
    use novarocks_catalog::schema::ColumnDef;
    use novarocks_catalog::schema::ColumnDefault;

    fn test_column(
        name: &str,
        data_type: DataType,
        write_default: Option<ColumnDefault>,
    ) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            data_type,
            nullable: true,
            write_default,
            logical_type: None,
        }
    }

    fn omitted_default_literal(
        data_type: DataType,
        write_default: Option<ColumnDefault>,
    ) -> Result<Literal, String> {
        let target_columns = vec![
            test_column("provided", DataType::Int32, None),
            test_column("omitted", data_type, write_default),
        ];
        let reordered = reorder_insert_rows(
            &[vec![Literal::Int(1)]],
            &["provided".to_string()],
            &target_columns,
        )?;
        Ok(reordered[0][1].clone())
    }

    fn test_map_type(key: DataType, value: DataType) -> DataType {
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Arc::new(Field::new("key", key, false)),
                    Arc::new(Field::new("value", value, true)),
                ])),
                false,
            )),
            false,
        )
    }

    #[test]
    fn reorder_insert_row_uses_write_default_for_omitted_column() {
        let target_columns = vec![
            ColumnDef {
                name: "a".to_string(),
                data_type: DataType::Int32,
                nullable: true,
                write_default: None,
                logical_type: None,
            },
            ColumnDef {
                name: "b".to_string(),
                data_type: DataType::Int32,
                nullable: true,
                write_default: Some(ColumnDefault::Int32(5)),
                logical_type: None,
            },
        ];
        let rows = vec![vec![Literal::Int(1)]];
        let insert_columns = vec!["a".to_string()];
        let result = reorder_insert_rows(&rows, &insert_columns, &target_columns).expect("reorder");
        assert_eq!(result[0][0], Literal::Int(1));
        assert_eq!(result[0][1], Literal::Int(5));
    }

    #[test]
    fn reorder_insert_row_rejects_extra_values_for_column_list() {
        let target_columns = vec![
            ColumnDef {
                name: "a".to_string(),
                data_type: DataType::Int32,
                nullable: true,
                write_default: None,
                logical_type: None,
            },
            ColumnDef {
                name: "b".to_string(),
                data_type: DataType::Int32,
                nullable: true,
                write_default: None,
                logical_type: None,
            },
        ];
        let rows = vec![vec![Literal::Int(1), Literal::Int(2)]];
        let insert_columns = vec!["a".to_string()];
        let err = reorder_insert_rows(&rows, &insert_columns, &target_columns)
            .expect_err("extra value must be rejected");
        assert!(
            err.contains("expected 1 values for column list, got 2"),
            "got: {err}"
        );
    }

    #[test]
    fn build_local_literal_array_large_binary_accepts_latin1_packed_bytes() {
        use crate::sql::literal::bytes_to_latin1_string;
        use arrow::array::{Array, LargeBinaryArray};

        let raw: &[u8] = &[1u8, 2, 3];
        let packed = bytes_to_latin1_string(raw);
        let lit_owned = vec![Literal::String(packed)];
        let lit_refs: Vec<&Literal> = lit_owned.iter().collect();

        let array = build_local_literal_array(&DataType::LargeBinary, &lit_refs, true)
            .expect("build LargeBinary array");
        let arr = array
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .expect("LargeBinaryArray");
        assert_eq!(arr.len(), 1);
        assert!(!arr.is_null(0));
        assert_eq!(arr.value(0), raw);
    }

    #[test]
    fn build_local_literal_array_large_binary_appends_null() {
        use arrow::array::{Array, LargeBinaryArray};

        let lit_owned = vec![Literal::Null];
        let lit_refs: Vec<&Literal> = lit_owned.iter().collect();
        let array = build_local_literal_array(&DataType::LargeBinary, &lit_refs, true)
            .expect("build LargeBinary array");
        let arr = array
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .expect("LargeBinaryArray");
        assert_eq!(arr.len(), 1);
        assert!(arr.is_null(0));
    }

    #[test]
    fn build_struct_literal_wraps_scalar_array_field_as_singleton() {
        use std::sync::Arc;

        use arrow::array::{Array, ListArray, StringArray, StructArray};
        use arrow::datatypes::Field;

        let array_field = Arc::new(Field::new(
            "col1",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ));
        let data_type = DataType::Struct(vec![array_field].into());
        let lit_owned = vec![Literal::Struct(vec![Literal::String("val1".to_string())])];
        let lit_refs = lit_owned.iter().collect::<Vec<_>>();

        let array =
            build_local_literal_array(&data_type, &lit_refs, true).expect("build struct array");

        let struct_array = array
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("StructArray");
        let list_array = struct_array
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("ListArray");
        assert_eq!(list_array.len(), 1);
        assert!(!list_array.is_null(0));
        let values = list_array.value(0);
        let strings = values
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("StringArray");
        assert_eq!(strings.len(), 1);
        assert_eq!(strings.value(0), "val1");
    }

    #[test]
    fn reorder_insert_row_uses_null_when_no_write_default() {
        let target_columns = vec![
            ColumnDef {
                name: "a".to_string(),
                data_type: DataType::Int32,
                nullable: true,
                write_default: None,
                logical_type: None,
            },
            ColumnDef {
                name: "b".to_string(),
                data_type: DataType::Int32,
                nullable: true,
                write_default: None,
                logical_type: None,
            },
        ];
        let rows = vec![vec![Literal::Int(1)]];
        let insert_columns = vec!["a".to_string()];
        let result = reorder_insert_rows(&rows, &insert_columns, &target_columns).expect("reorder");
        assert_eq!(result[0][1], Literal::Null);
    }

    #[test]
    fn reorder_insert_row_characterizes_neutral_write_defaults() {
        let full_binary = (0_u16..=255).map(|byte| byte as u8).collect::<Vec<_>>();
        let cases = vec![
            (
                "integer",
                DataType::Int32,
                ColumnDefault::Int32(5),
                Literal::Int(5),
            ),
            (
                "string",
                DataType::Utf8,
                ColumnDefault::String("value".to_string()),
                Literal::String("value".to_string()),
            ),
            (
                "decimal",
                DataType::Decimal128(10, 2),
                ColumnDefault::Decimal {
                    unscaled: 12_345,
                    precision: 10,
                    scale: 2,
                },
                Literal::String("123.45".to_string()),
            ),
            (
                "date",
                DataType::Date32,
                ColumnDefault::Date {
                    days_since_epoch: -1,
                },
                Literal::Date("1969-12-31".to_string()),
            ),
            (
                "datetime",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                ColumnDefault::TimestampMicros {
                    micros_since_epoch: 1_704_110_400_123_456,
                },
                Literal::String("2024-01-01 12:00:00".to_string()),
            ),
            (
                "datetime-ns",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                ColumnDefault::TimestampNanos {
                    nanos_since_epoch: 1_704_164_645_123_456_789,
                },
                Literal::String("2024-01-02 03:04:05.123456789".to_string()),
            ),
            (
                "binary",
                DataType::Binary,
                ColumnDefault::Binary(full_binary.clone()),
                Literal::String(crate::sql::literal::bytes_to_latin1_string(&full_binary)),
            ),
            (
                "empty-array",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                ColumnDefault::Array(Vec::new()),
                Literal::Array(Vec::new()),
            ),
            (
                "empty-map",
                test_map_type(DataType::Int32, DataType::Utf8),
                ColumnDefault::Map(Vec::new()),
                Literal::Map(Vec::new()),
            ),
        ];

        for (name, data_type, write_default, expected) in cases {
            assert_eq!(
                omitted_default_literal(data_type, Some(write_default)),
                Ok(expected),
                "case={name}"
            );
        }
    }

    #[test]
    fn reorder_insert_row_characterizes_non_empty_collection_default_errors() {
        let list_error = omitted_default_literal(
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            Some(ColumnDefault::Array(vec![ColumnDefault::Int32(1)])),
        )
        .unwrap_err();
        assert_eq!(
            list_error,
            "non-empty ARRAY write-default is not yet supported (1 elements)"
        );

        let map_error = omitted_default_literal(
            test_map_type(DataType::Int32, DataType::Utf8),
            Some(ColumnDefault::Map(vec![(
                ColumnDefault::Int32(1),
                ColumnDefault::String("value".to_string()),
            )])),
        )
        .unwrap_err();
        assert_eq!(
            map_error,
            "non-empty MAP write-default is not yet supported (1 entries)"
        );
    }
}

fn parse_decimal_string_to_i128(s: &str, scale: i8) -> Result<i128, String> {
    let s = s.trim();
    let factor = 10_i128
        .checked_pow(scale as u32)
        .ok_or_else(|| format!("decimal scale {} is too large", scale))?;
    if let Some(dot_pos) = s.find('.') {
        let negative = s.starts_with('-');
        let int_part = &s[if negative { 1 } else { 0 }..dot_pos];
        let frac_str = &s[dot_pos + 1..];
        let int_val: i128 = if int_part.is_empty() {
            0
        } else {
            int_part
                .parse()
                .map_err(|_| format!("invalid decimal literal `{s}`"))?
        };
        let frac_len = frac_str.len();
        let frac_val: i128 = if frac_str.is_empty() {
            0
        } else {
            frac_str
                .parse()
                .map_err(|_| format!("invalid decimal literal `{s}`"))?
        };
        let scale_u = scale as usize;
        let adjusted_frac = if frac_len <= scale_u {
            frac_val * 10_i128.pow((scale_u - frac_len) as u32)
        } else {
            frac_val / 10_i128.pow((frac_len - scale_u) as u32)
        };
        let abs_val = int_val * factor + adjusted_frac;
        Ok(if negative { -abs_val } else { abs_val })
    } else {
        let int_val: i128 = s
            .parse()
            .map_err(|_| format!("invalid decimal literal `{s}`"))?;
        Ok(int_val * factor)
    }
}
