//! Local-table INSERT path: reorder/validate inbound rows, build Arrow
//! arrays from `Literal` rows, and append to the parquet file on disk.
//!
//! Covers both `INSERT VALUES` and `INSERT SELECT`/`GenerateSeriesSelect`
//! sources — the SELECT-side fragment has already been lowered to a
//! `Vec<Literal>` row by the time we get here.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;

use super::{ColumnDef, TableDef, TableStorage, normalize_identifier};
use crate::sql::parser::ast::{InsertSource, Literal};
use crate::standalone::engine::sqlparse::expr::{
    latin1_string_to_bytes, literal_to_i128_for_integer,
};
use crate::standalone::engine::{
    ResolvedLocalTableName, StandaloneState, apply_local_table_semantics_if_needed,
    insert_generate_series_rows_local,
};

use super::parquet::{
    normalize_map_entries_nullability, parse_date_string_to_days, parse_datetime_string_to_micros,
    read_local_parquet_data, write_parquet_to_path,
};

pub(crate) fn reorder_insert_rows(
    rows: &[Vec<Literal>],
    insert_columns: &[String],
    target_columns: &[ColumnDef],
) -> Result<Vec<Vec<Literal>>, String> {
    if insert_columns.is_empty() {
        return Ok(rows.to_vec());
    }
    let mapping = build_insert_column_mapping(insert_columns, target_columns)?;
    rows.iter()
        .map(|row| reorder_insert_row(row, &mapping, target_columns.len()))
        .collect()
}

fn build_insert_column_mapping(
    insert_columns: &[String],
    target_columns: &[ColumnDef],
) -> Result<Vec<Option<usize>>, String> {
    let mut insert_index_by_name = HashMap::with_capacity(insert_columns.len());
    for (idx, column) in insert_columns.iter().enumerate() {
        let key = normalize_identifier(column)?;
        if insert_index_by_name.insert(key, idx).is_some() {
            return Err(format!("duplicate INSERT column `{column}`"));
        }
    }

    let mut mapping = Vec::with_capacity(target_columns.len());
    for column in target_columns {
        let key = normalize_identifier(&column.name)?;
        mapping.push(insert_index_by_name.remove(&key));
    }
    if let Some((name, _)) = insert_index_by_name.into_iter().next() {
        return Err(format!("unknown INSERT column `{name}`"));
    }
    Ok(mapping)
}

fn reorder_insert_row(
    row: &[Literal],
    mapping: &[Option<usize>],
    target_width: usize,
) -> Result<Vec<Literal>, String> {
    if row.len() > target_width {
        return Err(format!(
            "insert column count mismatch: expected at most {target_width} values, got {}",
            row.len()
        ));
    }
    let mut reordered = Vec::with_capacity(target_width);
    for source_idx in mapping {
        match source_idx {
            Some(idx) => {
                let value = row.get(*idx).cloned().ok_or_else(|| {
                    format!("insert value for column position {} is missing", idx + 1)
                })?;
                reordered.push(value);
            }
            None => reordered.push(Literal::Null),
        }
    }
    Ok(reordered)
}

/// Insert rows into a local parquet table.
pub(crate) fn insert_into_local_table(
    state: &Arc<StandaloneState>,
    resolved: &ResolvedLocalTableName,
    table_def: &TableDef,
    insert_columns: &[String],
    source: &InsertSource,
) -> Result<crate::standalone::engine::StatementResult, String> {
    use crate::standalone::engine::StatementResult;

    let rows = match source {
        InsertSource::Values(rows) => {
            reorder_insert_rows(rows, insert_columns, &table_def.columns)?
        }
        InsertSource::SelectLiteralRow(row) => reorder_insert_rows(
            std::slice::from_ref(row),
            insert_columns,
            &table_def.columns,
        )?,
        InsertSource::GenerateSeriesSelect(gen_source) => {
            insert_generate_series_rows_local(gen_source, insert_columns, &table_def.columns)?
        }
    };

    let path = match &table_def.storage {
        TableStorage::LocalParquetFile { path } => path.clone(),
        TableStorage::S3ParquetFiles { .. } => {
            return Err("INSERT into S3 tables is not supported".to_string());
        }
    };

    // Read existing data if any
    let existing_batch = read_local_parquet_data(&path, &table_def.columns)?;

    // Build new batch from inserted rows
    let new_batch = build_local_insert_batch(&table_def.columns, &rows)?;

    // Concatenate existing and new
    let combined = if existing_batch.num_rows() > 0 {
        arrow::compute::concat_batches(
            &new_batch.schema(),
            [&existing_batch, &new_batch].iter().copied(),
        )
        .map_err(|e| format!("concat local table batches failed: {e}"))?
    } else {
        new_batch
    };
    let combined =
        apply_local_table_semantics_if_needed(state, resolved, &table_def.columns, combined)?;

    // Write back
    write_parquet_to_path(&path, &combined)?;

    // Re-register table in catalog to update metadata
    let table_def_updated = TableDef {
        name: table_def.name.clone(),
        columns: table_def.columns.clone(),
        storage: TableStorage::LocalParquetFile { path: path.clone() },
    };
    let mut guard = state
        .catalog
        .write()
        .expect("standalone catalog write lock");
    // Drop and re-register to allow re-reading
    let _ = guard.drop_table(&resolved.database, &resolved.table);
    guard.register(&resolved.database, table_def_updated)?;

    Ok(StatementResult::Ok)
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
        arrays.push(build_local_literal_array(&column.data_type, &values)?);
    }

    RecordBatch::try_new(schema, arrays)
        .map_err(|e| format!("build local insert batch failed: {e}"))
}

/// Build an Arrow array from literal values for local table insertion.
fn build_local_literal_array(
    data_type: &DataType,
    values: &[&Literal],
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
                            i8::try_from(value)
                                .map(Some)
                                .map_err(|_| format!("literal {value} is out of range for TINYINT"))
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
                        |value| {
                            i16::try_from(value).map(Some).map_err(|_| {
                                format!("literal {value} is out of range for SMALLINT")
                            })
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
                        i32::try_from(value)
                            .map(Some)
                            .map_err(|_| format!("literal {value} is out of range for INT"))
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
                            i64::try_from(value)
                                .map(Some)
                                .map_err(|_| format!("literal {value} is out of range for BIGINT"))
                        })
                    }
                })
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        DataType::FixedSizeBinary(width)
            if *width == crate::common::largeint::LARGEINT_BYTE_WIDTH =>
        {
            let parsed = values
                .iter()
                .map(|literal| literal_to_i128_for_integer(literal, "LARGEINT"))
                .collect::<Result<Vec<_>, _>>()?;
            crate::common::largeint::array_from_i128(&parsed)
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

            let values = build_local_literal_array(field.data_type(), &flattened)?;
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
                    let refs = child_values[idx].iter().collect::<Vec<_>>();
                    build_local_literal_array(field.data_type(), &refs)
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

            for literal in values {
                match literal {
                    Literal::Null => {
                        map_nulls.append(false);
                    }
                    Literal::Map(items) => {
                        map_nulls.append(true);
                        for (key, value) in items {
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
            let key_array = build_local_literal_array(entry_fields[0].data_type(), &key_refs)?;
            let value_array = build_local_literal_array(entry_fields[1].data_type(), &value_refs)?;
            let entries_fields = if key_array.null_count() > 0 && !entry_fields[0].is_nullable() {
                let mut adjusted = entry_fields.iter().cloned().collect::<Vec<_>>();
                adjusted[0] = Arc::new(Field::new(
                    entry_fields[0].name(),
                    entry_fields[0].data_type().clone(),
                    true,
                ));
                Fields::from(adjusted)
            } else {
                entry_fields.clone()
            };
            let entries =
                StructArray::new(entries_fields.clone(), vec![key_array, value_array], None);
            let entries_field = Arc::new(Field::new(
                entries_field.name(),
                DataType::Struct(entries_fields),
                false,
            ));
            Ok(Arc::new(MapArray::new(
                entries_field,
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
