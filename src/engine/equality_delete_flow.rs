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

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array, Int32Array,
    Int64Array, StringArray, Time64MicrosecondArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use arrow::record_batch::RecordBatch;
use iceberg::Catalog;
use iceberg::spec::{FormatVersion, PrimitiveType, Type};

use crate::connector::iceberg::catalog::registry::{block_on_iceberg, build_hadoop_catalog};
use crate::connector::iceberg::commit::{
    CommitOpKind, EqualityDeleteColumn, IcebergCommitCollector, RunInput,
    ensure_equality_delete_single_partition_spec, ensure_no_variant_columns_for_row_level_mutation,
    run_iceberg_commit, write_equality_delete_file,
};
use crate::engine::backend_resolver::resolve_existing_table_target;
use crate::engine::parquet::{parse_date_string_to_days, parse_datetime_string_to_micros};
use crate::engine::statement::AddEqualityDeleteStmt;
use crate::engine::{StandaloneState, StatementResult};
use crate::sql::parser::ast::Literal;

pub(crate) fn execute_add_equality_delete_statement(
    state: &Arc<StandaloneState>,
    stmt: &AddEqualityDeleteStmt,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<StatementResult, String> {
    let target =
        resolve_existing_table_target(state, &stmt.table, current_catalog, current_database)?;
    if target.backend_name != "iceberg" {
        return Err(format!(
            "ADD EQUALITY DELETE only supports iceberg backends, got `{}`",
            target.backend_name
        ));
    }

    let entry = {
        let registry = state
            .iceberg_catalogs
            .read()
            .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
        registry.get(&target.catalog)?
    };
    let hadoop_catalog = build_hadoop_catalog(&entry)?;
    let catalog: Arc<dyn Catalog> = Arc::new(hadoop_catalog);
    let table_ident = iceberg::TableIdent::new(
        iceberg::NamespaceIdent::new(target.namespace.clone()),
        target.table.clone(),
    );
    let table = block_on_iceberg(async { catalog.load_table(&table_ident).await })?
        .map_err(|e| format!("load iceberg table {}: {e}", &table_ident))?;

    ensure_no_variant_columns_for_row_level_mutation(&table)
        .map_err(|e| format!("ADD EQUALITY DELETE: {e}"))?;
    let metadata = table.metadata();
    if metadata.format_version() == FormatVersion::V1 {
        return Err("ADD EQUALITY DELETE requires an Iceberg v2 or v3 table".to_string());
    }
    ensure_equality_delete_single_partition_spec(&table)?;
    if !metadata.default_partition_spec().fields().is_empty() {
        return Err(
            "ADD EQUALITY DELETE currently supports only unpartitioned iceberg tables".to_string(),
        );
    }

    let (delete_columns, batch) = build_equality_delete_batch(
        metadata.current_schema().as_ref(),
        &stmt.columns,
        &stmt.rows,
    )?;
    if batch.num_rows() == 0 {
        return Ok(StatementResult::Ok);
    }

    let staging_dir = format!(
        "{}/data/_staging/{}",
        metadata.location(),
        uuid::Uuid::new_v4()
    );
    let file_io = table.file_io().clone();
    let default_spec_id = metadata.default_partition_spec_id();
    let current_snapshot_id = metadata.current_snapshot().map(|s| s.snapshot_id());
    let last_sequence_number = metadata.last_sequence_number();
    let current_schema = metadata.current_schema().clone();
    let default_partition_spec = metadata.default_partition_spec().clone();

    let Some(written) = block_on_iceberg(async {
        write_equality_delete_file(
            &file_io,
            &staging_dir,
            default_spec_id,
            delete_columns,
            batch,
        )
        .await
    })??
    else {
        return Ok(StatementResult::Ok);
    };

    let collector = Arc::new(IcebergCommitCollector::new(
        CommitOpKind::RowDelta,
        table_ident,
        current_snapshot_id,
        last_sequence_number,
        current_schema,
        default_partition_spec,
        staging_dir,
        crate::common::types::UniqueId { hi: 0, lo: 0 },
    ));
    collector.inject_written_file(written);

    let abort_cleanup =
        crate::engine::iceberg_writer::build_abort_cleanup_for_catalog_entry(&entry)?;
    let _outcome = block_on_iceberg(async {
        run_iceberg_commit(RunInput {
            collector: collector.clone(),
            catalog: catalog.clone(),
            table,
            fs: abort_cleanup.fs,
            file_io,
            cleanup_path_mapper: abort_cleanup.path_mapper,
            cow_update_rewrite: None,
            target_ref: "main".to_string(),
            snapshot_properties: BTreeMap::new(),
        })
        .await
    })??;

    crate::engine::iceberg_writer::invalidate_iceberg_caches(state, &target)?;
    Ok(StatementResult::Ok)
}

fn build_equality_delete_batch(
    schema: &iceberg::spec::Schema,
    column_names: &[String],
    rows: &[Vec<Literal>],
) -> Result<(Vec<EqualityDeleteColumn>, RecordBatch), String> {
    if column_names.is_empty() {
        return Err("ADD EQUALITY DELETE requires at least one equality column".to_string());
    }
    let mut seen = HashSet::new();
    for name in column_names {
        let lowered = name.to_ascii_lowercase();
        if !seen.insert(lowered) {
            return Err(format!(
                "ADD EQUALITY DELETE has duplicate equality column `{name}`"
            ));
        }
    }
    for row in rows {
        if row.len() != column_names.len() {
            return Err(format!(
                "ADD EQUALITY DELETE row has {} values, expected {}",
                row.len(),
                column_names.len()
            ));
        }
    }

    let mut delete_columns = Vec::with_capacity(column_names.len());
    for column_name in column_names {
        let field = schema
            .as_struct()
            .fields()
            .iter()
            .find(|field| field.name.eq_ignore_ascii_case(column_name))
            .ok_or_else(|| format!("column `{column_name}` not found in iceberg table schema"))?;
        let primitive = match &*field.field_type {
            Type::Primitive(primitive) => primitive,
            other => {
                return Err(format!(
                    "ADD EQUALITY DELETE only supports primitive equality columns; column `{}` is {other:?}",
                    field.name
                ));
            }
        };
        delete_columns.push(EqualityDeleteColumn {
            name: field.name.clone(),
            field_id: field.id,
            data_type: primitive_to_arrow_type(primitive, &field.name)?,
            nullable: !field.required,
        });
    }

    let mut arrays = Vec::with_capacity(delete_columns.len());
    for (col_idx, column) in delete_columns.iter().enumerate() {
        let values = rows.iter().map(|row| &row[col_idx]).collect::<Vec<_>>();
        arrays.push(build_literal_array_for_equality(column, &values)?);
    }
    let arrow_schema = Arc::new(ArrowSchema::new(
        delete_columns
            .iter()
            .map(|column| Field::new(&column.name, column.data_type.clone(), column.nullable))
            .collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new(arrow_schema, arrays)
        .map_err(|e| format!("build equality-delete batch failed: {e}"))?;
    Ok((delete_columns, batch))
}

fn primitive_to_arrow_type(
    primitive: &PrimitiveType,
    column_name: &str,
) -> Result<DataType, String> {
    Ok(match primitive {
        PrimitiveType::Boolean => DataType::Boolean,
        PrimitiveType::Int => DataType::Int32,
        PrimitiveType::Long => DataType::Int64,
        PrimitiveType::Float => DataType::Float32,
        PrimitiveType::Double => DataType::Float64,
        PrimitiveType::Decimal { precision, scale } => {
            let precision = u8::try_from(*precision).map_err(|_| {
                format!("DECIMAL precision {precision} is out of range for column `{column_name}`")
            })?;
            let scale = i8::try_from(*scale).map_err(|_| {
                format!("DECIMAL scale {scale} is out of range for column `{column_name}`")
            })?;
            DataType::Decimal128(precision, scale)
        }
        PrimitiveType::Date => DataType::Date32,
        PrimitiveType::Time => DataType::Time64(TimeUnit::Microsecond),
        PrimitiveType::Timestamp | PrimitiveType::Timestamptz => {
            DataType::Timestamp(TimeUnit::Microsecond, None)
        }
        PrimitiveType::String => DataType::Utf8,
        other => {
            return Err(format!(
                "ADD EQUALITY DELETE does not yet support equality column `{column_name}` with type {other:?}"
            ));
        }
    })
}

fn build_literal_array_for_equality(
    column: &EqualityDeleteColumn,
    values: &[&Literal],
) -> Result<ArrayRef, String> {
    ensure_nullability(column, values)?;
    match &column.data_type {
        DataType::Boolean => Ok(Arc::new(BooleanArray::from(
            values
                .iter()
                .map(|value| match value {
                    Literal::Null => Ok(None),
                    Literal::Bool(v) => Ok(Some(*v)),
                    other => Err(format!(
                        "literal {:?} is not valid for BOOLEAN equality column `{}`",
                        other, column.name
                    )),
                })
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        DataType::Int32 => Ok(Arc::new(Int32Array::from(
            values
                .iter()
                .map(|value| match value {
                    Literal::Null => Ok(None),
                    Literal::Int(v) => i32::try_from(*v).map(Some).map_err(|_| {
                        format!(
                            "literal {v} is out of range for INT equality column `{}`",
                            column.name
                        )
                    }),
                    Literal::String(v) => v.trim().parse::<i32>().map(Some).map_err(|_| {
                        format!(
                            "literal `{v}` is not valid for INT equality column `{}`",
                            column.name
                        )
                    }),
                    other => Err(format!(
                        "literal {:?} is not valid for INT equality column `{}`",
                        other, column.name
                    )),
                })
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        DataType::Int64 => Ok(Arc::new(Int64Array::from(
            values
                .iter()
                .map(|value| match value {
                    Literal::Null => Ok(None),
                    Literal::Int(v) => Ok(Some(*v)),
                    Literal::String(v) => v.trim().parse::<i64>().map(Some).map_err(|_| {
                        format!(
                            "literal `{v}` is not valid for LONG equality column `{}`",
                            column.name
                        )
                    }),
                    other => Err(format!(
                        "literal {:?} is not valid for LONG equality column `{}`",
                        other, column.name
                    )),
                })
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        DataType::Float32 => Ok(Arc::new(Float32Array::from(
            values
                .iter()
                .map(|value| match value {
                    Literal::Null => Ok(None),
                    Literal::Int(v) => Ok(Some(*v as f32)),
                    Literal::Float(v) => Ok(Some(*v as f32)),
                    Literal::String(v) => v.trim().parse::<f32>().map(Some).map_err(|_| {
                        format!(
                            "literal `{v}` is not valid for FLOAT equality column `{}`",
                            column.name
                        )
                    }),
                    other => Err(format!(
                        "literal {:?} is not valid for FLOAT equality column `{}`",
                        other, column.name
                    )),
                })
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        DataType::Float64 => Ok(Arc::new(Float64Array::from(
            values
                .iter()
                .map(|value| match value {
                    Literal::Null => Ok(None),
                    Literal::Int(v) => Ok(Some(*v as f64)),
                    Literal::Float(v) => Ok(Some(*v)),
                    Literal::String(v) => v.trim().parse::<f64>().map(Some).map_err(|_| {
                        format!(
                            "literal `{v}` is not valid for DOUBLE equality column `{}`",
                            column.name
                        )
                    }),
                    other => Err(format!(
                        "literal {:?} is not valid for DOUBLE equality column `{}`",
                        other, column.name
                    )),
                })
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        DataType::Decimal128(precision, scale) => {
            let values = values
                .iter()
                .map(|value| match value {
                    Literal::Null => Ok(None),
                    Literal::Int(v) => scale_i128_decimal(i128::from(*v), *scale).map(Some),
                    Literal::Float(v) => {
                        parse_decimal_literal_to_i128(&v.to_string(), *scale).map(Some)
                    }
                    Literal::String(v) => parse_decimal_literal_to_i128(v, *scale).map(Some),
                    other => Err(format!(
                        "literal {:?} is not valid for DECIMAL equality column `{}`",
                        other, column.name
                    )),
                })
                .collect::<Result<Vec<_>, _>>()?;
            let array = Decimal128Array::from(values)
                .with_precision_and_scale(*precision, *scale)
                .map_err(|e| {
                    format!(
                        "build DECIMAL equality array for column `{}` failed: {e}",
                        column.name
                    )
                })?;
            Ok(Arc::new(array))
        }
        DataType::Utf8 => Ok(Arc::new(StringArray::from(
            values
                .iter()
                .map(|value| match value {
                    Literal::Null => Ok(None),
                    Literal::String(v) | Literal::Date(v) => Ok(Some(v.clone())),
                    other => Err(format!(
                        "literal {:?} is not valid for STRING equality column `{}`",
                        other, column.name
                    )),
                })
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        DataType::Date32 => Ok(Arc::new(Date32Array::from(
            values
                .iter()
                .map(|value| match value {
                    Literal::Null => Ok(None),
                    Literal::Date(v) | Literal::String(v) => parse_date_string_to_days(v).map(Some),
                    other => Err(format!(
                        "literal {:?} is not valid for DATE equality column `{}`",
                        other, column.name
                    )),
                })
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        DataType::Time64(TimeUnit::Microsecond) => Ok(Arc::new(Time64MicrosecondArray::from(
            values
                .iter()
                .map(|value| match value {
                    Literal::Null => Ok(None),
                    Literal::String(v) => parse_time_literal_to_micros(v).map(Some),
                    other => Err(format!(
                        "literal {:?} is not valid for TIME equality column `{}`",
                        other, column.name
                    )),
                })
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            Ok(Arc::new(TimestampMicrosecondArray::from(
                values
                    .iter()
                    .map(|value| match value {
                        Literal::Null => Ok(None),
                        Literal::Date(v) | Literal::String(v) => {
                            parse_datetime_string_to_micros(v).map(Some)
                        }
                        other => Err(format!(
                            "literal {:?} is not valid for TIMESTAMP equality column `{}`",
                            other, column.name
                        )),
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            )))
        }
        other => Err(format!(
            "ADD EQUALITY DELETE does not yet support equality column `{}` with arrow type {:?}",
            column.name, other
        )),
    }
}

fn ensure_nullability(column: &EqualityDeleteColumn, values: &[&Literal]) -> Result<(), String> {
    if column.nullable {
        return Ok(());
    }
    if values.iter().any(|value| matches!(value, Literal::Null)) {
        return Err(format!(
            "NULL is not valid for required equality column `{}`",
            column.name
        ));
    }
    Ok(())
}

fn parse_time_literal_to_micros(value: &str) -> Result<i64, String> {
    use chrono::{NaiveTime, Timelike};

    let time = NaiveTime::parse_from_str(value.trim(), "%H:%M:%S%.f")
        .or_else(|_| NaiveTime::parse_from_str(value.trim(), "%H:%M:%S"))
        .map_err(|e| format!("invalid time literal `{value}`: {e}"))?;
    Ok(i64::from(time.num_seconds_from_midnight()) * 1_000_000
        + i64::from(time.nanosecond() / 1_000))
}

fn scale_i128_decimal(value: i128, scale: i8) -> Result<i128, String> {
    if scale < 0 {
        return Err(format!("negative DECIMAL scale {scale} is not supported"));
    }
    let factor = 10_i128
        .checked_pow(scale as u32)
        .ok_or_else(|| format!("DECIMAL scale {scale} is out of range"))?;
    value
        .checked_mul(factor)
        .ok_or_else(|| format!("DECIMAL literal {value} is out of range"))
}

fn parse_decimal_literal_to_i128(value: &str, scale: i8) -> Result<i128, String> {
    const I128_MIN_ABS: &str = "170141183460469231731687303715884105728";

    if scale < 0 {
        return Err(format!("negative DECIMAL scale {scale} is not supported"));
    }
    let trimmed = value.trim();
    let (negative, raw) = if let Some(raw) = trimmed.strip_prefix('-') {
        (true, raw)
    } else if let Some(raw) = trimmed.strip_prefix('+') {
        (false, raw)
    } else {
        (false, trimmed)
    };
    let (whole, fraction) = raw.split_once('.').unwrap_or((raw, ""));
    if fraction.len() > scale as usize {
        return Err(format!(
            "DECIMAL literal `{value}` has more than {scale} fractional digits"
        ));
    }
    let padded_fraction = format!("{fraction:0<width$}", width = scale as usize);
    let combined = format!("{whole}{padded_fraction}");
    let combined = combined.trim_start_matches('+');
    let mut parsed = if combined.is_empty() {
        0_i128
    } else if negative && scale == 0 && combined == I128_MIN_ABS {
        i128::MIN
    } else {
        combined
            .parse::<i128>()
            .map_err(|_| format!("DECIMAL literal `{value}` is out of range"))?
    };
    if negative && parsed != i128::MIN {
        parsed = -parsed;
    }
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, Int32Array, StringArray};
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};

    use crate::sql::parser::ast::Literal;

    #[test]
    fn build_equality_delete_batch_projects_key_columns_with_field_ids() {
        let schema = Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Int),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "category",
                    Type::Primitive(PrimitiveType::String),
                )),
                Arc::new(NestedField::optional(
                    3,
                    "amount",
                    Type::Primitive(PrimitiveType::Long),
                )),
            ])
            .build()
            .expect("schema");
        let columns = vec!["id".to_string(), "category".to_string()];
        let rows = vec![
            vec![Literal::Int(2), Literal::String("B".to_string())],
            vec![Literal::Int(4), Literal::Null],
        ];

        let (delete_columns, batch) =
            super::build_equality_delete_batch(&schema, &columns, &rows).expect("batch");

        assert_eq!(
            delete_columns
                .iter()
                .map(|c| c.field_id)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(batch.num_rows(), 2);
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("id column");
        assert_eq!(ids.values(), &[2, 4]);
        let categories = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("category column");
        assert_eq!(categories.value(0), "B");
        assert!(categories.is_null(1));
    }
}
