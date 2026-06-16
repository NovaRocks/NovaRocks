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

use crate::connector::iceberg::catalog::registry::{block_on_iceberg, build_iceberg_catalog};
use crate::connector::iceberg::commit::{
    CommitOpKind, CommitOutcome, CommitServiceError, EqualityDeleteColumn, IcebergCommitCollector,
    ensure_equality_delete_single_partition_spec, ensure_no_variant_columns_for_row_level_mutation,
};
use crate::engine::backend_resolver::resolve_existing_table_target;
use crate::engine::parquet::{parse_date_string_to_days, parse_datetime_string_to_micros};
use crate::engine::statement::AddEqualityDeleteStmt;
use crate::engine::write_transaction::{
    IcebergWriteCommitExecutor, IcebergWriteCommitPolicy, IcebergWriteSource,
    IcebergWriteTransactionExecutor, IcebergWriteTransactionRunner, IcebergWriteTransactionSpec,
    IcebergWriteValidationPolicy, write_commit_has_files,
};
use crate::engine::{StandaloneState, StatementResult};
use crate::meta::repository::iceberg_operation::{IcebergOperationKind, IcebergOperationTarget};
use crate::runtime::coordinator::CoordinatedQueryResult;
use crate::runtime::write_coordinator::WriteCommitInput;
use crate::sql::catalog::ColumnDef;
use crate::sql::codegen::iceberg_write_sink::{IcebergWriteSinkMode, IcebergWriteSinkSpec};
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
    let catalog: Arc<dyn Catalog> = build_iceberg_catalog(&entry)?;
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
    let values_query = build_equality_delete_sink_query(&delete_columns, &stmt.rows)?;

    let current_snapshot_id = metadata.current_snapshot().map(|s| s.snapshot_id());
    // Route non-empty input through the distributed sink transaction.
    run_equality_delete_distributed_transaction(
        state,
        &target,
        catalog,
        table,
        entry,
        current_snapshot_id,
        &delete_columns,
        values_query,
    )?;
    Ok(StatementResult::Ok)
}

struct DistributedEqualityDeleteWriteExecutor {
    state: Arc<StandaloneState>,
    target: crate::engine::backend_resolver::TargetBackend,
    delete_query: sqlparser::ast::Query,
    sink_spec: IcebergWriteSinkSpec,
    commit_executor: IcebergWriteCommitExecutor,
}

impl IcebergWriteTransactionExecutor for DistributedEqualityDeleteWriteExecutor {
    fn run_coordinated_write(
        &self,
        _spec: &IcebergWriteTransactionSpec,
    ) -> Result<CoordinatedQueryResult, String> {
        let result = crate::engine::execute_query_as_iceberg_write(
            &self.state,
            Some(&self.target.catalog),
            &self.target.namespace,
            &self.delete_query,
            self.sink_spec.clone(),
            None,
            None,
        )?;
        if result
            .write_commit
            .as_ref()
            .is_none_or(|commit| !write_commit_has_files(commit))
        {
            return Err(
                "ADD EQUALITY DELETE produced no equality-delete files for non-empty input"
                    .to_string(),
            );
        }
        Ok(result)
    }

    fn commit(
        &self,
        _spec: &IcebergWriteTransactionSpec,
        write_commit: &WriteCommitInput,
    ) -> Result<CommitOutcome, CommitServiceError> {
        self.commit_executor.commit_write_input(write_commit)
    }

    fn finalize(&self, _spec: &IcebergWriteTransactionSpec) -> Result<(), String> {
        self.commit_executor.finalize()
    }
}

#[allow(clippy::too_many_arguments)]
fn run_equality_delete_distributed_transaction(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    catalog: Arc<dyn Catalog>,
    table: iceberg::table::Table,
    entry: crate::connector::iceberg::catalog::IcebergCatalogEntry,
    current_snapshot_id: Option<i64>,
    delete_columns: &[EqualityDeleteColumn],
    values_query: sqlparser::ast::Query,
) -> Result<(), String> {
    let resolved = {
        let registry = state.connectors.read().expect("connector registry read");
        let backend = registry.catalog_backend("iceberg")?;
        backend.load_table(&target.catalog, &target.namespace, &target.table)?
    };
    let mut sink_spec = crate::engine::iceberg_writer::build_equality_delete_sink_spec(
        target,
        &resolved,
        &table,
        &entry,
        delete_columns,
    )?;
    sink_spec.mode = IcebergWriteSinkMode::EqualityDeletes;
    sink_spec.set_planned_snapshot_id(current_snapshot_id)?;

    let metadata = table.metadata();
    let table_ident = iceberg::TableIdent::new(
        iceberg::NamespaceIdent::new(target.namespace.clone()),
        target.table.clone(),
    );
    let staging_dir = format!(
        "{}/data/_staging/{}",
        metadata.location(),
        uuid::Uuid::new_v4()
    );
    let collector = Arc::new(
        IcebergCommitCollector::new(
            CommitOpKind::RowDelta,
            table_ident,
            current_snapshot_id,
            metadata.last_sequence_number(),
            metadata.current_schema().clone(),
            metadata.default_partition_spec().clone(),
            staging_dir,
            crate::common::types::UniqueId { hi: 0, lo: 0 },
        )
        .with_table_metadata(metadata.clone()),
    );
    let abort_cleanup =
        crate::engine::iceberg_writer::build_abort_cleanup_for_catalog_entry(&entry)?;
    let commit_executor = IcebergWriteCommitExecutor {
        state: Arc::clone(state),
        target: target.clone(),
        catalog,
        table: table.clone(),
        collector: Arc::clone(&collector),
        fs: abort_cleanup.fs,
        cleanup_path_mapper: abort_cleanup.path_mapper,
        cow_update_rewrite: None,
        target_ref: "main".to_string(),
        snapshot_properties: BTreeMap::new(),
    };
    let spec = IcebergWriteTransactionSpec {
        target: IcebergOperationTarget {
            catalog: target.catalog.clone(),
            namespace: target.namespace.clone(),
            table: target.table.clone(),
            ref_name: None,
        },
        operation_kind: IcebergOperationKind::RowDelta,
        attempt_id: format!(
            "{}.{}.{}:equality-delete:{}",
            target.catalog,
            target.namespace,
            target.table,
            uuid::Uuid::new_v4()
        ),
        commit: IcebergWriteCommitPolicy {
            commit_op_kind: CommitOpKind::RowDelta,
            base_snapshot_id: current_snapshot_id,
            base_snapshot_map: BTreeMap::new(),
            target_ref: "main".to_string(),
            snapshot_properties: BTreeMap::new(),
        },
        validation: IcebergWriteValidationPolicy {
            require_v3_for_branch: false,
        },
        source: IcebergWriteSource::CoordinatedPlan,
    };
    let executor = DistributedEqualityDeleteWriteExecutor {
        state: Arc::clone(state),
        target: target.clone(),
        delete_query: values_query,
        sink_spec,
        commit_executor,
    };
    let runner = IcebergWriteTransactionRunner::new(Arc::clone(state), &executor);
    let _outcome = runner.run(spec)?;
    Ok(())
}

fn build_equality_delete_sink_query(
    delete_columns: &[EqualityDeleteColumn],
    rows: &[Vec<Literal>],
) -> Result<sqlparser::ast::Query, String> {
    if delete_columns.is_empty() {
        return Err("ADD EQUALITY DELETE requires at least one equality column".to_string());
    }
    if rows.is_empty() {
        return Err("ADD EQUALITY DELETE sink query requires at least one row".to_string());
    }
    for row in rows {
        if row.len() != delete_columns.len() {
            return Err(format!(
                "ADD EQUALITY DELETE row has {} values, expected {}",
                row.len(),
                delete_columns.len()
            ));
        }
    }

    let target_columns = equality_delete_target_columns(delete_columns);
    let alias = "__nr_eqdel";
    let rendered_rows = rows
        .iter()
        .map(|row| {
            let values = row
                .iter()
                .zip(target_columns.iter())
                .map(|(literal, column)| {
                    crate::engine::iceberg_writer::literal_to_sql_for_arrow_type(
                        literal,
                        &column.data_type,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?
                .join(", ");
            Ok(format!("({values})"))
        })
        .collect::<Result<Vec<_>, String>>()?;
    let value_columns = target_columns
        .iter()
        .map(|column| sql_identifier(&column.name))
        .collect::<Vec<_>>()
        .join(", ");
    let values_sql = format!(
        "(VALUES {}) AS {}({})",
        rendered_rows.join(", "),
        sql_identifier(alias),
        value_columns
    );
    let select_items = target_columns
        .iter()
        .map(|column| {
            Ok(format!(
                "{} AS {}",
                crate::engine::iceberg_writer::target_cast_expr_sql(
                    &qualify_column(alias, &column.name),
                    column,
                )?,
                sql_identifier(&column.name)
            ))
        })
        .collect::<Result<Vec<_>, String>>()?;
    let sql = format!("SELECT {} FROM {}", select_items.join(", "), values_sql);
    parse_generated_query(&sql, "ADD EQUALITY DELETE sink")
}

fn equality_delete_target_columns(delete_columns: &[EqualityDeleteColumn]) -> Vec<ColumnDef> {
    delete_columns
        .iter()
        .map(|column| ColumnDef {
            name: column.name.clone(),
            data_type: column.data_type.clone(),
            nullable: column.nullable,
            write_default: None,
            logical_type: None,
        })
        .collect()
}

fn parse_generated_query(sql: &str, context: &str) -> Result<sqlparser::ast::Query, String> {
    match crate::sql::parser::parse_sql_raw(sql)? {
        sqlparser::ast::Statement::Query(query) => Ok(*query),
        other => Err(format!("{context}: generated non-query statement: {other}")),
    }
}

fn qualify_column(alias: &str, column: &str) -> String {
    format!("{}.{}", sql_identifier(alias), sql_identifier(column))
}

fn sql_identifier(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
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

    fn function_body<'a>(src: &'a str, name: &str) -> &'a str {
        let rest = src
            .split(&format!("fn {name}"))
            .nth(1)
            .unwrap_or_else(|| panic!("missing function {name}"));
        let end = ["\nfn ", "\nstruct ", "\nimpl ", "\n    fn "]
            .into_iter()
            .filter_map(|marker| rest.find(marker))
            .min()
            .unwrap_or(rest.len());
        &rest[..end]
    }

    #[test]
    fn equality_delete_writes_file_on_be_not_coordinator() {
        let src = include_str!("equality_delete_flow.rs");
        let body = function_body(src, "execute_add_equality_delete_statement");
        assert!(
            !body.contains("write_equality_delete_file"),
            "equality-delete must not write the file on the coordinator"
        );
        assert!(
            !body.contains("local_writer_commit_input"),
            "equality-delete must not wrap a coordinator-written file (FE central write)"
        );
        assert!(
            body.contains("run_equality_delete_distributed_transaction"),
            "equality-delete must route through the distributed equality-delete transaction"
        );
        assert!(
            !body.contains("EqualityDeletes"),
            "statement-body guard must not depend on a comment-only EqualityDeletes marker"
        );
    }

    #[test]
    fn equality_delete_distributed_transaction_uses_row_delta_equality_delete_sink() {
        let src = include_str!("equality_delete_flow.rs");
        let body = function_body(src, "run_equality_delete_distributed_transaction");
        assert!(
            body.contains("IcebergWriteSinkMode::EqualityDeletes"),
            "distributed equality-delete transaction must set the EqualityDeletes sink mode"
        );
        assert!(
            body.contains("CommitOpKind::RowDelta"),
            "distributed equality-delete transaction must commit via RowDelta"
        );
    }

    #[test]
    fn equality_delete_distributed_executor_runs_sink_without_root_shuffle() {
        let src = include_str!("equality_delete_flow.rs");
        let body = function_body(src, "run_coordinated_write");
        let compact_body = body.split_whitespace().collect::<String>();
        assert!(
            body.contains("execute_query_as_iceberg_write"),
            "distributed equality-delete executor must write through execute_query_as_iceberg_write"
        );
        assert!(
            compact_body.contains("self.sink_spec.clone(),None,None"),
            "distributed equality-delete executor must not install a root shuffle resolver"
        );
        assert!(
            !body.contains("iceberg_write_shuffle"),
            "distributed equality-delete executor must not build a shuffle plan"
        );
    }

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

    #[test]
    fn build_equality_delete_sink_query_projects_typed_values() {
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
            ])
            .build()
            .expect("schema");
        let columns = vec!["id".to_string(), "category".to_string()];
        let rows = vec![
            vec![Literal::Int(2), Literal::String("B".to_string())],
            vec![Literal::Int(4), Literal::Null],
        ];
        let (delete_columns, _) =
            super::build_equality_delete_batch(&schema, &columns, &rows).expect("batch");

        let query = super::build_equality_delete_sink_query(&delete_columns, &rows).expect("query");
        let sql = query.to_string();

        assert!(sql.contains("VALUES"));
        assert!(sql.contains("CAST"));
        assert!(sql.contains("id"));
        assert!(sql.contains("category"));
        assert!(sql.contains("NULL"));
    }
}
