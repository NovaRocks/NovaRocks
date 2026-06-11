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

//! Standalone-mode iceberg INSERT INTO / INSERT OVERWRITE entry point.
//!
//! Routes from `insert_flow::run_insert` for iceberg targets whose source is
//! handled as one transaction. `UnionAll` remains split by `insert_flow` so
//! each part gets its own operation record.
//!
//! Phase 1 scope (per spec §0.4):
//! * `INSERT INTO iceberg ... SELECT ...` — handled here.
//! * `INSERT OVERWRITE iceberg ... SELECT ...` — handled here.
//! * `INSERT INTO iceberg VALUES (...)` — handled here.
//! * `INSERT OVERWRITE iceberg VALUES (...)` — handled here.

use std::collections::BTreeMap;
use std::sync::Arc;

use iceberg::Catalog;
use iceberg::spec::DataFile;
use iceberg::{NamespaceIdent, TableIdent};

use crate::connector::backend::ResolvedTable;
use crate::connector::iceberg::catalog::backend::iceberg_schema_def_for_codegen;
use crate::connector::iceberg::catalog::registry::{
    IcebergCatalogEntry, block_on_iceberg, build_iceberg_catalog,
};
use crate::connector::iceberg::commit::{
    CleanupPathMapper, CommitOpKind, CommitOutcome, CommitServiceError, IcebergCommitCollector,
    WrittenFile, ensure_iceberg_write_supported, ensure_no_equality_deletes,
    ensure_no_variant_columns_for_row_level_mutation, ensure_overwrite_single_partition_spec,
};
use crate::connector::starrocks::table::mv_refresh::query_result_to_chunks;
use crate::engine::backend_resolver::TargetBackend;
use crate::engine::write_transaction::{
    IcebergWriteCommitExecutor, IcebergWriteCommitPolicy, IcebergWriteSource,
    IcebergWriteTransactionExecutor, IcebergWriteTransactionRunner, IcebergWriteTransactionSpec,
    IcebergWriteValidationPolicy,
};
use crate::engine::{StandaloneState, StatementResult};
use crate::exec::chunk::Chunk;
use crate::meta::repository::iceberg_operation::{IcebergOperationKind, IcebergOperationTarget};
use crate::runtime::coordinator::CoordinatedQueryResult;
use crate::runtime::write_coordinator::WriteCommitInput;
use crate::sql::catalog::{
    ColumnDef, IcebergDataFileBinding, IcebergTableInfo, ScanSource, TableDef,
};
use crate::sql::codegen::iceberg_write_sink::{
    IcebergWriteSinkSpec, synthetic_iceberg_write_table_id,
};
use crate::sql::parser::ast::{InsertSource, Literal, SqlType};

pub(crate) fn execute_iceberg_insert_or_overwrite(
    state: &Arc<StandaloneState>,
    target: &TargetBackend,
    resolved: &ResolvedTable,
    insert_columns: &[String],
    source: &InsertSource,
    overwrite_mode: crate::sql::parser::ast::OverwriteMode,
    target_ref: &str,
) -> Result<StatementResult, String> {
    use crate::sql::parser::ast::OverwriteMode;
    debug_assert_eq!(target.backend_name, "iceberg");

    let overwrite_full_table = matches!(overwrite_mode, OverwriteMode::FullTable);
    let overwrite_partitions = matches!(overwrite_mode, OverwriteMode::DynamicPartitions);

    // Reject UNION ALL on this path; caller enforces this for branch writes,
    // and OVERWRITE with this source is never valid.
    if matches!(source, InsertSource::UnionAll(_)) {
        return Err(
            "iceberg INSERT/OVERWRITE does not support UNION ALL sources on this path".to_string(),
        );
    }

    // 1. Resolve catalog entry + build iceberg-rust Catalog handle.
    let entry = {
        let registry = state
            .iceberg_catalogs
            .read()
            .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
        registry.get(&target.catalog)?
    };
    let catalog: Arc<dyn Catalog> = build_iceberg_catalog(&entry)?;
    let table_ident = TableIdent::new(
        NamespaceIdent::new(target.namespace.clone()),
        target.table.clone(),
    );
    let table =
        block_on_iceberg(async { catalog.load_table(&table_ident).await })?.map_err(|e| {
            format!(
                "load iceberg table {target_str}: {e}",
                target_str = target_string(target)
            )
        })?;

    // 2. Pre-lowering validators.
    let _write_mode = ensure_iceberg_write_supported(&table)?;
    if overwrite_full_table {
        ensure_no_variant_columns_for_row_level_mutation(&table)
            .map_err(|e| format!("INSERT OVERWRITE: {e}"))?;
        ensure_overwrite_single_partition_spec(&table)?;
        ensure_no_equality_deletes(&table)?;
    }
    if overwrite_partitions {
        // OVERWRITE PARTITIONS shares the variant-write restriction with
        // full-table OVERWRITE (#87 spec). Then check the partition-table
        // requirement; v3 row-lineage + cross-historical-spec checks happen
        // in OverwritePartitionsCommit.
        ensure_no_variant_columns_for_row_level_mutation(&table)
            .map_err(|e| format!("INSERT OVERWRITE PARTITIONS: {e}"))?;
        if table.metadata().default_partition_spec().is_unpartitioned() {
            return Err(format!(
                "INSERT OVERWRITE PARTITIONS requires a partitioned table; \
                 table {} is unpartitioned (use OVERWRITE without PARTITIONS)",
                target_string(target),
            ));
        }
    }
    // Branch writes require Iceberg v3 (row-lineage semantics).
    if target_ref != "main" {
        let fmt = table.metadata().format_version();
        if fmt != iceberg::spec::FormatVersion::V3 {
            return Err(format!(
                "iceberg ref: branch writes require Iceberg v3 tables (table {} is v{})",
                target_string(target),
                fmt as u8,
            ));
        }
    }

    execute_iceberg_insert_distributed(
        state,
        target,
        resolved,
        insert_columns,
        source,
        overwrite_mode,
        target_ref,
        catalog,
        table,
        &entry,
        table_ident,
    )
}

#[allow(clippy::too_many_arguments)]
fn execute_iceberg_insert_distributed(
    state: &Arc<StandaloneState>,
    target: &TargetBackend,
    resolved: &ResolvedTable,
    insert_columns: &[String],
    source: &InsertSource,
    overwrite_mode: crate::sql::parser::ast::OverwriteMode,
    target_ref: &str,
    catalog: Arc<dyn Catalog>,
    table: iceberg::table::Table,
    entry: &IcebergCatalogEntry,
    table_ident: TableIdent,
) -> Result<StatementResult, String> {
    let query = append_source_to_query(source, insert_columns, &resolved.columns)?;
    let sink_spec = build_insert_write_sink_spec(target, resolved, &table, entry)?;

    let metadata = table.metadata();
    let commit_op_kind = commit_op_kind_for_overwrite_mode(overwrite_mode);
    let base_snapshot_id = write_base_snapshot_id(metadata, target_ref)?;
    let base_sequence_number = metadata.last_sequence_number();
    let current_schema = metadata.current_schema().clone();
    let default_partition_spec = metadata.default_partition_spec().clone();
    let staging_dir = format!(
        "{}/data/_staging/{}",
        metadata.location(),
        uuid::Uuid::new_v4()
    );
    let collector = Arc::new(IcebergCommitCollector::new(
        commit_op_kind,
        table_ident,
        base_snapshot_id,
        base_sequence_number,
        current_schema,
        default_partition_spec,
        staging_dir,
        crate::common::types::UniqueId { hi: 0, lo: 0 },
    ));

    let abort_cleanup = build_abort_cleanup_for_catalog_entry(entry)?;
    let commit_executor = IcebergWriteCommitExecutor {
        state: Arc::clone(state),
        target: target.clone(),
        catalog,
        table,
        collector,
        fs: abort_cleanup.fs,
        cleanup_path_mapper: abort_cleanup.path_mapper,
        cow_update_rewrite: None,
        target_ref: target_ref.to_string(),
        snapshot_properties: BTreeMap::new(),
    };
    let executor = DistributedInsertWriteExecutor {
        state: Arc::clone(state),
        target: target.clone(),
        query,
        sink_spec,
        commit_executor,
    };
    let spec = IcebergWriteTransactionSpec {
        target: IcebergOperationTarget {
            catalog: target.catalog.clone(),
            namespace: target.namespace.clone(),
            table: target.table.clone(),
            ref_name: (target_ref != "main").then(|| target_ref.to_string()),
        },
        operation_kind: operation_kind_for_commit_op_kind(commit_op_kind),
        attempt_id: format!("{}:{}", target_string(target), uuid::Uuid::new_v4()),
        commit: IcebergWriteCommitPolicy {
            commit_op_kind,
            base_snapshot_id,
            base_snapshot_map: BTreeMap::new(),
            target_ref: target_ref.to_string(),
            snapshot_properties: BTreeMap::new(),
        },
        validation: IcebergWriteValidationPolicy {
            require_v3_for_branch: target_ref != "main",
        },
        source: IcebergWriteSource::CoordinatedPlan,
    };
    let runner = IcebergWriteTransactionRunner::new(Arc::clone(state), &executor);
    let _outcome = runner.run(spec)?;

    Ok(StatementResult::Ok)
}

struct DistributedInsertWriteExecutor {
    state: Arc<StandaloneState>,
    target: TargetBackend,
    query: sqlparser::ast::Query,
    sink_spec: IcebergWriteSinkSpec,
    commit_executor: IcebergWriteCommitExecutor,
}

impl IcebergWriteTransactionExecutor for DistributedInsertWriteExecutor {
    fn run_coordinated_write(
        &self,
        _spec: &IcebergWriteTransactionSpec,
    ) -> Result<CoordinatedQueryResult, String> {
        crate::engine::execute_query_as_iceberg_write(
            &self.state,
            Some(&self.target.catalog),
            &self.target.namespace,
            &self.query,
            self.sink_spec.clone(),
            None,
        )
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

fn build_insert_write_sink_spec(
    target: &TargetBackend,
    resolved: &ResolvedTable,
    table: &iceberg::table::Table,
    entry: &IcebergCatalogEntry,
) -> Result<IcebergWriteSinkSpec, String> {
    let metadata = table.metadata();
    let iceberg = IcebergTableInfo {
        catalog: target.catalog.clone(),
        namespace: target.namespace.clone(),
        table: target.table.clone(),
        table_uuid: Some(metadata.uuid().to_string()),
        current_snapshot_id: metadata.current_snapshot_id(),
        schema_id: metadata.current_schema_id(),
        location: metadata.location().to_string(),
        schema: iceberg_schema_def_for_codegen(metadata.current_schema()),
        serialized_metadata: Some(
            serde_json::to_string(metadata)
                .map_err(|err| format!("serialize iceberg table metadata failed: {err}"))?,
        ),
        serialized_metadata_rows: None,
    };
    let cloud_properties = entry.cloud_properties_map();
    let target_table = TableDef {
        name: resolved.table.clone(),
        columns: resolved.columns.clone(),
        iceberg_row_lineage_metadata_columns: Vec::new(),
        source: ScanSource::IcebergDataFiles {
            table: iceberg.clone(),
            files: Vec::new(),
            cloud_properties: cloud_properties.clone(),
            binding: IcebergDataFileBinding::CurrentSnapshot,
        },
    };
    let table_location = metadata.location().to_string();
    let data_location = metadata
        .properties()
        .get("write.data.path")
        .cloned()
        .unwrap_or_else(|| format!("{}/data", table_location.trim_end_matches('/')));
    let cloud_configuration = (!cloud_properties.is_empty()).then(|| {
        crate::cloud_configuration::TCloudConfiguration::new(
            None::<crate::cloud_configuration::TCloudType>,
            None::<Vec<crate::cloud_configuration::TCloudProperty>>,
            Some(cloud_properties),
            None::<bool>,
        )
    });

    Ok(IcebergWriteSinkSpec {
        target_table_id: synthetic_iceberg_write_table_id(),
        target_table,
        iceberg,
        target_columns: resolved.columns.clone(),
        table_location,
        data_location,
        target_partition_spec_id: metadata.default_partition_spec_id(),
        cloud_configuration,
        file_format: "parquet".to_string(),
        compression: crate::types::TCompressionType::SNAPPY,
    })
}

fn append_source_to_query(
    source: &InsertSource,
    insert_columns: &[String],
    target_columns: &[ColumnDef],
) -> Result<sqlparser::ast::Query, String> {
    match source {
        InsertSource::FromQuery(query) if insert_columns.is_empty() => Ok((**query).clone()),
        InsertSource::FromQuery(query) => {
            wrap_insert_query_with_target_projection(query, insert_columns, target_columns)
        }
        InsertSource::Values(rows) => {
            values_append_source_to_query(rows, insert_columns, target_columns)
        }
        InsertSource::SelectLiteralRow(row) => {
            values_append_source_to_query(std::slice::from_ref(row), insert_columns, target_columns)
        }
        InsertSource::UnionAll(_) => {
            Err("iceberg INSERT append does not support UNION ALL sources on this path".to_string())
        }
    }
}

fn wrap_insert_query_with_target_projection(
    query: &sqlparser::ast::Query,
    insert_columns: &[String],
    target_columns: &[ColumnDef],
) -> Result<sqlparser::ast::Query, String> {
    let insert_idx_by_target = insert_column_index_by_target_name(insert_columns, target_columns)?;
    let source_alias = "__nr_insert_src";
    let mut projection = Vec::with_capacity(target_columns.len());
    for column in target_columns {
        let target_name = crate::engine::catalog::normalize_identifier(&column.name)?;
        let expr = if let Some(source_idx) = insert_idx_by_target.get(&target_name) {
            let source_expr = format!(
                "{}.{}",
                sql_identifier(source_alias),
                sql_identifier(&insert_columns[*source_idx])
            );
            target_cast_expr_sql(&source_expr, column)?
        } else {
            target_cast_expr_sql(&omitted_column_expr_sql(column)?, column)?
        };
        projection.push(format!("{expr} AS {}", sql_identifier(&column.name)));
    }
    let alias_columns = insert_columns
        .iter()
        .map(|column| sql_identifier(column))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT {} FROM ({}) AS {} ({})",
        projection.join(", "),
        query,
        sql_identifier(source_alias),
        alias_columns
    );
    parse_generated_query(&sql, "append INSERT SELECT projection")
}

fn values_append_source_to_query(
    rows: &[Vec<Literal>],
    insert_columns: &[String],
    target_columns: &[ColumnDef],
) -> Result<sqlparser::ast::Query, String> {
    let rows = if insert_columns.is_empty() {
        for row in rows {
            if row.len() != target_columns.len() {
                return Err(format!(
                    "insert column count mismatch: expected {} values, got {}",
                    target_columns.len(),
                    row.len()
                ));
            }
        }
        rows.to_vec()
    } else {
        crate::engine::insert::reorder_insert_rows(rows, insert_columns, target_columns)?
    };
    let rendered_rows = rows
        .iter()
        .map(|row| {
            let values = row
                .iter()
                .zip(target_columns.iter())
                .map(|(literal, column)| target_literal_expr_sql(literal, column))
                .collect::<Result<Vec<_>, _>>()?
                .join(", ");
            Ok(format!("({values})"))
        })
        .collect::<Result<Vec<_>, String>>()?;
    let sql = format!("VALUES {}", rendered_rows.join(", "));
    parse_generated_query(&sql, "append INSERT VALUES")
}

fn insert_column_index_by_target_name(
    insert_columns: &[String],
    target_columns: &[ColumnDef],
) -> Result<std::collections::HashMap<String, usize>, String> {
    let mut target_names = std::collections::HashSet::with_capacity(target_columns.len());
    for column in target_columns {
        target_names.insert(crate::engine::catalog::normalize_identifier(&column.name)?);
    }

    let mut mapping = std::collections::HashMap::with_capacity(insert_columns.len());
    for (idx, column) in insert_columns.iter().enumerate() {
        let normalized = crate::engine::catalog::normalize_identifier(column)?;
        if !target_names.contains(&normalized) {
            return Err(format!("unknown INSERT column `{column}`"));
        }
        if mapping.insert(normalized.clone(), idx).is_some() {
            return Err(format!("duplicate INSERT column `{column}`"));
        }
    }
    Ok(mapping)
}

fn omitted_column_expr_sql(column: &ColumnDef) -> Result<String, String> {
    let Some(write_default) = &column.write_default else {
        return Ok("NULL".to_string());
    };
    let sql_type = arrow_data_type_to_sql_type(&column.data_type)?;
    let literal =
        crate::connector::iceberg::default_value::iceberg_literal_to_ast(write_default, &sql_type)?;
    literal_to_sql_for_arrow_type(&literal, &column.data_type)
}

fn target_literal_expr_sql(literal: &Literal, column: &ColumnDef) -> Result<String, String> {
    target_cast_expr_sql(
        &literal_to_sql_for_arrow_type(literal, &column.data_type)?,
        column,
    )
}

fn target_cast_expr_sql(expr_sql: &str, column: &ColumnDef) -> Result<String, String> {
    Ok(format!(
        "CAST({expr_sql} AS {})",
        arrow_data_type_to_sql_type_name(&column.data_type)?
    ))
}

fn parse_generated_query(sql: &str, context: &str) -> Result<sqlparser::ast::Query, String> {
    match crate::sql::parser::parse_sql_raw(sql)? {
        sqlparser::ast::Statement::Query(query) => Ok(*query),
        other => Err(format!("{context}: generated non-query statement: {other}")),
    }
}

fn sql_identifier(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

fn literal_to_sql(literal: &Literal) -> Result<String, String> {
    Ok(match literal {
        Literal::Null => "NULL".to_string(),
        Literal::Bool(value) => {
            if *value {
                "TRUE".to_string()
            } else {
                "FALSE".to_string()
            }
        }
        Literal::Int(value) => value.to_string(),
        Literal::Float(value) => {
            if !value.is_finite() {
                return Err(format!(
                    "non-finite floating literal is not supported: {value}"
                ));
            }
            value.to_string()
        }
        Literal::String(value) | Literal::Date(value) => single_quoted_sql(value),
        Literal::Array(items) => format!(
            "[{}]",
            items
                .iter()
                .map(literal_to_sql)
                .collect::<Result<Vec<_>, _>>()?
                .join(", ")
        ),
        Literal::Map(entries) => {
            let mut args = Vec::with_capacity(entries.len() * 2);
            for (key, value) in entries {
                args.push(literal_to_sql(key)?);
                args.push(literal_to_sql(value)?);
            }
            format!("map({})", args.join(", "))
        }
        Literal::Struct(values) => format!(
            "row({})",
            values
                .iter()
                .map(literal_to_sql)
                .collect::<Result<Vec<_>, _>>()?
                .join(", ")
        ),
    })
}

fn literal_to_sql_for_arrow_type(
    literal: &Literal,
    data_type: &arrow::datatypes::DataType,
) -> Result<String, String> {
    use arrow::datatypes::DataType;

    match (literal, data_type) {
        (
            Literal::String(value) | Literal::Date(value),
            DataType::Binary | DataType::LargeBinary,
        ) => {
            let bytes = crate::engine::sql_expr::latin1_string_to_bytes(value)?;
            Ok(format!("X'{}'", hex::encode_upper(bytes)))
        }
        (Literal::Array(items), DataType::List(item_field)) => {
            let values = items
                .iter()
                .map(|item| literal_to_sql_for_arrow_type(item, item_field.data_type()))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(format!("[{}]", values.join(", ")))
        }
        (Literal::Map(entries), DataType::Map(entries_field, _)) => {
            let DataType::Struct(fields) = entries_field.data_type() else {
                return literal_to_sql(literal);
            };
            if fields.len() != 2 {
                return literal_to_sql(literal);
            }
            let mut args = Vec::with_capacity(entries.len() * 2);
            for (key, value) in entries {
                args.push(literal_to_sql_for_arrow_type(key, fields[0].data_type())?);
                args.push(literal_to_sql_for_arrow_type(value, fields[1].data_type())?);
            }
            Ok(format!("map({})", args.join(", ")))
        }
        (Literal::Struct(values), DataType::Struct(fields)) if values.len() == fields.len() => {
            let values = values
                .iter()
                .zip(fields.iter())
                .map(|(value, field)| literal_to_sql_for_arrow_type(value, field.data_type()))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(format!("row({})", values.join(", ")))
        }
        _ => literal_to_sql(literal),
    }
}

fn single_quoted_sql(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len() + 2);
    for ch in value.chars() {
        match ch {
            '\'' => escaped.push_str("''"),
            '\\' => escaped.push_str(r"\\"),
            _ => escaped.push(ch),
        }
    }
    format!("'{escaped}'")
}

fn arrow_data_type_to_sql_type(dt: &arrow::datatypes::DataType) -> Result<SqlType, String> {
    use arrow::datatypes::{DataType, TimeUnit};
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
        DataType::Utf8 | DataType::LargeUtf8 => SqlType::String,
        DataType::Date32 => SqlType::Date,
        DataType::Timestamp(TimeUnit::Nanosecond, _) => SqlType::DateTimeNs,
        DataType::Timestamp(TimeUnit::Microsecond, _) => SqlType::DateTime,
        DataType::Time64(TimeUnit::Microsecond | TimeUnit::Nanosecond) => SqlType::Time,
        DataType::Binary | DataType::LargeBinary => SqlType::Binary,
        DataType::List(element_field) => SqlType::Array(Box::new(arrow_data_type_to_sql_type(
            element_field.data_type(),
        )?)),
        DataType::Map(entries_field, _) => {
            let DataType::Struct(fields) = entries_field.data_type() else {
                return Err(format!("unsupported Arrow map entries type: {dt:?}"));
            };
            if fields.len() != 2 {
                return Err(format!("unsupported Arrow map entries field count: {dt:?}"));
            }
            SqlType::Map(
                Box::new(arrow_data_type_to_sql_type(fields[0].data_type())?),
                Box::new(arrow_data_type_to_sql_type(fields[1].data_type())?),
            )
        }
        DataType::Struct(fields) => SqlType::Struct(
            fields
                .iter()
                .map(|field| {
                    Ok((
                        field.name().clone(),
                        arrow_data_type_to_sql_type(field.data_type())?,
                    ))
                })
                .collect::<Result<Vec<_>, String>>()?,
        ),
        other => {
            return Err(format!(
                "unsupported Arrow type for INSERT default conversion: {other:?}"
            ));
        }
    })
}

fn arrow_data_type_to_sql_type_name(dt: &arrow::datatypes::DataType) -> Result<String, String> {
    sql_type_name(&arrow_data_type_to_sql_type(dt)?)
}

fn sql_type_name(sql_type: &SqlType) -> Result<String, String> {
    Ok(match sql_type {
        SqlType::TinyInt => "TINYINT".to_string(),
        SqlType::SmallInt => "SMALLINT".to_string(),
        SqlType::Int => "INT".to_string(),
        SqlType::BigInt => "BIGINT".to_string(),
        SqlType::LargeInt => "LARGEINT".to_string(),
        SqlType::Float => "FLOAT".to_string(),
        SqlType::Double => "DOUBLE".to_string(),
        SqlType::Decimal { precision, scale } => format!("DECIMAL({precision}, {scale})"),
        SqlType::String => "STRING".to_string(),
        SqlType::Json => "JSON".to_string(),
        SqlType::Binary => "BINARY".to_string(),
        SqlType::Bitmap => "BITMAP".to_string(),
        SqlType::Hll => "HLL".to_string(),
        SqlType::Boolean => "BOOLEAN".to_string(),
        SqlType::Date => "DATE".to_string(),
        SqlType::DateTime => "DATETIME".to_string(),
        SqlType::DateTimeNs => "DATETIME_NS".to_string(),
        SqlType::Time => "TIME".to_string(),
        SqlType::Array(inner) => format!("ARRAY<{}>", sql_type_name(inner)?),
        SqlType::Map(key, value) => {
            format!("MAP<{}, {}>", sql_type_name(key)?, sql_type_name(value)?)
        }
        SqlType::Struct(fields) => format!(
            "STRUCT<{}>",
            fields
                .iter()
                .map(|(name, ty)| Ok(format!("{} {}", sql_identifier(name), sql_type_name(ty)?)))
                .collect::<Result<Vec<_>, String>>()?
                .join(", ")
        ),
        SqlType::Variant => "VARIANT".to_string(),
    })
}

fn commit_op_kind_for_overwrite_mode(
    overwrite_mode: crate::sql::parser::ast::OverwriteMode,
) -> CommitOpKind {
    use crate::sql::parser::ast::OverwriteMode;
    match overwrite_mode {
        OverwriteMode::DynamicPartitions => CommitOpKind::OverwritePartitions,
        OverwriteMode::FullTable => CommitOpKind::Overwrite,
        OverwriteMode::None => CommitOpKind::FastAppend,
    }
}

fn operation_kind_for_commit_op_kind(kind: CommitOpKind) -> IcebergOperationKind {
    match kind {
        CommitOpKind::FastAppend => IcebergOperationKind::InsertAppend,
        CommitOpKind::Overwrite | CommitOpKind::OverwritePartitions => {
            IcebergOperationKind::InsertOverwrite
        }
        _ => IcebergOperationKind::Maintenance,
    }
}

fn write_base_snapshot_id(
    metadata: &iceberg::spec::TableMetadata,
    target_ref: &str,
) -> Result<Option<i64>, String> {
    if target_ref == "main" {
        return Ok(metadata.current_snapshot().map(|s| s.snapshot_id()));
    }
    metadata
        .refs()
        .get(target_ref)
        .map(|snapshot_ref| Some(snapshot_ref.snapshot_id))
        .ok_or_else(|| format!("iceberg ref: branch '{target_ref}' not found in table metadata"))
}

pub(crate) fn invalidate_iceberg_caches(
    state: &Arc<StandaloneState>,
    target: &TargetBackend,
) -> Result<(), String> {
    {
        let registry = state
            .iceberg_catalogs
            .read()
            .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
        let entry = registry.get(&target.catalog)?;
        entry.invalidate_table_cache(&target.namespace, &target.table);
    }
    crate::engine::query_prep::invalidate_catalog_mgr_table(
        state,
        &target.catalog,
        &target.namespace,
        &target.table,
    )
}

fn target_string(t: &TargetBackend) -> String {
    format!("{}.{}.{}", t.catalog, t.namespace, t.table)
}

pub(crate) fn data_file_to_written_file(
    df: &DataFile,
    partition_spec_id: i32,
) -> Result<WrittenFile, String> {
    Ok(WrittenFile {
        path: df.file_path().to_string(),
        format: df.file_format(),
        content: df.content_type(),
        partition_values: df.partition().clone(),
        partition_spec_id,
        record_count: df.record_count(),
        file_size_in_bytes: df.file_size_in_bytes(),
        split_offsets: df.split_offsets().map(|s| s.to_vec()).unwrap_or_default(),
        column_sizes: df.column_sizes().clone(),
        value_counts: df.value_counts().clone(),
        null_value_counts: df.null_value_counts().clone(),
        lower_bounds: df.lower_bounds().clone(),
        upper_bounds: df.upper_bounds().clone(),
        key_metadata: df.key_metadata().map(|s| s.to_vec()),
        referenced_data_file: df.referenced_data_file().map(|s| s.to_string()),
        equality_ids: df.equality_ids(),
        first_row_id: df.first_row_id(),
    })
}

pub(crate) fn run_select_to_chunks(
    state: &Arc<StandaloneState>,
    target: &TargetBackend,
    query: &sqlparser::ast::Query,
) -> Result<Vec<Chunk>, String> {
    // Pass `current_catalog` when the target is an iceberg table so that
    // 1-part and 2-part table references in the SELECT (e.g. `db.table`)
    // resolve against the active catalog.
    let current_catalog = if target.backend_name == "iceberg" && !target.catalog.is_empty() {
        Some(target.catalog.as_str())
    } else {
        None
    };

    let result = crate::engine::execute_query_with_catalog_mgr(
        state,
        current_catalog,
        &target.namespace,
        query,
        None,
    )?;
    query_result_to_chunks(result)
}

/// Like [`run_select_to_chunks`], but also returns the output schema columns
/// from the query plan. The schema is always populated even when the SELECT
/// produces zero rows — callers that need the column types for schema inference
/// (e.g. CTAS) should use this instead of `run_select_to_chunks`.
pub(crate) fn run_select_to_chunks_and_schema(
    state: &Arc<StandaloneState>,
    target: &TargetBackend,
    query: &sqlparser::ast::Query,
) -> Result<
    (
        Vec<Chunk>,
        Vec<crate::runtime::query_result::QueryResultColumn>,
    ),
    String,
> {
    // CTAS context: SELECT may reference iceberg tables (1-part or 2-part
    // names). Passing Some(target.catalog) routes unqualified refs to iceberg,
    // mirroring the standalone server's SELECT path.
    let current_catalog = if target.backend_name == "iceberg" && !target.catalog.is_empty() {
        Some(target.catalog.as_str())
    } else {
        None
    };
    let result = crate::engine::execute_query_with_catalog_mgr(
        state,
        current_catalog,
        &target.namespace,
        query,
        None,
    )?;
    let schema_cols = result.columns.clone();
    let chunks = query_result_to_chunks(result)?;
    Ok((chunks, schema_cols))
}

pub(crate) struct AbortCleanupOperator {
    pub(crate) fs: opendal::Operator,
    pub(crate) path_mapper: Option<CleanupPathMapper>,
}

pub(crate) fn build_abort_cleanup_for_catalog_entry(
    entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
) -> Result<AbortCleanupOperator, String> {
    if let Some(s3_config) = entry.object_store_config() {
        let fs = crate::fs::object_store::build_oss_operator(s3_config)
            .map_err(|e| format!("build S3 operator for iceberg abort cleanup: {e}"))?;
        let bucket = s3_config.bucket.clone();
        let mapper: CleanupPathMapper = Arc::new(move |path| {
            crate::connector::iceberg::catalog::add_files::parse_s3_path(path)
                .ok()
                .and_then(|(actual_bucket, key)| {
                    if actual_bucket == bucket {
                        Some(key)
                    } else {
                        None
                    }
                })
                .unwrap_or_else(|| path.to_string())
        });
        return Ok(AbortCleanupOperator {
            fs,
            path_mapper: Some(mapper),
        });
    }

    let builder = opendal::services::Fs::default().root("/");
    let fs = opendal::Operator::new(builder)
        .map_err(|e| format!("build local-FS operator failed: {e}"))?
        .finish();
    let mapper: CleanupPathMapper =
        Arc::new(|path: &str| path.strip_prefix("file://").unwrap_or(path).to_string());
    Ok(AbortCleanupOperator {
        fs,
        path_mapper: Some(mapper),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Fields, TimeUnit};
    use sqlparser::ast as sqlast;

    fn test_column(
        name: &str,
        data_type: DataType,
        write_default: Option<iceberg::spec::Literal>,
    ) -> crate::sql::catalog::ColumnDef {
        crate::sql::catalog::ColumnDef {
            name: name.to_string(),
            data_type,
            nullable: true,
            write_default,
            logical_type: None,
        }
    }

    fn parse_query(sql: &str) -> sqlast::Query {
        let stmt = crate::sql::parser::parse_sql_raw(sql).expect("parse query");
        let sqlast::Statement::Query(query) = stmt else {
            panic!("expected query statement");
        };
        *query
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

    fn test_struct_type(fields: Vec<(&str, DataType)>) -> DataType {
        DataType::Struct(Fields::from(
            fields
                .into_iter()
                .map(|(name, data_type)| Arc::new(Field::new(name, data_type, true)))
                .collect::<Vec<_>>(),
        ))
    }

    #[test]
    fn arrow_data_type_to_sql_type_accepts_time64_for_insert_defaults() {
        assert_eq!(
            arrow_data_type_to_sql_type(&DataType::Time64(TimeUnit::Microsecond)).expect("type"),
            crate::sql::parser::ast::SqlType::Time
        );
    }

    #[test]
    fn overwrite_path_uses_distributed_writer_not_local_collect() {
        let source = include_str!("iceberg_writer.rs");
        let entrypoint = source
            .split("pub(crate) fn execute_iceberg_insert_or_overwrite")
            .nth(1)
            .expect("insert/overwrite entrypoint must exist")
            .split("#[allow(clippy::too_many_arguments)]")
            .next()
            .expect("entrypoint source section");

        assert!(
            entrypoint.contains("execute_iceberg_insert_distributed"),
            "INSERT OVERWRITE must call the distributed iceberg sink path"
        );
        assert!(
            !entrypoint.contains("run_select_to_chunks"),
            "INSERT OVERWRITE must not collect SELECT output in the coordinator"
        );
        assert!(
            !entrypoint.contains("InsertOrOverwriteWriteExecutor"),
            "INSERT OVERWRITE must not use the local file writer executor"
        );
        assert!(
            !entrypoint.contains("synthetic_write_commit_input"),
            "INSERT OVERWRITE must not synthesize writer output"
        );
    }

    #[test]
    fn append_executor_does_not_use_synthetic_commit_input() {
        let source = include_str!("iceberg_writer.rs");
        let impl_source = source
            .split("impl IcebergWriteTransactionExecutor for DistributedInsertWriteExecutor")
            .nth(1)
            .expect("distributed append executor impl must exist")
            .split("fn build_insert_write_sink_spec")
            .next()
            .expect("append executor source section");

        assert!(
            impl_source.contains("execute_query_as_iceberg_write"),
            "append executor must use the distributed iceberg write path"
        );
        assert!(
            !impl_source.contains("synthetic_write_commit_input"),
            "append executor must not return a synthetic write commit"
        );
    }

    #[test]
    fn append_source_to_query_values_reorders_columns_and_fills_defaults() {
        let target_columns = vec![
            test_column("a", DataType::Int32, None),
            test_column(
                "b",
                DataType::Int32,
                Some(iceberg::spec::Literal::Primitive(
                    iceberg::spec::PrimitiveLiteral::Int(5),
                )),
            ),
            test_column("c", DataType::Int32, None),
        ];
        let source = InsertSource::Values(vec![vec![
            crate::sql::parser::ast::Literal::Int(30),
            crate::sql::parser::ast::Literal::Int(10),
        ]]);

        let query = append_source_to_query(
            &source,
            &["c".to_string(), "a".to_string()],
            &target_columns,
        )
        .expect("append source query");

        let sqlast::SetExpr::Values(values) = query.body.as_ref() else {
            panic!("expected VALUES query, got: {query}");
        };
        let row = values.rows.first().expect("one row");
        let rendered: Vec<String> = row.iter().map(ToString::to_string).collect();
        assert_eq!(
            rendered,
            vec!["CAST(10 AS INT)", "CAST(5 AS INT)", "CAST(30 AS INT)"]
        );
    }

    #[test]
    fn append_source_to_query_values_casts_literals_to_target_types() {
        let target_columns = vec![
            test_column("id", DataType::Int64, None),
            test_column("region", DataType::Utf8, None),
            test_column("amount", DataType::Float64, None),
        ];
        let source = InsertSource::Values(vec![
            vec![
                crate::sql::parser::ast::Literal::Int(1),
                crate::sql::parser::ast::Literal::String("us".to_string()),
                crate::sql::parser::ast::Literal::Float(10.5),
            ],
            vec![
                crate::sql::parser::ast::Literal::Int(2),
                crate::sql::parser::ast::Literal::String("eu".to_string()),
                crate::sql::parser::ast::Literal::Float(20.0),
            ],
        ]);

        let query =
            append_source_to_query(&source, &[], &target_columns).expect("append source query");

        let sqlast::SetExpr::Values(values) = query.body.as_ref() else {
            panic!("expected VALUES query, got: {query}");
        };
        let first_row: Vec<String> = values.rows[0].iter().map(ToString::to_string).collect();
        let second_row: Vec<String> = values.rows[1].iter().map(ToString::to_string).collect();
        assert_eq!(
            first_row,
            vec![
                "CAST(1 AS BIGINT)",
                "CAST('us' AS STRING)",
                "CAST(10.5 AS DOUBLE)"
            ]
        );
        assert_eq!(
            second_row,
            vec![
                "CAST(2 AS BIGINT)",
                "CAST('eu' AS STRING)",
                "CAST(20 AS DOUBLE)"
            ]
        );
    }

    #[test]
    fn append_source_to_query_values_preserves_backslash_string_literals() {
        let target_columns = vec![test_column("region", DataType::Utf8, None)];
        let source = InsertSource::Values(vec![vec![crate::sql::parser::ast::Literal::String(
            r"e\f".to_string(),
        )]]);

        let query =
            append_source_to_query(&source, &[], &target_columns).expect("append source query");

        let sqlast::SetExpr::Values(values) = query.body.as_ref() else {
            panic!("expected VALUES query, got: {query}");
        };
        let sqlast::Expr::Cast { expr, .. } = &values.rows[0][0] else {
            panic!("expected CAST expression");
        };
        let sqlast::Expr::Value(value) = expr.as_ref() else {
            panic!("expected string literal inside CAST");
        };
        let sqlast::Value::SingleQuotedString(s) = &value.value else {
            panic!("expected single-quoted string");
        };
        assert_eq!(s, r"e\f");
    }

    #[test]
    fn append_source_to_query_values_renders_binary_literals_as_hex() {
        let target_columns = vec![test_column("payload", DataType::Binary, None)];
        let packed = crate::engine::sql_expr::bytes_to_latin1_string(&[0xab, 0x01]);
        let source =
            InsertSource::Values(vec![vec![crate::sql::parser::ast::Literal::String(packed)]]);

        let query =
            append_source_to_query(&source, &[], &target_columns).expect("append source query");

        let sqlast::SetExpr::Values(values) = query.body.as_ref() else {
            panic!("expected VALUES query, got: {query}");
        };
        let sqlast::Expr::Cast { expr, .. } = &values.rows[0][0] else {
            panic!("expected CAST expression");
        };
        let sqlast::Expr::Value(value) = expr.as_ref() else {
            panic!("expected hex literal inside CAST");
        };
        let sqlast::Value::HexStringLiteral(s) = &value.value else {
            panic!("expected hex literal");
        };
        assert_eq!(s, "AB01");
    }

    #[test]
    fn append_source_to_query_values_rejects_column_list_width_mismatch() {
        let target_columns = vec![
            test_column("a", DataType::Int32, None),
            test_column("b", DataType::Int32, None),
        ];
        let source = InsertSource::Values(vec![vec![
            crate::sql::parser::ast::Literal::Int(1),
            crate::sql::parser::ast::Literal::Int(2),
        ]]);

        let err = append_source_to_query(&source, &["a".to_string()], &target_columns)
            .expect_err("extra value must be rejected");
        assert!(
            err.contains("expected 1 values for column list, got 2"),
            "got: {err}"
        );
    }

    #[test]
    fn append_source_to_query_from_query_column_list_wraps_projection() {
        let target_columns = vec![
            test_column("a", DataType::Int32, None),
            test_column(
                "b",
                DataType::Int32,
                Some(iceberg::spec::Literal::Primitive(
                    iceberg::spec::PrimitiveLiteral::Int(7),
                )),
            ),
            test_column("c", DataType::Int32, None),
        ];
        let source = InsertSource::FromQuery(Box::new(parse_query("SELECT x, y FROM src")));

        let query = append_source_to_query(
            &source,
            &["c".to_string(), "a".to_string()],
            &target_columns,
        )
        .expect("append source query");

        let rendered = query.to_string();
        assert!(
            rendered.contains("FROM (SELECT x, y FROM src) AS `__nr_insert_src` (`c`, `a`)"),
            "derived query should carry source column aliases, got: {rendered}"
        );
        assert!(
            rendered.starts_with(
                "SELECT CAST(`__nr_insert_src`.`a` AS INT) AS `a`, CAST(7 AS INT) AS `b`, CAST(`__nr_insert_src`.`c` AS INT) AS `c`"
            ),
            "projection should target table column order, got: {rendered}"
        );
    }

    #[test]
    fn append_source_to_query_from_query_omitted_complex_columns_parse() {
        let target_columns = vec![
            test_column("k1", DataType::Int64, None),
            test_column(
                "c_map",
                test_map_type(DataType::Int32, DataType::Int32),
                None,
            ),
            test_column(
                "c_struct",
                test_struct_type(vec![("k1", DataType::Int32), ("k2", DataType::Int32)]),
                None,
            ),
        ];
        let source = InsertSource::FromQuery(Box::new(parse_query(
            "SELECT idx FROM row_util ORDER BY idx LIMIT 1000",
        )));

        let query = append_source_to_query(&source, &["k1".to_string()], &target_columns)
            .expect("append source query");
        let rendered = query.to_string();

        assert!(
            rendered.contains("CAST(NULL AS MAP"),
            "omitted map column should be cast from NULL once, got: {rendered}"
        );
        assert!(
            rendered.contains("CAST(NULL AS STRUCT"),
            "omitted struct column should be cast from NULL once, got: {rendered}"
        );
        assert!(
            !rendered.contains("CAST(CAST(NULL"),
            "omitted complex columns must not produce nested casts, got: {rendered}"
        );
    }
}
