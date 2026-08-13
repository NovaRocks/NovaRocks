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

use std::collections::HashSet;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, TimeUnit};

use crate::engine::backend_resolver::resolve_existing_table_target;
use crate::engine::delete_engine::{
    DeleteOperation, PreparedDelete, PreparedDeleteExecution, prepared_delete,
};
use crate::engine::domain::DmlExecutionKernel;
use crate::engine::query_planning::bindings::QueryTableBindingStore;
use crate::engine::query_planning::write_sink::{
    admit_prepared_connector_write_target, sql_write_plan_input_for_admitted_target,
};
use crate::engine::statement::AddEqualityDeleteStmt;
use crate::query_execution::outcome::QueryExecutionResult;
use crate::query_execution::request_context::QueryExecutionContext;
use crate::sql::literal::{parse_date_string_to_days, parse_datetime_string_to_micros};
use crate::sql::parser::ast::Literal;
use novarocks_catalog::schema::ColumnDef;
use novarocks_spi::connector::{
    ConnectorWriteAdmissionPurpose, ConnectorWriteFieldRequest, ConnectorWriteInputRequest,
    ConnectorWriteIntent, ConnectorWriteOperationId,
};

pub(crate) fn prepare_equality_delete_statement(
    state: &DmlExecutionKernel,
    stmt: &AddEqualityDeleteStmt,
    current_catalog: Option<&str>,
    current_database: &str,
    execution: &QueryExecutionContext,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<PreparedDelete, String> {
    let target =
        resolve_existing_table_target(state, &stmt.table, current_catalog, current_database)?;
    if target.backend_name != "iceberg" {
        return Err(format!(
            "ADD EQUALITY DELETE only supports iceberg backends, got `{}`",
            target.backend_name
        ));
    }
    let planning_lease = crate::connector::acquire_metadata_planning_lease(
        state.connector_control().as_ref(),
        &target.catalog,
    )?;

    // Reject a managed materialized view from neutral metadata under an exact
    // generation, the same way INSERT, TRUNCATE and ADD FILES already do. This
    // check cannot move into row-mutation admission: incremental MV refresh
    // drives its own writes through that same admission, so at that level a
    // user statement is indistinguishable from the MV machinery maintaining its
    // own target.
    crate::engine::mv::iceberg_guard::reject_if_iceberg_mv_table_with_ports(
        state.connector_control().as_ref(),
        state.mv_storage_observation().as_ref(),
        &target,
        crate::engine::mv::iceberg_guard::IcebergMvUserMutation::Delete,
    )?;

    // The three table-shape gates this entry point used to answer -- Iceberg v1,
    // an evolved partition spec, and a partitioned target -- are Iceberg facts,
    // so they now run inside `ConnectorWriteControl::prepare_write` against the
    // frozen admitted metadata (keyed on the equality-delete input shape, which
    // no other row-delta write declares). Their messages are unchanged; they now
    // arrive with the `Iceberg write admission denied:` prefix the SPI `Denied`
    // outcome adds, and they fire after column and literal validation rather
    // than before it.
    let table_metadata = crate::connector::metadata_load_connector_table_with_planning_lease(
        &planning_lease,
        connector_context.clone(),
        &target.namespace,
        &target.table,
        novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
    )?;

    let delete_columns = equality_delete_key_columns(&table_metadata, &stmt.columns, &stmt.rows)?;
    if stmt.rows.is_empty() {
        return Err("ADD EQUALITY DELETE requires at least one row".to_string());
    }
    let values_query = build_equality_delete_sink_query(&delete_columns, &stmt.rows)?;

    // The durable operation journal records the snapshot this attempt is based
    // on. It is read through the same planning lease rather than a concrete
    // table, so this entry point no longer opens an Iceberg catalog.
    let current_snapshot_id = crate::connector::metadata_read_reference_facts_with_planning_lease(
        planning_lease.clone(),
        connector_context.clone(),
        &target.namespace,
        &target.table,
    )?
    .current_snapshot_id();
    // Route non-empty input through the distributed sink transaction.
    prepare_equality_delete_distributed_write(
        state,
        &target,
        current_snapshot_id,
        &delete_columns,
        values_query,
        execution,
        connector_context,
        planning_lease,
    )
}

struct DistributedEqualityDeleteWriteExecutor {
    state: DmlExecutionKernel,
    target: crate::engine::backend_resolver::TargetBackend,
    delete_query: sqlparser::ast::Query,
    sql_write_input: crate::sql::planner::distributed::write::contract::SqlWritePlanInput,
    table_bindings: Arc<QueryTableBindingStore>,
    execution: QueryExecutionContext,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
    connector_write: crate::query_execution::contract::ConnectorWritePlanningTemplate,
}

impl PreparedDeleteExecution for DistributedEqualityDeleteWriteExecutor {
    /// Equality DELETE activates its write generation during preparation, so the
    /// authority already exists before anything is dispatched.
    fn external_fence_authority(
        &self,
    ) -> Result<
        crate::engine::external_write_fence::ExternalWriteFenceAuthority,
        novarocks_spi::connector::ConnectorError,
    > {
        crate::engine::external_write_fence::ExternalWriteFenceAuthority::try_new(
            self.connector_write.lease(),
            self.connector_write.operation_id(),
            &self.target.namespace,
            &self.target.table,
            self.connector_write.preparation().target_ref().clone(),
            self.connector_context.clone(),
        )
    }

    fn run(&self) -> Result<QueryExecutionResult, String> {
        let result = crate::engine::execute_query_as_iceberg_write_with_connector_context(
            &self.state,
            Some(&self.target.catalog),
            &self.target.namespace,
            &self.delete_query,
            self.sql_write_input.clone(),
            Arc::clone(&self.table_bindings),
            None,
            crate::sql::compiler::RootDistributionRequirement::Any,
            Some(&self.execution),
            &self.connector_context,
            Some(self.connector_write.clone()),
        )?;
        if result.write_abort.is_none() && result.connector_completion.is_none() {
            return Err(
                "ADD EQUALITY DELETE completed without a sealed connector write completion for non-empty input"
                    .to_string(),
            );
        }
        Ok(result)
    }

    fn commit_terminal(
        &self,
        completion: &crate::query_execution::ConnectorWriteCompletion,
    ) -> Result<
        novarocks_spi::connector::ExternalMutationOutcome<
            novarocks_spi::connector::ConnectorWriteReceipt,
        >,
        String,
    > {
        completion
            .session()
            .commit(self.connector_context.clone())
            .map_err(|error| error.to_string())
    }

    fn finalize(&self) -> Result<(), String> {
        self.state.catalog_service().invalidate_table(
            &self.target.catalog,
            &self.target.namespace,
            &self.target.table,
        )
    }
}

#[allow(clippy::too_many_arguments)]
fn prepare_equality_delete_distributed_write(
    state: &DmlExecutionKernel,
    target: &crate::engine::backend_resolver::TargetBackend,
    current_snapshot_id: Option<i64>,
    delete_columns: &[Field],
    values_query: sqlparser::ast::Query,
    execution: &QueryExecutionContext,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    planning_lease: novarocks_spi::connector::ConnectorControlPlanningLease,
) -> Result<PreparedDelete, String> {
    let table_bindings = Arc::new(QueryTableBindingStore::try_new()?);
    let write_lease = planning_lease
        .derive_write_lease()
        .map_err(|error| format!("derive equality-delete write lease: {error}"))?;
    let preparation = crate::engine::iceberg_writer::prepare_iceberg_connector_write(
        &write_lease,
        target,
        "main",
        ConnectorWriteIntent::RowDelta,
        ConnectorWriteInputRequest::EqualityDelete {
            equality_fields: delete_columns
                .iter()
                .map(|column| ConnectorWriteFieldRequest::new(column.clone()))
                .collect(),
        },
        ConnectorWriteAdmissionPurpose::OrdinaryDml,
        connector_context.clone(),
    )?;
    let target_binding = admit_prepared_connector_write_target(
        table_bindings.as_ref(),
        crate::sql::planner::table::SqlTableIdentity {
            catalog: target.catalog.clone(),
            namespace: target.namespace.clone(),
            table: target.table.clone(),
        },
        preparation.clone(),
        planning_lease.clone(),
    )?;
    let sql_write_input = sql_write_plan_input_for_admitted_target(
        table_bindings.as_ref(),
        target_binding,
        crate::sql::planner::distributed::write::contract::SqlWriteSinkMode::EqualityDeletes,
        crate::sql::planner::distributed::write::contract::ConnectorWriteInputBinding::RootOutputByOrdinal,
        None,
    )?;

    let connector_operation_id = ConnectorWriteOperationId::new();
    let connector_write =
        crate::query_execution::contract::ConnectorWritePlanningTemplate::activate_prepared(
            connector_operation_id,
            preparation,
            connector_context.clone(),
            write_lease,
        )
        .map_err(|error| format!("activate Provider equality-delete write: {error}"))?;
    let executor = DistributedEqualityDeleteWriteExecutor {
        state: state.clone(),
        target: target.clone(),
        delete_query: values_query,
        sql_write_input,
        table_bindings,
        execution: execution.clone(),
        connector_context: connector_context.clone(),
        connector_write,
    };
    Ok(prepared_delete(
        DeleteOperation {
            catalog: target.catalog.clone(),
            namespace: target.namespace.clone(),
            table: target.table.clone(),
            target_ref: "main".to_string(),
            attempt_id: connector_operation_id.to_string(),
            base_snapshot_id: current_snapshot_id,
        },
        Arc::new(executor),
    ))
}

fn build_equality_delete_sink_query(
    delete_columns: &[Field],
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

fn equality_delete_target_columns(delete_columns: &[Field]) -> Vec<ColumnDef> {
    delete_columns
        .iter()
        .map(|column| ColumnDef {
            name: column.name().clone(),
            data_type: column.data_type().clone(),
            nullable: column.is_nullable(),
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

/// Resolve the declared equality key columns from neutral connector metadata,
/// then validate every literal against them.
///
/// This used to build an Arrow `RecordBatch` from the same literals. Nothing
/// consumed that batch: the values reach the writer as the SQL `VALUES` list
/// [`build_equality_delete_sink_query`] renders, and the only thing the caller
/// read off the batch was `num_rows() == 0`. What the array builder did
/// contribute was literal validation, so that is all this keeps.
///
/// The column set comes from `ConnectorTableMetadata` rather than the provider's
/// Iceberg schema, the same substitution `insert_columns_from_connector_metadata`
/// makes for INSERT: `schema` is the full physical Arrow schema with hidden
/// columns marked rather than removed, and `write_target_type` is the
/// provider-signed DML write type for the columns whose write encoding differs
/// from their read encoding (ADR-0055 decision 5).
fn equality_delete_key_columns(
    table_metadata: &novarocks_spi::connector::ConnectorTableMetadata,
    column_names: &[String],
    rows: &[Vec<Literal>],
) -> Result<Vec<Field>, String> {
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

    let column_facts = table_metadata.planning_facts.column_facts();
    let mut delete_columns = Vec::with_capacity(column_names.len());
    for column_name in column_names {
        let (ordinal, field) = table_metadata
            .schema
            .fields()
            .iter()
            .enumerate()
            .find(|(_, field)| field.name().eq_ignore_ascii_case(column_name))
            .ok_or_else(|| format!("column `{column_name}` not found in iceberg table schema"))?;
        let data_type = column_facts
            .get(ordinal)
            .and_then(|fact| fact.write_target_type())
            .cloned()
            .unwrap_or_else(|| field.data_type().clone());
        ensure_supported_equality_key_type(&data_type, field.name())?;
        delete_columns.push(Field::new(
            field.name().clone(),
            data_type,
            field.is_nullable(),
        ));
    }

    for (ordinal, column) in delete_columns.iter().enumerate() {
        let values = rows.iter().map(|row| &row[ordinal]).collect::<Vec<_>>();
        validate_equality_literals(column, &values)?;
    }
    Ok(delete_columns)
}

/// Reject an equality key column whose write type this statement cannot encode.
///
/// The two rejections the Iceberg-typed predecessor produced here are kept:
/// variant columns, which are never equality-delete keys, and every type the
/// literal validator below has no rule for. `LargeBinary` is the write-target
/// Arrow type the provider signs for an Iceberg variant column, the same
/// spelling `arrow_data_type_to_sql_type` maps to `SqlType::Variant`.
fn ensure_supported_equality_key_type(
    data_type: &DataType,
    column_name: &str,
) -> Result<(), String> {
    if matches!(data_type, DataType::LargeBinary) {
        return Err(format!(
            "ADD EQUALITY DELETE column `{column_name}` is variant; variant columns cannot be equality-delete keys"
        ));
    }
    if !matches!(
        data_type,
        DataType::Boolean
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Utf8
            | DataType::Date32
            | DataType::Time64(TimeUnit::Microsecond)
            | DataType::Timestamp(TimeUnit::Microsecond, _)
    ) {
        return Err(unsupported_equality_key_type(data_type, column_name));
    }
    Ok(())
}

fn unsupported_equality_key_type(data_type: &DataType, column_name: &str) -> String {
    format!(
        "ADD EQUALITY DELETE does not yet support equality column `{column_name}` with arrow type {data_type:?}"
    )
}

/// Every rejection the discarded Arrow array builder produced, with none of the
/// arrays. The accepted literal set per column type is unchanged.
fn validate_equality_literals(column: &Field, values: &[&Literal]) -> Result<(), String> {
    ensure_nullability(column, values)?;
    match column.data_type() {
        DataType::Boolean => values.iter().try_for_each(|value| match value {
            Literal::Null | Literal::Bool(_) => Ok(()),
            other => Err(format!(
                "literal {:?} is not valid for BOOLEAN equality column `{}`",
                other,
                column.name()
            )),
        }),
        DataType::Int32 => values.iter().try_for_each(|value| match value {
            Literal::Null => Ok(()),
            Literal::Int(v) => i32::try_from(*v).map(|_| ()).map_err(|_| {
                format!(
                    "literal {v} is out of range for INT equality column `{}`",
                    column.name()
                )
            }),
            Literal::String(v) => v.trim().parse::<i32>().map(|_| ()).map_err(|_| {
                format!(
                    "literal `{v}` is not valid for INT equality column `{}`",
                    column.name()
                )
            }),
            other => Err(format!(
                "literal {:?} is not valid for INT equality column `{}`",
                other,
                column.name()
            )),
        }),
        DataType::Int64 => values.iter().try_for_each(|value| match value {
            Literal::Null | Literal::Int(_) => Ok(()),
            Literal::String(v) => v.trim().parse::<i64>().map(|_| ()).map_err(|_| {
                format!(
                    "literal `{v}` is not valid for LONG equality column `{}`",
                    column.name()
                )
            }),
            other => Err(format!(
                "literal {:?} is not valid for LONG equality column `{}`",
                other,
                column.name()
            )),
        }),
        DataType::Float32 => values.iter().try_for_each(|value| match value {
            Literal::Null | Literal::Int(_) | Literal::Float(_) => Ok(()),
            Literal::String(v) => v.trim().parse::<f32>().map(|_| ()).map_err(|_| {
                format!(
                    "literal `{v}` is not valid for FLOAT equality column `{}`",
                    column.name()
                )
            }),
            other => Err(format!(
                "literal {:?} is not valid for FLOAT equality column `{}`",
                other,
                column.name()
            )),
        }),
        DataType::Float64 => values.iter().try_for_each(|value| match value {
            Literal::Null | Literal::Int(_) | Literal::Float(_) => Ok(()),
            Literal::String(v) => v.trim().parse::<f64>().map(|_| ()).map_err(|_| {
                format!(
                    "literal `{v}` is not valid for DOUBLE equality column `{}`",
                    column.name()
                )
            }),
            other => Err(format!(
                "literal {:?} is not valid for DOUBLE equality column `{}`",
                other,
                column.name()
            )),
        }),
        DataType::Decimal128(_, scale) => values.iter().try_for_each(|value| match value {
            Literal::Null => Ok(()),
            Literal::Int(v) => scale_i128_decimal(i128::from(*v), *scale).map(|_| ()),
            Literal::Float(v) => parse_decimal_literal_to_i128(&v.to_string(), *scale).map(|_| ()),
            Literal::String(v) => parse_decimal_literal_to_i128(v, *scale).map(|_| ()),
            other => Err(format!(
                "literal {:?} is not valid for DECIMAL equality column `{}`",
                other,
                column.name()
            )),
        }),
        DataType::Utf8 => values.iter().try_for_each(|value| match value {
            Literal::Null | Literal::String(_) | Literal::Date(_) => Ok(()),
            other => Err(format!(
                "literal {:?} is not valid for STRING equality column `{}`",
                other,
                column.name()
            )),
        }),
        DataType::Date32 => values.iter().try_for_each(|value| match value {
            Literal::Null => Ok(()),
            Literal::Date(v) | Literal::String(v) => parse_date_string_to_days(v).map(|_| ()),
            other => Err(format!(
                "literal {:?} is not valid for DATE equality column `{}`",
                other,
                column.name()
            )),
        }),
        DataType::Time64(TimeUnit::Microsecond) => {
            values.iter().try_for_each(|value| match value {
                Literal::Null => Ok(()),
                Literal::String(v) => parse_time_literal_to_micros(v).map(|_| ()),
                other => Err(format!(
                    "literal {:?} is not valid for TIME equality column `{}`",
                    other,
                    column.name()
                )),
            })
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            values.iter().try_for_each(|value| match value {
                Literal::Null => Ok(()),
                Literal::Date(v) | Literal::String(v) => {
                    parse_datetime_string_to_micros(v).map(|_| ())
                }
                other => Err(format!(
                    "literal {:?} is not valid for TIMESTAMP equality column `{}`",
                    other,
                    column.name()
                )),
            })
        }
        // Unreachable: `ensure_supported_equality_key_type` already rejected
        // every type without a rule above. Kept so the two lists cannot drift
        // apart silently.
        other => Err(unsupported_equality_key_type(other, column.name())),
    }
}

fn ensure_nullability(column: &Field, values: &[&Literal]) -> Result<(), String> {
    if column.is_nullable() {
        return Ok(());
    }
    if values.iter().any(|value| matches!(value, Literal::Null)) {
        return Err(format!(
            "NULL is not valid for required equality column `{}`",
            column.name()
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
    use std::time::{Duration, Instant};

    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
    use bytes::Bytes;
    use novarocks_spi::connector::{
        ConnectorCancellation, ConnectorInstanceId, ConnectorRequestContext,
        ConnectorTableColumnPlanningFact, ConnectorTableColumnRole,
        ConnectorTableColumnSemanticKind, ConnectorTableColumnVisibility,
        ConnectorTableDefinitionFacts, ConnectorTableHandle, ConnectorTableIdentity,
        ConnectorTableMetadata, ConnectorTablePlanningFacts,
    };

    use crate::sql::parser::ast::Literal;

    struct NeverCancelled;

    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    fn request_context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(60),
            Arc::new(NeverCancelled),
            64 * 1_024,
            64 * 1_024,
        )
        .expect("request context")
    }

    /// A neutral `ConnectorTableMetadata` shaped the way the Iceberg provider
    /// reports one: the full physical Arrow schema plus per-column planning
    /// facts, where `write_target_type` carries the provider-signed DML write
    /// type for the columns whose write encoding differs from the read one.
    fn loaded_table(fields: Vec<(Field, Option<DataType>)>) -> ConnectorTableMetadata {
        let instance_id = ConnectorInstanceId::parse("ice").expect("instance ID");
        let schema = Arc::new(ArrowSchema::new(
            fields
                .iter()
                .map(|(field, _)| field.clone())
                .collect::<Vec<_>>(),
        ));
        let column_facts = fields
            .iter()
            .enumerate()
            .map(|(ordinal, (_, write_target_type))| {
                ConnectorTableColumnPlanningFact::new(
                    u32::try_from(ordinal).expect("ordinal"),
                    ConnectorTableColumnVisibility::Sql,
                    ConnectorTableColumnSemanticKind::None,
                    ConnectorTableColumnRole::Ordinary,
                )
                .with_write_target_type(write_target_type.clone())
            })
            .collect();
        let planning_facts = ConnectorTablePlanningFacts::try_new(
            &schema,
            column_facts,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            &request_context(),
        )
        .expect("planning facts");
        ConnectorTableMetadata {
            identity: ConnectorTableIdentity {
                instance_id: instance_id.clone(),
                namespace: Arc::from("db"),
                table: Arc::from("t"),
            },
            schema,
            planning_facts,
            definition_facts: ConnectorTableDefinitionFacts::empty(),
            version: None,
            statistics_data_version: None,
            table: ConnectorTableHandle::try_new(instance_id, Bytes::from_static(b"table"))
                .expect("table handle"),
        }
    }

    /// `id INT NOT NULL`, `category STRING NULL`, `amount BIGINT NULL`.
    fn plain_table() -> ConnectorTableMetadata {
        loaded_table(vec![
            (Field::new("id", DataType::Int32, false), None),
            (Field::new("category", DataType::Utf8, true), None),
            (Field::new("amount", DataType::Int64, true), None),
        ])
    }

    #[test]
    fn equality_delete_key_columns_project_the_declared_columns_from_connector_metadata() {
        let table = plain_table();
        let columns = vec!["id".to_string(), "category".to_string()];
        let rows = vec![
            vec![Literal::Int(2), Literal::String("B".to_string())],
            vec![Literal::Int(4), Literal::Null],
        ];

        let delete_columns =
            super::equality_delete_key_columns(&table, &columns, &rows).expect("columns");

        // Name, Arrow write type and nullability, in declaration order. The
        // Iceberg field ID the predecessor also returned had no production
        // reader: the writer's own columns are rebuilt inside the provider from
        // the signed field bindings and the frozen schema.
        assert_eq!(
            delete_columns,
            vec![
                Field::new("id", DataType::Int32, false),
                Field::new("category", DataType::Utf8, true),
            ]
        );
    }

    #[test]
    fn equality_delete_key_columns_reject_a_variant_key_column() {
        // A variant column reads as a two-leaf struct and writes as the
        // engine's encoded `LargeBinary`; the provider signs the write type.
        let table = loaded_table(vec![
            (Field::new("id", DataType::Int32, false), None),
            (
                Field::new(
                    "payload",
                    DataType::Struct(
                        vec![
                            Field::new("metadata", DataType::Binary, false),
                            Field::new("value", DataType::Binary, false),
                        ]
                        .into(),
                    ),
                    true,
                ),
                Some(DataType::LargeBinary),
            ),
        ]);
        let columns = vec!["payload".to_string()];
        let rows = vec![vec![Literal::Null]];

        let err = super::equality_delete_key_columns(&table, &columns, &rows)
            .expect_err("variant equality key must be rejected");

        assert!(
            err.contains("variant columns cannot be equality-delete keys"),
            "{err}"
        );
    }

    #[test]
    fn equality_delete_key_columns_reject_an_unsupported_key_type() {
        let table = loaded_table(vec![(
            Field::new(
                "tags",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            None,
        )]);
        let columns = vec!["tags".to_string()];
        let rows = vec![vec![Literal::Null]];

        let err = super::equality_delete_key_columns(&table, &columns, &rows)
            .expect_err("non-scalar equality key must be rejected");

        assert!(
            err.starts_with("ADD EQUALITY DELETE does not yet support equality column `tags`"),
            "{err}"
        );
    }

    /// The literal rejections the discarded Arrow array builder produced must
    /// still be produced, now without building any array.
    #[test]
    fn equality_delete_key_columns_reject_the_literals_the_array_builder_rejected() {
        let table = plain_table();

        let out_of_range = super::equality_delete_key_columns(
            &table,
            &["id".to_string()],
            &[vec![Literal::Int(i64::from(i32::MAX) + 1)]],
        )
        .expect_err("out-of-range INT literal must be rejected");
        assert_eq!(
            out_of_range,
            "literal 2147483648 is out of range for INT equality column `id`"
        );

        let wrong_type = super::equality_delete_key_columns(
            &table,
            &["category".to_string()],
            &[vec![Literal::Bool(true)]],
        )
        .expect_err("boolean literal must be rejected for a STRING key");
        assert_eq!(
            wrong_type,
            "literal Bool(true) is not valid for STRING equality column `category`"
        );

        let required_null =
            super::equality_delete_key_columns(&table, &["id".to_string()], &[vec![Literal::Null]])
                .expect_err("NULL must be rejected for a required key");
        assert_eq!(
            required_null,
            "NULL is not valid for required equality column `id`"
        );

        let unknown = super::equality_delete_key_columns(
            &table,
            &["absent".to_string()],
            &[vec![Literal::Int(1)]],
        )
        .expect_err("unknown column must be rejected");
        assert_eq!(unknown, "column `absent` not found in iceberg table schema");

        let duplicate = super::equality_delete_key_columns(
            &table,
            &["id".to_string(), "ID".to_string()],
            &[vec![Literal::Int(1), Literal::Int(1)]],
        )
        .expect_err("duplicate column must be rejected");
        assert_eq!(
            duplicate,
            "ADD EQUALITY DELETE has duplicate equality column `ID`"
        );

        let arity = super::equality_delete_key_columns(
            &table,
            &["id".to_string()],
            &[vec![Literal::Int(1), Literal::Int(2)]],
        )
        .expect_err("row arity mismatch must be rejected");
        assert_eq!(arity, "ADD EQUALITY DELETE row has 2 values, expected 1");

        let no_columns = super::equality_delete_key_columns(&table, &[], &[])
            .expect_err("empty column list must be rejected");
        assert_eq!(
            no_columns,
            "ADD EQUALITY DELETE requires at least one equality column"
        );
    }

    /// A `timestamptz` key column reads as a UTC-stamped Arrow timestamp. The
    /// predecessor flattened the zone away when it derived the Arrow type from
    /// the Iceberg primitive; the neutral schema keeps it, so the validator has
    /// to accept either spelling.
    #[test]
    fn equality_delete_key_columns_accept_a_zone_stamped_microsecond_timestamp() {
        let table = loaded_table(vec![(
            Field::new(
                "event_at",
                DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
                true,
            ),
            None,
        )]);

        let delete_columns = super::equality_delete_key_columns(
            &table,
            &["event_at".to_string()],
            &[vec![Literal::String("2024-01-02 03:04:05".to_string())]],
        )
        .expect("zone-stamped timestamp key");
        assert_eq!(delete_columns.len(), 1);

        let err = super::equality_delete_key_columns(
            &table,
            &["event_at".to_string()],
            &[vec![Literal::Int(7)]],
        )
        .expect_err("integer literal must be rejected for a TIMESTAMP key");
        assert_eq!(
            err,
            "literal Int(7) is not valid for TIMESTAMP equality column `event_at`"
        );
    }

    #[test]
    fn build_equality_delete_sink_query_projects_typed_values() {
        let table = plain_table();
        let columns = vec!["id".to_string(), "category".to_string()];
        let rows = vec![
            vec![Literal::Int(2), Literal::String("B".to_string())],
            vec![Literal::Int(4), Literal::Null],
        ];
        let delete_columns =
            super::equality_delete_key_columns(&table, &columns, &rows).expect("columns");

        let query = super::build_equality_delete_sink_query(&delete_columns, &rows).expect("query");
        let sql = query.to_string();

        assert!(sql.contains("VALUES"));
        assert!(sql.contains("CAST"));
        assert!(sql.contains("id"));
        assert!(sql.contains("category"));
        assert!(sql.contains("NULL"));
    }
}
