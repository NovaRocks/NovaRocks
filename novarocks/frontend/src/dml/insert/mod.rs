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

//! Frontend-owned INSERT command conversion and source shaping.

mod command;
mod iceberg;
mod shaping;

use novarocks::engine::insert_engine::{
    IcebergInsertSource, InsertEngine, InsertOverwriteMode, InsertTargetName, InsertValue,
    PrepareIcebergInsert, ResolveInsertTarget, ResolvedInsertTarget,
};
use novarocks::engine::statistics::{
    StatisticsInsertObservation, StatisticsInsertSource, StatisticsLiteral, StatisticsOverwriteMode,
};
use novarocks::query_execution::request_context::RequestContext;
use novarocks::runtime::query_options::QueryOptions;

use crate::dml::error::DmlError;
use crate::dml::service::DmlService;

pub use command::{InsertCommand, InsertCommandSource, convert_insert_command};
pub use shaping::reorder_insert_rows;

use self::iceberg::{IcebergInsertWriteExecutor, write_transaction_spec};

#[derive(Clone, Debug, Eq, PartialEq)]
enum InsertTargetRef {
    Main,
    Branch(String),
    Tag(String),
}

impl DmlService {
    /// Recognize and execute one INSERT through the frontend application owner.
    ///
    /// `Ok(None)` means the SQL is not an INSERT and may be delegated to the
    /// remaining core command kernel.
    pub fn try_execute_insert(
        &self,
        engine: &dyn InsertEngine,
        sql: &str,
        context: &RequestContext,
        query_options: Option<&QueryOptions>,
    ) -> Result<Option<()>, DmlError> {
        let Some(statement) = novarocks::engine::insert_engine::parse_insert_statement(sql)
            .map_err(DmlError::executor)?
        else {
            return Ok(None);
        };
        let mut command = convert_insert_command(&statement).map_err(DmlError::executor)?;
        let statistics_source = statistics_source(&command.source);
        let (target, target_ref) = split_target_ref(&command.target).map_err(DmlError::executor)?;
        command.target = target.clone();

        let session = context.session();
        let resolved = engine
            .resolve_target(ResolveInsertTarget {
                current_catalog: session.current_catalog().map(ToOwned::to_owned),
                current_database: session.current_database().to_string(),
                target,
                query_options: query_options.cloned(),
                execution: context.execution().clone(),
            })
            .map_err(DmlError::executor)?;
        validate_target(&target_ref).map_err(DmlError::executor)?;
        self.execute_iceberg_source(
            engine,
            &resolved,
            &command.columns,
            &command.source,
            command.overwrite_mode,
            &target_ref,
            context,
            query_options,
        )?;

        let statistics_overwrite_mode = match command.overwrite_mode {
            InsertOverwriteMode::Append => StatisticsOverwriteMode::Append,
            InsertOverwriteMode::FullTable => StatisticsOverwriteMode::FullTable,
            InsertOverwriteMode::DynamicPartitions => StatisticsOverwriteMode::DynamicPartitions,
        };
        self.statistics()
            .observe_insert(
                engine,
                StatisticsInsertObservation {
                    database: &resolved.namespace,
                    table: &resolved.table,
                    insert_columns: &command.columns,
                    source: &statistics_source,
                    overwrite_mode: statistics_overwrite_mode,
                },
            )
            .map_err(DmlError::executor)?;
        Ok(Some(()))
    }

    #[allow(clippy::too_many_arguments)]
    fn execute_iceberg_source(
        &self,
        engine: &dyn InsertEngine,
        target: &ResolvedInsertTarget,
        insert_columns: &[String],
        source: &InsertCommandSource,
        overwrite_mode: InsertOverwriteMode,
        target_ref: &InsertTargetRef,
        context: &RequestContext,
        query_options: Option<&QueryOptions>,
    ) -> Result<(), DmlError> {
        self.require_journal()?;
        let (source, prepared_insert_columns) = match source {
            InsertCommandSource::Values(rows) => (
                IcebergInsertSource::Rows(
                    reorder_insert_rows(rows, insert_columns, &target.columns)
                        .map_err(DmlError::executor)?,
                ),
                Vec::new(),
            ),
            InsertCommandSource::SelectLiteralRow(row) => (
                IcebergInsertSource::Rows(
                    reorder_insert_rows(std::slice::from_ref(row), insert_columns, &target.columns)
                        .map_err(DmlError::executor)?,
                ),
                Vec::new(),
            ),
            InsertCommandSource::FromQuery(query) => (
                IcebergInsertSource::Query(query.clone()),
                insert_columns.to_vec(),
            ),
        };
        let prepared = engine
            .prepare_iceberg_write(PrepareIcebergInsert {
                target: target.clone(),
                insert_columns: prepared_insert_columns,
                source,
                overwrite_mode,
                target_ref: match target_ref {
                    InsertTargetRef::Main => "main".to_string(),
                    InsertTargetRef::Branch(name) => name.clone(),
                    InsertTargetRef::Tag(_) => unreachable!("tag rejected before execution"),
                },
                query_options: query_options.cloned(),
                execution: context.execution().clone(),
            })
            .map_err(DmlError::executor)?;
        let spec = write_transaction_spec(&prepared);
        let executor = IcebergInsertWriteExecutor::new(engine, &prepared);
        self.run_write(spec, &executor).map(|_| ())
    }
}

fn split_target_ref(
    target: &InsertTargetName,
) -> Result<(InsertTargetName, InsertTargetRef), String> {
    let Some(last) = target.parts.last() else {
        return Err("INSERT target is empty".to_string());
    };
    let target_ref = if let Some(name) = last.strip_prefix("branch_")
        && !name.is_empty()
    {
        Some(InsertTargetRef::Branch(name.to_string()))
    } else if let Some(name) = last.strip_prefix("tag_")
        && !name.is_empty()
    {
        Some(InsertTargetRef::Tag(name.to_string()))
    } else {
        None
    };
    let Some(target_ref) = target_ref else {
        return Ok((target.clone(), InsertTargetRef::Main));
    };
    let parts = target.parts[..target.parts.len() - 1].to_vec();
    if parts.is_empty() {
        return Err("INSERT target is empty before Iceberg ref suffix".to_string());
    }
    Ok((InsertTargetName { parts }, target_ref))
}

fn validate_target(target_ref: &InsertTargetRef) -> Result<(), String> {
    if let InsertTargetRef::Tag(name) = target_ref {
        return Err(format!(
            "iceberg ref: tag '{name}' is read-only; use a branch as DML target"
        ));
    }
    Ok(())
}

fn statistics_source(source: &InsertCommandSource) -> StatisticsInsertSource {
    match source {
        InsertCommandSource::Values(rows) => StatisticsInsertSource::Values(
            rows.iter()
                .map(|row| row.iter().map(statistics_literal).collect())
                .collect(),
        ),
        InsertCommandSource::SelectLiteralRow(row) => {
            StatisticsInsertSource::SelectLiteralRow(row.iter().map(statistics_literal).collect())
        }
        InsertCommandSource::FromQuery(query) => StatisticsInsertSource::FromQuery(query.clone()),
    }
}

fn statistics_literal(value: &InsertValue) -> StatisticsLiteral {
    match value {
        InsertValue::Null => StatisticsLiteral::Null,
        InsertValue::Bool(value) => StatisticsLiteral::Bool(*value),
        InsertValue::Int(value) => StatisticsLiteral::Int(*value),
        InsertValue::Float(value) => StatisticsLiteral::Float(*value),
        InsertValue::String(value) => StatisticsLiteral::String(value.clone()),
        InsertValue::Date(value) => StatisticsLiteral::Date(value.clone()),
        InsertValue::Array(values) => {
            StatisticsLiteral::Array(values.iter().map(statistics_literal).collect())
        }
        InsertValue::Map(values) => StatisticsLiteral::Map(
            values
                .iter()
                .map(|(key, value)| (statistics_literal(key), statistics_literal(value)))
                .collect(),
        ),
        InsertValue::Struct(values) => {
            StatisticsLiteral::Struct(values.iter().map(statistics_literal).collect())
        }
    }
}
