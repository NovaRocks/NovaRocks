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

use crate::query_execution::dml::insert::{
    IcebergInsertSource, InsertEngine, InsertOverwriteMode, InsertTargetName, PrepareIcebergInsert,
    ResolveInsertTarget, ResolvedInsertTarget,
};
use novarocks_proto_codec::lifecycle::QueryOptions;
use novarocks_query_application::admitted_query_context::RequestContext;
use novarocks_spi::connector::{LakePublicationFamily, LakePublicationId};

use crate::dml::error::DmlError;
use crate::dml::runner::StatementWriteTransactionRunner;
use crate::dml::service::DmlService;
use novarocks_query_application::sql::dml_admission::DmlAdmissionError;

pub use command::{InsertCommandSource, convert_insert_command};
pub use shaping::reorder_insert_rows;

use self::iceberg::{IcebergInsertWriteExecutor, write_transaction_spec};

#[derive(Clone, Debug, Eq, PartialEq)]
enum InsertTargetRef {
    Main,
    Branch(String),
    Tag(String),
}

impl DmlService {
    /// Execute one SQLP-5 typed INSERT through the frontend application owner.
    ///
    /// Statement-family routing is complete before this boundary. `source`
    /// exists solely for diagnostic locations derived from AST spans; it must
    /// not be reparsed or sliced to classify an INSERT form. Its query source
    /// is already the parser-owned typed AST.
    #[allow(
        clippy::result_large_err,
        reason = "Preserves the frozen DML error contract without a broad ABI migration."
    )]
    pub fn try_execute_insert(
        &self,
        engine: &dyn InsertEngine,
        statement: &novarocks_parser::ast::Insert,
        source: &str,
        context: &RequestContext,
        query_options: Option<&QueryOptions>,
    ) -> Result<(), DmlError> {
        let mut command = convert_insert_command(statement)
            .map_err(|error| insert_admit_error(source, statement.span, error))?;
        let (target, target_ref) = split_target_ref(&command.target)
            .map_err(|error| insert_admit_error(source, statement.target.span, error))?;
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
        validate_target(&target_ref)
            .map_err(|error| insert_admit_error(source, statement.target.span, error))?;
        self.execute_iceberg_source(
            engine,
            resolved,
            &command.columns,
            &command.source,
            source,
            command.overwrite_mode,
            &target_ref,
            context,
            query_options,
        )?;

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    #[allow(
        clippy::result_large_err,
        reason = "Preserves the frozen DML error contract without a broad ABI migration."
    )]
    fn execute_iceberg_source(
        &self,
        engine: &dyn InsertEngine,
        target: ResolvedInsertTarget,
        insert_columns: &[String],
        source: &InsertCommandSource,
        sql_source: &str,
        overwrite_mode: InsertOverwriteMode,
        target_ref: &InsertTargetRef,
        context: &RequestContext,
        query_options: Option<&QueryOptions>,
    ) -> Result<(), DmlError> {
        let publication_id = LakePublicationId::new_v7();
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
                publication_id,
                target,
                insert_columns: prepared_insert_columns,
                sql_source: sql_source.to_string(),
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
        StatementWriteTransactionRunner::new(&executor, LakePublicationFamily::Write)
            .run(spec)
            .map(|_| ())
    }
}

fn insert_admit_error(source: &str, span: novarocks_parser::Span, message: String) -> DmlError {
    DmlError::admit(DmlAdmissionError::InsertUnsupportedForm.to_user_error(source, span, message))
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
