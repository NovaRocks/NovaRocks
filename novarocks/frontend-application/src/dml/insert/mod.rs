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

pub(crate) mod command;
mod shaping;

use crate::dml::error::DmlError;
use crate::dml::service::DmlService;
use crate::query_execution::dml::insert::InsertTargetName;
use novarocks_query_application::sql::dml_admission::DmlAdmissionError;

pub use command::convert_insert_command;
pub(crate) use shaping::{
    attach_write_literal_contexts, capture_typed_write_constants, omitted_insert_expr,
    shape_typed_write_source,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum InsertTargetRef {
    Main,
    Branch(String),
    Tag(String),
}

impl DmlService {
    /// Pure command/target validation for the original statement action. No
    /// provider request, execution identity or statement authority is created.
    pub(crate) fn prepare_insert_shape(
        &self,
        statement: &novarocks_parser::ast::Insert,
        source: &str,
    ) -> Result<(command::InsertCommand, InsertTargetRef), DmlError> {
        let mut command = convert_insert_command(statement)
            .map_err(|error| insert_admit_error(source, statement.span, error))?;
        let (target, reference) = split_target_ref(&command.target)
            .map_err(|error| insert_admit_error(source, statement.target.span, error))?;
        validate_target(&reference)
            .map_err(|error| insert_admit_error(source, statement.target.span, error))?;
        command.target = target;
        Ok((command, reference))
    }
}

pub(crate) fn prepare_trino_insert_shape(
    statement: novarocks_parser::ast::TrinoInsert,
    source: &str,
) -> Result<(command::InsertCommand, InsertTargetRef), DmlError> {
    use crate::query_execution::dml::insert::InsertOverwriteMode;
    use novarocks_parser::ast::TrinoInsertMode;

    if statement.table.query_period.is_some() {
        return Err(insert_admit_error(
            source,
            statement.table.span,
            "INSERT target cannot select a historical table snapshot".into(),
        ));
    }
    let parts = statement
        .table
        .name
        .parts
        .into_iter()
        .map(|part| part.value)
        .collect::<Vec<_>>();
    command::validate_insert_source(&parts, &statement.query)
        .map_err(|error| insert_admit_error(source, statement.span, error))?;
    let reference = statement
        .table
        .branch
        .map_or(InsertTargetRef::Main, |branch| {
            InsertTargetRef::Branch(branch.value)
        });
    let overwrite_mode = match statement.mode {
        TrinoInsertMode::Into => InsertOverwriteMode::Append,
        TrinoInsertMode::Overwrite => InsertOverwriteMode::FullTable,
        TrinoInsertMode::OverwritePartitions => InsertOverwriteMode::DynamicPartitions,
    };
    Ok((
        command::InsertCommand {
            target: InsertTargetName { parts },
            columns: statement
                .columns
                .into_iter()
                .flatten()
                .map(|column| column.value)
                .collect(),
            source: statement.query.into_box(),
            overwrite_mode,
        },
        reference,
    ))
}

fn insert_admit_error(source: &str, span: novarocks_parser::Span, message: String) -> DmlError {
    DmlError::admit(DmlAdmissionError::InsertUnsupportedForm.to_user_error(source, span, message))
}

pub(crate) fn split_target_ref(
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

pub(crate) fn validate_target(target_ref: &InsertTargetRef) -> Result<(), String> {
    if let InsertTargetRef::Tag(name) = target_ref {
        return Err(format!(
            "iceberg ref: tag '{name}' is read-only; use a branch as DML target"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod variant_constants_tests;

#[cfg(test)]
mod trino_tests {
    use super::*;
    use crate::query_execution::dml::insert::InsertOverwriteMode;
    use novarocks_parser::{ast::TrinoStatement, trino_parser};

    #[test]
    fn trino_insert_uses_the_typed_branch_and_original_query_allocation() {
        for (sql, expected_mode, expected_reference) in [
            (
                r#"INSERT OVERWRITE c.s.branch_named@"Audit-Ref" (x) SELECT ?"#,
                InsertOverwriteMode::FullTable,
                InsertTargetRef::Branch("Audit-Ref".into()),
            ),
            (
                "INSERT OVERWRITE PARTITIONS c.s.branch_named@dev (x) SELECT ?",
                InsertOverwriteMode::DynamicPartitions,
                InsertTargetRef::Branch("dev".into()),
            ),
            (
                "INSERT INTO c.s.branch_named (x) SELECT ?",
                InsertOverwriteMode::Append,
                InsertTargetRef::Main,
            ),
        ] {
            let TrinoStatement::Insert(insert) = trino_parser::parse_statement(sql).unwrap() else {
                panic!("expected INSERT");
            };
            let original = std::ptr::from_ref(&*insert.query);
            let span = insert.query.span;
            let (command, reference) = prepare_trino_insert_shape(insert, sql).unwrap();
            assert_eq!(command.target.parts, ["c", "s", "branch_named"]);
            assert_eq!(command.columns, ["x"]);
            assert_eq!(command.overwrite_mode, expected_mode);
            assert_eq!(reference, expected_reference);
            assert_eq!(&sql[span.start()..span.end()], "SELECT ?");
            assert_eq!(command.source.span, span);
            assert_eq!(std::ptr::from_ref(command.source.as_ref()), original);
        }
    }

    #[test]
    fn trino_insert_keeps_the_literal_distinct_set_operation_refusal() {
        let sql = "INSERT INTO t SELECT 1 UNION SELECT 2";
        let TrinoStatement::Insert(insert) = trino_parser::parse_statement(sql).unwrap() else {
            panic!("expected INSERT");
        };
        let error = prepare_trino_insert_shape(insert, sql).unwrap_err();
        let error = error.user_error().unwrap();
        assert_eq!(error.code().as_str(), "sql.admit.insert_unsupported_form");
        assert_eq!(
            error.message(),
            "INSERT SELECT UNION requires UNION ALL (UNION/UNION DISTINCT unsupported)"
        );
    }
}
