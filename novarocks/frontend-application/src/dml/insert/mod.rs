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
    omitted_insert_expr, shape_insert_source, shape_insert_source_with_constants,
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
