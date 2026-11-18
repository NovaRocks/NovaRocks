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

//! Typed DML statement-shape admission errors.
//!
//! These errors are emitted before a role adapter selects any connector,
//! execution path, or publication owner. They therefore belong to SQL
//! admission rather than to a particular DML implementation.

use novarocks_parser::{
    Span,
    ast::{TablePartition, TableStatement},
};
use novarocks_user_error::{
    ErrorCodeDescriptor, ErrorCodeId, ErrorCodeStatus, ErrorPhase, RetryClass, UserError,
};

const DELETE_REQUIRES_WHERE: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.admit.delete_requires_where"),
    phase: ErrorPhase::Admit,
    status: ErrorCodeStatus::Active,
};
const DELETE_UNSUPPORTED_FORM: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.admit.delete_unsupported_form"),
    phase: ErrorPhase::Admit,
    status: ErrorCodeStatus::Active,
};
const UPDATE_UNSUPPORTED_FORM: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.admit.update_unsupported_form"),
    phase: ErrorPhase::Admit,
    status: ErrorCodeStatus::Active,
};
const MERGE_UNSUPPORTED_FORM: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.admit.merge_unsupported_form"),
    phase: ErrorPhase::Admit,
    status: ErrorCodeStatus::Active,
};
const INSERT_UNSUPPORTED_FORM: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.admit.insert_unsupported_form"),
    phase: ErrorPhase::Admit,
    status: ErrorCodeStatus::Active,
};
const CREATE_TABLE_UNSUPPORTED_FORM: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.admit.create_table_unsupported_form"),
    phase: ErrorPhase::Admit,
    status: ErrorCodeStatus::Active,
};

const TABLE_ALREADY_EXISTS: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.admit.table_already_exists"),
    phase: ErrorPhase::Admit,
    status: ErrorCodeStatus::Active,
};

const SCHEMA_ALREADY_EXISTS: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.admit.schema_already_exists"),
    phase: ErrorPhase::Admit,
    status: ErrorCodeStatus::Active,
};

/// DML statement admission descriptors consumed by the independent manifest tool.
pub const DML_ADMISSION_ERROR_CODE_DESCRIPTORS: &[ErrorCodeDescriptor] = &[
    DELETE_REQUIRES_WHERE,
    DELETE_UNSUPPORTED_FORM,
    UPDATE_UNSUPPORTED_FORM,
    MERGE_UNSUPPORTED_FORM,
    INSERT_UNSUPPORTED_FORM,
    CREATE_TABLE_UNSUPPORTED_FORM,
    TABLE_ALREADY_EXISTS,
    SCHEMA_ALREADY_EXISTS,
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DmlAdmissionError {
    DeleteRequiresWhere,
    DeleteUnsupportedForm,
    UpdateUnsupportedForm,
    MergeUnsupportedForm,
    InsertUnsupportedForm,
    CreateTableUnsupportedForm,
    TableAlreadyExists,
    SchemaAlreadyExists,
}

impl DmlAdmissionError {
    const fn descriptor(self) -> ErrorCodeDescriptor {
        match self {
            Self::DeleteRequiresWhere => DELETE_REQUIRES_WHERE,
            Self::DeleteUnsupportedForm => DELETE_UNSUPPORTED_FORM,
            Self::UpdateUnsupportedForm => UPDATE_UNSUPPORTED_FORM,
            Self::MergeUnsupportedForm => MERGE_UNSUPPORTED_FORM,
            Self::InsertUnsupportedForm => INSERT_UNSUPPORTED_FORM,
            Self::CreateTableUnsupportedForm => CREATE_TABLE_UNSUPPORTED_FORM,
            Self::TableAlreadyExists => TABLE_ALREADY_EXISTS,
            Self::SchemaAlreadyExists => SCHEMA_ALREADY_EXISTS,
        }
    }

    pub fn to_user_error(self, source: &str, span: Span, message: impl Into<String>) -> UserError {
        UserError::from_descriptor(
            self.descriptor(),
            message,
            Some(span.to_user_error_location(source)),
            RetryClass::Never,
        )
    }
}

/// Reject unsupported `CREATE TABLE` forms before SQL routing selects a
/// catalog product or Connector request context.
///
/// The Catalog application still validates its own lowered request as a
/// defensive boundary. This parser-level check owns the user-visible SQL
/// admission result and keeps the product route from deciding syntax support.
pub fn validate_table_statement_admission(
    statement: &TableStatement,
    source: &str,
) -> Result<(), UserError> {
    let TableStatement::Create(statement) = statement;
    let unsupported = |span, message| {
        DmlAdmissionError::CreateTableUnsupportedForm.to_user_error(source, span, message)
    };
    if statement.temporary || statement.external {
        return Err(unsupported(
            statement.span,
            "CREATE TABLE does not support TEMPORARY or EXTERNAL tables".to_owned(),
        ));
    }
    if let Some(engine) = &statement.engine
        && !engine.value.eq_ignore_ascii_case("iceberg")
    {
        return Err(unsupported(
            engine.span,
            format!("CREATE TABLE does not support ENGINE = {}", engine.value),
        ));
    }
    if let Some(TablePartition::LegacyRange(partition)) = &statement.partition {
        return Err(unsupported(
            partition.span,
            "CREATE TABLE does not support legacy RANGE partition definitions".to_owned(),
        ));
    }
    if !statement.order_by.is_empty() {
        return Err(unsupported(
            statement.span,
            "CREATE TABLE does not support ORDER BY".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{DML_ADMISSION_ERROR_CODE_DESCRIPTORS, validate_table_statement_admission};
    use crate::sql::parse_single_statement;
    use novarocks_parser::ast::Statement;

    #[test]
    fn manifest_contains_every_dml_statement_shape_code() {
        assert_eq!(DML_ADMISSION_ERROR_CODE_DESCRIPTORS.len(), 8);
        assert!(
            DML_ADMISSION_ERROR_CODE_DESCRIPTORS
                .iter()
                .all(|descriptor| descriptor.code.as_str().starts_with("sql.admit."))
        );
    }

    #[test]
    fn create_table_shape_admission_rejects_unsupported_forms_before_routing() {
        for (sql, expected_message) in [
            (
                "CREATE TEMPORARY TABLE rejected_table (id INT)",
                "CREATE TABLE does not support TEMPORARY or EXTERNAL tables",
            ),
            (
                "CREATE TABLE rejected_table (id INT) ENGINE = olap",
                "CREATE TABLE does not support ENGINE = olap",
            ),
            (
                "CREATE TABLE rejected_table (id INT) ORDER BY (id)",
                "CREATE TABLE does not support ORDER BY",
            ),
            (
                "CREATE TABLE rejected_table (d DATE) PARTITION BY RANGE (d) (PARTITION p1 VALUES [('2024-01-01'), ('2024-02-01')))",
                "CREATE TABLE does not support legacy RANGE partition definitions",
            ),
        ] {
            let Statement::Table(statement) = parse_single_statement(sql).expect("parse table")
            else {
                panic!("expected CREATE TABLE statement");
            };
            let error = validate_table_statement_admission(&statement, sql)
                .expect_err("unsupported form must be rejected at admission");
            assert_eq!(
                error.code().as_str(),
                "sql.admit.create_table_unsupported_form"
            );
            assert_eq!(error.message(), expected_message);
            assert_eq!(error.location().map(|location| location.line()), Some(1));
        }
    }

    #[test]
    fn create_table_shape_admission_accepts_iceberg_form() {
        let sql = "CREATE TABLE accepted_table (id INT) ENGINE = iceberg";
        let Statement::Table(statement) = parse_single_statement(sql).expect("parse table") else {
            panic!("expected CREATE TABLE statement");
        };
        validate_table_statement_admission(&statement, sql).expect("admission accepts Iceberg");
    }
}
