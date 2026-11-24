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

use novarocks_parser::Span;
use novarocks_user_error::{
    ErrorCodeDescriptor, ErrorCodeId, ErrorCodeStatus, ErrorPhase, RetryClass, UserError,
};

const DELETE_REQUIRES_WHERE: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.admit.delete_requires_where"),
    phase: ErrorPhase::Admit,
    status: ErrorCodeStatus::Deprecated,
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
const INVALID_DEFAULT_COLUMN_VALUE: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.admit.invalid_default_column_value"),
    phase: ErrorPhase::Admit,
    status: ErrorCodeStatus::Active,
};

const TABLE_ALREADY_EXISTS: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.admit.table_already_exists"),
    phase: ErrorPhase::Admit,
    status: ErrorCodeStatus::Active,
};

const MATERIALIZED_VIEW_TARGET: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.admit.materialized_view_target"),
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
    INVALID_DEFAULT_COLUMN_VALUE,
    TABLE_ALREADY_EXISTS,
    MATERIALIZED_VIEW_TARGET,
    SCHEMA_ALREADY_EXISTS,
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DmlAdmissionError {
    DeleteUnsupportedForm,
    UpdateUnsupportedForm,
    MergeUnsupportedForm,
    InsertUnsupportedForm,
    CreateTableUnsupportedForm,
    InvalidDefaultColumnValue,
    TableAlreadyExists,
    MaterializedViewTarget,
    SchemaAlreadyExists,
}

impl DmlAdmissionError {
    const fn descriptor(self) -> ErrorCodeDescriptor {
        match self {
            Self::DeleteUnsupportedForm => DELETE_UNSUPPORTED_FORM,
            Self::UpdateUnsupportedForm => UPDATE_UNSUPPORTED_FORM,
            Self::MergeUnsupportedForm => MERGE_UNSUPPORTED_FORM,
            Self::InsertUnsupportedForm => INSERT_UNSUPPORTED_FORM,
            Self::CreateTableUnsupportedForm => CREATE_TABLE_UNSUPPORTED_FORM,
            Self::InvalidDefaultColumnValue => INVALID_DEFAULT_COLUMN_VALUE,
            Self::TableAlreadyExists => TABLE_ALREADY_EXISTS,
            Self::MaterializedViewTarget => MATERIALIZED_VIEW_TARGET,
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

/// Validate DELETE-specific statement capabilities before any connector work.
pub(super) fn admit_delete_predicate(
    source: &str,
    predicate: &novarocks_parser::ast::Expr,
) -> Result<(), UserError> {
    use novarocks_parser::ast::{self, Expr, Predicate, Visit};

    #[derive(Default)]
    struct WhereShape {
        subquery: bool,
        unsupported: Option<Span>,
    }
    impl Visit for WhereShape {
        fn visit_query(&mut self, _: &ast::Query) {
            self.subquery = true;
        }

        fn visit_expr(&mut self, expression: &Expr) {
            if let Expr::Predicated(node) = expression
                && !matches!(
                    node.predicate,
                    Predicate::Comparison(_) | Predicate::In(_) | Predicate::IsNull(_)
                )
            {
                self.unsupported.get_or_insert(expression.span());
            }
            ast::walk_expr(self, expression);
        }
    }
    let mut shape = WhereShape::default();
    shape.visit_expr(predicate);
    if shape.subquery {
        return Err(DmlAdmissionError::DeleteUnsupportedForm.to_user_error(
            source,
            predicate.span(),
            "subqueries are unsupported in DELETE predicates",
        ));
    }
    if let Some(span) = shape.unsupported {
        return Err(DmlAdmissionError::DeleteUnsupportedForm.to_user_error(
            source,
            span,
            "phase 1 DELETE WHERE does not support this predicate form",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::DML_ADMISSION_ERROR_CODE_DESCRIPTORS;

    #[test]
    fn manifest_contains_every_dml_statement_shape_code() {
        assert_eq!(DML_ADMISSION_ERROR_CODE_DESCRIPTORS.len(), 10);
        assert!(
            DML_ADMISSION_ERROR_CODE_DESCRIPTORS
                .iter()
                .all(|descriptor| descriptor.code.as_str().starts_with("sql.admit."))
        );
    }

    #[test]
    fn removed_create_table_forms_are_grammar_rejections() {
        for source in [
            "CREATE TEMPORARY TABLE rejected_table (id INT)",
            "CREATE EXTERNAL TABLE rejected_table (id INT)",
            "CREATE TABLE rejected_table (id INT) ENGINE = olap",
            "CREATE TABLE rejected_table (id INT) ORDER BY (id)",
            "CREATE TABLE rejected_table (d DATE) PARTITION BY RANGE (d)",
        ] {
            let error = crate::sql::parse_single_statement(source).unwrap_err();
            assert_eq!(
                error.to_user_error(source).code().as_str(),
                "sql.parse.unexpected_token"
            );
        }
    }
}
