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

//! Stable domain-code to MySQL wire-kind adaptation.

use opensrv_mysql::ErrorKind;

pub fn error_kind_for_domain_code(code: &str) -> Option<ErrorKind> {
    Some(match code {
        "sql.lex.unexpected_character"
        | "sql.lex.unterminated_string"
        | "sql.lex.unterminated_quoted_identifier"
        | "sql.lex.unterminated_comment"
        | "sql.parse.unexpected_token"
        | "sql.validate.invalid_structure"
        | "sql.validate.duplicate_cte_name"
        | "sql.validate.duplicate_window_name"
        | "sql.validate.invalid_window_frame_bounds" => ErrorKind::ER_PARSE_ERROR,
        "sql.parse.unsupported_statement"
        | "sql.admit.delete_requires_where"
        | "sql.admit.delete_unsupported_form"
        | "sql.admit.update_unsupported_form"
        | "sql.admit.merge_unsupported_form"
        | "sql.admit.insert_unsupported_form"
        | "sql.admit.create_table_unsupported_form"
        | "sql.admit.persisted_definition_semantics_unsupported"
        | "sql.admit.session_global_scope_unsupported"
        | "sql.admit.session_transaction_unsupported"
        | "sql.analyze.unsupported_expression"
        | "sql.analyze.unsupported_query_shape" => ErrorKind::ER_NOT_SUPPORTED_YET,
        "sql.admit.kill_denied" => ErrorKind::ER_KILL_DENIED_ERROR,
        "sql.analyze.unknown_table" => ErrorKind::ER_NO_SUCH_TABLE,
        "sql.analyze.unknown_column" => ErrorKind::ER_BAD_FIELD_ERROR,
        "sql.analyze.unknown_function" => ErrorKind::ER_FUNCTION_NOT_DEFINED,
        "sql.analyze.type_mismatch" | "sql.analyze.invalid_argument" => {
            ErrorKind::ER_WRONG_ARGUMENTS
        }
        "sql.analyze.invalid_literal" => ErrorKind::ER_WRONG_VALUE,
        "sql.analyze.invalid_query_shape" => ErrorKind::ER_WRONG_USAGE,
        "sql.analyze.internal" => ErrorKind::ER_UNKNOWN_ERROR,
        _ => return None,
    })
}

#[cfg(test)]
#[path = "error_mapping/tests_manifest.rs"]
mod manifest_tests;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn persisted_definition_semantics_has_explicit_unsupported_wire_mapping() {
        assert_eq!(
            error_kind_for_domain_code("sql.admit.persisted_definition_semantics_unsupported"),
            Some(ErrorKind::ER_NOT_SUPPORTED_YET),
        );
    }

    #[test]
    fn maps_known_codes_and_rejects_unknown_codes() {
        assert_eq!(
            error_kind_for_domain_code("sql.analyze.unknown_table"),
            Some(ErrorKind::ER_NO_SUCH_TABLE)
        );
        assert_eq!(
            error_kind_for_domain_code("sql.admit.kill_denied"),
            Some(ErrorKind::ER_KILL_DENIED_ERROR)
        );
        assert_eq!(error_kind_for_domain_code("sql.analyze.unregistered"), None);
    }
}
