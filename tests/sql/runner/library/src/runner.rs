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

use crate::engine_error_codes::EngineErrorCode;
use crate::sql_error_codes::{SqlErrorDescriptor, is_sql_error_identity};
use crate::types::{ConnectionConfig, SqlErrorLocation};
use anyhow::{Result, bail};
use regex::Regex;
use std::collections::HashSet;
use std::sync::OnceLock;

pub fn error_message_matches(actual: &str, expected_substring: &str) -> bool {
    if expected_substring.trim().is_empty() {
        return false;
    }
    actual
        .to_ascii_lowercase()
        .contains(&expected_substring.to_ascii_lowercase())
}

pub fn extract_engine_error_code(actual: &str) -> Option<String> {
    let message = engine_error_message_body(actual)?;
    let candidate_start = message.strip_prefix('[')?;
    let close_idx = candidate_start.find(']')?;
    let candidate = &candidate_start[..close_idx];
    if EngineErrorCode::parse(candidate).is_some() {
        Some(candidate.to_string())
    } else {
        None
    }
}

pub fn extract_sql_error_code(actual: &str) -> Option<String> {
    extract_sql_error_code_with_descriptors(actual, crate::production_sql_error_descriptors())
}

fn extract_sql_error_code_with_descriptors(
    actual: &str,
    descriptors: &[SqlErrorDescriptor],
) -> Option<String> {
    let message = engine_error_message_body(actual)?;
    let candidate_start = message.strip_prefix('[')?;
    let close_idx = candidate_start.find(']')?;
    let candidate = &candidate_start[..close_idx];
    is_sql_error_identity(candidate, descriptors).then(|| candidate.to_string())
}

/// Extract a line/column pair only from an explicitly rendered SQL error
/// position clause.  The accepted spellings cover the native user-error
/// rendering without treating a bare line number as a source location.
pub fn extract_sql_error_location(actual: &str) -> Option<SqlErrorLocation> {
    static LOCATION_RE: OnceLock<Regex> = OnceLock::new();
    let re = LOCATION_RE.get_or_init(|| {
        Regex::new(r"(?i)\b(?:at\s+)?line\s+(\d+)\s*(?:,?\s*(?:column|col)\s+|:)(\d+)\b")
            .expect("static SQL error location regex compiles")
    });
    let captures = re.captures(engine_error_message_body(actual)?)?;
    let line = captures.get(1)?.as_str().parse().ok()?;
    let column = captures.get(2)?.as_str().parse().ok()?;
    (line > 0 && column > 0).then_some(SqlErrorLocation { line, column })
}

fn engine_error_message_body(actual: &str) -> Option<&str> {
    let mut rest = actual.trim_start();
    loop {
        if let Some(stripped) = strip_runner_error_prefix(rest) {
            rest = stripped.trim_start();
            continue;
        }
        if let Some(stripped) = strip_statement_failure_context(rest) {
            rest = stripped.trim_start();
            continue;
        }
        break;
    }
    // HTTP/transport/protocol diagnostics carry arbitrary text. Their body
    // cannot grant a product error identity or SQL source location.
    if [
        "TrinoHttp {",
        "TrinoTransport {",
        "Trino statement deadline expired",
        "Unsupported Trino session change header",
        "Trino protocol failure",
    ]
    .iter()
    .any(|prefix| rest.starts_with(prefix))
    {
        return None;
    }
    if rest.starts_with("TrinoError { ") {
        return strip_trino_error_prefix(rest).map(str::trim_start);
    }
    Some(rest)
}

fn strip_statement_failure_context(message: &str) -> Option<&str> {
    let rest = message.strip_prefix("statement ")?;
    let (ordinal, rest) = rest.split_once('/')?;
    let (count, rest) = rest.split_once(" (sha256=")?;
    if ordinal.is_empty()
        || count.is_empty()
        || !ordinal.bytes().all(|byte| byte.is_ascii_digit())
        || !count.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    let (digest, rest) = rest.split_once("): ")?;
    if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }
    Some(rest)
}

fn strip_runner_error_prefix(message: &str) -> Option<&str> {
    for prefix in ["target execute failed: ", "record source execute failed: "] {
        if let Some(rest) = message.strip_prefix(prefix) {
            return Some(rest);
        }
    }
    for prefix in ["ERROR (", "FAIL ("] {
        let Some(rest) = message.strip_prefix(prefix) else {
            continue;
        };
        let close_idx = rest.find("): ")?;
        return Some(&rest[close_idx + "): ".len()..]);
    }
    None
}

fn strip_trino_error_prefix(message: &str) -> Option<&str> {
    message
        .strip_prefix("TrinoError { ")?
        .split_once(" }: ")
        .map(|(_, body)| body)
}

pub fn is_transient_iceberg_commit_error(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("metadata file for version")
        && lower.contains("is missing under")
        && lower.contains("/metadata")
}

pub fn parse_selector_list(
    value: Option<&str>,
    available_case_ids: &HashSet<String>,
    flag_name: &str,
) -> Result<HashSet<String>> {
    let selectors: HashSet<String> = value
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToString::to_string)
        .collect();

    for selector in &selectors {
        if available_case_ids.contains(selector) {
            continue;
        }
        let Some((candidate_case_id, candidate_step)) = selector.rsplit_once('-') else {
            continue;
        };
        if candidate_step.parse::<usize>().is_ok() && available_case_ids.contains(candidate_case_id)
        {
            bail!(
                "{} no longer supports sub-query selectors like '{}'; use '{}' instead",
                flag_name,
                selector,
                candidate_case_id
            );
        }
    }

    Ok(selectors)
}

pub fn summarize_connection(label: &str, conn: &ConnectionConfig) -> String {
    let catalog = conn.catalog.as_deref().unwrap_or("");
    let db = conn.db.as_deref().unwrap_or("");
    format!(
        "{}: trino_http={}:{}, user={}, catalog={}, db={}",
        label, conn.host, conn.port, conn.user, catalog, db
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_engine_error_code_reads_bracket_prefix() {
        let actual = "TrinoError { name=GENERIC_INTERNAL_ERROR type=INTERNAL_ERROR code=65536 }: [IcebergWriteDescriptorMismatch] missing partition descriptor";

        assert_eq!(
            extract_engine_error_code(actual),
            Some("IcebergWriteDescriptorMismatch".to_string())
        );
    }

    #[test]
    fn extract_engine_error_code_reads_plain_bracket_prefix() {
        assert_eq!(
            extract_engine_error_code("[CommitUnknown] commit outcome unavailable"),
            Some("CommitUnknown".to_string())
        );
    }

    #[test]
    fn extract_engine_error_code_reads_trino_error_wrapper() {
        let actual = "FAIL (0.00s): TrinoError { name=NOT_SUPPORTED type=USER_ERROR code=13 }: [UnsupportedDistributedDmlShape] ADMIN RAISE ENGINE ERROR: forced P8 SQL runner error-code smoke";

        assert_eq!(
            extract_engine_error_code(actual),
            Some("UnsupportedDistributedDmlShape".to_string())
        );
    }

    #[test]
    fn extract_engine_error_code_reads_statement_failure_context() {
        let actual = "FAIL (0.00s): statement 1/1 (sha256=373d888744767278a2480d0ec9a6cf1de16a3c52ab3c7501ddbd79bf7c34f40e): TrinoError { name=NOT_SUPPORTED type=USER_ERROR code=13 }: [UnsupportedDistributedDmlShape] forced failure";
        assert_eq!(
            extract_engine_error_code(actual),
            Some("UnsupportedDistributedDmlShape".to_string())
        );
    }

    #[test]
    fn extract_engine_error_code_rejects_malformed_statement_failure_context() {
        let actual = "FAIL (0.00s): statement 1/1 (sha256=not-a-digest): TrinoError { name=NOT_SUPPORTED type=USER_ERROR code=13 }: [UnsupportedDistributedDmlShape] forced failure";
        assert_eq!(extract_engine_error_code(actual), None);
    }

    #[test]
    fn extract_compiled_identity_uses_exact_registered_descriptor_without_text_classification() {
        let registered = [SqlErrorDescriptor {
            code: "UnsupportedCompiledShape",
            phase: crate::sql_error_codes::SqlErrorPhase::Execution,
        }];
        let actual = "TrinoError { name=GENERIC_INTERNAL_ERROR type=INTERNAL_ERROR code=65536 }: [UnsupportedCompiledShape] FULL OUTER hash join";
        assert_eq!(
            extract_sql_error_code_with_descriptors(actual, &registered).as_deref(),
            Some("UnsupportedCompiledShape"),
        );
        assert_eq!(extract_sql_error_code_with_descriptors(actual, &[]), None);
        assert_eq!(
            extract_engine_error_code(actual).as_deref(),
            Some("UnsupportedCompiledShape")
        );
        for invalid in [
            "TrinoError { name=GENERIC_INTERNAL_ERROR type=INTERNAL_ERROR code=65536 }: [CommitUnknown] FULL OUTER hash join",
            "TrinoError { name=GENERIC_INTERNAL_ERROR type=INTERNAL_ERROR code=65536 }: [UnsupportedcompiledShape] FULL OUTER hash join",
            "TrinoError { name=GENERIC_INTERNAL_ERROR type=INTERNAL_ERROR code=65536 }: [UnsupportedCompiledShapeX] FULL OUTER hash join",
            "TrinoError { name=GENERIC_INTERNAL_ERROR type=INTERNAL_ERROR code=65536 }: [NotARealCode] FULL OUTER hash join",
            "TrinoError { name=GENERIC_INTERNAL_ERROR type=INTERNAL_ERROR code=65536 }: ordinary diagnostic near [UnsupportedCompiledShape]",
        ] {
            assert_eq!(
                extract_sql_error_code_with_descriptors(invalid, &registered),
                None
            );
        }
    }

    #[test]
    fn extract_sql_error_code_keeps_dotted_codes_separate_from_engine_codes() {
        assert_eq!(
            extract_sql_error_code(
                "TrinoError { name=SYNTAX_ERROR type=USER_ERROR code=1 }: [sql.parse.unexpected_token] unexpected FROM"
            ),
            Some("sql.parse.unexpected_token".to_string())
        );
        assert_eq!(extract_sql_error_code("[CommitUnknown] unavailable"), None);
    }

    #[test]
    fn extract_sql_error_location_requires_line_and_column() {
        assert_eq!(
            extract_sql_error_location(
                "TrinoError { name=SYNTAX_ERROR type=USER_ERROR code=1 }: [sql.parse.unexpected_token] at line 7, column 11: unexpected FROM"
            ),
            Some(SqlErrorLocation {
                line: 7,
                column: 11
            })
        );
        assert_eq!(
            extract_sql_error_location(
                "TrinoError { name=SYNTAX_ERROR type=USER_ERROR code=1 }: at line 7: incomplete location"
            ),
            None
        );
    }

    #[test]
    fn extract_engine_error_code_rejects_malformed_trino_error_wrapper() {
        let actual = "FAIL (0.00s): TrinoError { [CommitUnknown] bare wrapper }";

        assert_eq!(extract_engine_error_code(actual), None);
    }

    #[test]
    fn retired_mysql_carriers_do_not_establish_product_error_identity() {
        for actual in [
            "ERROR 1105 (HY000): [CommitUnknown] unavailable",
            "FAIL (0.00s): MySqlError { ERROR 1235 (42000): [UnsupportedCompiledShape] unavailable }",
            "ERROR 1064 (42000): [sql.parse.unexpected_token] unexpected FROM",
        ] {
            assert_eq!(extract_engine_error_code(actual), None);
            assert_eq!(extract_sql_error_code(actual), None);
        }
    }

    #[test]
    fn statement_failure_context_preserves_trino_sql_identity_and_location() {
        let actual = "FAIL (0.02s): statement 2/3 (sha256=373d888744767278a2480d0ec9a6cf1de16a3c52ab3c7501ddbd79bf7c34f40e): TrinoError { name=SYNTAX_ERROR type=USER_ERROR code=1 }: [sql.parse.unexpected_token] at line 7, column 11: unexpected FROM";
        assert_eq!(
            extract_sql_error_code(actual).as_deref(),
            Some("sql.parse.unexpected_token")
        );
        assert_eq!(
            extract_sql_error_location(actual),
            Some(SqlErrorLocation {
                line: 7,
                column: 11
            })
        );
    }

    #[test]
    fn non_domain_carriers_cannot_spoof_product_error_identity_or_location() {
        for outer in [
            "TrinoHttp { status=503 }: ",
            "TrinoTransport { Trino transport failure }: ",
            "Trino statement deadline expired: ",
            "Unsupported Trino session change header: ",
            "Trino protocol failure: ",
        ] {
            for body in [
                "TrinoError { name=SYNTAX_ERROR type=USER_ERROR code=1 }: [sql.parse.unexpected_token] at line 1 column 2",
                "TrinoError { name=GENERIC_INTERNAL_ERROR type=INTERNAL_ERROR code=65536 }: [CommitUnknown] at line 1 column 2",
            ] {
                let message = format!(
                    "FAIL (0.02s): statement 1/1 (sha256=373d888744767278a2480d0ec9a6cf1de16a3c52ab3c7501ddbd79bf7c34f40e): {outer}{body}"
                );
                assert_eq!(extract_engine_error_code(&message), None, "{message}");
                assert_eq!(extract_sql_error_code(&message), None, "{message}");
                assert_eq!(extract_sql_error_location(&message), None, "{message}");
            }
        }
    }

    #[test]
    fn extract_engine_error_code_rejects_non_prefix_brackets() {
        assert_eq!(
            extract_engine_error_code(
                "TrinoError { name=GENERIC_INTERNAL_ERROR type=INTERNAL_ERROR code=65536 }: validation failed near [CommitUnknown]"
            ),
            None
        );
        assert_eq!(
            extract_engine_error_code("plain context [CommitUnknown]"),
            None
        );
    }

    #[test]
    fn extract_engine_error_code_returns_none_for_plain_error() {
        assert_eq!(
            extract_engine_error_code(
                "TrinoError { name=GENERIC_INTERNAL_ERROR type=INTERNAL_ERROR code=65536 }: plain error"
            ),
            None
        );
    }

    #[test]
    fn extract_engine_error_code_rejects_lowercase_and_punctuation() {
        assert_eq!(
            extract_engine_error_code(
                "TrinoError { name=GENERIC_INTERNAL_ERROR type=INTERNAL_ERROR code=65536 }: [icebergWriteDescriptorMismatch] bad"
            ),
            None
        );
        assert_eq!(
            extract_engine_error_code(
                "TrinoError { name=GENERIC_INTERNAL_ERROR type=INTERNAL_ERROR code=65536 }: [Iceberg-Write] bad"
            ),
            None
        );
    }

    #[test]
    fn extract_engine_error_code_rejects_unknown_code_name() {
        assert_eq!(
            extract_engine_error_code(
                "TrinoError { name=GENERIC_INTERNAL_ERROR type=INTERNAL_ERROR code=65536 }: [NotARealCode] bad"
            ),
            None
        );
    }
}
