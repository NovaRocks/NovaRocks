use crate::types::ConnectionConfig;
use anyhow::{Result, bail};
use std::collections::HashSet;

pub fn error_message_matches(actual: &str, expected_substring: &str) -> bool {
    if expected_substring.trim().is_empty() {
        return false;
    }
    actual
        .to_ascii_lowercase()
        .contains(&expected_substring.to_ascii_lowercase())
}

pub fn extract_engine_error_code(actual: &str) -> Option<String> {
    let message = engine_error_message_body(actual);
    let Some(candidate_start) = message.strip_prefix('[') else {
        return None;
    };
    let close_idx = candidate_start.find(']')?;
    let candidate = &candidate_start[..close_idx];
    let mut chars = candidate.chars();
    if matches!(chars.next(), Some(ch) if ch.is_ascii_uppercase())
        && chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        Some(candidate.to_string())
    } else {
        None
    }
}

fn engine_error_message_body(actual: &str) -> &str {
    let mut rest = actual.trim_start();
    loop {
        if let Some(stripped) = strip_runner_error_prefix(rest) {
            rest = stripped.trim_start();
            continue;
        }
        if let Some(stripped) = strip_mysql_error_prefix(rest) {
            rest = stripped.trim_start();
            continue;
        }
        return rest;
    }
}

fn strip_runner_error_prefix(message: &str) -> Option<&str> {
    let rest = message.strip_prefix("ERROR (")?;
    let close_idx = rest.find("): ")?;
    Some(&rest[close_idx + "): ".len()..])
}

fn strip_mysql_error_prefix(message: &str) -> Option<&str> {
    let rest = message.strip_prefix("ERROR ")?;
    let mut rest = rest.strip_prefix(|ch: char| ch.is_ascii_digit())?;
    while let Some(stripped) = rest.strip_prefix(|ch: char| ch.is_ascii_digit()) {
        rest = stripped;
    }
    let rest = rest.strip_prefix(" (")?;
    let close_idx = rest.find("): ")?;
    let sql_state = &rest[..close_idx];
    if sql_state.len() == 5 && sql_state.chars().all(|ch| ch.is_ascii_alphanumeric()) {
        Some(&rest[close_idx + "): ".len()..])
    } else {
        None
    }
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
        "{}: mysql={}, host={}:{}, user={}, catalog={}, db={}",
        label, conn.mysql, conn.host, conn.port, conn.user, catalog, db
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_engine_error_code_reads_bracket_prefix() {
        let actual =
            "ERROR 1105 (HY000): [IcebergWriteDescriptorMismatch] missing partition descriptor";

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
    fn extract_engine_error_code_rejects_non_prefix_brackets() {
        assert_eq!(
            extract_engine_error_code("ERROR 1105 (HY000): validation failed near [CommitUnknown]"),
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
            extract_engine_error_code("ERROR 1105 (HY000): plain error"),
            None
        );
    }

    #[test]
    fn extract_engine_error_code_rejects_lowercase_and_punctuation() {
        assert_eq!(
            extract_engine_error_code("ERROR 1105 (HY000): [icebergWriteDescriptorMismatch] bad"),
            None
        );
        assert_eq!(
            extract_engine_error_code("ERROR 1105 (HY000): [Iceberg-Write] bad"),
            None
        );
    }
}
