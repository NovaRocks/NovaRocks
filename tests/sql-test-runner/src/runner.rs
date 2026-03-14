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
