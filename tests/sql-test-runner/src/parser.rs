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

use crate::config::{case_placeholder_variables, parse_bool, substitute_placeholders};
use crate::types::*;
use anyhow::{Context, Result, bail};
use regex::Regex;
use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::path::Path;

#[path = "../../../novarocks/core/src/common/engine_error_codes.rs"]
mod engine_error_codes;
use engine_error_codes::EngineErrorCode;

/// Scan raw SQL content (before placeholder substitution) for `${case_db}` and
/// `${case_db_N}` references.  Returns the resolved database names in order.
/// Index 0 is the primary `${case_db}`, index 1 is `${case_db_2}`, etc.
pub fn detect_case_dbs(raw_content: &str, variables: &HashMap<String, String>) -> Vec<String> {
    let mut indices: BTreeSet<usize> = BTreeSet::new();

    if raw_content.contains("${case_db}") {
        indices.insert(0);
    }
    for n in 2..=9 {
        let placeholder = format!("${{case_db_{}}}", n);
        if raw_content.contains(&placeholder) {
            // Secondary database implies primary is also needed.
            indices.insert(0);
            indices.insert(n - 1);
        }
    }

    indices
        .into_iter()
        .filter_map(|idx| {
            let key = if idx == 0 {
                "case_db".to_string()
            } else {
                format!("case_db_{}", idx + 1)
            };
            variables.get(&key).cloned()
        })
        .collect()
}

pub fn parse_meta_line(line: &str, meta_re: &Regex) -> Option<(String, String)> {
    let captures = meta_re.captures(line.trim())?;
    let key = captures.get(1)?.as_str().to_lowercase();
    let value = captures.get(2)?.as_str().trim().to_string();
    Some((key, value))
}

fn legacy_name_line_has_sequential_tag(line: &str) -> bool {
    let Some(body) = line.trim().strip_prefix("--").map(str::trim_start) else {
        return false;
    };
    if !body
        .get(..5)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("name:"))
    {
        return false;
    }
    body.split_whitespace()
        .any(|token| token.eq_ignore_ascii_case("@sequential"))
}

fn parse_imv_stateless_rebuild(raw: &str) -> anyhow::Result<ImvStatelessDirective> {
    let mut parts = raw.split(',').map(str::trim).filter(|s| !s.is_empty());
    let mv = parts
        .next()
        .ok_or_else(|| anyhow::anyhow!("@imv_stateless_rebuild requires an MV name"))?
        .to_string();
    let mut level = ImvStatelessLevel::Package;
    let mut catalog = None;
    for part in parts {
        if let Some(value) = part.strip_prefix("level=") {
            level = match value {
                "baseline" => ImvStatelessLevel::Baseline,
                "package" => ImvStatelessLevel::Package,
                "provenance" => ImvStatelessLevel::Provenance,
                "full" => ImvStatelessLevel::Full,
                other => anyhow::bail!("unknown @imv_stateless_rebuild level `{other}`"),
            };
        } else if let Some(value) = part.strip_prefix("catalog=") {
            catalog = Some(value.to_string());
        } else {
            anyhow::bail!("unknown @imv_stateless_rebuild option `{part}`");
        }
    }
    Ok(ImvStatelessDirective { mv, level, catalog })
}

fn detect_case_sequential(lines: &[String], file_meta_lines: &[String], meta_re: &Regex) -> bool {
    file_meta_lines.iter().any(|line| {
        parse_meta_line(line, meta_re)
            .map(|(k, v)| k == "sequential" && parse_bool(&v).unwrap_or(false))
            .unwrap_or(false)
    }) || lines
        .iter()
        .any(|line| legacy_name_line_has_sequential_tag(line))
}

pub fn parse_meta(lines: &[String], meta_re: &Regex) -> Result<QueryMeta> {
    let mut meta = QueryMeta::default();
    for line in lines {
        let Some((key, raw_value)) = parse_meta_line(line, meta_re) else {
            continue;
        };
        match key.as_str() {
            "order_sensitive" => {
                meta.order_sensitive = Some(parse_bool(&raw_value)?);
            }
            "float_epsilon" => {
                let value: f64 = raw_value
                    .parse()
                    .with_context(|| format!("invalid float_epsilon: {}", raw_value))?;
                if value <= 0.0 {
                    bail!("float_epsilon must be > 0, got {}", value);
                }
                meta.float_epsilon = Some(value);
            }
            "db" => {
                meta.db = Some(raw_value);
            }
            "expect_error" => {
                meta.expect_error = Some(raw_value);
            }
            "expect_error_code" => {
                if EngineErrorCode::parse(&raw_value).is_none() {
                    bail!("unknown expect_error_code: {}", raw_value);
                }
                meta.expect_error_code = Some(raw_value);
            }
            "result_contains" => {
                meta.result_contains.push(raw_value);
            }
            "result_contains_any" => {
                meta.result_contains_any.push(raw_value);
            }
            "result_not_contains" => {
                meta.result_not_contains.push(raw_value);
            }
            "explain_contains" => {
                meta.explain_contains.push(raw_value);
            }
            "explain_not_contains" => {
                meta.explain_not_contains.push(raw_value);
            }
            "normalize_explain_timing" => {
                meta.normalize_explain_timing = parse_bool(&raw_value)?;
            }
            "catalog" => {
                bail!(
                    "@catalog metadata is no longer supported; use suite init.sql metadata instead"
                );
            }
            "tags" => {
                meta.tags = raw_value
                    .split(',')
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(ToString::to_string)
                    .collect();
            }
            "skip_result_check" => {
                meta.skip_result_check = parse_bool(&raw_value)?;
            }
            "retry_count" => {
                let value: usize = raw_value
                    .parse()
                    .with_context(|| format!("invalid retry_count: {}", raw_value))?;
                if value == 0 {
                    bail!("retry_count must be > 0, got {}", value);
                }
                meta.retry_count = Some(value);
            }
            "retry_interval_ms" => {
                let value: u64 = raw_value
                    .parse()
                    .with_context(|| format!("invalid retry_interval_ms: {}", raw_value))?;
                meta.retry_interval_ms = Some(value);
            }
            "kill_be_index" => {
                let value: usize = raw_value
                    .parse()
                    .with_context(|| format!("invalid kill_be_index: {}", raw_value))?;
                meta.kill_be_index = Some(value);
            }
            "kill_be_after_fragment_start" => {
                let value: usize = raw_value.parse().with_context(|| {
                    format!("invalid kill_be_after_fragment_start: {}", raw_value)
                })?;
                meta.kill_be_after_fragment_start = Some(value);
            }
            "fail_fragment_after_start_be_index" => {
                let value: usize = raw_value.parse().with_context(|| {
                    format!("invalid fail_fragment_after_start_be_index: {}", raw_value)
                })?;
                meta.fail_fragment_after_start_be_index = Some(value);
            }
            "network_partition_be" => {
                let value: usize = raw_value
                    .parse()
                    .with_context(|| format!("invalid network_partition_be: {}", raw_value))?;
                meta.network_partition_be = Some(value);
            }
            "heartbeat_delay_ms" => {
                let value: u64 = raw_value
                    .parse()
                    .with_context(|| format!("invalid heartbeat_delay_ms: {}", raw_value))?;
                meta.heartbeat_delay_ms = Some(value);
            }
            "restart_be_delay_ms" => {
                let value: u64 = raw_value
                    .parse()
                    .with_context(|| format!("invalid restart_be_delay_ms: {}", raw_value))?;
                meta.restart_be_delay_ms = Some(value);
            }
            "restart_fe_after_step" => {
                meta.restart_fe_after_step = parse_bool(&raw_value)?;
            }
            "drop_next_init_ack_be_index" => {
                let value = raw_value
                    .parse::<usize>()
                    .with_context(|| format!("invalid drop_next_init_ack_be_index: {raw_value}"))?;
                meta.drop_next_init_ack_be_index = Some(value);
            }
            "stop_query_control_heartbeat_be_index" => {
                let value = raw_value.parse::<usize>().with_context(|| {
                    format!("invalid stop_query_control_heartbeat_be_index: {raw_value}")
                })?;
                meta.stop_query_control_heartbeat_be_index = Some(value);
            }
            "kill_fe_after_control_ready_count" => {
                let value = raw_value.parse::<usize>().with_context(|| {
                    format!("invalid kill_fe_after_control_ready_count: {raw_value}")
                })?;
                meta.kill_fe_after_control_ready_count = Some(value);
            }
            "restart_be_after_init_ack_index" => {
                let value = raw_value.parse::<usize>().with_context(|| {
                    format!("invalid restart_be_after_init_ack_index: {raw_value}")
                })?;
                meta.restart_be_after_init_ack_index = Some(value);
            }
            "kill_query_after_control_ready_count" => {
                let value = raw_value.parse::<usize>().with_context(|| {
                    format!("invalid kill_query_after_control_ready_count: {raw_value}")
                })?;
                meta.kill_query_after_control_ready_count = Some(value);
            }
            "fail_stage_prepare_ordinal" => {
                let value = raw_value
                    .parse::<usize>()
                    .with_context(|| format!("invalid fail_stage_prepare_ordinal: {raw_value}"))?;
                meta.fail_stage_prepare_ordinal = Some(value);
            }
            "drop_next_stage_ack_be_index" => {
                let value = raw_value.parse::<usize>().with_context(|| {
                    format!("invalid drop_next_stage_ack_be_index: {raw_value}")
                })?;
                meta.drop_next_stage_ack_be_index = Some(value);
            }
            "drop_next_start_ack_be_index" => {
                let value = raw_value.parse::<usize>().with_context(|| {
                    format!("invalid drop_next_start_ack_be_index: {raw_value}")
                })?;
                meta.drop_next_start_ack_be_index = Some(value);
            }
            "suppress_start_ack_be_index" => {
                let value = raw_value
                    .parse::<usize>()
                    .with_context(|| format!("invalid suppress_start_ack_be_index: {raw_value}"))?;
                meta.suppress_start_ack_be_index = Some(value);
            }
            "drop_next_terminal_ack_be_index" => {
                let value = raw_value.parse::<usize>().with_context(|| {
                    format!("invalid drop_next_terminal_ack_be_index: {raw_value}")
                })?;
                meta.drop_next_terminal_ack_be_index = Some(value);
            }
            "drop_terminal_snapshot_stream_be_index" => {
                let value = raw_value.parse::<usize>().with_context(|| {
                    format!("invalid drop_terminal_snapshot_stream_be_index: {raw_value}")
                })?;
                meta.drop_terminal_snapshot_stream_be_index = Some(value);
            }
            "terminal_snapshot_conflict_be_index" => {
                let value = raw_value.parse::<usize>().with_context(|| {
                    format!("invalid terminal_snapshot_conflict_be_index: {raw_value}")
                })?;
                meta.terminal_snapshot_conflict_be_index = Some(value);
            }
            "kill_query_at_lifecycle_phase" => {
                meta.kill_query_at_lifecycle_phase = QueryLifecyclePhase::parse(&raw_value)
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "invalid kill_query_at_lifecycle_phase: {raw_value}; expected staging, staged, starting, running, or terminal-retained"
                        )
                    })
                    .map(Some)?;
            }
            "kill_fe_at_lifecycle_phase" => {
                let phase = QueryLifecyclePhase::parse(&raw_value).ok_or_else(|| {
                    anyhow::anyhow!(
                        "invalid kill_fe_at_lifecycle_phase: {raw_value}; expected staged or terminal-retained"
                    )
                })?;
                if !matches!(
                    phase,
                    QueryLifecyclePhase::Staged | QueryLifecyclePhase::TerminalRetained
                ) {
                    bail!(
                        "invalid kill_fe_at_lifecycle_phase: {raw_value}; expected staged or terminal-retained"
                    );
                }
                meta.kill_fe_at_lifecycle_phase = Some(phase);
            }
            "stop_query_control_heartbeat_after_stage_be_index" => {
                let value = raw_value.parse::<usize>().with_context(|| {
                    format!(
                        "invalid stop_query_control_heartbeat_after_stage_be_index: {raw_value}"
                    )
                })?;
                meta.stop_query_control_heartbeat_after_stage_be_index = Some(value);
            }
            "hold_start_until_early_ingress" => {
                meta.hold_start_until_early_ingress = parse_bool(&raw_value)?;
            }
            "query_control_fragment_backend_limit" => {
                let value = raw_value.parse::<usize>().with_context(|| {
                    format!("invalid query_control_fragment_backend_limit: {raw_value}")
                })?;
                meta.query_control_fragment_backend_limit = Some(value);
            }
            "wait_alter_column" => {
                meta.wait_alter_column = Some(raw_value);
            }
            "wait_alter_rollup" => {
                meta.wait_alter_rollup = Some(raw_value);
            }
            "wait_alter_optimize" => {
                meta.wait_alter_optimize = Some(raw_value);
            }
            "imv_equivalence_check" => {
                meta.imv_equivalence_check = Some(raw_value);
            }
            "imv_stateless_rebuild" => {
                meta.imv_stateless_rebuild = Some(parse_imv_stateless_rebuild(&raw_value)?);
            }
            "be_log_contains" => {
                meta.be_log_contains.push(raw_value);
            }
            "be_log_not_contains" => {
                if raw_value.is_empty() {
                    bail!("@be_log_not_contains pattern must not be empty");
                }
                meta.be_log_not_contains.push(raw_value);
            }
            "be_log_count_at_least" => {
                let (pattern, count) = raw_value.rsplit_once(',').ok_or_else(|| {
                    anyhow::anyhow!("@be_log_count_at_least requires <pattern>,<positive-count>")
                })?;
                let pattern = pattern.trim();
                if pattern.is_empty() {
                    bail!("@be_log_count_at_least pattern must not be empty");
                }
                let count_raw = count.trim();
                let count = count_raw.parse::<usize>().with_context(|| {
                    format!("invalid @be_log_count_at_least count: {count_raw}")
                })?;
                if count == 0 {
                    bail!("@be_log_count_at_least count must be positive");
                }
                meta.be_log_count_at_least
                    .push((pattern.to_string(), count));
            }
            "be_log_be_count_at_least" => {
                let (pattern, count) = raw_value.rsplit_once(',').ok_or_else(|| {
                    anyhow::anyhow!("@be_log_be_count_at_least requires <pattern>,<positive-count>")
                })?;
                let pattern = pattern.trim();
                if pattern.is_empty() {
                    bail!("@be_log_be_count_at_least pattern must not be empty");
                }
                let count_raw = count.trim();
                let count = count_raw.parse::<usize>().with_context(|| {
                    format!("invalid @be_log_be_count_at_least count: {count_raw}")
                })?;
                if count == 0 {
                    bail!("@be_log_be_count_at_least count must be positive");
                }
                meta.be_log_be_count_at_least
                    .push((pattern.to_string(), count));
            }
            "be_log_exact_fragment_cancellation" => {
                let count = raw_value.parse::<usize>().with_context(|| {
                    format!("invalid @be_log_exact_fragment_cancellation BE count: {raw_value}")
                })?;
                if count == 0 {
                    bail!("@be_log_exact_fragment_cancellation BE count must be positive");
                }
                meta.be_log_exact_fragment_cancellation = Some(count);
            }
            "sequential" => {
                // Parsed here but ignored in merge_meta; handled at case level.
            }
            _ => {}
        }
    }
    Ok(meta)
}

pub fn merge_meta(base: &QueryMeta, override_meta: &QueryMeta) -> QueryMeta {
    QueryMeta {
        order_sensitive: override_meta.order_sensitive.or(base.order_sensitive),
        float_epsilon: override_meta.float_epsilon.or(base.float_epsilon),
        db: override_meta.db.clone().or_else(|| base.db.clone()),
        expect_error: override_meta
            .expect_error
            .clone()
            .or_else(|| base.expect_error.clone()),
        expect_error_code: override_meta
            .expect_error_code
            .clone()
            .or_else(|| base.expect_error_code.clone()),
        result_contains: if override_meta.result_contains.is_empty() {
            base.result_contains.clone()
        } else {
            override_meta.result_contains.clone()
        },
        result_contains_any: if override_meta.result_contains_any.is_empty() {
            base.result_contains_any.clone()
        } else {
            override_meta.result_contains_any.clone()
        },
        result_not_contains: if override_meta.result_not_contains.is_empty() {
            base.result_not_contains.clone()
        } else {
            override_meta.result_not_contains.clone()
        },
        explain_contains: if override_meta.explain_contains.is_empty() {
            base.explain_contains.clone()
        } else {
            override_meta.explain_contains.clone()
        },
        explain_not_contains: if override_meta.explain_not_contains.is_empty() {
            base.explain_not_contains.clone()
        } else {
            override_meta.explain_not_contains.clone()
        },
        normalize_explain_timing: override_meta.normalize_explain_timing
            || base.normalize_explain_timing,
        tags: if override_meta.tags.is_empty() {
            base.tags.clone()
        } else {
            override_meta.tags.clone()
        },
        skip_result_check: override_meta.skip_result_check || base.skip_result_check,
        retry_count: override_meta.retry_count.or(base.retry_count),
        retry_interval_ms: override_meta.retry_interval_ms.or(base.retry_interval_ms),
        kill_be_index: override_meta.kill_be_index.or(base.kill_be_index),
        kill_be_after_fragment_start: override_meta
            .kill_be_after_fragment_start
            .or(base.kill_be_after_fragment_start),
        fail_fragment_after_start_be_index: override_meta
            .fail_fragment_after_start_be_index
            .or(base.fail_fragment_after_start_be_index),
        network_partition_be: override_meta
            .network_partition_be
            .or(base.network_partition_be),
        heartbeat_delay_ms: override_meta.heartbeat_delay_ms.or(base.heartbeat_delay_ms),
        restart_be_delay_ms: override_meta
            .restart_be_delay_ms
            .or(base.restart_be_delay_ms),
        drop_next_init_ack_be_index: override_meta
            .drop_next_init_ack_be_index
            .or(base.drop_next_init_ack_be_index),
        stop_query_control_heartbeat_be_index: override_meta
            .stop_query_control_heartbeat_be_index
            .or(base.stop_query_control_heartbeat_be_index),
        kill_fe_after_control_ready_count: override_meta
            .kill_fe_after_control_ready_count
            .or(base.kill_fe_after_control_ready_count),
        restart_be_after_init_ack_index: override_meta
            .restart_be_after_init_ack_index
            .or(base.restart_be_after_init_ack_index),
        restart_fe_after_step: override_meta.restart_fe_after_step || base.restart_fe_after_step,
        kill_query_after_control_ready_count: override_meta
            .kill_query_after_control_ready_count
            .or(base.kill_query_after_control_ready_count),
        fail_stage_prepare_ordinal: override_meta
            .fail_stage_prepare_ordinal
            .or(base.fail_stage_prepare_ordinal),
        drop_next_stage_ack_be_index: override_meta
            .drop_next_stage_ack_be_index
            .or(base.drop_next_stage_ack_be_index),
        drop_next_start_ack_be_index: override_meta
            .drop_next_start_ack_be_index
            .or(base.drop_next_start_ack_be_index),
        suppress_start_ack_be_index: override_meta
            .suppress_start_ack_be_index
            .or(base.suppress_start_ack_be_index),
        drop_next_terminal_ack_be_index: override_meta
            .drop_next_terminal_ack_be_index
            .or(base.drop_next_terminal_ack_be_index),
        drop_terminal_snapshot_stream_be_index: override_meta
            .drop_terminal_snapshot_stream_be_index
            .or(base.drop_terminal_snapshot_stream_be_index),
        terminal_snapshot_conflict_be_index: override_meta
            .terminal_snapshot_conflict_be_index
            .or(base.terminal_snapshot_conflict_be_index),
        kill_query_at_lifecycle_phase: override_meta
            .kill_query_at_lifecycle_phase
            .or(base.kill_query_at_lifecycle_phase),
        kill_fe_at_lifecycle_phase: override_meta
            .kill_fe_at_lifecycle_phase
            .or(base.kill_fe_at_lifecycle_phase),
        stop_query_control_heartbeat_after_stage_be_index: override_meta
            .stop_query_control_heartbeat_after_stage_be_index
            .or(base.stop_query_control_heartbeat_after_stage_be_index),
        hold_start_until_early_ingress: override_meta.hold_start_until_early_ingress
            || base.hold_start_until_early_ingress,
        query_control_fragment_backend_limit: override_meta
            .query_control_fragment_backend_limit
            .or(base.query_control_fragment_backend_limit),
        wait_alter_column: override_meta
            .wait_alter_column
            .clone()
            .or_else(|| base.wait_alter_column.clone()),
        wait_alter_rollup: override_meta
            .wait_alter_rollup
            .clone()
            .or_else(|| base.wait_alter_rollup.clone()),
        wait_alter_optimize: override_meta
            .wait_alter_optimize
            .clone()
            .or_else(|| base.wait_alter_optimize.clone()),
        imv_equivalence_check: override_meta
            .imv_equivalence_check
            .clone()
            .or_else(|| base.imv_equivalence_check.clone()),
        imv_stateless_rebuild: override_meta
            .imv_stateless_rebuild
            .clone()
            .or_else(|| base.imv_stateless_rebuild.clone()),
        be_log_contains: if override_meta.be_log_contains.is_empty() {
            base.be_log_contains.clone()
        } else {
            override_meta.be_log_contains.clone()
        },
        be_log_not_contains: if override_meta.be_log_not_contains.is_empty() {
            base.be_log_not_contains.clone()
        } else {
            override_meta.be_log_not_contains.clone()
        },
        be_log_count_at_least: if override_meta.be_log_count_at_least.is_empty() {
            base.be_log_count_at_least.clone()
        } else {
            override_meta.be_log_count_at_least.clone()
        },
        be_log_be_count_at_least: if override_meta.be_log_be_count_at_least.is_empty() {
            base.be_log_be_count_at_least.clone()
        } else {
            override_meta.be_log_be_count_at_least.clone()
        },
        be_log_exact_fragment_cancellation: override_meta
            .be_log_exact_fragment_cancellation
            .or(base.be_log_exact_fragment_cancellation),
    }
}

pub fn extract_meta_and_sql(lines: &[String], meta_re: &Regex) -> Result<(QueryMeta, String)> {
    let mut preface_meta_lines: Vec<String> = Vec::new();
    let mut sql_lines: Vec<String> = Vec::new();
    let mut started = false;

    for line in lines {
        let stripped = line.trim();
        if !started {
            if stripped.is_empty() {
                continue;
            }
            if stripped.starts_with("--") {
                preface_meta_lines.push(line.clone());
                continue;
            }
            started = true;
        }
        sql_lines.push(line.trim_end().to_string());
    }

    let meta = parse_meta(&preface_meta_lines, meta_re)?;
    let sql = sql_lines.join("\n").trim().to_string();
    Ok((meta, sql))
}

pub fn extract_query_number(line: &str, marker_re: &Regex) -> Option<usize> {
    let captures = marker_re.captures(line.trim())?;
    captures.get(1)?.as_str().parse::<usize>().ok()
}

pub fn load_sql_case_from_file(
    sql_path: &Path,
    meta_re: &Regex,
    marker_re: &Regex,
    variables: &HashMap<String, String>,
) -> Result<Option<SqlCase>> {
    let base_name = sql_path
        .file_stem()
        .and_then(|s| s.to_str())
        .ok_or_else(|| anyhow::anyhow!("invalid SQL file name: {}", sql_path.display()))?
        .to_string();
    let content = match fs::read_to_string(sql_path) {
        Ok(c) => c,
        Err(exc) => {
            println!(
                "Warning: failed to read SQL file {}: {}",
                sql_path.display(),
                exc
            );
            return Ok(None);
        }
    };
    let case_variables = case_placeholder_variables(variables, &base_name);
    let case_dbs = detect_case_dbs(&content, &case_variables);
    let content = substitute_placeholders(
        &content,
        &case_variables,
        &format!("{}: placeholder substitution", sql_path.display()),
    )?;

    let lines: Vec<String> = content.lines().map(ToString::to_string).collect();
    let markers: Vec<(usize, usize)> = lines
        .iter()
        .enumerate()
        .filter_map(|(idx, line)| extract_query_number(line, marker_re).map(|num| (idx, num)))
        .collect();
    // A single marker (e.g. "-- query 14") is a decorative label, not a
    // multi-step split boundary.  Only split when there are 2+ markers with
    // consecutive numbering starting from 1.
    let markers = if markers.len() <= 1 {
        Vec::new()
    } else {
        for (expected_idx, (_, query_number)) in markers.iter().enumerate() {
            let expected_query_number = expected_idx + 1;
            if *query_number != expected_query_number {
                bail!(
                    "{}: expected marker '-- query {}', found '-- query {}'",
                    sql_path.display(),
                    expected_query_number,
                    query_number
                );
            }
        }
        markers
    };

    let file_meta_lines = if let Some((first_marker_idx, _)) = markers.first() {
        lines[..*first_marker_idx].to_vec()
    } else {
        lines.clone()
    };
    let (file_meta, _) = extract_meta_and_sql(&file_meta_lines, meta_re)
        .with_context(|| format!("{}: invalid file-level metadata", sql_path.display()))?;

    // Detect case-level sequential flags from the native runner metadata and
    // from migrated legacy `-- name: ... @sequential` markers.
    let is_sequential = detect_case_sequential(&lines, &file_meta_lines, meta_re);

    let sections: Vec<(usize, Vec<String>)> = if markers.is_empty() {
        // No query markers: split by semicolons into separate statements.
        // This handles files like q39 that have multiple SQL statements
        // separated by ';' without '-- query N' markers.
        let mut stmts: Vec<(usize, Vec<String>)> = Vec::new();
        let mut current_lines: Vec<String> = Vec::new();
        let mut stmt_num = 1usize;
        for line in &lines {
            current_lines.push(line.clone());
            let trimmed = line.trim();
            if trimmed.ends_with(';') {
                if !current_lines.iter().all(|l| {
                    let t = l.trim();
                    t.is_empty() || t.starts_with("--")
                }) {
                    stmts.push((stmt_num, std::mem::take(&mut current_lines)));
                    stmt_num += 1;
                } else {
                    current_lines.clear();
                }
            }
        }
        // Remaining lines (no trailing ';')
        if !current_lines.is_empty()
            && !current_lines
                .iter()
                .all(|l| l.trim().is_empty() || l.trim().starts_with("--"))
        {
            stmts.push((stmt_num, current_lines));
        }
        if stmts.is_empty() {
            vec![(1, lines.clone())]
        } else {
            stmts
        }
    } else {
        markers
            .iter()
            .enumerate()
            .map(|(idx, (start, query_number))| {
                let end = markers
                    .get(idx + 1)
                    .map(|(next_start, _)| *next_start)
                    .unwrap_or(lines.len());
                (*query_number, lines[*start..end].to_vec())
            })
            .collect()
    };

    let mut steps = Vec::new();
    for (query_number, section) in sections {
        let section_id = if query_number == 1 {
            base_name.as_str().to_string()
        } else {
            format!("{}-{}", base_name, query_number)
        };

        let (section_meta, sql) = extract_meta_and_sql(&section, meta_re).with_context(|| {
            format!("{} ({}): invalid metadata", sql_path.display(), section_id)
        })?;

        if sql.is_empty() {
            continue;
        }

        let merged_meta = merge_meta(&file_meta, &section_meta);
        steps.push(SqlStep {
            query_number,
            sql,
            meta: merged_meta,
        });
    }

    if steps.is_empty() {
        return Ok(None);
    }

    Ok(Some(SqlCase {
        source_file: sql_path.to_path_buf(),
        case_id: base_name,
        steps,
        case_dbs,
        sequential: is_sequential,
    }))
}

pub fn parse_suite_hook_meta(
    lines: &[String],
    meta_re: &Regex,
) -> Result<(Option<String>, Option<String>)> {
    let mut catalog = None;
    let mut db = None;
    for line in lines {
        let Some((key, raw_value)) = parse_meta_line(line, meta_re) else {
            continue;
        };
        match key.as_str() {
            "catalog" => catalog = Some(raw_value),
            "db" => db = Some(raw_value),
            other => {
                bail!(
                    "unsupported suite hook metadata key '{}'; only @catalog and @db are allowed",
                    other
                );
            }
        }
    }
    Ok((catalog, db))
}

pub fn extract_suite_hook(
    lines: &[String],
    meta_re: &Regex,
) -> Result<(Option<String>, Option<String>, String)> {
    let mut preface_meta_lines: Vec<String> = Vec::new();
    let mut sql_lines: Vec<String> = Vec::new();
    let mut started = false;

    for line in lines {
        let stripped = line.trim();
        if !started {
            if stripped.is_empty() {
                continue;
            }
            if stripped.starts_with("--") {
                preface_meta_lines.push(line.clone());
                continue;
            }
            started = true;
        }
        sql_lines.push(line.trim_end().to_string());
    }

    let (catalog, db) = parse_suite_hook_meta(&preface_meta_lines, meta_re)?;
    let sql = sql_lines.join("\n").trim().to_string();
    Ok((catalog, db, sql))
}

pub fn load_suite_hook(
    hook_path: Option<&Path>,
    meta_re: &Regex,
    variables: &HashMap<String, String>,
) -> Result<Option<SuiteHook>> {
    let Some(path) = hook_path else {
        return Ok(None);
    };
    if !path.exists() {
        return Ok(None);
    }

    let content =
        fs::read_to_string(path).with_context(|| format!("read failed: {}", path.display()))?;
    let content = substitute_placeholders(
        &content,
        variables,
        &format!("{}: placeholder substitution", path.display()),
    )?;
    let lines: Vec<String> = content.lines().map(ToString::to_string).collect();
    let (catalog, db, sql) = extract_suite_hook(&lines, meta_re)
        .with_context(|| format!("{}: invalid suite hook metadata", path.display()))?;
    if sql.is_empty() {
        return Ok(None);
    }

    Ok(Some(SuiteHook {
        path: path.to_path_buf(),
        sql,
        catalog,
        db,
    }))
}

#[cfg(test)]
mod opt5_directive_tests {
    use super::*;
    use regex::Regex;

    fn meta_re() -> Regex {
        // Same regex used by the runner — see tests/sql-test-runner/src/main.rs:1600.
        Regex::new(r"^--\s*@([a-zA-Z0-9_]+)\s*=\s*(.+?)\s*$").unwrap()
    }

    #[test]
    fn parse_meta_collects_explain_contains() {
        let re = meta_re();
        let lines = vec![
            "-- @explain_contains=INNER JOIN".to_string(),
            "-- @explain_contains=stats={rows=".to_string(),
        ];
        let meta = parse_meta(&lines, &re).expect("parse ok");
        assert_eq!(
            meta.explain_contains,
            vec!["INNER JOIN".to_string(), "stats={rows=".to_string()],
        );
    }

    #[test]
    fn parse_meta_collects_explain_not_contains() {
        let re = meta_re();
        let lines = vec![
            "-- @explain_not_contains=LogicalJoin".to_string(),
            "-- @explain_not_contains=ShuffleExchange".to_string(),
        ];
        let meta = parse_meta(&lines, &re).expect("parse ok");
        assert_eq!(
            meta.explain_not_contains,
            vec!["LogicalJoin".to_string(), "ShuffleExchange".to_string()],
        );
    }

    #[test]
    fn parse_meta_parses_normalize_explain_timing() {
        let re = meta_re();
        let lines = vec!["-- @normalize_explain_timing=true".to_string()];
        let meta = parse_meta(&lines, &re).expect("parse ok");
        assert!(meta.normalize_explain_timing);
    }

    #[test]
    fn merge_meta_inherits_explain_contains_from_base_when_override_empty() {
        let base = QueryMeta {
            explain_contains: vec!["X".to_string()],
            ..QueryMeta::default()
        };
        let override_meta = QueryMeta::default();
        let merged = merge_meta(&base, &override_meta);
        assert_eq!(merged.explain_contains, vec!["X".to_string()]);
    }

    #[test]
    fn merge_meta_inherits_explain_not_contains_from_base_when_override_empty() {
        let base = QueryMeta {
            explain_not_contains: vec!["Forbidden".to_string()],
            ..QueryMeta::default()
        };
        let override_meta = QueryMeta::default();
        let merged = merge_meta(&base, &override_meta);
        assert_eq!(merged.explain_not_contains, vec!["Forbidden".to_string()]);
    }

    #[test]
    fn merge_meta_overrides_explain_not_contains_when_override_present() {
        let base = QueryMeta {
            explain_not_contains: vec!["Base".to_string()],
            ..QueryMeta::default()
        };
        let override_meta = QueryMeta {
            explain_not_contains: vec!["Override".to_string()],
            ..QueryMeta::default()
        };
        let merged = merge_meta(&base, &override_meta);
        assert_eq!(merged.explain_not_contains, vec!["Override".to_string()]);
    }

    #[test]
    fn merge_meta_normalize_timing_is_logical_or() {
        let base = QueryMeta {
            normalize_explain_timing: true,
            ..QueryMeta::default()
        };
        let override_meta = QueryMeta::default();
        let merged = merge_meta(&base, &override_meta);
        assert!(merged.normalize_explain_timing);
    }

    #[test]
    fn parse_meta_parses_fault_injection_directives() {
        let re = meta_re();
        let lines = vec![
            "-- @kill_be_index=1".to_string(),
            "-- @kill_be_after_fragment_start=2".to_string(),
            "-- @fail_fragment_after_start_be_index=0".to_string(),
            "-- @network_partition_be=2".to_string(),
            "-- @heartbeat_delay_ms=250".to_string(),
            "-- @restart_be_delay_ms=500".to_string(),
        ];

        let meta = parse_meta(&lines, &re).expect("parse ok");

        assert_eq!(meta.kill_be_index, Some(1));
        assert_eq!(meta.kill_be_after_fragment_start, Some(2));
        assert_eq!(meta.fail_fragment_after_start_be_index, Some(0));
        assert_eq!(meta.network_partition_be, Some(2));
        assert_eq!(meta.heartbeat_delay_ms, Some(250));
        assert_eq!(meta.restart_be_delay_ms, Some(500));
    }

    #[test]
    fn parse_meta_parses_post_step_frontend_restart() {
        let re = meta_re();
        let meta = parse_meta(&["-- @restart_fe_after_step=true".to_string()], &re)
            .expect("parse frontend restart directive");
        assert!(meta.restart_fe_after_step);

        let inherited = merge_meta(
            &meta,
            &QueryMeta {
                restart_fe_after_step: false,
                ..QueryMeta::default()
            },
        );
        assert!(inherited.restart_fe_after_step);
    }

    #[test]
    fn parse_meta_parses_query_lifecycle_fault_directives() {
        let re = meta_re();
        let lines = vec![
            "-- @drop_next_init_ack_be_index=1".to_string(),
            "-- @stop_query_control_heartbeat_be_index=2".to_string(),
            "-- @kill_fe_after_control_ready_count=3".to_string(),
            "-- @restart_be_after_init_ack_index=0".to_string(),
            "-- @kill_query_after_control_ready_count=2".to_string(),
            "-- @fail_stage_prepare_ordinal=2".to_string(),
            "-- @drop_next_stage_ack_be_index=1".to_string(),
            "-- @drop_next_start_ack_be_index=2".to_string(),
            "-- @suppress_start_ack_be_index=0".to_string(),
            "-- @drop_next_terminal_ack_be_index=1".to_string(),
            "-- @kill_query_at_lifecycle_phase=starting".to_string(),
            "-- @kill_fe_at_lifecycle_phase=staged".to_string(),
            "-- @stop_query_control_heartbeat_after_stage_be_index=1".to_string(),
            "-- @hold_start_until_early_ingress=true".to_string(),
            "-- @query_control_fragment_backend_limit=2".to_string(),
        ];

        let meta = parse_meta(&lines, &re).expect("parse query lifecycle fault directives");

        assert_eq!(meta.drop_next_init_ack_be_index, Some(1));
        assert_eq!(meta.stop_query_control_heartbeat_be_index, Some(2));
        assert_eq!(meta.kill_fe_after_control_ready_count, Some(3));
        assert_eq!(meta.restart_be_after_init_ack_index, Some(0));
        assert_eq!(meta.kill_query_after_control_ready_count, Some(2));
        assert_eq!(meta.fail_stage_prepare_ordinal, Some(2));
        assert_eq!(meta.drop_next_stage_ack_be_index, Some(1));
        assert_eq!(meta.drop_next_start_ack_be_index, Some(2));
        assert_eq!(meta.suppress_start_ack_be_index, Some(0));
        assert_eq!(meta.drop_next_terminal_ack_be_index, Some(1));
        assert_eq!(
            meta.kill_query_at_lifecycle_phase,
            Some(QueryLifecyclePhase::Starting)
        );
        assert_eq!(
            meta.kill_fe_at_lifecycle_phase,
            Some(QueryLifecyclePhase::Staged)
        );
        assert_eq!(
            meta.stop_query_control_heartbeat_after_stage_be_index,
            Some(1)
        );
        assert!(meta.hold_start_until_early_ingress);
        assert_eq!(meta.query_control_fragment_backend_limit, Some(2));
    }

    #[test]
    fn parse_meta_rejects_invalid_query_lifecycle_fault_number_with_context() {
        let re = meta_re();
        for (directive, expected) in [
            (
                "drop_next_init_ack_be_index=first",
                "invalid drop_next_init_ack_be_index: first",
            ),
            (
                "stop_query_control_heartbeat_be_index=last",
                "invalid stop_query_control_heartbeat_be_index: last",
            ),
            (
                "kill_fe_after_control_ready_count=all",
                "invalid kill_fe_after_control_ready_count: all",
            ),
            (
                "restart_be_after_init_ack_index=middle",
                "invalid restart_be_after_init_ack_index: middle",
            ),
            (
                "kill_query_after_control_ready_count=some",
                "invalid kill_query_after_control_ready_count: some",
            ),
            (
                "query_control_fragment_backend_limit=two",
                "invalid query_control_fragment_backend_limit: two",
            ),
            (
                "fail_stage_prepare_ordinal=second",
                "invalid fail_stage_prepare_ordinal: second",
            ),
            (
                "drop_next_stage_ack_be_index=first",
                "invalid drop_next_stage_ack_be_index: first",
            ),
            (
                "drop_next_start_ack_be_index=first",
                "invalid drop_next_start_ack_be_index: first",
            ),
            (
                "suppress_start_ack_be_index=first",
                "invalid suppress_start_ack_be_index: first",
            ),
        ] {
            let error = parse_meta(&[format!("-- @{directive}")], &re)
                .expect_err("invalid lifecycle directive must fail");
            assert!(
                format!("{error:#}").contains(expected),
                "unexpected error for {directive}: {error:#}"
            );
        }

        for directive in [
            "kill_query_at_lifecycle_phase=after-start",
            "kill_fe_at_lifecycle_phase=running",
            "hold_start_until_early_ingress=maybe",
        ] {
            assert!(
                parse_meta(&[format!("-- @{directive}")], &re).is_err(),
                "invalid lifecycle directive must fail: {directive}"
            );
        }
    }

    #[test]
    fn parse_expect_error_code_meta() {
        let re = meta_re();
        let lines = vec!["-- @expect_error_code=IcebergWriteDescriptorMismatch".to_string()];

        let meta = parse_meta(&lines, &re).expect("parse ok");

        assert_eq!(
            meta.expect_error_code,
            Some("IcebergWriteDescriptorMismatch".to_string())
        );
    }

    #[test]
    fn parse_expect_error_code_rejects_unknown_code() {
        let re = meta_re();
        let lines = vec!["-- @expect_error_code=NotARealCode".to_string()];

        let err = parse_meta(&lines, &re).expect_err("unknown code should fail");

        assert!(
            err.to_string()
                .contains("unknown expect_error_code: NotARealCode"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn merge_meta_overrides_expect_error_code_when_present() {
        let base = QueryMeta {
            expect_error_code: Some("CommitUnknown".to_string()),
            ..QueryMeta::default()
        };
        let override_meta = QueryMeta {
            expect_error_code: Some("ProtocolDecodeError".to_string()),
            ..QueryMeta::default()
        };

        let merged = merge_meta(&base, &override_meta);

        assert_eq!(
            merged.expect_error_code,
            Some("ProtocolDecodeError".to_string())
        );
    }

    #[test]
    fn merge_meta_inherits_expect_error_code_when_override_empty() {
        let base = QueryMeta {
            expect_error_code: Some("CommitUnknown".to_string()),
            ..QueryMeta::default()
        };

        let merged = merge_meta(&base, &QueryMeta::default());

        assert_eq!(merged.expect_error_code, Some("CommitUnknown".to_string()));
    }

    #[test]
    fn parse_meta_collects_imv_equivalence_check() {
        let re = meta_re();
        let lines = vec!["-- @imv_equivalence_check=orders_mv".to_string()];
        let meta = parse_meta(&lines, &re).unwrap();
        assert_eq!(meta.imv_equivalence_check.as_deref(), Some("orders_mv"));
    }

    #[test]
    fn parse_meta_collects_imv_stateless_rebuild_with_default_level() {
        let re = meta_re();
        let lines = vec!["-- @imv_stateless_rebuild=orders_mv".to_string()];
        let meta = parse_meta(&lines, &re).expect("parse");
        let directive = meta.imv_stateless_rebuild.as_ref().expect("directive");
        assert_eq!(directive.mv, "orders_mv");
        assert_eq!(directive.level, ImvStatelessLevel::Package);
    }

    #[test]
    fn parse_meta_collects_imv_stateless_rebuild_with_explicit_level() {
        let re = meta_re();
        let lines = vec!["-- @imv_stateless_rebuild=orders_mv,level=baseline".to_string()];
        let meta = parse_meta(&lines, &re).expect("parse");
        let directive = meta.imv_stateless_rebuild.as_ref().expect("directive");
        assert_eq!(directive.mv, "orders_mv");
        assert_eq!(directive.level, ImvStatelessLevel::Baseline);
    }

    #[test]
    fn parse_meta_collects_imv_stateless_rebuild_with_catalog_and_level() {
        let re = meta_re();
        let lines =
            vec!["-- @imv_stateless_rebuild=orders_mv,catalog=mv_ice_x,level=package".to_string()];
        let meta = parse_meta(&lines, &re).expect("parse");
        let d = meta.imv_stateless_rebuild.as_ref().expect("directive");
        assert_eq!(d.mv, "orders_mv");
        assert_eq!(d.level, ImvStatelessLevel::Package);
        assert_eq!(d.catalog.as_deref(), Some("mv_ice_x"));
    }

    #[test]
    fn parse_meta_reports_invalid_fault_number_with_context() {
        let re = meta_re();
        let lines = vec!["-- @restart_be_delay_ms=soon".to_string()];

        let err = parse_meta(&lines, &re).expect_err("invalid number should fail");

        assert!(
            format!("{err:#}").contains("invalid restart_be_delay_ms: soon"),
            "unexpected error: {err:#}"
        );
    }

    #[test]
    fn merge_meta_inherits_fault_directives_from_base() {
        let base = QueryMeta {
            kill_be_index: Some(1),
            network_partition_be: Some(2),
            heartbeat_delay_ms: Some(250),
            restart_be_delay_ms: Some(500),
            ..QueryMeta::default()
        };
        let merged = merge_meta(&base, &QueryMeta::default());

        assert_eq!(merged.kill_be_index, Some(1));
        assert_eq!(merged.network_partition_be, Some(2));
        assert_eq!(merged.heartbeat_delay_ms, Some(250));
        assert_eq!(merged.restart_be_delay_ms, Some(500));
    }

    #[test]
    fn merge_meta_overrides_fault_directives_when_present() {
        let base = QueryMeta {
            kill_be_index: Some(1),
            network_partition_be: Some(2),
            heartbeat_delay_ms: Some(250),
            restart_be_delay_ms: Some(500),
            ..QueryMeta::default()
        };
        let override_meta = QueryMeta {
            kill_be_index: Some(3),
            network_partition_be: Some(4),
            heartbeat_delay_ms: Some(750),
            restart_be_delay_ms: Some(1000),
            ..QueryMeta::default()
        };
        let merged = merge_meta(&base, &override_meta);

        assert_eq!(merged.kill_be_index, Some(3));
        assert_eq!(merged.network_partition_be, Some(4));
        assert_eq!(merged.heartbeat_delay_ms, Some(750));
        assert_eq!(merged.restart_be_delay_ms, Some(1000));
    }

    #[test]
    fn merge_meta_inherits_and_overrides_query_lifecycle_fault_directives() {
        let base = QueryMeta {
            drop_next_init_ack_be_index: Some(0),
            stop_query_control_heartbeat_be_index: Some(1),
            kill_fe_after_control_ready_count: Some(2),
            restart_be_after_init_ack_index: Some(0),
            kill_query_after_control_ready_count: Some(1),
            fail_stage_prepare_ordinal: Some(2),
            drop_next_stage_ack_be_index: Some(0),
            drop_next_start_ack_be_index: Some(1),
            suppress_start_ack_be_index: Some(2),
            drop_next_terminal_ack_be_index: Some(1),
            kill_query_at_lifecycle_phase: Some(QueryLifecyclePhase::Staging),
            kill_fe_at_lifecycle_phase: Some(QueryLifecyclePhase::Staged),
            stop_query_control_heartbeat_after_stage_be_index: Some(1),
            hold_start_until_early_ingress: true,
            query_control_fragment_backend_limit: Some(2),
            ..QueryMeta::default()
        };
        let inherited = merge_meta(&base, &QueryMeta::default());
        assert_eq!(inherited.drop_next_init_ack_be_index, Some(0));
        assert_eq!(inherited.stop_query_control_heartbeat_be_index, Some(1));
        assert_eq!(inherited.kill_fe_after_control_ready_count, Some(2));
        assert_eq!(inherited.restart_be_after_init_ack_index, Some(0));
        assert_eq!(inherited.kill_query_after_control_ready_count, Some(1));
        assert_eq!(inherited.fail_stage_prepare_ordinal, Some(2));
        assert_eq!(inherited.drop_next_stage_ack_be_index, Some(0));
        assert_eq!(inherited.drop_next_start_ack_be_index, Some(1));
        assert_eq!(inherited.suppress_start_ack_be_index, Some(2));
        assert_eq!(inherited.drop_next_terminal_ack_be_index, Some(1));
        assert_eq!(
            inherited.kill_query_at_lifecycle_phase,
            Some(QueryLifecyclePhase::Staging)
        );
        assert_eq!(
            inherited.kill_fe_at_lifecycle_phase,
            Some(QueryLifecyclePhase::Staged)
        );
        assert_eq!(
            inherited.stop_query_control_heartbeat_after_stage_be_index,
            Some(1)
        );
        assert!(inherited.hold_start_until_early_ingress);
        assert_eq!(inherited.query_control_fragment_backend_limit, Some(2));

        let override_meta = QueryMeta {
            drop_next_init_ack_be_index: Some(2),
            stop_query_control_heartbeat_be_index: Some(0),
            kill_fe_after_control_ready_count: Some(3),
            restart_be_after_init_ack_index: Some(2),
            kill_query_after_control_ready_count: Some(3),
            fail_stage_prepare_ordinal: Some(4),
            drop_next_stage_ack_be_index: Some(2),
            drop_next_start_ack_be_index: Some(0),
            suppress_start_ack_be_index: Some(1),
            drop_next_terminal_ack_be_index: Some(0),
            kill_query_at_lifecycle_phase: Some(QueryLifecyclePhase::Running),
            stop_query_control_heartbeat_after_stage_be_index: Some(2),
            query_control_fragment_backend_limit: Some(1),
            ..QueryMeta::default()
        };
        let overridden = merge_meta(&base, &override_meta);
        assert_eq!(overridden.drop_next_init_ack_be_index, Some(2));
        assert_eq!(overridden.stop_query_control_heartbeat_be_index, Some(0));
        assert_eq!(overridden.kill_fe_after_control_ready_count, Some(3));
        assert_eq!(overridden.restart_be_after_init_ack_index, Some(2));
        assert_eq!(overridden.kill_query_after_control_ready_count, Some(3));
        assert_eq!(overridden.fail_stage_prepare_ordinal, Some(4));
        assert_eq!(overridden.drop_next_stage_ack_be_index, Some(2));
        assert_eq!(overridden.drop_next_start_ack_be_index, Some(0));
        assert_eq!(overridden.suppress_start_ack_be_index, Some(1));
        assert_eq!(overridden.drop_next_terminal_ack_be_index, Some(0));
        assert_eq!(
            overridden.kill_query_at_lifecycle_phase,
            Some(QueryLifecyclePhase::Running)
        );
        assert_eq!(
            overridden.kill_fe_at_lifecycle_phase,
            Some(QueryLifecyclePhase::Staged)
        );
        assert_eq!(
            overridden.stop_query_control_heartbeat_after_stage_be_index,
            Some(2)
        );
        assert!(overridden.hold_start_until_early_ingress);
        assert_eq!(overridden.query_control_fragment_backend_limit, Some(1));
    }

    #[test]
    fn be_log_directive_parser_collects_log_directives() {
        let re = meta_re();
        let lines = vec![
            "-- @be_log_contains=be_log_ingress method=exec_batch_plan_fragments".to_string(),
            "-- @be_log_not_contains=NOVAROCKS_CONNECTOR_WRITER_OPENED".to_string(),
            "-- @be_log_count_at_least=runtime_filter_receive,2".to_string(),
            "-- @be_log_be_count_at_least=exchange_receive eos=true,2".to_string(),
            "-- @be_log_exact_fragment_cancellation=3".to_string(),
        ];

        let meta = parse_meta(&lines, &re).expect("parse BE log directives");

        assert_eq!(
            meta.be_log_contains,
            vec!["be_log_ingress method=exec_batch_plan_fragments".to_string()]
        );
        assert_eq!(
            meta.be_log_not_contains,
            vec!["NOVAROCKS_CONNECTOR_WRITER_OPENED".to_string()]
        );
        assert_eq!(
            meta.be_log_count_at_least,
            vec![("runtime_filter_receive".to_string(), 2)]
        );
        assert_eq!(
            meta.be_log_be_count_at_least,
            vec![("exchange_receive eos=true".to_string(), 2)]
        );
        assert_eq!(meta.be_log_exact_fragment_cancellation, Some(3));
    }

    #[test]
    fn be_log_directive_parser_rejects_invalid_log_count() {
        let re = meta_re();
        let lines = vec!["-- @be_log_count_at_least=runtime_filter_receive,zero".to_string()];

        let error = parse_meta(&lines, &re).expect_err("invalid count must fail");

        assert!(
            format!("{error:#}").contains("invalid @be_log_count_at_least count: zero"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn exact_fragment_cancellation_requires_positive_be_count() {
        let re = meta_re();
        let lines = vec!["-- @be_log_exact_fragment_cancellation=0".to_string()];

        let error = parse_meta(&lines, &re).expect_err("zero BE coverage must fail");

        assert!(
            format!("{error:#}").contains("BE count must be positive"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn expected_error_parser_preserves_be_log_directives_for_post_error_checks() {
        let re = meta_re();
        let lines = vec![
            "-- @expect_error=planned rejection".to_string(),
            "-- @be_log_contains=be_log_ingress rejected".to_string(),
        ];

        let meta = parse_meta(&lines, &re).expect("parse expected error with BE log directives");

        assert_eq!(meta.expect_error.as_deref(), Some("planned rejection"));
        assert!(meta.has_be_log_directives());
        assert_eq!(
            meta.be_log_contains,
            vec!["be_log_ingress rejected".to_string()]
        );
    }
}
