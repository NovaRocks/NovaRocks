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
use crate::engine_error_codes::EngineErrorCode;
use crate::production_sql_error_descriptors;
use crate::sql_error_codes::{SqlErrorDescriptor, SqlErrorPhase, lookup_sql_error_descriptor};
use crate::types::*;
use anyhow::{Context, Result, bail};
use novarocks_failpoint::{
    cleanup_fault_directive_names, parse_cleanup_fault_directive, parse_runner_rfo_kind,
    runner_rfo_kind_names,
};
use regex::Regex;
use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::path::Path;

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

fn parse_publication_catalog_fault(raw: &str) -> anyhow::Result<PublicationCatalogFaultDirective> {
    let (action, fault) = raw
        .split_once(',')
        .ok_or_else(|| anyhow::anyhow!("@publication_catalog_fault requires <action>,<fault>"))?;
    let action = match action.trim() {
        "stage-create" => PublicationCatalogAction::StageCreate,
        "table-commit" => PublicationCatalogAction::TableCommit,
        "namespace-list" => PublicationCatalogAction::NamespaceList,
        "table-load" => PublicationCatalogAction::TableLoad,
        other => anyhow::bail!(
            "invalid @publication_catalog_fault action `{other}`; expected stage-create, table-commit, namespace-list, table-load"
        ),
    };
    let fault = match fault.trim() {
        "before-dispatch" => PublicationCatalogFault::BeforeDispatch,
        "before-dispatch-hold-for-concurrent-shell" => {
            PublicationCatalogFault::BeforeDispatchHoldForConcurrentShell
        }
        "after-commit-before-response" => PublicationCatalogFault::AfterCommitBeforeResponse,
        "after-commit-hold-for-frontend-kill" => {
            PublicationCatalogFault::AfterCommitHoldForFrontendKill
        }
        "incomplete-discovery" => PublicationCatalogFault::IncompleteDiscovery,
        "corrupt-package" => PublicationCatalogFault::CorruptPackage,
        other => anyhow::bail!(
            "invalid @publication_catalog_fault fault `{other}`; expected before-dispatch, before-dispatch-hold-for-concurrent-shell, after-commit-before-response, after-commit-hold-for-frontend-kill, incomplete-discovery, corrupt-package"
        ),
    };
    let compatible = matches!(
        (action, fault),
        (
            PublicationCatalogAction::StageCreate | PublicationCatalogAction::TableCommit,
            PublicationCatalogFault::BeforeDispatch
                | PublicationCatalogFault::AfterCommitBeforeResponse
                | PublicationCatalogFault::AfterCommitHoldForFrontendKill
        ) | (
            PublicationCatalogAction::TableCommit,
            PublicationCatalogFault::BeforeDispatchHoldForConcurrentShell
        ) | (
            PublicationCatalogAction::NamespaceList,
            PublicationCatalogFault::IncompleteDiscovery
        ) | (
            PublicationCatalogAction::TableLoad,
            PublicationCatalogFault::CorruptPackage
        )
    );
    if !compatible {
        anyhow::bail!(
            "invalid @publication_catalog_fault action/fault combination `{}` / `{}`",
            action.as_str(),
            fault.as_str()
        );
    }
    Ok(PublicationCatalogFaultDirective { action, fault })
}

fn parse_query_lifecycle_fault(raw: &str) -> anyhow::Result<QueryLifecycleFaultDirective> {
    let (kind, index) = raw.split_once(',').ok_or_else(|| {
        anyhow::anyhow!("@query_lifecycle_fault requires <kind>,<be_index>; received {raw:?}")
    })?;
    let kind = parse_runner_rfo_kind(kind.trim()).ok_or_else(|| {
        anyhow::anyhow!(
            "invalid query_lifecycle_fault kind {:?}; expected one of {}",
            kind.trim(),
            runner_rfo_kind_names().collect::<Vec<_>>().join(", ")
        )
    })?;
    let be_index = index
        .trim()
        .parse::<usize>()
        .with_context(|| format!("invalid query_lifecycle_fault BE index {:?}", index.trim()))?;
    Ok(QueryLifecycleFaultDirective { kind, be_index })
}

fn parse_kill_be_at_lifecycle_phase(raw: &str) -> anyhow::Result<KillBeAtLifecyclePhaseDirective> {
    let (be_index, phase) = raw.split_once(',').ok_or_else(|| {
        anyhow::anyhow!("@kill_be_at_lifecycle_phase requires <be_index>,<phase>; received {raw:?}")
    })?;
    let be_index = be_index
        .trim()
        .parse::<usize>()
        .with_context(|| format!("invalid kill_be_at_lifecycle_phase BE index {be_index:?}"))?;
    let phase = QueryLifecyclePhase::parse(phase.trim()).ok_or_else(|| {
        anyhow::anyhow!(
            "invalid kill_be_at_lifecycle_phase phase {phase:?}; expected terminal-retained"
        )
    })?;
    if phase != QueryLifecyclePhase::TerminalRetained {
        bail!(
            "invalid kill_be_at_lifecycle_phase phase {}; expected terminal-retained",
            phase.as_str()
        );
    }
    Ok(KillBeAtLifecyclePhaseDirective { be_index, phase })
}

fn parse_kill_be_after_be_log_directive(raw: &str) -> anyhow::Result<KillBeAfterBeLogDirective> {
    let (be_index, pattern) = raw.split_once(',').ok_or_else(|| {
        anyhow::anyhow!(
            "@kill_be_after_be_log_contains requires <be_index>,<pattern>; received {raw:?}"
        )
    })?;
    let be_index = be_index
        .trim()
        .parse::<usize>()
        .with_context(|| format!("invalid kill_be_after_be_log_contains BE index {be_index:?}"))?;
    let pattern = pattern.trim();
    if pattern.is_empty() {
        bail!("@kill_be_after_be_log_contains pattern must not be empty");
    }
    Ok(KillBeAfterBeLogDirective {
        be_index,
        pattern: pattern.to_string(),
    })
}

fn parse_lifecycle_metric_delta(raw: &str) -> anyhow::Result<QueryLifecycleMetricDeltaExpectation> {
    let (metric, delta) = raw.split_once(',').ok_or_else(|| {
        anyhow::anyhow!(
            "@expect_lifecycle_metric_delta requires <metric>,<signed_delta>; received {raw:?}"
        )
    })?;
    let metric = metric.trim();
    if metric.is_empty()
        || !metric
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        bail!("invalid lifecycle metric name {metric:?}");
    }
    let delta = delta
        .trim()
        .parse::<i64>()
        .with_context(|| format!("invalid lifecycle metric delta {:?}", delta.trim()))?;
    Ok(QueryLifecycleMetricDeltaExpectation {
        metric: metric.to_string(),
        delta,
    })
}

fn parse_runtime_filter_total_at_least(
    raw: &str,
) -> anyhow::Result<RuntimeFilterTotalAtLeastExpectation> {
    let (metric, value) = raw.split_once(',').ok_or_else(|| {
        anyhow::anyhow!(
            "@expect_runtime_filter_total_at_least requires <metric>,<positive-value>; received {raw:?}"
        )
    })?;
    let metric = RuntimeFilterTotalMetric::parse(metric.trim()).ok_or_else(|| {
        anyhow::anyhow!(
            "invalid runtime-filter total metric {:?}; expected one of {}",
            metric.trim(),
            RuntimeFilterTotalMetric::valid_names()
        )
    })?;
    let value = value.trim().parse::<u64>().with_context(|| {
        format!(
            "invalid runtime-filter total lower bound {:?}",
            value.trim()
        )
    })?;
    if value == 0 {
        bail!("runtime-filter total lower bound must be positive");
    }
    Ok(RuntimeFilterTotalAtLeastExpectation { metric, value })
}

fn structured_assertion_mut(meta: &mut QueryMeta) -> &mut QueryLifecycleStructuredAssertion {
    meta.query_lifecycle_structured_assertion
        .get_or_insert_with(|| QueryLifecycleStructuredAssertion {
            error_source: None,
            metric_deltas: Vec::new(),
            runtime_filter_availability: None,
            runtime_filter_details: Vec::new(),
            runtime_filter_totals_at_least: Vec::new(),
        })
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
    parse_meta_with_sql_error_descriptors(lines, meta_re, production_sql_error_descriptors())
}

fn parse_meta_with_sql_error_descriptors(
    lines: &[String],
    meta_re: &Regex,
    sql_error_descriptors: &[SqlErrorDescriptor],
) -> Result<QueryMeta> {
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
            "expect_sql_code" => {
                if lookup_sql_error_descriptor(sql_error_descriptors, &raw_value).is_none() {
                    bail!("unknown expect_sql_code: {}", raw_value);
                }
                meta.expect_sql_code = Some(raw_value);
            }
            "expect_sql_phase" => {
                meta.expect_sql_phase = Some(SqlErrorPhase::parse(&raw_value).ok_or_else(|| {
                    anyhow::anyhow!(
                        "invalid expect_sql_phase: {raw_value}; expected Lex, Parse, Validate, Analyze, or Admit"
                    )
                })?);
            }
            "expect_error_at" => {
                meta.expect_error_at =
                    Some(SqlErrorLocation::parse(&raw_value).ok_or_else(|| {
                        anyhow::anyhow!(
                            "invalid expect_error_at: {raw_value}; expected positive <line>:<col>"
                        )
                    })?);
            }
            "expect_error_tier" => {
                meta.expect_error_tier =
                    Some(SqlErrorTier::parse(&raw_value).ok_or_else(|| {
                        anyhow::anyhow!(
                            "invalid expect_error_tier: {raw_value}; expected drift or target"
                        )
                    })?);
            }
            "nova_extension" => {
                if raw_value.is_empty() {
                    bail!("@nova_extension must not be empty");
                }
                meta.nova_extension = Some(raw_value);
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
            "cleanup_fault" => {
                if parse_cleanup_fault_directive(&raw_value).is_none() {
                    bail!(
                        "invalid cleanup_fault: {raw_value}; expected one of {}",
                        cleanup_fault_directive_names()
                            .collect::<Vec<_>>()
                            .join(", ")
                    );
                }
                meta.cleanup_fault = Some(raw_value);
            }
            "publication_catalog_fault" => {
                meta.publication_catalog_fault = Some(parse_publication_catalog_fault(&raw_value)?);
            }
            "publication_catalog_concurrent_shell" => {
                if raw_value.is_empty() {
                    bail!("publication_catalog_concurrent_shell must not be empty");
                }
                meta.publication_catalog_concurrent_shell = Some(raw_value);
            }
            "kill_fe_after_mv_known_committed_before_projector_cas" => {
                meta.kill_fe_after_mv_known_committed_before_projector_cas =
                    parse_bool(&raw_value)?;
            }
            "restart_be_after_establish_context_index" => {
                let value = raw_value.parse::<usize>().with_context(|| {
                    format!("invalid restart_be_after_establish_context_index: {raw_value}")
                })?;
                meta.restart_be_after_establish_context_index = Some(value);
            }
            "kill_be_after_be_log_contains" => {
                meta.kill_be_after_be_log_contains =
                    Some(parse_kill_be_after_be_log_directive(&raw_value)?);
            }
            "kill_fe_after_be_log_contains" => {
                if raw_value.is_empty() {
                    bail!("kill_fe_after_be_log_contains must not be empty");
                }
                meta.kill_fe_after_be_log_contains = Some(raw_value);
            }
            "kill_query_after_be_log_contains" => {
                if raw_value.is_empty() {
                    bail!("kill_query_after_be_log_contains must not be empty");
                }
                meta.kill_query_after_be_log_contains = Some(raw_value);
            }
            "terminal_snapshot_conflict_be_index" => {
                let value = raw_value.parse::<usize>().with_context(|| {
                    format!("invalid terminal_snapshot_conflict_be_index: {raw_value}")
                })?;
                meta.terminal_snapshot_conflict_be_index = Some(value);
            }
            "query_lifecycle_fault" => {
                let fault = parse_query_lifecycle_fault(&raw_value)?;
                if meta.query_lifecycle_fault.is_none() {
                    meta.query_lifecycle_fault = Some(fault);
                }
                meta.query_lifecycle_faults.push(fault);
            }
            "expect_lifecycle_error_source" => {
                let source = QueryLifecycleErrorSource::parse(&raw_value).ok_or_else(|| {
                    anyhow::anyhow!(
                        "invalid expect_lifecycle_error_source: {raw_value}; expected backend-attestation, frontend-liveness, or no-outcome"
                    )
                })?;
                structured_assertion_mut(&mut meta).error_source = Some(source);
            }
            "expect_lifecycle_metric_delta" => {
                structured_assertion_mut(&mut meta)
                    .metric_deltas
                    .push(parse_lifecycle_metric_delta(&raw_value)?);
            }
            "expect_runtime_filter_available" => {
                let availability = RuntimeFilterAvailabilityExpectation::parse(&raw_value)
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "invalid expect_runtime_filter_available: {raw_value}; expected available"
                        )
                    })?;
                structured_assertion_mut(&mut meta).runtime_filter_availability =
                    Some(availability);
            }
            "expect_runtime_filter_detail" => {
                let detail =
                    RuntimeFilterDetailExpectation::parse(&raw_value).ok_or_else(|| {
                        anyhow::anyhow!(
                            "invalid expect_runtime_filter_detail: {raw_value}; expected one of {}",
                            RuntimeFilterDetailExpectation::valid_names()
                        )
                    })?;
                structured_assertion_mut(&mut meta)
                    .runtime_filter_details
                    .push(detail);
            }
            "expect_runtime_filter_total_at_least" => {
                structured_assertion_mut(&mut meta)
                    .runtime_filter_totals_at_least
                    .push(parse_runtime_filter_total_at_least(&raw_value)?);
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
            "kill_be_at_lifecycle_phase" => {
                meta.kill_be_at_lifecycle_phase =
                    Some(parse_kill_be_at_lifecycle_phase(&raw_value)?);
            }
            "query_control_fragment_backend_limit" => {
                let value = raw_value.parse::<usize>().with_context(|| {
                    format!("invalid query_control_fragment_backend_limit: {raw_value}")
                })?;
                meta.query_control_fragment_backend_limit = Some(value);
            }
            "iceberg_orphan_fixture" => {
                if raw_value.is_empty() {
                    bail!("@iceberg_orphan_fixture requires <namespace>.<table>");
                }
                meta.iceberg_orphan_fixture = Some(raw_value);
            }
            "iceberg_orphan_fixture_absent" => {
                meta.iceberg_orphan_fixture_absent = parse_bool(&raw_value)?;
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
            "imv_accelerator_wipe_restart" => {
                let mut directive = parse_imv_stateless_rebuild(&raw_value)?;
                directive.level = ImvStatelessLevel::Full;
                meta.imv_accelerator_wipe_restart = Some(directive);
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
            "sequential" => {
                // Parsed here but ignored in merge_meta; handled at case level.
            }
            // A directive this parser does not know is refused rather than
            // ignored.
            //
            // Silently dropping one is how a case comes to assert nothing: it
            // still reads as though it arms a fault or checks evidence, the
            // runner does neither, and the case passes on whatever the engine
            // happened to do. That is the exact shape of the failures this
            // suite exists to catch, so it must not be reachable through a
            // typo or a directive whose implementation was retired out from
            // under a case.
            other => bail!(
                "unknown directive @{other}: a directive the runner does not \
                 implement would assert nothing, so it is refused rather than \
                 ignored"
            ),
        }
    }
    Ok(meta)
}

fn validate_sql_error_expectations(
    meta: &QueryMeta,
    sql_error_descriptors: &[SqlErrorDescriptor],
) -> Result<()> {
    if let Some(code) = meta.expect_sql_code.as_deref()
        && lookup_sql_error_descriptor(sql_error_descriptors, code).is_none()
    {
        bail!("unknown expect_sql_code: {code}");
    }

    if meta.expect_sql_phase.is_some() && meta.expect_sql_code.is_none() {
        bail!("@expect_sql_phase requires @expect_sql_code");
    }

    if meta.sql_error_tier() == SqlErrorTier::Target {
        if meta.expect_sql_code.is_none() {
            bail!("@expect_error_tier=target requires @expect_sql_code");
        }
        if meta.expect_error_at.is_none() {
            bail!("@expect_error_tier=target requires @expect_error_at");
        }
    }

    Ok(())
}

fn merge_lifecycle_structured_assertion(
    base: Option<&QueryLifecycleStructuredAssertion>,
    override_meta: Option<&QueryLifecycleStructuredAssertion>,
) -> Option<QueryLifecycleStructuredAssertion> {
    let base = base?;
    let override_meta = override_meta.unwrap_or(base);
    Some(QueryLifecycleStructuredAssertion {
        error_source: override_meta
            .error_source
            .clone()
            .or_else(|| base.error_source.clone()),
        metric_deltas: if override_meta.metric_deltas.is_empty() {
            base.metric_deltas.clone()
        } else {
            override_meta.metric_deltas.clone()
        },
        runtime_filter_availability: override_meta
            .runtime_filter_availability
            .or(base.runtime_filter_availability),
        runtime_filter_details: if override_meta.runtime_filter_details.is_empty() {
            base.runtime_filter_details.clone()
        } else {
            override_meta.runtime_filter_details.clone()
        },
        runtime_filter_totals_at_least: if override_meta.runtime_filter_totals_at_least.is_empty() {
            base.runtime_filter_totals_at_least.clone()
        } else {
            override_meta.runtime_filter_totals_at_least.clone()
        },
    })
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
        expect_sql_code: override_meta
            .expect_sql_code
            .clone()
            .or_else(|| base.expect_sql_code.clone()),
        expect_sql_phase: override_meta.expect_sql_phase.or(base.expect_sql_phase),
        expect_error_at: override_meta.expect_error_at.or(base.expect_error_at),
        expect_error_tier: override_meta.expect_error_tier.or(base.expect_error_tier),
        nova_extension: override_meta
            .nova_extension
            .clone()
            .or_else(|| base.nova_extension.clone()),
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
        heartbeat_delay_ms: override_meta.heartbeat_delay_ms.or(base.heartbeat_delay_ms),
        restart_be_delay_ms: override_meta
            .restart_be_delay_ms
            .or(base.restart_be_delay_ms),
        kill_fe_after_mv_known_committed_before_projector_cas: override_meta
            .kill_fe_after_mv_known_committed_before_projector_cas
            || base.kill_fe_after_mv_known_committed_before_projector_cas,
        restart_be_after_establish_context_index: override_meta
            .restart_be_after_establish_context_index
            .or(base.restart_be_after_establish_context_index),
        restart_fe_after_step: override_meta.restart_fe_after_step || base.restart_fe_after_step,
        cleanup_fault: override_meta
            .cleanup_fault
            .clone()
            .or_else(|| base.cleanup_fault.clone()),
        publication_catalog_fault: override_meta
            .publication_catalog_fault
            .or(base.publication_catalog_fault),
        publication_catalog_concurrent_shell: override_meta
            .publication_catalog_concurrent_shell
            .clone()
            .or_else(|| base.publication_catalog_concurrent_shell.clone()),
        kill_query_after_be_log_contains: override_meta
            .kill_query_after_be_log_contains
            .clone()
            .or_else(|| base.kill_query_after_be_log_contains.clone()),
        kill_be_after_be_log_contains: override_meta
            .kill_be_after_be_log_contains
            .clone()
            .or_else(|| base.kill_be_after_be_log_contains.clone()),
        kill_fe_after_be_log_contains: override_meta
            .kill_fe_after_be_log_contains
            .clone()
            .or_else(|| base.kill_fe_after_be_log_contains.clone()),
        terminal_snapshot_conflict_be_index: override_meta
            .terminal_snapshot_conflict_be_index
            .or(base.terminal_snapshot_conflict_be_index),
        query_lifecycle_fault: override_meta
            .query_lifecycle_fault
            .or(base.query_lifecycle_fault),
        query_lifecycle_faults: if override_meta.query_lifecycle_faults.is_empty() {
            base.query_lifecycle_faults.clone()
        } else {
            override_meta.query_lifecycle_faults.clone()
        },
        query_lifecycle_structured_assertion: match (
            base.query_lifecycle_structured_assertion.as_ref(),
            override_meta.query_lifecycle_structured_assertion.as_ref(),
        ) {
            (None, None) => None,
            (None, Some(override_assertion)) => Some(override_assertion.clone()),
            (Some(base_assertion), override_assertion) => {
                merge_lifecycle_structured_assertion(Some(base_assertion), override_assertion)
            }
        },
        kill_query_at_lifecycle_phase: override_meta
            .kill_query_at_lifecycle_phase
            .or(base.kill_query_at_lifecycle_phase),
        kill_be_at_lifecycle_phase: override_meta
            .kill_be_at_lifecycle_phase
            .or(base.kill_be_at_lifecycle_phase),
        query_control_fragment_backend_limit: override_meta
            .query_control_fragment_backend_limit
            .or(base.query_control_fragment_backend_limit),
        iceberg_orphan_fixture: override_meta
            .iceberg_orphan_fixture
            .clone()
            .or_else(|| base.iceberg_orphan_fixture.clone()),
        iceberg_orphan_fixture_absent: override_meta.iceberg_orphan_fixture_absent
            || base.iceberg_orphan_fixture_absent,
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
        imv_accelerator_wipe_restart: override_meta
            .imv_accelerator_wipe_restart
            .clone()
            .or_else(|| base.imv_accelerator_wipe_restart.clone()),
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
    let case_id = sql_path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .ok_or_else(|| anyhow::anyhow!("invalid SQL file name: {}", sql_path.display()))?;
    let case_variables = case_placeholder_variables(variables, case_id);
    load_sql_case_from_file_with_variables(sql_path, meta_re, marker_re, &case_variables)
}

/// Load a case while preserving supplied placeholder tokens. This is used by
/// read-only source inventories whose output must not contain run-specific IDs.
pub fn load_sql_case_from_file_preserving_placeholders(
    sql_path: &Path,
    meta_re: &Regex,
    marker_re: &Regex,
    variables: &HashMap<String, String>,
) -> Result<Option<SqlCase>> {
    load_sql_case_from_file_with_variables(sql_path, meta_re, marker_re, variables)
}

fn load_sql_case_from_file_with_variables(
    sql_path: &Path,
    meta_re: &Regex,
    marker_re: &Regex,
    case_variables: &HashMap<String, String>,
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
    let case_dbs = detect_case_dbs(&content, case_variables);
    let content = substitute_placeholders(
        &content,
        case_variables,
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
        validate_sql_error_expectations(&merged_meta, production_sql_error_descriptors())
            .with_context(|| {
                format!(
                    "{} ({}): invalid SQL error expectation",
                    sql_path.display(),
                    section_id
                )
            })?;
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
        // Same regex used by the runner — see tests/sql/runner/src/main.rs:1600.
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
    fn parse_meta_parses_typed_publication_catalog_fault() {
        let re = meta_re();
        let lines = vec![
            "-- @publication_catalog_fault=table-commit,after-commit-before-response".to_string(),
        ];
        let meta = parse_meta(&lines, &re).expect("parse publication catalog fault");
        assert_eq!(
            meta.publication_catalog_fault,
            Some(PublicationCatalogFaultDirective {
                action: PublicationCatalogAction::TableCommit,
                fault: PublicationCatalogFault::AfterCommitBeforeResponse,
            })
        );
    }

    #[test]
    fn parse_meta_parses_catalog_discovery_and_package_read_faults() {
        let re = meta_re();
        for (directive, expected) in [
            (
                "-- @publication_catalog_fault=namespace-list,incomplete-discovery",
                PublicationCatalogFaultDirective {
                    action: PublicationCatalogAction::NamespaceList,
                    fault: PublicationCatalogFault::IncompleteDiscovery,
                },
            ),
            (
                "-- @publication_catalog_fault=table-load,corrupt-package",
                PublicationCatalogFaultDirective {
                    action: PublicationCatalogAction::TableLoad,
                    fault: PublicationCatalogFault::CorruptPackage,
                },
            ),
        ] {
            let meta =
                parse_meta(&[directive.to_string()], &re).expect("parse catalog observation fault");
            assert_eq!(meta.publication_catalog_fault, Some(expected));
        }
        let error = parse_meta(
            &["-- @publication_catalog_fault=namespace-list,corrupt-package".to_string()],
            &re,
        )
        .expect_err("crossed catalog observation fault must be rejected");
        assert!(error.to_string().contains("action/fault combination"));
    }

    #[test]
    fn parse_meta_parses_inflight_publication_frontend_kill_fault() {
        let re = meta_re();
        let meta = parse_meta(
            &[
                "-- @publication_catalog_fault=table-commit,after-commit-hold-for-frontend-kill"
                    .to_string(),
            ],
            &re,
        )
        .expect("parse in-flight frontend kill fault");
        assert_eq!(
            meta.publication_catalog_fault,
            Some(PublicationCatalogFaultDirective {
                action: PublicationCatalogAction::TableCommit,
                fault: PublicationCatalogFault::AfterCommitHoldForFrontendKill,
            })
        );
    }

    #[test]
    fn parse_meta_pairs_before_dispatch_hold_with_concurrent_shell() {
        let re = meta_re();
        let meta = parse_meta(
            &[
                "-- @publication_catalog_fault=table-commit,before-dispatch-hold-for-concurrent-shell".to_string(),
                "-- @publication_catalog_concurrent_shell=printf 'advance table\\n'".to_string(),
            ],
            &re,
        )
        .expect("parse concurrent publication hold");

        assert_eq!(
            meta.publication_catalog_fault,
            Some(PublicationCatalogFaultDirective {
                action: PublicationCatalogAction::TableCommit,
                fault: PublicationCatalogFault::BeforeDispatchHoldForConcurrentShell,
            })
        );
        assert_eq!(
            meta.publication_catalog_concurrent_shell.as_deref(),
            Some("printf 'advance table\\n'")
        );

        let error = parse_meta(
            &["-- @publication_catalog_fault=stage-create,before-dispatch-hold-for-concurrent-shell".to_string()],
            &re,
        )
        .expect_err("concurrent mutation hold is valid only for an existing table commit");
        assert!(
            error.to_string().contains("action/fault combination"),
            "{error:#}"
        );
    }

    #[test]
    fn parse_meta_rejects_unknown_publication_catalog_fault_action() {
        let re = meta_re();
        let lines = vec!["-- @publication_catalog_fault=inspect,before-dispatch".to_string()];
        let error = parse_meta(&lines, &re).expect_err("unknown action must fail closed");
        assert!(
            error
                .to_string()
                .contains("invalid @publication_catalog_fault action")
        );
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
            "-- @heartbeat_delay_ms=250".to_string(),
            "-- @restart_be_delay_ms=500".to_string(),
        ];

        let meta = parse_meta(&lines, &re).expect("parse ok");

        assert_eq!(meta.kill_be_index, Some(1));
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
            "-- @kill_fe_after_mv_known_committed_before_projector_cas=true".to_string(),
            "-- @restart_be_after_establish_context_index=1".to_string(),
            "-- @kill_query_after_be_log_contains=split_id=iceberg-metadata-0".to_string(),
            "-- @terminal_snapshot_conflict_be_index=2".to_string(),
            "-- @kill_query_at_lifecycle_phase=starting".to_string(),
            "-- @kill_be_at_lifecycle_phase=2,terminal-retained".to_string(),
            "-- @query_control_fragment_backend_limit=2".to_string(),
        ];

        let meta = parse_meta(&lines, &re).expect("parse query lifecycle fault directives");

        assert!(meta.kill_fe_after_mv_known_committed_before_projector_cas);
        assert_eq!(meta.restart_be_after_establish_context_index, Some(1));
        assert_eq!(
            meta.kill_query_after_be_log_contains.as_deref(),
            Some("split_id=iceberg-metadata-0")
        );
        assert_eq!(meta.terminal_snapshot_conflict_be_index, Some(2));
        assert_eq!(
            meta.kill_be_at_lifecycle_phase,
            Some(KillBeAtLifecyclePhaseDirective {
                be_index: 2,
                phase: QueryLifecyclePhase::TerminalRetained,
            })
        );
        assert_eq!(
            meta.kill_query_at_lifecycle_phase,
            Some(QueryLifecyclePhase::Starting)
        );
        assert_eq!(meta.query_control_fragment_backend_limit, Some(2));
    }

    #[test]
    fn parse_meta_rejects_invalid_query_lifecycle_fault_number_with_context() {
        let re = meta_re();
        for (directive, expected) in [
            (
                "query_control_fragment_backend_limit=two",
                "invalid query_control_fragment_backend_limit: two",
            ),
            (
                "restart_be_after_establish_context_index=first",
                "invalid restart_be_after_establish_context_index: first",
            ),
            (
                "terminal_snapshot_conflict_be_index=first",
                "invalid terminal_snapshot_conflict_be_index: first",
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
            "kill_be_at_lifecycle_phase=1,after-start",
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
    fn parse_sql_error_directives_with_test_only_descriptors() {
        let re = meta_re();
        let lines = vec![
            "-- @expect_sql_code=sql.test.fixture".to_string(),
            "-- @expect_sql_phase=Parse".to_string(),
            "-- @expect_error_at=7:11".to_string(),
            "-- @expect_error_tier=target".to_string(),
        ];

        let meta = parse_meta_with_sql_error_descriptors(
            &lines,
            &re,
            crate::sql_error_codes::TEST_SQL_ERROR_DESCRIPTORS,
        )
        .expect("test descriptor should be accepted");
        validate_sql_error_expectations(&meta, crate::sql_error_codes::TEST_SQL_ERROR_DESCRIPTORS)
            .expect("complete target assertion should validate");

        assert_eq!(meta.expect_sql_code.as_deref(), Some("sql.test.fixture"));
        assert_eq!(meta.expect_sql_phase, Some(SqlErrorPhase::Parse));
        assert_eq!(
            meta.expect_error_at,
            Some(SqlErrorLocation {
                line: 7,
                column: 11
            })
        );
        assert_eq!(meta.sql_error_tier(), SqlErrorTier::Target);
    }

    #[test]
    fn parse_nova_extension_is_declarative_metadata() {
        let re = meta_re();
        let meta = parse_meta(
            &["-- @nova_extension=iceberg branch and tag DDL".to_string()],
            &re,
        )
        .expect("extension annotation should parse");

        assert_eq!(
            meta.nova_extension.as_deref(),
            Some("iceberg branch and tag DDL")
        );
        assert!(!meta.has_error_expectation());
    }

    #[test]
    fn production_sql_error_manifest_accepts_parser_codes_and_rejects_unknown_codes() {
        let re = meta_re();
        let meta = parse_meta(
            &[
                "-- @expect_sql_code=sql.parse.unsupported_statement".to_string(),
                "-- @expect_sql_phase=Parse".to_string(),
            ],
            &re,
        )
        .expect("generated manifest must expose parser descriptors");
        assert_eq!(meta.expect_sql_phase, Some(SqlErrorPhase::Parse));

        let error = parse_meta(&["-- @expect_sql_code=sql.test.fixture".to_string()], &re)
            .expect_err("unknown production code must fail fast");

        assert!(
            error
                .to_string()
                .contains("unknown expect_sql_code: sql.test.fixture"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn target_tier_requires_code_and_location_after_meta_merge() {
        let incomplete = QueryMeta {
            expect_error_tier: Some(SqlErrorTier::Target),
            ..QueryMeta::default()
        };
        let error = validate_sql_error_expectations(
            &incomplete,
            crate::sql_error_codes::TEST_SQL_ERROR_DESCRIPTORS,
        )
        .expect_err("target tier without code must fail");
        assert!(error.to_string().contains("requires @expect_sql_code"));

        let missing_location = QueryMeta {
            expect_error_tier: Some(SqlErrorTier::Target),
            expect_sql_code: Some("sql.test.fixture".to_string()),
            ..QueryMeta::default()
        };
        let error = validate_sql_error_expectations(
            &missing_location,
            crate::sql_error_codes::TEST_SQL_ERROR_DESCRIPTORS,
        )
        .expect_err("target tier without location must fail");
        assert!(error.to_string().contains("requires @expect_error_at"));
    }

    #[test]
    fn sql_phase_requires_sql_code() {
        let meta = QueryMeta {
            expect_sql_phase: Some(SqlErrorPhase::Parse),
            ..QueryMeta::default()
        };
        let error = validate_sql_error_expectations(
            &meta,
            crate::sql_error_codes::TEST_SQL_ERROR_DESCRIPTORS,
        )
        .expect_err("phase must be descriptor-addressable by code");
        assert!(
            error
                .to_string()
                .contains("@expect_sql_phase requires @expect_sql_code")
        );
    }

    #[test]
    fn merge_meta_preserves_sql_error_assertion_fields() {
        let base = QueryMeta {
            expect_sql_code: Some("sql.test.fixture".to_string()),
            expect_sql_phase: Some(SqlErrorPhase::Parse),
            expect_error_at: Some(SqlErrorLocation { line: 3, column: 2 }),
            expect_error_tier: Some(SqlErrorTier::Target),
            ..QueryMeta::default()
        };
        let merged = merge_meta(&base, &QueryMeta::default());

        assert_eq!(merged.expect_sql_code, base.expect_sql_code);
        assert_eq!(merged.expect_sql_phase, base.expect_sql_phase);
        assert_eq!(merged.expect_error_at, base.expect_error_at);
        assert_eq!(merged.expect_error_tier, base.expect_error_tier);
    }

    #[test]
    fn merge_meta_preserves_nova_extension_annotation() {
        let base = QueryMeta {
            nova_extension: Some("native syntax".to_string()),
            ..QueryMeta::default()
        };
        let override_meta = QueryMeta {
            nova_extension: Some("more specific syntax".to_string()),
            ..QueryMeta::default()
        };

        assert_eq!(
            merge_meta(&base, &QueryMeta::default())
                .nova_extension
                .as_deref(),
            Some("native syntax")
        );
        assert_eq!(
            merge_meta(&base, &override_meta).nova_extension.as_deref(),
            Some("more specific syntax")
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
    fn parse_meta_collects_imv_accelerator_wipe_restart() {
        let re = meta_re();
        let lines = vec!["-- @imv_accelerator_wipe_restart=orders_mv,catalog=mv_ice_x".to_string()];
        let meta = parse_meta(&lines, &re).expect("parse");
        let d = meta
            .imv_accelerator_wipe_restart
            .as_ref()
            .expect("directive");
        assert_eq!(d.mv, "orders_mv");
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
            heartbeat_delay_ms: Some(250),
            restart_be_delay_ms: Some(500),
            ..QueryMeta::default()
        };
        let merged = merge_meta(&base, &QueryMeta::default());

        assert_eq!(merged.kill_be_index, Some(1));
        assert_eq!(merged.heartbeat_delay_ms, Some(250));
        assert_eq!(merged.restart_be_delay_ms, Some(500));
    }

    #[test]
    fn merge_meta_overrides_fault_directives_when_present() {
        let base = QueryMeta {
            kill_be_index: Some(1),
            heartbeat_delay_ms: Some(250),
            restart_be_delay_ms: Some(500),
            ..QueryMeta::default()
        };
        let override_meta = QueryMeta {
            kill_be_index: Some(3),
            heartbeat_delay_ms: Some(750),
            restart_be_delay_ms: Some(1000),
            ..QueryMeta::default()
        };
        let merged = merge_meta(&base, &override_meta);

        assert_eq!(merged.kill_be_index, Some(3));
        assert_eq!(merged.heartbeat_delay_ms, Some(750));
        assert_eq!(merged.restart_be_delay_ms, Some(1000));
    }

    #[test]
    fn merge_meta_inherits_and_overrides_query_lifecycle_fault_directives() {
        let base = QueryMeta {
            kill_fe_after_mv_known_committed_before_projector_cas: true,
            restart_be_after_establish_context_index: Some(0),
            kill_query_after_be_log_contains: Some("reader-open".to_string()),
            terminal_snapshot_conflict_be_index: Some(1),
            kill_query_at_lifecycle_phase: Some(QueryLifecyclePhase::Staging),
            query_control_fragment_backend_limit: Some(2),
            ..QueryMeta::default()
        };
        let inherited = merge_meta(&base, &QueryMeta::default());
        assert!(inherited.kill_fe_after_mv_known_committed_before_projector_cas);
        assert_eq!(inherited.restart_be_after_establish_context_index, Some(0));
        assert_eq!(
            inherited.kill_query_after_be_log_contains.as_deref(),
            Some("reader-open")
        );
        assert_eq!(inherited.terminal_snapshot_conflict_be_index, Some(1));
        assert_eq!(
            inherited.kill_query_at_lifecycle_phase,
            Some(QueryLifecyclePhase::Staging)
        );
        assert_eq!(inherited.query_control_fragment_backend_limit, Some(2));

        let override_meta = QueryMeta {
            restart_be_after_establish_context_index: Some(2),
            kill_query_after_be_log_contains: Some("metadata-open".to_string()),
            terminal_snapshot_conflict_be_index: Some(0),
            kill_query_at_lifecycle_phase: Some(QueryLifecyclePhase::Running),
            query_control_fragment_backend_limit: Some(1),
            ..QueryMeta::default()
        };
        let overridden = merge_meta(&base, &override_meta);
        assert!(overridden.kill_fe_after_mv_known_committed_before_projector_cas);
        assert_eq!(overridden.restart_be_after_establish_context_index, Some(2));
        assert_eq!(
            overridden.kill_query_after_be_log_contains.as_deref(),
            Some("metadata-open")
        );
        assert_eq!(overridden.terminal_snapshot_conflict_be_index, Some(0));
        assert_eq!(
            overridden.kill_query_at_lifecycle_phase,
            Some(QueryLifecyclePhase::Running)
        );
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

    #[test]
    fn rfo_8r2_fault_and_structured_assertions_parse_without_log_text_contracts() {
        let re = meta_re();
        let lines = vec![
            "-- @query_lifecycle_fault=runtime-filter-contribution-ack-drop,2".to_string(),
            "-- @expect_lifecycle_error_source=backend-attestation".to_string(),
            "-- @expect_lifecycle_metric_delta=terminal_retained,1".to_string(),
        ];

        let meta = parse_meta(&lines, &re).expect("parse RFO-8R2 directives");
        assert_eq!(
            meta.query_lifecycle_fault,
            Some(QueryLifecycleFaultDirective {
                kind: QueryLifecycleFaultKind::RuntimeFilterContributionAckDrop,
                be_index: 2,
            })
        );
        assert_eq!(
            meta.query_lifecycle_faults,
            vec![QueryLifecycleFaultDirective {
                kind: QueryLifecycleFaultKind::RuntimeFilterContributionAckDrop,
                be_index: 2,
            }]
        );
        let assertion = meta
            .query_lifecycle_structured_assertion
            .expect("structured assertion");
        assert_eq!(
            assertion.error_source,
            Some(QueryLifecycleErrorSource::BackendAttestation)
        );
        assert_eq!(
            assertion.metric_deltas,
            vec![QueryLifecycleMetricDeltaExpectation {
                metric: "terminal_retained".to_string(),
                delta: 1,
            }]
        );
    }

    #[test]
    fn parse_meta_allows_task_update_terminal_ack_drop() {
        let re = meta_re();
        let meta = parse_meta(
            &["-- @query_lifecycle_fault=task-update-terminal-ack-drop,1".to_string()],
            &re,
        )
        .expect("parse task update terminal acknowledgement fault");

        assert_eq!(
            meta.query_lifecycle_faults,
            vec![QueryLifecycleFaultDirective {
                kind: QueryLifecycleFaultKind::TaskUpdateTerminalAckDrop,
                be_index: 1,
            }]
        );
    }

    #[test]
    fn runtime_filter_terminal_directives_use_closed_typed_categories() {
        let re = meta_re();
        let lines = vec![
            "-- @expect_runtime_filter_available=available".to_string(),
            "-- @expect_runtime_filter_detail=completed-channel".to_string(),
            "-- @expect_runtime_filter_detail=accepted-producer".to_string(),
            "-- @expect_runtime_filter_detail=delivered-consumer".to_string(),
            "-- @expect_runtime_filter_total_at_least=transport_acked_count,1".to_string(),
            "-- @expect_runtime_filter_total_at_least=consumer_input_rows,20".to_string(),
        ];

        let meta = parse_meta(&lines, &re).expect("parse typed runtime-filter directives");
        let assertion = meta
            .query_lifecycle_structured_assertion
            .expect("structured runtime-filter assertion");
        assert_eq!(
            assertion.runtime_filter_availability,
            Some(RuntimeFilterAvailabilityExpectation::Available)
        );
        assert_eq!(
            assertion.runtime_filter_details,
            vec![
                RuntimeFilterDetailExpectation::CompletedChannel,
                RuntimeFilterDetailExpectation::AcceptedProducer,
                RuntimeFilterDetailExpectation::DeliveredConsumer,
            ]
        );
        assert_eq!(
            assertion.runtime_filter_totals_at_least,
            vec![
                RuntimeFilterTotalAtLeastExpectation {
                    metric: RuntimeFilterTotalMetric::TransportAckedCount,
                    value: 1,
                },
                RuntimeFilterTotalAtLeastExpectation {
                    metric: RuntimeFilterTotalMetric::ConsumerInputRows,
                    value: 20,
                },
            ]
        );

        for line in [
            "-- @expect_runtime_filter_available=partial",
            "-- @expect_runtime_filter_detail=json.path",
            "-- @expect_runtime_filter_total_at_least=raw_json,1",
            "-- @expect_runtime_filter_total_at_least=consumer_input_rows,0",
        ] {
            assert!(
                parse_meta(&[line.to_string()], &re).is_err(),
                "invalid typed runtime-filter directive must fail: {line}"
            );
        }
    }

    #[test]
    fn parse_meta_accumulates_rfo_8r2_fault_directives() {
        let re = meta_re();
        let lines = vec![
            "-- @query_lifecycle_fault=runtime-filter-contribution-ack-drop,2".to_string(),
            "-- @query_lifecycle_fault=task-update-terminal-ack-drop,2".to_string(),
        ];

        let meta = parse_meta(&lines, &re).expect("parse multiple RFO-8R2 directives");

        assert_eq!(
            meta.query_lifecycle_fault,
            Some(QueryLifecycleFaultDirective {
                kind: QueryLifecycleFaultKind::RuntimeFilterContributionAckDrop,
                be_index: 2,
            })
        );
        assert_eq!(
            meta.query_lifecycle_faults,
            vec![
                QueryLifecycleFaultDirective {
                    kind: QueryLifecycleFaultKind::RuntimeFilterContributionAckDrop,
                    be_index: 2,
                },
                QueryLifecycleFaultDirective {
                    kind: QueryLifecycleFaultKind::TaskUpdateTerminalAckDrop,
                    be_index: 2,
                },
            ]
        );
    }

    #[test]
    fn parse_meta_rejects_be_kill_before_terminal_retention() {
        let re = meta_re();
        let error = parse_meta(
            &["-- @kill_be_at_lifecycle_phase=1,running".to_string()],
            &re,
        )
        .expect_err("BE kill must wait for immutable terminal evidence");

        assert!(
            format!("{error:#}").contains("expected terminal-retained"),
            "unexpected error: {error:#}"
        );
    }

    /// The defect this catches: an unrecognised directive used to be ignored,
    /// so a case could read as though it armed a fault or checked evidence
    /// while the runner did neither -- and then pass on whatever the engine
    /// happened to do. Every failure this suite exists to catch has that
    /// shape, and a typo or a directive retired out from under a case was
    /// enough to reach it.
    #[test]
    fn an_unknown_directive_is_refused_rather_than_ignored() {
        let re = meta_re();
        let error = parse_meta(&["-- @not_a_directive=1".to_string()], &re)
            .expect_err("an unimplemented directive asserts nothing");
        let message = format!("{error:#}");
        assert!(
            message.contains("unknown directive @not_a_directive"),
            "the refusal has to name the directive, got: {message}"
        );

        // A directive parsed here and consumed elsewhere is still known.
        parse_meta(&["-- @sequential=true".to_string()], &re)
            .expect("a directive handled at case level is not unknown");
    }

    #[test]
    fn rfo_8r2_fault_parser_rejects_unknown_arm() {
        let re = meta_re();
        let error = parse_meta(
            &["-- @query_lifecycle_fault=unknown-arm,0".to_string()],
            &re,
        )
        .expect_err("unknown arm must fail");
        assert!(
            format!("{error:#}").contains("invalid query_lifecycle_fault kind"),
            "unexpected error: {error:#}"
        );
    }
}
