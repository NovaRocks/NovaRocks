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

use crate::cluster::{ClusterMode, ServerHandle};
use crate::types::{QueryMeta, SqlStep};
use crate::{Mode, RecordFrom};
use anyhow::{Context, Result, bail};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Write;
use std::time::{Duration, Instant};

#[cfg(not(test))]
const LOG_EVIDENCE_TIMEOUT: Duration = Duration::from_secs(3);
#[cfg(test)]
const LOG_EVIDENCE_TIMEOUT: Duration = Duration::from_millis(200);
const LOG_EVIDENCE_POLL_INTERVAL: Duration = Duration::from_millis(25);
const QUERY_LIFECYCLE_STEP_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) fn has_directives(meta: &QueryMeta) -> bool {
    meta.has_be_log_directives()
}

pub(crate) fn validate_mode(meta: &QueryMeta, mode: ClusterMode) -> Result<()> {
    if meta.has_be_log_directives() && mode == ClusterMode::AllInOne {
        bail!("BE log evidence directives require a runner-owned cross-process cluster");
    }
    Ok(())
}

pub(crate) fn validate_execution_mode(meta: &QueryMeta, mode: Mode) -> Result<()> {
    if has_directives(meta) && !matches!(mode, Mode::Verify | Mode::Record) {
        bail!("BE log directives require verify or record mode (got {mode:?})");
    }
    Ok(())
}

pub(crate) fn validate_record_source(
    meta: &QueryMeta,
    mode: Mode,
    record_from: RecordFrom,
) -> Result<()> {
    if has_directives(meta) && mode == Mode::Record && record_from == RecordFrom::Reference {
        bail!("BE log directives cannot run with record-from=reference");
    }
    Ok(())
}

#[derive(Debug, Default)]
pub(crate) struct BeLogSnapshot {
    counts: HashMap<(usize, String), usize>,
    fragment_failure_token: Option<String>,
    log_lengths: Vec<usize>,
    lifecycle_token: Option<(usize, &'static str, String)>,
    evidence_deadline: Option<Instant>,
}

impl BeLogSnapshot {
    pub(crate) fn evidence_deadline(&self) -> Option<Instant> {
        self.evidence_deadline
    }
}

pub(crate) fn snapshot(
    meta: &QueryMeta,
    server_handle: &dyn ServerHandle,
) -> Result<BeLogSnapshot> {
    snapshot_with_deadline(meta, server_handle, step_evidence_deadline(meta))
}

pub(crate) fn query_lifecycle_step_deadline(meta: &QueryMeta) -> Option<Instant> {
    is_query_lifecycle_step(meta).then(|| Instant::now() + QUERY_LIFECYCLE_STEP_TIMEOUT)
}

pub(crate) fn step_evidence_deadline(meta: &QueryMeta) -> Option<Instant> {
    (meta.kill_be_after_fragment_start.is_some()
        || meta.fail_fragment_after_start_be_index.is_some()
        || is_query_lifecycle_step(meta))
    .then(|| Instant::now() + QUERY_LIFECYCLE_STEP_TIMEOUT)
}

fn is_query_lifecycle_step(meta: &QueryMeta) -> bool {
    meta.drop_next_init_ack_be_index.is_some()
        || meta.stop_query_control_heartbeat_be_index.is_some()
        || meta.kill_fe_after_control_ready_count.is_some()
        || meta.restart_be_after_init_ack_index.is_some()
        || meta.drop_next_terminal_ack_be_index.is_some()
        || meta.kill_query_after_control_ready_count.is_some()
        || meta.query_control_fragment_backend_limit.is_some()
}

pub(crate) fn snapshot_with_deadline(
    meta: &QueryMeta,
    server_handle: &dyn ServerHandle,
    evidence_deadline: Option<Instant>,
) -> Result<BeLogSnapshot> {
    if !has_directives(meta) {
        return Ok(BeLogSnapshot {
            evidence_deadline,
            ..BeLogSnapshot::default()
        });
    }
    let be_count = server_handle.be_count();
    if be_count == 0 {
        bail!("BE log evidence directives require at least one runner-owned BE");
    }
    let patterns = meta
        .be_log_contains
        .iter()
        .chain(meta.be_log_not_contains.iter())
        .chain(
            meta.be_log_count_at_least
                .iter()
                .map(|(pattern, _)| pattern),
        )
        .chain(
            meta.be_log_be_count_at_least
                .iter()
                .map(|(pattern, _)| pattern),
        )
        .collect::<HashSet<_>>();
    let mut counts = HashMap::new();
    let log_lengths = (0..be_count)
        .map(|index| server_handle.be_log_contents(index).map(|log| log.len()))
        .collect::<Result<Vec<_>>>()?;
    for pattern in patterns {
        for index in 0..be_count {
            counts.insert(
                (index, pattern.clone()),
                server_handle.be_log_count(index, pattern)?,
            );
        }
    }
    let fragment_failure_token = if meta.be_log_exact_fragment_cancellation.is_some() {
        let index = meta.fail_fragment_after_start_be_index.context(
            "@be_log_exact_fragment_cancellation requires @fail_fragment_after_start_be_index",
        )?;
        Some(
            server_handle
                .armed_fragment_failure_token(index)?
                .with_context(|| {
                    format!(
                        "BE[{index}] has no armed fragment failure token for exact cancellation evidence"
                    )
                })?,
        )
    } else {
        None
    };
    let lifecycle_fault = meta
        .drop_next_init_ack_be_index
        .map(|index| (index, "init-ack-drop"))
        .or_else(|| {
            meta.stop_query_control_heartbeat_be_index
                .map(|index| (index, "heartbeat-stop"))
        })
        .or_else(|| {
            meta.restart_be_after_init_ack_index
                .map(|index| (index, "restart-after-init-ack"))
        });
    let lifecycle_token = lifecycle_fault
        .map(|(index, kind)| {
            server_handle
                .armed_query_lifecycle_fault_token(index, kind)?
                .map(|token| (index, kind, token))
                .with_context(|| format!("BE[{index}] has no armed {kind} token"))
        })
        .transpose()?;
    Ok(BeLogSnapshot {
        counts,
        fragment_failure_token,
        log_lengths,
        lifecycle_token,
        evidence_deadline,
    })
}

fn log_delta(
    snapshot: &BeLogSnapshot,
    server_handle: &dyn ServerHandle,
    index: usize,
    pattern: &str,
) -> Result<usize> {
    let before = snapshot
        .counts
        .get(&(index, pattern.to_string()))
        .copied()
        .unwrap_or(0);
    let after = server_handle.be_log_count(index, pattern)?;
    after.checked_sub(before).ok_or_else(|| {
        anyhow::anyhow!(
            "BE log {index} count for pattern {pattern:?} decreased from {before} to {after}"
        )
    })
}

enum LogEvidenceCheck {
    Satisfied(Vec<String>),
    Pending(String),
}

fn lifecycle_log_deltas(
    snapshot: &BeLogSnapshot,
    server_handle: &dyn ServerHandle,
    endpoint_count: usize,
) -> Result<Vec<String>> {
    (0..endpoint_count)
        .map(|index| {
            let log = server_handle.be_log_contents(index)?;
            let before = snapshot.log_lengths.get(index).copied().unwrap_or(0);
            if log.len() < before {
                bail!(
                    "BE[{index}] lifecycle log length decreased from {before} to {}",
                    log.len()
                );
            }
            Ok(log[before..].to_string())
        })
        .collect()
}

fn execution_field(line: &str, marker: &str) -> Result<Option<String>> {
    let Some(payload) = marker_payload(line, marker) else {
        return Ok(None);
    };
    Ok(Some(
        marker_fields(payload, marker)?
            .get("execution_id")
            .with_context(|| format!("{marker} is missing execution_id"))?
            .to_string(),
    ))
}

fn distinct_backends_for_execution(
    logs: &[String],
    marker: &str,
    execution_id: &str,
) -> Result<HashSet<usize>> {
    let mut result = HashSet::new();
    for (index, log) in logs.iter().enumerate() {
        for line in log.lines() {
            if execution_field(line, marker)?.as_deref() == Some(execution_id) {
                result.insert(index);
            }
        }
    }
    Ok(result)
}

fn lifecycle_evidence(
    step: &SqlStep,
    server_handle: &dyn ServerHandle,
    snapshot: &BeLogSnapshot,
    endpoint_count: usize,
) -> Result<Option<LogEvidenceCheck>> {
    let lifecycle_step = is_query_lifecycle_step(&step.meta);
    if !lifecycle_step {
        return Ok(None);
    }
    if endpoint_count != 3 {
        bail!("query lifecycle evidence requires exactly 3 BEs, found {endpoint_count}");
    }
    let logs = lifecycle_log_deltas(snapshot, server_handle, endpoint_count)?;

    if let Some(limit) = step.meta.query_control_fragment_backend_limit {
        let marker = "NOVAROCKS_QUERY_CONTROL_READY";
        let mut by_execution = BTreeMap::<String, Vec<(usize, usize)>>::new();
        for (index, log) in logs.iter().enumerate() {
            for line in log.lines() {
                let Some(execution) = execution_field(line, marker)? else {
                    continue;
                };
                let fields = marker_fields(marker_payload(line, marker).unwrap(), marker)?;
                let expected = fields
                    .get("expected_fragments")
                    .context("ControlReady missing expected_fragments")?
                    .parse::<usize>()?;
                by_execution
                    .entry(execution)
                    .or_default()
                    .push((index, expected));
            }
        }
        for (execution, participants) in by_execution {
            let participant_bes = participants
                .iter()
                .map(|(be, _)| *be)
                .collect::<HashSet<_>>();
            if participant_bes.len() != 3 {
                continue;
            }
            let services = participants
                .iter()
                .filter_map(|(be, expected)| (*expected == 0).then_some(*be))
                .collect::<HashSet<_>>();
            let executors = distinct_backends_for_execution(
                &logs,
                "NOVAROCKS_QUERY_FRAGMENT_ACCEPTED",
                &execution,
            )?;
            if services.len() == 1 && executors.len() == limit && services.is_disjoint(&executors) {
                return Ok(Some(LogEvidenceCheck::Satisfied(vec![format!(
                    "    query_lifecycle_evidence PASS kind=service-only execution_id={execution} participants=3 executors={limit} service_backend={}",
                    services.iter().next().unwrap()
                )])));
            }
        }
        return Ok(Some(LogEvidenceCheck::Pending(
            "no single execution proves 3 participants, exactly one service-only participant, and exactly 2 fragment executors".to_string(),
        )));
    }

    let anchor = if let Some((index, _, token)) = &snapshot.lifecycle_token {
        let marker = if step.meta.drop_next_init_ack_be_index.is_some() {
            "NOVAROCKS_QUERY_INIT_ACK_DROPPED"
        } else if step.meta.stop_query_control_heartbeat_be_index.is_some() {
            "NOVAROCKS_QUERY_CONTROL_HEARTBEAT_STOPPED"
        } else {
            "NOVAROCKS_QUERY_INIT_ACK_OBSERVED"
        };
        logs[*index]
            .lines()
            .find(|line| line.contains(marker) && line.contains(&format!("token={token}")))
            .and_then(|line| execution_field(line, marker).transpose())
            .transpose()?
    } else {
        None
    };

    if step.meta.restart_be_after_init_ack_index.is_some() {
        return Ok(Some(if anchor.is_some() {
            LogEvidenceCheck::Satisfied(vec![
                "    query_lifecycle_evidence PASS kind=be-restart token_scoped_init_ack=true"
                    .to_string(),
            ])
        } else {
            LogEvidenceCheck::Pending("token-scoped restart InitAck marker missing".to_string())
        }));
    }

    if step.meta.drop_next_init_ack_be_index.is_some() {
        let Some(execution) = anchor else {
            return Ok(Some(LogEvidenceCheck::Pending(
                "token-scoped InitAck drop marker missing".to_string(),
            )));
        };
        let applied =
            distinct_backends_for_execution(&logs, "NOVAROCKS_QUERY_INIT_APPLIED", &execution)?;
        let idempotent =
            distinct_backends_for_execution(&logs, "NOVAROCKS_QUERY_INIT_IDEMPOTENT", &execution)?;
        let target_index = snapshot
            .lifecycle_token
            .as_ref()
            .map(|(index, _, _)| *index)
            .context("InitAck loss evidence has no target backend")?;
        return Ok(Some(
            if applied.len() == 3 && idempotent.contains(&target_index) {
                LogEvidenceCheck::Satisfied(vec![format!(
                    "    query_lifecycle_evidence PASS kind=init-ack-loss execution_id={execution} applied_backends=3 idempotent_backends={}",
                    idempotent.len()
                )])
            } else {
                LogEvidenceCheck::Pending(format!(
                    "InitAck execution {execution} applied_backends={} idempotent_backends={}",
                    applied.len(),
                    idempotent.len()
                ))
            },
        ));
    }

    let (anchor_marker, required_anchor_backends, required_reason, required_reason_backends) =
        if step.meta.stop_query_control_heartbeat_be_index.is_some() {
            (
                "NOVAROCKS_QUERY_CONTROL_HEARTBEAT_STOPPED",
                1,
                Some("CoordinatorHeartbeatTimeout"),
                1,
            )
        } else if step.meta.kill_fe_after_control_ready_count.is_some() {
            (
                "NOVAROCKS_QUERY_CONTROL_COORDINATOR_LOST",
                3,
                Some("CoordinatorStreamLost"),
                3,
            )
        } else {
            (
                "NOVAROCKS_QUERY_LIFECYCLE_TERMINATED",
                3,
                Some("CoordinatorAbort"),
                3,
            )
        };
    let candidates = if let Some(anchor) = anchor {
        vec![anchor]
    } else {
        logs.iter()
            .flat_map(|log| log.lines())
            .filter_map(|line| execution_field(line, anchor_marker).transpose())
            .collect::<Result<Vec<_>>>()?
    };
    for execution in candidates {
        let terminated = distinct_backends_for_execution(
            &logs,
            "NOVAROCKS_QUERY_LIFECYCLE_TERMINATED",
            &execution,
        )?;
        let anchor_bes = distinct_backends_for_execution(&logs, anchor_marker, &execution)?;
        let cleanup = distinct_backends_for_execution(
            &logs,
            "NOVAROCKS_QUERY_LIFECYCLE_CLEANUP",
            &execution,
        )?;
        let reason_backends = required_reason.map_or(required_reason_backends, |reason| {
            logs.iter()
                .filter(|log| {
                    log.lines().any(|line| {
                        line.contains("NOVAROCKS_QUERY_LIFECYCLE_TERMINATED")
                            && line.contains(&format!("execution_id={execution}"))
                            && line.contains(&format!("reason={reason}"))
                    })
                })
                .count()
        });
        if terminated.len() == 3
            && anchor_bes.len() == required_anchor_backends
            && reason_backends >= required_reason_backends
            && cleanup.len() == 3
        {
            return Ok(Some(LogEvidenceCheck::Satisfied(vec![format!(
                "    query_lifecycle_evidence PASS execution_id={execution} terminated_backends=3 cleanup_backends={}",
                cleanup.len()
            )])));
        }
    }
    Ok(Some(LogEvidenceCheck::Pending(
        "no single execution correlates the lifecycle fault and terminal facts across all 3 BEs"
            .to_string(),
    )))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct QueryIdentity {
    hi: i64,
    lo: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct FragmentIdentity {
    query: QueryIdentity,
    finst_hi: i64,
    finst_lo: i64,
}

type FragmentMultiset = BTreeMap<FragmentIdentity, usize>;

fn marker_payload<'a>(line: &'a str, marker: &str) -> Option<&'a str> {
    line.find(marker)
        .map(|position| &line[position + marker.len()..])
}

fn marker_fields<'a>(payload: &'a str, marker: &str) -> Result<HashMap<&'a str, &'a str>> {
    let mut fields = HashMap::new();
    for field in payload.split_whitespace() {
        let (key, value) = field
            .split_once('=')
            .with_context(|| format!("malformed {marker} field {field:?}"))?;
        if key.is_empty() || value.is_empty() {
            bail!("malformed {marker} field {field:?}");
        }
        if fields.insert(key, value).is_some() {
            bail!("duplicate {marker} field {key:?}");
        }
    }
    Ok(fields)
}

fn parse_i64_field(fields: &HashMap<&str, &str>, marker: &str, field: &str) -> Result<i64> {
    fields
        .get(field)
        .with_context(|| format!("{marker} is missing {field}"))?
        .parse::<i64>()
        .with_context(|| format!("{marker} has invalid {field}"))
}

fn parse_fragment_identity(fields: &HashMap<&str, &str>, marker: &str) -> Result<FragmentIdentity> {
    Ok(FragmentIdentity {
        query: QueryIdentity {
            hi: parse_i64_field(fields, marker, "query_hi")?,
            lo: parse_i64_field(fields, marker, "query_lo")?,
        },
        finst_hi: parse_i64_field(fields, marker, "finst_hi")?,
        finst_lo: parse_i64_field(fields, marker, "finst_lo")?,
    })
}

fn parse_identity_markers(log: &str, marker: &str) -> Result<Vec<FragmentIdentity>> {
    log.lines()
        .filter_map(|line| marker_payload(line, marker))
        .map(|payload| {
            let fields = marker_fields(payload, marker)?;
            parse_fragment_identity(&fields, marker)
        })
        .collect()
}

fn parse_query_terminal_ack_markers(log: &str) -> Result<Vec<QueryIdentity>> {
    const MARKER: &str = "NOVAROCKS_QUERY_TERMINAL_ACK";
    log.lines()
        .filter_map(|line| marker_payload(line, MARKER))
        .map(|payload| {
            let fields = marker_fields(payload, MARKER)?;
            // Validate the complete terminal identity, while only query id is
            // needed to relate a terminal ACK to the injected fragment.
            parse_i64_field(&fields, MARKER, "query_hi")?;
            parse_i64_field(&fields, MARKER, "query_lo")?;
            fields
                .get("attempt")
                .with_context(|| format!("{MARKER} is missing attempt"))?
                .parse::<u64>()
                .with_context(|| format!("{MARKER} has invalid attempt"))?;
            fields
                .get("backend_id")
                .with_context(|| format!("{MARKER} is missing backend_id"))?
                .parse::<u64>()
                .with_context(|| format!("{MARKER} has invalid backend_id"))?;
            Ok(QueryIdentity {
                hi: parse_i64_field(&fields, MARKER, "query_hi")?,
                lo: parse_i64_field(&fields, MARKER, "query_lo")?,
            })
        })
        .collect()
}

fn parse_stage_fragment_acceptance_markers(log: &str) -> Result<Vec<FragmentIdentity>> {
    const MARKER: &str = "NOVAROCKS_QUERY_FRAGMENT_ACCEPTED";
    log.lines()
        .filter_map(|line| marker_payload(line, MARKER))
        .map(|payload| {
            let fields = marker_fields(payload, MARKER)?;
            let execution_id = fields
                .get("execution_id")
                .with_context(|| format!("{MARKER} is missing execution_id"))?;
            let mut execution_parts = execution_id.split(':');
            let query_hi = execution_parts
                .next()
                .context(format!("{MARKER} has malformed execution_id"))?
                .parse::<i64>()
                .with_context(|| format!("{MARKER} has invalid execution_id query high bits"))?;
            let query_lo = execution_parts
                .next()
                .context(format!("{MARKER} has malformed execution_id"))?
                .parse::<i64>()
                .with_context(|| format!("{MARKER} has invalid execution_id query low bits"))?;
            let attempt = execution_parts
                .next()
                .context(format!("{MARKER} has malformed execution_id"))?;
            attempt
                .parse::<u64>()
                .with_context(|| format!("{MARKER} has invalid execution_id attempt"))?;
            if execution_parts.next().is_some() {
                bail!("{MARKER} has malformed execution_id");
            }

            let finst_id = fields
                .get("finst_id")
                .with_context(|| format!("{MARKER} is missing finst_id"))?;
            let bytes = finst_id.as_bytes();
            if bytes.len() != 36
                || bytes.get(8) != Some(&b'-')
                || bytes.get(13) != Some(&b'-')
                || bytes.get(18) != Some(&b'-')
                || bytes.get(23) != Some(&b'-')
            {
                bail!("{MARKER} has malformed finst_id");
            }
            let compact = finst_id.replace('-', "");
            if compact.len() != 32 || !compact.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                bail!("{MARKER} has malformed finst_id");
            }
            let hi = u64::from_str_radix(&compact[..16], 16)
                .with_context(|| format!("{MARKER} has invalid finst_id high bits"))?;
            let lo = u64::from_str_radix(&compact[16..], 16)
                .with_context(|| format!("{MARKER} has invalid finst_id low bits"))?;
            Ok(FragmentIdentity {
                query: QueryIdentity {
                    hi: query_hi,
                    lo: query_lo,
                },
                finst_hi: hi as i64,
                finst_lo: lo as i64,
            })
        })
        .collect()
}

#[cfg(test)]
fn parse_legacy_submit_acceptance_markers(log: &str) -> Result<Vec<FragmentIdentity>> {
    parse_identity_markers(log, "NOVAROCKS_GRPC_SUBMIT_ACCEPTED")
}

fn parse_fragment_acceptance_markers(log: &str) -> Result<Vec<FragmentIdentity>> {
    let mut accepted = parse_stage_fragment_acceptance_markers(log)?;
    #[cfg(test)]
    accepted.extend(parse_legacy_submit_acceptance_markers(log)?);
    Ok(accepted)
}

fn parse_failure_markers(log: &str) -> Result<Vec<(String, FragmentIdentity)>> {
    const MARKER: &str = "NOVAROCKS_FRAGMENT_EXECUTOR_FAILURE_INJECTED";
    log.lines()
        .filter_map(|line| marker_payload(line, MARKER))
        .map(|payload| {
            let fields = marker_fields(payload, MARKER)?;
            let token = fields
                .get("token")
                .with_context(|| format!("{MARKER} is missing token"))?;
            Ok((
                (*token).to_string(),
                parse_fragment_identity(&fields, MARKER)?,
            ))
        })
        .collect()
}

fn identity_multiset(
    identities: impl IntoIterator<Item = FragmentIdentity>,
    query: QueryIdentity,
) -> FragmentMultiset {
    let mut result = FragmentMultiset::new();
    for identity in identities {
        if identity.query == query {
            *result.entry(identity).or_insert(0) += 1;
        }
    }
    result
}

fn exact_fragment_cancellation_evidence(
    server_handle: &dyn ServerHandle,
    snapshot: &BeLogSnapshot,
    endpoint_count: usize,
    required_be_count: usize,
) -> Result<LogEvidenceCheck> {
    if endpoint_count != required_be_count {
        bail!(
            "@be_log_exact_fragment_cancellation requires exactly {required_be_count} runner-owned BEs; found {endpoint_count}"
        );
    }
    let token = snapshot
        .fragment_failure_token
        .as_deref()
        .context("@be_log_exact_fragment_cancellation snapshot has no fragment failure token")?;
    let logs = (0..endpoint_count)
        .map(|index| server_handle.be_log_contents(index))
        .collect::<Result<Vec<_>>>()?;

    let failure_markers = logs
        .iter()
        .map(|log| parse_failure_markers(log))
        .collect::<Result<Vec<_>>>();
    let failure_markers = match failure_markers {
        Ok(markers) => markers,
        Err(error) => {
            return Ok(LogEvidenceCheck::Pending(format!(
                "malformed fragment failure marker: {error:#}"
            )));
        }
    };
    let anchors = failure_markers
        .into_iter()
        .enumerate()
        .flat_map(|(be_index, markers)| markers.into_iter().map(move |marker| (be_index, marker)))
        .filter_map(|(be_index, (marker_token, identity))| {
            (marker_token == token).then_some((be_index, identity))
        })
        .collect::<Vec<_>>();
    let (anchor_be_index, anchor) = match anchors.as_slice() {
        [] => {
            return Ok(LogEvidenceCheck::Pending(format!(
                "no fragment failure marker has current step token {token:?}"
            )));
        }
        [(be_index, identity)] => (*be_index, *identity),
        _ => {
            bail!(
                "fragment failure token {token:?} anchored {} failure markers; expected exactly one",
                anchors.len()
            );
        }
    };

    let acknowledgements = logs
        .iter()
        .map(|log| parse_identity_markers(log, "NOVAROCKS_FAILED_FRAGMENT_REPORT_ACK"))
        .collect::<Result<Vec<_>>>();
    let acknowledgements = match acknowledgements {
        Ok(markers) => markers,
        Err(error) => {
            return Ok(LogEvidenceCheck::Pending(format!(
                "malformed failed-report ACK marker: {error:#}"
            )));
        }
    };
    let acknowledgements_total = acknowledgements
        .iter()
        .flatten()
        .filter(|identity| **identity == anchor)
        .count();
    if acknowledgements_total == 0 {
        // Native QLC-4 stops final ReportExecStatus delivery. Its terminal
        // acknowledgement is query-scoped, immutable, and can arrive through
        // either the stream or unary fallback. The legacy marker above remains
        // available for older native lifecycle evidence.
        let terminal_acks = logs
            .iter()
            .map(|log| parse_query_terminal_ack_markers(log))
            .collect::<Result<Vec<_>>>();
        let terminal_acks = match terminal_acks {
            Ok(markers) => markers,
            Err(error) => {
                return Ok(LogEvidenceCheck::Pending(format!(
                    "malformed query-terminal ACK marker: {error:#}"
                )));
            }
        };
        let on_failure_be = terminal_acks[anchor_be_index]
            .iter()
            .filter(|identity| **identity == anchor.query)
            .count();
        if on_failure_be != 1 {
            return Ok(LogEvidenceCheck::Pending(format!(
                "no terminal ACK for injected fragment query {:?} on failure BE[{anchor_be_index}]",
                anchor.query
            )));
        }
    } else {
        if acknowledgements_total != 1 {
            bail!(
                "injected fragment {anchor:?} has {acknowledgements_total} explicit frontend ACK markers; expected exactly one"
            );
        }
        let acknowledgements_on_failure_be = acknowledgements[anchor_be_index]
            .iter()
            .filter(|identity| **identity == anchor)
            .count();
        if acknowledgements_on_failure_be != 1 {
            bail!("injected fragment {anchor:?} ACK is not on failure BE[{anchor_be_index}]");
        }
    }

    let mut total = 0usize;
    let mut mismatches = Vec::new();
    for (index, log) in logs.iter().enumerate() {
        let accepted = match parse_fragment_acceptance_markers(log) {
            Ok(identities) => identity_multiset(identities, anchor.query),
            Err(error) => {
                return Ok(LogEvidenceCheck::Pending(format!(
                    "BE[{index}] has malformed accepted-fragment marker: {error:#}"
                )));
            }
        };
        let cancelled = match parse_identity_markers(log, "NOVAROCKS_CANCEL_FINST") {
            Ok(identities) => identity_multiset(identities, anchor.query),
            Err(error) => {
                return Ok(LogEvidenceCheck::Pending(format!(
                    "BE[{index}] has malformed cancelled-fragment marker: {error:#}"
                )));
            }
        };
        total = total
            .checked_add(accepted.len())
            .context("accepted fragment identity count overflow")?;

        let accepted_duplicates = accepted
            .iter()
            .filter(|(_, count)| **count != 1)
            .collect::<Vec<_>>();
        let cancelled_duplicates = cancelled
            .iter()
            .filter(|(_, count)| **count != 1)
            .collect::<Vec<_>>();
        if accepted != cancelled {
            mismatches.push(format!(
                "BE[{index}] identity mismatch accepted={accepted:?} cancelled={cancelled:?}"
            ));
        } else if !accepted_duplicates.is_empty() {
            mismatches.push(format!(
                "BE[{index}] accepted duplicate fragment identities: {accepted_duplicates:?}"
            ));
        } else if !cancelled_duplicates.is_empty() {
            mismatches.push(format!(
                "BE[{index}] cancelled duplicate fragment identities: {cancelled_duplicates:?}"
            ));
        }
        if index == anchor_be_index && accepted.get(&anchor) != Some(&1) {
            mismatches.push(format!(
                "injected fragment {anchor:?} was not accepted exactly once on failure BE[{index}]"
            ));
        }
        if index == anchor_be_index && cancelled.get(&anchor) != Some(&1) {
            mismatches.push(format!(
                "injected fragment {anchor:?} was not cancelled exactly once on failure BE[{index}]"
            ));
        }
    }
    if !mismatches.is_empty() {
        return Ok(LogEvidenceCheck::Pending(mismatches.join("; ")));
    }

    Ok(LogEvidenceCheck::Satisfied(vec![format!(
        "    @be_log_exact_fragment_cancellation PASS query_hi={} query_lo={} be_count={} total={total}",
        anchor.query.hi, anchor.query.lo, endpoint_count
    )]))
}

fn evaluate_log_evidence(
    step: &SqlStep,
    server_handle: &dyn ServerHandle,
    snapshot: &BeLogSnapshot,
    endpoint_count: usize,
) -> Result<LogEvidenceCheck> {
    let mut successes = Vec::new();
    let mut pending = Vec::new();

    for pattern in &step.meta.be_log_contains {
        let mut total = 0usize;
        for index in 0..endpoint_count {
            total = total
                .checked_add(log_delta(snapshot, server_handle, index, pattern)?)
                .context("BE log occurrence count overflow")?;
        }
        if total == 0 {
            pending.push(format!("no BE log contains pattern {pattern:?}"));
        } else {
            successes.push(format!("    @be_log_contains PASS pattern={pattern:?}"));
        }
    }

    for pattern in &step.meta.be_log_not_contains {
        let mut total = 0usize;
        for index in 0..endpoint_count {
            total = total
                .checked_add(log_delta(snapshot, server_handle, index, pattern)?)
                .context("BE log occurrence count overflow")?;
        }
        if total != 0 {
            bail!(
                "BE log unexpectedly contains forbidden step-scoped pattern {pattern:?} {total} time(s)"
            );
        }
        successes.push(format!(
            "    @be_log_not_contains PASS pattern={pattern:?}"
        ));
    }

    for (pattern, required) in &step.meta.be_log_count_at_least {
        let mut total = 0usize;
        for index in 0..endpoint_count {
            total = total
                .checked_add(log_delta(snapshot, server_handle, index, pattern)?)
                .context("BE log occurrence count overflow")?;
        }
        if total < *required {
            pending.push(format!(
                "BE log pattern {pattern:?} occurred {total} times across all BE logs; required at least {required}"
            ));
        } else {
            successes.push(format!(
                "    @be_log_count_at_least PASS pattern={pattern:?} actual={total} required={required}"
            ));
        }
    }

    for (pattern, required) in &step.meta.be_log_be_count_at_least {
        let mut actual = 0usize;
        for index in 0..endpoint_count {
            if log_delta(snapshot, server_handle, index, pattern)? > 0 {
                actual += 1;
            }
        }
        if actual < *required {
            pending.push(format!(
                "BE log pattern {pattern:?} appeared in {actual} distinct BE logs after the step; required at least {required}"
            ));
        } else {
            successes.push(format!(
                "    @be_log_be_count_at_least PASS pattern={pattern:?} actual={actual} required={required}"
            ));
        }
    }

    if let Some(required_be_count) = step.meta.be_log_exact_fragment_cancellation {
        match exact_fragment_cancellation_evidence(
            server_handle,
            snapshot,
            endpoint_count,
            required_be_count,
        )? {
            LogEvidenceCheck::Satisfied(exact_successes) => successes.extend(exact_successes),
            LogEvidenceCheck::Pending(reason) => pending.push(reason),
        }
    }

    if let Some(check) = lifecycle_evidence(step, server_handle, snapshot, endpoint_count)? {
        match check {
            LogEvidenceCheck::Satisfied(lifecycle_successes) => {
                successes.extend(lifecycle_successes)
            }
            LogEvidenceCheck::Pending(reason) => pending.push(reason),
        }
    }

    if pending.is_empty() {
        Ok(LogEvidenceCheck::Satisfied(successes))
    } else {
        Ok(LogEvidenceCheck::Pending(pending.join("; ")))
    }
}

pub(crate) fn run(
    step: &SqlStep,
    server_handle: &dyn ServerHandle,
    snapshot: &BeLogSnapshot,
    log: &mut String,
) -> Result<()> {
    if !has_directives(&step.meta) {
        return Ok(());
    }
    if step.meta.has_be_log_directives() {
        let be_count = server_handle.be_count();
        if be_count == 0 {
            bail!("BE log evidence directives require at least one runner-owned BE");
        }
        let started = Instant::now();
        let deadline = snapshot
            .evidence_deadline
            .unwrap_or(started + LOG_EVIDENCE_TIMEOUT);
        let mut pending_reason = "evidence was not evaluated".to_string();
        loop {
            if Instant::now() >= deadline {
                let elapsed = started.elapsed();
                let fe_tail = server_handle
                    .fe_log_contents()
                    .map(|contents| {
                        contents
                            .lines()
                            .rev()
                            .take(20)
                            .collect::<Vec<_>>()
                            .into_iter()
                            .rev()
                            .collect::<Vec<_>>()
                            .join("\n")
                    })
                    .unwrap_or_else(|error| format!("<read failed: {error:#}>"));
                let be_tails = (0..be_count)
                    .map(|index| {
                        server_handle
                            .be_log_contents(index)
                            .map(|contents| {
                                contents
                                    .lines()
                                    .rev()
                                    .take(20)
                                    .collect::<Vec<_>>()
                                    .into_iter()
                                    .rev()
                                    .collect::<Vec<_>>()
                                    .join("\n")
                            })
                            .unwrap_or_else(|error| format!("<read failed: {error:#}>"))
                    })
                    .collect::<Vec<_>>();
                bail!(
                    "BE log evidence timed out after {}ms (poll interval {}ms): {pending_reason}; fe_tail={fe_tail:?}; be_tails={be_tails:?}",
                    elapsed.as_millis(),
                    LOG_EVIDENCE_POLL_INTERVAL.as_millis(),
                );
            }
            match evaluate_log_evidence(step, server_handle, snapshot, be_count)? {
                LogEvidenceCheck::Satisfied(successes) => {
                    for success in successes {
                        let _ = writeln!(log, "{success}");
                    }
                    break;
                }
                LogEvidenceCheck::Pending(reason) => {
                    pending_reason = reason;
                    std::thread::sleep(LOG_EVIDENCE_POLL_INTERVAL);
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{QueryMeta, SqlStep};
    use anyhow::{Result, bail};
    use std::sync::{Arc, Mutex};

    struct FakeBeLogHandle {
        logs: Mutex<Vec<String>>,
        fragment_failure_token: Option<String>,
    }

    impl FakeBeLogHandle {
        fn new(logs: Vec<&str>) -> Self {
            Self {
                logs: Mutex::new(logs.into_iter().map(ToString::to_string).collect()),
                fragment_failure_token: None,
            }
        }

        fn with_fragment_failure_token(mut self, token: &str) -> Self {
            self.fragment_failure_token = Some(token.to_string());
            self
        }

        fn append_log(&self, index: usize, text: &str) {
            self.logs.lock().expect("logs lock")[index].push_str(text);
        }
    }

    impl ServerHandle for FakeBeLogHandle {
        fn target_host(&self) -> Option<&str> {
            Some("127.0.0.1")
        }

        fn target_port(&self) -> Option<u16> {
            Some(9030)
        }

        fn be_count(&self) -> usize {
            self.logs.lock().expect("logs lock").len()
        }

        fn be_log_count(&self, index: usize, needle: &str) -> Result<usize> {
            let logs = self.logs.lock().expect("logs lock");
            let log = logs
                .get(index)
                .ok_or_else(|| anyhow::anyhow!("missing fake BE log {index}"))?;
            Ok(log.match_indices(needle).count())
        }

        fn be_log_contents(&self, index: usize) -> Result<String> {
            self.logs
                .lock()
                .expect("logs lock")
                .get(index)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("missing fake BE log {index}"))
        }

        fn fe_log_contents(&self) -> Result<String> {
            Ok("fe-tail-sentinel\n".to_string())
        }

        fn armed_fragment_failure_token(&self, index: usize) -> Result<Option<String>> {
            if index >= self.logs.lock().expect("logs lock").len() {
                bail!("missing fake BE {index}");
            }
            Ok(self.fragment_failure_token.clone())
        }
    }

    fn step(meta: QueryMeta) -> SqlStep {
        SqlStep {
            query_number: 1,
            sql: "SELECT 1".to_string(),
            meta,
        }
    }

    #[test]
    fn be_log_directive_allows_record_but_rejects_diff_mode() {
        let meta = QueryMeta {
            be_log_contains: vec!["be_log_ingress".to_string()],
            ..QueryMeta::default()
        };

        validate_execution_mode(&meta, Mode::Record)
            .expect("record mode writes goldens before verify executes BE log directives");
        let error = validate_execution_mode(&meta, Mode::Diff)
            .expect_err("diff mode must not silently skip BE log directives");
        assert!(
            error
                .to_string()
                .contains("BE log directives require verify or record mode"),
            "unexpected error: {error:#}"
        );
        validate_execution_mode(&meta, Mode::Verify)
            .expect("verify mode must execute BE log directives");
    }

    #[test]
    fn native_cross_process_mode_allows_be_log_evidence() {
        let meta = QueryMeta {
            be_log_contains: vec!["NOVAROCKS_FAILED_FRAGMENT_REPORT_ACK".to_string()],
            ..QueryMeta::default()
        };

        validate_mode(&meta, ClusterMode::CrossProcess)
            .expect("runner-owned native BE logs must support evidence directives");
        validate_mode(&meta, ClusterMode::AllInOne)
            .expect_err("all-in-one has no runner-owned BE logs");
    }

    #[test]
    fn be_log_directive_rejects_reference_recording() {
        let meta = QueryMeta {
            be_log_contains: vec!["be_log_ingress".to_string()],
            ..QueryMeta::default()
        };

        validate_record_source(&meta, Mode::Record, RecordFrom::Target)
            .expect("target recording can collect BE log evidence");
        let error = validate_record_source(&meta, Mode::Record, RecordFrom::Reference)
            .expect_err("reference recording cannot collect target BE evidence");

        assert!(error.to_string().contains("record-from=reference"));
    }

    #[test]
    fn be_log_directive_inspects_all_be_logs_and_sums_occurrences() {
        let handle = FakeBeLogHandle::new(vec!["old be_log_ingress\n", "", "unrelated\n"]);
        let step = step(QueryMeta {
            be_log_contains: vec!["be_log_ingress".to_string()],
            be_log_count_at_least: vec![("runtime_filter_receive".to_string(), 3)],
            be_log_be_count_at_least: vec![("runtime_filter_receive".to_string(), 2)],
            ..QueryMeta::default()
        });
        let mut log = String::new();
        let before = snapshot(&step.meta, &handle).expect("pre-step snapshot");
        handle.append_log(0, "be_log_ingress\nruntime_filter_receive\n");
        handle.append_log(1, "runtime_filter_receive\nruntime_filter_receive\n");

        run(&step, &handle, &before, &mut log)
            .expect("directives should inspect post-step deltas across every BE log");

        assert!(log.contains("@be_log_contains PASS pattern=\"be_log_ingress\""));
        assert!(log.contains(
            "@be_log_count_at_least PASS pattern=\"runtime_filter_receive\" actual=3 required=3"
        ));
        assert!(log.contains(
            "@be_log_be_count_at_least PASS pattern=\"runtime_filter_receive\" actual=2 required=2"
        ));
    }

    #[test]
    fn negative_log_directive_is_scoped_to_post_step_delta() {
        let handle = FakeBeLogHandle::new(vec!["NOVAROCKS_CONNECTOR_WRITER_OPENED old\n", "", ""]);
        let step = step(QueryMeta {
            be_log_not_contains: vec!["NOVAROCKS_CONNECTOR_WRITER_OPENED".to_string()],
            ..QueryMeta::default()
        });
        let mut log = String::new();
        let before = snapshot(&step.meta, &handle).expect("pre-step snapshot");

        run(&step, &handle, &before, &mut log)
            .expect("pre-step markers must not fail a step-scoped assertion");

        assert!(log.contains(
            "@be_log_not_contains PASS pattern=\"NOVAROCKS_CONNECTOR_WRITER_OPENED\""
        ));
    }

    #[test]
    fn negative_log_directive_rejects_post_step_marker() {
        let handle = FakeBeLogHandle::new(vec!["", "", ""]);
        let step = step(QueryMeta {
            be_log_not_contains: vec!["NOVAROCKS_CONNECTOR_WRITER_OPENED".to_string()],
            ..QueryMeta::default()
        });
        let mut log = String::new();
        let before = snapshot(&step.meta, &handle).expect("pre-step snapshot");
        handle.append_log(1, "NOVAROCKS_CONNECTOR_WRITER_OPENED new\n");

        let error = run(&step, &handle, &before, &mut log)
            .expect_err("post-step marker must fail the negative assertion");

        assert!(error
            .to_string()
            .contains("unexpectedly contains forbidden step-scoped pattern"));
    }

    #[test]
    fn exact_injected_query_cancellation_compares_per_be_identity_multisets() {
        let handle = FakeBeLogHandle::new(vec![
            "NOVAROCKS_QUERY_FRAGMENT_ACCEPTED execution_id=1:2:1 backend_id=0 finst_id=00000000-0000-0003-0000-000000000004\n",
            "",
            "",
        ])
        .with_fragment_failure_token("step-token");
        let step = step(QueryMeta {
            fail_fragment_after_start_be_index: Some(1),
            be_log_exact_fragment_cancellation: Some(3),
            ..QueryMeta::default()
        });
        let before = snapshot(&step.meta, &handle).expect("capture armed trigger token");
        handle.append_log(
            0,
            "NOVAROCKS_QUERY_FRAGMENT_ACCEPTED execution_id=7:8:1 backend_id=0 finst_id=00000000-0000-0009-0000-00000000000a\n",
        );
        handle.append_log(
            0,
            "NOVAROCKS_QUERY_FRAGMENT_ACCEPTED execution_id=10:20:1 backend_id=0 finst_id=00000000-0000-0065-0000-0000000000c9\nNOVAROCKS_CANCEL_FINST query_hi=10 query_lo=20 finst_hi=101 finst_lo=201\n",
        );
        handle.append_log(
            1,
            "NOVAROCKS_QUERY_FRAGMENT_ACCEPTED execution_id=10:20:1 backend_id=1 finst_id=00000000-0000-0066-0000-0000000000ca\nNOVAROCKS_FRAGMENT_EXECUTOR_FAILURE_INJECTED token=step-token query_hi=10 query_lo=20 finst_hi=102 finst_lo=202\nNOVAROCKS_FAILED_FRAGMENT_REPORT_ACK query_hi=10 query_lo=20 finst_hi=102 finst_lo=202\nNOVAROCKS_CANCEL_FINST query_hi=10 query_lo=20 finst_hi=102 finst_lo=202\n",
        );
        handle.append_log(
            2,
            "NOVAROCKS_QUERY_FRAGMENT_ACCEPTED execution_id=10:20:1 backend_id=2 finst_id=00000000-0000-0067-0000-0000000000cb\nNOVAROCKS_QUERY_FRAGMENT_ACCEPTED execution_id=10:20:1 backend_id=2 finst_id=00000000-0000-0068-0000-0000000000cc\nNOVAROCKS_CANCEL_FINST query_hi=10 query_lo=20 finst_hi=103 finst_lo=203\nNOVAROCKS_CANCEL_FINST query_hi=10 query_lo=20 finst_hi=104 finst_lo=204\n",
        );
        let mut log = String::new();

        run(&step, &handle, &before, &mut log)
            .expect("every accepted current-query identity is cancelled exactly once");

        assert!(
            log.contains(
                "@be_log_exact_fragment_cancellation PASS query_hi=10 query_lo=20 be_count=3 total=4"
            ),
            "{log}"
        );
    }

    #[test]
    fn exact_injected_query_cancellation_accepts_native_terminal_ack() {
        let handle =
            FakeBeLogHandle::new(vec!["", "", ""]).with_fragment_failure_token("terminal-token");
        let step = step(QueryMeta {
            fail_fragment_after_start_be_index: Some(1),
            be_log_exact_fragment_cancellation: Some(3),
            ..QueryMeta::default()
        });
        let before = snapshot(&step.meta, &handle).expect("capture armed trigger token");
        handle.append_log(
            0,
            "NOVAROCKS_QUERY_FRAGMENT_ACCEPTED execution_id=10:20:1 backend_id=0 finst_id=00000000-0000-0065-0000-0000000000c9\nNOVAROCKS_CANCEL_FINST query_hi=10 query_lo=20 finst_hi=101 finst_lo=201\n",
        );
        handle.append_log(
            1,
            "NOVAROCKS_QUERY_FRAGMENT_ACCEPTED execution_id=10:20:1 backend_id=1 finst_id=00000000-0000-0066-0000-0000000000ca\nNOVAROCKS_FRAGMENT_EXECUTOR_FAILURE_INJECTED token=terminal-token query_hi=10 query_lo=20 finst_hi=102 finst_lo=202\nNOVAROCKS_QUERY_TERMINAL_ACK query_hi=10 query_lo=20 attempt=1 backend_id=1\nNOVAROCKS_CANCEL_FINST query_hi=10 query_lo=20 finst_hi=102 finst_lo=202\n",
        );
        handle.append_log(
            2,
            "NOVAROCKS_QUERY_FRAGMENT_ACCEPTED execution_id=10:20:1 backend_id=2 finst_id=00000000-0000-0067-0000-0000000000cb\nNOVAROCKS_CANCEL_FINST query_hi=10 query_lo=20 finst_hi=103 finst_lo=203\n",
        );

        run(&step, &handle, &before, &mut String::new())
            .expect("native terminal ACK must replace native final-report ACK evidence");
    }

    #[test]
    fn exact_injected_query_cancellation_rejects_equal_counts_with_wrong_identity() {
        let handle =
            FakeBeLogHandle::new(vec!["", "", ""]).with_fragment_failure_token("step-token");
        let step = step(QueryMeta {
            fail_fragment_after_start_be_index: Some(1),
            be_log_exact_fragment_cancellation: Some(3),
            ..QueryMeta::default()
        });
        let before = snapshot(&step.meta, &handle).expect("capture armed trigger token");
        handle.append_log(
            0,
            "NOVAROCKS_GRPC_SUBMIT_ACCEPTED query_hi=10 query_lo=20 finst_hi=101 finst_lo=201\nNOVAROCKS_GRPC_SUBMIT_ACCEPTED query_hi=10 query_lo=20 finst_hi=105 finst_lo=205\nNOVAROCKS_CANCEL_FINST query_hi=10 query_lo=20 finst_hi=101 finst_lo=201\nNOVAROCKS_CANCEL_FINST query_hi=10 query_lo=20 finst_hi=101 finst_lo=201\n",
        );
        handle.append_log(
            1,
            "NOVAROCKS_GRPC_SUBMIT_ACCEPTED query_hi=10 query_lo=20 finst_hi=102 finst_lo=202\nNOVAROCKS_FRAGMENT_EXECUTOR_FAILURE_INJECTED token=step-token query_hi=10 query_lo=20 finst_hi=102 finst_lo=202\nNOVAROCKS_FAILED_FRAGMENT_REPORT_ACK query_hi=10 query_lo=20 finst_hi=102 finst_lo=202\nNOVAROCKS_CANCEL_FINST query_hi=10 query_lo=20 finst_hi=102 finst_lo=202\n",
        );
        handle.append_log(
            2,
            "NOVAROCKS_GRPC_SUBMIT_ACCEPTED query_hi=10 query_lo=20 finst_hi=103 finst_lo=203\nNOVAROCKS_CANCEL_FINST query_hi=10 query_lo=20 finst_hi=103 finst_lo=203\n",
        );
        let mut log = String::new();

        let error = run(&step, &handle, &before, &mut log)
            .expect_err("A/B accepted but A/A cancelled must fail exact identity evidence");

        assert!(
            error.to_string().contains("BE[0] identity mismatch"),
            "{error:#}"
        );
    }

    #[test]
    fn exact_injected_query_cancellation_compares_each_be_not_only_global_identity() {
        let handle =
            FakeBeLogHandle::new(vec!["", "", ""]).with_fragment_failure_token("step-token");
        let step = step(QueryMeta {
            fail_fragment_after_start_be_index: Some(1),
            be_log_exact_fragment_cancellation: Some(3),
            ..QueryMeta::default()
        });
        let before = snapshot(&step.meta, &handle).expect("capture armed trigger token");
        handle.append_log(
            0,
            "NOVAROCKS_GRPC_SUBMIT_ACCEPTED query_hi=10 query_lo=20 finst_hi=101 finst_lo=201\nNOVAROCKS_CANCEL_FINST query_hi=10 query_lo=20 finst_hi=102 finst_lo=202\n",
        );
        handle.append_log(
            1,
            "NOVAROCKS_GRPC_SUBMIT_ACCEPTED query_hi=10 query_lo=20 finst_hi=102 finst_lo=202\nNOVAROCKS_FRAGMENT_EXECUTOR_FAILURE_INJECTED token=step-token query_hi=10 query_lo=20 finst_hi=102 finst_lo=202\nNOVAROCKS_FAILED_FRAGMENT_REPORT_ACK query_hi=10 query_lo=20 finst_hi=102 finst_lo=202\nNOVAROCKS_CANCEL_FINST query_hi=10 query_lo=20 finst_hi=101 finst_lo=201\n",
        );
        handle.append_log(
            2,
            "NOVAROCKS_GRPC_SUBMIT_ACCEPTED query_hi=10 query_lo=20 finst_hi=103 finst_lo=203\nNOVAROCKS_CANCEL_FINST query_hi=10 query_lo=20 finst_hi=103 finst_lo=203\n",
        );

        let error = run(&step, &handle, &before, &mut String::new())
            .expect_err("globally equal identities assigned to the wrong BEs must fail");

        assert!(
            error.to_string().contains("BE[0] identity mismatch"),
            "{error:#}"
        );
    }

    #[test]
    fn exact_injected_query_cancellation_binds_failure_and_ack_to_the_same_be() {
        let handle =
            FakeBeLogHandle::new(vec!["", "", ""]).with_fragment_failure_token("step-token");
        let step = step(QueryMeta {
            fail_fragment_after_start_be_index: Some(1),
            be_log_exact_fragment_cancellation: Some(3),
            ..QueryMeta::default()
        });
        let before = snapshot(&step.meta, &handle).expect("capture armed trigger token");
        handle.append_log(
            0,
            "NOVAROCKS_GRPC_SUBMIT_ACCEPTED query_hi=10 query_lo=20 finst_hi=101 finst_lo=201\nNOVAROCKS_FAILED_FRAGMENT_REPORT_ACK query_hi=10 query_lo=20 finst_hi=102 finst_lo=202\nNOVAROCKS_CANCEL_FINST query_hi=10 query_lo=20 finst_hi=101 finst_lo=201\n",
        );
        handle.append_log(
            1,
            "NOVAROCKS_GRPC_SUBMIT_ACCEPTED query_hi=10 query_lo=20 finst_hi=105 finst_lo=205\nNOVAROCKS_FRAGMENT_EXECUTOR_FAILURE_INJECTED token=step-token query_hi=10 query_lo=20 finst_hi=102 finst_lo=202\nNOVAROCKS_CANCEL_FINST query_hi=10 query_lo=20 finst_hi=105 finst_lo=205\n",
        );
        handle.append_log(
            2,
            "NOVAROCKS_GRPC_SUBMIT_ACCEPTED query_hi=10 query_lo=20 finst_hi=103 finst_lo=203\nNOVAROCKS_CANCEL_FINST query_hi=10 query_lo=20 finst_hi=103 finst_lo=203\n",
        );

        let error = run(&step, &handle, &before, &mut String::new()).expect_err(
            "the injected identity and its ACK must be proven on the BE that consumed the token",
        );

        assert!(error.to_string().contains("injected fragment"), "{error:#}");
    }

    #[test]
    fn exact_injected_query_cancellation_rejects_duplicate_identity_evidence() {
        let handle =
            FakeBeLogHandle::new(vec!["", "", ""]).with_fragment_failure_token("step-token");
        let step = step(QueryMeta {
            fail_fragment_after_start_be_index: Some(1),
            be_log_exact_fragment_cancellation: Some(3),
            ..QueryMeta::default()
        });
        let before = snapshot(&step.meta, &handle).expect("capture armed trigger token");
        handle.append_log(
            0,
            "NOVAROCKS_GRPC_SUBMIT_ACCEPTED query_hi=10 query_lo=20 finst_hi=101 finst_lo=201\nNOVAROCKS_GRPC_SUBMIT_ACCEPTED query_hi=10 query_lo=20 finst_hi=101 finst_lo=201\nNOVAROCKS_CANCEL_FINST query_hi=10 query_lo=20 finst_hi=101 finst_lo=201\nNOVAROCKS_CANCEL_FINST query_hi=10 query_lo=20 finst_hi=101 finst_lo=201\n",
        );
        handle.append_log(
            1,
            "NOVAROCKS_GRPC_SUBMIT_ACCEPTED query_hi=10 query_lo=20 finst_hi=102 finst_lo=202\nNOVAROCKS_FRAGMENT_EXECUTOR_FAILURE_INJECTED token=step-token query_hi=10 query_lo=20 finst_hi=102 finst_lo=202\nNOVAROCKS_FAILED_FRAGMENT_REPORT_ACK query_hi=10 query_lo=20 finst_hi=102 finst_lo=202\nNOVAROCKS_CANCEL_FINST query_hi=10 query_lo=20 finst_hi=102 finst_lo=202\n",
        );
        handle.append_log(
            2,
            "NOVAROCKS_GRPC_SUBMIT_ACCEPTED query_hi=10 query_lo=20 finst_hi=103 finst_lo=203\nNOVAROCKS_CANCEL_FINST query_hi=10 query_lo=20 finst_hi=103 finst_lo=203\n",
        );

        let error = run(&step, &handle, &before, &mut String::new())
            .expect_err("matching duplicate identity evidence must fail closed");

        assert!(
            error
                .to_string()
                .contains("accepted duplicate fragment identities"),
            "{error:#}"
        );
    }

    #[test]
    fn exact_injected_query_cancellation_requires_declared_be_coverage() {
        let handle = FakeBeLogHandle::new(vec!["", ""]).with_fragment_failure_token("step-token");
        let step = step(QueryMeta {
            fail_fragment_after_start_be_index: Some(1),
            be_log_exact_fragment_cancellation: Some(3),
            ..QueryMeta::default()
        });
        let before = snapshot(&step.meta, &handle).expect("capture armed trigger token");

        let error = run(&step, &handle, &before, &mut String::new())
            .expect_err("a two-BE cluster cannot satisfy a three-BE proof");

        assert!(error.to_string().contains("found 2"), "{error:#}");
    }

    #[test]
    fn exact_injected_query_cancellation_rejects_malformed_markers() {
        let handle =
            FakeBeLogHandle::new(vec!["", "", ""]).with_fragment_failure_token("step-token");
        let step = step(QueryMeta {
            fail_fragment_after_start_be_index: Some(1),
            be_log_exact_fragment_cancellation: Some(3),
            ..QueryMeta::default()
        });
        let before = snapshot(&step.meta, &handle).expect("capture armed trigger token");
        handle.append_log(
            0,
            "NOVAROCKS_GRPC_SUBMIT_ACCEPTED query_hi=10 query_lo=20 finst_hi=101\n",
        );
        handle.append_log(
            1,
            "NOVAROCKS_FRAGMENT_EXECUTOR_FAILURE_INJECTED token=step-token query_hi=10 query_lo=20 finst_hi=102 finst_lo=202\nNOVAROCKS_FAILED_FRAGMENT_REPORT_ACK query_hi=10 query_lo=20 finst_hi=102 finst_lo=202\n",
        );

        let error = run(&step, &handle, &before, &mut String::new())
            .expect_err("malformed identity marker must fail closed");

        assert!(
            error
                .to_string()
                .contains("malformed accepted-fragment marker"),
            "{error:#}"
        );
    }

    #[test]
    fn post_query_fragment_fault_starts_the_shared_evidence_deadline_before_execution() {
        let handle = FakeBeLogHandle::new(vec!["", "", ""]);
        let meta = QueryMeta {
            fail_fragment_after_start_be_index: Some(1),
            ..QueryMeta::default()
        };

        let before = snapshot(&meta, &handle).expect("pre-step snapshot");

        assert!(before.evidence_deadline().is_some());
    }

    #[test]
    fn expired_shared_deadline_rejects_otherwise_satisfied_log_evidence() {
        let handle = FakeBeLogHandle::new(vec!["fresh-marker\n", "", ""]);
        let step = step(QueryMeta {
            be_log_contains: vec!["fresh-marker".to_string()],
            ..QueryMeta::default()
        });
        let before = BeLogSnapshot {
            evidence_deadline: Some(Instant::now()),
            ..BeLogSnapshot::default()
        };

        let error = run(&step, &handle, &before, &mut String::new())
            .expect_err("evidence observed after the shared deadline must fail");

        assert!(error.to_string().contains("timed out"));
        assert!(error.to_string().contains("fe-tail-sentinel"));
    }

    #[test]
    fn be_log_directive_polls_bounded_post_step_deltas_for_async_evidence() {
        let handle = Arc::new(FakeBeLogHandle::new(vec!["old close\n", "", ""]));
        let step = step(QueryMeta {
            be_log_be_count_at_least: vec![(
                "lookup_close direction=receive status=ok".to_string(),
                2,
            )],
            ..QueryMeta::default()
        });
        let before = snapshot(&step.meta, handle.as_ref()).expect("pre-step snapshot");
        let delayed_handle = Arc::clone(&handle);
        let writer = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            delayed_handle.append_log(0, "lookup_close direction=receive status=ok\n");
            delayed_handle.append_log(1, "lookup_close direction=receive status=ok\n");
        });
        let mut log = String::new();

        run(&step, handle.as_ref(), &before, &mut log)
            .expect("bounded polling should observe delayed post-step evidence");
        writer.join().expect("delayed log writer");

        assert!(log.contains("actual=2 required=2"), "{log}");
    }

    #[test]
    fn every_query_lifecycle_hook_receives_one_shared_deadline() {
        for meta in [
            QueryMeta {
                drop_next_init_ack_be_index: Some(0),
                ..QueryMeta::default()
            },
            QueryMeta {
                stop_query_control_heartbeat_be_index: Some(0),
                ..QueryMeta::default()
            },
            QueryMeta {
                kill_fe_after_control_ready_count: Some(1),
                ..QueryMeta::default()
            },
            QueryMeta {
                restart_be_after_init_ack_index: Some(0),
                ..QueryMeta::default()
            },
            QueryMeta {
                kill_query_after_control_ready_count: Some(1),
                ..QueryMeta::default()
            },
            QueryMeta {
                query_control_fragment_backend_limit: Some(1),
                ..QueryMeta::default()
            },
        ] {
            assert!(
                query_lifecycle_step_deadline(&meta).is_some(),
                "lifecycle hook did not receive the shared deadline: {meta:?}"
            );
        }
    }

    #[test]
    fn stale_pre_step_log_marker_does_not_satisfy_directive() {
        let handle = FakeBeLogHandle::new(vec!["be_log_ingress\n", "", ""]);
        let step = step(QueryMeta {
            be_log_contains: vec!["be_log_ingress".to_string()],
            ..QueryMeta::default()
        });
        let before = snapshot(&step.meta, &handle).expect("pre-step snapshot");

        let error = run(&step, &handle, &before, &mut String::new())
            .expect_err("stale marker must not satisfy post-step evidence");

        assert!(error.to_string().contains("no BE log contains pattern"));
    }

    #[test]
    fn service_only_evidence_correlates_one_execution_and_exact_backend_roles() {
        let execution = "10:20:1";
        let be0 = format!(
            "NOVAROCKS_QUERY_CONTROL_READY execution_id={execution} backend_id=0 expected_fragments=1\nNOVAROCKS_QUERY_FRAGMENT_ACCEPTED execution_id={execution} backend_id=0 finst_id=1:1\n"
        );
        let be1 = format!(
            "NOVAROCKS_QUERY_CONTROL_READY execution_id={execution} backend_id=1 expected_fragments=1\nNOVAROCKS_QUERY_FRAGMENT_ACCEPTED execution_id={execution} backend_id=1 finst_id=1:2\n"
        );
        let be2 = format!(
            "NOVAROCKS_QUERY_CONTROL_READY execution_id={execution} backend_id=2 expected_fragments=0\n"
        );
        let handle = FakeBeLogHandle::new(vec![&be0, &be1, &be2]);
        let step = step(QueryMeta {
            query_control_fragment_backend_limit: Some(2),
            ..QueryMeta::default()
        });
        let snapshot = BeLogSnapshot::default();

        let check = lifecycle_evidence(&step, &handle, &snapshot, 3)
            .expect("evaluate evidence")
            .expect("lifecycle check");

        assert!(matches!(check, LogEvidenceCheck::Satisfied(_)));
    }

    #[test]
    fn heartbeat_evidence_requires_token_anchor_terminal_and_cleanup_on_same_execution() {
        let execution = "10:20:1";
        let terminal = format!(
            "NOVAROCKS_QUERY_LIFECYCLE_TERMINATED execution_id={execution} backend_id=0 reason=CoordinatorHeartbeatTimeout expected_fragments=1\nNOVAROCKS_QUERY_LIFECYCLE_CLEANUP execution_id={execution} backend_id=0 active=false tombstone=true reason=CoordinatorHeartbeatTimeout\n"
        );
        let be1 = format!(
            "NOVAROCKS_QUERY_CONTROL_HEARTBEAT_STOPPED execution_id={execution} backend_index=1 backend_id=1 start_epoch=17 token=step-token\n{}",
            terminal.replace("backend_id=0", "backend_id=1")
        );
        let be2 = terminal.replace("backend_id=0", "backend_id=2");
        let handle = FakeBeLogHandle::new(vec![&terminal, &be1, &be2]);
        let step = step(QueryMeta {
            stop_query_control_heartbeat_be_index: Some(1),
            ..QueryMeta::default()
        });
        let snapshot = BeLogSnapshot {
            lifecycle_token: Some((1, "heartbeat-stop", "step-token".to_string())),
            ..BeLogSnapshot::default()
        };

        let check = lifecycle_evidence(&step, &handle, &snapshot, 3)
            .expect("evaluate evidence")
            .expect("lifecycle check");

        assert!(matches!(check, LogEvidenceCheck::Satisfied(_)));
    }

    #[test]
    fn fe_crash_evidence_rejects_terminal_without_cleanup_on_every_backend() {
        let execution = "10:20:1";
        let terminal = format!(
            "NOVAROCKS_QUERY_CONTROL_COORDINATOR_LOST execution_id={execution} backend_id=0 reason=CoordinatorStreamLost\nNOVAROCKS_QUERY_LIFECYCLE_TERMINATED execution_id={execution} backend_id=0 reason=CoordinatorStreamLost expected_fragments=1\n"
        );
        let handle = FakeBeLogHandle::new(vec![
            &terminal,
            &terminal.replace("backend_id=0", "backend_id=1"),
            &terminal.replace("backend_id=0", "backend_id=2"),
        ]);
        let step = step(QueryMeta {
            kill_fe_after_control_ready_count: Some(3),
            ..QueryMeta::default()
        });

        let check = lifecycle_evidence(&step, &handle, &BeLogSnapshot::default(), 3)
            .expect("evaluate evidence")
            .expect("lifecycle check");

        assert!(matches!(check, LogEvidenceCheck::Pending(_)));
    }

    #[test]
    fn kill_query_evidence_rejects_terminal_without_cleanup_on_every_backend() {
        let execution = "10:20:1";
        let terminal = format!(
            "NOVAROCKS_QUERY_LIFECYCLE_TERMINATED execution_id={execution} backend_id=0 reason=CoordinatorAbort expected_fragments=1\n"
        );
        let handle = FakeBeLogHandle::new(vec![
            &terminal,
            &terminal.replace("backend_id=0", "backend_id=1"),
            &terminal.replace("backend_id=0", "backend_id=2"),
        ]);
        let step = step(QueryMeta {
            kill_query_after_control_ready_count: Some(3),
            ..QueryMeta::default()
        });

        let check = lifecycle_evidence(&step, &handle, &BeLogSnapshot::default(), 3)
            .expect("evaluate evidence")
            .expect("lifecycle check");

        assert!(matches!(check, LogEvidenceCheck::Pending(_)));
    }
}
