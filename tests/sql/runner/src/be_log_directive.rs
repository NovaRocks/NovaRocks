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
    log_lengths: Vec<usize>,
    evidence_deadline: Option<Instant>,
}

impl BeLogSnapshot {
    pub(crate) fn evidence_deadline(&self) -> Option<Instant> {
        self.evidence_deadline
    }
}

#[cfg(test)]
pub(crate) fn snapshot(
    meta: &QueryMeta,
    server_handle: &dyn ServerHandle,
) -> Result<BeLogSnapshot> {
    snapshot_with_deadline(meta, server_handle, step_evidence_deadline(meta))
}

#[cfg(test)]
pub(crate) fn query_lifecycle_step_deadline(meta: &QueryMeta) -> Option<Instant> {
    is_query_lifecycle_step(meta).then(|| Instant::now() + QUERY_LIFECYCLE_STEP_TIMEOUT)
}

pub(crate) fn step_evidence_deadline(meta: &QueryMeta) -> Option<Instant> {
    // The two task-protocol process faults get the same step budget without
    // joining `is_query_lifecycle_step`: they need the shared deadline the
    // post-query worker runs against, but they must not be held to the
    // retired protocol's terminal facts.
    (meta.restart_be_after_establish_context_index.is_some()
        || meta.kill_fe_after_be_log_contains.is_some()
        || is_query_lifecycle_step(meta))
    .then(|| Instant::now() + QUERY_LIFECYCLE_STEP_TIMEOUT)
}

fn is_query_lifecycle_step(meta: &QueryMeta) -> bool {
    // A step named here gets the long query-lifecycle evidence budget rather
    // than the short BE-log one, because the facts it waits for are published
    // by the coordinator's own lifecycle rather than by the statement.
    //
    // `kill_query_after_be_log_contains` is deliberately NOT in the list. It
    // waits for whatever marker the case names, which makes it
    // protocol-neutral: a case that triggers a kill from task-protocol
    // evidence and asserts task-protocol evidence would otherwise be held to
    // terminal facts that no longer have producers, and would fail for a
    // reason unrelated to whether cancellation worked.
    meta.query_control_fragment_backend_limit.is_some() || meta.query_lifecycle_fault.is_some()
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
    // A configured owner-local lifecycle fault must already have reached its
    // arm file before the step runs. The token itself is not read here; the
    // point is to fail the step now rather than to wait out an evidence
    // deadline for a fault that was never armed.
    if let Some(fault) = meta.query_lifecycle_fault {
        let index = fault.be_index;
        let kind = fault.kind.as_str();
        server_handle
            .armed_query_lifecycle_fault_token(index, kind)?
            .with_context(|| format!("BE[{index}] has no armed {kind} token"))?;
    }
    Ok(BeLogSnapshot {
        counts,
        log_lengths,
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
    if !is_query_lifecycle_step(&step.meta) {
        return Ok(None);
    }
    // T4 deliberately creates the arm/bind/token contract before T5/T7/T9
    // add owner markers and structured snapshot producers. Do not turn this
    // temporary absence into a log-text assertion; T10 consumes the typed
    // structured assertion contract instead.
    if step.meta.query_lifecycle_fault.is_some() {
        return Ok(None);
    }
    // The fragment backend limit is the only remaining lifecycle step that
    // owns log-derived evidence.
    let Some(limit) = step.meta.query_control_fragment_backend_limit else {
        return Ok(None);
    };
    if endpoint_count != 3 {
        bail!("query lifecycle evidence requires exactly 3 BEs, found {endpoint_count}");
    }
    let logs = lifecycle_log_deltas(snapshot, server_handle, endpoint_count)?;

    if limit > endpoint_count {
        bail!(
            "query-control fragment backend limit {limit} exceeds available BE count {endpoint_count}"
        );
    }
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
        let expected_service_only = endpoint_count.saturating_sub(limit);
        if services.len() == expected_service_only
            && executors.len() == limit
            && services.is_disjoint(&executors)
        {
            let kind = if expected_service_only == 0 {
                "all-executors"
            } else {
                "service-only"
            };
            return Ok(Some(LogEvidenceCheck::Satisfied(vec![format!(
                "    query_lifecycle_evidence PASS kind={kind} execution_id={execution} participants={endpoint_count} executors={limit} service_only={expected_service_only}",
            )])));
        }
    }
    Ok(Some(LogEvidenceCheck::Pending(format!(
        "no single execution proves {endpoint_count} participants, {} service-only participants, and {limit} fragment executors",
        endpoint_count.saturating_sub(limit)
    ))))
}

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
        successes.push(format!("    @be_log_not_contains PASS pattern={pattern:?}"));
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
    use anyhow::Result;
    use std::sync::{Arc, Mutex};

    struct FakeBeLogHandle {
        logs: Mutex<Vec<String>>,
    }

    impl FakeBeLogHandle {
        fn new(logs: Vec<&str>) -> Self {
            Self {
                logs: Mutex::new(logs.into_iter().map(ToString::to_string).collect()),
            }
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

        assert!(
            log.contains("@be_log_not_contains PASS pattern=\"NOVAROCKS_CONNECTOR_WRITER_OPENED\"")
        );
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

        assert!(
            error
                .to_string()
                .contains("unexpectedly contains forbidden step-scoped pattern")
        );
    }

    #[test]
    fn post_query_process_fault_starts_the_shared_evidence_deadline_before_execution() {
        let handle = FakeBeLogHandle::new(vec!["", "", ""]);
        let meta = QueryMeta {
            restart_be_after_establish_context_index: Some(1),
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
                query_control_fragment_backend_limit: Some(1),
                ..QueryMeta::default()
            },
            QueryMeta {
                query_lifecycle_fault: Some(crate::types::QueryLifecycleFaultDirective {
                    kind: crate::types::QueryLifecycleFaultKind::TerminalOutcomeSuppress,
                    be_index: 0,
                }),
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
            "NOVAROCKS_QUERY_CONTROL_READY execution_id={execution} process_id=018f3d8a-2b4c-7d6e-8f90-123456789abc expected_fragments=1\nNOVAROCKS_QUERY_FRAGMENT_ACCEPTED execution_id={execution} process_id=018f3d8a-2b4c-7d6e-8f90-123456789abc finst_id=1:1\n"
        );
        let be1 = format!(
            "NOVAROCKS_QUERY_CONTROL_READY execution_id={execution} process_id=018f3d8a-2b4c-7d6e-8f90-123456789abd expected_fragments=1\nNOVAROCKS_QUERY_FRAGMENT_ACCEPTED execution_id={execution} process_id=018f3d8a-2b4c-7d6e-8f90-123456789abd finst_id=1:2\n"
        );
        let be2 = format!(
            "NOVAROCKS_QUERY_CONTROL_READY execution_id={execution} process_id=018f3d8a-2b4c-7d6e-8f90-123456789abe expected_fragments=0\n"
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
    fn all_executor_evidence_accepts_each_backend_receiving_a_fragment() {
        let execution = "10:20:1";
        let logs = (0..3)
            .map(|backend| {
                format!(
                    "NOVAROCKS_QUERY_CONTROL_READY execution_id={execution} process_id=018f3d8a-2b4c-7d6e-8f90-123456789ab{backend} expected_fragments=1\nNOVAROCKS_QUERY_FRAGMENT_ACCEPTED execution_id={execution} process_id=018f3d8a-2b4c-7d6e-8f90-123456789ab{backend} finst_id=1:{backend}\n"
                )
            })
            .collect::<Vec<_>>();
        let handle = FakeBeLogHandle::new(logs.iter().map(String::as_str).collect());
        let step = step(QueryMeta {
            query_control_fragment_backend_limit: Some(3),
            ..QueryMeta::default()
        });

        let check = lifecycle_evidence(&step, &handle, &BeLogSnapshot::default(), 3)
            .expect("evaluate evidence")
            .expect("lifecycle check");

        assert!(matches!(check, LogEvidenceCheck::Satisfied(_)));
    }
}
