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

use crate::cluster::{ClusterMode, TestClusterOwner};
use crate::types::{QueryMeta, SqlStep};
use anyhow::{Context, Result, bail};
use std::collections::{HashMap, HashSet};
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

#[derive(Debug, Default)]
pub(crate) struct BeLogSnapshot {
    counts: HashMap<(usize, String), usize>,
    evidence_deadline: Option<Instant>,
    original_work_deadline: Option<Instant>,
}

impl BeLogSnapshot {
    pub(crate) fn evidence_deadline(&self) -> Option<Instant> {
        self.evidence_deadline
    }
}

#[cfg(test)]
pub(crate) fn snapshot(
    meta: &QueryMeta,
    cluster_owner: &dyn TestClusterOwner,
) -> Result<BeLogSnapshot> {
    snapshot_with_deadline(meta, cluster_owner, step_evidence_deadline(meta), None)
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
    (meta
        .restart_be_after_establish_participation_index
        .is_some()
        || meta.kill_fe_after_be_log_contains.is_some()
        || is_query_lifecycle_step(meta))
    .then(|| Instant::now() + QUERY_LIFECYCLE_STEP_TIMEOUT)
}

fn is_query_lifecycle_step(meta: &QueryMeta) -> bool {
    // A step named here gets the long query-lifecycle evidence budget rather
    // than the short BE-log one, because the facts it waits for are published
    // by the coordinator's own lifecycle rather than by the statement.
    //
    // `cancel_query_after_be_log_contains` is deliberately NOT in the list. It
    // waits for whatever marker the case names, which makes it
    // protocol-neutral: a case that triggers a kill from task-protocol
    // evidence and asserts task-protocol evidence would otherwise be held to
    // terminal facts that no longer have producers, and would fail for a
    // reason unrelated to whether cancellation worked.
    meta.query_lifecycle_fault.is_some()
}

pub(crate) fn snapshot_with_deadline(
    meta: &QueryMeta,
    cluster_owner: &dyn TestClusterOwner,
    evidence_deadline: Option<Instant>,
    original_work_deadline: Option<Instant>,
) -> Result<BeLogSnapshot> {
    if !has_directives(meta) {
        return Ok(BeLogSnapshot {
            evidence_deadline,
            original_work_deadline,
            ..BeLogSnapshot::default()
        });
    }
    let be_count = cluster_owner.be_count();
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
    for pattern in patterns {
        for index in 0..be_count {
            counts.insert(
                (index, pattern.clone()),
                cluster_owner.be_log_count(index, pattern)?,
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
        cluster_owner
            .armed_query_lifecycle_fault_token(index, kind)?
            .with_context(|| format!("BE[{index}] has no armed {kind} token"))?;
    }
    Ok(BeLogSnapshot {
        counts,
        evidence_deadline,
        original_work_deadline,
    })
}

fn log_delta(
    snapshot: &BeLogSnapshot,
    cluster_owner: &dyn TestClusterOwner,
    index: usize,
    pattern: &str,
) -> Result<usize> {
    let before = snapshot
        .counts
        .get(&(index, pattern.to_string()))
        .copied()
        .unwrap_or(0);
    let after = cluster_owner.be_log_count(index, pattern)?;
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

fn evaluate_log_evidence(
    step: &SqlStep,
    cluster_owner: &dyn TestClusterOwner,
    snapshot: &BeLogSnapshot,
    endpoint_count: usize,
) -> Result<LogEvidenceCheck> {
    let mut successes = Vec::new();
    let mut pending = Vec::new();

    for pattern in &step.meta.be_log_contains {
        let mut total = 0usize;
        for index in 0..endpoint_count {
            total = total
                .checked_add(log_delta(snapshot, cluster_owner, index, pattern)?)
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
                .checked_add(log_delta(snapshot, cluster_owner, index, pattern)?)
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
                .checked_add(log_delta(snapshot, cluster_owner, index, pattern)?)
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
            if log_delta(snapshot, cluster_owner, index, pattern)? > 0 {
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

    if pending.is_empty() {
        Ok(LogEvidenceCheck::Satisfied(successes))
    } else {
        Ok(LogEvidenceCheck::Pending(pending.join("; ")))
    }
}

pub(crate) fn run(
    step: &SqlStep,
    cluster_owner: &dyn TestClusterOwner,
    snapshot: &BeLogSnapshot,
    log: &mut String,
) -> Result<()> {
    if !has_directives(&step.meta) {
        return Ok(());
    }
    if step.meta.has_be_log_directives() {
        let be_count = cluster_owner.be_count();
        if be_count == 0 {
            bail!("BE log evidence directives require at least one runner-owned BE");
        }
        let started = Instant::now();
        // Ordinary evidence gets its short window only after SQL returns.
        // A lifecycle step keeps its original pre-SQL clock; neither can
        // extend the fixture's original work cut.
        let evidence_deadline = snapshot
            .evidence_deadline
            .unwrap_or(started + LOG_EVIDENCE_TIMEOUT);
        let deadline = snapshot
            .original_work_deadline
            .map_or(evidence_deadline, |end| end.min(evidence_deadline));
        let mut pending_reason = "evidence was not evaluated".to_string();
        loop {
            if Instant::now() >= deadline {
                let elapsed = started.elapsed();
                let fe_tail = cluster_owner
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
                        cluster_owner
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
            match evaluate_log_evidence(step, cluster_owner, snapshot, be_count)? {
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

    impl TestClusterOwner for FakeBeLogHandle {
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
    fn native_cross_process_mode_allows_be_log_evidence() {
        let meta = QueryMeta {
            be_log_contains: vec![
                novarocks_failpoint::observation_marker::TASK_CREATE_ACK_DROPPED.to_string(),
            ],
            ..QueryMeta::default()
        };

        validate_mode(&meta, ClusterMode::CrossProcess)
            .expect("runner-owned native BE logs must support evidence directives");
        validate_mode(&meta, ClusterMode::AllInOne)
            .expect_err("all-in-one has no runner-owned BE logs");
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
        let handle = FakeBeLogHandle::new(vec![
            &format!(
                "{} old\n",
                novarocks_failpoint::observation_marker::CONNECTOR_WRITER_OPENED
            ),
            "",
            "",
        ]);
        let step = step(QueryMeta {
            be_log_not_contains: vec![
                novarocks_failpoint::observation_marker::CONNECTOR_WRITER_OPENED.to_string(),
            ],
            ..QueryMeta::default()
        });
        let mut log = String::new();
        let before = snapshot(&step.meta, &handle).expect("pre-step snapshot");

        run(&step, &handle, &before, &mut log)
            .expect("pre-step markers must not fail a step-scoped assertion");

        assert!(log.contains(&format!(
            "@be_log_not_contains PASS pattern={:?}",
            novarocks_failpoint::observation_marker::CONNECTOR_WRITER_OPENED
        )));
    }

    #[test]
    fn negative_log_directive_rejects_post_step_marker() {
        let handle = FakeBeLogHandle::new(vec!["", "", ""]);
        let step = step(QueryMeta {
            be_log_not_contains: vec![
                novarocks_failpoint::observation_marker::CONNECTOR_WRITER_OPENED.to_string(),
            ],
            ..QueryMeta::default()
        });
        let mut log = String::new();
        let before = snapshot(&step.meta, &handle).expect("pre-step snapshot");
        handle.append_log(
            1,
            &format!(
                "{} new\n",
                novarocks_failpoint::observation_marker::CONNECTOR_WRITER_OPENED
            ),
        );

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
            restart_be_after_establish_participation_index: Some(1),
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
    fn ordinary_evidence_window_starts_after_elapsed_sql_time() {
        let handle = Arc::new(FakeBeLogHandle::new(vec!["", "", ""]));
        let step = step(QueryMeta {
            be_log_contains: vec!["post-query-marker".to_string()],
            ..QueryMeta::default()
        });
        let before = snapshot_with_deadline(
            &step.meta,
            handle.as_ref(),
            step_evidence_deadline(&step.meta),
            Some(Instant::now() + QUERY_LIFECYCLE_STEP_TIMEOUT),
        )
        .expect("pre-step snapshot");
        assert!(before.evidence_deadline().is_none());
        // Model SQL taking longer than the ordinary evidence window.
        std::thread::sleep(LOG_EVIDENCE_TIMEOUT + LOG_EVIDENCE_POLL_INTERVAL);
        let delayed_handle = Arc::clone(&handle);
        let writer = std::thread::spawn(move || {
            std::thread::sleep(LOG_EVIDENCE_POLL_INTERVAL);
            delayed_handle.append_log(0, "post-query-marker\n");
        });
        let mut log = String::new();

        let result = run(&step, handle.as_ref(), &before, &mut log);
        writer.join().expect("post-query log writer");

        result.expect("elapsed SQL time must not consume the post-query evidence window");
        assert!(log.contains("@be_log_contains PASS"), "{log}");
    }

    #[test]
    fn ordinary_evidence_does_not_borrow_the_later_work_deadline() {
        let handle = FakeBeLogHandle::new(vec!["", "", ""]);
        let step = step(QueryMeta {
            be_log_contains: vec!["missing-marker".to_string()],
            ..QueryMeta::default()
        });
        let original_work_deadline = Instant::now() + QUERY_LIFECYCLE_STEP_TIMEOUT;
        let before = snapshot_with_deadline(
            &step.meta,
            &handle,
            step_evidence_deadline(&step.meta),
            Some(original_work_deadline),
        )
        .expect("pre-step snapshot");
        let mut log = String::new();

        let error = run(&step, &handle, &before, &mut log)
            .expect_err("the ordinary evidence window must expire before the work cut");

        assert!(error.to_string().contains("timed out"));
        assert!(
            Instant::now() < original_work_deadline,
            "the missing marker must time out while the original work cut is still in the future"
        );
        assert!(!log.contains("@be_log_contains PASS"), "{log}");
    }

    #[test]
    fn original_work_and_lifecycle_expiry_cannot_be_resurrected_by_fresh_evidence() {
        let expired = Instant::now();
        let later = expired + QUERY_LIFECYCLE_STEP_TIMEOUT;
        for (evidence_deadline, original_work_deadline) in [
            (None, Some(expired)),
            (Some(later), Some(expired)),
            (Some(expired), Some(later)),
        ] {
            let handle = FakeBeLogHandle::new(vec!["", "", ""]);
            let step = step(QueryMeta {
                be_log_contains: vec!["fresh-marker".to_string()],
                ..QueryMeta::default()
            });
            let before = snapshot_with_deadline(
                &step.meta,
                &handle,
                evidence_deadline,
                original_work_deadline,
            )
            .expect("pre-step snapshot");
            handle.append_log(0, "fresh-marker\n");
            let mut log = String::new();

            let error = run(&step, &handle, &before, &mut log)
                .expect_err("neither fresh evidence nor a later clock can revive an expired cut");

            assert!(error.to_string().contains("timed out"));
            assert!(!log.contains("@be_log_contains PASS"), "{log}");
        }
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
        let meta = QueryMeta {
            query_lifecycle_fault: Some(crate::types::QueryLifecycleFaultDirective {
                kind: crate::types::QueryLifecycleFaultKind::RuntimeFilterContributionAckDrop,
                be_index: 0,
            }),
            ..QueryMeta::default()
        };
        assert!(
            query_lifecycle_step_deadline(&meta).is_some(),
            "lifecycle hook did not receive the shared deadline: {meta:?}"
        );
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
}
