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

//! Product-owned process scheduler state for materialized-view refresh.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use super::scheduler::MvSchedulerConfig;
use crate::maintenance::{MvBackgroundEngineError, MvBackgroundEngineErrorKind};

/// Process-local failure detail. The cause does not determine scheduler policy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvRefreshFailure {
    message: String,
    compile_control: Option<novarocks_type_contract::CompileControlError>,
    execution: Option<novarocks_query_application::execution_error::ExecutionFailureInput>,
}

impl MvRefreshFailure {
    pub fn new(
        message: impl Into<String>,
        compile_control: Option<novarocks_type_contract::CompileControlError>,
    ) -> Self {
        Self {
            message: message.into(),
            compile_control,
            execution: None,
        }
    }
    /// Retain the original execution projection independently of compile control.
    /// Diagnostic payload does not determine the scheduler's product policy.
    pub fn with_execution_failure(
        mut self,
        execution: Option<novarocks_query_application::execution_error::ExecutionFailureInput>,
    ) -> Self {
        self.execution = execution;
        self
    }
    pub fn execution_failure(
        &self,
    ) -> Option<&novarocks_query_application::execution_error::ExecutionFailureInput> {
        self.execution.as_ref()
    }
    pub fn message(&self) -> &str {
        &self.message
    }
    pub fn compile_control_error(&self) -> Option<novarocks_type_contract::CompileControlError> {
        self.compile_control.clone()
    }
}
impl From<String> for MvRefreshFailure {
    fn from(message: String) -> Self {
        Self::new(message, None)
    }
}
impl From<&str> for MvRefreshFailure {
    fn from(message: &str) -> Self {
        Self::new(message, None)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MvRefreshDisposition {
    Completed,
    NoOp,
    AlreadyActive(MvRefreshFailure),
    TargetGone(MvRefreshFailure),
    TransientUnavailable(MvRefreshFailure),
    InvalidDefinition(MvRefreshFailure),
    TerminalFailure(MvRefreshFailure),
    Corruption(MvRefreshFailure),
    InvariantViolation(MvRefreshFailure),
    ShutdownCancelled(MvRefreshFailure),
}

impl MvRefreshDisposition {
    pub fn compile_control_error(&self) -> Option<novarocks_type_contract::CompileControlError> {
        match self {
            Self::TransientUnavailable(failure)
            | Self::InvalidDefinition(failure)
            | Self::TerminalFailure(failure)
            | Self::Corruption(failure)
            | Self::InvariantViolation(failure)
            | Self::AlreadyActive(failure)
            | Self::TargetGone(failure)
            | Self::ShutdownCancelled(failure) => failure.compile_control_error(),
            Self::Completed | Self::NoOp => None,
        }
    }

    pub fn execution_failure(
        &self,
    ) -> Option<&novarocks_query_application::execution_error::ExecutionFailureInput> {
        match self {
            Self::TransientUnavailable(failure)
            | Self::InvalidDefinition(failure)
            | Self::TerminalFailure(failure)
            | Self::Corruption(failure)
            | Self::InvariantViolation(failure)
            | Self::AlreadyActive(failure)
            | Self::TargetGone(failure)
            | Self::ShutdownCancelled(failure) => failure.execution_failure(),
            Self::Completed | Self::NoOp => None,
        }
    }

    pub fn from_background_error(error: MvBackgroundEngineError) -> Self {
        let failure = || MvRefreshFailure::new(error.message(), error.compile_control_error());
        match error.kind() {
            MvBackgroundEngineErrorKind::TargetGone => Self::TargetGone(failure()),
            MvBackgroundEngineErrorKind::TransientUnavailable => {
                Self::TransientUnavailable(failure())
            }
            MvBackgroundEngineErrorKind::InvalidDefinition => Self::InvalidDefinition(failure()),
            MvBackgroundEngineErrorKind::TerminalFailure => Self::TerminalFailure(failure()),
            MvBackgroundEngineErrorKind::Corruption => Self::Corruption(failure()),
            MvBackgroundEngineErrorKind::InvariantViolation => Self::InvariantViolation(failure()),
            MvBackgroundEngineErrorKind::ShutdownCancelled => Self::ShutdownCancelled(failure()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MvRefreshRuntimeDecision {
    Success,
    TransientBackoff {
        error: MvRefreshFailure,
        retry_at_ms: i64,
    },
    Blocked {
        error: MvRefreshFailure,
    },
    NoChange {
        error: MvRefreshFailure,
    },
}

impl MvRefreshRuntimeDecision {
    pub fn compile_control_error(&self) -> Option<novarocks_type_contract::CompileControlError> {
        match self {
            Self::TransientBackoff { error, .. }
            | Self::Blocked { error }
            | Self::NoChange { error } => error.compile_control_error(),
            Self::Success => None,
        }
    }
    pub fn execution_failure(
        &self,
    ) -> Option<&novarocks_query_application::execution_error::ExecutionFailureInput> {
        match self {
            Self::TransientBackoff { error, .. }
            | Self::Blocked { error }
            | Self::NoChange { error } => error.execution_failure(),
            Self::Success => None,
        }
    }
}

/// Coalesced product runtime.  Hosts interpret durable MV definitions and
/// execute concrete effects, but cannot own a second queue, capacity set, or
/// retry ledger.
#[derive(Debug)]
pub struct MvRefreshSchedulerRuntime<K, R> {
    config: MvSchedulerConfig,
    queue: VecDeque<(K, R)>,
    queued: BTreeSet<K>,
    running: BTreeSet<K>,
    failures: BTreeMap<K, u32>,
    retry_not_before_ms: BTreeMap<K, i64>,
    blocked: BTreeMap<K, MvRefreshFailure>,
}

/// The complete process-local refresh runtime.  In addition to queue and
/// terminal state, it owns the last source revision used to decide whether a
/// definition's cooldown/backoff may survive.  A repository adapter supplies
/// the revision and runnable request; it must not retain a parallel revision
/// ledger.
#[derive(Debug)]
pub struct MvRefreshProductRuntime<K, S, R> {
    scheduler: MvRefreshSchedulerRuntime<K, R>,
    source_revisions: BTreeMap<K, S>,
}

impl<K, S, R> MvRefreshProductRuntime<K, S, R>
where
    K: Clone + Ord,
    S: Eq,
{
    pub fn new(config: MvSchedulerConfig) -> Self {
        Self {
            scheduler: MvRefreshSchedulerRuntime::new(config),
            source_revisions: BTreeMap::new(),
        }
    }

    /// Starts one repository/provider observation.  It first invalidates
    /// obsolete process-local suppression using the exact frozen source
    /// revision, then reports whether discovery may perform provider I/O.
    pub fn begin_observation(&mut self, key: K, source_revision: S, now_ms: i64) -> bool {
        let source_changed = self
            .source_revisions
            .get(&key)
            .is_some_and(|current| current != &source_revision);
        if source_changed {
            self.scheduler.reset_after_source_change(&key);
        }
        self.source_revisions.insert(key.clone(), source_revision);
        !self.scheduler.is_suppressed(&key, now_ms)
    }

    pub const fn enabled(&self) -> bool {
        self.scheduler.enabled()
    }

    /// A delayed provider reply may only affect the source revision that
    /// requested it; it cannot queue or quarantine a newer projection.
    pub fn is_current_source(&self, key: &K, source_revision: &S) -> bool {
        self.source_revisions.get(key) == Some(source_revision)
    }

    pub fn enqueue(&mut self, key: K, request: R) {
        self.scheduler.enqueue(key, request);
    }

    pub fn take_ready(&mut self) -> Vec<R> {
        self.scheduler.take_ready()
    }

    pub fn mark_started(&mut self, key: &K) -> bool {
        self.scheduler.mark_started(key)
    }

    pub fn requeue(&mut self, key: K, request: R) {
        self.scheduler.requeue(key, request);
    }

    pub fn complete(
        &mut self,
        key: &K,
        disposition: MvRefreshDisposition,
        now_ms: i64,
    ) -> MvRefreshRuntimeDecision {
        self.scheduler.complete(key, disposition, now_ms)
    }

    pub fn record(
        &mut self,
        key: &K,
        disposition: MvRefreshDisposition,
        now_ms: i64,
    ) -> MvRefreshRuntimeDecision {
        self.scheduler.record(key, disposition, now_ms)
    }

    pub fn pending_len(&self) -> usize {
        self.scheduler.pending_len()
    }

    pub fn running_len(&self) -> usize {
        self.scheduler.running_len()
    }
}

impl<K, R> MvRefreshSchedulerRuntime<K, R>
where
    K: Clone + Ord,
{
    pub fn new(config: MvSchedulerConfig) -> Self {
        Self {
            config,
            queue: VecDeque::new(),
            queued: BTreeSet::new(),
            running: BTreeSet::new(),
            failures: BTreeMap::new(),
            retry_not_before_ms: BTreeMap::new(),
            blocked: BTreeMap::new(),
        }
    }

    pub const fn enabled(&self) -> bool {
        self.config.enabled()
    }

    pub fn is_suppressed(&self, key: &K, now_ms: i64) -> bool {
        self.retry_not_before_ms
            .get(key)
            .is_some_and(|retry_at_ms| now_ms < *retry_at_ms)
            || self.blocked.contains_key(key)
            || self.queued.contains(key)
            || self.running.contains(key)
    }

    pub fn enqueue(&mut self, key: K, request: R) {
        if !self.running.contains(&key) && self.queued.insert(key.clone()) {
            self.queue.push_back((key, request));
        }
    }

    pub fn take_ready(&mut self) -> Vec<R> {
        let capacity = self
            .config
            .max_concurrent_refreshes()
            .max(1)
            .saturating_sub(self.running.len());
        let mut ready = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            let Some((key, request)) = self.queue.pop_front() else {
                break;
            };
            self.queued.remove(&key);
            if !self.running.contains(&key) {
                ready.push(request);
            }
        }
        ready
    }

    pub fn mark_started(&mut self, key: &K) -> bool {
        if self.running.len() >= self.config.max_concurrent_refreshes().max(1)
            || self.running.contains(key)
        {
            return false;
        }
        self.running.insert(key.clone())
    }

    pub fn requeue(&mut self, key: K, request: R) {
        self.enqueue(key, request);
    }

    pub fn complete(
        &mut self,
        key: &K,
        disposition: MvRefreshDisposition,
        now_ms: i64,
    ) -> MvRefreshRuntimeDecision {
        self.running.remove(key);
        self.record(key, disposition, now_ms)
    }

    pub fn record(
        &mut self,
        key: &K,
        disposition: MvRefreshDisposition,
        now_ms: i64,
    ) -> MvRefreshRuntimeDecision {
        let decision = match disposition {
            MvRefreshDisposition::Completed | MvRefreshDisposition::NoOp => {
                MvRefreshRuntimeDecision::Success
            }
            MvRefreshDisposition::TransientUnavailable(error) => {
                let attempt = *self
                    .failures
                    .entry(key.clone())
                    .and_modify(|attempt| *attempt = attempt.saturating_add(1))
                    .or_insert(1);
                MvRefreshRuntimeDecision::TransientBackoff {
                    error,
                    retry_at_ms: now_ms.saturating_add(backoff_ms(&self.config, attempt)),
                }
            }
            MvRefreshDisposition::InvalidDefinition(error)
            | MvRefreshDisposition::TerminalFailure(error)
            | MvRefreshDisposition::Corruption(error)
            | MvRefreshDisposition::InvariantViolation(error) => {
                MvRefreshRuntimeDecision::Blocked { error }
            }
            MvRefreshDisposition::AlreadyActive(error)
            | MvRefreshDisposition::TargetGone(error)
            | MvRefreshDisposition::ShutdownCancelled(error) => {
                MvRefreshRuntimeDecision::NoChange { error }
            }
        };
        match &decision {
            MvRefreshRuntimeDecision::Success => {
                self.failures.remove(key);
                self.retry_not_before_ms.remove(key);
                self.blocked.remove(key);
            }
            MvRefreshRuntimeDecision::TransientBackoff { retry_at_ms, .. } => {
                self.retry_not_before_ms.insert(key.clone(), *retry_at_ms);
                self.blocked.remove(key);
            }
            MvRefreshRuntimeDecision::Blocked { error } => {
                self.failures.remove(key);
                self.retry_not_before_ms.remove(key);
                self.blocked.insert(key.clone(), error.clone());
            }
            MvRefreshRuntimeDecision::NoChange { .. } => {}
        }
        decision
    }

    pub fn reset_after_source_change(&mut self, key: &K) {
        self.failures.remove(key);
        self.retry_not_before_ms.remove(key);
        self.blocked.remove(key);
    }

    pub fn pending_len(&self) -> usize {
        self.queue.len()
    }

    pub fn running_len(&self) -> usize {
        self.running.len()
    }
}

fn backoff_ms(config: &MvSchedulerConfig, attempt: u32) -> i64 {
    let base = config.failure_backoff_ms().max(1);
    let maximum = config.max_failure_backoff_ms().max(base);
    let shift = attempt.saturating_sub(1).min(62);
    base.saturating_mul(1_i64.checked_shl(shift).unwrap_or(i64::MAX))
        .min(maximum)
}

#[cfg(test)]
mod tests {
    use super::{
        MvRefreshDisposition, MvRefreshProductRuntime, MvRefreshRuntimeDecision,
        MvRefreshSchedulerRuntime,
    };
    use crate::scheduler::MvSchedulerConfig;

    #[test]
    fn runtime_coalesces_and_releases_only_typed_terminal_work() {
        let mut runtime =
            MvRefreshSchedulerRuntime::new(MvSchedulerConfig::new(true, 1, 1, 10, 40));
        runtime.enqueue(7_i64, "first");
        runtime.enqueue(7_i64, "duplicate");
        assert_eq!(runtime.take_ready(), ["first"]);
        assert!(runtime.mark_started(&7));
        assert!(runtime.take_ready().is_empty());
        assert_eq!(
            runtime.complete(
                &7,
                MvRefreshDisposition::TransientUnavailable("offline".into()),
                100
            ),
            MvRefreshRuntimeDecision::TransientBackoff {
                error: "offline".into(),
                retry_at_ms: 110,
            }
        );
        assert!(runtime.is_suppressed(&7, 109));
        runtime.reset_after_source_change(&7);
        assert!(!runtime.is_suppressed(&7, 109));
    }

    #[test]
    fn source_revision_reset_is_owned_with_queue_and_backoff_state() {
        let mut runtime = MvRefreshProductRuntime::<i64, &str, ()>::new(MvSchedulerConfig::new(
            true, 1, 1, 10, 40,
        ));
        assert!(runtime.begin_observation(7_i64, "first", 100));
        assert!(matches!(
            runtime.record(
                &7,
                MvRefreshDisposition::TransientUnavailable("offline".into()),
                100,
            ),
            MvRefreshRuntimeDecision::TransientBackoff { .. }
        ));
        assert!(!runtime.begin_observation(7, "first", 101));
        assert!(runtime.begin_observation(7, "changed", 101));
    }
    #[test]
    fn compile_control_cause_survives_scheduler_disposition_decision_and_blocked_state() {
        use crate::maintenance::{MvBackgroundEngineError, MvBackgroundEngineErrorKind as K};
        use novarocks_type_contract::CompileControlError as C;
        for control in [C::Cancelled, C::DeadlineExceeded, C::ResourceExhausted] {
            for kind in [
                K::TargetGone,
                K::TransientUnavailable,
                K::InvalidDefinition,
                K::TerminalFailure,
                K::Corruption,
                K::InvariantViolation,
                K::ShutdownCancelled,
            ] {
                let mut runtime = MvRefreshSchedulerRuntime::<i64, ()>::new(
                    MvSchedulerConfig::new(true, 1, 1, 10, 40),
                );
                let error = MvBackgroundEngineError::new(kind, "actual failure")
                    .with_compile_control(Some(control.clone()));
                let disposition = MvRefreshDisposition::from_background_error(error);
                assert_eq!(disposition.compile_control_error(), Some(control.clone()));
                let decision = runtime.record(&7, disposition, 100);
                assert_eq!(decision.compile_control_error(), Some(control.clone()));
                match kind {
                    K::TransientUnavailable => {
                        assert!(matches!(
                            decision,
                            MvRefreshRuntimeDecision::TransientBackoff {
                                retry_at_ms: 110,
                                ..
                            }
                        ));
                        assert!(runtime.is_suppressed(&7, 109));
                        assert!(!runtime.is_suppressed(&7, 110));
                    }
                    K::TargetGone | K::ShutdownCancelled => {
                        assert!(matches!(
                            decision,
                            MvRefreshRuntimeDecision::NoChange { .. }
                        ));
                        assert!(!runtime.is_suppressed(&7, 100));
                    }
                    _ => {
                        assert!(matches!(decision, MvRefreshRuntimeDecision::Blocked { .. }));
                        let retained = runtime.blocked.get(&7).expect("blocked terminal retained");
                        assert_eq!(retained.compile_control_error(), Some(control.clone()));
                        assert_eq!(retained.message(), "actual failure");
                        assert!(runtime.is_suppressed(&7, 100));
                    }
                }
                runtime.reset_after_source_change(&7);
                assert!(!runtime.is_suppressed(&7, 100));
            }
        }
    }
}
