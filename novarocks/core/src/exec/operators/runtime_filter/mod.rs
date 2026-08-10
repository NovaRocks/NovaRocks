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

use std::collections::BTreeSet;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use novarocks_execution::runtime_filter as execution;

use crate::exec::chunk::Chunk;
use crate::exec::expr::ExprArena;
use crate::exec::node::runtime_filter::{
    RuntimeFilterConsumerBinding, RuntimeFilterExecutionContract,
};
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::profile::{
    OperatorProfiles, ProfileUnit, RUNTIME_FILTER_INPUT_ROWS, RUNTIME_FILTER_OUTPUT_ROWS,
};
use crate::runtime::runtime_state::RuntimeState;
use arrow::compute::filter_record_batch;

pub(crate) struct NativeOrderedLiveConsumerSet {
    inner: Arc<NativeOrderedLiveConsumerInner>,
}

struct NativeOrderedLiveConsumerInner {
    arena: Arc<ExprArena>,
    bindings: Mutex<Vec<NativeOrderedLiveBinding>>,
}

#[derive(Clone)]
struct NativeOrderedLiveBinding {
    spec: RuntimeFilterConsumerBinding,
    state: NativeOrderedLiveBindingState,
}

#[derive(Clone)]
enum NativeOrderedLiveBindingState {
    Unbound,
    BoundExecutionLive {
        subscription: Arc<dyn execution::NonBlockingLiveSubscription>,
        observed: Option<execution::LogicalVersion>,
        latest_snapshot: Option<Arc<execution::RuntimeFilterSnapshot>>,
        terminal: Option<execution::LiveTerminal>,
    },
    PassThrough,
}

enum NativeOrderedPredicateForApply {
    Execution(Arc<execution::RuntimeFilterSnapshot>),
}

impl Clone for NativeOrderedLiveConsumerSet {
    fn clone(&self) -> Self {
        let bindings = self
            .inner
            .bindings
            .lock()
            .expect("native ordered RF consumer lock")
            .clone();
        Self {
            inner: Arc::new(NativeOrderedLiveConsumerInner {
                arena: self.inner.arena.clone(),
                bindings: Mutex::new(bindings),
            }),
        }
    }
}

impl NativeOrderedLiveConsumerSet {
    pub(crate) fn scan_domain_snapshots(
        &self,
    ) -> Result<
        Vec<(
            execution::scan_domain::RuntimeFilterScanDomainBinding,
            Option<Arc<execution::RuntimeFilterSnapshot>>,
        )>,
        String,
    > {
        self.poll_updates()?;
        let bindings = self
            .inner
            .bindings
            .lock()
            .expect("native ordered RF consumer lock");
        Ok(bindings
            .iter()
            .filter_map(|binding| {
                let target = binding.spec.scan_domain.clone()?;
                let snapshot = match &binding.state {
                    NativeOrderedLiveBindingState::BoundExecutionLive {
                        latest_snapshot, ..
                    } => latest_snapshot.clone(),
                    _ => None,
                };
                Some((target, snapshot))
            })
            .collect())
    }

    pub(crate) fn from_plan(
        specs: &[RuntimeFilterConsumerBinding],
        arena: Arc<ExprArena>,
    ) -> Result<Self, String> {
        validate_ordered_live_plan_specs(specs, &arena)?;
        Ok(Self {
            inner: Arc::new(NativeOrderedLiveConsumerInner {
                arena,
                bindings: Mutex::new(
                    specs
                        .iter()
                        .cloned()
                        .map(|spec| NativeOrderedLiveBinding {
                            spec,
                            state: NativeOrderedLiveBindingState::Unbound,
                        })
                        .collect(),
                ),
            }),
        })
    }

    pub(crate) fn bind(&self, state: &RuntimeState) -> Result<(), String> {
        let mut bindings = self
            .inner
            .bindings
            .lock()
            .expect("native ordered RF consumer lock");
        if bindings
            .iter()
            .all(|binding| !matches!(binding.state, NativeOrderedLiveBindingState::Unbound))
        {
            return Ok(());
        }
        let Some(session) = state.runtime_filter_session() else {
            if bindings.is_empty() {
                return Ok(());
            }
            return Err(
                "native ordered runtime-filter consumers require an installed execution context"
                    .into(),
            );
        };
        for binding in bindings.iter_mut() {
            if !matches!(binding.state, NativeOrderedLiveBindingState::Unbound) {
                continue;
            }
            let request = execution::RuntimeFilterSubscriptionRequest::new(
                execution_ordered_live_consumer_contract(&binding.spec)?,
            );
            match execution::RuntimeFilterSession::subscribe(session.as_ref(), request) {
                Ok(execution::RuntimeFilterBindOutcome::Bound(
                    execution::RuntimeFilterSubscriptionHandle::Live(subscription),
                )) => {
                    binding.state = NativeOrderedLiveBindingState::BoundExecutionLive {
                        subscription,
                        observed: None,
                        latest_snapshot: None,
                        terminal: None,
                    };
                }
                Ok(execution::RuntimeFilterBindOutcome::Unavailable(_)) => {
                    binding.state = NativeOrderedLiveBindingState::PassThrough;
                }
                Ok(_) => {
                    return Err(format!(
                        "native ordered runtime-filter binding_id={} session returned a non-live subscription",
                        binding.spec.binding_id()
                    ));
                }
                Err(error)
                    if error.kind()
                        == execution::RuntimeFilterContractViolationKind::SessionClosed =>
                {
                    binding.state = NativeOrderedLiveBindingState::PassThrough;
                }
                Err(error) => return Err(error.to_string()),
            }
        }
        Ok(())
    }

    pub(crate) fn poll_and_apply_chunk(&self, chunk: Chunk) -> Result<Option<Chunk>, String> {
        self.poll_and_apply_chunk_profiled(chunk, None)
    }

    pub(crate) fn poll_and_apply_chunk_profiled(
        &self,
        chunk: Chunk,
        profiles: Option<&OperatorProfiles>,
    ) -> Result<Option<Chunk>, String> {
        self.poll_updates()?;
        self.apply_latest_chunk_profiled(chunk, profiles)
    }

    pub(crate) fn poll_updates(&self) -> Result<(), String> {
        self.poll()
    }

    pub(crate) fn apply_latest_chunk_profiled(
        &self,
        chunk: Chunk,
        profiles: Option<&OperatorProfiles>,
    ) -> Result<Option<Chunk>, String> {
        let (output, effects) = self.apply_chunk_inner(chunk)?;
        if let Some(profiles) = profiles {
            for effect in effects {
                profiles.common.counter_add(
                    RUNTIME_FILTER_INPUT_ROWS,
                    ProfileUnit::Unit,
                    i64::try_from(effect.input_rows()).unwrap_or(i64::MAX),
                );
                profiles.common.counter_add(
                    RUNTIME_FILTER_OUTPUT_ROWS,
                    ProfileUnit::Unit,
                    i64::try_from(effect.output_rows()).unwrap_or(i64::MAX),
                );
            }
        }
        Ok(output)
    }

    fn poll(&self) -> Result<(), String> {
        let pending = {
            let bindings = self
                .inner
                .bindings
                .lock()
                .expect("native ordered RF consumer lock");
            if bindings
                .iter()
                .any(|binding| matches!(binding.state, NativeOrderedLiveBindingState::Unbound))
            {
                return Err("native ordered runtime-filter consumers must bind before poll".into());
            }
            bindings
                .iter()
                .enumerate()
                .filter_map(|(index, binding)| match &binding.state {
                    NativeOrderedLiveBindingState::BoundExecutionLive {
                        subscription,
                        observed,
                        terminal: None,
                        ..
                    } => Some((index, binding.spec.clone(), subscription.clone(), *observed)),
                    _ => None,
                })
                .collect::<Vec<_>>()
        };
        for (index, spec, subscription, observed) in pending {
            let outcome = subscription.poll_after(observed);
            self.apply_execution_poll_outcome(index, &spec, outcome)?;
        }
        Ok(())
    }

    fn apply_execution_poll_outcome(
        &self,
        index: usize,
        spec: &RuntimeFilterConsumerBinding,
        outcome: execution::LivePollOutcome,
    ) -> Result<(), String> {
        let mut bindings = self
            .inner
            .bindings
            .lock()
            .expect("native ordered RF consumer lock");
        let Some(binding) = bindings.get_mut(index) else {
            return Err("native ordered runtime-filter binding index drifted".into());
        };
        let NativeOrderedLiveBindingState::BoundExecutionLive {
            observed,
            latest_snapshot,
            terminal,
            ..
        } = &mut binding.state
        else {
            return Ok(());
        };
        match outcome {
            execution::LivePollOutcome::Updated {
                snapshot,
                terminal: update_terminal,
            } => {
                if observed.is_none_or(|seen| snapshot.logical_version() > seen) {
                    *observed = Some(snapshot.logical_version());
                    *latest_snapshot = Some(snapshot);
                }
                if let Some(update_terminal) = update_terminal {
                    if latest_snapshot.is_none() {
                        binding.state = NativeOrderedLiveBindingState::PassThrough;
                    } else {
                        *terminal = Some(update_terminal);
                    }
                }
            }
            execution::LivePollOutcome::Idle {
                latest_version,
                terminal: idle_terminal,
            } => {
                if latest_version.is_some_and(|latest| observed.is_none_or(|seen| latest > seen)) {
                    return Err(format!(
                        "native ordered runtime-filter binding_id={} reported a newer live version without artifact",
                        spec.binding_id()
                    ));
                }
                if latest_version.is_some_and(|latest| observed.is_some_and(|seen| latest < seen)) {
                    return Err(format!(
                        "native ordered runtime-filter binding_id={} live cursor regressed",
                        spec.binding_id()
                    ));
                }
                if let Some(idle_terminal) = idle_terminal {
                    if latest_snapshot.is_none() {
                        binding.state = NativeOrderedLiveBindingState::PassThrough;
                    } else {
                        *terminal = Some(idle_terminal);
                    }
                }
            }
        }
        Ok(())
    }

    fn apply_chunk_inner(
        &self,
        chunk: Chunk,
    ) -> Result<(Option<Chunk>, Vec<execution::RuntimeFilterRowEffect>), String> {
        let active = {
            let bindings = self
                .inner
                .bindings
                .lock()
                .expect("native ordered RF consumer lock");
            bindings
                .iter()
                .filter_map(|binding| match &binding.state {
                    NativeOrderedLiveBindingState::BoundExecutionLive {
                        latest_snapshot: Some(snapshot),
                        ..
                    } => Some((
                        binding.spec.expr_id,
                        NativeOrderedPredicateForApply::Execution(Arc::clone(snapshot)),
                    )),
                    _ => None,
                })
                .collect::<Vec<_>>()
        };
        if active.is_empty() {
            return Ok((Some(chunk), Vec::new()));
        }
        let chunk = crate::exec::chunk::hydrate_dictionary_columns_except(&chunk, |_, _| false)?;
        let mut current = Some(chunk);
        let mut effects = Vec::new();
        for (expr_id, predicate) in active {
            let Some(input) = current else {
                return Ok((None, effects));
            };
            let array = self.inner.arena.eval(expr_id, &input)?;
            let mask = match predicate {
                NativeOrderedPredicateForApply::Execution(snapshot) => {
                    let outcome = execution::evaluator::evaluate_rows(
                        snapshot.binding_id(),
                        snapshot.logical_version(),
                        snapshot.artifact_query().as_ref(),
                        &array,
                    )
                    .map_err(|error| error.to_string())?;
                    match outcome.evaluation() {
                        execution::RuntimeFilterRowEvaluation::Evaluated { mask, .. } => {
                            effects.push(
                                outcome
                                    .effect()
                                    .expect("evaluated runtime-filter row outcome has an effect"),
                            );
                            mask.clone()
                        }
                        execution::RuntimeFilterRowEvaluation::NotEvaluated { .. } => {
                            current = Some(input);
                            continue;
                        }
                    }
                }
            };
            if mask.iter().all(|value| value == Some(true)) {
                current = Some(input);
            } else if mask.iter().all(|value| value != Some(true)) {
                current = None;
            } else {
                let filtered =
                    filter_record_batch(&input.batch, &mask).map_err(|error| error.to_string())?;
                current = Some(Chunk::try_new_like(filtered, &input)?);
            }
        }
        Ok((current, effects))
    }
}

#[derive(Clone)]
pub(crate) struct RuntimeFilterConsumerSet {
    inner: Arc<NativeConsumerInner>,
}

struct NativeConsumerInner {
    arena: Arc<ExprArena>,
    bindings: Mutex<Vec<NativeConsumerBinding>>,
    acquire_phase: Mutex<NativeConsumerAcquirePhase>,
    acquire_ready: Condvar,
    wait_timeout: Mutex<Duration>,
}

enum NativeConsumerAcquirePhase {
    Pending,
    Acquiring,
    Complete,
    Failed(String),
}

struct NativeConsumerBinding {
    spec: RuntimeFilterConsumerBinding,
    state: NativeConsumerBindingState,
}

enum NativeConsumerBindingState {
    Unbound,
    BoundBlocking(Arc<dyn execution::BlockingSnapshotSubscription>),
    BoundLive {
        subscription: Arc<dyn execution::NonBlockingLiveSubscription>,
        observed: Option<execution::LogicalVersion>,
    },
    Acquiring,
    Active(NativeConsumerPredicate),
    PassThrough,
}

enum NativeConsumerPredicate {
    Execution(Arc<execution::RuntimeFilterSnapshot>),
}

enum NativeConsumerPredicateForApply {
    Execution(Arc<execution::RuntimeFilterSnapshot>),
}

impl NativeConsumerPredicate {
    fn clone_for_apply(&self) -> NativeConsumerPredicateForApply {
        match self {
            Self::Execution(snapshot) => {
                NativeConsumerPredicateForApply::Execution(Arc::clone(snapshot))
            }
        }
    }
}

impl RuntimeFilterConsumerSet {
    pub(crate) fn scan_domain_snapshots(
        &self,
    ) -> Vec<(
        execution::scan_domain::RuntimeFilterScanDomainBinding,
        Option<Arc<execution::RuntimeFilterSnapshot>>,
    )> {
        let bindings = self.inner.bindings.lock().expect("native RF consumer lock");
        bindings
            .iter()
            .filter_map(|binding| {
                let target = binding.spec.scan_domain.clone()?;
                let snapshot = match &binding.state {
                    NativeConsumerBindingState::Active(NativeConsumerPredicate::Execution(
                        snapshot,
                    )) => Some(Arc::clone(snapshot)),
                    _ => None,
                };
                Some((target, snapshot))
            })
            .collect()
    }

    pub(crate) fn from_plan(
        specs: &[RuntimeFilterConsumerBinding],
        arena: Arc<ExprArena>,
    ) -> Result<Self, String> {
        validate_plan_specs(specs, &arena)?;
        Ok(Self {
            inner: Arc::new(NativeConsumerInner {
                arena,
                bindings: Mutex::new(
                    specs
                        .iter()
                        .cloned()
                        .map(|spec| NativeConsumerBinding {
                            spec,
                            state: NativeConsumerBindingState::Unbound,
                        })
                        .collect(),
                ),
                acquire_phase: Mutex::new(NativeConsumerAcquirePhase::Pending),
                acquire_ready: Condvar::new(),
                wait_timeout: Mutex::new(Duration::from_secs(1)),
            }),
        })
    }

    pub(crate) fn bind(&self, state: &RuntimeState) -> Result<(), String> {
        *self
            .inner
            .wait_timeout
            .lock()
            .expect("native RF timeout lock") = state
            .runtime_filter_wait_timeout()
            .unwrap_or(Duration::from_secs(1));
        let mut bindings = self.inner.bindings.lock().expect("native RF consumer lock");
        if bindings
            .iter()
            .all(|binding| !matches!(binding.state, NativeConsumerBindingState::Unbound))
        {
            return Ok(());
        }
        let Some(session) = state.runtime_filter_session() else {
            if bindings.is_empty() {
                return Ok(());
            }
            return Err(
                "native runtime-filter consumers require an installed execution context".into(),
            );
        };
        for binding in bindings.iter_mut() {
            if !matches!(binding.state, NativeConsumerBindingState::Unbound) {
                continue;
            }
            let request = execution::RuntimeFilterSubscriptionRequest::new(
                execution_membership_consumer_contract(&binding.spec)?,
            );
            match execution::RuntimeFilterSession::subscribe(session.as_ref(), request) {
                Ok(execution::RuntimeFilterBindOutcome::Bound(
                    execution::RuntimeFilterSubscriptionHandle::Blocking(subscription),
                )) if matches!(
                    binding.spec.activation(),
                    execution::ConsumerActivation::BlockingSnapshot
                ) =>
                {
                    binding.state = NativeConsumerBindingState::BoundBlocking(subscription);
                }
                Ok(execution::RuntimeFilterBindOutcome::Bound(
                    execution::RuntimeFilterSubscriptionHandle::Live(subscription),
                )) if matches!(
                    binding.spec.activation(),
                    execution::ConsumerActivation::NonBlockingLive {
                        late_apply: execution::RuntimeFilterLateApplyGranularity::Batch,
                    }
                ) =>
                {
                    binding.state = NativeConsumerBindingState::BoundLive {
                        subscription,
                        observed: None,
                    };
                }
                Ok(execution::RuntimeFilterBindOutcome::Unavailable(_)) => {
                    binding.state = NativeConsumerBindingState::PassThrough;
                }
                Ok(_) => {
                    return Err(format!(
                        "native Join runtime-filter binding_id={} session returned an activation-mismatched subscription",
                        binding.spec.binding_id()
                    ));
                }
                Err(error)
                    if error.kind()
                        == execution::RuntimeFilterContractViolationKind::SessionClosed =>
                {
                    binding.state = NativeConsumerBindingState::PassThrough;
                }
                Err(error) => return Err(error.to_string()),
            }
            match binding.spec.activation() {
                execution::ConsumerActivation::BlockingSnapshot
                | execution::ConsumerActivation::NonBlockingLive {
                    late_apply: execution::RuntimeFilterLateApplyGranularity::Batch,
                } => {}
                execution::ConsumerActivation::NonBlockingLive { .. } => {
                    return Err(format!(
                        "native Join runtime-filter binding_id={} has unsupported activation",
                        binding.spec.binding_id()
                    ));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn acquire_blocking(&self, timeout: Duration) -> Result<(), String> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .unwrap_or_else(Instant::now);
        let mut phase = self
            .inner
            .acquire_phase
            .lock()
            .expect("native RF acquire phase lock");
        loop {
            match &*phase {
                NativeConsumerAcquirePhase::Pending => {
                    *phase = NativeConsumerAcquirePhase::Acquiring;
                    break;
                }
                NativeConsumerAcquirePhase::Acquiring => {
                    phase = self
                        .inner
                        .acquire_ready
                        .wait(phase)
                        .expect("native RF acquire phase lock");
                }
                NativeConsumerAcquirePhase::Complete => return Ok(()),
                NativeConsumerAcquirePhase::Failed(error) => return Err(error.clone()),
            }
        }
        drop(phase);

        let result = self.acquire_once(deadline);
        let mut phase = self
            .inner
            .acquire_phase
            .lock()
            .expect("native RF acquire phase lock");
        *phase = match &result {
            Ok(()) => NativeConsumerAcquirePhase::Complete,
            Err(error) => NativeConsumerAcquirePhase::Failed(error.clone()),
        };
        self.inner.acquire_ready.notify_all();
        result
    }

    fn acquire_once(&self, deadline: Instant) -> Result<(), String> {
        let pending = {
            let mut bindings = self.inner.bindings.lock().expect("native RF consumer lock");
            bindings
                .iter_mut()
                .enumerate()
                .filter_map(|(index, binding)| {
                    let state = std::mem::replace(
                        &mut binding.state,
                        NativeConsumerBindingState::Acquiring,
                    );
                    match state {
                        NativeConsumerBindingState::BoundBlocking(subscription) => {
                            Some((index, subscription))
                        }
                        state => {
                            binding.state = state;
                            None
                        }
                    }
                })
                .collect::<Vec<_>>()
        };

        for (index, subscription) in pending {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let state = match subscription.acquire(remaining) {
                execution::SnapshotAcquireOutcome::Published(snapshot) => {
                    NativeConsumerBindingState::Active(NativeConsumerPredicate::Execution(snapshot))
                }
                execution::SnapshotAcquireOutcome::Unsupported(_)
                | execution::SnapshotAcquireOutcome::Unavailable(_)
                | execution::SnapshotAcquireOutcome::Cancelled
                | execution::SnapshotAcquireOutcome::TimedOut => {
                    NativeConsumerBindingState::PassThrough
                }
            };
            self.inner.bindings.lock().expect("native RF consumer lock")[index].state = state;
        }
        Ok(())
    }

    pub(crate) fn acquire_configured(&self) -> Result<(), String> {
        let timeout = *self
            .inner
            .wait_timeout
            .lock()
            .expect("native RF timeout lock");
        self.acquire_blocking(timeout)
    }

    pub(crate) fn set_wait_timeout(&self, timeout: Duration) {
        *self
            .inner
            .wait_timeout
            .lock()
            .expect("native RF timeout lock") = timeout;
    }

    pub(crate) fn apply_chunk(&self, chunk: Chunk) -> Result<Option<Chunk>, String> {
        self.apply_chunk_profiled(chunk, None)
    }

    pub(crate) fn apply_chunk_profiled(
        &self,
        chunk: Chunk,
        profiles: Option<&OperatorProfiles>,
    ) -> Result<Option<Chunk>, String> {
        let (output, effects) = self.apply_chunk_inner(chunk)?;
        if let Some(profiles) = profiles {
            for effect in effects {
                profiles.common.counter_add(
                    RUNTIME_FILTER_INPUT_ROWS,
                    ProfileUnit::Unit,
                    i64::try_from(effect.input_rows()).unwrap_or(i64::MAX),
                );
                profiles.common.counter_add(
                    RUNTIME_FILTER_OUTPUT_ROWS,
                    ProfileUnit::Unit,
                    i64::try_from(effect.output_rows()).unwrap_or(i64::MAX),
                );
            }
        }
        Ok(output)
    }

    fn apply_chunk_inner(
        &self,
        chunk: Chunk,
    ) -> Result<(Option<Chunk>, Vec<execution::RuntimeFilterRowEffect>), String> {
        self.poll_live_bindings()?;
        let active = {
            let bindings = self.inner.bindings.lock().expect("native RF consumer lock");
            if bindings.iter().any(|binding| {
                let unacquired = matches!(
                    binding.state,
                    NativeConsumerBindingState::Unbound
                        | NativeConsumerBindingState::BoundBlocking(_)
                        | NativeConsumerBindingState::Acquiring
                );
                unacquired
            }) {
                return Err("native runtime-filter consumers must acquire before apply".into());
            }
            bindings
                .iter()
                .enumerate()
                .filter_map(|(index, binding)| match &binding.state {
                    NativeConsumerBindingState::Active(predicate) => {
                        Some((index, binding.spec.expr_id, predicate.clone_for_apply()))
                    }
                    _ => None,
                })
                .collect::<Vec<_>>()
        };
        if active.is_empty() {
            return Ok((Some(chunk), Vec::new()));
        }
        let chunk = crate::exec::chunk::hydrate_dictionary_columns_except(&chunk, |_, _| false)?;
        let mut current = Some(chunk);
        let mut effects = Vec::new();
        for (index, expr_id, predicate) in active {
            let Some(input) = current else {
                return Ok((None, effects));
            };
            let array = self.inner.arena.eval(expr_id, &input)?;
            let mask = match predicate {
                NativeConsumerPredicateForApply::Execution(snapshot) => {
                    let outcome = execution::evaluator::evaluate_rows(
                        snapshot.binding_id(),
                        snapshot.logical_version(),
                        snapshot.artifact_query().as_ref(),
                        &array,
                    )
                    .map_err(|error| error.to_string())?;
                    match outcome.evaluation() {
                        execution::RuntimeFilterRowEvaluation::Evaluated { mask, .. } => {
                            effects.push(
                                outcome
                                    .effect()
                                    .expect("evaluated runtime-filter row outcome has an effect"),
                            );
                            mask.clone()
                        }
                        execution::RuntimeFilterRowEvaluation::NotEvaluated { .. } => {
                            self.inner.bindings.lock().expect("native RF consumer lock")[index]
                                .state = NativeConsumerBindingState::PassThrough;
                            current = Some(input);
                            continue;
                        }
                    }
                }
            };
            if mask.iter().all(|value| value == Some(true)) {
                current = Some(input);
            } else if mask.iter().all(|value| value != Some(true)) {
                current = None;
            } else {
                let filtered =
                    filter_record_batch(&input.batch, &mask).map_err(|e| e.to_string())?;
                current = Some(Chunk::try_new_like(filtered, &input)?);
            }
        }
        Ok((current, effects))
    }

    fn poll_live_bindings(&self) -> Result<(), String> {
        let pending = {
            let bindings = self.inner.bindings.lock().expect("native RF consumer lock");
            bindings
                .iter()
                .enumerate()
                .filter_map(|(index, binding)| match &binding.state {
                    NativeConsumerBindingState::BoundLive {
                        subscription,
                        observed,
                    } => Some((
                        index,
                        binding.spec.clone(),
                        Arc::clone(subscription),
                        *observed,
                    )),
                    _ => None,
                })
                .collect::<Vec<_>>()
        };
        for (index, spec, subscription, observed) in pending {
            let outcome = subscription.poll_after(observed);
            self.apply_execution_live_poll_outcome(index, &spec, outcome)?;
        }
        Ok(())
    }

    fn apply_execution_live_poll_outcome(
        &self,
        index: usize,
        spec: &RuntimeFilterConsumerBinding,
        outcome: execution::LivePollOutcome,
    ) -> Result<(), String> {
        let mut bindings = self.inner.bindings.lock().expect("native RF consumer lock");
        let Some(binding) = bindings.get_mut(index) else {
            return Err("native Join runtime-filter binding index drifted".into());
        };
        let NativeConsumerBindingState::BoundLive { observed, .. } = &mut binding.state else {
            return Ok(());
        };
        let observed_version = *observed;
        if let Some(version) = observed_version
            && version != execution::LogicalVersion::FIRST
        {
            return Err(format!(
                "native Join CompleteOnce runtime-filter binding_id={} private cursor must use LogicalVersion::FIRST, got {version:?}",
                spec.binding_id()
            ));
        }
        match outcome {
            execution::LivePollOutcome::Updated { snapshot, terminal } => {
                if snapshot.logical_version() != execution::LogicalVersion::FIRST {
                    return Err(format!(
                        "native Join CompleteOnce runtime-filter binding_id={} Updated artifact must use LogicalVersion::FIRST",
                        spec.binding_id()
                    ));
                }
                if terminal != Some(execution::LiveTerminal::Completed) {
                    return Err(format!(
                        "native Join CompleteOnce runtime-filter binding_id={} Updated artifact requires terminal Completed, got {terminal:?}",
                        spec.binding_id()
                    ));
                }
                binding.state = NativeConsumerBindingState::Active(
                    NativeConsumerPredicate::Execution(snapshot),
                );
            }
            execution::LivePollOutcome::Idle {
                latest_version,
                terminal,
            } => {
                if let Some(version) = latest_version
                    && version != execution::LogicalVersion::FIRST
                {
                    return Err(format!(
                        "native Join CompleteOnce runtime-filter binding_id={} Idle latest version must use LogicalVersion::FIRST, got {version:?}",
                        spec.binding_id()
                    ));
                }
                if terminal == Some(execution::LiveTerminal::Completed) {
                    return Err(format!(
                        "native Join CompleteOnce runtime-filter binding_id={} reported Completed without the final artifact",
                        spec.binding_id()
                    ));
                }
                match (observed_version, latest_version) {
                    (None, Some(_)) => {
                        return Err(format!(
                            "native Join CompleteOnce runtime-filter binding_id={} Idle cursor advanced without returning an artifact",
                            spec.binding_id()
                        ));
                    }
                    (Some(_), None) => {
                        return Err(format!(
                            "native Join CompleteOnce runtime-filter binding_id={} Idle cursor regressed from LogicalVersion::FIRST",
                            spec.binding_id()
                        ));
                    }
                    _ => {}
                }
                match terminal {
                    None => {}
                    Some(
                        execution::LiveTerminal::CompletedWithoutArtifact
                        | execution::LiveTerminal::Unavailable(_)
                        | execution::LiveTerminal::Cancelled,
                    ) => {
                        binding.state = NativeConsumerBindingState::PassThrough;
                    }
                    Some(execution::LiveTerminal::Completed) => unreachable!("handled above"),
                }
            }
        }
        Ok(())
    }
}

fn execution_membership_consumer_contract(
    spec: &RuntimeFilterConsumerBinding,
) -> Result<execution::RuntimeFilterConsumerContract, String> {
    if !matches!(
        spec.execution_contract(),
        RuntimeFilterExecutionContract::Membership(_)
    ) {
        return Err(format!(
            "native Join runtime-filter binding_id={} requires a Membership contract",
            spec.binding_id()
        ));
    }
    Ok(spec.contract().clone())
}

fn execution_ordered_live_consumer_contract(
    spec: &RuntimeFilterConsumerBinding,
) -> Result<execution::RuntimeFilterConsumerContract, String> {
    if !matches!(
        spec.execution_contract(),
        RuntimeFilterExecutionContract::Ordered(_)
    ) {
        return Err(format!(
            "native ordered runtime-filter binding_id={} requires an Ordered contract",
            spec.binding_id()
        ));
    }
    if !matches!(
        spec.activation(),
        execution::ConsumerActivation::NonBlockingLive {
            late_apply: execution::RuntimeFilterLateApplyGranularity::Batch
                | execution::RuntimeFilterLateApplyGranularity::Split,
        }
    ) {
        return Err(format!(
            "native ordered runtime-filter binding_id={} requires a non-blocking live activation",
            spec.binding_id()
        ));
    }
    Ok(spec.contract().clone())
}

fn validate_unique_consumer_bindings(specs: &[RuntimeFilterConsumerBinding]) -> Result<(), String> {
    let mut bindings = BTreeSet::new();
    for spec in specs {
        if !bindings.insert(spec.binding_id()) {
            return Err(format!(
                "duplicate native runtime-filter consumer binding_id={}",
                spec.binding_id()
            ));
        }
    }
    Ok(())
}

fn validate_plan_specs(
    specs: &[RuntimeFilterConsumerBinding],
    arena: &ExprArena,
) -> Result<(), String> {
    validate_unique_consumer_bindings(specs)?;
    for spec in specs {
        if !matches!(
            spec.activation(),
            execution::ConsumerActivation::BlockingSnapshot
                | execution::ConsumerActivation::NonBlockingLive {
                    late_apply: execution::RuntimeFilterLateApplyGranularity::Batch,
                }
        ) {
            return Err(format!(
                "native Join runtime-filter binding_id={} requires BlockingSnapshot or Batch NonBlockingLive",
                spec.binding_id()
            ));
        }
        if !matches!(
            spec.execution_contract(),
            RuntimeFilterExecutionContract::Membership(_)
        ) || spec.contract().reduction() != execution::RuntimeFilterReduction::SetUnion
        {
            return Err(format!(
                "native Join runtime-filter binding_id={} requires a membership SetUnion contract",
                spec.binding_id()
            ));
        }
        if arena.data_type(spec.expr_id).is_none() {
            return Err(format!(
                "native Join runtime-filter binding_id={} expression is missing",
                spec.binding_id()
            ));
        }
    }
    Ok(())
}

fn validate_ordered_live_plan_specs(
    specs: &[RuntimeFilterConsumerBinding],
    arena: &ExprArena,
) -> Result<(), String> {
    validate_unique_consumer_bindings(specs)?;
    for spec in specs {
        match spec.activation() {
            execution::ConsumerActivation::NonBlockingLive {
                late_apply:
                    execution::RuntimeFilterLateApplyGranularity::Batch
                    | execution::RuntimeFilterLateApplyGranularity::Split,
            } => {}
            execution::ConsumerActivation::NonBlockingLive { .. } => {
                return Err(format!(
                    "native ordered runtime-filter binding_id={} has unsupported late-apply granularity",
                    spec.binding_id()
                ));
            }
            execution::ConsumerActivation::BlockingSnapshot => {
                return Err(format!(
                    "native ordered runtime-filter binding_id={} requires NonBlockingLive",
                    spec.binding_id()
                ));
            }
        }
        if !matches!(
            spec.execution_contract(),
            RuntimeFilterExecutionContract::Ordered(_)
        ) || spec.contract().reduction()
            != execution::RuntimeFilterReduction::TightenOrderedBound
        {
            return Err(format!(
                "native ordered runtime-filter binding_id={} requires an ordered TightenOrderedBound contract",
                spec.binding_id()
            ));
        }
        let RuntimeFilterExecutionContract::Ordered(order_contract) = spec.execution_contract()
        else {
            unreachable!("ordered contract was checked above");
        };
        if order_contract.keys().len() != 1
            || arena.data_type(spec.expr_id)
                != order_contract.keys().first().map(|key| key.data_type())
        {
            return Err(format!(
                "native ordered runtime-filter binding_id={} expression does not match its frozen single-key contract",
                spec.binding_id()
            ));
        }
    }
    Ok(())
}

pub(crate) struct NativeRuntimeFilterProcessorFactory {
    name: String,
    consumers: RuntimeFilterConsumerSet,
}

impl NativeRuntimeFilterProcessorFactory {
    pub(crate) fn new(
        owner_node_id: i32,
        specs: &[RuntimeFilterConsumerBinding],
        arena: Arc<ExprArena>,
    ) -> Result<Self, String> {
        Ok(Self {
            name: format!("NativeRuntimeFilter (id={owner_node_id})"),
            consumers: RuntimeFilterConsumerSet::from_plan(specs, arena)?,
        })
    }
}

impl OperatorFactory for NativeRuntimeFilterProcessorFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(NativeRuntimeFilterProcessor {
            name: self.name.clone(),
            consumers: self.consumers.clone(),
            output: None,
            finishing: false,
            profiles: None,
        })
    }
}

struct NativeRuntimeFilterProcessor {
    name: String,
    consumers: RuntimeFilterConsumerSet,
    output: Option<Chunk>,
    finishing: bool,
    profiles: Option<OperatorProfiles>,
}

impl Operator for NativeRuntimeFilterProcessor {
    fn name(&self) -> &str {
        &self.name
    }

    fn set_profiles(&mut self, profiles: OperatorProfiles) {
        self.profiles = Some(profiles);
    }

    fn bind_runtime_state(&mut self, state: &RuntimeState) -> Result<(), String> {
        self.consumers.bind(state)
    }

    fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
        Some(self)
    }

    fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
        Some(self)
    }

    fn is_finished(&self) -> bool {
        self.finishing && self.output.is_none()
    }
}

impl ProcessorOperator for NativeRuntimeFilterProcessor {
    fn need_input(&self) -> bool {
        !self.finishing && self.output.is_none()
    }

    fn has_output(&self) -> bool {
        self.output.is_some()
    }

    fn push_chunk(&mut self, state: &RuntimeState, chunk: Chunk) -> Result<(), String> {
        if !self.need_input() {
            return Err("native runtime-filter processor cannot accept input".into());
        }
        self.consumers.acquire_blocking(
            state
                .runtime_filter_wait_timeout()
                .unwrap_or(Duration::from_secs(1)),
        )?;
        self.output = self
            .consumers
            .apply_chunk_profiled(chunk, self.profiles.as_ref())?;
        Ok(())
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        Ok(self.output.take())
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
        self.finishing = true;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use arrow::array::{Array, Int32Array};
    use arrow::datatypes::DataType;
    use novarocks_execution::runtime_filter::{
        self as execution, ConsumerActivation, RuntimeFilterArtifactQueryError,
        RuntimeFilterBindingId, RuntimeFilterChannelId, RuntimeFilterConsumerContract,
        RuntimeFilterExecutionContract, RuntimeFilterMembershipSchema, RuntimeFilterNullSemantics,
        RuntimeFilterScalarRef,
    };
    use novarocks_spi::connector::ConnectorScalarValue;

    use super::*;
    use crate::exec::chunk::{Chunk, ChunkSchema};
    use crate::exec::expr::ExprNode;
    use crate::runtime::runtime_state::RuntimeState;
    use novarocks_types::SlotId;

    struct Int32MembershipQuery {
        accepted: i32,
    }

    impl execution::evaluator::RuntimeFilterArtifactQuery for Int32MembershipQuery {
        fn data_type(&self) -> &DataType {
            &DataType::Int32
        }

        fn matches_null(&self) -> Result<bool, RuntimeFilterArtifactQueryError> {
            Ok(false)
        }

        fn has_non_null_matches(&self) -> Result<bool, RuntimeFilterArtifactQueryError> {
            Ok(true)
        }

        fn non_null_value_may_match(
            &self,
            value: RuntimeFilterScalarRef<'_>,
        ) -> Result<bool, RuntimeFilterArtifactQueryError> {
            match value {
                RuntimeFilterScalarRef::Int32(value) => Ok(value == self.accepted),
                _ => Err(RuntimeFilterArtifactQueryError::ContractViolation),
            }
        }

        fn non_null_range_may_match(
            &self,
            _: &ConnectorScalarValue,
            _: &ConnectorScalarValue,
        ) -> Result<bool, RuntimeFilterArtifactQueryError> {
            Ok(true)
        }
    }

    struct PublishedSubscription(Arc<execution::RuntimeFilterSnapshot>);

    impl execution::BlockingSnapshotSubscription for PublishedSubscription {
        fn acquire(&self, _: Duration) -> execution::SnapshotAcquireOutcome {
            execution::SnapshotAcquireOutcome::Published(Arc::clone(&self.0))
        }

        fn snapshot(&self) -> Option<Arc<execution::RuntimeFilterSnapshot>> {
            Some(Arc::clone(&self.0))
        }
    }

    struct SubscriptionSession {
        outcome: execution::RuntimeFilterBindOutcome<execution::RuntimeFilterSubscriptionHandle>,
    }

    impl execution::RuntimeFilterSession for SubscriptionSession {
        fn open_producer(
            &self,
            _: execution::RuntimeFilterProducerOpenRequest,
        ) -> Result<
            execution::RuntimeFilterBindOutcome<execution::RuntimeFilterProducerHandle>,
            execution::RuntimeFilterContractViolation,
        > {
            Err(execution::RuntimeFilterContractViolation::new(
                execution::RuntimeFilterContractViolationKind::UnauthorizedBinding,
                "consumer-only test session",
            ))
        }

        fn subscribe(
            &self,
            _: execution::RuntimeFilterSubscriptionRequest,
        ) -> Result<
            execution::RuntimeFilterBindOutcome<execution::RuntimeFilterSubscriptionHandle>,
            execution::RuntimeFilterContractViolation,
        > {
            match &self.outcome {
                execution::RuntimeFilterBindOutcome::Bound(handle) => match handle {
                    execution::RuntimeFilterSubscriptionHandle::Blocking(subscription) => {
                        Ok(execution::RuntimeFilterBindOutcome::Bound(
                            execution::RuntimeFilterSubscriptionHandle::Blocking(Arc::clone(
                                subscription,
                            )),
                        ))
                    }
                    execution::RuntimeFilterSubscriptionHandle::Live(subscription) => {
                        Ok(execution::RuntimeFilterBindOutcome::Bound(
                            execution::RuntimeFilterSubscriptionHandle::Live(Arc::clone(
                                subscription,
                            )),
                        ))
                    }
                },
                execution::RuntimeFilterBindOutcome::Unavailable(reason) => {
                    Ok(execution::RuntimeFilterBindOutcome::Unavailable(*reason))
                }
            }
        }

        fn open_final_domain_completion(
            &self,
            _: execution::RuntimeFilterFinalDomainOpenRequest,
        ) -> Result<
            execution::RuntimeFilterBindOutcome<
                execution::RuntimeFilterFinalDomainCompletionHandle,
            >,
            execution::RuntimeFilterContractViolation,
        > {
            Err(execution::RuntimeFilterContractViolation::new(
                execution::RuntimeFilterContractViolationKind::UnauthorizedBinding,
                "consumer-only test session",
            ))
        }
    }

    fn membership_spec(expr_id: crate::exec::expr::ExprId) -> RuntimeFilterConsumerBinding {
        let schema = RuntimeFilterMembershipSchema::new(
            &DataType::Int32,
            RuntimeFilterNullSemantics::NeverMatches,
        )
        .expect("membership schema");
        RuntimeFilterConsumerBinding::new(
            expr_id,
            RuntimeFilterConsumerContract::membership_blocking(
                RuntimeFilterBindingId::new(1),
                RuntimeFilterChannelId::new(2),
                RuntimeFilterExecutionContract::Membership(schema),
            )
            .expect("consumer contract"),
            None,
        )
    }

    #[test]
    fn consumer_plan_requires_the_execution_membership_contract() {
        let mut arena = ExprArena::default();
        let expr_id = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let consumers =
            RuntimeFilterConsumerSet::from_plan(&[membership_spec(expr_id)], Arc::new(arena));
        assert!(consumers.is_ok());
    }

    #[test]
    fn consumer_plan_rejects_missing_expression_coordinate() {
        let arena = Arc::new(ExprArena::default());
        let error = match RuntimeFilterConsumerSet::from_plan(
            &[membership_spec(crate::exec::expr::ExprId(99))],
            arena,
        ) {
            Ok(_) => panic!("missing expression must fail before subscription"),
            Err(error) => error,
        };
        assert!(error.contains("expression is missing"));
    }

    #[test]
    fn ordered_consumer_rejects_blocking_activation() {
        let mut arena = ExprArena::default();
        let expr_id = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int64);
        let key = execution::contribution::RuntimeOrderKey::with_order(
            DataType::Int64,
            execution::contribution::RuntimeOrderSortDirection::Ascending,
            execution::contribution::RuntimeOrderNullOrder::First,
        );
        let order = Arc::new(execution::contribution::RuntimeOrderContract::from_frozen(
            vec![key],
            [1; 32],
            [2; 32],
        ));
        let contract = RuntimeFilterConsumerContract::new(
            RuntimeFilterBindingId::new(1),
            RuntimeFilterChannelId::new(2),
            ConsumerActivation::BlockingSnapshot,
            RuntimeFilterExecutionContract::Ordered(order),
        );
        let error = match NativeOrderedLiveConsumerSet::from_plan(
            &[RuntimeFilterConsumerBinding::new(expr_id, contract, None)],
            Arc::new(arena),
        ) {
            Ok(_) => panic!("ordered consumers are live only"),
            Err(error) => error,
        };
        assert!(error.contains("requires NonBlockingLive"));
    }

    #[test]
    fn published_execution_snapshot_applies_the_membership_mask() {
        let mut arena = ExprArena::default();
        let expr_id = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
            &arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new(
                "v",
                DataType::Int32,
                false,
            )]),
            &[SlotId::new(1)],
        )
        .expect("chunk schema");
        let snapshot = Arc::new(execution::RuntimeFilterSnapshot::new(
            RuntimeFilterBindingId::new(1),
            execution::LogicalVersion::FIRST,
            [0; 32],
            Arc::new(Int32MembershipQuery { accepted: 2 }),
        ));
        let session: execution::RuntimeFilterSessionRef = Arc::new(SubscriptionSession {
            outcome: execution::RuntimeFilterBindOutcome::Bound(
                execution::RuntimeFilterSubscriptionHandle::Blocking(Arc::new(
                    PublishedSubscription(snapshot),
                )),
            ),
        });
        let consumers =
            RuntimeFilterConsumerSet::from_plan(&[membership_spec(expr_id)], Arc::new(arena))
                .expect("consumer set");
        let state = RuntimeState::default().with_runtime_filter_session(Some(session));
        consumers.bind(&state).expect("bind");
        consumers.acquire_configured().expect("acquire");
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema.arrow_schema_ref(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .expect("batch");
        let output = consumers
            .apply_chunk(Chunk::new_with_chunk_schema(batch, schema))
            .expect("apply")
            .expect("one matching row");
        let values = output.columns()[0]
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 output");
        assert_eq!(values.values(), &[2]);
    }

    #[test]
    fn unavailable_execution_subscription_is_chunk_exact_passthrough() {
        let mut arena = ExprArena::default();
        let expr_id = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let consumers =
            RuntimeFilterConsumerSet::from_plan(&[membership_spec(expr_id)], Arc::new(arena))
                .expect("consumer set");
        let session: execution::RuntimeFilterSessionRef = Arc::new(SubscriptionSession {
            outcome: execution::RuntimeFilterBindOutcome::Unavailable(
                execution::UnavailableReason::ResourceLimit,
            ),
        });
        let state = RuntimeState::default().with_runtime_filter_session(Some(session));
        consumers.bind(&state).expect("bind");
        consumers.acquire_configured().expect("acquire");
        let schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
            &arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new(
                "v",
                DataType::Int32,
                false,
            )]),
            &[SlotId::new(1)],
        )
        .expect("chunk schema");
        let input = Chunk::new_with_chunk_schema(
            arrow::record_batch::RecordBatch::try_new(
                schema.arrow_schema_ref(),
                vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
            )
            .expect("batch"),
            schema,
        );
        let output = consumers
            .apply_chunk(input.clone())
            .expect("apply")
            .expect("pass-through output");
        assert!(Arc::ptr_eq(output.batch.column(0), input.batch.column(0)));
    }
}
