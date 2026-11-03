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

use crate::exec::pipeline::operator::OperatorLifecycle;
use crate::runtime::task::{ExecutionFailure, ExecutionResult};

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::runtime_filter as execution;

use crate::exec::chunk::Chunk;
use crate::exec::pipeline::operator::{DriverBlockDeadline, Operator, forward_observable};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::schedule::observer::Observable;
use crate::runtime::runtime_state::RuntimeState;
use crate::runtime::task::io::{NoopTaskEventSink, TaskEvent, TaskEventSink};
use crate::runtime_filter::RuntimeFilterExecutionContract;
use arrow::array::ArrayRef;
use arrow::compute::filter_record_batch;

mod compiled;
mod compiled_ordered;
pub(crate) use compiled_ordered::{CompiledOrderedScanEvaluator, CompiledOrderedScanFactory};
mod live_cursor;
#[cfg(test)]
mod live_cursor_tests;
pub(crate) use compiled::{CompiledRuntimeFilterConsumers, CompiledRuntimeFilterKeys};

fn contract_failure(detail: impl Into<Arc<str>>) -> ExecutionFailure {
    execution::RuntimeFilterContractViolation::new(
        execution::RuntimeFilterContractViolationKind::ContractMismatch,
        detail,
    )
    .into()
}
#[derive(Debug)]
struct FilterArrowFailure(arrow::error::ArrowError);
impl std::fmt::Display for FilterArrowFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for FilterArrowFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}
impl novarocks_failure_contract::FailureProjection for FilterArrowFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureFact::new(
            novarocks_failure_contract::FailureClass::Invariant,
            crate::runtime::task::ExecutionReason::FilterArrowApplication.reason(),
        )
    }
}
fn arrow_failure(source: arrow::error::ArrowError) -> ExecutionFailure {
    ExecutionFailure::from_domain(FilterArrowFailure(source))
}

/// Deadline token of a consumer set's gate wait. The wait is frozen once per
/// set, so one token distinguishes it from a consumer's own block deadlines.
const RUNTIME_FILTER_GATE_DEADLINE_TOKEN: u64 = u64::MAX - 1;

/// What a consumer's input must wait for before it may pass the runtime
/// filters of its consumer set.
pub(crate) enum RuntimeFilterGate {
    /// Every blocking binding reached its outcome; input may pass.
    Open,
    /// A blocking binding is still pending, at most until
    /// [`RuntimeFilterConsumerState::gate_deadline`]. A publication notifies
    /// [`RuntimeFilterConsumerState::gate_observable`]; a consumer forwards
    /// that into the observable its driver parks on, whose generation the
    /// driver samples before it asks.
    Pending,
}

/// Evaluates the key of each binding of one consumer set over the chunk the
/// set filters. The consumer state decides which bindings apply, in which
/// order and over which rows; a provider only supplies what a binding's key
/// is, independently of compiled key evaluation.
pub(crate) trait RuntimeFilterKeyProvider {
    /// The chunk every key is evaluated over and every mask selects from,
    /// prepared once for each chunk at least one binding applies to.
    fn prepare_input(&mut self, chunk: Chunk) -> ExecutionResult<Chunk>;

    /// The key of the binding at position `binding` of the set over `input`.
    fn evaluate_key(&mut self, binding: usize, input: &Chunk) -> ExecutionResult<ArrayRef>;

    /// The original decoder and mask application policy. A direct ordered
    /// Scan overrides only this policy to retain its controlled output stock.
    fn apply_snapshot(
        &mut self,
        input: Chunk,
        array: &ArrayRef,
        snapshot: &execution::RuntimeFilterSnapshot,
    ) -> ExecutionResult<RuntimeFilterApplication> {
        apply_legacy_runtime_filter_snapshot(input, array, snapshot)
    }
}

fn apply_legacy_runtime_filter_snapshot(
    input: Chunk,
    array: &ArrayRef,
    snapshot: &execution::RuntimeFilterSnapshot,
) -> ExecutionResult<RuntimeFilterApplication> {
    apply_runtime_filter_snapshot_with(input, array, snapshot, |input, mask| {
        let batch = filter_record_batch(input.record_batch(), mask).map_err(arrow_failure)?;
        Chunk::try_new_like(batch, input).map_err(ExecutionFailure::from)
    })
}

fn apply_runtime_filter_snapshot_with<F>(
    input: Chunk,
    array: &ArrayRef,
    snapshot: &execution::RuntimeFilterSnapshot,
    mut filter: F,
) -> ExecutionResult<RuntimeFilterApplication>
where
    F: FnMut(&Chunk, &arrow::array::BooleanArray) -> ExecutionResult<Chunk>,
{
    let outcome = execution::evaluator::evaluate_rows(
        snapshot.binding_id(),
        snapshot.logical_version(),
        snapshot.artifact_query().as_ref(),
        array,
    )
    .map_err(ExecutionFailure::from)?;
    let execution::RuntimeFilterRowEvaluation::Evaluated { mask, .. } = outcome.evaluation() else {
        return Ok(RuntimeFilterApplication::NotEvaluated(input));
    };
    let effect = outcome
        .effect()
        .expect("evaluated runtime-filter row outcome has an effect");
    let output = if mask.iter().all(|value| value == Some(true)) {
        Some(input)
    } else if mask.iter().all(|value| value != Some(true)) {
        None
    } else {
        Some(filter(&input, mask)?)
    };
    Ok(RuntimeFilterApplication::Evaluated { output, effect })
}

pub(crate) enum RuntimeFilterApplication {
    NotEvaluated(Chunk),
    Evaluated {
        output: Option<Chunk>,
        effect: execution::RuntimeFilterRowEffect,
    },
}

/// Runtime filters one consumer applies to its input, shared by every driver
/// of that consumer: the bindings, their subscriptions and snapshots, and the
/// gate. It owns no expression; the keys a chunk is filtered by come from a
/// [`RuntimeFilterKeyProvider`].
///
/// Blocking-snapshot bindings hold input back behind one gate. The gate's
/// wait starts when a driver first has real input at hand and lasts at most
/// the configured timeout for the whole set: later touches, by any driver,
/// share that deadline. Nothing blocks a thread; a pending gate is waited on
/// through its observable and deadline.
#[derive(Clone)]
pub(crate) struct RuntimeFilterConsumerState {
    inner: Arc<NativeConsumerInner>,
}

struct NativeConsumerInner {
    bindings: Mutex<Vec<NativeConsumerBinding>>,
    gate: Mutex<NativeConsumerGate>,
    /// Notified when a blocking binding's outcome is published.
    gate_observable: Arc<Observable>,
    wait_timeout: Mutex<Duration>,
}

enum NativeConsumerGate {
    /// No input reached the gate yet; its wait has not started.
    Idle,
    /// Input is held back until every blocking binding settles, at most
    /// until `deadline`.
    Waiting { deadline: Instant },
    /// Every blocking binding settled.
    Open,
}

struct NativeConsumerBinding {
    contract: execution::RuntimeFilterConsumerContract,
    state: NativeConsumerBindingState,
}

enum NativeConsumerBindingState {
    Unbound,
    BoundBlocking(Arc<dyn execution::BlockingSnapshotSubscription>),
    BoundLive {
        subscription: Arc<dyn execution::NonBlockingLiveSubscription>,
        observed: Option<execution::LogicalVersion>,
    },
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

impl RuntimeFilterConsumerState {
    /// Validates every binding in order: its activation and membership
    /// contract, then `validate_key`, which checks the key coordinate the
    /// caller's provider evaluates for the binding at that position.
    pub(crate) fn from_contracts(
        owner: &'static str,
        contracts: Vec<execution::RuntimeFilterConsumerContract>,
        validate_key: impl FnMut(
            usize,
            &execution::RuntimeFilterConsumerContract,
        ) -> ExecutionResult<()>,
    ) -> ExecutionResult<Self> {
        validate_membership_consumer_contracts(owner, &contracts, validate_key)?;
        Ok(Self {
            inner: Arc::new(NativeConsumerInner {
                bindings: Mutex::new(
                    contracts
                        .into_iter()
                        .map(|contract| NativeConsumerBinding {
                            contract,
                            state: NativeConsumerBindingState::Unbound,
                        })
                        .collect(),
                ),
                gate: Mutex::new(NativeConsumerGate::Idle),
                gate_observable: Arc::new(Observable::new()),
                wait_timeout: Mutex::new(Duration::from_secs(1)),
            }),
        })
    }

    pub(crate) fn bind(&self, state: &RuntimeState) -> ExecutionResult<()> {
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
            return Err(execution::RuntimeFilterContractViolation::new(
                execution::RuntimeFilterContractViolationKind::MissingSession,
                "native runtime-filter consumers require an installed execution context",
            )
            .into());
        };
        for binding in bindings.iter_mut() {
            if !matches!(binding.state, NativeConsumerBindingState::Unbound) {
                continue;
            }
            let request = execution::RuntimeFilterSubscriptionRequest::new(
                execution_membership_consumer_contract(&binding.contract)?,
            );
            match execution::RuntimeFilterSession::subscribe(session.as_ref(), request) {
                Ok(execution::RuntimeFilterBindOutcome::Bound(
                    execution::RuntimeFilterSubscriptionPin::Blocking(subscription),
                )) if matches!(
                    binding.contract.activation(),
                    execution::ConsumerActivation::BlockingSnapshot
                ) =>
                {
                    forward_observable(
                        &subscription.outcome_observable(),
                        &self.inner.gate_observable,
                    );
                    binding.state = NativeConsumerBindingState::BoundBlocking(subscription);
                }
                Ok(execution::RuntimeFilterBindOutcome::Bound(
                    execution::RuntimeFilterSubscriptionPin::Live(subscription),
                )) if matches!(
                    binding.contract.activation(),
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
                    return Err(contract_failure(format!(
                        "native Join runtime-filter binding_id={} session returned an activation-mismatched subscription",
                        binding.contract.binding_id().get()
                    )));
                }
                Err(error)
                    if error.kind()
                        == execution::RuntimeFilterContractViolationKind::SessionClosed =>
                {
                    binding.state = NativeConsumerBindingState::PassThrough;
                }
                Err(error) => return Err(error.into()),
            }
            match binding.contract.activation() {
                execution::ConsumerActivation::BlockingSnapshot
                | execution::ConsumerActivation::NonBlockingLive {
                    late_apply: execution::RuntimeFilterLateApplyGranularity::Batch,
                } => {}
                execution::ConsumerActivation::NonBlockingLive { .. } => {
                    return Err(contract_failure(format!(
                        "native Join runtime-filter binding_id={} has unsupported activation",
                        binding.contract.binding_id().get()
                    )));
                }
            }
        }
        Ok(())
    }

    /// Polls the gate for input that is at hand. The first touch of the set
    /// starts its one total wait. Never blocks.
    pub(crate) fn poll_gate(&self) -> RuntimeFilterGate {
        let (gate, settled) = {
            let mut gate = self.inner.gate.lock().expect("native RF gate lock");
            let deadline = match *gate {
                NativeConsumerGate::Open => return RuntimeFilterGate::Open,
                NativeConsumerGate::Idle => {
                    let timeout = *self
                        .inner
                        .wait_timeout
                        .lock()
                        .expect("native RF timeout lock");
                    let deadline = Instant::now()
                        .checked_add(timeout)
                        .unwrap_or_else(Instant::now);
                    *gate = NativeConsumerGate::Waiting { deadline };
                    deadline
                }
                NativeConsumerGate::Waiting { deadline } => deadline,
            };
            let (open, settled) = self.settle_blocking_bindings(Instant::now() >= deadline);
            if open {
                *gate = NativeConsumerGate::Open;
                (RuntimeFilterGate::Open, settled)
            } else {
                (RuntimeFilterGate::Pending, settled)
            }
        };
        for (subscription, outcome) in settled {
            subscription.record_consumer_outcome(&outcome);
        }
        gate
    }

    /// Settles blocking bindings in binding order: a binding waits until every
    /// earlier one settled, so outcomes are recorded in that order. When the
    /// wait `expired`, every unsettled binding passes through as timed out.
    /// Returns whether every blocking binding settled, and what to record.
    #[expect(
        clippy::type_complexity,
        reason = "Each settled subscription is recorded with the outcome that settled it."
    )]
    fn settle_blocking_bindings(
        &self,
        expired: bool,
    ) -> (
        bool,
        Vec<(
            Arc<dyn execution::BlockingSnapshotSubscription>,
            execution::SnapshotAcquireOutcome,
        )>,
    ) {
        let mut settled = Vec::new();
        let mut bindings = self.inner.bindings.lock().expect("native RF consumer lock");
        for binding in bindings.iter_mut() {
            let NativeConsumerBindingState::BoundBlocking(subscription) = &binding.state else {
                continue;
            };
            let outcome = match subscription.try_outcome() {
                Some(outcome) => outcome,
                None if expired => execution::SnapshotAcquireOutcome::TimedOut,
                None => return (false, settled),
            };
            let subscription = Arc::clone(subscription);
            binding.state = match &outcome {
                execution::SnapshotAcquireOutcome::Published(snapshot) => {
                    NativeConsumerBindingState::Active(NativeConsumerPredicate::Execution(
                        Arc::clone(snapshot),
                    ))
                }
                execution::SnapshotAcquireOutcome::Unsupported(_)
                | execution::SnapshotAcquireOutcome::Unavailable(_)
                | execution::SnapshotAcquireOutcome::Cancelled
                | execution::SnapshotAcquireOutcome::TimedOut => {
                    NativeConsumerBindingState::PassThrough
                }
            };
            settled.push((subscription, outcome));
        }
        (true, settled)
    }

    /// Whether input already reached the gate and is still held back. Asking
    /// settles what it can, so a woken consumer observes a publication or an
    /// expired wait. An untouched gate holds nothing back.
    #[cfg(test)]
    pub(crate) fn gate_holds_input(&self) -> bool {
        let waiting = matches!(
            *self.inner.gate.lock().expect("native RF gate lock"),
            NativeConsumerGate::Waiting { .. }
        );
        waiting && matches!(self.poll_gate(), RuntimeFilterGate::Pending)
    }

    /// The deadline of a gate that holds input back.
    pub(crate) fn gate_deadline(&self) -> Option<DriverBlockDeadline> {
        match *self.inner.gate.lock().expect("native RF gate lock") {
            NativeConsumerGate::Waiting { deadline } => Some(DriverBlockDeadline::new(
                deadline,
                RUNTIME_FILTER_GATE_DEADLINE_TOKEN,
            )),
            NativeConsumerGate::Idle | NativeConsumerGate::Open => None,
        }
    }

    /// Notified when a blocking binding's outcome is published. Its identity
    /// is stable for the set's lifetime.
    pub(crate) fn gate_observable(&self) -> Arc<Observable> {
        Arc::clone(&self.inner.gate_observable)
    }

    pub(crate) fn set_wait_timeout(&self, timeout: Duration) {
        *self
            .inner
            .wait_timeout
            .lock()
            .expect("native RF timeout lock") = timeout;
    }

    /// Applies every active binding to `chunk` in binding order, with keys
    /// from `keys`, and records each evaluated row effect into `event_sink`.
    /// `None` when no row is left.
    pub(crate) fn apply_chunk_observed<K: RuntimeFilterKeyProvider + ?Sized>(
        &self,
        chunk: Chunk,
        keys: &mut K,
        event_sink: Option<&Arc<dyn TaskEventSink>>,
    ) -> ExecutionResult<Option<Chunk>> {
        let (output, effects) = self.apply_chunk_inner(chunk, keys)?;
        if let Some(event_sink) = event_sink {
            for effect in effects {
                event_sink.record(TaskEvent::RuntimeFilterRowEffect(effect));
            }
        }
        Ok(output)
    }

    fn apply_chunk_inner<K: RuntimeFilterKeyProvider + ?Sized>(
        &self,
        chunk: Chunk,
        keys: &mut K,
    ) -> ExecutionResult<(Option<Chunk>, Vec<execution::RuntimeFilterRowEffect>)> {
        self.poll_live_bindings()?;
        let active = {
            let bindings = self.inner.bindings.lock().expect("native RF consumer lock");
            if bindings.iter().any(|binding| {
                matches!(
                    binding.state,
                    NativeConsumerBindingState::Unbound
                        | NativeConsumerBindingState::BoundBlocking(_)
                )
            }) {
                return Err(contract_failure(
                    "native runtime-filter consumers must pass their gate before apply",
                ));
            }
            bindings
                .iter()
                .enumerate()
                .filter_map(|(index, binding)| match &binding.state {
                    NativeConsumerBindingState::Active(predicate) => {
                        Some((index, predicate.clone_for_apply()))
                    }
                    _ => None,
                })
                .collect::<Vec<_>>()
        };
        if active.is_empty() {
            return Ok((Some(chunk), Vec::new()));
        }
        let chunk = keys.prepare_input(chunk)?;
        let mut current = Some(chunk);
        let mut effects = Vec::new();
        for (index, predicate) in active {
            let Some(input) = current else {
                return Ok((None, effects));
            };
            let array = keys.evaluate_key(index, &input)?;
            let applied = match predicate {
                NativeConsumerPredicateForApply::Execution(snapshot) => {
                    keys.apply_snapshot(input, &array, &snapshot)?
                }
            };
            match applied {
                RuntimeFilterApplication::NotEvaluated(input) => {
                    self.inner.bindings.lock().expect("native RF consumer lock")[index].state =
                        NativeConsumerBindingState::PassThrough;
                    current = Some(input);
                }
                RuntimeFilterApplication::Evaluated { output, effect } => {
                    effects.push(effect);
                    current = output;
                }
            }
        }
        Ok((current, effects))
    }

    fn poll_live_bindings(&self) -> ExecutionResult<()> {
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
                        binding.contract.binding_id().get(),
                        Arc::clone(subscription),
                        *observed,
                    )),
                    _ => None,
                })
                .collect::<Vec<_>>()
        };
        for (index, binding_id, subscription, observed) in pending {
            let outcome = subscription.poll_after(observed);
            self.apply_execution_live_poll_outcome(index, binding_id, outcome)?;
        }
        Ok(())
    }

    fn apply_execution_live_poll_outcome(
        &self,
        index: usize,
        binding_id: u32,
        outcome: execution::LivePollOutcome,
    ) -> ExecutionResult<()> {
        let mut bindings = self.inner.bindings.lock().expect("native RF consumer lock");
        let Some(binding) = bindings.get_mut(index) else {
            return Err(contract_failure(
                "native Join runtime-filter binding index drifted",
            ));
        };
        let NativeConsumerBindingState::BoundLive { observed, .. } = &mut binding.state else {
            return Ok(());
        };
        let observed_version = *observed;
        if let Some(version) = observed_version
            && version != execution::LogicalVersion::FIRST
        {
            return Err(contract_failure(format!(
                "native Join CompleteOnce runtime-filter binding_id={binding_id} private cursor must use LogicalVersion::FIRST, got {version:?}"
            )));
        }
        match outcome {
            execution::LivePollOutcome::Updated { snapshot, terminal } => {
                if snapshot.logical_version() != execution::LogicalVersion::FIRST {
                    return Err(contract_failure(format!(
                        "native Join CompleteOnce runtime-filter binding_id={binding_id} Updated artifact must use LogicalVersion::FIRST"
                    )));
                }
                if terminal != Some(execution::LiveTerminal::Completed) {
                    return Err(contract_failure(format!(
                        "native Join CompleteOnce runtime-filter binding_id={binding_id} Updated artifact requires terminal Completed, got {terminal:?}"
                    )));
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
                    return Err(contract_failure(format!(
                        "native Join CompleteOnce runtime-filter binding_id={binding_id} Idle latest version must use LogicalVersion::FIRST, got {version:?}"
                    )));
                }
                if terminal == Some(execution::LiveTerminal::Completed) {
                    return Err(contract_failure(format!(
                        "native Join CompleteOnce runtime-filter binding_id={binding_id} reported Completed without the final artifact"
                    )));
                }
                match (observed_version, latest_version) {
                    (None, Some(_)) => {
                        return Err(contract_failure(format!(
                            "native Join CompleteOnce runtime-filter binding_id={binding_id} Idle cursor advanced without returning an artifact"
                        )));
                    }
                    (Some(_), None) => {
                        return Err(contract_failure(format!(
                            "native Join CompleteOnce runtime-filter binding_id={binding_id} Idle cursor regressed from LogicalVersion::FIRST"
                        )));
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
    contract: &execution::RuntimeFilterConsumerContract,
) -> ExecutionResult<execution::RuntimeFilterConsumerContract> {
    if !matches!(
        contract.contract(),
        RuntimeFilterExecutionContract::Membership(_)
    ) {
        return Err(contract_failure(format!(
            "native Join runtime-filter binding_id={} requires a Membership contract",
            contract.binding_id().get()
        )));
    }
    Ok(contract.clone())
}

fn validate_unique_consumer_bindings(
    binding_ids: impl IntoIterator<Item = u32>,
) -> ExecutionResult<()> {
    let mut bindings = BTreeSet::new();
    for binding_id in binding_ids {
        if !bindings.insert(binding_id) {
            return Err(contract_failure(format!(
                "duplicate native runtime-filter consumer binding_id={binding_id}"
            )));
        }
    }
    Ok(())
}

/// The activation and contract every operator that applies a membership
/// filter requires, named by the operator that is about to apply it: the same
/// contract is sound at one and not at another, so the message says which.
/// `validate_key` checks each binding's key coordinate after its contract.
fn validate_membership_consumer_contracts(
    owner: &'static str,
    contracts: &[execution::RuntimeFilterConsumerContract],
    mut validate_key: impl FnMut(
        usize,
        &execution::RuntimeFilterConsumerContract,
    ) -> ExecutionResult<()>,
) -> ExecutionResult<()> {
    validate_unique_consumer_bindings(
        contracts.iter().map(|contract| contract.binding_id().get()),
    )?;
    for (index, contract) in contracts.iter().enumerate() {
        if !matches!(
            contract.activation(),
            execution::ConsumerActivation::BlockingSnapshot
                | execution::ConsumerActivation::NonBlockingLive {
                    late_apply: execution::RuntimeFilterLateApplyGranularity::Batch,
                }
        ) {
            return Err(contract_failure(format!(
                "native {owner} runtime-filter binding_id={} requires BlockingSnapshot or Batch NonBlockingLive",
                contract.binding_id().get()
            )));
        }
        if !matches!(
            contract.contract(),
            RuntimeFilterExecutionContract::Membership(_)
        ) || contract.reduction() != execution::RuntimeFilterReduction::SetUnion
        {
            return Err(contract_failure(format!(
                "native {owner} runtime-filter binding_id={} requires a membership SetUnion contract",
                contract.binding_id().get()
            )));
        }
        validate_key(index, contract)?;
    }
    Ok(())
}

struct RuntimeFilterProcessorConsumers {
    state: RuntimeFilterConsumerState,
    keys: CompiledRuntimeFilterKeys,
}
impl RuntimeFilterProcessorConsumers {
    fn state(&self) -> &RuntimeFilterConsumerState {
        &self.state
    }
    fn apply_chunk_observed(
        &mut self,
        chunk: Chunk,
        event_sink: Option<&Arc<dyn TaskEventSink>>,
    ) -> ExecutionResult<Option<Chunk>> {
        self.state
            .apply_chunk_observed(chunk, &mut self.keys, event_sink)
    }
    fn bind_mem_tracker(&mut self, tracker: Arc<crate::runtime::mem_tracker::MemTracker>) {
        self.keys.bind_mem_tracker(tracker);
    }
    fn bind_runtime_state(&mut self, state: &RuntimeState) {
        self.keys.bind_runtime_state(state);
    }
}

pub(crate) struct NativeRuntimeFilterProcessorFactory {
    name: String,
    consumers: Arc<CompiledRuntimeFilterConsumers>,
}

impl NativeRuntimeFilterProcessorFactory {
    pub(crate) fn new_compiled(
        owner_node_id: i32,
        consumers: Arc<CompiledRuntimeFilterConsumers>,
    ) -> Self {
        Self {
            name: format!("CompiledRuntimeFilter (id={owner_node_id})"),
            consumers,
        }
    }
}

impl OperatorFactory for NativeRuntimeFilterProcessorFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(crate::exec::pipeline::shape::Transform::hydrating(
            NativeRuntimeFilterProcessor {
                name: self.name.clone(),
                consumers: RuntimeFilterProcessorConsumers {
                    state: self.consumers.state().clone(),
                    keys: self.consumers.driver_keys(),
                },
                event_sink: Arc::new(NoopTaskEventSink),
                construction: None,
            },
        ))
    }
}

struct NativeRuntimeFilterProcessor {
    name: String,
    consumers: RuntimeFilterProcessorConsumers,
    event_sink: Arc<dyn TaskEventSink>,
    construction: Option<Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>>,
}

impl OperatorLifecycle for NativeRuntimeFilterProcessor {
    fn creates_typed_storage(&self) -> bool {
        true
    }
    fn set_construction_factory(
        &mut self,
        factory: Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>,
    ) {
        self.consumers
            .keys
            .bind_construction_factory(Arc::clone(&factory));
        self.construction = Some(factory);
    }

    fn set_mem_tracker(&mut self, tracker: Arc<crate::runtime::mem_tracker::MemTracker>) {
        self.consumers.bind_mem_tracker(tracker);
    }
    fn name(&self) -> &str {
        &self.name
    }

    fn set_task_event_sink(&mut self, event_sink: Arc<dyn TaskEventSink>) {
        self.event_sink = event_sink;
    }

    fn bind_runtime_state(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        self.consumers.bind_runtime_state(state);
        Ok(())
    }

    fn activate(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        self.consumers.state().bind(state)
    }
}
impl crate::exec::pipeline::operator::InputEncodingPolicy for NativeRuntimeFilterProcessor {}
impl crate::exec::pipeline::shape::TransformKernel for NativeRuntimeFilterProcessor {
    fn admission(
        &self,
        _ctx: &mut crate::exec::pipeline::driver_step::DriverStep<'_>,
        _input: &crate::exec::pipeline::input::RawInput,
    ) -> ExecutionResult<Option<crate::exec::pipeline::step::AdmissionNext>> {
        use crate::exec::pipeline::step::{AdmissionNext, WaitFact, WaitReason};
        let observable = self.consumers.state().gate_observable();
        let observed_generation = observable.generation();
        match self.consumers.state().poll_gate() {
            RuntimeFilterGate::Open => Ok(None),
            RuntimeFilterGate::Pending => Ok(Some(AdmissionNext::Wait(WaitFact {
                reason: WaitReason::Dependency,
                observable,
                observed_generation,
                deadline: self.consumers.state().gate_deadline(),
            }))),
        }
    }
    fn apply(
        &mut self,
        _ctx: &mut crate::exec::pipeline::driver_step::DriverStep<'_>,
        input: crate::exec::pipeline::input::PreparedInput,
    ) -> ExecutionResult<crate::exec::pipeline::step::StepResult> {
        let chunk = input.into_uncovered_chunk()?;
        let rows = u64::try_from(chunk.len()).expect("supported row count fits u64");
        let output = self
            .consumers
            .apply_chunk_observed(chunk, Some(&self.event_sink))?;
        Ok(crate::exec::pipeline::step::StepResult::new(
            output,
            crate::exec::pipeline::step::NextStep::NeedInput,
            rows,
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use crate::runtime_filter::{
        self as execution, RuntimeFilterArtifactQueryError, RuntimeFilterBindingId,
        RuntimeFilterChannelId, RuntimeFilterConsumerContract, RuntimeFilterExecutionContract,
        RuntimeFilterMembershipSchema, RuntimeFilterNullSemantics, RuntimeFilterScalarRef,
    };
    use arrow::array::{Array, Int32Array};
    use arrow::datatypes::DataType;
    use novarocks_spi::connector::ConnectorScalarValue;

    use super::*;

    use novarocks_local_program::{
        BindingRequirement, BindingRequirements, CompiledProgramFacts, FilterConsumerActivation,
        FilterConsumerAtExpr, FilterNullSemantics, FilterReduction, ImmutableExpressions,
        LocalOperatorId, LocalOperatorOrigin, LocalOperatorProvenance, LocalProgram,
        LocalProgramGraph, ProgramChannelLayoutRole, ProgramChannelSite, ProgramControlFlow,
        ProgramExprId, ProgramExpressionArena, ProgramExpressionRootSite, ProgramExpressionUse,
        ProgramLexicalBindings, ProgramLexicalSource, ProgramNode, ProgramNodeExpressionRole,
        ProgramNodeId, ProgramNodeKind, ProgramResolvedCalls, ProgramRootControlBindings,
        ProgramRootUseBinding, ProgramSlotBinding, ProgramTypedChannels, ProgramTypedExpressions,
        ProgramUseRef, StaticExprKind, StaticExprNode, StaticFilterConsumer, StaticFilterContract,
    };
    use novarocks_type_contract::{
        CompileControlError, CompilePhase, EvaluationDemand, ExpressionEffectContext,
        ExpressionUseId, FunctionValueType, PureCompileControl,
    };
    use std::collections::{BTreeMap, BTreeSet};

    struct NoControl;
    impl PureCompileControl for NoControl {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            Ok(())
        }
    }

    /// One added key root: its site, and the existing leaf root whose control,
    /// definition and lexical source it shares, unless `definition` and `source`
    /// name its own definition and the slot of its own input port.
    struct AddedRoot {
        site: ProgramExpressionRootSite,
        like: ProgramExpressionRootSite,
        definition: Option<ProgramExprId>,
        source: Option<ProgramLexicalSource>,
    }

    /// What a rebuilt program adds to the compiled one.
    #[derive(Default)]
    struct Additions {
        /// A new root node, when an added node is the new root.
        root: Option<ProgramNodeId>,
        /// Definitions appended to the main expression arena, with their types.
        definitions: Vec<(StaticExprNode, FunctionValueType)>,
        requirements: Vec<BindingRequirement>,
        roots: Vec<AddedRoot>,
        channels: Vec<(ProgramChannelSite, FunctionValueType)>,
        operators: Vec<LocalOperatorProvenance>,
    }

    /// `program` with its checked chain rebuilt over `nodes` and `additions`:
    /// extra requirements, one eager key use per added root, the channels and
    /// operators of added nodes. Every other checked fact, provider address and
    /// provenance is carried over as is.
    fn rebuild(
        program: &LocalProgram,
        nodes: Vec<ProgramNode>,
        additions: Additions,
    ) -> Result<Arc<LocalProgram>, String> {
        let Additions {
            root,
            definitions: added_definitions,
            requirements,
            roots,
            channels: added_channels,
            operators: added_operators,
        } = additions;
        let control = NoControl;
        let graph = program.graph();
        let mut entries = graph.requirements().entries().to_vec();
        entries.extend(requirements);
        let main = graph.expressions();
        let arena = if added_definitions.is_empty() {
            Arc::clone(main)
        } else {
            Arc::new(
                ImmutableExpressions::try_new(
                    main.nodes()
                        .iter()
                        .cloned()
                        .chain(added_definitions.iter().map(|(node, _)| node.clone()))
                        .collect(),
                    main.allow_throw_exception(),
                    main.query_global_dicts().clone(),
                    main.session_time_zone().map(Arc::from),
                )
                .map_err(|error| format!("{error:?}"))?,
            )
        };
        let rebuilt = LocalProgramGraph::try_new_with_sink(
            nodes,
            root.unwrap_or(graph.root()),
            Arc::clone(&arena),
            graph.profile(),
            BindingRequirements::try_new(entries).map_err(|error| format!("{error:?}"))?,
            graph.sink().cloned(),
        )
        .map_err(|error| error.to_string())?;
        let checked = program.checked();
        let channels = checked.channels();
        let expressions = channels.expressions();
        let calls = expressions.resolved_calls();
        let snapshot = calls.snapshot();
        let mut flows = snapshot
            .flows()
            .iter()
            .map(|(arena, flow)| {
                (
                    *arena,
                    (
                        flow.domains().values().cloned().collect::<Vec<_>>(),
                        flow.uses().values().cloned().collect::<Vec<_>>(),
                    ),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let mut bindings = snapshot
            .bindings()
            .iter()
            .map(|(site, use_id)| ProgramRootUseBinding {
                site: *site,
                use_id: *use_id,
            })
            .collect::<Vec<_>>();
        let mut slots = checked
            .slots()
            .iter()
            .map(|(occurrence, source)| ProgramSlotBinding {
                occurrence: *occurrence,
                source: *source,
            })
            .collect::<Vec<_>>();
        for root in &roots {
            let arena = root.site.arena();
            let like_use = *snapshot
                .bindings()
                .get(&root.like)
                .ok_or("the template root is bound")?;
            let like = snapshot.flows()[&arena].uses()[&like_use].clone();
            if !like.arguments.is_empty() {
                return Err("a key root template must be a leaf".to_string());
            }
            let (_, uses) = flows.get_mut(&arena).ok_or("the root arena has a flow")?;
            let next = uses
                .iter()
                .map(|existing| existing.context.use_id.get())
                .max()
                .map_or(0, |max| max + 1);
            let use_id = ExpressionUseId::new(next);
            uses.push(ProgramExpressionUse {
                context: ExpressionEffectContext {
                    use_id,
                    domain: like.context.domain,
                    demand: EvaluationDemand::Value,
                },
                definition: root.definition.unwrap_or(like.definition),
                control: like.control,
                arguments: Box::default(),
            });
            bindings.push(ProgramRootUseBinding {
                site: root.site,
                use_id,
            });
            let source = match root.source {
                Some(source) => source,
                None => *checked
                    .slots()
                    .get(&ProgramUseRef {
                        arena,
                        use_id: like_use,
                    })
                    .ok_or("the template root reads a slot")?,
            };
            slots.push(ProgramSlotBinding {
                occurrence: ProgramUseRef { arena, use_id },
                source,
            });
        }
        let prepared_calls = calls
            .calls()
            .iter()
            .map(|(site, call)| (*site, call.specialization().clone()))
            .collect::<Vec<_>>();
        let main_definitions = arena.nodes().len();
        let flows = flows
            .into_iter()
            .map(|(arena, (domains, uses))| {
                let definitions = if arena == ProgramExpressionArena::Main {
                    main_definitions
                } else {
                    snapshot.roots().arenas()[&arena].nodes().len()
                };
                ProgramControlFlow::try_new(domains, uses, definitions, &control)
                    .map(|flow| (arena, flow))
                    .map_err(|error| format!("{error:?}"))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        let snapshot = ProgramRootControlBindings::try_new(rebuilt, flows, bindings, &control)
            .map_err(|error| format!("{error:?}"))?;
        let calls = ProgramResolvedCalls::try_new(snapshot, prepared_calls, &control)
            .map_err(|error| error.to_string())?;
        let typed = ProgramTypedExpressions::try_new(
            calls,
            expressions
                .types()
                .iter()
                .map(|(arena, types)| {
                    let mut types = types.to_vec();
                    if *arena == ProgramExpressionArena::Main {
                        types.extend(added_definitions.iter().map(|(_, value)| {
                            novarocks_type_contract::FunctionArgumentType::Value(value.clone())
                        }));
                    }
                    (*arena, types)
                })
                .collect(),
            &control,
        )
        .map_err(|error| error.to_string())?;
        let typed_channels = ProgramTypedChannels::try_new(
            typed,
            channels
                .channels()
                .iter()
                .map(|(site, value)| (*site, value.clone()))
                .chain(added_channels)
                .collect(),
            &control,
        )
        .map_err(|error| error.to_string())?;
        let lexical = ProgramLexicalBindings::try_new(
            typed_channels,
            checked.lambdas().values().cloned().collect(),
            slots,
            &control,
        )
        .map_err(|error| error.to_string())?;
        let operators = program
            .provenance()
            .operators()
            .values()
            .cloned()
            .chain(added_operators)
            .collect::<Vec<_>>();
        let allowed = operators
            .iter()
            .flat_map(|operator| operator.sources.iter().copied())
            .chain(
                graph
                    .nodes()
                    .iter()
                    .flat_map(|node| node.physical_sources().iter().copied()),
            )
            .collect::<BTreeSet<_>>();
        LocalProgram::try_new(
            lexical,
            operators,
            &allowed,
            CompiledProgramFacts {
                writes: program.write_recipes().clone(),
                exchange_inputs: program.exchange_inputs().clone(),
                scan_inputs: program.scan_inputs().clone(),
                aggregates: program.aggregates().clone(),
            },
            &control,
        )
        .map(Arc::new)
        .map_err(|error| error.to_string())
    }

    struct CompiledFixture {
        consumers: Arc<CompiledRuntimeFilterConsumers>,
        input_schema: arrow::datatypes::SchemaRef,
    }

    /// LocalCompiler authors the typed Int32 Values and identity Project. As in the existing
    /// compiled RF fixtures, add only the frozen RF node/root, control occurrence,
    /// lexical input port, requirement, type and provenance through the real checked
    /// LocalProgram constructors. No expression or runtime-filter evaluator is copied.
    /// This fixture exercises the processor factory directly, not whole-program
    /// admission of RuntimeFilterConsumer nodes by the pipeline builder.
    fn compiled_consumers(
        error: Arc<crate::runtime::runtime_state::RuntimeErrorState>,
    ) -> CompiledFixture {
        use crate::exec::pipeline::builder::{compile_family_fixture, family_package_with_sink};
        use novarocks_physical_plan::{
            ConstantPoolId, ConstantPools, ConstantReference, ExprKind, FragmentBuilder,
            FragmentId, FragmentSink, NodeId, ValueOrigin,
        };
        let mut builder = FragmentBuilder::new(FragmentId::new(19));
        let source = NodeId::new(18);
        let project = NodeId::new(19);
        let declared = FunctionValueType::new(DataType::Int32, false);
        // Physical Int64 literals have an exact Int64 carrier. Preserve the
        // original Int32 input through its checked immutable backing instead.
        let pool_id = ConstantPoolId::new(0);
        let pool = novarocks_functions::ConstantPool::try_new(
            Arc::new(declared.try_to_field("rf_key").unwrap()),
            declared.clone(),
            Int32Array::from(vec![1]).to_data(),
            crate::exec::expr::test_support::constant_policy(),
            CompilePhase::Validate,
            &NoControl,
        )
        .unwrap();
        let cell = builder
            .add_expression(
                source,
                declared.clone(),
                ExprKind::Constant(ConstantReference {
                    pool: pool_id,
                    ordinal: 0,
                }),
            )
            .unwrap();
        let input = builder
            .add_value(
                declared.clone(),
                ValueOrigin::NodeOutput {
                    node: source,
                    output_ordinal: 0,
                },
            )
            .unwrap();
        builder
            .add_values(source, Box::from([Box::from([cell])]), Box::from([input]))
            .unwrap();
        let input_values = [input];
        let mut constants = ConstantPools::empty();
        constants.insert(pool_id, pool).unwrap();
        let expression = builder
            .add_expression(project, declared.clone(), ExprKind::Value(input_values[0]))
            .unwrap();
        let output = builder
            .add_value(
                declared,
                ValueOrigin::Expr {
                    node: project,
                    expr: expression,
                },
            )
            .unwrap();
        builder
            .add_project(
                project,
                source,
                Box::from([(expression, output)]),
                Box::from([output]),
            )
            .unwrap();
        let base = compile_family_fixture(
            family_package_with_sink(builder, project, constants, 1, FragmentSink::Result),
            1,
        );
        let graph = base.graph();
        let input = graph.root();
        let output = graph.nodes()[input.index()].output_layout();
        assert_eq!(output.slots(), &[SlotId::new(1)]);
        let node = ProgramNodeId::new(graph.nodes().len());
        let expr = ProgramExprId::new(graph.expressions().nodes().len());
        let value_type = novarocks_type_contract::FunctionValueType::new(DataType::Int32, false);
        let binding = StaticFilterConsumer::try_new(
            1,
            2,
            FilterConsumerActivation::BlockingSnapshot,
            StaticFilterContract::membership(&value_type, FilterNullSemantics::NeverMatches)
                .unwrap(),
            FilterReduction::SetUnion,
        )
        .unwrap();
        let sources = graph.nodes()[input.index()].physical_sources().to_vec();
        let mut nodes = graph.nodes().to_vec();
        nodes.push(ProgramNode::new_local(
            node,
            sources.clone(),
            ProgramNodeKind::RuntimeFilterConsumer {
                input,
                bindings: vec![FilterConsumerAtExpr {
                    expr_id: expr,
                    consumer: binding,
                }],
            },
            output.clone(),
        ));
        let snapshot = base
            .checked()
            .channels()
            .expressions()
            .resolved_calls()
            .snapshot();
        let like = *snapshot
            .bindings()
            .iter()
            .find_map(|(site, use_id)| {
                let occurrence = &snapshot.flows()[&site.arena()].uses()[use_id];
                matches!(
                    graph.expressions().nodes()[occurrence.definition.index()].kind(),
                    StaticExprKind::SlotId(_)
                )
                .then_some(site)
            })
            .expect("the compiled identity Project has a slot leaf root");
        let owner = base
            .provenance()
            .operators()
            .values()
            .find(|owner| owner.lowered_nodes.contains(&input))
            .unwrap();
        let id = LocalOperatorId::new(
            base.provenance()
                .operators()
                .keys()
                .map(|id| id.get())
                .max()
                .unwrap()
                + 1,
        );
        let program = rebuild(
            &base,
            nodes,
            Additions {
                root: Some(node),
                definitions: vec![(
                    StaticExprNode::new(
                        StaticExprKind::SlotId(SlotId::new(1)),
                        DataType::Int32,
                        None,
                    ),
                    value_type.clone(),
                )],
                requirements: vec![BindingRequirement::RuntimeFilter { binding_id: 1 }],
                roots: vec![AddedRoot {
                    site: ProgramExpressionRootSite::Node {
                        node,
                        role: ProgramNodeExpressionRole::RuntimeFilter { binding: 0 },
                    },
                    like,
                    definition: Some(expr),
                    source: Some(ProgramLexicalSource::Input(ProgramChannelSite::Layout {
                        node: input,
                        role: ProgramChannelLayoutRole::NodeOutput,
                        ordinal: 0,
                    })),
                }],
                channels: vec![(
                    ProgramChannelSite::Layout {
                        node,
                        role: ProgramChannelLayoutRole::NodeOutput,
                        ordinal: 0,
                    },
                    value_type,
                )],
                operators: vec![LocalOperatorProvenance {
                    id,
                    lowered_nodes: Box::from([node]),
                    sources: sources.into_boxed_slice(),
                    origin: LocalOperatorOrigin::Split { piece: 1000 },
                    cost_owner: id,
                    metrics: owner.metrics,
                }],
            },
        )
        .expect("checked compiled membership key");
        let input_schema = Arc::clone(
            program
                .checked()
                .channels()
                .channel_layout(input, ProgramChannelLayoutRole::NodeOutput)
                .expect("exact RF input port")
                .schema(),
        );
        CompiledFixture {
            consumers: Arc::new(
                CompiledRuntimeFilterConsumers::try_new(
                    "Join",
                    program,
                    node,
                    vec![membership_contract()],
                    error,
                )
                .expect("actual compiled consumers"),
            ),
            input_schema,
        }
    }

    fn actual_evaluation_factory(
        state: &RuntimeState,
    ) -> Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory> {
        crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
            crate::runtime::mem_tracker::MemTracker::new_root(
                "runtime filter evaluation construction",
            ),
            state.error_state(),
        )
        .unwrap()
    }

    use crate::exec::chunk::{Chunk, ChunkSchema};
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

    struct PublishedSubscription {
        snapshot: Arc<execution::RuntimeFilterSnapshot>,
        published: Arc<crate::runtime::observable::Observable>,
    }

    impl PublishedSubscription {
        fn new(snapshot: Arc<execution::RuntimeFilterSnapshot>) -> Self {
            Self {
                snapshot,
                published: Arc::new(crate::runtime::observable::Observable::new()),
            }
        }
    }

    impl execution::BlockingSnapshotSubscription for PublishedSubscription {
        fn try_outcome(&self) -> Option<execution::SnapshotAcquireOutcome> {
            Some(execution::SnapshotAcquireOutcome::Published(Arc::clone(
                &self.snapshot,
            )))
        }

        fn outcome_observable(&self) -> Arc<crate::runtime::observable::Observable> {
            Arc::clone(&self.published)
        }

        fn record_consumer_outcome(&self, _: &execution::SnapshotAcquireOutcome) {}

        fn snapshot(&self) -> Option<Arc<execution::RuntimeFilterSnapshot>> {
            Some(Arc::clone(&self.snapshot))
        }
    }

    struct SubscriptionSession {
        outcome: execution::RuntimeFilterBindOutcome<execution::RuntimeFilterSubscriptionPin>,
        calls: Arc<AtomicUsize>,
    }

    impl execution::RuntimeFilterSession for SubscriptionSession {
        fn open_producer(
            &self,
            _: execution::RuntimeFilterProducerOpenRequest,
        ) -> Result<
            execution::RuntimeFilterBindOutcome<execution::SharedRuntimeFilterProducer>,
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
            execution::RuntimeFilterBindOutcome<execution::RuntimeFilterSubscriptionPin>,
            execution::RuntimeFilterContractViolation,
        > {
            self.calls.fetch_add(1, Ordering::SeqCst);
            match &self.outcome {
                execution::RuntimeFilterBindOutcome::Bound(handle) => match handle {
                    execution::RuntimeFilterSubscriptionPin::Blocking(subscription) => {
                        Ok(execution::RuntimeFilterBindOutcome::Bound(
                            execution::RuntimeFilterSubscriptionPin::Blocking(Arc::clone(
                                subscription,
                            )),
                        ))
                    }
                    execution::RuntimeFilterSubscriptionPin::Live(subscription) => {
                        Ok(execution::RuntimeFilterBindOutcome::Bound(
                            execution::RuntimeFilterSubscriptionPin::Live(Arc::clone(subscription)),
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
                execution::SharedRuntimeFilterFinalDomainCompletion,
            >,
            execution::RuntimeFilterContractViolation,
        > {
            Err(execution::RuntimeFilterContractViolation::new(
                execution::RuntimeFilterContractViolationKind::UnauthorizedBinding,
                "consumer-only test session",
            ))
        }
    }

    fn membership_contract() -> RuntimeFilterConsumerContract {
        let schema = RuntimeFilterMembershipSchema::new(
            &DataType::Int32,
            RuntimeFilterNullSemantics::NeverMatches,
        )
        .expect("membership schema");
        RuntimeFilterConsumerContract::membership_blocking(
            RuntimeFilterBindingId::new(1),
            RuntimeFilterChannelId::new(2),
            RuntimeFilterExecutionContract::Membership(schema),
        )
        .expect("consumer contract")
    }

    fn neutral_consumers() -> RuntimeFilterConsumerState {
        RuntimeFilterConsumerState::from_contracts("Join", vec![membership_contract()], |_, _| {
            Ok(())
        })
        .expect("membership consumer state")
    }

    /// The state/mask tests supply an already declared Arrow key. This is not
    /// an expression evaluator; compiled key execution is exercised below.
    struct ColumnKey;
    impl RuntimeFilterKeyProvider for ColumnKey {
        fn prepare_input(&mut self, chunk: Chunk) -> ExecutionResult<Chunk> {
            Ok(chunk)
        }
        fn evaluate_key(&mut self, binding: usize, input: &Chunk) -> ExecutionResult<ArrayRef> {
            assert_eq!(binding, 0);
            assert_eq!(input.columns()[0].data_type(), &DataType::Int32);
            Ok(Arc::clone(&input.columns()[0]))
        }
    }

    fn apply_neutral(
        consumers: &RuntimeFilterConsumerState,
        chunk: Chunk,
    ) -> ExecutionResult<Option<Chunk>> {
        consumers.apply_chunk_observed(chunk, &mut ColumnKey, None)
    }

    #[test]
    fn consumer_plan_requires_the_execution_membership_contract() {
        let consumers = RuntimeFilterConsumerState::from_contracts(
            "Join",
            vec![membership_contract()],
            |_, _| Ok(()),
        );
        assert!(consumers.is_ok());
    }

    #[test]
    fn processor_defers_subscription_until_activation() {
        let error = RuntimeState::default().error_state();
        let factory = NativeRuntimeFilterProcessorFactory::new_compiled(
            7,
            compiled_consumers(error).consumers,
        );
        let calls = Arc::new(AtomicUsize::new(0));
        let session: execution::RuntimeFilterSessionRef = Arc::new(SubscriptionSession {
            outcome: execution::RuntimeFilterBindOutcome::Unavailable(
                execution::UnavailableReason::ResourceLimit,
            ),
            calls: Arc::clone(&calls),
        });
        let state = RuntimeState::default().with_runtime_filter_session(Some(session));
        let mut processor = factory.create(1, 0);
        processor.set_construction_factory(actual_evaluation_factory(&state));

        processor.prepare().expect("prepare processor");
        processor
            .bind_runtime_state(&state)
            .expect("bind processor");
        assert_eq!(calls.load(Ordering::SeqCst), 0);

        processor.activate(&state).expect("activate processor");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn ordered_consumer_rejects_blocking_activation() {
        use novarocks_local_program::{
            FilterConsumerActivation, FilterNullOrder, FilterOrderKey, FilterOrderKeys,
            FilterReduction, FilterSortDirection, StaticFilterConsumer, StaticFilterContract,
        };
        let contract = StaticFilterContract::Ordered {
            keys: FilterOrderKeys::from_legacy(Arc::from([FilterOrderKey {
                value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, true),
                direction: FilterSortDirection::Ascending,
                null_order: FilterNullOrder::First,
            }])),
            // Canonical ascending nullable INT64 / NULLs first declaration.
            comparator_digest: [
                158, 44, 110, 71, 60, 4, 201, 17, 60, 183, 191, 230, 197, 165, 217, 97, 252, 8,
                107, 193, 30, 11, 64, 71, 100, 226, 118, 121, 250, 188, 138, 254,
            ],
            contract_digest: [
                96, 52, 127, 179, 84, 179, 212, 155, 42, 163, 147, 170, 172, 113, 186, 152, 247,
                217, 123, 253, 151, 126, 100, 10, 137, 217, 226, 211, 239, 1, 50, 184,
            ],
        };
        // The frozen vocabulary rejects BlockingSnapshot before a compiled
        // factory can be constructed; do not forge an invalid private binding.
        let error = StaticFilterConsumer::try_new(
            1,
            2,
            FilterConsumerActivation::BlockingSnapshot,
            contract,
            FilterReduction::TightenOrderedBound,
        )
        .expect_err("ordered consumers are live only");
        assert!(matches!(
            error,
            novarocks_local_program::StaticFilterError::BlockingOrderedConsumer
        ));
    }

    #[test]
    fn published_execution_snapshot_applies_the_membership_mask() {
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
            calls: Arc::new(AtomicUsize::new(0)),
            outcome: execution::RuntimeFilterBindOutcome::Bound(
                execution::RuntimeFilterSubscriptionPin::Blocking(Arc::new(
                    PublishedSubscription::new(snapshot),
                )),
            ),
        });
        let consumers = neutral_consumers();
        let state = RuntimeState::default().with_runtime_filter_session(Some(session));
        consumers.bind(&state).expect("bind");
        assert!(matches!(consumers.poll_gate(), RuntimeFilterGate::Open));
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema.arrow_schema_ref(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .expect("batch");
        let output = apply_neutral(&consumers, Chunk::new_with_chunk_schema(batch, schema))
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
        let consumers = neutral_consumers();
        let session: execution::RuntimeFilterSessionRef = Arc::new(SubscriptionSession {
            calls: Arc::new(AtomicUsize::new(0)),
            outcome: execution::RuntimeFilterBindOutcome::Unavailable(
                execution::UnavailableReason::ResourceLimit,
            ),
        });
        let state = RuntimeState::default().with_runtime_filter_session(Some(session));
        consumers.bind(&state).expect("bind");
        assert!(matches!(consumers.poll_gate(), RuntimeFilterGate::Open));
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
        struct NoKey;
        impl RuntimeFilterKeyProvider for NoKey {
            fn prepare_input(&mut self, _: Chunk) -> ExecutionResult<Chunk> {
                panic!("an unavailable binding cannot prepare a key")
            }
            fn evaluate_key(&mut self, _: usize, _: &Chunk) -> ExecutionResult<ArrayRef> {
                panic!("an unavailable binding cannot evaluate a key")
            }
        }
        let output = consumers
            .apply_chunk_observed(input.clone(), &mut NoKey, None)
            .expect("apply")
            .expect("pass-through output");
        assert!(Arc::ptr_eq(
            output.record_batch().column(0),
            input.record_batch().column(0)
        ));
    }

    /// A blocking subscription whose outcome the test publishes, keeping what
    /// the consumer records.
    struct ControlledSubscription {
        outcome: Mutex<Option<execution::SnapshotAcquireOutcome>>,
        published: Arc<Observable>,
        records: Mutex<Vec<&'static str>>,
    }

    impl ControlledSubscription {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                outcome: Mutex::new(None),
                published: Arc::new(Observable::new()),
                records: Mutex::new(Vec::new()),
            })
        }

        fn publish(&self, outcome: execution::SnapshotAcquireOutcome) {
            *self.outcome.lock().expect("outcome lock") = Some(outcome);
            self.published.notify_observers();
        }

        fn records(&self) -> Vec<&'static str> {
            self.records.lock().expect("records lock").clone()
        }
    }

    fn outcome_label(outcome: &execution::SnapshotAcquireOutcome) -> &'static str {
        match outcome {
            execution::SnapshotAcquireOutcome::Published(_) => "published",
            execution::SnapshotAcquireOutcome::Unsupported(_) => "unsupported",
            execution::SnapshotAcquireOutcome::Unavailable(_) => "unavailable",
            execution::SnapshotAcquireOutcome::Cancelled => "cancelled",
            execution::SnapshotAcquireOutcome::TimedOut => "timed_out",
        }
    }

    impl execution::BlockingSnapshotSubscription for ControlledSubscription {
        fn try_outcome(&self) -> Option<execution::SnapshotAcquireOutcome> {
            self.outcome.lock().expect("outcome lock").clone()
        }

        fn outcome_observable(&self) -> Arc<Observable> {
            Arc::clone(&self.published)
        }

        fn record_consumer_outcome(&self, outcome: &execution::SnapshotAcquireOutcome) {
            self.records
                .lock()
                .expect("records lock")
                .push(outcome_label(outcome));
        }

        fn snapshot(&self) -> Option<Arc<execution::RuntimeFilterSnapshot>> {
            match &*self.outcome.lock().expect("outcome lock") {
                Some(execution::SnapshotAcquireOutcome::Published(snapshot)) => {
                    Some(Arc::clone(snapshot))
                }
                _ => None,
            }
        }
    }

    fn accepting(value: i32) -> execution::SnapshotAcquireOutcome {
        execution::SnapshotAcquireOutcome::Published(Arc::new(
            execution::RuntimeFilterSnapshot::new(
                RuntimeFilterBindingId::new(1),
                execution::LogicalVersion::FIRST,
                [0; 32],
                Arc::new(Int32MembershipQuery { accepted: value }),
            ),
        ))
    }

    fn controlled_state(subscription: &Arc<ControlledSubscription>) -> RuntimeState {
        let subscription: Arc<dyn execution::BlockingSnapshotSubscription> =
            Arc::clone(subscription) as Arc<dyn execution::BlockingSnapshotSubscription>;
        let session: execution::RuntimeFilterSessionRef = Arc::new(SubscriptionSession {
            calls: Arc::new(AtomicUsize::new(0)),
            outcome: execution::RuntimeFilterBindOutcome::Bound(
                execution::RuntimeFilterSubscriptionPin::Blocking(subscription),
            ),
        });
        RuntimeState::default().with_runtime_filter_session(Some(session))
    }

    fn controlled_consumers(
        timeout: Duration,
    ) -> (RuntimeFilterConsumerState, Arc<ControlledSubscription>) {
        let subscription = ControlledSubscription::new();
        let consumers = neutral_consumers();
        consumers
            .bind(&controlled_state(&subscription))
            .expect("bind");
        consumers.set_wait_timeout(timeout);
        (consumers, subscription)
    }

    #[test]
    fn created_keys_preserve_the_original_fragment_failure() {
        let subscription = ControlledSubscription::new();
        let runtime = controlled_state(&subscription);
        let original = ExecutionFailure::from(novarocks_functions::KernelFailure::Operational(
            novarocks_functions::KernelDiagnostic::new("runtime-filter fragment failed"),
        ));
        let fixture = compiled_consumers(runtime.error_state());
        let consumers = fixture.consumers;
        consumers.state().bind(&runtime).expect("bind");
        subscription.publish(accepting(2));
        assert!(matches!(
            consumers.state().poll_gate(),
            RuntimeFilterGate::Open
        ));
        let creation_runtime = RuntimeState::default();
        let factory = actual_evaluation_factory(&creation_runtime);
        let mut keys = consumers.driver_keys();
        keys.bind_construction_factory(factory);
        keys.bind_runtime_state(&creation_runtime);
        runtime.error_state().set_error(original.clone());
        let error = consumers
            .state()
            .apply_chunk_observed(
                compiled_int_chunk(Arc::clone(&fixture.input_schema), &[1, 2, 3]),
                &mut keys,
                None,
            )
            .unwrap_err();
        assert_eq!(error, original);
        assert!(
            error.same_occurrence(&original),
            "the original source occurrence survives the created-key path"
        );
    }

    fn compiled_int_chunk(schema: arrow::datatypes::SchemaRef, values: &[i32]) -> Chunk {
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(values.to_vec())) as ArrayRef],
        )
        .unwrap();
        crate::exec::chunk::typed_fixture::chunk(
            batch,
            &[SlotId::new(1)],
            &[novarocks_type_contract::FunctionValueType::new(
                DataType::Int32,
                false,
            )],
        )
    }

    fn int_chunk(values: &[i32]) -> Chunk {
        let schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
            &arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new(
                "v",
                DataType::Int32,
                false,
            )]),
            &[SlotId::new(1)],
        )
        .expect("chunk schema");
        Chunk::new_with_chunk_schema(
            arrow::record_batch::RecordBatch::try_new(
                schema.arrow_schema_ref(),
                vec![Arc::new(Int32Array::from(values.to_vec()))],
            )
            .expect("batch"),
            schema,
        )
    }

    fn int_values(chunk: &Chunk) -> Vec<i32> {
        chunk.columns()[0]
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 column")
            .values()
            .to_vec()
    }

    #[test]
    fn a_gate_wait_starts_at_its_first_touch_and_is_shared_by_its_set() {
        let (consumers, _subscription) = controlled_consumers(Duration::from_millis(500));
        assert!(
            !consumers.gate_holds_input(),
            "an untouched gate holds nothing"
        );
        assert!(consumers.gate_deadline().is_none());

        std::thread::sleep(Duration::from_millis(30));
        let touched_at = Instant::now();
        assert!(
            matches!(consumers.poll_gate(), RuntimeFilterGate::Pending),
            "an unpublished snapshot keeps the gate pending"
        );
        let deadline = consumers
            .gate_deadline()
            .expect("a touched gate has a deadline");
        assert!(
            deadline.at() >= touched_at + Duration::from_millis(500),
            "the wait starts when input is at hand, not at bind"
        );

        let other_driver = consumers.clone();
        std::thread::sleep(Duration::from_millis(10));
        assert!(
            matches!(other_driver.poll_gate(), RuntimeFilterGate::Pending),
            "the shared gate is still pending"
        );
        let shared = other_driver.gate_deadline().expect("shared deadline");
        assert_eq!(shared, deadline, "drivers of one set share one total wait");
        assert!(other_driver.gate_holds_input());

        let (independent, _) = controlled_consumers(Duration::from_millis(500));
        assert!(
            independent.gate_deadline().is_none(),
            "another consumer set keeps its own wait"
        );
    }

    #[test]
    fn a_publication_after_the_check_moves_the_generation_and_opens_the_gate_once() {
        let (consumers, subscription) = controlled_consumers(Duration::from_secs(5));
        let observable = consumers.gate_observable();
        let generation = observable.generation();
        assert!(matches!(consumers.poll_gate(), RuntimeFilterGate::Pending));

        subscription.publish(accepting(2));
        assert!(
            observable.generation() > generation,
            "a publication must wake whoever parked on the pending answer"
        );
        assert!(matches!(consumers.poll_gate(), RuntimeFilterGate::Open));
        assert!(matches!(consumers.poll_gate(), RuntimeFilterGate::Open));
        assert!(!consumers.gate_holds_input());
        assert_eq!(subscription.records(), vec!["published"]);

        let output = apply_neutral(&consumers, int_chunk(&[1, 2, 3]))
            .expect("apply")
            .expect("one matching row");
        assert_eq!(int_values(&output), vec![2]);
    }

    #[test]
    fn an_expired_wait_passes_input_through_as_timed_out_once() {
        let (consumers, subscription) = controlled_consumers(Duration::from_millis(20));
        assert!(matches!(consumers.poll_gate(), RuntimeFilterGate::Pending));
        std::thread::sleep(Duration::from_millis(40));

        assert!(matches!(consumers.poll_gate(), RuntimeFilterGate::Open));
        // A late publication does not reopen a settled binding.
        subscription.publish(accepting(2));
        assert!(matches!(consumers.poll_gate(), RuntimeFilterGate::Open));
        assert_eq!(subscription.records(), vec!["timed_out"]);
        let output = apply_neutral(&consumers, int_chunk(&[1, 2, 3]))
            .expect("apply")
            .expect("pass-through");
        assert_eq!(int_values(&output), vec![1, 2, 3]);
    }

    /// A source that emits the chunks the test gives it.
    struct QueueSource {
        chunks: Arc<Mutex<std::collections::VecDeque<Chunk>>>,
        finished: Arc<std::sync::atomic::AtomicBool>,
        observable: Arc<Observable>,
    }

    impl OperatorLifecycle for QueueSource {
        fn name(&self) -> &str {
            "QUEUE_SOURCE"
        }
    }
    impl crate::exec::pipeline::shape::SourceKernel for QueueSource {
        fn poll_output(
            &mut self,
            _: &mut crate::exec::pipeline::driver_step::DriverStep<'_>,
        ) -> ExecutionResult<crate::exec::pipeline::step::StepResult> {
            let generation = self.observable.generation();
            let mut queue = self.chunks.lock().expect("chunks lock");
            let output = queue.pop_front();
            let closed = queue.is_empty() && self.finished.load(Ordering::Acquire);
            let next = if closed {
                crate::exec::pipeline::step::NextStep::Finished
            } else if output.is_some() {
                crate::exec::pipeline::step::NextStep::Runnable
            } else {
                crate::exec::pipeline::step::NextStep::Wait(crate::exec::pipeline::step::WaitFact {
                    reason: crate::exec::pipeline::step::WaitReason::Input,
                    observable: self.observable.clone(),
                    observed_generation: generation,
                    deadline: None,
                })
            };
            Ok(crate::exec::pipeline::step::StepResult::new(
                output, next, 0,
            ))
        }
    }
    struct CollectSink {
        values: Arc<Mutex<Vec<i32>>>,
    }
    impl OperatorLifecycle for CollectSink {
        fn name(&self) -> &str {
            "COLLECT_SINK"
        }
    }
    impl crate::exec::pipeline::operator::InputEncodingPolicy for CollectSink {}
    impl crate::exec::pipeline::shape::SinkKernel for CollectSink {
        fn consume(
            &mut self,
            _: &mut crate::exec::pipeline::driver_step::DriverStep<'_>,
            input: crate::exec::pipeline::input::PreparedInput,
        ) -> ExecutionResult<crate::exec::pipeline::shape::SinkStep> {
            let chunk = input.into_uncovered_chunk()?;
            self.values
                .lock()
                .expect("values lock")
                .extend(int_values(&chunk));
            Ok(crate::exec::pipeline::shape::SinkStep::new(
                crate::exec::pipeline::step::NextStep::NeedInput,
                chunk.len() as u64,
            ))
        }
    }

    struct GatedPipeline {
        driver: crate::exec::pipeline::driver::PipelineDriver,
        chunks: Arc<Mutex<std::collections::VecDeque<Chunk>>>,
        finished: Arc<std::sync::atomic::AtomicBool>,
        source: Arc<Observable>,
        values: Arc<Mutex<Vec<i32>>>,
        subscription: Arc<ControlledSubscription>,
        input_schema: arrow::datatypes::SchemaRef,
    }

    fn gated_pipeline(timeout: Duration) -> GatedPipeline {
        let subscription = ControlledSubscription::new();
        let session = controlled_state(&subscription)
            .runtime_filter_session()
            .cloned();
        let state = Arc::new(
            RuntimeState::for_local_execution(
                Some(crate::runtime::query_options::QueryOptions {
                    runtime_filter_wait_timeout_ms: Some(
                        i32::try_from(timeout.as_millis()).unwrap(),
                    ),
                    ..Default::default()
                }),
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .with_runtime_filter_session(session),
        );
        let fixture = compiled_consumers(state.error_state());
        let factory = NativeRuntimeFilterProcessorFactory::new_compiled(5, fixture.consumers);
        let mut processor = factory.create(1, 0);
        processor.set_construction_factory(actual_evaluation_factory(&state));
        processor
            .bind_runtime_state(&state)
            .expect("bind processor");
        let chunks = Arc::new(Mutex::new(std::collections::VecDeque::new()));
        let finished = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let source = Arc::new(Observable::new());
        let values = Arc::new(Mutex::new(Vec::new()));
        let driver = crate::exec::pipeline::driver::PipelineDriver::new(
            1,
            vec![
                Box::new(crate::exec::pipeline::shape::Source::new(QueueSource {
                    chunks: Arc::clone(&chunks),
                    finished: Arc::clone(&finished),
                    observable: Arc::clone(&source),
                })),
                processor,
                Box::new(crate::exec::pipeline::shape::Transform::hydrating(
                    CollectSink {
                        values: Arc::clone(&values),
                    },
                )),
            ],
            None,
            Vec::new(),
            state,
            None,
        );
        GatedPipeline {
            driver,
            chunks,
            finished,
            source,
            values,
            subscription,
            input_schema: fixture.input_schema,
        }
    }

    #[test]
    fn a_processor_gate_holds_the_first_chunk_on_its_edge_until_it_opens() {
        use crate::exec::pipeline::driver::DriverState;
        use crate::exec::pipeline::operator::BlockedReason;

        let mut pipeline = gated_pipeline(Duration::from_secs(5));
        // No input yet: the driver waits on its source, and the gate's wait
        // has not started however long the first chunk takes.
        assert!(matches!(
            pipeline.driver.process(Duration::from_millis(10)),
            DriverState::Blocked(BlockedReason::InputEmpty)
        ));
        std::thread::sleep(Duration::from_millis(20));
        pipeline
            .chunks
            .lock()
            .expect("chunks lock")
            .push_back(compiled_int_chunk(
                Arc::clone(&pipeline.input_schema),
                &[1, 2, 3],
            ));
        pipeline.source.notify_observers();

        let arrived = Instant::now();
        assert!(matches!(
            pipeline.driver.process(Duration::from_millis(10)),
            DriverState::Blocked(BlockedReason::OutputFull)
        ));
        let (observable, observed, deadline) = pipeline
            .driver
            .blocked_observable_snapshot()
            .expect("parked on the gate");
        let deadline = deadline.expect("a gate wait is bounded");
        assert!(deadline.at() >= arrived + Duration::from_secs(5));
        assert!(pipeline.values.lock().expect("values lock").is_empty());

        pipeline.subscription.publish(accepting(2));
        assert_ne!(
            observable.generation(),
            observed,
            "gate publication invalidates the driver snapshot"
        );
        assert!(matches!(
            pipeline.driver.process(Duration::from_millis(10)),
            DriverState::Blocked(BlockedReason::InputEmpty)
        ));
        assert_eq!(*pipeline.values.lock().expect("values lock"), vec![2]);

        pipeline
            .finished
            .store(true, std::sync::atomic::Ordering::Release);
        pipeline.source.notify_observers();
        assert!(matches!(
            pipeline.driver.process(Duration::from_millis(10)),
            DriverState::Finished
        ));
        assert_eq!(pipeline.subscription.records(), vec!["published"]);
    }

    #[test]
    fn a_pipeline_ending_before_its_first_chunk_never_touches_the_gate() {
        use crate::exec::pipeline::driver::DriverState;

        let mut pipeline = gated_pipeline(Duration::from_secs(5));
        pipeline
            .finished
            .store(true, std::sync::atomic::Ordering::Release);
        pipeline.source.notify_observers();
        assert!(matches!(
            pipeline.driver.process(Duration::from_millis(10)),
            DriverState::Finished
        ));
        assert!(
            pipeline.subscription.records().is_empty(),
            "no input reached the gate, so no outcome is consumed"
        );
    }
}
