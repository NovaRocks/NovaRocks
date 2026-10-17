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

use crate::runtime::fragment::ExecutionResult;

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::runtime_filter as execution;

use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use crate::exec::node::runtime_filter::{
    RuntimeFilterConsumerBinding, RuntimeFilterExecutionContract,
};
use crate::exec::pipeline::operator::{
    DriverBlockDeadline, Operator, ProcessorOperator, forward_observable,
};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::schedule::observer::Observable;
use crate::runtime::fragment::io::{FragmentEvent, FragmentEventSink, NoopFragmentEventSink};
use crate::runtime::runtime_state::RuntimeState;
use arrow::array::ArrayRef;
use arrow::compute::filter_record_batch;

mod compiled;
mod compiled_ordered;
pub(crate) use compiled_ordered::{CompiledOrderedScanDriver, CompiledOrderedScanFactory};
mod live_cursor;
#[cfg(test)]
mod live_cursor_tests;
pub(crate) use compiled::{CompiledRuntimeFilterConsumers, CompiledRuntimeFilterKeys};

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

#[allow(
    dead_code,
    reason = "Ordered live-consumer helpers remain available for native scan integration paths."
)]
impl NativeOrderedLiveConsumerSet {
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
        self.poll_and_apply_chunk_observed(chunk, None)
    }

    pub(crate) fn poll_and_apply_chunk_observed(
        &self,
        chunk: Chunk,
        event_sink: Option<&Arc<dyn FragmentEventSink>>,
    ) -> Result<Option<Chunk>, String> {
        self.poll_updates()?;
        self.apply_latest_chunk_observed(chunk, event_sink)
    }

    pub(crate) fn poll_updates(&self) -> Result<(), String> {
        self.poll()
    }

    pub(crate) fn apply_latest_chunk_observed(
        &self,
        chunk: Chunk,
        event_sink: Option<&Arc<dyn FragmentEventSink>>,
    ) -> Result<Option<Chunk>, String> {
        self.apply_latest_chunk_with(
            chunk,
            event_sink,
            |expr, input| self.inner.arena.eval(expr, input),
            |input| crate::exec::chunk::hydrate_dictionary_columns_except(&input, |_, _| false),
            |input, mask| {
                let batch = filter_record_batch(input.record_batch(), mask)
                    .map_err(|error| error.to_string())?;
                Chunk::try_new_like(batch, input)
            },
        )
    }

    pub(crate) fn apply_latest_chunk_created(
        &self,
        chunk: Chunk,
        factory: &dyn crate::exec::pipeline::operator::InputConstructionFactory,
        control: &dyn crate::exec::expr::ExprEvaluationControl,
        event_sink: Option<&Arc<dyn FragmentEventSink>>,
    ) -> ExecutionResult<Option<Chunk>> {
        self.apply_latest_chunk_with(
            chunk,
            event_sink,
            |expr, input| {
                let owner = factory.begin_input_construction(input)?;
                crate::exec::expr::ExprEvaluationContext::new(
                    &self.inner.arena,
                    owner.as_ref(),
                    control,
                )
                .eval(expr, input)
            },
            |input| {
                let owner = factory.begin_input_construction(&input)?;
                input.hydrate_for_evaluation(owner.as_ref(), |_, _| false)
            },
            |input, mask| {
                let owner = factory.begin_input_construction(input)?;
                input.filter_for_evaluation(mask, owner.as_ref())
            },
        )
    }

    fn apply_latest_chunk_with<E: From<String>, F, H, C>(
        &self,
        chunk: Chunk,
        event_sink: Option<&Arc<dyn FragmentEventSink>>,
        evaluate: F,
        hydrate: H,
        filter: C,
    ) -> Result<Option<Chunk>, E>
    where
        F: FnMut(ExprId, &Chunk) -> Result<ArrayRef, E>,
        H: FnMut(Chunk) -> Result<Chunk, E>,
        C: FnMut(&Chunk, &arrow::array::BooleanArray) -> Result<Chunk, E>,
    {
        let (output, effects) = self.apply_chunk_inner(chunk, evaluate, hydrate, filter)?;
        if let Some(event_sink) = event_sink {
            for effect in effects {
                event_sink.record(FragmentEvent::RuntimeFilterRowEffect(effect));
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
        match live_cursor::apply_poll_outcome(observed, latest_snapshot, terminal, outcome) {
            Ok(live_cursor::LiveCursorDisposition::Keep) => {}
            Ok(live_cursor::LiveCursorDisposition::PassThrough) => {
                binding.state = NativeOrderedLiveBindingState::PassThrough;
            }
            Err(live_cursor::LiveCursorViolation::NewerVersionWithoutArtifact) => {
                return Err(format!(
                    "native ordered runtime-filter binding_id={} reported a newer live version without artifact",
                    spec.binding_id()
                ));
            }
            Err(live_cursor::LiveCursorViolation::Regressed) => {
                return Err(format!(
                    "native ordered runtime-filter binding_id={} live cursor regressed",
                    spec.binding_id()
                ));
            }
        }
        Ok(())
    }

    fn apply_chunk_inner<E: From<String>, F, H, C>(
        &self,
        chunk: Chunk,
        mut evaluate: F,
        mut hydrate: H,
        mut filter: C,
    ) -> Result<(Option<Chunk>, Vec<execution::RuntimeFilterRowEffect>), E>
    where
        F: FnMut(ExprId, &Chunk) -> Result<ArrayRef, E>,
        H: FnMut(Chunk) -> Result<Chunk, E>,
        C: FnMut(&Chunk, &arrow::array::BooleanArray) -> Result<Chunk, E>,
    {
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
        let chunk = hydrate(chunk)?;
        let mut current = Some(chunk);
        let mut effects = Vec::new();
        for (expr_id, predicate) in active {
            let Some(input) = current else {
                return Ok((None, effects));
            };
            let array = evaluate(expr_id, &input)?;
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
                current = Some(filter(&input, &mask)?);
            }
        }
        Ok((current, effects))
    }
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
/// is, so the same state serves an ExprArena plan and a compiled program.
pub(crate) trait RuntimeFilterKeyProvider {
    /// A failure of preparation or key evaluation. Every failure the state
    /// itself observes is reported through `From<String>`.
    type Error: From<String>;

    /// The chunk every key is evaluated over and every mask selects from,
    /// prepared once for each chunk at least one binding applies to.
    fn prepare_input(&mut self, chunk: Chunk) -> Result<Chunk, Self::Error>;

    /// The key of the binding at position `binding` of the set over `input`.
    fn evaluate_key(&mut self, binding: usize, input: &Chunk) -> Result<ArrayRef, Self::Error>;

    /// The original decoder and mask application policy. A direct ordered
    /// Scan overrides only this policy to retain its controlled output stock.
    fn apply_snapshot(
        &mut self,
        input: Chunk,
        array: &ArrayRef,
        snapshot: &execution::RuntimeFilterSnapshot,
    ) -> Result<RuntimeFilterApplication, Self::Error> {
        apply_legacy_runtime_filter_snapshot(input, array, snapshot)
    }
}

fn apply_legacy_runtime_filter_snapshot<E: From<String>>(
    input: Chunk,
    array: &ArrayRef,
    snapshot: &execution::RuntimeFilterSnapshot,
) -> Result<RuntimeFilterApplication, E> {
    apply_runtime_filter_snapshot_with(input, array, snapshot, |input, mask| {
        let batch =
            filter_record_batch(input.record_batch(), mask).map_err(|error| error.to_string())?;
        Ok(Chunk::try_new_like(batch, input)?)
    })
}

fn apply_runtime_filter_snapshot_with<E: From<String>, F>(
    input: Chunk,
    array: &ArrayRef,
    snapshot: &execution::RuntimeFilterSnapshot,
    mut filter: F,
) -> Result<RuntimeFilterApplication, E>
where
    F: FnMut(&Chunk, &arrow::array::BooleanArray) -> Result<Chunk, E>,
{
    let outcome = execution::evaluator::evaluate_rows(
        snapshot.binding_id(),
        snapshot.logical_version(),
        snapshot.artifact_query().as_ref(),
        array,
    )
    .map_err(|error| error.to_string())?;
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
        validate_key: impl FnMut(usize, &execution::RuntimeFilterConsumerContract) -> Result<(), String>,
    ) -> Result<Self, String> {
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
                execution_membership_consumer_contract(&binding.contract)?,
            );
            match execution::RuntimeFilterSession::subscribe(session.as_ref(), request) {
                Ok(execution::RuntimeFilterBindOutcome::Bound(
                    execution::RuntimeFilterSubscriptionHandle::Blocking(subscription),
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
                    execution::RuntimeFilterSubscriptionHandle::Live(subscription),
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
                    return Err(format!(
                        "native Join runtime-filter binding_id={} session returned an activation-mismatched subscription",
                        binding.contract.binding_id().get()
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
            match binding.contract.activation() {
                execution::ConsumerActivation::BlockingSnapshot
                | execution::ConsumerActivation::NonBlockingLive {
                    late_apply: execution::RuntimeFilterLateApplyGranularity::Batch,
                } => {}
                execution::ConsumerActivation::NonBlockingLive { .. } => {
                    return Err(format!(
                        "native Join runtime-filter binding_id={} has unsupported activation",
                        binding.contract.binding_id().get()
                    ));
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
        event_sink: Option<&Arc<dyn FragmentEventSink>>,
    ) -> Result<Option<Chunk>, K::Error> {
        let (output, effects) = self.apply_chunk_inner(chunk, keys)?;
        if let Some(event_sink) = event_sink {
            for effect in effects {
                event_sink.record(FragmentEvent::RuntimeFilterRowEffect(effect));
            }
        }
        Ok(output)
    }

    fn apply_chunk_inner<K: RuntimeFilterKeyProvider + ?Sized>(
        &self,
        chunk: Chunk,
        keys: &mut K,
    ) -> Result<(Option<Chunk>, Vec<execution::RuntimeFilterRowEffect>), K::Error> {
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
                return Err(
                    "native runtime-filter consumers must pass their gate before apply"
                        .to_string()
                        .into(),
                );
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
                "native Join CompleteOnce runtime-filter binding_id={binding_id} private cursor must use LogicalVersion::FIRST, got {version:?}"
            ));
        }
        match outcome {
            execution::LivePollOutcome::Updated { snapshot, terminal } => {
                if snapshot.logical_version() != execution::LogicalVersion::FIRST {
                    return Err(format!(
                        "native Join CompleteOnce runtime-filter binding_id={binding_id} Updated artifact must use LogicalVersion::FIRST"
                    ));
                }
                if terminal != Some(execution::LiveTerminal::Completed) {
                    return Err(format!(
                        "native Join CompleteOnce runtime-filter binding_id={binding_id} Updated artifact requires terminal Completed, got {terminal:?}"
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
                        "native Join CompleteOnce runtime-filter binding_id={binding_id} Idle latest version must use LogicalVersion::FIRST, got {version:?}"
                    ));
                }
                if terminal == Some(execution::LiveTerminal::Completed) {
                    return Err(format!(
                        "native Join CompleteOnce runtime-filter binding_id={binding_id} reported Completed without the final artifact"
                    ));
                }
                match (observed_version, latest_version) {
                    (None, Some(_)) => {
                        return Err(format!(
                            "native Join CompleteOnce runtime-filter binding_id={binding_id} Idle cursor advanced without returning an artifact"
                        ));
                    }
                    (Some(_), None) => {
                        return Err(format!(
                            "native Join CompleteOnce runtime-filter binding_id={binding_id} Idle cursor regressed from LogicalVersion::FIRST"
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

/// An ExprArena plan's runtime-filter consumers: the arena-free state, and
/// the arena expression of each binding's key.
#[derive(Clone)]
pub(crate) struct RuntimeFilterConsumerSet {
    state: RuntimeFilterConsumerState,
    keys: Arc<ArenaRuntimeFilterKeys>,
}

/// The ExprArena key of every binding of a consumer set, in binding order.
/// Keys are evaluated over the chunk with every dictionary column hydrated,
/// and the hydrated chunk is what the filters select from.
struct ArenaRuntimeFilterKeys {
    arena: Arc<ExprArena>,
    expr_ids: Vec<ExprId>,
}

impl RuntimeFilterKeyProvider for &ArenaRuntimeFilterKeys {
    type Error = String;

    fn prepare_input(&mut self, chunk: Chunk) -> Result<Chunk, String> {
        crate::exec::chunk::hydrate_dictionary_columns_except(&chunk, |_, _| false)
    }

    fn evaluate_key(&mut self, binding: usize, input: &Chunk) -> Result<ArrayRef, String> {
        let expr_id = *self
            .expr_ids
            .get(binding)
            .ok_or("native runtime-filter binding index drifted")?;
        self.arena.eval(expr_id, input)
    }
}

struct CreatedArenaRuntimeFilterKeys<'a> {
    keys: &'a ArenaRuntimeFilterKeys,
    factory: &'a dyn crate::exec::pipeline::operator::InputConstructionFactory,
    control: &'a dyn crate::exec::expr::ExprEvaluationControl,
}
impl RuntimeFilterKeyProvider for CreatedArenaRuntimeFilterKeys<'_> {
    type Error = crate::runtime::fragment::ExecutionFailure;

    fn apply_snapshot(
        &mut self,
        input: Chunk,
        array: &ArrayRef,
        snapshot: &execution::RuntimeFilterSnapshot,
    ) -> ExecutionResult<RuntimeFilterApplication> {
        apply_runtime_filter_snapshot_with(input, array, snapshot, |input, mask| {
            let owner = self.factory.begin_input_construction(input)?;
            input.filter_for_evaluation(mask, owner.as_ref())
        })
    }

    fn prepare_input(&mut self, chunk: Chunk) -> ExecutionResult<Chunk> {
        let owner = self.factory.begin_input_construction(&chunk)?;
        chunk.hydrate_for_evaluation(owner.as_ref(), |_, _| false)
    }

    fn evaluate_key(&mut self, binding: usize, input: &Chunk) -> ExecutionResult<ArrayRef> {
        let expr = *self
            .keys
            .expr_ids
            .get(binding)
            .ok_or("native runtime-filter binding index drifted")?;
        let owner = self.factory.begin_input_construction(input)?;
        crate::exec::expr::ExprEvaluationContext::new(
            &self.keys.arena,
            owner.as_ref(),
            self.control,
        )
        .eval(expr, input)
    }
}

#[allow(
    dead_code,
    reason = "The direct consumer API remains available for non-polling integration callers."
)]
impl RuntimeFilterConsumerSet {
    pub(crate) fn from_plan(
        owner: &'static str,
        specs: &[RuntimeFilterConsumerBinding],
        arena: Arc<ExprArena>,
    ) -> Result<Self, String> {
        let state = RuntimeFilterConsumerState::from_contracts(
            owner,
            specs.iter().map(|spec| spec.contract().clone()).collect(),
            |index, contract| {
                if arena.data_type(specs[index].expr_id).is_none() {
                    return Err(format!(
                        "native Join runtime-filter binding_id={} expression is missing",
                        contract.binding_id().get()
                    ));
                }
                Ok(())
            },
        )?;
        Ok(Self {
            state,
            keys: Arc::new(ArenaRuntimeFilterKeys {
                arena,
                expr_ids: specs.iter().map(|spec| spec.expr_id).collect(),
            }),
        })
    }

    /// The arena-free state every driver of this consumer shares.
    pub(crate) fn state(&self) -> &RuntimeFilterConsumerState {
        &self.state
    }

    pub(crate) fn bind(&self, state: &RuntimeState) -> Result<(), String> {
        self.state.bind(state)
    }

    pub(crate) fn poll_gate(&self) -> RuntimeFilterGate {
        self.state.poll_gate()
    }

    pub(crate) fn gate_holds_input(&self) -> bool {
        self.state.gate_holds_input()
    }

    pub(crate) fn gate_deadline(&self) -> Option<DriverBlockDeadline> {
        self.state.gate_deadline()
    }

    pub(crate) fn gate_observable(&self) -> Arc<Observable> {
        self.state.gate_observable()
    }

    pub(crate) fn set_wait_timeout(&self, timeout: Duration) {
        self.state.set_wait_timeout(timeout);
    }

    pub(crate) fn apply_chunk_created(
        &self,
        chunk: Chunk,
        factory: &dyn crate::exec::pipeline::operator::InputConstructionFactory,
        control: &dyn crate::exec::expr::ExprEvaluationControl,
        event_sink: Option<&Arc<dyn FragmentEventSink>>,
    ) -> ExecutionResult<Option<Chunk>> {
        let mut keys = CreatedArenaRuntimeFilterKeys {
            keys: &self.keys,
            factory,
            control,
        };
        self.state
            .apply_chunk_observed(chunk, &mut keys, event_sink)
    }

    pub(crate) fn apply_chunk(&self, chunk: Chunk) -> Result<Option<Chunk>, String> {
        self.apply_chunk_observed(chunk, None)
    }

    pub(crate) fn apply_chunk_observed(
        &self,
        chunk: Chunk,
        event_sink: Option<&Arc<dyn FragmentEventSink>>,
    ) -> Result<Option<Chunk>, String> {
        self.state
            .apply_chunk_observed(chunk, &mut &*self.keys, event_sink)
    }
}

fn execution_membership_consumer_contract(
    contract: &execution::RuntimeFilterConsumerContract,
) -> Result<execution::RuntimeFilterConsumerContract, String> {
    if !matches!(
        contract.contract(),
        RuntimeFilterExecutionContract::Membership(_)
    ) {
        return Err(format!(
            "native Join runtime-filter binding_id={} requires a Membership contract",
            contract.binding_id().get()
        ));
    }
    Ok(contract.clone())
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

fn validate_unique_consumer_bindings(
    binding_ids: impl IntoIterator<Item = u32>,
) -> Result<(), String> {
    let mut bindings = BTreeSet::new();
    for binding_id in binding_ids {
        if !bindings.insert(binding_id) {
            return Err(format!(
                "duplicate native runtime-filter consumer binding_id={binding_id}"
            ));
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
    mut validate_key: impl FnMut(usize, &execution::RuntimeFilterConsumerContract) -> Result<(), String>,
) -> Result<(), String> {
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
            return Err(format!(
                "native {owner} runtime-filter binding_id={} requires BlockingSnapshot or Batch NonBlockingLive",
                contract.binding_id().get()
            ));
        }
        if !matches!(
            contract.contract(),
            RuntimeFilterExecutionContract::Membership(_)
        ) || contract.reduction() != execution::RuntimeFilterReduction::SetUnion
        {
            return Err(format!(
                "native {owner} runtime-filter binding_id={} requires a membership SetUnion contract",
                contract.binding_id().get()
            ));
        }
        validate_key(index, contract)?;
    }
    Ok(())
}

fn validate_ordered_live_plan_specs(
    specs: &[RuntimeFilterConsumerBinding],
    arena: &ExprArena,
) -> Result<(), String> {
    validate_unique_consumer_bindings(specs.iter().map(RuntimeFilterConsumerBinding::binding_id))?;
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

enum RuntimeFilterProcessorPlan {
    Arena(RuntimeFilterConsumerSet),
    Compiled(Arc<CompiledRuntimeFilterConsumers>),
}

#[cfg(test)]
impl RuntimeFilterProcessorPlan {
    fn gate_observable(&self) -> Arc<Observable> {
        match self {
            Self::Arena(consumers) => consumers.gate_observable(),
            Self::Compiled(consumers) => consumers.state().gate_observable(),
        }
    }
}

enum RuntimeFilterProcessorConsumers {
    Arena(RuntimeFilterConsumerSet),
    Compiled {
        state: RuntimeFilterConsumerState,
        keys: CompiledRuntimeFilterKeys,
    },
}
impl RuntimeFilterProcessorConsumers {
    fn state(&self) -> &RuntimeFilterConsumerState {
        match self {
            Self::Arena(value) => value.state(),
            Self::Compiled { state, .. } => state,
        }
    }
    fn apply_chunk_observed(
        &mut self,
        chunk: Chunk,
        event_sink: Option<&Arc<dyn FragmentEventSink>>,
        factory: Option<&dyn crate::exec::pipeline::operator::InputConstructionFactory>,
        control: &dyn crate::exec::expr::ExprEvaluationControl,
    ) -> ExecutionResult<Option<Chunk>> {
        match self {
            Self::Arena(value) => value.apply_chunk_created(
                chunk,
                factory
                    .ok_or(crate::exec::pipeline::operator::ConstructionFailure::MissingOwner)?,
                control,
                event_sink,
            ),
            Self::Compiled { state, keys } => state.apply_chunk_observed(chunk, keys, event_sink),
        }
    }
    fn bind_mem_tracker(&mut self, tracker: Arc<crate::runtime::mem_tracker::MemTracker>) {
        if let Self::Compiled { keys, .. } = self {
            keys.bind_mem_tracker(tracker);
        }
    }
    fn bind_runtime_state(&mut self, state: &RuntimeState) {
        if let Self::Compiled { keys, .. } = self {
            keys.bind_runtime_state(state);
        }
    }
}

pub(crate) struct NativeRuntimeFilterProcessorFactory {
    name: String,
    consumers: RuntimeFilterProcessorPlan,
}

impl NativeRuntimeFilterProcessorFactory {
    pub(crate) fn new(
        owner_node_id: i32,
        specs: &[RuntimeFilterConsumerBinding],
        arena: Arc<ExprArena>,
    ) -> Result<Self, String> {
        Ok(Self {
            name: format!("NativeRuntimeFilter (id={owner_node_id})"),
            consumers: RuntimeFilterProcessorPlan::Arena(RuntimeFilterConsumerSet::from_plan(
                "Join", specs, arena,
            )?),
        })
    }
}

impl NativeRuntimeFilterProcessorFactory {
    pub(crate) fn new_compiled(
        owner_node_id: i32,
        consumers: Arc<CompiledRuntimeFilterConsumers>,
    ) -> Self {
        Self {
            name: format!("CompiledRuntimeFilter (id={owner_node_id})"),
            consumers: RuntimeFilterProcessorPlan::Compiled(consumers),
        }
    }
}

impl OperatorFactory for NativeRuntimeFilterProcessorFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(NativeRuntimeFilterProcessor {
            name: self.name.clone(),
            consumers: match &self.consumers {
                RuntimeFilterProcessorPlan::Arena(value) => {
                    RuntimeFilterProcessorConsumers::Arena(value.clone())
                }
                RuntimeFilterProcessorPlan::Compiled(value) => {
                    RuntimeFilterProcessorConsumers::Compiled {
                        state: value.state().clone(),
                        keys: value.driver_keys(),
                    }
                }
            },
            output: None,
            finishing: false,
            event_sink: Arc::new(NoopFragmentEventSink),
            construction: None,
        })
    }
}

struct NativeRuntimeFilterProcessor {
    name: String,
    consumers: RuntimeFilterProcessorConsumers,
    output: Option<Chunk>,
    finishing: bool,
    event_sink: Arc<dyn FragmentEventSink>,
    construction: Option<Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>>,
}

impl Operator for NativeRuntimeFilterProcessor {
    fn creates_typed_storage(&self) -> bool {
        true
    }
    fn set_construction_factory(
        &mut self,
        factory: Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>,
    ) {
        if let RuntimeFilterProcessorConsumers::Compiled { keys, .. } = &mut self.consumers {
            keys.bind_construction_factory(factory.clone());
        }
        self.construction = Some(factory);
    }
    fn set_mem_tracker(&mut self, tracker: Arc<crate::runtime::mem_tracker::MemTracker>) {
        self.consumers.bind_mem_tracker(tracker);
    }
    fn name(&self) -> &str {
        &self.name
    }

    fn set_fragment_event_sink(&mut self, event_sink: Arc<dyn FragmentEventSink>) {
        self.event_sink = event_sink;
    }

    fn bind_runtime_state(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        self.consumers.bind_runtime_state(state);
        Ok(())
    }

    fn activate(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        Ok(self.consumers.state().bind(state)?)
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

impl NativeRuntimeFilterProcessor {
    fn has_room(&self) -> bool {
        !self.finishing && self.output.is_none()
    }
}

impl ProcessorOperator for NativeRuntimeFilterProcessor {
    /// Held back only once a chunk reached a pending gate: until then the
    /// gate's wait has not started and must not hold the pipeline.
    fn need_input(&self) -> bool {
        self.has_room() && !self.consumers.state().gate_holds_input()
    }

    /// The gate's wait starts here, with a chunk actually on the edge. While
    /// the gate is pending the chunk stays on the edge.
    fn can_accept_input(&self, _chunk: &Chunk) -> ExecutionResult<bool> {
        Ok(
            self.has_room()
                && matches!(self.consumers.state().poll_gate(), RuntimeFilterGate::Open),
        )
    }

    fn has_output(&self) -> bool {
        self.output.is_some()
    }

    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> ExecutionResult<()> {
        if !self.has_room() {
            return Err("native runtime-filter processor cannot accept input".into());
        }
        if !matches!(self.consumers.state().poll_gate(), RuntimeFilterGate::Open) {
            return Err(
                "native runtime-filter processor received input before its gate opened".into(),
            );
        }
        self.output = self.consumers.apply_chunk_observed(
            chunk,
            Some(&self.event_sink),
            self.construction.as_deref(),
            &super::compiled_expression::RuntimeKernelControl::new(_state.error_state()),
        )?;
        Ok(())
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> ExecutionResult<Option<Chunk>> {
        Ok(self.output.take())
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> ExecutionResult<()> {
        self.finishing = true;
        Ok(())
    }

    fn sink_observable(&self) -> Option<Arc<Observable>> {
        Some(self.consumers.state().gate_observable())
    }

    fn sink_block_deadline(&self) -> Option<DriverBlockDeadline> {
        self.consumers.state().gate_deadline()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use crate::runtime_filter::{
        self as execution, ConsumerActivation, RuntimeFilterArtifactQueryError,
        RuntimeFilterBindingId, RuntimeFilterChannelId, RuntimeFilterConsumerContract,
        RuntimeFilterExecutionContract, RuntimeFilterMembershipSchema, RuntimeFilterNullSemantics,
        RuntimeFilterScalarRef,
    };
    use arrow::array::{Array, Int32Array};
    use arrow::datatypes::DataType;
    use novarocks_spi::connector::ConnectorScalarValue;

    use super::*;

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
        outcome: execution::RuntimeFilterBindOutcome<execution::RuntimeFilterSubscriptionHandle>,
        calls: Arc<AtomicUsize>,
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
            self.calls.fetch_add(1, Ordering::SeqCst);
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
        )
    }

    #[test]
    fn consumer_plan_requires_the_execution_membership_contract() {
        let mut arena = ExprArena::default();
        let expr_id = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let consumers = RuntimeFilterConsumerSet::from_plan(
            "Join",
            &[membership_spec(expr_id)],
            Arc::new(arena),
        );
        assert!(consumers.is_ok());
    }

    #[test]
    fn processor_defers_subscription_until_activation() {
        let mut arena = ExprArena::default();
        let expr_id = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let factory = NativeRuntimeFilterProcessorFactory::new(
            7,
            &[membership_spec(expr_id)],
            Arc::new(arena),
        )
        .expect("processor factory");
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
    fn consumer_plan_rejects_missing_expression_coordinate() {
        let arena = Arc::new(ExprArena::default());
        let error = match RuntimeFilterConsumerSet::from_plan(
            "Join",
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
            RuntimeFilterExecutionContract::Ordered(order.into()),
        );
        let error = match NativeOrderedLiveConsumerSet::from_plan(
            &[RuntimeFilterConsumerBinding::new(expr_id, contract)],
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
            calls: Arc::new(AtomicUsize::new(0)),
            outcome: execution::RuntimeFilterBindOutcome::Bound(
                execution::RuntimeFilterSubscriptionHandle::Blocking(Arc::new(
                    PublishedSubscription::new(snapshot),
                )),
            ),
        });
        let consumers = RuntimeFilterConsumerSet::from_plan(
            "Join",
            &[membership_spec(expr_id)],
            Arc::new(arena),
        )
        .expect("consumer set");
        let state = RuntimeState::default().with_runtime_filter_session(Some(session));
        consumers.bind(&state).expect("bind");
        assert!(matches!(consumers.poll_gate(), RuntimeFilterGate::Open));
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
        let consumers = RuntimeFilterConsumerSet::from_plan(
            "Join",
            &[membership_spec(expr_id)],
            Arc::new(arena),
        )
        .expect("consumer set");
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
        let output = consumers
            .apply_chunk(input.clone())
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
                execution::RuntimeFilterSubscriptionHandle::Blocking(subscription),
            ),
        });
        RuntimeState::default().with_runtime_filter_session(Some(session))
    }

    fn one_binding_arena() -> (Arc<ExprArena>, crate::exec::expr::ExprId) {
        let mut arena = ExprArena::default();
        let expr_id = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        (Arc::new(arena), expr_id)
    }

    fn controlled_consumers(
        timeout: Duration,
    ) -> (RuntimeFilterConsumerSet, Arc<ControlledSubscription>) {
        let (arena, expr_id) = one_binding_arena();
        let subscription = ControlledSubscription::new();
        let consumers =
            RuntimeFilterConsumerSet::from_plan("Join", &[membership_spec(expr_id)], arena)
                .expect("consumer set");
        consumers
            .bind(&controlled_state(&subscription))
            .expect("bind");
        consumers.set_wait_timeout(timeout);
        (consumers, subscription)
    }

    #[test]
    fn created_keys_preserve_the_original_fragment_failure() {
        let (consumers, subscription) = controlled_consumers(Duration::from_secs(5));
        subscription.publish(accepting(2));
        assert!(matches!(consumers.poll_gate(), RuntimeFilterGate::Open));
        let creation_runtime = RuntimeState::default();
        let factory = actual_evaluation_factory(&creation_runtime);
        let runtime = RuntimeState::default();
        runtime
            .error_state()
            .set_error("runtime-filter fragment failed".to_string());
        let control =
            super::super::compiled_expression::RuntimeKernelControl::new(runtime.error_state());
        let error = consumers
            .apply_chunk_created(
                created_int_chunk(&[1, 2, 3]),
                factory.as_ref(),
                &control,
                None,
            )
            .unwrap_err();
        assert_eq!(
            error,
            crate::runtime::fragment::ExecutionFailure::from(
                "runtime-filter fragment failed".to_string()
            )
        );
    }

    fn created_int_chunk(values: &[i32]) -> Chunk {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("v", DataType::Int32, false),
        ]));
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

        let output = consumers
            .apply_chunk(int_chunk(&[1, 2, 3]))
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
        let output = consumers
            .apply_chunk(int_chunk(&[1, 2, 3]))
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

    impl Operator for QueueSource {
        fn name(&self) -> &str {
            "QUEUE_SOURCE"
        }

        fn is_finished(&self) -> bool {
            self.finished.load(std::sync::atomic::Ordering::Acquire)
                && self.chunks.lock().expect("chunks lock").is_empty()
        }

        fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
            Some(self)
        }

        fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
            Some(self)
        }
    }

    impl ProcessorOperator for QueueSource {
        fn need_input(&self) -> bool {
            false
        }

        fn has_output(&self) -> bool {
            !self.chunks.lock().expect("chunks lock").is_empty()
        }

        fn push_chunk(&mut self, _: &RuntimeState, _: Chunk) -> ExecutionResult<()> {
            Err("a source accepts no input".to_string().into())
        }

        fn pull_chunk(&mut self, _: &RuntimeState) -> ExecutionResult<Option<Chunk>> {
            Ok(self.chunks.lock().expect("chunks lock").pop_front())
        }

        fn set_finishing(&mut self, _: &RuntimeState) -> ExecutionResult<()> {
            Ok(())
        }

        fn source_observable(&self) -> Option<Arc<Observable>> {
            Some(Arc::clone(&self.observable))
        }
    }

    struct CollectSink {
        values: Arc<Mutex<Vec<i32>>>,
        finished: bool,
        observable: Arc<Observable>,
    }

    impl Operator for CollectSink {
        fn name(&self) -> &str {
            "COLLECT_SINK"
        }

        fn is_finished(&self) -> bool {
            self.finished
        }

        fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
            Some(self)
        }

        fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
            Some(self)
        }
    }

    impl ProcessorOperator for CollectSink {
        fn need_input(&self) -> bool {
            !self.finished
        }

        fn has_output(&self) -> bool {
            false
        }

        fn push_chunk(&mut self, _: &RuntimeState, chunk: Chunk) -> ExecutionResult<()> {
            self.values
                .lock()
                .expect("values lock")
                .extend(int_values(&chunk));
            Ok(())
        }

        fn pull_chunk(&mut self, _: &RuntimeState) -> ExecutionResult<Option<Chunk>> {
            Ok(None)
        }

        fn set_finishing(&mut self, _: &RuntimeState) -> ExecutionResult<()> {
            self.finished = true;
            Ok(())
        }

        fn sink_observable(&self) -> Option<Arc<Observable>> {
            Some(Arc::clone(&self.observable))
        }
    }

    struct GatedPipeline {
        driver: crate::exec::pipeline::driver::PipelineDriver,
        chunks: Arc<Mutex<std::collections::VecDeque<Chunk>>>,
        finished: Arc<std::sync::atomic::AtomicBool>,
        source: Arc<Observable>,
        values: Arc<Mutex<Vec<i32>>>,
        subscription: Arc<ControlledSubscription>,
        gate: Arc<Observable>,
    }

    fn gated_pipeline(timeout: Duration) -> GatedPipeline {
        let (arena, expr_id) = one_binding_arena();
        let factory =
            NativeRuntimeFilterProcessorFactory::new(5, &[membership_spec(expr_id)], arena)
                .expect("processor factory");
        let gate = factory.consumers.gate_observable();
        let subscription = ControlledSubscription::new();
        let session = controlled_state(&subscription)
            .runtime_filter_session()
            .cloned();
        let state = Arc::new(
            RuntimeState::new(
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
                Box::new(QueueSource {
                    chunks: Arc::clone(&chunks),
                    finished: Arc::clone(&finished),
                    observable: Arc::clone(&source),
                }),
                processor,
                Box::new(CollectSink {
                    values: Arc::clone(&values),
                    finished: false,
                    observable: Arc::new(Observable::new()),
                }),
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
            gate,
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
            .push_back(created_int_chunk(&[1, 2, 3]));
        pipeline.source.notify_observers();

        let arrived = Instant::now();
        assert!(matches!(
            pipeline.driver.process(Duration::from_millis(10)),
            DriverState::Blocked(BlockedReason::OutputFull)
        ));
        let (observable, _, deadline) = pipeline
            .driver
            .blocked_observable_snapshot()
            .expect("parked on the gate");
        assert!(Arc::ptr_eq(&observable, &pipeline.gate));
        let deadline = deadline.expect("a gate wait is bounded");
        assert!(deadline.at() >= arrived + Duration::from_secs(5));
        assert!(pipeline.values.lock().expect("values lock").is_empty());

        pipeline.subscription.publish(accepting(2));
        assert!(matches!(
            pipeline.driver.process(Duration::from_millis(10)),
            DriverState::Blocked(BlockedReason::InputEmpty)
        ));
        assert_eq!(*pipeline.values.lock().expect("values lock"), vec![2]);

        pipeline
            .finished
            .store(true, std::sync::atomic::Ordering::Release);
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
