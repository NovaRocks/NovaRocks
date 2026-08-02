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

use crate::common::ids::SlotId;
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprNode};
use crate::exec::node::runtime_filter::{
    RuntimeFilterConsumerBinding, RuntimeFilterExecutionContract, RuntimeFilterExecutionReduction,
};
use crate::exec::node::scan::ScanMorselPruneDecision;
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::profile::{
    OperatorProfiles, ProfileUnit, RUNTIME_FILTER_INPUT_ROWS, RUNTIME_FILTER_OUTPUT_ROWS,
};
use crate::runtime::runtime_state::RuntimeState;
use crate::runtime_filter::exec::execution_predicate::NativeExecutionPredicate;
use crate::runtime_filter::exec::membership_predicate::MembershipPredicateContract;
#[cfg(test)]
use crate::runtime_filter::exec::membership_predicate::{
    NativeRuntimeFilterPredicate, PredicateEvaluationError,
};
use crate::runtime_filter::exec::ordered_range_predicate::{
    NativeOrderedRangePredicate, OrderedRangePredicateContract,
};
#[cfg(test)]
use crate::runtime_filter::exec::ordered_range_predicate::{
    OrderedPredicateCompileError, OrderedPredicateEvaluationError,
};
use crate::runtime_filter::model::contract::{
    ArtifactCapability, ChannelId, ComparatorDigest, ConsumerActivation, LateApplyGranularity,
    OrderContract, OrderKeyContract,
};
#[cfg(test)]
use crate::runtime_filter::model::contract::{
    BindingId, ReductionRequirement, RuntimeFilterLifecycle,
};
use crate::runtime_filter::port::artifact::ArtifactMembershipSchema;
#[cfg(test)]
use crate::runtime_filter::port::artifact::{ArtifactKind, ConsumerArtifactProfile};
use crate::runtime_filter::port::identity::LogicalVersion;
use crate::runtime_filter::port::ordered_bound::RuntimeOrderContract;
#[cfg(test)]
use crate::runtime_filter::port::producer::RuntimeContractViolationKind;
#[cfg(test)]
use crate::runtime_filter::port::subscription::{
    ArtifactAcquireOutcome, BlockingSnapshotSubscription, LivePollOutcome, LiveTerminal,
    NonBlockingLiveSubscription, SubscriptionKind,
};
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
    #[cfg(test)]
    TestLive {
        subscription: Arc<dyn NonBlockingLiveSubscription>,
        last_seen: Option<LogicalVersion>,
        latest_predicate: Option<Arc<NativeOrderedRangePredicate>>,
        terminal: Option<LiveTerminal>,
    },
    PassThrough,
}

enum NativeOrderedPredicateForApply {
    Execution(Arc<execution::RuntimeFilterSnapshot>),
    #[cfg(test)]
    Test(Arc<NativeOrderedRangePredicate>),
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

    #[cfg(test)]
    pub(crate) fn from_bound_for_test(
        specs: Vec<RuntimeFilterConsumerBinding>,
        arena: Arc<ExprArena>,
        subscriptions: Vec<Arc<dyn NonBlockingLiveSubscription>>,
    ) -> Self {
        validate_ordered_live_plan_specs(&specs, &arena).unwrap();
        assert_eq!(specs.len(), subscriptions.len());
        let bindings = specs
            .into_iter()
            .zip(subscriptions)
            .map(|(spec, subscription)| NativeOrderedLiveBinding {
                spec,
                state: NativeOrderedLiveBindingState::TestLive {
                    subscription,
                    last_seen: None,
                    latest_predicate: None,
                    terminal: None,
                },
            })
            .collect();
        Self {
            inner: Arc::new(NativeOrderedLiveConsumerInner {
                arena,
                bindings: Mutex::new(bindings),
            }),
        }
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
                        binding.spec.binding_id
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

    pub(crate) fn poll_and_prune_morsel(
        &self,
        mut prune: impl FnMut(
            SlotId,
            &NativeOrderedRangePredicate,
        ) -> Result<ScanMorselPruneDecision, String>,
    ) -> Result<ScanMorselPruneDecision, String> {
        self.poll()?;
        let active = {
            let bindings = self
                .inner
                .bindings
                .lock()
                .expect("native ordered RF consumer lock");
            bindings
                .iter()
                .filter_map(|binding| {
                    let ConsumerActivation::NonBlockingLive {
                        late_apply: LateApplyGranularity::Split,
                    } = binding.spec.activation
                    else {
                        return None;
                    };
                    let predicate = match &binding.state {
                        NativeOrderedLiveBindingState::BoundExecutionLive {
                            latest_snapshot: Some(snapshot),
                            ..
                        } => snapshot
                            .predicate()
                            .as_any()
                            .downcast_ref::<NativeExecutionPredicate>()
                            .and_then(NativeExecutionPredicate::ordered_range)
                            .cloned(),
                        #[cfg(test)]
                        NativeOrderedLiveBindingState::TestLive {
                            latest_predicate: Some(predicate),
                            ..
                        } => Some(Arc::clone(predicate)),
                        _ => None,
                    }?;
                    let Some(ExprNode::SlotId(slot_id)) =
                        self.inner.arena.node(binding.spec.expr_id)
                    else {
                        return None;
                    };
                    Some((*slot_id, predicate))
                })
                .collect::<Vec<_>>()
        };
        for (slot_id, predicate) in active {
            if prune(slot_id, predicate.as_ref())? == ScanMorselPruneDecision::Skip {
                return Ok(ScanMorselPruneDecision::Skip);
            }
        }
        Ok(ScanMorselPruneDecision::Keep)
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
        let configured = !self
            .inner
            .bindings
            .lock()
            .expect("native ordered RF consumer lock")
            .is_empty();
        let input_rows = i64::try_from(chunk.len()).unwrap_or(i64::MAX);
        let output = self.apply_chunk_inner(chunk)?;
        if configured && let Some(profiles) = profiles {
            profiles
                .common
                .counter_add(RUNTIME_FILTER_INPUT_ROWS, ProfileUnit::Unit, input_rows);
            profiles.common.counter_add(
                RUNTIME_FILTER_OUTPUT_ROWS,
                ProfileUnit::Unit,
                output
                    .as_ref()
                    .map_or(0, |chunk| i64::try_from(chunk.len()).unwrap_or(i64::MAX)),
            );
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
        #[cfg(test)]
        {
            let pending = {
                let bindings = self
                    .inner
                    .bindings
                    .lock()
                    .expect("native ordered RF consumer lock");
                bindings
                    .iter()
                    .enumerate()
                    .filter_map(|(index, binding)| match &binding.state {
                        NativeOrderedLiveBindingState::TestLive {
                            subscription,
                            last_seen,
                            terminal: None,
                            ..
                        } => Some((
                            index,
                            binding.spec.clone(),
                            Arc::clone(subscription),
                            *last_seen,
                        )),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
            };
            for (index, spec, subscription, observed) in pending {
                self.apply_test_poll_outcome(index, &spec, subscription.poll_after(observed))?;
            }
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
                    let Some(predicate) = snapshot
                        .predicate()
                        .as_any()
                        .downcast_ref::<NativeExecutionPredicate>()
                    else {
                        return Err(format!(
                            "native ordered runtime-filter binding_id={} execution snapshot has no ordered predicate",
                            spec.binding_id
                        ));
                    };
                    if predicate.ordered_range().is_none() {
                        return Err(format!(
                            "native ordered runtime-filter binding_id={} execution snapshot has the wrong predicate kind",
                            spec.binding_id
                        ));
                    }
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
                        spec.binding_id
                    ));
                }
                if latest_version.is_some_and(|latest| observed.is_some_and(|seen| latest < seen)) {
                    return Err(format!(
                        "native ordered runtime-filter binding_id={} live cursor regressed",
                        spec.binding_id
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

    #[cfg(test)]
    fn apply_test_poll_outcome(
        &self,
        index: usize,
        spec: &RuntimeFilterConsumerBinding,
        outcome: LivePollOutcome,
    ) -> Result<(), String> {
        let mut bindings = self
            .inner
            .bindings
            .lock()
            .expect("native ordered RF consumer lock");
        let Some(binding) = bindings.get_mut(index) else {
            return Err("native ordered runtime-filter binding index drifted".into());
        };
        let NativeOrderedLiveBindingState::TestLive {
            last_seen,
            latest_predicate,
            terminal,
            ..
        } = &mut binding.state
        else {
            return Ok(());
        };
        match outcome {
            LivePollOutcome::Updated {
                bundle,
                terminal: update_terminal,
            } => {
                if last_seen.is_none_or(|seen| bundle.version() > seen) {
                    let expected = ordered_predicate_contract_with_version(spec, bundle.version())?;
                    match NativeOrderedRangePredicate::compile(&bundle, &expected) {
                        Ok(predicate) => {
                            *last_seen = Some(bundle.version());
                            *latest_predicate = Some(Arc::new(predicate));
                        }
                        Err(OrderedPredicateCompileError::ResourceUnavailable) => {
                            if latest_predicate.is_none() {
                                binding.state = NativeOrderedLiveBindingState::PassThrough;
                                return Ok(());
                            }
                        }
                        Err(error) => return Err(error.to_string()),
                    }
                }
                if let Some(update_terminal) = update_terminal {
                    if latest_predicate.is_none() {
                        binding.state = NativeOrderedLiveBindingState::PassThrough;
                    } else {
                        *terminal = Some(update_terminal);
                    }
                }
            }
            LivePollOutcome::Idle {
                latest_version,
                terminal: idle_terminal,
            } => {
                if latest_version.is_some_and(|latest| last_seen.is_none_or(|seen| latest > seen)) {
                    return Err(format!(
                        "native ordered runtime-filter binding_id={} reported a newer live version without artifact",
                        spec.binding_id
                    ));
                }
                if latest_version.is_some_and(|latest| last_seen.is_some_and(|seen| latest < seen))
                {
                    return Err(format!(
                        "native ordered runtime-filter binding_id={} live cursor regressed",
                        spec.binding_id
                    ));
                }
                if let Some(idle_terminal) = idle_terminal {
                    if latest_predicate.is_none() {
                        binding.state = NativeOrderedLiveBindingState::PassThrough;
                    } else {
                        *terminal = Some(idle_terminal);
                    }
                }
            }
        }
        Ok(())
    }

    fn apply_chunk_inner(&self, chunk: Chunk) -> Result<Option<Chunk>, String> {
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
                    #[cfg(test)]
                    NativeOrderedLiveBindingState::TestLive {
                        latest_predicate: Some(predicate),
                        ..
                    } => Some((
                        binding.spec.expr_id,
                        NativeOrderedPredicateForApply::Test(Arc::clone(predicate)),
                    )),
                    _ => None,
                })
                .collect::<Vec<_>>()
        };
        if active.is_empty() {
            return Ok(Some(chunk));
        }
        let chunk = crate::exec::chunk::hydrate_dictionary_columns_except(&chunk, |_, _| false)?;
        let mut current = Some(chunk);
        for (expr_id, predicate) in active {
            let Some(input) = current else {
                return Ok(None);
            };
            let array = self.inner.arena.eval(expr_id, &input)?;
            let mask = match predicate {
                NativeOrderedPredicateForApply::Execution(snapshot) => snapshot
                    .predicate()
                    .evaluate(&array)
                    .map_err(|error| error.to_string())?,
                #[cfg(test)]
                NativeOrderedPredicateForApply::Test(predicate) => {
                    match predicate.evaluate(array.as_ref()) {
                        Ok(mask) => mask,
                        Err(OrderedPredicateEvaluationError::ResourceUnavailable) => {
                            current = Some(input);
                            continue;
                        }
                        Err(error) => return Err(error.to_string()),
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
        Ok(current)
    }

    #[cfg(test)]
    fn last_seen_for_test(&self) -> Option<LogicalVersion> {
        self.inner
            .bindings
            .lock()
            .expect("native ordered RF consumer lock")
            .first()
            .and_then(|binding| match &binding.state {
                NativeOrderedLiveBindingState::TestLive { last_seen, .. } => *last_seen,
                _ => None,
            })
    }

    #[cfg(test)]
    fn terminal_for_test(&self) -> Option<LiveTerminal> {
        self.inner
            .bindings
            .lock()
            .expect("native ordered RF consumer lock")
            .first()
            .and_then(|binding| match &binding.state {
                NativeOrderedLiveBindingState::TestLive { terminal, .. } => *terminal,
                _ => None,
            })
    }

    #[cfg(test)]
    fn is_live_for_test(&self) -> bool {
        self.inner
            .bindings
            .lock()
            .expect("native ordered RF consumer lock")
            .iter()
            .all(|binding| {
                matches!(
                    binding.state,
                    NativeOrderedLiveBindingState::TestLive { .. }
                )
            })
    }

    #[cfg(test)]
    fn is_execution_live_for_test(&self) -> bool {
        self.inner
            .bindings
            .lock()
            .expect("native ordered RF consumer lock")
            .iter()
            .all(|binding| {
                matches!(
                    binding.state,
                    NativeOrderedLiveBindingState::BoundExecutionLive { .. }
                )
            })
    }

    #[cfg(test)]
    fn is_pass_through_for_test(&self) -> bool {
        self.inner
            .bindings
            .lock()
            .expect("native ordered RF consumer lock")
            .iter()
            .all(|binding| matches!(binding.state, NativeOrderedLiveBindingState::PassThrough))
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
    #[cfg(test)]
    TestBoundBlocking(Arc<dyn BlockingSnapshotSubscription>),
    #[cfg(test)]
    TestBoundLive {
        subscription: Arc<dyn NonBlockingLiveSubscription>,
        observed: Option<LogicalVersion>,
    },
}

enum NativeConsumerPredicate {
    Execution(Arc<execution::RuntimeFilterSnapshot>),
    #[cfg(test)]
    Test(NativeRuntimeFilterPredicate),
}

enum NativeConsumerPredicateForApply {
    Execution(Arc<execution::RuntimeFilterSnapshot>),
    #[cfg(test)]
    Test(NativeRuntimeFilterPredicate),
}

impl NativeConsumerPredicate {
    fn clone_for_apply(&self) -> NativeConsumerPredicateForApply {
        match self {
            Self::Execution(snapshot) => {
                NativeConsumerPredicateForApply::Execution(Arc::clone(snapshot))
            }
            #[cfg(test)]
            Self::Test(predicate) => NativeConsumerPredicateForApply::Test(predicate.clone()),
        }
    }
}

#[cfg(test)]
enum NativeConsumerTestSubscription {
    Blocking(Arc<dyn BlockingSnapshotSubscription>),
    Live(Arc<dyn NonBlockingLiveSubscription>),
}

impl RuntimeFilterConsumerSet {
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

    #[cfg(test)]
    pub(crate) fn from_bound_for_test(
        specs: Vec<RuntimeFilterConsumerBinding>,
        arena: Arc<ExprArena>,
        subscriptions: Vec<Arc<dyn BlockingSnapshotSubscription>>,
    ) -> Self {
        validate_plan_specs(&specs, &arena).unwrap();
        assert_eq!(specs.len(), subscriptions.len());
        let bindings = specs
            .into_iter()
            .zip(subscriptions)
            .map(|(spec, subscription)| NativeConsumerBinding {
                spec,
                state: NativeConsumerBindingState::TestBoundBlocking(subscription),
            })
            .collect();
        Self {
            inner: Arc::new(NativeConsumerInner {
                arena,
                bindings: Mutex::new(bindings),
                acquire_phase: Mutex::new(NativeConsumerAcquirePhase::Pending),
                acquire_ready: Condvar::new(),
                wait_timeout: Mutex::new(Duration::from_secs(1)),
            }),
        }
    }

    #[cfg(test)]
    pub(crate) fn from_live_bound_for_test(
        specs: Vec<RuntimeFilterConsumerBinding>,
        arena: Arc<ExprArena>,
        subscriptions: Vec<Arc<dyn NonBlockingLiveSubscription>>,
    ) -> Self {
        validate_plan_specs(&specs, &arena).unwrap();
        assert_eq!(specs.len(), subscriptions.len());
        let bindings = specs
            .into_iter()
            .zip(subscriptions)
            .map(|(spec, subscription)| NativeConsumerBinding {
                spec,
                state: NativeConsumerBindingState::TestBoundLive {
                    subscription,
                    observed: None,
                },
            })
            .collect();
        Self {
            inner: Arc::new(NativeConsumerInner {
                arena,
                bindings: Mutex::new(bindings),
                acquire_phase: Mutex::new(NativeConsumerAcquirePhase::Pending),
                acquire_ready: Condvar::new(),
                wait_timeout: Mutex::new(Duration::from_secs(1)),
            }),
        }
    }

    #[cfg(test)]
    fn from_execution_live_bound_for_test(
        specs: Vec<RuntimeFilterConsumerBinding>,
        arena: Arc<ExprArena>,
        subscriptions: Vec<Arc<dyn execution::NonBlockingLiveSubscription>>,
    ) -> Self {
        validate_plan_specs(&specs, &arena).unwrap();
        assert_eq!(specs.len(), subscriptions.len());
        let bindings = specs
            .into_iter()
            .zip(subscriptions)
            .map(|(spec, subscription)| NativeConsumerBinding {
                spec,
                state: NativeConsumerBindingState::BoundLive {
                    subscription,
                    observed: None,
                },
            })
            .collect();
        Self {
            inner: Arc::new(NativeConsumerInner {
                arena,
                bindings: Mutex::new(bindings),
                acquire_phase: Mutex::new(NativeConsumerAcquirePhase::Pending),
                acquire_ready: Condvar::new(),
                wait_timeout: Mutex::new(Duration::from_secs(1)),
            }),
        }
    }

    #[cfg(test)]
    fn from_mixed_bound_for_test(
        specs: Vec<RuntimeFilterConsumerBinding>,
        arena: Arc<ExprArena>,
        subscriptions: Vec<NativeConsumerTestSubscription>,
    ) -> Self {
        validate_plan_specs(&specs, &arena).unwrap();
        assert_eq!(specs.len(), subscriptions.len());
        let bindings = specs
            .into_iter()
            .zip(subscriptions)
            .map(|(spec, subscription)| NativeConsumerBinding {
                spec,
                state: match subscription {
                    NativeConsumerTestSubscription::Blocking(subscription) => {
                        NativeConsumerBindingState::TestBoundBlocking(subscription)
                    }
                    NativeConsumerTestSubscription::Live(subscription) => {
                        NativeConsumerBindingState::TestBoundLive {
                            subscription,
                            observed: None,
                        }
                    }
                },
            })
            .collect();
        Self {
            inner: Arc::new(NativeConsumerInner {
                arena,
                bindings: Mutex::new(bindings),
                acquire_phase: Mutex::new(NativeConsumerAcquirePhase::Pending),
                acquire_ready: Condvar::new(),
                wait_timeout: Mutex::new(Duration::from_secs(1)),
            }),
        }
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
                    binding.spec.activation,
                    ConsumerActivation::BlockingSnapshot
                ) =>
                {
                    binding.state = NativeConsumerBindingState::BoundBlocking(subscription);
                }
                Ok(execution::RuntimeFilterBindOutcome::Bound(
                    execution::RuntimeFilterSubscriptionHandle::Live(subscription),
                )) if matches!(
                    binding.spec.activation,
                    ConsumerActivation::NonBlockingLive {
                        late_apply: LateApplyGranularity::Batch,
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
                        binding.spec.binding_id
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
            match binding.spec.activation {
                ConsumerActivation::BlockingSnapshot
                | ConsumerActivation::NonBlockingLive {
                    late_apply: LateApplyGranularity::Batch,
                } => {}
                ConsumerActivation::NonBlockingLive { .. } => {
                    return Err(format!(
                        "native Join runtime-filter binding_id={} has unsupported activation",
                        binding.spec.binding_id
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
                        #[cfg(test)]
                        NativeConsumerBindingState::TestBoundBlocking(subscription) => {
                            binding.state =
                                NativeConsumerBindingState::TestBoundBlocking(subscription);
                            None
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
        #[cfg(test)]
        {
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
                            NativeConsumerBindingState::TestBoundBlocking(subscription) => {
                                Some((index, binding.spec.clone(), subscription))
                            }
                            state => {
                                binding.state = state;
                                None
                            }
                        }
                    })
                    .collect::<Vec<_>>()
            };
            for (index, spec, subscription) in pending {
                let remaining = deadline.saturating_duration_since(Instant::now());
                let state = match subscription.acquire(remaining) {
                    ArtifactAcquireOutcome::Published(bundle) => {
                        let contract = membership_predicate_contract(&spec)?;
                        NativeConsumerBindingState::Active(NativeConsumerPredicate::Test(
                            NativeRuntimeFilterPredicate::compile(&bundle, &contract)
                                .map_err(|error| error.to_string())?,
                        ))
                    }
                    ArtifactAcquireOutcome::Unsupported(_)
                    | ArtifactAcquireOutcome::Unavailable(_)
                    | ArtifactAcquireOutcome::Cancelled
                    | ArtifactAcquireOutcome::TimedOut => NativeConsumerBindingState::PassThrough,
                };
                self.inner.bindings.lock().expect("native RF consumer lock")[index].state = state;
            }
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
        let configured = !self
            .inner
            .bindings
            .lock()
            .expect("native RF consumer lock")
            .is_empty();
        let input_rows = i64::try_from(chunk.len()).unwrap_or(i64::MAX);
        let output = self.apply_chunk_inner(chunk)?;
        if configured && let Some(profiles) = profiles {
            profiles
                .common
                .counter_add(RUNTIME_FILTER_INPUT_ROWS, ProfileUnit::Unit, input_rows);
            profiles.common.counter_add(
                RUNTIME_FILTER_OUTPUT_ROWS,
                ProfileUnit::Unit,
                output
                    .as_ref()
                    .map_or(0, |chunk| i64::try_from(chunk.len()).unwrap_or(i64::MAX)),
            );
        }
        Ok(output)
    }

    fn apply_chunk_inner(&self, chunk: Chunk) -> Result<Option<Chunk>, String> {
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
                #[cfg(test)]
                let unacquired = unacquired
                    || matches!(
                        binding.state,
                        NativeConsumerBindingState::TestBoundBlocking(_)
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
            return Ok(Some(chunk));
        }
        let chunk = crate::exec::chunk::hydrate_dictionary_columns_except(&chunk, |_, _| false)?;
        let mut current = Some(chunk);
        for (index, expr_id, predicate) in active {
            let Some(input) = current else {
                return Ok(None);
            };
            let array = self.inner.arena.eval(expr_id, &input)?;
            let mask = match predicate {
                NativeConsumerPredicateForApply::Execution(snapshot) => snapshot
                    .predicate()
                    .evaluate(&array)
                    .map_err(|error| error.to_string())?,
                #[cfg(test)]
                NativeConsumerPredicateForApply::Test(predicate) => {
                    match predicate.evaluate(array.as_ref()) {
                        Ok(mask) => mask,
                        Err(PredicateEvaluationError::ResourceUnavailable) => {
                            self.inner.bindings.lock().expect("native RF consumer lock")[index]
                                .state = NativeConsumerBindingState::PassThrough;
                            current = Some(input);
                            continue;
                        }
                        Err(error) => return Err(error.to_string()),
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
        Ok(current)
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
        #[cfg(test)]
        {
            let pending = {
                let bindings = self.inner.bindings.lock().expect("native RF consumer lock");
                bindings
                    .iter()
                    .enumerate()
                    .filter_map(|(index, binding)| match &binding.state {
                        NativeConsumerBindingState::TestBoundLive {
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
                self.apply_test_live_poll_outcome(index, &spec, subscription.poll_after(observed))?;
            }
        }
        Ok(())
    }

    #[cfg(test)]
    fn apply_test_live_poll_outcome(
        &self,
        index: usize,
        spec: &RuntimeFilterConsumerBinding,
        outcome: LivePollOutcome,
    ) -> Result<(), String> {
        let mut bindings = self.inner.bindings.lock().expect("native RF consumer lock");
        let Some(binding) = bindings.get_mut(index) else {
            return Err("native Join runtime-filter binding index drifted".into());
        };
        let NativeConsumerBindingState::TestBoundLive { observed, .. } = &mut binding.state else {
            return Ok(());
        };
        let observed_version = *observed;
        if observed_version.is_some_and(|version| version != LogicalVersion::FIRST) {
            return Err(format!(
                "native Join CompleteOnce runtime-filter binding_id={} private cursor must use LogicalVersion::FIRST",
                spec.binding_id
            ));
        }
        match outcome {
            LivePollOutcome::Updated { bundle, terminal } => {
                if bundle.version() != LogicalVersion::FIRST
                    || terminal != Some(LiveTerminal::Completed)
                {
                    return Err(format!(
                        "native Join CompleteOnce runtime-filter binding_id={} returned an invalid terminal update",
                        spec.binding_id
                    ));
                }
                let contract = membership_predicate_contract(spec)?;
                binding.state = NativeConsumerBindingState::Active(NativeConsumerPredicate::Test(
                    NativeRuntimeFilterPredicate::compile(&bundle, &contract)
                        .map_err(|error| error.to_string())?,
                ));
            }
            LivePollOutcome::Idle {
                latest_version,
                terminal,
            } => {
                if latest_version.is_some_and(|version| version != LogicalVersion::FIRST) {
                    return Err(format!(
                        "native Join CompleteOnce runtime-filter binding_id={} Idle latest version is invalid",
                        spec.binding_id
                    ));
                }
                if terminal == Some(LiveTerminal::Completed) {
                    return Err(format!(
                        "native Join CompleteOnce runtime-filter binding_id={} completed without final artifact",
                        spec.binding_id
                    ));
                }
                if matches!(
                    (observed_version, latest_version),
                    (None, Some(_)) | (Some(_), None)
                ) {
                    return Err(format!(
                        "native Join CompleteOnce runtime-filter binding_id={} private cursor drifted",
                        spec.binding_id
                    ));
                }
                if terminal.is_some() {
                    binding.state = NativeConsumerBindingState::PassThrough;
                }
            }
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
                spec.binding_id
            ));
        }
        match outcome {
            execution::LivePollOutcome::Updated { snapshot, terminal } => {
                if snapshot.logical_version() != execution::LogicalVersion::FIRST {
                    return Err(format!(
                        "native Join CompleteOnce runtime-filter binding_id={} Updated artifact must use LogicalVersion::FIRST",
                        spec.binding_id
                    ));
                }
                if terminal != Some(execution::LiveTerminal::Completed) {
                    return Err(format!(
                        "native Join CompleteOnce runtime-filter binding_id={} Updated artifact requires terminal Completed, got {terminal:?}",
                        spec.binding_id
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
                        spec.binding_id
                    ));
                }
                if terminal == Some(execution::LiveTerminal::Completed) {
                    return Err(format!(
                        "native Join CompleteOnce runtime-filter binding_id={} reported Completed without the final artifact",
                        spec.binding_id
                    ));
                }
                match (observed_version, latest_version) {
                    (None, Some(_)) => {
                        return Err(format!(
                            "native Join CompleteOnce runtime-filter binding_id={} Idle cursor advanced without returning an artifact",
                            spec.binding_id
                        ));
                    }
                    (Some(_), None) => {
                        return Err(format!(
                            "native Join CompleteOnce runtime-filter binding_id={} Idle cursor regressed from LogicalVersion::FIRST",
                            spec.binding_id
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

#[cfg(test)]
fn join_profile() -> Result<ConsumerArtifactProfile, String> {
    ConsumerArtifactProfile::new(
        BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
        None,
    )
    .map_err(|error| format!("invalid native Join runtime-filter profile: {error:?}"))
}

fn execution_membership_consumer_contract(
    spec: &RuntimeFilterConsumerBinding,
) -> Result<execution::RuntimeFilterConsumerContract, String> {
    let RuntimeFilterExecutionContract::Membership {
        canonical_schema,
        schema_digest,
    } = &spec.contract
    else {
        return Err(format!(
            "native Join runtime-filter binding_id={} requires a Membership contract",
            spec.binding_id
        ));
    };
    let activation = match spec.activation {
        ConsumerActivation::BlockingSnapshot => execution::ConsumerActivation::BlockingSnapshot,
        ConsumerActivation::NonBlockingLive {
            late_apply: LateApplyGranularity::Batch,
        } => execution::ConsumerActivation::NonBlockingLive,
        ConsumerActivation::NonBlockingLive { .. } => {
            return Err(format!(
                "native Join runtime-filter binding_id={} has unsupported activation",
                spec.binding_id
            ));
        }
    };
    Ok(execution::RuntimeFilterConsumerContract::new(
        execution::RuntimeFilterBindingId::new(spec.binding_id),
        execution::RuntimeFilterChannelId::new(spec.channel_id),
        activation,
        execution::RuntimeFilterExecutionContract::Membership {
            canonical_schema: Arc::clone(canonical_schema),
            schema_digest: *schema_digest,
        },
    ))
}

fn execution_ordered_live_consumer_contract(
    spec: &RuntimeFilterConsumerBinding,
) -> Result<execution::RuntimeFilterConsumerContract, String> {
    let RuntimeFilterExecutionContract::Ordered {
        keys,
        comparator_digest,
        order_contract_digest,
    } = &spec.contract
    else {
        return Err(format!(
            "native ordered runtime-filter binding_id={} requires an Ordered contract",
            spec.binding_id
        ));
    };
    if !matches!(
        spec.activation,
        ConsumerActivation::NonBlockingLive {
            late_apply: LateApplyGranularity::Batch | LateApplyGranularity::Split,
        }
    ) {
        return Err(format!(
            "native ordered runtime-filter binding_id={} requires a non-blocking live activation",
            spec.binding_id
        ));
    }
    Ok(execution::RuntimeFilterConsumerContract::new(
        execution::RuntimeFilterBindingId::new(spec.binding_id),
        execution::RuntimeFilterChannelId::new(spec.channel_id),
        execution::ConsumerActivation::NonBlockingLive,
        spec.contract.clone(),
    ))
}

fn validate_unique_consumer_bindings(specs: &[RuntimeFilterConsumerBinding]) -> Result<(), String> {
    let mut bindings = BTreeSet::new();
    for spec in specs {
        if !bindings.insert(spec.binding_id) {
            return Err(format!(
                "duplicate native runtime-filter consumer binding_id={}",
                spec.binding_id
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
        if !spec.activation.is_blocking_or_batch_live() {
            return Err(format!(
                "native Join runtime-filter binding_id={} requires BlockingSnapshot or Batch NonBlockingLive",
                spec.binding_id
            ));
        }
        if spec.capabilities
            != BTreeSet::from([
                ArtifactCapability::Membership,
                ArtifactCapability::EmptyDomain,
            ])
        {
            return Err(format!(
                "native Join runtime-filter binding_id={} has an unsupported artifact capability profile",
                spec.binding_id
            ));
        }
        if spec.reduction != RuntimeFilterExecutionReduction::SetUnion {
            return Err(format!(
                "native Join runtime-filter binding_id={} requires SetUnion",
                spec.binding_id
            ));
        }
        membership_predicate_contract_with_arena(spec, arena)?;
    }
    Ok(())
}

fn validate_ordered_live_plan_specs(
    specs: &[RuntimeFilterConsumerBinding],
    arena: &ExprArena,
) -> Result<(), String> {
    validate_unique_consumer_bindings(specs)?;
    for spec in specs {
        match spec.activation {
            ConsumerActivation::NonBlockingLive {
                late_apply: LateApplyGranularity::Batch | LateApplyGranularity::Split,
            } => {}
            ConsumerActivation::NonBlockingLive { .. } => {
                return Err(format!(
                    "native ordered runtime-filter binding_id={} has unsupported late-apply granularity",
                    spec.binding_id
                ));
            }
            ConsumerActivation::BlockingSnapshot => {
                return Err(format!(
                    "native ordered runtime-filter binding_id={} requires NonBlockingLive",
                    spec.binding_id
                ));
            }
        }
        if spec.capabilities != BTreeSet::from([ArtifactCapability::OrderedRange]) {
            return Err(format!(
                "native ordered runtime-filter binding_id={} requires exactly OrderedRange capability",
                spec.binding_id
            ));
        }
        if spec.reduction != RuntimeFilterExecutionReduction::TightenOrderedBound {
            return Err(format!(
                "native ordered runtime-filter binding_id={} requires TightenOrderedBound",
                spec.binding_id
            ));
        }
        ordered_predicate_contract_with_arena(spec, arena, LogicalVersion::FIRST)?;
    }
    Ok(())
}

fn ordered_runtime_contract(
    spec: &RuntimeFilterConsumerBinding,
) -> Result<Arc<RuntimeOrderContract>, String> {
    let RuntimeFilterExecutionContract::Ordered {
        keys,
        comparator_digest,
        order_contract_digest,
    } = &spec.contract
    else {
        return Err(format!(
            "native ordered runtime-filter binding_id={} requires an Ordered contract",
            spec.binding_id
        ));
    };
    if keys.len() != 1 {
        return Err(format!(
            "native ordered runtime-filter binding_id={} requires exactly one order key",
            spec.binding_id
        ));
    }
    let plan = OrderContract {
        keys: crate::exec::node::runtime_filter::core_order_keys(keys)
            .iter()
            .map(|key| OrderKeyContract {
                data_type: key.data_type().clone(),
                direction: key.direction(),
                null_order: key.null_order(),
            })
            .collect(),
        inclusive: true,
        comparator_digest: ComparatorDigest::new(*comparator_digest),
    };
    let rebuilt = Arc::new(
        RuntimeOrderContract::try_from_plan(&plan).map_err(|error| {
            format!(
                "native ordered runtime-filter binding_id={} has invalid comparator contract: {error:?}",
                spec.binding_id
            )
        })?,
    );
    if rebuilt.keys() != crate::exec::node::runtime_filter::core_order_keys(keys).as_ref()
        || rebuilt.plan_comparator_digest().get() != *comparator_digest
        || rebuilt.digest().bytes() != *order_contract_digest
    {
        return Err(format!(
            "native ordered runtime-filter binding_id={} contract digest mismatch",
            spec.binding_id
        ));
    }
    Ok(rebuilt)
}

fn ordered_predicate_contract_with_arena(
    spec: &RuntimeFilterConsumerBinding,
    arena: &ExprArena,
    version: LogicalVersion,
) -> Result<OrderedRangePredicateContract, String> {
    let contract = ordered_runtime_contract(spec)?;
    let expression_type = arena.data_type(spec.expr_id).ok_or_else(|| {
        format!(
            "native ordered runtime-filter expression {:?} is missing",
            spec.expr_id
        )
    })?;
    if expression_type != contract.keys()[0].data_type() {
        return Err(format!(
            "native ordered runtime-filter binding_id={} expression type does not match its order key",
            spec.binding_id
        ));
    }
    OrderedRangePredicateContract::new(ChannelId::new(spec.channel_id), contract, version)
        .map_err(|error| format!("invalid native ordered predicate contract: {error:?}"))
}

#[cfg(test)]
fn ordered_predicate_contract_with_version(
    spec: &RuntimeFilterConsumerBinding,
    version: LogicalVersion,
) -> Result<OrderedRangePredicateContract, String> {
    let contract = ordered_runtime_contract(spec)?;
    OrderedRangePredicateContract::new(ChannelId::new(spec.channel_id), contract, version)
        .map_err(|error| format!("invalid native ordered predicate contract: {error:?}"))
}

fn membership_predicate_contract_with_arena(
    spec: &RuntimeFilterConsumerBinding,
    arena: &ExprArena,
) -> Result<MembershipPredicateContract, String> {
    let RuntimeFilterExecutionContract::Membership {
        canonical_schema,
        schema_digest,
    } = &spec.contract
    else {
        return Err(format!(
            "native Join runtime-filter binding_id={} requires a Membership contract",
            spec.binding_id
        ));
    };
    let view = ArtifactMembershipSchema::view(canonical_schema)
        .map_err(|error| format!("invalid native membership schema: {error:?}"))?;
    let data_type = arena
        .data_type(spec.expr_id)
        .ok_or_else(|| {
            format!(
                "native runtime-filter expression {:?} is missing",
                spec.expr_id
            )
        })?
        .clone();
    let rebuilt = ArtifactMembershipSchema::new(&data_type, view.null_semantics())
        .map_err(|error| format!("invalid native membership expression type: {error:?}"))?;
    if rebuilt.canonical_bytes() != canonical_schema.as_ref()
        || rebuilt.digest().bytes() != *schema_digest
    {
        return Err(format!(
            "native runtime-filter binding_id={} expression type/null contract does not match its canonical schema",
            spec.binding_id
        ));
    }
    MembershipPredicateContract::join(
        ChannelId::new(spec.channel_id),
        data_type,
        view.null_semantics(),
        LogicalVersion::FIRST,
    )
    .map_err(|error| format!("invalid native Join predicate contract: {error:?}"))
}

#[cfg(test)]
fn membership_predicate_contract(
    spec: &RuntimeFilterConsumerBinding,
) -> Result<MembershipPredicateContract, String> {
    let RuntimeFilterExecutionContract::Membership {
        canonical_schema, ..
    } = &spec.contract
    else {
        unreachable!("plan validation accepts only Membership")
    };
    let view = ArtifactMembershipSchema::view(canonical_schema)
        .map_err(|error| format!("invalid native membership schema: {error:?}"))?;
    let data_type = data_type_from_schema_view(view)?;
    MembershipPredicateContract::join(
        ChannelId::new(spec.channel_id),
        data_type,
        view.null_semantics(),
        LogicalVersion::FIRST,
    )
    .map_err(|error| format!("invalid native Join predicate contract: {error:?}"))
}

#[cfg(test)]
fn data_type_from_schema_view(
    view: crate::runtime_filter::port::artifact::ArtifactMembershipSchemaView<'_>,
) -> Result<arrow::datatypes::DataType, String> {
    use arrow::datatypes::{DataType, TimeUnit};
    Ok(match view.payload_tag() {
        1 => DataType::Boolean,
        2 => DataType::Int8,
        3 => DataType::Int16,
        4 => DataType::Int32,
        5 => DataType::Int64,
        6 => DataType::FixedSizeBinary(novarocks_types::largeint::LARGEINT_BYTE_WIDTH),
        7 => DataType::Float32,
        8 => DataType::Float64,
        9 => DataType::Utf8,
        10 => DataType::Date32,
        11 => {
            let (unit, timezone) = view
                .timestamp_contract()
                .ok_or_else(|| "missing timestamp membership contract".to_string())?;
            let unit = match unit {
                1 => TimeUnit::Second,
                2 => TimeUnit::Millisecond,
                3 => TimeUnit::Microsecond,
                4 => TimeUnit::Nanosecond,
                _ => return Err("invalid timestamp membership unit".into()),
            };
            DataType::Timestamp(unit, timezone.map(Arc::<str>::from))
        }
        12 => {
            let (precision, scale) = view
                .decimal_contract()
                .ok_or_else(|| "missing decimal membership contract".to_string())?;
            DataType::Decimal128(precision, scale)
        }
        _ => return Err("unsupported membership schema tag".into()),
    })
}

#[cfg(test)]
fn validate_resolved_consumer_activation(
    spec_activation: ConsumerActivation,
    resolved_activation: ConsumerActivation,
) -> Result<(), String> {
    if !spec_activation.is_blocking_or_batch_live() || spec_activation != resolved_activation {
        return Err(
            "native Join runtime-filter installed activation does not match the plan spec".into(),
        );
    }
    Ok(())
}

#[cfg(test)]
fn subscription_kind_for_activation(
    activation: ConsumerActivation,
) -> Result<SubscriptionKind, String> {
    match activation {
        ConsumerActivation::BlockingSnapshot => Ok(SubscriptionKind::BlockingSnapshot),
        ConsumerActivation::NonBlockingLive {
            late_apply: LateApplyGranularity::Batch,
        } => Ok(SubscriptionKind::NonBlockingLive),
        ConsumerActivation::NonBlockingLive { late_apply } => Err(format!(
            "native Join runtime-filter activation {late_apply:?} has no subscription kind"
        )),
    }
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
    use std::collections::{BTreeSet, VecDeque};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Condvar, Mutex, mpsc};
    use std::time::{Duration, Instant};

    use arrow::array::{Array, BooleanArray, Int32Array};
    use arrow::datatypes::DataType;
    use novarocks_execution::runtime_filter as execution;

    use super::{
        NativeConsumerBindingState, NativeConsumerTestSubscription, NativeExecutionPredicate,
        RuntimeFilterConsumerSet, membership_predicate_contract, subscription_kind_for_activation,
        validate_resolved_consumer_activation,
    };
    use crate::common::ids::SlotId;
    use crate::exec::expr::{ExprArena, ExprNode};
    use crate::exec::node::runtime_filter::{
        RuntimeFilterConsumerBinding, RuntimeFilterExecutionContract,
        RuntimeFilterExecutionReduction,
    };
    use crate::exec::operators::runtime_filter::tests_support::{
        chunk, membership_bundle, membership_bundle_with_version,
    };
    use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
    use crate::runtime::profile::{RUNTIME_FILTER_INPUT_ROWS, RUNTIME_FILTER_OUTPUT_ROWS};
    use crate::runtime_filter::exec::membership_predicate::NativeRuntimeFilterPredicate;
    use crate::runtime_filter::model::contract::{
        ArtifactCapability, ConsumerActivation, LateApplyGranularity, NullSemantics,
    };
    use crate::runtime_filter::port::artifact::ArtifactBundle;
    use crate::runtime_filter::port::identity::LogicalVersion;
    use crate::runtime_filter::port::subscription::{
        ArtifactAcquireOutcome, BlockingSnapshotSubscription, LivePollOutcome, LiveTerminal,
        NonBlockingLiveSubscription, SubscriptionKind, UnavailableReason,
    };

    struct TestSubscription {
        outcomes: Mutex<Vec<ArtifactAcquireOutcome>>,
        snapshot: Option<Arc<ArtifactBundle>>,
    }

    impl TestSubscription {
        fn new(outcomes: Vec<ArtifactAcquireOutcome>) -> Self {
            let snapshot = outcomes.iter().find_map(|outcome| match outcome {
                ArtifactAcquireOutcome::Published(bundle) => Some(Arc::clone(bundle)),
                _ => None,
            });
            Self {
                outcomes: Mutex::new(outcomes),
                snapshot,
            }
        }
    }

    impl BlockingSnapshotSubscription for TestSubscription {
        fn acquire(&self, _timeout: Duration) -> ArtifactAcquireOutcome {
            self.outcomes.lock().unwrap().remove(0)
        }

        fn snapshot(&self) -> Option<Arc<ArtifactBundle>> {
            self.snapshot.clone()
        }
    }

    fn fixture(
        outcomes: Vec<ArtifactAcquireOutcome>,
    ) -> (RuntimeFilterConsumerSet, crate::exec::chunk::Chunk) {
        let mut arena = ExprArena::default();
        let spec = consumer_spec(&mut arena);
        let subscription: Arc<dyn BlockingSnapshotSubscription> =
            Arc::new(TestSubscription::new(outcomes));
        (
            RuntimeFilterConsumerSet::from_bound_for_test(
                vec![spec],
                Arc::new(arena),
                vec![subscription],
            ),
            chunk(&[1, 2, 3, 4]),
        )
    }

    fn consumer_spec(arena: &mut ExprArena) -> RuntimeFilterConsumerBinding {
        let expr_id = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let schema = crate::runtime_filter::port::artifact::ArtifactMembershipSchema::new(
            &DataType::Int32,
            NullSemantics::NeverMatches,
        )
        .unwrap();
        RuntimeFilterConsumerBinding {
            binding_id: 11,
            channel_id: 7,
            expr_id,
            activation: ConsumerActivation::BlockingSnapshot,
            capabilities: BTreeSet::from([
                ArtifactCapability::Membership,
                ArtifactCapability::EmptyDomain,
            ]),
            contract: RuntimeFilterExecutionContract::Membership {
                canonical_schema: Arc::from(schema.canonical_bytes()),
                schema_digest: schema.digest().bytes(),
            },
            reduction: RuntimeFilterExecutionReduction::SetUnion,
        }
    }

    #[test]
    fn native_join_plan_accepts_only_blocking_or_batch_live_membership() {
        for activation in [
            ConsumerActivation::BlockingSnapshot,
            ConsumerActivation::NonBlockingLive {
                late_apply: LateApplyGranularity::Batch,
            },
        ] {
            let mut arena = ExprArena::default();
            let mut spec = consumer_spec(&mut arena);
            spec.activation = activation;
            assert!(RuntimeFilterConsumerSet::from_plan(&[spec], Arc::new(arena)).is_ok());
        }

        for late_apply in [
            LateApplyGranularity::Row,
            LateApplyGranularity::RowGroup,
            LateApplyGranularity::Split,
            LateApplyGranularity::File,
        ] {
            let mut arena = ExprArena::default();
            let mut spec = consumer_spec(&mut arena);
            spec.activation = ConsumerActivation::NonBlockingLive { late_apply };
            assert!(RuntimeFilterConsumerSet::from_plan(&[spec], Arc::new(arena)).is_err());
        }
    }

    #[test]
    fn native_join_resolved_activation_and_subscription_kind_must_match_the_spec() {
        let batch_live = ConsumerActivation::NonBlockingLive {
            late_apply: LateApplyGranularity::Batch,
        };
        assert!(validate_resolved_consumer_activation(batch_live, batch_live).is_ok());
        assert!(validate_resolved_consumer_activation(
            batch_live,
            ConsumerActivation::BlockingSnapshot,
        )
        .is_err());
        assert_eq!(
            subscription_kind_for_activation(ConsumerActivation::BlockingSnapshot).unwrap(),
            SubscriptionKind::BlockingSnapshot,
        );
        assert_eq!(
            subscription_kind_for_activation(batch_live).unwrap(),
            SubscriptionKind::NonBlockingLive,
        );
        for late_apply in [
            LateApplyGranularity::Row,
            LateApplyGranularity::RowGroup,
            LateApplyGranularity::Split,
            LateApplyGranularity::File,
        ] {
            assert!(
                subscription_kind_for_activation(ConsumerActivation::NonBlockingLive {
                    late_apply,
                })
                .is_err(),
                "{late_apply:?} must not request a live subscription for a Join consumer"
            );
        }
    }

    struct ScriptedLiveSubscription {
        outcomes: Mutex<VecDeque<LivePollOutcome>>,
        observed: Mutex<Vec<Option<LogicalVersion>>>,
    }

    impl ScriptedLiveSubscription {
        fn new(outcomes: impl IntoIterator<Item = LivePollOutcome>) -> Self {
            Self {
                outcomes: Mutex::new(outcomes.into_iter().collect()),
                observed: Mutex::new(Vec::new()),
            }
        }

        fn observed(&self) -> Vec<Option<LogicalVersion>> {
            self.observed.lock().unwrap().clone()
        }
    }

    impl NonBlockingLiveSubscription for ScriptedLiveSubscription {
        fn snapshot(&self) -> Option<Arc<ArtifactBundle>> {
            None
        }

        fn poll_after(&self, observed: Option<LogicalVersion>) -> LivePollOutcome {
            self.observed.lock().unwrap().push(observed);
            self.outcomes
                .lock()
                .unwrap()
                .pop_front()
                .expect("test supplies every live poll outcome")
        }
    }

    struct ScriptedExecutionLiveSubscription {
        outcomes: Mutex<VecDeque<execution::LivePollOutcome>>,
        observed: Mutex<Vec<Option<execution::LogicalVersion>>>,
    }

    impl ScriptedExecutionLiveSubscription {
        fn new(outcomes: impl IntoIterator<Item = execution::LivePollOutcome>) -> Self {
            Self {
                outcomes: Mutex::new(outcomes.into_iter().collect()),
                observed: Mutex::new(Vec::new()),
            }
        }

        fn observed(&self) -> Vec<Option<execution::LogicalVersion>> {
            self.observed.lock().unwrap().clone()
        }
    }

    impl execution::NonBlockingLiveSubscription for ScriptedExecutionLiveSubscription {
        fn snapshot(&self) -> Option<Arc<execution::RuntimeFilterSnapshot>> {
            None
        }

        fn poll_after(
            &self,
            observed: Option<execution::LogicalVersion>,
        ) -> execution::LivePollOutcome {
            self.observed.lock().unwrap().push(observed);
            self.outcomes
                .lock()
                .unwrap()
                .pop_front()
                .expect("test supplies every live poll outcome")
        }
    }

    fn live_spec(arena: &mut ExprArena) -> RuntimeFilterConsumerBinding {
        let mut spec = consumer_spec(arena);
        spec.activation = ConsumerActivation::NonBlockingLive {
            late_apply: LateApplyGranularity::Batch,
        };
        spec
    }

    fn live_fixture(
        outcomes: impl IntoIterator<Item = LivePollOutcome>,
    ) -> (
        RuntimeFilterConsumerSet,
        Arc<ScriptedExecutionLiveSubscription>,
    ) {
        let mut arena = ExprArena::default();
        let spec = live_spec(&mut arena);
        let execution_outcomes = outcomes
            .into_iter()
            .map(|outcome| execution_live_outcome(&spec, outcome));
        let subscription = Arc::new(ScriptedExecutionLiveSubscription::new(execution_outcomes));
        let typed: Arc<dyn execution::NonBlockingLiveSubscription> = subscription.clone();
        (
            RuntimeFilterConsumerSet::from_execution_live_bound_for_test(
                vec![spec],
                Arc::new(arena),
                vec![typed],
            ),
            subscription,
        )
    }

    fn set_live_observed(consumers: &RuntimeFilterConsumerSet, next: Option<LogicalVersion>) {
        let mut bindings = consumers
            .inner
            .bindings
            .lock()
            .expect("native RF consumer lock");
        let NativeConsumerBindingState::BoundLive { observed, .. } = &mut bindings[0].state else {
            panic!("fixture binding is live");
        };
        *observed = next.map(|version| execution::LogicalVersion::new(version.get()));
    }

    fn execution_live_outcome(
        spec: &RuntimeFilterConsumerBinding,
        outcome: LivePollOutcome,
    ) -> execution::LivePollOutcome {
        match outcome {
            LivePollOutcome::Updated { bundle, terminal } => execution::LivePollOutcome::Updated {
                snapshot: execution_membership_snapshot(spec, bundle),
                terminal: terminal.map(execution_live_terminal),
            },
            LivePollOutcome::Idle {
                latest_version,
                terminal,
            } => execution::LivePollOutcome::Idle {
                latest_version: latest_version
                    .map(|version| execution::LogicalVersion::new(version.get())),
                terminal: terminal.map(execution_live_terminal),
            },
        }
    }

    fn execution_membership_snapshot(
        spec: &RuntimeFilterConsumerBinding,
        bundle: Arc<ArtifactBundle>,
    ) -> Arc<execution::RuntimeFilterSnapshot> {
        let predicate: Arc<dyn execution::RuntimeFilterPredicate> = if bundle.version()
            == LogicalVersion::FIRST
        {
            Arc::new(NativeExecutionPredicate::Membership(
                NativeRuntimeFilterPredicate::compile(
                    &bundle,
                    &membership_predicate_contract(spec).expect("valid membership test contract"),
                )
                .expect("valid membership test artifact"),
            ))
        } else {
            Arc::new(AlwaysPassesTestPredicate)
        };
        Arc::new(execution::RuntimeFilterSnapshot::new(
            execution::RuntimeFilterBindingId::new(spec.binding_id),
            execution::LogicalVersion::new(bundle.version().get()),
            [0; 32],
            predicate,
        ))
    }

    struct AlwaysPassesTestPredicate;

    impl execution::RuntimeFilterPredicate for AlwaysPassesTestPredicate {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn evaluate(
            &self,
            input: &arrow::array::ArrayRef,
        ) -> Result<BooleanArray, execution::RuntimeFilterContractViolation> {
            Ok(BooleanArray::from(vec![true; input.len()]))
        }
    }

    fn execution_live_terminal(terminal: LiveTerminal) -> execution::LiveTerminal {
        match terminal {
            LiveTerminal::Completed => execution::LiveTerminal::Completed,
            LiveTerminal::CompletedWithoutArtifact
            | LiveTerminal::DegradedLogical(_)
            | LiveTerminal::DegradedArtifact(_)
            | LiveTerminal::DegradedDelivery(_)
            | LiveTerminal::Unavailable(_) => execution::LiveTerminal::CompletedWithoutArtifact,
            LiveTerminal::Cancelled => execution::LiveTerminal::Cancelled,
        }
    }

    fn assert_values(output: Option<crate::exec::chunk::Chunk>, expected: &[i32]) {
        let output = output.expect("test expects a nonempty output chunk");
        assert_eq!(
            output.columns()[0]
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values(),
            expected
        );
    }

    #[test]
    fn native_join_batch_live_idle_without_terminal_preserves_current_batch_and_binding() {
        let (consumers, subscription) = live_fixture([
            LivePollOutcome::Idle {
                latest_version: None,
                terminal: None,
            },
            LivePollOutcome::Idle {
                latest_version: None,
                terminal: None,
            },
        ]);

        consumers.acquire_configured().unwrap();
        assert!(
            subscription.observed().is_empty(),
            "acquire must not poll live"
        );
        assert_values(
            consumers.apply_chunk(chunk(&[1, 2, 3, 4])).unwrap(),
            &[1, 2, 3, 4],
        );
        assert_values(consumers.apply_chunk(chunk(&[5, 6])).unwrap(), &[5, 6]);
        assert_eq!(subscription.observed(), vec![None, None]);
    }

    #[test]
    fn native_join_batch_live_idle_rejects_ahead_version_without_artifact() {
        let (consumers, _) = live_fixture([LivePollOutcome::Idle {
            latest_version: Some(LogicalVersion::FIRST),
            terminal: None,
        }]);

        let error = consumers
            .apply_chunk(chunk(&[1, 2, 3, 4]))
            .expect_err("Idle must not advance a CompleteOnce cursor without an artifact");

        assert!(error.contains("advanced without returning an artifact"));
    }

    #[test]
    fn native_join_batch_live_idle_rejects_cursor_regression() {
        let (consumers, _) = live_fixture([LivePollOutcome::Idle {
            latest_version: None,
            terminal: None,
        }]);
        set_live_observed(&consumers, Some(LogicalVersion::FIRST));

        let error = consumers
            .apply_chunk(chunk(&[1, 2, 3, 4]))
            .expect_err("Idle must not regress a CompleteOnce cursor");

        assert!(error.contains("regressed"));
    }

    #[test]
    fn native_join_batch_live_idle_rejects_nonfirst_version() {
        let (consumers, _) = live_fixture([LivePollOutcome::Idle {
            latest_version: Some(LogicalVersion::new(2)),
            terminal: None,
        }]);

        let error = consumers
            .apply_chunk(chunk(&[1, 2, 3, 4]))
            .expect_err("CompleteOnce Idle must reject a non-FIRST version");

        assert!(error.contains("LogicalVersion::FIRST"));
    }

    #[test]
    fn native_join_batch_live_first_completed_artifact_filters_the_current_batch() {
        let (consumers, subscription) = live_fixture([LivePollOutcome::Updated {
            bundle: membership_bundle(&[2, 4]),
            terminal: Some(LiveTerminal::Completed),
        }]);

        consumers.acquire_configured().unwrap();
        assert!(
            subscription.observed().is_empty(),
            "acquire must not poll live"
        );
        assert_values(
            consumers.apply_chunk(chunk(&[1, 2, 3, 4])).unwrap(),
            &[2, 4],
        );
        assert_eq!(subscription.observed(), vec![None]);
        assert_values(
            consumers.apply_chunk(chunk(&[2, 3, 4, 5])).unwrap(),
            &[2, 4],
        );
        assert_eq!(
            subscription.observed(),
            vec![None],
            "active binding must not poll again"
        );
    }

    #[test]
    fn native_join_batch_live_updated_without_terminal_fails_fast() {
        let (consumers, _) = live_fixture([LivePollOutcome::Updated {
            bundle: membership_bundle(&[2, 4]),
            terminal: None,
        }]);
        consumers.acquire_configured().unwrap();

        let error = consumers
            .apply_chunk(chunk(&[1, 2, 3, 4]))
            .expect_err("an unterminated CompleteOnce update must fail");
        assert!(error.contains("Updated"));
    }

    #[test]
    fn native_join_batch_live_updated_nonfirst_version_fails_fast() {
        let (consumers, _) = live_fixture([LivePollOutcome::Updated {
            bundle: membership_bundle_with_version(&[2, 4], LogicalVersion::new(2)),
            terminal: Some(LiveTerminal::Completed),
        }]);
        consumers.acquire_configured().unwrap();

        let error = consumers
            .apply_chunk(chunk(&[1, 2, 3, 4]))
            .expect_err("CompleteOnce must reject a non-FIRST update");
        assert!(error.contains("FIRST"));
    }

    #[test]
    fn native_join_batch_live_updated_nonartifact_terminals_fail_fast() {
        for terminal in [
            LiveTerminal::CompletedWithoutArtifact,
            LiveTerminal::DegradedLogical(UnavailableReason::IncompleteCoverage),
            LiveTerminal::DegradedArtifact(UnavailableReason::MaterializationFailed),
            LiveTerminal::DegradedDelivery(UnavailableReason::RouteUnavailable),
            LiveTerminal::Unavailable(UnavailableReason::ProducerFailed),
            LiveTerminal::Cancelled,
        ] {
            let (consumers, _) = live_fixture([LivePollOutcome::Updated {
                bundle: membership_bundle(&[2, 4]),
                terminal: Some(terminal),
            }]);
            consumers.acquire_configured().unwrap();
            let error = consumers
                .apply_chunk(chunk(&[1, 2, 3, 4]))
                .expect_err("terminal without a usable artifact must reject Updated");
            assert!(error.contains("Updated"), "terminal={terminal:?}: {error}");
        }
    }

    #[test]
    fn native_join_batch_live_idle_nonartifact_terminals_become_sticky_pass_through() {
        for terminal in [
            LiveTerminal::CompletedWithoutArtifact,
            LiveTerminal::DegradedLogical(UnavailableReason::IncompleteCoverage),
            LiveTerminal::DegradedArtifact(UnavailableReason::MaterializationFailed),
            LiveTerminal::DegradedDelivery(UnavailableReason::RouteUnavailable),
            LiveTerminal::Unavailable(UnavailableReason::ProducerFailed),
            LiveTerminal::Cancelled,
        ] {
            let (consumers, subscription) = live_fixture([LivePollOutcome::Idle {
                latest_version: None,
                terminal: Some(terminal),
            }]);
            consumers.acquire_configured().unwrap();
            assert!(
                subscription.observed().is_empty(),
                "acquire must not poll live"
            );
            assert_values(
                consumers.apply_chunk(chunk(&[1, 2, 3, 4])).unwrap(),
                &[1, 2, 3, 4],
            );
            assert_values(consumers.apply_chunk(chunk(&[5, 6])).unwrap(), &[5, 6]);
            assert_eq!(
                subscription.observed(),
                vec![None],
                "terminal={terminal:?} must be sticky pass-through"
            );
        }
    }

    #[test]
    fn native_join_batch_live_idle_completed_without_final_artifact_fails_fast() {
        let (consumers, _) = live_fixture([LivePollOutcome::Idle {
            latest_version: Some(LogicalVersion::FIRST),
            terminal: Some(LiveTerminal::Completed),
        }]);
        consumers.acquire_configured().unwrap();

        let error = consumers
            .apply_chunk(chunk(&[1, 2, 3, 4]))
            .expect_err("Completed without its final artifact must fail");
        assert!(error.contains("Completed"));
    }

    #[test]
    fn native_join_mixed_acquire_uses_one_blocking_deadline_and_never_polls_live() {
        struct RecordingBlockingSubscription {
            delay: Duration,
            observed: Arc<Mutex<Vec<Duration>>>,
        }

        impl BlockingSnapshotSubscription for RecordingBlockingSubscription {
            fn acquire(&self, timeout: Duration) -> ArtifactAcquireOutcome {
                self.observed.lock().unwrap().push(timeout);
                std::thread::sleep(self.delay);
                ArtifactAcquireOutcome::TimedOut
            }

            fn snapshot(&self) -> Option<Arc<ArtifactBundle>> {
                None
            }
        }

        let mut arena = ExprArena::default();
        let blocking_first = consumer_spec(&mut arena);
        let mut live = live_spec(&mut arena);
        live.binding_id = 12;
        let mut blocking_second = consumer_spec(&mut arena);
        blocking_second.binding_id = 13;
        let first_seen = Arc::new(Mutex::new(Vec::new()));
        let second_seen = Arc::new(Mutex::new(Vec::new()));
        let live_subscription = Arc::new(ScriptedLiveSubscription::new([LivePollOutcome::Idle {
            latest_version: None,
            terminal: None,
        }]));
        let consumers = RuntimeFilterConsumerSet::from_mixed_bound_for_test(
            vec![blocking_first, live, blocking_second],
            Arc::new(arena),
            vec![
                NativeConsumerTestSubscription::Blocking(Arc::new(RecordingBlockingSubscription {
                    delay: Duration::from_millis(20),
                    observed: Arc::clone(&first_seen),
                })),
                NativeConsumerTestSubscription::Live(live_subscription.clone()),
                NativeConsumerTestSubscription::Blocking(Arc::new(RecordingBlockingSubscription {
                    delay: Duration::ZERO,
                    observed: Arc::clone(&second_seen),
                })),
            ],
        );

        consumers
            .acquire_blocking(Duration::from_millis(5))
            .unwrap();
        assert_eq!(first_seen.lock().unwrap().len(), 1);
        assert_eq!(second_seen.lock().unwrap().as_slice(), &[Duration::ZERO]);
        assert!(
            live_subscription.observed().is_empty(),
            "live polling must not consume the blocking deadline"
        );
    }

    #[test]
    fn native_join_all_live_acquire_configured_completes_immediately_without_polling() {
        let (consumers, subscription) = live_fixture([LivePollOutcome::Idle {
            latest_version: None,
            terminal: None,
        }]);
        consumers.set_wait_timeout(Duration::from_secs(30));

        let started = Instant::now();
        consumers.acquire_configured().unwrap();
        consumers.acquire_configured().unwrap();
        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(
            subscription.observed().is_empty(),
            "acquire phase must complete without touching live subscriptions"
        );
    }

    #[test]
    fn native_join_apply_rejects_unacquired_blocking_but_allows_pending_live() {
        let mut arena = ExprArena::default();
        let blocking = consumer_spec(&mut arena);
        let mut live = live_spec(&mut arena);
        live.binding_id = 12;
        let live_subscription = Arc::new(ScriptedLiveSubscription::new([LivePollOutcome::Idle {
            latest_version: None,
            terminal: None,
        }]));
        let mixed = RuntimeFilterConsumerSet::from_mixed_bound_for_test(
            vec![blocking, live],
            Arc::new(arena),
            vec![
                NativeConsumerTestSubscription::Blocking(Arc::new(TestSubscription::new(vec![
                    ArtifactAcquireOutcome::TimedOut,
                ]))),
                NativeConsumerTestSubscription::Live(live_subscription),
            ],
        );
        let error = mixed
            .apply_chunk(chunk(&[1, 2, 3, 4]))
            .expect_err("unacquired blocking consumer must reject apply");
        assert!(error.contains("must acquire before apply"));

        let (all_live, subscription) = live_fixture([LivePollOutcome::Idle {
            latest_version: None,
            terminal: None,
        }]);
        assert_values(
            all_live.apply_chunk(chunk(&[1, 2, 3, 4])).unwrap(),
            &[1, 2, 3, 4],
        );
        assert_eq!(subscription.observed(), vec![None]);
    }

    #[test]
    fn native_join_blocking_timeout_fails_open_without_late_reapply() {
        let artifact = membership_bundle(&[2, 4]);
        let (consumers, input) = fixture(vec![
            ArtifactAcquireOutcome::TimedOut,
            ArtifactAcquireOutcome::Published(artifact),
        ]);
        consumers.acquire_blocking(Duration::ZERO).unwrap();
        consumers.acquire_blocking(Duration::ZERO).unwrap();
        assert_eq!(consumers.apply_chunk(input).unwrap().unwrap().len(), 4);
    }

    #[test]
    fn native_join_unavailable_fails_open_without_result_drift() {
        let (consumers, input) = fixture(vec![ArtifactAcquireOutcome::Unavailable(
            UnavailableReason::ProducerFailed,
        )]);
        consumers.acquire_blocking(Duration::ZERO).unwrap();
        assert_eq!(consumers.apply_chunk(input).unwrap().unwrap().len(), 4);
    }

    #[test]
    fn empty_domain_filters_all_rows() {
        let artifact = membership_bundle(&[]);
        let (consumers, input) = fixture(vec![ArtifactAcquireOutcome::Published(artifact)]);
        consumers.acquire_blocking(Duration::ZERO).unwrap();
        assert!(consumers.apply_chunk(input).unwrap().is_none());
    }

    #[test]
    fn native_direct_wrapper_applies_the_shared_membership_mask() {
        let (consumers, _) = super::tests_support::published_consumer_set(
            super::tests_support::membership_bundle(&[2, 4]),
        );
        let state = crate::runtime::runtime_state::RuntimeState::default();
        let mut processor = super::NativeRuntimeFilterProcessor {
            name: "native-rf".into(),
            consumers,
            output: None,
            finishing: false,
            profiles: None,
        };
        let profiler = crate::runtime::profile::Profiler::new("native-rf-test");
        let profiles = crate::runtime::profile::OperatorProfiles::new(
            profiler.child("NativeRuntimeFilter (id=1)"),
        );
        processor.set_profiles(profiles);
        processor.bind_runtime_state(&state).unwrap();
        processor.push_chunk(&state, chunk(&[1, 2, 3, 4])).unwrap();
        let output = processor.pull_chunk(&state).unwrap().unwrap();
        let values = output.columns()[0]
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(values.values(), &[2, 4]);
        let tree = profiler.to_native_tree();
        let common = &tree.root.children[0].children[0];
        let counter = |name: &str| {
            common
                .counters
                .iter()
                .find(|counter| counter.name == name)
                .map(|counter| counter.value)
        };
        assert_eq!(counter("RuntimeFilterInputRows"), Some(4));
        assert_eq!(counter("RuntimeFilterOutputRows"), Some(2));
    }

    #[test]
    fn empty_consumer_set_does_not_record_apply_counters() {
        let consumers =
            RuntimeFilterConsumerSet::from_plan(&[], Arc::new(ExprArena::default())).unwrap();
        let profiler = crate::runtime::profile::Profiler::new("empty-native-rf-test");
        let profiles = crate::runtime::profile::OperatorProfiles::new(
            profiler.child("NativeRuntimeFilter (id=1)"),
        );

        let output = consumers
            .apply_chunk_profiled(chunk(&[1, 2, 3, 4]), Some(&profiles))
            .unwrap()
            .unwrap();

        assert_eq!(output.len(), 4);
        assert_eq!(
            profiles.common.counter_value(RUNTIME_FILTER_INPUT_ROWS),
            None
        );
        assert_eq!(
            profiles.common.counter_value(RUNTIME_FILTER_OUTPUT_ROWS),
            None
        );
    }

    #[test]
    fn configured_pass_through_records_equal_apply_counters() {
        let (consumers, input) = fixture(vec![ArtifactAcquireOutcome::Unavailable(
            UnavailableReason::ProducerFailed,
        )]);
        consumers.acquire_blocking(Duration::ZERO).unwrap();
        let profiler = crate::runtime::profile::Profiler::new("pass-through-native-rf-test");
        let profiles = crate::runtime::profile::OperatorProfiles::new(
            profiler.child("NativeRuntimeFilter (id=1)"),
        );

        let output = consumers
            .apply_chunk_profiled(input, Some(&profiles))
            .unwrap()
            .unwrap();

        assert_eq!(output.len(), 4);
        assert_eq!(
            profiles.common.counter_value(RUNTIME_FILTER_INPUT_ROWS),
            Some(4)
        );
        assert_eq!(
            profiles.common.counter_value(RUNTIME_FILTER_OUTPUT_ROWS),
            Some(4)
        );
    }

    #[test]
    fn native_consumer_rejects_duplicate_binding_before_subscribe() {
        let mut arena = ExprArena::default();
        let spec = consumer_spec(&mut arena);
        let error = RuntimeFilterConsumerSet::from_plan(&[spec.clone(), spec], Arc::new(arena))
            .err()
            .expect("duplicate binding must fail");
        assert!(error.contains("duplicate"));
    }

    #[test]
    fn native_consumer_rejects_artifact_outside_installed_join_profile() {
        use crate::runtime_filter::port::artifact::{
            ArtifactBundle, ArtifactKind, ConsumerArtifactProfile,
        };
        let valid = membership_bundle(&[2, 4]);
        let (kind, artifact) = &valid.artifacts()[0];
        assert_eq!(*kind, ArtifactKind::ValueSet);
        let profile =
            ConsumerArtifactProfile::new(BTreeSet::from([ArtifactKind::ValueSet]), None).unwrap();
        let wrong = Arc::new(
            ArtifactBundle::new(
                valid.channel_id(),
                valid.version(),
                &profile,
                vec![(*kind, Arc::clone(artifact))],
                usize::MAX,
            )
            .unwrap(),
        );
        let (consumers, _) = fixture(vec![ArtifactAcquireOutcome::Published(wrong)]);
        let error = consumers
            .acquire_blocking(Duration::ZERO)
            .expect_err("profile drift must fail synchronously");
        assert!(error.contains("ProfileMismatch"));
    }

    #[test]
    fn native_consumers_share_one_total_blocking_deadline() {
        struct RecordingSubscription {
            delay: Duration,
            outcome: ArtifactAcquireOutcome,
            observed: Arc<Mutex<Vec<Duration>>>,
        }
        impl BlockingSnapshotSubscription for RecordingSubscription {
            fn acquire(&self, timeout: Duration) -> ArtifactAcquireOutcome {
                self.observed.lock().unwrap().push(timeout);
                std::thread::sleep(self.delay);
                self.outcome.clone()
            }
            fn snapshot(&self) -> Option<Arc<ArtifactBundle>> {
                None
            }
        }

        let mut arena = ExprArena::default();
        let first = consumer_spec(&mut arena);
        let mut second = first.clone();
        second.binding_id = 12;
        let first_seen = Arc::new(Mutex::new(Vec::new()));
        let second_seen = Arc::new(Mutex::new(Vec::new()));
        let first_subscription: Arc<dyn BlockingSnapshotSubscription> =
            Arc::new(RecordingSubscription {
                delay: Duration::from_millis(20),
                outcome: ArtifactAcquireOutcome::TimedOut,
                observed: Arc::clone(&first_seen),
            });
        let second_subscription: Arc<dyn BlockingSnapshotSubscription> =
            Arc::new(RecordingSubscription {
                delay: Duration::ZERO,
                outcome: ArtifactAcquireOutcome::Unavailable(UnavailableReason::ProducerFailed),
                observed: Arc::clone(&second_seen),
            });
        let consumers = RuntimeFilterConsumerSet::from_bound_for_test(
            vec![first, second],
            Arc::new(arena),
            vec![first_subscription, second_subscription],
        );
        consumers
            .acquire_blocking(Duration::from_millis(5))
            .unwrap();
        assert_eq!(first_seen.lock().unwrap().len(), 1);
        assert_eq!(second_seen.lock().unwrap().as_slice(), &[Duration::ZERO]);
    }

    #[test]
    fn acquire_timeout_resolves_to_pass_through_within_bound() {
        struct GatedSubscription {
            gate: Arc<(Mutex<bool>, Condvar)>,
        }

        impl BlockingSnapshotSubscription for GatedSubscription {
            fn acquire(&self, timeout: Duration) -> ArtifactAcquireOutcome {
                let (open, ready) = &*self.gate;
                let opened = open.lock().unwrap();
                let (opened, _) = ready
                    .wait_timeout_while(opened, timeout, |open| !*open)
                    .unwrap();
                if *opened {
                    unreachable!("test gate must remain closed")
                }
                ArtifactAcquireOutcome::TimedOut
            }

            fn snapshot(&self) -> Option<Arc<ArtifactBundle>> {
                None
            }
        }

        let mut arena = ExprArena::default();
        let spec = consumer_spec(&mut arena);
        let subscription: Arc<dyn BlockingSnapshotSubscription> = Arc::new(GatedSubscription {
            gate: Arc::new((Mutex::new(false), Condvar::new())),
        });
        let consumers = RuntimeFilterConsumerSet::from_bound_for_test(
            vec![spec],
            Arc::new(arena),
            vec![subscription],
        );

        let started = std::time::Instant::now();
        consumers
            .acquire_blocking(Duration::from_millis(50))
            .expect("bounded acquire");
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "acquire must be bounded"
        );

        let input = chunk(&[1, 2, 3, 4]);
        let output = consumers.apply_chunk(input).unwrap().unwrap();
        let values = output.columns()[0]
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("test input must remain Int32");
        assert_eq!(
            values.values(),
            &[1, 2, 3, 4],
            "timed-out binding must preserve the input values"
        );
    }

    #[test]
    fn native_concurrent_acquire_is_single_flight_without_holding_bindings_lock() {
        struct GatedSubscription {
            calls: Arc<AtomicUsize>,
            entered: Mutex<Option<mpsc::Sender<()>>>,
            release: Arc<(Mutex<bool>, Condvar)>,
            bundle: Arc<ArtifactBundle>,
        }
        impl BlockingSnapshotSubscription for GatedSubscription {
            fn acquire(&self, _timeout: Duration) -> ArtifactAcquireOutcome {
                self.calls.fetch_add(1, Ordering::SeqCst);
                if let Some(entered) = self.entered.lock().unwrap().take() {
                    entered.send(()).unwrap();
                }
                let (lock, ready) = &*self.release;
                let mut released = lock.lock().unwrap();
                while !*released {
                    released = ready.wait(released).unwrap();
                }
                ArtifactAcquireOutcome::Published(Arc::clone(&self.bundle))
            }
            fn snapshot(&self) -> Option<Arc<ArtifactBundle>> {
                None
            }
        }

        let mut arena = ExprArena::default();
        let spec = consumer_spec(&mut arena);
        let calls = Arc::new(AtomicUsize::new(0));
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let (entered_tx, entered_rx) = mpsc::channel();
        let subscription: Arc<dyn BlockingSnapshotSubscription> = Arc::new(GatedSubscription {
            calls: Arc::clone(&calls),
            entered: Mutex::new(Some(entered_tx)),
            release: Arc::clone(&release),
            bundle: membership_bundle(&[2, 4]),
        });
        let consumers = RuntimeFilterConsumerSet::from_bound_for_test(
            vec![spec],
            Arc::new(arena),
            vec![subscription],
        );

        let first_consumers = consumers.clone();
        let first =
            std::thread::spawn(move || first_consumers.acquire_blocking(Duration::from_secs(1)));
        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();

        let second_consumers = consumers.clone();
        let second =
            std::thread::spawn(move || second_consumers.acquire_blocking(Duration::from_secs(1)));
        let apply_consumers = consumers.clone();
        let (apply_tx, apply_rx) = mpsc::channel();
        std::thread::spawn(move || {
            apply_tx
                .send(apply_consumers.apply_chunk(chunk(&[1, 2, 3, 4])))
                .unwrap();
        });
        let apply_error = apply_rx
            .recv_timeout(Duration::from_millis(100))
            .expect("bindings lock must remain available during external acquire")
            .expect_err("apply must wait for acquisition to complete");
        assert!(apply_error.contains("must acquire before apply"));
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let (lock, ready) = &*release;
        *lock.lock().unwrap() = true;
        ready.notify_all();
        first.join().unwrap().unwrap();
        second.join().unwrap().unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn native_consumer_acquires_once_and_reuses_one_immutable_version_across_chunks() {
        struct CountingSubscription {
            calls: Arc<AtomicUsize>,
            bundle: Arc<ArtifactBundle>,
        }
        impl BlockingSnapshotSubscription for CountingSubscription {
            fn acquire(&self, _timeout: Duration) -> ArtifactAcquireOutcome {
                self.calls.fetch_add(1, Ordering::SeqCst);
                ArtifactAcquireOutcome::Published(Arc::clone(&self.bundle))
            }
            fn snapshot(&self) -> Option<Arc<ArtifactBundle>> {
                Some(Arc::clone(&self.bundle))
            }
        }

        let mut arena = ExprArena::default();
        let spec = consumer_spec(&mut arena);
        let calls = Arc::new(AtomicUsize::new(0));
        let subscription: Arc<dyn BlockingSnapshotSubscription> = Arc::new(CountingSubscription {
            calls: Arc::clone(&calls),
            bundle: membership_bundle(&[2, 4]),
        });
        let consumers = RuntimeFilterConsumerSet::from_bound_for_test(
            vec![spec],
            Arc::new(arena),
            vec![subscription],
        );

        consumers.acquire_blocking(Duration::ZERO).unwrap();
        let first = consumers
            .apply_chunk(chunk(&[1, 2, 3, 4]))
            .unwrap()
            .unwrap();
        consumers.acquire_blocking(Duration::ZERO).unwrap();
        let second = consumers
            .apply_chunk(chunk(&[2, 3, 4, 5]))
            .unwrap()
            .unwrap();

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            first.columns()[0]
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values(),
            &[2, 4]
        );
        assert_eq!(
            second.columns()[0]
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values(),
            &[2, 4]
        );
    }

    #[test]
    fn native_consumer_keeps_active_binding_when_another_binding_is_unavailable() {
        let mut arena = ExprArena::default();
        let first = consumer_spec(&mut arena);
        let mut second = first.clone();
        second.binding_id = 12;
        let active: Arc<dyn BlockingSnapshotSubscription> = Arc::new(TestSubscription::new(vec![
            ArtifactAcquireOutcome::Published(membership_bundle(&[2, 4])),
        ]));
        let unavailable: Arc<dyn BlockingSnapshotSubscription> =
            Arc::new(TestSubscription::new(vec![
                ArtifactAcquireOutcome::Unavailable(UnavailableReason::ProducerFailed),
            ]));
        let consumers = RuntimeFilterConsumerSet::from_bound_for_test(
            vec![first, second],
            Arc::new(arena),
            vec![active, unavailable],
        );

        consumers.acquire_blocking(Duration::ZERO).unwrap();
        let output = consumers
            .apply_chunk(chunk(&[1, 2, 3, 4]))
            .unwrap()
            .unwrap();
        assert_eq!(
            output.columns()[0]
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values(),
            &[2, 4]
        );
    }

    #[test]
    fn native_consumer_unsupported_and_cancelled_are_sticky_pass_through() {
        use crate::runtime_filter::port::subscription::ArtifactUnsupportedReason;

        for outcome in [
            ArtifactAcquireOutcome::Unsupported(
                ArtifactUnsupportedReason::NoAcceptedRepresentation,
            ),
            ArtifactAcquireOutcome::Cancelled,
        ] {
            let (consumers, input) = fixture(vec![outcome]);
            consumers.acquire_blocking(Duration::ZERO).unwrap();
            consumers.acquire_blocking(Duration::ZERO).unwrap();
            assert_eq!(consumers.apply_chunk(input).unwrap().unwrap().len(), 4);
        }
    }
}

#[cfg(test)]
mod native_ordered_live_consumer_tests {
    use std::collections::{BTreeSet, VecDeque};
    use std::sync::{Arc, Mutex};

    use arrow::array::{ArrayRef, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::{NativeOrderedLiveConsumerSet, RuntimeFilterConsumerSet};
    use crate::common::ids::SlotId;
    use crate::exec::chunk::{Chunk, ChunkSchema};
    use crate::exec::expr::{ExprArena, ExprNode};
    use crate::exec::node::runtime_filter::{
        RuntimeFilterConsumerBinding, RuntimeFilterExecutionContract,
        RuntimeFilterExecutionReduction,
    };
    use crate::runtime::runtime_state::RuntimeState;
    use crate::runtime_filter::exec::ordered_range_predicate::tests_support::{bundle, contract};
    use crate::runtime_filter::model::contract::{
        ArtifactCapability, ConsumerActivation, LateApplyGranularity, NullOrder, NullSemantics,
        SortDirection,
    };
    use crate::runtime_filter::port::artifact::{ArtifactBundle, ArtifactMembershipSchema};
    use crate::runtime_filter::port::identity::LogicalVersion;
    use crate::runtime_filter::port::ordered_bound::{OrderedScalar, RuntimeOrderContract};
    use crate::runtime_filter::port::subscription::{
        LivePollOutcome, LiveTerminal, NonBlockingLiveSubscription, UnavailableReason,
    };

    struct TestLiveSubscription {
        outcomes: Mutex<VecDeque<LivePollOutcome>>,
        observed: Mutex<Vec<Option<LogicalVersion>>>,
    }

    impl TestLiveSubscription {
        fn new(outcomes: Vec<LivePollOutcome>) -> Self {
            Self {
                outcomes: Mutex::new(outcomes.into()),
                observed: Mutex::new(Vec::new()),
            }
        }

        fn observed(&self) -> Vec<Option<LogicalVersion>> {
            self.observed.lock().unwrap().clone()
        }
    }

    impl NonBlockingLiveSubscription for TestLiveSubscription {
        fn snapshot(&self) -> Option<Arc<ArtifactBundle>> {
            None
        }

        fn poll_after(&self, observed: Option<LogicalVersion>) -> LivePollOutcome {
            self.observed.lock().unwrap().push(observed);
            self.outcomes
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(LivePollOutcome::Idle {
                    latest_version: observed,
                    terminal: None,
                })
        }
    }

    fn ordered_spec(
        arena: &mut ExprArena,
        order: &Arc<RuntimeOrderContract>,
        activation: ConsumerActivation,
    ) -> RuntimeFilterConsumerBinding {
        let expr_id = arena.push_typed(
            ExprNode::SlotId(SlotId::new(1)),
            order.keys()[0].data_type().clone(),
        );
        RuntimeFilterConsumerBinding {
            binding_id: 2,
            channel_id: 1,
            expr_id,
            activation,
            capabilities: BTreeSet::from([ArtifactCapability::OrderedRange]),
            contract: RuntimeFilterExecutionContract::Ordered {
                keys: crate::exec::node::runtime_filter::execution_order_keys(order.keys()),
                comparator_digest: order.plan_comparator_digest().get(),
                order_contract_digest: order.digest().bytes(),
            },
            reduction: RuntimeFilterExecutionReduction::TightenOrderedBound,
        }
    }

    fn live_fixture(
        outcomes: Vec<LivePollOutcome>,
    ) -> (
        NativeOrderedLiveConsumerSet,
        Arc<TestLiveSubscription>,
        Arc<RuntimeOrderContract>,
    ) {
        let order = contract(DataType::Int64, SortDirection::Ascending, NullOrder::Last);
        let mut arena = ExprArena::default();
        let mut spec = ordered_spec(
            &mut arena,
            &order,
            ConsumerActivation::NonBlockingLive {
                late_apply: LateApplyGranularity::Batch,
            },
        );
        spec.channel_id = 7;
        let subscription = Arc::new(TestLiveSubscription::new(outcomes));
        let typed: Arc<dyn NonBlockingLiveSubscription> = subscription.clone();
        (
            NativeOrderedLiveConsumerSet::from_bound_for_test(
                vec![spec],
                Arc::new(arena),
                vec![typed],
            ),
            subscription,
            order,
        )
    }

    fn chunk(values: &[i64]) -> Chunk {
        let schema = Schema::new(vec![Field::new("v", DataType::Int64, false)]);
        let chunk_schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(&schema, &[SlotId::new(1)]).unwrap();
        Chunk::try_new_with_columns(
            chunk_schema,
            vec![Arc::new(Int64Array::from(values.to_vec())) as ArrayRef],
        )
        .unwrap()
    }

    fn values(output: Option<Chunk>) -> Vec<i64> {
        output
            .map(|chunk| {
                chunk.columns()[0]
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .unwrap_or_default()
    }

    fn update(
        order: Arc<RuntimeOrderContract>,
        version: u64,
        bound: i64,
        terminal: Option<LiveTerminal>,
    ) -> LivePollOutcome {
        LivePollOutcome::Updated {
            bundle: bundle(
                order,
                Some(OrderedScalar::Int64(bound)),
                LogicalVersion::new(version),
            ),
            terminal,
        }
    }

    #[test]
    fn native_ordered_live_consumer_idle_then_v1_then_v2_replaces_predicate_without_waiting() {
        let order = contract(DataType::Int64, SortDirection::Ascending, NullOrder::Last);
        let (consumers, subscription, _) = live_fixture(vec![
            LivePollOutcome::Idle {
                latest_version: None,
                terminal: None,
            },
            update(order.clone(), 1, 7, None),
            update(order, 2, 3, None),
        ]);

        assert_eq!(
            values(consumers.poll_and_apply_chunk(chunk(&[2, 5, 9])).unwrap()),
            vec![2, 5, 9]
        );
        assert_eq!(
            values(consumers.poll_and_apply_chunk(chunk(&[2, 5, 9])).unwrap()),
            vec![2, 5]
        );
        assert_eq!(
            values(consumers.poll_and_apply_chunk(chunk(&[2, 5, 9])).unwrap()),
            vec![2]
        );
        assert_eq!(
            subscription.observed(),
            vec![None, None, Some(LogicalVersion::FIRST)]
        );
        assert_eq!(consumers.last_seen_for_test(), Some(LogicalVersion::new(2)));
    }

    #[test]
    fn native_ordered_live_consumer_private_cursor_skips_versions_and_never_rolls_back() {
        let order = contract(DataType::Int64, SortDirection::Ascending, NullOrder::Last);
        let (consumers, subscription, _) = live_fixture(vec![
            update(order.clone(), 1, 7, None),
            update(order.clone(), 3, 3, None),
            update(order, 2, 9, None),
        ]);

        assert_eq!(
            values(consumers.poll_and_apply_chunk(chunk(&[2, 5, 8])).unwrap()),
            vec![2, 5]
        );
        assert_eq!(
            values(consumers.poll_and_apply_chunk(chunk(&[2, 5, 8])).unwrap()),
            vec![2]
        );
        assert_eq!(
            values(consumers.poll_and_apply_chunk(chunk(&[2, 5, 8])).unwrap()),
            vec![2]
        );
        assert_eq!(
            subscription.observed(),
            vec![
                None,
                Some(LogicalVersion::FIRST),
                Some(LogicalVersion::new(3))
            ]
        );
        assert_eq!(consumers.last_seen_for_test(), Some(LogicalVersion::new(3)));
    }

    #[test]
    fn native_ordered_live_consumer_clones_keep_private_cursors() {
        let order = contract(DataType::Int64, SortDirection::Ascending, NullOrder::Last);
        let (consumers, subscription, _) = live_fixture(vec![
            update(order.clone(), 1, 3, None),
            update(order, 1, 3, None),
        ]);
        let sibling = consumers.clone();

        assert_eq!(
            values(consumers.poll_and_apply_chunk(chunk(&[2, 5])).unwrap()),
            vec![2]
        );
        assert_eq!(
            values(sibling.poll_and_apply_chunk(chunk(&[2, 5])).unwrap()),
            vec![2]
        );

        assert_eq!(subscription.observed(), vec![None, None]);
        assert_eq!(consumers.last_seen_for_test(), Some(LogicalVersion::FIRST));
        assert_eq!(sibling.last_seen_for_test(), Some(LogicalVersion::FIRST));
    }

    #[test]
    fn native_ordered_live_consumer_rejects_idle_version_ahead_of_private_cursor() {
        let order = contract(DataType::Int64, SortDirection::Ascending, NullOrder::Last);
        let (consumers, _, _) = live_fixture(vec![
            update(order, 1, 7, None),
            LivePollOutcome::Idle {
                latest_version: Some(LogicalVersion::new(2)),
                terminal: None,
            },
        ]);
        consumers
            .poll_and_apply_chunk(chunk(&[2, 5]))
            .expect("v1 must compile");

        let error = consumers
            .poll_and_apply_chunk(chunk(&[2, 5]))
            .expect_err("Idle cannot hide a newer trusted artifact");

        assert!(error.contains("without artifact"), "{error}");
        assert_eq!(consumers.last_seen_for_test(), Some(LogicalVersion::FIRST));
    }

    #[test]
    fn native_ordered_live_consumer_terminal_update_retains_last_artifact() {
        let order = contract(DataType::Int64, SortDirection::Ascending, NullOrder::Last);
        let (consumers, subscription, _) =
            live_fixture(vec![update(order, 1, 3, Some(LiveTerminal::Completed))]);

        assert_eq!(
            values(consumers.poll_and_apply_chunk(chunk(&[2, 5])).unwrap()),
            vec![2]
        );
        assert_eq!(
            values(consumers.poll_and_apply_chunk(chunk(&[2, 5])).unwrap()),
            vec![2]
        );
        assert_eq!(subscription.observed().len(), 1);
        assert_eq!(consumers.terminal_for_test(), Some(LiveTerminal::Completed));
    }

    #[test]
    fn native_ordered_live_consumer_unavailable_before_first_artifact_is_pass_through() {
        let order = contract(DataType::Int64, SortDirection::Ascending, NullOrder::Last);
        let (consumers, subscription, _) = live_fixture(vec![
            LivePollOutcome::Idle {
                latest_version: None,
                terminal: Some(LiveTerminal::Unavailable(
                    UnavailableReason::RouteUnavailable,
                )),
            },
            update(order, 1, 3, None),
        ]);

        assert_eq!(
            values(consumers.poll_and_apply_chunk(chunk(&[2, 5])).unwrap()),
            vec![2, 5]
        );
        assert_eq!(
            values(consumers.poll_and_apply_chunk(chunk(&[2, 5])).unwrap()),
            vec![2, 5]
        );
        assert_eq!(subscription.observed().len(), 1);
        assert!(consumers.is_pass_through_for_test());
    }

    #[test]
    fn native_ordered_live_consumer_degradation_after_artifact_keeps_last_sound_predicate() {
        let order = contract(DataType::Int64, SortDirection::Ascending, NullOrder::Last);
        let (consumers, _, _) = live_fixture(vec![
            update(order, 1, 3, None),
            LivePollOutcome::Idle {
                latest_version: Some(LogicalVersion::FIRST),
                terminal: Some(LiveTerminal::DegradedDelivery(
                    UnavailableReason::RouteUnavailable,
                )),
            },
        ]);

        assert_eq!(
            values(consumers.poll_and_apply_chunk(chunk(&[2, 5])).unwrap()),
            vec![2]
        );
        assert_eq!(
            values(consumers.poll_and_apply_chunk(chunk(&[2, 5])).unwrap()),
            vec![2]
        );
        assert_eq!(
            consumers.terminal_for_test(),
            Some(LiveTerminal::DegradedDelivery(
                UnavailableReason::RouteUnavailable
            ))
        );
        assert_eq!(consumers.last_seen_for_test(), Some(LogicalVersion::FIRST));
    }

    #[test]
    fn native_ordered_live_consumer_build_rejects_blocking_or_non_ordered_specs() {
        let order = contract(DataType::Int64, SortDirection::Ascending, NullOrder::Last);
        let mut blocking_arena = ExprArena::default();
        let blocking = ordered_spec(
            &mut blocking_arena,
            &order,
            ConsumerActivation::BlockingSnapshot,
        );
        assert!(
            NativeOrderedLiveConsumerSet::from_plan(&[blocking], Arc::new(blocking_arena))
                .err()
                .expect("blocking ordered consumer must fail")
                .contains("NonBlockingLive")
        );

        let mut membership_arena = ExprArena::default();
        let expr_id =
            membership_arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int64);
        let schema =
            ArtifactMembershipSchema::new(&DataType::Int64, NullSemantics::NeverMatches).unwrap();
        let membership = RuntimeFilterConsumerBinding {
            binding_id: 2,
            channel_id: 1,
            expr_id,
            activation: ConsumerActivation::NonBlockingLive {
                late_apply: LateApplyGranularity::Batch,
            },
            capabilities: BTreeSet::from([ArtifactCapability::Membership]),
            contract: RuntimeFilterExecutionContract::Membership {
                canonical_schema: Arc::from(schema.canonical_bytes()),
                schema_digest: schema.digest().bytes(),
            },
            reduction: RuntimeFilterExecutionReduction::SetUnion,
        };
        assert!(
            NativeOrderedLiveConsumerSet::from_plan(&[membership], Arc::new(membership_arena))
                .err()
                .expect("membership live consumer must fail")
                .contains("Ordered")
        );
    }

    #[test]
    fn native_join_consumer_rejects_ordered_contract_even_when_batch_live() {
        let order = contract(DataType::Int64, SortDirection::Ascending, NullOrder::Last);
        let mut arena = ExprArena::default();
        let ordered = ordered_spec(
            &mut arena,
            &order,
            ConsumerActivation::NonBlockingLive {
                late_apply: LateApplyGranularity::Batch,
            },
        );

        assert!(
            RuntimeFilterConsumerSet::from_plan(&[ordered], Arc::new(arena))
                .err()
                .expect("ordered live spec must not enter Join consumer")
                .contains("unsupported artifact capability profile")
        );
    }
}

#[cfg(test)]
pub(crate) mod tests_support {
    use std::collections::BTreeSet;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use arrow::array::{ArrayRef, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};

    use crate::common::ids::SlotId;
    use crate::exec::chunk::{Chunk, ChunkSchema};
    use crate::exec::expr::{ExprArena, ExprNode};
    use crate::exec::node::runtime_filter::{
        RuntimeFilterConsumerBinding, RuntimeFilterExecutionContract,
        RuntimeFilterExecutionReduction,
    };
    use crate::exec::operators::runtime_filter::RuntimeFilterConsumerSet;
    use crate::runtime_filter::materializer::codec::{
        build_membership_index, encode_membership_leaf, inspect_membership_index,
    };
    use crate::runtime_filter::model::contract::{
        ArtifactCapability, ChannelId, ConsumerActivation, NullSemantics,
    };
    use crate::runtime_filter::port::artifact::{
        ArtifactBundle, ArtifactKind, ArtifactMembershipSchema, ConsumerArtifactProfile,
        PhysicalArtifact,
    };
    use crate::runtime_filter::port::identity::LogicalVersion;
    use crate::runtime_filter::port::subscription::{
        ArtifactAcquireOutcome, BlockingSnapshotSubscription,
    };
    use crate::runtime_filter::port::value_domain::{MembershipValues, ReducedMembershipDomain};

    pub(crate) fn chunk(values: &[i32]) -> Chunk {
        let schema = Schema::new(vec![Field::new("v", DataType::Int32, true)]);
        Chunk::try_new_with_columns(
            ChunkSchema::try_ref_from_schema_and_slot_ids(&schema, &[SlotId::new(1)]).unwrap(),
            vec![Arc::new(Int32Array::from(values.to_vec())) as ArrayRef],
        )
        .unwrap()
    }

    pub(crate) fn membership_bundle(values: &[i32]) -> Arc<ArtifactBundle> {
        membership_bundle_with_version(values, LogicalVersion::FIRST)
    }

    pub(crate) fn membership_bundle_with_version(
        values: &[i32],
        version: LogicalVersion,
    ) -> Arc<ArtifactBundle> {
        let null_semantics = NullSemantics::NeverMatches;
        let domain =
            ReducedMembershipDomain::new(MembershipValues::int32(values.iter().copied()), false);
        let encoded = encode_membership_leaf(&domain, null_semantics, version).unwrap();
        let kind = ArtifactKind::from_tag(encoded[6]).unwrap();
        let schema = ArtifactMembershipSchema::new(&DataType::Int32, null_semantics).unwrap();
        let plan = inspect_membership_index(&encoded).unwrap();
        let index = build_membership_index(&encoded, &plan).unwrap();
        let artifact = Arc::new(
            PhysicalArtifact::new_indexed_test(
                kind,
                schema.digest(),
                version,
                false,
                encoded.into(),
                index,
            )
            .unwrap(),
        );
        let profile = ConsumerArtifactProfile::new(
            BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
            None,
        )
        .unwrap();
        Arc::new(
            ArtifactBundle::new(
                ChannelId::new(7),
                version,
                &profile,
                vec![(kind, artifact)],
                usize::MAX,
            )
            .unwrap(),
        )
    }

    struct PublishedSubscription(Arc<ArtifactBundle>);

    impl BlockingSnapshotSubscription for PublishedSubscription {
        fn acquire(&self, _timeout: Duration) -> ArtifactAcquireOutcome {
            ArtifactAcquireOutcome::Published(Arc::clone(&self.0))
        }

        fn snapshot(&self) -> Option<Arc<ArtifactBundle>> {
            Some(Arc::clone(&self.0))
        }
    }

    pub(crate) struct AcquireObserver {
        calls: AtomicUsize,
        thread: Mutex<Option<std::thread::ThreadId>>,
    }

    impl AcquireObserver {
        pub(crate) fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }

        pub(crate) fn thread(&self) -> Option<std::thread::ThreadId> {
            *self.thread.lock().expect("acquire observer lock")
        }
    }

    struct ObservedPublishedSubscription {
        bundle: Arc<ArtifactBundle>,
        observer: Arc<AcquireObserver>,
    }

    impl BlockingSnapshotSubscription for ObservedPublishedSubscription {
        fn acquire(&self, _timeout: Duration) -> ArtifactAcquireOutcome {
            self.observer.calls.fetch_add(1, Ordering::SeqCst);
            *self.observer.thread.lock().expect("acquire observer lock") =
                Some(std::thread::current().id());
            ArtifactAcquireOutcome::Published(Arc::clone(&self.bundle))
        }

        fn snapshot(&self) -> Option<Arc<ArtifactBundle>> {
            Some(Arc::clone(&self.bundle))
        }
    }

    pub(crate) fn published_consumer_set(
        bundle: Arc<ArtifactBundle>,
    ) -> (RuntimeFilterConsumerSet, Arc<ExprArena>) {
        published_consumer_set_for(bundle, DataType::Int32)
    }

    pub(crate) fn observed_published_consumer_set(
        bundle: Arc<ArtifactBundle>,
    ) -> (
        RuntimeFilterConsumerSet,
        Arc<ExprArena>,
        Arc<AcquireObserver>,
    ) {
        let observer = Arc::new(AcquireObserver {
            calls: AtomicUsize::new(0),
            thread: Mutex::new(None),
        });
        let mut arena = ExprArena::default();
        let expr_id = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let arena = Arc::new(arena);
        let schema =
            ArtifactMembershipSchema::new(&DataType::Int32, NullSemantics::NeverMatches).unwrap();
        let spec = RuntimeFilterConsumerBinding {
            binding_id: 11,
            channel_id: 7,
            expr_id,
            activation: ConsumerActivation::BlockingSnapshot,
            capabilities: BTreeSet::from([
                ArtifactCapability::Membership,
                ArtifactCapability::EmptyDomain,
            ]),
            contract: RuntimeFilterExecutionContract::Membership {
                canonical_schema: Arc::from(schema.canonical_bytes()),
                schema_digest: schema.digest().bytes(),
            },
            reduction: RuntimeFilterExecutionReduction::SetUnion,
        };
        let subscription: Arc<dyn BlockingSnapshotSubscription> =
            Arc::new(ObservedPublishedSubscription {
                bundle,
                observer: Arc::clone(&observer),
            });
        (
            RuntimeFilterConsumerSet::from_bound_for_test(
                vec![spec],
                Arc::clone(&arena),
                vec![subscription],
            ),
            arena,
            observer,
        )
    }

    pub(crate) fn utf8_membership_bundle(values: &[&str]) -> Arc<ArtifactBundle> {
        membership_bundle_for(MembershipValues::utf8_set(
            values.iter().map(|value| (*value).to_string()).collect(),
        ))
    }

    pub(crate) fn published_utf8_consumer_set(
        bundle: Arc<ArtifactBundle>,
    ) -> (RuntimeFilterConsumerSet, Arc<ExprArena>) {
        published_consumer_set_for(bundle, DataType::Utf8)
    }

    fn published_consumer_set_for(
        bundle: Arc<ArtifactBundle>,
        data_type: DataType,
    ) -> (RuntimeFilterConsumerSet, Arc<ExprArena>) {
        let mut arena = ExprArena::default();
        let expr_id = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), data_type.clone());
        let arena = Arc::new(arena);
        let schema =
            ArtifactMembershipSchema::new(&data_type, NullSemantics::NeverMatches).unwrap();
        let spec = RuntimeFilterConsumerBinding {
            binding_id: 11,
            channel_id: 7,
            expr_id,
            activation: ConsumerActivation::BlockingSnapshot,
            capabilities: BTreeSet::from([
                ArtifactCapability::Membership,
                ArtifactCapability::EmptyDomain,
            ]),
            contract: RuntimeFilterExecutionContract::Membership {
                canonical_schema: Arc::from(schema.canonical_bytes()),
                schema_digest: schema.digest().bytes(),
            },
            reduction: RuntimeFilterExecutionReduction::SetUnion,
        };
        let subscription: Arc<dyn BlockingSnapshotSubscription> =
            Arc::new(PublishedSubscription(bundle));
        (
            RuntimeFilterConsumerSet::from_bound_for_test(
                vec![spec],
                Arc::clone(&arena),
                vec![subscription],
            ),
            arena,
        )
    }

    fn membership_bundle_for(values: MembershipValues) -> Arc<ArtifactBundle> {
        let null_semantics = NullSemantics::NeverMatches;
        let version = LogicalVersion::FIRST;
        let data_type = values.data_type();
        let domain = ReducedMembershipDomain::new(values, false);
        let encoded = encode_membership_leaf(&domain, null_semantics, version).unwrap();
        let kind = ArtifactKind::from_tag(encoded[6]).unwrap();
        let schema = ArtifactMembershipSchema::new(&data_type, null_semantics).unwrap();
        let plan = inspect_membership_index(&encoded).unwrap();
        let index = build_membership_index(&encoded, &plan).unwrap();
        let artifact = Arc::new(
            PhysicalArtifact::new_indexed_test(
                kind,
                schema.digest(),
                version,
                false,
                encoded.into(),
                index,
            )
            .unwrap(),
        );
        let profile = ConsumerArtifactProfile::new(
            BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
            None,
        )
        .unwrap();
        Arc::new(
            ArtifactBundle::new(
                ChannelId::new(7),
                version,
                &profile,
                vec![(kind, artifact)],
                usize::MAX,
            )
            .unwrap(),
        )
    }
}
