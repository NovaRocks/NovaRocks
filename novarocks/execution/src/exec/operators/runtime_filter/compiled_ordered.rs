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

//! One compiled scan driver's live ordered bindings. Frozen specifications
//! have one paid whole owner; each driver's mutable cursors are inline tracked
//! slots. Polling, key evaluation and application stay synchronous and ordered.

use super::live_cursor::{LiveCursorDisposition, LiveCursorViolation, apply_poll_outcome};
use crate::exec::chunk::Chunk;
use crate::exec::expr::compiled_program::CompiledExpressionInstance;
use crate::exec::hash_table::allocation::AggregateVec;
use crate::exec::operators::compiled_expression::{
    RuntimeKernelControl, evaluate_chunk_all, root_value_type,
};
use crate::exec::pipeline::operator::InputConstructionFactory;
use crate::runtime::runtime_state::RuntimeState;
use crate::runtime::task::io::{TaskEvent, TaskEventSink};
use crate::runtime::task::{ExecutionFailure, ExecutionResult};
use crate::runtime_filter::{self as execution, OrderedTopNAllocationHost};
use novarocks_functions::{
    KernelDiagnostic, KernelEvaluationControl, KernelFailure, SourceBackingOwner,
};
use novarocks_local_program::{
    FilterConsumerActivation, FilterLateApplyGranularity, FilterReduction, LocalProgram,
    ProgramExpressionRootSite, ProgramNodeExpressionRole, ProgramNodeId, ProgramNodeKind,
    StaticFilterContract,
};
use std::sync::Arc;

fn invalid(detail: &'static str) -> ExecutionFailure {
    KernelFailure::InvalidProgram(KernelDiagnostic::new(detail)).into()
}
fn contract_failure(detail: impl Into<Arc<str>>) -> ExecutionFailure {
    execution::RuntimeFilterContractViolation::new(
        execution::RuntimeFilterContractViolationKind::ContractMismatch,
        detail,
    )
    .into()
}

struct OrderedSpecification {
    site: ProgramExpressionRootSite,
    contract: execution::RuntimeFilterConsumerContract,
}
struct OrderedSpecifications {
    bindings: AggregateVec<OrderedSpecification>,
}
#[derive(Clone)]
pub(crate) struct CompiledOrderedScanFactory {
    program: Arc<LocalProgram>,
    specifications: SourceBackingOwner<OrderedSpecifications>,
    session: execution::RuntimeFilterSessionRef,
    host: OrderedTopNAllocationHost,
    control: RuntimeKernelControl,
}
impl CompiledOrderedScanFactory {
    pub(crate) fn validate_binding(
        binding: &novarocks_local_program::StaticFilterConsumer,
    ) -> ExecutionResult<()> {
        if !matches!(binding.contract(), StaticFilterContract::Ordered { .. }) {
            return Err(invalid(
                "compiled ordered Scan requires its frozen ordered contract",
            ));
        }
        if binding.reduction() != FilterReduction::TightenOrderedBound {
            return Err(invalid(
                "compiled ordered Scan does not support this frozen reduction; TightenOrderedBound is required",
            ));
        }
        if binding.activation()
            != (FilterConsumerActivation::NonBlockingLive {
                late_apply: FilterLateApplyGranularity::Batch,
            })
        {
            return Err(invalid(
                "compiled ordered Scan requires NonBlockingLive Batch activation",
            ));
        }
        Ok(())
    }
    pub(crate) fn try_new(
        program: Arc<LocalProgram>,
        node: ProgramNodeId,
        session: execution::RuntimeFilterSessionRef,
        host: &OrderedTopNAllocationHost,
        control: &RuntimeKernelControl,
    ) -> ExecutionResult<Self> {
        let Some(graph_node) = program.graph().nodes().get(node.index()) else {
            return Err(invalid("compiled ordered consumer Scan is absent"));
        };
        let ProgramNodeKind::Scan {
            runtime_filters, ..
        } = graph_node.kind()
        else {
            return Err(invalid(
                "compiled ordered consumer requires its actual Scan node",
            ));
        };
        let authority = novarocks_functions::opaque_memory::OpaqueRetainedCharge::try_new(
            host.scalar_allocator(),
        )?;
        let _prefix = authority.reserve_operation(
            novarocks_functions::source_custody_prefix_facts()
                .requests()
                .bytes(),
        )?;
        let mut work = novarocks_functions::EvaluationCheckpoints::new(control);
        work.flush()?;
        let mut count = 0usize;
        for binding in runtime_filters {
            work.step()?;
            if matches!(
                binding.consumer.contract(),
                StaticFilterContract::Ordered { .. }
            ) {
                Self::validate_binding(&binding.consumer)?;
                count = count
                    .checked_add(1)
                    .ok_or(KernelFailure::ResourceExhausted)?;
            }
        }
        work.finish()?;
        if count == 0 {
            return Err(invalid(
                "compiled ordered consumer Scan has no ordered binding",
            ));
        }
        let mut bindings = AggregateVec::new_in(host.vector_allocator());
        bindings
            .try_reserve_exact(count)
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        // The ordinal belongs to the full Scan list. Membership bindings may
        // precede or interleave these sites; no filtered subset is renumbered.
        for (ordinal, binding) in runtime_filters.iter().enumerate() {
            control.checkpoint(1)?;
            if !matches!(
                binding.consumer.contract(),
                StaticFilterContract::Ordered { .. }
            ) {
                continue;
            }
            let binding_ordinal =
                u32::try_from(ordinal).map_err(|_| KernelFailure::ResourceExhausted)?;
            let site = ProgramExpressionRootSite::Node {
                node,
                role: ProgramNodeExpressionRole::RuntimeFilter {
                    binding: binding_ordinal,
                },
            };
            let order = host.shared_order_contract(binding.consumer.contract(), control)?;
            let key = root_value_type(&program, site)?;
            let domain_matches = match order.keys() {
                [order_key] => match order_key.value_type() {
                    Some(expected) => {
                        key.same_value_domain_observed(expected, || control.checkpoint(1))?
                    }
                    None => false,
                },
                _ => false,
            };
            if !domain_matches {
                return Err(invalid(
                    "compiled ordered Scan key root differs from its complete frozen type",
                ));
            }
            let contract = execution::RuntimeFilterConsumerContract::ordered_live(
                execution::RuntimeFilterBindingId::new(binding.consumer.binding_id()),
                execution::RuntimeFilterChannelId::new(binding.consumer.channel_id()),
                execution::RuntimeFilterLateApplyGranularity::Batch,
                execution::RuntimeFilterExecutionContract::Ordered(order),
            )?;
            bindings.push(OrderedSpecification { site, contract });
        }
        let specifications = host.retain_shared(OrderedSpecifications { bindings }, control)?;
        Ok(Self {
            program,
            specifications,
            session,
            host: host.clone(),
            control: control.clone(),
        })
    }

    /// This infallible shell allocates no binding inventory or subscription.
    /// All fallible work happens in the source's original runtime bind phase.
    pub(crate) fn for_driver(&self) -> CompiledOrderedScanEvaluator {
        CompiledOrderedScanEvaluator {
            factory: self.clone(),
            control: self.control.clone(),
            bindings: AggregateVec::new_in(self.host.vector_allocator()),
            bound: false,
            failed: None,
        }
    }
}

enum OrderedBindingState {
    Unbound,
    Live {
        subscription: Arc<dyn execution::NonBlockingLiveSubscription>,
        observed: Option<execution::LogicalVersion>,
        latest_snapshot: Option<Arc<execution::RuntimeFilterSnapshot>>,
        terminal: Option<execution::LiveTerminal>,
    },
    PassThrough,
}
struct OrderedBinding {
    state: OrderedBindingState,
    instance: Option<CompiledExpressionInstance>,
}

pub(crate) struct CompiledOrderedScanEvaluator {
    factory: CompiledOrderedScanFactory,
    control: RuntimeKernelControl,
    bindings: AggregateVec<OrderedBinding>,
    bound: bool,
    failed: Option<ExecutionFailure>,
}
impl CompiledOrderedScanEvaluator {
    pub(crate) fn bind_mem_tracker(
        &mut self,
        tracker: Arc<crate::runtime::mem_tracker::MemTracker>,
    ) {
        self.control.bind_mem_tracker(tracker);
    }
    pub(crate) fn bind_construction_factory(&mut self, factory: Arc<dyn InputConstructionFactory>) {
        self.control.bind_construction_factory(factory);
    }
    fn latch<T>(&mut self, result: ExecutionResult<T>) -> ExecutionResult<T> {
        match result {
            Ok(value) => Ok(value),
            Err(cause) => {
                if self.failed.is_none() {
                    self.failed = Some(cause);
                    self.bindings.clear();
                }
                Err(self
                    .failed
                    .as_ref()
                    .expect("first ordered consumer failure")
                    .clone())
            }
        }
    }
    pub(crate) fn bind(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        if let Some(cause) = &self.failed {
            return Err(cause.clone());
        }
        if self.bound {
            return Ok(());
        }
        self.control.bind_runtime_memory(state);
        let result = self.bind_inner();
        self.latch(result)
    }
    fn bind_inner(&mut self) -> ExecutionResult<()> {
        self.control.checkpoint(0)?;
        let specifications = &self.factory.specifications.borrowed().bindings;
        self.bindings
            .try_reserve_exact(specifications.len())
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        for _ in specifications.iter() {
            self.control.checkpoint(1)?;
            self.bindings.push(OrderedBinding {
                state: OrderedBindingState::Unbound,
                instance: None,
            });
        }
        // Every slot exists before the first service call. A later subscribe
        // failure destroys the exact earlier handles through the first-cause
        // latch; no partial driver is eligible to read input.
        for (binding, specification) in self.bindings.iter_mut().zip(specifications.iter()) {
            self.control.checkpoint(1)?;
            let outcome =
                self.factory
                    .session
                    .subscribe(execution::RuntimeFilterSubscriptionRequest::new(
                        specification.contract.clone(),
                    ));
            binding.state = match outcome {
                Ok(execution::RuntimeFilterBindOutcome::Bound(
                    execution::RuntimeFilterSubscriptionPin::Live(subscription),
                )) => OrderedBindingState::Live {
                    subscription,
                    observed: None,
                    latest_snapshot: None,
                    terminal: None,
                },
                Ok(execution::RuntimeFilterBindOutcome::Unavailable(_)) => {
                    OrderedBindingState::PassThrough
                }
                Ok(_) => {
                    return Err(contract_failure(
                        "compiled ordered Scan received a non-live subscription",
                    ));
                }
                Err(cause)
                    if cause.kind()
                        == execution::RuntimeFilterContractViolationKind::SessionClosed =>
                {
                    OrderedBindingState::PassThrough
                }
                Err(cause) => return Err(cause.into()),
            };
        }
        self.control.checkpoint(0)?;
        self.bound = true;
        Ok(())
    }
    fn poll(&mut self) -> ExecutionResult<()> {
        if !self.bound {
            return Err(invalid("compiled ordered Scan must bind before polling"));
        }
        for (binding, specification) in self
            .bindings
            .iter_mut()
            .zip(self.factory.specifications.borrowed().bindings.iter())
        {
            self.control.checkpoint(1)?;
            let OrderedBindingState::Live {
                subscription,
                observed,
                terminal: None,
                ..
            } = &binding.state
            else {
                continue;
            };
            let outcome = subscription.poll_after(*observed);
            if let execution::LivePollOutcome::Updated { snapshot, .. } = &outcome {
                let execution::RuntimeFilterExecutionContract::Ordered(order) =
                    specification.contract.contract()
                else {
                    return Err(invalid(
                        "compiled ordered Scan lost its frozen ordered contract",
                    ));
                };
                if snapshot.binding_id() != specification.contract.binding_id()
                    || snapshot.contract_digest() != order.digest()
                {
                    return Err(contract_failure(
                        "compiled ordered Scan snapshot differs from its frozen binding or contract",
                    ));
                }
            }
            let OrderedBindingState::Live {
                observed,
                latest_snapshot,
                terminal,
                ..
            } = &mut binding.state
            else {
                unreachable!("polled original live state");
            };
            let disposition = apply_poll_outcome(observed, latest_snapshot, terminal, outcome)
                .map_err(|cause| {
                    let detail = match cause {
                        LiveCursorViolation::NewerVersionWithoutArtifact => format!(
                            "native ordered runtime-filter binding_id={} reported a newer live version without artifact", specification.contract.binding_id().get()),
                        LiveCursorViolation::Regressed => format!(
                            "native ordered runtime-filter binding_id={} live cursor regressed", specification.contract.binding_id().get()),
                    };
                    contract_failure(detail)
                })?;
            if disposition == LiveCursorDisposition::PassThrough {
                binding.state = OrderedBindingState::PassThrough;
            }
        }
        Ok(())
    }
    pub(crate) fn apply(
        &mut self,
        chunk: Chunk,
        event_sink: &Arc<dyn TaskEventSink>,
    ) -> ExecutionResult<Option<Chunk>> {
        if let Some(cause) = &self.failed {
            return Err(cause.clone());
        }
        let result = self.apply_inner(chunk, event_sink);
        self.latch(result)
    }
    fn apply_inner(
        &mut self,
        chunk: Chunk,
        event_sink: &Arc<dyn TaskEventSink>,
    ) -> ExecutionResult<Option<Chunk>> {
        self.poll()?;
        if !chunk.chunk_schema().is_typed() || !chunk.source_authority().is_attached() {
            return Err(invalid(
                "compiled ordered Scan requires its actual typed source authority",
            ));
        }
        if !self.bindings.iter().any(|binding| {
            matches!(
                binding.state,
                OrderedBindingState::Live {
                    latest_snapshot: Some(_),
                    ..
                }
            )
        }) {
            return Ok(Some(chunk));
        }
        let mut effects = AggregateVec::new_in(self.factory.host.vector_allocator());
        effects
            .try_reserve_exact(self.bindings.len())
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        let mut current = Some(chunk);
        for (binding, specification) in self
            .bindings
            .iter_mut()
            .zip(self.factory.specifications.borrowed().bindings.iter())
        {
            let Some(input) = current.take() else {
                break;
            };
            self.control.checkpoint(1)?;
            let OrderedBindingState::Live {
                latest_snapshot: Some(snapshot),
                ..
            } = &binding.state
            else {
                current = Some(input);
                continue;
            };
            if binding.instance.is_none() {
                binding.instance = Some(CompiledExpressionInstance::try_new_with_allocator(
                    Arc::clone(&self.factory.program),
                    specification.site,
                    &self.control,
                    self.control.allocator(),
                )?);
            }
            let value = evaluate_chunk_all(
                binding.instance.as_mut().expect("active ordered key"),
                specification.site,
                &input,
                &self.control,
            )?;
            let outcome = execution::evaluator::evaluate_rows_controlled(
                snapshot.binding_id(),
                snapshot.logical_version(),
                snapshot.artifact_query().as_ref(),
                &value,
                self.factory.host.scalar_allocator(),
                &self.control,
            )?;
            let execution::RuntimeFilterRowEvaluation::Evaluated { mask, .. } =
                outcome.outcome().evaluation()
            else {
                current = Some(input);
                continue;
            };
            let effect = outcome
                .effect_controlled(&self.control)?
                .ok_or_else(|| invalid("evaluated ordered mask has no effect"))?;
            let owner = self.control.construction_owner(input.chunk_schema())?;
            current = input.filter_ordered_scan_controlled(
                mask,
                self.factory.host.scalar_allocator(),
                &self.control,
                owner.as_ref(),
            )?;
            effects.push(effect);
            // The whole controlled outcome (and its actual mask fee) remains
            // alive throughout synchronous filtering; no naked mask escapes.
        }
        for effect in effects {
            event_sink.record(TaskEvent::RuntimeFilterRowEffect(effect));
        }
        Ok(current)
    }
}
