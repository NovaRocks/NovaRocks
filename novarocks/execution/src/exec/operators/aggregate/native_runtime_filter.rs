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

use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::exec::hash_table::allocation::AggregateVec;
use crate::exec::operators::aggregate::topn_boundary::OwnedAggregateTopNBoundaryState;
use crate::runtime::task::{ExecutionFailure, ExecutionResult};
use crate::runtime_filter as execution;
use crate::runtime_filter::RuntimeFilterExecutionContract;
use crate::runtime_filter::contribution::ContributionCodecError;
use crate::runtime_filter::{OrderedTopNAllocationHost, OwnedOrderKey};
use novarocks_functions::{KernelEvaluationControl, KernelFailure, SourceBackingOwner};

#[derive(Default)]
struct AggregateTopNProducerInstanceCoordinator {
    failed: AtomicBool,
}

#[derive(Clone)]
enum Coordinator {
    Owned(SourceBackingOwner<AggregateTopNProducerInstanceCoordinator>),
}
impl Coordinator {
    fn failed(&self) -> &AtomicBool {
        match self {
            Self::Owned(value) => &value.borrowed().failed,
        }
    }
}

/// The owned path retains the original domain source. Operator
/// context is added only at the execution boundary.
enum SessionFailure {
    Definition(execution::RuntimeFilterContractViolation),
    Contract {
        binding: u32,
        operation: &'static str,
        cause: execution::RuntimeFilterContractViolation,
    },
    Codec {
        binding: u32,
        cause: ContributionCodecError,
    },
    Execution(ExecutionFailure),
}
#[derive(Debug)]
struct AggregateTopNOperatorFailure<E> {
    binding: u32,
    operation: &'static str,
    cause: E,
}
impl<E: std::fmt::Display> std::fmt::Display for AggregateTopNOperatorFailure<E> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "aggregate TopN runtime-filter binding {} failed during {}: {}",
            self.binding, self.operation, self.cause
        )
    }
}
impl<E: std::error::Error + 'static> std::error::Error for AggregateTopNOperatorFailure<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.cause)
    }
}
impl<E: novarocks_failure_contract::FailureProjection> novarocks_failure_contract::FailureProjection
    for AggregateTopNOperatorFailure<E>
{
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        self.cause.failure_fact()
    }
}
impl SessionFailure {
    fn contract(detail: String) -> Self {
        Self::Definition(execution::RuntimeFilterContractViolation::new(
            execution::RuntimeFilterContractViolationKind::ContractMismatch,
            detail,
        ))
    }
    fn execution(self) -> ExecutionFailure {
        match self {
            Self::Definition(cause) => cause.into(),
            Self::Contract {
                binding,
                operation,
                cause,
            } => ExecutionFailure::from_domain(AggregateTopNOperatorFailure {
                binding,
                operation,
                cause,
            }),
            Self::Codec { binding, cause } => {
                ExecutionFailure::from_domain(AggregateTopNOperatorFailure {
                    binding,
                    operation: "encode ordered contribution",
                    cause,
                })
            }
            Self::Execution(cause) => cause,
        }
    }
}
impl From<KernelFailure> for SessionFailure {
    fn from(cause: KernelFailure) -> Self {
        Self::Execution(cause.into())
    }
}
fn checkpoint(control: Option<&dyn KernelEvaluationControl>) -> Result<(), SessionFailure> {
    if let Some(control) = control {
        control.checkpoint(1)?;
    }
    Ok(())
}

#[derive(Clone)]
struct AggregateTopNProducerBinding {
    binding_id: u32,
    execution_contract: execution::RuntimeFilterProducerContract,
    session: execution::RuntimeFilterSessionRef,
    coordinator: Coordinator,
}
impl AggregateTopNProducerBinding {
    fn from_parts(
        binding_id: u32,
        execution_contract: execution::RuntimeFilterProducerContract,
        session: execution::RuntimeFilterSessionRef,
        coordinator: impl FnOnce() -> Result<Coordinator, SessionFailure>,
    ) -> Result<Self, SessionFailure> {
        if !matches!(
            execution_contract.contract(),
            RuntimeFilterExecutionContract::Ordered(_)
        ) {
            return Err(SessionFailure::contract(format!(
                "native aggregate TopN producer binding_id={binding_id} requires an ordered contract"
            )));
        }
        if execution_contract.kind() != execution::RuntimeFilterProducerKind::OrderedBound {
            return Err(SessionFailure::contract(format!(
                "native aggregate TopN producer binding_id={binding_id} requires an ordered-bound producer contract"
            )));
        }
        Ok(Self {
            binding_id,
            execution_contract,
            session,
            coordinator: coordinator()?,
        })
    }
}

struct OwnedBindings {
    bindings: AggregateVec<AggregateTopNProducerBinding>,
}
enum BindingStorage {
    Owned(SourceBackingOwner<OwnedBindings>),
}
impl Deref for BindingStorage {
    type Target = [AggregateTopNProducerBinding];
    fn deref(&self) -> &Self::Target {
        match self {
            Self::Owned(owner) => &owner.borrowed().bindings,
        }
    }
}

pub(crate) struct AggregateTopNProducerSessionFactory {
    bindings: BindingStorage,
    local_partition_count: u32,
    host: Option<OrderedTopNAllocationHost>,
}
impl std::fmt::Debug for AggregateTopNProducerSessionFactory {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AggregateTopNProducerSessionFactory")
            .field("binding_count", &self.bindings.len())
            .field("local_partition_count", &self.local_partition_count)
            .finish()
    }
}
impl AggregateTopNProducerSessionFactory {
    pub(crate) fn try_from_compiled(
        specs: &[novarocks_local_program::AggregateTopNFilter],
        session: execution::RuntimeFilterSessionRef,
        actual_dop: i32,
        host: &OrderedTopNAllocationHost,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<Self> {
        let local_partition_count = validate_partition_count(actual_dop)?;
        control.checkpoint(0)?;
        let mut bindings = AggregateVec::new_in(host.vector_allocator());
        bindings
            .try_reserve_exact(specs.len())
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        for spec in specs {
            control.checkpoint(1)?;
            if spec.producer.kind() != novarocks_local_program::FilterProducerKind::OrderedBound
                || spec.producer.reduction()
                    != novarocks_local_program::FilterReduction::TightenOrderedBound
            {
                return Err(execution::RuntimeFilterContractViolation::new(
                    execution::RuntimeFilterContractViolationKind::ContractMismatch,
                    "compiled aggregate TopN requires an ordered-bound tightening producer",
                )
                .into());
            }

            let order = host.shared_order_contract(spec.producer.contract(), control)?;
            let contract = execution::RuntimeFilterProducerContract::ordered_bound(
                execution::RuntimeFilterBindingId::new(spec.producer.binding_id()),
                execution::RuntimeFilterChannelId::new(spec.producer.channel_id()),
                RuntimeFilterExecutionContract::Ordered(order),
            )?;
            bindings.push(
                AggregateTopNProducerBinding::from_parts(
                    spec.producer.binding_id(),
                    contract,
                    Arc::clone(&session),
                    || {
                        Ok(Coordinator::Owned(host.retain_shared(
                            AggregateTopNProducerInstanceCoordinator::default(),
                            control,
                        )?))
                    },
                )
                .map_err(SessionFailure::execution)?,
            );
        }
        let bindings = host.retain_shared(OwnedBindings { bindings }, control)?;
        Ok(Self {
            bindings: BindingStorage::Owned(bindings),
            local_partition_count,
            host: Some(host.clone()),
        })
    }

    #[cfg(test)]
    pub(crate) const fn local_partition_count(&self) -> u32 {
        self.local_partition_count
    }

    /// Borrow the same whole frozen order used by this factory's producer.
    pub(crate) fn order_contract(
        &self,
        index: usize,
    ) -> Option<&execution::RuntimeOrderContractRef> {
        self.bindings
            .get(index)
            .and_then(|binding| match binding.execution_contract.contract() {
                RuntimeFilterExecutionContract::Ordered(order) => Some(order),
                _ => None,
            })
    }
    pub(crate) fn create_for_driver_owned(
        &self,
        actual_dop: i32,
        local_index: i32,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<AggregateTopNProducerSession> {
        let partition = self.driver_partition(actual_dop, local_index)?;
        let host = self.host.as_ref().ok_or_else(|| {
            SessionFailure::contract("compiled aggregate TopN producer has no creating host".into())
                .execution()
        })?;
        control.checkpoint(0)?;
        let mut streams = AggregateVec::new_in(host.vector_allocator());
        // Every stream slot is admitted before any producer can be opened.
        streams
            .try_reserve_exact(self.bindings.len())
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        for binding in self.bindings.iter() {
            control.checkpoint(1)?;
            streams.push(AggregateTopNProducerStream::new(
                binding.clone(),
                partition,
                self.local_partition_count,
            ));
        }
        control.checkpoint(0)?;
        Ok(AggregateTopNProducerSession {
            streams: StreamStorage::Owned(streams),
            completed: false,
            host: Some(host.clone()),
        })
    }
    fn driver_partition(
        &self,
        actual_dop: i32,
        local_index: i32,
    ) -> ExecutionResult<execution::PartitionId> {
        let actual_dop = validate_partition_count(actual_dop)?;
        if actual_dop != self.local_partition_count {
            return Err(topn_producer_partition(format!(
                "native aggregate TopN producer DOP drifted between factory build and operator creation: expected={} actual={actual_dop}",
                self.local_partition_count
            )));
        }
        let local_index = u32::try_from(local_index).map_err(|_| topn_producer_partition(format!(
            "native aggregate TopN producer local index {local_index} cannot be represented as a partition id"
        )))?;
        if local_index >= self.local_partition_count {
            return Err(topn_producer_partition(format!(
                "native aggregate TopN producer local index {local_index} is outside DOP {}",
                self.local_partition_count
            )));
        }
        Ok(execution::PartitionId::new(local_index))
    }
}
fn topn_producer_partition(detail: String) -> ExecutionFailure {
    execution::RuntimeFilterContractViolation::new(
        execution::RuntimeFilterContractViolationKind::InvalidPartitionCount,
        detail,
    )
    .into()
}
fn validate_partition_count(local_partition_count: i32) -> ExecutionResult<u32> {
    let local_partition_count = u32::try_from(local_partition_count).map_err(|_| {
        topn_producer_partition(format!(
            "native aggregate TopN producer DOP {local_partition_count} cannot be represented as a partition count"
        ))
    })?;
    if local_partition_count == 0 {
        return Err(topn_producer_partition(
            "native aggregate TopN producer DOP must be positive".to_string(),
        ));
    }
    Ok(local_partition_count)
}

enum StreamStorage {
    Owned(AggregateVec<AggregateTopNProducerStream>),
}
impl Deref for StreamStorage {
    type Target = [AggregateTopNProducerStream];
    fn deref(&self) -> &Self::Target {
        match self {
            Self::Owned(values) => values,
        }
    }
}
impl DerefMut for StreamStorage {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Self::Owned(values) => values,
        }
    }
}
pub(crate) struct AggregateTopNProducerSession {
    streams: StreamStorage,
    completed: bool,
    host: Option<OrderedTopNAllocationHost>,
}
impl AggregateTopNProducerSession {
    pub(crate) fn bind_owned(
        &mut self,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<()> {
        let result = self.bind_core(Some(control));
        self.finish_owned_attempt(result)
    }
    fn bind_core(
        &mut self,
        control: Option<&dyn KernelEvaluationControl>,
    ) -> Result<(), SessionFailure> {
        for index in 0..self.streams.len() {
            checkpoint(control)?;
            if let Err(cause) = self.streams[index].bind() {
                let _ =
                    self.fail_incomplete(execution::RuntimeFilterProducerFailure::ExecutionFailed);
                return Err(cause);
            }
        }
        Ok(())
    }
    pub(crate) fn submit_pending_owned(
        &mut self,
        boundaries: &mut [OwnedAggregateTopNBoundaryState],
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<()> {
        self.update_owned(boundaries, false, control)
    }
    pub(crate) fn finish_owned(
        &mut self,
        boundaries: &mut [OwnedAggregateTopNBoundaryState],
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<()> {
        self.update_owned(boundaries, true, control)
    }
    fn update_owned(
        &mut self,
        boundaries: &mut [OwnedAggregateTopNBoundaryState],
        finish: bool,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<()> {
        let host = self.host.clone().ok_or_else(|| {
            SessionFailure::contract("compiled aggregate TopN session has no creating host".into())
                .execution()
        })?;
        let result = self.update(
            boundaries,
            finish,
            Some(control),
            |boundary, finish| {
                if finish {
                    boundary.finish_controlled(control)
                } else {
                    boundary.take_pending_controlled(control)
                }
                .map_err(|cause| SessionFailure::Execution(cause.into()))
            },
            |contract, key: OwnedOrderKey, cap| {
                let RuntimeFilterExecutionContract::Ordered(order) = contract.contract() else {
                    return Err(SessionFailure::Codec {
                        binding: contract.binding_id().get(),
                        cause: ContributionCodecError::SchemaMismatch,
                    });
                };
                host.ordered_contribution(key.borrowed(), order, cap, control)
                    .map_err(|cause| SessionFailure::Execution(cause.into()))
            },
        );
        self.finish_owned_attempt(result)
    }
    /// This is the owned final-flush/submission/close chronology.
    fn update<B, K>(
        &mut self,
        boundaries: &mut [B],
        finish: bool,
        control: Option<&dyn KernelEvaluationControl>,
        mut pending: impl FnMut(&mut B, bool) -> Result<Option<K>, SessionFailure>,
        mut encode: impl FnMut(
            &execution::RuntimeFilterProducerContract,
            K,
            usize,
        ) -> Result<execution::RuntimeFilterContribution, SessionFailure>,
    ) -> Result<(), SessionFailure> {
        if self.completed {
            return Ok(());
        }
        self.require_matching_boundaries(boundaries.len())?;
        for (index, boundary) in boundaries.iter_mut().enumerate() {
            checkpoint(control)?;
            let next = pending(boundary, finish)?;
            if let Some(bound) = next
                && let Err(cause) =
                    self.streams[index].submit(|contract, cap| encode(contract, bound, cap))
            {
                let _ =
                    self.fail_incomplete(execution::RuntimeFilterProducerFailure::ExecutionFailed);
                self.completed = true;
                return Err(cause);
            }
        }
        if finish {
            for index in 0..self.streams.len() {
                checkpoint(control)?;
                if let Err(cause) = self.streams[index].close() {
                    let _ = self
                        .fail_incomplete(execution::RuntimeFilterProducerFailure::ExecutionFailed);
                    self.completed = true;
                    return Err(cause);
                }
            }
            self.completed = true;
        }
        Ok(())
    }
    fn finish_owned_attempt(&mut self, result: Result<(), SessionFailure>) -> ExecutionResult<()> {
        match result {
            Ok(()) => Ok(()),
            Err(cause) => {
                if !self.completed {
                    let _ = self
                        .fail_incomplete(execution::RuntimeFilterProducerFailure::ExecutionFailed);
                    self.completed = true;
                }
                Err(cause.execution())
            }
        }
    }
    pub(crate) fn fail_owned(
        &mut self,
        reason: execution::RuntimeFilterProducerFailure,
    ) -> ExecutionResult<()> {
        self.fail_core(reason).map_err(SessionFailure::execution)
    }
    fn fail_core(
        &mut self,
        reason: execution::RuntimeFilterProducerFailure,
    ) -> Result<(), SessionFailure> {
        if self.completed {
            return Ok(());
        }
        let result = self.fail_incomplete(reason);
        self.completed = true;
        result
    }
    fn require_matching_boundaries(&mut self, boundaries: usize) -> Result<(), SessionFailure> {
        if self.streams.len() == boundaries {
            return Ok(());
        }
        let cause = SessionFailure::contract(format!(
            "native aggregate TopN producer session/boundary count mismatch: sessions={} boundaries={boundaries}",
            self.streams.len()
        ));
        let _ = self.fail_incomplete(execution::RuntimeFilterProducerFailure::ExecutionFailed);
        self.completed = true;
        Err(cause)
    }
    fn fail_incomplete(
        &mut self,
        reason: execution::RuntimeFilterProducerFailure,
    ) -> Result<(), SessionFailure> {
        let mut first = None;
        for stream in self.streams.iter_mut() {
            if let Err(cause) = stream.fail(reason)
                && first.is_none()
            {
                first = Some(cause);
            }
        }
        first.map_or(Ok(()), Err)
    }
}
impl Drop for AggregateTopNProducerSession {
    fn drop(&mut self) {
        if !self.completed {
            let _ = self.fail_incomplete(execution::RuntimeFilterProducerFailure::ExecutionFailed);
        }
    }
}

struct AggregateTopNProducerStream {
    binding: AggregateTopNProducerBinding,
    partition_id: execution::PartitionId,
    local_partition_count: u32,
    next_sequence: u64,
    terminal: bool,
    producer: Option<execution::SharedRuntimeFilterProducer>,
}
impl AggregateTopNProducerStream {
    fn new(
        binding: AggregateTopNProducerBinding,
        partition_id: execution::PartitionId,
        local_partition_count: u32,
    ) -> Self {
        Self {
            binding,
            partition_id,
            local_partition_count,
            next_sequence: 0,
            terminal: false,
            producer: None,
        }
    }
    fn error(
        &self,
        operation: &'static str,
        cause: execution::RuntimeFilterContractViolation,
    ) -> SessionFailure {
        SessionFailure::Contract {
            binding: self.binding.binding_id,
            operation,
            cause,
        }
    }
    fn bind(&mut self) -> Result<(), SessionFailure> {
        if self.producer.is_some() || self.terminal {
            return Ok(());
        }
        let request = execution::RuntimeFilterProducerOpenRequest::new(
            self.binding.execution_contract.clone(),
            self.local_partition_count,
        );
        match self.binding.session.open_producer(request) {
            Ok(execution::RuntimeFilterBindOutcome::Bound(producer)) => {
                self.producer = Some(producer);
                Ok(())
            }
            Ok(execution::RuntimeFilterBindOutcome::Unavailable(_)) => {
                self.mark_service_unavailable();
                Ok(())
            }
            Err(cause)
                if cause.kind() == execution::RuntimeFilterContractViolationKind::SessionClosed =>
            {
                self.mark_service_unavailable();
                Ok(())
            }
            Err(cause) => Err(self.error("open failed during operator bind", cause)),
        }
    }
    fn submit(
        &mut self,
        encode: impl FnOnce(
            &execution::RuntimeFilterProducerContract,
            usize,
        ) -> Result<execution::RuntimeFilterContribution, SessionFailure>,
    ) -> Result<(), SessionFailure> {
        if self.terminal || self.binding.coordinator.failed().load(Ordering::Acquire) {
            self.terminal = true;
            return Ok(());
        }
        let producer = self.producer.as_ref().ok_or_else(|| {
            SessionFailure::contract(format!(
                "native aggregate TopN producer binding_id={} was not bound before input",
                self.binding.binding_id
            ))
        })?;
        let contribution = encode(
            &self.binding.execution_contract,
            producer.max_contribution_bytes(),
        )?;
        match producer.submit(
            self.partition_id,
            execution::ProducerSequence::new(self.next_sequence),
            contribution,
        ) {
            Ok(execution::RuntimeFilterSubmitOutcome::TerminalNoop) => {
                self.binding
                    .coordinator
                    .failed()
                    .store(true, Ordering::Release);
                self.terminal = true;
                return Ok(());
            }
            Ok(_) => {}
            Err(cause)
                if cause.kind() == execution::RuntimeFilterContractViolationKind::SessionClosed =>
            {
                self.mark_service_unavailable();
                return Ok(());
            }
            Err(cause) => return Err(self.error("contribution failed", cause)),
        }
        self.next_sequence = self.next_sequence.checked_add(1).ok_or_else(|| {
            SessionFailure::Execution(execution::producer_sequence_extent(
                self.binding.binding_id,
                self.next_sequence,
            ))
        })?;
        Ok(())
    }
    fn close(&mut self) -> Result<(), SessionFailure> {
        if self.terminal || self.binding.coordinator.failed().load(Ordering::Acquire) {
            self.terminal = true;
            return Ok(());
        }
        let producer = self.producer.as_ref().ok_or_else(|| {
            SessionFailure::contract(format!(
                "native aggregate TopN producer binding_id={} was not bound before finish",
                self.binding.binding_id
            ))
        })?;
        match producer.close_partition(
            self.partition_id,
            execution::ProducerSequence::new(self.next_sequence),
        ) {
            Ok(execution::RuntimeFilterSubmitOutcome::TerminalNoop) => self
                .binding
                .coordinator
                .failed()
                .store(true, Ordering::Release),
            Ok(_) => {}
            Err(cause)
                if cause.kind() == execution::RuntimeFilterContractViolationKind::SessionClosed =>
            {
                self.mark_service_unavailable();
                return Ok(());
            }
            Err(cause) => return Err(self.error("close failed", cause)),
        }
        self.terminal = true;
        Ok(())
    }
    fn mark_service_unavailable(&mut self) {
        self.binding
            .coordinator
            .failed()
            .store(true, Ordering::Release);
        self.terminal = true;
    }
    fn fail(
        &mut self,
        reason: execution::RuntimeFilterProducerFailure,
    ) -> Result<(), SessionFailure> {
        self.terminal = true;
        // An owned driver can be destroyed after construction but before bind.
        // It has no service handle with which to settle a bound sibling, so it
        // must leave that sibling's once-fail right intact.
        if self.producer.is_none() {
            return Ok(());
        }
        if self
            .binding
            .coordinator
            .failed()
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Ok(());
        }
        let Some(producer) = self.producer.as_ref() else {
            return Ok(());
        };
        if let Err(cause) = producer.fail(reason)
            && cause.kind() != execution::RuntimeFilterContractViolationKind::SessionClosed
        {
            return Err(self.error("fail-open failed", cause));
        }
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::num::NonZeroU32;
    use std::sync::{Arc, Mutex};

    use arrow::datatypes::DataType;

    use super::{AggregateTopNProducerSessionFactory, SessionFailure, execution};

    #[test]
    fn failure_context_preserves_binding_operation_and_reason() {
        use novarocks_failure_contract::FailureProjection;

        let contract_cause = execution::RuntimeFilterContractViolation::new(
            execution::RuntimeFilterContractViolationKind::ContractMismatch,
            "producer contract mismatch",
        );
        let contract_fact = contract_cause.failure_fact();
        let contract_failure = SessionFailure::Contract {
            binding: 73,
            operation: "submit contribution",
            cause: contract_cause,
        }
        .execution();
        assert_eq!(contract_failure.failure_fact(), contract_fact);
        assert!(contract_failure.detail().contains("binding 73"));
        assert!(contract_failure.detail().contains("submit contribution"));

        let codec_cause =
            crate::runtime_filter::contribution::ContributionCodecError::SchemaMismatch;
        let codec_fact = codec_cause.failure_fact();
        let codec_failure = SessionFailure::Codec {
            binding: 74,
            cause: codec_cause,
        }
        .execution();
        assert_eq!(codec_failure.failure_fact(), codec_fact);
        assert!(codec_failure.detail().contains("binding 74"));
        assert!(
            codec_failure
                .detail()
                .contains("encode ordered contribution")
        );
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    enum Event {
        Open {
            binding_id: u32,
            local_partition_count: u32,
        },
        Submit {
            partition: u32,
            sequence: u64,
        },
        Close {
            partition: u32,
            sequence: u64,
        },
        Fail(execution::RuntimeFilterProducerFailure),
    }

    #[derive(Clone, Copy)]
    enum OpenAction {
        Bound,
        Unavailable,
        SessionClosed,
        Rejected,
    }

    #[derive(Clone, Copy)]
    enum SubmitAction {
        Applied,
        TerminalNoop,
    }

    struct RecordingState {
        events: Mutex<Vec<Event>>,
        open_actions: Mutex<VecDeque<OpenAction>>,
        submit_actions: Mutex<VecDeque<SubmitAction>>,
    }

    impl RecordingState {
        fn with_open_actions(open_actions: impl IntoIterator<Item = OpenAction>) -> Self {
            Self {
                events: Mutex::new(Vec::new()),
                open_actions: Mutex::new(open_actions.into_iter().collect()),
                submit_actions: Mutex::new(VecDeque::new()),
            }
        }

        fn events(&self) -> Vec<Event> {
            self.events.lock().expect("events lock").clone()
        }

        fn record(&self, event: Event) {
            self.events.lock().expect("events lock").push(event);
        }

        fn next_open_action(&self) -> OpenAction {
            self.open_actions
                .lock()
                .expect("open actions lock")
                .pop_front()
                .unwrap_or(OpenAction::Bound)
        }

        fn next_submit_action(&self) -> SubmitAction {
            self.submit_actions
                .lock()
                .expect("submit actions lock")
                .pop_front()
                .unwrap_or(SubmitAction::Applied)
        }
    }

    struct RecordingProducer {
        state: Arc<RecordingState>,
    }

    impl execution::RuntimeFilterProducer for RecordingProducer {
        fn max_contribution_bytes(&self) -> usize {
            1024
        }

        fn submit(
            &self,
            partition: execution::PartitionId,
            sequence: execution::ProducerSequence,
            _: execution::RuntimeFilterContribution,
        ) -> Result<execution::RuntimeFilterSubmitOutcome, execution::RuntimeFilterContractViolation>
        {
            self.state.record(Event::Submit {
                partition: partition.get(),
                sequence: sequence.get(),
            });
            Ok(match self.state.next_submit_action() {
                SubmitAction::Applied => execution::RuntimeFilterSubmitOutcome::Applied,
                SubmitAction::TerminalNoop => execution::RuntimeFilterSubmitOutcome::TerminalNoop,
            })
        }

        fn close_partition(
            &self,
            partition: execution::PartitionId,
            sequence: execution::ProducerSequence,
        ) -> Result<execution::RuntimeFilterSubmitOutcome, execution::RuntimeFilterContractViolation>
        {
            self.state.record(Event::Close {
                partition: partition.get(),
                sequence: sequence.get(),
            });
            Ok(execution::RuntimeFilterSubmitOutcome::Completed)
        }

        fn fail(
            &self,
            reason: execution::RuntimeFilterProducerFailure,
        ) -> Result<execution::RuntimeFilterSubmitOutcome, execution::RuntimeFilterContractViolation>
        {
            self.state.record(Event::Fail(reason));
            Ok(execution::RuntimeFilterSubmitOutcome::TerminalNoop)
        }
    }

    struct RecordingSession {
        state: Arc<RecordingState>,
    }

    impl execution::RuntimeFilterSession for RecordingSession {
        fn open_producer(
            &self,
            request: execution::RuntimeFilterProducerOpenRequest,
        ) -> Result<
            execution::RuntimeFilterBindOutcome<execution::SharedRuntimeFilterProducer>,
            execution::RuntimeFilterContractViolation,
        > {
            self.state.record(Event::Open {
                binding_id: request.contract().binding_id().get(),
                local_partition_count: request.local_partition_count(),
            });
            match self.state.next_open_action() {
                OpenAction::Bound => Ok(execution::RuntimeFilterBindOutcome::Bound(Arc::new(
                    RecordingProducer {
                        state: Arc::clone(&self.state),
                    },
                ))),
                OpenAction::Unavailable => Ok(execution::RuntimeFilterBindOutcome::Unavailable(
                    execution::UnavailableReason::ResourceLimit,
                )),
                OpenAction::SessionClosed => Err(execution::RuntimeFilterContractViolation::new(
                    execution::RuntimeFilterContractViolationKind::SessionClosed,
                    "recording session is closed",
                )),
                OpenAction::Rejected => Err(execution::RuntimeFilterContractViolation::new(
                    execution::RuntimeFilterContractViolationKind::UnauthorizedBinding,
                    "recording session rejects this binding",
                )),
            }
        }

        fn subscribe(
            &self,
            _: execution::RuntimeFilterSubscriptionRequest,
        ) -> Result<
            execution::RuntimeFilterBindOutcome<execution::RuntimeFilterSubscriptionPin>,
            execution::RuntimeFilterContractViolation,
        > {
            Err(execution::RuntimeFilterContractViolation::new(
                execution::RuntimeFilterContractViolationKind::UnauthorizedBinding,
                "recording producer session has no subscriptions",
            ))
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
                "recording producer session has no final-domain completion",
            ))
        }
    }

    fn recording_session(state: Arc<RecordingState>) -> execution::RuntimeFilterSessionRef {
        Arc::new(RecordingSession { state })
    }

    /// Thin test-only adapters keep the protocol scenarios readable while
    /// invoking the actual owned production methods and their control owner.
    /// They do not restore any uncontrolled production compatibility API.
    struct TestFactory {
        actual: AggregateTopNProducerSessionFactory,
        host: execution::OrderedTopNAllocationHost,
        control: OwnedControl,
    }
    impl TestFactory {
        fn new(
            specs: &[novarocks_local_program::AggregateTopNFilter],
            session: execution::RuntimeFilterSessionRef,
            dop: i32,
        ) -> crate::runtime::task::ExecutionResult<Self> {
            let (_tracker, host) = owned_host();
            let control = OwnedControl::pass();
            let actual = AggregateTopNProducerSessionFactory::try_from_compiled(
                specs, session, dop, &host, &control,
            )?;
            Ok(Self {
                actual,
                host,
                control,
            })
        }
        fn local_partition_count(&self) -> u32 {
            self.actual.local_partition_count()
        }
        fn create_for_driver(
            &self,
            dop: i32,
            index: i32,
        ) -> crate::runtime::task::ExecutionResult<TestSession> {
            Ok(TestSession {
                actual: self
                    .actual
                    .create_for_driver_owned(dop, index, &self.control)?,
                control: OwnedControl::pass(),
            })
        }
    }
    struct TestSession {
        actual: super::AggregateTopNProducerSession,
        control: OwnedControl,
    }
    impl TestSession {
        fn bind(&mut self) -> crate::runtime::task::ExecutionResult<()> {
            self.actual.bind_owned(&self.control)
        }
        fn submit_pending(
            &mut self,
            boundaries: &mut [super::OwnedAggregateTopNBoundaryState],
        ) -> crate::runtime::task::ExecutionResult<()> {
            self.actual.submit_pending_owned(boundaries, &self.control)
        }
        fn finish(
            &mut self,
            boundaries: &mut [super::OwnedAggregateTopNBoundaryState],
        ) -> crate::runtime::task::ExecutionResult<()> {
            self.actual.finish_owned(boundaries, &self.control)
        }
        fn fail(
            &mut self,
            reason: execution::RuntimeFilterProducerFailure,
        ) -> crate::runtime::task::ExecutionResult<()> {
            self.actual.fail_owned(reason)
        }
    }

    fn binding(binding_id: u32, limit: u32) -> novarocks_local_program::AggregateTopNFilter {
        let mut spec = compiled_spec(binding_id);
        spec.limit = NonZeroU32::new(limit).expect("non-zero TopN limit");
        spec
    }

    fn observe_protocol_group(
        factory: &TestFactory,
        boundary: &mut super::OwnedAggregateTopNBoundaryState,
        group_id: usize,
        value: &str,
    ) {
        let tuple = factory
            .host
            .copy_utf8(Some(value), &factory.control)
            .expect("actual owned UTF8 key");
        boundary
            .observe_owned_group(group_id, tuple, &factory.control)
            .expect("monotonic aggregate group id");
    }

    fn boundary(
        factory: &TestFactory,
        spec: &novarocks_local_program::AggregateTopNFilter,
        values: &[&str],
    ) -> super::OwnedAggregateTopNBoundaryState {
        let contract = factory
            .host
            .shared_order_contract(spec.producer.contract(), &factory.control)
            .expect("strict compiled order contract");
        let mut boundary = super::OwnedAggregateTopNBoundaryState::try_new_owned(
            spec.limit,
            contract,
            &factory.host,
        )
        .expect("actual owned aggregate TopN boundary");
        for (group_id, value) in values.iter().copied().enumerate() {
            observe_protocol_group(factory, &mut boundary, group_id, value);
        }
        boundary
    }

    #[test]
    fn aggregate_topn_runtime_filter_forwards_dop_partitions_and_sequences() {
        let state = Arc::new(RecordingState::with_open_actions([]));
        let spec = binding(7, 2);
        let factory = TestFactory::new(
            std::slice::from_ref(&spec),
            recording_session(Arc::clone(&state)),
            2,
        )
        .expect("factory");

        let mut first = factory.create_for_driver(2, 0).expect("first driver");
        let mut second = factory.create_for_driver(2, 1).expect("second driver");
        first.bind().expect("bind first driver");
        second.bind().expect("bind second driver");
        let mut first_boundaries = [boundary(&factory, &spec, &["9", "4"])];
        let mut second_boundaries = [boundary(&factory, &spec, &["8", "3"])];
        first
            .submit_pending(&mut first_boundaries)
            .expect("submit first boundary");
        second
            .submit_pending(&mut second_boundaries)
            .expect("submit second boundary");
        first
            .finish(&mut first_boundaries)
            .expect("finish first driver");
        second
            .finish(&mut second_boundaries)
            .expect("finish second driver");

        assert_eq!(
            state.events(),
            vec![
                Event::Open {
                    binding_id: 7,
                    local_partition_count: 2,
                },
                Event::Open {
                    binding_id: 7,
                    local_partition_count: 2,
                },
                Event::Submit {
                    partition: 0,
                    sequence: 0,
                },
                Event::Submit {
                    partition: 1,
                    sequence: 0,
                },
                Event::Close {
                    partition: 0,
                    sequence: 1,
                },
                Event::Close {
                    partition: 1,
                    sequence: 1,
                },
            ]
        );
    }

    #[test]
    fn aggregate_topn_runtime_filter_withholds_not_ready_and_flushes_final_tightening() {
        let state = Arc::new(RecordingState::with_open_actions([]));
        let spec = binding(8, 2);
        let factory = TestFactory::new(
            std::slice::from_ref(&spec),
            recording_session(Arc::clone(&state)),
            1,
        )
        .expect("factory");
        let mut session = factory.create_for_driver(1, 0).expect("driver");
        session.bind().expect("bind");

        let mut boundary = boundary(&factory, &spec, &["5"]);
        session
            .submit_pending(std::slice::from_mut(&mut boundary))
            .expect("not-ready boundary does not submit");
        observe_protocol_group(&factory, &mut boundary, 1, "3");
        session
            .finish(std::slice::from_mut(&mut boundary))
            .expect("finish flushes the first real tightening");

        assert_eq!(
            state.events(),
            vec![
                Event::Open {
                    binding_id: 8,
                    local_partition_count: 1,
                },
                Event::Submit {
                    partition: 0,
                    sequence: 0,
                },
                Event::Close {
                    partition: 0,
                    sequence: 1,
                },
            ]
        );
    }

    #[test]
    fn aggregate_topn_runtime_filter_rejects_dop_drift_and_invalid_partition() {
        let state = Arc::new(RecordingState::with_open_actions([]));
        let factory =
            TestFactory::new(&[binding(9, 1)], recording_session(state), 2).expect("factory");

        assert_eq!(factory.local_partition_count(), 2);
        assert!(
            factory.create_for_driver(1, 0).is_err(),
            "DOP drift is rejected"
        );
        assert!(
            factory.create_for_driver(2, -1).is_err(),
            "negative local index is rejected"
        );
        assert!(
            factory.create_for_driver(2, 2).is_err(),
            "out-of-range index is rejected"
        );
    }

    #[test]
    fn aggregate_topn_runtime_filter_fails_open_for_unavailable_and_closed_session() {
        for action in [OpenAction::Unavailable, OpenAction::SessionClosed] {
            let state = Arc::new(RecordingState::with_open_actions([action]));
            let spec = binding(10, 1);
            let factory = TestFactory::new(
                std::slice::from_ref(&spec),
                recording_session(Arc::clone(&state)),
                1,
            )
            .expect("factory");
            let mut session = factory.create_for_driver(1, 0).expect("driver");
            session.bind().expect("unavailable capability is fail-open");
            session
                .finish(&mut [boundary(&factory, &spec, &["1"])])
                .expect("unavailable capability remains fail-open");
            assert_eq!(
                state.events(),
                vec![Event::Open {
                    binding_id: 10,
                    local_partition_count: 1,
                }]
            );
        }
    }

    #[test]
    fn aggregate_topn_runtime_filter_partial_bind_failure_fails_bound_stream_once() {
        let state = Arc::new(RecordingState::with_open_actions([
            OpenAction::Bound,
            OpenAction::Rejected,
        ]));
        let factory = TestFactory::new(
            &[binding(11, 1), binding(12, 1)],
            recording_session(Arc::clone(&state)),
            1,
        )
        .expect("factory");
        {
            let mut session = factory.create_for_driver(1, 0).expect("driver");
            assert!(
                session.bind().is_err(),
                "non-fail-open bind error reaches the operator"
            );
        }

        assert_eq!(
            state.events(),
            vec![
                Event::Open {
                    binding_id: 11,
                    local_partition_count: 1,
                },
                Event::Open {
                    binding_id: 12,
                    local_partition_count: 1,
                },
                Event::Fail(execution::RuntimeFilterProducerFailure::ExecutionFailed),
            ]
        );
    }

    #[test]
    fn aggregate_topn_runtime_filter_explicit_fail_and_drop_are_single_fail_open() {
        let state = Arc::new(RecordingState::with_open_actions([]));
        let factory = TestFactory::new(&[binding(13, 1)], recording_session(Arc::clone(&state)), 1)
            .expect("factory");
        {
            let mut first = factory.create_for_driver(1, 0).expect("first driver");
            let mut second = factory.create_for_driver(1, 0).expect("second driver");
            first.bind().expect("bind first driver");
            second.bind().expect("bind second driver");
            first
                .fail(execution::RuntimeFilterProducerFailure::Cancelled)
                .expect("explicit fail");
            second
                .fail(execution::RuntimeFilterProducerFailure::Cancelled)
                .expect("shared coordinator suppresses duplicate fail");
        }
        assert_eq!(
            state.events(),
            vec![
                Event::Open {
                    binding_id: 13,
                    local_partition_count: 1,
                },
                Event::Open {
                    binding_id: 13,
                    local_partition_count: 1,
                },
                Event::Fail(execution::RuntimeFilterProducerFailure::Cancelled),
            ]
        );

        let drop_state = Arc::new(RecordingState::with_open_actions([]));
        let drop_factory = TestFactory::new(
            &[binding(14, 1)],
            recording_session(Arc::clone(&drop_state)),
            1,
        )
        .expect("factory");
        {
            let mut session = drop_factory.create_for_driver(1, 0).expect("driver");
            session.bind().expect("bind");
        }
        assert_eq!(
            drop_state.events(),
            vec![
                Event::Open {
                    binding_id: 14,
                    local_partition_count: 1,
                },
                Event::Fail(execution::RuntimeFilterProducerFailure::ExecutionFailed),
            ]
        );
    }

    #[test]
    fn aggregate_topn_runtime_filter_terminal_noop_stops_submission_and_close() {
        let state = Arc::new(RecordingState::with_open_actions([]));
        state
            .submit_actions
            .lock()
            .expect("submit actions lock")
            .push_back(SubmitAction::TerminalNoop);
        let spec = binding(15, 1);
        let factory = TestFactory::new(
            std::slice::from_ref(&spec),
            recording_session(Arc::clone(&state)),
            1,
        )
        .expect("factory");
        let mut session = factory.create_for_driver(1, 0).expect("driver");
        session.bind().expect("bind");
        session
            .submit_pending(&mut [boundary(&factory, &spec, &["1"])])
            .expect("terminal noop is fail-open");
        session
            .finish(&mut [boundary(&factory, &spec, &["1"])])
            .expect("terminal noop suppresses close");

        assert_eq!(
            state.events(),
            vec![
                Event::Open {
                    binding_id: 15,
                    local_partition_count: 1,
                },
                Event::Submit {
                    partition: 0,
                    sequence: 0,
                },
            ]
        );
    }

    struct OwnedControl {
        calls: std::sync::atomic::AtomicUsize,
        refuse_at: Option<usize>,
        cause: novarocks_functions::KernelFailure,
    }
    impl OwnedControl {
        fn pass() -> Self {
            Self {
                calls: std::sync::atomic::AtomicUsize::new(0),
                refuse_at: None,
                cause: novarocks_functions::KernelFailure::Cancelled,
            }
        }
    }
    impl novarocks_functions::KernelEvaluationControl for OwnedControl {
        fn checkpoint(&self, _: u32) -> Result<(), novarocks_functions::KernelFailure> {
            let ordinal = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if self.refuse_at == Some(ordinal) {
                Err(self.cause.clone())
            } else {
                Ok(())
            }
        }
        fn wait(&self, _: std::time::Duration) -> Result<(), novarocks_functions::KernelFailure> {
            panic!("aggregate TopN session never waits")
        }
    }
    fn compiled_spec(binding_id: u32) -> novarocks_local_program::AggregateTopNFilter {
        use novarocks_local_program::*;
        // The original canonical UTF8 ascending/NULL-last framing, pinned
        // independently from the strict production contract author.
        let contract = StaticFilterContract::Ordered {
            keys: FilterOrderKeys::from_legacy(Arc::from([FilterOrderKey {
                value_type: novarocks_functions::FunctionValueType::new(DataType::Utf8, true),
                direction: FilterSortDirection::Ascending,
                null_order: FilterNullOrder::Last,
            }])),
            comparator_digest: hex::decode(
                "c712d2a8677a0968f10c6e39490fcd77ab08fcd3ee9de29bb1df944a16223bae",
            )
            .unwrap()
            .try_into()
            .unwrap(),
            contract_digest: hex::decode(
                "a26b9e26239e0842aa3fd6016bcaf0d41d0f69d17b6a078ea57633f17e1eba26",
            )
            .unwrap()
            .try_into()
            .unwrap(),
        };
        AggregateTopNFilter {
            group_key_expr: ProgramExprId::new(1),
            group_key_ordinal: 0,
            limit: NonZeroU32::new(1).unwrap(),
            producer: StaticFilterProducer::try_new(
                binding_id,
                binding_id + 100,
                FilterProducerKind::OrderedBound,
                contract,
                FilterReduction::TightenOrderedBound,
            )
            .unwrap(),
        }
    }
    fn owned_host() -> (
        Arc<crate::runtime::mem_tracker::MemTracker>,
        execution::OrderedTopNAllocationHost,
    ) {
        let tracker = crate::runtime::mem_tracker::MemTracker::new_root("compiled-topn-session");
        tracker.install_limit_once(1024 * 1024).unwrap();
        let host = execution::OrderedTopNAllocationHost::new(tracker.clone());
        (tracker, host)
    }

    #[test]
    fn aggregate_topn_runtime_filter_owned_final_flush_and_last_order_alias() {
        let (tracker, host) = owned_host();
        let control = OwnedControl::pass();
        let state = Arc::new(RecordingState::with_open_actions([]));
        let factory = AggregateTopNProducerSessionFactory::try_from_compiled(
            &[compiled_spec(31)],
            recording_session(state.clone()),
            2,
            &host,
            &control,
        )
        .unwrap();
        let order = factory.order_contract(0).unwrap().clone();
        let held = tracker.current();
        let last_order = order.clone();
        assert_eq!(
            tracker.current(),
            held,
            "whole-order cloning borrows its paid owner"
        );
        let mut boundary = super::OwnedAggregateTopNBoundaryState::try_new_owned(
            NonZeroU32::new(1).unwrap(),
            order,
            &host,
        )
        .unwrap();
        let mut session = factory.create_for_driver_owned(2, 1, &control).unwrap();
        session.bind_owned(&control).unwrap();
        boundary
            .observe_owned_group(0, host.copy_utf8(Some("z"), &control).unwrap(), &control)
            .unwrap();
        session
            .submit_pending_owned(std::slice::from_mut(&mut boundary), &control)
            .unwrap();
        boundary
            .observe_owned_group(1, host.copy_utf8(Some("a"), &control).unwrap(), &control)
            .unwrap();
        session
            .finish_owned(std::slice::from_mut(&mut boundary), &control)
            .unwrap();
        assert_eq!(
            state.events(),
            vec![
                Event::Open {
                    binding_id: 31,
                    local_partition_count: 2
                },
                Event::Submit {
                    partition: 1,
                    sequence: 0
                },
                Event::Submit {
                    partition: 1,
                    sequence: 1
                },
                Event::Close {
                    partition: 1,
                    sequence: 2
                },
            ]
        );
        drop((session, boundary, factory));
        assert!(
            tracker.current() > 0,
            "last whole-order alias retains its real stock"
        );
        drop(last_order);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn aggregate_topn_runtime_filter_owned_open_error_keeps_nominal_cause_and_fails_once() {
        let (tracker, host) = owned_host();
        let control = OwnedControl::pass();
        let state = Arc::new(RecordingState::with_open_actions([
            OpenAction::Bound,
            OpenAction::Rejected,
        ]));
        let factory = AggregateTopNProducerSessionFactory::try_from_compiled(
            &[compiled_spec(32), compiled_spec(33)],
            recording_session(state.clone()),
            1,
            &host,
            &control,
        )
        .unwrap();
        let mut session = factory.create_for_driver_owned(1, 0, &control).unwrap();
        let cause = session.bind_owned(&control).unwrap_err();
        let crate::runtime::task::ExecutionFailureCause::Domain(shared) = cause.cause() else {
            panic!("the contextual failure must retain its original domain source: {cause:?}");
        };
        let context = shared
            .downcast_ref::<super::AggregateTopNOperatorFailure<
                execution::RuntimeFilterContractViolation,
            >>()
            .expect("context retains the nominal runtime-filter cause");
        assert_eq!(context.binding, 33);
        assert_eq!(context.operation, "open failed during operator bind");
        assert_eq!(
            &context.cause,
            &execution::RuntimeFilterContractViolation::new(
                execution::RuntimeFilterContractViolationKind::UnauthorizedBinding,
                "recording session rejects this binding",
            )
        );
        session
            .fail_owned(execution::RuntimeFilterProducerFailure::ExecutionFailed)
            .unwrap();
        drop((session, factory));
        assert_eq!(
            state.events(),
            vec![
                Event::Open {
                    binding_id: 32,
                    local_partition_count: 1
                },
                Event::Open {
                    binding_id: 33,
                    local_partition_count: 1
                },
                Event::Fail(execution::RuntimeFilterProducerFailure::ExecutionFailed),
            ]
        );
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn aggregate_topn_runtime_filter_owned_factory_control_prefixes_release_before_open() {
        use novarocks_functions::KernelFailure;
        use std::sync::atomic::Ordering;
        let (tracker, host) = owned_host();
        let state = Arc::new(RecordingState::with_open_actions([]));
        let spec = compiled_spec(34);
        let pass = OwnedControl::pass();
        drop(
            AggregateTopNProducerSessionFactory::try_from_compiled(
                std::slice::from_ref(&spec),
                recording_session(state.clone()),
                1,
                &host,
                &pass,
            )
            .unwrap(),
        );
        assert_eq!(tracker.current(), 0);
        for cause in [
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::ResourceExhausted,
        ] {
            for refuse_at in 0..pass.calls.load(Ordering::SeqCst) {
                let control = OwnedControl {
                    refuse_at: Some(refuse_at),
                    cause: cause.clone(),
                    ..OwnedControl::pass()
                };
                let error = AggregateTopNProducerSessionFactory::try_from_compiled(
                    std::slice::from_ref(&spec),
                    recording_session(state.clone()),
                    1,
                    &host,
                    &control,
                )
                .unwrap_err();
                assert_eq!(
                    error.cause(),
                    &crate::runtime::task::ExecutionFailureCause::Kernel(cause.clone())
                );
                assert_eq!(control.calls.load(Ordering::SeqCst), refuse_at + 1);
                assert_eq!(
                    tracker.current(),
                    0,
                    "unpublished factory prefix must release"
                );
            }
        }
        assert!(state.events().is_empty());
    }

    #[test]
    fn aggregate_topn_runtime_filter_owned_submission_control_failure_is_primary_and_final() {
        use novarocks_functions::KernelFailure;
        let (tracker, host) = owned_host();
        let control = OwnedControl::pass();
        let state = Arc::new(RecordingState::with_open_actions([]));
        let factory = AggregateTopNProducerSessionFactory::try_from_compiled(
            &[compiled_spec(35)],
            recording_session(state.clone()),
            1,
            &host,
            &control,
        )
        .unwrap();
        let mut boundary = super::OwnedAggregateTopNBoundaryState::try_new_owned(
            NonZeroU32::new(1).unwrap(),
            factory.order_contract(0).unwrap().clone(),
            &host,
        )
        .unwrap();
        boundary
            .observe_owned_group(0, host.copy_utf8(Some("a"), &control).unwrap(), &control)
            .unwrap();
        let mut session = factory.create_for_driver_owned(1, 0, &control).unwrap();
        session.bind_owned(&control).unwrap();
        let refuse = OwnedControl {
            refuse_at: Some(0),
            cause: KernelFailure::DeadlineExceeded,
            ..OwnedControl::pass()
        };
        let error = session
            .submit_pending_owned(std::slice::from_mut(&mut boundary), &refuse)
            .unwrap_err();
        assert_eq!(
            error.cause(),
            &crate::runtime::task::ExecutionFailureCause::Kernel(KernelFailure::DeadlineExceeded)
        );
        session
            .finish_owned(std::slice::from_mut(&mut boundary), &control)
            .unwrap();
        drop((session, boundary, factory));
        assert_eq!(
            state.events(),
            vec![
                Event::Open {
                    binding_id: 35,
                    local_partition_count: 1
                },
                Event::Fail(execution::RuntimeFilterProducerFailure::ExecutionFailed),
            ]
        );
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn aggregate_topn_runtime_filter_owned_small_capacity_refuses_before_open() {
        use novarocks_functions::KernelFailure;
        let tracker =
            crate::runtime::mem_tracker::MemTracker::new_root("compiled-topn-small-capacity");
        // The tracker admits positive limits only; one byte cannot hold the
        // first real binding slot requested by this nonempty factory.
        tracker.install_limit_once(1).unwrap();
        let host = execution::OrderedTopNAllocationHost::new(tracker.clone());
        let control = OwnedControl::pass();
        let state = Arc::new(RecordingState::with_open_actions([]));
        let error = AggregateTopNProducerSessionFactory::try_from_compiled(
            &[compiled_spec(36)],
            recording_session(state.clone()),
            1,
            &host,
            &control,
        )
        .unwrap_err();
        assert_eq!(
            error.cause(),
            &crate::runtime::task::ExecutionFailureCause::Kernel(KernelFailure::ResourceExhausted)
        );
        assert!(state.events().is_empty());
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn aggregate_topn_runtime_filter_owned_sibling_terminal_suppresses_later_publication() {
        let (tracker, host) = owned_host();
        let control = OwnedControl::pass();
        let state = Arc::new(RecordingState::with_open_actions([]));
        state
            .submit_actions
            .lock()
            .unwrap()
            .push_back(SubmitAction::TerminalNoop);
        let factory = AggregateTopNProducerSessionFactory::try_from_compiled(
            &[compiled_spec(37)],
            recording_session(state.clone()),
            2,
            &host,
            &control,
        )
        .unwrap();
        let mut first = factory.create_for_driver_owned(2, 0, &control).unwrap();
        let mut second = factory.create_for_driver_owned(2, 1, &control).unwrap();
        first.bind_owned(&control).unwrap();
        second.bind_owned(&control).unwrap();
        let mut left = super::OwnedAggregateTopNBoundaryState::try_new_owned(
            NonZeroU32::new(1).unwrap(),
            factory.order_contract(0).unwrap().clone(),
            &host,
        )
        .unwrap();
        let mut right = super::OwnedAggregateTopNBoundaryState::try_new_owned(
            NonZeroU32::new(1).unwrap(),
            factory.order_contract(0).unwrap().clone(),
            &host,
        )
        .unwrap();
        left.observe_owned_group(0, host.copy_utf8(Some("a"), &control).unwrap(), &control)
            .unwrap();
        right
            .observe_owned_group(0, host.copy_utf8(Some("b"), &control).unwrap(), &control)
            .unwrap();
        first
            .submit_pending_owned(std::slice::from_mut(&mut left), &control)
            .unwrap();
        second
            .finish_owned(std::slice::from_mut(&mut right), &control)
            .unwrap();
        first
            .finish_owned(std::slice::from_mut(&mut left), &control)
            .unwrap();
        drop((first, second, left, right, factory));
        assert_eq!(
            state.events(),
            vec![
                Event::Open {
                    binding_id: 37,
                    local_partition_count: 2
                },
                Event::Open {
                    binding_id: 37,
                    local_partition_count: 2
                },
                Event::Submit {
                    partition: 0,
                    sequence: 0
                },
            ]
        );
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn aggregate_topn_runtime_filter_owned_unbound_drop_preserves_bound_sibling_fail_right() {
        let (tracker, host) = owned_host();
        let control = OwnedControl::pass();
        let state = Arc::new(RecordingState::with_open_actions([]));
        let factory = AggregateTopNProducerSessionFactory::try_from_compiled(
            &[compiled_spec(38)],
            recording_session(state.clone()),
            2,
            &host,
            &control,
        )
        .unwrap();
        let mut bound = factory.create_for_driver_owned(2, 0, &control).unwrap();
        bound.bind_owned(&control).unwrap();
        // This models a later driver's schema preparation refusing before bind.
        let unbound = factory.create_for_driver_owned(2, 1, &control).unwrap();
        drop(unbound);
        bound
            .fail_owned(execution::RuntimeFilterProducerFailure::ExecutionFailed)
            .unwrap();
        drop((bound, factory));
        assert_eq!(
            state.events(),
            vec![
                Event::Open {
                    binding_id: 38,
                    local_partition_count: 2,
                },
                Event::Fail(execution::RuntimeFilterProducerFailure::ExecutionFailed),
            ]
        );
        assert_eq!(tracker.current(), 0);
    }
}
