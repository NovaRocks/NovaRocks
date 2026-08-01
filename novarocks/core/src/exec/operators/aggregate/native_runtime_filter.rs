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
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::exec::node::aggregate::AggregateTopNRuntimeFilterProducerBinding;
use crate::exec::node::runtime_filter::{
    RuntimeFilterExecutionContract, RuntimeFilterExecutionReduction,
};
use crate::exec::operators::aggregate::topn_boundary::AggregateTopNBoundaryBinding;
use crate::runtime_filter::model::contract::{
    BindingId, ChannelId, CompletionRequirement, ContributionKind, ReductionRequirement,
};
use crate::runtime_filter::port::identity::{PartitionId, ProducerSequence};
use crate::runtime_filter::port::ordered_bound::{
    OrderContractDigest, OrderedBoundUpdate, OrderedTuple, RuntimeOrderContract,
};
use crate::runtime_filter::port::producer::{
    OrderedBoundProducerAdapter, ProducerFailureReason, ProducerPortKind,
    RuntimeContractViolationKind, SubmitOutcome,
};
use crate::runtime_filter::service::{
    InstalledRuntimeFilterExecutionContract, NativeRuntimeFilterExecutionContext,
    ResolvedNativeProducer,
};

#[derive(Default)]
struct AggregateTopNProducerInstanceCoordinator {
    failed: AtomicBool,
}

#[derive(Clone)]
enum AggregateTopNProducerSource {
    Installed(Arc<ResolvedNativeProducer>),
    #[cfg(test)]
    Prebound(Arc<dyn OrderedBoundProducerAdapter>),
}

#[derive(Clone)]
struct AggregateTopNProducerBinding {
    binding_id: u32,
    contract: Arc<RuntimeOrderContract>,
    source: AggregateTopNProducerSource,
    coordinator: Arc<AggregateTopNProducerInstanceCoordinator>,
}

impl AggregateTopNProducerBinding {
    fn from_plan(
        spec: &AggregateTopNRuntimeFilterProducerBinding,
        context: &NativeRuntimeFilterExecutionContext,
    ) -> Result<Self, String> {
        let resolved = context
            .resolve_producer(
                BindingId::new(spec.binding_id),
                ChannelId::new(spec.channel_id),
                ProducerPortKind::OrderedBound,
            )
            .map_err(|error| {
                format!(
                    "native aggregate TopN producer binding_id={} resolution failed: {error}",
                    spec.binding_id
                )
            })?;
        let contract = validate_binding_contract(
            spec,
            resolved.kind(),
            resolved.contract(),
            resolved.reduction_requirement(),
            resolved.allowed_contribution_kinds(),
            resolved.completion_requirement(),
        )?;
        Ok(Self {
            binding_id: spec.binding_id,
            contract,
            source: AggregateTopNProducerSource::Installed(Arc::new(resolved)),
            coordinator: Arc::new(AggregateTopNProducerInstanceCoordinator::default()),
        })
    }

    #[cfg(test)]
    fn for_test(
        spec: &AggregateTopNRuntimeFilterProducerBinding,
        resolved: TestResolvedOrderedProducer,
    ) -> Result<Self, String> {
        let contract = validate_binding_contract(
            spec,
            resolved.port,
            &resolved.contract,
            resolved.reduction,
            &resolved.contribution_kinds,
            resolved.completion_requirement,
        )?;
        Ok(Self {
            binding_id: spec.binding_id,
            contract,
            source: AggregateTopNProducerSource::Prebound(resolved.adapter),
            coordinator: Arc::new(AggregateTopNProducerInstanceCoordinator::default()),
        })
    }
}

fn validate_binding_contract(
    spec: &AggregateTopNRuntimeFilterProducerBinding,
    port: ProducerPortKind,
    installed_contract: &InstalledRuntimeFilterExecutionContract,
    installed_reduction: ReductionRequirement,
    installed_contribution_kinds: &BTreeSet<ContributionKind>,
    installed_completion: CompletionRequirement,
) -> Result<Arc<RuntimeOrderContract>, String> {
    if port != ProducerPortKind::OrderedBound {
        return Err(format!(
            "native aggregate TopN producer binding_id={} requires the OrderedBound producer port",
            spec.binding_id
        ));
    }
    if spec.reduction != RuntimeFilterExecutionReduction::TightenOrderedBound
        || installed_reduction != ReductionRequirement::TightenOrderedBound
    {
        return Err(format!(
            "native aggregate TopN producer binding_id={} requires TightenOrderedBound reduction",
            spec.binding_id
        ));
    }
    let expected_contributions = BTreeSet::from([
        ContributionKind::OrderedBoundUpdate,
        ContributionKind::ProducerClosed,
    ]);
    if spec.contribution_kinds != expected_contributions
        || installed_contribution_kinds != &expected_contributions
    {
        return Err(format!(
            "native aggregate TopN producer binding_id={} requires exactly OrderedBoundUpdate and ProducerClosed contributions",
            spec.binding_id
        ));
    }
    if spec.completion_requirement != CompletionRequirement::ProducerClosed
        || installed_completion != CompletionRequirement::ProducerClosed
    {
        return Err(format!(
            "native aggregate TopN producer binding_id={} requires ProducerClosed completion",
            spec.binding_id
        ));
    }
    let (
        RuntimeFilterExecutionContract::Ordered {
            keys,
            comparator_digest,
            order_contract_digest,
        },
        InstalledRuntimeFilterExecutionContract::Ordered {
            keys: installed_keys,
            comparator_digest: installed_comparator_digest,
            order_contract_digest: installed_order_contract_digest,
        },
    ) = (&spec.contract, installed_contract)
    else {
        return Err(format!(
            "native aggregate TopN producer binding_id={} requires an ordered contract",
            spec.binding_id
        ));
    };
    if keys != installed_keys
        || comparator_digest != installed_comparator_digest
        || order_contract_digest != installed_order_contract_digest
    {
        return Err(format!(
            "native aggregate TopN producer binding_id={} ordered contract does not match the installed descriptor",
            spec.binding_id
        ));
    }
    RuntimeOrderContract::from_codec(
        keys.to_vec(),
        crate::runtime_filter::model::contract::ComparatorDigest::new(*comparator_digest),
        OrderContractDigest::from_bytes_for_codec(*order_contract_digest),
    )
    .map(Arc::new)
    .map_err(|error| {
        format!(
            "native aggregate TopN producer binding_id={} ordered contract is invalid: {error:?}",
            spec.binding_id
        )
    })
}

pub(crate) struct AggregateTopNProducerSessionFactory {
    bindings: Vec<AggregateTopNProducerBinding>,
    local_partition_count: u32,
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
    pub(crate) fn from_plan(
        specs: &[AggregateTopNRuntimeFilterProducerBinding],
        context: &NativeRuntimeFilterExecutionContext,
        local_partition_count: i32,
    ) -> Result<Self, String> {
        let local_partition_count = validate_partition_count(local_partition_count)?;
        let bindings = specs
            .iter()
            .map(|spec| AggregateTopNProducerBinding::from_plan(spec, context))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            bindings,
            local_partition_count,
        })
    }

    #[cfg(test)]
    fn for_test(
        specs: &[AggregateTopNRuntimeFilterProducerBinding],
        resolved: Vec<TestResolvedOrderedProducer>,
        local_partition_count: u32,
    ) -> Result<Self, String> {
        if local_partition_count == 0 {
            return Err("native aggregate TopN producer DOP must be positive".to_string());
        }
        if specs.len() != resolved.len() {
            return Err(format!(
                "native aggregate TopN producer test binding count mismatch: specs={} resolved={}",
                specs.len(),
                resolved.len()
            ));
        }
        let bindings = specs
            .iter()
            .zip(resolved)
            .map(|(spec, resolved)| AggregateTopNProducerBinding::for_test(spec, resolved))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            bindings,
            local_partition_count,
        })
    }

    pub(crate) const fn local_partition_count(&self) -> u32 {
        self.local_partition_count
    }

    pub(crate) fn create(&self, local_index: i32) -> Result<AggregateTopNProducerSession, String> {
        let local_index = u32::try_from(local_index).map_err(|_| {
            format!(
                "native aggregate TopN producer local index {local_index} cannot be represented as a partition id"
            )
        })?;
        if local_index >= self.local_partition_count {
            return Err(format!(
                "native aggregate TopN producer local index {local_index} is outside DOP {}",
                self.local_partition_count
            ));
        }
        Ok(AggregateTopNProducerSession {
            streams: self
                .bindings
                .iter()
                .cloned()
                .map(|binding| {
                    AggregateTopNProducerStream::new(
                        binding,
                        PartitionId::new(local_index),
                        self.local_partition_count,
                    )
                })
                .collect(),
            completed: false,
        })
    }

    pub(crate) fn create_for_driver(
        &self,
        actual_dop: i32,
        local_index: i32,
    ) -> Result<AggregateTopNProducerSession, String> {
        let actual_dop = validate_partition_count(actual_dop)?;
        if actual_dop != self.local_partition_count {
            return Err(format!(
                "native aggregate TopN producer DOP drifted between factory build and operator creation: expected={} actual={actual_dop}",
                self.local_partition_count
            ));
        }
        self.create(local_index)
    }
}

fn validate_partition_count(local_partition_count: i32) -> Result<u32, String> {
    let local_partition_count = u32::try_from(local_partition_count).map_err(|_| {
        format!(
            "native aggregate TopN producer DOP {local_partition_count} cannot be represented as a partition count"
        )
    })?;
    if local_partition_count == 0 {
        return Err("native aggregate TopN producer DOP must be positive".to_string());
    }
    Ok(local_partition_count)
}

pub(crate) struct AggregateTopNProducerSession {
    streams: Vec<AggregateTopNProducerStream>,
    completed: bool,
}

impl AggregateTopNProducerSession {
    pub(crate) fn bind(&mut self) -> Result<(), String> {
        for index in 0..self.streams.len() {
            if let Err(error) = self.streams[index].bind() {
                let _ = self.fail_incomplete(ProducerFailureReason::ExecutionFailed);
                return Err(error);
            }
        }
        Ok(())
    }

    pub(crate) fn submit_pending(
        &mut self,
        boundaries: &mut [AggregateTopNBoundaryBinding],
    ) -> Result<(), String> {
        if self.completed {
            return Ok(());
        }
        self.require_matching_boundaries(boundaries)?;
        for (index, boundary) in boundaries.iter_mut().enumerate() {
            let pending = boundary
                .state_mut()
                .take_pending_tightening()
                .map_err(|error| error.to_string())?;
            if let Some(bound) = pending
                && let Err(error) = self.streams[index].submit(bound)
            {
                let _ = self.fail_incomplete(ProducerFailureReason::ExecutionFailed);
                self.completed = true;
                return Err(error);
            }
        }
        Ok(())
    }

    pub(crate) fn finish(
        &mut self,
        boundaries: &mut [AggregateTopNBoundaryBinding],
    ) -> Result<(), String> {
        if self.completed {
            return Ok(());
        }
        self.require_matching_boundaries(boundaries)?;
        for (index, boundary) in boundaries.iter_mut().enumerate() {
            let pending = boundary
                .state_mut()
                .finish()
                .map_err(|error| error.to_string())?;
            if let Some(bound) = pending
                && let Err(error) = self.streams[index].submit(bound)
            {
                let _ = self.fail_incomplete(ProducerFailureReason::ExecutionFailed);
                self.completed = true;
                return Err(error);
            }
        }
        for index in 0..self.streams.len() {
            if let Err(error) = self.streams[index].close() {
                let _ = self.fail_incomplete(ProducerFailureReason::ExecutionFailed);
                self.completed = true;
                return Err(error);
            }
        }
        self.completed = true;
        Ok(())
    }

    pub(crate) fn fail(&mut self, reason: ProducerFailureReason) -> Result<(), String> {
        if self.completed {
            return Ok(());
        }
        let result = self.fail_incomplete(reason);
        self.completed = true;
        result
    }

    fn require_matching_boundaries(
        &mut self,
        boundaries: &[AggregateTopNBoundaryBinding],
    ) -> Result<(), String> {
        if self.streams.len() == boundaries.len() {
            return Ok(());
        }
        let error = format!(
            "native aggregate TopN producer session/boundary count mismatch: sessions={} boundaries={}",
            self.streams.len(),
            boundaries.len()
        );
        let _ = self.fail_incomplete(ProducerFailureReason::ExecutionFailed);
        self.completed = true;
        Err(error)
    }

    fn fail_incomplete(&mut self, reason: ProducerFailureReason) -> Result<(), String> {
        let mut first_error = None;
        for stream in &mut self.streams {
            if let Err(error) = stream.fail(reason)
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(())
    }
}

impl Drop for AggregateTopNProducerSession {
    fn drop(&mut self) {
        if !self.completed {
            let _ = self.fail_incomplete(ProducerFailureReason::ExecutionFailed);
        }
    }
}

struct AggregateTopNProducerStream {
    binding: AggregateTopNProducerBinding,
    partition_id: PartitionId,
    local_partition_count: u32,
    next_sequence: u64,
    terminal: bool,
    adapter: Option<Arc<dyn OrderedBoundProducerAdapter>>,
}

impl AggregateTopNProducerStream {
    fn new(
        binding: AggregateTopNProducerBinding,
        partition_id: PartitionId,
        local_partition_count: u32,
    ) -> Self {
        #[cfg(test)]
        let adapter = match &binding.source {
            AggregateTopNProducerSource::Installed(_) => None,
            AggregateTopNProducerSource::Prebound(adapter) => Some(Arc::clone(adapter)),
        };
        #[cfg(not(test))]
        let adapter = None;
        Self {
            binding,
            partition_id,
            local_partition_count,
            next_sequence: 0,
            terminal: false,
            adapter,
        }
    }

    fn bind(&mut self) -> Result<(), String> {
        if self.adapter.is_some() || self.terminal {
            return Ok(());
        }
        let AggregateTopNProducerSource::Installed(resolved) = &self.binding.source else {
            return Err(format!(
                "native aggregate TopN producer binding_id={} has no installed producer source",
                self.binding.binding_id
            ));
        };
        match resolved.open_ordered_bound(self.local_partition_count) {
            Ok(adapter) => {
                self.adapter = Some(adapter);
                Ok(())
            }
            Err(error) if error.kind() == RuntimeContractViolationKind::ServiceUnavailable => {
                self.binding
                    .coordinator
                    .failed
                    .store(true, Ordering::Release);
                self.terminal = true;
                Ok(())
            }
            Err(error) => Err(format!(
                "native aggregate TopN producer binding_id={} open failed during operator bind: {error}",
                self.binding.binding_id
            )),
        }
    }

    fn submit(&mut self, bound: OrderedTuple) -> Result<(), String> {
        if self.terminal || self.binding.coordinator.failed.load(Ordering::Acquire) {
            self.terminal = true;
            return Ok(());
        }
        let adapter = self.adapter.as_ref().ok_or_else(|| {
            format!(
                "native aggregate TopN producer binding_id={} was not bound before input",
                self.binding.binding_id
            )
        })?;
        let update =
            OrderedBoundUpdate::new(&self.binding.contract, bound).map_err(|error| {
                format!(
                    "native aggregate TopN producer binding_id={} update construction failed: {error:?}",
                    self.binding.binding_id
                )
            })?;
        let outcome = match adapter.submit_bound(
            self.partition_id,
            ProducerSequence::new(self.next_sequence),
            update,
        ) {
            Ok(outcome) => outcome,
            Err(error) if error.kind() == RuntimeContractViolationKind::ServiceUnavailable => {
                self.binding
                    .coordinator
                    .failed
                    .store(true, Ordering::Release);
                self.terminal = true;
                return Ok(());
            }
            Err(error) => {
                return Err(format!(
                    "native aggregate TopN producer binding_id={} contribution failed: {error}",
                    self.binding.binding_id
                ));
            }
        };
        if outcome == SubmitOutcome::TerminalNoop {
            self.fail(ProducerFailureReason::ExecutionFailed)?;
            return Ok(());
        }
        self.next_sequence = self.next_sequence.checked_add(1).ok_or_else(|| {
            format!(
                "native aggregate TopN producer binding_id={} sequence overflow",
                self.binding.binding_id
            )
        })?;
        Ok(())
    }

    fn close(&mut self) -> Result<(), String> {
        if self.terminal || self.binding.coordinator.failed.load(Ordering::Acquire) {
            self.terminal = true;
            return Ok(());
        }
        let adapter = self.adapter.as_ref().ok_or_else(|| {
            format!(
                "native aggregate TopN producer binding_id={} was not bound before finish",
                self.binding.binding_id
            )
        })?;
        let outcome = match adapter
            .close_partition(self.partition_id, ProducerSequence::new(self.next_sequence))
        {
            Ok(outcome) => outcome,
            Err(error) if error.kind() == RuntimeContractViolationKind::ServiceUnavailable => {
                self.binding
                    .coordinator
                    .failed
                    .store(true, Ordering::Release);
                self.terminal = true;
                return Ok(());
            }
            Err(error) => {
                return Err(format!(
                    "native aggregate TopN producer binding_id={} close failed: {error}",
                    self.binding.binding_id
                ));
            }
        };
        if outcome == SubmitOutcome::TerminalNoop {
            self.binding
                .coordinator
                .failed
                .store(true, Ordering::Release);
        }
        self.terminal = true;
        Ok(())
    }

    fn fail(&mut self, reason: ProducerFailureReason) -> Result<(), String> {
        if self.terminal {
            return Ok(());
        }
        self.terminal = true;
        let Some(adapter) = self.adapter.as_ref() else {
            return Ok(());
        };
        if self
            .binding
            .coordinator
            .failed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Ok(());
        }
        match adapter.fail(reason) {
            Ok(_) => Ok(()),
            Err(error) if error.kind() == RuntimeContractViolationKind::ServiceUnavailable => {
                Ok(())
            }
            Err(error) => Err(format!(
                "native aggregate TopN producer binding_id={} fail-open failed: {error}",
                self.binding.binding_id
            )),
        }
    }
}

#[cfg(test)]
struct TestResolvedOrderedProducer {
    port: ProducerPortKind,
    contract: InstalledRuntimeFilterExecutionContract,
    reduction: ReductionRequirement,
    contribution_kinds: BTreeSet<ContributionKind>,
    completion_requirement: CompletionRequirement,
    adapter: Arc<dyn OrderedBoundProducerAdapter>,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::num::NonZeroU32;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use arrow::array::RecordBatchOptions;
    use arrow::datatypes::DataType;
    use arrow::datatypes::Schema;
    use arrow::record_batch::RecordBatch;

    use super::{AggregateTopNProducerSessionFactory, TestResolvedOrderedProducer};
    use crate::exec::chunk::{Chunk, ChunkSchema};
    use crate::exec::node::aggregate::AggregateTopNRuntimeFilterProducerBinding;
    use crate::exec::node::runtime_filter::{
        RuntimeFilterExecutionContract, RuntimeFilterExecutionReduction,
    };
    use crate::exec::operators::aggregate::topn_boundary::{
        AggregateTopNBoundaryBinding, build_topn_boundary_bindings,
    };
    use crate::exec::pipeline::driver::{DriverState, PipelineDriver};
    use crate::runtime::runtime_state::RuntimeState;
    use crate::runtime_filter::model::contract::{
        CompletionRequirement, ContributionKind, NullOrder, OrderContract, OrderKeyContract,
        ReductionRequirement, SortDirection,
    };
    use crate::runtime_filter::port::identity::{PartitionId, ProducerSequence};
    use crate::runtime_filter::port::ordered_bound::{
        COMPARATOR_ALGORITHM_VERSION, OrderedBoundUpdate, OrderedScalar, OrderedTuple,
        RuntimeOrderContract, comparator_digest_for_test,
    };
    use crate::runtime_filter::port::producer::{
        OrderedBoundProducerAdapter, ProducerFailureReason, ProducerPortKind,
        RuntimeContractViolation, RuntimeContractViolationKind, SubmitOutcome,
    };
    use crate::runtime_filter::service::InstalledRuntimeFilterExecutionContract;

    #[derive(Clone, Debug, Eq, PartialEq)]
    enum Event {
        Submit {
            partition: u32,
            sequence: u64,
            bound: OrderedTuple,
        },
        Close {
            partition: u32,
            terminal_sequence: u64,
        },
        Fail(ProducerFailureReason),
    }

    #[derive(Default)]
    struct FakeOrderedAdapter {
        events: Mutex<Vec<Event>>,
        reject_submit: Mutex<bool>,
        reject_close: Mutex<bool>,
    }

    impl FakeOrderedAdapter {
        fn events(&self) -> Vec<Event> {
            self.events.lock().expect("events lock").clone()
        }

        fn reject_next_submit(&self) {
            *self.reject_submit.lock().expect("reject lock") = true;
        }

        fn reject_next_close(&self) {
            *self.reject_close.lock().expect("reject close lock") = true;
        }
    }

    impl OrderedBoundProducerAdapter for FakeOrderedAdapter {
        fn submit_bound(
            &self,
            partition_id: PartitionId,
            sequence: ProducerSequence,
            update: OrderedBoundUpdate,
        ) -> Result<SubmitOutcome, RuntimeContractViolation> {
            self.events
                .lock()
                .expect("events lock")
                .push(Event::Submit {
                    partition: partition_id.get(),
                    sequence: sequence.get(),
                    bound: update.bound().clone(),
                });
            if std::mem::take(&mut *self.reject_submit.lock().expect("reject lock")) {
                return Err(RuntimeContractViolation::new(
                    RuntimeContractViolationKind::InvalidContributionLease,
                    "fake resource rejection",
                ));
            }
            Ok(SubmitOutcome::Applied)
        }

        fn close_partition(
            &self,
            partition_id: PartitionId,
            terminal_sequence: ProducerSequence,
        ) -> Result<SubmitOutcome, RuntimeContractViolation> {
            self.events.lock().expect("events lock").push(Event::Close {
                partition: partition_id.get(),
                terminal_sequence: terminal_sequence.get(),
            });
            if std::mem::take(&mut *self.reject_close.lock().expect("reject close lock")) {
                return Err(RuntimeContractViolation::new(
                    RuntimeContractViolationKind::InvalidContributionLease,
                    "fake close rejection",
                ));
            }
            Ok(SubmitOutcome::Completed)
        }

        fn fail(
            &self,
            reason: ProducerFailureReason,
        ) -> Result<SubmitOutcome, RuntimeContractViolation> {
            self.events
                .lock()
                .expect("events lock")
                .push(Event::Fail(reason));
            Ok(SubmitOutcome::TerminalNoop)
        }
    }

    fn runtime_contract() -> Arc<RuntimeOrderContract> {
        let keys = vec![OrderKeyContract {
            data_type: DataType::Int64,
            direction: SortDirection::Ascending,
            null_order: NullOrder::Last,
        }];
        Arc::new(
            RuntimeOrderContract::try_from_plan(&OrderContract {
                comparator_digest: comparator_digest_for_test(&keys, COMPARATOR_ALGORITHM_VERSION),
                keys,
                inclusive: true,
            })
            .expect("valid order contract"),
        )
    }

    fn spec(
        contract: &RuntimeOrderContract,
        limit: u32,
    ) -> AggregateTopNRuntimeFilterProducerBinding {
        AggregateTopNRuntimeFilterProducerBinding {
            binding_id: 11,
            channel_id: 12,
            group_key_expr_id: crate::exec::expr::ExprId(13),
            group_key_ordinal: 0,
            limit: NonZeroU32::new(limit).expect("nonzero limit"),
            contract: RuntimeFilterExecutionContract::Ordered {
                keys: contract.keys().to_vec().into(),
                comparator_digest: contract.plan_comparator_digest().get(),
                order_contract_digest: contract.digest().bytes(),
            },
            reduction: RuntimeFilterExecutionReduction::TightenOrderedBound,
            contribution_kinds: BTreeSet::from([
                ContributionKind::OrderedBoundUpdate,
                ContributionKind::ProducerClosed,
            ]),
            completion_requirement: CompletionRequirement::ProducerClosed,
        }
    }

    fn resolved(
        contract: &RuntimeOrderContract,
        adapter: Arc<FakeOrderedAdapter>,
    ) -> TestResolvedOrderedProducer {
        TestResolvedOrderedProducer {
            port: ProducerPortKind::OrderedBound,
            contract: InstalledRuntimeFilterExecutionContract::Ordered {
                keys: contract.keys().to_vec().into(),
                comparator_digest: contract.plan_comparator_digest().get(),
                order_contract_digest: contract.digest().bytes(),
            },
            reduction: ReductionRequirement::TightenOrderedBound,
            contribution_kinds: BTreeSet::from([
                ContributionKind::OrderedBoundUpdate,
                ContributionKind::ProducerClosed,
            ]),
            completion_requirement: CompletionRequirement::ProducerClosed,
            adapter,
        }
    }

    fn factory(
        spec: &AggregateTopNRuntimeFilterProducerBinding,
        contract: &RuntimeOrderContract,
        adapter: Arc<FakeOrderedAdapter>,
        partition_count: u32,
    ) -> AggregateTopNProducerSessionFactory {
        AggregateTopNProducerSessionFactory::for_test(
            std::slice::from_ref(spec),
            vec![resolved(contract, adapter)],
            partition_count,
        )
        .expect("valid producer factory")
    }

    fn bindings(
        spec: &AggregateTopNRuntimeFilterProducerBinding,
    ) -> Vec<AggregateTopNBoundaryBinding> {
        build_topn_boundary_bindings(std::slice::from_ref(spec)).expect("boundary bindings")
    }

    fn observe(bindings: &mut [AggregateTopNBoundaryBinding], group_id: usize, value: i64) {
        let contract = Arc::clone(bindings[0].state().contract());
        bindings[0]
            .state_mut()
            .observe_new_group(
                group_id,
                OrderedTuple::try_new(&contract, [Some(OrderedScalar::Int64(value))])
                    .expect("ordered tuple"),
            )
            .expect("new group");
    }

    fn one_row_empty_chunk() -> Chunk {
        let schema = Arc::new(Schema::empty());
        let batch = RecordBatch::try_new_with_options(
            schema,
            Vec::new(),
            &RecordBatchOptions::new().with_row_count(Some(1)),
        )
        .expect("one-row empty batch");
        Chunk::try_new_with_chunk_schema(batch, Arc::new(ChunkSchema::empty()))
            .expect("one-row empty chunk")
    }

    #[test]
    fn aggregate_topn_producer_waits_until_n_candidates_and_starts_at_sequence_zero() {
        let contract = runtime_contract();
        let spec = spec(&contract, 2);
        let adapter = Arc::new(FakeOrderedAdapter::default());
        let factory = factory(&spec, &contract, Arc::clone(&adapter), 1);
        let mut session = factory.create(0).expect("partition session");
        session.bind().expect("bind");
        let mut bindings = bindings(&spec);

        observe(&mut bindings, 0, 20);
        session
            .submit_pending(&mut bindings)
            .expect("not-ready check");
        assert!(adapter.events().is_empty());

        observe(&mut bindings, 1, 10);
        session.submit_pending(&mut bindings).expect("first bound");
        assert_eq!(
            adapter.events(),
            vec![Event::Submit {
                partition: 0,
                sequence: 0,
                bound: OrderedTuple::try_new(&contract, [Some(OrderedScalar::Int64(20))]).unwrap(),
            }]
        );
    }

    #[test]
    fn aggregate_topn_producer_submits_only_strict_tightenings() {
        let contract = runtime_contract();
        let spec = spec(&contract, 2);
        let adapter = Arc::new(FakeOrderedAdapter::default());
        let factory = factory(&spec, &contract, Arc::clone(&adapter), 1);
        let mut session = factory.create(0).expect("partition session");
        session.bind().expect("bind");
        let mut bindings = bindings(&spec);

        observe(&mut bindings, 0, 10);
        observe(&mut bindings, 1, 20);
        session.submit_pending(&mut bindings).expect("first bound");
        observe(&mut bindings, 2, 20);
        session.submit_pending(&mut bindings).expect("equal bound");
        observe(&mut bindings, 3, 5);
        session
            .submit_pending(&mut bindings)
            .expect("tightened bound");

        assert_eq!(
            adapter
                .events()
                .iter()
                .filter_map(|event| match event {
                    Event::Submit {
                        sequence, bound, ..
                    } => Some((*sequence, bound.clone())),
                    _ => None,
                })
                .collect::<Vec<_>>(),
            vec![
                (
                    0,
                    OrderedTuple::try_new(&contract, [Some(OrderedScalar::Int64(20))]).unwrap()
                ),
                (
                    1,
                    OrderedTuple::try_new(&contract, [Some(OrderedScalar::Int64(10))]).unwrap()
                ),
            ]
        );
    }

    #[test]
    fn aggregate_topn_producer_sequences_are_independent_per_driver_partition() {
        let contract = runtime_contract();
        let spec = spec(&contract, 1);
        let adapter = Arc::new(FakeOrderedAdapter::default());
        let factory = factory(&spec, &contract, Arc::clone(&adapter), 2);
        let mut first = factory.create(0).expect("first partition");
        let mut second = factory.create(1).expect("second partition");
        first.bind().expect("bind first");
        second.bind().expect("bind second");
        let mut first_bindings = bindings(&spec);
        let mut second_bindings = bindings(&spec);

        observe(&mut first_bindings, 0, 10);
        observe(&mut second_bindings, 0, 20);
        first.submit_pending(&mut first_bindings).unwrap();
        second.submit_pending(&mut second_bindings).unwrap();
        first.finish(&mut first_bindings).unwrap();
        second.finish(&mut second_bindings).unwrap();

        assert_eq!(
            adapter.events(),
            vec![
                Event::Submit {
                    partition: 0,
                    sequence: 0,
                    bound: OrderedTuple::try_new(&contract, [Some(OrderedScalar::Int64(10))])
                        .unwrap(),
                },
                Event::Submit {
                    partition: 1,
                    sequence: 0,
                    bound: OrderedTuple::try_new(&contract, [Some(OrderedScalar::Int64(20))])
                        .unwrap(),
                },
                Event::Close {
                    partition: 0,
                    terminal_sequence: 1,
                },
                Event::Close {
                    partition: 1,
                    terminal_sequence: 1,
                },
            ]
        );
    }

    #[test]
    fn aggregate_topn_producer_final_check_precedes_one_exclusive_terminal_close() {
        let contract = runtime_contract();
        let spec = spec(&contract, 2);
        let adapter = Arc::new(FakeOrderedAdapter::default());
        let factory = factory(&spec, &contract, Arc::clone(&adapter), 1);
        let mut session = factory.create(0).expect("partition session");
        session.bind().expect("bind");
        let mut bindings = bindings(&spec);

        observe(&mut bindings, 0, 20);
        observe(&mut bindings, 1, 30);
        session.submit_pending(&mut bindings).unwrap();
        observe(&mut bindings, 2, 10);
        session.finish(&mut bindings).unwrap();
        session.finish(&mut bindings).unwrap();

        assert_eq!(
            adapter.events(),
            vec![
                Event::Submit {
                    partition: 0,
                    sequence: 0,
                    bound: OrderedTuple::try_new(&contract, [Some(OrderedScalar::Int64(30))])
                        .unwrap(),
                },
                Event::Submit {
                    partition: 0,
                    sequence: 1,
                    bound: OrderedTuple::try_new(&contract, [Some(OrderedScalar::Int64(20))])
                        .unwrap(),
                },
                Event::Close {
                    partition: 0,
                    terminal_sequence: 2,
                },
            ]
        );
    }

    #[test]
    fn aggregate_topn_producer_cancel_and_resource_failure_fail_without_close() {
        let contract = runtime_contract();
        let spec = spec(&contract, 1);

        let cancelled_adapter = Arc::new(FakeOrderedAdapter::default());
        let cancelled_factory = factory(&spec, &contract, Arc::clone(&cancelled_adapter), 1);
        let mut cancelled = cancelled_factory.create(0).unwrap();
        cancelled.bind().unwrap();
        cancelled.fail(ProducerFailureReason::Cancelled).unwrap();
        cancelled
            .fail(ProducerFailureReason::ExecutionFailed)
            .unwrap();
        assert_eq!(
            cancelled_adapter.events(),
            vec![Event::Fail(ProducerFailureReason::Cancelled)]
        );

        let rejected_adapter = Arc::new(FakeOrderedAdapter::default());
        let rejected_factory = factory(&spec, &contract, Arc::clone(&rejected_adapter), 1);
        let mut rejected = rejected_factory.create(0).unwrap();
        rejected.bind().unwrap();
        let mut rejected_bindings = bindings(&spec);
        observe(&mut rejected_bindings, 0, 10);
        rejected_adapter.reject_next_submit();
        let error = rejected
            .submit_pending(&mut rejected_bindings)
            .expect_err("resource rejection");
        assert!(error.contains("contribution failed"));
        assert!(matches!(
            rejected_adapter.events().as_slice(),
            [
                Event::Submit { .. },
                Event::Fail(ProducerFailureReason::ExecutionFailed)
            ]
        ));
        assert!(
            !rejected_adapter
                .events()
                .iter()
                .any(|event| matches!(event, Event::Close { .. }))
        );
    }

    #[test]
    fn aggregate_topn_producer_close_failure_does_not_fail_an_already_closed_binding() {
        let contract = runtime_contract();
        let first_spec = spec(&contract, 1);
        let mut second_spec = spec(&contract, 1);
        second_spec.binding_id = 21;
        second_spec.channel_id = 22;
        let first_adapter = Arc::new(FakeOrderedAdapter::default());
        let second_adapter = Arc::new(FakeOrderedAdapter::default());
        let factory = AggregateTopNProducerSessionFactory::for_test(
            &[first_spec.clone(), second_spec.clone()],
            vec![
                resolved(&contract, Arc::clone(&first_adapter)),
                resolved(&contract, Arc::clone(&second_adapter)),
            ],
            1,
        )
        .expect("valid producer factory");
        let mut session = factory.create(0).expect("partition session");
        session.bind().expect("bind");
        let mut boundaries =
            build_topn_boundary_bindings(&[first_spec, second_spec]).expect("boundary bindings");
        second_adapter.reject_next_close();

        let error = session
            .finish(&mut boundaries)
            .expect_err("second close rejection");
        assert!(error.contains("close failed"));
        assert_eq!(
            first_adapter.events(),
            vec![Event::Close {
                partition: 0,
                terminal_sequence: 0,
            }]
        );
        assert_eq!(
            second_adapter.events(),
            vec![
                Event::Close {
                    partition: 0,
                    terminal_sequence: 0,
                },
                Event::Fail(ProducerFailureReason::ExecutionFailed),
            ]
        );
    }

    #[test]
    fn aggregate_topn_producer_operator_execution_error_wins_over_driver_cancel() {
        let contract = runtime_contract();
        let spec = spec(&contract, 1);
        let adapter = Arc::new(FakeOrderedAdapter::default());
        let factory = factory(&spec, &contract, Arc::clone(&adapter), 1);
        let mut operator = super::super::aggregate_topn_test_operator(vec![spec], factory);
        operator
            .bind_runtime_state(&RuntimeState::default())
            .expect("bind native producer");

        let error = operator
            .as_processor_mut()
            .expect("aggregate processor")
            .push_chunk(&RuntimeState::default(), one_row_empty_chunk())
            .expect_err("unprepared aggregate input must fail");
        assert!(error.contains("operator not prepared"), "{error}");
        operator.cancel();
        operator.close().expect("duplicate close is idempotent");

        assert_eq!(
            adapter.events(),
            vec![Event::Fail(ProducerFailureReason::ExecutionFailed)]
        );
    }

    #[test]
    fn aggregate_topn_producer_streaming_finish_error_wins_over_driver_cancel() {
        let contract = runtime_contract();
        let spec = spec(&contract, 1);
        let adapter = Arc::new(FakeOrderedAdapter::default());
        let factory = factory(&spec, &contract, Arc::clone(&adapter), 1);
        let mut operator = super::super::streaming_sink::aggregate_streaming_topn_test_operator(
            vec![spec],
            factory,
        );
        operator
            .bind_runtime_state(&RuntimeState::default())
            .expect("bind native producer");

        let error = operator
            .as_processor_mut()
            .expect("streaming aggregate processor")
            .set_finishing(&RuntimeState::default())
            .expect_err("unprepared streaming aggregate finish must fail");
        assert!(error.contains("operator not prepared"), "{error}");
        operator.cancel();
        operator.close().expect("duplicate close is idempotent");

        assert_eq!(
            adapter.events(),
            vec![Event::Fail(ProducerFailureReason::ExecutionFailed)]
        );
    }

    #[test]
    fn aggregate_topn_producer_operator_cancel_uses_cancelled_failure() {
        let contract = runtime_contract();
        let spec = spec(&contract, 1);
        let adapter = Arc::new(FakeOrderedAdapter::default());
        let factory = factory(&spec, &contract, Arc::clone(&adapter), 1);
        let mut operator = super::super::aggregate_topn_test_operator(vec![spec], factory);
        operator
            .bind_runtime_state(&RuntimeState::default())
            .expect("bind native producer");

        operator.cancel();
        operator.close().expect("duplicate close is idempotent");

        assert_eq!(
            adapter.events(),
            vec![Event::Fail(ProducerFailureReason::Cancelled)]
        );
    }

    #[test]
    fn aggregate_topn_producer_runtime_state_failure_uses_execution_failed() {
        let contract = runtime_contract();
        let spec = spec(&contract, 1);
        let adapter = Arc::new(FakeOrderedAdapter::default());
        let factory = factory(&spec, &contract, Arc::clone(&adapter), 1);
        let mut operator = super::super::aggregate_topn_test_operator(vec![spec], factory);
        let runtime_state = Arc::new(RuntimeState::default());
        operator
            .bind_runtime_state(runtime_state.as_ref())
            .expect("bind native producer");
        runtime_state
            .error_state()
            .set_error("injected upstream failure".to_string());
        let mut driver =
            PipelineDriver::new(0, vec![operator], None, Vec::new(), runtime_state, None);

        assert_eq!(
            driver.process(Duration::from_millis(10)),
            DriverState::Failed("injected upstream failure".to_string())
        );
        assert_eq!(
            adapter.events(),
            vec![Event::Fail(ProducerFailureReason::ExecutionFailed)]
        );
    }

    #[test]
    fn aggregate_topn_producer_partial_bind_failure_reaches_bound_driver_once() {
        let contract = runtime_contract();
        let spec = spec(&contract, 1);
        let adapter = Arc::new(FakeOrderedAdapter::default());
        let factory = factory(&spec, &contract, Arc::clone(&adapter), 2);
        let mut unbound = factory.create(0).expect("unbound partition session");
        let mut bound = factory.create(1).expect("bound partition session");
        bound.bind().expect("bind sibling partition");
        unbound.streams[0].adapter = None;

        unbound
            .fail(ProducerFailureReason::ExecutionFailed)
            .expect("record pending failure");
        bound
            .fail(ProducerFailureReason::ExecutionFailed)
            .expect("deliver pending failure");
        bound
            .fail(ProducerFailureReason::Cancelled)
            .expect("duplicate failure is idempotent");

        assert_eq!(
            adapter.events(),
            vec![Event::Fail(ProducerFailureReason::ExecutionFailed)]
        );
    }

    #[test]
    fn aggregate_topn_producer_factory_rejects_wrong_port_reduction_and_order_digest() {
        let contract = runtime_contract();
        let spec = spec(&contract, 1);
        let adapter = Arc::new(FakeOrderedAdapter::default());

        let mut wrong_port = resolved(&contract, Arc::clone(&adapter));
        wrong_port.port = ProducerPortKind::Membership;
        let error = AggregateTopNProducerSessionFactory::for_test(
            std::slice::from_ref(&spec),
            vec![wrong_port],
            1,
        )
        .expect_err("wrong port");
        assert!(error.contains("OrderedBound"));

        let mut wrong_reduction = resolved(&contract, Arc::clone(&adapter));
        wrong_reduction.reduction = ReductionRequirement::SetUnion;
        let error = AggregateTopNProducerSessionFactory::for_test(
            std::slice::from_ref(&spec),
            vec![wrong_reduction],
            1,
        )
        .expect_err("wrong reduction");
        assert!(error.contains("TightenOrderedBound"));

        let mut wrong_digest = resolved(&contract, adapter);
        wrong_digest.contract = InstalledRuntimeFilterExecutionContract::Ordered {
            keys: contract.keys().to_vec().into(),
            comparator_digest: contract.plan_comparator_digest().get(),
            order_contract_digest: [99; 32],
        };
        let error = AggregateTopNProducerSessionFactory::for_test(&[spec], vec![wrong_digest], 1)
            .expect_err("wrong order digest");
        assert!(error.contains("ordered contract"));
    }
}
