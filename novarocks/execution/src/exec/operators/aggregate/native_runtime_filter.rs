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

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::runtime_filter as execution;
use crate::runtime_filter::contribution::OrderedTuple;

use crate::exec::node::aggregate::AggregateTopNRuntimeFilterProducerBinding;
use crate::exec::node::runtime_filter::RuntimeFilterExecutionContract;
use crate::exec::operators::aggregate::topn_boundary::AggregateTopNBoundaryBinding;

#[derive(Default)]
struct AggregateTopNProducerInstanceCoordinator {
    failed: AtomicBool,
}

#[derive(Clone)]
struct AggregateTopNProducerBinding {
    binding_id: u32,
    execution_contract: execution::RuntimeFilterProducerContract,
    session: execution::RuntimeFilterSessionRef,
    coordinator: Arc<AggregateTopNProducerInstanceCoordinator>,
}

impl AggregateTopNProducerBinding {
    fn from_plan(
        spec: &AggregateTopNRuntimeFilterProducerBinding,
        session: execution::RuntimeFilterSessionRef,
    ) -> Result<Self, String> {
        if !matches!(
            spec.contract().contract(),
            RuntimeFilterExecutionContract::Ordered(_)
        ) {
            return Err(format!(
                "native aggregate TopN producer binding_id={} requires an ordered contract",
                spec.binding_id()
            ));
        }
        if spec.contract().kind() != execution::RuntimeFilterProducerKind::OrderedBound {
            return Err(format!(
                "native aggregate TopN producer binding_id={} requires an ordered-bound producer contract",
                spec.binding_id()
            ));
        }
        Ok(Self {
            binding_id: spec.binding_id(),
            execution_contract: spec.contract().clone(),
            session,
            coordinator: Arc::new(AggregateTopNProducerInstanceCoordinator::default()),
        })
    }
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
        session: execution::RuntimeFilterSessionRef,
        local_partition_count: i32,
    ) -> Result<Self, String> {
        let local_partition_count = validate_partition_count(local_partition_count)?;
        let bindings = specs
            .iter()
            .map(|spec| AggregateTopNProducerBinding::from_plan(spec, Arc::clone(&session)))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            bindings,
            local_partition_count,
        })
    }

    pub(crate) const fn local_partition_count(&self) -> u32 {
        self.local_partition_count
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
        let local_index = u32::try_from(local_index).map_err(|_| format!(
            "native aggregate TopN producer local index {local_index} cannot be represented as a partition id"
        ))?;
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
                        execution::PartitionId::new(local_index),
                        self.local_partition_count,
                    )
                })
                .collect(),
            completed: false,
        })
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
                let _ =
                    self.fail_incomplete(execution::RuntimeFilterProducerFailure::ExecutionFailed);
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
                let _ =
                    self.fail_incomplete(execution::RuntimeFilterProducerFailure::ExecutionFailed);
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
                let _ =
                    self.fail_incomplete(execution::RuntimeFilterProducerFailure::ExecutionFailed);
                self.completed = true;
                return Err(error);
            }
        }
        for index in 0..self.streams.len() {
            if let Err(error) = self.streams[index].close() {
                let _ =
                    self.fail_incomplete(execution::RuntimeFilterProducerFailure::ExecutionFailed);
                self.completed = true;
                return Err(error);
            }
        }
        self.completed = true;
        Ok(())
    }

    pub(crate) fn fail(
        &mut self,
        reason: execution::RuntimeFilterProducerFailure,
    ) -> Result<(), String> {
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
        let _ = self.fail_incomplete(execution::RuntimeFilterProducerFailure::ExecutionFailed);
        self.completed = true;
        Err(error)
    }

    fn fail_incomplete(
        &mut self,
        reason: execution::RuntimeFilterProducerFailure,
    ) -> Result<(), String> {
        let mut first_error = None;
        for stream in &mut self.streams {
            if let Err(error) = stream.fail(reason)
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }
        first_error.map_or(Ok(()), Err)
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
    producer: Option<execution::RuntimeFilterProducerHandle>,
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

    fn bind(&mut self) -> Result<(), String> {
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
            Err(error)
                if error.kind() == execution::RuntimeFilterContractViolationKind::SessionClosed =>
            {
                self.mark_service_unavailable();
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
        let producer = self.producer.as_ref().ok_or_else(|| {
            format!(
                "native aggregate TopN producer binding_id={} was not bound before input",
                self.binding.binding_id
            )
        })?;
        let contribution = encode_execution_ordered_bound(
            &self.binding.execution_contract,
            &bound,
            producer.max_contribution_bytes(),
        ).map_err(|error| format!(
            "native aggregate TopN producer binding_id={} contribution encoding failed: {error}",
            self.binding.binding_id
        ))?;
        match producer.submit(
            self.partition_id,
            execution::ProducerSequence::new(self.next_sequence),
            contribution,
        ) {
            Ok(execution::RuntimeFilterSubmitOutcome::TerminalNoop) => {
                self.binding
                    .coordinator
                    .failed
                    .store(true, Ordering::Release);
                self.terminal = true;
                return Ok(());
            }
            Ok(_) => {}
            Err(error)
                if error.kind() == execution::RuntimeFilterContractViolationKind::SessionClosed =>
            {
                self.mark_service_unavailable();
                return Ok(());
            }
            Err(error) => {
                return Err(format!(
                    "native aggregate TopN producer binding_id={} contribution failed: {error}",
                    self.binding.binding_id
                ));
            }
        }
        self.next_sequence = self.next_sequence.checked_add(1).ok_or_else(|| {
            format!(
                "native aggregate TopN producer binding_id={} producer sequence overflow",
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
        let producer = self.producer.as_ref().ok_or_else(|| {
            format!(
                "native aggregate TopN producer binding_id={} was not bound before finish",
                self.binding.binding_id
            )
        })?;
        match producer.close_partition(
            self.partition_id,
            execution::ProducerSequence::new(self.next_sequence),
        ) {
            Ok(execution::RuntimeFilterSubmitOutcome::TerminalNoop) => {
                self.binding
                    .coordinator
                    .failed
                    .store(true, Ordering::Release);
            }
            Ok(_) => {}
            Err(error)
                if error.kind() == execution::RuntimeFilterContractViolationKind::SessionClosed =>
            {
                self.mark_service_unavailable();
                return Ok(());
            }
            Err(error) => {
                return Err(format!(
                    "native aggregate TopN producer binding_id={} close failed: {error}",
                    self.binding.binding_id
                ));
            }
        }
        self.terminal = true;
        Ok(())
    }

    fn mark_service_unavailable(&mut self) {
        self.binding
            .coordinator
            .failed
            .store(true, Ordering::Release);
        self.terminal = true;
    }

    fn fail(&mut self, reason: execution::RuntimeFilterProducerFailure) -> Result<(), String> {
        self.terminal = true;
        if self
            .binding
            .coordinator
            .failed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Ok(());
        }
        let Some(producer) = self.producer.as_ref() else {
            return Ok(());
        };
        if let Err(error) = producer.fail(reason)
            && error.kind() != execution::RuntimeFilterContractViolationKind::SessionClosed
        {
            return Err(format!(
                "native aggregate TopN producer binding_id={} fail-open failed: {error}",
                self.binding.binding_id
            ));
        }
        Ok(())
    }
}

fn encode_execution_ordered_bound(
    producer: &execution::RuntimeFilterProducerContract,
    bound: &OrderedTuple,
    max_contribution_bytes: usize,
) -> Result<execution::RuntimeFilterContribution, execution::contribution::ContributionCodecError> {
    let execution::RuntimeFilterExecutionContract::Ordered(contract) = producer.contract() else {
        return Err(execution::contribution::ContributionCodecError::SchemaMismatch);
    };
    let update = execution::contribution::OrderedBoundUpdate::try_new(contract, bound.clone())
        .map_err(|_| execution::contribution::ContributionCodecError::SchemaMismatch)?;
    let typed = execution::contribution::RuntimeFilterContribution::ordered_bound(update);
    let encoded = execution::contribution::encode_contribution(
        &typed,
        execution::contribution::ContributionCodecExpectation::OrderedBound(contract),
        max_contribution_bytes,
    )?;
    let (contract_digest, canonical_bytes) = encoded.into_parts();
    Ok(execution::RuntimeFilterContribution::new(
        execution::RuntimeFilterContributionKind::OrderedBound,
        contract_digest,
        canonical_bytes,
    ))
}
