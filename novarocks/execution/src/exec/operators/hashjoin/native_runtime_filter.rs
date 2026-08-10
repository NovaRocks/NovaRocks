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
use arrow::array::ArrayRef;

use crate::exec::expr::{ExprArena, ExprId};
use crate::exec::node::join::JoinRuntimeFilterProducerBinding;
use crate::exec::node::runtime_filter::RuntimeFilterExecutionContract;

#[derive(Default)]
struct NativeProducerInstanceCoordinator {
    failed: AtomicBool,
}

#[derive(Clone)]
struct NativeMembershipProducerBinding {
    join_key_ordinal: usize,
    contract: execution::RuntimeFilterProducerContract,
    session: execution::RuntimeFilterSessionRef,
    coordinator: Arc<NativeProducerInstanceCoordinator>,
}

impl NativeMembershipProducerBinding {
    #[cfg(test)]
    fn for_test(
        binding_id: u32,
        join_key_ordinal: usize,
        data_type: arrow::datatypes::DataType,
        session: execution::RuntimeFilterSessionRef,
    ) -> Self {
        let schema = execution::RuntimeFilterMembershipSchema::new(
            &data_type,
            execution::RuntimeFilterNullSemantics::NeverMatches,
        )
        .expect("test membership schema");
        Self {
            join_key_ordinal,
            contract: execution::RuntimeFilterProducerContract::membership(
                execution::RuntimeFilterBindingId::new(binding_id),
                execution::RuntimeFilterChannelId::new(binding_id),
                RuntimeFilterExecutionContract::Membership(schema),
            )
            .expect("test membership producer contract"),
            session,
            coordinator: Arc::new(NativeProducerInstanceCoordinator::default()),
        }
    }

    fn from_plan(
        spec: &JoinRuntimeFilterProducerBinding,
        build_keys: &[ExprId],
        eq_null_safe: &[bool],
        arena: &ExprArena,
        session: execution::RuntimeFilterSessionRef,
    ) -> Result<Self, String> {
        let build_expr = build_keys.get(spec.build_key_index).ok_or_else(|| {
            format!(
                "native runtime-filter binding_id={} join key ordinal {} is out of bounds for {} build keys",
                spec.binding_id(), spec.build_key_index, build_keys.len()
            )
        })?;
        if *build_expr != spec.build_expr_id {
            return Err(format!(
                "native runtime-filter binding_id={} build expression does not match join key ordinal {}",
                spec.binding_id(),
                spec.build_key_index
            ));
        }
        if eq_null_safe.get(spec.build_key_index).copied() != Some(false) {
            return Err(format!(
                "native runtime-filter binding_id={} requires a non-null-safe equality join key",
                spec.binding_id()
            ));
        }
        let data_type = arena.data_type(spec.build_expr_id).ok_or_else(|| {
            format!(
                "native runtime-filter binding_id={} build expression has no frozen data type",
                spec.binding_id()
            )
        })?;
        let RuntimeFilterExecutionContract::Membership(membership_schema) =
            spec.contract().contract()
        else {
            return Err(format!(
                "native runtime-filter binding_id={} hash join producer requires a membership contract",
                spec.binding_id()
            ));
        };
        if membership_schema.data_type() != data_type
            || membership_schema.null_semantics()
                != execution::RuntimeFilterNullSemantics::NeverMatches
        {
            return Err(format!(
                "native runtime-filter binding_id={} membership schema does not match build key ordinal {}",
                spec.binding_id(),
                spec.build_key_index
            ));
        }
        Ok(Self {
            join_key_ordinal: spec.build_key_index,
            contract: spec.contract().clone(),
            session,
            coordinator: Arc::new(NativeProducerInstanceCoordinator::default()),
        })
    }

    fn binding_id(&self) -> u32 {
        self.contract.binding_id().get()
    }
}

pub(crate) struct NativeRuntimeFilterProducerFactory {
    bindings: Vec<NativeMembershipProducerBinding>,
    local_partition_count: u32,
}

impl NativeRuntimeFilterProducerFactory {
    pub(crate) fn from_plan(
        specs: &[JoinRuntimeFilterProducerBinding],
        build_keys: &[ExprId],
        eq_null_safe: &[bool],
        arena: &ExprArena,
        session: execution::RuntimeFilterSessionRef,
        local_partition_count: i32,
    ) -> Result<Self, String> {
        let local_partition_count = u32::try_from(local_partition_count).map_err(|_| {
            format!(
                "native runtime-filter build DOP {local_partition_count} cannot be represented as a partition count"
            )
        })?;
        if local_partition_count == 0 {
            return Err("native runtime-filter build DOP must be positive".to_string());
        }
        let bindings = specs
            .iter()
            .map(|spec| {
                NativeMembershipProducerBinding::from_plan(
                    spec,
                    build_keys,
                    eq_null_safe,
                    arena,
                    Arc::clone(&session),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            bindings,
            local_partition_count,
        })
    }

    pub(crate) fn binding_count(&self) -> usize {
        self.bindings.len()
    }

    #[cfg(test)]
    pub(crate) fn for_test(
        session: execution::RuntimeFilterSessionRef,
        local_partition_count: u32,
    ) -> Result<Self, String> {
        if local_partition_count == 0 {
            return Err("native runtime-filter build DOP must be positive".to_string());
        }
        Ok(Self {
            bindings: vec![NativeMembershipProducerBinding::for_test(
                17,
                0,
                arrow::datatypes::DataType::Int32,
                session,
            )],
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
    ) -> Result<NativeRuntimeFilterProducerSet, String> {
        let actual_dop = u32::try_from(actual_dop).map_err(|_| {
            format!("native runtime-filter actual DOP {actual_dop} cannot be represented")
        })?;
        if actual_dop != self.local_partition_count {
            return Err(format!(
                "native runtime-filter build DOP drifted between factory build and operator creation: expected={} actual={actual_dop}",
                self.local_partition_count
            ));
        }
        let local_index = u32::try_from(local_index).map_err(|_| {
            format!("native runtime-filter local driver index {local_index} cannot be represented")
        })?;
        if local_index >= actual_dop {
            return Err(format!(
                "native runtime-filter local driver index {local_index} is outside DOP {actual_dop}"
            ));
        }
        Ok(NativeRuntimeFilterProducerSet {
            streams: self
                .bindings
                .iter()
                .cloned()
                .map(|binding| NativeMembershipProducerStream::new(binding, local_index))
                .collect(),
            completed: false,
        })
    }
}

pub(crate) struct NativeRuntimeFilterProducerSet {
    streams: Vec<NativeMembershipProducerStream>,
    completed: bool,
}

impl NativeRuntimeFilterProducerSet {
    pub(crate) fn bind(&mut self, local_partition_count: u32) -> Result<(), String> {
        for stream in &mut self.streams {
            if let Err(error) = stream.bind(local_partition_count) {
                let _ =
                    self.fail_incomplete(execution::RuntimeFilterProducerFailure::ExecutionFailed);
                return Err(error);
            }
        }
        Ok(())
    }

    pub(crate) fn submit(&mut self, key_arrays: &[ArrayRef]) -> Result<(), String> {
        if self.completed {
            return Ok(());
        }
        for index in 0..self.streams.len() {
            match self.streams[index].submit(key_arrays) {
                Ok(NativeMembershipSubmitOutcome::Applied) => {}
                Ok(NativeMembershipSubmitOutcome::Unavailable) => {
                    self.streams[index]
                        .fail(execution::RuntimeFilterProducerFailure::UpstreamUnavailable)?;
                }
                Err(error) => {
                    let _ = self
                        .fail_incomplete(execution::RuntimeFilterProducerFailure::ExecutionFailed);
                    self.completed = true;
                    return Err(error);
                }
            }
        }
        Ok(())
    }

    pub(crate) fn finish(&mut self) -> Result<(), String> {
        if self.completed {
            return Ok(());
        }
        for index in 0..self.streams.len() {
            if let Err(error) = self.streams[index].finish() {
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

impl Drop for NativeRuntimeFilterProducerSet {
    fn drop(&mut self) {
        if !self.completed {
            let _ = self.fail_incomplete(execution::RuntimeFilterProducerFailure::ExecutionFailed);
        }
    }
}

struct NativeMembershipProducerStream {
    binding: NativeMembershipProducerBinding,
    partition_id: execution::PartitionId,
    next_sequence: u64,
    terminal: bool,
    producer: Option<execution::RuntimeFilterProducerHandle>,
}

enum NativeMembershipSubmitOutcome {
    Applied,
    Unavailable,
}

impl NativeMembershipProducerStream {
    fn new(binding: NativeMembershipProducerBinding, local_index: u32) -> Self {
        Self {
            binding,
            partition_id: execution::PartitionId::new(local_index),
            next_sequence: 0,
            terminal: false,
            producer: None,
        }
    }

    fn bind(&mut self, local_partition_count: u32) -> Result<(), String> {
        if self.producer.is_some() || self.terminal {
            return Ok(());
        }
        let request = execution::RuntimeFilterProducerOpenRequest::new(
            self.binding.contract.clone(),
            local_partition_count,
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
                "native runtime-filter binding_id={} execution session open failed during operator bind: {error}",
                self.binding.binding_id()
            )),
        }
    }

    fn submit(&mut self, key_arrays: &[ArrayRef]) -> Result<NativeMembershipSubmitOutcome, String> {
        if self.terminal || self.binding.coordinator.failed.load(Ordering::Acquire) {
            return Ok(NativeMembershipSubmitOutcome::Applied);
        }
        let producer = self.producer.as_ref().ok_or_else(|| {
            format!(
                "native runtime-filter binding_id={} producer was not bound before build input",
                self.binding.binding_id()
            )
        })?;
        let array = key_arrays.get(self.binding.join_key_ordinal).ok_or_else(|| format!(
            "native runtime-filter binding_id={} build key ordinal {} is missing from evaluated arrays",
            self.binding.binding_id(), self.binding.join_key_ordinal
        ))?;
        let contributions = match execution::contribution::encode_membership_contributions(
            &self.binding.contract,
            array,
            producer.max_contribution_bytes(),
        )
        .map_err(|error| {
            format!(
                "native runtime-filter binding_id={} membership encoding failed: {error}",
                self.binding.binding_id()
            )
        })? {
            execution::contribution::MembershipContributionEncodingOutcome::Contributions(
                values,
            ) => values,
            execution::contribution::MembershipContributionEncodingOutcome::Unavailable(_) => {
                return Ok(NativeMembershipSubmitOutcome::Unavailable);
            }
        };
        for contribution in contributions {
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
                    return Ok(NativeMembershipSubmitOutcome::Applied);
                }
                Ok(_) => {}
                Err(error)
                    if error.kind()
                        == execution::RuntimeFilterContractViolationKind::SessionClosed =>
                {
                    self.mark_service_unavailable();
                    return Ok(NativeMembershipSubmitOutcome::Applied);
                }
                Err(error) => {
                    return Err(format!(
                        "native runtime-filter binding_id={} contribution failed: {error}",
                        self.binding.binding_id()
                    ));
                }
            }
            self.next_sequence = self.next_sequence.checked_add(1).ok_or_else(|| {
                format!(
                    "native runtime-filter binding_id={} producer sequence overflow",
                    self.binding.binding_id()
                )
            })?;
        }
        Ok(NativeMembershipSubmitOutcome::Applied)
    }

    fn finish(&mut self) -> Result<(), String> {
        if self.terminal || self.binding.coordinator.failed.load(Ordering::Acquire) {
            self.terminal = true;
            return Ok(());
        }
        let producer = self.producer.as_ref().ok_or_else(|| {
            format!(
                "native runtime-filter binding_id={} producer was not bound before finish",
                self.binding.binding_id()
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
                    "native runtime-filter binding_id={} close failed: {error}",
                    self.binding.binding_id()
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
                "native runtime-filter binding_id={} fail-open failed: {error}",
                self.binding.binding_id()
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use arrow::array::{ArrayRef, Int32Array};

    use super::{NativeRuntimeFilterProducerFactory, execution};

    #[derive(Debug, Eq, PartialEq)]
    enum Event {
        Submit {
            partition: u32,
            sequence: u64,
            kind: execution::RuntimeFilterContributionKind,
        },
        Close {
            partition: u32,
            sequence: u64,
        },
    }

    #[derive(Default)]
    struct RecordingProducer {
        events: Mutex<Vec<Event>>,
    }

    impl RecordingProducer {
        fn events(&self) -> Vec<Event> {
            self.events.lock().expect("events lock").drain(..).collect()
        }
    }

    impl execution::RuntimeFilterProducer for RecordingProducer {
        fn max_contribution_bytes(&self) -> usize {
            1024
        }

        fn submit(
            &self,
            partition: execution::PartitionId,
            sequence: execution::ProducerSequence,
            contribution: execution::RuntimeFilterContribution,
        ) -> Result<execution::RuntimeFilterSubmitOutcome, execution::RuntimeFilterContractViolation>
        {
            self.events
                .lock()
                .expect("events lock")
                .push(Event::Submit {
                    partition: partition.get(),
                    sequence: sequence.get(),
                    kind: contribution.kind(),
                });
            Ok(execution::RuntimeFilterSubmitOutcome::Applied)
        }

        fn close_partition(
            &self,
            partition: execution::PartitionId,
            sequence: execution::ProducerSequence,
        ) -> Result<execution::RuntimeFilterSubmitOutcome, execution::RuntimeFilterContractViolation>
        {
            self.events.lock().expect("events lock").push(Event::Close {
                partition: partition.get(),
                sequence: sequence.get(),
            });
            Ok(execution::RuntimeFilterSubmitOutcome::Completed)
        }

        fn fail(
            &self,
            _reason: execution::RuntimeFilterProducerFailure,
        ) -> Result<execution::RuntimeFilterSubmitOutcome, execution::RuntimeFilterContractViolation>
        {
            Ok(execution::RuntimeFilterSubmitOutcome::TerminalNoop)
        }
    }

    struct Session {
        producer: Arc<RecordingProducer>,
    }

    impl execution::RuntimeFilterSession for Session {
        fn open_producer(
            &self,
            _request: execution::RuntimeFilterProducerOpenRequest,
        ) -> Result<
            execution::RuntimeFilterBindOutcome<execution::RuntimeFilterProducerHandle>,
            execution::RuntimeFilterContractViolation,
        > {
            let producer = self.producer.clone() as execution::RuntimeFilterProducerHandle;
            Ok(execution::RuntimeFilterBindOutcome::Bound(producer))
        }

        fn subscribe(
            &self,
            _request: execution::RuntimeFilterSubscriptionRequest,
        ) -> Result<
            execution::RuntimeFilterBindOutcome<execution::RuntimeFilterSubscriptionHandle>,
            execution::RuntimeFilterContractViolation,
        > {
            Err(execution::RuntimeFilterContractViolation::new(
                execution::RuntimeFilterContractViolationKind::UnauthorizedBinding,
                "producer test session has no subscriptions",
            ))
        }

        fn open_final_domain_completion(
            &self,
            _request: execution::RuntimeFilterFinalDomainOpenRequest,
        ) -> Result<
            execution::RuntimeFilterBindOutcome<
                execution::RuntimeFilterFinalDomainCompletionHandle,
            >,
            execution::RuntimeFilterContractViolation,
        > {
            Err(execution::RuntimeFilterContractViolation::new(
                execution::RuntimeFilterContractViolationKind::UnauthorizedBinding,
                "producer test session has no final-domain completion",
            ))
        }
    }

    #[test]
    fn membership_producer_encodes_and_closes_through_execution_capability() {
        let recording = Arc::new(RecordingProducer::default());
        let session: execution::RuntimeFilterSessionRef = Arc::new(Session {
            producer: Arc::clone(&recording),
        });
        let factory = NativeRuntimeFilterProducerFactory::for_test(session, 1)
            .expect("test producer factory");
        let mut producers = factory
            .create_for_driver(1, 0)
            .expect("test producer stream");
        producers.bind(1).expect("bind producer");
        let arrays = vec![Arc::new(Int32Array::from(vec![2, 4, 2])) as ArrayRef];
        producers.submit(&arrays).expect("submit membership");
        producers.finish().expect("close producer");

        assert_eq!(
            recording.events(),
            vec![
                Event::Submit {
                    partition: 0,
                    sequence: 0,
                    kind: execution::RuntimeFilterContributionKind::Membership,
                },
                Event::Close {
                    partition: 0,
                    sequence: 1,
                },
            ]
        );
    }
}
