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

use crate::runtime::task::{ExecutionFailure, ExecutionResult};
fn producer_contract(detail: String) -> ExecutionFailure {
    execution::RuntimeFilterContractViolation::new(
        execution::RuntimeFilterContractViolationKind::ContractMismatch,
        detail,
    )
    .into()
}
fn producer_partition(detail: String) -> ExecutionFailure {
    execution::RuntimeFilterContractViolation::new(
        execution::RuntimeFilterContractViolationKind::InvalidPartitionCount,
        detail,
    )
    .into()
}
use crate::runtime_filter as execution;
use arrow::array::ArrayRef;

use crate::runtime_filter::RuntimeFilterExecutionContract;

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

    /// The checks every hash-join membership producer passes, whatever
    /// supplies its build key: the key's null-safe equality flag fixes the
    /// membership NULL semantics, and `key_type`, the frozen type of the
    /// evaluated build key, must be the membership key type.
    fn checked(
        contract: &execution::RuntimeFilterProducerContract,
        join_key_ordinal: usize,
        eq_null_safe: &[bool],
        key_type: impl FnOnce() -> ExecutionResult<arrow::datatypes::DataType>,
        session: execution::RuntimeFilterSessionRef,
    ) -> ExecutionResult<Self> {
        let binding_id = contract.binding_id().get();
        let expected_null_semantics = match eq_null_safe.get(join_key_ordinal).copied() {
            Some(false) => execution::RuntimeFilterNullSemantics::NeverMatches,
            Some(true) => execution::RuntimeFilterNullSemantics::NullSafeEqual,
            None => {
                return Err(producer_contract(format!(
                    "native runtime-filter binding_id={binding_id} join key ordinal {join_key_ordinal} has no null-safe equality flag"
                )));
            }
        };
        let data_type = key_type()?;
        if contract.kind() != execution::RuntimeFilterProducerKind::Membership {
            return Err(producer_contract(format!(
                "native runtime-filter binding_id={binding_id} hash join producer requires a membership producer contract"
            )));
        }
        let RuntimeFilterExecutionContract::Membership(membership_schema) = contract.contract()
        else {
            return Err(producer_contract(format!(
                "native runtime-filter binding_id={binding_id} hash join producer requires a membership contract"
            )));
        };
        if membership_schema.data_type() != &data_type
            || membership_schema.null_semantics() != expected_null_semantics
        {
            return Err(producer_contract(format!(
                "native runtime-filter binding_id={binding_id} membership schema does not match build key ordinal {join_key_ordinal}"
            )));
        }
        Ok(Self {
            join_key_ordinal,
            contract: contract.clone(),
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
    /// The membership producers of one compiled hash join. Each producer is
    /// the build-key ordinal it observes and its runtime contract;
    /// `build_key_type` gives the static type of the compiled build-key root
    /// at an ordinal, which is the type of every key array it evaluates.
    pub(crate) fn from_compiled(
        producers: &[(usize, execution::RuntimeFilterProducerContract)],
        eq_null_safe: &[bool],
        mut build_key_type: impl FnMut(usize) -> ExecutionResult<arrow::datatypes::DataType>,
        session: execution::RuntimeFilterSessionRef,
        local_partition_count: i32,
    ) -> ExecutionResult<Self> {
        let local_partition_count = checked_local_partition_count(local_partition_count)?;
        let bindings = producers
            .iter()
            .map(|(join_key_ordinal, contract)| {
                NativeMembershipProducerBinding::checked(
                    contract,
                    *join_key_ordinal,
                    eq_null_safe,
                    || build_key_type(*join_key_ordinal),
                    Arc::clone(&session),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            bindings,
            local_partition_count,
        })
    }

    #[cfg(test)]
    pub(crate) fn for_test(
        session: execution::RuntimeFilterSessionRef,
        local_partition_count: u32,
    ) -> ExecutionResult<Self> {
        if local_partition_count == 0 {
            return Err(producer_partition(
                "native runtime-filter build DOP must be positive".to_string(),
            ));
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
    ) -> ExecutionResult<NativeRuntimeFilterProducerSet> {
        let actual_dop = u32::try_from(actual_dop).map_err(|_| {
            producer_partition(format!(
                "native runtime-filter actual DOP {actual_dop} cannot be represented"
            ))
        })?;
        if actual_dop != self.local_partition_count {
            return Err(producer_partition(format!(
                "native runtime-filter build DOP drifted between factory build and operator creation: expected={} actual={actual_dop}",
                self.local_partition_count
            )));
        }
        let local_index = u32::try_from(local_index).map_err(|_| {
            producer_partition(format!(
                "native runtime-filter local driver index {local_index} cannot be represented"
            ))
        })?;
        if local_index >= actual_dop {
            return Err(producer_partition(format!(
                "native runtime-filter local driver index {local_index} is outside DOP {actual_dop}"
            )));
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

fn checked_local_partition_count(local_partition_count: i32) -> ExecutionResult<u32> {
    let local_partition_count = u32::try_from(local_partition_count).map_err(|_| {
        producer_partition(format!(
            "native runtime-filter build DOP {local_partition_count} cannot be represented as a partition count"
        ))
    })?;
    if local_partition_count == 0 {
        return Err(producer_partition(
            "native runtime-filter build DOP must be positive".to_string(),
        ));
    }
    Ok(local_partition_count)
}

pub(crate) struct NativeRuntimeFilterProducerSet {
    streams: Vec<NativeMembershipProducerStream>,
    completed: bool,
}

impl NativeRuntimeFilterProducerSet {
    pub(crate) fn bind(&mut self, local_partition_count: u32) -> ExecutionResult<()> {
        for stream in &mut self.streams {
            if let Err(error) = stream.bind(local_partition_count) {
                let _ =
                    self.fail_incomplete(execution::RuntimeFilterProducerFailure::ExecutionFailed);
                return Err(error);
            }
        }
        Ok(())
    }

    pub(crate) fn submit(&mut self, key_arrays: &[ArrayRef]) -> ExecutionResult<()> {
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

    pub(crate) fn finish(&mut self) -> ExecutionResult<()> {
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
    ) -> ExecutionResult<()> {
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
    ) -> ExecutionResult<()> {
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
    producer: Option<execution::SharedRuntimeFilterProducer>,
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

    fn bind(&mut self, local_partition_count: u32) -> ExecutionResult<()> {
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
            Err(error) => Err(error.into()),
        }
    }

    fn submit(
        &mut self,
        key_arrays: &[ArrayRef],
    ) -> ExecutionResult<NativeMembershipSubmitOutcome> {
        if self.terminal || self.binding.coordinator.failed.load(Ordering::Acquire) {
            return Ok(NativeMembershipSubmitOutcome::Applied);
        }
        let producer = self.producer.as_ref().ok_or_else(|| {
            producer_contract(format!(
                "native runtime-filter binding_id={} producer was not bound before build input",
                self.binding.binding_id()
            ))
        })?;
        let array = key_arrays.get(self.binding.join_key_ordinal).ok_or_else(|| producer_contract(format!(
            "native runtime-filter binding_id={} build key ordinal {} is missing from evaluated arrays",
            self.binding.binding_id(), self.binding.join_key_ordinal
        )))?;
        let contributions = match execution::contribution::encode_membership_contributions(
            &self.binding.contract,
            array,
            producer.max_contribution_bytes(),
        )
        .map_err(ExecutionFailure::from_domain)?
        {
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
                    return Err(error.into());
                }
            }
            self.next_sequence = self.next_sequence.checked_add(1).ok_or_else(|| {
                execution::producer_sequence_extent(self.binding.binding_id(), self.next_sequence)
            })?;
        }
        Ok(NativeMembershipSubmitOutcome::Applied)
    }

    fn finish(&mut self) -> ExecutionResult<()> {
        if self.terminal || self.binding.coordinator.failed.load(Ordering::Acquire) {
            self.terminal = true;
            return Ok(());
        }
        let producer = self.producer.as_ref().ok_or_else(|| {
            producer_contract(format!(
                "native runtime-filter binding_id={} producer was not bound before finish",
                self.binding.binding_id()
            ))
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
                return Err(error.into());
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

    fn fail(&mut self, reason: execution::RuntimeFilterProducerFailure) -> ExecutionResult<()> {
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
            return Err(error.into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use arrow::array::{ArrayRef, Int32Array};
    use arrow::datatypes::DataType;

    use super::{NativeRuntimeFilterProducerFactory, execution};
    use crate::runtime_filter::RuntimeFilterExecutionContract;

    #[derive(Debug, Eq, PartialEq)]
    enum Event {
        Submit {
            partition: u32,
            sequence: u64,
            contribution: execution::RuntimeFilterContribution,
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
                    contribution,
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
        producer_open_count: AtomicUsize,
    }

    impl Session {
        fn producer_open_count(&self) -> usize {
            self.producer_open_count.load(Ordering::Acquire)
        }
    }

    impl execution::RuntimeFilterSession for Session {
        fn open_producer(
            &self,
            _request: execution::RuntimeFilterProducerOpenRequest,
        ) -> Result<
            execution::RuntimeFilterBindOutcome<execution::SharedRuntimeFilterProducer>,
            execution::RuntimeFilterContractViolation,
        > {
            self.producer_open_count.fetch_add(1, Ordering::AcqRel);
            let producer = self.producer.clone() as execution::SharedRuntimeFilterProducer;
            Ok(execution::RuntimeFilterBindOutcome::Bound(producer))
        }

        fn subscribe(
            &self,
            _request: execution::RuntimeFilterSubscriptionRequest,
        ) -> Result<
            execution::RuntimeFilterBindOutcome<execution::RuntimeFilterSubscriptionPin>,
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
                execution::SharedRuntimeFilterFinalDomainCompletion,
            >,
            execution::RuntimeFilterContractViolation,
        > {
            Err(execution::RuntimeFilterContractViolation::new(
                execution::RuntimeFilterContractViolationKind::UnauthorizedBinding,
                "producer test session has no final-domain completion",
            ))
        }
    }

    fn producer_binding(
        null_semantics: execution::RuntimeFilterNullSemantics,
    ) -> (usize, execution::RuntimeFilterProducerContract) {
        let schema =
            execution::RuntimeFilterMembershipSchema::new(&DataType::Int32, null_semantics)
                .expect("test membership schema");
        let contract = execution::RuntimeFilterProducerContract::membership(
            execution::RuntimeFilterBindingId::new(17),
            execution::RuntimeFilterChannelId::new(23),
            RuntimeFilterExecutionContract::Membership(schema),
        )
        .expect("test membership producer contract");
        (0, contract)
    }

    #[test]
    fn membership_producer_requires_exact_null_semantics_for_join_key() {
        for (eq_null_safe, null_semantics, accepted) in [
            (
                false,
                execution::RuntimeFilterNullSemantics::NeverMatches,
                true,
            ),
            (
                true,
                execution::RuntimeFilterNullSemantics::NullSafeEqual,
                true,
            ),
            (
                false,
                execution::RuntimeFilterNullSemantics::NullSafeEqual,
                false,
            ),
            (
                true,
                execution::RuntimeFilterNullSemantics::NeverMatches,
                false,
            ),
        ] {
            let recording = Arc::new(RecordingProducer::default());
            let session = Arc::new(Session {
                producer: Arc::clone(&recording),
                producer_open_count: AtomicUsize::new(0),
            });
            let session_ref: execution::RuntimeFilterSessionRef = session.clone();
            let spec = producer_binding(null_semantics);

            let factory = NativeRuntimeFilterProducerFactory::from_compiled(
                &[spec],
                &[eq_null_safe],
                |_| Ok(DataType::Int32),
                session_ref,
                1,
            );

            assert_eq!(factory.is_ok(), accepted);
            assert_eq!(session.producer_open_count(), 0);
            assert!(recording.events().is_empty());
        }
    }

    #[test]
    fn membership_producer_rejects_non_membership_kind_before_open() {
        let recording = Arc::new(RecordingProducer::default());
        let session = Arc::new(Session {
            producer: Arc::clone(&recording),
            producer_open_count: AtomicUsize::new(0),
        });
        let session_ref: execution::RuntimeFilterSessionRef = session.clone();
        let schema = execution::RuntimeFilterMembershipSchema::new(
            &DataType::Int32,
            execution::RuntimeFilterNullSemantics::NeverMatches,
        )
        .expect("membership schema");
        let contract = execution::RuntimeFilterProducerContract::final_domain(
            execution::RuntimeFilterBindingId::new(17),
            execution::RuntimeFilterChannelId::new(23),
            RuntimeFilterExecutionContract::Membership(schema),
        )
        .expect("final-domain producer contract");
        let spec = (0, contract);

        let error = match NativeRuntimeFilterProducerFactory::from_compiled(
            &[spec],
            &[false],
            |_| Ok(DataType::Int32),
            session_ref,
            1,
        ) {
            Ok(_) => panic!("non-membership producer kind must fail"),
            Err(error) => error,
        };

        assert!(
            error
                .to_string()
                .contains("requires a membership producer contract")
        );
        assert_eq!(session.producer_open_count(), 0);
        assert!(recording.events().is_empty());
    }

    #[test]
    fn membership_producer_encodes_and_closes_through_execution_capability() {
        let recording = Arc::new(RecordingProducer::default());
        let session: execution::RuntimeFilterSessionRef = Arc::new(Session {
            producer: Arc::clone(&recording),
            producer_open_count: AtomicUsize::new(0),
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

        let events = recording.events();
        let [
            Event::Submit {
                partition,
                sequence,
                contribution,
            },
            Event::Close {
                partition: close_partition,
                sequence: close_sequence,
            },
        ] = events.as_slice()
        else {
            panic!("expected one membership contribution followed by close: {events:?}");
        };
        assert_eq!((*partition, *sequence), (0, 0));
        assert_eq!(
            contribution.kind(),
            execution::RuntimeFilterContributionKind::Membership
        );
        assert_eq!((*close_partition, *close_sequence), (0, 1));
    }

    #[test]
    fn null_safe_membership_producer_preserves_null_in_contribution() {
        let recording = Arc::new(RecordingProducer::default());
        let session = Arc::new(Session {
            producer: Arc::clone(&recording),
            producer_open_count: AtomicUsize::new(0),
        });
        let session_ref: execution::RuntimeFilterSessionRef = session;
        let spec = producer_binding(execution::RuntimeFilterNullSemantics::NullSafeEqual);
        let factory = NativeRuntimeFilterProducerFactory::from_compiled(
            &[spec],
            &[true],
            |_| Ok(DataType::Int32),
            session_ref,
            1,
        )
        .expect("null-safe membership binding is accepted");
        let mut producers = factory
            .create_for_driver(1, 0)
            .expect("test producer stream");
        producers.bind(1).expect("bind producer");
        let arrays = vec![Arc::new(Int32Array::from(vec![Some(2), None])) as ArrayRef];
        producers.submit(&arrays).expect("submit membership");
        producers.finish().expect("close producer");

        let events = recording.events();
        let [
            Event::Submit {
                partition,
                sequence,
                contribution,
            },
            Event::Close {
                partition: close_partition,
                sequence: close_sequence,
            },
        ] = events.as_slice()
        else {
            panic!("expected one membership contribution followed by close: {events:?}");
        };
        assert_eq!((*partition, *sequence), (0, 0));
        assert_eq!((*close_partition, *close_sequence), (0, 1));
        let decoded = execution::contribution::decode_contribution(
            contribution.canonical_bytes(),
            &contribution.contract_digest(),
            execution::contribution::ContributionCodecExpectation::membership(
                &DataType::Int32,
                contribution.contract_digest(),
            ),
            usize::MAX,
        )
        .expect("decode null-safe membership contribution");
        assert_eq!(
            decoded,
            execution::contribution::RuntimeFilterContribution::membership(
                execution::contribution::ValueDomainDelta::new(
                    execution::contribution::MembershipValues::int32([2]),
                    true,
                ),
            )
        );
    }
}
