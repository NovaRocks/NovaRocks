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
#[cfg(test)]
use std::sync::{Condvar, Mutex, OnceLock, Weak};
#[cfg(test)]
use std::time::{Duration, Instant};

use arrow::array::ArrayRef;
use arrow::datatypes::DataType;

use crate::exec::expr::{ExprArena, ExprId};
use crate::exec::node::join::JoinRuntimeFilterProducerBinding;
use crate::exec::node::runtime_filter::{
    RuntimeFilterExecutionContract, RuntimeFilterExecutionReduction,
};
use crate::runtime_filter::exec::membership_delta::{
    MembershipDeltaEncoder, MembershipEncodingOutcome,
};
use crate::runtime_filter::model::contract::{
    BindingId, ChannelId, CompletionRequirement, ContributionKind, NullSemantics,
};
use crate::runtime_filter::port::artifact::ArtifactMembershipSchema;
use crate::runtime_filter::port::identity::{PartitionId, ProducerSequence};
use crate::runtime_filter::port::producer::{
    ProducerAdapter, ProducerFailureReason, ProducerPortKind, RuntimeContractViolationKind,
    SubmitOutcome,
};
use crate::runtime_filter::service::{
    InstalledRuntimeFilterExecutionContract, NativeRuntimeFilterExecutionContext,
};

#[derive(Default)]
struct NativeProducerInstanceCoordinator {
    failed: AtomicBool,
}

#[derive(Clone)]
enum NativeMembershipProducerSource {
    Installed(NativeRuntimeFilterExecutionContext),
    #[cfg(test)]
    Prebound {
        adapter: Arc<dyn ProducerAdapter>,
        max_contribution_bytes: usize,
    },
}

#[derive(Clone)]
pub(crate) struct NativeMembershipProducerBinding {
    binding_id: u32,
    channel_id: u32,
    join_key_ordinal: usize,
    data_type: DataType,
    contract: RuntimeFilterExecutionContract,
    contribution_kinds: BTreeSet<ContributionKind>,
    completion_requirement: CompletionRequirement,
    reduction: RuntimeFilterExecutionReduction,
    source: NativeMembershipProducerSource,
    coordinator: Arc<NativeProducerInstanceCoordinator>,
}

impl NativeMembershipProducerBinding {
    #[cfg(test)]
    pub(crate) fn for_test(
        binding_id: u32,
        join_key_ordinal: usize,
        data_type: DataType,
        max_contribution_bytes: usize,
        adapter: Arc<dyn ProducerAdapter>,
    ) -> Self {
        let schema = ArtifactMembershipSchema::new(&data_type, NullSemantics::NeverMatches)
            .expect("test membership schema");
        Self {
            binding_id,
            channel_id: binding_id,
            join_key_ordinal,
            data_type,
            contract: RuntimeFilterExecutionContract::Membership {
                canonical_schema: Arc::from(schema.canonical_bytes()),
                schema_digest: schema.digest().bytes(),
            },
            contribution_kinds: BTreeSet::from([
                ContributionKind::ValueDomainDelta,
                ContributionKind::ProducerClosed,
            ]),
            completion_requirement: CompletionRequirement::ProducerClosed,
            reduction: RuntimeFilterExecutionReduction::SetUnion,
            source: NativeMembershipProducerSource::Prebound {
                adapter,
                max_contribution_bytes,
            },
            coordinator: Arc::new(NativeProducerInstanceCoordinator::default()),
        }
    }

    fn from_plan(
        spec: &JoinRuntimeFilterProducerBinding,
        build_keys: &[ExprId],
        eq_null_safe: &[bool],
        arena: &ExprArena,
        context: NativeRuntimeFilterExecutionContext,
    ) -> Result<Self, String> {
        let build_expr = build_keys.get(spec.build_key_index).ok_or_else(|| {
            format!(
                "native runtime-filter binding_id={} join key ordinal {} is out of bounds for {} build keys",
                spec.binding_id,
                spec.build_key_index,
                build_keys.len()
            )
        })?;
        if *build_expr != spec.build_expr_id {
            return Err(format!(
                "native runtime-filter binding_id={} build expression does not match join key ordinal {}",
                spec.binding_id, spec.build_key_index
            ));
        }
        if eq_null_safe.get(spec.build_key_index).copied() != Some(false) {
            return Err(format!(
                "native runtime-filter binding_id={} requires a non-null-safe equality join key",
                spec.binding_id
            ));
        }
        let data_type = arena
            .data_type(spec.build_expr_id)
            .cloned()
            .ok_or_else(|| {
                format!(
                    "native runtime-filter binding_id={} build expression has no frozen data type",
                    spec.binding_id
                )
            })?;
        let expected_schema = ArtifactMembershipSchema::new(&data_type, NullSemantics::NeverMatches)
            .map_err(|error| {
                format!(
                    "native runtime-filter binding_id={} build expression has an unsupported membership schema: {error}",
                    spec.binding_id
                )
            })?;
        let RuntimeFilterExecutionContract::Membership {
            canonical_schema,
            schema_digest,
        } = &spec.contract
        else {
            return Err(format!(
                "native runtime-filter binding_id={} hash join producer requires a membership contract",
                spec.binding_id
            ));
        };
        if canonical_schema.as_ref() != expected_schema.canonical_bytes()
            || *schema_digest != expected_schema.digest().bytes()
        {
            return Err(format!(
                "native runtime-filter binding_id={} membership schema does not match build key ordinal {}",
                spec.binding_id, spec.build_key_index
            ));
        }
        let expected_kinds = BTreeSet::from([
            ContributionKind::ValueDomainDelta,
            ContributionKind::ProducerClosed,
        ]);
        if spec.contribution_kinds != expected_kinds
            || spec.completion_requirement != CompletionRequirement::ProducerClosed
            || spec.reduction != RuntimeFilterExecutionReduction::SetUnion
        {
            return Err(format!(
                "native runtime-filter binding_id={} hash join producer contract is not Membership + SetUnion + ProducerClosed",
                spec.binding_id
            ));
        }
        Ok(Self {
            binding_id: spec.binding_id,
            channel_id: spec.channel_id,
            join_key_ordinal: spec.build_key_index,
            data_type,
            contract: spec.contract.clone(),
            contribution_kinds: spec.contribution_kinds.clone(),
            completion_requirement: spec.completion_requirement,
            reduction: spec.reduction.clone(),
            source: NativeMembershipProducerSource::Installed(context),
            coordinator: Arc::new(NativeProducerInstanceCoordinator::default()),
        })
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
        context: NativeRuntimeFilterExecutionContext,
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
                    context.clone(),
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
        bindings: Vec<NativeMembershipProducerBinding>,
        local_partition_count: u32,
    ) -> Result<Self, String> {
        Ok(Self {
            bindings,
            local_partition_count,
        })
    }

    #[cfg(test)]
    fn binding_ids(&self) -> Vec<u32> {
        self.bindings
            .iter()
            .map(|binding| binding.binding_id)
            .collect()
    }

    pub(crate) fn binding_count(&self) -> usize {
        self.bindings.len()
    }

    pub(crate) const fn local_partition_count(&self) -> u32 {
        self.local_partition_count
    }

    pub(crate) fn create(
        &self,
        local_index: i32,
    ) -> Result<NativeRuntimeFilterProducerSet, String> {
        let local_index = u32::try_from(local_index).map_err(|_| {
            format!(
                "native runtime-filter pipeline local index {local_index} cannot be represented as a partition id"
            )
        })?;
        if local_index >= self.local_partition_count {
            return Err(format!(
                "native runtime-filter pipeline local index {local_index} is outside build DOP {}",
                self.local_partition_count
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
        for index in 0..self.streams.len() {
            if let Err(error) = self.streams[index].bind(local_partition_count) {
                let _ = self.fail_incomplete(ProducerFailureReason::ExecutionFailed);
                return Err(error);
            }
        }
        Ok(())
    }

    pub(crate) fn submit(&mut self, key_arrays: &[ArrayRef]) -> Result<(), String> {
        for index in 0..self.streams.len() {
            match self.streams[index].submit(key_arrays) {
                Ok(NativeMembershipSubmitOutcome::Applied) => {}
                Ok(NativeMembershipSubmitOutcome::Unavailable) => {
                    self.streams[index].fail(ProducerFailureReason::UpstreamUnavailable)?;
                }
                Err(error) => {
                    let _ = self.fail_incomplete(ProducerFailureReason::ExecutionFailed);
                    return Err(error);
                }
            }
        }
        Ok(())
    }

    pub(crate) fn finish(&mut self) -> Result<(), String> {
        for index in 0..self.streams.len() {
            if let Err(error) = self.streams[index].finish() {
                let _ = self.fail_incomplete(ProducerFailureReason::ExecutionFailed);
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
        self.fail_incomplete(reason)
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

impl Drop for NativeRuntimeFilterProducerSet {
    fn drop(&mut self) {
        if !self.completed {
            let _ = self.fail_incomplete(ProducerFailureReason::ExecutionFailed);
        }
    }
}

struct NativeMembershipProducerStream {
    binding: NativeMembershipProducerBinding,
    partition_id: PartitionId,
    next_sequence: u64,
    terminal: bool,
    adapter: Option<Arc<dyn ProducerAdapter>>,
    max_contribution_bytes: Option<usize>,
}

enum NativeMembershipSubmitOutcome {
    Applied,
    Unavailable,
}

impl NativeMembershipProducerStream {
    fn new(binding: NativeMembershipProducerBinding, local_index: u32) -> Self {
        #[cfg(test)]
        let (adapter, max_contribution_bytes) = match &binding.source {
            NativeMembershipProducerSource::Installed(_) => (None, None),
            NativeMembershipProducerSource::Prebound {
                adapter,
                max_contribution_bytes,
            } => (Some(Arc::clone(adapter)), Some(*max_contribution_bytes)),
        };
        #[cfg(not(test))]
        let (adapter, max_contribution_bytes) = (None, None);
        Self {
            binding,
            partition_id: PartitionId::new(local_index),
            next_sequence: 0,
            terminal: false,
            adapter,
            max_contribution_bytes,
        }
    }

    fn bind(&mut self, local_partition_count: u32) -> Result<(), String> {
        if self.adapter.is_some() {
            return Ok(());
        }
        let NativeMembershipProducerSource::Installed(context) = &self.binding.source else {
            return Err(format!(
                "native runtime-filter binding_id={} has no installed producer source",
                self.binding.binding_id
            ));
        };
        let resolved = match context.resolve_producer(
            BindingId::new(self.binding.binding_id),
            ChannelId::new(self.binding.channel_id),
            ProducerPortKind::Membership,
        ) {
            Ok(resolved) => resolved,
            Err(error) if error.kind() == RuntimeContractViolationKind::ServiceUnavailable => {
                self.mark_service_unavailable();
                return Ok(());
            }
            Err(error) => {
                return Err(format!(
                    "native runtime-filter binding_id={} resolution failed during operator bind: {error}",
                    self.binding.binding_id
                ));
            }
        };
        validate_resolved_binding(&self.binding, &resolved)?;
        let max_contribution_bytes = resolved.max_contribution_bytes();
        let adapter = match resolved.open_membership(local_partition_count) {
            Ok(adapter) => adapter,
            Err(error) if error.kind() == RuntimeContractViolationKind::ServiceUnavailable => {
                self.mark_service_unavailable();
                return Ok(());
            }
            Err(error) => {
                return Err(format!(
                    "native runtime-filter binding_id={} open failed during operator bind: {error}",
                    self.binding.binding_id
                ));
            }
        };
        self.adapter = Some(adapter);
        self.max_contribution_bytes = Some(max_contribution_bytes);
        Ok(())
    }

    fn submit(&mut self, key_arrays: &[ArrayRef]) -> Result<NativeMembershipSubmitOutcome, String> {
        if self.terminal || self.binding.coordinator.failed.load(Ordering::Acquire) {
            return Ok(NativeMembershipSubmitOutcome::Applied);
        }
        let adapter = self.adapter.as_ref().ok_or_else(|| {
            format!(
                "native runtime-filter binding_id={} producer was not bound before build input",
                self.binding.binding_id
            )
        })?;
        let max_contribution_bytes = self.max_contribution_bytes.ok_or_else(|| {
            format!(
                "native runtime-filter binding_id={} installed contribution budget is missing",
                self.binding.binding_id
            )
        })?;
        let array = key_arrays
            .get(self.binding.join_key_ordinal)
            .ok_or_else(|| {
                format!(
                    "native runtime-filter binding_id={} build key ordinal {} is missing from evaluated arrays",
                    self.binding.binding_id, self.binding.join_key_ordinal
                )
            })?;
        let outcome = MembershipDeltaEncoder::encode(
            array.as_ref(),
            &self.binding.data_type,
            max_contribution_bytes,
        )
        .map_err(|error| {
            format!(
                "native runtime-filter binding_id={} membership encoding failed: {error}",
                self.binding.binding_id
            )
        })?;
        let MembershipEncodingOutcome::Deltas(deltas) = outcome else {
            return Ok(NativeMembershipSubmitOutcome::Unavailable);
        };
        for delta in deltas {
            if delta.values().is_empty() && !delta.contains_null() {
                continue;
            }
            let outcome = match adapter.submit(
                self.partition_id,
                ProducerSequence::new(self.next_sequence),
                delta,
            ) {
                Ok(outcome) => outcome,
                Err(error) if error.kind() == RuntimeContractViolationKind::ServiceUnavailable => {
                    self.mark_service_unavailable();
                    return Ok(NativeMembershipSubmitOutcome::Applied);
                }
                Err(error) => {
                    return Err(format!(
                        "native runtime-filter binding_id={} contribution failed: {error}",
                        self.binding.binding_id
                    ));
                }
            };
            if outcome == SubmitOutcome::TerminalNoop {
                self.binding
                    .coordinator
                    .failed
                    .store(true, Ordering::Release);
                self.terminal = true;
                return Ok(NativeMembershipSubmitOutcome::Applied);
            }
            self.next_sequence = self.next_sequence.checked_add(1).ok_or_else(|| {
                format!(
                    "native runtime-filter binding_id={} producer sequence overflow",
                    self.binding.binding_id
                )
            })?;
        }
        Ok(NativeMembershipSubmitOutcome::Applied)
    }

    fn finish(&mut self) -> Result<(), String> {
        if self.terminal {
            return Ok(());
        }
        if self.binding.coordinator.failed.load(Ordering::Acquire) {
            self.terminal = true;
            return Ok(());
        }
        let adapter = self.adapter.as_ref().ok_or_else(|| {
            format!(
                "native runtime-filter binding_id={} producer was not bound before finish",
                self.binding.binding_id
            )
        })?;
        #[cfg(test)]
        if let NativeMembershipProducerSource::Installed(context) = &self.binding.source {
            wait_at_native_producer_close_gate_for_test(
                context.query_id(),
                BindingId::new(self.binding.binding_id),
                context.fragment_instance_id(),
                Duration::from_secs(5),
            );
        }
        let outcome = match adapter
            .close_partition(self.partition_id, ProducerSequence::new(self.next_sequence))
        {
            Ok(outcome) => outcome,
            Err(error) if error.kind() == RuntimeContractViolationKind::ServiceUnavailable => {
                self.mark_service_unavailable();
                return Ok(());
            }
            Err(error) => {
                return Err(format!(
                    "native runtime-filter binding_id={} close failed: {error}",
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

    fn mark_service_unavailable(&mut self) {
        self.binding
            .coordinator
            .failed
            .store(true, Ordering::Release);
        self.terminal = true;
    }

    fn fail(&mut self, reason: ProducerFailureReason) -> Result<(), String> {
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
        let Some(adapter) = self.adapter.as_ref() else {
            return Ok(());
        };
        if let Err(error) = adapter.fail(reason) {
            if error.kind() == RuntimeContractViolationKind::ServiceUnavailable {
                return Ok(());
            }
            return Err(format!(
                "native runtime-filter binding_id={} fail-open failed: {error}",
                self.binding.binding_id
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
pub(crate) struct NativeProducerCloseGate {
    state: Mutex<(bool, bool)>,
    changed: Condvar,
}

#[cfg(test)]
impl NativeProducerCloseGate {
    pub(crate) fn wait_entered(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut state = self.state.lock().expect("native producer close gate lock");
        while !state.0 {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return false;
            }
            let (next, result) = self
                .changed
                .wait_timeout(state, remaining)
                .expect("native producer close gate lock");
            state = next;
            if result.timed_out() && !state.0 {
                return false;
            }
        }
        true
    }

    pub(crate) fn release(&self) {
        let mut state = self.state.lock().expect("native producer close gate lock");
        state.1 = true;
        self.changed.notify_all();
    }
}

#[cfg(test)]
pub(crate) struct NativeProducerCloseGateGuard {
    key: (
        crate::common::types::UniqueId,
        BindingId,
        crate::common::types::UniqueId,
    ),
    gate: Arc<NativeProducerCloseGate>,
}

#[cfg(test)]
impl NativeProducerCloseGateGuard {
    pub(crate) fn wait_entered(&self, timeout: Duration) -> bool {
        self.gate.wait_entered(timeout)
    }

    pub(crate) fn release(&self) {
        self.gate.release();
    }
}

#[cfg(test)]
impl Drop for NativeProducerCloseGateGuard {
    fn drop(&mut self) {
        self.gate.release();
        let mut gates = native_producer_close_gates()
            .lock()
            .expect("native producer close gates lock");
        if gates
            .get(&self.key)
            .is_some_and(|registered| registered.ptr_eq(&Arc::downgrade(&self.gate)))
        {
            gates.remove(&self.key);
        }
    }
}

#[cfg(test)]
fn native_producer_close_gates() -> &'static Mutex<
    std::collections::BTreeMap<
        (
            crate::common::types::UniqueId,
            BindingId,
            crate::common::types::UniqueId,
        ),
        Weak<NativeProducerCloseGate>,
    >,
> {
    static GATES: OnceLock<
        Mutex<
            std::collections::BTreeMap<
                (
                    crate::common::types::UniqueId,
                    BindingId,
                    crate::common::types::UniqueId,
                ),
                Weak<NativeProducerCloseGate>,
            >,
        >,
    > = OnceLock::new();
    GATES.get_or_init(|| Mutex::new(std::collections::BTreeMap::new()))
}

#[cfg(test)]
pub(crate) fn install_native_producer_close_gate_for_test(
    query_id: crate::common::types::UniqueId,
    binding_id: BindingId,
    fragment_instance_id: crate::common::types::UniqueId,
) -> NativeProducerCloseGateGuard {
    let key = (query_id, binding_id, fragment_instance_id);
    let gate = Arc::new(NativeProducerCloseGate {
        state: Mutex::new((false, false)),
        changed: Condvar::new(),
    });
    native_producer_close_gates()
        .lock()
        .expect("native producer close gates lock")
        .insert(key, Arc::downgrade(&gate));
    NativeProducerCloseGateGuard { key, gate }
}

#[cfg(test)]
fn wait_at_native_producer_close_gate_for_test(
    query_id: crate::common::types::UniqueId,
    binding_id: BindingId,
    fragment_instance_id: crate::common::types::UniqueId,
    timeout: Duration,
) {
    let gate = native_producer_close_gates()
        .lock()
        .expect("native producer close gates lock")
        .get(&(query_id, binding_id, fragment_instance_id))
        .and_then(Weak::upgrade);
    let Some(gate) = gate else {
        return;
    };
    let deadline = Instant::now() + timeout;
    let mut state = gate.state.lock().expect("native producer close gate lock");
    state.0 = true;
    gate.changed.notify_all();
    while !state.1 {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return;
        }
        let (next, result) = gate
            .changed
            .wait_timeout(state, remaining)
            .expect("native producer close gate lock");
        state = next;
        if result.timed_out() && !state.1 {
            return;
        }
    }
}

fn validate_resolved_binding(
    binding: &NativeMembershipProducerBinding,
    resolved: &crate::runtime_filter::service::ResolvedNativeProducer,
) -> Result<(), String> {
    let contract_matches = match (&binding.contract, resolved.contract()) {
        (
            RuntimeFilterExecutionContract::Membership {
                canonical_schema: expected_schema,
                schema_digest: expected_digest,
            },
            InstalledRuntimeFilterExecutionContract::Membership {
                canonical_schema: actual_schema,
                schema_digest: actual_digest,
            },
        ) => expected_schema == actual_schema && expected_digest == actual_digest,
        _ => false,
    };
    if !contract_matches
        || binding.reduction != RuntimeFilterExecutionReduction::SetUnion
        || resolved.reduction_requirement()
            != crate::runtime_filter::model::contract::ReductionRequirement::SetUnion
        || binding.contribution_kinds != *resolved.allowed_contribution_kinds()
        || binding.completion_requirement != resolved.completion_requirement()
    {
        return Err(format!(
            "native runtime-filter binding_id={} installed producer contract drifted before operator bind",
            binding.binding_id
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use arrow::datatypes::DataType;

    use super::{
        NativeMembershipProducerBinding, NativeRuntimeFilterProducerFactory,
        install_native_producer_close_gate_for_test, native_producer_close_gates,
    };
    use crate::runtime_filter::model::contract::BindingId;
    use crate::runtime_filter::port::identity::{PartitionId, ProducerSequence};
    use crate::runtime_filter::port::producer::{
        ProducerAdapter, ProducerFailureReason, RuntimeContractViolation,
        RuntimeContractViolationKind, SubmitOutcome,
    };
    use crate::runtime_filter::port::value_domain::{MembershipValues, ValueDomainDelta};

    #[derive(Clone, Debug, Eq, PartialEq)]
    enum Event {
        Submit {
            partition: u32,
            sequence: u64,
            delta: ValueDomainDelta,
        },
        Close {
            partition: u32,
            terminal_sequence: u64,
        },
        Fail(ProducerFailureReason),
    }

    #[derive(Default)]
    struct RecordingProducer {
        events: Mutex<Vec<Event>>,
    }

    #[test]
    fn native_producer_close_gate_guard_removes_registry_key() {
        let query_id = crate::common::types::UniqueId { hi: 81, lo: 82 };
        let binding_id = BindingId::new(83);
        let finst_id = crate::common::types::UniqueId { hi: 84, lo: 85 };
        let key = (query_id, binding_id, finst_id);
        let guard = install_native_producer_close_gate_for_test(query_id, binding_id, finst_id);
        assert!(
            native_producer_close_gates()
                .lock()
                .unwrap()
                .contains_key(&key)
        );
        drop(guard);
        assert!(
            !native_producer_close_gates()
                .lock()
                .unwrap()
                .contains_key(&key)
        );
    }

    impl RecordingProducer {
        fn events(&self) -> Vec<Event> {
            self.events.lock().expect("recording producer").clone()
        }
    }

    impl ProducerAdapter for RecordingProducer {
        fn submit(
            &self,
            partition_id: PartitionId,
            sequence: ProducerSequence,
            delta: ValueDomainDelta,
        ) -> Result<SubmitOutcome, RuntimeContractViolation> {
            self.events
                .lock()
                .expect("recording producer")
                .push(Event::Submit {
                    partition: partition_id.get(),
                    sequence: sequence.get(),
                    delta,
                });
            Ok(SubmitOutcome::Applied)
        }

        fn close_partition(
            &self,
            partition_id: PartitionId,
            terminal_sequence: ProducerSequence,
        ) -> Result<SubmitOutcome, RuntimeContractViolation> {
            self.events
                .lock()
                .expect("recording producer")
                .push(Event::Close {
                    partition: partition_id.get(),
                    terminal_sequence: terminal_sequence.get(),
                });
            Ok(SubmitOutcome::Completed)
        }

        fn fail(
            &self,
            reason: ProducerFailureReason,
        ) -> Result<SubmitOutcome, RuntimeContractViolation> {
            self.events
                .lock()
                .expect("recording producer")
                .push(Event::Fail(reason));
            Ok(SubmitOutcome::CompletedWithoutArtifact)
        }
    }

    struct TerminalNoopProducer {
        recording: Arc<RecordingProducer>,
    }

    impl ProducerAdapter for TerminalNoopProducer {
        fn submit(
            &self,
            partition_id: PartitionId,
            sequence: ProducerSequence,
            delta: ValueDomainDelta,
        ) -> Result<SubmitOutcome, RuntimeContractViolation> {
            self.recording.submit(partition_id, sequence, delta)?;
            Ok(SubmitOutcome::TerminalNoop)
        }

        fn close_partition(
            &self,
            partition_id: PartitionId,
            terminal_sequence: ProducerSequence,
        ) -> Result<SubmitOutcome, RuntimeContractViolation> {
            self.recording
                .close_partition(partition_id, terminal_sequence)
        }

        fn fail(
            &self,
            reason: ProducerFailureReason,
        ) -> Result<SubmitOutcome, RuntimeContractViolation> {
            self.recording.fail(reason)
        }
    }

    struct ServiceUnavailableProducer {
        calls: Mutex<usize>,
    }

    impl ProducerAdapter for ServiceUnavailableProducer {
        fn submit(
            &self,
            _partition_id: PartitionId,
            _sequence: ProducerSequence,
            _delta: ValueDomainDelta,
        ) -> Result<SubmitOutcome, RuntimeContractViolation> {
            *self.calls.lock().expect("unavailable calls") += 1;
            Err(RuntimeContractViolation::new(
                RuntimeContractViolationKind::ServiceUnavailable,
                "injected service shutdown",
            ))
        }

        fn close_partition(
            &self,
            _partition_id: PartitionId,
            _terminal_sequence: ProducerSequence,
        ) -> Result<SubmitOutcome, RuntimeContractViolation> {
            *self.calls.lock().expect("unavailable calls") += 1;
            Err(RuntimeContractViolation::new(
                RuntimeContractViolationKind::ServiceUnavailable,
                "injected service shutdown",
            ))
        }

        fn fail(
            &self,
            _reason: ProducerFailureReason,
        ) -> Result<SubmitOutcome, RuntimeContractViolation> {
            *self.calls.lock().expect("unavailable calls") += 1;
            Err(RuntimeContractViolation::new(
                RuntimeContractViolationKind::ServiceUnavailable,
                "injected service shutdown",
            ))
        }
    }

    struct CloseErrorProducer {
        recording: Arc<RecordingProducer>,
    }

    impl ProducerAdapter for CloseErrorProducer {
        fn submit(
            &self,
            partition_id: PartitionId,
            sequence: ProducerSequence,
            delta: ValueDomainDelta,
        ) -> Result<SubmitOutcome, RuntimeContractViolation> {
            self.recording.submit(partition_id, sequence, delta)
        }

        fn close_partition(
            &self,
            _partition_id: PartitionId,
            _terminal_sequence: ProducerSequence,
        ) -> Result<SubmitOutcome, RuntimeContractViolation> {
            Err(RuntimeContractViolation::new(
                RuntimeContractViolationKind::InvalidPartition,
                "injected close failure",
            ))
        }

        fn fail(
            &self,
            reason: ProducerFailureReason,
        ) -> Result<SubmitOutcome, RuntimeContractViolation> {
            self.recording.fail(reason)
        }
    }

    fn binding(
        binding_id: u32,
        join_key_ordinal: usize,
        data_type: DataType,
        max_contribution_bytes: usize,
        producer: Arc<RecordingProducer>,
    ) -> NativeMembershipProducerBinding {
        let adapter: Arc<dyn ProducerAdapter> = producer;
        NativeMembershipProducerBinding::for_test(
            binding_id,
            join_key_ordinal,
            data_type,
            max_contribution_bytes,
            adapter,
        )
    }

    fn int64(values: Vec<i64>) -> ArrayRef {
        Arc::new(Int64Array::from(values))
    }

    #[test]
    fn native_build_opens_exact_membership_binding() {
        let producer = Arc::new(RecordingProducer::default());
        let factory = NativeRuntimeFilterProducerFactory::for_test(
            vec![binding(17, 0, DataType::Int64, 1024, Arc::clone(&producer))],
            1,
        )
        .expect("factory");
        let mut stream = factory.create(0).expect("partition stream");

        stream.submit(&[int64(vec![11])]).expect("submit");

        assert_eq!(factory.binding_ids(), vec![17]);
        assert!(matches!(
            producer.events().as_slice(),
            [Event::Submit {
                partition: 0,
                sequence: 0,
                ..
            }]
        ));
    }

    #[test]
    fn native_build_uses_pipeline_local_index_as_rf_partition() {
        let producer = Arc::new(RecordingProducer::default());
        let factory = NativeRuntimeFilterProducerFactory::for_test(
            vec![binding(17, 0, DataType::Int64, 1024, Arc::clone(&producer))],
            4,
        )
        .expect("factory");
        let mut stream = factory.create(2).expect("partition stream");

        stream.submit(&[int64(vec![11])]).expect("submit");
        stream.finish().expect("finish");

        let events = producer.events();
        assert_eq!(events.len(), 2, "events={events:?}");
        assert!(events.iter().all(|event| match event {
            Event::Submit { partition, .. } | Event::Close { partition, .. } => *partition == 2,
            Event::Fail(_) => false,
        }));
    }

    #[test]
    fn broadcast_dop_gt_one_uses_rf_partitions_zero_through_dop_minus_one_even_when_hash_partition_is_zero()
     {
        let producer = Arc::new(RecordingProducer::default());
        let factory = NativeRuntimeFilterProducerFactory::for_test(
            vec![binding(17, 0, DataType::Int64, 1024, Arc::clone(&producer))],
            3,
        )
        .expect("factory");

        for local_index in 0..3 {
            let mut stream = factory.create(local_index).expect("partition stream");
            stream
                .submit(&[int64(vec![i64::from(local_index)])])
                .expect("submit");
            stream.finish().expect("finish");
        }

        let mut closed = producer
            .events()
            .into_iter()
            .filter_map(|event| match event {
                Event::Close { partition, .. } => Some(partition),
                _ => None,
            })
            .collect::<Vec<_>>();
        closed.sort_unstable();
        assert_eq!(closed, vec![0, 1, 2]);
    }

    #[test]
    fn native_build_submits_each_join_key_ordinal_independently() {
        let first = Arc::new(RecordingProducer::default());
        let second = Arc::new(RecordingProducer::default());
        let factory = NativeRuntimeFilterProducerFactory::for_test(
            vec![
                binding(17, 0, DataType::Int64, 1024, Arc::clone(&first)),
                binding(19, 1, DataType::Int64, 1024, Arc::clone(&second)),
            ],
            1,
        )
        .expect("factory");
        let mut stream = factory.create(0).expect("partition stream");

        stream
            .submit(&[int64(vec![11]), int64(vec![29])])
            .expect("submit");

        assert!(matches!(
            first.events().as_slice(),
            [Event::Submit { sequence: 0, delta, .. }]
                if delta.values() == &MembershipValues::int64([11])
        ));
        assert!(matches!(
            second.events().as_slice(),
            [Event::Submit { sequence: 0, delta, .. }]
                if delta.values() == &MembershipValues::int64([29])
        ));
    }

    #[test]
    fn native_build_splits_bounded_deltas() {
        let producer = Arc::new(RecordingProducer::default());
        let exact_one =
            crate::runtime_filter::exec::membership_delta::MembershipDeltaEncoder::encode(
                &Int64Array::from(vec![1]),
                &DataType::Int64,
                usize::MAX,
            )
            .unwrap()
            .into_deltas()
            .unwrap()[0]
                .canonical_encoded_len()
                .unwrap();
        let factory = NativeRuntimeFilterProducerFactory::for_test(
            vec![binding(
                17,
                0,
                DataType::Int64,
                exact_one,
                Arc::clone(&producer),
            )],
            1,
        )
        .expect("factory");
        let mut stream = factory.create(0).expect("partition stream");

        stream.submit(&[int64(vec![1, 2, 3, 4])]).expect("submit");

        let events = producer.events();
        let submits = events
            .iter()
            .filter(|event| matches!(event, Event::Submit { .. }))
            .collect::<Vec<_>>();
        assert!(submits.len() > 1, "events={events:?}");
        for (expected_sequence, event) in submits.into_iter().enumerate() {
            let Event::Submit {
                sequence, delta, ..
            } = event
            else {
                unreachable!()
            };
            assert_eq!(*sequence, expected_sequence as u64);
            assert!(delta.canonical_encoded_len().unwrap() <= exact_one);
        }
    }

    #[test]
    fn native_oversized_scalar_fails_filter_open() {
        let producer = Arc::new(RecordingProducer::default());
        let factory = NativeRuntimeFilterProducerFactory::for_test(
            vec![binding(17, 0, DataType::Utf8, 64, Arc::clone(&producer))],
            1,
        )
        .expect("factory");
        let mut stream = factory.create(0).expect("partition stream");

        stream
            .submit(&[Arc::new(StringArray::from(vec!["x".repeat(512)]))])
            .expect("runtime-filter size failure must not fail the join");

        assert_eq!(
            producer.events(),
            vec![Event::Fail(ProducerFailureReason::UpstreamUnavailable)]
        );
    }

    #[test]
    fn native_empty_partition_closes_at_sequence_zero() {
        let producer = Arc::new(RecordingProducer::default());
        let factory = NativeRuntimeFilterProducerFactory::for_test(
            vec![binding(17, 0, DataType::Int64, 1024, Arc::clone(&producer))],
            1,
        )
        .expect("factory");
        let mut stream = factory.create(0).expect("partition stream");

        stream.finish().expect("finish empty partition");

        assert_eq!(
            producer.events(),
            vec![Event::Close {
                partition: 0,
                terminal_sequence: 0,
            }]
        );
    }

    #[test]
    fn native_finish_closes_each_partition_once() {
        let producer = Arc::new(RecordingProducer::default());
        let factory = NativeRuntimeFilterProducerFactory::for_test(
            vec![binding(17, 0, DataType::Int64, 1024, Arc::clone(&producer))],
            1,
        )
        .expect("factory");
        let mut stream = factory.create(0).expect("partition stream");
        stream.submit(&[int64(vec![11])]).expect("submit");

        stream.finish().expect("first finish");
        stream.finish().expect("duplicate finish");
        drop(stream);

        assert_eq!(
            producer
                .events()
                .iter()
                .filter(|event| matches!(event, Event::Close { .. }))
                .count(),
            1
        );
    }

    #[test]
    fn native_first_cancel_or_error_fails_instance_once() {
        let producer = Arc::new(RecordingProducer::default());
        let factory = NativeRuntimeFilterProducerFactory::for_test(
            vec![binding(17, 0, DataType::Int64, 1024, Arc::clone(&producer))],
            2,
        )
        .expect("factory");
        let mut first = factory.create(0).expect("first partition");
        let mut sibling = factory.create(1).expect("sibling partition");

        first
            .fail(ProducerFailureReason::Cancelled)
            .expect("first failure");
        sibling
            .fail(ProducerFailureReason::ExecutionFailed)
            .expect("sibling failure");

        assert_eq!(
            producer
                .events()
                .iter()
                .filter(|event| matches!(event, Event::Fail(_)))
                .count(),
            1
        );
    }

    #[test]
    fn native_sibling_finish_drop_after_failure_is_idempotent() {
        let producer = Arc::new(RecordingProducer::default());
        let factory = NativeRuntimeFilterProducerFactory::for_test(
            vec![binding(17, 0, DataType::Int64, 1024, Arc::clone(&producer))],
            2,
        )
        .expect("factory");
        let mut first = factory.create(0).expect("first partition");
        let mut sibling = factory.create(1).expect("sibling partition");

        first
            .fail(ProducerFailureReason::ExecutionFailed)
            .expect("first failure");
        sibling.finish().expect("sibling finish after failure");
        drop(sibling);
        drop(first);

        assert_eq!(
            producer.events(),
            vec![Event::Fail(ProducerFailureReason::ExecutionFailed)]
        );
    }

    #[test]
    fn native_terminal_noop_stops_siblings_without_second_failure() {
        let recording = Arc::new(RecordingProducer::default());
        let adapter: Arc<dyn ProducerAdapter> = Arc::new(TerminalNoopProducer {
            recording: Arc::clone(&recording),
        });
        let factory = NativeRuntimeFilterProducerFactory::for_test(
            vec![NativeMembershipProducerBinding::for_test(
                17,
                0,
                DataType::Int64,
                1024,
                adapter,
            )],
            2,
        )
        .expect("factory");
        let mut first = factory.create(0).expect("first partition");
        let mut sibling = factory.create(1).expect("sibling partition");

        first.submit(&[int64(vec![11])]).expect("terminal noop");
        sibling.submit(&[int64(vec![29])]).expect("sibling no-op");
        sibling.finish().expect("sibling finish no-op");

        assert_eq!(
            recording.events(),
            vec![Event::Submit {
                partition: 0,
                sequence: 0,
                delta: ValueDomainDelta::new(MembershipValues::int64([11]), false),
            }]
        );
    }

    #[test]
    fn native_service_unavailable_is_terminal_fail_open_for_join() {
        let adapter = Arc::new(ServiceUnavailableProducer {
            calls: Mutex::new(0),
        });
        let producer: Arc<dyn ProducerAdapter> = adapter.clone();
        let factory = NativeRuntimeFilterProducerFactory::for_test(
            vec![NativeMembershipProducerBinding::for_test(
                17,
                0,
                DataType::Int64,
                1024,
                producer,
            )],
            2,
        )
        .expect("factory");
        let mut first = factory.create(0).expect("first partition");
        let mut sibling = factory.create(1).expect("sibling partition");

        first
            .submit(&[int64(vec![11])])
            .expect("ServiceUnavailable must not fail the join");
        sibling
            .submit(&[int64(vec![29])])
            .expect("terminal sibling is a no-op");
        first.finish().expect("terminal finish");
        sibling.finish().expect("terminal sibling finish");

        assert_eq!(*adapter.calls.lock().expect("unavailable calls"), 1);
    }

    #[test]
    fn native_multi_binding_close_error_rolls_back_already_closed_binding() {
        let first = Arc::new(RecordingProducer::default());
        let second = Arc::new(RecordingProducer::default());
        let second_adapter: Arc<dyn ProducerAdapter> = Arc::new(CloseErrorProducer {
            recording: Arc::clone(&second),
        });
        let factory = NativeRuntimeFilterProducerFactory::for_test(
            vec![
                binding(17, 0, DataType::Int64, 1024, Arc::clone(&first)),
                NativeMembershipProducerBinding::for_test(
                    19,
                    1,
                    DataType::Int64,
                    1024,
                    second_adapter,
                ),
            ],
            1,
        )
        .expect("factory");
        let mut stream = factory.create(0).expect("partition stream");

        let error = stream
            .finish()
            .expect_err("second binding close failure must be structural");

        assert!(error.contains("injected close failure"), "{error}");
        assert_eq!(
            first.events(),
            vec![
                Event::Close {
                    partition: 0,
                    terminal_sequence: 0,
                },
                Event::Fail(ProducerFailureReason::ExecutionFailed),
            ]
        );
        assert_eq!(
            second.events(),
            vec![Event::Fail(ProducerFailureReason::ExecutionFailed)]
        );
    }
}
