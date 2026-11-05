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

//! Installed preparation capability for ordinary consumer shapes.

use super::{
    AcceptedInput, AdmissionNext, DriverStep, InputPreparation, InputPreparationFailure,
    PreparationStep, RawInput, TransformKernel,
};
use crate::exec::pipeline::operator::{
    DictionaryCarrierStats, InputConstructionFactory, InputEncodingPolicy,
    dictionary_carrier_stats, hydrate_for_downstream_with_factory,
};
use crate::runtime::profile::{CounterRef, OperatorProfiles};
use crate::runtime::{
    mem_tracker::MemTracker,
    task::{ExecutionFailure, ExecutionResult},
};
use std::sync::Arc;

#[derive(Default)]
pub struct HydrateInput {
    tracker: Option<Arc<MemTracker>>,
    construction: Option<Arc<dyn InputConstructionFactory>>,
    profile: Option<InputProfile>,
}
struct InputProfile {
    counters: [CounterRef; 7],
}
impl InputProfile {
    fn new(profiles: OperatorProfiles) -> Self {
        let counters = profiles.input_encoding_counters();
        Self { counters }
    }
    fn record(&self, stats: DictionaryCarrierStats) {
        let values = [
            stats.input_rows,
            stats.input_columns,
            stats.kept_rows,
            stats.kept_columns,
            stats.hydrated_rows,
            stats.hydrated_columns,
            stats.unsupported_columns,
        ];
        for (counter, value) in self.counters.iter().zip(values) {
            counter.add(value);
        }
    }
}

#[derive(Debug)]
pub(super) enum ReceiverContractFailure {
    RootGrant,
    InputAfterFinish,
    MissingRootGrant,
    ForeignRootTask,
}
impl std::fmt::Display for ReceiverContractFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::RootGrant => "ordinary receiver cannot accept a root-covered input",
            Self::InputAfterFinish => "receiver was given input after data flow finished",
            Self::MissingRootGrant => "root input reached its receiver without pre-pull coverage",
            Self::ForeignRootTask => "root input grant belongs to a different Task",
        })
    }
}
impl std::error::Error for ReceiverContractFailure {}
impl novarocks_failure_contract::FailureProjection for ReceiverContractFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use crate::runtime::task::ExecutionReason;
        use novarocks_failure_contract::{FailureClass, FailureFact};
        let reason = match self {
            Self::RootGrant => ExecutionReason::ReceiverUnexpectedRootGrant,
            Self::InputAfterFinish => ExecutionReason::ReceiverInputAfterFinish,
            Self::MissingRootGrant => ExecutionReason::ReceiverMissingRootGrant,
            Self::ForeignRootTask => ExecutionReason::ReceiverForeignRootTask,
        };
        FailureFact::new(FailureClass::Invariant, reason.reason())
    }
}

impl HydrateInput {
    pub(crate) fn validate_uncovered(input: &RawInput) -> ExecutionResult<()> {
        if input.permit().is_some() {
            return Err(ExecutionFailure::from_domain(
                ReceiverContractFailure::RootGrant,
            ));
        }
        Ok(())
    }
    pub(crate) fn accept_raw(&mut self, input: RawInput) -> AcceptedInput {
        input.accept(self.tracker.as_ref())
    }
    pub(crate) fn prepare_accepted<K: InputEncodingPolicy + ?Sized>(
        &mut self,
        ctx: &mut DriverStep<'_>,
        input: AcceptedInput,
        kernel: &K,
    ) -> Result<PreparationStep, InputPreparationFailure> {
        input
            .prepare(|chunk| {
                let stats = self
                    .profile
                    .as_ref()
                    .map(|_| dictionary_carrier_stats(chunk, kernel));
                let prepared = hydrate_for_downstream_with_factory(
                    chunk,
                    kernel,
                    self.construction.as_deref(),
                    ctx.value_conversions(),
                )?;
                if let (Some(profile), Some(stats)) = (&self.profile, stats) {
                    profile.record(stats);
                }
                Ok(prepared)
            })
            .map(PreparationStep::Ready)
    }
    pub(crate) fn install_profiles(&mut self, profiles: OperatorProfiles) {
        self.profile = Some(InputProfile::new(profiles));
    }
    pub(crate) fn install_tracker(&mut self, tracker: Arc<MemTracker>) {
        self.tracker = Some(tracker);
    }
    pub(crate) fn install_factory(&mut self, factory: Arc<dyn InputConstructionFactory>) {
        self.construction = Some(factory);
    }
}

impl<K: InputEncodingPolicy + TransformKernel> InputPreparation<K> for HydrateInput {
    fn admission(
        &mut self,
        ctx: &mut DriverStep<'_>,
        input: &RawInput,
        kernel: &K,
    ) -> ExecutionResult<Option<AdmissionNext>> {
        Self::validate_uncovered(input)?;
        kernel.admission(ctx, input)
    }
    fn accept(&mut self, input: RawInput) -> AcceptedInput {
        self.accept_raw(input)
    }
    fn prepare(
        &mut self,
        ctx: &mut DriverStep<'_>,
        input: AcceptedInput,
        kernel: &K,
    ) -> Result<PreparationStep, InputPreparationFailure> {
        self.prepare_accepted(ctx, input, kernel)
    }
    fn creates_typed_storage(&self) -> bool {
        true
    }
    fn set_profiles(&mut self, profiles: OperatorProfiles) {
        self.install_profiles(profiles);
    }
    fn set_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
        self.install_tracker(tracker);
    }
    fn set_construction_factory(&mut self, factory: Arc<dyn InputConstructionFactory>) {
        self.install_factory(factory);
    }
}

/// Original-carrier preparation under an already acquired root grant. This
/// policy neither reads columns nor repeats the producer's pre-pull admission.
pub(crate) struct RootInputPreparation;
pub(crate) struct RootInputTarget {
    pub task: novarocks_execution_contract::TaskIdentity,
}
impl InputPreparation<RootInputTarget> for RootInputPreparation {
    fn admission(
        &mut self,
        _: &mut DriverStep<'_>,
        raw: &RawInput,
        target: &RootInputTarget,
    ) -> ExecutionResult<Option<AdmissionNext>> {
        let permit = raw.permit().ok_or_else(|| {
            ExecutionFailure::from_domain(ReceiverContractFailure::MissingRootGrant)
        })?;
        if permit.task() != target.task {
            return Err(ExecutionFailure::from_domain(
                ReceiverContractFailure::ForeignRootTask,
            ));
        }
        Ok(None)
    }
    fn accept(&mut self, raw: RawInput) -> AcceptedInput {
        raw.accept(None)
    }
    fn prepare(
        &mut self,
        _: &mut DriverStep<'_>,
        input: AcceptedInput,
        _: &RootInputTarget,
    ) -> Result<PreparationStep, InputPreparationFailure> {
        Ok(PreparationStep::Ready(input.prepare_original()))
    }
}
