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

#[path = "approx_distinct_hash.rs"]
mod hash;
#[path = "approx_distinct_sketch.rs"]
mod sketch;

use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::aggregate_scalar::{BinaryResultBuilder, ScalarWork};
use crate::kernel_control::{KernelControlObservation, invalid};
use crate::kernel_input::EvaluationCheckpoints;
use crate::{
    AggregateCallContract, AggregateStateAllocator, AggregateStateMemoryPolicy, FunctionValueType,
    KernelEvaluationControl, KernelFailure, PreparedAggregateKernel, SelectedAggregateMergeInput,
    SelectedAggregateUpdateInput,
};
use arrow_array::{Array, ArrayRef, BinaryArray, BooleanArray, builder::Int64Builder};
use arrow_schema::DataType;
use hash::HashProfile;
use novarocks_type_contract::{CompilePhase, PureCompileControl};
use sketch::Sketch;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ApproxProfile {
    Unknown,
    Boolean,
    Hash(HashProfile),
}

impl ApproxProfile {
    pub(super) fn prepare(value: &FunctionValueType) -> Option<Self> {
        match value.data_type() {
            DataType::Null => Some(Self::Unknown),
            DataType::Boolean => Some(Self::Boolean),
            _ => HashProfile::prepare(value).map(Self::Hash),
        }
    }

    fn new_state(self, allocator: &HostAggregateAllocator) -> State {
        match self {
            Self::Unknown => State::Unknown,
            Self::Boolean => State::Boolean(0),
            Self::Hash(_) => State::Hash(Sketch::new(allocator.clone())),
        }
    }
}

enum State {
    Unknown,
    Boolean(u8),
    Hash(Sketch<HostAggregateAllocator>),
    Failed,
}

pub(crate) struct ApproxDistinctState {
    allocator: HostAggregateAllocator,
    value: State,
}

#[derive(Debug)]
pub(crate) struct ApproxDistinctKernel {
    pub(super) contract: Arc<AggregateCallContract>,
    pub(super) profile: ApproxProfile,
}
pub(super) fn observed<T>(
    control: &dyn KernelEvaluationControl,
    f: impl FnOnce(&mut ScalarWork<'_, '_>) -> Result<T, KernelFailure>,
) -> Result<T, KernelFailure> {
    let observation = KernelControlObservation::new(control);
    observation.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(&observation);
    let result = f(&mut ScalarWork::new(Some(&mut work)));
    observation.finish(work.finish_result(result))
}

impl PreparedAggregateKernel for ApproxDistinctKernel {
    type State = ApproxDistinctState;
    type PreparedUpdateBatch<'a> = SelectedAggregateUpdateInput<'a, 'a>;
    type PreparedMergeBatch<'a> = SelectedAggregateMergeInput<'a, 'a>;

    fn contract(&self) -> &Arc<AggregateCallContract> {
        &self.contract
    }

    fn supports_local_stages(&self) -> bool {
        !self.contract.distinct() && self.contract.order_keys().is_empty()
    }
    fn clone_for_local_phase(
        &self,
        contract: Arc<AggregateCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<Self>, KernelFailure> {
        control
            .checkpoint(CompilePhase::FunctionSpecialization, 0)
            .map_err(crate::kernel_control::compile_failure)?;
        Ok(Arc::new(Self {
            contract,
            profile: self.profile,
        }))
    }

    fn memory_policy(&self) -> AggregateStateMemoryPolicy {
        AggregateStateMemoryPolicy::AllocationTracked
    }

    fn create_state(
        &self,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::State, KernelFailure> {
        control.checkpoint(0)?;
        Err(invalid("approx_distinct requires its host state allocator"))
    }

    fn create_state_with_allocator(
        &self,
        allocator: Option<Arc<dyn AggregateStateAllocator>>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::State, KernelFailure> {
        observed(control, |work| {
            work.flush()?;
            let allocator =
                HostAggregateAllocator::try_new(allocator.ok_or_else(|| {
                    invalid("approx_distinct requires its host state allocator")
                })?)?;
            let value = self.profile.new_state(&allocator);
            Ok(ApproxDistinctState { allocator, value })
        })
    }

    fn retained_bytes(&self, state: &Self::State) -> usize {
        state.allocator.metadata_bytes()
            + match &state.value {
                State::Hash(sketch) => sketch.retained_bytes(),
                _ => 0,
            }
    }

    fn prepare_update<'a>(
        &'a self,
        input: Self::PreparedUpdateBatch<'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedUpdateBatch<'a>, KernelFailure> {
        observed(control, |work| {
            work.step()?;
            if !std::ptr::eq(input.contract(), self.contract.as_ref())
                || !self.contract.phase().consumes_logical_arguments()
                || input.logical_arguments().len() != 1
                || !input.order_arguments().is_empty()
            {
                return Err(invalid(
                    "approx_distinct update differs from its exact selected call",
                ));
            }
            Ok(input)
        })
    }

    fn update_row<'a>(
        &self,
        state: &mut Self::State,
        input: &Self::PreparedUpdateBatch<'a>,
        ordinal: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        let result = observed(control, |work| {
            let row = input
                .selection()
                .row(ordinal)
                .ok_or_else(|| invalid("approx_distinct update ordinal is not selected"))?;
            let argument = input.logical_arguments()[0];
            let input = argument.array();
            let index = argument.value_row(ordinal, row);
            work.step()?;
            if index >= input.len() {
                return Err(invalid(
                    "approx_distinct selected value address is out of bounds",
                ));
            }
            match (&mut state.value, self.profile) {
                (State::Unknown, ApproxProfile::Unknown)
                    if input.data_type() == &DataType::Null => {}
                (State::Boolean(bits), ApproxProfile::Boolean) => {
                    let array = input
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .ok_or_else(|| invalid("approx_distinct BOOLEAN carrier mismatch"))?;
                    if !array.is_null(index) {
                        *bits |= 1 << u8::from(array.value(index));
                    }
                }
                (State::Hash(sketch), ApproxProfile::Hash(profile)) => {
                    if let Some(hash) = profile.hash(input.as_ref(), index, work)? {
                        sketch.add_hash(hash, work)?;
                    }
                }
                (State::Failed, _) => return Err(KernelFailure::InstanceFailed),
                _ => {
                    return Err(invalid(
                        "approx_distinct cannot update a foreign state profile",
                    ));
                }
            }
            Ok(())
        });
        if result.is_err() {
            state.value = State::Failed;
        }
        result
    }

    fn prepare_merge<'a>(
        &'a self,
        input: Self::PreparedMergeBatch<'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedMergeBatch<'a>, KernelFailure> {
        observed(control, |work| {
            work.step()?;
            if !std::ptr::eq(input.contract(), self.contract.as_ref())
                || self.contract.phase().consumes_logical_arguments()
            {
                return Err(invalid(
                    "approx_distinct merge differs from its exact selected state",
                ));
            }
            Ok(input)
        })
    }

    fn merge_row<'a>(
        &self,
        state: &mut Self::State,
        input: &Self::PreparedMergeBatch<'a>,
        ordinal: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        let result = observed(control, |work| {
            let row = input
                .selection()
                .row(ordinal)
                .ok_or_else(|| invalid("approx_distinct merge ordinal is not selected"))?;
            let argument = input.state();
            let array = argument
                .array()
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| {
                    invalid("approx_distinct merge requires its binary state carrier")
                })?;
            let index = argument.value_row(ordinal, row);
            work.step()?;
            if index >= array.len() || array.is_null(index) {
                return Err(invalid(
                    "approx_distinct has no state at the selected merge address",
                ));
            }
            let payload = array.value(index);
            match (&mut state.value, self.profile) {
                (State::Unknown, ApproxProfile::Unknown) if payload == b"NU\x01" => Ok(()),
                (State::Boolean(bits), ApproxProfile::Boolean) => {
                    let [b'N', b'B', 1, value] = payload else {
                        return Err(invalid("approx_distinct BOOLEAN state format mismatch"));
                    };
                    if *value > 3 {
                        return Err(invalid("approx_distinct BOOLEAN state bit set is invalid"));
                    }
                    *bits |= *value;
                    Ok(())
                }
                (State::Hash(sketch), ApproxProfile::Hash(profile)) => {
                    sketch.merge(payload, profile.transport_profile(), work)
                }
                (State::Failed, _) => Err(KernelFailure::InstanceFailed),
                _ => Err(invalid("approx_distinct has a foreign merge profile")),
            }
        });
        if result.is_err() {
            state.value = State::Failed;
        }
        result
    }

    fn build_intermediate<'s, I>(
        &self,
        states: I,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        I: ExactSizeIterator<Item = &'s Self::State>,
    {
        observed(control, |work| {
            if self.contract.phase().produces_final_result() {
                return Err(invalid(
                    "approx_distinct final phase cannot emit intermediate state",
                ));
            }
            let mut output = BinaryResultBuilder::new();
            for state in states {
                work.step()?;
                match (&state.value, self.profile) {
                    (State::Unknown, ApproxProfile::Unknown) => output.append_value(b"NU\x01")?,
                    (State::Boolean(bits), ApproxProfile::Boolean) => {
                        output.append_value(&[b'N', b'B', 1, *bits])?
                    }
                    (State::Hash(sketch), ApproxProfile::Hash(profile)) => {
                        let bytes = sketch.encode(profile.transport_profile(), work)?;
                        work.flush()?;
                        output.append_value(&bytes)?;
                    }
                    (State::Failed, _) => return Err(KernelFailure::InstanceFailed),
                    _ => {
                        return Err(invalid(
                            "approx_distinct has a foreign output state profile",
                        ));
                    }
                }
            }
            Ok(Arc::new(output.finish()?) as ArrayRef)
        })
    }

    fn build_final<'s, I>(
        &self,
        states: I,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        I: ExactSizeIterator<Item = &'s Self::State>,
    {
        observed(control, |work| {
            if !self.contract.phase().produces_final_result() {
                return Err(invalid(
                    "approx_distinct intermediate phase cannot emit a final result",
                ));
            }
            let mut output = Int64Builder::with_capacity(states.len());
            for state in states {
                work.step()?;
                let result = match (&state.value, self.profile) {
                    (State::Unknown, ApproxProfile::Unknown) => 0,
                    (State::Boolean(bits), ApproxProfile::Boolean) => i64::from(bits.count_ones()),
                    (State::Hash(sketch), ApproxProfile::Hash(_)) => sketch.estimate(work)?,
                    (State::Failed, _) => return Err(KernelFailure::InstanceFailed),
                    _ => {
                        return Err(invalid(
                            "approx_distinct has a foreign output state profile",
                        ));
                    }
                };
                output.append_value(result);
            }
            Ok(Arc::new(output.finish()) as ArrayRef)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;
    use std::{alloc::Layout, ptr::NonNull};

    struct Host;
    impl AggregateStateAllocator for Host {
        fn allocate(&self, layout: Layout) -> Result<NonNull<u8>, KernelFailure> {
            NonNull::new(unsafe { std::alloc::alloc(layout) })
                .ok_or(KernelFailure::ResourceExhausted)
        }

        unsafe fn release(&self, pointer: NonNull<u8>, layout: Layout) {
            unsafe { std::alloc::dealloc(pointer.as_ptr(), layout) }
        }
    }

    #[test]
    fn approx_distinct_sparse_and_dense_merge_match_single_with_fixed_accuracy_sample() {
        let profile = HashProfile::Int64;
        let allocator = HostAggregateAllocator::try_new(Arc::new(Host)).unwrap();
        let mut work = ScalarWork::new(None);
        let mut variance = 0.0;
        for group in 0..32 {
            let values = Int64Array::from_iter_values((1..=8192).map(|value| group * 8192 + value));
            let mut single = Sketch::new(allocator.clone());
            let mut merged = Sketch::new(allocator.clone());
            for partition in [0..64, 64..4096, 4096..8192] {
                let mut partial = Sketch::new(allocator.clone());
                for row in partition {
                    let hash = profile.hash(&values, row, &mut work).unwrap().unwrap();
                    single.add_hash(hash, &mut work).unwrap();
                    partial.add_hash(hash, &mut work).unwrap();
                }
                let bytes = partial
                    .encode(profile.transport_profile(), &mut work)
                    .unwrap();
                merged
                    .merge(&bytes, profile.transport_profile(), &mut work)
                    .unwrap();
            }
            let estimate = single.estimate(&mut work).unwrap();
            assert_eq!(estimate, merged.estimate(&mut work).unwrap());
            assert_eq!(
                single
                    .encode(profile.transport_profile(), &mut work)
                    .unwrap()
                    .as_slice(),
                merged
                    .encode(profile.transport_profile(), &mut work)
                    .unwrap()
                    .as_slice(),
            );
            variance += (estimate as f64 / 8192.0 - 1.0).powi(2);
        }
        let rmse = (variance / 32.0).sqrt();
        assert!(rmse <= 0.046, "fixed-sample relative RMSE {rmse}");
    }

    #[test]
    fn approx_distinct_uses_the_full_hash_and_rejects_foreign_state() {
        let profile = HashProfile::Int64;
        let allocator = HostAggregateAllocator::try_new(Arc::new(Host)).unwrap();
        let mut sketch = Sketch::new(allocator);
        let mut work = ScalarWork::new(None);
        for hash in [0, 0, u64::MAX, 1 << 63] {
            sketch.add_hash(hash, &mut work).unwrap();
        }
        assert_eq!(sketch.estimate(&mut work).unwrap(), 3);
        let bytes = sketch
            .encode(profile.transport_profile(), &mut work)
            .unwrap();
        assert!(
            sketch
                .merge(&bytes, HashProfile::Date.transport_profile(), &mut work)
                .is_err()
        );
        assert!(
            sketch
                .merge(&[1, 2, 0, 0, 0, 0], profile.transport_profile(), &mut work)
                .is_err()
        );
    }
}
