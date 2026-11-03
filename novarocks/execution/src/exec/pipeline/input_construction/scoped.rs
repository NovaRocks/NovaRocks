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

use super::{
    ConstructionFailure, DecodedInputSourceCustody, InputConstructionFactory,
    InputConstructionOwner,
};
use crate::exec::chunk::{ArrayNormalizationStep, ArrayPathStep, ChunkSchema};
use novarocks_type_contract::{
    CompileControlError, ValueTypeStorageHold, ValueTypeStorageLifetime,
};
use std::{
    alloc::Layout,
    sync::{Arc, Mutex},
};

/// Explicit caller profile. No runtime default or cumulative preparation
/// allowance is substituted for a new construction's finite bounds.
#[derive(Clone, Copy, Debug)]
pub struct InputConstructionProfile {
    pub max_requests: usize,
    pub max_request_bytes: usize,
    pub max_opaque_requests: usize,
    pub max_opaque_bytes: usize,
    pub max_work: usize,
    pub max_depth: usize,
}

/// Original caller's logical structure observation. These callbacks neither
/// mint a capacity grant nor publish allocator facts. Release balances the
/// successful requests after the actual constructed storage loses its hold.
pub trait InputConstructionRequestHost: Send + Sync {
    fn request(&self, layout: Layout) -> Result<(), ConstructionFailure>;
    fn observe_opaque_bound(&self, bytes_upper: usize) -> Result<(), ConstructionFailure>;
    fn release_requested_bytes(&self, bytes: usize);
}
pub trait InputConstructionWorkControl: Send + Sync {
    fn checkpoint(&self, units: usize) -> Result<(), ConstructionFailure>;
}

/// An explicitly classified locked-library observation, never an exact Layout
/// invoice for an opaque private Arrow allocation.
pub use novarocks_functions::array_projection::ArrayProjectionOpaqueBound as OpaqueConstructionBound;

/// Locked operation model supplied independently of capacity/control. It must
/// cover every operation it accepts; unsupported branches fail before running.
pub trait InputConstructionOperationModel: Send + Sync {
    fn observe(
        &self,
        step: ArrayNormalizationStep<'_>,
        request: &mut dyn FnMut(Layout) -> Result<(), ConstructionFailure>,
        opaque: &mut dyn FnMut(OpaqueConstructionBound) -> Result<(), ConstructionFailure>,
        work: &mut dyn FnMut(usize) -> Result<(), ConstructionFailure>,
    ) -> Result<(), ConstructionFailure>;
}

#[derive(Default)]
struct Usage {
    requests: usize,
    opaque_requests: usize,
    exact_bytes: usize,
    opaque_bytes: usize,
    work: usize,
}
struct CreatorGuard {
    host: Arc<dyn InputConstructionRequestHost>,
    profile: InputConstructionProfile,
    usage: Mutex<Usage>,
}
impl ValueTypeStorageHold for CreatorGuard {}
impl Drop for CreatorGuard {
    fn drop(&mut self) {
        let usage = self.usage.get_mut().expect("construction usage lock");
        self.host
            .release_requested_bytes(usage.exact_bytes + usage.opaque_bytes);
    }
}
fn exhausted() -> ConstructionFailure {
    CompileControlError::ResourceExhausted.into()
}
fn arc_layout<T>() -> Result<Layout, ConstructionFailure> {
    novarocks_type_contract::owned_resources::layout::arc_layout(Layout::new::<T>())
        .map_err(ConstructionFailure::from)
}
impl CreatorGuard {
    fn request(&self, layout: Layout) -> Result<(), ConstructionFailure> {
        let mut usage = self.usage.lock().expect("construction usage lock");
        let count = usage.requests.checked_add(1).ok_or_else(exhausted)?;
        let bytes = usage
            .exact_bytes
            .checked_add(layout.size())
            .ok_or_else(exhausted)?;
        if count > self.profile.max_requests || bytes > self.profile.max_request_bytes {
            return Err(exhausted());
        }
        if bytes
            .checked_add(usage.opaque_bytes)
            .is_none_or(|total| total > i64::MAX as usize)
        {
            return Err(exhausted());
        }
        self.host.request(layout)?;
        usage.requests = count;
        usage.exact_bytes = bytes;
        Ok(())
    }
    fn opaque(&self, bound: OpaqueConstructionBound) -> Result<(), ConstructionFailure> {
        let mut usage = self.usage.lock().expect("construction usage lock");
        let count = usage
            .opaque_requests
            .checked_add(bound.requests_upper)
            .ok_or_else(exhausted)?;
        let bytes = usage
            .opaque_bytes
            .checked_add(bound.bytes_upper)
            .ok_or_else(exhausted)?;
        let work = usage
            .work
            .checked_add(bound.work_upper)
            .ok_or_else(exhausted)?;
        if count > self.profile.max_opaque_requests
            || bytes > self.profile.max_opaque_bytes
            || work > self.profile.max_work
            || bytes
                .checked_add(usage.exact_bytes)
                .is_none_or(|total| total > i64::MAX as usize)
        {
            return Err(exhausted());
        }
        self.host.observe_opaque_bound(bound.bytes_upper)?;
        usage.opaque_requests = count;
        usage.opaque_bytes = bytes;
        usage.work = work;
        Ok(())
    }
    fn work(&self, units: usize) -> Result<(), ConstructionFailure> {
        let mut usage = self.usage.lock().expect("construction usage lock");
        let work = usage.work.checked_add(units).ok_or_else(exhausted)?;
        if work > self.profile.max_work {
            return Err(exhausted());
        }
        usage.work = work;
        Ok(())
    }
}

pub struct ObservedInputConstructionFactory {
    profile: InputConstructionProfile,
    host: Arc<dyn InputConstructionRequestHost>,
    control: Arc<dyn InputConstructionWorkControl>,
    model: Arc<dyn InputConstructionOperationModel>,
    decoded_source_custody: Option<Arc<dyn DecodedInputSourceCustody>>,
    _creation: Arc<CreatorGuard>,
}
impl ObservedInputConstructionFactory {
    /// Pure geometry of the SAME actual scope holder allocations. This is
    /// request evidence only; the original host still admits both creations.
    pub fn schema_scope_allocation_layouts()
    -> Result<[Layout; 2], novarocks_type_contract::owned_resources::layout::LayoutResourceError>
    {
        Ok([
            novarocks_type_contract::owned_resources::layout::arc_layout(Layout::new::<
                CreatorGuard,
            >())?,
            Layout::new::<Scope>(),
        ])
    }
    pub fn try_new(
        profile: InputConstructionProfile,
        host: Arc<dyn InputConstructionRequestHost>,
        control: Arc<dyn InputConstructionWorkControl>,
        model: Arc<dyn InputConstructionOperationModel>,
    ) -> Result<Arc<Self>, ConstructionFailure> {
        Self::try_new_inner(profile, host, control, model, None)
    }
    /// The real runtime composition supplies this capability explicitly. Pure
    /// observer-only factories do not acquire an allocator by inference.
    pub fn try_new_with_decoded_source_custody(
        profile: InputConstructionProfile,
        host: Arc<dyn InputConstructionRequestHost>,
        control: Arc<dyn InputConstructionWorkControl>,
        model: Arc<dyn InputConstructionOperationModel>,
        custody: Arc<dyn DecodedInputSourceCustody>,
    ) -> Result<Arc<Self>, ConstructionFailure> {
        Self::try_new_inner(profile, host, control, model, Some(custody))
    }
    fn try_new_inner(
        profile: InputConstructionProfile,
        host: Arc<dyn InputConstructionRequestHost>,
        control: Arc<dyn InputConstructionWorkControl>,
        model: Arc<dyn InputConstructionOperationModel>,
        decoded_source_custody: Option<Arc<dyn DecodedInputSourceCustody>>,
    ) -> Result<Arc<Self>, ConstructionFailure> {
        if profile.max_requests == 0
            || profile.max_request_bytes == 0
            || profile.max_work == 0
            || profile.max_request_bytes > i64::MAX as usize
            || profile.max_opaque_bytes > i64::MAX as usize
            || profile.max_depth == 0
            || profile.max_depth > novarocks_type_contract::MAX_VALUE_TYPE_DEPTH
        {
            return Err(ConstructionFailure::InvalidSource(
                "input construction profile is invalid".into(),
            ));
        }
        control.checkpoint(0)?;
        let creation = Self::creator(profile, host.clone())?;
        creation.request(arc_layout::<Self>()?)?;
        Ok(Arc::new(Self {
            profile,
            host,
            control,
            model,
            decoded_source_custody,
            _creation: creation,
        }))
    }
    fn creator(
        profile: InputConstructionProfile,
        host: Arc<dyn InputConstructionRequestHost>,
    ) -> Result<Arc<CreatorGuard>, ConstructionFailure> {
        let layout = Self::schema_scope_allocation_layouts()?[0];
        if profile.max_requests < 1 || layout.size() > profile.max_request_bytes {
            return Err(exhausted());
        }
        host.request(layout)?;
        Ok(Arc::new(CreatorGuard {
            host,
            profile,
            usage: Mutex::new(Usage {
                requests: 1,
                exact_bytes: layout.size(),
                ..Usage::default()
            }),
        }))
    }
    fn begin_scope(&self) -> Result<Box<dyn InputConstructionOwner>, ConstructionFailure> {
        self.control.checkpoint(0)?;
        let guard = Self::creator(self.profile, self.host.clone())?;
        guard.request(Self::schema_scope_allocation_layouts()?[1])?;
        Ok(Box::new(Scope {
            guard,
            control: self.control.clone(),
            model: self.model.clone(),
        }))
    }
}
impl InputConstructionFactory for ObservedInputConstructionFactory {
    fn retain_decoded_source(
        &self,
        array: arrow::array::ArrayRef,
        storage: ValueTypeStorageLifetime,
        work: &dyn InputConstructionOwner,
    ) -> Result<arrow::array::ArrayRef, ConstructionFailure> {
        let custody = self
            .decoded_source_custody
            .as_ref()
            .ok_or(ConstructionFailure::MissingOwner)?;
        self.control.checkpoint(0)?;
        custody.retain(array, storage, work)
    }

    fn observe_input_work(&self, units: usize) -> Result<(), ConstructionFailure> {
        self.control.checkpoint(units)
    }
    fn begin_typed_construction(
        &self,
        schema: &ChunkSchema,
    ) -> Result<Box<dyn InputConstructionOwner>, ConstructionFailure> {
        if !schema.is_typed() {
            return Err(ConstructionFailure::InvalidSource(
                "input creator requires explicit value bindings".into(),
            ));
        }
        self.begin_scope()
    }
    fn begin_compiled_layout_construction(
        &self,
        layout: &novarocks_local_program::StaticLayout,
    ) -> Result<Box<dyn InputConstructionOwner>, ConstructionFailure> {
        if !layout.is_typed() {
            return Err(ConstructionFailure::InvalidSource(
                "schema creator requires a complete compiled layout".into(),
            ));
        }
        self.begin_scope()
    }
}
struct Scope {
    guard: Arc<CreatorGuard>,
    control: Arc<dyn InputConstructionWorkControl>,
    model: Arc<dyn InputConstructionOperationModel>,
}
impl InputConstructionOwner for Scope {
    fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
        ValueTypeStorageLifetime::held(self.guard.clone())
    }
    fn request(&self, layout: Layout) -> Result<(), ConstructionFailure> {
        self.control.checkpoint(0)?;
        self.guard.request(layout)
    }
    fn observe_opaque_upper_bound(&self, bytes_upper: usize) -> Result<(), ConstructionFailure> {
        self.control.checkpoint(0)?;
        self.guard.opaque(OpaqueConstructionBound {
            requests_upper: 1,
            bytes_upper,
            work_upper: 0,
        })
    }
    fn observe(
        &self,
        _: usize,
        path: &[ArrayPathStep],
        step: ArrayNormalizationStep<'_>,
    ) -> Result<(), ConstructionFailure> {
        if let ArrayNormalizationStep::InspectDictionaryRows { row_count, .. } = step {
            return self.control.checkpoint(row_count);
        }
        if let ArrayNormalizationStep::ValueWork { units } = step {
            return self.control.checkpoint(units);
        }
        if path.len() > self.guard.profile.max_depth {
            return Err(exhausted());
        }
        self.control.checkpoint(0)?;
        self.model.observe(
            step,
            &mut |layout| self.request(layout),
            &mut |bound| self.guard.opaque(bound),
            &mut |units| {
                self.control.checkpoint(units)?;
                self.guard.work(units)
            },
        )?;
        self.control.checkpoint(0)
    }
}

#[cfg(test)]
mod tests {
    use super::super::{
        LockedArrowInputConstructionModel,
        runtime::{RuntimeInputConstructionControl, TrackedInputConstructionHost},
    };
    use super::*;
    use crate::exec::chunk::{Chunk, ChunkSchema};
    use crate::runtime::{mem_tracker::MemTracker, runtime_state::RuntimeErrorState};

    fn profile() -> InputConstructionProfile {
        InputConstructionProfile {
            max_requests: 256,
            max_request_bytes: 1 << 20,
            max_opaque_requests: 256,
            max_opaque_bytes: 1 << 20,
            max_work: 1 << 20,
            max_depth: 64,
        }
    }
    fn source() -> Chunk {
        let pure = ValueTypeStorageLifetime::pure();
        Chunk::try_new_typed_with_columns_owned_observed::<
            crate::runtime::task::ExecutionFailure,
            _,
        >(
            Arc::new(ChunkSchema::try_new_typed(vec![], pure.clone()).unwrap()),
            vec![],
            1,
            pure,
            |_| Ok(()),
        )
        .unwrap()
    }
    #[test]
    fn value_work_uses_original_control_without_spending_structural_work_quota() {
        use novarocks_failure_contract::FailureProjection;
        let tracker = MemTracker::new_root("separate value work");
        let error = Arc::new(RuntimeErrorState::default());
        let mut limits = profile();
        limits.max_work = 1;
        let factory = ObservedInputConstructionFactory::try_new(
            limits,
            Arc::new(TrackedInputConstructionHost::new(tracker)),
            Arc::new(RuntimeInputConstructionControl::new(error.clone())),
            Arc::new(LockedArrowInputConstructionModel),
        )
        .unwrap();
        let owner = factory.begin_input_construction(&source()).unwrap();
        owner
            .observe(0, &[], ArrayNormalizationStep::ValueWork { units: 65_536 })
            .unwrap();
        owner
            .observe(0, &[], ArrayNormalizationStep::ValueWork { units: 65_536 })
            .unwrap();
        assert!(
            owner
                .observe(
                    0,
                    &[],
                    ArrayNormalizationStep::InspectConstructionWork { units: 2 }
                )
                .is_err()
        );
        let original = crate::runtime::task::ExecutionFailure::from(
            novarocks_functions::KernelFailure::Cancelled,
        );
        error.set_error(original.clone());
        let refusal = owner
            .observe(0, &[], ArrayNormalizationStep::ValueWork { units: 0 })
            .unwrap_err();
        assert!(
            matches!(refusal, ConstructionFailure::Caller(source) if source.failure_fact() == original.failure_fact())
        );
    }
    #[test]
    fn actual_tracker_requests_survive_scope_and_control_until_final_creation_hold_drop() {
        let tracker = MemTracker::new_root("input construction");
        let error = Arc::new(RuntimeErrorState::default());
        let weak_error = Arc::downgrade(&error);
        let factory = ObservedInputConstructionFactory::try_new(
            profile(),
            Arc::new(TrackedInputConstructionHost::new(tracker.clone())),
            Arc::new(RuntimeInputConstructionControl::new(error)),
            Arc::new(LockedArrowInputConstructionModel),
        )
        .unwrap();
        let first = factory.begin_input_construction(&source()).unwrap();
        first.request(Layout::array::<u8>(32).unwrap()).unwrap();
        let first_hold = first.storage_lifetime();
        let second = factory.begin_input_construction(&source()).unwrap();
        second.request(Layout::array::<u8>(64).unwrap()).unwrap();
        let second_hold = second.storage_lifetime();
        drop(first);
        drop(second);
        drop(factory);
        assert!(weak_error.upgrade().is_none());
        let before = tracker.current();
        assert!(before >= 96);
        drop(first_hold);
        assert!(tracker.current() < before);
        assert!(tracker.current() >= 64);
        drop(second_hold);
        assert_eq!(tracker.current(), 0);
    }
    #[test]
    fn explicit_schema_starts_an_independent_scope_without_retaining_the_schema() {
        let tracker = MemTracker::new_root("schema-origin construction");
        let factory = ObservedInputConstructionFactory::try_new(
            profile(),
            Arc::new(TrackedInputConstructionHost::new(tracker.clone())),
            Arc::new(RuntimeInputConstructionControl::new(Arc::new(
                RuntimeErrorState::default(),
            ))),
            Arc::new(LockedArrowInputConstructionModel),
        )
        .unwrap();
        let schema =
            Arc::new(ChunkSchema::try_new_typed(vec![], ValueTypeStorageLifetime::pure()).unwrap());
        let weak_schema = Arc::downgrade(&schema);
        let owner = factory.begin_typed_construction(&schema).unwrap();
        owner.request(Layout::array::<u8>(64).unwrap()).unwrap();
        let hold = owner.storage_lifetime();
        drop(schema);
        assert!(weak_schema.upgrade().is_none());
        drop(owner);
        drop(factory);
        assert!(tracker.current() >= 64);
        drop(hold);
        assert_eq!(tracker.current(), 0);
    }
    #[test]
    fn input_origin_delegates_the_same_binding_check_before_any_scope_request() {
        let tracker = MemTracker::new_root("construction binding check");
        let factory = ObservedInputConstructionFactory::try_new(
            profile(),
            Arc::new(TrackedInputConstructionHost::new(tracker.clone())),
            Arc::new(RuntimeInputConstructionControl::new(Arc::new(
                RuntimeErrorState::default(),
            ))),
            Arc::new(LockedArrowInputConstructionModel),
        )
        .unwrap();
        let legacy = Chunk::default();
        let before = tracker.current();
        let direct = match factory.begin_typed_construction(legacy.chunk_schema()) {
            Err(error) => error,
            Ok(_) => panic!("legacy schema must not start a typed construction"),
        };
        let input = match factory.begin_input_construction(&legacy) {
            Err(error) => error,
            Ok(_) => panic!("legacy input must not start a typed construction"),
        };
        assert_eq!(direct, input);
        assert_eq!(tracker.current(), before);
        drop(factory);
        assert_eq!(tracker.current(), 0);
    }
    #[test]
    fn rejected_actual_host_request_rolls_back_only_that_unconstructed_request() {
        let tracker = MemTracker::new_root("input construction");
        let factory = ObservedInputConstructionFactory::try_new(
            profile(),
            Arc::new(TrackedInputConstructionHost::new(tracker.clone())),
            Arc::new(RuntimeInputConstructionControl::new(Arc::new(
                RuntimeErrorState::default(),
            ))),
            Arc::new(LockedArrowInputConstructionModel),
        )
        .unwrap();
        let scope = factory.begin_input_construction(&source()).unwrap();
        let before = tracker.current();
        tracker.install_limit_once(before + 8).unwrap();
        assert!(matches!(
            scope.request(Layout::array::<u8>(16).unwrap()),
            Err(ConstructionFailure::Memory(_))
        ));
        assert_eq!(tracker.current(), before);
        drop(scope);
        drop(factory);
        assert_eq!(tracker.current(), 0);
    }
    #[test]
    fn opaque_bounds_are_classified_separately_and_held_by_the_same_finite_creator() {
        let tracker = MemTracker::new_root("input construction");
        let host: Arc<dyn InputConstructionRequestHost> =
            Arc::new(TrackedInputConstructionHost::new(tracker.clone()));
        let creator = ObservedInputConstructionFactory::creator(profile(), host).unwrap();
        creator.request(Layout::array::<u8>(12).unwrap()).unwrap();
        creator
            .opaque(OpaqueConstructionBound {
                requests_upper: 4,
                bytes_upper: 128,
                work_upper: 7,
            })
            .unwrap();
        let usage = creator.usage.lock().unwrap();
        assert_eq!(usage.requests, 2);
        assert_eq!(usage.opaque_requests, 4);
        assert_eq!(usage.opaque_bytes, 128);
        drop(usage);
        drop(creator);
        assert_eq!(tracker.current(), 0);
    }
}
