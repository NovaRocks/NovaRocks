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

//! Synchronous, same-thread attribution steps with structural restoration.
use super::binding;
use crate::lane::{Lane, RecordRef};
use std::{marker::PhantomData, rc::Rc};

/// Funding-lane entry facts retain their original fallback contract.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum AmbientEntryObservation {
    #[default]
    NotAttempted,
    Bound,
    LaneEntryRefused,
    TlsUnavailable,
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum AmbientExitObservation {
    #[default]
    NotExited,
    EntryWasUnbound,
    Restored,
    RestoreTlsUnavailable,
}
/// Caller-owned funding-step facts; no callback or process-counter difference.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AmbientStepObservation {
    entry: AmbientEntryObservation,
    exit: AmbientExitObservation,
}
impl AmbientStepObservation {
    pub const fn entry(&self) -> AmbientEntryObservation {
        self.entry
    }
    pub const fn exit(&self) -> AmbientExitObservation {
        self.exit
    }
}
/// Observation owners shadow on unavailable coverage. They never change the
/// funding-lane facts or its sealed-entry fallback policy.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum OwnerEntryObservation {
    #[default]
    NotAttempted,
    Bound,
    UncoveredShadow,
    SealedShadow,
    TlsUnavailable,
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct OwnerStepObservation {
    entry: OwnerEntryObservation,
    exit: AmbientExitObservation,
}
impl OwnerStepObservation {
    pub const fn entry(&self) -> OwnerEntryObservation {
        self.entry
    }
    pub const fn exit(&self) -> AmbientExitObservation {
        self.exit
    }
}

// Both public policies use this fixed, allocation-free capture and the same
// private guard. Funding callers cannot request or receive a shadow entry.
enum Observation<'a> {
    Funding(&'a mut AmbientStepObservation),
    Owner(&'a mut OwnerStepObservation),
}
impl Observation<'_> {
    fn reset(&mut self) {
        match self {
            Self::Funding(facts) => **facts = AmbientStepObservation::default(),
            Self::Owner(facts) => **facts = OwnerStepObservation::default(),
        }
    }
    fn entered(&mut self, funding: AmbientEntryObservation, owner: OwnerEntryObservation) {
        match self {
            Self::Funding(facts) => facts.entry = funding,
            Self::Owner(facts) => facts.entry = owner,
        }
    }
    fn exited(&mut self, exit: AmbientExitObservation) {
        match self {
            Self::Funding(facts) => facts.exit = exit,
            Self::Owner(facts) => facts.exit = exit,
        }
    }
}

// No public guard: restoration always happens on this synchronous stack,
// before returning to an executor or releasing the installed lane owner.
pub(crate) struct AmbientStep<'lane, 'observation> {
    lane: Option<&'lane Lane>,
    previous: RecordRef,
    observation: Option<Observation<'observation>>,
    _thread: PhantomData<Rc<()>>,
}
impl<'lane, 'observation> AmbientStep<'lane, 'observation> {
    fn enter(
        lane: &'lane Lane,
        mut observation: Option<Observation<'observation>>,
        shadow_on_refusal: bool,
    ) -> Option<Self> {
        if let Some(facts) = observation.as_mut() {
            facts.reset();
        }
        if !lane.enter() {
            lane.store().faults.binding_failed();
            if shadow_on_refusal {
                return Self::shadow(observation, OwnerEntryObservation::SealedShadow);
            }
            if let Some(facts) = observation.as_mut() {
                facts.entered(
                    AmbientEntryObservation::LaneEntryRefused,
                    OwnerEntryObservation::SealedShadow,
                );
                facts.exited(AmbientExitObservation::EntryWasUnbound);
            }
            return None;
        }
        // SAFETY: the lane and outer guards retain their global-store owners;
        // this private guard restores on the same synchronous stack.
        let Some(previous) = (unsafe { binding::try_install_ambient(lane.reference()) }) else {
            lane.leave();
            lane.store().faults.binding_failed();
            if let Some(facts) = observation.as_mut() {
                facts.entered(
                    AmbientEntryObservation::TlsUnavailable,
                    OwnerEntryObservation::TlsUnavailable,
                );
                facts.exited(AmbientExitObservation::EntryWasUnbound);
            }
            return None;
        };
        if let Some(facts) = observation.as_mut() {
            facts.entered(AmbientEntryObservation::Bound, OwnerEntryObservation::Bound);
        }
        Some(Self {
            lane: Some(lane),
            previous,
            observation,
            _thread: PhantomData,
        })
    }
    fn shadow(
        mut observation: Option<Observation<'observation>>,
        entry: OwnerEntryObservation,
    ) -> Option<Self> {
        if let Some(facts) = observation.as_mut() {
            facts.reset();
        }
        // SAFETY: SHADOW denotes no record. The guard restores the exact outer
        // binding on the same stack while every outer owner is still held.
        let Some(previous) = (unsafe { binding::try_install_ambient(RecordRef::SHADOW) }) else {
            crate::lane::global_store().faults.binding_failed();
            if let Some(facts) = observation.as_mut() {
                facts.entered(
                    AmbientEntryObservation::TlsUnavailable,
                    OwnerEntryObservation::TlsUnavailable,
                );
                facts.exited(AmbientExitObservation::EntryWasUnbound);
            }
            return None;
        };
        if let Some(facts) = observation.as_mut() {
            facts.entered(AmbientEntryObservation::LaneEntryRefused, entry);
        }
        Some(Self {
            lane: None,
            previous,
            observation,
            _thread: PhantomData,
        })
    }
}
impl Drop for AmbientStep<'_, '_> {
    fn drop(&mut self) {
        // SAFETY: same thread and exact saved binding; all owners are still held.
        let restored = unsafe { binding::try_restore_ambient(self.previous) };
        if let Some(lane) = self.lane {
            lane.leave();
        }
        if let Some(facts) = self.observation.as_mut() {
            facts.exited(if restored {
                AmbientExitObservation::Restored
            } else {
                AmbientExitObservation::RestoreTlsUnavailable
            });
        }
    }
}
impl Lane {
    /// Attributes one synchronous funding step. An await must not occur inside.
    /// Entry refusal records coverage failure and keeps the existing outer
    /// binding; this policy neither admits nor refuses the submitted work.
    pub fn run<T>(&self, step: impl FnOnce() -> T) -> T {
        let _guard = AmbientStep::enter(self, None, false);
        step()
    }
    /// Retains funding entry/restoration facts, including after unwind.
    pub fn run_observed<T>(
        &self,
        observation: &mut AmbientStepObservation,
        step: impl FnOnce() -> T,
    ) -> T {
        let _guard = AmbientStep::enter(self, Some(Observation::Funding(observation)), false);
        step()
    }
    pub(crate) fn run_or_shadow<T>(&self, step: impl FnOnce() -> T) -> T {
        let _guard = AmbientStep::enter(self, None, true);
        step()
    }
    pub(crate) fn run_or_shadow_observed<T>(
        &self,
        observation: &mut OwnerStepObservation,
        step: impl FnOnce() -> T,
    ) -> T {
        let _guard = AmbientStep::enter(self, Some(Observation::Owner(observation)), true);
        step()
    }
}
pub(crate) fn run_shadowed<T>(step: impl FnOnce() -> T) -> T {
    let _guard = AmbientStep::shadow(None, OwnerEntryObservation::UncoveredShadow);
    step()
}
pub(crate) fn run_shadowed_observed<T>(
    observation: &mut OwnerStepObservation,
    step: impl FnOnce() -> T,
) -> T {
    let _guard = AmbientStep::shadow(
        Some(Observation::Owner(observation)),
        OwnerEntryObservation::UncoveredShadow,
    );
    step()
}

#[cfg(test)]
mod shadow_tests {
    use super::*;
    use crate::attribution::AttributingAllocator;
    use crate::lane::{ATTRIBUTION_THRESHOLD_BYTES, UNATTRIBUTED_SHARDS};
    use crate::{AccountKind, AuthorityConfig, ExternalRef, MemoryAuthority};
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::panic::{AssertUnwindSafe, catch_unwind};

    fn allocation_owner(allocator: &AttributingAllocator<System>) -> RecordRef {
        let layout = Layout::from_size_align(ATTRIBUTION_THRESHOLD_BYTES, 8).unwrap();
        // SAFETY: one genuine allocation, its reserved token, then its exact free.
        unsafe {
            let pointer = allocator.alloc(layout);
            assert!(!pointer.is_null());
            let reference = RecordRef::read(pointer.add(layout.size()));
            allocator.dealloc(pointer, layout);
            reference
        }
    }

    #[test]
    fn shadow_masks_ambient_and_restores_it_after_return_and_unwind() {
        let mut config = AuthorityConfig::new(1 << 20, 1 << 20, 0);
        config.max_accounts = 4;
        config.max_active_owners = 4;
        config.metadata_budget_bytes = 16 * 1024;
        let authority = MemoryAuthority::new(config).unwrap();
        let query = authority
            .create_account(AccountKind::Work, ExternalRef::NONE)
            .unwrap();
        let domain = query.create_domain(0).unwrap();
        let allocator = AttributingAllocator::new(System);
        domain.lane().run(|| {
            assert_eq!(allocation_owner(&allocator), domain.lane().reference());
            let mut observation = OwnerStepObservation::default();
            run_shadowed_observed(&mut observation, || {
                let reference = allocation_owner(&allocator);
                assert!(reference.index < UNATTRIBUTED_SHARDS);
                assert!(!reference.is_shadow());
                assert_eq!(
                    crate::lane::global_store()
                        .snapshot_ref(reference)
                        .unwrap()
                        .slot_pins,
                    0
                );
            });
            assert_eq!(observation.entry(), OwnerEntryObservation::UncoveredShadow);
            assert_eq!(observation.exit(), AmbientExitObservation::Restored);
            assert_eq!(allocation_owner(&allocator), domain.lane().reference());
            assert!(
                catch_unwind(AssertUnwindSafe(|| run_shadowed(|| panic!(
                    "shadow unwind"
                ))))
                .is_err()
            );
            assert_eq!(allocation_owner(&allocator), domain.lane().reference());
        });
    }
}
