// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Fixed poll backing within the original control inventory.
//!
//! This substrate quotes its frame/container storage only. It does not
//! provide a complete role profile, reserve input pointees, or activate Server.

pub(crate) mod ingress;
mod ready_queue;
use ingress::{MountedInput, SharedIngress};
use ready_queue::ReadyQueue;

use crate::{
    WorkError, WorkId, WorkOwner, WorkloadControl,
    control_storage::{SlotId, SlotPhase, arc_bytes},
    scope::Inner,
};
use std::{
    alloc::Layout,
    any::Any,
    future::Future,
    mem::size_of,
    num::NonZeroUsize,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
};
use tokio::sync::{Notify, futures::OwnedNotified};

/// Requested backing owned by the application's SAME role source. This
/// storage-only fact grants no Work, dispatch, payload or Native qualification.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PollRoleSourceBacking {
    per_slot: usize,
    shared: usize,
}
impl PollRoleSourceBacking {
    /// Original layout authors provide independent allocations only. A handle
    /// already inside an inline source is not charged a second time here.
    pub fn from_layouts(per_slot: &[Layout], shared: &[Layout]) -> Result<Self, WorkError> {
        let sum = |layouts: &[Layout]| {
            layouts.iter().try_fold(0usize, |n, l| {
                n.checked_add(l.size()).ok_or(WorkError::ArithmeticOverflow)
            })
        };
        Ok(Self {
            per_slot: sum(per_slot)?,
            shared: sum(shared)?,
        })
    }
    pub(crate) fn bytes(self, slots: usize) -> Result<usize, WorkError> {
        self.per_slot
            .checked_mul(slots)
            .and_then(|n| n.checked_add(self.shared))
            .ok_or(WorkError::ArithmeticOverflow)
    }
}

/// Pure layout facts for the actual factory's fixed frames and containers.
///
/// This is neither a capacity reservation nor installation authority. Independent
/// input/output/context/factory pointees, service tasks and complete role custody remain
/// the composing owner's responsibility.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PollRoleGeometry {
    per_slot: usize,
    shared: usize,
    failure_custody: Layout,
    completion_custody: Layout,
}
impl PollRoleGeometry {
    /// Derive facts from the same concrete factory used by the real bank.
    /// This does not invoke the factory or construct its input or owner.
    pub fn for_factory<I, F: Future, M: Fn(WorkOwner, I) -> F>(
        make: &M,
    ) -> Result<Self, WorkError> {
        Self::for_factory_with_context::<I, F, M, ()>(make)
    }
    /// Derive the same facts with context retained outside the actual future.
    /// This does not construct, clone or invoke the context or the factory.
    pub fn for_factory_with_context<I, F: Future, M: Fn(WorkOwner, I) -> F, C>(
        _: &M,
    ) -> Result<Self, WorkError> {
        let per_slot = size_of::<Position<F, C>>()
            .checked_add(size_of::<Option<MountedInput<I, C>>>())
            .ok_or(WorkError::ArithmeticOverflow)?
            .checked_add(size_of::<Option<F>>())
            .and_then(|n| n.checked_add(size_of::<PollBacking>()))
            .and_then(|n| n.checked_add(size_of::<Option<Arc<PollWake>>>()))
            .and_then(|n| n.checked_add(arc_bytes::<PollWake>().ok()?))
            .ok_or(WorkError::ArithmeticOverflow)?;
        let shared = size_of::<RolePollBank<I, F, M, C>>()
            .checked_add(arc_bytes::<SharedIngress<I, C>>()?)
            .and_then(|n| n.checked_add(arc_bytes::<Notify>().ok()?))
            .and_then(|n| n.checked_add(arc_bytes::<ReadyQueue>().ok()?))
            .and_then(|n| n.checked_add(size_of::<OwnedNotified>()))
            // Pinned macOS mutex backing: input cells, ready ring and Notify wait list.
            .and_then(|n| n.checked_add(if cfg!(target_os = "macos") { 192 } else { 0 }))
            .ok_or(WorkError::ArithmeticOverflow)?;
        Ok(Self {
            per_slot,
            shared,
            failure_custody: Layout::new::<PollOutput<PollFailure<F::Output>>>(),
            completion_custody: Layout::new::<PollTurnCompletion<C, F::Output>>(),
        })
    }
    /// Include original application source allocations in the SAME fixed
    /// role base. The bank installer must consume this identical fact.
    pub fn with_source_backing(mut self, source: PollRoleSourceBacking) -> Result<Self, WorkError> {
        self.per_slot = self
            .per_slot
            .checked_add(source.per_slot)
            .ok_or(WorkError::ArithmeticOverflow)?;
        self.shared = self
            .shared
            .checked_add(source.shared)
            .ok_or(WorkError::ArithmeticOverflow)?;
        Ok(self)
    }

    /// Fixed bank backing for the actual number of original inventory slots,
    /// including only the independent sources explicitly added by their author.
    /// Unquoted pointees and variable payload still require original funding.
    pub fn bytes(self, slots: usize) -> Result<usize, WorkError> {
        self.per_slot
            .checked_mul(slots)
            .and_then(|n| n.checked_add(self.shared))
            .ok_or(WorkError::ArithmeticOverflow)
    }

    /// One Box request for the whole original failure packet, including footer.
    /// This is a separate layout fact, not an amount already included in bytes().
    /// Payload pointees and retained-first/incoming overlap must be funded by
    /// the actual receiving owner before that owner can install a complete role.
    pub fn failure_custody_layout(&self) -> &Layout {
        &self.failure_custody
    }

    /// The actual outer completion, including context and its original Wake
    /// alias. This independent receiving layout is not included in bytes() or
    /// the stripped failure packet layout; the caller funds its real overlap.
    pub fn completion_custody_layout(&self) -> &Layout {
        &self.completion_custody
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PollPhase {
    Free,
    Mounted,
    Retired,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RegistrationPhase {
    Absent,
    Installed,
    Stopping,
}

pub(crate) struct PollBacking {
    geometry: PollRoleGeometry,
    wake: Arc<PollWake>,
    pub(crate) phase: PollPhase,
    registration: RegistrationPhase,
    registration_generation: u64,
    ingress_exported: bool,
}
impl PollBacking {
    pub(crate) fn is_installed(&self) -> bool {
        self.registration == RegistrationPhase::Installed
    }
    pub(crate) fn try_reap(&mut self) -> bool {
        if self.phase != PollPhase::Retired {
            return false;
        }
        let Some(wake) = Arc::get_mut(&mut self.wake) else {
            return false;
        };
        wake.queued.store(false, Ordering::Relaxed);
        if self.registration != RegistrationPhase::Installed {
            wake.changed = Weak::new();
        }
        self.phase = PollPhase::Free;
        true
    }
    pub(crate) fn new(geometry: PollRoleGeometry, slot: SlotId) -> Self {
        Self {
            geometry,
            wake: Arc::new(PollWake {
                queued: AtomicBool::new(false),
                slot,
                changed: Weak::new(),
            }),
            phase: PollPhase::Free,
            registration: RegistrationPhase::Absent,
            registration_generation: 0,
            ingress_exported: false,
        }
    }
}
struct PollWake {
    queued: AtomicBool,
    slot: SlotId,
    changed: Weak<ReadyQueue>,
}
impl Wake for PollWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        if let Some(queue) = self.changed.upgrade()
            && queue.publish(self)
        {
            queue.notify();
        }
    }
}

// Root-owned frame and result are destroyed before the actual mount footer.
struct Position<F: Future, C = ()> {
    frame: Option<Pin<Box<Option<F>>>>,
    output: Option<F::Output>,
    // Context remains strong through factory/poll/frame cleanup, including
    // unwind. It is never passed to the business factory or future.
    context: Option<C>,
    footer: Option<Footer>,
    // A receiving owner can decline a completed packet. Keep that SAME outer
    // completion in this original position instead of discarding its source.
    retained: Option<PollTurnCompletion<C, F::Output>>,
}
struct Footer {
    wake: Arc<PollWake>,
    _pin: MountPin,
}
struct MountPin {
    inner: Arc<Inner>,
    slot: SlotId,
    generation: u64,
    work: WorkId,
    role: usize,
}
impl Drop for MountPin {
    fn drop(&mut self) {
        {
            let mut state = self.inner.state.lock().unwrap();
            let slot = &mut state.cancellations.slots[self.slot.0];
            assert_eq!(slot.generation, self.generation);
            assert_eq!(slot.phase, SlotPhase::Node(self.work));
            let role = &mut slot.polls[self.role];
            assert_eq!(role.phase, PollPhase::Mounted);
            role.phase = PollPhase::Retired;
            state.cancellations.retired_polls += 1;
            state.advance_progress_revision();
            state.collect(self.work);
        }
        self.inner.notify_reclaim_progress();
    }
}

/// A completed value keeps its original footer until the value actually exits.
/// There is no consuming callback or raw take API.
pub struct PollOutput<O> {
    value: O,
    footer: Footer,
}
impl<O> PollOutput<O> {
    pub fn value(&self) -> &O {
        &self.value
    }
}
/// The two real catch sites retain their original payloads. If frame cleanup
/// fails after Ready, the original output remains under this same footer too.
/// Borrowing reveals facts; it cannot detach an owned value from its custody.
pub struct PollFailure<O> {
    primary: Box<dyn Any + Send>,
    cleanup: Option<Box<dyn Any + Send>>,
    output: Option<O>,
}
impl<O> PollFailure<O> {
    pub fn primary(&self) -> &(dyn Any + Send) {
        self.primary.as_ref()
    }
    pub fn cleanup(&self) -> Option<&(dyn Any + Send)> {
        self.cleanup.as_deref()
    }
    pub fn output(&self) -> Option<&O> {
        self.output.as_ref()
    }
}
pub enum PollCompletion<O> {
    Ready(PollOutput<O>),
    Panicked(PollOutput<PollFailure<O>>),
}
/// A completion's association is outside the unchanged original value/failure
/// packet. Receiving owners must remove this outer context before storing that
/// packet in the context's own source cell, otherwise they could create a cycle.
/// No source-exit fact follows from dropping the association or its absence.
pub struct PollTurnCompletion<C, O> {
    completion: PollCompletion<O>,
    context: C,
    // The packet leaves first, then C. This same original Wake keeps the slot
    // unreapable through C's actual destructor, without a new allocation.
    _context_backing: Arc<PollWake>,
}
impl<C, O> PollTurnCompletion<C, O> {
    /// Borrows the same association without cloning or granting source rights.
    pub fn context(&self) -> &C {
        &self.context
    }

    /// Borrows the unchanged packet; no value, cause or footer can be detached.
    pub fn completion(&self) -> &PollCompletion<O> {
        &self.completion
    }

    /// Separates the association from the original packet without changing
    /// that packet's value, failure payloads or footer. The receiving owner
    /// must keep their custody related after the move. The packet still owns
    /// its original footer when this outer temporary Wake alias leaves.
    pub fn into_parts(self) -> (C, PollCompletion<O>) {
        let Self {
            completion,
            context,
            _context_backing,
        } = self;
        drop(_context_backing);
        (context, completion)
    }
}
#[cfg(test)]
pub(crate) struct MountRejected<I, C = ()> {
    pub(crate) owner: WorkOwner,
    pub(crate) input: I,
    // Retain the original association on refusal until the receiving caller drops it.
    pub(crate) _context: C,
    pub(crate) error: WorkError,
}

/// Only the unique workload owner can install this bank in fixed storage. It has no
/// independent admission queue, free list, public mount guard, or capacity decision.
/// The composing application separately owns complete-profile validation and
/// the root task's actual exit; this generic bank certifies neither.
pub struct RolePollBank<I, F: Future, M, C = ()> {
    inner: Arc<Inner>,
    role: usize,
    make: M,
    changed: Arc<Notify>,
    ready: Arc<ReadyQueue>,
    waiter: Pin<Box<OwnedNotified>>,
    positions: Vec<Position<F, C>>,
    // The original receiving cells/registration leave after every root field.
    shared: Arc<SharedIngress<I, C>>,
}
impl<I, F: Future, M: Fn(WorkOwner, I) -> F> RolePollBank<I, F, M> {
    /// Unit-context convenience for existing factories; this is the same bank.
    pub fn install(control: &mut WorkloadControl, role: usize, make: M) -> Result<Self, WorkError> {
        Self::install_with_context(control, role, make)
    }

    #[cfg(test)]
    pub(crate) fn mount_owned(
        &mut self,
        owner: WorkOwner,
        input: I,
    ) -> Result<(), MountRejected<I>> {
        self.mount_owned_with_context(owner, input, ())
    }
}
impl<I, F: Future, M: Fn(WorkOwner, I) -> F, C> RolePollBank<I, F, M, C> {
    pub fn install_with_context(
        control: &mut WorkloadControl,
        role: usize,
        make: M,
    ) -> Result<Self, WorkError> {
        Self::install_with_context_and_source_backing(
            control,
            role,
            make,
            PollRoleSourceBacking::default(),
        )
    }

    /// Install the actual factory and the same original source backing fact
    /// used by fixed storage. Any mismatch refuses before allocation/mutation.
    pub fn install_with_context_and_source_backing(
        control: &mut WorkloadControl,
        role: usize,
        make: M,
        source: PollRoleSourceBacking,
    ) -> Result<Self, WorkError> {
        // Only the original unique startup owner can install. No shared Inner
        // entry point can allocate competing banks against the same paid base.
        let inner = &control.inner;
        let geometry = PollRoleGeometry::for_factory_with_context::<I, F, M, C>(&make)?
            .with_source_backing(source)?;
        let validate = |state: &mut crate::scope::State| -> Result<(usize, u64), WorkError> {
            if state.ready
                || state.root_admission_is_closed()
                || !state.cancellations.fixed
                || !state.nodes.is_empty()
            {
                return Err(WorkError::Conflict);
            }
            let generation = state
                .cancellations
                .slots
                .first()
                .and_then(|slot| slot.polls.get(role))
                .ok_or(WorkError::NotReady)?
                .registration_generation;
            let next_generation = generation
                .checked_add(1)
                .ok_or(WorkError::ArithmeticOverflow)?;
            for slot in &mut state.cancellations.slots {
                let backing = slot.polls.get_mut(role).ok_or(WorkError::NotReady)?;
                if slot.phase != SlotPhase::Free
                    || backing.registration != RegistrationPhase::Absent
                    || backing.registration_generation != generation
                    || backing.ingress_exported
                    || backing.phase != PollPhase::Free
                    || backing.geometry != geometry
                    || Arc::get_mut(&mut backing.wake).is_none()
                {
                    return Err(WorkError::Conflict);
                }
            }
            Ok((state.cancellations.slots.len(), next_generation))
        };
        let (count, generation) = validate(&mut inner.state.lock().unwrap())?;
        let changed = Arc::new(Notify::new());
        {
            let mut notified = std::pin::pin!(changed.notified());
            notified.as_mut().enable();
        }
        let ready = ReadyQueue::new(count, Arc::clone(&changed))?;
        let mut waiter = Box::pin(Arc::clone(&changed).notified_owned());
        waiter.as_mut().enable();
        let mut positions = Vec::new();
        positions
            .try_reserve_exact(count)
            .map_err(|_| WorkError::Capacity("fixed role positions"))?;
        for _ in 0..count {
            positions.push(Position {
                frame: None,
                output: None,
                context: None,
                footer: None,
                retained: None,
            });
        }
        let mut inputs = Vec::new();
        inputs
            .try_reserve_exact(count)
            .map_err(|_| WorkError::Capacity("fixed role input cells"))?;
        inputs.resize_with(count, || None);
        let inputs = std::sync::Mutex::new(inputs);
        // Materialize the original platform mutex before publishing registration.
        drop(inputs.lock().unwrap());
        // This is startup registration, with all backing already funded by
        // the original fixed base. It executes no runtime notification callback.
        inner.update_facts_silent(|state| {
            let (_, current_generation) = validate(state)?;
            assert_eq!(current_generation, generation);
            for slot in &mut state.cancellations.slots {
                let backing = &mut slot.polls[role];
                Arc::get_mut(&mut backing.wake).unwrap().changed = Arc::downgrade(&ready);
                backing.registration = RegistrationPhase::Installed;
                backing.registration_generation = generation;
            }
            Ok::<(), WorkError>(())
        })?;
        let shared = Arc::new(SharedIngress {
            inner: Arc::clone(inner),
            role,
            generation,
            ready: Arc::clone(&ready),
            inputs,
            _registration: BankRegistration {
                inner: Arc::clone(inner),
                role,
                generation,
            },
        });
        Ok(Self {
            inner: Arc::clone(inner),
            role,
            make,
            changed,
            ready,
            waiter,
            positions,
            shared,
        })
    }

    pub fn retirement_observer(&self) -> RoleBackingRetirementObserver {
        RoleBackingRetirementObserver {
            inner: Arc::downgrade(&self.inner),
            role: self.role,
            generation: self.shared.generation,
        }
    }

    /// Exporting an ingress permanently forbids pre-ready reinstallation of
    /// this role on the same authority. The original process/client owner keeps
    /// its weak header until clients actually exit; this is not hot restart.
    pub fn ingress(&self) -> Result<ingress::RoleIngress<I, C>, WorkError> {
        self.shared.export()
    }

    #[cfg(test)]
    pub(crate) fn mount_owned_with_context(
        &mut self,
        owner: WorkOwner,
        input: I,
        context: C,
    ) -> Result<(), MountRejected<I, C>> {
        let mut owner = Some(owner);
        let mut input = Some(input);
        let mut context = Some(context);
        // This exclusive bank borrow keeps the original shared value alive.
        // A refused sender cannot be its last Arc and run registration cleanup.
        let result = self
            .shared
            .attempt(&mut owner, &mut input, &mut context)
            .finish();
        match result {
            Ok(()) => Ok(()),
            Err(error) => Err(MountRejected {
                owner: owner.take().unwrap(),
                input: input.take().unwrap(),
                _context: context.take().unwrap(),
                error,
            }),
        }
    }

    /// Retain a declined, still-whole completion in its SAME original position.
    /// Refusal leaves the source unchanged. This does not run foreign Drop,
    /// poll a future, notify or create an exit/settlement fact. The caller must
    /// not split the association from its packet before this transaction.
    /// Retained failure custody prevents an ordinary successful root shutdown;
    /// loss of the bank itself is not a substitute for external root custody.
    pub fn retain_completion_in(
        &mut self,
        source: &mut Option<PollTurnCompletion<C, F::Output>>,
    ) -> Result<(), WorkError> {
        let original = source.as_ref().ok_or(WorkError::Released)?;
        let footer = match &original.completion {
            PollCompletion::Ready(output) => &output.footer,
            PollCompletion::Panicked(output) => &output.footer,
        };
        if !Arc::ptr_eq(&footer._pin.inner, &self.inner) {
            return Err(WorkError::ForeignAuthority);
        }
        if footer._pin.role != self.role {
            return Err(WorkError::Conflict);
        }
        // Preserve the sole original State -> input lock order. Failed/cancelled
        // work, stopped admission and a closed driver still need a receiver.
        let state = self.inner.state.lock().unwrap();
        let slot_id = footer._pin.slot;
        let slot = state
            .cancellations
            .slots
            .get(slot_id.0)
            .ok_or(WorkError::Conflict)?;
        let backing = slot.polls.get(self.role).ok_or(WorkError::Conflict)?;
        let node = state
            .nodes
            .get(&footer._pin.work)
            .ok_or(WorkError::Released)?;
        if slot.generation != footer._pin.generation
            || slot.phase != SlotPhase::Node(footer._pin.work)
            || node.cancellation != slot_id
            || backing.phase != PollPhase::Mounted
            || backing.registration_generation != self.shared.generation
            || !matches!(
                backing.registration,
                RegistrationPhase::Installed | RegistrationPhase::Stopping
            )
            || !Arc::ptr_eq(&backing.wake, &footer.wake)
        {
            return Err(WorkError::Conflict);
        }
        let inputs = self.shared.inputs.lock().unwrap();
        if inputs.get(slot_id.0).is_none_or(Option::is_some) {
            return Err(WorkError::Conflict);
        }
        let position = self
            .positions
            .get_mut(slot_id.0)
            .ok_or(WorkError::Conflict)?;
        if position.context.is_some()
            || position.footer.is_some()
            || position.output.is_some()
            || position.retained.is_some()
            || position
                .frame
                .as_ref()
                .is_some_and(|frame| frame.as_ref().get_ref().is_some())
        {
            return Err(WorkError::Conflict);
        }
        // Every destination and original fence is checked before this silent
        // move. There is no old value to destroy, including on either refusal.
        position.retained = source.take();
        Ok(())
    }

    pub fn stop_ingress(&self) {
        self.shared.stop();
    }

    /// Observes only sources still owned by this bank after its ingress cut.
    /// This excludes PollOutput packets already handed to a receiving owner,
    /// stale Wakers, sender tails and the root task itself. It is not a reclaim
    /// proof; join the root and observe retirement separately. A frame's empty
    /// prepaid Box is retained storage, not a live source.
    pub fn has_owned_sources_after_stop(&self) -> Result<bool, WorkError> {
        // Keep the original publication/take lock order. No user destructor,
        // future poll or notification executes under either metadata lock.
        let state = self.inner.state.lock().unwrap();
        if state.cancellations.slots.iter().any(|slot| {
            let role = &slot.polls[self.role];
            role.registration_generation != self.shared.generation
                || role.registration != RegistrationPhase::Stopping
        }) {
            return Err(WorkError::NotReady);
        }
        let inputs = self.shared.inputs.lock().unwrap();
        Ok(inputs.iter().any(Option::is_some)
            || self.positions.iter().any(|position| {
                position.footer.is_some()
                    || position.retained.is_some()
                    || position.context.is_some()
                    || position.output.is_some()
                    || position
                        .frame
                        .as_ref()
                        .is_some_and(|frame| frame.as_ref().get_ref().is_some())
            }))
    }

    /// Level-triggered readiness. Register before reading the real queue;
    /// a consumed Notify permit never stands in for the queue's contents.
    pub fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        self.waiter.as_mut().enable();
        if !self.ready.is_empty() {
            return Poll::Ready(());
        }
        match self.waiter.as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(()) => {
                self.waiter
                    .as_mut()
                    .set(Arc::clone(&self.changed).notified_owned());
                // A stale signal can cause one finite empty service turn.
                Poll::Ready(())
            }
        }
    }

    /// Poll at most the given number of actual ready hints. None means no
    /// completion in this turn; poll_ready still checks any remaining items.
    pub fn progress_turn(
        &mut self,
        budget: NonZeroUsize,
    ) -> Option<PollTurnCompletion<C, F::Output>> {
        for _ in 0..budget.get() {
            let Some(wake) = self.ready.pop() else {
                break;
            };
            let mounted = self.shared.take_input(wake.slot);
            let position = &mut self.positions[wake.slot.0];
            let input = mounted.map(
                |MountedInput {
                     owner,
                     input,
                     context,
                     footer,
                 }| {
                    assert!(position.footer.is_none());
                    assert!(position.output.is_none());
                    assert!(position.context.is_none());
                    assert!(position.retained.is_none());
                    position.context = Some(context);
                    position.footer = Some(footer);
                    (owner, input)
                },
            );
            let Some(footer) = position.footer.as_ref() else {
                // A completed role can still have a queued stale wake. Its
                // actual Arc exits here, outside the queue and State locks.
                continue;
            };
            assert!(
                Arc::ptr_eq(&footer.wake, &wake),
                "ready hint retains its original anchor"
            );
            let construction = if let Some((owner, input)) = input {
                catch_unwind(AssertUnwindSafe(|| {
                    // Materialize this original prepaid slot only on first use.
                    // Keep the same Box through every later reset and reuse.
                    let frame = position.frame.get_or_insert_with(|| Box::pin(None));
                    frame.as_mut().set(Some((self.make)(owner, input)))
                }))
            } else {
                Ok(())
            };
            let mut primary = construction.err();
            if primary.is_none() {
                let waker = Waker::from(Arc::clone(&position.footer.as_ref().unwrap().wake));
                let mut context = Context::from_waker(&waker);
                let polled = catch_unwind(AssertUnwindSafe(|| {
                    position
                        .frame
                        .as_mut()
                        .unwrap()
                        .as_mut()
                        .as_pin_mut()
                        .unwrap()
                        .poll(&mut context)
                }));
                match polled {
                    Ok(Poll::Pending) => continue,
                    Ok(Poll::Ready(value)) => position.output = Some(value),
                    Err(payload) => primary = Some(payload),
                }
            }
            // All frame/input temporaries and callbacks leave before the footer.
            let cleared = catch_unwind(AssertUnwindSafe(|| {
                if let Some(frame) = &mut position.frame {
                    frame.as_mut().set(None);
                }
            }));
            let mut cleanup = cleared.err();
            if primary.is_none() {
                primary = cleanup.take();
            }
            let context = position.context.take().unwrap();
            let footer = position.footer.take().unwrap();
            let context_backing = Arc::clone(&footer.wake);
            let completion = if let Some(primary) = primary {
                PollCompletion::Panicked(PollOutput {
                    value: PollFailure {
                        primary,
                        cleanup,
                        output: position.output.take(),
                    },
                    footer,
                })
            } else {
                PollCompletion::Ready(PollOutput {
                    value: position.output.take().unwrap(),
                    footer,
                })
            };
            return Some(PollTurnCompletion {
                completion,
                context,
                _context_backing: context_backing,
            });
        }
        None
    }
}

impl<I, F: Future, M, C> Drop for RolePollBank<I, F, M, C> {
    fn drop(&mut self) {
        // Stop before any make/frame/input destruction. Shared registration
        // remains until root fields and every original sender tail truly exit.
        self.shared.stop();
    }
}

struct BankRegistration {
    inner: Arc<Inner>,
    role: usize,
    generation: u64,
}
impl Drop for BankRegistration {
    fn drop(&mut self) {
        {
            let mut state = self.inner.state.lock().unwrap();
            for slot in &mut state.cancellations.slots {
                let role = &mut slot.polls[self.role];
                assert_eq!(role.registration, RegistrationPhase::Stopping);
                assert_eq!(role.registration_generation, self.generation);
                if let Some(wake) = Arc::get_mut(&mut role.wake) {
                    // A Weak retains the old Notify allocation after its value
                    // exits. Destroy it before a replacement bank may allocate.
                    wake.changed = Weak::new();
                }
                role.registration = RegistrationPhase::Absent;
            }
            state.advance_progress_revision();
        }
        // Original inventory/driver survives this bank, including external
        // PollOutput and silent stale Wakers that still retain its old slot.
        self.inner.notify_reclaim_progress();
    }
}

/// Non-owning proof for one original role bank's backing only. The caller must
/// separately retain and join its actual root task: this proof does not cover
/// the registration notification tail or the root future that contained it.
/// Exported ingress Weak headers remain with their process/client owner. They
/// forbid reinstallation, and this observer does not prove those headers free.
/// This finite, allocation-free scan is intended for shutdown observation.
pub struct RoleBackingRetirementObserver {
    inner: Weak<Inner>,
    role: usize,
    generation: u64,
}
impl RoleBackingRetirementObserver {
    pub fn is_reclaimed(&self) -> Result<bool, WorkError> {
        let inner = self.inner.upgrade().ok_or(WorkError::Released)?;
        let state = inner.state.lock().unwrap();
        if !state.cancellations.fixed {
            return Err(WorkError::NotReady);
        }
        let generation = state
            .cancellations
            .slots
            .first()
            .and_then(|slot| slot.polls.get(self.role))
            .ok_or(WorkError::Conflict)?
            .registration_generation;
        if generation < self.generation {
            return Err(WorkError::Conflict);
        }
        let mut reclaimed = true;
        for slot in &state.cancellations.slots {
            let backing = slot.polls.get(self.role).ok_or(WorkError::Conflict)?;
            if backing.registration_generation != generation {
                return Err(WorkError::Conflict);
            }
            reclaimed &= backing.registration == RegistrationPhase::Absent
                && backing.phase == PollPhase::Free;
        }
        // A later registration can be minted only after every old Wake has
        // really reset and the previous bank's fields have left. MAX refuses
        // installation instead of wrapping this original registration epoch.
        Ok(generation > self.generation || reclaimed)
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod source_backing_tests {
    use super::*;
    #[test]
    fn same_source_fact_is_required_at_original_bank_install() {
        let make = |_: WorkOwner, (): ()| async {};
        let raw = PollRoleGeometry::for_factory(&make).unwrap();
        let backing =
            PollRoleSourceBacking::from_layouts(&[Layout::new::<[u64; 4]>()], &[]).unwrap();
        let actual = raw.with_source_backing(backing).unwrap();
        assert_eq!(
            actual.bytes(4).unwrap() - raw.bytes(4).unwrap(),
            4 * Layout::new::<[u64; 4]>().size()
        );
        let (mut control, _handle, mut driver) =
            crate::scope::fixed_control_storage_tests::prototype_profile_with_roles(
                4,
                1,
                1,
                1,
                &[actual],
            );
        assert!(matches!(
            RolePollBank::install(&mut control, 0, make),
            Err(WorkError::Conflict)
        ));
        let bank = RolePollBank::<_, _, _, ()>::install_with_context_and_source_backing(
            &mut control,
            0,
            make,
            backing,
        )
        .unwrap();
        drop(bank);
        control.close_admission();
        for _ in 0..1000 {
            if matches!(
                driver.step(),
                crate::joint_admission::JointProgress::Idle(_)
            ) {
                break;
            }
        }
        assert!(control.shutdown().is_ok());
    }
}
