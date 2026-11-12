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

//! Original Shared Apply row custody. Source owns the one transport Join and
//! every credentialed disposal Join. Neither a body return nor dropping a row
//! loan can advance physical retirement. All locks are the existing Apply and
//! backing locks; no callback or opaque destructor runs under either lock.

use super::*;
use crate::task_execution::host::{
    custody::{SharedApplyJoinCompletion, TransportPhysicalRetirement},
    transport::ReturnedQuota,
};
use novarocks_control_runtime::obligation::{ReservationObligation, StepRegistration};
use novarocks_workload_control::joint_admission::NativeQueuedInputGrant;
use std::mem::ManuallyDrop;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DisposalPhase {
    Native,
    Physical,
    Action,
    Reply,
}

/// The original rich value remains here if a callback never takes it. After a
/// destructor panic, the same Source retains its raw successor and Return;
/// taking the value is not an exit proof and cannot make this row retry it.
struct NativeDisposalInput {
    value: Mutex<Option<RawValue>>,
    backing: Mutex<Option<DisposalBacking>>,
    origin: Arc<SharedOrigin>,
    phase: DisposalPhase,
}
impl Drop for NativeDisposalInput {
    fn drop(&mut self) {
        // Loss of an owner/return channel cannot run rich Drop or refund an
        // unproven phase. The actual row normally retains this cell; if the
        // entire owner disappears, retain its SAME unresolved objects.
        if let Some(value) = self
            .value
            .get_mut()
            .unwrap_or_else(|error| error.into_inner())
            .take()
        {
            std::mem::forget(value);
        }
        if let Some(backing) = self
            .backing
            .get_mut()
            .unwrap_or_else(|error| error.into_inner())
            .take()
        {
            std::mem::forget(backing);
        }
    }
}
enum DisposalBacking {
    Retained { _original: Arc<ReservationBacking> },
    // At Reply the actual final backing is inside value, not independently
    // cloned here: cloning would prevent its real destructor from running.
    InOriginalValue,
}

#[must_use = "submit this same request to its original paid disposal leg"]
pub(crate) struct NativeDisposalRequest {
    original: Arc<NativeDisposalInput>,
}
#[must_use = "dispose only inside the original credentialed blocking work"]
pub(crate) struct NativeDisposalBody {
    original: Arc<NativeDisposalInput>,
}
#[must_use = "retain until the same disposal Join, report and captures exit"]
pub(crate) struct NativeDisposalReturn {
    original: Arc<NativeDisposalInput>,
}
impl NativeDisposalRequest {
    pub(crate) fn into_parts(self) -> (NativeDisposalBody, NativeDisposalReturn) {
        (
            NativeDisposalBody {
                original: Arc::clone(&self.original),
            },
            NativeDisposalReturn {
                original: self.original,
            },
        )
    }
}
impl NativeDisposalBody {
    /// No lock spans rich Drop. If this body is cancelled without invocation,
    /// its ordinary Drop releases only an Arc; the row retains the same value.
    pub(crate) fn dispose(self) {
        let value = self
            .original
            .value
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
            .expect("one original Native disposal entry");
        drop(value);
    }
}

struct SharedOrigin;
enum EncodingCustody {
    Guard(ManuallyDrop<StepRegistration>),
    Taken,
}
/// If original Native destruction fails, keep the unspent SAME guard inside
/// the raw successor. It must not auto-drop while Native exit is unproven.
struct SharedNativeUnproven {
    _cause: RawValue,
    _encoding: ManuallyDrop<Option<StepRegistration>>,
}
struct SharedNativeDisposal {
    native: Option<OriginalNativeAcceptance>,
    encoding: Option<StepRegistration>,
}
impl Drop for SharedNativeDisposal {
    fn drop(&mut self) {
        let native = self
            .native
            .take()
            .expect("one original Shared Native disposal");
        if let Err(cause) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(native)))
        {
            std::panic::resume_unwind(Box::new(SharedNativeUnproven {
                _cause: cause,
                _encoding: ManuallyDrop::new(self.encoding.take()),
            }));
        }
        drop(self.encoding.take());
    }
}

/// These actual grants are returned to Source after protected physical
/// retirement. They must not drop before Source has released the original
/// queue and encoding quotas and called queue_exited_in for this send.
pub(crate) struct SharedApplyReservations {
    inputs: ManuallyDrop<Vec<NativeQueuedInputGrant>>,
    financial: Option<SharedApplyFunding>,
}
impl SharedApplyReservations {
    fn new(inputs: Vec<NativeQueuedInputGrant>) -> Self {
        Self {
            inputs: ManuallyDrop::new(inputs),
            financial: None,
        }
    }
    pub(super) fn inputs(&self) -> &[NativeQueuedInputGrant] {
        &self.inputs
    }
}

#[must_use = "return every original quota before exiting these send queue grants"]
pub(crate) struct SharedPhysicalReturn {
    original: ManuallyDrop<SharedApplyReservations>,
    retirement: TransportPhysicalRetirement,
}
/// Funding is a real object transfer, not an exit proof. Only the private
/// SharedPhysicalReturn constructor below certifies the Native return boundary.
pub(crate) enum SharedApplyFunding {
    Encoded(ReturnedQuota),
    UnusedQueue(ReservationObligation),
}
impl SharedPhysicalReturn {
    pub(crate) fn into_original(
        self,
    ) -> (
        SharedApplyFunding,
        Vec<NativeQueuedInputGrant>,
        TransportPhysicalRetirement,
    ) {
        let mut original = ManuallyDrop::into_inner(self.original);
        let financial = original
            .financial
            .take()
            .expect("same actual transport return installed");
        let inputs = ManuallyDrop::into_inner(original.inputs);
        (financial, inputs, self.retirement)
    }
}

/// Protected physical disposal transfers these SAME objects into the original
/// row's return cell. It does not release quota or finish WC queue custody.
type SharedReturnCell = Mutex<Option<ManuallyDrop<SharedApplyReservations>>>;
struct PhysicalReturnCarrier {
    original: Option<SharedApplyReservations>,
    returned: Arc<SharedReturnCell>,
}
impl Drop for PhysicalReturnCarrier {
    fn drop(&mut self) {
        let original = self.original.take().expect("one original physical return");
        let mut returned = self
            .returned
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        assert!(returned.is_none(), "one original physical return cell");
        *returned = Some(ManuallyDrop::new(original));
    }
}

enum SharedStage {
    AwaitTransport,
    Disposing {
        input: Arc<NativeDisposalInput>,
        request: Option<NativeDisposalRequest>,
    },
    ReplyAliases,
    Retired,
}
pub(super) struct SharedRecord {
    origin: Arc<SharedOrigin>,
    encoding: EncodingCustody,
    stage: SharedStage,
    returned: Arc<SharedReturnCell>,
}
impl SharedRecord {
    pub(super) fn permits_preparation(&self) -> bool {
        matches!(self.stage, SharedStage::AwaitTransport)
    }
}

/// A preparation loan contains no Join or exit authority. NativeRowLoan
/// retains the same row even if an original WC callback panics.
pub(crate) struct SharedApplyPreparation {
    core: Arc<Core>,
    index: usize,
    origin: Arc<SharedOrigin>,
}
#[must_use = "the original Source must observe its unique transport Join"]
pub(crate) struct SharedApplyJoinObligation {
    core: Arc<Core>,
    index: usize,
    origin: Arc<SharedOrigin>,
}
#[must_use = "drive original disposal and retain every unreturned object"]
pub(crate) struct SharedApplyRetirement {
    core: Arc<Core>,
    index: usize,
    origin: Arc<SharedOrigin>,
    physical_retirement: Option<TransportPhysicalRetirement>,
}

impl ApplySlot {
    pub(crate) fn prepare_shared(
        mut self,
        inputs: Vec<NativeQueuedInputGrant>,
        encoding: StepRegistration,
    ) -> Result<
        (SharedApplyPreparation, SharedApplyJoinObligation),
        (
            RegistrationFailure,
            Self,
            Vec<NativeQueuedInputGrant>,
            StepRegistration,
        ),
    > {
        let mut state = self
            .core
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if state.closed || inputs.is_empty() {
            let failure = if state.closed {
                RegistrationFailure::Closed
            } else {
                RegistrationFailure::MissingOriginalCharge
            };
            drop(state);
            return Err((failure, self, inputs, encoding));
        }
        let record = state.records[self.index]
            .as_mut()
            .expect("same reserved Apply row");
        assert!(
            matches!(record.state, OriginalState::Reserved),
            "one Shared row preparation"
        );
        let backing = record
            .reservations
            .get_or_insert_with(|| Arc::new(ReservationBacking::new(&self.core, 0, None, None)));
        *backing
            .physical
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(ApplyReservations::Shared(
            SharedApplyReservations::new(inputs),
        ));
        let origin = Arc::new(SharedOrigin);
        record.state = OriginalState::Shared(SharedRecord {
            origin: Arc::clone(&origin),
            encoding: EncodingCustody::Guard(ManuallyDrop::new(encoding)),
            stage: SharedStage::AwaitTransport,
            returned: Arc::new(Mutex::new(None)),
        });
        self.active = false;
        drop(state);
        let preparation = SharedApplyPreparation {
            core: Arc::clone(&self.core),
            index: self.index,
            origin: Arc::clone(&origin),
        };
        let join = SharedApplyJoinObligation {
            core: Arc::clone(&self.core),
            index: self.index,
            origin,
        };
        self.core.notify_change();
        Ok((preparation, join))
    }
}
impl SharedApplyPreparation {
    fn original_slot(&self) -> Result<ApplySlot, novarocks_workload_control::WorkError> {
        let state = self
            .core
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let Some(record) = state.records.get(self.index).and_then(Option::as_ref) else {
            return Err(novarocks_workload_control::WorkError::Released);
        };
        if !matches!(&record.state, OriginalState::Shared(shared) if Arc::ptr_eq(&self.origin,&shared.origin) && shared.permits_preparation())
        {
            return Err(novarocks_workload_control::WorkError::Conflict);
        }
        Ok(ApplySlot {
            core: Arc::clone(&self.core),
            index: self.index,
            active: false,
        })
    }
    pub(crate) fn take_encoding(
        &self,
    ) -> Result<StepRegistration, novarocks_workload_control::WorkError> {
        let mut state = self
            .core
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let record = state
            .records
            .get_mut(self.index)
            .and_then(Option::as_mut)
            .ok_or(novarocks_workload_control::WorkError::Released)?;
        let OriginalState::Shared(shared) = &mut record.state else {
            return Err(novarocks_workload_control::WorkError::Conflict);
        };
        if !Arc::ptr_eq(&self.origin, &shared.origin) || !shared.permits_preparation() {
            return Err(novarocks_workload_control::WorkError::Conflict);
        }
        match std::mem::replace(&mut shared.encoding, EncodingCustody::Taken) {
            EncodingCustody::Guard(original) => Ok(ManuallyDrop::into_inner(original)),
            EncodingCustody::Taken => Err(novarocks_workload_control::WorkError::Conflict),
        }
    }
    pub(crate) fn accept_native_in(
        &self,
        offer: &mut novarocks_workload_control::joint_admission::JointLocalOffer<'_>,
        physical: &mut Option<super::super::task_transport::NativePhysicalOffer>,
    ) -> Result<
        novarocks_workload_control::WorkloadProgressNotification,
        novarocks_workload_control::WorkError,
    > {
        self.original_slot()?.accept_native_in(offer, physical)
    }
    pub(crate) fn prepare_native_in(
        &self,
        admission: &novarocks_workload_control::joint_admission::NativeAsyncActionAdmission,
        items: usize,
    ) -> Result<
        (
            OriginalReplyPublisher,
            novarocks_workload_control::WorkloadProgressNotification,
            novarocks_workload_control::WorkloadProgressNotification,
        ),
        novarocks_workload_control::WorkError,
    > {
        self.original_slot()?.prepare_native_in(admission, items)
    }
    pub(crate) fn take_native_physical(
        &self,
    ) -> Result<
        super::super::task_transport::NativePhysicalOffer,
        novarocks_workload_control::WorkError,
    > {
        self.original_slot()?.take_native_physical()
    }
}

impl SharedRecord {
    fn disposal(&mut self, value: RawValue, backing: DisposalBacking, phase: DisposalPhase) {
        let input = Arc::new(NativeDisposalInput {
            value: Mutex::new(Some(value)),
            backing: Mutex::new(Some(backing)),
            origin: Arc::clone(&self.origin),
            phase,
        });
        self.stage = SharedStage::Disposing {
            request: Some(NativeDisposalRequest {
                original: Arc::clone(&input),
            }),
            input,
        };
    }
}

/// The Source completion proves this unique transport Join, captures and
/// producer report, without waiting for physical children to retire. Source
/// retains parent custody until the original returned obligations also close.
/// Refusal returns the whole original completion and its unchanged funding;
/// validation never strips the Source-only Join facts or move-only proof.
pub(crate) fn complete_shared_join(
    completion: SharedApplyJoinCompletion,
    physical: SharedApplyFunding,
) -> Result<SharedApplyRetirement, (SharedApplyJoinCompletion, SharedApplyFunding)> {
    if !completion.matches_funding(&physical) {
        return Err((completion, physical));
    }
    complete_shared_join_with_funding(completion, physical)
}

fn complete_shared_join_with_funding(
    completion: SharedApplyJoinCompletion,
    physical: SharedApplyFunding,
) -> Result<SharedApplyRetirement, (SharedApplyJoinCompletion, SharedApplyFunding)> {
    let join_facts = completion.join_facts();
    let original = completion.original();
    let mut state = original
        .core
        .state
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let Some(record) = state
        .records
        .get_mut(original.index)
        .and_then(Option::as_mut)
    else {
        drop(state);
        return Err((completion, physical));
    };
    let OriginalState::Shared(shared) = &mut record.state else {
        drop(state);
        return Err((completion, physical));
    };
    if !Arc::ptr_eq(&original.origin, &shared.origin)
        || !matches!(shared.stage, SharedStage::AwaitTransport)
    {
        drop(state);
        return Err((completion, physical));
    }
    if !matches!(
        (&shared.encoding, &physical),
        (EncodingCustody::Taken, SharedApplyFunding::Encoded(_))
            | (
                EncodingCustody::Guard(_),
                SharedApplyFunding::UnusedQueue(_)
            )
    ) {
        drop(state);
        return Err((completion, physical));
    }
    let unused_encoding = match std::mem::replace(&mut shared.encoding, EncodingCustody::Taken) {
        EncodingCustody::Guard(original) => Some(ManuallyDrop::into_inner(original)),
        EncodingCustody::Taken => None,
    };
    let backing = record
        .reservations
        .as_ref()
        .expect("same Shared original backing");
    {
        let mut current = backing
            .physical
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let Some(ApplyReservations::Shared(current)) = current.as_mut() else {
            panic!("Shared row retains its same grants");
        };
        assert!(
            current.financial.is_none(),
            "one original transport financial return"
        );
        current.financial = Some(physical);
    }
    let native = std::mem::replace(&mut record.native, OriginalNativeAcceptance::new());
    shared.disposal(
        Box::new(SharedNativeDisposal {
            native: Some(native),
            encoding: unused_encoding,
        }),
        DisposalBacking::Retained {
            _original: Arc::clone(backing),
        },
        DisposalPhase::Native,
    );
    if join_facts.panic || join_facts.cancelled {
        state.first_failure.get_or_insert(join_facts);
    }
    drop(state);
    original.core.notify_change();
    let (original, physical_retirement) = completion.into_original();
    Ok(SharedApplyRetirement {
        core: original.core,
        index: original.index,
        origin: original.origin,
        physical_retirement: Some(physical_retirement),
    })
}

impl SharedApplyRetirement {
    /// Register before checking this same row's request/return readiness.
    pub(crate) fn notified(&self) -> tokio::sync::futures::OwnedNotified {
        Arc::clone(&self.core.changed).notified_owned()
    }
    pub(crate) fn take_disposal_request(&mut self) -> Option<NativeDisposalRequest> {
        let mut state = self
            .core
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let record = state.records.get_mut(self.index)?.as_mut()?;
        let OriginalState::Shared(shared) = &mut record.state else {
            return None;
        };
        if !Arc::ptr_eq(&self.origin, &shared.origin) {
            return None;
        }
        if matches!(shared.stage, SharedStage::ReplyAliases) {
            let backing = record
                .reservations
                .as_ref()
                .expect("same Shared reply backing");
            if Arc::strong_count(backing) != 1 {
                return None;
            }
            let backing = record
                .reservations
                .take()
                .expect("unique original reply backing");
            // This last actual backing lives inside value. An extra Arc next
            // to value would postpone its real rich destructor past cleanup.
            shared.disposal(
                Box::new(backing),
                DisposalBacking::InOriginalValue,
                DisposalPhase::Reply,
            );
        }
        match &mut shared.stage {
            SharedStage::Disposing { request, .. } => request.take(),
            _ => None,
        }
    }
    /// A foreign/stale row refusal returns the SAME complete Source proof,
    /// allowing its owner to submit it to the matching row without reissuance.
    pub(crate) fn complete_disposal(
        &mut self,
        completion: crate::task_execution::host::custody::NativeDisposalCompletion,
    ) -> Result<(), crate::task_execution::host::custody::NativeDisposalCompletion> {
        let original = completion.original();
        let mut state = self
            .core
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let Some(record) = state.records.get_mut(self.index).and_then(Option::as_mut) else {
            drop(state);
            return Err(completion);
        };
        let OriginalState::Shared(shared) = &mut record.state else {
            drop(state);
            return Err(completion);
        };
        let matches = Arc::ptr_eq(&self.origin, &shared.origin)
            && Arc::ptr_eq(&original.original.origin, &shared.origin)
            && matches!(&shared.stage,SharedStage::Disposing {input,request:None} if Arc::ptr_eq(input,&original.original));
        if !matches {
            drop(state);
            return Err(completion);
        }
        // Same-object check only: the private Source completion, never this
        // empty-cell observation, proves actual Join/report/capture retirement.
        assert!(
            original
                .original
                .value
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .is_none(),
            "completed original disposal took its actual value"
        );
        let previous = std::mem::replace(&mut shared.stage, SharedStage::Retired);
        match original.original.phase {
            DisposalPhase::Native => {
                let backing = record
                    .reservations
                    .as_ref()
                    .expect("same original physical backing");
                let physical = backing
                    .physical
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .take()
                    .expect("same original physical packet");
                let physical = match physical {
                    ApplyReservations::Shared(original) => original,
                    #[cfg(test)]
                    ApplyReservations::Legacy { .. } => {
                        panic!("Shared cleanup cannot consume Legacy physical custody");
                    }
                };
                assert!(
                    physical.financial.is_some(),
                    "actual transport return precedes physical disposal"
                );
                shared.disposal(
                    Box::new(PhysicalReturnCarrier {
                        original: Some(physical),
                        returned: Arc::clone(&shared.returned),
                    }),
                    DisposalBacking::Retained {
                        _original: Arc::clone(backing),
                    },
                    DisposalPhase::Physical,
                );
            }
            DisposalPhase::Physical => {
                let backing = record
                    .reservations
                    .as_ref()
                    .expect("same original action backing");
                let action = backing
                    .action
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .take();
                if let Some(action) = action {
                    shared.disposal(
                        Box::new(action),
                        DisposalBacking::Retained {
                            _original: Arc::clone(backing),
                        },
                        DisposalPhase::Action,
                    );
                } else {
                    shared.stage = SharedStage::ReplyAliases;
                }
            }
            DisposalPhase::Action => shared.stage = SharedStage::ReplyAliases,
            DisposalPhase::Reply => {}
        }
        drop(state);
        // Only the same real Source completion author permits releasing this
        // phase's original backing. No owner lock spans its actual Arc exit.
        let backing = original
            .original
            .backing
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        drop(previous);
        drop(backing);
        drop(completion.into_original());
        self.core.notify_change();
        Ok(())
    }
    /// Callback publication alone is insufficient: this exposes the same
    /// packet only after its physical completion and any original action
    /// grant disposal. Reply aliases have their own later lifetime.
    pub(crate) fn take_physical_return(&mut self) -> Option<SharedPhysicalReturn> {
        let state = self
            .core
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let record = state.records.get(self.index)?.as_ref()?;
        let OriginalState::Shared(shared) = &record.state else {
            return None;
        };
        if !Arc::ptr_eq(&self.origin, &shared.origin) {
            return None;
        }
        if !matches!(
            shared.stage,
            SharedStage::ReplyAliases | SharedStage::Retired
        ) && !matches!(&shared.stage, SharedStage::Disposing { input, .. } if input.phase == DisposalPhase::Reply)
        {
            return None;
        }
        let original = shared
            .returned
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()?;
        Some(SharedPhysicalReturn {
            original,
            retirement: self
                .physical_retirement
                .take()
                .expect("one original physical retirement"),
        })
    }
    pub(crate) fn try_retire(self) -> Result<(), Self> {
        let mut state = self
            .core
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let Some(record) = state.records.get(self.index).and_then(Option::as_ref) else {
            drop(state);
            return Err(self);
        };
        let OriginalState::Shared(shared) = &record.state else {
            drop(state);
            return Err(self);
        };
        if !Arc::ptr_eq(&self.origin, &shared.origin)
            || !matches!(shared.stage, SharedStage::Retired)
            || self.physical_retirement.is_some()
            || shared
                .returned
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .is_some()
        {
            drop(state);
            return Err(self);
        }
        let record = state.records[self.index].take();
        drop(state);
        drop(record);
        self.core.notify_change();
        Ok(())
    }
}

/// Exact layouts for the original Source's registration quote. These facts
/// allocate nothing and grant no row, cleanup role, Work or capacity. The
/// Source adds its actual carrier/ExitCustody/report layouts separately.
pub(crate) struct SharedApplyLayouts {
    pub(crate) origin: std::alloc::Layout,
    pub(crate) physical_return_cell: std::alloc::Layout,
    pub(crate) reply_backing: std::alloc::Layout,
    pub(crate) disposal_input: std::alloc::Layout,
    pub(crate) native_value: std::alloc::Layout,
    pub(crate) physical_value: std::alloc::Layout,
    pub(crate) action_value: std::alloc::Layout,
    pub(crate) reply_value: std::alloc::Layout,
}
pub(crate) fn shared_apply_layouts()
-> Result<SharedApplyLayouts, novarocks_workload_control::WorkError> {
    use novarocks_type_contract::owned_resources::layout::arc_layout;
    use novarocks_workload_control::WorkError;
    use std::alloc::Layout;
    Ok(SharedApplyLayouts {
        origin: arc_layout(Layout::new::<SharedOrigin>())
            .map_err(|_| WorkError::ArithmeticOverflow)?,
        physical_return_cell: arc_layout(Layout::new::<SharedReturnCell>())
            .map_err(|_| WorkError::ArithmeticOverflow)?,
        reply_backing: arc_layout(Layout::new::<ReservationBacking>())
            .map_err(|_| WorkError::ArithmeticOverflow)?,
        disposal_input: arc_layout(Layout::new::<NativeDisposalInput>())
            .map_err(|_| WorkError::ArithmeticOverflow)?,
        native_value: Layout::new::<SharedNativeDisposal>(),
        physical_value: Layout::new::<PhysicalReturnCarrier>(),
        action_value: Layout::new::<
            novarocks_workload_control::joint_admission::NativeAsyncActionGrant,
        >(),
        reply_value: Layout::new::<Arc<ReservationBacking>>(),
    })
}

#[cfg(test)]
mod tests;
