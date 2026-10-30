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

//! Typed pins in the original prepaid ClaimRecord; no new holder allocation.
use super::*;
use crate::WorkloadProgressNotification;
use novarocks_resource_contract::{ExistingOwnerRetentionLease, RetentionOwner, RetentionPinError};

#[derive(Clone, Copy)]
pub(super) enum ClaimCustody {
    Raw,
    Production {
        epoch: u64,
    },
    Payload {
        epoch: u64,
        pins: usize,
    },
    Computation {
        epoch: u64,
        pins: usize,
    },
    NativeReply {
        epoch: u64,
        pins: usize,
    },
    NativeAction {
        epoch: u64,
        pins: usize,
    },
    NativeInput {
        epoch: u64,
        pins: usize,
        queued: bool,
    },
}
impl ClaimCustody {
    pub(super) fn is_raw(self) -> bool {
        matches!(self, Self::Raw)
    }
    pub(super) fn can_release_current(self) -> bool {
        matches!(
            self,
            Self::Raw
                | Self::Payload { pins: 0, .. }
                | Self::Computation { pins: 0, .. }
                | Self::NativeReply { pins: 0, .. }
                | Self::NativeAction { pins: 0, .. }
                | Self::NativeInput {
                    pins: 0,
                    queued: false,
                    ..
                }
        )
    }
}
#[derive(Clone, Copy)]
enum Kind {
    Payload,
    Computation,
    NativeReply,
    NativeAction,
    NativeInput,
}
impl Kind {
    fn resource(self) -> JointResource {
        match self {
            Self::Payload | Self::NativeReply | Self::NativeInput | Self::NativeAction => {
                JointResource::PayloadBytes
            }
            Self::Computation => JointResource::Computations,
        }
    }
    fn new_custody(self, epoch: u64) -> ClaimCustody {
        match self {
            Self::Payload => ClaimCustody::Payload { epoch, pins: 1 },
            Self::Computation => ClaimCustody::Computation { epoch, pins: 1 },
            Self::NativeReply => ClaimCustody::NativeReply { epoch, pins: 1 },
            Self::NativeAction => ClaimCustody::NativeAction { epoch, pins: 1 },
            Self::NativeInput => ClaimCustody::NativeInput {
                epoch,
                pins: 1,
                queued: true,
            },
        }
    }
    fn pins(self, custody: &mut ClaimCustody, epoch: u64) -> Result<&mut usize, RetentionPinError> {
        match custody {
            ClaimCustody::Payload {
                epoch: original,
                pins,
            } => {
                if matches!(self, Self::Payload) && *original == epoch {
                    Ok(pins)
                } else {
                    Err(RetentionPinError::Conflict)
                }
            }
            ClaimCustody::Computation {
                epoch: original,
                pins,
            } => {
                if matches!(self, Self::Computation) && *original == epoch {
                    Ok(pins)
                } else {
                    Err(RetentionPinError::Conflict)
                }
            }
            ClaimCustody::NativeReply {
                epoch: original,
                pins,
            } => {
                if matches!(self, Self::NativeReply) && *original == epoch {
                    Ok(pins)
                } else {
                    Err(RetentionPinError::Conflict)
                }
            }
            ClaimCustody::NativeAction {
                epoch: original,
                pins,
            } => {
                if matches!(self, Self::NativeAction) && *original == epoch {
                    Ok(pins)
                } else {
                    Err(RetentionPinError::Conflict)
                }
            }
            ClaimCustody::NativeInput {
                epoch: original,
                pins,
                ..
            } => {
                if matches!(self, Self::NativeInput) && *original == epoch {
                    Ok(pins)
                } else {
                    Err(RetentionPinError::Conflict)
                }
            }
            _ => Err(RetentionPinError::Conflict),
        }
    }
}
fn try_pin(inner: &Inner, key: [u64; 2], kind: Kind) -> Result<(), RetentionPinError> {
    let mut state = inner.state.lock().unwrap();
    let row = state
        .joint
        .as_mut()
        .and_then(|ledger| ledger.claims.get_mut(&key[0]))
        .ok_or(RetentionPinError::Released)?;
    let pins = kind.pins(&mut row.custody, key[1])?;
    if *pins == 0 {
        return Err(RetentionPinError::Released);
    }
    let next = pins.checked_add(1).ok_or(RetentionPinError::Overflow)?;
    *pins = next;
    Ok(())
}
fn drop_pin(inner: &Inner, key: [u64; 2], kind: Kind) {
    let last = {
        let mut state = inner.state.lock().unwrap();
        let Some(row) = state
            .joint
            .as_mut()
            .and_then(|ledger| ledger.claims.get_mut(&key[0]))
        else {
            return;
        };
        let Ok(pins) = kind.pins(&mut row.custody, key[1]) else {
            return;
        };
        if *pins == 0 {
            return;
        }
        *pins -= 1;
        *pins == 0
    };
    if last {
        // No foreign code between zero publication and this original release.
        // A paused Drop conservatively keeps the same record/charge alive.
        let waker = inner.update(|state| release_current(state, key[0]));
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}
fn payload_capacity(inner: &Inner, key: [u64; 2]) -> Result<u64, RetentionPinError> {
    payload_resource(inner, key, JointResource::PayloadBytes)
}
fn payload_resource(
    inner: &Inner,
    key: [u64; 2],
    resource: JointResource,
) -> Result<u64, RetentionPinError> {
    let state = inner.state.lock().unwrap();
    let row = state
        .joint
        .as_ref()
        .and_then(|ledger| ledger.claims.get(&key[0]))
        .ok_or(RetentionPinError::Released)?;
    match row.custody {
        ClaimCustody::Payload { epoch, pins }
        | ClaimCustody::NativeReply { epoch, pins }
        | ClaimCustody::NativeAction { epoch, pins }
        | ClaimCustody::NativeInput { epoch, pins, .. }
            if epoch == key[1] && pins != 0 =>
        {
            Ok(row.active_amounts.get(resource))
        }
        _ => Err(RetentionPinError::Conflict),
    }
}
fn retention_kind(inner: &Inner, key: [u64; 2]) -> Result<Kind, RetentionPinError> {
    let state = inner.state.lock().unwrap();
    let row = state
        .joint
        .as_ref()
        .and_then(|ledger| ledger.claims.get(&key[0]))
        .ok_or(RetentionPinError::Released)?;
    match row.custody {
        ClaimCustody::Payload { epoch, pins } if epoch == key[1] && pins != 0 => Ok(Kind::Payload),
        ClaimCustody::NativeReply { epoch, pins } if epoch == key[1] && pins != 0 => {
            Ok(Kind::NativeReply)
        }
        ClaimCustody::NativeAction { epoch, pins } if epoch == key[1] && pins != 0 => {
            Ok(Kind::NativeAction)
        }
        ClaimCustody::NativeInput { epoch, pins, .. } if epoch == key[1] && pins != 0 => {
            Ok(Kind::NativeInput)
        }
        _ => Err(RetentionPinError::Conflict),
    }
}
impl RetentionOwner for Inner {
    fn try_retain(&self, key: [u64; 2]) -> Result<(), RetentionPinError> {
        try_pin(self, key, retention_kind(self, key)?)
    }
    fn release_pin(&self, key: [u64; 2]) {
        if let Ok(kind) = retention_kind(self, key) {
            drop_pin(self, key, kind);
        }
    }
    fn capacity_bytes(&self, key: [u64; 2]) -> Result<u64, RetentionPinError> {
        payload_capacity(self, key)
    }
}

struct OriginalPin {
    inner: Arc<Inner>,
    key: Option<[u64; 2]>,
    kind: Kind,
}
impl OriginalPin {
    fn try_clone(&self) -> Result<Self, RetentionPinError> {
        let key = self.key.ok_or(RetentionPinError::Released)?;
        let inner = Arc::clone(&self.inner);
        try_pin(&inner, key, self.kind)?;
        Ok(Self {
            inner,
            key: Some(key),
            kind: self.kind,
        })
    }
    fn same(&self, other: &Self) -> bool {
        self.key.is_some() && self.key == other.key && Arc::ptr_eq(&self.inner, &other.inner)
    }
    fn is_for_scope(&self, scope: &WorkScope) -> bool {
        if !Arc::ptr_eq(&self.inner, &scope.inner) {
            return false;
        }
        let Some(key) = self.key else {
            return false;
        };
        let state = self.inner.state.lock().unwrap();
        state
            .joint
            .as_ref()
            .and_then(|ledger| ledger.claims.get(&key[0]))
            .is_some_and(|row| {
                row.owner == ClaimOwner::PhysicalWork(scope.id)
                    && match (self.kind, row.custody) {
                        (Kind::Payload, ClaimCustody::Payload { epoch, pins })
                        | (Kind::Computation, ClaimCustody::Computation { epoch, pins })
                        | (Kind::NativeReply, ClaimCustody::NativeReply { epoch, pins }) => {
                            epoch == key[1] && pins != 0
                        }
                        _ => false,
                    }
            })
    }
}
impl Drop for OriginalPin {
    fn drop(&mut self) {
        if let Some(key) = self.key.take() {
            drop_pin(&self.inner, key, self.kind);
        }
    }
}
enum Target<'a> {
    Payload(&'a mut Option<RetainedPayloadGrant>),
    Computation(&'a mut Option<InternalComputationGrant>),
    NativeReply(&'a mut Option<NativeReplyGrant>, &'a NativeActionFence),
    NativeAction(
        &'a mut Option<NativeAsyncActionGrant>,
        &'a NativeActionFence,
    ),
    NativeInput(
        &'a mut Option<NativeQueuedInputGrant>,
        &'a NativeActionFence,
    ),
}
impl Target<'_> {
    fn kind(&self) -> Kind {
        match self {
            Self::Payload(_) => Kind::Payload,
            Self::Computation(_) => Kind::Computation,
            Self::NativeReply(_, _) => Kind::NativeReply,
            Self::NativeAction(_, _) => Kind::NativeAction,
            Self::NativeInput(_, _) => Kind::NativeInput,
        }
    }
    fn is_empty(&self) -> bool {
        match self {
            Self::Payload(slot) => slot.is_none(),
            Self::Computation(slot) => slot.is_none(),
            Self::NativeReply(slot, _) => slot.is_none(),
            Self::NativeAction(slot, _) => slot.is_none(),
            Self::NativeInput(slot, _) => slot.is_none(),
        }
    }
}
fn validate_adoption(
    state: &State,
    scope: &WorkScope,
    original: &JointClaim,
    kind: Kind,
) -> Result<(u64, WorkClass, JointLane), WorkError> {
    if matches!(
        kind,
        Kind::NativeReply | Kind::NativeInput | Kind::NativeAction
    ) {
        // An already granted reply remains a custody obligation after business
        // cancellation. Its original Root owner and fixed control must survive.
        let node = state.nodes.get(&scope.id).ok_or(WorkError::Released)?;
        if node.owner != crate::OwnerState::Active {
            return Err(WorkError::Released);
        }
    } else {
        validate_active_control_node(state, scope)?;
    }
    let ledger = state.joint.as_ref().ok_or(WorkError::NotReady)?;
    let node = state.nodes.get(&scope.id).ok_or(WorkError::Released)?;
    let cell = &state.cancellations.slots[node.cancellation.0];
    let control = ledger
        .claims
        .get(&cell.claim.ok_or(WorkError::NotReady)?)
        .ok_or(WorkError::Released)?;
    if !control.custody.is_raw()
        || cell.phase != SlotPhase::Node(scope.id)
        || cell.generation == 0
        || control.owner != ClaimOwner::ControlSlot(node.cancellation)
        || control.pending.is_some()
        || control.amounts != cell.base
        || control.active_amounts != cell.base
        || control.facts.key != control.active_facts.key
        || !root_domains::has_effective_domains(state, &control.active_facts, false)
    {
        return Err(WorkError::Conflict);
    }
    let record = ledger
        .claims
        .get(&original.id.ok_or(WorkError::Released)?)
        .ok_or(WorkError::Released)?;
    if !record.custody.is_raw()
        || record.owner != ClaimOwner::PhysicalWork(scope.id)
        || record.pending.is_some()
        || record.amounts != record.active_amounts
        || record.facts.key != record.active_facts.key
        || record.active_facts.class != node.class
        || (!matches!(
            kind,
            Kind::NativeAction | Kind::NativeReply | Kind::NativeInput
        ) && record.active_facts.key.dependencies != control.active_facts.key.dependencies)
    {
        return Err(WorkError::Conflict);
    }
    if !record.active_facts.key.source_kind.is_empty() {
        return Err(WorkError::Conflict);
    }
    let capacity = record.active_amounts.get(kind.resource());
    let valid = match kind {
        Kind::Payload | Kind::NativeReply => {
            capacity != 0 && record.active_facts.lane == JointLane::Custody
        }
        Kind::NativeInput => record.active_facts.lane == JointLane::Custody,
        Kind::NativeAction => {
            capacity != 0
                && matches!(
                    record.active_facts.lane,
                    JointLane::Foreground | JointLane::Settlement
                )
        }
        Kind::Computation => {
            capacity == 1
                && matches!(
                    record.active_facts.lane,
                    JointLane::Foreground | JointLane::Settlement
                )
        }
    };
    let metadata = record.active_amounts.get(JointResource::MetadataBytes);
    // The enlarged ClaimRecord is prepaid by the original fixed map quote.
    // Any existing metadata amount stays on this same original claim; there
    // is no newly allocated self-funded carrier. Other coordinates are closed.
    let mut expected = JointResources::ZERO
        .with(JointResource::ClaimRecords, 1)
        .with(JointResource::MetadataBytes, metadata)
        .with(kind.resource(), capacity);
    if matches!(kind, Kind::NativeAction) {
        let physical_bytes = record.active_amounts.get(JointResource::PhysicalQueueBytes);
        if metadata == 0 || physical_bytes == 0 {
            return Err(WorkError::Conflict);
        }
        expected = expected
            .with(JointResource::AsyncIo, 1)
            .with(JointResource::PhysicalQueueRecords, 1)
            .with(JointResource::PhysicalQueueBytes, physical_bytes);
    }
    if matches!(kind, Kind::NativeReply) {
        let replies = record.active_amounts.get(JointResource::ReplyRecords);
        if replies == 0 {
            return Err(WorkError::Conflict);
        }
        expected = expected.with(JointResource::ReplyRecords, replies);
    }
    if matches!(kind, Kind::NativeInput) {
        if record.active_amounts.get(JointResource::QueueRecords) != 1 {
            return Err(WorkError::Conflict);
        }
        expected = expected.with(JointResource::QueueRecords, 1);
    }
    if !valid || record.active_amounts != expected {
        return Err(WorkError::Conflict);
    }
    record
        .structure_revision
        .checked_add(1)
        .ok_or(WorkError::ArithmeticOverflow)?;
    Ok((
        capacity,
        record.active_facts.class,
        record.active_facts.lane,
    ))
}
fn adopt_in(
    scope: &WorkScope,
    source: &mut Option<JointClaim>,
    mut target: Target<'_>,
) -> Result<WorkloadProgressNotification, WorkError> {
    if !target.is_empty() {
        return Err(WorkError::Conflict);
    }
    let original = source.as_ref().ok_or(WorkError::Released)?;
    if !Arc::ptr_eq(&scope.inner, &original.inner) {
        return Err(WorkError::ForeignAuthority);
    }
    let inner = Arc::clone(&original.inner);
    let kind = target.kind();
    let retired = {
        let mut state = inner.state.lock().unwrap();
        let original = source.as_mut().ok_or(WorkError::Released)?;
        if let Target::NativeInput(_, fence) = &target {
            paid_proposal::native_action::validate_native_fence(&state, scope, fence)?;
        }
        if let Target::NativeAction(_, fence) = &target {
            paid_proposal::native_action::validate_native_action_claim(
                &state, scope, fence, original, false,
            )?;
        }
        if let Target::NativeReply(_, fence) = &target {
            paid_proposal::native_action::validate_native_action_claim(
                &state, scope, fence, original, true,
            )?;
        }
        validate_adoption(&state, scope, original, kind)?;
        let id = original.id.ok_or(WorkError::Released)?;
        let row = state.joint.as_mut().unwrap().claims.get_mut(&id).unwrap();
        let epoch = row
            .structure_revision
            .checked_add(1)
            .ok_or(WorkError::ArithmeticOverflow)?;
        if matches!(kind, Kind::Computation)
            && !state.joint.as_ref().unwrap().claims[&id].internal_position
            && state.joint.as_ref().unwrap().claims[&id]
                .computation_window
                .is_none()
        {
            charge_internal_position(&mut state, scope.id)?;
            state
                .joint
                .as_mut()
                .unwrap()
                .claims
                .get_mut(&id)
                .unwrap()
                .internal_position = true;
        }
        let row = state.joint.as_mut().unwrap().claims.get_mut(&id).unwrap();
        row.structure_revision = epoch;
        row.custody = kind.new_custody(epoch);
        original.id = None;
        let pin = OriginalPin {
            inner: Arc::clone(&inner),
            key: Some([id, epoch]),
            kind,
        };
        match &mut target {
            Target::Payload(slot) => **slot = Some(RetainedPayloadGrant { pin }),
            Target::Computation(slot) => **slot = Some(InternalComputationGrant { pin }),
            Target::NativeReply(slot, _) => **slot = Some(NativeReplyGrant { pin }),
            Target::NativeAction(slot, _) => **slot = Some(NativeAsyncActionGrant { pin }),
            Target::NativeInput(slot, _) => **slot = Some(NativeQueuedInputGrant { pin }),
        }
        state.advance_progress_revision();
        source.take()
    };
    drop(retired);
    Ok(WorkloadProgressNotification::new(inner))
}

/// Original action custody. Its protocol aliases retain the same physical
/// vector through the actual source/body/upload-frame exit; reply aliases do
/// not retain this position. No alias can release individual coordinates.
#[must_use]
pub struct NativeAsyncActionGrant {
    pin: OriginalPin,
}
impl NativeAsyncActionGrant {
    pub fn retain_protocol_tail(&self) -> Result<ExistingOwnerRetentionLease, RetentionPinError> {
        let mut pin = self.pin.try_clone()?;
        let key = pin.key.take().ok_or(RetentionPinError::Released)?;
        Ok(ExistingOwnerRetentionLease::from_existing_pin(
            Arc::clone(&pin.inner) as Arc<dyn RetentionOwner>,
            key,
        ))
    }
}
pub(super) fn adopt_native_action_in(
    scope: &WorkScope,
    fence: &NativeActionFence,
    source: &mut Option<JointClaim>,
    target: &mut Option<NativeAsyncActionGrant>,
) -> Result<WorkloadProgressNotification, WorkError> {
    adopt_in(scope, source, Target::NativeAction(target, fence))
}

/// Closed Native response custody, distinct from generic retained payload.
/// The same original record pays the canonical receipt cells until their final
/// actual backing alias exits; no CPU, I/O or physical queue survives here.
#[must_use = "Keep this original pin in the canonical reply backing"]
pub struct NativeReplyGrant {
    pin: OriginalPin,
}
impl NativeReplyGrant {
    pub fn reply_records(&self) -> Result<u64, WorkError> {
        let key = self.pin.key.ok_or(WorkError::Released)?;
        let state = self.pin.inner.state.lock().unwrap();
        let row = state
            .joint
            .as_ref()
            .and_then(|ledger| ledger.claims.get(&key[0]))
            .ok_or(WorkError::Released)?;
        if !matches!(row.custody, ClaimCustody::NativeReply { epoch, pins } if epoch == key[1] && pins != 0)
        {
            return Err(WorkError::Conflict);
        }
        Ok(row.active_amounts.get(JointResource::ReplyRecords))
    }

    pub fn capacity_bytes(&self) -> Result<u64, RetentionPinError> {
        payload_capacity(
            &self.pin.inner,
            self.pin.key.ok_or(RetentionPinError::Released)?,
        )
    }
}

pub(super) fn adopt_native_reply_in(
    scope: &WorkScope,
    fence: &NativeActionFence,
    source: &mut Option<JointClaim>,
    target: &mut Option<NativeReplyGrant>,
) -> Result<WorkloadProgressNotification, WorkError> {
    adopt_in(scope, source, Target::NativeReply(target, fence))
}
pub(super) fn adopt_native_input_in(
    scope: &WorkScope,
    fence: &NativeActionFence,
    source: &mut Option<JointClaim>,
    target: &mut Option<NativeQueuedInputGrant>,
) -> Result<WorkloadProgressNotification, WorkError> {
    adopt_in(scope, source, Target::NativeInput(target, fence))
}
/// One original Native input, before dispatcher/physical publication.
/// The unique queue holder explicitly retires its one queue record only after
/// actual removal/transport exit. Independent immutable payload aliases retain
/// the original paid bytes; they cannot release the queue or mint another job.
#[must_use]
pub struct NativeQueuedInputGrant {
    pin: OriginalPin,
}
impl NativeQueuedInputGrant {
    pub fn retain_payload(&self) -> Result<ExistingOwnerRetentionLease, RetentionPinError> {
        let mut pin = self.pin.try_clone()?;
        let key = pin.key.take().ok_or(RetentionPinError::Released)?;
        Ok(ExistingOwnerRetentionLease::from_existing_pin(
            Arc::clone(&pin.inner) as Arc<dyn RetentionOwner>,
            key,
        ))
    }
    /// Original Native queue author only, after the same process permit and
    /// queued/actual send input have genuinely exited. This does not release
    /// payload aliases and does not certify remote mutation settlement.
    pub fn queue_exited_in(&self) -> Result<WorkloadProgressNotification, WorkError> {
        let key = self.pin.key.ok_or(WorkError::Released)?;
        self.pin.inner.update_facts_silent(|state|{
            let ledger=state.joint.as_mut().ok_or(WorkError::NotReady)?;
            let row=ledger.claims.get_mut(&key[0]).ok_or(WorkError::Released)?;
            if !matches!(row.custody,ClaimCustody::NativeInput{epoch,pins,queued:true} if epoch==key[1]&&pins!=0)
                ||row.pending.is_some()||row.amounts!=row.active_amounts||row.facts.key!=row.active_facts.key{
                return Err(WorkError::Conflict)
            }
            let revision=row.release_revision.checked_add(1).ok_or(WorkError::ArithmeticOverflow)?;
            let released=JointResources::ZERO.with(JointResource::QueueRecords,1);
            let charged=row.amounts.checked_sub(released)?;
            let active=row.active_amounts.checked_sub(released)?;
row.amounts=charged;row.active_amounts=active;row.release_revision=revision;
            let ClaimCustody::NativeInput{queued,..}=&mut row.custody else{unreachable!()};*queued=false;
            ledger.released();Ok(())
        })?;
        Ok(WorkloadProgressNotification::new(Arc::clone(
            &self.pin.inner,
        )))
    }
}

/// A payload-only original paid record. Its inline pin allocates no holder.
#[must_use = "Keep original payload custody through actual backing exit"]
pub struct RetainedPayloadGrant {
    pin: OriginalPin,
}
impl RetainedPayloadGrant {
    /// Inspect the same paid envelope without minting or retaining an alias.
    pub fn capacity_bytes(&self) -> Result<u64, RetentionPinError> {
        payload_capacity(
            &self.pin.inner,
            self.pin.key.ok_or(RetentionPinError::Released)?,
        )
    }
    /// Exact active metadata amount of this same live original pin.
    pub fn metadata_bytes(&self) -> Result<u64, RetentionPinError> {
        payload_resource(
            &self.pin.inner,
            self.pin.key.ok_or(RetentionPinError::Released)?,
            JointResource::MetadataBytes,
        )
    }
    /// Report original payload backing which has actually exited. The holder
    /// must preserve the complete remaining backing floor, including aliases.
    /// This changes only PayloadBytes; identity, domains and metadata stay paid.
    /// Neither cancellation nor a remote terminal is a disposal proof.
    pub fn retire_unused_to(
        &self,
        retained_bytes: std::num::NonZeroU64,
    ) -> Result<WorkloadProgressNotification, WorkError> {
        let key = self.pin.key.ok_or(WorkError::Released)?;
        self.pin.inner.update_facts_silent(|state| {
            let ledger = state.joint.as_mut().ok_or(WorkError::NotReady)?;
            let row = ledger.claims.get(&key[0]).ok_or(WorkError::Released)?;
            if !matches!(row.custody, ClaimCustody::Payload { epoch, pins }
                if epoch == key[1] && pins != 0)
                || !matches!(row.owner, ClaimOwner::PhysicalWork(_))
                || row.pending.is_some()
                || row.amounts != row.active_amounts
            {
                return Err(WorkError::Conflict);
            }
            let current = row.active_amounts.get(JointResource::PayloadBytes);
            let released = current
                .checked_sub(retained_bytes.get())
                .ok_or(WorkError::Conflict)?;
            let revision = row
                .release_revision
                .checked_add(1)
                .ok_or(WorkError::ArithmeticOverflow)?;
            let delta = JointResources::ZERO.with(JointResource::PayloadBytes, released);
            let active = row.active_amounts.checked_sub(delta)?;
            let charged = row.amounts.checked_sub(delta)?;
            if released == 0 {
                return Ok(());
            }
            let row = ledger.claims.get_mut(&key[0]).unwrap();
            row.active_amounts = active;
            row.amounts = charged;
            row.release_revision = revision;
            ledger.released();
            Ok(())
        })?;
        Ok(WorkloadProgressNotification::new(Arc::clone(
            &self.pin.inner,
        )))
    }
    /// Inspect the original paid record without retaining or notifying.
    pub fn is_for_scope(&self, scope: &WorkScope) -> bool {
        self.pin.is_for_scope(scope)
    }
    pub fn adopt_in(
        scope: &WorkScope,
        source: &mut Option<JointClaim>,
        target: &mut Option<Self>,
    ) -> Result<WorkloadProgressNotification, WorkError> {
        adopt_in(scope, source, Target::Payload(target))
    }
    pub fn retain_alias(&self) -> Result<RetainedPayloadAlias, RetentionPinError> {
        Ok(RetainedPayloadAlias {
            pin: self.pin.try_clone()?,
        })
    }
    pub fn into_alias(self) -> RetainedPayloadAlias {
        RetainedPayloadAlias { pin: self.pin }
    }
}
/// Retention only, never a new claim source.
/// ```compile_fail
/// use novarocks_workload_control::joint_admission::RetainedPayloadAlias;
/// fn no_source(value: &RetainedPayloadAlias) { value.view(); }
/// ```
pub struct RetainedPayloadAlias {
    pin: OriginalPin,
}
impl RetainedPayloadAlias {
    pub(crate) fn native_source_key(&self) -> Result<[u64; 2], WorkError> {
        self.pin.key.ok_or(WorkError::Released)
    }
    pub fn try_clone(&self) -> Result<Self, RetentionPinError> {
        Ok(Self {
            pin: self.pin.try_clone()?,
        })
    }
    pub fn is_for_scope(&self, scope: &WorkScope) -> bool {
        self.pin.is_for_scope(scope)
    }
    pub fn shares_payload_with(&self, other: &Self) -> bool {
        self.pin.same(&other.pin)
    }
    pub fn capacity_bytes(&self) -> Result<u64, RetentionPinError> {
        payload_capacity(
            &self.pin.inner,
            self.pin.key.ok_or(RetentionPinError::Released)?,
        )
    }
    pub fn check_backing_total(&self, total: u64) -> Result<(), WorkError> {
        if total
            > self.capacity_bytes().map_err(|error| match error {
                RetentionPinError::Released => WorkError::Released,
                RetentionPinError::Conflict => WorkError::Conflict,
                RetentionPinError::Overflow => WorkError::ArithmeticOverflow,
            })?
        {
            Err(WorkError::Capacity("paid retained payload envelope"))
        } else {
            Ok(())
        }
    }
    /// Move this exact already-counted pin, without another Arc allocation or
    /// count increment. Neither the original owner nor its key is exposed.
    pub fn into_retention(mut self) -> ExistingOwnerRetentionLease {
        let key = self.pin.key.take().expect("live original payload pin");
        ExistingOwnerRetentionLease::from_existing_pin(
            Arc::clone(&self.pin.inner) as Arc<dyn RetentionOwner>,
            key,
        )
    }
}
#[must_use = "Only actual compute producers may retain this position"]
pub struct InternalComputationGrant {
    pin: OriginalPin,
}
impl InternalComputationGrant {
    pub(crate) fn validate_client_position(
        &self,
        original: &crate::result_window::ClientComputationWindow,
    ) -> Result<(), WorkError> {
        let state = self.pin.inner.state.lock().unwrap();
        let key = self.pin.key.ok_or(WorkError::Released)?;
        let row = state
            .joint
            .as_ref()
            .ok_or(WorkError::NotReady)?
            .claims
            .get(&key[0])
            .ok_or(WorkError::Released)?;
        let window = row.computation_window.as_ref().ok_or(WorkError::Conflict)?;
        if row.internal_position || !window.same_original(original) {
            return Err(WorkError::Conflict);
        }
        window.validate_claim(key[0])
    }
    pub(crate) fn validate_internal_position(&self) -> Result<(), WorkError> {
        let state = self.pin.inner.state.lock().unwrap();
        let key = self.pin.key.ok_or(WorkError::Released)?;
        let row = state
            .joint
            .as_ref()
            .ok_or(WorkError::NotReady)?
            .claims
            .get(&key[0])
            .ok_or(WorkError::Released)?;
        if !row.internal_position {
            return Err(WorkError::Conflict);
        }
        Ok(())
    }

    /// Observe the actual original stage; no pin or callback.
    pub fn is_for_scope(&self, scope: &WorkScope) -> bool {
        self.pin.is_for_scope(scope)
    }
    pub fn adopt_in(
        scope: &WorkScope,
        source: &mut Option<JointClaim>,
        target: &mut Option<Self>,
    ) -> Result<WorkloadProgressNotification, WorkError> {
        adopt_in(scope, source, Target::Computation(target))
    }
    pub fn retain_alias(&self) -> Result<InternalComputationAlias, RetentionPinError> {
        Ok(InternalComputationAlias {
            pin: self.pin.try_clone()?,
        })
    }
    pub fn into_alias(self) -> InternalComputationAlias {
        InternalComputationAlias { pin: self.pin }
    }
    /// Pure capture from this positive stage grant. Capture can run under the
    /// original receiver lock; it allocates no backing, pins and notifies none.
    /// Construct the paid action outside every receiver/physical-owner lock.
    pub fn cpu_action_source(
        &self,
        scope: &WorkScope,
    ) -> Result<InternalCpuActionSource, WorkError> {
        super::paid_proposal::capture_cpu_action(&self.pin.inner, self.pin.key, scope)
    }
}
/// No alias can release a position still used by another actual producer.
/// Nor can retention-only pins author a new physical action.
/// ```compile_fail
/// use novarocks_workload_control::{WorkScope, joint_admission::InternalComputationAlias};
/// fn no_action(alias: &InternalComputationAlias, scope: &WorkScope) {
///     let _ = alias.cpu_action_source(scope);
/// }
/// ```
/// ```compile_fail
/// use novarocks_workload_control::joint_admission::InternalComputationAlias;
/// fn no_release(value: &mut InternalComputationAlias) { value.release_position(); }
/// ```
pub struct InternalComputationAlias {
    pin: OriginalPin,
}
impl InternalComputationAlias {
    pub fn try_clone(&self) -> Result<Self, RetentionPinError> {
        Ok(Self {
            pin: self.pin.try_clone()?,
        })
    }
    pub fn is_for_scope(&self, scope: &WorkScope) -> bool {
        self.pin.is_for_scope(scope)
    }
    pub fn shares_computation_with(&self, other: &Self) -> bool {
        self.pin.same(&other.pin)
    }
}

// Closed Root/stage construction borrows the original positive grant; aliases
// never gain this view. No pin, backing, owner or epoch is minted by capture.
pub(super) fn payload_growth_view(
    grant: &RetainedPayloadGrant,
    root: &WorkScope,
    state: &State,
) -> Result<JointClaimView, WorkError> {
    if !Arc::ptr_eq(&grant.pin.inner, &root.inner) {
        return Err(WorkError::ForeignAuthority);
    }
    typed_binding_view(
        state,
        root,
        grant.pin.key.ok_or(WorkError::Released)?,
        false,
    )
}
/// Canonical capture may observe an original incarnation held by the same
/// Root writer. It cannot begin a change until that writer's position is free.
pub(super) fn payload_capture_view(
    grant: &RetainedPayloadGrant,
    root: &WorkScope,
    state: &State,
) -> Result<JointClaimView, WorkError> {
    if !Arc::ptr_eq(&grant.pin.inner, &root.inner) {
        return Err(WorkError::ForeignAuthority);
    }
    typed_binding_capture_view(
        state,
        root,
        grant.pin.key.ok_or(WorkError::Released)?,
        false,
    )
}
/// Internal closed zero-growth binding capture. Only positive original grants
/// provide these keys; retention-only aliases never expose this constructor.
pub(super) fn computation_growth_view(
    grant: &InternalComputationGrant,
    stage: &WorkScope,
    state: &State,
) -> Result<JointClaimView, WorkError> {
    if !Arc::ptr_eq(&grant.pin.inner, &stage.inner) {
        return Err(WorkError::ForeignAuthority);
    }
    typed_binding_view(
        state,
        stage,
        grant.pin.key.ok_or(WorkError::Released)?,
        true,
    )
}
pub(super) fn computation_capture_view(
    grant: &InternalComputationGrant,
    stage: &WorkScope,
    state: &State,
) -> Result<JointClaimView, WorkError> {
    if !Arc::ptr_eq(&grant.pin.inner, &stage.inner) {
        return Err(WorkError::ForeignAuthority);
    }
    typed_binding_capture_view(
        state,
        stage,
        grant.pin.key.ok_or(WorkError::Released)?,
        true,
    )
}
pub(super) fn typed_binding_view(
    state: &State,
    work: &WorkScope,
    key: [u64; 2],
    computation: bool,
) -> Result<JointClaimView, WorkError> {
    typed_binding_view_mode(state, work, key, computation, false)
}
pub(super) fn typed_binding_capture_view(
    state: &State,
    work: &WorkScope,
    key: [u64; 2],
    computation: bool,
) -> Result<JointClaimView, WorkError> {
    typed_binding_view_mode(state, work, key, computation, true)
}
fn typed_binding_view_mode(
    state: &State,
    work: &WorkScope,
    key: [u64; 2],
    computation: bool,
    capture: bool,
) -> Result<JointClaimView, WorkError> {
    let row = state
        .joint
        .as_ref()
        .ok_or(WorkError::NotReady)?
        .claims
        .get(&key[0])
        .ok_or(WorkError::Released)?;
    if computation && !row.internal_position && row.computation_window.is_none() {
        return Err(WorkError::Conflict);
    }
    if computation && let Some(window) = &row.computation_window {
        window.validate_claim(key[0])?;
    }
    let custody = match row.custody {
        ClaimCustody::Computation { epoch, pins } if computation => epoch == key[1] && pins != 0,
        ClaimCustody::Payload { epoch, pins } if !computation => epoch == key[1] && pins != 0,
        _ => false,
    };
    if !custody
        || row.owner != ClaimOwner::PhysicalWork(work.id)
        || (!capture && row.pending.is_some())
        || (row.pending.is_none()
            && (row.amounts != row.active_amounts || row.facts.key != row.active_facts.key))
        || !row.active_facts.key.source_kind.is_empty()
    {
        return Err(WorkError::Conflict);
    }
    let view = JointClaimView {
        inner: Arc::downgrade(&work.inner),
        id: key[0],
        source: if computation {
            ClaimSource::Computation { epoch: key[1] }
        } else {
            ClaimSource::Payload { epoch: key[1] }
        },
        structure_revision: row.structure_revision,
        release_revision: row.release_revision,
    };
    if capture {
        root_domains::capture_view(state, view, work.id)
    } else {
        Ok(view)
    }
}

pub(super) fn same_payload_grant(
    grant: &RetainedPayloadGrant,
    inner: &Arc<Inner>,
    id: u64,
    epoch: u64,
) -> bool {
    Arc::ptr_eq(&grant.pin.inner, inner) && grant.pin.key == Some([id, epoch])
}
pub(super) fn install_computation_pin(
    inner: &Arc<Inner>,
    state: &mut State,
    id: u64,
    epoch: u64,
) -> InternalComputationGrant {
    let record = state.joint.as_mut().unwrap().claims.get_mut(&id).unwrap();
    record.structure_revision = epoch;
    record.custody = ClaimCustody::Computation { epoch, pins: 1 };
    InternalComputationGrant {
        pin: OriginalPin {
            inner: Arc::clone(inner),
            key: Some([id, epoch]),
            kind: Kind::Computation,
        },
    }
}

#[cfg(test)]
mod tests;

pub(super) mod unreceived;
pub use unreceived::{
    UnreceivedProductionAdmission, UnreceivedProductionGrant, UnreceivedProductionOwner,
    UnreceivedProductionQuote, UnreceivedProductionReady,
};

pub(crate) fn validate_native_payload_fence(
    state: &State,
    root: WorkId,
    key: [u64; 2],
) -> Result<(), WorkError> {
    let row = state
        .joint
        .as_ref()
        .and_then(|ledger| ledger.claims.get(&key[0]))
        .ok_or(WorkError::Released)?;
    if row.owner != ClaimOwner::PhysicalWork(root)
        || !matches!(row.custody, ClaimCustody::Payload { epoch, pins } if epoch == key[1] && pins != 0)
        || (row.pending.is_some()
            && !root_domains::pending_payload_is_original(state, root, key[0], key[1]))
        || (row.pending.is_none()
            && (row.amounts != row.active_amounts || row.facts.key != row.active_facts.key))
    {
        return Err(WorkError::Conflict);
    }
    Ok(())
}

/// Foreground Native production under a governed Client window still needs
/// the same positive stage computation. Settlement never uses this gate.
pub(crate) fn validate_native_computation_fence(
    state: &State,
    root: WorkId,
    stage: WorkId,
    key: [u64; 2],
) -> Result<(), WorkError> {
    let original = state.nodes.get(&root).ok_or(WorkError::Released)?;
    let execution = state.nodes.get(&stage).ok_or(WorkError::Released)?;
    let row = state
        .joint
        .as_ref()
        .and_then(|l| l.claims.get(&key[0]))
        .ok_or(WorkError::Released)?;
    if original.owner != crate::OwnerState::Active
        || original.parent.is_some()
        || original.root != root
        || execution.owner != crate::OwnerState::Active
        || execution.root != root
        || execution.parent != Some(root)
        || execution.class != original.class
        || row.owner != ClaimOwner::PhysicalWork(stage)
        || !matches!(row.custody, ClaimCustody::Computation { epoch, pins } if epoch == key[1] && pins != 0)
        || row.pending.is_some()
        || row.amounts != row.active_amounts
        || row.facts.key != row.active_facts.key
        || row.active_amounts.get(JointResource::Computations) != 1
        || !row.active_facts.key.source_kind.is_empty()
    {
        return Err(WorkError::Conflict);
    }
    Ok(())
}

/// Initial payload and canonical Root activation cross the same State cut.
/// The caller's stable target receives the real pin before a notification exists.
pub(super) fn adopt_initial_root_in(
    scope: &WorkScope,
    source: &mut Option<JointClaim>,
    receipt: Result<&mut JointAdjustmentReceipt, WorkError>,
    target: &mut Option<RetainedPayloadGrant>,
) -> Result<WorkloadProgressNotification, WorkError> {
    let receipt = receipt?;
    if target.is_some() {
        return Err(WorkError::Conflict);
    }
    let original = source.as_ref().ok_or(WorkError::Released)?;
    if !Arc::ptr_eq(&original.inner, &scope.inner) || !Arc::ptr_eq(&receipt.inner, &scope.inner) {
        return Err(WorkError::ForeignAuthority);
    }
    let inner = Arc::clone(&scope.inner);
    let ticket = receipt.id.ok_or(WorkError::Released)?;
    let (retired_graph, retired_guard) = {
        let mut state = inner.state.lock().unwrap();
        let original = source.as_mut().unwrap();
        root_domains::validate_install(&state, ticket, scope.id)?;
        validate_adoption(&state, scope, original, Kind::Payload)?;
        let id = original.id.unwrap();
        let row = state.joint.as_mut().unwrap().claims.get_mut(&id).unwrap();
        let epoch = row
            .structure_revision
            .checked_add(1)
            .ok_or(WorkError::ArithmeticOverflow)?;
        row.structure_revision = epoch;
        row.custody = Kind::Payload.new_custody(epoch);
        row.domain_birth = None;
        original.id = None;
        *target = Some(RetainedPayloadGrant {
            pin: OriginalPin {
                inner: Arc::clone(&inner),
                key: Some([id, epoch]),
                kind: Kind::Payload,
            },
        });
        let retired = root_domains::install(&mut state, ticket);
        receipt.id = None;
        remove_request(&mut state, ticket);
        state.advance_progress_revision();
        (retired, source.take())
    };
    drop(retired_graph);
    drop(retired_guard);
    Ok(WorkloadProgressNotification::new(inner))
}

mod native_dispatch;
pub use native_dispatch::{NativeDispatchFrozen, NativeDispatchSource};
pub(super) use native_dispatch::{NativeDispatchKind, NativeDispatchOrigin};
