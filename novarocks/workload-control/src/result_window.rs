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

//! Fixed result-owner capabilities. A position covers one complete declared
//! backing envelope; it does not reserve allocator bytes or imply reclamation.
//! Product admission commits this grant together with the computation permit.

use crate::{
    WorkError, WorkId, WorkScope, WorkloadControl,
    scope::{Inner, State},
};
use std::sync::{
    Arc, Weak,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

pub(crate) mod retained_bytes;
pub use retained_bytes::ResultPayloadBytes;
use retained_bytes::RetainedBytes;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResultWindowClass {
    Client,
    Local,
    Internal,
    Closing,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResultClosingCut {
    AcceptedCancellation,
    OriginatingFailure,
}
impl ResultWindowClass {
    pub(crate) const fn index(self) -> usize {
        match self {
            Self::Client => 0,
            Self::Local => 1,
            Self::Internal => 2,
            Self::Closing => 3,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResultCapacityConfig {
    pub client_compute_positions: usize,
    pub client_short_tail_positions: usize,
    pub supported_cancel_burst: u64,
    pub sustained_cancels_per_second: u64,
    pub short_tail_exit_millis: u64,
    pub positions: [usize; 4],
    pub all_objects_bytes: [u64; 4],
}
impl ResultCapacityConfig {
    pub const V1: Self = Self {
        client_compute_positions: 256,
        client_short_tail_positions: 64,
        supported_cancel_burst: 32,
        sustained_cancels_per_second: 16,
        short_tail_exit_millis: 2000,
        positions: [320, 16, 4, 64],
        all_objects_bytes: [
            8 * 1024 * 1024,
            96 * 1024 * 1024,
            1024 * 1024 * 1024,
            8 * 1024 * 1024,
        ],
    };
    pub fn validate(self) -> Result<Self, WorkError> {
        let tail = self
            .sustained_cancels_per_second
            .checked_mul(self.short_tail_exit_millis)
            .and_then(|value| value.checked_add(999))
            .map(|value| value / 1000)
            .and_then(|value| value.checked_add(self.supported_cancel_burst))
            .ok_or(WorkError::ArithmeticOverflow)?;
        let full = self
            .client_compute_positions
            .checked_add(self.client_short_tail_positions)
            .ok_or(WorkError::ArithmeticOverflow)?;
        if self.client_compute_positions == 0
            || self.short_tail_exit_millis == 0
            || tail > self.client_short_tail_positions as u64
            || full > self.positions[0]
        {
            return Err(WorkError::InvalidConfig(
                "client result computation and short-tail capacity",
            ));
        }
        // Positions count actual holders; each holder or Internal Root uses
        // its own byte budget. No product of these declarations is reserved.
        for (positions, bytes) in self.positions.into_iter().zip(self.all_objects_bytes) {
            if positions == 0 || bytes == 0 {
                return Err(WorkError::InvalidConfig("result owner capacity"));
            }
        }
        Ok(self)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ResultCapacitySnapshot {
    pub held_positions: [usize; 4],
}

#[derive(Clone)]
pub struct ResultCapacityHandle {
    inner: Arc<Inner>,
}
impl WorkloadControl {
    /// Host composition installs the complete profile once, before readiness.
    /// Existing callers remain unsupported until they explicitly install it.
    pub fn configure_result_capacity(
        &self,
        config: ResultCapacityConfig,
    ) -> Result<ResultCapacityHandle, WorkError> {
        let config = config.validate()?;
        if self.inner.config.query_concurrency_limit > config.client_compute_positions {
            return Err(WorkError::InvalidConfig(
                "result capacity does not cover computation admission",
            ));
        }
        self.inner.update(|state| {
            if state.ready
                || state.root_admission_is_closed()
                || !state.nodes.is_empty()
                || state.result_capacity.is_some()
            {
                return Err(WorkError::Conflict);
            }
            state.result_capacity = Some(config);
            Ok(ResultCapacityHandle {
                inner: Arc::clone(&self.inner),
            })
        })
    }
}
impl ResultCapacityHandle {
    pub fn snapshot(&self) -> ResultCapacitySnapshot {
        self.inner.state.lock().unwrap().result_windows
    }

    /// Nonblocking complete-position acquisition. Queue admission invokes the
    /// same reservation operation under its one authority transaction. A
    /// Closing grant may only be requested after an accepted cancellation or
    /// originating failure cut; it must never host a normal slow response.
    pub fn try_acquire(
        &self,
        scope: &WorkScope,
        class: ResultWindowClass,
    ) -> Result<ResultWindowGrant, WorkError> {
        if !Arc::ptr_eq(&self.inner, &scope.inner) {
            return Err(WorkError::ForeignAuthority);
        }
        if class == ResultWindowClass::Closing {
            return Err(WorkError::Conflict);
        }
        self.inner
            .update(|state| reserve_window(state, scope, class))
    }
    pub fn try_acquire_closing(
        &self,
        scope: &WorkScope,
        cut: ResultClosingCut,
    ) -> Result<ResultWindowGrant, WorkError> {
        if !Arc::ptr_eq(&self.inner, &scope.inner) {
            return Err(WorkError::ForeignAuthority);
        }
        self.inner.update(|state| {
            let node = state.nodes.get(&scope.id).ok_or(WorkError::Released)?;
            if cut == ResultClosingCut::AcceptedCancellation
                && state
                    .cancellations
                    .cell(node.cancellation)
                    .check_reason()
                    .is_none()
            {
                return Err(WorkError::Conflict);
            }
            let mut grant = reserve_window(state, scope, ResultWindowClass::Closing)?;
            grant.closing_cut = Some(cut);
            Ok(grant)
        })
    }
}

impl WorkScope {
    /// Access this scope's original installed host capacity. This never creates a
    /// fallback pool or changes the admission geometry.
    pub fn result_capacity(&self) -> Result<ResultCapacityHandle, WorkError> {
        if self.inner.state.lock().unwrap().result_capacity.is_none() {
            return Err(WorkError::NotReady);
        }
        Ok(ResultCapacityHandle {
            inner: Arc::clone(&self.inner),
        })
    }
}

pub(crate) fn reserve_window(
    state: &mut State,
    scope: &WorkScope,
    class: ResultWindowClass,
) -> Result<ResultWindowGrant, WorkError> {
    // Admission closure rejects new computation; an existing owner's bounded
    // terminal delivery still requires its independent Closing position.
    if state.closed && class != ResultWindowClass::Closing {
        return Err(WorkError::Closed);
    }
    let node = state.nodes.get(&scope.id).ok_or(WorkError::Released)?;
    // Closing keeps the original responsibility after cancellation. It does
    // not admit new computation and must not clear its cancellation reason.
    if class != ResultWindowClass::Closing {
        node.check(&state.cancellations)?;
    }
    let all_objects_bytes = count_window(state, scope.id, class)?;
    Ok(adopt_window(scope, class, all_objects_bytes))
}

/// Whether one more complete position of `class` fits right now.
pub(crate) fn window_room(
    capacity: &Option<ResultCapacityConfig>,
    held: &ResultCapacitySnapshot,
    class: ResultWindowClass,
) -> bool {
    capacity
        .is_some_and(|config| held.held_positions[class.index()] < config.positions[class.index()])
}

/// Count one complete position for `scope` under the authority transaction and
/// return its all-objects envelope. The caller turns the counted position into
/// exactly one grant, or gives it back with [`uncount_window`].
pub(crate) fn count_window(
    state: &mut State,
    scope: WorkId,
    class: ResultWindowClass,
) -> Result<u64, WorkError> {
    let config = state.result_capacity.ok_or(WorkError::NotReady)?;
    let index = class.index();
    if state.result_windows.held_positions[index] >= config.positions[index] {
        return Err(WorkError::Capacity("complete result window"));
    }
    let node = state.nodes.get_mut(&scope).ok_or(WorkError::Released)?;
    node.resource_holders = node
        .resource_holders
        .checked_add(1)
        .ok_or(WorkError::ArithmeticOverflow)?;
    node.result_windows.held_positions[index] += 1;
    state.result_windows.held_positions[index] += 1;
    Ok(config.all_objects_bytes[index])
}

/// Give back a counted position that never became a grant.
pub(crate) fn uncount_window(state: &mut State, scope: WorkId, class: ResultWindowClass) {
    state.result_windows.held_positions[class.index()] -= 1;
    let node = state
        .nodes
        .get_mut(&scope)
        .expect("a counted window retains its responsibility");
    node.resource_holders -= 1;
    node.result_windows.held_positions[class.index()] -= 1;
}

/// The unique grant for a position already counted for `scope`.
pub(crate) fn adopt_window(
    scope: &WorkScope,
    class: ResultWindowClass,
    all_objects_bytes: u64,
) -> ResultWindowGrant {
    ResultWindowGrant {
        holder: Arc::new(WindowHolder {
            scope: scope.clone(),
            class,
            all_objects_bytes,
            retained_bytes: RetainedBytes::new(all_objects_bytes),
            computation: Arc::new(ClientComputationPosition::default()),
        }),
        closing_cut: None,
    }
}

#[derive(Default)]
struct ClientComputationPosition {
    active_claim: AtomicU64,
    window_exited: AtomicBool,
}

/// A fence to one original Client position. State serializes every transition;
/// the cell never upgrades the holder or runs its destructor under State.
#[derive(Clone)]
pub(crate) struct ClientComputationWindow {
    holder: Weak<WindowHolder>,
    position: Arc<ClientComputationPosition>,
    root: WorkId,
}
impl ClientComputationWindow {
    pub(crate) fn room(&self) -> Result<bool, WorkError> {
        self.validate_live()?;
        Ok(self.position.active_claim.load(Ordering::Relaxed) == 0)
    }
    pub(crate) fn validate_live(&self) -> Result<(), WorkError> {
        if self.holder.strong_count() == 0 {
            Err(WorkError::Released)
        } else {
            Ok(())
        }
    }
    pub(crate) fn acquire(&self, claim: u64) {
        assert_eq!(self.position.active_claim.swap(claim, Ordering::Relaxed), 0);
    }
    pub(crate) fn validate_claim(&self, claim: u64) -> Result<(), WorkError> {
        self.validate_live()?;
        if self.position.active_claim.load(Ordering::Relaxed) != claim {
            return Err(WorkError::Conflict);
        }
        Ok(())
    }
    pub(crate) fn same_original(&self, other: &Self) -> bool {
        self.root == other.root
            && Weak::ptr_eq(&self.holder, &other.holder)
            && Arc::ptr_eq(&self.position, &other.position)
    }
    pub(crate) fn release(&self, state: &mut State, claim: u64) {
        assert_eq!(self.position.active_claim.swap(0, Ordering::Relaxed), claim);
        if self.position.window_exited.load(Ordering::Relaxed) {
            uncount_window(state, self.root, ResultWindowClass::Client);
            state.collect(self.root);
        }
    }
}

pub(crate) struct WindowHolder {
    computation: Arc<ClientComputationPosition>,
    retained_bytes: RetainedBytes,
    scope: WorkScope,
    class: ResultWindowClass,
    all_objects_bytes: u64,
}
impl Drop for WindowHolder {
    fn drop(&mut self) {
        self.scope.inner.update(|state| {
            self.computation
                .window_exited
                .store(true, Ordering::Relaxed);
            if self.computation.active_claim.load(Ordering::Relaxed) == 0 {
                uncount_window(state, self.scope.id, self.class);
                state.collect(self.scope.id);
            }
            // A live computation retains this exact counted Client position
            // until its last actual execution alias exits.
        });
    }
}

/// A unique owning grant, retained through fetch/codec/transport actual exit.
/// Moving a writer into closing requires a separately acquired Closing grant.
/// Dropping a timeout or JoinHandle is not evidence that its aliases exited.
#[must_use = "every result backing and physical alias must retain its grant"]
pub struct ResultWindowGrant {
    holder: Arc<WindowHolder>,
    closing_cut: Option<ResultClosingCut>,
}
impl ResultWindowGrant {
    pub fn scope_id(&self) -> WorkId {
        self.holder.scope.id()
    }
    pub fn class(&self) -> ResultWindowClass {
        self.holder.class
    }
    pub fn closing_cut(&self) -> Option<ResultClosingCut> {
        self.closing_cut
    }
    pub fn has_retained_aliases(&self) -> bool {
        Arc::strong_count(&self.holder) != 1
    }
    pub fn is_for_scope(&self, scope: &WorkScope) -> bool {
        self.holder.scope.id == scope.id && Arc::ptr_eq(&self.holder.scope.inner, &scope.inner)
    }
    pub fn all_objects_bytes(&self) -> u64 {
        self.holder.all_objects_bytes
    }
    pub fn check_backing_total(&self, simultaneously_live_bytes: u64) -> Result<(), WorkError> {
        if simultaneously_live_bytes > self.all_objects_bytes() {
            Err(WorkError::Capacity("result backing envelope"))
        } else {
            Ok(())
        }
    }
    /// Transfer/copy only after checking all simultaneously live backing
    /// capacities, including old + new. This guard covers the original grant
    /// and keeps its position live until the last physical alias exits.
    pub fn retain_alias(&self) -> ResultWindowAlias {
        ResultWindowAlias {
            backing: AliasBacking::Window {
                holder: Arc::clone(&self.holder),
                execution_scope: None,
                manifest: None,
                client_computation: None,
            },
        }
    }
}
/// A view of the original result owner's data budget and retained lifetime.
/// Internal Root pins do not acquire a delegated Node/control holder for an
/// attributed execution scope, nor does retention take a computation position.
pub struct ResultWindowAlias {
    backing: AliasBacking,
}
type NativeComputationStamp = (WorkId, [u64; 2]);

enum AliasBacking {
    Window {
        holder: Arc<WindowHolder>,
        execution_scope: Option<Arc<WindowExecutionScope>>,
        client_computation: Option<NativeComputationStamp>,
        manifest: Option<(
            crate::joint_admission::InternalManifestBinding,
            crate::joint_admission::RetainedPayloadAlias,
        )>,
    },
    RootRetention {
        budget: Arc<RetainedBytes>,
        pin: Arc<WindowExecutionScope>,
        execution: WorkScope,
    },
    InternalRoot {
        budget: Arc<RetainedBytes>,
        payload: crate::joint_admission::RetainedPayloadAlias,
        root: WorkScope,
        execution: WorkScope,
        binding: Option<crate::joint_admission::InternalManifestBinding>,
        streamed_computation: Option<NativeComputationStamp>,
    },
}

// One existing scope resource pin. Root data and legacy window attribution
// keep this original scope live independently of any computation position.
struct WindowExecutionScope {
    scope: WorkScope,
}
impl Drop for WindowExecutionScope {
    fn drop(&mut self) {
        self.scope.inner.update(|state| {
            let node = state
                .nodes
                .get_mut(&self.scope.id)
                .expect("result retention keeps its original scope");
            node.resource_holders -= 1;
            state.collect(self.scope.id);
        });
    }
}
fn internal_pin_error(error: novarocks_resource_contract::RetentionPinError) -> WorkError {
    use novarocks_resource_contract::RetentionPinError;
    match error {
        RetentionPinError::Released => WorkError::Released,
        RetentionPinError::Conflict => WorkError::Conflict,
        RetentionPinError::Overflow => WorkError::ArithmeticOverflow,
    }
}
#[derive(Clone)]
pub(crate) enum NativeCapacityFence {
    Window(std::sync::Weak<WindowHolder>),
    ClientWindow {
        holder: std::sync::Weak<WindowHolder>,
        stage: WorkId,
        key: [u64; 2],
    },
    InternalPayload {
        payload: [u64; 2],
        computation: Option<NativeComputationStamp>,
    },
}
impl NativeCapacityFence {
    pub(crate) fn validate(&self, state: &State, root: WorkId) -> Result<(), WorkError> {
        match self {
            Self::Window(holder) => {
                // No upgrade/last-holder Drop while State is locked. The
                // original alias was borrowed when this exact Weak was minted.
                if holder.strong_count() == 0 {
                    return Err(WorkError::Released);
                }
                Ok(())
            }
            Self::ClientWindow { holder, stage, key } => {
                if holder.strong_count() == 0 {
                    return Err(WorkError::Released);
                }
                crate::joint_admission::ledger::validate_native_computation_fence(
                    state, root, *stage, *key,
                )
            }
            Self::InternalPayload {
                payload,
                computation,
            } => {
                crate::joint_admission::ledger::validate_native_payload_fence(
                    state, root, *payload,
                )?;
                if let Some((stage, key)) = computation {
                    crate::joint_admission::ledger::validate_native_computation_fence(
                        state, root, *stage, *key,
                    )?;
                }
                Ok(())
            }
        }
    }
}
fn internal_root_budget(state: &mut State, root: WorkId) -> Result<Arc<RetainedBytes>, WorkError> {
    let limit = state
        .result_capacity
        .ok_or(WorkError::NotReady)?
        .all_objects_bytes[ResultWindowClass::Internal.index()];
    let node = state.nodes.get_mut(&root).ok_or(WorkError::Released)?;
    Ok(match node.internal_result_bytes.upgrade() {
        Some(budget) => budget,
        None => {
            let budget = Arc::new(RetainedBytes::new(limit));
            node.internal_result_bytes = Arc::downgrade(&budget);
            budget
        }
    })
}

impl ResultWindowAlias {
    /// Retain actual data under this unique active Root owner. This borrows
    /// the installed per-Root byte budget without taking a computation/window
    /// position, payload grant, manifest binding or physical execution source.
    pub fn from_root_owner(owner: &crate::WorkOwner) -> Result<Self, WorkError> {
        let root = owner.scope();
        root.inner.update(|state| {
            let node = state.nodes.get(&root.id).ok_or(WorkError::Released)?;
            node.check(&state.cancellations)?;
            if node.owner != crate::OwnerState::Active
                || node.parent.is_some()
                || node.root != root.id
            {
                return Err(WorkError::Conflict);
            }
            let holders = node
                .resource_holders
                .checked_add(1)
                .ok_or(WorkError::ArithmeticOverflow)?;
            let budget = internal_root_budget(state, root.id)?;
            state.nodes.get_mut(&root.id).unwrap().resource_holders = holders;
            Ok(Self {
                backing: AliasBacking::RootRetention {
                    budget,
                    pin: Arc::new(WindowExecutionScope {
                        scope: root.clone(),
                    }),
                    execution: root.clone(),
                },
            })
        })
    }

    pub(crate) fn client_computation_window(
        &self,
        root: &WorkScope,
    ) -> Result<ClientComputationWindow, WorkError> {
        match &self.backing {
            AliasBacking::Window { holder, .. }
                if holder.class == ResultWindowClass::Client && self.is_for_scope(root) =>
            {
                Ok(ClientComputationWindow {
                    holder: Arc::downgrade(holder),
                    position: Arc::clone(&holder.computation),
                    root: holder.scope.id,
                })
            }
            _ => Err(WorkError::Conflict),
        }
    }

    pub fn native_action_authority(
        &self,
    ) -> Result<crate::joint_admission::NativeAsyncActionAuthority, WorkError> {
        let fence = match &self.backing {
            AliasBacking::Window {
                holder,
                client_computation,
                ..
            } => {
                if holder.class == ResultWindowClass::Closing {
                    return Err(WorkError::Conflict);
                }
                if let Some((stage, key)) = client_computation {
                    if self.execution_scope().id != *stage {
                        return Err(WorkError::Conflict);
                    }
                    NativeCapacityFence::ClientWindow {
                        holder: Arc::downgrade(holder),
                        stage: *stage,
                        key: *key,
                    }
                } else {
                    NativeCapacityFence::Window(Arc::downgrade(holder))
                }
            }
            AliasBacking::RootRetention { .. } => return Err(WorkError::Conflict),
            AliasBacking::InternalRoot {
                payload,
                execution,
                streamed_computation,
                ..
            } => {
                if streamed_computation.is_some_and(|(stage, _)| execution.id != stage) {
                    return Err(WorkError::Conflict);
                }
                NativeCapacityFence::InternalPayload {
                    payload: payload.native_source_key()?,
                    computation: *streamed_computation,
                }
            }
        };
        crate::joint_admission::ledger::delegate_native_actions(
            self.root_scope(),
            self.execution_scope(),
            fence,
        )
    }
    /// Delegates only from the original result owner, never from a numeric
    /// window size. Neither capture nor its weak fence adds a position/pin.
    pub fn native_action_source(
        &self,
        lane: crate::joint_admission::JointLane,
        quote: crate::joint_admission::NativeAsyncActionQuote,
    ) -> Result<crate::joint_admission::NativeAsyncActionSource, WorkError> {
        let fence = match &self.backing {
            AliasBacking::Window {
                holder,
                client_computation,
                ..
            } => {
                if holder.class == ResultWindowClass::Closing {
                    return Err(WorkError::Conflict);
                }
                if let Some((stage, key)) = client_computation {
                    if self.execution_scope().id != *stage {
                        return Err(WorkError::Conflict);
                    }
                    NativeCapacityFence::ClientWindow {
                        holder: Arc::downgrade(holder),
                        stage: *stage,
                        key: *key,
                    }
                } else {
                    NativeCapacityFence::Window(Arc::downgrade(holder))
                }
            }
            AliasBacking::RootRetention { .. } => return Err(WorkError::Conflict),
            AliasBacking::InternalRoot {
                payload,
                execution,
                streamed_computation,
                ..
            } => {
                if streamed_computation.is_some_and(|(stage, _)| execution.id != stage) {
                    return Err(WorkError::Conflict);
                }
                NativeCapacityFence::InternalPayload {
                    payload: payload.native_source_key()?,
                    computation: *streamed_computation,
                }
            }
        };
        crate::joint_admission::ledger::capture_native_action(
            self.root_scope(),
            self.execution_scope(),
            fence,
            lane,
            quote,
        )
    }
    /// Project the original Root payload into an actual execution. This is
    /// retention/attribution only, never a computation or physical grant.
    pub fn from_internal_payload(
        root: &WorkScope,
        execution: &WorkScope,
        grant: &crate::joint_admission::RetainedPayloadGrant,
    ) -> Result<Self, WorkError> {
        // Validate before creating a pin: refusal must not destroy even a
        // temporary typed alias while an external receiver holds its lock.
        if !grant.is_for_scope(root) {
            return Err(WorkError::Conflict);
        }
        if !Arc::ptr_eq(&root.inner, &execution.inner) {
            return Err(WorkError::ForeignAuthority);
        }
        {
            let state = root.inner.state.lock().unwrap();
            let original = state.nodes.get(&root.id).ok_or(WorkError::Released)?;
            original.check(&state.cancellations)?;
            if original.parent.is_some() || original.root != root.id {
                return Err(WorkError::Conflict);
            }
            let current = state.nodes.get(&execution.id).ok_or(WorkError::Released)?;
            current.check(&state.cancellations)?;
            if current.class != original.class
                || (execution.id != root.id && current.parent != Some(root.id))
            {
                return Err(WorkError::Conflict);
            }
        }
        let budget = {
            let mut state = root.inner.state.lock().unwrap();
            internal_root_budget(&mut state, root.id)?
        };
        let payload = grant.retain_alias().map_err(internal_pin_error)?;
        Ok(Self {
            backing: AliasBacking::InternalRoot {
                budget,
                payload,
                root: root.clone(),
                execution: execution.clone(),
                binding: None,
                streamed_computation: None,
            },
        })
    }
    /// Author the closed manifest binding only while borrowing the original
    /// positive Root payload and actual direct-child computation grants.
    pub fn from_internal_computation(
        root: &WorkScope,
        stage: &crate::WorkOwner,
        payload: &crate::joint_admission::RetainedPayloadGrant,
        computation: &crate::joint_admission::InternalComputationGrant,
    ) -> Result<Self, WorkError> {
        computation.validate_internal_position()?;
        let binding = crate::joint_admission::InternalManifestBinding::from_grants(
            root,
            stage,
            payload,
            computation,
        )?;
        let mut alias = Self::from_internal_payload(root, &stage.scope(), payload)?;
        let AliasBacking::InternalRoot {
            binding: target, ..
        } = &mut alias.backing
        else {
            unreachable!()
        };
        *target = Some(binding);
        Ok(alias)
    }
    /// Attach only the same positive computation's weak Native liveness fence.
    /// This borrows the original grants; ownership moves only in the receiving
    /// QA custody author. Plain/borrowed Internal payloads keep their contract.
    pub fn from_streamed_internal_computation(
        root: &WorkScope,
        stage: &crate::WorkOwner,
        payload: &crate::joint_admission::RetainedPayloadGrant,
        computation: &crate::joint_admission::InternalComputationGrant,
    ) -> Result<Self, WorkError> {
        let mut alias = Self::from_internal_computation(root, stage, payload, computation)?;
        let AliasBacking::InternalRoot {
            binding: Some(binding),
            streamed_computation,
            ..
        } = &mut alias.backing
        else {
            unreachable!()
        };
        *streamed_computation = Some(binding.computation_identity());
        Ok(alias)
    }

    /// Preserve the actual Client window while attaching only the original
    /// positive Root/stage manifest authority. No Internal result window or
    /// new quota is acquired, and retention alone cannot call this constructor.
    pub fn from_client_computation(
        root: &WorkScope,
        stage: &crate::WorkOwner,
        original: &Self,
        payload: &crate::joint_admission::RetainedPayloadGrant,
        computation: &crate::joint_admission::InternalComputationGrant,
    ) -> Result<Self, WorkError> {
        if original.class() != ResultWindowClass::Client || !original.is_for_scope(root) {
            return Err(WorkError::Conflict);
        }
        computation.validate_client_position(&original.client_computation_window(root)?)?;
        let binding = crate::joint_admission::InternalManifestBinding::from_grants(
            root,
            stage,
            payload,
            computation,
        )?;
        let retained = payload.retain_alias().map_err(internal_pin_error)?;
        let mut alias = original.for_child(&stage.scope())?;
        let AliasBacking::Window {
            manifest,
            client_computation,
            ..
        } = &mut alias.backing
        else {
            return Err(WorkError::Conflict);
        };
        // The QA activity owns the unique positive grant. Window aliases keep
        // only its original incarnation fence and payload/window retention.
        *client_computation = Some(binding.computation_identity());
        *manifest = Some((binding, retained));
        Ok(alias)
    }
    /// Observe the exact admitted window envelope. This grants no additional capacity.
    pub fn all_objects_bytes(&self) -> Result<u64, WorkError> {
        match &self.backing {
            AliasBacking::Window { holder, .. } => Ok(holder.all_objects_bytes),
            _ => Err(WorkError::Conflict),
        }
    }
    /// Read the SAME original Client envelope, never a guessed peak constant.
    pub fn client_all_objects_bytes(&self) -> Result<u64, WorkError> {
        match &self.backing {
            AliasBacking::Window { holder, .. } if holder.class == ResultWindowClass::Client => {
                Ok(holder.all_objects_bytes)
            }
            _ => Err(WorkError::Conflict),
        }
    }
    /// Borrow the exact positive-grant-derived capability carried by this
    /// original alias. Plain payload/retention constructors cannot mint one.
    pub fn internal_manifest_binding(
        &self,
    ) -> Result<Option<&crate::joint_admission::InternalManifestBinding>, WorkError> {
        match &self.backing {
            AliasBacking::Window {
                manifest: Some((binding, payload)),
                ..
            } => {
                binding.validate_alias(
                    self.root_scope(),
                    self.execution_scope(),
                    payload.native_source_key()?,
                )?;
                Ok(Some(binding))
            }
            AliasBacking::InternalRoot {
                binding: Some(binding),
                root,
                execution,
                payload,
                ..
            } => {
                binding.validate_alias(root, execution, payload.native_source_key()?)?;
                Ok(Some(binding))
            }
            _ => Ok(None),
        }
    }
    /// A streamed alias cannot become a legacy execution allowance through a
    /// new activity binding. The marker grants no computation position itself.
    pub fn has_streamed_computation_stamp(&self) -> bool {
        matches!(
            &self.backing,
            AliasBacking::Window {
                client_computation: Some(_),
                ..
            } | AliasBacking::InternalRoot {
                streamed_computation: Some(_),
                ..
            }
        )
    }
    pub fn is_root_backed_internal(&self) -> bool {
        matches!(
            self.backing,
            AliasBacking::InternalRoot { .. } | AliasBacking::RootRetention { .. }
        )
    }
    /// The physical payload owner remains the statement/job Root.
    pub fn root_scope(&self) -> &WorkScope {
        match &self.backing {
            AliasBacking::Window { holder, .. } => &holder.scope,
            AliasBacking::RootRetention { pin, .. } => &pin.scope,
            AliasBacking::InternalRoot { root, .. } => root,
        }
    }
    pub fn is_for_scope(&self, scope: &WorkScope) -> bool {
        self.execution_scope().id == scope.id
            && Arc::ptr_eq(&self.execution_scope().inner, &scope.inner)
    }
    /// Move the same positive Root payload pin into statement retention.
    /// The completed computation's identity is no longer an execution scope;
    /// dropping its manifest capability cannot mint another computation grant.
    pub fn into_root_retention(mut self) -> Result<Self, WorkError> {
        match &mut self.backing {
            AliasBacking::RootRetention { pin, execution, .. } => {
                pin.scope.check()?;
                *execution = pin.scope.clone();
            }
            AliasBacking::InternalRoot {
                payload,
                root,
                execution,
                binding,
                ..
            } => {
                root.check()?;
                payload.check_backing_total(0)?;
                *execution = root.clone();
                *binding = None;
                // Keep any streamed marker: retention does not reopen the
                // old stage's Native Foreground computation authority.
            }
            AliasBacking::Window { .. } => return Err(WorkError::Conflict),
        }
        Ok(self)
    }

    /// Exact original Root identity, including its owning authority. This
    /// comparison grants no payload or computation capacity.
    pub fn is_for_root_scope(&self, root: &WorkScope) -> bool {
        let original = self.root_scope();
        original.id == root.id && Arc::ptr_eq(&original.inner, &root.inner)
    }
    pub fn shares_capacity_with(&self, other: &Self) -> bool {
        match (&self.backing, &other.backing) {
            (AliasBacking::Window { holder: a, .. }, AliasBacking::Window { holder: b, .. }) => {
                Arc::ptr_eq(a, b)
            }
            (
                AliasBacking::InternalRoot { budget: a, .. }
                | AliasBacking::RootRetention { budget: a, .. },
                AliasBacking::InternalRoot { budget: b, .. }
                | AliasBacking::RootRetention { budget: b, .. },
            ) => Arc::ptr_eq(a, b),
            _ => false,
        }
    }
    pub fn scope_id(&self) -> WorkId {
        self.execution_scope().id()
    }
    fn execution_scope(&self) -> &WorkScope {
        match &self.backing {
            AliasBacking::Window {
                holder,
                execution_scope,
                ..
            } => execution_scope
                .as_ref()
                .map_or(&holder.scope, |value| &value.scope),
            AliasBacking::InternalRoot { execution, .. }
            | AliasBacking::RootRetention { execution, .. } => execution,
        }
    }
    pub fn try_clone(&self) -> Result<Self, WorkError> {
        let backing = match &self.backing {
            AliasBacking::Window {
                holder,
                execution_scope,
                manifest,
                client_computation,
            } => AliasBacking::Window {
                holder: Arc::clone(holder),
                execution_scope: execution_scope.clone(),
                client_computation: *client_computation,
                manifest: manifest
                    .as_ref()
                    .map(|(binding, payload)| {
                        Ok((
                            binding.clone(),
                            payload.try_clone().map_err(internal_pin_error)?,
                        ))
                    })
                    .transpose()?,
            },
            AliasBacking::RootRetention {
                budget,
                pin,
                execution,
            } => AliasBacking::RootRetention {
                budget: Arc::clone(budget),
                pin: Arc::clone(pin),
                execution: execution.clone(),
            },
            AliasBacking::InternalRoot {
                budget,
                payload,
                root,
                execution,
                binding,
                streamed_computation,
            } => AliasBacking::InternalRoot {
                budget: Arc::clone(budget),
                payload: payload.try_clone().map_err(internal_pin_error)?,
                root: root.clone(),
                execution: execution.clone(),
                binding: binding.clone(),
                streamed_computation: *streamed_computation,
            },
        };
        Ok(Self { backing })
    }
    /// Move this exact original payload pin to SPI without allocating a holder.
    /// Legacy windows and Root-only retention have no such payload record and
    /// are returned unchanged instead of inventing a payload lease.
    pub fn into_internal_retention(
        self,
    ) -> Result<novarocks_resource_contract::ExistingOwnerRetentionLease, Self> {
        match self.backing {
            AliasBacking::InternalRoot { payload, .. } => Ok(payload.into_retention()),
            backing => Err(Self { backing }),
        }
    }
    pub fn for_child(&self, child: &WorkScope) -> Result<Self, WorkError> {
        let parent = self.execution_scope();
        if !Arc::ptr_eq(&parent.inner, &child.inner) {
            return Err(WorkError::ForeignAuthority);
        }
        if self.class() == ResultWindowClass::Closing {
            return Err(WorkError::Conflict);
        }
        if let AliasBacking::RootRetention { budget, pin, .. } = &self.backing {
            {
                let state = parent.inner.state.lock().unwrap();
                let original = state.nodes.get(&parent.id).ok_or(WorkError::Released)?;
                original.check(&state.cancellations)?;
                let current = state.nodes.get(&child.id).ok_or(WorkError::Released)?;
                current.check(&state.cancellations)?;
                if current.parent != Some(parent.id) || current.class != original.class {
                    return Err(WorkError::Conflict);
                }
            }
            return Ok(Self {
                backing: AliasBacking::RootRetention {
                    budget: Arc::clone(budget),
                    pin: Arc::clone(pin),
                    execution: child.clone(),
                },
            });
        }
        if let AliasBacking::InternalRoot {
            budget,
            payload,
            root,
            streamed_computation,
            ..
        } = &self.backing
        {
            // Clone while the original alias is borrowed, outside State. On
            // refusal this cannot be the last payload pin.
            let payload = payload.try_clone().map_err(internal_pin_error)?;
            {
                let state = parent.inner.state.lock().unwrap();
                let original = state.nodes.get(&parent.id).ok_or(WorkError::Released)?;
                original.check(&state.cancellations)?;
                let current = state.nodes.get(&child.id).ok_or(WorkError::Released)?;
                current.check(&state.cancellations)?;
                if current.parent != Some(parent.id) || current.class != original.class {
                    return Err(WorkError::Conflict);
                }
            }
            return Ok(Self {
                backing: AliasBacking::InternalRoot {
                    budget: Arc::clone(budget),
                    payload,
                    root: root.clone(),
                    execution: child.clone(),
                    // A different execution cannot inherit the old stage's
                    // positive-computation authority through retention alone.
                    binding: None,
                    streamed_computation: *streamed_computation,
                },
            });
        }
        let AliasBacking::Window {
            holder,
            client_computation,
            ..
        } = &self.backing
        else {
            unreachable!()
        };
        child.inner.update(|state| {
            state
                .nodes
                .get(&parent.id)
                .ok_or(WorkError::Released)?
                .check(&state.cancellations)?;
            let node = state.nodes.get_mut(&child.id).ok_or(WorkError::Released)?;
            node.check(&state.cancellations)?;
            if node.parent != Some(parent.id) {
                return Err(WorkError::Conflict);
            }
            node.resource_holders = node
                .resource_holders
                .checked_add(1)
                .ok_or(WorkError::ArithmeticOverflow)?;
            Ok(Self {
                backing: AliasBacking::Window {
                    holder: Arc::clone(holder),
                    execution_scope: Some(Arc::new(WindowExecutionScope {
                        scope: child.clone(),
                    })),
                    manifest: None,
                    client_computation: *client_computation,
                },
            })
        })
    }
    pub fn class(&self) -> ResultWindowClass {
        match &self.backing {
            AliasBacking::Window { holder, .. } => holder.class,
            AliasBacking::InternalRoot { .. } | AliasBacking::RootRetention { .. } => {
                ResultWindowClass::Internal
            }
        }
    }
    pub fn check_backing_total(&self, simultaneously_live_bytes: u64) -> Result<(), WorkError> {
        match &self.backing {
            AliasBacking::Window { holder, .. }
                if simultaneously_live_bytes > holder.all_objects_bytes =>
            {
                Err(WorkError::Capacity("result backing envelope"))
            }
            AliasBacking::Window { .. } => Ok(()),
            AliasBacking::InternalRoot { budget, .. }
            | AliasBacking::RootRetention { budget, .. } => {
                if simultaneously_live_bytes > budget.limit() {
                    Err(WorkError::Capacity("result backing envelope"))
                } else {
                    Ok(())
                }
            }
        }
    }
}
// Existing callers still have an infallible Clone contract. Typed Internal
// producers use try_clone; overflow preserves the original pin then fails fast.
impl Clone for ResultWindowAlias {
    fn clone(&self) -> Self {
        self.try_clone()
            .expect("original result backing pin clone failed")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{WorkClass, WorkRequest, WorkloadConfig};
    fn control() -> (WorkloadControl, ResultCapacityHandle) {
        let control = WorkloadControl::try_new_counted(WorkloadConfig {
            query_concurrency_limit: 1,
            ..WorkloadConfig::default()
        })
        .unwrap()
        .owner;
        let handle = control
            .configure_result_capacity(ResultCapacityConfig {
                client_compute_positions: 1,
                client_short_tail_positions: 0,
                supported_cancel_burst: 0,
                sustained_cancels_per_second: 0,
                positions: [1; 4],
                ..ResultCapacityConfig::V1
            })
            .unwrap();
        assert!(matches!(control.resources(), Err(WorkError::NotReady)));
        assert_eq!(control.snapshot().resource_limit_bytes, None);
        control.mark_ready().unwrap();
        (control, handle)
    }
    #[test]
    fn physical_window_equality_survives_child_attribution_but_refuses_other_allowances() {
        let (control, _) = control();
        let (root, window) = control
            .root_admission()
            .try_begin_root_with_result(
                WorkRequest::new(WorkClass::Management),
                ResultWindowClass::Internal,
            )
            .unwrap();
        let alias = window.retain_alias();
        let child = root
            .owner
            .scope()
            .child(WorkRequest::new(WorkClass::Management))
            .unwrap();
        let delegated = alias.for_child(&child.scope()).unwrap();
        assert!(alias.shares_capacity_with(&delegated));
        assert!(!alias.is_for_scope(&child.scope()));
        assert!(delegated.is_for_scope(&child.scope()));
        let (foreign_control, _) = self::control();
        let (foreign, foreign_window) = foreign_control
            .root_admission()
            .try_begin_root_with_result(
                WorkRequest::new(WorkClass::Management),
                ResultWindowClass::Internal,
            )
            .unwrap();
        assert_eq!(alias.scope_id(), foreign_window.scope_id());
        assert!(!alias.shares_capacity_with(&foreign_window.retain_alias()));
        drop(delegated);
        child.complete();
        drop(alias);
        drop(window);
        root.owner.complete();
        root.business.release();
        drop(foreign_window);
        foreign.owner.complete();
        foreign.business.release();
    }

    #[test]
    fn nonqueued_root_window_refusal_is_atomic_and_aliases_keep_the_root() {
        let (control, capacity) = control();
        let admission = control.root_admission();
        let (root, window) = admission
            .try_begin_root_with_result(
                WorkRequest::new(WorkClass::Management),
                ResultWindowClass::Local,
            )
            .unwrap();
        let alias = window.retain_alias();
        let before = control.snapshot();
        assert!(matches!(
            admission.try_begin_root_with_result(
                WorkRequest::new(WorkClass::Management),
                ResultWindowClass::Local
            ),
            Err(WorkError::Capacity("complete result window"))
        ));
        assert_eq!(
            control.snapshot().root_responsibilities,
            before.root_responsibilities
        );
        assert_eq!(control.snapshot().businesses, before.businesses);
        assert_eq!(control.inner.state.lock().unwrap().nodes.len(), 1);
        root.owner.complete();
        root.business.release();
        drop(window);
        assert_eq!(control.snapshot().businesses, 0);
        assert_eq!(control.snapshot().root_responsibilities, 1);
        assert_eq!(capacity.snapshot().held_positions, [0, 1, 0, 0]);
        drop(alias);
        assert_eq!(control.snapshot().root_responsibilities, 0);
        let (next, window) = admission
            .try_begin_root_with_result(
                WorkRequest::new(WorkClass::Management),
                ResultWindowClass::Local,
            )
            .unwrap();
        next.owner.complete();
        next.business.release();
        drop(window);
        assert_eq!(control.snapshot().root_responsibilities, 0);
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    }

    #[test]
    fn nonqueued_root_rejects_unconfigured_or_closing_window_without_work() {
        let unconfigured = WorkloadControl::try_new_counted(WorkloadConfig::default())
            .unwrap()
            .owner;
        unconfigured.mark_ready().unwrap();
        assert!(matches!(
            unconfigured.root_admission().try_begin_root_with_result(
                WorkRequest::new(WorkClass::Management),
                ResultWindowClass::Local
            ),
            Err(WorkError::NotReady)
        ));
        assert_eq!(unconfigured.snapshot().root_responsibilities, 0);
        assert_eq!(unconfigured.snapshot().businesses, 0);
        assert!(unconfigured.inner.state.lock().unwrap().nodes.is_empty());
        let (control, capacity) = control();
        assert!(matches!(
            control.root_admission().try_begin_root_with_result(
                WorkRequest::new(WorkClass::Management),
                ResultWindowClass::Closing
            ),
            Err(WorkError::Conflict)
        ));
        assert_eq!(control.snapshot().root_responsibilities, 0);
        assert_eq!(control.snapshot().businesses, 0);
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        assert!(matches!(
            control.root_admission().try_begin_root_with_result(
                WorkRequest::new(WorkClass::Query),
                ResultWindowClass::Client
            ),
            Err(WorkError::Conflict)
        ));
        assert_eq!(control.snapshot().root_responsibilities, 0);
    }

    #[test]
    fn delegated_window_keeps_exact_child_and_one_position_until_last_alias_exit() {
        let (control, capacity) = control();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let child = root
            .owner
            .scope()
            .child(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let sibling = root
            .owner
            .scope()
            .child(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let child_scope = child.scope();
        let window = capacity
            .try_acquire(&root.owner.scope(), ResultWindowClass::Internal)
            .unwrap();
        let alias = window.retain_alias().for_child(&child_scope).unwrap();
        assert!(alias.is_for_scope(&child_scope));
        assert!(!alias.is_for_scope(&root.owner.scope()));
        assert!(alias.for_child(&sibling.scope()).is_err());
        let (foreign, _) = self::control();
        let other = foreign
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        assert!(alias.for_child(&other.owner.scope()).is_err());
        assert_eq!(
            capacity.snapshot().held_positions[ResultWindowClass::Internal.index()],
            1
        );
        child.complete();
        assert!(
            child_scope
                .inner
                .state
                .lock()
                .unwrap()
                .nodes
                .contains_key(&child_scope.id)
        );
        drop(window);
        let late = alias.clone();
        drop(alias);
        assert_eq!(
            capacity.snapshot().held_positions[ResultWindowClass::Internal.index()],
            1
        );
        drop(late);
        assert!(
            !child_scope
                .inner
                .state
                .lock()
                .unwrap()
                .nodes
                .contains_key(&child_scope.id)
        );
        assert_eq!(
            capacity.snapshot().held_positions[ResultWindowClass::Internal.index()],
            0
        );
        sibling.complete();
        root.owner.complete();
        root.business.release();
        other.owner.complete();
        other.business.release();
    }

    #[test]
    fn alias_rejects_same_numeric_scope_from_a_different_host() {
        let (first, capacity) = control();
        let (second, _) = control();
        let a = first
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let b = second
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        assert_eq!(a.owner.scope().id(), b.owner.scope().id());
        let window = capacity
            .try_acquire(&a.owner.scope(), ResultWindowClass::Client)
            .unwrap();
        let alias = window.retain_alias();
        assert!(alias.is_for_scope(&a.owner.scope()));
        assert!(!alias.is_for_scope(&b.owner.scope()));
        assert!(alias.is_for_root_scope(&a.owner.scope()));
        assert!(!alias.is_for_root_scope(&b.owner.scope()));
    }

    #[test]
    fn root_drain_keeps_existing_windows_until_cancellation_or_execution_close() {
        for hard_close in [false, true] {
            let (control, capacity) = control();
            let root = control
                .try_begin_root(WorkRequest::new(WorkClass::Query))
                .unwrap();
            let scope = root.owner.scope();
            control.close_root_admission();
            control.close_root_admission();
            assert!(control.snapshot().admission_closed);
            assert_eq!(control.snapshot().serving, crate::ServingState::Closed);
            assert_eq!(control.mark_ready(), Err(WorkError::Closed));
            assert!(matches!(
                control.try_begin_root(WorkRequest::new(WorkClass::Management)),
                Err(WorkError::Closed)
            ));
            assert!(matches!(
                control.begin_warehouse_root(WorkRequest::new(WorkClass::Query)),
                Err(WorkError::Closed)
            ));
            let original = capacity
                .try_acquire(&scope, ResultWindowClass::Internal)
                .unwrap();
            let alias = original.retain_alias();
            drop(original);
            assert_eq!(capacity.snapshot().held_positions, [0, 0, 1, 0]);
            if hard_close {
                control.close_admission();
                assert!(matches!(
                    capacity.try_acquire(&scope, ResultWindowClass::Local),
                    Err(WorkError::Closed)
                ));
            } else {
                root.owner
                    .cancel(crate::CancellationReason::FrontendDrainDeadlineExceeded);
                assert!(matches!(
                    capacity.try_acquire(&scope, ResultWindowClass::Local),
                    Err(WorkError::Cancelled(
                        crate::CancellationReason::FrontendDrainDeadlineExceeded
                    ))
                ));
            }
            assert_eq!(capacity.snapshot().held_positions, [0, 0, 1, 0]);
            root.owner.complete_after_terminal_cancel_settled();
            root.business.release();
            assert_eq!(control.snapshot().root_responsibilities, 1);
            drop(alias);
            assert_eq!(capacity.snapshot().held_positions, [0; 4]);
            assert_eq!(control.snapshot().root_responsibilities, 0);
            assert_eq!(
                control
                    .snapshot()
                    .root_lifecycle
                    .completed_after_admission_closed
                    .query,
                1
            );
        }
    }

    #[test]
    fn closed_admission_preserves_only_bounded_existing_cancel_delivery() {
        let (control, capacity) = control();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let scope = root.owner.scope();
        control.close_admission();
        assert!(matches!(
            control.try_begin_root(WorkRequest::new(WorkClass::Management)),
            Err(WorkError::Closed)
        ));
        // Internal also covers scalar result delivery. None of the ordinary
        // classes can admit fresh backing after the root gate has closed.
        for class in [
            ResultWindowClass::Client,
            ResultWindowClass::Local,
            ResultWindowClass::Internal,
        ] {
            assert!(matches!(
                capacity.try_acquire(&scope, class),
                Err(WorkError::Closed)
            ));
        }
        assert!(matches!(
            capacity.try_acquire(&scope, ResultWindowClass::Closing),
            Err(WorkError::Conflict)
        ));
        assert!(matches!(
            capacity.try_acquire_closing(&scope, ResultClosingCut::AcceptedCancellation),
            Err(WorkError::Conflict)
        ));
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        assert_eq!(
            control.cancel_active_roots(crate::CancellationReason::FrontendDrainDeadlineExceeded),
            1
        );
        let closing = capacity
            .try_acquire_closing(&scope, ResultClosingCut::AcceptedCancellation)
            .unwrap();
        assert_eq!(closing.class(), ResultWindowClass::Closing);
        assert_eq!(
            closing.closing_cut(),
            Some(ResultClosingCut::AcceptedCancellation)
        );
        assert!(closing.is_for_scope(&scope));
        assert!(
            closing
                .check_backing_total(closing.all_objects_bytes())
                .is_ok()
        );
        assert!(matches!(
            closing.check_backing_total(closing.all_objects_bytes() + 1),
            Err(WorkError::Capacity("result backing envelope"))
        ));
        assert!(matches!(
            capacity.try_acquire_closing(&scope, ResultClosingCut::AcceptedCancellation),
            Err(WorkError::Capacity("complete result window"))
        ));
        assert_eq!(capacity.snapshot().held_positions, [0, 0, 0, 1]);

        let alias = closing.retain_alias();
        root.owner.complete_after_terminal_cancel_settled();
        root.business.release();
        drop(closing);
        assert_eq!(control.snapshot().businesses, 0);
        assert_eq!(control.snapshot().root_responsibilities, 1);
        assert_eq!(capacity.snapshot().held_positions, [0, 0, 0, 1]);
        assert!(matches!(
            capacity.try_acquire_closing(&scope, ResultClosingCut::AcceptedCancellation),
            Err(WorkError::Capacity("complete result window"))
        ));
        drop(alias);
        assert_eq!(control.snapshot().root_responsibilities, 0);
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        assert!(matches!(
            capacity.try_acquire_closing(&scope, ResultClosingCut::AcceptedCancellation),
            Err(WorkError::Released)
        ));
    }

    #[test]
    fn closed_admission_originating_failure_delivery_preserves_no_cancel_fact() {
        let (control, capacity) = control();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let scope = root.owner.scope();
        control.close_admission();
        let closing = capacity
            .try_acquire_closing(&scope, ResultClosingCut::OriginatingFailure)
            .unwrap();
        assert_eq!(
            closing.closing_cut(),
            Some(ResultClosingCut::OriginatingFailure)
        );
        assert!(scope.cancellation().unwrap().reason().is_none());
        assert!(matches!(
            capacity.try_acquire_closing(&scope, ResultClosingCut::AcceptedCancellation),
            Err(WorkError::Conflict)
        ));
        assert!(matches!(
            capacity.try_acquire_closing(&scope, ResultClosingCut::OriginatingFailure),
            Err(WorkError::Capacity("complete result window"))
        ));
        root.owner.complete();
        root.business.release();
        assert_eq!(control.snapshot().root_responsibilities, 1);
        drop(closing);
        assert_eq!(control.snapshot().root_responsibilities, 0);
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        assert!(matches!(
            capacity.try_acquire_closing(&scope, ResultClosingCut::OriginatingFailure),
            Err(WorkError::Released)
        ));
    }

    #[test]
    fn closed_admission_closing_refuses_foreign_numeric_scope_identity() {
        let (first, capacity) = control();
        let (second, _) = control();
        let own = first
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let foreign = second
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        assert_eq!(own.owner.scope().id(), foreign.owner.scope().id());
        first.close_admission();
        for cut in [
            ResultClosingCut::AcceptedCancellation,
            ResultClosingCut::OriginatingFailure,
        ] {
            assert!(matches!(
                capacity.try_acquire_closing(&foreign.owner.scope(), cut),
                Err(WorkError::ForeignAuthority)
            ));
        }
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        own.owner.complete();
        own.business.release();
        foreign.owner.complete();
        foreign.business.release();
        assert_eq!(first.snapshot().root_responsibilities, 0);
        assert_eq!(second.snapshot().root_responsibilities, 0);
    }

    #[test]
    fn position_and_scope_survive_timeout_and_late_alias_exit() {
        let (control, capacity) = control();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let scope = root.owner.scope().clone();
        let window = capacity
            .try_acquire(&scope, ResultWindowClass::Client)
            .unwrap();
        assert!(window.check_backing_total(8 * 1024 * 1024 + 1).is_err());
        let alias = window.retain_alias();
        drop(window);
        assert!(
            capacity
                .try_acquire(&scope, ResultWindowClass::Client)
                .is_err()
        );
        // A separate protocol closing position does not borrow the ordinary
        // window; its release cannot settle the fetch/transport alias.
        assert!(
            capacity
                .try_acquire(&scope, ResultWindowClass::Closing)
                .is_err()
        );
        assert!(
            capacity
                .try_acquire_closing(&scope, ResultClosingCut::AcceptedCancellation)
                .is_err()
        );
        drop(root);
        drop(
            capacity
                .try_acquire_closing(&scope, ResultClosingCut::AcceptedCancellation)
                .unwrap(),
        );
        assert_eq!(capacity.snapshot().held_positions, [1, 0, 0, 0]);
        assert!(
            scope
                .inner
                .state
                .lock()
                .unwrap()
                .nodes
                .contains_key(&scope.id)
        );
        drop(alias);
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    }
    /// Two query permits but one Client window, so the window decides.
    fn window_bound_control() -> (WorkloadControl, ResultCapacityHandle) {
        let control = WorkloadControl::try_new_counted(WorkloadConfig {
            query_concurrency_limit: 2,
            ..WorkloadConfig::default()
        })
        .unwrap()
        .owner;
        let handle = control
            .configure_result_capacity(ResultCapacityConfig {
                client_compute_positions: 2,
                client_short_tail_positions: 0,
                supported_cancel_burst: 0,
                sustained_cancels_per_second: 0,
                positions: [2, 1, 1, 1],
                ..ResultCapacityConfig::V1
            })
            .unwrap();
        assert!(matches!(control.resources(), Err(WorkError::NotReady)));
        assert_eq!(control.snapshot().resource_limit_bytes, None);
        control.mark_ready().unwrap();
        (control, handle)
    }
    fn admitted_queries(control: &WorkloadControl) -> usize {
        control.inner.state.lock().unwrap().admitted_queries
    }
    async fn pending<F: std::future::Future + Unpin>(future: &mut F) -> bool {
        tokio::time::timeout(std::time::Duration::from_millis(20), future)
            .await
            .is_err()
    }

    #[tokio::test]
    async fn permit_and_window_are_one_dequeue_transaction() {
        let (control, capacity) = window_bound_control();
        let first = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let second = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let other = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        // Another owner already holds one of the two Client windows.
        let held = capacity
            .try_acquire(&other.owner.scope(), ResultWindowClass::Client)
            .unwrap();
        let (permit, window) = first
            .owner
            .scope()
            .admit_query_with_result(ResultWindowClass::Client)
            .unwrap()
            .await
            .unwrap();
        assert_eq!(window.class(), ResultWindowClass::Client);
        assert!(window.is_for_scope(&first.owner.scope()));
        assert_eq!(capacity.snapshot().held_positions, [2, 0, 0, 0]);
        assert_eq!(admitted_queries(&control), 1);
        // A permit is free but no Client window is: the second query waits
        // without taking its permit.
        let mut waiting = second
            .owner
            .scope()
            .admit_query_with_result(ResultWindowClass::Client)
            .unwrap();
        assert!(pending(&mut waiting).await);
        assert_eq!(admitted_queries(&control), 1);
        // A query of another class is not blocked behind it.
        let (local_permit, local) = other
            .owner
            .scope()
            .admit_query_with_result(ResultWindowClass::Local)
            .unwrap()
            .await
            .unwrap();
        assert_eq!(admitted_queries(&control), 2);
        drop((local_permit, local));
        // Returning the permit alone does not admit the waiter; the window
        // it needs must exit too.
        drop(permit);
        assert!(pending(&mut waiting).await);
        drop(window);
        let (second_permit, second_window) = waiting.await.unwrap();
        assert!(second_window.is_for_scope(&second.owner.scope()));
        assert_eq!(capacity.snapshot().held_positions, [2, 0, 0, 0]);
        drop((second_permit, second_window, held));
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        assert_eq!(admitted_queries(&control), 0);
    }

    #[tokio::test]
    async fn abandoned_or_cancelled_admission_returns_both_parts() {
        let (control, capacity) = window_bound_control();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        // Granted by dispatch but never received: dropping returns both.
        let admission = root
            .owner
            .scope()
            .admit_query_with_result(ResultWindowClass::Client)
            .unwrap();
        assert_eq!(capacity.snapshot().held_positions, [1, 0, 0, 0]);
        assert_eq!(admitted_queries(&control), 1);
        drop(admission);
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        assert_eq!(admitted_queries(&control), 0);
        // Cancelled while granted but unreceived.
        let admission = root
            .owner
            .scope()
            .admit_query_with_result(ResultWindowClass::Internal)
            .unwrap();
        assert_eq!(capacity.snapshot().held_positions, [0, 0, 1, 0]);
        root.owner.cancel(crate::CancellationReason::Requested);
        assert!(matches!(admission.await, Err(WorkError::Cancelled(_))));
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        assert_eq!(admitted_queries(&control), 0);
    }

    #[tokio::test]
    async fn result_admission_requires_profile_and_an_ordinary_class() {
        let (control, _) = window_bound_control();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        assert!(matches!(
            root.owner
                .scope()
                .admit_query_with_result(ResultWindowClass::Closing),
            Err(WorkError::Conflict)
        ));
        let pending = root
            .owner
            .scope()
            .admit_query_with_result(ResultWindowClass::Client)
            .unwrap();
        // One query admission per root: this one was already granted.
        assert!(matches!(
            root.owner.scope().admit_query(),
            Err(WorkError::AlreadyAdmitted)
        ));
        drop(pending);
        let unconfigured = WorkloadControl::try_new_counted(WorkloadConfig::default())
            .unwrap()
            .owner;
        unconfigured.mark_ready().unwrap();
        let root = unconfigured
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        assert!(matches!(
            root.owner.scope().result_capacity(),
            Err(WorkError::NotReady)
        ));
        assert!(matches!(
            root.owner
                .scope()
                .admit_query_with_result(ResultWindowClass::Client),
            Err(WorkError::NotReady)
        ));
    }

    #[test]
    fn large_position_configuration_only_charges_actual_holder_and_bytes() {
        let control = WorkloadControl::try_new_counted(WorkloadConfig::default())
            .unwrap()
            .owner;
        // Independent finite configuration, not a request to allocate this
        // many holders or prepay their hypothetical combined payload.
        let positions = 1_000_001;
        let budget = 1u64 << 50;
        let capacity = control
            .configure_result_capacity(ResultCapacityConfig {
                positions: [320, 16, positions, 64],
                all_objects_bytes: [8 << 20, 96 << 20, budget, 8 << 20],
                ..ResultCapacityConfig::V1
            })
            .unwrap();
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        control.mark_ready().unwrap();
        let (root, window) = control
            .root_admission()
            .try_begin_root_with_result(
                WorkRequest::new(WorkClass::Management),
                ResultWindowClass::Internal,
            )
            .unwrap();
        let alias = window.retain_alias();
        let receipt = alias.reserve_payload_bytes(64).unwrap();
        let backing = vec![0u8; 64];
        assert_eq!(receipt.bytes(), backing.capacity() as u64);
        assert_eq!(window.all_objects_bytes(), budget);
        assert_eq!(alias.retained_payload_bytes(), 64);
        assert_eq!(capacity.snapshot().held_positions, [0, 0, 1, 0]);
        assert!(matches!(
            alias.reserve_payload_bytes(budget),
            Err(WorkError::Capacity("retained result bytes"))
        ));
        assert_eq!(alias.retained_payload_bytes(), 64);
        drop(window);
        drop(backing);
        drop(receipt);
        assert_eq!(alias.retained_payload_bytes(), 0);
        assert_eq!(capacity.snapshot().held_positions, [0, 0, 1, 0]);
        drop(alias);
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        root.owner.complete();
        root.business.release();
    }

    #[test]
    fn capacity_is_explicit_startup_only_and_foreign_scopes_reject() {
        let (owner, capacity) = control();
        assert!(
            owner
                .configure_result_capacity(ResultCapacityConfig::V1)
                .is_err()
        );
        let (other, other_capacity) = control();
        let root = other
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        assert!(matches!(
            capacity.try_acquire(&root.owner.scope(), ResultWindowClass::Client),
            Err(WorkError::ForeignAuthority)
        ));
        let scoped_capacity = root.owner.scope().result_capacity().unwrap();
        let window = scoped_capacity
            .try_acquire(&root.owner.scope(), ResultWindowClass::Client)
            .unwrap();
        assert_eq!(other_capacity.snapshot().held_positions, [1, 0, 0, 0]);
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        drop(window);
        assert_eq!(other_capacity.snapshot().held_positions, [0; 4]);
        assert!(
            ResultCapacityConfig {
                all_objects_bytes: [0; 4],
                ..ResultCapacityConfig::V1
            }
            .validate()
            .is_err()
        );
    }
}

#[cfg(test)]
mod internal_root_tests;
