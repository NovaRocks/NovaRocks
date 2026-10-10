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
use std::sync::Arc;

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
        let mut total = 0u64;
        for (positions, bytes) in self.positions.into_iter().zip(self.all_objects_bytes) {
            if positions == 0 || positions > 1_000_000 || bytes == 0 {
                return Err(WorkError::InvalidConfig("result owner capacity"));
            }
            total = total
                .checked_add(
                    bytes
                        .checked_mul(positions as u64)
                        .ok_or(WorkError::ArithmeticOverflow)?,
                )
                .ok_or(WorkError::ArithmeticOverflow)?;
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
                || state.closed
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
            if cut == ResultClosingCut::AcceptedCancellation && node.cancellation.reason().is_none()
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
        node.check()?;
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
        }),
        closing_cut: None,
    }
}

struct WindowHolder {
    scope: WorkScope,
    class: ResultWindowClass,
    all_objects_bytes: u64,
}
impl Drop for WindowHolder {
    fn drop(&mut self) {
        self.scope.inner.update(|state| {
            uncount_window(state, self.scope.id, self.class);
            state.collect(self.scope.id);
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
            holder: Arc::clone(&self.holder),
            execution_scope: None,
        }
    }
}
#[derive(Clone)]
pub struct ResultWindowAlias {
    holder: Arc<WindowHolder>,
    execution_scope: Option<Arc<WindowExecutionScope>>,
}

// A delegated alias holds the exact child's responsibility through real exit.
// It consumes no second result position and cannot change the admitted class.
struct WindowExecutionScope {
    scope: WorkScope,
}
impl Drop for WindowExecutionScope {
    fn drop(&mut self) {
        self.scope.inner.update(|state| {
            let node = state
                .nodes
                .get_mut(&self.scope.id)
                .expect("delegated window alias retains its child scope");
            node.resource_holders -= 1;
            state.collect(self.scope.id);
        });
    }
}
impl ResultWindowAlias {
    /// Numeric scope identities are local to one host. Capacity must match
    /// both the exact scope and the runtime that admitted it.
    pub fn is_for_scope(&self, scope: &WorkScope) -> bool {
        self.execution_scope().id == scope.id
            && Arc::ptr_eq(&self.execution_scope().inner, &scope.inner)
    }
    /// Compare the actual physical allowance, independently of child attribution.
    pub fn shares_capacity_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.holder, &other.holder)
    }

    pub fn scope_id(&self) -> WorkId {
        self.execution_scope().id()
    }
    fn execution_scope(&self) -> &WorkScope {
        self.execution_scope
            .as_ref()
            .map_or(&self.holder.scope, |owner| &owner.scope)
    }

    /// Authorize one live direct child to use this already-admitted window.
    /// Exact parent/host identity is checked before recording its holder; a
    /// sibling, foreign host or completed scope cannot obtain this capability.
    pub fn for_child(&self, child: &WorkScope) -> Result<Self, WorkError> {
        let parent = self.execution_scope();
        if !Arc::ptr_eq(&parent.inner, &child.inner) {
            return Err(WorkError::ForeignAuthority);
        }
        if self.holder.class == ResultWindowClass::Closing {
            return Err(WorkError::Conflict);
        }
        child.inner.update(|state| {
            state
                .nodes
                .get(&parent.id)
                .ok_or(WorkError::Released)?
                .check()?;
            let node = state.nodes.get_mut(&child.id).ok_or(WorkError::Released)?;
            node.check()?;
            if node.parent != Some(parent.id) {
                return Err(WorkError::Conflict);
            }
            node.resource_holders = node
                .resource_holders
                .checked_add(1)
                .ok_or(WorkError::ArithmeticOverflow)?;
            Ok(Self {
                holder: Arc::clone(&self.holder),
                execution_scope: Some(Arc::new(WindowExecutionScope {
                    scope: child.clone(),
                })),
            })
        })
    }

    pub fn class(&self) -> ResultWindowClass {
        self.holder.class
    }
    pub fn check_backing_total(&self, simultaneously_live_bytes: u64) -> Result<(), WorkError> {
        if simultaneously_live_bytes > self.holder.all_objects_bytes {
            Err(WorkError::Capacity("result backing envelope"))
        } else {
            Ok(())
        }
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
                all_objects_bytes: [u64::MAX; 4],
                ..ResultCapacityConfig::V1
            }
            .validate()
            .is_err()
        );
    }
}
