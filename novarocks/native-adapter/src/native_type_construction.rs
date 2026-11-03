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

//! BE preparation observes its existing query tracker before type allocation.
//! Returned types retain only this construction's responsibility, not the task,
//! query context, memory authority or stop controller. These logical requests
//! are not allocator facts and do not create a second MEM accounting authority.

use std::{
    alloc::Layout,
    fmt,
    sync::{Arc, Mutex},
};

use novarocks_execution::runtime::mem_tracker::MemTracker;
#[cfg(test)]
use novarocks_plan_codec::host_projection_v2::AdmissionRefusal;
use novarocks_spi::connector::ConnectorStopView;
use novarocks_type_contract::{
    CompileControlError, CompilePhase, PureCompileControl, ValueTypeStorageHold,
    ValueTypeStorageLifetime,
    owned_resources::layout::{LayoutResourceError, arc_layout},
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NativeTypeConstructionError {
    Control(CompileControlError),
    Capacity(novarocks_execution::runtime::mem_tracker::MemTrackerLimitExceeded),
    Counter(NativeTypeCounterFailure),
    Allocation(novarocks_execution_contract::failure::SharedFailureCause),
    Resource(LayoutResourceError),
    Funding(Box<crate::backend_task_execution::TypeMaterializationRefusal>),
}
impl From<LayoutResourceError> for NativeTypeConstructionError {
    fn from(error: LayoutResourceError) -> Self {
        Self::Resource(error)
    }
}
impl From<CompileControlError> for NativeTypeConstructionError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl fmt::Display for NativeTypeConstructionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(error) => error.fmt(f),
            Self::Capacity(error) => write!(f, "native type construction capacity: {error}"),
            Self::Counter(error) => error.fmt(f),
            Self::Allocation(error) => error.fmt(f),
            Self::Resource(error) => write!(f, "native type construction layout: {error:?}"),
            Self::Funding(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for NativeTypeConstructionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Funding(cause) => Some(cause.as_ref()),
            Self::Capacity(source) => Some(source),
            Self::Control(source) => Some(source),
            Self::Resource(source) => Some(source),
            Self::Counter(source) => Some(source),
            Self::Allocation(source) => Some(source),
        }
    }
}

impl novarocks_execution_contract::failure::FailureProjection for NativeTypeConstructionError {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        match self {
            Self::Control(cause) => cause.failure_fact(),
            Self::Capacity(cause) => cause.failure_fact(),
            Self::Counter(cause) => cause.failure_fact(),
            Self::Allocation(cause) => cause.failure_fact(),
            Self::Resource(cause) => cause.failure_fact(),
            Self::Funding(cause) => cause.failure_fact(),
        }
    }
}

/// Attribution counter width is opaque accounting, not an Arrow format or
/// permission to split a value. The original SDK conversion remains available.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NativeTypeCounterFailure {
    Width {
        requested: usize,
        source: std::num::TryFromIntError,
    },
    Total {
        committed: i64,
        requested: i64,
    },
}
impl std::fmt::Display for NativeTypeCounterFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Width { requested, .. } => write!(
                f,
                "native type construction request exceeds its signed attribution counter: {requested}"
            ),
            Self::Total {
                committed,
                requested,
            } => write!(
                f,
                "native type construction retained counter overflowed: {committed} + {requested}"
            ),
        }
    }
}
impl std::error::Error for NativeTypeCounterFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Width { source, .. } => Some(source),
            Self::Total { .. } => None,
        }
    }
}
impl novarocks_execution_contract::failure::FailureProjection for NativeTypeCounterFailure {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use novarocks_execution_contract::failure::*;
        match self {
            Self::Width { requested, .. } => novarocks_execution::runtime::mem_tracker::MemoryAccountingFailure::BytesOutOfRange { requested: *requested }.failure_fact(),
            Self::Total { committed, requested } => FailureFact::new(FailureClass::Resource, FailureReason::new(domain::NATIVE, std::num::NonZeroU16::new(4).unwrap()))
                .with_resource(ResourceFacts {
                    dimension: ResourceDimension::Bytes, request_scope: Some(ResourceScope::Request), constraint_scope: None,
                    requested: u64::try_from(*requested).ok(), required_growth: None, limit: Some(i64::MAX as u64), live: None,
                    committed: u64::try_from(*committed).ok(),
                }),
        }
    }
}

struct TypeReservation {
    tracker: Arc<MemTracker>,
    requested: Mutex<i64>,
}
impl ValueTypeStorageHold for TypeReservation {}
impl Drop for TypeReservation {
    fn drop(&mut self) {
        self.tracker.release(
            *self
                .requested
                .get_mut()
                .expect("type construction request lock"),
        );
    }
}
impl TypeReservation {
    fn pure_allocation_layout() -> Result<Layout, LayoutResourceError> {
        arc_layout(Layout::new::<Self>())
    }
    fn allocation_layout() -> Result<Layout, NativeTypeConstructionError> {
        Self::pure_allocation_layout().map_err(Into::into)
    }
    fn try_new(
        tracker: Arc<MemTracker>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<Self>, NativeTypeConstructionError> {
        Self::try_new_observed::<std::convert::Infallible>(tracker, control, None).map_err(
            |error| match error {
                novarocks_type_contract::ValueSchemaScopeBeginFailure::Creator(error) => error,
                novarocks_type_contract::ValueSchemaScopeBeginFailure::Observer(never) => {
                    match never {}
                }
            },
        )
    }
    fn try_new_observed<O>(
        tracker: Arc<MemTracker>,
        control: &dyn PureCompileControl,
        observe: Option<&mut dyn FnMut(Layout) -> Result<(), O>>,
    ) -> Result<
        Arc<Self>,
        novarocks_type_contract::ValueSchemaScopeBeginFailure<O, NativeTypeConstructionError>,
    > {
        use novarocks_type_contract::ValueSchemaScopeBeginFailure as Failure;
        control
            .checkpoint(CompilePhase::LowerProgram, 0)
            .map_err(NativeTypeConstructionError::Control)
            .map_err(Failure::Creator)?;
        let layout = Self::allocation_layout().map_err(Failure::Creator)?;
        if let Some(observe) = observe {
            observe(layout).map_err(Failure::Observer)?;
        }
        let bytes = i64::try_from(layout.size())
            .map_err(|source| {
                NativeTypeConstructionError::Counter(NativeTypeCounterFailure::Width {
                    requested: layout.size(),
                    source,
                })
            })
            .map_err(Failure::Creator)?;
        if let Err(error) = tracker.consume_and_check_limit_typed(bytes) {
            tracker.release(bytes);
            return Err(Failure::Creator(NativeTypeConstructionError::Capacity(
                error,
            )));
        }
        Ok(Arc::new(Self {
            tracker,
            requested: Mutex::new(bytes),
        }))
    }

    fn request(&self, layout: Layout) -> Result<(), NativeTypeConstructionError> {
        self.request_bytes(layout.size())
    }

    fn request_bytes(&self, requested_bytes: usize) -> Result<(), NativeTypeConstructionError> {
        if requested_bytes == 0 {
            return Ok(());
        }
        let bytes = i64::try_from(requested_bytes).map_err(|source| {
            NativeTypeConstructionError::Counter(NativeTypeCounterFailure::Width {
                requested: requested_bytes,
                source,
            })
        })?;
        let mut requested = self
            .requested
            .lock()
            .expect("type construction request lock");
        let total = requested.checked_add(bytes).ok_or_else(|| {
            NativeTypeConstructionError::Counter(NativeTypeCounterFailure::Total {
                committed: *requested,
                requested: bytes,
            })
        })?;
        if let Err(error) = self.tracker.consume_and_check_limit_typed(bytes) {
            // The tracker records a failed prospective request until balanced.
            self.tracker.release(bytes);
            return Err(NativeTypeConstructionError::Capacity(error));
        }
        *requested = total;
        Ok(())
    }
}

/// Mutable decoder resources remain separate from immutable native facts.
/// The caller supplies the established BE query's actual tracker and stop view.
pub(crate) struct NativeTypeConstruction<'control> {
    reservation: Arc<TypeReservation>,
    control: ConstructionControl<'control>,
}
enum ConstructionControl<'control> {
    Task {
        stop: ConnectorStopView,
        preparation_stop: novarocks_worker::TaskPreparationView,
    },
    Borrowed(&'control dyn PureCompileControl),
}
impl PureCompileControl for ConstructionControl<'_> {
    fn checkpoint(&self, phase: CompilePhase, node: u32) -> Result<(), CompileControlError> {
        match self {
            Self::Task {
                stop,
                preparation_stop,
            } => {
                check_preparation_stop(preparation_stop)?;
                if stop.is_stopped() {
                    Err(CompileControlError::Cancelled)
                } else {
                    Ok(())
                }
            }
            Self::Borrowed(control) => control.checkpoint(phase, node),
        }
    }
}
impl<'control> NativeTypeConstruction<'control> {
    pub(crate) fn try_new(
        tracker: Arc<MemTracker>,
        stop: ConnectorStopView,
        preparation_stop: novarocks_worker::TaskPreparationView,
    ) -> Result<Self, NativeTypeConstructionError> {
        Self::try_from_control(
            tracker,
            ConstructionControl::Task {
                stop,
                preparation_stop,
            },
        )
    }
    pub(crate) fn try_with_control(
        tracker: Arc<MemTracker>,
        control: &'control dyn PureCompileControl,
    ) -> Result<Self, NativeTypeConstructionError> {
        Self::try_from_control(tracker, ConstructionControl::Borrowed(control))
    }
    fn try_from_control(
        tracker: Arc<MemTracker>,
        control: ConstructionControl<'control>,
    ) -> Result<Self, NativeTypeConstructionError> {
        let reservation = TypeReservation::try_new(tracker, &control)?;
        Ok(Self {
            reservation,
            control,
        })
    }
    /// Begin one actual construction lifetime on the same established tracker.
    /// The scope borrows only the original control. Its storage holds neither
    /// this parent's reservation nor the stop controller. Operation-wide
    /// checkpoints and structural/count models remain owned by the caller.
    pub(crate) fn begin_scope(&self) -> Result<NativeTypeScope<'_>, NativeTypeConstructionError> {
        Ok(NativeTypeScope {
            reservation: TypeReservation::try_new(self.reservation.tracker.clone(), &self.control)?,
            control: &self.control,
        })
    }

    fn begin_scope_observed<O>(
        &self,
        observe: &mut dyn FnMut(Layout) -> Result<(), O>,
    ) -> Result<
        NativeTypeScope<'_>,
        novarocks_type_contract::ValueSchemaScopeBeginFailure<O, NativeTypeConstructionError>,
    > {
        Ok(NativeTypeScope {
            reservation: TypeReservation::try_new_observed(
                self.reservation.tracker.clone(),
                &self.control,
                Some(observe),
            )?,
            control: &self.control,
        })
    }

    pub(crate) fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
        ValueTypeStorageLifetime::held(self.reservation.clone())
    }
    pub(crate) fn request_layout(&self, layout: Layout) -> Result<(), NativeTypeConstructionError> {
        self.checkpoint(CompilePhase::LowerProgram, 0)?;
        self.reservation.request(layout)
    }

    /// A locked-library construction reports its opaque byte upper bound
    /// separately from public Layout requests. This observes the original
    /// tracker without presenting the bound as an actual allocation geometry.
    pub(crate) fn request_opaque_upper_bound(
        &self,
        bytes: usize,
    ) -> Result<(), NativeTypeConstructionError> {
        self.checkpoint(CompilePhase::LowerProgram, 0)?;
        self.reservation.request_bytes(bytes)
    }
    #[cfg(test)]
    pub(crate) fn request(
        &self,
        layout: Layout,
    ) -> Result<(), AdmissionRefusal<NativeTypeConstructionError>> {
        self.request_layout(layout).map_err(|cause| match cause {
            NativeTypeConstructionError::Control(cause) => AdmissionRefusal::Control(cause),
            cause => AdmissionRefusal::Host(cause),
        })
    }
}
/// One BE entity or temporary operation's actual creation responsibility.
/// No per-scope quota, work counter, source invoice or semantic authority is
/// created here. Dropping the scope releases its requests unless a published
/// entity still owns this scope's storage lifetime.
pub(crate) struct NativeTypeScope<'control> {
    reservation: Arc<TypeReservation>,
    control: &'control dyn PureCompileControl,
}
impl NativeTypeScope<'_> {
    pub(crate) fn reservation_allocation_layout() -> Result<Layout, NativeTypeConstructionError> {
        TypeReservation::allocation_layout()
    }
    pub(crate) fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
        ValueTypeStorageLifetime::held(self.reservation.clone())
    }
    pub(crate) fn request_layout(&self, layout: Layout) -> Result<(), NativeTypeConstructionError> {
        self.checkpoint(CompilePhase::LowerProgram, 0)?;
        self.reservation.request(layout)
    }
    pub(crate) fn request_opaque_upper_bound(
        &self,
        bytes: usize,
    ) -> Result<(), NativeTypeConstructionError> {
        self.checkpoint(CompilePhase::LowerProgram, 0)?;
        self.reservation.request_bytes(bytes)
    }
    #[cfg(test)]
    pub(crate) fn request(
        &self,
        layout: Layout,
    ) -> Result<(), AdmissionRefusal<NativeTypeConstructionError>> {
        self.request_layout(layout).map_err(|cause| match cause {
            NativeTypeConstructionError::Control(cause) => AdmissionRefusal::Control(cause),
            cause => AdmissionRefusal::Host(cause),
        })
    }
}
impl PureCompileControl for NativeTypeScope<'_> {
    fn checkpoint(&self, phase: CompilePhase, node: u32) -> Result<(), CompileControlError> {
        self.control.checkpoint(phase, node)
    }
}

impl novarocks_type_contract::ValueSchemaCreationScope<NativeTypeConstructionError>
    for NativeTypeScope<'_>
{
    fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
        NativeTypeScope::storage_lifetime(self)
    }
    fn request_layout(&mut self, layout: Layout) -> Result<(), NativeTypeConstructionError> {
        NativeTypeScope::request_layout(self, layout)
    }
    fn observe_opaque_upper_bound(
        &mut self,
        bytes: usize,
    ) -> Result<(), NativeTypeConstructionError> {
        self.request_opaque_upper_bound(bytes)
    }
}

/// This operation's actual construction requests, including each scope's small
/// reservation holder. The counter is provenance for the next source observer,
/// not retained precision, a resettable limit or capacity authority.
pub(crate) struct NativeTypeFactory<'borrow, 'control> {
    creation: &'borrow NativeTypeConstruction<'control>,
    request_upper: std::cell::Cell<usize>,
}
impl<'borrow, 'control> NativeTypeFactory<'borrow, 'control> {
    pub(crate) fn new(creation: &'borrow NativeTypeConstruction<'control>) -> Self {
        Self {
            creation,
            request_upper: std::cell::Cell::new(0),
        }
    }
    #[cfg(test)]
    pub(crate) fn construction_request_bytes_upper_bound(&self) -> usize {
        self.request_upper.get()
    }
}
pub(crate) struct NativeFactoryScope<'borrow> {
    scope: NativeTypeScope<'borrow>,
    request_upper: &'borrow std::cell::Cell<usize>,
}
impl NativeFactoryScope<'_> {
    fn next_upper(&self, bytes: usize) -> Result<usize, NativeTypeConstructionError> {
        self.request_upper
            .get()
            .checked_add(bytes)
            .ok_or(NativeTypeConstructionError::Resource(
                LayoutResourceError::Arithmetic,
            ))
    }
}
impl novarocks_type_contract::ValueSchemaCreationScope<NativeTypeConstructionError>
    for NativeFactoryScope<'_>
{
    fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
        self.scope.storage_lifetime()
    }
    fn request_layout(&mut self, layout: Layout) -> Result<(), NativeTypeConstructionError> {
        let next = self.next_upper(layout.size())?;
        self.scope.request_layout(layout)?;
        self.request_upper.set(next);
        Ok(())
    }
    fn observe_opaque_upper_bound(
        &mut self,
        bytes: usize,
    ) -> Result<(), NativeTypeConstructionError> {
        let next = self.next_upper(bytes)?;
        self.scope.request_opaque_upper_bound(bytes)?;
        self.request_upper.set(next);
        Ok(())
    }
}
impl novarocks_type_contract::ValueSchemaCreationFactory<NativeTypeConstructionError>
    for NativeTypeFactory<'_, '_>
{
    type Scope<'a>
        = NativeFactoryScope<'a>
    where
        Self: 'a;
    fn visit_scope_allocation_layouts<E>(
        &self,
        visit: &mut dyn FnMut(Layout) -> Result<(), E>,
    ) -> Result<(), E>
    where
        E: From<LayoutResourceError>,
    {
        visit(TypeReservation::pure_allocation_layout()?)
    }
    fn begin_scope(&self) -> Result<Self::Scope<'_>, NativeTypeConstructionError> {
        let holder = NativeTypeScope::reservation_allocation_layout()?;
        let next = self.request_upper.get().checked_add(holder.size()).ok_or(
            NativeTypeConstructionError::Resource(LayoutResourceError::Arithmetic),
        )?;
        let scope = self.creation.begin_scope()?;
        self.request_upper.set(next);
        Ok(NativeFactoryScope {
            scope,
            request_upper: &self.request_upper,
        })
    }
    fn begin_scope_observed<O>(
        &self,
        observe: &mut dyn FnMut(Layout) -> Result<(), O>,
    ) -> Result<
        Self::Scope<'_>,
        novarocks_type_contract::ValueSchemaScopeBeginFailure<O, NativeTypeConstructionError>,
    > {
        use novarocks_type_contract::ValueSchemaScopeBeginFailure as Failure;
        // The same original controller precedes arithmetic/model work; its
        // verdict is not replaced by a copied or newly invented stop domain.
        self.creation
            .checkpoint(CompilePhase::LowerProgram, 0)
            .map_err(NativeTypeConstructionError::Control)
            .map_err(Failure::Creator)?;
        let holder = NativeTypeScope::reservation_allocation_layout().map_err(Failure::Creator)?;
        let next = self
            .request_upper
            .get()
            .checked_add(holder.size())
            .ok_or(Failure::Creator(NativeTypeConstructionError::Resource(
                LayoutResourceError::Arithmetic,
            )))?;
        let scope = self.creation.begin_scope_observed(observe)?;
        self.request_upper.set(next);
        Ok(NativeFactoryScope {
            scope,
            request_upper: &self.request_upper,
        })
    }
}

fn check_preparation_stop(
    stop: &novarocks_worker::TaskPreparationView,
) -> Result<(), CompileControlError> {
    stop.borrow_control().checkpoint().map_err(|error| {
        CompileControlError::Source(
            novarocks_execution::runtime::task::SharedDomainFailure::new(error),
        )
    })
}
impl PureCompileControl for NativeTypeConstruction<'_> {
    fn checkpoint(&self, phase: CompilePhase, node: u32) -> Result<(), CompileControlError> {
        self.control.checkpoint(phase, node)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_spi::connector::ConnectorStopOwner;
    use novarocks_type_contract::{ValueKind, ValueTypeRef};

    #[test]
    fn independent_type_keeps_actual_creation_requests_until_its_last_reference() {
        let tracker = MemTracker::new_root("native type creation test");
        let stop = ConnectorStopOwner::new();
        let control = crate::backend_test_support::PureConnectorCompileControl(stop.view());
        let creator = NativeTypeConstruction::try_with_control(tracker.clone(), &control).unwrap();
        creator
            .request_layout(ValueTypeRef::storage_allocation_layout().unwrap())
            .unwrap();
        let value =
            ValueTypeRef::try_new(ValueKind::String, true, creator.storage_lifetime()).unwrap();
        let independent = value.clone();
        let held = tracker.current();
        assert!(held > 0);
        drop(value);
        drop(creator);
        drop(stop);
        assert_eq!(tracker.current(), held);
        drop(independent);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn original_stop_and_failed_capacity_preserve_prior_successful_requests() {
        let tracker = MemTracker::new_root("native type refusal test");
        let stop = ConnectorStopOwner::new();
        let control = crate::backend_test_support::PureConnectorCompileControl(stop.view());
        let creator = NativeTypeConstruction::try_with_control(tracker.clone(), &control).unwrap();
        let held = tracker.current();
        tracker.install_limit_once(held + 8).unwrap();
        assert!(matches!(
            creator.request(Layout::new::<[u8; 16]>()),
            Err(AdmissionRefusal::Host(
                NativeTypeConstructionError::Capacity(_)
            ))
        ));
        assert_eq!(tracker.current(), held);
        stop.request_stop();
        assert!(matches!(
            creator.request(Layout::new::<u8>()),
            Err(AdmissionRefusal::Control(CompileControlError::Cancelled))
        ));
        assert_eq!(tracker.current(), held);
        drop(creator);
        assert_eq!(tracker.current(), 0);
    }

    fn reservation_bytes() -> i64 {
        i64::try_from(arc_layout(Layout::new::<TypeReservation>()).unwrap().size()).unwrap()
    }

    struct ObservedControl {
        dropped: Arc<std::sync::atomic::AtomicUsize>,
        stopped: std::sync::atomic::AtomicBool,
        visits: Mutex<Vec<(CompilePhase, u32)>>,
    }
    impl PureCompileControl for ObservedControl {
        fn checkpoint(&self, phase: CompilePhase, node: u32) -> Result<(), CompileControlError> {
            self.visits.lock().unwrap().push((phase, node));
            if self.stopped.load(std::sync::atomic::Ordering::SeqCst) {
                Err(CompileControlError::DeadlineExceeded)
            } else {
                Ok(())
            }
        }
    }
    impl Drop for ObservedControl {
        fn drop(&mut self) {
            self.dropped
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }
    fn observed_control(dropped: Arc<std::sync::atomic::AtomicUsize>) -> ObservedControl {
        ObservedControl {
            dropped,
            stopped: std::sync::atomic::AtomicBool::new(false),
            visits: Mutex::new(Vec::new()),
        }
    }

    #[test]
    fn scoped_scratch_exits_without_retaining_its_parent_or_requests() {
        let tracker = MemTracker::new_root("native scratch scope");
        let stop = ConnectorStopOwner::new();
        let control = crate::backend_test_support::PureConnectorCompileControl(stop.view());
        let creator = NativeTypeConstruction::try_with_control(tracker.clone(), &control).unwrap();
        let parent = tracker.current();
        let scope = creator.begin_scope().unwrap();
        assert_eq!(tracker.current(), parent + reservation_bytes());
        scope
            .request_layout(Layout::array::<u32>(32).unwrap())
            .unwrap();
        scope.request_opaque_upper_bound(73).unwrap();
        assert_eq!(tracker.current(), parent + reservation_bytes() + 128 + 73);
        drop(scope);
        assert_eq!(tracker.current(), parent);
        drop(creator);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn scoped_late_leaf_holds_only_its_actual_entity_after_unused_and_control_exit() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let tracker = MemTracker::new_root("native late leaf scope");
        let dropped = Arc::new(AtomicUsize::new(0));
        let control = observed_control(dropped.clone());
        let creator = NativeTypeConstruction::try_with_control(tracker.clone(), &control).unwrap();
        creator.request_layout(Layout::new::<[u8; 23]>()).unwrap();
        let unused = creator.begin_scope().unwrap();
        unused.request_opaque_upper_bound(4096).unwrap();
        let leaf = creator.begin_scope().unwrap();
        let node = ValueTypeRef::storage_allocation_layout().unwrap();
        leaf.request_layout(node).unwrap();
        let value =
            ValueTypeRef::try_new(ValueKind::String, false, leaf.storage_lifetime()).unwrap();
        let late = value.clone();
        let own = reservation_bytes() + i64::try_from(node.size()).unwrap();
        drop(value);
        drop(unused);
        drop(leaf);
        drop(creator);
        drop(control);
        assert_eq!(
            dropped.load(Ordering::SeqCst),
            1,
            "leaf storage does not retain control"
        );
        assert_eq!(
            tracker.current(),
            own,
            "leaf does not retain parent or unused requests"
        );
        drop(late);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn scoped_first_tracker_refusal_preserves_original_diagnostic_and_rolls_back() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let tracker = MemTracker::new_root("native scope first refusal");
        let control = observed_control(Arc::new(AtomicUsize::new(0)));
        let creator = NativeTypeConstruction::try_with_control(tracker.clone(), &control).unwrap();
        let prior = tracker.current();
        let limit = prior + reservation_bytes() - 1;
        tracker.install_limit_once(limit).unwrap();
        let refusal = creator.begin_scope().err().unwrap();
        let NativeTypeConstructionError::Capacity(original) = refusal else {
            panic!("actual tracker cause was replaced");
        };
        assert_eq!(
            original.detail(),
            format!(
                "ResourceExhausted: memory limit exceeded for tracker native scope first refusal: current {} bytes, limit {limit} bytes",
                prior + reservation_bytes(),
            )
        );
        assert_eq!(tracker.current(), prior);
        assert_eq!(
            control.visits.lock().unwrap().len(),
            2,
            "no control footer after refusal"
        );
        control.stopped.store(true, Ordering::SeqCst);
        assert!(matches!(
            creator.begin_scope(),
            Err(NativeTypeConstructionError::Control(
                CompileControlError::DeadlineExceeded
            ))
        ));
        assert_eq!(
            tracker.current(),
            prior,
            "original stop precedes a new reservation request"
        );
        drop(creator);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn scopes_share_original_tracker_limit_and_checkpoint_domain() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let tracker = MemTracker::new_root("native scopes shared policy");
        let control = observed_control(Arc::new(AtomicUsize::new(0)));
        let creator = NativeTypeConstruction::try_with_control(tracker.clone(), &control).unwrap();
        let parent = tracker.current();
        let limit = parent + 2 * reservation_bytes() + 8;
        tracker.install_limit_once(limit).unwrap();
        let first = creator.begin_scope().unwrap();
        let second = creator.begin_scope().unwrap();
        first.request_layout(Layout::new::<[u8; 4]>()).unwrap();
        let prior = tracker.current();
        let before = control.visits.lock().unwrap().len();
        assert!(matches!(
            second.request(Layout::new::<[u8; 8]>()),
            Err(AdmissionRefusal::Host(
                NativeTypeConstructionError::Capacity(_)
            ))
        ));
        assert_eq!(tracker.current(), prior);
        assert_eq!(
            control.visits.lock().unwrap().len(),
            before + 1,
            "one original checkpoint, no refusal footer"
        );
        first.checkpoint(CompilePhase::Decode, 17).unwrap();
        second.checkpoint(CompilePhase::Encode, 19).unwrap();
        let visits = control.visits.lock().unwrap();
        assert_eq!(
            &visits[visits.len() - 2..],
            &[(CompilePhase::Decode, 17), (CompilePhase::Encode, 19)]
        );
        drop(visits);
        control.stopped.store(true, Ordering::SeqCst);
        assert!(matches!(
            first.request(Layout::new::<u8>()),
            Err(AdmissionRefusal::Control(
                CompileControlError::DeadlineExceeded
            ))
        ));
        assert!(matches!(
            second.request_opaque_upper_bound(1),
            Err(NativeTypeConstructionError::Control(
                CompileControlError::DeadlineExceeded
            ))
        ));
        assert_eq!(tracker.current(), prior);
        drop(first);
        drop(second);
        assert_eq!(tracker.current(), parent);
        drop(creator);
        assert_eq!(tracker.current(), 0);
    }
}
