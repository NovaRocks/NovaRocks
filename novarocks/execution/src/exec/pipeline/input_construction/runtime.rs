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

use super::{ConstructionFailure, InputConstructionRequestHost, InputConstructionWorkControl};
use crate::runtime::{mem_tracker::MemTracker, runtime_state::RuntimeErrorState};
use std::{
    alloc::Layout,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

/// Existing logical tracker adapter. Its finite ancestor chain contains no
/// query context or memory authority; these are observations, not physical facts.
pub struct TrackedInputConstructionHost {
    tracker: Arc<MemTracker>,
    _creation: Option<TrackedArcCharge>,
}
impl TrackedInputConstructionHost {
    pub fn new(tracker: Arc<MemTracker>) -> Self {
        Self {
            tracker,
            _creation: None,
        }
    }
    fn observe_bytes(&self, bytes: usize) -> Result<(), ConstructionFailure> {
        if bytes == 0 {
            return Ok(());
        }
        let bytes = i64::try_from(bytes).map_err(|_| {
            ConstructionFailure::Control(
                novarocks_type_contract::CompileControlError::ResourceExhausted,
            )
        })?;
        if let Err(error) = self.tracker.consume_and_check_limit_typed(bytes) {
            self.tracker.release(bytes);
            return Err(ConstructionFailure::Memory(error));
        }
        Ok(())
    }
}
impl InputConstructionRequestHost for TrackedInputConstructionHost {
    fn request(&self, layout: Layout) -> Result<(), ConstructionFailure> {
        self.observe_bytes(layout.size())
    }
    fn observe_opaque_bound(&self, bytes_upper: usize) -> Result<(), ConstructionFailure> {
        self.observe_bytes(bytes_upper)
    }
    fn release_requested_bytes(&self, bytes: usize) {
        if bytes != 0 {
            self.tracker
                .release(i64::try_from(bytes).expect("successful construction requests fit i64"));
        }
    }
}

/// The default control borrows the original kernel signal. A receiver control
/// instead owns its independent signal inline in this same tracked Arc.
pub struct RuntimeInputConstructionControl<S = Arc<RuntimeErrorState>> {
    error: S,
    _creation: Option<TrackedArcCharge>,
}
impl RuntimeInputConstructionControl {
    pub fn new(error: Arc<RuntimeErrorState>) -> Self {
        Self {
            error,
            _creation: None,
        }
    }
}

trait ConstructionErrorSignal: Send + Sync {
    fn checkpoint(&self) -> Result<(), ConstructionFailure>;
}
impl ConstructionErrorSignal for Arc<RuntimeErrorState> {
    fn checkpoint(&self) -> Result<(), ConstructionFailure> {
        match self.error() {
            Some(error) => Err(ConstructionFailure::Caller(Box::new(error))),
            None => Ok(()),
        }
    }
}

#[derive(Debug)]
enum ReceiverConstructionState {
    Open,
    Stopped(crate::runtime::fragment::ExecutionFailure),
    ClosedNeutral,
}
struct ReceiverConstructionSignal {
    state: Mutex<ReceiverConstructionState>,
    stopped: AtomicBool,
}
impl Default for ReceiverConstructionSignal {
    fn default() -> Self {
        Self {
            state: Mutex::new(ReceiverConstructionState::Open),
            stopped: AtomicBool::new(false),
        }
    }
}
impl ReceiverConstructionSignal {
    /// Rejection returns the exact source; no foreign destructor runs under
    /// the signal lock, including after its irreversible neutral closure.
    fn try_stop(
        &self,
        cause: crate::runtime::fragment::ExecutionFailure,
    ) -> Result<(), crate::runtime::fragment::ExecutionFailure> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if matches!(*state, ReceiverConstructionState::Open) {
            *state = ReceiverConstructionState::Stopped(cause);
            self.stopped.store(true, Ordering::Release);
            Ok(())
        } else {
            Err(cause)
        }
    }
    fn close(&self) -> Option<crate::runtime::fragment::ExecutionFailure> {
        let previous = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            let previous = std::mem::replace(&mut *state, ReceiverConstructionState::ClosedNeutral);
            self.stopped.store(true, Ordering::Release);
            previous
        };
        match previous {
            ReceiverConstructionState::Stopped(cause) => Some(cause),
            ReceiverConstructionState::Open | ReceiverConstructionState::ClosedNeutral => None,
        }
    }
}
impl ConstructionErrorSignal for ReceiverConstructionSignal {
    fn checkpoint(&self) -> Result<(), ConstructionFailure> {
        // Match the kernel signal's open fast path. This bit is monotonic;
        // stopped readers still inspect the canonical state under its lock.
        if !self.stopped.load(Ordering::Acquire) {
            return Ok(());
        }
        // Only clone the original Arc under this lock. Allocating the caller
        // envelope or a neutral observation happens after releasing it.
        let original = {
            let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            match &*state {
                ReceiverConstructionState::Open => return Ok(()),
                ReceiverConstructionState::Stopped(cause) => Some(cause.clone()),
                ReceiverConstructionState::ClosedNeutral => None,
            }
        };
        Err(ConstructionFailure::Caller(Box::new(
            original.unwrap_or_else(|| {
                crate::runtime::fragment::ExecutionFailure::from_domain(ReceiverConstructionClosed)
            }),
        )))
    }
}

#[derive(Debug)]
struct ReceiverConstructionClosed;
impl std::fmt::Display for ReceiverConstructionClosed {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("receiver input construction is closed")
    }
}
impl std::error::Error for ReceiverConstructionClosed {}
impl novarocks_failure_contract::FailureProjection for ReceiverConstructionClosed {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureFact::stop_observation(
            crate::runtime::fragment::ExecutionReason::ReceiverConstructionClosed.reason(),
        )
    }
}

/// The unique producer-side stop responsibility, moved with the receiver
/// registration. Factory and scope aliases retain only this same control;
/// neither a naked signal Arc nor a Task/runtime capability escapes.
pub struct ReceiverInputConstructionStopOwner {
    control: Arc<RuntimeInputConstructionControl<ReceiverConstructionSignal>>,
    stop_on_drop: bool,
}
impl ReceiverInputConstructionStopOwner {
    pub fn stop(&self, cause: crate::runtime::fragment::ExecutionFailure) {
        // A later source loses first-wins without being destroyed under the
        // signal lock. Its caller still supervises this synchronous capture.
        drop(self.control.error.try_stop(cause));
    }

    /// Close after the host seals its actual ingress borrowing gate. This
    /// only detaches the original source; the host must supervise its Drop
    /// outside every dependency lock. Neither source nor Work exit is proved.
    /// Late factory/scope metadata remains irreversibly stopped and retains
    /// no rich failure source. Consuming this owner revokes further stop calls.
    pub fn close_after_ingress(mut self) -> Option<crate::runtime::fragment::ExecutionFailure> {
        self.stop_on_drop = false;
        self.control.error.close()
    }
}
impl Drop for ReceiverInputConstructionStopOwner {
    fn drop(&mut self) {
        if self.stop_on_drop {
            self.stop(novarocks_functions::KernelFailure::Cancelled.into());
        }
    }
}

struct TrackedArcCharge {
    tracker: Arc<MemTracker>,
    bytes: usize,
}
impl Drop for TrackedArcCharge {
    fn drop(&mut self) {
        self.tracker
            .release(i64::try_from(self.bytes).expect("successful metadata request fits i64"));
    }
}
fn metadata_request<T>(tracker: &Arc<MemTracker>) -> Result<TrackedArcCharge, ConstructionFailure> {
    let layout = novarocks_type_contract::owned_resources::layout::arc_layout(Layout::new::<T>())
        .map_err(ConstructionFailure::from)?;
    let bytes = i64::try_from(layout.size()).map_err(|_| {
        ConstructionFailure::Control(
            novarocks_type_contract::CompileControlError::ResourceExhausted,
        )
    })?;
    if let Err(error) = tracker.consume_and_check_limit_typed(bytes) {
        tracker.release(bytes);
        return Err(ConstructionFailure::Memory(error));
    }
    Ok(TrackedArcCharge {
        tracker: tracker.clone(),
        bytes: layout.size(),
    })
}
struct TrackedArrowModel {
    _creation: TrackedArcCharge,
}
impl super::InputConstructionOperationModel for TrackedArrowModel {
    fn observe(
        &self,
        step: crate::exec::chunk::ArrayNormalizationStep<'_>,
        request: &mut dyn FnMut(Layout) -> Result<(), ConstructionFailure>,
        opaque: &mut dyn FnMut(super::OpaqueConstructionBound) -> Result<(), ConstructionFailure>,
        work: &mut dyn FnMut(usize) -> Result<(), ConstructionFailure>,
    ) -> Result<(), ConstructionFailure> {
        super::LockedArrowInputConstructionModel.observe(step, request, opaque, work)
    }
}

/// Production convenience constructor. Every newly allocated adapter, factory
/// and creator holder is observed before its own Arc construction and retains
/// its independent late-drop responsibility. No process-default tracker,
/// QueryMemoryBinding or entire RuntimeState is substituted for these inputs.
pub fn try_runtime_input_construction_factory(
    profile: super::InputConstructionProfile,
    tracker: Arc<MemTracker>,
    error: Arc<RuntimeErrorState>,
) -> Result<Arc<dyn super::InputConstructionFactory>, ConstructionFailure> {
    if let Some(error) = error.error() {
        return Err(ConstructionFailure::Caller(Box::new(error)));
    }
    build_tracked_input_construction_factory(profile, tracker, error, |_| ())
        .map(|(factory, ())| factory)
}

/// Construct a receiver's independently stopped control on the SAME actual
/// tracker/profile/Arrow model. Existing kernel failure still refuses creation
/// before any metadata request, but later normal kernel stop is not its owner.
pub fn try_receiver_input_construction_factory(
    profile: super::InputConstructionProfile,
    tracker: Arc<MemTracker>,
    kernel_error: Arc<RuntimeErrorState>,
) -> Result<
    (
        Arc<dyn super::InputConstructionFactory>,
        ReceiverInputConstructionStopOwner,
    ),
    ConstructionFailure,
> {
    if let Some(error) = kernel_error.error() {
        return Err(ConstructionFailure::Caller(Box::new(error)));
    }
    build_tracked_input_construction_factory(
        profile,
        tracker,
        ReceiverConstructionSignal::default(),
        |control| ReceiverInputConstructionStopOwner {
            control: Arc::clone(control),
            stop_on_drop: true,
        },
    )
}

fn build_tracked_input_construction_factory<S, G>(
    profile: super::InputConstructionProfile,
    tracker: Arc<MemTracker>,
    error: S,
    stop_owner: impl FnOnce(&Arc<RuntimeInputConstructionControl<S>>) -> G,
) -> Result<(Arc<dyn super::InputConstructionFactory>, G), ConstructionFailure>
where
    S: ConstructionErrorSignal + 'static,
{
    let host_charge = metadata_request::<TrackedInputConstructionHost>(&tracker)?;
    let host = Arc::new(TrackedInputConstructionHost {
        tracker: tracker.clone(),
        _creation: Some(host_charge),
    });
    let control_charge = metadata_request::<RuntimeInputConstructionControl<S>>(&tracker)?;
    let control = Arc::new(RuntimeInputConstructionControl {
        error,
        _creation: Some(control_charge),
    });
    // Establish the local guard as soon as the actual control exists, before
    // any subsequent model/factory request can refuse or unwind.
    let stop_owner = stop_owner(&control);
    let model_charge = metadata_request::<TrackedArrowModel>(&tracker)?;
    let model = Arc::new(TrackedArrowModel {
        _creation: model_charge,
    });
    super::ObservedInputConstructionFactory::try_new(profile, host, control, model).map(|factory| {
        (
            factory as Arc<dyn super::InputConstructionFactory>,
            stop_owner,
        )
    })
}
impl<S: ConstructionErrorSignal> InputConstructionWorkControl
    for RuntimeInputConstructionControl<S>
{
    fn checkpoint(&self, _: usize) -> Result<(), ConstructionFailure> {
        self.error.checkpoint()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::chunk::{Chunk, ChunkSchema};
    use novarocks_type_contract::ValueTypeStorageLifetime;
    fn profile() -> super::super::InputConstructionProfile {
        super::super::InputConstructionProfile {
            max_requests: 256,
            max_request_bytes: i64::MAX as usize,
            max_opaque_requests: 256,
            max_opaque_bytes: i64::MAX as usize,
            max_work: usize::MAX,
            max_depth: 64,
        }
    }
    fn source() -> Chunk {
        let storage = ValueTypeStorageLifetime::pure();
        Chunk::try_new_typed_with_columns_owned_observed::<
            crate::runtime::fragment::ExecutionFailure,
            _,
        >(
            Arc::new(ChunkSchema::try_new_typed(vec![], storage.clone()).unwrap()),
            vec![],
            1,
            storage,
            |_| Ok(()),
        )
        .unwrap()
    }
    #[test]
    fn production_metadata_is_observed_before_creation_and_all_late_drops_balance() {
        let tracker = MemTracker::new_root("construction metadata");
        let error = Arc::new(RuntimeErrorState::default());
        let error_weak = Arc::downgrade(&error);
        let factory =
            try_runtime_input_construction_factory(profile(), tracker.clone(), error).unwrap();
        assert!(tracker.current() > 0);
        let scope = factory.begin_input_construction(&source()).unwrap();
        let hold = scope.storage_lifetime();
        drop(factory);
        assert!(error_weak.upgrade().is_some());
        drop(scope);
        assert!(error_weak.upgrade().is_none());
        assert!(tracker.current() > 0);
        drop(hold);
        assert_eq!(tracker.current(), 0);
    }
    #[test]
    fn failed_first_adapter_request_creates_no_retained_charge() {
        let tracker = MemTracker::new_root("construction metadata");
        tracker.install_limit_once(1).unwrap();
        assert!(
            try_runtime_input_construction_factory(
                profile(),
                tracker.clone(),
                Arc::new(RuntimeErrorState::default())
            )
            .is_err()
        );
        assert_eq!(tracker.current(), 0);
    }
    #[test]
    fn runtime_control_returns_the_original_failure_payload() {
        let error = Arc::new(RuntimeErrorState::default());
        let original: crate::runtime::fragment::ExecutionFailure =
            novarocks_functions::KernelFailure::DeadlineExceeded.into();
        error.set_error(original.clone());
        let control = RuntimeInputConstructionControl::new(error);
        assert_eq!(
            control.checkpoint(0).unwrap_err(),
            ConstructionFailure::Caller(Box::new(original))
        );
    }

    // Receiver lifetime differs from kernel lifetime, not capacity or Arrow
    // semantics. These owner-local tests use the real finite tracker and the
    // original typed causes as independent oracles; no service/controller or
    // timing assumptions are involved. Keep them with the shared factory.
    #[test]
    fn receiver_factory_existing_kernel_failure_precedes_all_metadata_requests() {
        let tracker = MemTracker::new_root("receiver construction metadata");
        tracker.install_limit_once(1).unwrap();
        let kernel = Arc::new(RuntimeErrorState::default());
        let original: crate::runtime::fragment::ExecutionFailure =
            novarocks_functions::KernelFailure::DeadlineExceeded.into();
        kernel.set_error(original.clone());
        let before = tracker.allocated();
        let result = try_receiver_input_construction_factory(profile(), tracker.clone(), kernel);
        assert!(matches!(result, Err(ConstructionFailure::Caller(cause)) if *cause == original));
        assert_eq!(tracker.allocated(), before);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn receiver_factory_keeps_kernel_stop_separate_and_forwards_original_receiver_cause() {
        let tracker = MemTracker::new_root("receiver construction metadata");
        tracker.install_limit_once(1024 * 1024).unwrap();
        let kernel = Arc::new(RuntimeErrorState::default());
        let weak_kernel = Arc::downgrade(&kernel);
        let (factory, owner) =
            try_receiver_input_construction_factory(profile(), tracker.clone(), kernel.clone())
                .unwrap();
        kernel.set_error(novarocks_functions::KernelFailure::Cancelled);
        let scope = factory.begin_input_construction(&source()).unwrap();
        scope.request(Layout::new::<u64>()).unwrap();
        drop(kernel);
        assert!(
            weak_kernel.upgrade().is_none(),
            "receiver control never retains the kernel"
        );
        let original: crate::runtime::fragment::ExecutionFailure =
            novarocks_functions::KernelFailure::ResourceExhausted.into();
        owner.stop(original.clone());
        owner.stop(novarocks_functions::KernelFailure::DeadlineExceeded.into());
        assert_eq!(
            scope.request(Layout::new::<u64>()).unwrap_err(),
            ConstructionFailure::Caller(Box::new(original.clone())),
        );
        assert_eq!(
            factory.observe_input_work(1).unwrap_err(),
            ConstructionFailure::Caller(Box::new(original)),
        );
        drop(scope);
        drop(factory);
        assert!(
            tracker.current() > 0,
            "the unique stop owner still retains the original control block"
        );
        drop(owner);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn receiver_factory_metadata_refusal_releases_initial_and_owned_control_prefixes() {
        // Derive the later refusal point from the SAME original metadata
        // request author, not from guessed private bytes or platform sizes.
        let measured = MemTracker::new_root("receiver metadata request geometry");
        let host = metadata_request::<TrackedInputConstructionHost>(&measured).unwrap();
        let control =
            metadata_request::<RuntimeInputConstructionControl<ReceiverConstructionSignal>>(
                &measured,
            )
            .unwrap();
        let owned_control_prefix = measured.current();
        drop(control);
        drop(host);
        assert_eq!(measured.current(), 0);
        for limit in [1, owned_control_prefix] {
            let tracker = MemTracker::new_root("receiver metadata refusal");
            tracker.install_limit_once(limit).unwrap();
            let result = try_receiver_input_construction_factory(
                profile(),
                tracker.clone(),
                Arc::new(RuntimeErrorState::default()),
            );
            assert!(matches!(result, Err(ConstructionFailure::Memory(_))));
            assert_eq!(
                tracker.current(),
                0,
                "failed factory releases every constructed prefix"
            );
        }
        // The same production builder's owner constructor is the observation
        // seam: keep a private control alias to inspect the local guard after
        // the real next metadata request refuses. No public signal escapes.
        let tracker = MemTracker::new_root("receiver stopped construction prefix");
        tracker.install_limit_once(owned_control_prefix).unwrap();
        let mut observed_control = None;
        let result = build_tracked_input_construction_factory(
            profile(),
            tracker.clone(),
            ReceiverConstructionSignal::default(),
            |control| {
                observed_control = Some(Arc::clone(control));
                ReceiverInputConstructionStopOwner {
                    control: Arc::clone(control),
                    stop_on_drop: true,
                }
            },
        );
        assert!(matches!(result, Err(ConstructionFailure::Memory(_))));
        let control =
            observed_control.expect("the actual owned control was created before refusal");
        assert_eq!(
            control.checkpoint(0).unwrap_err(),
            ConstructionFailure::Caller(Box::new(
                novarocks_functions::KernelFailure::Cancelled.into()
            )),
        );
        drop(control);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn receiver_stop_owner_drop_stops_live_scopes_and_last_storage_release_balances() {
        let tracker = MemTracker::new_root("receiver construction aliases");
        tracker.install_limit_once(1024 * 1024).unwrap();
        let (factory, owner) = try_receiver_input_construction_factory(
            profile(),
            tracker.clone(),
            Arc::new(RuntimeErrorState::default()),
        )
        .unwrap();
        let control_weak = Arc::downgrade(&owner.control);
        let scope = factory.begin_input_construction(&source()).unwrap();
        let storage = scope.storage_lifetime();
        drop(factory);
        drop(owner);
        let cancelled: crate::runtime::fragment::ExecutionFailure =
            novarocks_functions::KernelFailure::Cancelled.into();
        assert_eq!(
            scope.request(Layout::new::<u64>()).unwrap_err(),
            ConstructionFailure::Caller(Box::new(cancelled)),
        );
        assert!(
            control_weak.upgrade().is_some(),
            "the live scope retains the stopped signal"
        );
        drop(scope);
        assert!(control_weak.upgrade().is_none());
        drop(control_weak);
        assert!(
            tracker.current() > 0,
            "independent constructed storage still holds its original creator"
        );
        drop(storage);
        assert_eq!(tracker.current(), 0);
    }

    fn assert_closed(failure: ConstructionFailure) {
        use novarocks_failure_contract::{FailureEvidence, FailureProjection};
        let ConstructionFailure::Caller(cause) = failure else {
            panic!("closed receiver has a typed caller observation");
        };
        assert_eq!(
            cause.failure_fact().evidence(),
            FailureEvidence::StopObservation
        );
        assert_eq!(
            cause.failure_fact().reason(),
            crate::runtime::fragment::ExecutionReason::ReceiverConstructionClosed.reason()
        );
        assert!(matches!(
            cause.cause(),
            crate::runtime::fragment::ExecutionFailureCause::Domain(source)
                if source.downcast_ref::<ReceiverConstructionClosed>().is_some()
        ));
    }

    #[derive(Debug)]
    struct ReentrantSource {
        control: std::sync::Weak<RuntimeInputConstructionControl<ReceiverConstructionSignal>>,
        drops: Arc<std::sync::atomic::AtomicUsize>,
        panic_on_drop: bool,
    }
    impl std::fmt::Display for ReentrantSource {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("original receiver source")
        }
    }
    impl std::error::Error for ReentrantSource {}
    impl novarocks_failure_contract::FailureProjection for ReentrantSource {
        fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
            novarocks_functions::KernelFailure::ResourceExhausted.failure_fact()
        }
    }
    impl Drop for ReentrantSource {
        fn drop(&mut self) {
            if let Some(control) = self.control.upgrade() {
                let state = control
                    .error
                    .state
                    .try_lock()
                    .expect("source destructor runs outside the signal lock");
                assert!(matches!(*state, ReceiverConstructionState::ClosedNeutral));
                drop(state);
                assert_closed(control.checkpoint(0).unwrap_err());
            }
            self.drops.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            assert!(
                !self.panic_on_drop,
                "injected original receiver source Drop"
            );
        }
    }

    #[test]
    fn receiver_close_returns_original_source_and_old_alias_controls_its_actual_drop() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let tracker = MemTracker::new_root("receiver source retirement");
        let (factory, owner) = try_receiver_input_construction_factory(
            profile(),
            tracker.clone(),
            Arc::new(RuntimeErrorState::default()),
        )
        .unwrap();
        let scope = factory.begin_input_construction(&source()).unwrap();
        let storage = scope.storage_lifetime();
        let weak_control = Arc::downgrade(&owner.control);
        let drops = Arc::new(AtomicUsize::new(0));
        let original = crate::runtime::fragment::ExecutionFailure::from_domain(ReentrantSource {
            control: weak_control.clone(),
            drops: drops.clone(),
            panic_on_drop: false,
        });
        let old_alias = original.clone();
        owner.stop(original);
        let ConstructionFailure::Caller(observed) =
            scope.request(Layout::new::<u64>()).unwrap_err()
        else {
            panic!("live stopped scope preserves its original failure");
        };
        assert!(observed.same_occurrence(&old_alias));
        drop(observed);
        let detached = owner.close_after_ingress().unwrap();
        assert!(detached.same_occurrence(&old_alias));
        assert_closed(scope.request(Layout::new::<u64>()).unwrap_err());
        let before = tracker.allocated();
        assert_closed(factory.observe_input_work(1).unwrap_err());
        assert_closed(
            factory
                .begin_input_construction(&source())
                .err()
                .expect("closed metadata cannot admit a new scope"),
        );
        assert_eq!(tracker.allocated(), before);
        drop(detached);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(old_alias);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        drop(factory);
        drop(scope);
        assert!(weak_control.upgrade().is_none());
        assert!(
            tracker.current() > 0,
            "late storage retains original capacity"
        );
        drop(storage);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn receiver_close_without_failure_is_sticky_and_late_signal_stop_returns_same_source() {
        let tracker = MemTracker::new_root("receiver closed neutral signal");
        let (factory, owner) = try_receiver_input_construction_factory(
            profile(),
            tracker.clone(),
            Arc::new(RuntimeErrorState::default()),
        )
        .unwrap();
        let control = owner.control.clone();
        assert!(owner.close_after_ingress().is_none());
        assert_closed(control.checkpoint(0).unwrap_err());
        let late: crate::runtime::fragment::ExecutionFailure =
            novarocks_functions::KernelFailure::DeadlineExceeded.into();
        let alias = late.clone();
        let returned = control.error.try_stop(late).unwrap_err();
        assert!(returned.same_occurrence(&alias));
        assert_closed(control.checkpoint(0).unwrap_err());
        assert!(control.error.close().is_none());
        assert_closed(factory.observe_input_work(1).unwrap_err());
        drop(returned);
        drop(alias);
        drop(control);
        drop(factory);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn open_receiver_checkpoint_does_not_wait_for_its_metadata_lock() {
        let tracker = MemTracker::new_root("receiver open checkpoint");
        let (factory, owner) = try_receiver_input_construction_factory(
            profile(),
            tracker.clone(),
            Arc::new(RuntimeErrorState::default()),
        )
        .unwrap();
        let control = owner.control.clone();
        let state = control.error.state.lock().unwrap();
        let worker = control.clone();
        let (sent, received) = std::sync::mpsc::channel();
        let checkpoint = std::thread::spawn(move || sent.send(worker.checkpoint(0)).unwrap());
        let result = received.recv_timeout(std::time::Duration::from_secs(5));
        // Always release and join, even if a regression blocks the checkpoint.
        drop(state);
        checkpoint.join().unwrap();
        assert!(
            matches!(result, Ok(Ok(()))),
            "an open checkpoint uses its original atomic fast path: {result:?}"
        );
        assert!(owner.close_after_ingress().is_none());
        assert_closed(control.checkpoint(0).unwrap_err());
        drop(control);
        drop(factory);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn detached_original_source_drop_panic_cannot_reopen_or_poison_receiver_signal() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let tracker = MemTracker::new_root("receiver source Drop failure");
        let (factory, owner) = try_receiver_input_construction_factory(
            profile(),
            tracker.clone(),
            Arc::new(RuntimeErrorState::default()),
        )
        .unwrap();
        let control = owner.control.clone();
        let drops = Arc::new(AtomicUsize::new(0));
        owner.stop(crate::runtime::fragment::ExecutionFailure::from_domain(
            ReentrantSource {
                control: Arc::downgrade(&control),
                drops: drops.clone(),
                panic_on_drop: true,
            },
        ));
        let detached = owner.close_after_ingress().unwrap();
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(detached))).is_err());
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert!(!control.error.state.is_poisoned());
        assert_closed(control.checkpoint(0).unwrap_err());
        assert_closed(factory.observe_input_work(1).unwrap_err());
        drop(control);
        drop(factory);
        assert_eq!(tracker.current(), 0);
    }
}
