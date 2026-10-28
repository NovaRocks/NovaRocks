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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt;

/// Cooperative control of pure preparation. A caller owns clocks, cancellation
/// and work budgets; the compiler has no scheduler, I/O or runtime capability.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum CompilePhase {
    CarrierPreflight,
    Encode,
    Decode,
    Validate,
    ProviderValidation,
    FunctionSpecialization,
    LowerProgram,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CompileControlError {
    Cancelled,
    DeadlineExceeded,
    ResourceExhausted,
    /// An unmaskable refusal authored by the original caller control. The
    /// pure compiler keeps its cause/facts without acquiring runtime services.
    Source(novarocks_failure_contract::SharedFailureCause),
}
/// Semantic provenance captured from an actual compile-control error. This
/// owns no originating error tree and is suitable for process-local history.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompileControlProvenance {
    Cancelled,
    DeadlineExceeded,
    ResourceExhausted,
    Source(novarocks_failure_contract::FailureFact),
}
impl CompileControlError {
    pub fn provenance(&self) -> CompileControlProvenance {
        use novarocks_failure_contract::FailureProjection;
        match self {
            Self::Cancelled => CompileControlProvenance::Cancelled,
            Self::DeadlineExceeded => CompileControlProvenance::DeadlineExceeded,
            Self::ResourceExhausted => CompileControlProvenance::ResourceExhausted,
            Self::Source(source) => CompileControlProvenance::Source(source.failure_fact()),
        }
    }
}
impl CompileControlError {
    pub fn detail(&self) -> &str {
        match self {
            Self::Cancelled => "pure compilation was cancelled",
            Self::DeadlineExceeded => "pure compilation deadline was exceeded",
            Self::ResourceExhausted => "pure compilation work budget was exhausted",
            Self::Source(cause) => cause.detail(),
        }
    }
    pub fn from_domain<E: novarocks_failure_contract::OuterFailureProjection>(cause: E) -> Self {
        Self::Source(novarocks_failure_contract::SharedFailureCause::new(cause))
    }
    /// Attach an already admitted guard to this owner's direct shared source
    /// node. Value-only refusals return the same guard for their caller's
    /// original value owner. This grants no capacity and does not inspect
    /// independently shared resources inside an opaque source payload.
    pub fn try_retain_source_lifetime(
        &self,
        custody: Box<dyn novarocks_failure_contract::SourceRetentionCapability>,
    ) -> Result<(), Box<dyn novarocks_failure_contract::SourceRetentionCapability>> {
        match self {
            Self::Source(source) => {
                source.retain_source_lifetime(custody);
                Ok(())
            }
            Self::Cancelled | Self::DeadlineExceeded | Self::ResourceExhausted => Err(custody),
        }
    }
}
impl fmt::Display for CompileControlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.detail())
    }
}
impl std::error::Error for CompileControlError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Source(cause) => Some(cause),
            _ => None,
        }
    }
}
impl novarocks_failure_contract::OuterFailureProjection for CompileControlError {}

pub trait PureCompileControl: Send + Sync {
    /// `work_units` accounts completed bounded operations since the previous
    /// checkpoint. Zero checks control before any work begins. Resource/control
    /// failures abort compilation and never enter the selected-row error carrier.
    fn checkpoint(&self, phase: CompilePhase, work_units: u32) -> Result<(), CompileControlError>;
}

pub const MAX_UNOBSERVED_COMPILE_WORK: u32 = 256;

pub struct CompileCheckpoints<'a> {
    owner: &'a dyn PureCompileControl,
    phase: CompilePhase,
    pending: u32,
    failed: Option<CompileControlError>,
}
impl<'a> CompileCheckpoints<'a> {
    pub fn try_new(
        owner: &'a dyn PureCompileControl,
        phase: CompilePhase,
    ) -> Result<Self, CompileControlError> {
        owner.checkpoint(phase, 0)?;
        Ok(Self {
            owner,
            phase,
            pending: 0,
            failed: None,
        })
    }
    /// Call after one bounded operation; expensive library operations require
    /// their own preflight and checkpoints within their expansion loop.
    pub fn step(&mut self) -> Result<(), CompileControlError> {
        if let Some(error) = &self.failed {
            return Err(error.clone());
        }
        self.pending += 1;
        if self.pending == MAX_UNOBSERVED_COMPILE_WORK {
            match self.owner.checkpoint(self.phase, self.pending) {
                Ok(()) => self.pending = 0,
                Err(error) => {
                    self.failed = Some(error.clone());
                    return Err(error);
                }
            }
        }
        Ok(())
    }
    /// Observe the completed tail before handing work to an exact owner. The
    /// next operation keeps this same control and budget; flushing is not a
    /// new admission or a resource reset.
    pub fn flush(&mut self) -> Result<(), CompileControlError> {
        if let Some(error) = &self.failed {
            return Err(error.clone());
        }
        match self.owner.checkpoint(self.phase, self.pending) {
            Ok(()) => {
                self.pending = 0;
                Ok(())
            }
            Err(error) => {
                self.failed = Some(error.clone());
                Err(error)
            }
        }
    }
    /// Borrow the original caller control for work delegated to another owner.
    /// The caller flushes its pending work before entering that owner.
    pub fn control(&self) -> &'a dyn PureCompileControl {
        self.owner
    }
    pub fn finish(mut self) -> Result<(), CompileControlError> {
        self.flush()
    }
    /// Complete an author's outer result with this same checkpoint scope.
    /// An already observed refusal remains first. Otherwise an authored outer
    /// error is returned without starting a later cooperative callback. A
    /// successful result, including a normal row-data carrier, still flushes.
    /// This is control observation only, never resource settlement or cleanup.
    pub fn finish_result<T, E>(
        mut self,
        result: Result<T, E>,
        control_error: impl FnOnce(CompileControlError) -> E,
    ) -> Result<T, E> {
        if let Some(earlier) = self.failed.take() {
            return Err(control_error(earlier));
        }
        match result {
            Err(original) => Err(original),
            Ok(value) => {
                self.flush().map_err(control_error)?;
                Ok(value)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    struct Owner {
        units: Mutex<Vec<u32>>,
        fail: Option<CompileControlError>,
    }
    impl PureCompileControl for Owner {
        fn checkpoint(&self, _: CompilePhase, work_units: u32) -> Result<(), CompileControlError> {
            self.units.lock().unwrap().push(work_units);
            if work_units > 0
                && let Some(error) = &self.fail
            {
                return Err(error.clone());
            }
            Ok(())
        }
    }
    #[test]
    fn bounded_observation_accounts_all_work() {
        let owner = Owner {
            units: Mutex::default(),
            fail: None,
        };
        let mut scope = CompileCheckpoints::try_new(&owner, CompilePhase::Validate).unwrap();
        for _ in 0..2000 {
            scope.step().unwrap();
        }
        scope.finish().unwrap();
        let units = owner.units.lock().unwrap();
        assert_eq!(units.iter().sum::<u32>(), 2000);
        assert!(
            units
                .iter()
                .all(|units| *units <= MAX_UNOBSERVED_COMPILE_WORK)
        );
    }
    #[test]
    fn control_failures_latch_without_reaccounting_work() {
        for error in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let owner = Owner {
                units: Mutex::default(),
                fail: Some(error.clone()),
            };
            let mut scope =
                CompileCheckpoints::try_new(&owner, CompilePhase::LowerProgram).unwrap();
            for _ in 1..MAX_UNOBSERVED_COMPILE_WORK {
                scope.step().unwrap();
            }
            assert_eq!(scope.step(), Err(error.clone()));
            assert_eq!(scope.step(), Err(error.clone()));
            assert_eq!(scope.finish(), Err(error));
            assert_eq!(
                *owner.units.lock().unwrap(),
                vec![0, MAX_UNOBSERVED_COMPILE_WORK]
            );
        }
    }

    struct PhaseOwner {
        calls: Mutex<Vec<(CompilePhase, u32)>>,
        fail_at: Option<(usize, CompileControlError)>,
    }
    impl PureCompileControl for PhaseOwner {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            let mut calls = self.calls.lock().unwrap();
            calls.push((phase, units));
            if let Some((index, error)) = self.fail_at.clone()
                && calls.len() == index
            {
                return Err(error);
            }
            Ok(())
        }
    }

    #[test]
    fn flush_observes_and_resets_tails_without_replacing_the_borrowed_control() {
        let owner = PhaseOwner {
            calls: Mutex::default(),
            fail_at: None,
        };
        let phase = CompilePhase::FunctionSpecialization;
        let mut checksum = 0u64;
        let original_control = {
            let mut scope = CompileCheckpoints::try_new(&owner, phase).unwrap();
            for value in 0u64..7 {
                checksum += value;
                scope.step().unwrap();
            }
            scope.flush().unwrap();
            // A flush with no intervening work still observes control.
            scope.flush().unwrap();
            for value in 7u64..266 {
                checksum += value;
                scope.step().unwrap();
            }
            let original = scope.control();
            scope.finish().unwrap();
            original
        };
        assert_eq!(checksum, (0u64..266).sum::<u64>());
        assert_eq!(
            *owner.calls.lock().unwrap(),
            vec![(phase, 0), (phase, 7), (phase, 0), (phase, 256), (phase, 3)]
        );
        // The getter borrows the original owner's lifetime, rather than the
        // completed checkpoint scope. This call still reaches that owner.
        original_control
            .checkpoint(CompilePhase::Encode, 0)
            .unwrap();
        assert_eq!(
            owner.calls.lock().unwrap().last(),
            Some(&(CompilePhase::Encode, 0))
        );
    }

    #[test]
    fn flush_failures_latch_short_and_zero_tails_without_repeated_charges() {
        for error in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for tail in [0u32, 7] {
                let owner = PhaseOwner {
                    calls: Mutex::default(),
                    // Only the first flush fails. Re-entering the owner would
                    // return success, exposing a lost latch or double charge.
                    fail_at: Some((2, error.clone())),
                };
                let phase = CompilePhase::LowerProgram;
                let mut scope = CompileCheckpoints::try_new(&owner, phase).unwrap();
                let mut checksum = 0u64;
                for value in 0..tail {
                    checksum += u64::from(value);
                    scope.step().unwrap();
                }
                assert_eq!(checksum, (0..tail).map(u64::from).sum::<u64>());
                assert_eq!(scope.flush(), Err(error.clone()));
                assert_eq!(scope.step(), Err(error.clone()));
                assert_eq!(scope.flush(), Err(error.clone()));
                assert_eq!(scope.finish(), Err(error.clone()));
                assert_eq!(
                    *owner.calls.lock().unwrap(),
                    vec![(phase, 0), (phase, tail)]
                );
            }
        }
    }
}

#[cfg(test)]
mod source_tests {
    use super::*;
    use crate::owned_resources::layout::LayoutResourceError;
    use novarocks_failure_contract::FailureProjection;
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct SourceOwner {
        failure: CompileControlError,
        calls: AtomicUsize,
    }
    impl PureCompileControl for SourceOwner {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                Ok(())
            } else {
                Err(self.failure.clone())
            }
        }
    }
    #[test]
    fn an_original_control_refusal_keeps_its_occurrence_and_suppresses_later_tails() {
        let raw = LayoutResourceError::SourceModel;
        let expected = raw.failure_fact();
        let original = CompileControlError::from_domain(raw);
        let owner = SourceOwner {
            failure: original.clone(),
            calls: AtomicUsize::new(0),
        };
        let mut checkpoints =
            CompileCheckpoints::try_new(&owner, CompilePhase::LowerProgram).unwrap();
        for _ in 1..MAX_UNOBSERVED_COMPILE_WORK {
            checkpoints.step().unwrap();
        }
        let first = checkpoints.step().unwrap_err();
        assert_eq!(first, original);
        assert_eq!(first.failure_fact(), expected);
        assert_eq!(checkpoints.flush().unwrap_err(), original);
        assert_eq!(owner.calls.load(Ordering::SeqCst), 2);
        let CompileControlError::Source(source) = first else {
            panic!("original source required");
        };
        assert_eq!(source.downcast_ref::<LayoutResourceError>(), Some(&raw));
    }
}

#[cfg(test)]
mod first_cause_tests {
    use super::*;
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };
    struct Control {
        trace: Mutex<Vec<u32>>,
        refusal: Option<(usize, CompileControlError)>,
    }
    impl PureCompileControl for Control {
        fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            let mut trace = self.trace.lock().unwrap();
            let at = trace.len();
            trace.push(units);
            match &self.refusal {
                Some((stop, cause)) if *stop == at => Err(cause.clone()),
                _ => Ok(()),
            }
        }
    }
    #[derive(Debug)]
    struct ParseOwner {
        source: std::num::ParseIntError,
        drops: Arc<AtomicUsize>,
    }
    impl std::fmt::Display for ParseOwner {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            std::fmt::Display::fmt(&self.source, f)
        }
    }
    impl std::error::Error for ParseOwner {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(&self.source)
        }
    }
    impl Drop for ParseOwner {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }
    #[test]
    fn real_parse_error_keeps_original_box_and_actual_error_owner_until_drop() {
        let control = Control {
            trace: Mutex::default(),
            refusal: Some((1, CompileControlError::Cancelled)),
        };
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
        for _ in 0..7 {
            work.step().unwrap();
        }
        let drops = Arc::new(AtomicUsize::new(0));
        let original: Box<dyn std::error::Error> = Box::new(ParseOwner {
            source: "bad".parse::<i64>().unwrap_err(),
            drops: drops.clone(),
        });
        let pointer = original.as_ref() as *const dyn std::error::Error as *const ();
        let result: Result<(), Box<dyn std::error::Error>> =
            work.finish_result(Err(original), |cause| Box::new(cause) as _);
        let returned = result.unwrap_err();
        assert_eq!(
            returned.as_ref() as *const dyn std::error::Error as *const (),
            pointer
        );
        assert!(returned.source().unwrap().is::<std::num::ParseIntError>());
        assert_eq!(*control.trace.lock().unwrap(), [0]);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(returned);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
    #[test]
    fn already_observed_source_beats_later_semantic_error_without_new_callback() {
        use crate::owned_resources::layout::LayoutResourceError;
        let original = CompileControlError::from_domain(LayoutResourceError::Arithmetic);
        let control = Control {
            trace: Mutex::default(),
            refusal: Some((1, original.clone())),
        };
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
        work.step().unwrap();
        assert_eq!(work.flush(), Err(original.clone()));
        let later = "bad".parse::<i64>().unwrap_err();
        let result: Result<(), Box<dyn std::error::Error>> =
            work.finish_result(Err(Box::new(later) as _), |cause| Box::new(cause) as _);
        let returned = result.unwrap_err();
        assert_eq!(
            returned.downcast_ref::<CompileControlError>(),
            Some(&original)
        );
        assert_eq!(*control.trace.lock().unwrap(), [0, 1]);
    }
    #[test]
    fn successful_carriers_still_flush_the_same_pending_work_and_original_source() {
        use crate::owned_resources::layout::LayoutResourceError;
        let original = CompileControlError::from_domain(LayoutResourceError::Arithmetic);
        for units in [0, 7] {
            let control = Control {
                trace: Mutex::default(),
                refusal: Some((1, original.clone())),
            };
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
            for _ in 0..units {
                work.step().unwrap();
            }
            // A successfully returned carrier can contain row-data values.
            let result = work
                .finish_result(Ok::<_, CompileControlError>(Some("normal data")), |cause| {
                    cause
                });
            assert_eq!(result, Err(original.clone()));
            assert_eq!(*control.trace.lock().unwrap(), [0, units]);
        }
    }
}

#[cfg(test)]
mod source_retention_tests {
    use super::CompileControlError;
    use novarocks_failure_contract::{
        FailureClass, FailureFact, FailureProjection, FailureReason, SharedFailureCause,
        SourceRetentionCapability, domain,
    };
    use std::{
        error::Error,
        fmt,
        num::NonZeroU16,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
    };

    #[derive(Debug, Default)]
    struct Observations {
        sources: AtomicUsize,
        projections: AtomicUsize,
        renderings: AtomicUsize,
        guards: AtomicUsize,
        sources_at_guard_exit: AtomicUsize,
        unknown: AtomicBool,
    }
    #[derive(Debug)]
    struct Original {
        observations: Arc<Observations>,
        panic_on_drop: bool,
    }
    impl fmt::Display for Original {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            self.observations.renderings.fetch_add(1, Ordering::SeqCst);
            f.write_str("original compile source")
        }
    }
    impl Error for Original {}
    impl FailureProjection for Original {
        fn failure_fact(&self) -> FailureFact {
            self.observations.projections.fetch_add(1, Ordering::SeqCst);
            FailureFact::new(
                FailureClass::Invariant,
                FailureReason::new(domain::KERNEL, NonZeroU16::new(407).unwrap()),
            )
        }
    }
    impl Drop for Original {
        fn drop(&mut self) {
            self.observations.sources.fetch_add(1, Ordering::SeqCst);
            assert!(!self.panic_on_drop, "original compile source destructor");
        }
    }
    #[derive(Debug)]
    struct Guard(Arc<Observations>);
    impl SourceRetentionCapability for Guard {}
    impl Drop for Guard {
        fn drop(&mut self) {
            self.0
                .sources_at_guard_exit
                .store(self.0.sources.load(Ordering::SeqCst), Ordering::SeqCst);
            self.0
                .unknown
                .store(std::thread::panicking(), Ordering::SeqCst);
            self.0.guards.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn shared_source_retention_follows_early_and_late_inner_aliases() {
        let observed = Arc::new(Observations::default());
        let shared = SharedFailureCause::new(Original {
            observations: observed.clone(),
            panic_on_drop: false,
        });
        let original = shared.downcast_ref::<Original>().unwrap() as *const Original;
        let early = shared.clone();
        let control = CompileControlError::Source(shared);
        let fact = control.failure_fact();
        control
            .try_retain_source_lifetime(Box::new(Guard(observed.clone())))
            .unwrap();
        let late = control
            .source()
            .unwrap()
            .downcast_ref::<SharedFailureCause>()
            .unwrap()
            .clone();
        assert_eq!(
            late.downcast_ref::<Original>().unwrap() as *const _,
            original
        );
        assert_eq!(control.failure_fact(), fact);
        assert_eq!(control.detail(), "original compile source");
        assert_eq!(observed.projections.load(Ordering::SeqCst), 1);
        assert_eq!(observed.renderings.load(Ordering::SeqCst), 1);
        drop(control);
        drop(early);
        assert_eq!(observed.sources.load(Ordering::SeqCst), 0);
        assert_eq!(observed.guards.load(Ordering::SeqCst), 0);
        drop(late);
        assert_eq!(observed.sources.load(Ordering::SeqCst), 1);
        assert_eq!(observed.guards.load(Ordering::SeqCst), 1);
        assert_eq!(observed.sources_at_guard_exit.load(Ordering::SeqCst), 1);
        assert!(!observed.unknown.load(Ordering::SeqCst));
    }

    #[test]
    fn source_drop_unwind_reaches_retention_guard() {
        let observed = Arc::new(Observations::default());
        let shared = SharedFailureCause::new(Original {
            observations: observed.clone(),
            panic_on_drop: true,
        });
        let early = shared.clone();
        let control = CompileControlError::Source(shared);
        control
            .try_retain_source_lifetime(Box::new(Guard(observed.clone())))
            .unwrap();
        drop(control);
        assert_eq!(observed.guards.load(Ordering::SeqCst), 0);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(early)));
        assert!(result.is_err());
        assert_eq!(observed.sources.load(Ordering::SeqCst), 1);
        assert_eq!(observed.guards.load(Ordering::SeqCst), 1);
        assert_eq!(observed.sources_at_guard_exit.load(Ordering::SeqCst), 1);
        assert!(observed.unknown.load(Ordering::SeqCst));
    }

    #[test]
    fn value_refusals_return_the_same_unused_guard() {
        for control in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let observed = Arc::new(Observations::default());
            let guard: Box<dyn SourceRetentionCapability> = Box::new(Guard(observed.clone()));
            let original = &*guard as *const dyn SourceRetentionCapability as *const ();
            let returned = control.try_retain_source_lifetime(guard).unwrap_err();
            assert_eq!(
                &*returned as *const dyn SourceRetentionCapability as *const (),
                original
            );
            drop(control);
            assert_eq!(observed.guards.load(Ordering::SeqCst), 0);
            drop(returned);
            assert_eq!(observed.guards.load(Ordering::SeqCst), 1);
        }
    }
}
