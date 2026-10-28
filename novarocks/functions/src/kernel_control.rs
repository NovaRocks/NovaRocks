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

//! Carrier-neutral typed failures and observed kernel evaluation control.

use novarocks_type_contract::CompileControlError;
use std::{fmt, sync::Mutex, time::Duration};

/// Bounded diagnostics on outer failures. Only RowDataError enters the
/// maskable row channel; this carrier cannot be converted to it implicitly.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KernelDiagnostic(Box<str>);
impl KernelDiagnostic {
    pub fn new(message: &str) -> Self {
        let mut end = message.len().min(crate::MAX_ROW_ERROR_MESSAGE_BYTES);
        while !message.is_char_boundary(end) {
            end -= 1;
        }
        Self(message[..end].into())
    }
    pub fn message(&self) -> &str {
        &self.0
    }
}

/// Opaque original cause for the outer channel. Only explicitly marked
/// producer errors enter it; RowDataError has no implicit conversion here.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KernelSourceFailure(novarocks_failure_contract::SharedFailureCause);
impl KernelSourceFailure {
    pub fn detail(&self) -> &str {
        self.0.detail()
    }
    /// Keep one admitted guard on this direct canonical source node, including
    /// aliases made before attachment. Its opaque payload's independently
    /// shared child resources remain their original owners' responsibility.
    pub fn retain_source_lifetime(
        &self,
        custody: Box<dyn novarocks_failure_contract::SourceRetentionCapability>,
    ) {
        self.0.retain_source_lifetime(custody);
    }
}
impl fmt::Display for KernelSourceFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for KernelSourceFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        std::error::Error::source(&self.0)
    }
}
impl novarocks_failure_contract::FailureProjection for KernelSourceFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        self.0.failure_fact()
    }
}

/// Known byte-limit refusal projected by its producing domain. This compact
/// value has no diagnostic allocation, tracker reference or grant authority.
/// It is suitable for allocation callbacks before error reporting can allocate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct KernelCapacityRefusal {
    reason: novarocks_failure_contract::FailureReason,
    requested: u64,
    limit: u64,
    committed: u64,
}
impl KernelCapacityRefusal {
    pub fn new(
        reason: novarocks_failure_contract::FailureReason,
        requested: u64,
        limit: u64,
        committed: u64,
    ) -> Self {
        Self {
            reason,
            requested,
            limit,
            committed,
        }
    }
}
impl fmt::Display for KernelCapacityRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "byte capacity refused (reason={}, requested={}, limit={}, committed={})",
            self.reason, self.requested, self.limit, self.committed
        )
    }
}
impl std::error::Error for KernelCapacityRefusal {}
impl novarocks_failure_contract::FailureProjection for KernelCapacityRefusal {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{
            FailureClass, FailureFact, ResourceDimension, ResourceFacts,
        };
        FailureFact::new(FailureClass::Resource, self.reason).with_resource(ResourceFacts {
            request_scope: None,
            constraint_scope: None,
            dimension: ResourceDimension::Bytes,
            requested: Some(self.requested),
            required_growth: None,
            limit: Some(self.limit),
            live: None,
            committed: Some(self.committed),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum KernelFailure {
    Capacity(KernelCapacityRefusal),
    Source(KernelSourceFailure),
    CompileControl(CompileControlError),
    Cancelled,
    DeadlineExceeded,
    ResourceExhausted,
    InvalidProgram(KernelDiagnostic),
    Internal(KernelDiagnostic),
    Operational(KernelDiagnostic),
    InstanceFailed,
}
impl fmt::Display for KernelFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Source(source) => source.fmt(f),
            Self::CompileControl(source) => source.fmt(f),
            Self::Capacity(source) => source.fmt(f),
            Self::InstanceFailed => f.write_str("kernel instance has already failed"),
            Self::Cancelled => f.write_str("kernel evaluation was cancelled"),
            Self::DeadlineExceeded => f.write_str("kernel evaluation deadline was exceeded"),
            Self::ResourceExhausted => f.write_str("kernel evaluation resources were exhausted"),
            Self::InvalidProgram(message) => {
                write!(f, "invalid kernel program: {}", message.message())
            }
            Self::Internal(message) => write!(f, "kernel internal failure: {}", message.message()),
            Self::Operational(message) => {
                write!(f, "kernel operational failure: {}", message.message())
            }
        }
    }
}
impl KernelFailure {
    /// Forward an already admitted guard through the known typed source
    /// owners. Value-only failures return the same guard to their original
    /// enclosing owner. No source projection, Work or capacity is created.
    pub fn try_retain_source_lifetime(
        &self,
        custody: Box<dyn novarocks_failure_contract::SourceRetentionCapability>,
    ) -> Result<(), Box<dyn novarocks_failure_contract::SourceRetentionCapability>> {
        match self {
            Self::Source(source) => {
                source.retain_source_lifetime(custody);
                Ok(())
            }
            Self::CompileControl(source) => source.try_retain_source_lifetime(custody),
            Self::Capacity(_)
            | Self::Cancelled
            | Self::DeadlineExceeded
            | Self::ResourceExhausted
            | Self::InvalidProgram(_)
            | Self::Internal(_)
            | Self::Operational(_)
            | Self::InstanceFailed => Err(custody),
        }
    }
    // Only RowDataError's checked admission calls this. A non-Data source is
    // unmaskable; avoid reprojecting or replacing its actual occurrence.
    pub(crate) fn rejected_row_source(
        source: novarocks_failure_contract::SharedFailureCause,
    ) -> Self {
        debug_assert_ne!(
            novarocks_failure_contract::FailureProjection::failure_fact(&source).class(),
            novarocks_failure_contract::FailureClass::Data
        );
        Self::Source(KernelSourceFailure(source))
    }
    pub(crate) fn is_interruption(&self) -> bool {
        use novarocks_failure_contract::{FailureClass, FailureProjection};
        matches!(self, Self::CompileControl(_) | Self::Capacity(_))
            || matches!(
                self.failure_fact().class(),
                FailureClass::Control | FailureClass::Resource
            )
    }
    /// These are already unmaskable originating failures. An optional completion
    /// observation must neither replace them nor start another fallible tail.
    /// RowDataError remains in its own journal; Task acceptance is separate.
    #[cfg(test)]
    pub(crate) fn is_atomic_failure(&self) -> bool {
        use novarocks_failure_contract::{FailureClass, FailureProjection};
        matches!(self, Self::CompileControl(_) | Self::Capacity(_))
            || matches!(
                self.failure_fact().class(),
                FailureClass::Control
                    | FailureClass::Resource
                    | FailureClass::Data
                    | FailureClass::Representation
            )
    }
    pub fn from_domain<E: novarocks_failure_contract::OuterFailureProjection>(source: E) -> Self {
        Self::Source(KernelSourceFailure(
            novarocks_failure_contract::SharedFailureCause::new(source),
        ))
    }
}
impl std::error::Error for KernelFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Source(source) => Some(source),
            Self::CompileControl(source) => Some(source),
            Self::Capacity(source) => Some(source),
            _ => None,
        }
    }
}

/// Runs a kernel during the caller's explicit compilation phase.
/// Compilation never acquires a waiting capability through this adapter.
pub struct CompileKernelControl<'a> {
    control: &'a dyn novarocks_type_contract::PureCompileControl,
    phase: novarocks_type_contract::CompilePhase,
}
impl<'a> CompileKernelControl<'a> {
    pub fn new(
        control: &'a dyn novarocks_type_contract::PureCompileControl,
        phase: novarocks_type_contract::CompilePhase,
    ) -> Self {
        Self { control, phase }
    }
}
impl KernelEvaluationControl for CompileKernelControl<'_> {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        self.control
            .checkpoint(self.phase, units)
            .map_err(compile_failure)
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        Err(KernelFailure::InvalidProgram(KernelDiagnostic::new(
            "compile-time kernel evaluation cannot wait",
        )))
    }
}

pub const MAX_UNOBSERVED_KERNEL_WORK: u32 = 256;

/// Runtime-owned interruption/work control, independent of statement semantic
/// time. Waiting is the exact sleep implementation's observable operation.
/// The host installs its formal memory scopes and authorizes known allocation
/// steps before invocation; this interface does not mint a second budget.
pub trait KernelEvaluationControl: Send + Sync {
    fn checkpoint(&self, work_units: u32) -> Result<(), KernelFailure>;
    fn wait(&self, duration: Duration) -> Result<(), KernelFailure>;
}

/// Borrow the original host control across a nested lifecycle call. This
/// observer forwards the original units and waits, without creating a meter,
/// budget or cancellation authority. A refusal remains primary across nested
/// post-call checks, including diagnostic failures returned by that control.
pub struct KernelControlObservation<'a> {
    original: &'a dyn KernelEvaluationControl,
    refusal: Mutex<Option<KernelFailure>>,
}
impl<'a> KernelControlObservation<'a> {
    pub fn new(original: &'a dyn KernelEvaluationControl) -> Self {
        Self {
            original,
            refusal: Mutex::new(None),
        }
    }
    fn observe(
        &self,
        operation: impl FnOnce() -> Result<(), KernelFailure>,
    ) -> Result<(), KernelFailure> {
        let mut refusal = self.refusal.lock().unwrap();
        if let Some(error) = refusal.as_ref() {
            return Err(error.clone());
        }
        let result = operation();
        if let Err(error) = &result {
            *refusal = Some(error.clone());
        }
        result
    }
    pub fn finish<T>(&self, result: Result<T, KernelFailure>) -> Result<T, KernelFailure> {
        match self.refusal.lock().unwrap().as_ref() {
            Some(error) => Err(error.clone()),
            None => result,
        }
    }
}
impl KernelEvaluationControl for KernelControlObservation<'_> {
    fn checkpoint(&self, work_units: u32) -> Result<(), KernelFailure> {
        self.observe(|| self.original.checkpoint(work_units))
    }
    fn wait(&self, duration: Duration) -> Result<(), KernelFailure> {
        self.observe(|| self.original.wait(duration))
    }
}

pub(crate) fn invalid(message: &str) -> KernelFailure {
    KernelFailure::InvalidProgram(KernelDiagnostic::new(message))
}
pub(crate) fn internal(message: &str) -> KernelFailure {
    KernelFailure::Internal(KernelDiagnostic::new(message))
}

pub(crate) fn compile_failure(error: CompileControlError) -> KernelFailure {
    KernelFailure::CompileControl(error)
}
pub(crate) fn type_failure(error: novarocks_type_contract::ValueTypeError) -> KernelFailure {
    KernelFailure::from_domain(error)
}

impl From<novarocks_type_contract::ValueTypeError> for KernelFailure {
    fn from(error: novarocks_type_contract::ValueTypeError) -> Self {
        type_failure(error)
    }
}
impl From<novarocks_type_contract::CarrierParameterError> for KernelFailure {
    fn from(error: novarocks_type_contract::CarrierParameterError) -> Self {
        Self::from_domain(error)
    }
}

impl From<crate::EvaluationContractError> for KernelFailure {
    fn from(error: crate::EvaluationContractError) -> Self {
        Self::from_domain(error)
    }
}

/// Existing protocol tests check the input/output channel independently of the
/// outer carrier. A typed source must be the actual evaluation-contract owner;
/// neither row Data nor Resource/Control failures satisfy these assertions.
#[cfg(test)]
pub(crate) mod test_contract {
    use super::*;
    use crate::EvaluationContractError;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    fn evaluation(error: &KernelFailure) -> Option<&EvaluationContractError> {
        let KernelFailure::Source(source) = error else {
            return None;
        };
        assert_eq!(source.failure_fact().class(), FailureClass::Invariant);
        source.0.downcast_ref()
    }
    pub fn input(error: &KernelFailure) -> bool {
        matches!(error, KernelFailure::InvalidProgram(_))
            || matches!(
                evaluation(error),
                Some(
                    EvaluationContractError::ArgumentLength
                        | EvaluationContractError::ArgumentType
                        | EvaluationContractError::ArgumentSelection
                        | EvaluationContractError::ArgumentRowErrors
                )
            )
    }
    pub fn output(error: &KernelFailure) -> bool {
        matches!(error, KernelFailure::Internal(_))
            || matches!(
                evaluation(error),
                Some(
                    EvaluationContractError::ArgumentLength
                        | EvaluationContractError::ArgumentType
                        | EvaluationContractError::ArgumentSelection
                        | EvaluationContractError::ArgumentRowErrors
                        | EvaluationContractError::ResultLength
                        | EvaluationContractError::ResultType
                        | EvaluationContractError::InvalidRowErrors
                )
            )
    }
    pub fn exact(error: &KernelFailure, expected: EvaluationContractError) {
        assert_eq!(evaluation(error), Some(&expected));
        assert_eq!(error.failure_fact(), expected.failure_fact());
    }
}

#[cfg(test)]
mod compile_source_tests {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    use novarocks_type_contract::owned_resources::layout::LayoutResourceError;
    #[test]
    fn compile_interruption_keeps_its_author_and_is_unmaskable_even_when_invariant() {
        let raw = LayoutResourceError::SourceModel;
        let expected = raw.failure_fact();
        let control = CompileControlError::from_domain(raw);
        let kernel = compile_failure(control.clone());
        assert_eq!(kernel.failure_fact(), expected);
        assert_eq!(kernel.failure_fact().class(), FailureClass::Invariant);
        assert!(kernel.is_interruption());
        assert!(kernel.is_atomic_failure());
        let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(&kernel);
        let mut original = None;
        while let Some(error) = cause {
            if let Some(found) = error.downcast_ref::<CompileControlError>() {
                original = Some(found);
                break;
            }
            cause = error.source();
        }
        assert_eq!(original, Some(&control));
    }
}

#[cfg(test)]
mod source_retention_tests {
    use super::{KernelCapacityRefusal, KernelDiagnostic, KernelFailure, KernelSourceFailure};
    use novarocks_failure_contract::{
        FailureClass, FailureFact, FailureProjection, FailureReason, SharedFailureCause,
        SourceRetentionCapability, domain,
    };
    use novarocks_type_contract::CompileControlError;
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
            f.write_str("original typed kernel source")
        }
    }
    impl Error for Original {}
    impl FailureProjection for Original {
        fn failure_fact(&self) -> FailureFact {
            self.observations.projections.fetch_add(1, Ordering::SeqCst);
            FailureFact::new(
                FailureClass::Invariant,
                FailureReason::new(domain::KERNEL, NonZeroU16::new(408).unwrap()),
            )
        }
    }
    impl Drop for Original {
        fn drop(&mut self) {
            self.observations.sources.fetch_add(1, Ordering::SeqCst);
            assert!(!self.panic_on_drop, "original kernel source destructor");
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
    fn failure(shared: SharedFailureCause, compile: bool) -> KernelFailure {
        if compile {
            KernelFailure::CompileControl(CompileControlError::Source(shared))
        } else {
            KernelFailure::Source(KernelSourceFailure(shared))
        }
    }
    fn inner(failure: &KernelFailure) -> &SharedFailureCause {
        match failure {
            KernelFailure::Source(source) => &source.0,
            KernelFailure::CompileControl(CompileControlError::Source(source)) => source,
            _ => panic!("the fixture supplies a real typed shared source"),
        }
    }

    #[test]
    fn typed_source_retention_follows_original_kernel_and_compile_nodes() {
        for compile in [false, true] {
            let observed = Arc::new(Observations::default());
            let failure = failure(
                SharedFailureCause::new(Original {
                    observations: observed.clone(),
                    panic_on_drop: false,
                }),
                compile,
            );
            // Keep the actual inner node before the outer typed owner receives
            // custody, including KernelSourceFailure's private canonical node.
            let early = inner(&failure).clone();
            let original = early.downcast_ref::<Original>().unwrap() as *const Original;
            let fact = failure.failure_fact();
            failure
                .try_retain_source_lifetime(Box::new(Guard(observed.clone())))
                .unwrap();
            let late = inner(&failure).clone();
            assert_eq!(
                late.downcast_ref::<Original>().unwrap() as *const _,
                original
            );
            assert_eq!(failure.failure_fact(), fact);
            assert_eq!(late.detail(), "original typed kernel source");
            assert_eq!(observed.projections.load(Ordering::SeqCst), 1);
            assert_eq!(observed.renderings.load(Ordering::SeqCst), 1);
            drop(failure);
            drop(early);
            assert_eq!(observed.sources.load(Ordering::SeqCst), 0);
            assert_eq!(observed.guards.load(Ordering::SeqCst), 0);
            drop(late);
            assert_eq!(observed.sources.load(Ordering::SeqCst), 1);
            assert_eq!(observed.guards.load(Ordering::SeqCst), 1);
            assert_eq!(observed.sources_at_guard_exit.load(Ordering::SeqCst), 1);
            assert!(!observed.unknown.load(Ordering::SeqCst));
        }
    }

    #[test]
    fn typed_source_retention_preserves_source_unwind() {
        for compile in [false, true] {
            let observed = Arc::new(Observations::default());
            let failure = failure(
                SharedFailureCause::new(Original {
                    observations: observed.clone(),
                    panic_on_drop: true,
                }),
                compile,
            );
            let early = inner(&failure).clone();
            failure
                .try_retain_source_lifetime(Box::new(Guard(observed.clone())))
                .unwrap();
            drop(failure);
            assert_eq!(observed.guards.load(Ordering::SeqCst), 0);
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(early)));
            assert!(result.is_err());
            assert_eq!(observed.sources.load(Ordering::SeqCst), 1);
            assert_eq!(observed.guards.load(Ordering::SeqCst), 1);
            assert_eq!(observed.sources_at_guard_exit.load(Ordering::SeqCst), 1);
            assert!(observed.unknown.load(Ordering::SeqCst));
        }
    }

    #[test]
    fn value_failures_return_the_same_unused_guard() {
        let reason = FailureReason::new(domain::KERNEL, NonZeroU16::new(409).unwrap());
        for failure in [
            KernelFailure::Capacity(KernelCapacityRefusal::new(reason, 3, 2, 1)),
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::ResourceExhausted,
            KernelFailure::InvalidProgram(KernelDiagnostic::new("invalid")),
            KernelFailure::Internal(KernelDiagnostic::new("internal")),
            KernelFailure::Operational(KernelDiagnostic::new("operational")),
            KernelFailure::InstanceFailed,
            KernelFailure::CompileControl(CompileControlError::Cancelled),
            KernelFailure::CompileControl(CompileControlError::DeadlineExceeded),
            KernelFailure::CompileControl(CompileControlError::ResourceExhausted),
        ] {
            let observed = Arc::new(Observations::default());
            let guard: Box<dyn SourceRetentionCapability> = Box::new(Guard(observed.clone()));
            let original = &*guard as *const dyn SourceRetentionCapability as *const ();
            let returned = failure.try_retain_source_lifetime(guard).unwrap_err();
            assert_eq!(
                &*returned as *const dyn SourceRetentionCapability as *const (),
                original
            );
            drop(failure);
            assert_eq!(observed.guards.load(Ordering::SeqCst), 0);
            drop(returned);
            assert_eq!(observed.guards.load(Ordering::SeqCst), 1);
        }
    }
}
