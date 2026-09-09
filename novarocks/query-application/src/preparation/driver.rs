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

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt,
    future::Future,
    pin::Pin,
    sync::Arc,
};

use novarocks_workload_control::{Stage, StageRequest, WorkScope};

use crate::observation::{
    NegotiationId, ObservationId, ObservationRequirement, PreparationBudget,
    PreparationBudgetError, PreparationLimit, PreparationLimits,
};

use super::{FrozenExecutionDescription, FrozenExecutionDescriptionDraft};

#[derive(Debug)]
pub struct ObservationRequest<R> {
    id: ObservationId,
    requirement: ObservationRequirement,
    request: R,
}

impl<R> ObservationRequest<R> {
    pub const fn new(id: ObservationId, requirement: ObservationRequirement, request: R) -> Self {
        Self {
            id,
            requirement,
            request,
        }
    }
    pub const fn id(&self) -> ObservationId {
        self.id
    }
    pub const fn requirement(&self) -> ObservationRequirement {
        self.requirement
    }
    pub const fn request(&self) -> &R {
        &self.request
    }
}

pub struct ObservationResponse<F> {
    fact: F,
}

impl<F> ObservationResponse<F> {
    pub const fn new(fact: F) -> Self {
        Self { fact }
    }
}

#[derive(Debug)]
pub struct NegotiationRequest<R> {
    id: NegotiationId,
    request: R,
}

impl<R> NegotiationRequest<R> {
    pub const fn new(id: NegotiationId, request: R) -> Self {
        Self { id, request }
    }
    pub const fn id(&self) -> NegotiationId {
        self.id
    }
    pub const fn request(&self) -> &R {
        &self.request
    }
}

pub struct NegotiationResponse<F> {
    fact: F,
}

impl<F> NegotiationResponse<F> {
    pub const fn new(fact: F) -> Self {
        Self { fact }
    }
}

/// A typed preparation value reports its retained in-process footprint at the
/// application adapter that owns the value. The driver never accepts a free
/// byte count detached from that value.
pub trait PreparationFootprint {
    fn preparation_footprint_bytes(&self) -> usize;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SourceErrorKind {
    OptionalUnavailable,
    RequiredBindingInvalid,
    Cancelled,
    DeadlineExceeded,
    ContractViolation,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceError {
    kind: SourceErrorKind,
    message: Arc<str>,
}

impl SourceError {
    pub fn new(kind: SourceErrorKind, message: impl Into<Arc<str>>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
    pub const fn kind(&self) -> SourceErrorKind {
        self.kind
    }
}

impl fmt::Display for SourceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}
impl Error for SourceError {}

pub type ObservationFuture<F> =
    Pin<Box<dyn Future<Output = Result<ObservationResponse<F>, SourceError>> + Send + 'static>>;

pub trait ObservationSource<R, F>: Send + Sync + 'static {
    fn observe(&self, request: ObservationRequest<R>) -> ObservationFuture<F>;
}

pub type NegotiationFuture<F> =
    Pin<Box<dyn Future<Output = Result<NegotiationResponse<F>, SourceError>> + Send + 'static>>;

pub trait NegotiationSource<R, F>: Send + Sync + 'static {
    fn negotiate(&self, request: NegotiationRequest<R>) -> NegotiationFuture<F>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OptionalObservationDiagnostic {
    id: ObservationId,
    message: Arc<str>,
}

impl OptionalObservationDiagnostic {
    pub const fn id(&self) -> ObservationId {
        self.id
    }
    pub fn message(&self) -> &str {
        &self.message
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NegotiationFact<F> {
    fact: F,
    changed: bool,
}

impl<F> NegotiationFact<F> {
    pub const fn fact(&self) -> &F {
        &self.fact
    }
    pub const fn changed(&self) -> bool {
        self.changed
    }
}

/// Values presented to the compiler. Associated fact types preserve their SQL
/// or Connector semantics; no bytes envelope or decoding layer is introduced.
pub struct CompilerInput<'a, O, N> {
    observations: &'a BTreeMap<ObservationId, O>,
    negotiations: &'a BTreeMap<NegotiationId, NegotiationFact<N>>,
    diagnostics: &'a [OptionalObservationDiagnostic],
    unavailable_optional: &'a BTreeSet<ObservationId>,
    diagnostic_budget_exhausted: bool,
}

impl<'a, O, N> CompilerInput<'a, O, N> {
    pub fn observation(&self, id: ObservationId) -> Option<&O> {
        self.observations.get(&id)
    }
    pub fn negotiation(&self, id: NegotiationId) -> Option<&NegotiationFact<N>> {
        self.negotiations.get(&id)
    }
    pub const fn diagnostics(&self) -> &[OptionalObservationDiagnostic] {
        self.diagnostics
    }
    pub fn optional_candidate_unavailable(&self, id: ObservationId) -> bool {
        self.unavailable_optional.contains(&id)
    }
    pub const fn diagnostic_budget_exhausted(&self) -> bool {
        self.diagnostic_budget_exhausted
    }
}

pub enum CompilerStep<OR, NR> {
    NeedObservations(Vec<ObservationRequest<OR>>),
    NeedNegotiations(Vec<NegotiationRequest<NR>>),
    Complete(Box<FrozenExecutionDescriptionDraft>),
}

pub trait PurePreparationCompiler<OR, OF, NR, NF> {
    fn advance(
        &mut self,
        input: CompilerInput<'_, OF, NF>,
    ) -> Result<CompilerStep<OR, NR>, PreparationError>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PreparationErrorKind {
    Governance,
    Cancelled,
    DeadlineExceeded,
    RequiredObservation,
    SourceContract,
    Budget,
    Compiler,
    NonConvergent,
    InvalidDescription,
}

#[derive(Debug)]
pub struct PreparationError {
    kind: PreparationErrorKind,
    message: Arc<str>,
}

impl PreparationError {
    pub fn new(kind: PreparationErrorKind, message: impl Into<Arc<str>>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
    pub const fn kind(&self) -> PreparationErrorKind {
        self.kind
    }
}

impl fmt::Display for PreparationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}
impl Error for PreparationError {}

impl From<PreparationBudgetError> for PreparationError {
    fn from(error: PreparationBudgetError) -> Self {
        let kind = match error.limit() {
            PreparationLimit::Deadline => PreparationErrorKind::DeadlineExceeded,
            _ => PreparationErrorKind::Budget,
        };
        Self::new(kind, Arc::<str>::from(error.to_string()))
    }
}

pub struct PreparationDriver<OR, OF, NR, NF> {
    observations: Arc<dyn ObservationSource<OR, OF>>,
    negotiations: Arc<dyn NegotiationSource<NR, NF>>,
    limits: PreparationLimits,
}

impl<OR, OF, NR, NF> PreparationDriver<OR, OF, NR, NF>
where
    OR: Send + 'static,
    OF: Send + 'static,
    NR: Send + 'static,
    NF: Clone + Eq + Send + 'static,
    OR: PreparationFootprint,
    OF: PreparationFootprint,
    NR: PreparationFootprint,
    NF: PreparationFootprint,
{
    pub fn new(
        observations: Arc<dyn ObservationSource<OR, OF>>,
        negotiations: Arc<dyn NegotiationSource<NR, NF>>,
        limits: PreparationLimits,
    ) -> Self {
        Self {
            observations,
            negotiations,
            limits,
        }
    }

    pub async fn prepare(
        &self,
        compiler: &mut dyn PurePreparationCompiler<OR, OF, NR, NF>,
        scope: &WorkScope,
    ) -> Result<FrozenExecutionDescription, PreparationError> {
        let permit = scope
            .acquire(StageRequest {
                stage: Stage::Preparation,
                retained_bytes: 0,
            })
            .map_err(governance_error)?
            .await
            .map_err(governance_error)?;
        permit
            .check(scope, Stage::Preparation)
            .map_err(governance_error)?;
        let cancellation = scope.cancellation().map_err(governance_error)?;
        let deadline = tokio::time::Instant::from_std(self.limits.deadline());
        let budget = PreparationBudget::new(self.limits);
        let mut observations = BTreeMap::new();
        let mut negotiations = BTreeMap::new();
        let mut histories = BTreeMap::<NegotiationId, Vec<NF>>::new();
        let mut diagnostics = Vec::new();
        let mut unavailable_optional = BTreeSet::new();
        let mut diagnostic_budget_exhausted = false;
        let mut stalled = BTreeSet::new();
        let mut issued_observation_ids = BTreeSet::new();

        loop {
            scope.check().map_err(governance_error)?;
            budget.begin_round()?;
            let input = CompilerInput {
                observations: &observations,
                negotiations: &negotiations,
                diagnostics: &diagnostics,
                unavailable_optional: &unavailable_optional,
                diagnostic_budget_exhausted,
            };
            match compiler.advance(input)? {
                CompilerStep::NeedObservations(requests) => {
                    if requests.is_empty() {
                        return Err(contract_error(
                            "compiler requested an empty observation round",
                        ));
                    }
                    for request in requests {
                        if !issued_observation_ids.insert(request.id) {
                            return Err(contract_error("compiler repeated an observation id"));
                        }
                        budget.begin_observation(request.request.preparation_footprint_bytes())?;
                        let id = request.id;
                        let requirement = request.requirement;
                        let future = self.observations.observe(request);
                        let result = tokio::select! {
                            reason = cancellation.cancelled() => {
                                return Err(PreparationError::new(PreparationErrorKind::Cancelled, format!("query preparation cancelled: {reason:?}")));
                            }
                            _ = tokio::time::sleep_until(deadline) => {
                                return Err(PreparationError::new(PreparationErrorKind::DeadlineExceeded, "query preparation deadline exceeded while observing provider facts"));
                            }
                            result = future => result,
                        };
                        match result {
                            Ok(response) => {
                                budget
                                    .record_response(response.fact.preparation_footprint_bytes())?;
                                observations.insert(id, response.fact);
                            }
                            Err(error)
                                if requirement == ObservationRequirement::OptionalCandidate
                                    && matches!(
                                        error.kind(),
                                        SourceErrorKind::OptionalUnavailable
                                            | SourceErrorKind::DeadlineExceeded
                                            | SourceErrorKind::ContractViolation
                                    ) =>
                            {
                                unavailable_optional.insert(id);
                                let message = error.to_string();
                                let diagnostic_result = budget.record_diagnostic(message.len());
                                if let Err(error) = &diagnostic_result
                                    && error.limit() == PreparationLimit::Deadline
                                {
                                    return Err(error.clone().into());
                                }
                                match diagnostic_result {
                                    Ok(()) => diagnostics.push(OptionalObservationDiagnostic {
                                        id,
                                        message: Arc::from(message),
                                    }),
                                    Err(_) => diagnostic_budget_exhausted = true,
                                }
                            }
                            Err(error) => return Err(source_error(error)),
                        }
                    }
                }
                CompilerStep::NeedNegotiations(requests) => {
                    if requests.is_empty() {
                        return Err(contract_error(
                            "compiler requested an empty negotiation round",
                        ));
                    }
                    let mut round_ids = BTreeSet::new();
                    for request in requests {
                        let id = request.id;
                        if !round_ids.insert(id) {
                            return Err(contract_error(
                                "compiler repeated a negotiation id in one round",
                            ));
                        }
                        if stalled.contains(&id) {
                            return Err(PreparationError::new(
                                PreparationErrorKind::NonConvergent,
                                format!("compiler repeated unchanged negotiation {}", id.get()),
                            ));
                        }
                        budget.begin_negotiation(request.request.preparation_footprint_bytes())?;
                        let future = self.negotiations.negotiate(request);
                        let response = tokio::select! {
                            reason = cancellation.cancelled() => {
                                return Err(PreparationError::new(PreparationErrorKind::Cancelled, format!("query preparation cancelled: {reason:?}")));
                            }
                            _ = tokio::time::sleep_until(deadline) => {
                                return Err(PreparationError::new(PreparationErrorKind::DeadlineExceeded, "query preparation deadline exceeded during Connector negotiation"));
                            }
                            result = future => result.map_err(source_error)?,
                        };
                        budget.record_response(response.fact.preparation_footprint_bytes())?;
                        let history = histories.entry(id).or_default();
                        let changed = history
                            .last()
                            .is_none_or(|previous| previous != &response.fact);
                        if changed && history.contains(&response.fact) {
                            return Err(PreparationError::new(
                                PreparationErrorKind::NonConvergent,
                                format!("Connector negotiation {} oscillated", id.get()),
                            ));
                        }
                        if changed {
                            history.push(response.fact.clone());
                            stalled.remove(&id);
                        } else {
                            stalled.insert(id);
                        }
                        negotiations.insert(
                            id,
                            NegotiationFact {
                                fact: response.fact,
                                changed,
                            },
                        );
                    }
                }
                CompilerStep::Complete(draft) => {
                    return FrozenExecutionDescription::try_freeze(*draft).map_err(|error| {
                        PreparationError::new(PreparationErrorKind::InvalidDescription, error)
                    });
                }
            }
        }
    }
}

fn governance_error(error: novarocks_workload_control::WorkError) -> PreparationError {
    PreparationError::new(PreparationErrorKind::Governance, error.to_string())
}

fn contract_error(message: &'static str) -> PreparationError {
    PreparationError::new(PreparationErrorKind::SourceContract, message)
}

fn source_error(error: SourceError) -> PreparationError {
    let kind = match error.kind() {
        SourceErrorKind::Cancelled => PreparationErrorKind::Cancelled,
        SourceErrorKind::DeadlineExceeded => PreparationErrorKind::DeadlineExceeded,
        SourceErrorKind::RequiredBindingInvalid => PreparationErrorKind::RequiredObservation,
        SourceErrorKind::OptionalUnavailable | SourceErrorKind::ContractViolation => {
            PreparationErrorKind::SourceContract
        }
    };
    PreparationError::new(kind, Arc::<str>::from(error.to_string()))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        num::{NonZeroU32, NonZeroUsize},
        sync::Mutex,
        time::{Duration, Instant},
    };

    use novarocks_sql::test_support::{NativePreparationFixture, native_preparation_plan};
    use novarocks_workload_control::{
        CancellationReason, ResourceConfig, WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
    };

    use super::*;
    use crate::{
        api::QueryExecutionKind,
        coordination::{ExecutionEffect, RecoveryMode},
        preparation::{ExecutionResourceRequirements, FrozenCostEstimate},
    };

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct Tiny(u8);

    impl PreparationFootprint for Tiny {
        fn preparation_footprint_bytes(&self) -> usize {
            1
        }
    }

    struct PendingObservation;

    impl ObservationSource<Tiny, Tiny> for PendingObservation {
        fn observe(&self, _request: ObservationRequest<Tiny>) -> ObservationFuture<Tiny> {
            Box::pin(std::future::pending())
        }
    }

    struct ImmediateObservation;

    impl ObservationSource<Tiny, Tiny> for ImmediateObservation {
        fn observe(&self, request: ObservationRequest<Tiny>) -> ObservationFuture<Tiny> {
            Box::pin(async move { Ok(ObservationResponse::new(request.request)) })
        }
    }

    struct OptionalFailureObservation(SourceErrorKind);

    impl ObservationSource<Tiny, Tiny> for OptionalFailureObservation {
        fn observe(&self, _request: ObservationRequest<Tiny>) -> ObservationFuture<Tiny> {
            let kind = self.0;
            Box::pin(async move { Err(SourceError::new(kind, "mv candidate unavailable")) })
        }
    }

    struct PendingNegotiation;

    impl NegotiationSource<Tiny, Tiny> for PendingNegotiation {
        fn negotiate(&self, _request: NegotiationRequest<Tiny>) -> NegotiationFuture<Tiny> {
            Box::pin(std::future::pending())
        }
    }

    struct SequenceNegotiation(Mutex<VecDeque<Tiny>>);

    impl NegotiationSource<Tiny, Tiny> for SequenceNegotiation {
        fn negotiate(&self, _request: NegotiationRequest<Tiny>) -> NegotiationFuture<Tiny> {
            let fact = self.0.lock().unwrap().pop_front().unwrap();
            Box::pin(async move { Ok(NegotiationResponse::new(fact)) })
        }
    }

    struct ObservationCompiler;

    impl PurePreparationCompiler<Tiny, Tiny, Tiny, Tiny> for ObservationCompiler {
        fn advance(
            &mut self,
            _input: CompilerInput<'_, Tiny, Tiny>,
        ) -> Result<CompilerStep<Tiny, Tiny>, PreparationError> {
            Ok(CompilerStep::NeedObservations(vec![
                ObservationRequest::new(
                    ObservationId::new(NonZeroU32::new(1).unwrap()),
                    ObservationRequirement::Required,
                    Tiny(1),
                ),
            ]))
        }
    }

    struct NegotiationCompiler;

    impl PurePreparationCompiler<Tiny, Tiny, Tiny, Tiny> for NegotiationCompiler {
        fn advance(
            &mut self,
            _input: CompilerInput<'_, Tiny, Tiny>,
        ) -> Result<CompilerStep<Tiny, Tiny>, PreparationError> {
            Ok(CompilerStep::NeedNegotiations(vec![
                NegotiationRequest::new(NegotiationId::new(NonZeroU32::new(1).unwrap()), Tiny(1)),
            ]))
        }
    }

    struct OptionalCompiler;

    impl PurePreparationCompiler<Tiny, Tiny, Tiny, Tiny> for OptionalCompiler {
        fn advance(
            &mut self,
            input: CompilerInput<'_, Tiny, Tiny>,
        ) -> Result<CompilerStep<Tiny, Tiny>, PreparationError> {
            let id = ObservationId::new(NonZeroU32::new(1).unwrap());
            if !input.optional_candidate_unavailable(id) {
                return Ok(CompilerStep::NeedObservations(vec![
                    ObservationRequest::new(id, ObservationRequirement::OptionalCandidate, Tiny(1)),
                ]));
            }
            Ok(CompilerStep::Complete(Box::new(
                FrozenExecutionDescriptionDraft::new(
                    QueryExecutionKind::Maintenance,
                    novarocks_sql::planning::query_execution::SealedPreparationPlan::seal(
                        native_preparation_plan(NativePreparationFixture::MissingResultOutput)
                            .unwrap(),
                    ),
                    Vec::new(),
                    None,
                    ExecutionEffect::None,
                    RecoveryMode::NoRecovery,
                    Vec::new(),
                    FrozenCostEstimate::default(),
                    ExecutionResourceRequirements::default(),
                ),
            )))
        }
    }

    struct OptionalDiagnosticBudgetCompiler;

    impl PurePreparationCompiler<Tiny, Tiny, Tiny, Tiny> for OptionalDiagnosticBudgetCompiler {
        fn advance(
            &mut self,
            input: CompilerInput<'_, Tiny, Tiny>,
        ) -> Result<CompilerStep<Tiny, Tiny>, PreparationError> {
            let id = ObservationId::new(NonZeroU32::new(1).unwrap());
            if !input.optional_candidate_unavailable(id) {
                return Ok(CompilerStep::NeedObservations(vec![
                    ObservationRequest::new(id, ObservationRequirement::OptionalCandidate, Tiny(1)),
                ]));
            }
            assert!(input.diagnostic_budget_exhausted());
            assert!(input.diagnostics().is_empty());
            Ok(CompilerStep::Complete(Box::new(
                FrozenExecutionDescriptionDraft::new(
                    QueryExecutionKind::Maintenance,
                    novarocks_sql::planning::query_execution::SealedPreparationPlan::seal(
                        native_preparation_plan(NativePreparationFixture::MissingResultOutput)
                            .unwrap(),
                    ),
                    Vec::new(),
                    None,
                    ExecutionEffect::None,
                    RecoveryMode::NoRecovery,
                    Vec::new(),
                    FrozenCostEstimate::default(),
                    ExecutionResourceRequirements::default(),
                ),
            )))
        }
    }

    struct TwoOptionalCompiler;

    impl PurePreparationCompiler<Tiny, Tiny, Tiny, Tiny> for TwoOptionalCompiler {
        fn advance(
            &mut self,
            input: CompilerInput<'_, Tiny, Tiny>,
        ) -> Result<CompilerStep<Tiny, Tiny>, PreparationError> {
            let first = ObservationId::new(NonZeroU32::new(1).unwrap());
            let second = ObservationId::new(NonZeroU32::new(2).unwrap());
            if !input.optional_candidate_unavailable(first) {
                return Ok(CompilerStep::NeedObservations(vec![
                    ObservationRequest::new(
                        first,
                        ObservationRequirement::OptionalCandidate,
                        Tiny(1),
                    ),
                    ObservationRequest::new(
                        second,
                        ObservationRequirement::OptionalCandidate,
                        Tiny(2),
                    ),
                ]));
            }
            assert!(input.optional_candidate_unavailable(second));
            assert_eq!(input.diagnostics().len(), 1);
            assert!(input.diagnostic_budget_exhausted());
            Ok(CompilerStep::Complete(Box::new(
                FrozenExecutionDescriptionDraft::new(
                    QueryExecutionKind::Maintenance,
                    novarocks_sql::planning::query_execution::SealedPreparationPlan::seal(
                        native_preparation_plan(NativePreparationFixture::MissingResultOutput)
                            .unwrap(),
                    ),
                    Vec::new(),
                    None,
                    ExecutionEffect::None,
                    RecoveryMode::NoRecovery,
                    Vec::new(),
                    FrozenCostEstimate::default(),
                    ExecutionResourceRequirements::default(),
                ),
            )))
        }
    }

    fn limits(deadline: Instant) -> PreparationLimits {
        PreparationLimits::new(
            crate::observation::PreparationCountLimits::new(
                NonZeroU32::new(8).unwrap(),
                NonZeroUsize::new(8).unwrap(),
                NonZeroUsize::new(8).unwrap(),
                NonZeroUsize::new(8).unwrap(),
            ),
            crate::observation::PreparationByteLimits::new(
                NonZeroUsize::new(64).unwrap(),
                NonZeroUsize::new(64).unwrap(),
                NonZeroUsize::new(256).unwrap(),
            ),
            deadline,
        )
    }

    fn scope() -> (novarocks_workload_control::RootWork, WorkScope) {
        let control = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1024,
                control_bytes: 128,
                per_scope_bytes: 896,
            },
        )
        .unwrap();
        control.mark_ready().unwrap();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let scope = root.owner.scope();
        (root, scope)
    }

    #[tokio::test]
    async fn deadline_interrupts_a_provider_future_that_never_returns() {
        let (_root, scope) = scope();
        let driver = PreparationDriver::new(
            Arc::new(PendingObservation),
            Arc::new(PendingNegotiation),
            limits(Instant::now() + Duration::from_millis(10)),
        );
        let error = driver
            .prepare(&mut ObservationCompiler, &scope)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), PreparationErrorKind::DeadlineExceeded);
    }

    #[tokio::test]
    async fn scope_cancellation_interrupts_a_provider_future_that_never_returns() {
        let (root, scope) = scope();
        let driver = PreparationDriver::new(
            Arc::new(PendingObservation),
            Arc::new(PendingNegotiation),
            limits(Instant::now() + Duration::from_secs(1)),
        );
        let cancel = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            root.owner.cancel(CancellationReason::Requested);
        });
        let error = driver
            .prepare(&mut ObservationCompiler, &scope)
            .await
            .unwrap_err();
        cancel.await.unwrap();
        assert_eq!(error.kind(), PreparationErrorKind::Cancelled);
    }

    #[tokio::test]
    async fn unchanged_negotiation_is_rejected_as_non_convergent() {
        let (_root, scope) = scope();
        let driver = PreparationDriver::new(
            Arc::new(PendingObservation),
            Arc::new(SequenceNegotiation(Mutex::new(VecDeque::from([
                Tiny(1),
                Tiny(1),
            ])))),
            limits(Instant::now() + Duration::from_secs(1)),
        );
        let error = driver
            .prepare(&mut NegotiationCompiler, &scope)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), PreparationErrorKind::NonConvergent);
    }

    #[tokio::test]
    async fn repeated_observation_id_is_rejected_before_map_overwrite() {
        let (_root, scope) = scope();
        let driver = PreparationDriver::new(
            Arc::new(ImmediateObservation),
            Arc::new(PendingNegotiation),
            limits(Instant::now() + Duration::from_secs(1)),
        );
        let error = driver
            .prepare(&mut ObservationCompiler, &scope)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), PreparationErrorKind::SourceContract);
    }

    #[tokio::test]
    async fn optional_candidate_failure_is_bounded_diagnostic_not_query_failure() {
        for kind in [
            SourceErrorKind::OptionalUnavailable,
            SourceErrorKind::DeadlineExceeded,
            SourceErrorKind::ContractViolation,
        ] {
            let (_root, scope) = scope();
            let driver = PreparationDriver::new(
                Arc::new(OptionalFailureObservation(kind)),
                Arc::new(PendingNegotiation),
                limits(Instant::now() + Duration::from_secs(1)),
            );
            let description = driver.prepare(&mut OptionalCompiler, &scope).await.unwrap();
            assert_eq!(description.kind(), QueryExecutionKind::Maintenance);
        }
    }

    #[tokio::test]
    async fn optional_diagnostic_budget_exhaustion_does_not_fail_the_query() {
        let (_root, scope) = scope();
        let limits = PreparationLimits::new(
            crate::observation::PreparationCountLimits::new(
                NonZeroU32::new(8).unwrap(),
                NonZeroUsize::new(8).unwrap(),
                NonZeroUsize::new(8).unwrap(),
                NonZeroUsize::new(8).unwrap(),
            ),
            crate::observation::PreparationByteLimits::new(
                NonZeroUsize::new(64).unwrap(),
                NonZeroUsize::new(1).unwrap(),
                NonZeroUsize::new(256).unwrap(),
            ),
            Instant::now() + Duration::from_secs(1),
        );
        let driver = PreparationDriver::new(
            Arc::new(OptionalFailureObservation(
                SourceErrorKind::OptionalUnavailable,
            )),
            Arc::new(PendingNegotiation),
            limits,
        );
        let description = driver
            .prepare(&mut OptionalDiagnosticBudgetCompiler, &scope)
            .await
            .unwrap();
        assert_eq!(description.kind(), QueryExecutionKind::Maintenance);
    }

    #[tokio::test]
    async fn optional_diagnostic_vector_never_exceeds_count_budget() {
        let (_root, scope) = scope();
        let limits = PreparationLimits::new(
            crate::observation::PreparationCountLimits::new(
                NonZeroU32::new(8).unwrap(),
                NonZeroUsize::new(8).unwrap(),
                NonZeroUsize::new(8).unwrap(),
                NonZeroUsize::new(1).unwrap(),
            ),
            crate::observation::PreparationByteLimits::new(
                NonZeroUsize::new(64).unwrap(),
                NonZeroUsize::new(64).unwrap(),
                NonZeroUsize::new(256).unwrap(),
            ),
            Instant::now() + Duration::from_secs(1),
        );
        let driver = PreparationDriver::new(
            Arc::new(OptionalFailureObservation(
                SourceErrorKind::OptionalUnavailable,
            )),
            Arc::new(PendingNegotiation),
            limits,
        );
        let description = driver
            .prepare(&mut TwoOptionalCompiler, &scope)
            .await
            .unwrap();
        assert_eq!(description.kind(), QueryExecutionKind::Maintenance);
    }

    #[tokio::test]
    async fn oscillating_negotiation_is_rejected() {
        let (_root, scope) = scope();
        let driver = PreparationDriver::new(
            Arc::new(PendingObservation),
            Arc::new(SequenceNegotiation(Mutex::new(VecDeque::from([
                Tiny(1),
                Tiny(2),
                Tiny(1),
            ])))),
            limits(Instant::now() + Duration::from_secs(1)),
        );
        let error = driver
            .prepare(&mut NegotiationCompiler, &scope)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), PreparationErrorKind::NonConvergent);
    }
}
