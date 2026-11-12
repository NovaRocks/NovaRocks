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

//! The original split delivery input and its retained Native payload.

use super::split::{
    BoundSplitRunner, SplitPayloadRegistration, SplitPump, SplitRecordError, SplitRetiredDelivery,
    SplitRunnerPublication,
};
use crate::{
    control_runtime::actor::{FrontendActor, FrontendInput},
    task_execution::error::TaskExecutionError,
};
use novarocks_attempt_protocol::{
    input::{AttemptInput, ExecutorFact, SplitDeliveryRef},
    start::PayloadRef,
};
use novarocks_control_runtime::{
    actor_turn::{ActorTurnError, StandingInputObligation},
    completion::ReportSlot,
    exit_custody::{ExitCustody, ExitCustodyPhase},
    exit_work::{ExitWorkError, PanicPayload, PanicSink, run_future},
    obligation::WorkRetirementObserver,
    reported_work::ReportedWork,
    transaction::ScopeHandle,
};
use novarocks_control_runtime_core::identity::{ConsumerId, InputId};
use novarocks_coordination_contract::AttemptActionId;
use std::{
    any::Any,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::{
    runtime::Handle,
    task::{JoinError, JoinHandle},
};

type Original = Box<dyn Any + Send>;
type DeliveryInput = StandingInputObligation<FrontendActor>;

#[derive(Debug)]
pub(crate) enum SplitPayloadError {
    DuplicateReference,
    MissingReference,
    ReferenceExhausted,
}

/// One exclusive borrow of the original Native request and admission maps.
/// These methods do not grant Work or create another payload allocation.
pub(crate) trait SplitPayloadStore {
    /// Commit both records for the same new reference, using the original
    /// backing quote. Full or error leaves both maps unchanged.
    fn install(
        &mut self,
        original: &SplitPayloadRegistration,
    ) -> Result<Option<PayloadRef>, SplitPayloadError>;

    /// None retains both records while the original admission claim, Native
    /// send aliases or their returns are live. On success the returned owner
    /// contains the original request alias and pin, dropping the pin last.
    fn detach(&mut self, reference: PayloadRef) -> Result<Option<Original>, SplitPayloadError>;
}

#[derive(Debug)]
#[allow(
    dead_code,
    reason = "The split owner retains the original payload, runtime or driver failure."
)]
pub(crate) enum SplitSourceError {
    Input(ActorTurnError),
    Record(SplitRecordError),
    Payload(SplitPayloadError),
    WrongDelivery,
    WrongRunner,
    UnresolvedRunner,
}

impl From<ActorTurnError> for SplitSourceError {
    fn from(error: ActorTurnError) -> Self {
        Self::Input(error)
    }
}

impl From<SplitRecordError> for SplitSourceError {
    fn from(error: SplitRecordError) -> Self {
        Self::Record(error)
    }
}

impl From<SplitPayloadError> for SplitSourceError {
    fn from(error: SplitPayloadError) -> Self {
        Self::Payload(error)
    }
}

impl std::fmt::Display for SplitSourceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "original split source: {self:?}")
    }
}

impl std::error::Error for SplitSourceError {}

enum Runner {
    Dormant,
    Live {
        start: Option<AttemptActionId>,
        custody: ExitCustody,
    },
    Retired,
}

struct Installed {
    delivery: SplitDeliveryRef,
    reference: PayloadRef,
    published: bool,
}

struct Disposal {
    // The producer's alias exits before the Native alias and its pin.
    _delivery: Original,
    _priced_payload: Option<Original>,
}

/// The common host visits this source once per paid unit. Holding the current
/// installation until disposal transfer prevents a second delivery from
/// entering Core while the preceding original payload still needs a return.
pub(crate) struct SplitSource {
    original: Option<Arc<SplitPump>>,
    publication: Option<Arc<SplitRunnerPublication>>,
    input: Option<DeliveryInput>,
    runner: Runner,
    stop_requested: bool,
    installed: Option<Installed>,
    disposal: Option<SplitRetiredDelivery>,
}

impl SplitSource {
    pub(crate) fn new(
        original: Arc<SplitPump>,
        input: DeliveryInput,
        publication: Arc<SplitRunnerPublication>,
    ) -> Self {
        Self {
            original: Some(original),
            publication: Some(publication),
            input: Some(input),
            runner: Runner::Dormant,
            stop_requested: false,
            installed: None,
            disposal: None,
        }
    }

    pub(crate) fn original(&self) -> Option<&Arc<SplitPump>> {
        self.original.as_ref()
    }

    pub(crate) fn bind_runner(
        &mut self,
        start: AttemptActionId,
        custody: ExitCustody,
    ) -> Result<BoundSplitRunner, SplitSourceError> {
        let original = self
            .original
            .as_ref()
            .ok_or(SplitSourceError::WrongRunner)?;
        let publication = self
            .publication
            .as_ref()
            .ok_or(SplitSourceError::WrongRunner)?;
        if self.stop_requested || !matches!(self.runner, Runner::Dormant) {
            return Err(SplitSourceError::WrongRunner);
        }
        let runner = original.bind_runner(start, custody.clone(), publication.clone());
        self.runner = Runner::Live {
            start: Some(start),
            custody,
        };
        Ok(runner)
    }

    pub(crate) fn dormant_cleanup_ready(&self) -> bool {
        self.stop_requested && matches!(self.runner, Runner::Dormant)
    }

    /// Bind only when Start cannot still launch. The common Source supplies
    /// an independently admitted Cleanup job and joins this exact capture.
    pub(crate) fn bind_dormant_cleanup(
        &mut self,
        custody: ExitCustody,
    ) -> Result<BoundSplitCleanup, SplitSourceError> {
        if !self.dormant_cleanup_ready() {
            return Err(SplitSourceError::WrongRunner);
        }
        let original = self
            .original
            .as_ref()
            .ok_or(SplitSourceError::WrongRunner)?;
        if original.has_started() {
            return Err(SplitSourceError::WrongRunner);
        }
        self.runner = Runner::Live {
            start: None,
            custody: custody.clone(),
        };
        Ok(BoundSplitCleanup {
            original: original.clone(),
            custody,
        })
    }

    /// Call after actual Join/report retirement and reaping the same Work
    /// header. A fenced Start has retired its work, but the original opened
    /// provider still owes its dormant close.
    pub(crate) fn runner_retired(
        &mut self,
        start: Option<AttemptActionId>,
    ) -> Result<(), SplitSourceError> {
        let Runner::Live {
            start: expected,
            custody,
        } = &self.runner
        else {
            return Err(SplitSourceError::WrongRunner);
        };
        if start != *expected || custody.phase() != ExitCustodyPhase::Retired {
            return Err(SplitSourceError::WrongRunner);
        }
        let original = self
            .original
            .as_ref()
            .ok_or(SplitSourceError::WrongRunner)?;
        self.runner = if original.deliveries().producer_closed() {
            Runner::Retired
        } else if start.is_some() && !original.has_started() {
            Runner::Dormant
        } else {
            return Err(SplitSourceError::UnresolvedRunner);
        };
        Ok(())
    }

    pub(crate) fn owns_input(&self, input: InputId) -> bool {
        self.input.as_ref().is_some_and(|position| {
            position.in_flight().expect("original split input") == Some(input)
        }) || self
            .publication
            .as_ref()
            .is_some_and(|publication| publication.owns_input(input))
    }

    pub(crate) fn is_drained(&self) -> bool {
        self.original.is_none()
    }

    /// The caller supplies one paid host visit and a real release position.
    /// `core_drained` covers pending Core notices and diagnostic/failure
    /// consumers. A cancellation request or a returned runner is insufficient.
    pub(crate) fn progress(
        &mut self,
        failed: bool,
        closing: bool,
        core_drained: bool,
        release_ready: bool,
        payloads: &mut impl SplitPayloadStore,
        dispose: impl FnOnce(Original),
    ) -> Result<bool, SplitSourceError> {
        let Some(original) = self.original.as_ref() else {
            return Ok(false);
        };
        if (failed || closing) && !self.stop_requested {
            self.stop_requested = true;
            original.request_stop();
            return Ok(true);
        }
        let retired = matches!(self.runner, Runner::Retired);
        if let Some(input) = self.input.as_mut() {
            if input.in_flight()?.is_some() {
                if failed {
                    match input.take_failed_role_payload() {
                        Ok(Some(neutral)) => {
                            neutral.dispose()?;
                            return Ok(true);
                        }
                        Ok(None) | Err(ActorTurnError::InputAuthorMismatch) => {}
                        Err(error) => return Err(error.into()),
                    }
                }
                // Only this occurrence's neutral projection has exited. The
                // priced payload remains retained through its Native aliases.
                return Ok(input.retire_ready_after_exit()?);
            }
            if retired {
                input.close_after_exit()?;
                self.input = None;
                return Ok(true);
            }
        }
        if retired
            && let Some(publication) = &self.publication
            && publication.retire_after_runner_exit(failed)?
        {
            self.publication = None;
            return Ok(true);
        }

        let records = original.deliveries();
        if release_ready && self.disposal.is_none() {
            self.disposal = records.take_retired_one().or_else(|| {
                (retired && core_drained)
                    .then(|| records.take_remaining_after_close())
                    .flatten()
            });
            if self.disposal.is_some() {
                return Ok(true);
            }
        }
        if let Some(held) = &self.disposal {
            if !release_ready {
                return Ok(false);
            }
            let reference = match (held.payload, &self.installed) {
                (Some(reference), Some(installed)) if reference == installed.reference => {
                    Some(reference)
                }
                // If the registry refused after the Native store accepted
                // the price, retain and unwind that same installation.
                (None, Some(installed)) if !installed.published && retired && core_drained => {
                    Some(installed.reference)
                }
                (None, None) => None,
                _ => return Err(SplitSourceError::WrongDelivery),
            };
            let priced_payload = if let Some(reference) = reference {
                let Some(priced) = payloads.detach(reference)? else {
                    return Ok(false);
                };
                self.installed = None;
                Some(priced)
            } else {
                None
            };
            let original = self.disposal.take().expect("original split disposal");
            dispose(Box::new(Disposal {
                _delivery: original.original,
                _priced_payload: priced_payload,
            }));
            return Ok(true);
        }
        if retired {
            if core_drained
                && release_ready
                && self.input.is_none()
                && self.publication.is_none()
                && self.installed.is_none()
            {
                dispose(Box::new(
                    self.original.take().expect("original split owner"),
                ));
                return Ok(true);
            }
            return Ok(false);
        }
        if self.stop_requested {
            return Ok(false);
        }

        if let Some(installed) = self.installed.as_mut() {
            if installed.published {
                return Ok(false);
            }
            let Some(fact @ ExecutorFact::PumpTaskUpdate { delivery, .. }) =
                records.next_input(original.reference())?
            else {
                return Err(SplitSourceError::WrongDelivery);
            };
            if delivery != installed.delivery {
                return Err(SplitSourceError::WrongDelivery);
            }
            let input = self
                .input
                .as_ref()
                .expect("original split delivery position");
            match input.fill(FrontendInput::Attempt(Box::new(AttemptInput::Executor(
                fact,
            )))) {
                Ok(_) => {
                    installed.published = true;
                    records.input_published(delivery)?;
                    return Ok(true);
                }
                Err((
                    ActorTurnError::StandingInputBusy
                    | ActorTurnError::ActorFailed
                    | ActorTurnError::Closed,
                    _,
                )) => return Ok(false),
                Err((error, _)) => return Err(error.into()),
            }
        }
        let Some(original) = records.next_unregistered() else {
            return Ok(false);
        };
        let Some(reference) = payloads.install(&original)? else {
            return Ok(false);
        };
        self.installed = Some(Installed {
            delivery: original.delivery,
            reference,
            published: false,
        });
        records.register_payload(original.delivery, reference)?;
        Ok(true)
    }
}

pub(crate) struct BoundSplitCleanup {
    original: Arc<SplitPump>,
    custody: ExitCustody,
}

impl BoundSplitCleanup {
    pub(crate) async fn run(self) -> Result<(), TaskExecutionError> {
        self.original.close_unstarted(self.custody).await
    }
}

type CleanupResult = Result<(), TaskExecutionError>;

#[derive(Debug)]
#[allow(
    dead_code,
    reason = "Split failure transfers the original Join and panic or exit refusal together."
)]
pub(crate) enum SplitCleanupFailure {
    Join(JoinError, Option<PanicPayload>),
    Work(ExitWorkError),
}

/// The dormant provider close has no Core effect ticket. Its original
/// Cleanup Work and Join are still retired by the same attempt Source.
pub(crate) struct OriginalSplitCleanup {
    join: Option<JoinHandle<Result<CleanupResult, ExitWorkError>>>,
    custody: ExitCustody,
    header: WorkRetirementObserver,
    report: ReportSlot,
    observer: ConsumerId,
    panic: PanicSink,
}

impl OriginalSplitCleanup {
    pub(crate) fn start(
        split: &mut SplitSource,
        executor: &Handle,
        work: ReportedWork,
        observer: ConsumerId,
    ) -> Self {
        let header = work.work().retirement_observer();
        let report = work.report().clone();
        let (custody, exit) = ExitCustody::from_admitted(work);
        let original = split
            .bind_dormant_cleanup(custody.clone())
            .expect("original dormant split cleanup");
        let panic = PanicSink::new();
        let join = executor.spawn(run_future(exit, original.run(), panic.clone()));
        Self {
            join: Some(join),
            custody,
            header,
            report,
            observer,
            panic,
        }
    }

    pub(crate) fn poll_retired(
        &mut self,
        scope: &ScopeHandle,
        cx: &mut Context<'_>,
    ) -> Poll<Result<CleanupResult, SplitCleanupFailure>> {
        let Some(join) = &mut self.join else {
            return Poll::Pending;
        };
        let outcome = std::task::ready!(Pin::new(join).poll(cx));
        self.join = None;
        let original = match outcome {
            Ok(Ok(original)) => original,
            Ok(Err(error)) => return Poll::Ready(Err(SplitCleanupFailure::Work(error))),
            Err(error) => {
                return Poll::Ready(Err(SplitCleanupFailure::Join(error, self.panic.take())));
            }
        };
        assert!(
            self.custody
                .actual_exit_ready()
                .expect("original dormant cleanup exit")
        );
        self.report
            .consume(self.observer)
            .expect("original dormant cleanup report");
        assert!(
            self.custody
                .try_retire()
                .expect("original dormant cleanup retirement")
        );
        assert!(
            scope
                .reap_observed_header(&self.header)
                .expect("original dormant cleanup header")
        );
        Poll::Ready(Ok(original))
    }
}
