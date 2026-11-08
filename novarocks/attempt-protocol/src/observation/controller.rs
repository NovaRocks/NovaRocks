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

//! One deterministic covered subscription. Directives are passive decisions;
//! the original Source owns every RPC, credit, result and actual exit. Old
//! generations may still contribute positive facts through their immutable
//! per-frame opening witness, without renewing the current generation.

use super::{
    opening::{
        COVERED_LIVENESS_BUDGET, COVERED_OPENING_BUDGET, OpeningEligibility, opening_eligibility,
    },
    read_state::{ReadConfirmation, ReadStateDecision},
    retry::{CoveredRetryBudget, RetryDisposition},
};
use crate::{
    error::TaskExecutionError,
    ids::IdSeed,
    input::{
        ObservationEvent, ObservationFact, ObservationFailure, ObservationFailureKind,
        ObservationOpening, ReadProgressSnapshot, ReadStateSnapshot, ReceivedObservation,
        SubscriptionPhaseSnapshot,
    },
};
use novarocks_coordination_contract::{AttemptRequestToken, MonotonicInstant};
use novarocks_execution_contract::ParticipationRef;
use std::num::NonZeroU64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SubscriptionStatus {
    Dormant,
    AwaitStart,
    Opening,
    Live,
    Backoff { until: MonotonicInstant },
    BudgetExhausted,
    ConnectionLossExhausted,
    Fatal(ObservationFailure),
    Stopped,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SubscriptionDirective {
    Open { generation: NonZeroU64 },
    Close { generation: NonZeroU64 },
    Confirm(ReadConfirmation),
}
/// A passive observation whose actual opening has passed this generation
/// policy. Entity/status/cursor acceptance still belongs to the original
/// execution owner; this value grants no resource, Source or host access right.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AcceptedObservation(ReceivedObservation);
impl AcceptedObservation {
    pub(crate) fn into_observation(self) -> ReceivedObservation {
        self.0
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ObservationDisposition {
    ApplyEntity(AcceptedObservation),
    Ignore,
}

#[derive(Debug)]
pub struct CoveredSubscription {
    participation: ParticipationRef,
    enabled: bool,
    status: SubscriptionStatus,
    last_generation: u64,
    started_at: Option<MonotonicInstant>,
    opened_at: Option<MonotonicInstant>,
    last_liveness_at: Option<MonotonicInstant>,
    bookmark: u64,
    catch_up_seen: bool,
    deadline: Option<MonotonicInstant>,
    paused_recheck: Option<MonotonicInstant>,
    confirmation: Option<ReadConfirmation>,
    confirmation_deferred: bool,
    resume_revision: u64,
    close_needed: bool,
    close_issued: bool,
    actual_exited: bool,
    retry: CoveredRetryBudget,
    last_failure: Option<ObservationFailure>,
}
impl CoveredSubscription {
    pub fn new(
        participation: ParticipationRef,
        seed: IdSeed,
        error_budget: u32,
    ) -> Result<Self, TaskExecutionError> {
        let participant = u128::from_be_bytes(participation.backend_process_id().to_bytes());
        Ok(Self {
            participation,
            enabled: false,
            status: SubscriptionStatus::Dormant,
            last_generation: 0,
            started_at: None,
            opened_at: None,
            last_liveness_at: None,
            bookmark: 0,
            catch_up_seen: false,
            deadline: None,
            paused_recheck: None,
            confirmation: None,
            confirmation_deferred: false,
            resume_revision: 0,
            close_needed: false,
            close_issued: false,
            actual_exited: true,
            retry: CoveredRetryBudget::new(error_budget, seed, participant)?,
            last_failure: None,
        })
    }
    pub const fn status(&self) -> SubscriptionStatus {
        self.status
    }
    pub const fn generation(&self) -> Option<NonZeroU64> {
        NonZeroU64::new(self.last_generation)
    }
    pub const fn last_failure(&self) -> Option<ObservationFailure> {
        self.last_failure
    }
    pub fn activate(&mut self) {
        self.enabled = true;
    }
    fn active(&self) -> bool {
        matches!(
            self.status,
            SubscriptionStatus::AwaitStart | SubscriptionStatus::Opening | SubscriptionStatus::Live
        )
    }
    fn final_status(&self) -> bool {
        matches!(
            self.status,
            SubscriptionStatus::BudgetExhausted
                | SubscriptionStatus::ConnectionLossExhausted
                | SubscriptionStatus::Fatal(_)
                | SubscriptionStatus::Stopped
        )
    }
    fn request_close(&mut self) {
        self.close_needed |= self.last_generation != 0 && !self.actual_exited && !self.close_issued;
        self.deadline = None;
        self.paused_recheck = None;
        self.confirmation = None;
        self.confirmation_deferred = false;
    }
    pub fn stop(&mut self) {
        self.enabled = false;
        self.status = SubscriptionStatus::Stopped;
        self.request_close();
    }
    /// Explicit reconciliation bypasses the previous stream's ordinary-break
    /// budget settlement. It cannot revive an exhausted or final subscription.
    pub fn reconcile(&mut self, now: MonotonicInstant) {
        if self.final_status() || self.last_generation == 0 {
            return;
        }
        self.status = SubscriptionStatus::Backoff { until: now };
        self.request_close();
    }
    pub fn observe_actual_exit(
        &mut self,
        generation: NonZeroU64,
    ) -> Result<(), TaskExecutionError> {
        if generation.get() > self.last_generation {
            return Err(TaskExecutionError::UnknownOperation);
        }
        if generation.get() == self.last_generation {
            self.actual_exited = true;
            self.close_needed = false;
        }
        Ok(())
    }
    /// Only an actual remaining directive supplies an immediate continuation.
    /// While a closed job is still live, no expired backoff timer is replayed.
    pub fn next_deadline(&self, now: MonotonicInstant) -> Option<MonotonicInstant> {
        if self.close_needed {
            return Some(now);
        }
        if !self.enabled {
            return None;
        }
        match self.status {
            SubscriptionStatus::Dormant => Some(now),
            SubscriptionStatus::Backoff { until } if self.actual_exited => Some(until),
            SubscriptionStatus::Opening | SubscriptionStatus::Live
                if self.confirmation.is_none() && !self.confirmation_deferred =>
            {
                self.deadline
            }
            _ => None,
        }
    }
    /// The caller prepays the actual effect/turn unit before this transition.
    /// minimum_generation comes from the latest applied subscription request;
    /// an opening failure without frames still consumes its allocated number.
    pub fn drive(
        &mut self,
        now: MonotonicInstant,
        minimum_generation: NonZeroU64,
        confirmation_token: Option<AttemptRequestToken>,
    ) -> Result<Option<SubscriptionDirective>, TaskExecutionError> {
        if self.close_needed {
            self.close_needed = false;
            self.close_issued = true;
            return Ok(Some(SubscriptionDirective::Close {
                generation: self.generation().expect("close has original generation"),
            }));
        }
        if !self.enabled {
            return Ok(None);
        }
        if matches!(self.status, SubscriptionStatus::Dormant)
            || matches!(self.status, SubscriptionStatus::Backoff { until } if self.actual_exited && now.has_reached(until))
        {
            let next = self.last_generation.checked_add(1).ok_or_else(|| {
                TaskExecutionError::Schedule("covered stream generation exhausted".into())
            })?;
            let generation =
                NonZeroU64::new(next.max(minimum_generation.get())).expect("positive generation");
            self.retry.begin_generation(generation)?;
            self.last_generation = generation.get();
            self.started_at = None;
            self.opened_at = None;
            self.last_liveness_at = None;
            self.bookmark = 0;
            self.catch_up_seen = false;
            self.deadline = None;
            self.paused_recheck = None;
            self.confirmation = None;
            self.confirmation_deferred = false;
            self.resume_revision = 0;
            self.close_issued = false;
            self.actual_exited = false;
            self.status = SubscriptionStatus::AwaitStart;
            return Ok(Some(SubscriptionDirective::Open { generation }));
        }
        if self.active()
            && self.confirmation.is_none()
            && !self.confirmation_deferred
            && self
                .deadline
                .is_some_and(|deadline| now.has_reached(deadline))
            && let Some(token) = confirmation_token
        {
            let request = ReadConfirmation {
                token,
                participation: self.participation,
                generation: self.generation().expect("active generation"),
                requested_at: now,
            };
            self.confirmation = Some(request);
            return Ok(Some(SubscriptionDirective::Confirm(request)));
        }
        Ok(None)
    }
    fn remember_opening(&mut self, opening: ObservationOpening) -> Result<(), TaskExecutionError> {
        if self
            .started_at
            .is_some_and(|started| started != opening.started_at)
            || opening.opened_at.is_some_and(|opened| {
                opened < opening.started_at || self.opened_at.is_some_and(|held| held != opened)
            })
        {
            return Err(TaskExecutionError::Schedule(
                "covered generation changed its immutable actual opening facts".into(),
            ));
        }
        self.started_at = Some(opening.started_at);
        if self.opened_at.is_none()
            && let Some(opened) = opening.opened_at
        {
            self.opened_at = Some(opened);
            self.last_liveness_at = Some(opened);
            self.deadline = Some(opened.saturating_add(COVERED_LIVENESS_BUDGET));
            self.status = SubscriptionStatus::Live;
        } else if self.opened_at.is_none() {
            self.deadline = Some(opening.started_at.saturating_add(COVERED_OPENING_BUDGET));
            self.status = SubscriptionStatus::Opening;
        }
        Ok(())
    }
    pub fn observe_phase(
        &mut self,
        phase: SubscriptionPhaseSnapshot,
    ) -> Result<(), TaskExecutionError> {
        if phase.participation != self.participation
            || phase.generation.get() > self.last_generation
        {
            return Err(TaskExecutionError::UnknownOperation);
        }
        if phase.generation.get() != self.last_generation || !self.active() {
            return Ok(());
        }
        self.remember_opening(ObservationOpening {
            started_at: phase.opening_started_at,
            opened_at: phase.opened_at,
        })?;
        if phase.opened_at.is_some_and(|opened| {
            opened
                > phase
                    .opening_started_at
                    .saturating_add(COVERED_OPENING_BUDGET)
        }) {
            self.failed(phase.opened_at.expect("late successful open"), false);
        }
        Ok(())
    }
    fn failed(&mut self, at: MonotonicInstant, observed_connection_loss: bool) {
        let Some(generation) = self.generation() else {
            return;
        };
        let disposition = self.retry.failed(generation, at, observed_connection_loss);
        self.status = match disposition {
            RetryDisposition::Ignored => return,
            RetryDisposition::Backoff { until } => SubscriptionStatus::Backoff { until },
            RetryDisposition::Exhausted {
                connection_loss: true,
            } => SubscriptionStatus::ConnectionLossExhausted,
            RetryDisposition::Exhausted {
                connection_loss: false,
            } => SubscriptionStatus::BudgetExhausted,
        };
        self.request_close();
    }
    /// This check precedes ALL execution/entity/cursor/coverage application.
    /// Timing on an old legal generation is still sufficient for its positive
    /// facts: the Source retains that immutable record until its inputs retire.
    pub fn observe(
        &mut self,
        observation: ReceivedObservation,
    ) -> Result<ObservationDisposition, TaskExecutionError> {
        if observation.participation != self.participation
            || observation.generation.get() > self.last_generation
        {
            return Err(TaskExecutionError::UnknownOperation);
        }
        let eligibility = opening_eligibility(&observation)?;
        let current = observation.generation.get() == self.last_generation;
        if eligibility == OpeningEligibility::TimedOut {
            if current && self.active() {
                self.failed(observation.received_at, false);
            }
            return Ok(ObservationDisposition::Ignore);
        }
        if current && self.active() {
            self.remember_opening(observation.opening)?;
        }
        match &observation.event {
            ObservationEvent::Fact { fact, .. } => {
                let fresh = match fact {
                    ObservationFact::CatchUpComplete { generation, .. }
                        if current && self.active() =>
                    {
                        if *generation != observation.generation.get() {
                            return Err(TaskExecutionError::Schedule(
                                "catch-up marker changed stream generation".into(),
                            ));
                        }
                        let fresh = !self.catch_up_seen;
                        self.catch_up_seen = true;
                        fresh
                    }
                    ObservationFact::Bookmark {
                        generation,
                        sequence,
                        ..
                    } if current && self.active() => {
                        if *generation != observation.generation.get() {
                            return Err(TaskExecutionError::Schedule(
                                "bookmark changed stream generation".into(),
                            ));
                        }
                        if *sequence > self.bookmark {
                            self.bookmark = *sequence;
                            true
                        } else {
                            false
                        }
                    }
                    _ => false,
                };
                if fresh {
                    self.retry.liveness(observation.generation);
                    if self
                        .last_liveness_at
                        .is_none_or(|last| observation.received_at >= last)
                    {
                        self.last_liveness_at = Some(observation.received_at);
                        self.deadline = Some(
                            observation
                                .received_at
                                .saturating_add(COVERED_LIVENESS_BUDGET),
                        );
                        self.paused_recheck = None;
                    }
                    self.confirmation_deferred = false;
                }
                Ok(ObservationDisposition::ApplyEntity(AcceptedObservation(
                    observation,
                )))
            }
            ObservationEvent::Closed => {
                if current && self.active() {
                    self.failed(observation.received_at, false);
                }
                Ok(ObservationDisposition::Ignore)
            }
            ObservationEvent::Failed(failure) => {
                if current && self.active() {
                    self.last_failure = Some(*failure);
                    match failure.kind {
                        ObservationFailureKind::RecoverableTransport {
                            observed_connection_loss,
                        } => self.failed(observation.received_at, observed_connection_loss),
                        _ => {
                            self.status = SubscriptionStatus::Fatal(*failure);
                            self.request_close();
                        }
                    }
                }
                Ok(ObservationDisposition::Ignore)
            }
        }
    }
    pub fn observe_read_state(
        &mut self,
        snapshot: ReadStateSnapshot,
    ) -> Result<(), TaskExecutionError> {
        if snapshot.participation != self.participation
            || snapshot.generation.get() > self.last_generation
        {
            return Err(TaskExecutionError::UnknownOperation);
        }
        if snapshot.generation.get() != self.last_generation || !self.active() {
            return Ok(());
        }
        let request = self
            .confirmation
            .ok_or(TaskExecutionError::UnknownOperation)?;
        let decision = request.evaluate(snapshot)?;
        self.confirmation = None;
        // Positive phase/marker facts accepted after this snapshot cannot be
        // revoked by its delayed reply (including equal-clock-tick ordering).
        if self
            .last_liveness_at
            .is_some_and(|last| last > snapshot.last_liveness_at)
            || (self.opened_at.is_some() && snapshot.opened_at.is_none())
        {
            return Ok(());
        }
        match decision {
            ReadStateDecision::Incomplete => {
                self.confirmation_deferred = true;
                self.deadline = None;
            }
            ReadStateDecision::Closed => {
                self.deadline = None;
                self.paused_recheck = None;
            }
            ReadStateDecision::AwaitStart => {}
            ReadStateDecision::Opening { deadline } => {
                self.started_at = snapshot.opening_started_at;
                self.status = SubscriptionStatus::Opening;
                self.deadline = Some(deadline);
            }
            ReadStateDecision::OpeningExpired => self.failed(snapshot.snapshot_at, false),
            ReadStateDecision::Live {
                deadline,
                had_liveness,
                reading_paused,
            } => {
                self.remember_opening(ObservationOpening {
                    started_at: snapshot.opening_started_at.expect("validated opening"),
                    opened_at: snapshot.opened_at,
                })?;
                if had_liveness {
                    self.retry.liveness(snapshot.generation);
                }
                self.last_liveness_at = Some(snapshot.last_liveness_at);
                let still_paused =
                    reading_paused && snapshot.resume_revision >= self.resume_revision;
                self.resume_revision = self.resume_revision.max(snapshot.resume_revision);
                self.paused_recheck = still_paused.then_some(deadline);
                self.deadline = Some(if still_paused {
                    snapshot.snapshot_at.saturating_add(COVERED_LIVENESS_BUDGET)
                } else {
                    deadline
                });
                self.confirmation_deferred = false;
            }
            ReadStateDecision::SilenceExpired { had_liveness } => {
                if had_liveness {
                    self.retry.liveness(snapshot.generation);
                }
                self.failed(snapshot.snapshot_at, false);
            }
        }
        Ok(())
    }
    /// This is only a resume hint. A fresh consistent confirmation still owns
    /// the silence decision; no pause duration or liveness is invented here.
    pub fn observe_read_progress(
        &mut self,
        progress: ReadProgressSnapshot,
    ) -> Result<(), TaskExecutionError> {
        if progress.participation != self.participation
            || progress.generation.get() > self.last_generation
        {
            return Err(TaskExecutionError::UnknownOperation);
        }
        if progress.generation.get() != self.last_generation || !self.active() {
            return Ok(());
        }
        if progress.resume_revision.get() <= self.resume_revision {
            return Ok(());
        }
        self.resume_revision = progress.resume_revision.get();
        if let Some(deadline) = self.paused_recheck.take() {
            self.deadline = Some(deadline);
        }
        if self.confirmation_deferred {
            self.confirmation_deferred = false;
            self.deadline = Some(progress.resumed_at);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input::{ReadPauseCoverage, ReceptionCoverage};
    use std::time::Duration;
    fn at(ms: u64) -> MonotonicInstant {
        MonotonicInstant::from_origin(Duration::from_millis(ms))
    }
    fn generation(value: u64) -> NonZeroU64 {
        NonZeroU64::new(value).unwrap()
    }
    fn token(value: u64) -> AttemptRequestToken {
        AttemptRequestToken::new(generation(value))
    }
    fn start(limit: u32) -> CoveredSubscription {
        let task = crate::test_support::remote(1);
        let mut controller =
            CoveredSubscription::new(task.participation(), IdSeed::new(100, 1), limit).unwrap();
        controller.activate();
        assert_eq!(
            controller.drive(at(0), generation(1), None).unwrap(),
            Some(SubscriptionDirective::Open {
                generation: generation(1)
            })
        );
        controller
    }
    fn phase(
        controller: &CoveredSubscription,
        started: u64,
        opened: Option<u64>,
    ) -> SubscriptionPhaseSnapshot {
        SubscriptionPhaseSnapshot {
            participation: controller.participation,
            generation: controller.generation().unwrap(),
            opening_started_at: at(started),
            opened_at: opened.map(at),
        }
    }
    fn opened() -> CoveredSubscription {
        let mut controller = start(8);
        controller
            .observe_phase(phase(&controller, 0, Some(1000)))
            .unwrap();
        controller
    }
    fn confirm(controller: &mut CoveredSubscription, now: u64, value: u64) -> ReadConfirmation {
        let Some(SubscriptionDirective::Confirm(request)) = controller
            .drive(at(now), generation(1), Some(token(value)))
            .unwrap()
        else {
            panic!("one prepaid confirmation");
        };
        request
    }
    fn snapshot(
        request: ReadConfirmation,
        at_ms: u64,
        started: u64,
        open: Option<u64>,
        base: u64,
    ) -> ReadStateSnapshot {
        ReadStateSnapshot {
            request: request.token,
            participation: request.participation,
            generation: request.generation,
            snapshot_at: at(at_ms),
            opening_started_at: Some(at(started)),
            opened_at: open.map(at),
            had_liveness: false,
            last_liveness_at: at(base),
            pauses: ReadPauseCoverage {
                from: at(base),
                through: at(at_ms),
                paused: Duration::ZERO,
            },
            reading_paused: false,
            resume_revision: 0,
            closed: false,
            received_through_snapshot: ReceptionCoverage::Complete,
        }
    }
    fn frame(
        controller: &CoveredSubscription,
        at_ms: u64,
        fact: ObservationFact,
    ) -> ReceivedObservation {
        ReceivedObservation {
            participation: controller.participation,
            generation: controller.generation().unwrap(),
            received_at: at(at_ms),
            opening: ObservationOpening {
                started_at: controller.started_at.unwrap_or(at(0)),
                opened_at: controller.opened_at,
            },
            event: ObservationEvent::Fact {
                source_revision: None,
                fact,
            },
        }
    }
    fn close_and_reopen(controller: &mut CoveredSubscription, now: u64) {
        let old = controller.generation().unwrap();
        assert_eq!(
            controller.drive(at(now), generation(1), None).unwrap(),
            Some(SubscriptionDirective::Close { generation: old })
        );
        assert_eq!(controller.next_deadline(at(now)), None);
        assert!(
            controller
                .drive(at(now + 1000), generation(1), None)
                .unwrap()
                .is_none()
        );
        controller.observe_actual_exit(old).unwrap();
        assert_eq!(
            controller
                .drive(at(now + 2000), generation(1), None)
                .unwrap(),
            Some(SubscriptionDirective::Open {
                generation: generation(old.get() + 1)
            })
        );
    }
    #[test]
    fn lazy_open_does_not_spend_opening_budget_and_success_starts_a_new_live_window() {
        let mut controller = start(8);
        assert_eq!(controller.next_deadline(at(10_000)), None);
        assert!(
            controller
                .drive(at(10_000), generation(1), Some(token(1)))
                .unwrap()
                .is_none()
        );
        controller
            .observe_phase(phase(&controller, 10_000, None))
            .unwrap();
        assert_eq!(controller.next_deadline(at(10_000)), Some(at(15_000)));
        controller
            .observe_phase(phase(&controller, 10_000, Some(14_000)))
            .unwrap();
        assert_eq!(controller.next_deadline(at(15_000)), Some(at(19_000)));
        assert!(
            controller
                .drive(at(18_999), generation(1), Some(token(1)))
                .unwrap()
                .is_none()
        );
        confirm(&mut controller, 19_000, 1);
    }
    #[test]
    fn blackhole_openings_exhaust_at_exact_budget_without_timer_spin_or_early_reopen() {
        let mut controller = start(2);
        controller
            .observe_phase(phase(&controller, 0, None))
            .unwrap();
        let request = confirm(&mut controller, 5000, 1);
        controller
            .observe_read_state(snapshot(request, 5000, 0, None, 0))
            .unwrap();
        assert!(matches!(
            controller.status(),
            SubscriptionStatus::Backoff { .. }
        ));
        close_and_reopen(&mut controller, 5000);
        controller
            .observe_phase(phase(&controller, 7000, None))
            .unwrap();
        let request = confirm(&mut controller, 12_000, 2);
        controller
            .observe_read_state(snapshot(request, 12_000, 7000, None, 7000))
            .unwrap();
        assert_eq!(controller.status(), SubscriptionStatus::BudgetExhausted);
        controller
            .observe_phase(phase(&controller, 7000, Some(8000)))
            .unwrap();
        assert_eq!(controller.status(), SubscriptionStatus::BudgetExhausted);
        assert_eq!(controller.retry.failures(), 2);
    }
    #[test]
    fn per_frame_late_open_is_declined_before_entity_adoption_and_charged_once() {
        let mut controller = start(8);
        controller
            .observe_phase(phase(&controller, 0, None))
            .unwrap();
        let mut received = frame(
            &controller,
            6100,
            ObservationFact::Unknown(crate::test_support::remote(1).identity()),
        );
        received.opening.opened_at = Some(at(6000));
        assert_eq!(
            controller.observe(received.clone()).unwrap(),
            ObservationDisposition::Ignore
        );
        assert_eq!(controller.retry.failures(), 1);
        assert_eq!(
            controller.observe(received.clone()).unwrap(),
            ObservationDisposition::Ignore
        );
        controller
            .observe_phase(phase(&controller, 0, Some(6000)))
            .unwrap();
        assert_eq!(controller.retry.failures(), 1);
        assert!(matches!(
            controller.status(),
            SubscriptionStatus::Backoff { .. }
        ));
    }
    #[test]
    fn ordinary_duplicate_and_old_generation_frames_never_renew_current_liveness() {
        let mut controller = opened();
        let ordinary = frame(
            &controller,
            4000,
            ObservationFact::Unknown(crate::test_support::remote(1).identity()),
        );
        assert!(matches!(
            controller.observe(ordinary.clone()).unwrap(),
            ObservationDisposition::ApplyEntity(_)
        ));
        assert_eq!(controller.next_deadline(at(4000)), Some(at(6000)));
        let catchup = frame(
            &controller,
            5000,
            ObservationFact::CatchUpComplete {
                generation: 1,
                initial_cut: 0,
            },
        );
        controller.observe(catchup.clone()).unwrap();
        let duplicate = frame(
            &controller,
            7000,
            ObservationFact::CatchUpComplete {
                generation: 1,
                initial_cut: 0,
            },
        );
        controller.observe(duplicate.clone()).unwrap();
        assert_eq!(controller.next_deadline(at(7000)), Some(at(10_000)));
        let old = frame(
            &controller,
            8000,
            ObservationFact::Bookmark {
                generation: 1,
                sequence: 2,
                covered_prefix: 0,
                source_cut: 0,
            },
        );
        controller.observe(old.clone()).unwrap();
        assert_eq!(controller.next_deadline(at(8000)), Some(at(13_000)));
        controller.reconcile(at(9000));
        close_and_reopen(&mut controller, 9000);
        controller
            .observe_phase(phase(&controller, 11_000, Some(12_000)))
            .unwrap();
        assert!(matches!(
            controller.observe(old.clone()).unwrap(),
            ObservationDisposition::ApplyEntity(_)
        ));
        assert_eq!(controller.next_deadline(at(12_000)), Some(at(17_000)));
    }
    #[test]
    fn closed_snapshot_waits_for_common_fifo_close_before_settling_retry_budget() {
        let mut controller = opened();
        let request = confirm(&mut controller, 6000, 1);
        let mut read = snapshot(request, 6000, 0, Some(1000), 1000);
        read.closed = true;
        controller.observe_read_state(read).unwrap();
        assert_eq!(controller.retry.failures(), 0);
        assert_eq!(controller.next_deadline(at(6000)), None);
        let mut closed = frame(
            &controller,
            6500,
            ObservationFact::Unknown(crate::test_support::remote(1).identity()),
        );
        closed.event = ObservationEvent::Closed;
        controller.observe(closed.clone()).unwrap();
        controller.observe(closed.clone()).unwrap();
        assert_eq!(controller.retry.failures(), 1);
    }
    #[test]
    fn paused_confirmation_is_rate_limited_until_real_resume_restores_original_measurement() {
        let mut controller = opened();
        let request = confirm(&mut controller, 6000, 1);
        let mut read = snapshot(request, 6000, 0, Some(1000), 1000);
        read.pauses.paused = Duration::from_millis(100);
        read.reading_paused = true;
        read.resume_revision = 1;
        controller.observe_read_state(read).unwrap();
        assert_eq!(controller.next_deadline(at(6000)), Some(at(11_000)));
        assert!(
            controller
                .drive(at(6100), generation(1), Some(token(2)))
                .unwrap()
                .is_none()
        );
        controller
            .observe_read_progress(ReadProgressSnapshot {
                participation: controller.participation,
                generation: generation(1),
                resumed_at: at(6050),
                resume_revision: generation(2),
            })
            .unwrap();
        assert_eq!(controller.next_deadline(at(6050)), Some(at(6100)));
        let request = confirm(&mut controller, 6100, 2);
        let mut read = snapshot(request, 6100, 0, Some(1000), 1000);
        read.pauses.paused = Duration::from_millis(150);
        read.resume_revision = 2;
        controller.observe_read_state(read).unwrap();
        assert_eq!(controller.next_deadline(at(6100)), Some(at(6150)));
        let request = confirm(&mut controller, 6150, 3);
        let mut read = snapshot(request, 6150, 0, Some(1000), 1000);
        read.pauses.paused = Duration::from_millis(150);
        read.resume_revision = 2;
        controller.observe_read_state(read).unwrap();
        assert!(matches!(
            controller.status(),
            SubscriptionStatus::Backoff { .. }
        ));
    }
    #[test]
    fn resume_before_delayed_same_tick_paused_snapshot_is_not_lost_or_replayed() {
        let mut controller = opened();
        let request = confirm(&mut controller, 6000, 1);
        controller
            .observe_read_progress(ReadProgressSnapshot {
                participation: controller.participation,
                generation: generation(1),
                resumed_at: at(6000),
                resume_revision: generation(2),
            })
            .unwrap();
        let mut read = snapshot(request, 6000, 0, Some(1000), 1000);
        read.pauses.paused = Duration::from_millis(100);
        read.reading_paused = true;
        read.resume_revision = 1;
        controller.observe_read_state(read).unwrap();
        assert_eq!(controller.next_deadline(at(6000)), Some(at(6100)));
        let request = confirm(&mut controller, 6100, 2);
        let mut read = snapshot(request, 6100, 0, Some(1000), 1000);
        read.pauses.paused = Duration::from_millis(200);
        read.reading_paused = true;
        read.resume_revision = 2;
        controller.observe_read_state(read).unwrap();
        assert_eq!(controller.next_deadline(at(6100)), Some(at(11_100)));
        controller
            .observe_read_progress(ReadProgressSnapshot {
                participation: controller.participation,
                generation: generation(1),
                resumed_at: at(6000),
                resume_revision: generation(2),
            })
            .unwrap();
        assert_eq!(controller.next_deadline(at(6100)), Some(at(11_100)));
    }
    #[test]
    fn known_new_liveness_cannot_be_retracted_by_an_older_confirmation() {
        let mut controller = opened();
        let request = confirm(&mut controller, 6000, 1);
        let fresh = frame(
            &controller,
            6500,
            ObservationFact::CatchUpComplete {
                generation: 1,
                initial_cut: 0,
            },
        );
        controller.observe(fresh.clone()).unwrap();
        controller
            .observe_read_state(snapshot(request, 6000, 0, Some(1000), 1000))
            .unwrap();
        assert_eq!(controller.status(), SubscriptionStatus::Live);
        assert_eq!(controller.next_deadline(at(6500)), Some(at(11_500)));
    }
    #[test]
    fn stop_closes_once_and_late_phase_never_revives_the_original_generation() {
        let mut controller = start(8);
        controller.stop();
        assert_eq!(
            controller.drive(at(100), generation(1), None).unwrap(),
            Some(SubscriptionDirective::Close {
                generation: generation(1)
            })
        );
        controller.stop();
        assert!(
            controller
                .drive(at(200), generation(1), None)
                .unwrap()
                .is_none()
        );
        controller
            .observe_phase(phase(&controller, 0, Some(1000)))
            .unwrap();
        controller.observe_actual_exit(generation(1)).unwrap();
        assert_eq!(controller.next_deadline(at(2000)), None);
        assert_eq!(controller.status(), SubscriptionStatus::Stopped);
    }
    #[test]
    fn incomplete_reply_does_not_spin_and_foreign_token_does_not_consume_original_request() {
        let mut controller = opened();
        let request = confirm(&mut controller, 6000, 1);
        let mut read = snapshot(request, 6000, 0, Some(1000), 1000);
        read.request = token(9);
        assert!(controller.observe_read_state(read).is_err());
        read.request = request.token;
        read.received_through_snapshot = ReceptionCoverage::Incomplete;
        controller.observe_read_state(read).unwrap();
        assert_eq!(controller.next_deadline(at(10_000)), None);
        assert!(
            controller
                .drive(at(10_000), generation(1), Some(token(2)))
                .unwrap()
                .is_none()
        );
        controller
            .observe_read_progress(ReadProgressSnapshot {
                participation: controller.participation,
                generation: generation(1),
                resumed_at: at(10_000),
                resume_revision: generation(1),
            })
            .unwrap();
        confirm(&mut controller, 10_000, 2);
    }
}

#[cfg(test)]
mod owner_cut_tests {
    use super::*;
    use crate::root_control::RootControl;
    use novarocks_coordination_contract::AttemptActivationIdentity;
    use novarocks_execution_contract::{TaskOutputFacts, TaskState, TaskStatus, TaskStatusVersion};
    use std::time::Duration;
    fn at(ms: u64) -> MonotonicInstant {
        MonotonicInstant::from_origin(Duration::from_millis(ms))
    }
    fn generation(value: u64) -> NonZeroU64 {
        NonZeroU64::new(value).unwrap()
    }
    fn controller(participation: ParticipationRef) -> CoveredSubscription {
        let mut result = CoveredSubscription::new(participation, IdSeed::new(100, 1), 8).unwrap();
        result.activate();
        result.drive(at(0), generation(1), None).unwrap();
        result
            .observe_phase(SubscriptionPhaseSnapshot {
                participation,
                generation: generation(1),
                opening_started_at: at(0),
                opened_at: None,
            })
            .unwrap();
        result
    }
    fn received(
        participation: ParticipationRef,
        status: TaskStatus,
        opened: u64,
        received: u64,
    ) -> ReceivedObservation {
        ReceivedObservation {
            participation,
            generation: generation(1),
            received_at: at(received),
            opening: ObservationOpening {
                started_at: at(0),
                opened_at: Some(at(opened)),
            },
            event: ObservationEvent::Fact {
                source_revision: None,
                fact: ObservationFact::Status(status),
            },
        }
    }
    #[test]
    fn late_open_finished_cannot_enter_original_status_owner_or_make_pending_seal_ready() {
        let mut execution = crate::test_support::execution_with_strict_input();
        let root = execution.graph().root_identity();
        let participation = execution
            .graph()
            .task(root.task_id())
            .unwrap()
            .participation();
        let mut subscription = controller(participation);
        let mut control = RootControl::new(
            AttemptActivationIdentity::from_parts(1, root.query_execution_id(), 1),
            execution.graph(),
        )
        .unwrap();
        control
            .request(AttemptRequestToken::new(generation(1)))
            .unwrap();
        let finished = TaskStatus::try_new(
            root,
            TaskStatusVersion::new(3).unwrap(),
            TaskState::Finished,
            None,
            TaskOutputFacts::new(true),
        )
        .unwrap();
        let decision = subscription
            .observe(received(participation, finished, 6000, 6100))
            .unwrap();
        if let ObservationDisposition::ApplyEntity(accepted) = decision {
            execution.apply_covered_observation(accepted).unwrap();
        }
        assert!(execution.task(root.task_id()).unwrap().status().is_none());
        assert!(control.evaluate(execution.root_seal_facts(false)).is_none());
        assert!(control.pending());
        assert!(!control.sealed());
    }
    #[test]
    fn legal_finished_fixes_the_original_seal_before_next_accepted_task_failure() {
        use novarocks_execution_contract::failure::{FailureClass, FailureFact, FailureReason};
        use novarocks_execution_contract::{
            SafeDetail, TaskFailure, TaskFailurePhase, TerminationDetail,
        };
        use std::num::NonZeroU16;
        let mut execution = crate::test_support::execution_with_strict_input();
        let root = execution.graph().root_identity();
        let participation = execution
            .graph()
            .task(root.task_id())
            .unwrap()
            .participation();
        let mut subscription = controller(participation);
        let mut control = RootControl::new(
            AttemptActivationIdentity::from_parts(1, root.query_execution_id(), 1),
            execution.graph(),
        )
        .unwrap();
        control
            .request(AttemptRequestToken::new(generation(1)))
            .unwrap();
        let finished = TaskStatus::try_new(
            root,
            TaskStatusVersion::new(3).unwrap(),
            TaskState::Finished,
            None,
            TaskOutputFacts::new(true),
        )
        .unwrap();
        let ObservationDisposition::ApplyEntity(accepted) = subscription
            .observe(received(participation, finished, 4000, 4100))
            .unwrap()
        else {
            panic!("legal original opening");
        };
        execution.apply_covered_observation(accepted).unwrap();
        let sealed = control.evaluate(execution.root_seal_facts(false)).unwrap();
        assert!(sealed.result.as_ref().unwrap().is_empty());
        let other = execution
            .graph()
            .tasks()
            .find(|task| task.identity() != root)
            .unwrap();
        let other_identity = other.identity();
        let other_participation = other.participation();
        let mut other_subscription = controller(other_participation);
        let detail = TerminationDetail::Failed(TaskFailure::new_in_phase(
            FailureFact::new(
                FailureClass::Data,
                FailureReason::new(
                    NonZeroU16::new(u16::MAX).unwrap(),
                    NonZeroU16::new(1).unwrap(),
                ),
            ),
            SafeDetail::truncating("later original Task failure"),
            TaskFailurePhase::Preparation,
        ));
        let failed = TaskStatus::try_new_with_installed(
            other_identity,
            TaskStatusVersion::new(4).unwrap(),
            TaskState::Failed,
            Some(detail),
            TaskOutputFacts::default(),
            false,
        )
        .unwrap();
        let ObservationDisposition::ApplyEntity(accepted) = other_subscription
            .observe(received(other_participation, failed, 4000, 4200))
            .unwrap()
        else {
            panic!("legal later observation");
        };
        execution.apply_covered_observation(accepted).unwrap();
        assert!(execution.failure_cause().is_some());
        assert!(control.evaluate(execution.root_seal_facts(false)).is_none());
        assert!(control.sealed());
        assert!(sealed.result.unwrap().is_empty());
    }
}
