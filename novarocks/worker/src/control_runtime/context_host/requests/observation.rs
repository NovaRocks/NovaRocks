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

//! A covered stream borrows the one original R and its one external observer.
//! Frame selection is never an acknowledgement of ordered transport delivery.

use super::{ContextRequestError, ContextRequestHandle};
use crate::control_runtime::{
    actors::{WorkerAction, WorkerActor},
    context_host::{
        ContextHostRecord,
        observation::{ContextObservationError, ObservationSubscription},
    },
    request_endpoint::{RequestAwaitError, RequestDelivery},
    service::ContextHostAddress,
};
use novarocks_control_runtime::{driver::EffectTicket, reply_notification::ReplyObserver};
use novarocks_control_runtime_core::identity::ScopedWorkRef;
use novarocks_execution_contract::OperationOutcome;
use novarocks_worker_protocol::{
    context::{ContextEffect, ContextReplyBody, RequestResult},
    observation::{CoveredObservationBookmark, SelectedCoveredObservation},
};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};

#[derive(Default)]
pub(super) struct ObservationRequest {
    pub(super) handed_off: bool,
    subscription: Option<ObservationSubscription>,
    initialized: bool,
    unsubscribing: bool,
    failure: Option<Arc<ContextObservationError>>,
}
pub(crate) enum ContextObservationStreamError {
    ContextRemoved,
    UnknownRequest,
    OwnerFailed,
    FrameOutstanding,
    Intake(RequestAwaitError),
    IntakeRejected(ContextReplyBody),
    Source(Arc<ContextObservationError>),
}

impl std::fmt::Display for ContextObservationStreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ContextRemoved => f.write_str("Observation stream Context was removed"),
            Self::UnknownRequest => f.write_str("Observation stream original request is unknown"),
            Self::OwnerFailed => f.write_str("Observation stream original owner failed"),
            Self::FrameOutstanding => {
                f.write_str("Observation stream still owns its selected frame")
            }
            Self::Intake(error) => {
                write!(f, "Observation stream original intake failed: {error:?}")
            }
            Self::IntakeRejected(_) => {
                f.write_str("Observation stream original intake was rejected; reply retained")
            }
            Self::Source(error) => write!(f, "Observation stream source failed: {error}"),
        }
    }
}
impl std::fmt::Debug for ContextObservationStreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The retained reply body is never traversed by diagnostics.
        f.debug_struct("ContextObservationStreamError")
            .field("cause", &format_args!("{self}"))
            .finish_non_exhaustive()
    }
}
impl std::error::Error for ContextObservationStreamError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Source(error) => Some(error.as_ref()),
            Self::Intake(RequestAwaitError::Notification(novarocks_control_runtime::reply_notification::ReplyNotificationError::Driver(error))) => Some(error),
            Self::ContextRemoved | Self::UnknownRequest | Self::OwnerFailed
            | Self::FrameOutstanding | Self::IntakeRejected(_)
            | Self::Intake(RequestAwaitError::AlreadyTaken | RequestAwaitError::Notification(
                novarocks_control_runtime::reply_notification::ReplyNotificationError::WrongWorkScope
                | novarocks_control_runtime::reply_notification::ReplyNotificationError::WorkNotLive
                | novarocks_control_runtime::reply_notification::ReplyNotificationError::ObserverEnded
                | novarocks_control_runtime::reply_notification::ReplyNotificationError::Failed)) => None,
        }
    }
}

/// The original reply delivery remains held until both stream and last frame
/// owner have ended. Only the stream holds the single external Waker observer.
pub(crate) struct ContextObservationStream {
    address: ContextHostAddress,
    work: ScopedWorkRef,
    intake: Option<ContextRequestHandle>,
    delivery: Option<Arc<RequestDelivery<ContextReplyBody>>>,
    observer: Option<ReplyObserver<WorkerActor>>,
    outstanding: Arc<AtomicBool>,
}
impl ContextObservationStream {
    pub(crate) fn from_request(
        address: ContextHostAddress,
        work: ScopedWorkRef,
        handle: ContextRequestHandle,
    ) -> Self {
        Self {
            address,
            work,
            intake: Some(handle),
            delivery: None,
            observer: None,
            outstanding: Arc::new(AtomicBool::new(false)),
        }
    }
    fn select(
        &mut self,
    ) -> Result<Option<ObservationFrameDelivery>, ContextObservationStreamError> {
        if self.outstanding.load(Ordering::Acquire) {
            return Err(ContextObservationStreamError::FrameOutstanding);
        }
        let Some(mut host) = self.address.lock() else {
            return Err(ContextObservationStreamError::ContextRemoved);
        };
        let selected = host.select_observation(self.work)?;
        drop(host);
        Ok(selected.map(|selected| {
            assert!(
                !self.outstanding.swap(true, Ordering::AcqRel),
                "one original selected frame"
            );
            ObservationFrameDelivery {
                address: self.address.clone(),
                work: self.work,
                selected: Some(selected),
                original: self
                    .delivery
                    .as_ref()
                    .expect("actual intake delivery")
                    .clone(),
                outstanding: self.outstanding.clone(),
            }
        }))
    }
    pub(crate) fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<ObservationFrameDelivery, ContextObservationStreamError>> {
        if let Some(handle) = self.intake.as_mut() {
            match Pin::new(handle).poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => {
                    return Poll::Ready(Err(ContextObservationStreamError::Intake(error)));
                }
                Poll::Ready(Ok(mut delivery)) => {
                    self.observer = delivery.take_observer();
                    let accepted = delivery
                        .body()
                        .matches(RequestResult::Outcome(OperationOutcome::Accepted));
                    self.delivery = Some(Arc::new(delivery));
                    self.intake = None;
                    if !accepted {
                        return Poll::Ready(Err(ContextObservationStreamError::IntakeRejected(
                            self.delivery
                                .as_ref()
                                .expect("same original delivery")
                                .body()
                                .clone(),
                        )));
                    }
                }
            }
        }
        match self.select() {
            Ok(Some(frame)) => return Poll::Ready(Ok(frame)),
            Err(error) => return Poll::Ready(Err(error)),
            Ok(None) => {}
        }
        // External code, including old Waker Drop, never runs under the
        // Context lock. Recheck the same source after arming its original R.
        match self
            .observer
            .as_mut()
            .expect("same original stream observer")
            .arm(cx.waker().clone())
        {
            Ok(armed) if !armed.failed => {}
            Ok(_) => return Poll::Ready(Err(ContextObservationStreamError::OwnerFailed)),
            Err(refusal) => {
                drop(refusal.waker);
                return Poll::Ready(Err(ContextObservationStreamError::Intake(
                    RequestAwaitError::Notification(refusal.error),
                )));
            }
        }
        self.delivery
            .as_ref()
            .expect("same original stream delivery")
            .notify_original();
        match self.select() {
            Ok(Some(frame)) => Poll::Ready(Ok(frame)),
            Err(error) => Poll::Ready(Err(error)),
            Ok(None) => Poll::Pending,
        }
    }
    pub(crate) fn bookmark(
        &self,
    ) -> Result<CoveredObservationBookmark, ContextObservationStreamError> {
        let Some(mut host) = self.address.lock() else {
            return Err(ContextObservationStreamError::ContextRemoved);
        };
        host.observation_bookmark(self.work)
    }
}
impl Drop for ContextObservationStream {
    fn drop(&mut self) {
        // Observer state is final before the last delivery can mark this R.
        drop(self.observer.take());
        drop(self.intake.take());
        drop(self.delivery.take());
    }
}

#[must_use = "Only actual ordered transport acceptance acknowledges this frame"]
pub(crate) struct ObservationFrameDelivery {
    address: ContextHostAddress,
    work: ScopedWorkRef,
    selected: Option<SelectedCoveredObservation>,
    original: Arc<RequestDelivery<ContextReplyBody>>,
    outstanding: Arc<AtomicBool>,
}
impl ObservationFrameDelivery {
    pub(crate) fn selected(&self) -> &SelectedCoveredObservation {
        self.selected.as_ref().expect("original selected frame")
    }
    pub(crate) fn delivered(
        self,
    ) -> Result<CoveredObservationBookmark, (ContextObservationStreamError, Self)> {
        let result = match self.address.lock() {
            Some(mut host) => {
                host.note_observation_delivered(self.work, self.selected().delivery_id)
            }
            None => Err(ContextObservationStreamError::ContextRemoved),
        };
        match result {
            Ok(bookmark) => Ok(bookmark),
            Err(error) => Err((error, self)),
        }
    }
}
impl Drop for ObservationFrameDelivery {
    fn drop(&mut self) {
        drop(self.selected.take());
        self.outstanding.store(false, Ordering::Release);
        self.original.notify_original();
    }
}

impl ContextHostRecord {
    /// Cancel only the exact original Subscribe action which never issued a
    /// source subscription. The failed owner already preserved the original
    /// intake reply; no replacement stream, reply or external exit is minted.
    pub(crate) fn settle_failed_subscribe(
        &mut self,
        stage: &super::super::failure::ScopePublicationsDrained<'_>,
        ticket: EffectTicket<WorkerAction>,
    ) -> Result<(), (ContextRequestError, EffectTicket<WorkerAction>)> {
        if let Err(error) = stage.validate_scope(&self.control_runtime, self.actor, self.scope()) {
            return Err((ContextRequestError::Driver(error), ticket));
        }
        let request = match &ticket.effect {
            WorkerAction::Context(ContextEffect::Subscribe { request, .. })
                if ticket.actor == self.actor =>
            {
                *request
            }
            _ => return Err((ContextRequestError::WrongRequest, ticket)),
        };
        let Some(work) = self.request_inputs.get(&request).copied() else {
            return Err((ContextRequestError::UnknownRequest, ticket));
        };
        let original = self
            .resources
            .requests
            .get_mut(&work)
            .expect("same Subscribe R index");
        if original.request.map(|request| request.token) != Some(request)
            || !original.endpoint.reply().is_committed()
        {
            return Err((ContextRequestError::WrongRequest, ticket));
        }
        let Some(state) = original.observation.as_mut() else {
            return Err((ContextRequestError::WrongRequest, ticket));
        };
        if state.handed_off || state.subscription.is_some() || state.failure.is_some() {
            return Err((ContextRequestError::WrongPhase, ticket));
        }
        if let Err(refusal) = self.control_runtime.effect_completed(ticket) {
            return Err((ContextRequestError::Driver(refusal.error), refusal.ticket));
        }
        // With no issued subscription, there is nothing to unsubscribe. The
        // real failed Context still supplies typed OwnerFailed to the reader.
        state.handed_off = true;
        original.reply_completed = true;
        original.notification.published();
        original.endpoint.external().notify();
        Ok(())
    }
    pub(in crate::control_runtime::context_host) fn handoff_observation_subscribe(
        &mut self,
        ticket: EffectTicket<WorkerAction>,
    ) -> Result<(), (ContextRequestError, EffectTicket<WorkerAction>)> {
        if let Err(error) = self.control_runtime.validate_effect_ticket(&ticket) {
            return Err((ContextRequestError::Driver(error), ticket));
        }
        let (request, generation, position) = match &ticket.effect {
            WorkerAction::Context(ContextEffect::Subscribe {
                request,
                generation,
                position,
            }) => (*request, *generation, position.clone()),
            _ => return Err((ContextRequestError::WrongRequest, ticket)),
        };
        let Some(work) = self.request_inputs.get(&request).copied() else {
            return Err((ContextRequestError::UnknownRequest, ticket));
        };
        let original = self
            .resources
            .requests
            .get_mut(&work)
            .expect("original request index");
        let Some(state) = original.observation.as_mut() else {
            return Err((ContextRequestError::WrongRequest, ticket));
        };
        if state.subscription.is_some() || state.failure.is_some() {
            return Err((ContextRequestError::WrongPhase, ticket));
        }
        let Some(source) = self.resources.observation.as_mut() else {
            return Err((
                ContextRequestError::Observation(ContextObservationError::MissingProjection),
                ticket,
            ));
        };
        // Share the same immutable original; the pure source validates one
        // cursor per continuation without copying its complete vectors.
        state.handed_off = true;
        match source.begin_original_request(&ticket, work, generation, position) {
            Ok(subscription) => state.subscription = Some(subscription),
            Err(error) => {
                state.failure = Some(Arc::new(error));
                original.notification.published();
            }
        }
        original.endpoint.external().notify();
        self.control_runtime
            .effect_completed(ticket)
            .map_err(|refusal| (ContextRequestError::Driver(refusal.error), refusal.ticket))
    }
    pub(in crate::control_runtime::context_host) fn advance_observation_request(
        &mut self,
        work: ScopedWorkRef,
        budget: usize,
    ) -> Result<(), ContextRequestError> {
        let Some(original) = self.resources.requests.get_mut(&work) else {
            return Ok(());
        };
        let Some(state) = original.observation.as_mut() else {
            return Ok(());
        };
        let Some(source) = self.resources.observation.as_mut() else {
            return Ok(());
        };
        if original.endpoint.external_ended() {
            if let Some(subscription) = &state.subscription {
                if !state.unsubscribing {
                    source
                        .unsubscribe(subscription)
                        .map_err(ContextRequestError::Observation)?;
                    state.unsubscribing = true;
                }
                let progress = source
                    .continue_unsubscribe(subscription, budget)
                    .map_err(ContextRequestError::Observation)?;
                if !progress.complete {
                    original.endpoint.external().notify();
                    return Ok(());
                }
            } else if !state.handed_off {
                // The actual Subscribe effect still owns initialization.
                return Ok(());
            }
            original.observation = None;
            return Ok(());
        }
        if !state.initialized
            && state.failure.is_none()
            && let Some(subscription) = &state.subscription
        {
            match source.continue_subscribe(subscription, budget) {
                Ok(progress) => {
                    state.initialized = progress.complete;
                    if !progress.complete {
                        original.endpoint.external().notify();
                    }
                }
                Err(error) => state.failure = Some(Arc::new(error)),
            }
            if state.initialized || state.failure.is_some() {
                original.notification.published();
                original.endpoint.external().notify();
            }
        }
        Ok(())
    }
    fn select_observation(
        &mut self,
        work: ScopedWorkRef,
    ) -> Result<Option<SelectedCoveredObservation>, ContextObservationStreamError> {
        if self.resources.failure.is_some() {
            return Err(ContextObservationStreamError::OwnerFailed);
        }
        let original = self
            .resources
            .requests
            .get_mut(&work)
            .ok_or(ContextObservationStreamError::UnknownRequest)?;
        let state = original
            .observation
            .as_ref()
            .ok_or(ContextObservationStreamError::UnknownRequest)?;
        if let Some(error) = &state.failure {
            return Err(ContextObservationStreamError::Source(error.clone()));
        }
        if !state.initialized {
            return Ok(None);
        }
        let source = self
            .resources
            .observation
            .as_mut()
            .ok_or(ContextObservationStreamError::OwnerFailed)?;
        source
            .select_next(
                state.subscription.as_ref().expect("initialized source"),
                &mut original.notification,
            )
            .map_err(|error| ContextObservationStreamError::Source(Arc::new(error)))
    }
    fn note_observation_delivered(
        &mut self,
        work: ScopedWorkRef,
        delivery: u64,
    ) -> Result<CoveredObservationBookmark, ContextObservationStreamError> {
        let original = self
            .resources
            .requests
            .get_mut(&work)
            .ok_or(ContextObservationStreamError::UnknownRequest)?;
        let state = original
            .observation
            .as_ref()
            .ok_or(ContextObservationStreamError::UnknownRequest)?;
        self.resources
            .observation
            .as_mut()
            .ok_or(ContextObservationStreamError::OwnerFailed)?
            .note_delivered(
                state
                    .subscription
                    .as_ref()
                    .ok_or(ContextObservationStreamError::UnknownRequest)?,
                delivery,
                &mut original.notification,
            )
            .map_err(|error| ContextObservationStreamError::Source(Arc::new(error)))
    }
    fn observation_bookmark(
        &mut self,
        work: ScopedWorkRef,
    ) -> Result<CoveredObservationBookmark, ContextObservationStreamError> {
        let original = self
            .resources
            .requests
            .get(&work)
            .ok_or(ContextObservationStreamError::UnknownRequest)?;
        let state = original
            .observation
            .as_ref()
            .ok_or(ContextObservationStreamError::UnknownRequest)?;
        self.resources
            .observation
            .as_mut()
            .ok_or(ContextObservationStreamError::OwnerFailed)?
            .bookmark(
                state
                    .subscription
                    .as_ref()
                    .ok_or(ContextObservationStreamError::UnknownRequest)?,
            )
            .map_err(|error| ContextObservationStreamError::Source(Arc::new(error)))
    }
}

impl super::ContextRequestCustody {
    pub(in crate::control_runtime::context_host) fn scope_id(
        &self,
    ) -> novarocks_control_runtime_core::identity::ScopeId {
        self.scope.id()
    }
    pub(in crate::control_runtime::context_host) fn observation_published(&mut self) {
        if self.observation.is_some() {
            self.notification.published();
            self.endpoint.external().notify();
        }
    }
}

#[must_use = "A failed subscription handoff retains its exact R or original admission partial"]
pub(crate) enum ContextSubscriptionRefusal {
    Admission(super::ContextRequestAdmissionRefusal),
    Submit {
        error: Box<super::ContextRequestSubmitRefusal>,
        work: ScopedWorkRef,
        _original_handle: ContextRequestHandle,
    },
}
impl std::fmt::Debug for ContextSubscriptionRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Admission(original) => f
                .debug_struct("SubscriptionAdmissionRefusal")
                .field("error", &format_args!("{}", original.error))
                .field("original_partial", &original.partial)
                .finish(),
            Self::Submit {
                error,
                work,
                _original_handle: _,
            } => f
                .debug_struct("SubscriptionSubmissionRefusal")
                .field("error", &format_args!("{}", error.error))
                .field("original_work", work)
                .field("neutral_command_retained", &error.command.is_some())
                .field("original_external_handle", &"retained")
                .finish_non_exhaustive(),
        }
    }
}
impl std::fmt::Display for ContextSubscriptionRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Admission(original) => write!(
                f,
                "Observation subscription admission failed: {}",
                original.error
            ),
            Self::Submit {
                error,
                work,
                _original_handle: _,
            } => write!(
                f,
                "Observation subscription original {work:?} submission failed: {}",
                error.error
            ),
        }
    }
}
impl std::error::Error for ContextSubscriptionRefusal {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Admission(original) => Some(&original.error),
            Self::Submit {
                error,
                work: _,
                _original_handle: _,
            } => Some(&error.error),
        }
    }
}
impl ContextHostRecord {
    pub(crate) fn begin_status_subscription(
        &mut self,
        ready: &crate::control_runtime::request_ready::ControlRequestReady,
        source: novarocks_control_runtime_core::identity::ActorId,
        operation: novarocks_execution_contract::TaskOperationId,
        deadline: novarocks_worker_protocol::lease::MonotonicInstant,
        generation: u64,
        position: novarocks_worker_protocol::observation::CoveredSubscriptionRequest,
    ) -> Result<(ScopedWorkRef, ContextRequestHandle), ContextSubscriptionRefusal> {
        let (work, handle) = self
            .admit_request(
                ready,
                source,
                novarocks_control_runtime_core::mailbox::InputClass::Control,
                operation,
                deadline,
            )
            .map_err(ContextSubscriptionRefusal::Admission)?;
        match self.submit_request(
            work,
            novarocks_worker_protocol::context::ContextCommand::Subscribe {
                generation,
                position,
            },
        ) {
            Ok(()) => Ok((work, handle)),
            Err(error) => Err(ContextSubscriptionRefusal::Submit {
                error: Box::new(error),
                work,
                _original_handle: handle,
            }),
        }
    }
}
