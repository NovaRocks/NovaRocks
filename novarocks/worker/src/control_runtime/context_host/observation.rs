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

//! Context-owned covered observation. The pure source remains the only
//! selection, coverage and replay algorithm. Original request custody owns
//! funding, the external observer and every physical notification job.

use super::super::actors::WorkerActor;
use novarocks_control_runtime::{
    driver::{ActorRegistration, ActorRuntime, DriverError, EffectTicket},
    reply_notification::{ReplyNotification, ReplyNotificationError},
};
use novarocks_control_runtime_core::identity::ScopedWorkRef;
use novarocks_execution_contract::{
    QueryContextRef, TaskConvergenceReceipt, TaskConvergenceVersion,
};
use novarocks_worker_protocol::{
    context::{ContextEffect, ContextReplyBody},
    observation::{
        CoveredObservationBookmark, CoveredObservationFact, CoveredSubscriptionError,
        CoveredSubscriptionRequest, ObservationProgress, ObservationSource,
        SelectedCoveredObservation, SubscriptionId,
    },
};
use std::{
    collections::BTreeMap,
    num::NonZeroUsize,
    ops::Bound::{Excluded, Included, Unbounded},
    sync::Arc,
};

/// Pure projection supplied by application composition. It owns no I/O or
/// captured state and is shared with the actual transport encoder.
#[derive(Clone, Copy, Debug)]
pub struct ObservationProjection {
    pub fact_bytes: fn(&CoveredObservationFact) -> Result<usize, ObservationProjectionError>,
    pub max_frame_bytes: NonZeroUsize,
    pub max_initial_bytes: NonZeroUsize,
}
/// Finite storage profile attached to the same admitted R geometry. These
/// bounds describe original storage, never a second quota or admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ObservationStorageManifest {
    pub source_fact_positions: usize,
    pub publication_fact_positions: usize,
    pub request_positions: usize,
    pub cursor_members_per_request: usize,
    pub fact_positions_per_request: usize,
    pub initial_wire_bytes_per_request: usize,
    pub selected_wire_bytes_per_request: usize,
    pub retained_wire_bytes_total: usize,
}
impl ObservationProjection {
    pub fn storage_manifest(
        self,
        max_tasks: NonZeroUsize,
        request_positions: NonZeroUsize,
    ) -> Result<ObservationStorageManifest, ObservationProjectionError> {
        let overflow = ObservationProjectionError::SizeOverflow;
        // One Status-or-Gone and one convergence per Task; Context
        // convergence and Quiesce add exactly two independent keys.
        let keys = max_tasks
            .get()
            .checked_mul(2)
            .and_then(|n| n.checked_add(2))
            .ok_or(overflow)?;
        let cursor_members = max_tasks.get().checked_mul(3).ok_or(overflow)?;
        let pending_bytes = keys
            .checked_mul(self.max_frame_bytes.get())
            .ok_or(overflow)?;
        // Initial-cut overrides and live pending facts can coexist. The
        // source-selected frame and its one external delivery also coexist;
        // the initial front remains covered by max_initial_bytes until ACK.
        let selected_bytes = self.max_frame_bytes.get().checked_mul(2).ok_or(overflow)?;
        let per_request = pending_bytes
            .checked_mul(2)
            .and_then(|n| n.checked_add(self.max_initial_bytes.get()))
            .and_then(|n| n.checked_add(selected_bytes))
            .ok_or(overflow)?;
        // One current publication and up to three previous key images.
        let publication_bytes = self.max_frame_bytes.get().checked_mul(4).ok_or(overflow)?;
        let retained_wire_bytes_total = per_request
            .checked_mul(request_positions.get())
            .and_then(|n| n.checked_add(pending_bytes))
            .and_then(|n| n.checked_add(publication_bytes))
            .ok_or(overflow)?;
        Ok(ObservationStorageManifest {
            source_fact_positions: keys,
            publication_fact_positions: 4,
            request_positions: request_positions.get(),
            cursor_members_per_request: cursor_members,
            fact_positions_per_request: keys
                .checked_mul(2)
                .and_then(|n| n.checked_add(2))
                .ok_or(overflow)?,
            initial_wire_bytes_per_request: self.max_initial_bytes.get(),
            selected_wire_bytes_per_request: selected_bytes,
            retained_wire_bytes_total,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObservationProjectionError {
    InvalidFact,
    SizeOverflow,
}

#[derive(Debug)]
pub(crate) enum ContextObservationError {
    Driver(DriverError),
    WrongRequest,
    Source(CoveredSubscriptionError),
    Notification(ReplyNotificationError),
    InvalidConvergenceVersion,
    MissingProjection,
    Projection(ObservationProjectionError),
}

impl std::fmt::Display for ObservationProjectionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::InvalidFact => "Observation projection fact is invalid",
            Self::SizeOverflow => "Observation projection size overflowed",
        })
    }
}
impl std::error::Error for ObservationProjectionError {}
impl std::fmt::Display for ContextObservationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Driver(error) => write!(f, "Context observation driver failed: {error}"),
            Self::WrongRequest => {
                f.write_str("Context observation belongs to another original request")
            }
            Self::Source(error) => write!(
                f,
                "Context observation source rejected the request: {error}"
            ),
            Self::Notification(error) => {
                write!(f, "Context observation notification failed: {error:?}")
            }
            Self::InvalidConvergenceVersion => {
                f.write_str("Context observation convergence version is invalid")
            }
            Self::MissingProjection => f.write_str("Context observation projection is missing"),
            Self::Projection(error) => write!(f, "Context observation projection failed: {error}"),
        }
    }
}
impl std::error::Error for ContextObservationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Driver(error) => Some(error),
            Self::Source(error) => Some(error),
            Self::Projection(error) => Some(error),
            Self::Notification(ReplyNotificationError::Driver(error)) => Some(error),
            Self::Notification(
                ReplyNotificationError::WrongWorkScope
                | ReplyNotificationError::WorkNotLive
                | ReplyNotificationError::ObserverEnded
                | ReplyNotificationError::Failed,
            )
            | Self::WrongRequest
            | Self::InvalidConvergenceVersion
            | Self::MissingProjection => None,
        }
    }
}
/// Metadata of one already admitted R, not another admission or Work token.
/// The same original R custody retains this non-Clone handle through bounded
/// unsubscribe and actual external/notification capture retirement.
#[derive(Debug)]
pub(crate) struct ObservationSubscription {
    id: SubscriptionId,
    work: ScopedWorkRef,
    owner: Arc<()>,
}
struct WakeSweep {
    after: Option<SubscriptionId>,
    last: Option<SubscriptionId>,
}
#[must_use = "Notify these same original R owners before completing publication"]
pub(crate) struct ObservationWakeBatch {
    pub requests: Vec<ScopedWorkRef>,
    pub progress: ObservationProgress,
}

pub(crate) struct ContextObservation {
    source: ObservationSource,
    owner: Arc<()>,
    runtime: ActorRuntime<WorkerActor>,
    registration: ActorRegistration,
    requests: BTreeMap<SubscriptionId, ScopedWorkRef>,
    last_subscription: u64,
    publication: Option<WakeSweep>,
    fanout_complete: bool,
}
impl ContextObservation {
    /// Limits come from the same paid Context/task geometry, shared global R
    /// positions and wire projection bounds. No capacity is reserved here.
    pub(crate) fn new(
        context: QueryContextRef,
        runtime: &ActorRuntime<WorkerActor>,
        registration: &ActorRegistration,
        max_tasks: NonZeroUsize,
        shared_request_positions: NonZeroUsize,
        max_frame_bytes: NonZeroUsize,
        max_initial_bytes: NonZeroUsize,
    ) -> Result<Self, ContextObservationError> {
        runtime
            .validate_registration(registration)
            .map_err(ContextObservationError::Driver)?;
        Ok(Self {
            source: ObservationSource::new(
                context,
                max_tasks.get(),
                shared_request_positions.get(),
                max_frame_bytes.get(),
                max_initial_bytes.get(),
            ),
            owner: Arc::new(()),
            runtime: runtime.clone(),
            registration: registration.clone(),
            requests: BTreeMap::new(),
            last_subscription: 0,
            publication: None,
            fanout_complete: true,
        })
    }
    pub(super) fn publication_pending(&self) -> bool {
        self.publication.is_some() || !self.fanout_complete
    }
    pub(crate) fn subscriptions(&self) -> usize {
        self.requests.len()
    }

    /// Only the original Subscribe effect may bind this already-admitted R.
    /// The financial author and destination Context need not share a Scope.
    pub(super) fn begin_original_request(
        &mut self,
        ticket: &EffectTicket<crate::control_runtime::actors::WorkerAction>,
        work: ScopedWorkRef,
        generation: u64,
        position: Arc<CoveredSubscriptionRequest>,
    ) -> Result<ObservationSubscription, ContextObservationError> {
        self.runtime
            .validate_registration(&self.registration)
            .map_err(ContextObservationError::Driver)?;
        // The original Subscribe effect carries the admitted financial author;
        // a numeric Work address or the destination's Scope cannot replace it.
        self.runtime
            .validate_effect_ticket(ticket)
            .map_err(ContextObservationError::Driver)?;
        if ticket.actor != self.registration.actor()
            || ticket.descriptor.work != work
            || !matches!(&ticket.effect,
                crate::control_runtime::actors::WorkerAction::Context(ContextEffect::Subscribe {
                    generation: original_generation, position: original_position, ..
                }) if *original_generation == generation && Arc::ptr_eq(original_position, &position))
        {
            return Err(ContextObservationError::WrongRequest);
        }
        let next = self
            .last_subscription
            .checked_add(1)
            .ok_or(ContextObservationError::Driver(
                DriverError::IdentityExhausted,
            ))?;
        let id = SubscriptionId(next);
        let initial = self
            .source
            .begin_shared_covered_subscription(id, generation, position)
            .map_err(ContextObservationError::Source)?;
        let _initial_cut = initial;
        self.last_subscription = next;
        let key = work;
        assert!(
            self.requests.insert(id, key).is_none(),
            "one source subscriber per original R"
        );
        Ok(ObservationSubscription {
            id,
            work: key,
            owner: self.owner.clone(),
        })
    }
    fn validate(
        &self,
        subscription: &ObservationSubscription,
    ) -> Result<(), ContextObservationError> {
        let matches = Arc::ptr_eq(&self.owner, &subscription.owner)
            && self
                .requests
                .get(&subscription.id)
                .is_some_and(|original| *original == subscription.work);
        if matches {
            Ok(())
        } else {
            Err(ContextObservationError::WrongRequest)
        }
    }
    pub(crate) fn continue_subscribe(
        &mut self,
        subscription: &ObservationSubscription,
        budget: usize,
    ) -> Result<ObservationProgress, ContextObservationError> {
        self.validate(subscription)?;
        self.source
            .continue_subscribe(subscription.id, budget)
            .map_err(ContextObservationError::Source)
    }
    /// A refusal leaves the effect's original neutral fact with its actual
    /// caller; no shadow pending-fact queue is installed in this leaf.
    pub(crate) fn publish(
        &mut self,
        fact: CoveredObservationFact,
        bytes: usize,
    ) -> Result<u64, (ContextObservationError, CoveredObservationFact)> {
        if self.publication.is_some() {
            return Err((
                ContextObservationError::Source(CoveredSubscriptionError::PublicationPending),
                fact,
            ));
        }
        let revision = match self.source.publish(fact.clone(), bytes) {
            Ok(revision) => revision,
            Err(error) => return Err((ContextObservationError::Source(error), fact)),
        };
        self.publication = Some(WakeSweep {
            after: None,
            last: self.requests.last_key_value().map(|(id, _)| *id),
        });
        self.fanout_complete = false;
        Ok(revision)
    }
    /// Pure fanout is bounded independently of notification metadata. Keep
    /// the next actual publication with its original host effect until both
    /// this fanout and the original R wake targets have been handed over.
    pub(crate) fn continue_publication(&mut self, budget: usize) -> ObservationProgress {
        let progress = self.source.continue_publication(budget);
        self.fanout_complete = progress.complete;
        ObservationProgress {
            work: progress.work,
            complete: progress.complete && self.publication.is_none(),
        }
    }
    /// IDs are only metadata. The caller looks up each exact R custody and
    /// calls its existing ReplyNotification::published, without foreign code.
    /// The frozen last ID prevents new subscriber churn from extending a turn.
    pub(crate) fn take_notification_targets(&mut self, budget: usize) -> ObservationWakeBatch {
        let mut requests = Vec::new();
        if !self.fanout_complete {
            return ObservationWakeBatch {
                requests,
                progress: ObservationProgress {
                    work: 0,
                    complete: false,
                },
            };
        }
        let Some(sweep) = self.publication.as_mut() else {
            return ObservationWakeBatch {
                requests,
                progress: ObservationProgress {
                    work: 0,
                    complete: true,
                },
            };
        };
        let mut work = 0;
        while work < budget {
            let Some(last) = sweep.last else {
                break;
            };
            let low = sweep.after.map_or(Unbounded, Excluded);
            let Some((id, key)) = self.requests.range((low, Included(last))).next() else {
                break;
            };
            sweep.after = Some(*id);
            requests.push(*key);
            work += 1;
        }
        let more = sweep.last.is_some_and(|last| {
            let low = sweep.after.map_or(Unbounded, Excluded);
            self.requests.range((low, Included(last))).next().is_some()
        });
        if !more {
            self.publication = None;
        }
        ObservationWakeBatch {
            requests,
            progress: ObservationProgress {
                work,
                complete: !more,
            },
        }
    }
    /// Clear and recheck under the same Context owner before parking. A prior
    /// physical wake may still arrive; it does not acknowledge any frame.
    pub(crate) fn select_next(
        &mut self,
        subscription: &ObservationSubscription,
        notification: &mut ReplyNotification<WorkerActor>,
    ) -> Result<Option<SelectedCoveredObservation>, ContextObservationError> {
        self.validate(subscription)?;
        notification
            .clear_published()
            .map_err(ContextObservationError::Notification)?;
        let selected = self
            .source
            .select_next(subscription.id)
            .map_err(ContextObservationError::Source)?;
        if selected.is_some() {
            notification.published();
        }
        Ok(selected)
    }
    /// Invoke only after the ordered external transport actually accepted the
    /// exact selected delivery. Selection/poll/Drop never calls this method.
    pub(crate) fn note_delivered(
        &mut self,
        subscription: &ObservationSubscription,
        delivery: u64,
        notification: &mut ReplyNotification<WorkerActor>,
    ) -> Result<CoveredObservationBookmark, ContextObservationError> {
        self.validate(subscription)?;
        notification
            .clear_published()
            .map_err(ContextObservationError::Notification)?;
        let bookmark = self
            .source
            .note_delivered(subscription.id, delivery)
            .map_err(ContextObservationError::Source)?;
        if self
            .source
            .select_next(subscription.id)
            .map_err(ContextObservationError::Source)?
            .is_some()
        {
            notification.published();
        }
        Ok(bookmark)
    }
    pub(crate) fn bookmark(
        &mut self,
        subscription: &ObservationSubscription,
    ) -> Result<CoveredObservationBookmark, ContextObservationError> {
        self.validate(subscription)?;
        self.source
            .bookmark(subscription.id)
            .ok_or(ContextObservationError::WrongRequest)
    }
    /// External observer Drop requests this metadata transition through its
    /// original R ready path. It never directly locks/calls this Context.
    pub(crate) fn unsubscribe(
        &mut self,
        subscription: &ObservationSubscription,
    ) -> Result<(), ContextObservationError> {
        self.validate(subscription)?;
        self.source.unsubscribe(subscription.id);
        Ok(())
    }
    pub(crate) fn continue_unsubscribe(
        &mut self,
        subscription: &ObservationSubscription,
        budget: usize,
    ) -> Result<ObservationProgress, ContextObservationError> {
        self.validate(subscription)?;
        let progress = self
            .source
            .continue_unsubscribe(subscription.id, budget)
            .map_err(ContextObservationError::Source)?;
        if progress.complete {
            self.requests.remove(&subscription.id);
        }
        Ok(progress)
    }
    /// Mechanical projection from already decided pure Task effects only.
    /// Context convergence needs its own actual Core receipt; Quiesce comes
    /// from the actual typed reply body below, not a guessed Context state.
    pub(crate) fn effect_fact(
        effect: &ContextEffect,
    ) -> Result<Option<CoveredObservationFact>, ContextObservationError> {
        Ok(match effect {
            ContextEffect::PublishTask { snapshot, .. }
            | ContextEffect::PublishMetrics { snapshot, .. } => {
                Some(CoveredObservationFact::Status(snapshot.clone()))
            }
            ContextEffect::PublishContextConvergence { receipt, .. } => {
                Some(CoveredObservationFact::ContextConvergence(*receipt))
            }
            ContextEffect::PublishTaskConvergence { task, snapshot, .. }
                if snapshot.actual_stopped() =>
            {
                Some(CoveredObservationFact::TaskConvergence(
                    TaskConvergenceReceipt::actual_stopped(
                        *task,
                        TaskConvergenceVersion::new(snapshot.version())
                            .map_err(|_| ContextObservationError::InvalidConvergenceVersion)?,
                    ),
                ))
            }
            _ => None,
        })
    }
    pub(crate) fn reply_fact(body: &ContextReplyBody) -> Option<CoveredObservationFact> {
        match body {
            ContextReplyBody::Quiesce { receipt, .. } => {
                Some(CoveredObservationFact::Quiesce(receipt.clone()))
            }
            _ => None,
        }
    }
}

impl super::ContextHostRecord {
    pub(super) fn configure_observation(
        &mut self,
        projection: Option<ObservationProjection>,
    ) -> Result<(), ContextObservationError> {
        let projection = projection.ok_or(ContextObservationError::MissingProjection)?;
        if let Some(original) = self.resources.observation_projection {
            if original.max_frame_bytes != projection.max_frame_bytes
                || original.max_initial_bytes != projection.max_initial_bytes
                || !std::ptr::fn_addr_eq(original.fact_bytes, projection.fact_bytes)
            {
                return Err(ContextObservationError::WrongRequest);
            }
            return Ok(());
        }
        projection
            .storage_manifest(
                NonZeroUsize::new(self.max_live_tasks).expect("admitted Context Task geometry"),
                NonZeroUsize::new(self.resources.observation_positions)
                    .expect("admitted original R geometry"),
            )
            .map_err(ContextObservationError::Projection)?;
        let source = ContextObservation::new(
            self.identity,
            &self.control_runtime,
            self.registration(),
            NonZeroUsize::new(self.max_live_tasks).expect("admitted Context Task geometry"),
            NonZeroUsize::new(self.resources.observation_positions)
                .expect("admitted original R geometry"),
            projection.max_frame_bytes,
            projection.max_initial_bytes,
        )?;
        self.resources.observation = Some(source);
        self.resources.observation_projection = Some(projection);
        Ok(())
    }
    pub(super) fn observation_effect_fact(
        &self,
        effect: &crate::control_runtime::actors::WorkerAction,
    ) -> Result<Option<(CoveredObservationFact, usize)>, ContextObservationError> {
        use crate::control_runtime::actors::WorkerAction;
        let fact = match effect {
            WorkerAction::Context(effect) => ContextObservation::effect_fact(effect)?,
            WorkerAction::Reply(reply) => ContextObservation::reply_fact(&reply.body),
            _ => None,
        };
        let Some(fact) = fact else {
            return Ok(None);
        };
        let projection = self
            .resources
            .observation_projection
            .ok_or(ContextObservationError::MissingProjection)?;
        let bytes = (projection.fact_bytes)(&fact).map_err(ContextObservationError::Projection)?;
        Ok(Some((fact, bytes)))
    }
    pub(super) fn observation_publication_pending(&self) -> bool {
        self.resources
            .observation
            .as_ref()
            .is_some_and(ContextObservation::publication_pending)
    }
    pub(super) fn commit_observation_fact(
        &mut self,
        fact: CoveredObservationFact,
        bytes: usize,
    ) -> Result<(), ContextObservationError> {
        self.resources
            .observation
            .as_mut()
            .ok_or(ContextObservationError::MissingProjection)?
            .publish(fact, bytes)
            .map(|_| ())
            .map_err(|(error, _)| error)
    }
    pub(super) fn advance_observation_publication(
        &mut self,
        budget: usize,
    ) -> Result<bool, ContextObservationError> {
        let Some(source) = self.resources.observation.as_mut() else {
            return Ok(false);
        };
        let progress = source.continue_publication(budget);
        let wakes = source.take_notification_targets(budget);
        for work in wakes.requests {
            // Unsubscribe can retire a target after the publication's frozen
            // frontier. No later numeric Work can replace that original R.
            if let Some(original) = self.resources.requests.get_mut(&work) {
                if original.scope_id() != work.scope {
                    return Err(ContextObservationError::WrongRequest);
                }
                original.observation_published();
            }
        }
        Ok(!progress.complete || !wakes.progress.complete)
    }
}

impl super::ContextHostRecord {
    pub(super) fn advance_observation_teardown(&mut self, budget: usize) -> bool {
        // The original Root RemoveHost transfer closes future control and
        // subscription issuance. RetireReservation follows HostRemoved, so it
        // cannot authorize the child metadata teardown which publishes that
        // fact. The parent payment still waits for its separate retirement.
        if !self.resources.control_admission_closed || !self.resources.requests.is_empty() {
            return false;
        }
        if let Some(source) = self.resources.observation.as_mut()
            && (source.publication_pending()
                || source.subscriptions() != 0
                || !source.source.drain_unsubscribed(budget).complete)
        {
            return false;
        }
        // These are original sealed neutral read observations, not rich Task
        // owners. Retire one indexed snapshot per charged metadata unit.
        for _ in 0..budget {
            if self.retired_task_reads.pop_first().is_none() {
                break;
            }
        }
        self.retired_task_reads.is_empty()
    }
}
