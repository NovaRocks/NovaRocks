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

//! Pure covered observation, using the same selection/delivery distinction as production.
use novarocks_execution_contract::{
    QueryContextConvergenceCursor, QueryContextConvergenceReceipt, QueryContextRef,
    QuiesceQueryContextReceipt, TaskConvergenceCursor, TaskConvergenceReceipt, TaskIdentity,
    TaskStatus, TaskStatusCursor,
};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::num::NonZeroU64;
use std::sync::Arc;
/// The facts share one source revision, independent of their protocol versions.
#[expect(
    clippy::large_enum_variant,
    reason = "This Worker-to-adapter fact carries the immutable TaskStatus snapshot by value; boxing would change the consumer contract and allocate per status event."
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CoveredObservationFact {
    Status(TaskStatus),
    StatusUnchanged(TaskIdentity),
    Unknown(TaskIdentity),
    Gone(TaskIdentity),
    TaskConvergence(TaskConvergenceReceipt),
    TaskConvergenceUnchanged(TaskIdentity),
    ContextConvergence(QueryContextConvergenceReceipt),
    Quiesce(Arc<QuiesceQueryContextReceipt>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CoveredObservationFrame {
    CatchUp(CoveredObservationFact),
    CatchUpComplete {
        initial_cut: u64,
    },
    Live {
        revision: u64,
        fact: CoveredObservationFact,
    },
}

/// Selection is not delivery. The caller must acknowledge the exact selected
/// frame only after its ordered transport has accepted the frame.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SelectedCoveredObservation {
    pub delivery_id: u64,
    pub frame: CoveredObservationFrame,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct CoveredObservationBookmark {
    pub generation: u64,
    pub sequence: u64,
    pub covered_prefix: u64,
    pub source_cut: u64,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct QuiesceObservationCursor {
    pub context: QueryContextRef,
    pub fence_version: Option<NonZeroU64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CoveredSubscriptionError {
    InvalidGeneration,
    MismatchedContext,
    FutureTaskCursor(TaskIdentity),
    FutureTaskConvergenceCursor(TaskIdentity),
    FutureContextCursor,
    FutureQuiesceCursor,
    TargetLimitExceeded,
    InitialSnapshotTooLarge { bytes: usize, limit: usize },
    FrameTooLarge { bytes: usize, limit: usize },
    WrongDelivery,
    SourceRegression,
    PublicationPending,
    SubscriptionInitializing,
    RevisionExhausted,
}

impl std::fmt::Display for CoveredSubscriptionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "covered observation subscription error: {self:?}"
        )
    }
}

impl std::error::Error for CoveredSubscriptionError {}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum ObservationKey {
    Status(TaskIdentity),
    Gone(TaskIdentity),
    TaskConvergence(TaskIdentity),
    ContextConvergence,
    Quiesce,
}

#[derive(Clone, Debug)]
struct OwedObservation {
    first_uncovered: u64,
    latest_revision: u64,
    fact: CoveredObservationFact,
}

#[derive(Clone, Debug)]
struct SelectedInternal {
    delivery_id: u64,
    frame: CoveredObservationFrame,
    key: Option<ObservationKey>,
    first_uncovered: Option<u64>,
}

#[derive(Debug)]
struct CoveredSubscriberState {
    generation: u64,
    bookmark_sequence: u64,
    initial_cut: u64,
    initial: VecDeque<CoveredObservationFact>,
    initial_complete: bool,
    selected: Option<SelectedInternal>,
    pending: BTreeMap<ObservationKey, OwedObservation>,
    order: BTreeSet<(u64, ObservationKey)>,
    tracked: BTreeSet<TaskIdentity>,
    max_targets: usize,
    overflow_at: Option<u64>,
    next_delivery_id: u64,
    noted_cut: u64,
    initialization: Option<InitialCatchUp>,
    retiring: bool,
}

impl CoveredSubscriberState {
    fn note_change(&mut self, key: ObservationKey, revision: u64, fact: CoveredObservationFact) {
        let identity = match key {
            ObservationKey::Status(identity)
            | ObservationKey::Gone(identity)
            | ObservationKey::TaskConvergence(identity) => Some(identity),
            ObservationKey::ContextConvergence | ObservationKey::Quiesce => None,
        };
        if let Some(identity) = identity
            && !self.tracked.contains(&identity)
        {
            if self.tracked.len() >= self.max_targets {
                self.overflow_at.get_or_insert(revision);
                return;
            }
            self.tracked.insert(identity);
        }
        if let Some(owed) = self.pending.get_mut(&key) {
            owed.latest_revision = revision;
            owed.fact = fact;
        } else {
            self.pending.insert(
                key,
                OwedObservation {
                    first_uncovered: revision,
                    latest_revision: revision,
                    fact,
                },
            );
            self.order.insert((revision, key));
        }
    }

    fn covered_prefix(&self, source_cut: u64) -> u64 {
        if !self.initial_complete {
            return 0;
        }
        let first = self
            .order
            .first()
            .map(|(revision, _)| *revision)
            .into_iter()
            .chain(
                self.selected
                    .as_ref()
                    .and_then(|selected| selected.first_uncovered),
            )
            .chain(self.overflow_at)
            .min();
        first.map_or(source_cut, |revision| revision - 1)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct SubscriptionId(pub u64);
#[derive(Clone, Debug)]
struct StoredFact {
    fact: CoveredObservationFact,
    bytes: usize,
}
/// The complete reconnect position. All vectors transfer by ownership; every
/// member is validated by a bounded catch-up step after `initial_cut` freezes.
#[derive(Debug, Default, Eq, PartialEq)]
pub struct CoveredSubscriptionRequest {
    pub status: Vec<TaskStatusCursor>,
    pub convergence: Vec<TaskConvergenceCursor>,
    pub context: Option<QueryContextConvergenceCursor>,
    pub quiesce: Option<QuiesceObservationCursor>,
    pub required: Vec<TaskIdentity>,
}
#[derive(Debug)]
struct InitialCatchUp {
    cursors: VecDeque<TaskStatusCursor>,
    requested: BTreeSet<TaskIdentity>,
    convergence: VecDeque<TaskConvergenceCursor>,
    convergence_requested: BTreeSet<TaskIdentity>,
    required: VecDeque<TaskIdentity>,
    context_cursor: Option<QueryContextConvergenceCursor>,
    context_observed: Option<novarocks_execution_contract::QueryContextConvergenceVersion>,
    quiesce_cursor: Option<QuiesceObservationCursor>,
    scan_after: Option<ObservationKey>,
    bytes: usize,
    scanned: bool,
    // Copy-on-change preserves the initial cut while source updates continue.
    // Each source key consumes at most one override for this subscriber.
    overrides: BTreeMap<ObservationKey, Option<StoredFact>>,
}
#[derive(Debug)]
struct Publication {
    revision: u64,
    key: ObservationKey,
    fact: CoveredObservationFact,
    previous: Vec<(ObservationKey, Option<StoredFact>)>,
    after: Option<SubscriptionId>,
    last: Option<SubscriptionId>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ObservationProgress {
    pub work: usize,
    pub complete: bool,
}
#[derive(Debug)]
pub struct ObservationSource {
    context: QueryContextRef,
    revision: u64,
    facts: BTreeMap<ObservationKey, StoredFact>,
    subscribers: BTreeMap<SubscriptionId, CoveredSubscriberState>,
    max_tasks: usize,
    max_subscribers: usize,
    max_frame_bytes: usize,
    max_initial_bytes: usize,
    targets: BTreeSet<TaskIdentity>,
    publication: Option<Publication>,
}
fn key(fact: &CoveredObservationFact) -> ObservationKey {
    match fact {
        CoveredObservationFact::Status(s) => ObservationKey::Status(s.identity()),
        CoveredObservationFact::StatusUnchanged(t) | CoveredObservationFact::Unknown(t) => {
            ObservationKey::Status(*t)
        }
        CoveredObservationFact::Gone(t) => ObservationKey::Gone(*t),
        CoveredObservationFact::TaskConvergence(r) => ObservationKey::TaskConvergence(r.identity()),
        CoveredObservationFact::TaskConvergenceUnchanged(t) => ObservationKey::TaskConvergence(*t),
        CoveredObservationFact::ContextConvergence(_) => ObservationKey::ContextConvergence,
        CoveredObservationFact::Quiesce(_) => ObservationKey::Quiesce,
    }
}
fn task(key: ObservationKey) -> Option<TaskIdentity> {
    match key {
        ObservationKey::Status(t)
        | ObservationKey::Gone(t)
        | ObservationKey::TaskConvergence(t) => Some(t),
        _ => None,
    }
}
impl ObservationSource {
    pub fn new(
        context: QueryContextRef,
        max_tasks: usize,
        max_subscribers: usize,
        max_frame_bytes: usize,
        max_initial_bytes: usize,
    ) -> Self {
        Self {
            context,
            revision: 0,
            facts: BTreeMap::new(),
            subscribers: BTreeMap::new(),
            max_tasks,
            max_subscribers,
            max_frame_bytes,
            max_initial_bytes,
            targets: BTreeSet::new(),
            publication: None,
        }
    }
    pub fn revision(&self) -> u64 {
        self.revision
    }
    /// `bytes` is the shared wire projection's conservative bounded cost,
    /// supplied before publication; no DTO or callback enters this state.
    pub fn publish(
        &mut self,
        fact: CoveredObservationFact,
        bytes: usize,
    ) -> Result<u64, CoveredSubscriptionError> {
        if self.publication.is_some() {
            return Err(CoveredSubscriptionError::PublicationPending);
        }
        if matches!(
            fact,
            CoveredObservationFact::StatusUnchanged(_)
                | CoveredObservationFact::Unknown(_)
                | CoveredObservationFact::TaskConvergenceUnchanged(_)
        ) {
            return Err(CoveredSubscriptionError::SourceRegression);
        }
        let k = key(&fact);
        if let Some(t) = task(k)
            && t.verify_query_context(self.context).is_err()
        {
            return Err(CoveredSubscriptionError::MismatchedContext);
        }
        match &fact {
            CoveredObservationFact::ContextConvergence(r) if r.context() != self.context => {
                return Err(CoveredSubscriptionError::MismatchedContext);
            }
            CoveredObservationFact::Quiesce(r) if r.context() != self.context => {
                return Err(CoveredSubscriptionError::MismatchedContext);
            }
            _ => {}
        }
        if bytes > self.max_frame_bytes {
            return Err(CoveredSubscriptionError::FrameTooLarge {
                bytes,
                limit: self.max_frame_bytes,
            });
        }
        if let Some(identity) = task(k)
            && !self.targets.contains(&identity)
            && self.targets.len() >= self.max_tasks
        {
            return Err(CoveredSubscriptionError::TargetLimitExceeded);
        }
        if let CoveredObservationFact::Status(status) = &fact {
            if self
                .facts
                .contains_key(&ObservationKey::Gone(status.identity()))
            {
                return Err(CoveredSubscriptionError::SourceRegression);
            }
            if let Some(StoredFact {
                fact: CoveredObservationFact::Status(previous),
                ..
            }) = self.facts.get(&k)
            {
                if status.version() < previous.version()
                    || (status.version() == previous.version() && status != previous)
                {
                    return Err(CoveredSubscriptionError::SourceRegression);
                }
                if status == previous {
                    return Ok(self.revision);
                }
            }
        }
        if let Some(previous) = self.facts.get(&k) {
            let progression = match (&previous.fact, &fact) {
                (
                    CoveredObservationFact::TaskConvergence(old),
                    CoveredObservationFact::TaskConvergence(new),
                ) => Some(new.version().cmp(&old.version())),
                (
                    CoveredObservationFact::ContextConvergence(old),
                    CoveredObservationFact::ContextConvergence(new),
                ) => Some(new.version().cmp(&old.version())),
                _ => None,
            };
            match progression {
                Some(std::cmp::Ordering::Less) => {
                    return Err(CoveredSubscriptionError::SourceRegression);
                }
                Some(std::cmp::Ordering::Equal) => return Ok(self.revision),
                _ => {}
            }
        }
        let mut previous = vec![(k, self.facts.get(&k).cloned())];
        if let CoveredObservationFact::Gone(identity) = &fact {
            for old in [
                ObservationKey::Status(*identity),
                ObservationKey::TaskConvergence(*identity),
            ] {
                previous.push((old, self.facts.remove(&old)));
            }
        }
        let revision = self
            .revision
            .checked_add(1)
            .ok_or(CoveredSubscriptionError::RevisionExhausted)?;
        self.revision = revision;
        if let Some(identity) = task(k) {
            self.targets.insert(identity);
        }
        self.facts.insert(
            k,
            StoredFact {
                fact: fact.clone(),
                bytes,
            },
        );
        self.publication = Some(Publication {
            revision,
            key: k,
            fact,
            previous,
            after: None,
            last: self.subscribers.last_key_value().map(|(id, _)| *id),
        });
        Ok(revision)
    }
    /// Complete at most `budget` subscriber visits. Until complete, the next
    /// publication stays in the runtime's already-admitted fact slot.
    pub fn continue_publication(&mut self, budget: usize) -> ObservationProgress {
        use std::ops::Bound::{Excluded, Included, Unbounded};
        let Some(mut publication) = self.publication.take() else {
            return ObservationProgress {
                work: 0,
                complete: true,
            };
        };
        let mut work = 0;
        while work < budget {
            let low = publication.after.map_or(Unbounded, Excluded);
            let Some(last) = publication.last else {
                break;
            };
            let Some((id, subscriber)) = self.subscribers.range_mut((low, Included(last))).next()
            else {
                break;
            };
            publication.after = Some(*id);
            work += 1;
            if subscriber.retiring || subscriber.initial_cut >= publication.revision {
                continue;
            }
            if let Some(initial) = &mut subscriber.initialization
                && !initial.scanned
            {
                for (key, old) in &publication.previous {
                    initial.overrides.entry(*key).or_insert_with(|| old.clone());
                }
            }
            subscriber.note_change(
                publication.key,
                publication.revision,
                publication.fact.clone(),
            );
            subscriber.noted_cut = publication.revision;
        }
        let more = publication.last.is_some_and(|last| {
            let low = publication.after.map_or(Unbounded, Excluded);
            self.subscribers
                .range((low, Included(last)))
                .next()
                .is_some()
        });
        if more {
            self.publication = Some(publication);
        }
        ObservationProgress {
            work,
            complete: !more,
        }
    }
    /// Registers the subscriber and freezes `initial_cut` before inspecting any
    /// cursor or target. Input vector ownership transfers in constant time.
    /// Call `continue_subscribe` until complete before selecting frames.
    pub fn begin_subscribe(
        &mut self,
        id: SubscriptionId,
        generation: u64,
        cursors: Vec<TaskStatusCursor>,
    ) -> Result<u64, CoveredSubscriptionError> {
        self.begin_covered_subscription(
            id,
            generation,
            CoveredSubscriptionRequest {
                status: cursors,
                ..CoveredSubscriptionRequest::default()
            },
        )
    }
    pub fn begin_covered_subscription(
        &mut self,
        id: SubscriptionId,
        generation: u64,
        request: CoveredSubscriptionRequest,
    ) -> Result<u64, CoveredSubscriptionError> {
        if generation == 0 {
            return Err(CoveredSubscriptionError::InvalidGeneration);
        }
        if self.subscribers.contains_key(&id)
            || self.subscribers.len() >= self.max_subscribers
            || request.status.len() > self.max_tasks
            || request.convergence.len() > self.max_tasks
            || request.required.len() > self.max_tasks
        {
            return Err(CoveredSubscriptionError::TargetLimitExceeded);
        }
        self.subscribers.insert(
            id,
            CoveredSubscriberState {
                generation,
                bookmark_sequence: 0,
                initial_cut: self.revision,
                initial: VecDeque::new(),
                initial_complete: false,
                selected: None,
                pending: BTreeMap::new(),
                order: BTreeSet::new(),
                tracked: BTreeSet::new(),
                max_targets: self.max_tasks,
                overflow_at: None,
                next_delivery_id: 1,
                noted_cut: self.revision,
                retiring: false,
                initialization: Some(InitialCatchUp {
                    cursors: request.status.into(),
                    convergence: request.convergence.into(),
                    convergence_requested: BTreeSet::new(),
                    required: request.required.into(),
                    context_observed: request.context.and_then(|cursor| cursor.current_version()),
                    context_cursor: request.context,
                    quiesce_cursor: request.quiesce,
                    requested: BTreeSet::new(),
                    scan_after: None,
                    bytes: 0,
                    scanned: false,
                    overrides: BTreeMap::new(),
                }),
            },
        );
        Ok(self.revision)
    }
    /// Each unit validates one cursor or visits one frozen source key. A
    /// concurrent publication's old values are visible even before its fanout
    /// reaches this subscriber, so catch-up never reads beyond `initial_cut`.
    pub fn continue_subscribe(
        &mut self,
        id: SubscriptionId,
        budget: usize,
    ) -> Result<ObservationProgress, CoveredSubscriptionError> {
        let result = self.continue_subscribe_inner(id, budget);
        if result.is_err()
            && let Some(subscriber) = self.subscribers.get_mut(&id)
        {
            // Failed initialization retains its prepaid storage until bounded
            // cleanup, but can never expose its partial initial snapshot.
            subscriber.retiring = true;
        }
        result
    }
    fn continue_subscribe_inner(
        &mut self,
        id: SubscriptionId,
        budget: usize,
    ) -> Result<ObservationProgress, CoveredSubscriptionError> {
        use std::ops::Bound::{Excluded, Unbounded};
        let subscriber = self
            .subscribers
            .get_mut(&id)
            .ok_or(CoveredSubscriptionError::WrongDelivery)?;
        if subscriber.retiring {
            return Err(CoveredSubscriptionError::WrongDelivery);
        }
        let initial_cut = subscriber.initial_cut;
        let Some(initial) = subscriber.initialization.as_mut() else {
            return Ok(ObservationProgress {
                work: 0,
                complete: true,
            });
        };
        let frozen =
            |key: ObservationKey, overrides: &BTreeMap<ObservationKey, Option<StoredFact>>| {
                if let Some(old) = overrides.get(&key) {
                    return old.clone();
                }
                if let Some(publication) = &self.publication
                    && initial_cut < publication.revision
                    && let Some((_, old)) = publication.previous.iter().find(|(k, _)| *k == key)
                {
                    return old.clone();
                }
                self.facts.get(&key).cloned()
            };
        let mut work = 0;
        let mut complete = false;
        while work < budget {
            if initial.scanned {
                // The completed snapshot no longer needs old-value/position
                // metadata. Each map member is retired in a charged slice.
                work += 1;
                if initial.overrides.pop_first().is_some() {
                    continue;
                }
                if initial.requested.pop_first().is_some() {
                    continue;
                }
                if initial.convergence_requested.pop_first().is_some() {
                    continue;
                }
                complete = true;
                break;
            }
            if let Some(cursor) = initial.cursors.pop_front() {
                work += 1;
                let identity = cursor.identity();
                if identity.verify_query_context(self.context).is_err() {
                    return Err(CoveredSubscriptionError::MismatchedContext);
                }
                if !initial.requested.insert(identity) {
                    continue;
                }
                subscriber.tracked.insert(identity);
                let stored = frozen(ObservationKey::Status(identity), &initial.overrides);
                let (fact, bytes) = match stored {
                    Some(stored) => {
                        if let CoveredObservationFact::Status(status) = &stored.fact {
                            if cursor
                                .current_version()
                                .is_some_and(|v| v > status.version())
                            {
                                return Err(CoveredSubscriptionError::FutureTaskCursor(identity));
                            }
                            if cursor.current_version() == Some(status.version()) {
                                (
                                    CoveredObservationFact::StatusUnchanged(identity),
                                    stored.bytes,
                                )
                            } else {
                                (stored.fact, stored.bytes)
                            }
                        } else {
                            (stored.fact, stored.bytes)
                        }
                    }
                    None => {
                        if let Some(gone) =
                            frozen(ObservationKey::Gone(identity), &initial.overrides)
                        {
                            (gone.fact, gone.bytes)
                        } else {
                            if cursor.current_version().is_some() {
                                return Err(CoveredSubscriptionError::FutureTaskCursor(identity));
                            }
                            (CoveredObservationFact::Unknown(identity), 64)
                        }
                    }
                };
                initial.bytes = initial.bytes.saturating_add(bytes);
                subscriber.initial.push_back(fact);
            } else if let Some(cursor) = initial.convergence.pop_front() {
                work += 1;
                let identity = cursor.identity();
                if identity.verify_query_context(self.context).is_err() {
                    return Err(CoveredSubscriptionError::MismatchedContext);
                }
                if !initial.convergence_requested.insert(identity) {
                    continue;
                }
                subscriber.tracked.insert(identity);
                if initial.requested.insert(identity) {
                    let status = frozen(ObservationKey::Status(identity), &initial.overrides)
                        .or_else(|| frozen(ObservationKey::Gone(identity), &initial.overrides));
                    let (fact, bytes) = status
                        .map_or((CoveredObservationFact::Unknown(identity), 64), |stored| {
                            (stored.fact, stored.bytes)
                        });
                    initial.bytes = initial.bytes.saturating_add(bytes);
                    subscriber.initial.push_back(fact);
                }
                let stored = frozen(
                    ObservationKey::TaskConvergence(identity),
                    &initial.overrides,
                );
                match stored {
                    Some(stored) => {
                        let CoveredObservationFact::TaskConvergence(receipt) = &stored.fact else {
                            return Err(CoveredSubscriptionError::SourceRegression);
                        };
                        if cursor
                            .current_version()
                            .is_some_and(|version| version > receipt.version())
                        {
                            return Err(CoveredSubscriptionError::FutureTaskConvergenceCursor(
                                identity,
                            ));
                        }
                        initial.bytes = initial.bytes.saturating_add(stored.bytes);
                        subscriber.initial.push_back(
                            if cursor.current_version() == Some(receipt.version()) {
                                CoveredObservationFact::TaskConvergenceUnchanged(identity)
                            } else {
                                stored.fact
                            },
                        );
                    }
                    None if cursor.current_version().is_some() => {
                        return Err(CoveredSubscriptionError::FutureTaskConvergenceCursor(
                            identity,
                        ));
                    }
                    None => {}
                }
            } else if let Some(identity) = initial.required.pop_front() {
                work += 1;
                if identity.verify_query_context(self.context).is_err() {
                    return Err(CoveredSubscriptionError::MismatchedContext);
                }
                subscriber.tracked.insert(identity);
                if initial.requested.insert(identity) {
                    let stored = frozen(ObservationKey::Status(identity), &initial.overrides)
                        .or_else(|| frozen(ObservationKey::Gone(identity), &initial.overrides));
                    let (fact, bytes) = stored
                        .map_or((CoveredObservationFact::Unknown(identity), 64), |stored| {
                            (stored.fact, stored.bytes)
                        });
                    initial.bytes = initial.bytes.saturating_add(bytes);
                    subscriber.initial.push_back(fact);
                }
            } else if let Some(cursor) = initial.context_cursor.take() {
                work += 1;
                if cursor.context() != self.context {
                    return Err(CoveredSubscriptionError::MismatchedContext);
                }
                let stored = frozen(ObservationKey::ContextConvergence, &initial.overrides);
                if let Some(version) = cursor.current_version()
                    && stored.as_ref().is_none_or(|stored| !matches!(stored.fact, CoveredObservationFact::ContextConvergence(receipt) if version <= receipt.version()))
                { return Err(CoveredSubscriptionError::FutureContextCursor); }
            } else if let Some(cursor) = initial.quiesce_cursor.take() {
                work += 1;
                if cursor.context != self.context {
                    return Err(CoveredSubscriptionError::MismatchedContext);
                }
                let stored = frozen(ObservationKey::Quiesce, &initial.overrides);
                if let Some(version) = cursor.fence_version
                    && stored.as_ref().is_none_or(|stored| !matches!(&stored.fact, CoveredObservationFact::Quiesce(receipt) if version.get() <= receipt.fence_version()))
                { return Err(CoveredSubscriptionError::FutureQuiesceCursor); }
            } else {
                let lower = initial.scan_after.map_or(Unbounded, Excluded);
                let pending_old = self
                    .publication
                    .as_ref()
                    .filter(|p| subscriber.initial_cut < p.revision)
                    .into_iter()
                    .flat_map(|p| p.previous.iter().map(|(k, _)| *k))
                    .filter(|k| initial.scan_after.is_none_or(|after| *k > after))
                    .min();
                let candidate = self
                    .facts
                    .range((lower, Unbounded))
                    .next()
                    .map(|(k, _)| *k)
                    .into_iter()
                    .chain(
                        initial
                            .overrides
                            .range((lower, Unbounded))
                            .next()
                            .map(|(k, _)| *k),
                    )
                    .chain(pending_old)
                    .min();
                let Some(k) = candidate else {
                    initial.scanned = true;
                    work += 1;
                    continue;
                };
                initial.scan_after = Some(k);
                work += 1;
                let Some(stored) = frozen(k, &initial.overrides) else {
                    continue;
                };
                if let Some(identity) = task(k) {
                    if ((matches!(k, ObservationKey::Status(_) | ObservationKey::Gone(_)))
                        && initial.requested.contains(&identity))
                        || (matches!(k, ObservationKey::TaskConvergence(_))
                            && initial.convergence_requested.contains(&identity))
                    {
                        continue;
                    }
                    subscriber.tracked.insert(identity);
                }
                if let CoveredObservationFact::ContextConvergence(receipt) = &stored.fact
                    && initial.context_observed == Some(receipt.version())
                {
                    continue;
                }
                initial.bytes = initial.bytes.saturating_add(stored.bytes);
                subscriber.initial.push_back(stored.fact);
            }
            if subscriber.tracked.len() > self.max_tasks {
                return Err(CoveredSubscriptionError::TargetLimitExceeded);
            }
            if initial.bytes > self.max_initial_bytes {
                return Err(CoveredSubscriptionError::InitialSnapshotTooLarge {
                    bytes: initial.bytes,
                    limit: self.max_initial_bytes,
                });
            }
        }
        if complete {
            subscriber.initialization = None;
        }
        Ok(ObservationProgress { work, complete })
    }
    /// Close the public subscription immediately; its prepaid metadata stays
    /// resident and counted until `continue_unsubscribe` drains every member.
    pub fn unsubscribe(&mut self, id: SubscriptionId) -> bool {
        let Some(subscriber) = self.subscribers.get_mut(&id) else {
            return false;
        };
        subscriber.retiring = true;
        true
    }
    pub fn continue_unsubscribe(
        &mut self,
        id: SubscriptionId,
        budget: usize,
    ) -> Result<ObservationProgress, CoveredSubscriptionError> {
        let subscriber = self
            .subscribers
            .get_mut(&id)
            .ok_or(CoveredSubscriptionError::WrongDelivery)?;
        if !subscriber.retiring {
            return Err(CoveredSubscriptionError::WrongDelivery);
        }
        let mut work = 0;
        let mut complete = false;
        while work < budget {
            work += 1;
            if let Some(initial) = subscriber.initialization.as_mut() {
                if initial.overrides.pop_first().is_some() {
                    continue;
                }
                if initial.requested.pop_first().is_some() {
                    continue;
                }
                if initial.convergence_requested.pop_first().is_some() {
                    continue;
                }
                // These remaining cursor vectors contain only Copy value IDs.
                initial.cursors.clear();
                initial.convergence.clear();
                initial.required.clear();
                subscriber.initialization = None;
                continue;
            }
            if subscriber.selected.take().is_some() {
                continue;
            }
            if subscriber.initial.pop_front().is_some() {
                continue;
            }
            if subscriber.pending.pop_first().is_some() {
                continue;
            }
            if subscriber.order.pop_first().is_some() {
                continue;
            }
            if subscriber.tracked.pop_first().is_some() {
                continue;
            }
            complete = true;
            break;
        }
        if complete {
            self.subscribers.remove(&id);
        }
        Ok(ObservationProgress { work, complete })
    }
    pub fn select_next(
        &mut self,
        id: SubscriptionId,
    ) -> Result<Option<SelectedCoveredObservation>, CoveredSubscriptionError> {
        let source_cut = self.revision;
        let s = self
            .subscribers
            .get_mut(&id)
            .ok_or(CoveredSubscriptionError::WrongDelivery)?;
        if s.retiring {
            return Err(CoveredSubscriptionError::WrongDelivery);
        }
        if s.initialization.is_some() {
            return Err(CoveredSubscriptionError::SubscriptionInitializing);
        }
        if let Some(selected) = &s.selected {
            return Ok(Some(SelectedCoveredObservation {
                delivery_id: selected.delivery_id,
                frame: selected.frame.clone(),
            }));
        }
        let next = if let Some(fact) = s.initial.front().cloned() {
            Some((CoveredObservationFrame::CatchUp(fact), None, None))
        } else if !s.initial_complete {
            Some((
                CoveredObservationFrame::CatchUpComplete {
                    initial_cut: s.initial_cut,
                },
                None,
                None,
            ))
        } else if let Some((first, key)) = s.order.pop_first() {
            let owed = s.pending.remove(&key).expect("pending order has a fact");
            debug_assert_eq!(
                first, owed.first_uncovered,
                "owed ordering index matches its retained first debt"
            );
            Some((
                CoveredObservationFrame::Live {
                    revision: owed.latest_revision,
                    fact: owed.fact,
                },
                Some(key),
                Some(owed.first_uncovered),
            ))
        } else {
            None
        };
        let Some((frame, key, first_uncovered)) = next else {
            let _ = source_cut;
            return Ok(None);
        };
        let delivery_id = s.next_delivery_id;
        s.next_delivery_id = delivery_id
            .checked_add(1)
            .expect("delivery identity exhausted");
        s.selected = Some(SelectedInternal {
            delivery_id,
            frame: frame.clone(),
            key,
            first_uncovered,
        });
        Ok(Some(SelectedCoveredObservation { delivery_id, frame }))
    }
    pub fn note_delivered(
        &mut self,
        id: SubscriptionId,
        delivery: u64,
    ) -> Result<CoveredObservationBookmark, CoveredSubscriptionError> {
        let s = self
            .subscribers
            .get_mut(&id)
            .ok_or(CoveredSubscriptionError::WrongDelivery)?;
        if s.retiring {
            return Err(CoveredSubscriptionError::WrongDelivery);
        }
        if s.selected
            .as_ref()
            .is_none_or(|v| v.delivery_id != delivery)
        {
            return Err(CoveredSubscriptionError::WrongDelivery);
        }
        let selected = s.selected.take().expect("checked selection");
        match selected.frame {
            CoveredObservationFrame::CatchUp(_) => {
                s.initial.pop_front();
            }
            CoveredObservationFrame::CatchUpComplete { .. } => s.initial_complete = true,
            CoveredObservationFrame::Live { .. } => {}
        }
        let _ = selected.key;
        s.bookmark_sequence = s
            .bookmark_sequence
            .checked_add(1)
            .expect("bookmark sequence exhausted");
        Ok(CoveredObservationBookmark {
            generation: s.generation,
            sequence: s.bookmark_sequence,
            covered_prefix: s.covered_prefix(s.noted_cut),
            source_cut: s.noted_cut,
        })
    }
    pub fn bookmark(&mut self, id: SubscriptionId) -> Option<CoveredObservationBookmark> {
        let s = self.subscribers.get_mut(&id)?;
        if s.retiring {
            return None;
        }
        s.bookmark_sequence = s
            .bookmark_sequence
            .checked_add(1)
            .expect("bookmark sequence exhausted");
        Some(CoveredObservationBookmark {
            generation: s.generation,
            sequence: s.bookmark_sequence,
            covered_prefix: s.covered_prefix(s.noted_cut),
            source_cut: s.noted_cut,
        })
    }
}
