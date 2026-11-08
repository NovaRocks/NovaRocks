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

//! Original backend/lane/FIFO expiry order, traversed under the turn budget.
//! Earliest deadline is only a wake index; it cannot select the first failure.
use super::{ExpiredOperation, OperationDispatcher};
use crate::capacity::StepContext;
use novarocks_coordination_contract::MonotonicInstant;
use novarocks_types::BackendProcessId;
use std::ops::Bound::{Excluded, Unbounded};

#[derive(Debug)]
pub(crate) struct QueueExpiryCursor {
    cut: MonotonicInstant,
    backend: Option<BackendProcessId>,
    lane: usize,
    after: Option<i128>,
    complete: bool,
}
#[derive(Debug)]
pub(crate) enum ExpiryProgress {
    Yield,
    Expired(ExpiredOperation),
    Complete,
}
impl QueueExpiryCursor {
    /// The caller enters the original expiry phase at this clock sample. No
    /// primary mutation, enqueue, dispatch or cleanup may cross this
    /// continuation and erase the original phase's expired first cause.
    /// Position stability alone is not permission for such a mutation.
    pub(crate) fn new(cut: MonotonicInstant) -> Self {
        Self {
            cut,
            backend: None,
            lane: 0,
            after: None,
            complete: false,
        }
    }
    pub(crate) fn advance(
        &mut self,
        dispatcher: &mut OperationDispatcher,
        context: &mut dyn StepContext,
    ) -> ExpiryProgress {
        loop {
            if dispatcher
                .queued_times
                .first()
                .is_none_or(|&(queued_at, _)| {
                    self.cut.saturating_duration_since(queued_at)
                        < dispatcher.transport.frontend_queue_residence()
                })
            {
                self.complete = true;
            }
            if self.complete {
                return ExpiryProgress::Complete;
            }
            if self.backend.is_none() {
                if !context.charge(1) {
                    return ExpiryProgress::Yield;
                }
                self.backend = dispatcher
                    .backends
                    .first_key_value()
                    .map(|(&backend, _)| backend);
                if self.backend.is_none() {
                    self.complete = true;
                    continue;
                }
            }
            let backend = self.backend.expect("original expiry backend");
            if self.lane == 4 {
                if !context.charge(1) {
                    return ExpiryProgress::Yield;
                }
                self.backend = dispatcher
                    .backends
                    .range((Excluded(backend), Unbounded))
                    .next()
                    .map(|(&backend, _)| backend);
                self.complete = self.backend.is_none();
                self.lane = 0;
                self.after = None;
                continue;
            }
            // Charge the actual row, or the bounded empty-lane transition,
            // before moving the original cursor or a retained grant.
            if !context.charge(1) {
                return ExpiryProgress::Yield;
            }
            let lane = &dispatcher.backends[&backend].lanes[self.lane];
            let row = match self.after {
                Some(after) => lane.entries.range((Excluded(after), Unbounded)).next(),
                None => lane.entries.first_key_value(),
            };
            let Some((&position, entry)) = row else {
                self.lane += 1;
                self.after = None;
                continue;
            };
            self.after = Some(position);
            let waited = self.cut.saturating_duration_since(entry.queued_at);
            if waited < dispatcher.transport.frontend_queue_residence() {
                continue;
            }
            let operation = entry.intent.operation_id();
            let entry = dispatcher
                .remove_queued(operation)
                .expect("original expiry row");
            return ExpiryProgress::Expired(ExpiredOperation {
                intent: entry.intent,
                waited,
                _queue_permit: entry.queue_permit,
            });
        }
    }
}
