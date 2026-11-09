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

//! Budgeted selection of the next exact original Native batch.

use super::{LANES, OperationDispatcher, batch_allowance, lane_index};
use crate::capacity::{QueueStage, StepContext};
use crate::config::DispatchLane;
use crate::effect::{AttemptEffect, AttemptEffectId, EffectBody, EffectClass};
use crate::error::TaskExecutionError;

#[derive(Debug)]
pub(crate) enum BatchDispatch {
    Ready(AttemptEffect),
    /// The original ledger installed the capacity wake. No immediate retry.
    Blocked,
    Yield,
    Empty,
}

/// A bounded scan of the original backend rotation. The Core owns this
/// cursor until the dispatch phase ends and applies no new primary mutation
/// across a yield. Neither a yield nor a refusal consumes any queued grant.
#[derive(Debug)]
pub(crate) struct BatchDispatchPass {
    control: bool,
    offset: usize,
    selected_lane: Option<usize>,
    serial: Option<u64>,
    skipped_target: bool,
}

impl BatchDispatchPass {
    pub(crate) const fn new() -> Self {
        Self {
            control: true,
            offset: 0,
            selected_lane: None,
            serial: None,
            skipped_target: false,
        }
    }

    /// One unit per scanned backend, then up to five visits to the same
    /// members: preview, dequeue with index updates, validate the acceptance,
    /// install the in-flight entries, and record exact batch membership.
    /// The host includes this atomic batch cost in its original step budget.
    pub(crate) fn max_batch_units(max_batch_items: usize) -> Option<usize> {
        max_batch_items.checked_mul(5)?.checked_add(1)
    }

    pub(crate) fn advance(
        &mut self,
        dispatcher: &mut OperationDispatcher,
        effect: AttemptEffectId,
        context: &mut dyn StepContext,
    ) -> Result<BatchDispatch, TaskExecutionError> {
        let backends = dispatcher.rotation.len();
        if dispatcher.queued_items == 0 {
            *self = Self::new();
            return Ok(BatchDispatch::Empty);
        }
        let serial = if let Some(serial) = self.serial {
            serial
        } else {
            let serial = dispatcher
                .dispatch_pass_serial
                .checked_add(1)
                .ok_or_else(|| {
                    TaskExecutionError::Schedule("dispatch pass identity exhausted".into())
                })?;
            dispatcher.dispatch_pass_serial = serial;
            self.serial = Some(serial);
            serial
        };
        loop {
            if self.offset == backends {
                if !self.control {
                    let blocked = self.skipped_target;
                    *self = Self::new();
                    return Ok(if blocked {
                        BatchDispatch::Blocked
                    } else {
                        BatchDispatch::Empty
                    });
                }
                self.control = false;
                self.offset = 0;
            }
            let index = (dispatcher.rotation_cursor + self.offset) % backends;
            let backend = dispatcher.rotation[index];
            let lane = if let Some(lane) = self.selected_lane {
                lane
            } else {
                if !context.charge(1) {
                    return Ok(BatchDispatch::Yield);
                }
                let queues = dispatcher
                    .backends
                    .get_mut(&backend)
                    .expect("original backend");
                if queues.blocked_dispatch_pass == serial {
                    self.offset += 1;
                    continue;
                }
                let lane = if self.control {
                    Some(lane_index(DispatchLane::Control))
                } else if queues.is_empty() {
                    None
                } else {
                    OperationDispatcher::pick_lane(queues, dispatcher.budget)
                };
                let Some(lane) = lane else {
                    self.offset += 1;
                    continue;
                };
                self.selected_lane = Some(lane);
                lane
            };
            let queues = &dispatcher.backends[&backend];
            let queue = &queues.lanes[lane];
            let allowance = batch_allowance(
                queues,
                lane,
                dispatcher.budget,
                dispatcher.transport.max_batch_items(),
            )
            .min(queue.len());
            if allowance == 0 {
                self.selected_lane = None;
                self.offset += 1;
                continue;
            }
            let units = Self::max_batch_units(allowance).ok_or_else(|| {
                TaskExecutionError::Schedule("dispatch batch work quote overflow".into())
            })?;
            if !context.charge(units - 1) {
                return Ok(BatchDispatch::Yield);
            }

            let prefix = queue
                .batch_prefix(allowance, dispatcher.transport.max_batch_encoded_bytes())
                .expect("enqueue validated the original head carrier");
            let reservation = match context.reserve_queue_stage(
                LANES[lane],
                prefix.control_progress,
                backend,
                prefix.items,
                prefix.bytes,
            ) {
                QueueStage::Granted(reservation) => reservation,
                QueueStage::TargetFull(target) if target == backend => {
                    dispatcher
                        .backends
                        .get_mut(&backend)
                        .expect("quoted backend")
                        .blocked_dispatch_pass = serial;
                    self.skipped_target = true;
                    self.selected_lane = None;
                    self.offset += 1;
                    continue;
                }
                QueueStage::TargetFull(_) => {
                    return Err(TaskExecutionError::Schedule(
                        "capacity refusal differs from its original backend".into(),
                    ));
                }
                QueueStage::ProcessFull => {
                    *self = Self::new();
                    return Ok(BatchDispatch::Blocked);
                }
            };
            let batch = dispatcher.take_batch_prefix(backend, lane, prefix);
            dispatcher.accept_emitted(&batch, effect)?;
            dispatcher.rotation_cursor = (index + 1) % backends;
            self.control = true;
            self.offset = 0;
            self.selected_lane = None;
            let class = if batch.control_progress {
                EffectClass::Settlement
            } else {
                EffectClass::Business
            };
            return Ok(BatchDispatch::Ready(AttemptEffect {
                id: effect,
                class,
                body: EffectBody::SendBatch { batch, reservation },
            }));
        }
    }
}
