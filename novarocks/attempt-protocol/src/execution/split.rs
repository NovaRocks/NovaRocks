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

//! Correlate the original split delivery with the Task's immutable Update.

use super::QueryTaskExecution;
use crate::{
    effect::{
        AttemptEffect, AttemptEffectId, EffectBody, PumpKind, QueuedInputPayload, TaskDomainIntent,
    },
    error::TaskExecutionError,
    input::{OperationReply, SplitDeliveryRef, UpdateAdmission},
    intent::TaskOperationQueueRequest,
    remote_task::RemoteTask,
    split_delivery::{self, SplitDeliverySettlement},
    start::PayloadRef,
};
use novarocks_execution_contract::{TaskIdentity, TaskOperationId};
use std::{
    collections::{BTreeMap, VecDeque},
    ops::Bound::{Excluded, Unbounded},
};

#[derive(Debug, Default, Eq, PartialEq)]
enum PumpState {
    #[default]
    Dormant,
    Running,
    Closing,
    Finished,
}

#[derive(Debug)]
struct Pending {
    delivery: SplitDeliveryRef,
    domain: TaskDomainIntent,
}

#[derive(Debug)]
struct Active {
    task: TaskIdentity,
    delivery: SplitDeliveryRef,
    released: bool,
}

#[derive(Debug, Default)]
pub(super) struct SplitDeliveries {
    source: Option<PayloadRef>,
    state: PumpState,
    pending: BTreeMap<TaskIdentity, Pending>,
    tasks: BTreeMap<TaskIdentity, SplitDeliveryRef>,
    active: BTreeMap<TaskOperationId, Active>,
    notices: VecDeque<EffectBody>,
}

impl SplitDeliveries {
    fn admission(
        &mut self,
        delivery: SplitDeliveryRef,
        settlement: Option<SplitDeliverySettlement>,
    ) {
        self.notices.push_back(EffectBody::PumpUpdateAdmitted {
            pump: self.source.expect("registered split pump"),
            delivery,
            settlement,
        });
    }

    fn discard(
        &mut self,
        task: TaskIdentity,
        delivery: SplitDeliveryRef,
        settlement: SplitDeliverySettlement,
    ) {
        self.tasks.remove(&task);
        self.admission(delivery, Some(settlement));
    }
}

impl QueryTaskExecution {
    pub(crate) fn bind_split_pump(&mut self, source: Option<PayloadRef>) {
        self.split.source = source;
    }

    pub(crate) fn split_pump_lifecycle_due(&self, running: bool) -> bool {
        self.split.source.is_some()
            && matches!(
                (&self.split.state, running),
                (PumpState::Dormant, true) | (PumpState::Running, false)
            )
    }

    pub(crate) fn split_pump_lifecycle(&mut self, running: bool) -> Option<EffectBody> {
        let pump = self.split.source?;
        match (&self.split.state, running) {
            (PumpState::Dormant, true) => {
                self.split.state = PumpState::Running;
                Some(EffectBody::StartPump {
                    pump,
                    kind: PumpKind::SplitAssignment,
                })
            }
            (PumpState::Running, false) => {
                self.split.state = PumpState::Closing;
                Some(EffectBody::StopPump { pump })
            }
            _ => None,
        }
    }

    pub(crate) fn finish_split_pump(&mut self, source: PayloadRef) -> bool {
        if self.split.source == Some(source)
            && matches!(self.split.state, PumpState::Running | PumpState::Closing)
        {
            self.split.state = PumpState::Finished;
            return true;
        }
        false
    }

    pub(crate) fn is_split_pump(&self, source: PayloadRef) -> bool {
        self.split.source == Some(source) && self.split.state != PumpState::Dormant
    }

    pub(crate) fn accept_split_delivery(
        &mut self,
        source: PayloadRef,
        delivery: SplitDeliveryRef,
        task: TaskIdentity,
        domains: Box<[TaskDomainIntent]>,
    ) -> Result<(), TaskExecutionError> {
        if !self.is_split_pump(source) {
            return Err(TaskExecutionError::Schedule(
                "delivery has no original split pump".to_owned(),
            ));
        }
        let mut domains = domains.into_vec().into_iter();
        let Some(domain @ TaskDomainIntent::SplitAssignment { .. }) = domains.next() else {
            return Err(TaskExecutionError::Schedule(
                "split delivery has no original assignment".to_owned(),
            ));
        };
        if domains.next().is_some() || self.task_by_identity_ref(task).is_none() {
            return Err(TaskExecutionError::UnknownOperation);
        }
        if let Some(previous) = self.split.tasks.get(&task) {
            if *previous == delivery {
                return Ok(());
            }
            return Err(TaskExecutionError::Schedule(
                "split producer already owns a delivery for this Task".to_owned(),
            ));
        }
        self.split.tasks.insert(task, delivery);
        self.split
            .pending
            .insert(task, Pending { delivery, domain });
        Ok(())
    }

    pub(crate) fn split_task_after(&self, after: Option<TaskIdentity>) -> Option<TaskIdentity> {
        match after {
            Some(after) => self
                .split
                .pending
                .range((Excluded(after), Unbounded))
                .next(),
            None => self.split.pending.first_key_value(),
        }
        .map(|(&task, _)| task)
    }

    pub(crate) fn prepare_split_delivery(
        &mut self,
        task: TaskIdentity,
        action: AttemptEffectId,
    ) -> Result<Option<AttemptEffect>, TaskExecutionError> {
        let stopping = self.terminal_cleanup_started || self.split.state != PumpState::Running;
        if !stopping
            && !self.normal_drain_started
            && self
                .task_by_identity_ref(task)
                .is_some_and(RemoteTask::input_preparation_pending)
        {
            return Ok(None);
        }
        let Pending { delivery, domain } = self
            .split
            .pending
            .remove(&task)
            .expect("original pending split");
        if stopping || self.normal_drain_started {
            let settlement = if stopping {
                SplitDeliverySettlement::Unknown(
                    "split assignment stopped before Native admission".to_owned(),
                )
            } else {
                split_delivery::admission(task, Ok(UpdateAdmission::DiscardedNoLongerNeeded))
                    .expect("normal input closure")
            };
            self.split.discard(task, delivery, settlement);
            return Ok(None);
        }
        let TaskDomainIntent::SplitAssignment { payload, .. } = domain else {
            unreachable!()
        };
        let validation =
            self.dispatcher
                .validate_queue_request(TaskOperationQueueRequest::task_update(
                    task.backend_process_id(),
                    &domain,
                ));
        let (disposition, intent) =
            match validation.and_then(|()| self.prepare_task_update(task, domain)) {
                Ok(value) => value,
                Err(error) => {
                    let settlement = split_delivery::admission(task, Err(&error))
                        .expect("local split admission failure");
                    self.split.discard(task, delivery, settlement);
                    return Ok(None);
                }
            };
        let Some(intent) = intent else {
            let settlement = split_delivery::admission(task, Ok(disposition))
                .expect("final split admission disposition");
            self.split.discard(task, delivery, settlement);
            return Ok(None);
        };
        let operation = intent.operation_id();
        let effect = self.begin_initial_update_admission(
            intent,
            action,
            QueuedInputPayload::NewTaskUpdate(payload),
        )?;
        self.split.active.insert(
            operation,
            Active {
                task,
                delivery,
                released: false,
            },
        );
        Ok(Some(effect))
    }

    pub(super) fn note_split_admission(
        &mut self,
        operation: TaskOperationId,
        result: Result<UpdateAdmission, &TaskExecutionError>,
    ) -> bool {
        let Some(row) = self.split.active.get(&operation) else {
            return false;
        };
        if let Some(settlement) = split_delivery::admission(row.task, result) {
            self.discard_unsent_split(operation, settlement);
        } else {
            self.split.admission(row.delivery, None);
        }
        true
    }

    pub(super) fn discard_unsent_split(
        &mut self,
        operation: TaskOperationId,
        settlement: SplitDeliverySettlement,
    ) {
        if self
            .split
            .active
            .get(&operation)
            .is_some_and(|row| !row.released)
        {
            let row = self
                .split
                .active
                .remove(&operation)
                .expect("unreleased split");
            self.split.discard(row.task, row.delivery, settlement);
        }
    }

    pub(crate) fn note_split_released(&mut self, operation: TaskOperationId) {
        let Some(row) = self.split.active.get_mut(&operation) else {
            return;
        };
        if !row.released {
            row.released = true;
            self.split
                .notices
                .push_back(EffectBody::PumpOperationReleased {
                    pump: self.split.source.expect("registered split pump"),
                    delivery: row.delivery,
                    operation,
                });
        }
    }

    pub(super) fn finish_split_no_more_input(&mut self, operation: TaskOperationId) {
        let Some(row) = self.split.active.get(&operation) else {
            return;
        };
        let settlement =
            split_delivery::admission(row.task, Ok(UpdateAdmission::DiscardedNoLongerNeeded))
                .expect("normal input closure");
        if row.released {
            self.settle_split(operation, settlement);
        } else {
            self.discard_unsent_split(operation, settlement);
        }
    }

    pub(crate) fn note_split_reply(&mut self, reply: &OperationReply) {
        let operation = reply.operation_id();
        let Some(row) = self.split.active.get(&operation).filter(|row| row.released) else {
            return;
        };
        if reply.dispatch_result() == crate::operation::OperationDispatchResult::TransportUnknown
            && self.task_by_identity_ref(row.task).is_some_and(|task| {
                !task.update_request_matches(operation)
                    && task.normally_stood_down()
                    && task
                        .status()
                        .and_then(novarocks_execution_contract::TaskStatus::termination)
                        .is_none_or(
                            novarocks_execution_contract::TerminationDetail::is_success_compatible,
                        )
            })
        {
            self.finish_split_no_more_input(operation);
        } else {
            self.settle_split(operation, split_delivery::settle(reply));
        }
    }

    fn settle_split(&mut self, operation: TaskOperationId, settlement: SplitDeliverySettlement) {
        let delivery = self.split.active[&operation].delivery;
        let retained = matches!(settlement, SplitDeliverySettlement::Unknown(_));
        self.split
            .notices
            .push_back(EffectBody::PumpOperationSettled {
                pump: self.split.source.expect("registered split pump"),
                delivery,
                operation,
                settlement,
            });
        if !retained {
            let row = self
                .split
                .active
                .remove(&operation)
                .expect("settled original split");
            self.split.tasks.remove(&row.task);
        }
    }

    pub(crate) fn has_split_notices(&self) -> bool {
        !self.split.notices.is_empty()
    }

    pub(crate) fn take_split_notice(&mut self) -> Option<EffectBody> {
        self.split.notices.pop_front()
    }
}
