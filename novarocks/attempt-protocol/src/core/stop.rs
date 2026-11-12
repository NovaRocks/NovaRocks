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

//! Preserve the original send arbiters while stopping ordinary production.

use crate::{
    capacity::StepContext,
    effect::{AttemptEffect, EffectBody, EffectClass},
    execution::QueryTaskExecution,
};
use novarocks_coordination_contract::AttemptActionId;
use novarocks_execution_contract::TaskOperationId;

#[derive(Debug, Default)]
pub(super) enum StopWithdrawals {
    #[default]
    Open,
    Requested,
    Admissions(Option<TaskOperationId>),
    Lifecycle(Option<TaskOperationId>),
    Sends(Option<AttemptActionId>),
    Closed,
}

#[expect(
    clippy::large_enum_variant,
    reason = "each charged stop step moves its withdrawal effect directly to the caller without a heap wrapper"
)]
pub(super) enum StopProgress {
    Yield,
    Effect(AttemptEffect),
    Done,
}

impl StopWithdrawals {
    pub(super) fn request(&mut self) -> bool {
        if matches!(self, Self::Open) {
            *self = Self::Requested;
            true
        } else {
            false
        }
    }

    pub(super) fn business_closed(&self) -> bool {
        !matches!(self, Self::Open)
    }

    pub(super) fn pending(&self) -> bool {
        matches!(
            self,
            Self::Requested | Self::Admissions(_) | Self::Lifecycle(_) | Self::Sends(_)
        )
    }

    pub(super) fn advance(
        &mut self,
        execution: &mut QueryTaskExecution,
        action: AttemptActionId,
        context: &mut dyn StepContext,
    ) -> StopProgress {
        loop {
            let body = match *self {
                Self::Open | Self::Closed => return StopProgress::Done,
                Self::Requested => {
                    execution.begin_terminal_cleanup();
                    *self = Self::Admissions(None);
                    continue;
                }
                Self::Admissions(after) => {
                    if !context.charge(1) {
                        return StopProgress::Yield;
                    }
                    match execution.next_stop_create_admission(after) {
                        Some((operation, body)) => {
                            *self = Self::Admissions(Some(operation));
                            body
                        }
                        None => {
                            *self = Self::Lifecycle(None);
                            continue;
                        }
                    }
                }
                Self::Lifecycle(after) => {
                    if !context.charge(1) {
                        return StopProgress::Yield;
                    }
                    match execution.next_stop_lifecycle_admission(after) {
                        Some((operation, body)) => {
                            *self = Self::Lifecycle(Some(operation));
                            body
                        }
                        None => {
                            *self = Self::Sends(None);
                            continue;
                        }
                    }
                }
                Self::Sends(after) => {
                    if !context.charge(1) {
                        return StopProgress::Yield;
                    }
                    match execution.next_stop_send_withdrawal(after) {
                        Some((effect, withdraw)) => {
                            *self = Self::Sends(Some(effect));
                            withdraw.then_some(EffectBody::Withdraw { effect })
                        }
                        None => {
                            *self = Self::Closed;
                            return StopProgress::Done;
                        }
                    }
                }
            };
            if let Some(body) = body {
                return StopProgress::Effect(AttemptEffect {
                    id: action,
                    class: EffectClass::Cleanup,
                    body,
                });
            }
        }
    }
}
