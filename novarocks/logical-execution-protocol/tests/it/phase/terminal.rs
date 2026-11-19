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

use super::NoEffects;
use crate::support::*;
use novarocks_logical_execution_protocol::{CoreFault, cancel::LogicalCancellationReason};
use std::num::NonZeroU64;

fn cancelled() -> Fixture {
    let mut f = Fixture::new(LogicalOutputMode::CompletionOnly);
    f.begin();
    f.send(LogicalInput::Cancelled(
        LogicalCancellationReason::Requested,
    ));
    f
}
#[test]
fn concluded_request_refusal_preserves_terminal_error_without_hiding_core_service_faults() {
    let mut f = cancelled();
    let original = f.core.facts().failure().unwrap().clone();
    let token = f.relay(RelayRequest::OpenDelivery {
        root: root(1),
        carrier: carrier(false),
    });
    assert!(f.effects.iter().any(|e| matches!(
        &e.body, LogicalEffectBody::Relay(RelayEffect::Reply {
            token: held, response: RelayResponse::Rejected(error), ..
        }) if *held == token && error == &original
    )));
    assert_eq!(
        f.core.facts().conclusion(),
        Some(LogicalConclusion::Cancelled)
    );
    assert_eq!(f.core.facts().failure(), Some(&original));
    for invalid in 0..5 {
        let mut f = cancelled();
        let original = f.core.facts().failure().unwrap().clone();
        let (input, expected) = match invalid {
            0 => (LogicalInput::Started, CoreFault::UnexpectedInput),
            1 => (
                LogicalInput::ActionResult {
                    action: LogicalActionId::new(NonZeroU64::new(u64::MAX).unwrap()),
                    result: ActionResult::Consumer(ConsumerFact::Applied),
                },
                CoreFault::UnknownAction,
            ),
            2 => {
                let native = f.take(|b| {
                    matches!(
                        b,
                        LogicalEffectBody::Native(NativeEffect::RetirePreparation)
                    )
                });
                (
                    LogicalInput::ActionResult {
                        action: native.action,
                        result: ActionResult::Consumer(ConsumerFact::Applied),
                    },
                    CoreFault::UnexpectedInput,
                )
            }
            3 => (
                LogicalInput::AttemptPort(Box::new(AttemptToLogical {
                    activation: AttemptActivationIdentity::from_parts(
                        activation(1).actor().get() + 1,
                        activation(1).execution(),
                        activation(1).generation(),
                    ),
                    action: AttemptActionId::new(NonZeroU64::MIN),
                    body: AttemptToLogicalBody::AttemptTerminal(AttemptTerminal::Completed),
                })),
                CoreFault::ForeignActivation,
            ),
            _ => {
                let foreign = novarocks_types::QueryExecutionId::new(
                    novarocks_types::QueryId::new(900, 901),
                    novarocks_types::AttemptId::new(1).unwrap(),
                )
                .unwrap();
                (
                    LogicalInput::AttemptPort(Box::new(AttemptToLogical {
                        activation: AttemptActivationIdentity::from_parts(
                            activation(1).actor().get(),
                            foreign,
                            activation(1).generation(),
                        ),
                        action: AttemptActionId::new(NonZeroU64::MIN),
                        body: AttemptToLogicalBody::AttemptTerminal(AttemptTerminal::Completed),
                    })),
                    CoreFault::ForeignExecution,
                )
            }
        };
        let result = f.core.handle(f.now, input, &mut Budget(32), &mut NoEffects);
        assert_eq!(result, Err(expected), "invalid service input {invalid}");
        assert_eq!(
            f.core.facts().conclusion(),
            Some(LogicalConclusion::Cancelled)
        );
        assert_eq!(f.core.facts().failure(), Some(&original));
    }
}
