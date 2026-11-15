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

pub use novarocks_coordination_contract::{
    AttemptActionId, AttemptActivationIdentity, AttemptConvergenceClosure, AttemptRequestToken,
    ExecutionEffect, IdSeed, MonotonicInstant, OperationIds, RecoveryMode,
    logical_port::{AttemptTerminal, AttemptToLogical, AttemptToLogicalBody, LogicalToAttemptBody},
};
pub use novarocks_execution_contract::{
    TaskIdentity, TaskOutputFacts, TaskState, TaskStatus, TaskStatusVersion,
    root_result::RootResultEnd,
};
pub use novarocks_logical_execution_protocol::{
    EffectSink, LogicalActionId, LogicalExecutionCore, LogicalRequestToken, LogicalSourceId,
    StepBudget, effect::*, input::*, start::*,
};
use novarocks_types::{AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId};
use std::{
    num::{NonZeroU32, NonZeroU64, NonZeroUsize},
    time::Duration,
};
pub fn activation(n: u64) -> AttemptActivationIdentity {
    AttemptActivationIdentity::from_parts(
        71,
        QueryExecutionId::new(QueryId::new(17, 19), AttemptId::new(n).unwrap()).unwrap(),
        11 + n,
    )
}
pub fn root(n: u64) -> TaskIdentity {
    let mut ids = OperationIds::new(IdSeed::new(23, 100));
    TaskIdentity::new(
        activation(n).execution(),
        StageId::new(1).unwrap(),
        TaskId::new(1).unwrap(),
        BackendProcessId::try_from_bytes(ids.issue().to_bytes()).unwrap(),
    )
}
pub fn end(packets: u64, rows: u64) -> RootResultEnd {
    RootResultEnd {
        sequence: NonZeroU64::new(packets + 1).unwrap(),
        output_rows: rows,
    }
}
pub fn carrier(internal: bool) -> RootCarrier {
    use novarocks_result_contract::{ClientRowProfile, RootOutputKind};
    if internal {
        RootCarrier {
            kind: RootOutputKind::CountOnly,
            client_rows: None,
        }
    } else {
        RootCarrier {
            kind: RootOutputKind::ClientRows,
            client_rows: Some(ClientRowProfile::try_new(16, 64).unwrap()),
        }
    }
}
pub const PLAN: LogicalPlanIdentity = LogicalPlanIdentity([97; 16]);
pub fn start(output: LogicalOutputMode) -> LogicalStart {
    LogicalStart {
        identity: activation(1).execution().query_id(),
        actor_instance: activation(1).actor(),
        policy: LogicalPolicy {
            recovery: RecoveryMode::NoRecovery,
            effect: ExecutionEffect::None,
            output,
            finish: match output {
                LogicalOutputMode::ResultStream => LogicalFinishBoundary::ClientEof,
                LogicalOutputMode::InternalResultStream => LogicalFinishBoundary::ComputationReady,
                LogicalOutputMode::CompletionOnly => LogicalFinishBoundary::TaskCompletion,
            },
            retain_diagnostics: false,
            root_carrier: (output != LogicalOutputMode::CompletionOnly)
                .then(|| carrier(output == LogicalOutputMode::InternalResultStream)),
            internal_plan: (output == LogicalOutputMode::InternalResultStream).then_some(PLAN),
        },
        budgets: LogicalBudgets {
            max_attempts: NonZeroU32::new(3).unwrap(),
            replacement_reservation_valid_for: None,
            remote_cleanup_timeout: Duration::from_millis(100),
            admission_issues_per_participation: NonZeroUsize::new(2).unwrap(),
            establish_per_participation: NonZeroUsize::new(2).unwrap(),
            abort_per_participation: NonZeroUsize::new(2).unwrap(),
            observation: ObservationBudgets {
                diagnostics: Duration::from_millis(20),
                convergence: Duration::from_millis(90),
            },
        },
        // Zero-participation fixtures isolate logical result/exit contracts.
        // They do not claim native topology or physical runtime validation.
        first_attempt: InitialAttempt {
            activation: activation(1),
            participations: Box::new([]),
        },
        statement_deadline: None,
        id_seed: IdSeed::new(7, 100),
        started_at: MonotonicInstant::ORIGIN,
    }
}
pub struct Budget(pub usize);
impl StepBudget for Budget {
    fn charge(&mut self, n: usize) -> bool {
        if self.0 < n {
            false
        } else {
            self.0 -= n;
            true
        }
    }
}
#[derive(Default)]
struct Sink(Vec<LogicalEffect>);
impl EffectSink for Sink {
    fn emit(&mut self, effect: LogicalEffect) {
        self.0.push(effect)
    }
}
pub struct Fixture {
    pub core: LogicalExecutionCore,
    pub effects: Vec<LogicalEffect>,
    pub now: MonotonicInstant,
    pub run: Option<LogicalActionId>,
    token: u64,
    port_action: u64,
    pub budget: usize,
    pub steps: usize,
}
impl Fixture {
    pub fn new(output: LogicalOutputMode) -> Self {
        Self::with_start(start(output))
    }
    pub fn with_start(start: LogicalStart) -> Self {
        Self {
            core: LogicalExecutionCore::new(start),
            effects: Vec::new(),
            now: MonotonicInstant::ORIGIN,
            run: None,
            token: 1,
            port_action: 1,
            budget: 32,
            steps: 0,
        }
    }
    pub fn send(&mut self, input: LogicalInput) {
        let mut out = Sink::default();
        self.steps += 1;
        self.core
            .handle(self.now, input, &mut Budget(self.budget), &mut out)
            .unwrap();
        assert!(out.0.len() <= self.budget);
        self.effects.extend(out.0);
        for _ in 0..8192 {
            self.core.check_invariants().unwrap();
            if self.core.next_deadline() != Some(self.now) {
                return;
            }
            let mut out = Sink::default();
            self.steps += 1;
            self.core
                .handle(
                    self.now,
                    LogicalInput::Timer,
                    &mut Budget(self.budget),
                    &mut out,
                )
                .unwrap();
            assert!(out.0.len() <= self.budget);
            self.effects.extend(out.0);
        }
        panic!("immediate work did not quiesce under finite turn budgets")
    }
    pub fn take(&mut self, predicate: impl Fn(&LogicalEffectBody) -> bool) -> LogicalEffect {
        let i = self
            .effects
            .iter()
            .position(|effect| predicate(&effect.body))
            .unwrap_or_else(|| panic!("expected effect absent"));
        self.effects.remove(i)
    }
    pub fn result(&mut self, action: LogicalActionId, result: ActionResult) {
        self.send(LogicalInput::ActionResult { action, result })
    }
    pub fn exit(&mut self, action: LogicalActionId) {
        self.send(LogicalInput::ActionExited { action })
    }
    pub fn ack(&mut self, action: LogicalActionId, result: ActionResult) {
        self.result(action, result);
        self.exit(action)
    }
    pub fn begin(&mut self) {
        self.send(LogicalInput::Started);
        let register = self.take(|b| {
            matches!(
                b,
                LogicalEffectBody::Governance(GovernanceEffect::RegisterObligation { .. })
            )
        });
        self.ack(
            register.action,
            ActionResult::Governance(GovernanceFact::ObligationRegistered {
                activation: activation(1),
                kind: ObligationKind::RunningWork,
            }),
        );
        if let Some(i) = self.effects.iter().position(|e| {
            matches!(
                e.body,
                LogicalEffectBody::Consumer(ConsumerEffect::ExposeOutput { .. })
            )
        }) {
            let expose = self.effects.remove(i);
            self.ack(
                expose.action,
                ActionResult::Consumer(ConsumerFact::OutputExposed),
            );
        }
        let activate = self.take(|b| {
            matches!(
                b,
                LogicalEffectBody::Native(NativeEffect::ActivateAttempt { .. })
            )
        });
        self.ack(
            activate.action,
            ActionResult::Native(NativeFact::AttemptActivated {
                activation: activation(1),
                root: Some(root(1)),
            }),
        );
        self.run = Some(
            self.take(|b| {
                matches!(
                    b,
                    LogicalEffectBody::Native(NativeEffect::RunAttempt { .. })
                )
            })
            .action,
        );
    }
    pub fn port(&mut self, body: AttemptToLogicalBody) {
        let action = self.port_action;
        self.port_action += 1;
        self.send(LogicalInput::AttemptPort(Box::new(AttemptToLogical {
            activation: activation(1),
            action: AttemptActionId::new(NonZeroU64::new(action).unwrap()),
            body,
        })));
    }
    pub fn relay(&mut self, request: RelayRequest) -> LogicalRequestToken {
        let token = LogicalRequestToken::new(NonZeroU64::new(self.token).unwrap());
        self.token += 1;
        self.send(LogicalInput::Relay {
            source: LogicalSourceId::new(NonZeroU64::new(1).unwrap()),
            token,
            activation: activation(1),
            request,
        });
        token
    }
    pub fn internal(&mut self, request: InternalRequest) -> LogicalRequestToken {
        let token = LogicalRequestToken::new(NonZeroU64::new(self.token).unwrap());
        self.token += 1;
        self.send(LogicalInput::Internal {
            source: LogicalSourceId::new(NonZeroU64::new(2).unwrap()),
            token,
            activation: activation(1),
            request,
        });
        token
    }
    pub fn finished_root(&mut self) {
        let status = TaskStatus::try_new(
            root(1),
            TaskStatusVersion::new(6).unwrap(),
            TaskState::Finished,
            None,
            TaskOutputFacts::new(true),
        )
        .unwrap();
        self.port(AttemptToLogicalBody::RootStatus(Box::new(
            novarocks_coordination_contract::AttemptRootProjection {
                status,
                failure: novarocks_coordination_contract::AttemptRootFailure::None,
            },
        )));
    }
    pub fn open(&mut self, internal: bool) {
        self.relay(RelayRequest::OpenDelivery {
            root: root(1),
            carrier: carrier(internal),
        });
        if !internal {
            let schema = self.take(|b| {
                matches!(
                    b,
                    LogicalEffectBody::Consumer(ConsumerEffect::WriteSchema { .. })
                )
            });
            self.ack(
                schema.action,
                ActionResult::Consumer(ConsumerFact::SchemaWritten),
            );
        }
    }
    pub fn seal(&mut self) {
        let effect=self.take(|b|matches!(b,LogicalEffectBody::AttemptPort(p) if matches!(p.body,LogicalToAttemptBody::SealRequest{..})));
        let LogicalEffectBody::AttemptPort(p) = effect.body else {
            unreachable!()
        };
        let LogicalToAttemptBody::SealRequest { token } = p.body else {
            unreachable!()
        };
        self.port(AttemptToLogicalBody::SealResult {
            token,
            result: Ok(std::sync::Arc::from([])),
        });
    }
    pub fn send_once(&mut self, input: LogicalInput, budget: usize) {
        let mut out = Sink::default();
        self.steps += 1;
        self.core
            .handle(self.now, input, &mut Budget(budget), &mut out)
            .unwrap();
        self.core.check_invariants().unwrap();
        assert!(out.0.len() <= budget);
        self.effects.extend(out.0);
    }
}
