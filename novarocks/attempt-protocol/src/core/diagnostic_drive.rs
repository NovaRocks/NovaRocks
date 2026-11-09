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

//! Drive the original diagnostic cut and keep result, apply, and exit distinct.

use super::{AttemptStep, Core};
use crate::{
    capacity::StepContext,
    effect::{AttemptEffect, EffectBody, EffectClass, Publication},
    failure::AttemptFailure,
    input::{DiagnosticRead, PublicationApplied},
};
use novarocks_coordination_contract::{AttemptActionId, AttemptRequestToken, MonotonicInstant};
use novarocks_execution_contract::TaskIdentity;

#[derive(Debug, Default)]
pub(super) struct DiagnosticDrive {
    read: Option<(AttemptActionId, AttemptRequestToken)>,
    publication: Option<AttemptActionId>,
    snapshot_settled: bool,
}

pub(super) enum DiagnosticStep {
    Idle,
    Yield,
    AwaitPublication,
    Effect(AttemptEffect),
}

impl Core {
    pub(super) fn diagnostic_before_input(&mut self, now: MonotonicInstant, cancelled: bool) {
        self.execution.with_diagnostics(|collector, sources| {
            collector.before_input(now, cancelled, sources);
        });
    }

    pub(super) fn diagnostic_after_input(&mut self, now: MonotonicInstant) {
        self.execution.with_diagnostics(|collector, sources| {
            collector.after_input(now, sources);
        });
        if self.execution.diagnostics().reads_closed() {
            self.execution.release_profile_diagnostics();
        }
    }

    pub(super) fn diagnostics_wait_for_publication(&self) -> bool {
        self.diagnostics.publication.is_some()
    }

    pub(super) fn diagnostic_work_due(&self) -> bool {
        !self.diagnostics_wait_for_publication()
            && (self.execution.diagnostics().has_snapshot_pending()
                || self.execution.diagnostics().read_ready())
    }

    pub(super) fn diagnostic_local_ready(&self) -> bool {
        let collector = self.execution.diagnostics();
        collector.actual_reads_exited()
            && (!collector.requires_snapshot() || self.diagnostics.snapshot_settled)
            && !self.diagnostics_wait_for_publication()
    }

    pub(super) fn advance_diagnostics(
        &mut self,
        action: AttemptActionId,
        request: AttemptRequestToken,
        context: &mut dyn StepContext,
    ) -> DiagnosticStep {
        if self.diagnostics_wait_for_publication() {
            return DiagnosticStep::AwaitPublication;
        }
        if self.execution.diagnostics().has_snapshot_pending() {
            let cut = self.execution.with_diagnostics(|collector, sources| {
                while collector.has_assembly_work() {
                    if !collector.advance(sources, context) {
                        return None;
                    }
                }
                if !context.charge(1) {
                    return None;
                }
                collector.take_publication()
            });
            let Some(cut) = cut else {
                return DiagnosticStep::Yield;
            };
            self.diagnostics.publication = Some(action);
            return DiagnosticStep::Effect(AttemptEffect {
                id: action,
                class: EffectClass::Publication,
                body: EffectBody::Publish(Publication::Diagnostics(cut)),
            });
        }
        if !self.execution.diagnostics().read_ready() {
            return DiagnosticStep::Idle;
        }
        let read = self
            .execution
            .with_diagnostics(|collector, _| collector.next_read(request, context));
        let Some(read) = read else {
            return DiagnosticStep::Yield;
        };
        self.diagnostics.read = Some((action, read.request));
        DiagnosticStep::Effect(AttemptEffect {
            id: action,
            class: EffectClass::Settlement,
            body: EffectBody::CollectDiagnostics {
                request: read.request,
                task: read.task,
                deadline: read.deadline,
            },
        })
    }

    pub(super) fn observe_diagnostic_result(
        &mut self,
        action: AttemptActionId,
        request: AttemptRequestToken,
        task: TaskIdentity,
        read: DiagnosticRead,
        result: &mut AttemptStep,
    ) {
        // Adopted references stay in the original Source until the complete
        // snapshot takes their rich payload exactly once.
        let application = self
            .execution
            .with_diagnostics(|collector, _| collector.result(request, task, read));
        let disposal = match application {
            Ok(disposal) => disposal,
            Err(error) => {
                let (error, disposal) = error.into_parts();
                result.diagnostic_failure = Some(AttemptFailure::TaskProtocol(Box::new(error)));
                disposal
            }
        };
        if let Some(disposal) = disposal {
            result.effect = Some(AttemptEffect {
                id: action,
                class: EffectClass::Cleanup,
                body: disposal.into_effect(),
            });
        }
        if self.execution.diagnostics().actual_reads_exited() {
            self.diagnostics.read = None;
        }
    }

    pub(super) fn observe_diagnostic_exit(
        &mut self,
        effect: AttemptActionId,
        result: &mut AttemptStep,
    ) -> bool {
        let Some((original, request)) = self.diagnostics.read else {
            return false;
        };
        if original != effect {
            return false;
        }
        if let Err(error) = self
            .execution
            .with_diagnostics(|collector, _| collector.actual_exit(request))
        {
            result.diagnostic_failure = Some(AttemptFailure::TaskProtocol(Box::new(error)));
        }
        if self.execution.diagnostics().actual_reads_exited() {
            self.diagnostics.read = None;
        }
        true
    }

    pub(super) fn observe_diagnostic_publication(
        &mut self,
        effect: AttemptActionId,
        applied: &Result<PublicationApplied, AttemptFailure>,
        result: &mut AttemptStep,
    ) -> bool {
        let Some(original) = self.diagnostics.publication else {
            return false;
        };
        if original != effect {
            return false;
        }
        match applied {
            Ok(actual) if *actual != PublicationApplied::Diagnostics => return true,
            Err(failure) => result.diagnostic_failure = Some(failure.clone()),
            Ok(_) => {}
        }
        self.diagnostics.publication = None;
        self.diagnostics.snapshot_settled = true;
        true
    }
}
