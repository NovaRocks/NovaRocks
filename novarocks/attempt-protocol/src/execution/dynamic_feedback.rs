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

//! Select feedback reads and settle the same version through the original FE consumer.

use super::QueryTaskExecution;
use crate::{
    effect::{AttemptEffect, EffectBody, EffectClass, PumpKind},
    error::TaskExecutionError,
    failure::AttemptFailure,
    input::DynamicFilterRead,
    start::PayloadRef,
};
use novarocks_coordination_contract::{AttemptActionId, AttemptRequestToken};
use novarocks_execution_contract::{DomainVersion, TaskIdentity};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum Lifecycle {
    #[default]
    Dormant,
    Open,
    Closed,
}

#[derive(Debug, Default)]
struct Cursor {
    advertised: Option<DomainVersion>,
    settled: Option<DomainVersion>,
    deferred_at: Option<u64>,
}

#[derive(Clone, Copy, Debug)]
struct Read {
    request: AttemptRequestToken,
    task: TaskIdentity,
    advertised: DomainVersion,
}

#[derive(Clone, Copy, Debug)]
enum Flight {
    Reading(Read),
    Ready {
        read: Read,
        version: DomainVersion,
        feedback: PayloadRef,
    },
    Admitting {
        read: Read,
        version: DomainVersion,
    },
}

impl Flight {
    fn read(self) -> Read {
        match self {
            Self::Reading(read) | Self::Ready { read, .. } | Self::Admitting { read, .. } => read,
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Action {
    Start(PayloadRef),
    Stop(PayloadRef),
    Read {
        task: TaskIdentity,
        advertised: DomainVersion,
        acknowledged: Option<DomainVersion>,
    },
    Admit {
        read: Read,
        version: DomainVersion,
        feedback: PayloadRef,
    },
}

#[derive(Debug, Default)]
pub(super) struct DynamicFeedback {
    source: Option<PayloadRef>,
    lifecycle: Lifecycle,
    flight: Option<Flight>,
    cursors: BTreeMap<TaskIdentity, Cursor>,
    ready: BTreeSet<TaskIdentity>,
    deferred: BTreeSet<(u64, TaskIdentity)>,
    progress: u64,
}

impl DynamicFeedback {
    fn next_action(&self, running: bool) -> Option<Action> {
        let source = self.source?;
        match (self.lifecycle, running) {
            (Lifecycle::Dormant, true) => return Some(Action::Start(source)),
            (Lifecycle::Open, false) => {
                return (!matches!(self.flight, Some(Flight::Admitting { .. })))
                    .then_some(Action::Stop(source));
            }
            (Lifecycle::Open, true) => {}
            _ => return None,
        }
        match self.flight {
            Some(Flight::Ready {
                read,
                version,
                feedback,
            }) => {
                return Some(Action::Admit {
                    read,
                    version,
                    feedback,
                });
            }
            Some(_) => return None,
            None => {}
        }
        let retry = self
            .deferred
            .first()
            .filter(|(epoch, _)| *epoch < self.progress)
            .map(|(_, task)| *task);
        let task = self.ready.first().copied().or(retry)?;
        let cursor = &self.cursors[&task];
        Some(Action::Read {
            task,
            advertised: cursor.advertised.expect("pending original advertisement"),
            acknowledged: cursor.settled,
        })
    }

    fn refresh(&mut self, task: TaskIdentity) {
        let cursor = &self.cursors[&task];
        if self.flight.is_some_and(|flight| flight.read().task == task)
            || !cursor
                .advertised
                .is_some_and(|version| Some(version) > cursor.settled)
        {
            return;
        }
        if let Some(epoch) = cursor.deferred_at {
            self.deferred.insert((epoch, task));
        } else {
            self.ready.insert(task);
        }
    }

    fn remove_candidate(&mut self, task: TaskIdentity) {
        self.ready.remove(&task);
        if let Some(epoch) = self
            .cursors
            .get_mut(&task)
            .and_then(|cursor| cursor.deferred_at.take())
        {
            self.deferred.remove(&(epoch, task));
        }
    }

    fn settle(&mut self, task: TaskIdentity, version: DomainVersion) {
        let cursor = self
            .cursors
            .get_mut(&task)
            .expect("original feedback cursor");
        cursor.settled = Some(version);
        self.refresh(task);
    }
}

impl QueryTaskExecution {
    pub(crate) fn bind_dynamic_feedback(&mut self, source: Option<PayloadRef>) {
        self.dynamic_feedback.source = source;
    }

    pub(super) fn note_dynamic_advertisement(
        &mut self,
        task: TaskIdentity,
        advertised: Option<DomainVersion>,
    ) {
        let feedback = &mut self.dynamic_feedback;
        if feedback.source.is_none() || feedback.lifecycle == Lifecycle::Closed {
            return;
        }
        if feedback
            .cursors
            .get(&task)
            .and_then(|cursor| cursor.advertised)
            == advertised
        {
            return;
        }
        feedback.remove_candidate(task);
        feedback.cursors.entry(task).or_default().advertised = advertised;
        feedback.refresh(task);
    }

    /// A retry needs real progress independent of the read's own result.
    pub(crate) fn signal_dynamic_feedback_progress(&mut self) {
        let feedback = &mut self.dynamic_feedback;
        if feedback.source.is_some() && feedback.lifecycle != Lifecycle::Closed {
            feedback.progress = feedback.progress.saturating_add(1);
        }
    }

    pub(crate) fn has_dynamic_feedback_work(&self, running: bool) -> bool {
        self.dynamic_feedback.next_action(running).is_some()
    }

    pub(crate) fn advance_dynamic_feedback(
        &mut self,
        running: bool,
        id: AttemptActionId,
        request: AttemptRequestToken,
    ) -> Option<AttemptEffect> {
        let action = self.dynamic_feedback.next_action(running)?;
        let operation = matches!(action, Action::Read { .. }).then(|| self.issue_operation_id());
        let feedback = &mut self.dynamic_feedback;
        let (body, class) = match action {
            Action::Start(pump) => {
                feedback.lifecycle = Lifecycle::Open;
                (
                    EffectBody::StartPump {
                        pump,
                        kind: PumpKind::DynamicFilters,
                    },
                    EffectClass::Business,
                )
            }
            Action::Stop(pump) => {
                feedback.lifecycle = Lifecycle::Closed;
                if matches!(feedback.flight, Some(Flight::Ready { .. })) {
                    // The original Source still owns the unconsumed read.
                    feedback.flight = None;
                }
                (EffectBody::StopPump { pump }, EffectClass::Cleanup)
            }
            Action::Read {
                task,
                advertised,
                acknowledged,
            } => {
                feedback.remove_candidate(task);
                feedback.flight = Some(Flight::Reading(Read {
                    request,
                    task,
                    advertised,
                }));
                (
                    EffectBody::ReadDynamicFilters {
                        operation: operation.expect("selected original dynamic read"),
                        request,
                        task,
                        acknowledged,
                        advertised,
                    },
                    EffectClass::Business,
                )
            }
            Action::Admit {
                read,
                version,
                feedback: payload,
            } => {
                feedback.flight = Some(Flight::Admitting { read, version });
                (
                    EffectBody::AdmitDynamicFilterFeedback {
                        request: read.request,
                        task: read.task,
                        version,
                        feedback: payload,
                    },
                    EffectClass::Business,
                )
            }
        };
        Some(AttemptEffect { id, body, class })
    }

    pub(crate) fn observe_dynamic_read(
        &mut self,
        request: AttemptRequestToken,
        task: TaskIdentity,
        result: DynamicFilterRead,
    ) -> Option<AttemptFailure> {
        let feedback = &mut self.dynamic_feedback;
        let Some(Flight::Reading(read)) = feedback.flight else {
            return None;
        };
        if read.request != request || read.task != task {
            return None;
        }
        feedback.flight = None;
        if feedback.lifecycle == Lifecycle::Closed {
            return None;
        }
        match result {
            DynamicFilterRead::Empty => feedback.settle(task, read.advertised),
            DynamicFilterRead::Unavailable(_) => {
                let cursor = feedback
                    .cursors
                    .get_mut(&task)
                    .expect("original feedback cursor");
                if cursor
                    .advertised
                    .is_some_and(|latest| latest <= read.advertised)
                {
                    cursor.deferred_at = Some(feedback.progress);
                }
                feedback.refresh(task);
            }
            DynamicFilterRead::Available {
                version,
                feedback: payload,
            } => {
                if version < read.advertised {
                    return Some(AttemptFailure::TaskProtocol(Box::new(
                        TaskExecutionError::Schedule(format!(
                            "dynamic filter read for task {task} answered version {} below the advertised version {}",
                            version.get(),
                            read.advertised.get()
                        )),
                    )));
                }
                feedback.flight = Some(Flight::Ready {
                    read,
                    version,
                    feedback: payload,
                });
            }
            DynamicFilterRead::Refused(failure) => return Some(failure),
        }
        None
    }

    pub(crate) fn observe_dynamic_admission(
        &mut self,
        request: AttemptRequestToken,
        task: TaskIdentity,
        result: Result<(), AttemptFailure>,
    ) -> Option<AttemptFailure> {
        let feedback = &mut self.dynamic_feedback;
        let Some(Flight::Admitting { read, version }) = feedback.flight else {
            return None;
        };
        if read.request != request || read.task != task {
            return None;
        }
        feedback.flight = None;
        match result {
            Ok(()) => {
                feedback.settle(task, version);
                None
            }
            Err(failure) => Some(failure),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        execution::StatusReport, failure::HostFailureRef, test_support::execution_with_strict_input,
    };
    use novarocks_execution_contract::{
        DynamicFilterAdvertisement, TaskOutputFacts, TaskState, TaskStatus, TaskStatusVersion,
    };
    use std::num::NonZeroU64;

    fn key(n: u64) -> NonZeroU64 {
        NonZeroU64::new(n).unwrap()
    }
    fn token(n: u64) -> AttemptRequestToken {
        AttemptRequestToken::new(key(n))
    }
    fn advance(execution: &mut QueryTaskExecution, running: bool, n: u64) -> Option<EffectBody> {
        execution
            .advance_dynamic_feedback(running, AttemptActionId::new(key(n)), token(n))
            .map(|effect| effect.body)
    }
    fn version(n: u64) -> DomainVersion {
        DomainVersion::new(n).unwrap()
    }

    fn advertise(
        execution: &mut QueryTaskExecution,
        task: TaskIdentity,
        status_version: u64,
        domain: u64,
    ) {
        let status = TaskStatus::try_new(
            task,
            TaskStatusVersion::new(status_version).unwrap(),
            TaskState::Running,
            None,
            TaskOutputFacts::default(),
        )
        .unwrap()
        .with_dynamic_filters(DynamicFilterAdvertisement::new(version(domain), 1));
        execution
            .apply_published(&status, &mut StatusReport::default())
            .unwrap();
    }

    #[test]
    fn unavailable_waits_for_independent_progress_and_only_admitted_feedback_advances_the_cursor() {
        let mut execution = execution_with_strict_input();
        let source = PayloadRef(key(1));
        execution.bind_dynamic_feedback(Some(source));
        let (first, second) = {
            let mut tasks = execution.graph().tasks().map(|task| task.identity());
            (tasks.next().unwrap(), tasks.next().unwrap())
        };
        advertise(&mut execution, first, 2, 1);
        advertise(&mut execution, second, 2, 1);
        assert!(matches!(advance(&mut execution, true, 1),
            Some(EffectBody::StartPump { pump, kind: PumpKind::DynamicFilters }) if pump == source));
        let Some(EffectBody::ReadDynamicFilters {
            operation: first_operation,
            request,
            task,
            advertised,
            acknowledged,
        }) = advance(&mut execution, true, 2)
        else {
            panic!("original advertised read")
        };
        assert_eq!(
            (request, task, advertised, acknowledged),
            (token(2), first, version(1), None)
        );
        assert!(
            execution
                .observe_dynamic_read(token(100), first, DynamicFilterRead::Empty)
                .is_none()
        );
        assert!(advance(&mut execution, true, 3).is_none());

        assert!(
            execution
                .observe_dynamic_read(
                    token(2),
                    first,
                    DynamicFilterRead::Unavailable(HostFailureRef(key(4)))
                )
                .is_none()
        );
        assert!(matches!(advance(&mut execution, true, 4),
            Some(EffectBody::ReadDynamicFilters { task, request, .. }) if task == second && request == token(4)));
        assert!(
            execution
                .observe_dynamic_read(token(4), second, DynamicFilterRead::Empty)
                .is_none()
        );
        assert!(advance(&mut execution, true, 5).is_none());
        execution.signal_dynamic_feedback_progress();
        assert!(matches!(advance(&mut execution, true, 6),
            Some(EffectBody::ReadDynamicFilters { operation, task, request, advertised, acknowledged: None })
                if operation != first_operation && task == first && request == token(6) && advertised == version(1)));

        let payload = PayloadRef(key(42));
        assert!(
            execution
                .observe_dynamic_read(
                    token(6),
                    first,
                    DynamicFilterRead::Available {
                        version: version(2),
                        feedback: payload
                    }
                )
                .is_none()
        );
        advertise(&mut execution, first, 3, 3);
        assert!(matches!(advance(&mut execution, true, 7),
            Some(EffectBody::AdmitDynamicFilterFeedback { task, request, version: actual, feedback })
                if task == first && request == token(6) && actual == version(2) && feedback == payload));
        assert!(
            execution
                .observe_dynamic_admission(token(6), second, Ok(()))
                .is_none()
        );
        assert!(advance(&mut execution, true, 8).is_none());
        assert!(
            execution
                .observe_dynamic_admission(token(6), first, Ok(()))
                .is_none()
        );
        assert!(matches!(advance(&mut execution, true, 9),
            Some(EffectBody::ReadDynamicFilters { task, advertised, acknowledged: Some(acked), .. })
                if task == first && advertised == version(3) && acked == version(2)));
        assert!(matches!(execution.observe_dynamic_read(token(9), first,
            DynamicFilterRead::Available { version: version(2), feedback: payload }),
            Some(AttemptFailure::TaskProtocol(error)) if matches!(*error, TaskExecutionError::Schedule(ref detail) if detail.contains("below the advertised version"))));
    }

    #[test]
    fn close_stops_new_feedback_but_waits_for_an_emitted_admission_result() {
        for emitted in [false, true] {
            let mut execution = execution_with_strict_input();
            let task = execution.graph().root_identity();
            let source = PayloadRef(key(1));
            execution.bind_dynamic_feedback(Some(source));
            advertise(&mut execution, task, 2, 1);
            advance(&mut execution, true, 1).unwrap();
            advance(&mut execution, true, 2).unwrap();
            let available = DynamicFilterRead::Available {
                version: version(1),
                feedback: PayloadRef(key(3)),
            };
            if emitted {
                execution.observe_dynamic_read(token(2), task, available.clone());
                assert!(matches!(
                    advance(&mut execution, true, 3),
                    Some(EffectBody::AdmitDynamicFilterFeedback { .. })
                ));
                assert!(advance(&mut execution, false, 4).is_none());
                execution.observe_dynamic_admission(token(2), task, Ok(()));
            }
            assert!(matches!(advance(&mut execution, false, 5),
                Some(EffectBody::StopPump { pump }) if pump == source));
            assert!(
                execution
                    .observe_dynamic_read(token(2), task, available)
                    .is_none()
            );
            assert!(advance(&mut execution, false, 6).is_none());
            assert!(advance(&mut execution, true, 7).is_none());
        }
    }
}
