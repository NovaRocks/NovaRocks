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

//! Read, then admit only on the core's explicit effect. Neither path advances
//! a version cursor or makes the core's advertisement/version decision.

use super::{OriginalJobRecord, PumpRecordError, project_task_error};
use crate::{
    native::task_result_transport::{
        DynamicFilterRead as HostRead, DynamicFilterReadError, NativeTaskResultTransport,
    },
    runtime_filter::feedback::{RuntimeFilterFeedbackAdmission, RuntimeFilterFeedbackState},
    task_execution::error::TaskExecutionError,
};
use novarocks_attempt_protocol::{
    failure::HostFailureRef,
    input::{DynamicFilterRead, ExecutorFact},
    start::PayloadRef,
};
use novarocks_control_runtime::exit_custody::ExitCustody;
use novarocks_coordination_contract::AttemptRequestToken;
use novarocks_execution_contract::{DomainVersion, TaskIdentity, TaskOperationId};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

pub(crate) struct DynamicReadRegistration {
    pub(crate) request: AttemptRequestToken,
    pub(crate) task: TaskIdentity,
    pub(crate) feedback: PayloadRef,
    pub(crate) failure: HostFailureRef,
    pub(crate) custody: ExitCustody,
}
struct ReadRecord {
    task: TaskIdentity,
    feedback: PayloadRef,
    failure: HostFailureRef,
    read_job: OriginalJobRecord,
    admission_job: Option<OriginalJobRecord>,
    started: bool,
    admission_started: bool,
    read: Option<HostRead>,
    read_error: Option<DynamicFilterReadError>,
    terminal_error: Option<TaskExecutionError>,
}
struct Records {
    feedback_closed: bool,
    reads: BTreeMap<AttemptRequestToken, ReadRecord>,
}
type RetiredDynamicFilterRead = (
    Option<(HostFailureRef, TaskExecutionError)>,
    Box<dyn std::any::Any + Send>,
);

pub(crate) struct DynamicFilterRecords {
    transport: Arc<NativeTaskResultTransport>,
    feedback: Arc<RuntimeFilterFeedbackState>,
    records: Mutex<Records>,
}
impl DynamicFilterRecords {
    pub(crate) fn new(
        transport: Arc<NativeTaskResultTransport>,
        feedback: Arc<RuntimeFilterFeedbackState>,
    ) -> Arc<Self> {
        Arc::new(Self {
            transport,
            feedback,
            records: Mutex::new(Records {
                feedback_closed: false,
                reads: BTreeMap::new(),
            }),
        })
    }
    /// The original Source supplies every reference and the exact job custody.
    /// Register before handing that job to an executor; this allocates no Work.
    pub(crate) fn register(
        &self,
        registration: DynamicReadRegistration,
    ) -> Result<(), (PumpRecordError, DynamicReadRegistration)> {
        let mut records = self.records.lock().expect("dynamic read canonical records");
        // Core can stop the feedback consumer while the original read
        // effect is still in the host queue. That read still owes its result.
        if records.reads.contains_key(&registration.request)
            || records
                .reads
                .values()
                .any(|r| r.feedback == registration.feedback)
        {
            return Err((PumpRecordError::DuplicateRecord, registration));
        }
        records.reads.insert(
            registration.request,
            ReadRecord {
                task: registration.task,
                feedback: registration.feedback,
                failure: registration.failure,
                read_job: OriginalJobRecord::new(registration.custody),
                admission_job: None,
                started: false,
                admission_started: false,
                read: None,
                read_error: None,
                terminal_error: None,
            },
        );
        Ok(())
    }
    /// Called inside the common executor's already credentialed original job.
    /// The owner-side async method uses the same Client and original 500 ms cap.
    pub(crate) async fn run_admitted_read(
        &self,
        request: AttemptRequestToken,
        task: TaskIdentity,
        operation: TaskOperationId,
        acknowledged: Option<DomainVersion>,
    ) -> Result<ExecutorFact, PumpRecordError> {
        let feedback_open = {
            let mut records = self.records.lock().expect("dynamic read canonical records");
            let feedback_open = !records.feedback_closed;
            let record = records
                .reads
                .get_mut(&request)
                .ok_or(PumpRecordError::MissingRecord)?;
            if record.task != task {
                return Err(PumpRecordError::WrongIdentity);
            }
            if record.started {
                return Err(PumpRecordError::AlreadyStarted);
            }
            record.started = true;
            feedback_open
        };
        let result = if feedback_open {
            self.transport
                .dynamic_filters_async(operation, task, acknowledged)
                .await
        } else {
            Err(DynamicFilterReadError::Unavailable(
                "dynamic filter feedback stopped before the original read started".into(),
            ))
        };
        let mut records = self.records.lock().expect("dynamic read canonical records");
        let record = records
            .reads
            .get_mut(&request)
            .ok_or(PumpRecordError::MissingRecord)?;
        let result = match result {
            Ok(read) => {
                let result = match read.version() {
                    Some(version) => DynamicFilterRead::Available {
                        version,
                        feedback: record.feedback,
                    },
                    None => DynamicFilterRead::Empty,
                };
                record.read = Some(read);
                result
            }
            Err(error) => {
                let fact = match &error {
                    DynamicFilterReadError::Unavailable(_) => {
                        DynamicFilterRead::Unavailable(record.failure)
                    }
                    DynamicFilterReadError::Refused(detail) => {
                        let original = TaskExecutionError::Schedule(format!(
                            "dynamic filter read for task {task} was refused: {detail}"
                        ));
                        let projected = project_task_error(record.failure, &original);
                        record.terminal_error = Some(original);
                        DynamicFilterRead::Refused(projected)
                    }
                };
                record.read_error = Some(error);
                fact
            }
        };
        Ok(ExecutorFact::DynamicFiltersRead {
            request,
            task,
            result,
        })
    }
    pub(crate) fn register_admission(
        &self,
        request: AttemptRequestToken,
        task: TaskIdentity,
        feedback: PayloadRef,
        custody: ExitCustody,
    ) -> Result<(), (PumpRecordError, ExitCustody)> {
        let mut records = self.records.lock().expect("dynamic read canonical records");
        let Some(record) = records.reads.get_mut(&request) else {
            return Err((PumpRecordError::MissingRecord, custody));
        };
        if record.task != task || record.feedback != feedback {
            return Err((PumpRecordError::WrongIdentity, custody));
        }
        if record.admission_job.is_some() {
            return Err((PumpRecordError::AlreadyStarted, custody));
        }
        record.admission_job = Some(OriginalJobRecord::new(custody));
        Ok(())
    }
    /// The core has checked its original advertisement before requesting this.
    /// Calling the existing FA domain owner preserves partial effects and order.
    pub(crate) fn run_admitted_feedback(
        &self,
        request: AttemptRequestToken,
        task: TaskIdentity,
        version: DomainVersion,
        feedback: PayloadRef,
    ) -> Result<ExecutorFact, PumpRecordError> {
        let (read, failure) = {
            let mut records = self.records.lock().expect("dynamic read canonical records");
            let record = records
                .reads
                .get_mut(&request)
                .ok_or(PumpRecordError::MissingRecord)?;
            if record.task != task || record.feedback != feedback || record.admission_job.is_none()
            {
                return Err(PumpRecordError::WrongIdentity);
            }
            if record.admission_started {
                return Err(PumpRecordError::AlreadyStarted);
            }
            let read = record.read.take().ok_or(PumpRecordError::MissingFeedback)?;
            // Identity of the canonical resource is not an advertisement test.
            if read.version() != Some(version) {
                record.read = Some(read);
                return Err(PumpRecordError::WrongIdentity);
            }
            record.admission_started = true;
            (read, record.failure)
        };
        let mut error = None;
        for value in read.feedback() {
            let admitted = self
                .feedback
                .admit_task_feedback(value, task.backend_process_id());
            let detail = match admitted {
                Ok(RuntimeFilterFeedbackAdmission::Applied) => continue,
                Ok(disposition) => format!(
                    "dynamic filter feedback from task {task} channel {} was fenced as {disposition:?}",
                    value.channel_id()
                ),
                Err(detail) => format!(
                    "dynamic filter feedback from task {task} channel {} is not admissible: {detail}",
                    value.channel_id()
                ),
            };
            error = Some(TaskExecutionError::Schedule(detail));
            break;
        }
        let result = error
            .as_ref()
            .map_or(Ok(()), |error| Err(project_task_error(failure, error)));
        let mut records = self.records.lock().expect("dynamic read canonical records");
        let record = records
            .reads
            .get_mut(&request)
            .ok_or(PumpRecordError::MissingRecord)?;
        record.read = Some(read);
        record.terminal_error = error;
        Ok(ExecutorFact::DynamicFilterFeedbackAdmitted {
            request,
            task,
            result,
        })
    }
    pub(super) fn resolve_result(
        &self,
        request: AttemptRequestToken,
        admission: bool,
    ) -> Result<(), PumpRecordError> {
        let mut records = self.records.lock().expect("dynamic read canonical records");
        let record = records
            .reads
            .get_mut(&request)
            .ok_or(PumpRecordError::MissingRecord)?;
        let job = if admission {
            record
                .admission_job
                .as_mut()
                .ok_or(PumpRecordError::MissingRecord)?
        } else {
            &mut record.read_job
        };
        job.resolve_result();
        Ok(())
    }
    /// Borrow the original typed cause for the bridge's existing error path.
    pub(super) fn project_failure(
        &self,
        failure: HostFailureRef,
    ) -> Option<novarocks_query_application::execution_error::ExecutionFailureInput> {
        let records = self.records.lock().expect("dynamic read canonical records");
        let record = records
            .reads
            .values()
            .find(|record| record.failure == failure)?;
        record
            .terminal_error
            .as_ref()
            .map(TaskExecutionError::preparation_input)
    }

    pub(super) fn take_failure(&self, failure: HostFailureRef) -> Option<TaskExecutionError> {
        let mut records = self.records.lock().expect("dynamic read canonical records");
        records
            .reads
            .values_mut()
            .find(|record| record.failure == failure)?
            .terminal_error
            .take()
    }
    pub(crate) fn close_external(&self) {
        self.records
            .lock()
            .expect("dynamic read canonical records")
            .feedback_closed = true;
        self.feedback.close();
    }
    /// The Source moves returned records to its existing disposal executor.
    /// No rich payload or original error is dropped while holding this lock.
    pub(super) fn take_retired(&self, closing: bool) -> Option<RetiredDynamicFilterRead> {
        let mut records = self.records.lock().expect("dynamic read canonical records");
        let closing = records.feedback_closed || closing;
        let id = records.reads.iter().find_map(|(id, r)| {
            let available = r.read.as_ref().is_some_and(|read| read.version().is_some());
            (r.read_job.retired()
                && r.admission_job
                    .as_ref()
                    .is_none_or(OriginalJobRecord::retired)
                && (closing || r.terminal_error.is_none())
                && (!available || r.admission_job.is_some() || closing))
                .then_some(*id)
        })?;
        let mut original = records.reads.remove(&id).expect("same retired read");
        let failure = original
            .terminal_error
            .take()
            .map(|error| (original.failure, error));
        Some((failure, Box::new(original)))
    }

    pub(super) fn read_count(&self) -> usize {
        self.records
            .lock()
            .expect("dynamic read canonical records")
            .reads
            .len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        native::{
            data_runtime::FrontendDataRuntime, task_result_transport::TaskReadGrace,
            task_transport::tests::Loopback,
        },
        runtime_filter::install_encoder::FrontendRuntimeFilterFeedbackDeclaration,
    };
    use novarocks_attempt_protocol::failure::AttemptFailure;
    use novarocks_control_runtime::{
        completion::ReportSlot,
        exit_work::{PanicSink, run_future_in_generation},
        reported_work::ReportedWork,
        transaction::RuntimeLedger,
    };
    use novarocks_control_runtime_core::{
        capacity::{ReservationResources, ResourceVector},
        completion::ConsumerKind,
        identity::{ConsumerId, Generation},
        owned_storage::OwnedSlotNeed,
        scope::{WorkAdmission, WorkKind},
    };
    use novarocks_types::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };
    use std::{num::NonZeroU64, time::Duration};

    #[tokio::test]
    async fn original_dynamic_read_still_settles_after_feedback_stop() {
        assert_read_during_stop(false).await;
        assert_read_during_stop(true).await;
    }

    async fn assert_read_during_stop(feedback_closed: bool) {
        let mut peer = Loopback::start().await;
        let process = BackendProcessId::try_from_bytes([
            1, 1, 1, 1, 1, 1, 0x71, 1, 0x81, 1, 1, 1, 1, 1, 1, 1,
        ])
        .unwrap();
        let execution =
            QueryExecutionId::new(QueryId::new(212, 9), AttemptId::new(1).unwrap()).unwrap();
        let task = TaskIdentity::new(
            execution,
            StageId::new(1).unwrap(),
            TaskId::new(1).unwrap(),
            process,
        );
        let mut runtime = FrontendDataRuntime::new(tokio::runtime::Handle::current());
        let transport = NativeTaskResultTransport::new(
            Arc::from([peer.descriptor(process)]),
            runtime.clone(),
            TaskReadGrace::new(Duration::from_secs(1)),
        )
        .unwrap();
        let feedback = RuntimeFilterFeedbackState::new(
            execution,
            FrontendRuntimeFilterFeedbackDeclaration::default(),
        )
        .unwrap();
        let records = DynamicFilterRecords::new(Arc::new(transport), Arc::new(feedback));

        let ledger = RuntimeLedger::new(ResourceVector::default());
        let generation = Generation::new(1).unwrap();
        let scope = ledger.open_scope_next(None, generation).unwrap();
        let (work, mut payer) = scope
            .reserve_next(
                generation,
                ReservationResources::new(
                    ResourceVector::default(),
                    OwnedSlotNeed {
                        work_headers: 1,
                        completions: 1,
                        captures: 1,
                        ..Default::default()
                    },
                ),
                WorkKind::Settlement,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Settlement)
            .unwrap();
        let observer = ConsumerId::new(1).unwrap();
        let report = ReportSlot::reserve_next(
            &mut payer,
            generation,
            1,
            [(observer, ConsumerKind::Observer)],
        )
        .unwrap();
        let (custody, exit) =
            ExitCustody::from_admitted(ReportedWork::try_new(work, payer, report.clone()).unwrap());

        let request = AttemptRequestToken::new(NonZeroU64::MIN);
        let failure = HostFailureRef(NonZeroU64::MIN);
        // Core already issued the effect when orderly stop fences new work.
        // When the exact StopPump has also closed feedback, settle locally.
        scope.request_stop().unwrap();
        if feedback_closed {
            records.close_external();
        }
        records
            .register(DynamicReadRegistration {
                request,
                task,
                feedback: PayloadRef(NonZeroU64::MIN),
                failure,
                custody: custody.clone(),
            })
            .unwrap_or_else(|(error, _)| panic!("{error}"));
        let fact = run_future_in_generation(
            exit,
            records.run_admitted_read(request, task, TaskOperationId::new_v7(), None),
            PanicSink::new(),
            generation,
        )
        .await
        .unwrap()
        .unwrap();
        let ExecutorFact::DynamicFiltersRead {
            request: actual_request,
            task: actual_task,
            result,
        } = fact
        else {
            panic!("original dynamic read result");
        };
        assert_eq!((actual_request, actual_task), (request, task));
        if feedback_closed {
            assert!(
                matches!(result, DynamicFilterRead::Unavailable(original) if original == failure)
            );
            assert!(records.take_failure(failure).is_none());
            assert!(matches!(
                records.records.lock().unwrap().reads[&request].read_error.as_ref(),
                Some(DynamicFilterReadError::Unavailable(detail))
                    if detail == "dynamic filter feedback stopped before the original read started"
            ));
        } else {
            assert!(
                matches!(result, DynamicFilterRead::Refused(AttemptFailure::Host(projected)) if projected.original == failure)
            );
            assert!(matches!(
                records.take_failure(failure),
                Some(TaskExecutionError::Schedule(detail)) if detail.contains("task wire test peer rejects FetchTaskDynamicFilters")
            ));
        }
        report.consume(observer).unwrap();
        assert!(custody.try_retire().unwrap());
        records.resolve_result(request, false).unwrap();
        assert!(records.take_retired(true).is_some());
        ledger.reap_scope_ready(scope.id(), 8).unwrap();
        scope.close_external().unwrap();
        assert!(scope.join_ready().unwrap());
        drop(records);
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        runtime.drain_original_apply_until(deadline).await.unwrap();
        runtime
            .drain_original_subscriptions_until(deadline)
            .await
            .unwrap();
        assert!(runtime.original_apply_joined() && runtime.original_subscriptions_joined());
        assert!(peer.settle().await);
    }
}
