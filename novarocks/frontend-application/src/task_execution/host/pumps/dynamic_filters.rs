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
use novarocks_control_runtime_core::identity::InputId;
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
    pub(crate) original_retention: Box<dyn std::any::Any + Send>,
}
struct ReadRecord {
    task: TaskIdentity,
    feedback: PayloadRef,
    failure: HostFailureRef,
    _original_retention: Box<dyn std::any::Any + Send>,
    read_job: OriginalJobRecord,
    admission_job: Option<OriginalJobRecord>,
    started: bool,
    admission_started: bool,
    read: Option<HostRead>,
    read_error: Option<Arc<DynamicFilterReadError>>,
    admission_error: Option<Arc<TaskExecutionError>>,
    refusal_error: Option<Arc<TaskExecutionError>>,
}
pub(crate) enum DynamicFilterFailureSource {
    Read(Arc<DynamicFilterReadError>),
    Admission(Arc<TaskExecutionError>),
}
struct Records {
    closing: bool,
    reads: BTreeMap<AttemptRequestToken, ReadRecord>,
}
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
                closing: false,
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
        if records.closing
            || records.reads.contains_key(&registration.request)
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
                _original_retention: registration.original_retention,
                read_job: OriginalJobRecord::new(registration.custody),
                admission_job: None,
                started: false,
                admission_started: false,
                read: None,
                read_error: None,
                admission_error: None,
                refusal_error: None,
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
        {
            let mut records = self.records.lock().expect("dynamic read canonical records");
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
        }
        let result = self
            .transport
            .dynamic_filters_async(operation, task, acknowledged)
            .await;
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
                        let original = Arc::new(TaskExecutionError::Schedule(format!(
                            "dynamic filter read for task {task} was refused: {detail}"
                        )));
                        let projected = project_task_error(record.failure, &original);
                        record.refusal_error = Some(original);
                        DynamicFilterRead::Refused(projected)
                    }
                };
                record.read_error = Some(Arc::new(error));
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
        record.admission_error = error.map(Arc::new);
        Ok(ExecutorFact::DynamicFilterFeedbackAdmitted {
            request,
            task,
            result,
        })
    }
    pub(crate) fn bind_reply_input(
        &self,
        request: AttemptRequestToken,
        admission: bool,
        input: InputId,
    ) -> Result<(), PumpRecordError> {
        let mut records = self.records.lock().expect("dynamic read canonical records");
        let record = records
            .reads
            .get_mut(&request)
            .ok_or(PumpRecordError::MissingRecord)?;
        if admission {
            record
                .admission_job
                .as_mut()
                .ok_or(PumpRecordError::MissingRecord)?
                .bind_reply(input)
        } else {
            record.read_job.bind_reply(input)
        }
    }
    pub(crate) fn core_consumed(&self, inputs: &[InputId]) {
        let mut records = self.records.lock().expect("dynamic read canonical records");
        for record in records.reads.values_mut() {
            record.read_job.core_consumed(inputs);
            if let Some(admission) = &mut record.admission_job {
                admission.core_consumed(inputs);
            }
        }
    }
    /// Borrow the original typed cause for the bridge's existing error path.
    pub(crate) fn failure_source(
        &self,
        failure: HostFailureRef,
    ) -> Option<DynamicFilterFailureSource> {
        let records = self.records.lock().expect("dynamic read canonical records");
        let record = records
            .reads
            .values()
            .find(|record| record.failure == failure)?;
        if let Some(error) = record
            .admission_error
            .as_ref()
            .or(record.refusal_error.as_ref())
        {
            return Some(DynamicFilterFailureSource::Admission(error.clone()));
        }
        record
            .read_error
            .as_ref()
            .map(|error| DynamicFilterFailureSource::Read(error.clone()))
    }
    pub(crate) fn close_external(&self) {
        self.records
            .lock()
            .expect("dynamic read canonical records")
            .closing = true;
        self.feedback.close();
    }
    /// The Source moves returned records to its existing disposal executor.
    /// No rich payload or original error is dropped while holding this lock.
    pub(crate) fn take_retired(&self) -> Vec<Box<dyn std::any::Any + Send>> {
        let mut records = self.records.lock().expect("dynamic read canonical records");
        let closing = records.closing;
        let ids = records
            .reads
            .iter()
            .filter_map(|(id, r)| {
                let available = r.read.as_ref().is_some_and(|read| read.version().is_some());
                (r.read_job.retired()
                    && r.admission_job
                        .as_ref()
                        .is_none_or(OriginalJobRecord::retired)
                    && (!available || r.admission_job.is_some() || closing))
                    .then_some(*id)
            })
            .collect::<Vec<_>>();
        ids.into_iter()
            .map(|id| {
                Box::new(records.reads.remove(&id).expect("same retired read"))
                    as Box<dyn std::any::Any + Send>
            })
            .collect()
    }
}
