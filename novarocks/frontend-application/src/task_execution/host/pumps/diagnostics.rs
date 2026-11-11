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

//! Exactly one physical final-info read. The core owns eligibility, original
//! terminal validation, window closure and the immutable diagnostic cut.

use super::{OriginalJobRecord, PumpRecordError};
use crate::native::task_result_transport::{FinalTaskInfoRead, TaskResultTransport};
use novarocks_attempt_protocol::{
    failure::HostFailureRef,
    input::{DiagnosticRead, ExecutorFact, FinalTaskInfoProjection, FinalTaskInfoRef},
};
use novarocks_control_runtime::exit_custody::ExitCustody;
#[cfg(test)]
use novarocks_control_runtime_core::identity::InputId;
use novarocks_coordination_contract::AttemptRequestToken;
use novarocks_execution_contract::{FinalTaskInfo, TaskIdentity};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

pub(crate) struct DiagnosticRegistration {
    pub(crate) request: AttemptRequestToken,
    pub(crate) task: TaskIdentity,
    pub(crate) failure: HostFailureRef,
    pub(crate) custody: ExitCustody,
    #[cfg(test)]
    pub(crate) original_retention: Option<Box<dyn std::any::Any + Send>>,
}
struct DiagnosticRecord {
    task: TaskIdentity,
    failure: HostFailureRef,
    #[cfg(test)]
    _original_retention: Option<Box<dyn std::any::Any + Send>>,
    job: OriginalJobRecord,
    started: bool,
    error: Option<String>,
}
/// The same original Source retains this allocation independently of the read
/// slot/job. A consumed row retains only its exact request/task marker. Original
/// Source metadata backing must cover these rows through final disposition;
/// neither the copied reference nor physical job retirement grants capacity.
struct InformationRecord {
    task: TaskIdentity,
    information: Option<Box<FinalTaskInfo>>,
}
struct Records {
    closing: bool,
    active: Option<AttemptRequestToken>,
    reads: BTreeMap<AttemptRequestToken, DiagnosticRecord>,
    information: BTreeMap<AttemptRequestToken, InformationRecord>,
}
pub(crate) struct DiagnosticRecords {
    transport: Arc<dyn TaskResultTransport>,
    records: Mutex<Records>,
}
impl DiagnosticRecords {
    pub(crate) fn new(transport: Arc<dyn TaskResultTransport>) -> Arc<Self> {
        Arc::new(Self {
            transport,
            records: Mutex::new(Records {
                closing: false,
                active: None,
                reads: BTreeMap::new(),
                information: BTreeMap::new(),
            }),
        })
    }
    /// A previous result alone cannot free the physical read position. Only
    /// its original custody proves actual body/capture exit, independently of
    /// the report/header's later retirement.
    pub(crate) fn register(
        &self,
        registration: DiagnosticRegistration,
    ) -> Result<(), (PumpRecordError, DiagnosticRegistration)> {
        let mut records = self.records.lock().expect("diagnostic canonical records");
        if records.closing {
            return Err((PumpRecordError::AlreadyStarted, registration));
        }
        if let Some(active) = records.active {
            let original = &records.reads[&active].job.custody;
            if !original.actual_exit_ready().unwrap_or(false) {
                return Err((PumpRecordError::AlreadyStarted, registration));
            }
        }
        if records.reads.contains_key(&registration.request)
            || records.information.contains_key(&registration.request)
        {
            return Err((PumpRecordError::DuplicateRecord, registration));
        }
        records.active = Some(registration.request);
        records.reads.insert(
            registration.request,
            DiagnosticRecord {
                task: registration.task,
                failure: registration.failure,
                #[cfg(test)]
                _original_retention: registration.original_retention,
                job: OriginalJobRecord::new(registration.custody),
                started: false,
                error: None,
            },
        );
        Ok(())
    }
    /// No diagnostic-deadline timeout is introduced here: expiry closes the
    /// core's observation window while this same original RPC still exits.
    pub(crate) async fn run_admitted_read(
        &self,
        request: AttemptRequestToken,
        task: TaskIdentity,
    ) -> Result<ExecutorFact, PumpRecordError> {
        {
            let mut records = self.records.lock().expect("diagnostic canonical records");
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
        let result = self.transport.final_task_info(task).await;
        match &result {
            Ok(FinalTaskInfoRead::Available(_)) => {}
            Ok(FinalTaskInfoRead::Unavailable(outcome)) => {
                tracing::debug!(%task, ?outcome, "Final task info is unavailable");
            }
            Err(error) => {
                tracing::warn!(%task, %error, "Final task info could not be read");
            }
        }
        let mut records = self.records.lock().expect("diagnostic canonical records");
        let record = records
            .reads
            .get_mut(&request)
            .ok_or(PumpRecordError::MissingRecord)?;
        let result = match result {
            Ok(FinalTaskInfoRead::Available(info)) => {
                // Keep the original allocation even if its actual status names a
                // foreign Task. Core validates that bounded status; the custody
                // reference remains bound to the original requested Task.
                let final_status = info.final_status().clone();
                records.information.insert(
                    request,
                    InformationRecord {
                        task,
                        information: Some(info),
                    },
                );
                DiagnosticRead::Available(FinalTaskInfoProjection {
                    reference: FinalTaskInfoRef::new(request, task),
                    final_status,
                })
            }
            Ok(FinalTaskInfoRead::Unavailable(outcome)) => DiagnosticRead::Unavailable(outcome),
            Err(error) => {
                record.error = Some(error);
                DiagnosticRead::Failed(record.failure)
            }
        };
        Ok(ExecutorFact::DiagnosticsFinished {
            request,
            task,
            result,
        })
    }
    /// Transfer the exact original allocation once. The caller must execute its
    /// rich publication/destruction in original credentialed off-poller work;
    /// the copied reference itself grants no access or disposal authority.
    pub(crate) fn take_information(
        &self,
        reference: FinalTaskInfoRef,
    ) -> Result<Box<FinalTaskInfo>, PumpRecordError> {
        self.discard_information(reference)?
            .ok_or(PumpRecordError::MissingRecord)
    }
    /// Idempotent disposition only for a known exact original request/Task.
    /// Returning the Box keeps its actual destructor outside this mutex and
    /// outside Core. Physical read retirement and close never clear these rows.
    pub(crate) fn discard_information(
        &self,
        reference: FinalTaskInfoRef,
    ) -> Result<Option<Box<FinalTaskInfo>>, PumpRecordError> {
        let mut records = self.records.lock().expect("diagnostic canonical records");
        let record = records
            .information
            .get_mut(&reference.request())
            .ok_or(PumpRecordError::MissingRecord)?;
        if record.task != reference.task() {
            return Err(PumpRecordError::WrongIdentity);
        }
        Ok(record.information.take())
    }
    #[cfg(test)]
    pub(crate) fn bind_reply_input(
        &self,
        request: AttemptRequestToken,
        input: InputId,
    ) -> Result<(), PumpRecordError> {
        self.records
            .lock()
            .expect("diagnostic canonical records")
            .reads
            .get_mut(&request)
            .ok_or(PumpRecordError::MissingRecord)?
            .job
            .bind_reply(input)
    }
    #[cfg(test)]
    pub(crate) fn core_consumed(&self, inputs: &[InputId]) {
        for record in self
            .records
            .lock()
            .expect("diagnostic canonical records")
            .reads
            .values_mut()
        {
            record.job.core_consumed(inputs);
        }
    }
    pub(super) fn resolve_result(
        &self,
        request: AttemptRequestToken,
    ) -> Result<(), PumpRecordError> {
        self.records
            .lock()
            .expect("diagnostic canonical records")
            .reads
            .get_mut(&request)
            .ok_or(PumpRecordError::MissingRecord)?
            .job
            .resolve_result();
        Ok(())
    }
    /// Diagnostics keep the actual transport text in the original record; it
    /// is logged as observation data and never classified as attempt failure.
    pub(crate) fn original_error(&self, failure: HostFailureRef) -> Option<String> {
        self.records
            .lock()
            .expect("diagnostic canonical records")
            .reads
            .values()
            .find(|record| record.failure == failure)?
            .error
            .clone()
    }
    pub(crate) fn close_external(&self) {
        self.records
            .lock()
            .expect("diagnostic canonical records")
            .closing = true;
    }
    pub(super) fn take_retired_one(&self) -> Option<Box<dyn std::any::Any + Send>> {
        let mut records = self.records.lock().expect("diagnostic canonical records");
        let id = records
            .reads
            .iter()
            .find_map(|(id, r)| r.job.retired().then_some(*id))?;
        if records.active == Some(id) {
            records.active = None;
        }
        Some(Box::new(
            records
                .reads
                .remove(&id)
                .expect("same retired diagnostic record"),
        ))
    }

    #[cfg(test)]
    fn take_retired(&self) -> Vec<Box<dyn std::any::Any + Send>> {
        std::iter::from_fn(|| self.take_retired_one()).collect()
    }

    pub(super) fn take_closing_information(&self) -> Option<Box<dyn std::any::Any + Send>> {
        self.records
            .lock()
            .expect("diagnostic canonical records")
            .information
            .pop_first()
            .map(|(_, original)| Box::new(original) as _)
    }

    pub(super) fn read_count(&self) -> usize {
        self.records
            .lock()
            .expect("diagnostic canonical records")
            .reads
            .len()
    }

    pub(super) fn backing_bytes(
        tasks: usize,
    ) -> Result<usize, super::super::observations::ObservationError> {
        use super::super::observations::backing;
        backing::sum([
            backing::arc::<Self>()?.size(),
            backing::tree::<AttemptRequestToken, DiagnosticRecord>(super::source::REPLY_ROLES)?,
            backing::tree::<AttemptRequestToken, InformationRecord>(tasks)?,
            backing::array::<FinalTaskInfo>(tasks)?,
            backing::array::<DiagnosticRecord>(2)?,
            backing::array::<InformationRecord>(2)?,
        ])
    }
}

#[cfg(test)]
mod tests;
