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
    input::{DiagnosticRead, ExecutorFact},
};
use novarocks_control_runtime::exit_custody::ExitCustody;
use novarocks_control_runtime_core::identity::InputId;
use novarocks_coordination_contract::AttemptRequestToken;
use novarocks_execution_contract::TaskIdentity;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

pub(crate) struct DiagnosticRegistration {
    pub(crate) request: AttemptRequestToken,
    pub(crate) task: TaskIdentity,
    pub(crate) failure: HostFailureRef,
    pub(crate) custody: ExitCustody,
    pub(crate) original_retention: Box<dyn std::any::Any + Send>,
}
struct DiagnosticRecord {
    task: TaskIdentity,
    failure: HostFailureRef,
    _original_retention: Box<dyn std::any::Any + Send>,
    job: OriginalJobRecord,
    started: bool,
    error: Option<String>,
}
struct Records {
    closing: bool,
    active: Option<AttemptRequestToken>,
    reads: BTreeMap<AttemptRequestToken, DiagnosticRecord>,
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
        if records.reads.contains_key(&registration.request) {
            return Err((PumpRecordError::DuplicateRecord, registration));
        }
        records.active = Some(registration.request);
        records.reads.insert(
            registration.request,
            DiagnosticRecord {
                task: registration.task,
                failure: registration.failure,
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
        let mut records = self.records.lock().expect("diagnostic canonical records");
        let record = records
            .reads
            .get_mut(&request)
            .ok_or(PumpRecordError::MissingRecord)?;
        let result = match result {
            Ok(FinalTaskInfoRead::Available(info)) => DiagnosticRead::Available(info),
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
    pub(crate) fn take_retired(&self) -> Vec<Box<dyn std::any::Any + Send>> {
        let mut records = self.records.lock().expect("diagnostic canonical records");
        let ids = records
            .reads
            .iter()
            .filter_map(|(id, r)| r.job.retired().then_some(*id))
            .collect::<Vec<_>>();
        ids.into_iter()
            .map(|id| {
                if records.active == Some(id) {
                    records.active = None;
                }
                Box::new(
                    records
                        .reads
                        .remove(&id)
                        .expect("same retired diagnostic record"),
                ) as Box<dyn std::any::Any + Send>
            })
            .collect()
    }
}
