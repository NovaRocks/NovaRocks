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

use std::any::Any;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use super::*;
use crate::query_execution::dml::add_files::{
    AddFilesArguments, AddFilesEvidence, AddFilesPrepared,
};
use novarocks_query_application::admitted_query_context::{RequestAdmission, RequestContext};
use novarocks_query_application::api::BackendTopologySnapshot;
use novarocks_query_application::cancellation::QueryCancellationSource;
use novarocks_spi::connector::{ConnectorAddFilesFormat, ConnectorAddFilesRecursion};
use novarocks_types::ClusterRole;

struct TestPrepared;

impl AddFilesPrepared for TestPrepared {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Default)]
struct RecordingAddFilesEngine {
    plan_calls: AtomicUsize,
    execute_calls: AtomicUsize,
    adjudicate_calls: AtomicUsize,
    outcome: Option<AddFilesOutcome>,
    adjudicated: Option<AddFilesOutcome>,
}

impl AddFilesEngine for RecordingAddFilesEngine {
    fn plan_add_files(
        &self,
        request: PlanAddFilesRequest,
    ) -> Result<PreparedAddFiles, AddFilesPlanError> {
        self.plan_calls.fetch_add(1, Ordering::SeqCst);
        Ok(PreparedAddFiles {
            facts: AddFilesPlanFacts {
                catalog: "ice".to_string(),
                namespace: "db".to_string(),
                table: "orders".to_string(),
                provider_id: "iceberg".to_string(),
                instance_id: "ice".to_string(),
                incarnation: [1; 16],
                mutation_operation_id: request.mutation_operation_id,
                request_digest: [2; 32],
                plan_digest: [3; 32],
                state_digest: [4; 32],
                public_plan_wire: vec![6],
            },
            handle: Arc::new(TestPrepared),
        })
    }

    fn execute_add_files(
        &self,
        _prepared: &dyn AddFilesPrepared,
        byte_author: ConnectorPayloadRetentionGuard,
    ) -> AddFilesOutcome {
        self.execute_calls.fetch_add(1, Ordering::SeqCst);
        let _receipt = byte_author
            .reserve_owned_bytes(8)
            .expect("original statement budget");
        self.outcome
            .clone()
            .expect("test may cross the dispatch edge")
    }

    fn adjudicate_add_files(
        &self,
        _prepared: &dyn AddFilesPrepared,
        _evidence: &AddFilesEvidence,
    ) -> AddFilesOutcome {
        self.adjudicate_calls.fetch_add(1, Ordering::SeqCst);
        self.adjudicated.clone().expect("test may adjudicate once")
    }
}

fn context() -> RequestContext {
    let cancellation = QueryCancellationSource::new();
    RequestContext::admit(RequestAdmission::new(
        Some("ice".to_string()),
        "db".to_string(),
        ClusterRole::Fe,
        BackendTopologySnapshot::empty(1),
        Some(Instant::now() + Duration::from_secs(30)),
        cancellation.view(),
        Default::default(),
        novarocks_sql::sql_mode::SqlSemanticSettings::default(),
        Some(
            novarocks_result_contract::ClientPresentationFamily::TrinoJson(
                novarocks_result_contract::TrinoJsonFacts {
                    parametric_datetime: true,
                    parametric_interval: false,
                    variant: true,
                    variant_binary: true,
                },
            ),
        ),
    ))
}

fn command() -> AddFilesCommand {
    AddFilesCommand {
        table_parts: vec!["ice".to_string(), "db".to_string(), "orders".to_string()],
        arguments: AddFilesArguments {
            location: Some("s3://bucket/source".into()),
            format: Some(ConnectorAddFilesFormat::Parquet),
            recursion: ConnectorAddFilesRecursion::Fail,
        },
    }
}

fn original_root() -> (
    novarocks_workload_control::WorkloadControl,
    novarocks_workload_control::RootWork,
    ResultWindowAlias,
) {
    use novarocks_workload_control::{
        ResourceConfig, ResultCapacityConfig, ResultWindowClass, WorkClass, WorkRequest,
        WorkloadConfig, WorkloadControl,
    };
    let control = WorkloadControl::try_new(
        WorkloadConfig::default(),
        ResourceConfig {
            total_bytes: 1 << 20,
            control_bytes: 1024,
            per_scope_bytes: (1 << 20) - 1024,
        },
    )
    .unwrap();
    control
        .configure_result_capacity(ResultCapacityConfig::V1)
        .unwrap();
    control.mark_ready().unwrap();
    let (root, window) = control
        .root_admission()
        .try_begin_root_with_result(
            WorkRequest::new(WorkClass::Management),
            ResultWindowClass::Local,
        )
        .unwrap();
    (control, root, window.retain_alias())
}

#[test]
fn planned_add_files_does_not_dispatch_until_the_second_edge() {
    let engine = RecordingAddFilesEngine::default();
    let prepared = DmlService::new()
        .prepare_add_files(&engine, command(), &context(), None)
        .expect("plan ADD FILES without dispatch");

    assert_eq!(engine.plan_calls.load(Ordering::SeqCst), 1);
    assert_eq!(engine.execute_calls.load(Ordering::SeqCst), 0);
    drop(prepared);
    assert_eq!(engine.execute_calls.load(Ordering::SeqCst), 0);
}

#[test]
fn add_files_receipts_use_the_same_root_and_release_when_backing_exits() {
    let (_control, _root, window) = original_root();
    let author = registration_byte_author(window.clone());
    let full = author
        .reserve_owned_bytes(window.payload_capacity_bytes() as usize)
        .unwrap();
    assert_eq!(
        window.retained_payload_bytes(),
        window.payload_capacity_bytes()
    );
    let denied = author.reserve_owned_bytes(1).unwrap_err();
    assert_eq!(denied.kind(), ConnectorErrorKind::ResourceExhausted);
    let last = full.clone();
    drop(full);
    drop(author);
    assert_eq!(
        window.retained_payload_bytes(),
        window.payload_capacity_bytes()
    );
    drop(last);
    assert_eq!(window.retained_payload_bytes(), 0);
}

#[test]
fn add_files_original_cause_survives_plan_refusal_and_dispatch_verdicts() {
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    use novarocks_spi::connector::LakePublicationDisposition;

    let failure = AddFilesFailure::from_connector(ConnectorError::new(
        ConnectorErrorKind::ResourceExhausted,
        "original registration capacity refusal",
    ));
    let planned = plan_error(AddFilesPlanError::KnownUncommitted(failure.clone()));
    let original = planned
        .application_error()
        .unwrap()
        .failure_cause()
        .unwrap();
    assert_eq!(original.failure_fact().class(), FailureClass::Resource);
    let cases = [
        (
            AddFilesOutcome::KnownUncommitted {
                failure: failure.clone(),
            },
            LakePublicationDisposition::KnownUncommitted,
        ),
        (
            AddFilesOutcome::ContractFailure {
                failure: failure.clone(),
                dispatch: AddFilesDispatchState::ConfirmedNotDispatched,
            },
            LakePublicationDisposition::KnownUncommitted,
        ),
        (
            AddFilesOutcome::ContractFailure {
                failure,
                dispatch: AddFilesDispatchState::PossiblyDispatched,
            },
            LakePublicationDisposition::CommitUnknown,
        ),
    ];
    let (_control, _root, window) = original_root();
    for (outcome, disposition) in cases {
        let engine = RecordingAddFilesEngine {
            outcome: Some(outcome),
            ..Default::default()
        };
        let dml = DmlService::new();
        let prepared = dml
            .prepare_add_files(&engine, command(), &context(), None)
            .unwrap();
        let error = dml
            .execute_prepared_add_files(&engine, prepared, registration_byte_author(window.clone()))
            .unwrap_err();
        assert_eq!(
            error.publication_terminal().unwrap().disposition(),
            disposition
        );
        assert_eq!(
            error.application_error().unwrap().failure_cause(),
            Some(original)
        );
        assert_eq!(window.retained_payload_bytes(), 0);
        assert_eq!(engine.execute_calls.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn add_files_unknown_adjudication_keeps_one_original_outcome() {
    use novarocks_spi::connector::LakePublicationDisposition;
    let failure = AddFilesFailure::from_connector(ConnectorError::new(
        ConnectorErrorKind::Unavailable,
        "response lost after dispatch",
    ));
    let evidence = AddFilesEvidence {
        schema_version: 1,
        digest: [0; 32],
        wire_bytes: vec![1],
    };
    let unknown = AddFilesOutcome::CommitUnknown { failure, evidence };
    let engine = RecordingAddFilesEngine {
        outcome: Some(unknown.clone()),
        adjudicated: Some(unknown),
        ..Default::default()
    };
    let dml = DmlService::new();
    let (_control, _root, window) = original_root();
    let prepared = dml
        .prepare_add_files(&engine, command(), &context(), None)
        .unwrap();
    let error = dml
        .execute_prepared_add_files(&engine, prepared, registration_byte_author(window))
        .unwrap_err();
    let terminal = error.publication_terminal().unwrap();
    assert_eq!(
        terminal.disposition(),
        LakePublicationDisposition::CommitUnknown
    );
    assert!(terminal.do_not_retry());
    assert_eq!(engine.execute_calls.load(Ordering::SeqCst), 1);
    assert_eq!(engine.adjudicate_calls.load(Ordering::SeqCst), 1);
}
