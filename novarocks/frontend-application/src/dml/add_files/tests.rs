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
    AddFilesEvidence, AddFilesPlanSummary, AddFilesPrepared,
};
use novarocks_query_application::admitted_query_context::{RequestAdmission, RequestContext};
use novarocks_query_application::api::BackendTopologySnapshot;
use novarocks_query_application::cancellation::QueryCancellationSource;
use novarocks_spi::connector::ConnectorDataMutationSourceScope;
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
                source_location: request.command.location,
                provider_id: "iceberg".to_string(),
                instance_id: "ice".to_string(),
                incarnation: [1; 16],
                mutation_operation_id: request.mutation_operation_id,
                request_digest: [2; 32],
                plan_digest: [3; 32],
                state_digest: [4; 32],
                summary: AddFilesPlanSummary {
                    file_count: 1,
                    row_count: 2,
                    total_bytes: 3,
                },
                source_scope: ConnectorDataMutationSourceScope::try_new_directory([5; 32])
                    .expect("valid source scope"),
                public_plan_wire: vec![6],
            },
            handle: Arc::new(TestPrepared),
        })
    }

    fn execute_add_files(&self, _prepared: &dyn AddFilesPrepared) -> AddFilesOutcome {
        self.execute_calls.fetch_add(1, Ordering::SeqCst);
        unreachable!("this test must not cross the dispatch edge")
    }

    fn adjudicate_add_files(
        &self,
        _prepared: &dyn AddFilesPrepared,
        _evidence: &AddFilesEvidence,
    ) -> AddFilesOutcome {
        unreachable!("this test must not adjudicate")
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
                    variant: true,
                    variant_binary: true,
                },
            ),
        ),
    ))
}

#[test]
fn caller_source_validation_does_not_treat_path_spelling_as_credentials() {
    assert!(is_secret_free_source_location(
        "s3://bucket/secret-data/files"
    ));
    assert!(!is_secret_free_source_location(
        "s3://key:secret@bucket/files"
    ));
    assert!(!is_secret_free_source_location(
        "s3://bucket/files?token=secret"
    ));
}

#[test]
fn planned_add_files_does_not_dispatch_until_the_second_edge() {
    let engine = RecordingAddFilesEngine::default();
    let prepared = DmlService::new()
        .prepare_add_files(
            &engine,
            AddFilesCommand {
                table_parts: vec!["ice".to_string(), "db".to_string(), "orders".to_string()],
                location: "s3://bucket/source".to_string(),
            },
            &context(),
            None,
        )
        .expect("plan ADD FILES without dispatch");

    assert_eq!(engine.plan_calls.load(Ordering::SeqCst), 1);
    assert_eq!(engine.execute_calls.load(Ordering::SeqCst), 0);
    drop(prepared);
    assert_eq!(engine.execute_calls.load(Ordering::SeqCst), 0);
}
