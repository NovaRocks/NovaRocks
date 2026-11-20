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

//! Observes which catalog generation and write recipe survive session admission.

use super::*;
use crate::catalog::{
    SnapshotLoading,
    admission::CatalogOperation,
    admission_test_support::{
        all_admitted_recording_with_snapshot_trace_and_dispatch_fault,
        all_admitted_with_snapshot_trace, publish_frozen,
    },
    error::CatalogOutcome,
};
use crate::commit::model::{AttemptToken, IsolationLevel, OperationToken, RequestShape};
use crate::commit::staging::{PreparedChange, freeze_existing_change};
use crate::iceberg::spec::FormatVersion;
use novarocks_spi::connector::write_stack::{
    ConnectorWriteControl, ConnectorWriteSessionAbortRequest,
};
use novarocks_spi::connector::{
    ConnectorMutationOperationId, ConnectorRowMutationPreparationOutcome,
    ConnectorWriteInputRequest, ConnectorWriteIntent,
};
use std::collections::HashMap;
use std::sync::Mutex;

const ISOLATION: &str = "write.delete.isolation-level";
const RETRIES: &str = "commit.retry.num-retries";

fn set_properties(lake: &FrozenRewriteLake, values: &[(&str, &str)]) {
    let table = lake.table();
    let context = request_context();
    let request = freeze_existing_change(
        table.identifier().clone(),
        "main",
        table.metadata(),
        table.metadata_location().unwrap(),
        RequestShape::MetadataOnly,
        AttemptToken::new(
            OperationToken::from_mutation(ConnectorMutationOperationId::new()),
            0,
        ),
        PreparedChange {
            requirements: vec![],
            updates: vec![crate::iceberg::TableUpdate::SetProperties {
                updates: values
                    .iter()
                    .map(|(key, value)| (key.to_string(), value.to_string()))
                    .collect::<HashMap<_, _>>(),
            }],
        },
        &context,
    )
    .unwrap();
    let outcome = lake.executor.block_on(publish_frozen(
        Arc::clone(lake.runtime.novarocks_catalog()),
        descriptor("rewrite-local"),
        lake.control.binding_key().incarnation,
        request,
        CatalogOperation::AlterProperties,
        context,
        None,
    ));
    assert!(
        matches!(outcome, CatalogOutcome::KnownCommitted { .. }),
        "{outcome:?}"
    );
}

#[test]
fn begin_keeps_admitted_generation_after_catalog_advances_and_never_observes_it() {
    let lake = FrozenRewriteLake::new_version(FormatVersion::V3);
    lake.write_rows(ConnectorWriteIntent::Append, &[1, 2], "source");
    set_properties(&lake, &[(ISOLATION, "SnApShOt"), (RETRIES, "2")]);
    let admitted = lake.table();
    let original_context = request_context();
    let preparation = lake.prepare_row_delete(&original_context);
    let start = admitted.metadata().current_snapshot_id();
    assert_eq!(preparation.base_version_ordinal(), start);
    lake.write_rows(ConnectorWriteIntent::Append, &[3], "racing");
    assert_ne!(lake.table().metadata().current_snapshot_id(), start);
    set_properties(&lake, &[(ISOLATION, "serializable"), (RETRIES, "0")]);

    let trace: Arc<Mutex<Vec<SnapshotLoading>>> = Arc::new(Mutex::new(Vec::new()));
    let catalog = all_admitted_with_snapshot_trace(
        Arc::clone(lake.runtime.novarocks_catalog()),
        Arc::clone(&trace),
    );
    let runtime = Arc::new(
        crate::metadata_context::IcebergMetadataContext::with_catalog_for_test(
            lake.runtime.control_state().clone(),
            lake.runtime.resources().clone(),
            catalog,
        ),
    );
    let control = crate::commit::write_stack::control::IcebergWriteSessionControl::new(
        descriptor("rewrite-local"),
        lake.control.binding_key().incarnation,
        CatalogHandle::new(
            lake.control.binding_key().instance_id.clone(),
            CatalogVersion::from_bytes([6; 32]),
        ),
        runtime,
    );
    let input = crate::commit::write_preparation::freeze_position_mutation_input(
        admitted.metadata(),
        "main",
        start,
        &ConnectorWriteInputRequest::DeletionVector {
            identity_fields: Vec::new(),
            partition_source_fields: Vec::new(),
        },
        &original_context,
    )
    .unwrap();
    let begin = ConnectorWriteBeginRequest {
        catalog_session: Default::default(),
        table: Arc::from("db.t"),
        target_ref: novarocks_spi::connector::ConnectorWriteTargetRef::main(),
        intent: ConnectorWriteIntent::RowDelta,
        purpose: ConnectorWriteAdmissionPurpose::OrdinaryDml,
        input,
        base: Some(preparation.base_version().clone()),
        flavor: ConnectorWriteSessionFlavor::RowMutation {
            admission: preparation.admission(),
        },
        context: original_context.clone(),
    };
    let session = control.begin_write(begin.clone()).unwrap();
    assert!(trace.lock().unwrap().is_empty());
    let handle = control
        .adapter_for_test()
        .commit_session(session.commit_session())
        .unwrap();
    assert_eq!(
        handle.row_mutation_isolation(),
        Some(IsolationLevel::Snapshot)
    );
    assert_eq!(handle.table().base_snapshot_id(), start);
    assert_eq!(
        handle.source_metadata_ref().unwrap().as_ref(),
        admitted.metadata()
    );
    assert_eq!(
        handle.source_metadata_ref().unwrap().properties()[RETRIES],
        "2"
    );
    handle
        .mutation_read()
        .unwrap()
        .validate_loaded(handle.table(), admitted.metadata())
        .unwrap();
    assert_eq!(
        handle.mutation_read().unwrap().metadata_location(),
        admitted.metadata_location().unwrap()
    );
    control
        .abort_write(ConnectorWriteSessionAbortRequest {
            commit: session.commit_session(),
            production:
                novarocks_spi::connector::write_stack::ConnectorWriteAbortProduction::NotStarted,
            context: original_context.clone(),
        })
        .unwrap();

    let mut incompatible = begin;
    incompatible.base = None;
    let Err(error) = control.begin_write(incompatible) else {
        panic!("begin accepted an admission that contradicted its request");
    };
    assert!(error.message().contains("differs from its admitted target"));
    assert!(trace.lock().unwrap().is_empty());
}

#[test]
fn an_unknown_delete_isolation_is_rejected_during_provider_admission() {
    let lake = FrozenRewriteLake::new_version(FormatVersion::V3);
    set_properties(&lake, &[(ISOLATION, "read-committed")]);
    let ConnectorRowMutationPreparationOutcome::Denied(error) =
        lake.admit_row_delete(&request_context())
    else {
        panic!("unknown delete isolation was admitted");
    };
    assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    assert_eq!(error.message(), "Invalid isolation level: read-committed");
}

#[test]
fn catalog_retry_reuses_the_original_received_vector() {
    let mut lake = FrozenRewriteLake::new_version(FormatVersion::V3);
    lake.write_rows(ConnectorWriteIntent::Append, &[1, 2], "two-row-data-file");
    set_properties(&lake, &[(ISOLATION, "snapshot")]);
    let table = lake.table();
    let files = lake
        .executor
        .block_on(crate::manifest::extract_data_files_with_stats(&table))
        .unwrap();
    let [file] = files.as_slice() else {
        panic!("fixture must have exactly one live data file");
    };
    let trace = Arc::new(Mutex::new(Vec::new()));
    let dispatch_vectors = Arc::new(Mutex::new(Vec::new()));
    let catalog = all_admitted_recording_with_snapshot_trace_and_dispatch_fault(
        Arc::clone(lake.runtime.novarocks_catalog()),
        Arc::new({
            let records = Arc::clone(&dispatch_vectors);
            let data = crate::commit::write_stack::control::iceberg_data_location(table.metadata());
            let staging = std::path::Path::new(data.trim_start_matches("file://")).join("_staging");
            move |_, _| {
                let paths = std::fs::read_dir(&staging)
                    .unwrap()
                    .flat_map(|query| std::fs::read_dir(query.unwrap().path()).unwrap())
                    .map(|object| object.unwrap().path())
                    .filter(|path| {
                        path.extension()
                            .is_some_and(|extension| extension == "puffin")
                    })
                    .map(|path| {
                        let file = std::fs::metadata(&path).unwrap();
                        (path, file.len(), file.modified().unwrap())
                    })
                    .collect::<Vec<_>>();
                records.lock().unwrap().push(paths);
            }
        }),
        Arc::clone(&trace),
        Arc::new(Mutex::new(Some(
            crate::iceberg::ErrorKind::CatalogCommitConflicts,
        ))),
    );
    let runtime = Arc::new(
        crate::metadata_context::IcebergMetadataContext::with_catalog_for_test(
            lake.runtime.control_state().clone(),
            lake.runtime.resources().clone(),
            catalog,
        ),
    );
    let binding = lake.control.binding_key().clone();
    lake.control = crate::commit::write_stack::control::IcebergWriteSessionControl::new(
        descriptor("rewrite-local"),
        binding.incarnation,
        CatalogHandle::new(binding.instance_id, CatalogVersion::from_bytes([6; 32])),
        runtime,
    );

    lake.seed_position_vector(file);

    assert_eq!(*trace.lock().unwrap(), [SnapshotLoading::All; 2]);
    let dispatches = dispatch_vectors.lock().unwrap();
    assert_eq!(dispatches.len(), 2);
    assert_eq!(dispatches[0].len(), 1, "{dispatches:?}");
    assert_eq!(dispatches[0], dispatches[1]);
}
