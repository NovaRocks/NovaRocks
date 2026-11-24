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

use super::*;
use crate::commit::write_stack::control::{
    IcebergWriteSessionControl, selected_rewrite_files, validate_context,
};
use crate::commit::write_stack::test_support::FixtureByteAuthor;
use crate::distributed_rewrite::FrozenRewritePlanContent;
use novarocks_failure_contract::{
    FailureProjection, connector_reason::ICEBERG_COMMIT_VALIDATION_CONFLICT,
};
use novarocks_spi::connector::write_stack::{
    ConnectorWriteAbortProduction, ConnectorWriteFinishPublication, ConnectorWriteFinishRequest,
    ConnectorWriteSessionAbortRequest,
};
use novarocks_spi::connector::{
    ConnectorMutationFailureKind, ConnectorOriginalResultScope, ConnectorOriginalResultScopeCheck,
    ConnectorPayloadByteBudget, ConnectorPayloadRetentionGuard, ConnectorProcessLocalPayload,
    ConnectorWriteIntent, OriginalResultCheckClass, OriginalResultCheckError,
};

struct OriginalRewriteFixture {
    context: ConnectorRequestContext,
    bytes: FixtureByteAuthor,
}

impl ConnectorOriginalResultScopeCheck for OriginalRewriteFixture {
    fn original_deadline(&self) -> Instant {
        self.context.deadline()
    }

    fn check_active(&self) -> Result<(), OriginalResultCheckError> {
        validate_context(&self.context).map_err(|error| {
            OriginalResultCheckError::new(OriginalResultCheckClass::Control, error)
        })
    }
}

impl ConnectorPayloadByteBudget for OriginalRewriteFixture {
    fn reserve_bytes(
        &self,
        bytes: usize,
    ) -> Result<ConnectorPayloadRetentionGuard, ConnectorError> {
        self.bytes.reserve_bytes(bytes)
    }
}

fn seed_two_files(lake: &FrozenRewriteLake) {
    lake.write_rows(ConnectorWriteIntent::Append, &[1], "first");
    lake.write_rows(ConnectorWriteIntent::Append, &[2], "second");
}

#[test]
fn distributed_rewrite_begin_uses_the_exact_frozen_source_with_catalog_offline() {
    let lake = FrozenRewriteLake::new();
    seed_two_files(&lake);
    let plan = lake.plan();
    let content = plan
        .frozen()
        .value::<FrozenRewritePlanContent>("rewrite-local")
        .unwrap();
    let planned_head = content.metadata.current_snapshot().unwrap();
    assert_eq!(content.base_snapshot_id, Some(planned_head.snapshot_id()));
    assert_eq!(content.base_sequence_number, planned_head.sequence_number());

    lake.write_rows(ConnectorWriteIntent::Append, &[99], "concurrent");
    let current = lake.table();
    assert_ne!(
        current.metadata().current_snapshot_id(),
        content.base_snapshot_id
    );
    let before = lake.inventory();

    // Exact same generation and local storage. This owner rejects any table
    // read or mutation and still supplies the original Catalog admission.
    let catalog = Arc::new(owner_admission_support::AdmissionCatalogSpy::new(
        Arc::clone(lake.runtime.novarocks_catalog()),
    ));
    let offline = Arc::new(
        crate::metadata_context::IcebergMetadataContext::with_catalog_for_test(
            lake.runtime.control_state().clone(),
            lake.runtime.resources().clone(),
            catalog.clone(),
        ),
    );
    let control = IcebergWriteSessionControl::new(
        descriptor("rewrite-local"),
        lake.control.binding_key().incarnation,
        CatalogHandle::new(
            lake.control.binding_key().instance_id.clone(),
            CatalogVersion::from_bytes([6; 32]),
        ),
        offline,
    );
    let begin = lake.request(&plan);
    let context = begin.context.clone();
    let session = lake
        .executor
        .block_on(control.begin_write_async(begin))
        .unwrap();
    catalog.assert_no_io();
    let handle = control
        .adapter_for_test()
        .commit_session(session.commit_session())
        .unwrap();
    assert_eq!(handle.table().base_snapshot_id(), content.base_snapshot_id);
    assert_eq!(
        handle.table().base_sequence_number(),
        content.base_sequence_number
    );
    assert!(Arc::ptr_eq(
        handle.source_metadata_ref().unwrap(),
        &content.metadata
    ));
    assert!(Arc::ptr_eq(
        handle.frozen_rewrite_removals().unwrap(),
        &content.removals
    ));
    let aborted = control
        .abort_write(ConnectorWriteSessionAbortRequest {
            commit: session.commit_session(),
            production: ConnectorWriteAbortProduction::NotStarted,
            context,
        })
        .unwrap();
    assert!(matches!(
        aborted,
        ConnectorWriteAbortOutcome::KnownUncommitted { .. }
    ));
    catalog.assert_no_io();
    assert_eq!(lake.inventory(), before);
    assert_eq!(lake.rows(), vec![1, 2, 99]);
}

fn truncate_table(lake: &FrozenRewriteLake) {
    use crate::catalog_control::data_mutation::IcebergDataMutationAdapter;
    use novarocks_spi::connector::{
        ConnectorDataMutation, ConnectorDataMutationExecuteRequest, ConnectorDataMutationOperation,
        ConnectorDataMutationPlanningRequest, ConnectorMetadata, ConnectorMutationOperationId,
        ConnectorTableIdentity, ConnectorTableRequest, ConnectorTableResolution,
    };

    let context = request_context();
    let key = lake.control.binding_key();
    let metadata = Arc::new(crate::metadata::IcebergMetadata::new(
        descriptor("rewrite-local"),
        key.incarnation,
        Arc::clone(&lake.runtime),
    ));
    let table = crate::metadata::with_pure_schema_creation(&context, |creation| {
        metadata
            .load_table(
                ConnectorTableRequest {
                    table: ConnectorTableIdentity {
                        instance_id: key.instance_id.clone(),
                        namespace: Arc::from("db"),
                        table: Arc::from("t"),
                    },
                    resolution: ConnectorTableResolution::StrictBaseTable,
                    context: context.clone(),
                },
                creation,
            )
            .map_err(crate::metadata::metadata_creation_error)
    })
    .unwrap();
    let mutation = IcebergDataMutationAdapter::try_new(metadata).unwrap();
    let plan = mutation
        .plan_mutation(
            ConnectorDataMutationPlanningRequest::try_new(
                ConnectorMutationOperationId::new(),
                key.clone(),
                ConnectorDataMutationOperation::truncate_main(table.table),
                context.clone(),
            )
            .unwrap(),
        )
        .unwrap();
    let outcome = mutation
        .execute(ConnectorDataMutationExecuteRequest::try_new(plan, context).unwrap())
        .unwrap();
    assert!(matches!(
        outcome,
        ExternalMutationOutcome::KnownCommitted { .. }
    ));
}

#[test]
fn distributed_rewrite_replaces_only_frozen_inputs_and_validates_the_current_base() {
    let cases = [
        (false, false, vec![1, 2]),
        (true, false, vec![1, 2, 99]),
        (true, true, vec![99]),
    ];
    for (concurrent_append, concurrent_truncate, expected_rows) in cases {
        let lake = FrozenRewriteLake::new();
        seed_two_files(&lake);
        let plan = lake.plan();
        let content = plan
            .frozen()
            .value::<FrozenRewritePlanContent>("rewrite-local")
            .unwrap();

        // Change the live base before write begin. The writer must retain E;
        // only publication may validate the later B.
        if concurrent_truncate {
            truncate_table(&lake);
        }
        if concurrent_append {
            lake.write_rows(ConnectorWriteIntent::Append, &[99], "concurrent");
        }
        let base = lake.table();
        let current_parent = base.metadata().current_snapshot_id();
        let before_begin = lake.inventory();
        let mut begin = lake.request(&plan);
        let context = begin.context.clone();
        let capture = ConnectorOriginalResultScope::from_budgeted_original(Arc::new(
            OriginalRewriteFixture {
                context: context.clone(),
                bytes: FixtureByteAuthor(Arc::new(std::sync::atomic::AtomicUsize::new(0))),
            },
        ));
        // Model the original snapshot retention handed over by scoped planning.
        let content = Arc::new(FrozenRewritePlanContent {
            removal_retention: Some(Arc::new(capture.retention_guard())),
            ..content.as_ref().clone()
        });
        begin.flavor = ConnectorWriteSessionFlavor::DistributedRewrite {
            shape: plan.shape(),
            frozen: ConnectorProcessLocalPayload::new(
                lake.control.binding_key().clone(),
                Arc::clone(&content),
            ),
        };
        let session = lake
            .executor
            .block_on(
                lake.control
                    .begin_rewrite_with_scope_for_test(begin, &capture),
            )
            .unwrap();
        assert_eq!(lake.inventory(), before_begin);
        assert_eq!(session.targets().len(), plan.cohorts().len());
        let [target] = session.targets() else {
            panic!("the unpartitioned fixture has one group")
        };
        assert_eq!(
            target.rewrite_group_digest(),
            Some(plan.cohorts()[0].group_digest())
        );
        let handle = lake
            .adapter
            .commit_session(session.commit_session())
            .unwrap();
        let selected = selected_rewrite_files(handle).unwrap();
        assert_eq!(
            selected.data_paths,
            content
                .groups
                .iter()
                .flat_map(|group| {
                    group.data_files.iter().map(|file| EntryIdentity::DataFile {
                        path: file.path.clone(),
                    })
                })
                .collect()
        );
        assert_eq!(handle.table().base_snapshot_id(), content.base_snapshot_id);
        assert_eq!(
            handle.table().base_sequence_number(),
            content.base_sequence_number
        );

        let finish = |row_count| {
            lake.control.finish_write(ConnectorWriteFinishRequest {
                commit: session.commit_session(),
                row_count,
                statistics: Vec::new(),
                publication: ConnectorWriteFinishPublication::None,
                context: context.clone(),
            })
        };
        if !concurrent_append {
            let error = finish(0).unwrap_err();
            assert!(
                error
                    .message()
                    .contains("does not cover every frozen group"),
                "{error}"
            );
            assert!(matches!(
                handle.state().unwrap(),
                IcebergWriteSessionState::Active
            ));
            assert_eq!(lake.inventory(), before_begin);
        }

        let row_count = lake.output(&session, &[1, 2], "rewritten");
        let outcome = finish(row_count).unwrap();
        let current = lake.table();
        if concurrent_truncate {
            let ExternalMutationOutcome::KnownUncommitted { failure, .. } = outcome else {
                panic!("the competing truncate removed the exact frozen inputs: {outcome:?}");
            };
            assert_eq!(failure.kind(), ConnectorMutationFailureKind::Conflict);
            assert_eq!(
                failure.failure_fact().reason(),
                ICEBERG_COMMIT_VALIDATION_CONFLICT
            );
            assert!(
                failure
                    .message()
                    .starts_with("Missing required files to delete: "),
                "{failure}"
            );
            assert_eq!(current.metadata().current_snapshot_id(), current_parent);
        } else {
            assert!(
                matches!(
                    outcome,
                    ExternalMutationOutcome::KnownCommitted {
                        effect: ExternalMutationEffect::Applied,
                        ..
                    }
                ),
                "{outcome:?}"
            );
            let snapshot = current.metadata().current_snapshot().unwrap();
            assert_eq!(snapshot.parent_snapshot_id(), current_parent);
            assert_eq!(
                snapshot.summary().operation,
                crate::iceberg::spec::Operation::Replace
            );
            let files = lake
                .executor
                .block_on(crate::manifest::extract_data_files_with_stats(&current))
                .unwrap();
            let rewritten = files
                .iter()
                .filter(|file| file.data_sequence_number == Some(content.base_sequence_number))
                .collect::<Vec<_>>();
            assert_eq!(rewritten.len(), 1);
            assert_eq!(rewritten[0].record_count, Some(2));
            assert!(!selected.data_paths.contains(&EntryIdentity::DataFile {
                path: rewritten[0].path.clone(),
            }));
            assert_eq!(files.len(), if concurrent_append { 2 } else { 1 });
        }
        assert_eq!(lake.rows(), expected_rows);
        lake.inventory();
    }
}
