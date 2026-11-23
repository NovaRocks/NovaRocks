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

//! Exercise exact query provenance in receiving and its handoff to publication.

use novarocks_spi::connector::write_stack::{
    ConnectorWriteControl, ConnectorWriteFinishPublication, ConnectorWriteFinishRequest,
    ConnectorWriteSessionAbortRequest, ConnectorWriterPhysicalContext,
};
use novarocks_spi::connector::{ConnectorPayloadRetentionGuard, ExternalMutationOutcome};

use super::*;
use crate::commit::write_stack::domain::{IcebergWriteBranch, IcebergWriteFlavor};
use crate::commit::write_stack::test_support::{
    paid_fragments, received_write_proof, write_query_identity,
};
use crate::commit::write_stack::tests::{
    FrozenRewriteLake, adapter, data_fragment, dv_session, ordinary_async_test_begin,
    request_context,
};

#[test]
fn registration_and_arrival_are_atomic_for_sealed_target_subsets() {
    let (handle, adapter) = dv_session();
    let [data, delete] = handle.targets() else {
        panic!("expected data and delete targets");
    };
    let data = data.ordinal();
    let delete = delete.ordinal();
    let query = write_query_identity();
    let mut received = IcebergReceiving::default();
    let outside = WriteTargetOrdinal::try_new(2).unwrap();
    let physical = ConnectorWriterPhysicalContext::new([2; 16], 1, [3; 16], 0, data.get());
    let mut proof = Some(received_write_proof(data, physical));
    assert_eq!(
        received
            .begin_query(query, &[outside], &mut proof, handle.targets(), None)
            .unwrap_err()
            .kind(),
        ConnectorErrorKind::InvalidRequest
    );
    assert!(received.is_unstarted());
    assert!(proof.is_some());
    received
        .begin_query(query, &[data], &mut proof, handle.targets(), None)
        .unwrap();
    assert!(proof.is_none());
    let arrive = |received: &mut IcebergReceiving, targets: &[(WriteTargetOrdinal, &str)]| {
        let (fragments, author) = paid_fragments(
            &adapter,
            targets
                .iter()
                .map(|(_, path)| adapter.wrap_commit_fragment(data_fragment(path)))
                .collect(),
        );
        received.arrive_original(
            &handle,
            &adapter,
            query,
            targets
                .iter()
                .map(|(target, _)| *target)
                .zip(fragments)
                .collect(),
            &author,
        )
    };
    let valid = (data, "s3://b/valid.parquet");
    let foreign = (delete, "s3://b/wrong-target.parquet");
    assert_eq!(
        arrive(&mut received, &[valid, foreign]).unwrap_err().kind(),
        ConnectorErrorKind::InvalidRequest
    );
    assert!(received.is_empty());
    arrive(&mut received, &[valid]).unwrap();
    received.confirm(query).unwrap();
    assert_eq!(received.compact_files().entries().count(), 1);
}

#[test]
fn same_target_queries_keep_separate_proofs_and_completion() {
    let (handle, adapter) = dv_session();
    let target = WriteTargetOrdinal::try_new(0).unwrap();
    let query1 = write_query_identity();
    let query2 = write_query_identity();
    let writer1 = ConnectorWriterPhysicalContext::new([1; 16], 1, [3; 16], 0, 0);
    let writer2 = ConnectorWriterPhysicalContext::new([2; 16], 1, [3; 16], 0, 0);
    let mut received = IcebergReceiving::default();
    received
        .begin_query(
            query1,
            &[target],
            &mut Some(received_write_proof(target, writer1)),
            handle.targets(),
            None,
        )
        .unwrap();
    received
        .begin_query(query2, &[target], &mut None, handle.targets(), None)
        .unwrap();
    for (query, path) in [
        (query1, "s3://b/one.parquet"),
        (query2, "s3://b/two.parquet"),
    ] {
        let (fragments, author) = paid_fragments(
            &adapter,
            vec![adapter.wrap_commit_fragment(data_fragment(path))],
        );
        received
            .arrive_original(
                &handle,
                &adapter,
                query,
                fragments
                    .into_iter()
                    .map(|fragment| (target, fragment))
                    .collect(),
                &author,
            )
            .unwrap();
    }
    received.confirm(query1).unwrap();
    assert_eq!(
        received.require_complete().unwrap_err().kind(),
        ConnectorErrorKind::Internal
    );
    assert!(received.confirm(query1).is_err());
    let (_, author) = paid_fragments(&adapter, Vec::new());
    assert!(
        received
            .arrive_original(&handle, &adapter, query1, Vec::new(), &author)
            .is_err()
    );
    received.confirm(query2).unwrap();
    received.require_complete().unwrap();
    assert_eq!(received.compact_files().entries().count(), 2);
    let proof = received.queries[&query1].production.as_ref().unwrap();
    assert!(proof.admits(target, &writer1).unwrap());
    assert!(!proof.admits(target, &writer2).unwrap());
    assert!(received.queries[&query2].production.is_none());
}

#[test]
fn finish_and_exact_abort_refuse_unconfirmed_queries_without_consuming_receiving() {
    let lake = FrozenRewriteLake::new();
    let session = lake
        .control
        .begin_write(ordinary_async_test_begin(request_context()))
        .unwrap();
    let commit = session.commit_session();
    let query = write_query_identity();
    lake.control
        .begin_write_query(commit, &query, &session.expected_targets(), None, None)
        .unwrap();
    let error = lake
        .control
        .finish_write(ConnectorWriteFinishRequest {
            commit,
            row_count: 0,
            statistics: Vec::new(),
            publication: ConnectorWriteFinishPublication::None,
            context: request_context(),
        })
        .unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::Internal);
    let error = lake.control.abort_write(ConnectorWriteSessionAbortRequest {
        commit,
        context: request_context(),
        production: novarocks_spi::connector::write_stack::ConnectorWriteAbortProduction::CompleteReceived,
    }).unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::Internal);
    lake.control.confirm_write_received(commit, &query).unwrap();
    lake.control.abort_write(ConnectorWriteSessionAbortRequest {
        commit,
        context: request_context(),
        production: novarocks_spi::connector::write_stack::ConnectorWriteAbortProduction::CompleteReceived,
    }).unwrap();
    assert!(
        lake.control
            .arrive_write_fragments(
                commit,
                &query,
                Vec::new(),
                &ConnectorPayloadRetentionGuard::new(()),
            )
            .is_err()
    );
    assert!(
        lake.control
            .begin_write_query(
                commit,
                &write_query_identity(),
                &session.expected_targets(),
                None,
                None,
            )
            .is_err()
    );
}

#[test]
fn finish_publishes_one_snapshot_containing_rows_from_both_queries() {
    let lake = FrozenRewriteLake::new();
    let session = lake
        .control
        .begin_write(ordinary_async_test_begin(request_context()))
        .unwrap();
    let row_count =
        lake.output(&session, &[1, 2], "first") + lake.output(&session, &[7, 9], "second");
    let outcome = lake
        .control
        .finish_write(ConnectorWriteFinishRequest {
            commit: session.commit_session(),
            row_count,
            statistics: Vec::new(),
            publication: ConnectorWriteFinishPublication::None,
            context: request_context(),
        })
        .unwrap();
    assert!(matches!(
        outcome,
        ExternalMutationOutcome::KnownCommitted { .. }
    ));
    assert_eq!(lake.rows(), vec![1, 2, 7, 9]);
}

#[test]
fn recipe_refusal_cleans_original_adopted_data_and_repeat_release_observes_it() {
    use crate::commit::write_stack::domain::{
        IcebergWriteFlavor, IcebergWriteSessionId, IcebergWriteTableFacts,
    };
    use crate::commit::write_stack::planning::{IcebergWriteSessionPlanInput, plan_write_session};
    use crate::commit::write_stack::test_support::data_branch_plan;
    use novarocks_spi::connector::write_stack::ConnectorWriteAbortProduction;
    use novarocks_spi::connector::{
        ConnectorWriteAbortOutcome, ConnectorWriteAdmissionPurpose, ExternalMutationFinalization,
    };

    let lake = FrozenRewriteLake::new();
    let directory = tempfile::tempdir().unwrap();
    let location = format!("file://{}", directory.path().display());
    let path = directory.path().join("adopted.parquet");
    std::fs::write(&path, b"received data").unwrap();
    let table = IcebergWriteTableFacts::try_new(
        uuid::Uuid::new_v4().to_string(),
        "db".into(),
        "t".into(),
        location.clone(),
        location,
        "main".into(),
        None,
        0,
        0,
        0,
        2,
    )
    .unwrap();
    // This synthetic session has valid received files but omits the pinned
    // source metadata the provider needs to construct a publication recipe.
    let (handle, _) = plan_write_session(
        IcebergWriteSessionId::new(),
        IcebergWriteSessionPlanInput {
            flavor: IcebergWriteFlavor::Append,
            purpose: ConnectorWriteAdmissionPurpose::OrdinaryDml,
            table,
            base_version_digest: None,
            staged_metadata: None,
            data: data_branch_plan(),
            deletes: Vec::new(),
        },
    )
    .unwrap();
    let expected = handle.expected_targets();
    let commit = lake.adapter.wrap_commit_session(handle);
    let (fragments, author) = super::super::test_support::paid_fragments(
        &lake.adapter,
        vec![
            lake.adapter
                .wrap_commit_fragment(data_fragment(&format!("file://{}", path.display()))),
        ],
    );
    let query = write_query_identity();
    lake.control
        .begin_write_query(&commit, &query, &expected, None, None)
        .unwrap();
    lake.control
        .arrive_write_fragments(
            &commit,
            &query,
            fragments
                .into_iter()
                .map(|fragment| (expected[0], fragment))
                .collect(),
            &author,
        )
        .unwrap();
    lake.control
        .confirm_write_received(&commit, &query)
        .unwrap();
    let ExternalMutationOutcome::KnownUncommitted { failure, cleanup } = lake
        .control
        .finish_write(ConnectorWriteFinishRequest {
            commit: &commit,
            row_count: 5,
            statistics: Vec::new(),
            publication: ConnectorWriteFinishPublication::None,
            context: request_context(),
        })
        .unwrap()
    else {
        panic!("the operation cannot publish without its original writer metadata");
    };
    assert!(
        failure
            .message()
            .contains("lost its admitted source metadata")
    );
    assert!(matches!(cleanup, ExternalMutationFinalization::Complete));
    assert!(!path.exists());
    assert!(matches!(
        lake.control
            .abort_write(ConnectorWriteSessionAbortRequest {
                commit: &commit,
                context: request_context(),
                production: ConnectorWriteAbortProduction::CompleteReceived,
            })
            .unwrap(),
        ConnectorWriteAbortOutcome::KnownUncommitted {
            cleanup: ExternalMutationFinalization::Complete
        }
    ));
}

mod deletion_effects;
mod failures;
mod logical_rows;

mod rewritten_target;

mod written_partitions;

mod rewrite_compact;
