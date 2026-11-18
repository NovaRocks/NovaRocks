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
    ConnectorWriteSessionAbortRequest,
};
use novarocks_spi::connector::{ConnectorPayloadRetentionGuard, ExternalMutationOutcome};

use super::*;
use crate::commit::write_stack::domain::IcebergWriteBranch;
use crate::commit::write_stack::test_support::{received_write_proof, write_query_identity};
use crate::commit::write_stack::tests::{
    FrozenRewriteLake, adapter, data_fragment, ordinary_async_test_begin, request_context,
};

#[test]
fn registration_and_arrival_are_atomic_for_sealed_target_subsets() {
    let zero = WriteTargetOrdinal::try_new(0).unwrap();
    let one = WriteTargetOrdinal::try_new(1).unwrap();
    let targets = [
        IcebergSealedWriteTarget::new(one, IcebergWriteBranch::Data, None),
        IcebergSealedWriteTarget::new(zero, IcebergWriteBranch::Data, None),
    ];
    let query = write_query_identity();
    let mut received = IcebergReceiving::default();
    let outside = WriteTargetOrdinal::try_new(2).unwrap();
    let physical = ConnectorWriterPhysicalContext::new([2; 16], 1, [3; 16], 0, 1);
    let mut proof = Some(received_write_proof(one, physical));
    assert_eq!(
        received
            .begin_query(query, &[outside], &mut proof, &targets)
            .unwrap_err()
            .kind(),
        ConnectorErrorKind::InvalidRequest
    );
    assert!(received.is_unstarted());
    assert!(proof.is_some());
    received
        .begin_query(query, &[one], &mut proof, &targets)
        .unwrap();
    assert!(proof.is_none());
    let adapter = adapter("unit", 1);
    let mut fragments = vec![
        (
            one,
            adapter.wrap_commit_fragment(data_fragment("s3://b/valid.parquet")),
        ),
        (
            zero,
            adapter.wrap_commit_fragment(data_fragment("s3://b/wrong-target.parquet")),
        ),
    ];
    assert_eq!(
        received.arrive(query, &mut fragments).unwrap_err().kind(),
        ConnectorErrorKind::InvalidRequest
    );
    assert_eq!(fragments.len(), 2);
    assert_eq!(received.fragments().count(), 0);
    fragments.pop();
    received.arrive(query, &mut fragments).unwrap();
    assert!(fragments.is_empty());
    received.confirm(query).unwrap();
    assert_eq!(received.fragments().count(), 1);
}

#[test]
fn same_target_queries_keep_separate_proofs_and_completion() {
    let target = WriteTargetOrdinal::try_new(0).unwrap();
    let sealed = [IcebergSealedWriteTarget::new(
        target,
        IcebergWriteBranch::Data,
        None,
    )];
    let query1 = write_query_identity();
    let query2 = write_query_identity();
    let writer1 = ConnectorWriterPhysicalContext::new([1; 16], 1, [3; 16], 0, 0);
    let writer2 = ConnectorWriterPhysicalContext::new([2; 16], 1, [3; 16], 0, 0);
    let adapter = adapter("unit", 1);
    let mut received = IcebergReceiving::default();
    received
        .begin_query(
            query1,
            &[target],
            &mut Some(received_write_proof(target, writer1)),
            &sealed,
        )
        .unwrap();
    received
        .begin_query(query2, &[target], &mut None, &sealed)
        .unwrap();
    for (query, path) in [
        (query1, "s3://b/one.parquet"),
        (query2, "s3://b/two.parquet"),
    ] {
        received
            .arrive(
                query,
                &mut vec![(target, adapter.wrap_commit_fragment(data_fragment(path)))],
            )
            .unwrap();
    }
    received.confirm(query1).unwrap();
    assert_eq!(
        received.require_complete().unwrap_err().kind(),
        ConnectorErrorKind::Internal
    );
    assert!(received.confirm(query1).is_err());
    assert!(received.arrive(query1, &mut Vec::new()).is_err());
    received.confirm(query2).unwrap();
    received.require_complete().unwrap();
    let fragments: Vec<_> = received.fragments().collect();
    assert_eq!(fragments.len(), 2);
    assert!(fragments[0].admits_creation(&writer1).unwrap());
    assert!(!fragments[0].admits_creation(&writer2).unwrap());
    assert!(!fragments[1].admits_creation(&writer1).unwrap());
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
        .begin_write_query(commit, &query, &session.expected_targets(), None)
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
                None
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
    use crate::commit::write_stack::test_support::{data_branch_plan, receive_write_query};
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
    receive_write_query(
        &lake.control,
        &commit,
        &expected,
        vec![(
            expected[0],
            lake.adapter
                .wrap_commit_fragment(data_fragment(&format!("file://{}", path.display()))),
        )],
    )
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
