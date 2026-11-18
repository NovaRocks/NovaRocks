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

//! Cross-cutting write-stack tests.
//!
use crate::commit::model::EntryIdentity;
use novarocks_spi::connector::write_stack::{ConnectorWriteBeginRequest, ConnectorWriteControl};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use novarocks_fs::{FsAccessResolver, TokioFileIoRuntime, TokioFileIoSpawner};
use novarocks_spi::connector::write_stack::session::{
    ConnectorWriteRouteFacts, ConnectorWriteSessionFlavor, ConnectorWriteSessionPlan,
    ConnectorWriteTargetPlan,
};
use novarocks_spi::connector::write_stack::{
    ConnectorManagedPublicationShape, ConnectorWriteQueryIdentity, WriteRuntimeAdapter,
    WriteTargetOrdinal,
};
use novarocks_spi::connector::{
    CatalogHandle, CatalogVersion, ConnectorCommittedVersion, ConnectorDistributedRewriteShape,
    ConnectorError, ConnectorErrorKind, ConnectorInstanceDescriptor, ConnectorInstanceId,
    ConnectorManagedPublicationEmptyInputDisposition, ConnectorManagedPublicationTechnique,
    ConnectorMutationRouteInput, ConnectorProviderId, ConnectorRequestContext,
    ConnectorRowMutationEffect, ConnectorWriteAbortOutcome, ConnectorWriteAdmissionPurpose,
    ConnectorWriteInputShape, ConnectorWriteRouteId, ExternalMutationEffect,
    ExternalMutationFinalization, ExternalMutationOutcome, ProviderBindingEpoch,
};

use crate::access_binding::IcebergReadBinding;
use crate::commit::CommitOpKind;
use crate::commit::write_stack::control::{
    empty_write_needed, release_session_state, session_plan_from_targets, validate_received_set,
};
use crate::commit::write_stack::domain::{
    IcebergCommitFragment, IcebergDataFileArtifact, IcebergEmptyWriteDecision,
    IcebergPositionDeleteFileArtifact, IcebergWriteBranch, IcebergWriteCommitSession,
    IcebergWriteFlavor, IcebergWriteSessionId, IcebergWriteSessionState, IcebergWriteTableFacts,
};
use crate::commit::write_stack::flavor::{
    IcebergFrozenRewriteBranch, IcebergSessionFlavorPlan, plan_distributed_rewrite_branches,
    plan_document_publication_branches, plan_ordinary_branches, plan_row_mutation_branches,
};
use crate::commit::write_stack::planning::{
    IcebergBranchSessionPlanInput, IcebergWriteBranchPlan, IcebergWriteSessionPlanInput,
    IcebergWriteTargetPlan, plan_branch_session, plan_write_session,
};
use crate::commit::write_stack::receiving::IcebergReceiving;
use crate::commit::write_stack::runtime::{IcebergWriteAdapter, IcebergWriteRuntime};
use crate::commit::write_stack::test_support::{
    binding, data_branch_plan, data_input_shape, delete_branch_plan, delete_input_shape,
    document_publication_facts, document_publication_facts_with_shape, dv_artifact,
    equality_delete_input_shape, equality_delete_recipe, lineage_only_input_shape,
    merge_on_read_input_shape, position_rewrite_target, sample_metrics, sample_partition,
    session_material, table_facts,
};
use crate::commit::write_stack::test_support::{receive_write_query, write_query_identity};
use crate::delete_file::IcebergFileFormat;
use crate::manifest::DataFileWithStats;

pub(in crate::commit) fn request_context() -> ConnectorRequestContext {
    ConnectorRequestContext::try_new(
        Instant::now() + Duration::from_secs(30),
        novarocks_spi::connector::ConnectorStopOwner::new().view(),
        64 * 1024,
        1024 * 1024,
    )
    .expect("request context")
}

fn descriptor(catalog: &str) -> ConnectorInstanceDescriptor {
    ConnectorInstanceDescriptor {
        provider_id: ConnectorProviderId::parse("iceberg").expect("provider id"),
        instance_id: ConnectorInstanceId::parse(catalog).expect("instance id"),
    }
}

pub(in crate::commit) fn adapter(catalog: &str, version: u8) -> IcebergWriteAdapter {
    let descriptor = descriptor(catalog);
    let catalog_handle = CatalogHandle::new(
        descriptor.instance_id.clone(),
        CatalogVersion::from_bytes([version; 32]),
    );
    WriteRuntimeAdapter::new(Arc::new(IcebergWriteRuntime::new(
        descriptor,
        catalog_handle,
    )))
}

// -------------------------------------------------------------------------
// Domain round-trips through the provider-private adapter.
// -------------------------------------------------------------------------

#[test]
fn every_fragment_kind_round_trips_through_its_own_adapter() {
    let adapter = adapter("unit", 1);
    let data = IcebergCommitFragment::data_file(
        IcebergDataFileArtifact::try_new(
            "s3://b/data/f.parquet".to_string(),
            IcebergFileFormat::Parquet,
            sample_partition(),
            sample_metrics(10, 2048),
            Some(64),
        )
        .expect("data artifact"),
    );
    let position_delete = IcebergCommitFragment::position_delete_file(
        IcebergPositionDeleteFileArtifact::try_new(
            "s3://b/data/d.parquet".to_string(),
            sample_partition(),
            sample_metrics(3, 512),
            "s3://b/data/f.parquet".to_string(),
            vec![EntryIdentity::DeleteFile {
                path: "s3://b/data/old.parquet".to_string(),
            }],
        )
        .expect("position delete artifact"),
    );
    let deletion_vector = IcebergCommitFragment::deletion_vector(
        dv_artifact(
            "s3://b/data/v.puffin",
            "s3://b/data/f.parquet",
            3,
            1024,
            4,
            64,
        )
        .expect("deletion vector artifact"),
    );

    for (original, branch) in [
        (data, IcebergWriteBranch::Data),
        (position_delete, IcebergWriteBranch::PositionDelete),
        (deletion_vector, IcebergWriteBranch::DeletionVector),
    ] {
        let path = original.path().to_string();
        let wrapped = adapter.wrap_commit_fragment(original);
        let recovered = adapter.commit_fragment(&wrapped).expect("recover fragment");
        assert_eq!(recovered.branch(), branch);
        assert_eq!(recovered.path(), path);
    }
}

#[test]
fn another_catalog_generation_cannot_recover_a_fragment() {
    let mine = adapter("unit", 1);
    let replacement = adapter("unit", 2);
    let wrapped = mine.wrap_commit_fragment(IcebergCommitFragment::data_file(
        IcebergDataFileArtifact::try_new(
            "s3://b/data/f.parquet".to_string(),
            IcebergFileFormat::Parquet,
            sample_partition(),
            sample_metrics(1, 64),
            None,
        )
        .expect("data artifact"),
    ));
    assert_eq!(
        replacement
            .commit_fragment(&wrapped)
            .expect_err("foreign generation")
            .kind(),
        ConnectorErrorKind::InvalidRequest
    );
}

// -------------------------------------------------------------------------
// Prepared-set validation and the single-commit latch.
// -------------------------------------------------------------------------

pub(super) fn fixed_delete_recipe(
    table: &IcebergWriteTableFacts,
) -> super::domain::IcebergMutationReadRecipe {
    super::domain::IcebergMutationReadRecipe::try_new(
        table,
        "s3://b/wh/db/t/metadata/v4.metadata.json".to_owned(),
        format!("sha256:{}", "a".repeat(64)),
        table.base_snapshot_id().map(|snapshot| {
            super::domain::IcebergMutationSnapshot::try_new(
                format!("s3://b/wh/db/t/metadata/snap-{snapshot}.avro"),
                table.schema_id(),
            )
            .unwrap()
        }),
    )
    .unwrap()
}

pub(in crate::commit) fn dv_session() -> (IcebergWriteCommitSession, IcebergWriteAdapter) {
    let (handle, _plans) = plan_write_session(
        IcebergWriteSessionId::new(),
        IcebergWriteSessionPlanInput {
            flavor: IcebergWriteFlavor::RowMutationDeletionVector,
            purpose: ConnectorWriteAdmissionPurpose::OrdinaryDml,
            table: table_facts(),
            base_version_digest: None,
            staged_metadata: None,
            data: data_branch_plan(),
            deletes: vec![delete_branch_plan(IcebergWriteBranch::DeletionVector, None)],
        },
    )
    .expect("session");
    let recipe = fixed_delete_recipe(handle.table());
    (handle.with_mutation_read(recipe), adapter("unit", 1))
}

fn ordinal(value: u32) -> WriteTargetOrdinal {
    WriteTargetOrdinal::try_new(value).expect("ordinal")
}

fn received(
    handle: &IcebergWriteCommitSession,
    adapter: &IcebergWriteAdapter,
    entries: Vec<(WriteTargetOrdinal, IcebergCommitFragment)>,
) -> IcebergReceiving {
    let mut received = IcebergReceiving::default();
    let query = write_query_identity();
    received
        .begin_query(
            query,
            &handle.expected_targets(),
            &mut None,
            handle.targets(),
        )
        .expect("register query");
    let mut fragments = entries
        .into_iter()
        .map(|(target, fragment)| (target, adapter.wrap_commit_fragment(fragment)))
        .collect();
    received.arrive(query, &mut fragments).expect("arrive");
    received.confirm(query).expect("confirm");
    received
}

fn dv_fragment(path: &str, referenced: &str, merged: Vec<EntryIdentity>) -> IcebergCommitFragment {
    IcebergCommitFragment::deletion_vector(
        crate::commit::write_stack::domain::IcebergDeletionVectorArtifact::try_new(
            path.to_string(),
            sample_partition(),
            sample_metrics(3, 1024),
            referenced.to_string(),
            crate::commit::write_stack::domain::IcebergContentRange::try_new(4, 64).expect("range"),
            3,
            merged,
        )
        .expect("deletion vector artifact"),
    )
}

pub(super) fn final_vector(
    vector: super::domain::IcebergDeletionVectorArtifact,
) -> IcebergCommitFragment {
    let target = super::domain::IcebergDeleteTargetLocator::try_new(
        "s3://b/wh/db/t/metadata/snap-77.avro".to_owned(),
        "s3://b/wh/db/t/metadata/manifest.avro".to_owned(),
        0,
        0,
        77,
        vector.referenced_data_file().to_owned(),
        100,
    )
    .unwrap();
    IcebergCommitFragment::final_delete(
        super::domain::IcebergFinalDeleteEffect::try_new(
            target,
            1,
            super::domain::IcebergFinalDeleteKind::ReplacePositions(Box::new(vector)),
        )
        .unwrap(),
    )
}

pub(in crate::commit) fn fixed_dv_fragment(
    path: &str,
    referenced: &str,
    merged: Vec<EntryIdentity>,
) -> IcebergCommitFragment {
    let vector = super::domain::IcebergDeletionVectorArtifact::try_new(
        path.to_owned(),
        sample_partition(),
        sample_metrics(3, 1024),
        referenced.to_owned(),
        super::domain::IcebergContentRange::try_new(4, 64).unwrap(),
        3,
        merged,
    )
    .unwrap();
    final_vector(vector)
}

pub(in crate::commit) fn data_fragment(path: &str) -> IcebergCommitFragment {
    IcebergCommitFragment::data_file(
        IcebergDataFileArtifact::try_new(
            path.to_string(),
            IcebergFileFormat::Parquet,
            sample_partition(),
            sample_metrics(5, 4096),
            None,
        )
        .expect("data artifact"),
    )
}

#[test]
fn a_zero_row_write_still_produces_a_valid_complete_prepared_set() {
    // A statement that matched nothing still reaches finish_write. Complete
    // empty input is valid; its exact row-delta author selects a NoOp rather
    // than confusing an absent action with an absent result.
    let (handle, adapter) = dv_session();
    let empty = received(&handle, &adapter, Vec::new());
    let validated =
        validate_received_set(&handle, &adapter, &empty).expect("an empty complete set is legal");
    assert!(validated.is_empty());
    assert_eq!(empty.fragments().count(), 0);
}

#[test]
fn a_valid_prepared_set_passes_every_sealed_check() {
    let (handle, adapter) = dv_session();
    let set = received(
        &handle,
        &adapter,
        vec![
            (ordinal(0), data_fragment("s3://b/wh/db/t/data/new.parquet")),
            (
                ordinal(1),
                fixed_dv_fragment(
                    "s3://b/wh/db/t/data/v.puffin",
                    "s3://b/wh/db/t/data/a.parquet",
                    vec![EntryIdentity::DeleteFile {
                        path: "s3://b/wh/db/t/data/old.parquet".to_string(),
                    }],
                ),
            ),
        ],
    );
    let validated = validate_received_set(&handle, &adapter, &set).expect("valid set");
    assert_eq!(validated.len(), 2);
}

#[test]
fn a_fragment_whose_branch_disagrees_with_its_target_is_rejected() {
    let (handle, adapter) = dv_session();
    let set = received(
        &handle,
        &adapter,
        vec![(
            ordinal(0),
            fixed_dv_fragment(
                "s3://b/wh/db/t/data/v.puffin",
                "s3://b/wh/db/t/data/a.parquet",
                Vec::new(),
            ),
        )],
    );
    let error = validate_received_set(&handle, &adapter, &set).expect_err("branch disagreement");
    assert!(
        error.message().contains("drives the data branch"),
        "{}",
        error.message()
    );
}

#[test]
fn two_delete_artifacts_for_one_data_file_are_rejected() {
    let (handle, adapter) = dv_session();
    let set = received(
        &handle,
        &adapter,
        vec![
            (
                ordinal(1),
                fixed_dv_fragment(
                    "s3://b/wh/db/t/data/v1.puffin",
                    "s3://b/wh/db/t/data/a.parquet",
                    Vec::new(),
                ),
            ),
            (
                ordinal(1),
                fixed_dv_fragment(
                    "s3://b/wh/db/t/data/v2.puffin",
                    "s3://b/wh/db/t/data/a.parquet",
                    Vec::new(),
                ),
            ),
        ],
    );
    let error = validate_received_set(&handle, &adapter, &set).expect_err("duplicate delete");
    assert!(
        error.message().contains("more than one delete artifact"),
        "{}",
        error.message()
    );
}

#[test]
fn a_position_rewrite_artifact_for_an_unselected_data_file_is_rejected() {
    let plan = plan_distributed_rewrite_branches(
        &session_material(delete_input_shape(IcebergWriteBranch::DeletionVector)),
        POSITION_DELETE_REWRITE,
        &position_delete_rewrite_groups(&["a"]),
    )
    .unwrap();
    let (handle, _) = flavor_session(plan);
    let adapter = adapter("unit", 1);
    let set = received(
        &handle,
        &adapter,
        vec![(
            ordinal(0),
            dv_fragment(
                "s3://b/wh/db/t/data/v.puffin",
                "s3://b/wh/db/t/data/never-selected.parquet",
                Vec::new(),
            ),
        )],
    );
    let error = validate_received_set(&handle, &adapter, &set).expect_err("unselected data file");
    assert!(
        error
            .message()
            .contains("differs from its selected data file")
    );
}

#[test]
fn a_repeated_staged_path_is_rejected() {
    let (handle, adapter) = dv_session();
    let set = received(
        &handle,
        &adapter,
        vec![
            (
                ordinal(0),
                data_fragment("s3://b/wh/db/t/data/same.parquet"),
            ),
            (
                ordinal(0),
                data_fragment("s3://b/wh/db/t/data/same.parquet"),
            ),
        ],
    );
    let error = validate_received_set(&handle, &adapter, &set).expect_err("duplicate path");
    assert!(
        error.message().contains("repeats staged artifact"),
        "{}",
        error.message()
    );
}

#[test]
fn a_prepared_set_accepts_distinct_vectors_in_the_same_puffin_object() {
    let paths = [
        "s3://b/wh/db/t/data/a.parquet",
        "s3://b/wh/db/t/data/b.parquet",
    ];
    let (handle, adapter) = dv_session();
    let fragments = paths
        .iter()
        .enumerate()
        .map(|(index, path)| {
            (
                ordinal(1),
                final_vector(
                    dv_artifact(
                        "s3://b/wh/db/t/data/shared.puffin",
                        path,
                        3,
                        1024,
                        4 + index as i64 * 128,
                        64,
                    )
                    .unwrap(),
                ),
            )
        })
        .collect();
    let set = received(&handle, &adapter, fragments);
    let validated = validate_received_set(&handle, &adapter, &set).unwrap();
    assert_eq!(validated.len(), 2);
    let mut fragments = set.fragments();
    let first = adapter
        .commit_fragment(fragments.next().unwrap().fragment)
        .unwrap();
    let second = adapter
        .commit_fragment(fragments.next().unwrap().fragment)
        .unwrap();
    assert_eq!(first.path(), second.path());
    assert_ne!(first.entry_identity(), second.entry_identity());
}

#[test]
fn a_session_dispatches_at_most_one_snapshot_commit() {
    let (handle, _adapter) = dv_session();
    handle.begin_commit().expect("first commit attempt");
    let second = handle
        .begin_commit()
        .expect_err("a second external commit must never be dispatched");
    assert!(second.message().contains("already has a commit in flight"));

    handle
        .settle(IcebergWriteSessionState::KnownCommitted {
            needed: std::sync::Arc::new(
                novarocks_spi::connector::write_stack::ConnectorWriteNeededReady::new(
                    ExternalMutationEffect::Applied,
                    crate::write_codec::connector_write_receipt_with_partitioning(42, None, None)
                        .expect("actual fixture receipt"),
                    novarocks_spi::connector::ExternalMutationFinalization::Complete,
                ),
            ),
        })
        .expect("settle committed");
    assert!(
        handle.begin_commit().is_err(),
        "a settled session cannot start another commit"
    );
}

#[test]
fn a_commit_unknown_session_stays_unknown_through_release() {
    let (handle, _adapter) = dv_session();
    let checked_evidence = novarocks_spi::connector::ExternalMutationEvidence::try_new(
        crate::commit::write_stack::control::ICEBERG_WRITE_SESSION_EVIDENCE_VERSION,
        descriptor("unit"),
        ProviderBindingEpoch::from_bytes([9; 16]),
        novarocks_spi::connector::ConnectorMutationOperationId::from_bytes(
            handle.session_id().to_bytes(),
        ),
        crate::commit::write_stack::control::ICEBERG_WRITE_SESSION_OPERATION_KIND,
        bytes::Bytes::from_static(b"complete checked ledger with retained attempt objects"),
    )
    .unwrap();
    handle
        .retain_recovery_evidence(checked_evidence.clone())
        .unwrap();
    handle.begin_commit().expect("commit attempt");
    handle
        .settle(IcebergWriteSessionState::CommitUnknown {
            message: "connection reset by peer".to_string(),
            staging_dir: handle.staging_dir(),
        })
        .expect("settle unknown");

    let outcome = release_session_state(
        &descriptor("unit"),
        ProviderBindingEpoch::from_bytes([9; 16]),
        &handle,
    )
    .expect("release");
    match outcome {
        ConnectorWriteAbortOutcome::CommitUnknown { failure, evidence } => {
            assert_eq!(
                evidence, checked_evidence,
                "release must reuse exact preflight bytes"
            );
            assert!(failure.message().contains("connection reset by peer"));
            assert!(failure.message().contains("staged files remain at"));
            assert_eq!(
                evidence.operation_kind(),
                crate::commit::write_stack::control::ICEBERG_WRITE_SESSION_OPERATION_KIND
            );
            assert_eq!(
                evidence.operation_id().to_bytes(),
                handle.session_id().to_bytes()
            );
        }
        other => panic!("abort must not resolve an unknown outcome: {other:?}"),
    }
}

#[test]
fn a_known_committed_session_cannot_be_aborted_into_uncommitted() {
    let (handle, _adapter) = dv_session();
    handle.begin_commit().expect("commit attempt");
    handle
        .settle(IcebergWriteSessionState::KnownCommitted {
            needed: std::sync::Arc::new(
                novarocks_spi::connector::write_stack::ConnectorWriteNeededReady::new(
                    ExternalMutationEffect::Applied,
                    crate::write_codec::connector_write_receipt_with_partitioning(7, None, None)
                        .expect("actual fixture receipt"),
                    novarocks_spi::connector::ExternalMutationFinalization::Complete,
                ),
            ),
        })
        .expect("settle committed");
    let outcome = release_session_state(
        &descriptor("unit"),
        ProviderBindingEpoch::from_bytes([9; 16]),
        &handle,
    )
    .expect("release");
    assert!(matches!(
        outcome,
        ConnectorWriteAbortOutcome::KnownCommitted { .. }
    ));
}

#[test]
fn a_release_cannot_race_an_in_flight_commit() {
    let (handle, _adapter) = dv_session();
    handle.begin_commit().expect("commit attempt");
    let error = release_session_state(
        &descriptor("unit"),
        ProviderBindingEpoch::from_bytes([9; 16]),
        &handle,
    )
    .expect_err("release during commit");
    assert_eq!(error.kind(), ConnectorErrorKind::Unavailable);
}

#[test]
fn every_flavor_maps_onto_a_dense_logical_target_map() {
    let mut seen: BTreeMap<IcebergWriteFlavor, usize> = BTreeMap::new();
    for flavor in [
        IcebergWriteFlavor::Append,
        IcebergWriteFlavor::Overwrite,
        IcebergWriteFlavor::PartitionOverwrite,
        IcebergWriteFlavor::StagedCreate,
        IcebergWriteFlavor::ManagedPublication,
        IcebergWriteFlavor::DistributedRewrite,
        IcebergWriteFlavor::TableMaintenance,
    ] {
        let (handle, plans) = plan_write_session(
            IcebergWriteSessionId::new(),
            IcebergWriteSessionPlanInput {
                flavor,
                purpose: ConnectorWriteAdmissionPurpose::OrdinaryDml,
                table: table_facts(),
                base_version_digest: None,
                // Exactly the staged flavor carries frozen metadata; every
                // other one loads its target instead.
                staged_metadata: (flavor == IcebergWriteFlavor::StagedCreate)
                    .then(crate::commit::write_stack::test_support::staged_table_metadata),
                data: data_branch_plan(),
                deletes: Vec::new(),
            },
        )
        .unwrap_or_else(|error| panic!("{} must plan: {error:?}", flavor.as_str()));
        assert_eq!(plans.len(), 1);
        assert_eq!(handle.expected_targets(), vec![ordinal(0)]);
        seen.insert(flavor, plans.len());
    }
    for (flavor, branch) in [
        (
            IcebergWriteFlavor::RowMutationPositionDelete,
            IcebergWriteBranch::PositionDelete,
        ),
        (
            IcebergWriteFlavor::RowMutationDeletionVector,
            IcebergWriteBranch::DeletionVector,
        ),
    ] {
        let (handle, plans) = plan_write_session(
            IcebergWriteSessionId::new(),
            IcebergWriteSessionPlanInput {
                flavor,
                purpose: ConnectorWriteAdmissionPurpose::OrdinaryDml,
                table: table_facts(),
                base_version_digest: None,
                staged_metadata: None,
                data: data_branch_plan(),
                deletes: vec![delete_branch_plan(branch, None)],
            },
        )
        .unwrap_or_else(|error| panic!("{} must plan: {error:?}", flavor.as_str()));
        assert_eq!(plans.len(), 2);
        assert_eq!(handle.branch_of(ordinal(1)), Some(branch));
        seen.insert(flavor, plans.len());
    }
    assert_eq!(seen.len(), 9, "every flavor must be covered");
}

// -------------------------------------------------------------------------
// Session flavors: how each one plans its logical branches.
// -------------------------------------------------------------------------

fn flavor_session(
    plan: IcebergSessionFlavorPlan,
) -> (IcebergWriteCommitSession, Vec<IcebergWriteTargetPlan>) {
    plan_branch_session(
        IcebergWriteSessionId::new(),
        IcebergBranchSessionPlanInput {
            flavor: plan.flavor,
            purpose: ConnectorWriteAdmissionPurpose::OrdinaryDml,
            table: table_facts(),
            base_version_digest: None,
            document_publication: plan.document_publication,
            staged_metadata: None,
            rewrite_inputs: plan.rewrite_inputs,
            repartition: None,
            writer_table: None,
            branches: plan.branches,
        },
    )
    .expect("seal the flavor's branches")
}

/// Take the neutral session plan the frontend actually receives, so a route the
/// provider attached is asserted where SQL would read it.
fn neutral_plan(
    adapter: &IcebergWriteAdapter,
    sealed: (IcebergWriteCommitSession, Vec<IcebergWriteTargetPlan>),
) -> Result<ConnectorWriteSessionPlan, ConnectorError> {
    let (handle, targets) = sealed;
    session_plan_from_targets(adapter, handle, targets, None, None)
}

fn effects(target: &ConnectorWriteTargetPlan) -> Vec<ConnectorRowMutationEffect> {
    target
        .route()
        .expect("branch is routed")
        .accepted_effects()
        .to_vec()
}

#[test]
fn an_ordinary_session_yields_unrouted_targets() {
    // An ordinary write has exactly one thing to do with every row it is
    // given, so nothing needs routing, and the branch structure is the one this
    // stack has always sealed.
    let adapter = adapter("ordinary", 1);
    let plan = plan_ordinary_branches(
        IcebergWriteFlavor::Append,
        &session_material(data_input_shape()),
    )
    .expect("plan an ordinary append");
    let sealed = neutral_plan(&adapter, flavor_session(plan)).expect("neutral plan");
    assert_eq!(sealed.targets().len(), 1);
    assert!(
        sealed
            .targets()
            .iter()
            .all(|target| target.route().is_none())
    );

    let material = session_material(delete_input_shape(IcebergWriteBranch::DeletionVector));
    assert!(
        plan_ordinary_branches(IcebergWriteFlavor::RowMutationDeletionVector, &material).is_err()
    );
}

#[test]
fn partitioned_ordinary_data_target_declares_affinity_and_refuses_unknown_transforms() {
    use novarocks_spi::connector::write_stack::ConnectorWriteLayoutPreference;

    let adapter = adapter("partitioned_ordinary", 1);
    let mut material = session_material(data_input_shape());
    material.write_binding = adapter.binding().clone();
    // Previewed managed repartition supplies writer-side spec identity here.
    material.write_partition_spec_id = 7;
    let schema = material
        .data_recipe
        .target_schema_json()
        .unwrap()
        .to_owned();
    let recipe = |transform: &str| {
        super::domain::IcebergDataBranchRecipe::try_new(
            Some(schema.clone()),
            vec!["k1".into()],
            vec!["p".into()],
            vec![transform.into()],
            false,
        )
        .unwrap()
    };
    material.data_recipe = recipe("identity");
    let plan = plan_ordinary_branches(IcebergWriteFlavor::Append, &material).unwrap();
    let sealed = neutral_plan(&adapter, flavor_session(plan)).unwrap();
    let target = &sealed.targets()[0];
    let token = target.input().fields()[0].token();
    let ConnectorWriteLayoutPreference::PartitionAffinity {
        inputs,
        recipe: layout,
    } = &target.route().unwrap().requirements().layout
    else {
        panic!("partitioned data target must declare affinity");
    };
    assert_eq!(inputs, &[token]);
    assert_eq!(layout.binding(), adapter.binding());
    let (_, spec) = crate::commit::partition_key::layout_facts(layout).unwrap();
    assert_eq!(spec.spec_id(), 7);

    material.data_recipe = recipe("unknown");
    let error = plan_ordinary_branches(IcebergWriteFlavor::Append, &material).unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
}

#[test]
fn a_row_mutation_yields_one_routed_target_per_branch() {
    // A merge-on-read mutation carries both halves of a change event in one
    // row: the delete branch consumes the before-image identity and the data
    // branch consumes the after-image values, so a Replace reaches both.
    let adapter = adapter("row_mutation", 2);
    let material = session_material(merge_on_read_input_shape());
    let plan = plan_row_mutation_branches(&material).expect("plan a merge-on-read mutation");
    assert_eq!(plan.flavor, IcebergWriteFlavor::RowMutationDeletionVector);
    let (handle, targets) = flavor_session(plan);
    assert_eq!(handle.branch_of(ordinal(0)), Some(IcebergWriteBranch::Data));
    assert_eq!(
        handle.branch_of(ordinal(1)),
        Some(IcebergWriteBranch::DeletionVector)
    );

    let sealed = neutral_plan(&adapter, (handle, targets)).expect("neutral plan");
    assert_eq!(sealed.expected_targets(), vec![ordinal(0), ordinal(1)]);
    assert!(
        sealed
            .targets()
            .iter()
            .all(|target| target.route().is_some())
    );
    assert_eq!(
        effects(&sealed.targets()[0]),
        vec![
            ConnectorRowMutationEffect::Replace,
            ConnectorRowMutationEffect::Insert
        ]
    );
    assert_eq!(
        effects(&sealed.targets()[1]),
        vec![
            ConnectorRowMutationEffect::Delete,
            ConnectorRowMutationEffect::Replace
        ]
    );
    // Two branches cannot share a route key, and the ordinals are dense.
    assert_ne!(
        sealed.targets()[0].route().expect("routed").route_id(),
        sealed.targets()[1].route().expect("routed").route_id()
    );
    // Each branch reads its own columns out of the one input row: the data
    // branch the two after-image values, the delete branch the two identity
    // columns that follow them.
    let positions = |target: &ConnectorWriteTargetPlan| {
        target
            .route()
            .expect("routed")
            .input_ordinals()
            .iter()
            .map(ConnectorMutationRouteInput::input_ordinal)
            .collect::<Vec<_>>()
    };
    assert_eq!(positions(&sealed.targets()[0]), vec![0, 1]);
    assert_eq!(positions(&sealed.targets()[1]), vec![2, 3]);
    assert_eq!(
        sealed.targets()[0]
            .route()
            .unwrap()
            .requirements()
            .colocation,
        novarocks_spi::connector::write_stack::ConnectorWriteColocation::None,
        "an after-image route has no old-file ownership requirement"
    );
}

#[test]
fn a_delete_only_row_mutation_seals_one_routed_delete_branch() {
    // A deletion-vector input carries no after-image half, so the mutation
    // needs exactly one branch, and that branch accepts only a delete. Its
    // partition source fields travel as routing facts because SQL, not the
    // writer, supplies them.
    let adapter = adapter("delete_only", 3);
    let material = session_material(ConnectorWriteInputShape::DeletionVector {
        identity_fields: vec![
            binding("_file", 2, DataType::Utf8),
            binding("_pos", 3, DataType::Int64),
        ],
        partition_source_fields: vec![binding("k1", 1, DataType::Int64)],
    });
    let plan = plan_row_mutation_branches(&material).expect("plan a delete-only mutation");
    let sealed = neutral_plan(&adapter, flavor_session(plan)).expect("neutral plan");
    assert_eq!(sealed.expected_targets(), vec![ordinal(0)]);
    assert_eq!(
        effects(&sealed.targets()[0]),
        vec![ConnectorRowMutationEffect::Delete]
    );
    let route = sealed.targets()[0].route().expect("routed");
    let file_token = route.input_ordinals()[0].token();
    assert_eq!(
        route.requirements().colocation,
        novarocks_spi::connector::write_stack::ConnectorWriteColocation::ByKey(vec![file_token])
    );
    let ConnectorWriteInputShape::DeletionVector {
        partition_source_fields,
        ..
    } = &material.input
    else {
        unreachable!()
    };
    assert_eq!(
        route.requirements().layout,
        novarocks_spi::connector::write_stack::ConnectorWriteLayoutPreference::ClusterBy(
            partition_source_fields
                .iter()
                .map(|field| field.token())
                .collect()
        ),
        "new partition values are a layout preference independent of old-file ownership"
    );
}

#[test]
fn a_row_mutation_whose_branches_share_a_route_key_is_refused() {
    // Two branches sharing a route key would make the router's choice
    // ambiguous, and the loser's rows would vanish. The provider's own route
    // key is derived per branch and never collides, so the refusal is proven by
    // sealing a mutation whose two branches were given the same key.
    let adapter = adapter("route_collision", 5);
    let collided = |input: &ConnectorWriteInputShape| {
        ConnectorWriteRouteFacts::try_new(
            ConnectorWriteRouteId::from_bytes([9; 32]),
            vec![ConnectorRowMutationEffect::Delete],
            input
                .fields()
                .into_iter()
                .enumerate()
                .map(|(ordinal, field)| {
                    ConnectorMutationRouteInput::new(
                        field.token(),
                        u32::try_from(ordinal).expect("bounded test input"),
                    )
                })
                .collect(),
            novarocks_spi::connector::write_stack::ConnectorWriteRouteRequirements {
                colocation: novarocks_spi::connector::write_stack::ConnectorWriteColocation::None,
                layout: novarocks_spi::connector::write_stack::ConnectorWriteLayoutPreference::None,
            },
        )
        .expect("route facts")
    };
    let data_route = collided(&data_input_shape());
    let delete_route = collided(&delete_input_shape(IcebergWriteBranch::DeletionVector));
    let sealed = plan_branch_session(
        IcebergWriteSessionId::new(),
        IcebergBranchSessionPlanInput {
            flavor: IcebergWriteFlavor::RowMutationDeletionVector,
            purpose: ConnectorWriteAdmissionPurpose::OrdinaryDml,
            table: table_facts(),
            base_version_digest: None,
            document_publication: None,
            staged_metadata: None,
            rewrite_inputs: Vec::new(),
            repartition: None,
            writer_table: None,
            branches: vec![
                IcebergWriteBranchPlan::Data {
                    plan: data_branch_plan(),
                    route: Some(data_route),
                },
                IcebergWriteBranchPlan::Delete {
                    plan: delete_branch_plan(IcebergWriteBranch::DeletionVector, None),
                    route: Some(delete_route),
                },
            ],
        },
    )
    .expect("seal two branches");
    let error = neutral_plan(&adapter, sealed).expect_err("duplicate route key");
    assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    assert!(
        error.message().contains("repeats a row-mutation route key"),
        "unexpected message: {}",
        error.message()
    );
}

/// The frozen data-file rewrite shape, whose selection the provider re-cuts
/// from the target alone.
const DATA_FILE_REWRITE: ConnectorDistributedRewriteShape =
    ConnectorDistributedRewriteShape::DataFiles { rewrite_all: true };

/// The frozen position-delete rewrite shape. Its selection facts travel with
/// it because the group set depends on them.
const POSITION_DELETE_REWRITE: ConnectorDistributedRewriteShape =
    ConnectorDistributedRewriteShape::PositionDeletes {
        rewrite_all: true,
        min_input_files: None,
    };

/// One live data file, with whatever delete artifacts are attached to it.
fn rewrite_data_file(
    partition: &str,
    delete_files: Vec<crate::scan_model::IcebergDeleteFileInfo>,
) -> DataFileWithStats {
    DataFileWithStats {
        path: format!("s3://b/wh/db/t/data/{partition}/f.parquet"),
        size: 1,
        record_count: Some(1),
        column_stats: None,
        partition_spec_id: Some(0),
        partition_key: Some(partition.to_string()),
        partition_values: None,
        manifest_path: None,
        partition_field_values: Vec::new(),
        first_row_id: None,
        data_sequence_number: Some(4),
        delete_files,
    }
}

/// One live Puffin deletion vector attached to `data_file`.
fn rewrite_deletion_vector(
    path: &str,
    data_file: &str,
) -> crate::scan_model::IcebergDeleteFileInfo {
    crate::scan_model::IcebergDeleteFileInfo {
        record_count: Some(1),
        partition_data_json: Some(r#"{"version":1,"values":[]}"#.to_string()),
        path: path.to_string(),
        file_format: crate::scan_model::IcebergDeleteFileFormat::Puffin,
        file_content: crate::scan_model::IcebergDeleteFileContent::Position,
        length: Some(1024),
        content_offset: Some(4),
        content_size_in_bytes: Some(32),
        sequence_number: Some(7),
        partition_spec_id: Some(0),
        partition_key: None,
        referenced_data_file: Some(data_file.to_string()),
        equality_column_names: Vec::new(),
        equality_field_ids: Vec::new(),
    }
}

/// Attach the frozen branch facts a data rewrite carries: none, because a data
/// branch owns no delete artifact.
fn data_rewrite_branches(
    groups: Vec<crate::distributed_rewrite::IcebergFrozenRewriteGroupV1>,
) -> Vec<IcebergFrozenRewriteBranch> {
    groups
        .into_iter()
        .map(|group| IcebergFrozenRewriteBranch {
            group,
            position_rewrite: None,
        })
        .collect()
}

/// Cut real rewrite groups, one per partition key, through the rewrite
/// planner the rewrite path already uses.
fn rewrite_groups(partitions: &[&str]) -> Vec<IcebergFrozenRewriteBranch> {
    let files = partitions
        .iter()
        .map(|partition| rewrite_data_file(partition, Vec::new()))
        .collect::<Vec<_>>();
    data_rewrite_branches(
        crate::distributed_rewrite::plan_data_file_groups(
            files,
            &std::collections::BTreeSet::new(),
        )
        .expect("plan rewrite groups"),
    )
}

/// Cut real position-delete rewrite groups, one per data file that carries
/// deletion vectors, and freeze each branch's own merge target the way
/// `begin_write` does: owning its group's data file with no old reference to
/// merge.
fn position_delete_rewrite_groups(partitions: &[&str]) -> Vec<IcebergFrozenRewriteBranch> {
    let files = partitions
        .iter()
        .map(|partition| {
            let path = format!("s3://b/wh/db/t/data/{partition}/f.parquet");
            rewrite_data_file(
                partition,
                vec![
                    rewrite_deletion_vector(
                        &format!("s3://b/wh/db/t/data/{partition}/d0.puffin"),
                        &path,
                    ),
                    rewrite_deletion_vector(
                        &format!("s3://b/wh/db/t/data/{partition}/d1.puffin"),
                        &path,
                    ),
                ],
            )
        })
        .collect::<Vec<_>>();
    crate::distributed_rewrite::plan_position_delete_groups(files, true, None)
        .expect("plan position-delete rewrite groups")
        .into_iter()
        .map(|group| {
            let position_rewrite = Some(position_rewrite_target(&group.data_files[0].path, 1));
            IcebergFrozenRewriteBranch {
                group,
                position_rewrite,
            }
        })
        .collect()
}

#[test]
fn a_distributed_rewrite_yields_one_target_per_rewrite_group() {
    let adapter = adapter("rewrite", 6);
    let groups = rewrite_groups(&["a", "b", "c"]);
    assert_eq!(groups.len(), 3);
    let plan = plan_distributed_rewrite_branches(
        &session_material(data_input_shape()),
        DATA_FILE_REWRITE,
        &groups,
    )
    .expect("plan a distributed rewrite");
    assert_eq!(plan.flavor, IcebergWriteFlavor::DistributedRewrite);
    let (handle, targets) = flavor_session(plan);
    assert_eq!(
        handle.expected_targets(),
        vec![ordinal(0), ordinal(1), ordinal(2)]
    );
    assert!(
        handle
            .targets()
            .iter()
            .all(|target| target.branch() == IcebergWriteBranch::Data)
    );
    let sealed = neutral_plan(&adapter, (handle, targets)).expect("neutral plan");
    assert_eq!(sealed.targets().len(), 3);
    // A rewrite routes nothing: it does not split rows by change event.
    assert!(
        sealed
            .targets()
            .iter()
            .all(|target| target.route().is_none())
    );
}

#[test]
fn a_distributed_rewrite_commits_the_exact_file_set_it_froze() {
    // The rewrite commit replaces files it named at planning time. Nothing in
    // the prepared write set can say which those are -- a fragment describes
    // what a writer produced, and a group whose rows were all compacted away
    // produces nothing at all while still having to be retired.
    //
    // The frozen publication runner rejects a selected rewrite without its exact file set
    // when its frozen file set is absent, so before the session carried its
    // frozen groups every rewrite commit failed with "requires its frozen file
    // set". This asserts the session now supplies exactly the union it froze.
    let live_deletes = std::collections::BTreeSet::from([
        EntryIdentity::DeleteFile {
            path: "s3://b/wh/db/t/data/d0.parquet".to_string(),
        },
        EntryIdentity::DeleteFile {
            path: "s3://b/wh/db/t/data/d1.parquet".to_string(),
        },
    ]);
    let groups = data_rewrite_branches(
        crate::distributed_rewrite::plan_data_file_groups(
            ["a", "b"]
                .iter()
                .map(|partition| rewrite_data_file(partition, Vec::new()))
                .collect(),
            &live_deletes,
        )
        .expect("plan rewrite groups"),
    );
    let plan = plan_distributed_rewrite_branches(
        &session_material(data_input_shape()),
        DATA_FILE_REWRITE,
        &groups,
    )
    .expect("plan a distributed rewrite");
    let (handle, _) = flavor_session(plan);

    let files = crate::commit::write_stack::control::selected_rewrite_files(&handle)
        .expect("a rewrite session commits the file set it froze");
    files
        .validate()
        .expect("the frozen file set satisfies the rewrite action");
    assert_eq!(
        files.kind,
        crate::commit::selected_rewrite::SelectedRewriteKind::Data
    );
    assert_eq!(
        files.data_paths,
        std::collections::BTreeSet::from([
            EntryIdentity::DataFile {
                path: "s3://b/wh/db/t/data/a/f.parquet".to_string()
            },
            EntryIdentity::DataFile {
                path: "s3://b/wh/db/t/data/b/f.parquet".to_string()
            },
        ])
    );
    // Every live delete artifact is retired with the data it applied to: the
    // rewritten files no longer contain the rows it removed.
    assert_eq!(files.delete_paths, live_deletes);
}

/// A position-delete rewrite repacks delete artifacts, so it seals a deletion
/// vector branch per frozen group -- never the data branch a data rewrite
/// seals. Sealing a data branch here would give SQL somewhere to send table
/// rows the operation never reads, and its commit would then republish data
/// files nothing rewrote.
#[test]
fn a_position_delete_rewrite_seals_one_delete_branch_per_frozen_group() {
    let adapter = adapter("rewrite-position-deletes", 6);
    let original = position_delete_rewrite_groups(&["a", "b"]);
    let mut groups = original.clone();
    let operation = (1u8..=255)
        .map(|n| novarocks_spi::connector::ConnectorWriteOperationId::from_bytes([n; 16]))
        .find(|id| {
            let cohort = |g: &IcebergFrozenRewriteBranch| {
                novarocks_spi::connector::ConnectorWriteCohortId::derive(
                    *id,
                    b"iceberg-distributed-rewrite-group",
                    crate::distributed_rewrite::decode_digest(&g.group.group_digest_hex).unwrap(),
                )
                .unwrap()
            };
            cohort(&original[0]) > cohort(&original[1])
        })
        .expect("cohort order must differ from group-digest order for this fixture");
    groups.sort_by_key(|g| {
        novarocks_spi::connector::ConnectorWriteCohortId::derive(
            operation,
            b"iceberg-distributed-rewrite-group",
            crate::distributed_rewrite::decode_digest(&g.group.group_digest_hex).unwrap(),
        )
        .unwrap()
    });
    assert_ne!(
        groups[0].group.group_digest_hex,
        original[0].group.group_digest_hex
    );
    assert_eq!(groups.len(), 2);
    let plan = plan_distributed_rewrite_branches(
        &session_material(delete_input_shape(IcebergWriteBranch::DeletionVector)),
        POSITION_DELETE_REWRITE,
        &groups,
    )
    .expect("plan a position-delete rewrite");
    assert_eq!(
        plan.flavor,
        IcebergWriteFlavor::DistributedRewritePositionDeletes
    );
    let (handle, targets) = flavor_session(plan);
    assert_eq!(handle.expected_targets(), vec![ordinal(0), ordinal(1)]);
    assert!(
        handle
            .targets()
            .iter()
            .all(|target| target.branch() == IcebergWriteBranch::DeletionVector)
    );
    for (index, branch) in groups.iter().enumerate() {
        let expected = branch.position_rewrite.as_ref().unwrap();
        assert_eq!(handle.targets()[index].position_rewrite(), Some(expected));
        assert_eq!(targets[index].handle().position_rewrite(), Some(expected));
        assert_eq!(expected.path(), branch.group.data_files[0].path);
    }
    let sealed = neutral_plan(&adapter, (handle, targets)).expect("neutral plan");
    assert_eq!(sealed.targets().len(), 2);
    assert!(
        sealed
            .targets()
            .iter()
            .all(|target| target.route().is_none())
    );
}

/// The commit replaces the half of the frozen set its operation names. A data
/// rewrite retires the group's data files plus the deletes they owned; a
/// position-delete rewrite retires only the vectors its groups selected, and
/// names their data files so the replacements can be proven to cover them.
#[test]
fn a_position_delete_rewrite_commits_the_delete_artifacts_it_froze() {
    let plan = plan_distributed_rewrite_branches(
        &session_material(delete_input_shape(IcebergWriteBranch::DeletionVector)),
        POSITION_DELETE_REWRITE,
        &position_delete_rewrite_groups(&["a"]),
    )
    .expect("plan a position-delete rewrite");
    let (handle, _) = flavor_session(plan);
    assert_eq!(handle.commit_op_kind(), CommitOpKind::SelectedRewrite);

    let files = crate::commit::write_stack::control::selected_rewrite_files(&handle)
        .expect("a rewrite session commits the file set it froze");
    files
        .validate()
        .expect("the frozen file set satisfies the rewrite action");
    assert_eq!(
        files.kind,
        crate::commit::selected_rewrite::SelectedRewriteKind::PositionDeletes
    );
    assert_eq!(
        files.delete_paths,
        std::collections::BTreeSet::from([
            crate::distributed_rewrite::delete_entry_identity(&rewrite_deletion_vector(
                "s3://b/wh/db/t/data/a/d0.puffin",
                "s3://b/wh/db/t/data/a/f.parquet"
            ))
            .unwrap(),
            crate::distributed_rewrite::delete_entry_identity(&rewrite_deletion_vector(
                "s3://b/wh/db/t/data/a/d1.puffin",
                "s3://b/wh/db/t/data/a/f.parquet"
            ))
            .unwrap(),
        ])
    );
    assert_eq!(
        files.data_paths,
        std::collections::BTreeSet::from([EntryIdentity::DataFile {
            path: "s3://b/wh/db/t/data/a/f.parquet".to_string()
        }])
    );
}

/// Which branch a rewrite seals follows the frozen operation, not the writer
/// input. Reading it off the input would let a caller signing a delete shape
/// turn a data rewrite into a delete rewrite -- and its commit would then
/// retire the wrong half of the frozen set -- so each shape refuses the
/// other's input outright.
#[test]
fn a_rewrite_refuses_the_input_its_frozen_operation_does_not_write() {
    let delete_input_to_data_rewrite = plan_distributed_rewrite_branches(
        &session_material(delete_input_shape(IcebergWriteBranch::DeletionVector)),
        DATA_FILE_REWRITE,
        &rewrite_groups(&["a"]),
    )
    .expect_err("a data rewrite writes data files");
    assert_eq!(
        delete_input_to_data_rewrite.kind(),
        ConnectorErrorKind::Unsupported
    );
    assert!(
        delete_input_to_data_rewrite
            .message()
            .contains("not a row-level delete input"),
        "unexpected message: {}",
        delete_input_to_data_rewrite.message()
    );

    let data_input_to_delete_rewrite = plan_distributed_rewrite_branches(
        &session_material(data_input_shape()),
        POSITION_DELETE_REWRITE,
        &position_delete_rewrite_groups(&["a"]),
    )
    .expect_err("a position-delete rewrite writes deletion vectors");
    assert_eq!(
        data_input_to_delete_rewrite.kind(),
        ConnectorErrorKind::Unsupported
    );
    assert!(
        data_input_to_delete_rewrite
            .message()
            .contains("not a data input"),
        "unexpected message: {}",
        data_input_to_delete_rewrite.message()
    );
}

#[test]
fn only_a_rewrite_session_carries_a_frozen_rewrite_file_set() {
    // A frozen input set retires live files. Handing one to a commit op that
    // does not replace files would delete data nothing superseded, so the
    // session seals it on exactly one flavor and offers it to exactly one
    // commit op.
    let (append, _) = flavor_session(
        plan_ordinary_branches(
            IcebergWriteFlavor::Append,
            &session_material(data_input_shape()),
        )
        .expect("plan an append"),
    );
    assert!(crate::commit::write_stack::control::selected_rewrite_files(&append).is_none());

    let error = IcebergWriteCommitSession::try_new_sealed(
        IcebergWriteSessionId::new(),
        table_facts(),
        IcebergWriteFlavor::Append,
        crate::commit::write_stack::domain::IcebergSessionFacts {
            purpose: ConnectorWriteAdmissionPurpose::OrdinaryDml,
            base_version_digest: None,
            document_publication: None,
            staged_metadata: None,
            rewrite_inputs: vec![
                crate::commit::write_stack::domain::IcebergFrozenRewriteBranchInput::try_new(
                    std::collections::BTreeSet::from([EntryIdentity::DataFile {
                        path: "s3://b/wh/db/t/data/a.parquet".to_string(),
                    }]),
                    std::collections::BTreeSet::new(),
                )
                .expect("frozen rewrite input"),
            ],
            repartition: None,
        },
        vec![
            crate::commit::write_stack::domain::IcebergSealedWriteTarget::new(
                ordinal(0),
                IcebergWriteBranch::Data,
                None,
            ),
        ],
    )
    .expect_err("an append cannot freeze a rewrite input");
    assert!(
        error
            .message()
            .contains("must carry a frozen rewrite input per branch"),
        "unexpected message: {}",
        error.message()
    );
}

#[test]
fn a_rewrite_is_not_gated_by_the_external_write_fence() {
    // The rewrite is arbitrated by the ordinary Iceberg base-state compare and
    // swap the frozen publication runner already performs against the frozen snapshot, so
    // it must not also take the distributed external write fence. Every other
    // flavor keeps it.
    for flavor in [
        IcebergWriteFlavor::Append,
        IcebergWriteFlavor::Overwrite,
        IcebergWriteFlavor::PartitionOverwrite,
        IcebergWriteFlavor::RowMutationPositionDelete,
        IcebergWriteFlavor::RowMutationDeletionVector,
        IcebergWriteFlavor::StagedCreate,
        IcebergWriteFlavor::ManagedPublication,
        IcebergWriteFlavor::TableMaintenance,
    ] {
        assert!(
            flavor.requires_external_write_fence(),
            "{} must keep the fence",
            flavor.as_str()
        );
    }
    assert!(!IcebergWriteFlavor::DistributedRewrite.requires_external_write_fence());
    assert!(!IcebergWriteFlavor::DistributedRewritePositionDeletes.requires_external_write_fence());

    let plan = plan_distributed_rewrite_branches(
        &session_material(data_input_shape()),
        DATA_FILE_REWRITE,
        &rewrite_groups(&["a"]),
    )
    .expect("plan a distributed rewrite");
    let (handle, _) = flavor_session(plan);
    assert!(!handle.requires_external_write_fence());
    // The compare-and-swap input the commit dispatch checks the loaded table
    // against, and the rewrite commit action it dispatches.
    assert_eq!(handle.table().base_snapshot_id(), Some(77));
    assert_eq!(handle.commit_op_kind(), CommitOpKind::SelectedRewrite);
}

#[test]
fn a_managed_publication_carries_its_technique_and_disposition_into_finish() {
    // A full refresh republishes the whole target, so it must commit as a
    // replacement; an incremental one adds to what is live.
    for (technique, op_kind) in [
        (
            ConnectorManagedPublicationTechnique::Full,
            CommitOpKind::Overwrite,
        ),
        (
            ConnectorManagedPublicationTechnique::Incremental,
            CommitOpKind::FastAppend,
        ),
    ] {
        let plan = plan_document_publication_branches(
            &session_material(data_input_shape()),
            document_publication_facts(
                technique,
                ConnectorManagedPublicationEmptyInputDisposition::CommitEmptyWrite,
            ),
        )
        .expect("plan a managed publication");
        let (handle, _) = flavor_session(plan);
        let publication = handle
            .document_publication()
            .expect("document declaration reaches finish")
            .declaration();
        assert_eq!(publication.technique(), technique);
        assert_eq!(
            publication.empty_input(),
            ConnectorManagedPublicationEmptyInputDisposition::CommitEmptyWrite
        );
        assert_eq!(handle.commit_op_kind(), op_kind);
    }
}

#[test]
fn an_atomic_repartition_writes_under_the_prospective_spec_and_swaps_on_the_current_one() {
    // The two generations are deliberately different. A writer has to stage
    // files under the spec the commit is about to establish, because that spec
    // and the snapshot land together; the session's own compare-and-swap has to
    // match the generation the table still holds, because that is what the
    // catalog will be asked to move from. Collapsing them either stamps
    // artifacts with a spec id the table has never had or refuses every commit
    // as a stale generation.
    let session_table = table_facts();
    let prospective = IcebergWriteTableFacts::try_new(
        session_table.table_uuid().to_string(),
        session_table.namespace().to_string(),
        session_table.table_name().to_string(),
        session_table.table_location().to_string(),
        session_table.data_location().to_string(),
        session_table.target_ref().to_string(),
        session_table.base_snapshot_id(),
        session_table.base_sequence_number(),
        session_table.schema_id(),
        session_table.default_partition_spec_id() + 1,
        3,
    )
    .expect("prospective table facts");

    let (handle, plans) = plan_branch_session(
        IcebergWriteSessionId::new(),
        IcebergBranchSessionPlanInput {
            flavor: IcebergWriteFlavor::ManagedPublication,
            purpose: ConnectorWriteAdmissionPurpose::MaterializedViewRefresh,
            table: session_table.clone(),
            base_version_digest: None,
            document_publication: Some(document_publication_facts(
                ConnectorManagedPublicationTechnique::Full,
                ConnectorManagedPublicationEmptyInputDisposition::CommitEmptyWrite,
            )),
            staged_metadata: None,
            rewrite_inputs: Vec::new(),
            repartition: None,
            writer_table: Some(prospective.clone()),
            branches: vec![IcebergWriteBranchPlan::Data {
                plan: data_branch_plan(),
                route: None,
            }],
        },
    )
    .expect("seal a repartitioning publication");

    assert_eq!(
        plans[0].handle().table().default_partition_spec_id(),
        prospective.default_partition_spec_id(),
        "the writer stamps its artifacts with the spec the commit establishes"
    );
    assert_eq!(
        handle.table().default_partition_spec_id(),
        session_table.default_partition_spec_id(),
        "the session compares and swaps against the generation the table holds"
    );
}

#[test]
fn a_change_stream_publication_seals_the_branches_a_row_mutation_needs() {
    // An incremental refresh applies a change stream, so SQL needs branches to
    // route Delete/Replace/Insert to. Sealing the one unrouted data branch a
    // wholesale republication seals left it nowhere to send a delete, and the
    // refresh could only ever append.
    let adapter = adapter("publication_mutation", 7);
    let plan = plan_document_publication_branches(
        &session_material(merge_on_read_input_shape()),
        document_publication_facts_with_shape(
            ConnectorManagedPublicationTechnique::Incremental,
            ConnectorManagedPublicationEmptyInputDisposition::CommitEmptyWrite,
            ConnectorManagedPublicationShape::RowMutation,
        ),
    )
    .expect("plan a change-stream publication");
    // It stays one publication: the commit is still decided by the publication
    // facts, not by a second row-mutation flavor.
    assert_eq!(plan.flavor, IcebergWriteFlavor::ManagedPublication);
    assert!(plan.document_publication.is_some());

    let (handle, targets) = flavor_session(plan);
    assert_eq!(handle.expected_targets(), vec![ordinal(0), ordinal(1)]);
    assert_eq!(handle.branch_of(ordinal(0)), Some(IcebergWriteBranch::Data));
    assert_eq!(
        handle.branch_of(ordinal(1)),
        Some(IcebergWriteBranch::DeletionVector)
    );

    let sealed = neutral_plan(&adapter, (handle, targets)).expect("neutral plan");
    assert_eq!(
        effects(&sealed.targets()[0]),
        vec![
            ConnectorRowMutationEffect::Replace,
            ConnectorRowMutationEffect::Insert
        ]
    );
    assert_eq!(
        effects(&sealed.targets()[1]),
        vec![
            ConnectorRowMutationEffect::Delete,
            ConnectorRowMutationEffect::Replace
        ]
    );
}

#[test]
fn an_insert_only_publication_seals_one_routed_data_branch() {
    // A fast-append incremental refresh sends its rows as change events, so the
    // wholesale-republication shape leaves them nowhere to be routed: SQL's
    // change-stream compile requires every branch to declare which effects it
    // accepts, and that shape seals an unrouted branch.
    let adapter = adapter("publication_insert_only", 11);
    let plan = plan_document_publication_branches(
        &session_material(data_input_shape()),
        document_publication_facts_with_shape(
            ConnectorManagedPublicationTechnique::Incremental,
            ConnectorManagedPublicationEmptyInputDisposition::CommitEmptyWrite,
            ConnectorManagedPublicationShape::InsertOnlyChangeStream,
        ),
    )
    .expect("plan an insert-only publication");
    assert_eq!(plan.flavor, IcebergWriteFlavor::ManagedPublication);

    let (handle, targets) = flavor_session(plan);
    assert_eq!(
        handle.expected_targets(),
        vec![ordinal(0)],
        "an insert-only publication supersedes nothing, so it seals no delete branch"
    );
    assert_eq!(handle.branch_of(ordinal(0)), Some(IcebergWriteBranch::Data));

    let sealed = neutral_plan(&adapter, (handle, targets)).expect("neutral plan");
    assert_eq!(
        effects(&sealed.targets()[0]),
        vec![ConnectorRowMutationEffect::Insert],
        "the branch accepts only Insert; nothing it could supersede reaches it"
    );
}

#[test]
fn an_insert_only_publication_commits_as_an_append() {
    // It seals no delete branch, so there is no artifact retiring a prior row
    // version and a plain append is the whole commit.
    let (handle, _) = flavor_session(
        plan_document_publication_branches(
            &session_material(data_input_shape()),
            document_publication_facts_with_shape(
                ConnectorManagedPublicationTechnique::Incremental,
                ConnectorManagedPublicationEmptyInputDisposition::CommitEmptyWrite,
                ConnectorManagedPublicationShape::InsertOnlyChangeStream,
            ),
        )
        .expect("plan an insert-only publication"),
    );
    assert_eq!(handle.commit_op_kind(), CommitOpKind::FastAppend);
}

#[test]
fn a_full_refresh_is_refused_as_an_insert_only_change_stream() {
    // A full refresh republishes every row, so it has no change stream to
    // apply -- the same reason the row-mutation shape refuses it.
    let error = plan_document_publication_branches(
        &session_material(data_input_shape()),
        document_publication_facts_with_shape(
            ConnectorManagedPublicationTechnique::Full,
            ConnectorManagedPublicationEmptyInputDisposition::CommitEmptyWrite,
            ConnectorManagedPublicationShape::InsertOnlyChangeStream,
        ),
    )
    .expect_err("a full refresh applies no change stream");
    assert!(
        error.to_string().contains("does not apply a change stream"),
        "{error}"
    );
}

#[test]
fn a_change_stream_publication_commits_as_a_delta_not_an_append() {
    // The technique alone stopped being enough once a publication could apply a
    // change stream. An incremental refresh that seals a delete branch publishes
    // a delta; committing it as a plain append would add every after-image while
    // dropping the deletion vector that retires the before-image, leaving both
    // versions of the row live.
    let (delta, _) = flavor_session(
        plan_document_publication_branches(
            &session_material(merge_on_read_input_shape()),
            document_publication_facts_with_shape(
                ConnectorManagedPublicationTechnique::Incremental,
                ConnectorManagedPublicationEmptyInputDisposition::CommitEmptyWrite,
                ConnectorManagedPublicationShape::RowMutation,
            ),
        )
        .expect("plan a change-stream publication"),
    );
    assert_eq!(delta.commit_op_kind(), CommitOpKind::MaterializedRowDelta);
    // It is the same op an ordinary DML merge-on-read mutation commits under --
    // the delta form follows the artifact the delete branch writes, and the
    // mapping is stated once.
    assert_eq!(
        delta.commit_op_kind(),
        IcebergWriteFlavor::RowMutationDeletionVector.commit_op_kind()
    );

    // A publication that only republishes rows keeps the technique's own op.
    for (technique, shape, op_kind) in [
        (
            ConnectorManagedPublicationTechnique::Incremental,
            ConnectorManagedPublicationShape::Data,
            CommitOpKind::FastAppend,
        ),
        (
            ConnectorManagedPublicationTechnique::Full,
            ConnectorManagedPublicationShape::Data,
            CommitOpKind::Overwrite,
        ),
    ] {
        let (handle, _) = flavor_session(
            plan_document_publication_branches(
                &session_material(data_input_shape()),
                document_publication_facts_with_shape(
                    technique,
                    ConnectorManagedPublicationEmptyInputDisposition::CommitEmptyWrite,
                    shape,
                ),
            )
            .expect("plan a data publication"),
        );
        assert_eq!(handle.commit_op_kind(), op_kind);
    }
}

#[test]
fn a_full_refresh_cannot_apply_a_change_stream() {
    // A full refresh replaces every live row, so nothing it publishes has a
    // prior version for a change event to supersede.
    let error = plan_document_publication_branches(
        &session_material(merge_on_read_input_shape()),
        document_publication_facts_with_shape(
            ConnectorManagedPublicationTechnique::Full,
            ConnectorManagedPublicationEmptyInputDisposition::CommitEmptyWrite,
            ConnectorManagedPublicationShape::RowMutation,
        ),
    )
    .expect_err("a full refresh does not apply a change stream");
    assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
    assert!(
        error.message().contains("does not apply a change stream"),
        "unexpected message: {}",
        error.message()
    );

    // Lineage alone cannot locate the physical before-image to delete.
    let error = plan_document_publication_branches(
        &session_material(lineage_only_input_shape()),
        document_publication_facts_with_shape(
            ConnectorManagedPublicationTechnique::Incremental,
            ConnectorManagedPublicationEmptyInputDisposition::CommitEmptyWrite,
            ConnectorManagedPublicationShape::RowMutation,
        ),
    )
    .expect_err("a publication needs a physical row identity");
    assert!(
        error
            .message()
            .contains("requires a `_file`/`_pos` row identity"),
        "unexpected message: {}",
        error.message()
    );
}

#[test]
fn a_merge_on_read_delete_branch_partitions_by_the_data_file_it_supersedes() {
    // Iceberg permits one deletion vector per data file, and the prepared set
    // refuses a second, so every change event touching one old file has to reach
    // one physical delete writer. Declaring no partition field made that true by
    // gathering the whole branch onto a single writer -- correct, and serial.
    //
    // `_file` is the exclusivity key itself, so hashing by it keeps the
    // guarantee and spreads distinct files across writers.
    let adapter = adapter("mor_partitioning", 8);
    let input = merge_on_read_input_shape();
    let plan =
        plan_row_mutation_branches(&session_material(input.clone())).expect("plan a mutation");
    let sealed = neutral_plan(&adapter, flavor_session(plan)).expect("neutral plan");

    let file_token = input
        .fields()
        .into_iter()
        .find(|field| field.field().name() == "_file")
        .expect("the row identity carries `_file`")
        .token();
    let delete_route = sealed.targets()[1].route().expect("routed");
    assert_eq!(
        delete_route.requirements().colocation,
        novarocks_spi::connector::write_stack::ConnectorWriteColocation::ByKey(vec![file_token])
    );
    // SQL resolves a partition token through the route's own input ordinals, so
    // a token the branch does not consume would fail to bind.
    assert!(
        delete_route
            .input_ordinals()
            .iter()
            .any(|binding| binding.token() == file_token)
    );
}

#[test]
fn only_a_managed_publication_projects_the_committed_row_count() {
    // A publication's caller records the row count the refresh published, so
    // its receipt has to carry one and the projection has to reload -- the
    // table this generation already holds is the pre-commit view by
    // construction. No ordinary DML receipt carries a row count at all.
    //
    // The catalog behind this generation is unreachable, so a reload cannot
    // succeed. An ordinary session answering `None` is therefore proof that it
    // never reloads, and the publication's failure is proof that it does.
    let (_executor, runtime) = unreachable_rest_runtime();
    let control = crate::commit::write_stack::control::IcebergWriteSessionControl::new(
        descriptor("unit"),
        ProviderBindingEpoch::new(),
        CatalogHandle::new(
            ConnectorInstanceId::parse("unit").expect("instance id"),
            CatalogVersion::from_bytes([1; 32]),
        ),
        Arc::clone(&runtime),
    );

    let (append, _) = flavor_session(
        plan_ordinary_branches(
            IcebergWriteFlavor::Append,
            &session_material(data_input_shape()),
        )
        .expect("plan an ordinary append"),
    );
    assert_eq!(
        control
            .publication_row_count(&append, 300, &request_context())
            .expect("an ordinary write claims no row count"),
        None
    );

    let (publication, _) = flavor_session(
        plan_document_publication_branches(
            &session_material(data_input_shape()),
            document_publication_facts(
                ConnectorManagedPublicationTechnique::Full,
                ConnectorManagedPublicationEmptyInputDisposition::CommitEmptyWrite,
            ),
        )
        .expect("plan a managed publication"),
    );
    control
        .publication_row_count(&publication, 300, &request_context())
        .expect_err("a publication reloads to read its committed row count");
}

#[test]
fn a_document_publication_prepares_exact_keyed_records_closure_and_session_marker() {
    use bytes::Bytes;
    use novarocks_spi::connector::{
        ConnectorDocumentFormat, ConnectorDocumentVersion, ConnectorSnapshotRecord,
        KeyedPublication,
    };
    let (handle, _) = flavor_session(
        plan_document_publication_branches(
            &session_material(data_input_shape()),
            document_publication_facts(
                ConnectorManagedPublicationTechnique::Full,
                ConnectorManagedPublicationEmptyInputDisposition::CommitEmptyWrite,
            ),
        )
        .expect("plan a document publication"),
    );
    let make = |name, version, refs, content: &'static [u8]| {
        ConnectorDocumentVersion::try_new(
            "novarocks.test",
            name,
            ConnectorDocumentFormat::try_new("novarocks.test", name, version).unwrap(),
            refs,
            Bytes::from_static(content),
        )
        .unwrap()
    };
    let definition = make("definition", 1, vec![], b"definition");
    let interpretation = make(
        "interpretation",
        2,
        vec![("definition".into(), definition.identity())],
        b"layout",
    );
    let binding = make(
        "binding",
        1,
        vec![
            ("definition".into(), definition.identity()),
            ("interpretation".into(), interpretation.identity()),
        ],
        b"binding",
    );
    let documents = KeyedPublication {
        versions: vec![definition.clone(), interpretation.clone(), binding.clone()],
        roots: BTreeMap::from([("novarocks.test".into(), binding.identity())]),
        records: vec![ConnectorSnapshotRecord {
            owner: "novarocks.test".into(),
            name: "publication".into(),
            binding: Some(binding.identity()),
            payload: Bytes::from_static(b"result"),
        }],
        ..Default::default()
    };
    let prepared = crate::document_storage::keyed::write::publication_updates(
        &super::test_support::staged_table_metadata(),
        &documents,
    )
    .expect("prepare record and its complete binding closure");
    let record_key = "novarocks.doc.v2.record.novarocks.test~publication";
    let expected_record = format!("2.{}.cmVzdWx0", binding.identity().to_hex());
    assert_eq!(
        prepared.summary,
        BTreeMap::from([(record_key.to_string(), expected_record.clone())])
    );
    handle
        .bind_document_records(prepared.summary.clone())
        .expect("bind exact record bytes");
    assert_eq!(
        handle.document_records().unwrap(),
        Some(prepared.summary.clone())
    );
    let mut different = prepared.summary.clone();
    different.insert(
        record_key.into(),
        format!("2.{}.bmV4dA", binding.identity().to_hex()),
    );
    assert!(
        handle.bind_document_records(different).is_err(),
        "one session cannot substitute another record payload"
    );
    let updates = prepared
        .updates
        .iter()
        .filter_map(|update| match update {
            crate::iceberg::TableUpdate::SetProperties { updates } => Some(updates),
            _ => None,
        })
        .flat_map(|updates| updates.iter())
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        updates.len(),
        4,
        "three complete versions plus the binding root"
    );
    assert_eq!(
        updates
            .get("novarocks.doc.v2.root.novarocks.test")
            .map(|v| v.as_str()),
        Some(binding.identity().to_hex().as_str())
    );
    for (version, content, refs) in [
        (&definition, "ZGVmaW5pdGlvbg", serde_json::json!([])),
        (
            &interpretation,
            "bGF5b3V0",
            serde_json::json!([
                {"rel": "definition", "id": definition.identity().to_hex()}
            ]),
        ),
        (
            &binding,
            "YmluZGluZw",
            serde_json::json!([
                {"rel": "definition", "id": definition.identity().to_hex()},
                {"rel": "interpretation", "id": interpretation.identity().to_hex()}
            ]),
        ),
    ] {
        let key = format!("novarocks.doc.v2.rev.{}", version.identity().to_hex());
        let envelope: serde_json::Value = serde_json::from_str(
            updates
                .get(&key)
                .expect("every closure version is retained"),
        )
        .unwrap();
        assert_eq!(envelope["id"], version.identity().to_hex());
        assert_eq!(envelope["name"], version.name());
        assert_eq!(envelope["content"], content);
        assert_eq!(envelope["refs"], refs);
    }
    let properties = crate::commit::write_stack::control::session_snapshot_properties(&handle)
        .expect("prepare session marker");
    assert_eq!(
        properties,
        BTreeMap::from([(
            crate::commit::write_stack::control::ICEBERG_WRITE_SESSION_MARKER_PROPERTY.to_string(),
            handle.session_id().to_string(),
        )])
    );
    // This unit test covers frozen preparation. Actual atomic snapshot+closure
    // publication is exercised by commit::run::keyed_session_tests.
    let mut snapshot_properties = properties;
    snapshot_properties.extend(prepared.summary);
    assert_eq!(snapshot_properties.get(record_key), Some(&expected_record));
    assert!(!snapshot_properties.contains_key("novarocks.mv.publication.v2"));
    assert!(!snapshot_properties.contains_key("novarocks.mv.publication_id"));
    let (append, _) = flavor_session(
        plan_ordinary_branches(
            IcebergWriteFlavor::Append,
            &session_material(data_input_shape()),
        )
        .expect("ordinary append"),
    );
    assert!(append.document_publication().is_none());
    assert!(append.document_records().unwrap().is_none());
    assert!(
        append
            .bind_document_records(BTreeMap::from([(record_key.to_string(), expected_record)]))
            .is_err()
    );
    assert_eq!(
        crate::commit::write_stack::control::session_snapshot_properties(&append).unwrap(),
        BTreeMap::from([(
            crate::commit::write_stack::control::ICEBERG_WRITE_SESSION_MARKER_PROPERTY.to_string(),
            append.session_id().to_string(),
        )])
    );
}

#[test]
fn an_empty_prepared_set_commits_or_aborts_by_the_publication_disposition() {
    // The decision is the caller's declared disposition, not an inference from
    // "there were no fragments": a zero-row INSERT reaches finish exactly the
    // same way and still publishes.
    let (append, _) = flavor_session(
        plan_ordinary_branches(
            IcebergWriteFlavor::Append,
            &session_material(data_input_shape()),
        )
        .expect("plan an ordinary append"),
    );
    assert_eq!(
        append.empty_write_decision(),
        IcebergEmptyWriteDecision::Commit
    );

    let publication = |empty_input| {
        flavor_session(
            plan_document_publication_branches(
                &session_material(data_input_shape()),
                document_publication_facts(ConnectorManagedPublicationTechnique::Full, empty_input),
            )
            .expect("plan a managed publication"),
        )
        .0
    };

    let commits = publication(ConnectorManagedPublicationEmptyInputDisposition::CommitEmptyWrite);
    assert_eq!(
        commits.empty_write_decision(),
        IcebergEmptyWriteDecision::Commit
    );

    let aborts =
        publication(ConnectorManagedPublicationEmptyInputDisposition::AbortWithoutExternalCommit);
    assert_eq!(
        aborts.empty_write_decision(),
        IcebergEmptyWriteDecision::SkipExternalCommit
    );
    // Terminating without an external commit needs no catalog at all: every
    // fact it reports was frozen at begin.
    let super::domain::ClaimedWriteFinish::EmptyNoOp(completion) = aborts
        .claim_finish(|| empty_write_needed(&aborts).map(Some))
        .expect("settle the completed empty write")
    else {
        panic!("an empty NoOp is settled synchronously");
    };
    let novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion::KnownCommitted { needed, .. } = &completion else {
        panic!("actual empty author must seal its exact NoOp");
    };
    let ConnectorWriteAbortOutcome::KnownCommitted {
        needed: released, ..
    } = release_session_state(
        &descriptor("unit"),
        ProviderBindingEpoch::from_bytes([9; 16]),
        &aborts,
    )
    .expect("release exact empty fact")
    else {
        panic!("NoOp fact lost on release");
    };
    assert!(std::sync::Arc::ptr_eq(needed, &released));
    assert_eq!(released.effect(), ExternalMutationEffect::NoOp);
    let outcome = completion
        .into_legacy_outcome()
        .expect("observed empty cleanup");
    match outcome {
        ExternalMutationOutcome::KnownCommitted {
            effect, receipt, ..
        } => {
            assert_eq!(effect, ExternalMutationEffect::NoOp);
            assert_eq!(
                receipt
                    .committed_version()
                    .and_then(ConnectorCommittedVersion::snapshot_id),
                Some(77),
                "the target keeps the head the session froze"
            );
        }
        other => panic!("unexpected outcome: {other:?}"),
    }

    // Empty plans terminate in FE without opening a write session.
    let error = plan_distributed_rewrite_branches(
        &session_material(data_input_shape()),
        DATA_FILE_REWRITE,
        &[],
    )
    .unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::Internal);
}

fn _assert_error_is_send_sync(error: ConnectorError) -> impl Send + Sync {
    error
}

// -------------------------------------------------------------------------
// Staged create: a target that has no catalog entry yet.
// -------------------------------------------------------------------------

/// A REST generation whose endpoint refuses every connection.
///
/// That is the point: any code path that reaches the catalog fails loudly here,
/// so a test that succeeds against it has demonstrably not reached one.
pub(super) fn unreachable_rest_runtime() -> (
    tokio::runtime::Runtime,
    Arc<crate::metadata_context::IcebergMetadataContext>,
) {
    let executor = tokio::runtime::Runtime::new().expect("runtime");
    let handle = executor.handle().clone();
    let configuration = crate::catalog_config::parse_catalog_configuration(
        "unit",
        &[
            ("iceberg.catalog.type".to_string(), "rest".to_string()),
            ("uri".to_string(), "http://127.0.0.1:1".to_string()),
            (
                "warehouse".to_string(),
                "file:///tmp/novarocks-staged-session".to_string(),
            ),
        ],
    )
    .expect("configuration");
    let binding = IcebergReadBinding::new(
        None,
        FsAccessResolver::new(),
        Arc::new(TokioFileIoRuntime::new(handle.clone())),
        novarocks_fs::TokioRefreshExecutor::shared(handle.clone()),
        Arc::new(TokioFileIoSpawner::new(handle.clone())),
    );
    let runtime = crate::metadata_context::IcebergMetadataContext::try_new(
        crate::catalog_control::IcebergCatalogControlState::new(configuration),
        crate::resources::IcebergMetadataResources::new(binding, handle),
    )
    .expect("control runtime");
    (executor, Arc::new(runtime))
}

/// The opaque target facts a staged-create capability vends: an Iceberg table
/// that exists as metadata and nowhere else.
fn staged_target_handle(
    runtime: &Arc<crate::metadata_context::IcebergMetadataContext>,
    incarnation: ProviderBindingEpoch,
) -> novarocks_spi::connector::ConnectorTableHandle {
    staged_target_handle_at(
        runtime,
        incarnation,
        "file:///tmp/novarocks-staged-session/table",
    )
}

fn staged_target_handle_at(
    runtime: &Arc<crate::metadata_context::IcebergMetadataContext>,
    incarnation: ProviderBindingEpoch,
    location: &str,
) -> novarocks_spi::connector::ConnectorTableHandle {
    let schema = crate::iceberg::spec::Schema::builder()
        .with_fields(vec![Arc::new(crate::iceberg::spec::NestedField::required(
            1,
            "id",
            crate::iceberg::spec::Type::Primitive(crate::iceberg::spec::PrimitiveType::Long),
        ))])
        .build()
        .expect("schema");
    let metadata = crate::iceberg::spec::TableMetadataBuilder::new(
        schema,
        crate::iceberg::spec::PartitionSpec::unpartition_spec(),
        crate::iceberg::spec::SortOrder::unsorted_order(),
        location.to_string(),
        crate::iceberg::spec::FormatVersion::V2,
        std::collections::HashMap::new(),
    )
    .expect("metadata builder")
    .build()
    .expect("metadata")
    .metadata;
    let table = crate::iceberg::table::Table::builder()
        .runtime(
            runtime
                .resources()
                .catalog_runtime()
                .iceberg_runtime()
                .clone(),
        )
        .identifier(crate::iceberg::TableIdent::from_strs(["db", "staged"]).expect("identifier"))
        .file_io(crate::fs_io::build_file_io_for_location(
            location,
            runtime.resources().planning_binding().clone(),
        ))
        .metadata(metadata)
        .build()
        .expect("table");
    let provider =
        crate::metadata::IcebergMetadata::new(descriptor("unit"), incarnation, Arc::clone(runtime));
    provider
        .staged_write_table_handle(
            &table,
            novarocks_spi::connector::ConnectorMutationOperationId::new(),
            &request_context(),
        )
        .expect("staged write table handle")
}

fn staged_begin_request(
    target: novarocks_spi::connector::ConnectorTableHandle,
) -> novarocks_spi::connector::write_stack::session::ConnectorWriteBeginRequest {
    novarocks_spi::connector::write_stack::session::ConnectorWriteBeginRequest {
        catalog_session: Default::default(),
        table: Arc::from("db.staged"),
        target_ref: novarocks_spi::connector::ConnectorWriteTargetRef::main(),
        intent: novarocks_spi::connector::ConnectorWriteIntent::Append,
        purpose: ConnectorWriteAdmissionPurpose::OrdinaryDml,
        input: novarocks_spi::connector::ConnectorWriteInputRequest::Data {
            fields: vec![
                novarocks_spi::connector::ConnectorWriteFieldRequest::try_new_typed(
                    Field::new("id", DataType::Int64, false),
                    novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
                )
                .expect("staged caller declares required Int64 id"),
            ],
        },
        base: None,
        flavor: novarocks_spi::connector::write_stack::ConnectorWriteSessionFlavor::StagedCreate(
            target,
        ),
        context: request_context(),
    }
}

/// A staged begin admits entirely from the facts it was handed.
///
/// The catalog behind this generation is unreachable, so a `load_table` would
/// fail. Admission succeeding is therefore proof that a staged target is never
/// looked up -- which is the whole reason the flavor exists.
#[test]
fn a_staged_begin_admits_without_reaching_the_catalog() {
    use novarocks_spi::connector::write_stack::session::ConnectorWriteControl;

    let incarnation = ProviderBindingEpoch::new();
    let (_executor, runtime) = unreachable_rest_runtime();
    let control = crate::commit::write_stack::control::IcebergWriteSessionControl::new(
        descriptor("unit"),
        incarnation,
        CatalogHandle::new(
            ConnectorInstanceId::parse("unit").expect("instance id"),
            CatalogVersion::from_bytes([1; 32]),
        ),
        Arc::clone(&runtime),
    );

    // The same generation refuses an ordinary write against the same name,
    // because that one does have to load the table.
    let mut ordinary = staged_begin_request(staged_target_handle(&runtime, incarnation));
    ordinary.flavor = novarocks_spi::connector::write_stack::ConnectorWriteSessionFlavor::Ordinary;
    control
        .begin_write(ordinary)
        .expect_err("an ordinary write must resolve its target through the catalog");

    let plan = control
        .begin_write(staged_begin_request(staged_target_handle(
            &runtime,
            incarnation,
        )))
        .expect("a staged begin needs no catalog");
    assert_eq!(plan.expected_targets(), vec![ordinal(0)]);
}

/// A staged finish seals its artifacts and commits nothing.
///
/// It runs against the same unreachable catalog, so a snapshot commit could not
/// have happened; and the receipt it returns carries no committed version,
/// because there is no version to name until the publication creates the table.
#[test]
fn a_staged_finish_seals_its_artifacts_without_committing() {
    use novarocks_spi::connector::write_stack::session::{
        ConnectorWriteControl, ConnectorWriteFinishRequest,
    };

    let incarnation = ProviderBindingEpoch::new();
    let (executor, runtime) = unreachable_rest_runtime();
    let control = crate::commit::write_stack::control::IcebergWriteSessionControl::new(
        descriptor("unit"),
        incarnation,
        CatalogHandle::new(
            ConnectorInstanceId::parse("unit").expect("instance id"),
            CatalogVersion::from_bytes([1; 32]),
        ),
        Arc::clone(&runtime),
    );
    let scratch = tempfile::TempDir::new().unwrap();
    let location = format!("file://{}/table", scratch.path().display());
    let plan = control
        .begin_write(staged_begin_request(staged_target_handle_at(
            &runtime,
            incarnation,
            &location,
        )))
        .expect("staged begin");
    let adapter = crate::commit::write_stack::runtime::build_write_adapter(
        descriptor("unit"),
        CatalogHandle::new(
            ConnectorInstanceId::parse("unit").expect("instance id"),
            CatalogVersion::from_bytes([1; 32]),
        ),
    );
    let (output, _writer) = seal_test_data_output(&control, &executor, &runtime, &adapter, &plan);
    control
        .confirm_write_received(plan.commit_session(), &output.query)
        .unwrap();

    let outcome = control
        .finish_write(ConnectorWriteFinishRequest {
            commit: plan.commit_session(),
            row_count: output.row_count,
            statistics: Vec::new(),
            publication:
                novarocks_spi::connector::write_stack::ConnectorWriteFinishPublication::None,
            context: request_context(),
        })
        .expect("a staged finish needs no catalog");

    let ExternalMutationOutcome::KnownCommitted {
        effect, receipt, ..
    } = outcome
    else {
        panic!("sealing is not in doubt: nothing external was attempted");
    };
    // Nothing was applied out there, and the receipt names no version because
    // the target has none until it is published.
    assert_eq!(effect, ExternalMutationEffect::NoOp);
    assert!(receipt.committed_version().is_none());
    assert!(receipt.payload().len() < 512);
    let owner = novarocks_spi::connector::ConnectorProviderBindingKey {
        instance_id: descriptor("unit").instance_id,
        incarnation,
    };
    assert!(receipt.process_local_payload(&owner).unwrap().is_some());
    let handle = adapter.commit_session(plan.commit_session()).unwrap();
    let files =
        crate::write_codec::open_staged_files(&receipt, handle.staged_metadata().unwrap(), &owner)
            .unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].record_count, 2);
    assert!(matches!(
        handle.state().unwrap(),
        IcebergWriteSessionState::Sealed
    ));

    // The single terminal is claimed, so a second finish cannot mint a second
    // receipt for the same sealed artifacts.
    control
        .finish_write(ConnectorWriteFinishRequest {
            commit: plan.commit_session(),
            row_count: 0,
            statistics: Vec::new(),
            publication:
                novarocks_spi::connector::write_stack::ConnectorWriteFinishPublication::None,
            context: request_context(),
        })
        .expect_err("a sealed session is finished");
}

/// File facts grow independently of the bounded seal. The domain session
/// supplies complete accepted artifacts; this test does not claim a writer run.
#[test]
fn a_staged_finish_seals_a_thousand_files_with_bounded_receipt_facts() {
    use novarocks_spi::connector::write_stack::ConnectorWriteFinishRequest;
    let incarnation = ProviderBindingEpoch::new();
    let (_executor, runtime) = unreachable_rest_runtime();
    let control = crate::commit::write_stack::control::IcebergWriteSessionControl::new(
        descriptor("unit"),
        incarnation,
        CatalogHandle::new(
            ConnectorInstanceId::parse("unit").unwrap(),
            CatalogVersion::from_bytes([1; 32]),
        ),
        runtime,
    );
    let adapter = adapter("unit", 1);
    let metadata = super::test_support::staged_table_metadata();
    let session_id = IcebergWriteSessionId::new();
    let facts = IcebergWriteTableFacts::try_new(
        metadata.uuid().to_string(),
        "db".into(),
        "staged".into(),
        metadata.location().into(),
        format!("{}/data", metadata.location()),
        "main".into(),
        None,
        0,
        metadata.current_schema_id(),
        metadata.default_partition_spec_id(),
        2,
    )
    .unwrap();
    let (session, _) = plan_write_session(
        session_id,
        IcebergWriteSessionPlanInput {
            flavor: IcebergWriteFlavor::StagedCreate,
            purpose: ConnectorWriteAdmissionPurpose::OrdinaryDml,
            table: facts,
            base_version_digest: None,
            staged_metadata: Some(metadata.clone()),
            data: data_branch_plan(),
            deletes: vec![],
        },
    )
    .unwrap();
    let expected = session.expected_targets();
    let commit = adapter.wrap_commit_session(session);
    let paths: Vec<_> = (0..1000)
        .map(|index| format!("{}/data/sealed-{index:04}.parquet", metadata.location()))
        .collect();
    let fragments = paths
        .iter()
        .map(|path| {
            (
                ordinal(0),
                adapter.wrap_commit_fragment(data_fragment(path)),
            )
        })
        .collect();
    receive_write_query(&control, &commit, &expected, fragments).unwrap();
    let result = control
        .finish_write(ConnectorWriteFinishRequest {
            commit: &commit,
            row_count: 5000,
            statistics: vec![],
            publication:
                novarocks_spi::connector::write_stack::ConnectorWriteFinishPublication::None,
            context: request_context(),
        })
        .unwrap();
    let ExternalMutationOutcome::KnownCommitted {
        effect: ExternalMutationEffect::NoOp,
        receipt,
        finalization: ExternalMutationFinalization::Complete,
        ..
    } = result
    else {
        panic!("the complete staged set must seal without Catalog publication");
    };
    assert!(receipt.payload().len() < 512);
    assert!(receipt.committed_version().is_none());
    let owner = novarocks_spi::connector::ConnectorProviderBindingKey {
        instance_id: descriptor("unit").instance_id,
        incarnation,
    };
    let files = crate::write_codec::open_staged_files(&receipt, &metadata, &owner).unwrap();
    assert_eq!(files.len(), 1000);
    assert_eq!(
        files.iter().map(|file| file.record_count).sum::<u64>(),
        5000
    );
    assert_eq!(
        files
            .iter()
            .map(|file| file.path.clone())
            .collect::<Vec<_>>(),
        paths
    );
    let reports = files
        .iter()
        .map(|file| {
            crate::commit::report::writer_report_from_written_file(file, &metadata).unwrap()
        })
        .collect::<Vec<_>>();
    assert!(
        crate::write_codec::encode_writer_reports(&reports, &metadata)
            .unwrap()
            .len()
            > 64 * 1024
    );
    assert!(matches!(
        adapter.commit_session(&commit).unwrap().state().unwrap(),
        IcebergWriteSessionState::Sealed
    ));
    assert_eq!(
        control.cleanup_responsibility(&commit).unwrap(),
        novarocks_spi::connector::ConnectorMetadataCleanupResponsibility::Retained
    );
    // Equal session identity/count with different file facts must remain a
    // different content receipt, even though the attachment is excluded from Eq.
    let mut changed = files.to_vec();
    changed[0].record_count += 1;
    let changed =
        crate::write_codec::seal_staged_files(changed, &metadata, session_id.to_bytes(), &owner)
            .unwrap();
    assert_ne!(receipt, changed);
    assert_ne!(receipt.digest(), changed.digest());
}

/// A staged write that produced no artifact still reaches the provider.
///
/// The empty-write shortcut settles against the version the target already
/// holds, and a staged target holds none. Taking it here would fail a CTAS that
/// selected no rows; the provider gets an empty seal instead and the
/// publication decides what an empty table means.
#[test]
fn an_empty_staged_write_seals_rather_than_settling_as_unchanged() {
    use novarocks_spi::connector::write_stack::session::{
        ConnectorWriteControl, ConnectorWriteFinishRequest,
    };

    let incarnation = ProviderBindingEpoch::new();
    let (_executor, runtime) = unreachable_rest_runtime();
    let control = crate::commit::write_stack::control::IcebergWriteSessionControl::new(
        descriptor("unit"),
        incarnation,
        CatalogHandle::new(
            ConnectorInstanceId::parse("unit").expect("instance id"),
            CatalogVersion::from_bytes([1; 32]),
        ),
        Arc::clone(&runtime),
    );
    let plan = control
        .begin_write(staged_begin_request(staged_target_handle(
            &runtime,
            incarnation,
        )))
        .expect("staged begin");
    receive_write_query(
        &control,
        plan.commit_session(),
        &plan.expected_targets(),
        Vec::new(),
    )
    .unwrap();

    let outcome = control
        .finish_write(ConnectorWriteFinishRequest {
            commit: plan.commit_session(),
            row_count: 0,
            statistics: Vec::new(),
            publication:
                novarocks_spi::connector::write_stack::ConnectorWriteFinishPublication::None,
            context: request_context(),
        })
        .expect("an empty staged write still seals");

    assert!(matches!(
        outcome,
        ExternalMutationOutcome::KnownCommitted { .. }
    ));
}

// ---------------------------------------------------------------------------
// Equality delete
// ---------------------------------------------------------------------------

/// `ALTER TABLE ... ADD EQUALITY DELETE` seals exactly one branch, and it is
/// not a data branch.
///
/// The statement writes delete files and nothing else. A data branch beside
/// them would give SQL somewhere to send rows the statement never produces, and
/// the sink would then be bound to a target no writer ever feeds.
#[test]
fn an_equality_delete_session_seals_one_equality_branch_and_no_data_branch() {
    let adapter = adapter("equality_delete", 1);
    let mut material = session_material(equality_delete_input_shape());
    material.equality = Some(equality_delete_recipe());

    let plan = plan_ordinary_branches(IcebergWriteFlavor::EqualityDelete, &material)
        .expect("plan an equality delete");
    assert_eq!(plan.flavor, IcebergWriteFlavor::EqualityDelete);
    assert_eq!(plan.branches.len(), 1);

    let (handle, targets) = flavor_session(plan);
    assert_eq!(
        handle.branch_of(ordinal(0)),
        Some(IcebergWriteBranch::EqualityDelete)
    );
    // It appends delete files rather than superseding a data file's deletes, so
    // it commits as an ordinary row delta.
    assert_eq!(handle.commit_op_kind(), CommitOpKind::RowDelta);
    assert!(handle.targets()[0].position_rewrite().is_none());

    let sealed = neutral_plan(&adapter, (handle, targets)).expect("neutral plan");
    assert_eq!(sealed.expected_targets(), vec![ordinal(0)]);
    assert!(sealed.targets()[0].route().is_none());
}

fn scalar_integer_staged_target_handle(
    runtime: &Arc<crate::metadata_context::IcebergMetadataContext>,
    incarnation: ProviderBindingEpoch,
    collect_statistics: bool,
) -> novarocks_spi::connector::ConnectorTableHandle {
    let location = "file:///tmp/novarocks-staged-session/table";
    let schema = crate::iceberg::spec::Schema::builder()
        .with_fields(vec![
            Arc::new(crate::iceberg::spec::NestedField::required(
                1,
                "id",
                crate::iceberg::spec::Type::Primitive(crate::iceberg::spec::PrimitiveType::Int),
            )),
            Arc::new(crate::iceberg::spec::NestedField::optional(
                2,
                "small",
                crate::iceberg::spec::Type::Primitive(crate::iceberg::spec::PrimitiveType::Int),
            )),
            Arc::new(crate::iceberg::spec::NestedField::required(
                3,
                "plain",
                crate::iceberg::spec::Type::Primitive(crate::iceberg::spec::PrimitiveType::Int),
            )),
        ])
        .build()
        .expect("schema");
    let metadata = crate::iceberg::spec::TableMetadataBuilder::new(
        schema,
        crate::iceberg::spec::PartitionSpec::unpartition_spec(),
        crate::iceberg::spec::SortOrder::unsorted_order(),
        location.to_string(),
        crate::iceberg::spec::FormatVersion::V2,
        std::collections::HashMap::from([
            (
                crate::schema_mapping::nominal_declaration::PROPERTY.to_string(),
                r#"{"version":1,"fields":[{"id":1,"domain":"tinyint"},{"id":2,"domain":"smallint"}]}"#.to_string(),
            ),
            (
                crate::stats_assembler::COLLECT_ON_WRITE_PROPERTY.to_string(),
                collect_statistics.to_string(),
            ),
        ]),
    )
    .expect("metadata builder")
    .build()
    .expect("metadata")
    .metadata;
    let table = crate::iceberg::table::Table::builder()
        .runtime(
            runtime
                .resources()
                .catalog_runtime()
                .iceberg_runtime()
                .clone(),
        )
        .identifier(crate::iceberg::TableIdent::from_strs(["db", "staged"]).expect("identifier"))
        .file_io(crate::fs_io::build_file_io_for_location(
            location,
            runtime.resources().planning_binding().clone(),
        ))
        .metadata(metadata)
        .build()
        .expect("table");
    let provider =
        crate::metadata::IcebergMetadata::new(descriptor("unit"), incarnation, Arc::clone(runtime));
    provider
        .staged_write_table_handle(
            &table,
            novarocks_spi::connector::ConnectorMutationOperationId::new(),
            &request_context(),
        )
        .expect("staged write table handle")
}

fn scalar_integer_fields() -> Vec<novarocks_spi::connector::ConnectorWriteFieldRequest> {
    use novarocks_type_contract::FunctionValueType;
    vec![
        (
            Field::new("id", DataType::Int8, false),
            FunctionValueType::new(DataType::Int8, false),
        ),
        (
            Field::new("small", DataType::Int16, true),
            FunctionValueType::new(DataType::Int16, true),
        ),
        (
            Field::new("plain", DataType::Int32, false),
            FunctionValueType::new(DataType::Int32, false),
        ),
    ]
    .into_iter()
    .enumerate()
    .map(|(ordinal, (field, value))| {
        // Independent SQL execution declarations accompany the provider's IDs.
        novarocks_spi::connector::ConnectorWriteFieldRequest::try_new_typed(
            field.with_metadata(std::collections::HashMap::<String, String>::from([(
                "PARQUET:field_id".into(),
                (ordinal + 1).to_string(),
            )])),
            value,
        )
        .expect("complete scalar integer caller declaration")
    })
    .collect()
}

#[test]
fn real_prepare_and_begin_preserve_the_same_scalar_integer_fields() {
    use novarocks_spi::connector::write_stack::session::ConnectorWriteControl;
    use novarocks_spi::connector::{
        ConnectorProviderBindingKey, ConnectorWriteInputRequest, ConnectorWritePreparationOutcome,
        ConnectorWritePreparationRequest,
    };
    for collect_statistics in [true, false] {
        let incarnation = ProviderBindingEpoch::new();
        let (_executor, runtime) = unreachable_rest_runtime();
        let target = scalar_integer_staged_target_handle(&runtime, incarnation, collect_statistics);
        let mut request = staged_begin_request(target.clone());
        request.input = ConnectorWriteInputRequest::Data {
            fields: scalar_integer_fields(),
        };
        let prepared = crate::commit::write_preparation::prepare_write(
            ConnectorWritePreparationRequest {
                table: target,
                target_ref: request.target_ref.clone(),
                intent: request.intent,
                purpose: request.purpose,
                input: request.input.clone(),
                context: request_context(),
            },
            &ConnectorProviderBindingKey {
                instance_id: ConnectorInstanceId::parse("unit").expect("instance"),
                incarnation,
            },
        )
        .expect("prepare");
        let ConnectorWritePreparationOutcome::Prepared(prepared) = prepared else {
            panic!("expected prepared scalar fields")
        };
        let control = crate::commit::write_stack::control::IcebergWriteSessionControl::new(
            descriptor("unit"),
            incarnation,
            CatalogHandle::new(
                ConnectorInstanceId::parse("unit").expect("instance"),
                CatalogVersion::from_bytes([1; 32]),
            ),
            Arc::clone(&runtime),
        );
        let session = control
            .begin_write(request)
            .expect("begin with authoritative scalar fields");
        let expected = scalar_integer_fields()
            .iter()
            .map(|f| f.field().clone())
            .collect::<Vec<_>>();
        let prepared_fields = prepared
            .input()
            .fields()
            .iter()
            .map(|f| f.field().clone())
            .collect::<Vec<_>>();
        assert_eq!(prepared_fields, expected);
        assert_eq!(session.targets().len(), 1);
        assert_eq!(
            session.targets()[0]
                .input()
                .fields()
                .iter()
                .map(|f| f.field().clone())
                .collect::<Vec<_>>(),
            prepared_fields
        );
        // Staged create deliberately has no collect-on-write aggregates. The
        // exact input-domain admission still applies before that eligibility gate.
        assert!(session.targets()[0].statistics().is_empty());
    }
}

#[test]
fn real_begin_rejects_storage_i32_forgery_even_when_statistics_are_disabled() {
    use novarocks_spi::connector::write_stack::session::ConnectorWriteControl;
    use novarocks_spi::connector::{
        ConnectorProviderBindingKey, ConnectorWriteFieldRequest, ConnectorWriteInputRequest,
        ConnectorWritePreparationOutcome, ConnectorWritePreparationRequest,
    };
    for collect_statistics in [true, false] {
        for forged_ordinal in [0, 1] {
            let incarnation = ProviderBindingEpoch::new();
            let (_executor, runtime) = unreachable_rest_runtime();
            let target =
                scalar_integer_staged_target_handle(&runtime, incarnation, collect_statistics);
            let mut fields = scalar_integer_fields();
            fields[forged_ordinal] = ConnectorWriteFieldRequest::try_new_typed(
                fields[forged_ordinal]
                    .field()
                    .clone()
                    .with_data_type(DataType::Int32),
                novarocks_type_contract::FunctionValueType::new(
                    DataType::Int32,
                    forged_ordinal == 1,
                ),
            )
            .expect("explicit forged scalar declaration");
            let mut request = staged_begin_request(target.clone());
            request.input = ConnectorWriteInputRequest::Data { fields };
            let outcome = crate::commit::write_preparation::prepare_write(
                ConnectorWritePreparationRequest {
                    table: target,
                    target_ref: request.target_ref.clone(),
                    intent: request.intent,
                    purpose: request.purpose,
                    input: request.input.clone(),
                    context: request_context(),
                },
                &ConnectorProviderBindingKey {
                    instance_id: ConnectorInstanceId::parse("unit").expect("instance"),
                    incarnation,
                },
            )
            .expect("prepare rebuilds from frozen metadata");
            let ConnectorWritePreparationOutcome::Prepared(prepared) = outcome else {
                panic!("expected preparation")
            };
            assert_eq!(
                prepared.input().fields()[forged_ordinal].field(),
                scalar_integer_fields()[forged_ordinal].field()
            );
            let control = crate::commit::write_stack::control::IcebergWriteSessionControl::new(
                descriptor("unit"),
                incarnation,
                CatalogHandle::new(
                    ConnectorInstanceId::parse("unit").expect("instance"),
                    CatalogVersion::from_bytes([1; 32]),
                ),
                Arc::clone(&runtime),
            );
            let error = control
                .begin_write(request)
                .expect_err("session must reject a forged carrier");
            assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
            assert!(
                error
                    .message()
                    .contains("authoritative scalar integer declaration")
            );
        }
    }
}

// Reuse the catalog and filesystem tripwires used by document admission tests.
use crate::document_storage::admission_support as owner_admission_support;

fn refusing_write_runtime(
    kind: &str,
) -> (
    tokio::runtime::Runtime,
    Arc<crate::metadata_context::IcebergMetadataContext>,
    Arc<owner_admission_support::AdmissionCatalogSpy>,
    Arc<owner_admission_support::AdmissionFileIoSpy>,
    tempfile::TempDir,
) {
    let executor = tokio::runtime::Runtime::new().expect("runtime");
    let warehouse = tempfile::tempdir().expect("warehouse");
    let mut properties = vec![
        ("iceberg.catalog.type".to_string(), kind.to_string()),
        (
            "warehouse".to_string(),
            format!("file://{}", warehouse.path().display()),
        ),
    ];
    if kind == "hive" {
        properties.push((
            "hive.metastore.uris".to_string(),
            "thrift://127.0.0.1:1".to_string(),
        ));
    }
    let configuration = crate::catalog_config::parse_catalog_configuration("unit", &properties)
        .expect("configuration");
    let file_io = Arc::new(owner_admission_support::AdmissionFileIoSpy::default());
    let binding = IcebergReadBinding::new(
        None,
        FsAccessResolver::new(),
        file_io.clone(),
        file_io.clone(),
        file_io.clone(),
    );
    let state = crate::catalog_control::IcebergCatalogControlState::new(configuration);
    let resources =
        crate::resources::IcebergMetadataResources::new(binding, executor.handle().clone());
    let native =
        crate::metadata_context::IcebergMetadataContext::try_new(state.clone(), resources.clone())
            .expect("native runtime");
    let spy = Arc::new(owner_admission_support::AdmissionCatalogSpy::new(
        Arc::clone(native.novarocks_catalog()),
    ));
    let runtime = Arc::new(
        crate::metadata_context::IcebergMetadataContext::with_catalog_for_test(
            state,
            resources,
            spy.clone(),
        ),
    );
    (executor, runtime, spy, file_io, warehouse)
}

/// Mint a structurally valid declaration under an explicitly admitted test
/// owner; the production owner is still asked again by begin_write.
fn document_publication_for_refusal(
    runtime: &Arc<crate::metadata_context::IcebergMetadataContext>,
    incarnation: ProviderBindingEpoch,
    catalog_handle: CatalogHandle,
    technique: ConnectorManagedPublicationTechnique,
) -> (
    ConnectorWriteSessionFlavor,
    novarocks_spi::connector::ConnectorWriteBaseVersion,
) {
    use novarocks_spi::connector::{
        ConnectorTableIdentity, ConnectorTableObjectId, ConnectorWriteBaseVersion,
    };
    let base =
        ConnectorWriteBaseVersion::try_new(bytes::Bytes::from_static(b"test-base")).expect("base");
    let declaration = super::test_support::admitted_document_publication_declaration(
        runtime,
        incarnation,
        catalog_handle,
        ConnectorTableIdentity {
            instance_id: descriptor("unit").instance_id,
            namespace: Arc::from("db"),
            table: Arc::from("staged"),
        },
        ConnectorTableObjectId::try_new(bytes::Bytes::from_static(b"table-object"))
            .expect("object id"),
        base.clone(),
        technique,
        ConnectorManagedPublicationEmptyInputDisposition::CommitEmptyWrite,
    );
    (
        ConnectorWriteSessionFlavor::ApplicationDocumentPublication {
            declaration: Box::new(declaration),
            shape: ConnectorManagedPublicationShape::Data,
        },
        base,
    )
}

fn all_write_admission_requests(
    runtime: &Arc<crate::metadata_context::IcebergMetadataContext>,
    incarnation: ProviderBindingEpoch,
    catalog_handle: CatalogHandle,
) -> Vec<(
    String,
    novarocks_spi::connector::write_stack::session::ConnectorWriteBeginRequest,
)> {
    use novarocks_spi::connector::ConnectorWriteIntent;
    let staged = staged_target_handle(runtime, incarnation);
    let ordinary = |intent| {
        let mut request = staged_begin_request(staged.clone());
        request.flavor = ConnectorWriteSessionFlavor::Ordinary;
        request.intent = intent;
        request
    };
    let mut requests = [
        ("append", ConnectorWriteIntent::Append),
        ("overwrite", ConnectorWriteIntent::Overwrite),
        (
            "partition overwrite",
            ConnectorWriteIntent::PartitionOverwrite,
        ),
        ("row delta", ConnectorWriteIntent::RowDelta),
    ]
    .into_iter()
    .map(|(name, intent)| (name.to_string(), ordinary(intent)))
    .collect::<Vec<_>>();
    let mut equality_delete = ordinary(ConnectorWriteIntent::RowDelta);
    equality_delete.input = novarocks_spi::connector::ConnectorWriteInputRequest::EqualityDelete {
        equality_fields: vec![
            novarocks_spi::connector::ConnectorWriteFieldRequest::try_new_typed(
                Field::new("id", DataType::Int64, false),
                novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
            )
            .expect("equality caller declares required Int64 id"),
        ],
    };
    requests.push(("equality delete".to_string(), equality_delete));
    for (name, flavor) in [
        (
            "staged create",
            ConnectorWriteSessionFlavor::StagedCreate(staged.clone()),
        ),
        ("row mutation", ConnectorWriteSessionFlavor::RowMutation),
        (
            "data rewrite",
            ConnectorWriteSessionFlavor::DistributedRewrite {
                shape: DATA_FILE_REWRITE,
                frozen: test_rewrite_payload(),
            },
        ),
        (
            "position delete rewrite",
            ConnectorWriteSessionFlavor::DistributedRewrite {
                shape: POSITION_DELETE_REWRITE,
                frozen: test_rewrite_payload(),
            },
        ),
    ] {
        let mut request = ordinary(ConnectorWriteIntent::Append);
        request.flavor = flavor;
        requests.push((name.to_string(), request));
    }
    for (name, technique, intent) in [
        (
            "full document publication",
            ConnectorManagedPublicationTechnique::Full,
            ConnectorWriteIntent::Overwrite,
        ),
        (
            "incremental document publication",
            ConnectorManagedPublicationTechnique::Incremental,
            ConnectorWriteIntent::Append,
        ),
    ] {
        let (flavor, base) = document_publication_for_refusal(
            runtime,
            incarnation,
            catalog_handle.clone(),
            technique,
        );
        let mut request = ordinary(intent);
        request.flavor = flavor;
        request.base = Some(base);
        requests.push((name.to_string(), request));
    }
    requests
}

#[test]
fn hms_write_admission_refuses_every_flavor_without_catalog_or_filesystem_io() {
    assert_write_admission_refused_without_io(
        "hive",
        novarocks_spi::connector::ConnectorRequestInitiation::Statement,
        "read-only compatibility entry",
    );
}

#[test]
fn hadoop_background_write_admission_refuses_every_flavor_without_io() {
    assert_write_admission_refused_without_io(
        "hadoop",
        novarocks_spi::connector::ConnectorRequestInitiation::Background,
        "Hadoop Iceberg catalog",
    );
}

fn assert_write_admission_refused_without_io(
    kind: &str,
    initiation: novarocks_spi::connector::ConnectorRequestInitiation,
    expected_message: &str,
) {
    use novarocks_spi::connector::write_stack::session::ConnectorWriteControl;
    let (_executor, runtime, catalog, file_io, _warehouse) = refusing_write_runtime(kind);
    let incarnation = ProviderBindingEpoch::new();
    let catalog_handle = CatalogHandle::new(
        descriptor("unit").instance_id,
        CatalogVersion::from_bytes([1; 32]),
    );
    let control = crate::commit::write_stack::control::IcebergWriteSessionControl::new(
        descriptor("unit"),
        incarnation,
        catalog_handle.clone(),
        Arc::clone(&runtime),
    );
    let requests = all_write_admission_requests(&runtime, incarnation, catalog_handle);
    assert_eq!(
        requests.len(),
        11,
        "all flavors, intents and equality delete"
    );
    for (name, mut request) in requests {
        request.context = request.context.with_initiation(initiation);
        let error = control
            .begin_write(request)
            .expect_err("owner must refuse before I/O");
        assert_eq!(
            error.kind(),
            ConnectorErrorKind::Unsupported,
            "{kind}: {name}"
        );
        let message = if kind == "hadoop" {
            match name.as_str() {
                "full document publication" | "incremental document publication" => {
                    "application-document management requires an Iceberg REST catalog"
                }
                "staged create" => "staged-create protocol",
                _ => "requires a single writer: background",
            }
        } else {
            expected_message
        };
        assert!(
            error.message().contains(message),
            "{kind}: {name}: {}",
            error.message()
        );
        catalog.assert_no_io();
        file_io.assert_no_dispatch();
    }
}

fn test_rewrite_payload() -> novarocks_spi::connector::ConnectorProcessLocalPayload {
    novarocks_spi::connector::ConnectorProcessLocalPayload::new(
        novarocks_spi::connector::ConnectorProviderBindingKey {
            instance_id: novarocks_spi::connector::ConnectorInstanceId::parse("test").unwrap(),
            incarnation: ProviderBindingEpoch::from_bytes([1; 16]),
        },
        std::sync::Arc::new(()),
    )
}

// Real local FileIO protects the attempt-input contract without an object-store
// test double. This owner runs with the ordinary Iceberg unit-test chain; SQL
// owns native FE/BE execution. TempDir and Runtime own isolation and real exit.
pub(in crate::commit) struct FrozenRewriteLake {
    executor: tokio::runtime::Runtime,
    _warehouse: tempfile::TempDir,
    runtime: Arc<crate::metadata_context::IcebergMetadataContext>,
    rewrite: crate::distributed_rewrite::IcebergDistributedRewriteControl,
    pub(in crate::commit) control: crate::commit::write_stack::control::IcebergWriteSessionControl,
    pub(in crate::commit) adapter: IcebergWriteAdapter,
}
impl FrozenRewriteLake {
    pub(in crate::commit) fn new() -> Self {
        Self::new_version(crate::iceberg::spec::FormatVersion::V2)
    }
    fn new_version(version: crate::iceberg::spec::FormatVersion) -> Self {
        Self::new_version_with_activity(version, None)
    }
    fn new_version_with_activity(
        version: crate::iceberg::spec::FormatVersion,
        activity: Option<Arc<crate::activity::GenerationActivity>>,
    ) -> Self {
        let executor = tokio::runtime::Runtime::new().unwrap();
        let warehouse = tempfile::tempdir().unwrap();
        let configuration = crate::catalog_config::parse_catalog_configuration(
            "rewrite-local",
            &[(
                "iceberg.catalog.warehouse".to_string(),
                warehouse.path().display().to_string(),
            )],
        )
        .unwrap();
        let binding = IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(executor.handle().clone())),
            novarocks_fs::TokioRefreshExecutor::shared(executor.handle().clone()),
            Arc::new(TokioFileIoSpawner::new(executor.handle().clone())),
        );
        let resources =
            crate::resources::IcebergMetadataResources::new(binding, executor.handle().clone());
        let resources = match activity {
            Some(activity) => resources.with_activity(activity),
            None => resources,
        };
        let runtime = Arc::new(
            crate::metadata_context::IcebergMetadataContext::try_new(
                crate::catalog_control::IcebergCatalogControlState::new(configuration),
                resources,
            )
            .unwrap(),
        );
        let descriptor = descriptor("rewrite-local");
        let incarnation = ProviderBindingEpoch::from_bytes([6; 16]);
        let provider = Arc::new(crate::metadata::IcebergMetadata::new(
            descriptor.clone(),
            incarnation,
            runtime.clone(),
        ));
        let rewrite = crate::distributed_rewrite::IcebergDistributedRewriteControl::new(
            descriptor.clone(),
            incarnation,
            runtime.clone(),
            provider,
        )
        .unwrap();
        let control = crate::commit::write_stack::control::IcebergWriteSessionControl::new(
            descriptor.clone(),
            incarnation,
            CatalogHandle::new(descriptor.instance_id, CatalogVersion::from_bytes([6; 32])),
            runtime.clone(),
        );
        executor.block_on(async {
            let schema = crate::iceberg::spec::Schema::builder()
                .with_fields(vec![Arc::new(crate::iceberg::spec::NestedField::required(
                    1,
                    "id",
                    crate::iceberg::spec::Type::Primitive(
                        crate::iceberg::spec::PrimitiveType::Long,
                    ),
                ))])
                .build()
                .unwrap();
            crate::catalog::admission_test_support::create_static_fixture_table(
                &runtime,
                incarnation,
                crate::catalog::CatalogNamespaceName::new("db"),
                crate::iceberg::TableCreation::builder()
                    .name("t".to_string())
                    .schema(schema)
                    .format_version(version)
                    .properties(std::collections::HashMap::from([
                        (
                            "write.metadata.metrics.default".to_string(),
                            "none".to_string(),
                        ),
                        (
                            crate::stats_assembler::COLLECT_ON_WRITE_PROPERTY.to_string(),
                            "false".to_string(),
                        ),
                    ]))
                    .build(),
            )
            .await;
        });
        Self {
            executor,
            _warehouse: warehouse,
            runtime,
            rewrite,
            control,
            adapter: adapter("rewrite-local", 6),
        }
    }
    fn table(&self) -> crate::iceberg::table::Table {
        self.executor.block_on(
            crate::catalog::admission_test_support::load_static_fixture_table(
                &self.runtime,
                &crate::iceberg::TableIdent::new(
                    crate::iceberg::NamespaceIdent::new("db".to_string()),
                    "t".to_string(),
                ),
            ),
        )
    }
    fn plan(&self, position: bool) -> novarocks_spi::connector::ConnectorDistributedRewritePlan {
        use novarocks_spi::connector::{
            ConnectorDistributedRewrite, ConnectorDistributedRewriteOperation,
            ConnectorDistributedRewritePlanningRequest, ConnectorTableHandle,
            ConnectorWriteOperationId,
        };
        let table = ConnectorTableHandle::try_new(self.control.binding_key().instance_id.clone(), bytes::Bytes::from_static(br#"{"namespace":"db","table":"t","metadata_location":null,"table_info":null,"metadata_columns":[],"metadata_table_type":null,"prepared_files":[],"explicit_files":null}"#)).unwrap();
        let mut layout = |_| Ok(());
        let mut work = |_| Ok(());
        let mut creation = novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation::new(
            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
            &mut layout,
            &mut work,
        );
        self.rewrite
            .plan_rewrite(
                ConnectorDistributedRewritePlanningRequest::try_new(
                    ConnectorWriteOperationId::new(),
                    self.control.binding_key().clone(),
                    if position {
                        ConnectorDistributedRewriteOperation::RewritePositionDeletes {
                            table,
                            rewrite_all: true,
                            min_input_files: None,
                        }
                    } else {
                        ConnectorDistributedRewriteOperation::RewriteDataFiles {
                            table,
                            rewrite_all: true,
                        }
                    },
                    request_context(),
                )
                .unwrap(),
                &mut creation,
            )
            .unwrap()
    }
    fn request(
        &self,
        plan: &novarocks_spi::connector::ConnectorDistributedRewritePlan,
    ) -> ConnectorWriteBeginRequest {
        let prep = plan.cohorts()[0].preparation();
        ConnectorWriteBeginRequest {
            catalog_session: Default::default(),
            table: Arc::from("db.t"),
            target_ref: prep.target_ref().clone(),
            intent: prep.intent(),
            purpose: ConnectorWriteAdmissionPurpose::OrdinaryDml,
            input: {
                use novarocks_spi::connector::{
                    ConnectorWriteFieldBinding, ConnectorWriteFieldRequest,
                    ConnectorWriteInputRequest,
                };
                let fields = |bindings: &[ConnectorWriteFieldBinding]| {
                    bindings
                        .iter()
                        .map(|binding| {
                            ConnectorWriteFieldRequest::try_new_typed(
                                binding.field().clone(),
                                binding
                                    .execution_type()
                                    .expect("rewrite field declaration")
                                    .clone(),
                            )
                            .expect("frozen writer declaration")
                        })
                        .collect()
                };
                // Preserve the actual provider preparation, as FE does. In
                // particular, delete artifacts are not ordinary data fields.
                match prep.input() {
                    ConnectorWriteInputShape::Data { fields: input } => {
                        ConnectorWriteInputRequest::Data {
                            fields: fields(input),
                        }
                    }
                    ConnectorWriteInputShape::RowLineage {
                        data_fields,
                        row_identity_fields,
                    } => ConnectorWriteInputRequest::RowLineage {
                        data_fields: fields(data_fields),
                        row_identity_fields: fields(row_identity_fields),
                    },
                    ConnectorWriteInputShape::PositionDelete {
                        identity_fields,
                        partition_source_fields,
                    } => ConnectorWriteInputRequest::PositionDelete {
                        identity_fields: fields(identity_fields),
                        partition_source_fields: fields(partition_source_fields),
                    },
                    ConnectorWriteInputShape::DeletionVector {
                        identity_fields,
                        partition_source_fields,
                    } => ConnectorWriteInputRequest::DeletionVector {
                        identity_fields: fields(identity_fields),
                        partition_source_fields: fields(partition_source_fields),
                    },
                    ConnectorWriteInputShape::EqualityDelete { equality_fields } => {
                        ConnectorWriteInputRequest::EqualityDelete {
                            equality_fields: fields(equality_fields),
                        }
                    }
                }
            },
            base: None,
            flavor: ConnectorWriteSessionFlavor::DistributedRewrite {
                shape: plan.shape(),
                frozen: plan.frozen().clone(),
            },
            context: request_context(),
        }
    }
    fn write_rows(
        &self,
        intent: novarocks_spi::connector::ConnectorWriteIntent,
        rows: &[i64],
        name: &str,
    ) {
        use novarocks_spi::connector::write_stack::{
            ConnectorWriteControl, ConnectorWriteFinishPublication, ConnectorWriteFinishRequest,
        };
        let request = ConnectorWriteBeginRequest {
            catalog_session: Default::default(),
            table: Arc::from("db.t"),
            target_ref: novarocks_spi::connector::ConnectorWriteTargetRef::main(),
            intent,
            purpose: ConnectorWriteAdmissionPurpose::OrdinaryDml,
            input: novarocks_spi::connector::ConnectorWriteInputRequest::Data {
                fields: vec![
                    novarocks_spi::connector::ConnectorWriteFieldRequest::try_new_typed(
                        Field::new("id", DataType::Int64, false),
                        novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
                    )
                    .expect("fixture declares required Int64 id"),
                ],
            },
            base: None,
            flavor: ConnectorWriteSessionFlavor::Ordinary,
            context: request_context(),
        };
        let session = self.control.begin_write(request).unwrap();
        let row_count = self.output(&session, rows, name);
        let result = self
            .control
            .finish_write(ConnectorWriteFinishRequest {
                commit: session.commit_session(),
                row_count,
                statistics: Vec::new(),
                publication: ConnectorWriteFinishPublication::None,
                context: request_context(),
            })
            .unwrap();
        assert!(
            matches!(result, ExternalMutationOutcome::KnownCommitted { .. }),
            "{result:?}"
        );
    }
    pub(super) fn output(
        &self,
        session: &novarocks_spi::connector::write_stack::ConnectorWriteSessionPlan,
        rows: &[i64],
        _name: &str,
    ) -> u64 {
        let target = &session.targets()[0];
        let ConnectorWriteInputShape::Data { fields } = target.input() else {
            panic!("fixture data writer input");
        };
        let schema = Arc::new(Schema::new(
            fields
                .iter()
                .map(|field| field.field().clone())
                .collect::<Vec<_>>(),
        ));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(rows.to_vec()))]).unwrap();
        self.stage_batch(session, target, batch, None, rows.len() as u64)
    }

    fn stage_batch(
        &self,
        session: &novarocks_spi::connector::write_stack::ConnectorWriteSessionPlan,
        target: &ConnectorWriteTargetPlan,
        batch: RecordBatch,
        source: Option<novarocks_spi::connector::write_stack::ConnectorPositionShardSource>,
        row_count: u64,
    ) -> u64 {
        use novarocks_spi::connector::write_stack::{
            ConnectorOpenWriterRequest, ConnectorWriteExecution, ConnectorWriterPhysicalContext,
        };
        let execution = super::execution::IcebergWriteStackExecution::new(
            session.commit_session().binding().catalog_handle().clone(),
            self.adapter.clone(),
            self.runtime.resources().planning_binding().clone(),
        );
        let physical = ConnectorWriterPhysicalContext::new(
            *uuid::Uuid::now_v7().as_bytes(),
            0,
            [2; 16],
            0,
            target.ordinal().get(),
        );
        let query = write_query_identity();
        self.control
            .begin_write_query(
                session.commit_session(),
                &query,
                &[target.ordinal()],
                Some(super::test_support::received_write_proof(
                    target.ordinal(),
                    physical,
                )),
            )
            .unwrap();
        let fragments = self.executor.block_on(async {
            let mut writer = execution
                .open_writer(ConnectorOpenWriterRequest {
                    resources: Some(super::test_support::writer_resources()),
                    handle: target.handle().clone(),
                    target: target.ordinal(),
                    expected_schema: batch.schema(),
                    physical,
                    context: request_context(),
                })
                .await
                .unwrap();
            if let Some(source) = source {
                writer.append_position_shards(source, batch).await.unwrap();
            } else {
                writer.append(batch).await.unwrap();
            }
            writer.finish().await.unwrap()
        });
        let adapter = self.control.adapter_for_test();
        let (fragments, author) = super::test_support::paid_fragments(adapter, fragments);
        self.control
            .arrive_write_fragments(
                session.commit_session(),
                &query,
                fragments
                    .into_iter()
                    .map(|fragment| (target.ordinal(), fragment))
                    .collect(),
                &author,
            )
            .unwrap();
        self.control
            .confirm_write_received(session.commit_session(), &query)
            .unwrap();
        row_count
    }

    fn seed_position_vector(&self, file: &DataFileWithStats) {
        use arrow::array::{BinaryArray, Int32Array, StructArray};
        use novarocks_spi::connector::ConnectorWriteInputRequest;
        use novarocks_spi::connector::write_stack::{
            ConnectorPositionShardSource, ConnectorWriteFinishPublication,
            ConnectorWriteFinishRequest,
        };
        let table = self.table();
        let context = request_context();
        let snapshot = table.metadata().current_snapshot_id();
        let shape = ConnectorWriteInputRequest::DeletionVector {
            identity_fields: Vec::new(),
            partition_source_fields: Vec::new(),
        };
        let input = crate::commit::write_preparation::freeze_position_mutation_input(
            table.metadata(),
            "main",
            snapshot,
            &shape,
            &context,
        )
        .unwrap();
        let base = crate::commit::write_shared::position_mutation_base(
            table.metadata(),
            table.metadata_location(),
            "main",
            snapshot,
        )
        .unwrap();
        let session = self
            .control
            .begin_write(ConnectorWriteBeginRequest {
                catalog_session: Default::default(),
                table: Arc::from("db.t"),
                target_ref: novarocks_spi::connector::ConnectorWriteTargetRef::main(),
                intent: novarocks_spi::connector::ConnectorWriteIntent::RowDelta,
                purpose: ConnectorWriteAdmissionPurpose::OrdinaryDml,
                input,
                base: Some(base),
                flavor: ConnectorWriteSessionFlavor::RowMutation,
                context,
            })
            .unwrap();
        let [target] = session.targets() else {
            panic!("a delete-only fixture must seal one vector writer");
        };
        let group = self
            .adapter
            .writer_handle(target.handle())
            .unwrap()
            .position_group()
            .unwrap();
        let source = ConnectorPositionShardSource::new(group, [3; 16], 1.try_into().unwrap());
        let mut bitmap = Vec::new();
        roaring::RoaringBitmap::from_iter([0])
            .serialize_into(&mut bitmap)
            .unwrap();
        let batch = RecordBatch::try_from_iter(vec![
            (
                "_file",
                Arc::new(StringArray::from(vec![file.path.as_str()])) as arrow::array::ArrayRef,
            ),
            (
                "_spec_id",
                Arc::new(Int32Array::from(vec![file.partition_spec_id.unwrap()])),
            ),
            (
                "_partition",
                Arc::new(StructArray::new_empty_fields(1, None)),
            ),
            ("driver", Arc::new(Int64Array::from(vec![0]))),
            ("high_word", Arc::new(Int32Array::from(vec![0]))),
            ("container_key", Arc::new(Int32Array::from(vec![0]))),
            ("shard_index", Arc::new(Int64Array::from(vec![0]))),
            ("shard_count", Arc::new(Int64Array::from(vec![1]))),
            (
                "positions",
                Arc::new(BinaryArray::from(vec![bitmap.as_slice()])),
            ),
        ])
        .unwrap();
        let row_count = self.stage_batch(&session, target, batch, Some(source), 1);
        let result = self
            .control
            .finish_write(ConnectorWriteFinishRequest {
                commit: session.commit_session(),
                row_count,
                statistics: Vec::new(),
                publication: ConnectorWriteFinishPublication::None,
                context: request_context(),
            })
            .unwrap();
        assert!(
            matches!(result, ExternalMutationOutcome::KnownCommitted { .. }),
            "{result:?}"
        );
    }
    pub(super) fn rows(&self) -> Vec<i64> {
        let table = self.table();
        let files = self
            .executor
            .block_on(crate::manifest::extract_data_files_with_stats(&table))
            .unwrap();
        let mut rows = Vec::new();
        for file in files {
            let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
                std::fs::File::open(file.path.trim_start_matches("file://")).unwrap(),
            )
            .unwrap()
            .build()
            .unwrap();
            for batch in reader {
                let batch = batch.unwrap();
                rows.extend(
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .values()
                        .iter()
                        .copied(),
                );
            }
        }
        rows.sort();
        rows
    }
    fn inventory(&self) -> Vec<std::path::PathBuf> {
        fn walk(root: &std::path::Path, paths: &mut Vec<std::path::PathBuf>) {
            for entry in std::fs::read_dir(root).unwrap() {
                let entry = entry.unwrap();
                assert_ne!(entry.file_name(), "_novarocks");
                paths.push(entry.path());
                if entry.file_type().unwrap().is_dir() {
                    walk(&entry.path(), paths);
                }
            }
        }
        let table = self.table();
        let mut paths = Vec::new();
        walk(
            std::path::Path::new(table.metadata().location().trim_start_matches("file://")),
            &mut paths,
        );
        paths.sort();
        paths
    }
}

#[path = "tests/baseline_contract_tests.rs"]
mod baseline_contract_tests;

#[test]
fn distributed_rewrite_inputs_are_attempt_owned_and_never_written() {
    let lake = FrozenRewriteLake::new();
    for position in [false, true] {
        let before = lake.inventory();
        let plan = lake.plan(position);
        assert!(plan.cohorts().is_empty());
        assert_eq!(lake.inventory(), before);
    }
    lake.write_rows(
        novarocks_spi::connector::ConnectorWriteIntent::Append,
        &[1],
        "a",
    );
    lake.write_rows(
        novarocks_spi::connector::ConnectorWriteIntent::Append,
        &[2],
        "b",
    );
    let before = lake.inventory();
    let plan = lake.plan(false);
    assert_eq!(lake.inventory(), before);
    let content = plan
        .frozen()
        .value::<crate::distributed_rewrite::FrozenRewritePlanContent>("rewrite-local")
        .unwrap();
    let weak = Arc::downgrade(content);
    assert_eq!(content.groups.len(), plan.cohorts().len());
    for (group, cohort) in content.groups.iter().zip(plan.cohorts()) {
        assert_eq!(
            crate::distributed_rewrite::decode_digest(&group.group_digest_hex).unwrap(),
            cohort.group_digest()
        );
    }
    assert!(
        content
            .validate_identity(plan.operation_id(), &"ff".repeat(32))
            .is_err()
    );
    assert!(
        content
            .validate_identity(
                novarocks_spi::connector::ConnectorWriteOperationId::new(),
                &content
                    .manifest_digest
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect::<String>()
            )
            .is_err()
    );
    assert_eq!(
        content.group(&"ff".repeat(32)).unwrap_err().kind(),
        ConnectorErrorKind::Internal
    );
    let cloned = plan.clone();
    drop(plan);
    assert!(weak.upgrade().is_some());
    drop(cloned);
    assert!(weak.upgrade().is_none());
    assert_eq!(lake.inventory(), before);
}

#[test]
fn distributed_rewrite_refuses_a_head_that_moved_before_begin_without_writing() {
    use novarocks_spi::connector::write_stack::ConnectorWriteControl;
    let lake = FrozenRewriteLake::new();
    lake.write_rows(
        novarocks_spi::connector::ConnectorWriteIntent::Append,
        &[1],
        "old",
    );
    let shared_context = request_context();
    // Production FE clones this exact context across metadata, planning and begin.
    let observed = lake
        .runtime
        .load_table_for_request("db", "t", &shared_context)
        .unwrap();
    let plan = lake.plan(false);
    assert_eq!(
        observed.table.metadata().current_snapshot_id(),
        plan.frozen()
            .value::<crate::distributed_rewrite::FrozenRewritePlanContent>("rewrite-local")
            .unwrap()
            .base_snapshot_id
    );
    lake.write_rows(
        novarocks_spi::connector::ConnectorWriteIntent::Overwrite,
        &[99],
        "concurrent",
    );
    let before = lake.inventory();
    let mut request = lake.request(&plan);
    request.context = shared_context.clone();
    assert_eq!(
        lake.runtime
            .load_table_for_request("db", "t", &shared_context)
            .unwrap()
            .table
            .metadata()
            .current_snapshot_id(),
        observed.table.metadata().current_snapshot_id(),
        "the cached planning observation intentionally remains S0"
    );
    let error = lake.control.begin_write(request).unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    assert!(error.message().contains("moved after planning"));
    assert_eq!(lake.inventory(), before);
    assert_eq!(lake.rows(), vec![99]);
}

#[test]
fn distributed_rewrite_replaces_only_frozen_inputs_and_rejects_concurrent_commit() {
    use novarocks_spi::connector::write_stack::{
        ConnectorWriteControl, ConnectorWriteFinishPublication, ConnectorWriteFinishRequest,
    };
    for concurrent in [false, true] {
        let lake = FrozenRewriteLake::new();
        lake.write_rows(
            novarocks_spi::connector::ConnectorWriteIntent::Append,
            &[1, 2],
            "old",
        );
        let plan = lake.plan(false);
        let session = lake.control.begin_write(lake.request(&plan)).unwrap();
        for (target, cohort) in session.targets().iter().zip(plan.cohorts()) {
            assert_eq!(target.rewrite_group_digest(), Some(cohort.group_digest()));
        }
        let handle = lake
            .adapter
            .commit_session(session.commit_session())
            .unwrap();
        let selected = crate::commit::write_stack::control::selected_rewrite_files(handle).unwrap();
        let content = plan
            .frozen()
            .value::<crate::distributed_rewrite::FrozenRewritePlanContent>("rewrite-local")
            .unwrap();
        assert_eq!(
            selected.data_paths,
            content
                .groups
                .iter()
                .flat_map(|g| g.data_files.iter().map(|f| EntryIdentity::DataFile {
                    path: f.path.clone()
                }))
                .collect()
        );
        let row_count = lake.output(&session, &[1, 2], "rewritten");
        if concurrent {
            lake.write_rows(
                novarocks_spi::connector::ConnectorWriteIntent::Overwrite,
                &[99],
                "concurrent",
            );
        }
        let result = lake
            .control
            .finish_write(ConnectorWriteFinishRequest {
                commit: session.commit_session(),
                row_count,
                statistics: Vec::new(),
                publication: ConnectorWriteFinishPublication::None,
                context: request_context(),
            })
            .unwrap();
        if concurrent {
            assert!(
                matches!(result, ExternalMutationOutcome::KnownUncommitted { .. }),
                "{result:?}"
            );
            assert_eq!(lake.rows(), vec![99]);
        } else {
            assert!(
                matches!(result, ExternalMutationOutcome::KnownCommitted { .. }),
                "{result:?}"
            );
            assert_eq!(lake.rows(), vec![1, 2]);
        }
        lake.inventory();
    }
}

#[test]
fn distributed_rewrite_refuses_foreign_missing_and_mismatched_frozen_inputs() {
    let lake = FrozenRewriteLake::new();
    lake.write_rows(
        novarocks_spi::connector::ConnectorWriteIntent::Append,
        &[1],
        "old",
    );
    let plan = lake.plan(false);
    let before = lake.inventory();
    let original = plan
        .frozen()
        .value::<crate::distributed_rewrite::FrozenRewritePlanContent>("rewrite-local")
        .unwrap();
    for case in 0..8 {
        let mut content = original.as_ref().clone();
        let mut owner = lake.control.binding_key().clone();
        match case {
            0 => owner.instance_id = ConnectorInstanceId::parse("foreign").unwrap(),
            1 => {}
            2 => content.artifact.namespace = "other".to_string(),
            3 => content.artifact.operation_kind = "other".to_string(),
            4 => content.artifact.target_ref = "other".to_string(),
            5 => content.artifact.groups.clear(),
            6 => content.artifact.table_uuid = "other".to_string(),
            7 => content.artifact.base_snapshot_id = None,
            _ => unreachable!(),
        }
        let frozen = if case == 1 {
            novarocks_spi::connector::ConnectorProcessLocalPayload::new(owner, Arc::new(()))
        } else {
            novarocks_spi::connector::ConnectorProcessLocalPayload::new(owner, Arc::new(content))
        };
        let mut request = lake.request(&plan);
        request.flavor = ConnectorWriteSessionFlavor::DistributedRewrite {
            shape: plan.shape(),
            frozen,
        };
        let error = lake.control.begin_write(request).unwrap_err();
        assert_eq!(
            error.kind(),
            if case == 0 || case >= 6 {
                ConnectorErrorKind::InvalidRequest
            } else {
                ConnectorErrorKind::Internal
            },
            "case {case}: {error}"
        );
        assert_eq!(lake.inventory(), before);
    }
    assert_eq!(lake.rows(), vec![1]);
}

#[test]
fn distributed_rewrite_position_inputs_reach_real_table_execute_splits_without_artifacts() {
    use novarocks_spi::connector::read_stack::{
        ConnectorReadConstraint, ConnectorReadDynamicFilterSnapshot,
        ConnectorReadFrozenRewriteGroup, ConnectorReadRequestControlFactory,
        ConnectorReadTableExecuteProcedure, ConnectorSession, SchemaTableName,
    };
    use novarocks_spi::connector::{
        ConnectorFrozenRewriteGroup, ConnectorPlanningContext, ConnectorProcessLocalPayload,
        ConnectorRewriteCohortRead,
    };
    let lake = FrozenRewriteLake::new_version(crate::iceberg::spec::FormatVersion::V3);
    lake.write_rows(
        novarocks_spi::connector::ConnectorWriteIntent::Append,
        &[1, 2],
        "first",
    );
    lake.write_rows(
        novarocks_spi::connector::ConnectorWriteIntent::Append,
        &[3, 4],
        "second",
    );
    let table = lake.table();
    let files = lake
        .executor
        .block_on(crate::manifest::extract_data_files_with_stats(&table))
        .unwrap();
    for file in &files {
        lake.seed_position_vector(file);
    }
    let before = lake.inventory();
    let plan = lake.plan(true);
    assert_eq!(plan.cohorts().len(), 2);
    assert_eq!(lake.inventory(), before);
    // The real planner freezes file_path/pos artifact identities. Enter C1
    // using that exact preparation, rather than testing only its read splits.
    // These requests must never gain ordinary scan-metadata alias authority.
    let source_snapshot = lake.table().metadata().current_snapshot_id();
    for case in 0..11 {
        let mut request = lake.request(&plan);
        let novarocks_spi::connector::ConnectorWriteInputRequest::DeletionVector {
            identity_fields,
            ..
        } = &mut request.input
        else {
            panic!("frozen position-delete preparation")
        };
        match case {
            0 => {
                identity_fields.pop();
            }
            1 => {
                identity_fields[0] =
                    novarocks_spi::connector::ConnectorWriteFieldRequest::try_new_typed(
                        Field::new("_file", DataType::Utf8, false),
                        novarocks_type_contract::FunctionValueType::new(DataType::Utf8, false),
                    )
                    .unwrap();
            }
            2 => identity_fields.swap(0, 1),
            3 | 4 => {
                let data_type = if case == 3 {
                    DataType::Int32
                } else {
                    DataType::Int64
                };
                let nullable = case == 4;
                identity_fields[1] =
                    novarocks_spi::connector::ConnectorWriteFieldRequest::try_new_typed(
                        Field::new("pos", data_type.clone(), nullable),
                        novarocks_type_contract::FunctionValueType::new(data_type, nullable),
                    )
                    .unwrap();
            }
            5 => request.flavor = ConnectorWriteSessionFlavor::Ordinary,
            6 => {
                request.input = novarocks_spi::connector::ConnectorWriteInputRequest::Data {
                    fields: Vec::new(),
                }
            }
            7..=10 => {
                let content = plan
                    .frozen()
                    .value::<crate::distributed_rewrite::FrozenRewritePlanContent>("rewrite-local")
                    .unwrap();
                let mut content = content.as_ref().clone();
                let mut owner = lake.control.binding_key().clone();
                match case {
                    7 => content.artifact.table_uuid = "replaced".into(),
                    8 => content.artifact.target_ref = "another".into(),
                    9 => content.artifact.base_snapshot_id = None,
                    10 => owner.instance_id = ConnectorInstanceId::parse("foreign").unwrap(),
                    _ => unreachable!(),
                }
                request.flavor = ConnectorWriteSessionFlavor::DistributedRewrite {
                    shape: plan.shape(),
                    frozen: ConnectorProcessLocalPayload::new(owner, Arc::new(content)),
                };
            }
            _ => unreachable!(),
        }
        let error = lake.control.begin_write(request).unwrap_err();
        assert_eq!(
            error.kind(),
            if case == 8 {
                ConnectorErrorKind::Internal
            } else {
                ConnectorErrorKind::InvalidRequest
            },
            "case {case}: {error}"
        );
        assert_eq!(
            lake.inventory(),
            before,
            "rejected begin {case} wrote an artifact"
        );
        assert_eq!(
            lake.table().metadata().current_snapshot_id(),
            source_snapshot
        );
    }
    let writer = lake.control.begin_write(lake.request(&plan)).unwrap();
    assert_eq!(writer.targets().len(), plan.cohorts().len());
    for target in writer.targets() {
        assert_eq!(
            lake.adapter
                .writer_handle(target.handle())
                .unwrap()
                .branch(),
            IcebergWriteBranch::DeletionVector
        );
        let ConnectorWriteInputShape::DeletionVector {
            identity_fields,
            partition_source_fields,
        } = target.input()
        else {
            panic!("position-delete writer lost its artifact input")
        };
        assert!(partition_source_fields.is_empty());
        assert_eq!(identity_fields.len(), 2);
        for (binding, (name, data_type)) in identity_fields
            .iter()
            .zip([("file_path", DataType::Utf8), ("pos", DataType::Int64)])
        {
            assert_eq!(binding.field(), &Field::new(name, data_type.clone(), false));
            assert_eq!(
                binding.execution_type().unwrap(),
                &novarocks_type_contract::FunctionValueType::new(data_type, false)
            );
        }
    }
    assert_eq!(
        lake.inventory(),
        before,
        "C1 begin must not write artifacts"
    );
    let aborted = lake
        .control
        .abort_write(
            novarocks_spi::connector::write_stack::ConnectorWriteSessionAbortRequest {
                commit: writer.commit_session(),
                production: novarocks_spi::connector::write_stack::ConnectorWriteAbortProduction::NotStarted,
                context: request_context(),
            },
        )
        .unwrap();
    assert!(matches!(
        aborted,
        ConnectorWriteAbortOutcome::KnownUncommitted { .. }
    ));
    drop(writer);
    assert_eq!(lake.inventory(), before);
    assert_eq!(
        lake.table().metadata().current_snapshot_id(),
        source_snapshot
    );
    let descriptor = descriptor("rewrite-local");
    let catalog = CatalogHandle::new(
        descriptor.instance_id.clone(),
        CatalogVersion::from_bytes([6; 32]),
    );
    let boundary = Arc::new(crate::typed_boundary::IcebergTypedBoundary::new(
        descriptor,
        lake.control.binding_key().incarnation,
        catalog,
        crate::typed_read::HiveTransactionHandle::new(true, [1; 16]),
        lake.runtime.clone(),
    ));
    let factory = crate::typed_boundary::IcebergTypedRequestControlFactory::new(boundary);
    let control = factory
        .for_planning(&ConnectorPlanningContext::try_from_request(request_context()).unwrap())
        .unwrap();
    let metadata = control.metadata();
    let split_manager = control.splits();
    let session = ConnectorSession::try_new(
        "rewrite-test",
        "user",
        "UTC",
        "en_US",
        std::time::SystemTime::now(),
    )
    .unwrap();
    let name = SchemaTableName::try_new("db", "t").unwrap();
    let content = plan
        .frozen()
        .value::<crate::distributed_rewrite::FrozenRewritePlanContent>("rewrite-local")
        .unwrap();
    let weak = Arc::downgrade(content);
    for cohort in plan.cohorts() {
        let ConnectorRewriteCohortRead::DeleteArtifactGroup(group) = cohort.read() else {
            panic!("position rewrite")
        };
        let procedure = ConnectorReadTableExecuteProcedure::RewritePositionDeleteFiles(
            ConnectorReadFrozenRewriteGroup::from_cohort(group, cohort.group_digest()),
        );
        let opened = metadata
            .get_table_execute_plan(&session, &name, procedure)
            .unwrap()
            .unwrap();
        let mut original_split_owner = None;
        let mut source = lake
            .executor
            .block_on(split_manager.get_splits(
                novarocks_spi::connector::read_stack::ConnectorReadSplitOpenRequest::new(
                    session.clone(),
                    opened,
                    Vec::new(),
                    std::collections::BTreeSet::new(),
                    ConnectorReadConstraint::of_summary(
                        novarocks_spi::connector::read_stack::TupleDomain::all(),
                    ),
                ),
                &mut original_split_owner,
            ))
            .unwrap();
        let batch = source
            .next_batch(1, &ConnectorReadDynamicFilterSnapshot::all_complete())
            .unwrap();
        assert_eq!(batch.splits().len(), 1);
        assert!(source.is_finished());
        source.close().unwrap();
        assert_eq!(lake.inventory(), before);
    }
    let group = match plan.cohorts()[0].read() {
        ConnectorRewriteCohortRead::DeleteArtifactGroup(g) => g,
        _ => unreachable!(),
    };
    for case in 0..9 {
        let mut owner = lake.control.binding_key().clone();
        let mut private = content.as_ref().clone();
        let mut op = group.operation_id();
        let mut digest = group.artifact_digest();
        let mut group_digest = plan.cohorts()[0].group_digest();
        match case {
            0 => owner.instance_id = ConnectorInstanceId::parse("foreign").unwrap(),
            1 => {}
            2 => op = novarocks_spi::connector::ConnectorWriteOperationId::new(),
            3 => digest = [255; 32],
            4 => group_digest = [255; 32],
            5 => private.artifact.table_uuid = "different".to_string(),
            6 => private.artifact.schema_id += 1,
            7 => private.artifact.base_snapshot_id = None,
            8 => owner.incarnation = ProviderBindingEpoch::new(),
            _ => unreachable!(),
        }
        let payload = if case == 1 {
            ConnectorProcessLocalPayload::new(owner, Arc::new(()))
        } else {
            ConnectorProcessLocalPayload::new(owner, Arc::new(private))
        };
        let wrong = ConnectorFrozenRewriteGroup::try_new("db", "t", op, digest, payload).unwrap();
        let error = metadata
            .get_table_execute_plan(
                &session,
                &name,
                ConnectorReadTableExecuteProcedure::RewritePositionDeleteFiles(
                    ConnectorReadFrozenRewriteGroup::from_cohort(&wrong, group_digest),
                ),
            )
            .unwrap_err();
        assert_eq!(
            error.kind(),
            if (1..5).contains(&case) {
                ConnectorErrorKind::Internal
            } else {
                ConnectorErrorKind::InvalidRequest
            },
            "case {case}: {error}"
        );
        if case == 8 {
            assert_eq!(
                error.message(),
                "process-local payload belongs to another connector generation"
            );
        }
        assert_eq!(lake.inventory(), before);
    }
    drop(plan);
    assert!(
        weak.upgrade().is_none(),
        "read and split scopes must release content after their actual exit"
    );
}

fn project_prepared_managed_rows(
    technique: ConnectorManagedPublicationTechnique,
    shape: ConnectorManagedPublicationShape,
    entries: Vec<(WriteTargetOrdinal, IcebergCommitFragment)>,
) -> Result<Option<novarocks_spi::connector::write_stack::ConnectorWriteRowChange>, ConnectorError>
{
    project_managed_rows_fixture(technique, shape, entries, false).map(|(_, rows)| rows)
}

pub(super) fn project_managed_rows_fixture(
    technique: ConnectorManagedPublicationTechnique,
    shape: ConnectorManagedPublicationShape,
    entries: Vec<(WriteTargetOrdinal, IcebergCommitFragment)>,
    use_legacy_receiver: bool,
) -> Result<
    (
        bool,
        Option<novarocks_spi::connector::write_stack::ConnectorWriteRowChange>,
    ),
    ConnectorError,
> {
    let material = session_material(if shape == ConnectorManagedPublicationShape::RowMutation {
        merge_on_read_input_shape()
    } else {
        data_input_shape()
    });
    let (handle, _) = flavor_session(plan_document_publication_branches(
        &material,
        document_publication_facts_with_shape(
            technique,
            ConnectorManagedPublicationEmptyInputDisposition::CommitEmptyWrite,
            shape,
        ),
    )?);
    let handle = if shape == ConnectorManagedPublicationShape::RowMutation {
        use crate::commit::write_stack::domain::{
            IcebergMutationReadRecipe, IcebergMutationSnapshot,
        };
        let snapshot = handle
            .table()
            .base_snapshot_id()
            .map(|_| IcebergMutationSnapshot::try_new("s3://b/list.avro".into(), 1).unwrap());
        let recipe = IcebergMutationReadRecipe::try_new(
            handle.table(),
            "s3://b/table.metadata.json".into(),
            format!("sha256:{}", "a".repeat(64)),
            snapshot,
        )?;
        handle.with_mutation_read(recipe)
    } else {
        handle
    };
    let chooses_compact = handle.with_active_receiving(|receiving| {
        let compact = receiving.compact_files().is_some();
        if use_legacy_receiver {
            *receiving = IcebergReceiving::default();
        }
        Ok(compact)
    })?;
    let expected = handle.expected_targets();
    let adapter = adapter("unit", 1);
    let commit = adapter.wrap_commit_session(handle);
    // A logical row projection has no affected-row input. Exercise the same
    // measured decoder used by the original writer output.
    let (ordinals, fragments): (Vec<_>, Vec<_>) = entries
        .into_iter()
        .map(|(ordinal, fragment)| (ordinal, adapter.wrap_commit_fragment(fragment)))
        .unzip();
    let (fragments, author) = super::test_support::paid_fragments(&adapter, fragments);
    let (_executor, runtime) = unreachable_rest_runtime();
    let control = crate::commit::write_stack::control::IcebergWriteSessionControl::new(
        descriptor("unit"),
        ProviderBindingEpoch::new(),
        CatalogHandle::new(
            ConnectorInstanceId::parse("unit").unwrap(),
            CatalogVersion::from_bytes([1; 32]),
        ),
        runtime,
    );
    let query = super::test_support::write_query_identity();
    control.begin_write_query(&commit, &query, &expected, None)?;
    control.arrive_write_fragments(
        &commit,
        &query,
        ordinals.into_iter().zip(fragments).collect(),
        &author,
    )?;
    control.confirm_write_received(&commit, &query)?;
    let result = control.received_logical_row_change(&commit, &request_context());
    assert!(matches!(
        adapter.commit_session(&commit)?.state()?,
        IcebergWriteSessionState::Active
    ));
    result.map(|rows| (chooses_compact, rows))
}

fn logical_data_fragment(path: &str, rows: u64) -> IcebergCommitFragment {
    IcebergCommitFragment::data_file(
        IcebergDataFileArtifact::try_new(
            path.into(),
            IcebergFileFormat::Parquet,
            sample_partition(),
            sample_metrics(rows, 4096),
            None,
        )
        .unwrap(),
    )
}

pub(super) fn logical_final_delete(
    data: &str,
    new_rows: u64,
    total_rows: u64,
    replace: Option<(&str, u64)>,
) -> Result<IcebergCommitFragment, ConnectorError> {
    use crate::commit::write_stack::domain::{
        IcebergContentRange, IcebergDeleteTargetLocator, IcebergDeletionVectorArtifact,
        IcebergFinalDeleteEffect, IcebergFinalDeleteKind,
    };
    use novarocks_spi::connector::write_stack::ConnectorWriterPhysicalContext;
    let target = IcebergDeleteTargetLocator::try_new(
        "s3://b/list.avro".into(),
        "s3://b/source.avro".into(),
        0,
        0,
        1,
        data.into(),
        total_rows,
    )?;
    let kind = match replace {
        None => IcebergFinalDeleteKind::RemoveDataFile {
            partition: sample_partition(),
            old_exclusive_deletes: Box::new([]),
        },
        Some((path, rows)) => IcebergFinalDeleteKind::ReplacePositions(Box::new(
            IcebergDeletionVectorArtifact::try_new(
                path.into(),
                sample_partition(),
                sample_metrics(rows, 4096),
                data.into(),
                IcebergContentRange::try_new(4, 64)?,
                rows,
                vec![],
            )?,
        )),
    };
    let fragment = IcebergCommitFragment::final_delete(IcebergFinalDeleteEffect::try_new(
        target, new_rows, kind,
    )?);
    if replace.is_some() {
        // This fixture has no managed creation scope. The wire representation
        // still requires structurally complete creation evidence.
        let physical = ConnectorWriterPhysicalContext::new([1; 16], 1, [2; 16], 0, 1);
        fragment.try_with_creation(super::domain::IcebergCreationReceipt::try_new(
            physical, [3; 16], [4; 32], 4096, [5; 32],
        )?)
    } else {
        Ok(fragment)
    }
}

#[test]
fn managed_logical_rows_count_newly_deleted_rows_from_both_final_effects() {
    use novarocks_spi::connector::write_stack::ConnectorWriteRowChange::Delta;

    let a = logical_final_delete("s3://b/a.parquet", 3, 9, Some(("s3://b/a.puffin", 5))).unwrap();
    let b = logical_final_delete("s3://b/b.parquet", 4, 4, None).unwrap();
    let insert = logical_data_fragment("s3://b/new.parquet", 5);
    let project = |entries| {
        project_prepared_managed_rows(
            ConnectorManagedPublicationTechnique::Incremental,
            ConnectorManagedPublicationShape::RowMutation,
            entries,
        )
    };
    assert_eq!(
        project(vec![
            (ordinal(0), insert),
            (ordinal(1), a.clone()),
            (ordinal(1), b)
        ])
        .unwrap(),
        Some(Delta {
            inserted: 5,
            deleted: 7
        }),
    );
    assert!(
        project(vec![(ordinal(1), a.clone()), (ordinal(1), a)])
            .unwrap_err()
            .message()
            .contains("repeats staged artifact")
    );
    assert!(logical_final_delete("s3://b/a.parquet", 10, 9, None).is_err());
    assert!(logical_final_delete("s3://b/a.parquet", 3, 9, Some(("s3://b/a.puffin", 2))).is_err());
}

#[test]
fn managed_logical_rows_refuse_insertion_count_overflow() {
    let error = project_prepared_managed_rows(
        ConnectorManagedPublicationTechnique::Full,
        ConnectorManagedPublicationShape::Data,
        ["a", "b", "c"]
            .into_iter()
            .map(|name| {
                (
                    ordinal(0),
                    logical_data_fragment(&format!("s3://b/{name}.parquet"), i64::MAX as u64),
                )
            })
            .collect(),
    )
    .unwrap_err();
    assert!(
        error
            .message()
            .contains("logical insertion count overflowed")
    );
}

#[test]
fn managed_prepared_logical_rows_distinguish_replacement_append_and_empty_delta() {
    use novarocks_spi::connector::write_stack::ConnectorWriteRowChange::{Delta, Replace};
    for (technique, expected) in [
        (ConnectorManagedPublicationTechnique::Full, Replace(12)),
        (
            ConnectorManagedPublicationTechnique::Incremental,
            Delta {
                inserted: 12,
                deleted: 0,
            },
        ),
    ] {
        assert_eq!(
            project_prepared_managed_rows(
                technique,
                ConnectorManagedPublicationShape::Data,
                vec![
                    (ordinal(0), logical_data_fragment("s3://b/new-a.parquet", 5)),
                    (ordinal(0), logical_data_fragment("s3://b/new-b.parquet", 7)),
                ]
            )
            .unwrap(),
            Some(expected)
        );
    }
    for (technique, shape, expected) in [
        (
            ConnectorManagedPublicationTechnique::Full,
            ConnectorManagedPublicationShape::Data,
            Replace(0),
        ),
        (
            ConnectorManagedPublicationTechnique::Incremental,
            ConnectorManagedPublicationShape::RowMutation,
            Delta {
                inserted: 0,
                deleted: 0,
            },
        ),
        (
            ConnectorManagedPublicationTechnique::MetadataOnly,
            ConnectorManagedPublicationShape::Data,
            Delta {
                inserted: 0,
                deleted: 0,
            },
        ),
    ] {
        assert_eq!(
            project_prepared_managed_rows(technique, shape, vec![]).unwrap(),
            Some(expected)
        );
    }
}

#[test]
fn prepared_logical_rows_require_exact_adapter_and_live_context_and_exclude_ordinary_dml() {
    let (_executor, runtime) = unreachable_rest_runtime();
    let control = crate::commit::write_stack::control::IcebergWriteSessionControl::new(
        descriptor("unit"),
        ProviderBindingEpoch::new(),
        CatalogHandle::new(
            ConnectorInstanceId::parse("unit").unwrap(),
            CatalogVersion::from_bytes([1; 32]),
        ),
        runtime,
    );
    let (ordinary, _) = flavor_session(
        plan_ordinary_branches(
            IcebergWriteFlavor::Append,
            &session_material(data_input_shape()),
        )
        .unwrap(),
    );
    let adapter = adapter("unit", 1);
    let commit = adapter.wrap_commit_session(ordinary);
    receive_write_query(&control, &commit, &[ordinal(0)], Vec::new()).unwrap();
    assert_eq!(
        control
            .received_logical_row_change(&commit, &request_context())
            .unwrap(),
        None
    );
    let (other, _) = flavor_session(
        plan_ordinary_branches(
            IcebergWriteFlavor::Append,
            &session_material(data_input_shape()),
        )
        .unwrap(),
    );
    let foreign_adapter = self::adapter("unit", 2);
    let foreign = foreign_adapter.wrap_commit_session(other);
    assert_eq!(
        control
            .received_logical_row_change(&foreign, &request_context())
            .unwrap_err()
            .kind(),
        ConnectorErrorKind::InvalidRequest
    );
    let wrong_generation = receive_write_query(
        &control,
        &commit,
        &[ordinal(0)],
        vec![(
            ordinal(0),
            foreign_adapter.wrap_commit_fragment(data_fragment("s3://b/foreign.parquet")),
        )],
    )
    .unwrap_err();
    assert_eq!(wrong_generation.kind(), ConnectorErrorKind::InvalidRequest);
    let stop = novarocks_spi::connector::ConnectorStopOwner::new();
    let cancelled = ConnectorRequestContext::try_new(
        Instant::now() + Duration::from_secs(30),
        stop.view(),
        65536,
        1048576,
    )
    .unwrap();
    stop.request_stop();
    assert_eq!(
        control
            .received_logical_row_change(&commit, &cancelled)
            .unwrap_err()
            .kind(),
        ConnectorErrorKind::Cancelled
    );
}

#[test]
fn async_write_control_commits_real_files_without_a_catalog_join_bridge() {
    use novarocks_spi::connector::CatalogActivityReader;
    use novarocks_spi::connector::write_stack::{
        ConnectorWriteFinishPublication, ConnectorWriteFinishRequestRef,
    };
    let activity =
        crate::activity::CatalogActivity::new(novarocks_spi::connector::CatalogScopeIdentity::new(
            novarocks_spi::connector::ConnectorProviderId::parse("iceberg").unwrap(),
            novarocks_spi::connector::ConnectorInstanceId::parse("rewrite-local").unwrap(),
            uuid::Uuid::now_v7(),
        ));
    let lake = FrozenRewriteLake::new_version_with_activity(
        crate::iceberg::spec::FormatVersion::V2,
        Some(activity.generation()),
    );
    let bridges = || {
        activity
            .snapshot()
            .phases
            .iter()
            .find(|p| p.phase == novarocks_spi::connector::CatalogActivityPhase::Bridge)
            .unwrap()
            .started
    };
    let before = bridges();
    let request = ConnectorWriteBeginRequest {
        catalog_session: Default::default(),
        table: Arc::from("db.t"),
        target_ref: novarocks_spi::connector::ConnectorWriteTargetRef::main(),
        intent: novarocks_spi::connector::ConnectorWriteIntent::Append,
        purpose: ConnectorWriteAdmissionPurpose::OrdinaryDml,
        input: novarocks_spi::connector::ConnectorWriteInputRequest::Data {
            fields: vec![
                novarocks_spi::connector::ConnectorWriteFieldRequest::try_new_typed(
                    Field::new("id", DataType::Int64, false),
                    novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
                )
                .unwrap(),
            ],
        },
        base: None,
        flavor: ConnectorWriteSessionFlavor::Ordinary,
        context: request_context(),
    };
    let session = lake
        .executor
        .block_on(lake.control.begin_write_async(request))
        .unwrap();
    assert_eq!(
        bridges(),
        before,
        "async begin must not enter the sync bridge"
    );
    let (output, _writer) = sealed_writer_output_for_abort(&lake, &session);
    lake.control
        .confirm_write_received(session.commit_session(), &output.query)
        .unwrap();
    let statistics = Vec::new();
    let publication = ConnectorWriteFinishPublication::None;
    let context = request_context();
    let finish_before = bridges();
    let outcome = lake
        .executor
        .block_on(
            lake.control
                .finish_write_async(ConnectorWriteFinishRequestRef {
                commit: session.commit_session(),
                row_count: output.row_count,
                statistics: &statistics,
                publication: &publication,
                context: &context,
                production:
                    novarocks_spi::connector::write_stack::ConnectorWriteAbortProduction::Unproven,
            }),
        )
        .unwrap();
    assert_eq!(
        bridges(),
        finish_before,
        "async finish must not enter the sync bridge"
    );
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
    assert_eq!(lake.rows(), vec![7, 9]);
    assert_eq!(
        lake.control
            .cleanup_responsibility(session.commit_session())
            .unwrap(),
        novarocks_spi::connector::ConnectorMetadataCleanupResponsibility::Disposed
    );
    // The original packet is borrowed, but its received inventory was already
    // transferred to Provider custody by the root-result arrival.
    assert_eq!(output.paths.len(), 1);
}

pub(super) fn ordinary_async_test_begin(
    context: ConnectorRequestContext,
) -> ConnectorWriteBeginRequest {
    ConnectorWriteBeginRequest {
        catalog_session: Default::default(),
        table: Arc::from("db.t"),
        target_ref: novarocks_spi::connector::ConnectorWriteTargetRef::main(),
        intent: novarocks_spi::connector::ConnectorWriteIntent::Append,
        purpose: ConnectorWriteAdmissionPurpose::OrdinaryDml,
        input: novarocks_spi::connector::ConnectorWriteInputRequest::Data {
            fields: vec![
                novarocks_spi::connector::ConnectorWriteFieldRequest::try_new_typed(
                    Field::new("id", DataType::Int64, false),
                    novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
                )
                .unwrap(),
            ],
        },
        base: None,
        flavor: ConnectorWriteSessionFlavor::Ordinary,
        context,
    }
}

#[test]
fn async_finish_first_poll_cancellation_is_authoritatively_uncommitted() {
    use novarocks_spi::connector::write_stack::{
        ConnectorWriteFinishPublication, ConnectorWriteFinishRequestRef,
    };
    let lake = FrozenRewriteLake::new_version(crate::iceberg::spec::FormatVersion::V2);
    let session = lake
        .executor
        .block_on(
            lake.control
                .begin_write_async(ordinary_async_test_begin(request_context())),
        )
        .unwrap();
    let row_count = lake.output(&session, &[7, 9], "cancel-before-provider-poll");
    let inventory = lake.inventory();
    let stop = novarocks_spi::connector::ConnectorStopOwner::new();
    let context = ConnectorRequestContext::try_new(
        std::time::Instant::now() + std::time::Duration::from_secs(60),
        stop.view(),
        novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        novarocks_spi::connector::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    )
    .unwrap();
    let statistics = Vec::new();
    let publication = ConnectorWriteFinishPublication::None;
    let future = lake
        .control
        .finish_write_async(ConnectorWriteFinishRequestRef {
            commit: session.commit_session(),
            row_count,
            statistics: &statistics,
            publication: &publication,
            context: &context,
            production:
                novarocks_spi::connector::write_stack::ConnectorWriteAbortProduction::Unproven,
        });
    stop.request_stop();
    let outcome = lake.executor.block_on(future).unwrap();
    let ExternalMutationOutcome::KnownUncommitted { failure, cleanup } = outcome else {
        panic!("cancelled before the actual Provider poll cannot dispatch publication")
    };
    assert_eq!(
        failure.kind(),
        novarocks_spi::connector::ConnectorMutationFailureKind::Cancelled
    );
    assert_eq!(
        failure.connector_source().unwrap().kind(),
        ConnectorErrorKind::Cancelled
    );
    assert!(
        matches!(cleanup, ExternalMutationFinalization::Failed(_)),
        "received files are not an empty cleanup inventory"
    );
    let handle = lake
        .adapter
        .commit_session(session.commit_session())
        .unwrap();
    assert!(matches!(
        handle.state().unwrap(),
        IcebergWriteSessionState::KnownUncommitted { .. }
    ));
    assert_eq!(
        lake.inventory(),
        inventory,
        "no metadata object or data deletion was dispatched"
    );
    assert!(lake.rows().is_empty());
    assert_eq!(
        lake.control
            .cleanup_responsibility(session.commit_session())
            .unwrap(),
        novarocks_spi::connector::ConnectorMetadataCleanupResponsibility::Retained
    );
}

#[test]
fn cancelled_abort_retires_only_the_proven_unstarted_empty_session() {
    use novarocks_spi::connector::write_stack::ConnectorWriteAbortProduction as P;
    let lake = FrozenRewriteLake::new_version(crate::iceberg::spec::FormatVersion::V2);
    let session = lake
        .executor
        .block_on(
            lake.control
                .begin_write_async(ordinary_async_test_begin(request_context())),
        )
        .unwrap();
    let inventory = lake.inventory();
    let stop = novarocks_spi::connector::ConnectorStopOwner::new();
    let context = ConnectorRequestContext::try_new(
        std::time::Instant::now() + std::time::Duration::from_secs(60),
        stop.view(),
        novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        novarocks_spi::connector::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    )
    .unwrap();
    stop.request_stop();
    let outcome = lake
        .executor
        .block_on(lake.control.abort_write_async(
            novarocks_spi::connector::write_stack::ConnectorWriteSessionAbortRequest {
                commit: session.commit_session(),
                context,
                production: P::NotStarted,
            },
        ))
        .unwrap();
    assert!(matches!(
        outcome,
        ConnectorWriteAbortOutcome::KnownUncommitted {
            cleanup: ExternalMutationFinalization::Complete,
        }
    ));
    assert_eq!(
        lake.control
            .cleanup_responsibility(session.commit_session())
            .unwrap(),
        novarocks_spi::connector::ConnectorMetadataCleanupResponsibility::Disposed
    );
    assert_eq!(
        lake.inventory(),
        inventory,
        "local retirement does not delete objects"
    );
    assert!(lake.rows().is_empty());
    // The same Provider must retain a second session with unproven production,
    // even if the fixture happens not to have placed a file on disk.
    let unproven = lake
        .executor
        .block_on(
            lake.control
                .begin_write_async(ordinary_async_test_begin(request_context())),
        )
        .unwrap();
    let outcome = lake
        .executor
        .block_on(lake.control.abort_write_async(
            novarocks_spi::connector::write_stack::ConnectorWriteSessionAbortRequest {
                commit: unproven.commit_session(),
                context: request_context(),
                production: P::Unproven,
            },
        ))
        .unwrap();
    assert!(matches!(
        outcome,
        ConnectorWriteAbortOutcome::KnownUncommitted {
            cleanup: ExternalMutationFinalization::Failed(_),
        }
    ));
    assert_eq!(
        lake.control
            .cleanup_responsibility(unproven.commit_session())
            .unwrap(),
        novarocks_spi::connector::ConnectorMetadataCleanupResponsibility::Retained
    );
}

struct TestWriterOutput {
    row_count: u64,
    paths: Vec<String>,
    query: ConnectorWriteQueryIdentity,
}

fn sealed_writer_output_for_abort(
    lake: &FrozenRewriteLake,
    session: &novarocks_spi::connector::write_stack::ConnectorWriteSessionPlan,
) -> (
    TestWriterOutput,
    Box<dyn novarocks_spi::connector::write_stack::ConnectorBatchWriter>,
) {
    seal_test_data_output(
        &lake.control,
        &lake.executor,
        &lake.runtime,
        &lake.adapter,
        session,
    )
}

fn seal_test_data_output(
    control: &dyn ConnectorWriteControl,
    executor: &tokio::runtime::Runtime,
    runtime: &Arc<crate::metadata_context::IcebergMetadataContext>,
    adapter: &IcebergWriteAdapter,
    session: &novarocks_spi::connector::write_stack::ConnectorWriteSessionPlan,
) -> (
    TestWriterOutput,
    Box<dyn novarocks_spi::connector::write_stack::ConnectorBatchWriter>,
) {
    use novarocks_spi::connector::write_stack::{
        ConnectorOpenWriterRequest, ConnectorWriteExecution, ConnectorWriterPhysicalContext,
    };
    let execution = super::execution::IcebergWriteStackExecution::new(
        session.commit_session().binding().catalog_handle().clone(),
        adapter.clone(),
        runtime.resources().planning_binding().clone(),
    );
    let target = &session.targets()[0];
    let novarocks_spi::connector::ConnectorWriteInputShape::Data { fields } = target.input() else {
        panic!("ordinary writer input");
    };
    let schema = Arc::new(Schema::new(
        fields
            .iter()
            .map(|field| field.field().clone())
            .collect::<Vec<_>>(),
    ));
    let physical = ConnectorWriterPhysicalContext::new([1; 16], 1, [2; 16], 0, 0);
    let query = write_query_identity();
    control
        .begin_write_query(
            session.commit_session(),
            &query,
            &[target.ordinal()],
            Some(super::test_support::received_write_proof(
                target.ordinal(),
                physical,
            )),
        )
        .unwrap();
    let mut writer = executor
        .block_on(execution.open_writer(ConnectorOpenWriterRequest {
            resources: Some(super::test_support::writer_resources()),
            handle: target.handle().clone(),
            target: target.ordinal(),
            expected_schema: schema.clone(),
            physical,
            context: request_context(),
        }))
        .unwrap();
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![7, 9]))]).unwrap();
    executor.block_on(writer.append(batch)).unwrap();
    let fragments = executor.block_on(writer.finish()).unwrap();
    let paths = fragments
        .iter()
        .map(|fragment| adapter.commit_fragment(fragment).unwrap().path().to_owned())
        .collect();
    let (fragments, author) = super::test_support::paid_fragments(adapter, fragments);
    control
        .arrive_write_fragments(
            session.commit_session(),
            &query,
            fragments
                .into_iter()
                .map(|fragment| (target.ordinal(), fragment))
                .collect(),
            &author,
        )
        .unwrap();
    (
        TestWriterOutput {
            row_count: 2,
            paths,
            query,
        },
        writer,
    )
}

#[test]
fn complete_received_abort_deletes_only_the_actual_sealed_writer_output() {
    use novarocks_spi::connector::write_stack::{
        ConnectorWriteAbortProduction as P, ConnectorWriteSessionAbortRequest,
    };
    let lake = FrozenRewriteLake::new_version(crate::iceberg::spec::FormatVersion::V2);
    let session = lake
        .executor
        .block_on(
            lake.control
                .begin_write_async(ordinary_async_test_begin(request_context())),
        )
        .unwrap();
    let (output, mut writer) = sealed_writer_output_for_abort(&lake, &session);
    let paths = output.paths;
    assert!(!paths.is_empty());
    // BE-local abort no longer owns successfully sealed output.
    lake.executor.block_on(writer.abort()).unwrap();
    assert!(
        paths
            .iter()
            .all(|path| std::path::Path::new(path.trim_start_matches("file://")).is_file())
    );
    let survivor = lake
        ._warehouse
        .path()
        .join("not-registered-by-this-operation");
    std::fs::write(&survivor, b"not owned").unwrap();
    // Arrival alone does not authorize a complete-received cleanup.
    let rejected = lake.executor.block_on(lake.control.abort_write_async(
        ConnectorWriteSessionAbortRequest {
            commit: session.commit_session(),
            context: request_context(),
            production: P::CompleteReceived,
        },
    ));
    assert!(rejected.is_err());
    assert!(matches!(
        lake.adapter
            .commit_session(session.commit_session())
            .unwrap()
            .state()
            .unwrap(),
        IcebergWriteSessionState::Active
    ));
    lake.control
        .confirm_write_received(session.commit_session(), &output.query)
        .unwrap();
    let stop = novarocks_spi::connector::ConnectorStopOwner::new();
    let context = ConnectorRequestContext::try_new(
        std::time::Instant::now() + std::time::Duration::from_secs(60),
        stop.view(),
        novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        novarocks_spi::connector::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    )
    .unwrap();
    stop.request_stop();
    let outcome = lake
        .executor
        .block_on(
            lake.control
                .abort_write_async(ConnectorWriteSessionAbortRequest {
                    commit: session.commit_session(),
                    context,
                    production: P::CompleteReceived,
                }),
        )
        .unwrap();
    assert!(matches!(
        outcome,
        ConnectorWriteAbortOutcome::KnownUncommitted {
            cleanup: ExternalMutationFinalization::Complete
        }
    ));
    assert!(
        paths
            .iter()
            .all(|path| !std::path::Path::new(path.trim_start_matches("file://")).exists())
    );
    assert!(survivor.is_file(), "cleanup never scans for other objects");
    assert!(lake.rows().is_empty(), "abort did not publish a snapshot");
    let handle = lake
        .adapter
        .commit_session(session.commit_session())
        .unwrap();
    assert_eq!(
        handle.cleanup_responsibility(),
        novarocks_spi::connector::ConnectorMetadataCleanupResponsibility::Disposed
    );
    assert!(
        matches!(handle.early_cleanup_report_for_test(), Some(crate::commit::model::IcebergCleanupReport::Complete { deleted }) if deleted == paths.len())
    );
}

#[test]
fn complete_received_first_poll_refusal_cleans_same_output_without_publication() {
    use novarocks_spi::connector::write_stack::{
        ConnectorWriteAbortProduction as P, ConnectorWriteFinishPublication,
        ConnectorWriteFinishRequestRef,
    };
    let lake = FrozenRewriteLake::new_version(crate::iceberg::spec::FormatVersion::V2);
    let session = lake
        .executor
        .block_on(
            lake.control
                .begin_write_async(ordinary_async_test_begin(request_context())),
        )
        .unwrap();
    let (output, _sealed_writer) = sealed_writer_output_for_abort(&lake, &session);
    lake.control
        .confirm_write_received(session.commit_session(), &output.query)
        .unwrap();
    let paths = output.paths;
    let stop = novarocks_spi::connector::ConnectorStopOwner::new();
    let context = ConnectorRequestContext::try_new(
        std::time::Instant::now() + std::time::Duration::from_secs(60),
        stop.view(),
        novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        novarocks_spi::connector::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    )
    .unwrap();
    let publication = ConnectorWriteFinishPublication::None;
    let future = lake
        .control
        .finish_write_async(ConnectorWriteFinishRequestRef {
            commit: session.commit_session(),
            row_count: output.row_count,
            statistics: &[],
            publication: &publication,
            context: &context,
            production: P::CompleteReceived,
        });
    stop.request_stop();
    let outcome = lake.executor.block_on(future).unwrap();
    let ExternalMutationOutcome::KnownUncommitted { failure, cleanup } = outcome else {
        panic!("negative dispatch proof")
    };
    assert_eq!(
        failure.connector_source().unwrap().kind(),
        ConnectorErrorKind::Cancelled
    );
    assert!(matches!(cleanup, ExternalMutationFinalization::Complete));
    assert!(
        paths
            .iter()
            .all(|path| !std::path::Path::new(path.trim_start_matches("file://")).exists())
    );
    assert!(lake.rows().is_empty());
    assert_eq!(
        lake.adapter
            .commit_session(session.commit_session())
            .unwrap()
            .cleanup_responsibility(),
        novarocks_spi::connector::ConnectorMetadataCleanupResponsibility::Disposed
    );
}

#[test]
fn failed_received_cleanup_keeps_exact_remaining_object_and_retained_responsibility() {
    use novarocks_spi::connector::write_stack::{
        ConnectorWriteAbortProduction as P, ConnectorWriteSessionAbortRequest,
    };
    let lake = FrozenRewriteLake::new_version(crate::iceberg::spec::FormatVersion::V2);
    let session = lake
        .executor
        .block_on(
            lake.control
                .begin_write_async(ordinary_async_test_begin(request_context())),
        )
        .unwrap();
    let (output, _sealed_writer) = sealed_writer_output_for_abort(&lake, &session);
    lake.control
        .confirm_write_received(session.commit_session(), &output.query)
        .unwrap();
    let original = &output.paths[0];
    let path = std::path::Path::new(original.trim_start_matches("file://"));
    std::fs::remove_file(path).unwrap();
    std::fs::create_dir(path).unwrap();
    std::fs::write(path.join("still-present"), b"forced delete failure").unwrap();
    let outcome = lake
        .executor
        .block_on(
            lake.control
                .abort_write_async(ConnectorWriteSessionAbortRequest {
                    commit: session.commit_session(),
                    context: request_context(),
                    production: P::CompleteReceived,
                }),
        )
        .unwrap();
    assert!(matches!(
        outcome,
        ConnectorWriteAbortOutcome::KnownUncommitted {
            cleanup: ExternalMutationFinalization::Failed(_)
        }
    ));
    let handle = lake
        .adapter
        .commit_session(session.commit_session())
        .unwrap();
    let Some(crate::commit::model::IcebergCleanupReport::Partial { remaining, .. }) =
        handle.early_cleanup_report_for_test()
    else {
        panic!("actual cleanup report")
    };
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].object.path(), original);
    assert!(path.join("still-present").is_file());
    assert_eq!(
        handle.cleanup_responsibility(),
        novarocks_spi::connector::ConnectorMetadataCleanupResponsibility::Retained
    );
    assert!(lake.rows().is_empty());
}

fn empty_finish_request<'a>(
    commit: &'a novarocks_spi::connector::write_stack::ConnectorWriteCommitSession,
    context: &'a ConnectorRequestContext,
) -> novarocks_spi::connector::write_stack::ConnectorWriteFinishRequestRef<'a> {
    use novarocks_spi::connector::write_stack::{
        ConnectorWriteAbortProduction, ConnectorWriteFinishPublication,
        ConnectorWriteFinishRequestRef,
    };
    ConnectorWriteFinishRequestRef {
        commit,
        row_count: 0,
        statistics: &[],
        publication: &ConnectorWriteFinishPublication::None,
        context,
        production: ConnectorWriteAbortProduction::CompleteReceived,
    }
}

#[test]
fn empty_artifact_row_deltas_settle_noop_before_any_catalog_io() {
    use novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion;
    let (_executor, runtime) = unreachable_rest_runtime();
    let control = crate::commit::write_stack::control::IcebergWriteSessionControl::new(
        descriptor("unit"),
        ProviderBindingEpoch::new(),
        CatalogHandle::new(
            ConnectorInstanceId::parse("unit").unwrap(),
            CatalogVersion::from_bytes([1; 32]),
        ),
        Arc::clone(&runtime),
    );
    for snapshot in [Some(77), None] {
        for flavor in [
            IcebergWriteFlavor::RowMutationPositionDelete,
            IcebergWriteFlavor::RowMutationDeletionVector,
            IcebergWriteFlavor::EqualityDelete,
        ] {
            let mut material = match flavor {
                IcebergWriteFlavor::RowMutationPositionDelete => {
                    session_material(delete_input_shape(IcebergWriteBranch::PositionDelete))
                }
                IcebergWriteFlavor::RowMutationDeletionVector => {
                    session_material(delete_input_shape(IcebergWriteBranch::DeletionVector))
                }
                _ => {
                    let mut material = session_material(equality_delete_input_shape());
                    material.equality = Some(equality_delete_recipe());
                    material
                }
            };
            material.table = IcebergWriteTableFacts::try_new(
                "6f7c3a0e-0000-4000-8000-000000000001".into(),
                "db".into(),
                "t".into(),
                "s3://b/wh/db/t".into(),
                "s3://b/wh/db/t/data".into(),
                "main".into(),
                snapshot,
                snapshot.map_or(0, |_| 4),
                1,
                0,
                if flavor == IcebergWriteFlavor::RowMutationPositionDelete {
                    2
                } else {
                    3
                },
            )
            .unwrap();
            let position_mutation = flavor != IcebergWriteFlavor::EqualityDelete;
            let plan = if position_mutation {
                plan_row_mutation_branches(&material)
            } else {
                plan_ordinary_branches(flavor, &material)
            }
            .unwrap();
            assert_eq!(plan.flavor, flavor);
            let (handle, _) = plan_branch_session(
                IcebergWriteSessionId::new(),
                IcebergBranchSessionPlanInput {
                    flavor,
                    purpose: ConnectorWriteAdmissionPurpose::OrdinaryDml,
                    table: material.table,
                    base_version_digest: Some([42; 32]),
                    document_publication: None,
                    staged_metadata: None,
                    rewrite_inputs: plan.rewrite_inputs,
                    repartition: None,
                    writer_table: None,
                    branches: plan.branches,
                },
            )
            .unwrap();
            let handle = if position_mutation {
                let recipe = fixed_delete_recipe(handle.table());
                handle.with_mutation_read(recipe)
            } else {
                handle
            };
            let empty = IcebergReceiving::default();
            let rejected = control
                .validate_received_commit(&handle, &empty, 1, &request_context())
                .expect_err("a positive logical delta without artifacts cannot claim NoOp");
            assert_eq!(rejected.kind(), ConnectorErrorKind::CorruptData);
            assert!(matches!(
                handle.state().unwrap(),
                IcebergWriteSessionState::Active
            ));
            let stop = novarocks_spi::connector::ConnectorStopOwner::new();
            let cancelled = ConnectorRequestContext::try_new(
                Instant::now() + Duration::from_secs(30),
                stop.view(),
                64 * 1024,
                1024 * 1024,
            )
            .unwrap();
            stop.request_stop();
            let cancelled_result = control
                .validate_received_commit(&handle, &empty, 0, &cancelled)
                .expect_err("ordinary cancelled Work cannot mint a NoOp success");
            assert_eq!(cancelled_result.kind(), ConnectorErrorKind::Cancelled);
            assert!(matches!(
                handle.state().unwrap(),
                IcebergWriteSessionState::Active
            ));
            let commit = adapter("unit", 1).wrap_commit_session(handle);
            let outcome = futures::executor::block_on(control.finish_write_needed_original(
                empty_finish_request(&commit, &request_context()),
                None,
            ))
            .expect("empty exact row delta must not reach the unreachable REST catalog");
            let handle = adapter("unit", 1).commit_session(&commit).unwrap();
            let ConnectorWritePublicationCompletion::KnownCommitted { needed, cleanup } = outcome
            else {
                panic!("empty complete row delta must prove its NoOp")
            };
            assert_eq!(needed.effect(), ExternalMutationEffect::NoOp);
            assert_eq!(
                needed.finalization(),
                &ExternalMutationFinalization::Complete
            );
            assert_eq!(
                cleanup,
                novarocks_spi::connector::write_stack::ConnectorWriteCleanupState::Observed(
                    ExternalMutationFinalization::Complete
                )
            );
            assert_eq!(
                needed.receipt().committed_version().unwrap().snapshot_id(),
                snapshot
            );
            let bytes = needed.receipt().try_to_wire_v1().unwrap();
            let restored =
                novarocks_spi::connector::ConnectorWriteReceipt::try_from_wire_v1(&bytes).unwrap();
            assert_eq!(&restored, needed.receipt());
            assert_eq!(
                crate::write_codec::decode_write_receipt_output_facts(&restored).unwrap(),
                None
            );
            if snapshot.is_some() {
                assert_eq!(
                    restored.payload(),
                    &crate::write_codec::encode_write_receipt(77).unwrap()
                );
            } else {
                let payload: serde_json::Value =
                    serde_json::from_slice(restored.payload()).unwrap();
                assert_eq!(payload["kind"], "unchanged_snapshotless_target");
                assert!(payload["snapshot_id"].is_null());
                assert_eq!(payload["table_uuid"], handle.table().table_uuid());
                assert_eq!(payload["target_ref"], "main");
                assert_eq!(
                    payload["base_version_digest"],
                    serde_json::to_value([42_u8; 32]).unwrap()
                );
            }
            let ConnectorWriteAbortOutcome::KnownCommitted {
                needed: released, ..
            } = release_session_state(
                &descriptor("unit"),
                ProviderBindingEpoch::from_bytes([9; 16]),
                handle,
            )
            .unwrap()
            else {
                panic!("release must preserve the original NoOp")
            };
            assert!(Arc::ptr_eq(&needed, &released));
            assert!(handle.begin_commit().is_err());
        }
    }
}

#[test]
fn empty_data_replacements_and_managed_publications_keep_their_own_decision() {
    for flavor in [
        IcebergWriteFlavor::Append,
        IcebergWriteFlavor::Overwrite,
        IcebergWriteFlavor::PartitionOverwrite,
        IcebergWriteFlavor::TableMaintenance,
    ] {
        let (handle, _) = flavor_session(
            plan_ordinary_branches(flavor, &session_material(data_input_shape())).unwrap(),
        );
        assert_eq!(
            handle.empty_write_decision(),
            IcebergEmptyWriteDecision::Commit
        );
    }
    for disposition in [
        ConnectorManagedPublicationEmptyInputDisposition::CommitEmptyWrite,
        ConnectorManagedPublicationEmptyInputDisposition::AbortWithoutExternalCommit,
    ] {
        let (managed, _) = flavor_session(
            plan_document_publication_branches(
                &session_material(data_input_shape()),
                document_publication_facts(ConnectorManagedPublicationTechnique::Full, disposition),
            )
            .unwrap(),
        );
        assert_eq!(
            managed.empty_write_decision(),
            match disposition {
                ConnectorManagedPublicationEmptyInputDisposition::CommitEmptyWrite =>
                    IcebergEmptyWriteDecision::Commit,
                ConnectorManagedPublicationEmptyInputDisposition::AbortWithoutExternalCommit =>
                    IcebergEmptyWriteDecision::SkipExternalCommit,
            }
        );
    }
}

#[test]
fn an_empty_finish_cannot_replace_an_original_terminal_or_inflight_fact() {
    use novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion;
    use novarocks_spi::connector::{ConnectorMutationFailure, ConnectorMutationFailureKind};
    for flavor in 0..3 {
        for previous in 0..5 {
            let handle = match flavor {
                0 => dv_session().0,
                1 => flavor_session(plan_distributed_rewrite_branches(
                    &session_material(data_input_shape()),
                    DATA_FILE_REWRITE,
                    &rewrite_groups(&["a"]),
                ).unwrap()).0,
                _ => flavor_session(plan_document_publication_branches(
                    &session_material(data_input_shape()),
                    document_publication_facts(
                        ConnectorManagedPublicationTechnique::Full,
                        ConnectorManagedPublicationEmptyInputDisposition::AbortWithoutExternalCommit,
                    ),
                ).unwrap()).0,
            };
            assert_eq!(
                handle.empty_write_decision(),
                IcebergEmptyWriteDecision::SkipExternalCommit
            );
            // Exercise the same no-op author directly for each flavor.
            // The managed fixture does not construct a publication packet.
            let finish = || -> Result<_, ConnectorError> {
                match handle.claim_finish(|| empty_write_needed(&handle).map(Some))? {
                    super::domain::ClaimedWriteFinish::EmptyNoOp(completion) => Ok(completion),
                    _ => panic!("an empty NoOp must never enter publication"),
                }
            };
            let mut original_needed = None;
            let mut original_evidence = None;
            match previous {
                0 => {
                    let ConnectorWritePublicationCompletion::KnownCommitted { needed, .. } =
                        finish().expect("original Active session may settle its NoOp")
                    else {
                        panic!("empty Active finish lost its NoOp")
                    };
                    assert_eq!(needed.effect(), ExternalMutationEffect::NoOp);
                    original_needed = Some(needed);
                }
                1 => {
                    assert!(matches!(
                        release_session_state(
                            &descriptor("unit"),
                            ProviderBindingEpoch::from_bytes([9; 16]),
                            &handle,
                        )
                        .unwrap(),
                        ConnectorWriteAbortOutcome::KnownUncommitted { .. }
                    ));
                }
                2 => {
                    handle.begin_commit().unwrap();
                }
                3 => {
                    handle.begin_commit().unwrap();
                    let evidence = novarocks_spi::connector::ExternalMutationEvidence::try_new(
                        crate::commit::write_stack::control::ICEBERG_WRITE_SESSION_EVIDENCE_VERSION,
                        descriptor("unit"),
                        ProviderBindingEpoch::from_bytes([9; 16]),
                        novarocks_spi::connector::ConnectorMutationOperationId::from_bytes(
                            handle.session_id().to_bytes(),
                        ),
                        crate::commit::write_stack::control::ICEBERG_WRITE_SESSION_OPERATION_KIND,
                        bytes::Bytes::from_static(b"same checked original Unknown inventory"),
                    )
                    .unwrap();
                    handle.retain_recovery_evidence(evidence.clone()).unwrap();
                    handle
                        .settle(IcebergWriteSessionState::CommitUnknown {
                            message: "original transport outcome remains unknown".into(),
                            staging_dir: handle.staging_dir(),
                        })
                        .unwrap();
                    original_evidence = Some(evidence);
                }
                _ => {
                    let needed = Arc::new(
                        novarocks_spi::connector::write_stack::ConnectorWriteNeededReady::new(
                            ExternalMutationEffect::Applied,
                            crate::write_codec::connector_write_receipt_with_partitioning(
                                77, None, None,
                            )
                            .unwrap(),
                            ExternalMutationFinalization::Complete,
                        ),
                    );
                    handle
                        .settle(IcebergWriteSessionState::KnownCommitted {
                            needed: Arc::clone(&needed),
                        })
                        .unwrap();
                    original_needed = Some(needed);
                }
            }
            // Distinct retained failures expose any accidental Complete rewrite.
            let retained = ExternalMutationFinalization::Failed(ConnectorMutationFailure::new(
                ConnectorMutationFailureKind::Unavailable,
                "original cleanup cause must survive a refused empty finish",
            ));
            handle.retain_cleanup(retained.clone());
            handle.retain_finalization(retained.clone());
            let state_before = format!("{:?}", handle.state().unwrap());
            let cleanup_before = handle.terminal_cleanup();
            let responsibility_before = handle.cleanup_responsibility();
            let rejected = finish().expect_err("empty finish cannot replace an earlier fact");
            assert_eq!(rejected.kind(), ConnectorErrorKind::InvalidRequest);
            assert_eq!(format!("{:?}", handle.state().unwrap()), state_before);
            assert_eq!(handle.terminal_cleanup(), cleanup_before);
            assert_eq!(handle.terminal_finalization(), retained);
            assert_eq!(handle.cleanup_responsibility(), responsibility_before);
            if let Some(original) = original_needed {
                let IcebergWriteSessionState::KnownCommitted { needed } = handle.state().unwrap()
                else {
                    panic!("refused finish replaced the original committed state")
                };
                assert!(Arc::ptr_eq(&original, &needed));
                let ConnectorWriteAbortOutcome::KnownCommitted {
                    needed: released,
                    cleanup,
                } = release_session_state(
                    &descriptor("unit"),
                    ProviderBindingEpoch::from_bytes([9; 16]),
                    &handle,
                )
                .unwrap()
                else {
                    panic!("abort must preserve the original committed fact")
                };
                assert!(Arc::ptr_eq(&original, &released));
                assert_eq!(cleanup, cleanup_before);
            }
            if let Some(original) = original_evidence {
                assert_eq!(handle.recovery_evidence().unwrap(), original);
                let ConnectorWriteAbortOutcome::CommitUnknown { failure, evidence } =
                    release_session_state(
                        &descriptor("unit"),
                        ProviderBindingEpoch::from_bytes([9; 16]),
                        &handle,
                    )
                    .unwrap()
                else {
                    panic!("refused empty finish resolved an Unknown outcome")
                };
                assert_eq!(evidence, original);
                assert!(
                    failure
                        .message()
                        .contains("original transport outcome remains unknown")
                );
            }
        }
    }
}

#[test]
fn borrowed_provider_finish_and_abort_preserve_the_winning_terminal_fact() {
    use novarocks_spi::connector::write_stack::{
        ConnectorWriteAbortProduction, ConnectorWriteCommitSession,
        ConnectorWriteFinishPublication, ConnectorWriteFinishRequestRef,
        ConnectorWriteSessionAbortRequest,
    };

    fn finish(
        control: &crate::commit::write_stack::control::IcebergWriteSessionControl,
        commit: &ConnectorWriteCommitSession,
    ) -> Result<
        ExternalMutationOutcome<novarocks_spi::connector::ConnectorWriteReceipt>,
        ConnectorError,
    > {
        futures::executor::block_on(control.finish_write_async(ConnectorWriteFinishRequestRef {
            commit,
            row_count: 0,
            statistics: &[],
            publication: &ConnectorWriteFinishPublication::None,
            context: &request_context(),
            production: ConnectorWriteAbortProduction::CompleteReceived,
        }))
    }
    fn abort(
        control: &crate::commit::write_stack::control::IcebergWriteSessionControl,
        commit: &ConnectorWriteCommitSession,
    ) -> Result<ConnectorWriteAbortOutcome, ConnectorError> {
        futures::executor::block_on(
            control.abort_write_async(ConnectorWriteSessionAbortRequest {
                commit,
                context: request_context(),
                production: ConnectorWriteAbortProduction::Unproven,
            }),
        )
    }

    let (_executor, runtime) = unreachable_rest_runtime();
    let control = crate::commit::write_stack::control::IcebergWriteSessionControl::new(
        descriptor("unit"),
        ProviderBindingEpoch::new(),
        CatalogHandle::new(
            ConnectorInstanceId::parse("unit").unwrap(),
            CatalogVersion::from_bytes([1; 32]),
        ),
        runtime,
    );
    // Two controlled orders and one simultaneous start use the same borrowed
    // public commit session. No clone or alternative execution owner is made.
    for order in 0..3 {
        let (handle, adapter) = dv_session();
        let commit = adapter.wrap_commit_session(handle);
        let start = std::sync::Barrier::new(3);
        let (finished, aborted) = std::thread::scope(|scope| {
            let (finish_done, wait_finish) = std::sync::mpsc::sync_channel::<()>(0);
            let (abort_done, wait_abort) = std::sync::mpsc::sync_channel::<()>(0);
            let start_ref = &start;
            let control_ref = &control;
            let commit_ref = &commit;
            let first = scope.spawn(move || {
                start_ref.wait();
                if order == 1 {
                    wait_abort.recv().unwrap();
                }
                let result = finish(control_ref, commit_ref);
                if order == 0 {
                    finish_done.send(()).unwrap();
                }
                result
            });
            let second = scope.spawn(move || {
                start_ref.wait();
                if order == 0 {
                    wait_finish.recv().unwrap();
                }
                let result = abort(control_ref, commit_ref);
                if order == 1 {
                    abort_done.send(()).unwrap();
                }
                result
            });
            start.wait();
            (first.join().unwrap(), second.join().unwrap())
        });
        let original = adapter.commit_session(&commit).unwrap();
        match finished {
            Ok(ExternalMutationOutcome::KnownCommitted {
                effect, receipt, ..
            }) => {
                assert_ne!(
                    order, 1,
                    "abort-first must retain its original negative decision"
                );
                assert_eq!(effect, ExternalMutationEffect::NoOp);
                let ConnectorWriteAbortOutcome::KnownCommitted { needed, cleanup } =
                    aborted.unwrap()
                else {
                    panic!("abort overwrote the completed original NoOp")
                };
                assert_eq!(needed.effect(), ExternalMutationEffect::NoOp);
                assert_eq!(needed.receipt(), &receipt);
                let IcebergWriteSessionState::KnownCommitted { needed: retained } =
                    original.state().unwrap()
                else {
                    panic!("winning NoOp was overwritten after both original calls returned")
                };
                assert!(Arc::ptr_eq(&needed, &retained));
                assert_eq!(cleanup, original.terminal_cleanup());
                assert_eq!(
                    original.cleanup_responsibility(),
                    novarocks_spi::connector::ConnectorMetadataCleanupResponsibility::Disposed
                );
            }
            Ok(ExternalMutationOutcome::KnownUncommitted { .. }) | Err(_) => {
                assert_ne!(
                    order, 0,
                    "finish-first must retain its original positive decision"
                );
                assert!(matches!(
                    aborted.unwrap(),
                    ConnectorWriteAbortOutcome::KnownUncommitted { .. }
                ));
                let IcebergWriteSessionState::KnownUncommitted { message } =
                    original.state().unwrap()
                else {
                    panic!("winning release was replaced by empty publication")
                };
                assert_eq!(
                    message,
                    "Iceberg write session was released before any external commit"
                );
                assert_eq!(
                    original.cleanup_responsibility(),
                    novarocks_spi::connector::ConnectorMetadataCleanupResponsibility::Retained
                );
            }
            Ok(ExternalMutationOutcome::CommitUnknown { .. }) => {
                panic!("empty original finish/abort cannot fabricate an external dispatch")
            }
        }
    }
}

#[test]
fn partition_replace_routes_after_image_partition_and_old_file_identity() {
    let input = merge_on_read_input_shape();
    let mut material = session_material(input.clone());
    material.data_recipe = super::domain::IcebergDataBranchRecipe::try_new(
        Some(
            serde_json::json!({"type":"struct","schema-id":1,"fields":[
                {"id":1,"name":"k1","required":false,"type":"long"},
                {"id":4,"name":"v1","required":false,"type":"string"}
            ]})
            .to_string(),
        ),
        vec!["k1".into()],
        vec!["partition_key".into()],
        vec!["bucket[8]".into()],
        false,
    )
    .unwrap();
    let plan = plan_row_mutation_branches(&material).unwrap();
    let (_, plans) = flavor_session(plan);
    let data = &plans[0];
    let delete = &plans[1];
    let new_partition = input
        .fields()
        .into_iter()
        .find(|field| field.field().name() == "k1")
        .unwrap()
        .token();
    let old_file_token = input
        .fields()
        .into_iter()
        .find(|field| field.field().name() == "_file")
        .unwrap()
        .token();
    use novarocks_spi::connector::write_stack::ConnectorWriteLayoutPreference;
    let layout = &data.route().unwrap().requirements().layout;
    assert!(
        matches!(layout, ConnectorWriteLayoutPreference::PartitionAffinity { inputs, recipe }
        if inputs == &[new_partition] && recipe.binding() == &material.write_binding)
    );
    assert_eq!(
        delete.route().unwrap().requirements().layout,
        ConnectorWriteLayoutPreference::ClusterBy(vec![old_file_token])
    );

    assert!(
        data.route()
            .unwrap()
            .input_ordinals()
            .iter()
            .any(|field| field.token() == new_partition)
    );
    assert_eq!(
        data.handle().data().unwrap().transform_exprs(),
        &["bucket[8]".to_string()]
    );
    assert_eq!(
        delete.route().unwrap().requirements().colocation,
        novarocks_spi::connector::write_stack::ConnectorWriteColocation::ByKey(vec![
            old_file_token
        ])
    );
    assert!(
        !delete
            .route()
            .unwrap()
            .input_ordinals()
            .iter()
            .any(|field| field.token() == new_partition),
        "new partition data cannot become old-delete ownership"
    );
    assert!(
        data.route()
            .unwrap()
            .accepted_effects()
            .contains(&ConnectorRowMutationEffect::Replace)
    );
    assert!(
        delete
            .route()
            .unwrap()
            .accepted_effects()
            .contains(&ConnectorRowMutationEffect::Replace)
    );
}

#[test]
fn a_row_mutation_requires_a_physical_row_identity() {
    let error = plan_row_mutation_branches(&session_material(lineage_only_input_shape()))
        .expect_err("lineage alone cannot locate a physical delete row");
    assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
    assert!(
        error
            .message()
            .contains("requires a `_file`/`_pos` row identity"),
        "unexpected message: {}",
        error.message()
    );
}

fn open_streaming_data_writer(
    lake: &FrozenRewriteLake,
    context: ConnectorRequestContext,
) -> (
    Box<dyn novarocks_spi::connector::write_stack::ConnectorBatchWriter>,
    arrow::datatypes::SchemaRef,
) {
    use novarocks_spi::connector::write_stack::{
        ConnectorOpenWriterRequest, ConnectorWriteExecution, ConnectorWriterPhysicalContext,
    };
    let session = lake
        .executor
        .block_on(
            lake.control
                .begin_write_async(ordinary_async_test_begin(context.clone())),
        )
        .unwrap();
    let target = &session.targets()[0];
    let ConnectorWriteInputShape::Data { fields } = target.input() else {
        panic!("data writer input")
    };
    let schema = Arc::new(Schema::new(
        fields
            .iter()
            .map(|field| field.field().clone())
            .collect::<Vec<_>>(),
    ));
    let execution = super::execution::IcebergWriteStackExecution::new(
        session.commit_session().binding().catalog_handle().clone(),
        lake.adapter.clone(),
        lake.runtime.resources().planning_binding().clone(),
    );
    let writer = lake
        .executor
        .block_on(execution.open_writer(ConnectorOpenWriterRequest {
            resources: Some(super::test_support::writer_resources()),
            handle: target.handle().clone(),
            target: target.ordinal(),
            expected_schema: schema.clone(),
            physical: ConnectorWriterPhysicalContext::new([1; 16], 1, [2; 16], 0, 0),
            context,
        }))
        .unwrap();
    (writer, schema)
}
#[test]
fn actual_data_stack_append_pages_share_one_file_until_finish() {
    let lake = FrozenRewriteLake::new();
    let (mut writer, schema) = open_streaming_data_writer(&lake, request_context());
    for value in [7, 9, 11] {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![value]))],
        )
        .unwrap();
        lake.executor.block_on(writer.append(batch)).unwrap();
    }
    let fragments = lake.executor.block_on(writer.finish()).unwrap();
    assert_eq!(
        fragments.len(),
        1,
        "one driver consumes three pages into one data file"
    );
    let fragment = lake.adapter.commit_fragment(&fragments[0]).unwrap();
    let super::domain::IcebergCommitArtifact::DataFile(artifact) = fragment.artifact() else {
        panic!("data file fragment")
    };
    assert_eq!(artifact.metrics().record_count(), 3);
    assert!(std::path::Path::new(artifact.path().trim_start_matches("file://")).is_file());
}
#[test]
fn cancelled_data_stack_aborts_open_streams_before_deleting_original_objects() {
    let lake = FrozenRewriteLake::new();
    let stop = novarocks_spi::connector::ConnectorStopOwner::new();
    let context = ConnectorRequestContext::try_new(
        Instant::now() + Duration::from_secs(30),
        stop.view(),
        64 * 1024,
        1024 * 1024,
    )
    .unwrap();
    let (mut writer, schema) = open_streaming_data_writer(&lake, context);
    for value in [7, 9, 11] {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![value]))],
        )
        .unwrap();
        lake.executor.block_on(writer.append(batch)).unwrap();
    }
    stop.request_stop();
    let error = lake
        .executor
        .block_on(writer.finish())
        .expect_err("cancelled finish");
    assert_eq!(error.kind(), ConnectorErrorKind::Cancelled);
    lake.executor.block_on(writer.abort()).unwrap();
    lake.executor.block_on(writer.abort()).unwrap();
    // No data object may remain after the actual original abort. Catalog
    // metadata files are independent, and are not this driver's objects.
    fn parquet_paths(path: &std::path::Path) -> Vec<std::path::PathBuf> {
        let mut found = Vec::new();
        for entry in std::fs::read_dir(path).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                found.extend(parquet_paths(&path));
            } else if path
                .extension()
                .is_some_and(|extension| extension == "parquet")
            {
                found.push(path);
            }
        }
        found
    }
    assert!(parquet_paths(lake._warehouse.path()).is_empty());
}
