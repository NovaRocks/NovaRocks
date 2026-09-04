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
//! The D10 tests read real Parquet position-delete files off a temporary local
//! filesystem so that "missing", "corrupt", and "stale" are genuine I/O
//! outcomes rather than mocked verdicts.

use std::collections::BTreeMap;
use std::fs;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use novarocks_fs::{
    FileIoRuntime, FileTaskSpawner, FsAccessResolver, TokioFileIoRuntime, TokioFileTaskSpawner,
};
use novarocks_spi::connector::write_stack::session::{
    ConnectorWriteRouteFacts, ConnectorWriteSessionFlavor, ConnectorWriteSessionPlan,
    ConnectorWriteTargetPlan,
};
use novarocks_spi::connector::write_stack::{
    ConnectorManagedPublicationShape, ConnectorPreparedWriteSet, WriteRuntimeAdapter,
    WriteTargetOrdinal,
};
use novarocks_spi::connector::{
    CatalogHandle, CatalogVersion, ConnectorCancellation, ConnectorCommittedVersion,
    ConnectorDistributedRewriteShape, ConnectorError, ConnectorErrorKind,
    ConnectorInstanceDescriptor, ConnectorInstanceId,
    ConnectorManagedPublicationEmptyInputDisposition, ConnectorManagedPublicationTechnique,
    ConnectorMutationRouteInput, ConnectorProviderId, ConnectorRequestContext,
    ConnectorRowMutationEffect, ConnectorWriteAbortOutcome, ConnectorWriteAdmissionPurpose,
    ConnectorWriteInputShape, ConnectorWriteRouteId, ExternalMutationEffect,
    ExternalMutationOutcome, ProviderBindingEpoch,
};
use parquet::arrow::ArrowWriter;

use crate::access_binding::IcebergReadBinding;
use crate::commit::CommitOpKind;
use crate::commit::write_stack::control::{
    eager_conflict_backoff, release_session_state, session_freezes_old_deletes,
    session_plan_from_targets, settle_empty_write_without_commit, validate_prepared_set,
};
use crate::commit::write_stack::copy_on_write::{IcebergCowBranchInput, IcebergCowBranchRecipe};
use crate::commit::write_stack::domain::{
    IcebergCommitFragment, IcebergCommitHandle, IcebergDataFileArtifact, IcebergEmptyWriteDecision,
    IcebergPositionDeleteFileArtifact, IcebergWriteBranch, IcebergWriteFlavor,
    IcebergWriteSessionId, IcebergWriteSessionState, IcebergWriteTableFacts,
};
use crate::commit::write_stack::flavor::{
    IcebergFrozenRewriteBranch, IcebergSessionFlavorPlan, plan_copy_on_write_branches,
    plan_distributed_rewrite_branches, plan_managed_publication_branches, plan_ordinary_branches,
    plan_row_mutation_branches,
};
use crate::commit::write_stack::old_delete::{
    IcebergOldDeleteMergeTarget, read_and_merge_old_deletes,
};
use crate::commit::write_stack::planning::{
    IcebergBranchSessionPlanInput, IcebergWriteBranchPlan, IcebergWriteSessionPlanInput,
    IcebergWriteTargetPlan, plan_branch_session, plan_write_session,
};
use crate::commit::write_stack::runtime::{IcebergWriteAdapter, IcebergWriteRuntime};
use crate::commit::write_stack::test_support::{
    binding, copy_on_write_input_shape, data_branch_plan, data_input_shape, delete_branch_plan,
    delete_input_shape, dv_artifact, equality_delete_input_shape, equality_delete_recipe,
    merge_on_read_input_shape, merge_target, parquet_ref, publication_facts,
    publication_facts_with_shape, publication_flavor, publication_id, sample_metrics,
    sample_partition, session_material, table_facts,
};
use crate::delete_file::IcebergFileFormat;
use crate::manifest::DataFileWithStats;
use crate::position_delete::{FILE_PATH_COLUMN, POS_COLUMN};

struct NeverCancelled;

impl ConnectorCancellation for NeverCancelled {
    fn is_cancelled(&self) -> bool {
        false
    }
}

fn request_context() -> ConnectorRequestContext {
    ConnectorRequestContext::try_new(
        Instant::now() + Duration::from_secs(30),
        Arc::new(NeverCancelled),
        64 * 1024,
        1024 * 1024,
    )
    .expect("request context")
}

struct SwitchCancellation(std::sync::atomic::AtomicBool);

impl ConnectorCancellation for SwitchCancellation {
    fn is_cancelled(&self) -> bool {
        self.0.load(std::sync::atomic::Ordering::Acquire)
    }
}

#[test]
fn conflict_backoff_observes_cancellation_before_another_attempt() {
    let (_executor, runtime) = unreachable_rest_runtime();
    let cancellation = Arc::new(SwitchCancellation(std::sync::atomic::AtomicBool::new(
        false,
    )));
    let context = ConnectorRequestContext::try_new(
        Instant::now() + Duration::from_secs(1),
        cancellation.clone(),
        64 * 1024,
        1024 * 1024,
    )
    .expect("context");
    let cancel = Arc::clone(&cancellation);
    let worker = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(12));
        cancel.0.store(true, std::sync::atomic::Ordering::Release);
    });
    let started = Instant::now();
    let error =
        eager_conflict_backoff(runtime.as_ref(), &context, 2).expect_err("cancelled backoff");
    worker.join().expect("cancel thread");
    assert_eq!(error.kind(), ConnectorErrorKind::Cancelled);
    assert!(started.elapsed() < Duration::from_millis(80));
}

fn descriptor(catalog: &str) -> ConnectorInstanceDescriptor {
    ConnectorInstanceDescriptor {
        provider_id: ConnectorProviderId::parse("iceberg").expect("provider id"),
        instance_id: ConnectorInstanceId::parse(catalog).expect("instance id"),
    }
}

fn adapter(catalog: &str, version: u8) -> IcebergWriteAdapter {
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

fn local_binding() -> (IcebergReadBinding, tokio::runtime::Runtime) {
    let runtime = tokio::runtime::Runtime::new().expect("build Tokio runtime");
    let file_runtime: Arc<dyn FileIoRuntime> =
        Arc::new(TokioFileIoRuntime::new(runtime.handle().clone()));
    let task_spawner: Arc<dyn FileTaskSpawner> =
        Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone()));
    let binding =
        IcebergReadBinding::new(None, FsAccessResolver::new(), file_runtime, task_spawner);
    (binding, runtime)
}

fn write_delete_parquet(path: &std::path::Path, file_paths: &[&str], positions: &[i64]) -> u64 {
    let schema = Arc::new(Schema::new(vec![
        Field::new(FILE_PATH_COLUMN, DataType::Utf8, false),
        Field::new(POS_COLUMN, DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(file_paths.to_vec())),
            Arc::new(Int64Array::from(positions.to_vec())),
        ],
    )
    .expect("build delete batch");
    let file = fs::File::create(path).expect("create delete file");
    let mut writer = ArrowWriter::try_new(file, schema, None).expect("create parquet writer");
    writer.write(&batch).expect("write delete batch");
    writer.close().expect("close parquet writer");
    fs::metadata(path).expect("stat delete file").len()
}

fn location(path: &std::path::Path) -> String {
    format!("file://{}", path.display())
}

// -------------------------------------------------------------------------
// D10: the backend reads, validates, and merges the frozen references.
// -------------------------------------------------------------------------

#[test]
fn multiple_old_delete_files_for_one_data_file_are_read_and_merged() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let data_file = "/data/a.parquet";
    let first = directory.path().join("d1.parquet");
    let second = directory.path().join("d2.parquet");
    let first_size = write_delete_parquet(&first, &[data_file, data_file], &[2, 5]);
    let second_size = write_delete_parquet(&second, &[data_file, "/data/b.parquet"], &[7, 9]);

    let target = IcebergOldDeleteMergeTarget::try_new(
        data_file.to_string(),
        100,
        Some(4),
        sample_partition(),
        77,
        vec![
            parquet_ref(&location(&first), Some(data_file), first_size, 2).expect("reference"),
            // The second file is shared with another data file, so it is frozen
            // without an exclusive reference and may contribute fewer rows.
            parquet_ref(&location(&second), None, second_size, 2).expect("reference"),
        ],
    )
    .expect("merge target");

    let (binding, _runtime) = local_binding();
    let merged = read_and_merge_old_deletes(&target, &binding, &request_context())
        .expect("read and merge old deletes");
    assert_eq!(
        merged.positions().iter().collect::<Vec<_>>(),
        vec![2, 5, 7],
        "the merge is the union of every frozen reference's positions for this data file"
    );
    assert_eq!(
        merged.merged_references(),
        &[location(&first), location(&second)],
        "the artifact records exactly which references it superseded"
    );
}

#[test]
fn a_data_file_with_no_frozen_references_merges_to_nothing_without_touching_storage() {
    let target = merge_target("s3://b/a.parquet", 100, Vec::new());
    let (binding, _runtime) = local_binding();
    let merged = read_and_merge_old_deletes(&target, &binding, &request_context())
        .expect("no references is a decision, not a read");
    assert!(merged.positions().is_empty());
    assert!(merged.merged_references().is_empty());
}

#[test]
fn a_missing_old_delete_artifact_fails_closed_instead_of_reading_as_empty() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let absent = directory.path().join("gone.parquet");
    let target = IcebergOldDeleteMergeTarget::try_new(
        "/data/a.parquet".to_string(),
        100,
        None,
        sample_partition(),
        77,
        vec![parquet_ref(&location(&absent), Some("/data/a.parquet"), 512, 2).expect("reference")],
    )
    .expect("merge target");

    let (binding, _runtime) = local_binding();
    let error = read_and_merge_old_deletes(&target, &binding, &request_context())
        .expect_err("a missing artifact must fail the writer");
    assert_ne!(
        error.kind(),
        ConnectorErrorKind::Unsupported,
        "a missing artifact must not be silently absorbed"
    );
}

#[test]
fn a_stale_old_delete_artifact_is_detected_before_it_is_parsed() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("d.parquet");
    let real_size = write_delete_parquet(&path, &["/data/a.parquet"], &[3]);
    let target = IcebergOldDeleteMergeTarget::try_new(
        "/data/a.parquet".to_string(),
        100,
        None,
        sample_partition(),
        77,
        vec![
            // The frozen size no longer matches what is in storage: the
            // artifact was replaced between planning and execution.
            parquet_ref(&location(&path), Some("/data/a.parquet"), real_size + 1, 1)
                .expect("reference"),
        ],
    )
    .expect("merge target");

    let (binding, _runtime) = local_binding();
    let error = read_and_merge_old_deletes(&target, &binding, &request_context())
        .expect_err("a stale artifact must fail the writer");
    assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
    assert!(error.message().contains("is stale"), "{}", error.message());
}

#[test]
fn a_corrupt_old_delete_artifact_fails_closed() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("corrupt.parquet");
    fs::write(&path, b"this is not a parquet file").expect("write corrupt file");
    let size = fs::metadata(&path).expect("stat").len();
    let target = IcebergOldDeleteMergeTarget::try_new(
        "/data/a.parquet".to_string(),
        100,
        None,
        sample_partition(),
        77,
        vec![parquet_ref(&location(&path), Some("/data/a.parquet"), size, 1).expect("reference")],
    )
    .expect("merge target");

    let (binding, _runtime) = local_binding();
    let error = read_and_merge_old_deletes(&target, &binding, &request_context())
        .expect_err("a corrupt artifact must fail the writer");
    assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
}

#[test]
fn an_exclusive_artifact_that_reads_empty_is_corruption_not_an_empty_delete_set() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("foreign.parquet");
    // The file exists and parses, but every row belongs to another data file.
    // Frozen as exclusive to `/data/a.parquet`, that is a contradiction.
    let size = write_delete_parquet(&path, &["/data/b.parquet"], &[1]);
    let target = IcebergOldDeleteMergeTarget::try_new(
        "/data/a.parquet".to_string(),
        100,
        None,
        sample_partition(),
        77,
        vec![parquet_ref(&location(&path), None, size, 0).expect("reference")],
    )
    .expect("merge target");
    // Re-freeze the same artifact as exclusive to /data/a.parquet.
    let exclusive = IcebergOldDeleteMergeTarget::try_new(
        "/data/a.parquet".to_string(),
        100,
        None,
        sample_partition(),
        77,
        vec![parquet_ref(&location(&path), Some("/data/a.parquet"), size, 0).expect("reference")],
    )
    .expect("merge target");

    let (binding, _runtime) = local_binding();
    // The shared framing is allowed to contribute nothing.
    assert!(
        read_and_merge_old_deletes(&target, &binding, &request_context())
            .expect("shared artifact")
            .positions()
            .is_empty()
    );
    // The exclusive framing is not.
    let error = read_and_merge_old_deletes(&exclusive, &binding, &request_context())
        .expect_err("an exclusive artifact must contribute at least one position");
    assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
    assert!(
        error.message().contains("decoded no positions"),
        "{}",
        error.message()
    );
}

#[test]
fn a_position_outside_the_referenced_data_file_is_rejected() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("d.parquet");
    let size = write_delete_parquet(&path, &["/data/a.parquet"], &[500]);
    let target = IcebergOldDeleteMergeTarget::try_new(
        "/data/a.parquet".to_string(),
        // The data file has only 100 rows, so position 500 cannot exist.
        100,
        None,
        sample_partition(),
        77,
        vec![parquet_ref(&location(&path), Some("/data/a.parquet"), size, 1).expect("reference")],
    )
    .expect("merge target");

    let (binding, _runtime) = local_binding();
    let error = read_and_merge_old_deletes(&target, &binding, &request_context())
        .expect_err("a position past the data file must fail");
    assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
    assert!(
        error.message().contains("which has only 100 rows"),
        "{}",
        error.message()
    );
}

#[test]
fn an_artifact_frozen_for_another_data_file_is_rejected_before_any_io() {
    let error = IcebergOldDeleteMergeTarget::try_new(
        "/data/a.parquet".to_string(),
        100,
        None,
        sample_partition(),
        77,
        vec![parquet_ref("s3://b/d.parquet", Some("/data/b.parquet"), 512, 1).expect("reference")],
    )
    .expect_err("mismatched reference");
    assert!(error.message().contains("belongs to data file"));
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
            vec!["s3://b/data/old.parquet".to_string()],
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

fn dv_session() -> (IcebergCommitHandle, IcebergWriteAdapter) {
    let (handle, _plans) = plan_write_session(
        IcebergWriteSessionId::new(),
        IcebergWriteSessionPlanInput {
            flavor: IcebergWriteFlavor::RowMutationDeletionVector,
            purpose: ConnectorWriteAdmissionPurpose::OrdinaryDml,
            table: table_facts(),
            base_version_digest: None,
            staged_metadata: None,
            data: data_branch_plan(),
            deletes: vec![delete_branch_plan(
                IcebergWriteBranch::DeletionVector,
                vec![merge_target(
                    "s3://b/wh/db/t/data/a.parquet",
                    100,
                    vec![
                        parquet_ref("s3://b/wh/db/t/data/old.parquet", None, 512, 2)
                            .expect("reference"),
                    ],
                )],
            )],
        },
    )
    .expect("session");
    (handle, adapter("unit", 1))
}

fn ordinal(value: u32) -> WriteTargetOrdinal {
    WriteTargetOrdinal::try_new(value).expect("ordinal")
}

fn prepared(
    adapter: &IcebergWriteAdapter,
    entries: Vec<(WriteTargetOrdinal, IcebergCommitFragment)>,
    expected: &[WriteTargetOrdinal],
) -> ConnectorPreparedWriteSet {
    let wrapped = entries
        .into_iter()
        .map(|(target, fragment)| (target, adapter.wrap_commit_fragment(fragment)))
        .collect();
    ConnectorPreparedWriteSet::try_new(0, wrapped, expected).expect("prepared set")
}

fn dv_fragment(path: &str, referenced: &str, merged: Vec<String>) -> IcebergCommitFragment {
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

fn data_fragment(path: &str) -> IcebergCommitFragment {
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
    // A statement that matched nothing still reaches finish_write, and an empty
    // complete set is legal: it commits an empty snapshot rather than being
    // mistaken for a lost result.
    let (handle, adapter) = dv_session();
    let empty = prepared(&adapter, Vec::new(), &handle.expected_targets());
    let validated =
        validate_prepared_set(&handle, &adapter, &empty).expect("an empty complete set is legal");
    assert!(validated.is_empty());
    assert_eq!(empty.row_count(), 0);
}

#[test]
fn a_valid_prepared_set_passes_every_sealed_check() {
    let (handle, adapter) = dv_session();
    let set = prepared(
        &adapter,
        vec![
            (ordinal(0), data_fragment("s3://b/wh/db/t/data/new.parquet")),
            (
                ordinal(1),
                dv_fragment(
                    "s3://b/wh/db/t/data/v.puffin",
                    "s3://b/wh/db/t/data/a.parquet",
                    vec!["s3://b/wh/db/t/data/old.parquet".to_string()],
                ),
            ),
        ],
        &handle.expected_targets(),
    );
    let validated = validate_prepared_set(&handle, &adapter, &set).expect("valid set");
    assert_eq!(validated.len(), 2);
}

#[test]
fn a_fragment_whose_branch_disagrees_with_its_target_is_rejected() {
    let (handle, adapter) = dv_session();
    let set = prepared(
        &adapter,
        vec![(
            ordinal(0),
            dv_fragment(
                "s3://b/wh/db/t/data/v.puffin",
                "s3://b/wh/db/t/data/a.parquet",
                Vec::new(),
            ),
        )],
        &handle.expected_targets(),
    );
    let error = validate_prepared_set(&handle, &adapter, &set).expect_err("branch disagreement");
    assert!(
        error.message().contains("drives the data branch"),
        "{}",
        error.message()
    );
}

#[test]
fn two_delete_artifacts_for_one_data_file_are_rejected() {
    let (handle, adapter) = dv_session();
    let set = prepared(
        &adapter,
        vec![
            (
                ordinal(1),
                dv_fragment(
                    "s3://b/wh/db/t/data/v1.puffin",
                    "s3://b/wh/db/t/data/a.parquet",
                    Vec::new(),
                ),
            ),
            (
                ordinal(1),
                dv_fragment(
                    "s3://b/wh/db/t/data/v2.puffin",
                    "s3://b/wh/db/t/data/a.parquet",
                    Vec::new(),
                ),
            ),
        ],
        &handle.expected_targets(),
    );
    let error = validate_prepared_set(&handle, &adapter, &set).expect_err("duplicate delete");
    assert!(
        error.message().contains("more than one delete artifact"),
        "{}",
        error.message()
    );
}

#[test]
fn a_delete_artifact_for_an_unrouted_data_file_is_rejected() {
    let (handle, adapter) = dv_session();
    let set = prepared(
        &adapter,
        vec![(
            ordinal(1),
            dv_fragment(
                "s3://b/wh/db/t/data/v.puffin",
                "s3://b/wh/db/t/data/never-routed.parquet",
                Vec::new(),
            ),
        )],
        &handle.expected_targets(),
    );
    let error = validate_prepared_set(&handle, &adapter, &set).expect_err("unrouted data file");
    assert!(
        error.message().contains("never routed"),
        "{}",
        error.message()
    );
}

#[test]
fn a_repeated_staged_path_is_rejected() {
    let (handle, adapter) = dv_session();
    let set = prepared(
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
        &handle.expected_targets(),
    );
    let error = validate_prepared_set(&handle, &adapter, &set).expect_err("duplicate path");
    assert!(
        error.message().contains("repeats staged artifact"),
        "{}",
        error.message()
    );
}

#[test]
fn a_delete_artifact_must_supersede_exactly_the_frozen_references() {
    let (handle, adapter) = dv_session();
    let frozen = handle.frozen_old_references();
    assert_eq!(
        frozen[&ordinal(1)]["s3://b/wh/db/t/data/a.parquet"],
        vec!["s3://b/wh/db/t/data/old.parquet".to_string()]
    );

    let matching = prepared(
        &adapter,
        vec![(
            ordinal(1),
            dv_fragment(
                "s3://b/wh/db/t/data/v.puffin",
                "s3://b/wh/db/t/data/a.parquet",
                vec!["s3://b/wh/db/t/data/old.parquet".to_string()],
            ),
        )],
        &handle.expected_targets(),
    );
    let validated = validate_prepared_set(&handle, &adapter, &matching).expect("valid");
    crate::commit::write_stack::control::validate_merged_old_references(&frozen, &validated)
        .expect("the artifact superseded exactly the frozen references");

    // A writer that merged nothing must not be committed: the new artifact
    // replaces the old ones, so the old deletes would silently disappear.
    let dropped = prepared(
        &adapter,
        vec![(
            ordinal(1),
            dv_fragment(
                "s3://b/wh/db/t/data/v.puffin",
                "s3://b/wh/db/t/data/a.parquet",
                Vec::new(),
            ),
        )],
        &handle.expected_targets(),
    );
    let validated = validate_prepared_set(&handle, &adapter, &dropped).expect("valid shape");
    let error =
        crate::commit::write_stack::control::validate_merged_old_references(&frozen, &validated)
            .expect_err("dropped old references");
    assert!(
        error.message().contains("merged 0 old references"),
        "{}",
        error.message()
    );
}

/// A statement that deletes from some data files and not others is the normal
/// case, not a corrupt one.
///
/// The session freezes old-delete references for *every* data file of the base
/// snapshot, because `begin_write` cannot know which files the predicate will
/// match. A writer stages an artifact only for the files it actually deleted a
/// row from, and the commit carries every untouched file's delete manifest
/// through unchanged -- so an unsuperseded frozen reference is a file this
/// statement did not touch, and nothing is lost by committing.
#[test]
fn a_frozen_data_file_this_statement_never_touched_is_not_an_error() {
    let (handle, adapter) = dv_session();
    let frozen = handle.frozen_old_references();
    assert!(
        frozen[&ordinal(1)].contains_key("s3://b/wh/db/t/data/a.parquet"),
        "the fixture must freeze at least one data file carrying an old delete"
    );

    // Nothing staged at all: a DELETE whose predicate matched no row of any
    // frozen file.
    let untouched = prepared(&adapter, Vec::new(), &handle.expected_targets());
    let validated = validate_prepared_set(&handle, &adapter, &untouched).expect("valid shape");
    crate::commit::write_stack::control::validate_merged_old_references(&frozen, &validated)
        .expect("an untouched frozen data file keeps its existing deletes");
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
        .settle(IcebergWriteSessionState::KnownCommitted { snapshot_id: 42 })
        .expect("settle committed");
    assert!(
        handle.begin_commit().is_err(),
        "a settled session cannot start another commit"
    );
}

#[test]
fn a_commit_unknown_session_stays_unknown_through_release() {
    let (handle, _adapter) = dv_session();
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
        .settle(IcebergWriteSessionState::KnownCommitted { snapshot_id: 7 })
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
                deletes: vec![delete_branch_plan(
                    branch,
                    vec![merge_target(
                        "s3://b/wh/db/t/data/a.parquet",
                        10,
                        Vec::new(),
                    )],
                )],
            },
        )
        .unwrap_or_else(|error| panic!("{} must plan: {error:?}", flavor.as_str()));
        assert_eq!(plans.len(), 2);
        assert_eq!(handle.branch_of(ordinal(1)), Some(branch));
        seen.insert(flavor, plans.len());
    }
    // Copy-on-write seals one branch per rewritten file rather than the
    // canonical shape, so it is covered by its own branch-planning tests and
    // deliberately cannot be sealed here.
    let (handle, plans) = flavor_session(
        plan_copy_on_write_branches(
            &session_material(copy_on_write_input_shape()),
            &cow_recipes(&["s3://b/wh/db/t/data/a.parquet"]),
        )
        .expect("row-mutation-copy-on-write must plan"),
    );
    assert_eq!(plans.len(), 2);
    assert_eq!(handle.expected_targets(), vec![ordinal(0), ordinal(1)]);
    seen.insert(IcebergWriteFlavor::RowMutationCopyOnWrite, plans.len());
    assert_eq!(seen.len(), 10, "every flavor must be covered");
}

// -------------------------------------------------------------------------
// Session flavors: how each one plans its logical branches.
// -------------------------------------------------------------------------

fn flavor_session(
    plan: IcebergSessionFlavorPlan,
) -> (IcebergCommitHandle, Vec<IcebergWriteTargetPlan>) {
    plan_branch_session(
        IcebergWriteSessionId::new(),
        IcebergBranchSessionPlanInput {
            flavor: plan.flavor,
            purpose: ConnectorWriteAdmissionPurpose::OrdinaryDml,
            table: table_facts(),
            base_version_digest: None,
            publication: plan.publication,
            staged_metadata: None,
            rewrite_inputs: plan.rewrite_inputs,
            copy_on_write: plan.copy_on_write,
            repartition: None,
            writer_table: None,
            branches: plan.branches,
        },
    )
    .expect("seal the flavor's branches")
}

/// The frozen copy-on-write recipes of a mutation that touched `old_files`,
/// plus the trailing append branch a folded `MERGE` insert reaches.
///
/// The read contract is assembled directly rather than frozen from a catalog:
/// these tests assert the branch structure a recipe set produces, and the
/// freeze that produces the recipes has its own unit tests beside it.
fn cow_recipes(old_files: &[&str]) -> Vec<IcebergCowBranchRecipe> {
    let mut recipes = old_files
        .iter()
        .map(|old_file| {
            IcebergCowBranchRecipe::for_test(
                IcebergCowBranchInput::Rewrite {
                    old_file: (*old_file).to_string(),
                    matched_row_ids: vec![100],
                },
                Some(cow_rewrite_source(old_file)),
            )
        })
        .collect::<Vec<_>>();
    recipes.push(IcebergCowBranchRecipe::for_test(
        IcebergCowBranchInput::Append,
        None,
    ));
    recipes
}

/// One rewrite branch's read contract, pinned to the single file it replaces.
fn cow_rewrite_source(
    old_file: &str,
) -> novarocks_spi::connector::write_stack::session::ConnectorWriteRewriteSource {
    let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("k1", DataType::Int64, true),
        arrow::datatypes::Field::new("_row_id", DataType::Int64, true),
        arrow::datatypes::Field::new("_last_updated_sequence_number", DataType::Int64, true),
    ]));
    novarocks_spi::connector::write_stack::session::ConnectorWriteRewriteSource::new(
        novarocks_spi::connector::ConnectorTableHandle::try_new(
            ConnectorInstanceId::parse("copy_on_write").expect("instance id"),
            bytes::Bytes::from_static(b"frozen-source"),
        )
        .expect("frozen source handle"),
        novarocks_spi::connector::ConnectorPinnedFileSet::try_new("db", "t", 77, [old_file])
            .expect("pinned source"),
        [5; 32],
        schema,
        Vec::new(),
        Vec::new(),
        None,
    )
}

/// A sealed copy-on-write session holding exactly these frozen branches.
fn cow_handle(branches: &[IcebergCowBranchInput]) -> IcebergCommitHandle {
    IcebergCommitHandle::try_new_sealed(
        IcebergWriteSessionId::new(),
        table_facts(),
        IcebergWriteFlavor::RowMutationCopyOnWrite,
        crate::commit::write_stack::domain::IcebergSessionFacts {
            purpose: ConnectorWriteAdmissionPurpose::OrdinaryDml,
            base_version_digest: None,
            publication: None,
            staged_metadata: None,
            rewrite_inputs: Vec::new(),
            copy_on_write: branches.to_vec(),
            repartition: None,
        },
        branches
            .iter()
            .enumerate()
            .map(|(index, _)| {
                crate::commit::write_stack::domain::IcebergSealedWriteTarget::new(
                    ordinal(u32::try_from(index).expect("ordinal")),
                    IcebergWriteBranch::Data,
                    std::collections::BTreeMap::new(),
                )
            })
            .collect(),
    )
    .expect("seal a copy-on-write session")
}

/// One replacement data file, as the commit half sees it after a fragment is
/// interpreted against the target's metadata.
fn written_data_file(path: &str) -> crate::commit::WrittenFile {
    crate::commit::WrittenFile {
        path: path.to_string(),
        format: crate::iceberg::spec::DataFileFormat::Parquet,
        content: crate::iceberg::spec::DataContentType::Data,
        partition_values: crate::iceberg::spec::Struct::empty(),
        partition_spec_id: 0,
        record_count: 3,
        file_size_in_bytes: 128,
        split_offsets: Vec::new(),
        column_sizes: std::collections::HashMap::new(),
        value_counts: std::collections::HashMap::new(),
        null_value_counts: std::collections::HashMap::new(),
        nan_value_counts: std::collections::HashMap::new(),
        lower_bounds: std::collections::HashMap::new(),
        upper_bounds: std::collections::HashMap::new(),
        key_metadata: None,
        referenced_data_file: None,
        equality_ids: None,
        first_row_id: None,
        content_offset: None,
        content_size_in_bytes: None,
        cardinality: None,
    }
}

/// Take the neutral session plan the frontend actually receives, so a route the
/// provider attached is asserted where SQL would read it.
fn neutral_plan(
    adapter: &IcebergWriteAdapter,
    sealed: (IcebergCommitHandle, Vec<IcebergWriteTargetPlan>),
) -> Result<ConnectorWriteSessionPlan, ConnectorError> {
    let (handle, targets) = sealed;
    session_plan_from_targets(adapter, handle, targets, None)
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

    let mut material = session_material(delete_input_shape(IcebergWriteBranch::DeletionVector));
    material.merge_targets = vec![merge_target(
        "s3://b/wh/db/t/data/a.parquet",
        10,
        Vec::new(),
    )];
    let plan = plan_ordinary_branches(IcebergWriteFlavor::RowMutationDeletionVector, &material)
        .expect("plan an ordinary row-level write");
    let sealed = neutral_plan(&adapter, flavor_session(plan)).expect("neutral plan");
    assert_eq!(sealed.targets().len(), 2);
    assert!(
        sealed
            .targets()
            .iter()
            .all(|target| target.route().is_none())
    );
}

#[test]
fn a_row_mutation_yields_one_routed_target_per_branch() {
    // A merge-on-read mutation carries both halves of a change event in one
    // row: the delete branch consumes the before-image identity and the data
    // branch consumes the after-image values, so a Replace reaches both.
    let adapter = adapter("row_mutation", 2);
    let mut material = session_material(merge_on_read_input_shape());
    material.merge_targets = vec![merge_target(
        "s3://b/wh/db/t/data/a.parquet",
        10,
        Vec::new(),
    )];
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
}

#[test]
fn a_delete_only_row_mutation_seals_one_routed_delete_branch() {
    // A deletion-vector input carries no after-image half, so the mutation
    // needs exactly one branch, and that branch accepts only a delete. Its
    // partition source fields travel as routing facts because SQL, not the
    // writer, supplies them.
    let adapter = adapter("delete_only", 3);
    let mut material = session_material(ConnectorWriteInputShape::DeletionVector {
        identity_fields: vec![
            binding("_file", 2, DataType::Utf8),
            binding("_pos", 3, DataType::Int64),
        ],
        partition_source_fields: vec![binding("k1", 1, DataType::Int64)],
    });
    material.merge_targets = vec![merge_target(
        "s3://b/wh/db/t/data/a.parquet",
        10,
        Vec::new(),
    )];
    let plan = plan_row_mutation_branches(&material).expect("plan a delete-only mutation");
    let sealed = neutral_plan(&adapter, flavor_session(plan)).expect("neutral plan");
    assert_eq!(sealed.expected_targets(), vec![ordinal(0)]);
    assert_eq!(
        effects(&sealed.targets()[0]),
        vec![ConnectorRowMutationEffect::Delete]
    );
    assert_eq!(
        sealed.targets()[0]
            .route()
            .expect("routed")
            .partition_fields()
            .len(),
        1
    );
}

/// A copy-on-write mutation that touched three data files must seal three
/// branches, not one.
///
/// One branch would have given every file's replacement rows the same writer,
/// and the prepared write set cannot notice: every fragment would name a target
/// the session really did seal. The branch order is the recipe order, so each
/// file's ordinal is the only name it needs.
#[test]
fn a_copy_on_write_row_mutation_seals_one_branch_per_rewritten_file() {
    let adapter = adapter("copy_on_write", 4);
    let recipes = cow_recipes(&[
        "s3://b/wh/db/t/data/a.parquet",
        "s3://b/wh/db/t/data/b.parquet",
    ]);
    let plan =
        plan_copy_on_write_branches(&session_material(copy_on_write_input_shape()), &recipes)
            .expect("plan a copy-on-write mutation");
    assert_eq!(plan.flavor, IcebergWriteFlavor::RowMutationCopyOnWrite);

    let sealed = neutral_plan(&adapter, flavor_session(plan)).expect("neutral plan");
    assert_eq!(
        sealed.expected_targets(),
        vec![ordinal(0), ordinal(1), ordinal(2)]
    );
    // Each rewrite branch replaces rows that already exist; only the trailing
    // append branch may receive a net-new row.
    assert_eq!(
        effects(&sealed.targets()[0]),
        vec![
            ConnectorRowMutationEffect::Delete,
            ConnectorRowMutationEffect::Replace
        ]
    );
    assert_eq!(
        effects(&sealed.targets()[1]),
        vec![
            ConnectorRowMutationEffect::Delete,
            ConnectorRowMutationEffect::Replace
        ]
    );
    assert_eq!(
        effects(&sealed.targets()[2]),
        vec![ConnectorRowMutationEffect::Insert]
    );
    // Two branches sharing a route key would make SQL's choice ambiguous and
    // one file's rows would vanish.
    let routes = sealed
        .targets()
        .iter()
        .map(|target| target.route().expect("routed").route_id())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(routes.len(), 3);
}

/// Each branch re-reads exactly the file it replaces.
///
/// A branch that replaced one file while reading another would silently drop or
/// duplicate rows, so the read contract travels with the target rather than
/// being re-derived beside it.
#[test]
fn each_copy_on_write_branch_carries_the_read_contract_of_its_own_file() {
    let adapter = adapter("copy_on_write_source", 4);
    let recipes = cow_recipes(&[
        "s3://b/wh/db/t/data/a.parquet",
        "s3://b/wh/db/t/data/b.parquet",
    ]);
    let plan =
        plan_copy_on_write_branches(&session_material(copy_on_write_input_shape()), &recipes)
            .expect("plan a copy-on-write mutation");
    let sealed = neutral_plan(&adapter, flavor_session(plan)).expect("neutral plan");

    let pinned = |index: usize| {
        sealed.targets()[index]
            .rewrite_source()
            .expect("a rewrite branch carries its read contract")
            .pinned_source()
            .files()
            .iter()
            .map(|file| file.to_string())
            .collect::<Vec<_>>()
    };
    assert_eq!(pinned(0), vec!["s3://b/wh/db/t/data/a.parquet".to_string()]);
    assert_eq!(pinned(1), vec!["s3://b/wh/db/t/data/b.parquet".to_string()]);
    // The append branch replaces nothing, so it reads nothing.
    assert!(sealed.targets()[2].rewrite_source().is_none());
}

/// The commit keys every replacement record by the write target ordinal.
///
/// Two rewritten files must reach two `CowUpdateTouchedFile` records, each
/// holding only its own artifacts. Merging them would attribute one file's
/// replacement to the other and retire a file nothing superseded.
#[test]
fn each_copy_on_write_branch_reaches_its_own_replacement_record() {
    let handle = cow_handle(&[
        IcebergCowBranchInput::Rewrite {
            old_file: "s3://b/wh/db/t/data/a.parquet".to_string(),
            matched_row_ids: vec![101, 100],
        },
        IcebergCowBranchInput::Rewrite {
            old_file: "s3://b/wh/db/t/data/b.parquet".to_string(),
            matched_row_ids: vec![300],
        },
        IcebergCowBranchInput::Append,
    ]);
    let rewrite = crate::commit::write_stack::control::cow_update_rewrite_set(
        &handle,
        &[
            (ordinal(0), written_data_file("s3://b/new/a-0.parquet")),
            (ordinal(1), written_data_file("s3://b/new/b-0.parquet")),
            (ordinal(0), written_data_file("s3://b/new/a-1.parquet")),
            (ordinal(2), written_data_file("s3://b/new/inserted.parquet")),
        ],
    )
    .expect("copy-on-write rewrite set")
    .expect("a copy-on-write session commits a rewrite set");

    assert_eq!(rewrite.touched_data_files.len(), 2);
    assert_eq!(
        rewrite.touched_data_files[0].old_file,
        "s3://b/wh/db/t/data/a.parquet"
    );
    assert_eq!(
        rewrite.touched_data_files[0].new_files,
        vec![
            "s3://b/new/a-0.parquet".to_string(),
            "s3://b/new/a-1.parquet".to_string()
        ]
    );
    assert_eq!(
        rewrite.touched_data_files[1].new_files,
        vec!["s3://b/new/b-0.parquet".to_string()]
    );
    // The row ids reach the commit exactly as the selection reported them: the
    // minimum becomes the replacement manifest's `first_row_id`, so a reordered
    // or synthesized value would corrupt row lineage rather than fail a check.
    assert_eq!(rewrite.touched_data_files[0].row_ids, vec![101, 100]);
    assert_eq!(rewrite.touched_data_files[1].row_ids, vec![300]);
    assert_eq!(rewrite.updated_row_ids, vec![100, 101, 300]);
    // A net-new row belongs to no rewritten file and is added beside them.
    assert_eq!(
        rewrite
            .appended_files
            .iter()
            .map(|file| file.path.clone())
            .collect::<Vec<_>>(),
        vec!["s3://b/new/inserted.parquet".to_string()]
    );
    assert_eq!(rewrite.base_snapshot_id, 77);
}

/// A copy-on-write session cannot be sealed from the canonical data shape.
///
/// Its branch count follows the files its match selection touched, so a session
/// sealed without those recipes would have nothing to replace and would commit
/// a rewrite that retires no file at all.
#[test]
fn a_copy_on_write_session_without_frozen_branches_is_refused() {
    let error = plan_write_session(
        IcebergWriteSessionId::new(),
        IcebergWriteSessionPlanInput {
            flavor: IcebergWriteFlavor::RowMutationCopyOnWrite,
            purpose: ConnectorWriteAdmissionPurpose::OrdinaryDml,
            table: table_facts(),
            base_version_digest: None,
            staged_metadata: None,
            data: data_branch_plan(),
            deletes: Vec::new(),
        },
    )
    .expect_err("a copy-on-write session must carry its frozen branches");
    assert!(
        error
            .message()
            .contains("frozen copy-on-write branch per target"),
        "unexpected message: {}",
        error.message()
    );
}

/// A copy-on-write mutation reaching the row-mutation flavor is refused.
///
/// The row-mutation flavor carries no match selection, so it cannot know how
/// many files the statement touched. Admitting it would seal one branch for a
/// statement that rewrites several.
#[test]
fn a_copy_on_write_identity_is_refused_by_the_row_mutation_flavor() {
    let error = plan_row_mutation_branches(&session_material(copy_on_write_input_shape()))
        .expect_err("a copy-on-write mutation needs its own session flavor");
    assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
    assert!(
        error.message().contains("copy-on-write session flavor"),
        "unexpected message: {}",
        error.message()
    );
}

#[test]
fn a_row_mutation_whose_branches_share_a_route_key_is_refused() {
    // Two branches sharing a route key would make the router's choice
    // ambiguous, and the loser's rows would vanish. The provider's own route
    // key is derived per branch and never collides, so the refusal is proven by
    // sealing a mutation whose two branches were given the same key.
    let adapter = adapter("route_collision", 5);
    let collided = ConnectorWriteRouteFacts::try_new(
        ConnectorWriteRouteId::from_bytes([9; 32]),
        vec![ConnectorRowMutationEffect::Delete],
        Vec::new(),
        Vec::new(),
    )
    .expect("route facts");
    let sealed = plan_branch_session(
        IcebergWriteSessionId::new(),
        IcebergBranchSessionPlanInput {
            flavor: IcebergWriteFlavor::RowMutationDeletionVector,
            purpose: ConnectorWriteAdmissionPurpose::OrdinaryDml,
            table: table_facts(),
            base_version_digest: None,
            publication: None,
            staged_metadata: None,
            rewrite_inputs: Vec::new(),
            copy_on_write: Vec::new(),
            repartition: None,
            writer_table: None,
            branches: vec![
                IcebergWriteBranchPlan::Data {
                    plan: data_branch_plan(),
                    route: Some(collided.clone()),
                },
                IcebergWriteBranchPlan::Delete {
                    plan: delete_branch_plan(
                        IcebergWriteBranch::DeletionVector,
                        vec![merge_target(
                            "s3://b/wh/db/t/data/a.parquet",
                            10,
                            Vec::new(),
                        )],
                    ),
                    route: Some(collided),
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
            delete_targets: Vec::new(),
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
            let delete_targets = group
                .data_files
                .iter()
                .map(|file| merge_target(&file.path, 1, Vec::new()))
                .collect();
            IcebergFrozenRewriteBranch {
                group,
                delete_targets,
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
    // `run_iceberg_commit` refuses `CommitOpKind::SelectedRewrite` outright
    // when its frozen file set is absent, so before the session carried its
    // frozen groups every rewrite commit failed with "requires its frozen file
    // set". This asserts the session now supplies exactly the union it froze.
    let live_deletes = std::collections::BTreeSet::from([
        "s3://b/wh/db/t/data/d0.puffin".to_string(),
        "s3://b/wh/db/t/data/d1.puffin".to_string(),
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
            "s3://b/wh/db/t/data/a/f.parquet".to_string(),
            "s3://b/wh/db/t/data/b/f.parquet".to_string(),
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
    let groups = position_delete_rewrite_groups(&["a", "b"]);
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
    // Each branch owns exactly its own group's data file, which is what makes
    // the unique-owner proof pass with two delete branches in one session.
    // Ordinals follow the frozen group order, which is the planner's digest
    // order rather than the partition order it was cut from.
    assert_eq!(handle.delete_owner().len(), 2);
    for (index, branch) in groups.iter().enumerate() {
        for file in &branch.group.data_files {
            assert_eq!(
                handle
                    .delete_owner()
                    .get(&file.path)
                    .map(|target| target.get()),
                Some(index as u32),
                "{} must be owned by its own group's branch",
                file.path
            );
        }
    }
    // A rewrite reads the old vectors through its own scan, so its writer has
    // no old reference to merge -- and the commit still retires them.
    assert!(
        handle
            .frozen_old_references()
            .values()
            .flat_map(|files| files.values())
            .all(|references| references.is_empty())
    );
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
            "s3://b/wh/db/t/data/a/d0.puffin".to_string(),
            "s3://b/wh/db/t/data/a/d1.puffin".to_string(),
        ])
    );
    assert_eq!(
        files.data_paths,
        std::collections::BTreeSet::from(["s3://b/wh/db/t/data/a/f.parquet".to_string()])
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

    let error = IcebergCommitHandle::try_new_sealed(
        IcebergWriteSessionId::new(),
        table_facts(),
        IcebergWriteFlavor::Append,
        crate::commit::write_stack::domain::IcebergSessionFacts {
            purpose: ConnectorWriteAdmissionPurpose::OrdinaryDml,
            base_version_digest: None,
            publication: None,
            staged_metadata: None,
            rewrite_inputs: vec![
                crate::commit::write_stack::domain::IcebergFrozenRewriteBranchInput::try_new(
                    std::collections::BTreeSet::from(["s3://b/wh/db/t/data/a.parquet".to_string()]),
                    std::collections::BTreeSet::new(),
                )
                .expect("frozen rewrite input"),
            ],
            copy_on_write: Vec::new(),
            repartition: None,
        },
        vec![
            crate::commit::write_stack::domain::IcebergSealedWriteTarget::new(
                ordinal(0),
                IcebergWriteBranch::Data,
                std::collections::BTreeMap::new(),
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
    // swap `dispatch_commit` already performs against the frozen snapshot, so
    // it must not also take the distributed external write fence. Every other
    // flavor keeps it.
    for flavor in [
        IcebergWriteFlavor::Append,
        IcebergWriteFlavor::Overwrite,
        IcebergWriteFlavor::PartitionOverwrite,
        IcebergWriteFlavor::RowMutationPositionDelete,
        IcebergWriteFlavor::RowMutationDeletionVector,
        IcebergWriteFlavor::RowMutationCopyOnWrite,
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
        let plan = plan_managed_publication_branches(
            &session_material(data_input_shape()),
            publication_facts(
                technique,
                ConnectorManagedPublicationEmptyInputDisposition::CommitEmptyWrite,
            ),
        )
        .expect("plan a managed publication");
        let (handle, _) = flavor_session(plan);
        let publication = handle
            .publication()
            .expect("publication facts reach finish");
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
            publication: Some(publication_facts(
                ConnectorManagedPublicationTechnique::Full,
                ConnectorManagedPublicationEmptyInputDisposition::CommitEmptyWrite,
            )),
            staged_metadata: None,
            rewrite_inputs: Vec::new(),
            copy_on_write: Vec::new(),
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
    let plan = plan_managed_publication_branches(
        &session_material(merge_on_read_input_shape()),
        publication_facts_with_shape(
            ConnectorManagedPublicationTechnique::Incremental,
            ConnectorManagedPublicationEmptyInputDisposition::CommitEmptyWrite,
            ConnectorManagedPublicationShape::RowMutation,
        ),
    )
    .expect("plan a change-stream publication");
    // It stays one publication: the commit is still decided by the publication
    // facts, not by a second row-mutation flavor.
    assert_eq!(plan.flavor, IcebergWriteFlavor::ManagedPublication);
    assert!(plan.publication.is_some());

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
    let plan = plan_managed_publication_branches(
        &session_material(data_input_shape()),
        publication_facts_with_shape(
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
        plan_managed_publication_branches(
            &session_material(data_input_shape()),
            publication_facts_with_shape(
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
    let error = plan_managed_publication_branches(
        &session_material(data_input_shape()),
        publication_facts_with_shape(
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
        plan_managed_publication_branches(
            &session_material(merge_on_read_input_shape()),
            publication_facts_with_shape(
                ConnectorManagedPublicationTechnique::Incremental,
                ConnectorManagedPublicationEmptyInputDisposition::CommitEmptyWrite,
                ConnectorManagedPublicationShape::RowMutation,
            ),
        )
        .expect("plan a change-stream publication"),
    );
    assert_eq!(delta.commit_op_kind(), CommitOpKind::RowDeltaDvFromFiles);
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
            plan_managed_publication_branches(
                &session_material(data_input_shape()),
                publication_facts_with_shape(
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
    let error = plan_managed_publication_branches(
        &session_material(merge_on_read_input_shape()),
        publication_facts_with_shape(
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

    // A copy-on-write change stream is refused for a sharper reason: it seals
    // only a data branch, so its publication would resolve to a plain append and
    // publish every after-image while the before-images stayed live.
    let error = plan_managed_publication_branches(
        &session_material(copy_on_write_input_shape()),
        publication_facts_with_shape(
            ConnectorManagedPublicationTechnique::Incremental,
            ConnectorManagedPublicationEmptyInputDisposition::CommitEmptyWrite,
            ConnectorManagedPublicationShape::RowMutation,
        ),
    )
    .expect_err("a copy-on-write refresh is not supported");
    assert!(
        error
            .message()
            .contains("copy-on-write refresh is not supported"),
        "unexpected message: {}",
        error.message()
    );
}

#[test]
fn a_change_stream_publication_freezes_the_old_deletes_it_supersedes() {
    // A merge-on-read refresh stages a deletion vector that must account for
    // every delete artifact already attached to the data file it touches. The
    // session freezes those references at admission, and a publication that
    // skipped the freeze would stage an artifact that silently dropped them.
    let signed = merge_on_read_input_shape();
    assert!(session_freezes_old_deletes(
        &publication_flavor(
            ConnectorManagedPublicationTechnique::Incremental,
            ConnectorManagedPublicationShape::RowMutation,
        ),
        &signed,
    ));
    // A wholesale republication seals no delete branch, so it has nothing to
    // supersede and must not pay for the read.
    assert!(!session_freezes_old_deletes(
        &publication_flavor(
            ConnectorManagedPublicationTechnique::Full,
            ConnectorManagedPublicationShape::Data,
        ),
        &data_input_shape(),
    ));
    assert!(!session_freezes_old_deletes(
        &publication_flavor(
            ConnectorManagedPublicationTechnique::Incremental,
            ConnectorManagedPublicationShape::RowMutation,
        ),
        &data_input_shape(),
    ));
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
    assert_eq!(delete_route.partition_fields(), &[file_token]);
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
        plan_managed_publication_branches(
            &session_material(data_input_shape()),
            publication_facts(
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
fn a_managed_publication_stamps_its_publication_id_onto_the_snapshot_it_commits() {
    // The publication fence fast-forwards a staged refresh only after reading
    // `MV_PUBLICATION_ID_PROP` back off the staging snapshot's own summary
    // (`catalog_control::catalog_mutation` -> `snapshot_matches_publication_marker`).
    // Before the session retained the publication id, its commit wrote only the
    // write-session marker, so every refresh it published was a snapshot the
    // fence could never claim -- and silently, because nothing before the fence
    // looks for the property.
    let (handle, _) = flavor_session(
        plan_managed_publication_branches(
            &session_material(data_input_shape()),
            publication_facts(
                ConnectorManagedPublicationTechnique::Full,
                ConnectorManagedPublicationEmptyInputDisposition::CommitEmptyWrite,
            ),
        )
        .expect("plan a managed publication"),
    );
    let properties = crate::commit::write_stack::control::session_snapshot_properties(&handle, 9)
        .expect("build the commit's snapshot properties");

    // The reconciliation marker stays: an unknown outcome still has to be
    // adjudicable.
    assert_eq!(
        properties
            .get(crate::commit::write_stack::control::ICEBERG_WRITE_SESSION_MARKER_PROPERTY)
            .map(String::as_str),
        Some(handle.session_id().to_string().as_str())
    );

    let snapshot = crate::iceberg::spec::Snapshot::builder()
        .with_snapshot_id(300)
        .with_sequence_number(1)
        .with_timestamp_ms(1)
        .with_manifest_list("file:/tmp/manifest-list.avro".to_string())
        .with_summary(crate::iceberg::spec::Summary {
            operation: crate::iceberg::spec::Operation::Overwrite,
            additional_properties: properties.clone().into_iter().collect(),
        })
        .with_schema_id(0)
        .build();
    assert!(
        crate::commit::snapshot_matches_publication_marker(
            &snapshot,
            &crate::commit::MvPublicationSnapshotMarker {
                publication_id: publication_id(),
            },
        ),
        "the fence must be able to claim the snapshot this commit creates"
    );

    // The provenance rides along on the same snapshot, seeded with the rows
    // this write staged; the commit action refines it to `total-records`.
    let provenance = crate::commit::MvPublicationProvenanceV2::from_snapshot_summary(&snapshot)
        .expect("decode the provenance")
        .expect("a publication snapshot carries provenance");
    assert_eq!(provenance.publication_id, publication_id());
    assert_eq!(provenance.technique, crate::commit::RefreshTechnique::Full);
    assert_eq!(provenance.rows, 9);

    // An ordinary write carries no publication facts at all: the id belongs to
    // the one session that publishes under it.
    let (append, _) = flavor_session(
        plan_ordinary_branches(
            IcebergWriteFlavor::Append,
            &session_material(data_input_shape()),
        )
        .expect("plan an ordinary append"),
    );
    assert_eq!(
        crate::commit::write_stack::control::session_snapshot_properties(&append, 9)
            .expect("build the commit's snapshot properties")
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        vec![crate::commit::write_stack::control::ICEBERG_WRITE_SESSION_MARKER_PROPERTY]
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
            plan_managed_publication_branches(
                &session_material(data_input_shape()),
                publication_facts(ConnectorManagedPublicationTechnique::Full, empty_input),
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
    let outcome = settle_empty_write_without_commit(&aborts).expect("settle the empty write");
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

    // A rewrite with nothing to rewrite is the same no-op, and it is decided by
    // the flavor rather than by a publication disposition.
    let (rewrite, _) = flavor_session(
        plan_distributed_rewrite_branches(
            &session_material(data_input_shape()),
            DATA_FILE_REWRITE,
            &[],
        )
        .expect("plan an empty distributed rewrite"),
    );
    assert_eq!(
        rewrite.empty_write_decision(),
        IcebergEmptyWriteDecision::SkipExternalCommit
    );
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
fn unreachable_rest_runtime() -> (
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
        ],
    )
    .expect("configuration");
    let binding = IcebergReadBinding::new(
        None,
        FsAccessResolver::new(),
        Arc::new(TokioFileIoRuntime::new(handle.clone())),
        Arc::new(TokioFileTaskSpawner::new(handle.clone())),
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
    let location = "file:///tmp/novarocks-staged-session/table";
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
        table: Arc::from("db.staged"),
        target_ref: novarocks_spi::connector::ConnectorWriteTargetRef::main(),
        intent: novarocks_spi::connector::ConnectorWriteIntent::Append,
        purpose: ConnectorWriteAdmissionPurpose::OrdinaryDml,
        input: novarocks_spi::connector::ConnectorWriteInputRequest::Data {
            fields: vec![novarocks_spi::connector::ConnectorWriteFieldRequest::new(
                Field::new("id", DataType::Int64, false),
            )],
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
    let adapter = crate::commit::write_stack::runtime::build_write_adapter(
        descriptor("unit"),
        CatalogHandle::new(
            ConnectorInstanceId::parse("unit").expect("instance id"),
            CatalogVersion::from_bytes([1; 32]),
        ),
    );
    let set = prepared(
        &adapter,
        vec![(
            ordinal(0),
            data_fragment("file:///tmp/novarocks-staged-session/table/data/a.parquet"),
        )],
        &[ordinal(0)],
    );

    let outcome = control
        .finish_write(ConnectorWriteFinishRequest {
            commit: plan.commit_handle(),
            prepared: set,
            statistics: Vec::new(),
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
    assert!(!receipt.payload().is_empty());

    // The single terminal is claimed, so a second finish cannot mint a second
    // receipt for the same sealed artifacts.
    control
        .finish_write(ConnectorWriteFinishRequest {
            commit: plan.commit_handle(),
            prepared: prepared(&adapter, Vec::new(), &[ordinal(0)]),
            statistics: Vec::new(),
            context: request_context(),
        })
        .expect_err("a sealed session is finished");
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
    let adapter = crate::commit::write_stack::runtime::build_write_adapter(
        descriptor("unit"),
        CatalogHandle::new(
            ConnectorInstanceId::parse("unit").expect("instance id"),
            CatalogVersion::from_bytes([1; 32]),
        ),
    );

    let outcome = control
        .finish_write(ConnectorWriteFinishRequest {
            commit: plan.commit_handle(),
            prepared: prepared(&adapter, Vec::new(), &[ordinal(0)]),
            statistics: Vec::new(),
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
    // And it froze no old-delete reference, because it supersedes nothing.
    assert!(handle.frozen_old_references().is_empty());

    let sealed = neutral_plan(&adapter, (handle, targets)).expect("neutral plan");
    assert_eq!(sealed.expected_targets(), vec![ordinal(0)]);
    assert!(sealed.targets()[0].route().is_none());
}

/// An equality delete supersedes no existing artifact, so the session must not
/// pay for -- or freeze -- an old-delete merge.
#[test]
fn an_equality_delete_session_freezes_no_old_deletes() {
    assert!(!session_freezes_old_deletes(
        &ConnectorWriteSessionFlavor::Ordinary,
        &equality_delete_input_shape(),
    ));
}
