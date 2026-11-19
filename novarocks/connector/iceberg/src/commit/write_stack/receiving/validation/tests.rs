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

//! Validate the shared admission rules and record-local index deltas.

use crate::commit::write_stack::domain::{
    IcebergCreationScope, IcebergDataFileArtifact, IcebergPositionDeleteFileArtifact,
    IcebergWriteFlavor, IcebergWriteSessionId,
};
use crate::commit::write_stack::planning::{IcebergWriteSessionPlanInput, plan_write_session};
use crate::commit::write_stack::test_support::{
    data_branch_plan, delete_branch_plan, dv_artifact, sample_metrics, sample_partition,
    table_facts,
};
use crate::commit::write_stack::tests::{
    data_fragment, dv_session, final_vector, fixed_delete_recipe, fixed_dv_fragment,
};
use crate::iceberg::spec::{DataFileBuilder, DataFileFormat};
use novarocks_spi::connector::ConnectorWriteAdmissionPurpose;

use super::*;

fn ordinal(value: u32) -> WriteTargetOrdinal {
    WriteTargetOrdinal::try_new(value).unwrap()
}

#[test]
fn repeated_entry_in_a_new_record_does_not_install_the_valid_prefix() {
    let (handle, _) = dv_session();
    let mut statement = StatementIndexes::default();
    let first = data_fragment("s3://b/first.parquet");
    let prefix = data_fragment("s3://b/prefix.parquet");

    let mut record = FragmentAdmission::for_record(&handle, &statement);
    let bytes = record
        .admit_record_fragment(ordinal(0), &first, |_| Ok(false), None)
        .unwrap();
    let pending = record.finish_record();
    assert!(matches!(
        pending.0.entries.first(),
        Some(EntryKey::Data(Cow::Borrowed(_)))
    ));
    let delta = pending.into_owned();
    let Some(EntryKey::Data(Cow::Owned(path))) = delta.0.entries.first() else {
        panic!("expected owned data path after funding");
    };
    assert_eq!(bytes, size_of::<EntryKey<'_>>() + path.capacity());
    statement.apply(delta);

    let mut record = FragmentAdmission::for_record(&handle, &statement);
    record
        .admit_record_fragment(ordinal(0), &prefix, |_| Ok(false), None)
        .unwrap();
    let error = record
        .admit_record_fragment(ordinal(0), &first, |_| Ok(false), None)
        .unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
    assert!(error.message().contains("repeats staged artifact"));
    drop(record);

    let mut replay = FragmentAdmission::for_record(&handle, &statement);
    replay
        .admit_record_fragment(ordinal(0), &prefix, |_| Ok(false), None)
        .unwrap();
}

fn created(
    handle: &IcebergWriteCommitSession,
    fragment: IcebergCommitFragment,
    format: IcebergFileFormat,
    create_id: [u8; 16],
    content_digest: [u8; 32],
    physical: ConnectorWriterPhysicalContext,
) -> IcebergCommitFragment {
    let identity = handle
        .creation_scope()
        .unwrap()
        .identity(physical, fragment.new_object_path().unwrap(), format)
        .unwrap();
    let bytes = fragment.metrics().unwrap().file_size_in_bytes();
    fragment
        .try_with_creation(
            IcebergCreationReceipt::try_new(physical, create_id, identity, bytes, content_digest)
                .unwrap(),
        )
        .unwrap()
}

fn managed_session() -> IcebergWriteCommitSession {
    let (handle, _) = dv_session();
    let scope = IcebergCreationScope::try_new(handle.session_id(), [2; 16], [3; 32]).unwrap();
    handle.try_with_creation_scope(scope).unwrap()
}

#[test]
fn a_create_id_cannot_name_another_physical_path_in_a_later_record() {
    let handle = managed_session();
    let physical = ConnectorWriterPhysicalContext::new([1; 16], 3, [4; 16], 0, 0);
    let files = ["s3://b/a.parquet", "s3://b/b.parquet"].map(|path| {
        created(
            &handle,
            data_fragment(path),
            IcebergFileFormat::Parquet,
            [4; 16],
            [5; 32],
            physical,
        )
    });
    let mut statement = StatementIndexes::default();
    let mut record = FragmentAdmission::for_record(&handle, &statement);
    record
        .admit_record_fragment(ordinal(0), &files[0], |value| Ok(value == &physical), None)
        .unwrap();
    let delta = record.finish_record().into_owned();
    statement.apply(delta);

    let mut record = FragmentAdmission::for_record(&handle, &statement);
    let error = record
        .admit_record_fragment(ordinal(0), &files[1], |value| Ok(value == &physical), None)
        .unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
    assert!(
        error
            .message()
            .contains("differs from its completed writer cohort")
    );
}

#[test]
fn shared_puffin_requires_exactly_the_same_creation_facts() {
    let handle = managed_session();
    let physical = ConnectorWriterPhysicalContext::new([1; 16], 3, [4; 16], 0, 1);
    let path = "s3://b/shared.puffin";
    let first = created(
        &handle,
        fixed_dv_fragment(path, "s3://b/first.parquet", Vec::new()),
        IcebergFileFormat::Puffin,
        [4; 16],
        [5; 32],
        physical,
    );
    let second = |digest| {
        created(
            &handle,
            final_vector(dv_artifact(path, "s3://b/second.parquet", 3, 1024, 132, 64).unwrap()),
            IcebergFileFormat::Puffin,
            [4; 16],
            digest,
            physical,
        )
    };
    let mut statement = StatementIndexes::default();
    let mut record = FragmentAdmission::for_record(&handle, &statement);
    let first_bytes = record
        .admit_record_fragment(ordinal(1), &first, |value| Ok(value == &physical), None)
        .unwrap();
    let delta = record.finish_record().into_owned();
    statement.apply(delta);

    let mismatched = second([6; 32]);
    let matching = second([5; 32]);
    let mut record = FragmentAdmission::for_record(&handle, &statement);
    let error = record
        .admit_record_fragment(
            ordinal(1),
            &mismatched,
            |value| Ok(value == &physical),
            None,
        )
        .unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
    assert!(
        error
            .message()
            .contains("creation facts disagree about a physical object")
    );
    drop(record);
    let mut record = FragmentAdmission::for_record(&handle, &statement);
    let shared_bytes = record
        .admit_record_fragment(ordinal(1), &matching, |value| Ok(value == &physical), None)
        .unwrap();
    assert!(
        first_bytes > shared_bytes,
        "a known Puffin needs no new physical creation keys"
    );
}

#[test]
fn two_v3_delete_effects_cannot_own_one_target_across_records() {
    let (handle, _) = dv_session();
    let target = "s3://b/target.parquet";
    let a = fixed_dv_fragment("s3://b/a.puffin", target, Vec::new());
    let b = fixed_dv_fragment("s3://b/b.puffin", target, Vec::new());
    let mut statement = StatementIndexes::default();
    let mut record = FragmentAdmission::for_record(&handle, &statement);
    record
        .admit_record_fragment(ordinal(1), &a, |_| Ok(false), None)
        .unwrap();
    let delta = record.finish_record().into_owned();
    statement.apply(delta);
    let mut record = FragmentAdmission::for_record(&handle, &statement);
    let error = record
        .admit_record_fragment(ordinal(1), &b, |_| Ok(false), None)
        .unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
    assert!(error.message().contains("more than one delete artifact"));
}

#[test]
fn distributed_v2_records_can_repeat_a_target_with_an_exact_matching_partition() {
    let (handle, _) = plan_write_session(
        IcebergWriteSessionId::new(),
        IcebergWriteSessionPlanInput {
            flavor: IcebergWriteFlavor::RowMutationPositionDelete,
            purpose: ConnectorWriteAdmissionPurpose::OrdinaryDml,
            table: table_facts(),
            base_version_digest: None,
            staged_metadata: None,
            data: data_branch_plan(),
            deletes: vec![delete_branch_plan(IcebergWriteBranch::PositionDelete, None)],
        },
    )
    .unwrap();
    let recipe = fixed_delete_recipe(handle.table());
    let handle = handle.with_mutation_read(recipe);
    let ordinal = handle
        .targets()
        .iter()
        .find(|target| target.branch() == IcebergWriteBranch::PositionDelete)
        .unwrap()
        .ordinal();
    let position = |path: &str, partition: IcebergArtifactPartition| {
        IcebergCommitFragment::position_delete_file(
            IcebergPositionDeleteFileArtifact::try_new(
                path.into(),
                partition,
                sample_metrics(2, 1024),
                "s3://b/target.parquet".into(),
                Vec::new(),
            )
            .unwrap(),
        )
    };
    let original = sample_partition();
    let different = IcebergArtifactPartition::try_new(
        "different".into(),
        original.null_fingerprint().to_owned(),
        original.partition_spec_id(),
        original.descriptor().clone(),
    )
    .unwrap();
    let first = position("s3://b/a.parquet", original.clone());
    let second = position("s3://b/b.parquet", original.clone());
    let third = position("s3://b/c.parquet", different);
    let mut statement = StatementIndexes::default();
    let mut packet = FragmentAdmission::for_record(&handle, &statement);
    packet
        .admit_record_fragment(ordinal, &first, |_| Ok(false), None)
        .unwrap();
    prove_partition_refusal(packet.admit_record_fragment(ordinal, &third, |_| Ok(false), None));
    drop(packet);
    let mut packet = FragmentAdmission::for_record(&handle, &statement);
    packet
        .admit_record_fragment(ordinal, &first, |_| Ok(false), None)
        .unwrap();
    let delta = packet.finish_record().into_owned();
    statement.apply(delta);

    let mut packet = FragmentAdmission::for_record(&handle, &statement);
    packet
        .admit_record_fragment(ordinal, &second, |_| Ok(false), Some(&original))
        .unwrap();
    prove_partition_refusal(packet.admit_record_fragment(
        ordinal,
        &third,
        |_| Ok(false),
        Some(&original),
    ));

    // Whole-set receiving continues to defer exact partition grouping to
    // its original materialized publication step.
    let mut whole = FragmentAdmission::for_whole_set(&handle);
    whole
        .admit_whole_fragment(ordinal, &first, |_| Ok(false))
        .unwrap();
    whole
        .admit_whole_fragment(ordinal, &third, |_| Ok(false))
        .unwrap();
}

fn prove_partition_refusal(result: Result<usize, ConnectorError>) {
    let error = result.unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
    assert_eq!(
        error.message(),
        "Iceberg materialized step has conflicting effects for one data file"
    );
}

#[test]
fn materialized_data_rejects_assigned_first_row_id_during_compact_admission() {
    let (handle, _) = dv_session();
    let fragment = IcebergCommitFragment::data_file(
        IcebergDataFileArtifact::try_new(
            "s3://b/new.parquet".into(),
            IcebergFileFormat::Parquet,
            sample_partition(),
            sample_metrics(1, 1024),
            Some(91),
        )
        .unwrap(),
    );
    let statement = StatementIndexes::default();
    let mut record = FragmentAdmission::for_record(&handle, &statement);
    let error = record
        .admit_record_fragment(ordinal(0), &fragment, |_| Ok(false), None)
        .unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    assert_eq!(
        error.message(),
        "unsupported materialized row-delta addition"
    );
}

#[test]
fn materialized_projection_requires_inheritance_and_the_original_declared_target() {
    let path = "s3://b/new-delete.parquet";
    let target = "s3://b/target.parquet";
    let file = DataFileBuilder::default()
        .content(DataContentType::PositionDeletes)
        .file_path(path.into())
        .file_format(DataFileFormat::Parquet)
        .record_count(1)
        .file_size_in_bytes(100)
        .referenced_data_file(Some(target.into()))
        .build()
        .unwrap();
    let admitted = EntryIdentity::DeleteFile { path: path.into() };
    let explicit = AddedContent::rewritten_data(file.clone(), 0, 7).unwrap();
    let error = validate_materialized_addition(&admitted, Some(target), &explicit).unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    assert_eq!(
        error.message(),
        "materialized write has a repeated or non-inheriting addition"
    );
    let inheriting = AddedContent::new_logical_data(file, 0).unwrap();
    let error =
        validate_materialized_addition(&admitted, Some("s3://b/other.parquet"), &inheriting)
            .unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    assert_eq!(
        error.message(),
        "materialized output references a different target"
    );
    validate_materialized_addition(&admitted, Some(target), &inheriting).unwrap();
}
