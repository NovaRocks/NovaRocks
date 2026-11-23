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

//! Observes retained target witnesses, effect projections and their custody at the original arrival.

use super::*;
use crate::commit::write_stack::control::publication::observation_target_bytes;
use crate::commit::write_stack::domain::{
    IcebergCommitFragment, IcebergCreationReceipt, IcebergCreationScope,
    IcebergDeleteTargetLocator, IcebergFinalDeleteEffect, IcebergFinalDeleteKind,
    IcebergPositionDeleteFileArtifact, IcebergWriteSessionId,
};
use crate::commit::write_stack::planning::{IcebergWriteSessionPlanInput, plan_write_session};
use crate::commit::write_stack::test_support::{
    data_branch_plan, delete_branch_plan, paid_fragments, sample_metrics, sample_partition,
    table_facts,
};
use crate::commit::write_stack::tests::{dv_session, fixed_delete_recipe, fixed_dv_fragment};
use novarocks_spi::connector::ConnectorWriteAdmissionPurpose;

fn register_delete(
    handle: &IcebergWriteCommitSession,
    physical: Option<ConnectorWriterPhysicalContext>,
) -> (ConnectorWriteQueryIdentity, WriteTargetOrdinal) {
    let target = handle
        .targets()
        .iter()
        .find(|target| target.branch().writes_deletes())
        .unwrap()
        .ordinal();
    let query = write_query_identity();
    let mut proof = physical.map(|physical| received_write_proof(target, physical));
    handle
        .with_active_receiving(|received| {
            received.begin_query(query, &[target], &mut proof, handle.targets(), None)
        })
        .unwrap();
    (query, target)
}

fn arrive(
    handle: &IcebergWriteCommitSession,
    adapter: &IcebergWriteAdapter,
    query: ConnectorWriteQueryIdentity,
    target: WriteTargetOrdinal,
    fragment: IcebergCommitFragment,
) -> Result<(), ConnectorError> {
    let (fragments, author) = paid_fragments(adapter, vec![adapter.wrap_commit_fragment(fragment)]);
    handle.with_active_receiving(|received| {
        received.arrive_original(
            handle,
            adapter,
            query,
            fragments
                .into_iter()
                .map(|fragment| (target, fragment))
                .collect(),
            &author,
        )
    })
}

#[test]
fn v2_keeps_one_exact_partition_witness_and_every_position_file() {
    let (handle, _) = plan_write_session(
        IcebergWriteSessionId::new(),
        IcebergWriteSessionPlanInput {
            flavor: IcebergWriteFlavor::RowMutationPositionDelete,
            purpose: ConnectorWriteAdmissionPurpose::OrdinaryDml,
            table: table_facts(),
            base_version_digest: None,
            staged_metadata: None,
            data: data_branch_plan(),
            deletes: vec![delete_branch_plan(IcebergWriteBranch::PositionDelete)],
        },
    )
    .unwrap();
    let recipe = fixed_delete_recipe(handle.table());
    let handle = handle.with_mutation_read(recipe);
    let adapter = adapter("unit", 1);
    let (query, ordinal) = register_delete(&handle, None);
    let target = "s3://b/target.parquet";
    let original = sample_partition();
    let different = IcebergArtifactPartition::try_new(
        "different".into(),
        String::new(),
        original.partition_spec_id(),
        original.descriptor().clone(),
    )
    .unwrap();
    let position = |path: &str, partition: IcebergArtifactPartition| {
        IcebergCommitFragment::position_delete_file(
            IcebergPositionDeleteFileArtifact::try_new(
                path.into(),
                partition,
                sample_metrics(2, 1024),
                target.into(),
                Vec::new(),
            )
            .unwrap(),
        )
    };
    arrive(
        &handle,
        &adapter,
        query,
        ordinal,
        position("s3://b/a.parquet", original.clone()),
    )
    .unwrap();
    let error = arrive(
        &handle,
        &adapter,
        query,
        ordinal,
        position("s3://b/b.parquet", different),
    )
    .unwrap_err();
    assert!(error.message().contains("conflicting effects"));
    arrive(
        &handle,
        &adapter,
        query,
        ordinal,
        position("s3://b/b.parquet", original.clone()),
    )
    .unwrap();
    handle
        .with_active_receiving(|received| {
            let compact = received.compact_files();
            assert_eq!(compact.v2_targets.len(), 1);
            assert_eq!(compact.v2_targets[target], original);
            assert_eq!(compact.counts.position_deletes.files, 2);
            assert_eq!(compact.entries().count(), 2);
            Ok(())
        })
        .unwrap();
}

#[test]
fn v3_arrival_keeps_exact_replace_and_remove_effects() {
    use crate::commit::model::EntryIdentity;
    use crate::commit::write_stack::receiving::compact::CompactDeleteKind;

    let (handle, adapter) = dv_session();
    let scope = IcebergCreationScope::try_new(handle.session_id(), [2; 16], [3; 32]).unwrap();
    let handle = handle.try_with_creation_scope(scope).unwrap();
    let target_ordinal = handle
        .targets()
        .iter()
        .find(|target| target.branch() == IcebergWriteBranch::DeletionVector)
        .unwrap()
        .ordinal();
    let physical =
        ConnectorWriterPhysicalContext::new([1; 16], 1, [4; 16], 0, target_ordinal.get());
    let (query, ordinal) = register_delete(&handle, Some(physical));
    let created = |fragment: IcebergCommitFragment, id| {
        let path = fragment.new_object_path().unwrap();
        let identity = scope
            .identity(
                physical,
                path,
                crate::delete_file::IcebergFileFormat::Puffin,
            )
            .unwrap();
        let receipt =
            IcebergCreationReceipt::try_new(physical, [id; 16], identity, 1024, [6; 32]).unwrap();
        fragment.try_with_creation(receipt).unwrap()
    };
    let target = "s3://b/target.parquet";
    let old = EntryIdentity::DeleteFile {
        path: "s3://b/prior-delete.parquet".into(),
    };
    arrive(
        &handle,
        &adapter,
        query,
        ordinal,
        created(
            fixed_dv_fragment("s3://b/replacement.puffin", target, vec![old.clone()]),
            7,
        ),
    )
    .unwrap();
    let repeated = arrive(
        &handle,
        &adapter,
        query,
        ordinal,
        created(
            fixed_dv_fragment("s3://b/repeated.puffin", target, Vec::new()),
            8,
        ),
    )
    .unwrap_err();
    assert!(repeated.message().contains("more than one delete artifact"));
    let removed_path = "s3://b/remove.parquet";
    let locator = IcebergDeleteTargetLocator::try_new(
        "s3://b/list.avro".into(),
        "s3://b/data.avro".into(),
        0,
        1,
        4,
        removed_path.into(),
        5,
    )
    .unwrap();
    let removed = IcebergFinalDeleteEffect::try_new(
        locator,
        5,
        IcebergFinalDeleteKind::RemoveDataFile {
            partition: sample_partition(),
            old_exclusive_deletes: Box::default(),
        },
    )
    .unwrap();
    arrive(
        &handle,
        &adapter,
        query,
        ordinal,
        IcebergCommitFragment::final_delete(removed),
    )
    .unwrap();
    handle
        .with_active_receiving(|received| {
            let compact = received.compact_files();
            let mut entries = compact.entries();
            let replacement = entries.next().unwrap();
            let effect = replacement.delete_effect.as_ref().unwrap();
            let CompactDeleteKind::ReplacePositions {
                partition,
                merged_old_references,
            } = &effect.kind
            else {
                panic!("the accepted replacement lost its original delete effect");
            };
            assert_eq!(effect.target.data_file_path(), target);
            assert_eq!(partition.as_ref(), &sample_partition());
            assert_eq!(merged_old_references, std::slice::from_ref(&old));
            assert!(replacement.addition.is_some());
            let removal = entries.next().unwrap();
            assert!(removal.addition.is_none());
            assert_eq!(
                removal
                    .delete_effect
                    .as_ref()
                    .unwrap()
                    .target
                    .data_file_path(),
                removed_path
            );
            assert_eq!(
                removal.delete_effect.as_ref().unwrap().newly_deleted_rows,
                5
            );
            assert_eq!(compact.counts.position_deletes.files, 1);
            assert_eq!(
                compact.counts.newly_deleted_rows,
                effect.newly_deleted_rows + 5
            );
            Ok(())
        })
        .unwrap();
}

#[test]
fn observation_quote_does_not_repay_provider_partition_backing() {
    let small = sample_partition();
    let large = IcebergArtifactPartition::try_new(
        "long partition path/".repeat(128),
        "null fingerprint".repeat(128),
        small.partition_spec_id(),
        small.descriptor().clone(),
    )
    .unwrap();
    let path = "s3://b/data.parquet";
    let quote = observation_target_bytes(path, &small).unwrap();
    assert_eq!(observation_target_bytes(path, &large).unwrap(), quote);
    let longer_key = format!("{path}/new-key");
    assert_eq!(
        observation_target_bytes(&longer_key, &large).unwrap(),
        quote + u64::try_from(longer_key.len() - path.len()).unwrap(),
    );
}

#[tokio::test]
async fn v2_compact_position_target_must_exist_in_the_original_snapshot() {
    use crate::commit::fast_append::FastAppendPreparer;
    use crate::commit::model::AddedContent;
    use crate::commit::new_manifests::PendingNewFiles;
    use crate::commit::overwrite::preparer_tests::{Fixture, data};
    use crate::commit::write_stack::codec::CompactFragmentCodec;
    use crate::commit::write_stack::control::publication::MaterializedDeleteStep;
    use crate::iceberg::spec::{FormatVersion, Struct};

    let fixture = Fixture::new();
    let empty = fixture.metadata(FormatVersion::V2);
    let live = format!("{}/data/live.parquet", empty.location());
    let intent = fixture.intent(
        &empty,
        "main",
        vec![AddedContent::new_logical_data(data(&live, 3, Struct::empty()), 0).unwrap()],
    );
    let (base, _) = fixture.stage(empty, &intent, &FastAppendPreparer).await;
    let snapshot = base.current_snapshot_id().unwrap();
    let base = Arc::new(base);
    let adapter = adapter("compact-target-liveness", 1);
    let mut received = ReceivedCompactFiles::default();
    let missing = format!("{}/data/absent.parquet", base.location());
    received
        .v2_targets
        .insert(missing.clone(), sample_partition());
    fixture
        .operation
        .install_stage_new_files(
            0,
            PendingNewFiles {
                received,
                source: Arc::clone(&base),
                codec: CompactFragmentCodec::for_adapter(&adapter),
            },
        )
        .unwrap();
    let step =
        MaterializedDeleteStep::compact(fixture.operation.clone(), 0, AddedCounters::default());
    let io = fixture.operation.begin_attempt().unwrap();
    let error = step
        .observe(base, snapshot, &io, fixture.operation.request_context())
        .await
        .unwrap_err();
    assert!(error.to_string().contains(&missing), "{error}");
    assert!(
        error
            .to_string()
            .contains("absent from the declared snapshot range"),
        "{error}"
    );
}
