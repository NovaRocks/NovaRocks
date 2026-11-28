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

use super::rewritten_target::{owner, sealed};
use super::*;
use crate::commit::conflict::MayMatch;
use crate::commit::conflict_scope::tests::{binding, metadata, partition};
use crate::commit::write_stack::domain::{
    IcebergCommitFragment, IcebergContentRange, IcebergCreationReceipt, IcebergCreationScope,
    IcebergDataFileArtifact, IcebergDeleteTargetLocator, IcebergDeletionVectorArtifact,
    IcebergFinalDeleteEffect, IcebergFinalDeleteKind, IcebergPositionDeleteFileArtifact,
    IcebergWriteSessionId,
};
use crate::commit::write_stack::planning::{IcebergWriteSessionPlanInput, plan_write_session};
use crate::commit::write_stack::test_support::{
    data_branch_plan, delete_branch_plan, paid_fragments_counted, sample_metrics, table_facts,
};
use crate::commit::write_stack::tests::{dv_session, fixed_delete_recipe};
use crate::delete_file::IcebergFileFormat;
use crate::iceberg::spec::{
    DataContentType, DataFileBuilder, DataFileFormat, Datum, PrimitiveType, Struct, Transform, Type,
};
use novarocks_spi::connector::ConnectorWriteAdmissionPurpose;

fn register(metadata: &TableMetadata, original: &RewrittenTargetBinding) -> IcebergReceiving {
    let mut receiving = IcebergReceiving::new(None);
    receiving
        .bind_rewritten_target(owner("unit", [1; 16]), original.clone(), metadata)
        .unwrap();
    receiving
}

#[test]
fn receiving_freezes_partition_values_for_every_row_delta_artifact() {
    let metadata = metadata(
        Type::Primitive(PrimitiveType::Long),
        Some(Transform::Identity),
    );
    let part = |value| {
        partition(
            &metadata,
            metadata.default_partition_spec_id(),
            Some(Datum::string(value)),
        )
    };
    let (v3_handle, adapter) = dv_session();
    let creation = IcebergCreationScope::try_new(v3_handle.session_id(), [2; 16], [3; 32]).unwrap();
    let v3_handle = v3_handle.try_with_creation_scope(creation).unwrap();
    let ordinal = |branch| {
        v3_handle
            .targets()
            .iter()
            .find(|target| target.branch() == branch)
            .unwrap()
            .ordinal()
    };
    let physical = |target: WriteTargetOrdinal| {
        ConnectorWriterPhysicalContext::new([8; 16], 1, [6; 16], 0, target.get())
    };
    let data_target = ordinal(IcebergWriteBranch::Data);
    let dv_target = ordinal(IcebergWriteBranch::DeletionVector);
    struct Production([(WriteTargetOrdinal, ConnectorWriterPhysicalContext); 2]);
    impl ConnectorWriteProductionProof for Production {
        fn admits(
            &self,
            target: WriteTargetOrdinal,
            writer: &ConnectorWriterPhysicalContext,
        ) -> Result<bool, ConnectorError> {
            Ok(self
                .0
                .iter()
                .any(|(ordinal, physical)| *ordinal == target && physical == writer))
        }
    }
    let proof = Arc::new(Production([
        (data_target, physical(data_target)),
        (dv_target, physical(dv_target)),
    ]));
    let created = |fragment: IcebergCommitFragment, target, format, id| {
        let writer = physical(target);
        let identity = creation
            .identity(writer, fragment.new_object_path().unwrap(), format)
            .unwrap();
        let receipt =
            IcebergCreationReceipt::try_new(writer, [id; 16], identity, 100, [1; 32]).unwrap();
        fragment.try_with_creation(receipt).unwrap()
    };
    let v3_query = write_query_identity();
    let admitted = binding(&metadata);
    let mut v3 = register(&metadata, &admitted);
    let fact = sealed(&metadata, admitted.clone(), owner("unit", [1; 16]));
    v3.begin_query(
        v3_query,
        &v3_handle.expected_targets(),
        &mut Some(proof),
        v3_handle.targets(),
        Some(&fact),
    )
    .unwrap();
    let metrics = sample_metrics(3, 100);
    let vector = IcebergDeletionVectorArtifact::try_new(
        "s3://b/written.puffin".into(),
        part("vector"),
        metrics.clone(),
        "s3://b/vector-target.parquet".into(),
        IcebergContentRange::try_new(4, 64).unwrap(),
        3,
        Vec::new(),
    )
    .unwrap();
    let located = |path, records| {
        IcebergDeleteTargetLocator::try_new(
            "s3://b/list.avro".into(),
            "s3://b/data.avro".into(),
            0,
            1,
            4,
            path,
            records,
        )
        .unwrap()
    };
    let replacement = IcebergFinalDeleteEffect::try_new(
        located("s3://b/vector-target.parquet".into(), 4),
        3,
        IcebergFinalDeleteKind::ReplacePositions(Box::new(vector)),
    )
    .unwrap();
    let removal = IcebergFinalDeleteEffect::try_new(
        located("s3://b/removed-target.parquet".into(), 3),
        3,
        IcebergFinalDeleteKind::RemoveDataFile {
            partition: part("remove"),
            old_exclusive_deletes: Box::default(),
        },
    )
    .unwrap();
    let data = IcebergCommitFragment::data_file(
        IcebergDataFileArtifact::try_new(
            "s3://b/written.parquet".into(),
            IcebergFileFormat::Parquet,
            part("data"),
            metrics.clone(),
            None,
        )
        .unwrap(),
    );
    let writes = [
        (
            data_target,
            created(data, data_target, IcebergFileFormat::Parquet, 1),
        ),
        (
            dv_target,
            created(
                IcebergCommitFragment::final_delete(replacement),
                dv_target,
                IcebergFileFormat::Puffin,
                2,
            ),
        ),
        (dv_target, IcebergCommitFragment::final_delete(removal)),
    ];
    let refs = writes.iter().map(|(target, _)| *target).collect::<Vec<_>>();
    let (paid, author, held) = paid_fragments_counted(
        &adapter,
        writes
            .into_iter()
            .map(|(_, f)| adapter.wrap_commit_fragment(f))
            .collect(),
    );
    v3.arrive_original(
        &v3_handle,
        &adapter,
        v3_query,
        refs.into_iter().zip(paid).collect(),
        &author,
    )
    .unwrap();
    v3.confirm(v3_query).unwrap();
    let scope = v3.row_conflict_scope(&metadata).unwrap().unwrap();
    drop(v3.take_compact_files());
    assert!(held.load(std::sync::atomic::Ordering::SeqCst) > 0);
    for value in ["data", "vector", "remove", "other"] {
        assert_eq!(might(&scope, &metadata, value), value != "other");
    }
    drop(scope);
    assert_eq!(held.load(std::sync::atomic::Ordering::SeqCst), 0);

    let (v2_handle, _) = plan_write_session(
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
    let recipe = fixed_delete_recipe(v2_handle.table());
    let v2_handle = v2_handle.with_mutation_read(recipe);
    let v2_query = write_query_identity();
    let mut v2 = register(&metadata, &admitted);
    let target = v2_handle
        .targets()
        .iter()
        .find(|target| target.branch() == IcebergWriteBranch::PositionDelete)
        .unwrap()
        .ordinal();
    v2.begin_query(
        v2_query,
        &[target],
        &mut None,
        v2_handle.targets(),
        Some(&fact),
    )
    .unwrap();
    let position = IcebergCommitFragment::position_delete_file(
        IcebergPositionDeleteFileArtifact::try_new(
            "s3://b/position.parquet".into(),
            part("v2"),
            metrics,
            "s3://b/target.parquet".into(),
            Vec::new(),
        )
        .unwrap(),
    );
    let (paid, author, _) =
        paid_fragments_counted(&adapter, vec![adapter.wrap_commit_fragment(position)]);
    v2.arrive_original(
        &v2_handle,
        &adapter,
        v2_query,
        paid.into_iter().map(|f| (target, f)).collect(),
        &author,
    )
    .unwrap();
    v2.confirm(v2_query).unwrap();
    let scope = v2.row_conflict_scope(&metadata).unwrap().unwrap();
    assert!(might(&scope, &metadata, "v2"));
    assert!(!might(&scope, &metadata, "other"));
}

fn might(scope: &ConflictScope, metadata: &TableMetadata, value: &str) -> bool {
    let file = DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path("s3://b/candidate.parquet".into())
        .file_format(DataFileFormat::Parquet)
        .partition(Struct::from_iter([Some(Datum::string(value).into())]))
        .record_count(1)
        .file_size_in_bytes(100)
        .build()
        .unwrap();
    scope
        .may_match(&crate::commit::conflict::CandidateFile {
            spec_id: metadata.default_partition_spec_id(),
            metadata,
            file: &file,
        })
        .unwrap()
}
