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

//! Shared constructors for the write-stack unit tests.
//!
//! They build *valid* values so each test can perturb exactly one fact and
//! assert that the perturbation is what fails.

use arrow::datatypes::{DataType, Field};
use novarocks_spi::connector::{
    ConnectorError, ConnectorInstanceId, ConnectorMutationEffectField,
    ConnectorMutationMatchContract, ConnectorMutationSourceField, ConnectorMutationTargetField,
    ConnectorProviderBindingKey, ConnectorTableHandle, ConnectorWriteBaseVersion,
    ConnectorWriteFieldBinding, ConnectorWriteFieldToken, ConnectorWriteInputShape,
    ProviderBindingEpoch,
};

use crate::commit::write_stack::domain::{
    IcebergArtifactMetrics, IcebergArtifactPartition, IcebergContentRange, IcebergDataBranchRecipe,
    IcebergDeletionVectorArtifact, IcebergDocumentPublicationFacts,
    IcebergEqualityDeleteColumnFacts, IcebergEqualityDeleteRecipe, IcebergWriteBranch,
    IcebergWriteTableFacts, IcebergWriterOutput,
};
use crate::commit::write_stack::flavor::IcebergSessionMaterial;
use crate::commit::write_stack::old_delete::{
    IcebergOldDeleteArtifactRef, IcebergOldDeleteMergeTarget, IcebergStorageRoute,
};
use crate::commit::write_stack::planning::{IcebergDataBranchPlan, IcebergDeleteBranchPlan};
use crate::delete_file::{IcebergFileContent, IcebergFileFormat};
use crate::write_descriptor::IcebergPartitionDescriptor;

pub(crate) fn table_facts() -> IcebergWriteTableFacts {
    IcebergWriteTableFacts::try_new(
        "6f7c3a0e-0000-4000-8000-000000000001".to_string(),
        "db".to_string(),
        "t".to_string(),
        "s3://b/wh/db/t".to_string(),
        "s3://b/wh/db/t/data".to_string(),
        "main".to_string(),
        Some(77),
        4,
        1,
        0,
        3,
    )
    .expect("table facts")
}

/// A frozen staged target, as the staged-create capability would hand one to a
/// session. It carries no snapshot, because nothing has ever been written to a
/// staged table.
pub(crate) fn staged_table_metadata() -> std::sync::Arc<crate::iceberg::spec::TableMetadata> {
    let schema = crate::iceberg::spec::Schema::builder()
        .with_fields(vec![std::sync::Arc::new(
            crate::iceberg::spec::NestedField::required(
                1,
                "id",
                crate::iceberg::spec::Type::Primitive(crate::iceberg::spec::PrimitiveType::Long),
            ),
        )])
        .build()
        .expect("staged schema");
    let metadata = crate::iceberg::spec::TableMetadataBuilder::new(
        schema,
        crate::iceberg::spec::PartitionSpec::unpartition_spec(),
        crate::iceberg::spec::SortOrder::unsorted_order(),
        "s3://b/wh/_novarocks/ctas-staging/v1/staged/table".to_string(),
        crate::iceberg::spec::FormatVersion::V2,
        std::collections::HashMap::new(),
    )
    .expect("staged metadata builder")
    .build()
    .expect("staged metadata")
    .metadata;
    std::sync::Arc::new(metadata)
}

/// A stable test publication identity, bound by a real native admission.
fn document_publication_id() -> novarocks_spi::connector::LakePublicationId {
    novarocks_spi::connector::LakePublicationId::try_from_uuid(
        uuid::Uuid::parse_str("019200aa-0000-7000-8000-0000000000ab").expect("publication uuid"),
    )
    .expect("publication id")
}

/// Ask the actual document provider for admission, under the same explicitly
/// admitted catalog fixture used by document_publication_for_refusal.
pub(crate) fn admitted_document_publication_declaration(
    runtime: &std::sync::Arc<crate::metadata_context::IcebergMetadataContext>,
    incarnation: ProviderBindingEpoch,
    catalog_handle: novarocks_spi::connector::CatalogHandle,
    target: novarocks_spi::connector::ConnectorTableIdentity,
    object_id: novarocks_spi::connector::ConnectorTableObjectId,
    base: ConnectorWriteBaseVersion,
    technique: novarocks_spi::connector::ConnectorManagedPublicationTechnique,
    empty_input: novarocks_spi::connector::ConnectorManagedPublicationEmptyInputDisposition,
) -> novarocks_spi::connector::ConnectorDocumentPublicationDeclaration {
    use novarocks_spi::connector::*;
    use std::sync::Arc;
    let descriptor = ConnectorInstanceDescriptor {
        provider_id: ConnectorProviderId::parse("iceberg").expect("provider"),
        instance_id: catalog_handle.catalog_name().clone(),
    };
    let admitted_runtime = Arc::new(
        crate::metadata_context::IcebergMetadataContext::with_catalog_for_test(
            runtime.control_state().clone(),
            runtime.resources().clone(),
            crate::catalog::admission_test_support::all_admitted(Arc::clone(
                runtime.novarocks_catalog(),
            )),
        ),
    );
    let provider = Arc::new(crate::metadata::IcebergMetadata::new(
        descriptor.clone(),
        incarnation,
        Arc::clone(&admitted_runtime),
    ));
    let storage = Arc::new(crate::document_storage::IcebergDocumentStorage::new(
        descriptor.clone(),
        incarnation,
        admitted_runtime,
    ));
    let documents = ConnectorDocumentStorageBinding::try_new(
        descriptor.clone(),
        incarnation,
        Some(storage.clone()),
        Some(storage),
    )
    .expect("document binding");
    let binding = ConnectorControlBinding::try_new(
        descriptor.clone(),
        incarnation,
        provider.clone(),
        provider.clone(),
        Arc::new(crate::provider_binding::IcebergInstanceDistribution::new(
            descriptor.clone(),
            incarnation,
        )),
        Some(provider),
    )
    .and_then(|binding| {
        binding.with_catalog_properties(
            CatalogProperties::new(
                catalog_handle.clone(),
                descriptor.provider_id,
                1,
                vec![],
                vec![],
            )
            .expect("properties"),
        )
    })
    .and_then(|binding| binding.try_with_document_storage(Some(documents)))
    .expect("control binding");
    let lease = ConnectorControlPlanningPin::new(Arc::new(binding), || {})
        .derive_document_storage_lease()
        .expect("document lease");
    let publication_id = document_publication_id();
    let admission = lease
        .admit_management(
            ConnectorDocumentManagementAdmissionRequest::try_new(
                lease.owner().clone(),
                catalog_handle,
                ConnectorMutationOperationId::from_bytes(publication_id.to_bytes()),
                target,
                Some(object_id.clone()),
                ConnectorDocumentManagementOperation::Publication,
                super::tests::request_context(),
            )
            .expect("admission request"),
        )
        .expect("actual provider admission");
    ConnectorDocumentPublicationDeclaration::try_new(
        publication_id,
        admission,
        object_id,
        base,
        technique,
        empty_input,
        None,
        None,
    )
    .expect("document publication declaration")
}

pub(crate) fn document_publication_facts(
    technique: novarocks_spi::connector::ConnectorManagedPublicationTechnique,
    empty_input: novarocks_spi::connector::ConnectorManagedPublicationEmptyInputDisposition,
) -> IcebergDocumentPublicationFacts {
    document_publication_facts_with_shape(
        technique,
        empty_input,
        novarocks_spi::connector::write_stack::ConnectorManagedPublicationShape::Data,
    )
}

pub(crate) fn document_publication_facts_with_shape(
    technique: novarocks_spi::connector::ConnectorManagedPublicationTechnique,
    empty_input: novarocks_spi::connector::ConnectorManagedPublicationEmptyInputDisposition,
    shape: novarocks_spi::connector::write_stack::ConnectorManagedPublicationShape,
) -> IcebergDocumentPublicationFacts {
    use novarocks_spi::connector::*;
    // No network request is made by provider admission: the catalog explicitly
    // admits it, while ordinary I/O against this REST endpoint still refuses.
    let (_executor, runtime) = super::tests::unreachable_rest_runtime();
    let table = table_facts();
    let declaration = admitted_document_publication_declaration(
        &runtime,
        ProviderBindingEpoch::from_bytes([7; 16]),
        CatalogHandle::new(
            ConnectorInstanceId::parse("unit").unwrap(),
            CatalogVersion::from_bytes([1; 32]),
        ),
        ConnectorTableIdentity {
            instance_id: ConnectorInstanceId::parse("unit").unwrap(),
            namespace: table.namespace().into(),
            table: table.table_name().into(),
        },
        ConnectorTableObjectId::try_new(bytes::Bytes::copy_from_slice(
            table.table_uuid().as_bytes(),
        ))
        .unwrap(),
        ConnectorWriteBaseVersion::try_new(bytes::Bytes::from_static(b"base")).unwrap(),
        technique,
        empty_input,
    );
    IcebergDocumentPublicationFacts::new(declaration, shape)
}

pub(crate) fn document_publication_flavor(
    technique: novarocks_spi::connector::ConnectorManagedPublicationTechnique,
    shape: novarocks_spi::connector::write_stack::ConnectorManagedPublicationShape,
) -> novarocks_spi::connector::write_stack::ConnectorWriteSessionFlavor {
    let facts = document_publication_facts_with_shape(technique,
        novarocks_spi::connector::ConnectorManagedPublicationEmptyInputDisposition::CommitEmptyWrite, shape);
    novarocks_spi::connector::write_stack::ConnectorWriteSessionFlavor::ApplicationDocumentPublication {
        declaration: facts.declaration().clone(),
        shape,
    }
}

pub(crate) fn sample_partition() -> IcebergArtifactPartition {
    IcebergArtifactPartition::try_new(
        String::new(),
        String::new(),
        0,
        IcebergPartitionDescriptor { values: Vec::new() },
    )
    .expect("partition")
}

pub(crate) fn sample_metrics(record_count: u64, file_size: u64) -> IcebergArtifactMetrics {
    IcebergArtifactMetrics::try_new(record_count, file_size, Vec::new(), None).expect("metrics")
}

pub(crate) fn dv_artifact(
    path: &str,
    referenced: &str,
    cardinality: u64,
    file_size: u64,
    offset: i64,
    size: i64,
) -> Result<IcebergDeletionVectorArtifact, ConnectorError> {
    IcebergDeletionVectorArtifact::try_new(
        path.to_string(),
        sample_partition(),
        sample_metrics(cardinality, file_size),
        referenced.to_string(),
        IcebergContentRange::try_new(offset, size)?,
        cardinality,
        Vec::new(),
    )
}

pub(crate) fn parquet_ref(
    path: &str,
    referenced: Option<&str>,
    file_size: u64,
    record_count: u64,
) -> Result<IcebergOldDeleteArtifactRef, ConnectorError> {
    IcebergOldDeleteArtifactRef::try_new(
        path.to_string(),
        IcebergFileContent::PositionDeletes,
        IcebergFileFormat::Parquet,
        file_size,
        (record_count > 0).then_some(record_count),
        None,
        referenced.map(str::to_string),
        Some(3),
        None,
        0,
        IcebergStorageRoute::try_for_location(path)?,
    )
}

pub(crate) fn puffin_ref(
    path: &str,
    referenced: Option<&str>,
    file_size: u64,
    record_count: u64,
    offset: i64,
    size: i64,
) -> Result<IcebergOldDeleteArtifactRef, ConnectorError> {
    IcebergOldDeleteArtifactRef::try_new(
        path.to_string(),
        IcebergFileContent::PositionDeletes,
        IcebergFileFormat::Puffin,
        file_size,
        (record_count > 0).then_some(record_count),
        Some(IcebergContentRange::try_new(offset, size)?),
        referenced.map(str::to_string),
        Some(3),
        None,
        0,
        IcebergStorageRoute::try_for_location(path)?,
    )
}

pub(crate) fn merge_target(
    data_file: &str,
    record_count: u64,
    references: Vec<IcebergOldDeleteArtifactRef>,
) -> IcebergOldDeleteMergeTarget {
    IcebergOldDeleteMergeTarget::try_new(
        data_file.to_string(),
        record_count,
        Some(4),
        sample_partition(),
        77,
        references,
    )
    .expect("merge target")
}

pub(crate) fn binding(name: &str, seed: u8, data_type: DataType) -> ConnectorWriteFieldBinding {
    ConnectorWriteFieldBinding::new(
        ConnectorWriteFieldToken::from_bytes([seed; 32]),
        Field::new(name, data_type, false),
    )
}

/// A row-lineage input whose identity is `_file`/`_pos`: the merge-on-read
/// shape, carrying both halves of a change event in one row.
pub(crate) fn merge_on_read_input_shape() -> ConnectorWriteInputShape {
    ConnectorWriteInputShape::RowLineage {
        data_fields: vec![
            binding("k1", 1, DataType::Int64),
            binding("v1", 4, DataType::Utf8),
        ],
        row_identity_fields: vec![
            binding("_file", 2, DataType::Utf8),
            binding("_pos", 3, DataType::Int64),
        ],
    }
}

/// A row-lineage input whose identity is `_row_id`/`_last_updated_sequence_number`:
/// the copy-on-write shape.
pub(crate) fn copy_on_write_input_shape() -> ConnectorWriteInputShape {
    ConnectorWriteInputShape::RowLineage {
        data_fields: vec![binding("k1", 1, DataType::Int64)],
        row_identity_fields: vec![
            binding("_row_id", 5, DataType::Int64),
            ConnectorWriteFieldBinding::new(
                ConnectorWriteFieldToken::from_bytes([6; 32]),
                Field::new("_last_updated_sequence_number", DataType::Int64, true),
            ),
        ],
    }
}

pub(crate) fn copy_on_write_match_contract() -> ConnectorMutationMatchContract {
    let instance = ConnectorInstanceId::parse("iceberg").expect("instance");
    let identity = [
        ("_file", DataType::Utf8, false, 0_u32, 21_u8),
        ("_pos", DataType::Int64, false, 1, 22),
        ("_row_id", DataType::Int64, false, 2, 23),
        (
            "_last_updated_sequence_number",
            DataType::Int64,
            true,
            3,
            24,
        ),
    ]
    .into_iter()
    .map(|(name, data_type, nullable, ordinal, tag)| {
        ConnectorMutationSourceField::try_new_typed(
            ConnectorWriteFieldToken::from_bytes([tag; 32]),
            Field::new(name, data_type.clone(), nullable),
            ordinal,
            novarocks_type_contract::FunctionValueType::new(data_type, nullable),
        )
        .expect("complete source role declaration")
    })
    .collect::<Vec<_>>();
    ConnectorMutationMatchContract::try_new(
        ConnectorProviderBindingKey {
            instance_id: instance.clone(),
            incarnation: ProviderBindingEpoch::from_bytes([7; 16]),
        },
        ConnectorTableHandle::try_new(instance, bytes::Bytes::from_static(b"table"))
            .expect("table"),
        ConnectorWriteBaseVersion::try_new(bytes::Bytes::from_static(b"base")).expect("base"),
        identity.clone(),
        vec![
            ConnectorMutationTargetField::try_new_typed(
                ConnectorWriteFieldToken::from_bytes([25; 32]),
                Field::new("k1", DataType::Int64, false),
                4,
                novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
            )
            .expect("complete target declaration"),
        ],
        vec![
            ConnectorMutationTargetField::try_new_typed(
                ConnectorWriteFieldToken::from_bytes([26; 32]),
                Field::new("k1", DataType::Int64, false),
                5,
                novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
            )
            .expect("complete target declaration"),
        ],
        vec![identity[2].token()],
        ConnectorMutationEffectField::try_new(
            ConnectorWriteFieldToken::from_bytes([27; 32]),
            Field::new("__row_mutation_effect", DataType::Int8, false),
            6,
        )
        .expect("effect"),
    )
    .expect("COW match contract")
}

/// The frozen material a flavor's branch planning is cut from.
///
/// It is deliberately built the same way `begin_write` builds it, so a test
/// perturbs only the input shape or the merge targets and asserts that the
/// branch structure is what changes.
pub(crate) fn session_material(input: ConnectorWriteInputShape) -> IcebergSessionMaterial {
    let data = data_branch_plan();
    IcebergSessionMaterial {
        table: table_facts(),
        input,
        data_output: data.output,
        data_recipe: data.recipe,
        merge_targets: Vec::new(),
        equality: None,
    }
}

pub(crate) fn data_input_shape() -> ConnectorWriteInputShape {
    ConnectorWriteInputShape::Data {
        fields: vec![binding("k1", 1, DataType::Int64)],
    }
}

pub(crate) fn delete_input_shape(branch: IcebergWriteBranch) -> ConnectorWriteInputShape {
    let identity_fields = vec![
        binding("_file", 2, DataType::Utf8),
        binding("_pos", 3, DataType::Int64),
    ];
    match branch {
        IcebergWriteBranch::DeletionVector => ConnectorWriteInputShape::DeletionVector {
            identity_fields,
            partition_source_fields: Vec::new(),
        },
        _ => ConnectorWriteInputShape::PositionDelete {
            identity_fields,
            partition_source_fields: Vec::new(),
        },
    }
}

/// The `ALTER TABLE ... ADD EQUALITY DELETE` input: the match columns and
/// nothing else.
pub(crate) fn equality_delete_input_shape() -> ConnectorWriteInputShape {
    ConnectorWriteInputShape::EqualityDelete {
        equality_fields: vec![binding("k1", 1, DataType::Int64)],
    }
}

/// The match key `begin_write` resolves against the frozen schema, as branch
/// planning receives it.
pub(crate) fn equality_delete_recipe() -> IcebergEqualityDeleteRecipe {
    IcebergEqualityDeleteRecipe::try_new(vec![
        IcebergEqualityDeleteColumnFacts::try_new(
            "k1".to_string(),
            1,
        )
        .expect("equality column"),
    ], serde_json::json!({"type":"struct","schema-id":1,"fields":[{"id":1,"name":"k1","required":false,"type":"long"}]}).to_string())
    .expect("equality recipe")
}

pub(crate) fn data_branch_plan() -> IcebergDataBranchPlan {
    IcebergDataBranchPlan {
        output: IcebergWriterOutput::try_new(
            IcebergFileFormat::Parquet,
            parquet::basic::Compression::SNAPPY,
            None,
        )
        .expect("output"),
        recipe: IcebergDataBranchRecipe::try_new(Some(serde_json::json!({"type":"struct","schema-id":1,"fields":[{"id":1,"name":"k1","required":false,"type":"long"}]}).to_string()), Vec::new(), Vec::new(), Vec::new(), false)
            .expect("recipe"),
        input: data_input_shape(),
    }
}

pub(crate) fn delete_branch_plan(
    branch: IcebergWriteBranch,
    merge_targets: Vec<IcebergOldDeleteMergeTarget>,
) -> IcebergDeleteBranchPlan {
    let file_format = match branch {
        IcebergWriteBranch::DeletionVector => IcebergFileFormat::Puffin,
        _ => IcebergFileFormat::Parquet,
    };
    IcebergDeleteBranchPlan {
        branch,
        output: IcebergWriterOutput::try_new(
            file_format,
            parquet::basic::Compression::SNAPPY,
            None,
        )
        .expect("output"),
        merge_targets,
        input: delete_input_shape(branch),
    }
}
