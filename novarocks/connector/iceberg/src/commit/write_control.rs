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

//! Generation-local Iceberg write admission.
//!
//! Everything a write *does* — sealing branches, opening writers, and the one
//! external commit — belongs to
//! [`write_stack`](crate::commit::write_stack). What is left here is the
//! admission half that runs before any session exists: reading the frozen
//! admitted table and answering, from Iceberg facts alone, whether the
//! statement can be written at all and in which physical shape.
//!
//! Those three answers have no write-stack equivalent because they are needed
//! *before* the caller can build a begin request:
//!
//! * [`prepare_write`](super::write_preparation::prepare_write) runs the
//!   Iceberg write-support guards and signs the Arrow input;
//! * [`prepare_row_mutation`](super::row_mutation_preparation::prepare_row_mutation)
//!   picks merge-on-read or copy-on-write and signs the match contract whose
//!   effect column the session later reads; and
//! * `preview_managed_partition_spec` answers which partitioning a managed
//!   replacement would establish, without proposing it.
//! * `certify_pre_ready_write_planning` proves that activation and planning
//!   remain generation-local until `ControlReady`, so Frontend may safely
//!   replan after a participant is replaced before that barrier.
//!
//! It owns no operation table, no activation, and no commit authority.

use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorInstanceDescriptor,
    ConnectorManagedPartitionSpecPreview, ConnectorManagedPartitionSpecPreviewRequest,
    ConnectorPreReadyWritePlanningProof, ConnectorPreReadyWritePlanningRequest,
    ConnectorProviderBindingKey, ConnectorRequestContext, ConnectorRowMutationPreparationOutcome,
    ConnectorRowMutationPreparationRequest, ConnectorWriteControl,
    ConnectorWritePreparationOutcome, ConnectorWritePreparationRequest, MvExactPartitionField,
    ProviderBindingEpoch,
};

use crate::metadata::IcebergMetadata;
use crate::metadata_context::IcebergMetadataContext;

/// Concrete write-admission capability assembled with every other capability
/// of one Iceberg control generation.
#[derive(Clone)]
pub struct IcebergWriteControl {
    key: ConnectorProviderBindingKey,
    provider: IcebergMetadata,
}

impl IcebergWriteControl {
    /// Construct an exact-generation capability over `runtime`; callers cannot
    /// inject an independent catalog client or registry.
    pub fn new(
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ProviderBindingEpoch,
        runtime: Arc<IcebergMetadataContext>,
    ) -> Self {
        let key = ConnectorProviderBindingKey {
            instance_id: descriptor.instance_id.clone(),
            incarnation,
        };
        let provider = IcebergMetadata::new(descriptor, incarnation, runtime);
        Self { key, provider }
    }
}

impl ConnectorWriteControl for IcebergWriteControl {
    fn binding_key(&self) -> &ConnectorProviderBindingKey {
        &self.key
    }

    fn prepare_write(
        &self,
        request: ConnectorWritePreparationRequest,
    ) -> Result<ConnectorWritePreparationOutcome, ConnectorError> {
        super::write_preparation::prepare_write(request, &self.key)
    }

    fn prepare_row_mutation(
        &self,
        request: ConnectorRowMutationPreparationRequest,
    ) -> Result<ConnectorRowMutationPreparationOutcome, ConnectorError> {
        super::row_mutation_preparation::prepare_row_mutation(request, &self.key)
    }

    fn certify_pre_ready_write_planning(
        &self,
        request: ConnectorPreReadyWritePlanningRequest,
    ) -> Result<ConnectorPreReadyWritePlanningProof, ConnectorError> {
        // Iceberg creates writers, staged objects, and catalog effects only
        // after ControlReady. The pre-ready path is confined to the exact
        // generation's admission facts, so its proof is safe to bind to this
        // activation request without granting commit authority to a BE.
        validate_context(&request.activation().context)?;
        ConnectorPreReadyWritePlanningProof::try_issue(self.key.clone(), &request)
    }

    fn preview_managed_partition_spec(
        &self,
        request: ConnectorManagedPartitionSpecPreviewRequest,
    ) -> Result<ConnectorManagedPartitionSpecPreview, ConnectorError> {
        validate_context(request.context())?;
        request.validate(&self.key)?;
        let table = self.provider.table_payload(request.table())?;
        if table.metadata_table_type.is_some() {
            return Err(invalid(
                "Iceberg metadata tables cannot be repartition targets",
            ));
        }
        let table_info = table.table_info.ok_or_else(|| {
            corrupt("admitted Iceberg repartition target is missing its frozen table descriptor")
        })?;
        let serialized = table_info.serialized_metadata.as_deref().ok_or_else(|| {
            corrupt("admitted Iceberg repartition target is missing frozen metadata")
        })?;
        let metadata = crate::metadata_json::from_metadata_str(serialized).map_err(|error| {
            corrupt(format!(
                "decode admitted Iceberg repartition target metadata: {error}"
            ))
        })?;
        // The same preview the write session runs when it prepares the real
        // transition, so an accepted preview and the transition that follows it
        // cannot disagree about the partitioning they establish.
        let prepared = super::write_stack::repartition::preview_managed_repartition(
            &metadata,
            request.replacement(),
        )?;
        let (exact_partition_spec_version, exact_partition_fields) =
            exact_preview_partition_binding(&prepared, request.context())?;
        ConnectorManagedPartitionSpecPreview::try_new(
            self.key.clone(),
            request.operation_id(),
            prepared.committed().clone(),
            exact_partition_spec_version,
            exact_partition_fields,
        )
    }
}

fn exact_preview_partition_binding(
    prepared: &super::write_stack::repartition::IcebergPreparedRepartition,
    context: &ConnectorRequestContext,
) -> Result<(Bytes, Vec<MvExactPartitionField>), ConnectorError> {
    let committed = prepared.committed();
    if prepared.prospective_metadata().default_partition_spec_id() != committed.spec_id() {
        return Err(corrupt(
            "Iceberg repartition preview spec differs from prospective metadata",
        ));
    }
    let exact_partition_fields = crate::storage_inspector::prepared_create_partition_fields(
        prepared.prospective_metadata(),
        context,
    )?;
    if exact_partition_fields.len() != committed.fields().len()
        || exact_partition_fields
            .iter()
            .zip(committed.fields())
            .any(|(exact, field)| {
                exact.partition_field_id().as_ref() != field.partition_field_id().to_be_bytes()
                    || exact.source_target_field_id().as_ref()
                        != field.source_field_id().to_be_bytes()
            })
    {
        return Err(corrupt(
            "Iceberg repartition preview field identities differ from prospective metadata",
        ));
    }
    Ok((
        crate::storage_inspector::exact_partition_spec_version(committed.spec_id()),
        exact_partition_fields,
    ))
}

fn validate_context(context: &ConnectorRequestContext) -> Result<(), ConnectorError> {
    if context.is_cancelled() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::Cancelled,
            "connector request was cancelled",
        ));
    }
    if Instant::now() >= context.deadline() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::DeadlineExceeded,
            "connector request deadline elapsed",
        ));
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}

fn corrupt(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, message)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use arrow::datatypes::{DataType, Field};
    use bytes::Bytes;
    use novarocks_fs::{FsAccessResolver, TokioFileIoRuntime, TokioFileIoSpawner};
    use novarocks_spi::connector::{
        ConnectorInstanceId, ConnectorManagedPartitionField,
        ConnectorManagedPartitionSpecObservation, ConnectorManagedPartitionSpecReplacement,
        ConnectorManagedPartitionTransform, ConnectorPreReadyWritePlanningRequest,
        ConnectorProviderId, ConnectorTableHandle, ConnectorWriteActivationIntent,
        ConnectorWriteActivationRequest, ConnectorWriteActivationSource, ConnectorWriteBaseVersion,
        ConnectorWriteFieldBinding, ConnectorWriteFieldToken, ConnectorWriteInputShape,
        ConnectorWriteIntent, ConnectorWriteOperationId, ConnectorWritePreparation,
        ConnectorWriteTargetRef, MvExactPartitionTransform,
    };

    use crate::access_binding::IcebergReadBinding;
    use crate::catalog_control::IcebergCatalogControlState;
    use crate::resources::IcebergMetadataResources;

    use super::*;

    fn request_context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(30),
            novarocks_spi::connector::ConnectorStopOwner::new().view(),
            1024 * 1024,
            4 * 1024 * 1024,
        )
        .expect("request context")
    }

    fn control() -> (tokio::runtime::Runtime, IcebergWriteControl) {
        let executor = tokio::runtime::Runtime::new().expect("runtime");
        let warehouse = tempfile::tempdir().expect("warehouse");
        let configuration = crate::catalog_config::parse_catalog_configuration(
            "ice",
            &[(
                "iceberg.catalog.warehouse".to_string(),
                warehouse.path().display().to_string(),
            )],
        )
        .expect("configuration");
        let binding = IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(executor.handle().clone())),
            Arc::new(TokioFileIoSpawner::new(executor.handle().clone())),
        );
        let resources = IcebergMetadataResources::new(binding, executor.handle().clone());
        let runtime = Arc::new(
            IcebergMetadataContext::try_new(
                IcebergCatalogControlState::new(configuration),
                resources,
            )
            .expect("control runtime"),
        );
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").expect("provider"),
            instance_id: ConnectorInstanceId::parse("ice").expect("instance"),
        };
        let control = IcebergWriteControl::new(
            descriptor,
            ProviderBindingEpoch::from_bytes([7; 16]),
            runtime,
        );
        (executor, control)
    }

    fn pre_ready_request(
        owner: &ConnectorProviderBindingKey,
    ) -> ConnectorPreReadyWritePlanningRequest {
        let preparation = ConnectorWritePreparation::try_new(
            owner.clone(),
            ConnectorTableHandle::try_new(
                owner.instance_id.clone(),
                Bytes::from_static(b"admitted-table"),
            )
            .expect("table handle"),
            ConnectorWriteTargetRef::main(),
            ConnectorWriteIntent::Append,
            ConnectorWriteBaseVersion::try_new(Bytes::from_static(b"base")).expect("base"),
            ConnectorWriteInputShape::Data {
                fields: vec![ConnectorWriteFieldBinding::new(
                    ConnectorWriteFieldToken::from_bytes([1; 32]),
                    Field::new("value", DataType::Int64, true),
                )],
            },
            Bytes::from_static(b"preparation"),
        )
        .expect("preparation");
        ConnectorPreReadyWritePlanningRequest::new(ConnectorWriteActivationRequest {
            operation_id: ConnectorWriteOperationId::new(),
            source: ConnectorWriteActivationSource::Prepared(preparation),
            intent: ConnectorWriteActivationIntent::Ordinary,
            context: request_context(),
        })
    }

    #[test]
    fn pre_ready_planning_proof_binds_the_exact_iceberg_activation() {
        let (_executor, control) = control();
        let owner = control.binding_key().clone();
        let request = pre_ready_request(&owner);

        let proof = control
            .certify_pre_ready_write_planning(request.clone())
            .expect("Iceberg proves its effect-free pre-ready planning path");
        proof
            .validates(&owner, &request)
            .expect("proof binds the exact owner and activation request");
    }

    #[test]
    fn repartition_preview_uses_the_same_exact_binding_as_iceberg_observation() {
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
            "s3://b/wh/db/t".to_string(),
            crate::iceberg::spec::FormatVersion::V2,
            std::collections::HashMap::new(),
        )
        .expect("metadata builder")
        .build()
        .expect("metadata")
        .metadata;
        let operation_id = ConnectorWriteOperationId::new();
        let prior = ConnectorManagedPartitionSpecObservation::try_from_fields(
            metadata.default_partition_spec_id(),
            &[],
        )
        .expect("unpartitioned prior");
        let replacement = ConnectorManagedPartitionSpecReplacement::try_new(
            operation_id,
            prior,
            vec![
                ConnectorManagedPartitionField::try_new(
                    1,
                    0,
                    ConnectorManagedPartitionTransform::Identity,
                )
                .expect("replacement field"),
            ],
        )
        .expect("replacement");
        let prepared = super::super::write_stack::repartition::preview_managed_repartition(
            &metadata,
            &replacement,
        )
        .expect("prepared repartition");
        let (spec_version, fields) = exact_preview_partition_binding(&prepared, &request_context())
            .expect("exact preview binding");
        let (_executor, control) = control();
        let preview = ConnectorManagedPartitionSpecPreview::try_new(
            control.binding_key().clone(),
            operation_id,
            prepared.committed().clone(),
            spec_version,
            fields,
        )
        .expect("preview");
        let committed = preview.committed_partitioning();
        assert_ne!(committed.spec_id(), 0);
        assert_eq!(
            preview.exact_partition_spec_version().as_ref(),
            committed.spec_id().to_le_bytes(),
        );
        let exact = &preview.exact_partition_fields()[0];
        let committed_field = &committed.fields()[0];
        assert_eq!(
            exact.partition_field_id().as_ref(),
            committed_field.partition_field_id().to_be_bytes(),
        );
        assert_eq!(
            exact.source_target_field_id().as_ref(),
            committed_field.source_field_id().to_be_bytes(),
        );
        assert_eq!(exact.transform(), &MvExactPartitionTransform::Identity);
    }
}
