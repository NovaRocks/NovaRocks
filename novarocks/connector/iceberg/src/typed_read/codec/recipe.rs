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

//! Pure validation and canonicalization of an Iceberg read recipe.

use novarocks_connector_contract::{
    ConnectorReadBinding, ConnectorReadRecipeSplitDraft, ConnectorReadRecipeSplitFacts,
    ConnectorReadRecipeSplitKind, ConnectorReadRelationRecipeCompiler as RecipeCompiler,
    ConnectorReadRelationRecipeDraft, MAX_CONNECTOR_RECIPE_PAYLOAD_BYTES,
};
use novarocks_spi::connector::read_stack::{ConnectorReadSplitFacts, HostAddress, SplitWeight};
use novarocks_spi::connector::{
    ConnectorCodecCategory, ConnectorCodecError, ConnectorCodecErrorKind, ConnectorCodecRevision,
    ConnectorDecodeContext, ConnectorDecodeLedger, ConnectorDecodeLimits, ConnectorEncodedPayload,
    ConnectorFieldPath, ConnectorReadRelationPayload,
};

use crate::PROVIDER_ID;
use crate::provider_types::IcebergReadTypes;

use super::{
    ICEBERG_READ_CODEC_REVISION, MAX_PRIVATE_READ_BYTES, MAX_PRIVATE_RETAINED_BYTES, split_category,
};

/// Pure codec for static relations and dynamic Task split inputs.
#[derive(Clone, Copy, Debug, Default)]
pub struct IcebergReadRecipeCompiler;

impl IcebergReadRecipeCompiler {
    /// Validate a Task update split without admitting it into a static program.
    pub fn validate_and_canonicalize_split(
        &self,
        binding: &ConnectorReadBinding,
        split: &ConnectorReadRecipeSplitDraft,
    ) -> Result<ConnectorReadRecipeSplitDraft, ConnectorCodecError> {
        if binding.descriptor().provider_id.as_str() != PROVIDER_ID {
            return Err(invalid(
                ConnectorFieldPath::root("binding").field("provider_id"),
                "Iceberg split requires the Iceberg provider binding",
            ));
        }
        let revision = ConnectorCodecRevision::try_new(ICEBERG_READ_CODEC_REVISION)
            .expect("Iceberg read codec revision is non-zero");
        let facts = spi_facts(split.facts())?;
        let value = decode_with_binding(
            binding,
            split.payload(),
            ConnectorCodecCategory::ReadSplit,
            revision,
            |bytes, context| IcebergReadTypes::wire_codecs().decode_split(bytes, &facts, context),
        )?;
        if recipe_kind(split_category(&value)) != split.kind() {
            return Err(inconsistent(
                ConnectorFieldPath::root("split").field("kind"),
                "Iceberg public split kind disagrees with the private split",
            ));
        }
        Ok(ConnectorReadRecipeSplitDraft::new(
            split.kind(),
            with_bytes(
                split.payload(),
                IcebergReadTypes::wire_codecs().encode_split(&value)?,
            ),
            split.facts().clone(),
        ))
    }
}

impl RecipeCompiler for IcebergReadRecipeCompiler {
    type Error = ConnectorCodecError;

    fn compile_split_private(
        &self,
        binding: &ConnectorReadBinding,
        draft: &ConnectorReadRecipeSplitDraft,
    ) -> Result<ConnectorReadRecipeSplitDraft, ConnectorCodecError> {
        self.validate_and_canonicalize_split(binding, draft)
    }

    fn compile_private(
        &self,
        draft: &ConnectorReadRelationRecipeDraft,
    ) -> Result<ConnectorReadRelationRecipeDraft, ConnectorCodecError> {
        if draft.binding().descriptor().provider_id.as_str() != PROVIDER_ID {
            return Err(invalid(
                ConnectorFieldPath::root("binding").field("provider_id"),
                "Iceberg recipe requires the Iceberg provider binding",
            ));
        }
        let revision = ConnectorCodecRevision::try_new(ICEBERG_READ_CODEC_REVISION)
            .expect("Iceberg read codec revision is non-zero");
        let codecs = IcebergReadTypes::wire_codecs();
        let relation = draft.relation();
        let table = decode(
            draft,
            relation.table(),
            ConnectorCodecCategory::ReadTable,
            revision,
            |bytes, context| codecs.decode_table(bytes, context),
        )?;
        let view = decode(
            draft,
            relation.view(),
            ConnectorCodecCategory::ReadView,
            revision,
            |bytes, context| codecs.decode_read_view(bytes, context),
        )?;
        if table.kind() != relation.kind() {
            return Err(inconsistent(
                ConnectorFieldPath::root("relation").field("kind"),
                "Iceberg public relation kind disagrees with the private table",
            ));
        }
        let relation = ConnectorReadRelationPayload::new(
            relation.kind(),
            with_bytes(relation.table(), codecs.encode_table(&table)?),
            with_bytes(relation.view(), codecs.encode_read_view(&view)?),
        );
        let mut columns = Vec::with_capacity(draft.columns().len());
        for (index, payload) in draft.columns().iter().enumerate() {
            let column = decode(
                draft,
                payload,
                ConnectorCodecCategory::ReadColumn,
                revision,
                |bytes, context| codecs.decode_column(bytes, context),
            )
            .map_err(|error| indexed(error, "columns", index))?;
            columns.push(with_bytes(payload, codecs.encode_column(&column)?));
        }
        ConnectorReadRelationRecipeDraft::try_new(draft.binding().clone(), relation, columns)
            .map_err(|error| invalid(ConnectorFieldPath::root("recipe"), error.to_string()))
    }
}

fn decode<T>(
    draft: &ConnectorReadRelationRecipeDraft,
    payload: &ConnectorEncodedPayload,
    category: ConnectorCodecCategory,
    revision: ConnectorCodecRevision,
    decode_private: impl FnOnce(
        &[u8],
        &mut ConnectorDecodeContext<'_>,
    ) -> Result<T, ConnectorCodecError>,
) -> Result<T, ConnectorCodecError> {
    decode_with_binding(draft.binding(), payload, category, revision, decode_private)
}

fn decode_with_binding<T>(
    binding: &ConnectorReadBinding,
    payload: &ConnectorEncodedPayload,
    category: ConnectorCodecCategory,
    revision: ConnectorCodecRevision,
    decode_private: impl FnOnce(
        &[u8],
        &mut ConnectorDecodeContext<'_>,
    ) -> Result<T, ConnectorCodecError>,
) -> Result<T, ConnectorCodecError> {
    if payload.payload().len() > MAX_CONNECTOR_RECIPE_PAYLOAD_BYTES {
        return Err(ConnectorCodecError::new(
            ConnectorFieldPath::root("provider_payload"),
            ConnectorCodecErrorKind::Capacity,
            "private payload exceeds the recipe hard limit",
        ));
    }
    payload.header().validate_expected::<ConnectorCodecError>(
        &binding.descriptor().provider_id,
        binding.catalog_handle(),
        category,
        revision,
    )?;
    let limits = ConnectorDecodeLimits::try_new(
        MAX_PRIVATE_READ_BYTES,
        MAX_PRIVATE_RETAINED_BYTES,
        MAX_PRIVATE_READ_BYTES,
        1_000_000,
        64,
    )
    .expect("Iceberg recipe decode limits are finite");
    let mut ledger = ConnectorDecodeLedger::new(limits);
    let mut context = ConnectorDecodeContext::new(payload.header(), &mut ledger);
    decode_private(payload.payload(), &mut context)
}

fn with_bytes(original: &ConnectorEncodedPayload, bytes: bytes::Bytes) -> ConnectorEncodedPayload {
    ConnectorEncodedPayload::new(original.header().clone(), bytes)
}

fn spi_facts(
    facts: &ConnectorReadRecipeSplitFacts,
) -> Result<ConnectorReadSplitFacts, ConnectorCodecError> {
    let addresses = facts
        .addresses()
        .iter()
        .map(|address| {
            HostAddress::try_new(address.host(), address.port()).map_err(|error| {
                invalid(
                    ConnectorFieldPath::root("split_facts").field("addresses"),
                    error.to_string(),
                )
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let weight = SplitWeight::try_from_raw(facts.split_weight()).map_err(|error| {
        invalid(
            ConnectorFieldPath::root("split_facts").field("split_weight"),
            error.to_string(),
        )
    })?;
    Ok(ConnectorReadSplitFacts::new(
        facts.remotely_accessible(),
        addresses,
        facts.affinity_key(),
        weight,
        facts.retained_size_in_bytes(),
    ))
}

fn recipe_kind(
    category: novarocks_spi::connector::ConnectorReadSplitCategory,
) -> ConnectorReadRecipeSplitKind {
    use novarocks_spi::connector::ConnectorReadSplitCategory as Category;
    match category {
        Category::Data => ConnectorReadRecipeSplitKind::Data,
        Category::TableChanges => ConnectorReadRecipeSplitKind::TableChanges,
        Category::ChangeWindow => ConnectorReadRecipeSplitKind::ChangeWindow,
        Category::SystemFiles => ConnectorReadRecipeSplitKind::SystemFiles,
        Category::RewritePositionDeleteFiles => {
            ConnectorReadRecipeSplitKind::RewritePositionDeleteFiles
        }
    }
}

fn indexed(error: ConnectorCodecError, root: &str, index: usize) -> ConnectorCodecError {
    ConnectorCodecError::new(
        ConnectorFieldPath::root(root).index(index),
        error.kind(),
        format!("{}: {}", error.path(), error.detail()),
    )
}

fn invalid(path: ConnectorFieldPath, detail: impl AsRef<str>) -> ConnectorCodecError {
    ConnectorCodecError::new(path, ConnectorCodecErrorKind::InvalidValue, detail)
}

fn inconsistent(path: ConnectorFieldPath, detail: impl AsRef<str>) -> ConnectorCodecError {
    ConnectorCodecError::new(path, ConnectorCodecErrorKind::InconsistentFields, detail)
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use novarocks_connector_contract::{
        CatalogHandle, CatalogVersion, ConnectorEnvelopeHeader, ConnectorInstanceDescriptor,
        ConnectorInstanceId, ConnectorProviderId, ConnectorReadBinding, ConnectorReadRelationKind,
        ConnectorReadRelationRecipe,
    };

    use super::*;
    use crate::provider_types::IcebergReadView;
    use crate::typed_read::{
        ColumnIdentity, ColumnIdentityCategory, HiveTransactionHandle, IcebergColumnHandle,
        IcebergColumnHandleParams, IcebergRuntimeRelation, IcebergSystemTableReference,
        IcebergSystemTableReferenceParams, IcebergSystemTableType,
    };
    use novarocks_spi::connector::read_stack::SchemaTableName;

    #[test]
    fn malformed_private_table_cannot_become_a_recipe() {
        let instance = ConnectorInstanceId::try_from_canonical("lake").unwrap();
        let binding = ConnectorReadBinding::new(
            ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse(PROVIDER_ID).unwrap(),
                instance_id: instance.clone(),
            },
            CatalogHandle::new(instance, CatalogVersion::from_bytes([1; 32])),
        );
        let make_payload = |category| {
            ConnectorEncodedPayload::new(
                ConnectorEnvelopeHeader::new(
                    binding.descriptor().provider_id.clone(),
                    binding.catalog_handle().clone(),
                    category,
                    ConnectorCodecRevision::try_new(ICEBERG_READ_CODEC_REVISION).unwrap(),
                ),
                Bytes::new(),
            )
        };
        let relation = ConnectorReadRelationPayload::new(
            ConnectorReadRelationKind::Table,
            make_payload(ConnectorCodecCategory::ReadTable),
            make_payload(ConnectorCodecCategory::ReadView),
        );
        let split_payload = make_payload(ConnectorCodecCategory::ReadSplit);
        let draft =
            ConnectorReadRelationRecipeDraft::try_new(binding.clone(), relation, vec![]).unwrap();
        assert!(
            ConnectorReadRelationRecipe::try_compile_with_provider(
                &draft,
                &IcebergReadRecipeCompiler
            )
            .is_err()
        );
        let facts =
            ConnectorReadRecipeSplitFacts::try_new(true, vec![], None::<&str>, 100, 0).unwrap();
        let split = ConnectorReadRecipeSplitDraft::new(
            ConnectorReadRecipeSplitKind::Data,
            split_payload,
            facts,
        );
        assert!(
            IcebergReadRecipeCompiler
                .validate_and_canonicalize_split(&binding, &split)
                .is_err()
        );
    }

    #[test]
    fn valid_private_relation_is_canonical_and_generation_bound() {
        let instance = ConnectorInstanceId::try_from_canonical("lake").unwrap();
        let binding = ConnectorReadBinding::new(
            ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse(PROVIDER_ID).unwrap(),
                instance_id: instance.clone(),
            },
            CatalogHandle::new(instance, CatalogVersion::from_bytes([7; 32])),
        );
        let table = IcebergRuntimeRelation::SystemTable(
            IcebergSystemTableReference::try_new(IcebergSystemTableReferenceParams {
                schema_table_name: SchemaTableName::try_new("db", "events").unwrap(),
                system_table_type: IcebergSystemTableType::Files,
                metadata_file_location: "s3://warehouse/db/events/metadata/v1.json".to_string(),
                table_uuid: "00000000-0000-0000-0000-000000000007".to_string(),
                snapshot_id: Some(11),
            })
            .unwrap(),
        );
        let view = IcebergReadView::new(HiveTransactionHandle::new(true, [3; 16]));
        let column = IcebergColumnHandle::try_new(IcebergColumnHandleParams {
            base_column_identity: ColumnIdentity::try_new(
                1,
                "id",
                ColumnIdentityCategory::Primitive,
                vec![],
            )
            .unwrap(),
            base_type_json: "\"long\"".to_string(),
            field_id_path: vec![],
            type_json: "\"long\"".to_string(),
            nullable: false,
            comment: None,
        })
        .unwrap();
        let envelope = |category, bytes| {
            ConnectorEncodedPayload::new(
                ConnectorEnvelopeHeader::new(
                    binding.descriptor().provider_id.clone(),
                    binding.catalog_handle().clone(),
                    category,
                    ConnectorCodecRevision::try_new(ICEBERG_READ_CODEC_REVISION).unwrap(),
                ),
                bytes,
            )
        };
        let codecs = IcebergReadTypes::wire_codecs();
        let draft = ConnectorReadRelationRecipeDraft::try_new(
            binding.clone(),
            ConnectorReadRelationPayload::new(
                ConnectorReadRelationKind::SystemTable,
                envelope(
                    ConnectorCodecCategory::ReadTable,
                    codecs.encode_table(&table).unwrap(),
                ),
                envelope(
                    ConnectorCodecCategory::ReadView,
                    codecs.encode_read_view(&view).unwrap(),
                ),
            ),
            vec![envelope(
                ConnectorCodecCategory::ReadColumn,
                codecs.encode_column(&column).unwrap(),
            )],
        )
        .unwrap();
        let recipe = ConnectorReadRelationRecipe::try_compile_with_provider(
            &draft,
            &IcebergReadRecipeCompiler,
        )
        .unwrap();
        assert_eq!(recipe.draft().columns().len(), 1);
        assert_eq!(recipe.validate_binding(&binding), Ok(()));
        assert_eq!(recipe.draft(), &draft);
    }
}
