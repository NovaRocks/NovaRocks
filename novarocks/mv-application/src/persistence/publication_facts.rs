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

//! Freeze the exact occurrence watermark and the result carried by compact P.
use crate::persistence::codec::{BindingDocument, PublicationInput};
use crate::persistence::definition::MvAcceleratorSourceRevision;
use crate::persistence::exact_revision::persist_exact_connector_revision;
use crate::persistence::identity::PublicationIdentity;
use novarocks_spi::connector::{ConnectorExactSemanticRevision, LakePublicationId};
use std::collections::BTreeMap;

/// One refresh's complete input watermark, in B's own occurrence order.
///
/// The order and multiplicity are B's, not a table's: a self-join occupies two
/// occurrences of one physical object and records two inputs, because the
/// publication has to be able to say what each occurrence read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvPublicationInputs {
    inputs: Vec<PublicationInput>,
}

impl MvPublicationInputs {
    /// Freeze the watermark from the exact revision each B occurrence was
    /// pinned at.
    ///
    /// Every occurrence must be present and must still name the object B
    /// bound: a refresh that read something else is not a refresh of this
    /// definition, and saying so here is cheaper than discovering it when the
    /// document set is validated.
    pub fn freeze(
        binding: &BindingDocument,
        revision_by_occurrence: &BTreeMap<u32, ConnectorExactSemanticRevision>,
    ) -> Result<Self, String> {
        if revision_by_occurrence.len() != binding.occurrences.len() {
            return Err(format!(
                "MV publication needs one pinned revision per binding occurrence, but {} \
                 occurrences were pinned for {} occurrences",
                revision_by_occurrence.len(),
                binding.occurrences.len(),
            ));
        }
        let inputs = binding
            .occurrences
            .iter()
            .map(|occurrence| {
                let revision = revision_by_occurrence
                    .get(&occurrence.occurrence_id)
                    .ok_or_else(|| {
                        format!(
                            "MV publication has no pinned revision for binding occurrence {}",
                            occurrence.occurrence_id,
                        )
                    })?;
                let (object_id, native_data_version) = persist_exact_connector_revision(revision)
                    .map_err(|error| {
                    format!(
                        "persist the exact revision of binding occurrence {}: {error}",
                        occurrence.occurrence_id,
                    )
                })?;
                if object_id != occurrence.object_id {
                    return Err(format!(
                        "MV publication read a different object than binding occurrence {} \
                         binds",
                        occurrence.occurrence_id,
                    ));
                }
                Ok(PublicationInput {
                    relation_occurrence_id: occurrence.occurrence_id,
                    object_id,
                    native_data_version,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        Ok(Self { inputs })
    }

    pub fn inputs(&self) -> &[PublicationInput] {
        &self.inputs
    }
}

/// Freeze the actual layout-1 publication facts and exact occurrence watermark.
pub fn freeze_keyed_publication_document(
    source: &MvAcceleratorSourceRevision,
    publication_id: LakePublicationId,
    inputs: &MvPublicationInputs,
    logical_result_rows: Option<u64>,
    prepared_at_ms: u64,
) -> Result<crate::persistence::codec::keyed::KeyedPublicationDocument, String> {
    if prepared_at_ms > i64::MAX as u64 {
        return Err("publication frozen timestamp exceeds i64".into());
    }
    Ok(crate::persistence::codec::keyed::KeyedPublicationDocument {
        publication_prepared_at_ms: prepared_at_ms,
        publication_id: PublicationIdentity::try_new(publication_id.to_bytes().to_vec())
            .map_err(|e| e.to_string())?,
        definition_revision: source.definition_revision,
        interpretation_revision: source.interpretation_revision,
        inputs: inputs.inputs.clone(),
        logical_result_rows,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use novarocks_spi::connector::{ConnectorProviderId, ConnectorTableObjectId};
    fn revision(object_value: u8, snapshot_id: i64) -> ConnectorExactSemanticRevision {
        let object = ConnectorTableObjectId::try_new(Bytes::copy_from_slice(&[object_value]))
            .expect("source object");
        ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(
            ConnectorProviderId::parse("iceberg").expect("provider"),
            &object,
            Some(snapshot_id),
        )
        .expect("source revision")
    }

    /// The fixture's occurrences are bound at snapshot 9, so reading them
    /// there is what "unchanged sources" looks like.
    fn pinned_at_binding() -> BTreeMap<u32, ConnectorExactSemanticRevision> {
        BTreeMap::from([(7, revision(11, 9)), (8, revision(11, 9))])
    }

    #[test]
    fn a_self_join_records_one_input_per_occurrence() {
        let fixture = crate::persistence::test_support::MvDocumentFixture::new();
        let inputs = MvPublicationInputs::freeze(&fixture.binding, &pinned_at_binding())
            .expect("every occurrence is pinned");

        assert_eq!(
            inputs
                .inputs()
                .iter()
                .map(|input| input.relation_occurrence_id)
                .collect::<Vec<_>>(),
            vec![7, 8],
            "both occurrences of one object must survive"
        );
        assert_eq!(inputs.inputs()[0].object_id, inputs.inputs()[1].object_id);
    }

    #[test]
    fn an_unpinned_occurrence_fails_closed() {
        let fixture = crate::persistence::test_support::MvDocumentFixture::new();
        let error =
            MvPublicationInputs::freeze(&fixture.binding, &BTreeMap::from([(7, revision(11, 9))]))
                .expect_err("a partial watermark cannot describe this refresh");

        assert!(
            error.contains("one pinned revision per binding occurrence"),
            "{error}"
        );
    }

    #[test]
    fn an_occurrence_read_at_another_object_fails_closed() {
        let fixture = crate::persistence::test_support::MvDocumentFixture::new();
        let error = MvPublicationInputs::freeze(
            &fixture.binding,
            &BTreeMap::from([(7, revision(11, 9)), (8, revision(12, 9))]),
        )
        .expect_err("a refresh that read elsewhere is not a refresh of this definition");

        assert!(
            error.contains("different object than binding occurrence 8"),
            "{error}"
        );
    }

    fn frozen_roundtrip(
        rows: Option<u64>,
    ) -> crate::persistence::codec::keyed::KeyedPublicationDocument {
        use crate::persistence::codec::keyed::{
            decode_publication_payload, encode_publication_payload,
        };
        let facts = crate::persistence::test_support::KeyedProjectionFixture::new(
            crate::product::MvTarget::from_parts(Some("ice"), "sales", "mv"),
            Some(1),
        )
        .build()
        .unwrap();
        let inputs = MvPublicationInputs::freeze(facts.binding(), &pinned_at_binding()).unwrap();
        let frozen = freeze_keyed_publication_document(
            facts.source_revision(),
            LakePublicationId::try_from_uuid(uuid::Uuid::now_v7()).unwrap(),
            &inputs,
            rows,
            1_700_000_020_000,
        )
        .unwrap();
        let encoded =
            encode_publication_payload(&frozen, &facts.versions().binding_document).unwrap();
        let decoded = decode_publication_payload(
            &encoded,
            facts.source_revision().definition_revision,
            facts.source_revision().interpretation_revision,
            &facts.versions().binding_document,
        )
        .unwrap();
        assert_eq!(decoded, frozen);
        decoded
    }

    #[test]
    fn a_frozen_publication_completes_its_own_document_set() {
        let publication = frozen_roundtrip(Some(3));
        assert_eq!(publication.logical_result_rows, Some(3));
        assert_eq!(publication.inputs.len(), 2);
    }

    #[test]
    fn a_publication_that_materialized_no_row_says_so() {
        assert_eq!(frozen_roundtrip(Some(0)).logical_result_rows, Some(0));
        assert_eq!(frozen_roundtrip(None).logical_result_rows, None);
    }
}
