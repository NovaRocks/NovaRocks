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

use novarocks_spi::connector::{
    ConnectorInstanceId, ConnectorProviderId, ConnectorTableIdentity, ConnectorTableObjectId,
};
use std::sync::Arc;
/// Mutable keyed projection inputs for application tests; every build decodes an actual keyed Current view.
pub struct KeyedProjectionFixture {
    pub target: crate::product::MvTarget,
    pub object_id: ConnectorTableObjectId,
    pub definition: crate::persistence::codec::DefinitionDocument,
    pub interpretation: crate::persistence::codec::InterpretationDocument,
    pub configuration: crate::persistence::codec::ConfigurationDocument,
    pub publication: Option<crate::persistence::codec::keyed::KeyedPublicationDocument>,
    pub metadata_version: novarocks_spi::connector::ConnectorCommittedVersion,
    pub file_totals: novarocks_spi::connector::SnapshotFileTotals,
}
impl KeyedProjectionFixture {
    pub fn new(target: crate::product::MvTarget, snapshot: Option<i64>) -> Self {
        let f = crate::persistence::test_support::MvDocumentFixture::new();
        let (catalog, view) =
            crate::persistence::keyed_documents::keyed_view_fixture(snapshot, snapshot.is_some());
        let observed =
            crate::persistence::keyed_documents::decode_current_keyed_documents(view, catalog)
                .expect("keyed fixture");
        Self {
            target,
            object_id: f.object_id,
            definition: f.definition,
            interpretation: f.interpretation,
            configuration: f.configuration,
            file_totals: observed.view.current_snapshot_file_totals,
            publication: observed.publication,
            metadata_version: novarocks_spi::connector::ConnectorCommittedVersion::try_new(
                bytes::Bytes::from_static(b"fixture-metadata"),
                snapshot,
            )
            .expect("fixture metadata"),
        }
    }
    pub fn with_retraction_count(mut self) -> Self {
        self.interpretation = crate::persistence::test_support::MvDocumentFixture::new()
            .with_retraction_count()
            .interpretation;
        self
    }
    pub fn build(
        mut self,
    ) -> Result<crate::persistence::keyed_documents::MvKeyedDocumentProjection, String> {
        use crate::persistence::{
            codec::keyed::{BindingDocument, DataVersionFormat},
            identity::ObjectIdentity,
            keyed_documents::*,
        };
        self.definition = crate::persistence::codec::build_definition(
            self.definition.created_at_ms,
            self.definition.query.clone(),
            self.definition.relation_occurrences.clone(),
            self.definition.outputs.clone(),
        )
        .map_err(|e| e.to_string())?;
        self.interpretation.computation_identity = self.definition.computation_identity;
        self.interpretation.target.object_id =
            ObjectIdentity::try_new(self.object_id.as_bytes().to_vec())
                .map_err(|e| e.to_string())?;
        let formats=self.definition.relation_occurrences.iter().map(|o|{
            let object=crate::persistence::exact_revision::restore_persisted_object(&o.object_id).map_err(|e|e.to_string())?;
            let fact=novarocks_spi::connector::ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(ConnectorProviderId::parse("iceberg").unwrap(),&object,Some(17)).map_err(|e|e.to_string())?;
            DataVersionFormat::from_issued_fact(fact.data_version()).map_err(|e|e.to_string())
        }).collect::<Result<Vec<_>,String>>()?;
        let versions = build_keyed_versions(
            &self.definition,
            &self.interpretation,
            BindingDocument {
                data_versions: formats,
            },
        )
        .map_err(|e| e.to_string())?;
        let snapshot = self.metadata_version.snapshot_id();
        let (_, mut view) = keyed_view_fixture(snapshot, false);
        view.table = ConnectorTableIdentity {
            instance_id: ConnectorInstanceId::parse(
                self.target
                    .catalog()
                    .ok_or("fixture target catalog missing")?,
            )
            .map_err(|e| e.to_string())?,
            namespace: Arc::from(self.target.namespace()),
            table: Arc::from(self.target.name()),
        };
        let catalog = novarocks_spi::connector::CatalogHandle::new(
            view.table.instance_id.clone(),
            novarocks_spi::connector::CatalogVersion::from_bytes([1; 32]),
        );
        view.owner.instance_id = view.table.instance_id.clone();
        view.object_id = self.object_id;
        view.metadata_version = self.metadata_version;
        view.current_snapshot_file_totals = self.file_totals;
        view.versions = versions
            .versions()
            .into_iter()
            .map(|v| (v.identity(), v))
            .collect();
        view.roots.get_mut("novarocks.mv").unwrap().identity = versions.binding.identity();
        view.slots
            .get_mut(&configuration_slot_name().map_err(|e| e.to_string())?)
            .unwrap()
            .value = keyed_configuration_slot_change(&self.configuration)
            .map_err(|e| e.to_string())?
            .slots
            .into_values()
            .next()
            .unwrap();
        view.slots
            .get_mut(&management_slot_name().map_err(|e| e.to_string())?)
            .unwrap()
            .value = management_slot(
            &crate::management::DeploymentOwner::parse("test-deployment").unwrap(),
            &crate::management::ProcessIncarnation::parse("test-process").unwrap(),
        )
        .map_err(|e| e.to_string())?;
        if let Some(p) = self.publication.as_mut() {
            p.definition_revision = versions.definition_revision();
            p.interpretation_revision = versions.interpretation_revision();
            view.current_records = keyed_publication_document_set(&versions, &self.definition, p)
                .map_err(|e| e.to_string())?
                .records;
        }
        let observed = decode_current_keyed_documents(view, catalog).map_err(|e| e.to_string())?;
        MvKeyedDocumentProjection::try_from_current(observed)
    }
}
pub fn sample_keyed_projection(
    target: crate::product::MvTarget,
    snapshot: Option<i64>,
) -> crate::persistence::keyed_documents::MvKeyedDocumentProjection {
    KeyedProjectionFixture::new(target, snapshot)
        .build()
        .expect("keyed projection")
}

/// Reconstruct the typed current facts for unit-only readiness sources. This
/// creates no management admission; those tests use their explicit test-only
/// admission helper separately.
pub fn observed_current_keyed(
    facts: &crate::persistence::keyed_documents::MvKeyedDocumentProjection,
    catalog: novarocks_spi::connector::CatalogHandle,
) -> crate::persistence::keyed_documents::MvKeyedObservedCurrentDocuments {
    use crate::persistence::keyed_documents::*;
    let source = facts.source_revision();
    let (_, mut view) = keyed_view_fixture(facts.metadata_version().snapshot_id(), false);
    view.owner.instance_id = source.target.instance_id.clone();
    view.table = source.target.clone();
    view.object_id = source.target_object_id.clone();
    view.metadata_version = facts.metadata_version().clone();
    view.current_snapshot_file_totals = facts.file_totals;
    view.versions = facts
        .versions()
        .versions()
        .into_iter()
        .map(|v| (v.identity(), v))
        .collect();
    view.roots.get_mut(MV_DOCUMENT_OWNER).unwrap().identity = facts.versions().binding.identity();
    view.slots
        .get_mut(&configuration_slot_name().unwrap())
        .unwrap()
        .value = keyed_configuration_slot_change(facts.configuration())
        .unwrap()
        .slots
        .into_values()
        .next()
        .unwrap();
    view.slots
        .get_mut(&management_slot_name().unwrap())
        .unwrap()
        .value = management_slot(&source.deployment_owner, &source.process_incarnation).unwrap();
    view.current_records = facts
        .publication()
        .map(|p| {
            keyed_publication_document_set(facts.versions(), facts.definition(), p)
                .unwrap()
                .records
        })
        .unwrap_or_default();
    decode_current_keyed_documents(view, catalog).expect("typed keyed Current fixture")
}
