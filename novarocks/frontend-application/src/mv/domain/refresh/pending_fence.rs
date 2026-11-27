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

//! Freeze the exact ContentLocate window and validate its one marker effect.

use crate::mv::domain::rewrite::context::IcebergMvRewriteContext;
use novarocks_mv_application::{
    persistence::{
        codec::{Marker, PendingEvidence, PendingVerificationRequirement, SourceVersionWindow},
        exact_revision::persist_exact_connector_revision,
        identity::{DocumentRevision, PublicationIdentity},
        keyed_documents::{MvKeyedObservedCurrentDocuments, MvStrictRecoveryPublication},
        resolution::ApplicablePublication,
    },
    product::MvRefreshAttemptIdentity,
};
use novarocks_spi::connector::{
    ConnectorMarkerKey, ConnectorMarkerValue, ConnectorSnapshotRecord, KeyedDocumentView,
};
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct PendingFence {
    publication: PublicationIdentity,
    evidence: PendingEvidence,
    recovery: Option<Arc<MvStrictRecoveryPublication>>,
    baseline_rows: Option<u64>,
    baseline_record: ConnectorSnapshotRecord,
    before: KeyedDocumentView,
    key: ConnectorMarkerKey,
    value: ConnectorMarkerValue,
}

impl PendingFence {
    pub(crate) fn freeze(
        original: &MvKeyedObservedCurrentDocuments,
        rewrite: &IcebergMvRewriteContext,
        attempt: &MvRefreshAttemptIdentity,
        requirement: PendingVerificationRequirement,
    ) -> Result<Self, String> {
        let ApplicablePublication::Bound(publication) = original.applicable_publication() else {
            return Err("MV content location has no applicable original publication".into());
        };
        let applicability = original.eligibility();
        if !applicability.rewrite_serving || applicability.strict_recovery_required {
            return Err("MV original publication cannot start a new content verification".into());
        }
        if original.view.object_id
            != rewrite
                .mv_definition
                .facts
                .source_revision()
                .target_object_id
            || original.versions != *rewrite.mv_definition.facts.versions()
            || original.publication_revision
                != rewrite
                    .mv_definition
                    .facts
                    .source_revision()
                    .publication_revision
            || original.view.current_snapshot != rewrite.target_snapshot_id
        {
            return Err(
                "MV content fence and the immutable rewrite use different generations".into(),
            );
        }
        let baseline_snapshot = rewrite
            .target_snapshot_id
            .ok_or("MV content verification has no original main snapshot")?;
        let storage = uuid::Uuid::parse_str(&rewrite.target_table_uuid)
            .map_err(|error| format!("MV content storage identity is invalid: {error}"))?;
        if storage.to_string().as_bytes() != original.view.object_id.as_bytes().as_ref() {
            return Err("MV content UUID differs from the original object identity".into());
        }
        let mut source_windows = rewrite
            .base_refs
            .iter()
            .map(|base| {
                let from = rewrite.previous_revision(base)?;
                let to = rewrite
                    .pin
                    .get(&base.occurrence_id)
                    .ok_or("MV content fence lost its exact To observation")?;
                let (from_object, from) =
                    persist_exact_connector_revision(from).map_err(|error| error.to_string())?;
                let (to_object, to) = persist_exact_connector_revision(&to.semantic_revision)
                    .map_err(|error| error.to_string())?;
                if from_object != to_object {
                    return Err("MV content window spans different source objects".into());
                }
                Ok(SourceVersionWindow {
                    occurrence: base.occurrence_id.get(),
                    from,
                    to,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        source_windows.sort_by_key(|window| window.occurrence);
        let evidence = PendingEvidence {
            attempt: attempt.publication_id.to_bytes(),
            refresh_identity: uuid::Uuid::now_v7(),
            source_windows,
            storage_uuid: *storage.as_bytes(),
            baseline_snapshot,
            mapping_revision: DocumentRevision::try_from_bytes(
                &original.versions.mapping.identity().to_bytes(),
            )
            .map_err(|error| error.to_string())?,
            verification_requirement: requirement,
        };
        let (key, value) = (Marker::Pending {
            publication: publication.clone(),
            evidence: evidence.clone(),
        })
        .encode()
        .map_err(|error| error.to_string())?;
        if original.view.markers.contains_key(&key) {
            return Err("MV content attempt already has an original marker".into());
        }
        let baseline_record = original
            .view
            .current_records
            .iter()
            .find(|record| {
                record.owner
                    == novarocks_mv_application::persistence::keyed_documents::MV_DOCUMENT_OWNER
                    && record.name == "publication"
            })
            .ok_or("MV content fence has no exact current publication record")?
            .clone();
        Ok(Self {
            publication: publication.clone(),
            evidence,
            recovery: None,
            baseline_record,
            baseline_rows: original
                .publication
                .as_ref()
                .and_then(|publication| publication.logical_result_rows),
            before: original.view.clone(),
            key,
            value,
        })
    }

    pub(crate) fn recover(
        observed: &MvKeyedObservedCurrentDocuments,
        publication: PublicationIdentity,
        evidence: PendingEvidence,
        baseline_rows: Option<u64>,
        baseline_record: ConnectorSnapshotRecord,
        recovery: MvStrictRecoveryPublication,
    ) -> Result<Self, String> {
        let (key, value) = (Marker::Pending {
            publication: publication.clone(),
            evidence: evidence.clone(),
        })
        .encode()
        .map_err(|error| error.to_string())?;
        let fence = Self {
            publication,
            evidence,
            before: observed.view.clone(),
            key,
            value,
            recovery: Some(Arc::new(recovery)),
            baseline_rows,
            baseline_record,
        };
        fence.verify(observed)?;
        Ok(fence)
    }

    pub(crate) fn baseline_record(&self) -> &ConnectorSnapshotRecord {
        &self.baseline_record
    }

    pub(crate) fn evidence(&self) -> &PendingEvidence {
        &self.evidence
    }

    pub(crate) const fn baseline_rows(&self) -> Option<u64> {
        self.baseline_rows
    }

    pub(crate) const fn is_recovery(&self) -> bool {
        self.recovery.is_some()
    }

    pub(crate) fn recovery_publication(&self) -> Option<Arc<MvStrictRecoveryPublication>> {
        self.recovery.clone()
    }

    pub(crate) fn marker(&self) -> Marker {
        Marker::Pending {
            publication: self.publication.clone(),
            evidence: self.evidence.clone(),
        }
    }

    pub(crate) fn release(&self) -> Marker {
        Marker::PendingReleased {
            publication: self.publication.clone(),
            attempt: self.evidence.attempt,
        }
    }

    pub(crate) fn is_present(&self, view: &KeyedDocumentView) -> bool {
        view.object_id == self.before.object_id && view.markers.get(&self.key) == Some(&self.value)
    }

    pub(crate) fn key(&self) -> &ConnectorMarkerKey {
        &self.key
    }

    pub(crate) fn publication(&self) -> &PublicationIdentity {
        &self.publication
    }

    pub(crate) fn requirement(&self) -> PendingVerificationRequirement {
        self.evidence.verification_requirement
    }

    pub(crate) fn verify(&self, documents: &MvKeyedObservedCurrentDocuments) -> Result<(), String> {
        let mut expected = self.before.clone();
        expected
            .markers
            .insert(self.key.clone(), self.value.clone());
        expected.metadata_version = documents.view.metadata_version.clone();
        let current_publication = match documents.applicable_publication() {
            ApplicablePublication::Bound(publication) => publication == &self.publication,
            ApplicablePublication::Unattributed => self.is_recovery(),
            _ => false,
        };
        if documents.view != expected
            || !current_publication
            || !documents.eligibility().strict_recovery_required
        {
            return Err(
                "MV Current differs from the original plus its exact Pending marker".into(),
            );
        }
        Ok(())
    }
}
