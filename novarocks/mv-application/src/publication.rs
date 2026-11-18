// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information regarding
// copyright ownership.  The ASF licenses this file to you under the
// Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain a copy of the
// License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Immutable MV publication facts owned by the product.

use std::collections::BTreeSet;

use novarocks_spi::connector::document_storage::{
    ConnectorDocumentManagementAdmission, ConnectorDocumentManagementOperation,
    ConnectorDocumentPublicationSlotUpdate,
};
use novarocks_spi::connector::{
    ConnectorCommittedPartitioning, ConnectorCommittedVersion,
    ConnectorManagedPartitionSpecReplacement, ConnectorTableObjectId, ConnectorWriteReceipt,
    LakePublicationId,
};

/// The application publication technique selected before provider execution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MvRefreshPublicationTechnique {
    Full,
    Incremental,
    MetadataOnly,
}

/// One complete input watermark frozen before the refresh write is admitted.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvRefreshPublicationBase {
    /// Which mention of `table_fqn` this is. A definition may read one table
    /// twice, and the two mentions are read at their own points; the name is
    /// what they share, so it cannot be what tells them apart.
    occurrence_id: u32,
    table_fqn: String,
    table_object_id: ConnectorTableObjectId,
    from_snapshot: Option<i64>,
    to_snapshot: i64,
}

impl MvRefreshPublicationBase {
    pub fn try_new(
        occurrence_id: u32,
        table_fqn: String,
        table_object_id: ConnectorTableObjectId,
        from_snapshot: Option<i64>,
        to_snapshot: i64,
    ) -> Result<Self, String> {
        if table_fqn.is_empty()
            || from_snapshot.is_some_and(|snapshot| snapshot < 0)
            || to_snapshot < 0
        {
            return Err("invalid MV refresh publication base fact".to_string());
        }
        Ok(Self {
            occurrence_id,
            table_fqn,
            table_object_id,
            from_snapshot,
            to_snapshot,
        })
    }

    pub const fn occurrence_id(&self) -> u32 {
        self.occurrence_id
    }

    pub fn table_fqn(&self) -> &str {
        &self.table_fqn
    }

    pub fn table_object_id(&self) -> &ConnectorTableObjectId {
        &self.table_object_id
    }

    pub const fn from_snapshot(&self) -> Option<i64> {
        self.from_snapshot
    }

    pub const fn to_snapshot(&self) -> i64 {
        self.to_snapshot
    }
}

/// Complete immutable intent known before the writer commits. It deliberately
/// has no row-count or committed-version field: SQL preparation cannot
/// fabricate those provider facts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvRefreshPublicationIntent {
    publication_id: LakePublicationId,
    target_object_id: ConnectorTableObjectId,
    expected_target_snapshot_id: Option<i64>,
    /// The single management admission this publication commits under. It
    /// pins the exact target object and the publication identity, so a
    /// declaration built from it cannot name another generation.
    admission: ConnectorDocumentManagementAdmission,
    technique: MvRefreshPublicationTechnique,
    bases: Vec<MvRefreshPublicationBase>,
    definition_fingerprint: String,
    target_catalog: String,
    target_namespace: String,
    target_name: String,
    partition_spec_replacement: Option<ConnectorManagedPartitionSpecReplacement>,
    expected_committed_partitioning: Option<ConnectorCommittedPartitioning>,
    storage_projection_digest: Option<[u8; 32]>,
    configuration_slot_update: Option<ConnectorDocumentPublicationSlotUpdate>,
}

impl MvRefreshPublicationIntent {
    pub fn try_new(
        publication_id: LakePublicationId,
        target_object_id: ConnectorTableObjectId,
        expected_target_snapshot_id: Option<i64>,
        admission: ConnectorDocumentManagementAdmission,
        technique: MvRefreshPublicationTechnique,
        bases: Vec<MvRefreshPublicationBase>,
        definition_fingerprint: String,
        target_catalog: String,
        target_namespace: String,
        target_name: String,
    ) -> Result<Self, String> {
        if admission.operation() != ConnectorDocumentManagementOperation::Publication
            || admission.operation_id().to_bytes() != publication_id.to_bytes()
            || admission.expected_object_id() != Some(&target_object_id)
        {
            return Err(
                "MV refresh publication admission does not match its exact target".to_string(),
            );
        }
        if expected_target_snapshot_id.is_some_and(|snapshot| snapshot < 0)
            || bases.is_empty()
            || definition_fingerprint.is_empty()
            || target_catalog.is_empty()
            || target_namespace.is_empty()
            || target_name.is_empty()
        {
            return Err("invalid MV refresh publication intent".to_string());
        }
        // By occurrence, not by table: a definition may read one table twice,
        // and the two mentions are two inputs with one name and one object. An
        // occurrence repeated is two facts claiming to be the same input,
        // which is what this refuses.
        let mut occurrences = BTreeSet::new();
        if bases
            .iter()
            .any(|base| !occurrences.insert(base.occurrence_id))
        {
            return Err("MV refresh publication intent has duplicate base identity".to_string());
        }
        Ok(Self {
            publication_id,
            target_object_id,
            expected_target_snapshot_id,
            admission,
            technique,
            bases,
            definition_fingerprint,
            target_catalog,
            target_namespace,
            target_name,
            partition_spec_replacement: None,
            expected_committed_partitioning: None,
            storage_projection_digest: None,
            configuration_slot_update: None,
        })
    }

    pub const fn publication_id(&self) -> LakePublicationId {
        self.publication_id
    }
    pub fn target_object_id(&self) -> &ConnectorTableObjectId {
        &self.target_object_id
    }
    pub const fn expected_target_snapshot_id(&self) -> Option<i64> {
        self.expected_target_snapshot_id
    }
    pub const fn admission(&self) -> &ConnectorDocumentManagementAdmission {
        &self.admission
    }
    pub const fn technique(&self) -> MvRefreshPublicationTechnique {
        self.technique
    }
    pub fn bases(&self) -> &[MvRefreshPublicationBase] {
        &self.bases
    }
    pub fn definition_fingerprint(&self) -> &str {
        &self.definition_fingerprint
    }
    pub fn target_catalog(&self) -> &str {
        &self.target_catalog
    }
    pub fn target_namespace(&self) -> &str {
        &self.target_namespace
    }
    pub fn target_name(&self) -> &str {
        &self.target_name
    }

    pub fn staging_branch(&self) -> String {
        format!("__novarocks_mv_publication_{}", self.publication_id)
    }

    pub fn with_partition_spec_replacement(
        mut self,
        replacement: ConnectorManagedPartitionSpecReplacement,
        expected_committed_partitioning: ConnectorCommittedPartitioning,
        storage_projection_digest: Option<[u8; 32]>,
        configuration_slot_update: Option<ConnectorDocumentPublicationSlotUpdate>,
    ) -> Self {
        self.partition_spec_replacement = Some(replacement);
        self.expected_committed_partitioning = Some(expected_committed_partitioning);
        self.storage_projection_digest = storage_projection_digest;
        self.configuration_slot_update = configuration_slot_update;
        self
    }

    pub fn partition_spec_replacement(&self) -> Option<&ConnectorManagedPartitionSpecReplacement> {
        self.partition_spec_replacement.as_ref()
    }

    pub fn expected_committed_partitioning(&self) -> Option<&ConnectorCommittedPartitioning> {
        self.expected_committed_partitioning.as_ref()
    }

    pub const fn storage_projection_digest(&self) -> Option<[u8; 32]> {
        self.storage_projection_digest
    }

    pub const fn configuration_slot_update(
        &self,
    ) -> Option<&ConnectorDocumentPublicationSlotUpdate> {
        self.configuration_slot_update.as_ref()
    }
}

/// Facts admitted only after the provider has committed the exact write.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvRefreshCommittedFacts {
    intent: MvRefreshPublicationIntent,
    committed_version: ConnectorCommittedVersion,
    resulting_row_count: i64,
    committed_partitioning: Option<ConnectorCommittedPartitioning>,
}

impl MvRefreshCommittedFacts {
    pub fn from_write_receipt(
        intent: MvRefreshPublicationIntent,
        receipt: &ConnectorWriteReceipt,
    ) -> Result<Self, String> {
        let committed_version = receipt
            .committed_version()
            .cloned()
            .ok_or_else(|| "MV refresh write committed without a provider version".to_string())?;
        let resulting_row_count =
            i64::try_from(receipt.resulting_row_count().ok_or_else(|| {
                "MV refresh write committed without resulting row-count fact".to_string()
            })?)
            .map_err(|_| "MV refresh committed row count exceeds i64 range".to_string())?;
        let committed_partitioning = receipt.committed_partitioning().cloned();
        if intent.partition_spec_replacement().is_some() != committed_partitioning.is_some() {
            return Err(
                "MV refresh committed partitioning does not match the requested transition"
                    .to_string(),
            );
        }
        if committed_partitioning.as_ref() != intent.expected_committed_partitioning() {
            return Err(
                "MV refresh committed partitioning does not match its exact prepared preview"
                    .to_string(),
            );
        }
        Ok(Self {
            intent,
            committed_version,
            resulting_row_count,
            committed_partitioning,
        })
    }

    pub fn intent(&self) -> &MvRefreshPublicationIntent {
        &self.intent
    }
    pub fn committed_version(&self) -> &ConnectorCommittedVersion {
        &self.committed_version
    }
    pub const fn resulting_row_count(&self) -> i64 {
        self.resulting_row_count
    }
    pub fn committed_partitioning(&self) -> Option<&ConnectorCommittedPartitioning> {
        self.committed_partitioning.as_ref()
    }
}

/// Facts shared by data-producing and metadata-only refreshes once their
/// publication is known committed. Metadata-only refreshes have no write
/// receipt, so this value deliberately records the common publication proof
/// rather than fabricating write-committed facts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvRefreshPublicationFinalizationFacts {
    intent: MvRefreshPublicationIntent,
    publication_version: ConnectorCommittedVersion,
}

impl MvRefreshPublicationFinalizationFacts {
    pub fn try_new(
        intent: MvRefreshPublicationIntent,
        publication_version: ConnectorCommittedVersion,
    ) -> Result<Self, String> {
        Ok(Self {
            intent,
            publication_version,
        })
    }

    pub fn intent(&self) -> &MvRefreshPublicationIntent {
        &self.intent
    }

    pub fn publication_version(&self) -> &ConnectorCommittedVersion {
        &self.publication_version
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn publication_intent_rejects_duplicate_base_identity() {
        let base = MvRefreshPublicationBase::try_new(
            0,
            "ice.db.base".to_string(),
            ConnectorTableObjectId::try_new(bytes::Bytes::from_static(b"base-object-1"))
                .expect("valid test object ID"),
            None,
            7,
        )
        .expect("base");
        let publication_id = LakePublicationId::new_v7();
        let target_object_id =
            ConnectorTableObjectId::try_new(bytes::Bytes::from_static(b"mv-target-object"))
                .expect("valid target object ID");
        let error = MvRefreshPublicationIntent::try_new(
            publication_id,
            target_object_id.clone(),
            Some(7),
            crate::test_admission::publication_admission(
                "ice",
                "db",
                "mv",
                publication_id,
                &target_object_id,
            ),
            MvRefreshPublicationTechnique::Full,
            vec![base.clone(), base],
            "fingerprint".to_string(),
            "ice".to_string(),
            "db".to_string(),
            "mv".to_string(),
        )
        .expect_err("duplicate base must fail");
        assert_eq!(
            error,
            "MV refresh publication intent has duplicate base identity"
        );
    }
}
