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

//! Provider-private reconciliation payloads carried through SPI as opaque bytes.

use serde::{Deserialize, Serialize};

pub const ICEBERG_MUTATION_EVIDENCE_VERSION: u16 = 2;
pub const ICEBERG_STATISTICS_EVIDENCE_VERSION: u16 = 1;
pub const ICEBERG_STAGED_PUBLICATION_PROOF_VERSION: u16 = 1;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct IcebergStagedPublicationProofV1 {
    pub version: u16,
    pub descriptor_digest: Vec<u8>,
    pub namespace: String,
    pub table: String,
    pub table_uuid: String,
    pub staging_ref: String,
    pub staging_snapshot_id: Option<i64>,
    pub target_ref: String,
    pub target_snapshot_id: Option<i64>,
    pub refresh_id: i64,
    pub mv_id: i64,
    pub marker_token: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct IcebergMutationEvidenceV1 {
    pub version: u16,
    pub target: IcebergMutationEvidenceTarget,
}

#[derive(Debug, Deserialize, Serialize)]
pub enum IcebergMutationEvidenceTarget {
    Namespace {
        namespace: String,
        should_exist: bool,
    },
    Table {
        namespace: String,
        table: String,
        should_exist: bool,
        before_uuid: Option<String>,
    },
    HadoopCreate {
        namespace: String,
        table: String,
        expected_uuid: String,
        metadata_location: String,
        metadata_digest: String,
        operation_id: String,
    },
    View {
        namespace: String,
        view: String,
        should_exist: bool,
    },
    TableVersion {
        namespace: String,
        table: String,
        table_uuid: String,
        before_metadata_location: Option<String>,
    },
    ApplicationDocuments {
        namespace: String,
        table: String,
        table_uuid: String,
        operation_marker: String,
        manifest_digest: [u8; 32],
        managed_kind: String,
        managed_owner: String,
        managed_incarnation: String,
    },
    MvMetadataOnlyStage {
        namespace: String,
        table: String,
        table_uuid: String,
        staging_branch: String,
        staging_snapshot_id: i64,
        provenance_hash: String,
    },
    Ref {
        namespace: String,
        table: String,
        table_uuid: String,
        ref_name: String,
        expected_snapshot_id: Option<i64>,
    },
    GuardedFastForward {
        namespace: String,
        table: String,
        table_uuid: String,
        before_metadata_location: Option<String>,
        source_branch: String,
        target_branch: String,
        source_snapshot_id: i64,
        expected_target_snapshot_id: Option<i64>,
        guard_digest: [u8; 32],
    },
}

#[derive(Debug, Deserialize, Serialize)]
pub struct IcebergStatisticsEvidenceV1 {
    pub version: u16,
    pub namespace: String,
    pub table: String,
    pub data_version: Vec<u8>,
    pub statistics_path: String,
}

pub fn encode_mutation_evidence(value: &IcebergMutationEvidenceV1) -> Result<Vec<u8>, String> {
    require_version(
        value.version,
        ICEBERG_MUTATION_EVIDENCE_VERSION,
        "mutation evidence",
    )?;
    serde_json::to_vec(value).map_err(|error| format!("encode Iceberg mutation evidence: {error}"))
}

pub fn decode_mutation_evidence(payload: &[u8]) -> Result<IcebergMutationEvidenceV1, String> {
    let value: IcebergMutationEvidenceV1 = serde_json::from_slice(payload)
        .map_err(|error| format!("decode Iceberg mutation evidence: {error}"))?;
    require_version(
        value.version,
        ICEBERG_MUTATION_EVIDENCE_VERSION,
        "mutation evidence",
    )?;
    Ok(value)
}

pub fn encode_statistics_evidence(value: &IcebergStatisticsEvidenceV1) -> Result<Vec<u8>, String> {
    require_version(
        value.version,
        ICEBERG_STATISTICS_EVIDENCE_VERSION,
        "statistics evidence",
    )?;
    serde_json::to_vec(value)
        .map_err(|error| format!("encode Iceberg statistics evidence: {error}"))
}

pub fn decode_statistics_evidence(payload: &[u8]) -> Result<IcebergStatisticsEvidenceV1, String> {
    let value: IcebergStatisticsEvidenceV1 = serde_json::from_slice(payload)
        .map_err(|error| format!("decode Iceberg statistics evidence: {error}"))?;
    require_version(
        value.version,
        ICEBERG_STATISTICS_EVIDENCE_VERSION,
        "statistics evidence",
    )?;
    Ok(value)
}

pub fn encode_staged_publication_proof(
    value: &IcebergStagedPublicationProofV1,
) -> Result<Vec<u8>, String> {
    require_version(
        value.version,
        ICEBERG_STAGED_PUBLICATION_PROOF_VERSION,
        "staged-publication proof",
    )?;
    serde_json::to_vec(value)
        .map_err(|error| format!("encode Iceberg staged-publication proof: {error}"))
}

pub fn decode_staged_publication_proof(
    payload: &[u8],
) -> Result<IcebergStagedPublicationProofV1, String> {
    let value: IcebergStagedPublicationProofV1 = serde_json::from_slice(payload)
        .map_err(|error| format!("decode Iceberg staged-publication proof: {error}"))?;
    require_version(
        value.version,
        ICEBERG_STAGED_PUBLICATION_PROOF_VERSION,
        "staged-publication proof",
    )?;
    Ok(value)
}

fn require_version(actual: u16, expected: u16, kind: &str) -> Result<(), String> {
    if actual == expected {
        Ok(())
    } else {
        Err(format!("unsupported Iceberg {kind} version"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reconciliation_codecs_reject_unsupported_versions() {
        let evidence = IcebergStatisticsEvidenceV1 {
            version: 2,
            namespace: "db".to_string(),
            table: "t".to_string(),
            data_version: vec![1],
            statistics_path: "stats.puffin".to_string(),
        };
        assert!(encode_statistics_evidence(&evidence).is_err());

        let mutation = IcebergMutationEvidenceV1 {
            version: 1,
            target: IcebergMutationEvidenceTarget::Namespace {
                namespace: "db".to_string(),
                should_exist: true,
            },
        };
        assert!(encode_mutation_evidence(&mutation).is_err());
    }
}
