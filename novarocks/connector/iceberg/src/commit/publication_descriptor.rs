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

//! Immutable request and attribution facts; no live client or credential.
use super::model::{BaseIdentity, FrozenRequest};
use crate::iceberg::{Error, ErrorKind, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum AttributionPlan {
    ObserveOnly,
    Snapshot {
        table_uuid: Uuid,
        snapshot_id: i64,
        marker_key: String,
        marker_value: String,
    },
    Property {
        table_uuid: Uuid,
        key: String,
        value: String,
    },
    ExactMetadata {
        table_uuid: Uuid,
        location: String,
        digest: String,
    },
    ViewOperation {
        view_uuid: Option<Uuid>,
        operation_id: String,
    },
}

/// Facts returned by the exact read-only probe declared in AttributionPlan.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PositiveAttributionObservation {
    SnapshotMarker {
        table_uuid: Uuid,
        snapshot_id: i64,
        marker_key: String,
        marker_value: String,
    },
    PropertyMarker {
        table_uuid: Uuid,
        key: String,
        value: String,
    },
    ExactMetadata {
        table_uuid: Uuid,
        location: String,
        digest: String,
    },
    ViewOperation {
        view_uuid: Option<Uuid>,
        operation_id: String,
    },
}
impl AttributionPlan {
    /// Extract only the positive probe frozen for this request. Missing markers
    /// and current ref state provide no observation; the journal checks exact
    /// UUID, snapshot and marker bytes before accepting the returned facts.
    pub(crate) fn observe_table_metadata(
        &self,
        metadata: &crate::iceberg::spec::TableMetadata,
    ) -> Option<PositiveAttributionObservation> {
        match self {
            Self::Snapshot {
                snapshot_id,
                marker_key,
                ..
            } => {
                let snapshot = metadata.snapshot_by_id(*snapshot_id)?;
                Some(PositiveAttributionObservation::SnapshotMarker {
                    table_uuid: metadata.uuid(),
                    snapshot_id: *snapshot_id,
                    marker_key: marker_key.clone(),
                    marker_value: snapshot
                        .summary()
                        .additional_properties
                        .get(marker_key)?
                        .clone(),
                })
            }
            Self::Property { key, .. } => Some(PositiveAttributionObservation::PropertyMarker {
                table_uuid: metadata.uuid(),
                key: key.clone(),
                value: metadata.properties().get(key)?.clone(),
            }),
            _ => None,
        }
    }

    pub(crate) fn matches_observation(&self, observation: &PositiveAttributionObservation) -> bool {
        match (self, observation) {
            (
                Self::Snapshot {
                    table_uuid: a,
                    snapshot_id: b,
                    marker_key: c,
                    marker_value: d,
                },
                PositiveAttributionObservation::SnapshotMarker {
                    table_uuid: w,
                    snapshot_id: x,
                    marker_key: y,
                    marker_value: z,
                },
            ) => (a, b, c, d) == (w, x, y, z),
            (
                Self::Property {
                    table_uuid: a,
                    key: b,
                    value: c,
                },
                PositiveAttributionObservation::PropertyMarker {
                    table_uuid: x,
                    key: y,
                    value: z,
                },
            ) => (a, b, c) == (x, y, z),
            (
                Self::ExactMetadata {
                    table_uuid: a,
                    location: b,
                    digest: c,
                },
                PositiveAttributionObservation::ExactMetadata {
                    table_uuid: x,
                    location: y,
                    digest: z,
                },
            ) => (a, b, c) == (x, y, z),
            (
                Self::ViewOperation {
                    view_uuid: a,
                    operation_id: b,
                },
                PositiveAttributionObservation::ViewOperation {
                    view_uuid: x,
                    operation_id: y,
                },
            ) => (a, b) == (x, y),
            _ => false,
        }
    }
    fn table_uuid(&self) -> Option<Uuid> {
        match self {
            Self::Snapshot { table_uuid, .. }
            | Self::Property { table_uuid, .. }
            | Self::ExactMetadata { table_uuid, .. } => Some(*table_uuid),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum RecoveryPrincipalScope {
    Anonymous,
    CatalogGeneration { generation: [u8; 16] },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RecoveryBinding {
    pub generation: [u8; 16],
    pub method: String,
    pub endpoint: String,
    pub warehouse: Option<String>,
    pub query: Vec<(String, String)>,
    pub principal_scope: RecoveryPrincipalScope,
    pub deduplication_domain: String,
    pub lifetime: Option<std::time::Duration>,
}

impl RecoveryBinding {
    pub(crate) fn validate_generation(&self, generation: [u8; 16]) -> Result<()> {
        let url = reqwest::Url::parse(&self.endpoint)
            .map_err(|_| Error::new(ErrorKind::DataInvalid, "Invalid REST recovery endpoint"))?;
        let query: Vec<_> = url
            .query_pairs()
            .map(|(key, value)| (key.into_owned(), value.into_owned()))
            .collect();
        if self.generation != generation
            || matches!(&self.principal_scope, RecoveryPrincipalScope::CatalogGeneration { generation: principal } if *principal != generation)
            || !matches!(self.method.as_str(), "POST" | "DELETE")
            || !matches!(url.scheme(), "http" | "https")
            || !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
            || self.deduplication_domain.is_empty()
            || query != self.query
            || self.lifetime.is_some_and(|duration| duration.is_zero())
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "REST recovery binding differs from its exact generation or request scope",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PublicationDescriptor {
    pub operation_id: [u8; 16],
    pub key: Uuid,
    pub targets: Vec<String>,
    pub table_uuid: Option<Uuid>,
    pub request: Vec<u8>,
    pub fingerprint: [u8; 32],
    pub attribution: AttributionPlan,
    pub binding: Option<RecoveryBinding>,
    pub first_dispatch_ms: Option<i64>,
    // Recovery cannot infer monotonic guard consumption from the wire alone.
    pub base_format_version: Option<u8>,
    pub base_last_sequence_number: Option<i64>,
    pub base_next_row_id: Option<u64>,
}
impl std::fmt::Debug for PublicationDescriptor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PublicationDescriptor")
            .field("key", &self.key)
            .field("table_uuid", &self.table_uuid)
            .field("request_bytes", &self.request.len())
            .field("fingerprint", &self.fingerprint)
            .field("bound", &self.binding.is_some())
            .field("first_dispatch_ms", &self.first_dispatch_ms)
            .finish_non_exhaustive()
    }
}

impl PublicationDescriptor {
    pub(crate) fn catalog(
        operation_id: [u8; 16],
        targets: Vec<String>,
        request: Vec<u8>,
        attribution: AttributionPlan,
    ) -> Result<Self> {
        Self::catalog_with_key(operation_id, targets, request, attribution, Uuid::now_v7())
    }
    fn catalog_with_key(
        operation_id: [u8; 16],
        targets: Vec<String>,
        request: Vec<u8>,
        attribution: AttributionPlan,
        key: Uuid,
    ) -> Result<Self> {
        if targets.is_empty() || targets.iter().any(|target| target.is_empty()) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Publication requires exact nonempty targets",
            ));
        }
        let fingerprint = Sha256::digest(&request).into();
        Ok(Self {
            operation_id,
            key,
            targets,
            table_uuid: attribution.table_uuid(),
            request,
            fingerprint,
            attribution,
            binding: None,
            first_dispatch_ms: None,
            base_format_version: None,
            base_last_sequence_number: None,
            base_next_row_id: None,
        })
    }
    pub(crate) fn frozen(request: &FrozenRequest) -> Result<Self> {
        Self::frozen_with_key(request, Uuid::now_v7())
    }
    pub(crate) fn validate_catalog(
        &self,
        operation_id: [u8; 16],
        targets: Vec<String>,
        request: Vec<u8>,
        attribution: AttributionPlan,
    ) -> Result<()> {
        self.validate()?;
        let mut expected =
            Self::catalog_with_key(operation_id, targets, request, attribution, self.key)?;
        expected.binding = self.binding.clone();
        if self != &expected {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Catalog publication descriptor differs from its exact request facts",
            ));
        }
        Ok(())
    }
    pub(crate) fn validate_frozen(&self, request: &FrozenRequest) -> Result<()> {
        self.validate()?;
        let mut expected = Self::frozen_with_key(request, self.key)?;
        expected.binding = self.binding.clone();
        if self != &expected {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Frozen publication descriptor differs from its exact request facts",
            ));
        }
        Ok(())
    }
    fn frozen_with_key(request: &FrozenRequest, key: Uuid) -> Result<Self> {
        let table_uuid = match request.base() {
            BaseIdentity::Existing { uuid, .. } => *uuid,
            BaseIdentity::Create { staged } => staged.initial_metadata().uuid(),
        };
        // Presence of an old snapshot or a matching ref is not operation
        // attribution. Only markers written by this exact request are probes.
        let mut attribution = AttributionPlan::ObserveOnly;
        for update in request.updates() {
            match update {
                crate::iceberg::TableUpdate::AddSnapshot { snapshot } => {
                    if let Some((key, value)) = snapshot
                        .summary()
                        .additional_properties
                        .iter()
                        .find(|(key, _)| {
                            matches!(
                                key.as_str(),
                                "novarocks.write.session.v1"
                                    | "novarocks.connector.data-mutation.v1"
                                    | "novarocks.connector.maintenance.v1"
                                    | "novarocks.connector.maintenance.v2"
                                    | "novarocks.mv.publication_id"
                            )
                        })
                    {
                        attribution = AttributionPlan::Snapshot {
                            table_uuid,
                            snapshot_id: snapshot.snapshot_id(),
                            marker_key: key.clone(),
                            marker_value: value.clone(),
                        };
                    }
                }
                crate::iceberg::TableUpdate::SetProperties { updates } => {
                    if let Some((key, value)) = updates.iter().find(|(key, _)| {
                        matches!(
                            key.as_str(),
                            "novarocks.connector.maintenance.v1"
                                | "novarocks.connector.maintenance.v2"
                                | "novarocks.ctas.operation-id"
                                | "novarocks.documents.update.operation-id.v1"
                        )
                    }) {
                        attribution = AttributionPlan::Property {
                            table_uuid,
                            key: key.clone(),
                            value: value.clone(),
                        };
                    }
                }
                _ => {}
            }
        }
        let bytes = serde_json::to_vec(&request.to_rest_json().map_err(|error| {
            Error::new(ErrorKind::DataInvalid, "Cannot encode frozen publication")
                .with_source(error)
        })?)
        .map_err(|error| {
            Error::new(ErrorKind::DataInvalid, "Cannot encode frozen publication")
                .with_source(error)
        })?;
        let mut descriptor = Self::catalog_with_key(
            request.artifacts().attempt().operation().to_bytes(),
            vec![format!("{}", request.identifier())],
            bytes,
            attribution,
            key,
        )?;
        descriptor.table_uuid = Some(table_uuid);
        if let Some(facts) = request.base_facts() {
            descriptor.base_format_version = Some(facts.format_version);
            descriptor.base_last_sequence_number = Some(facts.last_sequence_number);
            descriptor.base_next_row_id = Some(facts.next_row_id);
        }
        Ok(descriptor)
    }
    pub(crate) fn validate(&self) -> Result<()> {
        if self.key.get_version_num() != 7
            || self.targets.is_empty()
            || self.targets.iter().any(|target| target.is_empty())
            || <[u8; 32]>::from(Sha256::digest(&self.request)) != self.fingerprint
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Invalid frozen publication descriptor",
            ));
        }
        if let Some(binding) = &self.binding {
            binding.validate_generation(binding.generation)?;
        }
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_request_round_trip_has_unique_v7_key_and_checked_fingerprint() {
        let original = PublicationDescriptor::catalog(
            [1; 16],
            vec!["namespace:db".into()],
            br#"{"namespace":["db"]}"#.to_vec(),
            AttributionPlan::ObserveOnly,
        )
        .unwrap();
        let restored: PublicationDescriptor =
            serde_json::from_slice(&serde_json::to_vec(&original).unwrap()).unwrap();
        restored.validate().unwrap();
        assert_eq!(original, restored);
        let next = PublicationDescriptor::catalog(
            [1; 16],
            vec!["namespace:db".into()],
            original.request.clone(),
            AttributionPlan::ObserveOnly,
        )
        .unwrap();
        assert_ne!(original.key, next.key);
        let mut changed = restored;
        changed.request.push(b' ');
        assert!(changed.validate().is_err());
    }
    #[test]
    fn recovery_binding_preserves_submillisecond_lifetime_without_exposing_request_debug() {
        let mut descriptor = PublicationDescriptor::catalog(
            [1; 16],
            vec!["namespace:db".into()],
            br#"{"credential":"request-canary"}"#.to_vec(),
            AttributionPlan::ObserveOnly,
        )
        .unwrap();
        descriptor.binding = Some(RecoveryBinding {
            generation: [2; 16],
            method: "POST".into(),
            endpoint: "https://catalog.example/v1/namespaces".into(),
            warehouse: None,
            query: vec![],
            principal_scope: RecoveryPrincipalScope::CatalogGeneration {
                generation: [2; 16],
            },
            deduplication_domain: "catalog.example".into(),
            lifetime: Some(std::time::Duration::from_nanos(1)),
        });
        let restored: PublicationDescriptor =
            serde_json::from_slice(&serde_json::to_vec(&descriptor).unwrap()).unwrap();
        assert_eq!(
            restored.binding.unwrap().lifetime,
            Some(std::time::Duration::from_nanos(1))
        );
        for debug in [format!("{descriptor:?}"), format!("{descriptor:#?}")] {
            assert!(!debug.contains("request-canary"));
            assert!(!debug.contains("credential"));
        }
    }

    #[tokio::test]
    async fn metadata_descriptors_capture_baseline_and_actual_ctas_document_markers() {
        use crate::commit::model::*;
        use crate::commit::staging::{PreparedChange, StagingBase, StagingEngine};
        use crate::iceberg::spec::FormatVersion;
        use crate::iceberg::{NamespaceIdent, TableIdent, TableUpdate};
        use std::collections::{BTreeMap, HashMap};
        let fixture = crate::commit::overwrite::preparer_tests::Fixture::new();
        let metadata = fixture.metadata(FormatVersion::V3);
        for key in [
            "novarocks.ctas.operation-id",
            "novarocks.documents.update.operation-id.v1",
        ] {
            let attempt = fixture.operation.begin_attempt().unwrap();
            let intent = OperationIntent::new(OperationIntentParts {
                target: TableTarget {
                    ident: TableIdent::new(NamespaceIdent::new("db".into()), "t".into()),
                    uuid: Some(metadata.uuid()),
                },
                target_ref: "main".into(),
                start: None,
                changes: FileChanges::default(),
                dependencies: vec![],
                isolation: IsolationLevel::Snapshot,
                shape: RequestShape::MetadataOnly,
                summary: BTreeMap::new(),
                token: fixture.operation.token(),
            })
            .unwrap();
            let mut engine = StagingEngine::begin(
                StagingBase::Existing {
                    metadata: metadata.clone(),
                    metadata_location: "s3://bucket/metadata/base.json".into(),
                },
                &intent,
                &attempt,
            )
            .unwrap();
            engine
                .stage_change(PreparedChange {
                    updates: vec![TableUpdate::SetProperties {
                        updates: HashMap::from([(key.into(), "operation-1".into())]),
                    }],
                    requirements: vec![],
                })
                .unwrap();
            let request = engine.freeze().unwrap();
            let descriptor = PublicationDescriptor::frozen(&request).unwrap();
            assert_eq!(descriptor.base_format_version, Some(3));
            assert_eq!(
                descriptor.base_last_sequence_number,
                Some(metadata.last_sequence_number())
            );
            assert_eq!(descriptor.base_next_row_id, Some(metadata.next_row_id()));
            assert_eq!(
                descriptor.attribution,
                AttributionPlan::Property {
                    table_uuid: metadata.uuid(),
                    key: key.into(),
                    value: "operation-1".into()
                }
            );
        }
        let request = FrozenRequest::new(FrozenRequestParts {
            shape: RequestShape::RefRetention,
            target: TableIdent::new(NamespaceIdent::new("db".into()), "t".into()),
            target_ref: "main".into(),
            base: BaseIdentity::Existing {
                uuid: metadata.uuid(),
                parent: None,
                metadata_location: "s3://bucket/metadata/base.json".into(),
            },
            requirements: vec![crate::iceberg::TableRequirement::UuidMatch {
                uuid: metadata.uuid(),
            }],
            updates: vec![],
            artifacts: AttemptArtifacts::empty(
                fixture.operation.begin_attempt().unwrap().attempt_token(),
            ),
        })
        .unwrap()
        .with_base_facts(&metadata)
        .unwrap();
        assert_eq!(
            PublicationDescriptor::frozen(&request).unwrap().attribution,
            AttributionPlan::ObserveOnly
        );
    }
}
