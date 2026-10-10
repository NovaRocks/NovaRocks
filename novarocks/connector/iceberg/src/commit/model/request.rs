// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Frozen requests already contain every requirement and update before owner handoff.

use std::sync::Arc;

use serde_json::Value;
use uuid::Uuid;

use super::{AttemptArtifacts, OperationToken, invalid};
use crate::iceberg::spec::TableMetadata;
use crate::iceberg::{Result, TableCommit, TableIdent, TableRequirement, TableUpdate};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequestShape {
    SnapshotProducing,
    MetadataOnly,
    Create,
}

#[derive(Clone, Debug)]
pub struct StagedCreateIdentity {
    operation: OperationToken,
    initial_metadata: Arc<TableMetadata>,
}

impl StagedCreateIdentity {
    pub fn new(operation: OperationToken, initial_metadata: Arc<TableMetadata>) -> Self {
        Self {
            operation,
            initial_metadata,
        }
    }
    pub const fn operation(&self) -> OperationToken {
        self.operation
    }
    pub fn initial_metadata(&self) -> &TableMetadata {
        &self.initial_metadata
    }
}

#[derive(Clone, Debug)]
pub enum BaseIdentity {
    Existing {
        uuid: Uuid,
        parent: Option<i64>,
        metadata_location: String,
    },
    Create {
        staged: StagedCreateIdentity,
    },
}

#[derive(Debug)]
pub struct FrozenRequestParts {
    pub shape: RequestShape,
    pub target: TableIdent,
    pub target_ref: String,
    pub base: BaseIdentity,
    pub requirements: Vec<TableRequirement>,
    pub updates: Vec<TableUpdate>,
    pub artifacts: AttemptArtifacts,
}

/// Deliberately not Clone: one owner accepts the complete request and dispatches it once.
#[derive(Debug)]
pub struct FrozenRequest {
    parts: FrozenRequestParts,
}

impl FrozenRequest {
    pub fn new(parts: FrozenRequestParts) -> Result<Self> {
        if parts.target.name.is_empty() || parts.target_ref.is_empty() {
            return Err(invalid(
                "Frozen request requires an exact table name and target ref",
            ));
        }
        match (&parts.base, parts.shape) {
            (
                BaseIdentity::Existing {
                    uuid,
                    parent,
                    metadata_location,
                },
                RequestShape::SnapshotProducing | RequestShape::MetadataOnly,
            ) => {
                if metadata_location.is_empty() {
                    return Err(invalid(
                        "Existing request is missing its loaded metadata location",
                    ));
                }
                if !parts
                    .requirements
                    .contains(&TableRequirement::UuidMatch { uuid: *uuid })
                {
                    return Err(invalid(
                        "Existing frozen request must assert its table UUID",
                    ));
                }
                for requirement in &parts.requirements {
                    match requirement {
                        TableRequirement::UuidMatch { uuid: required } if required != uuid => {
                            return Err(invalid(
                                "Frozen request carries conflicting table UUID assertions",
                            ));
                        }
                        TableRequirement::NotExist => {
                            return Err(invalid("Existing request cannot assert table creation"));
                        }
                        TableRequirement::RefSnapshotIdMatch { r#ref, snapshot_id }
                            if r#ref == &parts.target_ref && snapshot_id != parent =>
                        {
                            return Err(invalid(
                                "Frozen request target-ref assertion does not match its loaded parent",
                            ));
                        }
                        _ => {}
                    }
                }
                if parts.shape == RequestShape::SnapshotProducing {
                    let cas = TableRequirement::RefSnapshotIdMatch {
                        r#ref: parts.target_ref.clone(),
                        snapshot_id: *parent,
                    };
                    if !parts.requirements.contains(&cas) {
                        return Err(invalid(
                            "Snapshot-producing request must assert its exact target-ref parent",
                        ));
                    }
                } else if parts
                    .requirements
                    .iter()
                    .any(|r| matches!(r, TableRequirement::RefSnapshotIdMatch { .. }))
                {
                    return Err(invalid(
                        "Metadata-only publication must not acquire a ref CAS",
                    ));
                }
                if parts.shape == RequestShape::MetadataOnly
                    && parts.updates.iter().any(|update| {
                        matches!(
                            update,
                            TableUpdate::AddSnapshot { .. } | TableUpdate::SetSnapshotRef { .. }
                        )
                    })
                {
                    return Err(invalid(
                        "Metadata-only publication cannot produce a snapshot or advance a ref",
                    ));
                }
            }
            (BaseIdentity::Create { staged }, RequestShape::Create) => {
                if parts.requirements != [TableRequirement::NotExist] {
                    return Err(invalid(
                        "Create frozen request must contain only assert-create",
                    ));
                }
                if parts.artifacts.attempt().operation() != staged.operation() {
                    return Err(invalid(
                        "Create request artifacts belong to another staged operation",
                    ));
                }
            }
            _ => {
                return Err(invalid(
                    "Frozen request shape does not match its base identity",
                ));
            }
        }
        Ok(Self { parts })
    }
    pub const fn shape(&self) -> RequestShape {
        self.parts.shape
    }
    pub fn identifier(&self) -> &TableIdent {
        &self.parts.target
    }
    pub fn target_ref(&self) -> &str {
        &self.parts.target_ref
    }
    pub fn base(&self) -> &BaseIdentity {
        &self.parts.base
    }
    pub fn requirements(&self) -> &[TableRequirement] {
        &self.parts.requirements
    }
    pub fn updates(&self) -> &[TableUpdate] {
        &self.parts.updates
    }
    pub fn artifacts(&self) -> &AttemptArtifacts {
        &self.parts.artifacts
    }
    pub fn has_updates(&self) -> bool {
        !self.parts.updates.is_empty()
    }
    /// Matches the publication owner query: the final SetSnapshotRef for this exact ref.
    pub fn ref_snapshot_after(&self, ref_name: &str) -> Option<i64> {
        self.parts
            .updates
            .iter()
            .rev()
            .find_map(|update| match update {
                TableUpdate::SetSnapshotRef {
                    ref_name: updated_ref,
                    reference,
                } if updated_ref == ref_name => Some(reference.snapshot_id),
                _ => None,
            })
    }
    /// The standard REST commit envelope. Arrays preserve execution order; object keys are stable.
    pub fn to_rest_json(&self) -> serde_json::Result<Value> {
        #[derive(serde::Serialize)]
        struct Envelope<'a> {
            identifier: &'a TableIdent,
            requirements: &'a [TableRequirement],
            updates: &'a [TableUpdate],
        }
        serde_json::to_value(Envelope {
            identifier: self.identifier(),
            requirements: self.requirements(),
            updates: self.updates(),
        })
        .map(canonical_json)
    }
    /// Only the catalog dispatch boundary calls this transitional SDK projection.
    /// Rust restricted visibility cannot name the sibling catalog module.
    pub(crate) fn into_table_commit(&self) -> TableCommit {
        TableCommit::builder()
            .ident(self.parts.target.clone())
            .requirements(self.parts.requirements.clone())
            .updates(self.parts.updates.clone())
            .build()
    }
}

fn canonical_json(value: Value) -> Value {
    match value {
        Value::Object(map) => {
            let sorted: std::collections::BTreeMap<_, _> = map
                .into_iter()
                .map(|(key, value)| (key, canonical_json(value)))
                .collect();
            Value::Object(sorted.into_iter().collect())
        }
        Value::Array(values) => Value::Array(values.into_iter().map(canonical_json).collect()),
        value => value,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::model::AttemptToken;
    use crate::iceberg::NamespaceIdent;
    use crate::iceberg::spec::{SnapshotReference, SnapshotRetention};
    use novarocks_spi::connector::ConnectorWriteOperationId;
    use std::collections::HashMap;

    fn parts(shape: RequestShape) -> FrozenRequestParts {
        let uuid = Uuid::from_u128(1);
        let token = OperationToken::from_write(ConnectorWriteOperationId::from_bytes([1; 16]));
        let mut requirements = vec![TableRequirement::UuidMatch { uuid }];
        if shape == RequestShape::SnapshotProducing {
            requirements.push(TableRequirement::RefSnapshotIdMatch {
                r#ref: "main".into(),
                snapshot_id: Some(10),
            });
        }
        FrozenRequestParts {
            shape,
            target: TableIdent::new(NamespaceIdent::new("db".into()), "table".into()),
            target_ref: "main".into(),
            base: BaseIdentity::Existing {
                uuid,
                parent: Some(10),
                metadata_location: "s3://bucket/metadata/v1.json".into(),
            },
            requirements,
            updates: Vec::new(),
            artifacts: AttemptArtifacts::empty(AttemptToken::new(token, 0)),
        }
    }
    fn set_ref(name: &str, id: i64) -> TableUpdate {
        TableUpdate::SetSnapshotRef {
            ref_name: name.into(),
            reference: SnapshotReference::new(
                id,
                SnapshotRetention::Branch {
                    min_snapshots_to_keep: None,
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                },
            ),
        }
    }

    #[test]
    fn query_methods_preserve_the_final_set_for_the_exact_ref() {
        let empty = FrozenRequest::new(parts(RequestShape::SnapshotProducing)).unwrap();
        assert!(!empty.has_updates());
        assert_eq!(empty.ref_snapshot_after("main"), None);
        let mut input = parts(RequestShape::SnapshotProducing);
        input.updates = vec![
            set_ref("main", 20),
            set_ref("other", 99),
            set_ref("main", 21),
        ];
        let request = FrozenRequest::new(input).unwrap();
        assert!(request.has_updates());
        assert_eq!(request.ref_snapshot_after("main"), Some(21));
        assert_eq!(request.ref_snapshot_after("other"), Some(99));
        assert_eq!(request.ref_snapshot_after("MAIN"), None);
        let expected_updates = request.updates().to_vec();
        let expected_requirements = request.requirements().to_vec();
        let mut commit = request.into_table_commit();
        assert_eq!(commit.take_updates(), expected_updates);
        assert_eq!(commit.take_requirements(), expected_requirements);
    }

    #[test]
    fn rest_envelope_has_stable_golden_encoding_and_round_trips_updates() {
        let mut input = parts(RequestShape::MetadataOnly);
        input.updates.push(TableUpdate::SetProperties {
            updates: HashMap::from([("z".into(), "last".into()), ("a".into(), "first".into())]),
        });
        let request = FrozenRequest::new(input).unwrap();
        let json = request.to_rest_json().unwrap();
        assert_eq!(
            serde_json::to_string(&json).unwrap(),
            r#"{"identifier":{"name":"table","namespace":["db"]},"requirements":[{"type":"assert-table-uuid","uuid":"00000000-0000-0000-0000-000000000001"}],"updates":[{"action":"set-properties","updates":{"a":"first","z":"last"}}]}"#
        );
        assert_eq!(
            serde_json::from_value::<Vec<TableRequirement>>(json["requirements"].clone()).unwrap(),
            request.requirements()
        );
        assert_eq!(
            serde_json::from_value::<Vec<TableUpdate>>(json["updates"].clone()).unwrap(),
            request.updates()
        );
        for _ in 0..8 {
            assert_eq!(request.to_rest_json().unwrap(), json);
        }
        assert_eq!(request.ref_snapshot_after("main"), None);
    }

    #[test]
    fn incomplete_or_contradictory_existing_requests_are_rejected() {
        let mut input = parts(RequestShape::SnapshotProducing);
        input.requirements.pop();
        assert!(FrozenRequest::new(input).is_err());
        let mut input = parts(RequestShape::MetadataOnly);
        input
            .requirements
            .push(TableRequirement::RefSnapshotIdMatch {
                r#ref: "main".into(),
                snapshot_id: Some(10),
            });
        assert!(FrozenRequest::new(input).is_err());
        let mut input = parts(RequestShape::SnapshotProducing);
        input
            .requirements
            .push(TableRequirement::RefSnapshotIdMatch {
                r#ref: "main".into(),
                snapshot_id: Some(11),
            });
        assert!(FrozenRequest::new(input).is_err());
        let mut input = parts(RequestShape::MetadataOnly);
        input.requirements.clear();
        assert!(FrozenRequest::new(input).is_err());
        let mut input = parts(RequestShape::MetadataOnly);
        input.updates.push(set_ref("main", 11));
        assert!(FrozenRequest::new(input).is_err());
        let mut input = parts(RequestShape::MetadataOnly);
        input.updates.push(TableUpdate::AddSnapshot {
            snapshot: crate::iceberg::spec::Snapshot::builder()
                .with_snapshot_id(11)
                .with_sequence_number(2)
                .with_timestamp_ms(1)
                .with_manifest_list("s3://bucket/list.avro")
                .with_summary(crate::iceberg::spec::Summary {
                    operation: crate::iceberg::spec::Operation::Append,
                    additional_properties: HashMap::new(),
                })
                .build(),
        });
        assert!(FrozenRequest::new(input).is_err());
    }
    #[test]
    fn create_request_keeps_authoritative_initialization_and_only_assert_create() {
        use crate::iceberg::spec::{
            FormatVersion, NestedField, PartitionSpec, PrimitiveType, Schema, SortOrder,
            TableMetadataBuilder, Type,
        };
        let initial = TableMetadataBuilder::new(
            Schema::builder()
                .with_fields([Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                ))])
                .build()
                .unwrap(),
            PartitionSpec::unpartition_spec(),
            SortOrder::unsorted_order(),
            "s3://bucket/table".into(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap();
        let mut input = parts(RequestShape::MetadataOnly);
        let operation = input.artifacts.attempt().operation();
        input.shape = RequestShape::Create;
        input.base = BaseIdentity::Create {
            staged: StagedCreateIdentity::new(operation, Arc::new(initial.metadata)),
        };
        input.requirements = vec![TableRequirement::NotExist];
        input.updates = initial.changes;
        let request = FrozenRequest::new(input).unwrap();
        assert_eq!(
            request.to_rest_json().unwrap()["requirements"],
            serde_json::json!([{"type": "assert-create"}])
        );
        assert!(request.has_updates());
        let expected_updates = request.updates().to_vec();
        let mut commit = request.into_table_commit();
        assert_eq!(commit.take_updates(), expected_updates);
        assert_eq!(commit.take_requirements(), [TableRequirement::NotExist]);
    }
}
