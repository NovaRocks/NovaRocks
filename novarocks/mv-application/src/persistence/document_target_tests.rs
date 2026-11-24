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

use super::*;
use crate::management::{DeploymentOwner, ProcessIncarnation};
use crate::persistence::{
    keyed_documents::keyed_create_document_set,
    test_support::{MvDocumentFixture, fixture_type},
};
use bytes::Bytes;
use novarocks_spi::connector::*;
use std::collections::BTreeMap;

fn field(ordinal: u32, id: Bytes) -> ConnectorPreparedCreateFieldBinding {
    ConnectorPreparedCreateFieldBinding::try_new(
        ordinal,
        id,
        format!("field_{ordinal}"),
        fixture_type("binary"),
        false,
    )
    .unwrap()
}
fn fixture() -> (MvDocumentFixture, ConnectorPreparedCreateDocumentTarget) {
    let f = MvDocumentFixture::new();
    let instance = ConnectorInstanceId::parse("ice").unwrap();
    let owner = ConnectorProviderBindingKey {
        instance_id: instance.clone(),
        incarnation: ProviderBindingEpoch::from_bytes([1; 16]),
    };
    let mut unique = BTreeMap::new();
    for binding in &f.interpretation.target.fields {
        unique
            .entry(binding.target_field_id.clone())
            .or_insert(binding);
    }
    let fields = unique
        .into_values()
        .enumerate()
        .map(|(i, value)| {
            ConnectorPreparedCreateFieldBinding::try_new(
                i as u32,
                Bytes::copy_from_slice(value.target_field_id.as_bytes()),
                format!("field_{i}"),
                value.source_type.clone(),
                value.nullable,
            )
            .unwrap()
        })
        .collect();
    let target = ConnectorPreparedCreateDocumentTarget::try_new(
        owner,
        CatalogHandle::new(instance.clone(), CatalogVersion::from_bytes([2; 32])),
        ConnectorStagedCreateOperationId::new(),
        ConnectorTableIdentity {
            instance_id: instance,
            namespace: "sales".into(),
            table: "mv".into(),
        },
        f.object_id.clone(),
        Bytes::copy_from_slice(f.interpretation.target.schema_version.as_bytes()),
        Bytes::copy_from_slice(f.interpretation.target.partition_spec_version.as_bytes()),
        fields,
        vec![],
        Bytes::from_static(b"prepared-target"),
    )
    .unwrap();
    (f, target)
}
fn with_fields(
    target: &ConnectorPreparedCreateDocumentTarget,
    fields: Vec<ConnectorPreparedCreateFieldBinding>,
) -> Result<ConnectorPreparedCreateDocumentTarget, ConnectorError> {
    ConnectorPreparedCreateDocumentTarget::try_new(
        target.owner().clone(),
        target.catalog_handle().clone(),
        target.operation_id(),
        target.target().clone(),
        target.object_id().clone(),
        target.schema_version().clone(),
        target.partition_spec_version().clone(),
        fields,
        target.partition_fields().to_vec(),
        target.provider_token().clone(),
    )
}
fn create(
    f: &MvDocumentFixture,
    target: &ConnectorPreparedCreateDocumentTarget,
) -> Result<KeyedCreate, MvDocumentError> {
    keyed_create_document_set(
        &f.definition,
        &f.interpretation,
        &f.configuration,
        f.binding.clone(),
        f.mapping.clone(),
        target,
        &DeploymentOwner::parse("owner").unwrap(),
        &ProcessIncarnation::parse("process").unwrap(),
    )
}
#[test]
fn keyed_create_preserves_exact_prepared_versions_and_full_definition_identity() {
    let (f, target) = fixture();
    let documents = create(&f, &target).unwrap();
    assert_eq!(documents.versions.len(), 4);
    assert!(
        documents
            .versions
            .iter()
            .any(|v| v.identity().to_bytes().as_slice()
                == f.interpretation.definition_revision.as_bytes())
    );
    for variant in 0..3 {
        let mut wrong = MvDocumentFixture::new();
        match variant {
            0 => {
                wrong.interpretation.target.object_id =
                    crate::persistence::identity::ObjectIdentity::try_new(vec![99]).unwrap()
            }
            1 => {
                wrong.interpretation.target.schema_version =
                    crate::persistence::identity::SchemaVersion::try_new(vec![99]).unwrap()
            }
            _ => {
                wrong.interpretation.target.partition_spec_version =
                    crate::persistence::identity::PartitionSpecVersion::try_new(vec![99]).unwrap()
            }
        }
        assert!(create(&wrong, &target).is_err());
    }
}
#[test]
fn keyed_create_binds_by_identity_in_the_providers_own_column_order() {
    let (f, target) = fixture();
    let reversed = target
        .fields()
        .iter()
        .rev()
        .enumerate()
        .map(|(i, v)| {
            ConnectorPreparedCreateFieldBinding::try_new(
                i as u32,
                v.provider_field_id().clone(),
                v.name().to_string(),
                v.source_type().clone(),
                v.nullable(),
            )
            .unwrap()
        })
        .collect();
    create(&f, &with_fields(&target, reversed).unwrap()).unwrap();
}
#[test]
fn keyed_create_allows_branches_to_share_one_prepared_physical_column() {
    let (f, target) = fixture();
    assert_eq!(f.interpretation.branches.len(), 2);
    assert!(f.interpretation.target.fields.len() > target.fields().len());
    create(&f, &target).unwrap();
}
#[test]
fn keyed_create_refuses_missing_extra_or_unprepared_physical_columns() {
    let (f, target) = fixture();
    let mut missing = target.fields().to_vec();
    missing.pop();
    assert!(create(&f, &with_fields(&target, missing).unwrap()).is_err());
    let mut extra = target.fields().to_vec();
    extra.push(field(extra.len() as u32, Bytes::from_static(b"extra")));
    assert!(create(&f, &with_fields(&target, extra).unwrap()).is_err());
    let mut unknown = target.fields().to_vec();
    unknown[0] = field(0, Bytes::from_static(b"unprepared"));
    assert!(create(&f, &with_fields(&target, unknown).unwrap()).is_err());
}
#[test]
fn keyed_create_refuses_interpretation_field_bindings_that_disagree_with_logical_outputs() {
    let (mut f, target) = fixture();
    let first = f.interpretation.target.fields[0].target_field_id.clone();
    f.interpretation.target.fields[0].target_field_id =
        f.interpretation.target.fields[1].target_field_id.clone();
    f.interpretation.target.fields[1].target_field_id = first;
    assert!(create(&f, &target).is_err());
}
#[test]
fn prepared_target_refuses_duplicate_and_non_dense_request_ordinals() {
    let (_, target) = fixture();
    assert!(
        with_fields(
            &target,
            vec![
                field(0, target.fields()[0].provider_field_id().clone()),
                field(0, target.fields()[1].provider_field_id().clone())
            ]
        )
        .is_err()
    );
    assert!(
        with_fields(
            &target,
            vec![
                field(0, target.fields()[0].provider_field_id().clone()),
                field(2, target.fields()[1].provider_field_id().clone())
            ]
        )
        .is_err()
    );
}
