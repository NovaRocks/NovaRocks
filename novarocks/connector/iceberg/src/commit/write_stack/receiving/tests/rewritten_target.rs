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
use crate::commit::conflict_scope::tests::{binding, metadata};
use crate::iceberg::spec::{PrimitiveType, Type};
use novarocks_spi::connector::read_stack::TupleDomain;
use novarocks_spi::connector::{ConnectorProcessLocalPayload, ConnectorProviderBinding};

pub(super) fn owner(instance: &str, incarnation: [u8; 16]) -> ConnectorProviderBindingKey {
    ConnectorProviderBinding::iceberg(instance, incarnation, "receiving")
        .unwrap()
        .binding_key()
        .clone()
}

pub(super) fn sealed(
    metadata: &TableMetadata,
    binding: RewrittenTargetBinding,
    owner: ConnectorProviderBindingKey,
) -> ConnectorRewrittenTargetFact {
    let fact = IcebergRewrittenTargetFact::new(
        binding,
        Arc::clone(metadata.current_schema()),
        &TupleDomain::all(),
        &TupleDomain::all(),
    )
    .unwrap();
    ConnectorProcessLocalPayload::new(owner, Arc::new(fact))
}

#[test]
fn rewritten_target_must_come_from_the_exact_admitted_table_snapshot_and_preparation() {
    let metadata = metadata(Type::Primitive(PrimitiveType::Long), None);
    let original = binding(&metadata);
    let provider = owner("receiving", [2; 16]);
    let target = WriteTargetOrdinal::try_new(0).unwrap();
    let targets = [IcebergSealedWriteTarget::new(
        target,
        IcebergWriteBranch::Data,
    )];
    let mut receiving = IcebergReceiving::with_compact_mode(true, None);
    receiving
        .bind_rewritten_target(provider.clone(), original, &metadata)
        .unwrap();
    let proof = received_write_proof(
        target,
        ConnectorWriterPhysicalContext::new([8; 16], 1, [6; 16], 0, 0),
    );
    let mut production = Some(proof);
    let query = write_query_identity();
    let wrong_facts = [
        None,
        Some(sealed(
            &metadata,
            RewrittenTargetBinding {
                table_uuid: uuid::Uuid::nil(),
                ..original
            },
            provider.clone(),
        )),
        Some(sealed(
            &metadata,
            RewrittenTargetBinding {
                snapshot_id: Some(77),
                ..original
            },
            provider.clone(),
        )),
        Some(sealed(
            &metadata,
            RewrittenTargetBinding {
                preparation_digest: [9; 32],
                ..original
            },
            provider.clone(),
        )),
        Some(sealed(&metadata, original, owner("receiving", [3; 16]))),
        Some(ConnectorProcessLocalPayload::new(
            provider.clone(),
            Arc::new("not a provider fact"),
        )),
    ];
    for wrong in wrong_facts {
        let error = receiving
            .begin_query(query, &[target], &mut production, &targets, wrong.as_ref())
            .unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::Internal);
        assert!(receiving.is_unstarted());
        assert!(production.is_some());
    }
    let good = sealed(&metadata, original, provider);
    receiving
        .begin_query(query, &[target], &mut production, &targets, Some(&good))
        .unwrap();
    assert!(production.is_none());
    assert!(receiving.row_conflict_scope(&metadata).is_err());
    receiving.confirm(query).unwrap();
    assert!(matches!(
        receiving.row_conflict_scope(&metadata).unwrap(),
        Some(ConflictScope::All)
    ));
    assert!(receiving.row_conflict_scope(&metadata).is_err());

    let mut ordinary = IcebergReceiving::default();
    ordinary
        .begin_query(query, &[target], &mut None, &targets, None)
        .unwrap();
    assert!(ordinary.row_conflict_scope(&metadata).unwrap().is_none());
}
