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
use crate::catalog_application::query_bindings::{
    QueryFrozenReadInput, QueryTableBindingAdmission, QueryTableBindingKey, QueryTableBindingStore,
};
use crate::catalog_application::query_materializer::{
    CatalogServiceMaterializer, iceberg_table_binding_loader,
};
use arrow::datatypes::{DataType, Field};
use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorMutationEffectField, ConnectorMutationMatchContract, ConnectorMutationSourceField,
    ConnectorRowMutationIntent, ConnectorRowMutationPreparation, ConnectorRowMutationStrategy,
    ConnectorWriteBaseVersion, ConnectorWriteFieldToken, ConnectorWriteOperationId,
    ConnectorWriteTargetRef,
};
use novarocks_sql::compiler::{
    CatalogRelationOutcome, SqlCapturedCatalogPreparation, SqlCompileControl,
};
use novarocks_sql::planning::catalog::{catalog_table, table_binding_id};

#[tokio::test(flavor = "current_thread")]
async fn mutation_current_sources_reuse_complete_admission_and_current_schema() {
    let resolver = fixture(None, None);
    let request = ConnectorTableRequest {
        table: ConnectorTableIdentity {
            instance_id: resolver.binding.descriptor().instance_id.clone(),
            namespace: "db".into(),
            table: "orders".into(),
        },
        resolution: ConnectorTableResolution::StrictBaseTable,
        context: context(),
    };
    let metadata = resolver.binding.metadata();
    let admitted = crate::connector::schema_creation::load_frontend_source_table_async(
        metadata.as_ref(),
        request.clone(),
    )
    .await
    .unwrap();
    let signed = sign_match_source(&admitted, resolver.binding.incarnation());
    let pin = resolver
        .acquire_current(&request.table.instance_id)
        .unwrap();
    let service = crate::catalog_application::query_catalog::new_query_catalog_service();
    let bindings = Arc::new(QueryTableBindingStore::try_new().unwrap());
    bindings
        .register_row_mutation_current(&admitted, &pin, &signed)
        .unwrap();
    let materializer = CatalogServiceMaterializer::new(
        Some("ice"),
        &service,
        bindings.clone(),
        iceberg_table_binding_loader(&resolver, context()),
    );
    let key = QueryTableBindingKey::strict_base("ice", "db", "orders");
    let id = bindings.resolved_lookup(&key).unwrap().unwrap();

    // UPDATE's assigned scalar subquery and MERGE's joined source see the
    // same Current binding even though the signed mutation ref is older.
    for sql in [
        "SELECT (SELECT max(added) FROM ice.db.orders) FROM ice.db.orders",
        "SELECT src.added FROM ice.db.orders src JOIN ice.db.orders dst ON src.x = dst.x",
    ] {
        let query = novarocks_parser::parser::parse_query(sql).unwrap();
        let facts = SqlCapturedCatalogPreparation::try_new(
            query,
            Some("ice"),
            "db",
            SqlCompileControl::unbounded(),
        )
        .unwrap();
        assert!(!facts.needs().is_empty());
        for need in facts.needs() {
            let resolved = materializer
                .resolve_relation_fact_async(need)
                .await
                .unwrap();
            let CatalogRelationOutcome::Resolved(table) = resolved.outcome() else {
                panic!("same-table Current occurrence lost its original binding");
            };
            assert_eq!(table_binding_id(table), id);
            assert!(
                catalog_table(table)
                    .columns
                    .iter()
                    .any(|field| field.name == "added")
            );
        }
    }
    let observed = bindings.binding(id).unwrap();
    let QueryTableBindingAdmission::RowMutationCurrent(_, source) = &observed.admission else {
        panic!("ordinary source lost its admitted mutation ref");
    };
    assert_eq!(source.snapshot_id(), Some(10));
    assert_eq!(source.target_ref().as_str(), "branch");
    assert_eq!(
        bindings
            .frozen_read_input(id, QueryFrozenReadInput::Current)
            .unwrap()
            .table,
        admitted.table,
    );
    assert_eq!(resolver.counts.source_prepare.load(Ordering::Relaxed), 1);
    assert_eq!(resolver.counts.table_prepare.load(Ordering::Relaxed), 0);
    assert_eq!(resolver.counts.table_load.load(Ordering::Relaxed), 0);

    // Equality DELETE keeps the ordinary strict table preparation.
    let equality =
        crate::connector::schema_creation::load_frontend_table_async(metadata.as_ref(), request)
            .await
            .unwrap();
    assert!(equality.source_read_facts().is_none());
    assert_eq!(resolver.counts.source_prepare.load(Ordering::Relaxed), 1);
    assert_eq!(resolver.counts.table_prepare.load(Ordering::Relaxed), 1);
}

fn sign_match_source(
    admitted: &ConnectorTableMetadata,
    epoch: ProviderBindingEpoch,
) -> ConnectorRowMutationPreparation {
    let owner = ConnectorProviderBindingKey {
        instance_id: admitted.identity.instance_id.clone(),
        incarnation: epoch,
    };
    let base = ConnectorWriteBaseVersion::try_new(Bytes::from_static(b"base")).unwrap();
    let contract = ConnectorMutationMatchContract::try_new(
        owner.clone(),
        admitted.table.clone(),
        base.clone(),
        vec![
            ConnectorMutationSourceField::try_new_typed(
                ConnectorWriteFieldToken::from_bytes([1; 32]),
                Field::new("x", DataType::Int64, false),
                0,
                FunctionValueType::new(DataType::Int64, false),
            )
            .unwrap(),
        ],
        Vec::new(),
        Vec::new(),
        ConnectorMutationEffectField::try_new(
            ConnectorWriteFieldToken::from_bytes([2; 32]),
            Field::new("effect", DataType::Int8, false),
            1,
        )
        .unwrap(),
    )
    .unwrap();
    ConnectorRowMutationPreparation::try_new(
        owner,
        ConnectorWriteOperationId::from_bytes([3; 16]),
        admitted.table.clone(),
        admitted.table.clone(),
        Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
            "x",
            DataType::Int64,
            false,
        )])),
        ConnectorWriteTargetRef::parse("branch").unwrap(),
        ConnectorRowMutationIntent::Delete,
        base,
        contract,
        ConnectorRowMutationStrategy::MergeOnRead,
        Some(10),
        Bytes::from_static(b"test preparation"),
    )
    .unwrap()
}
