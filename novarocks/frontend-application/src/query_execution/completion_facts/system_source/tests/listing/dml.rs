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

//! INSERT and CTAS retain their actual DmlWriteCompletion owner. A test-owned
//! target declaration freezes the producer and sink contract; no external
//! writer is opened or committed by this component test. Native SQL suites
//! own actual publication and counted command completion.
use super::*;
use novarocks_connector_contract::{ConnectorWriteBinding, ConnectorWriteRecipeDraft};
use novarocks_query_application::preparation::CatalogFactPort;
use novarocks_spi::connector::{
    CatalogHandle, CatalogVersion, ConnectorCodecCategory, ConnectorCodecRevision,
    ConnectorEncodedPayload, ConnectorEnvelopeHeader, ConnectorInstanceDescriptor,
    ConnectorInstanceId, ConnectorProviderId, ConnectorWriteFieldBinding, ConnectorWriteFieldToken,
    ConnectorWriteInputShape,
};
use novarocks_sql::{compiler::*, planning::dml::*};
use std::collections::BTreeMap;
fn columns() -> Vec<novarocks_types::schema::ColumnDef> {
    ["table_name", "table_type"]
        .into_iter()
        .map(|name| novarocks_types::schema::ColumnDef {
            name: name.into(),
            data_type: arrow::datatypes::DataType::Utf8,
            nullable: false,
            write_default: None,
            logical_type: None,
        })
        .collect()
}
fn token(ordinal: usize) -> ConnectorWriteFieldToken {
    ConnectorWriteFieldToken::from_bytes([u8::try_from(ordinal + 1).unwrap(); 32])
}
fn sink() -> DmlWritePlanInput {
    let mut bindings = novarocks_sql::binding::SqlTableBindingAllocator::new_unique().unwrap();
    let columns = columns();
    DmlWritePlanInput::try_new(
        DmlWriteSinkMode::Data,
        DmlWriteTarget {
            binding: bindings.allocate().unwrap(),
            catalog: "snapshot_target".into(),
            namespace: "db".into(),
            table: "target".into(),
            fields: columns
                .iter()
                .enumerate()
                .map(|(index, column)| DmlWriteTargetField {
                    token: token(index),
                    column: column.clone(),
                    is_hidden: false,
                })
                .collect(),
        },
        columns,
        ConnectorWriteInputBinding::RootOutputByOrdinal,
    )
    .unwrap()
}
fn target() -> (ConnectorEncodedPayload, ConnectorWriteRecipeDraft) {
    let provider = ConnectorProviderId::parse("snapshot_test").unwrap();
    let instance = ConnectorInstanceId::parse("snapshot_target").unwrap();
    let catalog = CatalogHandle::new(instance.clone(), CatalogVersion::from_bytes([53; 32]));
    let binding = ConnectorWriteBinding::new(
        ConnectorInstanceDescriptor {
            provider_id: provider.clone(),
            instance_id: instance,
        },
        catalog.clone(),
    );
    let handle = ConnectorEncodedPayload::new(
        ConnectorEnvelopeHeader::new(
            provider,
            catalog,
            ConnectorCodecCategory::WriteHandle,
            ConnectorCodecRevision::try_new(1).unwrap(),
        ),
        bytes::Bytes::from_static(b"owned-system-snapshot-test-target"),
    );
    let recipe = ConnectorWriteRecipeDraft::try_new(
        binding,
        handle.clone(),
        ConnectorWriteInputShape::Data {
            fields: columns()
                .iter()
                .enumerate()
                .map(|(index, column)| {
                    ConnectorWriteFieldBinding::try_new_typed(
                        token(index),
                        arrow::datatypes::Field::new(
                            &column.name,
                            column.data_type.clone(),
                            column.nullable,
                        ),
                        novarocks_type_contract::FunctionValueType::new(
                            column.data_type.clone(),
                            column.nullable,
                        ),
                    )
                    .unwrap()
                })
                .collect(),
        },
    )
    .unwrap();
    (handle, recipe)
}
#[tokio::test]
async fn original_insert_and_ctas_completion_install_system_snapshots_before_writer_lowering() {
    for ctas in [false, true] {
        let provider = ListingFixture::new(FailurePoint::Views, ConnectorErrorKind::Unavailable);
        let mut fixture = Fixture::new(&provider, ResultWindowClass::Internal).await;
        let source = fixture.source();
        let catalog = FrontendCatalogFacts::new(source.owners.clone(), source.scope.clone());
        let control = source.scope.planning.as_ref().unwrap().control.clone();
        let functions = build_builtin_engine_function_catalog().unwrap();
        let policy = crate::application::test_constant_policy();
        let settings = SessionOptimizerSettings {
            enable_materialized_view_rewrite: Some(false),
            ..Default::default()
        };
        let semantics = novarocks_sql::sql_mode::SqlSemanticSettings::default();
        let decimal = semantics.sql_mode().decimal_overflow_policy();
        let [novarocks_parser::ast::Statement::Query(query)]: [_; 1] = novarocks_parser::parse(
            "SELECT table_name, table_type FROM system.information_schema.tables",
        )
        .unwrap()
        .try_into()
        .unwrap() else {
            panic!("producer query")
        };
        let mut preparation = SqlDmlCatalogPreparation::try_new(
            SqlStatementInput::parsed_query(Box::new(query)),
            SqlSessionContext {
                current_catalog: Some("system".into()),
                current_database: "metadata".into(),
                sql_semantics: semantics,
                optimizer_settings: settings.clone(),
            },
            control.clone(),
            RootDistributionRequirement::Any,
        )
        .unwrap();
        while !preparation.needs().is_empty() {
            let facts = catalog
                .resolve_relations(preparation.needs())
                .await
                .unwrap();
            preparation = preparation
                .continue_catalog(facts.into_boxed_slice())
                .unwrap();
        }
        let analyzed = preparation
            .analyze(
                &[],
                &functions,
                crate::query_execution::constant_eval::constant_evaluator(),
                policy,
            )
            .unwrap();
        let statistics = DmlStatisticsSnapshot::empty();
        let optimize = SqlOptimizeRequest::new(analyzed, &statistics, control.clone());
        let ordinal =
            novarocks_spi::connector::write_stack::WriteTargetOrdinal::try_new(0).unwrap();
        let (mut completion, reads) = if ctas {
            let source = compile_ctas_source(optimize).unwrap();
            begin_final_ctas_connector_write_plan(
                &source,
                sink(),
                ordinal,
                &[],
                &settings,
                decimal,
                &control,
            )
            .unwrap()
        } else {
            begin_final_connector_write_plan(optimize, sink(), ordinal, &[], &settings, decimal)
                .unwrap()
        };
        assert!(
            reads.is_empty(),
            "a system producer never acquires external scan access"
        );
        assert_eq!(completion.system_table_needs().len(), 1);
        let facts = tokio::time::timeout(
            Duration::from_secs(5),
            driven(
                &fixture.control,
                &mut fixture.driver,
                source.resolve_system_tables(completion.system_table_needs()),
            ),
        )
        .await
        .unwrap()
        .unwrap();
        completion
            .install_system_tables(facts.into_boxed_slice(), &control)
            .unwrap();
        let (handle, recipe) = target();
        let authored = completion
            .finish(
                novarocks_physical_plan::PlanVersionId::try_new([53; 16]).unwrap(),
                novarocks_physical_plan::PipelineDopDomain {
                    min: 1,
                    max: 8,
                    requires_power_of_two: true,
                },
                DmlFinalizedProviderReadSet::empty(),
                DmlFinalizedWriteTargetSet::try_new([DmlFinalizedWriteTarget { ordinal, handle }])
                    .unwrap(),
                &control,
            )
            .unwrap();
        let candidate = novarocks_query_application::preparation::CompletedPhysicalPlanCandidate::for_sql_program(authored, &control).and_then(|candidate| candidate.freeze_root_output(novarocks_result_contract::FrozenRootOutput::InternalFacts(novarocks_result_contract::InternalResultDomain::PreparedWriteCommitV1))).unwrap();
        let batches = candidate
            .plan()
            .fragments()
            .values()
            .flat_map(|fragment| fragment.nodes().values())
            .filter_map(|node| match &node.kind {
                novarocks_physical_plan::NodeKind::SystemRelation {
                    data: novarocks_physical_plan::SystemRelationData::Batches { batches, .. },
                    ..
                } => Some(batches),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(batches.len(), 1);
        assert_eq!(
            batches[0]
                .iter()
                .map(|batch| batch.num_rows())
                .sum::<usize>(),
            1
        );
        assert!(
            candidate
                .plan()
                .fragments()
                .values()
                .flat_map(|fragment| fragment.nodes().values())
                .any(|node| matches!(
                    node.kind,
                    novarocks_physical_plan::NodeKind::TableWriter { .. }
                ))
        );
        let packages = crate::query_execution::package_freeze::extract_checked_packages(
            &candidate,
            &BTreeMap::new(),
            &BTreeMap::from([(ordinal, recipe)]),
            policy,
            &novarocks_plan_codec::physical_package_v2::FROZEN_PACKAGE_ADMISSION,
            &control,
        )
        .unwrap();
        assert_eq!(packages.len(), candidate.plan().fragments().len());
        assert!(provider.calls().is_empty());
        drop(packages);
        drop(batches);
        drop(candidate);
        drop(catalog);
        drop(source);
        fixture.finish(false).await;
    }
}

#[tokio::test]
async fn mutation_purpose_refuses_direct_and_cte_system_sources_in_the_catalog_round() {
    for sql in [
        "SELECT table_name FROM system.information_schema.tables",
        "WITH names AS (SELECT table_name FROM system.information_schema.tables) SELECT table_name FROM names",
    ] {
        let provider = ListingFixture::new(FailurePoint::Views, ConnectorErrorKind::Unavailable);
        let fixture = Fixture::new(&provider, ResultWindowClass::Internal).await;
        let source = fixture.source();
        let scope = source
            .scope
            .clone()
            .with_system_table_source_policy(SystemTableSourcePolicy::Mutation);
        let catalog = FrontendCatalogFacts::new(source.owners.clone(), scope);
        let [novarocks_parser::ast::Statement::Query(query)]: [_; 1] =
            novarocks_parser::parse(sql).unwrap().try_into().unwrap()
        else {
            panic!("producer query")
        };
        let preparation = SqlDmlCatalogPreparation::try_new(
            SqlStatementInput::parsed_query(Box::new(query)),
            SqlSessionContext {
                current_catalog: Some("system".into()),
                current_database: "metadata".into(),
                sql_semantics: Default::default(),
                optimizer_settings: Default::default(),
            },
            source.scope.planning.as_ref().unwrap().control.clone(),
            RootDistributionRequirement::Any,
        )
        .unwrap();
        assert!(!preparation.needs().is_empty());
        let error = catalog
            .resolve_relations(preparation.needs())
            .await
            .unwrap_err();
        let SqlCompileError::CompilationSource(cause) = error else {
            panic!("mutation source refusal must remain typed")
        };
        assert_eq!(
            cause.failure_fact().class(),
            novarocks_failure_contract::FailureClass::Unsupported
        );
        assert!(provider.calls().is_empty());
        assert!(
            source
                .scope
                .planning
                .as_ref()
                .unwrap()
                .cpu
                .as_ref()
                .unwrap()
                .system_snapshot_field_for_test()
                .is_none(),
            "refused sources never enter snapshot construction"
        );
        drop(catalog);
        drop(source);
        fixture.finish(false).await;
    }
}
