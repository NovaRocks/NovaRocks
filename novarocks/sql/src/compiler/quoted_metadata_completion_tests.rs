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

//! Pure catalog completion regressions. The tests supply synthetic catalog
//! observations and resolved table facts at the SQL boundary.

use super::*;
use crate::analyze_error::AnalyzeErrorKind;
use crate::catalog::{SqlViewColumn, SqlViewDefinition};
use crate::compiler::{CatalogQualification, SqlCompileControl};
use crate::planner::table::{SqlScanSource, SqlTableIdentity, SqlTableVersionSelector, TableDef};
use arrow::datatypes::DataType;
use novarocks_spi::connector::{ConnectorProviderBinding, ConnectorProviderId};
use novarocks_type_contract::FunctionValueType;
use novarocks_types::schema::ColumnDef;
use std::sync::Arc;

fn legacy_query(sql: &str) -> ast::Query {
    let statements = novarocks_parser::parse(sql).expect("compatible SQL must parse");
    let [ast::Statement::Query(query)] = statements.as_slice() else {
        panic!("expected one query");
    };
    query.clone()
}

fn instance(name: &str) -> ConnectorInstanceId {
    ConnectorInstanceId::try_from_canonical(name).unwrap()
}

fn ready(provider: &str, catalog: &str, epoch: u8) -> CatalogQualification {
    let provider = ConnectorProviderId::parse(provider).unwrap();
    let binding = ConnectorProviderBinding::try_new(
        provider.clone(),
        catalog,
        [epoch; 16],
        "unit-test-binding",
    )
    .unwrap();
    CatalogQualification::Ready {
        provider,
        binding: binding.binding_key().clone(),
    }
}

fn observations(catalog: &str, observation: CatalogQualification) -> CatalogQualifications {
    let mut qualifications = CatalogQualifications::default();
    qualifications
        .record(instance(catalog), observation)
        .unwrap();
    qualifications
}

fn state(
    query: ast::Query,
    qualifications: CatalogQualifications,
) -> Result<CatalogCompletionState, SqlCompileError> {
    CatalogCompletionState::try_new_with_qualifications(
        query,
        &[],
        &[],
        Some("caller"),
        "caller_db",
        TableLookupMode::SchemaOnly,
        0,
        &crate::sql_mode::SqlSemanticSettings::default(),
        qualifications,
    )
}

fn only_need(state: &CatalogCompletionState) -> &CatalogRelationNeed {
    let [need] = state.needs() else {
        panic!(
            "expected one catalog relation need, got {:?}",
            state.needs()
        );
    };
    need
}

fn assert_metadata_need(
    need: &CatalogRelationNeed,
    identity: TableIdentity,
    kind: SqlMetadataTableKind,
    binding: &ConnectorProviderBindingKey,
) {
    assert_eq!(need.relation(), &identity);
    assert_eq!(need.target(), CatalogLookupTarget::IcebergMetadata { kind });
    assert_eq!(need.quoted_metadata_binding(), Some(binding));
    assert!(!need.allows_local_view_shadow());
    assert!(need.version().is_none());
}

#[test]
fn quoted_metadata_requests_the_exact_base_kind_and_admitted_binding() {
    let control = SqlCompileControl::unbounded();
    for (source, catalog, namespace, base, kind) in [
        (
            r#"SELECT 1 FROM "Orders$FILES""#,
            "caller",
            "caller_db",
            "orders",
            SqlMetadataTableKind::Files,
        ),
        (
            r#"SELECT 1 FROM Sales."Orders$Refs""#,
            "caller",
            "sales",
            "orders",
            SqlMetadataTableKind::Refs,
        ),
        (
            r#"SELECT 1 FROM Ice.Sales."Events$snapshots""#,
            "ice",
            "sales",
            "events",
            SqlMetadataTableKind::Snapshots,
        ),
    ] {
        let observed = ready("iceberg", catalog, 2);
        let CatalogQualification::Ready { binding, .. } = &observed else {
            unreachable!();
        };
        for query in [
            legacy_query(source),
            novarocks_parser::trino_parser::parse_query(source).unwrap(),
        ] {
            assert_eq!(
                quoted_metadata_catalogs(&query, Some("caller"), "caller_db", &control).unwrap(),
                [instance(catalog)],
                "{source}",
            );
            let state = state(query, observations(catalog, observed.clone())).unwrap();
            assert_metadata_need(
                only_need(&state),
                TableIdentity::new(catalog, namespace, base),
                kind,
                binding,
            );
        }
    }
}

#[test]
fn quoted_metadata_does_not_share_the_unquoted_need_or_accept_its_fact() {
    let control = SqlCompileControl::unbounded();
    let observed = ready("iceberg", "ice", 2);
    let CatalogQualification::Ready { binding, .. } = &observed else {
        unreachable!();
    };
    let mut state = state(
        legacy_query(
            r#"SELECT 1 FROM ice.sales.orders$files AS a
                CROSS JOIN ice.sales."orders$files" AS b"#,
        ),
        observations("ice", observed.clone()),
    )
    .unwrap();
    let [legacy, quoted] = state.needs() else {
        panic!("the same base and kind must have two exact needs");
    };
    assert_ne!(legacy.id(), quoted.id());
    assert_eq!(legacy.relation(), quoted.relation());
    assert_eq!(legacy.target(), quoted.target());
    assert!(legacy.quoted_metadata_binding().is_none());
    assert_metadata_need(
        quoted,
        TableIdentity::new("ice", "sales", "orders"),
        SqlMetadataTableKind::Files,
        binding,
    );

    let legacy_fact = CatalogRelationFact::missing(legacy, "legacy failure").unwrap();
    let legacy_retry = CatalogRelationFact::missing(legacy, "legacy failure").unwrap();
    let quoted_fact = CatalogRelationFact::missing(quoted, "quoted failure").unwrap();
    let forged =
        CatalogRelationNeed::try_new(quoted.id(), quoted.relation().clone(), quoted.target())
            .unwrap();
    let wrong_fact = CatalogRelationFact::missing(&forged, "missing without admission").unwrap();
    let error = state
        .accept_facts(vec![legacy_fact, wrong_fact].into(), &control)
        .unwrap_err();
    assert!(
        matches!(error, SqlCompileError::Compilation(message) if message.contains("does not match its exact request"))
    );

    state
        .accept_facts(vec![legacy_retry, quoted_fact].into(), &control)
        .unwrap();
    let catalog = state.completed_catalog().unwrap();
    let error = crate::analyzer::analyze(state.query(), &catalog, "caller_db").unwrap_err();
    assert_eq!(error.kind(), AnalyzeErrorKind::UnknownTable);
    assert_eq!(
        error.message(),
        "catalog relation 'orders' is missing: legacy failure",
    );
    let quoted_query = legacy_query(r#"SELECT 1 FROM ice.sales."orders$files""#);
    let error = crate::analyzer::analyze(&quoted_query, &catalog, "caller_db").unwrap_err();
    assert_eq!(error.kind(), AnalyzeErrorKind::UnknownTable);
    assert_eq!(error.message(), "quoted failure");
}

#[test]
fn observed_initial_absence_and_other_providers_keep_the_original_name_error() {
    let control = SqlCompileControl::unbounded();
    let query = legacy_query(r#"SELECT 1 FROM ice.sales."orders$files""#);
    assert_eq!(
        quoted_metadata_catalogs(&query, Some("caller"), "caller_db", &control).unwrap(),
        [instance("ice")],
    );
    for qualifications in [
        CatalogQualifications::default(),
        observations("ice", CatalogQualification::Absent),
        observations("ice", CatalogQualification::Unavailable),
        observations("ice", ready("paimon", "ice", 2)),
    ] {
        let Err(SqlCompileError::Compilation(message)) = state(query.clone(), qualifications)
        else {
            panic!("expected the original compiler-owned identifier error");
        };
        assert_eq!(message, "unsupported identifier `orders$files`");
    }
}

#[test]
fn a_complete_quoted_cte_name_uses_no_catalog_qualification_or_relation_need() {
    let control = SqlCompileControl::unbounded();
    let source = r#"WITH "orders$files" AS (SELECT 1 AS x) SELECT x FROM "orders$files""#;
    for query in [
        legacy_query(source),
        novarocks_parser::trino_parser::parse_query(source).unwrap(),
    ] {
        assert!(
            quoted_metadata_catalogs(&query, Some("caller"), "caller_db", &control)
                .unwrap()
                .is_empty()
        );
        let state = state(query, observations("caller", ready("iceberg", "caller", 2))).unwrap();
        assert!(state.needs().is_empty());
    }
}

fn saved_view(need: &CatalogRelationNeed) -> Arc<SqlViewDefinition> {
    Arc::new(SqlViewDefinition {
        identity: need.relation().clone(),
        version: "saved-view-version".into(),
        raw_sql: r#"SELECT snapshot_id AS x FROM "orders$snapshots""#.into(),
        default_catalog: "ice".into(),
        default_database: "saved_db".into(),
        columns: vec![SqlViewColumn {
            name: "x".into(),
            value_type: FunctionValueType::new(DataType::Int64, true),
        }]
        .into(),
    })
}

fn snapshots_fact(need: &CatalogRelationNeed) -> CatalogRelationFact {
    let identity = need.relation();
    let table = ResolvedAnalyzerTable::from_planner(
        Some(&identity.catalog),
        &identity.namespace,
        TableDef {
            name: identity.table.clone(),
            columns: vec![ColumnDef {
                name: "snapshot_id".into(),
                data_type: DataType::Int64,
                nullable: true,
                write_default: None,
                logical_type: None,
            }],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::Sql(SqlScanSource::new(
                crate::binding::SqlTableBindingId::new_for_test(60),
                SqlTableIdentity::try_new(
                    identity.catalog.clone(),
                    identity.namespace.clone(),
                    identity.table.clone(),
                )
                .unwrap(),
                SqlScanKind::Metadata {
                    kind: SqlMetadataTableKind::Snapshots,
                    version: SqlTableVersionSelector::Current,
                },
            )),
        },
    );
    CatalogRelationFact::resolved(need, table).unwrap()
}

#[test]
fn saved_view_sidecar_discovers_late_metadata_in_the_exact_saved_context() {
    let control = SqlCompileControl::unbounded();
    let mut state = state(
        legacy_query("SELECT x FROM root_view"),
        CatalogQualifications::default(),
    )
    .unwrap();
    let root = only_need(&state);
    assert_eq!(
        root.relation(),
        &TableIdentity::new("caller", "caller_db", "root_view"),
    );
    let definition = saved_view(root);
    assert_eq!(
        quoted_metadata_catalogs_for_view(&definition, &control).unwrap(),
        [instance("ice")],
    );
    let ready = ready("iceberg", "ice", 2);
    let CatalogQualification::Ready { binding, .. } = &ready else {
        unreachable!();
    };
    let fact = CatalogRelationFact::view(root, definition)
        .unwrap()
        .with_view_qualifications(observations("ice", ready.clone()))
        .unwrap();
    state.accept_facts(vec![fact].into(), &control).unwrap();
    assert!(state.is_discovery_round());
    let late = only_need(&state);
    assert_metadata_need(
        late,
        TableIdentity::new("ice", "saved_db", "orders"),
        SqlMetadataTableKind::Snapshots,
        binding,
    );
    let fact = snapshots_fact(late);
    state.accept_facts(vec![fact].into(), &control).unwrap();
    assert!(state.needs().is_empty());

    let catalog = state.completed_catalog().unwrap();
    let (analyzed, _, _) = crate::analyzer::analyze(state.query(), &catalog, "caller_db").unwrap();
    let [field] = analyzed.output_columns.as_slice() else {
        panic!("one view output");
    };
    assert_eq!(field.field_name.as_deref(), Some("x"));
    assert_eq!(
        field.value_type,
        FunctionValueType::new(DataType::Int64, true)
    );
}

#[test]
fn saved_view_sidecar_refuses_a_wrong_scope_and_a_conflicting_observation() {
    let control = SqlCompileControl::unbounded();
    for sidecar in [
        CatalogQualifications::default(),
        observations("caller", ready("iceberg", "caller", 2)),
    ] {
        let mut state = state(
            legacy_query("SELECT x FROM root_view"),
            CatalogQualifications::default(),
        )
        .unwrap();
        let root = only_need(&state);
        let fact = CatalogRelationFact::view(root, saved_view(root))
            .unwrap()
            .with_view_qualifications(sidecar)
            .unwrap();
        let error = state.accept_facts(vec![fact].into(), &control).unwrap_err();
        assert!(
            matches!(error, SqlCompileError::InvalidRequest(message) if message.contains("do not match the saved view's quoted metadata dependencies"))
        );
    }

    let mut state = state(
        legacy_query(
            r#"SELECT v.x FROM root_view AS v
                CROSS JOIN ice.live_db."root_orders$snapshots" AS s"#,
        ),
        observations("ice", ready("iceberg", "ice", 2)),
    )
    .unwrap();
    let [root, root_metadata] = state.needs() else {
        panic!("the original view and metadata relation precede late discovery");
    };
    assert_eq!(
        root_metadata.relation(),
        &TableIdentity::new("ice", "live_db", "root_orders"),
    );
    let view = CatalogRelationFact::view(root, saved_view(root))
        .unwrap()
        .with_view_qualifications(observations("ice", ready("iceberg", "ice", 3)))
        .unwrap();
    let original = snapshots_fact(root_metadata);
    let error = state
        .accept_facts(vec![view, original].into(), &control)
        .unwrap_err();
    assert!(
        matches!(error, SqlCompileError::InvalidRequest(message) if message.contains("catalog qualification changed during one statement"))
    );
}
