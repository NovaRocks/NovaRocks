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

//! Pure SQL-owner regressions for the declaration/snapshot seam. The independent
//! oracle is the frozen declaration and exact completion protocol. No external
//! fixture, process or cleanup is required; package tests consume this coverage.
use super::tests::{answer_statistics, incomplete, provider_contract, request, resolved_table};
use super::*;
use crate::compiler::{
    CatalogRelationNeed, SqlCompilation, SqlCompileProgress, SqlCompletedPlan, SqlFactBatch,
    SystemTableSnapshotFact, SystemTableSnapshotNeed,
};
use crate::system_table::{
    SystemTableBinding, SystemTableColumn, SystemTableDeclaration, SystemTableLookupError,
};
use novarocks_physical_plan::{NodeKind, SystemRelationData};
use novarocks_type_contract::{FunctionValueType, ValueTypeStorageHold, ValueTypeStorageLifetime};

fn declaration(need: &CatalogRelationNeed) -> Arc<SystemTableDeclaration> {
    Arc::new(SystemTableDeclaration {
        binding: if need.relation().catalog == "system" {
            SystemTableBinding::System
        } else {
            SystemTableBinding::Catalog(need.relation().catalog.clone())
        },
        schema: need.relation().namespace.clone(),
        table: need.relation().table.clone(),
        columns: ["schema_name", "payload"]
            .into_iter()
            .map(|name| SystemTableColumn {
                name: name.into(),
                value_type: FunctionValueType::new(DataType::Utf8, false),
            })
            .collect::<Vec<_>>()
            .into(),
        key_columns: vec![0].into(),
    })
}
fn start(sql: &str, intent: SqlCompileIntent) -> SqlCompilation {
    incomplete(
        SqlCompiler::start(
            request(sql, intent).try_into_completion().unwrap(),
            &SqlCompileControl::unbounded(),
        )
        .unwrap(),
    )
}
fn answer_declarations(compilation: SqlCompilation) -> SqlCompileProgress {
    let SqlNeedBatch::CatalogRelations(needs) = compilation.needs() else {
        panic!("declaration round required");
    };
    let facts = needs
        .iter()
        .map(|need| {
            if need.target() == super::super::completion::CatalogLookupTarget::SystemTable {
                CatalogRelationFact::system_table(need, Ok(declaration(need))).unwrap()
            } else {
                CatalogRelationFact::resolved(need, resolved_table(need)).unwrap()
            }
        })
        .collect::<Vec<_>>();
    SqlCompiler::finish(
        compilation,
        SqlFactBatch::CatalogRelations(facts.into()),
        &SqlCompileControl::unbounded(),
    )
    .unwrap()
}
fn snapshot_need(sql: &str) -> SqlCompilation {
    let mut progress = answer_declarations(start(sql, SqlCompileIntent::Query));
    loop {
        let compilation = incomplete(progress);
        match compilation.needs() {
            SqlNeedBatch::Statistics(_) => progress = answer_statistics(compilation),
            SqlNeedBatch::ProviderInputs { .. } => return compilation,
            other => panic!("unexpected round {other:?}"),
        }
    }
}
struct Hold;
impl ValueTypeStorageHold for Hold {}
fn fact(need: &SystemTableSnapshotNeed, rows: usize) -> SystemTableSnapshotFact {
    use arrow::array::{ArrayRef, StringArray};
    let fields = need
        .required_columns()
        .iter()
        .map(|ordinal| {
            let column = &need.declaration().columns[*ordinal];
            Arc::new(column.value_type.try_to_field(&column.name).unwrap())
        })
        .collect::<Vec<_>>();
    let arrays = fields
        .iter()
        .map(|_| Arc::new(StringArray::from(vec!["Db"; rows])) as ArrayRef)
        .collect();
    let batch = arrow::record_batch::RecordBatch::try_new_with_options(
        Arc::new(arrow::datatypes::Schema::new(fields)),
        arrays,
        &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(rows)),
    )
    .unwrap();
    SystemTableSnapshotFact::try_new(
        need,
        vec![batch].into(),
        ValueTypeStorageLifetime::held(Arc::new(Hold)),
    )
    .unwrap()
}
fn finish(compilation: SqlCompilation) -> SqlCompletedPlan {
    let SqlNeedBatch::ProviderInputs {
        reads,
        system_tables,
    } = compilation.needs()
    else {
        panic!("compound provider round required");
    };
    let reads = reads
        .iter()
        .map(|need| ProviderReadFact::negotiated(need, provider_contract(need)).unwrap())
        .collect::<Vec<_>>();
    let system_tables = system_tables
        .iter()
        .map(|need| fact(need, 3))
        .collect::<Vec<_>>();
    SqlCompiler::finish(
        compilation,
        SqlFactBatch::ProviderInputs {
            reads: reads.into(),
            system_tables: system_tables.into(),
        },
        &SqlCompileControl::unbounded(),
    )
    .unwrap()
    .into_complete()
    .unwrap()
}

#[test]
fn declaration_then_zero_column_snapshot_preserves_rows_and_original_storage() {
    let catalog = start(
        "SELECT count(*) FROM system.metadata.fixture",
        SqlCompileIntent::Query,
    );
    let SqlNeedBatch::CatalogRelations(needs) = catalog.needs() else {
        panic!();
    };
    assert_eq!(needs.len(), 1);
    assert_eq!(
        needs[0].target(),
        super::super::completion::CatalogLookupTarget::SystemTable
    );
    let provider = incomplete(answer_declarations(catalog));
    let SqlNeedBatch::ProviderInputs {
        reads,
        system_tables,
    } = provider.needs()
    else {
        panic!();
    };
    assert!(reads.is_empty());
    assert_eq!(system_tables.len(), 1);
    assert!(system_tables[0].required_columns().is_empty());
    let completed = finish(provider);
    let data = completed
        .plan()
        .fragments()
        .values()
        .flat_map(|fragment| fragment.nodes().values())
        .find_map(|node| {
            if let NodeKind::SystemRelation {
                output_fields,
                data,
            } = &node.kind
            {
                assert!(output_fields.is_empty());
                Some(data)
            } else {
                None
            }
        })
        .unwrap();
    assert_eq!(data.resource_facts().unwrap().rows, 3);
    assert!(matches!(data, SystemRelationData::Batches { storage, .. } if !storage.is_pure()));
    assert!(
        completed
            .plan()
            .annotations()
            .iter()
            .any(|annotation| annotation.key.as_ref() == "sql.relation"
                && annotation.value.as_ref() == "system.metadata.fixture")
    );
}

#[test]
fn plain_explain_never_requests_snapshot_but_analyze_does() {
    let sql = "SELECT payload FROM system.metadata.fixture";
    let plain = answer_declarations(start(
        sql,
        SqlCompileIntent::Explain {
            level: crate::explain::ExplainLevel::Verbose,
            analyze: false,
        },
    ))
    .into_complete()
    .unwrap();
    assert!(
        plain
            .plan()
            .fragments()
            .values()
            .flat_map(|fragment| fragment.nodes().values())
            .any(|node| matches!(
                &node.kind,
                NodeKind::SystemRelation {
                    data: SystemRelationData::Deferred,
                    ..
                }
            ))
    );
    let analyze = incomplete(answer_declarations(start(
        sql,
        SqlCompileIntent::Explain {
            level: crate::explain::ExplainLevel::Verbose,
            analyze: true,
        },
    )));
    assert!(
        matches!(analyze.needs(), SqlNeedBatch::ProviderInputs { system_tables, .. } if system_tables.len() == 1)
    );
    let _ = finish(analyze);
}

#[test]
fn conjunctive_key_constraint_is_exact_and_engine_filter_remains() {
    let provider = snapshot_need(
        "SELECT payload FROM system.metadata.fixture WHERE schema_name IN ('Db', 'other') AND schema_name = 'Db' AND payload = 'unrelated'",
    );
    let SqlNeedBatch::ProviderInputs { system_tables, .. } = provider.needs() else {
        panic!();
    };
    let constraint = system_tables[0].key_constraint();
    assert!(constraint.allows(0, "Db"));
    assert!(!constraint.allows(0, "db"));
    assert!(!constraint.allows(0, "other"));
    assert_eq!(constraint.columns().len(), 1);
    let completed = finish(provider);
    assert!(
        completed
            .plan()
            .fragments()
            .values()
            .flat_map(|fragment| fragment.nodes().values())
            .any(|node| matches!(node.kind, NodeKind::Filter { .. }))
    );
}

#[test]
fn disjunction_and_nonkey_filters_do_not_prune_provider_rows() {
    for predicate in [
        "schema_name = 'a' OR schema_name = 'b'",
        "payload = 'a'",
        "schema_name > 'a'",
    ] {
        let provider = snapshot_need(&format!(
            "SELECT payload FROM system.metadata.fixture WHERE {predicate}"
        ));
        let SqlNeedBatch::ProviderInputs { system_tables, .. } = provider.needs() else {
            panic!();
        };
        assert!(
            system_tables[0].key_constraint().columns().is_empty(),
            "{predicate}"
        );
    }
}

#[test]
fn every_repeated_reference_has_its_own_occurrence_even_with_empty_projection() {
    let provider = snapshot_need(
        "SELECT count(*) FROM system.metadata.fixture a CROSS JOIN system.metadata.fixture b",
    );
    let SqlNeedBatch::ProviderInputs { system_tables, .. } = provider.needs() else {
        panic!();
    };
    assert_eq!(system_tables.len(), 2);
    assert_ne!(system_tables[0].id(), system_tables[1].id());
    assert_ne!(system_tables[0].occurrence(), system_tables[1].occurrence());
    assert!(
        system_tables
            .iter()
            .all(|need| need.required_columns().is_empty())
    );
    let _ = finish(provider);
}

#[test]
fn ordinary_scan_and_system_snapshot_share_one_exact_provider_round() {
    let provider =
        snapshot_need("SELECT s.schema_name FROM system.metadata.fixture s CROSS JOIN orders o");
    let SqlNeedBatch::ProviderInputs {
        reads,
        system_tables,
    } = provider.needs()
    else {
        panic!();
    };
    assert_eq!((reads.len(), system_tables.len()), (1, 1));
    assert_ne!(reads[0].id(), system_tables[0].id());
    let _ = finish(provider);
}

#[test]
fn declaration_lookup_preserves_catalog_schema_and_table_error_categories() {
    use crate::analyze_error::AnalyzeErrorKind;
    for (failure, expected) in [
        (
            SystemTableLookupError::CatalogNotFound,
            AnalyzeErrorKind::UnknownCatalog,
        ),
        (
            SystemTableLookupError::SchemaNotFound,
            AnalyzeErrorKind::UnknownSchema,
        ),
        (
            SystemTableLookupError::TableNotFound,
            AnalyzeErrorKind::UnknownTable,
        ),
    ] {
        let sql = match failure {
            SystemTableLookupError::CatalogNotFound => {
                "SELECT * FROM absent.information_schema.fixture"
            }
            SystemTableLookupError::SchemaNotFound => "SELECT * FROM system.absent.fixture",
            SystemTableLookupError::TableNotFound => "SELECT * FROM system.metadata.absent",
        };
        let catalog = start(sql, SqlCompileIntent::Query);
        let SqlNeedBatch::CatalogRelations(needs) = catalog.needs() else {
            panic!();
        };
        let facts = SqlFactBatch::CatalogRelations(
            vec![CatalogRelationFact::system_table(&needs[0], Err(failure)).unwrap()].into(),
        );
        let error = SqlCompiler::finish(catalog, facts, &SqlCompileControl::unbounded())
            .err()
            .expect("lookup must fail");
        let crate::compiler::SqlCompileProgressError::Compile(SqlCompileError::Analyze(error)) =
            error
        else {
            panic!("expected typed analysis failure: {error:?}");
        };
        assert_eq!(error.kind(), expected);
    }
}

#[test]
fn system_names_follow_the_session_catalog_after_cte_resolution() {
    for (sql, catalog, database, expected) in [
        (
            "SELECT payload FROM system.metadata.fixture",
            "iceberg",
            "db",
            "system.metadata.fixture",
        ),
        (
            "SELECT payload FROM information_schema.fixture",
            "iceberg",
            "db",
            "iceberg.information_schema.fixture",
        ),
        (
            "SELECT payload FROM metadata.fixture",
            "system",
            "db",
            "system.metadata.fixture",
        ),
        (
            "SELECT payload FROM fixture",
            "system",
            "metadata",
            "system.metadata.fixture",
        ),
        (
            "SELECT payload FROM system.information_schema.fixture",
            "iceberg",
            "db",
            "system.information_schema.fixture",
        ),
    ] {
        let mut input = request(sql, SqlCompileIntent::Query);
        input.session.current_catalog = Some(catalog.into());
        input.session.current_database = database.into();
        let catalog = incomplete(
            SqlCompiler::start(
                input.try_into_completion().unwrap(),
                &SqlCompileControl::unbounded(),
            )
            .unwrap(),
        );
        let SqlNeedBatch::CatalogRelations(needs) = catalog.needs() else {
            panic!();
        };
        assert_eq!(needs.len(), 1);
        assert_eq!(needs[0].relation().fqn(), expected);
        assert_eq!(
            needs[0].target(),
            crate::compiler::CatalogLookupTarget::SystemTable
        );
        let provider = incomplete(answer_declarations(catalog));
        let _ = finish(provider);
    }
    let input = request(
        "WITH fixture AS (SELECT 'local' AS payload) SELECT payload FROM fixture",
        SqlCompileIntent::Query,
    );
    assert!(
        SqlCompiler::start(
            input.try_into_completion().unwrap(),
            &SqlCompileControl::unbounded()
        )
        .unwrap()
        .into_complete()
        .is_ok()
    );
}

#[test]
fn incompatible_key_sets_make_the_snapshot_constraint_empty() {
    let provider = snapshot_need(
        "SELECT payload FROM system.metadata.fixture WHERE schema_name IN ('a', 'b') AND schema_name IN ('c', 'd')",
    );
    let SqlNeedBatch::ProviderInputs { system_tables, .. } = provider.needs() else {
        panic!();
    };
    assert!(system_tables[0].key_constraint().is_none());
}

#[test]
fn view_definition_needs_only_the_declaration_and_saved_context_is_exact() {
    use crate::compiler::SqlCapturedCatalogPreparation;
    let body = (*crate::compiler::test_query("SELECT * FROM information_schema.fixture")).clone();
    let source = SqlCapturedCatalogPreparation::try_new_definition(
        body.clone(),
        "ice",
        "db",
        None,
        SqlCompileControl::unbounded(),
    )
    .unwrap();
    let [need] = source.needs() else {
        panic!("one declaration need");
    };
    assert_eq!(
        need.target(),
        crate::compiler::CatalogLookupTarget::SystemTable
    );
    let fact = CatalogRelationFact::system_table(need, Ok(declaration(need))).unwrap();
    let columns = source
        .continue_catalog(vec![fact].into())
        .unwrap()
        .analyze_view_definition(
            crate::compiler::builtin_sql_function_catalog(),
            crate::constant::test_constant_policy(),
        )
        .unwrap();
    assert_eq!(
        columns
            .iter()
            .map(|column| column.name.as_ref())
            .collect::<Vec<_>>(),
        vec!["schema_name", "payload"]
    );
    let view = Arc::new(crate::catalog::SqlViewDefinition {
        identity: novarocks_types::naming::TableIdentity::new("ice", "db", "v"),
        version: "v1".into(),
        raw_sql: "SELECT * FROM information_schema.fixture".into(),
        default_catalog: "ice".into(),
        default_database: "db".into(),
        columns: columns.into(),
    });
    let outer = (*crate::compiler::test_query("SELECT v.payload FROM ice.db.v")).clone();
    let source = SqlCapturedCatalogPreparation::try_new(
        outer.clone(),
        Some("caller"),
        "elsewhere",
        SqlCompileControl::unbounded(),
    )
    .unwrap();
    let [need] = source.needs() else {
        panic!("one view need");
    };
    let fact = CatalogRelationFact::view(need, view).unwrap();
    let source = source.continue_catalog(vec![fact].into()).unwrap();
    let [need] = source.needs() else {
        panic!("one saved-body declaration need");
    };
    assert_eq!(need.relation().fqn(), "ice.information_schema.fixture");
    assert_eq!(
        need.target(),
        crate::compiler::CatalogLookupTarget::SystemTable
    );
    let fact = CatalogRelationFact::system_table(need, Ok(declaration(need))).unwrap();
    let source = source.continue_catalog(vec![fact].into()).unwrap();
    assert!(source.needs().is_empty());
    let catalog = source.capture(&[]).unwrap();
    let (resolved, _, _) = crate::analyzer::analyze_with_function_catalog(
        &outer,
        catalog.provider(),
        "elsewhere",
        crate::compiler::builtin_sql_function_catalog(),
        crate::constant::test_constant_policy(),
        &SqlCompileControl::unbounded(),
    )
    .unwrap();
    let crate::analysis::QueryBody::Select(outer) = &resolved.body else {
        panic!();
    };
    let Some(crate::analysis::Relation::Subquery { query, .. }) = &outer.from else {
        panic!();
    };
    let crate::analysis::QueryBody::Select(body) = &query.body else {
        panic!();
    };
    assert!(
        matches!(&body.from, Some(crate::analysis::Relation::SystemTable { declaration, .. }) if declaration.binding.catalog_name() == "ice")
    );
}

#[test]
fn historical_system_references_are_refused_before_snapshot_or_provider_read() {
    for sql in [
        "SELECT * FROM system.metadata.fixture FOR VERSION AS OF 17",
        "SELECT * FROM iceberg.information_schema.fixture FOR SYSTEM_TIME AS OF '2026-01-01'",
    ] {
        let error = request(sql, SqlCompileIntent::Query)
            .try_into_completion()
            .err()
            .expect("historical system reference must fail");
        assert!(
            matches!(error, SqlCompileError::Analyze(error) if error.kind() == crate::analyze_error::AnalyzeErrorKind::UnsupportedQueryShape)
        );
    }
}

#[test]
fn snapshot_schema_and_empty_rows_keep_the_original_full_declaration() {
    use arrow::array::{ArrayRef, StringArray};
    let provider = snapshot_need("SELECT schema_name AS renamed FROM system.metadata.fixture");
    let SqlNeedBatch::ProviderInputs { system_tables, .. } = provider.needs() else {
        panic!();
    };
    let need = &system_tables[0];
    assert_eq!(need.required_columns(), &[0]);
    let storage = ValueTypeStorageLifetime::held(Arc::new(Hold));
    assert!(
        SystemTableSnapshotFact::try_new(need, Arc::from([]), ValueTypeStorageLifetime::pure())
            .is_ok()
    );
    for (name, nullable, rows) in [
        ("renamed", false, vec![Some("value")]),
        ("schema_name", true, vec![Some("value")]),
        ("schema_name", false, Vec::new()),
    ] {
        let array = Arc::new(StringArray::from(rows)) as ArrayRef;
        let batch = arrow::record_batch::RecordBatch::try_new(
            Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new(name, DataType::Utf8, nullable),
            ])),
            vec![array],
        )
        .unwrap();
        assert!(
            SystemTableSnapshotFact::try_new(need, vec![batch].into(), storage.clone()).is_err()
        );
    }
    let completed = finish(provider);
    let annotations = completed.plan().annotations();
    assert!(annotations.iter().any(|annotation| annotation.key.as_ref()
        == "sql.system.required_columns"
        && annotation.value.as_ref() == "[0]"));
}
