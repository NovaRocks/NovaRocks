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
mod merge_router_completion;
mod selected_action;
#[cfg(novarocks_dev_controls)]
mod target_occurrence;

use arrow::datatypes::DataType;
use novarocks_types::schema::ColumnDef;

fn test_query(source: &str) -> ast::Query {
    novarocks_parser::parser::parse_query(source).unwrap()
}
fn test_expr(source: &str) -> ast::Expr {
    let query = test_query(&format!("SELECT {source}"));
    let ast::SetExpr::Select(mut select) = query.body.into_inner() else {
        panic!("select fixture");
    };
    let ast::SelectItem::SingleColumn(column) = select.projection.remove(0) else {
        panic!("expression fixture");
    };
    assert!(column.alias.is_none());
    column.into_inner().expression
}
fn prepared_update(source: &str) -> PreparedUpdateStatement {
    let ast::TrinoStatement::Update(statement) =
        novarocks_parser::parser::parse_statement(source).unwrap()
    else {
        panic!("update fixture");
    };
    let super::super::mutation::OwnedMutationStatement::Update(prepared) =
        crate::dml::mutation::prepare_trino_update_shape(statement, source).unwrap()
    else {
        panic!("update fixture");
    };
    *prepared
}
fn prepared_merge(source: &str) -> PreparedMergeStatement {
    let ast::TrinoStatement::Merge(statement) =
        novarocks_parser::parser::parse_statement(source).unwrap()
    else {
        panic!("merge fixture");
    };
    let super::super::mutation::OwnedMutationStatement::Merge(prepared) =
        crate::dml::mutation::prepare_trino_merge_shape(statement, source).unwrap()
    else {
        panic!("merge fixture");
    };
    *prepared
}
fn prepared_assignment(column: &str, value: &str) -> PreparedMutationAssignment {
    PreparedMutationAssignment {
        column: column.into(),
        column_span: SYNTHETIC,
        value: test_expr(value),
    }
}
fn test_relation(source: &str) -> ast::Relation {
    let query = test_query(&format!("SELECT * FROM {source}"));
    let ast::SetExpr::Select(mut select) = query.body.into_inner() else {
        panic!("select fixture");
    };
    select.from.take().expect("FROM relation")
}
#[expect(
    clippy::too_many_arguments,
    reason = "The compiler witness supplies independent literal relations for every production MERGE clause."
)]
fn merge_query_fixture(
    target: &str,
    target_alias: &str,
    source: &str,
    on: &str,
    matched_predicate: Option<&str>,
    insert_predicate: Option<&str>,
    columns: &[ColumnDef],
    assignments: &[(&str, &str)],
    inserts: &[(&str, &str)],
    matched_action: Option<i32>,
    insert: bool,
) -> ast::Query {
    build_merge_match_query(
        test_relation(target),
        build::ident(target_alias, false),
        test_relation(source),
        test_expr(on),
        matched_predicate.map(test_expr),
        insert_predicate.map(test_expr),
        columns,
        assignments
            .iter()
            .map(|(name, value)| (build::ident(*name, false), test_expr(value)))
            .collect(),
        inserts
            .iter()
            .map(|(name, value)| (build::ident(*name, false), test_expr(value)))
            .collect(),
        matched_action,
        insert,
    )
    .unwrap()
}
fn connector_context_for_test() -> novarocks_spi::connector::ConnectorRequestContext {
    novarocks_spi::connector::ConnectorRequestContext::try_new(
        std::time::Instant::now() + std::time::Duration::from_secs(30),
        novarocks_spi::connector::ConnectorStopOwner::new().view(),
        64 * 1024,
        1024 * 1024,
    )
    .expect("connector request context")
}

fn col(name: &str) -> ColumnDef {
    ColumnDef {
        name: name.to_string(),
        data_type: DataType::Int64,
        nullable: true,
        write_default: None,
        logical_type: None,
    }
}

#[allow(
    dead_code,
    reason = "Retained for staged query-execution DML recovery and connector wiring."
)]
fn non_null_col(name: &str) -> ColumnDef {
    ColumnDef {
        name: name.to_string(),
        data_type: DataType::Int64,
        nullable: false,
        write_default: None,
        logical_type: None,
    }
}

fn iceberg_target() -> crate::catalog_application::resolver::TargetBackend {
    crate::catalog_application::resolver::TargetBackend {
        provider_id: novarocks_spi::connector::ConnectorProviderId::parse("iceberg")
            .expect("static Iceberg provider ID"),
        catalog: "ice".to_string(),
        namespace: "db1".to_string(),
        table: "t".to_string(),
    }
}

#[test]
fn mor_source_names_retain_cross_catalog_identity() {
    let target = iceberg_target();
    for (catalog, namespace, expected) in [
        ("ice", "db1", vec!["s"]),
        ("ice", "db2", vec!["db2", "s"]),
        ("other", "db1", vec!["other", "db1", "s"]),
    ] {
        let source = novarocks_types::naming::TableIdentity::new(catalog, namespace, "s");
        let name = mutation_source_table_name(&source, &target).unwrap();
        assert_eq!(
            name.parts
                .iter()
                .map(|part| part.value.as_str())
                .collect::<Vec<_>>(),
            expected
        );
    }
}

#[test]
fn mor_source_table_and_internal_names_keep_admitted_identity() {
    let merge = prepared_merge(
        "MERGE INTO ice.db1.t AS t\nUSING other.db2.source AS s\nON t.id = s.id\nWHEN MATCHED THEN UPDATE SET \"odd name\" = 7",
    );
    let source = mutation_source_relation(&merge.source, Some("ice"), &iceberg_target()).unwrap();
    let ast::Relation::Aliased(source) = source else {
        panic!("source alias");
    };
    let ast::Relation::Table(table) = source.relation.as_ref() else {
        panic!("source table");
    };
    assert_eq!(
        table
            .name
            .parts
            .iter()
            .map(|part| part.value.as_str())
            .collect::<Vec<_>>(),
        ["other", "db2", "source"]
    );
    let prepared = prepared_update("UPDATE ice.db1.t SET \"odd name\" = 7 WHERE id = 1");
    let columns = [col("id"), col("odd name")];
    let query = build_update_match_query(
        mutation_target_relation(&iceberg_target(), build::ident("t", false), "main").unwrap(),
        build::ident("t", false),
        None,
        update_assignment_projection(&prepared.assignments, &columns).unwrap(),
        prepared.selection.clone(),
    )
    .unwrap();
    let ast::SetExpr::Select(select) = query.body.as_ref() else {
        panic!("SELECT");
    };
    let ast::SelectItem::SingleColumn(single_column) = select.projection.last().unwrap() else {
        panic!("assignment alias");
    };
    let Some(alias) = &single_column.alias else {
        panic!("assignment alias");
    };

    assert_eq!(alias.value, "__nr_new_odd name");
    assert_eq!(alias.span, prepared.assignments[0].column_span);
}

#[test]
fn mor_target_branch_literal_keeps_backslashes_without_text_escaping() {
    let relation =
        mutation_target_relation(&iceberg_target(), build::ident("t", false), "dev\\branch")
            .unwrap();
    let ast::Relation::Aliased(alias) = &relation else {
        panic!("target alias");
    };
    let ast::Relation::Table(table) = alias.relation.as_ref() else {
        panic!("target table");
    };
    let Some(version) = &table.query_period else {
        panic!("versioned target");
    };
    assert_eq!(version.end.as_deref(), Some(&build::string("dev\\branch")));
}

#[test]
fn mor_lowering_preserves_admitted_ast_without_reading_source_spans() {
    use ast::SyntaxEq;
    let columns = [col("id"), col("v")];
    let source = "UPDATE ice.db1.t SET v = 99 WHERE id = 1";
    let original = prepared_update(source);
    let ast::TrinoStatement::Update(mut update) =
        novarocks_parser::parser::parse_statement(source).unwrap()
    else {
        panic!("UPDATE");
    };
    let ast::Expr::Literal(value) = &mut update.assignments[0].value else {
        panic!("literal");
    };
    value.span = novarocks_parser::Span::new(source.len() + 1, source.len() + 2);
    let super::super::mutation::OwnedMutationStatement::Update(lowered) =
        crate::dml::mutation::prepare_trino_update_shape(update, source).unwrap()
    else {
        panic!("UPDATE");
    };
    let (expected, _, _) =
        build_update_mor_producer(&iceberg_target(), &original, Some("ice"), &columns, "main")
            .unwrap();
    let (actual, _, _) =
        build_update_mor_producer(&iceberg_target(), &lowered, Some("ice"), &columns, "main")
            .unwrap();
    assert!(actual.syntax_eq(&expected));

    let source = "MERGE INTO ice.db1.t t USING (SELECT 1 AS id, 3 AS v) s ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = 99";
    let original = prepared_merge(source);
    let ast::TrinoStatement::Merge(mut merge) =
        novarocks_parser::parser::parse_statement(source).unwrap()
    else {
        panic!("MERGE");
    };
    let ast::TrinoMergeCase::Update { assignments, .. } = &mut merge.cases[0] else {
        panic!("UPDATE clause");
    };
    let ast::Expr::Literal(value) = &mut assignments[0].value else {
        panic!("literal");
    };
    value.span = novarocks_parser::Span::new(source.len() + 1, source.len() + 2);
    let super::super::mutation::OwnedMutationStatement::Merge(lowered) =
        crate::dml::mutation::prepare_trino_merge_shape(merge, source).unwrap()
    else {
        panic!("MERGE");
    };
    let (expected, _, _) = build_merge_mor_producer(
        &iceberg_target(),
        &original,
        Some("ice"),
        &columns,
        None,
        "main",
    )
    .unwrap();
    let (actual, _, _) = build_merge_mor_producer(
        &iceberg_target(),
        &lowered,
        Some("ice"),
        &columns,
        None,
        "main",
    )
    .unwrap();
    assert!(actual.syntax_eq(&expected));
}

fn analyze_mutation_query(
    query: ast::Query,
) -> Result<(), novarocks_sql::compiler::SqlCompileError> {
    use novarocks_sql::compiler::{
        SqlAnalyzeRequest, SqlCompileControl, SqlCompileIntent, SqlCompiler,
        SqlPlannerTableSnapshot, SqlPlanningEnvironment, SqlSessionContext, SqlStatementInput,
    };
    let catalog = novarocks_sql::planning::catalog::PlannerMemoryCatalog::default();
    let snapshot = SqlPlannerTableSnapshot::new(&catalog);
    let functions = novarocks_sql::compiler::build_builtin_engine_function_catalog().unwrap();
    let assignment = match query.body.as_ref() {
        ast::SetExpr::Select(select) => {
            select
                .projection
                .iter()
                .enumerate()
                .find_map(|(ordinal, item)| match item {
                    ast::SelectItem::SingleColumn(column)
                        if column
                            .alias
                            .as_ref()
                            .is_some_and(|alias| alias.value == "__nr_new_v") =>
                    {
                        Some((ordinal, column.expression.span()))
                    }
                    _ => None,
                })
        }
        _ => None,
    };
    let request = SqlAnalyzeRequest::new(
        SqlStatementInput::parsed_query(Box::new(query)),
        SqlCompileIntent::ChangeStreamWrite,
        SqlSessionContext {
            current_catalog: None,
            current_database: "default_database".into(),
            optimizer_settings: dml_change_stream_optimizer_settings(),
            sql_semantics: Default::default(),
        },
        SqlPlanningEnvironment::Distributed,
        &snapshot,
        &functions,
        novarocks_functions::testkit::shared_installed_builtin_owner_catalogue(),
        None,
        crate::application::test_constant_policy(),
        SqlCompileControl::unbounded(),
    );
    let request = match assignment {
        Some((ordinal, span)) => {
            let control = SqlCompileControl::unbounded();
            let site = novarocks_sql::compiler::SqlWriteAssignmentSite::try_new(
                ordinal,
                novarocks_type_contract::FunctionValueType::new(DataType::Int64, true),
                None,
                &control,
            )?
            .with_source_span(span);
            request.with_write_assignments(Arc::new(
                novarocks_sql::compiler::SqlWriteAssignments::try_new(
                    vec![site].into_boxed_slice(),
                    &control,
                )?,
            ))?
        }
        None => request,
    };
    SqlCompiler::analyze(request).map(|_| ())
}

#[test]
fn mor_generated_errors_point_to_user_expression_and_typed_assignment() {
    // The source expression is independently admitted. Its ordinary
    // assignment conversion belongs to the complete-query typed projection.
    let incompatible_assignment = "CAST(NULL AS DATE)";
    analyze_mutation_query(test_query(&format!("SELECT {incompatible_assignment}"))).unwrap();
    for (value, code) in [
        ("s.missing", "sql.analyze.unknown_column"),
        (incompatible_assignment, "sql.analyze.type_mismatch"),
    ] {
        let source = format!(
            "MERGE INTO ice.db1.t AS t\nUSING (SELECT 1 AS id) AS s ON t.id = s.id\nWHEN MATCHED THEN UPDATE SET v = {value}"
        );
        let prepared = prepared_merge(&source);
        let (mut query, _, _) = build_merge_mor_producer(
            &iceberg_target(),
            &prepared,
            Some("ice"),
            &[col("id"), col("v")],
            None,
            "main",
        )
        .unwrap();
        let ast::SetExpr::Select(select) = query.body.as_mut() else {
            panic!("SELECT");
        };
        // Supply exactly the external target's facts through a local
        // relation, retaining the production-generated expression tree.
        let Some(ast::Relation::Join(join)) = select.from.as_mut() else {
            panic!("MERGE join");
        };
        *join.right = test_relation(
            "(SELECT 1 AS id, 2 AS v, 'f' AS _file, CAST(0 AS BIGINT) AS _pos, CAST(1 AS BIGINT) AS _row_id, CAST(1 AS BIGINT) AS _last_updated_sequence_number, CAST(0 AS INTEGER) AS _spec_id, CAST(NULL AS ROW(bucket INTEGER)) AS _partition) AS t",
        );
        let error = analyze_mutation_query(query).unwrap_err();
        let error =
            crate::dml::error::DmlExecutionError::from_compile(error).into_dml_error(Some(&source));
        let user = error.user_error().expect("typed SQL analysis error");
        assert_eq!(user.code().as_str(), code, "{user}");
        let location = user.location().expect("original expression location");
        let expected = novarocks_parser::Span::new(
            source.find(value).unwrap(),
            source.find(value).unwrap() + value.len(),
        )
        .to_user_error_location(&source);
        assert_eq!(
            (location.line(), location.column()),
            (expected.line(), expected.column()),
            "{user}"
        );
        assert_eq!(location.line(), 3);
    }
}

#[test]
fn reject_reserved_update_columns() {
    let err = validate_update_assignments(
        &[prepared_assignment("_row_id", "1")],
        &[col("id"), col("v")],
    )
    .expect_err("must reject");
    assert!(err.contains("reserved Iceberg metadata column"), "{err}");
}

#[test]
fn replacement_assignment_uses_target_conversion_and_original_old_identity() {
    use ast::SyntaxEq;
    let columns = [col("id"), col("p")];
    let assignments = [prepared_assignment("P", "NULL")];
    validate_update_assignments(&assignments, &columns).unwrap();
    let projected = update_assignment_projection(&assignments, &columns).unwrap();
    assert_eq!(projected[0].0.value, "p");
    assert!(projected[0].1.syntax_eq(&test_expr("(NULL)")));
    let query = build_update_match_query(
        test_relation("ice.db.t AS t"),
        build::ident("t", false),
        None,
        projected,
        Some(test_expr("t.id = 1")),
    )
    .unwrap();
    assert!(query.syntax_eq(&test_query("SELECT t._file AS __nr_file, t._pos AS __nr_pos, t._row_id AS __nr_row_id, t._last_updated_sequence_number AS __nr_last_updated_sequence_number, t._spec_id AS __nr_spec_id, t._partition AS __nr_partition, t.*, (NULL) AS __nr_new_p FROM ice.db.t AS t WHERE t.id = 1")));
    for assignments in [
        vec![prepared_assignment("missing", "1")],
        vec![prepared_assignment("p", "1"), prepared_assignment("P", "2")],
    ] {
        assert!(validate_update_assignments(&assignments, &columns).is_err());
    }
}

#[test]
fn update_match_query_projects_identity_columns() {
    use ast::SyntaxEq;
    let query = build_update_match_query(
        test_relation("ice.db1.t AS t"),
        build::ident("t", false),
        Some(test_relation("staging.s AS s")),
        vec![(build::ident("v", false), test_expr("s.v"))],
        Some(test_expr("t.id = s.id")),
    )
    .unwrap();
    assert!(query.syntax_eq(&test_query("SELECT t._file AS __nr_file, t._pos AS __nr_pos, t._row_id AS __nr_row_id, t._last_updated_sequence_number AS __nr_last_updated_sequence_number, t._spec_id AS __nr_spec_id, t._partition AS __nr_partition, t.*, s.v AS __nr_new_v FROM ice.db1.t AS t CROSS JOIN staging.s AS s WHERE t.id = s.id")));
}

#[test]
fn update_change_stream_target_pins_branch_read_snapshot() {
    use ast::SyntaxEq;
    let target =
        mutation_target_relation(&iceberg_target(), build::ident("t", false), "dev").unwrap();
    assert!(target.syntax_eq(&test_relation(
        r#""ice"."db1"."t" FOR VERSION AS OF 'dev' AS t"#
    )));
}

#[test]
fn update_assignment_projection_casts_to_target_type() {
    use ast::SyntaxEq;
    let projected = update_assignment_projection(
        &[prepared_assignment("v", "src_v")],
        &[
            typed_col("id", DataType::Int64),
            typed_col("v", DataType::Int32),
        ],
    )
    .expect("assignment projection");
    assert_eq!(projected.len(), 1);
    assert_eq!(projected[0].0.value, "v");
    assert!(projected[0].1.syntax_eq(&test_expr("(src_v)")));
}

#[test]
fn update_change_stream_match_query_uses_casted_assignment_projection() {
    use ast::SyntaxEq;
    let projected = update_assignment_projection(
        &[prepared_assignment("v", "src_v")],
        &[
            typed_col("id", DataType::Int64),
            typed_col("v", DataType::Int32),
        ],
    )
    .expect("assignment projection");
    let query = build_update_match_query(
        mutation_target_relation(&iceberg_target(), build::ident("t", false), "main").unwrap(),
        build::ident("t", false),
        Some(test_relation("staging.s AS s")),
        projected,
        Some(test_expr("t.id = s.id")),
    )
    .unwrap();
    assert!(query.syntax_eq(&test_query("SELECT t._file AS __nr_file, t._pos AS __nr_pos, t._row_id AS __nr_row_id, t._last_updated_sequence_number AS __nr_last_updated_sequence_number, t._spec_id AS __nr_spec_id, t._partition AS __nr_partition, t.*, (src_v) AS __nr_new_v FROM \"ice\".\"db1\".\"t\" AS t CROSS JOIN staging.s AS s WHERE t.id = s.id")));
}

fn typed_col(name: &str, data_type: DataType) -> ColumnDef {
    ColumnDef {
        name: name.to_string(),
        data_type,
        nullable: false,
        write_default: None,
        logical_type: None,
    }
}

#[test]
fn merge_actions_keep_clause_predicates_nested_in_original_order() {
    let matched = test_expr("s.v > 5 OR (s.v = 5 AND s.id > 0)");
    let inserted = test_expr("s.id > 10 AND (s.v > 0 OR s.v IS NULL)");
    let query = build_merge_match_query(
        test_relation("ice.db1.t AS t"),
        build::ident("t", false),
        test_relation("staging.s AS s"),
        test_expr("t.id = s.id"),
        Some(matched.clone()),
        Some(inserted.clone()),
        &[col("id"), col("v")],
        Vec::new(),
        Vec::new(),
        Some(MERGE_ACTION_MATCHED_UPDATE),
        true,
    )
    .unwrap();
    let ast::SetExpr::Select(select) = query.body.as_ref() else {
        panic!("SELECT");
    };
    let action = select
        .projection
        .iter()
        .find_map(|item| match item {
            ast::SelectItem::SingleColumn(column)
                if column
                    .alias
                    .as_ref()
                    .is_some_and(|alias| alias.value == "__nr_merge_action") =>
            {
                Some(&column.expression)
            }
            _ => None,
        })
        .unwrap();
    let ast::Expr::SearchedCase(case) = action else {
        panic!("ordered action CASE");
    };
    assert_eq!(case.when_clauses.len(), 2);
    assert!(matches!(
        &case.default_value,
        Some(ast::Expr::Literal(ast::Literal {
            kind: ast::LiteralKind::Long {
                parsed_value: 0,
                ..
            },
            ..
        }))
    ));
    for (index, (original, guard, result)) in [
        (&matched, true, MERGE_ACTION_MATCHED_UPDATE),
        (&inserted, false, MERGE_ACTION_NOT_MATCHED_INSERT),
    ]
    .into_iter()
    .enumerate()
    {
        let branch = &case.when_clauses[index];
        let ast::WhenMatch::Operand(condition) = &branch.match_value else {
            panic!("searched CASE condition must be an operand");
        };
        let ast::Expr::Logical(condition) = condition else {
            panic!("action AND");
        };
        assert_eq!(condition.operator, ast::LogicalOperator::And);
        assert_eq!(condition.terms.len(), 2);
        let ast::Expr::Predicated(predicate) = &condition.terms[0] else {
            panic!("action null guard");
        };
        assert!(
            matches!(&predicate.predicate, ast::Predicate::IsNull(null) if null.negated == guard)
        );
        assert!(
            ast::identifier_chain_observed(
                &predicate.value,
                |_| Ok::<(), std::convert::Infallible>(())
            )
            .unwrap_or_else(|never| match never {})
            .is_some_and(|chain| chain.last().value == "_file")
        );
        assert_eq!(&condition.terms[1], original);
        assert_eq!(condition.terms[1].span(), original.span());
        assert!(
            matches!(&branch.result, ast::Expr::Literal(ast::Literal { kind: ast::LiteralKind::Long { parsed_value, .. }, .. }) if *parsed_value == i64::from(result))
        );
    }
}

#[test]
fn merge_match_query_projects_nullable_target_columns() {
    use ast::SyntaxEq;
    let query = merge_query_fixture(
        "ice.db1.t AS t",
        "t",
        "staging.s AS s",
        "t.id = s.id",
        None,
        None,
        &[col("id"), col("v")],
        &[("v", "s.v")],
        &[("id", "s.id"), ("v", "s.v")],
        Some(MERGE_ACTION_MATCHED_UPDATE),
        true,
    );
    let ast::SetExpr::Select(select) = query.body.as_ref() else {
        panic!("SELECT");
    };
    assert!(
        !select
            .projection
            .iter()
            .any(|item| matches!(item, ast::SelectItem::AllColumns(_)))
    );
    for (alias, expected) in [
        (
            "id",
            r#"CASE WHEN t."_file" IS NOT NULL THEN t."id" ELSE NULL END"#,
        ),
        (
            "__nr_row_id",
            r#"CASE WHEN t."_file" IS NOT NULL THEN t."_row_id" ELSE NULL END"#,
        ),
        ("__nr_new_v", "(s.v)"),
        ("__nr_ins_id", "(s.id)"),
    ] {
        let expr = select
            .projection
            .iter()
            .find_map(|item| match item {
                ast::SelectItem::SingleColumn(column)
                    if column
                        .alias
                        .as_ref()
                        .is_some_and(|name| name.value == alias) =>
                {
                    Some(&column.expression)
                }
                _ => None,
            })
            .expect("projected alias");
        assert!(expr.syntax_eq(&test_expr(expected)), "{alias}");
    }
    assert!(select.projection.iter().any(|item| matches!(item, ast::SelectItem::SingleColumn(column) if column.alias.as_ref().is_some_and(|alias| alias.value == "__nr_merge_action"))));
}

// ---- the merge-on-read write-session data plane ----------------------
//
// The session under test is scripted rather than provider-backed: what these
// cases are about is which branch a row reaches, how many times the
// connector is asked to commit, and whether it is asked at all -- none of
// which needs a real Iceberg table.

#[derive(Clone, Debug, Eq, PartialEq)]
struct FakeRowMutationCommit;
#[derive(Clone, Debug, Eq, PartialEq)]
struct FakeRowMutationWriter(u32);
#[derive(Clone, Debug, Eq, PartialEq)]
struct FakeRowMutationFragment;

struct FakeRowMutationProvider {
    descriptor: novarocks_spi::connector::ConnectorInstanceDescriptor,
    catalog_handle: novarocks_spi::connector::CatalogHandle,
}

impl novarocks_spi::connector::write_stack::ProviderWriteRuntime for FakeRowMutationProvider {
    type CommitSession = FakeRowMutationCommit;
    type WriterHandle = FakeRowMutationWriter;
    type CommitFragment = FakeRowMutationFragment;

    fn descriptor(&self) -> &novarocks_spi::connector::ConnectorInstanceDescriptor {
        &self.descriptor
    }

    fn catalog_handle(&self) -> &novarocks_spi::connector::CatalogHandle {
        &self.catalog_handle
    }
}

fn row_mutation_catalog_handle() -> novarocks_spi::connector::CatalogHandle {
    novarocks_spi::connector::CatalogHandle::new(
        novarocks_spi::connector::ConnectorInstanceId::parse("mor_session_unit")
            .expect("instance id"),
        novarocks_spi::connector::CatalogVersion::from_bytes([9; 32]),
    )
}

fn row_mutation_payload(
    category: novarocks_spi::connector::ConnectorCodecCategory,
    payload: impl Into<bytes::Bytes>,
) -> novarocks_spi::connector::ConnectorEncodedPayload {
    novarocks_spi::connector::ConnectorEncodedPayload::new(
        novarocks_spi::connector::ConnectorEnvelopeHeader::new(
            novarocks_spi::connector::ConnectorProviderId::parse("fake").expect("provider id"),
            row_mutation_catalog_handle(),
            category,
            novarocks_spi::connector::ConnectorCodecRevision::try_new(1).expect("codec revision"),
        ),
        payload.into(),
    )
}

fn row_mutation_catalog_properties() -> novarocks_spi::connector::CatalogProperties {
    novarocks_spi::connector::CatalogProperties::new(
        row_mutation_catalog_handle(),
        novarocks_spi::connector::ConnectorProviderId::parse("iceberg")
            .expect("static provider ID"),
        1,
        Vec::new(),
        Vec::new(),
    )
    .expect("test catalog properties")
}

fn row_mutation_adapter()
-> novarocks_spi::connector::write_stack::WriteRuntimeAdapter<FakeRowMutationProvider> {
    let handle = row_mutation_catalog_handle();
    novarocks_spi::connector::write_stack::WriteRuntimeAdapter::new(Arc::new(
        FakeRowMutationProvider {
            descriptor: novarocks_spi::connector::ConnectorInstanceDescriptor {
                provider_id: novarocks_spi::connector::ConnectorProviderId::parse("fake")
                    .expect("provider id"),
                instance_id: handle.catalog_name().clone(),
            },
            catalog_handle: handle,
        },
    ))
}

#[derive(Default)]
struct RowMutationSessionCalls {
    finish: usize,
    abort: usize,
}

/// A row-mutation control that seals the merge-on-read branch pair.
///
/// The branches are handed back in descending ordinal order on purpose: a
/// consumer that took a branch's identity from its position in this list
/// would give every route the other branch's writer.
struct FakeRowMutationControl {
    adapter: novarocks_spi::connector::write_stack::WriteRuntimeAdapter<FakeRowMutationProvider>,
    binding_key: novarocks_spi::connector::ConnectorProviderBindingKey,
    routed: bool,
    calls: Arc<Mutex<RowMutationSessionCalls>>,
    finish_outcome: Mutex<
        Option<
            novarocks_spi::connector::ExternalMutationOutcome<
                novarocks_spi::connector::ConnectorWriteReceipt,
            >,
        >,
    >,
}

fn field_binding(
    key: u8,
    name: &str,
    data_type: DataType,
) -> novarocks_spi::connector::ConnectorWriteFieldBinding {
    novarocks_spi::connector::ConnectorWriteFieldBinding::new(
        novarocks_spi::connector::ConnectorWriteFieldToken::from_bytes([key; 32]),
        arrow::datatypes::Field::new(name, data_type, false),
    )
}

/// The data branch: after-image values, accepting replacements and inserts.
const MOR_DATA_ORDINAL: u32 = 0;
/// The deletion-vector branch: row identity, accepting deletes and the
/// before-image half of a replacement.
const MOR_DELETE_ORDINAL: u32 = 1;

fn mor_branch(
    adapter: &novarocks_spi::connector::write_stack::WriteRuntimeAdapter<FakeRowMutationProvider>,
    ordinal: u32,
    routed: bool,
) -> Result<
    novarocks_spi::connector::write_stack::ConnectorWriteTargetPlan,
    novarocks_spi::connector::ConnectorError,
> {
    use novarocks_spi::connector::ConnectorRowMutationEffect;
    use novarocks_spi::connector::write_stack::{
        ConnectorWriteRouteFacts, ConnectorWriteTargetPlan, WriteTargetOrdinal,
    };

    let (input, effects) = if ordinal == MOR_DATA_ORDINAL {
        (
            novarocks_spi::connector::ConnectorWriteInputShape::Data {
                fields: vec![field_binding(1, "v", DataType::Int64)],
            },
            vec![
                ConnectorRowMutationEffect::Replace,
                ConnectorRowMutationEffect::Insert,
            ],
        )
    } else {
        (
            novarocks_spi::connector::ConnectorWriteInputShape::DeletionVector {
                identity_fields: vec![
                    field_binding(2, "_file", DataType::Utf8),
                    field_binding(3, "_pos", DataType::Int64),
                ],
                partition_source_fields: Vec::new(),
            },
            vec![
                ConnectorRowMutationEffect::Delete,
                ConnectorRowMutationEffect::Replace,
            ],
        )
    };
    let input_ordinals = input
        .fields()
        .into_iter()
        .enumerate()
        .map(|(index, field)| {
            novarocks_spi::connector::ConnectorMutationRouteInput::new(
                field.token(),
                u32::try_from(index).expect("bounded test input"),
            )
        })
        .collect();
    let plan = ConnectorWriteTargetPlan::new(
        WriteTargetOrdinal::try_new(ordinal)?,
        adapter.wrap_writer_handle(FakeRowMutationWriter(ordinal)),
        input,
    );
    if !routed {
        return Ok(plan);
    }
    Ok(plan.with_route(ConnectorWriteRouteFacts::try_new(
        novarocks_spi::connector::ConnectorWriteRouteId::from_bytes(
            [u8::try_from(ordinal).expect("bounded ordinal"); 32],
        ),
        effects,
        input_ordinals,
        novarocks_spi::connector::write_stack::ConnectorWriteRouteRequirements {
            colocation: novarocks_spi::connector::write_stack::ConnectorWriteColocation::None,
            layout: novarocks_spi::connector::write_stack::ConnectorWriteLayoutPreference::None,
        },
    )?))
}

impl novarocks_spi::connector::write_stack::ConnectorWriteControl for FakeRowMutationControl {
    fn io_profile(
        &self,
    ) -> Result<
        novarocks_spi::connector::ConnectorMetadataMaintenanceIoProfile,
        novarocks_spi::connector::ConnectorError,
    > {
        unreachable!("scripted row-mutation fixture performs no asynchronous Provider I/O")
    }
    fn cleanup_responsibility(
        &self,
        _commit: &novarocks_spi::connector::write_stack::ConnectorWriteCommitSession,
    ) -> Result<
        novarocks_spi::connector::ConnectorMetadataCleanupResponsibility,
        novarocks_spi::connector::ConnectorError,
    > {
        unreachable!("scripted row-mutation fixture publishes no cleanup responsibility")
    }

    fn begin_write_query(
        &self,
        _: &novarocks_spi::connector::write_stack::ConnectorWriteCommitSession,
        _: &novarocks_spi::connector::write_stack::ConnectorWriteQueryIdentity,
        _: &[novarocks_spi::connector::write_stack::WriteTargetOrdinal],
        _: Option<
            std::sync::Arc<
                dyn novarocks_spi::connector::write_stack::ConnectorWriteProductionProof,
            >,
        >,
        _: Option<&novarocks_spi::connector::read_stack::runtime::ConnectorRewrittenTargetFact>,
    ) -> Result<(), novarocks_spi::connector::ConnectorError> {
        Ok(())
    }

    fn arrive_write_fragments(
        &self,
        _: &novarocks_spi::connector::write_stack::ConnectorWriteCommitSession,
        _: &novarocks_spi::connector::write_stack::ConnectorWriteQueryIdentity,
        _: Vec<(
            novarocks_spi::connector::write_stack::WriteTargetOrdinal,
            novarocks_spi::connector::write_stack::ConnectorCommitFragment,
        )>,
        _: &novarocks_spi::connector::ConnectorPayloadRetentionGuard,
    ) -> Result<(), novarocks_spi::connector::ConnectorError> {
        unreachable!("scripted row-mutation fixture receives no commit fragments")
    }

    fn confirm_write_received(
        &self,
        _: &novarocks_spi::connector::write_stack::ConnectorWriteCommitSession,
        _: &novarocks_spi::connector::write_stack::ConnectorWriteQueryIdentity,
    ) -> Result<(), novarocks_spi::connector::ConnectorError> {
        Ok(())
    }

    fn begin_write_async<'a>(
        &'a self,
        request: novarocks_spi::connector::write_stack::ConnectorWriteBeginRequest,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<
                        novarocks_spi::connector::write_stack::ConnectorWriteSessionPlan,
                        novarocks_spi::connector::ConnectorError,
                    >,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move { self.begin_write(request) })
    }

    fn finish_write_async<'a>(
        &'a self,
        _request: novarocks_spi::connector::write_stack::ConnectorWriteFinishRequestRef<'a>,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<
                        novarocks_spi::connector::ExternalMutationOutcome<
                            novarocks_spi::connector::ConnectorWriteReceipt,
                        >,
                        novarocks_spi::connector::ConnectorError,
                    >,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            self.calls.lock().expect("recorded calls").finish += 1;
            self.finish_outcome
                .lock()
                .expect("scripted outcome")
                .take()
                .ok_or_else(|| {
                    novarocks_spi::connector::ConnectorError::new(
                        novarocks_spi::connector::ConnectorErrorKind::Internal,
                        "no scripted commit outcome",
                    )
                })
        })
    }

    fn finish_write_with_needed_ready_async<'a>(
        &'a self,
        _request: novarocks_spi::connector::write_stack::ConnectorWriteFinishRequestRef<'a>,

        receiving: &'a novarocks_spi::connector::write_stack::ConnectorWriteNeededReadyReceiving,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<
                        novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion,
                        novarocks_spi::connector::ConnectorError,
                    >,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let outcome = async {
                self.calls.lock().expect("recorded calls").finish += 1;
                self.finish_outcome
                    .lock()
                    .expect("scripted outcome")
                    .take()
                    .ok_or_else(|| {
                        novarocks_spi::connector::ConnectorError::new(
                            novarocks_spi::connector::ConnectorErrorKind::Internal,
                            "no scripted commit outcome",
                        )
                    })
            }
            .await.map(novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion::from_outcome);
            if let Ok(novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion::KnownCommitted { needed, .. }) = &outcome {
                receiving.publish(std::sync::Arc::clone(needed))?;
            }
            outcome
        })
    }

    fn abort_write_async<'a>(
        &'a self,
        request: novarocks_spi::connector::write_stack::ConnectorWriteSessionAbortRequest<'a>,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<
                        novarocks_spi::connector::ConnectorWriteAbortOutcome,
                        novarocks_spi::connector::ConnectorError,
                    >,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move { self.abort_write(request) })
    }

    fn reconcile_write_async<'a>(
        &'a self,
        request: novarocks_spi::connector::write_stack::ConnectorWriteSessionReconcileRequest<'a>,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<
                        novarocks_spi::connector::ExternalMutationOutcome<
                            novarocks_spi::connector::ConnectorWriteReceipt,
                        >,
                        novarocks_spi::connector::ConnectorError,
                    >,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move { self.reconcile_write(request) })
    }

    fn reconcile_write_with_needed_ready_async<'a>(
        &'a self,
        request: novarocks_spi::connector::write_stack::ConnectorWriteSessionReconcileRequest<'a>,

        receiving: &'a novarocks_spi::connector::write_stack::ConnectorWriteNeededReadyReceiving,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<
                        novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion,
                        novarocks_spi::connector::ConnectorError,
                    >,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let outcome = async { self.reconcile_write(request) }.await.map(novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion::from_outcome);
            if let Ok(novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion::KnownCommitted { needed, .. }) = &outcome {
                receiving.publish(std::sync::Arc::clone(needed))?;
            }
            outcome
        })
    }

    fn binding_key(&self) -> &novarocks_spi::connector::ConnectorProviderBindingKey {
        &self.binding_key
    }

    fn begin_write(
        &self,
        _request: novarocks_spi::connector::write_stack::ConnectorWriteBeginRequest,
    ) -> Result<
        novarocks_spi::connector::write_stack::ConnectorWriteSessionPlan,
        novarocks_spi::connector::ConnectorError,
    > {
        novarocks_spi::connector::write_stack::ConnectorWriteSessionPlan::try_new(
            self.adapter.wrap_commit_session(FakeRowMutationCommit),
            vec![
                mor_branch(&self.adapter, MOR_DELETE_ORDINAL, self.routed)?,
                mor_branch(&self.adapter, MOR_DATA_ORDINAL, self.routed)?,
            ],
        )
    }

    fn finish_write(
        &self,
        _request: novarocks_spi::connector::write_stack::ConnectorWriteFinishRequest<'_>,
    ) -> Result<
        novarocks_spi::connector::ExternalMutationOutcome<
            novarocks_spi::connector::ConnectorWriteReceipt,
        >,
        novarocks_spi::connector::ConnectorError,
    > {
        self.calls.lock().expect("recorded calls").finish += 1;
        self.finish_outcome
            .lock()
            .expect("scripted outcome")
            .take()
            .ok_or_else(|| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::Internal,
                    "no scripted commit outcome",
                )
            })
    }

    fn abort_write(
        &self,
        _request: novarocks_spi::connector::write_stack::ConnectorWriteSessionAbortRequest<'_>,
    ) -> Result<
        novarocks_spi::connector::ConnectorWriteAbortOutcome,
        novarocks_spi::connector::ConnectorError,
    > {
        self.calls.lock().expect("recorded calls").abort += 1;
        Ok(
            novarocks_spi::connector::ConnectorWriteAbortOutcome::KnownUncommitted {
                cleanup: novarocks_spi::connector::ExternalMutationFinalization::Complete,
            },
        )
    }

    fn reconcile_write(
        &self,
        _request: novarocks_spi::connector::write_stack::ConnectorWriteSessionReconcileRequest<'_>,
    ) -> Result<
        novarocks_spi::connector::ExternalMutationOutcome<
            novarocks_spi::connector::ConnectorWriteReceipt,
        >,
        novarocks_spi::connector::ConnectorError,
    > {
        Err(novarocks_spi::connector::ConnectorError::new(
            novarocks_spi::connector::ConnectorErrorKind::Internal,
            "reconcile is not scripted in this test",
        ))
    }
}

struct FakeRowMutationEncoder;

impl novarocks_spi::connector::ConnectorWriteHandleWireEncoder for FakeRowMutationEncoder {
    fn owner(&self) -> &str {
        "fake"
    }

    fn encode_writer_handle_payload(
        &self,
        _handle: &novarocks_spi::connector::write_stack::ConnectorWriterHandle,
    ) -> Result<
        novarocks_spi::connector::ConnectorEncodedPayload,
        novarocks_spi::connector::ConnectorCodecError,
    > {
        Ok(row_mutation_payload(
            novarocks_spi::connector::ConnectorCodecCategory::WriteHandle,
            bytes::Bytes::from_static(b"row-mutation-handle"),
        ))
    }
}

struct FakeRowMutationDecoder {
    adapter: novarocks_spi::connector::write_stack::WriteRuntimeAdapter<FakeRowMutationProvider>,
}

impl novarocks_spi::connector::ConnectorWriteFragmentWireDecoder for FakeRowMutationDecoder {
    fn owner(&self) -> &str {
        "fake"
    }

    fn decode_commit_fragment_payload(
        &self,
        _fragment: &novarocks_spi::connector::ConnectorEncodedPayload,
        _author: &novarocks_spi::connector::ConnectorPayloadRetentionGuard,
    ) -> Result<
        novarocks_spi::connector::write_stack::ConnectorCommitFragment,
        novarocks_spi::connector::ConnectorCodecError,
    > {
        Ok(self.adapter.wrap_commit_fragment(FakeRowMutationFragment))
    }
}

struct UnusedLegacyWriteControl;

impl novarocks_spi::connector::ConnectorWriteControl for UnusedLegacyWriteControl {
    fn binding_key(&self) -> &novarocks_spi::connector::ConnectorProviderBindingKey {
        unreachable!("the legacy control is not exercised by a write session")
    }
}

struct RowMutationSessionFixture {
    session: Arc<ConnectorWriteSession>,
}

fn row_mutation_session_fixture(
    routed: bool,
    outcome: novarocks_spi::connector::ExternalMutationOutcome<
        novarocks_spi::connector::ConnectorWriteReceipt,
    >,
) -> RowMutationSessionFixture {
    let adapter = row_mutation_adapter();
    let calls = Arc::new(Mutex::new(RowMutationSessionCalls::default()));
    let control = Arc::new(FakeRowMutationControl {
        adapter: adapter.clone(),
        binding_key: novarocks_spi::connector::ConnectorProviderBindingKey {
            instance_id: row_mutation_catalog_handle().catalog_name().clone(),
            incarnation: novarocks_spi::connector::ProviderBindingEpoch::new(),
        },
        routed,
        calls: Arc::clone(&calls),
        finish_outcome: Mutex::new(Some(outcome)),
    });
    let admission = novarocks_spi::connector::ConnectorProcessLocalPayload::new(
        control.binding_key.clone(),
        Arc::new(()),
    );
    let lease = novarocks_catalog_application::test_support::write_stack_lease(
        novarocks_spi::connector::ConnectorControlRuntimeId::new(),
        novarocks_spi::connector::ConnectorControlWriteBinding::new(
            Arc::new(UnusedLegacyWriteControl),
            control,
            Arc::new(FakeRowMutationEncoder),
            Arc::new(FakeRowMutationDecoder { adapter }),
        ),
        || {},
    );
    let session = crate::query_execution::write_session::tests::begin_for_test(
        lease,
        row_mutation_catalog_properties(),
        novarocks_spi::connector::write_stack::ConnectorWriteBeginRequest {
            catalog_session: Default::default(),
            table: Arc::from("db1.t"),
            target_ref: novarocks_spi::connector::ConnectorWriteTargetRef::main(),
            intent: novarocks_spi::connector::ConnectorWriteIntent::RowDelta,
            purpose: novarocks_spi::connector::ConnectorWriteAdmissionPurpose::OrdinaryDml,
            input: novarocks_spi::connector::ConnectorWriteInputRequest::Data {
                fields: vec![novarocks_spi::connector::ConnectorWriteFieldRequest::new(
                    arrow::datatypes::Field::new("v", DataType::Int64, true),
                )],
            },
            base: None,
            flavor:
                novarocks_spi::connector::write_stack::ConnectorWriteSessionFlavor::RowMutation {
                    admission,
                },
            context: connector_context_for_test(),
        },
    )
    .expect("begin row-mutation write session");
    RowMutationSessionFixture { session }
}

fn known_uncommitted_outcome() -> novarocks_spi::connector::ExternalMutationOutcome<
    novarocks_spi::connector::ConnectorWriteReceipt,
> {
    novarocks_spi::connector::ExternalMutationOutcome::KnownUncommitted {
        failure: novarocks_spi::connector::ConnectorMutationFailure::new(
            novarocks_spi::connector::ConnectorMutationFailureKind::Unavailable,
            "scripted",
        ),
        cleanup: novarocks_spi::connector::ExternalMutationFinalization::Complete,
    }
}

/// Each merge-on-read branch feeds the writer holding its own sealed
/// ordinal, and the router sees them in ordinal order.
///
/// The scripted provider returns its branches in descending ordinal order,
/// so a builder that read a branch's identity from its position in the list
/// would pair the delete route with the data writer and vice versa.
#[test]
fn a_row_mutation_session_routes_each_branch_to_its_own_write_target_ordinal() {
    let fixture = row_mutation_session_fixture(true, known_uncommitted_outcome());
    let routed = change_stream_routed_targets(&fixture.session).expect("routed branches");

    assert_eq!(
        routed
            .iter()
            .map(|(write_target, _)| write_target.ordinal().get())
            .collect::<Vec<_>>(),
        vec![MOR_DATA_ORDINAL, MOR_DELETE_ORDINAL],
    );
    for (write_target, route) in &routed {
        // The route the branch carries is the one the provider signed for
        // that exact ordinal, not the one sitting at the same position.
        assert_eq!(
            route.route_id(),
            novarocks_spi::connector::ConnectorWriteRouteId::from_bytes(
                [u8::try_from(write_target.ordinal().get()).expect("bounded ordinal"); 32]
            ),
        );
    }
    let data_effects = routed[0].1.accepted_effects();
    assert!(data_effects.contains(&novarocks_spi::connector::ConnectorRowMutationEffect::Insert));
    let delete_effects = routed[1].1.accepted_effects();
    assert!(delete_effects.contains(&novarocks_spi::connector::ConnectorRowMutationEffect::Delete));
}

/// A branch that reached the router without routing facts would leave SQL
/// with rows it has nowhere to send, so it is refused rather than defaulted.
#[test]
fn a_row_mutation_branch_without_routing_facts_fails_closed() {
    let fixture = row_mutation_session_fixture(false, known_uncommitted_outcome());
    let error = change_stream_routed_targets(&fixture.session)
        .expect_err("an unrouted branch must fail closed");
    assert!(
        error.contains("carries no provider routing facts"),
        "{error}"
    );
}
