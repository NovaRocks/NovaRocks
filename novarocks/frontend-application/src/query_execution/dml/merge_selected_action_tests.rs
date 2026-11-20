// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use novarocks_expression_contract::{BoundExprSet, ExprId, ExprKind, RootOrdinal};
use novarocks_physical_plan::{FragmentId, NodeId, NodeKind, PhysicalPlan, ValueId, ValueOrigin};

// These are compiler witnesses, not a second evaluator. Literal relations
// replace provider reads; production SQL analysis, folding, optimization and
// physical completion still author every expression inspected below.
fn compile_selected_action(
    matched_action: Option<i32>,
    matched_predicate: &str,
    insert_predicate: &str,
    insert: bool,
) -> novarocks_sql::compiler::SqlAuthoredPhysicalPlan {
    use novarocks_physical_plan::{PipelineDopDomain, PlanVersionId};
    use novarocks_sql::compiler::{
        DEFAULT_COMPLETION_LIMITS, SqlCompileControl, SqlCompileIntent, SqlCompiler,
        SqlFinalPlanCompileRequest, SqlPlanningEnvironment, SqlSessionContext, SqlStatementInput,
    };

    let query = merge_query_fixture(
        "(SELECT CAST(2 AS BIGINT) AS id, CAST(20 AS BIGINT) AS _row_id, \
         'fixture.parquet' AS _file, CAST(0 AS BIGINT) AS _pos, \
         CAST(1 AS BIGINT) AS _last_updated_sequence_number, CAST(0 AS INTEGER) AS _spec_id, \
         CAST(NULL AS ROW(bucket INTEGER)) AS _partition) AS t",
        "t",
        "(SELECT CAST(2 AS BIGINT) AS id UNION ALL SELECT CAST(99 AS BIGINT)) AS s",
        "t.id = s.id",
        Some(matched_predicate),
        Some(insert_predicate),
        &[col("id")],
        &[("id", "s.id")],
        &[("id", "s.id")],
        matched_action,
        insert,
    );
    let sql = novarocks_parser::printer::print_query(&query).unwrap();
    let control = SqlCompileControl::unbounded();
    let request = SqlFinalPlanCompileRequest::new(
        PlanVersionId::try_new([61; 16]).unwrap(),
        SqlStatementInput::parsed_query(Box::new(query)),
        SqlCompileIntent::Query,
        SqlSessionContext {
            current_catalog: None,
            current_database: "default_database".into(),
            optimizer_settings: dml_change_stream_optimizer_settings(),
            sql_semantics: Default::default(),
        },
        SqlPlanningEnvironment::Distributed,
        novarocks_sql::compiler::builtin_sql_function_catalog().snapshot(),
        novarocks_functions::testkit::shared_installed_builtin_owner_catalogue(),
        crate::application::test_constant_policy(),
        control.clone(),
        PipelineDopDomain {
            min: 1,
            max: 1,
            requires_power_of_two: false,
        },
        DEFAULT_COMPLETION_LIMITS,
    );
    SqlCompiler::start(request.try_into_completion().unwrap(), &control)
        .unwrap_or_else(|error| panic!("MERGE match completion: {error:#?}\n{sql}"))
        .into_complete()
        .expect("literal relations need no external facts")
        .into_plan()
}

// Follow only value bindings and imports, never reconstruct a SQL expression
// from its display name. This finds the actual selected-action expression.
fn selected_action_author(plan: &PhysicalPlan) -> (FragmentId, NodeId, RootOrdinal) {
    let result = plan.result_port().unwrap();
    let field = result
        .fields
        .iter()
        .find(|field| field.alias.as_deref().unwrap_or(&field.name) == "__nr_merge_action")
        .unwrap();
    let mut fragment = result.fragment;
    let mut value: ValueId = field.value;
    loop {
        let owner = &plan.fragments()[&fragment];
        match &owner.values()[&value].origin {
            ValueOrigin::Expr { node, root } => {
                if let ExprKind::Value(next) = owner.root_expression(*node, *root).unwrap().kind {
                    value = next;
                } else {
                    return (fragment, *node, *root);
                }
            }
            ValueOrigin::ExchangeImport { edge, source_value } => {
                fragment = plan.edges()[edge].source.fragment;
                value = *source_value;
            }
            other => panic!("selected action must have an expression author, got {other:?}"),
        }
    }
}

fn expression_occurrences(unit: &BoundExprSet, root: ExprId) -> Vec<ExprId> {
    fn visit(unit: &BoundExprSet, id: ExprId, out: &mut Vec<ExprId>) {
        out.push(id);
        unit.definition(id)
            .unwrap()
            .kind
            .invocation_references_observed(|child| {
                visit(unit, child, out);
                Ok::<_, std::convert::Infallible>(())
            })
            .unwrap();
    }
    let mut occurrences = Vec::new();
    visit(unit, root, &mut occurrences);
    occurrences
}

fn is_rand(kind: &ExprKind) -> bool {
    matches!(kind, ExprKind::FunctionCall { function, .. }
        if function.function_id.as_str() == "builtin.scalar/rand/v1")
}

#[test]
fn volatile_when_has_one_physical_author_per_clause() {
    for matched in [MERGE_ACTION_MATCHED_UPDATE, MERGE_ACTION_MATCHED_DELETE] {
        let authored =
            compile_selected_action(Some(matched), "rand(17) > 0.5", "rand(29) > 0.5", true);
        let plan = authored.plan();
        let (fragment, owner, action) = selected_action_author(plan);
        let unit = plan.fragments()[&fragment].unit(owner).unwrap();
        let action_occurrences =
            expression_occurrences(unit, unit.root(action).unwrap().definition);
        assert_eq!(
            action_occurrences
                .iter()
                .filter(|id| is_rand(&unit.definition(**id).unwrap().kind))
                .count(),
            2
        );

        let mut authors = Vec::new();
        for (fragment_id, fragment) in plan.fragments() {
            assert!(
                fragment
                    .nodes()
                    .values()
                    .all(|node| !matches!(node.kind, NodeKind::Window(_)))
            );
            for node in fragment.nodes().values() {
                if let NodeKind::Project { expressions } = &node.kind {
                    for (root, _) in expressions {
                        let unit = node.expressions.as_deref().unwrap();
                        let calls =
                            expression_occurrences(unit, unit.root(*root).unwrap().definition)
                                .iter()
                                .filter(|id| is_rand(&unit.definition(**id).unwrap().kind))
                                .count();
                        if calls != 0 {
                            authors.push((*fragment_id, node.id, *root, calls));
                        }
                    }
                }
            }
        }
        assert_eq!(
            authors,
            vec![(fragment, owner, action, 2)],
            "only the materialized action may own WHEN calls"
        );
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum SelectedScalar {
    Null,
    Boolean(bool),
    Integer(i64),
}

fn scalar(plan: &PhysicalPlan, unit: &BoundExprSet, expr: ExprId) -> Option<SelectedScalar> {
    use arrow::array::Array;
    let expr = unit.definition(expr).unwrap();
    match &expr.kind {
        ExprKind::Cast {
            expr: source,
            target,
            ..
        } => {
            // CASE can widen a constant's NULL declaration without changing
            // its value, logical domain or carrier. Follow only that exact
            // binding; this reader never executes a value conversion.
            let input = unit.definition(*source).unwrap();
            if !input.ty.nullable()
                && expr.ty.nullable()
                && input.ty.data_type() == target
                && input.ty.with_nullable(true).ok().as_ref() == Some(&expr.ty)
            {
                scalar(plan, unit, *source)
            } else {
                None
            }
        }
        ExprKind::Constant(reference) => {
            let array = plan.constants().entries()[&reference.pool].array();
            let row = reference.ordinal as usize;
            if array.is_null(row) {
                return Some(SelectedScalar::Null);
            }
            macro_rules! integer {
                ($array:ty) => {
                    if let Some(array) = array.as_any().downcast_ref::<$array>() {
                        return Some(SelectedScalar::Integer(i64::from(array.value(row))));
                    }
                };
            }
            integer!(arrow::array::Int8Array);
            integer!(arrow::array::Int16Array);
            integer!(arrow::array::Int32Array);
            integer!(arrow::array::Int64Array);
            array
                .as_any()
                .downcast_ref::<arrow::array::BooleanArray>()
                .map(|array| SelectedScalar::Boolean(array.value(row)))
        }
        _ => None,
    }
}

#[test]
fn literal_when_matrix_retains_truth_only_case_and_exact_action_tags() {
    // FALSE and UNKNOWN must never select a mutation. Inspect the compiler's
    // actual CASE/constant structure rather than interpreting it in this test.
    for matched in [
        None,
        Some(MERGE_ACTION_MATCHED_UPDATE),
        Some(MERGE_ACTION_MATCHED_DELETE),
    ] {
        for (predicate, rejected_guard) in [
            ("TRUE", None),
            ("FALSE", Some(SelectedScalar::Boolean(false))),
            ("CAST(NULL AS BOOLEAN)", Some(SelectedScalar::Null)),
        ] {
            let authored = compile_selected_action(matched, predicate, predicate, true);
            let plan = authored.plan();
            let (fragment, owner, root) = selected_action_author(plan);
            let unit = plan.fragments()[&fragment].unit(owner).unwrap();
            let action = unit.root(root).unwrap().definition;
            if let Some(value) = scalar(plan, unit, action) {
                assert_eq!(
                    value,
                    SelectedScalar::Integer(0),
                    "{matched:?}, {predicate}"
                );
                assert!(rejected_guard.is_some());
                continue;
            }
            let ExprKind::Case {
                operand,
                when_then,
                else_expr,
            } = &unit.definition(action).unwrap().kind
            else {
                panic!(
                    "selected action must be CASE or folded NoAction: {:?}",
                    unit.definition(action)
                );
            };
            assert!(operand.is_none(), "searched CASE consumes Boolean truth");
            assert_eq!(
                scalar(plan, unit, else_expr.unwrap()),
                Some(SelectedScalar::Integer(0)),
                "closed ELSE subtree ({matched:?}, {predicate}): {:?}",
                expression_occurrences(unit, else_expr.unwrap())
                    .iter()
                    .map(|id| unit.definition(*id))
                    .collect::<Vec<_>>()
            );
            let mut expected = matched.into_iter().map(i64::from).collect::<Vec<_>>();
            expected.push(3);
            let actual = when_then
                .iter()
                .map(|(_, then)| scalar(plan, unit, *then))
                .collect::<Vec<_>>();
            assert_eq!(
                actual,
                expected
                    .into_iter()
                    .map(|tag| Some(SelectedScalar::Integer(tag)))
                    .collect::<Vec<_>>()
            );
            if let Some(rejected_guard) = rejected_guard {
                for (guard, _) in when_then {
                    assert!(
                        expression_occurrences(unit, *guard).iter().any(|id| scalar(
                            plan, unit, *id
                        ) == Some(
                            rejected_guard
                        )),
                        "FALSE/UNKNOWN must remain in each clause guard: {predicate}"
                    );
                }
            }
        }
    }
}

#[test]
fn insert_only_checks_matched_target_positions_without_synthetic_identity() {
    let assertion = mor_merge_pre_expand_assertion();
    assert_eq!(
        assertion.key_columns,
        vec![
            ("__nr_file".into(), "_file".into()),
            ("__nr_pos".into(), "_pos".into()),
            ("__nr_merge_action".into(), "__nr_merge_action".into()),
        ]
    );
    assert!(
        assertion.applicability_output_ordinal.is_none(),
        "SQL owns the matched-file scope ordinal"
    );
    let authored = compile_selected_action(None, "TRUE", "TRUE", true);
    let plan = authored.plan();
    let fields = &plan.result_port().unwrap().fields;
    let identity = fields
        .iter()
        .find(|field| field.alias.as_deref().unwrap_or(&field.name) == "__nr_row_id")
        .unwrap();
    assert_eq!(identity.ty.data_type(), &DataType::Int64);
    assert!(
        identity.ty.nullable(),
        "unmatched rows retain their actual NULL target identity"
    );
    assert!(
        plan.fragments()
            .values()
            .flat_map(|fragment| fragment.nodes().values())
            .all(|node| !matches!(node.kind, NodeKind::Window(_)))
    );
}
