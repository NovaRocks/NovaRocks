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

//! Actual SQL/analyzer/optimizer regressions with semantic property oracles.
use super::*;
use crate::common::{BinOp, LiteralValue};
use crate::optimizer::{
    OptimizedOperatorNode,
    operator::Operator,
    scalar::{HashableLiteral, ScalarArena, ScalarId, ScalarNode},
};
use arrow::datatypes::DataType;
use novarocks_type_contract::ValueLogicalType;

fn compile(
    score_type: DataType,
    predicate: &str,
    mode: SqlPhysicalEmissionMode,
) -> SqlOptimizedOutput {
    let mut catalog = crate::planning::catalog::PlannerMemoryCatalog::default();
    catalog.create_database("db").unwrap();
    let mut evidence = Vec::new();
    for name in ["residual_rng_l", "residual_rng_r"] {
        let source = crate::planner::table::test_sql_scan_source(
            crate::planner::table::SqlScanKind::ConnectorRead,
        );
        let crate::planner::table::ScanSource::Sql(scan) = &source else {
            unreachable!()
        };
        evidence.push(crate::planning::dml::DmlStatisticsEvidence::Missing {
            binding: scan.binding,
            label: format!(
                "{}.{}.{}",
                scan.table.catalog, scan.table.namespace, scan.table.table
            ),
            reason: "independent fixture has no published statistics".into(),
        });
        catalog
            .register(
                "db",
                crate::planner::table::TableDef {
                    name: name.into(),
                    columns: ["k", "score", "payload"]
                        .into_iter()
                        .map(|name| novarocks_types::schema::ColumnDef {
                            name: name.into(),
                            data_type: if name == "score" {
                                score_type.clone()
                            } else {
                                DataType::Int32
                            },
                            nullable: true,
                            write_default: None,
                            logical_type: None,
                        })
                        .collect(),
                    iceberg_row_lineage_metadata_columns: Vec::new(),
                    source,
                },
            )
            .unwrap();
    }
    let statistics = crate::planning::dml::DmlStatisticsSnapshot::from_evidence(evidence);
    let snapshot = SqlPlannerTableSnapshot::new(&catalog);
    let control = SqlCompileControl::unbounded();
    let sql = format!(
        "SELECT l.payload, r.payload FROM residual_rng_l l JOIN residual_rng_r r ON l.k=r.k WHERE {predicate}"
    );
    let analyzed = SqlCompiler::analyze(SqlAnalyzeRequest::new(
        SqlStatementInput::sql(sql),
        SqlCompileIntent::Query,
        SqlSessionContext {
            sql_semantics: Default::default(),
            current_catalog: None,
            current_database: "db".into(),
            optimizer_settings: SessionOptimizerSettings {
                enable_materialized_view_rewrite: Some(false),
                enable_common_subexpr_reuse: Some(false),
                ..Default::default()
            },
        },
        SqlPlanningEnvironment::Distributed,
        &snapshot,
        crate::functions::builtin_sql_function_catalog(),
        noop_constant_evaluator(),
        None,
        crate::constant::test_constant_policy(),
        mode,
        control.clone(),
    ))
    .unwrap()
    .into_pending()
    .unwrap();
    SqlCompiler::optimize(SqlOptimizeRequest::new(analyzed, &statistics, control))
        .unwrap()
        .into_optimized_output()
        .unwrap()
}
const ORIGINAL: &str = "(l.score=r.score AND l.score BETWEEN 10 AND 20) OR (l.score=r.score AND l.score BETWEEN 30 AND 40)";

fn integer(arena: &ScalarArena, id: ScalarId) -> Option<i64> {
    match arena.node(id) {
        ScalarNode::Literal(HashableLiteral(LiteralValue::Int(value))) => Some(*value),
        ScalarNode::Constant(value)
            if value.value_type().logical_type() == ValueLogicalType::Physical =>
        {
            value.try_i64().ok().flatten()
        }
        _ => None,
    }
}
fn unwrap_nested(arena: &ScalarArena, mut id: ScalarId) -> ScalarId {
    while let ScalarNode::Nested(inner) = arena.node(id) {
        id = *inner;
    }
    id
}
fn score_origin(arena: &ScalarArena, id: ScalarId) -> Option<(String, Vec<(DataType, DataType)>)> {
    let id = unwrap_nested(arena, id);
    match arena.node(id) {
        ScalarNode::ColumnRef(column) => {
            let display = arena.column_display(*column)?;
            (display.column == "score").then(|| (display.qualifier.clone().unwrap(), Vec::new()))
        }
        ScalarNode::Cast { child, target, .. } => {
            let (alias, mut casts) = score_origin(arena, *child)?;
            casts.push((arena.data_type(*child).clone(), target.clone()));
            Some((alias, casts))
        }
        _ => None,
    }
}
fn interval(
    arena: &ScalarArena,
    id: ScalarId,
) -> Option<(String, Vec<(DataType, DataType)>, i64, i64)> {
    let ScalarNode::Between {
        child,
        low,
        high,
        negated: false,
    } = arena.node(unwrap_nested(arena, id))
    else {
        return None;
    };
    let (alias, casts) = score_origin(arena, *child)?;
    Some((alias, casts, integer(arena, *low)?, integer(arena, *high)?))
}
fn visit(node: &OptimizedOperatorNode, run: &mut impl FnMut(&OptimizedOperatorNode, &ScalarArena)) {
    run(
        node,
        node.execution_props
            .scalar_arena
            .as_ref()
            .expect("real optimizer scalar arena"),
    );
    for child in &node.children {
        visit(child, run);
    }
}
fn scan_envelopes(tree: &OptimizedOperatorNode) -> Vec<(String, Vec<(DataType, DataType)>)> {
    let mut found = Vec::new();
    visit(tree, &mut |node, arena| {
        let Operator::PhysicalScan(scan) = &node.op else {
            return;
        };
        for &predicate in &scan.predicates {
            let mut conjuncts = Vec::new();
            crate::optimizer::scalar_expr::split_conjuncts(arena, predicate, &mut conjuncts);
            for id in conjuncts {
                if let Some((alias, casts, 10, 40)) = interval(arena, id) {
                    assert_eq!(
                        scan.alias.as_deref(),
                        Some(alias.as_str()),
                        "envelope remains on its own scan axis"
                    );
                    found.push((alias, casts));
                }
            }
        }
    });
    found.sort_by(|left, right| left.0.cmp(&right.0));
    found.dedup();
    found
}
fn top_level_scan_interval_count(tree: &OptimizedOperatorNode) -> usize {
    let mut count = 0;
    visit(tree, &mut |node, arena| {
        let Operator::PhysicalScan(scan) = &node.op else {
            return;
        };
        for &predicate in &scan.predicates {
            let mut terms = Vec::new();
            crate::optimizer::scalar_expr::split_conjuncts(arena, predicate, &mut terms);
            count += terms
                .iter()
                .filter(|id| {
                    matches!(
                        arena.node(unwrap_nested(arena, **id)),
                        ScalarNode::Between { negated: false, .. }
                    )
                })
                .count();
        }
    });
    count
}
fn retains_original_interval_union(tree: &OptimizedOperatorNode) -> bool {
    let mut found = false;
    visit(tree, &mut |node, arena| {
        let predicates = match &node.op {
            Operator::PhysicalScan(scan) => scan.predicates.clone(),
            Operator::PhysicalFilter(filter) => vec![filter.predicate],
            Operator::PhysicalHashJoin(join) => join.other_condition.into_iter().collect(),
            _ => Vec::new(),
        };
        for predicate in predicates {
            let mut conjuncts = Vec::new();
            crate::optimizer::scalar_expr::split_conjuncts(arena, predicate, &mut conjuncts);
            for conjunct in conjuncts {
                if !matches!(
                    arena.node(unwrap_nested(arena, conjunct)),
                    ScalarNode::BinaryOp { op: BinOp::Or, .. }
                ) {
                    continue;
                }
                let mut branches = Vec::new();
                crate::optimizer::scalar_expr::split_disjuncts(arena, conjunct, &mut branches);
                if branches.len() != 2 {
                    continue;
                }
                let intervals: Vec<_> = branches
                    .into_iter()
                    .filter_map(|branch| {
                        let mut terms = Vec::new();
                        crate::optimizer::scalar_expr::split_conjuncts(arena, branch, &mut terms);
                        terms.into_iter().find_map(|term| interval(arena, term))
                    })
                    .collect();
                let mut bounds: Vec<_> = intervals.iter().map(|item| (item.2, item.3)).collect();
                bounds.sort();
                found |= intervals.len() == 2
                    && intervals.iter().all(|item| item.0 == "l")
                    && bounds == vec![(10, 20), (30, 40)];
            }
        }
    });
    found
}
fn retains_residual_or(tree: &OptimizedOperatorNode) -> bool {
    let mut found = false;
    visit(tree, &mut |node, arena| {
        let predicates = match &node.op {
            Operator::PhysicalScan(scan) => scan.predicates.clone(),
            Operator::PhysicalFilter(filter) => vec![filter.predicate],
            Operator::PhysicalHashJoin(join) => join.other_condition.into_iter().collect(),
            _ => Vec::new(),
        };
        for predicate in predicates {
            let mut terms = Vec::new();
            crate::optimizer::scalar_expr::split_conjuncts(arena, predicate, &mut terms);
            found |= terms.iter().any(|id| {
                matches!(
                    arena.node(unwrap_nested(arena, *id)),
                    ScalarNode::BinaryOp { op: BinOp::Or, .. }
                )
            });
        }
    });
    found
}
fn retains_score_join_equality(tree: &OptimizedOperatorNode) -> bool {
    let mut found = false;
    visit(tree, &mut |node, arena| {
        let Operator::PhysicalHashJoin(join) = &node.op else {
            return;
        };
        found |= join.eq_conditions.iter().any(|condition| {
            match (
                score_origin(arena, condition.left),
                score_origin(arena, condition.right),
            ) {
                (Some((left, _)), Some((right, _))) => {
                    left != right
                        && [left.as_str(), right.as_str()]
                            .iter()
                            .all(|alias| ["l", "r"].contains(alias))
                }
                _ => false,
            }
        });
    });
    found
}
#[test]
fn real_int32_sql_keeps_original_or_and_both_exact_widened_range_envelopes() {
    for mode in [
        SqlPhysicalEmissionMode::OriginalNativeV1,
        SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration,
    ] {
        let output = compile(DataType::Int32, ORIGINAL, mode);
        let tree = &output.optimized_tree;
        assert!(
            retains_original_interval_union(tree),
            "10..20 OR 30..40 must survive; 10..40 alone accepts the gap"
        );
        assert!(
            retains_score_join_equality(tree),
            "score equality must survive envelope derivation"
        );
        assert_eq!(
            scan_envelopes(tree),
            vec![
                ("l".into(), vec![(DataType::Int32, DataType::Int64)]),
                ("r".into(), vec![(DataType::Int32, DataType::Int64)]),
            ],
            "both axes need necessary 10..40 predicates, retaining the exact comparison cast"
        );
    }
}
#[test]
fn real_int64_sql_direct_domain_control_keeps_original_or_and_both_envelopes() {
    let output = compile(
        DataType::Int64,
        ORIGINAL,
        SqlPhysicalEmissionMode::OriginalNativeV1,
    );
    assert!(retains_original_interval_union(&output.optimized_tree));
    assert!(retains_score_join_equality(&output.optimized_tree));
    assert_eq!(
        scan_envelopes(&output.optimized_tree),
        vec![("l".into(), vec![]), ("r".into(), vec![])]
    );
}
#[test]
fn real_sql_does_not_derive_range_envelopes_through_unproven_cast_domains() {
    for target in ["SMALLINT", "DOUBLE", "DECIMAL(12,2)"] {
        let predicate = format!(
            "(l.score=r.score AND CAST(l.score AS {target}) BETWEEN 10 AND 20) OR (l.score=r.score AND CAST(l.score AS {target}) BETWEEN 30 AND 40)"
        );
        let output = compile(
            DataType::Int64,
            &predicate,
            SqlPhysicalEmissionMode::OriginalNativeV1,
        );
        assert!(
            retains_residual_or(&output.optimized_tree),
            "original OR remains for {target}"
        );
        assert_eq!(
            top_level_scan_interval_count(&output.optimized_tree),
            0,
            "no new standalone scan interval through unproven {target}; do not hide a float/decimal envelope behind a signed-only oracle"
        );
    }
}
