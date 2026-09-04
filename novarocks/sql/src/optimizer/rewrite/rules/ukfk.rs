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

//! UK/FK-based logical rewrites over frozen SQL table facts.

use std::collections::{HashMap, HashSet};

use arrow::datatypes::DataType;

use crate::column_id::ColumnId;
use crate::common::{BinOp, JoinKind};
use crate::optimizer::operator::{
    FilterOp, LogicalJoinOp, Operator, ProjectOp, ScalarAggregateSpec, ScalarProjectItem, ScanOp,
};
use crate::optimizer::opt_expr::OptExpr;
use crate::optimizer::pattern::{OpKind, Pattern};
use crate::optimizer::rewrite::context::RewriteContext;
use crate::optimizer::rewrite::phase::RewritePhase;
use crate::optimizer::rewrite::result::RewriteResult;
use crate::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::optimizer::rewrite::rules::utils::collect_output_ids_opt;
use crate::optimizer::scalar::{ScalarArena, ScalarId, ScalarNode};
use crate::optimizer::scalar_expr;
use crate::planner::table::{ScanSource, SqlUkFkTableFacts};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Side {
    Left,
    Right,
}

pub(crate) struct PruneUkFkJoin;

impl LogicalRewriteRule for PruneUkFkJoin {
    fn name(&self) -> &'static str {
        "PruneUkFkJoin"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn pattern(&self) -> Pattern {
        Pattern::Op {
            kind: OpKind::Project,
            children: vec![Pattern::Op {
                kind: OpKind::Join,
                children: vec![Pattern::MultiLeaf],
            }],
        }
    }

    fn matches(&self, _expr: &OptExpr, _ctx: &RewriteContext) -> bool {
        true
    }

    fn apply(&self, expr: OptExpr, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let settings = ctx.session_settings();
        let table_prune_enabled = settings.enable_query_rewrite_table_prune
            || settings.enable_cbo_table_prune
            || settings.enable_table_prune_on_update;
        if !table_prune_enabled && !settings.enable_ukfk_opt {
            return Ok(RewriteResult::Unchanged);
        }

        let OptExpr {
            op,
            mut children,
            required_output_columns,
        } = expr;
        let Operator::LogicalProject(project) = op else {
            return Ok(RewriteResult::Unchanged);
        };
        if children.len() != 1 {
            return Ok(RewriteResult::Unchanged);
        }
        let join_expr = children.remove(0);
        let OptExpr {
            op: join_op,
            children: mut join_children,
            required_output_columns: _,
        } = join_expr;
        let Operator::LogicalJoin(join) = join_op else {
            return Ok(RewriteResult::Unchanged);
        };
        if join_children.len() != 2 {
            return Ok(RewriteResult::Unchanged);
        }
        let right = join_children.remove(1);
        let left = join_children.remove(0);

        let arena_rc = ctx.scalar_arena();

        let retained_side =
            match project_referenced_side(&project.items, &left, &right, &arena_rc.borrow())? {
                Some(s) => s,
                None => return Ok(RewriteResult::Unchanged),
            };
        let eq_pairs = join_equality_pairs(&join, &left, &right, &arena_rc.borrow())?;
        if eq_pairs.is_empty() {
            return Ok(RewriteResult::Unchanged);
        }
        let left_cols: Vec<String> = eq_pairs.iter().map(|(left, _)| left.clone()).collect();
        let right_cols: Vec<String> = eq_pairs.iter().map(|(_, right)| right.clone()).collect();
        let left_scan = root_scan(&left);
        let right_scan = root_scan(&right);
        let (Some(left_scan), Some(right_scan)) = (left_scan, right_scan) else {
            return Ok(RewriteResult::Unchanged);
        };

        let retained = match (join.join_type, retained_side) {
            (JoinKind::LeftOuter, Side::Left)
                if table_prune_enabled && table_has_unique_key(right_scan, &right_cols) =>
            {
                Some(left.clone())
            }
            (JoinKind::RightOuter, Side::Right)
                if table_prune_enabled && table_has_unique_key(left_scan, &left_cols) =>
            {
                Some(right.clone())
            }
            (JoinKind::Inner, Side::Left)
                if settings.enable_ukfk_opt
                    && foreign_key_matches(left_scan, right_scan, &left_cols, &right_cols) =>
            {
                Some(add_not_null_filter(
                    left.clone(),
                    left_scan,
                    &left_cols,
                    &mut arena_rc.borrow_mut(),
                ))
            }
            (JoinKind::Inner, Side::Right)
                if settings.enable_ukfk_opt
                    && foreign_key_matches(right_scan, left_scan, &right_cols, &left_cols) =>
            {
                Some(add_not_null_filter(
                    right.clone(),
                    right_scan,
                    &right_cols,
                    &mut arena_rc.borrow_mut(),
                ))
            }
            _ => None,
        };
        let Some(retained) = retained else {
            return Ok(RewriteResult::Unchanged);
        };

        Ok(RewriteResult::Changed(OptExpr {
            op: Operator::LogicalProject(project),
            children: vec![retained],
            required_output_columns,
        }))
    }
}

pub(crate) struct EliminateUniqueAggregate;

impl LogicalRewriteRule for EliminateUniqueAggregate {
    fn name(&self) -> &'static str {
        "EliminateUniqueAggregate"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn pattern(&self) -> Pattern {
        Pattern::Op {
            kind: OpKind::Project,
            children: vec![Pattern::Op {
                kind: OpKind::Aggregate,
                children: vec![Pattern::MultiLeaf],
            }],
        }
    }

    fn matches(&self, _expr: &OptExpr, _ctx: &RewriteContext) -> bool {
        true
    }

    fn apply(&self, expr: OptExpr, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let settings = ctx.session_settings();
        if !settings.enable_eliminate_agg {
            return Ok(RewriteResult::Unchanged);
        }

        let OptExpr {
            op,
            mut children,
            required_output_columns,
        } = expr;
        let Operator::LogicalProject(project) = op else {
            return Ok(RewriteResult::Unchanged);
        };
        if children.len() != 1 {
            return Ok(RewriteResult::Unchanged);
        }
        let aggregate_expr = children.remove(0);
        let OptExpr {
            op: agg_op,
            children: mut agg_children,
            required_output_columns: _,
        } = aggregate_expr;
        let Operator::LogicalAggregate(aggregate) = agg_op else {
            return Ok(RewriteResult::Unchanged);
        };
        if agg_children.len() != 1 {
            return Ok(RewriteResult::Unchanged);
        }
        let aggregate_input = agg_children.remove(0);
        let scan = match root_scan(&aggregate_input) {
            Some(s) => s,
            None => return Ok(RewriteResult::Unchanged),
        };

        let arena_rc = ctx.scalar_arena();
        let group_columns = match group_by_columns(&aggregate.group_by, scan, &arena_rc.borrow()) {
            Some(cols) => cols,
            None => return Ok(RewriteResult::Unchanged),
        };
        if group_columns.is_empty() || !table_has_unique_key(scan, &group_columns) {
            return Ok(RewriteResult::Unchanged);
        }
        if aggregate.aggregates.is_empty()
            || !aggregate
                .aggregates
                .iter()
                .all(|a| is_eliminable_count(a, &arena_rc.borrow()))
        {
            return Ok(RewriteResult::Unchanged);
        }
        let eliminated_count_outputs: HashSet<ColumnId> = aggregate
            .aggregates
            .iter()
            .map(|aggregate| aggregate.output_column_id)
            .filter(|id| *id != ColumnId::UNSET)
            .collect();
        let items = project
            .items
            .into_iter()
            .map(|item| {
                rewrite_eliminated_aggregate_project_item(
                    item,
                    &eliminated_count_outputs,
                    &mut arena_rc.borrow_mut(),
                )
            })
            .collect::<Option<Vec<_>>>();
        let Some(items) = items else {
            return Ok(RewriteResult::Unchanged);
        };

        Ok(RewriteResult::Changed(OptExpr {
            op: Operator::LogicalProject(ProjectOp {
                items,
                output_qualifier: project.output_qualifier,
            }),
            children: vec![aggregate_input],
            required_output_columns,
        }))
    }
}

fn root_scan(expr: &OptExpr) -> Option<&ScanOp> {
    match &expr.op {
        Operator::LogicalScan(scan) => Some(scan),
        Operator::LogicalFilter(_) => root_scan(expr.unary_input()),
        _ => None,
    }
}

fn project_referenced_side(
    items: &[ScalarProjectItem],
    left: &OptExpr,
    right: &OptExpr,
    arena: &ScalarArena,
) -> Result<Option<Side>, String> {
    let mut left_ids = collect_output_ids_opt(left);
    let mut right_ids = collect_output_ids_opt(right);
    left_ids.remove(&ColumnId::UNSET);
    right_ids.remove(&ColumnId::UNSET);
    let mut side = None;
    for item in items {
        let ids = match scalar_expr::collect_column_ids_strict(arena, item.expr) {
            Some(ids) => ids,
            None => return Ok(None),
        };
        if ids.is_empty() {
            continue;
        }
        let reference_side = match referenced_side(&ids, &left_ids, &right_ids) {
            Some(s) => s,
            None => return Ok(None),
        };
        if let Some(existing) = side {
            if existing != reference_side {
                return Ok(None);
            }
        } else {
            side = Some(reference_side);
        }
    }
    Ok(side)
}

fn join_equality_pairs(
    join: &LogicalJoinOp,
    left: &OptExpr,
    right: &OptExpr,
    arena: &ScalarArena,
) -> Result<Vec<(String, String)>, String> {
    let Some(cond_id) = join.condition else {
        return Ok(vec![]);
    };
    let mut left_ids = collect_output_ids_opt(left);
    let mut right_ids = collect_output_ids_opt(right);
    left_ids.remove(&ColumnId::UNSET);
    right_ids.remove(&ColumnId::UNSET);
    let left_names = output_column_name_map(left);
    let right_names = output_column_name_map(right);
    let mut pairs = Vec::new();
    let ok = collect_join_equality_pairs(
        arena,
        cond_id,
        &left_ids,
        &right_ids,
        &left_names,
        &right_names,
        &mut pairs,
    );
    if ok.is_some() && !pairs.is_empty() {
        Ok(pairs)
    } else {
        Ok(vec![])
    }
}

fn collect_join_equality_pairs(
    arena: &ScalarArena,
    expr: ScalarId,
    left_ids: &HashSet<ColumnId>,
    right_ids: &HashSet<ColumnId>,
    left_names: &HashMap<ColumnId, String>,
    right_names: &HashMap<ColumnId, String>,
    pairs: &mut Vec<(String, String)>,
) -> Option<()> {
    match arena.node(expr) {
        ScalarNode::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            collect_join_equality_pairs(
                arena,
                *left,
                left_ids,
                right_ids,
                left_names,
                right_names,
                pairs,
            )?;
            collect_join_equality_pairs(
                arena,
                *right,
                left_ids,
                right_ids,
                left_names,
                right_names,
                pairs,
            )
        }
        ScalarNode::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
        } => {
            let left_ref =
                classify_column_ref(arena, *left, left_ids, right_ids, left_names, right_names)?;
            let right_ref =
                classify_column_ref(arena, *right, left_ids, right_ids, left_names, right_names)?;
            match (left_ref, right_ref) {
                ((Side::Left, left_col), (Side::Right, right_col)) => {
                    pairs.push((left_col, right_col));
                    Some(())
                }
                ((Side::Right, right_col), (Side::Left, left_col)) => {
                    pairs.push((left_col, right_col));
                    Some(())
                }
                _ => None,
            }
        }
        _ => None,
    }
}

fn referenced_side(
    id_refs: &HashSet<ColumnId>,
    left_ids: &HashSet<ColumnId>,
    right_ids: &HashSet<ColumnId>,
) -> Option<Side> {
    let mut side = None;
    for id in id_refs {
        let reference_side = match (left_ids.contains(id), right_ids.contains(id)) {
            (true, false) => Side::Left,
            (false, true) => Side::Right,
            _ => return None,
        };
        if let Some(existing) = side {
            if existing != reference_side {
                return None;
            }
        } else {
            side = Some(reference_side);
        }
    }
    side
}

fn classify_column_ref(
    arena: &ScalarArena,
    expr: ScalarId,
    left_ids: &HashSet<ColumnId>,
    right_ids: &HashSet<ColumnId>,
    left_names: &HashMap<ColumnId, String>,
    right_names: &HashMap<ColumnId, String>,
) -> Option<(Side, String)> {
    match arena.node(expr) {
        ScalarNode::ColumnRef(column_id) => {
            if *column_id == ColumnId::UNSET {
                return None;
            }
            match (left_ids.contains(column_id), right_ids.contains(column_id)) {
                (true, false) => column_name_for_id(arena, *column_id, left_names)
                    .map(|column| (Side::Left, column)),
                (false, true) => column_name_for_id(arena, *column_id, right_names)
                    .map(|column| (Side::Right, column)),
                _ => None,
            }
        }
        ScalarNode::Cast { child, .. } | ScalarNode::Nested(child) => {
            classify_column_ref(arena, *child, left_ids, right_ids, left_names, right_names)
        }
        _ => None,
    }
}

fn output_column_name_map(expr: &OptExpr) -> HashMap<ColumnId, String> {
    match &expr.op {
        Operator::LogicalScan(scan) => scan
            .columns
            .iter()
            .filter(|column| column.column_id != ColumnId::UNSET)
            .map(|column| (column.column_id, normalize_identifier(&column.name)))
            .collect(),
        Operator::LogicalFilter(_) => output_column_name_map(expr.unary_input()),
        Operator::LogicalProject(project) => project
            .items
            .iter()
            .filter(|item| item.output_column_id != ColumnId::UNSET)
            .map(|item| {
                (
                    item.output_column_id,
                    normalize_identifier(&item.output_name),
                )
            })
            .collect(),
        Operator::LogicalAggregate(aggregate) => aggregate
            .output_columns
            .iter()
            .filter(|column| column.column_id != ColumnId::UNSET)
            .map(|column| (column.column_id, normalize_identifier(&column.name)))
            .collect(),
        Operator::LogicalWindow(window) => window
            .output_columns
            .iter()
            .filter(|column| column.column_id != ColumnId::UNSET)
            .map(|column| (column.column_id, normalize_identifier(&column.name)))
            .collect(),
        _ => HashMap::new(),
    }
}

fn column_name_for_id(
    arena: &ScalarArena,
    column_id: ColumnId,
    names: &HashMap<ColumnId, String>,
) -> Option<String> {
    names.get(&column_id).cloned().or_else(|| {
        arena
            .column_display(column_id)
            .map(|d| normalize_identifier(&d.column))
    })
}

fn group_by_columns(
    group_by: &[ScalarId],
    scan: &ScanOp,
    arena: &ScalarArena,
) -> Option<Vec<String>> {
    group_by
        .iter()
        .map(|id| {
            let column_id = scalar_expr::column_id(arena, *id)?;
            scan.columns
                .iter()
                .find(|column| column.column_id == column_id)
                .map(|column| normalize_identifier(&column.name))
        })
        .collect()
}

fn table_has_unique_key(scan: &ScanOp, columns: &[String]) -> bool {
    sql_ukfk_facts(scan).is_some_and(|facts| facts.has_unique_key(columns))
}

fn foreign_key_matches(
    local_scan: &ScanOp,
    referenced_scan: &ScanOp,
    local_columns: &[String],
    referenced_columns: &[String],
) -> bool {
    let (Some(local_facts), Some(referenced_facts)) =
        (sql_ukfk_facts(local_scan), sql_ukfk_facts(referenced_scan))
    else {
        return false;
    };
    referenced_facts.has_unique_key(referenced_columns)
        && local_facts.has_matching_foreign_key(
            local_columns,
            &referenced_scan.table.name,
            referenced_scan.alias.as_deref(),
            referenced_columns,
        )
}

fn sql_ukfk_facts(scan: &ScanOp) -> Option<&SqlUkFkTableFacts> {
    let ScanSource::Sql(source) = &scan.table.source;
    Some(source.ukfk_facts())
}

fn normalize_identifier(raw: &str) -> String {
    let trimmed = raw
        .trim()
        .trim_matches('`')
        .trim_matches('"')
        .trim_matches('\'');
    let leaf = trimmed.rsplit('.').next().unwrap_or(trimmed);
    leaf.trim()
        .trim_matches('`')
        .trim_matches('"')
        .trim_matches('\'')
        .to_ascii_lowercase()
}

fn add_not_null_filter(
    plan: OptExpr,
    scan: &ScanOp,
    columns: &[String],
    arena: &mut ScalarArena,
) -> OptExpr {
    let qualifier = scan
        .alias
        .clone()
        .unwrap_or_else(|| scan.table.name.clone());
    let mut predicates = Vec::new();
    for column in columns {
        let Some(output) = scan
            .columns
            .iter()
            .find(|candidate| candidate.name.eq_ignore_ascii_case(column))
            .filter(|output| output.column_id != ColumnId::UNSET)
        else {
            continue;
        };
        arena.remember_source_column_display(
            output.column_id,
            Some(qualifier.clone()),
            output.name.clone(),
        );
        let child = arena.intern(
            ScalarNode::ColumnRef(output.column_id),
            output.data_type.clone(),
            output.nullable,
        );
        predicates.push(arena.intern(
            ScalarNode::IsNull {
                child,
                negated: true,
            },
            DataType::Boolean,
            false,
        ));
    }
    if predicates.is_empty() {
        return plan;
    }
    match scalar_expr::combine_conjuncts(arena, predicates) {
        Some(predicate) => {
            OptExpr::new(Operator::LogicalFilter(FilterOp { predicate }), vec![plan])
        }
        None => plan,
    }
}

fn is_eliminable_count(aggregate: &ScalarAggregateSpec, arena: &ScalarArena) -> bool {
    if !aggregate.name.eq_ignore_ascii_case("count") {
        return false;
    }
    if aggregate.distinct {
        return false;
    }
    if !aggregate.order_by.is_empty() {
        return false;
    }
    aggregate
        .args
        .iter()
        .all(|id| scalar_expr::is_literal_count_arg(arena, *id))
}

fn rewrite_eliminated_aggregate_project_item(
    item: ScalarProjectItem,
    eliminated_count_outputs: &HashSet<ColumnId>,
    arena: &mut ScalarArena,
) -> Option<ScalarProjectItem> {
    let new_expr_id =
        rewrite_eliminated_aggregate_expr(arena, item.expr, eliminated_count_outputs)?;
    Some(ScalarProjectItem {
        expr: new_expr_id,
        output_name: item.output_name,
        output_column_id: item.output_column_id,
        expr_display: item.expr_display,
    })
}

fn rewrite_eliminated_aggregate_expr(
    arena: &mut ScalarArena,
    expr: ScalarId,
    eliminated_count_outputs: &HashSet<ColumnId>,
) -> Option<ScalarId> {
    match arena.node(expr).clone() {
        ScalarNode::ColumnRef(column_id) if eliminated_count_outputs.contains(&column_id) => {
            Some(scalar_expr::int_literal(arena, 1))
        }
        ScalarNode::AggregateCall {
            name,
            distinct,
            order_by,
            ..
        } if name.eq_ignore_ascii_case("count") && !distinct && order_by.is_empty() => {
            Some(scalar_expr::int_literal(arena, 1))
        }
        _ if !scalar_expr::contains_aggregate(arena, expr) => Some(expr),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use arrow::datatypes::{Field, Schema};
    use novarocks_spi::connector::{
        ConnectorCancellation, ConnectorInstanceId, ConnectorRequestContext,
        ConnectorTableForeignKeyConstraint, ConnectorTableIdentity, ConnectorTablePlanningFacts,
        ConnectorTableUniqueConstraint,
    };

    use super::*;

    use crate::analysis::{LiteralValue, OutputColumn};
    use crate::optimizer::operator::{AggregateOutputLayout, LogicalAggregateOp};
    use crate::optimizer::rewrite::tree_binder::bind_tree;
    use crate::optimizer::scalar::{HashableLiteral, ScalarNode};
    use crate::planner::table::TableDef;
    use novarocks_types::schema::ColumnDef;

    fn output_col(id: u32, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId::new_for_test(id),
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: false,
            is_internal: false,
        }
    }

    fn column_def(name: &str) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: false,
            write_default: None,
            logical_type: None,
        }
    }

    fn scan_expr(table_name: &str, cols: &[(u32, &str)]) -> OptExpr {
        let columns = cols
            .iter()
            .map(|(_, name)| column_def(name))
            .collect::<Vec<_>>();
        let outputs = cols
            .iter()
            .map(|(id, name)| output_col(*id, name))
            .collect::<Vec<_>>();
        OptExpr::leaf(Operator::LogicalScan(ScanOp {
            database: "default".to_string(),
            table: TableDef {
                name: table_name.to_string(),
                columns,
                iceberg_row_lineage_metadata_columns: vec![],
                source: crate::compiler::mv_rewrite::test_scan_source(
                    crate::planner::table::SqlScanKind::ConnectorRead,
                ),
            },
            alias: None,
            stats_ref: None,
            columns: outputs,
            predicates: vec![],
            required_columns: None,
            variant_columns: vec![],
            mv_rewritten_from: None,
        }))
    }

    fn col(arena: &mut ScalarArena, id: u32) -> ScalarId {
        arena.intern(
            ScalarNode::ColumnRef(ColumnId::new_for_test(id)),
            DataType::Int64,
            false,
        )
    }

    fn empty_project(input: OptExpr) -> OptExpr {
        OptExpr::new(
            Operator::LogicalProject(ProjectOp {
                items: vec![],
                output_qualifier: None,
            }),
            vec![input],
        )
    }

    fn join_expr() -> OptExpr {
        OptExpr::new(
            Operator::LogicalJoin(LogicalJoinOp {
                join_type: JoinKind::Inner,
                condition: None,
            }),
            vec![
                scan_expr("left_t", &[(1, "left_key")]),
                scan_expr("right_t", &[(2, "right_key")]),
            ],
        )
    }

    fn aggregate_expr(input: OptExpr) -> OptExpr {
        OptExpr::new(
            Operator::LogicalAggregate(LogicalAggregateOp::single(
                vec![],
                vec![],
                AggregateOutputLayout::new(vec![], vec![]),
                vec![],
            )),
            vec![input],
        )
    }

    #[test]
    fn prune_ukfk_join_pattern_matches_project_join_only() {
        let rule = PruneUkFkJoin;
        let project_join = empty_project(join_expr());
        let project_aggregate = empty_project(aggregate_expr(scan_expr("t", &[(1, "k")])));

        assert!(bind_tree(&rule.pattern(), &project_join).is_some());
        assert!(bind_tree(&rule.pattern(), &project_aggregate).is_none());
    }

    #[test]
    fn eliminate_unique_aggregate_pattern_matches_project_aggregate_only() {
        let rule = EliminateUniqueAggregate;
        let project_aggregate = empty_project(aggregate_expr(scan_expr("t", &[(1, "k")])));
        let project_join = empty_project(join_expr());

        assert!(bind_tree(&rule.pattern(), &project_aggregate).is_some());
        assert!(bind_tree(&rule.pattern(), &project_join).is_none());
    }

    #[test]
    fn project_referenced_side_rejects_cross_side_scalar_refs() {
        let mut arena = ScalarArena::new();
        let left = scan_expr("left_t", &[(1, "left_key")]);
        let right = scan_expr("right_t", &[(2, "right_key")]);
        let left_key = col(&mut arena, 1);
        let right_key = col(&mut arena, 2);
        let expr = arena.intern(
            ScalarNode::BinaryOp {
                op: BinOp::Add,
                left: left_key,
                right: right_key,
            },
            DataType::Int64,
            false,
        );
        let items = vec![ScalarProjectItem {
            expr,
            output_name: "mixed".to_string(),
            output_column_id: ColumnId::new_for_test(10),
            expr_display: None,
        }];

        assert_eq!(
            project_referenced_side(&items, &left, &right, &arena).unwrap(),
            None
        );
    }

    #[test]
    fn join_equality_pairs_accepts_nested_or_cast_column_refs() {
        let mut arena = ScalarArena::new();
        let left = scan_expr("left_t", &[(1, "left_key")]);
        let right = scan_expr("right_t", &[(2, "right_key")]);
        let left_key = col(&mut arena, 1);
        let right_key = col(&mut arena, 2);
        let nested_left = arena.intern(ScalarNode::Nested(left_key), DataType::Int64, false);
        let cast_right = arena.intern(
            ScalarNode::Cast {
                child: right_key,
                target: DataType::Int64,
            },
            DataType::Int64,
            false,
        );
        let condition = arena.intern(
            ScalarNode::BinaryOp {
                op: BinOp::Eq,
                left: nested_left,
                right: cast_right,
            },
            DataType::Boolean,
            false,
        );
        let join = LogicalJoinOp {
            join_type: JoinKind::Inner,
            condition: Some(condition),
        };

        assert_eq!(
            join_equality_pairs(&join, &left, &right, &arena).unwrap(),
            vec![("left_key".to_string(), "right_key".to_string())]
        );
    }

    #[test]
    fn eliminable_count_accepts_literal_count_args_without_materializing() {
        let mut arena = ScalarArena::new();
        let one = arena.intern(
            ScalarNode::Literal(HashableLiteral(LiteralValue::Int(1))),
            DataType::Int64,
            false,
        );
        let null = arena.intern(
            ScalarNode::Literal(HashableLiteral(LiteralValue::Null)),
            DataType::Null,
            true,
        );

        let count_one = ScalarAggregateSpec {
            output_column_id: ColumnId::new_for_test(9001),
            name: "count".to_string(),
            args: vec![one],
            distinct: false,
            order_by: vec![],
            resolved: crate::functions::test_resolved_aggregate("count", &[DataType::Int64], false),
        };
        let count_null = ScalarAggregateSpec {
            output_column_id: ColumnId::new_for_test(9002),
            name: "count".to_string(),
            args: vec![null],
            distinct: false,
            order_by: vec![],
            resolved: crate::functions::test_resolved_aggregate("count", &[DataType::Null], false),
        };

        assert!(is_eliminable_count(&count_one, &arena));
        assert!(is_eliminable_count(&count_null, &arena));
    }

    #[test]
    fn eliminated_unique_aggregate_rewrites_count_output_ref_to_literal() {
        let mut arena = ScalarArena::new();
        let count_output = ColumnId::new_for_test(9001);
        let count_ref = arena.intern(ScalarNode::ColumnRef(count_output), DataType::Int64, false);
        let item = ScalarProjectItem {
            expr: count_ref,
            output_name: "cnt".to_string(),
            output_column_id: ColumnId::new_for_test(9002),
            expr_display: None,
        };
        let eliminated_outputs = HashSet::from([count_output]);

        let rewritten =
            rewrite_eliminated_aggregate_project_item(item, &eliminated_outputs, &mut arena)
                .expect("count output reference should be rewritten");

        assert!(matches!(
            arena.node(rewritten.expr),
            ScalarNode::Literal(HashableLiteral(LiteralValue::Int(1)))
        ));
    }

    #[test]
    fn sqlx2_ukfk_facts_match_typed_connector_constraints() {
        struct NeverCancelled;

        impl ConnectorCancellation for NeverCancelled {
            fn is_cancelled(&self) -> bool {
                false
            }
        }

        let schema = Arc::new(Schema::new(vec![Field::new(
            "customer_id",
            DataType::Int64,
            false,
        )]));
        let context = ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(1),
            Arc::new(NeverCancelled),
            4_096,
            4_096,
        )
        .expect("valid connector context");
        let planning_facts = ConnectorTablePlanningFacts::try_new(
            &schema,
            vec![],
            vec![ConnectorTableUniqueConstraint::new(vec![0])],
            vec![ConnectorTableForeignKeyConstraint::new(
                vec![0],
                ConnectorTableIdentity {
                    instance_id: ConnectorInstanceId::parse("iceberg").expect("valid instance ID"),
                    namespace: Arc::from("sales"),
                    table: Arc::from("customers"),
                },
                vec![Arc::from("id")],
            )],
            vec![],
            &context,
        )
        .expect("valid typed planning facts");
        let facts = SqlUkFkTableFacts::from_connector_planning_facts(&schema, &planning_facts);

        assert!(facts.has_unique_key(&["CUSTOMER_ID".to_string()]));
        assert!(facts.has_matching_foreign_key(
            &["customer_id".to_string()],
            "customers",
            Some("c"),
            &["id".to_string()],
        ));
        assert!(!facts.has_matching_foreign_key(
            &["customer_id".to_string()],
            "orders",
            None,
            &["id".to_string()],
        ));
    }
}
