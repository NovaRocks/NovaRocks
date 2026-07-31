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

//! Flatten a contiguous inner/cross join chain in the memo into a
//! [`MultiJoinGraph`]. Ported from `join_reorder/reorder.rs::extract_join_graph`,
//! re-expressed over memo `GroupId`s.

use std::collections::{HashMap, HashSet};

use crate::sql::column_id::ColumnId;
use crate::sql::common::JoinKind;
use crate::sql::optimizer::memo::{GroupId, Memo};
use crate::sql::optimizer::operator::{LogicalJoinOp, Operator};
use crate::sql::optimizer::scalar::{ScalarArena, ScalarId, ScalarNode};
use crate::sql::optimizer::statistics::{Confidence, Statistics};

use super::super::implement::get_group_column_ids;
use super::{EquiClass, MultiJoinGraph};

/// Flatten the inner/cross join chain rooted at `root`. Descends only through
/// inner/cross `LogicalJoin` groups (and a `LogicalFilter` sitting directly on
/// such a join, whose predicate is absorbed); every other group — outer/semi
/// joins, `LogicalProject`, scans, aggregates, CTE consumes, etc. — is an opaque
/// atom (M4: the chain never descends through a projection or a non-inner/cross
/// join). Returns `None` for fewer than two atoms or more than 32 (mask cap).
pub(crate) fn flatten_join_chain(memo: &mut Memo, root: GroupId) -> Option<MultiJoinGraph> {
    let mut atoms: Vec<GroupId> = Vec::new();
    let mut raw_predicates: Vec<ScalarId> = Vec::new();
    let mut chain_joins: Vec<GroupId> = Vec::new();
    collect_chain(
        memo,
        root,
        &mut atoms,
        &mut raw_predicates,
        &mut chain_joins,
    );

    if atoms.len() < 2 || atoms.len() > 32 {
        return None;
    }

    let atom_cols: Vec<HashSet<ColumnId>> = atoms
        .iter()
        .map(|&g| get_group_column_ids(memo, g))
        .collect();
    let atom_stats: Vec<Statistics> = atoms.iter().map(|&g| group_stats(memo, g)).collect();
    let mut predicates: Vec<(ScalarId, u32)> = Vec::new();

    for &pred in &raw_predicates {
        let mask = relation_mask(&memo.scalars, pred, &atom_cols);
        if mask.count_ones() < 2 {
            // A single-relation or constant predicate left inside a join
            // condition (rare after predicate pushdown). Bail rather than risk
            // dropping it during materialization — this chain keeps its
            // original (un-reordered) join order.
            return None;
        }
        predicates.push((pred, mask));
    }

    let equi_classes = project_root_equi_classes(memo, root, &atoms, &atom_cols);

    Some(MultiJoinGraph {
        atoms,
        atom_stats,
        predicates,
        chain_join_groups: chain_joins,
        equi_classes,
    })
}

/// Project root strict equivalence facts onto the atoms in this join chain.
/// Representatives are interned from atom output metadata, not borrowed from
/// raw predicate operands, so synthesized edges use the atom's current type and
/// nullability metadata.
fn project_root_equi_classes(
    memo: &mut Memo,
    root: GroupId,
    atoms: &[GroupId],
    atom_cols: &[HashSet<ColumnId>],
) -> Vec<EquiClass> {
    let classes: Vec<_> = memo
        .groups
        .get(root)
        .and_then(|group| group.logical_props.as_ref())
        .map(|props| props.equivalence_classes.classes().to_vec())
        .unwrap_or_default();

    let mut out = Vec::new();
    for class in &classes {
        let mut reps: Vec<(usize, ScalarId)> = Vec::new();
        for (atom_idx, atom) in atoms.iter().enumerate() {
            let rep_column = class
                .iter()
                .filter(|column_id| atom_cols[atom_idx].contains(column_id))
                .filter_map(|column_id| atom_output_column(memo, *atom, column_id))
                .min_by_key(|column| column.column_id);
            if let Some(column) = rep_column {
                let scalar = intern_output_column_ref(memo, &column);
                reps.push((atom_idx, scalar));
            }
        }
        if reps.len() >= 2 {
            out.push(EquiClass::new(class.iter().collect(), reps));
        }
    }
    out
}

fn atom_output_column(
    memo: &Memo,
    atom: GroupId,
    column_id: ColumnId,
) -> Option<crate::sql::common::OutputColumn> {
    memo.groups
        .get(atom)
        .and_then(|group| group.logical_props.as_ref())
        .and_then(|props| {
            props
                .output_columns
                .iter()
                .find(|column| column.column_id == column_id)
                .cloned()
        })
}

fn intern_output_column_ref(
    memo: &mut Memo,
    column: &crate::sql::common::OutputColumn,
) -> ScalarId {
    memo.scalars
        .remember_source_column_display(column.column_id, None, column.name.clone());
    memo.scalars.intern(
        ScalarNode::ColumnRef(column.column_id),
        column.data_type.clone(),
        column.nullable,
    )
}

fn collect_chain(
    memo: &Memo,
    group: GroupId,
    atoms: &mut Vec<GroupId>,
    predicates: &mut Vec<ScalarId>,
    chain_joins: &mut Vec<GroupId>,
) {
    let Some(expr) = memo.groups.get(group).and_then(|g| g.logical_exprs.first()) else {
        atoms.push(group);
        return;
    };
    match &expr.op {
        Operator::LogicalJoin(LogicalJoinOp {
            join_type,
            condition,
        }) if matches!(join_type, JoinKind::Inner | JoinKind::Cross)
            && expr.children.len() == 2 =>
        {
            // This inner/cross join is part of the chain; record it so the
            // reorder pass can mark it reorder-owned (D2).
            chain_joins.push(group);
            collect_chain(memo, expr.children[0], atoms, predicates, chain_joins);
            collect_chain(memo, expr.children[1], atoms, predicates, chain_joins);
            if let Some(cond) = condition {
                predicates.extend(split_and_scalar(&memo.scalars, *cond));
            }
        }
        Operator::LogicalFilter(f)
            if expr.children.len() == 1 && is_inner_cross_join(memo, expr.children[0]) =>
        {
            // Absorb a filter sitting directly on an inner/cross join. The filter
            // group itself is not a join (JoinAssociativity never matches it), so
            // only the join below it is recorded as a chain join.
            predicates.extend(split_and_scalar(&memo.scalars, f.predicate));
            collect_chain(memo, expr.children[0], atoms, predicates, chain_joins);
        }
        // Any other operator (incl. LogicalProject and outer/semi joins) is an
        // opaque atom — the chain stops here.
        _ => atoms.push(group),
    }
}

fn is_inner_cross_join(memo: &Memo, group: GroupId) -> bool {
    memo.groups
        .get(group)
        .and_then(|g| g.logical_exprs.first())
        .is_some_and(|e| {
            matches!(
                &e.op,
                Operator::LogicalJoin(LogicalJoinOp { join_type, .. })
                    if matches!(join_type, JoinKind::Inner | JoinKind::Cross)
            )
        })
}

fn group_stats(memo: &Memo, group: GroupId) -> Statistics {
    memo.groups
        .get(group)
        .and_then(|g| g.logical_props.as_ref())
        .map(|p| Statistics {
            output_row_count: p.row_count,
            row_count_confidence: p.row_count_confidence,
            column_statistics: p.column_statistics.clone(),
        })
        .unwrap_or_else(|| Statistics {
            output_row_count: 1.0,
            row_count_confidence: Confidence::Fallback,
            column_statistics: HashMap::new(),
        })
}

/// Bitmask of atom indices whose columns the predicate references.
fn relation_mask(arena: &ScalarArena, pred: ScalarId, atom_cols: &[HashSet<ColumnId>]) -> u32 {
    let mut ids = HashSet::new();
    collect_scalar_column_ids(arena, pred, &mut ids);
    let mut mask = 0u32;
    for id in ids {
        for (i, cols) in atom_cols.iter().enumerate() {
            if cols.contains(&id) {
                mask |= 1u32 << i;
            }
        }
    }
    mask
}

fn split_and_scalar(arena: &ScalarArena, expr: ScalarId) -> Vec<ScalarId> {
    let mut out = Vec::new();
    split_and_scalar_inner(arena, expr, &mut out);
    out
}

fn split_and_scalar_inner(arena: &ScalarArena, expr: ScalarId, out: &mut Vec<ScalarId>) {
    match arena.node(expr) {
        ScalarNode::BinaryOp {
            op: crate::sql::common::BinOp::And,
            left,
            right,
        } => {
            split_and_scalar_inner(arena, *left, out);
            split_and_scalar_inner(arena, *right, out);
        }
        ScalarNode::Nested(inner) => split_and_scalar_inner(arena, *inner, out),
        _ => out.push(expr),
    }
}

fn collect_scalar_column_ids(arena: &ScalarArena, expr: ScalarId, out: &mut HashSet<ColumnId>) {
    match arena.node(expr) {
        ScalarNode::ColumnRef(id) => {
            if *id != ColumnId::UNSET {
                out.insert(*id);
            }
        }
        ScalarNode::LambdaParamRef { .. } | ScalarNode::Literal(_) => {}
        ScalarNode::BinaryOp { left, right, .. } => {
            collect_scalar_column_ids(arena, *left, out);
            collect_scalar_column_ids(arena, *right, out);
        }
        ScalarNode::UnaryOp { child, .. }
        | ScalarNode::Cast { child, .. }
        | ScalarNode::IsNull { child, .. }
        | ScalarNode::Like { child, .. }
        | ScalarNode::IsTruthValue { child, .. }
        | ScalarNode::Nested(child) => collect_scalar_column_ids(arena, *child, out),
        ScalarNode::FunctionCall { args, .. } | ScalarNode::AggregateCall { args, .. } => {
            for arg in args {
                collect_scalar_column_ids(arena, *arg, out);
            }
            if let ScalarNode::AggregateCall { order_by, .. } = arena.node(expr) {
                for key in order_by {
                    collect_scalar_column_ids(arena, key.expr, out);
                }
            }
        }
        ScalarNode::LambdaFunction { body, .. } | ScalarNode::Lambda { body, .. } => {
            collect_scalar_column_ids(arena, *body, out);
        }
        ScalarNode::InList { child, list, .. } => {
            collect_scalar_column_ids(arena, *child, out);
            for item in list {
                collect_scalar_column_ids(arena, *item, out);
            }
        }
        ScalarNode::Between {
            child, low, high, ..
        } => {
            collect_scalar_column_ids(arena, *child, out);
            collect_scalar_column_ids(arena, *low, out);
            collect_scalar_column_ids(arena, *high, out);
        }
        ScalarNode::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(operand) = operand {
                collect_scalar_column_ids(arena, *operand, out);
            }
            for (when, then) in when_then {
                collect_scalar_column_ids(arena, *when, out);
                collect_scalar_column_ids(arena, *then, out);
            }
            if let Some(else_expr) = else_expr {
                collect_scalar_column_ids(arena, *else_expr, out);
            }
        }
        ScalarNode::WindowCall {
            args,
            partition_by,
            order_by,
            ..
        } => {
            for arg in args {
                collect_scalar_column_ids(arena, *arg, out);
            }
            for part in partition_by {
                collect_scalar_column_ids(arena, *part, out);
            }
            for key in order_by {
                collect_scalar_column_ids(arena, key.expr, out);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::common::{BinOp, LiteralValue, OutputColumn};
    use crate::sql::optimizer::memo::{JoinTree, LogicalProperties, MExpr};
    use crate::sql::optimizer::operator::ValuesOp;
    use crate::sql::optimizer::scalar::HashableLiteral;
    use crate::sql::optimizer::statistics::ColumnStatistic;
    use crate::sql::optimizer::stats::copy_in_join_tree;
    use crate::sql::optimizer::stats_input::OptimizerStatsInput;
    use arrow::datatypes::DataType;
    use std::collections::HashMap as Map;

    fn empty_stats_input() -> OptimizerStatsInput {
        OptimizerStatsInput::from_test_table_statistics(&Map::new())
    }

    fn col(memo: &mut Memo, id: u32) -> ScalarId {
        memo.scalars.intern(
            ScalarNode::ColumnRef(ColumnId::new_for_test(id)),
            DataType::Int64,
            false,
        )
    }

    fn int_lit(memo: &mut Memo, v: i64) -> ScalarId {
        memo.scalars.intern(
            ScalarNode::Literal(HashableLiteral(LiteralValue::Int(v))),
            DataType::Int64,
            false,
        )
    }

    fn binary(memo: &mut Memo, op: BinOp, left: ScalarId, right: ScalarId) -> ScalarId {
        memo.scalars.intern(
            ScalarNode::BinaryOp { left, op, right },
            DataType::Boolean,
            false,
        )
    }

    fn eq(memo: &mut Memo, left: ScalarId, right: ScalarId) -> ScalarId {
        binary(memo, BinOp::Eq, left, right)
    }

    fn eq_for_null(memo: &mut Memo, left: ScalarId, right: ScalarId) -> ScalarId {
        binary(memo, BinOp::EqForNull, left, right)
    }

    fn and(memo: &mut Memo, left: ScalarId, right: ScalarId) -> ScalarId {
        binary(memo, BinOp::And, left, right)
    }

    fn eq_cols(memo: &mut Memo, left: u32, right: u32) -> ScalarId {
        let left = col(memo, left);
        let right = col(memo, right);
        eq(memo, left, right)
    }

    fn null_safe_eq_cols(memo: &mut Memo, left: u32, right: u32) -> ScalarId {
        let left = col(memo, left);
        let right = col(memo, right);
        eq_for_null(memo, left, right)
    }

    fn eq_col_lit(memo: &mut Memo, column: u32, value: i64) -> ScalarId {
        let column = col(memo, column);
        let value = int_lit(memo, value);
        eq(memo, column, value)
    }

    fn out_col(id: u32) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId::new_for_test(id),
            name: format!("c{id}"),
            data_type: DataType::Int64,
            nullable: false,
            is_internal: false,
        }
    }

    fn leaf(memo: &mut Memo, col_id: u32, rows: f64) -> GroupId {
        let g = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalValues(ValuesOp {
                rows: vec![],
                columns: vec![],
            }),
            children: vec![],
        });
        let mut props = LogicalProperties::new(vec![out_col(col_id)], rows);
        props.row_count_confidence = Confidence::Estimated;
        props.column_statistics.insert(
            ColumnId::new_for_test(col_id),
            ColumnStatistic {
                min_value: 0.0,
                max_value: rows,
                nulls_fraction: 0.0,
                average_row_size: 8.0,
                ..ColumnStatistic::for_test_with_ndv(rows, Confidence::Estimated)
            },
        );
        memo.groups[g].logical_props = Some(props);
        g
    }

    fn leaf_with_outputs(memo: &mut Memo, outputs: Vec<OutputColumn>, rows: f64) -> GroupId {
        let g = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalValues(ValuesOp {
                rows: vec![],
                columns: vec![],
            }),
            children: vec![],
        });
        let mut props = LogicalProperties::new(outputs.clone(), rows);
        props.row_count_confidence = Confidence::Estimated;
        for column in &outputs {
            props.column_statistics.insert(
                column.column_id,
                ColumnStatistic {
                    min_value: 0.0,
                    max_value: rows,
                    nulls_fraction: 0.0,
                    average_row_size: 8.0,
                    ..ColumnStatistic::for_test_with_ndv(rows, Confidence::Estimated)
                },
            );
        }
        memo.groups[g].logical_props = Some(props);
        g
    }

    fn inner(cond: ScalarId) -> LogicalJoinOp {
        LogicalJoinOp {
            join_type: JoinKind::Inner,
            condition: Some(cond),
        }
    }

    #[test]
    fn flatten_collects_atoms_and_classifies_predicates() {
        let mut memo = Memo::new();
        let a = leaf(&mut memo, 1, 1000.0);
        let b = leaf(&mut memo, 2, 100.0);
        let c = leaf(&mut memo, 3, 50.0);
        let c1_eq_c2 = eq_cols(&mut memo, 1, 2);
        let c1_eq_c3 = eq_cols(&mut memo, 1, 3);
        // (A ⋈[c1=c2] B) ⋈[c1=c3] C
        let tree = JoinTree::Join {
            left: Box::new(JoinTree::Join {
                left: Box::new(JoinTree::Leaf(a)),
                right: Box::new(JoinTree::Leaf(b)),
                op: inner(c1_eq_c2),
            }),
            right: Box::new(JoinTree::Leaf(c)),
            op: inner(c1_eq_c3),
        };
        let root = copy_in_join_tree(&mut memo, &tree, &empty_stats_input());

        let graph = flatten_join_chain(&mut memo, root).expect("3-atom chain flattens");
        assert_eq!(graph.atoms, vec![a, b, c], "left-to-right atom order");
        assert_eq!(graph.predicates.len(), 2, "two multi-relation join edges");
        let masks: std::collections::HashSet<u32> =
            graph.predicates.iter().map(|(_, m)| *m).collect();
        assert!(masks.contains(&0b011), "c1=c2 touches atoms A,B");
        assert!(masks.contains(&0b101), "c1=c3 touches atoms A,C");
    }

    #[test]
    fn flatten_does_not_descend_through_non_inner_join() {
        // A LEFT OUTER join inside the chain is an opaque atom boundary — the
        // same code path that stops at a LogicalProject (M4).
        let mut memo = Memo::new();
        let a = leaf(&mut memo, 1, 1000.0);
        let b = leaf(&mut memo, 2, 100.0);
        let c = leaf(&mut memo, 3, 50.0);
        let lo_cond = eq_cols(&mut memo, 1, 2);
        let c1_eq_c3 = eq_cols(&mut memo, 1, 3);
        let lo = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalJoin(LogicalJoinOp {
                join_type: JoinKind::LeftOuter,
                condition: Some(lo_cond),
            }),
            children: vec![a, b],
        });
        let mut lo_props = LogicalProperties::new(vec![out_col(1), out_col(2)], 1000.0);
        lo_props.row_count_confidence = Confidence::Estimated;
        memo.groups[lo].logical_props = Some(lo_props);

        let tree = JoinTree::Join {
            left: Box::new(JoinTree::Leaf(lo)),
            right: Box::new(JoinTree::Leaf(c)),
            op: inner(c1_eq_c3),
        };
        let root = copy_in_join_tree(&mut memo, &tree, &empty_stats_input());

        let graph = flatten_join_chain(&mut memo, root).expect("chain over {LO, C}");
        assert_eq!(
            graph.atoms.len(),
            2,
            "LEFT OUTER join is an atom, not descended"
        );
        assert!(graph.atoms.contains(&lo), "the outer-join group is an atom");
        assert!(
            !graph.atoms.contains(&a) && !graph.atoms.contains(&b),
            "must not descend into the outer join's inputs"
        );
    }

    #[test]
    fn flatten_bails_on_single_side_predicate() {
        let mut memo = Memo::new();
        let a = leaf(&mut memo, 1, 1000.0);
        let b = leaf(&mut memo, 2, 100.0);
        // condition: c1 = c2  AND  c1 = 5  (the second conjunct is single-side on A)
        let c1_eq_c2 = eq_cols(&mut memo, 1, 2);
        let c1_eq_5 = eq_col_lit(&mut memo, 1, 5);
        let cond = and(&mut memo, c1_eq_c2, c1_eq_5);
        let tree = JoinTree::Join {
            left: Box::new(JoinTree::Leaf(a)),
            right: Box::new(JoinTree::Leaf(b)),
            op: inner(cond),
        };
        let root = copy_in_join_tree(&mut memo, &tree, &empty_stats_input());

        // A single-relation predicate inside the join condition makes the chain
        // non-reorderable (we never drop predicates); flatten returns None and
        // the original order / RBO path handles this chain.
        assert!(
            flatten_join_chain(&mut memo, root).is_none(),
            "chain with a single-side join-condition predicate must not be reordered"
        );
    }

    #[test]
    fn flatten_builds_transitive_equivalence_class() {
        let mut memo = Memo::new();
        let a = leaf(&mut memo, 1, 1000.0);
        let b = leaf(&mut memo, 2, 100.0);
        let c = leaf(&mut memo, 3, 50.0);
        let c1_eq_c2 = eq_cols(&mut memo, 1, 2);
        let c2_eq_c3 = eq_cols(&mut memo, 2, 3);
        // (A ⋈[c1=c2] B) ⋈[c2=c3] C — A and C share an equi class only
        // transitively (through c2); there is no literal c1=c3 edge.
        let tree = JoinTree::Join {
            left: Box::new(JoinTree::Join {
                left: Box::new(JoinTree::Leaf(a)),
                right: Box::new(JoinTree::Leaf(b)),
                op: inner(c1_eq_c2),
            }),
            right: Box::new(JoinTree::Leaf(c)),
            op: inner(c2_eq_c3),
        };
        let root = copy_in_join_tree(&mut memo, &tree, &empty_stats_input());
        let graph = flatten_join_chain(&mut memo, root).expect("3-atom chain flattens");

        // Literal edges only connect A-B and B-C; no literal A-C edge exists.
        let masks: std::collections::HashSet<u32> =
            graph.predicates.iter().map(|(_, m)| *m).collect();
        assert!(
            masks.contains(&0b011) && masks.contains(&0b110),
            "literal A-B and B-C edges present"
        );
        assert!(!masks.contains(&0b101), "no literal A-C edge");

        // The class {c1,c2,c3} spans all three atoms and transitively straddles
        // A (atom 0) and C (atom 2).
        assert_eq!(
            graph.equi_classes.len(),
            1,
            "one cross-atom equivalence class"
        );
        let class = &graph.equi_classes[0];
        assert!(class.rep_in(0b001).is_some(), "class has a rep in atom A");
        assert!(class.rep_in(0b100).is_some(), "class has a rep in atom C");
        assert!(
            class.straddles(0b001, 0b100),
            "class transitively connects A and C"
        );
    }

    #[test]
    fn flatten_projects_root_equivalence_class_with_atom_internal_strict_fact() {
        let mut memo = Memo::new();
        let a = leaf_with_outputs(&mut memo, vec![out_col(1), out_col(2)], 1000.0);
        let b = leaf(&mut memo, 3, 100.0);
        let c = leaf(&mut memo, 4, 50.0);
        let c1_eq_c3 = eq_cols(&mut memo, 1, 3);
        let c2_eq_c4 = eq_cols(&mut memo, 2, 4);
        let tree = JoinTree::Join {
            left: Box::new(JoinTree::Join {
                left: Box::new(JoinTree::Leaf(a)),
                right: Box::new(JoinTree::Leaf(b)),
                op: inner(c1_eq_c3),
            }),
            right: Box::new(JoinTree::Leaf(c)),
            op: inner(c2_eq_c4),
        };
        let root = copy_in_join_tree(&mut memo, &tree, &empty_stats_input());
        let mut root_props =
            LogicalProperties::new(vec![out_col(1), out_col(2), out_col(3), out_col(4)], 50.0);
        root_props
            .equivalence_classes
            .merge_pair(ColumnId::new_for_test(1), ColumnId::new_for_test(2));
        root_props
            .equivalence_classes
            .merge_pair(ColumnId::new_for_test(1), ColumnId::new_for_test(3));
        root_props
            .equivalence_classes
            .merge_pair(ColumnId::new_for_test(2), ColumnId::new_for_test(4));
        memo.groups[root].logical_props = Some(root_props);

        let graph = flatten_join_chain(&mut memo, root).expect("3-atom chain flattens");

        assert_eq!(
            graph.equi_classes.len(),
            1,
            "root strict facts should project one class spanning A, B, and C"
        );
        let class = &graph.equi_classes[0];
        assert!(class.rep_in(0b001).is_some(), "class has a rep in atom A");
        assert!(class.rep_in(0b010).is_some(), "class has a rep in atom B");
        assert!(class.rep_in(0b100).is_some(), "class has a rep in atom C");
        assert!(
            class.straddles(0b010, 0b100),
            "B-C cut is connected only through A's internal strict equality"
        );
    }

    #[test]
    fn flatten_does_not_use_null_safe_atom_internal_fact_for_strict_class() {
        let mut memo = Memo::new();
        let a = leaf_with_outputs(&mut memo, vec![out_col(1), out_col(2)], 1000.0);
        let b = leaf(&mut memo, 3, 100.0);
        let c = leaf(&mut memo, 4, 50.0);
        let c1_eq_c3 = eq_cols(&mut memo, 1, 3);
        let c2_eq_c4 = eq_cols(&mut memo, 2, 4);
        let tree = JoinTree::Join {
            left: Box::new(JoinTree::Join {
                left: Box::new(JoinTree::Leaf(a)),
                right: Box::new(JoinTree::Leaf(b)),
                op: inner(c1_eq_c3),
            }),
            right: Box::new(JoinTree::Leaf(c)),
            op: inner(c2_eq_c4),
        };
        let root = copy_in_join_tree(&mut memo, &tree, &empty_stats_input());
        let mut root_props =
            LogicalProperties::new(vec![out_col(1), out_col(2), out_col(3), out_col(4)], 50.0);
        root_props
            .equivalence_classes
            .merge_pair(ColumnId::new_for_test(1), ColumnId::new_for_test(3));
        root_props
            .equivalence_classes
            .merge_pair(ColumnId::new_for_test(2), ColumnId::new_for_test(4));
        memo.groups[root].logical_props = Some(root_props);

        let graph = flatten_join_chain(&mut memo, root).expect("3-atom chain flattens");

        assert!(
            !graph
                .equi_classes
                .iter()
                .any(|class| class.straddles(0b010, 0b100)),
            "B-C cut must not be connected without a root strict class spanning both atoms"
        );
    }

    #[test]
    fn flatten_does_not_build_class_for_pure_null_safe_chain() {
        let mut memo = Memo::new();
        let a = leaf(&mut memo, 1, 1000.0);
        let b = leaf(&mut memo, 2, 100.0);
        let c = leaf(&mut memo, 3, 50.0);
        let c1_null_safe_c2 = null_safe_eq_cols(&mut memo, 1, 2);
        let c2_null_safe_c3 = null_safe_eq_cols(&mut memo, 2, 3);
        let tree = JoinTree::Join {
            left: Box::new(JoinTree::Join {
                left: Box::new(JoinTree::Leaf(a)),
                right: Box::new(JoinTree::Leaf(b)),
                op: inner(c1_null_safe_c2),
            }),
            right: Box::new(JoinTree::Leaf(c)),
            op: inner(c2_null_safe_c3),
        };
        let root = copy_in_join_tree(&mut memo, &tree, &empty_stats_input());
        memo.groups[root].logical_props = Some(LogicalProperties::new(
            vec![out_col(1), out_col(2), out_col(3)],
            50.0,
        ));

        let graph = flatten_join_chain(&mut memo, root).expect("3-atom chain flattens");

        assert!(
            graph.equi_classes.is_empty(),
            "pure null-safe chains must not expose strict transitive classes"
        );
    }

    #[test]
    fn flatten_drops_single_atom_equivalence() {
        // Two atoms joined on c1=c2 form a 2-atom class (kept). A redundant
        // self-referential conjunct cannot occur here, so the class set holds
        // exactly the one cross-atom class.
        let mut memo = Memo::new();
        let a = leaf(&mut memo, 1, 1000.0);
        let b = leaf(&mut memo, 2, 100.0);
        let c1_eq_c2 = eq_cols(&mut memo, 1, 2);
        let tree = JoinTree::Join {
            left: Box::new(JoinTree::Leaf(a)),
            right: Box::new(JoinTree::Leaf(b)),
            op: inner(c1_eq_c2),
        };
        let root = copy_in_join_tree(&mut memo, &tree, &empty_stats_input());
        let graph = flatten_join_chain(&mut memo, root).expect("2-atom chain flattens");
        assert_eq!(graph.equi_classes.len(), 1, "one 2-atom class");
        assert!(graph.equi_classes[0].straddles(0b01, 0b10));
    }
}
