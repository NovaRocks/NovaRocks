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

use crate::column_id::ColumnId;
use crate::optimizer::operator::ScalarProjectItem;
use crate::optimizer::property::{ColumnIdSet, EquivalenceClasses, SortKey};
use crate::optimizer::scalar::{ScalarArena, ScalarId, ScalarNode, SortKey as ScalarSortKey};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TopNWindow {
    pub(crate) limit: i64,
    pub(crate) offset: i64,
}

impl TopNWindow {
    pub(crate) fn from_limit_offset(limit: Option<i64>, offset: Option<i64>) -> Option<Self> {
        let limit = limit?;
        if limit < 0 {
            return None;
        }
        let offset = offset.unwrap_or(0);
        if offset < 0 {
            return None;
        }
        Some(Self { limit, offset })
    }

    pub(crate) fn end_exclusive(self) -> Option<i64> {
        self.offset.checked_add(self.limit)
    }

    pub(crate) fn covers(self, needed: Self) -> bool {
        let Some(self_end) = self.end_exclusive() else {
            return false;
        };
        let Some(needed_end) = needed.end_exclusive() else {
            return false;
        };
        self.offset <= needed.offset && self_end >= needed_end
    }
}

pub(crate) fn scalar_expr_to_column_id(
    arena: &ScalarArena,
    expr: crate::optimizer::scalar::ScalarId,
) -> Option<ColumnId> {
    match arena.node(expr) {
        ScalarNode::ColumnRef(column_id) if *column_id != ColumnId::UNSET => Some(*column_id),
        _ => None,
    }
}

pub(crate) fn collect_column_ids(arena: &ScalarArena, expr: ScalarId) -> ColumnIdSet {
    let mut columns = Vec::new();
    collect_column_ids_inner(arena, expr, &mut columns);
    ColumnIdSet::from_columns(columns)
}

fn collect_column_ids_inner(arena: &ScalarArena, expr: ScalarId, out: &mut Vec<ColumnId>) {
    match arena.node(expr) {
        ScalarNode::ColumnRef(column_id) => out.push(*column_id),
        ScalarNode::LambdaParamRef { .. } | ScalarNode::Literal(_) => {}
        ScalarNode::BinaryOp { left, right, .. } => {
            collect_column_ids_inner(arena, *left, out);
            collect_column_ids_inner(arena, *right, out);
        }
        ScalarNode::UnaryOp { child, .. }
        | ScalarNode::LambdaFunction { body: child, .. }
        | ScalarNode::Cast { child, .. }
        | ScalarNode::IsNull { child, .. }
        | ScalarNode::IsTruthValue { child, .. }
        | ScalarNode::Nested(child)
        | ScalarNode::Lambda { body: child, .. } => {
            collect_column_ids_inner(arena, *child, out);
        }
        ScalarNode::FunctionCall { args, .. } | ScalarNode::AggregateCall { args, .. } => {
            for arg in args {
                collect_column_ids_inner(arena, *arg, out);
            }
        }
        ScalarNode::WindowCall {
            args,
            partition_by,
            order_by,
            ..
        } => {
            for arg in args {
                collect_column_ids_inner(arena, *arg, out);
            }
            for partition_expr in partition_by {
                collect_column_ids_inner(arena, *partition_expr, out);
            }
            for key in order_by {
                collect_column_ids_inner(arena, key.expr, out);
            }
        }
        ScalarNode::InList { child, list, .. } => {
            collect_column_ids_inner(arena, *child, out);
            for item in list {
                collect_column_ids_inner(arena, *item, out);
            }
        }
        ScalarNode::Between {
            child, low, high, ..
        } => {
            collect_column_ids_inner(arena, *child, out);
            collect_column_ids_inner(arena, *low, out);
            collect_column_ids_inner(arena, *high, out);
        }
        ScalarNode::Like { child, pattern, .. } => {
            collect_column_ids_inner(arena, *child, out);
            collect_column_ids_inner(arena, *pattern, out);
        }
        ScalarNode::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(operand) = operand {
                collect_column_ids_inner(arena, *operand, out);
            }
            for (when, then_expr) in when_then {
                collect_column_ids_inner(arena, *when, out);
                collect_column_ids_inner(arena, *then_expr, out);
            }
            if let Some(else_expr) = else_expr {
                collect_column_ids_inner(arena, *else_expr, out);
            }
        }
    }
}

pub(crate) fn sort_keys_to_keys(
    arena: &ScalarArena,
    items: &[ScalarSortKey],
) -> Option<Vec<SortKey>> {
    items
        .iter()
        .map(|item| {
            scalar_expr_to_column_id(arena, item.expr).map(|column| SortKey {
                column,
                asc: item.asc,
                nulls_first: item.nulls_first,
            })
        })
        .collect()
}

pub(crate) fn sort_keys_equivalent(
    left: &[SortKey],
    right: &[SortKey],
    equivalences: Option<&EquivalenceClasses>,
) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter().zip(right).all(|(l, r)| {
        l.asc == r.asc
            && l.nulls_first == r.nulls_first
            && columns_equivalent(l.column, r.column, equivalences)
    })
}

pub(crate) fn ordering_covers(
    provided: &[SortKey],
    required: &[SortKey],
    equivalences: Option<&EquivalenceClasses>,
) -> bool {
    provided.len() >= required.len()
        && provided
            .iter()
            .take(required.len())
            .zip(required)
            .all(|(p, r)| {
                p.asc == r.asc
                    && p.nulls_first == r.nulls_first
                    && columns_equivalent(p.column, r.column, equivalences)
            })
}

pub(crate) fn columns_equivalent(
    left: ColumnId,
    right: ColumnId,
    equivalences: Option<&EquivalenceClasses>,
) -> bool {
    if left == right {
        return true;
    }
    equivalences
        .and_then(|classes| classes.class_containing(left))
        .map(|class| class.contains(right))
        .unwrap_or(false)
}

#[allow(
    dead_code,
    reason = "Retained for staged SQL planner migration consumers and test helpers."
)]
pub(crate) fn passthrough_project_column_remap(
    arena: &ScalarArena,
    items: &[ScalarProjectItem],
) -> Vec<(ColumnId, ColumnId)> {
    items
        .iter()
        .filter_map(|item| {
            let column_id = scalar_expr_to_column_id(arena, item.expr)?;
            if item.output_column_id == ColumnId::UNSET {
                return None;
            }
            Some((item.output_column_id, column_id))
        })
        .collect()
}

pub(crate) fn remap_sort_keys_through_project(
    arena: &ScalarArena,
    items: &[ScalarSortKey],
    project_items: &[ScalarProjectItem],
    source_columns: &ColumnIdSet,
) -> Option<Vec<ScalarSortKey>> {
    items
        .iter()
        .map(|item| {
            let output_col = scalar_expr_to_column_id(arena, item.expr)?;
            let project_item = project_items
                .iter()
                .find(|project_item| project_item.output_column_id == output_col)?;
            let source_col = scalar_expr_to_column_id(arena, project_item.expr)?;
            if source_col == ColumnId::UNSET || project_item.output_column_id == ColumnId::UNSET {
                return None;
            }
            if !source_columns.contains(source_col) {
                // A Project output id can represent a slot materialized only at
                // the Project boundary, such as a FULL OUTER USING merged
                // column. Pushdown is valid only when the child exposes the
                // remapped source slot directly.
                return None;
            }
            Some(ScalarSortKey {
                expr: project_item.expr,
                asc: item.asc,
                nulls_first: item.nulls_first,
                display: project_item.expr_display.clone(),
            })
        })
        .collect()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ScanTopNCapability {
    NoOrdering,
    #[allow(
        dead_code,
        reason = "Retained for staged SQL planner migration consumers and test helpers."
    )]
    OrderedTopK,
}

pub(crate) fn default_scan_topn_capability() -> ScanTopNCapability {
    ScanTopNCapability::NoOrdering
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis::{ExprKind, LiteralValue, ProjectItem, SortItem, TypedExpr};
    use crate::optimizer::scalar::{
        ColumnDisplay, HashableLiteral, ScalarArena, ScalarNode, SortKey as ScalarSortKey,
    };
    use crate::planner::optimizer_bridge::scalar::{intern_project_items, intern_sort_items};
    use arrow::datatypes::DataType;

    fn col(id: u32, name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId(id),
                qualifier: None,
                column: name.to_string(),
            },
            data_type: DataType::Int64,
            nullable: true,
        }
    }

    fn qualified_col(id: u32, qualifier: &str, name: &str, nullable: bool) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId(id),
                qualifier: Some(qualifier.to_string()),
                column: name.to_string(),
            },
            data_type: DataType::Utf8,
            nullable,
        }
    }

    fn sort_item(id: u32, asc: bool, nulls_first: bool) -> SortItem {
        SortItem {
            expr: col(id, &format!("c{id}")),
            asc,
            nulls_first,
        }
    }

    fn sort_key(id: u32, asc: bool, nulls_first: bool) -> SortKey {
        SortKey {
            column: ColumnId(id),
            asc,
            nulls_first,
        }
    }

    fn scalar_col(arena: &mut ScalarArena, id: u32, ty: DataType, nullable: bool) -> ScalarSortKey {
        let expr = arena.intern(ScalarNode::ColumnRef(ColumnId(id)), ty, nullable);
        ScalarSortKey {
            expr,
            asc: true,
            nulls_first: false,
            display: Some(ColumnDisplay {
                qualifier: None,
                column: format!("c{id}"),
            }),
        }
    }

    #[test]
    fn topn_window_requires_finite_non_negative_limit_and_offset() {
        assert_eq!(
            TopNWindow::from_limit_offset(Some(10), Some(2)),
            Some(TopNWindow {
                limit: 10,
                offset: 2
            })
        );
        assert_eq!(TopNWindow::from_limit_offset(None, Some(2)), None);
        assert_eq!(TopNWindow::from_limit_offset(Some(-1), Some(0)), None);
        assert_eq!(TopNWindow::from_limit_offset(Some(1), Some(-1)), None);
    }

    #[test]
    fn topn_window_covers_required_range() {
        let inner = TopNWindow {
            limit: 20,
            offset: 0,
        };
        let outer = TopNWindow {
            limit: 5,
            offset: 10,
        };
        assert!(inner.covers(outer));
        assert!(!outer.covers(inner));
    }

    #[test]
    fn collect_column_ids_finds_nested_binary_columns() {
        let mut arena = ScalarArena::new();
        let left = arena.intern(ScalarNode::ColumnRef(ColumnId(1)), DataType::Int64, true);
        let right = arena.intern(ScalarNode::ColumnRef(ColumnId(2)), DataType::Int64, true);
        let expr = arena.intern(
            ScalarNode::BinaryOp {
                op: crate::common::BinOp::Add,
                left,
                right,
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            },
            DataType::Int64,
            true,
        );

        let columns = collect_column_ids(&arena, expr);

        assert!(columns.contains(ColumnId(1)));
        assert!(columns.contains(ColumnId(2)));
        assert_eq!(columns.len(), 2);
    }

    #[test]
    fn collect_column_ids_ignores_literals_and_unset_columns() {
        let mut arena = ScalarArena::new();
        let unset = arena.intern(
            ScalarNode::ColumnRef(ColumnId::UNSET),
            DataType::Int64,
            true,
        );
        let lit = arena.intern(
            ScalarNode::Literal(HashableLiteral(LiteralValue::Int(7))),
            DataType::Int64,
            false,
        );
        let expr = arena.intern(
            ScalarNode::BinaryOp {
                op: crate::common::BinOp::Add,
                left: unset,
                right: lit,
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            },
            DataType::Int64,
            true,
        );

        assert!(collect_column_ids(&arena, expr).is_empty());
    }

    #[test]
    fn sort_keys_use_equivalence_classes() {
        let mut eq = EquivalenceClasses::default();
        eq.merge_pair(ColumnId(1), ColumnId(2));
        let mut arena = ScalarArena::new();
        let left_items = intern_sort_items(&mut arena, &[sort_item(1, true, false)]);
        let right_items = intern_sort_items(&mut arena, &[sort_item(2, true, false)]);
        let left = sort_keys_to_keys(&arena, &left_items).unwrap();
        let right = sort_keys_to_keys(&arena, &right_items).unwrap();
        assert!(sort_keys_equivalent(&left, &right, Some(&eq)));
    }

    #[test]
    fn sort_keys_reject_direction_or_null_order_mismatch() {
        let mut arena = ScalarArena::new();
        let asc_items = intern_sort_items(&mut arena, &[sort_item(1, true, false)]);
        let desc_items = intern_sort_items(&mut arena, &[sort_item(1, false, false)]);
        let nulls_first_items = intern_sort_items(&mut arena, &[sort_item(1, true, true)]);
        let asc = sort_keys_to_keys(&arena, &asc_items).unwrap();
        let desc = sort_keys_to_keys(&arena, &desc_items).unwrap();
        let nulls_first = sort_keys_to_keys(&arena, &nulls_first_items).unwrap();
        assert!(!sort_keys_equivalent(&asc, &desc, None));
        assert!(!sort_keys_equivalent(&asc, &nulls_first, None));
    }

    #[test]
    fn ordering_covers_required_prefix() {
        let provided = vec![sort_key(1, true, false), sort_key(2, false, true)];
        let required = vec![sort_key(1, true, false)];
        assert!(ordering_covers(&provided, &required, None));
    }

    #[test]
    fn ordering_covers_rejects_shorter_provided_ordering() {
        let provided = vec![sort_key(1, true, false)];
        let required = vec![sort_key(1, true, false), sort_key(2, true, false)];
        assert!(!ordering_covers(&provided, &required, None));
    }

    #[test]
    fn ordering_covers_rejects_direction_or_null_order_mismatch() {
        let required = vec![sort_key(1, true, false)];
        assert!(!ordering_covers(
            &[sort_key(1, false, false)],
            &required,
            None
        ));
        assert!(!ordering_covers(
            &[sort_key(1, true, true)],
            &required,
            None
        ));
    }

    #[test]
    fn ordering_covers_uses_equivalence_classes() {
        let mut eq = EquivalenceClasses::default();
        eq.merge_pair(ColumnId(1), ColumnId(2));
        assert!(ordering_covers(
            &[sort_key(1, true, false)],
            &[sort_key(2, true, false)],
            Some(&eq)
        ));
    }

    #[test]
    fn project_remap_accepts_column_refs_only() {
        let project_items = vec![
            ProjectItem {
                expr: col(1, "a"),
                output_name: "x".to_string(),
                output_column_id: ColumnId(10),
            },
            ProjectItem {
                expr: TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::Int(7)),
                    data_type: DataType::Int64,
                    nullable: false,
                },
                output_name: "lit".to_string(),
                output_column_id: ColumnId(11),
            },
        ];
        let mut arena = ScalarArena::new();
        let project_items = intern_project_items(&mut arena, &project_items);
        let sort_10 = intern_sort_items(&mut arena, &[sort_item(10, true, false)]);
        let sort_11 = intern_sort_items(&mut arena, &[sort_item(11, true, false)]);
        let source_columns = ColumnIdSet::from_columns([ColumnId(1)]);

        assert_eq!(
            passthrough_project_column_remap(&arena, &project_items),
            vec![(ColumnId(10), ColumnId(1))]
        );
        assert!(
            remap_sort_keys_through_project(&arena, &sort_10, &project_items, &source_columns)
                .is_some()
        );
        assert!(
            remap_sort_keys_through_project(&arena, &sort_11, &project_items, &source_columns)
                .is_none()
        );
    }

    #[test]
    fn scalar_project_remap_accepts_column_refs_only() {
        let mut arena = ScalarArena::new();
        let source_key = scalar_col(&mut arena, 1, DataType::Utf8, false);
        let output_key = scalar_col(&mut arena, 10, DataType::Utf8, false);
        let literal = arena.intern(
            ScalarNode::Literal(HashableLiteral(LiteralValue::Int(7))),
            DataType::Int64,
            false,
        );
        let project_items = vec![
            ScalarProjectItem {
                expr: source_key.expr,
                output_name: "x".to_string(),
                output_column_id: ColumnId(10),
                expr_display: Some(ColumnDisplay {
                    qualifier: Some("src".to_string()),
                    column: "source_name".to_string(),
                }),
            },
            ScalarProjectItem {
                expr: literal,
                output_name: "lit".to_string(),
                output_column_id: ColumnId(11),
                expr_display: None,
            },
        ];
        let source_columns = ColumnIdSet::from_columns([ColumnId(1)]);

        assert_eq!(
            passthrough_project_column_remap(&arena, &project_items),
            vec![(ColumnId(10), ColumnId(1))]
        );
        let remapped =
            remap_sort_keys_through_project(&arena, &[output_key], &project_items, &source_columns)
                .expect("sort key should remap through passthrough scalar project");
        assert_eq!(remapped.len(), 1);
        assert_eq!(remapped[0].expr, source_key.expr);
        assert_eq!(remapped[0].display, project_items[0].expr_display);

        let literal_output_key = scalar_col(&mut arena, 11, DataType::Int64, false);
        assert!(
            remap_sort_keys_through_project(
                &arena,
                &[literal_output_key],
                &project_items,
                &source_columns
            )
            .is_none()
        );
    }

    #[test]
    fn project_remap_rejects_source_missing_from_child() {
        let mut arena = ScalarArena::new();
        let identity = scalar_col(&mut arena, 29, DataType::Int64, true);
        let project_items = vec![ScalarProjectItem {
            expr: identity.expr,
            output_name: "k1".to_string(),
            output_column_id: ColumnId(29),
            expr_display: Some(ColumnDisplay {
                qualifier: None,
                column: "k1".to_string(),
            }),
        }];
        let source_columns = ColumnIdSet::from_columns([ColumnId(30)]);

        assert!(
            remap_sort_keys_through_project(&arena, &[identity], &project_items, &source_columns)
                .is_none(),
            "a source slot that is not produced by the child must not be pushed below the Project"
        );
    }

    #[test]
    fn project_remap_accepts_unqualified_identity_produced_by_child() {
        let mut arena = ScalarArena::new();
        let identity = scalar_col(&mut arena, 5, DataType::Int64, true);
        let project_items = vec![ScalarProjectItem {
            expr: identity.expr,
            output_name: "id".to_string(),
            output_column_id: ColumnId(5),
            expr_display: Some(ColumnDisplay {
                qualifier: None,
                column: "id".to_string(),
            }),
        }];
        let source_columns = ColumnIdSet::from_columns([ColumnId(5)]);

        assert!(
            remap_sort_keys_through_project(&arena, &[identity], &project_items, &source_columns)
                .is_some(),
            "an identity projection is executable when the child exposes the same slot"
        );
    }

    #[test]
    fn project_remap_preserves_passthrough_expr_metadata() {
        let project_items = vec![ProjectItem {
            expr: qualified_col(1, "src", "source_name", false),
            output_name: "alias_name".to_string(),
            output_column_id: ColumnId(10),
        }];
        let mut arena = ScalarArena::new();
        let project_items = intern_project_items(&mut arena, &project_items);
        let sort_10 = intern_sort_items(&mut arena, &[sort_item(10, false, true)]);
        let source_columns = ColumnIdSet::from_columns([ColumnId(1)]);

        let remapped =
            remap_sort_keys_through_project(&arena, &sort_10, &project_items, &source_columns)
                .expect("sort item should remap through passthrough project");

        assert_eq!(remapped.len(), 1);
        assert!(!remapped[0].asc);
        assert!(remapped[0].nulls_first);
        assert_eq!(arena.data_type(remapped[0].expr), &DataType::Utf8);
        assert!(!arena.nullable(remapped[0].expr));
        assert_eq!(
            scalar_expr_to_column_id(&arena, remapped[0].expr),
            Some(ColumnId(1))
        );
        let display = remapped[0].display.as_ref().unwrap();
        assert_eq!(display.qualifier.as_deref(), Some("src"));
        assert_eq!(display.column, "source_name");
    }

    #[test]
    fn default_scan_capability_does_not_claim_ordered_topk() {
        assert_eq!(
            default_scan_topn_capability(),
            ScanTopNCapability::NoOrdering
        );
    }
}
