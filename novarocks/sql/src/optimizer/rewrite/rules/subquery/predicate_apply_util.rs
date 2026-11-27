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

//! Shared helpers for the EXISTS/IN to-join rules (ExistentialApplyToJoin,
//! QuantifiedApplyToJoin). Locates the inner subquery's correlated WHERE and
//! lifts it into a join ON condition, leaving an outer-reference-free `right`
//! subtree.

use std::collections::HashSet;

use super::scalar_utils;
use crate::column_id::ColumnId;
use crate::optimizer::operator::{Operator, ProjectOp, ScalarProjectItem};
use crate::optimizer::opt_expr::OptExpr;
use crate::optimizer::scalar::{ScalarArena, ScalarId};

pub(super) struct LiftedInnerOpt {
    pub right: OptExpr,
    pub on_predicate: Option<ScalarId>,
}

// A rejected candidate short-circuits separately from a typed control failure.
macro_rules! candidate_or_none {
    ($candidate:expr) => {
        match $candidate {
            Some(value) => value,
            None => return Ok(None),
        }
    };
}

pub(super) fn lift_correlated_inner_opt(
    inner: OptExpr,
    outer_correlation_column_ids: &[ColumnId],
    arena: &mut ScalarArena,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<Option<LiftedInnerOpt>, crate::compiler::SqlCompileError> {
    let OptExpr {
        op,
        mut children,
        required_output_columns,
    } = inner;
    Ok(match op {
        Operator::LogicalProject(project) => {
            if children.len() != 1 {
                return Ok(None);
            }
            let filter_plan = children.remove(0);
            let OptExpr {
                op,
                mut children,
                required_output_columns: _,
            } = filter_plan;
            let Operator::LogicalFilter(filter) = op else {
                return Ok(None);
            };
            if children.len() != 1 {
                return Ok(None);
            }
            let input = children.remove(0);
            let predicate = filter.predicate;
            let items = candidate_or_none!(expose_predicate_inner_columns_opt(
                arena,
                project.items,
                predicate,
                &input,
                outer_correlation_column_ids,
                control
            )?);
            let mut right = OptExpr::new(
                Operator::LogicalProject(ProjectOp {
                    items,
                    output_qualifier: project.output_qualifier,
                }),
                vec![input],
            );
            right.required_output_columns = required_output_columns;
            Some(LiftedInnerOpt {
                right,
                on_predicate: Some(predicate),
            })
        }
        Operator::LogicalFilter(filter) => {
            if children.len() != 1 {
                return Ok(None);
            }
            let input = children.remove(0);
            let predicate = filter.predicate;
            if !candidate_or_none!(predicate_inner_refs_available_opt(
                arena,
                predicate,
                &input,
                outer_correlation_column_ids
            )) {
                return Ok(None);
            }
            Some(LiftedInnerOpt {
                right: input,
                on_predicate: Some(predicate),
            })
        }
        _ => None,
    })
}

fn expose_predicate_inner_columns_opt(
    arena: &mut ScalarArena,
    mut items: Vec<ScalarProjectItem>,
    predicate: ScalarId,
    child: &OptExpr,
    outer_correlation_column_ids: &[ColumnId],
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<Option<Vec<ScalarProjectItem>>, crate::compiler::SqlCompileError> {
    let outer_ids: HashSet<ColumnId> = outer_correlation_column_ids.iter().copied().collect();
    let projected_ids: HashSet<ColumnId> = items.iter().map(|item| item.output_column_id).collect();
    let mut missing_project_ids: HashSet<ColumnId> =
        scalar_utils::collect_column_ids(arena, predicate)
            .into_iter()
            .filter(|column_id| !outer_ids.contains(column_id))
            .filter(|column_id| !projected_ids.contains(column_id))
            .collect();
    if missing_project_ids.is_empty() {
        return Ok(Some(items));
    }

    for output in candidate_or_none!(scalar_utils::opt_output_columns(child, arena).ok()) {
        if missing_project_ids.remove(&output.column_id) {
            items.push(scalar_utils::project_item_for_column(
                arena, &output, control,
            )?);
        }
    }

    Ok(if missing_project_ids.is_empty() {
        Some(items)
    } else {
        None
    })
}

fn predicate_inner_refs_available_opt(
    arena: &ScalarArena,
    predicate: ScalarId,
    child: &OptExpr,
    outer_correlation_column_ids: &[ColumnId],
) -> Option<bool> {
    let outer_ids: HashSet<ColumnId> = outer_correlation_column_ids.iter().copied().collect();
    let inner_ids: HashSet<ColumnId> = scalar_utils::collect_column_ids(arena, predicate)
        .into_iter()
        .filter(|column_id| !outer_ids.contains(column_id))
        .collect();
    if inner_ids.is_empty() {
        return Some(true);
    }

    let child_output_ids: HashSet<ColumnId> = scalar_utils::opt_output_columns(child, arena)
        .ok()?
        .into_iter()
        .map(|output| output.column_id)
        .collect();
    Some(
        inner_ids
            .into_iter()
            .all(|column_id| child_output_ids.contains(&column_id)),
    )
}

#[cfg(test)]
mod legacy {
    use std::collections::HashSet;

    use arrow::datatypes::DataType;

    use crate::analysis::{BinOp, ExprKind, LiteralValue, ProjectItem, TypedExpr};
    use crate::column_id::ColumnId;
    use crate::optimizer::rewrite::rules::utils::collect_column_id_refs;
    use crate::planner::logical::{LogicalPlanKind, LogicalPlanNode};
    use crate::planner::payload::PlanProjectNode;
    use crate::planner::plan_output_columns;

    /// Result of lifting a correlated subquery's WHERE into a join ON.
    #[allow(dead_code)] // Consumed by unregistered Task 4/Task 5 rules until Task 6.
    pub(super) struct LiftedInner {
        /// The outer subtree's right child for the join (no outer references).
        pub right: LogicalPlanNode,
        /// The predicate lifted out of the inner Filter (correlation + residual),
        /// or None when the inner had no Filter to lift.
        pub on_predicate: Option<TypedExpr>,
    }

    /// For a correlated Apply.right of shape `[Project?] Filter(<rel>)`, return the
    /// `<rel>` (with the Project re-applied if present) plus the Filter predicate to
    /// move into the ON. Returns None if the expected shape is absent or if the
    /// projected right side cannot expose the inner columns referenced by the lifted
    /// predicate (caller -> Unchanged). For an uncorrelated inner, callers should
    /// NOT call this - they keep `right` intact and build the ON from the IN key /
    /// `true`.
    #[allow(dead_code)] // Consumed by unregistered Task 4/Task 5 rules until Task 6.
    pub(super) fn lift_correlated_inner(
        inner: LogicalPlanNode,
        outer_correlation_column_ids: &[ColumnId],
    ) -> Option<LiftedInner> {
        let LogicalPlanNode {
            kind,
            mut children,
            required_output_columns,
        } = inner;
        match kind {
            LogicalPlanKind::Project(p) => {
                if children.len() != 1 {
                    return None;
                }
                let filter_plan = children.remove(0);
                let LogicalPlanNode {
                    kind,
                    mut children,
                    required_output_columns: _,
                } = filter_plan;
                let LogicalPlanKind::Filter(f) = kind else {
                    return None;
                };
                if children.len() != 1 {
                    return None;
                }
                let input = children.remove(0);
                let predicate = f.predicate;
                let items = expose_predicate_inner_columns(
                    p.items,
                    &predicate,
                    &input,
                    outer_correlation_column_ids,
                )?;
                Some(LiftedInner {
                    right: LogicalPlanNode::new(
                        LogicalPlanKind::Project(PlanProjectNode {
                            items,
                            output_qualifier: p.output_qualifier,
                        }),
                        vec![input],
                        required_output_columns,
                    ),
                    on_predicate: Some(predicate),
                })
            }
            LogicalPlanKind::Filter(f) => {
                if children.len() != 1 {
                    return None;
                }
                let input = children.remove(0);
                let predicate = f.predicate;
                if !predicate_inner_refs_available(
                    &predicate,
                    &input,
                    outer_correlation_column_ids,
                )? {
                    return None;
                }
                Some(LiftedInner {
                    right: input,
                    on_predicate: Some(predicate),
                })
            }
            _ => None,
        }
    }

    fn expose_predicate_inner_columns(
        mut items: Vec<ProjectItem>,
        predicate: &TypedExpr,
        child: &LogicalPlanNode,
        outer_correlation_column_ids: &[ColumnId],
    ) -> Option<Vec<ProjectItem>> {
        let outer_ids: HashSet<ColumnId> = outer_correlation_column_ids.iter().copied().collect();
        let projected_ids: HashSet<ColumnId> =
            items.iter().map(|item| item.output_column_id).collect();
        let mut missing_project_ids: HashSet<ColumnId> = collect_column_id_refs(predicate)
            .into_iter()
            .filter(|column_id| !outer_ids.contains(column_id))
            .filter(|column_id| !projected_ids.contains(column_id))
            .collect();

        if missing_project_ids.is_empty() {
            return Some(items);
        }

        for output in plan_output_columns(child).ok()? {
            if missing_project_ids.remove(&output.column_id) {
                items.push(ProjectItem {
                    expr: TypedExpr {
                        kind: ExprKind::ColumnRef {
                            column_id: output.column_id,
                            qualifier: None,
                            column: output.name.clone(),
                        },
                        value_type: output.value_type.clone(),
                    },
                    output_name: output.name,
                    output_column_id: output.column_id,
                });
            }
        }

        if missing_project_ids.is_empty() {
            Some(items)
        } else {
            None
        }
    }

    fn predicate_inner_refs_available(
        predicate: &TypedExpr,
        child: &LogicalPlanNode,
        outer_correlation_column_ids: &[ColumnId],
    ) -> Option<bool> {
        let outer_ids: HashSet<ColumnId> = outer_correlation_column_ids.iter().copied().collect();
        let inner_ids: HashSet<ColumnId> = collect_column_id_refs(predicate)
            .into_iter()
            .filter(|column_id| !outer_ids.contains(column_id))
            .collect();
        if inner_ids.is_empty() {
            return Some(true);
        }

        let child_output_ids: HashSet<ColumnId> = plan_output_columns(child)
            .ok()?
            .into_iter()
            .map(|output| output.column_id)
            .collect();
        Some(
            inner_ids
                .into_iter()
                .all(|column_id| child_output_ids.contains(&column_id)),
        )
    }

    /// `Literal(true)` Boolean expr (uncorrelated EXISTS join ON).
    #[allow(dead_code)] // Consumed by unregistered Task 4 rule until Task 6.
    pub(super) fn literal_true() -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Bool(true)),
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Boolean, false),
        }
    }

    /// Build a bare `Eq` Boolean predicate `left = right` (the IN join key).
    #[allow(dead_code)]
    pub(super) fn eq(left: TypedExpr, right: TypedExpr) -> TypedExpr {
        crate::common::binary_operator::test_operator(
            BinOp::Eq,
            left,
            right,
            novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
        )
    }
}

#[cfg(test)]
use legacy::lift_correlated_inner;

#[cfg(test)]
mod tests {
    use crate::planner::logical::*;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::analysis::{ExprKind, LiteralValue, OutputColumn, ProjectItem, TypedExpr};
    use crate::column_id::ColumnId;
    use crate::planner::logical::LogicalPlanKind;
    use crate::planner::payload::{PlanFilterNode, PlanProjectNode, PlanScanNode};
    use crate::planner::table::TableDef;

    const INNER_K: ColumnId = ColumnId(1);
    const OUTER_K: ColumnId = ColumnId(2);
    const CONST_ONE: ColumnId = ColumnId(3);
    const MISSING_INNER: ColumnId = ColumnId(4);

    fn scan() -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanKind::scan(PlanScanNode {
                database: "default".to_string(),
                table: TableDef {
                    name: "t".to_string(),
                    columns: vec![],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: crate::compiler::mv_rewrite::test_scan_source(
                        crate::planner::table::SqlScanKind::ConnectorRead,
                    ),
                },
                alias: None,
                columns: vec![OutputColumn {
                    column_id: INNER_K,
                    name: "k".to_string(),
                    value_type: novarocks_type_contract::FunctionValueType::new(
                        DataType::Int64,
                        false,
                    ),

                    is_internal: false,
                }],
                predicates: vec![],
                required_columns: None,
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        )
    }

    fn col_ref(id: ColumnId, name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: id,
                qualifier: None,
                column: name.to_string(),
            },
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
        }
    }

    fn typed_col_ref(id: ColumnId, name: &str, data_type: DataType) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: id,
                qualifier: None,
                column: name.to_string(),
            },
            value_type: novarocks_type_contract::FunctionValueType::new(data_type, false),
        }
    }

    fn predicate() -> TypedExpr {
        crate::common::binary_operator::test_operator(
            crate::common::BinOp::Eq,
            col_ref(INNER_K, "k"),
            col_ref(OUTER_K, "k"),
            novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
        )
    }

    fn typed_inner_left_predicate(inner_type: DataType, outer_type: DataType) -> TypedExpr {
        crate::common::binary_operator::test_operator(
            crate::common::BinOp::Eq,
            typed_col_ref(INNER_K, "k", inner_type),
            typed_col_ref(OUTER_K, "k", outer_type),
            novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
        )
    }

    fn missing_inner_predicate() -> TypedExpr {
        crate::common::binary_operator::test_operator(
            crate::common::BinOp::Eq,
            col_ref(MISSING_INNER, "missing_k"),
            col_ref(OUTER_K, "k"),
            novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
        )
    }

    fn project(input: LogicalPlanNode) -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanKind::Project(PlanProjectNode {
                items: vec![ProjectItem {
                    expr: col_ref(INNER_K, "k"),
                    output_name: "k".to_string(),
                    output_column_id: INNER_K,
                }],
                output_qualifier: None,
            }),
            vec![input],
            None,
        )
    }

    fn project_literal(input: LogicalPlanNode) -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanKind::Project(PlanProjectNode {
                items: vec![ProjectItem {
                    expr: TypedExpr {
                        kind: ExprKind::Literal(LiteralValue::Int(1)),
                        value_type: novarocks_type_contract::FunctionValueType::new(
                            DataType::Int64,
                            false,
                        ),
                    },
                    output_name: "1".to_string(),
                    output_column_id: CONST_ONE,
                }],
                output_qualifier: None,
            }),
            vec![input],
            None,
        )
    }

    fn filter(input: LogicalPlanNode) -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanKind::Filter(PlanFilterNode {
                predicate: predicate(),
            }),
            vec![input],
            None,
        )
    }

    fn filter_missing_inner(input: LogicalPlanNode) -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanKind::Filter(PlanFilterNode {
                predicate: missing_inner_predicate(),
            }),
            vec![input],
            None,
        )
    }

    fn filter_with_predicate(input: LogicalPlanNode, predicate: TypedExpr) -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanKind::Filter(PlanFilterNode { predicate }),
            vec![input],
            None,
        )
    }

    #[test]
    fn lift_project_filter_returns_rel_and_pred() {
        let lifted =
            lift_correlated_inner(project(filter(scan())), &[OUTER_K]).expect("inner must lift");

        assert!(lifted.on_predicate.is_some());
        let LogicalPlanKind::Project(_) = &lifted.right.kind else {
            panic!("expected project");
        };
        assert!(matches!(
            &lifted.right.unary_input().kind,
            LogicalPlanKind::Scan(_)
        ));
    }

    #[test]
    fn lift_project_filter_adds_missing_inner_predicate_columns() {
        let lifted = lift_correlated_inner(project_literal(filter(scan())), &[OUTER_K])
            .expect("inner must lift");

        let LogicalPlanKind::Project(project) = &lifted.right.kind else {
            panic!("expected project");
        };
        assert!(
            project
                .items
                .iter()
                .any(|item| item.output_column_id == INNER_K),
            "lifted right project must expose INNER_K for the join ON"
        );
    }

    #[test]
    fn lift_project_scan_returns_none() {
        assert!(lift_correlated_inner(project(scan()), &[OUTER_K]).is_none());
    }

    #[test]
    fn lift_bare_filter() {
        let lifted = lift_correlated_inner(filter(scan()), &[OUTER_K]).expect("inner must lift");

        assert!(lifted.on_predicate.is_some());
        assert!(matches!(&lifted.right.kind, LogicalPlanKind::Scan(_)));
    }

    #[test]
    fn lift_preserves_selected_correlation_order_and_cast() {
        let lifted = lift_correlated_inner(
            filter_with_predicate(
                scan(),
                typed_inner_left_predicate(DataType::Int32, DataType::Int64),
            ),
            &[OUTER_K],
        )
        .expect("inner must lift");
        let predicate = lifted.on_predicate.expect("lifted predicate");
        let Some((crate::common::BinOp::Eq, [left, right])) =
            crate::common::binary_operator::typed_comparison(&predicate)
        else {
            panic!("expected selected equality, got: {predicate:?}");
        };
        let ExprKind::Cast { expr, target, .. } = &left.kind else {
            panic!("expected original inner key cast, got: {left:?}");
        };
        assert_eq!(target, &DataType::Int64);
        assert_column_ref(expr, INNER_K);
        assert_column_ref(right, OUTER_K);
    }

    #[test]
    fn lift_bare_filter_missing_inner_ref_returns_none() {
        assert!(lift_correlated_inner(filter_missing_inner(scan()), &[OUTER_K]).is_none());
    }

    #[test]
    fn lift_scan_returns_none() {
        assert!(lift_correlated_inner(scan(), &[OUTER_K]).is_none());
    }

    fn assert_column_ref(expr: &TypedExpr, expected: ColumnId) {
        let ExprKind::ColumnRef { column_id, .. } = &expr.kind else {
            panic!("expected ColumnRef, got: {expr:?}");
        };
        assert_eq!(*column_id, expected);
    }
}
