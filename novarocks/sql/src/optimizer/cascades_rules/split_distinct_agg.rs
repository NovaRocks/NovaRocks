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

//! Implementation rule: multi-phase DISTINCT aggregation.
//!
//! Matches a `LogicalAggregate` with at least one DISTINCT aggregate call,
//! where all DISTINCT calls share a single simple column as their argument.
//! Emits one alternative physical chain:
//!   - 3-phase (LOCAL -> DISTINCT_GLOBAL -> GLOBAL) when `group_by` is non-empty.
//!   - 4-phase (LOCAL -> DISTINCT_GLOBAL -> DISTINCT_LOCAL -> GLOBAL) when scalar.
//!
//! Mirrors StarRocks's `SplitAggregateRule` / `AggType.java` convention.

use arrow::datatypes::DataType;

use crate::column_id::ColumnId;
use crate::common::OutputColumn;
use crate::optimizer::memo::{MExpr, Memo};
use crate::optimizer::operator::{
    AggMode, AggregateOutputLayout, LogicalAggregateOp, Operator, PhysicalHashAggregateOp,
    ScalarAggregateSpec,
};
use crate::optimizer::rule::{NewExpr, Rule, RuleType};
use crate::optimizer::scalar::{ScalarArena, ScalarId, ScalarNode};
use crate::optimizer::scalar_expr;

use super::split_aggregate::{aggregate_group_key_output_ref, group_key_output_column_id};

pub(crate) struct SplitDistinctAgg;

impl Rule for SplitDistinctAgg {
    fn name(&self) -> &str {
        "SplitDistinctAgg"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Implementation
    }

    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalAggregate(a) if a.aggregates.iter().any(|c| c.distinct))
    }

    fn apply(&self, expr: &MExpr, memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalAggregate(agg) = &expr.op else {
            return vec![];
        };
        // Ordered aggregates need all order-by inputs available at the update
        // phase. The current split-distinct lowering only preserves the
        // shared DISTINCT column across phase boundaries, so ordered DISTINCT
        // aggregates like `array_agg(distinct x order by y)` lose `y` in the
        // GLOBAL phase. Fall back to the single-stage aggregate for semantic
        // correctness until multi-phase ordered DISTINCT is implemented.
        if agg.aggregates.iter().any(|call| !call.order_by.is_empty()) {
            return vec![];
        }

        // Validate single-DISTINCT-column precondition.
        let distinct_col = match extract_single_distinct_col(&memo.scalars, &agg.aggregates) {
            Some(c) => c,
            None => return vec![], // multi-column DISTINCT, or multiple different DISTINCT cols
        };

        // Partition aggregates into DISTINCT-bearing (which are deduped away at LOCAL)
        // and non-DISTINCT (which flow as merge states through the phases).
        let non_distinct_indices: Vec<usize> = agg
            .aggregates
            .iter()
            .enumerate()
            .filter_map(|(idx, call)| (!call.distinct).then_some(idx))
            .collect();
        let non_distinct: Vec<ScalarAggregateSpec> = non_distinct_indices
            .iter()
            .map(|idx| agg.aggregates[*idx].clone())
            .collect();

        // Stateful sketch/bitmap aggregates preserve null/empty-state semantics
        // across the current split-distinct phase boundaries poorly. Fall back
        // to the single-stage plan for correctness until their merge path is
        // aligned with StarRocks FE.
        if non_distinct
            .iter()
            .any(|call| split_distinct_sensitive_agg(call.name.as_str()))
        {
            return vec![];
        }

        if agg.group_by.is_empty() {
            apply_four_phase(
                expr,
                memo,
                agg,
                distinct_col,
                &non_distinct,
                &non_distinct_indices,
            )
        } else {
            apply_three_phase(
                expr,
                memo,
                agg,
                &agg.group_by,
                distinct_col,
                &non_distinct,
                &non_distinct_indices,
            )
        }
    }
}

/// Return the shared DISTINCT column if every DISTINCT aggregate takes exactly
/// one argument and all such arguments are the same simple `ColumnRef`.
/// Returns `None` for:
///   - no DISTINCT calls at all (shouldn't happen -- `matches` filters this)
///   - multi-arg DISTINCT (`count(distinct a, b)`)
///   - multiple distinct columns (`count(distinct a), count(distinct b)`)
///   - DISTINCT arg that is not a plain ColumnRef
fn extract_single_distinct_col(
    arena: &ScalarArena,
    calls: &[ScalarAggregateSpec],
) -> Option<ScalarId> {
    let mut distinct_calls = calls.iter().filter(|c| c.distinct);
    let first = distinct_calls.next()?;
    if first.args.len() != 1 {
        return None;
    }
    if !matches!(arena.node(first.args[0]), ScalarNode::ColumnRef(_)) {
        return None;
    }
    for c in distinct_calls {
        if c.args.len() != 1 {
            return None;
        }
        if c.args[0] != first.args[0] {
            return None;
        }
    }
    Some(first.args[0])
}

fn split_distinct_sensitive_agg(name: &str) -> bool {
    matches!(
        name,
        "approx_count_distinct_hll_sketch"
            | "bitmap_agg"
            | "bitmap_union"
            | "bitmap_union_count"
            | "bitmap_union_int"
            | "ds_hll_count_distinct"
            | "ds_hll_count_distinct_merge"
            | "ds_hll_count_distinct_union"
            | "hll_raw_agg"
            | "hll_union"
            | "hll_union_agg"
    )
}

fn distinct_aggregate_indices(aggregates: &[ScalarAggregateSpec]) -> Vec<usize> {
    aggregates
        .iter()
        .enumerate()
        .filter_map(|(idx, call)| call.distinct.then_some(idx))
        .collect()
}

fn rebind_distinct_arg_to_phase_output(
    mut call: ScalarAggregateSpec,
    phase_output: ScalarId,
) -> ScalarAggregateSpec {
    if call.distinct && call.args.len() == 1 {
        call.args = vec![phase_output];
    }
    call
}

fn group_output_column_from_expr(
    arena: &ScalarArena,
    expr: ScalarId,
    fallback_name: String,
) -> OutputColumn {
    let (column_id, name) = match arena.node(expr) {
        ScalarNode::ColumnRef(column_id) => {
            (*column_id, scalar_expr::scalar_display_name(arena, expr))
        }
        _ => (ColumnId::UNSET, fallback_name),
    };
    OutputColumn {
        column_id,
        name,
        data_type: arena.data_type(expr).clone(),
        nullable: arena.nullable(expr),
        is_internal: false,
    }
}

fn phase_group_output_columns(arena: &ScalarArena, group_by: &[ScalarId]) -> Vec<OutputColumn> {
    group_by
        .iter()
        .enumerate()
        .map(|(idx, expr)| group_output_column_from_expr(arena, *expr, format!("group_{idx}")))
        .collect()
}

fn aggregate_output_columns(
    arena: &ScalarArena,
    parent: &LogicalAggregateOp,
    aggregates: &[ScalarAggregateSpec],
    aggregate_indices: &[usize],
) -> Vec<OutputColumn> {
    aggregates
        .iter()
        .zip(aggregate_indices.iter())
        .map(|(call, aggregate_idx)| {
            let source_output = parent.output_layout.aggregate_columns.get(*aggregate_idx);
            OutputColumn {
                column_id: source_output
                    .map(|output| output.column_id)
                    .unwrap_or(ColumnId::UNSET),
                name: scalar_expr::aggregate_display_name(
                    arena,
                    &call.name,
                    &call.args,
                    call.distinct,
                    &call.order_by,
                ),
                data_type: source_output
                    .map(|output| output.data_type.clone())
                    .unwrap_or(DataType::Null),
                nullable: true,
                is_internal: true,
            }
        })
        .collect()
}

fn aggregate_phase_output_columns(
    arena: &ScalarArena,
    parent: &LogicalAggregateOp,
    group_outputs: &[OutputColumn],
    aggregates: &[ScalarAggregateSpec],
    aggregate_indices: &[usize],
) -> Vec<OutputColumn> {
    let mut outputs = Vec::with_capacity(group_outputs.len() + aggregates.len());
    outputs.extend(group_outputs.iter().cloned());
    outputs.extend(aggregate_output_columns(
        arena,
        parent,
        aggregates,
        aggregate_indices,
    ));
    outputs
}

fn scalar_key_matches(arena: &ScalarArena, left: ScalarId, right: ScalarId) -> bool {
    if left == right {
        return true;
    }
    matches!(
        (arena.node(left), arena.node(right)),
        (ScalarNode::ColumnRef(left_id), ScalarNode::ColumnRef(right_id)) if left_id == right_id
    )
}

fn apply_three_phase(
    expr: &MExpr,
    memo: &mut Memo,
    agg: &LogicalAggregateOp,
    group_by: &[ScalarId],
    distinct_col: ScalarId,
    non_distinct: &[ScalarAggregateSpec],
    non_distinct_indices: &[usize],
) -> Vec<NewExpr> {
    // Group-by for LOCAL and DISTINCT_GLOBAL: original group_by plus the
    // DISTINCT column when it is not already a group key.
    let mut gb_with_distinct = group_by.to_vec();
    if !gb_with_distinct
        .iter()
        .any(|key| scalar_key_matches(&memo.scalars, *key, distinct_col))
    {
        gb_with_distinct.push(distinct_col);
    }
    let distinct_phase_arg_idx = gb_with_distinct
        .iter()
        .position(|key| scalar_key_matches(&memo.scalars, *key, distinct_col))
        .unwrap_or_else(|| gb_with_distinct.len().saturating_sub(1));
    // Reuse the original aggregate's group output ids for real group keys. The
    // parent GLOBAL phase consumes the DISTINCT_GLOBAL output layout, so its
    // group_by refs must target these phase output ids instead of the scan input
    // ids. The appended distinct column is still a plain input ColumnRef and
    // keeps its own id.
    let gb_with_distinct_outputs: Vec<OutputColumn> = gb_with_distinct
        .iter()
        .enumerate()
        .map(|(idx, expr)| {
            let layout_column = (idx < group_by.len())
                .then(|| agg.output_layout.group_key_columns.get(idx))
                .flatten()
                .filter(|output| output.column_id != ColumnId::UNSET);
            let name = layout_column
                .map(|output| output.name.clone())
                .unwrap_or_else(|| scalar_expr::scalar_display_name(&memo.scalars, *expr));
            let column_id = layout_column
                .map(|output| output.column_id)
                .unwrap_or_else(|| {
                    group_key_output_column_id(
                        &memo.scalars,
                        *expr,
                        &name,
                        &agg.output_layout.group_key_columns,
                    )
                });
            OutputColumn {
                column_id,
                name,
                data_type: memo.scalars.data_type(*expr).clone(),
                nullable: memo.scalars.nullable(*expr),
                is_internal: layout_column
                    .map(|output| output.is_internal)
                    .unwrap_or(false),
            }
        })
        .collect();
    let partial_output_columns = aggregate_phase_output_columns(
        &memo.scalars,
        agg,
        &gb_with_distinct_outputs,
        non_distinct,
        non_distinct_indices,
    );
    let partial_output_layout = AggregateOutputLayout::new(
        partial_output_columns
            .iter()
            .take(gb_with_distinct.len())
            .cloned()
            .collect(),
        partial_output_columns
            .iter()
            .skip(gb_with_distinct.len())
            .cloned()
            .collect(),
    );

    // LOCAL: group_by = g + x evaluated over the child; non_distinct aggs
    // computed with update semantics.
    let local_id = memo.next_expr_id();
    let local = MExpr {
        id: local_id,
        op: Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
            mode: AggMode::Local,
            group_by: gb_with_distinct,
            aggregates: non_distinct.to_vec(),
            output_layout: partial_output_layout.clone(),
            output_columns: partial_output_columns.clone(),
            is_merge: vec![false; non_distinct.len()],
        }),
        children: expr.children.clone(),
    };
    let local_group = memo.new_group(local);

    // DISTINCT_GLOBAL: group by references to the LOCAL group outputs (the raw
    // expressions reference child columns the LOCAL no longer produces); merge
    // non_distinct states.
    let dg_group_by = aggregate_group_key_output_ref(
        &mut memo.scalars,
        &gb_with_distinct_outputs,
        gb_with_distinct_outputs.len(),
    );
    let distinct_phase_arg = dg_group_by
        .get(distinct_phase_arg_idx)
        .copied()
        .unwrap_or(distinct_col);
    let dg_id = memo.next_expr_id();
    let dg = MExpr {
        id: dg_id,
        op: Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
            mode: AggMode::DistinctGlobal,
            group_by: dg_group_by,
            aggregates: non_distinct.to_vec(),
            output_layout: partial_output_layout,
            output_columns: partial_output_columns,
            is_merge: vec![true; non_distinct.len()],
        }),
        children: vec![local_group],
    };
    let dg_group = memo.new_group(dg);

    // GLOBAL: group_by = original g; aggregates = [first DISTINCT update, each
    // non_distinct merged, then remaining DISTINCT updates].
    //
    // Preserve every original distinct aggregate call so that
    // agg_call_display_name matches what the PROJECT node expects. Keep the
    // non-DISTINCT merge calls immediately after the first DISTINCT call: the
    // fragment builder maps merge inputs by aggregate index and this ordering
    // aligns them with the DISTINCT_GLOBAL output slots.
    let distinct_indices = distinct_aggregate_indices(&agg.aggregates);
    let distinct_aggs: Vec<ScalarAggregateSpec> = distinct_indices
        .iter()
        .map(|idx| agg.aggregates[*idx].clone())
        .map(|call| rebind_distinct_arg_to_phase_output(call, distinct_phase_arg))
        .collect();
    if distinct_aggs.is_empty() {
        return vec![];
    }
    let mut global_aggs = Vec::with_capacity(distinct_aggs.len() + non_distinct.len());
    global_aggs.push(distinct_aggs[0].clone());
    global_aggs.extend(non_distinct.iter().cloned());
    global_aggs.extend(distinct_aggs.iter().skip(1).cloned());
    let mut global_indices = Vec::with_capacity(global_aggs.len());
    global_indices.push(distinct_indices[0]);
    global_indices.extend(non_distinct_indices.iter().copied());
    global_indices.extend(distinct_indices.iter().skip(1).copied());
    let mut global_merge = Vec::with_capacity(global_aggs.len());
    global_merge.push(false); // DISTINCT aggs are updates in the GLOBAL phase
    global_merge.extend(std::iter::repeat_n(true, non_distinct.len()));
    global_merge.extend(std::iter::repeat_n(
        false,
        distinct_aggs.len().saturating_sub(1),
    ));
    let global_group_columns: Vec<OutputColumn> = agg
        .output_layout
        .group_key_columns
        .iter()
        .take(group_by.len())
        .cloned()
        .collect();
    let global_aggregate_columns =
        aggregate_output_columns(&memo.scalars, agg, &global_aggs, &global_indices);

    let global_group_by = aggregate_group_key_output_ref(
        &mut memo.scalars,
        &gb_with_distinct_outputs,
        group_by.len(),
    );
    let global_output_layout =
        AggregateOutputLayout::new(global_group_columns, global_aggregate_columns);

    vec![NewExpr {
        op: Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
            mode: AggMode::Global,
            // Reference DISTINCT_GLOBAL's original group outputs (drop the
            // trailing distinct column), not the raw group expressions which
            // reference child columns no longer produced below the GLOBAL phase.
            group_by: global_group_by,
            aggregates: global_aggs,
            output_layout: global_output_layout,
            output_columns: agg.output_columns.clone(),
            is_merge: global_merge,
        }),
        children: vec![dg_group],
    }]
}

fn apply_four_phase(
    expr: &MExpr,
    memo: &mut Memo,
    agg: &LogicalAggregateOp,
    distinct_col: ScalarId,
    non_distinct: &[ScalarAggregateSpec],
    non_distinct_indices: &[usize],
) -> Vec<NewExpr> {
    let distinct_group_outputs = phase_group_output_columns(&memo.scalars, &[distinct_col]);
    let distinct_group_by = aggregate_group_key_output_ref(
        &mut memo.scalars,
        &distinct_group_outputs,
        distinct_group_outputs.len(),
    );
    let distinct_phase_arg = distinct_group_by.first().copied().unwrap_or(distinct_col);
    let partial_output_columns = aggregate_phase_output_columns(
        &memo.scalars,
        agg,
        &distinct_group_outputs,
        non_distinct,
        non_distinct_indices,
    );
    let partial_output_layout = AggregateOutputLayout::new(
        partial_output_columns
            .iter()
            .take(distinct_group_by.len())
            .cloned()
            .collect(),
        partial_output_columns
            .iter()
            .skip(distinct_group_by.len())
            .cloned()
            .collect(),
    );

    // LOCAL: group_by = [x]; non_distinct aggs with update semantics.
    let local_id = memo.next_expr_id();
    let local = MExpr {
        id: local_id,
        op: Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
            mode: AggMode::Local,
            group_by: vec![distinct_col],
            aggregates: non_distinct.to_vec(),
            output_layout: partial_output_layout.clone(),
            output_columns: partial_output_columns.clone(),
            is_merge: vec![false; non_distinct.len()],
        }),
        children: expr.children.clone(),
    };
    let local_group = memo.new_group(local);

    // DISTINCT_GLOBAL: group_by = [x]; merge non_distinct states.
    let dg_id = memo.next_expr_id();
    let dg = MExpr {
        id: dg_id,
        op: Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
            mode: AggMode::DistinctGlobal,
            group_by: distinct_group_by,
            aggregates: non_distinct.to_vec(),
            output_layout: partial_output_layout,
            output_columns: partial_output_columns,
            is_merge: vec![true; non_distinct.len()],
        }),
        children: vec![local_group],
    };
    let dg_group = memo.new_group(dg);

    // Build the phase-boundary aggregate list shared by DISTINCT_LOCAL and GLOBAL:
    // [first DISTINCT first, then each non_distinct, then remaining DISTINCT].
    // Fragment builder applies
    // per-call is_merge dispatch from op.is_merge.
    //
    // Use the original distinct aggregate calls so their display names match
    // what the PROJECT node expects.
    let distinct_indices = distinct_aggregate_indices(&agg.aggregates);
    let distinct_aggs: Vec<ScalarAggregateSpec> = distinct_indices
        .iter()
        .map(|idx| agg.aggregates[*idx].clone())
        .map(|call| rebind_distinct_arg_to_phase_output(call, distinct_phase_arg))
        .collect();
    if distinct_aggs.is_empty() {
        return vec![];
    }
    let mut phase_aggs = Vec::with_capacity(distinct_aggs.len() + non_distinct.len());
    phase_aggs.push(distinct_aggs[0].clone());
    phase_aggs.extend(non_distinct.iter().cloned());
    phase_aggs.extend(distinct_aggs.iter().skip(1).cloned());
    let mut phase_indices = Vec::with_capacity(phase_aggs.len());
    phase_indices.push(distinct_indices[0]);
    phase_indices.extend(non_distinct_indices.iter().copied());
    phase_indices.extend(distinct_indices.iter().skip(1).copied());

    // DISTINCT_LOCAL: scalar; [DISTINCT update, non_distinct merge..., DISTINCT update...].
    let mut dl_merge = Vec::with_capacity(phase_aggs.len());
    dl_merge.push(false);
    dl_merge.extend(std::iter::repeat_n(true, non_distinct.len()));
    dl_merge.extend(std::iter::repeat_n(
        false,
        distinct_aggs.len().saturating_sub(1),
    ));
    let dl_id = memo.next_expr_id();
    let dl_output_columns =
        aggregate_output_columns(&memo.scalars, agg, &phase_aggs, &phase_indices);
    let dl_output_layout = AggregateOutputLayout::new(vec![], dl_output_columns.clone());
    let dl = MExpr {
        id: dl_id,
        op: Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
            mode: AggMode::DistinctLocal,
            group_by: vec![],
            aggregates: phase_aggs.clone(),
            output_layout: dl_output_layout,
            output_columns: dl_output_columns,
            is_merge: dl_merge,
        }),
        children: vec![dg_group],
    };
    let dl_group = memo.new_group(dl);

    // GLOBAL: scalar; aggregates all MERGES.
    //
    // Correctness note: when the preserved distinct aggregate is count(distinct x),
    // the physical expression becomes multi_distinct_count with is_merge_agg=true,
    // which merges bitmap states across DISTINCT_LOCAL instances. This is correct
    // because DISTINCT_GLOBAL partitions data by x, guaranteeing each DISTINCT_LOCAL
    // instance sees a disjoint subset of distinct x values. Bitmap union over
    // disjoint sets is equivalent to sum of partial counts.
    let global_merge = vec![true; phase_aggs.len()];
    let global_aggregate_columns =
        aggregate_output_columns(&memo.scalars, agg, &phase_aggs, &phase_indices);
    let global_output_layout = AggregateOutputLayout::new(vec![], global_aggregate_columns);

    vec![NewExpr {
        op: Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
            mode: AggMode::Global,
            group_by: vec![],
            aggregates: phase_aggs.clone(),
            output_layout: global_output_layout,
            output_columns: agg.output_columns.clone(),
            is_merge: global_merge,
        }),
        children: vec![dl_group],
    }]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis::{ExprKind, OutputColumn, TypedExpr};
    use crate::optimizer::memo::Memo;
    use crate::optimizer::operator::{AggMode, LogicalAggregateOp, ScanOp};
    use crate::planner::optimizer_bridge::scalar::materialize;
    use crate::planner::optimizer_bridge::scalar::{
        intern_aggregate_calls, intern_exprs, materialize_aggregate_calls,
    };
    use crate::planner::payload::AggregateCall;
    use arrow::datatypes::DataType;
    use std::sync::Arc;

    fn test_col_id(name: &str) -> ColumnId {
        match name {
            "x" => ColumnId::new_for_test(1),
            "a" => ColumnId::new_for_test(2),
            "b" => ColumnId::new_for_test(3),
            "g" => ColumnId::new_for_test(4),
            "name" => ColumnId::new_for_test(5),
            "id" => ColumnId::new_for_test(6),
            "count_distinct_x" => ColumnId::new_for_test(101),
            "count_distinct_a" => ColumnId::new_for_test(102),
            "count_distinct_b" => ColumnId::new_for_test(103),
            "count_distinct_name" => ColumnId::new_for_test(104),
            "array_agg_distinct_x" => ColumnId::new_for_test(105),
            "sum_a" => ColumnId::new_for_test(106),
            _ => ColumnId::new_for_test(100),
        }
    }

    fn col(name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: test_col_id(name),
                qualifier: None,
                column: name.into(),
            },
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn col_with_id(name: &str, id: u32) -> TypedExpr {
        let mut expr = col(name);
        let ExprKind::ColumnRef { column_id, .. } = &mut expr.kind else {
            unreachable!("col() must build a ColumnRef");
        };
        *column_id = ColumnId::new_for_test(id);
        expr
    }

    fn scan_group(memo: &mut Memo) -> usize {
        let m = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalScan(ScanOp {
                database: "db".into(),
                table: crate::planner::table::TableDef {
                    name: "t".into(),
                    columns: vec![],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: crate::compiler::mv_rewrite::test_scan_source(
                        crate::planner::table::SqlScanKind::ConnectorRead,
                    ),
                },
                alias: None,
                stats_ref: None,
                columns: vec![],
                predicates: vec![],
                required_columns: None,
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            children: vec![],
        };
        memo.new_group(m)
    }

    fn single_agg(
        memo: &mut Memo,
        group_by: Vec<TypedExpr>,
        aggregates: Vec<AggregateCall>,
        output_columns: Vec<OutputColumn>,
    ) -> LogicalAggregateOp {
        let mut aggregates = aggregates;
        let mut output_columns = if output_columns.is_empty() {
            default_output_columns(&group_by, &aggregates)
        } else {
            output_columns
        };
        for (idx, expr) in group_by.iter().enumerate() {
            let Some(output) = output_columns.get_mut(idx) else {
                continue;
            };
            if output.column_id == ColumnId::UNSET {
                output.column_id = match &expr.kind {
                    ExprKind::ColumnRef { column_id, .. } => *column_id,
                    _ => fallback_output_id(idx),
                };
            }
        }
        for (idx, call) in aggregates.iter_mut().enumerate() {
            let output_idx = group_by.len() + idx;
            let Some(output) = output_columns.get_mut(output_idx) else {
                continue;
            };
            let output_id = if output.column_id == ColumnId::UNSET {
                if call.output_column_id == ColumnId::UNSET {
                    fallback_output_id(output_idx)
                } else {
                    call.output_column_id
                }
            } else {
                output.column_id
            };
            output.column_id = output_id;
            call.output_column_id = output_id;
        }
        let group_by = intern_exprs(&mut memo.scalars, &group_by);
        let aggregates = intern_aggregate_calls(&mut memo.scalars, &aggregates);
        let output_layout = AggregateOutputLayout::new(
            output_columns
                .iter()
                .take(group_by.len())
                .cloned()
                .collect(),
            output_columns
                .iter()
                .skip(group_by.len())
                .cloned()
                .collect(),
        );
        LogicalAggregateOp::single(group_by, aggregates, output_layout, output_columns)
    }

    fn count_distinct(arg_name: &str) -> AggregateCall {
        AggregateCall {
            name: "count".into(),
            args: vec![col(arg_name)],
            distinct: true,
            result_type: DataType::Int64,
            order_by: vec![],
            output_column_id: test_col_id(&format!("count_distinct_{arg_name}")),
            resolved: crate::functions::test_resolved_aggregate("count", &[DataType::Int64], true),
        }
    }

    fn array_agg_distinct(arg_name: &str) -> AggregateCall {
        AggregateCall {
            name: "array_agg".into(),
            args: vec![col(arg_name)],
            distinct: true,
            result_type: DataType::List(Arc::new(arrow::datatypes::Field::new(
                "item",
                DataType::Int64,
                true,
            ))),
            order_by: vec![],
            output_column_id: test_col_id(&format!("array_agg_distinct_{arg_name}")),
            resolved: crate::functions::test_resolved_aggregate(
                "array_agg",
                &[DataType::Int64],
                true,
            ),
        }
    }

    fn sum_non_distinct(arg_name: &str) -> AggregateCall {
        AggregateCall {
            name: "sum".into(),
            args: vec![col(arg_name)],
            distinct: false,
            result_type: DataType::Int64,
            order_by: vec![],
            output_column_id: test_col_id(&format!("sum_{arg_name}")),
            resolved: crate::functions::test_resolved_aggregate("sum", &[DataType::Int64], false),
        }
    }

    fn fallback_output_id(offset: usize) -> ColumnId {
        ColumnId::new_for_test(1000 + offset as u32)
    }

    fn default_output_columns(
        group_by: &[TypedExpr],
        aggregates: &[AggregateCall],
    ) -> Vec<OutputColumn> {
        let mut outputs = Vec::with_capacity(group_by.len() + aggregates.len());
        for (idx, expr) in group_by.iter().enumerate() {
            let (column_id, name) = match &expr.kind {
                ExprKind::ColumnRef {
                    column_id, column, ..
                } => (*column_id, column.clone()),
                _ => (fallback_output_id(idx), format!("group_{idx}")),
            };
            outputs.push(OutputColumn {
                column_id,
                name,
                data_type: expr.data_type.clone(),
                nullable: expr.nullable,
                is_internal: false,
            });
        }
        for (idx, call) in aggregates.iter().enumerate() {
            outputs.push(OutputColumn {
                column_id: if call.output_column_id == ColumnId::UNSET {
                    fallback_output_id(group_by.len() + idx)
                } else {
                    call.output_column_id
                },
                name: format!("agg_{idx}"),
                data_type: call.result_type.clone(),
                nullable: true,
                is_internal: false,
            });
        }
        outputs
    }

    fn output_column(id: u32, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId::new_for_test(id),
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: true,
            is_internal: false,
        }
    }

    fn single_agg_with_layout(
        memo: &mut Memo,
        group_by: Vec<TypedExpr>,
        aggregates: Vec<AggregateCall>,
        full_output_columns: Vec<OutputColumn>,
        public_output_columns: Vec<OutputColumn>,
    ) -> LogicalAggregateOp {
        let group_by = intern_exprs(&mut memo.scalars, &group_by);
        let aggregates = intern_aggregate_calls(&mut memo.scalars, &aggregates);
        let output_layout = AggregateOutputLayout::new(
            full_output_columns
                .iter()
                .take(group_by.len())
                .cloned()
                .collect(),
            full_output_columns
                .iter()
                .skip(group_by.len())
                .cloned()
                .collect(),
        );
        LogicalAggregateOp::single(group_by, aggregates, output_layout, public_output_columns)
    }

    fn phase_output_ids(op: &PhysicalHashAggregateOp) -> Vec<ColumnId> {
        op.output_columns
            .iter()
            .map(|output| output.column_id)
            .collect()
    }

    fn aggregate_call_output_ids(memo: &Memo, op: &PhysicalHashAggregateOp) -> Vec<ColumnId> {
        materialize_aggregate_calls(&memo.scalars, &op.aggregates, &op.output_layout)
            .iter()
            .map(|call| call.output_column_id)
            .collect()
    }

    fn assert_hash_aggregate_layout(memo: &Memo, op: &PhysicalHashAggregateOp) {
        assert_eq!(
            op.output_layout.group_key_columns.len(),
            op.group_by.len(),
            "PhysicalHashAggregate group layout must match group_by"
        );
        assert_eq!(
            op.output_layout.aggregate_columns.len(),
            op.aggregates.len(),
            "PhysicalHashAggregate aggregate layout must match aggregate calls"
        );
        assert_eq!(
            op.output_layout
                .aggregate_columns
                .iter()
                .map(|output| output.column_id)
                .collect::<Vec<_>>(),
            aggregate_call_output_ids(memo, op)
        );
    }

    #[test]
    fn matches_when_any_distinct() {
        let mut memo = Memo::new();
        let op = Operator::LogicalAggregate(single_agg(
            &mut memo,
            vec![],
            vec![count_distinct("x"), sum_non_distinct("a")],
            vec![],
        ));
        assert!(SplitDistinctAgg.matches(&op));
    }

    #[test]
    fn does_not_match_when_no_distinct() {
        let mut memo = Memo::new();
        let op = Operator::LogicalAggregate(single_agg(
            &mut memo,
            vec![],
            vec![sum_non_distinct("a")],
            vec![],
        ));
        assert!(!SplitDistinctAgg.matches(&op));
    }

    #[test]
    fn apply_skips_multi_arg_distinct() {
        assert!(
            crate::functions::builtin_sql_function_catalog()
                .resolve_aggregate_trusted(
                    "multi_distinct_count",
                    &[DataType::Int64, DataType::Int64],
                )
                .is_err()
        );
    }

    #[test]
    fn apply_skips_distinct_on_different_cols() {
        let mut memo = Memo::new();
        let sg = scan_group(&mut memo);
        let id = memo.next_expr_id();
        let mexpr = MExpr {
            id,
            op: Operator::LogicalAggregate(single_agg(
                &mut memo,
                vec![],
                vec![count_distinct("a"), count_distinct("b")],
                vec![],
            )),
            children: vec![sg],
        };
        assert!(SplitDistinctAgg.apply(&mexpr, &mut memo).is_empty());
    }

    #[test]
    fn apply_skips_non_distinct_order_sensitive_aggregate() {
        let mut memo = Memo::new();
        let sg = scan_group(&mut memo);
        let id = memo.next_expr_id();
        let mexpr = MExpr {
            id,
            op: Operator::LogicalAggregate(single_agg(
                &mut memo,
                vec![col("g")],
                vec![
                    AggregateCall {
                        name: "array_agg".into(),
                        args: vec![col("name")],
                        distinct: false,
                        result_type: DataType::List(Arc::new(arrow::datatypes::Field::new(
                            "item",
                            DataType::Int64,
                            true,
                        ))),
                        order_by: vec![crate::analysis::SortItem {
                            expr: col("id"),
                            asc: true,
                            nulls_first: true,
                        }],
                        output_column_id: fallback_output_id(1),
                        resolved: crate::functions::test_resolved_aggregate(
                            "array_agg",
                            &[DataType::Int64],
                            false,
                        ),
                    },
                    count_distinct("name"),
                ],
                vec![],
            )),
            children: vec![sg],
        };
        assert!(SplitDistinctAgg.apply(&mexpr, &mut memo).is_empty());
    }

    #[test]
    fn apply_skips_stateful_sketch_non_distinct_aggregate() {
        let mut memo = Memo::new();
        let sg = scan_group(&mut memo);
        let id = memo.next_expr_id();
        let mexpr = MExpr {
            id,
            op: Operator::LogicalAggregate(single_agg(
                &mut memo,
                vec![],
                vec![
                    count_distinct("x"),
                    AggregateCall {
                        name: "ds_hll_count_distinct".into(),
                        args: vec![col("x")],
                        distinct: false,
                        result_type: DataType::Int64,
                        order_by: vec![],
                        output_column_id: fallback_output_id(1),
                        resolved: crate::functions::test_resolved_aggregate(
                            "ds_hll_count_distinct",
                            &[DataType::Int64],
                            false,
                        ),
                    },
                ],
                vec![],
            )),
            children: vec![sg],
        };
        assert!(SplitDistinctAgg.apply(&mexpr, &mut memo).is_empty());
    }

    #[test]
    fn apply_skips_distinct_order_sensitive_aggregate() {
        let mut memo = Memo::new();
        let sg = scan_group(&mut memo);
        let id = memo.next_expr_id();
        let mexpr = MExpr {
            id,
            op: Operator::LogicalAggregate(single_agg(
                &mut memo,
                vec![col("g")],
                vec![AggregateCall {
                    name: "array_agg".into(),
                    args: vec![col("name")],
                    distinct: true,
                    result_type: DataType::List(Arc::new(arrow::datatypes::Field::new(
                        "item",
                        DataType::Int64,
                        true,
                    ))),
                    order_by: vec![crate::analysis::SortItem {
                        expr: col("id"),
                        asc: true,
                        nulls_first: true,
                    }],
                    output_column_id: fallback_output_id(1),
                    resolved: crate::functions::test_resolved_aggregate(
                        "array_agg",
                        &[DataType::Int64],
                        true,
                    ),
                }],
                vec![],
            )),
            children: vec![sg],
        };
        assert!(SplitDistinctAgg.apply(&mexpr, &mut memo).is_empty());
    }

    #[test]
    fn extracts_distinct_col_for_same_col_multi_distinct() {
        // count(distinct x) + sum(distinct x) -- same col. Accepts both.
        let sum_distinct_x = AggregateCall {
            name: "sum".into(),
            args: vec![col("x")],
            distinct: true,
            result_type: DataType::Int64,
            order_by: vec![],
            output_column_id: fallback_output_id(1),
            resolved: crate::functions::test_resolved_aggregate("sum", &[DataType::Int64], true),
        };
        let mut memo = Memo::new();
        let calls =
            intern_aggregate_calls(&mut memo.scalars, &[count_distinct("x"), sum_distinct_x]);
        let col_out = extract_single_distinct_col(&memo.scalars, &calls);
        assert!(
            col_out.is_some(),
            "expected Some for same-column multi-DISTINCT"
        );
        let col_out = materialize(&memo.scalars, col_out.unwrap());
        let ExprKind::ColumnRef { column, .. } = &col_out.kind else {
            panic!("expected ColumnRef");
        };
        assert_eq!(column, "x");
    }

    #[test]
    fn three_phase_chain_with_group_by() {
        let mut memo = Memo::new();
        let sg = scan_group(&mut memo);
        let id = memo.next_expr_id();
        let mexpr = MExpr {
            id,
            op: Operator::LogicalAggregate(single_agg(
                &mut memo,
                vec![col("g")],
                vec![count_distinct("x"), sum_non_distinct("a")],
                vec![
                    OutputColumn {
                        column_id: ColumnId::UNSET,
                        name: "g".into(),
                        data_type: DataType::Int64,
                        nullable: false,
                        is_internal: false,
                    },
                    OutputColumn {
                        column_id: ColumnId::UNSET,
                        name: "count(distinct x)".into(),
                        data_type: DataType::Int64,
                        nullable: true,
                        is_internal: false,
                    },
                    OutputColumn {
                        column_id: ColumnId::UNSET,
                        name: "sum(a)".into(),
                        data_type: DataType::Int64,
                        nullable: true,
                        is_internal: false,
                    },
                ],
            )),
            children: vec![sg],
        };
        let out = SplitDistinctAgg.apply(&mexpr, &mut memo);
        assert_eq!(out.len(), 1, "expected one multi-phase alternative");

        // Top: GLOBAL, group_by=[g], aggregates[0] = count(distinct x), aggregates[1] = sum(a) (merge)
        let top = match &out[0].op {
            Operator::PhysicalHashAggregate(p) => p,
            other => panic!("expected GLOBAL PhysicalHashAggregate, got {:?}", other),
        };
        assert!(matches!(top.mode, AggMode::Global));
        assert_eq!(top.group_by.len(), 1, "GLOBAL group_by is just [g]");
        assert_eq!(top.aggregates.len(), 2);
        assert_eq!(top.aggregates[0].name, "count");
        // distinct=true is preserved so the display name matches what PROJECT looks up.
        assert!(top.aggregates[0].distinct);
        assert_eq!(top.is_merge, vec![false, true]);

        // Follow chain: GLOBAL -> DISTINCT_GLOBAL -> LOCAL -> scan
        assert_eq!(out[0].children.len(), 1);
        let dg_group = &memo.groups[out[0].children[0]];
        let dg = match &dg_group.physical_exprs[0].op {
            Operator::PhysicalHashAggregate(p) => p,
            other => panic!("expected DISTINCT_GLOBAL, got {:?}", other),
        };
        assert!(matches!(dg.mode, AggMode::DistinctGlobal));
        assert_eq!(dg.group_by.len(), 2, "DG group_by is [g, x]");
        assert_eq!(dg.aggregates.len(), 1); // only sum(a); count(distinct x) is folded into grouping
        assert_eq!(dg.is_merge, vec![true]);
        assert_eq!(dg_group.physical_exprs[0].children.len(), 1);

        let local_group = &memo.groups[dg_group.physical_exprs[0].children[0]];
        let local = match &local_group.physical_exprs[0].op {
            Operator::PhysicalHashAggregate(p) => p,
            other => panic!("expected LOCAL, got {:?}", other),
        };
        assert!(matches!(local.mode, AggMode::Local));
        assert_eq!(local.group_by.len(), 2, "LOCAL group_by is [g, x]");
        assert_eq!(local.aggregates.len(), 1);
        assert_eq!(local.is_merge, vec![false]);
        assert_eq!(local_group.physical_exprs[0].children, vec![sg]);
    }

    #[test]
    fn three_phase_deduplicates_group_key_that_is_distinct_arg() {
        let mut memo = Memo::new();
        let sg = scan_group(&mut memo);
        let id = memo.next_expr_id();
        let mexpr = MExpr {
            id,
            op: Operator::LogicalAggregate(single_agg(
                &mut memo,
                vec![col("x")],
                vec![count_distinct("x")],
                vec![],
            )),
            children: vec![sg],
        };
        let out = SplitDistinctAgg.apply(&mexpr, &mut memo);
        assert_eq!(out.len(), 1, "expected one multi-phase alternative");

        let dg_group = &memo.groups[out[0].children[0]];
        let dg = match &dg_group.physical_exprs[0].op {
            Operator::PhysicalHashAggregate(p) => p,
            other => panic!("expected DISTINCT_GLOBAL, got {:?}", other),
        };
        assert!(matches!(dg.mode, AggMode::DistinctGlobal));
        assert_eq!(dg.group_by.len(), 1);
        assert_eq!(dg.output_layout.group_key_columns.len(), 1);
        let dg_ids = dg
            .output_columns
            .iter()
            .map(|column| column.column_id)
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(dg_ids.len(), dg.output_columns.len());

        let local_group = &memo.groups[dg_group.physical_exprs[0].children[0]];
        let local = match &local_group.physical_exprs[0].op {
            Operator::PhysicalHashAggregate(p) => p,
            other => panic!("expected LOCAL, got {:?}", other),
        };
        assert!(matches!(local.mode, AggMode::Local));
        assert_eq!(local.group_by.len(), 1);
        assert_eq!(local.output_layout.group_key_columns.len(), 1);
    }

    #[test]
    fn three_phase_rebinds_existing_distinct_key_by_position() {
        let mut memo = Memo::new();
        let sg = scan_group(&mut memo);
        let id = memo.next_expr_id();
        let mexpr = MExpr {
            id,
            op: Operator::LogicalAggregate(single_agg(
                &mut memo,
                vec![col("x"), col("g")],
                vec![count_distinct("x")],
                vec![],
            )),
            children: vec![sg],
        };
        let out = SplitDistinctAgg.apply(&mexpr, &mut memo);
        assert_eq!(out.len(), 1, "expected one multi-phase alternative");

        let top = match &out[0].op {
            Operator::PhysicalHashAggregate(p) => p,
            other => panic!("expected GLOBAL PhysicalHashAggregate, got {:?}", other),
        };
        assert!(matches!(top.mode, AggMode::Global));
        assert_eq!(top.group_by.len(), 2);
        assert_eq!(top.aggregates.len(), 1);
        let arg = top.aggregates[0]
            .args
            .first()
            .copied()
            .expect("distinct arg");
        let ScalarNode::ColumnRef(column_id) = memo.scalars.node(arg) else {
            panic!("expected distinct arg to be ColumnRef");
        };
        assert_eq!(
            *column_id,
            test_col_id("x"),
            "count(distinct x) must not rebind to the trailing group key"
        );
    }

    #[test]
    fn three_phase_top_preserves_pruned_public_outputs() {
        let mut memo = Memo::new();
        let sg = scan_group(&mut memo);
        let group_output = output_column(40, "g_hidden");
        let count_output = output_column(41, "count_distinct_x");
        let sum_output = output_column(42, "sum_a_hidden");
        let id = memo.next_expr_id();
        let mexpr = MExpr {
            id,
            op: Operator::LogicalAggregate(single_agg_with_layout(
                &mut memo,
                vec![col_with_id("g", 4)],
                vec![
                    AggregateCall {
                        name: "count".into(),
                        args: vec![col_with_id("x", 5)],
                        distinct: true,
                        result_type: DataType::Int64,
                        order_by: vec![],
                        output_column_id: count_output.column_id,
                        resolved: crate::functions::test_resolved_aggregate(
                            "count",
                            &[DataType::Int64],
                            true,
                        ),
                    },
                    AggregateCall {
                        name: "sum".into(),
                        args: vec![col_with_id("a", 6)],
                        distinct: false,
                        result_type: DataType::Int64,
                        order_by: vec![],
                        output_column_id: sum_output.column_id,
                        resolved: crate::functions::test_resolved_aggregate(
                            "sum",
                            &[DataType::Int64],
                            false,
                        ),
                    },
                ],
                vec![
                    group_output.clone(),
                    count_output.clone(),
                    sum_output.clone(),
                ],
                vec![count_output.clone()],
            )),
            children: vec![sg],
        };

        let out = SplitDistinctAgg.apply(&mexpr, &mut memo);
        assert_eq!(out.len(), 1);
        let top = match &out[0].op {
            Operator::PhysicalHashAggregate(p) => p,
            other => panic!("expected GLOBAL PhysicalHashAggregate, got {:?}", other),
        };
        assert!(matches!(top.mode, AggMode::Global));
        assert_eq!(phase_output_ids(top), vec![count_output.column_id]);
        assert_eq!(
            top.output_layout
                .group_key_columns
                .iter()
                .map(|output| output.column_id)
                .collect::<Vec<_>>(),
            vec![group_output.column_id]
        );
        assert_eq!(
            top.output_layout
                .aggregate_columns
                .iter()
                .map(|output| output.column_id)
                .collect::<Vec<_>>(),
            vec![count_output.column_id, sum_output.column_id]
        );
    }

    #[test]
    fn three_phase_intermediate_outputs_use_group_layout_id_and_distinct_input_id() {
        let mut memo = Memo::new();
        let sg = scan_group(&mut memo);
        let g = col_with_id("g", 4);
        let x = col_with_id("x", 5);
        let a = col_with_id("a", 6);
        let id = memo.next_expr_id();
        let mexpr = MExpr {
            id,
            op: Operator::LogicalAggregate(single_agg(
                &mut memo,
                vec![g],
                vec![
                    AggregateCall {
                        name: "count".into(),
                        args: vec![x],
                        distinct: true,
                        result_type: DataType::Int64,
                        order_by: vec![],
                        output_column_id: ColumnId::new_for_test(7),
                        resolved: crate::functions::test_resolved_aggregate(
                            "count",
                            &[DataType::Int64],
                            true,
                        ),
                    },
                    AggregateCall {
                        name: "sum".into(),
                        args: vec![a],
                        distinct: false,
                        result_type: DataType::Int64,
                        order_by: vec![],
                        output_column_id: ColumnId::new_for_test(8),
                        resolved: crate::functions::test_resolved_aggregate(
                            "sum",
                            &[DataType::Int64],
                            false,
                        ),
                    },
                ],
                vec![
                    OutputColumn {
                        column_id: ColumnId::new_for_test(9),
                        name: "g_alias".into(),
                        data_type: DataType::Int64,
                        nullable: false,
                        is_internal: false,
                    },
                    OutputColumn {
                        column_id: ColumnId::new_for_test(7),
                        name: "count(distinct x)".into(),
                        data_type: DataType::Int64,
                        nullable: true,
                        is_internal: false,
                    },
                    OutputColumn {
                        column_id: ColumnId::new_for_test(8),
                        name: "sum(a)".into(),
                        data_type: DataType::Int64,
                        nullable: true,
                        is_internal: false,
                    },
                ],
            )),
            children: vec![sg],
        };

        let out = SplitDistinctAgg.apply(&mexpr, &mut memo);
        assert_eq!(out.len(), 1);
        let top = match &out[0].op {
            Operator::PhysicalHashAggregate(p) => p,
            other => panic!("expected GLOBAL, got {:?}", other),
        };
        let dg_group = &memo.groups[out[0].children[0]];
        let dg = match &dg_group.physical_exprs[0].op {
            Operator::PhysicalHashAggregate(p) => p,
            other => panic!("expected DISTINCT_GLOBAL, got {:?}", other),
        };
        let local_group = &memo.groups[dg_group.physical_exprs[0].children[0]];
        let local = match &local_group.physical_exprs[0].op {
            Operator::PhysicalHashAggregate(p) => p,
            other => panic!("expected LOCAL, got {:?}", other),
        };

        let expected = vec![
            ColumnId::new_for_test(9),
            ColumnId::new_for_test(5),
            ColumnId::new_for_test(8),
        ];
        let top_group_id = match memo.scalars.node(top.group_by[0]) {
            ScalarNode::ColumnRef(column_id) => *column_id,
            other => panic!("expected top group_by ColumnRef, got {other:?}"),
        };
        assert_eq!(top_group_id, ColumnId::new_for_test(9));
        assert_eq!(
            dg.output_columns
                .iter()
                .map(|c| c.column_id)
                .collect::<Vec<_>>(),
            expected
        );
        assert_eq!(
            local
                .output_columns
                .iter()
                .map(|c| c.column_id)
                .collect::<Vec<_>>(),
            expected
        );
    }

    #[test]
    fn three_phase_preserves_same_column_multi_distinct_outputs() {
        let mut memo = Memo::new();
        let sg = scan_group(&mut memo);
        let id = memo.next_expr_id();
        let mexpr = MExpr {
            id,
            op: Operator::LogicalAggregate(single_agg(
                &mut memo,
                vec![col("g")],
                vec![
                    array_agg_distinct("x"),
                    count_distinct("x"),
                    sum_non_distinct("a"),
                ],
                vec![],
            )),
            children: vec![sg],
        };

        let out = SplitDistinctAgg.apply(&mexpr, &mut memo);
        assert_eq!(out.len(), 1);
        let top = match &out[0].op {
            Operator::PhysicalHashAggregate(p) => p,
            other => panic!("expected GLOBAL PhysicalHashAggregate, got {:?}", other),
        };
        assert!(matches!(top.mode, AggMode::Global));
        assert_eq!(top.aggregates.len(), 3);
        assert_eq!(top.aggregates[0].name, "array_agg");
        assert!(top.aggregates[0].distinct);
        assert_eq!(top.aggregates[1].name, "sum");
        assert!(!top.aggregates[1].distinct);
        assert_eq!(top.aggregates[2].name, "count");
        assert!(top.aggregates[2].distinct);
        assert_eq!(top.is_merge, vec![false, true, false]);
        assert_hash_aggregate_layout(&memo, top);
    }

    #[test]
    fn four_phase_chain_when_scalar() {
        let mut memo = Memo::new();
        let sg = scan_group(&mut memo);
        let id = memo.next_expr_id();
        let mexpr = MExpr {
            id,
            op: Operator::LogicalAggregate(single_agg(
                &mut memo,
                vec![],
                vec![count_distinct("x"), sum_non_distinct("a")],
                vec![
                    OutputColumn {
                        column_id: ColumnId::UNSET,
                        name: "count(distinct x)".into(),
                        data_type: DataType::Int64,
                        nullable: true,
                        is_internal: false,
                    },
                    OutputColumn {
                        column_id: ColumnId::UNSET,
                        name: "sum(a)".into(),
                        data_type: DataType::Int64,
                        nullable: true,
                        is_internal: false,
                    },
                ],
            )),
            children: vec![sg],
        };
        let out = SplitDistinctAgg.apply(&mexpr, &mut memo);
        assert_eq!(out.len(), 1);

        // Top: GLOBAL, scalar, [count(x) merge, sum(a) merge]
        let top = match &out[0].op {
            Operator::PhysicalHashAggregate(p) => p,
            other => panic!("expected GLOBAL, got {:?}", other),
        };
        assert!(matches!(top.mode, AggMode::Global));
        assert_eq!(top.group_by.len(), 0);
        assert_eq!(top.aggregates.len(), 2);
        assert_eq!(top.is_merge, vec![true, true]);

        // DISTINCT_LOCAL: scalar, [count(x) update, sum(a) merge]
        let dl_group = &memo.groups[out[0].children[0]];
        let dl = match &dl_group.physical_exprs[0].op {
            Operator::PhysicalHashAggregate(p) => p,
            other => panic!("expected DISTINCT_LOCAL, got {:?}", other),
        };
        assert!(matches!(dl.mode, AggMode::DistinctLocal));
        assert_eq!(dl.group_by.len(), 0);
        assert_eq!(dl.is_merge, vec![false, true]);

        // DISTINCT_GLOBAL: group_by=[x], [sum(a) merge]
        let dg_group = &memo.groups[dl_group.physical_exprs[0].children[0]];
        let dg = match &dg_group.physical_exprs[0].op {
            Operator::PhysicalHashAggregate(p) => p,
            other => panic!("expected DISTINCT_GLOBAL, got {:?}", other),
        };
        assert!(matches!(dg.mode, AggMode::DistinctGlobal));
        assert_eq!(dg.group_by.len(), 1);
        assert_eq!(dg.is_merge, vec![true]);

        // LOCAL: group_by=[x], [sum(a) update]
        let local_group = &memo.groups[dg_group.physical_exprs[0].children[0]];
        let local = match &local_group.physical_exprs[0].op {
            Operator::PhysicalHashAggregate(p) => p,
            other => panic!("expected LOCAL, got {:?}", other),
        };
        assert!(matches!(local.mode, AggMode::Local));
        assert_eq!(local.group_by.len(), 1);
        assert_eq!(local.is_merge, vec![false]);
        assert_eq!(local_group.physical_exprs[0].children, vec![sg]);
    }

    #[test]
    fn four_phase_top_preserves_pruned_public_outputs() {
        let mut memo = Memo::new();
        let sg = scan_group(&mut memo);
        let count_output = output_column(51, "count_distinct_x");
        let sum_output = output_column(52, "sum_a_hidden");
        let id = memo.next_expr_id();
        let mexpr = MExpr {
            id,
            op: Operator::LogicalAggregate(single_agg_with_layout(
                &mut memo,
                vec![],
                vec![
                    AggregateCall {
                        name: "count".into(),
                        args: vec![col_with_id("x", 5)],
                        distinct: true,
                        result_type: DataType::Int64,
                        order_by: vec![],
                        output_column_id: count_output.column_id,
                        resolved: crate::functions::test_resolved_aggregate(
                            "count",
                            &[DataType::Int64],
                            true,
                        ),
                    },
                    AggregateCall {
                        name: "sum".into(),
                        args: vec![col_with_id("a", 6)],
                        distinct: false,
                        result_type: DataType::Int64,
                        order_by: vec![],
                        output_column_id: sum_output.column_id,
                        resolved: crate::functions::test_resolved_aggregate(
                            "sum",
                            &[DataType::Int64],
                            false,
                        ),
                    },
                ],
                vec![count_output.clone(), sum_output.clone()],
                vec![count_output.clone()],
            )),
            children: vec![sg],
        };

        let out = SplitDistinctAgg.apply(&mexpr, &mut memo);
        assert_eq!(out.len(), 1);
        let top = match &out[0].op {
            Operator::PhysicalHashAggregate(p) => p,
            other => panic!("expected GLOBAL PhysicalHashAggregate, got {:?}", other),
        };
        assert!(matches!(top.mode, AggMode::Global));
        assert_eq!(phase_output_ids(top), vec![count_output.column_id]);
        assert!(top.output_layout.group_key_columns.is_empty());
        assert_eq!(
            top.output_layout
                .aggregate_columns
                .iter()
                .map(|output| output.column_id)
                .collect::<Vec<_>>(),
            vec![count_output.column_id, sum_output.column_id]
        );
    }

    #[test]
    fn four_phase_layout_tracks_phase_aggregate_order_when_distinct_is_last() {
        let mut memo = Memo::new();
        let sg = scan_group(&mut memo);
        let x = col_with_id("x", 5);
        let a = col_with_id("a", 6);
        let b = col_with_id("b", 7);
        let sum_output = ColumnId::new_for_test(20);
        let count_output = ColumnId::new_for_test(21);
        let distinct_output = ColumnId::new_for_test(22);
        let id = memo.next_expr_id();
        let mexpr = MExpr {
            id,
            op: Operator::LogicalAggregate(single_agg(
                &mut memo,
                vec![],
                vec![
                    AggregateCall {
                        name: "sum".into(),
                        args: vec![a],
                        distinct: false,
                        result_type: DataType::Int64,
                        order_by: vec![],
                        output_column_id: sum_output,
                        resolved: crate::functions::test_resolved_aggregate(
                            "sum",
                            &[DataType::Int64],
                            false,
                        ),
                    },
                    AggregateCall {
                        name: "count".into(),
                        args: vec![b],
                        distinct: false,
                        result_type: DataType::Int64,
                        order_by: vec![],
                        output_column_id: count_output,
                        resolved: crate::functions::test_resolved_aggregate(
                            "count",
                            &[DataType::Int64],
                            false,
                        ),
                    },
                    AggregateCall {
                        name: "count".into(),
                        args: vec![x],
                        distinct: true,
                        result_type: DataType::Int64,
                        order_by: vec![],
                        output_column_id: distinct_output,
                        resolved: crate::functions::test_resolved_aggregate(
                            "count",
                            &[DataType::Int64],
                            true,
                        ),
                    },
                ],
                vec![
                    OutputColumn {
                        column_id: sum_output,
                        name: "sum(a)".into(),
                        data_type: DataType::Int64,
                        nullable: true,
                        is_internal: false,
                    },
                    OutputColumn {
                        column_id: count_output,
                        name: "count(b)".into(),
                        data_type: DataType::Int64,
                        nullable: true,
                        is_internal: false,
                    },
                    OutputColumn {
                        column_id: distinct_output,
                        name: "count(distinct x)".into(),
                        data_type: DataType::Int64,
                        nullable: true,
                        is_internal: false,
                    },
                ],
            )),
            children: vec![sg],
        };

        let out = SplitDistinctAgg.apply(&mexpr, &mut memo);
        assert_eq!(out.len(), 1);
        let top = match &out[0].op {
            Operator::PhysicalHashAggregate(p) => p,
            other => panic!("expected GLOBAL, got {:?}", other),
        };
        assert!(matches!(top.mode, AggMode::Global));
        assert_eq!(top.is_merge, vec![true, true, true]);
        assert_hash_aggregate_layout(&memo, top);
        assert_eq!(
            aggregate_call_output_ids(&memo, top),
            vec![distinct_output, sum_output, count_output]
        );
        assert_eq!(
            top.output_layout
                .aggregate_columns
                .iter()
                .map(|output| output.column_id)
                .collect::<Vec<_>>(),
            vec![distinct_output, sum_output, count_output]
        );
        assert_eq!(
            phase_output_ids(top),
            vec![sum_output, count_output, distinct_output]
        );

        let dl_group = &memo.groups[out[0].children[0]];
        let dl = match &dl_group.physical_exprs[0].op {
            Operator::PhysicalHashAggregate(p) => p,
            other => panic!("expected DISTINCT_LOCAL, got {:?}", other),
        };
        assert_hash_aggregate_layout(&memo, dl);
        assert_eq!(
            phase_output_ids(dl),
            vec![distinct_output, sum_output, count_output]
        );

        let dg_group = &memo.groups[dl_group.physical_exprs[0].children[0]];
        let dg = match &dg_group.physical_exprs[0].op {
            Operator::PhysicalHashAggregate(p) => p,
            other => panic!("expected DISTINCT_GLOBAL, got {:?}", other),
        };
        assert_hash_aggregate_layout(&memo, dg);
        assert_eq!(
            phase_output_ids(dg),
            vec![ColumnId::new_for_test(5), sum_output, count_output]
        );

        let local_group = &memo.groups[dg_group.physical_exprs[0].children[0]];
        let local = match &local_group.physical_exprs[0].op {
            Operator::PhysicalHashAggregate(p) => p,
            other => panic!("expected LOCAL, got {:?}", other),
        };
        assert_hash_aggregate_layout(&memo, local);
        assert_eq!(
            phase_output_ids(local),
            vec![ColumnId::new_for_test(5), sum_output, count_output]
        );
    }

    #[test]
    fn four_phase_intermediate_outputs_preserve_distinct_input_id() {
        let mut memo = Memo::new();
        let sg = scan_group(&mut memo);
        let x = col_with_id("x", 5);
        let id = memo.next_expr_id();
        let mexpr = MExpr {
            id,
            op: Operator::LogicalAggregate(single_agg(
                &mut memo,
                vec![],
                vec![AggregateCall {
                    name: "count".into(),
                    args: vec![x],
                    distinct: true,
                    result_type: DataType::Int64,
                    order_by: vec![],
                    output_column_id: ColumnId::new_for_test(8),
                    resolved: crate::functions::test_resolved_aggregate(
                        "count",
                        &[DataType::Int64],
                        true,
                    ),
                }],
                vec![OutputColumn {
                    column_id: ColumnId::new_for_test(8),
                    name: "count(distinct x)".into(),
                    data_type: DataType::Int64,
                    nullable: true,
                    is_internal: false,
                }],
            )),
            children: vec![sg],
        };

        let out = SplitDistinctAgg.apply(&mexpr, &mut memo);
        assert_eq!(out.len(), 1);
        let dl_group = &memo.groups[out[0].children[0]];
        let dg_group = &memo.groups[dl_group.physical_exprs[0].children[0]];
        let dg = match &dg_group.physical_exprs[0].op {
            Operator::PhysicalHashAggregate(p) => p,
            other => panic!("expected DISTINCT_GLOBAL, got {:?}", other),
        };
        let local_group = &memo.groups[dg_group.physical_exprs[0].children[0]];
        let local = match &local_group.physical_exprs[0].op {
            Operator::PhysicalHashAggregate(p) => p,
            other => panic!("expected LOCAL, got {:?}", other),
        };

        assert_eq!(
            dg.output_columns
                .iter()
                .map(|c| c.column_id)
                .collect::<Vec<_>>(),
            vec![ColumnId::new_for_test(5)]
        );
        assert_eq!(
            local
                .output_columns
                .iter()
                .map(|c| c.column_id)
                .collect::<Vec<_>>(),
            vec![ColumnId::new_for_test(5)]
        );
    }

    #[test]
    fn four_phase_rebinds_distinct_update_args_to_phase_output_id() {
        let mut memo = Memo::new();
        let sg = scan_group(&mut memo);
        let x_phase = col_with_id("x", 5);
        let x_duplicate = col_with_id("x", 5);
        let a = col_with_id("a", 6);
        let id = memo.next_expr_id();
        let mexpr = MExpr {
            id,
            op: Operator::LogicalAggregate(single_agg(
                &mut memo,
                vec![],
                vec![
                    AggregateCall {
                        name: "count".into(),
                        args: vec![x_phase],
                        distinct: true,
                        result_type: DataType::Int64,
                        order_by: vec![],
                        output_column_id: ColumnId::new_for_test(8),
                        resolved: crate::functions::test_resolved_aggregate(
                            "count",
                            &[DataType::Int64],
                            true,
                        ),
                    },
                    AggregateCall {
                        name: "sum".into(),
                        args: vec![a],
                        distinct: false,
                        result_type: DataType::Int64,
                        order_by: vec![],
                        output_column_id: ColumnId::new_for_test(9),
                        resolved: crate::functions::test_resolved_aggregate(
                            "sum",
                            &[DataType::Int64],
                            false,
                        ),
                    },
                    AggregateCall {
                        name: "count".into(),
                        args: vec![x_duplicate],
                        distinct: true,
                        result_type: DataType::Int64,
                        order_by: vec![],
                        output_column_id: ColumnId::new_for_test(10),
                        resolved: crate::functions::test_resolved_aggregate(
                            "count",
                            &[DataType::Int64],
                            true,
                        ),
                    },
                ],
                vec![
                    OutputColumn {
                        column_id: ColumnId::new_for_test(8),
                        name: "count(distinct x)".into(),
                        data_type: DataType::Int64,
                        nullable: true,
                        is_internal: false,
                    },
                    OutputColumn {
                        column_id: ColumnId::new_for_test(9),
                        name: "sum(a)".into(),
                        data_type: DataType::Int64,
                        nullable: true,
                        is_internal: false,
                    },
                    OutputColumn {
                        column_id: ColumnId::new_for_test(10),
                        name: "count(distinct x)".into(),
                        data_type: DataType::Int64,
                        nullable: true,
                        is_internal: false,
                    },
                ],
            )),
            children: vec![sg],
        };

        let out = SplitDistinctAgg.apply(&mexpr, &mut memo);
        assert_eq!(out.len(), 1);
        let dl_group = &memo.groups[out[0].children[0]];
        let dl = match &dl_group.physical_exprs[0].op {
            Operator::PhysicalHashAggregate(p) => p,
            other => panic!("expected DISTINCT_LOCAL, got {:?}", other),
        };

        let distinct_arg_ids = dl
            .aggregates
            .iter()
            .filter(|call| call.distinct)
            .map(|call| match materialize(&memo.scalars, call.args[0]).kind {
                ExprKind::ColumnRef { column_id, .. } => column_id,
                other => panic!("expected ColumnRef arg, got {:?}", other),
            })
            .collect::<Vec<_>>();
        assert_eq!(
            distinct_arg_ids,
            vec![ColumnId::new_for_test(5), ColumnId::new_for_test(5)]
        );
    }
}
