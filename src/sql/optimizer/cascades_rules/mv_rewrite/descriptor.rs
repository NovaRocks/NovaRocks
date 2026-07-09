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

//! SPJG (select-project-join-group-by, single-table subset) decomposition.
//!
//! Both sides of MV rewrite matching are normalized into this shape:
//! the MV defining plan (built at candidate-prep time from the planner
//! LogicalPlanNode) and the query subtree (rebuilt from memo MExprs by the rule).

use std::collections::{HashMap, HashSet};

use crate::sql::analysis::BinOp;
use crate::sql::catalog::TableDef;
use crate::sql::column_id::ColumnId;
use crate::sql::common::OutputColumn;
use crate::sql::common::expr::JoinKind;
use crate::sql::optimizer::memo::{MExpr, Memo};
use crate::sql::optimizer::operator::{
    AggStage, LogicalAggregateOp, Operator, ProjectOp, ScalarAggregateSpec, ScanOp,
};
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::scalar::{ScalarArena, ScalarId, ScalarNode, SortKey};
use crate::sql::optimizer::scalar_expr;

/// What the alternative must reproduce at the matched group's top.
///
/// Carries the original operator shape so the rule can reuse the matched
/// group's output `ColumnId`s when constructing the rewritten alternative
/// (memo-group equivalence requires equivalent expressions to share their
/// output columns).
#[derive(Clone, Debug)]
pub(crate) enum MatchedShape {
    /// Top is the scan itself or `Filter(Scan)`: outputs are scan columns.
    Spj,
    /// Top is `LogicalAggregate`: the original op is cloned so the rule can
    /// reuse its `output_columns` ids.
    Spjg { original_agg: LogicalAggregateOp },
}

/// One visible output of the SPJG subtree, in output order.
#[derive(Clone, Debug)]
pub(crate) struct SpjgOutput {
    pub name: String,
    /// The ColumnId this output is addressed by at the subtree top.
    pub column_id: ColumnId,
    pub expr: SpjgOutputExpr,
}

#[derive(Clone, Debug)]
pub(crate) enum SpjgOutputExpr {
    /// Expression over base-table columns (projection item or group key).
    Dimension(ScalarId),
    /// Aggregate call over base-table columns.
    Aggregate(ScalarAggregateSpec),
}

#[derive(Clone, Debug)]
pub(crate) struct SpjgAggregate {
    /// Group keys, composed down to base-table column expressions.
    /// Rollup matching only needs the group-by set; aggregate calls are matched
    /// through the descriptor's `outputs` (the `Aggregate` variant), so they are
    /// not duplicated here.
    pub group_by: Vec<ScalarId>,
}

/// One additional (non-driving) base input of an inner-join SPJG shape.
/// The driving (left-deep first) scan stays in the descriptor's own
/// `table`/`scan_columns`; every other inner-join input lands here.
#[derive(Clone, Debug)]
pub(crate) struct JoinInput {
    pub table: TableDef,
    /// Scan output columns of this input: ColumnId -> base column binding.
    pub scan_columns: Vec<OutputColumn>,
}

/// Equi-join key `left_column = right_column`, addressed by the ColumnIds
/// visible at the two joined inputs. Non-equi join conjuncts are folded into
/// the descriptor's flat `predicates` list, not here.
#[derive(Clone, Debug)]
pub(crate) struct EquiEdge {
    pub left: ColumnId,
    pub right: ColumnId,
}

/// Inner-join shape attached to an `SpjgDescriptor`. `inputs` are every base
/// table joined onto the driving scan (left-deep order); `equi_edges` are the
/// equi-join keys. `None` on the descriptor means single-table (the original,
/// unchanged shape).
#[derive(Clone, Debug)]
pub(crate) struct JoinShape {
    pub inputs: Vec<JoinInput>,
    pub equi_edges: Vec<EquiEdge>,
}

#[derive(Clone, Debug)]
pub(crate) struct SpjgDescriptor {
    pub table: TableDef,
    /// Scan output columns: ColumnId -> base column binding.
    pub scan_columns: Vec<OutputColumn>,
    /// All conjuncts below the aggregate (scan predicates + filter, CNF-split).
    pub predicates: Vec<ScalarId>,
    pub aggregate: Option<SpjgAggregate>,
    /// Visible outputs in order (the subtree's output schema).
    pub outputs: Vec<SpjgOutput>,
    /// Inner-join shape, or `None` for a single-table SPJG. Populated by the
    /// descriptor builders when a left-deep inner-equi-join sits below the
    /// filter chain. The rewrite rule does not yet act on multi-table
    /// descriptors (see `rule::try_rewrite` guard) - extraction only.
    pub joins: Option<JoinShape>,
}

/// `catalog.namespace.table` identity of an Iceberg data-file table, or
/// `None` if `table` is not backed by `ScanSource::IcebergDataFiles`. Shared
/// by `base_name_of`'s column-key qualification and by
/// `has_unsupported_multitable_identity`'s self-join/non-Iceberg detection
/// (in turn consumed by `rule::join_graph_matches` and
/// `mv_rewrite_prep::supports_current_mv_rewrite_shape`) -- all key
/// physical-table identity the same way `rule::same_iceberg_table`'s
/// pairwise single-table check already does.
fn iceberg_fqn(table: &TableDef) -> Option<String> {
    use crate::sql::catalog::ScanSource;
    match &table.source {
        // Separate the three identity components with `\u{1}` (a control char
        // no SQL identifier can contain) rather than `.`, so a multi-level
        // (dot-joined) namespace can never make two distinct tables collapse to
        // the same key — e.g. `{ns:"a.b", tbl:"t"}` vs `{ns:"a", tbl:"b.t"}`.
        // This matches the field-wise identity `rule::same_iceberg_table` uses
        // and the `\u{1}` care `qualified_key` already takes for columns. The
        // key is internal-only (matching/self-join detection), never serialized.
        ScanSource::IcebergDataFiles { table: info, .. } => Some(format!(
            "{}\u{1}{}\u{1}{}",
            info.catalog, info.namespace, info.table
        )),
        _ => None,
    }
}

/// Qualifier prefix for `base_name_of`'s keys: the table's Iceberg FQN when
/// available, else the bare table name. The fallback keeps `base_name_of`
/// total -- it must still work for the non-Iceberg `TableDef`s many unit
/// tests in this module use. Multi-table rewrite MATCHING is Iceberg-only in
/// practice (`has_unsupported_multitable_identity` / `rule::same_iceberg_table`
/// reject non-Iceberg tables before any qualified key is ever compared
/// across descriptor sides), so the fallback never needs to be
/// cross-side-comparable; it only needs `base_name_of` to keep working.
fn table_qualifier(table: &TableDef) -> String {
    iceberg_fqn(table).unwrap_or_else(|| table.name.clone())
}

/// Join a table qualifier and a bare column name into `base_name_of`'s key
/// format. `\u{1}` (a control character no SQL identifier can contain)
/// separates the two so two different tables' same-named columns can never
/// collide, whatever the qualifier or column name are.
fn qualified_key(table_qualifier: &str, column_name: &str) -> String {
    format!("{table_qualifier}\u{1}{column_name}")
}

impl SpjgDescriptor {
    /// Map from scan ColumnId to an FQN-qualified base-column key, for
    /// cross-side matching: the two sides (query, MV) allocate independent
    /// ColumnIds for the same physical table(s), but a shared qualified
    /// string lets `column_mapping::normalize` / `predicate_split::check_containment`
    /// compare them structurally without changing either function's own
    /// structure -- they already key purely off whatever string this map
    /// produces (E1-bc design spec §2.1). Multi-table descriptors qualify
    /// every join input's columns too, so `t1.id` and `t2.id` never
    /// collide. Single-table descriptors are ALSO qualified (not left
    /// bare): both sides scan the SAME physical table, so both compute the
    /// SAME qualifier, and equality-based matching is unaffected (design
    /// spec §2.1 "单表零回退").
    pub(crate) fn base_name_of(&self) -> HashMap<ColumnId, String> {
        let driving_qualifier = table_qualifier(&self.table);
        let mut map: HashMap<ColumnId, String> = self
            .scan_columns
            .iter()
            .map(|c| (c.column_id, qualified_key(&driving_qualifier, &c.name)))
            .collect();
        if let Some(joins) = &self.joins {
            for input in &joins.inputs {
                let input_qualifier = table_qualifier(&input.table);
                for c in &input.scan_columns {
                    map.insert(c.column_id, qualified_key(&input_qualifier, &c.name));
                }
            }
        }
        map
    }

    /// Every base-table FQN this descriptor's join shape touches: the
    /// driving table first, then each `joins.inputs` entry in order. `None`
    /// if any table is not an Iceberg data-file scan. Only meaningful when
    /// `self.joins.is_some()` -- callers must check that first, or use
    /// `has_unsupported_multitable_identity`, which does.
    pub(crate) fn join_table_fqns(&self) -> Option<Vec<String>> {
        let joins = self.joins.as_ref()?;
        let mut out = Vec::with_capacity(1 + joins.inputs.len());
        out.push(iceberg_fqn(&self.table)?);
        for input in &joins.inputs {
            out.push(iceberg_fqn(&input.table)?);
        }
        Some(out)
    }

    /// True (fail-open) if this descriptor carries a join shape whose
    /// tables cannot all be verified as distinct Iceberg identities: either
    /// some table is not an Iceberg data-file scan, or the same physical
    /// table appears twice (self-join, unsupported in v1 -- the
    /// FQN-qualified column scheme in `base_name_of` cannot disambiguate
    /// two occurrences of one table). Always `false` for a single-table
    /// descriptor (never blocks the unchanged single-table path).
    pub(crate) fn has_unsupported_multitable_identity(&self) -> bool {
        if self.joins.is_none() {
            return false;
        }
        let Some(fqns) = self.join_table_fqns() else {
            return true;
        };
        let unique: HashSet<&String> = fqns.iter().collect();
        unique.len() != fqns.len()
    }

    pub(crate) fn from_opt_expr(
        expr: &OptExpr,
        arena: &mut ScalarArena,
    ) -> Result<SpjgDescriptor, String> {
        // Accepted normal form, peeled top-down:
        //   [Project] -> [Aggregate] -> [Project] -> [Filter]* -> Scan
        // Anything else (Join/Sort/Limit/Window/Union/CTE/...) is rejected.
        let mut node = expr;

        // Optional top project (rebinding of aggregate/scan outputs).
        let top_project = match &node.op {
            Operator::LogicalProject(p) => {
                node = node
                    .children
                    .first()
                    .ok_or_else(|| "project without child in MV rewrite shape".to_string())?;
                Some(p)
            }
            _ => None,
        };

        let aggregate = match &node.op {
            Operator::LogicalAggregate(a) => {
                if a.stage != AggStage::Single || a.is_split || aggregate_has_order_by(a) {
                    return Err("unsupported aggregate shape for MV rewrite".to_string());
                }
                node = node
                    .children
                    .first()
                    .ok_or_else(|| "aggregate without child in MV rewrite shape".to_string())?;
                Some(a)
            }
            _ => None,
        };

        // Optional pre-aggregate project (planner may compute group-key /
        // agg-arg expressions in a project below the aggregate).
        let mid_project = match &node.op {
            Operator::LogicalProject(p) => {
                node = node
                    .children
                    .first()
                    .ok_or_else(|| "project without child in MV rewrite shape".to_string())?;
                Some(p)
            }
            _ => None,
        };

        let mut predicates: Vec<ScalarId> = Vec::new();
        while let Operator::LogicalFilter(f) = &node.op {
            scalar_expr::split_conjuncts(arena, f.predicate, &mut predicates);
            node = node
                .children
                .first()
                .ok_or_else(|| "filter without child in MV rewrite shape".to_string())?;
        }

        let (scan, joins) = peel_join_spine_opt(node, arena, &mut predicates)?;
        predicates.extend(scan.predicates.iter().copied());

        // Composition map: ColumnId -> defining expr over scan columns
        // (from the mid project). Identity for scan columns themselves.
        let mut defs: HashMap<ColumnId, ScalarId> = HashMap::new();
        if let Some(p) = mid_project {
            for item in &p.items {
                let composed = substitute_scalar(arena, item.expr, &defs);
                defs.insert(item.output_column_id, composed);
            }
        }

        let (agg, outputs) = match aggregate {
            Some(a) => {
                let group_by: Vec<ScalarId> = a
                    .group_by
                    .iter()
                    .map(|expr| substitute_scalar(arena, *expr, &defs))
                    .collect();
                let aggregates: Vec<ScalarAggregateSpec> = a
                    .aggregates
                    .iter()
                    .map(|c| substitute_aggregate(arena, c, &defs))
                    .collect::<Option<Vec<_>>>()
                    .ok_or_else(|| "unsupported aggregate order_by in MV rewrite".to_string())?;
                if a.output_layout.group_key_columns.len() != a.group_by.len()
                    || a.output_layout.aggregate_columns.len() != a.aggregates.len()
                {
                    return Err(format!(
                        "aggregate output layout mismatch: group_keys {} != group_by {}, aggregate_columns {} != aggs {}",
                        a.output_layout.group_key_columns.len(),
                        a.group_by.len(),
                        a.output_layout.aggregate_columns.len(),
                        a.aggregates.len()
                    ));
                }
                // Binding map at the aggregate's outputs.
                let mut agg_outputs: Vec<SpjgOutput> = Vec::new();
                for (idx, oc) in a.output_layout.group_key_columns.iter().enumerate() {
                    agg_outputs.push(SpjgOutput {
                        name: oc.name.clone(),
                        column_id: oc.column_id,
                        expr: SpjgOutputExpr::Dimension(group_by[idx]),
                    });
                }
                for (idx, oc) in a.output_layout.aggregate_columns.iter().enumerate() {
                    agg_outputs.push(SpjgOutput {
                        name: oc.name.clone(),
                        column_id: oc.column_id,
                        expr: SpjgOutputExpr::Aggregate(aggregates[idx].clone()),
                    });
                }
                let outputs = apply_top_project(arena, top_project, agg_outputs)?;
                (Some(SpjgAggregate { group_by }), outputs)
            }
            None => {
                let mut scan_outputs: Vec<SpjgOutput> = scan
                    .columns
                    .iter()
                    .map(|c| SpjgOutput {
                        name: c.name.clone(),
                        column_id: c.column_id,
                        expr: SpjgOutputExpr::Dimension(column_ref(arena, c)),
                    })
                    .collect();
                // Multi-table SPJ (no aggregate, no mid_project): the top
                // project can reference any joined table's column directly
                // (e.g. `SELECT o.id, c.name FROM o JOIN c ...`), so the base
                // map `apply_top_project` rebinds against must expose every
                // join input's columns too, not just the driving scan's.
                // Mirrors `SpjgDescriptor::base_name_of`'s driving +
                // join-inputs union. A no-op for single-table descriptors
                // (`joins` is `None`).
                if let Some(j) = &joins {
                    for input in &j.inputs {
                        for c in &input.scan_columns {
                            scan_outputs.push(SpjgOutput {
                                name: c.name.clone(),
                                column_id: c.column_id,
                                expr: SpjgOutputExpr::Dimension(column_ref(arena, c)),
                            });
                        }
                    }
                }
                // mid_project without aggregate is just "the" project.
                let scan_outputs = match mid_project {
                    Some(p) => p
                        .items
                        .iter()
                        .map(|item| SpjgOutput {
                            name: item.output_name.clone(),
                            column_id: item.output_column_id,
                            expr: SpjgOutputExpr::Dimension(substitute_scalar(
                                arena, item.expr, &defs,
                            )),
                        })
                        .collect(),
                    None => scan_outputs,
                };
                let outputs = apply_top_project(arena, top_project, scan_outputs)?;
                (None, outputs)
            }
        };

        Ok(SpjgDescriptor {
            table: scan.table.clone(),
            scan_columns: scan.columns.clone(),
            predicates,
            aggregate: agg,
            outputs,
            joins,
        })
    }

    /// Rebuild the SPJG view of the subtree rooted at `expr`, walking the
    /// memo by following the FIRST logical expression of each child group.
    ///
    /// The first logical expr of a group is always its original (unsplit)
    /// shape — `memo_copy` seeds each group with the planner node, and
    /// transformation rules only ever append alternatives. MvRewrite runs in
    /// the same explore round as `SplitAggregateRule`, so an aggregate group
    /// may already carry a split Local/Global alternative; the first expr is
    /// still the original Single aggregate, which is the only form this
    /// accepts.
    ///
    /// Mirrors [`SpjgDescriptor::from_opt_expr`] arm-by-arm but over
    /// `Operator` variants. Returns `None` for any non-SPJG operator in the
    /// chain (the same fail-closed contract). Unlike `from_opt_expr` there
    /// is no top-project arm: the rule only matches on Aggregate/Filter/Scan,
    /// so the matched node is the subtree top.
    pub(crate) fn from_memo(
        expr: &MExpr,
        memo: &mut Memo,
    ) -> Option<(SpjgDescriptor, MatchedShape)> {
        // Peel an optional top aggregate.
        let (aggregate, mut node) = match &expr.op {
            Operator::LogicalAggregate(a) => {
                // Only the original, unsplit Single aggregate is accepted.
                if a.stage != AggStage::Single || a.is_split || aggregate_has_order_by(a) {
                    return None;
                }
                let child = first_logical_expr(memo, *expr.children.first()?)?;
                (Some(a.clone()), child)
            }
            _ => (None, expr.clone()),
        };

        // Optional pre-aggregate (or sole) project below the current node.
        let mid_project = match &node.op {
            Operator::LogicalProject(p) => {
                let child = first_logical_expr(memo, *node.children.first()?)?;
                let saved = p.clone();
                node = child;
                Some(saved)
            }
            _ => None,
        };

        let mut predicates: Vec<ScalarId> = Vec::new();
        let (scan, joins) = peel_join_spine_memo(&node, memo, &mut predicates)?;
        // Reject scans already injected by a prior MV rewrite (MV-on-MV).
        if scan.mv_rewritten_from.is_some() {
            return None;
        }
        predicates.extend(scan.predicates.iter().copied());

        // Composition map from the mid project (ColumnId -> expr over scan cols).
        let mut defs: HashMap<ColumnId, ScalarId> = HashMap::new();
        if let Some(p) = &mid_project {
            for item in &p.items {
                let composed = substitute_scalar(&mut memo.scalars, item.expr, &defs);
                defs.insert(item.output_column_id, composed);
            }
        }

        let (agg, outputs, shape) = match aggregate {
            Some(a) => {
                let group_by: Vec<ScalarId> = a
                    .group_by
                    .iter()
                    .map(|expr| substitute_scalar(&mut memo.scalars, *expr, &defs))
                    .collect();
                let aggregates: Vec<ScalarAggregateSpec> = a
                    .aggregates
                    .iter()
                    .map(|c| substitute_aggregate(&mut memo.scalars, c, &defs))
                    .collect::<Option<Vec<_>>>()?;
                if a.output_layout.group_key_columns.len() != a.group_by.len()
                    || a.output_layout.aggregate_columns.len() != a.aggregates.len()
                {
                    return None;
                }
                let mut agg_outputs: Vec<SpjgOutput> = Vec::new();
                for (idx, oc) in a.output_layout.group_key_columns.iter().enumerate() {
                    agg_outputs.push(SpjgOutput {
                        name: oc.name.clone(),
                        column_id: oc.column_id,
                        expr: SpjgOutputExpr::Dimension(group_by[idx]),
                    });
                }
                for (idx, oc) in a.output_layout.aggregate_columns.iter().enumerate() {
                    agg_outputs.push(SpjgOutput {
                        name: oc.name.clone(),
                        column_id: oc.column_id,
                        expr: SpjgOutputExpr::Aggregate(aggregates[idx].clone()),
                    });
                }
                (
                    Some(SpjgAggregate { group_by }),
                    agg_outputs,
                    MatchedShape::Spjg {
                        original_agg: a.clone(),
                    },
                )
            }
            None => {
                let outputs: Vec<SpjgOutput> = match mid_project {
                    Some(p) => p
                        .items
                        .iter()
                        .map(|item| SpjgOutput {
                            name: item.output_name.clone(),
                            column_id: item.output_column_id,
                            expr: SpjgOutputExpr::Dimension(substitute_scalar(
                                &mut memo.scalars,
                                item.expr,
                                &defs,
                            )),
                        })
                        .collect(),
                    // No surviving Project: the scan's output IS the subtree
                    // output. Honor the scan's pruned `required_columns` when
                    // present so the descriptor reflects the columns the plan
                    // actually produces. Without this, columns the optimizer
                    // prunes away — the row-lineage metadata columns (`_file`,
                    // `_pos`, `_row_id`, `_last_updated_sequence_number`) that
                    // are mandatory on format-v3 row-lineage Iceberg base
                    // tables, plus any unreferenced data column — leak into the
                    // SPJ output set and never map onto the MV's narrower
                    // output list, so every SPJ rewrite is rejected. The
                    // physical scan is pruned to exactly `required_columns`, so
                    // matching on that set is sound. `None` keeps the full
                    // column list (the planner did not record a pruned set).
                    None => {
                        let pruned: Option<&Vec<String>> = scan.required_columns.as_ref();
                        scan.columns
                            .iter()
                            .filter(|c| pruned.is_none_or(|req| req.contains(&c.name)))
                            .map(|c| SpjgOutput {
                                name: c.name.clone(),
                                column_id: c.column_id,
                                expr: SpjgOutputExpr::Dimension(column_ref(&mut memo.scalars, c)),
                            })
                            .collect()
                    }
                };
                (None, outputs, MatchedShape::Spj)
            }
        };

        Some((
            SpjgDescriptor {
                table: scan.table.clone(),
                scan_columns: scan.columns.clone(),
                predicates,
                aggregate: agg,
                outputs,
                joins,
            },
            shape,
        ))
    }
}

/// Rebind outputs through an optional top project. MVP: top project items
/// must be bare ColumnRefs into the inputs (renames only); complex exprs
/// over aggregate results reject the shape.
fn apply_top_project(
    arena: &mut ScalarArena,
    project: Option<&ProjectOp>,
    inputs: Vec<SpjgOutput>,
) -> Result<Vec<SpjgOutput>, String> {
    let Some(p) = project else {
        return Ok(inputs);
    };
    let by_id: HashMap<ColumnId, &SpjgOutput> = inputs.iter().map(|o| (o.column_id, o)).collect();
    p.items
        .iter()
        .map(|item| match arena.node(item.expr) {
            ScalarNode::ColumnRef(column_id) => by_id
                .get(column_id)
                .map(|o| SpjgOutput {
                    name: item.output_name.clone(),
                    column_id: item.output_column_id,
                    expr: o.expr.clone(),
                })
                .ok_or_else(|| "top project references unknown column".to_string()),
            // A computed top-project item over a pure-dimension input can be
            // composed; over aggregate outputs it is rejected (MVP).
            _ => {
                let mut defs: HashMap<ColumnId, ScalarId> = HashMap::new();
                for o in &inputs {
                    match &o.expr {
                        SpjgOutputExpr::Dimension(e) => {
                            defs.insert(o.column_id, *e);
                        }
                        SpjgOutputExpr::Aggregate(_) => {}
                    }
                }
                let composed = substitute_scalar(arena, item.expr, &defs);
                if references_any(arena, composed, &inputs_agg_ids(&inputs)) {
                    Err("computed top-project over aggregate output (unsupported)".to_string())
                } else {
                    Ok(SpjgOutput {
                        name: item.output_name.clone(),
                        column_id: item.output_column_id,
                        expr: SpjgOutputExpr::Dimension(composed),
                    })
                }
            }
        })
        .collect()
}

fn first_logical_expr(memo: &Memo, gid: usize) -> Option<MExpr> {
    memo.groups[gid].logical_exprs.first().cloned()
}

fn inputs_agg_ids(inputs: &[SpjgOutput]) -> Vec<ColumnId> {
    inputs
        .iter()
        .filter(|o| matches!(o.expr, SpjgOutputExpr::Aggregate(_)))
        .map(|o| o.column_id)
        .collect()
}

fn references_any(arena: &ScalarArena, expr: ScalarId, ids: &[ColumnId]) -> bool {
    match arena.node(expr) {
        ScalarNode::ColumnRef(column_id) => ids.contains(column_id),
        node => scalar_children(node)
            .into_iter()
            .any(|child| references_any(arena, child, ids)),
    }
}

fn aggregate_has_order_by(agg: &LogicalAggregateOp) -> bool {
    agg.aggregates.iter().any(|call| !call.order_by.is_empty())
}

fn substitute_aggregate(
    arena: &mut ScalarArena,
    call: &ScalarAggregateSpec,
    defs: &HashMap<ColumnId, ScalarId>,
) -> Option<ScalarAggregateSpec> {
    if !call.order_by.is_empty() {
        return None;
    }
    Some(ScalarAggregateSpec {
        output_column_id: call.output_column_id,
        name: call.name.clone(),
        args: call
            .args
            .iter()
            .map(|arg| substitute_scalar(arena, *arg, defs))
            .collect(),
        distinct: call.distinct,
        order_by: vec![],
    })
}

fn substitute_sort_key(
    arena: &mut ScalarArena,
    key: &SortKey,
    defs: &HashMap<ColumnId, ScalarId>,
) -> SortKey {
    SortKey {
        expr: substitute_scalar(arena, key.expr, defs),
        asc: key.asc,
        nulls_first: key.nulls_first,
        display: key.display.clone(),
    }
}

fn peel_join_spine_memo(
    node: &MExpr,
    memo: &Memo,
    predicates: &mut Vec<ScalarId>,
) -> Option<(ScanOp, Option<JoinShape>)> {
    let mut n = node.clone();
    while let Operator::LogicalFilter(f) = &n.op {
        scalar_expr::split_conjuncts(&memo.scalars, f.predicate, predicates);
        n = first_logical_expr(memo, *n.children.first()?)?;
    }
    match &n.op {
        Operator::LogicalScan(scan) => Some((scan.clone(), None)),
        Operator::LogicalJoin(join) => {
            if join.join_type != JoinKind::Inner {
                return None;
            }
            let left = first_logical_expr(memo, *n.children.first()?)?;
            let right = first_logical_expr(memo, *n.children.get(1)?)?;

            let (driving, left_joins) = peel_join_spine_memo(&left, memo, predicates)?;
            let right_input = peel_filter_scan_memo(&right, memo, predicates)?;

            let left_columns = join_left_column_ids(&driving.columns, left_joins.as_ref());
            let right_columns = column_id_set(&right_input.scan_columns);
            let (edges, residuals) =
                split_join_condition(&memo.scalars, join.condition, &left_columns, &right_columns);
            if edges.is_empty() {
                return None;
            }
            predicates.extend(residuals);

            let mut shape = left_joins.unwrap_or(JoinShape {
                inputs: Vec::new(),
                equi_edges: Vec::new(),
            });
            shape.inputs.push(right_input);
            shape.equi_edges.extend(edges);
            Some((driving, Some(shape)))
        }
        _ => None,
    }
}

fn peel_filter_scan_memo(
    node: &MExpr,
    memo: &Memo,
    predicates: &mut Vec<ScalarId>,
) -> Option<JoinInput> {
    let mut n = node.clone();
    while let Operator::LogicalFilter(f) = &n.op {
        scalar_expr::split_conjuncts(&memo.scalars, f.predicate, predicates);
        n = first_logical_expr(memo, *n.children.first()?)?;
    }
    let Operator::LogicalScan(scan) = &n.op else {
        return None;
    };
    if scan.mv_rewritten_from.is_some() {
        return None;
    }
    predicates.extend(scan.predicates.iter().copied());
    Some(JoinInput {
        table: scan.table.clone(),
        scan_columns: scan.columns.clone(),
    })
}

/// Peel a left-deep inner-join spine (or a bare scan) at the bottom of the
/// SPJG shape. On entry `node` is the operator directly below the filter
/// chain. Returns the driving (left-deep first) scan and an optional
/// `JoinShape` describing every additional inner-join input. Predicates pushed
/// onto join inputs and residual (non-equi) join conjuncts are appended to
/// `predicates` (the descriptor keeps a single flat predicate list). Rejects
/// any non-`Inner` join kind, a right child that is not `Filter*->Scan`, and
/// any non-scan/non-join leaf.
fn peel_join_spine_opt<'a>(
    node: &'a OptExpr,
    arena: &ScalarArena,
    predicates: &mut Vec<ScalarId>,
) -> Result<(&'a ScanOp, Option<JoinShape>), String> {
    let mut n = node;
    while let Operator::LogicalFilter(f) = &n.op {
        scalar_expr::split_conjuncts(arena, f.predicate, predicates);
        n = n
            .children
            .first()
            .ok_or_else(|| "filter without child in MV rewrite join spine".to_string())?;
    }
    match &n.op {
        Operator::LogicalScan(scan) => Ok((scan, None)),
        Operator::LogicalJoin(join) => {
            if join.join_type != JoinKind::Inner {
                return Err(format!(
                    "unsupported join kind for MV rewrite: {:?}",
                    join.join_type
                ));
            }
            let left = n
                .children
                .first()
                .ok_or_else(|| "inner join without left child in MV rewrite shape".to_string())?;
            let right = n
                .children
                .get(1)
                .ok_or_else(|| "inner join without right child in MV rewrite shape".to_string())?;

            // Left-deep: recurse the left spine; the right child must reduce to
            // Filter*->Scan (one base input). A right-side join is rejected.
            let (driving, left_joins) = peel_join_spine_opt(left, arena, predicates)?;
            let right_input = peel_filter_scan_opt(right, arena, predicates)?;

            let left_columns = join_left_column_ids(&driving.columns, left_joins.as_ref());
            let right_columns = column_id_set(&right_input.scan_columns);
            let (edges, residuals) =
                split_join_condition(arena, join.condition, &left_columns, &right_columns);
            if edges.is_empty() {
                return Err("inner join without equi edge in MV rewrite shape".to_string());
            }
            predicates.extend(residuals);

            let mut shape = left_joins.unwrap_or(JoinShape {
                inputs: Vec::new(),
                equi_edges: Vec::new(),
            });
            shape.inputs.push(right_input);
            shape.equi_edges.extend(edges);
            Ok((driving, Some(shape)))
        }
        _ => Err(format!(
            "not an SPJG base shape: unexpected node {:?}",
            std::mem::discriminant(&n.op)
        )),
    }
}

/// Peel one join input's `Filter*->Scan`, pushing its predicates into
/// `predicates`. Rejects a nested join (right-deep/bushy) or any non-scan leaf.
fn peel_filter_scan_opt(
    node: &OptExpr,
    arena: &ScalarArena,
    predicates: &mut Vec<ScalarId>,
) -> Result<JoinInput, String> {
    let mut n = node;
    while let Operator::LogicalFilter(f) = &n.op {
        scalar_expr::split_conjuncts(arena, f.predicate, predicates);
        n = n
            .children
            .first()
            .ok_or_else(|| "filter without child in MV rewrite join input".to_string())?;
    }
    let Operator::LogicalScan(scan) = &n.op else {
        return Err(format!(
            "unsupported join input shape (only Filter*->Scan): {:?}",
            std::mem::discriminant(&n.op)
        ));
    };
    predicates.extend(scan.predicates.iter().copied());
    Ok(JoinInput {
        table: scan.table.clone(),
        scan_columns: scan.columns.clone(),
    })
}

/// Split a join `ON` condition into equi-join edges (`col = col`) and residual
/// conjuncts (everything else). A `None` condition (e.g. a cross join, which we
/// reject earlier anyway) yields two empty vectors.
fn split_join_condition(
    arena: &ScalarArena,
    condition: Option<ScalarId>,
    left_columns: &HashSet<ColumnId>,
    right_columns: &HashSet<ColumnId>,
) -> (Vec<EquiEdge>, Vec<ScalarId>) {
    let Some(cond) = condition else {
        return (Vec::new(), Vec::new());
    };
    let mut conjuncts: Vec<ScalarId> = Vec::new();
    scalar_expr::split_conjuncts(arena, cond, &mut conjuncts);

    let mut edges = Vec::new();
    let mut residuals = Vec::new();
    for c in conjuncts {
        let edge = match arena.node(c) {
            ScalarNode::BinaryOp {
                op: BinOp::Eq,
                left,
                right,
            } => match (arena.node(*left), arena.node(*right)) {
                (ScalarNode::ColumnRef(l), ScalarNode::ColumnRef(r)) => {
                    normalize_equi_edge(*l, *r, left_columns, right_columns)
                }
                _ => None,
            },
            _ => None,
        };
        match edge {
            Some(e) => edges.push(e),
            None => residuals.push(c),
        }
    }
    debug_assert!(
        edges
            .iter()
            .all(|edge| left_columns.contains(&edge.left) && right_columns.contains(&edge.right))
    );
    (edges, residuals)
}

fn normalize_equi_edge(
    left_expr: ColumnId,
    right_expr: ColumnId,
    left_columns: &HashSet<ColumnId>,
    right_columns: &HashSet<ColumnId>,
) -> Option<EquiEdge> {
    match (
        join_side(left_expr, left_columns, right_columns),
        join_side(right_expr, left_columns, right_columns),
    ) {
        (Some(JoinSide::Left), Some(JoinSide::Right)) => Some(EquiEdge {
            left: left_expr,
            right: right_expr,
        }),
        (Some(JoinSide::Right), Some(JoinSide::Left)) => Some(EquiEdge {
            left: right_expr,
            right: left_expr,
        }),
        _ => None,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum JoinSide {
    Left,
    Right,
}

fn join_side(
    column: ColumnId,
    left_columns: &HashSet<ColumnId>,
    right_columns: &HashSet<ColumnId>,
) -> Option<JoinSide> {
    match (
        left_columns.contains(&column),
        right_columns.contains(&column),
    ) {
        (true, false) => Some(JoinSide::Left),
        (false, true) => Some(JoinSide::Right),
        _ => None,
    }
}

fn join_left_column_ids(
    driving_columns: &[OutputColumn],
    joins: Option<&JoinShape>,
) -> HashSet<ColumnId> {
    let mut columns = column_id_set(driving_columns);
    if let Some(joins) = joins {
        for input in &joins.inputs {
            columns.extend(input.scan_columns.iter().map(|c| c.column_id));
        }
    }
    columns
}

fn column_id_set(columns: &[OutputColumn]) -> HashSet<ColumnId> {
    columns.iter().map(|c| c.column_id).collect()
}

pub(crate) fn substitute_scalar(
    arena: &mut ScalarArena,
    expr: ScalarId,
    defs: &HashMap<ColumnId, ScalarId>,
) -> ScalarId {
    if let ScalarNode::ColumnRef(column_id) = arena.node(expr)
        && let Some(replacement) = defs.get(column_id)
    {
        return *replacement;
    }
    let original = arena.node(expr).clone();
    let rewritten = match original {
        ScalarNode::ColumnRef(_) | ScalarNode::LambdaParamRef { .. } | ScalarNode::Literal(_) => {
            return expr;
        }
        ScalarNode::BinaryOp { op, left, right } => ScalarNode::BinaryOp {
            op,
            left: substitute_scalar(arena, left, defs),
            right: substitute_scalar(arena, right, defs),
        },
        ScalarNode::UnaryOp { op, child } => ScalarNode::UnaryOp {
            op,
            child: substitute_scalar(arena, child, defs),
        },
        ScalarNode::FunctionCall {
            name,
            args,
            distinct,
        } => ScalarNode::FunctionCall {
            name,
            args: args
                .into_iter()
                .map(|arg| substitute_scalar(arena, arg, defs))
                .collect(),
            distinct,
        },
        ScalarNode::AggregateCall {
            name,
            args,
            distinct,
            order_by,
        } => ScalarNode::AggregateCall {
            name,
            args: args
                .into_iter()
                .map(|arg| substitute_scalar(arena, arg, defs))
                .collect(),
            distinct,
            order_by: order_by
                .iter()
                .map(|key| substitute_sort_key(arena, key, defs))
                .collect(),
        },
        ScalarNode::Cast { child, target } => ScalarNode::Cast {
            child: substitute_scalar(arena, child, defs),
            target,
        },
        ScalarNode::IsNull { child, negated } => ScalarNode::IsNull {
            child: substitute_scalar(arena, child, defs),
            negated,
        },
        ScalarNode::InList {
            child,
            list,
            negated,
        } => ScalarNode::InList {
            child: substitute_scalar(arena, child, defs),
            list: list
                .into_iter()
                .map(|item| substitute_scalar(arena, item, defs))
                .collect(),
            negated,
        },
        ScalarNode::Between {
            child,
            low,
            high,
            negated,
        } => ScalarNode::Between {
            child: substitute_scalar(arena, child, defs),
            low: substitute_scalar(arena, low, defs),
            high: substitute_scalar(arena, high, defs),
            negated,
        },
        ScalarNode::Like {
            child,
            pattern,
            negated,
        } => ScalarNode::Like {
            child: substitute_scalar(arena, child, defs),
            pattern: substitute_scalar(arena, pattern, defs),
            negated,
        },
        ScalarNode::Case {
            operand,
            when_then,
            else_expr,
        } => ScalarNode::Case {
            operand: operand.map(|item| substitute_scalar(arena, item, defs)),
            when_then: when_then
                .into_iter()
                .map(|(when, then)| {
                    (
                        substitute_scalar(arena, when, defs),
                        substitute_scalar(arena, then, defs),
                    )
                })
                .collect(),
            else_expr: else_expr.map(|item| substitute_scalar(arena, item, defs)),
        },
        ScalarNode::IsTruthValue {
            child,
            value,
            negated,
        } => ScalarNode::IsTruthValue {
            child: substitute_scalar(arena, child, defs),
            value,
            negated,
        },
        ScalarNode::Nested(inner) => ScalarNode::Nested(substitute_scalar(arena, inner, defs)),
        ScalarNode::WindowCall {
            name,
            args,
            distinct,
            partition_by,
            order_by,
            window_frame,
            ignore_nulls,
        } => ScalarNode::WindowCall {
            name,
            args: args
                .into_iter()
                .map(|arg| substitute_scalar(arena, arg, defs))
                .collect(),
            distinct,
            partition_by: partition_by
                .into_iter()
                .map(|item| substitute_scalar(arena, item, defs))
                .collect(),
            order_by: order_by
                .iter()
                .map(|key| substitute_sort_key(arena, key, defs))
                .collect(),
            window_frame,
            ignore_nulls,
        },
        ScalarNode::LambdaFunction { params, body } => ScalarNode::LambdaFunction {
            params,
            body: substitute_scalar(arena, body, defs),
        },
        ScalarNode::Lambda { params, body } => ScalarNode::Lambda {
            params,
            body: substitute_scalar(arena, body, defs),
        },
    };
    arena.intern(
        rewritten,
        arena.data_type(expr).clone(),
        arena.nullable(expr),
    )
}

fn scalar_children(node: &ScalarNode) -> Vec<ScalarId> {
    match node {
        ScalarNode::ColumnRef(_) | ScalarNode::LambdaParamRef { .. } | ScalarNode::Literal(_) => {
            vec![]
        }
        ScalarNode::BinaryOp { left, right, .. } => vec![*left, *right],
        ScalarNode::UnaryOp { child, .. }
        | ScalarNode::Cast { child, .. }
        | ScalarNode::IsNull { child, .. }
        | ScalarNode::Nested(child)
        | ScalarNode::IsTruthValue { child, .. } => vec![*child],
        ScalarNode::FunctionCall { args, .. } | ScalarNode::AggregateCall { args, .. } => {
            args.clone()
        }
        ScalarNode::LambdaFunction { body, .. } | ScalarNode::Lambda { body, .. } => vec![*body],
        ScalarNode::InList { child, list, .. } => {
            let mut out = Vec::with_capacity(1 + list.len());
            out.push(*child);
            out.extend(list.iter().copied());
            out
        }
        ScalarNode::Between {
            child, low, high, ..
        } => vec![*child, *low, *high],
        ScalarNode::Like { child, pattern, .. } => vec![*child, *pattern],
        ScalarNode::Case {
            operand,
            when_then,
            else_expr,
        } => {
            let mut out = Vec::with_capacity(operand.iter().count() + when_then.len() * 2 + 1);
            out.extend(operand.iter().copied());
            for (when, then) in when_then {
                out.push(*when);
                out.push(*then);
            }
            out.extend(else_expr.iter().copied());
            out
        }
        ScalarNode::WindowCall {
            args, partition_by, ..
        } => {
            let mut out = Vec::with_capacity(args.len() + partition_by.len());
            out.extend(args.iter().copied());
            out.extend(partition_by.iter().copied());
            out
        }
    }
}

pub(crate) fn column_ref(arena: &mut ScalarArena, c: &OutputColumn) -> ScalarId {
    arena.remember_project_output_display(c.column_id, None, c.name.clone());
    arena.intern(
        ScalarNode::ColumnRef(c.column_id),
        c.data_type.clone(),
        c.nullable,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{ExprKind, LiteralValue, OutputColumn, ProjectItem, TypedExpr};
    use crate::sql::catalog::{ColumnDef, ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::scalar::ScalarArena;
    use crate::sql::planner::plan::{
        AggregateCall, LogicalAggregateNode, LogicalFilterNode, LogicalPlanKind, LogicalPlanNode,
        LogicalProjectNode, LogicalScanNode, LogicalSortNode,
    };
    use arrow::datatypes::DataType;

    fn col(id: u32, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId(id),
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: true,
            is_internal: false,
        }
    }

    fn col_ref(c: &OutputColumn) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: c.column_id,
                qualifier: None,
                column: c.name.clone(),
            },
            data_type: c.data_type.clone(),
            nullable: c.nullable,
        }
    }

    fn int_lit(v: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(v)),
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn cmp(left: TypedExpr, op: crate::sql::analysis::BinOp, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            },
            data_type: DataType::Boolean,
            nullable: true,
        }
    }

    /// Simplest `ScanSource` to construct in unit tests. The descriptor logic
    /// never inspects `ScanSource`, so the cheap `StarRocks` variant (two i64s)
    /// is sufficient and avoids building a heavy `IcebergTableInfo`.
    fn test_scan_source() -> ScanSource {
        ScanSource::StarRocks {
            db_id: 0,
            table_id: 0,
        }
    }

    fn scan(cols: &[OutputColumn]) -> LogicalScanNode {
        LogicalScanNode {
            database: "db".to_string(),
            table: TableDef {
                name: "t".to_string(),
                columns: cols
                    .iter()
                    .map(|c| ColumnDef {
                        name: c.name.clone(),
                        data_type: c.data_type.clone(),
                        nullable: c.nullable,
                        write_default: None,
                        logical_type: None,
                    })
                    .collect(),
                iceberg_row_lineage_metadata_columns: vec![],
                source: test_scan_source(),
            },
            alias: None,
            columns: cols.to_vec(),
            predicates: vec![],
            required_columns: None,
            variant_columns: vec![],
            mv_rewritten_from: None,
        }
    }

    fn scan_plan(cols: &[OutputColumn]) -> LogicalPlanNode {
        LogicalPlanNode::new(LogicalPlanKind::Scan(scan(cols)), vec![], None)
    }

    /// A scan over a named table (the shared `scan()` helper hardcodes "t").
    fn scan_named(table_name: &str, cols: &[OutputColumn]) -> LogicalScanNode {
        let mut s = scan(cols);
        s.table.name = table_name.to_string();
        s
    }

    fn scan_plan_named(table_name: &str, cols: &[OutputColumn]) -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanKind::Scan(scan_named(table_name, cols)),
            vec![],
            None,
        )
    }

    /// Build `Join(left, right)` with the given kind and ON condition.
    fn join_plan(
        kind: crate::sql::common::expr::JoinKind,
        left: LogicalPlanNode,
        right: LogicalPlanNode,
        on: Option<TypedExpr>,
    ) -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanKind::Join(crate::sql::planner::plan::LogicalJoinNode {
                join_type: kind,
                condition: on,
            }),
            vec![left, right],
            None,
        )
    }

    fn descriptor_from_plan(
        plan: &LogicalPlanNode,
    ) -> Result<(SpjgDescriptor, ScalarArena), String> {
        let mut arena = ScalarArena::new();
        let opt_expr = crate::sql::planner::optimizer_bridge::plan::try_logical_plan_to_opt_expr(
            plan, &mut arena,
        )?;
        let descriptor = SpjgDescriptor::from_opt_expr(&opt_expr, &mut arena)?;
        Ok((descriptor, arena))
    }

    #[test]
    fn split_join_condition_separates_equi_edges_from_residuals() {
        use crate::sql::analysis::BinOp;

        let mut arena = ScalarArena::new();
        // a(=col 1) = b(=col 2)  AND  a > b   (the `>` conjunct is a residual;
        // two ColumnRefs avoid depending on the ScalarNode::Literal payload type).
        let a_ref = arena.intern(ScalarNode::ColumnRef(ColumnId(1)), DataType::Int64, true);
        let b_ref = arena.intern(ScalarNode::ColumnRef(ColumnId(2)), DataType::Int64, true);
        let eq = arena.intern(
            ScalarNode::BinaryOp {
                op: BinOp::Eq,
                left: a_ref,
                right: b_ref,
            },
            DataType::Boolean,
            true,
        );
        let gt = arena.intern(
            ScalarNode::BinaryOp {
                op: BinOp::Gt,
                left: a_ref,
                right: b_ref,
            },
            DataType::Boolean,
            true,
        );
        let cond = arena.intern(
            ScalarNode::BinaryOp {
                op: BinOp::And,
                left: eq,
                right: gt,
            },
            DataType::Boolean,
            true,
        );

        let left_columns = HashSet::from([ColumnId(1)]);
        let right_columns = HashSet::from([ColumnId(2)]);
        let (edges, residuals) =
            super::split_join_condition(&arena, Some(cond), &left_columns, &right_columns);
        assert_eq!(edges.len(), 1, "one equi edge");
        assert_eq!(edges[0].left, ColumnId(1));
        assert_eq!(edges[0].right, ColumnId(2));
        assert_eq!(residuals.len(), 1, "the a>b conjunct is a residual");
        assert_eq!(residuals, vec![gt]);
    }

    #[test]
    fn split_join_condition_none_is_empty() {
        let arena = ScalarArena::new();
        let left_columns = HashSet::from([ColumnId(1)]);
        let right_columns = HashSet::from([ColumnId(2)]);
        let (edges, residuals) =
            super::split_join_condition(&arena, None, &left_columns, &right_columns);
        assert!(edges.is_empty() && residuals.is_empty());
    }

    #[test]
    fn split_join_condition_normalizes_reversed_equi_edge() {
        use crate::sql::analysis::BinOp;

        let mut arena = ScalarArena::new();
        let a_ref = arena.intern(ScalarNode::ColumnRef(ColumnId(1)), DataType::Int64, true);
        let c_ref = arena.intern(ScalarNode::ColumnRef(ColumnId(3)), DataType::Int64, true);
        let cond = arena.intern(
            ScalarNode::BinaryOp {
                op: BinOp::Eq,
                left: c_ref,
                right: a_ref,
            },
            DataType::Boolean,
            true,
        );

        let left_columns = HashSet::from([ColumnId(1), ColumnId(2)]);
        let right_columns = HashSet::from([ColumnId(3), ColumnId(4)]);
        let (edges, residuals) =
            super::split_join_condition(&arena, Some(cond), &left_columns, &right_columns);
        assert!(residuals.is_empty());
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].left, ColumnId(1));
        assert_eq!(edges[0].right, ColumnId(3));
    }

    #[test]
    fn split_join_condition_keeps_same_side_equality_as_residual() {
        use crate::sql::analysis::BinOp;

        let mut arena = ScalarArena::new();
        let a_ref = arena.intern(ScalarNode::ColumnRef(ColumnId(1)), DataType::Int64, true);
        let b_ref = arena.intern(ScalarNode::ColumnRef(ColumnId(2)), DataType::Int64, true);
        let cond = arena.intern(
            ScalarNode::BinaryOp {
                op: BinOp::Eq,
                left: a_ref,
                right: b_ref,
            },
            DataType::Boolean,
            true,
        );

        let left_columns = HashSet::from([ColumnId(1), ColumnId(2)]);
        let right_columns = HashSet::from([ColumnId(3), ColumnId(4)]);
        let (edges, residuals) =
            super::split_join_condition(&arena, Some(cond), &left_columns, &right_columns);
        assert!(edges.is_empty());
        assert_eq!(residuals, vec![cond]);
    }

    #[test]
    fn extracts_inner_join_two_tables() {
        use crate::sql::common::expr::JoinKind;

        // SELECT ... FROM t1 JOIN t2 ON t1.a = t2.c
        let a = col(1, "a");
        let b = col(2, "b");
        let c = col(3, "c");
        let d = col(4, "d");
        let on = cmp(col_ref(&a), crate::sql::analysis::BinOp::Eq, col_ref(&c));
        let plan = join_plan(
            JoinKind::Inner,
            scan_plan_named("t1", &[a.clone(), b.clone()]),
            scan_plan_named("t2", &[c.clone(), d.clone()]),
            Some(on),
        );
        let (desc, _arena) = descriptor_from_plan(&plan).expect("inner join spjg");
        assert_eq!(desc.table.name, "t1", "driving (left-deep first) scan");
        let joins = desc.joins.as_ref().expect("join shape present");
        assert_eq!(joins.inputs.len(), 1);
        assert_eq!(joins.inputs[0].table.name, "t2");
        assert_eq!(joins.equi_edges.len(), 1);
        assert_eq!(joins.equi_edges[0].left, ColumnId(1));
        assert_eq!(joins.equi_edges[0].right, ColumnId(3));
    }

    #[test]
    fn from_opt_expr_top_project_over_multitable_join_binds_both_sides() {
        // A bare `SELECT <driving col>, <join-input col> FROM t1 JOIN t2 ...`
        // (no aggregate, no project directly under the join) is the shape a
        // real 2-table-join MV definition produces end-to-end through the
        // standalone analyze/plan pipeline (E1-bc Task 6). Regression guard
        // for a `from_opt_expr` gap where the no-aggregate arm's base map
        // only carried the driving scan's own columns, so a top-project item
        // addressing a join INPUT's column (here, t2.d) failed with "top
        // project references unknown column" even though the join shape
        // itself was extracted correctly.
        use crate::sql::common::expr::JoinKind;

        let a = col(1, "a");
        let c = col(3, "c");
        let d = col(4, "d");
        let on = cmp(col_ref(&a), crate::sql::analysis::BinOp::Eq, col_ref(&c));
        let join = join_plan(
            JoinKind::Inner,
            scan_plan_named("t1", std::slice::from_ref(&a)),
            scan_plan_named("t2", &[c.clone(), d.clone()]),
            Some(on),
        );
        let out_a = col(10, "out_a");
        let out_d = col(11, "out_d");
        let project = LogicalPlanNode::new(
            LogicalPlanKind::Project(LogicalProjectNode {
                items: vec![
                    ProjectItem {
                        expr: col_ref(&a),
                        output_name: "out_a".to_string(),
                        output_column_id: out_a.column_id,
                    },
                    ProjectItem {
                        expr: col_ref(&d),
                        output_name: "out_d".to_string(),
                        output_column_id: out_d.column_id,
                    },
                ],
                output_qualifier: None,
            }),
            vec![join],
            None,
        );

        let (desc, _arena) =
            descriptor_from_plan(&project).expect("top project over multitable join spjg");
        assert_eq!(desc.outputs.len(), 2);
        assert_eq!(desc.outputs[0].column_id, out_a.column_id);
        assert_eq!(desc.outputs[0].name, "out_a");
        assert_eq!(
            desc.outputs[1].column_id, out_d.column_id,
            "top project item addressing the JOIN INPUT's column (t2.d) must bind"
        );
        assert_eq!(desc.outputs[1].name, "out_d");
    }

    #[test]
    fn base_name_of_includes_join_input_columns() {
        use crate::sql::common::expr::JoinKind;

        let a = col(1, "a");
        let b = col(2, "b");
        let c = col(3, "c");
        let d = col(4, "d");
        let on = cmp(col_ref(&a), crate::sql::analysis::BinOp::Eq, col_ref(&c));
        let plan = join_plan(
            JoinKind::Inner,
            scan_plan_named("t1", &[a.clone(), b.clone()]),
            scan_plan_named("t2", &[c.clone(), d.clone()]),
            Some(on),
        );
        let (desc, _arena) = descriptor_from_plan(&plan).expect("inner join spjg");
        let names = desc.base_name_of();
        // Driving scan columns, qualified by table identity. `scan_named`'s
        // fixture uses non-Iceberg `test_scan_source()`, so qualification
        // falls back to the bare table name ("t1"/"t2") rather than an
        // Iceberg FQN -- see `table_qualifier`.
        assert_eq!(
            names.get(&ColumnId(1)).map(String::as_str),
            Some("t1\u{1}a")
        );
        assert_eq!(
            names.get(&ColumnId(2)).map(String::as_str),
            Some("t1\u{1}b")
        );
        // Join-input columns must also be present, qualified by "t2":
        assert_eq!(
            names.get(&ColumnId(3)).map(String::as_str),
            Some("t2\u{1}c")
        );
        assert_eq!(
            names.get(&ColumnId(4)).map(String::as_str),
            Some("t2\u{1}d")
        );
    }

    #[test]
    fn base_name_of_qualifies_same_named_columns_across_tables() {
        use crate::sql::common::expr::JoinKind;

        // t1.a and t2.a share the bare column name "a"; base_name_of must
        // produce DISTINCT qualified keys so downstream matching
        // (normalize / check_containment) never confuses the two tables'
        // columns (E1-bc design spec §2.1).
        let a1 = col(1, "a");
        let a2 = col(2, "a");
        let on = cmp(col_ref(&a1), crate::sql::analysis::BinOp::Eq, col_ref(&a2));
        let plan = join_plan(
            JoinKind::Inner,
            scan_plan_named("t1", std::slice::from_ref(&a1)),
            scan_plan_named("t2", std::slice::from_ref(&a2)),
            Some(on),
        );
        let (desc, _arena) = descriptor_from_plan(&plan).expect("inner join spjg");
        let names = desc.base_name_of();
        let t1_a = names.get(&ColumnId(1)).expect("t1.a key").clone();
        let t2_a = names.get(&ColumnId(2)).expect("t2.a key").clone();
        assert_ne!(
            t1_a, t2_a,
            "t1.a and t2.a must not collide despite sharing the bare name \"a\""
        );
    }

    #[test]
    fn base_name_of_single_table_key_matches_across_independent_column_ids() {
        // Zero-regression check: the SAME physical table, scanned with
        // completely different ColumnId ranges on two independent
        // descriptors (mirroring query-side vs MV-side allocation), must
        // produce the SAME qualified key for the same column name --
        // qualification must not break single-table cross-side matching.
        // (This assertion already holds trivially under the OLD bare-name
        // scheme too -- it locks a property that must keep holding, it is
        // not itself a red/green test for this task.)
        let query_a = col(1, "a");
        let (query_desc, _q_arena) =
            descriptor_from_plan(&scan_plan(std::slice::from_ref(&query_a))).expect("query spjg");
        let mv_a = col(101, "a");
        let (mv_desc, _mv_arena) =
            descriptor_from_plan(&scan_plan(std::slice::from_ref(&mv_a))).expect("mv spjg");

        let query_names = query_desc.base_name_of();
        let mv_names = mv_desc.base_name_of();
        assert_eq!(
            query_names.get(&ColumnId(1)),
            mv_names.get(&ColumnId(101)),
            "same physical table + same column name -> same qualified key \
             regardless of ColumnId range"
        );
    }

    #[test]
    fn extracts_inner_join_with_driving_filter() {
        use crate::sql::common::expr::JoinKind;

        // SELECT ... FROM (SELECT * FROM t1 WHERE a >= 5) t1 JOIN t2 ON t1.a = t2.c
        let a = col(1, "a");
        let b = col(2, "b");
        let c = col(3, "c");
        let driving = LogicalPlanNode::new(
            LogicalPlanKind::Filter(LogicalFilterNode {
                predicate: cmp(col_ref(&a), crate::sql::analysis::BinOp::Ge, int_lit(5)),
            }),
            vec![scan_plan_named("t1", &[a.clone(), b.clone()])],
            None,
        );
        let on = cmp(col_ref(&a), crate::sql::analysis::BinOp::Eq, col_ref(&c));
        let plan = join_plan(
            JoinKind::Inner,
            driving,
            scan_plan_named("t2", std::slice::from_ref(&c)),
            Some(on),
        );

        let (desc, _arena) = descriptor_from_plan(&plan).expect("inner join spjg");
        assert_eq!(desc.table.name, "t1");
        assert_eq!(
            desc.predicates.len(),
            1,
            "driving-side filter predicate must be included exactly once"
        );
        let joins = desc.joins.as_ref().expect("join shape present");
        assert_eq!(joins.inputs.len(), 1);
        assert_eq!(joins.equi_edges.len(), 1);
    }

    #[test]
    fn rejects_inner_join_without_condition() {
        use crate::sql::common::expr::JoinKind;

        let a = col(1, "a");
        let c = col(3, "c");
        let plan = join_plan(
            JoinKind::Inner,
            scan_plan_named("t1", std::slice::from_ref(&a)),
            scan_plan_named("t2", std::slice::from_ref(&c)),
            None,
        );
        assert!(
            descriptor_from_plan(&plan).is_err(),
            "inner join without equi edges must be rejected"
        );
    }

    #[test]
    fn rejects_inner_join_with_only_residual_condition() {
        use crate::sql::common::expr::JoinKind;

        let a = col(1, "a");
        let c = col(3, "c");
        let on = cmp(col_ref(&a), crate::sql::analysis::BinOp::Gt, col_ref(&c));
        let plan = join_plan(
            JoinKind::Inner,
            scan_plan_named("t1", std::slice::from_ref(&a)),
            scan_plan_named("t2", std::slice::from_ref(&c)),
            Some(on),
        );
        assert!(
            descriptor_from_plan(&plan).is_err(),
            "inner join without equi edges must be rejected"
        );
    }

    #[test]
    fn rejects_inner_join_with_only_same_side_equality() {
        use crate::sql::common::expr::JoinKind;

        let a = col(1, "a");
        let b = col(2, "b");
        let c = col(3, "c");
        let on = cmp(col_ref(&a), crate::sql::analysis::BinOp::Eq, col_ref(&b));
        let plan = join_plan(
            JoinKind::Inner,
            scan_plan_named("t1", &[a.clone(), b.clone()]),
            scan_plan_named("t2", std::slice::from_ref(&c)),
            Some(on),
        );
        assert!(
            descriptor_from_plan(&plan).is_err(),
            "same-side equality must not be extracted as a join edge"
        );
    }

    #[test]
    fn rejects_left_outer_join() {
        use crate::sql::common::expr::JoinKind;

        let a = col(1, "a");
        let c = col(3, "c");
        let on = cmp(col_ref(&a), crate::sql::analysis::BinOp::Eq, col_ref(&c));
        let plan = join_plan(
            JoinKind::LeftOuter,
            scan_plan_named("t1", std::slice::from_ref(&a)),
            scan_plan_named("t2", std::slice::from_ref(&c)),
            Some(on),
        );
        assert!(
            descriptor_from_plan(&plan).is_err(),
            "outer join must be rejected"
        );
    }

    #[test]
    fn rejects_right_deep_join() {
        use crate::sql::common::expr::JoinKind;

        // t1 JOIN (t2 JOIN t3): right child is itself a join -> unsupported in v1.
        let a = col(1, "a");
        let c = col(3, "c");
        let e = col(5, "e");
        let on_inner = cmp(col_ref(&c), crate::sql::analysis::BinOp::Eq, col_ref(&e));
        let inner = join_plan(
            JoinKind::Inner,
            scan_plan_named("t2", std::slice::from_ref(&c)),
            scan_plan_named("t3", std::slice::from_ref(&e)),
            Some(on_inner),
        );
        let on_outer = cmp(col_ref(&a), crate::sql::analysis::BinOp::Eq, col_ref(&c));
        let plan = join_plan(
            JoinKind::Inner,
            scan_plan_named("t1", std::slice::from_ref(&a)),
            inner,
            Some(on_outer),
        );
        assert!(
            descriptor_from_plan(&plan).is_err(),
            "right-deep join must be rejected"
        );
    }

    #[test]
    fn extracts_filter_scan_shape() {
        let a = col(1, "a");
        let b = col(2, "b");
        let plan = LogicalPlanNode::new(
            LogicalPlanKind::Filter(LogicalFilterNode {
                predicate: cmp(col_ref(&a), crate::sql::analysis::BinOp::Ge, int_lit(5)),
            }),
            vec![scan_plan(&[a.clone(), b.clone()])],
            None,
        );
        let (d, _arena) = descriptor_from_plan(&plan).expect("spjg");
        assert_eq!(d.table.name, "t");
        assert_eq!(d.predicates.len(), 1);
        assert!(d.aggregate.is_none());
        assert_eq!(d.outputs.len(), 2); // pass-through scan columns
    }

    #[test]
    fn single_table_descriptor_has_no_joins() {
        let a = col(1, "a");
        let b = col(2, "b");
        let plan = LogicalPlanNode::new(
            LogicalPlanKind::Filter(LogicalFilterNode {
                predicate: cmp(col_ref(&a), crate::sql::analysis::BinOp::Ge, int_lit(5)),
            }),
            vec![scan_plan(&[a.clone(), b.clone()])],
            None,
        );
        let (d, _arena) = descriptor_from_plan(&plan).expect("spjg");
        assert!(
            d.joins.is_none(),
            "single-table shape must not carry a JoinShape"
        );
    }

    #[test]
    fn extracts_aggregate_shape_and_rejects_join() {
        let a = col(1, "a");
        let v = col(2, "v");
        let sum_out = col(3, "s");
        let plan = LogicalPlanNode::new(
            LogicalPlanKind::Aggregate(LogicalAggregateNode {
                group_by: vec![col_ref(&a)],
                aggregates: vec![AggregateCall {
                    name: "sum".to_string(),
                    args: vec![col_ref(&v)],
                    distinct: false,
                    result_type: DataType::Int64,
                    order_by: vec![],
                    output_column_id: sum_out.column_id,
                }],
                output_columns: vec![col(1, "a"), sum_out.clone()],
                already_pushed: false,
            }),
            vec![scan_plan(&[a.clone(), v.clone()])],
            None,
        );
        let (d, _arena) = descriptor_from_plan(&plan).expect("spjg");
        let agg = d.aggregate.as_ref().expect("aggregate present");
        assert_eq!(agg.group_by.len(), 1);
        // outputs: Dimension(a) then Aggregate(sum(v)); the aggregate call is
        // captured in `outputs`, not duplicated on `SpjgAggregate`.
        assert_eq!(d.outputs.len(), 2);
        assert!(matches!(d.outputs[0].expr, SpjgOutputExpr::Dimension(_)));
        assert!(matches!(d.outputs[1].expr, SpjgOutputExpr::Aggregate(_)));
    }

    #[test]
    fn rejects_sort_and_window() {
        // Any node outside {Scan, Filter, Project, Aggregate} must yield Err.
        let a = col(1, "a");
        let plan = LogicalPlanNode::new(
            LogicalPlanKind::Sort(LogicalSortNode {
                items: vec![],
                analytic_partition_by: vec![],
                output_columns: vec![],
                offset: None,
                partition_limit: None,
                topn_type: None,
            }),
            vec![scan_plan(std::slice::from_ref(&a))],
            None,
        );
        assert!(descriptor_from_plan(&plan).is_err());
    }

    /// Build a memo from a plan and return the first logical expr of the root
    /// group (cloned) plus the memo, ready for `from_memo`.
    fn memo_root(plan: &LogicalPlanNode) -> (crate::sql::optimizer::memo::Memo, MExpr) {
        let mut memo = crate::sql::optimizer::memo::Memo::new();
        let opt_expr = crate::sql::planner::optimizer_bridge::plan::try_logical_plan_to_opt_expr(
            plan,
            &mut memo.scalars,
        )
        .expect("logical plan to opt expr");
        let root = crate::sql::optimizer::memo_copy::opt_expr_to_memo(&opt_expr, &mut memo);
        let root_expr = memo.groups[root].logical_exprs[0].clone();
        (memo, root_expr)
    }

    #[test]
    fn from_memo_matches_filter_scan() {
        let a = col(1, "a");
        let b = col(2, "b");
        let plan = LogicalPlanNode::new(
            LogicalPlanKind::Filter(LogicalFilterNode {
                predicate: cmp(col_ref(&a), crate::sql::analysis::BinOp::Ge, int_lit(5)),
            }),
            vec![scan_plan(&[a.clone(), b.clone()])],
            None,
        );
        let (logical, _arena) = descriptor_from_plan(&plan).expect("spjg");
        let (mut memo, root_expr) = memo_root(&plan);
        let (mem, shape) = SpjgDescriptor::from_memo(&root_expr, &mut memo).expect("from_memo");
        assert!(matches!(shape, MatchedShape::Spj));
        assert_eq!(mem.table.name, logical.table.name);
        assert_eq!(mem.predicates.len(), logical.predicates.len());
        assert!(mem.aggregate.is_none());
        assert_eq!(mem.outputs.len(), logical.outputs.len());
    }

    #[test]
    fn from_memo_extracts_inner_join_two_tables() {
        use crate::sql::common::expr::JoinKind;

        let a = col(1, "a");
        let b = col(2, "b");
        let c = col(3, "c");
        let d = col(4, "d");
        let on = cmp(col_ref(&a), crate::sql::analysis::BinOp::Eq, col_ref(&c));
        let plan = join_plan(
            JoinKind::Inner,
            scan_plan_named("t1", &[a.clone(), b.clone()]),
            scan_plan_named("t2", &[c.clone(), d.clone()]),
            Some(on),
        );
        let (mut memo, root_expr) = memo_root(&plan);
        let (mem, shape) =
            SpjgDescriptor::from_memo(&root_expr, &mut memo).expect("from_memo join");
        assert!(matches!(shape, MatchedShape::Spj));
        assert_eq!(mem.table.name, "t1");
        let joins = mem.joins.as_ref().expect("join shape present");
        assert_eq!(joins.inputs.len(), 1);
        assert_eq!(joins.inputs[0].table.name, "t2");
        assert_eq!(joins.equi_edges.len(), 1);
        assert_eq!(joins.equi_edges[0].left, ColumnId(1));
        assert_eq!(joins.equi_edges[0].right, ColumnId(3));
    }

    #[test]
    fn from_memo_rejects_left_outer_join() {
        use crate::sql::common::expr::JoinKind;

        let a = col(1, "a");
        let c = col(3, "c");
        let on = cmp(col_ref(&a), crate::sql::analysis::BinOp::Eq, col_ref(&c));
        let plan = join_plan(
            JoinKind::LeftOuter,
            scan_plan_named("t1", std::slice::from_ref(&a)),
            scan_plan_named("t2", std::slice::from_ref(&c)),
            Some(on),
        );
        let (mut memo, root_expr) = memo_root(&plan);
        assert!(SpjgDescriptor::from_memo(&root_expr, &mut memo).is_none());
    }

    #[test]
    fn from_memo_extracts_inner_join_with_driving_filter() {
        use crate::sql::common::expr::JoinKind;

        let a = col(1, "a");
        let b = col(2, "b");
        let c = col(3, "c");
        let driving = LogicalPlanNode::new(
            LogicalPlanKind::Filter(LogicalFilterNode {
                predicate: cmp(col_ref(&a), crate::sql::analysis::BinOp::Ge, int_lit(5)),
            }),
            vec![scan_plan_named("t1", &[a.clone(), b.clone()])],
            None,
        );
        let on = cmp(col_ref(&a), crate::sql::analysis::BinOp::Eq, col_ref(&c));
        let plan = join_plan(
            JoinKind::Inner,
            driving,
            scan_plan_named("t2", std::slice::from_ref(&c)),
            Some(on),
        );
        let (mut memo, root_expr) = memo_root(&plan);
        let (mem, shape) =
            SpjgDescriptor::from_memo(&root_expr, &mut memo).expect("from_memo join");
        assert!(matches!(shape, MatchedShape::Spj));
        assert_eq!(mem.table.name, "t1");
        assert_eq!(
            mem.predicates.len(),
            1,
            "driving-side filter predicate must be included exactly once"
        );
        let joins = mem.joins.as_ref().expect("join shape present");
        assert_eq!(joins.inputs.len(), 1);
        assert_eq!(joins.equi_edges.len(), 1);
    }

    #[test]
    fn from_memo_rejects_inner_join_without_condition() {
        use crate::sql::common::expr::JoinKind;

        let a = col(1, "a");
        let c = col(3, "c");
        let plan = join_plan(
            JoinKind::Inner,
            scan_plan_named("t1", std::slice::from_ref(&a)),
            scan_plan_named("t2", std::slice::from_ref(&c)),
            None,
        );
        let (mut memo, root_expr) = memo_root(&plan);
        assert!(SpjgDescriptor::from_memo(&root_expr, &mut memo).is_none());
    }

    #[test]
    fn from_memo_matches_aggregate() {
        let a = col(1, "a");
        let v = col(2, "v");
        let sum_out = col(3, "s");
        let plan = LogicalPlanNode::new(
            LogicalPlanKind::Aggregate(LogicalAggregateNode {
                group_by: vec![col_ref(&a)],
                aggregates: vec![AggregateCall {
                    name: "sum".to_string(),
                    args: vec![col_ref(&v)],
                    distinct: false,
                    result_type: DataType::Int64,
                    order_by: vec![],
                    output_column_id: sum_out.column_id,
                }],
                output_columns: vec![col(1, "a"), sum_out.clone()],
                already_pushed: false,
            }),
            vec![scan_plan(&[a.clone(), v.clone()])],
            None,
        );
        let (logical, _arena) = descriptor_from_plan(&plan).expect("spjg");
        let (mut memo, root_expr) = memo_root(&plan);
        let (mem, shape) = SpjgDescriptor::from_memo(&root_expr, &mut memo).expect("from_memo");
        // Shape carries the original aggregate op for output-id reuse.
        let MatchedShape::Spjg { original_agg } = &shape else {
            panic!("expected Spjg shape");
        };
        assert_eq!(original_agg.output_columns.len(), 2);
        assert_eq!(original_agg.stage, AggStage::Single);
        let agg = mem.aggregate.as_ref().expect("aggregate present");
        let logical_agg = logical.aggregate.as_ref().expect("aggregate present");
        assert_eq!(agg.group_by.len(), logical_agg.group_by.len());
        // Aggregate-call parity is covered by output-length equality
        // (outputs = group keys + aggregates) below.
        assert_eq!(mem.outputs.len(), logical.outputs.len());
    }

    #[test]
    fn from_memo_rejects_split_aggregate() {
        use crate::sql::optimizer::operator::{
            AggregateOutputLayout, LogicalAggregateOp, Operator,
        };
        use crate::sql::planner::optimizer_bridge::scalar::{intern_aggregate_calls, intern_exprs};
        // A split (Local) aggregate is not the original Single shape and must
        // be rejected even when it sits at the matched position.
        let a = col(1, "a");
        let v = col(2, "v");
        let sum_out = col(3, "s");
        let scan_op = scan(&[a.clone(), v.clone()]);
        let plan = LogicalPlanNode::new(LogicalPlanKind::Scan(scan_op), vec![], None);
        let mut memo = crate::sql::optimizer::memo::Memo::new();
        let opt_expr = crate::sql::planner::optimizer_bridge::plan::try_logical_plan_to_opt_expr(
            &plan,
            &mut memo.scalars,
        )
        .expect("logical plan to opt expr");
        let scan_gid = crate::sql::optimizer::memo_copy::opt_expr_to_memo(&opt_expr, &mut memo);
        let group_by = intern_exprs(&mut memo.scalars, &[col_ref(&a)]);
        let aggregates = intern_aggregate_calls(
            &mut memo.scalars,
            &[AggregateCall {
                name: "sum".to_string(),
                args: vec![col_ref(&v)],
                distinct: false,
                result_type: DataType::Int64,
                order_by: vec![],
                output_column_id: sum_out.column_id,
            }],
        );
        let output_columns = vec![col(1, "a"), sum_out.clone()];
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
        let split = LogicalAggregateOp::staged(
            AggStage::Local,
            group_by,
            aggregates,
            output_layout,
            output_columns,
            vec![false],
            true,
        );
        let expr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalAggregate(split),
            children: vec![scan_gid],
        };
        assert!(SpjgDescriptor::from_memo(&expr, &mut memo).is_none());
    }
}
