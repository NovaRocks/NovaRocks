//! The MvRewrite Cascades transformation rule.
//!
//! Given a set of prepared `MvRewriteCandidate`s, this rule extracts the SPJG
//! view of each matched memo subtree (`SpjgDescriptor::from_memo`), checks each
//! candidate for table identity + predicate containment + aggregate rollup
//! compatibility, and — when all checks pass — injects an alternative
//! expression that reads the MV's materialized target table instead of the base
//! table. The alternative is added to the SAME memo group, so the cost-based
//! search later picks whichever is cheaper.
//!
//! StarRocks counterparts: MaterializedViewRewriter /
//! AggregatedMaterializedViewRewriter.

use std::collections::HashSet;
use std::sync::Mutex;

use crate::sql::analysis::{ExprKind, LiteralValue, OutputColumn, ProjectItem, TypedExpr};
use crate::sql::optimizer::memo::{MExpr, MExprId, Memo};
use crate::sql::optimizer::operator::{
    LogicalAggregateOp, LogicalFilterOp, LogicalProjectOp, LogicalScanOp, Operator,
};
use crate::sql::optimizer::rule::{NewExpr, Rule, RuleType};
use crate::sql::planner::plan::AggregateCall;

use super::aggregate_rollup::{RollupKind, plan_rollup};
use super::column_mapping::{MvColumnMap, NormExpr, normalize};
use super::descriptor::{MatchedShape, SpjgDescriptor, SpjgOutputExpr};
use super::predicate_split::check_containment;
use super::{MvRewriteCandidate, RULE_NAME};

pub(crate) struct MvRewriteRule {
    candidates: Vec<MvRewriteCandidate>,
    /// (matched MExpr id, candidate index) pairs already attempted. The
    /// explore loop re-visits expressions every round; without this guard
    /// each round would mint fresh child groups forever.
    applied: Mutex<HashSet<(MExprId, usize)>>,
}

impl MvRewriteRule {
    pub(crate) fn new(candidates: Vec<MvRewriteCandidate>) -> Self {
        Self {
            candidates,
            applied: Mutex::new(HashSet::new()),
        }
    }
}

impl Rule for MvRewriteRule {
    fn name(&self) -> &str {
        RULE_NAME
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Transformation
    }

    fn matches(&self, op: &Operator) -> bool {
        matches!(
            op,
            Operator::LogicalAggregate(_) | Operator::LogicalFilter(_) | Operator::LogicalScan(_)
        )
    }

    fn apply(&self, expr: &MExpr, memo: &mut Memo) -> Vec<NewExpr> {
        let Some((query, shape)) = SpjgDescriptor::from_memo(expr, memo) else {
            return vec![];
        };
        let mut out = Vec::new();
        for (idx, cand) in self.candidates.iter().enumerate() {
            {
                let mut applied = self.applied.lock().expect("mv rewrite applied set");
                if !applied.insert((expr.id, idx)) {
                    continue;
                }
            }
            if let Some(alt) = try_rewrite(&query, &shape, cand, memo) {
                out.push(alt);
            }
        }
        out
    }
}

fn try_rewrite(
    query: &SpjgDescriptor,
    shape: &MatchedShape,
    cand: &MvRewriteCandidate,
    memo: &mut Memo,
) -> Option<NewExpr> {
    // 1. Same physical base table (compare Iceberg identity, not names).
    if !same_iceberg_table(&query.table, &cand.mv.table) {
        return None;
    }
    let q_names = query.base_name_of();
    let m_names = cand.mv.base_name_of();

    // 2. Predicate containment + compensation (still over base columns).
    let containment =
        check_containment(&query.predicates, &cand.mv.predicates, &q_names, &m_names)?;

    // 3. Allocate the MV scan: one new ColumnId per MV visible output,
    //    bound by NAME to the target table columns.
    let mut scan_columns: Vec<OutputColumn> = Vec::new();
    let mut dims: Vec<(NormExpr, OutputColumn)> = Vec::new();
    let mut agg_cols: Vec<Option<OutputColumn>> = vec![None; cand.mv.outputs.len()];
    for (i, mv_out) in cand.mv.outputs.iter().enumerate() {
        let col_def = cand
            .target_table
            .columns
            .iter()
            .find(|c| c.name == mv_out.name)?; // visible-by-name mapping (spec §5)
        let id = memo.factory.create(
            Some(cand.target_table.name.clone()),
            col_def.name.clone(),
            col_def.data_type.clone(),
            col_def.nullable,
        );
        let oc = OutputColumn {
            column_id: id,
            name: col_def.name.clone(),
            data_type: col_def.data_type.clone(),
            nullable: col_def.nullable,
            is_internal: false,
        };
        scan_columns.push(oc.clone());
        match &mv_out.expr {
            SpjgOutputExpr::Dimension(e) => {
                dims.push((normalize(e, &m_names)?, oc));
            }
            SpjgOutputExpr::Aggregate(_) => agg_cols[i] = Some(oc),
        }
    }
    let col_map = MvColumnMap::new(dims);

    // 4. Compensation predicates rewritten onto MV columns. For SPJG MVs
    //    they may only land on group-key columns (spec §6.3): aggregate
    //    columns are not row-filterable. MvColumnMap only contains
    //    Dimension outputs, so any compensation touching an aggregate
    //    column simply fails to rewrite -> candidate dropped.
    let compensation: Vec<TypedExpr> = containment
        .compensation
        .iter()
        .map(|p| col_map.rewrite(p, &q_names))
        .collect::<Option<Vec<_>>>()?;

    // 5. Build the operator chain bottom-up.
    let scan_group = memo.new_group(MExpr {
        id: memo.next_expr_id(),
        op: Operator::LogicalScan(LogicalScanOp {
            database: cand.target_database.clone(),
            table: cand.target_table.clone(),
            alias: None,
            columns: scan_columns,
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
            variant_columns: vec![],
            mv_rewritten_from: Some(cand.mv_name.clone()),
        }),
        children: vec![],
    });
    let mut child_group = scan_group;
    if !compensation.is_empty() {
        let predicate = combine_and(compensation);
        child_group = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalFilter(LogicalFilterOp { predicate }),
            children: vec![scan_group],
        });
    }

    // 6. Top operator: reproduce the matched group's output ColumnIds.
    match (shape, &cand.mv.aggregate) {
        // SPJ query on SPJ MV: Project binding original output ids.
        (MatchedShape::Spj, None) => {
            let items = query
                .outputs
                .iter()
                .map(|o| {
                    let SpjgOutputExpr::Dimension(e) = &o.expr else {
                        return None;
                    };
                    Some(ProjectItem {
                        expr: col_map.rewrite(e, &q_names)?,
                        output_name: o.name.clone(),
                        output_column_id: o.column_id,
                    })
                })
                .collect::<Option<Vec<_>>>()?;
            Some(NewExpr {
                op: Operator::LogicalProject(LogicalProjectOp {
                    items,
                    output_qualifier: None,
                }),
                children: vec![child_group],
            })
        }
        // SPJ query cannot read an aggregated MV (detail rows are gone).
        (MatchedShape::Spj, Some(_)) => None,
        // SPJG query on SPJ MV: keep the query aggregate, args rewritten.
        (MatchedShape::Spjg { original_agg }, None) => {
            let group_by = original_agg
                .group_by
                .iter()
                .map(|e| col_map.rewrite(e, &q_names))
                .collect::<Option<Vec<_>>>()?;
            let aggregates = original_agg
                .aggregates
                .iter()
                .map(|c| {
                    Some(AggregateCall {
                        args: c
                            .args
                            .iter()
                            .map(|a| col_map.rewrite(a, &q_names))
                            .collect::<Option<Vec<_>>>()?,
                        ..c.clone()
                    })
                })
                .collect::<Option<Vec<_>>>()?;
            // DISTINCT over an SPJ MV is sound (detail rows preserved); args
            // were rewritten like any other above, so no special handling.
            Some(NewExpr {
                op: Operator::LogicalAggregate(LogicalAggregateOp::single(
                    group_by,
                    aggregates,
                    original_agg.output_columns.clone(),
                )),
                children: vec![child_group],
            })
        }
        // SPJG query on SPJG MV: direct mapping or rollup.
        (MatchedShape::Spjg { original_agg }, Some(mv_agg)) => {
            let plan = plan_rollup(
                &original_agg.group_by,
                &original_agg.aggregates,
                &q_names,
                mv_agg,
                &cand.mv.outputs,
                &m_names,
            )?;
            let n_keys = original_agg.group_by.len();
            match plan.kind {
                RollupKind::Direct => {
                    // One row per group already: Project binding the original
                    // output ids (group keys then agg results).
                    let mut items: Vec<ProjectItem> = Vec::new();
                    for (i, oc) in original_agg.output_columns.iter().enumerate() {
                        let expr = if i < n_keys {
                            col_map.rewrite(&original_agg.group_by[i], &q_names)?
                        } else {
                            let item = &plan.items[i - n_keys];
                            let mv_col = agg_cols[item.mv_output_index].clone()?;
                            column_ref(&mv_col)
                        };
                        items.push(ProjectItem {
                            expr,
                            output_name: oc.name.clone(),
                            output_column_id: oc.column_id,
                        });
                    }
                    Some(NewExpr {
                        op: Operator::LogicalProject(LogicalProjectOp {
                            items,
                            output_qualifier: None,
                        }),
                        children: vec![child_group],
                    })
                }
                RollupKind::Rollup => {
                    let group_by = original_agg
                        .group_by
                        .iter()
                        .map(|e| col_map.rewrite(e, &q_names))
                        .collect::<Option<Vec<_>>>()?;
                    let needs_coalesce = plan.items.iter().any(|i| i.needs_coalesce);
                    // Aggregate outputs: reuse original ids directly unless a
                    // COALESCE wrapper project is needed (then mint fresh ids
                    // for the aggregate and bind originals in the project).
                    let mut agg_outputs = original_agg.output_columns.clone();
                    if needs_coalesce {
                        for oc in agg_outputs.iter_mut().skip(n_keys) {
                            oc.column_id = memo.factory.create(
                                None,
                                oc.name.clone(),
                                oc.data_type.clone(),
                                oc.nullable,
                            );
                        }
                    }
                    let aggregates = plan
                        .items
                        .iter()
                        .enumerate()
                        .map(|(i, item)| {
                            let mv_col = agg_cols[item.mv_output_index].clone()?;
                            let orig = &original_agg.aggregates[i];
                            Some(AggregateCall {
                                name: item.rollup_fn.to_string(),
                                args: vec![column_ref(&mv_col)],
                                distinct: false,
                                result_type: orig.result_type.clone(),
                                order_by: vec![],
                                output_column_id: agg_outputs[n_keys + i].column_id,
                            })
                        })
                        .collect::<Option<Vec<_>>>()?;
                    let agg_op = Operator::LogicalAggregate(LogicalAggregateOp::single(
                        group_by,
                        aggregates,
                        agg_outputs.clone(),
                    ));
                    if !needs_coalesce {
                        return Some(NewExpr {
                            op: agg_op,
                            children: vec![child_group],
                        });
                    }
                    // Scalar COUNT rollup: wrap with COALESCE(sum, 0).
                    let agg_group = memo.new_group(MExpr {
                        id: memo.next_expr_id(),
                        op: agg_op,
                        children: vec![child_group],
                    });
                    let items = original_agg
                        .output_columns
                        .iter()
                        .enumerate()
                        .map(|(i, oc)| {
                            let inner = column_ref(&agg_outputs[i]);
                            let expr = if i >= n_keys && plan.items[i - n_keys].needs_coalesce {
                                TypedExpr {
                                    kind: ExprKind::FunctionCall {
                                        name: "coalesce".to_string(),
                                        args: vec![
                                            inner,
                                            TypedExpr {
                                                kind: ExprKind::Literal(LiteralValue::Int(0)),
                                                data_type: oc.data_type.clone(),
                                                nullable: false,
                                            },
                                        ],
                                        distinct: false,
                                    },
                                    data_type: oc.data_type.clone(),
                                    nullable: false,
                                }
                            } else {
                                inner
                            };
                            ProjectItem {
                                expr,
                                output_name: oc.name.clone(),
                                output_column_id: oc.column_id,
                            }
                        })
                        .collect();
                    Some(NewExpr {
                        op: Operator::LogicalProject(LogicalProjectOp {
                            items,
                            output_qualifier: None,
                        }),
                        children: vec![agg_group],
                    })
                }
            }
        }
    }
}

fn column_ref(c: &OutputColumn) -> TypedExpr {
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

fn combine_and(mut preds: Vec<TypedExpr>) -> TypedExpr {
    let first = preds.remove(0);
    preds.into_iter().fold(first, |l, r| TypedExpr {
        nullable: l.nullable || r.nullable,
        data_type: arrow::datatypes::DataType::Boolean,
        kind: ExprKind::BinaryOp {
            left: Box::new(l),
            op: crate::sql::analysis::BinOp::And,
            right: Box::new(r),
        },
    })
}

/// Identity match on `(catalog, namespace, table)` only. `table_uuid` and the
/// snapshot binding are deliberately ignored at this layer: this rule cannot
/// validate MV freshness. Freshness/uuid safety (so a stale MV or a
/// drop+recreate of the base table cannot match) is the responsibility of the
/// engine-side candidate preparation, which only hands the optimizer
/// candidates whose base snapshots match the MV's refresh pins.
fn same_iceberg_table(
    a: &crate::sql::catalog::TableDef,
    b: &crate::sql::catalog::TableDef,
) -> bool {
    use crate::sql::catalog::ScanSource;
    match (&a.source, &b.source) {
        (
            ScanSource::IcebergDataFiles { table: ta, .. },
            ScanSource::IcebergDataFiles { table: tb, .. },
        ) => ta.catalog == tb.catalog && ta.namespace == tb.namespace && ta.table == tb.table,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{BinOp, ExprKind, LiteralValue, OutputColumn, TypedExpr};
    use crate::sql::catalog::{
        ColumnDef, IcebergDataFileBinding, IcebergSchemaDef, IcebergTableInfo, ScanSource, TableDef,
    };
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::convert::logical_plan_to_memo;
    use crate::sql::optimizer::memo::Memo;
    use crate::sql::planner::plan::{
        AggregateCall, AggregateNode, FilterNode, LogicalPlan, ScanNode,
    };
    use arrow::datatypes::DataType;

    // --- fixture helpers --------------------------------------------------

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

    fn ge(left: TypedExpr, v: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op: BinOp::Ge,
                right: Box::new(int_lit(v)),
            },
            data_type: DataType::Boolean,
            nullable: true,
        }
    }

    fn iceberg_info(catalog: &str, ns: &str, tbl: &str) -> IcebergTableInfo {
        IcebergTableInfo {
            catalog: catalog.to_string(),
            namespace: ns.to_string(),
            table: tbl.to_string(),
            table_uuid: None,
            current_snapshot_id: None,
            schema_id: 0,
            location: String::new(),
            schema: IcebergSchemaDef { fields: vec![] },
            serialized_metadata: None,
            serialized_metadata_rows: None,
        }
    }

    /// A `TableDef` over `ScanSource::IcebergDataFiles` with the given identity
    /// and column names (all Int64). `same_iceberg_table` keys only on the
    /// `(catalog, namespace, table)` triple, so the base table and the MV
    /// target table differ ONLY in the `table` component.
    fn iceberg_table(catalog: &str, ns: &str, tbl: &str, columns: &[&str]) -> TableDef {
        TableDef {
            name: tbl.to_string(),
            columns: columns
                .iter()
                .map(|n| ColumnDef {
                    name: n.to_string(),
                    data_type: DataType::Int64,
                    nullable: true,
                    write_default: None,
                    logical_type: None,
                })
                .collect(),
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::IcebergDataFiles {
                table: iceberg_info(catalog, ns, tbl),
                files: vec![],
                cloud_properties: Default::default(),
                binding: IcebergDataFileBinding::CurrentSnapshot,
            },
        }
    }

    /// `Scan` over the base table identity `cat.ns.t` exposing `columns`.
    fn base_scan(columns: &[OutputColumn]) -> LogicalPlan {
        let names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
        LogicalPlan::Scan(ScanNode {
            database: "ns".to_string(),
            table: iceberg_table("cat", "ns", "t", &names),
            alias: None,
            columns: columns.to_vec(),
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
            variant_columns: vec![],
            required_output_columns: None,
        })
    }

    fn sum_call(arg: &OutputColumn, out: &OutputColumn) -> AggregateCall {
        AggregateCall {
            name: "sum".to_string(),
            args: vec![col_ref(arg)],
            distinct: false,
            result_type: DataType::Int64,
            order_by: vec![],
            output_column_id: out.column_id,
        }
    }

    fn count_star(out: &OutputColumn) -> AggregateCall {
        AggregateCall {
            name: "count".to_string(),
            args: vec![],
            distinct: false,
            result_type: DataType::Int64,
            order_by: vec![],
            output_column_id: out.column_id,
        }
    }

    /// Advance `memo.factory` past id `up_to` so that freshly minted MV-scan
    /// column ids never collide with the test's hardcoded query/MV ids. This
    /// mirrors production, where the factory is shared and already advanced
    /// past every analyzer-minted id by the time MvRewrite runs.
    fn advance_factory(memo: &mut Memo, up_to: u32) {
        while memo
            .factory
            .create(None, "pad".to_string(), DataType::Int64, true)
            .0
            <= up_to
        {}
    }

    /// MV defining plan `SELECT a, b, sum(v) AS s FROM t WHERE a >= mv_low
    /// GROUP BY a, b`, over the SAME base identity but a DISTINCT id range
    /// (100..=110). Returned as a built `SpjgDescriptor` via the already-tested
    /// `from_logical_plan`.
    fn mv_descriptor(mv_low: i64) -> SpjgDescriptor {
        let a = col(100, "a");
        let b = col(101, "b");
        let v = col(102, "v");
        let s = col(110, "s");
        let plan = LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(LogicalPlan::Filter(FilterNode {
                input: Box::new(base_scan(&[a.clone(), b.clone(), v.clone()])),
                predicate: ge(col_ref(&a), mv_low),
                required_output_columns: None,
            })),
            group_by: vec![col_ref(&a), col_ref(&b)],
            aggregates: vec![sum_call(&v, &s)],
            output_columns: vec![a.clone(), b.clone(), s.clone()],
            already_pushed: false,
            required_output_columns: None,
        });
        SpjgDescriptor::from_logical_plan(&plan).expect("mv spjg")
    }

    /// Candidate over MV `agg_mv(a, b, s)` materializing `mv_descriptor`.
    fn agg_candidate(mv_low: i64) -> MvRewriteCandidate {
        MvRewriteCandidate {
            mv_name: "agg_mv".to_string(),
            mv: mv_descriptor(mv_low),
            target_database: "ns".to_string(),
            target_table: iceberg_table("cat", "ns", "agg_mv", &["a", "b", "s"]),
        }
    }

    /// Walk a child-group chain from `gid`, following first logical expr, and
    /// return the first `LogicalScan` op reached (panics if none).
    fn find_scan(memo: &Memo, gid: usize) -> &LogicalScanOp {
        let expr = &memo.groups[gid].logical_exprs[0];
        match &expr.op {
            Operator::LogicalScan(s) => s,
            Operator::LogicalFilter(_)
            | Operator::LogicalProject(_)
            | Operator::LogicalAggregate(_) => find_scan(memo, expr.children[0]),
            other => panic!("unexpected op while walking to scan: {other:?}"),
        }
    }

    /// True if the chain from `gid` contains a `LogicalFilter`.
    fn has_filter(memo: &Memo, gid: usize) -> bool {
        let expr = &memo.groups[gid].logical_exprs[0];
        match &expr.op {
            Operator::LogicalFilter(_) => true,
            Operator::LogicalScan(_) => false,
            _ => has_filter(memo, expr.children[0]),
        }
    }

    // --- tests ------------------------------------------------------------

    #[test]
    fn injects_rollup_alternative() {
        // Query: SELECT a, sum(v) FROM t WHERE a >= 10 GROUP BY a.
        // MV:    SELECT a, b, sum(v) s FROM t WHERE a >= 0 GROUP BY a, b.
        // Query group-by {a} ⊂ MV {a, b}  -> Rollup; a>=10 ⊃ a>=0 compensation.
        let a = col(1, "a");
        let v = col(2, "v");
        let s = col(3, "s"); // original aggregate sum output id
        let query_plan = LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(LogicalPlan::Filter(FilterNode {
                input: Box::new(base_scan(&[a.clone(), v.clone()])),
                predicate: ge(col_ref(&a), 10),
                required_output_columns: None,
            })),
            group_by: vec![col_ref(&a)],
            aggregates: vec![sum_call(&v, &s)],
            output_columns: vec![a.clone(), s.clone()],
            already_pushed: false,
            required_output_columns: None,
        });

        let mut memo = Memo::new();
        let root = logical_plan_to_memo(&query_plan, &mut memo);
        advance_factory(&mut memo, 200);
        let root_expr = memo.groups[root].logical_exprs[0].clone();

        let rule = MvRewriteRule::new(vec![agg_candidate(0)]);
        let alts = rule.apply(&root_expr, &mut memo);
        assert_eq!(alts.len(), 1, "exactly one rollup alternative");

        // Top must be a LogicalAggregate reusing the ORIGINAL output ids.
        let Operator::LogicalAggregate(agg) = &alts[0].op else {
            panic!("expected rollup aggregate, got {:?}", alts[0].op);
        };
        assert_eq!(agg.output_columns[0].column_id, a.column_id);
        assert_eq!(agg.output_columns[1].column_id, s.column_id);
        // The rollup aggregate re-aggregates with SUM over the MV's `s` column.
        assert_eq!(agg.aggregates.len(), 1);
        assert_eq!(agg.aggregates[0].name, "sum");

        // Child chain: a compensation Filter (a >= 10) over Scan(agg_mv).
        let child = alts[0].children[0];
        assert!(has_filter(&memo, child), "compensation filter expected");
        let scan = find_scan(&memo, child);
        assert_eq!(scan.table.name, "agg_mv");
        assert_eq!(scan.mv_rewritten_from.as_deref(), Some("agg_mv"));

        // Idempotency: a second apply on the same expr injects nothing.
        assert!(
            rule.apply(&root_expr, &mut memo).is_empty(),
            "second apply must be a no-op"
        );
    }

    #[test]
    fn no_injection_when_predicate_not_contained() {
        // MV WHERE a >= 100; query WHERE a >= 10 reads rows the MV dropped.
        let a = col(1, "a");
        let v = col(2, "v");
        let s = col(3, "s");
        let query_plan = LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(LogicalPlan::Filter(FilterNode {
                input: Box::new(base_scan(&[a.clone(), v.clone()])),
                predicate: ge(col_ref(&a), 10),
                required_output_columns: None,
            })),
            group_by: vec![col_ref(&a)],
            aggregates: vec![sum_call(&v, &s)],
            output_columns: vec![a.clone(), s.clone()],
            already_pushed: false,
            required_output_columns: None,
        });

        let mut memo = Memo::new();
        let root = logical_plan_to_memo(&query_plan, &mut memo);
        advance_factory(&mut memo, 200);
        let root_expr = memo.groups[root].logical_exprs[0].clone();

        let rule = MvRewriteRule::new(vec![agg_candidate(100)]);
        assert!(
            rule.apply(&root_expr, &mut memo).is_empty(),
            "predicate not contained -> no alternative"
        );
    }

    #[test]
    fn spj_query_on_spj_mv_injects_project() {
        // SPJ MV: SELECT a, b, v FROM t WHERE a >= 0  (no aggregate).
        let mv_a = col(100, "a");
        let mv_b = col(101, "b");
        let mv_v = col(102, "v");
        let mv_plan = LogicalPlan::Filter(FilterNode {
            input: Box::new(base_scan(&[mv_a.clone(), mv_b.clone(), mv_v.clone()])),
            predicate: ge(col_ref(&mv_a), 0),
            required_output_columns: None,
        });
        let mv = SpjgDescriptor::from_logical_plan(&mv_plan).expect("mv spjg");
        let candidate = MvRewriteCandidate {
            mv_name: "spj_mv".to_string(),
            mv,
            target_database: "ns".to_string(),
            target_table: iceberg_table("cat", "ns", "spj_mv", &["a", "b", "v"]),
        };

        // SPJ query: SELECT a, b FROM t WHERE a >= 10. (top = Filter(Scan))
        let a = col(1, "a");
        let b = col(2, "b");
        let v = col(3, "v");
        let query_plan = LogicalPlan::Filter(FilterNode {
            input: Box::new(base_scan(&[a.clone(), b.clone(), v.clone()])),
            predicate: ge(col_ref(&a), 10),
            required_output_columns: None,
        });

        let mut memo = Memo::new();
        let root = logical_plan_to_memo(&query_plan, &mut memo);
        advance_factory(&mut memo, 200);
        let root_expr = memo.groups[root].logical_exprs[0].clone();

        let rule = MvRewriteRule::new(vec![candidate]);
        let alts = rule.apply(&root_expr, &mut memo);
        assert_eq!(alts.len(), 1);

        // Top must be a LogicalProject binding the ORIGINAL scan output ids.
        let Operator::LogicalProject(p) = &alts[0].op else {
            panic!("expected project, got {:?}", alts[0].op);
        };
        let ids: Vec<ColumnId> = p.items.iter().map(|i| i.output_column_id).collect();
        assert_eq!(ids, vec![a.column_id, b.column_id, v.column_id]);

        // Child chain: compensation Filter (a >= 10) over Scan(spj_mv).
        let child = alts[0].children[0];
        assert!(has_filter(&memo, child));
        let scan = find_scan(&memo, child);
        assert_eq!(scan.table.name, "spj_mv");
        assert_eq!(scan.mv_rewritten_from.as_deref(), Some("spj_mv"));
    }

    #[test]
    fn scalar_count_rollup_wraps_with_coalesce() {
        // MV:    SELECT a, count(*) c FROM t WHERE a >= 0 GROUP BY a.
        // Query: SELECT count(*) FROM t WHERE a >= 0  (scalar, no group-by).
        // {} ⊂ {a} -> Rollup; count -> SUM over MV `c`; scalar count over an
        // empty MV result is NULL where COUNT must be 0 -> COALESCE(sum, 0).
        let mv_a = col(100, "a");
        let mv_c = col(110, "c");
        let mv_plan = LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(LogicalPlan::Filter(FilterNode {
                input: Box::new(base_scan(std::slice::from_ref(&mv_a))),
                predicate: ge(col_ref(&mv_a), 0),
                required_output_columns: None,
            })),
            group_by: vec![col_ref(&mv_a)],
            aggregates: vec![count_star(&mv_c)],
            output_columns: vec![mv_a.clone(), mv_c.clone()],
            already_pushed: false,
            required_output_columns: None,
        });
        let mv = SpjgDescriptor::from_logical_plan(&mv_plan).expect("mv spjg");
        let candidate = MvRewriteCandidate {
            mv_name: "cnt_mv".to_string(),
            mv,
            target_database: "ns".to_string(),
            target_table: iceberg_table("cat", "ns", "cnt_mv", &["a", "c"]),
        };

        let a = col(1, "a");
        let cnt = col(3, "cnt"); // original scalar count output id
        let query_plan = LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(LogicalPlan::Filter(FilterNode {
                input: Box::new(base_scan(std::slice::from_ref(&a))),
                predicate: ge(col_ref(&a), 0),
                required_output_columns: None,
            })),
            group_by: vec![],
            aggregates: vec![count_star(&cnt)],
            output_columns: vec![cnt.clone()],
            already_pushed: false,
            required_output_columns: None,
        });

        let mut memo = Memo::new();
        let root = logical_plan_to_memo(&query_plan, &mut memo);
        advance_factory(&mut memo, 200);
        let root_expr = memo.groups[root].logical_exprs[0].clone();

        let rule = MvRewriteRule::new(vec![candidate]);
        let alts = rule.apply(&root_expr, &mut memo);
        assert_eq!(alts.len(), 1);

        // Top must be a LogicalProject whose sole item is COALESCE(_, 0) bound
        // to the ORIGINAL count output id.
        let Operator::LogicalProject(p) = &alts[0].op else {
            panic!("expected coalesce project, got {:?}", alts[0].op);
        };
        assert_eq!(p.items.len(), 1);
        assert_eq!(p.items[0].output_column_id, cnt.column_id);
        let ExprKind::FunctionCall { name, args, .. } = &p.items[0].expr.kind else {
            panic!("expected coalesce call, got {:?}", p.items[0].expr.kind);
        };
        assert_eq!(name, "coalesce");
        assert_eq!(args.len(), 2);
        // arg0 references the inner aggregate output (a freshly-minted id, NOT
        // the original cnt id — the original id is reused only at the project).
        let ExprKind::ColumnRef { column_id, .. } = &args[0].kind else {
            panic!("coalesce arg0 must be a column ref to the inner sum");
        };
        assert_ne!(*column_id, cnt.column_id);
        // arg1 is the literal 0.
        assert!(matches!(
            &args[1].kind,
            ExprKind::Literal(LiteralValue::Int(0))
        ));

        // The child group is the inner rollup aggregate: SUM over MV `c`.
        let agg_group = alts[0].children[0];
        let Operator::LogicalAggregate(inner) = &memo.groups[agg_group].logical_exprs[0].op else {
            panic!("expected inner rollup aggregate");
        };
        assert!(inner.group_by.is_empty());
        assert_eq!(inner.aggregates.len(), 1);
        assert_eq!(inner.aggregates[0].name, "sum");
        // The inner aggregate's output id is the freshly-minted one used by the
        // coalesce arg, confirming the original id is not duplicated mid-tree.
        assert_eq!(inner.output_columns[0].column_id, *column_id);

        // Scan(cnt_mv) at the bottom (no compensation: predicates identical).
        let scan = find_scan(&memo, agg_group);
        assert_eq!(scan.table.name, "cnt_mv");
        assert_eq!(scan.mv_rewritten_from.as_deref(), Some("cnt_mv"));
    }
}
