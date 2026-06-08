//! Cardinality estimation for the cost-based optimizer.
//!
//! Walks the [`LogicalPlan`] bottom-up and propagates [`Statistics`] through
//! each operator.  Selectivity estimation follows StarRocks-aligned heuristics.

use std::collections::HashMap;

use crate::sql::optimizer::estimate::cardinality::{
    JoinCardInput, estimate_join_cardinality, except_rows, intersect_rows, union_all_rows,
    union_distinct_rows,
};
use crate::sql::optimizer::estimate::join_condition::estimate_join_condition;
use crate::sql::optimizer::estimate::ndv::{agg_group_rows, cap_ndv_at_rows, get_expr_ndv};
use crate::sql::optimizer::estimate::selectivity::apply_filter;
use crate::sql::optimizer::statistics::*;
use crate::sql::optimizer::stats::estimate_selectivity;
use crate::sql::optimizer::stats::extract_column_id;
use crate::sql::planner::plan::*;

/// Estimate output statistics for a logical plan node recursively.
pub(crate) fn estimate_statistics(
    plan: &LogicalPlan,
    table_stats: &HashMap<String, TableStatistics>,
) -> Statistics {
    match plan {
        LogicalPlan::Scan(s) => estimate_scan(s, table_stats),
        LogicalPlan::Filter(f) => estimate_filter(f, table_stats),
        LogicalPlan::Project(p) => estimate_project(p, table_stats),
        LogicalPlan::Aggregate(a) => estimate_aggregate(a, table_stats),
        LogicalPlan::Join(j) => estimate_join(j, table_stats),
        LogicalPlan::Sort(s) => {
            // Sort preserves row count.
            estimate_statistics(&s.input, table_stats)
        }
        LogicalPlan::Limit(l) => estimate_limit(l, table_stats),
        LogicalPlan::Window(w) => {
            // Window preserves row count.
            estimate_statistics(&w.input, table_stats)
        }
        LogicalPlan::Union(u) => estimate_union(u, table_stats),
        LogicalPlan::Intersect(i) => estimate_intersect(i, table_stats),
        LogicalPlan::Except(e) => estimate_except(e, table_stats),
        LogicalPlan::Repeat(r) => {
            let input = estimate_statistics(&r.input, table_stats);
            let repeat_times = r.repeat_column_ref_list.len() as f64;
            Statistics {
                output_row_count: input.output_row_count * repeat_times,
                row_count_confidence: Confidence::Estimated,
                column_statistics: input.column_statistics,
            }
        }
        LogicalPlan::CTEAnchor(node) => estimate_statistics(&node.consumer, table_stats),
        LogicalPlan::CTEProduce(node) => estimate_statistics(&node.input, table_stats),
        LogicalPlan::Values(v) => Statistics {
            output_row_count: v.rows.len() as f64,
            row_count_confidence: Confidence::Exact,
            column_statistics: HashMap::new(),
        },
        LogicalPlan::GenerateSeries(g) => Statistics {
            output_row_count: generate_series_row_count_f64(g.start, g.end, g.step),
            row_count_confidence: Confidence::Exact,
            column_statistics: HashMap::new(),
        },
        LogicalPlan::TableFunction(t) => {
            let child = estimate_statistics(&t.input, table_stats);
            let estimated_rows = child.output_row_count * 3.0;
            Statistics {
                output_row_count: if t.is_left_join {
                    estimated_rows.max(child.output_row_count)
                } else {
                    estimated_rows.max(1.0)
                },
                row_count_confidence: Confidence::Estimated,
                column_statistics: HashMap::new(),
            }
        }
        LogicalPlan::CTEConsume(_) => Statistics {
            output_row_count: 1000.0,
            row_count_confidence: Confidence::Fallback,
            column_statistics: HashMap::new(),
        },
        LogicalPlan::Decode(d) => estimate_statistics(&d.input, table_stats),
        LogicalPlan::AggregateStateMerge(n) => {
            let old_stats = estimate_statistics(&n.old_input, table_stats);
            let delta_stats = estimate_statistics(&n.delta_input, table_stats);
            Statistics {
                output_row_count: (old_stats.output_row_count + delta_stats.output_row_count)
                    .max(1.0),
                row_count_confidence: Confidence::Estimated,
                column_statistics: HashMap::new(),
            }
        }
        LogicalPlan::ImvDelta(_) | LogicalPlan::ImvVersion(_) => {
            panic!("imv marker leaked into non-IMV plan");
        }
    }
}

fn estimate_scan(scan: &ScanNode, table_stats: &HashMap<String, TableStatistics>) -> Statistics {
    let alias_key = scan.alias.as_deref().map(|a| a.to_lowercase());
    let table_key = scan.table.name.to_lowercase();
    let ts_opt = alias_key
        .as_deref()
        .and_then(|k| table_stats.get(k))
        .or_else(|| table_stats.get(&table_key));

    if let Some(ts) = ts_opt {
        let row_count = ts.row_count.max(1) as f64;
        let table_column_statistics = map_table_column_stats_to_ids(&scan.columns, ts);

        let mut output_rows = row_count;
        let mut row_count_confidence = Confidence::Exact;
        for pred in &scan.predicates {
            let selectivity = estimate_selectivity(pred, &table_column_statistics);
            (output_rows, row_count_confidence) =
                apply_filter(output_rows, row_count_confidence, selectivity);
        }

        let mut column_statistics: HashMap<crate::sql::column_id::ColumnId, ColumnStatistic> = scan
            .columns
            .iter()
            .map(|c| {
                let col_name = c.name.to_lowercase();
                let cs = ts
                    .column_stats
                    .get(&col_name)
                    .cloned()
                    .unwrap_or_else(ColumnStatistic::unknown);
                (c.column_id, cs)
            })
            .collect();
        cap_column_ndvs(&mut column_statistics, output_rows);

        Statistics {
            output_row_count: output_rows,
            row_count_confidence,
            column_statistics,
        }
    } else {
        // No table stats available: use defaults.
        let mut column_statistics: HashMap<crate::sql::column_id::ColumnId, ColumnStatistic> = scan
            .columns
            .iter()
            .map(|c| (c.column_id, ColumnStatistic::unknown()))
            .collect();
        let mut output_rows = 10_000.0;
        let mut row_count_confidence = Confidence::Fallback;
        for pred in &scan.predicates {
            let selectivity = estimate_selectivity(pred, &column_statistics);
            (output_rows, row_count_confidence) =
                apply_filter(output_rows, row_count_confidence, selectivity);
        }
        cap_column_ndvs(&mut column_statistics, output_rows);
        Statistics {
            output_row_count: output_rows,
            row_count_confidence,
            column_statistics,
        }
    }
}

fn estimate_filter(
    filter: &FilterNode,
    table_stats: &HashMap<String, TableStatistics>,
) -> Statistics {
    let input_stats = estimate_statistics(&filter.input, table_stats);
    let selectivity = estimate_selectivity(&filter.predicate, &input_stats.column_statistics);
    let (output_rows, row_count_confidence) = apply_filter(
        input_stats.output_row_count,
        input_stats.row_count_confidence,
        selectivity,
    );
    let mut column_statistics = input_stats.column_statistics;
    cap_column_ndvs(&mut column_statistics, output_rows);
    Statistics {
        output_row_count: output_rows,
        row_count_confidence,
        column_statistics,
    }
}

fn estimate_project(
    project: &ProjectNode,
    table_stats: &HashMap<String, TableStatistics>,
) -> Statistics {
    let input_stats = estimate_statistics(&project.input, table_stats);
    // Filter column_statistics to only projected columns.
    let projected: HashMap<crate::sql::column_id::ColumnId, ColumnStatistic> = project
        .items
        .iter()
        .filter_map(|item| {
            extract_column_id(&item.expr)
                .and_then(|column_id| input_stats.column_statistics.get(&column_id))
                .cloned()
                .map(|cs| (item.output_column_id, cs))
        })
        .collect();
    Statistics {
        output_row_count: input_stats.output_row_count,
        row_count_confidence: Confidence::Estimated,
        column_statistics: projected,
    }
}

fn estimate_aggregate(
    agg: &AggregateNode,
    table_stats: &HashMap<String, TableStatistics>,
) -> Statistics {
    let input_stats = estimate_statistics(&agg.input, table_stats);

    if agg.group_by.is_empty() {
        // Scalar aggregation: exactly one output row.
        return Statistics {
            output_row_count: 1.0,
            row_count_confidence: Confidence::Estimated,
            column_statistics: HashMap::new(),
        };
    }

    let group_key_ndvs: Vec<f64> = agg
        .group_by
        .iter()
        .map(|gb_expr| get_expr_ndv(gb_expr, &input_stats.column_statistics))
        .collect();
    let output_rows = agg_group_rows(&group_key_ndvs, input_stats.output_row_count);

    Statistics {
        output_row_count: output_rows,
        row_count_confidence: Confidence::derive(&[input_stats.row_count_confidence], false),
        column_statistics: HashMap::new(),
    }
}

fn estimate_join(join: &JoinNode, table_stats: &HashMap<String, TableStatistics>) -> Statistics {
    let left_stats = estimate_statistics(&join.left, table_stats);
    let right_stats = estimate_statistics(&join.right, table_stats);

    let join_condition = estimate_join_condition(
        join.condition.as_ref(),
        &left_stats.column_statistics,
        &right_stats.column_statistics,
    );
    let (output_rows, row_count_confidence) = estimate_join_cardinality(&JoinCardInput {
        left: (left_stats.output_row_count, left_stats.row_count_confidence),
        right: (
            right_stats.output_row_count,
            right_stats.row_count_confidence,
        ),
        kind: join.join_type,
        eq_key_ndvs: join_condition.eq_key_ndvs,
        non_equi_selectivity: join_condition.residual_selectivity,
    });

    // Merge column statistics from both sides.
    let mut column_statistics = left_stats.column_statistics;
    column_statistics.extend(right_stats.column_statistics);

    Statistics {
        output_row_count: output_rows,
        row_count_confidence,
        column_statistics,
    }
}

fn estimate_limit(limit: &LimitNode, table_stats: &HashMap<String, TableStatistics>) -> Statistics {
    let input_stats = estimate_statistics(&limit.input, table_stats);
    let output_rows = if let Some(lim) = limit.limit {
        (lim as f64).min(input_stats.output_row_count)
    } else {
        input_stats.output_row_count
    };
    Statistics {
        output_row_count: output_rows.max(0.0),
        row_count_confidence: Confidence::Estimated,
        column_statistics: input_stats.column_statistics,
    }
}

fn estimate_union(union: &UnionNode, table_stats: &HashMap<String, TableStatistics>) -> Statistics {
    let input_stats: Vec<_> = union
        .inputs
        .iter()
        .map(|input| estimate_statistics(input, table_stats))
        .collect();
    let input_rows: Vec<_> = input_stats.iter().map(|s| s.output_row_count).collect();
    let (formula_rows, saturated_or_defaulted) = if union.all {
        union_all_rows(&input_rows)
    } else {
        union_distinct_rows(&input_rows)
    };
    let (output_row_count, defaulted_output_rows) = positive_set_op_output_rows(formula_rows);
    let row_confidences: Vec<_> = input_stats.iter().map(|s| s.row_count_confidence).collect();
    let column_statistics = input_stats
        .first()
        .map(|s| s.column_statistics.clone())
        .unwrap_or_default();

    Statistics {
        output_row_count,
        row_count_confidence: Confidence::derive(
            &row_confidences,
            saturated_or_defaulted || defaulted_output_rows,
        ),
        column_statistics,
    }
}

fn estimate_intersect(
    intersect: &IntersectNode,
    table_stats: &HashMap<String, TableStatistics>,
) -> Statistics {
    let input_stats: Vec<_> = intersect
        .inputs
        .iter()
        .map(|input| estimate_statistics(input, table_stats))
        .collect();
    let input_rows: Vec<_> = input_stats.iter().map(|s| s.output_row_count).collect();
    let (formula_rows, saturated_or_defaulted) = intersect_rows(&input_rows);
    let (output_row_count, defaulted_output_rows) = positive_set_op_output_rows(formula_rows);
    let row_confidences: Vec<_> = input_stats.iter().map(|s| s.row_count_confidence).collect();
    let column_statistics = input_stats
        .iter()
        .min_by(|a, b| {
            a.output_row_count
                .partial_cmp(&b.output_row_count)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .map(|s| s.column_statistics.clone())
        .unwrap_or_default();

    Statistics {
        output_row_count,
        row_count_confidence: Confidence::derive(
            &row_confidences,
            saturated_or_defaulted || defaulted_output_rows,
        ),
        column_statistics,
    }
}

fn estimate_except(
    except: &ExceptNode,
    table_stats: &HashMap<String, TableStatistics>,
) -> Statistics {
    let input_stats: Vec<_> = except
        .inputs
        .iter()
        .map(|input| estimate_statistics(input, table_stats))
        .collect();
    let input_rows: Vec<_> = input_stats.iter().map(|s| s.output_row_count).collect();
    let (formula_rows, saturated_or_defaulted) = except_rows(&input_rows);
    let (output_row_count, defaulted_output_rows) = positive_set_op_output_rows(formula_rows);
    let row_confidences: Vec<_> = input_stats.iter().map(|s| s.row_count_confidence).collect();
    let column_statistics = input_stats
        .first()
        .map(|s| s.column_statistics.clone())
        .unwrap_or_default();

    Statistics {
        output_row_count,
        row_count_confidence: Confidence::derive(
            &row_confidences,
            saturated_or_defaulted || defaulted_output_rows,
        ),
        column_statistics,
    }
}

fn positive_set_op_output_rows(rows: f64) -> (f64, bool) {
    if !rows.is_finite() || rows < 1.0 {
        (1.0, true)
    } else {
        (rows, false)
    }
}

fn cap_column_ndvs(
    column_statistics: &mut HashMap<crate::sql::column_id::ColumnId, ColumnStatistic>,
    output_rows: f64,
) {
    for stat in column_statistics.values_mut() {
        stat.distinct_values_count = cap_ndv_at_rows(stat.distinct_values_count, output_rows);
    }
}

fn map_table_column_stats_to_ids(
    columns: &[crate::sql::analysis::OutputColumn],
    table_stats: &TableStatistics,
) -> HashMap<crate::sql::column_id::ColumnId, ColumnStatistic> {
    columns
        .iter()
        .map(|column| {
            let stat = table_stats
                .column_stats
                .get(&column.name.to_lowercase())
                .cloned()
                .unwrap_or_else(ColumnStatistic::unknown);
            (column.column_id, stat)
        })
        .collect()
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{BinOp, ExprKind, JoinKind, LiteralValue, OutputColumn, TypedExpr};
    use crate::sql::catalog::{
        ColumnDef, IcebergDataFileInfo, IcebergSchemaDef, IcebergTableInfo, ScanSource, TableDef,
    };
    use crate::sql::column_id::ColumnId;
    use arrow::datatypes::DataType;

    fn test_iceberg_table_info() -> IcebergTableInfo {
        IcebergTableInfo {
            catalog: "test_catalog".to_string(),
            namespace: "test_db".to_string(),
            table: "test_table".to_string(),
            table_uuid: Some("00000000-0000-0000-0000-000000000001".to_string()),
            current_snapshot_id: Some(7),
            schema_id: 1,
            location: "file:///tmp/test_table".to_string(),
            schema: IcebergSchemaDef { fields: vec![] },
            serialized_metadata: None,
            serialized_metadata_rows: None,
        }
    }

    fn make_table_stats(
        name: &str,
        row_count: u64,
        columns: &[(&str, f64)],
    ) -> (String, TableStatistics) {
        let mut cs = HashMap::new();
        for &(col_name, ndv) in columns {
            cs.insert(
                col_name.to_string(),
                ColumnStatistic {
                    min_value: 0.0,
                    max_value: row_count as f64,
                    nulls_fraction: 0.01,
                    average_row_size: 8.0,
                    distinct_values_count: ndv,
                    confidence: Confidence::Exact,
                },
            );
        }
        (
            name.to_string(),
            TableStatistics {
                row_count,
                column_stats: cs,
            },
        )
    }

    fn test_col_id(name: &str) -> ColumnId {
        let id = match name {
            "id" | "a" | "x" | "l_orderkey" => 1,
            "status" | "b" | "o_orderkey" => 2,
            "amount" => 3,
            other => {
                let mut hash = 2_166_136_261u32;
                for byte in other.bytes() {
                    hash ^= byte as u32;
                    hash = hash.wrapping_mul(16_777_619);
                }
                1_000 + (hash % 100_000)
            }
        };
        ColumnId::new_for_test(id)
    }

    fn stat_by_name<'a>(
        stats: &'a HashMap<ColumnId, ColumnStatistic>,
        name: &str,
    ) -> &'a ColumnStatistic {
        stats.get(&test_col_id(name)).unwrap()
    }

    fn scan_plan(name: &str, cols: &[&str]) -> LogicalPlan {
        let columns: Vec<OutputColumn> = cols
            .iter()
            .map(|c| OutputColumn {
                column_id: test_col_id(c),
                name: c.to_string(),
                data_type: DataType::Int32,
                nullable: false,
                is_internal: false,
            })
            .collect();
        let col_defs: Vec<ColumnDef> = cols
            .iter()
            .map(|c| ColumnDef {
                name: c.to_string(),
                data_type: DataType::Int32,
                nullable: false,
                write_default: None,
                logical_type: None,
            })
            .collect();
        LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table: TableDef {
                name: name.to_string(),
                columns: col_defs,
                iceberg_row_lineage_metadata_columns: vec![],
                source: ScanSource::IcebergDataFiles {
                    table: test_iceberg_table_info(),
                    files: vec![IcebergDataFileInfo {
                        path: format!("s3://bucket/{}.parquet", name),
                        size: 1000,
                        row_count: Some(1000),
                        column_stats: None,
                        partition_spec_id: None,
                        partition_key: None,
                        first_row_id: None,
                        data_sequence_number: None,
                        ivm_change_op: None,
                        delete_files: vec![],
                        manifest_path: None,
                        partition_values: vec![],
                    }],
                    cloud_properties: Default::default(),
                    binding: crate::sql::catalog::IcebergDataFileBinding::CurrentSnapshot,
                },
            },
            alias: None,
            columns,
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
            required_output_columns: None,
        })
    }

    fn scan_plan_with_alias(name: &str, alias: &str, cols: &[&str]) -> LogicalPlan {
        let LogicalPlan::Scan(mut node) = scan_plan(name, cols) else {
            unreachable!("scan_plan always returns a Scan");
        };
        node.alias = Some(alias.to_string());
        LogicalPlan::Scan(node)
    }

    fn scan_plan_with_predicates(
        name: &str,
        cols: &[&str],
        predicates: Vec<TypedExpr>,
    ) -> LogicalPlan {
        let LogicalPlan::Scan(mut node) = scan_plan(name, cols) else {
            unreachable!("scan_plan always returns a Scan");
        };
        node.predicates = predicates;
        LogicalPlan::Scan(node)
    }

    fn col_ref(name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: test_col_id(name),
                qualifier: None,
                column: name.to_string(),
            },
            data_type: DataType::Int32,
            nullable: false,
        }
    }

    fn int_lit(v: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(v)),
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn eq_expr(left: TypedExpr, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Boolean,
            nullable: false,
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op: BinOp::Eq,
                right: Box::new(right),
            },
        }
    }

    fn and_expr(left: TypedExpr, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Boolean,
            nullable: false,
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op: BinOp::And,
                right: Box::new(right),
            },
        }
    }

    fn or_expr(left: TypedExpr, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Boolean,
            nullable: false,
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op: BinOp::Or,
                right: Box::new(right),
            },
        }
    }

    #[test]
    fn scan_uses_table_stats() {
        let (name, ts) = make_table_stats("orders", 100_000, &[("id", 100_000.0), ("status", 5.0)]);
        let mut table_stats = HashMap::new();
        table_stats.insert(name, ts);

        let plan = scan_plan("orders", &["id", "status"]);
        let stats = estimate_statistics(&plan, &table_stats);

        assert!((stats.output_row_count - 100_000.0).abs() < 1.0);
        assert!(stats.column_statistics.contains_key(&test_col_id("id")));
        assert!(stats.column_statistics.contains_key(&test_col_id("status")));
        assert_eq!(stats.row_count_confidence, Confidence::Exact);
        assert_eq!(
            stat_by_name(&stats.column_statistics, "id").confidence,
            Confidence::Exact
        );
    }

    #[test]
    fn aliased_scan_falls_back_to_table_name_stats() {
        let (name, ts) = make_table_stats("orders", 100_000, &[("id", 100_000.0)]);
        let mut table_stats = HashMap::new();
        table_stats.insert(name, ts);

        let plan = scan_plan_with_alias("orders", "o", &["id"]);
        let stats = estimate_statistics(&plan, &table_stats);

        assert!((stats.output_row_count - 100_000.0).abs() < 1.0);
        assert_eq!(stats.row_count_confidence, Confidence::Exact);
    }

    #[test]
    fn scan_with_table_stats_and_predicate_downgrades_row_confidence() {
        let (name, ts) = make_table_stats("orders", 100_000, &[("id", 100.0)]);
        let mut table_stats = HashMap::new();
        table_stats.insert(name, ts);

        let plan =
            scan_plan_with_predicates("orders", &["id"], vec![eq_expr(col_ref("id"), int_lit(42))]);
        let stats = estimate_statistics(&plan, &table_stats);

        assert!((stats.output_row_count - 1_000.0).abs() < 1.0);
        assert_eq!(stats.row_count_confidence, Confidence::Estimated);
    }

    #[test]
    fn scan_with_table_stats_and_predicate_caps_ndv_at_output_rows() {
        let (name, ts) = make_table_stats("orders", 1_000, &[("id", 1_000_000.0)]);
        let mut table_stats = HashMap::new();
        table_stats.insert(name, ts);

        let plan =
            scan_plan_with_predicates("orders", &["id"], vec![eq_expr(col_ref("id"), int_lit(42))]);
        let stats = estimate_statistics(&plan, &table_stats);

        assert_eq!(stats.output_row_count, 1.0);
        assert_eq!(stats.row_count_confidence, Confidence::Fallback);
        assert_eq!(
            stat_by_name(&stats.column_statistics, "id").distinct_values_count,
            1.0
        );
    }

    #[test]
    fn scan_without_stats_uses_default() {
        let plan = scan_plan("unknown", &["x"]);
        let stats = estimate_statistics(&plan, &HashMap::new());
        assert!((stats.output_row_count - 10_000.0).abs() < 1.0);
        assert_eq!(stats.row_count_confidence, Confidence::Fallback);
        assert_eq!(
            stat_by_name(&stats.column_statistics, "x").confidence,
            Confidence::Fallback
        );
    }

    #[test]
    fn scan_without_stats_applies_predicate_selectivity_as_fallback() {
        let plan =
            scan_plan_with_predicates("unknown", &["x"], vec![eq_expr(col_ref("x"), int_lit(42))]);
        let stats = estimate_statistics(&plan, &HashMap::new());

        assert!((stats.output_row_count - 2_500.0).abs() < 1.0);
        assert_eq!(stats.row_count_confidence, Confidence::Fallback);
    }

    #[test]
    fn filter_reduces_rows() {
        let (name, ts) = make_table_stats("t", 10_000, &[("a", 100.0)]);
        let mut table_stats = HashMap::new();
        table_stats.insert(name, ts);

        let scan = scan_plan("t", &["a"]);
        let pred = eq_expr(col_ref("a"), int_lit(42));
        let plan = LogicalPlan::Filter(FilterNode {
            input: Box::new(scan),
            predicate: pred,
            required_output_columns: None,
        });

        let stats = estimate_statistics(&plan, &table_stats);
        // sel = 1/100, rows = 10000/100 = 100
        assert!((stats.output_row_count - 100.0).abs() < 1.0);
    }

    #[test]
    fn and_selectivity_uses_damped_conjunction() {
        let col_stats: HashMap<ColumnId, ColumnStatistic> = [
            (
                test_col_id("a"),
                ColumnStatistic {
                    min_value: 0.0,
                    max_value: 100.0,
                    nulls_fraction: 0.0,
                    average_row_size: 4.0,
                    distinct_values_count: 100.0,
                    confidence: Confidence::Exact,
                    ..Default::default()
                },
            ),
            (
                test_col_id("b"),
                ColumnStatistic {
                    min_value: 0.0,
                    max_value: 50.0,
                    nulls_fraction: 0.0,
                    average_row_size: 4.0,
                    distinct_values_count: 50.0,
                    confidence: Confidence::Exact,
                    ..Default::default()
                },
            ),
        ]
        .into_iter()
        .collect();

        let pred = and_expr(
            eq_expr(col_ref("a"), int_lit(1)),
            eq_expr(col_ref("b"), int_lit(2)),
        );
        let sel = estimate_selectivity(&pred, &col_stats);
        // Damped conjunction sorts 0.01 and 0.02 ascending:
        // 0.01 * sqrt(0.02) ~= 0.001414213562.
        let expected = 0.01_f64 * 0.02_f64.sqrt();
        assert!(
            (sel - expected).abs() < 1e-12,
            "expected damped selectivity {expected}, got {sel}"
        );
    }

    #[test]
    fn or_selectivity() {
        let col_stats: HashMap<ColumnId, ColumnStatistic> = [(
            test_col_id("a"),
            ColumnStatistic {
                min_value: 0.0,
                max_value: 100.0,
                nulls_fraction: 0.0,
                average_row_size: 4.0,
                distinct_values_count: 4.0,
                ..Default::default()
            },
        )]
        .into_iter()
        .collect();

        let pred = or_expr(
            eq_expr(col_ref("a"), int_lit(1)),
            eq_expr(col_ref("a"), int_lit(2)),
        );
        let sel = estimate_selectivity(&pred, &col_stats);
        // 0.25 + 0.25 - 0.25*0.25 = 0.4375
        assert!((sel - 0.4375).abs() < 0.001);
    }

    #[test]
    fn is_null_selectivity() {
        let col_stats: HashMap<ColumnId, ColumnStatistic> = [(
            test_col_id("x"),
            ColumnStatistic {
                min_value: 0.0,
                max_value: 100.0,
                nulls_fraction: 0.05,
                average_row_size: 4.0,
                distinct_values_count: 100.0,
                ..Default::default()
            },
        )]
        .into_iter()
        .collect();

        let expr = TypedExpr {
            kind: ExprKind::IsNull {
                expr: Box::new(col_ref("x")),
                negated: false,
            },
            data_type: DataType::Boolean,
            nullable: false,
        };
        let sel = estimate_selectivity(&expr, &col_stats);
        assert!((sel - 0.05).abs() < 0.001);
    }

    #[test]
    fn inner_join_cardinality() {
        let (ln, lt) = make_table_stats("lineitem", 6_000_000, &[("l_orderkey", 1_500_000.0)]);
        let (on, ot) = make_table_stats("orders", 1_500_000, &[("o_orderkey", 1_500_000.0)]);
        let mut table_stats = HashMap::new();
        table_stats.insert(ln, lt);
        table_stats.insert(on, ot);

        let left = scan_plan("lineitem", &["l_orderkey"]);
        let right = scan_plan("orders", &["o_orderkey"]);
        let cond = eq_expr(col_ref("l_orderkey"), col_ref("o_orderkey"));

        let plan = LogicalPlan::Join(JoinNode {
            left: Box::new(left),
            right: Box::new(right),
            join_type: JoinKind::Inner,
            condition: Some(cond),
            required_output_columns: None,
        });

        let stats = estimate_statistics(&plan, &table_stats);
        // Containment formula: 6M * 1.5M / max(1.5M, 1.5M) = 6M.
        // The equality predicate must not be applied again as residual
        // selectivity.
        assert!((stats.output_row_count - 6_000_000.0).abs() < 1.0);
    }

    #[test]
    fn filter_floor_downgrades_confidence_and_caps_payload_ndv() {
        let (name, ts) = make_table_stats("t", 1_000, &[("a", 1_000.0), ("b", 1_000.0)]);
        let mut table_stats = HashMap::new();
        table_stats.insert(name, ts);

        let scan = scan_plan("t", &["a", "b"]);
        let pred = and_expr(
            eq_expr(col_ref("a"), int_lit(42)),
            eq_expr(col_ref("b"), int_lit(7)),
        );
        let plan = LogicalPlan::Filter(FilterNode {
            input: Box::new(scan),
            predicate: pred,
            required_output_columns: None,
        });

        let stats = estimate_statistics(&plan, &table_stats);
        assert_eq!(stats.output_row_count, 1.0);
        assert_eq!(stats.row_count_confidence, Confidence::Fallback);
        assert_eq!(
            stat_by_name(&stats.column_statistics, "a").distinct_values_count,
            1.0
        );
        assert_eq!(
            stat_by_name(&stats.column_statistics, "b").distinct_values_count,
            1.0
        );
    }

    #[test]
    fn aggregate_reduces_rows() {
        let (name, ts) = make_table_stats("t", 100_000, &[("status", 5.0), ("amount", 50_000.0)]);
        let mut table_stats = HashMap::new();
        table_stats.insert(name, ts);

        let scan = scan_plan("t", &["status", "amount"]);
        let plan = LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(scan),
            group_by: vec![col_ref("status")],
            aggregates: vec![],
            output_columns: vec![OutputColumn {
                column_id: ColumnId::UNSET,
                name: "status".to_string(),
                data_type: DataType::Int32,
                nullable: false,
                is_internal: false,
            }],
            already_pushed: false,
            required_output_columns: None,
        });

        let stats = estimate_statistics(&plan, &table_stats);
        // NDV of status = 5, capped at 100000*0.75=75000 => min(5, 75000) = 5
        assert!((stats.output_row_count - 5.0).abs() < 1.0);
    }

    #[test]
    fn aggregate_multi_key_rows_use_damped_ndv_product() {
        let (name, ts) = make_table_stats(
            "t",
            1_000_000,
            &[("k1", 100.0), ("k2", 100.0), ("k3", 100.0)],
        );
        let mut table_stats = HashMap::new();
        table_stats.insert(name, ts);

        let scan = scan_plan("t", &["k1", "k2", "k3"]);
        let plan = LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(scan),
            group_by: vec![col_ref("k1"), col_ref("k2"), col_ref("k3")],
            aggregates: vec![],
            output_columns: vec![
                OutputColumn {
                    column_id: ColumnId::UNSET,
                    name: "k1".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                    is_internal: false,
                },
                OutputColumn {
                    column_id: ColumnId::UNSET,
                    name: "k2".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                    is_internal: false,
                },
                OutputColumn {
                    column_id: ColumnId::UNSET,
                    name: "k3".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                    is_internal: false,
                },
            ],
            already_pushed: false,
            required_output_columns: None,
        });

        let stats = estimate_statistics(&plan, &table_stats);
        let expected = 100.0 * 100.0_f64.sqrt() * 100.0_f64.powf(0.25);
        assert!(
            (stats.output_row_count - expected).abs() < 0.000_001,
            "expected damped aggregate rows {expected}, got {}",
            stats.output_row_count
        );
        assert!(stats.output_row_count < 100.0 * 100.0 * 100.0);
        assert_eq!(stats.row_count_confidence, Confidence::Estimated);
    }

    #[test]
    fn aggregate_row_count_confidence_follows_fallback_child() {
        let scan = scan_plan("missing_stats", &["k1", "k2", "k3"]);
        let plan = LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(scan),
            group_by: vec![col_ref("k1"), col_ref("k2"), col_ref("k3")],
            aggregates: vec![],
            output_columns: vec![],
            already_pushed: false,
            required_output_columns: None,
        });

        let stats = estimate_statistics(&plan, &HashMap::new());
        assert_eq!(stats.row_count_confidence, Confidence::Fallback);
    }

    #[test]
    fn limit_caps_rows() {
        let (name, ts) = make_table_stats("t", 100_000, &[("a", 100.0)]);
        let mut table_stats = HashMap::new();
        table_stats.insert(name, ts);

        let scan = scan_plan("t", &["a"]);
        let plan = LogicalPlan::Limit(LimitNode {
            input: Box::new(scan),
            limit: Some(10),
            offset: None,
            required_output_columns: None,
        });

        let stats = estimate_statistics(&plan, &table_stats);
        assert!((stats.output_row_count - 10.0).abs() < 0.01);
    }

    #[test]
    fn cross_join_cardinality() {
        let (ln, lt) = make_table_stats("a", 100, &[]);
        let (rn, rt) = make_table_stats("b", 200, &[]);
        let mut table_stats = HashMap::new();
        table_stats.insert(ln, lt);
        table_stats.insert(rn, rt);

        let left = scan_plan("a", &["x"]);
        let right = scan_plan("b", &["y"]);
        let plan = LogicalPlan::Join(JoinNode {
            left: Box::new(left),
            right: Box::new(right),
            join_type: JoinKind::Cross,
            condition: None,
            required_output_columns: None,
        });

        let stats = estimate_statistics(&plan, &table_stats);
        assert!((stats.output_row_count - 20_000.0).abs() < 1.0);
    }

    #[test]
    fn left_anti_join_selectivity() {
        let (ln, lt) = make_table_stats("a", 1000, &[("id", 1000.0)]);
        let (rn, rt) = make_table_stats("b", 500, &[("id", 500.0)]);
        let mut table_stats = HashMap::new();
        table_stats.insert(ln, lt);
        table_stats.insert(rn, rt);

        let left = scan_plan("a", &["id"]);
        let right = scan_plan("b", &["id"]);
        let plan = LogicalPlan::Join(JoinNode {
            left: Box::new(left),
            right: Box::new(right),
            join_type: JoinKind::LeftAnti,
            condition: Some(eq_expr(col_ref("id"), col_ref("id"))),
            required_output_columns: None,
        });

        let stats = estimate_statistics(&plan, &table_stats);
        // 1000 * 0.4 = 400
        assert!((stats.output_row_count - 400.0).abs() < 1.0);
    }

    #[test]
    fn union_all_cardinality_saturates_rows_and_degrades_confidence() {
        let (ln, lt) = make_table_stats("a", 900_000_000_000_000, &[("k", 100.0)]);
        let (rn, rt) = make_table_stats("b", 900_000_000_000_000, &[("k", 100.0)]);
        let mut table_stats = HashMap::new();
        table_stats.insert(ln, lt);
        table_stats.insert(rn, rt);

        let plan = LogicalPlan::Union(UnionNode {
            inputs: vec![scan_plan("a", &["k"]), scan_plan("b", &["k"])],
            all: true,
            output_columns: vec![OutputColumn {
                column_id: ColumnId::UNSET,
                name: "k".to_string(),
                data_type: DataType::Int32,
                nullable: false,
                is_internal: false,
            }],
            required_output_columns: None,
        });

        let stats = estimate_statistics(&plan, &table_stats);

        assert_eq!(
            stats.output_row_count,
            crate::sql::optimizer::estimate::arith::MAX_ROW_COUNT
        );
        assert_eq!(stats.row_count_confidence, Confidence::Fallback);
    }
}
