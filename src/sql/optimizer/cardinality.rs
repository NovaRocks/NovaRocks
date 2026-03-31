//! Cardinality estimation for the cost-based optimizer.
//!
//! Walks the [`LogicalPlan`] bottom-up and propagates [`Statistics`] through
//! each operator.  Selectivity estimation follows StarRocks-aligned heuristics.

use std::collections::HashMap;

use crate::sql::ir::*;
use crate::sql::plan::*;
use crate::sql::statistics::*;

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
        LogicalPlan::SubqueryAlias(s) => estimate_statistics(&s.input, table_stats),
        LogicalPlan::Repeat(r) => {
            let input = estimate_statistics(&r.input, table_stats);
            let repeat_times = r.repeat_column_ref_list.len() as f64;
            Statistics {
                output_row_count: input.output_row_count * repeat_times,
                column_statistics: input.column_statistics,
            }
        }
        LogicalPlan::Values(v) => Statistics {
            output_row_count: v.rows.len() as f64,
            column_statistics: HashMap::new(),
        },
        LogicalPlan::GenerateSeries(g) => {
            let rows = if g.step != 0 {
                ((g.end - g.start) / g.step + 1).max(0) as f64
            } else {
                1.0
            };
            Statistics {
                output_row_count: rows,
                column_statistics: HashMap::new(),
            }
        }
        LogicalPlan::CTEConsume(_) => Statistics {
            output_row_count: 1000.0,
            column_statistics: HashMap::new(),
        }
    }
}

fn estimate_scan(scan: &ScanNode, table_stats: &HashMap<String, TableStatistics>) -> Statistics {
    let key = scan
        .alias
        .as_deref()
        .unwrap_or(&scan.table.name)
        .to_lowercase();

    if let Some(ts) = table_stats.get(&key) {
        let row_count = ts.row_count.max(1) as f64;

        // Apply scan-level predicate selectivity.
        let mut selectivity = 1.0;
        for pred in &scan.predicates {
            selectivity *= estimate_selectivity(pred, &ts.column_stats);
        }

        let output_rows = (row_count * selectivity).max(1.0);

        let column_statistics: HashMap<String, ColumnStatistic> = scan
            .columns
            .iter()
            .map(|c| {
                let col_name = c.name.to_lowercase();
                let cs = ts
                    .column_stats
                    .get(&col_name)
                    .cloned()
                    .unwrap_or_else(ColumnStatistic::unknown);
                (col_name, cs)
            })
            .collect();

        Statistics {
            output_row_count: output_rows,
            column_statistics,
        }
    } else {
        // No table stats available: use defaults.
        let column_statistics: HashMap<String, ColumnStatistic> = scan
            .columns
            .iter()
            .map(|c| (c.name.to_lowercase(), ColumnStatistic::unknown()))
            .collect();
        Statistics {
            output_row_count: 10_000.0,
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
    let output_rows = (input_stats.output_row_count * selectivity).max(1.0);
    Statistics {
        output_row_count: output_rows,
        column_statistics: input_stats.column_statistics,
    }
}

fn estimate_project(
    project: &ProjectNode,
    table_stats: &HashMap<String, TableStatistics>,
) -> Statistics {
    let input_stats = estimate_statistics(&project.input, table_stats);
    // Filter column_statistics to only projected columns.
    let projected: HashMap<String, ColumnStatistic> = project
        .items
        .iter()
        .filter_map(|item| {
            let name = item.output_name.to_lowercase();
            input_stats
                .column_statistics
                .get(&name)
                .cloned()
                .map(|cs| (name, cs))
        })
        .collect();
    Statistics {
        output_row_count: input_stats.output_row_count,
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
            column_statistics: HashMap::new(),
        };
    }

    // Product of NDVs of group-by keys, capped by correlation factor.
    let mut ndv_product = 1.0f64;
    for gb_expr in &agg.group_by {
        let ndv = get_expr_ndv(gb_expr, &input_stats.column_statistics);
        ndv_product *= ndv;
    }

    let capped = input_stats.output_row_count * UNKNOWN_GROUP_BY_CORRELATION;
    let output_rows = ndv_product.min(capped).max(1.0);

    Statistics {
        output_row_count: output_rows,
        column_statistics: HashMap::new(),
    }
}

fn estimate_join(join: &JoinNode, table_stats: &HashMap<String, TableStatistics>) -> Statistics {
    let left_stats = estimate_statistics(&join.left, table_stats);
    let right_stats = estimate_statistics(&join.right, table_stats);

    let left_rows = left_stats.output_row_count.max(1.0);
    let right_rows = right_stats.output_row_count.max(1.0);

    let output_rows = match join.join_type {
        JoinKind::Cross => left_rows * right_rows,
        JoinKind::Inner => {
            if let Some(ref cond) = join.condition {
                let key_ndv = get_join_key_ndv(
                    cond,
                    &left_stats.column_statistics,
                    &right_stats.column_statistics,
                );
                (left_rows * right_rows / key_ndv).max(1.0)
            } else {
                left_rows * right_rows
            }
        }
        JoinKind::LeftOuter => {
            if let Some(ref cond) = join.condition {
                let key_ndv = get_join_key_ndv(
                    cond,
                    &left_stats.column_statistics,
                    &right_stats.column_statistics,
                );
                let inner = left_rows * right_rows / key_ndv;
                inner.max(left_rows)
            } else {
                left_rows * right_rows
            }
        }
        JoinKind::RightOuter => {
            if let Some(ref cond) = join.condition {
                let key_ndv = get_join_key_ndv(
                    cond,
                    &left_stats.column_statistics,
                    &right_stats.column_statistics,
                );
                let inner = left_rows * right_rows / key_ndv;
                inner.max(right_rows)
            } else {
                left_rows * right_rows
            }
        }
        JoinKind::FullOuter => {
            if let Some(ref cond) = join.condition {
                let key_ndv = get_join_key_ndv(
                    cond,
                    &left_stats.column_statistics,
                    &right_stats.column_statistics,
                );
                let inner = left_rows * right_rows / key_ndv;
                inner.max(left_rows).max(right_rows)
            } else {
                left_rows * right_rows
            }
        }
        JoinKind::LeftSemi => {
            // At most left_rows, apply selectivity.
            if let Some(ref cond) = join.condition {
                let sel = estimate_selectivity(cond, &left_stats.column_statistics);
                (left_rows * sel).max(1.0)
            } else {
                left_rows
            }
        }
        JoinKind::RightSemi => {
            if let Some(ref cond) = join.condition {
                let sel = estimate_selectivity(cond, &right_stats.column_statistics);
                (right_rows * sel).max(1.0)
            } else {
                right_rows
            }
        }
        JoinKind::LeftAnti => (left_rows * ANTI_JOIN_SELECTIVITY).max(1.0),
        JoinKind::RightAnti => (right_rows * ANTI_JOIN_SELECTIVITY).max(1.0),
    };

    // Merge column statistics from both sides.
    let mut column_statistics = left_stats.column_statistics;
    column_statistics.extend(right_stats.column_statistics);

    Statistics {
        output_row_count: output_rows,
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
        column_statistics: input_stats.column_statistics,
    }
}

fn estimate_union(union: &UnionNode, table_stats: &HashMap<String, TableStatistics>) -> Statistics {
    let mut total_rows = 0.0;
    let mut column_statistics = HashMap::new();
    for input in &union.inputs {
        let s = estimate_statistics(input, table_stats);
        total_rows += s.output_row_count;
        if column_statistics.is_empty() {
            column_statistics = s.column_statistics;
        }
    }
    if !union.all {
        total_rows *= UNKNOWN_GROUP_BY_CORRELATION;
    }
    Statistics {
        output_row_count: total_rows.max(1.0),
        column_statistics,
    }
}

fn estimate_intersect(
    intersect: &IntersectNode,
    table_stats: &HashMap<String, TableStatistics>,
) -> Statistics {
    let mut min_rows = f64::MAX;
    let mut column_statistics = HashMap::new();
    for input in &intersect.inputs {
        let s = estimate_statistics(input, table_stats);
        if s.output_row_count < min_rows {
            min_rows = s.output_row_count;
            column_statistics = s.column_statistics;
        }
    }
    Statistics {
        output_row_count: (min_rows * 0.5).max(1.0),
        column_statistics,
    }
}

fn estimate_except(
    except: &ExceptNode,
    table_stats: &HashMap<String, TableStatistics>,
) -> Statistics {
    if let Some(first) = except.inputs.first() {
        let s = estimate_statistics(first, table_stats);
        Statistics {
            output_row_count: (s.output_row_count * 0.5).max(1.0),
            column_statistics: s.column_statistics,
        }
    } else {
        Statistics {
            output_row_count: 1.0,
            column_statistics: HashMap::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Selectivity estimation
// ---------------------------------------------------------------------------

/// Estimate selectivity of a predicate expression (0.0..1.0).
pub(crate) fn estimate_selectivity(
    expr: &TypedExpr,
    column_stats: &HashMap<String, ColumnStatistic>,
) -> f64 {
    match &expr.kind {
        ExprKind::BinaryOp { left, op, right } => match op {
            BinOp::And => {
                let l = estimate_selectivity(left, column_stats);
                let r = estimate_selectivity(right, column_stats);
                l * r
            }
            BinOp::Or => {
                let l = estimate_selectivity(left, column_stats);
                let r = estimate_selectivity(right, column_stats);
                l + r - l * r
            }
            BinOp::Eq | BinOp::EqForNull => estimate_eq_selectivity(left, right, column_stats),
            BinOp::Ne => 1.0 - estimate_eq_selectivity(left, right, column_stats),
            BinOp::Lt | BinOp::Le | BinOp::Gt | BinOp::Ge => {
                estimate_range_selectivity(left, right, *op, column_stats)
            }
            _ => PREDICATE_UNKNOWN_FILTER,
        },
        ExprKind::IsNull { negated, expr } => {
            let col_name = extract_column_name(expr);
            let null_frac = col_name
                .and_then(|name| column_stats.get(&name.to_lowercase()))
                .map(|cs| {
                    if cs.nulls_fraction > 0.0 {
                        cs.nulls_fraction
                    } else {
                        IS_NULL_FILTER
                    }
                })
                .unwrap_or(IS_NULL_FILTER);
            if *negated { 1.0 - null_frac } else { null_frac }
        }
        ExprKind::InList {
            expr,
            list,
            negated,
        } => {
            let col_name = extract_column_name(expr);
            let ndv = col_name
                .and_then(|name| column_stats.get(&name.to_lowercase()))
                .map(|cs| cs.distinct_values_count.max(1.0))
                .unwrap_or(0.0);

            let sel = if ndv > 0.0 {
                (list.len() as f64 / ndv).min(1.0)
            } else {
                IN_PREDICATE_DEFAULT_FILTER
            };
            if *negated { 1.0 - sel } else { sel }
        }
        ExprKind::Between { negated, .. } => {
            let sel = PREDICATE_UNKNOWN_FILTER; // conservative
            if *negated { 1.0 - sel } else { sel }
        }
        ExprKind::Like { negated, .. } => {
            let sel = PREDICATE_UNKNOWN_FILTER;
            if *negated { 1.0 - sel } else { sel }
        }
        ExprKind::UnaryOp {
            op: UnOp::Not,
            expr,
        } => 1.0 - estimate_selectivity(expr, column_stats),
        ExprKind::IsTruthValue { value, negated, .. } => {
            // IS TRUE / IS NOT TRUE / IS FALSE / IS NOT FALSE
            let base = if *value { 0.5 } else { 0.5 };
            if *negated { 1.0 - base } else { base }
        }
        ExprKind::Nested(inner) => estimate_selectivity(inner, column_stats),
        _ => PREDICATE_UNKNOWN_FILTER,
    }
}

fn estimate_eq_selectivity(
    left: &TypedExpr,
    right: &TypedExpr,
    column_stats: &HashMap<String, ColumnStatistic>,
) -> f64 {
    // col = literal: use 1/ndv
    let col_name = extract_column_name(left).or_else(|| extract_column_name(right));

    if let Some(name) = col_name {
        if let Some(cs) = column_stats.get(&name.to_lowercase()) {
            if cs.distinct_values_count > 1.0 {
                return 1.0 / cs.distinct_values_count;
            }
        }
    }
    PREDICATE_UNKNOWN_FILTER
}

fn estimate_range_selectivity(
    left: &TypedExpr,
    right: &TypedExpr,
    op: BinOp,
    column_stats: &HashMap<String, ColumnStatistic>,
) -> f64 {
    // Try to use min/max range if available.
    let col_name = extract_column_name(left);
    let literal_val = extract_literal_f64(right);

    if let (Some(name), Some(val)) = (col_name, literal_val) {
        if let Some(cs) = column_stats.get(&name.to_lowercase()) {
            let min = cs.min_value;
            let max = cs.max_value;
            if min.is_finite() && max.is_finite() && max > min {
                let range = max - min;
                return match op {
                    BinOp::Lt => ((val - min) / range).clamp(0.01, 0.99),
                    BinOp::Le => ((val - min + 1.0) / range).clamp(0.01, 0.99),
                    BinOp::Gt => ((max - val) / range).clamp(0.01, 0.99),
                    BinOp::Ge => ((max - val + 1.0) / range).clamp(0.01, 0.99),
                    _ => 0.5,
                };
            }
        }
    }
    0.5 // default for range predicates
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

/// Extract column name from a simple column reference expression.
fn extract_column_name(expr: &TypedExpr) -> Option<&str> {
    match &expr.kind {
        ExprKind::ColumnRef { column, .. } => Some(column.as_str()),
        ExprKind::Cast { expr, .. } => extract_column_name(expr),
        ExprKind::Nested(inner) => extract_column_name(inner),
        _ => None,
    }
}

/// Try to extract a numeric literal value as f64.
fn extract_literal_f64(expr: &TypedExpr) -> Option<f64> {
    match &expr.kind {
        ExprKind::Literal(LiteralValue::Int(v)) => Some(*v as f64),
        ExprKind::Literal(LiteralValue::Float(v)) => Some(*v),
        ExprKind::Literal(LiteralValue::Decimal(s)) => s.parse::<f64>().ok(),
        ExprKind::Cast { expr, .. } => extract_literal_f64(expr),
        ExprKind::Nested(inner) => extract_literal_f64(inner),
        _ => None,
    }
}

/// Get the NDV (number of distinct values) for an expression, looking up
/// column stats if the expression is a simple column reference.
fn get_expr_ndv(expr: &TypedExpr, column_stats: &HashMap<String, ColumnStatistic>) -> f64 {
    if let Some(name) = extract_column_name(expr) {
        if let Some(cs) = column_stats.get(&name.to_lowercase()) {
            return cs.distinct_values_count.max(1.0);
        }
    }
    // Default NDV for unknown expressions.
    10.0
}

/// For a join condition, extract the max NDV of join keys from both sides.
fn get_join_key_ndv(
    condition: &TypedExpr,
    left_stats: &HashMap<String, ColumnStatistic>,
    right_stats: &HashMap<String, ColumnStatistic>,
) -> f64 {
    // For a simple `left_col = right_col`, take max(ndv(left), ndv(right)).
    match &condition.kind {
        ExprKind::BinaryOp {
            left,
            op: BinOp::Eq | BinOp::EqForNull,
            right,
        } => {
            let left_ndv = get_expr_ndv(left, left_stats).max(get_expr_ndv(left, right_stats));
            let right_ndv = get_expr_ndv(right, left_stats).max(get_expr_ndv(right, right_stats));
            left_ndv.max(right_ndv).max(1.0)
        }
        ExprKind::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            // AND of multiple join keys: take the max NDV across all.
            let l = get_join_key_ndv(left, left_stats, right_stats);
            let r = get_join_key_ndv(right, left_stats, right_stats);
            l.max(r)
        }
        _ => 1.0, // Conservative default.
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::catalog::{ColumnDef, S3FileInfo, TableDef, TableStorage};
    use crate::sql::ir::{BinOp, ExprKind, JoinKind, LiteralValue, OutputColumn};
    use arrow::datatypes::DataType;

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

    fn scan_plan(name: &str, cols: &[&str]) -> LogicalPlan {
        let columns: Vec<OutputColumn> = cols
            .iter()
            .map(|c| OutputColumn {
                name: c.to_string(),
                data_type: DataType::Int32,
                nullable: false,
            })
            .collect();
        let col_defs: Vec<ColumnDef> = cols
            .iter()
            .map(|c| ColumnDef {
                name: c.to_string(),
                data_type: DataType::Int32,
                nullable: false,
            })
            .collect();
        LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table: TableDef {
                name: name.to_string(),
                columns: col_defs,
                storage: TableStorage::S3ParquetFiles {
                    files: vec![S3FileInfo {
                        path: format!("s3://bucket/{}.parquet", name),
                        size: 1000,
                        row_count: Some(1000),
                        column_stats: None,
                    }],
                    cloud_properties: Default::default(),
                },
            },
            alias: None,
            columns,
            predicates: vec![],
            required_columns: None,
        })
    }

    fn col_ref(name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
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
        assert!(stats.column_statistics.contains_key("id"));
        assert!(stats.column_statistics.contains_key("status"));
    }

    #[test]
    fn scan_without_stats_uses_default() {
        let plan = scan_plan("unknown", &["x"]);
        let stats = estimate_statistics(&plan, &HashMap::new());
        assert!((stats.output_row_count - 10_000.0).abs() < 1.0);
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
        });

        let stats = estimate_statistics(&plan, &table_stats);
        // sel = 1/100, rows = 10000/100 = 100
        assert!((stats.output_row_count - 100.0).abs() < 1.0);
    }

    #[test]
    fn and_selectivity_multiplies() {
        let col_stats: HashMap<String, ColumnStatistic> = [
            (
                "a".to_string(),
                ColumnStatistic {
                    min_value: 0.0,
                    max_value: 100.0,
                    nulls_fraction: 0.0,
                    average_row_size: 4.0,
                    distinct_values_count: 100.0,
                },
            ),
            (
                "b".to_string(),
                ColumnStatistic {
                    min_value: 0.0,
                    max_value: 50.0,
                    nulls_fraction: 0.0,
                    average_row_size: 4.0,
                    distinct_values_count: 50.0,
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
        // 1/100 * 1/50 = 0.0002
        assert!((sel - 0.0002).abs() < 0.0001);
    }

    #[test]
    fn or_selectivity() {
        let col_stats: HashMap<String, ColumnStatistic> = [(
            "a".to_string(),
            ColumnStatistic {
                min_value: 0.0,
                max_value: 100.0,
                nulls_fraction: 0.0,
                average_row_size: 4.0,
                distinct_values_count: 4.0,
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
        let col_stats: HashMap<String, ColumnStatistic> = [(
            "x".to_string(),
            ColumnStatistic {
                min_value: 0.0,
                max_value: 100.0,
                nulls_fraction: 0.05,
                average_row_size: 4.0,
                distinct_values_count: 100.0,
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
        });

        let stats = estimate_statistics(&plan, &table_stats);
        // 6M * 1.5M / max(1.5M, 1.5M) = 6M
        assert!((stats.output_row_count - 6_000_000.0).abs() < 1.0);
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
                name: "status".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            }],
        });

        let stats = estimate_statistics(&plan, &table_stats);
        // NDV of status = 5, capped at 100000*0.75=75000 => min(5, 75000) = 5
        assert!((stats.output_row_count - 5.0).abs() < 1.0);
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
        });

        let stats = estimate_statistics(&plan, &table_stats);
        // 1000 * 0.4 = 400
        assert!((stats.output_row_count - 400.0).abs() < 1.0);
    }
}
