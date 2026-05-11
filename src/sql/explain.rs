//! EXPLAIN plan formatter — produces text from LogicalPlan or PhysicalPlan.

use std::collections::HashSet;
use std::fmt::Write;
use std::fs::File;

use arrow::array::{Array, BinaryArray, LargeBinaryArray, LargeStringArray, StringArray};
use arrow::datatypes::DataType;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::sql::analysis::{BinOp, ExprKind, JoinKind, LiteralValue, TypedExpr, UnOp};
use crate::sql::catalog::TableStorage;
use crate::sql::optimizer::operator::{AggMode, JoinDistribution, Operator};
use crate::sql::optimizer::physical_plan::PhysicalPlanNode;
use crate::sql::optimizer::property::DistributionSpec;
use crate::sql::planner::plan::LogicalPlan;

/// Detail level for EXPLAIN output.
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ExplainLevel {
    Normal,
    Verbose,
    Costs,
}

/// Format a single LogicalPlan tree as EXPLAIN text lines.
#[allow(dead_code)]
pub(crate) fn explain_plan(plan: &LogicalPlan, level: ExplainLevel) -> Vec<String> {
    let mut out = Vec::new();
    format_node(plan, level, 0, &mut out);
    out
}

#[allow(dead_code)]
fn format_node(plan: &LogicalPlan, level: ExplainLevel, indent: usize, out: &mut Vec<String>) {
    let pad = "  ".repeat(indent);
    match plan {
        LogicalPlan::Scan(node) => {
            let alias = node
                .alias
                .as_deref()
                .map(|a| format!(" (alias={a})"))
                .unwrap_or_default();
            out.push(format!(
                "{pad}0:SCAN {db}.{tbl}{alias}",
                db = node.database,
                tbl = node.table.name
            ));
            if let Some(ref cols) = node.required_columns
                && matches!(level, ExplainLevel::Verbose | ExplainLevel::Costs)
            {
                out.push(format!("{pad}     columns: {}", cols.join(", ")));
            }
            if !node.predicates.is_empty() {
                let preds: Vec<String> = node.predicates.iter().map(format_expr).collect();
                out.push(format!("{pad}     predicates: {}", preds.join(" AND ")));
            }
        }
        LogicalPlan::Filter(node) => {
            out.push(format!("{pad}FILTER"));
            out.push(format!(
                "{pad}  predicate: {}",
                format_expr(&node.predicate)
            ));
            format_node(&node.input, level, indent + 1, out);
        }
        LogicalPlan::Project(node) => {
            let items: Vec<String> = node
                .items
                .iter()
                .map(|item| {
                    let expr_str = format_expr(&item.expr);
                    if item.output_name != expr_str {
                        format!("{expr_str} AS {}", item.output_name)
                    } else {
                        expr_str
                    }
                })
                .collect();
            out.push(format!("{pad}PROJECT [{}]", items.join(", ")));
            format_node(&node.input, level, indent + 1, out);
        }
        LogicalPlan::Aggregate(node) => {
            let groups: Vec<String> = node.group_by.iter().map(format_expr).collect();
            let aggs: Vec<String> = node
                .aggregates
                .iter()
                .map(|a| {
                    let args: Vec<String> = a.args.iter().map(format_expr).collect();
                    let distinct = if a.distinct { "DISTINCT " } else { "" };
                    format!("{}({}{})", a.name, distinct, args.join(", "))
                })
                .collect();
            out.push(format!("{pad}AGGREGATE"));
            if !groups.is_empty() {
                out.push(format!("{pad}  group by: {}", groups.join(", ")));
            }
            if !aggs.is_empty() {
                out.push(format!("{pad}  aggregations: {}", aggs.join(", ")));
            }
            format_node(&node.input, level, indent + 1, out);
        }
        LogicalPlan::Join(node) => {
            let join_str = match node.join_type {
                JoinKind::Inner => "INNER JOIN",
                JoinKind::LeftOuter => "LEFT OUTER JOIN",
                JoinKind::RightOuter => "RIGHT OUTER JOIN",
                JoinKind::FullOuter => "FULL OUTER JOIN",
                JoinKind::Cross => "CROSS JOIN",
                JoinKind::LeftSemi => "LEFT SEMI JOIN",
                JoinKind::RightSemi => "RIGHT SEMI JOIN",
                JoinKind::LeftAnti => "LEFT ANTI JOIN",
                JoinKind::RightAnti => "RIGHT ANTI JOIN",
            };
            out.push(format!("{pad}{join_str}"));
            if let Some(ref cond) = node.condition {
                out.push(format!("{pad}  on: {}", format_expr(cond)));
            }
            format_node(&node.left, level, indent + 1, out);
            format_node(&node.right, level, indent + 1, out);
        }
        LogicalPlan::Sort(node) => {
            let items: Vec<String> = node
                .items
                .iter()
                .map(|s| {
                    let dir = if s.asc { "ASC" } else { "DESC" };
                    let nulls = if s.nulls_first {
                        " NULLS FIRST"
                    } else {
                        " NULLS LAST"
                    };
                    format!("{} {dir}{nulls}", format_expr(&s.expr))
                })
                .collect();
            out.push(format!("{pad}SORT BY [{}]", items.join(", ")));
            format_node(&node.input, level, indent + 1, out);
        }
        LogicalPlan::Limit(node) => {
            let mut parts = Vec::new();
            if let Some(limit) = node.limit {
                parts.push(format!("limit={limit}"));
            }
            if let Some(offset) = node.offset {
                parts.push(format!("offset={offset}"));
            }
            out.push(format!("{pad}LIMIT [{}]", parts.join(", ")));
            format_node(&node.input, level, indent + 1, out);
        }
        LogicalPlan::Union(node) => {
            let kind = if node.all { "UNION ALL" } else { "UNION" };
            out.push(format!("{pad}{kind}"));
            for input in &node.inputs {
                format_node(input, level, indent + 1, out);
            }
        }
        LogicalPlan::Intersect(node) => {
            out.push(format!("{pad}INTERSECT"));
            for input in &node.inputs {
                format_node(input, level, indent + 1, out);
            }
        }
        LogicalPlan::Except(node) => {
            out.push(format!("{pad}EXCEPT"));
            for input in &node.inputs {
                format_node(input, level, indent + 1, out);
            }
        }
        LogicalPlan::Window(node) => {
            let fns: Vec<String> = node
                .window_exprs
                .iter()
                .map(|w| {
                    let args: Vec<String> = w.args.iter().map(format_expr).collect();
                    let partition: Vec<String> = w.partition_by.iter().map(format_expr).collect();
                    let order: Vec<String> = w
                        .order_by
                        .iter()
                        .map(|s| {
                            let dir = if s.asc { "ASC" } else { "DESC" };
                            format!("{} {dir}", format_expr(&s.expr))
                        })
                        .collect();
                    let mut over_parts = Vec::new();
                    if !partition.is_empty() {
                        over_parts.push(format!("PARTITION BY {}", partition.join(", ")));
                    }
                    if !order.is_empty() {
                        over_parts.push(format!("ORDER BY {}", order.join(", ")));
                    }
                    format!(
                        "{}({}) OVER ({})",
                        w.name,
                        args.join(", "),
                        over_parts.join(" ")
                    )
                })
                .collect();
            out.push(format!("{pad}WINDOW [{}]", fns.join("; ")));
            format_node(&node.input, level, indent + 1, out);
        }
        LogicalPlan::Values(node) => {
            out.push(format!("{pad}VALUES ({} rows)", node.rows.len()));
        }
        LogicalPlan::GenerateSeries(node) => {
            out.push(format!(
                "{pad}GENERATE_SERIES({}, {}, {})",
                node.start, node.end, node.step
            ));
        }
        LogicalPlan::SubqueryAlias(node) => {
            out.push(format!("{pad}SUBQUERY ALIAS [{}]", node.alias));
            format_node(&node.input, level, indent + 1, out);
        }
        LogicalPlan::Repeat(node) => {
            out.push(format!(
                "{pad}REPEAT ({} grouping sets)",
                node.grouping_ids.len()
            ));
            format_node(&node.input, level, indent + 1, out);
        }
        LogicalPlan::CTEAnchor(node) => {
            out.push(format!("{pad}CTE_ANCHOR(cte_id={})", node.cte_id));
            format_node(&node.produce, level, indent + 1, out);
            format_node(&node.consumer, level, indent + 1, out);
        }
        LogicalPlan::CTEProduce(node) => {
            out.push(format!("{pad}CTE_PRODUCE(cte_id={})", node.cte_id));
            format_node(&node.input, level, indent + 1, out);
        }
        LogicalPlan::CTEConsume(node) => {
            out.push(format!("{pad}CTE_CONSUME(cte_id={})", node.cte_id));
        }
    }
}

// ---------------------------------------------------------------------------
// Physical plan formatting
// ---------------------------------------------------------------------------

/// Format a PhysicalPlanNode tree as EXPLAIN text lines.
pub(crate) fn explain_physical_plan(plan: &PhysicalPlanNode, level: ExplainLevel) -> Vec<String> {
    let mut out = Vec::new();
    format_physical_node(plan, level, 0, &mut out);
    out
}

fn format_physical_node(
    node: &PhysicalPlanNode,
    level: ExplainLevel,
    indent: usize,
    out: &mut Vec<String>,
) {
    let pad = "  ".repeat(indent);
    let costs_suffix = if matches!(level, ExplainLevel::Costs) {
        format!(" (rows={:.0})", node.stats.output_row_count)
    } else {
        String::new()
    };

    match &node.op {
        Operator::PhysicalScan(op) => {
            let alias = op
                .alias
                .as_deref()
                .map(|a| format!(" (alias={a})"))
                .unwrap_or_default();
            out.push(format!(
                "{pad}SCAN {}.{}{alias}{costs_suffix}",
                op.database, op.table.name
            ));
            if let Some(ref cols) = op.required_columns
                && matches!(level, ExplainLevel::Verbose | ExplainLevel::Costs)
            {
                out.push(format!("{pad}     columns: {}", cols.join(", ")));
            }
            let local_hints = explain_hints_for_scan(op);
            if matches!(level, ExplainLevel::Costs) && local_hints.has_decode {
                out.push(format!("{pad}     Decode"));
            }
            if matches!(level, ExplainLevel::Verbose) && local_hints.has_min_max_stats {
                out.push(format!("{pad}     min-max stats"));
            }
            if !op.predicates.is_empty() {
                let preds: Vec<String> = op.predicates.iter().map(format_expr).collect();
                out.push(format!("{pad}     predicates: {}", preds.join(" AND ")));
            }
        }
        Operator::PhysicalFilter(op) => {
            out.push(format!("{pad}FILTER{costs_suffix}"));
            out.push(format!("{pad}  predicate: {}", format_expr(&op.predicate)));
            for child in &node.children {
                format_physical_node(child, level, indent + 1, out);
            }
        }
        Operator::PhysicalProject(op) => {
            let items: Vec<String> = op
                .items
                .iter()
                .map(|item| {
                    let expr_str = format_expr(&item.expr);
                    if item.output_name != expr_str {
                        format!("{expr_str} AS {}", item.output_name)
                    } else {
                        expr_str
                    }
                })
                .collect();
            out.push(format!("{pad}PROJECT [{}]{costs_suffix}", items.join(", ")));
            for child in &node.children {
                format_physical_node(child, level, indent + 1, out);
            }
        }
        Operator::PhysicalHashJoin(op) => {
            let dist = match op.distribution {
                JoinDistribution::Shuffle => "SHUFFLE",
                JoinDistribution::Broadcast => "BROADCAST",
                JoinDistribution::Colocate => "COLOCATE",
            };
            let join_str = match op.join_type {
                JoinKind::Inner => "INNER",
                JoinKind::LeftOuter => "LEFT OUTER",
                JoinKind::RightOuter => "RIGHT OUTER",
                JoinKind::FullOuter => "FULL OUTER",
                JoinKind::Cross => "CROSS",
                JoinKind::LeftSemi => "LEFT SEMI",
                JoinKind::RightSemi => "RIGHT SEMI",
                JoinKind::LeftAnti => "LEFT ANTI",
                JoinKind::RightAnti => "RIGHT ANTI",
            };
            let eq: Vec<String> = op
                .eq_conditions
                .iter()
                .map(|eq| {
                    format!(
                        "{} {} {}",
                        format_expr(&eq.left),
                        if eq.null_safe { "<=>" } else { "=" },
                        format_expr(&eq.right)
                    )
                })
                .collect();
            out.push(format!(
                "{pad}HASH JOIN ({dist}, {join_str}, eq: [{}]){costs_suffix}",
                eq.join(", ")
            ));
            if let Some(ref other) = op.other_condition {
                out.push(format!("{pad}  other: {}", format_expr(other)));
            }
            for child in &node.children {
                format_physical_node(child, level, indent + 1, out);
            }
        }
        Operator::PhysicalNestLoopJoin(op) => {
            let join_str = match op.join_type {
                JoinKind::Inner => "INNER",
                JoinKind::LeftOuter => "LEFT OUTER",
                JoinKind::RightOuter => "RIGHT OUTER",
                JoinKind::FullOuter => "FULL OUTER",
                JoinKind::Cross => "CROSS",
                JoinKind::LeftSemi => "LEFT SEMI",
                JoinKind::RightSemi => "RIGHT SEMI",
                JoinKind::LeftAnti => "LEFT ANTI",
                JoinKind::RightAnti => "RIGHT ANTI",
            };
            out.push(format!("{pad}NEST LOOP JOIN ({join_str}){costs_suffix}"));
            if let Some(ref cond) = op.condition {
                out.push(format!("{pad}  on: {}", format_expr(cond)));
            }
            for child in &node.children {
                format_physical_node(child, level, indent + 1, out);
            }
        }
        Operator::PhysicalHashAggregate(op) => {
            let mode = match op.mode {
                AggMode::Single => "SINGLE",
                AggMode::Local => "LOCAL",
                AggMode::Global => "GLOBAL",
                AggMode::DistinctGlobal => "DISTINCT_GLOBAL",
                AggMode::DistinctLocal => "DISTINCT_LOCAL",
            };
            let groups: Vec<String> = op.group_by.iter().map(format_expr).collect();
            let aggs: Vec<String> = op
                .aggregates
                .iter()
                .map(|a| {
                    let args: Vec<String> = a.args.iter().map(format_expr).collect();
                    let distinct = if a.distinct { "DISTINCT " } else { "" };
                    format!("{}({}{})", a.name, distinct, args.join(", "))
                })
                .collect();
            let mut detail = format!("{pad}HASH AGGREGATE ({mode}");
            if !groups.is_empty() {
                let _ = write!(detail, ", group by: [{}]", groups.join(", "));
            }
            let _ = write!(detail, "){costs_suffix}");
            out.push(detail);
            if !aggs.is_empty() {
                out.push(format!("{pad}  aggregations: {}", aggs.join(", ")));
            }
            for child in &node.children {
                format_physical_node(child, level, indent + 1, out);
            }
        }
        Operator::PhysicalSort(op) => {
            let items: Vec<String> = op
                .items
                .iter()
                .map(|s| {
                    let dir = if s.asc { "ASC" } else { "DESC" };
                    let nulls = if s.nulls_first {
                        " NULLS FIRST"
                    } else {
                        " NULLS LAST"
                    };
                    format!("{} {dir}{nulls}", format_expr(&s.expr))
                })
                .collect();
            out.push(format!("{pad}SORT BY [{}]{costs_suffix}", items.join(", ")));
            for child in &node.children {
                format_physical_node(child, level, indent + 1, out);
            }
        }
        Operator::PhysicalTopN(op) => {
            let items: Vec<String> = op
                .items
                .iter()
                .map(|s| {
                    let dir = if s.asc { "ASC" } else { "DESC" };
                    let nulls = if s.nulls_first {
                        " NULLS FIRST"
                    } else {
                        " NULLS LAST"
                    };
                    format!("{} {dir}{nulls}", format_expr(&s.expr))
                })
                .collect();
            let mut parts = Vec::new();
            if let Some(l) = op.limit {
                parts.push(format!("limit={l}"));
            }
            if let Some(o) = op.offset {
                parts.push(format!("offset={o}"));
            }
            out.push(format!(
                "{pad}TOP-N ({}) [{}]{costs_suffix}",
                parts.join(", "),
                items.join(", ")
            ));
            for child in &node.children {
                format_physical_node(child, level, indent + 1, out);
            }
        }
        Operator::PhysicalLimit(op) => {
            let mut parts = Vec::new();
            if let Some(limit) = op.limit {
                parts.push(format!("limit={limit}"));
            }
            if let Some(offset) = op.offset {
                parts.push(format!("offset={offset}"));
            }
            out.push(format!("{pad}LIMIT [{}]{costs_suffix}", parts.join(", ")));
            for child in &node.children {
                format_physical_node(child, level, indent + 1, out);
            }
        }
        Operator::PhysicalDistribution(op) => {
            let label = match &op.spec {
                DistributionSpec::Any => "ANY EXCHANGE".to_string(),
                DistributionSpec::Gather => "GATHER EXCHANGE".to_string(),
                DistributionSpec::HashPartitioned(cols) => {
                    let col_names: Vec<String> = cols
                        .iter()
                        .map(|c| match &c.qualifier {
                            Some(q) => format!("{q}.{}", c.column),
                            None => c.column.clone(),
                        })
                        .collect();
                    format!("HASH EXCHANGE (hash: [{}])", col_names.join(", "))
                }
            };
            out.push(format!("{pad}{label}{costs_suffix}"));
            for child in &node.children {
                format_physical_node(child, level, indent + 1, out);
            }
        }
        Operator::PhysicalWindow(op) => {
            let fns: Vec<String> = op
                .window_exprs
                .iter()
                .map(|w| {
                    let args: Vec<String> = w.args.iter().map(format_expr).collect();
                    format!("{}({})", w.name, args.join(", "))
                })
                .collect();
            out.push(format!("{pad}WINDOW [{}]{costs_suffix}", fns.join("; ")));
            for child in &node.children {
                format_physical_node(child, level, indent + 1, out);
            }
        }
        Operator::PhysicalCTEAnchor(op) => {
            out.push(format!(
                "{pad}CTE ANCHOR (cte_id={}){costs_suffix}",
                op.cte_id
            ));
            for child in &node.children {
                format_physical_node(child, level, indent + 1, out);
            }
        }
        Operator::PhysicalCTEProduce(op) => {
            out.push(format!(
                "{pad}CTE PRODUCE (cte_id={}){costs_suffix}",
                op.cte_id
            ));
            for child in &node.children {
                format_physical_node(child, level, indent + 1, out);
            }
        }
        Operator::PhysicalCTEConsume(op) => {
            out.push(format!(
                "{pad}CTE CONSUME (cte_id={}){costs_suffix}",
                op.cte_id
            ));
        }
        Operator::PhysicalRepeat(op) => {
            out.push(format!(
                "{pad}REPEAT ({} grouping sets){costs_suffix}",
                op.grouping_ids.len()
            ));
            for child in &node.children {
                format_physical_node(child, level, indent + 1, out);
            }
        }
        Operator::PhysicalUnion(op) => {
            let kind = if op.all { "UNION ALL" } else { "UNION" };
            out.push(format!("{pad}{kind}{costs_suffix}"));
            for child in &node.children {
                format_physical_node(child, level, indent + 1, out);
            }
        }
        Operator::PhysicalIntersect(_) => {
            out.push(format!("{pad}INTERSECT{costs_suffix}"));
            for child in &node.children {
                format_physical_node(child, level, indent + 1, out);
            }
        }
        Operator::PhysicalExcept(_) => {
            out.push(format!("{pad}EXCEPT{costs_suffix}"));
            for child in &node.children {
                format_physical_node(child, level, indent + 1, out);
            }
        }
        Operator::PhysicalValues(op) => {
            out.push(format!(
                "{pad}VALUES ({} rows){costs_suffix}",
                op.rows.len()
            ));
        }
        Operator::PhysicalGenerateSeries(op) => {
            out.push(format!(
                "{pad}GENERATE_SERIES({}, {}, {}){costs_suffix}",
                op.start, op.end, op.step
            ));
        }
        Operator::PhysicalSubqueryAlias(op) => {
            out.push(format!("{pad}SUBQUERY ALIAS [{}]{costs_suffix}", op.alias));
            for child in &node.children {
                format_physical_node(child, level, indent + 1, out);
            }
        }
        // Logical operators should not appear in physical plan
        _ => {
            out.push(format!("{pad}<logical operator>{costs_suffix}"));
        }
    }
}

#[derive(Default)]
struct LocalScanExplainHints {
    has_decode: bool,
    has_min_max_stats: bool,
}

fn explain_hints_for_scan(
    op: &crate::sql::optimizer::operator::PhysicalScanOp,
) -> LocalScanExplainHints {
    let Some(required_columns) = op.required_columns.as_ref() else {
        return LocalScanExplainHints::default();
    };
    if required_columns.is_empty() {
        return LocalScanExplainHints::default();
    }

    LocalScanExplainHints {
        has_decode: scan_supports_decode_hint(&op.table, required_columns),
        has_min_max_stats: scan_supports_min_max_stats(&op.table, required_columns),
    }
}

fn scan_supports_decode_hint(
    table: &crate::sql::catalog::TableDef,
    required_columns: &[String],
) -> bool {
    match &table.storage {
        TableStorage::LocalParquetFile { path } => {
            local_parquet_has_low_cardinality_string_dict(table, path)
        }
        TableStorage::S3ParquetFiles { .. } => required_columns.iter().any(|required| {
            table
                .columns
                .iter()
                .find(|column| column.name.eq_ignore_ascii_case(required))
                .map(|column| supports_scan_decode_hint(&column.data_type))
                .unwrap_or(false)
        }),
        // Iceberg metadata-table scans are JVM-bridged; the parquet decode
        // hint path does not apply.
        TableStorage::IcebergMetadataTable { .. } => false,
    }
}

fn scan_supports_min_max_stats(
    table: &crate::sql::catalog::TableDef,
    required_columns: &[String],
) -> bool {
    match &table.storage {
        TableStorage::LocalParquetFile { .. } | TableStorage::S3ParquetFiles { .. } => {}
        // Iceberg metadata tables do not produce parquet column statistics.
        TableStorage::IcebergMetadataTable { .. } => return false,
    }
    required_columns.iter().all(|required| {
        table
            .columns
            .iter()
            .find(|column| column.name.eq_ignore_ascii_case(required))
            .map(|column| supports_scan_min_max_stats(&column.data_type))
            .unwrap_or(false)
    })
}

fn supports_scan_min_max_stats(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Date32
            | DataType::Timestamp(_, _)
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::FixedSizeBinary(_)
    )
}

fn supports_scan_decode_hint(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary
    )
}

fn local_parquet_has_low_cardinality_string_dict(
    table: &crate::sql::catalog::TableDef,
    path: &std::path::Path,
) -> bool {
    const LOW_CARDINALITY_THRESHOLD: usize = 256;

    let candidate_columns: Vec<String> = table
        .columns
        .iter()
        .filter(|column| {
            matches!(
                column.data_type,
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary
            )
        })
        .map(|column| column.name.clone())
        .collect();
    if candidate_columns.is_empty() {
        return false;
    }

    let file = match File::open(path) {
        Ok(file) => file,
        Err(_) => return false,
    };
    let reader = match ParquetRecordBatchReaderBuilder::try_new(file)
        .and_then(|builder| builder.with_batch_size(4096).build())
    {
        Ok(reader) => reader,
        Err(_) => return false,
    };

    let mut distinct_sets: Vec<HashSet<Vec<u8>>> = candidate_columns
        .iter()
        .map(|_| HashSet::with_capacity(LOW_CARDINALITY_THRESHOLD + 1))
        .collect();
    let mut non_null_counts = vec![0usize; candidate_columns.len()];

    for batch in reader {
        let Ok(batch) = batch else {
            return false;
        };
        for (idx, column_name) in candidate_columns.iter().enumerate() {
            if distinct_sets[idx].len() > LOW_CARDINALITY_THRESHOLD {
                continue;
            }
            let Some(column_index) = batch
                .schema()
                .fields()
                .iter()
                .position(|field| field.name().eq_ignore_ascii_case(column_name))
            else {
                continue;
            };
            let column = batch.column(column_index);
            match column.data_type() {
                DataType::Utf8 => {
                    let Some(values) = column.as_any().downcast_ref::<StringArray>() else {
                        continue;
                    };
                    for row in 0..values.len() {
                        if values.is_null(row) {
                            continue;
                        }
                        non_null_counts[idx] += 1;
                        distinct_sets[idx].insert(values.value(row).as_bytes().to_vec());
                        if distinct_sets[idx].len() > LOW_CARDINALITY_THRESHOLD {
                            break;
                        }
                    }
                }
                DataType::LargeUtf8 => {
                    let Some(values) = column.as_any().downcast_ref::<LargeStringArray>() else {
                        continue;
                    };
                    for row in 0..values.len() {
                        if values.is_null(row) {
                            continue;
                        }
                        non_null_counts[idx] += 1;
                        distinct_sets[idx].insert(values.value(row).as_bytes().to_vec());
                        if distinct_sets[idx].len() > LOW_CARDINALITY_THRESHOLD {
                            break;
                        }
                    }
                }
                DataType::Binary => {
                    let Some(values) = column.as_any().downcast_ref::<BinaryArray>() else {
                        continue;
                    };
                    for row in 0..values.len() {
                        if values.is_null(row) {
                            continue;
                        }
                        non_null_counts[idx] += 1;
                        distinct_sets[idx].insert(values.value(row).to_vec());
                        if distinct_sets[idx].len() > LOW_CARDINALITY_THRESHOLD {
                            break;
                        }
                    }
                }
                DataType::LargeBinary => {
                    let Some(values) = column.as_any().downcast_ref::<LargeBinaryArray>() else {
                        continue;
                    };
                    for row in 0..values.len() {
                        if values.is_null(row) {
                            continue;
                        }
                        non_null_counts[idx] += 1;
                        distinct_sets[idx].insert(values.value(row).to_vec());
                        if distinct_sets[idx].len() > LOW_CARDINALITY_THRESHOLD {
                            break;
                        }
                    }
                }
                _ => {}
            }
        }
    }

    distinct_sets
        .iter()
        .zip(non_null_counts.iter())
        .any(|(distinct, non_null_count)| {
            *non_null_count > distinct.len()
                && !distinct.is_empty()
                && distinct.len() <= LOW_CARDINALITY_THRESHOLD
        })
}

fn format_expr(expr: &TypedExpr) -> String {
    format_expr_kind(&expr.kind)
}

fn format_expr_kind(kind: &ExprKind) -> String {
    match kind {
        ExprKind::ColumnRef { qualifier, column } => match qualifier {
            Some(q) => format!("{q}.{column}"),
            None => column.clone(),
        },
        ExprKind::Literal(lit) => match lit {
            LiteralValue::Null => "NULL".to_string(),
            LiteralValue::Bool(b) => b.to_string(),
            LiteralValue::Int(n) => n.to_string(),
            LiteralValue::LargeInt(n) => n.to_string(),
            LiteralValue::Float(f) => f.to_string(),
            LiteralValue::Decimal(d) => d.clone(),
            LiteralValue::String(s) => format!("'{s}'"),
        },
        ExprKind::BinaryOp { left, op, right } => {
            let op_str = match op {
                BinOp::Add => "+",
                BinOp::Sub => "-",
                BinOp::Mul => "*",
                BinOp::Div => "/",
                BinOp::Mod => "%",
                BinOp::Eq => "=",
                BinOp::Ne => "!=",
                BinOp::Lt => "<",
                BinOp::Le => "<=",
                BinOp::Gt => ">",
                BinOp::Ge => ">=",
                BinOp::EqForNull => "<=>",
                BinOp::And => "AND",
                BinOp::Or => "OR",
            };
            format!("{} {op_str} {}", format_expr(left), format_expr(right))
        }
        ExprKind::UnaryOp { op, expr } => {
            let op_str = match op {
                UnOp::Not => "NOT",
                UnOp::Negate => "-",
                UnOp::BitwiseNot => "~",
            };
            format!("{op_str} {}", format_expr(expr))
        }
        ExprKind::FunctionCall {
            name,
            args,
            distinct,
            ..
        } => {
            let args_str: Vec<String> = args.iter().map(format_expr).collect();
            let distinct_str = if *distinct { "DISTINCT " } else { "" };
            format!("{name}({distinct_str}{})", args_str.join(", "))
        }
        ExprKind::AggregateCall {
            name,
            args,
            distinct,
            ..
        } => {
            let args_str: Vec<String> = args.iter().map(format_expr).collect();
            let distinct_str = if *distinct { "DISTINCT " } else { "" };
            format!("{name}({distinct_str}{})", args_str.join(", "))
        }
        ExprKind::Cast { expr, target } => {
            format!("CAST({} AS {target:?})", format_expr(expr))
        }
        ExprKind::IsNull { expr, negated } => {
            let not = if *negated { " NOT" } else { "" };
            format!("{} IS{not} NULL", format_expr(expr))
        }
        ExprKind::InList {
            expr,
            list,
            negated,
        } => {
            let not = if *negated { " NOT" } else { "" };
            let items: Vec<String> = list.iter().map(format_expr).collect();
            format!("{}{not} IN ({})", format_expr(expr), items.join(", "))
        }
        ExprKind::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let not = if *negated { " NOT" } else { "" };
            format!(
                "{}{not} BETWEEN {} AND {}",
                format_expr(expr),
                format_expr(low),
                format_expr(high)
            )
        }
        ExprKind::Like {
            expr,
            pattern,
            negated,
        } => {
            let not = if *negated { " NOT" } else { "" };
            format!("{}{not} LIKE {}", format_expr(expr), format_expr(pattern))
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            let mut s = String::from("CASE");
            if let Some(op) = operand {
                let _ = write!(s, " {}", format_expr(op));
            }
            for (w, t) in when_then {
                let _ = write!(s, " WHEN {} THEN {}", format_expr(w), format_expr(t));
            }
            if let Some(e) = else_expr {
                let _ = write!(s, " ELSE {}", format_expr(e));
            }
            s.push_str(" END");
            s
        }
        ExprKind::IsTruthValue {
            expr,
            value,
            negated,
        } => {
            let not = if *negated { " NOT" } else { "" };
            let val = if *value { "TRUE" } else { "FALSE" };
            format!("{} IS{not} {val}", format_expr(expr))
        }
        ExprKind::Nested(inner) => format_expr(inner),
        ExprKind::WindowCall { name, args, .. } => {
            let args_str: Vec<String> = args.iter().map(format_expr).collect();
            format!("{name}({})", args_str.join(", "))
        }
        ExprKind::SubqueryPlaceholder { id, .. } => format!("<subquery_{id}>"),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};

    use arrow::datatypes::DataType;

    use super::{ExplainLevel, explain_physical_plan};
    use crate::sql::analysis::OutputColumn;
    use crate::sql::catalog::{ColumnDef, TableDef, TableStorage};
    use crate::sql::optimizer::operator::{Operator, PhysicalScanOp};
    use crate::sql::optimizer::physical_plan::PhysicalPlanNode;
    use crate::sql::optimizer::statistics::Statistics;

    #[test]
    fn s3_scan_verbose_explain_reports_min_max_stats_for_supported_required_columns() {
        let column = ColumnDef {
            name: "c_2_0".to_string(),
            data_type: DataType::FixedSizeBinary(16),
            nullable: false,
            write_default: None,
        };
        let plan = PhysicalPlanNode {
            op: Operator::PhysicalScan(PhysicalScanOp {
                database: "db1".to_string(),
                table: TableDef {
                    name: "t3".to_string(),
                    columns: vec![column.clone()],
                    iceberg_row_lineage_metadata_columns: Vec::new(),
                    iceberg_table: None,
                    storage: TableStorage::S3ParquetFiles {
                        files: Vec::new(),
                        cloud_properties: BTreeMap::new(),
                    },
                },
                alias: None,
                columns: vec![OutputColumn {
                    name: column.name.clone(),
                    data_type: column.data_type.clone(),
                    nullable: column.nullable,
                }],
                predicates: Vec::new(),
                required_columns: Some(vec![column.name.clone()]),
            }),
            children: Vec::new(),
            stats: Statistics {
                output_row_count: 3.0,
                column_statistics: HashMap::new(),
            },
            output_columns: Vec::new(),
        };

        let lines = explain_physical_plan(&plan, ExplainLevel::Verbose);

        assert!(
            lines.iter().any(|line| line.contains("min-max stats")),
            "verbose explain lines: {lines:?}"
        );
    }

    #[test]
    fn s3_scan_costs_explain_reports_decode_for_string_required_columns() {
        let column = ColumnDef {
            name: "c8".to_string(),
            data_type: DataType::Utf8,
            nullable: true,
            write_default: None,
        };
        let plan = PhysicalPlanNode {
            op: Operator::PhysicalScan(PhysicalScanOp {
                database: "db1".to_string(),
                table: TableDef {
                    name: "all_t0".to_string(),
                    columns: vec![column.clone()],
                    iceberg_row_lineage_metadata_columns: Vec::new(),
                    iceberg_table: None,
                    storage: TableStorage::S3ParquetFiles {
                        files: Vec::new(),
                        cloud_properties: BTreeMap::new(),
                    },
                },
                alias: None,
                columns: vec![OutputColumn {
                    name: column.name.clone(),
                    data_type: column.data_type.clone(),
                    nullable: column.nullable,
                }],
                predicates: Vec::new(),
                required_columns: Some(vec![column.name.clone()]),
            }),
            children: Vec::new(),
            stats: Statistics {
                output_row_count: 3.0,
                column_statistics: HashMap::new(),
            },
            output_columns: Vec::new(),
        };

        let lines = explain_physical_plan(&plan, ExplainLevel::Costs);

        assert!(
            lines.iter().any(|line| line.contains("Decode")),
            "costs explain lines: {lines:?}"
        );
    }
}
