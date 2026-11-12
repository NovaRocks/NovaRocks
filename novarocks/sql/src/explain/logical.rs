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

//! Borrowed logical EXPLAIN rendering into the shared bounded line collector.

use std::fmt::{self, Write};

use arrow::datatypes::DataType;

use super::completed::{ExplainRenderBudget, ExplainRenderOutput, RenderDiagnostic};
use super::{ExplainLevel, PlanNodeExplainStage};
use crate::analysis::{
    BinOp, ExprKind, JoinKind, LiteralValue, ProjectItem, SortItem, TypedExpr, UnOp,
};
use crate::common::ApplyKind;
use crate::planner::logical::{LogicalPlanKind, LogicalPlanNode};
use crate::planner::payload::{PlanAssertOneRowNode, PlanRowCountAssertion, WindowExpr};
use crate::planner::table::{
    ScanSource, SqlMvTargetStatePartitionConstraint, SqlMvTargetStateRowFilter, SqlScanKind,
};

// This formatter uses an internal bounded recursion stack, checked before
// entering the next frame. It is not a logical-plan semantic contract. Byte
// and line bounds remain independently enforced by ExplainRenderOutput.
const MAX_DEPTH: usize = 64;

pub(super) fn render_observed(
    plan: &LogicalPlanNode,
    level: ExplainLevel,
    budget: ExplainRenderBudget,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<Vec<String>, crate::compiler::SqlCompileError> {
    let mut output = ExplainRenderOutput::new_observed(budget, control)?;
    let diagnostic = RenderDiagnostic::new(control);
    let result = visit(plan, level, 0, &mut output, &diagnostic);
    if let Some(error) = diagnostic.take_error() {
        return Err(error);
    }
    result?;
    output.finish_observed()
}
#[cfg(test)]
pub(super) fn render(
    plan: &LogicalPlanNode,
    level: ExplainLevel,
    budget: ExplainRenderBudget,
) -> Result<Vec<String>, String> {
    render_observed(
        plan,
        level,
        budget,
        &crate::compiler::SqlCompileControl::unbounded(),
    )
    .map_err(|error| error.to_string())
}

fn visit(
    plan: &LogicalPlanNode,
    level: ExplainLevel,
    depth: usize,
    output: &mut ExplainRenderOutput<'_>,
    diagnostic: &RenderDiagnostic<'_>,
) -> Result<(), crate::compiler::SqlCompileError> {
    if depth >= MAX_DEPTH {
        return Err(crate::compiler::SqlCompileError::InvalidRequest(
            "logical EXPLAIN exceeds its 64-level rendering depth".into(),
        ));
    }
    validate_node_expressions(&plan.kind)
        .map_err(crate::compiler::SqlCompileError::InvalidRequest)?;
    let pad = Indent(depth);
    let verbose = matches!(
        level,
        ExplainLevel::Verbose | ExplainLevel::Costs | ExplainLevel::Analyze
    );
    macro_rules! line {
        ($($arguments:tt)*) => {
            output.push(format_args!($($arguments)*))?
        };
    }
    let children = match &plan.kind {
        LogicalPlanKind::Scan(node) => {
            line!(
                "{pad}0:{}",
                Header(&plan.kind, PlanNodeExplainStage::Logical, Some(diagnostic))
            );
            if verbose && let Some(columns) = &node.required_columns {
                line!(
                    "{pad}     columns: {}",
                    DisplayFn(|out: &mut dyn fmt::Write| {
                        for (index, required) in columns.iter().enumerate() {
                            separator(out, index, ", ")?;
                            if let Some(column) = node
                                .columns
                                .iter()
                                .find(|column| column.column_id == *required)
                            {
                                out.write_str(&column.name)?;
                            } else {
                                write!(out, "ColumnId({})", required.0)?;
                            }
                        }
                        Ok(())
                    })
                );
            }
            if verbose && has_scan_label(&node.table.source) {
                line!("{pad}     source: {}", ScanLabel(&node.table.source));
            }
            if !node.predicates.is_empty() {
                line!(
                    "{pad}     predicates: {}",
                    Expressions(&node.predicates, " AND ", Some(diagnostic))
                );
            }
            0
        }
        LogicalPlanKind::Filter(node) => {
            line!("{pad}FILTER");
            line!(
                "{pad}  predicate: {}",
                Expression(&node.predicate, Some(diagnostic))
            );
            1
        }
        LogicalPlanKind::Project(_)
        | LogicalPlanKind::Sort(_)
        | LogicalPlanKind::Window(_)
        | LogicalPlanKind::TableFunction(_)
        | LogicalPlanKind::Repeat(_)
        | LogicalPlanKind::AssertOneRow(_) => {
            line!(
                "{pad}{}",
                Header(&plan.kind, PlanNodeExplainStage::Logical, Some(diagnostic))
            );
            1
        }
        LogicalPlanKind::Aggregate(node) => {
            line!("{pad}AGGREGATE");
            if !node.group_by.is_empty() {
                line!(
                    "{pad}  group by: {}",
                    Expressions(&node.group_by, ", ", Some(diagnostic))
                );
            }
            if !node.aggregates.is_empty() {
                line!(
                    "{pad}  aggregations: {}",
                    DisplayFn(|out: &mut dyn fmt::Write| {
                        for (index, aggregate) in node.aggregates.iter().enumerate() {
                            separator(out, index, ", ")?;
                            write!(out, "{}(", aggregate.name)?;
                            if aggregate.distinct {
                                out.write_str("DISTINCT ")?;
                            }
                            expressions(
                                out,
                                aggregate.source.arguments(),
                                ", ",
                                0,
                                Some(diagnostic),
                            )?;
                            out.write_char(')')?;
                        }
                        Ok(())
                    })
                );
            }
            1
        }
        LogicalPlanKind::Join(node) => {
            let kind = match node.join_type {
                JoinKind::LeftMark { .. } => "NULL AWARE LEFT MARK JOIN",
                JoinKind::Inner => "INNER JOIN",
                JoinKind::LeftOuter => "LEFT OUTER JOIN",
                JoinKind::RightOuter => "RIGHT OUTER JOIN",
                JoinKind::FullOuter => "FULL OUTER JOIN",
                JoinKind::Cross => "CROSS JOIN",
                JoinKind::LeftSemi => "LEFT SEMI JOIN",
                JoinKind::RightSemi => "RIGHT SEMI JOIN",
                JoinKind::LeftAnti => "LEFT ANTI JOIN",
                JoinKind::RightAnti => "RIGHT ANTI JOIN",
                JoinKind::NullAwareLeftAnti => "NULL AWARE LEFT ANTI JOIN",
            };
            line!("{pad}{kind}");
            if let Some(condition) = &node.condition {
                line!("{pad}  on: {}", Expression(condition, Some(diagnostic)));
            }
            2
        }
        LogicalPlanKind::Limit(node) => {
            line!(
                "{pad}LIMIT [{}]",
                DisplayFn(|out: &mut dyn fmt::Write| {
                    if let Some(limit) = node.limit {
                        write!(out, "limit={limit}")?;
                    }
                    if let Some(offset) = node.offset {
                        if node.limit.is_some() {
                            out.write_str(", ")?;
                        }
                        write!(out, "offset={offset}")?;
                    }
                    Ok(())
                })
            );
            1
        }
        LogicalPlanKind::Union(node) => {
            line!("{pad}{}", if node.all { "UNION ALL" } else { "UNION" });
            plan.children.len()
        }
        LogicalPlanKind::Intersect(_) => {
            line!("{pad}INTERSECT");
            plan.children.len()
        }
        LogicalPlanKind::Except(_) => {
            line!("{pad}EXCEPT");
            plan.children.len()
        }
        LogicalPlanKind::Values(_)
        | LogicalPlanKind::SystemTable(_)
        | LogicalPlanKind::GenerateSeries(_) => {
            line!(
                "{pad}{}",
                Header(&plan.kind, PlanNodeExplainStage::Logical, Some(diagnostic))
            );
            0
        }
        LogicalPlanKind::CTEAnchor(node) => {
            line!("{pad}CTE_ANCHOR(cte_id={})", node.cte_id);
            2
        }
        LogicalPlanKind::CTEProduce(node) => {
            line!("{pad}CTE_PRODUCE(cte_id={})", node.cte_id);
            1
        }
        LogicalPlanKind::CTEConsume(node) => {
            line!("{pad}CTE_CONSUME(cte_id={})", node.cte_id);
            0
        }
        LogicalPlanKind::Apply(node) => {
            let kind = match node.kind {
                ApplyKind::Scalar => "SCALAR",
                ApplyKind::Exists { negated: false } => "EXISTS",
                ApplyKind::Exists { negated: true } => "NOT EXISTS",
                ApplyKind::In { negated: false } => "IN",
                ApplyKind::In { negated: true } => "NOT IN",
            };
            line!(
                "{pad}APPLY ({kind}, correlated={}, use_semi_anti={})",
                !node.correlation_column_ids.is_empty(),
                node.use_semi_anti
            );
            2
        }
        LogicalPlanKind::ImvDelta(_) | LogicalPlanKind::ImvVersion(_) => {
            return Err(crate::compiler::SqlCompileError::InvalidRequest(
                "imv marker leaked into non-IMV plan".into(),
            ));
        }
    };
    if plan.children.len() != children {
        return Err(crate::compiler::SqlCompileError::InvalidRequest(
            "logical EXPLAIN node has an invalid child count".into(),
        ));
    }
    for child in &plan.children {
        visit(child, level, depth + 1, output, diagnostic)?;
    }
    Ok(())
}

struct Indent(usize);
impl fmt::Display for Indent {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        for _ in 0..self.0 {
            out.write_str("  ")?;
        }
        Ok(())
    }
}

struct DisplayFn<F>(F);
impl<F> fmt::Display for DisplayFn<F>
where
    F: Fn(&mut dyn fmt::Write) -> fmt::Result,
{
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        (self.0)(out)
    }
}

fn separator(out: &mut dyn fmt::Write, index: usize, separator: &str) -> fmt::Result {
    if index != 0 {
        out.write_str(separator)?;
    }
    Ok(())
}

pub(super) struct Header<'a>(
    pub &'a LogicalPlanKind,
    pub PlanNodeExplainStage,
    pub Option<&'a RenderDiagnostic<'a>>,
);
impl fmt::Display for Header<'_> {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            LogicalPlanKind::Scan(node) => {
                write!(out, "SCAN {}.{}", node.database, node.table.name)?;
                if let Some(alias) = &node.alias {
                    write!(out, " (alias={alias})")?;
                }
                Ok(())
            }
            LogicalPlanKind::Filter(_) => out.write_str("FILTER"),
            LogicalPlanKind::Project(node) => {
                out.write_str("PROJECT [")?;
                for (index, item) in node.items.iter().enumerate() {
                    separator(out, index, ", ")?;
                    project(out, item, self.2)?;
                }
                out.write_char(']')
            }
            LogicalPlanKind::Sort(node) => {
                out.write_str("SORT BY [")?;
                sort_items(out, &node.items, true, self.2)?;
                out.write_char(']')
            }
            LogicalPlanKind::Window(node) => {
                out.write_str("WINDOW [")?;
                for (index, window) in node.window_exprs.iter().enumerate() {
                    separator(out, index, "; ")?;
                    window_expr(out, window, self.1, self.2)?;
                }
                out.write_char(']')
            }
            LogicalPlanKind::Values(node) => write!(out, "VALUES ({} rows)", node.rows.len()),
            LogicalPlanKind::SystemTable(node) => write!(
                out,
                "SYSTEM TABLE {} [columns={:?}]",
                node.declaration.qualified_name(),
                node.required_columns
            ),
            LogicalPlanKind::Repeat(node) => {
                write!(out, "REPEAT ({} grouping sets)", node.grouping_ids.len())
            }
            LogicalPlanKind::GenerateSeries(node) => write!(
                out,
                "GENERATE_SERIES({}, {}, {})",
                node.start, node.end, node.step
            ),
            LogicalPlanKind::TableFunction(node) => {
                write!(
                    out,
                    "TABLE_FUNCTION [{} ",
                    if node.is_left_join { "LEFT" } else { "CROSS" }
                )?;
                for character in node.function_name.chars() {
                    for uppercase in character.to_uppercase() {
                        out.write_char(uppercase)?;
                    }
                }
                out.write_char(']')
            }
            LogicalPlanKind::AssertOneRow(node) => assertion(out, node, self.1),
            _ => Err(fmt::Error),
        }
    }
}

fn assertion(
    out: &mut dyn fmt::Write,
    node: &PlanAssertOneRowNode,
    stage: PlanNodeExplainStage,
) -> fmt::Result {
    if stage == PlanNodeExplainStage::Logical {
        return out.write_str("ASSERT ONE ROW");
    }
    let relation = match node.assertion {
        PlanRowCountAssertion::Eq => "=",
        PlanRowCountAssertion::Ne => "!=",
        PlanRowCountAssertion::Lt => "<",
        PlanRowCountAssertion::Le => "<=",
        PlanRowCountAssertion::Gt => ">",
        PlanRowCountAssertion::Ge => ">=",
    };
    let desired = node.desired_num_rows.unwrap_or(1);
    if node.group_key_column_ids.is_empty() {
        return write!(out, "ASSERT NUM ROWS ({relation} {desired})");
    }
    write!(out, "ASSERT NUM ROWS (PER KEY {relation} {desired} BY [")?;
    if node.group_key_labels.is_empty() {
        for (index, column) in node.group_key_column_ids.iter().enumerate() {
            separator(out, index, ", ")?;
            write!(out, "column_{}", column.0)?;
        }
    } else {
        for (index, label) in node.group_key_labels.iter().enumerate() {
            separator(out, index, ", ")?;
            out.write_str(label)?;
        }
    }
    out.write_str("])")
}

fn has_scan_label(source: &ScanSource) -> bool {
    matches!(source, ScanSource::Sql(source) if matches!(source.kind,
        SqlScanKind::Delta { .. }
        | SqlScanKind::FrozenInputSet {
            version: crate::planner::table::SqlTableVersionSelector::Snapshot(_),
        }
        | SqlScanKind::MvTargetState { .. } | SqlScanKind::MvTargetLocator { .. }))
}

struct ScanLabel<'a>(&'a ScanSource);
impl fmt::Display for ScanLabel<'_> {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ScanSource::Sql(source) = self.0;
        match &source.kind {
            SqlScanKind::Delta {
                from_snapshot_id,
                to_snapshot_id,
            } => write!(
                out,
                "IcebergDeltaTable from_snapshot_id={from_snapshot_id} to_snapshot_id={to_snapshot_id}"
            ),
            SqlScanKind::FrozenInputSet { version } => match version {
                crate::planner::table::SqlTableVersionSelector::Snapshot(id) => {
                    write!(out, "IcebergVersionTable snapshot_id={id}")
                }
                crate::planner::table::SqlTableVersionSelector::Current => Err(fmt::Error),
            },
            SqlScanKind::MvTargetLocator { facts } => {
                write!(
                    out,
                    "IcebergMvTargetLocator target={}.{}.{} apply_key={}",
                    source.table.catalog,
                    source.table.namespace,
                    source.table.table,
                    facts.apply_key_column
                )?;
                if let Some(branch) = &facts.branch_id_column {
                    write!(out, " branch_id={branch}")?;
                }
                Ok(())
            }
            SqlScanKind::MvTargetState { facts } => {
                write!(
                    out,
                    "IcebergMvTargetState target={}.{}.{} keys=[",
                    source.table.catalog, source.table.namespace, source.table.table
                )?;
                for (index, name) in facts.group_key_names.iter().enumerate() {
                    separator(out, index, ",")?;
                    out.write_str(name)?;
                }
                out.write_str("] states=[")?;
                for (index, name) in facts.aggregate_state_names.iter().enumerate() {
                    separator(out, index, ",")?;
                    out.write_str(name)?;
                }
                write!(out, "] uuid={} snapshot=", facts.target_table_uuid)?;
                match facts.target_snapshot_id {
                    Some(id) => write!(out, "{id}")?,
                    None => out.write_str("none")?,
                }
                write!(out, " layout={} ", facts.aggregate_state_layout_version)?;
                match &facts.row_filter {
                    SqlMvTargetStateRowFilter::DeltaInputRowIds {
                        row_id_column_name,
                        branch_scope,
                    } => {
                        write!(out, "row_filter=delta_input_row_ids({row_id_column_name}")?;
                        if let Some(scope) = branch_scope {
                            write!(out, ", {}={}", scope.branch_id_column_name, scope.branch_id)?;
                        }
                        out.write_str(") ")?;
                    }
                }
                out.write_str(match facts.partition_constraint {
                    SqlMvTargetStatePartitionConstraint::Unpartitioned => "partition=unpartitioned",
                    SqlMvTargetStatePartitionConstraint::AffectedPartitionAllowListRequired => {
                        "partition=affected_allow_list_required"
                    }
                })
            }
            _ => Err(fmt::Error),
        }
    }
}

pub(super) struct Expression<'a>(pub &'a TypedExpr, pub Option<&'a RenderDiagnostic<'a>>);
impl fmt::Display for Expression<'_> {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        expression(out, self.0, 0, self.1)
    }
}

struct Expressions<'a>(
    &'a [TypedExpr],
    &'static str,
    Option<&'a RenderDiagnostic<'a>>,
);
impl fmt::Display for Expressions<'_> {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        expressions(out, self.0, self.1, 0, self.2)
    }
}

fn expressions(
    out: &mut dyn fmt::Write,
    items: &[TypedExpr],
    delimiter: &str,
    depth: usize,
    diagnostic: Option<&RenderDiagnostic<'_>>,
) -> fmt::Result {
    for (index, item) in items.iter().enumerate() {
        separator(out, index, delimiter)?;
        expression(out, item, depth, diagnostic)?;
    }
    Ok(())
}

// Compare against the alias while emitting the expression once to the real
// bounded writer. Neither an expression String nor a second render is needed.
struct ComparingWrite<'a, 'b> {
    out: &'a mut dyn fmt::Write,
    name: &'b [u8],
    offset: usize,
    equal: bool,
}
impl fmt::Write for ComparingWrite<'_, '_> {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        self.out.write_str(text)?;
        let end = self.offset.checked_add(text.len()).ok_or(fmt::Error)?;
        self.equal &= self.name.get(self.offset..end) == Some(text.as_bytes());
        self.offset = end;
        Ok(())
    }
}

#[cfg(test)]
pub(super) struct Project<'a>(pub &'a ProjectItem, pub Option<&'a RenderDiagnostic<'a>>);

#[cfg(test)]
impl fmt::Display for Project<'_> {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        project(out, self.0, self.1)
    }
}

fn project(
    out: &mut dyn fmt::Write,
    item: &ProjectItem,
    diagnostic: Option<&RenderDiagnostic<'_>>,
) -> fmt::Result {
    let equal = {
        let mut comparison = ComparingWrite {
            out,
            name: item.output_name.as_bytes(),
            offset: 0,
            equal: true,
        };
        expression(&mut comparison, &item.expr, 0, diagnostic)?;
        comparison.equal && comparison.offset == comparison.name.len()
    };
    if !equal {
        write!(out, " AS {}", item.output_name)?;
    }
    Ok(())
}

#[cfg(test)]
pub(super) struct SortItems<'a>(pub &'a [SortItem], pub Option<&'a RenderDiagnostic<'a>>);
#[cfg(test)]
impl fmt::Display for SortItems<'_> {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        sort_items(out, self.0, true, self.1)
    }
}

fn sort_items(
    out: &mut dyn fmt::Write,
    items: &[SortItem],
    nulls: bool,
    diagnostic: Option<&RenderDiagnostic<'_>>,
) -> fmt::Result {
    for (index, item) in items.iter().enumerate() {
        separator(out, index, ", ")?;
        expression(out, &item.expr, 0, diagnostic)?;
        write!(out, " {}", if item.asc { "ASC" } else { "DESC" })?;
        if nulls {
            out.write_str(if item.nulls_first {
                " NULLS FIRST"
            } else {
                " NULLS LAST"
            })?;
        }
    }
    Ok(())
}

fn window_expr(
    out: &mut dyn fmt::Write,
    window: &WindowExpr,
    stage: PlanNodeExplainStage,
    diagnostic: Option<&RenderDiagnostic<'_>>,
) -> fmt::Result {
    write!(out, "{}(", window.name)?;
    expressions(out, &window.args, ", ", 0, diagnostic)?;
    out.write_char(')')?;
    if stage == PlanNodeExplainStage::Logical {
        out.write_str(" OVER (")?;
        if !window.partition_by.is_empty() {
            out.write_str("PARTITION BY ")?;
            expressions(out, &window.partition_by, ", ", 0, diagnostic)?;
        }
        if !window.order_by.is_empty() {
            if !window.partition_by.is_empty() {
                out.write_char(' ')?;
            }
            out.write_str("ORDER BY ")?;
            sort_items(out, &window.order_by, false, diagnostic)?;
        }
        out.write_char(')')?;
    }
    Ok(())
}

fn expression(
    out: &mut dyn fmt::Write,
    expr: &TypedExpr,
    depth: usize,
    diagnostic: Option<&RenderDiagnostic<'_>>,
) -> fmt::Result {
    if depth >= MAX_DEPTH {
        return Err(fmt::Error);
    }
    let next = depth + 1;
    match &expr.kind {
        ExprKind::ColumnRef {
            qualifier, column, ..
        } => {
            if let Some(qualifier) = qualifier {
                write!(out, "{qualifier}.")?;
            }
            out.write_str(column)
        }
        ExprKind::LambdaParamRef { name, .. } => out.write_str(name),
        ExprKind::Constant(value) => match diagnostic {
            Some(diagnostic) => diagnostic.constant(value, out),
            None => Err(fmt::Error),
        },
        ExprKind::Literal(value) => match value {
            LiteralValue::Null => out.write_str("NULL"),
            LiteralValue::Bool(value) => write!(out, "{value}"),
            LiteralValue::Int(value) => write!(out, "{value}"),
            LiteralValue::LargeInt(value) => write!(out, "{value}"),
            LiteralValue::Float(value) => write!(out, "{value}"),
            LiteralValue::Decimal(value) => out.write_str(value),
            LiteralValue::String(value) => write!(out, "'{value}'"),
            LiteralValue::Binary(bytes) => {
                out.write_str("X'")?;
                for byte in bytes {
                    write!(out, "{byte:02X}")?;
                }
                out.write_str("'")
            }
        },
        ExprKind::BinaryOp {
            left, op, right, ..
        } => {
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
            let (left, right) = if matches!(op, BinOp::Eq | BinOp::EqForNull)
                && matches!(left.kind, ExprKind::Literal(_) | ExprKind::Constant(_))
                && matches!(right.kind, ExprKind::ColumnRef { .. })
            {
                (right.as_ref(), left.as_ref())
            } else {
                (left.as_ref(), right.as_ref())
            };
            expression(out, left, next, diagnostic)?;
            write!(out, " {op_str} ")?;
            expression(out, right, next, diagnostic)
        }
        ExprKind::UnaryOp { op, expr } => {
            out.write_str(match op {
                UnOp::Not => "NOT ",
                UnOp::Negate => "- ",
                UnOp::BitwiseNot => "~ ",
            })?;
            expression(out, expr, next, diagnostic)
        }
        ExprKind::FunctionCall {
            name,
            args,
            distinct,
            ..
        }
        | ExprKind::AggregateCall {
            name,
            args,
            distinct,
            ..
        } => {
            write!(out, "{name}(")?;
            if *distinct {
                out.write_str("DISTINCT ")?;
            }
            expressions(out, args, ", ", next, diagnostic)?;
            out.write_char(')')
        }
        ExprKind::WindowCall { name, args, .. } => {
            write!(out, "{name}(")?;
            expressions(out, args, ", ", next, diagnostic)?;
            out.write_char(')')
        }
        ExprKind::LambdaFunction { params, body } => {
            out.write_char('(')?;
            for (index, param) in params.iter().enumerate() {
                separator(out, index, ", ")?;
                out.write_str(&param.name)?;
            }
            out.write_str(") -> ")?;
            expression(out, body, next, diagnostic)
        }
        ExprKind::Lambda { params, body } => {
            if params.len() != 1 {
                out.write_char('(')?;
            }
            for (index, param) in params.iter().enumerate() {
                separator(out, index, ", ")?;
                out.write_str(param)?;
            }
            if params.len() != 1 {
                out.write_char(')')?;
            }
            out.write_str(" -> ")?;
            expression(out, body, next, diagnostic)
        }
        ExprKind::Cast { expr, target, .. } => {
            // Arrow Debug recursively walks child types; validate its bounded
            // depth before asking that third-party formatter to descend.
            validate_type_depth(target, 0)?;
            out.write_str("CAST(")?;
            expression(out, expr, next, diagnostic)?;
            write!(out, " AS {target:?})")
        }
        ExprKind::IsNull { expr, negated } => {
            expression(out, expr, next, diagnostic)?;
            out.write_str(if *negated { " IS NOT NULL" } else { " IS NULL" })
        }
        ExprKind::IsTruthValue {
            expr,
            value,
            negated,
        } => {
            expression(out, expr, next, diagnostic)?;
            write!(
                out,
                " IS{} {}",
                if *negated { " NOT" } else { "" },
                if *value { "TRUE" } else { "FALSE" }
            )
        }
        ExprKind::InList {
            expr,
            list,
            negated,
        } => {
            expression(out, expr, next, diagnostic)?;
            out.write_str(if *negated { " NOT IN (" } else { " IN (" })?;
            expressions(out, list, ", ", next, diagnostic)?;
            out.write_char(')')
        }
        ExprKind::Between {
            expr,
            low,
            high,
            negated,
        } => {
            expression(out, expr, next, diagnostic)?;
            out.write_str(if *negated {
                " NOT BETWEEN "
            } else {
                " BETWEEN "
            })?;
            expression(out, low, next, diagnostic)?;
            out.write_str(" AND ")?;
            expression(out, high, next, diagnostic)
        }
        ExprKind::Like {
            expr,
            pattern,
            negated,
        } => {
            expression(out, expr, next, diagnostic)?;
            out.write_str(if *negated { " NOT LIKE " } else { " LIKE " })?;
            expression(out, pattern, next, diagnostic)
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            out.write_str("CASE")?;
            if let Some(operand) = operand {
                out.write_char(' ')?;
                expression(out, operand, next, diagnostic)?;
            }
            for (when, then) in when_then {
                out.write_str(" WHEN ")?;
                expression(out, when, next, diagnostic)?;
                out.write_str(" THEN ")?;
                expression(out, then, next, diagnostic)?;
            }
            if let Some(otherwise) = else_expr {
                out.write_str(" ELSE ")?;
                expression(out, otherwise, next, diagnostic)?;
            }
            out.write_str(" END")
        }
        ExprKind::Nested(inner) => expression(out, inner, next, diagnostic),
        ExprKind::SubqueryPlaceholder { id, .. } => write!(out, "<subquery_{id}>"),
    }
}

// A borrowed structural walk keeps deep expressions and Arrow Debug types out
// of formatting recursion. Its bounded work performs no render or allocation.
fn validate_node_expressions(kind: &LogicalPlanKind) -> Result<(), String> {
    let mut visited = 0;
    let mut check = |expr: &TypedExpr| {
        validate_expression_depth(expr, 0, &mut visited).map_err(|_| {
            "logical EXPLAIN expression exceeds its depth or structural work bound".to_owned()
        })
    };
    match kind {
        LogicalPlanKind::Scan(node) => {
            for expr in &node.predicates {
                check(expr)?;
            }
        }
        LogicalPlanKind::Filter(node) => check(&node.predicate)?,
        LogicalPlanKind::Project(node) => {
            for item in &node.items {
                check(&item.expr)?;
            }
        }
        LogicalPlanKind::Sort(node) => {
            for item in &node.items {
                check(&item.expr)?;
            }
        }
        LogicalPlanKind::Window(node) => {
            for window in &node.window_exprs {
                for expr in window.args.iter().chain(&window.partition_by) {
                    check(expr)?;
                }
                for item in &window.order_by {
                    check(&item.expr)?;
                }
            }
        }
        LogicalPlanKind::Aggregate(node) => {
            for expr in &node.group_by {
                check(expr)?;
            }
            for aggregate in &node.aggregates {
                for expr in aggregate.source.arguments() {
                    check(expr)?;
                }
            }
        }
        LogicalPlanKind::Join(node) => {
            if let Some(condition) = &node.condition {
                check(condition)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn validate_expression_depth(expr: &TypedExpr, depth: usize, visited: &mut usize) -> fmt::Result {
    if depth >= MAX_DEPTH || *visited >= 65_536 {
        return Err(fmt::Error);
    }
    *visited += 1;
    let next = depth + 1;
    match &expr.kind {
        ExprKind::BinaryOp { left, right, .. } => {
            validate_expression_depth(left, next, visited)?;
            validate_expression_depth(right, next, visited)?;
        }
        ExprKind::UnaryOp { expr, .. }
        | ExprKind::IsNull { expr, .. }
        | ExprKind::IsTruthValue { expr, .. }
        | ExprKind::Nested(expr) => validate_expression_depth(expr, next, visited)?,
        ExprKind::Cast { expr, target, .. } => {
            validate_type_depth(target, 0)?;
            validate_expression_depth(expr, next, visited)?;
        }
        ExprKind::FunctionCall { args, .. }
        | ExprKind::AggregateCall { args, .. }
        | ExprKind::WindowCall { args, .. } => {
            for expr in args {
                validate_expression_depth(expr, next, visited)?;
            }
        }
        ExprKind::LambdaFunction { body, .. } | ExprKind::Lambda { body, .. } => {
            validate_expression_depth(body, next, visited)?
        }
        ExprKind::InList { expr, list, .. } => {
            validate_expression_depth(expr, next, visited)?;
            for expr in list {
                validate_expression_depth(expr, next, visited)?;
            }
        }
        ExprKind::Between {
            expr, low, high, ..
        } => {
            validate_expression_depth(expr, next, visited)?;
            validate_expression_depth(low, next, visited)?;
            validate_expression_depth(high, next, visited)?;
        }
        ExprKind::Like { expr, pattern, .. } => {
            validate_expression_depth(expr, next, visited)?;
            validate_expression_depth(pattern, next, visited)?;
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(expr) = operand {
                validate_expression_depth(expr, next, visited)?;
            }
            for (when, then) in when_then {
                validate_expression_depth(when, next, visited)?;
                validate_expression_depth(then, next, visited)?;
            }
            if let Some(expr) = else_expr {
                validate_expression_depth(expr, next, visited)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn validate_type_depth(data_type: &DataType, depth: usize) -> fmt::Result {
    if depth >= MAX_DEPTH {
        return Err(fmt::Error);
    }
    match data_type {
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => validate_type_depth(field.data_type(), depth + 1),
        DataType::Struct(fields) => {
            for field in fields {
                validate_type_depth(field.data_type(), depth + 1)?;
            }
            Ok(())
        }
        DataType::Union(fields, _) => {
            for (_, field) in fields.iter() {
                validate_type_depth(field.data_type(), depth + 1)?;
            }
            Ok(())
        }
        DataType::Dictionary(key, value) => {
            validate_type_depth(key, depth + 1)?;
            validate_type_depth(value, depth + 1)
        }
        DataType::RunEndEncoded(run_ends, values) => {
            validate_type_depth(run_ends.data_type(), depth + 1)?;
            validate_type_depth(values.data_type(), depth + 1)
        }
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct SourceProbe {
        remaining: usize,
        calls: usize,
        borrowed: Option<(*const u8, usize)>,
        saw_borrowed: bool,
    }

    impl fmt::Write for SourceProbe {
        fn write_str(&mut self, value: &str) -> fmt::Result {
            self.calls += 1;
            if self.borrowed == Some((value.as_ptr(), value.len())) {
                self.saw_borrowed = true;
            }
            self.remaining = self.remaining.checked_sub(value.len()).ok_or(fmt::Error)?;
            Ok(())
        }
    }

    #[test]
    fn over_bound_literal_is_borrowed_and_binary_emission_stops_without_whole_value_encoding() {
        let text = "literal".repeat(4096);
        let pointer = (text.as_ptr(), text.len());
        let literal = TypedExpr {
            kind: ExprKind::Literal(LiteralValue::String(text)),
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Utf8, false),
        };
        let mut probe = SourceProbe {
            remaining: 8,
            calls: 0,
            borrowed: Some(pointer),
            saw_borrowed: false,
        };
        assert!(write!(&mut probe, "{}", Expression(&literal, None)).is_err());
        assert!(
            probe.saw_borrowed,
            "the source literal must reach the writer without an owned rendering"
        );
        assert!(
            probe.calls <= 3,
            "refusal must stop before emitting trailing fragments"
        );
        let binary = TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Binary(vec![0xab; 4096])),
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Binary, false),
        };
        let mut probe = SourceProbe {
            remaining: 8,
            calls: 0,
            borrowed: None,
            saw_borrowed: false,
        };
        assert!(write!(&mut probe, "{}", Expression(&binary, None)).is_err());
        assert!(
            probe.calls < 16,
            "the binary source must stop after a finite emitted prefix"
        );
    }
}
