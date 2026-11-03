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

use crate::compiler::SqlCompileError;
use novarocks_type_contract::{CompileCheckpoints, CompilePhase, PureCompileControl};

use crate::common::CteId;
use crate::common::{JoinKind, OutputColumn};
use crate::optimizer::operator::{Operator, ProjectOp, ScalarProjectItem};
use crate::optimizer::opt_expr::OptExpr;
use crate::optimizer::scalar::{ColumnDisplay, ScalarArena, ScalarNode};
use std::collections::{HashMap, HashSet};

#[derive(Clone, Debug, Default)]
pub(crate) struct CTEContext {
    pub produces: HashSet<CteId>,
    pub consume_count: HashMap<CteId, usize>,
}

// One counter belongs to the complete invocation, including replacement
// traversal and output adaptation. It observes the caller's policy; it is not
// a separate work allowance or memory wallet.
struct CteWork<'a> {
    control: &'a dyn PureCompileControl,
    checkpoints: CompileCheckpoints<'a>,
}
impl<'a> CteWork<'a> {
    fn try_new(control: &'a dyn PureCompileControl) -> Result<Self, SqlCompileError> {
        Ok(Self {
            control,
            checkpoints: CompileCheckpoints::try_new(control, CompilePhase::Validate)?,
        })
    }
    fn step(&mut self) -> Result<(), SqlCompileError> {
        self.checkpoints.step().map_err(SqlCompileError::from)
    }
    fn finish(self) -> Result<(), SqlCompileError> {
        self.checkpoints.finish().map_err(SqlCompileError::from)
    }
    // Nested Arrow comparisons/operator payload clones remain opaque
    // owner operations. These observations do not prove internal cooperation.
    fn opaque<T>(&mut self, operation: impl FnOnce() -> T) -> Result<T, SqlCompileError> {
        self.control.checkpoint(CompilePhase::Validate, 0)?;
        let value = operation();
        self.control.checkpoint(CompilePhase::Validate, 0)?;
        self.step()?;
        Ok(value)
    }
}

pub(crate) fn collect_cte_counts(
    expr: &OptExpr,
    control: &dyn PureCompileControl,
) -> Result<CTEContext, SqlCompileError> {
    let mut work = CteWork::try_new(control)?;
    let mut ctx = CTEContext::default();
    let mut pending = vec![expr];
    while let Some(expr) = pending.pop() {
        work.step()?;
        match &expr.op {
            Operator::LogicalCTEAnchor(node) => {
                ctx.produces.insert(node.cte_id);
            }
            Operator::LogicalCTEConsume(node) => {
                let count = ctx.consume_count.entry(node.cte_id).or_insert(0);
                *count = count
                    .checked_add(1)
                    .ok_or(SqlCompileError::ResourceExhausted)?;
                // A consume is a leaf in the existing count contract.
                continue;
            }
            Operator::LogicalImvDelta(_) | Operator::LogicalImvVersion(_) => {
                return Err(SqlCompileError::Compilation(
                    "imv marker leaked into non-IMV plan".to_string(),
                ));
            }
            _ => {}
        }
        // Reverse push preserves the original left-to-right visit order.
        for child in expr.children.iter().rev() {
            work.step()?;
            pending.push(child);
        }
    }
    work.finish()?;
    Ok(ctx)
}

pub(crate) fn inline_single_use_ctes(
    expr: OptExpr,
    ctx: &CTEContext,
    scalars: &mut ScalarArena,
    control: &dyn PureCompileControl,
) -> Result<OptExpr, SqlCompileError> {
    let mut work = CteWork::try_new(control)?;
    let output = inline_ctes(expr, ctx, scalars, &mut work)?;
    work.finish()?;
    Ok(output)
}

fn inline_ctes(
    mut expr: OptExpr,
    ctx: &CTEContext,
    scalars: &mut ScalarArena,
    work: &mut CteWork<'_>,
) -> Result<OptExpr, SqlCompileError> {
    work.step()?;
    match &expr.op {
        Operator::LogicalCTEAnchor(node) => {
            let cte_id = node.cte_id;
            let mut children = std::mem::take(&mut expr.children);
            let produce = inline_ctes(children.remove(0), ctx, scalars, work)?;
            let consumer = inline_ctes(children.remove(0), ctx, scalars, work)?;
            let consume_count = ctx.consume_count.get(&cte_id).copied().unwrap_or(0);
            // Preserve the existing single-use versus MultiCast decision.
            if ctx.produces.contains(&cte_id) && consume_count <= 1 {
                let produce_input = if matches!(&produce.op,
                    Operator::LogicalCTEProduce(node) if node.cte_id == cte_id)
                {
                    into_single_child(produce)
                } else {
                    produce
                };
                replace_cte_consume(consumer, cte_id, &produce_input, scalars, work)
            } else {
                expr.children = vec![produce, consumer];
                Ok(expr)
            }
        }
        Operator::LogicalImvDelta(_) | Operator::LogicalImvVersion(_) => Err(
            SqlCompileError::Compilation("imv marker leaked into non-IMV plan".to_string()),
        ),
        _ => {
            let mut children = Vec::with_capacity(expr.children.len());
            for child in std::mem::take(&mut expr.children) {
                children.push(inline_ctes(child, ctx, scalars, work)?);
            }
            expr.children = children;
            Ok(expr)
        }
    }
}

fn clone_replacement(expr: &OptExpr, work: &mut CteWork<'_>) -> Result<OptExpr, SqlCompileError> {
    work.step()?;
    let op = work.opaque(|| expr.op.clone())?;
    let required_output_columns = if let Some(columns) = &expr.required_output_columns {
        let mut cloned = HashSet::with_capacity(columns.len());
        for column in columns {
            work.step()?;
            cloned.insert(*column);
        }
        Some(cloned)
    } else {
        None
    };
    let mut children = Vec::with_capacity(expr.children.len());
    for child in &expr.children {
        children.push(clone_replacement(child, work)?);
    }
    Ok(OptExpr {
        op,
        children,
        required_output_columns,
    })
}

fn replace_cte_consume(
    mut expr: OptExpr,
    cte_id: CteId,
    replacement: &OptExpr,
    scalars: &mut ScalarArena,
    work: &mut CteWork<'_>,
) -> Result<OptExpr, SqlCompileError> {
    work.step()?;
    match &expr.op {
        Operator::LogicalCTEConsume(node) if node.cte_id == cte_id => {
            work.opaque(|| node.validate_mapping())?
                .map_err(SqlCompileError::Compilation)?;
            adapt_cte_replacement_output_with_qualifier(
                clone_replacement(replacement, work)?,
                &node.output_columns,
                &node.producer_column_ids,
                Some(&node.alias),
                scalars,
                work,
            )
        }
        Operator::LogicalCTEConsume(_) => Ok(expr),
        Operator::LogicalImvDelta(_) | Operator::LogicalImvVersion(_) => Err(
            SqlCompileError::Compilation("imv marker leaked into non-IMV plan".to_string()),
        ),
        _ => {
            let mut children = Vec::with_capacity(expr.children.len());
            for child in std::mem::take(&mut expr.children) {
                children.push(replace_cte_consume(
                    child,
                    cte_id,
                    replacement,
                    scalars,
                    work,
                )?);
            }
            expr.children = children;
            Ok(expr)
        }
    }
}

fn into_single_child(mut expr: OptExpr) -> OptExpr {
    assert_eq!(expr.children.len(), 1, "expected one logical plan child");
    expr.children.remove(0)
}

fn clone_columns(
    columns: &[OutputColumn],
    work: &mut CteWork<'_>,
) -> Result<Vec<OutputColumn>, SqlCompileError> {
    let mut output = Vec::with_capacity(columns.len());
    for column in columns {
        output.push(work.opaque(|| column.clone())?);
    }
    Ok(output)
}

fn opt_expr_output_columns(
    expr: &OptExpr,
    scalars: &ScalarArena,
    work: &mut CteWork<'_>,
) -> Result<Vec<OutputColumn>, SqlCompileError> {
    work.step()?;
    match &expr.op {
        Operator::LogicalScan(node) => clone_columns(&node.columns, work),
        Operator::LogicalFilter(_)
        | Operator::LogicalSort(_)
        | Operator::LogicalLimit(_)
        | Operator::LogicalTopN(_)
        | Operator::LogicalRepeat(_)
        | Operator::LogicalAssertOneRow(_) => {
            opt_expr_output_columns(expr.unary_input(), scalars, work)
        }
        Operator::LogicalProject(node) => {
            let mut columns = Vec::with_capacity(node.items.len());
            for item in &node.items {
                columns.push(work.opaque(|| OutputColumn {
                    column_id: item.output_column_id,
                    name: item.output_name.clone(),
                    value_type: scalars.value_type(item.expr).clone(),

                    is_internal: false,
                })?);
            }
            Ok(columns)
        }
        Operator::LogicalAggregate(node) => clone_columns(&node.output_columns, work),
        Operator::LogicalJoin(node) => {
            let left = opt_expr_output_columns(expr.left(), scalars, work)?;
            let right = opt_expr_output_columns(expr.right(), scalars, work)?;
            join_output_columns(node.join_type, left, right, work)
        }
        Operator::LogicalUnion(node) => clone_columns(&node.output_columns, work),
        Operator::LogicalIntersect(node) => clone_columns(&node.output_columns, work),
        Operator::LogicalExcept(node) => clone_columns(&node.output_columns, work),
        Operator::LogicalValues(node) => clone_columns(&node.columns, work),
        Operator::LogicalGenerateSeries(node) => work.opaque(|| {
            vec![OutputColumn {
                column_id: node.output_column_id,
                name: node.column_name.clone(),
                value_type: novarocks_type_contract::FunctionValueType::new(
                    arrow::datatypes::DataType::Int64,
                    false,
                ),

                is_internal: false,
            }]
        }),
        Operator::LogicalTableFunction(node) => {
            let mut columns = opt_expr_output_columns(expr.unary_input(), scalars, work)?;
            for column in &node.output_columns {
                columns.push(work.opaque(|| column.clone())?);
            }
            Ok(columns)
        }
        Operator::LogicalWindow(node) => clone_columns(&node.output_columns, work),
        Operator::LogicalCTEAnchor(_) => opt_expr_output_columns(expr.child(1), scalars, work),
        Operator::LogicalCTEProduce(node) => clone_columns(&node.output_columns, work),
        Operator::LogicalCTEConsume(node) => clone_columns(&node.output_columns, work),
        Operator::LogicalApply(node) => {
            let mut columns = opt_expr_output_columns(expr.left(), scalars, work)?;
            columns.push(work.opaque(|| node.output_column.clone())?);
            Ok(columns)
        }
        Operator::LogicalImvDelta(_) | Operator::LogicalImvVersion(_) => {
            Err(SqlCompileError::Compilation(
                "imv marker leaked into non-IMV planner output adaptation".to_string(),
            ))
        }
        other => Err(SqlCompileError::Compilation(format!(
            "physical operator leaked into CTE output adaptation: {other:?}"
        ))),
    }
}

fn join_output_columns(
    join_type: JoinKind,
    mut left: Vec<OutputColumn>,
    mut right: Vec<OutputColumn>,
    work: &mut CteWork<'_>,
) -> Result<Vec<OutputColumn>, SqlCompileError> {
    match join_type {
        JoinKind::LeftMark { .. } => {
            let mut output = left;
            output.push(join_type.mark_output().expect("mark output"));
            return Ok(output);
        }
        JoinKind::LeftSemi | JoinKind::LeftAnti | JoinKind::NullAwareLeftAnti => return Ok(left),
        JoinKind::RightSemi | JoinKind::RightAnti => return Ok(right),
        JoinKind::LeftOuter => make_nullable(&mut right, work)?,
        JoinKind::RightOuter => make_nullable(&mut left, work)?,
        JoinKind::FullOuter => {
            make_nullable(&mut left, work)?;
            make_nullable(&mut right, work)?;
        }
        JoinKind::Inner | JoinKind::Cross => {}
    }
    for column in right {
        work.step()?;
        left.push(column);
    }
    Ok(left)
}
fn make_nullable(
    columns: &mut [OutputColumn],
    work: &mut CteWork<'_>,
) -> Result<(), SqlCompileError> {
    for column in columns {
        work.step()?;
        column.value_type = column
            .value_type
            .with_nullable(true)
            .expect("validated root NULL contract");
    }
    Ok(())
}

#[allow(
    dead_code,
    reason = "Retained for staged SQL planner migration consumers and test helpers."
)]
fn adapt_opt_expr_output_with_qualifier(
    input: OptExpr,
    target_output_columns: &[OutputColumn],
    output_qualifier: Option<&str>,
    scalars: &mut ScalarArena,
    work: &mut CteWork<'_>,
) -> Result<OptExpr, SqlCompileError> {
    let source_output_columns = opt_expr_output_columns(&input, scalars, work)?;
    if source_output_columns.len() != target_output_columns.len() {
        return Err(SqlCompileError::Compilation(format!(
            "output column count mismatch while adapting subquery/CTE output: child has {}, target has {}",
            source_output_columns.len(),
            target_output_columns.len()
        )));
    }

    let mut metadata_equal = true;
    for (source, target) in source_output_columns.iter().zip(target_output_columns) {
        work.step()?;
        if !work.opaque(|| output_column_metadata_equal(source, target))? {
            metadata_equal = false;
            break;
        }
    }
    if metadata_equal && output_qualifier.is_none() {
        return Ok(input);
    }

    let mut items = Vec::with_capacity(target_output_columns.len());
    for (source, target) in source_output_columns
        .iter()
        .zip(target_output_columns.iter())
    {
        work.step()?;
        if work.opaque(|| source.value_type.data_type() != target.value_type.data_type())? {
            return Err(SqlCompileError::Compilation(format!(
                "output type mismatch while adapting subquery/CTE column '{}': child={:?}, target={:?}",
                target.name,
                source.value_type.data_type(),
                target.value_type.data_type()
            )));
        }
        if source.value_type.nullable() && !target.value_type.nullable() {
            return Err(SqlCompileError::Compilation(format!(
                "output nullability mismatch while adapting subquery/CTE column '{}': child={}, target={}",
                target.name,
                source.value_type.nullable(),
                target.value_type.nullable()
            )));
        }
        work.opaque(|| {
            scalars.remember_source_column_display(source.column_id, None, source.name.clone())
        })?;
        work.checkpoints.flush()?;
        let expr = scalars.intern_observed(
            ScalarNode::ColumnRef(source.column_id),
            (source.value_type.clone())
                .with_nullable(target.value_type.nullable())
                .expect("valid value root nullability transform"),
            work.control,
        )?;
        items.push(work.opaque(|| {
            let expr_display = Some(ColumnDisplay {
                qualifier: None,
                column: source.name.clone(),
            });
            scalars.remember_project_output_display(target.column_id, None, target.name.clone());
            ScalarProjectItem {
                expr,
                output_name: target.name.clone(),
                output_column_id: target.column_id,
                expr_display,
            }
        })?);
    }

    Ok(OptExpr::new(
        Operator::LogicalProject(ProjectOp {
            items,
            output_qualifier: output_qualifier.map(str::to_string),
        }),
        vec![input],
    ))
}

fn adapt_cte_replacement_output_with_qualifier(
    input: OptExpr,
    target_output_columns: &[OutputColumn],
    producer_column_ids: &[crate::column_id::ColumnId],
    output_qualifier: Option<&str>,
    scalars: &mut ScalarArena,
    work: &mut CteWork<'_>,
) -> Result<OptExpr, SqlCompileError> {
    if target_output_columns.len() != producer_column_ids.len() {
        return Err(SqlCompileError::Compilation(format!(
            "CTE output/producers arity mismatch while adapting inline replacement: output has {}, producers has {}",
            target_output_columns.len(),
            producer_column_ids.len()
        )));
    }

    let source_output_columns = opt_expr_output_columns(&input, scalars, work)?;
    let mut items = Vec::with_capacity(target_output_columns.len());
    for (target, producer_column_id) in target_output_columns.iter().zip(producer_column_ids) {
        work.step()?;
        let mut matched = None;
        for source in &source_output_columns {
            work.step()?;
            if source.column_id == *producer_column_id {
                matched = Some(source);
                break;
            }
        }
        let source = matched
            .ok_or_else(|| {
                format!(
                    "CTE inline replacement missing producer column {} for output '{}'",
                    producer_column_id.0, target.name
                )
            })
            .map_err(SqlCompileError::Compilation)?;
        if work.opaque(|| source.value_type.data_type() != target.value_type.data_type())? {
            return Err(SqlCompileError::Compilation(format!(
                "output type mismatch while adapting subquery/CTE column '{}': child={:?}, target={:?}",
                target.name,
                source.value_type.data_type(),
                target.value_type.data_type()
            )));
        }
        if source.value_type.nullable() && !target.value_type.nullable() {
            return Err(SqlCompileError::Compilation(format!(
                "output nullability mismatch while adapting subquery/CTE column '{}': child={}, target={}",
                target.name,
                source.value_type.nullable(),
                target.value_type.nullable()
            )));
        }
        work.opaque(|| {
            scalars.remember_source_column_display(source.column_id, None, source.name.clone())
        })?;
        work.checkpoints.flush()?;
        let expr = scalars.intern_observed(
            ScalarNode::ColumnRef(source.column_id),
            (source.value_type.clone())
                .with_nullable(target.value_type.nullable())
                .expect("valid value root nullability transform"),
            work.control,
        )?;
        items.push(work.opaque(|| {
            let expr_display = Some(ColumnDisplay {
                qualifier: None,
                column: source.name.clone(),
            });
            scalars.remember_project_output_display(target.column_id, None, target.name.clone());
            ScalarProjectItem {
                expr,
                output_name: target.name.clone(),
                output_column_id: target.column_id,
                expr_display,
            }
        })?);
    }

    Ok(OptExpr::new(
        Operator::LogicalProject(ProjectOp {
            items,
            output_qualifier: output_qualifier.map(str::to_string),
        }),
        vec![input],
    ))
}

#[allow(
    dead_code,
    reason = "Retained for staged SQL planner migration consumers and test helpers."
)]
fn output_column_metadata_equal(left: &OutputColumn, right: &OutputColumn) -> bool {
    left.column_id == right.column_id
        && left.name == right.name
        && left.value_type.data_type() == right.value_type.data_type()
        && left.value_type.nullable() == right.value_type.nullable()
        && left.is_internal == right.is_internal
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis::{ExprKind, OutputColumn, TypedExpr};
    use crate::column_id::ColumnId;
    use crate::optimizer::operator::{
        CTEAnchorOp, CTEConsumeOp, CTEProduceOp, Operator, ScanOp, UnionOp, ValuesOp,
    };
    use crate::optimizer::opt_expr::OptExpr;
    use crate::optimizer::scalar::ScalarArena;
    use crate::planner::table::TableDef;
    use arrow::datatypes::DataType;
    use novarocks_types::schema::ColumnDef;

    fn test_control() -> &'static dyn PureCompileControl {
        crate::optimizer::rewrite::context::unbounded_rewrite_test_control()
    }

    fn scan_plan() -> OptExpr {
        OptExpr::leaf(Operator::LogicalScan(ScanOp {
            database: "db".to_string(),
            table: TableDef {
                name: "t1".to_string(),
                columns: vec![ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                }],
                iceberg_row_lineage_metadata_columns: vec![],
                source: crate::compiler::mv_rewrite::test_scan_source(
                    crate::planner::table::SqlScanKind::ConnectorRead,
                ),
            },
            alias: None,
            stats_ref: None,
            columns: vec![OutputColumn {
                column_id: ColumnId::new_for_test(1),
                name: "id".to_string(),
                value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int32, false),

                is_internal: false,
            }],
            predicates: vec![],
            required_columns: None,
            variant_columns: vec![],
            mv_rewritten_from: None,
        }))
    }

    fn output_columns() -> Vec<OutputColumn> {
        vec![OutputColumn {
            column_id: ColumnId::new_for_test(1),
            name: "id".to_string(),
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int32, false),

            is_internal: false,
        }]
    }

    fn output_columns_with_id_and_name(column_id: ColumnId, name: &str) -> Vec<OutputColumn> {
        vec![OutputColumn {
            column_id,
            name: name.to_string(),
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int32, false),

            is_internal: false,
        }]
    }

    fn consume_plan(cte_id: CteId, alias: &str) -> OptExpr {
        let output_columns = output_columns();
        OptExpr::leaf(Operator::LogicalCTEConsume(CTEConsumeOp {
            cte_id,
            alias: alias.to_string(),
            producer_column_ids: output_columns.iter().map(|c| c.column_id).collect(),
            output_columns,
        }))
    }

    fn consume_plan_with_output_columns(
        cte_id: CteId,
        alias: &str,
        output_columns: Vec<OutputColumn>,
        producer_column_ids: Vec<ColumnId>,
    ) -> OptExpr {
        OptExpr::leaf(Operator::LogicalCTEConsume(CTEConsumeOp {
            cte_id,
            alias: alias.to_string(),
            producer_column_ids,
            output_columns,
        }))
    }

    fn cte_produce(cte_id: CteId, input: OptExpr) -> OptExpr {
        OptExpr::new(
            Operator::LogicalCTEProduce(CTEProduceOp {
                cte_id,
                output_columns: output_columns(),
            }),
            vec![input],
        )
    }

    fn cte_anchor(cte_id: CteId, produce: OptExpr, consumer: OptExpr) -> OptExpr {
        OptExpr::new(
            Operator::LogicalCTEAnchor(CTEAnchorOp { cte_id }),
            vec![produce, consumer],
        )
    }

    fn union(children: Vec<OptExpr>) -> OptExpr {
        OptExpr::new(
            Operator::LogicalUnion(UnionOp {
                all: true,
                output_columns: vec![],
                child_output_columns: vec![],
            }),
            children,
        )
    }

    fn scalar_arena() -> ScalarArena {
        ScalarArena::new()
    }

    fn opt_output_columns(
        plan: &OptExpr,
        arena: &ScalarArena,
    ) -> Result<Vec<OutputColumn>, String> {
        let mut memo = crate::optimizer::Memo::new();
        memo.scalars = arena.clone();
        let root_group =
            crate::optimizer::memo_copy::opt_expr_to_memo(plan, &mut memo, test_control())
                .map_err(|error| error.to_string())?;
        let stats_input =
            crate::optimizer::stats_input::OptimizerStatsInput::from_test_table_statistics(
                &HashMap::new(),
            );
        crate::optimizer::stats::derive_group_statistics(&mut memo, &stats_input, test_control())
            .map_err(|error| error.to_string())?;
        Ok(memo.groups[root_group]
            .logical_props
            .as_ref()
            .expect("logical properties should be derived")
            .output_columns
            .clone())
    }

    fn column_ref(column: &OutputColumn) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: column.column_id,
                qualifier: None,
                column: column.name.clone(),
            },
            value_type: column.value_type.clone(),
        }
    }

    #[test]
    fn test_collect_cte_counts_counts_consumes() {
        let plan = cte_anchor(1, cte_produce(1, scan_plan()), consume_plan(1, "t"));

        let ctx = collect_cte_counts(&plan, test_control()).unwrap();
        assert!(ctx.produces.contains(&1));
        assert_eq!(ctx.consume_count.get(&1), Some(&1));
    }

    #[test]
    fn test_inline_single_use_cte_removes_anchor_without_alias_node() {
        let plan = cte_anchor(1, cte_produce(1, scan_plan()), consume_plan(1, "t"));

        let ctx = collect_cte_counts(&plan, test_control()).unwrap();
        let mut arena = scalar_arena();
        let rewritten = inline_single_use_ctes(plan, &ctx, &mut arena, test_control())
            .expect("inline should succeed");
        assert!(matches!(
            &rewritten.op,
            Operator::LogicalScan(_) | Operator::LogicalProject(_)
        ));
    }

    #[test]
    fn test_inline_single_use_cte_preserves_consumer_output_columns_with_project() {
        let consume_output_id = ColumnId::new_for_test(42);
        let consume_output_columns = output_columns_with_id_and_name(consume_output_id, "x_id");
        let plan = cte_anchor(
            1,
            cte_produce(1, scan_plan()),
            consume_plan_with_output_columns(
                1,
                "x",
                consume_output_columns.clone(),
                vec![output_columns()[0].column_id],
            ),
        );

        let ctx = collect_cte_counts(&plan, test_control()).unwrap();
        let mut arena = scalar_arena();
        let rewritten = inline_single_use_ctes(plan, &ctx, &mut arena, test_control())
            .expect("inline should succeed");

        let output = opt_output_columns(&rewritten, &arena)
            .expect("rewritten output columns should be derivable");
        assert_eq!(output.len(), consume_output_columns.len());
        assert_eq!(output[0].column_id, consume_output_columns[0].column_id);
        assert_eq!(output[0].name, consume_output_columns[0].name);
        assert_eq!(
            output[0].value_type.data_type(),
            consume_output_columns[0].value_type.data_type()
        );
        assert_eq!(
            output[0].value_type.nullable(),
            consume_output_columns[0].value_type.nullable()
        );
        let Operator::LogicalProject(project) = &rewritten.op else {
            panic!("expected Project adapter");
        };
        assert_eq!(project.items[0].output_name, "x_id");
        assert_eq!(project.items[0].output_column_id, consume_output_id);
        let materialized =
            crate::planner::optimizer_bridge::scalar::materialize(&arena, project.items[0].expr);
        let expected = column_ref(&output_columns()[0]);
        assert_eq!(
            materialized.value_type.data_type(),
            expected.value_type.data_type()
        );
        assert_eq!(
            materialized.value_type.nullable(),
            expected.value_type.nullable()
        );
        let ExprKind::ColumnRef {
            column_id,
            qualifier,
            column,
        } = materialized.kind
        else {
            panic!("expected ColumnRef project expression");
        };
        assert_eq!(column_id, output_columns()[0].column_id);
        assert!(qualifier.is_none());
        assert_eq!(column, output_columns()[0].name);
    }

    #[test]
    fn test_inline_single_use_cte_projects_consumer_mapping_when_producer_keeps_extra_columns() {
        let producer_sort_column = OutputColumn {
            column_id: ColumnId::new_for_test(10),
            name: "sort_key".to_string(),
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int32, false),

            is_internal: false,
        };
        let producer_join_column = OutputColumn {
            column_id: ColumnId::new_for_test(11),
            name: "join_key".to_string(),
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int32, false),

            is_internal: false,
        };
        let consumer_join_column = OutputColumn {
            column_id: ColumnId::new_for_test(42),
            name: "join_key".to_string(),
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int32, false),

            is_internal: false,
        };
        let produce_input = OptExpr::leaf(Operator::LogicalValues(ValuesOp {
            rows: vec![],
            columns: vec![producer_sort_column.clone(), producer_join_column.clone()],
        }));
        let produce = OptExpr::new(
            Operator::LogicalCTEProduce(CTEProduceOp {
                cte_id: 1,
                output_columns: vec![producer_sort_column, producer_join_column.clone()],
            }),
            vec![produce_input],
        );
        let consume = OptExpr::leaf(Operator::LogicalCTEConsume(CTEConsumeOp {
            cte_id: 1,
            alias: "w1".to_string(),
            output_columns: vec![consumer_join_column.clone()],
            producer_column_ids: vec![producer_join_column.column_id],
        }));
        let plan = cte_anchor(1, produce, consume);

        let ctx = collect_cte_counts(&plan, test_control()).unwrap();
        let mut arena = scalar_arena();
        let rewritten = inline_single_use_ctes(plan, &ctx, &mut arena, test_control())
            .expect("inline should succeed");

        let Operator::LogicalProject(project) = &rewritten.op else {
            panic!("expected Project adapter");
        };
        assert_eq!(project.items.len(), 1);
        assert_eq!(
            project.items[0].output_column_id,
            consumer_join_column.column_id
        );

        let materialized =
            crate::planner::optimizer_bridge::scalar::materialize(&arena, project.items[0].expr);
        let ExprKind::ColumnRef { column_id, .. } = materialized.kind else {
            panic!("expected producer ColumnRef");
        };
        assert_eq!(column_id, producer_join_column.column_id);

        let output = opt_output_columns(&rewritten, &arena)
            .expect("rewritten output columns should be derivable");
        assert_eq!(output.len(), 1);
        assert_eq!(output[0].column_id, consumer_join_column.column_id);
        assert_eq!(output[0].name, consumer_join_column.name);
        assert_eq!(
            output[0].value_type.data_type(),
            consumer_join_column.value_type.data_type()
        );
        assert_eq!(
            output[0].value_type.nullable(),
            consumer_join_column.value_type.nullable()
        );
        assert_eq!(output[0].is_internal, consumer_join_column.is_internal);
    }

    #[test]
    fn test_inline_single_use_cte_keeps_multi_use_anchor() {
        let plan = cte_anchor(
            1,
            cte_produce(1, scan_plan()),
            union(vec![consume_plan(1, "t1"), consume_plan(1, "t2")]),
        );

        let ctx = collect_cte_counts(&plan, test_control()).unwrap();
        assert_eq!(ctx.consume_count.get(&1), Some(&2));

        let mut arena = scalar_arena();
        let rewritten = inline_single_use_ctes(plan, &ctx, &mut arena, test_control())
            .expect("inline should succeed");
        assert!(matches!(&rewritten.op, Operator::LogicalCTEAnchor(_)));
    }

    #[test]
    fn test_inline_single_use_cte_inlines_nested_cte_inside_later_produce() {
        let plan = cte_anchor(
            1,
            cte_produce(1, scan_plan()),
            cte_anchor(
                2,
                cte_produce(
                    2,
                    cte_anchor(1, cte_produce(1, scan_plan()), consume_plan(1, "a")),
                ),
                union(vec![consume_plan(2, "b1"), consume_plan(2, "b2")]),
            ),
        );

        let ctx = collect_cte_counts(&plan, test_control()).unwrap();
        assert_eq!(ctx.consume_count.get(&1), Some(&1));
        assert_eq!(ctx.consume_count.get(&2), Some(&2));

        let mut arena = scalar_arena();
        let rewritten = inline_single_use_ctes(plan, &ctx, &mut arena, test_control())
            .expect("inline should succeed");

        match &rewritten.op {
            Operator::LogicalCTEAnchor(anchor) => {
                assert_eq!(anchor.cte_id, 2);
                let produce_plan = rewritten.child(0);
                match &produce_plan.op {
                    Operator::LogicalCTEProduce(_) => match &produce_plan.unary_input().op {
                        Operator::LogicalScan(_) | Operator::LogicalProject(_) => {}
                        other => panic!("expected nested inline replacement, got {other:?}"),
                    },
                    other => panic!("expected CTEProduce for b, got {other:?}"),
                }
                assert!(matches!(&rewritten.child(1).op, Operator::LogicalUnion(_)));
            }
            other => panic!("expected surviving anchor for b, got {other:?}"),
        }
    }

    #[test]
    fn test_replace_cte_consume_only_rewrites_targeted_cte_id() {
        let plan = cte_anchor(
            2,
            cte_produce(2, scan_plan()),
            union(vec![consume_plan(1, "target"), consume_plan(2, "shadow")]),
        );

        let mut arena = scalar_arena();
        let mut work = CteWork::try_new(test_control()).unwrap();
        let rewritten = replace_cte_consume(plan, 1, &scan_plan(), &mut arena, &mut work)
            .expect("replace should succeed");
        work.finish().unwrap();

        match &rewritten.op {
            Operator::LogicalCTEAnchor(_) => match &rewritten.child(1).op {
                Operator::LogicalUnion(_) => {
                    let union_plan = rewritten.child(1);
                    match &union_plan.child(0).op {
                        Operator::LogicalScan(_) | Operator::LogicalProject(_) => {}
                        other => panic!("expected targeted consume to be rewritten, got {other:?}"),
                    }
                    assert!(matches!(
                        &union_plan.child(1).op,
                        Operator::LogicalCTEConsume(_)
                    ));
                }
                other => panic!("expected union consumer, got {other:?}"),
            },
            other => panic!("expected outer anchor, got {other:?}"),
        }
    }

    #[derive(Clone, Copy)]
    enum StopPoint {
        Entry,
        Batch(usize),
        Finish,
    }
    struct ObservedControl {
        units: std::sync::Mutex<Vec<u32>>,
        stop: Option<(StopPoint, novarocks_type_contract::CompileControlError)>,
    }
    impl PureCompileControl for ObservedControl {
        fn checkpoint(
            &self,
            _: CompilePhase,
            units: u32,
        ) -> Result<(), novarocks_type_contract::CompileControlError> {
            let mut observations = self.units.lock().unwrap();
            observations.push(units);
            if let Some((point, error)) = self.stop.clone() {
                let stop = match point {
                    StopPoint::Entry => units == 0,
                    StopPoint::Batch(ordinal) => {
                        units == 256
                            && observations.iter().filter(|units| **units == 256).count() == ordinal
                    }
                    StopPoint::Finish => units > 0 && units < 256,
                };
                if stop {
                    return Err(error);
                }
            }
            Ok(())
        }
    }
    fn observed_control(
        stop: Option<(StopPoint, novarocks_type_contract::CompileControlError)>,
    ) -> ObservedControl {
        ObservedControl {
            units: Default::default(),
            stop,
        }
    }
    fn wide_multi_consume() -> OptExpr {
        cte_anchor(
            1,
            cte_produce(1, scan_plan()),
            union(
                (0..320)
                    .map(|ordinal| consume_plan(1, &format!("c{ordinal}")))
                    .collect(),
            ),
        )
    }
    #[test]
    fn wide_cte_counts_and_inline_visit_all_actual_nodes_and_edges() {
        let control = observed_control(None);
        let plan = wide_multi_consume();
        let ctx = collect_cte_counts(&plan, &control).unwrap();
        assert_eq!(ctx.consume_count.get(&1), Some(&320));
        assert_eq!(*control.units.lock().unwrap(), vec![0, 256, 256, 135]);
        let control = observed_control(None);
        let rewritten = inline_single_use_ctes(plan, &ctx, &mut scalar_arena(), &control).unwrap();
        assert!(matches!(rewritten.op, Operator::LogicalCTEAnchor(_)));
        assert_eq!(rewritten.child(1).children.len(), 320);
        assert_eq!(*control.units.lock().unwrap(), vec![0, 256, 68]);
    }
    #[test]
    fn cte_count_and_inline_keep_three_control_categories_at_all_boundaries() {
        use novarocks_type_contract::CompileControlError as Error;
        let ctx = collect_cte_counts(&wide_multi_consume(), test_control()).unwrap();
        for error in [
            Error::Cancelled,
            Error::DeadlineExceeded,
            Error::ResourceExhausted,
        ] {
            for point in [StopPoint::Entry, StopPoint::Batch(1), StopPoint::Finish] {
                let control = observed_control(Some((point, error.clone())));
                let failure = collect_cte_counts(&wide_multi_consume(), &control).unwrap_err();
                assert_eq!(failure, SqlCompileError::from(error.clone()));
                assert!(
                    control
                        .units
                        .lock()
                        .unwrap()
                        .iter()
                        .all(|units| *units <= 256)
                );
                let control = observed_control(Some((point, error.clone())));
                let failure = inline_single_use_ctes(
                    wide_multi_consume(),
                    &ctx,
                    &mut scalar_arena(),
                    &control,
                )
                .unwrap_err();
                assert_eq!(failure, SqlCompileError::from(error.clone()));
                let units = control.units.lock().unwrap();
                assert!(units.iter().all(|units| *units <= 256));
                if matches!(point, StopPoint::Entry) {
                    assert_eq!(*units, vec![0]);
                }
                if matches!(point, StopPoint::Batch(1)) {
                    assert_eq!(*units, vec![0, 256]);
                }
            }
        }
    }
    #[test]
    fn cte_replacement_clone_observes_control_inside_actual_child_traversal() {
        use novarocks_type_contract::CompileControlError as Error;
        for error in [
            Error::Cancelled,
            Error::DeadlineExceeded,
            Error::ResourceExhausted,
        ] {
            let input = OptExpr::new(
                Operator::LogicalUnion(UnionOp {
                    all: true,
                    output_columns: output_columns(),
                    child_output_columns: vec![output_columns(); 320],
                }),
                (0..320).map(|_| scan_plan()).collect(),
            );
            let plan = cte_anchor(1, cte_produce(1, input), consume_plan(1, "c"));
            let ctx = collect_cte_counts(&plan, test_control()).unwrap();
            // The first 256-work checkpoint is in normal inlining; the
            // second is in the actual replacement clone's child traversal.
            let control = observed_control(Some((StopPoint::Batch(2), error.clone())));
            let failure =
                inline_single_use_ctes(plan, &ctx, &mut scalar_arena(), &control).unwrap_err();
            assert_eq!(failure, SqlCompileError::from(error));
            assert_eq!(
                control
                    .units
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|units| **units == 256)
                    .count(),
                2
            );
        }
    }
    #[test]
    fn cte_adaptation_observes_control_during_wide_output_mapping() {
        use novarocks_type_contract::CompileControlError as Error;
        let columns = (0..320)
            .map(|ordinal| OutputColumn {
                column_id: ColumnId::new_for_test(ordinal + 1),
                name: format!("v{ordinal}"),
                value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int32, false),

                is_internal: false,
            })
            .collect::<Vec<_>>();
        for error in [
            Error::Cancelled,
            Error::DeadlineExceeded,
            Error::ResourceExhausted,
        ] {
            let input = OptExpr::leaf(Operator::LogicalValues(ValuesOp {
                rows: vec![],
                columns: columns.clone(),
            }));
            let control = observed_control(Some((StopPoint::Batch(1), error.clone())));
            let mut work = CteWork::try_new(&control).unwrap();
            let failure = adapt_cte_replacement_output_with_qualifier(
                input,
                &columns,
                &columns
                    .iter()
                    .map(|column| column.column_id)
                    .collect::<Vec<_>>(),
                Some("c"),
                &mut scalar_arena(),
                &mut work,
            )
            .unwrap_err();
            assert_eq!(failure, SqlCompileError::from(error));
            assert_eq!(control.units.lock().unwrap().last(), Some(&256));
        }
    }
    #[test]
    fn cte_mapping_semantic_failure_stays_compilation() {
        let plan = cte_anchor(
            1,
            cte_produce(1, scan_plan()),
            consume_plan_with_output_columns(1, "c", output_columns(), vec![]),
        );
        let ctx = collect_cte_counts(&plan, test_control()).unwrap();
        let failure =
            inline_single_use_ctes(plan, &ctx, &mut scalar_arena(), test_control()).unwrap_err();
        assert_eq!(
            failure,
            SqlCompileError::Compilation(
                "CTEConsume output/producers arity mismatch for cte_id=1".to_string()
            )
        );
    }
    #[test]
    fn count_and_inline_products_do_not_retain_the_borrowed_control() {
        let owner = std::sync::Arc::new(observed_control(None));
        let weak = std::sync::Arc::downgrade(&owner);
        let plan = cte_anchor(1, cte_produce(1, scan_plan()), consume_plan(1, "c"));
        let ctx = collect_cte_counts(&plan, owner.as_ref()).unwrap();
        let output =
            inline_single_use_ctes(plan, &ctx, &mut scalar_arena(), owner.as_ref()).unwrap();
        drop(owner);
        assert!(weak.upgrade().is_none());
        assert_eq!(ctx.consume_count.get(&1), Some(&1));
        assert!(matches!(output.op, Operator::LogicalProject(_)));
    }

    #[test]
    fn cte_actual_replacement_insertion_propagates_original_control_without_publishing_id() {
        use novarocks_type_contract::CompileControlError;
        struct RefuseSecondPositive {
            error: CompileControlError,
            observations: std::sync::Mutex<Vec<u32>>,
        }
        impl PureCompileControl for RefuseSecondPositive {
            fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
                let mut observations = self.observations.lock().unwrap();
                observations.push(units);
                if units > 0 && observations.iter().filter(|units| **units > 0).count() == 2 {
                    Err(self.error.clone())
                } else {
                    Ok(())
                }
            }
        }
        for error in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let columns = output_columns();
            let input = OptExpr::leaf(Operator::LogicalValues(ValuesOp {
                rows: vec![],
                columns: columns.clone(),
            }));
            let control = RefuseSecondPositive {
                error: error.clone(),
                observations: Default::default(),
            };
            let mut work = CteWork::try_new(&control).unwrap();
            let mut arena = scalar_arena();
            let failure = adapt_cte_replacement_output_with_qualifier(
                input,
                &columns,
                &[columns[0].column_id],
                Some("c"),
                &mut arena,
                &mut work,
            )
            .unwrap_err();
            assert_eq!(failure, SqlCompileError::from(error));
            // Mapping work is flushed before the actual interner starts;
            // its positive tail is the next observation that refuses.
            let observations = control.observations.lock().unwrap();
            assert!(observations.last().is_some_and(|units| *units > 0));
            assert_eq!(observations.iter().filter(|units| **units > 0).count(), 2);
            let first = arena.intern(
                ScalarNode::ColumnRef(columns[0].column_id),
                columns[0].value_type.clone(),
            );
            let expected = scalar_arena().intern(
                ScalarNode::ColumnRef(columns[0].column_id),
                columns[0].value_type.clone(),
            );
            assert_eq!(first, expected);
        }
    }
}
