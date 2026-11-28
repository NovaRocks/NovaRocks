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

//! `FoldConstant` — evaluate constant scalar sub-expressions at plan time.
//!
//! The rule walks every scalar field of every operator, folds bottom-up, and
//! replaces a node with a literal as soon as all of its children are already
//! literals and the node is *safe* to evaluate on the frontend.
//!
//! Each candidate and its constant children lower to a standalone scalar unit.
//! The result is cached by its arena input across operators and rewrite rounds.
//!
//! The rule runs in `LogicalNormalize`, before predicate pushdown, so that a
//! `Cast(Literal)` has already collapsed into a bare literal by the time
//! static-predicate extraction inspects the plan.

use crate::compiler::SqlCompileError;
use novarocks_type_contract::{CompileCheckpoints, CompilePhase, FunctionValueType};
use std::collections::HashMap;

use arrow::datatypes::DataType;

use crate::binding::SqlFunctionBinding;
use crate::common::{BinOp, LiteralValue, UnOp};
use crate::compiler::SqlAuthoredScalarUnit;
use novarocks_functions::PureCallLifecycle;

use crate::functions::FunctionVolatility;
use crate::optimizer::operator::Operator;
use crate::optimizer::opt_expr::OptExpr;
use crate::optimizer::rewrite::context::RewriteContext;
use crate::optimizer::rewrite::phase::RewritePhase;
use crate::optimizer::rewrite::result::RewriteResult;
use crate::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::optimizer::scalar::{HashableLiteral, ScalarArena, ScalarId, ScalarNode, SortKey};

mod admission;
mod evaluation;

/// Functions that are classified `Immutable` by the SQL function catalog but
/// whose execution kernel still reads the *host process* environment — the
/// session/process timezone or the wall clock.
///
/// Folding those on the frontend would silently move the environment read from
/// BE to FE, so the same query could produce a different answer depending on
/// which process happened to evaluate it. The list is intentionally
/// conservative: a name is denied whenever its kernel touches
/// `chrono::Local`, `chrono_tz`, or "now", even when only one argument shape
/// actually hits that path.
///
/// These functions consult the process clock or timezone for at least one
/// admitted argument shape.
///
const ENVIRONMENT_SENSITIVE_FUNCTIONS: &[&str] = &[
    "get_variant_date",
    "get_variant_datetime",
    "get_variant_time",
];

fn is_environment_sensitive_function(name: &str) -> bool {
    let lowered = name.to_ascii_lowercase();
    ENVIRONMENT_SENSITIVE_FUNCTIONS
        .iter()
        .any(|denied| *denied == lowered)
}

/// Output types a folded literal is allowed to have.
///
/// Temporal constants carry their complete declaration through the original
/// constant pool. The physical width alone grants no additional fold support.
fn is_wire_encodable_literal_type(value_type: &FunctionValueType) -> bool {
    if let Some(temporal) = value_type.value_type().temporal() {
        return value_type.data_type() == &temporal.carrier();
    }
    let data_type = value_type.data_type();
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::Date32
            | DataType::Decimal128(..)
            | DataType::Decimal256(..)
            | DataType::List(_)
            | DataType::LargeList(_)
            | DataType::Struct(_)
    ) || novarocks_types::largeint::is_largeint_data_type(data_type)
}

// Design: ADR-0242 (docs/adr/ADR-0242-constant-folding-compiles-expression-units.md)
pub(crate) struct FoldConstant;

impl LogicalRewriteRule for FoldConstant {
    fn name(&self) -> &'static str {
        "FoldConstant"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::LogicalNormalize
    }

    fn matches(&self, expr: &OptExpr, _ctx: &RewriteContext) -> bool {
        operator_has_scalars(&expr.op)
    }

    fn apply(
        &self,
        expr: OptExpr,
        ctx: &mut RewriteContext,
    ) -> Result<RewriteResult, SqlCompileError> {
        let control = ctx.control_view();
        let work = CompileCheckpoints::try_new(&control, CompilePhase::Validate)?;

        let OptExpr {
            mut op,
            children,
            required_output_columns,
        } = expr;

        let arena = ctx.scalar_arena();
        let changed = {
            let mut arena = arena.borrow_mut();
            let mut folder = ConstantFolder::new(&mut arena, ctx, work);
            let changed = folder.fold_operator(&mut op)?;
            folder.work.finish()?;
            changed
        };

        let expr = OptExpr {
            op,
            children,
            required_output_columns,
        };
        if changed {
            Ok(RewriteResult::Changed(expr))
        } else {
            Ok(RewriteResult::Unchanged)
        }
    }
}

// ---------------------------------------------------------------------------
// Scalar folding
// ---------------------------------------------------------------------------

fn admit_parent(
    ctx: &RewriteContext<'_>,
    binding: &SqlFunctionBinding,
    lifecycle: PureCallLifecycle,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<(), SqlCompileError> {
    let catalog = ctx.function_catalog();
    catalog.admit_bound_lifecycle_observed(binding.resolved(), lifecycle, control)?;
    catalog.admit_authored_environment_observed(binding, control)?;
    Ok(())
}

/// Traverse shared scalar nodes once per operator. Evaluation outcomes have
/// their own cache in the rewrite context, keyed by the rebuilt input.
struct ConstantFolder<'a, 'context, 'control> {
    arena: &'a mut ScalarArena,
    ctx: &'context RewriteContext<'context>,
    memo: HashMap<ScalarId, ScalarId>,
    work: CompileCheckpoints<'control>,
}

impl<'a, 'context, 'control> ConstantFolder<'a, 'context, 'control> {
    fn new(
        arena: &'a mut ScalarArena,
        ctx: &'context RewriteContext<'context>,
        work: CompileCheckpoints<'control>,
    ) -> Self {
        Self {
            arena,
            ctx,
            memo: HashMap::new(),
            work,
        }
    }

    fn fold_slot(&mut self, slot: &mut ScalarId) -> Result<bool, SqlCompileError> {
        let folded = fold_scalar_id(self.arena, *slot, self.ctx, &mut self.memo, &mut self.work)?;
        let changed = folded != *slot;
        *slot = folded;
        Ok(changed)
    }

    fn fold_optional_slot(&mut self, slot: &mut Option<ScalarId>) -> Result<bool, SqlCompileError> {
        self.work.step()?;
        Ok(match slot {
            Some(id) => self.fold_slot(id)?,
            None => false,
        })
    }

    fn fold_slots(&mut self, slots: &mut [ScalarId]) -> Result<bool, SqlCompileError> {
        self.work.step()?;
        let mut changed = false;
        for slot in slots {
            changed |= self.fold_slot(slot)?;
        }
        Ok(changed)
    }

    fn fold_sort_keys(&mut self, keys: &mut [SortKey]) -> Result<bool, SqlCompileError> {
        self.work.step()?;
        let mut changed = false;
        for key in keys {
            changed |= self.fold_slot(&mut key.expr)?;
        }
        Ok(changed)
    }

    /// Fold every scalar field carried by one operator.
    ///
    /// The match is exhaustive on purpose: adding a scalar-bearing operator
    /// must be a compile error here, not a silently-skipped field. The covered
    /// field set mirrors `rewrite::required_columns::tag_required_columns`.
    fn fold_operator(&mut self, op: &mut Operator) -> Result<bool, SqlCompileError> {
        self.work.step()?;
        Ok(match op {
            Operator::LogicalScan(scan) | Operator::PhysicalScan(scan) => {
                self.fold_slots(&mut scan.predicates)?
            }
            Operator::LogicalFilter(filter) | Operator::PhysicalFilter(filter) => {
                self.fold_slot(&mut filter.predicate)?
            }
            Operator::LogicalProject(project) | Operator::PhysicalProject(project) => {
                self.work.step()?;
                let mut changed = false;
                for item in &mut project.items {
                    changed |= self.fold_slot(&mut item.expr)?;
                }
                changed
            }
            Operator::LogicalAggregate(agg) => {
                let mut changed = self.fold_slots(&mut agg.group_by)?;
                for aggregate in &mut agg.aggregates {
                    // Normalized calls retain their original binding outside ScalarArena.
                    admit_parent(self.ctx,
                        aggregate.source.binding(),
                        novarocks_functions::PureCallLifecycle::Aggregate,
                        self.work.control(),
                    )?;
                    if aggregate.source.has_projected_inputs() {
                        continue;
                    }
                    changed |= aggregate.source.rewrite_channels(|arguments, order_by| {
                        let args_changed = self.fold_slots(arguments)?;
                        Ok::<_, SqlCompileError>(args_changed | self.fold_sort_keys(order_by)?)
                    })?;
                }
                changed
            }
            Operator::PhysicalHashAggregate(agg) => {
                let mut changed = self.fold_slots(&mut agg.group_by)?;
                for aggregate in &mut agg.aggregates {
                    // Normalized calls retain their original binding outside ScalarArena.
                    admit_parent(self.ctx,
                        aggregate.source.binding(),
                        novarocks_functions::PureCallLifecycle::Aggregate,
                        self.work.control(),
                    )?;
                    if aggregate.source.has_projected_inputs() {
                        continue;
                    }
                    changed |= aggregate.source.rewrite_channels(|arguments, order_by| {
                        let args_changed = self.fold_slots(arguments)?;
                        Ok::<_, SqlCompileError>(args_changed | self.fold_sort_keys(order_by)?)
                    })?;
                }
                changed
            }
            Operator::LogicalJoin(join) => {
                let mut changed = self.fold_optional_slot(&mut join.condition)?;
                if let Some(author) = &mut join.naa_author {
                    changed |= self.fold_slot(&mut author.comparison)?;
                    changed |= self.fold_optional_slot(&mut author.eligibility)?;
                }
                changed
            }
            Operator::PhysicalHashJoin(join) => {
                self.work.step()?;
                let mut changed = false;
                for condition in join.condition.keys_mut() {
                    changed |= self.fold_slot(&mut condition.left)?;
                    changed |= self.fold_slot(&mut condition.right)?;
                }
                if let Some(keys) = join.condition.scope_keys_mut() {
                    for condition in keys {
                        changed |= self.fold_slot(&mut condition.left)?;
                        changed |= self.fold_slot(&mut condition.right)?;
                    }
                }
                changed |= self.fold_optional_slot(join.condition.residual_mut())?;
                changed
            }
            Operator::PhysicalNestLoopJoin(join) => self.fold_optional_slot(&mut join.condition)?,
            Operator::LogicalSort(sort) | Operator::PhysicalSort(sort) => {
                let mut changed = self.fold_sort_keys(&mut sort.items)?;
                changed |= self.fold_slots(&mut sort.analytic_partition_exprs)?;
                changed
            }
            Operator::LogicalTopN(topn) | Operator::PhysicalTopN(topn) => {
                self.fold_sort_keys(&mut topn.items)?
            }
            Operator::LogicalWindow(window) | Operator::PhysicalWindow(window) => {
                self.work.step()?;
                let mut changed = false;
                for spec in &mut window.window_exprs {
                    // OVER's lifecycle comes from the original normalized source author.
                    let lifecycle = if spec.aggregate_binding.is_some() {
                        novarocks_functions::PureCallLifecycle::AggregateWindow {
                            distinct: spec.distinct,
                            ordered: !spec.function_order_by.is_empty(),
                        }
                    } else {
                        novarocks_functions::PureCallLifecycle::Window
                    };
                    admit_parent(self.ctx,
                        &spec.binding,
                        lifecycle,
                        self.work.control(),
                    )?;
                    changed |= self.fold_slots(&mut spec.args)?;
                    changed |= self.fold_sort_keys(&mut spec.function_order_by)?;
                    changed |= self.fold_slots(&mut spec.partition_by)?;
                    changed |= self.fold_sort_keys(&mut spec.order_by)?;
                }
                changed
            }
            Operator::LogicalValues(values) | Operator::PhysicalValues(values) => {
                self.work.step()?;
                let mut changed = false;
                for row in &mut values.rows {
                    changed |= self.fold_slots(row)?;
                }
                changed
            }
            Operator::LogicalTableFunction(func) | Operator::PhysicalTableFunction(func) => {
                admit_parent(self.ctx,
                    &func.binding,
                    novarocks_functions::PureCallLifecycle::Table,
                    self.work.control(),
                )?;
                self.fold_slots(&mut func.args)?
            }
            Operator::LogicalChangeEventExpand(expand)
            | Operator::PhysicalChangeEventExpand(expand) => {
                self.work.step()?;
                let mut changed = false;
                for event in &mut expand.events {
                    changed |= self.fold_optional_slot(&mut event.predicate)?;
                    for assignment in &mut event.assignments {
                        changed |= self.fold_optional_slot(&mut assignment.expr)?;
                    }
                }
                changed
            }
            Operator::LogicalApply(apply) => {
                let mut changed = self.fold_slot(&mut apply.subquery_expr)?;
                changed |= self.fold_slots(&mut apply.correlation_conjuncts)?;
                changed |= self.fold_optional_slot(&mut apply.residual_predicate)?;
                changed
            }
            // Operators without scalar fields.
            Operator::LogicalLimit(_)
            | Operator::PhysicalLimit(_)
            | Operator::LogicalUnion(_)
            | Operator::PhysicalUnion(_)
            | Operator::LogicalIntersect(_)
            | Operator::PhysicalIntersect(_)
            | Operator::LogicalExcept(_)
            | Operator::PhysicalExcept(_)
            | Operator::LogicalGenerateSeries(_)
            | Operator::PhysicalGenerateSeries(_)
            | Operator::LogicalSystemTable(_)
            | Operator::PhysicalSystemTable(_)
            | Operator::LogicalRepeat(_)
            | Operator::PhysicalRepeat(_)
            | Operator::LogicalCTEAnchor(_)
            | Operator::PhysicalCTEAnchor(_)
            | Operator::LogicalCTEProduce(_)
            | Operator::PhysicalCTEProduce(_)
            | Operator::LogicalCTEConsume(_)
            | Operator::PhysicalCTEConsume(_)
            // AssertOneRow only carries the original subquery text.
            | Operator::LogicalAssertOneRow(_)
            | Operator::PhysicalAssertOneRow(_)
            | Operator::LogicalImvDelta(_)
            | Operator::LogicalImvVersion(_)
            | Operator::LogicalContentApply(_)
            | Operator::PhysicalContentApply(_)
            | Operator::PhysicalDistribution(_) => false,
        })
    }
}

/// Whether an operator carries at least one scalar field.
///
/// Kept in lockstep with `ConstantFolder::fold_operator`; both matches are
/// exhaustive so a new operator cannot slip past either one.
fn operator_has_scalars(op: &Operator) -> bool {
    match op {
        Operator::LogicalScan(scan) | Operator::PhysicalScan(scan) => !scan.predicates.is_empty(),
        Operator::LogicalFilter(_) | Operator::PhysicalFilter(_) => true,
        Operator::LogicalProject(project) | Operator::PhysicalProject(project) => {
            !project.items.is_empty()
        }
        Operator::LogicalAggregate(agg) => !agg.group_by.is_empty() || !agg.aggregates.is_empty(),
        Operator::PhysicalHashAggregate(agg) => {
            !agg.group_by.is_empty() || !agg.aggregates.is_empty()
        }
        Operator::LogicalJoin(join) => join.condition.is_some(),
        Operator::PhysicalHashJoin(join) => {
            !join.condition.keys().is_empty() || join.condition.residual().is_some()
        }
        Operator::PhysicalNestLoopJoin(join) => join.condition.is_some(),
        Operator::LogicalSort(sort) | Operator::PhysicalSort(sort) => {
            !sort.items.is_empty() || !sort.analytic_partition_exprs.is_empty()
        }
        Operator::LogicalTopN(topn) | Operator::PhysicalTopN(topn) => !topn.items.is_empty(),
        Operator::LogicalWindow(window) | Operator::PhysicalWindow(window) => {
            !window.window_exprs.is_empty()
        }
        Operator::LogicalValues(values) | Operator::PhysicalValues(values) => {
            values.rows.iter().any(|row| !row.is_empty())
        }
        Operator::LogicalTableFunction(func) | Operator::PhysicalTableFunction(func) => {
            !func.args.is_empty()
        }
        Operator::LogicalChangeEventExpand(expand)
        | Operator::PhysicalChangeEventExpand(expand) => expand.events.iter().any(|event| {
            event.predicate.is_some()
                || event
                    .assignments
                    .iter()
                    .any(|assignment| assignment.expr.is_some())
        }),
        Operator::LogicalApply(_) => true,
        Operator::LogicalLimit(_)
        | Operator::PhysicalLimit(_)
        | Operator::LogicalUnion(_)
        | Operator::PhysicalUnion(_)
        | Operator::LogicalIntersect(_)
        | Operator::PhysicalIntersect(_)
        | Operator::LogicalExcept(_)
        | Operator::PhysicalExcept(_)
        | Operator::LogicalGenerateSeries(_)
        | Operator::PhysicalGenerateSeries(_)
        | Operator::LogicalSystemTable(_)
        | Operator::PhysicalSystemTable(_)
        | Operator::LogicalRepeat(_)
        | Operator::PhysicalRepeat(_)
        | Operator::LogicalCTEAnchor(_)
        | Operator::PhysicalCTEAnchor(_)
        | Operator::LogicalCTEProduce(_)
        | Operator::PhysicalCTEProduce(_)
        | Operator::LogicalCTEConsume(_)
        | Operator::PhysicalCTEConsume(_)
        | Operator::LogicalAssertOneRow(_)
        | Operator::PhysicalAssertOneRow(_)
        | Operator::LogicalImvDelta(_)
        | Operator::LogicalImvVersion(_)
        | Operator::LogicalContentApply(_)
        | Operator::PhysicalContentApply(_)
        | Operator::PhysicalDistribution(_) => false,
    }
}

#[recursive::recursive]
fn fold_scalar_id(
    arena: &mut ScalarArena,
    id: ScalarId,
    ctx: &RewriteContext<'_>,
    memo: &mut HashMap<ScalarId, ScalarId>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ScalarId, SqlCompileError> {
    work.step()?;
    if let Some(&cached) = memo.get(&id) {
        return Ok(cached);
    }
    let folded = fold_scalar_uncached(arena, id, ctx, memo, work)?;
    memo.insert(id, folded);
    Ok(folded)
}

fn fold_scalar_uncached(
    arena: &mut ScalarArena,
    id: ScalarId,
    ctx: &RewriteContext<'_>,
    memo: &mut HashMap<ScalarId, ScalarId>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ScalarId, SqlCompileError> {
    work.step()?;
    let value_type = arena.value_type(id).clone();
    work.step()?;
    let mut node = arena.node(id).clone();

    // Borrow the original selected parent before post-order child work. This
    // is the existing traversal, not a second scanner or a payload evaluator.
    use novarocks_functions::PureCallLifecycle as Lifecycle;
    match &node {
        ScalarNode::FunctionCall { binding, .. } => {
            admit_parent(ctx, binding, Lifecycle::Scalar, work.control())?;
        }
        ScalarNode::AggregateCall { resolved, .. } => {
            admit_parent(ctx, resolved, Lifecycle::Aggregate, work.control())?;
        }
        ScalarNode::WindowCall {
            binding,
            aggregate_binding,
            distinct,
            function_order_by,
            ..
        } => {
            let lifecycle = if aggregate_binding.is_some() {
                Lifecycle::AggregateWindow {
                    distinct: *distinct,
                    ordered: !function_order_by.is_empty(),
                }
            } else {
                Lifecycle::Window
            };
            admit_parent(ctx, binding, lifecycle, work.control())?;
        }
        _ => {}
    }

    if ordered_regexp_null(arena, &node, ctx, work)? {
        return arena.intern_observed(
            ScalarNode::Literal(HashableLiteral(LiteralValue::Null)),
            value_type,
            work.control(),
        );
    }

    // Post-order: children first, so a node only ever sees already-folded
    // children and "all children are literals" is decidable locally.
    match &mut node {
        ScalarNode::ColumnRef(_)
        | ScalarNode::LambdaParamRef { .. }
        | ScalarNode::Literal(_)
        | ScalarNode::Constant(_) => {}
        ScalarNode::BinaryOp { left, right, .. } => {
            *left = fold_scalar_id(arena, *left, ctx, memo, work)?;
            *right = fold_scalar_id(arena, *right, ctx, memo, work)?;
        }
        ScalarNode::UnaryOp { child, .. }
        | ScalarNode::Cast { child, .. }
        | ScalarNode::IsNull { child, .. }
        | ScalarNode::IsTruthValue { child, .. } => {
            *child = fold_scalar_id(arena, *child, ctx, memo, work)?;
        }
        ScalarNode::Nested(child) => {
            *child = fold_scalar_id(arena, *child, ctx, memo, work)?;
        }
        ScalarNode::FunctionCall { args, .. } => {
            for arg in args.iter_mut() {
                *arg = fold_scalar_id(arena, *arg, ctx, memo, work)?;
            }
        }
        ScalarNode::LambdaFunction { body, .. } | ScalarNode::Lambda { body, .. } => {
            *body = fold_scalar_id(arena, *body, ctx, memo, work)?;
        }
        ScalarNode::AggregateCall {
            args,
            order_by,
            filter,
            ..
        } => {
            if let Some(predicate) = filter {
                *predicate = fold_scalar_id(arena, *predicate, ctx, memo, work)?;
            }
            for arg in args.iter_mut() {
                *arg = fold_scalar_id(arena, *arg, ctx, memo, work)?;
            }
            for key in order_by.iter_mut() {
                key.expr = fold_scalar_id(arena, key.expr, ctx, memo, work)?;
            }
        }
        ScalarNode::WindowCall {
            args,
            function_order_by,
            partition_by,
            order_by,
            ..
        } => {
            for arg in args.iter_mut() {
                *arg = fold_scalar_id(arena, *arg, ctx, memo, work)?;
            }
            for expr in partition_by.iter_mut() {
                *expr = fold_scalar_id(arena, *expr, ctx, memo, work)?;
            }
            for key in function_order_by.iter_mut().chain(order_by) {
                key.expr = fold_scalar_id(arena, key.expr, ctx, memo, work)?;
            }
        }
        ScalarNode::InList { child, list, .. } => {
            *child = fold_scalar_id(arena, *child, ctx, memo, work)?;
            for item in list.iter_mut() {
                *item = fold_scalar_id(arena, *item, ctx, memo, work)?;
            }
        }
        ScalarNode::Between {
            child, low, high, ..
        } => {
            *child = fold_scalar_id(arena, *child, ctx, memo, work)?;
            *low = fold_scalar_id(arena, *low, ctx, memo, work)?;
            *high = fold_scalar_id(arena, *high, ctx, memo, work)?;
        }
        ScalarNode::Like { child, pattern, .. } => {
            *child = fold_scalar_id(arena, *child, ctx, memo, work)?;
            *pattern = fold_scalar_id(arena, *pattern, ctx, memo, work)?;
        }
        ScalarNode::Case {
            operand,
            when_then,
            else_expr,
            ..
        } => {
            if let Some(operand) = operand {
                *operand = fold_scalar_id(arena, *operand, ctx, memo, work)?;
            }
            for (when, then) in when_then.iter_mut() {
                *when = fold_scalar_id(arena, *when, ctx, memo, work)?;
                *then = fold_scalar_id(arena, *then, ctx, memo, work)?;
            }
            if let Some(else_expr) = else_expr {
                *else_expr = fold_scalar_id(arena, *else_expr, ctx, memo, work)?;
            }
        }
    }

    // Re-intern with the rebuilt children, keeping this node's own type
    // metadata. `intern` may canonicalize commutative operand order, so the
    // fold step below reads the children back out of the arena.
    let rebuilt = arena.intern_observed(node, value_type, work.control())?;
    Ok(try_fold_node_current(arena, rebuilt, ctx, work)?.unwrap_or(rebuilt))
}

fn ordered_regexp_null(
    arena: &ScalarArena,
    node: &ScalarNode,
    ctx: &RewriteContext<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, SqlCompileError> {
    use novarocks_functions::{FunctionArgumentEvaluation, FunctionSpecializationFailure};
    use novarocks_type_contract::ArgumentControl;

    let ScalarNode::FunctionCall { binding, args, .. } = node else {
        return Ok(false);
    };
    if binding.semantics.argument_evaluation != FunctionArgumentEvaluation::ShortCircuit {
        return Ok(false);
    }
    let Some(functions) = ctx.sealed_functions() else {
        return Ok(false);
    };
    let mut known_null = false;
    for &argument in args {
        let mut argument = argument;
        loop {
            work.step()?;
            match arena.node(argument) {
                ScalarNode::Literal(HashableLiteral(LiteralValue::Null)) => known_null = true,
                ScalarNode::Constant(value) => {
                    known_null = value
                        .is_null_observed(CompilePhase::FunctionSpecialization, work.control())?;
                }
                ScalarNode::Nested(child) | ScalarNode::Cast { child, .. } => {
                    argument = *child;
                    continue;
                }
                _ => {}
            }
            break;
        }
        if known_null {
            break;
        }
    }
    if !known_null {
        return Ok(false);
    }
    let selected = functions
        .metadata()
        .pure_overload_declaration_observed(
            &binding.function_id,
            binding.kind,
            &binding.selected.overload,
            work.control(),
        )
        .map_err(|cause| match cause {
            FunctionSpecializationFailure::Control(cause) => cause.into(),
            cause => SqlCompileError::CompilationSource(
                novarocks_failure_contract::SharedFailureCause::new(cause),
            ),
        })?;
    Ok(selected.effects().argument_control == ArgumentControl::OrderedRegexp)
}

/// Fold a rebuilt node only after every direct child has become a constant.
fn try_fold_node_current(
    arena: &mut ScalarArena,
    id: ScalarId,
    ctx: &RewriteContext<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Option<ScalarId>, SqlCompileError> {
    work.step()?;
    let node = arena.node(id);
    let out_type = arena.value_type(id);
    if ordered_regexp_null(arena, node, ctx, work)? {
        return Ok(Some(arena.intern_observed(
            ScalarNode::Literal(HashableLiteral(LiteralValue::Null)),
            out_type.clone(),
            work.control(),
        )?));
    }
    if !is_wire_encodable_literal_type(out_type) {
        return Ok(None);
    }
    if let ScalarNode::Nested(inner) = node {
        return match arena.node(*inner) {
            literal @ (ScalarNode::Literal(_) | ScalarNode::Constant(_)) => Ok(Some(
                arena.intern_observed(literal.clone(), out_type.clone(), work.control())?,
            )),
            _ => Ok(None),
        };
    }
    if ctx.root_allow_throw_exception() {
        return Ok(None);
    }

    let children = match node {
        ScalarNode::BinaryOp {
            op, left, right, ..
        } => {
            if *op == BinOp::EqForNull
                && arena.data_type(*left) == &DataType::Null
                && arena.data_type(*right) == &DataType::Null
            {
                return Ok(None);
            }
            &[*left, *right][..]
        }
        ScalarNode::UnaryOp {
            op: UnOp::Not,
            child,
        }
        | ScalarNode::Cast { child, .. } => std::slice::from_ref(child),
        ScalarNode::FunctionCall {
            name,
            args,
            distinct: false,
            volatility: FunctionVolatility::Immutable,
            ..
        } if !is_environment_sensitive_function(name) => args,
        _ => return Ok(None),
    };
    if let Some(outcome) = ctx.cached_fold(id) {
        return Ok(outcome);
    }
    for child in children {
        work.step()?;
        if !matches!(
            arena.node(*child),
            ScalarNode::Constant(_) | ScalarNode::Literal(_)
        ) {
            return Ok(None);
        }
    }
    if !admission::fold_value_type_supported(out_type, work)? {
        return Ok(None);
    }
    for child in children {
        if !admission::fold_value_type_supported(arena.value_type(*child), work)? {
            return Ok(None);
        }
    }

    work.flush()?;
    let result = evaluate_fold_input(arena, id, ctx, work);
    let folded = result?
        .map(|value| {
            arena.intern_observed(
                ScalarNode::Constant(value),
                arena.value_type(id).clone(),
                work.control(),
            )
        })
        .transpose()?;
    ctx.cache_fold(id, folded);
    Ok(folded)
}

fn evaluate_fold_input(
    arena: &ScalarArena,
    id: ScalarId,
    ctx: &RewriteContext<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Option<novarocks_functions::ConstantValue>, SqlCompileError> {
    let result = evaluation::fold_node(id, arena, ctx)?;
    let value = match result.outcome {
        evaluation::FoldNodeOutcome::Folded(value) => Some(value),
        evaluation::FoldNodeOutcome::Kept => None,
    };
    if let (Some(observer), Some(unit)) = (ctx.fold_dependency_observer(), result.unit) {
        observer.observe_fold_unit_observed(
            &SqlAuthoredScalarUnit::from_emission(unit),
            value.as_ref(),
            work.control(),
        )?;
    }
    work.step()?;
    Ok(value)
}

#[cfg(test)]
mod tests;
