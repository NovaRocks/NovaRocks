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
//! Evaluation itself is delegated to the [`SqlConstantEvaluator`] port
//! (`crate::compiler`), so the folded literal comes out of the very kernels the
//! runtime would have used. When no evaluator is attached the rule degrades to
//! a no-op instead of changing plan semantics.
//!
//! The rule runs in `LogicalNormalize`, before predicate pushdown, so that a
//! `Cast(Literal)` has already collapsed into a bare literal by the time
//! static-predicate extraction inspects the plan.

use crate::compiler::SqlCompileError;
use novarocks_type_contract::{CompileCheckpoints, CompilePhase};
use std::collections::HashMap;

use arrow::datatypes::DataType;

use crate::compiler::{
    FoldArg, FoldNodeKind, FoldRequest, SqlConstantEvaluationError, SqlConstantEvaluator,
};
use crate::functions::FunctionVolatility;
use crate::optimizer::operator::Operator;
use crate::optimizer::opt_expr::OptExpr;
use crate::optimizer::rewrite::context::RewriteContext;
use crate::optimizer::rewrite::phase::RewritePhase;
use crate::optimizer::rewrite::result::RewriteResult;
use crate::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::optimizer::scalar::{HashableLiteral, ScalarArena, ScalarId, ScalarNode, SortKey};

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
/// Verified against the execution kernels under
/// `novarocks/execution/src/exec/expr/function/`:
/// - `date/from_unixtime.rs` — `default_time_zone()` falls back to
///   `TimeZoneSpec::Local` (`from_unixtime`, `from_unixtime_ms`).
/// - `date/hour_from_unixtime.rs` — `Local.timestamp_opt(..)`.
/// - `date/unix_timestamp.rs` — the zero-arg form reads
///   `datetime_from_local_now()`.
/// - `date/convert_tz.rs` — `chrono_tz` timezone database lookups.
/// - `date/date.rs` — `epoch_to_datetime(.., timezone_aware = true)` uses
///   `chrono::Local` for `to_datetime` / `timestamp`.
/// - `variant/get_variant.rs` — `Local::now().offset().fix()` for
///   `get_variant_date` / `get_variant_datetime` / `get_variant_time`.
///
/// The `now`-family names are already `Volatile` in
/// `crate::functions::builtin_function_volatility`, so gate 2 alone would stop
/// them; they are repeated here so the denylist stays readable as the single
/// "never fold this on the FE" statement even if a volatility classification
/// ever changes.
const ENVIRONMENT_SENSITIVE_FUNCTIONS: &[&str] = &[
    // Wall clock / session clock.
    "now",
    "current_timestamp",
    "localtime",
    "localtimestamp",
    "curdate",
    "current_date",
    "curtime",
    "current_time",
    "utc_time",
    "utc_timestamp",
    "unix_timestamp",
    // Process/session timezone conversions.
    "convert_tz",
    "from_unixtime",
    "from_unixtime_ms",
    "hour_from_unixtime",
    "to_datetime",
    "timestamp",
    "get_variant_date",
    "get_variant_datetime",
    "get_variant_time",
];

/// Functions whose string values carry raw bytes rather than text.
///
/// NovaRocks represents binary payloads inside `Utf8` values for this family,
/// and the byte convention is not preserved by the literal representation: a
/// folded `aes_encrypt(..)` re-materializes as an ordinary string literal, and
/// downstream consumers such as `to_base64` then read different bytes than the
/// runtime produced. Excluding the family keeps folded output bit-identical to
/// runtime output; the cost is only a missed optimization.
const BYTE_CARRYING_STRING_FUNCTIONS: &[&str] = &[
    "aes_encrypt",
    "aes_decrypt",
    "from_base64",
    "from_binary",
    "base64_decode_binary",
    "base64_decode_string",
    "encode_fingerprint_sha256",
    "encode_row_id",
    "encode_sort_key",
    "sm3",
    "unhex",
];

fn is_byte_carrying_string_function(name: &str) -> bool {
    let lowered = name.to_ascii_lowercase();
    BYTE_CARRYING_STRING_FUNCTIONS
        .iter()
        .any(|denied| *denied == lowered)
}

fn is_environment_sensitive_function(name: &str) -> bool {
    let lowered = name.to_ascii_lowercase();
    ENVIRONMENT_SENSITIVE_FUNCTIONS
        .iter()
        .any(|denied| *denied == lowered)
}

/// Output types a folded literal is allowed to have.
///
/// Hard constraint, not an optimization heuristic: a folded literal has to
/// survive the FE -> BE plan encoding. The authoritative decode arms live in
/// `novarocks/native-adapter/src/fragment_expression/literal.rs` (`lower_literal_at`
/// / `lower_int_literal` / `lower_decimal_literal`) — the wire literal message
/// has no timestamp variant and no composite variant, so folding an expression
/// whose output type is `Timestamp(..)`, a list, a struct, a map, or any other
/// composite would produce a literal that cannot be sent to a backend.
fn is_wire_encodable_literal_type(data_type: &DataType) -> bool {
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
    ) || novarocks_types::largeint::is_largeint_data_type(data_type)
}

// Design: ADR-0100 (docs/adr/ADR-0100-constant-folding-reuses-execution-kernels-through-an-injected-port.md)
pub(crate) struct FoldConstant;

impl LogicalRewriteRule for FoldConstant {
    fn name(&self) -> &'static str {
        "FoldConstant"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::LogicalNormalize
    }

    fn matches(&self, expr: &OptExpr, ctx: &RewriteContext) -> bool {
        ctx.constant_evaluator().is_some() && operator_has_scalars(&expr.op)
    }

    fn apply(
        &self,
        expr: OptExpr,
        ctx: &mut RewriteContext,
    ) -> Result<RewriteResult, SqlCompileError> {
        let control = ctx.control_view();
        let work = CompileCheckpoints::try_new(&control, CompilePhase::Validate)?;
        let Some(evaluator) = ctx.constant_evaluator() else {
            work.finish()?;
            return Ok(RewriteResult::Unchanged);
        };

        let OptExpr {
            mut op,
            children,
            required_output_columns,
        } = expr;

        let arena = ctx.scalar_arena();
        let evaluator =
            crate::compiler::SqlFoldEvaluatorLoan::new(evaluator, ctx.fold_dependency_observer())
                .with_catalog(ctx.function_catalog());
        let changed = {
            let mut arena = arena.borrow_mut();
            let mut folder = ConstantFolder::new(&mut arena, &evaluator, work);
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

/// Bottom-up constant folder over one `ScalarArena`.
///
/// `memo` maps an original `ScalarId` to its folded replacement so a shared
/// (hash-consed) sub-expression is folded once per `apply`, not once per
/// reference. The work scope borrows the original request and never enters
/// the arena, memo values or rewritten output.
struct ConstantFolder<'a, 'control> {
    arena: &'a mut ScalarArena,
    evaluator: &'control dyn SqlConstantEvaluator,
    memo: HashMap<ScalarId, ScalarId>,
    work: CompileCheckpoints<'control>,
}

impl<'a, 'control> ConstantFolder<'a, 'control> {
    fn new(
        arena: &'a mut ScalarArena,
        evaluator: &'control dyn SqlConstantEvaluator,
        work: CompileCheckpoints<'control>,
    ) -> Self {
        Self {
            arena,
            evaluator,
            memo: HashMap::new(),
            work,
        }
    }

    fn fold_slot(&mut self, slot: &mut ScalarId) -> Result<bool, SqlCompileError> {
        Ok(
            match fold_scalar(
                self.arena,
                *slot,
                self.evaluator,
                &mut self.memo,
                &mut self.work,
            )? {
                Some(folded) => {
                    *slot = folded;
                    true
                }
                None => false,
            },
        )
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
                    self.evaluator.admit_fold_parent_observed(
                        aggregate.source.binding(),
                        novarocks_functions::PureCallLifecycle::Aggregate,
                        self.work.control(),
                    )?;
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
                    self.evaluator.admit_fold_parent_observed(
                        aggregate.source.binding(),
                        novarocks_functions::PureCallLifecycle::Aggregate,
                        self.work.control(),
                    )?;
                    changed |= aggregate.source.rewrite_channels(|arguments, order_by| {
                        let args_changed = self.fold_slots(arguments)?;
                        Ok::<_, SqlCompileError>(args_changed | self.fold_sort_keys(order_by)?)
                    })?;
                }
                changed
            }
            Operator::LogicalJoin(join) => self.fold_optional_slot(&mut join.condition)?,
            Operator::PhysicalHashJoin(join) => {
                self.work.step()?;
                let mut changed = false;
                for condition in &mut join.eq_conditions {
                    changed |= self.fold_slot(&mut condition.left)?;
                    changed |= self.fold_slot(&mut condition.right)?;
                }
                changed |= self.fold_optional_slot(&mut join.other_condition)?;
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
                        novarocks_functions::PureCallLifecycle::AggregateWindow
                    } else {
                        novarocks_functions::PureCallLifecycle::Window
                    };
                    self.evaluator.admit_fold_parent_observed(
                        &spec.binding,
                        lifecycle,
                        self.work.control(),
                    )?;
                    changed |= self.fold_slots(&mut spec.args)?;
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
                self.evaluator.admit_fold_parent_observed(
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
            !join.eq_conditions.is_empty() || join.other_condition.is_some()
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
        | Operator::PhysicalDistribution(_) => false,
    }
}

/// Fold one scalar expression bottom-up.
///
/// Returns `Some(new_id)` when the expression changed (a sub-tree collapsed to
/// a literal), `None` when it is already fully folded. Type metadata is never
/// altered: a replacement literal is interned with the original node's own
/// `DataType` and nullable flag.
fn fold_scalar(
    arena: &mut ScalarArena,
    id: ScalarId,
    evaluator: &dyn SqlConstantEvaluator,
    memo: &mut HashMap<ScalarId, ScalarId>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Option<ScalarId>, SqlCompileError> {
    let folded = fold_scalar_id(arena, id, evaluator, memo, work)?;
    Ok((folded != id).then_some(folded))
}

fn fold_scalar_id(
    arena: &mut ScalarArena,
    id: ScalarId,
    evaluator: &dyn SqlConstantEvaluator,
    memo: &mut HashMap<ScalarId, ScalarId>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ScalarId, SqlCompileError> {
    work.step()?;
    if let Some(&cached) = memo.get(&id) {
        return Ok(cached);
    }
    let folded = fold_scalar_uncached(arena, id, evaluator, memo, work)?;
    memo.insert(id, folded);
    Ok(folded)
}

fn fold_scalar_uncached(
    arena: &mut ScalarArena,
    id: ScalarId,
    evaluator: &dyn SqlConstantEvaluator,
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
            evaluator.admit_fold_parent_observed(binding, Lifecycle::Scalar, work.control())?;
        }
        ScalarNode::AggregateCall { resolved, .. } => {
            evaluator.admit_fold_parent_observed(resolved, Lifecycle::Aggregate, work.control())?;
        }
        ScalarNode::WindowCall {
            binding,
            aggregate_binding,
            ..
        } => {
            let lifecycle = if aggregate_binding.is_some() {
                Lifecycle::AggregateWindow
            } else {
                Lifecycle::Window
            };
            evaluator.admit_fold_parent_observed(binding, lifecycle, work.control())?;
        }
        _ => {}
    }

    // Post-order: children first, so a node only ever sees already-folded
    // children and "all children are literals" is decidable locally.
    match &mut node {
        ScalarNode::ColumnRef(_)
        | ScalarNode::LambdaParamRef { .. }
        | ScalarNode::Literal(_)
        | ScalarNode::Constant(_) => {}
        ScalarNode::BinaryOp { left, right, .. } => {
            *left = fold_scalar_id(arena, *left, evaluator, memo, work)?;
            *right = fold_scalar_id(arena, *right, evaluator, memo, work)?;
        }
        ScalarNode::UnaryOp { child, .. }
        | ScalarNode::Cast { child, .. }
        | ScalarNode::IsNull { child, .. }
        | ScalarNode::IsTruthValue { child, .. } => {
            *child = fold_scalar_id(arena, *child, evaluator, memo, work)?;
        }
        ScalarNode::Nested(child) => {
            *child = fold_scalar_id(arena, *child, evaluator, memo, work)?;
        }
        ScalarNode::FunctionCall { name, args, .. } => {
            for arg in args.iter_mut() {
                // TIME_TO_SEC distinguishes a SEC_TO_TIME result from a raw
                // string. Folding that producer would erase its provenance.
                *arg = if name == "time_to_sec" && has_sec_to_time_source(arena, *arg, work)? {
                    fold_sec_to_time_source(arena, *arg, evaluator, memo, work)?
                } else {
                    fold_scalar_id(arena, *arg, evaluator, memo, work)?
                };
            }
        }
        ScalarNode::LambdaFunction { body, .. } | ScalarNode::Lambda { body, .. } => {
            *body = fold_scalar_id(arena, *body, evaluator, memo, work)?;
        }
        ScalarNode::AggregateCall { args, order_by, .. } => {
            for arg in args.iter_mut() {
                *arg = fold_scalar_id(arena, *arg, evaluator, memo, work)?;
            }
            for key in order_by.iter_mut() {
                key.expr = fold_scalar_id(arena, key.expr, evaluator, memo, work)?;
            }
        }
        ScalarNode::WindowCall {
            args,
            partition_by,
            order_by,
            ..
        } => {
            for arg in args.iter_mut() {
                *arg = fold_scalar_id(arena, *arg, evaluator, memo, work)?;
            }
            for expr in partition_by.iter_mut() {
                *expr = fold_scalar_id(arena, *expr, evaluator, memo, work)?;
            }
            for key in order_by.iter_mut() {
                key.expr = fold_scalar_id(arena, key.expr, evaluator, memo, work)?;
            }
        }
        ScalarNode::InList { child, list, .. } => {
            *child = fold_scalar_id(arena, *child, evaluator, memo, work)?;
            for item in list.iter_mut() {
                *item = fold_scalar_id(arena, *item, evaluator, memo, work)?;
            }
        }
        ScalarNode::Between {
            child, low, high, ..
        } => {
            *child = fold_scalar_id(arena, *child, evaluator, memo, work)?;
            *low = fold_scalar_id(arena, *low, evaluator, memo, work)?;
            *high = fold_scalar_id(arena, *high, evaluator, memo, work)?;
        }
        ScalarNode::Like { child, pattern, .. } => {
            *child = fold_scalar_id(arena, *child, evaluator, memo, work)?;
            *pattern = fold_scalar_id(arena, *pattern, evaluator, memo, work)?;
        }
        ScalarNode::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(operand) = operand {
                *operand = fold_scalar_id(arena, *operand, evaluator, memo, work)?;
            }
            for (when, then) in when_then.iter_mut() {
                *when = fold_scalar_id(arena, *when, evaluator, memo, work)?;
                *then = fold_scalar_id(arena, *then, evaluator, memo, work)?;
            }
            if let Some(else_expr) = else_expr {
                *else_expr = fold_scalar_id(arena, *else_expr, evaluator, memo, work)?;
            }
        }
    }

    // Re-intern with the rebuilt children, keeping this node's own type
    // metadata. `intern` may canonicalize commutative operand order, so the
    // fold step below reads the children back out of the arena.
    let rebuilt = arena.intern_observed(node, value_type, work.control())?;
    Ok(try_fold_node(arena, rebuilt, evaluator, work)?.unwrap_or(rebuilt))
}

fn has_sec_to_time_source(
    arena: &ScalarArena,
    mut id: ScalarId,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, SqlCompileError> {
    loop {
        work.step()?;
        match arena.node(id) {
            ScalarNode::FunctionCall { name, .. } => return Ok(name == "sec_to_time"),
            ScalarNode::Cast { child, .. } | ScalarNode::Nested(child) => id = *child,
            _ => return Ok(false),
        }
    }
}

/// Keep the source-sensitive path intact, while still folding the producer's
/// numeric arguments. Do not memoize this contextual result: the same producer
/// may independently be folded when it is another projection's root.
fn fold_sec_to_time_source(
    arena: &mut ScalarArena,
    id: ScalarId,
    evaluator: &dyn SqlConstantEvaluator,
    memo: &mut HashMap<ScalarId, ScalarId>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ScalarId, SqlCompileError> {
    work.step()?;
    let mut node = arena.node(id).clone();
    match &mut node {
        ScalarNode::FunctionCall { args, .. } => {
            for arg in args {
                *arg = fold_scalar_id(arena, *arg, evaluator, memo, work)?;
            }
        }
        ScalarNode::Cast { child, .. } | ScalarNode::Nested(child) => {
            *child = fold_sec_to_time_source(arena, *child, evaluator, memo, work)?;
        }
        _ => unreachable!("source path was checked before folding"),
    }
    arena.intern_observed(node, arena.value_type(id).clone(), work.control())
}

/// Try to replace one node (whose children are already folded) with a literal.
///
/// Returns `Ok(None)` when a safety gate fails or the legacy evaluator declines
/// or reports an evaluation error. Caller control failures remain typed errors.
fn try_fold_node(
    arena: &mut ScalarArena,
    id: ScalarId,
    evaluator: &dyn SqlConstantEvaluator,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Option<ScalarId>, SqlCompileError> {
    work.step()?;
    let node = arena.node(id).clone();
    let out_type = arena.data_type(id).clone();

    // `Nested` is a pure syntactic wrapper: when its inner expression is a
    // literal the wrapper collapses onto that literal, no evaluation needed.
    if let ScalarNode::Nested(inner) = &node {
        let literal = match arena.node(*inner) {
            ScalarNode::Literal(_) | ScalarNode::Constant(_) => arena.node(*inner).clone(),
            _ => return Ok(None),
        };
        if !is_wire_encodable_literal_type(&out_type) {
            return Ok(None);
        }
        return Ok(Some(arena.intern_observed(
            literal,
            arena.value_type(id).clone(),
            work.control(),
        )?));
    }

    // Gate 2: a volatile or DISTINCT function is never a constant.
    // Gate 3: an environment-sensitive function must stay on the backend.
    let kind = match &node {
        ScalarNode::BinaryOp {
            op,
            decimal_overflow_policy,
            ..
        } => FoldNodeKind::BinaryOp(*op, *decimal_overflow_policy),
        ScalarNode::UnaryOp { op, .. } => FoldNodeKind::UnaryOp(*op),
        ScalarNode::Cast {
            decimal_overflow_policy,
            ..
        } => FoldNodeKind::Cast(*decimal_overflow_policy),
        ScalarNode::FunctionCall {
            name,
            distinct,
            volatility,
            ..
        } => {
            if *distinct || *volatility != FunctionVolatility::Immutable {
                return Ok(None);
            }
            if is_environment_sensitive_function(name) || is_byte_carrying_string_function(name) {
                return Ok(None);
            }
            FoldNodeKind::Function { name: name.clone() }
        }
        // Every other node shape stays unfolded in v1; its children were
        // already folded above.
        _ => return Ok(None),
    };

    // Gate 4: the folded literal has to survive the FE -> BE plan encoding.
    if !is_wire_encodable_literal_type(&out_type) {
        return Ok(None);
    }

    let children: Vec<ScalarId> = match &node {
        ScalarNode::BinaryOp { left, right, .. } => vec![*left, *right],
        ScalarNode::UnaryOp { child, .. } | ScalarNode::Cast { child, .. } => vec![*child],
        ScalarNode::FunctionCall { args, .. } => args.clone(),
        _ => return Ok(None),
    };

    // Gate 1: every child must already be a literal.
    let mut args = Vec::with_capacity(children.len());
    for child in children {
        work.step()?;
        let value = match arena.node(child) {
            ScalarNode::Constant(value) => value.clone(),
            ScalarNode::Literal(HashableLiteral(value)) => {
                work.flush()?;
                let value = crate::constant::admit_syntax_constant(
                    value,
                    arena.value_type(child),
                    arena.constant_policy(),
                    work.control(),
                )?;
                work.flush()?;
                value
            }
            _ => return Ok(None),
        };
        args.push(FoldArg {
            value,
            value_type: arena.value_type(child).clone(),
        });
    }

    // All arguments at this point are admitted literals/folded constants.
    // The exact binding, rather than its spelling or CV payload, identifies
    // the emitted-source contract that the real runtime will consume.
    let regexp_count_pattern_source = match &node {
        ScalarNode::FunctionCall { binding, .. }
            if binding.function_id.as_str() == "builtin.scalar/regexp_count/v1" =>
        {
            let [_, pattern] = args.as_slice() else {
                return Err("regexp_count fold source requires two arguments".into());
            };
            Some(
                novarocks_type_contract::RegexpCountPatternSource::from_native_v1_emitted_shape(
                    &pattern.value_type,
                    true,
                )
                .map_err(str::to_owned)?,
            )
        }
        _ => None,
    };
    let request = FoldRequest {
        regexp_count_pattern_source,
        kind,
        args,
        result_type: arena.value_type(id).clone(),
        constant_policy: arena.constant_policy(),
    };

    work.step()?;
    work.flush()?;
    let source = match &node {
        ScalarNode::FunctionCall { binding, .. } => {
            crate::compiler::SqlFoldDependencySource::Function(binding)
        }
        ScalarNode::BinaryOp { .. } | ScalarNode::UnaryOp { .. } | ScalarNode::Cast { .. } => {
            crate::compiler::SqlFoldDependencySource::Intrinsic
        }
        _ => unreachable!("original foldability gate checked the node shape"),
    };
    let input = crate::compiler::SqlFoldDependencyInput {
        source,
        request: &request,
    };
    let evaluated = match crate::compiler::evaluate_fold_dependency_observed(
        evaluator,
        input,
        work.control(),
    ) {
        Ok(value) => Ok(value),
        Err(SqlConstantEvaluationError::Evaluation(error)) => Err(error),
        Err(SqlConstantEvaluationError::Control(error)) => return Err(error.into()),
        Err(SqlConstantEvaluationError::Constant(error)) => return Err(error.into()),
        Err(SqlConstantEvaluationError::InvalidType(error)) => {
            return Err(SqlCompileError::InvalidRequest(error.to_string()));
        }
        Err(SqlConstantEvaluationError::Preparation(error)) => {
            use novarocks_functions::KernelFailure;
            use novarocks_type_contract::CompileControlError;
            return Err(match error {
                KernelFailure::Cancelled => CompileControlError::Cancelled.into(),
                KernelFailure::DeadlineExceeded => CompileControlError::DeadlineExceeded.into(),
                KernelFailure::ResourceExhausted => CompileControlError::ResourceExhausted.into(),
                error @ KernelFailure::InvalidProgram(_) => {
                    SqlCompileError::InvalidRequest(error.to_string())
                }
                other => SqlCompileError::Compilation(other.to_string()),
            });
        }
    };
    work.step()?;
    Ok(match evaluated {
        Ok(Some(value)) => Some(arena.intern_observed(
            ScalarNode::Constant(value),
            arena.value_type(id).clone(),
            work.control(),
        )?),
        // The evaluator declined this shape.
        Ok(None) => None,
        // Fail-open: keep the original expression and swallow the error. The
        // runtime is still allowed to produce a value — or its own error — for
        // this expression, so a failed fold must never become a planning error.
        Err(_) => None,
    })
}

#[cfg(test)]
fn test_fold_value(
    request: &FoldRequest,
    value: crate::common::LiteralValue,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<Option<novarocks_functions::ConstantValue>, SqlConstantEvaluationError> {
    Ok(Some(crate::constant::admit_syntax_constant(
        &value,
        &request.result_type,
        request.constant_policy,
        control,
    )?))
}

#[cfg(test)]
fn try_fold_test_node(
    arena: &mut ScalarArena,
    id: ScalarId,
    evaluator: &'static dyn SqlConstantEvaluator,
) -> Option<ScalarId> {
    let mut work = CompileCheckpoints::try_new(
        crate::optimizer::rewrite::context::unbounded_rewrite_test_control(),
        CompilePhase::Validate,
    )
    .unwrap();
    let result = try_fold_node(arena, id, evaluator, &mut work).unwrap();
    work.finish().unwrap();
    result
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use arrow::datatypes::{DataType, TimeUnit};

    use super::*;
    use crate::column_id::ColumnId;
    use crate::common::{BinOp, JoinKind, LiteralValue};
    use crate::optimizer::operator::{
        FilterOp, LogicalJoinOp, ProjectOp, ScalarProjectItem, SortOp, ValuesOp,
    };

    // -- fake evaluator ----------------------------------------------------

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum FakeMode {
        /// Fold integer `+`/`*`, and any function call to a marker literal.
        Fold,
        /// Always decline.
        Decline,
        /// Always fail.
        Fail,
    }

    /// Marker value returned for a folded `FunctionCall`, so a test can tell
    /// "the function was folded" from "the function was gated".
    const FUNCTION_MARKER: i64 = 4242;

    #[derive(Debug)]
    struct FakeEvaluator {
        mode: FakeMode,
        calls: AtomicUsize,
    }

    impl FakeEvaluator {
        fn new(mode: FakeMode) -> &'static Self {
            Box::leak(Box::new(Self {
                mode,
                calls: AtomicUsize::new(0),
            }))
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    impl SqlConstantEvaluator for FakeEvaluator {
        fn eval_scalar(
            &self,
            request: &FoldRequest,
            control: &dyn novarocks_type_contract::PureCompileControl,
        ) -> Result<Option<novarocks_functions::ConstantValue>, SqlConstantEvaluationError>
        {
            self.calls.fetch_add(1, Ordering::SeqCst);
            match self.mode {
                FakeMode::Decline => return Ok(None),
                FakeMode::Fail => return Err("fake evaluator failure".to_string().into()),
                FakeMode::Fold => {}
            }
            match &request.kind {
                FoldNodeKind::BinaryOp(op @ (BinOp::Add | BinOp::Mul), _) => {
                    let mut values = Vec::new();
                    for arg in &request.args {
                        let Some(value) = arg.value.signed_integer_observed(
                            CompilePhase::FunctionSpecialization,
                            control,
                        )?
                        else {
                            return Ok(None);
                        };
                        values.push(value);
                    }
                    if values.len() != 2 {
                        return Ok(None);
                    }
                    let folded = match op {
                        BinOp::Add => values[0] + values[1],
                        _ => values[0] * values[1],
                    };
                    test_fold_value(request, LiteralValue::Int(folded), control)
                }
                FoldNodeKind::Function { .. } => test_fold_value(
                    request,
                    if *request.result_type.data_type() == DataType::Utf8 {
                        LiteralValue::String(FUNCTION_MARKER.to_string())
                    } else {
                        LiteralValue::Int(FUNCTION_MARKER)
                    },
                    control,
                ),
                _ => Ok(None),
            }
        }
    }

    // -- fixture helpers ---------------------------------------------------

    struct Fixture {
        arena: Rc<RefCell<ScalarArena>>,
        ctx: RewriteContext<'static>,
        evaluator: Option<&'static FakeEvaluator>,
    }

    impl Fixture {
        fn with_mode(mode: FakeMode) -> Self {
            let evaluator = FakeEvaluator::new(mode);
            let arena = Rc::new(RefCell::new(ScalarArena::new()));
            let mut ctx = RewriteContext::for_query(Vec::<String>::new());
            ctx.set_function_catalog(crate::functions::test_function_catalog_snapshot());
            ctx.set_scalar_arena(Rc::clone(&arena));
            ctx.set_function_catalog(crate::functions::builtin_sql_function_catalog().snapshot());
            ctx.set_constant_evaluator(evaluator);
            Self {
                arena,
                ctx,
                evaluator: Some(evaluator),
            }
        }

        /// A context with no evaluator attached at all.
        fn without_evaluator() -> Self {
            let arena = Rc::new(RefCell::new(ScalarArena::new()));
            let mut ctx = RewriteContext::for_query(Vec::<String>::new());
            ctx.set_function_catalog(crate::functions::test_function_catalog_snapshot());
            ctx.set_scalar_arena(Rc::clone(&arena));
            ctx.set_function_catalog(crate::functions::builtin_sql_function_catalog().snapshot());
            Self {
                arena,
                ctx,
                evaluator: None,
            }
        }

        fn intern(
            &self,
            node: ScalarNode,
            value_type: novarocks_type_contract::FunctionValueType,
        ) -> ScalarId {
            self.arena.borrow_mut().intern(node, value_type)
        }

        fn int_literal(&self, value: i64) -> ScalarId {
            self.intern(
                ScalarNode::Literal(HashableLiteral(LiteralValue::Int(value))),
                novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
            )
        }

        fn column(&self, id: u32) -> ScalarId {
            self.intern(
                ScalarNode::ColumnRef(ColumnId::new_for_test(id)),
                novarocks_type_contract::FunctionValueType::new(DataType::Int64, true),
            )
        }

        fn function_call(
            &self,
            name: &str,
            args: Vec<ScalarId>,
            distinct: bool,
            volatility: FunctionVolatility,
            data_type: DataType,
            nullable: bool,
        ) -> ScalarId {
            let binding = crate::optimizer::scalar::test_function_binding(
                &self.arena.borrow(),
                name,
                &args,
                data_type.clone(),
                nullable,
                volatility,
            );
            self.intern(
                ScalarNode::FunctionCall {
                    binding,
                    name: name.to_string(),
                    args,
                    distinct,
                    volatility,
                },
                novarocks_type_contract::FunctionValueType::new(data_type, nullable),
            )
        }

        fn binary(&self, op: BinOp, left: ScalarId, right: ScalarId) -> ScalarId {
            self.binary_typed(op, left, right, DataType::Int64, false)
        }

        fn binary_typed(
            &self,
            op: BinOp,
            left: ScalarId,
            right: ScalarId,
            data_type: DataType,
            nullable: bool,
        ) -> ScalarId {
            self.intern(
                ScalarNode::BinaryOp {
                    op,
                    left,
                    right,
                    decimal_overflow_policy:
                        novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
                },
                novarocks_type_contract::FunctionValueType::new(data_type, nullable),
            )
        }

        fn node(&self, id: ScalarId) -> ScalarNode {
            self.arena.borrow().node(id).clone()
        }

        fn data_type(&self, id: ScalarId) -> DataType {
            self.arena.borrow().data_type(id).clone()
        }

        fn nullable(&self, id: ScalarId) -> bool {
            self.arena.borrow().nullable(id)
        }

        fn apply(&mut self, plan: OptExpr) -> RewriteResult {
            FoldConstant
                .apply(plan, &mut self.ctx)
                .expect("unbounded fold fixture must succeed")
        }

        fn matches(&self, plan: &OptExpr) -> bool {
            FoldConstant.matches(plan, &self.ctx)
        }

        fn calls(&self) -> usize {
            self.evaluator.map(|e| e.calls()).unwrap_or(0)
        }
    }

    fn project(expr: ScalarId) -> OptExpr {
        OptExpr::leaf(Operator::LogicalProject(ProjectOp {
            items: vec![ScalarProjectItem {
                expr,
                output_name: "c".to_string(),
                output_column_id: ColumnId::new_for_test(1),
                expr_display: None,
            }],
            output_qualifier: None,
        }))
    }

    fn project_expr(plan: &OptExpr) -> ScalarId {
        let Operator::LogicalProject(project) = &plan.op else {
            panic!("expected LogicalProject, got {:?}", plan.op);
        };
        project.items[0].expr
    }

    fn changed(result: RewriteResult) -> OptExpr {
        match result {
            RewriteResult::Changed(plan) => plan,
            other => panic!("expected Changed, got {other:?}"),
        }
    }

    fn assert_int_literal(fixture: &Fixture, id: ScalarId, expected: i64) {
        match fixture.node(id) {
            ScalarNode::Constant(value) => {
                assert_eq!(
                    value
                        .signed_integer_observed(
                            CompilePhase::Validate,
                            crate::optimizer::test_optimizer_control()
                        )
                        .unwrap(),
                    Some(expected)
                );
            }
            ScalarNode::Literal(HashableLiteral(LiteralValue::Int(value))) => {
                assert_eq!(value, expected)
            }
            other => panic!("expected checked integer value {expected}, got {other:?}"),
        }
    }

    struct FoldControl {
        observations: std::sync::Mutex<Vec<(CompilePhase, u32)>>,
        failure: Option<novarocks_type_contract::CompileControlError>,
        fail_at: usize,
    }
    impl novarocks_type_contract::PureCompileControl for FoldControl {
        fn checkpoint(
            &self,
            phase: CompilePhase,
            units: u32,
        ) -> Result<(), novarocks_type_contract::CompileControlError> {
            // The rule observes Validate; checked CV authors and selected
            // readers observe FunctionSpecialization on the same request.
            assert!(matches!(
                phase,
                CompilePhase::Validate | CompilePhase::FunctionSpecialization
            ));
            let mut observations = self.observations.lock().unwrap();
            observations.push((phase, units));
            if observations.len() == self.fail_at
                && let Some(error) = self.failure
            {
                return Err(error);
            }
            Ok(())
        }
    }
    fn apply_with_control(
        fixture: &Fixture,
        plan: OptExpr,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<RewriteResult, SqlCompileError> {
        apply_with_evaluator(
            fixture,
            plan,
            fixture
                .evaluator
                .map(|value| value as &'static dyn SqlConstantEvaluator),
            control,
        )
    }

    fn apply_with_evaluator(
        fixture: &Fixture,
        plan: OptExpr,
        evaluator: Option<&'static dyn SqlConstantEvaluator>,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<RewriteResult, SqlCompileError> {
        let mut ctx = RewriteContext::for_query_with_settings(
            Default::default(),
            novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            control,
        );
        ctx.set_function_catalog(crate::functions::test_function_catalog_snapshot());
        ctx.set_scalar_arena(Rc::clone(&fixture.arena));
        ctx.set_function_catalog(crate::functions::builtin_sql_function_catalog().snapshot());
        if let Some(evaluator) = evaluator {
            ctx.set_constant_evaluator(evaluator);
        }
        FoldConstant.apply(plan, &mut ctx)
    }

    #[test]
    fn fold_control_observes_real_nodes_cached_edges_and_final_publication() {
        use novarocks_type_contract::{CompileControlError, PureCompileControl};
        for shared in [false, true] {
            let fixture = Fixture::with_mode(FakeMode::Fold);
            let one = fixture.int_literal(1);
            let items = (0..320)
                .map(|index| {
                    let value = fixture.int_literal(if shared { 0 } else { index });
                    ScalarProjectItem {
                        expr: fixture.binary(BinOp::Add, value, one),
                        output_name: format!("c{index}"),
                        output_column_id: ColumnId::new_for_test(index as u32 + 1),
                        expr_display: None,
                    }
                })
                .collect();
            let plan = OptExpr::leaf(Operator::LogicalProject(ProjectOp {
                items,
                output_qualifier: None,
            }));
            let owner = FoldControl {
                observations: Default::default(),
                failure: None,
                fail_at: usize::MAX,
            };
            let original_arena = fixture.arena.borrow().clone();
            let result = apply_with_control(&fixture, plan.clone(), &owner).unwrap();
            let rewritten = changed(result);
            let Operator::LogicalProject(project) = rewritten.op else {
                panic!("project")
            };
            for (index, item) in project.items.iter().enumerate() {
                assert_int_literal(
                    &fixture,
                    item.expr,
                    if shared { 1 } else { index as i64 + 1 },
                );
            }
            assert_eq!(fixture.calls(), if shared { 1 } else { 320 });
            let observations = owner.observations.lock().unwrap().clone();
            assert_eq!(observations[0], (CompilePhase::Validate, 0));
            assert_eq!(observations.last().unwrap().0, CompilePhase::Validate);
            assert!(observations.iter().all(|(_, units)| *units <= 256));
            assert!(observations.iter().map(|(_, units)| units).sum::<u32>() >= 320);
            assert!(
                observations
                    .iter()
                    .any(|(phase, _)| *phase == CompilePhase::FunctionSpecialization)
            );
            // Derive refusal locations from actual callbacks. In particular,
            // CV entry/tail callbacks are not cached-edge Validate quanta.
            let mut refusal_points = std::collections::BTreeSet::from([1, 2, observations.len()]);
            for phase in [CompilePhase::Validate, CompilePhase::FunctionSpecialization] {
                let first = observations.iter().position(|(p, _)| *p == phase).unwrap();
                let last = observations.iter().rposition(|(p, _)| *p == phase).unwrap();
                refusal_points.insert(first + 1);
                refusal_points.insert(last + 1);
            }
            if let Some(quantum) = observations.iter().position(|(_, units)| *units == 256) {
                refusal_points.insert(quantum + 1);
            }
            if shared {
                // Cached edges stay in the rule's pending Validate scope;
                // fresh CV authors instead flush that scope at each handoff.
                assert!(observations.iter().any(|(_, units)| *units == 256));
            }
            for failure in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                for fail_at in refusal_points.iter().copied() {
                    *fixture.arena.borrow_mut() = original_arena.clone();
                    let owner = FoldControl {
                        observations: Default::default(),
                        failure: Some(failure),
                        fail_at,
                    };
                    assert!(
                        matches!(apply_with_control(&fixture, plan.clone(), &owner), Err(error) if error == SqlCompileError::from(failure))
                    );
                    assert_eq!(*owner.observations.lock().unwrap(), observations[..fail_at]);
                    // A later successful observation cannot recover this result.
                    owner.checkpoint(CompilePhase::Validate, 0).unwrap();
                }
            }
        }
    }

    #[test]
    fn fold_control_covers_no_evaluator_and_declined_or_failed_folds() {
        use novarocks_type_contract::CompileControlError;
        for fixture in [
            Fixture::without_evaluator(),
            Fixture::with_mode(FakeMode::Decline),
            Fixture::with_mode(FakeMode::Fail),
        ] {
            let one = fixture.int_literal(1);
            let sum = fixture.binary(BinOp::Add, one, one);
            for failure in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                for fail_at in [1, 2] {
                    let owner = FoldControl {
                        observations: Default::default(),
                        failure: Some(failure),
                        fail_at,
                    };
                    assert!(
                        matches!(apply_with_control(&fixture, project(sum), &owner), Err(error) if error == SqlCompileError::from(failure))
                    );
                    assert_eq!(owner.observations.lock().unwrap().len(), fail_at);
                }
            }
        }
    }

    #[test]
    fn folded_output_and_arena_do_not_retain_request_control() {
        let fixture = Fixture::with_mode(FakeMode::Fold);
        let one = fixture.int_literal(1);
        let sum = fixture.binary(BinOp::Add, one, one);
        let owner = std::sync::Arc::new(FoldControl {
            observations: Default::default(),
            failure: None,
            fail_at: usize::MAX,
        });
        let weak = std::sync::Arc::downgrade(&owner);
        let rewritten =
            changed(apply_with_control(&fixture, project(sum), owner.as_ref()).unwrap());
        assert!(
            owner
                .observations
                .lock()
                .unwrap()
                .iter()
                .any(|(phase, _)| *phase == CompilePhase::FunctionSpecialization)
        );
        drop(owner);
        assert!(weak.upgrade().is_none());
        assert_int_literal(&fixture, project_expr(&rewritten), 2);
    }

    #[test]
    fn evaluator_receives_original_phase_control_and_control_errors_never_decline() {
        use novarocks_type_contract::{CompileControlError, PureCompileControl};
        struct ObservedEvaluator;
        impl SqlConstantEvaluator for ObservedEvaluator {
            fn eval_scalar(
                &self,
                request: &FoldRequest,
                control: &dyn PureCompileControl,
            ) -> Result<Option<novarocks_functions::ConstantValue>, SqlConstantEvaluationError>
            {
                let mut work =
                    CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
                let mut sum = 0;
                for argument in &request.args {
                    work.flush()?;
                    let value = argument
                        .value
                        .signed_integer_observed(CompilePhase::FunctionSpecialization, control)?
                        .expect("expected real integer fold arguments");
                    sum += value;
                    work.step()?;
                }
                work.finish()?;
                test_fold_value(request, LiteralValue::Int(sum), control)
            }
        }
        static EVALUATOR: ObservedEvaluator = ObservedEvaluator;
        struct Owner {
            checks: std::sync::Mutex<Vec<(CompilePhase, u32)>>,
            failure: Option<(usize, CompileControlError)>,
        }
        impl PureCompileControl for Owner {
            fn checkpoint(
                &self,
                phase: CompilePhase,
                units: u32,
            ) -> Result<(), CompileControlError> {
                let mut checks = self.checks.lock().unwrap();
                let index = checks.len();
                checks.push((phase, units));
                if let Some((fail_at, error)) = self.failure
                    && index == fail_at
                {
                    return Err(error);
                }
                Ok(())
            }
        }
        let fixture = Fixture::without_evaluator();
        let lhs = fixture.int_literal(1);
        let rhs = fixture.int_literal(2);
        let sum = fixture.binary(BinOp::Add, lhs, rhs);
        let owner = Owner {
            checks: Default::default(),
            failure: None,
        };
        let original_arena = fixture.arena.borrow().clone();
        let rewritten = changed(
            apply_with_evaluator(&fixture, project(sum), Some(&EVALUATOR), &owner).unwrap(),
        );
        assert_int_literal(&fixture, project_expr(&rewritten), 3);
        let checks = owner.checks.lock().unwrap().clone();
        assert!(checks.iter().all(|(_, units)| *units <= 256));
        assert!(
            checks.iter().any(
                |(phase, units)| *phase == CompilePhase::FunctionSpecialization && *units == 0
            )
        );
        assert!(
            checks
                .iter()
                .any(|(phase, units)| *phase == CompilePhase::FunctionSpecialization && *units > 0)
        );
        let first_owner = checks
            .iter()
            .position(|(phase, _)| *phase == CompilePhase::FunctionSpecialization)
            .unwrap();
        assert_eq!(checks[first_owner - 1].0, CompilePhase::Validate);
        assert!(
            checks[first_owner - 1].1 > 0,
            "caller work must flush before the owner"
        );
        for error in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for fail_at in 0..checks.len() {
                *fixture.arena.borrow_mut() = original_arena.clone();
                let owner = Owner {
                    checks: Default::default(),
                    failure: Some((fail_at, error)),
                };
                assert!(
                    matches!(apply_with_evaluator(&fixture,project(sum),Some(&EVALUATOR),&owner),
                    Err(actual) if actual == SqlCompileError::from(error))
                );
                assert_eq!(*owner.checks.lock().unwrap(), checks[..=fail_at]);
            }
        }
    }

    #[test]
    fn legacy_evaluation_error_text_does_not_impersonate_typed_request_failure() {
        struct LegacyError(AtomicUsize);
        impl SqlConstantEvaluator for LegacyError {
            fn eval_scalar(
                &self,
                _: &FoldRequest,
                _: &dyn novarocks_type_contract::PureCompileControl,
            ) -> Result<Option<novarocks_functions::ConstantValue>, SqlConstantEvaluationError>
            {
                self.0.fetch_add(1, Ordering::SeqCst);
                Err(SqlConstantEvaluationError::Evaluation(
                    "pure compilation was cancelled".to_string(),
                ))
            }
        }
        static EVALUATOR: LegacyError = LegacyError(AtomicUsize::new(0));
        let fixture = Fixture::without_evaluator();
        let one = fixture.int_literal(1);
        let sum = fixture.binary(BinOp::Add, one, one);
        let owner = FoldControl {
            observations: Default::default(),
            failure: None,
            fail_at: usize::MAX,
        };
        assert!(matches!(
            apply_with_evaluator(&fixture, project(sum), Some(&EVALUATOR), &owner).unwrap(),
            RewriteResult::Unchanged
        ));
        assert!(matches!(fixture.node(sum), ScalarNode::BinaryOp { .. }));
        assert_eq!(EVALUATOR.0.load(Ordering::SeqCst), 1);
        let observations = owner.observations.lock().unwrap();
        assert_eq!(observations.first(), Some(&(CompilePhase::Validate, 0)));
        assert_eq!(observations.last().unwrap().0, CompilePhase::Validate);
        assert!(
            observations
                .iter()
                .any(|(phase, _)| *phase == CompilePhase::FunctionSpecialization)
        );
    }

    // -- tests -------------------------------------------------------------

    #[test]
    fn folds_integer_addition_to_single_literal() {
        let mut fixture = Fixture::with_mode(FakeMode::Fold);
        let one = fixture.int_literal(1);
        let sum = fixture.binary(BinOp::Add, one, one);
        let plan = project(sum);

        assert!(fixture.matches(&plan));
        let rewritten = changed(fixture.apply(plan));

        let folded = project_expr(&rewritten);
        assert_int_literal(&fixture, folded, 2);
        // Type metadata is preserved verbatim.
        assert_eq!(fixture.data_type(folded), DataType::Int64);
        assert!(!fixture.nullable(folded));
    }

    #[test]
    fn preserves_non_default_type_metadata_of_the_folded_node() {
        let mut fixture = Fixture::with_mode(FakeMode::Fold);
        let one = fixture.int_literal(1);
        // The node's own metadata is Int32 / nullable, unlike its children.
        let sum = fixture.binary_typed(BinOp::Add, one, one, DataType::Int32, true);
        let plan = project(sum);

        let rewritten = changed(fixture.apply(plan));
        let folded = project_expr(&rewritten);

        assert_int_literal(&fixture, folded, 2);
        assert_eq!(fixture.data_type(folded), DataType::Int32);
        assert!(fixture.nullable(folded));
    }

    #[test]
    fn preserves_sec_to_time_provenance_but_folds_its_numeric_argument() {
        let mut fixture = Fixture::with_mode(FakeMode::Fold);
        let negative = fixture.int_literal(-2);
        let one = fixture.int_literal(1);
        let seconds = fixture.binary(BinOp::Add, negative, one);
        let source = fixture.function_call(
            "sec_to_time",
            vec![seconds],
            false,
            FunctionVolatility::Immutable,
            DataType::Utf8,
            true,
        );
        let cast_source = fixture.intern(
            ScalarNode::Cast {
                child: source,
                target: DataType::Utf8,
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            },
            novarocks_type_contract::FunctionValueType::new(DataType::Utf8, true),
        );
        let wrapped = fixture.intern(
            ScalarNode::Nested(cast_source),
            novarocks_type_contract::FunctionValueType::new(DataType::Utf8, true),
        );
        let consumer = fixture.function_call(
            "time_to_sec",
            vec![wrapped],
            false,
            FunctionVolatility::Immutable,
            DataType::Int64,
            true,
        );

        let rewritten = changed(fixture.apply(project(consumer)));
        let ScalarNode::FunctionCall { args, .. } = fixture.node(project_expr(&rewritten)) else {
            panic!("source-sensitive consumer must remain a call");
        };
        let ScalarNode::Nested(cast_source) = fixture.node(args[0]) else {
            panic!("source wrapper must remain intact");
        };
        let ScalarNode::Cast { child: source, .. } = fixture.node(cast_source) else {
            panic!("source cast must remain intact");
        };
        let ScalarNode::FunctionCall { name, args, .. } = fixture.node(source) else {
            panic!("SEC_TO_TIME source must not become a string literal");
        };
        assert_eq!(name, "sec_to_time");
        assert_int_literal(&fixture, args[0], -1);
        assert_eq!(fixture.calls(), 1, "only the numeric argument is folded");
    }

    #[test]
    fn shared_sec_to_time_can_fold_without_erasing_roundtrip_source() {
        let fixture = Fixture::with_mode(FakeMode::Fold);
        let seconds = fixture.int_literal(-1);
        let source = fixture.function_call(
            "sec_to_time",
            vec![seconds],
            false,
            FunctionVolatility::Immutable,
            DataType::Utf8,
            true,
        );
        let consumer = fixture.function_call(
            "time_to_sec",
            vec![source],
            false,
            FunctionVolatility::Immutable,
            DataType::Int64,
            true,
        );
        let mut arena = fixture.arena.borrow_mut();
        let mut memo = HashMap::new();
        let evaluator = fixture.evaluator.unwrap();
        let mut work = CompileCheckpoints::try_new(
            crate::optimizer::rewrite::context::unbounded_rewrite_test_control(),
            CompilePhase::Validate,
        )
        .unwrap();
        let standalone =
            fold_scalar_id(&mut arena, source, evaluator, &mut memo, &mut work).unwrap();
        let roundtrip =
            fold_scalar_id(&mut arena, consumer, evaluator, &mut memo, &mut work).unwrap();
        work.finish().unwrap();
        assert!(matches!(arena.node(standalone), ScalarNode::Constant(_)));
        let ScalarNode::FunctionCall { args, .. } = arena.node(roundtrip) else {
            panic!("roundtrip must not reuse the standalone folded literal");
        };
        assert_eq!(args, &[source]);
        assert_eq!(fixture.calls(), 1);
    }

    #[test]
    fn folds_nested_arithmetic_fully() {
        let mut fixture = Fixture::with_mode(FakeMode::Fold);
        let one = fixture.int_literal(1);
        let two = fixture.int_literal(2);
        let three = fixture.int_literal(3);
        let sum = fixture.binary(BinOp::Add, one, two);
        let product = fixture.binary(BinOp::Mul, sum, three);
        let plan = project(product);

        let rewritten = changed(fixture.apply(plan));
        let folded = project_expr(&rewritten);

        assert_int_literal(&fixture, folded, 9);
        assert_eq!(fixture.calls(), 2, "one call per folded node");
    }

    #[test]
    fn does_not_fold_expression_referencing_a_column() {
        let mut fixture = Fixture::with_mode(FakeMode::Fold);
        let column = fixture.column(7);
        let one = fixture.int_literal(1);
        let sum = fixture.binary(BinOp::Add, column, one);
        let plan = project(sum);

        assert!(matches!(fixture.apply(plan), RewriteResult::Unchanged));
        assert_eq!(fixture.calls(), 0, "a column child is never foldable");
    }

    #[test]
    fn folds_constant_subtree_under_a_column_reference() {
        let mut fixture = Fixture::with_mode(FakeMode::Fold);
        let one = fixture.int_literal(1);
        let two = fixture.int_literal(2);
        let column = fixture.column(7);
        let constant_sum = fixture.binary(BinOp::Add, one, two);
        let outer = fixture.binary(BinOp::Add, constant_sum, column);
        let plan = project(outer);

        let rewritten = changed(fixture.apply(plan));
        let folded = project_expr(&rewritten);

        // `1 + 2 + col` becomes `3 + col`: the constant sub-tree collapsed but
        // the outer node still references a column and stays put.
        let ScalarNode::BinaryOp {
            op, left, right, ..
        } = fixture.node(folded)
        else {
            panic!("expected a BinaryOp root, got {:?}", fixture.node(folded));
        };
        assert_eq!(op, BinOp::Add);
        assert_int_literal(&fixture, left, 3);
        assert!(matches!(fixture.node(right), ScalarNode::ColumnRef(_)));
    }

    #[test]
    fn does_not_fold_volatile_function_call() {
        let mut fixture = Fixture::with_mode(FakeMode::Fold);
        let one = fixture.int_literal(1);
        let call = fixture.function_call(
            "rand",
            vec![one],
            false,
            FunctionVolatility::Volatile,
            DataType::Int64,
            false,
        );
        let plan = project(call);

        assert!(matches!(fixture.apply(plan), RewriteResult::Unchanged));
        assert_eq!(fixture.calls(), 0, "a volatile call never reaches the port");
    }

    #[test]
    fn does_not_fold_side_effecting_sleep_from_catalog_volatility() {
        // `sleep(10)` has every surface property of a foldable constant: an
        // immutable literal argument and a `Boolean` output that encodes onto
        // the wire. Its only observable behavior is the delay it imposes on
        // whichever thread evaluates it, so folding it on the frontend blocked
        // the planner for the sleep duration and then shipped a bare `true` to
        // the backends -- the delay vanished from execution.
        //
        // The gate is the catalog's volatility classification, so read it from
        // the catalog here instead of hardcoding `Volatile`: this fails if
        // `sleep` ever drifts back to `Immutable`.
        for name in ["sleep", "SLEEP"] {
            let mut fixture = Fixture::with_mode(FakeMode::Fold);
            let ten = fixture.int_literal(10);
            let call = fixture.function_call(
                name,
                vec![ten],
                false,
                crate::functions::builtin_function_volatility(name),
                DataType::Boolean,
                false,
            );
            let plan = project(call);

            assert!(
                matches!(fixture.apply(plan), RewriteResult::Unchanged),
                "{name} must not be folded"
            );
            assert_eq!(fixture.calls(), 0, "{name} must not reach the port");
        }
    }

    #[test]
    fn does_not_fold_distinct_function_call() {
        let mut fixture = Fixture::with_mode(FakeMode::Fold);
        let one = fixture.int_literal(1);
        let call = fixture.function_call(
            "some_agg_like_call",
            vec![one],
            true,
            FunctionVolatility::Immutable,
            DataType::Int64,
            false,
        );
        let plan = project(call);

        assert!(matches!(fixture.apply(plan), RewriteResult::Unchanged));
        assert_eq!(fixture.calls(), 0);
    }

    #[test]
    fn folds_immutable_function_call() {
        let mut fixture = Fixture::with_mode(FakeMode::Fold);
        let one = fixture.int_literal(1);
        let call = fixture.function_call(
            "abs",
            vec![one],
            false,
            FunctionVolatility::Immutable,
            DataType::Int64,
            false,
        );
        let plan = project(call);

        let rewritten = changed(fixture.apply(plan));
        assert_int_literal(&fixture, project_expr(&rewritten), FUNCTION_MARKER);
    }

    #[test]
    fn does_not_fold_denylisted_function_even_when_marked_immutable() {
        for name in [
            "from_unixtime",
            "FROM_UNIXTIME",
            "hour_from_unixtime",
            "now",
        ] {
            let mut fixture = Fixture::with_mode(FakeMode::Fold);
            let one = fixture.int_literal(1);
            let call = fixture.function_call(
                name,
                vec![one],
                false,
                // Deliberately Immutable: the denylist, not volatility, is
                // what must stop this fold.
                FunctionVolatility::Immutable,
                DataType::Int64,
                false,
            );
            let plan = project(call);

            assert!(
                matches!(fixture.apply(plan), RewriteResult::Unchanged),
                "{name} must not be folded"
            );
            assert_eq!(fixture.calls(), 0, "{name} must not reach the port");
        }
    }

    #[test]
    fn does_not_fold_byte_carrying_string_function() {
        // These carry raw bytes inside a Utf8 value. Folding one turns it into
        // an ordinary string literal, after which a consumer such as
        // `to_base64` reads different bytes than the runtime produced.
        for name in ["aes_encrypt", "AES_ENCRYPT", "from_base64", "unhex"] {
            let mut fixture = Fixture::with_mode(FakeMode::Fold);
            let one = fixture.int_literal(1);
            let call = fixture.function_call(
                name,
                vec![one],
                false,
                FunctionVolatility::Immutable,
                DataType::Utf8,
                false,
            );
            let plan = project(call);

            assert!(
                matches!(fixture.apply(plan), RewriteResult::Unchanged),
                "{name} must not be folded"
            );
            assert_eq!(fixture.calls(), 0, "{name} must not reach the port");
        }
    }

    #[test]
    fn does_not_fold_node_whose_output_type_is_not_wire_encodable() {
        let mut fixture = Fixture::with_mode(FakeMode::Fold);
        let one = fixture.int_literal(1);
        let sum = fixture.binary_typed(
            BinOp::Add,
            one,
            one,
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        );
        let plan = project(sum);

        assert!(matches!(fixture.apply(plan), RewriteResult::Unchanged));
        assert_eq!(
            fixture.calls(),
            0,
            "the wire whitelist is checked before the port is called"
        );
    }

    #[test]
    fn wire_whitelist_accepts_exactly_the_decodable_literal_types() {
        for accepted in [
            DataType::Boolean,
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
            DataType::Utf8,
            DataType::LargeUtf8,
            DataType::Binary,
            DataType::LargeBinary,
            DataType::Date32,
            DataType::Decimal128(38, 9),
            DataType::Decimal256(76, 10),
            DataType::FixedSizeBinary(novarocks_types::largeint::LARGEINT_BYTE_WIDTH),
        ] {
            assert!(
                is_wire_encodable_literal_type(&accepted),
                "{accepted:?} must be foldable"
            );
        }

        for rejected in [
            DataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            DataType::Date64,
            DataType::Null,
            DataType::List(std::sync::Arc::new(arrow::datatypes::Field::new(
                "item",
                DataType::Int64,
                true,
            ))),
            DataType::Struct(arrow::datatypes::Fields::empty()),
        ] {
            assert!(
                !is_wire_encodable_literal_type(&rejected),
                "{rejected:?} must not be foldable"
            );
        }
    }

    #[test]
    fn evaluator_error_leaves_expression_unchanged_without_planning_error() {
        let mut fixture = Fixture::with_mode(FakeMode::Fail);
        let one = fixture.int_literal(1);
        let sum = fixture.binary(BinOp::Add, one, one);
        let plan = project(sum);

        // `apply` returns Ok — a fold failure is never a planning error.
        let result = FoldConstant
            .apply(plan, &mut fixture.ctx)
            .expect("evaluator errors must be swallowed");
        assert!(matches!(result, RewriteResult::Unchanged));
        assert_eq!(fixture.calls(), 1, "the port was consulted and failed");
    }

    #[test]
    fn evaluator_declining_leaves_expression_unchanged() {
        let mut fixture = Fixture::with_mode(FakeMode::Decline);
        let one = fixture.int_literal(1);
        let sum = fixture.binary(BinOp::Add, one, one);
        let plan = project(sum);

        assert!(matches!(fixture.apply(plan), RewriteResult::Unchanged));
        assert_eq!(fixture.calls(), 1);
    }

    #[test]
    fn rule_is_a_noop_without_a_constant_evaluator() {
        let mut fixture = Fixture::without_evaluator();
        let one = fixture.int_literal(1);
        let sum = fixture.binary(BinOp::Add, one, one);
        let plan = project(sum);

        assert!(
            !fixture.matches(&plan),
            "matches must gate on the evaluator"
        );
        assert!(matches!(fixture.apply(plan), RewriteResult::Unchanged));
    }

    #[test]
    fn matches_requires_at_least_one_scalar_field() {
        let fixture = Fixture::with_mode(FakeMode::Fold);
        let empty_values = OptExpr::leaf(Operator::LogicalValues(ValuesOp {
            rows: vec![],
            columns: vec![],
        }));
        assert!(!fixture.matches(&empty_values));
    }

    #[test]
    fn folds_filter_predicate() {
        let mut fixture = Fixture::with_mode(FakeMode::Fold);
        let one = fixture.int_literal(1);
        let sum = fixture.binary(BinOp::Add, one, one);
        let plan = OptExpr::leaf(Operator::LogicalFilter(FilterOp { predicate: sum }));

        let rewritten = changed(fixture.apply(plan));
        let Operator::LogicalFilter(filter) = &rewritten.op else {
            panic!("expected LogicalFilter");
        };
        assert_int_literal(&fixture, filter.predicate, 2);
    }

    #[test]
    fn folds_values_rows_join_condition_and_sort_keys() {
        let mut fixture = Fixture::with_mode(FakeMode::Fold);
        let one = fixture.int_literal(1);
        let two = fixture.int_literal(2);
        let sum = fixture.binary(BinOp::Add, one, two);

        let values = OptExpr::leaf(Operator::LogicalValues(ValuesOp {
            rows: vec![vec![sum]],
            columns: vec![],
        }));
        let rewritten = changed(fixture.apply(values));
        let Operator::LogicalValues(values) = &rewritten.op else {
            panic!("expected LogicalValues");
        };
        assert_int_literal(&fixture, values.rows[0][0], 3);

        let join = OptExpr::new(
            Operator::LogicalJoin(LogicalJoinOp {
                join_type: JoinKind::Inner,
                condition: Some(sum),
            }),
            vec![],
        );
        let rewritten = changed(fixture.apply(join));
        let Operator::LogicalJoin(join) = &rewritten.op else {
            panic!("expected LogicalJoin");
        };
        assert_int_literal(&fixture, join.condition.unwrap(), 3);

        let sort = OptExpr::leaf(Operator::LogicalSort(SortOp {
            items: vec![SortKey {
                expr: sum,
                asc: true,
                nulls_first: false,
                display: None,
            }],
            analytic_partition_exprs: vec![sum],
            partition_limit: None,
            topn_type: None,
        }));
        let rewritten = changed(fixture.apply(sort));
        let Operator::LogicalSort(sort) = &rewritten.op else {
            panic!("expected LogicalSort");
        };
        assert_int_literal(&fixture, sort.items[0].expr, 3);
        assert_int_literal(&fixture, sort.analytic_partition_exprs[0], 3);
    }

    #[test]
    fn collapsing_nested_literal_preserves_the_authored_value_domain() {
        let mut fixture = Fixture::with_mode(FakeMode::Fold);
        let value_type = novarocks_type_contract::FunctionValueType::try_with_logical_type(
            DataType::Utf8,
            false,
            novarocks_type_contract::ValueLogicalType::Json,
        )
        .unwrap();
        let literal = fixture.intern(
            ScalarNode::Literal(HashableLiteral(LiteralValue::String("{\"k\":1}".into()))),
            value_type.clone(),
        );
        let nested = fixture.intern(ScalarNode::Nested(literal), value_type.clone());
        let folded = project_expr(&changed(fixture.apply(project(nested))));
        assert_eq!(fixture.arena.borrow().value_type(folded), &value_type);
        assert_eq!(fixture.calls(), 0);
    }

    #[test]
    fn folds_through_nested_wrapper() {
        let mut fixture = Fixture::with_mode(FakeMode::Fold);
        let one = fixture.int_literal(1);
        let sum = fixture.binary(BinOp::Add, one, one);
        let nested = fixture.intern(
            ScalarNode::Nested(sum),
            novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
        );
        let plan = project(nested);

        let rewritten = changed(fixture.apply(plan));
        assert_int_literal(&fixture, project_expr(&rewritten), 2);
    }

    #[test]
    fn folds_constants_inside_a_case_without_folding_the_case_itself() {
        let mut fixture = Fixture::with_mode(FakeMode::Fold);
        let one = fixture.int_literal(1);
        let two = fixture.int_literal(2);
        let column = fixture.column(7);
        let when = fixture.binary_typed(BinOp::Eq, column, one, DataType::Boolean, true);
        let then = fixture.binary(BinOp::Add, one, two);
        let case = fixture.intern(
            ScalarNode::Case {
                operand: None,
                when_then: vec![(when, then)],
                else_expr: None,
            },
            novarocks_type_contract::FunctionValueType::new(DataType::Int64, true),
        );
        let plan = project(case);

        let rewritten = changed(fixture.apply(plan));
        let folded = project_expr(&rewritten);
        let ScalarNode::Case { when_then, .. } = fixture.node(folded) else {
            panic!("Case must not be folded away in v1");
        };
        assert_int_literal(&fixture, when_then[0].1, 3);
    }

    #[test]
    fn second_pass_over_a_folded_plan_reports_unchanged() {
        let mut fixture = Fixture::with_mode(FakeMode::Fold);
        let one = fixture.int_literal(1);
        let sum = fixture.binary(BinOp::Add, one, one);
        let plan = project(sum);

        let rewritten = changed(fixture.apply(plan));
        assert!(
            matches!(fixture.apply(rewritten), RewriteResult::Unchanged),
            "folding must reach a fixed point in one pass"
        );
    }
}

#[cfg(test)]
mod overflow_policy_tests {
    use super::*;
    use crate::common::{BinOp, LiteralValue};
    use novarocks_type_contract::DecimalOverflowPolicy::{OutputNull, ReportError};
    struct CheckedEvaluator;
    impl SqlConstantEvaluator for CheckedEvaluator {
        fn eval_scalar(
            &self,
            request: &FoldRequest,
            control: &dyn novarocks_type_contract::PureCompileControl,
        ) -> Result<Option<novarocks_functions::ConstantValue>, SqlConstantEvaluationError>
        {
            match request.kind {
                FoldNodeKind::BinaryOp(BinOp::Add, ReportError)
                | FoldNodeKind::Cast(ReportError) => Err("checked overflow".to_string().into()),
                FoldNodeKind::BinaryOp(BinOp::Add, OutputNull) | FoldNodeKind::Cast(OutputNull) => {
                    test_fold_value(request, LiteralValue::Null, control)
                }
                _ => Ok(None),
            }
        }
    }
    #[test]
    fn throwing_fold_request_retains_original_node_and_policy_and_nullable_sibling_folds() {
        static EVALUATOR: CheckedEvaluator = CheckedEvaluator;
        let mut arena = ScalarArena::new();
        let child = arena.intern(
            ScalarNode::Literal(HashableLiteral(LiteralValue::Decimal(
                "99999999999999999999999999999999999999".to_string(),
            ))),
            novarocks_type_contract::FunctionValueType::new(DataType::Decimal128(38, 0), false),
        );
        for cast in [false, true] {
            let node = |policy| {
                if cast {
                    ScalarNode::Cast {
                        child,
                        target: DataType::Decimal128(9, 0),
                        decimal_overflow_policy: policy,
                    }
                } else {
                    ScalarNode::BinaryOp {
                        op: BinOp::Add,
                        left: child,
                        right: child,
                        decimal_overflow_policy: policy,
                    }
                }
            };
            let data_type = if cast {
                DataType::Decimal128(9, 0)
            } else {
                DataType::Decimal128(38, 0)
            };
            let nullable = arena.intern(
                node(OutputNull),
                novarocks_type_contract::FunctionValueType::new(data_type.clone(), true),
            );
            let throwing = arena.intern(
                node(ReportError),
                novarocks_type_contract::FunctionValueType::new(data_type, true),
            );
            let original = arena.node(throwing).clone();
            assert!(try_fold_test_node(&mut arena, throwing, &EVALUATOR).is_none());
            assert_eq!(
                arena
                    .intern_observed(
                        original,
                        arena.value_type(throwing).clone(),
                        crate::optimizer::test_optimizer_control()
                    )
                    .unwrap(),
                throwing
            );
            let folded = try_fold_test_node(&mut arena, nullable, &EVALUATOR).unwrap();
            let ScalarNode::Constant(value) = arena.node(folded) else {
                panic!("checked NULL expected")
            };
            assert!(
                value
                    .is_null_observed(
                        CompilePhase::Validate,
                        crate::optimizer::test_optimizer_control()
                    )
                    .unwrap()
            );
            assert!(try_fold_test_node(&mut arena, throwing, &EVALUATOR).is_none());
        }
    }
    #[test]
    fn legacy_allow_disables_numeric_fold_without_disabling_pure_ast_reduction() {
        use novarocks_type_contract::DecimalOverflowPolicy::OutputNull;
        struct NullableEvaluator;
        impl SqlConstantEvaluator for NullableEvaluator {
            fn eval_scalar(
                &self,
                request: &FoldRequest,
                control: &dyn novarocks_type_contract::PureCompileControl,
            ) -> Result<Option<novarocks_functions::ConstantValue>, SqlConstantEvaluationError>
            {
                match request.kind {
                    FoldNodeKind::BinaryOp(BinOp::Mul, OutputNull)
                    | FoldNodeKind::Cast(OutputNull) => {
                        test_fold_value(request, LiteralValue::Null, control)
                    }
                    _ => Ok(None),
                }
            }
        }
        static NULLABLE: NullableEvaluator = NullableEvaluator;
        for cast in [false, true] {
            let mut arena = ScalarArena::new();
            let literal = arena.intern(
                ScalarNode::Literal(HashableLiteral(LiteralValue::Decimal(
                    "99999999999999999999999999999999999999".to_string(),
                ))),
                novarocks_type_contract::FunctionValueType::new(DataType::Decimal128(38, 0), false),
            );
            let original = if cast {
                ScalarNode::Cast {
                    child: literal,
                    target: DataType::Decimal128(9, 0),
                    decimal_overflow_policy: OutputNull,
                }
            } else {
                ScalarNode::BinaryOp {
                    left: literal,
                    right: literal,
                    op: BinOp::Mul,
                    decimal_overflow_policy: OutputNull,
                }
            };
            let id = arena.intern(
                original.clone(),
                novarocks_type_contract::FunctionValueType::new(
                    if cast {
                        DataType::Decimal128(9, 0)
                    } else {
                        DataType::Decimal128(38, 0)
                    },
                    true,
                ),
            );
            let guarded =
                crate::compiler::constant_evaluator_for_legacy_mode(Some(&NULLABLE), true).unwrap();
            assert!(try_fold_test_node(&mut arena, id, guarded).is_none());
            assert_eq!(
                arena
                    .intern_observed(
                        original,
                        arena.value_type(id).clone(),
                        crate::optimizer::test_optimizer_control()
                    )
                    .unwrap(),
                id
            );
            let default =
                crate::compiler::constant_evaluator_for_legacy_mode(Some(&NULLABLE), false)
                    .unwrap();
            let folded = try_fold_test_node(&mut arena, id, default).unwrap();
            let ScalarNode::Constant(value) = arena.node(folded) else {
                panic!("checked NULL expected")
            };
            assert!(
                value
                    .is_null_observed(
                        CompilePhase::Validate,
                        crate::optimizer::test_optimizer_control()
                    )
                    .unwrap()
            );
            let nested = arena.intern(
                ScalarNode::Nested(literal),
                novarocks_type_contract::FunctionValueType::new(DataType::Decimal128(38, 0), false),
            );
            let pure = try_fold_test_node(&mut arena, nested, guarded).unwrap();
            assert!(matches!(arena.node(pure), ScalarNode::Literal(_)));
        }
        assert!(crate::compiler::constant_evaluator_for_legacy_mode(None, true).is_none());
    }
}

#[cfg(test)]
mod complete_fold_type_tests {
    use super::*;
    use crate::compiler::LiteralValue;
    use arrow::datatypes::Field;
    use novarocks_type_contract::{FunctionValueType, ValueLogicalType, ValueTypeError};
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    struct Requests(Mutex<Vec<FoldRequest>>);
    impl SqlConstantEvaluator for Requests {
        fn eval_scalar(
            &self,
            request: &FoldRequest,
            _: &dyn novarocks_type_contract::PureCompileControl,
        ) -> Result<Option<novarocks_functions::ConstantValue>, SqlConstantEvaluationError>
        {
            self.0.lock().unwrap().push(request.clone());
            Ok(None)
        }
    }

    #[test]
    #[allow(
        deprecated,
        reason = "Dictionary identity is an explicit frozen field fact."
    )]
    fn real_fold_author_preserves_root_domains_and_nested_field_identity() {
        static EVALUATOR: Requests = Requests(Mutex::new(Vec::new()));
        EVALUATOR.0.lock().unwrap().clear();
        let dictionary = Field::new_dict(
            "encoded",
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            true,
            73,
            true,
        )
        .with_metadata([("provider.identity".into(), "source-field-9".into())].into());
        let nested_type =
            FunctionValueType::new(DataType::Struct(vec![Arc::new(dictionary)].into()), true);
        let root_types = [
            FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
                .unwrap(),
            FunctionValueType::try_with_logical_type(
                DataType::LargeBinary,
                true,
                ValueLogicalType::Variant,
            )
            .unwrap(),
            FunctionValueType::try_with_logical_type(
                DataType::FixedSizeBinary(16),
                true,
                ValueLogicalType::Uuid,
            )
            .unwrap(),
            FunctionValueType::new(DataType::Decimal256(76, -7), false),
        ];
        let mut arena = ScalarArena::new();
        let child = arena.intern(
            ScalarNode::Literal(HashableLiteral(LiteralValue::Null)),
            nested_type.clone(),
        );
        for result_type in &root_types {
            let root = arena.intern(
                ScalarNode::Cast {
                    child,
                    target: result_type.data_type().clone(),
                    decimal_overflow_policy:
                        novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
                },
                result_type.clone(),
            );
            assert!(try_fold_test_node(&mut arena, root, &EVALUATOR).is_none());
        }
        let requests = EVALUATOR.0.lock().unwrap();
        assert_eq!(requests.len(), root_types.len());
        for (request, expected) in requests.iter().zip(&root_types) {
            assert_eq!(&request.result_type, expected);
            assert_eq!(request.args.len(), 1);
            assert!(
                request.args[0]
                    .value
                    .is_null_observed(
                        CompilePhase::Validate,
                        crate::optimizer::test_optimizer_control()
                    )
                    .unwrap()
            );
            assert_eq!(request.args[0].value_type, nested_type);
            let DataType::Struct(fields) = &request.args[0].value_type.data_type() else {
                panic!("the authored structure must survive the real request");
            };
            assert_eq!(fields[0].name(), "encoded");
            assert!(fields[0].is_nullable());
            assert_eq!(fields[0].data_type(), &DataType::Utf8);
            assert_eq!(fields[0].dict_id(), None);
            assert_eq!(fields[0].dict_is_ordered(), None);
            assert_eq!(fields[0].metadata().capacity(), 0);
        }
    }

    struct PreparationFailure(Mutex<Option<novarocks_functions::KernelFailure>>);
    impl SqlConstantEvaluator for PreparationFailure {
        fn eval_scalar(
            &self,
            _: &FoldRequest,
            _: &dyn novarocks_type_contract::PureCompileControl,
        ) -> Result<Option<novarocks_functions::ConstantValue>, SqlConstantEvaluationError>
        {
            Err(SqlConstantEvaluationError::Preparation(
                self.0.lock().unwrap().as_ref().unwrap().clone(),
            ))
        }
    }

    #[test]
    fn preparation_failures_never_become_optional_fold_misses() {
        use novarocks_functions::{KernelDiagnostic, KernelFailure};
        use novarocks_type_contract::CompileControlError;
        static EVALUATOR: PreparationFailure = PreparationFailure(Mutex::new(None));
        let mut arena = ScalarArena::new();
        let child = arena.intern(
            ScalarNode::Literal(HashableLiteral(LiteralValue::Int(1))),
            FunctionValueType::new(DataType::Int64, false),
        );
        let root = arena.intern(
            ScalarNode::Cast {
                child,
                target: DataType::Int32,
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            },
            FunctionValueType::new(DataType::Int32, true),
        );
        for (failure, expected) in [
            (KernelFailure::Cancelled, SqlCompileError::Cancelled),
            (
                KernelFailure::DeadlineExceeded,
                SqlCompileError::DeadlineExceeded,
            ),
            (
                KernelFailure::ResourceExhausted,
                SqlCompileError::ResourceExhausted,
            ),
            (
                KernelFailure::InvalidProgram(KernelDiagnostic::new("frozen binding mismatch")),
                SqlCompileError::InvalidRequest(
                    "invalid kernel program: frozen binding mismatch".into(),
                ),
            ),
            (
                KernelFailure::Internal(KernelDiagnostic::new(
                    "unexpected pure preparation failure",
                )),
                SqlCompileError::Compilation(
                    "kernel internal failure: unexpected pure preparation failure".into(),
                ),
            ),
        ] {
            let converted = SqlConstantEvaluationError::from(failure.clone());
            match failure {
                KernelFailure::Cancelled => assert_eq!(
                    converted,
                    SqlConstantEvaluationError::Control(CompileControlError::Cancelled)
                ),
                KernelFailure::DeadlineExceeded => assert_eq!(
                    converted,
                    SqlConstantEvaluationError::Control(CompileControlError::DeadlineExceeded)
                ),
                KernelFailure::ResourceExhausted => assert_eq!(
                    converted,
                    SqlConstantEvaluationError::Control(CompileControlError::ResourceExhausted)
                ),
                _ => assert_eq!(
                    converted,
                    SqlConstantEvaluationError::Preparation(failure.clone())
                ),
            }
            *EVALUATOR.0.lock().unwrap() = Some(failure);
            let mut work = CompileCheckpoints::try_new(
                crate::optimizer::rewrite::context::unbounded_rewrite_test_control(),
                CompilePhase::Validate,
            )
            .unwrap();
            assert_eq!(
                try_fold_node(&mut arena, root, &EVALUATOR, &mut work),
                Err(expected)
            );
            assert!(matches!(arena.node(root), ScalarNode::Cast { .. }));
        }
    }

    struct InvalidFrozenType;
    impl SqlConstantEvaluator for InvalidFrozenType {
        fn eval_scalar(
            &self,
            _: &FoldRequest,
            _: &dyn novarocks_type_contract::PureCompileControl,
        ) -> Result<Option<novarocks_functions::ConstantValue>, SqlConstantEvaluationError>
        {
            Err(ValueTypeError::InvalidLogicalCarrier(ValueLogicalType::Json).into())
        }
    }

    #[test]
    fn invalid_frozen_type_is_fatal_in_the_real_fold_rule() {
        static EVALUATOR: InvalidFrozenType = InvalidFrozenType;
        let mut arena = ScalarArena::new();
        let child = arena.intern(
            ScalarNode::Literal(HashableLiteral(LiteralValue::Int(1))),
            FunctionValueType::new(DataType::Int64, false),
        );
        let root = arena.intern(
            ScalarNode::Cast {
                child,
                target: DataType::Int32,
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            },
            FunctionValueType::new(DataType::Int32, true),
        );
        let control = crate::optimizer::rewrite::context::unbounded_rewrite_test_control();
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate).unwrap();
        assert_eq!(
            try_fold_node(&mut arena, root, &EVALUATOR, &mut work),
            Err(SqlCompileError::InvalidRequest(
                ValueTypeError::InvalidLogicalCarrier(ValueLogicalType::Json).to_string(),
            )),
        );
        assert!(matches!(arena.node(root), ScalarNode::Cast { .. }));
    }
}

#[cfg(test)]
#[path = "fold_constant/cv_tests.rs"]
mod cv_tests;
