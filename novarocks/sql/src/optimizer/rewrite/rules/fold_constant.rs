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

use std::collections::HashMap;

use arrow::datatypes::DataType;

use crate::compiler::{FoldArg, FoldNodeKind, FoldRequest, SqlConstantEvaluator};
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
/// `novarocks/backend/src/fragment/decode/expression/literal.rs` (`lower_literal_value`
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

// Design: ADR-0090 (docs/adr/ADR-0090-constant-folding-reuses-execution-kernels-through-an-injected-port.md)
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

    fn apply(&self, expr: OptExpr, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let Some(evaluator) = ctx.constant_evaluator() else {
            return Ok(RewriteResult::Unchanged);
        };

        let OptExpr {
            mut op,
            children,
            required_output_columns,
        } = expr;

        let arena = ctx.scalar_arena();
        let changed = {
            let mut arena = arena.borrow_mut();
            let mut folder = ConstantFolder::new(&mut arena, evaluator);
            folder.fold_operator(&mut op)
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
/// reference.
struct ConstantFolder<'a> {
    arena: &'a mut ScalarArena,
    evaluator: &'static dyn SqlConstantEvaluator,
    memo: HashMap<ScalarId, ScalarId>,
}

impl<'a> ConstantFolder<'a> {
    fn new(arena: &'a mut ScalarArena, evaluator: &'static dyn SqlConstantEvaluator) -> Self {
        Self {
            arena,
            evaluator,
            memo: HashMap::new(),
        }
    }

    fn fold_slot(&mut self, slot: &mut ScalarId) -> bool {
        match fold_scalar(self.arena, *slot, self.evaluator, &mut self.memo) {
            Some(folded) => {
                *slot = folded;
                true
            }
            None => false,
        }
    }

    fn fold_optional_slot(&mut self, slot: &mut Option<ScalarId>) -> bool {
        match slot {
            Some(id) => self.fold_slot(id),
            None => false,
        }
    }

    fn fold_slots(&mut self, slots: &mut [ScalarId]) -> bool {
        let mut changed = false;
        for slot in slots {
            changed |= self.fold_slot(slot);
        }
        changed
    }

    fn fold_sort_keys(&mut self, keys: &mut [SortKey]) -> bool {
        let mut changed = false;
        for key in keys {
            changed |= self.fold_slot(&mut key.expr);
        }
        changed
    }

    /// Fold every scalar field carried by one operator.
    ///
    /// The match is exhaustive on purpose: adding a scalar-bearing operator
    /// must be a compile error here, not a silently-skipped field. The covered
    /// field set mirrors `rewrite::required_columns::tag_required_columns`.
    fn fold_operator(&mut self, op: &mut Operator) -> bool {
        match op {
            Operator::LogicalScan(scan) | Operator::PhysicalScan(scan) => {
                self.fold_slots(&mut scan.predicates)
            }
            Operator::LogicalFilter(filter) | Operator::PhysicalFilter(filter) => {
                self.fold_slot(&mut filter.predicate)
            }
            Operator::LogicalProject(project) | Operator::PhysicalProject(project) => {
                let mut changed = false;
                for item in &mut project.items {
                    changed |= self.fold_slot(&mut item.expr);
                }
                changed
            }
            Operator::LogicalAggregate(agg) => {
                let mut changed = self.fold_slots(&mut agg.group_by);
                for aggregate in &mut agg.aggregates {
                    changed |= self.fold_slots(&mut aggregate.args);
                    changed |= self.fold_sort_keys(&mut aggregate.order_by);
                }
                changed
            }
            Operator::PhysicalHashAggregate(agg) => {
                let mut changed = self.fold_slots(&mut agg.group_by);
                for aggregate in &mut agg.aggregates {
                    changed |= self.fold_slots(&mut aggregate.args);
                    changed |= self.fold_sort_keys(&mut aggregate.order_by);
                }
                changed
            }
            Operator::LogicalJoin(join) => self.fold_optional_slot(&mut join.condition),
            Operator::PhysicalHashJoin(join) => {
                let mut changed = false;
                for condition in &mut join.eq_conditions {
                    changed |= self.fold_slot(&mut condition.left);
                    changed |= self.fold_slot(&mut condition.right);
                }
                changed |= self.fold_optional_slot(&mut join.other_condition);
                changed
            }
            Operator::PhysicalNestLoopJoin(join) => self.fold_optional_slot(&mut join.condition),
            Operator::LogicalSort(sort) | Operator::PhysicalSort(sort) => {
                let mut changed = self.fold_sort_keys(&mut sort.items);
                changed |= self.fold_slots(&mut sort.analytic_partition_exprs);
                changed
            }
            Operator::LogicalTopN(topn) | Operator::PhysicalTopN(topn) => {
                self.fold_sort_keys(&mut topn.items)
            }
            Operator::LogicalWindow(window) | Operator::PhysicalWindow(window) => {
                let mut changed = false;
                for spec in &mut window.window_exprs {
                    changed |= self.fold_slots(&mut spec.args);
                    changed |= self.fold_slots(&mut spec.partition_by);
                    changed |= self.fold_sort_keys(&mut spec.order_by);
                }
                changed
            }
            Operator::LogicalValues(values) | Operator::PhysicalValues(values) => {
                let mut changed = false;
                for row in &mut values.rows {
                    changed |= self.fold_slots(row);
                }
                changed
            }
            Operator::LogicalTableFunction(func) | Operator::PhysicalTableFunction(func) => {
                self.fold_slots(&mut func.args)
            }
            Operator::LogicalChangeEventExpand(expand)
            | Operator::PhysicalChangeEventExpand(expand) => {
                let mut changed = false;
                for event in &mut expand.events {
                    changed |= self.fold_optional_slot(&mut event.predicate);
                    for assignment in &mut event.assignments {
                        changed |= self.fold_optional_slot(&mut assignment.expr);
                    }
                }
                changed
            }
            Operator::LogicalApply(apply) => {
                let mut changed = self.fold_slot(&mut apply.subquery_expr);
                changed |= self.fold_slots(&mut apply.correlation_conjuncts);
                changed |= self.fold_optional_slot(&mut apply.residual_predicate);
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
        }
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
    evaluator: &'static dyn SqlConstantEvaluator,
    memo: &mut HashMap<ScalarId, ScalarId>,
) -> Option<ScalarId> {
    let folded = fold_scalar_id(arena, id, evaluator, memo);
    (folded != id).then_some(folded)
}

fn fold_scalar_id(
    arena: &mut ScalarArena,
    id: ScalarId,
    evaluator: &'static dyn SqlConstantEvaluator,
    memo: &mut HashMap<ScalarId, ScalarId>,
) -> ScalarId {
    if let Some(&cached) = memo.get(&id) {
        return cached;
    }
    let folded = fold_scalar_uncached(arena, id, evaluator, memo);
    memo.insert(id, folded);
    folded
}

fn fold_scalar_uncached(
    arena: &mut ScalarArena,
    id: ScalarId,
    evaluator: &'static dyn SqlConstantEvaluator,
    memo: &mut HashMap<ScalarId, ScalarId>,
) -> ScalarId {
    let data_type = arena.data_type(id).clone();
    let nullable = arena.nullable(id);
    let mut node = arena.node(id).clone();

    // Post-order: children first, so a node only ever sees already-folded
    // children and "all children are literals" is decidable locally.
    match &mut node {
        ScalarNode::ColumnRef(_) | ScalarNode::LambdaParamRef { .. } | ScalarNode::Literal(_) => {}
        ScalarNode::BinaryOp { left, right, .. } => {
            *left = fold_scalar_id(arena, *left, evaluator, memo);
            *right = fold_scalar_id(arena, *right, evaluator, memo);
        }
        ScalarNode::UnaryOp { child, .. }
        | ScalarNode::Cast { child, .. }
        | ScalarNode::IsNull { child, .. }
        | ScalarNode::IsTruthValue { child, .. } => {
            *child = fold_scalar_id(arena, *child, evaluator, memo);
        }
        ScalarNode::Nested(child) => {
            *child = fold_scalar_id(arena, *child, evaluator, memo);
        }
        ScalarNode::FunctionCall { args, .. } => {
            for arg in args.iter_mut() {
                *arg = fold_scalar_id(arena, *arg, evaluator, memo);
            }
        }
        ScalarNode::LambdaFunction { body, .. } | ScalarNode::Lambda { body, .. } => {
            *body = fold_scalar_id(arena, *body, evaluator, memo);
        }
        ScalarNode::AggregateCall { args, order_by, .. } => {
            for arg in args.iter_mut() {
                *arg = fold_scalar_id(arena, *arg, evaluator, memo);
            }
            for key in order_by.iter_mut() {
                key.expr = fold_scalar_id(arena, key.expr, evaluator, memo);
            }
        }
        ScalarNode::WindowCall {
            args,
            partition_by,
            order_by,
            ..
        } => {
            for arg in args.iter_mut() {
                *arg = fold_scalar_id(arena, *arg, evaluator, memo);
            }
            for expr in partition_by.iter_mut() {
                *expr = fold_scalar_id(arena, *expr, evaluator, memo);
            }
            for key in order_by.iter_mut() {
                key.expr = fold_scalar_id(arena, key.expr, evaluator, memo);
            }
        }
        ScalarNode::InList { child, list, .. } => {
            *child = fold_scalar_id(arena, *child, evaluator, memo);
            for item in list.iter_mut() {
                *item = fold_scalar_id(arena, *item, evaluator, memo);
            }
        }
        ScalarNode::Between {
            child, low, high, ..
        } => {
            *child = fold_scalar_id(arena, *child, evaluator, memo);
            *low = fold_scalar_id(arena, *low, evaluator, memo);
            *high = fold_scalar_id(arena, *high, evaluator, memo);
        }
        ScalarNode::Like { child, pattern, .. } => {
            *child = fold_scalar_id(arena, *child, evaluator, memo);
            *pattern = fold_scalar_id(arena, *pattern, evaluator, memo);
        }
        ScalarNode::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(operand) = operand {
                *operand = fold_scalar_id(arena, *operand, evaluator, memo);
            }
            for (when, then) in when_then.iter_mut() {
                *when = fold_scalar_id(arena, *when, evaluator, memo);
                *then = fold_scalar_id(arena, *then, evaluator, memo);
            }
            if let Some(else_expr) = else_expr {
                *else_expr = fold_scalar_id(arena, *else_expr, evaluator, memo);
            }
        }
    }

    // Re-intern with the rebuilt children, keeping this node's own type
    // metadata. `intern` may canonicalize commutative operand order, so the
    // fold step below reads the children back out of the arena.
    let rebuilt = arena.intern(node, data_type, nullable);
    try_fold_node(arena, rebuilt, evaluator).unwrap_or(rebuilt)
}

/// Try to replace one node (whose children are already folded) with a literal.
///
/// Returns `None` — leaving the node alone — whenever any safety gate fails or
/// the evaluator declines or errors.
fn try_fold_node(
    arena: &mut ScalarArena,
    id: ScalarId,
    evaluator: &'static dyn SqlConstantEvaluator,
) -> Option<ScalarId> {
    let node = arena.node(id).clone();
    let out_type = arena.data_type(id).clone();
    let out_nullable = arena.nullable(id);

    // `Nested` is a pure syntactic wrapper: when its inner expression is a
    // literal the wrapper collapses onto that literal, no evaluation needed.
    if let ScalarNode::Nested(inner) = &node {
        let ScalarNode::Literal(literal) = arena.node(*inner).clone() else {
            return None;
        };
        if !is_wire_encodable_literal_type(&out_type) {
            return None;
        }
        return Some(arena.intern(ScalarNode::Literal(literal), out_type, out_nullable));
    }

    // Gate 2: a volatile or DISTINCT function is never a constant.
    // Gate 3: an environment-sensitive function must stay on the backend.
    let kind = match &node {
        ScalarNode::BinaryOp { op, .. } => FoldNodeKind::BinaryOp(*op),
        ScalarNode::UnaryOp { op, .. } => FoldNodeKind::UnaryOp(*op),
        ScalarNode::Cast { .. } => FoldNodeKind::Cast,
        ScalarNode::FunctionCall {
            name,
            distinct,
            volatility,
            ..
        } => {
            if *distinct || *volatility != FunctionVolatility::Immutable {
                return None;
            }
            if is_environment_sensitive_function(name) || is_byte_carrying_string_function(name) {
                return None;
            }
            FoldNodeKind::Function { name: name.clone() }
        }
        // Every other node shape stays unfolded in v1; its children were
        // already folded above.
        _ => return None,
    };

    // Gate 4: the folded literal has to survive the FE -> BE plan encoding.
    if !is_wire_encodable_literal_type(&out_type) {
        return None;
    }

    let children: Vec<ScalarId> = match &node {
        ScalarNode::BinaryOp { left, right, .. } => vec![*left, *right],
        ScalarNode::UnaryOp { child, .. } | ScalarNode::Cast { child, .. } => vec![*child],
        ScalarNode::FunctionCall { args, .. } => args.clone(),
        _ => return None,
    };

    // Gate 1: every child must already be a literal.
    let mut args = Vec::with_capacity(children.len());
    for child in children {
        let ScalarNode::Literal(HashableLiteral(value)) = arena.node(child) else {
            return None;
        };
        args.push(FoldArg {
            value: value.clone(),
            data_type: arena.data_type(child).clone(),
            nullable: arena.nullable(child),
        });
    }

    let request = FoldRequest {
        kind,
        args,
        out_type: out_type.clone(),
        out_nullable,
    };

    match evaluator.eval_scalar(&request) {
        Ok(Some(value)) => Some(arena.intern(
            ScalarNode::Literal(HashableLiteral(value)),
            out_type,
            out_nullable,
        )),
        // The evaluator declined this shape.
        Ok(None) => None,
        // Fail-open: keep the original expression and swallow the error. The
        // runtime is still allowed to produce a value — or its own error — for
        // this expression, so a failed fold must never become a planning error.
        Err(_) => None,
    }
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
        fn eval_scalar(&self, request: &FoldRequest) -> Result<Option<LiteralValue>, String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            match self.mode {
                FakeMode::Decline => return Ok(None),
                FakeMode::Fail => return Err("fake evaluator failure".to_string()),
                FakeMode::Fold => {}
            }
            match &request.kind {
                FoldNodeKind::BinaryOp(op @ (BinOp::Add | BinOp::Mul)) => {
                    let mut values = Vec::new();
                    for arg in &request.args {
                        let LiteralValue::Int(value) = arg.value else {
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
                    Ok(Some(LiteralValue::Int(folded)))
                }
                FoldNodeKind::Function { .. } => Ok(Some(LiteralValue::Int(FUNCTION_MARKER))),
                _ => Ok(None),
            }
        }
    }

    // -- fixture helpers ---------------------------------------------------

    struct Fixture {
        arena: Rc<RefCell<ScalarArena>>,
        ctx: RewriteContext,
        evaluator: Option<&'static FakeEvaluator>,
    }

    impl Fixture {
        fn with_mode(mode: FakeMode) -> Self {
            let evaluator = FakeEvaluator::new(mode);
            let arena = Rc::new(RefCell::new(ScalarArena::new()));
            let mut ctx = RewriteContext::for_query(Vec::<String>::new());
            ctx.set_scalar_arena(Rc::clone(&arena));
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
            ctx.set_scalar_arena(Rc::clone(&arena));
            Self {
                arena,
                ctx,
                evaluator: None,
            }
        }

        fn intern(&self, node: ScalarNode, data_type: DataType, nullable: bool) -> ScalarId {
            self.arena.borrow_mut().intern(node, data_type, nullable)
        }

        fn int_literal(&self, value: i64) -> ScalarId {
            self.intern(
                ScalarNode::Literal(HashableLiteral(LiteralValue::Int(value))),
                DataType::Int64,
                false,
            )
        }

        fn column(&self, id: u32) -> ScalarId {
            self.intern(
                ScalarNode::ColumnRef(ColumnId::new_for_test(id)),
                DataType::Int64,
                true,
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
                ScalarNode::BinaryOp { op, left, right },
                data_type,
                nullable,
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
                .expect("FoldConstant must never return a planning error")
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
            ScalarNode::Literal(HashableLiteral(LiteralValue::Int(value))) => {
                assert_eq!(value, expected)
            }
            other => panic!("expected Literal(Int({expected})), got {other:?}"),
        }
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
        let ScalarNode::BinaryOp { op, left, right } = fixture.node(folded) else {
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
        let call = fixture.intern(
            ScalarNode::FunctionCall {
                name: "rand".to_string(),
                args: vec![one],
                distinct: false,
                volatility: FunctionVolatility::Volatile,
            },
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
            let call = fixture.intern(
                ScalarNode::FunctionCall {
                    name: name.to_string(),
                    args: vec![ten],
                    distinct: false,
                    volatility: crate::functions::builtin_function_volatility(name),
                },
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
        let call = fixture.intern(
            ScalarNode::FunctionCall {
                name: "some_agg_like_call".to_string(),
                args: vec![one],
                distinct: true,
                volatility: FunctionVolatility::Immutable,
            },
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
        let call = fixture.intern(
            ScalarNode::FunctionCall {
                name: "abs".to_string(),
                args: vec![one],
                distinct: false,
                volatility: FunctionVolatility::Immutable,
            },
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
            let call = fixture.intern(
                ScalarNode::FunctionCall {
                    name: name.to_string(),
                    args: vec![one],
                    distinct: false,
                    // Deliberately Immutable: the denylist, not volatility, is
                    // what must stop this fold.
                    volatility: FunctionVolatility::Immutable,
                },
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
            let call = fixture.intern(
                ScalarNode::FunctionCall {
                    name: name.to_string(),
                    args: vec![one],
                    distinct: false,
                    volatility: FunctionVolatility::Immutable,
                },
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
    fn folds_through_nested_wrapper() {
        let mut fixture = Fixture::with_mode(FakeMode::Fold);
        let one = fixture.int_literal(1);
        let sum = fixture.binary(BinOp::Add, one, one);
        let nested = fixture.intern(ScalarNode::Nested(sum), DataType::Int64, false);
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
            DataType::Int64,
            true,
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
