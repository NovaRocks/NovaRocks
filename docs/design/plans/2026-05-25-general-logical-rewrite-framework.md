# General Logical Rewrite Framework Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a generic logical rewrite framework that can be used by both normal query optimization and MV refresh rewrite without changing current query or MV semantics.

**Architecture:** Add `src/sql/optimizer/rewrite/` as a standalone `LogicalPlan -> LogicalPlan` rewrite substrate with context, phases, rule results, trace, tree traversal, pipeline, and registry. Keep the existing RBO driver unchanged in phase 1, and insert an empty query rewrite pipeline before the current RBO passes to prove query-side integration is safe.

**Tech Stack:** Rust, existing `LogicalPlan` tree in `src/sql/planner/plan.rs`, existing optimizer settings in `src/sql/optimizer/options.rs`, existing cargo test workflow.

---

## File Structure

- Create `src/sql/optimizer/rewrite/mod.rs`: module exports.
- Create `src/sql/optimizer/rewrite/phase.rs`: stable phase enum and names.
- Create `src/sql/optimizer/rewrite/result.rs`: rule result and diagnostics.
- Create `src/sql/optimizer/rewrite/trace.rs`: trace events and collector.
- Create `src/sql/optimizer/rewrite/context.rs`: per-call context, policy, disabled rules, consumer metadata.
- Create `src/sql/optimizer/rewrite/rule.rs`: generic logical rewrite rule trait.
- Create `src/sql/optimizer/rewrite/tree.rs`: reusable top-down and bottom-up `LogicalPlan` traversal.
- Create `src/sql/optimizer/rewrite/pipeline.rs`: phase-ordered fixed-point driver.
- Create `src/sql/optimizer/rewrite/registry.rs`: query and MV pipeline factories plus rewrite rule-name lookup.
- Modify `src/sql/optimizer/mod.rs`: expose `rewrite`, run empty query pipeline before current RBO, and include rewrite rule names in `is_known_rule_name`.

## Task 1: Core Rewrite Types

**Files:**
- Create: `src/sql/optimizer/rewrite/mod.rs`
- Create: `src/sql/optimizer/rewrite/phase.rs`
- Create: `src/sql/optimizer/rewrite/result.rs`
- Create: `src/sql/optimizer/rewrite/trace.rs`
- Create: `src/sql/optimizer/rewrite/context.rs`
- Modify: `src/sql/optimizer/mod.rs`

- [ ] **Step 1: Add module declaration and failing phase test**

In `src/sql/optimizer/mod.rs`, add the module next to the other optimizer modules:

```rust
pub(crate) mod rewrite;
```

Create `src/sql/optimizer/rewrite/mod.rs`:

```rust
pub(crate) mod context;
pub(crate) mod phase;
pub(crate) mod pipeline;
pub(crate) mod registry;
pub(crate) mod result;
pub(crate) mod rule;
pub(crate) mod trace;
pub(crate) mod tree;
```

Create `src/sql/optimizer/rewrite/phase.rs` with only this failing test:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn phase_names_are_stable() {
        assert_eq!(RewritePhase::LogicalNormalize.as_str(), "LogicalNormalize");
        assert_eq!(RewritePhase::StructuralRewrite.as_str(), "StructuralRewrite");
        assert_eq!(RewritePhase::SemanticRewrite.as_str(), "SemanticRewrite");
        assert_eq!(RewritePhase::Validation.as_str(), "Validation");
    }
}
```

- [ ] **Step 2: Run the failing phase test**

Run:

```bash
cargo test --lib sql::optimizer::rewrite::phase::tests::phase_names_are_stable -- --exact
```

Expected: compile failure containing `use of undeclared type RewritePhase`.

- [ ] **Step 3: Implement `RewritePhase`**

Replace `src/sql/optimizer/rewrite/phase.rs` with:

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum RewritePhase {
    LogicalNormalize,
    StructuralRewrite,
    SemanticRewrite,
    Validation,
}

impl RewritePhase {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::LogicalNormalize => "LogicalNormalize",
            Self::StructuralRewrite => "StructuralRewrite",
            Self::SemanticRewrite => "SemanticRewrite",
            Self::Validation => "Validation",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn phase_names_are_stable() {
        assert_eq!(RewritePhase::LogicalNormalize.as_str(), "LogicalNormalize");
        assert_eq!(RewritePhase::StructuralRewrite.as_str(), "StructuralRewrite");
        assert_eq!(RewritePhase::SemanticRewrite.as_str(), "SemanticRewrite");
        assert_eq!(RewritePhase::Validation.as_str(), "Validation");
    }
}
```

- [ ] **Step 4: Add failing diagnostics and trace tests**

Create `src/sql/optimizer/rewrite/result.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejected_diagnostic_preserves_rule_and_message() {
        let diagnostic = RewriteDiagnostic::rejected("RuleA", "unsupported join shape");
        assert_eq!(diagnostic.rule, "RuleA");
        assert_eq!(diagnostic.message, "unsupported join shape");
        assert_eq!(diagnostic.kind, RewriteDiagnosticKind::Rejected);
    }
}
```

Create `src/sql/optimizer/rewrite/trace.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::optimizer::rewrite::phase::RewritePhase;

    #[test]
    fn trace_records_phase_and_rule_events() {
        let mut trace = RewriteTrace::default();
        trace.phase_started(RewritePhase::LogicalNormalize);
        trace.rule_skipped(RewritePhase::LogicalNormalize, "RuleA", "disabled");
        trace.phase_ended(RewritePhase::LogicalNormalize);

        assert_eq!(trace.events().len(), 3);
        assert!(matches!(
            trace.events()[0],
            RewriteTraceEvent::PhaseStarted {
                phase: RewritePhase::LogicalNormalize
            }
        ));
    }
}
```

- [ ] **Step 5: Run the failing diagnostics and trace tests**

Run:

```bash
cargo test --lib sql::optimizer::rewrite::result::tests::rejected_diagnostic_preserves_rule_and_message -- --exact
cargo test --lib sql::optimizer::rewrite::trace::tests::trace_records_phase_and_rule_events -- --exact
```

Expected: both commands fail to compile with missing `RewriteDiagnostic`, `RewriteTrace`, or `RewriteTraceEvent`.

- [ ] **Step 6: Implement diagnostics and trace**

Replace `src/sql/optimizer/rewrite/result.rs` with:

```rust
use crate::sql::planner::plan::LogicalPlan;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RewriteDiagnosticKind {
    Rejected,
    Error,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RewriteDiagnostic {
    pub(crate) rule: &'static str,
    pub(crate) message: String,
    pub(crate) kind: RewriteDiagnosticKind,
}

impl RewriteDiagnostic {
    pub(crate) fn rejected(rule: &'static str, message: impl Into<String>) -> Self {
        Self {
            rule,
            message: message.into(),
            kind: RewriteDiagnosticKind::Rejected,
        }
    }

    pub(crate) fn error(rule: &'static str, message: impl Into<String>) -> Self {
        Self {
            rule,
            message: message.into(),
            kind: RewriteDiagnosticKind::Error,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) enum RewriteResult {
    Unchanged,
    Changed(LogicalPlan),
    Rejected(RewriteDiagnostic),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejected_diagnostic_preserves_rule_and_message() {
        let diagnostic = RewriteDiagnostic::rejected("RuleA", "unsupported join shape");
        assert_eq!(diagnostic.rule, "RuleA");
        assert_eq!(diagnostic.message, "unsupported join shape");
        assert_eq!(diagnostic.kind, RewriteDiagnosticKind::Rejected);
    }
}
```

Replace `src/sql/optimizer/rewrite/trace.rs` with:

```rust
use crate::sql::optimizer::rewrite::phase::RewritePhase;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RewriteTraceEvent {
    PhaseStarted {
        phase: RewritePhase,
    },
    PhaseEnded {
        phase: RewritePhase,
    },
    IterationStarted {
        phase: RewritePhase,
        iteration: usize,
    },
    RuleSkipped {
        phase: RewritePhase,
        rule: &'static str,
        reason: String,
    },
    RuleMatched {
        phase: RewritePhase,
        rule: &'static str,
    },
    RuleChanged {
        phase: RewritePhase,
        rule: &'static str,
        elapsed_micros: u128,
    },
    RuleRejected {
        phase: RewritePhase,
        rule: &'static str,
        message: String,
    },
    RuleFailed {
        phase: RewritePhase,
        rule: &'static str,
        message: String,
    },
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct RewriteTrace {
    events: Vec<RewriteTraceEvent>,
}

impl RewriteTrace {
    pub(crate) fn events(&self) -> &[RewriteTraceEvent] {
        &self.events
    }

    pub(crate) fn phase_started(&mut self, phase: RewritePhase) {
        self.events.push(RewriteTraceEvent::PhaseStarted { phase });
    }

    pub(crate) fn phase_ended(&mut self, phase: RewritePhase) {
        self.events.push(RewriteTraceEvent::PhaseEnded { phase });
    }

    pub(crate) fn iteration_started(&mut self, phase: RewritePhase, iteration: usize) {
        self.events
            .push(RewriteTraceEvent::IterationStarted { phase, iteration });
    }

    pub(crate) fn rule_skipped(
        &mut self,
        phase: RewritePhase,
        rule: &'static str,
        reason: impl Into<String>,
    ) {
        self.events.push(RewriteTraceEvent::RuleSkipped {
            phase,
            rule,
            reason: reason.into(),
        });
    }

    pub(crate) fn rule_matched(&mut self, phase: RewritePhase, rule: &'static str) {
        self.events
            .push(RewriteTraceEvent::RuleMatched { phase, rule });
    }

    pub(crate) fn rule_changed(
        &mut self,
        phase: RewritePhase,
        rule: &'static str,
        elapsed_micros: u128,
    ) {
        self.events.push(RewriteTraceEvent::RuleChanged {
            phase,
            rule,
            elapsed_micros,
        });
    }

    pub(crate) fn rule_rejected(
        &mut self,
        phase: RewritePhase,
        rule: &'static str,
        message: impl Into<String>,
    ) {
        self.events.push(RewriteTraceEvent::RuleRejected {
            phase,
            rule,
            message: message.into(),
        });
    }

    pub(crate) fn rule_failed(
        &mut self,
        phase: RewritePhase,
        rule: &'static str,
        message: impl Into<String>,
    ) {
        self.events.push(RewriteTraceEvent::RuleFailed {
            phase,
            rule,
            message: message.into(),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::optimizer::rewrite::phase::RewritePhase;

    #[test]
    fn trace_records_phase_and_rule_events() {
        let mut trace = RewriteTrace::default();
        trace.phase_started(RewritePhase::LogicalNormalize);
        trace.rule_skipped(RewritePhase::LogicalNormalize, "RuleA", "disabled");
        trace.phase_ended(RewritePhase::LogicalNormalize);

        assert_eq!(trace.events().len(), 3);
        assert!(matches!(
            trace.events()[0],
            RewriteTraceEvent::PhaseStarted {
                phase: RewritePhase::LogicalNormalize
            }
        ));
    }
}
```

- [ ] **Step 7: Add context tests and implementation**

Create `src/sql/optimizer/rewrite/context.rs` with:

```rust
use std::any::Any;
use std::collections::HashSet;
use std::sync::Arc;

use crate::sql::optimizer::rewrite::trace::RewriteTrace;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RewriteConsumer {
    Query,
    MaterializedViewRefresh,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RewriteFailurePolicy {
    CollectDiagnostics,
    FailFast,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RewritePolicy {
    pub(crate) failure_policy: RewriteFailurePolicy,
    pub(crate) max_iterations: usize,
}

impl Default for RewritePolicy {
    fn default() -> Self {
        Self {
            failure_policy: RewriteFailurePolicy::CollectDiagnostics,
            max_iterations: 8,
        }
    }
}

#[derive(Clone)]
pub(crate) struct RewriteContext {
    consumer: RewriteConsumer,
    disabled_rules: HashSet<String>,
    policy: RewritePolicy,
    trace: RewriteTrace,
    extension: Option<Arc<dyn Any + Send + Sync>>,
}

impl RewriteContext {
    pub(crate) fn new(consumer: RewriteConsumer) -> Self {
        Self {
            consumer,
            disabled_rules: HashSet::new(),
            policy: RewritePolicy::default(),
            trace: RewriteTrace::default(),
            extension: None,
        }
    }

    pub(crate) fn for_query(disabled_rules: impl IntoIterator<Item = String>) -> Self {
        let mut ctx = Self::new(RewriteConsumer::Query);
        ctx.disabled_rules = disabled_rules.into_iter().collect();
        ctx
    }

    pub(crate) fn for_mv_refresh(disabled_rules: impl IntoIterator<Item = String>) -> Self {
        let mut ctx = Self::new(RewriteConsumer::MaterializedViewRefresh);
        ctx.disabled_rules = disabled_rules.into_iter().collect();
        ctx.policy.failure_policy = RewriteFailurePolicy::FailFast;
        ctx
    }

    pub(crate) fn consumer(&self) -> RewriteConsumer {
        self.consumer
    }

    pub(crate) fn policy(&self) -> &RewritePolicy {
        &self.policy
    }

    pub(crate) fn policy_mut(&mut self) -> &mut RewritePolicy {
        &mut self.policy
    }

    pub(crate) fn is_rule_enabled(&self, rule_name: &str) -> bool {
        !self.disabled_rules.contains(rule_name)
    }

    pub(crate) fn trace(&self) -> &RewriteTrace {
        &self.trace
    }

    pub(crate) fn trace_mut(&mut self) -> &mut RewriteTrace {
        &mut self.trace
    }

    pub(crate) fn set_extension<T>(&mut self, extension: T)
    where
        T: Any + Send + Sync,
    {
        self.extension = Some(Arc::new(extension));
    }

    pub(crate) fn extension<T>(&self) -> Option<&T>
    where
        T: Any + Send + Sync,
    {
        self.extension.as_ref()?.downcast_ref::<T>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq, Eq)]
    struct TestExtension {
        value: i32,
    }

    #[test]
    fn query_context_uses_disabled_rules() {
        let ctx = RewriteContext::for_query(vec!["RuleA".to_string()]);
        assert_eq!(ctx.consumer(), RewriteConsumer::Query);
        assert!(!ctx.is_rule_enabled("RuleA"));
        assert!(ctx.is_rule_enabled("RuleB"));
    }

    #[test]
    fn mv_context_defaults_to_fail_fast() {
        let ctx = RewriteContext::for_mv_refresh(Vec::<String>::new());
        assert_eq!(ctx.consumer(), RewriteConsumer::MaterializedViewRefresh);
        assert_eq!(
            ctx.policy().failure_policy,
            RewriteFailurePolicy::FailFast
        );
    }

    #[test]
    fn context_extension_round_trips() {
        let mut ctx = RewriteContext::for_mv_refresh(Vec::<String>::new());
        ctx.set_extension(TestExtension { value: 7 });
        assert_eq!(ctx.extension::<TestExtension>(), Some(&TestExtension { value: 7 }));
        assert!(ctx.extension::<String>().is_none());
    }
}
```

- [ ] **Step 8: Run core type tests**

Run:

```bash
cargo test --lib sql::optimizer::rewrite -- --nocapture
```

Expected: all tests under `sql::optimizer::rewrite::{phase,result,trace,context}` pass.

- [ ] **Step 9: Commit core rewrite types**

Run:

```bash
git add src/sql/optimizer/mod.rs src/sql/optimizer/rewrite
git commit -m "feat: add logical rewrite core types"
```

## Task 2: Rule Trait and Tree Traversal

**Files:**
- Create: `src/sql/optimizer/rewrite/rule.rs`
- Create: `src/sql/optimizer/rewrite/tree.rs`

- [ ] **Step 1: Add rule trait**

Create `src/sql/optimizer/rewrite/rule.rs`:

```rust
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::planner::plan::LogicalPlan;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RewriteTraversal {
    TopDown,
    BottomUp,
}

pub(crate) trait LogicalRewriteRule: Send + Sync {
    fn name(&self) -> &'static str;

    fn phase(&self) -> RewritePhase;

    fn traversal(&self) -> RewriteTraversal {
        RewriteTraversal::BottomUp
    }

    fn matches(&self, plan: &LogicalPlan, ctx: &RewriteContext) -> bool;

    fn apply(
        &self,
        plan: LogicalPlan,
        ctx: &mut RewriteContext,
    ) -> Result<RewriteResult, String>;
}
```

- [ ] **Step 2: Add failing tree traversal tests**

Create `src/sql/optimizer/rewrite/tree.rs` with this test module first:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{ExprKind, OutputColumn, TypedExpr};
    use crate::sql::catalog::{TableDef, TableStorage};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::optimizer::rewrite::phase::RewritePhase;
    use crate::sql::optimizer::rewrite::result::RewriteResult;
    use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
    use crate::sql::planner::plan::*;
    use arrow::datatypes::DataType;

    struct RenameScanRule;

    impl LogicalRewriteRule for RenameScanRule {
        fn name(&self) -> &'static str {
            "RenameScanRule"
        }

        fn phase(&self) -> RewritePhase {
            RewritePhase::StructuralRewrite
        }

        fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
            matches!(plan, LogicalPlan::Scan(_))
        }

        fn apply(
            &self,
            plan: LogicalPlan,
            _ctx: &mut RewriteContext,
        ) -> Result<RewriteResult, String> {
            let LogicalPlan::Scan(mut scan) = plan else {
                return Ok(RewriteResult::Unchanged);
            };
            scan.table.name = "renamed".to_string();
            Ok(RewriteResult::Changed(LogicalPlan::Scan(scan)))
        }
    }

    struct RejectProjectRule;

    impl LogicalRewriteRule for RejectProjectRule {
        fn name(&self) -> &'static str {
            "RejectProjectRule"
        }

        fn phase(&self) -> RewritePhase {
            RewritePhase::Validation
        }

        fn traversal(&self) -> RewriteTraversal {
            RewriteTraversal::TopDown
        }

        fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
            matches!(plan, LogicalPlan::Project(_))
        }

        fn apply(
            &self,
            _plan: LogicalPlan,
            _ctx: &mut RewriteContext,
        ) -> Result<RewriteResult, String> {
            Ok(RewriteResult::Rejected(
                crate::sql::optimizer::rewrite::result::RewriteDiagnostic::rejected(
                    self.name(),
                    "project is rejected by test rule",
                ),
            ))
        }
    }

    fn output_col(name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId::UNSET,
            name: name.to_string(),
            data_type: DataType::Int32,
            nullable: false,
        }
    }

    fn scan(name: &str) -> LogicalPlan {
        LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table: TableDef {
                name: name.to_string(),
                columns: vec![],
                iceberg_row_lineage_metadata_columns: vec![],
                iceberg_table: None,
                storage: TableStorage::LocalParquetFile {
                    path: std::path::PathBuf::from("/tmp/test.parquet"),
                },
            },
            alias: None,
            columns: vec![output_col("c1")],
            predicates: vec![],
            required_columns: None,
        })
    }

    fn project_over_scan() -> LogicalPlan {
        LogicalPlan::Project(ProjectNode {
            input: Box::new(scan("original")),
            items: vec![crate::sql::analysis::ProjectItem {
                expr: TypedExpr {
                    kind: ExprKind::ColumnRef {
                        column_id: ColumnId::UNSET,
                        qualifier: None,
                        column: "c1".to_string(),
                    },
                    data_type: DataType::Int32,
                    nullable: false,
                },
                output_name: "c1".to_string(),
            }],
        })
    }

    #[test]
    fn bottom_up_rewrite_rebuilds_project_child() {
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        let before = project_over_scan();
        let (after, changed) =
            rewrite_with_rule(before, &RenameScanRule, &mut ctx).expect("rewrite");

        assert!(changed);
        let LogicalPlan::Project(project) = after else {
            panic!("expected project");
        };
        let LogicalPlan::Scan(scan) = project.input.as_ref() else {
            panic!("expected scan child");
        };
        assert_eq!(scan.table.name, "renamed");
    }

    #[test]
    fn rejected_rule_collects_diagnostic_without_changing_plan() {
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        let before = project_over_scan();
        let before_debug = format!("{before:?}");
        let (after, changed) =
            rewrite_with_rule(before, &RejectProjectRule, &mut ctx).expect("rewrite");

        assert!(!changed);
        assert_eq!(format!("{after:?}"), before_debug);
        assert!(ctx.trace().events().iter().any(|event| matches!(
            event,
            crate::sql::optimizer::rewrite::trace::RewriteTraceEvent::RuleRejected {
                rule: "RejectProjectRule",
                ..
            }
        )));
    }
}
```

- [ ] **Step 3: Run the failing tree tests**

Run:

```bash
cargo test --lib sql::optimizer::rewrite::tree::tests::bottom_up_rewrite_rebuilds_project_child -- --exact
cargo test --lib sql::optimizer::rewrite::tree::tests::rejected_rule_collects_diagnostic_without_changing_plan -- --exact
```

Expected: both commands fail to compile with missing `rewrite_with_rule`.

- [ ] **Step 4: Implement tree traversal**

Add this implementation above the tests in `src/sql/optimizer/rewrite/tree.rs`:

```rust
use std::time::Instant;

use crate::sql::optimizer::rewrite::context::{RewriteContext, RewriteFailurePolicy};
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
use crate::sql::planner::plan::*;

pub(crate) fn rewrite_with_rule(
    plan: LogicalPlan,
    rule: &dyn LogicalRewriteRule,
    ctx: &mut RewriteContext,
) -> Result<(LogicalPlan, bool), String> {
    match rule.traversal() {
        RewriteTraversal::TopDown => rewrite_top_down(plan, rule, ctx),
        RewriteTraversal::BottomUp => rewrite_bottom_up(plan, rule, ctx),
    }
}

fn rewrite_top_down(
    plan: LogicalPlan,
    rule: &dyn LogicalRewriteRule,
    ctx: &mut RewriteContext,
) -> Result<(LogicalPlan, bool), String> {
    let (plan, node_changed) = apply_rule_to_node(plan, rule, ctx)?;
    let (plan, child_changed) = rewrite_children(plan, rule, ctx)?;
    Ok((plan, node_changed || child_changed))
}

fn rewrite_bottom_up(
    plan: LogicalPlan,
    rule: &dyn LogicalRewriteRule,
    ctx: &mut RewriteContext,
) -> Result<(LogicalPlan, bool), String> {
    let (plan, child_changed) = rewrite_children(plan, rule, ctx)?;
    let (plan, node_changed) = apply_rule_to_node(plan, rule, ctx)?;
    Ok((plan, child_changed || node_changed))
}

fn apply_rule_to_node(
    plan: LogicalPlan,
    rule: &dyn LogicalRewriteRule,
    ctx: &mut RewriteContext,
) -> Result<(LogicalPlan, bool), String> {
    if !rule.matches(&plan, ctx) {
        return Ok((plan, false));
    }

    let phase = rule.phase();
    let name = rule.name();
    ctx.trace_mut().rule_matched(phase, name);
    let start = Instant::now();
    match rule.apply(plan.clone(), ctx) {
        Ok(RewriteResult::Unchanged) => Ok((plan, false)),
        Ok(RewriteResult::Changed(next)) => {
            ctx.trace_mut()
                .rule_changed(phase, name, start.elapsed().as_micros());
            Ok((next, true))
        }
        Ok(RewriteResult::Rejected(diagnostic)) => {
            ctx.trace_mut()
                .rule_rejected(phase, name, diagnostic.message.clone());
            match ctx.policy().failure_policy {
                RewriteFailurePolicy::CollectDiagnostics => Ok((plan, false)),
                RewriteFailurePolicy::FailFast => Err(diagnostic.message),
            }
        }
        Err(err) => {
            ctx.trace_mut().rule_failed(phase, name, err.clone());
            Err(err)
        }
    }
}

fn rewrite_children(
    plan: LogicalPlan,
    rule: &dyn LogicalRewriteRule,
    ctx: &mut RewriteContext,
) -> Result<(LogicalPlan, bool), String> {
    macro_rules! rec_box {
        ($child:expr) => {{
            let (next, changed) = rewrite_with_rule(*$child, rule, ctx)?;
            (Box::new(next), changed)
        }};
    }

    match plan {
        LogicalPlan::Scan(_)
        | LogicalPlan::Values(_)
        | LogicalPlan::GenerateSeries(_)
        | LogicalPlan::CTEConsume(_) => Ok((plan, false)),

        LogicalPlan::Filter(n) => {
            let (input, changed) = rec_box!(n.input);
            Ok((LogicalPlan::Filter(FilterNode { input, predicate: n.predicate }), changed))
        }
        LogicalPlan::Project(n) => {
            let (input, changed) = rec_box!(n.input);
            Ok((LogicalPlan::Project(ProjectNode { input, items: n.items }), changed))
        }
        LogicalPlan::Aggregate(n) => {
            let (input, changed) = rec_box!(n.input);
            Ok((LogicalPlan::Aggregate(AggregateNode { input, ..n }), changed))
        }
        LogicalPlan::Sort(n) => {
            let (input, changed) = rec_box!(n.input);
            Ok((
                LogicalPlan::Sort(SortNode {
                    input,
                    items: n.items,
                    analytic_partition_by: n.analytic_partition_by,
                }),
                changed,
            ))
        }
        LogicalPlan::Limit(n) => {
            let (input, changed) = rec_box!(n.input);
            Ok((LogicalPlan::Limit(LimitNode { input, limit: n.limit, offset: n.offset }), changed))
        }
        LogicalPlan::Window(n) => {
            let (input, changed) = rec_box!(n.input);
            Ok((LogicalPlan::Window(WindowNode { input, ..n }), changed))
        }
        LogicalPlan::TableFunction(n) => {
            let (input, changed) = rec_box!(n.input);
            Ok((LogicalPlan::TableFunction(TableFunctionNode { input, ..n }), changed))
        }
        LogicalPlan::SubqueryAlias(n) => {
            let (input, changed) = rec_box!(n.input);
            Ok((
                LogicalPlan::SubqueryAlias(SubqueryAliasNode {
                    input,
                    alias: n.alias,
                    output_columns: n.output_columns,
                }),
                changed,
            ))
        }
        LogicalPlan::Repeat(n) => {
            let (input, changed) = rec_box!(n.input);
            Ok((LogicalPlan::Repeat(RepeatPlanNode { input, ..n }), changed))
        }
        LogicalPlan::CTEProduce(n) => {
            let (input, changed) = rec_box!(n.input);
            Ok((
                LogicalPlan::CTEProduce(CTEProduceNode {
                    cte_id: n.cte_id,
                    input,
                    output_columns: n.output_columns,
                }),
                changed,
            ))
        }
        LogicalPlan::Join(n) => {
            let (left, left_changed) = rec_box!(n.left);
            let (right, right_changed) = rec_box!(n.right);
            Ok((
                LogicalPlan::Join(JoinNode {
                    left,
                    right,
                    join_type: n.join_type,
                    condition: n.condition,
                }),
                left_changed || right_changed,
            ))
        }
        LogicalPlan::CTEAnchor(n) => {
            let (produce, produce_changed) = rec_box!(n.produce);
            let (consumer, consumer_changed) = rec_box!(n.consumer);
            Ok((
                LogicalPlan::CTEAnchor(CTEAnchorNode {
                    cte_id: n.cte_id,
                    produce,
                    consumer,
                }),
                produce_changed || consumer_changed,
            ))
        }
        LogicalPlan::Union(n) => {
            let (inputs, changed) = rewrite_input_list(n.inputs, rule, ctx)?;
            Ok((LogicalPlan::Union(UnionNode { inputs, all: n.all }), changed))
        }
        LogicalPlan::Intersect(n) => {
            let (inputs, changed) = rewrite_input_list(n.inputs, rule, ctx)?;
            Ok((LogicalPlan::Intersect(IntersectNode { inputs }), changed))
        }
        LogicalPlan::Except(n) => {
            let (inputs, changed) = rewrite_input_list(n.inputs, rule, ctx)?;
            Ok((LogicalPlan::Except(ExceptNode { inputs }), changed))
        }
    }
}

fn rewrite_input_list(
    inputs: Vec<LogicalPlan>,
    rule: &dyn LogicalRewriteRule,
    ctx: &mut RewriteContext,
) -> Result<(Vec<LogicalPlan>, bool), String> {
    let mut changed_any = false;
    let mut out = Vec::with_capacity(inputs.len());
    for input in inputs {
        let (next, changed) = rewrite_with_rule(input, rule, ctx)?;
        changed_any |= changed;
        out.push(next);
    }
    Ok((out, changed_any))
}
```

- [ ] **Step 5: Run tree traversal tests**

Run:

```bash
cargo test --lib sql::optimizer::rewrite::tree::tests::bottom_up_rewrite_rebuilds_project_child -- --exact
cargo test --lib sql::optimizer::rewrite::tree::tests::rejected_rule_collects_diagnostic_without_changing_plan -- --exact
```

Expected: both commands pass.

- [ ] **Step 6: Commit rule trait and tree traversal**

Run:

```bash
git add src/sql/optimizer/rewrite/rule.rs src/sql/optimizer/rewrite/tree.rs
git commit -m "feat: add logical rewrite tree traversal"
```

## Task 3: Pipeline and Registry

**Files:**
- Create: `src/sql/optimizer/rewrite/pipeline.rs`
- Create: `src/sql/optimizer/rewrite/registry.rs`

- [ ] **Step 1: Add failing pipeline tests**

Create `src/sql/optimizer/rewrite/pipeline.rs` with:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::optimizer::rewrite::phase::RewritePhase;
    use crate::sql::optimizer::rewrite::result::RewriteResult;
    use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
    use crate::sql::planner::plan::{LogicalPlan, ValuesNode};

    struct CountingNoopRule;

    impl LogicalRewriteRule for CountingNoopRule {
        fn name(&self) -> &'static str {
            "CountingNoopRule"
        }

        fn phase(&self) -> RewritePhase {
            RewritePhase::LogicalNormalize
        }

        fn matches(&self, _plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
            true
        }

        fn apply(
            &self,
            _plan: LogicalPlan,
            _ctx: &mut RewriteContext,
        ) -> Result<RewriteResult, String> {
            Ok(RewriteResult::Unchanged)
        }
    }

    fn values_plan() -> LogicalPlan {
        LogicalPlan::Values(ValuesNode {
            rows: vec![],
            columns: vec![],
        })
    }

    #[test]
    fn empty_pipeline_preserves_plan_and_records_phases() {
        let pipeline = RewritePipeline::new(
            vec![RewritePhase::LogicalNormalize, RewritePhase::Validation],
            vec![],
        );
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        let before = values_plan();
        let before_debug = format!("{before:?}");
        let after = pipeline.rewrite(before, &mut ctx).expect("rewrite");

        assert_eq!(format!("{after:?}"), before_debug);
        assert!(ctx.trace().events().iter().any(|event| matches!(
            event,
            crate::sql::optimizer::rewrite::trace::RewriteTraceEvent::PhaseStarted {
                phase: RewritePhase::LogicalNormalize
            }
        )));
        assert!(ctx.trace().events().iter().any(|event| matches!(
            event,
            crate::sql::optimizer::rewrite::trace::RewriteTraceEvent::PhaseEnded {
                phase: RewritePhase::Validation
            }
        )));
    }

    #[test]
    fn disabled_rule_is_skipped_before_match() {
        let pipeline = RewritePipeline::new(
            vec![RewritePhase::LogicalNormalize],
            vec![Box::new(CountingNoopRule)],
        );
        let mut ctx = RewriteContext::for_query(vec!["CountingNoopRule".to_string()]);
        let _ = pipeline.rewrite(values_plan(), &mut ctx).expect("rewrite");

        assert!(ctx.trace().events().iter().any(|event| matches!(
            event,
            crate::sql::optimizer::rewrite::trace::RewriteTraceEvent::RuleSkipped {
                rule: "CountingNoopRule",
                ..
            }
        )));
        assert!(!ctx.trace().events().iter().any(|event| matches!(
            event,
            crate::sql::optimizer::rewrite::trace::RewriteTraceEvent::RuleMatched {
                rule: "CountingNoopRule",
                ..
            }
        )));
    }
}
```

- [ ] **Step 2: Run failing pipeline tests**

Run:

```bash
cargo test --lib sql::optimizer::rewrite::pipeline::tests::empty_pipeline_preserves_plan_and_records_phases -- --exact
cargo test --lib sql::optimizer::rewrite::pipeline::tests::disabled_rule_is_skipped_before_match -- --exact
```

Expected: both commands fail to compile with missing `RewritePipeline`.

- [ ] **Step 3: Implement pipeline**

Add this implementation above the tests in `src/sql/optimizer/rewrite/pipeline.rs`:

```rust
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::optimizer::rewrite::tree::rewrite_with_rule;
use crate::sql::planner::plan::LogicalPlan;

pub(crate) struct RewritePipeline {
    phases: Vec<RewritePhase>,
    rules: Vec<Box<dyn LogicalRewriteRule>>,
}

impl RewritePipeline {
    pub(crate) fn new(
        phases: Vec<RewritePhase>,
        rules: Vec<Box<dyn LogicalRewriteRule>>,
    ) -> Self {
        Self { phases, rules }
    }

    pub(crate) fn rule_names(&self) -> Vec<&'static str> {
        self.rules.iter().map(|rule| rule.name()).collect()
    }

    pub(crate) fn rewrite(
        &self,
        plan: LogicalPlan,
        ctx: &mut RewriteContext,
    ) -> Result<LogicalPlan, String> {
        let mut current = plan;
        for phase in &self.phases {
            ctx.trace_mut().phase_started(*phase);
            for iteration in 1..=ctx.policy().max_iterations {
                ctx.trace_mut().iteration_started(*phase, iteration);
                let mut changed_this_iteration = false;
                for rule in self.rules.iter().filter(|rule| rule.phase() == *phase) {
                    if !ctx.is_rule_enabled(rule.name()) {
                        ctx.trace_mut()
                            .rule_skipped(*phase, rule.name(), "disabled");
                        continue;
                    }
                    let before_rule = current.clone();
                    match rewrite_with_rule(current, rule.as_ref(), ctx) {
                        Ok((next, changed)) => {
                            current = next;
                            changed_this_iteration |= changed;
                        }
                        Err(err) => {
                            current = before_rule;
                            ctx.trace_mut().rule_failed(*phase, rule.name(), err.clone());
                            return Err(err);
                        }
                    }
                }
                if !changed_this_iteration {
                    break;
                }
            }
            ctx.trace_mut().phase_ended(*phase);
        }
        Ok(current)
    }
}
```

- [ ] **Step 4: Add registry tests**

Create `src/sql/optimizer/rewrite/registry.rs`:

```rust
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::pipeline::RewritePipeline;

pub(crate) fn default_rewrite_phases() -> Vec<RewritePhase> {
    vec![
        RewritePhase::LogicalNormalize,
        RewritePhase::StructuralRewrite,
        RewritePhase::SemanticRewrite,
        RewritePhase::Validation,
    ]
}

pub(crate) fn query_rewrite_pipeline() -> RewritePipeline {
    RewritePipeline::new(default_rewrite_phases(), vec![])
}

pub(crate) fn mv_rewrite_pipeline() -> RewritePipeline {
    RewritePipeline::new(default_rewrite_phases(), vec![])
}

pub(crate) fn is_known_rewrite_rule_name(name: &str) -> bool {
    query_rewrite_pipeline().rule_names().contains(&name)
        || mv_rewrite_pipeline().rule_names().contains(&name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::planner::plan::{LogicalPlan, ValuesNode};

    #[test]
    fn query_pipeline_is_empty_and_noop_in_phase_one() {
        let pipeline = query_rewrite_pipeline();
        assert!(pipeline.rule_names().is_empty());

        let plan = LogicalPlan::Values(ValuesNode {
            rows: vec![],
            columns: vec![],
        });
        let before = format!("{plan:?}");
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        let after = pipeline.rewrite(plan, &mut ctx).expect("rewrite");
        assert_eq!(format!("{after:?}"), before);
    }

    #[test]
    fn mv_pipeline_is_empty_and_noop_in_phase_one() {
        let pipeline = mv_rewrite_pipeline();
        assert!(pipeline.rule_names().is_empty());

        let plan = LogicalPlan::Values(ValuesNode {
            rows: vec![],
            columns: vec![],
        });
        let before = format!("{plan:?}");
        let mut ctx = RewriteContext::for_mv_refresh(Vec::<String>::new());
        ctx.set_extension("iceberg-mv-refresh-context");
        let after = pipeline.rewrite(plan, &mut ctx).expect("rewrite");
        assert_eq!(format!("{after:?}"), before);
        assert_eq!(
            ctx.extension::<&'static str>(),
            Some(&"iceberg-mv-refresh-context")
        );
    }

    #[test]
    fn rewrite_registry_has_no_rule_names_before_rules_are_migrated() {
        assert!(!is_known_rewrite_rule_name("JoinCommutativity"));
        assert!(!is_known_rewrite_rule_name(""));
    }
}
```

- [ ] **Step 5: Run pipeline and registry tests**

Run:

```bash
cargo test --lib sql::optimizer::rewrite::pipeline -- --nocapture
cargo test --lib sql::optimizer::rewrite::registry -- --nocapture
```

Expected: pipeline and registry test modules pass.

- [ ] **Step 6: Commit pipeline and registry**

Run:

```bash
git add src/sql/optimizer/rewrite/pipeline.rs src/sql/optimizer/rewrite/registry.rs
git commit -m "feat: add logical rewrite pipeline"
```

## Task 4: Query Optimizer Integration

**Files:**
- Modify: `src/sql/optimizer/mod.rs`

- [ ] **Step 1: Add failing integration test**

Append this test to the existing `#[cfg(test)] mod is_known_rule_name_tests` in `src/sql/optimizer/mod.rs`:

```rust
    #[test]
    fn optimize_accepts_empty_query_rewrite_pipeline() {
        use std::collections::HashMap;

        use crate::sql::column_id::ColumnRefFactory;
        use crate::sql::planner::plan::{LogicalPlan, ValuesNode};

        let plan = LogicalPlan::Values(ValuesNode {
            rows: vec![],
            columns: vec![],
        });
        let factory = ColumnRefFactory::new();
        let physical = optimize(plan, &HashMap::new(), factory).expect("optimize values");
        let physical_debug = format!("{physical:?}");
        assert!(physical_debug.contains("PhysicalValues"));
    }
```

- [ ] **Step 2: Run the integration test before wiring the pipeline**

Run:

```bash
cargo test --lib sql::optimizer::is_known_rule_name_tests::optimize_accepts_empty_query_rewrite_pipeline -- --exact
```

Expected: pass before wiring. This establishes the baseline query plan still optimizes.

- [ ] **Step 3: Wire empty query rewrite pipeline into `optimize()`**

In `src/sql/optimizer/mod.rs`, replace the current optimizer settings construction:

```rust
let options =
    options::OptimizerOptions::from_session(&options::current_session_optimizer_settings());
```

with:

```rust
let session_settings = options::current_session_optimizer_settings();
let options = options::OptimizerOptions::from_session(&session_settings);
let mut rewrite_ctx = rewrite::context::RewriteContext::for_query(
    session_settings.disabled_rules.clone(),
);
let plan = rewrite::registry::query_rewrite_pipeline().rewrite(plan, &mut rewrite_ctx)?;
```

In the same file, update `is_known_rule_name` by appending the rewrite registry check:

```rust
        || rewrite::registry::is_known_rewrite_rule_name(name)
```

The final function should keep existing checks and add rewrite lookup as the last disjunct.

- [ ] **Step 4: Run query optimizer integration test after wiring**

Run:

```bash
cargo test --lib sql::optimizer::is_known_rule_name_tests::optimize_accepts_empty_query_rewrite_pipeline -- --exact
```

Expected: pass with the same `PhysicalValues` assertion.

- [ ] **Step 5: Run known-rule tests**

Run:

```bash
cargo test --lib sql::optimizer::is_known_rule_name_tests -- --nocapture
```

Expected: existing known-rule tests still pass. `JoinCommutativity` remains known through existing CBO registry; unknown names remain unknown.

- [ ] **Step 6: Commit query integration**

Run:

```bash
git add src/sql/optimizer/mod.rs
git commit -m "feat: wire query logical rewrite pipeline"
```

## Task 5: Focused Verification

**Files:**
- No source edits.

- [ ] **Step 1: Run all rewrite unit tests**

Run:

```bash
cargo test --lib sql::optimizer::rewrite -- --nocapture
```

Expected: all rewrite module tests pass.

- [ ] **Step 2: Run optimizer known-rule and integration tests**

Run:

```bash
cargo test --lib sql::optimizer::is_known_rule_name_tests -- --nocapture
```

Expected: all tests in `is_known_rule_name_tests` pass.

- [ ] **Step 3: Run focused existing RBO test group**

Run:

```bash
cargo test --lib sql::optimizer::rbo -- --nocapture
```

Expected: existing RBO tests pass, demonstrating the new pipeline did not disturb current RBO behavior.

- [ ] **Step 4: Run formatting**

Run:

```bash
cargo fmt
```

Expected: formatting completes without changing unrelated files.

- [ ] **Step 5: Run diff hygiene**

Run:

```bash
git diff --check
```

Expected: no whitespace errors.

- [ ] **Step 6: Commit verification-only formatting if needed**

If `cargo fmt` changed files that belong to this plan, run:

```bash
git add src/sql/optimizer/mod.rs src/sql/optimizer/rewrite
git commit -m "style: format logical rewrite framework"
```

If `cargo fmt` produced no diff, do not create an empty commit.

## Task 6: Plan Completion Check

**Files:**
- No source edits.

- [ ] **Step 1: Inspect final diff against the design**

Run:

```bash
git log --oneline -5
git status --short
```

Expected: recent commits include the logical rewrite framework commits, and status is clean.

- [ ] **Step 2: Confirm scope boundaries**

Run:

```bash
rg -n "Delta|Version|Action|Ivm|IVM|iceberg" src/sql/optimizer/rewrite src/sql/optimizer/mod.rs
```

Expected: no `Delta`, `Version`, `Action`, `Ivm`, or `IVM` hits under `src/sql/optimizer/rewrite`; an `iceberg` hit is acceptable only if it appears in a test extension string outside framework type names.

- [ ] **Step 3: Summarize implementation**

Prepare a short summary with:

```text
- Added generic logical rewrite framework under src/sql/optimizer/rewrite.
- Wired an empty query rewrite pipeline before existing RBO passes.
- Added empty MV pipeline/context adapter without MV-specific rewrite semantics.
- Verified rewrite, optimizer known-rule, RBO tests, fmt, and diff hygiene.
```
