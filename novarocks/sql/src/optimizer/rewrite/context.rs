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

use std::any::Any;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Instant;

use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};

use crate::column_id::ColumnRefFactory;
use crate::compiler::SqlCompileError;
use crate::optimizer::options::SessionOptimizerSettings;
use crate::optimizer::rewrite::trace::RewriteTrace;
use crate::optimizer::scalar::{ScalarArena, ScalarId};
use crate::optimizer::stats_input::OptimizerStatsInput;

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
pub(crate) struct RewriteContext<'a> {
    control: &'a dyn PureCompileControl,
    #[allow(
        dead_code,
        reason = "Retained for staged SQL planner migration consumers and test helpers."
    )]
    consumer: RewriteConsumer,
    disabled_rules: HashSet<String>,
    session_settings: SessionOptimizerSettings,
    decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy,
    root_allow_throw_exception: bool,
    policy: RewritePolicy,
    trace: RewriteTrace,
    extension: Option<Arc<dyn Any + Send + Sync>>,
    query_stats_input: Option<Arc<OptimizerStatsInput>>,
    deadline: Option<Instant>,
    column_ref_factory: Option<Rc<RefCell<ColumnRefFactory>>>,
    /// Interned scalar arena for the current optimize() call. Set before the
    /// rewrite phase (mirrors `column_ref_factory`); rules that inspect or
    /// build scalars go through this. Unwrapped into `Memo.scalars` at convert.
    scalar_arena: Option<Rc<RefCell<ScalarArena>>>,
    /// Indexed by the rebuilt scalar with all constant children.
    fold_cache: RefCell<HashMap<ScalarId, Option<ScalarId>>>,
    /// Process-lifetime constant evaluator injected at the compiler boundary.
    /// `None` means the compile path has no execution capability attached, and
    /// constant folding degrades to a no-op.
    constant_evaluator: Option<&'static dyn crate::compiler::SqlConstantEvaluator>,
    fold_dependency_observer: Option<Arc<dyn crate::compiler::SqlFoldDependencyObserver>>,
    function_catalog: Option<Arc<dyn crate::compiler::SqlFunctionCatalog>>,
    sealed_functions: Option<Arc<novarocks_functions::PureEngineFunctionCatalog>>,
}

impl<'a> RewriteContext<'a> {
    pub(crate) fn new(
        consumer: RewriteConsumer,
        session_settings: SessionOptimizerSettings,
        decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy,
        control: &'a dyn PureCompileControl,
    ) -> Self {
        Self {
            control,
            consumer,
            disabled_rules: session_settings.disabled_rules.iter().cloned().collect(),
            session_settings,
            decimal_overflow_policy,
            root_allow_throw_exception: false,
            policy: RewritePolicy::default(),
            trace: RewriteTrace::default(),
            extension: None,
            query_stats_input: None,
            deadline: None,
            column_ref_factory: None,
            scalar_arena: None,
            fold_cache: RefCell::new(HashMap::new()),
            constant_evaluator: None,
            fold_dependency_observer: None,
            function_catalog: None,
            sealed_functions: None,
        }
    }

    pub(crate) fn for_query_with_settings(
        session_settings: SessionOptimizerSettings,
        decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy,
        control: &'a dyn PureCompileControl,
    ) -> Self {
        Self::new(
            RewriteConsumer::Query,
            session_settings,
            decimal_overflow_policy,
            control,
        )
    }

    pub(crate) fn for_mv_refresh_with_settings(
        session_settings: SessionOptimizerSettings,
        decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy,
        control: &'a dyn PureCompileControl,
    ) -> Self {
        let mut ctx = Self::new(
            RewriteConsumer::MaterializedViewRefresh,
            session_settings,
            decimal_overflow_policy,
            control,
        );
        ctx.policy.failure_policy = RewriteFailurePolicy::FailFast;
        ctx
    }

    #[cfg(test)]
    pub(crate) fn for_query(disabled_rules: impl IntoIterator<Item = String>) -> Self {
        Self::for_query_with_settings(
            SessionOptimizerSettings {
                disabled_rules: disabled_rules.into_iter().collect(),
                ..Default::default()
            },
            novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            unbounded_rewrite_test_control(),
        )
    }

    #[cfg(test)]
    pub(crate) fn for_mv_refresh(disabled_rules: impl IntoIterator<Item = String>) -> Self {
        Self::for_mv_refresh_with_settings(
            SessionOptimizerSettings {
                disabled_rules: disabled_rules.into_iter().collect(),
                ..Default::default()
            },
            novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            unbounded_rewrite_test_control(),
        )
    }

    #[allow(
        dead_code,
        reason = "Retained for staged SQL planner migration consumers and test helpers."
    )]
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

    pub(crate) fn session_settings(&self) -> &SessionOptimizerSettings {
        &self.session_settings
    }

    pub(crate) fn decimal_overflow_policy(&self) -> novarocks_type_contract::DecimalOverflowPolicy {
        self.decimal_overflow_policy
    }

    pub(crate) fn set_root_allow_throw_exception(&mut self, value: bool) {
        self.root_allow_throw_exception = value;
    }
    pub(crate) fn root_allow_throw_exception(&self) -> bool {
        self.root_allow_throw_exception
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

    pub(crate) fn set_query_stats_input(&mut self, stats_input: OptimizerStatsInput) {
        self.query_stats_input = Some(Arc::new(stats_input));
    }

    pub(crate) fn query_stats_input(&self) -> Option<&OptimizerStatsInput> {
        self.query_stats_input.as_deref()
    }

    pub(crate) fn set_deadline(&mut self, deadline: Instant) {
        self.deadline = Some(deadline);
    }

    pub(crate) fn set_column_ref_factory(&mut self, factory: Rc<RefCell<ColumnRefFactory>>) {
        self.column_ref_factory = Some(factory);
    }

    pub(crate) fn column_ref_factory(&self) -> Option<&Rc<RefCell<ColumnRefFactory>>> {
        self.column_ref_factory.as_ref()
    }

    pub(crate) fn set_sealed_functions(
        &mut self,
        functions: Arc<novarocks_functions::PureEngineFunctionCatalog>,
    ) {
        self.sealed_functions = Some(functions);
    }

    pub(crate) fn sealed_functions(
        &self,
    ) -> Option<&Arc<novarocks_functions::PureEngineFunctionCatalog>> {
        self.sealed_functions.as_ref()
    }

    pub(crate) fn set_constant_evaluator(
        &mut self,
        evaluator: &'static dyn crate::compiler::SqlConstantEvaluator,
    ) {
        self.constant_evaluator = Some(evaluator);
    }

    pub(crate) fn constant_evaluator(
        &self,
    ) -> Option<&'static dyn crate::compiler::SqlConstantEvaluator> {
        self.constant_evaluator
    }

    pub(crate) fn set_fold_dependency_observer(
        &mut self,
        observer: Option<Arc<dyn crate::compiler::SqlFoldDependencyObserver>>,
    ) {
        self.fold_dependency_observer = observer;
    }
    pub(crate) fn fold_dependency_observer(
        &self,
    ) -> Option<&dyn crate::compiler::SqlFoldDependencyObserver> {
        self.fold_dependency_observer.as_deref()
    }
    pub(crate) fn set_function_catalog(
        &mut self,
        catalog: Arc<dyn crate::compiler::SqlFunctionCatalog>,
    ) {
        self.function_catalog = Some(catalog);
    }

    pub(crate) fn function_catalog(&self) -> &dyn crate::compiler::SqlFunctionCatalog {
        self.function_catalog_arc().as_ref()
    }

    pub(crate) fn function_catalog_arc(&self) -> &Arc<dyn crate::compiler::SqlFunctionCatalog> {
        self.function_catalog
            .as_ref()
            .expect("function catalog must be set before optimizer rewrite")
    }

    pub(crate) fn cached_fold(&self, input: ScalarId) -> Option<Option<ScalarId>> {
        self.fold_cache.borrow().get(&input).copied()
    }

    pub(crate) fn cache_fold(&self, input: ScalarId, folded: Option<ScalarId>) {
        self.fold_cache.borrow_mut().insert(input, folded);
    }

    pub(crate) fn set_scalar_arena(&mut self, arena: Rc<RefCell<ScalarArena>>) {
        self.fold_cache.get_mut().clear();
        self.scalar_arena = Some(arena);
    }

    /// The interned scalar arena for this rewrite run. Panics if accessed
    /// before being set — the arena is always installed before the pipeline.
    pub(crate) fn scalar_arena(&self) -> Rc<RefCell<ScalarArena>> {
        Rc::clone(
            self.scalar_arena
                .as_ref()
                .expect("scalar arena must be set before rewrite"),
        )
    }

    /// A temporary observation view; neither the request nor this view enters
    /// the scalar arena, rewrite trace, or rewritten output.
    pub(crate) fn control_view(&self) -> RewriteControl<'a> {
        RewriteControl {
            request: self.control,
            deadline: self.deadline,
        }
    }

    pub(crate) fn check_deadline(&self, _operation: &str) -> Result<(), SqlCompileError> {
        self.control_view()
            .checkpoint(CompilePhase::Validate, 0)
            .map_err(crate::compiler::SqlCompileError::from)
    }
}

#[derive(Clone, Copy)]
pub(crate) struct RewriteControl<'a> {
    request: &'a dyn PureCompileControl,
    deadline: Option<Instant>,
}

impl PureCompileControl for RewriteControl<'_> {
    fn checkpoint(&self, phase: CompilePhase, work_units: u32) -> Result<(), CompileControlError> {
        self.request.checkpoint(phase, work_units)?;
        if self
            .deadline
            .is_some_and(|deadline| Instant::now() > deadline)
        {
            return Err(CompileControlError::DeadlineExceeded);
        }
        Ok(())
    }
}

#[cfg(test)]
pub(crate) fn unbounded_rewrite_test_control() -> &'static dyn PureCompileControl {
    static CONTROL: std::sync::LazyLock<crate::compiler::SqlCompileControl> =
        std::sync::LazyLock::new(crate::compiler::SqlCompileControl::unbounded);
    &*CONTROL
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::optimizer::rewrite::phase::RewritePhase;

    #[derive(Debug, PartialEq, Eq)]
    struct TestExtension {
        value: i32,
    }

    #[test]
    fn query_context_uses_disabled_rules() {
        let ctx = RewriteContext::for_query(vec!["RuleA".to_string()]);
        assert_eq!(ctx.consumer(), RewriteConsumer::Query);
        assert_eq!(
            ctx.policy().failure_policy,
            RewriteFailurePolicy::CollectDiagnostics
        );
        assert_eq!(ctx.policy().max_iterations, 8);
        assert!(!ctx.is_rule_enabled("RuleA"));
        assert!(ctx.is_rule_enabled("RuleB"));
    }

    #[test]
    fn context_exposes_mutable_policy_and_trace() {
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        ctx.policy_mut().max_iterations = 3;
        ctx.trace_mut().phase_started(RewritePhase::Validation);

        assert_eq!(ctx.policy().max_iterations, 3);
        assert_eq!(ctx.trace().events().len(), 1);
    }

    #[test]
    fn mv_context_defaults_to_fail_fast() {
        let ctx = RewriteContext::for_mv_refresh(Vec::<String>::new());
        assert_eq!(ctx.consumer(), RewriteConsumer::MaterializedViewRefresh);
        assert_eq!(ctx.policy().failure_policy, RewriteFailurePolicy::FailFast);
    }

    #[test]
    fn context_extension_round_trips() {
        let mut ctx = RewriteContext::for_mv_refresh(Vec::<String>::new());
        ctx.set_extension(TestExtension { value: 7 });
        assert_eq!(
            ctx.extension::<TestExtension>(),
            Some(&TestExtension { value: 7 })
        );
        assert!(ctx.extension::<String>().is_none());
    }

    #[test]
    fn query_context_exposes_stats_input() {
        use crate::optimizer::statistics::TableStatistics;
        use std::collections::HashMap;

        let mut stats = HashMap::new();
        stats.insert(
            "db.tbl".to_string(),
            TableStatistics {
                row_count: 10,
                column_stats: HashMap::new(),
            },
        );

        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        ctx.set_query_stats_input(OptimizerStatsInput::from_test_table_statistics(&stats));

        assert!(
            ctx.query_stats_input()
                .unwrap()
                .test_table_statistics()
                .unwrap()
                .contains_key("db.tbl")
        );
    }

    #[test]
    fn column_ref_factory_can_be_set_and_read() {
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        assert!(ctx.column_ref_factory().is_none());
        let factory = Rc::new(RefCell::new(ColumnRefFactory::default()));
        ctx.set_column_ref_factory(Rc::clone(&factory));
        assert!(ctx.column_ref_factory().is_some());
    }

    #[test]
    fn existing_optimizer_deadline_is_typed_and_clone_borrows_same_request() {
        let control = crate::compiler::SqlCompileControl::unbounded();
        let mut ctx = RewriteContext::for_query_with_settings(
            Default::default(),
            novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            &control,
        );
        ctx.set_deadline(Instant::now() - std::time::Duration::from_millis(1));
        let cloned = ctx.clone();
        assert!(std::ptr::eq(ctx.control, cloned.control));
        assert_eq!(
            ctx.check_deadline("test"),
            Err(SqlCompileError::DeadlineExceeded)
        );
    }
}
