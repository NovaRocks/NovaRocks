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
use std::collections::HashSet;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Instant;

use crate::sql::column_id::ColumnRefFactory;
use crate::sql::optimizer::options::SessionOptimizerSettings;
use crate::sql::optimizer::rewrite::trace::RewriteTrace;
use crate::sql::optimizer::scalar::ScalarArena;
use crate::sql::optimizer::stats_input::OptimizerStatsInput;

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
    session_settings: SessionOptimizerSettings,
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
}

impl RewriteContext {
    pub(crate) fn new(
        consumer: RewriteConsumer,
        session_settings: SessionOptimizerSettings,
    ) -> Self {
        Self {
            consumer,
            disabled_rules: session_settings.disabled_rules.iter().cloned().collect(),
            session_settings,
            policy: RewritePolicy::default(),
            trace: RewriteTrace::default(),
            extension: None,
            query_stats_input: None,
            deadline: None,
            column_ref_factory: None,
            scalar_arena: None,
        }
    }

    pub(crate) fn for_query_with_settings(session_settings: SessionOptimizerSettings) -> Self {
        Self::new(RewriteConsumer::Query, session_settings)
    }

    pub(crate) fn for_mv_refresh_with_settings(session_settings: SessionOptimizerSettings) -> Self {
        let mut ctx = Self::new(RewriteConsumer::MaterializedViewRefresh, session_settings);
        ctx.policy.failure_policy = RewriteFailurePolicy::FailFast;
        ctx
    }

    #[cfg(test)]
    pub(crate) fn for_query(disabled_rules: impl IntoIterator<Item = String>) -> Self {
        Self::for_query_with_settings(SessionOptimizerSettings {
            disabled_rules: disabled_rules.into_iter().collect(),
            ..Default::default()
        })
    }

    #[cfg(test)]
    pub(crate) fn for_mv_refresh(disabled_rules: impl IntoIterator<Item = String>) -> Self {
        Self::for_mv_refresh_with_settings(SessionOptimizerSettings {
            disabled_rules: disabled_rules.into_iter().collect(),
            ..Default::default()
        })
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

    pub(crate) fn session_settings(&self) -> &SessionOptimizerSettings {
        &self.session_settings
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

    pub(crate) fn set_scalar_arena(&mut self, arena: Rc<RefCell<ScalarArena>>) {
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

    pub(crate) fn check_deadline(&self, operation: &str) -> Result<(), String> {
        if self
            .deadline
            .is_some_and(|deadline| Instant::now() > deadline)
        {
            Err(format!("optimizer timeout during {operation}"))
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::optimizer::rewrite::phase::RewritePhase;

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
        use crate::sql::optimizer::statistics::TableStatistics;
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
}
