use std::any::Any;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use crate::engine::dictionary::model::DictionarySnapshot;
use crate::sql::catalog::TableDef;
use crate::sql::optimizer::rewrite::trace::RewriteTrace;
use crate::sql::optimizer::statistics::TableStatistics;

/// Loads dictionary snapshots for scan-time low-cardinality string columns.
/// Implemented by the engine layer (production) and by tests (fakes).
pub(crate) trait QueryDictionaryProvider: Send + Sync {
    fn load_active_snapshot(
        &self,
        table: &TableDef,
        database: &str,
        column_name: &str,
    ) -> Result<Option<DictionarySnapshot>, String>;
}

thread_local! {
    /// Per-thread fallback dictionary provider. Set by
    /// `StandaloneSession::execute_in_context` for the duration of one
    /// SQL statement so that the many downstream engine entry points
    /// that funnel into `optimize()` do not each have to thread the
    /// provider through their signatures. The provider passed
    /// explicitly to `optimize()` takes precedence.
    static CURRENT_DICTIONARY_PROVIDER: RefCell<Option<Arc<dyn QueryDictionaryProvider>>> =
        const { RefCell::new(None) };
}

/// RAII guard restoring the previous dictionary provider on scope exit.
/// Restoration runs in `Drop`, which fires on both normal return and
/// stack unwinding from a panic. This guarantees the TLS slot cannot
/// leak a provider into the next query that lands on the same worker
/// thread (e.g. a `spawn_blocking` pool thread).
struct DictionaryProviderGuard {
    previous: Option<Arc<dyn QueryDictionaryProvider>>,
}

impl Drop for DictionaryProviderGuard {
    fn drop(&mut self) {
        let previous = self.previous.take();
        CURRENT_DICTIONARY_PROVIDER.with(|cell| {
            cell.replace(previous);
        });
    }
}

/// Install `provider` as the current-thread fallback for the duration
/// of `f`. The previous binding is restored on scope exit by an RAII
/// guard, so a panic inside `f` still restores correctly during stack
/// unwinding (relevant on worker pools where the same thread is reused
/// across queries).
pub(crate) fn with_dictionary_provider<T>(
    provider: Arc<dyn QueryDictionaryProvider>,
    f: impl FnOnce() -> T,
) -> T {
    let previous = CURRENT_DICTIONARY_PROVIDER.with(|cell| cell.replace(Some(provider)));
    let _guard = DictionaryProviderGuard { previous };
    f()
}

pub(crate) fn current_dictionary_provider() -> Option<Arc<dyn QueryDictionaryProvider>> {
    CURRENT_DICTIONARY_PROVIDER.with(|cell| cell.borrow().clone())
}

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
    query_table_stats: Option<Arc<HashMap<String, TableStatistics>>>,
    deadline: Option<Instant>,
    dictionary_provider: Option<Arc<dyn QueryDictionaryProvider>>,
}

impl RewriteContext {
    pub(crate) fn new(consumer: RewriteConsumer) -> Self {
        Self {
            consumer,
            disabled_rules: HashSet::new(),
            policy: RewritePolicy::default(),
            trace: RewriteTrace::default(),
            extension: None,
            query_table_stats: None,
            deadline: None,
            dictionary_provider: None,
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

    pub(crate) fn set_query_table_stats(&mut self, table_stats: HashMap<String, TableStatistics>) {
        self.query_table_stats = Some(Arc::new(table_stats));
    }

    pub(crate) fn query_table_stats(&self) -> Option<&HashMap<String, TableStatistics>> {
        self.query_table_stats.as_deref()
    }

    pub(crate) fn set_deadline(&mut self, deadline: Instant) {
        self.deadline = Some(deadline);
    }

    pub(crate) fn set_dictionary_provider(&mut self, provider: Arc<dyn QueryDictionaryProvider>) {
        self.dictionary_provider = Some(provider);
    }

    pub(crate) fn dictionary_provider(&self) -> Option<&Arc<dyn QueryDictionaryProvider>> {
        self.dictionary_provider.as_ref()
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

    // --- Item 2 (Critical) regression: with_dictionary_provider RAII ---

    /// Marker provider that records its label so the test can prove
    /// which `Arc` is currently installed in the TLS slot.
    struct LabelledProvider {
        label: &'static str,
    }

    impl QueryDictionaryProvider for LabelledProvider {
        fn load_active_snapshot(
            &self,
            _table: &TableDef,
            _database: &str,
            _column_name: &str,
        ) -> Result<Option<DictionarySnapshot>, String> {
            Err(format!("LabelledProvider({})", self.label))
        }
    }

    fn current_provider_label() -> Option<String> {
        let provider = current_dictionary_provider()?;
        // Trigger load_active_snapshot to extract the embedded label.
        // We have no `TableDef::dummy()` helper handy, so reach for the
        // engine layer's dictionary model and synthesize one inline.
        use crate::sql::catalog::{ScanSource, TableDef};
        let table = TableDef {
            name: "probe".to_string(),
            columns: vec![],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::StarRocks { db_id: 0, table_id: 0 },
        };
        let err = provider
            .load_active_snapshot(&table, "db", "col")
            .err()
            .expect("LabelledProvider always errors");
        Some(err)
    }

    #[test]
    fn with_dictionary_provider_restores_after_panic() {
        // Sanity: TLS starts unset for this thread.
        let baseline = current_dictionary_provider();

        let outer: Arc<dyn QueryDictionaryProvider> = Arc::new(LabelledProvider { label: "outer" });
        with_dictionary_provider(outer.clone(), || {
            assert_eq!(
                current_provider_label().as_deref(),
                Some("LabelledProvider(outer)"),
                "outer provider must be visible inside its scope"
            );

            // Now run an inner scope that installs a different provider
            // and panics. The outer provider must be restored after
            // the catch_unwind.
            let inner_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let inner: Arc<dyn QueryDictionaryProvider> =
                    Arc::new(LabelledProvider { label: "inner" });
                with_dictionary_provider(inner, || {
                    assert_eq!(
                        current_provider_label().as_deref(),
                        Some("LabelledProvider(inner)")
                    );
                    panic!("simulated optimizer panic");
                });
            }));
            assert!(inner_result.is_err(), "inner closure must propagate panic");

            // RAII guard from with_dictionary_provider must have
            // restored the outer provider on unwind.
            assert_eq!(
                current_provider_label().as_deref(),
                Some("LabelledProvider(outer)"),
                "outer provider must be restored after inner panic"
            );
        });

        // Outer scope exited normally — TLS must match the pre-test
        // state (None on a fresh thread; otherwise whatever the
        // surrounding test runner installed).
        let after = current_dictionary_provider();
        match (baseline.as_ref(), after.as_ref()) {
            (None, None) => {}
            (Some(a), Some(b)) => assert!(Arc::ptr_eq(a, b)),
            _ => panic!("TLS provider state differs across with_dictionary_provider call"),
        }
    }

    #[test]
    fn query_context_exposes_table_statistics() {
        let mut stats = HashMap::new();
        stats.insert(
            "db.tbl".to_string(),
            TableStatistics {
                row_count: 10,
                column_stats: HashMap::new(),
            },
        );

        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        ctx.set_query_table_stats(stats);

        assert!(ctx.query_table_stats().unwrap().contains_key("db.tbl"));
    }
}
