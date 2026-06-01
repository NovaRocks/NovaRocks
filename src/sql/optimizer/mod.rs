//! Cascades optimizer framework.

pub(crate) mod cascades_rules;
pub(crate) mod convert;
pub(crate) mod cost;
pub(crate) mod cte_rewrite;
pub(crate) mod derive;
pub(crate) mod extract;
pub(crate) mod logical_props;
pub(crate) mod memo;
pub(crate) mod operator;
pub(crate) mod options;
pub(crate) mod physical_plan;
pub(crate) mod property;
pub(crate) mod rewrite;
pub(crate) mod rule;
pub(crate) mod runtime_filter_pass;
pub(crate) mod runtime_filter_planner;
pub(crate) mod search;
pub(crate) mod statistics;
pub(crate) mod stats;

pub(crate) use memo::Memo;
pub(crate) use operator::Operator;
pub(crate) use physical_plan::PhysicalPlanNode;
pub(crate) use property::PhysicalPropertySet;

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::time::{Duration, Instant};

use crate::sql::column_id::ColumnRefFactory;
use crate::sql::optimizer::statistics::TableStatistics;
use crate::sql::planner::plan::LogicalPlan;
use memo::MExpr;
use rule::Rule;

/// Wall-clock timeout for the entire optimization pipeline.
///
/// Raised from 10 s to 30 s after profiling the join-suite timeouts on
/// wide-table queries (`join_one_key` step 19 — a `LEFT SEMI` over a
/// 33-column 1.28M-row table — used the entire 10 s budget on
/// statistics-driven cost estimation in Cascades, and the same cost
/// model dominated every other affected join case). 30 s matches the
/// budget StarRocks FE and other comparable engines use, leaves room
/// for genuinely large plans, and is still small enough that a runaway
/// rule will surface rather than silently spin.
const OPTIMIZE_TIMEOUT: Duration = Duration::from_secs(30);

/// Maximum number of memo groups allowed during exploration.
/// Prevents exponential blowup from join associativity on large join graphs.
const EXPLORE_MAX_GROUPS: usize = 5000;

/// Main entry point for the Cascades optimizer.
///
/// Takes a logical plan and table statistics, applies query logical rewrites,
/// converts to Memo, explores logical alternatives, generates physical
/// alternatives, runs top-down cost-based search with property enforcement,
/// and extracts the best physical plan.
pub(crate) fn optimize(
    plan: LogicalPlan,
    table_stats: &HashMap<String, TableStatistics>,
    factory: ColumnRefFactory,
    dictionary_provider: Option<std::sync::Arc<dyn rewrite::context::QueryDictionaryProvider>>,
) -> Result<PhysicalPlanNode, String> {
    let deadline = Instant::now() + OPTIMIZE_TIMEOUT;

    // Wrap factory in Rc<RefCell<...>> so it can be shared with RewriteContext
    // for the duration of the rewrite phase (needed for auto-fill column minting
    // in per-operator column-pruning rules). The Rc is unwrapped back to a plain
    // ColumnRefFactory before the Memo build step.
    let factory = Rc::new(RefCell::new(factory));

    // 1. Query logical rewrite pipeline. The ordered stages preserve the
    //    legacy-safe sequence: pushdown → join reorder → pushdown →
    //    aggregate pushdown → column pruning → low-cardinality dict rewrite.
    let session_settings = options::current_session_optimizer_settings();
    let options = options::OptimizerOptions::from_session(&session_settings);
    let mut rewrite_ctx =
        rewrite::context::RewriteContext::for_query(session_settings.disabled_rules.clone());
    rewrite_ctx.policy_mut().max_iterations = options.rewrite_max_iterations;
    rewrite_ctx.set_query_table_stats(table_stats.clone());
    rewrite_ctx.set_deadline(deadline);
    if let Some(provider) = resolve_dictionary_provider(dictionary_provider) {
        rewrite_ctx.set_dictionary_provider(provider);
    }
    rewrite_ctx.set_column_ref_factory(Rc::clone(&factory));
    let rewritten =
        rewrite::registry::query_rewrite_pipeline(table_stats).rewrite(plan, &mut rewrite_ctx)?;

    // 4. CTE cleanup: intentional pre-Memo structural rewrite for CTE shape
    //    cleanup, not a second full logical optimization pass.
    let cte_ctx = cte_rewrite::collect_cte_counts(&rewritten);
    let rewritten = cte_rewrite::inline_single_use_ctes(rewritten, &cte_ctx);

    // 5. Convert to Memo. Unwrap the factory from Rc<RefCell<...>> — rewrite
    //    is done so the only two references at this call site are the local
    //    `factory` binding and the clone stored in `rewrite_ctx`. Drop the
    //    context's reference first, then unwrap the local one.
    debug_assert_eq!(
        Rc::strong_count(&factory),
        2,
        "expected exactly 2 Rc references (factory + rewrite_ctx) before drop; \
         a rewrite rule stored an extra clone of the context — check column_ref_factory() call sites"
    );
    drop(rewrite_ctx);
    let factory = Rc::try_unwrap(factory)
        .expect(
            "ColumnRefFactory Rc must be uniquely owned after rewrite; \
             a rule cloned the context and did not drop the clone",
        )
        .into_inner();
    let mut memo = Memo::new();
    memo.factory = factory;
    let root_group = convert::logical_plan_to_memo(&rewritten, &mut memo);

    // 6. Derive initial statistics.
    stats::derive_group_statistics(&mut memo, table_stats);

    check_deadline(deadline)?;

    // 7. Explore: apply transformation rules (logical -> logical).
    let transform_rules = cascades_rules::all_transformation_rules();
    explore(&mut memo, &transform_rules, &options, deadline)?;

    check_deadline(deadline)?;

    // 8. Implement: apply implementation rules (logical -> physical).
    let impl_rules = cascades_rules::all_implementation_rules();
    implement(&mut memo, &impl_rules, &options);

    // 9. Re-derive statistics for any newly created groups (e.g. from AggSplit).
    stats::derive_group_statistics(&mut memo, table_stats);

    check_deadline(deadline)?;

    // 10. Top-down search with property enforcement.
    let root_required = PhysicalPropertySet::gather();
    let mut ctx = search::SearchContext::new(table_stats.clone());
    ctx.optimize_group(&memo, root_group, &root_required)?;

    check_deadline(deadline)?;

    // 11. Extract best plan.
    let mut physical = extract::extract_best(&memo, root_group, &root_required, &ctx.winners)?;

    // 12. Annotate physical plan with runtime filter descriptors.
    runtime_filter_pass::annotate(&mut physical, &options);

    Ok(physical)
}

/// Resolve which dictionary provider should drive the
/// `LowCardinalityDictionaryRewrite` rule for this `optimize()` call.
///
/// Precedence: an explicit `parameter` (passed by the caller) wins over
/// any `with_dictionary_provider` TLS binding. The parameter wins even
/// when it is a provider that returns `None` for every column, so
/// production sites that pass `None` deliberately fall through to TLS
/// instead of overriding it with a silent no-op.
fn resolve_dictionary_provider(
    parameter: Option<std::sync::Arc<dyn rewrite::context::QueryDictionaryProvider>>,
) -> Option<std::sync::Arc<dyn rewrite::context::QueryDictionaryProvider>> {
    parameter.or_else(rewrite::context::current_dictionary_provider)
}

/// True if `name` is the stable name of any rule that participates in
/// the standard `optimize()` rule pipelines (query logical rewrite,
/// CBO transformations, CBO implementations).
///
/// Used by the server-side `SET disable_optimizer_rules` parser to
/// detect typos in rule names so they can be surfaced via `warn!`
/// without rejecting the SET statement.
pub(crate) fn is_known_rule_name(name: &str) -> bool {
    cascades_rules::all_transformation_rules()
        .iter()
        .any(|r| r.name() == name)
        || cascades_rules::all_implementation_rules()
            .iter()
            .any(|r| r.name() == name)
        || rewrite::registry::is_known_rewrite_rule_name(name)
        || name == runtime_filter_pass::RUNTIME_FILTER_RULE
}

fn check_deadline(deadline: Instant) -> Result<(), String> {
    if Instant::now() > deadline {
        return Err(format!(
            "optimizer timeout: exceeded {}s budget",
            OPTIMIZE_TIMEOUT.as_secs()
        ));
    }
    Ok(())
}

/// Apply transformation rules to all groups in a fixed-point loop.
///
/// Terminates when:
/// - No new expressions are added (fixed-point)
/// - Iteration limit reached
/// - Memo group count exceeds budget (join associativity explosion guard)
/// - Wall-clock deadline exceeded
const EXPLORE_MAX_ITERATIONS: usize = 16;

fn explore(
    memo: &mut Memo,
    rules: &[Box<dyn Rule>],
    options: &options::OptimizerOptions,
    deadline: Instant,
) -> Result<(), String> {
    for _round in 0..EXPLORE_MAX_ITERATIONS {
        if Instant::now() > deadline {
            return Err(format!(
                "optimizer timeout during exploration: exceeded {}s budget",
                OPTIMIZE_TIMEOUT.as_secs()
            ));
        }
        let mut changed = false;
        let num_groups = memo.groups.len();
        for group_id in 0..num_groups {
            if Instant::now() > deadline {
                return Err(format!(
                    "optimizer timeout: exceeded {}s budget",
                    OPTIMIZE_TIMEOUT.as_secs()
                ));
            }
            let exprs: Vec<MExpr> = memo.groups[group_id].logical_exprs.clone();
            for expr in &exprs {
                for rule in rules {
                    if !options.is_enabled(rule.name()) {
                        continue;
                    }
                    // Skip JoinAssociativity when the memo has grown large
                    // to prevent combinatorial explosion. The query rewrite
                    // join reorder stage already handles large join graphs.
                    if rule.name() == "JoinAssociativity" && memo.groups.len() > 200 {
                        continue;
                    }
                    if rule.matches(&expr.op) {
                        let new_exprs = rule.apply(expr, memo);
                        for new_expr in new_exprs {
                            // Dedup: compare operator AND children to avoid
                            // infinite JoinCommutativity A<->B oscillation.
                            let already_exists =
                                memo.groups[group_id].logical_exprs.iter().any(|existing| {
                                    existing.children == new_expr.children
                                        && op_equal(&existing.op, &new_expr.op)
                                });
                            if !already_exists {
                                let mexpr = MExpr {
                                    id: memo.next_expr_id(),
                                    op: new_expr.op,
                                    children: new_expr.children,
                                };
                                memo.add_expr_to_group(group_id, mexpr);
                                changed = true;
                            }
                        }
                    }
                }
            }
            // Stop if memo grew too large (exponential join enumeration).
            if memo.groups.len() > EXPLORE_MAX_GROUPS {
                return Ok(());
            }
        }
        if !changed {
            break;
        }
    }
    Ok(())
}

/// Apply implementation rules to all groups.
///
/// Single pass — each logical expr gets physical alternatives once.
fn implement(memo: &mut Memo, rules: &[Box<dyn Rule>], options: &options::OptimizerOptions) {
    let mut changed = true;
    while changed {
        changed = false;
        let num_groups = memo.groups.len();
        for group_id in 0..num_groups {
            let exprs: Vec<MExpr> = memo.groups[group_id].logical_exprs.clone();
            for expr in &exprs {
                for rule in rules {
                    if !options.is_enabled(rule.name()) {
                        continue;
                    }
                    if rule.matches(&expr.op) {
                        let new_exprs = rule.apply(expr, memo);
                        for new_expr in new_exprs {
                            let already_exists = memo.groups[group_id]
                                .physical_exprs
                                .iter()
                                .any(|existing| op_equal(&existing.op, &new_expr.op));
                            if !already_exists {
                                let mexpr = MExpr {
                                    id: memo.next_expr_id(),
                                    op: new_expr.op,
                                    children: new_expr.children,
                                };
                                memo.add_expr_to_group(group_id, mexpr);
                                changed = true;
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Shallow equality check for operators (structural comparison via Debug format).
///
/// This is conservative: two operators are equal only if their Debug
/// representations match exactly. False negatives are harmless (we just
/// keep a duplicate in the group).
fn op_equal(a: &Operator, b: &Operator) -> bool {
    format!("{:?}", a) == format!("{:?}", b)
}

#[cfg(test)]
mod is_known_rule_name_tests {
    use super::*;

    #[test]
    fn optimize_accepts_migrated_query_rewrite_pipeline() {
        use std::collections::HashMap;

        use crate::sql::column_id::ColumnRefFactory;
        use crate::sql::planner::plan::{LogicalPlan, ValuesNode};

        let plan = LogicalPlan::Values(ValuesNode {
            rows: vec![],
            columns: vec![],
            required_output_columns: None,
        });
        let factory = ColumnRefFactory::new();
        let physical = optimize(plan, &HashMap::new(), factory, None).expect("optimize values");
        let physical_debug = format!("{physical:?}");
        assert!(physical_debug.contains("PhysicalValues"));
    }

    #[test]
    fn is_known_rule_name_recognizes_real_rule() {
        // JoinCommutativity is a transformation rule that has been stable
        // for a while; if this assertion fails because the rule was renamed,
        // pick another known rule name from src/sql/optimizer/cascades_rules/.
        assert!(is_known_rule_name("JoinCommutativity"));
    }

    #[test]
    fn is_known_rule_name_recognizes_split_aggregate_rule() {
        assert!(is_known_rule_name("SplitAggregateRule"));
    }

    #[test]
    fn is_known_rule_name_rejects_typos() {
        assert!(!is_known_rule_name("TotallyNotARealRule"));
        assert!(!is_known_rule_name(""));
    }

    #[test]
    fn is_known_rule_name_recognizes_runtime_filter() {
        assert!(is_known_rule_name("RuntimeFilterPushDown"));
    }

    // --- Item 4 (Important): provider precedence tests for optimize() ---

    use std::sync::Arc;

    use arrow::datatypes::DataType;

    use crate::engine::dictionary::model::{
        DictionaryOwner, DictionarySnapshot, DictionaryState, DictionaryValue, DictionaryWatermark,
    };
    use crate::sql::analysis::{ExprKind, OutputColumn, TypedExpr};
    use crate::sql::catalog::{ColumnDef, ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::rewrite::context::{
        QueryDictionaryProvider, RewriteContext, current_dictionary_provider,
        with_dictionary_provider,
    };
    use crate::sql::optimizer::rewrite::registry::query_rewrite_pipeline;
    use crate::sql::planner::plan::{AggregateCall, AggregateNode, LogicalPlan, ScanNode};

    struct AlwaysSomeProvider;
    impl QueryDictionaryProvider for AlwaysSomeProvider {
        fn load_active_snapshot(
            &self,
            _table: &TableDef,
            _database: &str,
            column_name: &str,
        ) -> Result<Option<DictionarySnapshot>, String> {
            Ok(Some(DictionarySnapshot {
                dictionary_id: 1,
                owner: DictionaryOwner::StarRocksTable {
                    database: "db".to_string(),
                    table: "t".to_string(),
                    db_id: 1,
                    table_id: 2,
                },
                column_id: Some(10),
                column_name: column_name.to_string(),
                data_type: DataType::Utf8,
                version: 1,
                watermark: DictionaryWatermark::Iceberg {
                    snapshot_id: None,
                    schema_id: 0,
                },
                values: vec![DictionaryValue {
                    id: 1,
                    bytes: b"a".to_vec(),
                }],
                null_id: 0,
                state: DictionaryState::Active,
                order_preserving: true,
            }))
        }
    }

    struct AlwaysNoneProvider;
    impl QueryDictionaryProvider for AlwaysNoneProvider {
        fn load_active_snapshot(
            &self,
            _table: &TableDef,
            _database: &str,
            _column_name: &str,
        ) -> Result<Option<DictionarySnapshot>, String> {
            Ok(None)
        }
    }

    /// Build the minimal Aggregate(Scan) shape that the rewrite rule
    /// can act on: GROUP BY on the string column `s`.
    fn agg_over_string_scan() -> LogicalPlan {
        let table = TableDef {
            name: "t".to_string(),
            columns: vec![ColumnDef {
                name: "s".to_string(),
                data_type: DataType::Utf8,
                nullable: false,
                write_default: None,
                logical_type: None,
            }],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::StarRocks {
                db_id: 0,
                table_id: 0,
            },
        };
        let s_col = OutputColumn {
            column_id: ColumnId::UNSET,
            name: "s".to_string(),
            data_type: DataType::Utf8,
            nullable: false,
            is_internal: false,
        };
        let scan = LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table,
            alias: None,
            columns: vec![s_col.clone()],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
            required_output_columns: None,
        });
        let s_ref = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::UNSET,
                qualifier: None,
                column: "s".to_string(),
            },
            data_type: DataType::Utf8,
            nullable: false,
        };
        LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(scan),
            group_by: vec![s_ref],
            aggregates: vec![AggregateCall {
                name: "count".to_string(),
                args: vec![],
                distinct: false,
                result_type: DataType::Int64,
                order_by: vec![],
            }],
            output_columns: vec![
                s_col,
                OutputColumn {
                    column_id: ColumnId::UNSET,
                    name: "cnt".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    is_internal: false,
                },
            ],
            already_pushed: false,
            required_output_columns: None,
        })
    }

    /// Run the query rewrite pipeline against `agg_over_string_scan`
    /// using the same precedence rule that `optimize()` applies.
    fn rewrite_with(parameter: Option<Arc<dyn QueryDictionaryProvider>>) -> LogicalPlan {
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        if let Some(provider) = resolve_dictionary_provider(parameter) {
            ctx.set_dictionary_provider(provider);
        }
        let table_stats = HashMap::new();
        let pipeline = query_rewrite_pipeline(&table_stats);
        pipeline.rewrite(agg_over_string_scan(), &mut ctx).unwrap()
    }

    fn contains_decode(plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::Decode(_) => true,
            LogicalPlan::Aggregate(a) => contains_decode(&a.input),
            LogicalPlan::Filter(f) => contains_decode(&f.input),
            LogicalPlan::Project(p) => contains_decode(&p.input),
            LogicalPlan::Sort(s) => contains_decode(&s.input),
            LogicalPlan::Limit(l) => contains_decode(&l.input),
            _ => false,
        }
    }

    #[test]
    fn optimize_with_none_provider_does_not_rewrite() {
        // Sanity: ensure TLS is unset for this thread.
        assert!(current_dictionary_provider().is_none());
        let rewritten = rewrite_with(None);
        assert!(
            !contains_decode(&rewritten),
            "no provider → no Decode boundary"
        );
        let LogicalPlan::Aggregate(agg) = &rewritten else {
            panic!("expected aggregate root")
        };
        let LogicalPlan::Scan(scan) = &*agg.input else {
            panic!("expected scan child")
        };
        assert!(scan.dict_columns.is_empty());
    }

    #[test]
    fn optimize_with_tls_provider_rewrites() {
        let provider: Arc<dyn QueryDictionaryProvider> = Arc::new(AlwaysSomeProvider);
        let rewritten = with_dictionary_provider(provider, || rewrite_with(None));
        // Plan must contain a Decode boundary above the aggregate.
        assert!(
            matches!(rewritten, LogicalPlan::Decode(_)),
            "TLS provider must drive the rewrite, got {rewritten:?}"
        );
    }

    #[test]
    fn optimize_parameter_overrides_tls() {
        // TLS provider would dict-encode. The explicit parameter is a
        // provider that always returns None — the parameter must win,
        // so the plan is left untouched.
        let tls_provider: Arc<dyn QueryDictionaryProvider> = Arc::new(AlwaysSomeProvider);
        let param_provider: Arc<dyn QueryDictionaryProvider> = Arc::new(AlwaysNoneProvider);
        let rewritten =
            with_dictionary_provider(tls_provider, || rewrite_with(Some(param_provider.clone())));
        assert!(
            !contains_decode(&rewritten),
            "parameter must override TLS — no Decode expected"
        );
        let LogicalPlan::Aggregate(agg) = &rewritten else {
            panic!("expected aggregate root, got {rewritten:?}")
        };
        let LogicalPlan::Scan(scan) = &*agg.input else {
            panic!("expected scan child, got {:?}", *agg.input)
        };
        assert!(scan.dict_columns.is_empty());
    }
}
