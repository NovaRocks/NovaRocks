//! MV rewrite candidate preparation (engine side).
//!
//! Runs after plan_query and before optimize(): discovers fresh Iceberg MVs
//! related to the query's base tables, re-analyzes their defining SQL with
//! the query's ColumnRefFactory, validates the SPJG shape, builds the
//! executable target TableDef, and loads target-table statistics.
//! Every failure is a warn-and-skip: rewrite is an optional optimization.

use std::sync::Arc;

use crate::sql::catalog::{CatalogProvider, ScanSource};
use crate::sql::column_id::ColumnRefFactory;
use crate::sql::optimizer::cascades_rules::mv_rewrite::{
    MvRewriteCandidate, descriptor::SpjgDescriptor,
};
use crate::sql::planner::plan::LogicalPlanNode;

use super::StandaloneState;
use super::query_stats::{QueryStatsPlan, QueryStatsProviders};

/// Upper bound on candidates per query; aligned with the StarRocks default
/// cbo_materialized_view_rewrite_related_mvs_limit = 16.
const MAX_MV_CANDIDATES: usize = 16;

struct PreparedMvRewriteCandidate {
    mv_name: String,
    mv: SpjgDescriptor,
    mv_scalars: crate::sql::optimizer::scalar::ScalarArena,
    target_database: String,
    target_table: crate::sql::catalog::TableDef,
}

pub(crate) fn prepare_mv_rewrite_candidates(
    state: &Arc<StandaloneState>,
    analyzer_catalog: &dyn CatalogProvider,
    current_database: &str,
    logical: &LogicalPlanNode,
    factory: &mut ColumnRefFactory,
    query_stats: &mut QueryStatsPlan,
) -> Vec<MvRewriteCandidate> {
    if !crate::sql::optimizer::options::current_session_optimizer_settings().mv_rewrite_enabled() {
        return Vec::new();
    }
    match try_prepare(
        state,
        analyzer_catalog,
        current_database,
        logical,
        factory,
        query_stats,
    ) {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!("mv rewrite candidate preparation failed: {e}");
            Vec::new()
        }
    }
}

fn try_prepare(
    state: &Arc<StandaloneState>,
    analyzer_catalog: &dyn CatalogProvider,
    current_database: &str,
    logical: &LogicalPlanNode,
    factory: &mut ColumnRefFactory,
    query_stats: &mut QueryStatsPlan,
) -> Result<Vec<MvRewriteCandidate>, String> {
    // 1. Iceberg base tables referenced by the query, as "cat.ns.tbl" FQNs
    //    (the exact format of StoredMvDefinition.base_table_refs, produced
    //    by IcebergTableRef::fqn at MV creation).
    let mut query_fqns: Vec<String> = Vec::new();
    collect_iceberg_fqns(logical, &mut query_fqns);
    if query_fqns.is_empty() {
        return Ok(Vec::new());
    }

    // 2. List stored MVs.
    let Some(provider) = state.metadata_provider.as_ref() else {
        return Ok(Vec::new());
    };
    let read = provider
        .begin_read()
        .map_err(|e| format!("mv metadata read txn: {e}"))?;
    let definitions = state
        .mv_repo
        .list_definitions(read.as_ref())
        .map_err(|e| format!("list mv definitions: {e}"))?;

    let stats_providers = QueryStatsProviders::from_standalone_state(state);
    let mut candidates = Vec::new();
    for def in definitions {
        if candidates.len() >= MAX_MV_CANDIDATES {
            tracing::warn!("mv rewrite: candidate cap {MAX_MV_CANDIDATES} reached, rest skipped");
            break;
        }
        // Storage filter only. In-flight refresh does NOT disqualify a
        // candidate: pins always point at committed snapshots.
        if def.storage_engine != "iceberg" {
            continue;
        }
        if !def.base_table_refs.iter().any(|r| query_fqns.contains(r)) {
            continue;
        }
        match build_candidate(state, analyzer_catalog, current_database, &def, factory) {
            Ok(Some(c)) => {
                let (target_label, target_stats) = super::query_stats::collect_table_stats(
                    &stats_providers,
                    &c.target_database,
                    &c.target_table,
                );
                let target_stats_ref = query_stats.add_stats(target_label, target_stats);
                candidates.push(MvRewriteCandidate {
                    mv_name: c.mv_name,
                    mv: c.mv,
                    mv_scalars: c.mv_scalars,
                    target_database: c.target_database,
                    target_table: c.target_table,
                    target_stats_ref,
                });
            }
            Ok(None) => {}
            Err(e) => tracing::warn!("mv rewrite: skipping mv {}: {e}", def.mv_id),
        }
    }
    Ok(candidates)
}

fn build_candidate(
    state: &Arc<StandaloneState>,
    analyzer_catalog: &dyn CatalogProvider,
    current_database: &str,
    def: &crate::meta::repository::mv::StoredMvDefinition,
    factory: &mut ColumnRefFactory,
) -> Result<Option<PreparedMvRewriteCandidate>, String> {
    // 2b. Strict freshness: every base table's CURRENT snapshot must equal
    //     the pinned snapshot from the last refresh. Never refreshed -> skip.
    if def.last_refresh_snapshots.is_empty() {
        return Ok(None);
    }
    let base_refs = crate::connector::starrocks::table::mv_refresh::parse_iceberg_table_refs(
        &def.base_table_refs,
    )?;
    for r in &base_refs {
        let fqn = r.fqn();
        let Some(pinned) = def.last_refresh_snapshots.get(&fqn) else {
            return Ok(None);
        };
        let current = current_snapshot_id(state, r)?;
        if current != Some(*pinned) {
            return Ok(None); // stale (or unreadable) -> strict mode skips
        }
        if let Some(pinned_uuid) = def.last_refresh_table_uuids.get(&fqn)
            && current_table_uuid(state, r)?.as_deref() != Some(pinned_uuid.as_str())
        {
            // table was dropped & recreated
            return Ok(None);
        }
    }

    // 3. Re-analyze the defining SQL on a CLONE of the query's factory, then
    //    adopt the advanced factory only on success. A parse/plan failure here
    //    is an expected warn-and-skip (design §9: "MV SQL parse failure"), and
    //    it MUST be side-effect-free: an earlier version used
    //    `std::mem::take(factory)` and only wrote the factory back on success,
    //    so any `?` left the caller's `*factory` as a fresh Default (next_id =
    //    1). That reset factory then flowed into `optimize()`, whose RBO
    //    column-pruning auto-fill (and the MvRewrite rule) mint ColumnIds from
    //    it — colliding with the query's existing columns and corrupting even
    //    the non-rewritten plan. Cloning keeps `*factory` untouched until we
    //    have a fully analyzed+planned MV; on success the write-back threads
    //    the advanced ids so the query and every candidate stay collision-free.
    let select = crate::engine::mv::iceberg_refresh::parse_mv_select_query(&def.select_sql)?;
    let (resolved, ctes, returned) = crate::sql::analyzer::analyze_with_factory(
        &select,
        analyzer_catalog,
        current_database,
        factory.clone(),
    )?;
    let mut returned = returned;
    let mv_logical = crate::sql::planner::plan_query(resolved, ctes, &mut returned)?;
    *factory = returned;
    let mut mv_scalars = crate::sql::optimizer::scalar::ScalarArena::new();
    let mv_opt_expr = crate::sql::planner::optimizer_bridge::plan::try_logical_plan_to_opt_expr(
        &mv_logical,
        &mut mv_scalars,
    )?;
    let mv_desc = SpjgDescriptor::from_opt_expr(&mv_opt_expr, &mut mv_scalars)?;

    // 3b. Fail closed on name-resolution drift: the analyzed scan must be
    //     one of the recorded base tables.
    let ScanSource::IcebergDataFiles { table, .. } = &mv_desc.table.source else {
        return Ok(None);
    };
    let scan_fqn = format!("{}.{}.{}", table.catalog, table.namespace, table.table);
    if !def.base_table_refs.contains(&scan_fqn) {
        return Err(format!(
            "mv select resolved to {scan_fqn}, not in recorded base refs"
        ));
    }

    // 4. Build the executable target TableDef via the iceberg connector pair.
    //    This does not materialize local catalog state; ScanOp embeds the
    //    TableDef directly.
    let (Some(cat), Some(ns), Some(tbl)) = (
        &def.target_catalog,
        &def.target_namespace,
        &def.target_table,
    ) else {
        return Ok(None);
    };
    let (catalog_backend, table_source) = {
        let registry = state
            .connectors
            .read()
            .expect("standalone connector registry read lock");
        (
            registry.catalog_backend("iceberg")?,
            registry.table_source("iceberg")?,
        )
    };
    let resolved_tbl = catalog_backend.load_table(cat, ns, tbl)?;
    let target_table = table_source.build_schema_table_def(&resolved_tbl)?;

    // Duplicate output names break the by-name visible-column mapping.
    let mut names: Vec<&str> = mv_desc.outputs.iter().map(|o| o.name.as_str()).collect();
    names.sort_unstable();
    if names.windows(2).any(|w| w[0] == w[1]) {
        return Ok(None);
    }

    Ok(Some(PreparedMvRewriteCandidate {
        mv_name: tbl.clone(),
        mv: mv_desc,
        mv_scalars,
        target_database: ns.clone(),
        target_table,
    }))
}

/// Recursively collect "cat.ns.tbl" FQNs of every Iceberg data-file scan in
/// the plan. Mirrors query-stats collector scan-source coverage.
fn collect_iceberg_fqns(plan: &LogicalPlanNode, out: &mut Vec<String>) {
    match &plan.kind {
        crate::sql::planner::plan::PlanNodeKind::Scan(s) => {
            if let ScanSource::IcebergDataFiles { table, .. } = &s.table.source {
                let fqn = format!("{}.{}.{}", table.catalog, table.namespace, table.table);
                if !out.contains(&fqn) {
                    out.push(fqn);
                }
            }
        }
        crate::sql::planner::plan::PlanNodeKind::ImvDelta(_)
        | crate::sql::planner::plan::PlanNodeKind::ImvVersion(_) => {
            // IMV markers never appear on the standalone query path; ignore.
        }
        _ => {}
    }
    for child in &plan.children {
        collect_iceberg_fqns(child, out);
    }
}

/// Current snapshot id of a base table, read through the query's shared
/// catalog cache view.
///
/// Unlike the test helper at engine/mod.rs:~7009 this deliberately does NOT
/// call `entry.invalidate_table_cache(...)`: the freshness check must observe
/// the same snapshot the query's own scan resolution will use. Forcing a
/// disk re-read here could see a newer snapshot than the one the query binds,
/// which would make a candidate look stale (or fresh) inconsistently with the
/// plan being optimized.
fn current_snapshot_id(
    state: &Arc<StandaloneState>,
    r: &crate::connector::starrocks::table::model::IcebergTableRef,
) -> Result<Option<i64>, String> {
    let registry = state
        .iceberg_catalogs
        .read()
        .expect("iceberg catalogs read lock");
    let entry = registry.get(&r.catalog)?;
    let loaded = crate::connector::iceberg::catalog::load_table(&entry, &r.namespace, &r.table)?;
    Ok(loaded
        .table
        .metadata()
        .current_snapshot()
        .map(|s| s.snapshot_id()))
}

/// Current table UUID of a base table, read through the same shared cache
/// view as `current_snapshot_id` (no cache invalidation; see its docs).
fn current_table_uuid(
    state: &Arc<StandaloneState>,
    r: &crate::connector::starrocks::table::model::IcebergTableRef,
) -> Result<Option<String>, String> {
    let registry = state
        .iceberg_catalogs
        .read()
        .expect("iceberg catalogs read lock");
    let entry = registry.get(&r.catalog)?;
    let loaded = crate::connector::iceberg::catalog::load_table(&entry, &r.namespace, &r.table)?;
    Ok(Some(loaded.table.metadata().uuid().to_string()))
}
