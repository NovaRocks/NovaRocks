//! MV rewrite candidate preparation (engine side).
//!
//! Runs after plan_query and before optimize(): discovers fresh Iceberg MVs
//! related to the query's base tables, re-analyzes their defining SQL with
//! the query's ColumnRefFactory, validates the SPJG shape, builds the
//! executable target TableDef, and loads target-table statistics.
//! Every failure is a warn-and-skip: rewrite is an optional optimization.

use std::collections::HashMap;
use std::sync::Arc;

use crate::sql::catalog::{CatalogProvider, ScanSource};
use crate::sql::column_id::ColumnRefFactory;
use crate::sql::optimizer::cascades_rules::mv_rewrite::{
    MvRewriteCandidate, descriptor::SpjgDescriptor,
};
use crate::sql::optimizer::statistics::TableStatistics;
use crate::sql::planner::plan::LogicalPlanNode;

use super::StandaloneState;

/// Upper bound on candidates per query; aligned with the StarRocks default
/// cbo_materialized_view_rewrite_related_mvs_limit = 16.
const MAX_MV_CANDIDATES: usize = 16;

pub(crate) fn prepare_mv_rewrite_candidates(
    state: &Arc<StandaloneState>,
    analyzer_catalog: &dyn CatalogProvider,
    current_database: &str,
    logical: &LogicalPlanNode,
    factory: &mut ColumnRefFactory,
    table_stats: &mut HashMap<String, TableStatistics>,
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
        table_stats,
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
    table_stats: &mut HashMap<String, TableStatistics>,
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
                // 5. Inject target-table statistics (bare lowercase name key;
                //    see collect_scan_stats insert / derive_scan_statistics
                //    lookup). A name collision with a query table makes stats
                //    ambiguous: drop the candidate (spec §5.5).
                let key = c.target_table.name.to_ascii_lowercase();
                if table_stats.contains_key(&key) {
                    tracing::warn!(
                        "mv rewrite: target table name {key} collides with a query table; skipping {}",
                        c.mv_name
                    );
                    continue;
                }
                if let Some(ts) = load_target_stats(state, &c.target_table) {
                    table_stats.insert(key, ts);
                }
                candidates.push(c);
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
) -> Result<Option<MvRewriteCandidate>, String> {
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
    let mv_desc = SpjgDescriptor::from_logical_plan(&mv_logical)?;

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

    // 4. Build the executable target TableDef via the iceberg connector pair
    //    (same mechanism as register_external_table_by_name; no global
    //    registration needed — ScanOp embeds the TableDef).
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

    Ok(Some(MvRewriteCandidate {
        mv_name: tbl.clone(),
        mv: mv_desc,
        target_database: ns.clone(),
        target_table,
    }))
}

/// Recursively collect "cat.ns.tbl" FQNs of every Iceberg data-file scan in
/// the plan. Mirrors `collect_scan_stats` (engine/mod.rs) node coverage.
fn collect_iceberg_fqns(plan: &LogicalPlanNode, out: &mut Vec<String>) {
    match &plan.kind {
        crate::sql::planner::plan::LogicalPlanNodeKind::Scan(s) => {
            if let ScanSource::IcebergDataFiles { table, .. } = &s.table.source {
                let fqn = format!("{}.{}.{}", table.catalog, table.namespace, table.table);
                if !out.contains(&fqn) {
                    out.push(fqn);
                }
            }
        }
        crate::sql::planner::plan::LogicalPlanNodeKind::ImvDelta(_)
        | crate::sql::planner::plan::LogicalPlanNodeKind::ImvVersion(_) => {
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

/// Best-effort target-table statistics for the rewritten scan, mirroring
/// `collect_scan_stats`.
///
/// The target TableDef is built schema-only (CurrentSnapshot binding, empty
/// `files`), so reading its `files` vector directly always yields `None` and
/// the CBO falls back to a default row count. With a small base table that
/// default is *larger* than the real MV cardinality, so the MV alternative
/// loses on cost and the rewrite never fires (plan §"已知风险" #6). To cost
/// the alternative correctly we enumerate the target table's CURRENT-snapshot
/// data files here — the same enumeration the ANALYZE/scan path uses
/// (`extract_data_files_with_stats` -> `data_file_with_stats_to_iceberg_data_file_info`)
/// — and build stats from those real files plus Puffin NDV.
///
/// This is costing metadata only: the injected scan still resolves files at
/// execution time from its CurrentSnapshot binding, so the stats snapshot used
/// here never affects result correctness. Fail-closed: any registry/IO error
/// yields `None` (CBO fallback), never a panic.
fn load_target_stats(
    state: &Arc<StandaloneState>,
    table_def: &crate::sql::catalog::TableDef,
) -> Option<TableStatistics> {
    let ScanSource::IcebergDataFiles {
        table,
        cloud_properties,
        ..
    } = &table_def.source
    else {
        return None;
    };

    let files = match current_snapshot_data_files(state, table) {
        Ok(files) => files,
        Err(e) => {
            tracing::debug!(
                "mv rewrite: failed to enumerate current-snapshot files for target table {}: {e}; using CBO fallback",
                table_def.name
            );
            return None;
        }
    };

    let (ndv_by_name, name_to_field_id) =
        super::load_iceberg_puffin_ndv(Some(table), cloud_properties);
    let stats = crate::sql::optimizer::statistics::build_table_statistics_with_ndv(
        &files,
        &table_def.columns,
        &ndv_by_name,
        &name_to_field_id,
    );
    if stats.is_none() {
        tracing::debug!(
            "mv rewrite: no derivable stats for target table {} ({} current-snapshot files); using CBO fallback",
            table_def.name,
            files.len()
        );
    }
    stats
}

/// Enumerate the CURRENT-snapshot data files of an Iceberg target table as
/// `IcebergDataFileInfo`, reusing the catalog registry view (no cache
/// invalidation, matching `current_snapshot_id`'s consistency contract).
fn current_snapshot_data_files(
    state: &Arc<StandaloneState>,
    table: &crate::sql::catalog::IcebergTableInfo,
) -> Result<Vec<crate::sql::catalog::IcebergDataFileInfo>, String> {
    let registry = state
        .iceberg_catalogs
        .read()
        .expect("iceberg catalogs read lock");
    let entry = registry.get(&table.catalog)?;
    let loaded =
        crate::connector::iceberg::catalog::load_table(&entry, &table.namespace, &table.table)?;
    let data_files =
        crate::connector::iceberg::catalog::registry::extract_data_files_with_stats(&loaded.table)?;
    Ok(data_files
        .into_iter()
        .map(crate::connector::iceberg::catalog::backend::data_file_with_stats_to_iceberg_data_file_info)
        .collect())
}
