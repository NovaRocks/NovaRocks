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
use crate::sql::planner::plan::LogicalPlanNode;

use super::StandaloneState;
use super::query_stats::{QueryStatsPlan, QueryStatsProviders};

/// Upper bound on candidates per query; aligned with the StarRocks default
/// cbo_materialized_view_rewrite_related_mvs_limit = 16.
const MAX_MV_CANDIDATES: usize = 16;

/// Target-table Iceberg property carrying the per-MV query-rewrite staleness
/// tolerance, in seconds. `0`/absent/unparseable = strict (default). Set at
/// CREATE via `PROPERTIES('query_rewrite_max_staleness_sec'='N')` or later via
/// `ALTER MATERIALIZED VIEW ... SET TBLPROPERTIES(...)`.
pub(crate) const MV_QUERY_REWRITE_MAX_STALENESS_SEC_PROP: &str = "query_rewrite_max_staleness_sec";

/// Parse the per-MV staleness tolerance (seconds) from target-table properties.
/// Absent or unparseable => 0 (strict).
fn parse_mv_staleness_property_sec(props: &HashMap<String, String>) -> u64 {
    props
        .get(MV_QUERY_REWRITE_MAX_STALENESS_SEC_PROP)
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(0)
}

/// Resolve the effective tolerance window (seconds): session override wins when
/// set (including an explicit `Some(0)` to force strict); otherwise the MV's own
/// property value. Default (both unset) => 0 = strict.
fn effective_staleness_window_sec(session: Option<u64>, mv_property_sec: u64) -> u64 {
    session.unwrap_or(mv_property_sec)
}

/// Sound bounded-staleness verdict for one base table. `true` iff the base has
/// advanced no more than `window_sec` (measured by snapshot commit-ts gap in ms,
/// compared exactly in ms). `window_sec == 0` is strict (always false here — the
/// caller only reaches this on a stale base). Missing commit timestamps or a
/// negative gap (rollback/anomaly) => false (skip), never a wrong answer.
fn staleness_within_window(
    window_sec: u64,
    pinned_commit_ms: Option<i64>,
    current_commit_ms: Option<i64>,
) -> bool {
    if window_sec == 0 {
        return false;
    }
    let (Some(pinned), Some(current)) = (pinned_commit_ms, current_commit_ms) else {
        return false;
    };
    if current < pinned {
        return false;
    }
    // `current >= pinned` here, so abs_diff is the exact non-negative gap; it
    // also sidesteps any theoretical i64 subtraction overflow.
    current.abs_diff(pinned) <= window_sec.saturating_mul(1000)
}

struct PreparedMvRewriteCandidate {
    mv_name: String,
    mv: SpjgDescriptor,
    mv_scalars: crate::sql::optimizer::scalar::ScalarArena,
    target_database: String,
    target_table: crate::sql::catalog::TableDef,
}

fn supports_current_mv_rewrite_shape(desc: &SpjgDescriptor) -> bool {
    !desc.has_unsupported_multitable_identity()
}

pub(crate) fn prepare_mv_rewrite_candidates(
    state: &Arc<StandaloneState>,
    analyzer_catalog: &dyn CatalogProvider,
    current_database: &str,
    logical: &LogicalPlanNode,
    factory: &mut ColumnRefFactory,
    query_stats: &mut QueryStatsPlan,
) -> Vec<MvRewriteCandidate> {
    let settings = crate::sql::optimizer::options::current_session_optimizer_settings();
    if !settings.mv_rewrite_enabled() {
        return Vec::new();
    }
    match try_prepare(
        state,
        analyzer_catalog,
        current_database,
        logical,
        factory,
        query_stats,
        settings.mv_rewrite_max_staleness_sec,
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
    session_max_staleness_sec: Option<u64>,
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
        match build_candidate(
            state,
            analyzer_catalog,
            current_database,
            &def,
            factory,
            session_max_staleness_sec,
        ) {
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
    session_max_staleness_sec: Option<u64>,
) -> Result<Option<PreparedMvRewriteCandidate>, String> {
    // 2b. Bounded-staleness freshness: every base table's CURRENT snapshot must
    //     equal the pinned snapshot from the last refresh, UNLESS the effective
    //     staleness window (session var, else MV property, else strict 0) covers
    //     the commit-ts gap. Never refreshed -> skip.
    if def.last_refresh_snapshots.is_empty() {
        return Ok(None);
    }
    let base_refs = crate::engine::mv::refresh_io::parse_iceberg_table_refs(&def.base_table_refs)?;
    // Lazily loaded once (only if a base is stale and no session override is set).
    let mut mv_property_window: Option<u64> = None;
    for r in &base_refs {
        let fqn = r.fqn();
        let Some(pinned) = def.last_refresh_snapshots.get(&fqn) else {
            return Ok(None);
        };
        let current = current_snapshot_id(state, r)?;
        if current != Some(*pinned) {
            // Base advanced. Rewrite only if within the effective tolerance window.
            let window = match session_max_staleness_sec {
                Some(w) => w,
                None => {
                    if mv_property_window.is_none() {
                        mv_property_window = Some(load_mv_staleness_property_sec(state, def)?);
                    }
                    mv_property_window.unwrap()
                }
            };
            if window == 0 {
                // Strict: skip before touching base commit timestamps, matching
                // the original strict gate's immediate short-circuit (spec §3.3
                // order). Keeps the strict-default path off the commit-ts read.
                return Ok(None);
            }
            let (pinned_commit, current_commit) = base_commit_ts_pair(state, r, *pinned)?;
            if !staleness_within_window(window, pinned_commit, current_commit) {
                return Ok(None); // beyond window, rollback, or unresolvable
            }
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
    if !supports_current_mv_rewrite_shape(&mv_desc) {
        return Ok(None);
    }

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
        mv_name: rewrite_candidate_display_name(tbl),
        mv: mv_desc,
        mv_scalars,
        target_database: ns.clone(),
        target_table,
    }))
}

fn rewrite_candidate_display_name(target_table: &str) -> String {
    target_table.to_string()
}

/// Recursively collect "cat.ns.tbl" FQNs of every Iceberg data-file scan in
/// the plan. Mirrors query-stats collector scan-source coverage.
fn collect_iceberg_fqns(plan: &LogicalPlanNode, out: &mut Vec<String>) {
    match &plan.kind {
        crate::sql::planner::plan::LogicalPlanKind::Scan(s) => {
            if let ScanSource::IcebergDataFiles { table, .. } = &s.table.source {
                let fqn = format!("{}.{}.{}", table.catalog, table.namespace, table.table);
                if !out.contains(&fqn) {
                    out.push(fqn);
                }
            }
        }
        crate::sql::planner::plan::LogicalPlanKind::ImvDelta(_)
        | crate::sql::planner::plan::LogicalPlanKind::ImvVersion(_) => {
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
    r: &crate::engine::mv::table_ref::IcebergTableRef,
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
    r: &crate::engine::mv::table_ref::IcebergTableRef,
) -> Result<Option<String>, String> {
    let registry = state
        .iceberg_catalogs
        .read()
        .expect("iceberg catalogs read lock");
    let entry = registry.get(&r.catalog)?;
    let loaded = crate::connector::iceberg::catalog::load_table(&entry, &r.namespace, &r.table)?;
    Ok(Some(loaded.table.metadata().uuid().to_string()))
}

/// Commit timestamps (ms) of the `pinned` snapshot and the current snapshot of a
/// base table, read through the query's shared catalog view (same non-invalidating
/// contract as `current_snapshot_id`). Either element is `None` if that snapshot is
/// not resolvable (e.g. pinned expired, or no current snapshot). One catalog load.
fn base_commit_ts_pair(
    state: &Arc<StandaloneState>,
    r: &crate::engine::mv::table_ref::IcebergTableRef,
    pinned: i64,
) -> Result<(Option<i64>, Option<i64>), String> {
    let registry = state
        .iceberg_catalogs
        .read()
        .expect("iceberg catalogs read lock");
    let entry = registry.get(&r.catalog)?;
    let loaded = crate::connector::iceberg::catalog::load_table(&entry, &r.namespace, &r.table)?;
    let metadata = loaded.table.metadata();
    let pinned_ms = metadata.snapshot_by_id(pinned).map(|s| s.timestamp_ms());
    let current_ms = metadata.current_snapshot().map(|s| s.timestamp_ms());
    Ok((pinned_ms, current_ms))
}

/// Load the target table's `query_rewrite_max_staleness_sec` property (seconds),
/// through the shared catalog view. Missing target or property => 0 (strict).
fn load_mv_staleness_property_sec(
    state: &Arc<StandaloneState>,
    def: &crate::meta::repository::mv::StoredMvDefinition,
) -> Result<u64, String> {
    let (Some(cat), Some(ns), Some(tbl)) = (
        &def.target_catalog,
        &def.target_namespace,
        &def.target_table,
    ) else {
        return Ok(0);
    };
    let registry = state
        .iceberg_catalogs
        .read()
        .expect("iceberg catalogs read lock");
    let entry = registry.get(cat)?;
    let loaded = crate::connector::iceberg::catalog::load_table(&entry, ns, tbl)?;
    Ok(parse_mv_staleness_property_sec(
        loaded.table.metadata().properties(),
    ))
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::*;
    use crate::sql::column_id::ColumnId;
    use crate::sql::common::OutputColumn;
    use crate::sql::optimizer::cascades_rules::mv_rewrite::descriptor::{
        EquiEdge, JoinInput, JoinShape,
    };

    fn table(name: &str) -> crate::sql::catalog::TableDef {
        crate::sql::catalog::TableDef {
            name: name.to_string(),
            columns: Vec::new(),
            iceberg_row_lineage_metadata_columns: Vec::new(),
            source: ScanSource::StarRocks {
                db_id: 0,
                table_id: 0,
            },
        }
    }

    fn output_column(id: u32, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId(id),
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: true,
            is_internal: false,
        }
    }

    fn descriptor_with_joins(joins: Option<JoinShape>) -> SpjgDescriptor {
        SpjgDescriptor {
            table: table("t"),
            scan_columns: Vec::new(),
            predicates: Vec::new(),
            aggregate: None,
            outputs: Vec::new(),
            joins,
        }
    }

    #[test]
    fn current_mv_rewrite_shape_support_accepts_single_table_descriptor() {
        let desc = descriptor_with_joins(None);

        assert!(supports_current_mv_rewrite_shape(&desc));
    }

    #[test]
    fn current_mv_rewrite_shape_support_rejects_join_descriptor() {
        // `table("t2")` is non-Iceberg (StarRocks source) -- rejected because
        // its identity cannot be verified, not because it is multi-table.
        // A well-formed Iceberg multi-table shape IS accepted after E1-bc:
        // see `current_mv_rewrite_shape_support_accepts_well_formed_multitable_descriptor`.
        let desc = descriptor_with_joins(Some(JoinShape {
            inputs: vec![JoinInput {
                table: table("t2"),
                scan_columns: vec![output_column(2, "c")],
            }],
            equi_edges: vec![EquiEdge {
                left: ColumnId(1),
                right: ColumnId(2),
            }],
        }));

        assert!(!supports_current_mv_rewrite_shape(&desc));
    }

    /// An Iceberg-sourced `TableDef` for the well-formed-multitable-gate
    /// tests below. `table()` (used by the sibling tests above) is
    /// deliberately non-Iceberg (`ScanSource::StarRocks`), which is why
    /// `current_mv_rewrite_shape_support_rejects_join_descriptor` rejects
    /// -- these two new tests need a genuinely Iceberg-identified pair to
    /// exercise the "well-formed" accept path and the self-join reject path.
    fn iceberg_table_for_test(
        catalog: &str,
        ns: &str,
        name: &str,
    ) -> crate::sql::catalog::TableDef {
        crate::sql::catalog::TableDef {
            name: name.to_string(),
            columns: Vec::new(),
            iceberg_row_lineage_metadata_columns: Vec::new(),
            source: ScanSource::IcebergDataFiles {
                table: crate::sql::catalog::IcebergTableInfo {
                    catalog: catalog.to_string(),
                    namespace: ns.to_string(),
                    table: name.to_string(),
                    table_uuid: None,
                    current_snapshot_id: None,
                    schema_id: 0,
                    location: String::new(),
                    schema: crate::sql::catalog::IcebergSchemaDef { fields: vec![] },
                    serialized_metadata: None,
                    serialized_metadata_rows: None,
                },
                files: vec![],
                cloud_properties: Default::default(),
                binding: crate::sql::catalog::IcebergDataFileBinding::CurrentSnapshot,
            },
        }
    }

    #[test]
    fn current_mv_rewrite_shape_support_accepts_well_formed_multitable_descriptor() {
        let mut desc = descriptor_with_joins(Some(JoinShape {
            inputs: vec![JoinInput {
                table: iceberg_table_for_test("cat", "ns", "t2"),
                scan_columns: vec![output_column(2, "c")],
            }],
            equi_edges: vec![EquiEdge {
                left: ColumnId(1),
                right: ColumnId(2),
            }],
        }));
        desc.table = iceberg_table_for_test("cat", "ns", "t1"); // distinct from "t2"

        assert!(supports_current_mv_rewrite_shape(&desc));
    }

    #[test]
    fn current_mv_rewrite_shape_support_rejects_self_join_descriptor() {
        let mut desc = descriptor_with_joins(Some(JoinShape {
            inputs: vec![JoinInput {
                table: iceberg_table_for_test("cat", "ns", "t1"), // SAME fqn as driving
                scan_columns: vec![output_column(2, "a")],
            }],
            equi_edges: vec![EquiEdge {
                left: ColumnId(1),
                right: ColumnId(2),
            }],
        }));
        desc.table = iceberg_table_for_test("cat", "ns", "t1");

        assert!(!supports_current_mv_rewrite_shape(&desc));
    }

    #[test]
    fn rewrite_candidate_display_name_uses_target_table_name_directly() {
        assert_eq!(rewrite_candidate_display_name("agg_mv"), "agg_mv");
        assert_eq!(
            rewrite_candidate_display_name("target_agg_mv"),
            "target_agg_mv"
        );
    }

    #[test]
    fn parse_mv_staleness_property_reads_seconds_or_zero() {
        use std::collections::HashMap;
        let mut p = HashMap::new();
        assert_eq!(
            super::parse_mv_staleness_property_sec(&p),
            0,
            "absent -> strict"
        );
        p.insert(
            "query_rewrite_max_staleness_sec".to_string(),
            "300".to_string(),
        );
        assert_eq!(super::parse_mv_staleness_property_sec(&p), 300);
        p.insert(
            "query_rewrite_max_staleness_sec".to_string(),
            "  90 ".to_string(),
        );
        assert_eq!(super::parse_mv_staleness_property_sec(&p), 90, "trims");
        p.insert(
            "query_rewrite_max_staleness_sec".to_string(),
            "bad".to_string(),
        );
        assert_eq!(
            super::parse_mv_staleness_property_sec(&p),
            0,
            "unparseable -> strict"
        );
    }

    #[test]
    fn effective_window_prefers_session_over_property() {
        assert_eq!(
            super::effective_staleness_window_sec(Some(10), 300),
            10,
            "session wins"
        );
        assert_eq!(
            super::effective_staleness_window_sec(Some(0), 300),
            0,
            "session 0 forces strict"
        );
        assert_eq!(
            super::effective_staleness_window_sec(None, 300),
            300,
            "fall back to property"
        );
        assert_eq!(
            super::effective_staleness_window_sec(None, 0),
            0,
            "default strict"
        );
    }

    #[test]
    fn staleness_within_window_boundary_and_anomalies() {
        // window 0 => always skip (strict), even with zero gap.
        assert!(!super::staleness_within_window(0, Some(1_000), Some(1_000)));
        // exact boundary in ms: 300s window, gap 300_000ms -> within.
        assert!(super::staleness_within_window(
            300,
            Some(1_000_000),
            Some(1_300_000)
        ));
        // one ms over -> skip.
        assert!(!super::staleness_within_window(
            300,
            Some(1_000_000),
            Some(1_300_001)
        ));
        // negative gap (rollback) -> skip.
        assert!(!super::staleness_within_window(
            300,
            Some(2_000_000),
            Some(1_000_000)
        ));
        // missing commit ts -> skip.
        assert!(!super::staleness_within_window(300, None, Some(1_000_000)));
        assert!(!super::staleness_within_window(300, Some(1_000_000), None));
    }
}
