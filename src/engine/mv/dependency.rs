use std::sync::Arc;

use crate::connector::starrocks::managed::model::IcebergTableRef;
use crate::connector::starrocks::managed::mv_ddl::ResolvedTableRef;
use crate::engine::StandaloneState;
use crate::meta::repository::mv::{
    CreateMvDependencyRequest, MvDependencyObjectRef, MvDependencyObjectType,
    MvDependencyStorageEngine, StoredMvDefinition,
};

pub(crate) struct ResolvedCreateMvDependencies {
    pub(crate) base_refs: Vec<IcebergTableRef>,
    pub(crate) dependencies: Vec<CreateMvDependencyRequest>,
}

pub(crate) fn iceberg_table_dependency_ref(base: &IcebergTableRef) -> MvDependencyObjectRef {
    MvDependencyObjectRef {
        catalog: Some(base.catalog.clone()),
        database_or_namespace: base.namespace.clone(),
        name: base.table.clone(),
        object_type: MvDependencyObjectType::Table,
        storage_engine: MvDependencyStorageEngine::Iceberg,
    }
}

pub(crate) fn iceberg_mv_dependency_ref(
    catalog: &str,
    namespace: &str,
    table: &str,
) -> MvDependencyObjectRef {
    MvDependencyObjectRef {
        catalog: Some(catalog.to_string()),
        database_or_namespace: namespace.to_string(),
        name: table.to_string(),
        object_type: MvDependencyObjectType::MaterializedView,
        storage_engine: MvDependencyStorageEngine::Iceberg,
    }
}

pub(crate) fn managed_mv_dependency_ref(database: &str, table: &str) -> MvDependencyObjectRef {
    MvDependencyObjectRef {
        catalog: None,
        database_or_namespace: database.to_string(),
        name: table.to_string(),
        object_type: MvDependencyObjectType::MaterializedView,
        storage_engine: MvDependencyStorageEngine::StarRocks,
    }
}

pub(crate) fn iceberg_table_object_ref(
    catalog: &str,
    namespace: &str,
    table: &str,
) -> MvDependencyObjectRef {
    MvDependencyObjectRef {
        catalog: Some(catalog.to_string()),
        database_or_namespace: namespace.to_string(),
        name: table.to_string(),
        object_type: MvDependencyObjectType::Table,
        storage_engine: MvDependencyStorageEngine::Iceberg,
    }
}

pub(crate) fn managed_table_object_ref(database: &str, table: &str) -> MvDependencyObjectRef {
    MvDependencyObjectRef {
        catalog: None,
        database_or_namespace: database.to_string(),
        name: table.to_string(),
        object_type: MvDependencyObjectType::Table,
        storage_engine: MvDependencyStorageEngine::StarRocks,
    }
}

pub(crate) fn ensure_no_downstream_dependencies(
    state: &Arc<StandaloneState>,
    upstream: &MvDependencyObjectRef,
) -> Result<(), String> {
    let Some(provider) = state.metadata_provider.as_ref() else {
        return Ok(());
    };
    let read = provider
        .begin_read()
        .map_err(|e| format!("open MV dependency drop guard read failed: {e}"))?;
    state
        .mv_repo
        .ensure_no_downstream_dependencies(read.as_ref(), upstream)
        .map_err(|e| e.to_string())
}

pub(crate) fn stored_definition_dependency_ref(
    definition: &StoredMvDefinition,
    managed_name: Option<(&str, &str)>,
) -> Result<MvDependencyObjectRef, String> {
    if definition.storage_engine.eq_ignore_ascii_case("iceberg") {
        let catalog = definition
            .target_catalog
            .as_deref()
            .ok_or_else(|| "iceberg MV definition missing target catalog".to_string())?;
        let namespace = definition
            .target_namespace
            .as_deref()
            .ok_or_else(|| "iceberg MV definition missing target namespace".to_string())?;
        let table = definition
            .target_table
            .as_deref()
            .ok_or_else(|| "iceberg MV definition missing target table".to_string())?;
        return Ok(iceberg_mv_dependency_ref(catalog, namespace, table));
    }
    let (database, table) = managed_name.ok_or_else(|| {
        "managed-lake MV definition requires database/table name for dependency ref".to_string()
    })?;
    Ok(managed_mv_dependency_ref(database, table))
}

pub(crate) fn resolve_create_mv_dependencies(
    state: &Arc<StandaloneState>,
    resolved_refs: &[ResolvedTableRef],
    created_at_ms: i64,
) -> Result<ResolvedCreateMvDependencies, String> {
    let provider = state.metadata_provider.as_ref().ok_or_else(|| {
        "materialized view dependency resolution requires metadata provider".to_string()
    })?;
    let read = provider
        .begin_read()
        .map_err(|e| format!("open MV dependency metadata read transaction failed: {e}"))?;

    let mut base_refs = Vec::new();
    let mut dependencies = Vec::new();
    for table_ref in resolved_refs {
        match table_ref {
            ResolvedTableRef::Iceberg {
                catalog,
                namespace,
                table,
            } => {
                let base = IcebergTableRef {
                    catalog: catalog.clone(),
                    namespace: namespace.clone(),
                    table: table.clone(),
                };
                if !base_refs.contains(&base) {
                    base_refs.push(base.clone());
                }
                let upstream = if state
                    .mv_repo
                    .find_by_target(read.as_ref(), catalog, namespace, table)
                    .map_err(|e| format!("load MV target dependency failed: {e}"))?
                    .is_some()
                {
                    iceberg_mv_dependency_ref(catalog, namespace, table)
                } else {
                    iceberg_table_dependency_ref(&base)
                };
                dependencies.push(CreateMvDependencyRequest {
                    upstream,
                    created_at_ms,
                });
            }
            ResolvedTableRef::StarRocks { database, table } => {
                let managed = state
                    .managed_lake
                    .read()
                    .expect("standalone managed lake read lock");
                let runtime = managed.table(database, table).map_err(|err| {
                    format!("resolve managed-lake MV dependency {database}.{table} failed: {err}")
                })?;
                if runtime.table.kind
                    != crate::connector::starrocks::managed::model::ManagedTableKind::MaterializedView
                {
                    return Err(format!(
                        "materialized view base tables must be Iceberg tables or materialized views; found managed lake table `{database}.{table}`"
                    ));
                }
                return Err(format!(
                    "managed-lake MV-on-MV dependency `{database}.{table}` is recognized but cannot be used as an incremental Iceberg base in this release"
                ));
            }
        }
    }
    if base_refs.is_empty() {
        return Err("materialized view base tables must be Iceberg tables".to_string());
    }
    Ok(ResolvedCreateMvDependencies {
        base_refs,
        dependencies,
    })
}

fn object_in_iceberg_scope(
    object: &MvDependencyObjectRef,
    scope_catalog: &str,
    scope_namespace: Option<&str>,
) -> bool {
    if object.storage_engine != MvDependencyStorageEngine::Iceberg {
        return false;
    }
    let Some(obj_catalog) = object.catalog.as_deref() else {
        return false;
    };
    if !obj_catalog.eq_ignore_ascii_case(scope_catalog) {
        return false;
    }
    if let Some(ns) = scope_namespace
        && !object.database_or_namespace.eq_ignore_ascii_case(ns)
    {
        return false;
    }
    true
}

/// Pure orphan-prevention check: given the full set of MV targets and their
/// upstream dependencies, reject the scope drop if any MV outside the scope
/// depends on an upstream inside the scope.
pub(crate) fn validate_no_external_dependents_for_scope(
    scope_catalog: &str,
    scope_namespace: Option<&str>,
    definitions_with_deps: &[(MvDependencyObjectRef, Vec<MvDependencyObjectRef>)],
) -> Result<(), String> {
    let mut external_dependents: Vec<String> = Vec::new();
    for (target, upstreams) in definitions_with_deps {
        let target_in_scope = object_in_iceberg_scope(target, scope_catalog, scope_namespace);
        if target_in_scope {
            continue;
        }
        for upstream in upstreams {
            if object_in_iceberg_scope(upstream, scope_catalog, scope_namespace) {
                external_dependents.push(format!(
                    "{} depends on {}",
                    target.display_name(),
                    upstream.display_name(),
                ));
                break;
            }
        }
    }

    if external_dependents.is_empty() {
        return Ok(());
    }
    external_dependents.sort();
    let scope_str = match scope_namespace {
        Some(ns) => format!("`{scope_catalog}.{ns}`"),
        None => format!("`{scope_catalog}`"),
    };
    Err(format!(
        "cannot drop {scope_str}: would orphan downstream materialized views: {}",
        external_dependents.join(", ")
    ))
}

/// State-aware wrapper around `validate_no_external_dependents_for_scope`:
/// loads MV definitions and their upstream dependencies from the repository,
/// then delegates to the pure helper.
pub(crate) fn ensure_no_external_iceberg_dependents(
    state: &Arc<StandaloneState>,
    scope_catalog: &str,
    scope_namespace: Option<&str>,
) -> Result<(), String> {
    let Some(provider) = state.metadata_provider.as_ref() else {
        return Ok(());
    };
    let read = provider
        .begin_read()
        .map_err(|e| format!("open MV dependency drop scope read failed: {e}"))?;

    let definitions = state
        .mv_repo
        .list_definitions(read.as_ref())
        .map_err(|e| format!("load MV definitions for drop scope check failed: {e}"))?;

    let mut edges: Vec<(MvDependencyObjectRef, Vec<MvDependencyObjectRef>)> =
        Vec::with_capacity(definitions.len());
    for def in &definitions {
        let mv_target = stored_definition_dependency_ref_from_state(state, def)?;
        let upstreams = state
            .mv_repo
            .list_dependencies_by_downstream(read.as_ref(), def.mv_id)
            .map_err(|e| format!("load MV dependencies for drop scope check failed: {e}"))?
            .into_iter()
            .map(|dep| dep.upstream)
            .collect::<Vec<_>>();
        edges.push((mv_target, upstreams));
    }

    validate_no_external_dependents_for_scope(scope_catalog, scope_namespace, &edges)
}

pub(crate) fn validate_no_cycle_for_edges(
    new_target: &MvDependencyObjectRef,
    new_upstreams: &[MvDependencyObjectRef],
    existing_edges: &[(MvDependencyObjectRef, Vec<MvDependencyObjectRef>)],
) -> Result<(), String> {
    let mut graph: std::collections::BTreeMap<MvDependencyObjectRef, Vec<MvDependencyObjectRef>> =
        std::collections::BTreeMap::new();
    for (downstream, upstreams) in existing_edges {
        graph.insert(downstream.clone(), upstreams.clone());
    }
    graph.insert(new_target.clone(), new_upstreams.to_vec());

    fn visit(
        graph: &std::collections::BTreeMap<MvDependencyObjectRef, Vec<MvDependencyObjectRef>>,
        node: &MvDependencyObjectRef,
        target: &MvDependencyObjectRef,
        path: &mut Vec<MvDependencyObjectRef>,
    ) -> Option<Vec<MvDependencyObjectRef>> {
        if path.contains(node) {
            return None;
        }
        path.push(node.clone());
        for upstream in graph.get(node).cloned().unwrap_or_default() {
            if &upstream == target {
                let mut cycle = path.clone();
                cycle.push(upstream);
                return Some(cycle);
            }
            if upstream.object_type == MvDependencyObjectType::MaterializedView
                && let Some(cycle) = visit(graph, &upstream, target, path)
            {
                return Some(cycle);
            }
        }
        path.pop();
        None
    }

    if let Some(cycle) = visit(&graph, new_target, new_target, &mut Vec::new()) {
        let display = cycle
            .iter()
            .map(MvDependencyObjectRef::display_name)
            .collect::<Vec<_>>()
            .join(" -> ");
        return Err(format!("dependency cycle detected: {display}"));
    }
    Ok(())
}

pub(crate) fn topological_upstream_order_for_edges(
    target: &MvDependencyObjectRef,
    existing_edges: &[(MvDependencyObjectRef, Vec<MvDependencyObjectRef>)],
) -> Result<Vec<MvDependencyObjectRef>, String> {
    let mut graph: std::collections::BTreeMap<MvDependencyObjectRef, Vec<MvDependencyObjectRef>> =
        std::collections::BTreeMap::new();
    for (downstream, upstreams) in existing_edges {
        graph.insert(downstream.clone(), upstreams.clone());
    }

    let mut permanent = std::collections::BTreeSet::new();
    let mut temporary = std::collections::BTreeSet::new();
    let mut ordered = Vec::new();

    fn visit(
        node: &MvDependencyObjectRef,
        graph: &std::collections::BTreeMap<MvDependencyObjectRef, Vec<MvDependencyObjectRef>>,
        permanent: &mut std::collections::BTreeSet<MvDependencyObjectRef>,
        temporary: &mut std::collections::BTreeSet<MvDependencyObjectRef>,
        ordered: &mut Vec<MvDependencyObjectRef>,
    ) -> Result<(), String> {
        if permanent.contains(node) {
            return Ok(());
        }
        if !temporary.insert(node.clone()) {
            return Err(format!(
                "dependency cycle detected while planning refresh at {}",
                node.display_name()
            ));
        }
        for upstream in graph.get(node).cloned().unwrap_or_default() {
            if upstream.object_type == MvDependencyObjectType::MaterializedView {
                visit(&upstream, graph, permanent, temporary, ordered)?;
            }
        }
        temporary.remove(node);
        permanent.insert(node.clone());
        ordered.push(node.clone());
        Ok(())
    }

    visit(target, &graph, &mut permanent, &mut temporary, &mut ordered)?;
    Ok(ordered)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MvRefreshDependencyStep {
    pub(crate) object: MvDependencyObjectRef,
    pub(crate) target: crate::engine::mv::lifecycle::MvTarget,
    pub(crate) storage_engine: crate::engine::mv::lifecycle::MvStorageEngine,
}

pub(crate) fn refresh_step_for_dependency_object(
    object: &MvDependencyObjectRef,
) -> Result<MvRefreshDependencyStep, String> {
    if object.object_type != MvDependencyObjectType::MaterializedView {
        return Err(format!(
            "refresh dependency object is not a materialized view: {}",
            object.display_name()
        ));
    }
    let storage_engine = match object.storage_engine {
        MvDependencyStorageEngine::StarRocks => {
            crate::engine::mv::lifecycle::MvStorageEngine::StarRocks
        }
        MvDependencyStorageEngine::Iceberg => {
            crate::engine::mv::lifecycle::MvStorageEngine::Iceberg
        }
        MvDependencyStorageEngine::ExternalTable => {
            return Err(format!(
                "external table cannot be refreshed as materialized view: {}",
                object.display_name()
            ));
        }
    };
    Ok(MvRefreshDependencyStep {
        object: object.clone(),
        target: crate::engine::mv::lifecycle::MvTarget {
            catalog: object.catalog.clone(),
            database: object.database_or_namespace.clone(),
            name: object.name.clone(),
        },
        storage_engine,
    })
}

pub(crate) fn build_upstream_refresh_steps(
    state: &Arc<StandaloneState>,
    requested: &MvDependencyObjectRef,
) -> Result<Vec<MvRefreshDependencyStep>, String> {
    let Some(provider) = state.metadata_provider.as_ref() else {
        return Ok(vec![refresh_step_for_dependency_object(requested)?]);
    };
    let read = provider
        .begin_read()
        .map_err(|e| format!("open MV dependency refresh graph read failed: {e}"))?;
    let definitions = state
        .mv_repo
        .list_definitions(read.as_ref())
        .map_err(|e| format!("load MV definitions for refresh graph failed: {e}"))?;

    let mut edges = Vec::new();
    for definition in definitions {
        let target = stored_definition_dependency_ref_from_state(state, &definition)?;
        let upstream_mvs = state
            .mv_repo
            .list_dependencies_by_downstream(read.as_ref(), definition.mv_id)
            .map_err(|e| format!("load MV dependencies for refresh graph failed: {e}"))?
            .into_iter()
            .filter(|dep| dep.upstream.object_type == MvDependencyObjectType::MaterializedView)
            .map(|dep| dep.upstream)
            .collect::<Vec<_>>();
        edges.push((target, upstream_mvs));
    }

    topological_upstream_order_for_edges(requested, &edges)?
        .iter()
        .map(refresh_step_for_dependency_object)
        .collect()
}

pub(crate) fn validate_no_create_cycle(
    state: &Arc<StandaloneState>,
    new_target: &MvDependencyObjectRef,
    new_dependencies: &[CreateMvDependencyRequest],
) -> Result<(), String> {
    let Some(provider) = state.metadata_provider.as_ref() else {
        return Ok(());
    };
    let read = provider
        .begin_read()
        .map_err(|e| format!("open MV dependency graph read failed: {e}"))?;
    let definitions = state
        .mv_repo
        .list_definitions(read.as_ref())
        .map_err(|e| format!("load MV definitions for dependency cycle check failed: {e}"))?;
    let mut edges = Vec::new();
    for definition in definitions {
        let target = stored_definition_dependency_ref_from_state(state, &definition)?;
        let dependencies = state
            .mv_repo
            .list_dependencies_by_downstream(read.as_ref(), definition.mv_id)
            .map_err(|e| format!("load MV dependencies for cycle check failed: {e}"))?
            .into_iter()
            .filter(|dep| dep.upstream.object_type == MvDependencyObjectType::MaterializedView)
            .map(|dep| dep.upstream)
            .collect::<Vec<_>>();
        edges.push((target, dependencies));
    }
    let new_upstreams = new_dependencies
        .iter()
        .filter(|dep| dep.upstream.object_type == MvDependencyObjectType::MaterializedView)
        .map(|dep| dep.upstream.clone())
        .collect::<Vec<_>>();
    validate_no_cycle_for_edges(new_target, &new_upstreams, &edges)
}

fn stored_definition_dependency_ref_from_state(
    state: &Arc<StandaloneState>,
    definition: &StoredMvDefinition,
) -> Result<MvDependencyObjectRef, String> {
    if definition.storage_engine.eq_ignore_ascii_case("iceberg") {
        return stored_definition_dependency_ref(definition, None);
    }
    let managed = state
        .managed_lake
        .read()
        .expect("standalone managed lake read lock");
    let table = managed
        .snapshot
        .tables
        .iter()
        .find(|table| table.table_id == definition.mv_id)
        .ok_or_else(|| {
            format!(
                "managed-lake MV definition {} is missing runtime table metadata",
                definition.mv_id
            )
        })?;
    let database = managed
        .snapshot
        .databases
        .iter()
        .find(|database| database.db_id == table.db_id)
        .ok_or_else(|| {
            format!(
                "managed-lake MV definition {} is missing runtime database metadata",
                definition.mv_id
            )
        })?;
    stored_definition_dependency_ref(definition, Some((&database.name, &table.name)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dependency_ref_display_distinguishes_table_and_mv() {
        let table = iceberg_table_dependency_ref(&IcebergTableRef {
            catalog: "ice".to_string(),
            namespace: "sales".to_string(),
            table: "orders".to_string(),
        });
        let mv = iceberg_mv_dependency_ref("ice", "sales", "orders_mv");

        assert_eq!(table.display_name(), "ice.sales.orders");
        assert_eq!(mv.display_name(), "mv:ice.sales.orders_mv");
    }

    #[test]
    fn dependency_cycle_detector_rejects_new_back_edge() {
        let mv_a = iceberg_mv_dependency_ref("ice", "sales", "mv_a");
        let mv_b = iceberg_mv_dependency_ref("ice", "sales", "mv_b");
        let mv_c = iceberg_mv_dependency_ref("ice", "sales", "mv_c");
        let existing = vec![
            (mv_a.clone(), vec![mv_b.clone()]),
            (mv_b.clone(), vec![mv_c.clone()]),
        ];

        let err = validate_no_cycle_for_edges(&mv_c, &[mv_a.clone()], &existing)
            .expect_err("c -> a should form a cycle");
        assert_eq!(
            err,
            "dependency cycle detected: mv:ice.sales.mv_c -> mv:ice.sales.mv_a -> mv:ice.sales.mv_b -> mv:ice.sales.mv_c"
        );
    }

    #[test]
    fn dependency_cycle_detector_accepts_dag() {
        let mv_a = iceberg_mv_dependency_ref("ice", "sales", "mv_a");
        let mv_b = iceberg_mv_dependency_ref("ice", "sales", "mv_b");
        let mv_c = iceberg_mv_dependency_ref("ice", "sales", "mv_c");
        let existing = vec![(mv_b.clone(), vec![mv_a.clone()])];

        validate_no_cycle_for_edges(&mv_c, &[mv_b], &existing).expect("dag should be accepted");
        let _ = mv_a;
    }

    #[test]
    fn topological_upstream_order_runs_deepest_first() {
        let mv_a = iceberg_mv_dependency_ref("ice", "sales", "mv_a");
        let mv_b = iceberg_mv_dependency_ref("ice", "sales", "mv_b");
        let mv_c = iceberg_mv_dependency_ref("ice", "sales", "mv_c");
        let edges = vec![
            (mv_b.clone(), vec![mv_a.clone()]),
            (mv_c.clone(), vec![mv_b.clone()]),
        ];

        let order = topological_upstream_order_for_edges(&mv_c, &edges).expect("order");
        assert_eq!(order, vec![mv_a, mv_b, mv_c]);
    }

    #[test]
    fn external_dependents_scope_passes_when_scope_is_self_contained() {
        // Downstream MV is *inside* the scope (cat1.db1), so dropping the
        // scope also drops the downstream — no orphan risk.
        let mv_target = iceberg_mv_dependency_ref("cat1", "db1", "mv_inside");
        let upstream = iceberg_table_object_ref("cat1", "db1", "orders");
        let edges = vec![(mv_target, vec![upstream])];

        validate_no_external_dependents_for_scope("cat1", Some("db1"), &edges)
            .expect("scope-internal MV must not block the drop");
    }

    #[test]
    fn external_dependents_scope_rejects_external_dependent() {
        // Downstream MV lives outside the scope but depends on a table inside
        // it — dropping the scope would orphan the MV.
        let mv_target = iceberg_mv_dependency_ref("cat2", "db2", "mv_outside");
        let upstream = iceberg_table_object_ref("cat1", "db1", "orders");
        let edges = vec![(mv_target, vec![upstream])];

        let err = validate_no_external_dependents_for_scope("cat1", Some("db1"), &edges)
            .expect_err("orphaning MV must be rejected");
        assert!(
            err.contains("cannot drop `cat1.db1`"),
            "err missing scope label: {err}"
        );
        assert!(
            err.contains("mv:cat2.db2.mv_outside depends on cat1.db1.orders"),
            "err missing dependent detail: {err}"
        );
    }

    #[test]
    fn external_dependents_scope_at_catalog_granularity() {
        // DROP CATALOG cat1 — same risk, but the scope spans every namespace
        // under cat1. An MV in cat2.* depending on anything under cat1.*
        // must block the drop.
        let mv_target = iceberg_mv_dependency_ref("cat2", "db2", "mv_outside");
        let upstream_a = iceberg_table_object_ref("cat1", "ns1", "events");
        let upstream_b = iceberg_table_object_ref("cat1", "ns2", "orders");
        let edges = vec![(mv_target, vec![upstream_a.clone(), upstream_b.clone()])];

        let err = validate_no_external_dependents_for_scope("cat1", None, &edges)
            .expect_err("catalog-wide drop must reject the orphan");
        assert!(err.contains("cannot drop `cat1`"), "err: {err}");

        // Reverse: dropping cat2 should be fine — cat2.mv depends only on
        // cat1.* upstreams; nothing inside cat2 has external dependents.
        validate_no_external_dependents_for_scope("cat2", None, &edges)
            .expect("dropping the catalog that contains only an MV is allowed");
    }

    #[test]
    fn external_dependents_scope_ignores_non_iceberg_upstreams() {
        // Managed-lake upstreams are never in an Iceberg scope, even if the
        // catalog/namespace strings happen to match.
        let mv_target = iceberg_mv_dependency_ref("cat2", "db2", "mv_outside");
        let upstream = managed_table_object_ref("cat1", "orders");
        let edges = vec![(mv_target, vec![upstream])];

        validate_no_external_dependents_for_scope("cat1", Some("orders"), &edges)
            .expect("non-iceberg upstreams must not block iceberg-scope drops");
    }

    #[test]
    fn external_dependents_scope_case_insensitive_matching() {
        // Catalog/namespace identifiers are normalized to lowercase by the
        // resolver; ensure the scope check also works when the caller passes
        // mixed-case values.
        let mv_target = iceberg_mv_dependency_ref("cat2", "db2", "mv_outside");
        let upstream = iceberg_table_object_ref("cat1", "db1", "orders");
        let edges = vec![(mv_target, vec![upstream])];

        let err = validate_no_external_dependents_for_scope("CAT1", Some("DB1"), &edges)
            .expect_err("case-insensitive scope match must still reject orphan");
        assert!(err.contains("cannot drop `CAT1.DB1`"), "err: {err}");
    }

    #[test]
    fn topological_upstream_order_deduplicates_shared_dependencies() {
        let mv_a = iceberg_mv_dependency_ref("ice", "sales", "mv_a");
        let mv_b = iceberg_mv_dependency_ref("ice", "sales", "mv_b");
        let mv_c = iceberg_mv_dependency_ref("ice", "sales", "mv_c");
        let mv_d = iceberg_mv_dependency_ref("ice", "sales", "mv_d");
        let edges = vec![
            (mv_b.clone(), vec![mv_a.clone()]),
            (mv_c.clone(), vec![mv_a.clone()]),
            (mv_d.clone(), vec![mv_b.clone(), mv_c.clone()]),
        ];

        let order = topological_upstream_order_for_edges(&mv_d, &edges).expect("order");
        assert_eq!(order, vec![mv_a, mv_b, mv_c, mv_d]);
    }
}
