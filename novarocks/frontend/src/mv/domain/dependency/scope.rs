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

use crate::mv::domain::dependency::model::{MvDependencyObjectRef, MvDependencyStorageEngine};

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

fn iceberg_mv_target_in_scope(
    target: &MvDependencyObjectRef,
    scope_catalog: &str,
    scope_namespace: Option<&str>,
) -> Option<String> {
    if !object_in_iceberg_scope(target, scope_catalog, scope_namespace) {
        return None;
    }
    let catalog = target.catalog.as_deref()?;
    Some(format!(
        "{catalog}.{}.{}",
        target.database_or_namespace, target.name
    ))
}

pub(crate) fn validate_no_iceberg_mv_targets_in_scope(
    scope_catalog: &str,
    scope_namespace: Option<&str>,
    targets: &[MvDependencyObjectRef],
) -> Result<(), String> {
    let mut in_scope_targets = targets
        .iter()
        .filter_map(|target| iceberg_mv_target_in_scope(target, scope_catalog, scope_namespace))
        .collect::<Vec<_>>();
    if in_scope_targets.is_empty() {
        return Ok(());
    }
    in_scope_targets.sort();
    in_scope_targets.dedup();
    let scope_str = match scope_namespace {
        Some(ns) => format!("`{scope_catalog}.{ns}`"),
        None => format!("`{scope_catalog}`"),
    };
    Err(format!(
        "cannot drop {scope_str}: contains materialized views: {}; use DROP MATERIALIZED VIEW first",
        in_scope_targets.join(", ")
    ))
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mv::domain::dependency::model::{
        iceberg_mv_dependency_ref, iceberg_table_object_ref, starrocks_mv_dependency_ref,
        starrocks_table_object_ref,
    };

    #[test]
    fn iceberg_mv_targets_scope_rejects_namespace_scope() {
        let targets = vec![iceberg_mv_dependency_ref("ice", "analytics", "mv_orders")];

        let err = validate_no_iceberg_mv_targets_in_scope("ice", Some("analytics"), &targets)
            .expect_err("namespace drop containing an iceberg MV must be rejected");
        assert!(err.contains("cannot drop `ice.analytics`"), "err: {err}");
        assert!(err.contains("ice.analytics.mv_orders"), "err: {err}");
        assert!(err.contains("DROP MATERIALIZED VIEW"), "err: {err}");
    }

    #[test]
    fn iceberg_mv_targets_scope_rejects_catalog_scope() {
        let targets = vec![iceberg_mv_dependency_ref("ice", "analytics", "mv_orders")];

        let err = validate_no_iceberg_mv_targets_in_scope("ice", None, &targets)
            .expect_err("catalog drop containing an iceberg MV must be rejected");
        assert!(err.contains("cannot drop `ice`"), "err: {err}");
        assert!(err.contains("ice.analytics.mv_orders"), "err: {err}");
    }

    #[test]
    fn iceberg_mv_targets_scope_ignores_non_iceberg_and_outside_scope() {
        let targets = vec![
            starrocks_mv_dependency_ref("analytics", "mv_starrocks"),
            iceberg_mv_dependency_ref("ice", "other", "mv_other"),
            iceberg_mv_dependency_ref("other_catalog", "analytics", "mv_other_catalog"),
        ];

        validate_no_iceberg_mv_targets_in_scope("ice", Some("analytics"), &targets)
            .expect("only in-scope iceberg MV targets should block the drop");
    }

    #[test]
    fn iceberg_mv_targets_scope_case_insensitive_matching() {
        let targets = vec![iceberg_mv_dependency_ref("ICE", "Analytics", "mv_orders")];

        let err = validate_no_iceberg_mv_targets_in_scope("ice", Some("analytics"), &targets)
            .expect_err("case-insensitive scope match must reject the drop");
        assert!(err.contains("ICE.Analytics.mv_orders"), "err: {err}");
    }

    #[test]
    fn external_dependents_scope_passes_when_scope_is_self_contained() {
        let mv_target = iceberg_mv_dependency_ref("cat1", "db1", "mv_inside");
        let upstream = iceberg_table_object_ref("cat1", "db1", "orders");
        let edges = vec![(mv_target, vec![upstream])];

        validate_no_external_dependents_for_scope("cat1", Some("db1"), &edges)
            .expect("scope-internal MV must not block the drop");
    }

    #[test]
    fn external_dependents_scope_rejects_external_dependent() {
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
        let mv_target = iceberg_mv_dependency_ref("cat2", "db2", "mv_outside");
        let upstream_a = iceberg_table_object_ref("cat1", "ns1", "events");
        let upstream_b = iceberg_table_object_ref("cat1", "ns2", "orders");
        let edges = vec![(mv_target, vec![upstream_a.clone(), upstream_b.clone()])];

        let err = validate_no_external_dependents_for_scope("cat1", None, &edges)
            .expect_err("catalog-wide drop must reject the orphan");
        assert!(err.contains("cannot drop `cat1`"), "err: {err}");

        validate_no_external_dependents_for_scope("cat2", None, &edges)
            .expect("dropping the catalog that contains only an MV is allowed");
    }

    #[test]
    fn external_dependents_scope_ignores_non_iceberg_upstreams() {
        let mv_target = iceberg_mv_dependency_ref("cat2", "db2", "mv_outside");
        let upstream = starrocks_table_object_ref("cat1", "orders");
        let edges = vec![(mv_target, vec![upstream])];

        validate_no_external_dependents_for_scope("cat1", Some("orders"), &edges)
            .expect("non-iceberg upstreams must not block iceberg-scope drops");
    }

    #[test]
    fn external_dependents_scope_case_insensitive_matching() {
        let mv_target = iceberg_mv_dependency_ref("cat2", "db2", "mv_outside");
        let upstream = iceberg_table_object_ref("cat1", "db1", "orders");
        let edges = vec![(mv_target, vec![upstream])];

        let err = validate_no_external_dependents_for_scope("CAT1", Some("DB1"), &edges)
            .expect_err("case-insensitive scope match must still reject orphan");
        assert!(err.contains("cannot drop `CAT1.DB1`"), "err: {err}");
    }
}
