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

//! Generic admission for one provider-frozen cohort read of a pinned file set.
//!
//! A mutation or rewrite cohort commits a replacement for exactly the files it
//! read. This module carries the connector's own pinned set through SQL
//! planning as a synthetic, query-local relation and hands it back to the same
//! connector generation at preparation. It never derives, widens, or narrows
//! the set: the engine has no basis on which it could, and a rewrite that
//! reads less than its commit replaces corrupts the relation.

use std::collections::BTreeMap;

use arrow::datatypes::SchemaRef;

use crate::catalog_application::query_bindings::{
    QueryTableBinding, QueryTableBindingAdmission, QueryTableBindingKey, QueryTableBindingStore,
};
use crate::catalog_application::query_materializer::QueryLocalTableOverlay;
use crate::query_execution::preparation::scan::{
    QueryPinnedFileSetRead, ResolvedScanExecution, ScanBindingResolver,
};
use novarocks_sql::binding::SqlTableBindingId;
use novarocks_sql::planning::query_execution::{
    FrozenConnectorScanIdentity, FrozenConnectorScanPlan, build_pinned_file_set_scan_plan,
    matches_pinned_file_set_scan, pinned_file_set_resolved_analyzer_table,
};

/// Admit the synthetic SQL binding one pinned cohort read is planned through.
pub(crate) fn admit_pinned_file_set_scan_binding(
    bindings: &QueryTableBindingStore,
    identity: &FrozenConnectorScanIdentity,
    input_schema: &SchemaRef,
) -> Result<SqlTableBindingId, String> {
    bindings.resolve_or_insert_with_id(pinned_file_set_binding_key(identity), |binding| {
        pinned_file_set_query_table_binding(identity.clone(), input_schema.clone(), binding)
    })
}

/// Build the request-local catalog overlay a SQL-shaped cohort read resolves
/// through. The overlay and the resolver must be created from the same identity
/// and binding store; neither is published to shared catalog state.
pub(crate) fn pinned_file_set_query_local_overlay(
    identity: &FrozenConnectorScanIdentity,
    input_schema: &SchemaRef,
) -> QueryLocalTableOverlay {
    let identity = identity.clone();
    let schema = input_schema.clone();
    QueryLocalTableOverlay::new(
        identity.namespace().to_string(),
        identity.table().to_string(),
        pinned_file_set_binding_key(&identity),
        move |binding| {
            pinned_file_set_query_table_binding(identity.clone(), schema.clone(), binding)
        },
    )
}

/// Build the minimal physical scan carrier for one pinned cohort read.
pub(crate) fn pinned_file_set_scan_physical_plan(
    identity: &FrozenConnectorScanIdentity,
    input_schema: &SchemaRef,
    binding: SqlTableBindingId,
) -> FrozenConnectorScanPlan {
    build_pinned_file_set_scan_plan(identity, input_schema, binding)
}

fn pinned_file_set_binding_key(identity: &FrozenConnectorScanIdentity) -> QueryTableBindingKey {
    QueryTableBindingKey::strict_base(identity.catalog(), identity.namespace(), identity.table())
}

fn pinned_file_set_query_table_binding(
    identity: FrozenConnectorScanIdentity,
    input_schema: SchemaRef,
    binding: SqlTableBindingId,
) -> Result<QueryTableBinding, String> {
    Ok(QueryTableBinding {
        resolved: pinned_file_set_resolved_analyzer_table(&identity, input_schema, binding),
        statistics_pin: None,
        admission: QueryTableBindingAdmission::Local,
        scan_materialization: None,
        mv_target_read: None,
        write_target_admission: None,
        frozen_snapshot_materializations: BTreeMap::new(),
        admitted_change_scans: BTreeMap::new(),
    })
}

/// Injection of one provider-frozen cohort read into scan preparation.
///
/// Unlike an opaque frozen read, nothing is consumed here: the pinned facts are
/// a description, not a planned scan, so the same cohort read may legitimately
/// answer more than one matching scan node of its generated statement.
pub(crate) struct PinnedFileSetReadResolver {
    binding: SqlTableBindingId,
    identity: FrozenConnectorScanIdentity,
    read: QueryPinnedFileSetRead,
}

impl PinnedFileSetReadResolver {
    pub(crate) const fn new(
        binding: SqlTableBindingId,
        identity: FrozenConnectorScanIdentity,
        read: QueryPinnedFileSetRead,
    ) -> Self {
        Self {
            binding,
            identity,
            read,
        }
    }
}

impl ScanBindingResolver for PinnedFileSetReadResolver {
    fn resolve_scan(
        &self,
        _node_id: i32,
        scan: &novarocks_sql::plan_read::PlanScanNode,
    ) -> Result<Option<ResolvedScanExecution>, String> {
        if !matches_pinned_file_set_scan(scan, self.binding, &self.identity) {
            return Ok(None);
        }
        Ok(Some(ResolvedScanExecution::AdmittedPinnedFileSet(
            self.read.clone(),
        )))
    }
}
