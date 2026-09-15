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

//! Generic admission for one distributed procedure's cohort read.
//!
//! A rewrite cohort whose input is not table rows -- re-encoding delete
//! artifacts is the one such procedure -- reads the relation its frozen group
//! names. This module carries that group through SQL planning as a synthetic,
//! query-local relation and hands it back to the same connector generation at
//! preparation. It never resolves the group to artifacts, and never restates
//! the rule that selected it: the same group is what the cohort's commit
//! replaces, and a set re-derived here could differ from it.

use std::collections::BTreeMap;

use arrow::datatypes::SchemaRef;

use crate::catalog_application::query_bindings::{
    QueryTableBinding, QueryTableBindingAdmission, QueryTableBindingKey, QueryTableBindingStore,
};
use crate::query_execution::preparation::scan::{
    QueryRewriteGroupRead, ResolvedScanExecution, ScanBindingResolver,
};
use novarocks_sql::binding::SqlTableBindingId;
use novarocks_sql::planning::query_execution::{
    FrozenConnectorScanIdentity, FrozenConnectorScanPlan, build_table_execute_scan_plan,
    matches_table_execute_scan, table_execute_resolved_analyzer_table,
};

/// Admit the synthetic SQL binding one procedure cohort read is planned
/// through.
pub(crate) fn admit_table_execute_scan_binding(
    bindings: &QueryTableBindingStore,
    identity: &FrozenConnectorScanIdentity,
    input_schema: &SchemaRef,
) -> Result<SqlTableBindingId, String> {
    bindings.resolve_or_insert_with_id(table_execute_binding_key(identity), |binding| {
        Ok(QueryTableBinding {
            resolved: table_execute_resolved_analyzer_table(
                identity,
                input_schema.clone(),
                binding,
            ),
            statistics_pin: None,
            admission: QueryTableBindingAdmission::Local,
            scan_materialization: None,
            mv_target_read: None,
            write_target_admission: None,
            frozen_snapshot_materializations: BTreeMap::new(),
            admitted_change_scans: BTreeMap::new(),
        })
    })
}

/// Build the minimal physical scan carrier for one procedure cohort read.
pub(crate) fn table_execute_scan_physical_plan(
    identity: &FrozenConnectorScanIdentity,
    input_schema: &SchemaRef,
    binding: SqlTableBindingId,
) -> FrozenConnectorScanPlan {
    build_table_execute_scan_plan(identity, input_schema, binding)
}

fn table_execute_binding_key(identity: &FrozenConnectorScanIdentity) -> QueryTableBindingKey {
    QueryTableBindingKey::strict_base(identity.catalog(), identity.namespace(), identity.table())
}

/// Injection of one distributed procedure's cohort read into scan preparation.
///
/// Nothing is consumed here: the group is a description, not a planned scan, so
/// the same cohort read may legitimately answer more than one matching scan
/// node of its generated statement.
pub(crate) struct RewriteGroupReadResolver {
    binding: SqlTableBindingId,
    identity: FrozenConnectorScanIdentity,
    read: QueryRewriteGroupRead,
}

impl RewriteGroupReadResolver {
    pub(crate) const fn new(
        binding: SqlTableBindingId,
        identity: FrozenConnectorScanIdentity,
        read: QueryRewriteGroupRead,
    ) -> Self {
        Self {
            binding,
            identity,
            read,
        }
    }
}

impl ScanBindingResolver for RewriteGroupReadResolver {
    fn resolve_scan(
        &self,
        _node_id: i32,
        scan: &novarocks_sql::plan_read::PlanScanNode,
    ) -> Result<Option<ResolvedScanExecution>, String> {
        if !matches_table_execute_scan(scan, self.binding, &self.identity) {
            return Ok(None);
        }
        Ok(Some(ResolvedScanExecution::AdmittedTableExecute(
            self.read.clone(),
        )))
    }
}
