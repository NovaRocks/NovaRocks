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

use novarocks_connector_contract::ConnectorReadExecutionSchema;

use crate::catalog_application::query_bindings::{
    QueryFrozenCohortRead, QueryTableBinding, QueryTableBindingAdmission, QueryTableBindingKey,
    QueryTableBindingStore,
};
use crate::query_execution::cohort_read::QueryRewriteGroupRead;
use novarocks_sql::binding::SqlTableBindingId;
use novarocks_sql::planning::query_execution::{
    FrozenConnectorScanIdentity, FrozenConnectorScanPlan, build_table_execute_scan_plan,
    table_execute_resolved_analyzer_table,
};

/// Admit the synthetic SQL binding one procedure cohort read is planned
/// through.
///
/// The group is admitted with it. The binding's name is query-local and
/// synthetic, so nothing could recover the group from it later; the read that
/// will be frozen is recorded here, where the binding is minted.
pub(crate) fn admit_table_execute_scan_binding(
    bindings: &QueryTableBindingStore,
    identity: &FrozenConnectorScanIdentity,
    input_schema: &ConnectorReadExecutionSchema,
    read: QueryRewriteGroupRead,
) -> Result<SqlTableBindingId, String> {
    bindings.resolve_or_insert_with_id(table_execute_binding_key(identity), |binding| {
        let planning_lease = read.planning_lease.clone();
        Ok(QueryTableBinding {
            resolved: table_execute_resolved_analyzer_table(
                identity,
                super::distributed_rewrite::connector_execution_columns(input_schema)?,
                binding,
            ),
            statistics_pin: None,
            admission: QueryTableBindingAdmission::FrozenRead(planning_lease),
            source_metadata: None,
            scan_materialization: None,
            mv_target_read: None,
            write_target_admission: None,
            frozen_cohort_read: Some(QueryFrozenCohortRead::TableExecute(read)),
            frozen_snapshot_materializations: BTreeMap::new(),
            prepared_source_reads: BTreeMap::new(),
        })
    })
}

/// Build the minimal physical scan carrier for one procedure cohort read.
pub(crate) fn table_execute_scan_physical_plan(
    identity: &FrozenConnectorScanIdentity,
    input_schema: &ConnectorReadExecutionSchema,
    binding: SqlTableBindingId,
) -> Result<FrozenConnectorScanPlan, String> {
    build_table_execute_scan_plan(
        identity,
        &super::distributed_rewrite::connector_execution_columns(input_schema)?,
        binding,
    )
}

fn table_execute_binding_key(identity: &FrozenConnectorScanIdentity) -> QueryTableBindingKey {
    QueryTableBindingKey::strict_base(identity.catalog(), identity.namespace(), identity.table())
}
