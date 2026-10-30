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

use novarocks_connector_contract::ConnectorReadExecutionSchema;

use crate::catalog_application::query_bindings::{
    QueryFrozenCohortRead, QueryTableBinding, QueryTableBindingAdmission, QueryTableBindingKey,
    QueryTableBindingStore,
};
use crate::catalog_application::query_materializer::QueryLocalTableOverlay;
use crate::query_execution::cohort_read::QueryPinnedFileSetRead;
use novarocks_sql::binding::SqlTableBindingId;
use novarocks_sql::planning::query_execution::{
    FrozenConnectorScanIdentity, FrozenConnectorScanPlan, build_pinned_file_set_scan_plan,
    pinned_file_set_resolved_analyzer_table,
};

/// Admit the synthetic SQL binding one pinned cohort read is planned through.
///
/// The cohort itself is admitted with it. A pinned set is not something a
/// later lookup could recover from the binding's name -- that name is
/// query-local and synthetic -- so the read that will be frozen is recorded
/// here, where the binding that carries it through planning is minted.
pub(crate) fn admit_pinned_file_set_scan_binding(
    bindings: &QueryTableBindingStore,
    identity: &FrozenConnectorScanIdentity,
    input_schema: &ConnectorReadExecutionSchema,
    read: QueryPinnedFileSetRead,
) -> Result<SqlTableBindingId, String> {
    bindings.resolve_or_insert_with_id(pinned_file_set_binding_key(identity), move |binding| {
        pinned_file_set_query_table_binding(identity.clone(), input_schema.clone(), binding, read)
    })
}

/// Build the request-local catalog overlay a SQL-shaped cohort read resolves
/// through. The overlay and the resolver must be created from the same identity
/// and binding store; neither is published to shared catalog state.
pub(crate) fn pinned_file_set_query_local_overlay(
    identity: &FrozenConnectorScanIdentity,
    input_schema: &ConnectorReadExecutionSchema,
    read: QueryPinnedFileSetRead,
) -> QueryLocalTableOverlay {
    let retained = PinnedFileSetOverlay {
        identity: identity.clone(),
        schema: input_schema.clone(),
        read,
    };
    QueryLocalTableOverlay::new(
        retained.identity.namespace().to_string(),
        retained.identity.table().to_string(),
        pinned_file_set_binding_key(&retained.identity),
        move |binding| retained.materialize(binding),
    )
}

// One captured owner fixes field destruction order across schema aliases.
pub(crate) struct PinnedFileSetOverlay {
    identity: FrozenConnectorScanIdentity,
    schema: ConnectorReadExecutionSchema,
    read: QueryPinnedFileSetRead,
}
impl PinnedFileSetOverlay {
    fn materialize(&self, binding: SqlTableBindingId) -> Result<QueryTableBinding, String> {
        pinned_file_set_query_table_binding(
            self.identity.clone(),
            self.schema.clone(),
            binding,
            self.read.clone(),
        )
    }
}

/// Build the minimal physical scan carrier for one pinned cohort read.
pub(crate) fn pinned_file_set_scan_physical_plan(
    identity: &FrozenConnectorScanIdentity,
    input_schema: &ConnectorReadExecutionSchema,
    binding: SqlTableBindingId,
) -> Result<FrozenConnectorScanPlan, String> {
    build_pinned_file_set_scan_plan(
        identity,
        &super::distributed_rewrite::connector_execution_columns(input_schema)?,
        binding,
    )
}

fn pinned_file_set_binding_key(identity: &FrozenConnectorScanIdentity) -> QueryTableBindingKey {
    QueryTableBindingKey::strict_base(identity.catalog(), identity.namespace(), identity.table())
}

fn pinned_file_set_query_table_binding(
    identity: FrozenConnectorScanIdentity,
    input_schema: ConnectorReadExecutionSchema,
    binding: SqlTableBindingId,
    read: QueryPinnedFileSetRead,
) -> Result<QueryTableBinding, String> {
    let planning_lease = read.planning_lease.clone();
    Ok(QueryTableBinding {
        resolved: pinned_file_set_resolved_analyzer_table(
            &identity,
            super::distributed_rewrite::connector_execution_columns(&input_schema)?,
            binding,
        ),
        statistics_pin: None,
        admission: QueryTableBindingAdmission::FrozenRead(planning_lease),
        source_metadata: None,
        scan_materialization: None,
        mv_target_read: None,
        write_target_admission: None,
        frozen_cohort_read: Some(QueryFrozenCohortRead::PinnedFileSet(read)),
        frozen_snapshot_materializations: BTreeMap::new(),
        prepared_source_reads: BTreeMap::new(),
    })
}
