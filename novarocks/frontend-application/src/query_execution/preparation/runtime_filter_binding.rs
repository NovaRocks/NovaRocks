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

//! Core-side resolution of SQL-projected scan-domain requests.
//!
//! SQL owns runtime-filter semantics. Core only confirms that the typed scan
//! preparation just froze really carries the filter, bound to the one scan
//! output column the request names.
//!
//! The binding travels on the scan carrier itself as `filter id -> variable ->
//! ColumnHandle`, which is column identity — for Iceberg, the field ID. Nothing
//! here, and nothing downstream, needs a provider schema field index: the
//! opaque read that used to freeze one has no producer left.

use crate::query_execution::preparation::scan::ScanExecutionBindings;
use novarocks_sql::planning::query_execution::{
    SqlRuntimeFilterSourceResolution, SqlRuntimeFilterSourceScanRequest,
};

pub(super) fn resolve_runtime_filter_source_targets(
    requests: impl IntoIterator<Item = SqlRuntimeFilterSourceScanRequest>,
    scan_bindings: &ScanExecutionBindings,
) -> Result<Vec<SqlRuntimeFilterSourceResolution>, String> {
    requests
        .into_iter()
        .map(|request| resolve_target(request, scan_bindings))
        .collect()
}

fn resolve_target(
    request: SqlRuntimeFilterSourceScanRequest,
    scan_bindings: &ScanExecutionBindings,
) -> Result<SqlRuntimeFilterSourceResolution, String> {
    let binding = scan_bindings.binding(request.node_id).ok_or_else(|| format!(
        "runtime filter binding id={} scan-domain target has no pinned scan binding for node_id={}",
        request.binding_id, request.node_id
    ))?;
    let physical = binding
        .physical_columns
        .iter()
        .filter(|column| column.planner.column_id == request.column_id)
        .collect::<Vec<_>>();
    let [physical] = physical.as_slice() else {
        return Err(format!(
            "runtime filter binding id={} scan-domain target column id {} does not resolve to exactly one pinned physical scan output",
            request.binding_id, request.column_id
        ));
    };
    if physical.planner.data_type != request.data_type
        || physical.planner.nullable != request.nullable
        || physical.source.data_type != request.data_type
        || physical.source.nullable != request.nullable
    {
        return Err(format!(
            "runtime filter binding id={} scan-domain target column '{}' type/nullability drifted from its pinned scan binding",
            request.binding_id, physical.source.name
        ));
    }
    let typed = scan_bindings
        .typed_scan(request.fragment_id, request.node_id)
        .ok_or_else(|| format!(
            "runtime filter binding id={} scan-domain target requires a typed connector scan for fragment_id={} node_id={}",
            request.binding_id, request.fragment_id, request.node_id
        ))?;
    // Preparation bound this filter before the relation was frozen. Confirming
    // it here is what proves the reader really receives the filter, instead of
    // a producer publishing into a scan that never consults it.
    let bound_output = typed
        .prepared
        .dynamic_filter_output(request.binding_id)
        .ok_or_else(|| format!(
            "runtime filter binding id={} scan-domain target was never offered to the typed scan of fragment_id={} node_id={}",
            request.binding_id, request.fragment_id, request.node_id
        ))?;
    if bound_output != physical.planner.name {
        return Err(format!(
            "runtime filter binding id={} scan-domain target names column '{}' but the typed scan bound it to '{bound_output}'",
            request.binding_id, physical.planner.name
        ));
    }
    // The carrier is the only thing the backend reads, so the binding must be
    // on it exactly once and must name an assignment this scan really makes.
    let source = typed.prepared.table_scan.source();
    let carried = source
        .dynamic_filters()
        .iter()
        .filter(|filter| filter.filter_id() == request.binding_id)
        .collect::<Vec<_>>();
    let [carried] = carried.as_slice() else {
        return Err(format!(
            "runtime filter binding id={} scan-domain target does not appear exactly once on the typed scan carrier of fragment_id={} node_id={}",
            request.binding_id, request.fragment_id, request.node_id
        ));
    };
    if !source
        .assignments()
        .iter()
        .any(|assignment| assignment.variable() == carried.variable())
    {
        return Err(format!(
            "runtime filter binding id={} scan-domain target names scan variable '{}', which the typed scan does not assign",
            request.binding_id,
            carried.variable()
        ));
    }
    Ok(SqlRuntimeFilterSourceResolution {
        binding_id: request.binding_id,
        data_type: request.data_type,
        nullable: request.nullable,
    })
}
