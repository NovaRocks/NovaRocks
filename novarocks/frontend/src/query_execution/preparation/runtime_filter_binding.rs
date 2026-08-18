// Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
// See the NOTICE file distributed with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

//! Core-side resolution of SQL-projected scan-domain requests.
//!
//! SQL owns runtime-filter semantics. Core only matches each request against
//! the exact connector read already pinned during scan preparation.

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
    let read = scan_bindings
        .connector_read(request.fragment_id, request.node_id)
        .ok_or_else(|| format!(
            "runtime filter binding id={} scan-domain target requires a pinned connector read for fragment_id={} node_id={}",
            request.binding_id, request.fragment_id, request.node_id
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
    let output_matches = read
        .scan
        .output_schema()
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| field.name().eq_ignore_ascii_case(&physical.source.name))
        .collect::<Vec<_>>();
    let [(output_ordinal, output)] = output_matches.as_slice() else {
        return Err(format!(
            "runtime filter binding id={} scan-domain target source column '{}' does not resolve to exactly one pinned connector output",
            request.binding_id, physical.source.name
        ));
    };
    if output.data_type() != &request.data_type || output.is_nullable() != request.nullable {
        return Err(format!(
            "runtime filter binding id={} scan-domain target source column '{}' type/nullability drifted from pinned connector output",
            request.binding_id, physical.source.name
        ));
    }
    let field_ordinal = *read.provider_field_ordinals.get(*output_ordinal).ok_or_else(|| format!(
        "runtime filter binding id={} scan-domain target connector output ordinal {} has no pinned provider ordinal",
        request.binding_id, output_ordinal
    ))?;
    Ok(SqlRuntimeFilterSourceResolution {
        binding_id: request.binding_id,
        field_ordinal,
        data_type: request.data_type,
        nullable: request.nullable,
    })
}
