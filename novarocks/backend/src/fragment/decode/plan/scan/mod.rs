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

//! Fragment scan plan decoding.

mod typed;

use super::node::DecodedNode;
use novarocks_execution::exec::expr::ExprArena;
use novarocks_native_adapter::fragment_decode_context::NativePlanDecodeContext;
use novarocks_native_adapter::fragment_error::NativeFragmentDecodeError;
use novarocks_native_adapter::fragment_scan_output::decode_scan_output_columns;
use novarocks_native_adapter::fragment_variant_path::parse_native_scan_variant_path_columns;
use novarocks_proto_codec::FieldPath;
use novarocks_proto_models::plan;

pub(crate) fn lower_scan_node(
    node: &plan::DistributedNode,
    _physical: &plan::PlanNode,
    scan: &plan::ScanNode,
    path: FieldPath,
    ctx: &NativePlanDecodeContext,
    arena: &mut ExprArena,
) -> Result<DecodedNode, NativeFragmentDecodeError> {
    if !scan.dict_columns.is_empty() {
        return Err(NativeFragmentDecodeError::unsupported(
            path.clone().field("dict_columns"),
            "ScanNode dict_columns are not supported by native lowering yet",
        ));
    }
    let table = scan.table.as_ref().ok_or_else(|| {
        NativeFragmentDecodeError::missing(path.clone().field("table"), "ScanNode table missing")
    })?;
    let source = table.source.as_ref().ok_or_else(|| {
        NativeFragmentDecodeError::missing(
            path.clone().field("table").field("source"),
            "ScanNode table source missing",
        )
    })?;
    let source = source.kind.as_ref().ok_or_else(|| {
        NativeFragmentDecodeError::missing(
            path.clone().field("table").field("source").field("kind"),
            "ScanNode table source kind missing",
        )
    })?;
    let source_path = path.clone().field("table").field("source");
    let output_columns = decode_scan_output_columns(scan, path.clone())?;
    let variant_path_plan =
        parse_native_scan_variant_path_columns(scan, table, output_columns.columns())
            .map_err(|error| error.into_native(path.clone()))?;
    match source {
        plan::scan_source::Kind::TypedConnectorRead(source) => typed::lower_typed_connector_scan(
            node,
            scan,
            source,
            &output_columns,
            &variant_path_plan,
            ctx,
            arena,
        )
        .map_err(|error| error.into_native(source_path.field("typed_connector_read"))),
    }
}
