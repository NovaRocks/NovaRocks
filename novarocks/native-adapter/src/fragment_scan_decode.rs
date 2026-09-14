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

//! Native fragment scan decoding helpers.

use arrow::datatypes::DataType;

use crate::fragment_error::NativeFragmentLeafDecodeError;
use crate::fragment_expression::decode_expr_for_slot_layout;
use novarocks_execution::exec::chunk::SlotLayout as Layout;
use novarocks_execution::exec::expr::{ExprArena, ExprId, ExprNode};
use novarocks_execution::exec::variant_read::VariantPathSpec;
use novarocks_proto_codec::{FieldPath, ProtocolErrorKind};
use novarocks_proto_models::plan;
use novarocks_types::SlotId;
/// Refuse a plan whose read columns omit a VARIANT path's source column.
///
/// The source column is what the synthetic column is extracted from, so a read
/// layout without it describes a derivation with nothing to derive from.
/// `field` names the wire field that decided the read layout, which differs per
/// carrier: the opaque one publishes it as an Arrow schema, the typed one as
/// its ordered assignments.
pub fn validate_variant_path_read_slots(
    specs: &[VariantPathSpec],
    read_slot_ids: &[SlotId],
    field: &'static str,
) -> Result<(), NativeFragmentLeafDecodeError> {
    for spec in specs {
        if !read_slot_ids.contains(&spec.source_read_slot_id) {
            return Err(NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::InconsistentFields,
                field,
                format!(
                    "scan read columns omit VARIANT source `{}` for output `{}`",
                    spec.source_name, spec.output_name
                ),
            ));
        }
    }
    Ok(())
}

pub fn lower_scan_predicate(
    scan: &plan::ScanNode,
    arena: &mut ExprArena,
    layout: &Layout,
) -> Result<Option<ExprId>, NativeFragmentLeafDecodeError> {
    let mut predicate = None;
    for (idx, expr) in scan.predicates.iter().enumerate() {
        let expr_id = decode_expr_for_slot_layout(
            expr,
            FieldPath::root("scan").field("predicates").index(idx),
            arena,
            layout,
        )
        .map_err(|err| {
            NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::InvalidValue,
                "predicates",
                format!("ScanNode predicate {idx}: {err}"),
            )
            .append_index(idx)
        })?;
        predicate = Some(match predicate {
            Some(prev) => arena.push_typed(ExprNode::And(prev, expr_id), DataType::Boolean),
            None => expr_id,
        });
    }
    Ok(predicate)
}

pub fn parse_scan_limit(limit: i64) -> Result<Option<usize>, NativeFragmentLeafDecodeError> {
    if limit == -1 {
        Ok(None)
    } else if limit < 0 {
        Err(NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::OutOfRange,
            "limit",
            format!("ScanNode limit must be -1 or >= 0, got {limit}"),
        ))
    } else {
        Ok(Some(limit as usize))
    }
}
