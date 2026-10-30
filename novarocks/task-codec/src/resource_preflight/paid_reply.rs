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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Actual wire-occurrence backing preflight for one already-paid reply plan.
//! This is a checked requested-backing envelope, not a replacement protobuf
//! interpreter. Prost still owns malformed input and all semantic decoding.
use super::{ResourcePreflightError, ScanError, ScanResult, Value, finish, for_fields, limit};
use novarocks_proto_models::{
    generated_resource_layouts, novarocks as wire,
    resource_layout::{GeneratedResourceLayout, ObjectLayout, RustLayout, Storage, WireField},
};

/// The original request creator chooses this bound before its effect. It is
/// not a grant: its entire backing is included in the same Joint reply claim.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OperationReplyDecodePlan {
    peak: usize,
}
impl OperationReplyDecodePlan {
    pub fn for_transport(budget: crate::TransportBudget) -> Self {
        // Reuse the actual configured per-action byte policy; do not require
        // every grammar alternative's theoretical maximum simultaneously.
        Self {
            peak: budget.max_batch_encoded_bytes(),
        }
    }
    pub fn peak_bytes(self) -> usize {
        self.peak
    }
    pub fn check(self, raw: &[u8]) -> Result<(), ResourcePreflightError> {
        super::check_operation_reply(raw)?;
        let mut charge = Charge {
            used: 0,
            maximum: self.peak,
        };
        // Owned text projections/SafeDetail coexist with the generated tree.
        // Replaced and duplicate singular occurrences are counted, never only
        // the final visible value. Raw receive backing is charged separately.
        charge
            .add(
                raw.len()
                    .checked_mul(8)
                    .ok_or(ResourcePreflightError("Native reply text backing overflow"))?,
            )
            .map_err(|error| match error {
                ScanError::Limit(error) => error,
                ScanError::Malformed => unreachable!(),
            })?;
        finish(scan_object(
            wire::ApplyTaskOperationsResponse::RESOURCE_LAYOUT,
            raw,
            &mut charge,
            0,
        ))
    }
}
struct Charge {
    used: usize,
    maximum: usize,
}
impl Charge {
    fn add(&mut self, bytes: usize) -> ScanResult {
        self.used = self
            .used
            .checked_add(bytes)
            .ok_or_else(|| limit("Native reply backing overflow"))?;
        if self.used > self.maximum {
            return Err(limit("Native reply exceeds its paid backing plan"));
        }
        Ok(())
    }
    fn array(&mut self, size: usize, entries: usize) -> ScanResult {
        self.add(
            size.checked_mul(entries)
                .and_then(|v| v.checked_mul(4))
                .ok_or_else(|| limit("Native reply vector backing overflow"))?,
        )
    }
}
fn object(id: &str) -> Result<&'static ObjectLayout, ScanError> {
    generated_resource_layouts()
        .find(|layout| layout.schema_id == id)
        .ok_or_else(|| limit("Native reply has an unsupported generated backing shape"))
}
fn scan_object(layout: &ObjectLayout, raw: &[u8], charge: &mut Charge, depth: usize) -> ScanResult {
    if depth > 32 {
        return Err(limit("Native reply backing depth exceeds its source"));
    }
    // Count every typed array member while scanning the actual corresponding
    // generated occurrence. Earlier canonical receipts remain paid by this
    // same envelope; terminal telemetry is moved into its canonical cell.
    use novarocks_execution_contract::{
        identity::TaskIdentity,
        operation::{PlanNodeSplitReceipt, QueryContextDomainReceipt, TaskDomainReceipt},
    };
    let typed = match layout.schema_id {
        "novarocks.TaskDomainReceipt" => size_of::<TaskDomainReceipt>(),
        "novarocks.TaskSplitPlanNodeReceipt" => size_of::<PlanNodeSplitReceipt>(),
        "novarocks.QueryContextDomainReceipt" => size_of::<QueryContextDomainReceipt>(),
        "novarocks.TaskIdentity" => size_of::<TaskIdentity>(),
        _ => 0,
    };
    if typed != 0 {
        charge.array(typed, 1)?
    }
    for_fields(raw, |tag, value| {
        let Some((field, wire)) = layout.fields.iter().find_map(|field| {
            field
                .wire
                .iter()
                .find(|wire| wire.number == tag)
                .map(|wire| (field, wire))
        }) else {
            return Ok(());
        };
        scan_storage(&field.rust, wire, value, charge, depth)
    })
}
fn scan_storage(
    layout: &RustLayout,
    wire: &WireField,
    value: Value<'_>,
    charge: &mut Charge,
    depth: usize,
) -> ScanResult {
    match layout.storage {
        Storage::Scalar => Ok(()),
        Storage::Option(child) => scan_storage(child, wire, value, charge, depth),
        Storage::Box(child) => {
            // Replacement may coexist with the previous boxed value.
            charge.add(
                child
                    .size
                    .checked_mul(2)
                    .ok_or_else(|| limit("Native reply Box backing overflow"))?,
            )?;
            scan_storage(child, wire, value, charge, depth)
        }
        Storage::String | Storage::Bytes => {
            if let Value::Bytes(raw) = value {
                charge.add(
                    raw.len()
                        .max(8)
                        .checked_mul(4)
                        .ok_or_else(|| limit("Native reply string backing overflow"))?,
                )?
            }
            Ok(())
        }
        Storage::Message(id) => {
            if let Value::Bytes(raw) = value {
                scan_object(object(id)?, raw, charge, depth + 1)
            } else {
                Ok(())
            }
        }
        Storage::Oneof(id) => {
            let oneof = object(id)?;
            let Some(field) = oneof
                .fields
                .iter()
                .find(|field| field.wire.iter().any(|v| v.number == wire.number))
            else {
                return Err(limit(
                    "Native reply oneof layout does not match its original field",
                ));
            };
            scan_storage(&field.rust, wire, value, charge, depth + 1)
        }
        Storage::Vec(child) => {
            if wire.kind == novarocks_proto_models::resource_layout::WireKind::Bytes {
                if let Value::Bytes(raw) = value {
                    charge.add(
                        raw.len()
                            .max(8)
                            .checked_mul(4)
                            .ok_or_else(|| limit("Native reply bytes backing overflow"))?,
                    )?
                }
                return Ok(());
            }
            // For packed scalar values the raw byte length bounds their count
            // including zero/noncanonical varints. Four requests per entry
            // cover Vec minimum capacity and moving old+new growth coexistence.
            let entries = if wire.packed {
                match value {
                    Value::Bytes(raw) => raw.len(),
                    _ => 1,
                }
            } else {
                1
            };
            charge.array(child.size, entries)?;
            if layout_contains_open_edges(wire) {
                charge.array(
                    size_of::<novarocks_execution_contract::domain::ExchangeEdgeId>(),
                    entries,
                )?;
            }
            if !wire.packed {
                scan_storage(child, wire, value, charge, depth + 1)?
            }
            Ok(())
        }
        Storage::BTreeMap { .. } | Storage::HashMap { .. } => {
            Err(limit("Native reply map lacks an original backing author"))
        }
    }
}
fn layout_contains_open_edges(wire: &WireField) -> bool {
    wire.name == "opened_edge_ids"
}
