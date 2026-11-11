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

//! Actual wire-occurrence backing preflight for one original registered reply layout.
//! This is a checked requested-backing envelope, not a replacement protobuf
//! interpreter. Prost still owns malformed input and all semantic decoding.
use super::{ResourcePreflightError, ScanError, ScanResult, Value, finish, for_fields, limit};
use novarocks_proto_models::{
    generated_resource_layouts, novarocks as wire,
    resource_layout::{GeneratedResourceLayout, ObjectLayout, RustLayout, Storage, WireField},
};

/// A layout-accounting bound for one original request. It neither reserves
/// physical memory nor establishes a process-wide reply capacity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OperationReplyDecodePlan {
    peak: usize,
    expected_receipts: usize,
    response_max_encoded_bytes: usize,
}
impl OperationReplyDecodePlan {
    pub fn for_response(
        expected_receipts: usize,
        response_max_encoded_bytes: usize,
    ) -> Result<Self, ResourcePreflightError> {
        if expected_receipts == 0 || response_max_encoded_bytes == 0 {
            return Err(ResourcePreflightError(
                "Native reply requires its original nonempty request and effective message limit",
            ));
        }
        let mut rate = 0usize;
        let root = wire::ApplyTaskOperationsResponse::RESOURCE_LAYOUT;
        quote_object(root, None, &mut rate)?;
        // Each local fixed charge is assigned to its field's own key byte;
        // leaf text/packed charges use only their non-recursive payload bytes.
        // Those bytes are disjoint even for repeated singular/oneof fields.
        // Keeping the root Vec charge in K is conservative and also covers
        // malformed prefixes; its n-specific array remains explicitly recorded.
        let receipts = array_backing(size_of::<wire::TaskOperationReceipt>(), expected_receipts)?;
        let peak = rate
            .checked_add(8)
            .and_then(|v| v.checked_mul(response_max_encoded_bytes))
            .and_then(|v| v.checked_add(receipts))
            .ok_or(ResourcePreflightError("Native reply layout bound overflow"))?;
        Ok(Self {
            peak,
            expected_receipts,
            response_max_encoded_bytes,
        })
    }
    pub fn peak_bytes(self) -> usize {
        self.peak
    }
    pub fn response_max_encoded_bytes(self) -> usize {
        self.response_max_encoded_bytes
    }
    pub fn check(self, raw: &[u8]) -> Result<(), ResourcePreflightError> {
        super::check_operation_reply(raw, self.expected_receipts, self.response_max_encoded_bytes)?;
        let mut charge = Charge {
            used: 0,
            maximum: self.peak,
        };
        charge
            .add(
                raw.len()
                    .checked_mul(8)
                    .ok_or(ResourcePreflightError("Native reply text backing overflow"))?,
            )
            .map_err(scan_error)?;
        finish(scan_object(
            wire::ApplyTaskOperationsResponse::RESOURCE_LAYOUT,
            raw,
            &mut charge,
            0,
        ))
    }
}
fn scan_error(error: ScanError) -> ResourcePreflightError {
    match error {
        ScanError::Limit(error) => error,
        ScanError::Malformed => unreachable!(),
    }
}
fn array_backing(size: usize, entries: usize) -> Result<usize, ResourcePreflightError> {
    size.checked_mul(entries)
        .and_then(|v| v.checked_mul(4))
        .ok_or(ResourcePreflightError(
            "Native reply vector backing overflow",
        ))
}
fn box_backing(size: usize) -> Result<usize, ResourcePreflightError> {
    size.checked_mul(2)
        .ok_or(ResourcePreflightError("Native reply Box backing overflow"))
}
fn text_backing(size: usize) -> Result<usize, ResourcePreflightError> {
    size.max(8)
        .checked_mul(4)
        .ok_or(ResourcePreflightError("Native reply text backing overflow"))
}
fn typed_size(id: &str) -> usize {
    use novarocks_execution_contract::{
        identity::TaskIdentity,
        operation::{ParticipationDomainReceipt, PlanNodeSplitReceipt, TaskDomainReceipt},
    };
    match id {
        "novarocks.TaskDomainReceipt" => size_of::<TaskDomainReceipt>(),
        "novarocks.TaskSplitPlanNodeReceipt" => size_of::<PlanNodeSplitReceipt>(),
        "novarocks.ParticipationDomainReceipt" => size_of::<ParticipationDomainReceipt>(),
        "novarocks.TaskIdentity" => size_of::<TaskIdentity>(),
        _ => 0,
    }
}
// Borrowed stack ancestry needs no heap allocation or arbitrary type-depth
// bound. A repeated Message edge is already covered by its local wire charge.
struct QuotePath<'a> {
    id: &'static str,
    parent: Option<&'a QuotePath<'a>>,
}
fn quote_object(
    layout: &'static ObjectLayout,
    parent: Option<&QuotePath<'_>>,
    rate: &mut usize,
) -> Result<(), ResourcePreflightError> {
    let mut cursor = parent;
    while let Some(path) = cursor {
        if path.id == layout.schema_id {
            return Ok(());
        }
        cursor = path.parent;
    }
    let path = QuotePath {
        id: layout.schema_id,
        parent,
    };
    for field in layout.fields {
        for wire in field.wire {
            let fixed = quote_storage(&field.rust, wire, &path, rate)?;
            *rate = (*rate).max(fixed);
        }
    }
    Ok(())
}
fn quote_storage(
    layout: &RustLayout,
    wire: &WireField,
    path: &QuotePath<'_>,
    rate: &mut usize,
) -> Result<usize, ResourcePreflightError> {
    let add = |a: usize, b: usize| {
        a.checked_add(b)
            .ok_or(ResourcePreflightError("Native reply local layout overflow"))
    };
    match layout.storage {
        Storage::Scalar => Ok(0),
        Storage::Option(child) => quote_storage(child, wire, path, rate),
        Storage::Box(child) => add(
            box_backing(child.size)?,
            quote_storage(child, wire, path, rate)?,
        ),
        Storage::String | Storage::Bytes => {
            *rate = (*rate).max(4);
            text_backing(0)
        }
        Storage::Message(id) => {
            let child = object(id).map_err(scan_error)?;
            quote_object(child, Some(path), rate)?;
            array_backing(typed_size(id), 1)
        }
        Storage::Oneof(id) => {
            let oneof = object(id).map_err(scan_error)?;
            let field = oneof
                .fields
                .iter()
                .find(|field| field.wire.iter().any(|v| v.number == wire.number))
                .ok_or(ResourcePreflightError(
                    "Native reply oneof has no original wire field",
                ))?;
            quote_storage(&field.rust, wire, path, rate)
        }
        Storage::Vec(child) => {
            if wire.kind == novarocks_proto_models::resource_layout::WireKind::Bytes {
                *rate = (*rate).max(4);
                return text_backing(0);
            }
            let mut cost = array_backing(child.size, 1)?;
            if layout_contains_open_edges(wire) {
                cost = add(
                    cost,
                    array_backing(
                        size_of::<novarocks_execution_contract::domain::ExchangeEdgeId>(),
                        1,
                    )?,
                )?;
            }
            if wire.packed {
                *rate = (*rate).max(cost);
                Ok(cost)
            } else {
                add(cost, quote_storage(child, wire, path, rate)?)
            }
        }
        Storage::BTreeMap { .. } | Storage::HashMap { .. } => Err(ResourcePreflightError(
            "Native reply map lacks an original backing author",
        )),
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
            return Err(limit("Native reply exceeds its registered layout bound"));
        }
        Ok(())
    }
    fn array(&mut self, size: usize, entries: usize) -> ScanResult {
        self.add(array_backing(size, entries).map_err(ScanError::Limit)?)
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
    let typed = typed_size(layout.schema_id);
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
            charge.add(box_backing(child.size).map_err(ScanError::Limit)?)?;
            scan_storage(child, wire, value, charge, depth)
        }
        Storage::String | Storage::Bytes => {
            if let Value::Bytes(raw) = value {
                charge.add(text_backing(raw.len()).map_err(ScanError::Limit)?)?
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
                    charge.add(text_backing(raw.len()).map_err(ScanError::Limit)?)?
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

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_execution_contract::identity::TaskOperationId;
    use novarocks_execution_contract::operation::{OperationOutcome, OperationResult};
    use prost::Message;

    fn message(tag: u32, bytes: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        prost::encoding::encode_varint(((tag << 3) | 2) as u64, &mut out);
        prost::encoding::encode_varint(bytes.len() as u64, &mut out);
        out.extend_from_slice(bytes);
        out
    }

    // Correctness of the original reply layout model, without network or a
    // second byte budget. Prost and the canonical header decoder are the
    // independent wire/semantic oracle; ordinary owner tests maintain it.
    #[test]
    fn registered_model_decodes_a_default_reply_above_one_eighth_of_its_wire_limit() {
        let id = TaskOperationId::new_v7();
        let receipt = crate::operation::encode_receipt(
            id,
            OperationResult::Verdict {
                outcome: OperationOutcome::OperationTimedOut,
                detail: None,
                acknowledgement: None,
            },
        );
        let Some(wire::task_operation_receipt::Result::Verdict(verdict)) = receipt.result else {
            panic!("verdict");
        };
        // Repeated empty singular strings are legal protobuf. The final
        // visible value remains the same empty, redacted producer detail.
        let mut details = [0x12, 0x00].repeat(9 * 1024 * 1024 / 2);
        details.extend(verdict.encode_to_vec());
        let mut receipt_bytes = wire::TaskOperationReceipt {
            operation_id: receipt.operation_id,
            result: None,
        }
        .encode_to_vec();
        receipt_bytes.extend(message(2, &details));
        let raw = message(1, &receipt_bytes);
        let maximum = crate::TransportMessageLimits::DEFAULT.ordinary_response_max_bytes;
        assert!(raw.len() > maximum / 8 && raw.len() <= maximum);
        let plan = OperationReplyDecodePlan::for_response(1, maximum).unwrap();
        plan.check(&raw).unwrap();
        let decoded = wire::ApplyTaskOperationsResponse::decode(raw.as_slice()).unwrap();
        let headers = crate::operation::decode_receipt_batch(
            &decoded,
            &[id],
            novarocks_proto_codec::FieldPath::root("reply"),
        )
        .unwrap();
        assert_eq!(headers.len(), 1);
    }

    #[test]
    fn original_count_and_effective_wire_limit_are_checked_before_decode() {
        let raw = wire::ApplyTaskOperationsResponse {
            receipts: vec![wire::TaskOperationReceipt::default(); 2],
        }
        .encode_to_vec();
        assert!(
            OperationReplyDecodePlan::for_response(1, raw.len())
                .unwrap()
                .check(&raw)
                .is_err()
        );
        assert!(
            OperationReplyDecodePlan::for_response(2, raw.len() - 1)
                .unwrap()
                .check(&raw)
                .is_err()
        );
        assert!(
            OperationReplyDecodePlan::for_response(2, raw.len())
                .unwrap()
                .check(&raw)
                .is_ok()
        );
        assert!(OperationReplyDecodePlan::for_response(0, 1024).is_err());
    }

    #[test]
    fn layout_quote_is_accounting_and_uses_checked_representation() {
        let maximum = crate::TransportMessageLimits::DEFAULT.ordinary_response_max_bytes;
        let plan = OperationReplyDecodePlan::for_response(1, maximum).unwrap();
        assert!(plan.peak_bytes() >= maximum * 8);
        assert_eq!(plan.response_max_encoded_bytes(), maximum);
        assert!(OperationReplyDecodePlan::for_response(1, usize::MAX).is_err());
    }

    #[test]
    fn layout_bound_covers_repeated_typed_identity_and_generated_arrays_in_both_wire_forms() {
        use novarocks_execution_contract::identity::TaskIdentity;
        use novarocks_types::identity::{
            AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
        };
        let identity = TaskIdentity::new(
            QueryExecutionId::new(QueryId::new(1, 2), AttemptId::new(1).unwrap()).unwrap(),
            StageId::new(1).unwrap(),
            TaskId::new(1).unwrap(),
            BackendProcessId::new_v7(),
        );
        let id = TaskOperationId::new_v7();
        for packed in [false, true] {
            let mut edges = Vec::new();
            for value in 1..=crate::domain::MAX_OPEN_EDGES {
                if !packed {
                    edges.push(0x08);
                }
                prost::encoding::encode_varint(value as u64, &mut edges);
            }
            let mut open = if packed { message(1, &edges) } else { edges };
            open.extend([0x10, 0x01]);
            let mut domain = message(3, &open);
            domain.extend([0x20, 0x01]);
            // Singular nested messages merge. Each original occurrence is
            // counted even though Prost retains only the final identity.
            let mut ack = message(1, &[]).repeat(1024);
            ack.extend(message(
                1,
                &crate::identity::encode_task_identity(identity).encode_to_vec(),
            ));
            ack.extend(message(2, &domain));
            let mut verdict = wire::TaskOperationVerdict {
                outcome: wire::TaskOperationOutcome::Accepted as i32,
                ..Default::default()
            }
            .encode_to_vec();
            verdict.extend(message(6, &ack));
            let mut receipt = wire::TaskOperationReceipt {
                operation_id: Some(crate::identity::encode_task_operation_id(id)),
                result: None,
            }
            .encode_to_vec();
            receipt.extend(message(2, &verdict));
            let raw = message(1, &receipt);
            let plan = OperationReplyDecodePlan::for_response(1, raw.len()).unwrap();
            plan.check(&raw).unwrap();
            let mut charge = Charge {
                used: raw.len() * 8,
                maximum: plan.peak_bytes(),
            };
            finish(scan_object(
                wire::ApplyTaskOperationsResponse::RESOURCE_LAYOUT,
                &raw,
                &mut charge,
                0,
            ))
            .unwrap();
            assert!(charge.used >= 1025 * array_backing(size_of::<TaskIdentity>(), 1).unwrap());
            let decoded = wire::ApplyTaskOperationsResponse::decode(raw.as_slice()).unwrap();
            crate::operation::decode_receipt_batch(
                &decoded,
                &[id],
                novarocks_proto_codec::FieldPath::root("reply"),
            )
            .unwrap();
            let Some(wire::task_operation_receipt::Result::Verdict(verdict)) =
                &decoded.receipts[0].result
            else {
                panic!("verdict");
            };
            let Some(wire::task_operation_verdict::Ack::UpdateTask(ack)) = &verdict.ack else {
                panic!("update acknowledgement");
            };
            let typed = crate::operation::decode_update_task_ack(
                ack,
                identity,
                novarocks_proto_codec::FieldPath::root("ack"),
            )
            .unwrap();
            assert_eq!(typed.identity(), identity);
            assert_eq!(ack.accepted_domains.len(), 1);
        }
    }
}
