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

//! Small, allocation-free resource checks before prost builds Native task objects.
//!
//! This scanner recognizes only fields needed to bound repeated object expansion
//! and task metadata. It does not decide protobuf encoding legality or
//! business semantics. A malformed cursor is left to prost, which remains the
//! sole protobuf interpreter; unknown fields and noncanonical varints are not
//! rejected here. The typed codec still validates every semantic relationship.

use std::fmt;

mod paid_reply;
pub use paid_reply::OperationReplyDecodePlan;
mod operation_reply;
pub use operation_reply::check_operation_reply;

use crate::TransportBudget;
use crate::creation::MAX_INITIAL_SCAN_NODES;
use crate::descriptor::{
    MAX_EDGE_DESTINATIONS, MAX_INBOUND_SOURCES, MAX_SPLIT_PLAN_NODES, MAX_TOPOLOGY_ENTRIES,
};
use crate::domain::{MAX_DOMAIN_UPDATES, MAX_OPEN_EDGES};

const MAX_STATUS_CURSORS: usize = 4096;
// Three distinct repeated target projections may legitimately name the same
// 4096 tasks. The typed decoder checks the exact identity union after prost.
const MAX_STATUS_SUBSCRIPTION_OBJECTS: usize = MAX_STATUS_CURSORS * 3;
const MAX_STATUS_SUBSCRIPTION_BYTES: usize = 16 * 1024 * 1024;
const MAX_QUIESCE_CURSOR_BYTES: usize = 4096;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResourcePreflightError(&'static str);

impl fmt::Display for ResourcePreflightError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl std::error::Error for ResourcePreflightError {}

#[derive(Debug)]
enum ScanError {
    Malformed,
    Limit(ResourcePreflightError),
}

type ScanResult = Result<(), ScanError>;

fn finish(result: ScanResult) -> Result<(), ResourcePreflightError> {
    match result {
        Ok(()) | Err(ScanError::Malformed) => Ok(()),
        Err(ScanError::Limit(error)) => Err(error),
    }
}

fn limit(message: &'static str) -> ScanError {
    ScanError::Limit(ResourcePreflightError(message))
}

/// Checks the repeated operation count before prost constructs a task batch.
pub fn check_operation_batch(raw: &[u8]) -> Result<(), ResourcePreflightError> {
    if raw.len() > TransportBudget::DEFAULT.max_batch_encoded_bytes() {
        return Err(ResourcePreflightError(
            "task operation batch exceeds 48 MiB",
        ));
    }
    finish(scan_batch(raw, false))
}

/// Checks the closed control method's repeated operation count.
pub fn check_control_operation_batch(raw: &[u8]) -> Result<(), ResourcePreflightError> {
    finish(scan_batch(raw, true))
}

/// Checks every repeated covered target before prost allocates its objects.
pub fn check_status_subscription(raw: &[u8]) -> Result<(), ResourcePreflightError> {
    if raw.len() > MAX_STATUS_SUBSCRIPTION_BYTES {
        return Err(ResourcePreflightError("status subscription exceeds 16 MiB"));
    }
    finish(scan_status_subscription(raw))
}

/// Bounds a root read before prost builds its fixed identity and kind.
pub fn check_root_result_read(raw: &[u8]) -> Result<(), ResourcePreflightError> {
    if raw.len() > novarocks_result_contract::RootProfileV1::ENVELOPE_BYTES {
        return Err(ResourcePreflightError("root read exceeds fixed envelope"));
    }
    Ok(())
}
/// Flat body and fixed metadata are separate bounds. Count every occurrence,
/// including repeated singular messages that prost merges.
pub fn check_root_result_reply(raw: &[u8]) -> Result<(), ResourcePreflightError> {
    use novarocks_result_contract::RootProfileV1;
    if raw.len() > RootProfileV1::SEGMENT_BYTES + RootProfileV1::ENVELOPE_BYTES {
        return Err(ResourcePreflightError(
            "root reply exceeds segment and envelope",
        ));
    }
    let mut body_bytes = 0usize;
    let mut data_objects = 0usize;
    finish(for_fields(raw, |field, value| {
        if field == 4
            && let Value::Bytes(receipt) = value
        {
            for_fields(receipt, |field, value| {
                if field == 6
                    && let Value::Bytes(data) = value
                {
                    checked_increment(&mut data_objects, 1, "root reply repeats Data objects")?;
                    for_fields(data, |field, value| {
                        if field == 2
                            && let Value::Bytes(body) = value
                        {
                            body_bytes = body_bytes
                                .checked_add(body.len())
                                .ok_or_else(|| limit("root body size overflow"))?;
                            if body_bytes > RootProfileV1::SEGMENT_BYTES {
                                return Err(limit("root reply body exceeds segment"));
                            }
                        }
                        Ok(())
                    })?;
                }
                Ok(())
            })?;
        }
        Ok(())
    }))?;
    if raw.len().saturating_sub(body_bytes) > RootProfileV1::ENVELOPE_BYTES {
        return Err(ResourcePreflightError(
            "root reply metadata exceeds fixed envelope",
        ));
    }
    Ok(())
}
/// Root schema vocabulary has no live capability. Bound protobuf object
/// expansion and depth before decoding the canonical semantic projection.
pub fn check_client_render_schema(raw: &[u8]) -> Result<(), ResourcePreflightError> {
    finish(RenderSchemaScan::new().scan(raw))
}
struct RenderSchemaScan {
    columns: usize,
    nodes: usize,
    wire_bytes: usize,
    #[cfg(test)]
    root_wire_bytes: usize,
    scalar_initialized: bool,
    scalar_nodes: usize,
    expansion: RenderExpansion,
}
impl RenderSchemaScan {
    fn new() -> Self {
        Self {
            columns: 0,
            nodes: 0,
            wire_bytes: 0,
            #[cfg(test)]
            root_wire_bytes: 0,
            scalar_initialized: false,
            scalar_nodes: 0,
            expansion: RenderExpansion {
                backing: size_of::<novarocks_proto_models::result::ClientRenderSchema>(),
                growth_old: 0,
            },
        }
    }
    fn scan(&mut self, raw: &[u8]) -> ScanResult {
        use novarocks_proto_models::result as wire;
        use novarocks_result_contract::RootProfileV1;
        // The complete schema includes both family alternatives (fields 3/4).
        // Their inline bool facts allocate no separate backing; raw bytes still
        // participate in the wire and simultaneous decode-overlap allowances.
        self.wire_bytes = self
            .wire_bytes
            .checked_add(raw.len())
            .ok_or_else(|| limit("render schema size overflow"))?;
        if self.wire_bytes > RootProfileV1::SCHEMA_WIRE_BYTES {
            return Err(limit("render schema exceeds wire profile"));
        }
        self.expansion.backing = self
            .expansion
            .backing
            .checked_add(raw.len())
            .ok_or_else(|| limit("render schema expansion overflow"))?;
        self.expansion.check()?;
        let Self {
            columns,
            nodes,
            expansion,
            ..
        } = self;
        for_fields(raw, |field, value| {
            match (field, value) {
                (1, Value::Bytes(column)) => {
                    checked_increment(
                        columns,
                        RootProfileV1::MAX_COLUMNS,
                        "render schema exceeds column profile",
                    )?;
                    expansion.grow_vec(*columns, size_of::<wire::RenderColumn>())?;
                    for_fields(column, |field, value| {
                        if let (3, Value::Bytes(name)) = (field, value) {
                            if name.len() > RootProfileV1::MAX_NAME_BYTES {
                                return Err(limit("render column name exceeds profile"));
                            }
                            expansion.string(name.len())?;
                        }
                        Ok(())
                    })?;
                }
                (2, Value::Bytes(node)) => {
                    checked_increment(
                        nodes,
                        RootProfileV1::SCHEMA_TYPE_NODES,
                        "render schema exceeds decoded node profile",
                    )?;
                    expansion.grow_vec(*nodes, size_of::<wire::RenderField>())?;
                    scan_render_field(node, expansion)?;
                }
                _ => {}
            }
            Ok(())
        })
    }
    fn scan_scalar(&mut self, raw: &[u8]) -> ScanResult {
        use novarocks_proto_models::result as wire;
        use novarocks_result_contract::RootProfileV1;
        self.wire_bytes = self
            .wire_bytes
            .checked_add(raw.len())
            .ok_or_else(|| limit("scalar schema size overflow"))?;
        if self.wire_bytes > RootProfileV1::SCHEMA_WIRE_BYTES {
            return Err(limit("scalar schema exceeds wire profile"));
        }
        if !self.scalar_initialized {
            self.expansion.backing = self
                .expansion
                .backing
                .checked_add(size_of::<wire::ScalarSchema>())
                .ok_or_else(|| limit("scalar schema expansion overflow"))?;
            self.scalar_initialized = true;
        }
        self.expansion.backing = self
            .expansion
            .backing
            .checked_add(raw.len())
            .ok_or_else(|| limit("scalar schema expansion overflow"))?;
        self.expansion.check()?;
        for_fields(raw, |field, value| {
            if let (3, Value::Bytes(node)) = (field, value) {
                checked_increment(
                    &mut self.scalar_nodes,
                    RootProfileV1::SCHEMA_TYPE_NODES,
                    "scalar schema exceeds decoded node profile",
                )?;
                self.expansion
                    .grow_vec(self.scalar_nodes, size_of::<wire::ScalarField>())?;
                scan_scalar_field(node, &mut self.expansion)?;
            }
            Ok(())
        })
    }
    #[cfg(test)]
    fn scan_root(&mut self, raw: &[u8]) -> ScanResult {
        use novarocks_result_contract::RootProfileV1;
        self.root_wire_bytes = self
            .root_wire_bytes
            .checked_add(raw.len())
            .ok_or_else(|| limit("root contract size overflow"))?;
        if self.root_wire_bytes > RootProfileV1::SCHEMA_WIRE_BYTES + RootProfileV1::ENVELOPE_BYTES {
            return Err(limit("root output contract exceeds its profile"));
        }
        for_fields(raw, |field, value| {
            match (field, value) {
                (3, Value::Bytes(schema)) => self.scan(schema)?,
                (4, Value::Bytes(schema)) => self.scan_scalar(schema)?,
                _ => {}
            }
            Ok(())
        })
    }
}

struct RenderExpansion {
    backing: usize,
    growth_old: usize,
}
impl RenderExpansion {
    fn grow_vec(&mut self, count: usize, item_bytes: usize) -> ScanResult {
        // prost repeated-message decode pushes into an initially empty Vec.
        // These DTO elements use Rust's four-element minimum capacity.
        let capacity = count
            .max(4)
            .checked_next_power_of_two()
            .ok_or_else(|| limit("render schema capacity overflow"))?;
        let previous = if count == 1 {
            0
        } else {
            (count - 1)
                .max(4)
                .checked_next_power_of_two()
                .ok_or_else(|| limit("render schema capacity overflow"))?
        };
        if capacity != previous {
            let added = (capacity - previous)
                .checked_mul(item_bytes)
                .ok_or_else(|| limit("render schema expansion overflow"))?;
            self.backing = self
                .backing
                .checked_add(added)
                .ok_or_else(|| limit("render schema expansion overflow"))?;
            self.growth_old = self.growth_old.max(
                previous
                    .checked_mul(item_bytes)
                    .ok_or_else(|| limit("render schema expansion overflow"))?,
            );
        }
        self.check()
    }
    fn string(&mut self, bytes: usize) -> ScanResult {
        if bytes != 0 {
            let capacity = bytes
                .max(8)
                .checked_next_power_of_two()
                .ok_or_else(|| limit("render string capacity overflow"))?;
            // Repeated string replacements retain their old capacity while
            // reserve may allocate its replacement. Counting every occurrence
            // covers that overlap without relying on semantic validity.
            self.backing = self
                .backing
                .checked_add(capacity)
                .ok_or_else(|| limit("render string capacity overflow"))?;
        }
        self.check()
    }
    fn check(&self) -> ScanResult {
        if self
            .backing
            .checked_add(self.growth_old)
            .is_none_or(|peak| {
                peak > novarocks_result_contract::RootProfileV1::SCHEMA_BACKING_BYTES
            })
        {
            return Err(limit("render schema exceeds decoded backing profile"));
        }
        Ok(())
    }
}
fn scan_render_field(raw: &[u8], expansion: &mut RenderExpansion) -> ScanResult {
    use novarocks_proto_models::result as wire;
    use novarocks_result_contract::RootProfileV1;
    let mut children = 0usize;
    for_fields(raw, |field, value| {
        if let (2, Value::Bytes(ty)) = (field, value) {
            for_fields(ty, |field, value| {
                match (field, value) {
                    (6, Value::Bytes(zone)) => {
                        if zone.len() > RootProfileV1::MAX_NAME_BYTES {
                            return Err(limit("render timezone exceeds profile"));
                        }
                        expansion.string(zone.len())?;
                    }
                    (8, Value::Bytes(child)) => {
                        checked_increment(
                            &mut children,
                            RootProfileV1::MAX_COLUMNS,
                            "render type exceeds child profile",
                        )?;
                        // Even an empty or malformed child allocates a DTO.
                        expansion.grow_vec(children, size_of::<wire::NamedRenderField>())?;
                        for_fields(child, |field, value| {
                            if let (1, Value::Bytes(name)) = (field, value) {
                                if name.len() > RootProfileV1::MAX_NAME_BYTES {
                                    return Err(limit("render child name exceeds profile"));
                                }
                                expansion.string(name.len())?;
                            }
                            Ok(())
                        })?;
                    }
                    _ => {}
                }
                Ok(())
            })?;
        }
        Ok(())
    })
}

/// Allocation preflight only, before prost builds the private scalar schema.
/// Business validation (sole slot, exact type, tree reachability) is separate.
pub fn check_scalar_schema(raw: &[u8]) -> Result<(), ResourcePreflightError> {
    finish(RenderSchemaScan::new().scan_scalar(raw))
}
fn scan_scalar_field(raw: &[u8], expansion: &mut RenderExpansion) -> ScanResult {
    use novarocks_proto_models::result as wire;
    use novarocks_result_contract::RootProfileV1;
    let mut children = 0usize;
    // All occurrences of singular nested messages merge into the same Vec;
    // never reset its count at a duplicate value_type declaration.
    for_fields(raw, |field, value| {
        if let (2, Value::Bytes(ty)) = (field, value) {
            for_fields(ty, |field, value| {
                match (field, value) {
                    (6, Value::Bytes(zone)) => {
                        if zone.len() > RootProfileV1::MAX_NAME_BYTES {
                            return Err(limit("scalar timezone exceeds profile"));
                        }
                        expansion.string(zone.len())?;
                    }
                    (8, Value::Bytes(child)) => {
                        checked_increment(
                            &mut children,
                            RootProfileV1::MAX_COLUMNS,
                            "scalar type exceeds child profile",
                        )?;
                        expansion.grow_vec(children, size_of::<wire::NamedScalarField>())?;
                        for_fields(child, |field, value| {
                            if let (1, Value::Bytes(name)) = (field, value) {
                                if name.len() > RootProfileV1::MAX_NAME_BYTES {
                                    return Err(limit("scalar child name exceeds profile"));
                                }
                                expansion.string(name.len())?;
                            }
                            Ok(())
                        })?;
                    }
                    _ => {}
                }
                Ok(())
            })?;
        }
        Ok(())
    })
}

fn scan_status_subscription(raw: &[u8]) -> ScanResult {
    let mut status_cursors = 0;
    let mut convergence_cursors = 0;
    let mut required_identities = 0;
    let mut quiesce_cursors = 0;
    let mut combined = 0;
    for_fields(raw, |field, value| {
        let Value::Bytes(bytes) = value else {
            return Ok(());
        };
        let (count, message) = match field {
            2 => (
                &mut status_cursors,
                "status subscription exceeds 4096 cursors",
            ),
            5 => (
                &mut convergence_cursors,
                "status subscription exceeds 4096 convergence cursors",
            ),
            6 => (
                &mut required_identities,
                "status subscription exceeds 4096 required identities",
            ),
            7 => {
                checked_increment(
                    &mut quiesce_cursors,
                    1,
                    "status subscription repeats quiesce cursor",
                )?;
                if bytes.len() > MAX_QUIESCE_CURSOR_BYTES {
                    return Err(limit(
                        "status subscription quiesce cursor exceeds 4096 bytes",
                    ));
                }
                return Ok(());
            }
            _ => return Ok(()),
        };
        checked_increment(count, MAX_STATUS_CURSORS, message)?;
        checked_increment(
            &mut combined,
            MAX_STATUS_SUBSCRIPTION_OBJECTS,
            "status subscription exceeds combined target object bound",
        )
    })
}

/// Checks the static creation carrier before decoding its protobuf object.
pub fn check_frozen_fragment(raw: &[u8]) -> Result<(), ResourcePreflightError> {
    if raw.len() > TransportBudget::DEFAULT.max_descriptor_encoded_bytes() {
        return Err(ResourcePreflightError("frozen fragment exceeds 16 MiB"));
    }
    Ok(())
}

/// Checks the task-local carrier and the two carriers' combined plan bytes
/// before decoding the metadata protobuf object.
pub fn check_creation_metadata(
    raw: &[u8],
    frozen_raw_bytes: usize,
) -> Result<(), ResourcePreflightError> {
    finish(scan_creation_metadata(raw, frozen_raw_bytes))
}

fn checked_increment(count: &mut usize, max: usize, message: &'static str) -> ScanResult {
    *count += 1;
    if *count > max {
        return Err(limit(message));
    }
    Ok(())
}

fn scan_repeated_message_field(
    raw: &[u8],
    field_number: u32,
    max: usize,
    message: &'static str,
) -> ScanResult {
    let mut count = 0;
    for_fields(raw, |field, value| {
        if field == field_number && matches!(value, Value::Bytes(_)) {
            checked_increment(&mut count, max, message)?;
        }
        Ok(())
    })
}

fn scan_batch(raw: &[u8], control: bool) -> ScanResult {
    let mut count = 0;
    for_fields(raw, |field, value| {
        if field == 1
            && let Value::Bytes(operation) = value
        {
            checked_increment(
                &mut count,
                TransportBudget::DEFAULT.max_batch_items(),
                "task operation batch exceeds 32 items",
            )?;
            if !control {
                scan_operation(operation)?;
            } else {
                let mut count = 0;
                for_fields(operation, |field, value| {
                    if let (7, Value::Bytes(close)) = (field, value) {
                        for_fields(close, |field, value| {
                            if let (2, Value::Bytes(_)) = (field, value) {
                                checked_increment(
                                    &mut count,
                                    MAX_DOMAIN_UPDATES,
                                    "close projection exceeds 256 destinations",
                                )?;
                            }
                            Ok(())
                        })?;
                    }
                    Ok(())
                })?;
            }
        }
        Ok(())
    })
}

fn scan_operation(raw: &[u8]) -> ScanResult {
    let mut update_domains = 0;
    for_fields(raw, |field, value| {
        match (field, value) {
            (3, Value::Bytes(update)) => scan_update_task(update, &mut update_domains)?,
            (4, Value::Bytes(update)) => scan_update_context(update)?,
            _ => {}
        }
        Ok(())
    })
}

fn scan_update_task(raw: &[u8], count: &mut usize) -> ScanResult {
    // Repeated instances of the same oneof message merge in prost.
    for_fields(raw, |field, value| {
        if field == 2
            && let Value::Bytes(domain) = value
        {
            checked_increment(count, MAX_DOMAIN_UPDATES, "task update exceeds 256 domains")?;
            scan_task_domain(domain)?;
        }
        Ok(())
    })
}

fn scan_task_domain(raw: &[u8]) -> ScanResult {
    for_fields(raw, |field, value| {
        if field == 3
            && let Value::Bytes(open) = value
        {
            let mut edges = 0;
            count_repeated_varints(
                open,
                2,
                &mut edges,
                MAX_OPEN_EDGES,
                "open exchange domain exceeds 256 edges",
            )?;
        }
        Ok(())
    })
}

fn scan_update_context(raw: &[u8]) -> ScanResult {
    for_fields(raw, |field, value| {
        match (field, value) {
            (1, Value::Bytes(establish)) => {
                let mut descriptors = 0;
                for_fields(establish, |field, value| {
                    if field == 4
                        && let Value::Bytes(credential) = value
                    {
                        scan_credential_domain(credential, &mut descriptors)?;
                    }
                    Ok(())
                })?;
            }
            (2, Value::Bytes(advance)) => {
                let mut descriptors = 0;
                for_fields(advance, |field, value| {
                    if field == 2
                        && let Value::Bytes(domain) = value
                    {
                        for_fields(domain, |field, value| {
                            if field == 3
                                && let Value::Bytes(credential) = value
                            {
                                scan_credential_domain(credential, &mut descriptors)?;
                            }
                            Ok(())
                        })?;
                    }
                    Ok(())
                })?;
            }
            _ => {}
        }
        Ok(())
    })
}

fn scan_credential_domain(raw: &[u8], count: &mut usize) -> ScanResult {
    for_fields(raw, |field, value| {
        if field == 3 && matches!(value, Value::Bytes(_)) {
            checked_increment(
                count,
                crate::domain::MAX_CREDENTIAL_DESCRIPTORS,
                "query credential domain exceeds 64 descriptors",
            )?;
        }
        Ok(())
    })
}

#[derive(Default)]
struct MetadataCounts {
    assignment_bytes: usize,
    initial_domains: usize,
    split_nodes: usize,
    outbound_edges: usize,
    inbound_nodes: usize,
    initial_scan_nodes: usize,
    sink_edges: usize,
}

fn scan_creation_metadata(raw: &[u8], frozen_raw_bytes: usize) -> ScanResult {
    let mut counts = MetadataCounts::default();
    for_fields(raw, |field, value| {
        match (field, value) {
            (2, Value::Bytes(descriptor)) => scan_descriptor(descriptor, &mut counts)?,
            // The static fragment and the task assignment together describe
            // what one task runs, so they share one plan-carrier bound. Every
            // occurrence counts: prost merges repeated singular messages.
            (5, Value::Bytes(assignment)) => {
                counts.assignment_bytes = counts
                    .assignment_bytes
                    .checked_add(assignment.len())
                    .ok_or_else(|| limit("creation plan carriers exceed 16 MiB"))?;
                if frozen_raw_bytes
                    .checked_add(counts.assignment_bytes)
                    .is_none_or(|total| {
                        total > TransportBudget::DEFAULT.max_descriptor_encoded_bytes()
                    })
                {
                    return Err(limit("creation plan carriers exceed 16 MiB"));
                }
                scan_assignment(assignment, &mut counts)?;
            }
            (4, Value::Bytes(domain)) => {
                checked_increment(
                    &mut counts.initial_domains,
                    MAX_DOMAIN_UPDATES,
                    "creation metadata exceeds 256 initial domains",
                )?;
                scan_task_domain(domain)?;
            }
            _ => {}
        }
        Ok(())
    })
}

fn scan_descriptor(raw: &[u8], counts: &mut MetadataCounts) -> ScanResult {
    for_fields(raw, |field, value| {
        match (field, value) {
            (4, Value::Varint) => checked_increment(
                &mut counts.split_nodes,
                MAX_SPLIT_PLAN_NODES,
                "task descriptor exceeds 1024 split nodes",
            )?,
            (4, Value::Bytes(packed)) => count_packed_varints(
                packed,
                &mut counts.split_nodes,
                MAX_SPLIT_PLAN_NODES,
                "task descriptor exceeds 1024 split nodes",
            )?,
            (5, Value::Bytes(topology)) => scan_topology(topology, counts)?,
            _ => {}
        }
        Ok(())
    })
}

fn scan_topology(raw: &[u8], counts: &mut MetadataCounts) -> ScanResult {
    for_fields(raw, |field, value| {
        match (field, value) {
            (1, Value::Bytes(edge)) => {
                checked_increment(
                    &mut counts.outbound_edges,
                    MAX_TOPOLOGY_ENTRIES,
                    "task topology exceeds 256 outbound edges",
                )?;
                scan_repeated_message_field(
                    edge,
                    4,
                    MAX_EDGE_DESTINATIONS,
                    "exchange edge exceeds 4096 destinations",
                )?;
            }
            (2, Value::Bytes(node)) => {
                checked_increment(
                    &mut counts.inbound_nodes,
                    MAX_TOPOLOGY_ENTRIES,
                    "task topology exceeds 256 inbound nodes",
                )?;
                scan_repeated_message_field(
                    node,
                    2,
                    MAX_INBOUND_SOURCES,
                    "exchange inbound exceeds 4096 sources",
                )?;
            }
            _ => {}
        }
        Ok(())
    })
}

fn scan_assignment(raw: &[u8], counts: &mut MetadataCounts) -> ScanResult {
    for_fields(raw, |field, value| {
        if field == 2 && matches!(value, Value::Bytes(_)) {
            checked_increment(
                &mut counts.initial_scan_nodes,
                MAX_INITIAL_SCAN_NODES,
                "task assignment exceeds 1024 initial scan nodes",
            )?;
        }
        Ok(())
    })?;
    count_repeated_varints(
        raw,
        3,
        &mut counts.sink_edges,
        MAX_TOPOLOGY_ENTRIES,
        "task assignment exceeds 256 sink edges",
    )
}

fn count_repeated_varints(
    raw: &[u8],
    field_number: u32,
    count: &mut usize,
    max: usize,
    message: &'static str,
) -> ScanResult {
    for_fields(raw, |field, value| {
        if field == field_number {
            match value {
                Value::Varint => checked_increment(count, max, message)?,
                Value::Bytes(packed) => count_packed_varints(packed, count, max, message)?,
                Value::Other => {}
            }
        }
        Ok(())
    })
}

fn count_packed_varints(
    raw: &[u8],
    count: &mut usize,
    max: usize,
    message: &'static str,
) -> ScanResult {
    let mut cursor = Cursor::new(raw);
    while !cursor.is_empty() {
        cursor.varint()?;
        checked_increment(count, max, message)?;
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum Value<'a> {
    Varint,
    Bytes(&'a [u8]),
    Other,
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn is_empty(&self) -> bool {
        self.offset == self.bytes.len()
    }

    fn varint(&mut self) -> Result<u64, ScanError> {
        let mut value = 0_u64;
        for index in 0..10 {
            let byte = *self.bytes.get(self.offset).ok_or(ScanError::Malformed)?;
            self.offset += 1;
            if index == 9 && byte > 1 {
                return Err(ScanError::Malformed);
            }
            value |= u64::from(byte & 0x7f) << (index * 7);
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err(ScanError::Malformed)
    }

    fn advance(&mut self, bytes: usize) -> ScanResult {
        self.offset = self.offset.checked_add(bytes).ok_or(ScanError::Malformed)?;
        if self.offset > self.bytes.len() {
            return Err(ScanError::Malformed);
        }
        Ok(())
    }

    fn field(&mut self) -> Result<(u32, Value<'a>), ScanError> {
        let tag = self.varint()?;
        let number = u32::try_from(tag >> 3).map_err(|_| ScanError::Malformed)?;
        let value = match tag & 7 {
            0 => {
                self.varint()?;
                Value::Varint
            }
            1 => {
                self.advance(8)?;
                Value::Other
            }
            2 => {
                let len = usize::try_from(self.varint()?).map_err(|_| ScanError::Malformed)?;
                let start = self.offset;
                self.advance(len)?;
                Value::Bytes(&self.bytes[start..self.offset])
            }
            3 => {
                self.skip_group(number, 1)?;
                Value::Other
            }
            5 => {
                self.advance(4)?;
                Value::Other
            }
            _ => return Err(ScanError::Malformed),
        };
        Ok((number, value))
    }

    fn skip_group(&mut self, number: u32, depth: usize) -> ScanResult {
        if depth > 100 {
            return Err(ScanError::Malformed);
        }
        loop {
            let tag = self.varint()?;
            let child_number = u32::try_from(tag >> 3).map_err(|_| ScanError::Malformed)?;
            match tag & 7 {
                0 => {
                    self.varint()?;
                }
                1 => self.advance(8)?,
                2 => {
                    let len = usize::try_from(self.varint()?).map_err(|_| ScanError::Malformed)?;
                    self.advance(len)?;
                }
                3 => self.skip_group(child_number, depth + 1)?,
                4 if child_number == number => return Ok(()),
                5 => self.advance(4)?,
                _ => return Err(ScanError::Malformed),
            }
        }
    }
}

fn for_fields(raw: &[u8], mut visit: impl FnMut(u32, Value<'_>) -> ScanResult) -> ScanResult {
    let mut cursor = Cursor::new(raw);
    while !cursor.is_empty() {
        let (field, value) = cursor.field()?;
        visit(field, value)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_proto_models::novarocks;
    use prost::Message;

    #[test]
    fn frozen_package_outer_carrier_keeps_the_transport_byte_bound() {
        let maximum = TransportBudget::DEFAULT.max_descriptor_encoded_bytes();
        let raw = vec![0_u8; maximum + 1];
        assert!(check_frozen_fragment(&raw[..maximum]).is_ok());
        assert_eq!(
            check_frozen_fragment(&raw).unwrap_err().to_string(),
            "frozen fragment exceeds 16 MiB"
        );
    }

    #[test]
    fn batch_rejects_33_operations_before_decode() {
        let raw = novarocks::ApplyTaskOperationsRequest {
            operations: vec![novarocks::TaskOperation::default(); 33],
        }
        .encode_to_vec();
        assert_eq!(
            check_operation_batch(&raw).unwrap_err().to_string(),
            "task operation batch exceeds 32 items"
        );
        assert!(
            check_operation_batch(
                &novarocks::ApplyTaskOperationsRequest {
                    operations: vec![novarocks::TaskOperation::default(); 32]
                }
                .encode_to_vec()
            )
            .is_ok()
        );
    }

    #[test]
    fn metadata_counts_packed_and_unpacked_split_nodes() {
        let mut descriptor = Vec::new();
        // 1024 packed int32 values and one unpacked value share one limit.
        descriptor.extend_from_slice(&[0x22, 0x80, 0x08]);
        descriptor.extend(std::iter::repeat_n(0_u8, 1024));
        descriptor.extend_from_slice(&[0x20, 0]);
        let mut metadata = Vec::new();
        write_bytes_field(&mut metadata, 2, &descriptor);
        assert_eq!(
            check_creation_metadata(&metadata, 0)
                .unwrap_err()
                .to_string(),
            "task descriptor exceeds 1024 split nodes"
        );
    }

    #[test]
    fn metadata_counts_repeated_singular_descriptor_fields() {
        let descriptor = novarocks::TaskDescriptor {
            split_plan_nodes: vec![0; 600],
            ..Default::default()
        }
        .encode_to_vec();
        let mut metadata = Vec::new();
        write_bytes_field(&mut metadata, 2, &descriptor);
        write_bytes_field(&mut metadata, 2, &descriptor);
        assert_eq!(
            novarocks::CreationMetadata::decode(metadata.as_slice())
                .unwrap()
                .descriptor
                .unwrap()
                .split_plan_nodes
                .len(),
            1200
        );
        assert_eq!(
            check_creation_metadata(&metadata, 0)
                .unwrap_err()
                .to_string(),
            "task descriptor exceeds 1024 split nodes"
        );
    }

    #[test]
    fn carrier_pair_checks_assignment_bytes_before_metadata_decode() {
        let mut metadata = Vec::new();
        write_bytes_field(&mut metadata, 5, &[0x08, 0x00]);
        let max = TransportBudget::DEFAULT.max_descriptor_encoded_bytes();
        assert!(check_creation_metadata(&metadata, max - 2).is_ok());
        assert_eq!(
            check_creation_metadata(&metadata, max - 1)
                .unwrap_err()
                .to_string(),
            "creation plan carriers exceed 16 MiB"
        );
        // prost merges repeated occurrences of one singular message, so the
        // bound is over every occurrence rather than the last one.
        let mut repeated = metadata.clone();
        write_bytes_field(&mut repeated, 5, &[0x08, 0x00]);
        assert!(check_creation_metadata(&repeated, max - 4).is_ok());
        assert!(check_creation_metadata(&repeated, max - 3).is_err());
    }

    #[test]
    fn assignment_bounds_scan_nodes_and_sink_edges_symmetrically() {
        let scan_nodes = |count: usize| {
            let mut assignment = Vec::new();
            for _ in 0..count {
                write_bytes_field(&mut assignment, 2, &[]);
            }
            let mut metadata = Vec::new();
            write_bytes_field(&mut metadata, 5, &assignment);
            metadata
        };
        assert!(check_creation_metadata(&scan_nodes(MAX_INITIAL_SCAN_NODES), 0).is_ok());
        assert_eq!(
            check_creation_metadata(&scan_nodes(MAX_INITIAL_SCAN_NODES + 1), 0)
                .unwrap_err()
                .to_string(),
            "task assignment exceeds 1024 initial scan nodes"
        );

        let sink_edges = |count: usize| {
            let assignment = novarocks::TaskAssignment {
                sink_edge_ids: (1..=count as u32).collect(),
                ..Default::default()
            }
            .encode_to_vec();
            let mut metadata = Vec::new();
            write_bytes_field(&mut metadata, 5, &assignment);
            metadata
        };
        assert!(check_creation_metadata(&sink_edges(MAX_TOPOLOGY_ENTRIES), 0).is_ok());
        assert_eq!(
            check_creation_metadata(&sink_edges(MAX_TOPOLOGY_ENTRIES + 1), 0)
                .unwrap_err()
                .to_string(),
            "task assignment exceeds 256 sink edges"
        );
    }

    #[test]
    fn a_retired_instance_parameter_field_is_no_longer_a_creation_carrier() {
        // Field 3 is reserved. It is not a carrier this scanner bounds, so
        // it neither counts toward the plan-carrier bound nor is walked; the
        // codec's decode is the only interpreter of what it would mean.
        let mut metadata = Vec::new();
        write_bytes_field(&mut metadata, 3, &[0x5a, 0x00]);
        let max = TransportBudget::DEFAULT.max_descriptor_encoded_bytes();
        assert!(check_creation_metadata(&metadata, max).is_ok());
    }

    #[test]
    fn topology_rejects_destination_expansion_before_metadata_decode() {
        let mut edge = Vec::new();
        for _ in 0..=MAX_EDGE_DESTINATIONS {
            write_bytes_field(&mut edge, 4, &[]);
        }
        let mut topology = Vec::new();
        write_bytes_field(&mut topology, 1, &edge);
        let mut descriptor = Vec::new();
        write_bytes_field(&mut descriptor, 5, &topology);
        let mut metadata = Vec::new();
        write_bytes_field(&mut metadata, 2, &descriptor);
        assert_eq!(
            check_creation_metadata(&metadata, 0)
                .unwrap_err()
                .to_string(),
            "exchange edge exceeds 4096 destinations"
        );
    }

    #[test]
    fn control_batch_and_subscription_bound_repeated_objects() {
        let control = novarocks::ApplyTaskControlOperationsRequest {
            operations: vec![novarocks::TaskControlOperation::default(); 33],
        }
        .encode_to_vec();
        assert!(check_control_operation_batch(&control).is_err());
        let subscription = novarocks::SubscribeTaskStatusRequest {
            cursors: vec![novarocks::TaskStatusCursor::default(); MAX_STATUS_CURSORS + 1],
            ..Default::default()
        }
        .encode_to_vec();
        assert!(check_status_subscription(&subscription).is_err());
    }

    #[test]
    fn covered_subscription_preflight_bounds_every_repeated_target() {
        for (field, error) in [
            (2, "status subscription exceeds 4096 cursors"),
            (5, "status subscription exceeds 4096 convergence cursors"),
            (6, "status subscription exceeds 4096 required identities"),
        ] {
            let mut raw = Vec::new();
            for _ in 0..MAX_STATUS_CURSORS {
                write_bytes_field(&mut raw, field, &[]);
            }
            assert!(check_status_subscription(&raw).is_ok());
            write_bytes_field(&mut raw, field, &[]);
            assert_eq!(
                check_status_subscription(&raw).unwrap_err().to_string(),
                error
            );
        }

        // All three projections may name the same legal 4096 identities. The
        // typed decoder, after prost, checks the exact distinct union.
        let mut raw = Vec::new();
        for field in [2, 5, 6] {
            for _ in 0..MAX_STATUS_CURSORS {
                write_bytes_field(&mut raw, field, &[]);
            }
        }
        assert!(check_status_subscription(&raw).is_ok());
    }

    #[test]
    fn covered_subscription_preflight_bounds_quiesce_and_total_bytes() {
        let mut quiesce = Vec::new();
        write_bytes_field(&mut quiesce, 7, &vec![0; MAX_QUIESCE_CURSOR_BYTES]);
        assert!(check_status_subscription(&quiesce).is_ok());
        let mut too_large = Vec::new();
        write_bytes_field(&mut too_large, 7, &vec![0; MAX_QUIESCE_CURSOR_BYTES + 1]);
        assert_eq!(
            check_status_subscription(&too_large)
                .unwrap_err()
                .to_string(),
            "status subscription quiesce cursor exceeds 4096 bytes"
        );
        write_bytes_field(&mut quiesce, 7, &[]);
        assert_eq!(
            check_status_subscription(&quiesce).unwrap_err().to_string(),
            "status subscription repeats quiesce cursor"
        );

        let oversize = vec![0; MAX_STATUS_SUBSCRIPTION_BYTES + 1];
        assert_eq!(
            check_status_subscription(&oversize)
                .unwrap_err()
                .to_string(),
            "status subscription exceeds 16 MiB"
        );
    }

    #[test]
    fn nested_domain_limits_are_checked_before_prost_expansion() {
        let mut open = Vec::new();
        write_bytes_field(&mut open, 2, &vec![0; MAX_OPEN_EDGES + 1]);
        let mut domain = Vec::new();
        write_bytes_field(&mut domain, 3, &open);
        let mut metadata = Vec::new();
        write_bytes_field(&mut metadata, 4, &domain);
        assert_eq!(
            check_creation_metadata(&metadata, 0)
                .unwrap_err()
                .to_string(),
            "open exchange domain exceeds 256 edges"
        );

        let mut credential = Vec::new();
        for _ in 0..=crate::domain::MAX_CREDENTIAL_DESCRIPTORS {
            write_bytes_field(&mut credential, 3, &[]);
        }
        let mut establish = Vec::new();
        write_bytes_field(&mut establish, 4, &credential);
        let mut update = Vec::new();
        write_bytes_field(&mut update, 1, &establish);
        let mut operation = Vec::new();
        write_bytes_field(&mut operation, 4, &update);
        let mut batch = Vec::new();
        write_bytes_field(&mut batch, 1, &operation);
        assert_eq!(
            check_operation_batch(&batch).unwrap_err().to_string(),
            "query credential domain exceeds 64 descriptors"
        );

        let mut query_domain = Vec::new();
        write_bytes_field(&mut query_domain, 3, &credential);
        let mut advance = Vec::new();
        write_bytes_field(&mut advance, 2, &query_domain);
        let mut update = Vec::new();
        write_bytes_field(&mut update, 2, &advance);
        let mut operation = Vec::new();
        write_bytes_field(&mut operation, 4, &update);
        let mut batch = Vec::new();
        write_bytes_field(&mut batch, 1, &operation);
        assert_eq!(
            check_operation_batch(&batch).unwrap_err().to_string(),
            "query credential domain exceeds 64 descriptors"
        );
    }

    #[test]
    fn unknown_field_and_noncanonical_varint_follow_prost() {
        // An unknown length-delimited field and a noncanonical one-byte value
        // are legal protobuf shapes. The scanner must not enforce canonicality.
        let raw = [0x98, 0x06, 0x80, 0x00, 0xa2, 0x06, 0x01, 0xff];
        assert!(check_operation_batch(&raw).is_ok());
        assert!(novarocks::ApplyTaskOperationsRequest::decode(raw.as_slice()).is_ok());
        assert!(check_operation_batch(&[0x0a, 0x80]).is_ok());
        assert!(novarocks::ApplyTaskOperationsRequest::decode(&[0x0a, 0x80][..]).is_err());
    }

    fn write_bytes_field(destination: &mut Vec<u8>, number: u32, bytes: &[u8]) {
        encode_varint(destination, u64::from(number << 3 | 2));
        encode_varint(destination, bytes.len() as u64);
        destination.extend_from_slice(bytes);
    }

    fn encode_varint(destination: &mut Vec<u8>, mut value: u64) {
        while value >= 0x80 {
            destination.push((value as u8 & 0x7f) | 0x80);
            value >>= 7;
        }
        destination.push(value as u8);
    }
}

#[cfg(test)]
mod root_preflight_tests {
    use super::*;
    use novarocks_proto_models::{novarocks, result};
    use novarocks_result_contract::RootProfileV1;
    use prost::Message;
    fn bytes_field(out: &mut Vec<u8>, field: u8, body: &[u8]) {
        out.push(field << 3 | 2);
        let mut n = body.len();
        while n >= 128 {
            out.push((n as u8 & 127) | 128);
            n >>= 7;
        }
        out.push(n as u8);
        out.extend_from_slice(body);
    }
    #[test]
    fn root_reply_distinguishes_payload_and_envelope() {
        let data = result::RootData {
            sequence: 1,
            body: vec![0; RootProfileV1::SEGMENT_BYTES].into(),
            end_after_data: None,
        };
        let response = novarocks::FetchRootResultResponse {
            result: Some(novarocks::fetch_root_result_response::Result::Reply(
                novarocks::RootReadReceipt {
                    outcome: Some(novarocks::root_read_receipt::Outcome::Data(data.clone())),
                    ..Default::default()
                },
            )),
            ..Default::default()
        };
        assert!(check_root_result_reply(&response.encode_to_vec()).is_ok());
        let mut large = data;
        large.body = vec![0; RootProfileV1::SEGMENT_BYTES + 1].into();
        let response = novarocks::FetchRootResultResponse {
            result: Some(novarocks::fetch_root_result_response::Result::Reply(
                novarocks::RootReadReceipt {
                    outcome: Some(novarocks::root_read_receipt::Outcome::Data(large)),
                    ..Default::default()
                },
            )),
            ..Default::default()
        };
        assert!(check_root_result_reply(&response.encode_to_vec()).is_err());
        let mut metadata = Vec::new();
        bytes_field(&mut metadata, 15, &vec![0; RootProfileV1::ENVELOPE_BYTES]);
        assert!(check_root_result_reply(&metadata).is_err());
        assert!(check_root_result_read(&metadata).is_err());
    }
    #[test]
    fn repeated_singular_data_and_schema_columns_are_bounded_before_prost() {
        let mut repeated = Vec::new();
        let mut receipt = Vec::new();
        bytes_field(&mut receipt, 6, &[]);
        bytes_field(&mut repeated, 4, &receipt);
        bytes_field(&mut repeated, 4, &receipt);
        assert!(check_root_result_reply(&repeated).is_err());
        let mut columns = Vec::new();
        for _ in 0..RootProfileV1::MAX_COLUMNS {
            bytes_field(&mut columns, 1, &[]);
        }
        assert!(check_client_render_schema(&columns).is_ok());
        bytes_field(&mut columns, 1, &[]);
        assert!(check_client_render_schema(&columns).is_err());
    }
    #[test]
    fn presentation_family_bytes_count_toward_wire_and_decode_overlap() {
        for (field, payload) in [(3, vec![]), (4, vec![8, 1, 16, 1, 24, 1])] {
            let mut raw = Vec::new();
            bytes_field(&mut raw, field, &payload);
            let mut scan = RenderSchemaScan::new();
            let initial = scan.expansion.backing;
            scan.scan(&raw).unwrap();
            assert_eq!(scan.wire_bytes, raw.len());
            assert_eq!(scan.expansion.backing, initial + raw.len());
            // Repeated family messages consume the same profile even though
            // prost retains only the oneof's latest value.
            while raw.len() <= RootProfileV1::SCHEMA_WIRE_BYTES {
                bytes_field(&mut raw, field, &payload);
            }
            assert!(check_client_render_schema(&raw).is_err());
        }
    }
    #[test]
    fn widest_short_primitive_schema_passes_raw_prost_and_neutral_decode() {
        use novarocks_result_contract::{
            ClientRenderSchema, NativeRenderType, RenderColumn, RenderField, RenderPresentation,
        };
        let columns = (0..RootProfileV1::MAX_COLUMNS)
            .map(|index| RenderColumn {
                source_ordinal: index as u32,
                source_slot: Some(index as u32),
                name: "x".into(),
                field: RenderField {
                    nullable: true,
                    native_type: NativeRenderType::String,
                    presentation: RenderPresentation::ScalarText,
                },
            })
            .collect();
        let schema = ClientRenderSchema::try_new(
            novarocks_result_contract::ClientPresentationFamily::MysqlText,
            columns,
            RootProfileV1::MAX_COLUMNS,
        )
        .unwrap();
        let encoded =
            novarocks_proto_codec::root_result::encode_client_schema(&schema).encode_to_vec();
        check_client_render_schema(&encoded).unwrap();
        let decoded =
            novarocks_proto_models::result::ClientRenderSchema::decode(encoded.as_slice()).unwrap();
        assert_eq!(
            novarocks_proto_codec::root_result::decode_client_schema(
                &decoded,
                RootProfileV1::MAX_COLUMNS,
                novarocks_proto_codec::FieldPath::root("schema")
            )
            .unwrap(),
            schema
        );
    }
    #[test]
    fn merged_close_and_update_messages_share_their_domain_counter() {
        for control in [false, true] {
            let batch = |last_count| {
                let mut first = Vec::new();
                let mut second = Vec::new();
                for _ in 0..128 {
                    bytes_field(&mut first, 2, &[]);
                }
                for _ in 0..last_count {
                    bytes_field(&mut second, 2, &[]);
                }
                let mut operation = Vec::new();
                let field = if control { 7 } else { 3 };
                bytes_field(&mut operation, field, &first);
                bytes_field(&mut operation, field, &second);
                let mut batch = Vec::new();
                bytes_field(&mut batch, 1, &operation);
                batch
            };
            assert!(finish(scan_batch(&batch(128), control)).is_ok());
            assert!(finish(scan_batch(&batch(129), control)).is_err());
        }
    }

    #[test]
    fn empty_children_and_repeated_merged_types_are_bounded_before_prost() {
        let mut ty = Vec::new();
        for _ in 0..65_537 {
            bytes_field(&mut ty, 8, &[]);
        }
        let mut field = Vec::new();
        bytes_field(&mut field, 2, &ty);
        let mut schema = Vec::new();
        bytes_field(&mut schema, 2, &field);
        assert!(schema.len() < RootProfileV1::SCHEMA_WIRE_BYTES);
        assert!(check_client_render_schema(&schema).is_err());
        let mut half = Vec::new();
        for _ in 0..2049 {
            bytes_field(&mut half, 8, &[]);
        }
        field.clear();
        bytes_field(&mut field, 2, &half);
        bytes_field(&mut field, 2, &half);
        schema.clear();
        bytes_field(&mut schema, 2, &field);
        assert!(check_client_render_schema(&schema).is_err());
    }
    #[test]
    fn scalar_schema_raw_expansion_preflight_counts_nodes_names_and_empty_children() {
        let mut schema = Vec::new();
        bytes_field(&mut schema, 3, &[]);
        assert!(check_scalar_schema(&schema).is_ok());
        for _ in 0..RootProfileV1::SCHEMA_TYPE_NODES {
            bytes_field(&mut schema, 3, &[]);
        }
        assert!(schema.len() < RootProfileV1::SCHEMA_WIRE_BYTES);
        assert!(check_scalar_schema(&schema).is_err());
        let mut ty = Vec::new();
        bytes_field(&mut ty, 6, &vec![b'x'; RootProfileV1::MAX_NAME_BYTES + 1]);
        let mut field = Vec::new();
        bytes_field(&mut field, 2, &ty);
        schema.clear();
        bytes_field(&mut schema, 3, &field);
        assert!(check_scalar_schema(&schema).is_err());
        ty.clear();
        for _ in 0..=RootProfileV1::MAX_COLUMNS {
            bytes_field(&mut ty, 8, &[]);
        }
        field.clear();
        bytes_field(&mut field, 2, &ty);
        schema.clear();
        bytes_field(&mut schema, 3, &field);
        assert!(check_scalar_schema(&schema).is_err());
    }
    #[test]
    fn scalar_schema_raw_preflight_tracks_singular_type_merges_before_prost() {
        let mut half = Vec::new();
        for _ in 0..2049 {
            bytes_field(&mut half, 8, &[]);
        }
        let mut field = Vec::new();
        bytes_field(&mut field, 2, &half);
        bytes_field(&mut field, 2, &half);
        let mut schema = Vec::new();
        bytes_field(&mut schema, 3, &field);
        assert!(schema.len() < RootProfileV1::SCHEMA_WIRE_BYTES);
        assert!(check_scalar_schema(&schema).is_err());
    }
    #[test]
    fn scalar_schema_raw_preflight_bounds_repeated_root_schema_messages() {
        let mut schema = Vec::new();
        for _ in 0..=RootProfileV1::SCHEMA_TYPE_NODES {
            bytes_field(&mut schema, 3, &[]);
        }
        let mut root = Vec::new();
        bytes_field(&mut root, 4, &schema);
        assert!(RenderSchemaScan::new().scan_root(&root).is_err());
        // Duplicate singular schema messages merge their field-node Vec.
        // Counts must survive the wrapper boundary as well as one scan call.
        schema.clear();
        for _ in 0..2048 {
            bytes_field(&mut schema, 3, &[]);
        }
        root.clear();
        bytes_field(&mut root, 4, &schema);
        let mut state = RenderSchemaScan::new();
        for _ in 0..4 {
            if state.scan_root(&root).is_err() {
                return;
            }
        }
        assert!(state.scan_root(&root).is_err());
    }
}
