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
//! and the native plan tree. It does not decide protobuf encoding legality or
//! business semantics. A malformed cursor is left to prost, which remains the
//! sole protobuf interpreter; unknown fields and noncanonical varints are not
//! rejected here. The typed codec still validates every semantic relationship.

use std::fmt;

use crate::TransportBudget;
use crate::creation::MAX_INITIAL_SCAN_NODES;
use crate::descriptor::{
    MAX_EDGE_DESTINATIONS, MAX_INBOUND_SOURCES, MAX_SPLIT_PLAN_NODES, MAX_TOPOLOGY_ENTRIES,
};
use crate::domain::{MAX_DOMAIN_UPDATES, MAX_OPEN_EDGES};

// Keep in sync with plan-codec's NATIVE_V1_MAX_TREE_DEPTH. Task codec must not
// depend on the plan encoder crate across the native wire boundary.
const MAX_PLAN_TREE_DEPTH: usize = 64;
// A 16 MiB carrier can encode millions of empty nested messages. This bound
// limits object expansion before prost allocation while leaving ample room for
// any useful executable fragment, independently of the fragment's byte cap.
const MAX_PLAN_TREE_NODES: usize = 65_536;
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
    finish(scan_frozen_fragment(raw))
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
            }
        }
        Ok(())
    })
}

fn scan_operation(raw: &[u8]) -> ScanResult {
    for_fields(raw, |field, value| {
        match (field, value) {
            (3, Value::Bytes(update)) => scan_update_task(update)?,
            (4, Value::Bytes(update)) => scan_update_context(update)?,
            _ => {}
        }
        Ok(())
    })
}

fn scan_update_task(raw: &[u8]) -> ScanResult {
    // TaskOperation.operation is a oneof. Prost replaces an earlier variant,
    // so count each candidate without combining duplicates.
    let mut count = 0;
    for_fields(raw, |field, value| {
        if field == 2
            && let Value::Bytes(domain) = value
        {
            checked_increment(
                &mut count,
                MAX_DOMAIN_UPDATES,
                "task update exceeds 256 domains",
            )?;
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

fn scan_frozen_fragment(raw: &[u8]) -> ScanResult {
    let mut nodes = 0;
    for_fields(raw, |field, value| {
        if field == 5
            && let Value::Bytes(plan) = value
        {
            scan_plan_fragment(plan, &mut nodes)?;
        }
        Ok(())
    })
}

fn scan_plan_fragment(raw: &[u8], nodes: &mut usize) -> ScanResult {
    for_fields(raw, |field, value| {
        if field == 2
            && let Value::Bytes(root) = value
        {
            scan_plan_node(root, 1, nodes)?;
        }
        Ok(())
    })
}

fn scan_plan_node(raw: &[u8], depth: usize, nodes: &mut usize) -> ScanResult {
    if depth > MAX_PLAN_TREE_DEPTH {
        return Err(limit("native plan tree exceeds 64 nodes on one path"));
    }
    checked_increment(
        nodes,
        MAX_PLAN_TREE_NODES,
        "native plan tree exceeds 65536 nodes",
    )?;
    for_fields(raw, |field, value| {
        if field == 8
            && let Value::Bytes(child) = value
        {
            scan_plan_node(child, depth + 1, nodes)?;
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
    use novarocks_proto_models::{novarocks, plan};
    use prost::Message;

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
    fn frozen_fragment_bounds_plan_tree_before_prost() {
        let mut node = plan::DistributedNode::default().encode_to_vec();
        for _ in 1..=MAX_PLAN_TREE_DEPTH {
            let mut parent = Vec::new();
            write_bytes_field(&mut parent, 8, &node);
            node = parent;
        }
        let mut fragment = Vec::new();
        write_bytes_field(&mut fragment, 2, &node);
        let mut frozen = Vec::new();
        write_bytes_field(&mut frozen, 5, &fragment);
        assert_eq!(
            check_frozen_fragment(&frozen).unwrap_err().to_string(),
            "native plan tree exceeds 64 nodes on one path"
        );
    }

    #[test]
    fn frozen_fragment_bounds_many_shallow_plan_nodes() {
        let mut root = Vec::with_capacity(MAX_PLAN_TREE_NODES * 2);
        for _ in 0..MAX_PLAN_TREE_NODES {
            write_bytes_field(&mut root, 8, &[]);
        }
        let mut fragment = Vec::new();
        write_bytes_field(&mut fragment, 2, &root);
        let mut frozen = Vec::new();
        write_bytes_field(&mut frozen, 5, &fragment);
        assert_eq!(
            check_frozen_fragment(&frozen).unwrap_err().to_string(),
            "native plan tree exceeds 65536 nodes"
        );
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
