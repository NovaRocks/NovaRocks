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

//! Cumulative requested-resource projection from generated wire/layout facts.
//! Every occurrence contributes, including overwritten singular and oneof
//! values. A malformed prefix retains its charges. This does not decode DTOs,
//! validate UTF-8/business semantics, or authorize a host allocation.

use std::collections::btree_map;

use novarocks_proto_models::resource_layout::WireKind;
use novarocks_type_contract::{CompileCheckpoints, CompileControlError};
use prost::encoding::WireType;

use super::{
    DecodeProjectionLimits, DecodeResourceProjection, DecodeResourceUsage, PROST_RECURSION_LIMIT,
    ResourceCursorStatus, ResourceModelError as E,
    allocation::{
        checked_add, checked_mul, error_heap, repeated_slots, replacement_bytes,
        slice_bytes_payload,
    },
    schema::{ModelField, Schema},
    wire::Cursor,
};

/// Actual DAG depth, including the root message, computed without recursive
/// Rust calls. The generated schema owner establishes reachability/acyclicity;
/// this traversal also refuses a broken reference or cycle rather than guessing.
pub(super) fn message_depth(
    schema: &Schema,
    work: &mut CompileCheckpoints<'_>,
) -> Result<usize, E> {
    struct Frame<'a> {
        index: usize,
        fields: btree_map::Values<'a, u32, ModelField>,
        child_depth: usize,
    }
    let root = schema
        .objects
        .get(schema.root)
        .ok_or(E::Schema("missing resource schema root"))?;
    let mut states = Vec::with_capacity(schema.objects.len());
    let mut depths = Vec::with_capacity(schema.objects.len());
    for _ in &schema.objects {
        work.step()?;
        states.push(0_u8);
        depths.push(0_usize);
    }
    states[schema.root] = 1;
    let mut stack = vec![Frame {
        index: schema.root,
        fields: root.fields.values(),
        child_depth: 0,
    }];
    while let Some(frame) = stack.last_mut() {
        work.step()?;
        if let Some(field) = frame.fields.next() {
            let Some(child) = field.message else { continue };
            let object = schema
                .objects
                .get(child)
                .ok_or(E::Schema("invalid resource message target"))?;
            match states[child] {
                1 => return Err(E::Schema("recursive resource message graph")),
                2 => frame.child_depth = frame.child_depth.max(depths[child]),
                _ => {
                    states[child] = 1;
                    stack.push(Frame {
                        index: child,
                        fields: object.fields.values(),
                        child_depth: 0,
                    });
                }
            }
        } else {
            let depth = checked_add(frame.child_depth, 1)?;
            let index = frame.index;
            states[index] = 2;
            depths[index] = depth;
            stack.pop();
            if let Some(parent) = stack.last_mut() {
                parent.child_depth = parent.child_depth.max(depth);
            }
        }
    }
    Ok(depths[schema.root])
}

type Admission<'a> = dyn FnMut(&DecodeResourceUsage) -> Result<(), CompileControlError> + 'a;

struct Accounting<'borrow, 'admit> {
    limits: DecodeProjectionLimits,
    usage: DecodeResourceUsage,
    admit: Option<&'borrow mut Admission<'admit>>,
}
impl Accounting<'_, '_> {
    fn gate(&mut self) -> Result<(), E> {
        if let Some(admit) = &mut self.admit {
            admit(&self.usage)?;
        }
        Ok(())
    }
    fn heap(&mut self, bytes: usize) -> Result<(), E> {
        self.usage.cumulative_requested_heap_bytes_upper =
            checked_add(self.usage.cumulative_requested_heap_bytes_upper, bytes)?;
        self.usage.peak_requested_heap_bytes_upper =
            self.usage.cumulative_requested_heap_bytes_upper;
        if self.usage.cumulative_requested_heap_bytes_upper > self.limits.max_requested_heap_bytes {
            return Err(E::Limit("requested protobuf heap exceeds caller limit"));
        }
        Ok(())
    }
    fn copied(&mut self, bytes: usize) -> Result<(), E> {
        self.usage.copied_bytes_upper = checked_add(self.usage.copied_bytes_upper, bytes)?;
        if self.usage.copied_bytes_upper > self.limits.max_copied_bytes {
            return Err(E::Limit("protobuf copy bytes exceed caller limit"));
        }
        Ok(())
    }
    fn initialized(&mut self, bytes: usize) -> Result<(), E> {
        self.usage.initialization_bytes_upper =
            checked_add(self.usage.initialization_bytes_upper, bytes)?;
        if self.usage.initialization_bytes_upper > self.limits.max_initialization_bytes {
            return Err(E::Limit(
                "protobuf initialization bytes exceed caller limit",
            ));
        }
        Ok(())
    }
    fn message(&mut self, size: usize) -> Result<(), E> {
        self.usage.message_occurrences = checked_add(self.usage.message_occurrences, 1)?;
        if self.usage.message_occurrences > self.limits.max_message_occurrences {
            return Err(E::Limit("protobuf message occurrences exceed caller limit"));
        }
        // Inline Default construction is initialization, not a new heap block.
        self.initialized(size)
    }
    fn field(&mut self) -> Result<(), E> {
        self.usage.field_occurrences = checked_add(self.usage.field_occurrences, 1)?;
        if self.usage.field_occurrences > self.limits.max_field_occurrences {
            return Err(E::Limit("protobuf field occurrences exceed caller limit"));
        }
        Ok(())
    }
    fn slots(&mut self, field: &ModelField) -> Result<(), E> {
        if let Some(element) = field.repeated {
            let requests = repeated_slots(element.size, 1)?;
            self.heap(requests)?;
            // Cumulative requests also bound copies of old backing on growth.
            self.copied(requests)?;
        }
        Ok(())
    }
    fn scalar(&mut self, field: &ModelField) -> Result<(), E> {
        self.usage.scalar_elements = checked_add(self.usage.scalar_elements, 1)?;
        if self.usage.scalar_elements > self.limits.max_scalar_elements {
            return Err(E::Limit("protobuf scalar elements exceed caller limit"));
        }
        self.slots(field)?;
        // Count the actual leaf or optional inline wrapper conservatively.
        self.initialized(field.repeated.unwrap_or(field.rust).size)
    }
    fn bytes(&mut self, field: &ModelField, bytes: usize) -> Result<(), E> {
        self.scalar(field)?;
        self.heap(if field.wire.kind == WireKind::String {
            replacement_bytes(bytes)?
        } else {
            slice_bytes_payload(bytes)?
        })?;
        // String copy plus UTF-8 inspection, or Bytes temporary plus Vec copy,
        // plus old destination capacity moved by realloc. A growth implies
        // old_capacity < new_length, so one further length bounds that move.
        self.copied(checked_mul(bytes, 3)?)
    }
}

fn scalar_wire(kind: WireKind) -> Option<WireType> {
    match kind {
        WireKind::Double | WireKind::Fixed64 | WireKind::Sfixed64 => Some(WireType::SixtyFourBit),
        WireKind::Float | WireKind::Fixed32 | WireKind::Sfixed32 => Some(WireType::ThirtyTwoBit),
        WireKind::Int64
        | WireKind::Uint64
        | WireKind::Int32
        | WireKind::Bool
        | WireKind::Uint32
        | WireKind::Enum
        | WireKind::Sint32
        | WireKind::Sint64 => Some(WireType::Varint),
        WireKind::String | WireKind::Bytes | WireKind::Message => None,
    }
}
fn scalar_value(
    cursor: &mut Cursor<'_>,
    wire: WireType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), E> {
    match wire {
        WireType::Varint => {
            cursor.varint(work)?;
        }
        WireType::ThirtyTwoBit => {
            cursor.fixed(4, work)?;
        }
        WireType::SixtyFourBit => {
            cursor.fixed(8, work)?;
        }
        _ => return Err(E::Schema("invalid generated scalar wire")),
    }
    Ok(())
}

/// Return merge_loop's stopping position without clipping or consuming the
/// body. Nested field reads must still see the complete enclosing Buf suffix.
fn delimited_end(cursor: &mut Cursor<'_>, work: &mut CompileCheckpoints<'_>) -> Result<usize, E> {
    let length = usize::try_from(cursor.varint(work)?).map_err(|_| E::Malformed)?;
    cursor
        .remaining()
        .len()
        .checked_sub(length)
        .ok_or(E::Malformed)
}

pub(super) fn scan(
    schema: &Schema,
    max_message_depth: usize,
    raw: &[u8],
    limits: DecodeProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<DecodeResourceProjection, E> {
    scan_core(schema, max_message_depth, raw, limits, None, work)
}

pub(super) fn scan_in(
    schema: &Schema,
    max_message_depth: usize,
    raw: &[u8],
    limits: DecodeProjectionLimits,
    admit: &mut Admission<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<DecodeResourceProjection, E> {
    match scan_core(schema, max_message_depth, raw, limits, Some(admit), work) {
        // All Limit branches belong to the original numeric envelope. Schema
        // drift and malformed-prefix semantics retain their original owners.
        Err(E::Limit(_)) => Err(E::Control(CompileControlError::ResourceExhausted)),
        result => result,
    }
}

fn scan_core(
    schema: &Schema,
    max_message_depth: usize,
    raw: &[u8],
    limits: DecodeProjectionLimits,
    admit: Option<&mut Admission<'_>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<DecodeResourceProjection, E> {
    let parent = admit.is_some();
    if limits.max_wire_depth > PROST_RECURSION_LIMIT {
        return Err(E::Schema(
            "wire depth exceeds locked Prost recursion allowance",
        ));
    }
    if max_message_depth == 0 {
        return Err(E::Schema("resource message depth excludes root"));
    }
    if raw.len() > limits.max_input_bytes {
        return Err(E::Limit("protobuf input exceeds caller limit"));
    }
    let root = schema
        .objects
        .get(schema.root)
        .ok_or(E::Schema("missing resource schema root"))?;
    let mut accounting = Accounting {
        limits,
        admit,
        usage: DecodeResourceUsage {
            input_bytes: raw.len(),
            root_inline_bytes: root.layout.size,
            ..DecodeResourceUsage::default()
        },
    };
    let errors = error_heap(max_message_depth)?;
    accounting.usage.error_requested_heap_bytes_upper = errors;
    accounting.heap(errors)?;
    // The same conservative request bound covers DecodeError construction,
    // formatting/context writes and relocation of its growing backing.
    accounting.copied(errors)?;
    accounting.initialized(errors)?;
    accounting.message(root.layout.size)?;
    accounting.gate()?;
    struct Frame {
        object: usize,
        end_remaining: usize,
    }
    // Prost merge_loop reads each field against the complete shared suffix,
    // then checks whether that field crossed the enclosing length boundary.
    // A clipped child slice would miss allocations made before this error.
    let mut cursor = Cursor::new(raw);
    // Root has zero descents. These fixed frames are host-accounted scanner
    // scratch; none is a decoded object allocation or heap traversal vector.
    let mut frames: [Option<Frame>; PROST_RECURSION_LIMIT + 1] = std::array::from_fn(|_| None);
    frames[0] = Some(Frame {
        object: schema.root,
        end_remaining: 0,
    });
    let mut depth = 0_usize;
    let result = (|| -> Result<(), E> {
        loop {
            work.step()?;
            let frame = frames[depth]
                .as_ref()
                .ok_or(E::Schema("missing active resource cursor"))?;
            if cursor.remaining().len() <= frame.end_remaining {
                if cursor.remaining().len() != frame.end_remaining {
                    return Err(E::Malformed);
                }
                frames[depth] = None;
                if depth == 0 {
                    return Ok(());
                }
                depth -= 1;
                continue;
            }
            let (tag, wire) = cursor.key(work)?.ok_or(E::Malformed)?;
            accounting.field()?;
            let object = schema
                .objects
                .get(frame.object)
                .ok_or(E::Schema("invalid active resource object"))?;
            let Some(field) = object.fields.get(&tag) else {
                if parent {
                    Cursor::check_group_depth(wire, 0, limits.max_wire_depth - depth)?;
                }
                accounting.gate()?;
                let mut observe_key = || {
                    accounting.field()?;
                    accounting.gate()
                };
                if parent {
                    cursor.skip_observed_policy(
                        wire,
                        tag,
                        limits.max_wire_depth - depth,
                        true,
                        work,
                        &mut observe_key,
                    )?;
                } else {
                    cursor.skip_observed(
                        wire,
                        tag,
                        limits.max_wire_depth - depth,
                        work,
                        observe_key,
                    )?;
                }
                continue;
            };
            // One key may change an optional/oneof discriminant whose size
            // is not present in the flattened payload layout. Charge the
            // actual enclosing object once per known key, conservatively,
            // then each scalar payload separately (including packed items).
            // This is initialization work, never another heap allocation.
            accounting.initialized(object.layout.size)?;
            if let Some(expected) = scalar_wire(field.wire.kind) {
                if wire == WireType::LengthDelimited && field.repeated.is_some() {
                    // Prost accepts packed and unpacked numeric occurrences,
                    // regardless of the descriptor's preferred packed flag.
                    accounting.gate()?;
                    let end = delimited_end(&mut cursor, work)?;
                    while cursor.remaining().len() > end {
                        if parent {
                            accounting.scalar(field)?;
                            accounting.gate()?;
                            work.step()?;
                        } else {
                            work.step()?;
                            accounting.scalar(field)?;
                        }
                        scalar_value(&mut cursor, expected, work)?;
                    }
                    if cursor.remaining().len() != end {
                        return Err(E::Malformed);
                    }
                } else {
                    if wire != expected {
                        accounting.gate()?;
                        return Err(E::Malformed);
                    }
                    accounting.scalar(field)?;
                    accounting.gate()?;
                    scalar_value(&mut cursor, expected, work)?;
                }
            } else if field.wire.kind == WireKind::Message {
                let child = field
                    .message
                    .ok_or(E::Schema("generated message lacks target"))?;
                let child_object = schema
                    .objects
                    .get(child)
                    .ok_or(E::Schema("invalid resource message target"))?;
                // Optional/default and repeated temporary message construction
                // precedes parsing the nested body. Never discard that prefix.
                accounting.slots(field)?;
                accounting.message(child_object.layout.size)?;
                if parent && depth < PROST_RECURSION_LIMIT && depth >= limits.max_wire_depth {
                    return Err(E::Limit("protobuf message depth exceeds caller limit"));
                }
                accounting.gate()?;
                if wire != WireType::LengthDelimited {
                    return Err(E::Malformed);
                }
                let end_remaining = delimited_end(&mut cursor, work)?;
                if depth == PROST_RECURSION_LIMIT {
                    return Err(E::Malformed);
                }
                if depth >= limits.max_wire_depth {
                    return Err(E::Limit("protobuf message depth exceeds caller limit"));
                }
                depth += 1;
                frames[depth] = Some(Frame {
                    object: child,
                    end_remaining,
                });
            } else {
                if wire != WireType::LengthDelimited {
                    accounting.gate()?;
                    return Err(E::Malformed);
                }
                if parent {
                    // The enclosing optional/oneof initialization is already
                    // known before the length read; payload charges wait for
                    // a real captured available slice, including malformed input.
                    accounting.gate()?;
                    cursor.length_delimited_captured(work, |bytes| {
                        accounting.bytes(field, bytes.len())?;
                        accounting.gate()
                    })?;
                } else {
                    let bytes = cursor.length_delimited(work)?;
                    accounting.bytes(field, bytes.len())?;
                }
            }
        }
    })();
    match result {
        Ok(()) => Ok(DecodeResourceProjection {
            status: ResourceCursorStatus::Complete,
            usage: accounting.usage,
        }),
        Err(E::Malformed) => {
            accounting.gate()?;
            Ok(DecodeResourceProjection {
                status: ResourceCursorStatus::MalformedPrefix,
                usage: accounting.usage,
            })
        }
        Err(error) => Err(error),
    }
}
