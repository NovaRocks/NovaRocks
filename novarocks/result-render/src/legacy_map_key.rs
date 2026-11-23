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

//! Bounded collision validation for the legacy Trino MAP key projection.
use super::*;

const WINDOW: usize = 512;
const TABLE: usize = WINDOW * 2;
const INITIAL_HASH: u64 = 0xcbf29ce484222325;

#[derive(Clone, Copy, Default)]
struct KeyHash {
    generation: u64,
    hash: u64,
    row: usize,
}

struct Check {
    array: ArrayRef,
    path: Path,
    window: usize,
    end: usize,
    row: usize,
    hash: u64,
    phase: CheckPhase,
}

enum CheckPhase {
    Start,
    Hash,
    Probe(usize),
    Compare {
        slot: usize,
        left: Option<Option<u8>>,
    },
}

/// Only maps with day-time interval keys use this scratch. Each window inserts
/// up to WINDOW keys and checks all later keys against it. A hash match is
/// confirmed by comparing the same byte streams used to emit the JSON name.
/// Windowing keeps storage independent of row width and key count.
pub(super) struct LegacyMapKeys {
    fields: Vec<usize>,
    table: Box<[KeyHash]>,
    generation: u64,
    check: Option<Check>,
    candidate: CellCursor,
    original: CellCursor,
}

impl LegacyMapKeys {
    pub(super) fn for_schema(
        schema: &ClientRenderSchema,
    ) -> Result<Option<Box<Self>>, RenderError> {
        let mut fields = Vec::new();
        observe_legacy_maps(schema, |field| {
            if fields.len() == V::SCHEMA_TYPE_NODES {
                return Err(error(E::SchemaMismatch));
            }
            if fields.is_empty() {
                fields.reserve_exact(V::SCHEMA_TYPE_NODES);
            }
            fields.push(std::ptr::from_ref(field).addr());
            Ok(())
        })?;
        if fields.is_empty() {
            return Ok(None);
        }
        fields.sort_unstable();
        Ok(Some(Box::new(Self {
            fields,
            table: vec![KeyHash::default(); TABLE].into_boxed_slice(),
            generation: 0,
            check: None,
            candidate: CellCursor::new(),
            original: CellCursor::new(),
        })))
    }

    pub(super) fn required_for(schema: &ClientRenderSchema) -> bool {
        let mut needed = false;
        let result = observe_legacy_maps(schema, |_| {
            needed = true;
            Ok(())
        });
        debug_assert!(result.is_ok(), "the observer is infallible");
        needed
    }

    pub(super) fn field_ids(&self) -> &[usize] {
        &self.fields
    }

    pub(super) fn scratch_capacity(&self) -> usize {
        Self::comparison_capacity() + self.fields.capacity() * size_of::<usize>()
    }

    pub(super) const fn scratch_capacity_bound() -> usize {
        Self::comparison_capacity() + V::SCHEMA_TYPE_NODES * size_of::<usize>()
    }

    const fn comparison_capacity() -> usize {
        size_of::<Self>()
            + TABLE * size_of::<KeyHash>()
            + 2 * CELL_TASK_CAPACITY * size_of::<EncodeStep>()
    }

    pub(super) fn begin(
        &mut self,
        array: ArrayRef,
        start: usize,
        end: usize,
        path: Path,
    ) -> Result<(), RenderError> {
        if self.check.is_some() {
            return Err(invalid());
        }
        self.generation = self.generation.checked_add(1).ok_or_else(invalid)?;
        self.check = Some(Check {
            array,
            path,
            window: start,
            end,
            row: start,
            hash: INITIAL_HASH,
            phase: CheckPhase::Start,
        });
        Ok(())
    }

    pub(super) fn clear(&mut self) {
        self.check = None;
        self.candidate.stack.clear();
        self.original.stack.clear();
    }

    /// Returns true after validation and false when the current turn is full.
    pub(super) fn advance(
        &mut self,
        schema: &ClientRenderSchema,
        b: &mut Budget,
    ) -> Result<bool, RenderError> {
        let Some(check) = self.check.as_mut() else {
            return Ok(true);
        };
        while b.room(4096) {
            match check.phase {
                CheckPhase::Start => {
                    if check.row == check.end {
                        check.window += WINDOW;
                        if check.window >= check.end {
                            self.clear();
                            return Ok(true);
                        }
                        check.row = check.window;
                        self.generation = self.generation.checked_add(1).ok_or_else(invalid)?;
                    }
                    b.scan(32);
                    check.hash = INITIAL_HASH;
                    self.candidate
                        .reset_key(check.array.clone(), check.row, check.path);
                    check.phase = CheckPhase::Hash;
                }
                CheckPhase::Hash => match key_byte(&mut self.candidate, schema, b)? {
                    Some(Some(byte)) => {
                        check.hash = (check.hash ^ u64::from(byte)).wrapping_mul(0x100000001b3);
                    }
                    Some(None) => {
                        check.phase = CheckPhase::Probe(check.hash as usize & (TABLE - 1));
                    }
                    None => return Ok(false),
                },
                CheckPhase::Probe(slot) => {
                    b.scan(size_of::<KeyHash>());
                    let entry = &mut self.table[slot];
                    if entry.generation != self.generation {
                        if check.row < check.window + WINDOW {
                            *entry = KeyHash {
                                generation: self.generation,
                                hash: check.hash,
                                row: check.row,
                            };
                        }
                        check.row += 1;
                        check.phase = CheckPhase::Start;
                    } else if entry.hash == check.hash {
                        self.candidate
                            .reset_key(check.array.clone(), check.row, check.path);
                        self.original
                            .reset_key(check.array.clone(), entry.row, check.path);
                        check.phase = CheckPhase::Compare { slot, left: None };
                    } else {
                        check.phase = CheckPhase::Probe((slot + 1) & (TABLE - 1));
                    }
                }
                CheckPhase::Compare { slot, left } => {
                    let left = if let Some(left) = left {
                        left
                    } else {
                        let Some(left) = key_byte(&mut self.candidate, schema, b)? else {
                            return Ok(false);
                        };
                        check.phase = CheckPhase::Compare {
                            slot,
                            left: Some(left),
                        };
                        left
                    };
                    let Some(right) = key_byte(&mut self.original, schema, b)? else {
                        return Ok(false);
                    };
                    if left != right {
                        check.phase = CheckPhase::Probe((slot + 1) & (TABLE - 1));
                    } else if left.is_none() {
                        return Err(error(E::LegacyIntervalMapKeyCollision));
                    } else {
                        check.phase = CheckPhase::Compare { slot, left: None };
                    }
                }
            }
        }
        Ok(false)
    }
}

// The outer option is the cooperative yield. The inner option is the end of
// one key, which also has to be compared to distinguish a proper prefix.
fn key_byte(
    cursor: &mut CellCursor,
    schema: &ClientRenderSchema,
    b: &mut Budget,
) -> Result<Option<Option<u8>>, RenderError> {
    match cursor.next_raw(schema, b, &[])? {
        Poll::Byte(byte) => Ok(Some(Some(byte))),
        Poll::Done => Ok(Some(None)),
        Poll::Yield => Ok(None),
        Poll::CheckLegacyKeys { .. } => Err(invalid()),
    }
}

fn observe_legacy_maps(
    schema: &ClientRenderSchema,
    mut observe: impl FnMut(&RenderField) -> Result<(), RenderError>,
) -> Result<(), RenderError> {
    use novarocks_result_contract::ClientPresentationFamily;
    let ClientPresentationFamily::TrinoJson(facts) = *schema.family();
    if !facts.parametric_interval {
        for column in schema.columns() {
            observe_legacy_map_field(&column.field, &mut observe)?;
        }
    }
    Ok(())
}

// Report affected maps before any cooperative turn. An encoder may record their
// addresses as lookup keys because it also retains the immutable schema owner.
fn observe_legacy_map_field(
    field: &RenderField,
    observe: &mut impl FnMut(&RenderField) -> Result<(), RenderError>,
) -> Result<bool, RenderError> {
    use novarocks_result_contract::{RenderIntervalField, RenderTemporalType};
    Ok(match &field.native_type {
        N::Temporal(RenderTemporalType::Interval { start, .. }) => {
            *start >= RenderIntervalField::Day
        }
        N::List(child) => observe_legacy_map_field(child, observe)?,
        N::Struct(children) => {
            let mut contains_interval = false;
            for child in children {
                contains_interval |= observe_legacy_map_field(&child.field, observe)?;
            }
            contains_interval
        }
        N::Map { key, value } => {
            let key_interval = observe_legacy_map_field(key, observe)?;
            let value_interval = observe_legacy_map_field(value, observe)?;
            if key_interval {
                observe(field)?;
            }
            key_interval || value_interval
        }
        _ => false,
    })
}
